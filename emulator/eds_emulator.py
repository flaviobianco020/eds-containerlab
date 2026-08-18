#!/usr/bin/env python3
"""
eds_emulator.py - Host-side control-plane of the Event-Driven Simulator emulator.

Brings the Phase 1 components that live in software in the simulator onto the
ContainerLab emulator, making them operate in REAL TIME on the real network:

  * Traffic Generator   -> launches the UDP agent (agent/eds_node.py) in the
                           containers, reproducing the simulator's FlowModel/TrafficClass.
  * Event Scheduler     -> RTScheduler: event heap on the real wall-clock.
  * Congestion State    -> CongestionStateMachine identical to simulator/network/
    Machine                congestion.py, fed by the real occupancy read from
                           `tc -s qdisc`, applies DROP_LOW_PRIORITY via tc.
  * Queue Manager       -> is the tbf+netem(limit) qdisc created by deploy.sh;
                           here we read it (backlog, drop, sent).
  * Metrics Engine      -> throughput, PDR, latency, occupancy, drop,
                           state transitions, fairness.

NB: the network (topology, capacity, queues) is the one deployed by ./deploy.sh.
This module does NOT modify the topology: it drives it and measures it.

Requires: Docker, an already-deployed lab, and python3 inside the containers
(ghcr.io/srl-labs/network-multitool image) with the agent mounted at /opt/eds.
"""
from __future__ import annotations

import heapq
import json
import os
import re
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field, replace
from enum import Enum
from typing import Callable, Optional

# The MAPPO Actor (Phase 3) lives in agent/eds_actor.py: add the folder to the
# path so the control-plane can load it to deploy the learned policy.
_AGENT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "agent")
if _AGENT_DIR not in sys.path:
    sys.path.insert(0, _AGENT_DIR)

AGENT_PATH      = "/opt/eds/eds_node.py"
COMPRESSOR_PATH = "/opt/eds/eds_compressor.py"
COMP_STATE_FILE = "/tmp/eds_comp_state"
COMP_STATS_FILE = "/tmp/eds_comp_stats"
NFQUEUE_NUM     = 1

# Minimum IP length (bytes) for a packet to go through the NFQUEUE compressor.
# Default 500: only VIDEO (>=~1428B) is compressed; CONTROL (128B) and TELEMETRY
# (228-328B) bypass, so as not to load the userspace. Lowering it to ~200 lets
# TELEMETRY (structured data, compresses ~4x) in too: a lever for the PDR of
# scenario 5 (theoretical bound ->100%), at the cost of more load on the compressor.
#   EDS_NFQUEUE_MINLEN=200 python3 emulator/scenarios.py 5 --phase2
NFQUEUE_MINLEN  = int(os.environ.get("EDS_NFQUEUE_MINLEN", "500"))

# Netem delay on the bottleneck. DEFAULT 0ms (BYTE-limited bottleneck, on par
# with the simulator). With a delay, by Little's law the packets in flight are
# rate*delay: with `limit 20` the delay "consumes" the buffer and the bottleneck
# saturates at limit/delay pkt/s INDEPENDENT of the bytes -> compression does not
# raise the PDR (scenarios 1/5), because the service becomes ∝ packets instead of
# ∝ bytes. With delay=0 the buffer is a pure drop-tail, the constraint goes back
# to the bandwidth (tbf) and compression empties the queue. The link latency lives
# on the access links (deploy.sh, 5ms). To reproduce the old packet-limited behavior:
#   EDS_BOTTLENECK_DELAY=5ms python3 emulator/scenarios.py 1 --mappo CKPT
BOTTLENECK_DELAY = os.environ.get("EDS_BOTTLENECK_DELAY", "0ms")

# --- Phase 3: Actor observation parameters (mirror of simulator/marl/env.py)
MAPPO_DT          = 1.0    # policy decision cadence: 1 s (== env.DT)
MAPPO_T_MAX_STATE = 30.0   # normalization t_state/T_max (doc Table 7)
MAPPO_CAP_BPS     = 10e6   # nominal bottleneck capacity (10 Mbit/s)
MAPPO_EMERGENCY_OCCUPANCY = float(os.environ.get("EDS_MAPPO_EMERGENCY_OCC", "0.95"))
MAPPO_ACTION_NAMES = ("ESCALATE", "MAINTAIN", "DEESCALATE")

# --- Phase 3: anti-empty-compression guardrail (deploy-side, OPT-IN) --------
# HISTORY: the original MAPPO policy (checkpoint "current") was trained with a
# reward WITHOUT compression cost, in a simulator where compression is NON
# destructive (simulator/control/compressor.py sets pkt.compressed_size WITHOUT
# touching pkt.size). There compressing is nearly free, so the policy compressed
# eagerly. On the emulator compression is DESTRUCTIVE (eds_compressor.py truncates
# the payload) and every packet pays the latency of the NFQUEUE middlebox:
# compressing when we are NOT losing packets throws away throughput without
# improving the PDR (scenario 2). To plug that behavior this deploy-side guardrail
# was needed: ESCALATE allowed only under real LOSS PRESSURE (drop_rate above
# threshold) or queue emergency. (Occupancy alone is NOT enough: in scenario 2
# the queue sits at 60-75% even on an idle network because of the CONTROL flow,
# ~2500 pkt/s of small packets, without overflowing -> drop~0, PDR~100%.)
#
# NOW: the checkpoint "mappo_ccgated" is trained with the GATED COMPRESSION COST
# reward (-lambda_c*(mean_state/4)*(1-congestion)): the penalty is paid only on an
# idle network, vanishing under congestion. This way the policy self-regulates
# compression AT THE SOURCE and the deploy-side guardrail becomes redundant
# (verified: scenario 2 at gate OFF -> 1.00x, no waste). Therefore the guardrail
# is OFF by default. It stays as opt-in (EDS_MAPPO_GATE=1) for the legacy
# checkpoints without compression-cost, which waste on S2 without the guardrail.
MAPPO_COMPRESSION_GATE = os.environ.get("EDS_MAPPO_GATE", "0") != "0"
MAPPO_GATE_DROP_RATE = float(os.environ.get("EDS_MAPPO_GATE_DROP_RATE", "0.01"))  # fraction of lost arrivals above which ESCALATE is justified (sweep with EDS_MAPPO_GATE_DROP_RATE)
MAPPO_TRACE_FEATURES = (
    "ewma_occupancy", "congestion_state", "high_priority_ratio",
    "low_priority_ratio", "drop_rate", "link_utilisation", "time_in_state",
)

_QDISC_SENT_RE = re.compile(r"Sent (\d+) bytes (\d+) pkt")
_QDISC_DROP_RE = re.compile(r"dropped (\d+)")
_QDISC_BACKLOG_RE = re.compile(r"backlog \S+ (\d+)p")


def parse_qdisc_stats(output: str, queue_limit: int,
                      queue_handle: str = "10:") -> dict:
    """Extracts counters without summing the parent and child qdisc.

    The TBF root and the netem child often expose the same drop. The transmitted
    traffic is read from the root, while drop and backlog come from the netem
    that implements the finite queue.
    """
    blocks = [b for b in re.split(r"(?=^qdisc )", output, flags=re.MULTILINE)
              if b.startswith("qdisc ")]
    root = next((b for b in blocks if " root " in b.splitlines()[0]), "")
    queue = next((b for b in blocks
                  if b.splitlines()[0].split()[2] == queue_handle), "")
    if not queue:
        queue = root
    sent_match = _QDISC_SENT_RE.search(root or queue)
    drop_match = _QDISC_DROP_RE.search(queue)
    backlog_match = _QDISC_BACKLOG_RE.search(queue)
    sent_bytes = int(sent_match.group(1)) if sent_match else 0
    sent_pkts = int(sent_match.group(2)) if sent_match else 0
    dropped = int(drop_match.group(1)) if drop_match else 0
    backlog_pkts = int(backlog_match.group(1)) if backlog_match else 0
    occupancy = backlog_pkts / queue_limit if queue_limit else 0.0
    return {
        "sent_bytes": sent_bytes,
        "sent_pkts": sent_pkts,
        "dropped": dropped,
        "backlog_pkts": backlog_pkts,
        "occupancy": min(max(occupancy, 0.0), 1.0),
    }


def mappo_action_mask(now: float, last_transition: float,
                      min_state_dwell: float, occupancy: float,
                      drop_rate: float = 0.0) -> list[bool]:
    """Deploy constraint identical to the robust simulator (dwell + emergency
    override). The anti-empty-compression guardrail (MAPPO_COMPRESSION_GATE) is
    OPT-IN and off by default: with the gated-compression-cost policy it is not
    needed, so with dwell=0 the mask is [True,True,True] (pure policy).

    Action order: [ESCALATE, MAINTAIN, DEESCALATE].
    """
    dwell_ok = min_state_dwell <= 0.0 or now - last_transition >= min_state_dwell
    if not dwell_ok:
        # Freeze during the dwell (as in the robust simulator), with an emergency
        # override that lets it rise if the queue is almost full.
        if occupancy >= MAPPO_EMERGENCY_OCCUPANCY:
            return [True, True, False]
        return [False, True, False]
    # Dwell satisfied: free actions, BUT no "empty" ESCALATE. Compressing only
    # makes sense under real loss pressure (or queue emergency); without it, an
    # ESCALATE only reduces the throughput at an already-full PDR.
    escalate_ok = (not MAPPO_COMPRESSION_GATE
                   or drop_rate > MAPPO_GATE_DROP_RATE
                   or occupancy >= MAPPO_EMERGENCY_OCCUPANCY)
    return [escalate_ok, True, True]

# ----------------------- Traffic classes / FlowModel ------------------------
# They mirror the classes used in the simulator's examples/scenarios.py.
# tuple: (size_lo, size_hi, priority, tos)  -- tos = DSCP for the priority.
#   priority 0 (control)   -> CS6  0xc0  (protected)
#   priority 1 (telemetry) -> CS2  0x40
#   priority 2 (video)     -> AF11 0x28
VIDEO = (1400, 1500, 2, 0x28)
TELEMETRY = (200, 300, 1, 0x40)
CONTROL = (100, 100, 0, 0xc0)

# DSCP dropped when the state is DROP_LOW_PRIORITY (priority > 0)
LOW_PRIORITY_TOS = (0x28, 0x40)


class FlowModel(str, Enum):
    CBR = "cbr"
    POISSON = "poisson"
    BURSTY = "bursty"
    PERIODIC_TELEMETRY = "periodic_telemetry"
    VIDEO = "video"
    CONTROL = "control"


# ----------------------- Congestion State Machine ---------------------------
# Identical to simulator/network/congestion.py.
class CongestionState(Enum):
    NORMAL = 0
    HEADER_COMPRESSION = 1
    DELTA_COMPRESSION = 2
    INCREMENTAL_COMPRESSION = 3
    DROP_LOW_PRIORITY = 4


DEFAULT_THRESHOLDS = {
    CongestionState.HEADER_COMPRESSION: 0.50,
    CongestionState.DELTA_COMPRESSION: 0.70,
    CongestionState.INCREMENTAL_COMPRESSION: 0.85,
    CongestionState.DROP_LOW_PRIORITY: 0.95,
}

# Phase 2 (eFRAC paper §3.3) — identical to simulator/network/congestion.py
PHASE2_EWMA_ALPHA: float = 0.125           # Jacobson/Karn α
PHASE2_ESCALATION_DEBOUNCE: float = 1.5   # seconds of sustained excess before rising
PHASE2_DEESCALATION_COOLDOWN: float = 4.5 # seconds below threshold before falling (3:1)


class CongestionStateMachine:
    """
    Phase 1 (default): instantaneous transitions, no smoothing — backward-compatible.
    Phase 2 (enable_phase2=True): EWMA α=1/8 + asymmetric hysteresis (eFRAC §3.3).

    Identical to simulator/network/congestion.py.
    """

    def __init__(self, thresholds=None,
                 ewma_alpha: float = 1.0,
                 escalation_debounce: float = 0.0,
                 deescalation_cooldown: float = 0.0):
        self.current_state = CongestionState.NORMAL
        self.thresholds = dict(thresholds or DEFAULT_THRESHOLDS)
        self.transitions = 0
        # EWMA
        self._alpha = ewma_alpha
        self._ewma: float = 0.0
        # Hysteresis
        self._escalation_debounce = escalation_debounce
        self._deescalation_cooldown = deescalation_cooldown
        self._escalate_since: Optional[float] = None
        self._deescalate_since: Optional[float] = None

    @property
    def ewma_occupancy(self) -> float:
        return self._ewma

    def evaluate(self, occupancy: float) -> CongestionState:
        result = CongestionState.NORMAL
        for state in (
            CongestionState.HEADER_COMPRESSION,
            CongestionState.DELTA_COMPRESSION,
            CongestionState.INCREMENTAL_COMPRESSION,
            CongestionState.DROP_LOW_PRIORITY,
        ):
            if occupancy >= self.thresholds[state]:
                result = state
        return result

    def _transition(self, new_state: CongestionState) -> bool:
        if new_state != self.current_state:
            self.current_state = new_state
            self.transitions += 1
            return True
        return False

    def update(self, occupancy: float, sim_time: float = 0.0) -> bool:
        """
        Phase 1 (alpha=1.0, debounce=0.0): instant jump to the target, no smoothing.
        Phase 2: EWMA + one step at a time with asymmetric debounce/cooldown.
        """
        self._ewma = (1.0 - self._alpha) * self._ewma + self._alpha * occupancy
        target = self.evaluate(self._ewma)

        if target.value > self.current_state.value:
            if self._escalation_debounce <= 0.0:
                self._escalate_since = None
                self._deescalate_since = None
                return self._transition(target)
            if self._escalate_since is None:
                self._escalate_since = sim_time
            self._deescalate_since = None
            if sim_time - self._escalate_since >= self._escalation_debounce:
                next_s = CongestionState(self.current_state.value + 1)
                self._escalate_since = None
                return self._transition(next_s)
            return False

        elif target.value < self.current_state.value:
            if self._deescalation_cooldown <= 0.0:
                self._escalate_since = None
                self._deescalate_since = None
                return self._transition(target)
            if self._deescalate_since is None:
                self._deescalate_since = sim_time
            self._escalate_since = None
            if sim_time - self._deescalate_since >= self._deescalation_cooldown:
                prev_s = CongestionState(self.current_state.value - 1)
                self._deescalate_since = None
                return self._transition(prev_s)
            return False

        else:
            self._escalate_since = None
            self._deescalate_since = None
            return False


# Expected ratios per state (weighted average over the simulator's three traffic classes).
# Used to estimate the compression_ratio in the metrics when enable_phase2=True.
# Source: simulator/control/compressor.py _RATIOS, average (pri=0, pri=1, pri=2).
#   NORMAL        : 1.00
#   HC            : mean(0.760,0.904,0.983) = 0.882 → ratio=1/0.882≈1.13
#   DELTA         : mean(0.550,0.500,0.667) = 0.572 → ratio=1/0.572≈1.75
#   INCREMENTAL   : mean(0.500,0.250,0.667) = 0.472 → ratio=1/0.472≈2.12
#   DROP          : only CONTROL survives → conservative ratio ≈ 1.0
_EXPECTED_COMPRESSION_RATIO: dict[str, float] = {
    "NORMAL":                  1.00,
    "HEADER_COMPRESSION":      1.13,
    "DELTA_COMPRESSION":       1.75,
    "INCREMENTAL_COMPRESSION": 2.12,
    "DROP_LOW_PRIORITY":       1.00,
}

# --------------------------------- Topologies --------------------------------
@dataclass
class Topo:
    key: str
    lab: str               # ContainerLab lab name (container prefix clab-<lab>-)
    dst_node: str
    dst_ip: str
    bottleneck_node: str   # node on which to read/act the queue
    bottleneck_if: str
    queue_limit: int = 20
    entry_node: Optional[str] = None  # if set, all flows enter here
                                      # (single-path topologies: multi_hop, mesh)


TOPOS = {
    "single_bottleneck": Topo("single_bottleneck", "single-bottleneck",
                              "dst", "10.0.30.1", "router", "eth4", 20),
    # multi_hop and mesh are single-path topologies: a single entry node
    # (entry_node) from which all flows start, and the monitored/compressed queue
    # is the one of the first hop. The canonical scenarios (sources src0/1/2) are
    # remapped onto this node in run_emulation.
    "multi_hop": Topo("multi_hop", "multi-hop",
                      "n3", "10.0.3.2", "n0", "eth1", 20, entry_node="n0"),
    "mesh": Topo("mesh", "mesh",
                 "n12", "10.1.7.2", "n00", "eth1", 20, entry_node="n00"),
}


# ------------------------------- docker / tc --------------------------------
class Net:
    """Helper to run commands in the containers and read/act on tc."""

    def __init__(self, topo: Topo, verbose: bool = True):
        self.topo = topo
        self.verbose = verbose
        self._drop_active = False
        self._comp_rules = []    # exact specs of the NFQUEUE iptables rules (FORWARD/OUTPUT)
        # current netem parameters of the bottleneck (for the failure via loss 100%)
        self._netem_delay = "0ms"
        self._netem_limit = topo.queue_limit

    def container(self, node: str) -> str:
        return f"clab-{self.topo.lab}-{node}"

    def exec(self, node: str, *args: str, timeout: float = 20.0) -> subprocess.CompletedProcess:
        cmd = ["docker", "exec", self.container(node), *args]
        return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)

    def sh(self, node: str, script: str, timeout: float = 20.0) -> subprocess.CompletedProcess:
        return self.exec(node, "sh", "-c", script, timeout=timeout)

    # --- preflight ----------------------------------------------------------
    def preflight(self):
        node = self.topo.dst_node
        try:
            r = self.exec(node, "python3", "--version", timeout=10.0)
        except (subprocess.TimeoutExpired, FileNotFoundError) as e:
            raise RuntimeError(f"Cannot reach {self.container(node)}: {e}")
        if r.returncode != 0:
            raise RuntimeError(
                "python3 not found in the container. Install with "
                f"`docker exec {self.container(node)} apk add --no-cache python3` "
                "or use an image with python3.")
        r = self.exec(node, "test", "-f", AGENT_PATH, timeout=10.0)
        if r.returncode != 0:
            raise RuntimeError(
                f"Agent not mounted at {AGENT_PATH}. Check the `binds` in the "
                ".clab.yml file (../agent:/opt/eds:ro) and redeploy.")

    # --- queue read (Queue Manager) -----------------------------------------
    def qdisc_stats(self) -> dict:
        """Reads tc -s qdisc on the bottleneck interface."""
        r = self.sh(self.topo.bottleneck_node,
                    f"tc -s qdisc show dev {self.topo.bottleneck_if}")
        return parse_qdisc_stats(r.stdout or "", self.topo.queue_limit)

    # --- scheduler / state machine actions ----------------------------------
    def set_bottleneck_rate(self, rate_mbit: float):
        node, iface = self.topo.bottleneck_node, self.topo.bottleneck_if
        self.sh(node, f"tc qdisc change dev {iface} root handle 1: "
                      f"tbf rate {rate_mbit}mbit burst 1mbit limit 1m")
        if self.verbose:
            print(f"      [tc] {iface}: bandwidth -> {rate_mbit} Mbit/s")

    def set_queue_limit(self, limit_pkts: int, delay: str = "0ms"):
        """Aligns the queue drop-tail (netem limit) to the requested value.
        delay=0 => byte-limited bottleneck (pure drop-tail); see BOTTLENECK_DELAY."""
        node, iface = self.topo.bottleneck_node, self.topo.bottleneck_if
        self._netem_delay = delay
        self._netem_limit = limit_pkts
        self.sh(node, f"tc qdisc change dev {iface} parent 1:1 handle 10: "
                      f"netem delay {delay} limit {limit_pkts}")
        if self.verbose:
            print(f"      [tc] {iface}: drop-tail queue -> {limit_pkts} packets")

    def link_down(self):
        # Failure modeled with netem 'loss 100%' instead of 'ip link down': all the
        # packets on the bottleneck are dropped, but the interface stays up and the
        # routes (including the STATIC ones of the source nodes on multi_hop/mesh)
        # are NOT removed by the kernel -> recovery is clean on every topology.
        # Preserves the current netem delay and limit.
        node, iface = self.topo.bottleneck_node, self.topo.bottleneck_if
        self.sh(node, f"tc qdisc change dev {iface} parent 1:1 handle 10: "
                      f"netem delay {self._netem_delay} limit {self._netem_limit} loss 100%")
        if self.verbose:
            print("      [link] bottleneck DOWN (netem loss 100%, routes intact)")

    def link_up(self):
        # Restore: netem without loss (current delay+limit).
        node, iface = self.topo.bottleneck_node, self.topo.bottleneck_if
        self.sh(node, f"tc qdisc change dev {iface} parent 1:1 handle 10: "
                      f"netem delay {self._netem_delay} limit {self._netem_limit}")
        if self.verbose:
            print("      [link] bottleneck UP")

    # --- Cleanup of residual state from previous runs -----------------------

    def cleanup_stale(self, port: int = 5000):
        """
        Removes any artifacts left by previous interrupted runs:
          - kills the compressor process still alive (causes ENOBUFS in the kernel)
          - flushes the FORWARD chain (residual NFQUEUE or DROP rules)
          - removes the DROP_LOW_PRIORITY tc filters

        ALWAYS called at the start of run_emulation, regardless of the mode.
        This prevents an interrupted Phase 2 run from blocking subsequent Phase 1
        runs because of an NFQUEUE rule with a full queue (packets dropped by kernel).
        """
        node, iface = self.topo.bottleneck_node, self.topo.bottleneck_if
        self.sh(node,
                "pid=$(cat /tmp/eds_comp.pid 2>/dev/null); "
                "[ -n \"$pid\" ] && kill \"$pid\" 2>/dev/null; true")
        self.sh(node, "iptables -F FORWARD 2>/dev/null || true")
        self.sh(node, "iptables -F OUTPUT 2>/dev/null || true")  # residual NFQUEUE rules on the source node (multi_hop/mesh)
        self.apply_drop_low_priority(False)

    # --- Phase 2: NFQUEUE compressor on the router side ---------------------

    def install_compressor_deps(self):
        """
        Installs libnetfilter_queue + NetfilterQueue Python in the router container.
        Called once at the start of the scenario with enable_phase2=True.
        """
        node = self.topo.bottleneck_node
        print(f"  [compressor] installing dependencies in {node} ...")
        # Alpine: build toolchain + dev headers + runtime libs + pip
        self.sh(node,
                "apk add --no-cache gcc musl-dev python3-dev py3-pip linux-headers "
                "libnetfilter_queue libnetfilter_queue-dev libmnl libmnl-dev 2>&1 | tail -1",
                timeout=180.0)
        # NetfilterQueue (C extension, compiled from source)
        r = self.sh(node,
                    "python3 -m pip install --break-system-packages -q NetfilterQueue 2>&1 || "
                    "python3 -m pip install -q NetfilterQueue 2>&1",
                    timeout=180.0)
        last = (r.stdout or "").strip().splitlines()
        if last:
            print(f"      [pip] {last[-1]}")
        # verify the import actually works (fails immediately if something is missing)
        chk = self.sh(node, "python3 -c 'import netfilterqueue' 2>&1")
        if chk.returncode != 0:
            raise RuntimeError("NetfilterQueue not importable in the router: "
                               + (chk.stdout or "").strip())
        print("      [compressor] NetfilterQueue ready")

    def start_compressor(self, port: int = 5000):
        """
        Adds the NFQUEUE iptables rule on the bottleneck link and starts
        eds_compressor.py in the background in the router container.

        The rule intercepts only UDP towards the receiver port (EDS_PORT)
        transiting on the egress interface of the bottleneck.
        --queue-bypass: if the process crashes, the packets pass through
        uncompressed instead of being dropped (fail-open for robustness).
        """
        node, iface = self.topo.bottleneck_node, self.topo.bottleneck_if
        # Initial state = 0 (NORMAL)
        self.sh(node, f"echo 0 > {COMP_STATE_FILE}")
        # Only packets >= 500 bytes (total IP) go through NFQUEUE.
        # The CONTROL packets (100 B payload = 128 B IP) pass directly:
        # reduces the load on the userspace Python process by ~90%.
        self.sh(node, f"rm -f {COMP_STATS_FILE}")  # reset the counters of the previous run
        # Hook the rule on FORWARD *and* OUTPUT: on single_bottleneck the compressor
        # node FORWARDS the traffic (FORWARD chain), but on the single-path
        # topologies (multi_hop/mesh) the entry node ORIGINATES the traffic (OUTPUT
        # chain). A packet crosses only one of the two chains, so there is no double
        # compression; hooking both makes the compressor independent of the node's role.
        rule_spec = (f"-o {iface} -p udp --dport {port} "
                     f"-m length --length {NFQUEUE_MINLEN}:65535 "
                     f"-j NFQUEUE --queue-num {NFQUEUE_NUM} --queue-bypass")
        self._comp_rules = [f"FORWARD {rule_spec}", f"OUTPUT {rule_spec}"]
        # removes any residual rules from previous runs, then adds
        for rule in self._comp_rules:
            self.sh(node, f"iptables -D {rule} 2>/dev/null || true")
            self.sh(node, f"iptables -A {rule}")
        # Start the compressor in the background, log in /tmp/eds_comp.log
        self.sh(node,
                f"python3 {COMPRESSOR_PATH} {NFQUEUE_NUM} "
                f"> /tmp/eds_comp.log 2>&1 & echo $! > /tmp/eds_comp.pid")
        time.sleep(0.8)  # wait for the process to start
        # Health-check: if the process died at startup (e.g. failed import after a
        # flaky install), remove the rule -> REAL fail-open. With the rule present
        # and no consumer, --queue-bypass should be enough, but removing the rule
        # eliminates any risk of a residual black-hole.
        alive = self.sh(node,
                        "pid=$(cat /tmp/eds_comp.pid 2>/dev/null); "
                        "[ -n \"$pid\" ] && kill -0 \"$pid\" 2>/dev/null "
                        "&& echo alive || echo dead")
        log = self.sh(node, "cat /tmp/eds_comp.log")
        if "alive" not in (alive.stdout or ""):
            print("      [compressor] WARNING: process not active after start "
                  "-> removing the NFQUEUE rule (fail-open).")
            print(f"      [compressor:log] {(log.stdout or '').strip()}")
            for rule in self._comp_rules:
                self.sh(node, f"iptables -D {rule} 2>/dev/null || true")
            self._comp_rules = []
        else:
            nfq = self.nfqueue_stats()
            bound = "yes" if (nfq and nfq.get("peer_portid", 0) != 0) else "NO"
            if self.verbose:
                print(f"      [compressor] {(log.stdout or '').strip()}  "
                      f"(pid alive, queue bound: {bound})")

    def update_compression_state(self, state_value: int):
        """
        Writes the state value (0-4) to the file read by the compressor.
        Called by the controller tick at every state transition.
        """
        self.sh(self.topo.bottleneck_node,
                f"echo {state_value} > {COMP_STATE_FILE}")

    def nfqueue_stats(self) -> Optional[dict]:
        """Reads /proc/net/netfilter/nfnetlink_queue for the NFQUEUE_NUM queue.

        Columns: queue_num peer_portid queue_total copy_mode copy_range
                 queue_dropped user_dropped id_sequence.

        Middlebox diagnostics (scenario 5 Phase 2, "anomalous run"):
          * missing row / peer_portid == 0  -> NO consumer bound
            (--queue-bypass lets the packets through: fail-open).
          * peer_portid != 0 + queue_dropped growing -> consumer bound but
            STUCK: the kernel drops the packets at a full queue (black-hole). This
            is the case that --queue-bypass does NOT cover.
        Returns None if the queue is not bound.
        """
        r = self.sh(self.topo.bottleneck_node,
                    "cat /proc/net/netfilter/nfnetlink_queue 2>/dev/null || true")
        for line in (r.stdout or "").splitlines():
            f = line.split()
            if len(f) >= 7 and f[0] == str(NFQUEUE_NUM):
                return {"peer_portid": int(f[1]), "queue_total": int(f[2]),
                        "queue_dropped": int(f[5]), "user_dropped": int(f[6])}
        return None

    def compressor_stats(self) -> Optional[dict]:
        """Reads the counters written by eds_compressor.py in COMP_STATS_FILE:
        pkts (processed) bytes_in bytes_out compressed. Real on-the-wire ratio =
        bytes_in/bytes_out. Returns None if the file is not there."""
        r = self.sh(self.topo.bottleneck_node,
                    f"cat {COMP_STATS_FILE} 2>/dev/null || true")
        parts = (r.stdout or "").split()
        if len(parts) < 4:
            return None
        try:
            return {"pkts": int(parts[0]), "bytes_in": int(parts[1]),
                    "bytes_out": int(parts[2]), "compressed": int(parts[3])}
        except ValueError:
            return None

    def nfqueue_rule_matched(self) -> Optional[int]:
        """Packets that matched the NFQUEUE rule (iptables counter). Compared with
        compressor_stats()['pkts'] it measures the BYPASS: matched - processed =
        packets passed through uncompressed (--queue-bypass).

        Sums over FORWARD and OUTPUT: on single_bottleneck the match is on FORWARD
        (forwarding node), on multi_hop/mesh on OUTPUT (originating node)."""
        total = None
        for chain in ("FORWARD", "OUTPUT"):
            r = self.sh(self.topo.bottleneck_node,
                        f"iptables -nvxL {chain} 2>/dev/null || true")
            for line in (r.stdout or "").splitlines():
                if f"NFQUEUE num {NFQUEUE_NUM}" in line:
                    f = line.split()
                    if len(f) >= 1 and f[0].isdigit():
                        total = (total or 0) + int(f[0])   # 'pkts' column
        return total

    def stop_compressor(self):
        """Removes the iptables rule (exact match) and stops the compressor."""
        node = self.topo.bottleneck_node
        for rule in self._comp_rules:
            self.sh(node, f"iptables -D {rule} 2>/dev/null || true")
        self.sh(node,
                "pid=$(cat /tmp/eds_comp.pid 2>/dev/null); "
                "[ -n \"$pid\" ] && kill $pid 2>/dev/null || true")
        if self.verbose:
            print("      [compressor] stopped, iptables rule removed")

    def apply_drop_low_priority(self, active: bool):
        """Adds/removes the tc filters that drop the low-priority traffic."""
        node, iface = self.topo.bottleneck_node, self.topo.bottleneck_if
        if active and not self._drop_active:
            for tos in LOW_PRIORITY_TOS:
                self.sh(node, f"tc filter add dev {iface} parent 1: protocol ip prio 5 "
                              f"u32 match ip tos {hex(tos)} 0xfc action drop")
            self._drop_active = True
            if self.verbose:
                print("      [state] DROP_LOW_PRIORITY active (dropping priority > 0)")
        elif not active and self._drop_active:
            self.sh(node, f"tc filter del dev {iface} parent 1: prio 5")
            self._drop_active = False
            if self.verbose:
                print("      [state] DROP_LOW_PRIORITY disabled")


# ------------------------- Real-time scheduler ------------------------------
@dataclass(order=True)
class _SchedItem:
    t: float
    seq: int
    fn: Callable = field(compare=False)
    args: tuple = field(default=(), compare=False)


class RTScheduler:
    """Heap of events ordered by time, executed on the real wall-clock."""

    def __init__(self):
        self._q: list[_SchedItem] = []
        self._seq = 0
        self._t0 = 0.0

    def at(self, t: float, fn: Callable, *args):
        heapq.heappush(self._q, _SchedItem(t, self._seq, fn, args))
        self._seq += 1

    def now(self) -> float:
        return time.monotonic() - self._t0

    def run(self, end_time: float):
        self._t0 = time.monotonic()
        while self._q:
            item = heapq.heappop(self._q)
            if item.t > end_time:
                break
            delay = item.t - self.now()
            if delay > 0:
                time.sleep(delay)
            try:
                item.fn(*item.args)
            except Exception as e:  # noqa: BLE001 - an event must not stop the run
                print(f"      [scheduler] event error: {e}")


# --------------------------------- Flows ------------------------------------
@dataclass
class FlowSpec:
    fid: int
    src: str                 # source node
    model: FlowModel
    mbit: float              # target bandwidth (== simulator pkt/s rate)
    tclass: tuple            # (size_lo, size_hi, priority, tos)
    start: float = 0.0
    stop: Optional[float] = None   # None => until end of simulation

    def pps(self) -> float:
        lo, hi, _pri, _tos = self.tclass
        avg = (lo + hi) / 2.0
        return max(self.mbit * 1e6 / (avg * 8.0), 1.0)


# --------------------------- Metrics Engine ---------------------------------
class Metrics:
    """Equivalent of simulator/metrics.py, but on real measurements."""

    def __init__(self):
        self.samples = []          # (t, occupancy, state, throughput_pps)
        self.transitions = 0
        # time-per-state tracking (Phase 2: compression_ratio estimate)
        self._state_time: dict[str, float] = {s.name: 0.0 for s in CongestionState}
        self._state_enter_t: float = 0.0
        self._last_state: str = CongestionState.NORMAL.name

    def record_state_time(self, new_state_name: str, now: float) -> None:
        """Closes the timer of the previous state, opens the one of the new state."""
        self._state_time[self._last_state] += now - self._state_enter_t
        self._state_enter_t = now
        self._last_state = new_state_name

    def close_state_time(self, end_t: float) -> None:
        """Closes the timer of the current state at the end of the simulation."""
        self._state_time[self._last_state] += end_t - self._state_enter_t

    def compression_ratio(self) -> float:
        """Estimates the compression_ratio as a weighted average over time per state."""
        total = sum(self._state_time.values())
        if total <= 0.0:
            return 1.0
        return sum(
            self._state_time[s] * _EXPECTED_COMPRESSION_RATIO[s]
            for s in self._state_time
        ) / total

    def jain(self, values) -> float:
        vals = [v for v in values if v is not None]
        if not vals:
            return 1.0
        s = sum(vals)
        s2 = sum(v * v for v in vals)
        n = len(vals)
        return (s * s) / (n * s2) if s2 > 0 else 1.0


# ------------------------ Observation for the MAPPO Actor -------------------
def _offered_priority_ratios(flows: list[FlowSpec], t: float,
                             end_time: float) -> tuple:
    """
    Estimates hi_pri_ratio / lo_pri_ratio from the OFFERED traffic mix at time t.

    In the simulator these features (doc Table 7) are the fraction of CONTROL /
    VIDEO packets in the queue; on the emulator the queue composition is not
    inspectable via `tc`, so it is approximated with the fraction of pkt/s offered
    per priority by the active flows. It is the only sim-to-real gap point of the
    observation (documented in the README).
    """
    hi = lo = tot = 0.0
    for fs in flows:
        stop = end_time if fs.stop is None else fs.stop
        if fs.start <= t < stop:
            pps = fs.pps()
            pri = fs.tclass[2]
            tot += pps
            if pri == 0:
                hi += pps
            elif pri == 2:
                lo += pps
    if tot <= 0.0:
        return 0.0, 0.0
    return hi / tot, lo / tot


# --------------------------------- Runner -----------------------------------
def run_emulation(topo_key: str, flows: list[FlowSpec], events: list[tuple],
                  end_time: float, metric_interval: float = 10.0,
                  tick: float = 0.5, seed: int = 42, port: int = 5000,
                  title: str = "", queue_limit: Optional[int] = None,
                  enable_phase2: bool = False,
                  mappo_ckpt: Optional[str] = None,
                  mappo_trace_path: Optional[str] = None) -> dict:
    """
    Runs a complete scenario:
      - starts the receiver on the destination node,
      - schedules flows (FLOW_START/STOP), network events and METRIC_SAMPLE samples,
      - runs the congestion state machine controller in real time,
      - collects and prints the final metrics.
    """
    topo = TOPOS[topo_key]
    if queue_limit is not None:
        topo = replace(topo, queue_limit=queue_limit)
    # Single-path topologies (multi_hop, mesh): remap every source onto the entry
    # node, so the same canonical scenarios (src0/src1/src2) run as-is by making
    # all flows enter at the first hop towards topo.dst_node.
    if topo.entry_node is not None:
        flows = [replace(f, src=topo.entry_node) for f in flows]
    net = Net(topo)
    print("=" * 70)
    if title:
        print(f"  {title}")
    print(f"  Topology: {topo_key}   destination: {topo.dst_node} ({topo.dst_ip})")
    print("=" * 70)
    net.preflight()
    net.cleanup_stale(port=port)
    # Always (re)apply the netem configuration of the bottleneck with the chosen
    # delay (default 5ms = deploy.sh). With a low EDS_BOTTLENECK_DELAY the buffer
    # goes back to pure drop-tail and compression becomes effective again (see constant).
    net.set_queue_limit(topo.queue_limit, delay=BOTTLENECK_DELAY)
    _blm = ("byte-limited (pure drop-tail)"
            if BOTTLENECK_DELAY in ("0ms", "0", "0.0ms") else "packet-limited")
    print(f"  bottleneck: tbf + netem delay {BOTTLENECK_DELAY} limit "
          f"{topo.queue_limit}  [{_blm}]  (EDS_BOTTLENECK_DELAY)")

    mappo_mode = mappo_ckpt is not None
    actor = None
    min_state_dwell = 0.0
    mappo_trace_rows = []
    if mappo_mode:
        from eds_actor import Actor as MappoActor  # lazy import (only if needed)
        actor = MappoActor.from_checkpoint(mappo_ckpt)
        min_state_dwell = float(getattr(actor, "meta", {}).get(
            "min_state_dwell", 0.0))
        # override for the deploy-knob experiments (without re-exporting the ckpt)
        if os.environ.get("EDS_MAPPO_DWELL") is not None:
            min_state_dwell = float(os.environ["EDS_MAPPO_DWELL"])

    # Phase 2 and Phase 3 use the same compression infrastructure (NFQUEUE
    # middlebox): only WHO decides the state changes. In Phase 3 it is the Actor.
    if enable_phase2 or mappo_mode:
        net.install_compressor_deps()
        net.start_compressor(port=port)
        classes = "VIDEO" if NFQUEUE_MINLEN > 328 else "VIDEO+TELEMETRY"
        print(f"  NFQUEUE compressor: min threshold {NFQUEUE_MINLEN}B -> compresses "
              f"{classes}  (EDS_NFQUEUE_MINLEN to change it)")

    if mappo_mode:
        # PASSIVE state machine: only updates the EWMA, does not transition on its
        # own; the transitions are decided by the policy (like AgentControlledStateMachine).
        sm = CongestionStateMachine(ewma_alpha=PHASE2_EWMA_ALPHA)
        meta = getattr(actor, "meta", {})
        print(f"  Mode: PHASE 3  (MAPPO — the learned policy drives the state machine)")
        print(f"  checkpoint: {os.path.basename(mappo_ckpt)}  "
              f"(episode {meta.get('episode','?')}, "
              f"λ_stab={meta.get('stability_penalty', 0.0)}, "
              f"dwell={min_state_dwell:.1f}s)")
        if MAPPO_COMPRESSION_GATE:
            print(f"  compression guardrail: ACTIVE (opt-in)  (ESCALATE only if "
                  f"drop_rate>{MAPPO_GATE_DROP_RATE:.0%} or occ>="
                  f"{MAPPO_EMERGENCY_OCCUPANCY:.0%}; for the legacy checkpoints)")
        else:
            print("  compression guardrail: OFF (default)  (pure policy; "
                  "EDS_MAPPO_GATE=1 to re-enable it on the legacy checkpoints)")
    elif enable_phase2:
        sm = CongestionStateMachine(
            ewma_alpha=PHASE2_EWMA_ALPHA,
            escalation_debounce=PHASE2_ESCALATION_DEBOUNCE,
            deescalation_cooldown=PHASE2_DEESCALATION_COOLDOWN,
        )
        print(f"  Mode: PHASE 2  (EWMA α={PHASE2_EWMA_ALPHA}, "
              f"escalation={PHASE2_ESCALATION_DEBOUNCE}s, "
              f"cooldown={PHASE2_DEESCALATION_COOLDOWN}s)")
    else:
        sm = CongestionStateMachine()
        print("  Mode: PHASE 1  (instantaneous transitions)")
    metrics = Metrics()
    results = {"send": [], "recv": None}
    threads: list[threading.Thread] = []

    # --- receiver ----------------------------------------------------------
    def _recv_worker():
        cmd = ["docker", "exec",
               "-e", f"EDS_PORT={port}",
               "-e", f"EDS_DURATION={end_time}",
               net.container(topo.dst_node),
               "python3", AGENT_PATH, "recv"]
        try:
            r = subprocess.run(cmd, capture_output=True, text=True, timeout=end_time + 30)
            results["recv"] = _parse_json(r.stdout)
        except subprocess.TimeoutExpired:
            results["recv"] = None

    rt = threading.Thread(target=_recv_worker, daemon=True)
    rt.start()
    threads.append(rt)
    time.sleep(1.0)  # let the receiver come up

    # --- sender per flow ---------------------------------------------------
    def _send_worker(fs: FlowSpec, duration: float):
        lo, hi, _pri, tos = fs.tclass
        cmd = ["docker", "exec",
               "-e", "EDS_MODE=send",
               "-e", f"EDS_DST={topo.dst_ip}",
               "-e", f"EDS_PORT={port}",
               "-e", f"EDS_FLOW_ID={fs.fid}",
               "-e", f"EDS_MODEL={fs.model.value}",
               "-e", f"EDS_RATE={fs.pps():.3f}",
               "-e", f"EDS_SIZE_LO={lo}",
               "-e", f"EDS_SIZE_HI={hi}",
               "-e", f"EDS_TOS={tos}",
               "-e", f"EDS_DURATION={duration:.3f}",
               "-e", f"EDS_SEED={seed}",
               net.container(fs.src),
               "python3", AGENT_PATH, "send"]
        try:
            r = subprocess.run(cmd, capture_output=True, text=True, timeout=duration + 30)
            parsed = _parse_json(r.stdout)
            if parsed:
                results["send"].append(parsed)
        except subprocess.TimeoutExpired:
            pass

    def _start_flow(fs: FlowSpec):
        stop = end_time if fs.stop is None else fs.stop
        duration = max(stop - fs.start, 0.5)
        print(f"  [t={fs.start:5.1f}] FLOW_START  flow {fs.fid}  {fs.src}->{topo.dst_node}  "
              f"{fs.model.value} {fs.mbit:.0f}Mbit pri={fs.tclass[2]}")
        th = threading.Thread(target=_send_worker, args=(fs, duration), daemon=True)
        th.start()
        threads.append(th)

    # --- Phase 1/2 controller (automatic state machine) --------------------
    def _controller_tick(sched: RTScheduler):
        now = sched.now()
        st = net.qdisc_stats()
        changed = sm.update(st["occupancy"], sim_time=now)
        if changed:
            metrics.transitions += 1
            metrics.record_state_time(sm.current_state.name, now)
            print(f"  [t={now:5.1f}] STATE -> {sm.current_state.name}  "
                  f"(EWMA occ={sm.ewma_occupancy*100:.1f}%)")
            if enable_phase2:
                net.update_compression_state(sm.current_state.value)
        net.apply_drop_low_priority(sm.current_state == CongestionState.DROP_LOW_PRIORITY)
        nt = now + tick
        if nt <= end_time:
            sched.at(nt, _controller_tick, sched)

    # --- Phase 3 controller (the MAPPO policy drives the state machine) -----
    mstate = {"sent_bytes": 0, "sent_pkts": 0, "dropped": 0, "t": 0.0,
              "last_transition": 0.0, "cap_bps": MAPPO_CAP_BPS, "ewma": 0.0}

    def _mappo_tick(sched: RTScheduler):
        now = sched.now()
        st = net.qdisc_stats()
        dt = max(now - mstate["t"], 1e-6)
        d_bytes = max(st["sent_bytes"] - mstate["sent_bytes"], 0)
        d_sent = max(st["sent_pkts"] - mstate["sent_pkts"], 0)
        d_drop = max(st["dropped"] - mstate["dropped"], 0)
        mstate["sent_bytes"] = st["sent_bytes"]
        mstate["sent_pkts"] = st["sent_pkts"]
        mstate["dropped"] = st["dropped"]
        mstate["t"] = now

        # EWMA of the occupancy (α = Phase 2), updated by hand: the machine stays passive
        mstate["ewma"] = ((1.0 - PHASE2_EWMA_ALPHA) * mstate["ewma"]
                          + PHASE2_EWMA_ALPHA * st["occupancy"])
        sm._ewma = mstate["ewma"]  # for consistent logging

        # drop_rate = fraction of arrivals dropped in the window (proxy of deltas["drop"]/gen)
        arrived = d_sent + d_drop
        drop_rate = d_drop / arrived if arrived > 0 else 0.0
        link_util = min(d_bytes * 8.0 / (mstate["cap_bps"] * dt), 1.0)
        hi, lo = _offered_priority_ratios(flows, now, end_time)
        t_in_state = min((now - mstate["last_transition"]) / MAPPO_T_MAX_STATE, 1.0)

        obs = [
            min(max(mstate["ewma"], 0.0), 1.0),      # ewma_occ
            sm.current_state.value / 4.0,            # state / 4
            hi,                                      # hi_pri_ratio (offered)
            lo,                                      # lo_pri_ratio (offered)
            min(max(drop_rate, 0.0), 1.0),           # window drop rate
            link_util,                               # link utilization
            t_in_state,                              # t_state / T_max
        ]
        action_mask = mappo_action_mask(
            now, mstate["last_transition"], min_state_dwell, st["occupancy"],
            drop_rate=drop_rate)
        action_probs = actor.probs(obs, action_mask=action_mask)
        action = actor.act(obs, action_mask=action_mask)

        cur = sm.current_state.value
        if action == 0:      # ESCALATE
            new = min(cur + 1, CongestionState.DROP_LOW_PRIORITY.value)
        elif action == 2:    # DE-ESCALATE
            new = max(cur - 1, CongestionState.NORMAL.value)
        else:                # MAINTAIN
            new = cur
        if new != cur:
            sm.current_state = CongestionState(new)
            sm.transitions += 1
            metrics.transitions += 1
            metrics.record_state_time(sm.current_state.name, now)
            mstate["last_transition"] = now
            net.update_compression_state(sm.current_state.value)
            print(f"  [t={now:5.1f}] MAPPO -> {sm.current_state.name}  "
                  f"(occ={st['occupancy']*100:.0f}% ewma={mstate['ewma']*100:.0f}% "
                  f"util={link_util*100:.0f}%)")
        if mappo_trace_path:
            mappo_trace_rows.append({
                "t": now,
                "observation": dict(zip(MAPPO_TRACE_FEATURES, obs)),
                "action": MAPPO_ACTION_NAMES[action],
                "action_id": action,
                "action_probabilities": action_probs,
                "action_mask": action_mask,
                "state_after": sm.current_state.value,
            })
        net.apply_drop_low_priority(sm.current_state == CongestionState.DROP_LOW_PRIORITY)
        nt = now + MAPPO_DT
        if nt <= end_time:
            sched.at(nt, _mappo_tick, sched)

    # --- metric sample -----------------------------------------------------
    state_prev = {"sent": 0, "t": 0.0}

    def _metric_sample(sched: RTScheduler):
        t = sched.now()
        st = net.qdisc_stats()
        dt = max(t - state_prev["t"], 1e-6)
        thr = (st["sent_pkts"] - state_prev["sent"]) / dt
        state_prev["sent"] = st["sent_pkts"]
        state_prev["t"] = t
        metrics.samples.append((t, st["occupancy"], sm.current_state.name, thr))
        # drops relative to the start of the run (the tc counters are cumulative from deploy)
        drops_run = st["dropped"] - stats0["dropped"]
        print(f"  [t={t:5.1f}] METRIC  occ={st['occupancy']*100:5.1f}%  "
              f"state={sm.current_state.name:<22}  thr={max(thr,0.0):7.1f} pkt/s  "
              f"drop={drops_run}")
        nt = t + metric_interval
        if nt <= end_time + 1e-6:
            sched.at(nt, _metric_sample, sched)

    # --- scheduler construction -------------------------------------------
    def _rate_change(mbit):
        net.set_bottleneck_rate(mbit)
        mstate["cap_bps"] = mbit * 1e6   # keeps link_util consistent after a bandwidth change

    sched = RTScheduler()
    for fs in flows:
        sched.at(fs.start, _start_flow, fs)
    for (et, kind, param) in events:
        if kind == "rate":
            sched.at(et, _rate_change, param)
        elif kind == "down":
            sched.at(et, net.link_down)
        elif kind == "up":
            sched.at(et, net.link_up)
    if mappo_mode:
        sched.at(MAPPO_DT, _mappo_tick, sched)
    else:
        sched.at(tick, _controller_tick, sched)
    sched.at(metric_interval, _metric_sample, sched)

    stats0 = net.qdisc_stats()
    # align the references to the initial state (tc counters cumulative from deploy)
    state_prev["sent"] = stats0["sent_pkts"]
    mstate["sent_bytes"] = stats0["sent_bytes"]
    mstate["sent_pkts"] = stats0["sent_pkts"]
    mstate["dropped"] = stats0["dropped"]
    metrics._state_enter_t = 0.0  # the run starts at t=0
    sched.run(end_time)

    # --- closing: wait for sender/receiver ---------------------------------
    print("  ... waiting for sender and receiver to close ...")
    for th in threads:
        th.join(timeout=40)
    if enable_phase2 or mappo_mode:
        # Middlebox diagnostics BEFORE stopping it: capture in the run log whether
        # the NFQUEUE queue black-holed (queue_dropped) or was not bound. It is the
        # proof that confirms/refutes the "anomalous run" of scenario 5 Phase 2.
        nfq_end = net.nfqueue_stats()
        if nfq_end is None:
            print("  [nfqueue] queue NOT bound at end of run (no consumer; "
                  "--queue-bypass active, packets passed through uncompressed).")
        else:
            print(f"  [nfqueue] peer_portid={nfq_end['peer_portid']}  "
                  f"queue_total={nfq_end['queue_total']}  "
                  f"queue_dropped={nfq_end['queue_dropped']}  "
                  f"user_dropped={nfq_end['user_dropped']}")
            if nfq_end["queue_dropped"] > 0:
                print("  [nfqueue] WARNING: queue_dropped>0 -> the compressor did "
                      "not keep up with the rate: packets dropped by the kernel at a "
                      "full queue (black-hole; --queue-bypass does NOT cover this case).")
        # REAL on-the-wire compression + bypass: answers "why does the PDR of
        # scenario 1/5 not rise?". real_ratio = bytes_in/bytes_out (deep vs no-op);
        # bypass = packets that matched but were NOT compressed
        # (--queue-bypass under load) = matched - processed.
        cs = net.compressor_stats()
        matched = net.nfqueue_rule_matched()
        if cs is not None:
            ratio = (cs["bytes_in"] / cs["bytes_out"]) if cs["bytes_out"] else 1.0
            frac_c = (cs["compressed"] / cs["pkts"] * 100.0) if cs["pkts"] else 0.0
            line = (f"  [compressor] processed={cs['pkts']}  real_wire_ratio="
                    f"{ratio:.3f}x  truncated={frac_c:.0f}%")
            if matched is not None:
                bypass = max(matched - cs["pkts"], 0)
                bp = (bypass / matched * 100.0) if matched else 0.0
                line += f"  matched={matched}  bypass={bypass} ({bp:.0f}%)"
            print(line)
            if matched and (matched - cs["pkts"]) > 0.2 * matched:
                print("  [compressor] NB: high bypass -> the userspace compressor "
                      "does not keep up with the rate; many packets pass through "
                      "UNCOMPRESSED (explains why compression does not empty the queue).")
            elif ratio < 1.05:
                print("  [compressor] NB: real ratio ~1.0 -> the controller is NOT "
                      "reaching/holding a deep compression state.")
        log = net.sh(net.topo.bottleneck_node, "cat /tmp/eds_comp.log 2>/dev/null")
        tail = (log.stdout or "").strip().splitlines()[-5:]
        if tail:
            print("  [compressor:log] " + " | ".join(tail))
        net.stop_compressor()
    net.apply_drop_low_priority(False)  # cleans up the tc filters
    stats1 = net.qdisc_stats()
    metrics.close_state_time(end_time)

    if mappo_mode and mappo_trace_path:
        trace_dir = os.path.dirname(os.path.abspath(mappo_trace_path))
        os.makedirs(trace_dir, exist_ok=True)
        with open(mappo_trace_path, "w") as fh:
            json.dump({
                "schema": "eds-mappo-observation-trace-v1",
                "features": list(MAPPO_TRACE_FEATURES),
                "checkpoint": os.path.abspath(mappo_ckpt),
                "min_state_dwell": min_state_dwell,
                "rows": mappo_trace_rows,
            }, fh, indent=2)
        print(f"  MAPPO trace saved: {os.path.abspath(mappo_trace_path)}")

    summary = _summarize(topo, flows, results, metrics, stats0, stats1, end_time)
    # Detection of an "anomalous run": delivers almost nothing (like scenario 5
    # Phase 2 rep 1: throughput 0.06, 0 transitions). Made noisy in the log instead
    # of disappearing as a ~0 average throughput in the benchmark.
    if summary["generated"] > 100 and summary["packet_delivery_ratio"] < 0.1:
        print("  " + "!" * 66)
        print(f"  ANOMALOUS RUN: delivered only "
              f"{summary['packet_delivery_ratio']*100:.1f}% of the generated packets.")
        if enable_phase2 or mappo_mode:
            print("  Main suspect: NFQUEUE middlebox (compressor stuck or dead). "
                  "See [nfqueue] and [compressor:log] above.")
        else:
            print("  Main suspect: receiver or forwarding (no middlebox in this mode).")
        print("  " + "!" * 66)
    return summary


def _parse_json(text: str) -> Optional[dict]:
    if not text:
        return None
    for line in reversed(text.strip().splitlines()):
        line = line.strip()
        if line.startswith("{"):
            try:
                return json.loads(line)
            except ValueError:
                continue
    return None


def _summarize(topo, flows, results, metrics, stats0, stats1, end_time) -> dict:
    sent_total = sum(s.get("sent", 0) for s in results["send"])
    bytes_sent_total = sum(s.get("bytes", 0) for s in results["send"])
    recv = results.get("recv") or {"flows": {}}
    recv_flows = recv.get("flows", {})
    recv_total = sum(f.get("recv", 0) for f in recv_flows.values())
    bytes_total = sum(f.get("bytes", 0) for f in recv_flows.values())
    lat_sum = sum(f.get("lat_sum", 0.0) for f in recv_flows.values())
    lat_n = sum(f.get("lat_n", 0) for f in recv_flows.values())

    pdr = (recv_total / sent_total) if sent_total else 0.0
    thr_pps = recv_total / end_time if end_time else 0.0
    thr_mbps = (bytes_total * 8.0 / end_time / 1e6) if end_time else 0.0
    latency_ms = (lat_sum / lat_n * 1000.0) if lat_n else 0.0
    drops = stats1["dropped"] - stats0["dropped"]
    endpoint_loss = max(sent_total - recv_total, 0)
    occ_avg = (sum(s[1] for s in metrics.samples) / len(metrics.samples)
               if metrics.samples else 0.0)

    # Real compression ratio from measured bytes.
    # avg_orig_size = average original size per packet (sender side, pre-compression).
    # bytes_total = bytes actually received (post-compression on the bottleneck link).
    # ratio = (orig_per_pkt × delivered_pkt) / received_bytes  →  > 1.0 if compression active.
    avg_orig_size = bytes_sent_total / sent_total if sent_total else 0.0
    if bytes_total > 0 and avg_orig_size > 0:
        real_compression_ratio = (avg_orig_size * recv_total) / bytes_total
    else:
        real_compression_ratio = metrics.compression_ratio()

    # fairness (Jain) on the per-flow throughput
    per_flow_thr = {str(fid): 0 for fid in (f.fid for f in flows)}
    for fid_str, f in recv_flows.items():
        per_flow_thr[str(fid_str)] = f.get("recv", 0)
    fairness = metrics.jain(list(per_flow_thr.values()))

    # Per-class KPIs, derived from the flow identifiers present both in the sender
    # report and in the receiver report. They do not depend on the tc counters.
    class_names = {0: "CONTROL", 1: "TELEMETRY", 2: "VIDEO"}
    flow_priority = {str(f.fid): f.tclass[2] for f in flows}
    class_metrics = {
        name: {"generated": 0, "delivered": 0, "pdr": 0.0}
        for name in class_names.values()
    }
    for sender in results["send"]:
        fid = str(sender.get("flow_id"))
        pri = flow_priority.get(fid)
        if pri in class_names:
            class_metrics[class_names[pri]]["generated"] += sender.get("sent", 0)
    for fid, received in recv_flows.items():
        pri = flow_priority.get(str(fid))
        if pri in class_names:
            class_metrics[class_names[pri]]["delivered"] += received.get("recv", 0)
    for values in class_metrics.values():
        if values["generated"]:
            values["pdr"] = round(values["delivered"] / values["generated"], 4)

    summary = {
        "generated": sent_total,
        "delivered": recv_total,
        "packet_delivery_ratio": round(pdr, 4),
        "throughput_pps": round(thr_pps, 2),
        "throughput_mbps": round(thr_mbps, 3),
        "end_to_end_latency_ms": round(latency_ms, 3),
        "avg_queue_occupancy": round(occ_avg, 4),
        "drop_count": drops,
        "endpoint_loss_count": endpoint_loss,
        "congestion_state_transitions": metrics.transitions,
        "fairness_jain": round(fairness, 4),
        "compression_ratio": round(real_compression_ratio, 3),
        "state_time_s": {k: round(v, 2) for k, v in metrics._state_time.items()},
        "class_metrics": class_metrics,
    }

    print("-" * 70)
    print("  RESULTS (Metrics Engine)")
    print("-" * 70)
    print(f"  Packets generated .............. {summary['generated']}")
    print(f"  Packets delivered .............. {summary['delivered']}")
    print(f"  Packet Delivery Ratio .......... {summary['packet_delivery_ratio']*100:.2f}%")
    print(f"  Throughput ..................... {summary['throughput_pps']:.1f} pkt/s "
          f"({summary['throughput_mbps']:.3f} Mbit/s)")
    print(f"  End-to-end latency ............. {summary['end_to_end_latency_ms']:.2f} ms")
    print(f"  Avg queue occupancy ............ {summary['avg_queue_occupancy']*100:.1f}%")
    print(f"  Netem queue drops (non-dup.) ... {summary['drop_count']}")
    print(f"  End-to-end losses .............. {summary['endpoint_loss_count']}")
    print(f"  Congestion state transitions ... {summary['congestion_state_transitions']}")
    print(f"  Fairness (Jain) ................ {summary['fairness_jain']:.3f}")
    print(f"  Compression ratio (real, bytes)  {summary['compression_ratio']:.3f}x")
    st_line = "  ".join(f"{k[:4]}={v:.1f}s" for k, v in summary["state_time_s"].items() if v > 0)
    if st_line:
        print(f"  Time per state ................. {st_line}")
    print("  PDR per class:")
    for name, values in summary["class_metrics"].items():
        if values["generated"]:
            print(f"    {name:<10} {values['pdr']*100:6.2f}%  "
                  f"({values['delivered']}/{values['generated']})")
    print("-" * 70)
    return summary
