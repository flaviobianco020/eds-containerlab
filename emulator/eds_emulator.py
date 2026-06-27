#!/usr/bin/env python3
"""
eds_emulator.py - Control-plane host-side dell'emulatore Event-Driven Simulator.

Porta sull'emulatore ContainerLab i componenti della Fase 1 che nel simulatore
vivono in software, facendoli operare in TEMPO REALE sulla rete vera:

  * Traffic Generator   -> lancia l'agente UDP (agent/eds_node.py) nei container
                           riproducendo i FlowModel/TrafficClass del simulatore.
  * Event Scheduler     -> RTScheduler: heap di eventi su wall-clock reale.
  * Congestion State    -> CongestionStateMachine identica a simulator/network/
    Machine                congestion.py, alimentata dall'occupancy reale letta
                           da `tc -s qdisc`, applica DROP_LOW_PRIORITY via tc.
  * Queue Manager       -> e' la qdisc tbf+netem(limit) creata da deploy.sh;
                           qui la leggiamo (backlog, drop, sent).
  * Metrics Engine      -> throughput, PDR, latenza, occupancy, drop,
                           transizioni di stato, fairness.

NB: la rete (topologia, capacita', code) e' quella deployata da ./deploy.sh.
Questo modulo NON modifica la topologia: la guida e la misura.

Richiede: Docker, un lab gia' deployato, e python3 dentro i container
(immagine ghcr.io/srl-labs/network-multitool) con l'agente montato in /opt/eds.
"""
from __future__ import annotations

import heapq
import json
import re
import subprocess
import threading
import time
from dataclasses import dataclass, field, replace
from enum import Enum
from typing import Callable, Optional

AGENT_PATH = "/opt/eds/eds_node.py"

# ----------------------- Traffic classes / FlowModel ------------------------
# Rispecchiano le classi usate in examples/scenarios.py del simulatore.
# tuple: (size_lo, size_hi, priority, tos)  -- tos = DSCP per la priorita'.
#   priority 0 (control)   -> CS6  0xc0  (protetto)
#   priority 1 (telemetry) -> CS2  0x40
#   priority 2 (video)     -> AF11 0x28
VIDEO = (1400, 1500, 2, 0x28)
TELEMETRY = (200, 300, 1, 0x40)
CONTROL = (100, 100, 0, 0xc0)

# DSCP scartati quando lo stato e' DROP_LOW_PRIORITY (priority > 0)
LOW_PRIORITY_TOS = (0x28, 0x40)


class FlowModel(str, Enum):
    CBR = "cbr"
    POISSON = "poisson"
    BURSTY = "bursty"
    PERIODIC_TELEMETRY = "periodic_telemetry"
    VIDEO = "video"
    CONTROL = "control"


# ----------------------- Congestion State Machine ---------------------------
# Identica a simulator/network/congestion.py.
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


class CongestionStateMachine:
    def __init__(self, thresholds=None):
        self.current_state = CongestionState.NORMAL
        self.thresholds = dict(thresholds or DEFAULT_THRESHOLDS)
        self.transitions = 0

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

    def update(self, occupancy: float) -> bool:
        new_state = self.evaluate(occupancy)
        if new_state != self.current_state:
            self.current_state = new_state
            self.transitions += 1
            return True
        return False


# --------------------------------- Topologie --------------------------------
@dataclass
class Topo:
    key: str
    lab: str               # nome lab ContainerLab (prefisso container clab-<lab>-)
    dst_node: str
    dst_ip: str
    bottleneck_node: str   # nodo su cui leggere/agire la coda
    bottleneck_if: str
    queue_limit: int = 20


TOPOS = {
    "single_bottleneck": Topo("single_bottleneck", "single-bottleneck",
                              "dst", "10.0.30.1", "router", "eth4", 20),
    "multi_hop": Topo("multi_hop", "multi-hop",
                      "n3", "10.0.3.2", "n0", "eth1", 20),
    "mesh": Topo("mesh", "mesh",
                 "n12", "10.1.7.2", "n00", "eth1", 20),
}


# ------------------------------- docker / tc --------------------------------
class Net:
    """Helper per eseguire comandi nei container e leggere/agire su tc."""

    def __init__(self, topo: Topo, verbose: bool = True):
        self.topo = topo
        self.verbose = verbose
        self._drop_active = False

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
            raise RuntimeError(f"Impossibile contattare {self.container(node)}: {e}")
        if r.returncode != 0:
            raise RuntimeError(
                "python3 non trovato nel container. Installa con "
                f"`docker exec {self.container(node)} apk add --no-cache python3` "
                "oppure usa un'immagine con python3.")
        r = self.exec(node, "test", "-f", AGENT_PATH, timeout=10.0)
        if r.returncode != 0:
            raise RuntimeError(
                f"Agente non montato in {AGENT_PATH}. Verifica i `binds` nel file "
                ".clab.yml (../agent:/opt/eds:ro) e ridai il deploy.")

    # --- lettura coda (Queue Manager) ---------------------------------------
    _re_sent = re.compile(r"Sent (\d+) bytes (\d+) pkt")
    _re_drop = re.compile(r"dropped (\d+)")
    _re_backlog = re.compile(r"backlog \S+ (\d+)p")

    def qdisc_stats(self) -> dict:
        """Legge tc -s qdisc sull'interfaccia del collo di bottiglia."""
        r = self.sh(self.topo.bottleneck_node,
                    f"tc -s qdisc show dev {self.topo.bottleneck_if}")
        out = r.stdout or ""
        m = self._re_sent.search(out)
        sent_bytes = int(m.group(1)) if m else 0
        sent_pkts = int(m.group(2)) if m else 0
        dropped = sum(int(x) for x in self._re_drop.findall(out))
        backlogs = [int(x) for x in self._re_backlog.findall(out)]
        backlog_pkts = max(backlogs) if backlogs else 0
        occ = backlog_pkts / self.topo.queue_limit if self.topo.queue_limit else 0.0
        return {"sent_bytes": sent_bytes, "sent_pkts": sent_pkts,
                "dropped": dropped, "backlog_pkts": backlog_pkts,
                "occupancy": min(occ, 1.0)}

    # --- azioni dello scheduler / state machine -----------------------------
    def set_bottleneck_rate(self, rate_mbit: float):
        node, iface = self.topo.bottleneck_node, self.topo.bottleneck_if
        self.sh(node, f"tc qdisc change dev {iface} root handle 1: "
                      f"tbf rate {rate_mbit}mbit burst 1mbit limit 1m")
        if self.verbose:
            print(f"      [tc] {iface}: banda -> {rate_mbit} Mbit/s")

    def set_queue_limit(self, limit_pkts: int, delay: str = "5ms"):
        """Allinea il drop-tail della coda (netem limit) al valore richiesto."""
        node, iface = self.topo.bottleneck_node, self.topo.bottleneck_if
        self.sh(node, f"tc qdisc change dev {iface} parent 1:1 handle 10: "
                      f"netem delay {delay} limit {limit_pkts}")
        if self.verbose:
            print(f"      [tc] {iface}: coda drop-tail -> {limit_pkts} pacchetti")

    def link_down(self):
        self.sh(self.topo.bottleneck_node, f"ip link set dev {self.topo.bottleneck_if} down")
        if self.verbose:
            print("      [link] collo di bottiglia GIU'")

    def link_up(self):
        self.sh(self.topo.bottleneck_node, f"ip link set dev {self.topo.bottleneck_if} up")
        if self.verbose:
            print("      [link] collo di bottiglia SU")

    def apply_drop_low_priority(self, active: bool):
        """Aggiunge/rimuove i filtri tc che scartano il traffico a bassa priorita'."""
        node, iface = self.topo.bottleneck_node, self.topo.bottleneck_if
        if active and not self._drop_active:
            for tos in LOW_PRIORITY_TOS:
                self.sh(node, f"tc filter add dev {iface} parent 1: protocol ip prio 5 "
                              f"u32 match ip tos {hex(tos)} 0xfc action drop")
            self._drop_active = True
            if self.verbose:
                print("      [state] DROP_LOW_PRIORITY attivo (scarto priorita' > 0)")
        elif not active and self._drop_active:
            self.sh(node, f"tc filter del dev {iface} parent 1: prio 5")
            self._drop_active = False
            if self.verbose:
                print("      [state] DROP_LOW_PRIORITY disattivato")


# ------------------------- Scheduler real-time ------------------------------
@dataclass(order=True)
class _SchedItem:
    t: float
    seq: int
    fn: Callable = field(compare=False)
    args: tuple = field(default=(), compare=False)


class RTScheduler:
    """Heap di eventi ordinati per tempo, eseguiti sul wall-clock reale."""

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
            except Exception as e:  # noqa: BLE001 - un evento non deve fermare il run
                print(f"      [scheduler] errore evento: {e}")


# --------------------------------- Flussi -----------------------------------
@dataclass
class FlowSpec:
    fid: int
    src: str                 # nodo sorgente
    model: FlowModel
    mbit: float              # banda obiettivo (== rate pkt/s del simulatore)
    tclass: tuple            # (size_lo, size_hi, priority, tos)
    start: float = 0.0
    stop: Optional[float] = None   # None => fino a fine simulazione

    def pps(self) -> float:
        lo, hi, _pri, _tos = self.tclass
        avg = (lo + hi) / 2.0
        return max(self.mbit * 1e6 / (avg * 8.0), 1.0)


# --------------------------- Metrics Engine ---------------------------------
class Metrics:
    """Equivalente di simulator/metrics.py, ma su misure reali."""

    def __init__(self):
        self.samples = []          # (t, occupancy, state, throughput_pps)
        self.transitions = 0

    def jain(self, values) -> float:
        vals = [v for v in values if v is not None]
        if not vals:
            return 1.0
        s = sum(vals)
        s2 = sum(v * v for v in vals)
        n = len(vals)
        return (s * s) / (n * s2) if s2 > 0 else 1.0


# --------------------------------- Runner -----------------------------------
def run_emulation(topo_key: str, flows: list[FlowSpec], events: list[tuple],
                  end_time: float, metric_interval: float = 10.0,
                  tick: float = 0.5, seed: int = 42, port: int = 5000,
                  title: str = "", queue_limit: Optional[int] = None) -> dict:
    """
    Esegue uno scenario completo:
      - avvia il ricevitore sul nodo destinazione,
      - schedula flussi (FLOW_START/STOP), eventi di rete e i campioni METRIC_SAMPLE,
      - fa girare il controller della congestion state machine in tempo reale,
      - raccoglie e stampa le metriche finali.
    """
    topo = TOPOS[topo_key]
    if queue_limit is not None:
        topo = replace(topo, queue_limit=queue_limit)
    net = Net(topo)
    print("=" * 70)
    if title:
        print(f"  {title}")
    print(f"  Topologia: {topo_key}   destinazione: {topo.dst_node} ({topo.dst_ip})")
    print("=" * 70)
    net.preflight()
    if queue_limit is not None:
        net.set_queue_limit(queue_limit)

    sm = CongestionStateMachine()
    metrics = Metrics()
    results = {"send": [], "recv": None}
    threads: list[threading.Thread] = []

    # --- ricevitore --------------------------------------------------------
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
    time.sleep(1.0)  # lascia salire il ricevitore

    # --- sender per flusso -------------------------------------------------
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

    # --- controller (congestion state machine) -----------------------------
    def _controller_tick(sched: RTScheduler):
        st = net.qdisc_stats()
        changed = sm.update(st["occupancy"])
        if changed:
            metrics.transitions += 1
        net.apply_drop_low_priority(sm.current_state == CongestionState.DROP_LOW_PRIORITY)
        nt = sched.now() + tick
        if nt <= end_time:
            sched.at(nt, _controller_tick, sched)

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
        print(f"  [t={t:5.1f}] METRIC  occ={st['occupancy']*100:5.1f}%  "
              f"stato={sm.current_state.name:<22}  thr={thr:7.1f} pkt/s  "
              f"drop_tot={st['dropped']}")
        nt = t + metric_interval
        if nt <= end_time + 1e-6:
            sched.at(nt, _metric_sample, sched)

    # --- costruzione scheduler --------------------------------------------
    sched = RTScheduler()
    for fs in flows:
        sched.at(fs.start, _start_flow, fs)
    for (et, kind, param) in events:
        if kind == "rate":
            sched.at(et, net.set_bottleneck_rate, param)
        elif kind == "down":
            sched.at(et, net.link_down)
        elif kind == "up":
            sched.at(et, net.link_up)
    sched.at(tick, _controller_tick, sched)
    sched.at(metric_interval, _metric_sample, sched)

    stats0 = net.qdisc_stats()
    sched.run(end_time)

    # --- chiusura: attende sender/ricevitore -------------------------------
    print("  ... attendo la chiusura di sender e ricevitore ...")
    for th in threads:
        th.join(timeout=40)
    net.apply_drop_low_priority(False)  # ripulisce i filtri
    stats1 = net.qdisc_stats()

    return _summarize(topo, flows, results, metrics, stats0, stats1, end_time)


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
    occ_avg = (sum(s[1] for s in metrics.samples) / len(metrics.samples)
               if metrics.samples else 0.0)

    # fairness (Jain) sul throughput per-flusso
    per_flow_thr = {str(fid): 0 for fid in (f.fid for f in flows)}
    for fid_str, f in recv_flows.items():
        per_flow_thr[str(fid_str)] = f.get("recv", 0)
    fairness = metrics.jain(list(per_flow_thr.values()))

    summary = {
        "generated": sent_total,
        "delivered": recv_total,
        "packet_delivery_ratio": round(pdr, 4),
        "throughput_pps": round(thr_pps, 2),
        "throughput_mbps": round(thr_mbps, 3),
        "end_to_end_latency_ms": round(latency_ms, 3),
        "avg_queue_occupancy": round(occ_avg, 4),
        "drop_count": drops,
        "congestion_state_transitions": metrics.transitions,
        "fairness_jain": round(fairness, 4),
        "compression_ratio": 1.0,  # placeholder, come nel simulatore (Fase 1)
    }

    print("-" * 70)
    print("  RISULTATI (Metrics Engine)")
    print("-" * 70)
    print(f"  Pacchetti generati .............. {summary['generated']}")
    print(f"  Pacchetti consegnati ............ {summary['delivered']}")
    print(f"  Packet Delivery Ratio ........... {summary['packet_delivery_ratio']*100:.2f}%")
    print(f"  Throughput ...................... {summary['throughput_pps']:.1f} pkt/s "
          f"({summary['throughput_mbps']:.3f} Mbit/s)")
    print(f"  Latenza end-to-end .............. {summary['end_to_end_latency_ms']:.2f} ms")
    print(f"  Occupancy media coda ............ {summary['avg_queue_occupancy']*100:.1f}%")
    print(f"  Drop totali (coda) .............. {summary['drop_count']}")
    print(f"  Transizioni stato congestione ... {summary['congestion_state_transitions']}")
    print(f"  Fairness (Jain) ................. {summary['fairness_jain']:.3f}")
    print("-" * 70)
    return summary
