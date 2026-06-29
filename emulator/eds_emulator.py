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

AGENT_PATH      = "/opt/eds/eds_node.py"
COMPRESSOR_PATH = "/opt/eds/eds_compressor.py"
COMP_STATE_FILE = "/tmp/eds_comp_state"
NFQUEUE_NUM     = 1

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

# Fase 2 (eFRAC paper §3.3) — identici a simulator/network/congestion.py
PHASE2_EWMA_ALPHA: float = 0.125           # Jacobson/Karn α
PHASE2_ESCALATION_DEBOUNCE: float = 1.5   # secondi di eccedenza sostenuta prima di salire
PHASE2_DEESCALATION_COOLDOWN: float = 4.5 # secondi sotto soglia prima di scendere (3:1)


class CongestionStateMachine:
    """
    Fase 1 (default): transizioni istantanee, nessuno smoothing — backward-compatible.
    Fase 2 (enable_phase2=True): EWMA α=1/8 + hysteresis asimmetrica (eFRAC §3.3).

    Identica a simulator/network/congestion.py.
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
        Fase 1 (alpha=1.0, debounce=0.0): salto istantaneo al target, nessuno smoothing.
        Fase 2: EWMA + un passo alla volta con debounce/cooldown asimmetrici.
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


# Ratio attese per stato (media pesata sui tre traffic class del simulatore).
# Usate per stimare il compression_ratio nelle metriche quando enable_phase2=True.
# Fonte: simulator/control/compressor.py _RATIOS, media (pri=0, pri=1, pri=2).
#   NORMAL        : 1.00
#   HC            : media(0.760,0.904,0.983) = 0.882 → ratio=1/0.882≈1.13
#   DELTA         : media(0.550,0.500,0.667) = 0.572 → ratio=1/0.572≈1.75
#   INCREMENTAL   : media(0.500,0.250,0.667) = 0.472 → ratio=1/0.472≈2.12
#   DROP          : solo CONTROL sopravvive → ratio conservativa ≈ 1.0
_EXPECTED_COMPRESSION_RATIO: dict[str, float] = {
    "NORMAL":                  1.00,
    "HEADER_COMPRESSION":      1.13,
    "DELTA_COMPRESSION":       1.75,
    "INCREMENTAL_COMPRESSION": 2.12,
    "DROP_LOW_PRIORITY":       1.00,
}

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
        self._comp_rule = None   # spec esatta della regola iptables NFQUEUE

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

    # --- Pulizia stato residuo da run precedenti ----------------------------

    def cleanup_stale(self, port: int = 5000):
        """
        Rimuove eventuali artefatti lasciati da run precedenti interrotti:
          - uccide il processo compressore ancora in vita (causa ENOBUFS sul kernel)
          - svuota la catena FORWARD (regole NFQUEUE o DROP residue)
          - rimuove i filtri tc DROP_LOW_PRIORITY

        Chiamata SEMPRE all'inizio di run_emulation, indipendentemente dalla modalita'.
        Questo evita che una run Phase 2 interrotta blocchi le run Phase 1 successive
        a causa di una regola NFQUEUE con queue piena (pacchetti droppati da kernel).
        """
        node, iface = self.topo.bottleneck_node, self.topo.bottleneck_if
        self.sh(node,
                "pid=$(cat /tmp/eds_comp.pid 2>/dev/null); "
                "[ -n \"$pid\" ] && kill \"$pid\" 2>/dev/null; true")
        self.sh(node, "iptables -F FORWARD 2>/dev/null || true")
        self.apply_drop_low_priority(False)

    # --- Fase 2: compressore NFQUEUE lato router ----------------------------

    def install_compressor_deps(self):
        """
        Installa libnetfilter_queue + NetfilterQueue Python nel container router.
        Chiamata una volta sola all'avvio dello scenario con enable_phase2=True.
        """
        node = self.topo.bottleneck_node
        print(f"  [compressor] installazione dipendenze in {node} ...")
        # Alpine: toolchain di build + header dev + runtime libs + pip
        self.sh(node,
                "apk add --no-cache gcc musl-dev python3-dev py3-pip linux-headers "
                "libnetfilter_queue libnetfilter_queue-dev libmnl libmnl-dev 2>&1 | tail -1",
                timeout=180.0)
        # NetfilterQueue (estensione C, compilata da sorgente)
        r = self.sh(node,
                    "python3 -m pip install --break-system-packages -q NetfilterQueue 2>&1 || "
                    "python3 -m pip install -q NetfilterQueue 2>&1",
                    timeout=180.0)
        last = (r.stdout or "").strip().splitlines()
        if last:
            print(f"      [pip] {last[-1]}")
        # verifica che l'import funzioni davvero (fallisce subito se manca qualcosa)
        chk = self.sh(node, "python3 -c 'import netfilterqueue' 2>&1")
        if chk.returncode != 0:
            raise RuntimeError("NetfilterQueue non importabile nel router: "
                               + (chk.stdout or "").strip())
        print("      [compressor] NetfilterQueue pronto")

    def start_compressor(self, port: int = 5000):
        """
        Aggiunge la regola iptables NFQUEUE sul link bottleneck e avvia
        eds_compressor.py in background nel container router.

        La regola intercetta solo UDP verso la porta del ricevitore (EDS_PORT)
        in transito sull'interfaccia di uscita del collo di bottiglia.
        --queue-bypass: se il processo crasha, i pacchetti passano non compressi
        invece di essere scartati (fail-open per robustezza).
        """
        node, iface = self.topo.bottleneck_node, self.topo.bottleneck_if
        # Stato iniziale = 0 (NORMAL)
        self.sh(node, f"echo 0 > {COMP_STATE_FILE}")
        # Solo pacchetti >= 500 byte (IP totale) passano per NFQUEUE.
        # I pacchetti CONTROL (100 B payload = 128 B IP) passano direttamente:
        # riduce il carico sul processo Python userspace di ~90%.
        self._comp_rule = (f"FORWARD -o {iface} -p udp --dport {port} "
                           f"-m length --length 500:65535 "
                           f"-j NFQUEUE --queue-num {NFQUEUE_NUM} --queue-bypass")
        # rimuove eventuali regole residue da run precedenti, poi aggiunge
        self.sh(node, f"iptables -D {self._comp_rule} 2>/dev/null || true")
        self.sh(node, f"iptables -A {self._comp_rule}")
        # Avvia compressore in background, log in /tmp/eds_comp.log
        self.sh(node,
                f"python3 {COMPRESSOR_PATH} {NFQUEUE_NUM} "
                f"> /tmp/eds_comp.log 2>&1 & echo $! > /tmp/eds_comp.pid")
        time.sleep(0.8)  # attendi avvio processo
        if self.verbose:
            r = self.sh(node, "cat /tmp/eds_comp.log")
            print(f"      [compressor] {(r.stdout or '').strip()}")

    def update_compression_state(self, state_value: int):
        """
        Scrive il valore di stato (0-4) nel file letto dal compressore.
        Chiamata dal controller tick ad ogni transizione di stato.
        """
        self.sh(self.topo.bottleneck_node,
                f"echo {state_value} > {COMP_STATE_FILE}")

    def stop_compressor(self):
        """Rimuove la regola iptables (match esatto) e termina il compressore."""
        node = self.topo.bottleneck_node
        if self._comp_rule:
            self.sh(node, f"iptables -D {self._comp_rule} 2>/dev/null || true")
        self.sh(node,
                "pid=$(cat /tmp/eds_comp.pid 2>/dev/null); "
                "[ -n \"$pid\" ] && kill $pid 2>/dev/null || true")
        if self.verbose:
            print("      [compressor] fermato, regola iptables rimossa")

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
        # tracking tempo per stato (Fase 2: stima compression_ratio)
        self._state_time: dict[str, float] = {s.name: 0.0 for s in CongestionState}
        self._state_enter_t: float = 0.0
        self._last_state: str = CongestionState.NORMAL.name

    def record_state_time(self, new_state_name: str, now: float) -> None:
        """Chiude il timer dello stato precedente, apre quello del nuovo."""
        self._state_time[self._last_state] += now - self._state_enter_t
        self._state_enter_t = now
        self._last_state = new_state_name

    def close_state_time(self, end_t: float) -> None:
        """Chiude il timer dello stato corrente alla fine della simulazione."""
        self._state_time[self._last_state] += end_t - self._state_enter_t

    def compression_ratio(self) -> float:
        """Stima il compression_ratio come media pesata sul tempo per stato."""
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


# --------------------------------- Runner -----------------------------------
def run_emulation(topo_key: str, flows: list[FlowSpec], events: list[tuple],
                  end_time: float, metric_interval: float = 10.0,
                  tick: float = 0.5, seed: int = 42, port: int = 5000,
                  title: str = "", queue_limit: Optional[int] = None,
                  enable_phase2: bool = False) -> dict:
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
    net.cleanup_stale(port=port)
    if queue_limit is not None:
        net.set_queue_limit(queue_limit)
    if enable_phase2:
        net.install_compressor_deps()
        net.start_compressor(port=port)

    if enable_phase2:
        sm = CongestionStateMachine(
            ewma_alpha=PHASE2_EWMA_ALPHA,
            escalation_debounce=PHASE2_ESCALATION_DEBOUNCE,
            deescalation_cooldown=PHASE2_DEESCALATION_COOLDOWN,
        )
        print(f"  Modalità: FASE 2  (EWMA α={PHASE2_EWMA_ALPHA}, "
              f"escalation={PHASE2_ESCALATION_DEBOUNCE}s, "
              f"cooldown={PHASE2_DEESCALATION_COOLDOWN}s)")
    else:
        sm = CongestionStateMachine()
        print("  Modalità: FASE 1  (transizioni istantanee)")
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
        now = sched.now()
        st = net.qdisc_stats()
        changed = sm.update(st["occupancy"], sim_time=now)
        if changed:
            metrics.transitions += 1
            metrics.record_state_time(sm.current_state.name, now)
            print(f"  [t={now:5.1f}] STATO -> {sm.current_state.name}  "
                  f"(EWMA occ={sm.ewma_occupancy*100:.1f}%)")
            if enable_phase2:
                net.update_compression_state(sm.current_state.value)
        net.apply_drop_low_priority(sm.current_state == CongestionState.DROP_LOW_PRIORITY)
        nt = now + tick
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
        # drop relativi all'inizio della run (i contatori tc sono cumulativi dal deploy)
        drops_run = st["dropped"] - stats0["dropped"]
        print(f"  [t={t:5.1f}] METRIC  occ={st['occupancy']*100:5.1f}%  "
              f"stato={sm.current_state.name:<22}  thr={max(thr,0.0):7.1f} pkt/s  "
              f"drop={drops_run}")
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
    # allinea il riferimento del throughput allo stato iniziale (contatori tc cumulativi)
    state_prev["sent"] = stats0["sent_pkts"]
    metrics._state_enter_t = 0.0  # il run inizia a t=0
    sched.run(end_time)

    # --- chiusura: attende sender/ricevitore -------------------------------
    print("  ... attendo la chiusura di sender e ricevitore ...")
    for th in threads:
        th.join(timeout=40)
    if enable_phase2:
        net.stop_compressor()
    net.apply_drop_low_priority(False)  # ripulisce i filtri tc
    stats1 = net.qdisc_stats()
    metrics.close_state_time(end_time)

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
    occ_avg = (sum(s[1] for s in metrics.samples) / len(metrics.samples)
               if metrics.samples else 0.0)

    # Compression ratio reale da byte misurati.
    # avg_orig_size = dimensione originale media per pacchetto (lato sender, pre-compressione).
    # bytes_total = byte effettivamente ricevuti (post-compressione sul link bottleneck).
    # ratio = (orig_per_pkt × pkt_consegnati) / byte_ricevuti  →  > 1.0 se compressione attiva.
    avg_orig_size = bytes_sent_total / sent_total if sent_total else 0.0
    if bytes_total > 0 and avg_orig_size > 0:
        real_compression_ratio = (avg_orig_size * recv_total) / bytes_total
    else:
        real_compression_ratio = metrics.compression_ratio()

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
        "compression_ratio": round(real_compression_ratio, 3),
        "state_time_s": {k: round(v, 2) for k, v in metrics._state_time.items()},
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
    print(f"  Compression ratio (reale, byte) . {summary['compression_ratio']:.3f}x")
    st_line = "  ".join(f"{k[:4]}={v:.1f}s" for k, v in summary["state_time_s"].items() if v > 0)
    if st_line:
        print(f"  Tempo per stato ................. {st_line}")
    print("-" * 70)
    return summary
