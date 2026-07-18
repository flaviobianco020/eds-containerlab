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
import os
import re
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field, replace
from enum import Enum
from typing import Callable, Optional

# L'Actor MAPPO (Fase 3) vive in agent/eds_actor.py: aggiungo la cartella al path
# cosi' il control-plane puo' caricarlo per il deploy della policy appresa.
_AGENT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "agent")
if _AGENT_DIR not in sys.path:
    sys.path.insert(0, _AGENT_DIR)

AGENT_PATH      = "/opt/eds/eds_node.py"
COMPRESSOR_PATH = "/opt/eds/eds_compressor.py"
COMP_STATE_FILE = "/tmp/eds_comp_state"
COMP_STATS_FILE = "/tmp/eds_comp_stats"
NFQUEUE_NUM     = 1

# Lunghezza IP minima (byte) per cui un pacchetto passa dal compressore NFQUEUE.
# Default 500: solo VIDEO (>=~1428B) viene compresso; CONTROL (128B) e TELEMETRY
# (228-328B) bypassano, per non caricare lo userspace. Abbassandola a ~200 anche
# TELEMETRY (dati strutturati, comprime ~4x) entra: leva per il PDR dello
# scenario 5 (bound teorico ->100%), al costo di piu' carico sul compressore.
#   EDS_NFQUEUE_MINLEN=200 python3 emulator/scenarios.py 5 --phase2
NFQUEUE_MINLEN  = int(os.environ.get("EDS_NFQUEUE_MINLEN", "500"))

# --- Fase 3: parametri di osservazione dell'Actor (specchio di simulator/marl/env.py)
MAPPO_DT          = 1.0    # cadenza di decisione della policy: 1 s (== env.DT)
MAPPO_T_MAX_STATE = 30.0   # normalizzazione t_stato/T_max (doc Tabella 7)
MAPPO_CAP_BPS     = 10e6   # capacita' nominale del collo di bottiglia (10 Mbit/s)
MAPPO_EMERGENCY_OCCUPANCY = 0.95
MAPPO_ACTION_NAMES = ("ESCALATE", "MAINTAIN", "DEESCALATE")

# --- Fase 3: guardrail anti-compressione-a-vuoto (deploy-side) --------------
# La policy MAPPO e' addestrata in un simulatore dove la compressione e' NON
# distruttiva: riduce solo il tempo di servizio in coda (simulator/control/
# compressor.py imposta pkt.compressed_size SENZA toccare pkt.size). Li'
# comprimere e' quasi gratis, quindi la policy comprime volentieri. Sull'emulatore
# la compressione e' DISTRUTTIVA (eds_compressor.py tronca il payload) e ogni
# pacchetto paga la latenza del middlebox NFQUEUE: comprimere quando NON stiamo
# perdendo pacchetti butta throughput senza migliorare il PDR (scenario 2).
#
# NB: l'occupancy NON e' un buon segnale per "serve comprimere?": nello scenario 2
# la coda sta stabilmente al 60-75% anche a rete scarica (il flusso CONTROL e'
# ~2500 pkt/s di pacchetti piccoli che tengono una coda "in piedi" ma senza
# traboccare -> drop~0, PDR~100%). Il segnale corretto e' la PRESSIONE DI PERDITA:
# si permette l'ESCALATE solo se stiamo davvero scartando pacchetti (drop_rate
# oltre soglia) o la coda e' in emergenza. Cosi' scenario 2 (drop~0) non comprime
# mai, scenario 1/5 (overload, drop pesante) comprimono come prima.
#
# Disattivabile per confronto A/B con EDS_MAPPO_GATE=0.
MAPPO_COMPRESSION_GATE = os.environ.get("EDS_MAPPO_GATE", "1") != "0"
MAPPO_GATE_DROP_RATE = 0.01   # frazione di arrivi persi oltre cui l'ESCALATE e' giustificato
MAPPO_TRACE_FEATURES = (
    "ewma_occupancy", "congestion_state", "high_priority_ratio",
    "low_priority_ratio", "drop_rate", "link_utilisation", "time_in_state",
)

_QDISC_SENT_RE = re.compile(r"Sent (\d+) bytes (\d+) pkt")
_QDISC_DROP_RE = re.compile(r"dropped (\d+)")
_QDISC_BACKLOG_RE = re.compile(r"backlog \S+ (\d+)p")


def parse_qdisc_stats(output: str, queue_limit: int,
                      queue_handle: str = "10:") -> dict:
    """Estrae contatori senza sommare qdisc padre e figlia.

    Il TBF root e la netem figlia espongono spesso lo stesso drop. Il traffico
    trasmesso viene letto dalla root, mentre drop e backlog provengono dalla
    netem che implementa la coda finita.
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
    """Vincolo di deploy identico al simulatore robusto, piu' il guardrail
    anti-compressione-a-vuoto (vedi MAPPO_COMPRESSION_GATE).

    Ordine azioni: [ESCALATE, MAINTAIN, DEESCALATE].
    """
    dwell_ok = min_state_dwell <= 0.0 or now - last_transition >= min_state_dwell
    if not dwell_ok:
        # Congelamento durante il dwell (come nel simulatore robusto), con
        # override di emergenza che lascia salire se la coda e' quasi piena.
        if occupancy >= MAPPO_EMERGENCY_OCCUPANCY:
            return [True, True, False]
        return [False, True, False]
    # Dwell soddisfatto: azioni libere, MA niente ESCALATE "a vuoto". Comprimere
    # ha senso solo sotto pressione di perdita reale (o emergenza coda); senza,
    # un ESCALATE riduce solo il throughput a PDR gia' pieno.
    escalate_ok = (not MAPPO_COMPRESSION_GATE
                   or drop_rate > MAPPO_GATE_DROP_RATE
                   or occupancy >= MAPPO_EMERGENCY_OCCUPANCY)
    return [escalate_ok, True, True]

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
    def qdisc_stats(self) -> dict:
        """Legge tc -s qdisc sull'interfaccia del collo di bottiglia."""
        r = self.sh(self.topo.bottleneck_node,
                    f"tc -s qdisc show dev {self.topo.bottleneck_if}")
        return parse_qdisc_stats(r.stdout or "", self.topo.queue_limit)

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
        self.sh(node, f"rm -f {COMP_STATS_FILE}")  # azzera i contatori del run precedente
        self._comp_rule = (f"FORWARD -o {iface} -p udp --dport {port} "
                           f"-m length --length {NFQUEUE_MINLEN}:65535 "
                           f"-j NFQUEUE --queue-num {NFQUEUE_NUM} --queue-bypass")
        # rimuove eventuali regole residue da run precedenti, poi aggiunge
        self.sh(node, f"iptables -D {self._comp_rule} 2>/dev/null || true")
        self.sh(node, f"iptables -A {self._comp_rule}")
        # Avvia compressore in background, log in /tmp/eds_comp.log
        self.sh(node,
                f"python3 {COMPRESSOR_PATH} {NFQUEUE_NUM} "
                f"> /tmp/eds_comp.log 2>&1 & echo $! > /tmp/eds_comp.pid")
        time.sleep(0.8)  # attendi avvio processo
        # Health-check: se il processo e' morto all'avvio (es. import fallito dopo
        # un'installazione flaky), rimuovo la regola -> fail-open REALE. Con la
        # regola presente e nessun consumatore, --queue-bypass dovrebbe bastare,
        # ma togliere la regola elimina ogni rischio di black-hole residuo.
        alive = self.sh(node,
                        "pid=$(cat /tmp/eds_comp.pid 2>/dev/null); "
                        "[ -n \"$pid\" ] && kill -0 \"$pid\" 2>/dev/null "
                        "&& echo alive || echo dead")
        log = self.sh(node, "cat /tmp/eds_comp.log")
        if "alive" not in (alive.stdout or ""):
            print("      [compressor] AVVISO: processo non attivo dopo lo start "
                  "-> rimuovo la regola NFQUEUE (fail-open).")
            print(f"      [compressor:log] {(log.stdout or '').strip()}")
            self.sh(node, f"iptables -D {self._comp_rule} 2>/dev/null || true")
            self._comp_rule = None
        else:
            nfq = self.nfqueue_stats()
            bound = "si" if (nfq and nfq.get("peer_portid", 0) != 0) else "NO"
            if self.verbose:
                print(f"      [compressor] {(log.stdout or '').strip()}  "
                      f"(pid vivo, coda bound: {bound})")

    def update_compression_state(self, state_value: int):
        """
        Scrive il valore di stato (0-4) nel file letto dal compressore.
        Chiamata dal controller tick ad ogni transizione di stato.
        """
        self.sh(self.topo.bottleneck_node,
                f"echo {state_value} > {COMP_STATE_FILE}")

    def nfqueue_stats(self) -> Optional[dict]:
        """Legge /proc/net/netfilter/nfnetlink_queue per la coda NFQUEUE_NUM.

        Colonne: queue_num peer_portid queue_total copy_mode copy_range
                 queue_dropped user_dropped id_sequence.

        Diagnostica del middlebox (scenario 5 Fase 2, "run anomalo"):
          * riga assente / peer_portid == 0  -> NESSUN consumatore bound
            (--queue-bypass fa passare i pacchetti: fail-open).
          * peer_portid != 0 + queue_dropped che cresce -> consumatore bound ma
            STUCK: il kernel scarta i pacchetti a coda piena (black-hole). E' il
            caso che --queue-bypass NON copre.
        Ritorna None se la coda non e' bound.
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
        """Legge i contatori scritti da eds_compressor.py in COMP_STATS_FILE:
        pkts (processati) bytes_in bytes_out compressed. Ratio reale sul filo =
        bytes_in/bytes_out. Ritorna None se il file non c'e'."""
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
        """Pacchetti che hanno fatto match sulla regola NFQUEUE (contatore
        iptables). Confrontato con compressor_stats()['pkts'] misura il BYPASS:
        matched - processati = pacchetti passati non compressi (--queue-bypass)."""
        r = self.sh(self.topo.bottleneck_node,
                    "iptables -nvxL FORWARD 2>/dev/null || true")
        for line in (r.stdout or "").splitlines():
            if f"NFQUEUE num {NFQUEUE_NUM}" in line:
                f = line.split()
                if len(f) >= 1 and f[0].isdigit():
                    return int(f[0])   # colonna 'pkts' di iptables -v
        return None

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


# ------------------------ Osservazione per l'Actor MAPPO --------------------
def _offered_priority_ratios(flows: list[FlowSpec], t: float,
                             end_time: float) -> tuple:
    """
    Stima hi_pri_ratio / lo_pri_ratio dal mix di traffico OFFERTO ai tempo t.

    Nel simulatore queste feature (doc Tabella 7) sono la frazione di pacchetti
    CONTROL / VIDEO nella coda; sull'emulatore la composizione della coda non e'
    ispezionabile via `tc`, quindi la si approssima con la frazione di pkt/s
    offerti per priorita' dai flussi attivi. E' l'unico punto di scarto
    sim-to-real dell'osservazione (documentato nel README).
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

    mappo_mode = mappo_ckpt is not None
    actor = None
    min_state_dwell = 0.0
    mappo_trace_rows = []
    if mappo_mode:
        from eds_actor import Actor as MappoActor  # import pigro (solo se serve)
        actor = MappoActor.from_checkpoint(mappo_ckpt)
        min_state_dwell = float(getattr(actor, "meta", {}).get(
            "min_state_dwell", 0.0))

    # La Fase 2 e la Fase 3 usano la stessa infrastruttura di compressione
    # (middlebox NFQUEUE): cambia solo CHI decide lo stato. In Fase 3 e' l'Actor.
    if enable_phase2 or mappo_mode:
        net.install_compressor_deps()
        net.start_compressor(port=port)
        classes = "VIDEO" if NFQUEUE_MINLEN > 328 else "VIDEO+TELEMETRY"
        print(f"  compressore NFQUEUE: soglia min {NFQUEUE_MINLEN}B -> comprime "
              f"{classes}  (EDS_NFQUEUE_MINLEN per cambiarla)")

    if mappo_mode:
        # macchina di stato PASSIVA: aggiorna solo l'EWMA, non transisce da sola;
        # le transizioni le decide la policy (come AgentControlledStateMachine).
        sm = CongestionStateMachine(ewma_alpha=PHASE2_EWMA_ALPHA)
        meta = getattr(actor, "meta", {})
        print(f"  Modalità: FASE 3  (MAPPO — policy appresa pilota la macchina di stato)")
        print(f"  checkpoint: {os.path.basename(mappo_ckpt)}  "
              f"(episodio {meta.get('episode','?')}, "
              f"λ_stab={meta.get('stability_penalty', 0.0)}, "
              f"dwell={min_state_dwell:.1f}s)")
        if MAPPO_COMPRESSION_GATE:
            print(f"  guardrail compressione: ATTIVO  (ESCALATE solo se drop_rate>"
                  f"{MAPPO_GATE_DROP_RATE:.0%} o occ>={MAPPO_EMERGENCY_OCCUPANCY:.0%};"
                  f" EDS_MAPPO_GATE=0 per disattivare)")
        else:
            print("  guardrail compressione: DISATTIVO  (EDS_MAPPO_GATE=0)")
    elif enable_phase2:
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

    # --- controller Fase 1/2 (macchina di stato automatica) ----------------
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

    # --- controller Fase 3 (policy MAPPO pilota la macchina di stato) -------
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

        # EWMA dell'occupancy (α = Fase 2), aggiornata a mano: la macchina resta passiva
        mstate["ewma"] = ((1.0 - PHASE2_EWMA_ALPHA) * mstate["ewma"]
                          + PHASE2_EWMA_ALPHA * st["occupancy"])
        sm._ewma = mstate["ewma"]  # per il logging coerente

        # drop_rate = frazione di arrivi scartati nella finestra (proxy di deltas["drop"]/gen)
        arrived = d_sent + d_drop
        drop_rate = d_drop / arrived if arrived > 0 else 0.0
        link_util = min(d_bytes * 8.0 / (mstate["cap_bps"] * dt), 1.0)
        hi, lo = _offered_priority_ratios(flows, now, end_time)
        t_in_state = min((now - mstate["last_transition"]) / MAPPO_T_MAX_STATE, 1.0)

        obs = [
            min(max(mstate["ewma"], 0.0), 1.0),      # ewma_occ
            sm.current_state.value / 4.0,            # stato / 4
            hi,                                      # hi_pri_ratio (offerto)
            lo,                                      # lo_pri_ratio (offerto)
            min(max(drop_rate, 0.0), 1.0),           # drop rate finestra
            link_util,                               # utilizzo link
            t_in_state,                              # t_stato / T_max
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
        # drop relativi all'inizio della run (i contatori tc sono cumulativi dal deploy)
        drops_run = st["dropped"] - stats0["dropped"]
        print(f"  [t={t:5.1f}] METRIC  occ={st['occupancy']*100:5.1f}%  "
              f"stato={sm.current_state.name:<22}  thr={max(thr,0.0):7.1f} pkt/s  "
              f"drop={drops_run}")
        nt = t + metric_interval
        if nt <= end_time + 1e-6:
            sched.at(nt, _metric_sample, sched)

    # --- costruzione scheduler --------------------------------------------
    def _rate_change(mbit):
        net.set_bottleneck_rate(mbit)
        mstate["cap_bps"] = mbit * 1e6   # tiene link_util coerente dopo un cambio banda

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
    # allinea i riferimenti allo stato iniziale (contatori tc cumulativi dal deploy)
    state_prev["sent"] = stats0["sent_pkts"]
    mstate["sent_bytes"] = stats0["sent_bytes"]
    mstate["sent_pkts"] = stats0["sent_pkts"]
    mstate["dropped"] = stats0["dropped"]
    metrics._state_enter_t = 0.0  # il run inizia a t=0
    sched.run(end_time)

    # --- chiusura: attende sender/ricevitore -------------------------------
    print("  ... attendo la chiusura di sender e ricevitore ...")
    for th in threads:
        th.join(timeout=40)
    if enable_phase2 or mappo_mode:
        # Diagnostica middlebox PRIMA di fermarlo: cattura nel log del run se la
        # coda NFQUEUE ha fatto black-hole (queue_dropped) o non era bound. E' la
        # prova che conferma/smentisce il "run anomalo" dello scenario 5 Fase 2.
        nfq_end = net.nfqueue_stats()
        if nfq_end is None:
            print("  [nfqueue] coda NON bound a fine run (nessun consumatore; "
                  "--queue-bypass attivo, pacchetti passati non compressi).")
        else:
            print(f"  [nfqueue] peer_portid={nfq_end['peer_portid']}  "
                  f"queue_total={nfq_end['queue_total']}  "
                  f"queue_dropped={nfq_end['queue_dropped']}  "
                  f"user_dropped={nfq_end['user_dropped']}")
            if nfq_end["queue_dropped"] > 0:
                print("  [nfqueue] ATTENZIONE: queue_dropped>0 -> il compressore "
                      "non ha retto il rate: pacchetti scartati dal kernel a coda "
                      "piena (black-hole; --queue-bypass NON copre questo caso).")
        # Compressione REALE sul filo + bypass: risponde a "perche' il PDR di
        # scenario 1/5 non sale?". ratio_reale = bytes_in/bytes_out (deep vs no-op);
        # bypass = pacchetti che hanno fatto match ma NON sono stati compressi
        # (--queue-bypass sotto carico) = matched - processati.
        cs = net.compressor_stats()
        matched = net.nfqueue_rule_matched()
        if cs is not None:
            ratio = (cs["bytes_in"] / cs["bytes_out"]) if cs["bytes_out"] else 1.0
            frac_c = (cs["compressed"] / cs["pkts"] * 100.0) if cs["pkts"] else 0.0
            line = (f"  [compressor] processati={cs['pkts']}  ratio_reale_sul_filo="
                    f"{ratio:.3f}x  troncati={frac_c:.0f}%")
            if matched is not None:
                bypass = max(matched - cs["pkts"], 0)
                bp = (bypass / matched * 100.0) if matched else 0.0
                line += f"  matched={matched}  bypass={bypass} ({bp:.0f}%)"
            print(line)
            if matched and (matched - cs["pkts"]) > 0.2 * matched:
                print("  [compressor] NB: bypass elevato -> il compressore userspace "
                      "non regge il rate; molti pacchetti passano NON compressi "
                      "(spiega perche' la compressione non svuota la coda).")
            elif ratio < 1.05:
                print("  [compressor] NB: ratio reale ~1.0 -> il controller NON sta "
                      "raggiungendo/mantenendo uno stato di compressione profonda.")
        log = net.sh(net.topo.bottleneck_node, "cat /tmp/eds_comp.log 2>/dev/null")
        tail = (log.stdout or "").strip().splitlines()[-5:]
        if tail:
            print("  [compressor:log] " + " | ".join(tail))
        net.stop_compressor()
    net.apply_drop_low_priority(False)  # ripulisce i filtri tc
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
        print(f"  traccia MAPPO salvata: {os.path.abspath(mappo_trace_path)}")

    summary = _summarize(topo, flows, results, metrics, stats0, stats1, end_time)
    # Rilevamento "run anomalo": consegna quasi nulla (come lo scenario 5 Fase 2
    # rep 1: throughput 0.06, 0 transizioni). Reso rumoroso nel log invece di
    # sparire come un throughput ~0 medio nel benchmark.
    if summary["generated"] > 100 and summary["packet_delivery_ratio"] < 0.1:
        print("  " + "!" * 66)
        print(f"  RUN ANOMALO: consegnato solo "
              f"{summary['packet_delivery_ratio']*100:.1f}% dei pacchetti generati.")
        if enable_phase2 or mappo_mode:
            print("  Sospetto principale: middlebox NFQUEUE (compressore stuck o "
                  "morto). Vedi [nfqueue] e [compressor:log] qui sopra.")
        else:
            print("  Sospetto principale: ricevitore o forwarding (nessun "
                  "middlebox in questa modalita').")
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

    # KPI per classe, ricavati dagli identificativi di flusso presenti sia nel
    # report sender sia nel report receiver. Non dipendono dai contatori tc.
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
    print("  RISULTATI (Metrics Engine)")
    print("-" * 70)
    print(f"  Pacchetti generati .............. {summary['generated']}")
    print(f"  Pacchetti consegnati ............ {summary['delivered']}")
    print(f"  Packet Delivery Ratio ........... {summary['packet_delivery_ratio']*100:.2f}%")
    print(f"  Throughput ...................... {summary['throughput_pps']:.1f} pkt/s "
          f"({summary['throughput_mbps']:.3f} Mbit/s)")
    print(f"  Latenza end-to-end .............. {summary['end_to_end_latency_ms']:.2f} ms")
    print(f"  Occupancy media coda ............ {summary['avg_queue_occupancy']*100:.1f}%")
    print(f"  Drop coda netem (non duplicati) . {summary['drop_count']}")
    print(f"  Perdite end-to-end .............. {summary['endpoint_loss_count']}")
    print(f"  Transizioni stato congestione ... {summary['congestion_state_transitions']}")
    print(f"  Fairness (Jain) ................. {summary['fairness_jain']:.3f}")
    print(f"  Compression ratio (reale, byte) . {summary['compression_ratio']:.3f}x")
    st_line = "  ".join(f"{k[:4]}={v:.1f}s" for k, v in summary["state_time_s"].items() if v > 0)
    if st_line:
        print(f"  Tempo per stato ................. {st_line}")
    print("  PDR per classe:")
    for name, values in summary["class_metrics"].items():
        if values["generated"]:
            print(f"    {name:<10} {values['pdr']*100:6.2f}%  "
                  f"({values['delivered']}/{values['generated']})")
    print("-" * 70)
    return summary
