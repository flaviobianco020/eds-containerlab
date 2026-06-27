# EDS ContainerLab

Topologie [ContainerLab](https://containerlab.dev) per il progetto
**[Event-Driven Simulator](https://github.com/flaviobianco020/Event-Driven_Simulator)** (EDS).

Queste topologie riproducono in un **ambiente di rete reale** (container Linux
collegati da coppie `veth`, con shaping del traffico tramite `tc`) le tre
topologie definite nel simulatore EDS in
[`simulator/network/topology.py`](https://github.com/flaviobianco020/Event-Driven_Simulator/blob/main/simulator/network/topology.py),
e gli scenari di congestione di
[`examples/scenarios.py`](https://github.com/flaviobianco020/Event-Driven_Simulator/blob/main/examples/scenarios.py).

| Topologia | Simulatore EDS | ContainerLab |
|-----------|----------------|--------------|
| `single_bottleneck` | `NetworkTopology.single_bottleneck()` — N sorgenti → router → dst | `src0`, `src1`, `src2` → `router` → `dst` |
| `multi_hop` | `NetworkTopology.multi_hop()` — catena lineare | `n0 → n1 → n2 → n3` (3 hop) |
| `mesh` | `NetworkTopology.mesh()` — griglia di nodi | griglia 2×3 (`n00 … n12`) |

> **Mappatura delle capacità.** Nel simulatore le capacità dei link sono
> espresse in *pacchetti per unità di tempo*. Qui il collo di bottiglia
> (`capacity = 10`) è mappato su **10 Mbps**, mentre i link di accesso ad alta
> capacità (`capacity = 1000`) non vengono limitati in banda: ricevono solo un
> ritardo via `netem`, così da non introdurre un collo di bottiglia artificiale.

### Parità con la Fase 1 del simulatore

Oltre alle topologie, l'emulatore replica **in tempo reale** i componenti della
Fase 1 (PDF §4) tramite un control-plane Python (cartella `emulator/`) e un
agente di traffico nei container (`agent/eds_node.py`):

| Componente Fase 1 (§4) | Nel simulatore | Nell'emulatore |
|---|---|---|
| Network Topology + capacità/ritardi | `network/topology.py` | `deploy.sh` (`veth` + `tbf`/`netem`) |
| Queue Manager (coda finita, drop) | `queue_manager.py` (`max_size=20`) | `netem limit 20` (drop-tail reale), letta da `tc -s qdisc` |
| Traffic Generator (CBR/Poisson/Bursty/Video/Control/Telemetry) | `traffic/flow.py`, `generator.py` | `agent/eds_node.py` (UDP, stesse formule di inter-arrivo) |
| Event Scheduler | `scheduler.py` | `RTScheduler` (heap su wall-clock reale) |
| **Congestion State Machine** (soglie 0.50/0.70/0.85/0.95) | `congestion.py` | `eds_emulator.py` legge l'occupancy reale e applica `DROP_LOW_PRIORITY` via `tc` |
| Metrics Engine (throughput/PDR/latenza/occupancy/drop/transizioni/fairness) | `metrics.py` | `eds_emulator.py` (misure reali da agente + `tc`) |
| 6 scenari (§4.5) | `examples/scenarios.py` | `emulator/scenarios.py` (stessi parametri ed eventi) |

> **Compressione/stati intermedi.** Nella Fase 1 gli stati `HEADER/DELTA/INCREMENTAL_COMPRESSION`
> non alterano il traffico (vedi `core.py`): l'unica azione effettiva è
> `DROP_LOW_PRIORITY`. L'emulatore riproduce esattamente questo comportamento e
> conta le transizioni di stato; il `compression_ratio` resta `1.0` come nel
> simulatore. La compressione *semantica* (Fase 1 §5.1) è logica applicativa e
> non è in scopo per l'emulatore di rete.

> **Mappatura dei rate.** I rate dei flussi del simulatore (pkt/s) sono mappati
> 1:1 in Mbit/s (capacità collo di bottiglia 10 → 10 Mbit/s), così i rapporti
> carico/capacità di ogni scenario restano identici (es. scenario 1: 8+5 vs 10).

## Prerequisiti

- [Docker](https://docs.docker.com/engine/install/)
- [ContainerLab](https://containerlab.dev/install/) (comando `containerlab`)
- Immagine con strumenti di rete `ghcr.io/srl-labs/network-multitool` (contiene
  `ip`, `tc`, `iperf3`, `ping`, **`python3`**); viene scaricata al primo deploy.
- `python3` sull'host (per il control-plane dell'emulatore — solo stdlib).

## Struttura del repository

```
.
├── topologies/
│   ├── single_bottleneck.clab.yml   # include il bind ../agent:/opt/eds
│   ├── multi_hop.clab.yml
│   └── mesh.clab.yml
├── deploy.sh            # deploy + rete: IP, routing, tc (Topology + Queue Manager)
├── agent/
│   └── eds_node.py      # Traffic Generator UDP in-container (FlowModel del simulatore)
├── emulator/
│   ├── eds_emulator.py  # control-plane: scheduler + state machine + metrics engine
│   └── scenarios.py     # i 6 scenari della Fase 1 (§4.5)
├── run_simulation.sh    # test rapido per topologia (iperf3 / ping)
├── scenarios.sh         # variante "leggera" in bash dei 6 scenari
├── .vscode/
│   └── extensions.json  # estensioni VS Code consigliate
└── README.md
```

## Uso rapido

```bash
chmod +x deploy.sh run_simulation.sh scenarios.sh

# 1) Deploy della rete (Topology + Queue Manager via tc)
./deploy.sh single_bottleneck          # oppure: multi_hop | mesh

# 2) Emulatore Fase 1: esegue uno dei 6 scenari in tempo reale
python3 emulator/scenarios.py 1        # ... fino a 6
python3 emulator/scenarios.py 3 --scale 0.5   # tempi dimezzati per demo rapide

# (in alternativa) test di rete rapido o scenari bash
./run_simulation.sh single_bottleneck
./scenarios.sh 1

# Smontaggio del lab
./deploy.sh single_bottleneck destroy
```

Lo scenario 6 usa una coda da 30 pacchetti: il control-plane la imposta da solo
via `tc`. La dimensione di default (20) è configurabile anche al deploy:

```bash
QUEUE_LIMIT=30 ./deploy.sh single_bottleneck
```

---

## Emulatore Fase 1 (`emulator/`)

`emulator/scenarios.py` orchestra uno scenario completo sulla topologia
`single_bottleneck` (come `examples/scenarios.py` del simulatore). Per ogni run:

1. avvia il **ricevitore** UDP sul nodo `dst` (`agent/eds_node.py recv`);
2. uno **scheduler real-time** fa partire i flussi (`FLOW_START/STOP`), gli
   eventi di rete (`LINK_RATE_CHANGE`, `LINK_FAILURE/RECOVERY`) e i campioni
   periodici (`METRIC_SAMPLE`) ai tempi previsti dallo scenario;
3. i **generatori di traffico** (`eds_node.py send`) inviano UDP riproducendo i
   `FlowModel` del simulatore e marcano i pacchetti con DSCP per priorità
   (control `CS6`, telemetry `CS2`, video `AF11`);
4. un **controller** legge ogni 0.5 s l'occupancy reale della coda da
   `tc -s qdisc` e fa girare la **Congestion State Machine** (soglie identiche
   0.50/0.70/0.85/0.95); in stato `DROP_LOW_PRIORITY` installa filtri `tc` che
   scartano il traffico a priorità > 0 (come il simulatore);
5. a fine run stampa le **metriche** (throughput, PDR, latenza end-to-end,
   occupancy media, drop, transizioni di stato, fairness di Jain).

Output di esempio (per riga di `METRIC_SAMPLE` e riepilogo finale):

```
  [t= 30.0] METRIC  occ= 96.0%  stato=DROP_LOW_PRIORITY      thr=  812.0 pkt/s  drop_tot=134
  ...
  Packet Delivery Ratio ........... 78.41%
  Transizioni stato congestione ... 4
  Fairness (Jain) ................. 0.812
```

> **Nota di stato.** Il control-plane è stato verificato a livello di sintassi
> (`py_compile`, `bash -n`) ma **non ancora eseguito su un lab reale**. Alcuni
> dettagli `tc` (filtri `action drop` su `tbf`, persistenza dopo `ip link down/up`)
> vanno confermati al primo deploy: vedi la sezione *Verifica* in fondo.

---

## Topologie e indirizzamento

### `single_bottleneck`

Tre sorgenti raggiungono `dst` attraverso un unico `router`. I link di accesso
(`src* → router`) sono ad alta capacità; il link `router → dst` è il collo di
bottiglia da 10 Mbps con coda da 20 pacchetti.

```
src0 ──┐
src1 ──┼── router ════(10 Mbps, coda 20)════ dst
src2 ──┘
```

| Link | Subnet | Indirizzi |
|------|--------|-----------|
| `src0 ↔ router` (accesso) | `10.0.10.0/24` | `src0=.1`, `router=.254` |
| `src1 ↔ router` (accesso) | `10.0.20.0/24` | `src1=.1`, `router=.254` |
| `src2 ↔ router` (accesso) | `10.0.40.0/24` | `src2=.1`, `router=.254` |
| `router ↔ dst` (collo di bottiglia) | `10.0.30.0/24` | `router=.254`, `dst=.1` |

### `multi_hop`

Catena lineare di 4 nodi (3 hop), ogni link da 10 Mbps (coda 20).

```
n0 ══(10 Mbps)══ n1 ══(10 Mbps)══ n2 ══(10 Mbps)══ n3
```

| Link | Subnet | Indirizzi |
|------|--------|-----------|
| `n0 ↔ n1` | `10.0.1.0/24` | `n0=.1`, `n1=.2` |
| `n1 ↔ n2` | `10.0.2.0/24` | `n1=.1`, `n2=.2` |
| `n2 ↔ n3` | `10.0.3.0/24` | `n2=.1`, `n3=.2` |

### `mesh`

Griglia 2×3 con link bidirezionali, ognuno da 10 Mbps (coda 20).

```
n00 ── n01 ── n02
 │      │      │
n10 ── n11 ── n12
```

| Link | Subnet | | Link | Subnet |
|------|--------|---|------|--------|
| `n00 ↔ n01` | `10.1.1.0/24` | | `n00 ↔ n10` | `10.1.5.0/24` |
| `n01 ↔ n02` | `10.1.2.0/24` | | `n01 ↔ n11` | `10.1.6.0/24` |
| `n10 ↔ n11` | `10.1.3.0/24` | | `n02 ↔ n12` | `10.1.7.0/24` |
| `n11 ↔ n12` | `10.1.4.0/24` | | | |

Il routing della mesh è configurato per il percorso dimostrativo **angolo →
angolo** `n00 (10.1.1.1) ↔ n12 (10.1.7.2)` lungo `n00 → n01 → n02 → n12`. Il
forwarding IP è abilitato su tutti i nodi, quindi è possibile aggiungere altre
route per percorsi diversi.

---

## Scenari di congestione (`scenarios.sh`)

Riproducono i 6 scenari di `examples/scenarios.py` sulla topologia
`single_bottleneck` (vanno eseguiti dopo `./deploy.sh single_bottleneck`). Il
traffico è UDP (`iperf3 -u`), così i drop dovuti alla coda piena sono visibili
nel report come datagrammi persi — l'equivalente di `drop_count` nel simulatore.

| # | Scenario | Eventi a tempo riprodotti |
|---|----------|---------------------------|
| 1 | `single_bottleneck` | overload costante (load 13 > cap 10) |
| 2 | `flash_crowd` | flusso surge extra da t=20 a t=50 |
| 3 | `bandwidth_degradation` | `tc change` banda 10→4 a t=30, 10 a t=60 |
| 4 | `link_failure_recovery` | `ip link down` a t=30, `up` a t=55 |
| 5 | `persistent_overload` | 3 flussi, overload sostenuto (load 15) |
| 6 | `mixed_telemetry_video` | 3 classi con priorità (HTB + DSCP) |

> La variante bash `scenarios.sh` è una demo rapida con `iperf3`. Per la parità
> fedele con il simulatore (state machine, metriche, FlowModel) usa
> `python3 emulator/scenarios.py <1-6>`.

### Scenario 6 — priorità

Lo scenario 6 riconfigura il collo di bottiglia con una qdisc **HTB** a 3 classi
e filtri per **DSCP**; le sorgenti marcano il traffico con `iperf3 -S`:

| Classe | DSCP | Sorgente | Priorità HTB |
|--------|------|----------|--------------|
| `control`   | CS6 (`0xc0`) | `src2` | `1:10` (prio 0, protetta) |
| `telemetry` | CS2 (`0x40`) | `src1` | `1:20` (prio 1) |
| `video`     | best-effort  | `src0` | `1:30` (prio 2, default) |

Sotto congestione il traffico `control` (priorità più alta) resta protetto,
mentre il `video` subisce la maggior parte dei drop — come la transizione
`DROP_LOW_PRIORITY` della macchina a stati del simulatore. Per tornare al
drop-tail semplice basta rilanciare `./deploy.sh single_bottleneck`.

---

## Dettagli sullo shaping (`tc`)

Lo shaping è applicato da `deploy.sh` dopo il deploy. Due casi:

- **Link da 10 Mbps (colli di bottiglia).** `tbf` con il **fix del burst** (un
  `burst` troppo piccolo impedisce di raggiungere il rate nominale, quindi
  `burst 1mbit` ≈ 125 kB), e `netem` come qdisc figlia per il ritardo e per la
  **coda finita** `limit 20` (drop-tail). Il `tbf` ha un `limit` ampio così i
  drop avvengono nel `netem`, a numero di pacchetti, esattamente come in
  `QueueManager.enqueue()`:

  ```
  tc qdisc replace dev <if> root   handle 1:  tbf   rate 10mbit burst 1mbit limit 1m
  tc qdisc replace dev <if> parent 1:1 handle 10: netem delay 5ms limit 20
  ```

- **Link di accesso ad alta capacità.** Nessun `tbf` (introdurrebbe un collo di
  bottiglia artificiale): solo `netem` per il ritardo.

  ```
  tc qdisc replace dev <if> root netem delay 1ms
  ```

### Routing

Le route sono **specifiche per subnet** e vengono aggiunte con
`ip route replace <subnet> via <gateway>`. **Il default gateway non viene
toccato**: resta quello assegnato da ContainerLab/Docker per la rete di
management del lab.

---

## Verifica

Dopo `./deploy.sh single_bottleneck`:

- **Rete di base** (`scenarios.sh` / `run_simulation.sh`): flussi `iperf3` verso
  `dst` condividono ~**10 Mbps**; in overload UDP compaiono i drop (coda 20).
- **Emulatore Fase 1** (`python3 emulator/scenarios.py <1-6>`): le righe
  `METRIC_SAMPLE` mostrano occupancy e stato della congestione che salgono con
  il carico; in `DROP_LOW_PRIORITY` il `control` resta consegnato mentre
  video/telemetry vengono scartati.
- `multi_hop`: throughput `n0 → n3` ~10 Mbps, RTT crescente con gli hop.
- `mesh`: throughput `n00 → n12` ~10 Mbps lungo il percorso configurato.

Nota: il control-plane Python è validato per sintassi ma non ancora eseguito su
un lab reale; i comandi `tc` (filtri `action drop` su `tbf`, ripristino dopo
`ip link down/up`) vanno confermati al primo deploy.
