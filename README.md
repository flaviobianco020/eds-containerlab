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

### Cosa è riprodotto a livello di rete

| Logica del simulatore | Resa in ContainerLab |
|---|---|
| Topologie, capacità, ritardi | struttura `veth` + `tbf`/`netem` |
| Collo di bottiglia | `tbf rate 10mbit` |
| Coda finita `QueueManager(max_size=20)` → drop-tail | `netem limit 20` come qdisc figlia del `tbf` (drop reali) |
| 6 scenari di `examples/scenarios.py` | `scenarios.sh` (eventi a tempo) |
| Classi/priorità (control/telemetry/video) | scenario 6: `htb` a 3 classi + filtri DSCP |
| Macchina a stati di congestione / compressione | **non** riprodotta (è logica applicativa del simulatore, non di rete) |

## Prerequisiti

- [Docker](https://docs.docker.com/engine/install/)
- [ContainerLab](https://containerlab.dev/install/) (comando `containerlab`)
- Immagine con strumenti di rete `ghcr.io/srl-labs/network-multitool` (contiene
  `ip`, `tc`, `iperf3`, `ping`, ...); viene scaricata al primo deploy.

## Struttura del repository

```
.
├── topologies/
│   ├── single_bottleneck.clab.yml
│   ├── multi_hop.clab.yml
│   └── mesh.clab.yml
├── deploy.sh            # deploy + configurazione (IP, routing, tc, drop-tail)
├── run_simulation.sh    # test rapido per topologia (iperf3 / ping)
├── scenarios.sh         # 6 scenari di congestione EDS (single_bottleneck)
├── .vscode/
│   └── extensions.json  # estensioni VS Code consigliate
└── README.md
```

## Uso rapido

```bash
chmod +x deploy.sh run_simulation.sh scenarios.sh

# Deploy + configurazione di una topologia
./deploy.sh single_bottleneck      # oppure: multi_hop | mesh

# Test rapido (latenza + throughput)
./run_simulation.sh single_bottleneck

# Scenari di congestione EDS 1-6 (solo single_bottleneck)
./scenarios.sh 1                   # ... fino a 6

# Smontaggio del lab
./deploy.sh single_bottleneck destroy
```

La dimensione della coda drop-tail è configurabile (default 20 pacchetti):

```bash
QUEUE_LIMIT=30 ./deploy.sh single_bottleneck   # come lo scenario 6 del sim
```

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

## Verifica attesa

- `single_bottleneck`: flussi `iperf3` simultanei verso `dst` condividono circa
  **10 Mbps** complessivi; in overload UDP compaiono i drop (coda da 20).
- `multi_hop`: throughput `n0 → n3` limitato a ~10 Mbps e RTT crescente con il
  numero di hop (3 × ~5 ms per direzione).
- `mesh`: throughput `n00 → n12` ~10 Mbps lungo il percorso configurato.
- `scenarios.sh`: i drop/throughput cambiano in corrispondenza degli eventi a
  tempo (degrado banda, link down/up, surge, priorità).
