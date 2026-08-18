# EDS ContainerLab

[ContainerLab](https://containerlab.dev) topologies for the
**[Event-Driven Simulator](https://github.com/flaviobianco020/Event-Driven_Simulator)** (EDS) project.

These topologies reproduce, in a **real network environment** (Linux containers
connected by `veth` pairs, with traffic shaping via `tc`), the three topologies
defined in the EDS simulator in
[`simulator/network/topology.py`](https://github.com/flaviobianco020/Event-Driven_Simulator/blob/main/simulator/network/topology.py),
and the congestion scenarios of
[`examples/scenarios.py`](https://github.com/flaviobianco020/Event-Driven_Simulator/blob/main/examples/scenarios.py).

| Topology | EDS Simulator | ContainerLab |
|-----------|----------------|--------------|
| `single_bottleneck` | `NetworkTopology.single_bottleneck()` — N sources → router → dst | `src0`, `src1`, `src2` → `router` → `dst` |
| `multi_hop` | `NetworkTopology.multi_hop()` — linear chain | `n0 → n1 → n2 → n3` (3 hops) |
| `mesh` | `NetworkTopology.mesh()` — node grid | 2×3 grid (`n00 … n12`) |

> **Capacity mapping.** In the simulator the link capacities are expressed in
> *packets per unit of time*. Here the bottleneck (`capacity = 10`) is mapped to
> **10 Mbps**, while the high-capacity access links (`capacity = 1000`) are not
> bandwidth-limited: they only get a delay via `netem`, so as not to introduce an
> artificial bottleneck.

### Parity with the simulator's Phase 1

Besides the topologies, the emulator replicates **in real time** the Phase 1
components (PDF §4) via a Python control-plane (`emulator/` folder) and a traffic
agent in the containers (`agent/eds_node.py`):

| Phase 1 component (§4) | In the simulator | In the emulator |
|---|---|---|
| Network Topology + capacity/delays | `network/topology.py` | `deploy.sh` (`veth` + `tbf`/`netem`) |
| Queue Manager (finite queue, drop) | `queue_manager.py` (`max_size=20`) | `netem limit 20` (real drop-tail), read from `tc -s qdisc` |
| Traffic Generator (CBR/Poisson/Bursty/Video/Control/Telemetry) | `traffic/flow.py`, `generator.py` | `agent/eds_node.py` (UDP, same inter-arrival formulas) |
| Event Scheduler | `scheduler.py` | `RTScheduler` (heap on the real wall-clock) |
| **Congestion State Machine** (thresholds 0.50/0.70/0.85/0.95) | `congestion.py` | `eds_emulator.py` reads the real occupancy and applies `DROP_LOW_PRIORITY` via `tc` |
| Metrics Engine (throughput/PDR/latency/occupancy/drop/transitions/fairness) | `metrics.py` | `eds_emulator.py` (real measurements from agent + `tc`) |
| 6 scenarios (§4.5) | `examples/scenarios.py` | `emulator/scenarios.py` (same parameters and events) |

> **Compression/intermediate states.** In Phase 1 the `HEADER/DELTA/INCREMENTAL_COMPRESSION`
> states do not alter the traffic (see `core.py`): the only effective action is
> `DROP_LOW_PRIORITY`. The emulator reproduces exactly this behavior and counts
> the state transitions; the `compression_ratio` stays `1.0` as in the simulator.
> The *semantic* compression (Phase 1 §5.1) is application logic and is out of
> scope for the network emulator.

> **Rate mapping.** The simulator flow rates (pkt/s) are mapped 1:1 to Mbit/s
> (bottleneck capacity 10 → 10 Mbit/s), so the load/capacity ratios of each
> scenario stay identical (e.g. scenario 1: 8+5 vs 10).

> **Byte-limited bottleneck (parity with the simulator).** The bottleneck is
> `tbf 10Mbit` + `netem limit 20` **without delay** (pure drop-tail): the
> constraint is the **bandwidth** and the service is ∝ bytes, exactly like
> `QueueManager` in the simulator — so compression reduces the bytes and lets more
> packets through. A delay on the bottleneck netem, instead, by Little's law would
> consume the 20-packet buffer (`in flight = rate × delay`) making the bottleneck
> *packet-limited* (it saturates at `limit/delay`, independent of the bytes):
> compression would never empty the queue. This is why the **link latency**
> (~5ms) is modeled on the **access links** (`deploy.sh`), not on the bottleneck.
> To reproduce the old packet-limited behavior for comparison:
> `EDS_BOTTLENECK_DELAY=5ms`.

## Prerequisites

- [Docker](https://docs.docker.com/engine/install/)
- [ContainerLab](https://containerlab.dev/install/) (`containerlab` command)
- Image with network tools `ghcr.io/srl-labs/network-multitool` (contains
  `ip`, `tc`, `iperf3`, `ping`, **`python3`**); it is pulled on the first deploy.
- `python3` on the host (for the emulator control-plane — stdlib only).

## Repository structure

```
.
├── topologies/
│   ├── single_bottleneck.clab.yml   # includes the bind ../agent:/opt/eds
│   ├── multi_hop.clab.yml
│   └── mesh.clab.yml
├── deploy.sh            # deploy + network: IP, routing, tc (Topology + Queue Manager)
├── agent/
│   └── eds_node.py      # in-container UDP Traffic Generator (simulator FlowModel)
├── emulator/
│   ├── eds_emulator.py  # control-plane: scheduler + state machine + metrics engine
│   └── scenarios.py     # the 6 Phase 1 scenarios (§4.5)
├── run_simulation.sh    # quick per-topology test (iperf3 / ping)
├── scenarios.sh         # "lightweight" bash variant of the 6 scenarios
├── .vscode/
│   └── extensions.json  # recommended VS Code extensions
└── README.md
```

## Quick usage

```bash
chmod +x deploy.sh run_simulation.sh scenarios.sh

# 1) Deploy the network (Topology + Queue Manager via tc)
./deploy.sh single_bottleneck          # or: multi_hop | mesh

# 2) Phase 1 emulator: runs one of the 6 scenarios in real time
python3 emulator/scenarios.py 1        # ... up to 6
python3 emulator/scenarios.py 3 --scale 0.5   # halved times for quick demos

# (alternatively) quick network test or bash scenarios
./run_simulation.sh single_bottleneck
./scenarios.sh 1

# Tear down the lab
./deploy.sh single_bottleneck destroy
```

Scenario 6 uses a 30-packet queue: the control-plane sets it up on its own via
`tc`. The default size (20) is also configurable at deploy time:

```bash
QUEUE_LIMIT=30 ./deploy.sh single_bottleneck
```

---

## The three control modes

Every scenario can run under three different controllers. The network, the
queues and the traffic are identical: only **who decides the compression state**
changes.

| Mode | Flag | Who decides the transitions |
|---|---|---|
| **Phase 1** | *(none)* | instantaneous thresholds on the occupancy |
| **Phase 2** | `--phase2` | EWMA + asymmetric hysteresis (eFRAC) |
| **Phase 3** | `--mappo CKPT` | **MAPPO policy trained** in the simulator |

```bash
python3 emulator/scenarios.py 1                 # Phase 1
python3 emulator/scenarios.py 1 --phase2        # Phase 2 (NFQUEUE compressor)
python3 emulator/scenarios.py 1 \
    --mappo checkpoints/mappo_ccgated2.json     # Phase 3 (recommended policy)
```

> **Recommended Phase 3 policy: `checkpoints/mappo_ccgated2.json`.** It is trained
> with the *gated compression-cost* reward (see below): it self-regulates at the
> source and **does not require the deploy-side guardrail** (which stays off by
> default). It compresses only when needed — on an idle network (S2) or a downed
> link (S4) it stays at 1.00×, without truncating video for nothing — and beats
> the eFRAC baseline.

### Deploying the learned policy (Phase 3)

Phase 3 closes the **simulator → emulator** loop: the Actor neural network trained
with MAPPO (repo `Event-Driven_Simulator`, branch `phase3-mappo`) is loaded into
the control-plane and drives the state machine on the real ContainerLab network,
using the same NFQUEUE compression middlebox as Phase 2.

- **`agent/eds_actor.py`** loads the JSON checkpoint and computes its forward
  pass in **pure Python** (no numpy/torch): the Actor runs anywhere, consistent
  with the MAPPO document (Table 10, "Emulator deploy"). The forward pass is
  verified for numerical parity (error ≈ 1e-16) against the simulator's NumPy
  Actor.
- Every second the control-plane builds the 7-feature observation from the real
  `tc` statistics and the offered traffic mix, queries the Actor and applies
  `ESCALATE / MAINTAIN / DE-ESCALATE`.
- Robust checkpoints can declare `meta.min_state_dwell`. During this interval the
  emulator applies the same **action mask** as the simulator: normally only
  `MAINTAIN` is allowed; with a raw occupancy at least 95% `ESCALATE` also stays
  allowed. This prevents rapid oscillations without blocking the response to
  emergencies.
- **Anti-empty-compression guardrail (OPT-IN, off by default).** The *legacy*
  policy (`mappo_best_stab_emulator_aligned_stress_rho7.json`) is trained with a
  reward *without* compression cost, in a simulator where compressing is *non
  destructive* (it only reduces the queue service time): it thus learns to
  compress eagerly. On the emulator compression *truncates the payload* and adds
  the latency of the NFQUEUE middlebox, so compressing when packets are not being
  lost throws away throughput without improving the PDR (scenario 2 *flash crowd*
  → 1.31×, scenario 4 *link down* → 1.28×, for zero gain). To plug that behavior
  the `mappo_action_mask` could block `ESCALATE` until there was **real loss
  pressure** (`drop_rate` above threshold) or a queue emergency (occupancy ≥ 95%).
  **Today the recommended policy `mappo_ccgated2` moves this regulation into the
  reward** (gated compression cost, see below) and self-regulates, so the
  guardrail is **off by default**: `EDS_MAPPO_GATE=1` re-enables it only for the
  legacy checkpoints. It acts only as an upper bound anyway —
  `MAINTAIN`/`DE-ESCALATE` are always allowed.
- **Gated compression-cost reward (policy `mappo_ccgated2`).** Instead of
  correcting at deploy time, the compression penalty in the simulator is
  multiplied by `(1 − congestion)`, with `congestion = max(occ/0.2, loss/0.01)`:
  full on an idle network (pushes towards `NORMAL`), null under congestion
  (compresses freely). This way the policy *learns* to tie compression to the real
  congestion, not to the link. On the emulator: clean idle (S2/S4 at 1.00×) **and**
  compression under load (S5 on par with the legacy), average PDR 85.9% with half
  the destructive truncation (1.12× vs 1.28×). See `simulator/marl/env.py` in the
  simulator repo.
- **Compression coverage and diagnostics.** Under heavy overload (scenarios 1 and
  5) the PDR stays at the "no compression" bound: compression does not empty the
  queue. Two levers/tools to investigate it:
  - `EDS_NFQUEUE_MINLEN` (default 500) tunes the minimum IP size of the packets
    sent to the compressor. At 500 only the `video` packets are compressed; at
    ~200 the `telemetry` (structured data, compresses ~4×) enters too, which
    raises the theoretical PDR bound of scenario 5 towards 100% — at the cost of
    more load on the userspace compressor.
  - At the end of a run (Phase 2/3) the emulator prints the real compressor
    counters to the run log: `real_wire_ratio` (`bytes_in/bytes_out`), the
    percentage of packets actually truncated and the `bypass` (packets that
    matched the rule but passed through uncompressed because the compressor could
    not keep up with the rate). They serve to distinguish whether compression
    "does not bite" because the controller does not reach the deep states or
    because the userspace middlebox is saturated.

To save the observations, probabilities, mask and action actually used during a
run:

```bash
python3 emulator/scenarios.py 3 \
    --mappo /path/to/mappo_best_stab_emulator_aligned_stress_rho7.json \
    --mappo-trace logs/alignment_trace_emu_s3.json
```

The trace uses the `eds-mappo-observation-trace-v1` schema and is comparable with
the one produced by the simulator, so the sim-to-real gap can be measured feature
by feature instead of being inferred only from the final KPIs.

**Sim-to-real gap:** two of the seven features (fraction of high- and low-priority
packets in the queue) are not inspectable via `tc` on the emulator and are
approximated with the composition of the *offered* traffic. It is the only point
where the deploy observation differs from the training one.

To obtain a checkpoint, in the simulator repo:

```bash
# recommended policy (gated reward): stability + compression-cost
python3 examples/train_mappo.py --episodes 4000 --stability-penalty 0.1 --compression-cost 0.5
# → produces checkpoints/mappo_best_stab_cc.json  (here renamed mappo_ccgated2.json)
```

### Automatic comparison of the three controllers

`emulator/benchmark.py` runs the three controllers on all the scenarios **on the
same hardware**, re-running `deploy.sh` between runs to always start from a clean
`tc` state, and prints the comparative KPI table (averaged over several
repetitions, because the emulator has real noise).

```bash
python3 emulator/benchmark.py \
    --mappo-ckpt checkpoints/mappo_ccgated2.json \
    --scale 0.5                 # halved times for a faster sweep

# direct comparison of two policies (e.g. legacy vs ccgated2) in the same table:
python3 emulator/benchmark.py --scenarios 1,2,3,4,5,6,7 --repeats 2 \
    --mappo-ckpt   checkpoints/mappo_best_stab_emulator_aligned_stress_rho7.json \
    --mappo-ckpt-b checkpoints/mappo_ccgated2.json \
    --label-a current --label-b ccgated2 \
    --out logs/benchmark.csv
```

---

## Phase 1 emulator (`emulator/`)

`emulator/scenarios.py` orchestrates a complete scenario on the
`single_bottleneck` topology (like the simulator's `examples/scenarios.py`). For
each run:

1. it starts the UDP **receiver** on the `dst` node (`agent/eds_node.py recv`);
2. a **real-time scheduler** launches the flows (`FLOW_START/STOP`), the network
   events (`LINK_RATE_CHANGE`, `LINK_FAILURE/RECOVERY`) and the periodic samples
   (`METRIC_SAMPLE`) at the times prescribed by the scenario;
3. the **traffic generators** (`eds_node.py send`) send UDP reproducing the
   simulator's `FlowModel` and mark the packets with DSCP by priority
   (control `CS6`, telemetry `CS2`, video `AF11`);
4. a **controller** reads the real queue occupancy from `tc -s qdisc` every 0.5 s
   and runs the **Congestion State Machine** (identical thresholds
   0.50/0.70/0.85/0.95); in state `DROP_LOW_PRIORITY` it installs `tc` filters
   that drop the priority > 0 traffic (like the simulator);
5. at the end of the run it prints the **metrics** (throughput, PDR, end-to-end
   latency, average occupancy, drop, state transitions, Jain's fairness).

Example output (per `METRIC_SAMPLE` line and final summary):

```
  [t= 30.0] METRIC  occ= 96.0%  state=DROP_LOW_PRIORITY      thr=  812.0 pkt/s  drop=134
  ...
  Packet Delivery Ratio .......... 78.41%
  Congestion state transitions ... 4
  Fairness (Jain) ................ 0.812
```

> **Status note.** The control-plane was verified at the syntax level
> (`py_compile`, `bash -n`) but **not yet run on a real lab**. Some `tc` details
> (`action drop` filters on `tbf`, persistence after `ip link down/up`) are to be
> confirmed on the first deploy: see the *Verification* section at the bottom.

---

## Topologies and addressing

### `single_bottleneck`

Three sources reach `dst` through a single `router`. The access links
(`src* → router`) are high-capacity; the `router → dst` link is the 10 Mbps
bottleneck with a 20-packet queue.

```
src0 ──┐
src1 ──┼── router ════(10 Mbps, queue 20)════ dst
src2 ──┘
```

| Link | Subnet | Addresses |
|------|--------|-----------|
| `src0 ↔ router` (access) | `10.0.10.0/24` | `src0=.1`, `router=.254` |
| `src1 ↔ router` (access) | `10.0.20.0/24` | `src1=.1`, `router=.254` |
| `src2 ↔ router` (access) | `10.0.40.0/24` | `src2=.1`, `router=.254` |
| `router ↔ dst` (bottleneck) | `10.0.30.0/24` | `router=.254`, `dst=.1` |

### `multi_hop`

Linear chain of 4 nodes (3 hops), each link 10 Mbps (queue 20).

```
n0 ══(10 Mbps)══ n1 ══(10 Mbps)══ n2 ══(10 Mbps)══ n3
```

| Link | Subnet | Addresses |
|------|--------|-----------|
| `n0 ↔ n1` | `10.0.1.0/24` | `n0=.1`, `n1=.2` |
| `n1 ↔ n2` | `10.0.2.0/24` | `n1=.1`, `n2=.2` |
| `n2 ↔ n3` | `10.0.3.0/24` | `n2=.1`, `n3=.2` |

### `mesh`

2×3 grid with bidirectional links, each 10 Mbps (queue 20).

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

The mesh routing is configured for the demonstrative **corner → corner** path
`n00 (10.1.1.1) ↔ n12 (10.1.7.2)` along `n00 → n01 → n02 → n12`. IP forwarding is
enabled on all nodes, so other routes can be added for different paths.

---

## Congestion scenarios (`scenarios.sh`)

They reproduce the 6 scenarios of `examples/scenarios.py` on the
`single_bottleneck` topology (they must be run after
`./deploy.sh single_bottleneck`). The traffic is UDP (`iperf3 -u`), so the drops
caused by the full queue are visible in the report as lost datagrams — the
equivalent of `drop_count` in the simulator.

| # | Scenario | Timed events reproduced |
|---|----------|---------------------------|
| 1 | `single_bottleneck` | constant overload (load 13 > cap 10) |
| 2 | `flash_crowd` | extra surge flow from t=20 to t=50 |
| 3 | `bandwidth_degradation` | `tc change` bandwidth 10→4 at t=30, 10 at t=60 |
| 4 | `link_failure_recovery` | `ip link down` at t=30, `up` at t=55 |
| 5 | `persistent_overload` | 3 flows, sustained overload (load 15) |
| 6 | `mixed_telemetry_video` | 3 classes with priority (HTB + DSCP) |

> The bash variant `scenarios.sh` is a quick `iperf3` demo. For faithful parity
> with the simulator (state machine, metrics, FlowModel) use
> `python3 emulator/scenarios.py <1-6>`.

### Scenario 6 — priority

Scenario 6 reconfigures the bottleneck with a 3-class **HTB** qdisc and **DSCP**
filters; the sources mark the traffic with `iperf3 -S`:

| Class | DSCP | Source | HTB priority |
|--------|------|----------|--------------|
| `control`   | CS6 (`0xc0`) | `src2` | `1:10` (prio 0, protected) |
| `telemetry` | CS2 (`0x40`) | `src1` | `1:20` (prio 1) |
| `video`     | best-effort  | `src0` | `1:30` (prio 2, default) |

Under congestion the `control` traffic (highest priority) stays protected, while
the `video` takes most of the drops — like the `DROP_LOW_PRIORITY` transition of
the simulator's state machine. To go back to the plain drop-tail just re-run
`./deploy.sh single_bottleneck`.

---

## Shaping details (`tc`)

The shaping is applied by `deploy.sh` after the deploy. Two cases:

- **10 Mbps links (bottlenecks).** `tbf` with the **burst fix** (a `burst` that is
  too small prevents reaching the nominal rate, hence `burst 1mbit` ≈ 125 kB), and
  `netem` as the child qdisc for the delay and the **finite queue** `limit 20`
  (drop-tail). The `tbf` has a wide `limit` so the drops happen in the `netem`, by
  number of packets, exactly as in `QueueManager.enqueue()`:

  ```
  tc qdisc replace dev <if> root   handle 1:  tbf   rate 10mbit burst 1mbit limit 1m
  tc qdisc replace dev <if> parent 1:1 handle 10: netem delay 5ms limit 20
  ```

- **High-capacity access links.** No `tbf` (it would introduce an artificial
  bottleneck): only `netem` for the delay.

  ```
  tc qdisc replace dev <if> root netem delay 1ms
  ```

### Routing

The routes are **subnet-specific** and are added with
`ip route replace <subnet> via <gateway>`. **The default gateway is not touched**:
it stays the one assigned by ContainerLab/Docker for the lab management network.

---

## Verification

After `./deploy.sh single_bottleneck`:

- **Basic network** (`scenarios.sh` / `run_simulation.sh`): `iperf3` flows towards
  `dst` share ~**10 Mbps**; under UDP overload the drops appear (queue 20).
- **Phase 1 emulator** (`python3 emulator/scenarios.py <1-6>`): the
  `METRIC_SAMPLE` lines show the occupancy and congestion state rising with the
  load; in `DROP_LOW_PRIORITY` the `control` stays delivered while video/telemetry
  are dropped.
- `multi_hop`: throughput `n0 → n3` ~10 Mbps, RTT growing with the hops.
- `mesh`: throughput `n00 → n12` ~10 Mbps along the configured path.

Note: the Python control-plane is validated for syntax but not yet run on a real
lab; the `tc` commands (`action drop` filters on `tbf`, restore after
`ip link down/up`) are to be confirmed on the first deploy.
