#!/usr/bin/env python3
"""
scenarios.py - I 6 scenari canonici della Fase 1 (PDF §4.5) per l'EMULATORE.

Rispecchiano examples/scenarios.py del simulatore: stesse sorgenti, stessi
FlowModel/classi, stessi rate (qui mappati 1:1 da pkt/s a Mbit/s, dato che nel
simulatore la capacita' del collo di bottiglia e' 10 e qui e' 10 Mbit/s) e
stessi eventi temporizzati (degrado banda, link failure/recovery, flash crowd).

Prerequisito:
    ./deploy.sh single_bottleneck

Uso:
    python3 emulator/scenarios.py <1-6> [--scale 0.5]

    1  single_bottleneck      - overload di base (load 13 > cap 10)
    2  flash_crowd            - flusso surge bursty da t=20 a t=50
    3  bandwidth_degradation  - banda 10->4 a t=30, 10 a t=60
    4  link_failure_recovery  - link giu' a t=30, su a t=55
    5  persistent_overload    - 3 flussi, overload sostenuto (load 15)
    6  mixed_telemetry_video  - 3 classi con priorita' (control protetto)

--scale moltiplica tutti i tempi (per demo piu' rapide; default 1.0).
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from eds_emulator import (  # noqa: E402
    FlowSpec, FlowModel, run_emulation,
    VIDEO, TELEMETRY, CONTROL,
)

TOPO = "single_bottleneck"


def scenario_1(k):
    end = 60.0 * k
    flows = [
        FlowSpec(0, "src0", FlowModel.POISSON, 8.0, VIDEO,   0.0, end),
        FlowSpec(1, "src1", FlowModel.CONTROL, 5.0, CONTROL, 0.0, end),
    ]
    return run_emulation(TOPO, flows, [], end, metric_interval=10.0 * k,
                         title="Scenario 1 - Single Bottleneck (load=13 > cap=10)")


def scenario_2(k):
    end = 80.0 * k
    s20, s50 = 20.0 * k, 50.0 * k
    flows = [
        FlowSpec(0, "src0", FlowModel.POISSON, 4.0, VIDEO,   0.0, end),
        FlowSpec(2, "src2", FlowModel.CONTROL, 2.0, CONTROL, 0.0, end),
        FlowSpec(1, "src1", FlowModel.BURSTY,  6.0, VIDEO,   s20, s50),  # surge
    ]
    return run_emulation(TOPO, flows, [], end, metric_interval=10.0 * k,
                         title="Scenario 2 - Flash Crowd (surge t=20->50)")


def scenario_3(k):
    end = 80.0 * k
    flows = [
        FlowSpec(0, "src0", FlowModel.POISSON, 7.0, VIDEO,   0.0, end),
        FlowSpec(1, "src1", FlowModel.CONTROL, 2.0, CONTROL, 0.0, end),
    ]
    events = [(30.0 * k, "rate", 4.0), (60.0 * k, "rate", 10.0)]
    return run_emulation(TOPO, flows, events, end, metric_interval=10.0 * k,
                         title="Scenario 3 - Bandwidth Degradation (10->4 a t=30, 10 a t=60)")


def scenario_4(k):
    end = 90.0 * k
    flows = [
        FlowSpec(0, "src0", FlowModel.POISSON, 6.0, VIDEO,   0.0, end),
        FlowSpec(1, "src1", FlowModel.CONTROL, 3.0, CONTROL, 0.0, end),
    ]
    events = [(30.0 * k, "down", None), (55.0 * k, "up", None)]
    return run_emulation(TOPO, flows, events, end, metric_interval=10.0 * k,
                         title="Scenario 4 - Link Failure & Recovery (giu' t=30, su t=55)")


def scenario_5(k):
    end = 100.0 * k
    flows = [
        FlowSpec(0, "src0", FlowModel.POISSON, 7.0, VIDEO,     0.0, end),
        FlowSpec(1, "src1", FlowModel.POISSON, 5.0, TELEMETRY, 0.0, end),
        FlowSpec(2, "src2", FlowModel.CONTROL, 3.0, CONTROL,   0.0, end),
    ]
    return run_emulation(TOPO, flows, [], end, metric_interval=10.0 * k,
                         title="Scenario 5 - Persistent Overload (load=15 >> cap=10)")


def scenario_6(k):
    end = 80.0 * k
    flows = [
        FlowSpec(0, "src0", FlowModel.VIDEO,              5.0, VIDEO,     0.0, end),
        FlowSpec(1, "src1", FlowModel.PERIODIC_TELEMETRY, 4.0, TELEMETRY, 0.0, end),
        FlowSpec(2, "src2", FlowModel.CONTROL,            2.0, CONTROL,   0.0, end),
    ]
    # queue_size=30 come nello scenario 6 del simulatore
    return run_emulation(TOPO, flows, [], end, metric_interval=10.0 * k, queue_limit=30,
                         title="Scenario 6 - Mixed Telemetry & Video (control protetto)")


SCENARIOS = {
    "1": scenario_1, "2": scenario_2, "3": scenario_3,
    "4": scenario_4, "5": scenario_5, "6": scenario_6,
}


def main():
    if len(sys.argv) < 2 or sys.argv[1] not in SCENARIOS:
        print(__doc__)
        sys.exit(1)
    which = sys.argv[1]
    scale = 1.0
    if "--scale" in sys.argv:
        try:
            scale = float(sys.argv[sys.argv.index("--scale") + 1])
        except (IndexError, ValueError):
            print("--scale richiede un numero (es. --scale 0.5)")
            sys.exit(1)
    SCENARIOS[which](scale)


if __name__ == "__main__":
    main()
