#!/usr/bin/env python3
"""
scenarios.py - The 6 canonical Phase 1 scenarios (PDF §4.5) for the EMULATOR.

They mirror the simulator's examples/scenarios.py: same sources, same
FlowModel/classes, same rates (here mapped 1:1 from pkt/s to Mbit/s, since in
the simulator the bottleneck capacity is 10 and here it is 10 Mbit/s) and the
same timed events (bandwidth degradation, link failure/recovery, flash crowd).

Prerequisite:
    ./deploy.sh <single_bottleneck|multi_hop|mesh>

Usage:
    python3 emulator/scenarios.py <1-7> [--topo TOPO] [--scale 0.5] [--phase2 | --mappo CKPT]

    --topo single_bottleneck (default) | multi_hop | mesh
           on multi_hop/mesh the same scenarios run single-path: all the flows
           enter at the first hop (n0 / n00) towards the destination, and the
           monitored/compressed queue is the one of the first link.

    1  single_bottleneck      - basic overload (load 13 > cap 10)
    2  flash_crowd            - bursty surge flow from t=20 to t=50
    3  bandwidth_degradation  - bandwidth 10->4 at t=30, 10 at t=60
    4  link_failure_recovery  - link down at t=30, up at t=55
    5  persistent_overload    - 3 flows, sustained overload (load 15)
    6  mixed_telemetry_video  - 3 classes with priority (control protected)
    7  oscillating            - bottleneck pulses 10->4 for 3s every 7s (load 9)

Control modes (alternatives):
    (none)           PHASE 1 - instantaneous state transitions on occupancy
    --phase2         PHASE 2 - EWMA + hysteresis (eFRAC) + NFQUEUE compressor
    --mappo CKPT     PHASE 3 - the trained MAPPO policy drives the state
                     machine; CKPT is the JSON checkpoint produced in the simulator.
                     Recommended policy: checkpoints/mappo_ccgated2.json (gated
                     reward, self-regulating -> guardrail not needed). Uses the
                     same NFQUEUE compressor as Phase 2: only the "brain" changes.

--scale multiplies all the times (for faster demos; default 1.0).
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from eds_emulator import (  # noqa: E402
    FlowSpec, FlowModel, run_emulation,
    VIDEO, TELEMETRY, CONTROL,
)

TOPO = "single_bottleneck"


def scenario_1(k, phase2=False, mappo=None, trace=None):
    end = 60.0 * k
    flows = [
        FlowSpec(0, "src0", FlowModel.POISSON, 8.0, VIDEO,   0.0, end),
        FlowSpec(1, "src1", FlowModel.CONTROL, 5.0, CONTROL, 0.0, end),
    ]
    return run_emulation(TOPO, flows, [], end, metric_interval=10.0 * k,
                         enable_phase2=phase2, mappo_ckpt=mappo, mappo_trace_path=trace,
                         title="Scenario 1 - Single Bottleneck (load=13 > cap=10)")


def scenario_2(k, phase2=False, mappo=None, trace=None):
    end = 80.0 * k
    s20, s50 = 20.0 * k, 50.0 * k
    flows = [
        FlowSpec(0, "src0", FlowModel.POISSON, 4.0, VIDEO,   0.0, end),
        FlowSpec(2, "src2", FlowModel.CONTROL, 2.0, CONTROL, 0.0, end),
        FlowSpec(1, "src1", FlowModel.BURSTY,  6.0, VIDEO,   s20, s50),  # surge
    ]
    return run_emulation(TOPO, flows, [], end, metric_interval=10.0 * k,
                         enable_phase2=phase2, mappo_ckpt=mappo, mappo_trace_path=trace,
                         title="Scenario 2 - Flash Crowd (surge t=20->50)")


def scenario_3(k, phase2=False, mappo=None, trace=None):
    end = 80.0 * k
    flows = [
        FlowSpec(0, "src0", FlowModel.POISSON, 7.0, VIDEO,   0.0, end),
        FlowSpec(1, "src1", FlowModel.CONTROL, 2.0, CONTROL, 0.0, end),
    ]
    events = [(30.0 * k, "rate", 4.0), (60.0 * k, "rate", 10.0)]
    return run_emulation(TOPO, flows, events, end, metric_interval=10.0 * k,
                         enable_phase2=phase2, mappo_ckpt=mappo, mappo_trace_path=trace,
                         title="Scenario 3 - Bandwidth Degradation (10->4 at t=30, 10 at t=60)")


def scenario_4(k, phase2=False, mappo=None, trace=None):
    end = 90.0 * k
    flows = [
        FlowSpec(0, "src0", FlowModel.POISSON, 6.0, VIDEO,   0.0, end),
        FlowSpec(1, "src1", FlowModel.CONTROL, 3.0, CONTROL, 0.0, end),
    ]
    events = [(30.0 * k, "down", None), (55.0 * k, "up", None)]
    return run_emulation(TOPO, flows, events, end, metric_interval=10.0 * k,
                         enable_phase2=phase2, mappo_ckpt=mappo, mappo_trace_path=trace,
                         title="Scenario 4 - Link Failure & Recovery (down t=30, up t=55)")


def scenario_5(k, phase2=False, mappo=None, trace=None):
    end = 100.0 * k
    flows = [
        FlowSpec(0, "src0", FlowModel.POISSON, 7.0, VIDEO,     0.0, end),
        FlowSpec(1, "src1", FlowModel.POISSON, 5.0, TELEMETRY, 0.0, end),
        FlowSpec(2, "src2", FlowModel.CONTROL, 3.0, CONTROL,   0.0, end),
    ]
    return run_emulation(TOPO, flows, [], end, metric_interval=10.0 * k,
                         enable_phase2=phase2, mappo_ckpt=mappo, mappo_trace_path=trace,
                         title="Scenario 5 - Persistent Overload (load=15 >> cap=10)")


def scenario_6(k, phase2=False, mappo=None, trace=None):
    end = 80.0 * k
    flows = [
        FlowSpec(0, "src0", FlowModel.VIDEO,              5.0, VIDEO,     0.0, end),
        FlowSpec(1, "src1", FlowModel.PERIODIC_TELEMETRY, 4.0, TELEMETRY, 0.0, end),
        FlowSpec(2, "src2", FlowModel.CONTROL,            2.0, CONTROL,   0.0, end),
    ]
    # queue_size=30 as in scenario 6 of the simulator
    return run_emulation(TOPO, flows, [], end, metric_interval=10.0 * k, queue_limit=30,
                         enable_phase2=phase2, mappo_ckpt=mappo, mappo_trace_path=trace,
                         title="Scenario 6 - Mixed Telemetry & Video (control protected)")


def scenario_7(k, phase2=False, mappo=None, trace=None):
    """Oscillating: short congestion pulses (bottleneck 10->4 for 3s) every 7s, with
    a fixed load of 9 (VIDEO 7 + CONTROL 2). The bottleneck flickers below/above the
    load, forcing the policy to go up and down quickly: here the dwell and the
    guardrail threshold become binding (unlike the canonical scenarios, which do
    not oscillate -> flat A/B experiments)."""
    end = 80.0 * k
    flows = [
        FlowSpec(0, "src0", FlowModel.POISSON, 7.0, VIDEO,   0.0, end),
        FlowSpec(1, "src1", FlowModel.CONTROL, 2.0, CONTROL, 0.0, end),
    ]
    events = []
    t = 10.0 * k
    while t < end - 4.0 * k:
        events.append((t, "rate", 4.0))             # pulse: bottleneck below the load
        events.append((t + 3.0 * k, "rate", 10.0))  # restore
        t += 7.0 * k
    return run_emulation(TOPO, flows, events, end, metric_interval=5.0 * k,
                         enable_phase2=phase2, mappo_ckpt=mappo, mappo_trace_path=trace,
                         title="Scenario 7 - Oscillating (bottleneck pulses 10->4 x3s every 7s)")


SCENARIOS = {
    "1": scenario_1, "2": scenario_2, "3": scenario_3,
    "4": scenario_4, "5": scenario_5, "6": scenario_6,
    "7": scenario_7,
}


def main():
    if len(sys.argv) < 2 or sys.argv[1] not in SCENARIOS:
        print(__doc__)
        sys.exit(1)
    which = sys.argv[1]
    scale = 1.0
    phase2 = "--phase2" in sys.argv
    mappo = None
    trace = None
    if "--topo" in sys.argv:
        global TOPO
        try:
            TOPO = sys.argv[sys.argv.index("--topo") + 1]
        except IndexError:
            print("--topo requires a name (single_bottleneck|multi_hop|mesh)")
            sys.exit(1)
        if TOPO not in ("single_bottleneck", "multi_hop", "mesh"):
            print(f"unknown topology: {TOPO} "
                  "(valid: single_bottleneck, multi_hop, mesh)")
            sys.exit(1)
    if "--scale" in sys.argv:
        try:
            scale = float(sys.argv[sys.argv.index("--scale") + 1])
        except (IndexError, ValueError):
            print("--scale requires a number (e.g. --scale 0.5)")
            sys.exit(1)
    if "--mappo" in sys.argv:
        try:
            mappo = sys.argv[sys.argv.index("--mappo") + 1]
        except IndexError:
            print("--mappo requires the path of the JSON checkpoint "
                  "(e.g. --mappo checkpoints/mappo_ccgated2.json)")
            sys.exit(1)
        if not os.path.exists(mappo):
            print(f"checkpoint not found: {mappo}")
            sys.exit(1)
        if phase2:
            print("--mappo and --phase2 are alternatives: Phase 3 already uses "
                  "the compressor. Ignoring --phase2.")
            phase2 = False
    if "--mappo-trace" in sys.argv:
        try:
            trace = sys.argv[sys.argv.index("--mappo-trace") + 1]
        except IndexError:
            print("--mappo-trace requires an output JSON file")
            sys.exit(1)
        if mappo is None:
            print("--mappo-trace also requires --mappo CKPT")
            sys.exit(1)
    SCENARIOS[which](scale, phase2=phase2, mappo=mappo, trace=trace)


if __name__ == "__main__":
    main()
