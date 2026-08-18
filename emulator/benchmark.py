#!/usr/bin/env python3
"""
benchmark.py — Automatic comparison of the three controllers ON the real EMULATOR.

Runs, on the same ContainerLab hardware, each of the canonical scenarios under:

    Phase 1  — instantaneous transitions on occupancy
    Phase 2  — EWMA + hysteresis (eFRAC) + NFQUEUE compressor
    Phase 3  — trained MAPPO policy (simulator JSON checkpoint)

and produces a comparative table of the measured KPIs (PDR, throughput, latency,
occupancy, drop, transitions, compression ratio). Averaged over several
repetitions, because the emulator has real noise.

Prerequisite:  the lab must already be deployed at least once
    ./deploy.sh single_bottleneck

Usage:
    # all scenarios, the three controllers, 1 repetition, halved times:
    python3 emulator/benchmark.py \
        --mappo-ckpt ../Event-Driven_Simulator/checkpoints/mappo_best_stab.json \
        --scale 0.5

    # only scenarios 1,3,5, two repetitions, save CSV:
    python3 emulator/benchmark.py --scenarios 1,3,5 --repeats 2 \
        --mappo-ckpt ../Event-Driven_Simulator/checkpoints/mappo_best_stab.json \
        --out logs/benchmark.csv

Options:
    --topo TOPO               single_bottleneck (default) | multi_hop | mesh
    --scenarios 1,2,3,4,5,6   which scenarios (default: all)
    --modes phase1,phase2,mappo   which controllers (default: all three)
    --mappo-ckpt PATH         JSON checkpoint (required if 'mappo' is among the modes)
    --repeats N               repetitions per (scenario, mode), averaged (default 1)
    --scale K                 scales the scenario times (default 1.0)
    --no-redeploy             do NOT re-run deploy.sh before each run
    --out FILE                save the raw rows to CSV
"""
from __future__ import annotations

import argparse
import contextlib
import csv
import os
import statistics
import subprocess
import sys
import time

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.abspath(os.path.join(_HERE, ".."))
sys.path.insert(0, _HERE)

import scenarios as scen  # noqa: E402

SCEN_NAMES = {
    "1": "single bottleneck", "2": "flash crowd", "3": "bandwidth degr.",
    "4": "link fail/recov.", "5": "persistent overload", "6": "mixed traffic",
    "7": "oscillating",
}
# mappo = checkpoint A (--mappo-ckpt), mappob = checkpoint B (--mappo-ckpt-b):
# allows the direct comparison of two MAPPO policies in the same table (e.g.
# current vs ccgated), interleaved on the same redeploys to reduce the noise.
MODE_LABEL = {"phase1": "Phase 1", "phase2": "Phase 2",
              "mappo": "MAPPO-A", "mappob": "MAPPO-B"}
MAPPO_MODES = ("mappo", "mappob")
KPIS = [
    ("packet_delivery_ratio", "PDR", "%"),
    ("throughput_mbps", "thr", "Mbit"),
    ("end_to_end_latency_ms", "lat", "ms"),
    ("avg_queue_occupancy", "occ", "%"),
    ("drop_count", "drop", ""),
    ("congestion_state_transitions", "trans", ""),
    ("compression_ratio", "compr", "x"),
]


def _redeploy(topo: str = "single_bottleneck") -> bool:
    """Re-runs deploy.sh <topo> to reset the tc state between runs."""
    try:
        r = subprocess.run(["./deploy.sh", topo], cwd=_ROOT,
                           capture_output=True, text=True, timeout=180)
        return r.returncode == 0
    except (subprocess.TimeoutExpired, OSError):
        return False


def _run_once(which: str, mode: str, scale: float, ckpts: dict, logpath: str):
    """Runs one scenario in one mode, capturing the output in the log.

    ckpts: mode->path map for the MAPPO modes ({"mappo": pathA, "mappob": pathB}).
    """
    fn = scen.SCENARIOS[which]
    kwargs = {"phase2": mode == "phase2",
              "mappo": ckpts.get(mode) if mode in MAPPO_MODES else None}
    try:
        with open(logpath, "w") as logf, contextlib.redirect_stdout(logf):
            return fn(scale, **kwargs)
    except Exception as exc:  # noqa: BLE001 — a failure must not stop the sweep
        with open(logpath, "a") as logf:
            logf.write(f"\n[benchmark] ERROR: {exc}\n")
        return None


def _fmt(key: str, unit: str, v):
    if v is None:
        return "  —"
    if unit == "%":
        return f"{v*100:.1f}%"
    if unit == "Mbit":
        return f"{v:.2f}"
    if unit == "ms":
        return f"{v:.1f}"
    if unit == "x":
        return f"{v:.2f}x"
    return f"{v:.0f}"


def main() -> None:
    ap = argparse.ArgumentParser(description="Benchmark 3 controllers on the emulator")
    ap.add_argument("--scenarios", default="1,2,3,4,5,6")
    ap.add_argument("--modes", default="phase1,phase2,mappo")
    ap.add_argument("--mappo-ckpt", default=None, help="MAPPO checkpoint A (mode 'mappo')")
    ap.add_argument("--mappo-ckpt-b", default=None, help="MAPPO checkpoint B (mode 'mappob')")
    ap.add_argument("--label-a", default=None, help="label for mode 'mappo' (default: file name)")
    ap.add_argument("--label-b", default=None, help="label for mode 'mappob' (default: file name)")
    ap.add_argument("--repeats", type=int, default=1)
    ap.add_argument("--scale", type=float, default=1.0)
    ap.add_argument("--topo", default="single_bottleneck",
                    choices=["single_bottleneck", "multi_hop", "mesh"],
                    help="ContainerLab topology (default single_bottleneck)")
    ap.add_argument("--no-redeploy", action="store_true")
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    scen.TOPO = args.topo  # the scenarios read this global to pick the topology

    which_list = [s.strip() for s in args.scenarios.split(",") if s.strip()]
    modes = [m.strip() for m in args.modes.split(",") if m.strip()]
    for m in modes:
        if m not in MODE_LABEL:
            print(f"unknown mode: {m} (valid: phase1, phase2, mappo, mappob)")
            sys.exit(1)
    # checkpoints for the two MAPPO modes (A = --mappo-ckpt, B = --mappo-ckpt-b)
    ckpts: dict[str, str] = {}
    for mode, arg, argname in (("mappo", args.mappo_ckpt, "--mappo-ckpt"),
                               ("mappob", args.mappo_ckpt_b, "--mappo-ckpt-b")):
        if mode in modes:
            if not arg:
                print(f"{argname} is required when '{mode}' is among the modes.")
                sys.exit(1)
            if not os.path.exists(arg):
                print(f"checkpoint not found: {arg}")
                sys.exit(1)
            ckpts[mode] = os.path.abspath(arg)

    # readable labels for the table (default: file name without mappo_/.json)
    def _short(path):
        base = os.path.basename(path)
        for p in ("mappo_", ".json"):
            base = base.replace(p, "")
        return base[:12]
    if "mappo" in ckpts:
        MODE_LABEL["mappo"] = args.label_a or _short(ckpts["mappo"])
    if "mappob" in ckpts:
        MODE_LABEL["mappob"] = args.label_b or _short(ckpts["mappob"])

    logdir = os.path.join(_ROOT, "logs")
    os.makedirs(logdir, exist_ok=True)

    n_runs = len(which_list) * len(modes) * args.repeats
    print("=" * 78)
    print("  BENCHMARK controllers on the ContainerLab emulator")
    print(f"  topo={args.topo}  scenarios={','.join(which_list)}  modes={','.join(modes)}  "
          f"repeats={args.repeats}  scale={args.scale}")
    print(f"  total runs: {n_runs}  (detailed logs in logs/bench_*.log)")
    print("=" * 78)

    # raw[(scenario, mode)] = list of summary dicts (one per repetition)
    raw: dict[tuple, list] = {}
    csv_rows = []
    run_i = 0
    t0 = time.time()

    for which in which_list:
        for mode in modes:
            samples = []
            for rep in range(args.repeats):
                run_i += 1
                tag = f"s{which}_{mode}_r{rep+1}"
                if not args.no_redeploy:
                    ok = _redeploy(args.topo)
                    if not ok:
                        print(f"  [{run_i}/{n_runs}] {tag}: deploy FAILED, skipping")
                        continue
                print(f"  [{run_i}/{n_runs}] scenario {which} · {MODE_LABEL[mode]} "
                      f"· rep {rep+1} ...", end="", flush=True)
                logpath = os.path.join(logdir, f"bench_{tag}.log")
                summ = _run_once(which, mode, args.scale, ckpts, logpath)
                if summ is None:
                    print(" ERROR (see log)")
                    continue
                samples.append(summ)
                row = {"scenario": which, "mode": mode, "rep": rep + 1}
                row.update({k: summ.get(k) for k, _, _ in KPIS})
                csv_rows.append(row)
                print(f" PDR {summ['packet_delivery_ratio']*100:.1f}%  "
                      f"thr {summ['throughput_mbps']:.2f}  "
                      f"trans {summ['congestion_state_transitions']}")
            if samples:
                raw[(which, mode)] = samples

    # ---- mean and standard deviation per (scenario, mode) ----------------
    def _vals(which, mode, key):
        s = raw.get((which, mode))
        return [d[key] for d in s if d.get(key) is not None] if s else []

    def avg(which, mode, key):
        vals = _vals(which, mode, key)
        return statistics.fmean(vals) if vals else None

    def std(which, mode, key):
        vals = _vals(which, mode, key)
        return statistics.pstdev(vals) if len(vals) > 1 else 0.0

    print("\n" + "=" * 78)
    print(f"  RESULTS  (mean over {args.repeats} repetition(s))")
    print("=" * 78)
    for which in which_list:
        print(f"\n  Scenario {which} — {SCEN_NAMES.get(which, '?')}")
        header = f"    {'controller':<10}" + "".join(f"{lab:>10}" for _, lab, _ in KPIS)
        print(header)
        for mode in modes:
            if (which, mode) not in raw:
                continue
            cells = "".join(f"{_fmt(k, u, avg(which, mode, k)):>10}" for k, _, u in KPIS)
            print(f"    {MODE_LABEL[mode]:<10}{cells}")

    # ---- global mean (over all scenarios) --------------------------------
    print("\n" + "=" * 78)
    print("  GLOBAL MEAN (all scenarios)")
    header = f"    {'controller':<10}" + "".join(f"{lab:>10}" for _, lab, _ in KPIS)
    print(header)
    for mode in modes:
        cells = []
        for k, _, u in KPIS:
            per_scen = [avg(w, mode, k) for w in which_list if (w, mode) in raw]
            per_scen = [v for v in per_scen if v is not None]
            cells.append(_fmt(k, u, statistics.fmean(per_scen) if per_scen else None))
        print(f"    {MODE_LABEL[mode]:<10}" + "".join(f"{c:>10}" for c in cells))
    print("=" * 78)

    # ---- uncertainty (± std dev) when there are several repetitions -------
    if args.repeats > 1:
        print(f"\n  UNCERTAINTY  (mean ± std dev over {args.repeats} repetitions)")
        print(f"    {'scenario':<22}{'controller':<10}{'PDR':>16}{'compr':>16}")
        for which in which_list:
            for mode in modes:
                if (which, mode) not in raw:
                    continue
                pm, ps = avg(which, mode, "packet_delivery_ratio"), std(which, mode, "packet_delivery_ratio")
                cm, cs = avg(which, mode, "compression_ratio"), std(which, mode, "compression_ratio")
                print(f"    {SCEN_NAMES.get(which,'?'):<22}{MODE_LABEL[mode]:<10}"
                      f"{pm*100:8.1f}% ±{ps*100:4.1f}{cm:9.2f}x ±{cs:4.2f}")
        print("=" * 78)

    print(f"  Completed in {time.time()-t0:.0f}s")

    # ---- optional CSV -----------------------------------------------------
    if args.out:
        outp = args.out if os.path.isabs(args.out) else os.path.join(_ROOT, args.out)
        os.makedirs(os.path.dirname(outp) or ".", exist_ok=True)
        with open(outp, "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=["scenario", "mode", "rep"]
                               + [k for k, _, _ in KPIS])
            w.writeheader()
            w.writerows(csv_rows)
        print(f"  Raw CSV saved to {outp}")


if __name__ == "__main__":
    main()
