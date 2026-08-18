#!/usr/bin/env python3
"""
trace_diff.py - Compares two `eds-mappo-observation-trace-v1` traces
(typically simulator vs emulator) feature by feature.

The two traces have different timelines (the simulator runs at ~13 pkt/s, the
emulator at ~7000), so they are NOT compared row by row: the DISTRIBUTIONS of
each feature are compared (mean, std dev, range). The goal is to quantify the
sim-to-real gap of the observation, and in particular to see whether the two
features APPROXIMATED on the emulator (high_priority_ratio, low_priority_ratio)
diverge more than the others — that is, whether the approximation "offered mix
instead of the real backlog" actually costs anything.

Usage:
    python3 examples/trace_diff.py <trace_sim.json> <trace_emu.json>
"""
import json
import statistics
import sys

# features estimated on the emulator (offered mix instead of the queued backlog)
APPROX_FEATURES = {"high_priority_ratio", "low_priority_ratio"}


def _load(path):
    with open(path) as fh:
        blob = json.load(fh)
    if blob.get("schema") != "eds-mappo-observation-trace-v1":
        print(f"WARNING: {path} does not have the expected schema")
    feats = blob["features"]
    cols = {f: [] for f in feats}
    for row in blob["rows"]:
        for f in feats:
            cols[f].append(float(row["observation"][f]))
    return blob, feats, cols


def _stats(v):
    if not v:
        return (0.0, 0.0, 0.0, 0.0)
    return (statistics.fmean(v), statistics.pstdev(v) if len(v) > 1 else 0.0,
            min(v), max(v))


def main():
    if len(sys.argv) < 3:
        print("Usage: trace_diff.py <trace_sim.json> <trace_emu.json>")
        sys.exit(1)
    a_blob, a_feats, a_cols = _load(sys.argv[1])
    b_blob, b_feats, b_cols = _load(sys.argv[2])
    feats = [f for f in a_feats if f in b_feats]

    a_src = a_blob.get("source", "A")
    b_src = b_blob.get("source", "B")
    print("=" * 80)
    print("  OBSERVATION TRACE COMPARISON  (per-feature distributions)")
    print(f"  A = {a_src:<10} {len(a_blob['rows'])} steps   |   "
          f"B = {b_src:<10} {len(b_blob['rows'])} steps")
    print(f"  ▸ the features marked * are APPROXIMATED on the emulator")
    print("=" * 80)
    print(f"  {'feature':<22}{'mean A':>9}{'mean B':>9}{'Δmean':>9}"
          f"{'std A':>8}{'std B':>8}")

    diffs = []
    for f in feats:
        am, asd, *_ = _stats(a_cols[f])
        bm, bsd, *_ = _stats(b_cols[f])
        dm = bm - am
        diffs.append((abs(dm), f, am, bm, dm, asd, bsd))
        star = "*" if f in APPROX_FEATURES else " "
        print(f" {star}{f:<22}{am:9.3f}{bm:9.3f}{dm:+9.3f}{asd:8.3f}{bsd:8.3f}")

    print("-" * 80)
    print("  Features sorted by |Δmean| (where the two worlds diverge most):")
    for absd, f, am, bm, dm, *_ in sorted(diffs, reverse=True):
        star = " (approximated)" if f in APPROX_FEATURES else ""
        print(f"    {f:<22} |Δ|={absd:.3f}{star}")

    approx = [d for d in diffs if d[1] in APPROX_FEATURES]
    other = [d for d in diffs if d[1] not in APPROX_FEATURES]
    if approx and other:
        ma = statistics.fmean(d[0] for d in approx)
        mo = statistics.fmean(d[0] for d in other)
        print("=" * 80)
        print(f"  mean |Δmean| of APPROXIMATED features .... {ma:.3f}")
        print(f"  mean |Δmean| of other features .......... {mo:.3f}")
        if ma <= mo + 0.02:
            print("  → the approximated features do NOT diverge more than the others:")
            print("    the 'offered mix' approximation holds, the gap stays physical.")
        else:
            print("  → the approximated features diverge more:")
            print("    the observation contributes to the gap, the per-priority fix would make sense.")
    print("=" * 80)


if __name__ == "__main__":
    main()
