#!/usr/bin/env python3
"""
trace_diff.py - Confronta due tracce `eds-mappo-observation-trace-v1`
(tipicamente simulatore vs emulatore) feature per feature.

Le due tracce hanno linee temporali diverse (il simulatore gira a ~13 pkt/s,
l'emulatore a ~7000), quindi NON si confrontano riga per riga: si confrontano
le DISTRIBUZIONI di ciascuna feature (media, dev. std, range). L'obiettivo e'
quantificare lo scarto sim-to-real dell'osservazione, e in particolare vedere
se le due feature APPROSSIMATE sull'emulatore (high_priority_ratio,
low_priority_ratio) divergono piu' delle altre — cioe' se l'approssimazione
"mix offerto invece del backlog reale" costa davvero qualcosa.

Uso:
    python3 examples/trace_diff.py <trace_sim.json> <trace_emu.json>
"""
import json
import statistics
import sys

# feature stimate sull'emulatore (mix offerto invece del backlog in coda)
APPROX_FEATURES = {"high_priority_ratio", "low_priority_ratio"}


def _load(path):
    with open(path) as fh:
        blob = json.load(fh)
    if blob.get("schema") != "eds-mappo-observation-trace-v1":
        print(f"ATTENZIONE: {path} non ha lo schema atteso")
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
        print("Uso: trace_diff.py <trace_sim.json> <trace_emu.json>")
        sys.exit(1)
    a_blob, a_feats, a_cols = _load(sys.argv[1])
    b_blob, b_feats, b_cols = _load(sys.argv[2])
    feats = [f for f in a_feats if f in b_feats]

    a_src = a_blob.get("source", "A")
    b_src = b_blob.get("source", "B")
    print("=" * 80)
    print("  CONFRONTO TRACCE OSSERVAZIONE  (distribuzioni per feature)")
    print(f"  A = {a_src:<10} {len(a_blob['rows'])} passi   |   "
          f"B = {b_src:<10} {len(b_blob['rows'])} passi")
    print(f"  ▸ le feature contrassegnate * sono APPROSSIMATE sull'emulatore")
    print("=" * 80)
    print(f"  {'feature':<22}{'media A':>9}{'media B':>9}{'Δmedia':>9}"
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
    print("  Feature ordinate per |Δmedia| (dove i due mondi divergono di piu'):")
    for absd, f, am, bm, dm, *_ in sorted(diffs, reverse=True):
        star = " (approssimata)" if f in APPROX_FEATURES else ""
        print(f"    {f:<22} |Δ|={absd:.3f}{star}")

    approx = [d for d in diffs if d[1] in APPROX_FEATURES]
    other = [d for d in diffs if d[1] not in APPROX_FEATURES]
    if approx and other:
        ma = statistics.fmean(d[0] for d in approx)
        mo = statistics.fmean(d[0] for d in other)
        print("=" * 80)
        print(f"  |Δmedia| medio feature APPROSSIMATE ...... {ma:.3f}")
        print(f"  |Δmedia| medio altre feature ............ {mo:.3f}")
        if ma <= mo + 0.02:
            print("  → le feature approssimate NON divergono piu' delle altre:")
            print("    l'approssimazione 'mix offerto' regge, il gap resta fisico.")
        else:
            print("  → le feature approssimate divergono di piu':")
            print("    l'osservazione contribuisce al gap, il fix per-priorita' avrebbe senso.")
    print("=" * 80)


if __name__ == "__main__":
    main()
