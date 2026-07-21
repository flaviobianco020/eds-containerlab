#!/usr/bin/env python3
"""
benchmark.py — Confronto automatico dei tre controller SULL'EMULATORE reale.

Esegue, sullo stesso ferro ContainerLab, ognuno degli scenari canonici sotto:

    Fase 1  — transizioni istantanee sull'occupancy
    Fase 2  — EWMA + isteresi (eFRAC) + compressore NFQUEUE
    Fase 3  — policy MAPPO addestrata (checkpoint JSON del simulatore)

e produce una tabella comparativa dei KPI misurati (PDR, throughput, latenza,
occupancy, drop, transizioni, compression ratio). Media su piu' ripetizioni,
perche' l'emulatore ha rumore reale.

Prerequisito:  il lab deve essere gia' deployato almeno una volta
    ./deploy.sh single_bottleneck

Uso:
    # tutti gli scenari, i tre controller, 1 ripetizione, tempi dimezzati:
    python3 emulator/benchmark.py \
        --mappo-ckpt ../Event-Driven_Simulator/checkpoints/mappo_best_stab.json \
        --scale 0.5

    # solo scenari 1,3,5, due ripetizioni, salva CSV:
    python3 emulator/benchmark.py --scenarios 1,3,5 --repeats 2 \
        --mappo-ckpt ../Event-Driven_Simulator/checkpoints/mappo_best_stab.json \
        --out logs/benchmark.csv

Opzioni:
    --topo TOPO               single_bottleneck (default) | multi_hop | mesh
    --scenarios 1,2,3,4,5,6   quali scenari (default: tutti)
    --modes phase1,phase2,mappo   quali controller (default: tutti e tre)
    --mappo-ckpt PATH         checkpoint JSON (obbligatorio se 'mappo' e' fra i modi)
    --repeats N               ripetizioni per (scenario, modo), media (default 1)
    --scale K                 scala i tempi degli scenari (default 1.0)
    --no-redeploy             NON rieseguire deploy.sh prima di ogni run
    --out FILE                salva le righe grezze in CSV
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
    "7": "oscillante",
}
# mappo = checkpoint A (--mappo-ckpt), mappob = checkpoint B (--mappo-ckpt-b):
# permette il confronto diretto di due policy MAPPO nella stessa tabella (es.
# current vs ccgated), alternate sugli stessi redeploy per ridurre il rumore.
MODE_LABEL = {"phase1": "Fase 1", "phase2": "Fase 2",
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
    """Riesegue deploy.sh <topo> per azzerare lo stato tc fra i run."""
    try:
        r = subprocess.run(["./deploy.sh", topo], cwd=_ROOT,
                           capture_output=True, text=True, timeout=180)
        return r.returncode == 0
    except (subprocess.TimeoutExpired, OSError):
        return False


def _run_once(which: str, mode: str, scale: float, ckpts: dict, logpath: str):
    """Esegue uno scenario in una modalita', catturando l'output nel log.

    ckpts: mappa modo->path per i modi MAPPO ({"mappo": pathA, "mappob": pathB}).
    """
    fn = scen.SCENARIOS[which]
    kwargs = {"phase2": mode == "phase2",
              "mappo": ckpts.get(mode) if mode in MAPPO_MODES else None}
    try:
        with open(logpath, "w") as logf, contextlib.redirect_stdout(logf):
            return fn(scale, **kwargs)
    except Exception as exc:  # noqa: BLE001 — un fallimento non deve fermare lo sweep
        with open(logpath, "a") as logf:
            logf.write(f"\n[benchmark] ERRORE: {exc}\n")
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
    ap = argparse.ArgumentParser(description="Benchmark 3 controller sull'emulatore")
    ap.add_argument("--scenarios", default="1,2,3,4,5,6")
    ap.add_argument("--modes", default="phase1,phase2,mappo")
    ap.add_argument("--mappo-ckpt", default=None, help="checkpoint MAPPO A (modo 'mappo')")
    ap.add_argument("--mappo-ckpt-b", default=None, help="checkpoint MAPPO B (modo 'mappob')")
    ap.add_argument("--label-a", default=None, help="etichetta per il modo 'mappo' (default: nome file)")
    ap.add_argument("--label-b", default=None, help="etichetta per il modo 'mappob' (default: nome file)")
    ap.add_argument("--repeats", type=int, default=1)
    ap.add_argument("--scale", type=float, default=1.0)
    ap.add_argument("--topo", default="single_bottleneck",
                    choices=["single_bottleneck", "multi_hop", "mesh"],
                    help="topologia ContainerLab (default single_bottleneck)")
    ap.add_argument("--no-redeploy", action="store_true")
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    scen.TOPO = args.topo  # gli scenari leggono questo global per scegliere la topologia

    which_list = [s.strip() for s in args.scenarios.split(",") if s.strip()]
    modes = [m.strip() for m in args.modes.split(",") if m.strip()]
    for m in modes:
        if m not in MODE_LABEL:
            print(f"modo sconosciuto: {m} (validi: phase1, phase2, mappo, mappob)")
            sys.exit(1)
    # checkpoint per i due modi MAPPO (A = --mappo-ckpt, B = --mappo-ckpt-b)
    ckpts: dict[str, str] = {}
    for mode, arg, argname in (("mappo", args.mappo_ckpt, "--mappo-ckpt"),
                               ("mappob", args.mappo_ckpt_b, "--mappo-ckpt-b")):
        if mode in modes:
            if not arg:
                print(f"{argname} e' obbligatorio quando '{mode}' e' fra i modi.")
                sys.exit(1)
            if not os.path.exists(arg):
                print(f"checkpoint non trovato: {arg}")
                sys.exit(1)
            ckpts[mode] = os.path.abspath(arg)

    # etichette leggibili per la tabella (default: nome file senza mappo_/.json)
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
    print("  BENCHMARK controller sull'emulatore ContainerLab")
    print(f"  topo={args.topo}  scenari={','.join(which_list)}  modi={','.join(modes)}  "
          f"ripetizioni={args.repeats}  scale={args.scale}")
    print(f"  run totali: {n_runs}  (log dettagliati in logs/bench_*.log)")
    print("=" * 78)

    # raw[(scenario, mode)] = lista di dict-summary (una per ripetizione)
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
                        print(f"  [{run_i}/{n_runs}] {tag}: deploy FALLITO, salto")
                        continue
                print(f"  [{run_i}/{n_runs}] scenario {which} · {MODE_LABEL[mode]} "
                      f"· rep {rep+1} ...", end="", flush=True)
                logpath = os.path.join(logdir, f"bench_{tag}.log")
                summ = _run_once(which, mode, args.scale, ckpts, logpath)
                if summ is None:
                    print(" ERRORE (vedi log)")
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

    # ---- media e deviazione standard per (scenario, modo) ----------------
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
    print(f"  RISULTATI  (media su {args.repeats} ripetizione/i)")
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

    # ---- media globale (su tutti gli scenari) ----------------------------
    print("\n" + "=" * 78)
    print("  MEDIA GLOBALE (tutti gli scenari)")
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

    # ---- incertezza (± dev. std) quando ci sono piu' ripetizioni ----------
    if args.repeats > 1:
        print(f"\n  INCERTEZZA  (media ± dev. std su {args.repeats} ripetizioni)")
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

    print(f"  Completato in {time.time()-t0:.0f}s")

    # ---- CSV opzionale ----------------------------------------------------
    if args.out:
        outp = args.out if os.path.isabs(args.out) else os.path.join(_ROOT, args.out)
        os.makedirs(os.path.dirname(outp) or ".", exist_ok=True)
        with open(outp, "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=["scenario", "mode", "rep"]
                               + [k for k, _, _ in KPIS])
            w.writeheader()
            w.writerows(csv_rows)
        print(f"  CSV grezzo salvato in {outp}")


if __name__ == "__main__":
    main()
