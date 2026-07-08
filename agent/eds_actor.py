#!/usr/bin/env python3
"""
eds_actor.py — Actor MAPPO (Fase 3) in Python puro, per il deploy sull'emulatore.

Carica il checkpoint JSON prodotto da examples/train_mappo.py del simulatore
(formato "eds-mappo-v1") e ne calcola il forward pass SENZA numpy/torch:
solo libreria standard. Cosi' la policy appresa nel simulatore puo' pilotare
la macchina di stato dell'emulatore sulla rete vera (documento MAPPO,
Tabella 10, riga "Deploy emulatore").

Architettura (identica a simulator/marl/networks.py, doc Tabella 5):
    Input(7) → LayerNorm(non-affine) → Linear(7→64) → Tanh
             → Linear(64→64) → Tanh → Linear(64→3) → argmax

Le azioni sono {0: ESCALATE, 1: MAINTAIN, 2: DE-ESCALATE} (doc Tabella 8).

Il forward pass e' verificato per parita' numerica contro l'Actor NumPy del
simulatore (vedi tools/check_actor_parity.py).
"""
from __future__ import annotations

import json
import math

_LN_EPS = 1e-5

ESCALATE, MAINTAIN, DEESCALATE = 0, 1, 2


def _matvec(vec, mat, out_dim):
    """vec (len k) · mat (k righe × out_dim colonne) → lista out_dim."""
    out = [0.0] * out_dim
    for i, v in enumerate(vec):
        if v == 0.0:
            continue
        row = mat[i]
        for j in range(out_dim):
            out[j] += v * row[j]
    return out


def _layernorm(x):
    """LayerNorm non-affine: (x - media) / sqrt(varianza_popolazione + eps)."""
    n = len(x)
    mu = sum(x) / n
    var = sum((v - mu) ** 2 for v in x) / n
    inv = 1.0 / math.sqrt(var + _LN_EPS)
    return [(v - mu) * inv for v in x]


class Actor:
    """
    Policy π(a|o) in Python puro. Carica i pesi da un checkpoint JSON e
    seleziona l'azione in modalita' deterministica (argmax), coerente con la
    valutazione/deploy del documento (Tabella 10).
    """

    OBS_DIM = 7
    N_ACTIONS = 3

    def __init__(self, params: dict):
        # params: dict con W1(7×64) b1(64) W2(64×64) b2(64) W3(64×3) b3(3)
        self.W1 = params["W1"]; self.b1 = params["b1"]
        self.W2 = params["W2"]; self.b2 = params["b2"]
        self.W3 = params["W3"]; self.b3 = params["b3"]
        self.h1 = len(self.b1)
        self.h2 = len(self.b2)
        self.n_out = len(self.b3)

    @classmethod
    def from_checkpoint(cls, path: str) -> "Actor":
        with open(path) as fh:
            blob = json.load(fh)
        if "actor" not in blob:
            raise ValueError(f"{path}: non e' un checkpoint MAPPO (manca 'actor').")
        params = {k: v for k, v in blob["actor"].items()}
        act = cls(params)
        act.meta = blob.get("meta", {})
        return act

    def logits(self, obs):
        """obs: lista di 7 float → logits (lista di 3 float)."""
        if len(obs) != self.OBS_DIM:
            raise ValueError(f"osservazione a {len(obs)} dim, attese {self.OBS_DIM}")
        xhat = _layernorm(obs)
        z1 = _matvec(xhat, self.W1, self.h1)
        h1 = [b + z for b, z in zip(self.b1, z1)]
        h1 = [math.tanh(v) for v in h1]
        z2 = _matvec(h1, self.W2, self.h2)
        h2 = [b + z for b, z in zip(self.b2, z2)]
        h2 = [math.tanh(v) for v in h2]
        z3 = _matvec(h2, self.W3, self.n_out)
        return [b + z for b, z in zip(self.b3, z3)]

    def probs(self, obs):
        """Softmax stabile sui logits (per logging/diagnostica)."""
        lg = self.logits(obs)
        m = max(lg)
        e = [math.exp(v - m) for v in lg]
        s = sum(e)
        return [v / s for v in e]

    def act(self, obs) -> int:
        """Azione deterministica: argmax dei logits (== argmax softmax)."""
        lg = self.logits(obs)
        best_i, best_v = 0, lg[0]
        for i in range(1, len(lg)):
            if lg[i] > best_v:
                best_i, best_v = i, lg[i]
        return best_i
