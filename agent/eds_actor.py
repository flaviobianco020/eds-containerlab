#!/usr/bin/env python3
"""
eds_actor.py — MAPPO Actor (Phase 3) in pure Python, for deployment on the emulator.

Loads the JSON checkpoint produced by the simulator's examples/train_mappo.py
(format "eds-mappo-v1") and computes its forward pass WITHOUT numpy/torch:
standard library only. This way the policy learned in the simulator can drive
the emulator's state machine on the real network (MAPPO document, Table 10,
row "Emulator deploy").

Architecture (identical to simulator/marl/networks.py, doc Table 5):
    Input(7) → LayerNorm(non-affine) → Linear(7→64) → Tanh
             → Linear(64→64) → Tanh → Linear(64→3) → argmax

The actions are {0: ESCALATE, 1: MAINTAIN, 2: DE-ESCALATE} (doc Table 8).

The forward pass is verified for numerical parity against the simulator's NumPy
Actor (see tools/check_actor_parity.py).
"""
from __future__ import annotations

import json
import math

_LN_EPS = 1e-5

ESCALATE, MAINTAIN, DEESCALATE = 0, 1, 2


def _matvec(vec, mat, out_dim):
    """vec (len k) · mat (k rows × out_dim columns) → list of out_dim."""
    out = [0.0] * out_dim
    for i, v in enumerate(vec):
        if v == 0.0:
            continue
        row = mat[i]
        for j in range(out_dim):
            out[j] += v * row[j]
    return out


def _layernorm(x):
    """Non-affine LayerNorm: (x - mean) / sqrt(population_variance + eps)."""
    n = len(x)
    mu = sum(x) / n
    var = sum((v - mu) ** 2 for v in x) / n
    inv = 1.0 / math.sqrt(var + _LN_EPS)
    return [(v - mu) * inv for v in x]


class Actor:
    """
    Policy π(a|o) in pure Python. Loads the weights from a JSON checkpoint and
    selects the action deterministically (argmax), consistent with the
    evaluation/deploy of the document (Table 10).
    """

    OBS_DIM = 7
    N_ACTIONS = 3

    def __init__(self, params: dict):
        # params: dict with W1(7×64) b1(64) W2(64×64) b2(64) W3(64×3) b3(3)
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
            raise ValueError(f"{path}: not a MAPPO checkpoint (missing 'actor').")
        params = {k: v for k, v in blob["actor"].items()}
        act = cls(params)
        act.meta = blob.get("meta", {})
        return act

    def logits(self, obs):
        """obs: list of 7 floats → logits (list of 3 floats)."""
        if len(obs) != self.OBS_DIM:
            raise ValueError(f"observation has {len(obs)} dims, expected {self.OBS_DIM}")
        xhat = _layernorm(obs)
        z1 = _matvec(xhat, self.W1, self.h1)
        h1 = [b + z for b, z in zip(self.b1, z1)]
        h1 = [math.tanh(v) for v in h1]
        z2 = _matvec(h1, self.W2, self.h2)
        h2 = [b + z for b, z in zip(self.b2, z2)]
        h2 = [math.tanh(v) for v in h2]
        z3 = _matvec(h2, self.W3, self.n_out)
        return [b + z for b, z in zip(self.b3, z3)]

    @staticmethod
    def _masked_logits(logits, action_mask=None):
        if action_mask is None:
            return logits
        if len(action_mask) != len(logits):
            raise ValueError("action_mask incompatible with the number of actions")
        if not any(action_mask):
            raise ValueError("action_mask must allow at least one action")
        return [v if allowed else -1e30
                for v, allowed in zip(logits, action_mask)]

    def probs(self, obs, action_mask=None):
        """Stable softmax, optionally restricted to the allowed actions."""
        lg = self._masked_logits(self.logits(obs), action_mask)
        m = max(lg)
        e = [math.exp(v - m) for v in lg]
        s = sum(e)
        return [v / s for v in e]

    def act(self, obs, action_mask=None) -> int:
        """Deterministic action: argmax of the logits (== argmax softmax)."""
        lg = self._masked_logits(self.logits(obs), action_mask)
        best_i, best_v = 0, lg[0]
        for i in range(1, len(lg)):
            if lg[i] > best_v:
                best_i, best_v = i, lg[i]
        return best_i
