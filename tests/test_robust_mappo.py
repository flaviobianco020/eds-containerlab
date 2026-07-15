import os
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "agent"))
sys.path.insert(0, os.path.join(ROOT, "emulator"))

from eds_actor import Actor, ESCALATE, MAINTAIN  # noqa: E402
from eds_emulator import mappo_action_mask  # noqa: E402


def _actor_with_fixed_logits():
    return Actor({
        "W1": [[0.0] * 64 for _ in range(7)], "b1": [0.0] * 64,
        "W2": [[0.0] * 64 for _ in range(64)], "b2": [0.0] * 64,
        "W3": [[0.0] * 3 for _ in range(64)], "b3": [3.0, 2.0, 1.0],
    })


class TestRobustMappoDeploy(unittest.TestCase):
    def test_actor_respects_mask(self):
        actor = _actor_with_fixed_logits()
        obs = [0.0] * 7
        self.assertEqual(actor.act(obs), ESCALATE)
        self.assertEqual(actor.act(obs, [False, True, False]), MAINTAIN)
        self.assertEqual(actor.probs(obs, [False, True, False]), [0.0, 1.0, 0.0])

    def test_dwell_allows_only_maintain(self):
        self.assertEqual(mappo_action_mask(2.0, 0.0, 3.0, 0.50),
                         [False, True, False])

    def test_emergency_can_escalate_during_dwell(self):
        self.assertEqual(mappo_action_mask(2.0, 0.0, 3.0, 0.95),
                         [True, True, False])

    def test_all_actions_after_dwell(self):
        self.assertEqual(mappo_action_mask(3.0, 0.0, 3.0, 1.0),
                         [True, True, True])

    def test_old_checkpoint_semantics_have_no_dwell(self):
        self.assertEqual(mappo_action_mask(0.1, 0.0, 0.0, 0.20),
                         [True, True, True])


if __name__ == "__main__":
    unittest.main()
