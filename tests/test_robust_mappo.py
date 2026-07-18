import os
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "agent"))
sys.path.insert(0, os.path.join(ROOT, "emulator"))

import eds_emulator  # noqa: E402
from eds_actor import Actor, ESCALATE, MAINTAIN  # noqa: E402
from eds_emulator import mappo_action_mask, parse_qdisc_stats  # noqa: E402

# Pressione di perdita oltre soglia: isola i test del dwell dal guardrail
# anti-compressione (che altrimenti bloccherebbe l'ESCALATE a drop_rate=0).
_LOSS = eds_emulator.MAPPO_GATE_DROP_RATE + 0.1


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
        # Dwell soddisfatto e pressione di perdita presente: tutto permesso.
        self.assertEqual(mappo_action_mask(3.0, 0.0, 3.0, 1.0, drop_rate=_LOSS),
                         [True, True, True])

    def test_old_checkpoint_semantics_have_no_dwell(self):
        # min_state_dwell=0 -> il dwell non congela nulla. Passo pressione di
        # perdita per isolare questo dal guardrail (testato separatamente sotto).
        self.assertEqual(mappo_action_mask(0.1, 0.0, 0.0, 0.20, drop_rate=_LOSS),
                         [True, True, True])


    def test_qdisc_parent_child_drop_is_not_double_counted(self):
        output = """qdisc tbf 1: root refcnt 2 rate 10Mbit
 Sent 100000 bytes 1000 pkt (dropped 200, overlimits 0 requeues 0)
 backlog 1000b 10p requeues 0
qdisc netem 10: parent 1:1 limit 40 delay 5ms
 Sent 100000 bytes 1000 pkt (dropped 200, overlimits 0 requeues 0)
 backlog 1000b 10p requeues 0
"""
        stats = parse_qdisc_stats(output, queue_limit=40)
        self.assertEqual(stats["dropped"], 200)
        self.assertEqual(stats["sent_pkts"], 1000)
        self.assertEqual(stats["backlog_pkts"], 10)
        self.assertEqual(stats["occupancy"], 0.25)


class TestCompressionGuardrail(unittest.TestCase):
    """Guardrail anti-compressione-a-vuoto: ESCALATE solo sotto pressione."""

    def test_blocks_escalation_without_loss(self):
        # Scenario 2: dwell soddisfatto, coda occupata ma senza drop -> niente
        # compressione (solo MAINTAIN/DEESCALATE consentiti).
        self.assertEqual(mappo_action_mask(3.0, 0.0, 3.0, 0.65, drop_rate=0.0),
                         [False, True, True])

    def test_allows_escalation_under_loss(self):
        # Scenario 1/5: perdita reale -> ESCALATE consentito.
        self.assertEqual(mappo_action_mask(3.0, 0.0, 3.0, 0.65, drop_rate=0.2),
                         [True, True, True])

    def test_allows_escalation_on_emergency_occupancy(self):
        # Coda quasi piena (>=95%) anche senza drop ancora contati -> consentito.
        self.assertEqual(mappo_action_mask(3.0, 0.0, 3.0, 0.96, drop_rate=0.0),
                         [True, True, True])

    def test_gate_can_be_disabled(self):
        # EDS_MAPPO_GATE=0: comportamento robusto originale (tutto permesso).
        original = eds_emulator.MAPPO_COMPRESSION_GATE
        eds_emulator.MAPPO_COMPRESSION_GATE = False
        try:
            self.assertEqual(mappo_action_mask(3.0, 0.0, 3.0, 0.65, drop_rate=0.0),
                             [True, True, True])
        finally:
            eds_emulator.MAPPO_COMPRESSION_GATE = original


if __name__ == "__main__":
    unittest.main()
