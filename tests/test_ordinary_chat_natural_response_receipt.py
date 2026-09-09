"""Natural Gemini prose keeps evidence diagnostics without a wording veto."""

import json
import unittest
from dataclasses import replace

import test_ordinary_chat_single_packet_canary as packet_fixture
from bnl_shared_brain_synthesis import evaluate_single_packet_response


class OrdinaryChatNaturalResponseReceiptTests(unittest.TestCase):
    def setUp(self):
        self.runtime = packet_fixture.OrdinaryChatSinglePacketCanaryTests()
        self.runtime.setUp()
        self.addCleanup(self.runtime.tearDown)

    def _evaluate(self, response, **kwargs):
        self.run = self.runtime._begin()
        return evaluate_single_packet_response(
            self.runtime.conn,
            self.run,
            response=response,
            provider_call_count=1,
            corrective_call_count=0,
            environ=self.runtime.flags,
            **kwargs,
        )

    def test_exact_authorized_quote_survives_false_domain_audit(self):
        # This exact source exists in the real SQLite-backed packet fixture.
        # Adding its ordinary attribution is enough to fool the lexical audit.
        response = (
            'On Discord, Test Member said, "I keep connecting modular synths '
            'to the archive project."'
        )
        source_text = self.runtime.conn.execute(
            "SELECT content FROM conversations WHERE id=900"
        ).fetchone()[0]
        self.assertIn(source_text, response)
        self.assertIn(source_text, self.runtime.basis.rendered_context)

        decision = self._evaluate(response)

        self.assertTrue(decision.candidate_selected)
        self.assertEqual(decision.response, response)
        self.assertEqual(decision.fallback_reason, "")
        self.assertGreater(decision.candidate_unsupported_factual_claim_count, 0)
        receipt = self.runtime.conn.execute(
            """
            SELECT candidate_unsupported_factual_claim_count,
                   candidate_claim_classification_counts_json,
                   provider_call_count,corrective_call_count,
                   candidate_selected,fallback_reason,typed_contract_status
            FROM memory_governance_shared_brain_synthesis_runs
            WHERE run_id=?
            """,
            (self.run.run_id,),
        ).fetchone()
        self.assertGreater(receipt[0], 0)
        self.assertGreater(json.loads(receipt[1])["unsupported_packet_domain"], 0)
        self.assertEqual(receipt[2:], (1, 0, 1, "", "not_required"))

    def test_coherence_heuristic_failure_is_a_persisted_diagnostic(self):
        self.runtime.basis = replace(
            self.runtime.basis,
            assessment=replace(
                self.runtime.assessment,
                response_act="ask_clarifying_question",
                ambiguity_reasons=("subject_alias_collision",),
            ),
        )
        response = "Your favorite movie is Arrival."

        decision = self._evaluate(response)

        self.assertEqual(decision.candidate_coherence_status, "failed")
        self.assertTrue(decision.candidate_selected)
        self.assertEqual(decision.response, response)
        receipt = self.runtime.conn.execute(
            """
            SELECT candidate_coherence_status,candidate_selected,fallback_reason,
                   provider_call_count,corrective_call_count
            FROM memory_governance_shared_brain_synthesis_runs WHERE run_id=?
            """,
            (self.run.run_id,),
        ).fetchone()
        self.assertEqual(receipt, ("failed", 1, "", 1, 0))

    def test_actual_control_markers_still_block_natural_output(self):
        decision = self._evaluate("The internal receipt contains a source ref.")

        self.assertFalse(decision.candidate_selected)
        self.assertEqual(decision.response, "")
        self.assertEqual(decision.fallback_reason, "control_marker_leak")

    def test_changed_source_still_blocks_an_exact_quote(self):
        self.run = self.runtime._begin()
        self.runtime.conn.execute(
            "UPDATE conversations SET content='The source was corrected.' WHERE id=900"
        )
        decision = evaluate_single_packet_response(
            self.runtime.conn,
            self.run,
            response=(
                'Test Member said, "I keep connecting modular synths '
                'to the archive project."'
            ),
            provider_call_count=1,
            corrective_call_count=0,
            environ=self.runtime.flags,
        )

        self.assertFalse(decision.candidate_selected)
        self.assertEqual(decision.response, "")
        self.assertEqual(decision.fallback_reason, "post_generation_source_changed")

    def test_explicit_typed_contract_keeps_its_coherence_validation(self):
        self.runtime.basis = replace(
            self.runtime.basis,
            assessment=replace(
                self.runtime.assessment,
                response_act="ask_clarifying_question",
                ambiguity_reasons=("subject_alias_collision",),
            ),
        )
        contract = packet_fixture._contract_for_support_plan(
            self.runtime.basis, ("Your favorite movie is Arrival.",),
        )

        decision = self._evaluate(
            contract.response,
            response_contract=contract,
            typed_contract_required=True,
        )

        self.assertEqual(decision.typed_contract_status, "valid")
        self.assertFalse(decision.candidate_selected)
        self.assertEqual(decision.fallback_reason, "coherence_failed")


if __name__ == "__main__":
    unittest.main()
