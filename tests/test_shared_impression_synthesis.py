"""Subjective projection boundaries; Moment/packet tests own source eligibility."""
from dataclasses import replace
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import bnl_shared_brain_synthesis as synthesis
from bnl_unified_intelligence_packet import IntelligencePacketItem
from tests import test_ordinary_chat_single_packet_canary as ordinary_fixture


class SharedImpressionSynthesisTests(unittest.TestCase):
    def setUp(self):
        self.fixture = ordinary_fixture.OrdinaryChatSinglePacketCanaryTests("runTest")
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        self.companion = IntelligencePacketItem(
            lane="moment", source_class="moment_gist", source_type="impression_moment_context",
            source_ref="moment:test-impression", source_digest="companion-digest",
            subject_key="discord_user:7", predicate_key="shared_moment",
            text="Test Member connected modular synths to the archive project.",
            visibility="public", confidence="medium", lifecycle="active", authority=2,
            usage="episodic_gist", observed_at="2026-08-10T12:00:00+00:00",
            revalidation_kind="impression", revalidation_key="test-impression", event_ref="test-impression",
        )
        self.impression = IntelligencePacketItem(
            lane="bnl_impression", source_class="derived_summary", source_type="bnl_retained_impression",
            source_ref="impression:test-impression", source_digest="impression-digest",
            subject_key="bnl_01", predicate_key="bnl_impression",
            text="I liked the unfinished archive project; it made me curious about what could grow around it.",
            visibility="public", confidence="medium", lifecycle="active", authority=0,
            usage="subjective_perspective", observed_at="2026-08-10T12:00:00+00:00",
            revalidation_kind="impression", revalidation_key="test-impression", event_ref="test-impression",
            attribution_mode="bnl_subjective", uncertainty_status="revisable_impression_zero_fact_weight",
        )

    def basis(self, impression=None):
        packet = replace(self.fixture.packet, items=(
            *self.fixture.packet.items, self.companion, impression or self.impression,
        ))
        rendered, lanes, count, digests = synthesis.render_packet_context(packet, profile_expression=False)
        return replace(self.fixture.basis, packet=packet, rendered_context=rendered,
                       rendered_lane_counts=lanes, rendered_item_count=count,
                       rendered_source_digests=digests,
                       rendered_evidence_refs=synthesis._ordinary_rendered_evidence_refs(packet, digests))

    def test_retained_perspective_renders_with_original_context_and_its_original_date(self):
        basis = self.basis()
        self.assertIn(self.companion.text, basis.rendered_context)
        self.assertIn(self.impression.text, basis.rendered_context)
        self.assertIn("BNL's retained impression; BNL's revisable perspective", basis.rendered_context)
        self.assertIn("zero independent fact or recurrence weight", basis.rendered_context)
        self.assertIn("source exchange last activity 2026-08-10T05:00:00-07:00", basis.rendered_context)
        self.assertIn("Original evidence and current corrections prevail", basis.rendered_context)
        self.assertIn("no callback or repeated wording is required", basis.rendered_context)
        self.assertIn(self.impression.source_digest, basis.rendered_source_digests)
        self.assertIn(self.companion.source_digest, basis.rendered_source_digests)
        self.assertLess(basis.rendered_context.index(self.companion.text),
                        basis.rendered_context.index(self.impression.text))

    def test_factual_task_support_uses_originals_never_the_retained_impression(self):
        basis = self.basis()
        impression_ids = {ref[0] for ref in basis.rendered_evidence_refs if ref[1] == "bnl_impression"}
        self.assertTrue(impression_ids)
        plans = synthesis.ordinary_chat_task_support_plan(basis)
        self.assertTrue(plans)
        self.assertTrue(any(plan.evidence_ids for plan in plans))
        self.assertFalse(impression_ids & {ref for plan in plans for ref in plan.evidence_ids})
        self.assertNotIn("bnl_impression", synthesis._ordinary_task_allowed_lanes(
            SimpleNamespace(object_kind="unknown", subject_requirement="not_applicable")))
        segments = synthesis._ordinary_chat_authorized_support_segments(basis)
        self.assertTrue(any(self.companion.text in text for text, _subject, _lane in segments))
        self.assertFalse(any(lane == "bnl_impression" for _text, _subject, lane in segments))
        self.assertFalse(any(self.impression.text in text for text, _subject, _lane in segments))

    def test_impression_cannot_launder_a_concrete_member_claim_into_authorized_support(self):
        claim = "You were born in 1999."
        impression = replace(self.impression, text=claim)
        basis = self.basis(impression)
        self.assertIn(claim, basis.rendered_context)
        self.assertEqual(synthesis._item_evidence_text(impression), "")
        classifications, unsupported = synthesis.audit_ordinary_chat_candidate_claims(basis, claim)
        self.assertEqual(unsupported, 1)
        self.assertIn("unsupported_packet_domain", classifications)

    def test_actual_original_evidence_remains_usable_with_subjective_context(self):
        basis = self.basis()
        classifications, unsupported = synthesis.audit_ordinary_chat_candidate_claims(
            basis, "You keep connecting modular synths to the archive project.")
        self.assertEqual(unsupported, 0)
        self.assertNotIn("unsupported_packet_domain", classifications)

    def test_impression_adds_no_member_profile_points_even_if_put_in_validation_pool(self):
        response = "You keep connecting modular synths to the archive project."
        baseline = synthesis.candidate_profile_coverage(self.fixture.basis, response)
        basis = self.basis(replace(self.impression, text=response))
        basis = replace(basis, packet=replace(
            basis.packet, validation_items=(*basis.packet.validation_items, basis.packet.items[-1])))
        with_impression = synthesis.candidate_profile_coverage(basis, response)
        self.assertEqual(with_impression.covered_member_point_identities,
                         baseline.covered_member_point_identities)
        self.assertEqual(with_impression.covered_member_detail_point_identities,
                         baseline.covered_member_detail_point_identities)

    def test_absent_impression_adds_no_instruction_or_prompt_lane(self):
        rendered, lanes, _count, _digests = synthesis.render_packet_context(
            self.fixture.packet, profile_expression=False)
        self.assertNotIn("bnl_impression", dict(lanes))
        self.assertNotIn("retained impression", rendered)
        self.assertEqual(rendered, self.fixture.basis.rendered_context)

    def test_canon_budget_cannot_render_short_impression_after_skipping_its_context(self):
        companion = replace(self.companion, text="The member described the archive's unfinished wiring. " * 7)
        impression = replace(self.impression, text="I remained curious.")
        canon = replace(self.companion, lane="canon", source_type="recognized_canon_fact",
                        source_ref="canon:test", source_digest="canon-digest",
                        text="Test Member is a BARCODE artist.", observed_at="")
        packet = replace(self.fixture.packet, items=(companion, impression, canon))
        with patch.object(synthesis, "_profile_requires_canon", return_value=True):
            for max_chars in (340, 400, 450):
                with self.subTest(max_chars=max_chars):
                    rendered, lanes, _count, digests = synthesis.render_packet_context(
                        packet, max_items=3, max_chars=max_chars, profile_expression=False)
                    self.assertIn(canon.text, rendered)
                    self.assertNotIn(companion.source_digest, digests)
                    self.assertNotIn(impression.source_digest, digests)
                    self.assertNotIn("bnl_impression", dict(lanes))

    def test_impression_without_renderable_companion_is_not_projected(self):
        packet = replace(self.fixture.packet, items=(self.impression,))
        self.assertEqual(synthesis.render_packet_context(packet), ("", (), 0, ()))


if __name__ == "__main__":
    unittest.main()
