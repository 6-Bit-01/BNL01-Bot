import os
import sqlite3
import unittest
from unittest import mock

from bnl_canon_source_contract import Confidence, SourceClass, Visibility
import bnl_memory_ledger as ledger
import bnl_moment_engine as moments
import bnl_relationship_engine as relationships
from bnl_shadow_acceptance import (
    build_v2_shadow_acceptance_snapshot,
    render_v2_shadow_acceptance_lines,
)
from bnl_shared_brain_synthesis import (
    begin_run,
    build_basis,
    build_evaluation_report,
    configuration,
    ensure_schema,
    evaluate_candidate,
    finalize_run,
    render_packet_context,
    revalidate_basis,
    scope_enabled,
)
from bnl_unified_intelligence_packet import (
    IntelligencePacketRequest,
    PacketConversationEvidence,
    build_evaluation_report as build_packet_report,
    build_packet,
    mark_packet_application,
)
from bnl_unified_response_assessment import (
    build_unified_response_assessment,
)


class SharedBrainSynthesisCanaryTests(unittest.TestCase):
    def setUp(self):
        self.flags = {
            "BNL_MEMORY_LEDGER_SHADOW_ENABLED": "true",
            "BNL_MOMENT_ENGINE_SHADOW_ENABLED": "true",
            "BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED": "true",
            "BNL_RELATIONSHIP_V2_SHADOW_ENABLED": "true",
            "BNL_UNIFIED_RESPONSE_ASSESSMENT_SHADOW_ENABLED": "true",
            "BNL_UNIFIED_INTELLIGENCE_PACKET_SHADOW_ENABLED": "true",
            "BNL_SHARED_BRAIN_SYNTHESIS_CANARY_ENABLED": "true",
            "BNL_SHARED_BRAIN_SYNTHESIS_CANARY_GUILD_IDS": "1",
            "BNL_SHARED_BRAIN_SYNTHESIS_CANARY_USER_IDS": "7",
            "BNL_SHARED_BRAIN_SYNTHESIS_CANARY_CHANNEL_IDS": "10",
            "BNL_MEMORY_GOVERNANCE_LIVE_ENABLED": "false",
            "BNL_RELATIONSHIP_V2_LIVE_ENABLED": "false",
            "BNL_ACTIVE_ENGAGEMENT_V2_LIVE_ENABLED": "false",
        }
        self.env = mock.patch.dict(os.environ, self.flags, clear=False)
        self.env.start()
        self.conn = sqlite3.connect(":memory:")
        ledger.ensure_memory_ledger_schema(self.conn)
        moments.ensure_moment_schema(self.conn)
        relationships.ensure_relationship_v2_schema(self.conn)
        self.conn.execute(
            """
            CREATE TABLE conversations (
                id INTEGER PRIMARY KEY,
                guild_id INTEGER,
                user_id INTEGER,
                user_name TEXT,
                role TEXT,
                content TEXT,
                channel_id INTEGER,
                channel_policy TEXT,
                timestamp TEXT
            )
            """
        )
        self.conn.execute(
            """
            INSERT INTO conversations(
                id,guild_id,user_id,user_name,role,content,channel_id,
                channel_policy,timestamp
            ) VALUES(900,1,7,'Crow','user',?,10,'public_context',?)
            """,
            (
                "I keep connecting modular synths to the archive project.",
                "2026-07-25T12:00:00+00:00",
            ),
        )
        result = ledger.insert_ledger_entry(
            self.conn,
            ledger.LedgerEntry(
                guild_id=1,
                source_table="conversations",
                source_row_id=901,
                source_revision="901",
                source_role="member_self_report",
                entry_type="preference",
                subject_key="discord_user:7",
                subject_display_name="Crow",
                predicate_key="favorite_instrument",
                value="modular synths",
                source_class=SourceClass.FIRST_PARTY_RECORD,
                route_mode="normal_chat",
                channel_id=10,
                channel_name="bnl-testing",
                channel_policy="public_context",
                visibility=Visibility.PUBLIC,
                confidence=Confidence.HIGH,
                public_usable=True,
                observed_at="2026-07-25T12:00:01+00:00",
                lifecycle_status="active",
                participants=(
                    ledger.LedgerParticipant(
                        "discord_user:7",
                        "Crow",
                        "author",
                        0,
                    ),
                ),
            ),
        )
        self.fact_entry_id = result.entry_id
        self.packet = self._build_packet()
        self.assessment = self._build_assessment(self.packet)
        self.basis = self._build_basis(self.packet, self.assessment)

    def tearDown(self):
        self.conn.close()
        self.env.stop()

    def _request(self):
        text = "BNL-01, what am I all about?"
        return IntelligencePacketRequest(
            guild_id=1,
            subject_user_id=7,
            route_mode="normal_chat",
            conversation_surface="mention_or_reply",
            channel_id=10,
            channel_name="bnl-testing",
            channel_policy="public_context",
            visibility_allowance="public_safe",
            user_text=text,
            participant_user_ids=(7,),
            direct_state="direct",
            budget_chars=5000,
            conversation_evidence=(
                PacketConversationEvidence(
                    text=(
                        "I keep connecting modular synths to the "
                        "archive project."
                    ),
                    source_id=900,
                    speaker_user_id=7,
                    speaker_label="Crow",
                ),
                PacketConversationEvidence(
                    text=text,
                    speaker_user_id=7,
                    speaker_label="Crow",
                    current_turn=True,
                ),
            ),
            now="2026-07-25T12:01:00+00:00",
        )

    def _build_packet(self):
        return build_packet(
            self.conn,
            self._request(),
            persist=True,
            environ=self.flags,
        )

    def _build_assessment(self, packet):
        return build_unified_response_assessment(
            guild_id=1,
            route_mode="normal_chat",
            channel_policy="public_context",
            conversation_surface="mention_or_reply",
            current_speaker_user_ids=(7,),
            participant_user_ids=(7,),
            speaker_labels=("Crow",),
            current_exchange_source_ids=(900,),
            governed_entry_ids=packet.governed_refs,
            canon_refs=packet.canon_refs,
            prompt_lanes=("current_exchange", "conversation_context"),
            current_text=packet.request.user_text,
            packet_selected_lanes=packet.assessment_lanes,
            packet_excluded_lanes=packet.assessment_exclusions,
            packet_conflict_reasons=packet.diagnostics.conflict_reasons,
            packet_missing_lanes=packet.assessment_missing_lanes,
            packet_revalidation_status=(
                packet.diagnostics.revalidation_status
            ),
        )

    def _build_basis(self, packet, assessment):
        basis = build_basis(
            guild_id=1,
            user_id=7,
            channel_id=10,
            route_mode="normal_chat",
            channel_policy="public_context",
            current_direct=True,
            user_text=packet.request.user_text,
            packet=packet,
            assessment=assessment,
            environ=self.flags,
        )
        self.assertIsNotNone(basis)
        return basis

    def test_configuration_requires_one_exact_scope_and_no_global_live_gate(self):
        configured = configuration(self.flags)
        self.assertTrue(configured["effective"])
        self.assertEqual(configured["reason"], "scoped_canary")
        self.assertEqual(configured["guild_allowlist_count"], 1)
        self.assertEqual(configured["user_allowlist_count"], 1)
        self.assertEqual(configured["channel_allowlist_count"], 1)

        incomplete = configuration(
            {
                **self.flags,
                "BNL_SHARED_BRAIN_SYNTHESIS_CANARY_CHANNEL_IDS": "10,11",
            }
        )
        self.assertFalse(incomplete["effective"])
        self.assertEqual(incomplete["reason"], "scope_incomplete")

        global_live = configuration(
            {
                **self.flags,
                "BNL_RELATIONSHIP_V2_LIVE_ENABLED": "true",
            }
        )
        self.assertFalse(global_live["effective"])
        self.assertEqual(
            global_live["reason"],
            "global_live_authority_detected",
        )

    def test_scope_excludes_wrong_route_surface_media_and_attribution(self):
        common = {
            "guild_id": 1,
            "user_id": 7,
            "channel_id": 10,
            "route_mode": "normal_chat",
            "channel_policy": "public_context",
            "current_direct": True,
            "user_text": "What am I all about?",
            "packet": self.packet,
            "assessment": self.assessment,
            "environ": self.flags,
        }
        self.assertTrue(scope_enabled(**common))
        for override in (
            {"guild_id": 2},
            {"user_id": 8},
            {"channel_id": 11},
            {"route_mode": "direct_payload"},
            {"channel_policy": "public_home"},
            {"current_direct": False},
            {"user_text": "Tell me a joke."},
            {
                "user_text": (
                    "What am I all about, and tell me a joke?"
                )
            },
            {"has_media": True},
            {"exact_quote_requested": True},
            {"third_party_attribution_requested": True},
        ):
            self.assertFalse(scope_enabled(**{**common, **override}))

    def test_renderer_excludes_current_intent_and_relationship_posture(self):
        (
            rendered,
            lane_counts,
            item_count,
            _source_digests,
        ) = render_packet_context(self.packet)
        self.assertGreater(item_count, 0)
        self.assertIn("modular synths", rendered)
        self.assertNotIn("what am I all about", rendered)
        self.assertNotIn("relationship posture", rendered.lower())
        self.assertNotIn("source_ref", rendered)
        self.assertNotIn("conversation:900", rendered)
        self.assertNotIn("current_intent", dict(lane_counts))
        self.assertNotIn("relationship_posture", dict(lane_counts))
        self.assertNotIn("approved canon", rendered)

    def test_candidate_is_applied_only_after_revalidation_and_grounding(self):
        run = begin_run(
            self.conn,
            self.basis,
            baseline_response=(
                "You keep connecting music and project work."
            ),
            environ=self.flags,
        )
        self.assertTrue(run.prompt_applied)
        decision = evaluate_candidate(
            self.conn,
            run,
            baseline_response=(
                "You keep connecting music and project work."
            ),
            candidate_response=(
                "You are about modular synths, music, and building "
                "the archive project into something shared."
            ),
            environ=self.flags,
        )
        self.assertTrue(decision.candidate_selected)
        self.assertGreater(
            decision.candidate_evidence_coverage_count,
            0,
        )
        self.assertTrue(
            finalize_run(
                self.conn,
                decision,
                final_response=decision.response,
                response_sent=True,
                candidate_live=True,
                guard_status="candidate_sent",
            )
        )

        report = build_evaluation_report(self.conn, guild_id=1)
        self.assertEqual(report["runs"], 1)
        self.assertEqual(report["promptAppliedRuns"], 1)
        self.assertEqual(report["candidateSelectedRuns"], 1)
        self.assertEqual(report["liveAppliedRuns"], 1)
        self.assertEqual(report["responseSentRuns"], 1)
        self.assertGreater(report["candidateEvidenceCoverageTotal"], 0)
        self.assertEqual(report["liveInvalidRevalidationRuns"], 0)
        self.assertEqual(report["liveUngroundedRuns"], 0)
        self.assertEqual(report["relationshipPostureAppliedRuns"], 0)
        self.assertEqual(report["contentFieldsPresent"], [])

        packet_report = build_packet_report(self.conn, guild_id=1)
        self.assertEqual(packet_report["promptAppliedRuns"], 1)
        self.assertEqual(packet_report["liveAppliedRuns"], 1)

    def test_source_change_and_control_leak_fall_back_to_established_path(self):
        run = begin_run(
            self.conn,
            self.basis,
            baseline_response="You are about music and project work.",
            environ=self.flags,
        )
        self.conn.execute(
            """
            UPDATE conversations
            SET content='The source row changed before generation.'
            WHERE id=900
            """
        )
        valid, status = revalidate_basis(
            self.conn,
            self.basis,
            environ=self.flags,
        )
        self.assertFalse(valid)
        self.assertEqual(status, "source_changed")
        changed = evaluate_candidate(
            self.conn,
            run,
            baseline_response="You are about music and project work.",
            candidate_response=(
                "You are about modular synths and the archive project."
            ),
            environ=self.flags,
        )
        self.assertFalse(changed.candidate_selected)
        self.assertEqual(
            changed.fallback_reason,
            "post_generation_source_changed",
        )

        fresh_packet = self._build_packet()
        fresh_basis = self._build_basis(
            fresh_packet,
            self._build_assessment(fresh_packet),
        )
        leak_run = begin_run(
            self.conn,
            fresh_basis,
            baseline_response="You are about music and project work.",
            environ=self.flags,
        )
        leaked = evaluate_candidate(
            self.conn,
            leak_run,
            baseline_response="You are about music and project work.",
            candidate_response=(
                "The unified intelligence packet says modular synths."
            ),
            environ=self.flags,
        )
        self.assertFalse(leaked.candidate_selected)
        self.assertEqual(
            leaked.fallback_reason,
            "candidate_control_marker_leak",
        )

    def test_acceptance_reconciles_scoped_packet_application(self):
        run = begin_run(
            self.conn,
            self.basis,
            baseline_response="You are about music and project work.",
            environ=self.flags,
        )
        decision = evaluate_candidate(
            self.conn,
            run,
            baseline_response="You are about music and project work.",
            candidate_response=(
                "You are all about modular synths and the archive "
                "project; both keep showing up in the way you build."
            ),
            environ=self.flags,
        )
        self.assertTrue(decision.candidate_selected)
        finalize_run(
            self.conn,
            decision,
            final_response=decision.response,
            response_sent=True,
            candidate_live=True,
            guard_status="candidate_sent",
        )

        snapshot = build_v2_shadow_acceptance_snapshot(
            self.conn,
            guild_id=1,
            environ=self.flags,
        )
        self.assertNotIn(
            "unified_intelligence_packet:promptAppliedRuns",
            snapshot["blockers"],
        )
        self.assertNotIn(
            "unified_intelligence_packet:liveAppliedRuns",
            snapshot["blockers"],
        )
        canary = snapshot["sharedBrainSynthesisCanary"]
        self.assertTrue(canary["requested"])
        self.assertTrue(canary["effective"])
        self.assertTrue(canary["fullyScoped"])
        self.assertEqual(
            canary["unauthorizedPacketApplications"],
            {"prompt": 0, "live": 0},
        )
        self.assertTrue(snapshot["behaviorChangesApplied"])
        rendered = "\n".join(
            render_v2_shadow_acceptance_lines(snapshot)
        )
        self.assertIn("- shared_brain_synthesis_canary:", rendered)
        self.assertIn("prompt_applied=`1`", rendered)
        self.assertIn("live_applied=`1`", rendered)
        self.assertIn("relationship_posture_applied=`0`", rendered)
        self.assertNotIn("modular synths", rendered)

    def test_acceptance_blocks_packet_application_without_canary_receipt(self):
        self.assertTrue(
            mark_packet_application(
                self.conn,
                self.packet,
                prompt_applied=True,
            )
        )
        snapshot = build_v2_shadow_acceptance_snapshot(
            self.conn,
            guild_id=1,
            environ=self.flags,
        )
        self.assertIn(
            (
                "unified_intelligence_packet:"
                "promptAppliedWithoutSynthesisReceipt"
            ),
            snapshot["blockers"],
        )

    def test_receipt_schema_contains_no_member_or_response_content(self):
        ensure_schema(self.conn)
        columns = {
            row[1]
            for row in self.conn.execute(
                """
                PRAGMA table_info(
                  memory_governance_shared_brain_synthesis_runs
                )
                """
            )
        }
        forbidden = {
            "request_text",
            "packet_content",
            "source_text",
            "response_text",
            "baseline_response",
            "candidate_response",
            "participant_ids",
            "source_refs",
        }
        self.assertFalse(columns & forbidden)


if __name__ == "__main__":
    unittest.main()
