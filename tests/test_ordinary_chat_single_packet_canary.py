import os
import sqlite3
import unittest
from dataclasses import replace
from unittest import mock

from bnl_canon_source_contract import Confidence, SourceClass, Visibility
import bnl_memory_ledger as ledger
import bnl_moment_engine as moments
import bnl_relationship_engine as relationships
from bnl_shared_brain_synthesis import (
    ORDINARY_CHAT_AUTHORITY,
    ORDINARY_CHAT_ROUTE_FAMILY,
    audit_ordinary_chat_candidate_claims,
    begin_single_packet_run,
    build_evaluation_report,
    build_ordinary_chat_basis,
    build_packet_owned_prompt,
    candidate_profile_coverage,
    evaluate_single_packet_response,
    finalize_run,
    ordinary_chat_configuration,
    ordinary_chat_route_scope_decision,
    record_single_packet_block,
)
from bnl_unified_intelligence_packet import (
    IntelligencePacketRequest,
    PacketConversationEvidence,
    PacketFrameSubject,
    PacketSubjectResolution,
    build_packet,
)
from bnl_unified_response_assessment import (
    build_situation_frame_v1,
    build_unified_response_assessment,
    revalidate_situation_frame,
)


class OrdinaryChatSinglePacketCanaryTests(unittest.TestCase):
    def setUp(self):
        self.flags = {
            "BNL_MEMORY_LEDGER_SHADOW_ENABLED": "true",
            "BNL_MOMENT_ENGINE_SHADOW_ENABLED": "true",
            "BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED": "true",
            "BNL_RELATIONSHIP_V2_SHADOW_ENABLED": "true",
            "BNL_UNIFIED_RESPONSE_ASSESSMENT_SHADOW_ENABLED": "true",
            "BNL_UNIFIED_INTELLIGENCE_PACKET_SHADOW_ENABLED": "true",
            "BNL_SHARED_BRAIN_SYNTHESIS_CANARY_ENABLED": "false",
            "BNL_PUBLIC_HOME_BROAD_RECALL_OWNER_ENABLED": "false",
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_ENABLED": "true",
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_GUILD_IDS": "1",
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_USER_IDS": "7",
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_CHANNEL_IDS": "10",
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
                route_mode TEXT NOT NULL,
                timestamp TEXT
            )
            """
        )
        self.conn.execute(
            """
            INSERT INTO conversations(
                id,guild_id,user_id,user_name,role,content,channel_id,
                channel_policy,route_mode,timestamp
            ) VALUES(900,1,7,'Test Member','user',?,10,
                     'public_context','normal_chat',?)
            """,
            (
                "I keep connecting modular synths to the archive project.",
                "2026-08-10T12:00:00+00:00",
            ),
        )
        context_result = ledger.shadow_conversation_row(
            self.conn,
            row_id=900,
            user_id=7,
            user_name="Test Member",
            guild_id=1,
            role="user",
            content=(
                "I keep connecting modular synths to the archive project."
            ),
            channel_name="bnl-testing",
            channel_policy="public_context",
            channel_id=10,
            route_mode="normal_chat",
            observed_at="2026-08-10T12:00:00+00:00",
        )
        self.assertEqual(context_result.outcome, "inserted")
        fact = ledger.insert_ledger_entry(
            self.conn,
            ledger.LedgerEntry(
                guild_id=1,
                source_table="conversations",
                source_row_id=901,
                source_revision="901",
                source_role="member_self_report",
                entry_type="preference",
                subject_key="discord_user:7",
                subject_display_name="Test Member",
                predicate_key="favorite_movie",
                value="Arrival",
                source_class=SourceClass.FIRST_PARTY_RECORD,
                route_mode="normal_chat",
                channel_id=10,
                channel_name="bnl-testing",
                channel_policy="public_context",
                visibility=Visibility.PUBLIC,
                confidence=Confidence.HIGH,
                public_usable=True,
                observed_at="2026-08-10T12:00:01+00:00",
                source_sequence=901,
                lifecycle_status="active",
                participants=(
                    ledger.LedgerParticipant(
                        "discord_user:7",
                        "Test Member",
                        "author",
                        0,
                    ),
                ),
            ),
        )
        self.assertTrue(fact.entry_id)
        self.text = "What do you remember about me?"
        self.frame = build_situation_frame_v1(
            route_allowed=True,
            route_mode="normal_chat",
            conversation_surface="mention_or_reply",
            channel_policy="public_context",
            current_text=self.text,
            current_speaker_user_ids=(7,),
            current_speaker_labels=("Test Member",),
            addressee_kinds=("discord_mention",),
            source_message_ids=(301,),
            explicit_mention_count=1,
            subject_user_ids=(7,),
            subject_label_hints=("Test Member",),
            referent_status="resolved",
            response_act="answer",
            packet_revision="turn_ordinary_01",
        )
        self.packet = self._build_packet()
        frame_revalidation = revalidate_situation_frame(
            self.frame,
            current_text=self.text,
            route_mode="normal_chat",
            conversation_surface="mention_or_reply",
            channel_policy="public_context",
            packet_source_snapshot_digest=(
                self.packet.source_snapshot_digest
            ),
        )
        profile = self.packet.profile_sufficiency
        self.assessment = build_unified_response_assessment(
            guild_id=1,
            route_mode="normal_chat",
            channel_policy="public_context",
            conversation_surface="mention_or_reply",
            current_speaker_user_ids=(7,),
            participant_user_ids=(7,),
            speaker_labels=("Test Member",),
            current_exchange_source_ids=(900,),
            governed_entry_ids=self.packet.governed_refs,
            canon_refs=self.packet.canon_refs,
            prompt_lanes=("current_exchange", "conversation_context"),
            current_text=self.text,
            packet_selected_lanes=self.packet.assessment_lanes,
            packet_excluded_lanes=self.packet.assessment_exclusions,
            packet_conflict_reasons=self.packet.diagnostics.conflict_reasons,
            packet_missing_lanes=self.packet.assessment_missing_lanes,
            packet_revalidation_status=(
                self.packet.diagnostics.revalidation_status
            ),
            profile_sufficiency_status=profile.status,
            profile_sufficiency_met=profile.satisfied,
            profile_required_point_count=profile.required_point_count,
            profile_selected_point_count=profile.selected_point_count,
            profile_independent_root_count=profile.independent_root_count,
            profile_independent_occurrence_count=(
                profile.independent_occurrence_count
            ),
            profile_sufficiency_reasons=profile.reason_codes,
            situation_frame=self.frame,
            frame_revalidation=frame_revalidation,
        )
        self.basis = build_ordinary_chat_basis(
            guild_id=1,
            user_id=7,
            channel_id=10,
            route_mode="normal_chat",
            channel_policy="public_context",
            current_direct=True,
            user_text=self.text,
            packet=self.packet,
            assessment=self.assessment,
            environ=self.flags,
        )
        self.assertIsNotNone(self.basis)

    def tearDown(self):
        self.conn.close()
        self.env.stop()

    def _build_packet(self):
        frame_subjects = tuple(
            PacketFrameSubject(
                user_id=subject.user_id,
                entity_ref=subject.entity_ref,
                label_hint=subject.label_hint,
                binding_method=subject.binding_method,
                confidence=subject.confidence,
                role_hints=subject.role_hints,
                domain_hints=subject.domain_hints,
            )
            for subject in self.frame.subjects
        )
        request = IntelligencePacketRequest(
            guild_id=1,
            subject_user_id=7,
            route_mode="normal_chat",
            conversation_surface="mention_or_reply",
            subject_display_name="Test Member",
            channel_id=10,
            channel_name="bnl-testing",
            channel_policy="public_context",
            visibility_allowance="public_safe",
            user_text=self.text,
            participant_user_ids=(7,),
            direct_state="direct",
            budget_chars=5000,
            conversation_evidence=(
                PacketConversationEvidence(
                    text=(
                        "I keep connecting modular synths to the archive "
                        "project."
                    ),
                    source_id=900,
                    speaker_user_id=7,
                    speaker_label="Test Member",
                ),
                PacketConversationEvidence(
                    text=self.text,
                    speaker_user_id=7,
                    speaker_label="Test Member",
                    current_turn=True,
                ),
            ),
            declared_canon_authorized=True,
            frame_schema_version=self.frame.schema_version,
            frame_revision=self.frame.frame_revision,
            frame_input_evidence_digest=self.frame.input_evidence_digest,
            frame_status=self.frame.status,
            frame_subject_requirement=self.frame.subject_requirement,
            frame_subjects=frame_subjects,
            frame_role_hints=self.frame.role_hints,
            frame_domain_hints=self.frame.domain_hints,
            frame_event_ref=self.frame.event_ref,
            frame_event_relation=self.frame.event_relation,
            frame_task_kind=self.frame.task_kind,
            frame_object_kind=self.frame.object_kind,
            frame_phase=self.frame.phase,
            frame_temporal_scope=self.frame.temporal_scope,
            frame_currentness=self.frame.currentness,
            now="2026-08-10T12:01:00+00:00",
        )
        return build_packet(
            self.conn,
            request,
            persist=True,
            environ=self.flags,
        )

    def _begin(self):
        return begin_single_packet_run(
            self.conn,
            self.basis,
            prompt_ready=True,
            frame_revalidation_status="valid",
            environ=self.flags,
        )

    def test_configuration_is_default_off_exact_scope_and_conflict_closed(self):
        disabled = ordinary_chat_configuration(
            {
                **self.flags,
                "BNL_ORDINARY_CHAT_SINGLE_PACKET_ENABLED": "false",
            }
        )
        self.assertFalse(disabled["effective"])
        self.assertEqual(disabled["reason"], "disabled")

        configured = ordinary_chat_configuration(self.flags)
        self.assertTrue(configured["effective"])
        self.assertEqual(configured["provider_call_limit"], 1)
        self.assertEqual(configured["corrective_call_limit"], 0)
        self.assertEqual(
            configured["kill_switch_env"],
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_ENABLED",
        )

        expanded = ordinary_chat_configuration(
            {
                **self.flags,
                "BNL_ORDINARY_CHAT_SINGLE_PACKET_USER_IDS": "7,8",
            }
        )
        self.assertFalse(expanded["effective"])
        self.assertEqual(expanded["reason"], "scope_limit_exceeded")

        conflicting = ordinary_chat_configuration(
            {
                **self.flags,
                "BNL_SHARED_BRAIN_SYNTHESIS_CANARY_ENABLED": "true",
            }
        )
        self.assertFalse(conflicting["effective"])
        self.assertEqual(
            conflicting["reason"],
            "comparison_authority_conflict",
        )

    def test_scope_excludes_wrong_identity_media_and_specialized_routes(self):
        eligible = ordinary_chat_route_scope_decision(
            guild_id=1,
            user_id=7,
            channel_id=10,
            route_mode="normal_chat",
            channel_policy="public_context",
            current_direct=True,
            user_text=self.text,
            environ=self.flags,
        )
        self.assertTrue(eligible.eligible)
        self.assertTrue(eligible.requested)
        self.assertTrue(eligible.effective)

        cases = (
            ({"user_id": 8}, "user_not_allowlisted"),
            ({"route_mode": "direct_payload_task"}, "route_mode_not_supported"),
            ({"has_media": True}, "media_present"),
            (
                {"specialized_owner_present": True},
                "specialized_owner_present",
            ),
        )
        base = {
            "guild_id": 1,
            "user_id": 7,
            "channel_id": 10,
            "route_mode": "normal_chat",
            "channel_policy": "public_context",
            "current_direct": True,
            "user_text": self.text,
            "environ": self.flags,
        }
        for overrides, reason in cases:
            with self.subTest(reason=reason):
                decision = ordinary_chat_route_scope_decision(
                    **{**base, **overrides}
                )
                self.assertFalse(decision.eligible)
                self.assertEqual(decision.reason, reason)

    def test_packet_owned_prompt_rejects_legacy_and_nonpacket_owners(self):
        base_prompt = (
            "Current user request: What do you remember about me?\n"
            "Current exact reply evidence: Test Member asked directly."
        )
        owned = build_packet_owned_prompt(base_prompt, self.basis)
        self.assertTrue(owned.ready)
        self.assertIn("PACKET-OWNED RESPONSE CONTRACT", owned.prompt)
        self.assertIn(self.basis.rendered_context, owned.prompt)
        self.assertEqual(owned.prompt.count(self.basis.rendered_context), 1)
        self.assertNotIn("Durable memory context:", owned.prompt)

        legacy = build_packet_owned_prompt(
            base_prompt + "\nDurable memory context: old view",
            self.basis,
        )
        self.assertFalse(legacy.ready)
        self.assertEqual(
            legacy.reason,
            "legacy_factual_prompt_marker_present",
        )

        blocked_assessment = replace(
            self.assessment,
            selected_lanes=(*self.assessment.selected_lanes, "show_state"),
        )
        blocked_basis = build_ordinary_chat_basis(
            guild_id=1,
            user_id=7,
            channel_id=10,
            route_mode="normal_chat",
            channel_policy="public_context",
            current_direct=True,
            user_text=self.text,
            packet=self.packet,
            assessment=blocked_assessment,
            environ=self.flags,
        )
        self.assertIsNotNone(blocked_basis)
        blocked = build_packet_owned_prompt(base_prompt, blocked_basis)
        self.assertFalse(blocked.ready)
        self.assertEqual(blocked.reason, "nonpacket_factual_owner_selected")

    def test_receipt_is_content_free_and_counts_one_call(self):
        run = self._begin()
        self.assertTrue(run.prompt_applied)
        decision = evaluate_single_packet_response(
            self.conn,
            run,
            response="Your favorite movie is Arrival.",
            provider_call_count=1,
            corrective_call_count=0,
            generation_latency_ms=42,
            environ=self.flags,
        )
        self.assertTrue(decision.candidate_selected)
        self.assertTrue(
            finalize_run(
                self.conn,
                decision,
                final_response=decision.response,
                response_sent=True,
                candidate_live=True,
                guard_status="single_packet_candidate_sent",
            )
        )
        row = self.conn.execute(
            """
            SELECT route_family,authority_mode,baseline_generated,
                   provider_call_count,corrective_call_count,
                   frame_revision,frame_input_evidence_digest,
                   source_snapshot_digest,frame_revalidation_status,
                   source_revalidation_status,response_sent,live_applied
            FROM memory_governance_shared_brain_synthesis_runs
            WHERE run_id=?
            """,
            (run.run_id,),
        ).fetchone()
        self.assertEqual(row[0], ORDINARY_CHAT_ROUTE_FAMILY)
        self.assertEqual(row[1], ORDINARY_CHAT_AUTHORITY)
        self.assertEqual(row[2:5], (0, 1, 0))
        self.assertTrue(row[5])
        self.assertTrue(row[6])
        self.assertTrue(row[7])
        self.assertEqual(row[8], "valid")
        self.assertTrue(str(row[9]).startswith("passed"))
        self.assertEqual(row[10:], (1, 1))

        columns = {
            str(column[1])
            for column in self.conn.execute(
                "PRAGMA table_info(memory_governance_shared_brain_synthesis_runs)"
            )
        }
        self.assertFalse(
            columns
            & {
                "request_text",
                "packet_content",
                "source_text",
                "response_text",
                "baseline_response",
                "candidate_response",
            }
        )
        report = build_evaluation_report(self.conn, guild_id=1)
        self.assertEqual(report["ordinaryChatRuns"], 1)
        self.assertEqual(report["providerCallTotal"], 1)
        self.assertEqual(report["correctiveCallTotal"], 0)
        self.assertEqual(report["ordinaryCallCountViolationRuns"], 0)
        self.assertEqual(report["ordinaryCorrectiveCallViolationRuns"], 0)
        self.assertEqual(report["invalidScopeRuns"], 0)

    def test_unsupported_packet_domain_claims_are_rejected_before_selection(self):
        candidates = (
            "Your favorite movie is Blade Runner.",
            "You work as a network engineer.",
            "He works as a network engineer.",
            "You and Mac Modem are siblings.",
            "BARCODE Radio started in 1999.",
        )
        for candidate in candidates:
            with self.subTest(candidate=candidate):
                decision = evaluate_single_packet_response(
                    self.conn,
                    self._begin(),
                    response=candidate,
                    provider_call_count=1,
                    corrective_call_count=0,
                    environ=self.flags,
                )
                self.assertFalse(decision.candidate_selected)
                self.assertEqual(
                    decision.fallback_reason,
                    "unsupported_packet_domain_claim",
                )
                self.assertGreaterEqual(
                    decision.candidate_unsupported_factual_claim_count,
                    1,
                )
                self.assertIn(
                    "unsupported_packet_domain",
                    decision.candidate_claim_classifications,
                )

    def test_external_public_knowledge_is_not_made_packet_authority(self):
        external_packet = replace(
            self.packet,
            request=replace(
                self.packet.request,
                subject_user_id=0,
                subject_display_name="",
                user_text="Where is Seattle?",
                frame_subject_requirement="not_required",
                frame_subjects=(),
                frame_event_ref="",
                frame_event_relation="not_applicable",
            ),
            subject_resolution=PacketSubjectResolution(
                status="not_applicable",
                reason_codes=("subject_not_required",),
            ),
        )
        external_basis = replace(self.basis, packet=external_packet)
        response = "Seattle is in Washington."
        coverage = candidate_profile_coverage(external_basis, response)
        classifications, unsupported = audit_ordinary_chat_candidate_claims(
            external_basis,
            response,
            coverage=coverage,
        )
        self.assertEqual(unsupported, 0)
        self.assertEqual(classifications, ("external_public_knowledge",))

    def test_invalid_call_counts_fail_closed_and_are_reported(self):
        run = self._begin()
        decision = evaluate_single_packet_response(
            self.conn,
            run,
            response="One generated answer.",
            provider_call_count=2,
            corrective_call_count=1,
            environ=self.flags,
        )
        self.assertFalse(decision.candidate_selected)
        self.assertEqual(
            decision.fallback_reason,
            "provider_call_count_invalid",
        )
        report = build_evaluation_report(self.conn, guild_id=1)
        self.assertEqual(report["ordinaryCallCountViolationRuns"], 1)
        self.assertEqual(report["ordinaryCorrectiveCallViolationRuns"], 1)

    def test_source_change_after_generation_rejects_without_fallback_candidate(self):
        run = self._begin()
        self.conn.execute(
            """
            UPDATE conversations
            SET content='The source row changed after the provider call.'
            WHERE id=900
            """
        )
        decision = evaluate_single_packet_response(
            self.conn,
            run,
            response="A generated answer that must not be sent.",
            provider_call_count=1,
            corrective_call_count=0,
            environ=self.flags,
        )
        self.assertFalse(decision.candidate_selected)
        self.assertEqual(
            decision.fallback_reason,
            "post_generation_source_changed",
        )
        self.assertNotEqual(decision.response, "Established response.")
        row = self.conn.execute(
            """
            SELECT provider_call_count,corrective_call_count,
                   source_revalidation_status,candidate_selected
            FROM memory_governance_shared_brain_synthesis_runs
            WHERE run_id=?
            """,
            (run.run_id,),
        ).fetchone()
        self.assertEqual(row, (1, 0, "source_changed", 0))

    def test_processing_block_preserves_call_accounting(self):
        run = self._begin()
        decision = evaluate_single_packet_response(
            self.conn,
            run,
            response="A candidate that passed deterministic evaluation.",
            provider_call_count=1,
            corrective_call_count=0,
            environ=self.flags,
        )
        blocked = record_single_packet_block(
            self.conn,
            decision,
            reason="single_packet_guard_suppressed",
            provider_call_count=1,
            corrective_call_count=0,
            frame_revalidation_status="stale",
        )
        self.assertFalse(blocked.candidate_selected)
        row = self.conn.execute(
            """
            SELECT provider_call_count,corrective_call_count,
                   frame_revalidation_status,candidate_selected,
                   fallback_reason
            FROM memory_governance_shared_brain_synthesis_runs
            WHERE run_id=?
            """,
            (run.run_id,),
        ).fetchone()
        self.assertEqual(row[:4], (1, 0, "stale", 0))
        self.assertEqual(row[4], "single_packet_guard_suppressed")


if __name__ == "__main__":
    unittest.main()
