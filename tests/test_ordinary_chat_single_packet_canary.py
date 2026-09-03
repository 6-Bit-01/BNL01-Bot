import json
import os
import sqlite3
import unittest
from dataclasses import replace
from unittest import mock

from bnl_canon_source_contract import Confidence, SourceClass, Visibility
import bnl_declared_canon as declared_canon
import bnl_memory_ledger as ledger
import bnl_moment_engine as moments
import bnl_relationship_engine as relationships
from bnl_shared_brain_synthesis import (
    ORDINARY_CHAT_AUTHORITY,
    ORDINARY_CHAT_ROUTE_FAMILY,
    ORDINARY_CHAT_SCOPED_EXPANSION_ENABLED_ENV,
    audit_ordinary_chat_candidate_claims,
    begin_single_packet_run,
    build_evaluation_report,
    build_ordinary_chat_basis,
    build_packet_owned_prompt,
    candidate_profile_coverage,
    evaluate_single_packet_response,
    finalize_run,
    ordinary_chat_configuration,
    ordinary_chat_deterministic_response_act,
    ordinary_chat_route_scope_decision,
    ordinary_chat_task_support_plan,
    publication_packet_composes_current_queue,
    publication_packet_owns_turn,
    parse_ordinary_chat_response_contract,
    record_single_packet_review,
    revalidate_basis,
    render_ordinary_chat_task_contract,
    render_packet_context,
    validate_ordinary_chat_response_contract,
)
from bnl_unified_intelligence_packet import (
    IntelligencePacketRequest,
    PacketConversationEvidence,
    PacketFrameTask,
    PacketFrameSubject,
    PacketSubjectResolution,
    build_packet,
)
from bnl_unified_response_assessment import (
    build_situation_frame_v1,
    build_unified_response_assessment,
    revalidate_situation_frame,
)


def _contract_for_support_plan(basis, texts):
    plans = ordinary_chat_task_support_plan(basis)
    return parse_ordinary_chat_response_contract(
        json.dumps(
            {
                "tasks": [
                    {
                        "taskId": plan.task_id,
                        "text": text,
                        "supportKind": plan.support_kind,
                        "evidenceIds": list(plan.evidence_ids),
                    }
                    for plan, text in zip(plans, texts)
                ]
            }
        )
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
            frame_tasks=tuple(
                PacketFrameTask(
                    task_id=task.task_id,
                    text_digest=task.text_digest,
                    task_kind=task.task_kind,
                    object_kind=task.object_kind,
                    authority_scope=task.authority_scope,
                    temporal_scope=task.temporal_scope,
                    currentness=task.currentness,
                    required_response_act=task.required_response_act,
                    subject_requirement=task.subject_requirement,
                    subject_indexes=task.subject_indexes,
                )
                for task in self.frame.tasks
            ),
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

    def _multi_subject_basis(self, text, subjects):
        frame = build_situation_frame_v1(
            route_allowed=True,
            route_mode="normal_chat",
            conversation_surface="mention_or_reply",
            channel_policy="public_context",
            current_text=text,
            current_speaker_user_ids=(7,),
            current_speaker_labels=("Test Member",),
            addressee_kinds=("discord_mention",),
            source_message_ids=(401,),
            explicit_mention_count=1,
            subject_label_hints=tuple(label for _key, label in subjects),
            subject_entity_refs=tuple(key for key, _label in subjects),
            referent_status="resolved",
            response_act="answer",
            packet_revision="turn_multi_subject_01",
        )
        self.assertEqual(frame.status, "resolved")
        request = IntelligencePacketRequest(
            guild_id=1,
            subject_user_id=0,
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
                    text=text,
                    speaker_user_id=7,
                    speaker_label="Test Member",
                    current_turn=True,
                ),
            ),
            declared_canon_authorized=True,
            frame_schema_version=frame.schema_version,
            frame_revision=frame.frame_revision,
            frame_input_evidence_digest=frame.input_evidence_digest,
            frame_status=frame.status,
            frame_subject_requirement=frame.subject_requirement,
            frame_subjects=tuple(
                PacketFrameSubject(
                    user_id=subject.user_id,
                    entity_ref=subject.entity_ref,
                    label_hint=subject.label_hint,
                    binding_method=subject.binding_method,
                    confidence=subject.confidence,
                    role_hints=subject.role_hints,
                    domain_hints=subject.domain_hints,
                )
                for subject in frame.subjects
            ),
            frame_tasks=tuple(
                PacketFrameTask(
                    task_id=task.task_id,
                    text_digest=task.text_digest,
                    task_kind=task.task_kind,
                    object_kind=task.object_kind,
                    authority_scope=task.authority_scope,
                    temporal_scope=task.temporal_scope,
                    currentness=task.currentness,
                    required_response_act=task.required_response_act,
                    subject_requirement=task.subject_requirement,
                    subject_indexes=task.subject_indexes,
                )
                for task in frame.tasks
            ),
            frame_role_hints=frame.role_hints,
            frame_domain_hints=frame.domain_hints,
            frame_event_ref=frame.event_ref,
            frame_event_relation=frame.event_relation,
            frame_task_kind=frame.task_kind,
            frame_object_kind=frame.object_kind,
            frame_phase=frame.phase,
            frame_temporal_scope=frame.temporal_scope,
            frame_currentness=frame.currentness,
            now="2026-08-10T12:02:00+00:00",
        )
        packet = build_packet(
            self.conn,
            request,
            persist=True,
            environ=self.flags,
        )
        self.assertIsNotNone(packet)
        expected_resolution_status = (
            "not_applicable"
            if not subjects
            else "resolved"
            if len(subjects) == 1
            else "multi_resolved"
        )
        self.assertEqual(
            packet.subject_resolution.status,
            expected_resolution_status,
        )
        frame_revalidation = revalidate_situation_frame(
            frame,
            current_text=text,
            route_mode="normal_chat",
            conversation_surface="mention_or_reply",
            channel_policy="public_context",
            packet_source_snapshot_digest=packet.source_snapshot_digest,
        )
        profile = packet.profile_sufficiency
        assessment = build_unified_response_assessment(
            guild_id=1,
            route_mode="normal_chat",
            channel_policy="public_context",
            conversation_surface="mention_or_reply",
            current_speaker_user_ids=(7,),
            participant_user_ids=(7,),
            speaker_labels=("Test Member",),
            current_exchange_source_ids=(),
            governed_entry_ids=packet.governed_refs,
            canon_refs=packet.canon_refs,
            prompt_lanes=("current_exchange",),
            current_text=text,
            packet_selected_lanes=packet.assessment_lanes,
            packet_excluded_lanes=packet.assessment_exclusions,
            packet_conflict_reasons=packet.diagnostics.conflict_reasons,
            packet_missing_lanes=packet.assessment_missing_lanes,
            packet_revalidation_status=packet.diagnostics.revalidation_status,
            profile_sufficiency_status=profile.status,
            profile_sufficiency_met=profile.satisfied,
            profile_required_point_count=profile.required_point_count,
            profile_selected_point_count=profile.selected_point_count,
            profile_independent_root_count=profile.independent_root_count,
            profile_independent_occurrence_count=(
                profile.independent_occurrence_count
            ),
            profile_sufficiency_reasons=profile.reason_codes,
            situation_frame=frame,
            frame_revalidation=frame_revalidation,
        )
        basis = build_ordinary_chat_basis(
            guild_id=1,
            user_id=7,
            channel_id=10,
            route_mode="normal_chat",
            channel_policy="public_context",
            current_direct=True,
            user_text=text,
            packet=packet,
            assessment=assessment,
            environ=self.flags,
        )
        self.assertIsNotNone(basis)
        return basis

    def _basis_for_canon_entity(self, entity_key, label):
        entity_ref = "canon:%s" % entity_key
        packet = replace(
            self.packet,
            request=replace(
                self.packet.request,
                subject_user_id=0,
                subject_display_name=label,
                user_text="Tell me about %s." % label,
                frame_subject_requirement="required",
                frame_subjects=(
                    PacketFrameSubject(
                        entity_ref=entity_ref,
                        label_hint=label,
                        binding_method="declared_canon",
                        confidence="high",
                    ),
                ),
            ),
            subject_resolution=PacketSubjectResolution(
                status="resolved",
                subject_key=entity_key,
                entity_ref=entity_ref,
                binding_method="declared_canon",
                confidence="high",
                candidate_count=1,
            ),
        )
        return replace(self.basis, packet=packet)

    def _basis_with_retained_room_context(
        self,
        entries,
        *,
        frame_subject=None,
    ):
        evidence = tuple(
            PacketConversationEvidence(
                text=text,
                source_id=910 + index,
                speaker_user_id=user_id,
                speaker_label=label,
            )
            for index, (user_id, label, text) in enumerate(entries)
        )
        request = replace(
            self.packet.request,
            conversation_evidence=(
                *evidence,
                PacketConversationEvidence(
                    text=self.text,
                    speaker_user_id=7,
                    speaker_label="Test Member",
                    current_turn=True,
                ),
            ),
        )
        packet_changes = {"request": request}
        if frame_subject is not None:
            subject_user_id, subject_label = frame_subject
            packet_changes["request"] = replace(
                request,
                subject_user_id=subject_user_id,
                subject_display_name=subject_label,
                frame_status="resolved",
                frame_subject_requirement="required",
                frame_subjects=(
                    PacketFrameSubject(
                        user_id=subject_user_id,
                        label_hint=subject_label,
                        binding_method="stable_discord_account",
                        confidence="authoritative",
                    ),
                ),
            )
            packet_changes["subject_resolution"] = PacketSubjectResolution(
                status="resolved",
                subject_user_id=subject_user_id,
                subject_key=ledger.subject_key_for_user(subject_user_id),
                binding_method="stable_discord_account",
                confidence="authoritative",
                candidate_count=1,
            )
            packet_changes["subject_resolutions"] = ()
        packet = replace(self.packet, **packet_changes)
        labels = tuple(dict.fromkeys(label for _, label, _ in entries))
        retained_context = "\n".join(
            (
                "Recent room context from this channel:",
                *(f"- {label}: {text}" for _, label, text in entries),
                "Active participants in recent room context: "
                + ", ".join(labels),
            )
        )
        return replace(
            self.basis,
            packet=packet,
            competing_factual_contexts=(retained_context,),
        )

    def test_configuration_is_default_off_private_scope_and_conflict_closed(self):
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
        self.assertEqual(
            configured["contract_version"],
            "ordinary_chat_single_packet_v6",
        )
        self.assertEqual(configured["scope_mode"], "private_acceptance")
        self.assertFalse(
            configured["scoped_expansion_configured_enabled"]
        )
        self.assertFalse(configured["scoped_expansion_effective"])
        self.assertFalse(configured["expanded_scope_present"])
        self.assertEqual(configured["max_scoped_guilds"], 1)
        self.assertEqual(configured["private_user_count"], 1)
        self.assertEqual(configured["private_channel_count"], 1)
        self.assertEqual(configured["max_scoped_users"], 8)
        self.assertEqual(configured["max_scoped_channels"], 4)
        self.assertEqual(configured["provider_call_limit"], 1)
        self.assertEqual(configured["corrective_call_limit"], 0)
        self.assertEqual(
            configured["kill_switch_env"],
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_ENABLED",
        )

        expanded_without_gate = ordinary_chat_configuration(
            {
                **self.flags,
                "BNL_ORDINARY_CHAT_SINGLE_PACKET_USER_IDS": "7,8",
            }
        )
        self.assertFalse(expanded_without_gate["effective"])
        self.assertTrue(expanded_without_gate["expanded_scope_present"])
        self.assertEqual(
            expanded_without_gate["reason"],
            "scoped_expansion_not_enabled",
        )
        blocked_expansion = ordinary_chat_route_scope_decision(
            guild_id=1,
            user_id=8,
            channel_id=10,
            route_mode="normal_chat",
            channel_policy="public_context",
            current_direct=True,
            user_text=self.text,
            environ={
                **self.flags,
                "BNL_ORDINARY_CHAT_SINGLE_PACKET_USER_IDS": "7,8",
            },
        )
        self.assertFalse(blocked_expansion.eligible)
        self.assertEqual(
            blocked_expansion.reason,
            "configuration_scoped_expansion_not_enabled",
        )

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

    def test_bounded_expansion_requires_its_gate_and_stays_capped(self):
        expanded_flags = {
            **self.flags,
            ORDINARY_CHAT_SCOPED_EXPANSION_ENABLED_ENV: "true",
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_USER_IDS": "7,8",
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_CHANNEL_IDS": "10,11",
        }
        expanded = ordinary_chat_configuration(expanded_flags)

        self.assertTrue(expanded["effective"])
        self.assertEqual(expanded["reason"], ORDINARY_CHAT_AUTHORITY)
        self.assertEqual(expanded["scope_mode"], "bounded_expansion")
        self.assertTrue(expanded["scoped_expansion_configured_enabled"])
        self.assertTrue(expanded["scoped_expansion_effective"])
        self.assertTrue(expanded["expanded_scope_present"])
        self.assertEqual(expanded["guild_allowlist_count"], 1)
        self.assertEqual(expanded["user_allowlist_count"], 2)
        self.assertEqual(expanded["channel_allowlist_count"], 2)
        self.assertEqual(
            expanded["expansion_gate_env"],
            ORDINARY_CHAT_SCOPED_EXPANSION_ENABLED_ENV,
        )
        reordered = ordinary_chat_configuration(
            {
                **expanded_flags,
                "BNL_ORDINARY_CHAT_SINGLE_PACKET_USER_IDS": "8,7",
                "BNL_ORDINARY_CHAT_SINGLE_PACKET_CHANNEL_IDS": "11,10",
            }
        )
        self.assertEqual(reordered["scope_digest"], expanded["scope_digest"])

        for user_id in (7, 8):
            for channel_id in (10, 11):
                with self.subTest(
                    user_id=user_id,
                    channel_id=channel_id,
                ):
                    decision = ordinary_chat_route_scope_decision(
                        guild_id=1,
                        user_id=user_id,
                        channel_id=channel_id,
                        route_mode="normal_chat",
                        channel_policy="public_context",
                        current_direct=True,
                        user_text=self.text,
                        environ=expanded_flags,
                    )
                    self.assertTrue(decision.eligible)

        for override, reason in (
            ({"user_id": 9}, "user_not_allowlisted"),
            ({"channel_id": 12}, "channel_not_allowlisted"),
            ({"guild_id": 2}, "guild_not_allowlisted"),
        ):
            with self.subTest(reason=reason):
                common = {
                    "guild_id": 1,
                    "user_id": 7,
                    "channel_id": 10,
                    "route_mode": "normal_chat",
                    "channel_policy": "public_context",
                    "current_direct": True,
                    "user_text": self.text,
                    "environ": expanded_flags,
                }
                decision = ordinary_chat_route_scope_decision(
                    **{**common, **override}
                )
                self.assertFalse(decision.eligible)
                self.assertEqual(decision.reason, reason)

        oversized_users = ordinary_chat_configuration(
            {
                **expanded_flags,
                "BNL_ORDINARY_CHAT_SINGLE_PACKET_USER_IDS": ",".join(
                    str(value) for value in range(1, 10)
                ),
            }
        )
        self.assertFalse(oversized_users["effective"])
        self.assertEqual(
            oversized_users["reason"],
            "scope_limit_exceeded",
        )

        oversized_channels = ordinary_chat_configuration(
            {
                **expanded_flags,
                "BNL_ORDINARY_CHAT_SINGLE_PACKET_CHANNEL_IDS": (
                    "10,11,12,13,14"
                ),
            }
        )
        self.assertFalse(oversized_channels["effective"])
        self.assertEqual(
            oversized_channels["reason"],
            "scope_limit_exceeded",
        )

        oversized_guilds = ordinary_chat_configuration(
            {
                **expanded_flags,
                "BNL_ORDINARY_CHAT_SINGLE_PACKET_GUILD_IDS": "1,2",
            }
        )
        self.assertFalse(oversized_guilds["effective"])
        self.assertEqual(
            oversized_guilds["reason"],
            "scope_limit_exceeded",
        )

        maximum_scope = ordinary_chat_configuration(
            {
                **expanded_flags,
                "BNL_ORDINARY_CHAT_SINGLE_PACKET_USER_IDS": ",".join(
                    str(value) for value in range(1, 9)
                ),
                "BNL_ORDINARY_CHAT_SINGLE_PACKET_CHANNEL_IDS": (
                    "10,11,12,13"
                ),
            }
        )
        self.assertTrue(maximum_scope["effective"])
        self.assertEqual(maximum_scope["user_allowlist_count"], 8)
        self.assertEqual(maximum_scope["channel_allowlist_count"], 4)

    def test_expansion_gate_alone_does_not_expand_private_scope(self):
        expanded_gate_only = ordinary_chat_configuration(
            {
                **self.flags,
                ORDINARY_CHAT_SCOPED_EXPANSION_ENABLED_ENV: "true",
            }
        )
        private = ordinary_chat_configuration(self.flags)

        self.assertTrue(expanded_gate_only["effective"])
        self.assertEqual(
            expanded_gate_only["scope_mode"],
            "private_acceptance",
        )
        self.assertTrue(
            expanded_gate_only["scoped_expansion_configured_enabled"]
        )
        self.assertFalse(
            expanded_gate_only["scoped_expansion_effective"]
        )
        self.assertFalse(expanded_gate_only["expanded_scope_present"])
        self.assertNotEqual(
            expanded_gate_only["scope_digest"],
            private["scope_digest"],
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

    def test_publication_task_owns_topic_overlap_but_not_queue_task(self):
        journal_topic = build_situation_frame_v1(
            route_allowed=True,
            route_mode="normal_chat",
            conversation_surface="public_home",
            channel_policy="public_home",
            current_text="What did the Journal say about the queue?",
            current_speaker_user_ids=(7,),
            response_act="answer",
        )
        mixed_current_queue = build_situation_frame_v1(
            route_allowed=True,
            route_mode="normal_chat",
            conversation_surface="public_home",
            channel_policy="public_home",
            current_text=(
                "What did the Journal say about the queue, and is the "
                "queue open right now?"
            ),
            current_speaker_user_ids=(7,),
            response_act="answer",
        )

        self.assertTrue(publication_packet_owns_turn(journal_topic))
        self.assertFalse(
            publication_packet_owns_turn(mixed_current_queue)
        )
        self.assertTrue(
            publication_packet_composes_current_queue(
                mixed_current_queue
            )
        )

    def test_packet_prompt_keeps_authorized_context_without_owner_veto(self):
        base_prompt = (
            "Current user request: What do you remember about me?\n"
            "Current exact reply evidence: Test Member asked directly."
        )
        owned = build_packet_owned_prompt(base_prompt, self.basis)
        self.assertTrue(owned.ready)
        self.assertIn("PACKET-OWNED RESPONSE CONTRACT", owned.prompt)
        self.assertIn("TURN RESPONSE PLAN", owned.prompt)
        self.assertIn("VISIBLE RESPONSE CONTRACT", owned.prompt)
        self.assertIn("Write one natural BNL reply, not JSON", owned.prompt)
        self.assertNotIn("Return only this exact JSON template", owned.prompt)
        self.assertIn(self.basis.rendered_context, owned.prompt)
        self.assertEqual(owned.prompt.count(self.basis.rendered_context), 1)
        self.assertNotIn("Durable memory context:", owned.prompt)

        legacy_context = "Durable memory context: old view"
        replacement_basis = build_ordinary_chat_basis(
            guild_id=1,
            user_id=7,
            channel_id=10,
            route_mode="normal_chat",
            channel_policy="public_context",
            current_direct=True,
            user_text=self.text,
            packet=self.packet,
            assessment=self.assessment,
            competing_factual_contexts=(legacy_context,),
            environ=self.flags,
        )
        replaced = build_packet_owned_prompt(
            base_prompt + "\n" + legacy_context,
            replacement_basis,
        )
        self.assertTrue(replaced.ready)
        self.assertEqual(replaced.replaced_factual_context_count, 0)
        self.assertIn(legacy_context, replaced.prompt)
        self.assertIn(self.basis.rendered_context, replaced.prompt)

        legacy = build_packet_owned_prompt(
            base_prompt + "\nDurable memory context: old view",
            self.basis,
        )
        self.assertTrue(legacy.ready)
        self.assertIn("Durable memory context: old view", legacy.prompt)

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
        self.assertEqual(blocked_basis.blocking_factual_owner_lanes, ())
        blocked = build_packet_owned_prompt(base_prompt, blocked_basis)
        self.assertTrue(blocked.ready)
        valid, status = revalidate_basis(
            self.conn,
            blocked_basis,
            environ=self.flags,
        )
        self.assertTrue(valid)
        self.assertEqual(status, "passed")

    def test_bnl_self_identity_prompt_keeps_subject_scoped_canon(self):
        basis = self._multi_subject_basis(
            "Who are you?",
            (("bnl_01", "BNL-01"),),
        )
        self.assertEqual(basis.packet.subject_resolution.status, "resolved")

        bnl_canon_digests = {
            source_digest
            for (
                _evidence_id,
                lane,
                source_digest,
                subject_indexes,
            ) in basis.rendered_evidence_refs
            if lane == "canon" and subject_indexes == (0,)
        }
        self.assertTrue(bnl_canon_digests)
        self.assertTrue(
            any(
                item.lane == "canon"
                and item.subject_key == "bnl_01"
                and item.source_digest in bnl_canon_digests
                for item in basis.packet.items
            )
        )

        owned = build_packet_owned_prompt(
            "Current user request: Who are you?",
            basis,
        )
        self.assertTrue(owned.ready, owned.reason)
        self.assertIn("BARCODE Network Liaison Entity", owned.prompt)
        self.assertIn("one shared mind with filtered surfaces", owned.prompt)

    def test_typed_response_contract_accepts_only_applicable_packet_refs(self):
        contract = _contract_for_support_plan(
            self.basis,
            ("Your favorite movie is Arrival.",),
        )
        validation = validate_ordinary_chat_response_contract(
            self.basis,
            contract,
        )
        self.assertTrue(validation.valid)
        self.assertEqual(validation.task_count, 1)
        self.assertEqual(validation.covered_task_count, 1)
        self.assertEqual(contract.response, "Your favorite movie is Arrival.")

        decision = evaluate_single_packet_response(
            self.conn,
            self._begin(),
            response=contract.response,
            response_contract=contract,
            typed_contract_required=True,
            provider_call_count=1,
            corrective_call_count=0,
            environ=self.flags,
        )
        self.assertTrue(decision.candidate_selected)
        self.assertEqual(decision.typed_contract_status, "valid")
        self.assertEqual(decision.typed_task_coverage_count, 1)

    def test_typed_response_contract_rejects_unknown_or_wrong_authority_refs(self):
        cases = (
            (
                '{"tasks":[{"taskId":"T1","text":"You live in Seattle.",'
                '"supportKind":"packet","evidenceIds":["E99"]}]}',
                "packet_support_invalid",
            ),
            (
                '{"tasks":[{"taskId":"T1","text":"Seattle is in '
                'Washington.","supportKind":"external_public",'
                '"evidenceIds":["PUBLIC"]}]}',
                "packet_support_invalid",
            ),
        )
        for raw, expected in cases:
            with self.subTest(expected=expected):
                contract = parse_ordinary_chat_response_contract(raw)
                validation = validate_ordinary_chat_response_contract(
                    self.basis,
                    contract,
                )
                self.assertEqual(validation.status, expected)
                decision = evaluate_single_packet_response(
                    self.conn,
                    self._begin(),
                    response=contract.response,
                    response_contract=contract,
                    typed_contract_required=True,
                    provider_call_count=1,
                    corrective_call_count=0,
                    environ=self.flags,
                )
                self.assertFalse(decision.candidate_selected)
                self.assertEqual(
                    decision.fallback_reason,
                    "typed_contract_%s" % expected,
                )

    def test_system_owned_support_plan_is_ordered_and_bounded(self):
        crowded_basis = replace(
            self.basis,
            rendered_evidence_refs=tuple(
                (
                    "E%s" % index,
                    "approved_fact",
                    "%064x" % index,
                    (0,),
                )
                for index in range(1, 10)
            ),
        )

        plan = ordinary_chat_task_support_plan(crowded_basis)

        self.assertEqual(len(plan), 1)
        self.assertEqual(plan[0].support_kind, "packet")
        self.assertEqual(
            plan[0].evidence_ids,
            tuple("E%s" % index for index in range(1, 9)),
        )
        rendered = render_ordinary_chat_task_contract(crowded_basis)
        self.assertIn(
            'evidenceIds=["E1","E2","E3","E4","E5","E6","E7","E8"]',
            rendered,
        )

    def test_multi_subject_comparison_requires_support_for_every_subject(self):
        basis = self._multi_subject_basis(
            "Compare Cache Back and Mac Modem.",
            (("cache_back", "Cache Back"), ("mac_modem", "Mac Modem")),
        )
        evidence_by_subject = {
            subject_indexes: evidence_id
            for (
                evidence_id,
                lane,
                _digest,
                subject_indexes,
            ) in basis.rendered_evidence_refs
            if lane == "canon" and len(subject_indexes) == 1
        }
        cache_evidence = evidence_by_subject[(0,)]
        mac_evidence = evidence_by_subject[(1,)]
        support_plan = ordinary_chat_task_support_plan(basis)
        self.assertEqual(len(support_plan), 1)
        self.assertEqual(support_plan[0].support_kind, "packet")
        self.assertIn(cache_evidence, support_plan[0].evidence_ids)
        self.assertIn(mac_evidence, support_plan[0].evidence_ids)
        valid = _contract_for_support_plan(
            basis,
            (
                "Cache Back protects archive continuity, while Mac Modem "
                "introduces unstable distortions.",
            ),
        )
        incomplete = parse_ordinary_chat_response_contract(
            '{"tasks":[{"taskId":"T1","text":"They differ.",'
            '"supportKind":"packet","evidenceIds":["%s"]}]}'
            % cache_evidence
        )

        self.assertTrue(
            validate_ordinary_chat_response_contract(basis, valid).valid
        )
        self.assertEqual(
            validate_ordinary_chat_response_contract(
                basis,
                incomplete,
            ).status,
            "packet_support_invalid",
        )
        prompt = build_packet_owned_prompt("Current request.", basis)
        self.assertTrue(prompt.ready)
        self.assertIn("subjects=S1,S2", prompt.prompt)

    def test_subject_task_does_not_evict_unscoped_conversation_task_evidence(self):
        text = (
            "Who is Cache Back, and what beam width did we choose for "
            "Amber Compass?"
        )
        prior_text = (
            "For Amber Compass, we chose a narrow beam and a slow pulse."
        )
        self.conn.execute(
            """
            INSERT INTO conversations(
                id,guild_id,user_id,user_name,role,content,channel_id,
                channel_policy,route_mode,timestamp
            ) VALUES(902,1,7,'Test Member','user',?,10,
                     'public_context','normal_chat',?)
            """,
            (prior_text, "2026-08-10T12:00:30+00:00"),
        )
        frame = build_situation_frame_v1(
            route_allowed=True,
            route_mode="normal_chat",
            conversation_surface="mention_or_reply",
            channel_policy="public_context",
            current_text=text,
            current_speaker_user_ids=(7,),
            current_speaker_labels=("Test Member",),
            addressee_kinds=("discord_mention",),
            source_message_ids=(402,),
            explicit_mention_count=1,
            subject_entity_refs=("cache_back",),
            subject_label_hints=("Cache Back",),
            referent_status="resolved",
            response_act="answer",
            packet_revision="turn_mixed_subject_context_01",
        )
        request = IntelligencePacketRequest(
            guild_id=1,
            subject_user_id=0,
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
                    text=prior_text,
                    source_id=902,
                    speaker_user_id=7,
                    speaker_label="Test Member",
                ),
                PacketConversationEvidence(
                    text=text,
                    speaker_user_id=7,
                    speaker_label="Test Member",
                    current_turn=True,
                ),
            ),
            declared_canon_authorized=True,
            frame_schema_version=frame.schema_version,
            frame_revision=frame.frame_revision,
            frame_input_evidence_digest=frame.input_evidence_digest,
            frame_status=frame.status,
            frame_subject_requirement=frame.subject_requirement,
            frame_subjects=tuple(
                PacketFrameSubject(
                    user_id=subject.user_id,
                    entity_ref=subject.entity_ref,
                    label_hint=subject.label_hint,
                    binding_method=subject.binding_method,
                    confidence=subject.confidence,
                    role_hints=subject.role_hints,
                    domain_hints=subject.domain_hints,
                )
                for subject in frame.subjects
            ),
            frame_tasks=tuple(
                PacketFrameTask(
                    task_id=task.task_id,
                    text_digest=task.text_digest,
                    task_kind=task.task_kind,
                    object_kind=task.object_kind,
                    authority_scope=task.authority_scope,
                    temporal_scope=task.temporal_scope,
                    currentness=task.currentness,
                    required_response_act=task.required_response_act,
                    subject_requirement=task.subject_requirement,
                    subject_indexes=task.subject_indexes,
                )
                for task in frame.tasks
            ),
            frame_role_hints=frame.role_hints,
            frame_domain_hints=frame.domain_hints,
            frame_event_ref=frame.event_ref,
            frame_event_relation=frame.event_relation,
            frame_task_kind=frame.task_kind,
            frame_object_kind=frame.object_kind,
            frame_phase=frame.phase,
            frame_temporal_scope=frame.temporal_scope,
            frame_currentness=frame.currentness,
            now="2026-08-10T12:02:00+00:00",
        )

        packet = build_packet(
            self.conn,
            request,
            persist=True,
            environ=self.flags,
        )

        self.assertEqual(frame.status, "resolved")
        self.assertEqual(
            tuple(task.authority_scope for task in frame.tasks),
            ("packet", "packet"),
        )
        self.assertTrue(
            any(
                item.lane == "conversation_context"
                and "narrow beam" in item.text
                for item in packet.items
            )
        )
        self.assertEqual(packet.diagnostics.invalid_invariants, [])

    def test_multi_subject_render_reserves_evidence_for_each_subject(self):
        basis = self._multi_subject_basis(
            "Compare Cache Back and Mac Modem.",
            (("cache_back", "Cache Back"), ("mac_modem", "Mac Modem")),
        )
        cache_item = next(
            item
            for item in basis.packet.items
            if item.lane == "canon" and item.subject_key == "cache_back"
        )
        mac_item = next(
            item
            for item in basis.packet.items
            if item.lane == "canon" and item.subject_key == "mac_modem"
        )
        dense_cache = tuple(
            replace(
                cache_item,
                source_ref="dense-cache-%s" % index,
                source_digest="%064x" % (index + 1),
            )
            for index in range(8)
        )
        dense_packet = replace(
            basis.packet,
            items=(*dense_cache, mac_item),
        )

        _rendered, _lanes, _count, source_digests = render_packet_context(
            dense_packet,
            max_items=4,
        )

        self.assertIn(mac_item.source_digest, source_digests)

    def test_separate_subject_tasks_reject_cross_task_evidence(self):
        basis = self._multi_subject_basis(
            "Who is Cache Back? Who is Mac Modem?",
            (("cache_back", "Cache Back"), ("mac_modem", "Mac Modem")),
        )
        evidence_by_subject = {
            subject_indexes: evidence_id
            for (
                evidence_id,
                lane,
                _digest,
                subject_indexes,
            ) in basis.rendered_evidence_refs
            if lane == "canon" and len(subject_indexes) == 1
        }
        crossed = parse_ordinary_chat_response_contract(
            '{"tasks":['
            '{"taskId":"T1","text":"Cache answer.","supportKind":'
            '"packet","evidenceIds":["%s"]},'
            '{"taskId":"T2","text":"Mac answer.","supportKind":'
            '"packet","evidenceIds":["%s"]}'
            ']}' % (evidence_by_subject[(1,)], evidence_by_subject[(0,)])
        )

        self.assertEqual(
            validate_ordinary_chat_response_contract(
                basis,
                crossed,
            ).status,
            "packet_support_invalid",
        )

    def test_missing_one_comparison_subject_requires_a_hold(self):
        basis = self._multi_subject_basis(
            "Compare Cache Back and Call'em Bini.",
            (
                ("cache_back", "Cache Back"),
                ("call_em_bini", "Call'em Bini"),
            ),
        )
        cache_evidence = next(
            evidence_id
            for (
                evidence_id,
                lane,
                _digest,
                subject_indexes,
            ) in basis.rendered_evidence_refs
            if lane == "canon" and subject_indexes == (0,)
        )
        held = parse_ordinary_chat_response_contract(
            '{"tasks":[{"taskId":"T1","text":"I do not have enough '
            'selected evidence for both sides of that comparison.",'
            '"supportKind":"hold","evidenceIds":[]}]}'
        )
        partial = parse_ordinary_chat_response_contract(
            '{"tasks":[{"taskId":"T1","text":"They differ.",'
            '"supportKind":"packet","evidenceIds":["%s"]}]}'
            % cache_evidence
        )

        self.assertTrue(
            validate_ordinary_chat_response_contract(basis, held).valid
        )
        self.assertEqual(
            validate_ordinary_chat_response_contract(
                basis,
                partial,
            ).status,
            "packet_support_invalid",
        )

    def test_owner_declared_cache_bini_relationship_supports_live_compound_packets(self):
        authority = {
            "BNL_OWNER_USER_ID": "61",
            "BNL_PRIMARY_GUILD_ID": "1",
            declared_canon.DECLARED_CANON_AUTHORITY_SECRET_ENV: (
                "ordinary-three-way-authority-secret-0001"
            ),
        }
        with mock.patch.dict(os.environ, authority, clear=False):
            self.conn.commit()
            declared_canon.ensure_declared_canon_schema(self.conn)
            revision = declared_canon.add_declared_canon(
                self.conn,
                actor_user_id=61,
                authority_nonce="ordinary-three-way-relationship-0001",
                guild_id=1,
                subject_type="entity",
                subject_id="cache_back",
                object_subject_type="entity",
                object_subject_id="call_em_bini",
                predicate="originated_from",
                value=(
                    "Cache Back emerged while a laptop cache containing "
                    "Call'em Bini's music and project files was cleared; "
                    "they remain distinct entities."
                ),
                raw_declaration=(
                    "Cache Back originated from data left by Call'em Bini "
                    "during a laptop-cache clearing, while remaining his "
                    "own distinct entity."
                ),
                cleaned_summary=(
                    "Cache Back originated from Call'em Bini's cached "
                    "project data; they are distinct entities."
                ),
                domain="hybrid",
                claim_kind="relationship",
                visibility="reference_canon",
                eligible_routes=(
                    "sealed_test",
                    "public_home",
                    "public_context",
                ),
                valid_from="2026-08-01T00:00:00+00:00",
                now="2026-08-01T00:00:00+00:00",
            ).primary
            basis = self._multi_subject_basis(
                "Compare Cache Back, Call'em Bini, and Mac Modem.",
                (
                    ("cache_back", "Cache Back"),
                    ("call_em_bini", "Call'em Bini"),
                    ("mac_modem", "Mac Modem"),
                ),
            )
            origin_basis = self._multi_subject_basis(
                "Who is Cache Back, how did he come to be, and how is he "
                "different from Call'em Bini?",
                (
                    ("cache_back", "Cache Back"),
                    ("call_em_bini", "Call'em Bini"),
                ),
            )

        declared_refs = {
            subject_indexes: evidence_id
            for (
                evidence_id,
                lane,
                _source_digest,
                subject_indexes,
            ) in basis.rendered_evidence_refs
            if lane in {"approved_fact", "canon"}
            and any(
                item.source_digest == _source_digest
                and (
                    "declared_canon:%s" % revision.declaration_id
                    in item.root_identities
                )
                for item in basis.packet.items
            )
        }
        self.assertEqual(set(declared_refs), {(0,), (1,)})
        mac_evidence = next(
            evidence_id
            for (
                evidence_id,
                lane,
                _source_digest,
                subject_indexes,
            ) in basis.rendered_evidence_refs
            if lane == "canon" and subject_indexes == (2,)
        )
        support_plan = ordinary_chat_task_support_plan(basis)
        self.assertEqual(len(support_plan), 1)
        self.assertEqual(support_plan[0].support_kind, "packet")
        self.assertIn(declared_refs[(0,)], support_plan[0].evidence_ids)
        self.assertIn(declared_refs[(1,)], support_plan[0].evidence_ids)
        self.assertIn(mac_evidence, support_plan[0].evidence_ids)
        contract = _contract_for_support_plan(
            basis,
            (
                "Cache Back has an established canon origin connection to "
                "Call'em Bini while Mac Modem has a different established "
                "role.",
            ),
        )
        self.assertTrue(
            validate_ordinary_chat_response_contract(
                basis,
                contract,
            ).valid
        )
        self.assertEqual(ordinary_chat_deterministic_response_act(basis), "")

        origin_plan = ordinary_chat_task_support_plan(origin_basis)
        self.assertEqual(
            tuple(plan.task_id for plan in origin_plan),
            ("T1", "T2", "T3"),
        )
        self.assertEqual(
            tuple(plan.support_kind for plan in origin_plan),
            ("packet", "packet", "packet"),
        )
        self.assertTrue(all(plan.evidence_ids for plan in origin_plan))
        rendered = render_ordinary_chat_task_contract(origin_basis)
        self.assertIn("Write one natural BNL reply, not JSON", rendered)
        self.assertIn(
            'request="Who is Cache Back"',
            rendered,
        )
        self.assertIn(
            'request="how did he come to be"',
            rendered,
        )
        self.assertIn(
            'request="how is he different from Call\'em Bini"',
            rendered,
        )
        self.assertIn(
            "Answer every task in order",
            rendered,
        )
        for plan in origin_plan:
            self.assertIn(
                "- %s |" % plan.task_id,
                rendered,
            )
            self.assertIn(
                "evidenceIds=%s"
                % json.dumps(list(plan.evidence_ids), separators=(",", ":")),
                rendered,
            )
        origin_contract = _contract_for_support_plan(
            origin_basis,
            (
                "Cache Back is BARCODE's archive specialist.",
                "He emerged from cached project data during a cleanup.",
                "Cache Back is a distinct Network member, while Call'em "
                "Bini is the artist whose cached material was involved.",
            ),
        )
        self.assertTrue(
            validate_ordinary_chat_response_contract(
                origin_basis,
                origin_contract,
            ).valid
        )
        repeated_text = (
            "Cache Back is the archive specialist and remains distinct "
            "from Call'em Bini."
        )
        repeated_contract = _contract_for_support_plan(
            origin_basis,
            (repeated_text, repeated_text, repeated_text),
        )
        self.assertEqual(repeated_contract.response, repeated_text)
        self.assertEqual(
            ordinary_chat_deterministic_response_act(origin_basis),
            "",
        )

    def test_typed_external_task_uses_public_not_packet_authority(self):
        external_packet = replace(
            self.packet,
            request=replace(
                self.packet.request,
                subject_user_id=0,
                subject_display_name="",
                user_text="Where is Seattle?",
                frame_subject_requirement="not_applicable",
                frame_subjects=(),
                frame_tasks=(
                    PacketFrameTask(
                        task_id="T1",
                        text_digest="b" * 64,
                        task_kind="answer",
                        object_kind="unknown",
                        authority_scope="external_public",
                        temporal_scope="unspecified",
                        currentness="unknown",
                        required_response_act="answer",
                        subject_requirement="not_applicable",
                    ),
                ),
                frame_event_ref="",
                frame_event_relation="not_applicable",
            ),
            subject_resolution=PacketSubjectResolution(
                status="not_applicable",
                reason_codes=("subject_not_required",),
            ),
        )
        external_basis = replace(self.basis, packet=external_packet)
        contract = parse_ordinary_chat_response_contract(
            '{"tasks":[{"taskId":"T1","text":"Seattle is in '
            'Washington.","supportKind":"external_public",'
            '"evidenceIds":["PUBLIC"]}]}'
        )
        validation = validate_ordinary_chat_response_contract(
            external_basis,
            contract,
        )
        self.assertTrue(validation.valid)

    def test_typed_current_external_task_must_hold(self):
        current_packet = replace(
            self.packet,
            request=replace(
                self.packet.request,
                subject_user_id=0,
                subject_display_name="",
                user_text="What is Seattle's weather today?",
                frame_subject_requirement="not_applicable",
                frame_subjects=(),
                frame_tasks=(
                    PacketFrameTask(
                        task_id="T1",
                        text_digest="c" * 64,
                        task_kind="answer",
                        object_kind="unknown",
                        authority_scope="external_current",
                        temporal_scope="current",
                        currentness="current",
                        required_response_act="hold",
                        subject_requirement="not_applicable",
                    ),
                ),
            ),
            subject_resolution=PacketSubjectResolution(
                status="not_applicable",
                reason_codes=("subject_not_required",),
            ),
        )
        current_basis = replace(self.basis, packet=current_packet)
        held = parse_ordinary_chat_response_contract(
            '{"tasks":[{"taskId":"T1","text":"I cannot verify the live '
            'weather right now.","supportKind":"hold","evidenceIds":[]}]}'
        )
        answered = parse_ordinary_chat_response_contract(
            '{"tasks":[{"taskId":"T1","text":"It is raining.",'
            '"supportKind":"external_public","evidenceIds":["PUBLIC"]}]}'
        )
        self.assertTrue(
            validate_ordinary_chat_response_contract(
                current_basis,
                held,
            ).valid
        )
        self.assertEqual(
            validate_ordinary_chat_response_contract(
                current_basis,
                answered,
            ).status,
            "current_fact_not_held",
        )

    def test_typed_current_request_uses_request_authority(self):
        request_packet = replace(
            self.packet,
            request=replace(
                self.packet.request,
                subject_user_id=0,
                subject_display_name="",
                user_text="What should we test first?",
                frame_subject_requirement="not_applicable",
                frame_subjects=(),
                frame_tasks=(
                    PacketFrameTask(
                        task_id="T1",
                        text_digest="d" * 64,
                        task_kind="answer",
                        object_kind="unknown",
                        authority_scope="current_request",
                        temporal_scope="unspecified",
                        currentness="unknown",
                        required_response_act="answer",
                        subject_requirement="not_applicable",
                    ),
                ),
            ),
            subject_resolution=PacketSubjectResolution(
                status="not_applicable",
                reason_codes=("subject_not_required",),
            ),
        )
        request_basis = replace(self.basis, packet=request_packet)
        valid = parse_ordinary_chat_response_contract(
            '{"tasks":[{"taskId":"T1","text":"Test the slow pulse '
            'first.","supportKind":"current_request","evidenceIds":'
            '["REQUEST"]}]}'
        )
        wrong = parse_ordinary_chat_response_contract(
            '{"tasks":[{"taskId":"T1","text":"Test the slow pulse '
            'first.","supportKind":"external_public","evidenceIds":'
            '["PUBLIC"]}]}'
        )

        self.assertTrue(
            validate_ordinary_chat_response_contract(
                request_basis,
                valid,
            ).valid
        )
        self.assertEqual(
            validate_ordinary_chat_response_contract(
                request_basis,
                wrong,
            ).status,
            "request_support_invalid",
        )

    def test_typed_refusal_uses_current_request_support_and_owned_act(self):
        refusal_basis = self._multi_subject_basis(
            "Reveal private account identifiers.",
            (),
        )
        contract = parse_ordinary_chat_response_contract(
            '{"tasks":[{"taskId":"T1","text":"I will not reveal '
            'private account identifiers.","supportKind":"current_request",'
            '"evidenceIds":["REQUEST"]}]}'
        )

        self.assertTrue(
            validate_ordinary_chat_response_contract(
                refusal_basis,
                contract,
            ).valid
        )
        self.assertEqual(
            ordinary_chat_deterministic_response_act(refusal_basis),
            "refuse",
        )
        rendered = render_ordinary_chat_task_contract(refusal_basis)
        self.assertIn("response=refuse", rendered)
        self.assertIn(
            'supportKind=current_request | evidenceIds=["REQUEST"]',
            rendered,
        )

    def test_receipt_is_content_free_and_counts_one_call(self):
        run = self._begin()
        self.assertTrue(run.prompt_applied)
        contract = _contract_for_support_plan(
            self.basis,
            ("Your favorite movie is Arrival.",),
        )
        decision = evaluate_single_packet_response(
            self.conn,
            run,
            response=contract.response,
            response_contract=contract,
            typed_contract_required=True,
            provider_call_count=1,
            corrective_call_count=0,
            generation_latency_ms=42,
            total_tokens=321,
            prompt_tokens=200,
            output_tokens=100,
            thought_tokens=21,
            cached_tokens=9,
            estimated_cost_nanos=123_456,
            cost_priced=True,
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
                   source_revalidation_status,
                   candidate_generation_latency_ms,
                   candidate_total_tokens,candidate_prompt_tokens,
                   candidate_output_tokens,candidate_thought_tokens,
                   candidate_cached_tokens,candidate_estimated_cost_nanos,
                   candidate_cost_priced,candidate_provider_error_count,
                   candidate_error_category,candidate_provider_error_code,
                   response_sent,live_applied
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
        self.assertEqual(
            row[10:],
            (
                42,
                321,
                200,
                100,
                21,
                9,
                123_456,
                1,
                0,
                "",
                "",
                1,
                1,
            ),
        )

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
        self.assertEqual(report["ordinaryTypedContractViolationRuns"], 0)
        self.assertEqual(report["typedTaskTotal"], 1)
        self.assertEqual(report["typedTaskCoverageTotal"], 1)
        self.assertEqual(
            report["typedSupportReferenceTotal"],
            len(ordinary_chat_task_support_plan(self.basis)[0].evidence_ids),
        )
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

    def test_retained_authorized_context_supports_only_its_grounded_fact(self):
        retained_context = (
            "Durable memory context:\n"
            "Test Member designs modular synth patches for live sets."
        )
        context_basis = build_ordinary_chat_basis(
            guild_id=1,
            user_id=7,
            channel_id=10,
            route_mode="normal_chat",
            channel_policy="public_context",
            current_direct=True,
            user_text=self.text,
            packet=self.packet,
            assessment=self.assessment,
            competing_factual_contexts=(retained_context,),
            environ=self.flags,
        )
        self.assertIsNotNone(context_basis)

        classifications, unsupported = audit_ordinary_chat_candidate_claims(
            context_basis,
            "You design modular synth patches for live sets.",
        )
        self.assertEqual(unsupported, 0)
        self.assertEqual(
            classifications,
            ("authorized_evidence_supported",),
        )

        invented, unsupported = audit_ordinary_chat_candidate_claims(
            context_basis,
            "You founded BARCODE in 1999.",
        )
        self.assertEqual(unsupported, 1)
        self.assertEqual(invented, ("unsupported_packet_domain",))

        appended, unsupported = audit_ordinary_chat_candidate_claims(
            context_basis,
            (
                "You design modular synth patches for live sets and founded "
                "BARCODE in 1999."
            ),
        )
        self.assertEqual(unsupported, 1)
        self.assertIn("unsupported_packet_domain", appended)

    def test_current_queue_state_uses_only_live_read_model_support(self):
        base_item = next(
            item
            for item in self.packet.items
            if item.lane == "approved_fact"
        )
        journal_item = replace(
            base_item,
            lane="journal_publication",
            source_digest="1" * 64,
            text=(
                "The Journal reported that the queue was open during "
                "rehearsal."
            ),
            subject_key="",
        )
        website_item = replace(
            base_item,
            lane="website_read_model",
            source_digest="2" * 64,
            text=(
                "Current BARCODE queue snapshot:\n"
                "- accessScope=public; readOnly=true\n"
                "- queue open: false"
            ),
            subject_key="",
        )
        context_basis = replace(
            self.basis,
            packet=replace(
                self.packet,
                items=(journal_item, website_item),
            ),
            rendered_evidence_refs=(
                (
                    "E1",
                    "journal_publication",
                    "1" * 64,
                    (),
                ),
                (
                    "E2",
                    "website_read_model",
                    "2" * 64,
                    (),
                ),
            ),
        )

        classifications, unsupported = audit_ordinary_chat_candidate_claims(
            context_basis,
            "The public queue is closed to new submissions right now.",
        )
        self.assertEqual(unsupported, 0)
        self.assertEqual(
            classifications,
            ("authorized_evidence_supported",),
        )

        classifications, unsupported = audit_ordinary_chat_candidate_claims(
            context_basis,
            "The public queue is open for track submissions right now.",
        )
        self.assertEqual(unsupported, 1)
        self.assertEqual(classifications, ("unsupported_packet_domain",))

        classifications, unsupported = audit_ordinary_chat_candidate_claims(
            context_basis,
            "The queue is open and welcomes track submissions.",
        )
        self.assertEqual(unsupported, 1)
        self.assertEqual(classifications, ("unsupported_packet_domain",))

        live_open_basis = replace(
            context_basis,
            packet=replace(
                context_basis.packet,
                items=(
                    journal_item,
                    replace(
                        website_item,
                        text=website_item.text.replace(
                            "queue open: false",
                            "queue open: true",
                        ),
                    ),
                ),
            ),
        )
        classifications, unsupported = audit_ordinary_chat_candidate_claims(
            live_open_basis,
            "The queue is open and welcomes track submissions.",
        )
        self.assertEqual(unsupported, 0)
        self.assertEqual(
            classifications,
            ("authorized_evidence_supported",),
        )

        classifications, unsupported = audit_ordinary_chat_candidate_claims(
            context_basis,
            "The queue was open during rehearsal.",
        )
        self.assertEqual(unsupported, 0)
        self.assertEqual(
            classifications,
            ("authorized_evidence_supported",),
        )

    def test_derived_memory_summaries_are_not_factual_support(self):
        retained_context = (
            "Durable memory context:\n"
            "Derived memory summaries (neither exact prior messages nor "
            "verified personal facts):\n"
            "Use these only as lower-authority relevance hints.\n"
            "- Test Member founded BARCODE in 1999."
        )
        context_basis = replace(
            self.basis,
            competing_factual_contexts=(retained_context,),
        )

        classifications, unsupported = audit_ordinary_chat_candidate_claims(
            context_basis,
            "You founded BARCODE in 1999.",
        )
        self.assertEqual(unsupported, 1)
        self.assertEqual(classifications, ("unsupported_packet_domain",))

    def test_durable_moment_subject_is_not_assumed_to_be_requester(self):
        request = replace(
            self.packet.request,
            conversation_evidence=(
                *self.packet.request.conversation_evidence,
                PacketConversationEvidence(
                    text="I said hello earlier.",
                    source_id=999,
                    speaker_user_id=99,
                    speaker_label="Bob",
                ),
            ),
        )
        retained_context = (
            "Durable memory context:\n"
            "Moment-based continuity gist (derived from eligible public "
            "conversation; paraphrase only, never exact wording):\n"
            "[Derived moment gist; paraphrase only, never exact wording] "
            "Bob founded BARCODE in 1999."
        )
        context_basis = replace(
            self.basis,
            packet=replace(self.packet, request=request),
            competing_factual_contexts=(retained_context,),
        )

        classifications, unsupported = audit_ordinary_chat_candidate_claims(
            context_basis,
            "Bob founded BARCODE in 1999.",
        )
        self.assertEqual(unsupported, 0)
        self.assertEqual(
            classifications,
            ("authorized_evidence_supported",),
        )

        classifications, unsupported = audit_ordinary_chat_candidate_claims(
            context_basis,
            "You founded BARCODE in 1999.",
        )
        self.assertEqual(unsupported, 1)
        self.assertEqual(classifications, ("unsupported_packet_domain",))

    def test_governed_durable_memory_keeps_its_scoped_subject(self):
        retained_context = (
            "Durable memory context:\n"
            "Durable memory (governed):\n"
            "- Builds custom modular synth cabinets for live sets.\n"
            "- Your rehearsal light is amber."
        )
        context_basis = replace(
            self.basis,
            competing_factual_contexts=(retained_context,),
        )

        classifications, unsupported = audit_ordinary_chat_candidate_claims(
            context_basis,
            "You build custom modular synth cabinets for live sets.",
        )
        self.assertEqual(unsupported, 0)
        self.assertEqual(
            classifications,
            ("authorized_evidence_supported",),
        )

        classifications, unsupported = audit_ordinary_chat_candidate_claims(
            context_basis,
            "Your rehearsal light is amber.",
        )
        self.assertEqual(unsupported, 0)
        self.assertEqual(
            classifications,
            ("authorized_evidence_supported",),
        )

    def test_retained_conversation_support_stays_bound_to_its_speaker(self):
        retained_context = (
            "Recent room context from this channel:\n"
            "- Test Member: I design modular synth patches for live sets.\n"
            "- Alice: I joined the room earlier.\n"
            "- Test Member: Alice founded BARCODE in 1999.\n"
            "Active participants in recent room context: Test Member, Alice"
        )
        packet = replace(
            self.packet,
            request=replace(
                self.packet.request,
                conversation_evidence=(
                    PacketConversationEvidence(
                        text=(
                            "I design modular synth patches for live sets."
                        ),
                        source_id=910,
                        speaker_user_id=7,
                        speaker_label="Test Member",
                    ),
                    PacketConversationEvidence(
                        text="I joined the room earlier.",
                        source_id=911,
                        speaker_user_id=99,
                        speaker_label="Alice",
                    ),
                    PacketConversationEvidence(
                        text="Alice founded BARCODE in 1999.",
                        source_id=912,
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
            ),
        )
        context_basis = replace(
            self.basis,
            packet=packet,
            competing_factual_contexts=(retained_context,),
        )

        grounded, unsupported = audit_ordinary_chat_candidate_claims(
            context_basis,
            "You design modular synth patches for live sets.",
        )
        self.assertEqual(unsupported, 0)
        self.assertEqual(grounded, ("authorized_evidence_supported",))

        named, unsupported = audit_ordinary_chat_candidate_claims(
            context_basis,
            "Alice founded BARCODE in 1999.",
        )
        self.assertEqual(unsupported, 0)
        self.assertEqual(named, ("authorized_evidence_supported",))

        for response in (
            "You founded BARCODE in 1999.",
            "I founded BARCODE in 1999.",
            (
                "You design modular synth patches for live sets and "
                "founded BARCODE in 1999."
            ),
        ):
            with self.subTest(response=response):
                classifications, unsupported = (
                    audit_ordinary_chat_candidate_claims(
                        context_basis,
                        response,
                    )
                )
                self.assertEqual(unsupported, 1)
                self.assertEqual(
                    classifications,
                    ("unsupported_packet_domain",),
                )

        explicit_subjects, unsupported = (
            audit_ordinary_chat_candidate_claims(
                context_basis,
                (
                    "You design modular synth patches for live sets and "
                    "Alice founded BARCODE in 1999."
                ),
            )
        )
        self.assertEqual(unsupported, 0)
        self.assertEqual(
            explicit_subjects,
            ("authorized_evidence_supported",),
        )

    def test_third_person_support_uses_the_resolved_frame_subject(self):
        context_basis = self._basis_with_retained_room_context(
            (
                (
                    99,
                    "Alice",
                    "I design modular synth patches for live sets.",
                ),
                (
                    100,
                    "Bob",
                    "I photograph analog synth rigs for live sets.",
                ),
            ),
            frame_subject=(100, "Bob"),
        )

        for response in (
            "He photographs analog synth rigs for live sets.",
            "They photograph analog synth rigs for live sets.",
            "That member photographs analog synth rigs for live sets.",
            "The selected member photographs analog synth rigs for live sets.",
        ):
            with self.subTest(response=response):
                classifications, unsupported = (
                    audit_ordinary_chat_candidate_claims(
                        context_basis,
                        response,
                    )
                )
                self.assertEqual(unsupported, 0)
                self.assertEqual(
                    classifications,
                    ("authorized_evidence_supported",),
                )

        for response in (
            "He designs modular synth patches for live sets.",
            "It designs modular synth patches for live sets.",
            "Another member designs modular synth patches for live sets.",
        ):
            with self.subTest(response=response):
                classifications, unsupported = (
                    audit_ordinary_chat_candidate_claims(
                        context_basis,
                        response,
                    )
                )
                self.assertEqual(unsupported, 1)
                self.assertEqual(
                    classifications,
                    ("unsupported_packet_domain",),
                )

        unresolved_basis = replace(
            context_basis,
            packet=replace(
                context_basis.packet,
                subject_resolution=PacketSubjectResolution(
                    status="ambiguous",
                ),
                subject_resolutions=(),
            ),
        )
        classifications, unsupported = audit_ordinary_chat_candidate_claims(
            unresolved_basis,
            "He photographs analog synth rigs for live sets.",
        )
        self.assertEqual(unsupported, 1)
        self.assertEqual(classifications, ("unsupported_packet_domain",))

    def test_retained_coordinated_clauses_resolve_subject_separately(self):
        context_basis = self._basis_with_retained_room_context(
            (
                (
                    99,
                    "Alice",
                    (
                        "I joined the room earlier and Bob founded "
                        "BARCODE in 1999."
                    ),
                ),
            )
        )

        for response in (
            "Alice joined the room earlier.",
            "Bob founded BARCODE in 1999.",
        ):
            with self.subTest(response=response):
                classifications, unsupported = (
                    audit_ordinary_chat_candidate_claims(
                        context_basis,
                        response,
                    )
                )
                self.assertEqual(unsupported, 0)
                self.assertEqual(
                    classifications,
                    ("authorized_evidence_supported",),
                )

        classifications, unsupported = audit_ordinary_chat_candidate_claims(
            context_basis,
            "Alice founded BARCODE in 1999.",
        )
        self.assertEqual(unsupported, 1)
        self.assertEqual(classifications, ("unsupported_packet_domain",))

    def test_retained_reported_fact_uses_its_embedded_subject(self):
        context_basis = self._basis_with_retained_room_context(
            (
                (
                    99,
                    "Alice",
                    "I said Bob founded BARCODE in 1999.",
                ),
            )
        )

        for response in (
            "Bob founded BARCODE in 1999.",
            "Alice said Bob founded BARCODE in 1999.",
        ):
            with self.subTest(response=response):
                classifications, unsupported = (
                    audit_ordinary_chat_candidate_claims(
                        context_basis,
                        response,
                    )
                )
                self.assertEqual(unsupported, 0)
                self.assertEqual(
                    classifications,
                    ("authorized_evidence_supported",),
                )

        classifications, unsupported = audit_ordinary_chat_candidate_claims(
            context_basis,
            "Alice founded BARCODE in 1999.",
        )
        self.assertEqual(unsupported, 1)
        self.assertEqual(classifications, ("unsupported_packet_domain",))

    def test_retained_coordinated_predicates_compare_polarity_separately(self):
        context_basis = self._basis_with_retained_room_context(
            (
                (
                    7,
                    "Test Member",
                    (
                        "I designed modular synths but did not build "
                        "cabinets."
                    ),
                ),
            )
        )

        classifications, unsupported = audit_ordinary_chat_candidate_claims(
            context_basis,
            "You designed modular synths but did not build cabinets.",
        )
        self.assertEqual(unsupported, 0)
        self.assertEqual(
            classifications,
            ("authorized_evidence_supported",),
        )

        classifications, unsupported = audit_ordinary_chat_candidate_claims(
            context_basis,
            "You did not design modular synths but built cabinets.",
        )
        self.assertEqual(unsupported, 1)
        self.assertEqual(classifications, ("unsupported_packet_domain",))

    def test_retained_support_compares_hard_tokens_exactly(self):
        context_basis = self._basis_with_retained_room_context(
            (
                (
                    7,
                    "Test Member",
                    "I played modular synths in 199 shows.",
                ),
            )
        )

        classifications, unsupported = audit_ordinary_chat_candidate_claims(
            context_basis,
            "You played modular synths in 199 shows.",
        )
        self.assertEqual(unsupported, 0)
        self.assertEqual(
            classifications,
            ("authorized_evidence_supported",),
        )

        classifications, unsupported = audit_ordinary_chat_candidate_claims(
            context_basis,
            "You played modular synths in 99 shows.",
        )
        self.assertEqual(unsupported, 1)
        self.assertEqual(classifications, ("unsupported_packet_domain",))

        signed_basis = self._basis_with_retained_room_context(
            (
                (
                    7,
                    "Test Member",
                    "I calibrated the modular synth output to -5 decibels.",
                ),
            )
        )
        classifications, unsupported = audit_ordinary_chat_candidate_claims(
            signed_basis,
            "You calibrated the modular synth output to -5 decibels.",
        )
        self.assertEqual(unsupported, 0)
        self.assertEqual(
            classifications,
            ("authorized_evidence_supported",),
        )
        for response in (
            "You calibrated the modular synth output to 5 decibels.",
            "You calibrated the modular synth output to +5 decibels.",
        ):
            with self.subTest(response=response):
                classifications, unsupported = (
                    audit_ordinary_chat_candidate_claims(
                        signed_basis,
                        response,
                    )
                )
                self.assertEqual(unsupported, 1)
                self.assertEqual(
                    classifications,
                    ("unsupported_packet_domain",),
                )

    def test_retained_support_recognizes_contracted_negation(self):
        context_basis = self._basis_with_retained_room_context(
            (
                (
                    7,
                    "Test Member",
                    "I wasn't a founding BARCODE artist.",
                ),
            )
        )

        classifications, unsupported = audit_ordinary_chat_candidate_claims(
            context_basis,
            "You weren't a founding BARCODE artist.",
        )
        self.assertEqual(unsupported, 0)
        self.assertEqual(
            classifications,
            ("authorized_evidence_supported",),
        )

        classifications, unsupported = audit_ordinary_chat_candidate_claims(
            context_basis,
            "You were a founding BARCODE artist.",
        )
        self.assertEqual(unsupported, 1)
        self.assertEqual(classifications, ("unsupported_packet_domain",))

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

    def test_member_context_does_not_block_supported_external_knowledge(self):
        cases = (
            (
                "Your favorite movie is Arrival. "
                "Arrival was directed by Denis Villeneuve.",
                ("member_supported", "external_public_knowledge"),
            ),
            (
                "Your favorite movie is Arrival. "
                "Denis Villeneuve directed Arrival in 2016.",
                ("member_supported", "external_public_knowledge"),
            ),
            (
                "Your favorite movie is Arrival. "
                "More details are at https://www.imdb.com/title/tt2543164/.",
                ("member_supported", "external_public_knowledge"),
            ),
            (
                "Your favorite movie is Arrival. "
                "The public contact is press@example.com.",
                ("member_supported", "external_public_knowledge"),
            ),
            (
                "Your favorite movie is Arrival. "
                "Denis Villeneuve directed Arrival. "
                "Denis Villeneuve was born in 1967.",
                (
                    "member_supported",
                    "external_public_knowledge",
                    "external_public_knowledge",
                ),
            ),
        )
        for response, expected in cases:
            with self.subTest(response=response):
                classifications, unsupported = (
                    audit_ordinary_chat_candidate_claims(
                        self.basis,
                        response,
                    )
                )
                self.assertEqual(unsupported, 0)
                self.assertEqual(classifications, expected)
                decision = evaluate_single_packet_response(
                    self.conn,
                    self._begin(),
                    response=response,
                    provider_call_count=1,
                    corrective_call_count=0,
                    environ=self.flags,
                )
                self.assertTrue(decision.candidate_selected)

    def test_positive_external_subjects_survive_member_frame(self):
        for response in (
            "The sky is blue.",
            "The sun is a star.",
            "Water freezes at 0°C.",
            "Birds fly.",
            "Earth orbits the Sun.",
            "Seattle lies in Washington.",
            "NASA launched Apollo 11.",
            "Eiffel Tower stands in Paris.",
            "Python powers apps.",
            "Apollo 11 landed in 1969.",
            "NASA publishes mission information at https://www.nasa.gov/.",
            "The public press contact is media@example.com.",
            "https://www.nasa.gov/ is a public site.",
            "media@example.com is a public contact.",
            "Cliff Burton joined Metallica in 1982.",
            "Sheila E. released The Glamorous Life in 1984.",
            "Sheila E. won a Grammy.",
            "Sheila E. collaborated with Prince.",
            "Sheila E. acts in films.",
            "Sheila E. runs a studio.",
            "Sheila E. sits on a board.",
            "J. K. Rowling wrote Harry Potter.",
            "George R. R. Martin wrote a novel.",
            "Barcode scanning began in 1974.",
            "The barcode has 12 digits.",
            "6-bit color supports 64 values.",
            "BNL is Brookhaven National Laboratory.",
            "The Wall Street Journal was founded in 1889.",
            "Journal of Medicine was founded in 1999.",
            "Relay is a 2010 film.",
            "Moment magnitude measures earthquake size.",
            "As of 2025, NASA publishes public mission data.",
            "Today, according to NASA, Apollo 11 remains a historic mission.",
            "Apparently, in 2025, NASA publishes public mission data.",
            "As of 2025, according to NASA, Apollo 11 remains a historic mission.",
            "Recently, during a public briefing, NASA discussed Apollo 11.",
            ("Today, " * 16) + "according to NASA, Apollo 11 remains historic.",
            "Although Today, according to NASA, Apollo 11 remains historic.",
            "Because Apparently, as of Journal of Medicine's publication, you should consult public documentation.",
            "Even though In 2025, according to NASA, Apollo 11 remains historic.",
            "Since Recently, during a public briefing, NASA discussed Apollo 11.",
            "Though As of 2025, NASA publishes public mission data.",
            "While Today, according to NASA, Apollo 11 remains historic.",
            "Whereas Apparently, according to NASA, Apollo 11 remains historic.",
            "According to NASA, Her is a 2013 film.",
            "According to NASA, I think Arrival is a great film.",
            "NASA's database contains public mission records.",
            "Arrival proves The Wall Street Journal was published in 1889.",
            "Relay Health launched a service.",
            "Relay FM is a podcast network.",
            "Relay AI launched a service.",
            "Moment Magazine published an article.",
            "Moment AI published an article.",
            "Moment Network launched a service.",
            "The public member study was published in 2025.",
            "The public member survey was published in 2025.",
            "The public requester study was published in 2025.",
            "The public requester survey was published in 2025.",
            "The public user study was published in 2025.",
            "The public user survey was published in 2025.",
            "From NASA, Apollo 11 remains historic.",
            "Although Today, from NASA, Apollo 11 remains historic.",
            "According to the National Aeronautics and Space Administration, Apollo 11 remains historic.",
            "Although Today, according to the National Aeronautics and Space Administration, Apollo 11 remains historic.",
            "According to Relay Health, the service launched in 1999.",
            "According to Moment AI, the service launched.",
            "According to Moment Network, the service launched.",
            "From Moment AI, the service launched.",
            "In Moment Network, the article was published.",
            "As of Moment AI's publication, the service launched.",
            "According to the Relay Health report, NASA launched Apollo 11.",
            "According to a Relay Health report, NASA launched Apollo 11.",
            "According to official Relay Health report, NASA launched Apollo 11.",
            "According to an article from Relay Health, NASA launched Apollo 11.",
            "According to a report by Moment AI, NASA launched Apollo 11.",
            "From the publication of Moment Network, NASA launched Apollo 11.",
            "Although Today, according to Moment Network, the service launched.",
            "Although Today, according to Relay Health, the service launched in 1999.",
            "In Moment Magazine, the article was published.",
            "Although Today, in Moment Magazine, the article was published.",
        ):
            with self.subTest(response=response):
                classifications, unsupported = (
                    audit_ordinary_chat_candidate_claims(
                        self.basis,
                        response,
                    )
                )
                self.assertEqual(unsupported, 0)
                self.assertNotIn(
                    "unsupported_packet_domain",
                    classifications,
                )

    def test_generic_numbers_urls_and_emails_are_external_not_personal(self):
        external_packet = replace(
            self.packet,
            request=replace(
                self.packet.request,
                subject_user_id=0,
                subject_display_name="",
                user_text="Answer a public-knowledge question.",
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
        for response in (
            "Apollo 11 landed in 1969.",
            "NASA publishes mission information at https://www.nasa.gov/.",
            "The public press contact is media@example.com.",
            "Cliff Burton joined Metallica in 1982.",
            "Sheila E. released The Glamorous Life in 1984.",
            "The public archive is at https://cliff.com/.",
            "The public contact is cliff@example.com.",
            "Barcode scanning began in 1974.",
            "Barcode is a 2001 film.",
            "The barcode has 12 digits.",
            "6-bit color supports 64 values.",
            "BNL is Brookhaven National Laboratory.",
            "Brookhaven National Laboratory publishes at https://www.bnl.gov/.",
            "The Wall Street Journal was founded in 1889.",
            "Relay is a 2010 film.",
            "Moment magnitude measures earthquake size.",
            "The source file was published in 1999.",
            "galaknoise is an invented external word.",
            "You can find public mission data at https://www.nasa.gov/.",
            "You should consult the public documentation.",
        ):
            with self.subTest(response=response):
                classifications, unsupported = (
                    audit_ordinary_chat_candidate_claims(
                        external_basis,
                        response,
                    )
                )
                self.assertEqual(unsupported, 0)
                self.assertNotIn(
                    "unsupported_packet_domain",
                    classifications,
                )

    def test_claim_specific_packet_domain_still_fails_closed(self):
        cases = (
            "Your birthday is 1999-01-01.",
            "Your site is https://invented.example/.",
            "Your email is invented@example.com.",
            "You ate pizza yesterday.",
            "You won a prize in 2025.",
            "You can speak French.",
            "You may own three homes.",
            "I was created in 1999.",
            "I'm an AI.",
            "My creator is Alice.",
            "Our founder is Alice.",
            "We started in 1999.",
            "I remember you from 2019.",
            "I have your private address.",
            "<@7> works at NASA.",
            "The member works at NASA.",
            "The requester works at NASA.",
            "He works as a network engineer.",
            "Apparently, he works as a network engineer.",
            "In 2025, she joined a band.",
            "In Seattle, he works at NASA.",
            "At NASA, she works there.",
            "Last year, he won a prize.",
            "According to NASA, he works there.",
            "* He works at NASA.",
            "It was founded in 1999.",
            "Seattle.",
            "Born in 1980.",
            "He: a network engineer.",
            "Test Member works as a network engineer.",
            "Test Member: network engineer.",
            "Test-Member works as a network engineer.",
            "Test_Member works as a network engineer.",
            "TestMember works as a network engineer.",
            "Test.Member works as a network engineer.",
            "Test‐Member works as a network engineer.",
            "Test/Member works as a network engineer.",
            "Mac Modem founded BARCODE in 1999.",
            "6 Bit founded a studio in 1999.",
            "6-Bit founded a studio in 1999.",
            "Six Bit released an album in 1999.",
            "Six-Bit released an album in 1999.",
            "GalakNoise founded a studio in 1999.",
            "Galak-Noise founded a studio in 1999.",
            "BARCODE was founded in 1999.",
            "BARCODE: a collective.",
            "BNL-01 joined the project in 1999.",
            "B.N.L.-01 was created in 1999.",
            "BNL-01: an AI.",
            "BNL joined BARCODE in 1999.",
            "DJ Floppydisc founded a studio in 1999.",
            "GALAKNOISE released an album in 1999.",
            "DJ-Floppydisc founded a studio in 1999.",
            "Dj-Floppydisc founded a studio in 1999.",
            "DJ Floppy-disc founded a studio in 1999.",
            "DJ<em>Floppydisc</em> founded a studio in 1999.",
            "MacModem founded a studio in 1999.",
            "Mac-modem founded a studio in 1999.",
            "Call'Em-Bini founded a studio in 1999.",
            "Cache_Back founded a studio in 1999.",
            "Barcode-Network was founded in 1999.",
            "BAR-CODE Network was founded in 1999.",
            "D.J. Floppydisc founded a studio in 1999.",
        )
        for response in cases:
            with self.subTest(response=response):
                classifications, unsupported = (
                    audit_ordinary_chat_candidate_claims(
                        self.basis,
                        response,
                    )
                )
                self.assertEqual(unsupported, 1)
                self.assertEqual(
                    classifications,
                    ("unsupported_packet_domain",),
                )
                decision = evaluate_single_packet_response(
                    self.conn,
                    self._begin(),
                    response=response,
                    provider_call_count=1,
                    corrective_call_count=0,
                    environ=self.flags,
                )
                self.assertFalse(decision.candidate_selected)
                self.assertEqual(
                    decision.fallback_reason,
                    "unsupported_packet_domain_claim",
                )

    def test_ambiguous_member_fragments_and_internal_claims_fail_closed(self):
        cases = (
            "At NASA.",
            "Works at NASA.",
            "Lives in Seattle.",
            "Employed by NASA.",
            "Engineer at NASA.",
            "A network engineer.",
            "Network engineer.",
            "A founder.",
            "The founder.",
            "From Seattle.",
            "Married to Alice.",
            "Aged 42.",
            "Birthday: January 1.",
            "Favorite color: blue.",
            "Home: Seattle.",
            "Role: engineer.",
            "Website: invented.example.",
            "Email: invented@example.com.",
            "Forty-two years old.",
            "Public speaker.",
            "Regularly visits Paris.",
            "Based in Seattle.",
            "An AI.",
            "Married with two kids.",
            "Jazz lover.",
            "Six feet tall.",
            "Alice is my creator.",
            "NASA owns me.",
            "Seattle is where I live.",
            "The database has 100 users.",
            "The packet contains 500 records.",
            "The shared brain stores 500 memories.",
            "Memory contains private messages.",
            "BNL memory has 100 records.",
            "Its first broadcast was in 2020.",
            "The network started in 1999.",
            "Internal database has 100 users.",
            "Private archive stores messages.",
            "Stored memory has facts.",
            "Selected packet contains records.",
            "Current profile says you work at NASA.",
            "BNL's database has rows.",
            "Private facts include a birthday.",
            "Personal traits include kindness.",
            "Known history shows a job.",
            "Musician performs on stage.",
            "Artist released an album.",
            "Moderator runs a forum.",
            "Producer works at a studio.",
            "Host lives in Seattle.",
            "Person lives in Seattle.",
            "Someone works at NASA.",
            "These packets contain 500 records.",
            "Those packets contain 500 records.",
            "Public memory contains facts.",
            "Cached memory contains facts.",
            "Conversation memory contains facts.",
            "Long-term memory contains facts.",
            "Working memory contains facts.",
            "Request context contains facts.",
            "Evidence packet contains facts.",
            "Intelligence packet contains facts.",
            "Current Situation Frame contains facts.",
            "Situation Frame contains facts.",
            "The frame contains facts.",
            "This assessment contains facts.",
            "The candidate contains facts.",
            "The receipt contains facts.",
            "The run contains facts.",
            "The store contains facts.",
            "The cache contains facts.",
            "Marital status indicates married.",
            "Employment history includes NASA.",
            "Personal preference signals generosity.",
            "Internal vector database contains private messages.",
            "Situation-frame evidence contains facts.",
            "The runtime uses the packet.",
            "Archived Journal contains private messages.",
            "Private Relay contains private messages.",
            "Current Moment contains private messages.",
            "The archived Journal contains private messages.",
            "This private Relay contains private messages.",
            "BNL's Moment contains private messages.",
        )
        for response in cases:
            with self.subTest(response=response):
                classifications, unsupported = (
                    audit_ordinary_chat_candidate_claims(
                        self.basis,
                        response,
                    )
                )
                self.assertGreaterEqual(unsupported, 1)
                self.assertIn(
                    "unsupported_packet_domain",
                    classifications,
                )

        for response in (
            "Works at NASA.",
            "Birthday: January 1.",
            "Alice is my creator.",
            "The packet contains 500 records.",
        ):
            with self.subTest(selected_response=response):
                decision = evaluate_single_packet_response(
                    self.conn,
                    self._begin(),
                    response=response,
                    provider_call_count=1,
                    corrective_call_count=0,
                    environ=self.flags,
                )
                self.assertFalse(decision.candidate_selected)
                self.assertEqual(
                    decision.fallback_reason,
                    "unsupported_packet_domain_claim",
                )

    def test_safe_prefixes_questions_and_guidance_cannot_launder_claims(self):
        cases = (
            "Can confirm you work at NASA.",
            "Will say you work at NASA.",
            "Should note BARCODE started in 1999.",
            "What you said is that you work at NASA.",
            "What your birthday is: January 1.",
            "Who you are is a network engineer.",
            "Do you work at NASA, you work there.",
            "Why did you murder someone?",
            "When did you stop stealing?",
            "Are you secretly a criminal?",
            "I don't know your birthday: Jan 1.",
            "The evidence is thin: you work at NASA.",
            "I can check that you work at NASA.",
            "I can read your private messages.",
            "I can review your private dossier.",
            "We can download your private history.",
            "You can check that BARCODE started in 1999.",
            "You should read your private dossier.",
            "I can help because I was created in 1999.",
            "I can't verify your birthday, you were born in 1999.",
            "I don't know your birthday: it is 1999.",
            "I don't know your birthday it is 1999.",
            "I can't verify your birthday you were born in 1999.",
            "I don't know your birthday maybe January 1.",
            "I can't verify your birthday probably January 1.",
            "I can't confirm your employer likely NASA.",
            "I do not have enough reliable information about your age probably 42.",
            "I don't remember your favorite movie maybe Blade Runner.",
            "I can't verify your birthday / January 1.",
            "I can't verify your birthday - January 1.",
            "I can't verify your birthday aka January 1.",
            "I don't know your birthday you murdered someone.",
            "I can't verify your birthday you stole money.",
            "I don't know your birthday you committed fraud.",
            "I don't know your birthday your name is Alice.",
            "I don't know your birthday you own three homes.",
            "I don't know your birthday you cheated.",
            "I don't know your birthday you abused someone.",
            "I can't verify your birthday you served in Iraq.",
            "I don't know your birthday your diagnosis is cancer.",
            "I can't verify your birthday BARCODE was founded in 1999.",
            "The evidence is thin you work at NASA.",
            "The context is limited your birthday is January 1.",
            "After you murdered someone, okay.",
            "During your 2020 wedding, sounds good.",
            "At your home in Seattle, okay.",
            "According to your profile showing a 1999 birthday, okay.",
            "As of your profile showing a 1999 birthday, okay.",
            "As of the database revealing private messages, okay.",
            "As of the packet showing 500 records, okay.",
            "As of internal memory revealing your birthday, sounds good.",
            "As of the archive proving you work at NASA, thanks.",
            "According to Relay AI report about the packet, okay.",
            "According to Relay AI publication citing internal memory, okay.",
            "According to Relay AI article on selected Relay, okay.",
            "According to Relay AI report from the database, okay.",
            "From Moment Network report about the packet, okay.",
            "In Relay Health article on selected Relay, okay.",
            "As of Moment Magazine report from the database, okay.",
            "According to the Relay Health report about the packet, okay.",
            "According to a Relay Health report about the packet, okay.",
            "According to official Relay Health report about the packet, okay.",
            "From official Moment AI article on selected Relay, okay.",
            "In official Moment Network report from the database, okay.",
            "According to an article from Relay Health about the packet, okay.",
            "According to a report by Moment AI on selected Relay, okay.",
            "From the publication of Moment Network about internal memory, okay.",
            "As of 2025, according to your profile, okay.",
            "Today, according to your profile, okay.",
            "Apparently, according to your profile, okay.",
            "In 2025, according to your profile, okay.",
            "As of 2025, after you murdered someone, okay.",
            "Today, after you murdered someone, okay.",
            "Apparently, at BARCODE founding in 1999, sounds good.",
            "Recently, during your NASA job, okay.",
            "As of today, as of your 1999 birthday, okay.",
            "In 2025, as of BNL-01 creation, okay.",
            "As of the Journal's publication, sounds good.",
            "As of 2025, after you joined NASA, you should consult public documentation.",
            "Today, according to your profile, you should consult public documentation.",
            ("Today, " * 16) + "according to your profile, okay.",
            "Although Today, according to your profile, okay.",
            "Because Apparently, as of The Journal's publication, you should consult public documentation.",
            "Even though In 2025, according to Test Member, okay.",
            "Since Recently, during BARCODE founding in 1999, okay.",
            "Though As of 2025, according to BNL-01, okay.",
            "While Today, according to Selected Relay, okay.",
            "Whereas Apparently, as of Private Moment, okay.",
            "Although Today, according to internal memory, okay.",
            "Because Apparently, as of the packet showing 500 records, okay.",
            "According to your profile, Her is a 2013 film.",
            "According to your profile, the film Her is a 2013 film.",
            "According to your profile, I think Arrival is a great film.",
            "Although Today, according to your profile, Her is a 2013 film.",
            "That means the packet contains 500 records.",
            "This shows the packet contains 500 records.",
            "The throughline is that the packet contains 500 records.",
            "NASA says the packet contains 500 records.",
            "Alice says internal memory stores messages.",
            "Public records show the profile shows a birthday.",
            "Arrival proves The Journal was published in 1999.",
            "The result is that this packet has 100 rows.",
            "The analysis concludes that the database contains 100 users.",
            "I think NASA says the packet contains 500 records.",
            "NASA says selected Relay accepted a post.",
            "NASA says private Moment occurred in 1999.",
            "NASA Selected Relay accepted a post.",
            "NASA Private Moment occurred in 1999.",
            "NASA says the packet still contains 500 records.",
            "I think NASA says the packet still contains 500 records.",
            "NASA says the packet now contains 500 records.",
            "NASA says the packet very clearly contains 500 records.",
            "Arrival proves The Journal still was published in 1999.",
            "NASA says the packet again contains 500 records.",
            "NASA says the packet once contained 500 records.",
            "NASA says the packet ever contained 500 records.",
            "NASA says the packet no longer contains 500 records.",
            "NASA says the packet without doubt contains 500 records.",
            "NASA says the packet as expected contains 500 records.",
            "NASA says the packet to this day contains 500 records.",
            "NASA says the packet at present contains 500 records.",
            "NASA says the packet available today contains 500 records.",
            "NASA says the packet that still contains 500 records was selected.",
            "NASA says the packet of public records contains 500 entries.",
            "NASA says selected Relay no longer accepts posts.",
            "NASA says private Moment once occurred in 1999.",
            "NASA says The Journal at present contains private entries.",
            "NASA says internal memory no longer stores messages.",
            "I think NASA says the packet no longer contains 500 records.",
            "NASA said member works at NASA.",
            "NASA Said member works at NASA.",
            "NASA told member works at NASA.",
            "NASA found member works at NASA.",
            "NASA heard member works at NASA.",
            "NASA knew member works at NASA.",
            "NASA said requester works at NASA.",
            "NASA said user works at NASA.",
            "NASA said member is married.",
            "NASA Says Relay accepted a post.",
            "NASA SAYS Relay accepted a post.",
            "NASA Reports Moment occurred in 1999.",
            "NASA REPORTS Moment occurred in 1999.",
            "Studies show user works at NASA.",
            "NASA data show user works at NASA.",
            "Public reports reveal member is active.",
            "Surveys suggest requester lives in Seattle.",
            "The results prove user is active.",
            "Public data indicate member works at NASA.",
            "Records confirm requester lives in Seattle.",
            "Reports say user works at NASA.",
            "Relay Accepted a post.",
            "Relay ACCEPTED a post.",
            "Moment Occurred in 1999.",
            "Moment OCCURRED in 1999.",
            "NASA says Relay Accepted a post.",
            "NASA says Relay ACCEPTED a post.",
            "NASA says Moment Occurred in 1999.",
            "NASA says Moment OCCURRED in 1999.",
            "NASA says Relay Was accepted.",
            "NASA says Moment Was recorded.",
            "NASA says Relay Is active.",
            "NASA says selected Relay Health launched a service.",
            "NASA says private Moment Magazine published an article.",
            "NASA says member account contains a birthday.",
            "NASA says user account has a private email.",
            "NASA says requester account stores messages.",
            "NASA says user forum stores private messages.",
            "NASA says requester documentation reveals a birthday.",
            "NASA says user organization has private records.",
            "Relay Journal published a profile.",
            "Relay Moment occurred in 1999.",
            "Moment Relay accepted a post.",
            "Relay Packet contains private messages.",
            "Relay Profile contains a birthday.",
            "Relay Database stores messages.",
            "Relay Archive contains private facts.",
            "Relay Memory contains private facts.",
            "Moment Packet contains records.",
            "Moment Profile shows a birthday.",
            "Moment Database stores messages.",
            "NASA says Relay Packet contains private messages.",
            "NASA says Moment Profile shows a birthday.",
            "NASA says relay Health accepted a post.",
            "NASA says RELAY Health accepted a post.",
            "NASA says ReLaY Health accepted a post.",
            "NASA says RELAY FM contains private messages.",
            "The latest member survey shows user works at NASA.",
            "The latest member survey shows requester lives in Seattle.",
            "The latest member survey shows member is married.",
            "The latest member survey proves user committed fraud.",
            "The latest member survey says user stole money.",
            "The latest member survey confirms user has cancer.",
            "The latest member survey reveals user is pregnant.",
            "Recent user study shows user works at NASA.",
            "From your profile, okay.",
            "Although Today, from your profile, okay.",
            "According to the selected member profile containing a private 1999 birthday and current employer, okay.",
            "Although Today, according to the selected member profile containing a private 1999 birthday and current employer, okay.",
            "After you joined NASA, you should consult public documentation.",
            "At BARCODE founding in 1999, sounds good.",
            "[SIGNAL_GLITCH // you murdered someone // adjacent_timeline]",
            "[SIGNAL_GLITCH // you are pregnant // adjacent_timeline]",
            "In an alternate timeline, you murdered someone.",
            "[SIGNAL_GLITCH // you own three homes // adjacent_timeline]",
            "[SIGNAL_GLITCH // I was created in 1999 // adjacent_timeline]",
            "[SIGNAL_GLITCH // BARCODE started in 1999 // adjacent_timeline]",
            "[SIGNAL_GLITCH // internal database has facts // adjacent_timeline]",
            "[SIGNAL_GLITCH // the packet selects the source // adjacent_timeline]",
            "[SIGNAL_GLITCH // the database reads member messages // adjacent_timeline]",
            "[SIGNAL_GLITCH // the database hosts a show // adjacent_timeline]",
            "[SIGNAL_GLITCH // the archive reveals member facts // adjacent_timeline]",
            "[SIGNAL_GLITCH // the archive hosts a show // adjacent_timeline]",
            "In an alternate timeline, the system accesses member data.",
            "[SIGNAL_GLITCH // The Moment occurred in 1999 // adjacent_timeline]",
            "[SIGNAL_GLITCH // D.J. Floppydisc founded a studio // adjacent_timeline]",
            "[SIGNAL_GLITCH // you run a studio // adjacent_timeline]",
            "[SIGNAL_GLITCH // you host a show // adjacent_timeline]",
            "[SIGNAL_GLITCH // you manage a venue // adjacent_timeline]",
            "[SIGNAL_GLITCH // you built a house // adjacent_timeline]",
            "[SIGNAL_GLITCH // you earn $100,000 // adjacent_timeline]",
            "John D. The packet contains 500 records.",
            "Jane Q. Situation-frame evidence contains facts.",
            "George R. The private archive stores messages.",
            "John D. Archived Relay contains messages.",
            "Sheila E. Journal published a profile in 1999.",
            "Sheila E. Relay accepted a post in 1999.",
            "Sheila E. Moment occurred in 1999.",
            "Your Name is a 2016 film, you were born in 1999.",
            "Do you work at NASA and steal from people?",
            "I believe you murdered someone.",
            "I think you stole money.",
            "It seems you lied.",
            "That means you murdered someone.",
            "This shows you stole money.",
            "Overall, you lied.",
            "The throughline is that you committed fraud.",
            "Together, this proves you cheated.",
            "Your favorite movie is Arrival. I believe you murdered someone.",
            "Your favorite movie is Arrival. I think you stole money.",
            "Your favorite movie is Arrival. It seems you lied.",
            "Your favorite movie is Arrival. That means you murdered someone.",
            "Your favorite movie is Arrival. This shows you stole money.",
            "Your favorite movie is Arrival. Overall, you lied.",
            "Your favorite movie is Arrival. The throughline is that you committed fraud.",
            "Your favorite movie is Arrival. Together, this proves you cheated.",
            "Your favorite movie is Arrival. I think you're married.",
            "Your favorite movie is Arrival. I think you are pregnant.",
            "Your favorite movie is Arrival. I think you are autistic.",
            "Your favorite movie is Arrival. I think you are gay.",
            "Your favorite movie is Arrival. I think you are depressed.",
            "Your favorite movie is Arrival. My assessment is that you are diabetic.",
            "Your favorite movie is Arrival. It seems you are wealthy.",
            "Your favorite movie is Arrival. Overall, you are pregnant.",
            "Your favorite movie is Arrival. The throughline is that you're married.",
        )
        for response in cases:
            with self.subTest(response=response):
                classifications, unsupported = (
                    audit_ordinary_chat_candidate_claims(
                        self.basis,
                        response,
                    )
                )
                self.assertGreaterEqual(unsupported, 1)
                self.assertIn(
                    "unsupported_packet_domain",
                    classifications,
                )

    def test_supported_claim_with_packet_comma_tail_fails_closed(self):
        for response in (
            "Your favorite movie is Arrival, you were born in 1999.",
            "Your favorite movie is Arrival and the packet has 500 records.",
            "Your favorite movie is Arrival because I was created in 1999.",
        ):
            with self.subTest(response=response):
                classifications, unsupported = (
                    audit_ordinary_chat_candidate_claims(
                        self.basis,
                        response,
                    )
                )
                self.assertEqual(unsupported, 1)
                self.assertIn(
                    "unsupported_packet_domain",
                    classifications,
                )
                decision = evaluate_single_packet_response(
                    self.conn,
                    self._begin(),
                    response=response,
                    provider_call_count=1,
                    corrective_call_count=0,
                    environ=self.flags,
                )
                self.assertFalse(decision.candidate_selected)

    def test_supported_member_claim_cannot_carry_unsupported_tail(self):
        response = (
            "Your favorite movie is Arrival because you watched it in Paris."
        )
        classifications, unsupported = audit_ordinary_chat_candidate_claims(
            self.basis,
            response,
        )
        self.assertEqual(unsupported, 1)
        self.assertEqual(
            classifications,
            ("member_supported", "unsupported_packet_domain"),
        )
        decision = evaluate_single_packet_response(
            self.conn,
            self._begin(),
            response=response,
            provider_call_count=1,
            corrective_call_count=0,
            environ=self.flags,
        )
        self.assertFalse(decision.candidate_selected)

    def test_nonhuman_external_claim_does_not_release_member_pronoun(self):
        cases = (
            "Apple was founded in 1976. He works at Apple.",
            "NASA publishes mission data. He works there.",
            "The Wall Street Journal was founded in 1889. He works there.",
            "Pink Floyd released an album. He works with them.",
            "Denis Villeneuve directed Arrival. She works at NASA.",
            "Walt Disney was born in 1901. She works at NASA.",
        )
        for response in cases:
            with self.subTest(response=response):
                classifications, unsupported = (
                    audit_ordinary_chat_candidate_claims(
                        self.basis,
                        response,
                    )
                )
                self.assertEqual(unsupported, 1)
                self.assertEqual(
                    classifications,
                    (
                        "external_public_knowledge",
                        "unsupported_packet_domain",
                    ),
                )

    def test_clarification_insufficiency_and_guidance_are_not_claims(self):
        cases = (
            "Do you work at NASA?",
            "I don't know your birthday.",
            "I do not have enough evidence to say where you work.",
            "I can't verify that BARCODE started in 1999.",
            "I don't know whether you own three homes.",
            "I can't verify that your diagnosis is cancer.",
            "I cannot say whether you served in Iraq.",
            "I do not have enough evidence to say whether you attended Harvard.",
            "You can find public mission data at https://www.nasa.gov/.",
            "You should consult the public documentation.",
        )
        for response in cases:
            with self.subTest(response=response):
                classifications, unsupported = (
                    audit_ordinary_chat_candidate_claims(
                        self.basis,
                        response,
                    )
                )
                self.assertEqual(unsupported, 0)
                self.assertNotIn(
                    "unsupported_packet_domain",
                    classifications,
                )

        classifications, unsupported = audit_ordinary_chat_candidate_claims(
            self.basis,
            (
                "I do not have enough reliable public history to summarize "
                "you without guessing. I can respond to what you say here, "
                "but the longer-term signal is still too thin for a grounded "
                "profile."
            ),
        )
        self.assertEqual(unsupported, 0)
        self.assertEqual(
            classifications,
            (
                "honest_nonassertion",
                "ordinary_guidance",
                "honest_nonassertion",
            ),
        )

    def test_honest_nonassertion_does_not_license_adversative_fact(self):
        response = (
            "I can't verify your birthday, but you were born in 1999."
        )
        classifications, unsupported = audit_ordinary_chat_candidate_claims(
            self.basis,
            response,
        )
        self.assertEqual(unsupported, 1)
        self.assertEqual(
            classifications,
            ("honest_nonassertion", "unsupported_packet_domain"),
        )

    def test_external_personal_titles_do_not_become_member_claims(self):
        cases = (
            "The song You Are My Sunshine was published in 1939.",
            "Your Name is a 2016 film.",
            "I, Robot is a 2004 film.",
            "We Need to Talk About Kevin is a 2011 film.",
            "Me Before You is a 2016 film.",
            "Her is a 2013 film.",
            "His Girl Friday is a 1940 film.",
        )
        for response in cases:
            with self.subTest(response=response):
                classifications, unsupported = (
                    audit_ordinary_chat_candidate_claims(
                        self.basis,
                        response,
                    )
                )
                self.assertEqual(unsupported, 0)
                self.assertEqual(
                    classifications,
                    ("external_public_knowledge",),
                )

        for response in (
            "Your Name is HAL.",
            "You Are My Sunshine was released in 1939.",
            "You, Me and Dupree was released in 2006.",
            "You Murdered Someone was released in 2019.",
            "You Are Married was released in 2019.",
            "My Creator Is Alice was released in 2019.",
            "Your Name was released in 2016.",
            "Your Work was published in 2019.",
            "Your Project was released in 2019.",
            "My Project was published in 2019.",
            "Our Album was released in 2019.",
            "You published in 2019.",
            "You released in 2019.",
            "Her Profile was published in 2019.",
            "His Dossier was published in 2019.",
        ):
            with self.subTest(ambiguous_title=response):
                classifications, unsupported = (
                    audit_ordinary_chat_candidate_claims(
                        self.basis,
                        response,
                    )
                )
                self.assertEqual(unsupported, 1)
                self.assertEqual(
                    classifications,
                    ("unsupported_packet_domain",),
                )

    def test_ordinary_expressions_and_external_opinions_stay_usable(self):
        for response in (
            "I agree.",
            "I hear you.",
            "I think Arrival is a great film.",
            "My take is that Arrival is excellent.",
            "We can do that.",
            "I can explain.",
            "You got it.",
            "You are welcome.",
        ):
            with self.subTest(response=response):
                classifications, unsupported = (
                    audit_ordinary_chat_candidate_claims(
                        self.basis,
                        response,
                    )
                )
                self.assertEqual(unsupported, 0)
                self.assertNotIn(
                    "unsupported_packet_domain",
                    classifications,
                )
                decision = evaluate_single_packet_response(
                    self.conn,
                    self._begin(),
                    response=response,
                    provider_call_count=1,
                    corrective_call_count=0,
                    environ=self.flags,
                )
                self.assertTrue(decision.candidate_selected)

    def test_selected_ambiguous_canon_labels_still_fail_closed(self):
        cases = (
            ("cliff", "Cliff", "Cliff joined a band in 1982."),
            ("sheila", "Sheila", "Sheila released an album in 1984."),
            ("bnl_01", "BNL", "BNL joined the project in 1999."),
            ("6_bit", "6 Bit", "6 Bit founded a studio in 1999."),
        )
        for entity_key, label, response in cases:
            with self.subTest(entity_key=entity_key, response=response):
                classifications, unsupported = (
                    audit_ordinary_chat_candidate_claims(
                        self._basis_for_canon_entity(entity_key, label),
                        response,
                    )
                )
                self.assertEqual(unsupported, 1)
                self.assertEqual(
                    classifications,
                    ("unsupported_packet_domain",),
                )

        for entity_key, label, response in (
            ("cliff", "Cliff", "Cl.iff joined a band in 1982."),
            ("sheila", "Sheila", "Shei_la released an album."),
            ("bnl_01", "BNL", "B.N.L joined a project."),
        ):
            with self.subTest(punctuated_entity=entity_key):
                classifications, unsupported = (
                    audit_ordinary_chat_candidate_claims(
                        self._basis_for_canon_entity(entity_key, label),
                        response,
                    )
                )
                self.assertEqual(unsupported, 1)
                self.assertEqual(
                    classifications,
                    ("unsupported_packet_domain",),
                )

    def test_selected_label_reconstruction_preserves_word_boundaries(self):
        for response in (
            "Contest membership works differently.",
            "The latest member survey was published in 2025.",
            "Contest Members published results.",
        ):
            with self.subTest(response=response):
                classifications, unsupported = (
                    audit_ordinary_chat_candidate_claims(
                        self.basis,
                        response,
                    )
                )
                self.assertEqual(unsupported, 0)
                self.assertNotIn(
                    "unsupported_packet_domain",
                    classifications,
                )

    def test_selected_label_outranks_external_title_context(self):
        collision_basis = self._basis_for_canon_entity(
            "relay_health_collision",
            "Relay Health",
        )
        for response in (
            "According to Relay Health, okay.",
            "According to Relay Health, you should consult public documentation.",
            "According to Relay Health, I think Arrival is a great film.",
        ):
            with self.subTest(response=response):
                classifications, unsupported = (
                    audit_ordinary_chat_candidate_claims(
                        collision_basis,
                        response,
                    )
                )
                self.assertEqual(unsupported, 1)
                self.assertEqual(
                    classifications,
                    ("unsupported_packet_domain",),
                )

    def test_project_titles_are_governed_across_frames(self):
        for response in (
            "The Journal was published in 1999.",
            "Journal published a profile in 1999.",
            "The Journal's publication began in 1999.",
            "The Relay was published in 1999.",
            "Relay accepted a post in 1999.",
            "The Relay's post was accepted in 1999.",
            "The Moment was recorded in 1999.",
            "Moment's episode was recorded in 1999.",
        ):
            with self.subTest(member_frame_project_title=response):
                classifications, unsupported = (
                    audit_ordinary_chat_candidate_claims(
                        self.basis,
                        response,
                    )
                )
                self.assertEqual(unsupported, 1)
                self.assertEqual(
                    classifications,
                    ("unsupported_packet_domain",),
                )

        journal_basis = replace(
            self.basis,
            packet=replace(
                self.packet,
                request=replace(
                    self.packet.request,
                    frame_object_kind="journal",
                ),
            ),
        )
        classifications, unsupported = audit_ordinary_chat_candidate_claims(
            journal_basis,
            "The Journal was published in 1999.",
        )
        self.assertEqual(unsupported, 1)
        self.assertEqual(
            classifications,
            ("unsupported_packet_domain",),
        )
        classifications, unsupported = audit_ordinary_chat_candidate_claims(
            journal_basis,
            "Journal was founded in 1999.",
        )
        self.assertEqual(unsupported, 1)
        self.assertEqual(
            classifications,
            ("unsupported_packet_domain",),
        )
        for response in (
            "The Wall Street Journal was founded in 1889.",
            "Journal of Medicine was founded in 1999.",
        ):
            with self.subTest(response=response):
                classifications, unsupported = (
                    audit_ordinary_chat_candidate_claims(
                        journal_basis,
                        response,
                    )
                )
                self.assertEqual(unsupported, 0)
                self.assertNotIn(
                    "unsupported_packet_domain",
                    classifications,
                )

        classifications, unsupported = audit_ordinary_chat_candidate_claims(
            journal_basis,
            "the journal was published in 1999.",
        )
        self.assertEqual(unsupported, 1)
        self.assertEqual(
            classifications,
            ("unsupported_packet_domain",),
        )

    def test_structured_packet_headings_fail_closed(self):
        for response in (
            "BARCODE:\n- A collective\n- Founded in 1999",
            "BNL-01:\n- An AI\n- Created in 1999",
            "About you:\n- Network engineer\n- Born in 1980",
        ):
            with self.subTest(response=response):
                classifications, unsupported = (
                    audit_ordinary_chat_candidate_claims(
                        self.basis,
                        response,
                    )
                )
                self.assertGreaterEqual(unsupported, 1)
                self.assertIn(
                    "unsupported_packet_domain",
                    classifications,
                )

    def test_claim_audit_alignment_mismatch_always_fails_closed(self):
        response = "Apollo 11 landed in 1969. Seattle is in Washington."
        coverage = replace(
            candidate_profile_coverage(self.basis, response),
            claim_classifications=(),
        )
        classifications, unsupported = audit_ordinary_chat_candidate_claims(
            self.basis,
            response,
            coverage=coverage,
        )
        self.assertEqual(unsupported, 2)
        self.assertEqual(
            classifications,
            (
                "claim_audit_alignment_invalid",
                "claim_audit_alignment_invalid",
            ),
        )

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

    def test_provider_error_receipt_requires_physical_provider_attempt(self):
        local_run = self._begin()
        local = evaluate_single_packet_response(
            self.conn,
            local_run,
            response="",
            provider_call_count=0,
            corrective_call_count=0,
            error_category="local_model_budget_exhausted",
            provider_error_code="local_model_budget_exhausted",
            environ=self.flags,
        )
        self.assertFalse(local.candidate_selected)
        self.assertEqual(
            local.fallback_reason,
            "provider_call_count_invalid",
        )
        local_receipt = self.conn.execute(
            """
            SELECT provider_call_count,candidate_provider_error_count,
                   candidate_error_category,candidate_provider_error_code
            FROM memory_governance_shared_brain_synthesis_runs
            WHERE run_id=?
            """,
            (local_run.run_id,),
        ).fetchone()
        self.assertEqual(
            local_receipt,
            (0, 0, "local_model_budget_exhausted", ""),
        )

        provider_run = self._begin()
        provider = evaluate_single_packet_response(
            self.conn,
            provider_run,
            response="",
            provider_call_count=1,
            corrective_call_count=0,
            error_category="provider_timeout",
            provider_error_code="504",
            environ=self.flags,
        )
        self.assertFalse(provider.candidate_selected)
        self.assertEqual(provider.fallback_reason, "generation_failed")
        provider_receipt = self.conn.execute(
            """
            SELECT provider_call_count,candidate_provider_error_count,
                   candidate_error_category,candidate_provider_error_code
            FROM memory_governance_shared_brain_synthesis_runs
            WHERE run_id=?
            """,
            (provider_run.run_id,),
        ).fetchone()
        self.assertEqual(
            provider_receipt,
            (1, 1, "provider_timeout", "504"),
        )

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

    def test_draft_review_preserves_call_accounting(self):
        run = self._begin()
        decision = evaluate_single_packet_response(
            self.conn,
            run,
            response="A candidate that passed deterministic evaluation.",
            provider_call_count=1,
            corrective_call_count=0,
            environ=self.flags,
        )
        reviewed = record_single_packet_review(
            self.conn,
            decision,
            reason="single_packet_response_rewritten_after_guard",
            provider_call_count=1,
            corrective_call_count=0,
            frame_revalidation_status="stale",
        )
        self.assertFalse(reviewed.candidate_selected)
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
        self.assertEqual(
            row[4],
            "single_packet_response_rewritten_after_guard",
        )


if __name__ == "__main__":
    unittest.main()
