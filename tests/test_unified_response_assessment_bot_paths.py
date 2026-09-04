import os
import sqlite3
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl01_bot


class UnifiedResponseAssessmentBotPathTests(unittest.TestCase):
    def conversation_basis(self, participant_count=10):
        return bnl01_bot.ConversationPromptSourceBasis(
            expected_digest="digest",
            rendered_context="bounded room context",
            guild_id=1,
            current_user_id=101,
            channel_id=303,
            channel_name="bnl-testing",
            channel_policy="sealed_test",
            source_row_ids=tuple(range(1, participant_count + 1)),
            participant_user_ids=tuple(
                range(101, 101 + participant_count)
            ),
            speaker_labels=tuple(
                "Member %s" % index
                for index in range(1, participant_count + 1)
            ),
        )

    def test_bot_adapter_keeps_any_participant_count_and_current_precedence(self):
        basis = self.conversation_basis(10)
        with mock.patch.object(
            bnl01_bot,
            "unified_response_assessment_shadow_enabled",
            return_value=True,
        ):
            assessment = (
                bnl01_bot.build_unified_response_assessment_shadow(
                    guild_id=1,
                    route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                    channel_policy="sealed_test",
                    conversation_surface="test",
                    current_text="What did everyone just decide?",
                    current_speaker_user_ids=tuple(range(101, 111)),
                    current_speaker_labels=basis.speaker_labels,
                    prompt_source_bases=(basis,),
                    memory_source_metadata={
                        "governed_entry_ids": ("private-ledger-ref",),
                        "governed_candidate_count": 1,
                        "moment_candidate_count": 3,
                        "legacy_memory_present": True,
                        "legacy_relationship_present": True,
                    },
                    prompt_lanes=(
                        "current_exchange",
                        "conversation_context",
                        "legacy_memory",
                        "relationship",
                    ),
                    continuity_required=True,
                )
            )

        self.assertIsNotNone(assessment)
        self.assertEqual(len(assessment.participant_user_ids), 10)
        self.assertEqual(len(assessment.speaker_labels), 10)
        self.assertEqual(
            assessment.selected_lanes,
            ("current_exchange", "conversation_context"),
        )
        self.assertEqual(
            assessment.response_act,
            "recap_current_exchange",
        )
        self.assertIn(
            ("governed_memory", "current_exchange_precedence"),
            assessment.excluded_lanes,
        )
        self.assertIn(
            ("relationship", "legacy_only_live_authority_off"),
            assessment.excluded_lanes,
        )

    def test_bot_adapter_carries_active_episode_source_moments(self):
        historical_marker = bnl01_bot.build_conversation_evidence_item(
            text="This is a separate task: Project Copper Kite.",
            source_id=41,
            speaker_user_id=101,
            speaker_label="Member 1",
            current_turn=False,
        )
        basis = self.conversation_basis(1)
        basis = bnl01_bot.replace(
            basis,
            evidence_items=(historical_marker,),
        )
        active_episode_reader = mock.Mock(
            return_value=SimpleNamespace(
                episode_id="episode-one",
                source_moment_ids=("moment-one",),
            )
        )
        current_text = "Is this a separate task, or should we continue?"
        situation_frame = bnl01_bot.build_situation_frame_v1(
            route_allowed=True,
            route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
            conversation_surface="sealed_test",
            channel_policy="sealed_test",
            current_text=current_text,
            current_speaker_user_ids=(101,),
            subject_user_ids=(101,),
            moment_id="moment-one",
            moment_situation_state="recent_active",
            moment_topic_coherent=True,
            moment_participant_overlap=True,
            response_act="answer",
        )
        self.assertEqual(situation_frame.event_relation, "resume")
        with mock.patch.object(
            bnl01_bot,
            "unified_response_assessment_shadow_enabled",
            return_value=True,
        ), mock.patch.object(
            bnl01_bot,
            "_active_episode_reference_for_unified_assessment",
            new=active_episode_reader,
        ), mock.patch.object(
            bnl01_bot,
            "_build_unified_intelligence_packet_shadow",
            return_value=None,
        ):
            assessment = bnl01_bot.build_unified_response_assessment_shadow(
                guild_id=1,
                route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                channel_policy="sealed_test",
                conversation_surface="test",
                current_text=current_text,
                current_speaker_user_ids=(101,),
                current_speaker_labels=("Member 1",),
                channel_id=303,
                prompt_source_bases=(basis,),
                prompt_lanes=("current_exchange", "active_episode"),
                continuity_required=True,
                situation_frame=situation_frame,
            )

        self.assertIsNotNone(assessment)
        self.assertEqual(assessment.active_episode_id, "episode-one")
        self.assertEqual(
            assessment.active_episode_source_moment_ids,
            ("moment-one",),
        )
        reader_kwargs = active_episode_reader.call_args.kwargs
        self.assertIn("separate task", reader_kwargs["topic_text"])
        self.assertEqual(
            reader_kwargs["current_turn_text"],
            current_text,
        )

    def test_canary_refresh_reconciles_episode_source_moments(self):
        stale_assessment = bnl01_bot.build_unified_response_assessment(
            guild_id=1,
            route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
            channel_policy="sealed_test",
            conversation_surface="sealed_test",
            current_speaker_user_ids=(101,),
            participant_user_ids=(101,),
            active_episode_id="episode-one",
            active_episode_source_moment_ids=("moment-one",),
            prompt_lanes=("current_exchange", "active_episode"),
            continuity_required=True,
            current_text="What changed, and what remains open?",
        )
        fresh_reference = bnl01_bot.ActiveEpisodeReference(
            episode_id="episode-one",
            lifecycle_status="active",
            source_moment_ids=("moment-one", "moment-two"),
            participant_count=1,
            open_loop_count=1,
            semantic_types=("action", "open_loop"),
        )
        episode_context = (
            "[Active same-channel episode signal]\n"
            "- Shared human participants: 1."
        )

        expected_episode_ids = []

        def render_validated_episode(*_args, reference_out=None, **kwargs):
            expected_episode_ids.append(kwargs["expected_episode_id"])
            reference_out["reference"] = fresh_reference
            return episode_context

        with tempfile.NamedTemporaryFile() as database_file, mock.patch.object(
            bnl01_bot,
            "DB_FILE",
            database_file.name,
        ), mock.patch.object(
            bnl01_bot,
            "unified_moment_canary_enabled",
            return_value=True,
        ), mock.patch.object(
            bnl01_bot,
            "render_active_episode_canary_context",
            side_effect=render_validated_episode,
        ):
            rendered, present, reconciled = (
                bnl01_bot._render_unified_moment_canary_context(
                    stale_assessment,
                    guild_id=1,
                    channel_id=303,
                    channel_policy="sealed_test",
                    route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                    topic_text="What changed, and what remains open?",
                    participant_user_ids=(101,),
                )
            )
            basis = bnl01_bot.UnifiedMomentCanaryPromptSourceBasis(
                expected_digest=bnl01_bot._prompt_source_digest(rendered),
                rendered_context=rendered,
                assessment=stale_assessment,
                guild_id=1,
                channel_id=303,
                channel_policy="sealed_test",
                route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                topic_text="What changed, and what remains open?",
                participant_user_ids=(101,),
                episode_context_present=True,
            )
            fresh_basis, changed = bnl01_bot.refresh_prompt_source_basis(
                basis
            )

            self.assertTrue(present)
            self.assertTrue(changed)
            self.assertEqual(
                reconciled.active_episode_source_moment_ids,
                ("moment-one", "moment-two"),
            )
            self.assertEqual(
                fresh_basis.assessment.active_episode_source_moment_ids,
                ("moment-one", "moment-two"),
            )
            self.assertEqual(
                expected_episode_ids,
                ["episode-one", "episode-one"],
            )

        with tempfile.NamedTemporaryFile() as receipt_file, mock.patch.object(
            bnl01_bot,
            "DB_FILE",
            receipt_file.name,
        ):
            run_id = bnl01_bot.record_unified_response_assessment_shadow(
                stale_assessment,
                response="The phase changed; one action remains open.",
                guard_diagnostics={
                    "_revalidated_prompt_source_bases": (fresh_basis,),
                },
            )
            receipt_db = sqlite3.connect(receipt_file.name)
            try:
                receipt = receipt_db.execute(
                    """
                    SELECT active_episode_present,prior_moment_count
                    FROM unified_response_assessment_shadow_runs
                    WHERE run_id=?
                    """,
                    (run_id,),
                ).fetchone()
            finally:
                receipt_db.close()
        self.assertEqual(receipt, (1, 2))

    def test_canary_refresh_clears_rejected_episode_sources(self):
        stale_assessment = bnl01_bot.build_unified_response_assessment(
            guild_id=1,
            route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
            channel_policy="sealed_test",
            conversation_surface="sealed_test",
            current_speaker_user_ids=(101,),
            active_episode_id="episode-one",
            active_episode_source_moment_ids=("moment-one",),
            prompt_lanes=("current_exchange", "active_episode"),
            continuity_required=True,
            current_text="What changed, and what remains open?",
        )
        with tempfile.NamedTemporaryFile() as database_file, mock.patch.object(
            bnl01_bot,
            "DB_FILE",
            database_file.name,
        ), mock.patch.object(
            bnl01_bot,
            "unified_moment_canary_enabled",
            return_value=True,
        ), mock.patch.object(
            bnl01_bot,
            "render_active_episode_canary_context",
            return_value="",
        ):
            rendered, present, reconciled = (
                bnl01_bot._render_unified_moment_canary_context(
                    stale_assessment,
                    guild_id=1,
                    channel_id=303,
                    channel_policy="sealed_test",
                    route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                    topic_text="What changed, and what remains open?",
                    participant_user_ids=(101,),
                )
            )

        self.assertFalse(present)
        self.assertEqual(reconciled.active_episode_id, "")
        self.assertEqual(reconciled.active_episode_source_moment_ids, ())
        self.assertNotIn("active_episode", reconciled.prompt_lanes)
        rejected_basis = bnl01_bot.UnifiedMomentCanaryPromptSourceBasis(
            expected_digest=bnl01_bot._prompt_source_digest(rendered),
            rendered_context=rendered,
            assessment=reconciled,
            guild_id=1,
            channel_id=303,
            channel_policy="sealed_test",
            route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
            topic_text="What changed, and what remains open?",
            participant_user_ids=(101,),
            episode_context_present=False,
        )

        with tempfile.NamedTemporaryFile() as receipt_file, mock.patch.object(
            bnl01_bot,
            "DB_FILE",
            receipt_file.name,
        ):
            rejected_run_id = (
                bnl01_bot.record_unified_response_assessment_shadow(
                    stale_assessment,
                    response="The phase changed; one action remains open.",
                    guard_diagnostics={
                        "_revalidated_prompt_source_bases": (
                            rejected_basis,
                        ),
                    },
                )
            )
            source_neutral_run_id = (
                bnl01_bot.record_unified_response_assessment_shadow(
                    stale_assessment,
                    response="Current-turn answer without episode support.",
                    guard_diagnostics={
                        "_revalidated_prompt_source_bases": (),
                        "source_neutral_recovery": True,
                    },
                )
            )
            receipt_db = sqlite3.connect(receipt_file.name)
            try:
                receipts = receipt_db.execute(
                """
                SELECT active_episode_present,prior_moment_count
                FROM unified_response_assessment_shadow_runs
                WHERE run_id IN (?,?) ORDER BY run_id
                """,
                    (rejected_run_id, source_neutral_run_id),
                ).fetchall()
            finally:
                receipt_db.close()
        self.assertEqual(receipts, [(0, 0), (0, 0)])

    def test_direct_prompt_is_byte_identical_with_shadow_on_or_off(self):
        visual_basis = SimpleNamespace(status="not_requested")
        conversation_basis = self.conversation_basis(3)
        packet_only_sentinel = "PACKET-ONLY-PRIVATE-SENTINEL"
        packet = SimpleNamespace(
            private_packet_text=packet_only_sentinel,
            diagnostics=SimpleNamespace(
                processing_errors=(),
                invalid_invariants=(),
                revalidation_status="passed",
                candidates_by_lane={"atomic_knowledge": 1},
                excluded_by_reason={},
                conflict_reasons=(),
            ),
            moment_refs=(),
            governed_refs=("atomic:opaque-packet-ref",),
            relationship_refs=(),
            canon_refs=(),
            assessment_lanes=("governed_memory",),
            assessment_exclusions=(),
            assessment_missing_lanes=(),
        )

        def memory_context(*_args, **kwargs):
            metadata = kwargs.get("source_metadata")
            if metadata is not None:
                metadata.update(
                    {
                        "moment_gist_rendered": False,
                        "approved_fact_count": 1,
                        "legacy_relationship_present": True,
                        "legacy_memory_present": True,
                        "relationship_v2_candidate_present": False,
                        "governed_entry_ids": ("governed-ref",),
                        "governed_candidate_count": 1,
                        "governance_exclusion_count": 2,
                        "governance_contradiction_count": 0,
                        "moment_candidate_count": 0,
                        "prompt_budget": 900,
                    }
                )
            return (
                "Approved direct self-reports:\n"
                "- Favorite color: blue\n"
                "Relationship state: stage=known."
            )

        common_patches = (
            mock.patch.object(
                bnl01_bot,
                "get_user_profile",
                return_value=("Member 1", ""),
            ),
            mock.patch.object(
                bnl01_bot,
                "should_allow_greeting",
                return_value=False,
            ),
            mock.patch.object(
                bnl01_bot,
                "choose_response_style",
                return_value=("balanced", "Respond naturally."),
            ),
            mock.patch.object(
                bnl01_bot,
                "build_user_memory_context",
                side_effect=memory_context,
            ),
            mock.patch.object(
                bnl01_bot,
                "memory_governance_live_enabled",
                return_value=False,
            ),
            mock.patch.object(
                bnl01_bot,
                "build_broadcast_memory_context",
                return_value="",
            ),
            mock.patch.object(
                bnl01_bot,
                "build_conversation_prompt_source_basis",
                return_value=conversation_basis,
            ),
            mock.patch.object(
                bnl01_bot,
                "build_community_visual_basis",
                return_value=visual_basis,
            ),
            mock.patch.object(
                bnl01_bot,
                "render_community_visual_basis_for_prompt",
                return_value="",
            ),
            mock.patch.object(
                bnl01_bot,
                "get_guild_config",
                return_value=303,
            ),
        )
        for patcher in common_patches:
            patcher.start()
            self.addCleanup(patcher.stop)

        metadata_off = {}
        with mock.patch.object(
            bnl01_bot,
            "unified_response_assessment_shadow_enabled",
            return_value=False,
        ):
            prompt_off, *_ = bnl01_bot.build_user_aware_prompt(
                101,
                1,
                "Member 1",
                "What did everyone just decide?",
                room_context="bounded room context",
                channel_name="bnl-testing",
                channel_id=303,
                channel_policy="sealed_test",
                route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                is_direct_interaction=True,
                prompt_metadata=metadata_off,
            )

        metadata_on = {}
        synthesis_basis = object()
        with mock.patch.object(
            bnl01_bot,
            "unified_response_assessment_shadow_enabled",
            return_value=True,
        ), mock.patch.object(
            bnl01_bot,
            "_active_episode_reference_for_unified_assessment",
            return_value=SimpleNamespace(
                episode_id="mep_opaque_shadow_reference",
                source_moment_ids=(),
            ),
        ), mock.patch.object(
            bnl01_bot,
            "_build_unified_intelligence_packet_shadow",
            return_value=packet,
        ), mock.patch.object(
            bnl01_bot,
            "build_shared_brain_synthesis_basis",
            return_value=synthesis_basis,
        ):
            prompt_on, *_ = bnl01_bot.build_user_aware_prompt(
                101,
                1,
                "Member 1",
                "What did everyone just decide?",
                room_context="bounded room context",
                channel_name="bnl-testing",
                channel_id=303,
                channel_policy="sealed_test",
                route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                is_direct_interaction=True,
                prompt_metadata=metadata_on,
            )

        self.assertEqual(prompt_on, prompt_off)
        self.assertNotIn(packet_only_sentinel, prompt_on)
        self.assertIs(
            metadata_on["shared_brain_synthesis_canary_basis"],
            synthesis_basis,
        )
        self.assertIsNone(
            metadata_off["unified_response_assessment_shadow"]
        )
        assessment = metadata_on["unified_response_assessment_shadow"]
        self.assertIsNotNone(assessment)
        self.assertEqual(
            assessment.response_act,
            "recap_current_exchange",
        )
        self.assertEqual(
            assessment.selected_lanes,
            ("current_exchange", "conversation_context"),
        )
        self.assertEqual(
            assessment.active_episode_id,
            "mep_opaque_shadow_reference",
        )
        self.assertIn(
            ("active_episode", "current_exchange_precedence"),
            assessment.excluded_lanes,
        )

    def test_expanded_second_user_keeps_ordinary_packet_owner(self):
        text = (
            "What do you know about Mac Modem, and what do you know "
            "about DJ Floppy Disc?"
        )
        flags = {
            "BNL_MEMORY_LEDGER_SHADOW_ENABLED": "true",
            "BNL_MOMENT_ENGINE_SHADOW_ENABLED": "true",
            "BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED": "true",
            "BNL_RELATIONSHIP_V2_SHADOW_ENABLED": "true",
            "BNL_UNIFIED_RESPONSE_ASSESSMENT_SHADOW_ENABLED": "true",
            "BNL_UNIFIED_INTELLIGENCE_PACKET_SHADOW_ENABLED": "true",
            "BNL_SHARED_BRAIN_SYNTHESIS_CANARY_ENABLED": "false",
            "BNL_PUBLIC_HOME_BROAD_RECALL_OWNER_ENABLED": "false",
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_ENABLED": "true",
            (
                "BNL_ORDINARY_CHAT_SINGLE_PACKET_"
                "SCOPED_EXPANSION_ENABLED"
            ): "true",
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_GUILD_IDS": "1",
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_USER_IDS": "101,202",
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_CHANNEL_IDS": "303",
            "BNL_MEMORY_GOVERNANCE_LIVE_ENABLED": "false",
            "BNL_RELATIONSHIP_V2_LIVE_ENABLED": "false",
            "BNL_ACTIVE_ENGAGEMENT_V2_LIVE_ENABLED": "false",
        }
        packet = object()
        assessment = SimpleNamespace()
        ordinary_basis = object()
        visual_basis = SimpleNamespace(status="not_requested")
        assessment_calls = []

        def assessment_builder(**kwargs):
            assessment_calls.append(kwargs)
            kwargs["intelligence_packet_out"]["packet"] = packet
            return assessment

        broadcast_builder = mock.Mock(
            return_value=(
                "Broadcast memory entity match:\n"
                "- 2026-08-01 broadcast_memory_note: DJ Floppy Disc"
            )
        )
        memory_builder = mock.Mock(
            return_value=(
                "Approved direct self-reports:\n"
                "- Established memory sentinel: DJ Floppy Disc"
            )
        )
        with (
            mock.patch.dict(os.environ, flags, clear=False),
            mock.patch.object(
                bnl01_bot,
                "get_user_profile",
                return_value=("Member 1", ""),
            ),
            mock.patch.object(
                bnl01_bot,
                "should_allow_greeting",
                return_value=False,
            ),
            mock.patch.object(
                bnl01_bot,
                "choose_response_style",
                return_value=("balanced", "Respond naturally."),
            ),
            mock.patch.object(
                bnl01_bot,
                "build_conversation_prompt_source_basis",
                return_value=None,
            ),
            mock.patch.object(
                bnl01_bot,
                "build_user_memory_context",
                new=memory_builder,
            ),
            mock.patch.object(
                bnl01_bot,
                "build_broadcast_memory_context",
                new=broadcast_builder,
            ),
            mock.patch.object(
                bnl01_bot,
                "build_community_visual_basis",
                return_value=visual_basis,
            ),
            mock.patch.object(
                bnl01_bot,
                "render_community_visual_basis_for_prompt",
                return_value="",
            ),
            mock.patch.object(
                bnl01_bot,
                "build_unified_response_assessment_shadow",
                side_effect=assessment_builder,
            ),
            mock.patch.object(
                bnl01_bot,
                "build_ordinary_chat_basis",
                return_value=ordinary_basis,
            ) as ordinary_basis_builder,
            mock.patch.object(
                bnl01_bot,
                "build_shared_brain_synthesis_basis",
                return_value=None,
            ),
        ):
            metadata = {}
            prompt, *_ = bnl01_bot.build_user_aware_prompt(
                202,
                1,
                "Member 2",
                text,
                channel_name="bnl-testing",
                channel_id=303,
                channel_policy="sealed_test",
                route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                is_direct_interaction=True,
                prompt_metadata=metadata,
            )

            broadcast_builder.assert_called_once()
            memory_builder.assert_called_once()
            self.assertEqual(len(assessment_calls), 1)
            self.assertFalse(
                assessment_calls[0]["broadcast_memory_present"]
            )
            self.assertNotIn(
                "broadcast_memory",
                assessment_calls[0]["prompt_lanes"],
            )
            self.assertTrue(
                metadata["ordinary_chat_single_packet_applied"]
            )
            self.assertIs(
                metadata["ordinary_chat_single_packet_basis"],
                ordinary_basis,
            )
            self.assertEqual(
                metadata["ordinary_chat_single_packet_scope"].reason,
                "eligible",
            )
            self.assertNotIn(
                "ordinary_chat_legacy_baseline_request",
                metadata,
            )
            self.assertIn("Broadcast memory context:", prompt)
            self.assertIn("Durable memory context:", prompt)
            ordinary_basis_call = ordinary_basis_builder.call_args.kwargs
            competing_contexts = ordinary_basis_call[
                "competing_factual_contexts"
            ]
            self.assertTrue(
                any(
                    "Broadcast memory context:" in context
                    for context in competing_contexts
                )
            )
            self.assertTrue(
                any(
                    "Durable memory context:" in context
                    for context in competing_contexts
                )
            )

        memory_builder.assert_called_once()
        broadcast_builder.assert_called_once()

    def test_journal_and_current_queue_compose_in_one_packet(self):
        text = (
            "What did the Journal say about the queue, and is the queue "
            "open right now?"
        )
        frame = bnl01_bot.build_situation_frame_v1(
            route_allowed=True,
            route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
            conversation_surface="sealed_test",
            channel_policy="sealed_test",
            current_text=text,
            current_speaker_user_ids=(101,),
            response_act="answer",
        )
        orchestration = bnl01_bot.ConversationOrchestrationDecision(
            response_act="answer",
            reason="direct_request",
            response_required=True,
            address_kind="discord_mention",
            continuity_required=False,
            referent_status="not_requested",
            referent_candidate_count=0,
            referent_candidate_labels=(),
            moment_situation_state="none",
            moment_topic_coherent=False,
            moment_participant_overlap=False,
            moment_human_entry_count=0,
            moment_model_entry_count=0,
            engagement_decision="respond",
            engagement_reason="direct_request",
            influence_mode="sealed_canary",
            packet_version="test",
            packet_revision="test",
            governed_memory_state="owner_not_requested",
            relationship_state="owner_tone_only",
            canon_state="owner_not_requested",
            source_control_state="route_policy_only",
            situation_frame=frame,
        )
        flags = {
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
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_USER_IDS": "101",
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_CHANNEL_IDS": "303",
            "BNL_MEMORY_GOVERNANCE_LIVE_ENABLED": "false",
            "BNL_RELATIONSHIP_V2_LIVE_ENABLED": "false",
            "BNL_ACTIVE_ENGAGEMENT_V2_LIVE_ENABLED": "false",
        }
        packet = object()
        assessment = SimpleNamespace()
        ordinary_basis = object()
        visual_basis = SimpleNamespace(status="not_requested")
        assessment_calls = []

        def assessment_builder(**kwargs):
            assessment_calls.append(kwargs)
            kwargs["intelligence_packet_out"]["packet"] = packet
            return assessment

        with (
            mock.patch.dict(os.environ, flags, clear=False),
            mock.patch.object(
                bnl01_bot,
                "get_user_profile",
                return_value=("Member 1", ""),
            ),
            mock.patch.object(
                bnl01_bot,
                "should_allow_greeting",
                return_value=False,
            ),
            mock.patch.object(
                bnl01_bot,
                "choose_response_style",
                return_value=("balanced", "Respond naturally."),
            ),
            mock.patch.object(
                bnl01_bot,
                "build_conversation_prompt_source_basis",
                return_value=None,
            ),
            mock.patch.object(
                bnl01_bot,
                "build_broadcast_memory_context",
                return_value="",
            ),
            mock.patch.object(
                bnl01_bot,
                "build_queue_artist_memory_context",
                return_value="QUEUE OWNER SENTINEL",
            ),
            mock.patch.object(
                bnl01_bot,
                "build_bnl_queue_packet_snapshot",
                return_value="CURRENT QUEUE SNAPSHOT",
            ) as queue_snapshot,
            mock.patch.object(
                bnl01_bot,
                "build_tiktok_show_evidence_context_for_turn",
                return_value="SHOW OWNER SENTINEL",
            ),
            mock.patch.object(
                bnl01_bot,
                "finalized_show_packet_owner_requested",
                return_value=False,
            ),
            mock.patch.object(
                bnl01_bot,
                "build_community_visual_basis",
                return_value=visual_basis,
            ),
            mock.patch.object(
                bnl01_bot,
                "render_community_visual_basis_for_prompt",
                return_value="",
            ),
            mock.patch.object(
                bnl01_bot,
                "build_unified_response_assessment_shadow",
                side_effect=assessment_builder,
            ),
            mock.patch.object(
                bnl01_bot,
                "build_ordinary_chat_basis",
                return_value=ordinary_basis,
            ) as ordinary_basis_builder,
            mock.patch.object(
                bnl01_bot,
                "build_shared_brain_synthesis_basis",
                return_value=None,
            ),
        ):
            metadata = {}
            prompt, *_ = bnl01_bot.build_user_aware_prompt(
                101,
                1,
                "Member 1",
                text,
                show_state_context="SHOW STATE OWNER SENTINEL",
                channel_name="bnl-testing",
                channel_id=303,
                channel_policy="sealed_test",
                route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                is_direct_interaction=True,
                room_context="RECENT ROOM SENTINEL",
                website_read_model_context="WEBSITE QUEUE OWNER SENTINEL",
                conversation_orchestration=orchestration,
                prompt_metadata=metadata,
            )

        self.assertTrue(metadata["ordinary_chat_single_packet_applied"])
        self.assertEqual(
            metadata["ordinary_chat_single_packet_scope"].reason,
            "eligible",
        )
        self.assertIs(
            metadata["ordinary_chat_single_packet_basis"],
            ordinary_basis,
        )
        self.assertEqual(len(assessment_calls), 1)
        self.assertTrue(
            assessment_calls[0]["website_read_model_present"]
        )
        self.assertFalse(assessment_calls[0]["show_state_present"])
        self.assertIn(
            "website_read_model",
            assessment_calls[0]["prompt_lanes"],
        )
        self.assertNotIn(
            "queue_artist_memory",
            assessment_calls[0]["prompt_lanes"],
        )
        self.assertEqual(
            assessment_calls[0]["operational_context_snapshot"],
            "CURRENT QUEUE SNAPSHOT",
        )
        self.assertTrue(
            assessment_calls[0][
                "packet_operational_context_authorized"
            ]
        )
        queue_snapshot.assert_called_once_with(
            text,
            "sealed_test",
            force=False,
        )
        self.assertNotIn("WEBSITE QUEUE OWNER SENTINEL", prompt)
        self.assertIn("RECENT ROOM SENTINEL", prompt)
        self.assertNotIn("SHOW STATE OWNER SENTINEL", prompt)
        self.assertIn("QUEUE OWNER SENTINEL", prompt)
        self.assertIn("SHOW OWNER SENTINEL", prompt)
        competing_contexts = (
            ordinary_basis_builder.call_args.kwargs[
                "competing_factual_contexts"
            ]
        )
        self.assertFalse(
            any(
                "WEBSITE QUEUE OWNER SENTINEL" in context
                for context in competing_contexts
            )
        )
        self.assertTrue(
            any(
                "RECENT ROOM SENTINEL" in context
                for context in competing_contexts
            )
        )
        self.assertFalse(
            any(
                "SHOW STATE OWNER SENTINEL" in context
                for context in competing_contexts
            )
        )

    def test_bot_recorder_persists_only_aggregate_receipt(self):
        with mock.patch.object(
            bnl01_bot,
            "unified_response_assessment_shadow_enabled",
            return_value=True,
        ):
            assessment = (
                bnl01_bot.build_unified_response_assessment_shadow(
                    guild_id=1,
                    route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                    channel_policy="sealed_test",
                    conversation_surface="test",
                    current_text="What did everyone just decide?",
                    current_speaker_user_ids=(101, 102),
                    current_speaker_labels=("PRIVATE A", "PRIVATE B"),
                    prompt_source_bases=(self.conversation_basis(2),),
                    prompt_lanes=(
                        "current_exchange",
                        "conversation_context",
                    ),
                    continuity_required=True,
                )
            )

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "assessment.db")
            with mock.patch.object(bnl01_bot, "DB_FILE", db_path):
                run_id = (
                    bnl01_bot.record_unified_response_assessment_shadow(
                        assessment,
                        response=(
                            "PRIVATE A has the intro and PRIVATE B has the art."
                        ),
                    )
                )

            self.assertTrue(run_id.startswith("ura_"))
            with sqlite3.connect(db_path) as conn:
                row = conn.execute(
                    "SELECT current_speaker_count, participant_count, "
                    "response_length, behavior_changed, new_authority_applied "
                    "FROM unified_response_assessment_shadow_runs"
                ).fetchone()
                self.assertEqual(row[:2], (2, 2))
                self.assertGreater(row[2], 0)
                self.assertEqual(row[3:], (0, 0))
                schema = "\n".join(
                    str(item)
                    for item in conn.execute(
                        "SELECT sql FROM sqlite_master "
                        "WHERE name='unified_response_assessment_shadow_runs'"
                    ).fetchone()
                )
                self.assertNotIn("response_text", schema)
                self.assertNotIn("speaker_labels", schema)

    def test_conversation_basis_carries_typed_human_contributions(self):
        selected_rows = (
            {
                "id": 10,
                "role": "user",
                "content": (
                    "The hidden room should sound like a place, "
                    "not a character."
                ),
                "user_id": 202,
                "user_name": "Miss Bit",
            },
            {
                "id": 11,
                "role": "model",
                "content": "Prior BNL reply.",
                "user_id": 0,
                "user_name": "BNL-01",
            },
        )
        with mock.patch.object(
            bnl01_bot,
            "conversation_context_v2_enabled",
            return_value=True,
        ):
            with mock.patch.object(
                bnl01_bot,
                "get_conversation_context_v2_rows",
                return_value=list(selected_rows),
            ):
                with mock.patch.object(
                    bnl01_bot,
                    "_conversation_prompt_source_rows",
                    return_value=list(selected_rows),
                ):
                    basis = (
                        bnl01_bot.build_conversation_prompt_source_basis(
                            "Conversation continuity:\nselected",
                            guild_id=1,
                            current_user_id=101,
                            channel_id=303,
                            channel_name="bnl-testing",
                            channel_policy="sealed_test",
                        )
                    )

        self.assertIsNotNone(basis)
        self.assertEqual(len(basis.evidence_items), 1)
        item = basis.evidence_items[0]
        self.assertEqual(item.source_id, 10)
        self.assertEqual(item.speaker_user_id, 202)
        self.assertEqual(item.speaker_label, "Miss Bit")
        self.assertIn("criterion", item.semantic_roles)
        self.assertEqual(item.criterion_positive_terms, ("place",))
        self.assertEqual(item.criterion_negative_terms, ("character",))

    def test_conversation_basis_tracks_exact_reply_and_hidden_competitor(self):
        exact_row = {
            "id": 10,
            "role": "user",
            "content": (
                "Idea A: a radio tower that wakes up at midnight."
            ),
            "user_id": 101,
            "user_name": "Test Member",
        }
        competing_row = {
            "id": 11,
            "role": "user",
            "content": (
                "Idea B: a vending machine that trades memories."
            ),
            "user_id": 101,
            "user_name": "Test Member",
        }
        context_result = SimpleNamespace(
            referent_status="resolved",
            referent_reason="discord_reply_source",
            referent_selected_row_ids=(10,),
            referent_competing_row_ids=(11,),
            referent_scope_expanded=False,
        )
        rendered = (
            "Conversation continuity:\n"
            "Test Member (exact Discord reply source): "
            "Idea A: a radio tower that wakes up at midnight."
        )
        with (
            mock.patch.object(
                bnl01_bot,
                "conversation_context_v2_enabled",
                return_value=True,
            ),
            mock.patch.object(
                bnl01_bot,
                "get_conversation_context_v2_rows",
                return_value=[exact_row, competing_row],
            ),
            mock.patch.object(
                bnl01_bot,
                "_conversation_prompt_source_rows",
                return_value=[exact_row],
            ),
        ):
            basis = bnl01_bot.build_conversation_prompt_source_basis(
                rendered,
                guild_id=1,
                current_user_id=101,
                channel_id=303,
                channel_name="bnl-testing",
                channel_policy="sealed_test",
                context_result=context_result,
            )

        self.assertIsNotNone(basis)
        self.assertEqual(basis.source_row_ids, (10,))
        self.assertEqual(basis.revalidation_row_ids, (10, 11))
        self.assertEqual(
            tuple(item.source_id for item in basis.evidence_items),
            (10,),
        )
        self.assertEqual(
            tuple(
                item.text
                for item in basis.referent_source_evidence_items
            ),
            ("Idea A: a radio tower that wakes up at midnight.",),
        )
        self.assertEqual(
            tuple(
                item.text
                for item in basis.referent_competing_evidence_items
            ),
            ("Idea B: a vending machine that trades memories.",),
        )
        self.assertNotIn("vending machine", basis.rendered_context)
        self.assertFalse(basis.referent_scope_expanded)

    def test_bot_adapter_distinguishes_current_fragments_from_prior_choice(self):
        basis = bnl01_bot.ConversationPromptSourceBasis(
            expected_digest="digest",
            rendered_context=(
                "Conversation continuity:\n"
                "User/member: Pick Ghost Signal or Neon Static.\n"
                "BNL-01: Ghost Signal is stronger.\n"
                "User/member (current payload fragment): "
                "“Dead Channel” sounds abandoned.\n"
                "User/member (current payload fragment): "
                "“Open Circuit” sounds active."
            ),
            guild_id=1,
            current_user_id=101,
            channel_id=303,
            channel_name="bnl-testing",
            channel_policy="sealed_test",
            source_row_ids=(1, 2, 3, 4),
            participant_user_ids=(101, 102),
            speaker_labels=("Test Member", "Miss Bit"),
            evidence_items=(
                bnl01_bot.build_conversation_evidence_item(
                    text="Pick Ghost Signal or Neon Static.",
                    source_id=1,
                    speaker_user_id=101,
                    speaker_label="Test Member",
                ),
                bnl01_bot.build_conversation_evidence_item(
                    text="“Dead Channel” sounds abandoned.",
                    source_id=3,
                    speaker_user_id=101,
                    speaker_label="Test Member",
                ),
                bnl01_bot.build_conversation_evidence_item(
                    text=(
                        "The hidden test zone should sound abandoned, "
                        "not active."
                    ),
                    source_id=4,
                    speaker_user_id=102,
                    speaker_label="Miss Bit",
                ),
                bnl01_bot.build_conversation_evidence_item(
                    text="“Open Circuit” sounds active.",
                    source_id=5,
                    speaker_user_id=101,
                    speaker_label="Test Member",
                ),
            ),
        )
        with mock.patch.object(
            bnl01_bot,
            "unified_response_assessment_shadow_enabled",
            return_value=True,
        ):
            assessment = (
                bnl01_bot.build_unified_response_assessment_shadow(
                    guild_id=1,
                    route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                    channel_policy="sealed_test",
                    conversation_surface="test",
                    current_text=(
                        "Which title fits a hidden test zone better, and why?"
                    ),
                    current_speaker_user_ids=(101,),
                    current_speaker_labels=("Test Member",),
                    prompt_source_bases=(basis,),
                    prompt_lanes=(
                        "current_exchange",
                        "conversation_context",
                    ),
                )
            )

        self.assertEqual(
            assessment.current_payload_anchors,
            ("dead channel", "open circuit"),
        )
        self.assertEqual(
            assessment.prior_thread_anchors,
            ("ghost signal", "neon static"),
        )
        self.assertEqual(assessment.thread_focus_mode, "new_thread")
        self.assertEqual(assessment.comparison_status, "match")
        self.assertEqual(assessment.objective_kind, "compare_options")
        self.assertEqual(
            assessment.attributed_criteria[0].speaker_user_id,
            102,
        )
        self.assertEqual(
            assessment.expected_answer_shape,
            "choice_then_reason",
        )


class CurrentPayloadGroundingGuardTests(unittest.IsolatedAsyncioTestCase):
    async def test_shared_guard_regenerates_stale_option_substitution(self):
        provider = mock.AsyncMock(
            return_value=(
                "Dead Channel fits the hidden test zone better because it "
                "sounds deliberately abandoned."
            )
        )
        current_text = (
            'Pick between “Dead Channel” and “Open Circuit” for the hidden '
            "test zone."
        )
        with mock.patch.object(
            bnl01_bot,
            "get_gemini_response_with_optional_typing",
            provider,
        ):
            response, diagnostics = (
                await bnl01_bot.apply_guarded_response_regeneration(
                    "Ghost Signal is the stronger choice.",
                    prompt="Current user request: " + current_text,
                    user_id=101,
                    guild_id=1,
                    route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                    channel_policy="sealed_test",
                    current_user_text=current_text,
                )
            )

        self.assertIn("Dead Channel", response)
        self.assertTrue(
            diagnostics["current_payload_grounding_guard_triggered"]
        )
        self.assertTrue(
            diagnostics["current_payload_grounding_regenerated"]
        )
        self.assertEqual(
            diagnostics["current_payload_grounding_status"],
            "grounded_current_payload",
        )
        self.assertFalse(diagnostics["suppressed"])
        provider.assert_awaited_once()
        self.assertIn(
            "CURRENT-PAYLOAD CORRECTION REQUIRED",
            provider.await_args.args[1],
        )
        retry_prompt = provider.await_args.args[1].lower()
        self.assertIn("resolved current alternatives", retry_prompt)
        self.assertIn("dead channel", retry_prompt)
        self.assertIn("open circuit", retry_prompt)

    async def test_shared_guard_accepts_referential_current_choice_answer(self):
        provider = mock.AsyncMock()
        current_text = (
            "Between Circuit Saint and Null Chapel, which fits the hidden "
            "room better?"
        )
        with mock.patch.object(
            bnl01_bot,
            "get_gemini_response_with_optional_typing",
            provider,
        ):
            response, diagnostics = (
                await bnl01_bot.apply_guarded_response_regeneration(
                    (
                        "The latter fits better because it reads as a place "
                        "instead of a person."
                    ),
                    prompt="Current user request: " + current_text,
                    user_id=101,
                    guild_id=1,
                    route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                    channel_policy="sealed_test",
                    current_user_text=current_text,
                )
            )

        self.assertIn("The latter fits better", response)
        self.assertEqual(
            diagnostics["current_payload_grounding_status"],
            "grounded_current_payload_reference",
        )
        self.assertFalse(
            diagnostics["current_payload_grounding_guard_triggered"]
        )
        self.assertFalse(diagnostics["suppressed"])
        provider.assert_not_awaited()

    async def test_shared_guard_accepts_referential_grounding_retry(self):
        provider = mock.AsyncMock(
            return_value=(
                "The second option fits better because it sounds like a place."
            )
        )
        current_text = (
            "Between Circuit Saint and Null Chapel, which fits the hidden "
            "room better?"
        )
        with mock.patch.object(
            bnl01_bot,
            "get_gemini_response_with_optional_typing",
            provider,
        ):
            response, diagnostics = (
                await bnl01_bot.apply_guarded_response_regeneration(
                    "Ghost Signal is still the stronger archive name.",
                    prompt="Current user request: " + current_text,
                    user_id=101,
                    guild_id=1,
                    route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                    channel_policy="sealed_test",
                    current_user_text=current_text,
                )
            )

        self.assertIn("The second option fits better", response)
        self.assertTrue(
            diagnostics["current_payload_grounding_guard_triggered"]
        )
        self.assertTrue(
            diagnostics["current_payload_grounding_regenerated"]
        )
        self.assertEqual(
            diagnostics["current_payload_grounding_status"],
            "grounded_current_payload_reference",
        )
        self.assertFalse(diagnostics["suppressed"])
        provider.assert_awaited_once()

    async def test_shared_guard_suppresses_second_unanswered_choice(self):
        provider = mock.AsyncMock(
            return_value="Ghost Signal still feels strongest."
        )
        current_text = (
            'Choose “Dead Channel” or “Open Circuit” for the hidden test zone.'
        )
        with mock.patch.object(
            bnl01_bot,
            "get_gemini_response_with_optional_typing",
            provider,
        ):
            response, diagnostics = (
                await bnl01_bot.apply_guarded_response_regeneration(
                    "Neon Static is the better title.",
                    prompt="Current user request: " + current_text,
                    user_id=101,
                    guild_id=1,
                    route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                    channel_policy="sealed_test",
                    current_user_text=current_text,
                )
            )

        self.assertEqual(response, "")
        self.assertTrue(diagnostics["suppressed"])
        self.assertEqual(
            diagnostics["suppression_reason"],
            "current_payload_grounding_after_retry",
        )


class ExactReplyGroundingGuardTests(unittest.IsolatedAsyncioTestCase):
    def reply_basis(self):
        return bnl01_bot.ConversationPromptSourceBasis(
            expected_digest="stable",
            rendered_context=(
                "Conversation continuity:\n"
                "Test Member (exact Discord reply source): "
                "Idea A: a radio tower that wakes up at midnight."
            ),
            guild_id=1,
            current_user_id=101,
            channel_id=303,
            channel_name="bnl-testing",
            channel_policy="sealed_test",
            referent_status="resolved",
            referent_reason="discord_reply_source",
            referent_source_evidence_items=(
                bnl01_bot.build_conversation_evidence_item(
                    text=(
                        "Idea A: a radio tower that wakes up at midnight."
                    ),
                    source_id=10,
                    speaker_user_id=101,
                    speaker_label="Test Member",
                ),
            ),
            referent_competing_evidence_items=(
                bnl01_bot.build_conversation_evidence_item(
                    text=(
                        "Idea B: a vending machine that trades memories."
                    ),
                    source_id=11,
                    speaker_user_id=101,
                    speaker_label="Test Member",
                ),
            ),
        )

    async def test_shared_guard_regenerates_exact_canary_source_switch(self):
        provider = mock.AsyncMock(
            return_value=(
                "At midnight, the radio tower wakes and broadcasts one "
                "forgotten voice across the sleeping city."
            )
        )
        basis = self.reply_basis()
        prompt = (
            "Current user request: BNL, improve this idea in one sentence.\n\n"
            + basis.rendered_context
        )
        with (
            mock.patch.object(
                bnl01_bot,
                "get_gemini_response_with_optional_typing",
                provider,
            ),
            mock.patch.object(
                bnl01_bot,
                "refresh_prompt_source_bases",
                return_value=(prompt, (basis,), (), False),
            ),
            mock.patch.object(
                bnl01_bot,
                "prompt_source_basis_failure",
                return_value="",
            ),
        ):
            response, diagnostics = (
                await bnl01_bot.apply_guarded_response_regeneration(
                    (
                        "The vending machine hums after dark, accepting "
                        "memories instead of coins."
                    ),
                    prompt=prompt,
                    user_id=101,
                    guild_id=1,
                    route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                    channel_policy="sealed_test",
                    current_user_text=(
                        "BNL, improve this idea in one sentence."
                    ),
                    prompt_source_bases=(basis,),
                )
            )

        self.assertIn("radio tower", response)
        self.assertTrue(
            diagnostics["exact_reply_grounding_guard_triggered"]
        )
        self.assertTrue(
            diagnostics["exact_reply_grounding_regenerated"]
        )
        self.assertEqual(
            diagnostics["exact_reply_grounding_status"],
            "grounded_exact_reply_source",
        )
        self.assertFalse(diagnostics["suppressed"])
        provider.assert_awaited_once()
        retry_prompt = provider.await_args.args[1]
        self.assertIn(
            "EXACT-REPLY GROUNDING CORRECTION REQUIRED",
            retry_prompt,
        )
        self.assertIn("radio tower", retry_prompt)
        self.assertNotIn("vending machine", retry_prompt)

    async def test_shared_guard_suppresses_a_second_competing_reply_answer(self):
        provider = mock.AsyncMock(
            return_value=(
                "The vending machine trades memories after midnight."
            )
        )
        basis = self.reply_basis()
        prompt = (
            "Current user request: BNL, improve this idea in one sentence.\n\n"
            + basis.rendered_context
        )
        with (
            mock.patch.object(
                bnl01_bot,
                "get_gemini_response_with_optional_typing",
                provider,
            ),
            mock.patch.object(
                bnl01_bot,
                "refresh_prompt_source_bases",
                return_value=(prompt, (basis,), (), False),
            ),
            mock.patch.object(
                bnl01_bot,
                "prompt_source_basis_failure",
                return_value="",
            ),
        ):
            response, diagnostics = (
                await bnl01_bot.apply_guarded_response_regeneration(
                    (
                        "The vending machine hums after dark, accepting "
                        "memories instead of coins."
                    ),
                    prompt=prompt,
                    user_id=101,
                    guild_id=1,
                    route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                    channel_policy="sealed_test",
                    current_user_text=(
                        "BNL, improve this idea in one sentence."
                    ),
                    prompt_source_bases=(basis,),
                )
            )

        self.assertEqual(response, "")
        self.assertTrue(diagnostics["suppressed"])
        self.assertEqual(
            diagnostics["suppression_reason"],
            "exact_reply_grounding_after_retry",
        )


if __name__ == "__main__":
    unittest.main()
