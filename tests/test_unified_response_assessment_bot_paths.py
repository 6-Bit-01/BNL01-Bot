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
            "_active_episode_id_for_unified_assessment",
            return_value="mep_opaque_shadow_reference",
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
