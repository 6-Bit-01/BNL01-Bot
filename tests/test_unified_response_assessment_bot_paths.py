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
        with mock.patch.object(
            bnl01_bot,
            "unified_response_assessment_shadow_enabled",
            return_value=True,
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
            speaker_labels=("Jon", "Miss Bit"),
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
                    current_speaker_labels=("Jon",),
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


if __name__ == "__main__":
    unittest.main()
