import os
from types import SimpleNamespace
import unittest
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl01_bot


CANARY_ENV = {
    "BNL_MEMORY_LEDGER_SHADOW_ENABLED": "true",
    "BNL_MOMENT_ENGINE_SHADOW_ENABLED": "true",
    "BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED": "true",
    "BNL_RELATIONSHIP_V2_SHADOW_ENABLED": "true",
    "BNL_MEMORY_GOVERNANCE_LIVE_ENABLED": "false",
    "BNL_RELATIONSHIP_V2_LIVE_ENABLED": "false",
    "BNL_ACTIVE_ENGAGEMENT_V2_LIVE_ENABLED": "false",
    "BNL_UNIFIED_RESPONSE_ASSESSMENT_SHADOW_ENABLED": "true",
    "BNL_UNIFIED_MOMENT_CANARY_ENABLED": "true",
    "BNL_UNIFIED_MOMENT_CANARY_GUILD_IDS": "1",
    "BNL_UNIFIED_MOMENT_CANARY_CHANNEL_IDS": "303",
}


class UnifiedMomentSealedCanaryTests(unittest.IsolatedAsyncioTestCase):
    def assessment(self):
        evidence = (
            bnl01_bot.build_conversation_evidence_item(
                text="“Chrome Prophet” sounds like a person.",
                source_id=10,
                speaker_user_id=101,
                speaker_label="Jon",
            ),
            bnl01_bot.build_conversation_evidence_item(
                text=(
                    "The hidden room should sound like a place, "
                    "not a character."
                ),
                source_id=11,
                speaker_user_id=202,
                speaker_label="Miss Bit",
            ),
            bnl01_bot.build_conversation_evidence_item(
                text="“Null Basilica” sounds like a place.",
                source_id=12,
                speaker_user_id=101,
                speaker_label="Jon",
            ),
        )
        return bnl01_bot.build_unified_response_assessment(
            guild_id=1,
            route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
            channel_policy="sealed_test",
            conversation_surface="test",
            current_speaker_user_ids=(101,),
            participant_user_ids=(101, 202),
            speaker_labels=("Jon", "Miss Bit"),
            current_exchange_source_ids=(10, 11, 12),
            prompt_lanes=("current_exchange", "conversation_context"),
            current_payload_anchors=(
                "chrome prophet",
                "null basilica",
            ),
            thread_focus_mode="new_thread",
            current_text=(
                "Between Chrome Prophet and Null Basilica, which fits that "
                "requirement better, and why?"
            ),
            conversation_evidence_items=evidence,
        )

    def build_basis(self):
        with mock.patch.object(bnl01_bot, "DB_FILE", ":memory:"):
            return (
                bnl01_bot
                .build_unified_moment_canary_prompt_source_basis(
                    self.assessment(),
                    guild_id=1,
                    channel_id=303,
                    channel_policy="sealed_test",
                    route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                    topic_text=(
                        "Between Chrome Prophet and Null Basilica, which "
                        "fits that requirement better, and why?"
                    ),
                    participant_user_ids=(101, 202),
                )
            )

    def test_configuration_requires_exact_sealed_guild_and_channel(self):
        with mock.patch.dict(os.environ, CANARY_ENV, clear=False):
            self.assertTrue(
                bnl01_bot.unified_moment_canary_enabled(
                    guild_id=1,
                    channel_id=303,
                    route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                    channel_policy="sealed_test",
                )
            )
            self.assertFalse(
                bnl01_bot.unified_moment_canary_enabled(
                    guild_id=1,
                    channel_id=303,
                    route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                    channel_policy="public_home",
                )
            )
            self.assertFalse(
                bnl01_bot.unified_moment_canary_enabled(
                    guild_id=1,
                    channel_id=304,
                    route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                    channel_policy="sealed_test",
                )
            )
            configuration = (
                bnl01_bot.unified_moment_canary_configuration()
            )
            self.assertTrue(configuration["fully_scoped"])
            self.assertEqual(configuration["guild_allowlist_count"], 1)
            self.assertEqual(
                configuration["channel_allowlist_count"],
                1,
            )
        broadened = {
            **CANARY_ENV,
            "BNL_UNIFIED_MOMENT_CANARY_GUILD_IDS": "1,2",
            "BNL_UNIFIED_MOMENT_CANARY_CHANNEL_IDS": "303,304",
        }
        with mock.patch.dict(os.environ, broadened, clear=False):
            self.assertFalse(
                bnl01_bot.unified_moment_canary_configuration()[
                    "fully_scoped"
                ]
            )
            self.assertFalse(
                bnl01_bot.unified_moment_canary_enabled(
                    guild_id=1,
                    channel_id=303,
                    route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                    channel_policy="sealed_test",
                )
            )

        disabled = {**CANARY_ENV, "BNL_UNIFIED_MOMENT_CANARY_ENABLED": "false"}
        with mock.patch.dict(os.environ, disabled, clear=False):
            self.assertFalse(
                bnl01_bot.unified_moment_canary_enabled(
                    guild_id=1,
                    channel_id=303,
                    route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                    channel_policy="sealed_test",
                )
            )

    def test_prompt_basis_exists_only_for_the_scoped_sealed_route(self):
        with mock.patch.dict(os.environ, CANARY_ENV, clear=False):
            basis = self.build_basis()
            self.assertIsNotNone(basis)
            self.assertIn(
                "SEALED UNIFIED CONVERSATION CANARY",
                basis.rendered_context,
            )
            self.assertFalse(basis.episode_context_present)
            self.assertNotIn(
                "active_episode",
                basis.assessment.prompt_lanes,
            )

            public_assessment = bnl01_bot.build_unified_response_assessment(
                guild_id=1,
                route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                channel_policy="public_home",
                conversation_surface="public",
                current_speaker_user_ids=(101,),
                prompt_lanes=("current_exchange",),
            )
            with mock.patch.object(bnl01_bot, "DB_FILE", ":memory:"):
                public_basis = (
                    bnl01_bot
                    .build_unified_moment_canary_prompt_source_basis(
                        public_assessment,
                        guild_id=1,
                        channel_id=303,
                        channel_policy="public_home",
                        route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                        topic_text="What do you think?",
                        participant_user_ids=(101,),
                    )
                )
            self.assertIsNone(public_basis)

    def test_direct_prompt_applies_in_sealed_channel_and_public_is_identical(self):
        conversation_basis = bnl01_bot.ConversationPromptSourceBasis(
            expected_digest="digest",
            rendered_context="bounded room context",
            guild_id=1,
            current_user_id=101,
            channel_id=303,
            channel_name="bnl-testing",
            channel_policy="sealed_test",
            source_row_ids=(10, 11, 12),
            participant_user_ids=(101, 202),
            speaker_labels=("Jon", "Miss Bit"),
            evidence_items=self.assessment().conversation_evidence_items,
        )

        def conversation_basis_for_route(
            _rendered,
            *,
            channel_policy,
            **_kwargs,
        ):
            return bnl01_bot.replace(
                conversation_basis,
                channel_policy=channel_policy,
            )

        patches = (
            mock.patch.object(
                bnl01_bot,
                "get_user_profile",
                return_value=("Jon", ""),
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
                return_value="No route-safe durable memory for this mode/channel.",
            ),
            mock.patch.object(
                bnl01_bot,
                "build_broadcast_memory_context",
                return_value="",
            ),
            mock.patch.object(
                bnl01_bot,
                "build_conversation_prompt_source_basis",
                side_effect=conversation_basis_for_route,
            ),
            mock.patch.object(
                bnl01_bot,
                "build_community_visual_basis",
                return_value=SimpleNamespace(status="not_requested"),
            ),
            mock.patch.object(
                bnl01_bot,
                "render_community_visual_basis_for_prompt",
                return_value="",
            ),
            mock.patch.object(
                bnl01_bot,
                "_active_episode_id_for_unified_assessment",
                return_value="opaque_active_episode",
            ),
            mock.patch.object(bnl01_bot, "DB_FILE", ":memory:"),
        )
        for patcher in patches:
            patcher.start()
            self.addCleanup(patcher.stop)

        request = (
            "Between Chrome Prophet and Null Basilica, which fits that "
            "requirement better, and why?"
        )
        with mock.patch.dict(os.environ, CANARY_ENV, clear=False):
            sealed_metadata = {}
            sealed_prompt, *_ = bnl01_bot.build_user_aware_prompt(
                101,
                1,
                "Jon",
                request,
                room_context="bounded room context",
                channel_name="bnl-testing",
                channel_id=303,
                channel_policy="sealed_test",
                route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                is_direct_interaction=True,
                prompt_metadata=sealed_metadata,
            )
            public_prompt_on, *_ = bnl01_bot.build_user_aware_prompt(
                101,
                1,
                "Jon",
                request,
                room_context="bounded room context",
                channel_name="general",
                channel_id=303,
                channel_policy="public_home",
                route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                is_direct_interaction=True,
                prompt_metadata={},
            )
        disabled = {**CANARY_ENV, "BNL_UNIFIED_MOMENT_CANARY_ENABLED": "false"}
        with mock.patch.dict(os.environ, disabled, clear=False):
            public_prompt_off, *_ = bnl01_bot.build_user_aware_prompt(
                101,
                1,
                "Jon",
                request,
                room_context="bounded room context",
                channel_name="general",
                channel_id=303,
                channel_policy="public_home",
                route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                is_direct_interaction=True,
                prompt_metadata={},
            )

        self.assertIn("SEALED UNIFIED CONVERSATION CANARY", sealed_prompt)
        self.assertTrue(sealed_metadata["unified_moment_canary_applied"])
        self.assertNotIn(
            "SEALED UNIFIED CONVERSATION CANARY",
            public_prompt_on,
        )
        self.assertEqual(public_prompt_on, public_prompt_off)

    async def test_coherence_guard_repairs_conclusion_reason_contradiction(self):
        with mock.patch.dict(os.environ, CANARY_ENV, clear=False):
            basis = self.build_basis()
            provider = mock.AsyncMock(
                return_value=(
                    "Null Basilica fits better because it reads as a place "
                    "instead of a person."
                )
            )
            with mock.patch.object(
                bnl01_bot,
                "get_gemini_response_with_optional_typing",
                provider,
            ), mock.patch.object(bnl01_bot, "DB_FILE", ":memory:"):
                response, diagnostics = (
                    await bnl01_bot.apply_guarded_response_regeneration(
                        (
                            "Chrome Prophet is the better fit because it "
                            "sounds like a person, while Null Basilica sounds "
                            "like a place."
                        ),
                        prompt=basis.rendered_context,
                        user_id=101,
                        guild_id=1,
                        route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                        channel_policy="sealed_test",
                        current_user_text=(
                            "Between Chrome Prophet and Null Basilica, which "
                            "fits that requirement better, and why?"
                        ),
                        channel=SimpleNamespace(id=303),
                        prompt_source_bases=(basis,),
                    )
                )

        self.assertIn("Null Basilica fits better", response)
        self.assertTrue(diagnostics["unified_moment_canary_applied"])
        self.assertTrue(diagnostics["unified_moment_canary_scope_valid"])
        self.assertTrue(
            diagnostics[
                "unified_moment_canary_coherence_guard_triggered"
            ]
        )
        self.assertTrue(
            diagnostics["unified_moment_canary_coherence_regenerated"]
        )
        self.assertEqual(
            diagnostics["unified_moment_canary_coherence_status"],
            "passed",
        )
        self.assertFalse(diagnostics["suppressed"])
        provider.assert_awaited_once()
        self.assertIn(
            "CANARY COHERENCE CORRECTION REQUIRED",
            provider.await_args.args[1],
        )

    async def test_output_leak_guard_repairs_internal_narration(self):
        with mock.patch.dict(os.environ, CANARY_ENV, clear=False):
            basis = self.build_basis()
            provider = mock.AsyncMock(
                return_value=(
                    "Null Basilica fits better because it reads as a place "
                    "instead of a person."
                )
            )
            with mock.patch.object(
                bnl01_bot,
                "get_gemini_response_with_optional_typing",
                provider,
            ), mock.patch.object(bnl01_bot, "DB_FILE", ":memory:"):
                response, diagnostics = (
                    await bnl01_bot.apply_guarded_response_regeneration(
                        (
                            "The unified response assessment says Null "
                            "Basilica fits better."
                        ),
                        prompt=basis.rendered_context,
                        user_id=101,
                        guild_id=1,
                        route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                        channel_policy="sealed_test",
                        current_user_text=(
                            "Between Chrome Prophet and Null Basilica, which "
                            "fits that requirement better, and why?"
                        ),
                        channel=SimpleNamespace(id=303),
                        prompt_source_bases=(basis,),
                    )
                )

        self.assertNotIn("unified response assessment", response.lower())
        self.assertTrue(
            diagnostics[
                "unified_moment_canary_output_leak_guard_triggered"
            ]
        )
        self.assertTrue(
            diagnostics["unified_moment_canary_output_leak_regenerated"]
        )
        self.assertFalse(diagnostics["suppressed"])

    async def test_guard_fails_closed_if_basis_crosses_channel_scope(self):
        with mock.patch.dict(os.environ, CANARY_ENV, clear=False):
            basis = self.build_basis()
            with mock.patch.object(bnl01_bot, "DB_FILE", ":memory:"):
                response, diagnostics = (
                    await bnl01_bot.apply_guarded_response_regeneration(
                        "Null Basilica fits the place criterion.",
                        prompt=basis.rendered_context,
                        user_id=101,
                        guild_id=1,
                        route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                        channel_policy="sealed_test",
                        current_user_text="Which option fits?",
                        channel=SimpleNamespace(id=304),
                        prompt_source_bases=(basis,),
                    )
                )

        self.assertEqual(response, "")
        self.assertTrue(diagnostics["suppressed"])
        self.assertFalse(
            diagnostics["unified_moment_canary_scope_valid"]
        )
        self.assertEqual(
            diagnostics["suppression_reason"],
            "unified_moment_canary_scope_invalid",
        )

    def test_kill_switch_invalidates_existing_canary_basis(self):
        with mock.patch.dict(os.environ, CANARY_ENV, clear=False):
            basis = self.build_basis()
        disabled = {**CANARY_ENV, "BNL_UNIFIED_MOMENT_CANARY_ENABLED": "false"}
        with mock.patch.dict(os.environ, disabled, clear=False), mock.patch.object(
            bnl01_bot,
            "DB_FILE",
            ":memory:",
        ):
            self.assertEqual(
                bnl01_bot.prompt_source_basis_failure((basis,)),
                "unified_moment_canary_source_changed",
            )


if __name__ == "__main__":
    unittest.main()
