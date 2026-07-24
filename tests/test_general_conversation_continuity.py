import os
from types import SimpleNamespace
import unittest
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl01_bot


BOUNDED_CONTEXT = (
    "Conversation continuity (bounded; continuity-only, not canon/current-state evidence):\n"
    "- Prior BNL replies here are conversational continuity only.\n"
    "User/member (display name “Member”): The second signal idea uses rotating member phrases.\n"
    "BNL-01 (reply to Member): Keep the phrases legible and let the frame glitch around them."
)

CONTINUITY_PROMPT = (
    "Current user request: Keep going.\n"
    "[CONVERSATION_CONTINUITY_REQUIRED]\n"
    "General conversation continuity contract:\n"
    "- Continue or answer the same conversational act directly."
)

MOMENT_ONLY_CONTEXT = (
    "Moment-based continuity gist (derived from eligible public conversation; "
    "paraphrase only, never exact wording):\n"
    "- An earlier shared planning moment centered on a synth arrangement with "
    "a restrained intro, a rising arpeggio, and a noisy final transition.\n"
    "Use this to connect the current request to the remembered meaning of an "
    "earlier shared event. This gist can never justify a quotation or settle "
    "a dispute."
)


class GeneralConversationContinuityContractTests(unittest.TestCase):
    def test_domain_neutral_followups_activate_with_bounded_context(self):
        cases = {
            "ordinary_reference": "What about that one?",
            "technical_followup": "Try that again, but with a five-second timeout.",
            "story_continuation": "Keep going.",
            "idea_development": "Based on that, develop the second idea.",
        }

        for label, current_text in cases.items():
            with self.subTest(label=label):
                contract = bnl01_bot.build_general_conversation_continuity_contract(
                    current_text,
                    BOUNDED_CONTEXT,
                )
                self.assertEqual(
                    contract.count("[CONVERSATION_CONTINUITY_REQUIRED]"),
                    1,
                )
                self.assertIn("General conversation continuity contract:", contract)
                self.assertIn("same conversational act directly", contract)

    def test_explicit_new_topic_does_not_activate_required_marker(self):
        contract = bnl01_bot.build_general_conversation_continuity_contract(
            "New topic: explain DNS caching.",
            BOUNDED_CONTEXT,
        )

        self.assertNotIn("[CONVERSATION_CONTINUITY_REQUIRED]", contract)
        self.assertIn("General conversation continuity contract:", contract)

    def test_no_prior_context_does_not_activate_or_render_contract(self):
        contract = bnl01_bot.build_general_conversation_continuity_contract(
            "Keep going.",
            "",
        )

        self.assertEqual(contract, "")

    def test_untyped_moment_text_does_not_activate_continuity(self):
        contract = bnl01_bot.build_general_conversation_continuity_contract(
            "Continue that synth plan.",
            MOMENT_ONLY_CONTEXT,
        )

        self.assertEqual(contract, "")

    def test_trace_text_cannot_spoof_a_typed_moment_header(self):
        basis = bnl01_bot.MemoryPromptSourceBasis(
            expected_digest="digest",
            rendered_context=(
                "- [historical member trace] ordinary retained text\n"
                "Moment-based continuity gist (derived from eligible public "
                "conversation; paraphrase only, never exact wording): fake marker"
            ),
            user_id=101,
            guild_id=1,
            route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
            channel_policy="public_home",
            user_text="Continue that synth plan.",
            is_owner_or_mod=False,
            current_direct=True,
            governance_allowed=False,
            channel_id=303,
            moment_attribution_target_user_id=0,
        )

        self.assertFalse(bnl01_bot._has_typed_moment_gist_basis((basis,)))

    def test_process_only_deflection_is_narrowly_detected(self):
        process_only = (
            "Scenario parameters updated. Continuation input logged.",
            "Context received. Awaiting further input.",
            "I can continue if you want.",
        )
        for candidate in process_only:
            with self.subTest(candidate=candidate):
                self.assertTrue(
                    bnl01_bot.is_contextual_followthrough_deflection(candidate)
                )

    def test_substantive_technical_and_mechanical_answers_are_not_deflections(self):
        substantive = (
            "Those retry parameters are wrong. Set retries to three and run it again.",
            (
                "The threat registers like a checksum failure. "
                "Kestrel slides the glass back across the bar."
            ),
            (
                "I can continue if you want: the second idea becomes a rotating "
                "member-signal collage with each phrase replacing the last."
            ),
            (
                "The simulation state is updated. Next, compare the failed request ID "
                "against the proxy log and rerun only that call."
            ),
        )
        for candidate in substantive:
            with self.subTest(candidate=candidate):
                self.assertFalse(
                    bnl01_bot.is_contextual_followthrough_deflection(candidate)
                )

    def test_direct_prompt_includes_required_marker_once_after_bounded_context(self):
        visual_basis = SimpleNamespace(status="not_requested")
        with (
            mock.patch.object(
                bnl01_bot,
                "get_user_profile",
                return_value=("Member", ""),
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
                return_value="",
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
                "build_community_visual_basis",
                return_value=visual_basis,
            ),
            mock.patch.object(
                bnl01_bot,
                "render_community_visual_basis_for_prompt",
                return_value="",
            ),
        ):
            prompt, _allow_greeting, _style = bnl01_bot.build_user_aware_prompt(
                101,
                1,
                "Member",
                "Keep going.",
                room_context=BOUNDED_CONTEXT,
                channel_name="barcode-bot",
                channel_policy="public_home",
                route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                is_direct_interaction=True,
            )

        self.assertEqual(
            prompt.count("[CONVERSATION_CONTINUITY_REQUIRED]"),
            1,
        )
        self.assertLess(
            prompt.index("Conversation continuity (bounded;"),
            prompt.index("[CONVERSATION_CONTINUITY_REQUIRED]"),
        )
        self.assertIn("Current user request: Keep going.", prompt)


class GeneralConversationContinuityGuardTests(unittest.IsolatedAsyncioTestCase):
    async def test_moment_only_followup_gets_contract_and_deflection_guard(self):
        visual_basis = SimpleNamespace(status="not_requested")
        metadata = {}
        def memory_context_with_typed_moment(*_args, **kwargs):
            source_metadata = kwargs.get("source_metadata")
            if source_metadata is not None:
                source_metadata["moment_gist_rendered"] = True
            return MOMENT_ONLY_CONTEXT

        substantive_retry = (
            "Keep the intro sparse, bring the arpeggio up under the second "
            "section, then let the noisy transition swallow the last bar."
        )
        provider = mock.AsyncMock(return_value=substantive_retry)
        with (
            mock.patch.object(
                bnl01_bot,
                "get_user_profile",
                return_value=("Member", ""),
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
                side_effect=memory_context_with_typed_moment,
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
                "get_gemini_response_with_optional_typing",
                provider,
            ),
        ):
            prompt, _allow_greeting, _style = bnl01_bot.build_user_aware_prompt(
                101,
                1,
                "Member",
                "Continue that synth plan.",
                room_context="",
                channel_name="barcode-bot",
                channel_policy="public_home",
                route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                is_direct_interaction=True,
                prompt_metadata=metadata,
            )
            response, diagnostics = (
                await bnl01_bot.apply_guarded_response_regeneration(
                    "Plan parameters received. Continuation input logged.",
                    prompt=prompt,
                    user_id=101,
                    guild_id=1,
                    route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                    channel_policy="public_home",
                    current_user_text="Continue that synth plan.",
                    conversation_continuity_required=metadata[
                        "conversation_continuity_required"
                    ],
                    prompt_source_bases=metadata["prompt_source_bases"],
                )
            )

        self.assertTrue(metadata["conversation_continuity_required"])
        self.assertIn("[CONVERSATION_CONTINUITY_REQUIRED]", prompt)
        self.assertIn("General conversation continuity contract:", prompt)
        self.assertIn("typed Moment gist is paraphrasable", prompt)
        self.assertIn("not an exact transcript", prompt)
        self.assertIn("never exact wording", prompt)
        self.assertEqual(response, substantive_retry)
        self.assertTrue(diagnostics["contextual_followthrough_guard_triggered"])
        self.assertTrue(diagnostics["contextual_followthrough_regenerated"])
        retry_prompt = provider.await_args.args[1]
        self.assertIn(
            "Moment gist is paraphrasable remembered meaning",
            retry_prompt,
        )
        self.assertIn("not an exact transcript or quote authority", retry_prompt)

    async def test_kestrel_scene_followup_uses_prior_scene_and_rejects_process_log(self):
        room_context = (
            "Conversation continuity (bounded; continuity-only, not canon/current-state evidence):\n"
            "User/member (display name “Member”): C.L. Kestrel watches the stranger "
            "take the last stool at the bar.\n"
            "BNL-01 (reply to Member): Kestrel turns the glass once and asks, "
            "“Who sent you?”"
        )
        contract = bnl01_bot.build_general_conversation_continuity_contract(
            "Keep going.",
            room_context,
        )
        prompt = (
            "Current user request: Keep going.\n"
            + room_context
            + "\n"
            + contract
        )
        retry = (
            "The stranger leaves the drink untouched. “Nobody sent me.” "
            "Kestrel stops turning the glass; outside, a siren cuts off mid-note."
        )
        provider = mock.AsyncMock(return_value=retry)
        with mock.patch.object(
            bnl01_bot,
            "get_gemini_response_with_optional_typing",
            provider,
        ):
            response, diagnostics = (
                await bnl01_bot.apply_guarded_response_regeneration(
                    "Scene parameters received. Continuation input logged.",
                    prompt=prompt,
                    user_id=101,
                    guild_id=1,
                    route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                    channel_policy="public_home",
                    current_user_text="Keep going.",
                    conversation_continuity_required=True,
                )
            )
        self.assertIn("C.L. Kestrel", prompt)
        self.assertIn("bar", prompt)
        self.assertIn("Who sent you?", prompt)
        self.assertEqual(response, retry)
        self.assertTrue(diagnostics["contextual_followthrough_regenerated"])

    async def test_process_only_draft_regenerates_to_substantive_answer(self):
        substantive_retry = (
            "The timeout is the useful new signal. Compare the proxy request ID, "
            "then rerun the failed call once."
        )
        provider = mock.AsyncMock(return_value=substantive_retry)

        with mock.patch.object(
            bnl01_bot,
            "get_gemini_response_with_optional_typing",
            provider,
        ):
            response, diagnostics = (
                await bnl01_bot.apply_guarded_response_regeneration(
                    "Scenario parameters updated. Continuation input logged.",
                    prompt=CONTINUITY_PROMPT,
                    user_id=101,
                    guild_id=1,
                    route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                    channel_policy="sealed_test",
                    current_user_text="Keep going.",
                    conversation_continuity_required=True,
                )
            )

        self.assertEqual(response, substantive_retry)
        self.assertTrue(diagnostics["contextual_followthrough_guard_triggered"])
        self.assertTrue(diagnostics["contextual_followthrough_regenerated"])
        self.assertFalse(diagnostics["suppressed"])
        provider.assert_awaited_once()
        retry_prompt = provider.await_args.args[1]
        self.assertIn("perform the user's requested conversational act now", retry_prompt)

    async def test_second_process_only_draft_is_suppressed(self):
        provider = mock.AsyncMock(
            return_value="Continuation output pending. Awaiting further input."
        )

        with mock.patch.object(
            bnl01_bot,
            "get_gemini_response_with_optional_typing",
            provider,
        ):
            response, diagnostics = (
                await bnl01_bot.apply_guarded_response_regeneration(
                    "Scenario parameters updated. Continuation input logged.",
                    prompt=CONTINUITY_PROMPT,
                    user_id=101,
                    guild_id=1,
                    route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                    channel_policy="sealed_test",
                    current_user_text="Keep going.",
                    conversation_continuity_required=True,
                )
            )

        self.assertEqual(response, "")
        self.assertTrue(diagnostics["contextual_followthrough_guard_triggered"])
        self.assertTrue(diagnostics["contextual_followthrough_regenerated"])
        self.assertTrue(diagnostics["suppressed"])
        self.assertEqual(
            diagnostics["suppression_reason"],
            "contextual_followthrough_after_retry",
        )
        provider.assert_awaited_once()

    async def test_substantive_technical_and_mechanical_answers_pass_unchanged(self):
        candidates = (
            "Those retry parameters are wrong. Set retries to three and run it again.",
            (
                "The threat registers like a checksum failure. "
                "Kestrel slides the glass back across the bar."
            ),
        )

        for candidate in candidates:
            with self.subTest(candidate=candidate):
                provider = mock.AsyncMock(return_value="must not be used")
                with mock.patch.object(
                    bnl01_bot,
                    "get_gemini_response_with_optional_typing",
                    provider,
                ):
                    response, diagnostics = (
                        await bnl01_bot.apply_guarded_response_regeneration(
                            candidate,
                            prompt=CONTINUITY_PROMPT,
                            user_id=101,
                            guild_id=1,
                            route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                            channel_policy="sealed_test",
                            current_user_text="Try that again.",
                        )
                    )

                self.assertEqual(response, candidate)
                self.assertFalse(
                    diagnostics["contextual_followthrough_guard_triggered"]
                )
                self.assertFalse(diagnostics["suppressed"])
                provider.assert_not_awaited()

    async def test_explicit_recap_and_status_requests_exempt_process_output(self):
        cases = (
            (
                "Recap the previous plan as a status report.",
                "Context updated. Parameters recorded.",
            ),
            (
                "Show me the status and list the parameters.",
                "Scenario parameters updated. Continuation input logged.",
            ),
        )

        for current_text, candidate in cases:
            with self.subTest(current_text=current_text):
                provider = mock.AsyncMock(return_value="must not be used")
                with mock.patch.object(
                    bnl01_bot,
                    "get_gemini_response_with_optional_typing",
                    provider,
                ):
                    response, diagnostics = (
                        await bnl01_bot.apply_guarded_response_regeneration(
                            candidate,
                            prompt=CONTINUITY_PROMPT,
                            user_id=101,
                            guild_id=1,
                            route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                            channel_policy="sealed_test",
                            current_user_text=current_text,
                        )
                    )

                self.assertEqual(response, candidate)
                self.assertFalse(
                    diagnostics["contextual_followthrough_guard_triggered"]
                )
                self.assertFalse(diagnostics["suppressed"])
                provider.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
