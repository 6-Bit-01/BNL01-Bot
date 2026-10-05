"""Task authority reaches the real reply guard and its repair boundary."""

from dataclasses import replace
import unittest
from unittest import mock

import test_packet_recovery_style as generation_fixture
from bnl_conversation_context_v2 import assess_reply_referent_grounding
from bnl_unified_response_assessment import (
    build_situation_frame_v1,
    situation_request_texts,
    source_dependent_task_texts,
)


bot = generation_fixture.bot
SOURCE = "Idea A: a radio tower that wakes up at midnight."
COMPETITOR = "Idea B: a vending machine that trades memories for coins."
TRANSFORM = "BNL, improve this idea in one sentence."
INDEPENDENT = "How does a vending machine dispense snacks?"
WRONG = "The vending machine trades memories instead of coins after dark."
CORRECT = (
    "At midnight, the radio tower wakes and broadcasts one forgotten voice "
    "over the sleeping city."
)
SNACK_ANSWER = (
    "A vending machine turns a motor-driven spiral to drop the selected "
    "snack into the pickup tray."
)
MIXED = INDEPENDENT + " Also improve this idea in one sentence."
MIXED_CORRECT = (
    "A vending machine drops snacks with a motor-driven spiral. "
    "Give the tower a voice that whispers lost lullabies over the city."
)
MIXED_WRONG = (
    "A vending machine drops snacks with a motor-driven spiral. "
    "Improve it by letting the vending machine trade memories for coins."
)


def frame_for(text, **changes):
    values = dict(
        route_allowed=True,
        route_mode=bot.ROUTE_MODE_NORMAL_CHAT,
        conversation_surface="sealed_test",
        channel_policy="sealed_test",
        current_text=text,
        current_speaker_user_ids=(101,),
        current_speaker_labels=("Test Member",),
        reply_message_ids=(1001,),
        exact_source_row_ids=(10,),
        referent_status="resolved",
        response_act="answer",
    )
    values.update(changes)
    return build_situation_frame_v1(**values)


def reply_basis():
    source = bot.build_conversation_evidence_item(
        text=SOURCE, source_id=10, speaker_user_id=101,
        speaker_label="Test Member",
    )
    competitor = bot.build_conversation_evidence_item(
        text=COMPETITOR, source_id=11, speaker_user_id=101,
        speaker_label="Test Member",
    )
    return bot.ConversationPromptSourceBasis(
        expected_digest="fixture-stable",
        rendered_context=(
            "Conversation continuity:\nTest Member "
            "(exact Discord reply source): " + SOURCE
        ),
        guild_id=1, current_user_id=101, channel_id=303,
        channel_name="bnl-testing", channel_policy="sealed_test",
        referent_status="resolved", referent_reason="discord_reply_source",
        evidence_items=(source,),
        referent_source_evidence_items=(source,),
        referent_competing_evidence_items=(competitor,),
    )


class CurrentTaskGuardIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def guard(self, request, candidate, *, frame, repaired=CORRECT,
                    final_source_failure="", **changes):
        basis = reply_basis()
        prompt = "Current user request: " + request + "\n\n" + basis.rendered_context
        provider = mock.AsyncMock(return_value=repaired)
        values = dict(
            prompt=prompt, user_id=101, guild_id=1,
            route_mode=bot.ROUTE_MODE_NORMAL_CHAT, channel_policy="sealed_test",
            current_user_text=request, is_reply=True,
            source_context_available=True, prompt_source_bases=(basis,),
            situation_frame=frame,
        )
        values.update(changes)
        # Source lifecycle/delivery adapters are isolated; the complete guard,
        # bound task helper, lexical assessment and repair owner are real.
        with (
            mock.patch.object(bot, "get_gemini_response_with_optional_typing", provider),
            mock.patch.object(bot, "refresh_prompt_source_bases",
                              return_value=(prompt, (basis,), (), False)) as refresh,
            mock.patch.object(bot, "prompt_source_basis_failure",
                              return_value=final_source_failure) as final_check,
        ):
            result, diagnostics = await bot.apply_guarded_response_regeneration(
                candidate, **values,
            )
        refresh.assert_called_once()
        return result, diagnostics, provider, final_check

    def assert_unchanged(self, result, diagnostics, provider, expected):
        self.assertEqual(result, expected)
        self.assertFalse(diagnostics["suppressed"])
        self.assertFalse(diagnostics["exact_reply_grounding_guard_triggered"])
        self.assertFalse(diagnostics["exact_reply_grounding_regenerated"])
        provider.assert_not_awaited()

    async def test_complete_new_question_referencing_old_source_needs_no_source_repair(self):
        # Without task authority, the old source guard positively misreads the
        # correct independent vending answer as a switch from the radio idea.
        self.assertTrue(assess_reply_referent_grounding(
            SNACK_ANSWER, referent_texts=(SOURCE,), competing_texts=(COMPETITOR,),
        ).failed)
        frame = frame_for(INDEPENDENT)
        self.assertEqual(source_dependent_task_texts(frame, current_text=INDEPENDENT), ())
        result, diagnostics, provider, final_check = await self.guard(
            INDEPENDENT, SNACK_ANSWER, frame=frame,
        )
        self.assert_unchanged(result, diagnostics, provider, SNACK_ANSWER)
        self.assertEqual(diagnostics["exact_reply_grounding_status"], "not_applicable")
        final_check.assert_called_once()

    async def test_complete_aspiration_question_does_not_have_to_answer_old_radio_idea(self):
        request = "What do you want to be when you grow up?"
        answer = "A vending machine for improbable jokes: press a button, receive a terrible pun."
        result, diagnostics, provider, _ = await self.guard(
            request, answer, frame=frame_for(request),
        )
        self.assert_unchanged(result, diagnostics, provider, answer)

    async def test_wrong_source_transform_uses_existing_repair_once(self):
        result, diagnostics, provider, final_check = await self.guard(
            TRANSFORM, WRONG, frame=frame_for(TRANSFORM),
            generation_route=bot.ORDINARY_CHAT_SINGLE_PACKET_ROUTE,
        )
        self.assertEqual(result, CORRECT)
        self.assertTrue(diagnostics["exact_reply_grounding_guard_triggered"])
        self.assertTrue(diagnostics["exact_reply_grounding_regenerated"])
        self.assertFalse(diagnostics["suppressed"])
        provider.assert_awaited_once()
        repair_prompt = provider.await_args.args[1]
        self.assertIn("EXACT-REPLY GROUNDING CORRECTION REQUIRED", repair_prompt)
        self.assertIn(TRANSFORM, repair_prompt)
        self.assertIn("radio tower", repair_prompt)
        self.assertNotIn("vending machine", repair_prompt)
        self.assertEqual(provider.await_args.kwargs["route"], bot.ORDINARY_CHAT_SINGLE_PACKET_ROUTE)
        self.assertFalse(provider.await_args.kwargs["allow_style_rewrite"])
        final_check.assert_called_once()

    async def test_correct_source_transform_passes_without_regeneration(self):
        result, diagnostics, provider, _ = await self.guard(
            TRANSFORM, CORRECT, frame=frame_for(TRANSFORM),
        )
        self.assert_unchanged(result, diagnostics, provider, CORRECT)
        self.assertEqual(diagnostics["exact_reply_grounding_status"], "grounded_exact_reply_source")

    async def test_repair_that_still_substitutes_competitor_remains_suppressed(self):
        result, diagnostics, provider, _ = await self.guard(
            TRANSFORM, WRONG, frame=frame_for(TRANSFORM), repaired=WRONG,
        )
        self.assertEqual(result, "")
        self.assertTrue(diagnostics["suppressed"])
        self.assertEqual(diagnostics["suppression_reason"], "exact_reply_grounding_after_retry")
        provider.assert_awaited_once()

    async def test_mixed_correct_answer_keeps_requested_competitor_words(self):
        frame = frame_for(MIXED)
        self.assertEqual(len(frame.tasks), 2)
        self.assertEqual(source_dependent_task_texts(frame, current_text=MIXED),
                         ("improve this idea in one sentence",))
        self.assertTrue(assess_reply_referent_grounding(
            MIXED_CORRECT, referent_texts=(SOURCE,), competing_texts=(COMPETITOR,),
        ).failed)
        result, diagnostics, provider, _ = await self.guard(
            MIXED, MIXED_CORRECT, frame=frame,
        )
        self.assert_unchanged(result, diagnostics, provider, MIXED_CORRECT)
        self.assertEqual(diagnostics["exact_reply_competing_term_hits"], 0)

    async def test_mixed_wrong_transformation_is_repaired_despite_allowed_independent_words(self):
        result, diagnostics, provider, _ = await self.guard(
            MIXED, MIXED_WRONG, frame=frame_for(MIXED), repaired=MIXED_CORRECT,
        )
        self.assertEqual(result, MIXED_CORRECT)
        self.assertTrue(diagnostics["exact_reply_grounding_guard_triggered"])
        self.assertTrue(diagnostics["exact_reply_grounding_regenerated"])
        self.assertFalse(diagnostics["suppressed"])
        provider.assert_awaited_once()
        self.assertIn(INDEPENDENT, provider.await_args.args[1])
        self.assertIn("radio tower", provider.await_args.args[1])

    async def test_missing_and_changed_frames_retain_source_checking(self):
        for frame in (None, frame_for("What do you want to be when you grow up?"),
                      replace(frame_for(INDEPENDENT), tasks=())):
            with self.subTest(frame=frame):
                result, diagnostics, provider, _ = await self.guard(
                    INDEPENDENT, SNACK_ANSWER, frame=frame, regeneration_allowed=False,
                )
                self.assertEqual(result, "")
                self.assertTrue(diagnostics["exact_reply_grounding_guard_triggered"])
                self.assertEqual(diagnostics["suppression_reason"],
                                 "exact_reply_grounding_validation_only")
                provider.assert_not_awaited()

    async def test_stale_route_and_policy_frame_cannot_authorize_exemption(self):
        for changes in (dict(route_mode="show_status"),
                        dict(channel_policy="public_context"),
                        dict(route_allowed=False)):
            with self.subTest(changes=changes):
                result, diagnostics, provider, _ = await self.guard(
                    INDEPENDENT, SNACK_ANSWER, frame=frame_for(INDEPENDENT, **changes),
                    regeneration_allowed=False,
                )
                self.assertEqual(result, "")
                self.assertTrue(diagnostics["exact_reply_grounding_guard_triggered"])
                provider.assert_not_awaited()

    async def test_incidental_setup_cannot_exempt_competing_source_terms(self):
        request = "Someone mentioned a vending machine that trades memories for coins; improve this idea."
        frame = frame_for(request)
        self.assertEqual(situation_request_texts(frame, current_text=request), ("improve this idea",))
        result, diagnostics, provider, _ = await self.guard(
            request, WRONG, frame=frame, regeneration_allowed=False,
        )
        self.assertEqual(result, "")
        self.assertTrue(diagnostics["exact_reply_grounding_guard_triggered"])
        provider.assert_not_awaited()

    async def test_task_exemption_preserves_final_source_freshness_check(self):
        result, diagnostics, provider, final_check = await self.guard(
            INDEPENDENT, SNACK_ANSWER, frame=frame_for(INDEPENDENT),
            final_source_failure="conversation_source_changed",
        )
        self.assertEqual(result, "")
        self.assertTrue(diagnostics["suppressed"])
        self.assertEqual(diagnostics["suppression_reason"], "conversation_source_changed_before_send")
        self.assertFalse(diagnostics["exact_reply_grounding_guard_triggered"])
        provider.assert_not_awaited()
        final_check.assert_called_once()


if __name__ == "__main__":
    unittest.main()
