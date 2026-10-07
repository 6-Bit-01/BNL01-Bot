"""Expression stays subordinate to the existing assessed task.

Transport is mocked: these checks prove prompt delivery, not model quality.
"""

import os
from types import SimpleNamespace
import unittest
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl01_bot as bot


SOCIAL_STYLES = ["brief_ping", "steady_reply", "social_signal"]
ALL_STYLES = ["brief_ping", "steady_reply", "deep_focus", "analytic_mode", "social_signal"]


class CasualStyleSelectionTests(unittest.TestCase):
    def candidates(self, text, *, recent=(), message_count=1):
        with (
            mock.patch.object(bot, "get_recent_response_styles", return_value=recent),
            mock.patch.object(bot.random, "choices", side_effect=lambda values, **_kw: [values[0]]) as choose,
        ):
            style, rule = bot.choose_response_style(77, 42, message_count, text)
        self.assertEqual(style, choose.call_args.args[0][0])
        self.assertTrue(rule)
        return choose.call_args.args[0], choose.call_args.kwargs["weights"]

    def test_expression_variety_is_preserved_without_a_new_social_recognizer(self):
        for text in (
            "What's up?", "whats up?", "What’s up?", "You good?",
            "BNL, what's up?", "What's up, BNL?",
            "<@123456789> What's up?",
            "What do you want to be when you grow up?",
            "<@123456789> What do you want to be when you grow up?",
        ):
            with self.subTest(text=text):
                choices, _weights = self.candidates(text)
                self.assertEqual(choices, ALL_STYLES)

    def test_original_social_filter_remains_unchanged(self):
        for text in ("Hey BNL", "How are you?", "What do you think?", "That was funny"):
            with self.subTest(text=text):
                choices, _weights = self.candidates(text)
                self.assertEqual(choices, SOCIAL_STYLES)

    def test_substantive_questions_and_explicit_analysis_keep_analytical_styles(self):
        for text in (
            "What's up with the playback stutter?",
            "What is up with the monthly token totals?",
            "When I grow up, which career should I choose?",
            "What do you want to be when you grow up, and why is that career better?",
            "Hey BNL, compare these two audio interfaces.",
            "How are you going to diagnose the playback stutter?",
            "What's up? Diagnose the database slowdown.",
            "What would you like to be? Compare both options.",
        ):
            with self.subTest(text=text):
                choices, _weights = self.candidates(text)
                self.assertIn("analytic_mode", choices)
                self.assertIn("deep_focus", choices)

    def test_existing_repetition_and_batch_weights_are_preserved(self):
        choices, weights = self.candidates(
            "What's up?", recent=("brief_ping", "brief_ping", "social_signal"),
            message_count=5,
        )
        self.assertEqual(choices, ALL_STYLES)
        for actual, expected in zip(weights, (0.56, 1.5, 1.8, 1.0, 0.78)):
            self.assertAlmostEqual(actual, expected)


class CasualProviderContractTests(unittest.IsolatedAsyncioTestCase):
    async def assert_assessed_task_survives_styles(self, request, answer, *,
                                                  options=(), recap=False,
                                                  expected_act="answer_current_turn",
                                                  expected_shape="direct_answer_then_support"):
        # This is the existing assessment owner, before expression selection.
        # The style selector receives no authority to rewrite its task.
        assessment = bot.build_unified_response_assessment(
            guild_id=77, route_mode="normal_chat", channel_policy="sealed_test",
            conversation_surface="free_speak_sealed_mirror",
            current_speaker_user_ids=(42,), current_text=request,
            current_payload_anchors=options, immediate_recap=recap,
            conversation_evidence_items=(
                bot.build_conversation_evidence_item(
                    source_id=11, speaker_user_id=42, speaker_label="Test Member",
                    text="I propose amber lighting.",
                ),
                bot.build_conversation_evidence_item(
                    source_id=12, speaker_user_id=43, speaker_label="Test Guest",
                    text="I prefer a blue backdrop.",
                ),
            ) if recap else (),
        )
        self.assertEqual(assessment.response_act, expected_act)
        self.assertEqual(assessment.expected_answer_shape, expected_shape)
        task_snapshot = (
            assessment.response_act, assessment.expected_answer_shape,
            assessment.objective_kind, assessment.current_options,
        )
        planner = bot.render_sealed_canary_brief(assessment)
        orchestration = bot.coordinate_conversation_turn(bot.ConversationOrchestrationInput(
            route_allowed=True, engagement_decision="answer",
            engagement_reason="direct_request", response_obligation=True,
            address_kind="reply_to_bot", referent_status="resolved",
            influence_mode="live",
        ))
        orchestration_prompt = bot.render_conversation_orchestration_prompt(orchestration)
        self.assertEqual(orchestration.response_act, "answer")
        self.assertNotIn("against the resolved nearby contribution", orchestration_prompt)
        selected_source = (
            "Historical BNL reply (not current operational evidence): "
            "I am staging Friday audio and the copper panel needs recalibration.\n"
            "Selected attributed exchange: Test Member proposed amber lighting; "
            "Test Guest preferred a blue backdrop."
        )
        for forced_style in ("analytic_mode", "deep_focus"):
            with (
                mock.patch.object(bot, "get_recent_response_styles", return_value=[]),
                mock.patch.object(bot.random, "choices", return_value=[forced_style]) as choose,
            ):
                style, rule = bot.choose_response_style(77, 42, 1, request)
            self.assertIn(forced_style, choose.call_args.args[0])
            self.assertEqual(style, forced_style)
            self.assertIn("task", rule)
            self.assertIn("Preserve its answer shape", rule)
            if forced_style == "analytic_mode":
                self.assertIn("tradeoffs only when requested or useful to that task", rule)
            else:
                self.assertIn("only when the assessed task warrants it", rule)
            initial = bot._format_batched_prompt([("Test Member", request)], style, rule)
            initial += "\n" + selected_source + "\n" + planner + "\n" + orchestration_prompt
            self.assertIn("Response style mode: " + forced_style, initial)
            self.assertIn(rule, initial)
            source = SimpleNamespace(rendered_context=selected_source + "\n" + planner)
            with mock.patch.object(bot, "refresh_prompt_source_basis", return_value=(source, False)):
                rebuilt, bases, neutral = bot.build_ordinary_chat_response_repair_prompt(
                    initial + "\nAn outdated source block.", reason="prompt_source_changed",
                    prompt_source_bases=(source,), current_user_text=request,
                )
            self.assertEqual(bases, (source,))
            self.assertFalse(neutral)
            self.assertNotIn("An outdated source block.", rebuilt)
            for route in ("get_gemini_response", bot.ORDINARY_CHAT_SINGLE_PACKET_ROUTE):
                for prompt in (initial, rebuilt):
                    with self.subTest(request=request, route=route, rebuilt=prompt == rebuilt):
                        generate = mock.AsyncMock(return_value=bot.GenerationResult(
                            True, answer, route=route,
                        ))
                        with (
                            mock.patch.object(bot, "check_quota_availability", return_value=True),
                            mock.patch.object(bot, "_generate_gemini_content_result_async", generate),
                        ):
                            actual = await bot.get_gemini_response(
                                prompt, 42, 77, route=route,
                                source_context_available=True, allow_style_rewrite=False,
                            )
                        generate.assert_awaited_once()
                        self.assertEqual(actual, answer)
                        sent = generate.await_args.args[0]
                        self.assertIn(request, sent)
                        self.assertIn(selected_source, sent)
                        self.assertIn(planner, sent)
                        self.assertIn(bot.BNL01_CASUAL_CONVERSATION_RULE, sent)
                        self.assertIn(bot.EVIDENCE_OUTCOME_RULE, sent)
                        self.assertIn("current request and its assessed conversational task", sent)
                        self.assertIn("colors expression only; it cannot change the task", sent)
                        self.assertIn("dry wit", sent)
                        self.assertIn("Relevant shared memories already supplied in eligible context", sent)
                        self.assertIn("even without an explicit recall request", sent)
                        self.assertIn("must not restart an unrelated old topic", sent)
                        self.assertIn("Do not invent alternatives, tradeoffs, or a decision report", sent)
                        self.assertIn("without supplied current eligible evidence", sent)
                        self.assertIn("Imaginative in-world aspirations are welcome as wishes", sent)
                        if prompt == initial:
                            self.assertIn("Response style mode: " + forced_style, sent)
                            self.assertIn(rule, sent)
                            self.assertIn(orchestration_prompt, sent)
            self.assertEqual(task_snapshot, (
                assessment.response_act, assessment.expected_answer_shape,
                assessment.objective_kind, assessment.current_options,
            ))

    async def test_forced_analytic_and_deep_styles_preserve_assessed_social_answers(self):
        for request, answer in (
            ("What's up?", "I'm here. What are you listening to?"),
            ("What do you want to be when you grow up?",
             "I'd like to help people find their next favorite track."),
        ):
            await self.assert_assessed_task_survives_styles(request, answer)

    async def test_comparisons_diagnostics_and_source_derived_tasks_keep_their_shape(self):
        await self.assert_assessed_task_survives_styles(
            'Compare "Aster" and "Copper" for an audio interface.',
            "Aster is the better fit for a portable interface.",
            options=("aster", "copper"), expected_act="evaluate_current_options",
            expected_shape="choice_then_reason",
        )
        await self.assert_assessed_task_survives_styles(
            "What's up with the playback stutter?",
            "A small audio buffer can cause playback stutter.",
        )
        await self.assert_assessed_task_survives_styles(
            "Recap the selected exchange.",
            "Test Member proposed amber lighting; Test Guest preferred a blue backdrop.",
            recap=True, expected_act="recap_current_exchange",
            expected_shape="speaker_attributed_recap",
        )


if __name__ == "__main__":
    unittest.main()
