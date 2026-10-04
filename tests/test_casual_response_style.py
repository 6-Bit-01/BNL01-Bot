"""Casual style selection and its actual provider-bound prompt contract.

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

    def test_standalone_check_ins_exclude_analytical_styles(self):
        for text in (
            "What's up?", "whats up", "What is up?", "What’s up?",
            "What's   up?!", "sup?", "You good?", "How's it going?",
            "How’s things?", "How are things?", "How have you been?",
            "How are you doing?", "How are you feeling?",
            "@BNL-01, What's up?", "BNL, whats up?", "What's up, BNL?",
            "<@123456789> What's up?", "<@!123456789> How's it going?",
        ):
            with self.subTest(text=text):
                choices, _weights = self.candidates(text)
                self.assertEqual(choices, SOCIAL_STYLES)

    def test_standalone_bnl_aspiration_excludes_analytical_styles(self):
        for text in (
            "What do you want to be when you grow up?",
            "What would you like to be when you grow up?",
            "When you grow up, what do you want to be?",
            "What do you want to be?",
            "BNL-01, what would you like to be?",
            "<@123456789> What do you want to be when you grow up?",
        ):
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

    def test_social_filter_preserves_existing_repetition_and_batch_weights(self):
        choices, weights = self.candidates(
            "What's up?", recent=("brief_ping", "brief_ping", "social_signal"),
            message_count=5,
        )
        self.assertEqual(choices, SOCIAL_STYLES)
        for actual, expected in zip(weights, (0.56, 1.5, 0.78)):
            self.assertAlmostEqual(actual, expected)


class CasualProviderContractTests(unittest.IsolatedAsyncioTestCase):
    async def test_initial_and_rebuilt_normal_and_packet_prompts_keep_casual_grounding(self):
        old_source = (
            "Historical BNL reply (not current operational evidence): "
            "I am staging Friday audio and the copper panel needs recalibration."
        )
        for request, answer in (
            ("What's up?", "I'm here. What are you listening to?"),
            ("What do you want to be when you grow up?",
             "I'd like to help people find their next favorite track."),
        ):
            with (
                mock.patch.object(bot, "get_recent_response_styles", return_value=[]),
                mock.patch.object(bot.random, "choices", side_effect=lambda values, **_kw: [values[0]]),
            ):
                style, rule = bot.choose_response_style(77, 42, 1, request)
            initial = bot._format_batched_prompt([("Test Member", request)], style, rule)
            initial += "\n" + old_source
            self.assertIn("Response style mode: brief_ping", initial)
            self.assertIn(rule, initial)
            source = SimpleNamespace(rendered_context=old_source)
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
                        self.assertIn(old_source, sent)
                        self.assertIn(bot.BNL01_CASUAL_CONVERSATION_RULE, sent)
                        self.assertIn(bot.EVIDENCE_OUTCOME_RULE, sent)
                        self.assertIn("dry wit", sent)
                        self.assertIn("Do not introduce older archived details into simple greetings", sent)
                        self.assertIn("Do not invent alternatives, tradeoffs, or a decision report", sent)
                        self.assertIn("without supplied current eligible evidence", sent)
                        self.assertIn("Imaginative in-world aspirations are welcome as wishes", sent)
                        if prompt == initial:
                            self.assertIn("Response style mode: brief_ping", sent)


if __name__ == "__main__":
    unittest.main()
