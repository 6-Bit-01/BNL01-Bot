"""Historical Relay wording cannot stand in for approved source evidence."""
import asyncio
import os
import unittest
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-token")
import bnl01_bot as bot


class RelaySourceValidationTests(unittest.TestCase):
    def test_lane_words_cannot_substitute_for_a_source(self):
        for text in ("residue", "echo", "afterimage", "archive", "still on", "recent"):
            with self.subTest(text=text):
                self.assertEqual(
                    bot._validate_relay_lane_adherence(text, "residual_echo", False, 1),
                    (False, "missing_residual_source"),
                )

    def test_empty_excluded_or_nonhistorical_selection_cannot_supply_residue(self):
        for decision in (
            bot.RelaySourceDecision("published_journal", "  "),
            bot.RelaySourceDecision("published_journal", "Publication context", skip_reason="excluded"),
            bot.RelaySourceDecision("canon", "General creative context"),
        ):
            with self.subTest(decision=decision):
                self.assertFalse(bot._relay_source_has_public_residue(decision))

    def test_approved_historical_source_classes_allow_natural_wording(self):
        for kind in ("conversation_continuity", "broadcast_memory", *bot.RELAY_SHARED_SOURCE_CLASSES):
            with self.subTest(kind=kind):
                decision = bot.RelaySourceDecision(kind, "Approved historical context")
                self.assertEqual(bot._validate_relay_lane_adherence(
                    "That earlier discussion left a question worth returning to.", "residual_echo", False, 1,
                    has_public_residue=bot._relay_source_has_public_residue(decision),
                ), (True, ""))

    def test_legacy_low_signal_path_carries_its_existing_source_decision(self):
        text = "That earlier discussion left a question worth returning to."
        with mock.patch.object(bot, "check_quota_availability", return_value=True), \
             mock.patch.object(bot, "_generate_gemini_content_with_fallback", return_value=object()) as generate, \
             mock.patch.object(bot, "_extract_text_and_tokens", return_value=(text, 1)), \
             mock.patch.object(bot, "_sanitize_low_signal_candidate", return_value=text), \
             mock.patch.object(bot, "_build_low_signal_fallback_message") as fallback:
            result = asyncio.run(bot.build_low_signal_relay_message(
                1, "public_context_weak", [], {"has_relay_context": True}, "residual_echo"))
        self.assertEqual(result, text)
        generate.assert_called_once()
        fallback.assert_not_called()


if __name__ == "__main__":
    unittest.main()
