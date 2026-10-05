"""A multiword correction never silently counts only its first word."""
import unittest

from bnl_tiktok_live_context import (
    _tiktok_word_frequency_target_inert_query,
    count_tiktok_show_word_frequency,
    requested_tiktok_show_word_count,
)


class CorrectionLiteralTests(unittest.TestCase):
    def test_unquoted_multiword_correction_stays_unsupported(self):
        for correction in ("I meant red panda", "not panda, red panda",
                           "I meant red panda please", "I meant red panda in the whole stream"):
            query = "How many times did chat say panda?\nCurrent follow-up: " + correction
            with self.subTest(correction=correction):
                self.assertEqual(requested_tiktok_show_word_count(query), "red panda")
                result = count_tiktok_show_word_frequency({}, [], query)
                self.assertEqual(result["reason"], "unsupported_word_target")
                self.assertIsNone(result["occurrenceCount"])

    def test_single_word_correction_preserves_separate_episode_scope(self):
        for correction in ("I meant goat during the October 2, 2026 stream",
                           "not panda, goat in the whole stream", "I meant goat, please",
                           "I meant goat today", "I meant goat tonight", "I meant goat please",
                           "I meant goat this stream"):
            query = "How many times did chat say panda?\nCurrent follow-up: " + correction
            with self.subTest(correction=correction):
                self.assertEqual(requested_tiktok_show_word_count(query), "goat")

    def test_quoted_phrase_preserves_existing_quote_forms(self):
        for literal in ('"red panda"', '“red panda”', "'red panda'", "‘red panda’"):
            query = 'Count panda in the stream\nCurrent follow-up: I meant ' + literal
            with self.subTest(literal=literal):
                self.assertEqual(requested_tiktok_show_word_count(query), "red panda")
                self.assertEqual(count_tiktok_show_word_frequency({}, [], query)["reason"],
                                 "unsupported_word_target")

    def test_standalone_or_pronominal_correction_cannot_create_a_new_target(self):
        self.assertEqual(requested_tiktok_show_word_count("I meant red panda"), "")
        query = "Count panda in the stream\nCurrent follow-up: I meant that"
        self.assertEqual(requested_tiktok_show_word_count(query), "panda")

    def test_phrase_target_is_inert_but_explicit_episode_date_remains(self):
        query = ("Count panda in the stream\nCurrent follow-up: "
                 "I meant current show during the October 2, 2026 stream")
        self.assertEqual(requested_tiktok_show_word_count(query), "current show")
        inert = _tiktok_word_frequency_target_inert_query(query)
        self.assertNotIn("current show", inert)
        self.assertIn("during the October 2, 2026 stream", inert)
