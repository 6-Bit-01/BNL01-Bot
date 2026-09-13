import os
import random
import re
import unittest
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl01_bot as bot
import bnl_creative_protocol as protocol


class ShowCreativeProtocolTests(unittest.IsolatedAsyncioTestCase):
    def test_submitted_credits_win_over_filename_and_uploader_metadata(self):
        track = {
            "submittedArtistName": "Test Creator", "submittedSongTitle": "Original Song",
            "detectedArtistName": "Upload Account", "detectedSongTitle": "Filename Song",
            "providerTitle": "Wrong Credit", "lane": "wheel", "sourceType": "upload",
        }
        self.assertEqual(bot._track_label(track), "Test Creator — Original Song (wheel)")
        self.assertEqual(bot._track_label({"artist": "Legacy Artist", "title": "Legacy Song", "detectedArtistName": "Uploader"}), "Legacy Artist — Legacy Song")
        self.assertEqual(bot._track_label({"title": "Song", "detectedArtistName": "Uploader"}), "Unknown artist — Song")
        self.assertEqual(bot._track_label({"detectedArtistName": "Uploader", "providerTitle": "Filename"}), "")
        self.assertEqual(bot._track_label({}), "")
        self.assertEqual(track["detectedArtistName"], "Upload Account")

    def test_variation_changes_form_and_stays_inside_the_requested_style_bounds(self):
        rng = random.Random(611)
        hints = [protocol.creative_variation_hint(rng) for _ in range(100)]
        self.assertEqual(len(set(hints)), 100)
        previous_form = None
        for hint in hints:
            form = re.search(r"try (.*?), drawing", hint).group(1)
            self.assertNotEqual(form, previous_form)
            previous_form = form
            year, genres = re.search(r"consider (\d{4}): (.*?)\. The user's", hint).groups()
            self.assertLessEqual(1970, int(year))
            self.assertLessEqual(int(year), 2010)
            self.assertIn(len(genres.split(" + ")), (2, 3, 4))
            self.assertNotIn("hip hop", genres)
            self.assertIn("take precedence", hint)
        self.assertLessEqual(len(protocol._recent_glyphs), 64)
        self.assertLessEqual(len(protocol._recent_styles), 64)

    async def test_active_packet_receives_defaults_and_returns_full_lyrics_in_one_call(self):
        lines = [f"The lantern carries our melody through corridor {i}." for i in range(36)]
        lyrics = "[Verse 1]\n" + "\n".join(lines[:12]) + "\n[Chorus]\n" + "\n".join(lines[12:24]) + "\n[Bridge]\n" + "\n".join(lines[24:])
        self.assertGreaterEqual(len(lyrics), 1400)
        answer = "1. Lyrics\n" + lyrics + "\n\n2. Style\n1982: bluegrass + industrial + space ambient."
        provider = mock.AsyncMock(return_value=bot.GenerationResult(True, answer))
        with mock.patch.object(bot, "check_quota_availability", return_value=True), \
             mock.patch.object(bot, "_generate_gemini_content_result_async", provider), \
             mock.patch.object(bot, "get_conversation_history", side_effect=AssertionError("No extra history read")):
            result = await bot.get_gemini_response("Write the end-of-show song for Suno from the supplied show context.", 7, 1, route=bot.ORDINARY_CHAT_SINGLE_PACKET_ROUTE)
        self.assertEqual(result, answer)
        provider.assert_awaited_once()
        prompt = provider.call_args.args[0]
        self.assertIn("1,400 characters", prompt)
        self.assertIn("2–4 contrasting genres", prompt)
        self.assertIn("1970–2010", prompt)
        self.assertIn("hip hop is not the default", prompt)
        self.assertIn("'1. Lyrics'", prompt)
        self.assertIn("'2. Style'", prompt)
        self.assertIn("Never perform a glitch", prompt)
        self.assertIn("approved feedback", prompt)
        self.assertIn("Optional variation", prompt)
        chunks = bot.split_message(result)
        self.assertGreater(len(chunks), 1)
        self.assertTrue(all(len(chunk) <= 1900 for chunk in chunks))
        self.assertEqual(" ".join(" ".join(chunks).split()), " ".join(answer.split()))

    async def test_explicit_override_and_normal_route_get_same_protocol_without_a_rewrite_call(self):
        request = "Write an eight-line hip hop chorus set in 2020. No Style section."
        answer = "[Chorus]\n" + "The station lights are coming home.\n" * 8
        provider = mock.AsyncMock(return_value=bot.GenerationResult(True, answer))
        with mock.patch.object(bot, "check_quota_availability", return_value=True), \
             mock.patch.object(bot, "conversation_context_v2_enabled", return_value=True), \
             mock.patch.object(bot, "_generate_gemini_content_result_async", provider):
            result = await bot.get_gemini_response(request, 7, 1, allow_style_rewrite=False)
        self.assertEqual(result.strip(), answer.strip())
        provider.assert_awaited_once()
        prompt = provider.call_args.args[0]
        self.assertIn(request, prompt)
        self.assertIn("explicit user genre, era, format or length override", prompt)
        self.assertIn("Optional variation", prompt)


if __name__ == "__main__":
    unittest.main()
