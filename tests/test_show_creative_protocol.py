import os
import random
import re
import unittest
from types import SimpleNamespace
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl01_bot as bot
import bnl_creative_protocol as protocol


class ShowCreativeProtocolTests(unittest.IsolatedAsyncioTestCase):
    def test_provider_preserves_all_visible_parts_without_thoughts_or_other_candidates(self):
        # A first part ending at "Test Artist" must not discard the rest of
        # the song. Preserve byte adjacency for split words and JSON too.
        for pieces in (
            ("1. Lyrics\n[Verse]\nTest Artist", " B sings.\n", "2. Style\n1983: soul + dub."),
            ('{"response_', 'text":"A complete answer.","support":[]}'),
        ):
            parts = [SimpleNamespace(text="Private reasoning", thought=True)]
            parts += [SimpleNamespace(text=piece) for piece in pieces]
            parts += [SimpleNamespace(text=None)]
            response = SimpleNamespace(
                candidates=[
                    SimpleNamespace(content=SimpleNamespace(parts=parts), finish_reason="STOP"),
                    SimpleNamespace(content=SimpleNamespace(parts=[SimpleNamespace(text="Another candidate")])),
                ],
                usage_metadata=SimpleNamespace(total_token_count=123),
            )
            with self.assertLogs(level="INFO") as logs:
                result, tokens = bot._extract_text_and_tokens(response)
            self.assertEqual(result, "".join(pieces))
            self.assertEqual(tokens, 123)
            self.assertIn("finish_reason=STOP", " ".join(logs.output))
            self.assertNotIn("Private reasoning", " ".join(logs.output))
        self.assertEqual(bot._extract_text_and_tokens(None), ("", None))

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
        self.assertIn(protocol.SUNO_LYRIC_PROTOCOL, prompt)
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
            with mock.patch.object(bot.random, "random", return_value=0.0), \
                 mock.patch.object(bot, "_generate_gemini_content_with_fallback_async", side_effect=AssertionError("No decorative rewrite of lyrics")):
                result = await bot.get_gemini_response(request, 7, 1, allow_style_rewrite=True)
        self.assertEqual(result.strip(), answer.strip())
        provider.assert_awaited_once()
        prompt = provider.call_args.args[0]
        self.assertIn(request, prompt)
        self.assertIn("Honor an explicit era, lyric length or no-Style request", prompt)
        self.assertIn(protocol.SUNO_LYRIC_PROTOCOL, prompt)
        self.assertIn("Optional variation", prompt)
        hint = next(line for line in prompt.splitlines() if line.startswith("Optional variation"))
        self.assertNotIn("drawing from", hint)
        self.assertFalse(any(glyph in hint for glyph in protocol._GLYPHS))

    async def test_unlabeled_followup_with_formatted_lyrics_skips_automatic_rewrites(self):
        answer = "1. Lyrics\n[Verse]\nThe lantern carries our melody home.\n2. Style\n1982: bluegrass + industrial."
        provider = mock.AsyncMock(return_value=bot.GenerationResult(True, answer))
        with mock.patch.object(bot, "check_quota_availability", return_value=True), \
             mock.patch.object(bot, "conversation_context_v2_enabled", return_value=True), \
             mock.patch.object(bot, "_generate_gemini_content_result_async", provider), \
             mock.patch.object(bot.random, "random", return_value=0.0), \
             mock.patch.object(bot, "_generate_gemini_content_with_fallback_async", side_effect=AssertionError("No style call")):
            result = await bot.get_gemini_response("Make it less repetitive.", 7, 1)
        self.assertEqual(result, answer)
        provider.assert_awaited_once()
        # The existing prompt carries craft guidance even when feedback does
        # not repeat "song" or "Suno". There is no extra critic/provider call.
        self.assertIn(protocol.SUNO_LYRIC_PROTOCOL, provider.call_args.args[0])

    async def test_packet_envelope_remains_untouched(self):
        envelope = '{"response_text":"[Chorus]\\nA clean line.","support":[]}'
        provider = mock.AsyncMock(return_value=bot.GenerationResult(True, envelope))
        with mock.patch.object(bot, "check_quota_availability", return_value=True), \
             mock.patch.object(bot, "_generate_gemini_content_result_async", provider):
            result = await bot.get_gemini_response("Write a chorus.", 7, 1, route=bot.ORDINARY_CHAT_SINGLE_PACKET_ROUTE)
        self.assertEqual(result, envelope)
        provider.assert_awaited_once()


class SunoStyleLimitTests(unittest.TestCase):
    def test_long_style_preserves_lyrics_blend_and_separate_notes(self):
        lyrics = "1. Lyrics\n[Verse]\n" + "A specific original lyric line.\n" * 80
        blend = "1983: psychedelic soul + breakbeat. "
        style = blend + "Muted bass supports a close lead vocal. " * 40
        notes = "\n\n3. Exclude\nDistorted guitars.\n\nNotes\n" + "Long review note. " * 40
        text = lyrics + "\n2. Style\n" + style + notes
        result = protocol.bound_suno_style_copy(text)
        self.assertTrue(result.startswith(lyrics + "\n2. Style\n" + blend))
        self.assertTrue(result.endswith(notes))
        delivered = result.split("2. Style\n", 1)[1].split("\n\n3. Exclude", 1)[0]
        self.assertLessEqual(len(delivered), 500)
        self.assertTrue(delivered.endswith("."))
        self.assertEqual(protocol.bound_suno_style_copy(result), result)

    def test_style_only_markdown_and_fenced_field(self):
        body = "1978: cabaret + dub; " + "accordion answers the bass; " * 70
        for heading in ("Suno Style", "**Suno Style:**", "### Suno Style"):
            with self.subTest(heading=heading):
                text = heading + "\n```text\n" + body + "\n```"
                result = protocol.bound_suno_style_copy(text)
                self.assertTrue(result.startswith(heading + "\n```\n1978: cabaret + dub;"))
                self.assertTrue(result.endswith("\n```"))
                self.assertLessEqual(len(result.split("```\n", 1)[1].rsplit("\n```", 1)[0]), 500)

    def test_exact_limit_short_style_and_lyric_only_are_unchanged(self):
        for text in (
            "Suno Style\n" + "a" * 500,
            "1. Lyrics\n[Chorus]\nA line.\n2. Style\n1978: cabaret + dub.",
            "[Chorus]\n" + "A complete original lyric line.\n" * 80,
        ):
            with self.subTest(prefix=text[:30]):
                self.assertEqual(protocol.bound_suno_style_copy(text), text)

    def test_prose_quotes_and_non_song_style_headings_are_unchanged(self):
        large = "A detailed explanation. " * 80
        for text in (
            large,
            "2. Style\n" + large,
            "```\n1. Lyrics\nA quoted line.\n2. Style\n" + large + "\n```",
            "> Suno Style\n> " + large,
        ):
            with self.subTest(prefix=text[:30]):
                self.assertEqual(protocol.bound_suno_style_copy(text), text)

    def test_no_sentence_boundary_still_respects_ceiling_and_whole_words(self):
        core = "2000: cumbia + post-punk "
        text = "Suno Style\n" + core + "interlocking guitars " * 80
        result = protocol.bound_suno_style_copy(text).split("\n", 1)[1]
        self.assertTrue(result.startswith(core))
        self.assertLessEqual(len(result), 500)
        self.assertIn(result.split()[-1], ("interlocking", "guitars"))
        oversized_token = "Suno Style\n" + "a" * 800
        self.assertEqual(len(protocol.bound_suno_style_copy(oversized_token).split("\n", 1)[1]), 500)


if __name__ == "__main__":
    unittest.main()
