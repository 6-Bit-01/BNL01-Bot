import os
import unittest
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl01_bot as bot


class ShowGuardOwnerScopeTests(unittest.IsolatedAsyncioTestCase):
    async def test_publication_and_timeline_answer_keeps_publication_clock_and_lore(self):
        request = "What did the Journal say, and give me a timeline of last night's show?"
        prompt = (
            "Current user request: " + request + "\n"
            "Published Journal: Entries release at 7:00 PM; Cliff discussed the queue test.\n"
            "Durable BARCODE Radio show episode memory:\n"
            "- t+1.2m [track play started] Neon Fox — First Signal\n"
            "Finalized BARCODE Radio episode priority:\n"
            "- Use this for recorded show claims only.\n"
        )
        answer = (
            "The Journal releases at 7:00 PM and covered Cliff's queue test. "
            "The show timeline begins at t+1.2m with Neon Fox's First Signal."
        )
        self.assertTrue(bot.finalized_show_packet_owner_requested(request, prompt))
        with mock.patch.object(
            bot, "get_gemini_response_with_optional_typing", new=mock.AsyncMock()
        ) as regenerate:
            result, diagnostics = await bot.apply_guarded_response_regeneration(
                answer, prompt=prompt, current_user_text=request,
                user_id=101, guild_id=1, route_mode=bot.ROUTE_MODE_NORMAL_CHAT,
                channel_policy="public_home", source_context_available=True,
            )
        self.assertEqual(result, answer)
        self.assertFalse(diagnostics["tiktok_show_episode_guard_triggered"])
        self.assertFalse(diagnostics["suppressed"])
        regenerate.assert_not_awaited()

    async def test_dedicated_tiktok_topics_use_evidence_without_a_lexical_retry(self):
        request = "What recurring topics came from TikTok chat during the show?"
        prompt = (
            "Current user request: " + request + "\n"
            "Durable TikTok show analysis context:\n"
            "- Analysis intent=chat_topics.\n"
            '- Signal "green visuals": 3 messages / 3 unique chatters.\n'
        )
        answer = "Three different viewers commented on the green look, one remark apiece."
        with mock.patch.object(
            bot, "get_gemini_response_with_optional_typing",
            new=mock.AsyncMock(),
        ) as regenerate:
            result, diagnostics = await bot.apply_guarded_response_regeneration(
                answer, prompt=prompt, current_user_text=request,
                user_id=101, guild_id=1, route_mode=bot.ROUTE_MODE_NORMAL_CHAT,
                channel_policy="public_home", source_context_available=True,
            )
        self.assertEqual(result, answer)
        self.assertFalse(diagnostics["tiktok_show_analysis_regenerated"])
        self.assertFalse(diagnostics["suppressed"])
        regenerate.assert_not_awaited()

    async def test_packet_generated_answer_retains_typed_packet_validation_owner(self):
        request = "Give me a timeline of last night's show."
        prompt = (
            "Current user request: " + request + "\n"
            "Durable BARCODE Radio show episode memory:\n"
            "- t+1.2m [track play started] Neon Fox — First Signal\n"
            "Finalized BARCODE Radio episode priority:\n"
            "- Use this for recorded show claims only.\n"
        )
        answer = "First Signal opened the show."
        with mock.patch.object(
            bot, "get_gemini_response_with_optional_typing", new=mock.AsyncMock()
        ) as regenerate:
            result, diagnostics = await bot.apply_guarded_response_regeneration(
                answer, prompt=prompt, current_user_text=request,
                user_id=101, guild_id=1, route_mode=bot.ROUTE_MODE_NORMAL_CHAT,
                channel_policy="public_home", source_context_available=True,
                generation_route=bot.ORDINARY_CHAT_SINGLE_PACKET_ROUTE,
            )
        self.assertEqual(result, answer)
        self.assertFalse(diagnostics["suppressed"])
        regenerate.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
