"""Provider-bound grounding instructions; these are not model-quality tests."""

import os
import unittest
from datetime import datetime
from types import SimpleNamespace
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl01_bot as bot
from bnl_tiktok_live_context import build_durable_show_prompt_context


def show_context(events_available=True):
    def stamp(value):
        return int(datetime.fromisoformat(value).timestamp() * 1000)

    show = {
        "sessionId": "test-quote-show",
        "title": "Test Broadcast",
        "showDate": "2026-08-28",
        "status": "archived",
        "milestones": [
            {"sequence": 1, "eventType": "broadcast_started",
             "occurredAt": "2026-08-29T00:00:00+00:00"},
            {"sequence": 2, "eventType": "session_archived",
             "occurredAt": "2026-08-29T00:10:00+00:00"},
        ],
    }
    events = [
        {
            "event_id": "test-event-a",
            "occurred_at_ms": stamp("2026-08-29T00:02:00+00:00"),
            "subject_ref": "tiktok_handle:test.member",
            "private_display_name": "Test Member",
            "raw_text": "The green lights changed.",
            "metadata": {"eventType": "comment", "handle": "test.member"},
        },
        {
            "event_id": "test-event-b",
            "occurred_at_ms": stamp("2026-08-29T00:03:00+00:00"),
            "subject_ref": "tiktok_handle:test.guest",
            "private_display_name": "Test Guest",
            "raw_text": "The green lights are bright.",
            "metadata": {"eventType": "comment", "handle": "test.guest"},
        },
    ]
    return build_durable_show_prompt_context(
        {"latestShow": show, "shows": []},
        events if events_available else None,
        "What did people discuss in the last show?",
    )


class ShowQuoteProviderContractTests(unittest.IsolatedAsyncioTestCase):
    async def test_authored_pairs_and_quote_contract_reach_both_provider_routes(self):
        context = show_context()
        batch = bot._format_batched_prompt(
            [("Test Member", "Give me some quotes.")], "balanced", "",
        )
        prompt = (
            context + "\n"
            + bot.build_tiktok_show_analysis_turn_contract(context)
            + batch
        )
        authored_pairs = (
            'Test Member (@test.member): "The green lights changed."',
            'Test Guest (@test.guest): "The green lights are bright."',
        )
        for route in ("get_gemini_response", bot.ORDINARY_CHAT_SINGLE_PACKET_ROUTE):
            with self.subTest(route=route):
                generate = mock.AsyncMock(
                    return_value=bot.GenerationResult(True, "A supported answer.", route=route)
                )
                with (
                    mock.patch.object(bot, "check_quota_availability", return_value=True),
                    mock.patch.object(bot, "conversation_context_v2_enabled", return_value=True),
                    mock.patch.object(bot, "_generate_gemini_content_result_async", generate),
                ):
                    await bot.get_gemini_response(
                        prompt, 101, 1, route=route,
                        source_context_available=True, allow_style_rewrite=False,
                    )
                generate.assert_awaited_once()
                request = " ".join(generate.await_args.args[0].split())
                for pair in authored_pairs:
                    self.assertIn(pair, request)
                self.assertIn("Never invent participants, handles, quotations, or transcripts", request)
                self.assertIn("speaker on that same source event", request)
                self.assertIn("acknowledge unsupported BNL wording", request)
                self.assertIn("these bounded excerpts", request)
                self.assertIn("consequential current-room exact-quote request still requires", request)
                self.assertNotIn("Exact wording is allowed only when a typed", request)
                self.assertNotIn("Does not repeat from its database verbatim", request)
                self.assertFalse(bot.is_consequential_exact_quote_request("Give me some quotes."))

    async def test_missing_archive_keeps_specific_uncertainty_at_provider_boundary(self):
        context = show_context(events_available=False)
        generate = mock.AsyncMock(return_value=bot.GenerationResult(True, "I cannot verify that quote."))
        with (
            mock.patch.object(bot, "check_quota_availability", return_value=True),
            mock.patch.object(bot, "_generate_gemini_content_result_async", generate),
        ):
            await bot.get_gemini_response(
                context + "\nCurrent user request: Give me some quotes.",
                101, 1, route=bot.ORDINARY_CHAT_SINGLE_PACKET_ROUTE,
                source_context_available=True,
            )
        request = " ".join(generate.await_args.args[0].split())
        self.assertIn("durable TikTok event archive could not be read", request)
        self.assertIn("When one exact fact is unavailable, answer everything else that is supported", request)
        self.assertIn("Missing bounded evidence does not prove a person or event never existed", request)
        self.assertNotIn("The green lights changed.", request)

    async def test_each_optional_style_provider_receives_quote_preservation_contract(self):
        original = 'Test Member said, "The green lights changed."'
        for expected_route, rolls in (
            ("glitch_rewrite", [0.0, 1.0]),
            ("cross_universe_bleed", [1.0, 0.0]),
        ):
            with self.subTest(route=expected_route):
                generate = mock.AsyncMock(return_value=bot.GenerationResult(True, original))
                style = mock.AsyncMock(return_value=SimpleNamespace(
                    candidates=[SimpleNamespace(content=SimpleNamespace(
                        parts=[SimpleNamespace(text=original)],
                    ))],
                    usage_metadata=SimpleNamespace(total_token_count=4),
                ))
                with (
                    mock.patch.object(bot, "check_quota_availability", return_value=True),
                    mock.patch.object(bot, "conversation_context_v2_enabled", return_value=True),
                    mock.patch.object(bot, "_generate_gemini_content_result_async", generate),
                    mock.patch.object(bot, "_generate_gemini_content_with_fallback_async", style),
                    mock.patch.object(bot.random, "random", side_effect=rolls),
                ):
                    await bot.get_gemini_response(
                        show_context() + "\nCurrent user request: Give me some quotes.",
                        101, 1, source_context_available=True,
                    )
                style.assert_awaited_once()
                self.assertEqual(style.await_args.args[1], expected_route)
                request = style.await_args.args[0]
                self.assertIn(original, request)
                self.assertIn("Preserve all supplied participant names, quoted wording, and speaker attribution unchanged", request)
                self.assertIn("Lore may color the voice around the answer", request)

    def test_final_episode_contract_preserves_source_roles_and_scoped_uncertainty(self):
        context = (
            "Durable BARCODE Radio show episode memory:\n"
            "Source-linked authored examples:\n"
            '- [tiktok] t+2.0m "Test Member": "The green lights changed."\n'
            "Public Discord interactions with BNL during this episode:\n"
            '  BNL replied: "Test Phantom said the lights were blue."\n'
        )
        contract = bot.build_tiktok_show_episode_turn_contract(context)
        self.assertIn("paired with the speaker on that same source event", contract)
        self.assertIn("earlier BNL replies are not independent evidence of audience wording", contract)
        self.assertIn("A missing detail does not establish that the person never appeared", contract)
        self.assertIn("Acknowledge and correct BNL's own unsupported wording", contract)
        self.assertEqual(bot.build_tiktok_show_episode_turn_contract(""), "")
        self.assertEqual(bot.build_tiktok_show_episode_turn_contract("Current queue state: closed"), "")


if __name__ == "__main__":
    unittest.main()
