import asyncio
import os
import threading
import unittest
from types import SimpleNamespace
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl01_bot


def gemini_response(text="BNL relay response.", tokens=17):
    return SimpleNamespace(
        candidates=[SimpleNamespace(content=SimpleNamespace(parts=[SimpleNamespace(text=text)]))],
        usage_metadata=SimpleNamespace(total_token_count=tokens),
    )


class GeminiOffloadTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_gemini_helper_offloads_sync_generation(self):
        loop_thread = threading.get_ident()
        called_threads = []

        def fake_sync(contents, route):
            called_threads.append(threading.get_ident())
            self.assertEqual(contents, "prompt")
            self.assertEqual(route, "website_relay_generation")
            return gemini_response("offloaded", 3)

        with mock.patch.object(bnl01_bot, "_generate_gemini_content_with_fallback", side_effect=fake_sync):
            with self.assertLogs(level="INFO") as logs:
                response = await bnl01_bot._generate_gemini_content_with_fallback_async("prompt", "website_relay_generation")

        self.assertEqual(bnl01_bot._extract_text_and_tokens(response), ("offloaded", 3))
        self.assertTrue(called_threads)
        self.assertNotEqual(called_threads[0], loop_thread)
        joined = "\n".join(logs.output)
        self.assertIn("gemini_generation_offloaded route=website_relay_generation", joined)
        self.assertIn("gemini_generation_completed route=website_relay_generation", joined)

    async def test_async_gemini_helper_preserves_fallback_model_behavior(self):
        calls = []

        def fake_generate_content(model, contents):
            calls.append((model, contents))
            if len(calls) == 1:
                raise Exception("503 service unavailable")
            return gemini_response("fallback ok", 5)

        with mock.patch.object(bnl01_bot.gemini_client.models, "generate_content", side_effect=fake_generate_content):
            response = await bnl01_bot._generate_gemini_content_with_fallback_async("contents", "fallback_route")

        self.assertEqual(bnl01_bot._extract_text_and_tokens(response), ("fallback ok", 5))
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[0][0], bnl01_bot._gemini_model_resource_name(bnl01_bot.GEMINI_MODEL))
        self.assertEqual(calls[1][0], bnl01_bot._gemini_model_resource_name(bnl01_bot.GEMINI_FALLBACK_MODEL))

    async def test_get_gemini_response_keeps_token_accounting(self):
        with mock.patch.object(bnl01_bot, "check_quota_availability", return_value=True), \
             mock.patch.object(bnl01_bot, "get_conversation_history", return_value=[]), \
             mock.patch.object(bnl01_bot, "_generate_gemini_content_with_fallback_async", return_value=gemini_response("token text", 42)), \
             mock.patch.object(bnl01_bot, "increment_token_usage") as inc, \
             mock.patch.object(bnl01_bot.random, "random", return_value=1.0):
            text = await bnl01_bot.get_gemini_response("hello", user_id=123, guild_id=456, route="normal_chat")

        self.assertEqual(text, "token text")
        inc.assert_called_once_with(42)

    async def test_get_gemini_response_keeps_quota_handling_without_generation(self):
        with mock.patch.object(bnl01_bot, "check_quota_availability", return_value=False), \
             mock.patch.object(bnl01_bot, "get_usage_stats", return_value=(bnl01_bot.DAILY_TOKEN_LIMIT, "today")), \
             mock.patch.object(bnl01_bot, "_generate_gemini_content_with_fallback_async") as generate:
            text = await bnl01_bot.get_gemini_response("hello", user_id=123, guild_id=456)

        self.assertIn("Daily quota exhausted", text)
        generate.assert_not_called()

    async def test_glitch_rewrite_uses_offloaded_generation(self):
        responses = [gemini_response("base text", 11), gemini_response("glitched text", 2)]

        async def fake_async(contents, route):
            return responses.pop(0)

        with mock.patch.object(bnl01_bot, "check_quota_availability", return_value=True), \
             mock.patch.object(bnl01_bot, "get_conversation_history", return_value=[]), \
             mock.patch.object(bnl01_bot, "_generate_gemini_content_with_fallback_async", side_effect=fake_async) as generate, \
             mock.patch.object(bnl01_bot, "update_website_status_controlled_async", return_value=True), \
             mock.patch.object(bnl01_bot, "increment_token_usage"), \
             mock.patch.object(bnl01_bot.random, "random", side_effect=[0.01, 1.0]):
            text = await bnl01_bot.get_gemini_response("hello", user_id=0, guild_id=456)

        self.assertEqual(text, "glitched text")
        self.assertEqual([call.args[1] for call in generate.await_args_list], ["get_gemini_response", "glitch_rewrite"])


class WebsiteRelayGuardTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        bnl01_bot._website_relay_generation_tasks_by_guild.clear()

    async def asyncTearDown(self):
        for task in list(bnl01_bot._website_relay_generation_tasks_by_guild.values()):
            task.cancel()
        await asyncio.sleep(0)
        bnl01_bot._website_relay_generation_tasks_by_guild.clear()

    async def test_slow_relay_generation_times_out_safely_and_logs(self):
        original_timeout = bnl01_bot.BNL_WEBSITE_RELAY_GENERATION_TIMEOUT_SECONDS
        release = asyncio.Event()

        async def slow_generation(guild_id):
            await release.wait()
            return bnl01_bot.WebsiteRelayDecision(True, message="late", directive="directive", mode="OBSERVATION")

        bnl01_bot.BNL_WEBSITE_RELAY_GENERATION_TIMEOUT_SECONDS = 0.01
        try:
            with mock.patch.object(bnl01_bot, "generate_dynamic_website_relay", side_effect=slow_generation):
                with self.assertLogs(level="WARNING") as logs:
                    decision = await bnl01_bot._generate_website_relay_guarded(99)
                self.assertEqual(decision.mode, "OBSERVATION")
                self.assertFalse(decision.publish)
                self.assertEqual(decision.metadata["reason"], "relay_generation_timeout")
                self.assertIn("website_relay_generation_timeout", "\n".join(logs.output))
        finally:
            release.set()
            await asyncio.sleep(0.02)
            bnl01_bot.BNL_WEBSITE_RELAY_GENERATION_TIMEOUT_SECONDS = original_timeout

    async def test_inflight_relay_generation_skips_second_tick_and_clears_after_success(self):
        release = asyncio.Event()

        async def slow_generation(guild_id):
            await release.wait()
            return ("OBSERVATION", "message", "directive", {"reason": "ok"})

        with mock.patch.object(bnl01_bot, "generate_dynamic_website_relay", side_effect=slow_generation):
            first = asyncio.create_task(bnl01_bot._generate_website_relay_guarded(123))
            await asyncio.sleep(0)
            with self.assertLogs(level="INFO") as logs:
                skipped = await bnl01_bot._generate_website_relay_guarded(123)
            self.assertIsNone(skipped)
            self.assertIn("website_relay_generation_skipped_inflight", "\n".join(logs.output))
            release.set()
            result = await first

        self.assertEqual(result[1], "message")
        await asyncio.sleep(0)
        self.assertNotIn(123, bnl01_bot._website_relay_generation_tasks_by_guild)

    async def test_status_push_wrapper_offloads_sync_status_update(self):
        loop_thread = threading.get_ident()
        called_threads = []

        def fake_status(**kwargs):
            called_threads.append(threading.get_ident())
            self.assertEqual(kwargs["mode"], "OBSERVATION")
            return True

        with mock.patch.object(bnl01_bot, "update_website_status_controlled", side_effect=fake_status):
            with self.assertLogs(level="INFO") as logs:
                ok = await bnl01_bot.update_website_status_controlled_async(mode="OBSERVATION", message="msg")

        self.assertTrue(ok)
        self.assertTrue(called_threads)
        self.assertNotEqual(called_threads[0], loop_thread)
        self.assertIn("website_status_push_offloaded", "\n".join(logs.output))


if __name__ == "__main__":
    unittest.main()
