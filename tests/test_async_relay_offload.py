import asyncio
import os
import subprocess
import sys
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


class GeminiClientLifecycleTests(unittest.TestCase):
    def test_module_import_does_not_construct_gemini_client(self):
        probe = """
from google import genai
def fail_if_constructed(*args, **kwargs):
    raise RuntimeError('Gemini client constructed during import')
genai.Client = fail_if_constructed
import bnl01_bot
assert bnl01_bot.gemini_client is None
"""
        completed = subprocess.run(
            [sys.executable, "-c", probe],
            cwd=os.path.dirname(os.path.dirname(__file__)),
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)

    def test_gemini_client_is_created_once_on_first_generation(self):
        fake_client = SimpleNamespace(models=SimpleNamespace(generate_content=mock.Mock()))
        with mock.patch.object(bnl01_bot, "gemini_client", None), \
             mock.patch.object(bnl01_bot.genai, "Client", return_value=fake_client) as factory:
            self.assertIs(bnl01_bot.get_gemini_client(), fake_client)
            self.assertIs(bnl01_bot.get_gemini_client(), fake_client)
        factory.assert_called_once()
        kwargs = factory.call_args.kwargs
        self.assertEqual(kwargs["api_key"], bnl01_bot.GEMINI_API_KEY)
        self.assertEqual(kwargs["http_options"].timeout, 120_000)
        self.assertEqual(kwargs["http_options"].retry_options.attempts, 1)


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
        fallback_response = gemini_response("fallback ok", 5)
        primary_resource = bnl01_bot._gemini_model_resource_name(
            bnl01_bot.GEMINI_MODEL
        )

        def fake_generate_content(model, contents, config=None):
            calls.append((model, contents, config))
            if model == primary_resource:
                raise Exception("503 service unavailable")
            return fallback_response

        fake_client = SimpleNamespace(
            models=SimpleNamespace(
                generate_content=mock.Mock(side_effect=fake_generate_content)
            )
        )
        with mock.patch.object(bnl01_bot, "gemini_client", fake_client), \
             mock.patch.object(
                 bnl01_bot,
                 "reserve_local_model_budget",
                 return_value=17,
             ) as reserve, \
             mock.patch.object(
                 bnl01_bot,
                 "release_local_model_budget",
             ) as release, \
             mock.patch.object(
                 bnl01_bot,
                 "record_generation_token_usage",
             ) as record, \
             mock.patch.object(
                 bnl01_bot,
                 "record_failed_generation_attempt",
             ) as record_failed, \
             mock.patch.object(bnl01_bot.time, "sleep"):
            response = await bnl01_bot._generate_gemini_content_with_fallback_async(
                "contents",
                "fallback_route",
            )

        self.assertEqual(
            bnl01_bot._extract_text_and_tokens(response),
            ("fallback ok", 5),
        )
        self.assertEqual(response.model_name, bnl01_bot.GEMINI_FALLBACK_MODEL)
        self.assertTrue(response.fallback_used)
        primary_calls = [call for call in calls if call[0] == primary_resource]
        fallback_calls = [call for call in calls if call[0] != primary_resource]
        self.assertEqual(len(primary_calls), 2)
        self.assertEqual(len(fallback_calls), 1)
        self.assertIsNone(primary_calls[0][2].thinking_config)
        self.assertEqual(
            primary_calls[0][2].max_output_tokens,
            bnl01_bot.policy_for_route("fallback_route").max_output_tokens,
        )
        reserve.assert_called_once_with("contents", "fallback_route")
        self.assertEqual(record_failed.call_count, 2)
        record.assert_called_once()
        self.assertEqual(
            record.call_args.kwargs["model"],
            bnl01_bot.GEMINI_FALLBACK_MODEL,
        )
        release.assert_called_once_with(17)

    async def test_journal_does_not_automatically_fallback(self):
        calls = []

        def fail_generation(model, contents, config=None):
            calls.append(model)
            raise Exception("503 service unavailable")

        fake_client = SimpleNamespace(
            models=SimpleNamespace(
                generate_content=mock.Mock(side_effect=fail_generation)
            )
        )
        with mock.patch.object(bnl01_bot, "gemini_client", fake_client), \
             mock.patch.object(
                 bnl01_bot,
                 "reserve_local_model_budget",
                 return_value=17,
             ), \
             mock.patch.object(bnl01_bot, "release_local_model_budget"), \
             mock.patch.object(
                 bnl01_bot,
                 "record_failed_generation_attempt",
             ), \
             mock.patch.object(bnl01_bot.time, "sleep"):
            with self.assertRaisesRegex(Exception, "503"):
                await bnl01_bot._generate_gemini_content_with_fallback_async(
                    "journal contents",
                    bnl01_bot.JOURNAL_ROUTE,
                )

        self.assertEqual(len(calls), 1)
        self.assertTrue(
            all(
                model
                == bnl01_bot._gemini_model_resource_name(
                    bnl01_bot.GEMINI_MODEL
                )
                for model in calls
            )
        )

    async def test_get_gemini_response_does_not_double_count_central_accounting(self):
        with mock.patch.object(bnl01_bot, "check_quota_availability", return_value=True), \
             mock.patch.object(bnl01_bot, "get_conversation_history", return_value=[]), \
             mock.patch.object(bnl01_bot, "_generate_gemini_content_with_fallback_async", return_value=gemini_response("token text", 42)), \
             mock.patch.object(bnl01_bot, "increment_token_usage") as inc, \
             mock.patch.object(bnl01_bot.random, "random", return_value=1.0):
            text = await bnl01_bot.get_gemini_response("hello", user_id=123, guild_id=456, route="normal_chat")

        self.assertEqual(text, "token text")
        inc.assert_not_called()

    async def test_get_gemini_response_keeps_quota_handling_without_generation(self):
        with mock.patch.object(bnl01_bot, "check_quota_availability", return_value=False), \
             mock.patch.object(bnl01_bot, "_generate_gemini_content_with_fallback_async") as generate, \
             mock.patch.object(bnl01_bot, "record_generation_result_status") as record_status:
            text = await bnl01_bot.get_gemini_response("hello", user_id=123, guild_id=456)

        self.assertEqual(text, "")
        generate.assert_not_called()
        result = record_status.call_args.args[0]
        self.assertFalse(result.success)
        self.assertEqual(
            result.error_category,
            bnl01_bot.GENERATION_ERROR_LOCAL_MODEL_BUDGET,
        )

    async def test_tracked_generation_counts_zero_when_local_quota_blocks(self):
        with mock.patch.object(bnl01_bot, "BNL_TYPING_INDICATOR_ENABLED", False), \
             mock.patch.object(bnl01_bot, "check_quota_availability", return_value=False), \
             mock.patch.object(bnl01_bot, "_generate_gemini_content_with_fallback_async") as generate:
            result = await bnl01_bot.get_tracked_gemini_response_with_optional_typing(
                None,
                "packet-owned prompt",
                7,
                1,
                bnl01_bot.ORDINARY_CHAT_SINGLE_PACKET_ROUTE,
                source_context_available=True,
            )

        self.assertEqual(result.text, "")
        self.assertEqual(result.provider_call_count, 0)
        generate.assert_not_called()

    async def test_provider_counter_marks_successful_physical_invocation(self):
        counter = bnl01_bot.ProviderAttemptCounter()
        fake_client = SimpleNamespace(
            models=SimpleNamespace(
                generate_content=mock.Mock(
                    return_value=gemini_response("single packet", 5)
                )
            )
        )
        with mock.patch.object(bnl01_bot, "gemini_client", fake_client), \
             mock.patch.object(bnl01_bot, "reserve_local_model_budget", return_value=17), \
             mock.patch.object(bnl01_bot, "release_local_model_budget"), \
             mock.patch.object(bnl01_bot, "record_generation_token_usage"):
            response = await bnl01_bot._generate_gemini_content_with_fallback_async(
                "packet-owned prompt",
                bnl01_bot.ORDINARY_CHAT_SINGLE_PACKET_ROUTE,
                attempt_counter=counter,
            )

        self.assertEqual(
            bnl01_bot._extract_text_and_tokens(response),
            ("single packet", 5),
        )
        self.assertEqual(counter.count, 1)
        fake_client.models.generate_content.assert_called_once()

    async def test_provider_counter_stays_zero_when_client_setup_fails(self):
        counter = bnl01_bot.ProviderAttemptCounter()
        with mock.patch.object(bnl01_bot, "reserve_local_model_budget", return_value=17), \
             mock.patch.object(
                 bnl01_bot,
                 "get_gemini_client",
                 side_effect=RuntimeError("client setup failed"),
             ), \
             mock.patch.object(bnl01_bot, "release_local_model_budget") as release:
            with self.assertRaisesRegex(RuntimeError, "client setup failed"):
                await bnl01_bot._generate_gemini_content_with_fallback_async(
                    "packet-owned prompt",
                    bnl01_bot.ORDINARY_CHAT_SINGLE_PACKET_ROUTE,
                    attempt_counter=counter,
                )

        self.assertEqual(counter.count, 0)
        release.assert_called_once_with(17)

    async def test_provider_counter_marks_failed_physical_invocation(self):
        counter = bnl01_bot.ProviderAttemptCounter()
        fake_client = SimpleNamespace(
            models=SimpleNamespace(
                generate_content=mock.Mock(
                    side_effect=Exception("503 service unavailable")
                )
            )
        )
        with mock.patch.object(bnl01_bot, "gemini_client", fake_client), \
             mock.patch.object(bnl01_bot, "reserve_local_model_budget", return_value=17), \
             mock.patch.object(bnl01_bot, "release_local_model_budget"), \
             mock.patch.object(bnl01_bot, "record_failed_generation_attempt"):
            with self.assertRaisesRegex(Exception, "503"):
                await bnl01_bot._generate_gemini_content_with_fallback_async(
                    "packet-owned prompt",
                    bnl01_bot.ORDINARY_CHAT_SINGLE_PACKET_ROUTE,
                    attempt_counter=counter,
                )

        self.assertEqual(counter.count, 1)
        fake_client.models.generate_content.assert_called_once()

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
