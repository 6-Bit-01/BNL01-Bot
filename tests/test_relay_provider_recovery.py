import asyncio
from contextlib import ExitStack
import os
import threading
import unittest
from types import SimpleNamespace
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl01_bot as bot


ROUTE = "website_relay_event"


def provider_error(status=503):
    error = RuntimeError(f"{status} provider failure")
    error.code = status
    return error


def provider_context(generate, release=None):
    stack = ExitStack()
    stack.enter_context(mock.patch.object(bot, "gemini_client", SimpleNamespace(
        models=SimpleNamespace(generate_content=generate))))
    stack.enter_context(mock.patch.object(bot, "GEMINI_MODEL", "gemini-3.6-flash"))
    stack.enter_context(mock.patch.object(bot, "GEMINI_FALLBACK_MODEL", "gemini-3.5-flash"))
    stack.enter_context(mock.patch.object(bot, "reserve_local_model_budget", return_value=17))
    stack.enter_context(mock.patch.object(bot, "release_local_model_budget", side_effect=release))
    return stack


class RelayRecoveryTests(unittest.TestCase):
    def test_primary_503_uses_backup_once_and_accounts_both(self):
        response = object()
        error = provider_error()
        generate = mock.Mock(side_effect=[error, response])
        counter = bot.ProviderAttemptCounter()
        with (provider_context(generate),
              mock.patch.object(bot, "record_failed_generation_attempt") as failed,
              mock.patch.object(bot, "record_generation_token_usage") as succeeded,
              mock.patch.object(bot.time, "sleep") as pause):
            result = bot._generate_gemini_content_with_fallback("approved context", ROUTE,
                                                               attempt_counter=counter)
        self.assertIs(result.raw_response, response)
        self.assertTrue(result.fallback_used)
        self.assertEqual(result.model_name, "gemini-3.5-flash")
        self.assertEqual(counter.count, 2)
        self.assertEqual([c.kwargs["model"] for c in generate.call_args_list],
                         ["models/gemini-3.6-flash", "models/gemini-3.5-flash"])
        failed.assert_called_once()
        self.assertFalse(failed.call_args.kwargs["is_fallback"])
        succeeded.assert_called_once()
        self.assertTrue(succeeded.call_args.kwargs["is_fallback"])
        self.assertFalse(succeeded.call_args.kwargs["is_retry"])
        pause.assert_called_once()
        self.assertGreaterEqual(pause.call_args.args[0], 0.5)
        self.assertLessEqual(pause.call_args.args[0], 1.0)

    def test_primary_success_has_no_backup_or_pause(self):
        generate = mock.Mock(return_value=object())
        with (provider_context(generate),
              mock.patch.object(bot, "record_generation_token_usage"),
              mock.patch.object(bot.time, "sleep") as pause):
            result = bot._generate_gemini_content_with_fallback("context", ROUTE)
        self.assertFalse(result.fallback_used)
        self.assertEqual(generate.call_count, 1)
        pause.assert_not_called()

    def test_two_failures_stop_at_two_physical_calls(self):
        primary, backup = provider_error(), provider_error()
        generate = mock.Mock(side_effect=[primary, backup])
        release = mock.Mock()
        with (provider_context(generate, release),
              mock.patch.object(bot, "record_failed_generation_attempt") as failed,
              mock.patch.object(bot.time, "sleep")):
            with self.assertRaises(RuntimeError) as caught:
                bot._generate_gemini_content_with_fallback("context", ROUTE)
        self.assertIs(caught.exception, backup)
        self.assertEqual(generate.call_count, 2)
        self.assertEqual(failed.call_count, 2)
        self.assertTrue(failed.call_args.kwargs["is_fallback"])
        release.assert_called_once_with(17)

    def test_other_errors_do_not_fallback(self):
        for status in (400, 401, 402, 403, 404, 429, 500, 502, 504):
            with self.subTest(status=status):
                generate = mock.Mock(side_effect=provider_error(status))
                with (provider_context(generate),
                      mock.patch.object(bot, "record_failed_generation_attempt"),
                      mock.patch.object(bot.time, "sleep") as pause):
                    with self.assertRaises(RuntimeError):
                        bot._generate_gemini_content_with_fallback("context", ROUTE)
                self.assertEqual(generate.call_count, 1)
                pause.assert_not_called()

    def test_absent_or_identical_backup_is_not_called(self):
        for backup in ("", "gemini-3.6-flash"):
            with self.subTest(backup=backup):
                generate = mock.Mock(side_effect=provider_error())
                with (provider_context(generate),
                      mock.patch.object(bot, "GEMINI_FALLBACK_MODEL", backup),
                      mock.patch.object(bot, "record_failed_generation_attempt")):
                    with self.assertRaises(RuntimeError):
                        bot._generate_gemini_content_with_fallback("context", ROUTE)
                self.assertEqual(generate.call_count, 1)

    def test_cancelled_before_worker_starts_has_no_provider_call(self):
        cancelled = threading.Event()
        cancelled.set()
        generate, release = mock.Mock(), mock.Mock()
        with provider_context(generate, release):
            with self.assertRaisesRegex(RuntimeError, "relay_generation_cancelled"):
                bot._generate_gemini_content_with_fallback("context", ROUTE,
                                                          cancel_event=cancelled)
        generate.assert_not_called()
        release.assert_called_once_with(17)


class RelayCancellationTests(unittest.IsolatedAsyncioTestCase):
    async def test_timed_out_primary_cannot_start_backup_or_publish_late(self):
        started, finish_primary, worker_done = (threading.Event() for _ in range(3))
        def slow_primary(**kwargs):
            started.set()
            finish_primary.wait(3)
            raise provider_error()
        generate = mock.Mock(side_effect=slow_primary)
        async def relay(guild_id, **kwargs):
            await bot._generate_gemini_content_with_fallback_async("context", ROUTE)
            return bot.WebsiteRelayDecision(True, message="late draft")
        with (provider_context(generate, lambda *args, **kwargs: worker_done.set()),
              mock.patch.object(bot, "record_failed_generation_attempt") as failed,
              mock.patch.object(bot, "generate_dynamic_website_relay", side_effect=relay),
              mock.patch.object(bot, "relay_get_cursor", return_value=23),
              mock.patch.object(bot, "BNL_WEBSITE_RELAY_GENERATION_TIMEOUT_SECONDS", 0.05)):
            task = asyncio.create_task(bot._generate_website_relay_guarded(42))
            try:
                self.assertTrue(await asyncio.to_thread(started.wait, 2))
                decision = await task
                self.assertFalse(decision.publish)
                self.assertEqual(decision.skipReason, "relay_generation_timeout")
                self.assertEqual(decision.sourceCursor, 23)
            finally:
                finish_primary.set()
                self.assertTrue(await asyncio.to_thread(worker_done.wait, 2))
            self.assertEqual(generate.call_count, 1)
            failed.assert_called_once()
        self.assertNotIn(42, bot._website_relay_generation_tasks_by_guild)


if __name__ == "__main__":
    unittest.main()
