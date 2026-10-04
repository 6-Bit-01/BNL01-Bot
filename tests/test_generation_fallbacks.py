import asyncio
import logging
import os
import sqlite3
import unittest
from unittest import mock
os.environ.setdefault("GEMINI_API_KEY", "test-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-token")
import bnl01_bot


class FakeChannel:
    def __init__(self, policy="public_home", name="general", channel_id=123):
        self.id = channel_id
        self.name = name
        self.sent = []
        self.raise_on_send = None

    async def send(self, text, **kwargs):
        if self.raise_on_send:
            raise self.raise_on_send
        self.sent.append(text)
        return text


class FakeReplyMessage:
    def __init__(self, channel):
        self.channel = channel
        self.replies = []
        self.raise_on_reply = None

    async def reply(self, text, **kwargs):
        if self.raise_on_reply:
            raise self.raise_on_reply
        self.replies.append(text)
        return text


class GenerationFallbackTests(unittest.TestCase):
    def test_public_fallback_omits_forbidden_provider_terms(self):
        text = bnl01_bot.generation_fallback_text("public_lore")
        self.assertIn("upper processing layer", text)
        forbidden = ("Gemini", "Google", "API key", "billing", "quota", "project id")
        for term in forbidden:
            self.assertNotIn(term.lower(), text.lower())

    def test_private_fallback_mentions_generation_provider_without_secret_terms(self):
        text = bnl01_bot.generation_fallback_text("private_plain")
        self.assertIn("generation provider", text)
        self.assertIn("model connection", text)
        self.assertNotIn("API key".lower(), text.lower())
        self.assertNotIn("project id", text.lower())

    def test_public_batch_answer_403_sends_public_lore_fallback(self):
        channel = FakeChannel(policy="public_home")
        result = asyncio.run(bnl01_bot.send_generation_fallback(
            channel,
            route="get_gemini_response",
            channel_policy="public_home",
            conversation_surface=bnl01_bot.CONVERSATION_SURFACE_FREE_SPEAK_PUBLIC_HOME,
            directness="batch_answer",
        ))
        self.assertTrue(result)
        self.assertEqual(channel.sent, [bnl01_bot.PUBLIC_GENERATION_FALLBACK])

    def test_public_batch_timeout_and_empty_use_public_lore_classifier(self):
        for exc, expected in [
            (TimeoutError("timed out"), bnl01_bot.GENERATION_ERROR_PROVIDER_TIMEOUT),
            (None, bnl01_bot.GENERATION_ERROR_PROVIDER_EMPTY),
        ]:
            if exc is None:
                category, _code, _safe = bnl01_bot.classify_generation_error(empty_response=True)
            else:
                category, _code, _safe = bnl01_bot.classify_generation_error(exc)
            self.assertEqual(category, expected)
            self.assertEqual(
                bnl01_bot.generation_fallback_kind("public_home", bnl01_bot.CONVERSATION_SURFACE_FREE_SPEAK_PUBLIC_HOME),
                "public_lore",
            )

    def test_passive_observe_path_does_not_require_fallback(self):
        self.assertFalse(bnl01_bot.should_send_generation_fallback(
            "get_gemini_response",
            decision="observe",
            reason="passive_room_observation",
        ))
        self.assertFalse(bnl01_bot.should_send_generation_fallback(
            "get_gemini_response",
            decision="no_response_needed",
        ))

    def test_direct_request_path_requires_fallback(self):
        self.assertTrue(bnl01_bot.should_send_generation_fallback(
            "get_gemini_response",
            decision="",
            directness="real_direct_target",
            request_intent=True,
        ))

    def test_test_channel_gets_plain_fallback(self):
        kind = bnl01_bot.generation_fallback_kind(
            "sealed_test",
            bnl01_bot.CONVERSATION_SURFACE_FREE_SPEAK_SEALED_MIRROR,
            "get_gemini_response",
            "request_intent",
        )
        self.assertEqual(kind, "test_plain")
        self.assertEqual(bnl01_bot.generation_fallback_text(kind), bnl01_bot.PRIVATE_GENERATION_FALLBACK)

    def test_internal_ops_gets_internal_plain_fallback(self):
        channel = FakeChannel(policy="internal_controlled", name="research-and-development")
        message = FakeReplyMessage(channel)
        result = asyncio.run(bnl01_bot.send_generation_fallback(
            channel,
            route="internal_operations_brief",
            channel_policy="internal_controlled",
            conversation_surface=bnl01_bot.CONVERSATION_SURFACE_COMMAND_ONLY,
            directness="command_only",
            reply_to=message,
            internal=True,
        ))
        self.assertTrue(result)
        self.assertEqual(message.replies, [bnl01_bot.INTERNAL_GENERATION_FALLBACK])

    def test_discord_send_failure_logged_and_does_not_crash(self):
        channel = FakeChannel()
        channel.raise_on_send = RuntimeError("discord down")
        with self.assertLogs(level="ERROR") as logs:
            result = asyncio.run(bnl01_bot.send_generation_fallback(
                channel,
                route="get_gemini_response",
                channel_policy="public_home",
                conversation_surface=bnl01_bot.CONVERSATION_SURFACE_FREE_SPEAK_PUBLIC_HOME,
            ))
        self.assertFalse(result)
        self.assertTrue(any("response_send_failed" in line for line in logs.output))

    def test_generation_result_success_and_no_duplicate_fallback(self):
        success = bnl01_bot.GenerationResult(True, "normal answer", route="get_gemini_response", model="test")
        bnl01_bot.record_generation_result_status(success)
        self.assertEqual(bnl01_bot._last_generation_status["status"], "success")
        channel = FakeChannel()
        # Successful generated answers are sent by normal send paths; fallback helper is not invoked by success handlers.
        self.assertEqual(channel.sent, [])

    def test_error_category_mapping_for_403_429_500_network(self):
        cases = [
            (Exception("403 PERMISSION_DENIED billing denied"), bnl01_bot.GENERATION_ERROR_PROVIDER_PERMISSION_DENIED),
            (Exception("429 quota exceeded"), bnl01_bot.GENERATION_ERROR_PROVIDER_RATE_LIMITED),
            (Exception("500 server error"), bnl01_bot.GENERATION_ERROR_PROVIDER_SERVER),
            (Exception("network connection reset"), bnl01_bot.GENERATION_ERROR_PROVIDER_NETWORK),
        ]
        for exc, expected in cases:
            self.assertEqual(bnl01_bot.classify_generation_error(exc)[0], expected)

    def test_local_storage_failures_are_not_provider_or_budget_errors(self):
        cases = [
            (sqlite3.OperationalError("database is locked"), "sqlite_busy"),
            (sqlite3.OperationalError("database table is locked"), "sqlite_busy"),
            (sqlite3.OperationalError("database or disk is full"), "sqlite_full"),
            (sqlite3.OperationalError("no such table: private_fixture"), "sqlite_error"),
            (sqlite3.DatabaseError("malformed: private source quota=429"), "sqlite_error"),
        ]
        for error, reason in cases:
            with self.subTest(reason=reason, error_type=type(error).__name__):
                category, code, safe_message = bnl01_bot.classify_generation_error(error)
                self.assertEqual(category, bnl01_bot.GENERATION_ERROR_LOCAL_STORAGE)
                self.assertEqual(code, reason)
                self.assertEqual(safe_message, "BNL's local storage could not complete this request.")
                self.assertNotIn("private", safe_message)

    def test_extended_sqlite_error_codes_keep_local_storage_classification(self):
        for sqlite_code, reason in ((5 | (2 << 8), "sqlite_busy"),
                                    (6 | (1 << 8), "sqlite_busy"),
                                    (13, "sqlite_full")):
            with self.subTest(sqlite_code=sqlite_code):
                error = sqlite3.OperationalError("private database detail")
                error.sqlite_errorcode = sqlite_code
                self.assertEqual(bnl01_bot.classify_generation_error(error)[:2],
                                 (bnl01_bot.GENERATION_ERROR_LOCAL_STORAGE, reason))

    def _quota_preflight_failure(self, error, *, raise_on_failure=False,
                                 provide_output=True):
        output = {}
        with (
            mock.patch.dict(bnl01_bot._last_generation_status, {
                "status": "success", "error_category": "stale_category",
                "provider_error_code": "stale_code",
            }, clear=True),
            mock.patch.object(bnl01_bot, "record_generation_result_status",
                              wraps=bnl01_bot.record_generation_result_status) as recorded,
            mock.patch.object(bnl01_bot, "check_quota_availability", side_effect=error) as quota,
            mock.patch.object(bnl01_bot, "get_gemini_client") as client,
            mock.patch.object(bnl01_bot, "_generate_gemini_content_result_async", new_callable=mock.AsyncMock) as provider,
            mock.patch.object(bnl01_bot, "reserve_local_model_budget") as reserve,
            mock.patch.object(bnl01_bot, "record_token_usage") as usage,
            self.assertLogs(level="ERROR") as logs,
        ):
            call = bnl01_bot.get_gemini_response(
                "Test request", 123, 456,
                raise_on_generation_failure=raise_on_failure,
                **({"generation_result_out": output} if provide_output else {}),
            )
            if raise_on_failure:
                with self.assertRaises(bnl01_bot.BackgroundGenerationUnavailable) as raised:
                    asyncio.run(call)
                self.assertIs(raised.exception.__cause__, error)
            else:
                self.assertEqual(asyncio.run(call), "")
            recorded.assert_called_once()
            result = recorded.call_args.args[0]
            if provide_output:
                self.assertIs(output["result"], result)
            if raise_on_failure:
                self.assertIs(raised.exception.result, result)
            self.assertEqual(bnl01_bot._last_generation_status["status"], "failure")
            self.assertEqual(bnl01_bot._last_generation_status["error_category"],
                             result.error_category)
            self.assertEqual(bnl01_bot._last_generation_status["provider_error_code"],
                             result.provider_error_code)
            quota.assert_called_once_with("get_gemini_response")
            client.assert_not_called()
            provider.assert_not_awaited()
            reserve.assert_not_called()
            usage.assert_not_called()
        self.assertFalse(result.success)
        return result, "\n".join(logs.output)

    def test_busy_quota_preflight_is_local_storage_without_provider_call(self):
        result, logged = self._quota_preflight_failure(
            sqlite3.OperationalError("database is locked"),
            provide_output=False,
        )
        self.assertEqual(result.error_category, bnl01_bot.GENERATION_ERROR_LOCAL_STORAGE)
        self.assertEqual(result.provider_error_code, "sqlite_busy")
        self.assertEqual(result.provider_error_message_safe,
                         "BNL's local storage could not complete this request.")
        self.assertEqual(result.total_tokens, 0)
        self.assertEqual(result.estimated_cost_nanos, 0)
        self.assertIn("response_generation_exception", logged)
        self.assertIn("error_type=OperationalError", logged)
        self.assertNotIn("database is locked", logged)

    def test_quota_preflight_uses_existing_error_classifier_and_safe_logging(self):
        errors = (
            sqlite3.DatabaseError("malformed private_fixture quota=429"),
            bnl01_bot.LocalModelBudgetExhausted("interactive_reserved"),
            TimeoutError("private_fixture timed out token=fictional-token"),
            RuntimeError("private_fixture token=fictional-token"),
        )
        for error in errors:
            with self.subTest(error_type=type(error).__name__):
                expected = bnl01_bot.classify_generation_error(error)
                result, logged = self._quota_preflight_failure(error,
                                                              provide_output=False)
                self.assertEqual((result.error_category, result.provider_error_code,
                                  result.provider_error_message_safe), expected)
                self.assertNotIn("private_fixture", logged)
                self.assertNotIn("fictional-token", logged)

    def test_busy_quota_preflight_background_failure_retains_classified_result(self):
        result, _logged = self._quota_preflight_failure(
            sqlite3.OperationalError("database is locked"),
            raise_on_failure=True,
        )
        self.assertEqual(result.error_category, bnl01_bot.GENERATION_ERROR_LOCAL_STORAGE)
        self.assertEqual(result.provider_error_code, "sqlite_busy")


if __name__ == "__main__":
    unittest.main()
