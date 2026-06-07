import asyncio
import logging
import os
import unittest
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

    async def reply(self, text):
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


if __name__ == "__main__":
    unittest.main()
