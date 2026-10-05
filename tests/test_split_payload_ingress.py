"""Request prose must not impersonate the deferred list it refers to.

The ingress regression uses the real decorated handler, mandatory SQLite
capture, typed mention, existing session owner, and mocked final transport.
"""

from __future__ import annotations

import asyncio
import gc
import os
import sqlite3
import threading
import unittest
from types import SimpleNamespace
from unittest import mock

import test_conversation_batching as existing
import test_deferred_capture_ordering as capture_tests


bot = existing.bnl01_bot


class ReferencedPayloadExtractionTests(unittest.TestCase):
    def test_modified_references_are_not_literal_payload_items(self):
        for request in (
            "Make one short joke for each of these fictional names.",
            "Tell me something about each of those made-up characters.",
            "Give me one for each of these short-listed entries.",
            "Write a line about each of these imaginary radio people.",
        ):
            with self.subTest(request=request):
                self.assertTrue(bot._detect_request_payload_expectation(request)[0])
                self.assertEqual(bot._collect_inline_direct_payload_items(request), [])

    def test_explicit_inline_and_multiline_data_remain_payload(self):
        for request, expected in (
            (
                "Make one short joke for each of these fictional names: Test Sparrow, Test Comet.",
                ["Test Sparrow", "Test Comet"],
            ),
            (
                "Make one short joke for each of these fictional names:\n"
                "Test Sparrow\nTest Lantern\nTest Comet",
                ["Test Sparrow", "Test Lantern", "Test Comet"],
            ),
            (
                "Tell me something about each of these people: Test Sparrow, Test Comet.",
                ["Test Sparrow", "Test Comet"],
            ),
            (
                'Tell me something about each of these names: "These Fictional Names", Test Comet.',
                ['"These Fictional Names"', "Test Comet"],
            ),
            (
                "Make one short joke for each of these fictional names: These Fictional Names, Test Comet.",
                ["These Fictional Names", "Test Comet"],
            ),
            (
                "Make one short joke for each of these fictional names:\nThese Fictional Names\nTest Comet",
                ["These Fictional Names", "Test Comet"],
            ),
            (
                "Reply with exactly this name and no other words: These Fictional Names",
                ["These Fictional Names"],
            ),
            (
                "Tell me something about each of these people: These Names",
                ["These Names"],
            ),
            (
                "Tell me something about each of these people: Thanks",
                ["Thanks"],
            ),
        ):
            with self.subTest(request=request):
                self.assertEqual(bot._collect_inline_direct_payload_items(request), expected)

    def test_deictic_grammar_does_not_consume_literal_boundaries(self):
        for text in (
            "These Fictional Names: Test Sparrow",
            '"These Fictional Names"',
            "These Fictional Names, Test Comet",
            "Test Sparrow",
        ):
            with self.subTest(text=text):
                self.assertFalse(bot._is_deictic_payload_placeholder(text))


class SplitPayloadIngressTests(unittest.IsolatedAsyncioTestCase):
    asyncTearDown = existing.ConversationBatchCoordinatorTests.asyncTearDown
    _channel = existing.ConversationBatchCoordinatorTests._channel
    _on_message_runtime = existing.ConversationBatchCoordinatorTests._on_message_runtime
    setUp = capture_tests.DeferredCaptureOrderingTests.setUp
    _clear_capture_waiters = capture_tests.DeferredCaptureOrderingTests._clear_capture_waiters

    async def asyncSetUp(self):
        await existing.ConversationBatchCoordinatorTests.asyncSetUp(self)
        if os.name == "nt":
            # sqlite3 connection cycles must release Windows fixture handles
            # before TemporaryDirectory removes this synthetic database.
            self.addCleanup(gc.collect)

    async def test_actual_mention_request_and_rapid_list_survive_delayed_capture(self):
        channel = self._channel(991670)
        author = existing.FakeAuthor(100, "Test Member")
        mention = SimpleNamespace(id=999, display_name="BNL-01", bot=True)
        request = existing.FakeMessage(
            channel,
            "<@999> Make one short joke for each of these fictional names.",
            author=author,
            mentions=[mention],
        )
        list_text = "Test Sparrow\nTest Lantern\nTest Comet"
        payload = existing.FakeMessage(channel, list_text, author=author)
        key = (channel.guild.id, channel.id, author.id)
        captured, looked_up = asyncio.Event(), asyncio.Event()
        release_capture = threading.Event()
        loop = asyncio.get_running_loop()
        real_capture, real_key = bot.save_user_message, bot._direct_session_key
        if os.name == "nt":
            # The POSIX capture privacy fence requires native Linux CI. The
            # Windows fixture still runs its real SQLite writer/projections,
            # not a fake successful flock or an invented capture result.
            real_capture = real_capture.__wrapped__

        def delayed_capture(*args, **kwargs):
            result = real_capture(*args, **kwargs)
            if kwargs.get("message_id") == request.id:
                loop.call_soon_threadsafe(captured.set)
                if not release_capture.wait(5):
                    raise TimeoutError("bounded split payload fixture timed out")
            return result

        def observe_key(message):
            if message is payload:
                looked_up.set()
            return real_key(message)

        provider = mock.AsyncMock(side_effect=AssertionError("premature generation"))
        with (
            self._on_message_runtime(channel.id, followup_candidate=False),
            mock.patch.object(bot, "save_user_message", side_effect=delayed_capture),
            mock.patch.object(bot, "_direct_session_key", side_effect=observe_key),
            mock.patch.object(bot, "_direct_session_timer", new=mock.AsyncMock()),
            mock.patch.object(bot, "get_gemini_response", new=provider),
            mock.patch.object(bot, "get_gemini_response_with_optional_typing", new=provider),
        ):
            first = asyncio.create_task(bot.on_message(request))
            second = None
            try:
                await asyncio.wait_for(captured.wait(), timeout=3)
                self.assertIs(bot.client.on_message, bot.on_message)
                self.assertIn(key, bot._direct_payload_capture_waiters)
                self.assertNotIn(key, bot._direct_payload_sessions)
                second = asyncio.create_task(bot.on_message(payload))
                await asyncio.wait_for(looked_up.wait(), timeout=3)
                for _ in range(8):
                    await asyncio.sleep(0)
                self.assertFalse(second.done())
                self.assertEqual(list(bot._channel_buffers[channel.id]), [])
            finally:
                release_capture.set()
                # On pre-fix failure, cancel after the real capture finishes so
                # the fixture never enters provider/context generation.
                if second is None:
                    first.cancel()
                    await asyncio.gather(first, return_exceptions=True)
                else:
                    await asyncio.wait_for(first, timeout=3)
                    await asyncio.wait_for(second, timeout=3)
            provider.assert_not_awaited()

        session = bot._direct_payload_sessions[key]
        self.assertEqual(session["anchor_message_id"], request.id)
        self.assertEqual(session["payload_lines"], [list_text])
        self.assertEqual(session["revision"], 1)
        self.assertEqual(list(bot._channel_buffers[channel.id]), [])
        self.assertNotIn(key, bot._direct_payload_capture_waiters)
        self.assertEqual(request.replies + payload.replies + channel.sent, [])
        with sqlite3.connect(bot.DB_FILE) as conn:
            captured_rows = conn.execute(
                "SELECT message_id,role,content,channel_policy FROM conversations "
                "WHERE guild_id=? AND channel_id=? ORDER BY id",
                (channel.guild.id, channel.id),
            ).fetchall()
        conn.close()
        self.assertEqual(
            captured_rows,
            [(request.id, "user", "Make one short joke for each of these fictional names.", "sealed_test")],
        )
        await self._deliver_list(key, request, payload, list_text)

    async def test_modified_request_uses_same_session_outside_active_channel(self):
        for active_id in (8000, None):
            with self.subTest(active_id=active_id):
                channel = self._channel(991672 if active_id is None else 991671)
                request = existing.FakeMessage(
                    channel,
                    "<@999> Make one short joke for each of these fictional names.",
                    mentions=[SimpleNamespace(id=999, display_name="BNL-01", bot=True)],
                )
                payload = existing.FakeMessage(
                    channel, "Test Sparrow\nTest Lantern\nTest Comet", author=request.author,
                )
                provider = mock.AsyncMock(side_effect=AssertionError("premature generation"))
                with (
                    self._on_message_runtime(channel.id, followup_candidate=False),
                    mock.patch.object(bot, "get_guild_config", return_value=active_id),
                    mock.patch.object(bot, "resolve_channel_policy", return_value="public_context"),
                    mock.patch.object(bot, "_direct_session_timer", new=mock.AsyncMock()),
                    mock.patch.object(bot, "get_gemini_response_with_optional_typing", new=provider),
                ):
                    await bot.on_message(request)
                    await bot.on_message(payload)
                session = bot._direct_payload_sessions[bot._direct_session_key(request)]
                self.assertEqual(session["anchor_message_id"], request.id)
                self.assertEqual(session["payload_lines"], [payload.content])
                self.assertEqual(list(bot._channel_buffers[channel.id]), [])
                self.assertEqual(request.replies + payload.replies + channel.sent, [])
                provider.assert_not_awaited()

    async def _deliver_list(self, key, request, payload, list_text):
        answer = (
            "Test Sparrow: tiny bird, enormous rider.\n"
            "Test Lantern: the only headliner with a light bill.\n"
            "Test Comet: always late, always makes an entrance."
        )
        provider = mock.AsyncMock(return_value=answer)
        real_guard = bot.apply_guarded_response_regeneration
        real_freshness = bot.prompt_source_basis_failure_async
        with (
            mock.patch.object(bot, "build_room_first_direct_context", return_value=""),
            mock.patch.object(bot, "maybe_build_bnl_read_model_context", return_value=""),
            mock.patch.object(bot, "maybe_build_source_context_for_direct_message", new=mock.AsyncMock(return_value="")),
            mock.patch.object(bot, "build_user_aware_prompt", side_effect=lambda _user, _guild, _name, text, **_kw: (text, False, "balanced")),
            mock.patch.object(bot, "log_response_style"),
            mock.patch.object(bot, "_apply_direct_response_pacing", new=mock.AsyncMock()),
            mock.patch.object(bot, "get_gemini_response_with_optional_typing", new=provider),
            mock.patch.object(bot, "apply_guarded_response_regeneration", wraps=real_guard) as guard,
            mock.patch.object(bot, "prompt_source_basis_failure_async", wraps=real_freshness) as freshness,
            mock.patch.object(bot, "suppress_stale_media_fallback", side_effect=lambda response, **_kw: response),
            mock.patch.object(bot, "is_privileged_member", return_value=False),
            mock.patch.object(bot, "DIRECT_PRE_SEND_GRACE_SECONDS", 0),
            mock.patch.object(bot, "model_response_persistence_allowed_with_website_context", return_value=False),
            mock.patch.object(bot, "persist_bnl_self_name_decision_after_send_async", new=mock.AsyncMock()),
            mock.patch.object(bot, "record_unified_response_assessment_shadow_after_send", new=mock.AsyncMock()),
        ):
            await bot._generate_direct_payload_session(key, "quiet_timeout")
        provider.assert_awaited_once()
        prompt = provider.await_args.args[1]
        self.assertIn("Make one short joke for each of these fictional names.", prompt)
        self.assertIn(list_text, prompt)
        self.assertIn("DIRECT REQUEST PAYLOAD ITEMS:", prompt)
        guard.assert_awaited_once()
        self.assertIn(list_text, guard.await_args.kwargs["current_user_text"])
        self.assertIn(list_text, guard.await_args.kwargs["situation_frame_current_text"])
        self.assertGreaterEqual(freshness.await_count, 1)
        self.assertEqual(bot._missing_request_payload_items(list_text.splitlines(), answer), [])
        self.assertEqual(request.replies, [answer])
        self.assertEqual(payload.replies + request.channel.sent, [])
        self.assertEqual(bot._direct_payload_sessions[key]["last_committed_revision"], 1)
        self.assertEqual(bot._direct_payload_sessions[key]["last_committed_payload_count"], 1)
        self.assertFalse(bot._direct_payload_sessions[key]["generating"])
        self.assertNotIn(key, bot._direct_payload_capture_waiters)


if __name__ == "__main__":
    unittest.main()
