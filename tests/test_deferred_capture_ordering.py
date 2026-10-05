"""Deferred ownership spans ingress through mandatory capture and handoff.

Capture uses the real SQLite owner throughout. Coordinator inputs and external
transport/provider remain synthetic.
"""

import asyncio
import sqlite3
import threading
import unittest
from unittest import mock

import test_conversation_batching as existing


bot = existing.bnl01_bot


class DeferredCaptureOrderingTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = existing.ConversationBatchCoordinatorTests.asyncSetUp
    asyncTearDown = existing.ConversationBatchCoordinatorTests.asyncTearDown
    _channel = existing.ConversationBatchCoordinatorTests._channel
    def _on_message_runtime(self, channel_id, *, followup_candidate=True):
        capture = bot.save_user_message
        stack = existing.ConversationBatchCoordinatorTests._on_message_runtime(
            self, channel_id, followup_candidate=followup_candidate,
        )
        stack.enter_context(mock.patch.object(bot, "save_user_message", side_effect=capture))
        return stack

    def setUp(self):
        self.addCleanup(self._clear_capture_waiters)

    def _clear_capture_waiters(self):
        channel_ids = getattr(self, "channel_ids", ())
        for key in list(getattr(bot, "_direct_payload_capture_waiters", {})):
            if len(key) >= 2 and key[1] in channel_ids:
                bot._direct_payload_capture_waiters.pop(key)["event"].set()

    async def test_sequential_payload_is_retained_control(self):
        channel = self._channel(991649)
        request = existing.FakeMessage(
            channel, "BNL, tell me something about each of these people",
        )
        payload = existing.FakeMessage(channel, "Test Subject")
        key = (channel.guild.id, channel.id, request.author.id)
        real_capture = bot.save_user_message
        with (
            self._on_message_runtime(channel.id, followup_candidate=True),
            mock.patch.object(bot, "save_user_message", side_effect=real_capture),
            mock.patch.object(
                bot, "_direct_session_timer", new=mock.AsyncMock(return_value=None),
            ),
        ):
            await bot.on_message(request)
            await bot.on_message(payload)
        self.assertEqual(bot._direct_payload_sessions[key]["payload_lines"], ["Test Subject"])
        self.assertEqual(channel.sent + request.replies + payload.replies, [])

    async def test_rapid_payload_during_request_capture_wait_is_retained(self):
        channel = self._channel(991648)
        author = existing.FakeAuthor(100, "Test Member")
        request = existing.FakeMessage(
            channel, "BNL, tell me something about each of these people", author=author,
        )
        payload = existing.FakeMessage(channel, "Test Subject", author=author)
        key = (channel.guild.id, channel.id, author.id)
        captured, payload_lookup = asyncio.Event(), asyncio.Event()
        release_capture = threading.Event()
        loop = asyncio.get_running_loop()
        real_capture, real_session_key = bot.save_user_message, bot._direct_session_key

        def observed_session_key(message):
            if message is payload:
                payload_lookup.set()
            return real_session_key(message)

        def delayed_capture(*args, **kwargs):
            result = real_capture(*args, **kwargs)
            if kwargs.get("message_id") == request.id:
                loop.call_soon_threadsafe(captured.set)
                if not release_capture.wait(5):
                    raise TimeoutError("bounded capture ordering fixture timed out")
            return result

        provider = mock.AsyncMock(side_effect=AssertionError("provider must not run"))
        with (
            self._on_message_runtime(channel.id, followup_candidate=True),
            mock.patch.object(bot, "save_user_message", side_effect=delayed_capture),
            mock.patch.object(bot, "_direct_session_key", side_effect=observed_session_key),
            mock.patch.object(
                bot, "_direct_session_timer", new=mock.AsyncMock(return_value=None),
            ),
            mock.patch.object(bot, "get_gemini_response", new=provider),
            mock.patch.object(bot, "get_gemini_response_with_optional_typing", new=provider),
        ):
            first, second = asyncio.create_task(bot.on_message(request)), None
            try:
                await asyncio.wait_for(captured.wait(), timeout=3)
                self.assertFalse(first.done())
                self.assertNotIn(key, bot._direct_payload_sessions)
                second = asyncio.create_task(bot.on_message(payload))
                await asyncio.wait_for(payload_lookup.wait(), timeout=3)
                # Yield scheduling while the anchor's capture stays held.
                for _ in range(8):
                    await asyncio.sleep(0)
                self.assertFalse(second.done())
                self.assertEqual(list(bot._channel_buffers[channel.id]), [])
                self.assertNotIn(key, bot._direct_payload_sessions)
            finally:
                release_capture.set()
                await asyncio.wait_for(first, timeout=3)
                if second is not None:
                    await asyncio.wait_for(second, timeout=3)
            self.assertEqual(bot._direct_payload_sessions[key]["payload_lines"], ["Test Subject"])
            self.assertEqual(list(bot._channel_buffers[channel.id]), [])
            self.assertNotIn(key, bot._direct_payload_capture_waiters)
            provider.assert_not_awaited()
            self.assertEqual(channel.sent + request.replies + payload.replies, [])
            conn = sqlite3.connect(bot.DB_FILE)
            try:
                rows = conn.execute(
                    "SELECT role,content FROM conversations WHERE guild_id=? AND channel_id=? ORDER BY id",
                    (channel.guild.id, channel.id),
                ).fetchall()
            finally:
                conn.close()
            self.assertEqual(rows, [("user", request.content), ("user", payload.content)])

    async def _capture_exit_releases_followup(self, *, cancel):
        channel = self._channel(991650 if cancel else 991651)
        request = existing.FakeMessage(
            channel, "BNL, tell me something about each of these people",
        )
        payload = existing.FakeMessage(channel, "Test Subject")
        key = (channel.guild.id, channel.id, request.author.id)
        captured, returned, payload_lookup = asyncio.Event(), asyncio.Event(), asyncio.Event()
        release = threading.Event()
        loop = asyncio.get_running_loop()
        real_capture, real_key = bot.save_user_message, bot._direct_session_key

        def capture_with_exit(*args, **kwargs):
            result = real_capture(*args, **kwargs)
            if kwargs.get("message_id") == request.id:
                loop.call_soon_threadsafe(captured.set)
                if not release.wait(5):
                    raise TimeoutError("bounded capture exit fixture timed out")
                loop.call_soon_threadsafe(returned.set)
                if not cancel:
                    raise RuntimeError("intentional capture failure fixture")
            return result

        def observe_key(message):
            if message is payload:
                payload_lookup.set()
            return real_key(message)

        provider = mock.AsyncMock(side_effect=AssertionError("provider must not run"))
        with (
            self._on_message_runtime(channel.id, followup_candidate=True),
            mock.patch.object(bot, "save_user_message", side_effect=capture_with_exit),
            mock.patch.object(bot, "_direct_session_key", side_effect=observe_key),
            mock.patch.object(
                bot, "_direct_session_timer", new=mock.AsyncMock(return_value=None),
            ),
            mock.patch.object(bot, "get_gemini_response", new=provider),
            mock.patch.object(bot, "get_gemini_response_with_optional_typing", new=provider),
        ):
            first, second, waiter = asyncio.create_task(bot.on_message(request)), None, None
            try:
                await asyncio.wait_for(captured.wait(), timeout=3)
                waiter = bot._direct_payload_capture_waiters.get(key)
                second = asyncio.create_task(bot.on_message(payload))
                await asyncio.wait_for(payload_lookup.wait(), timeout=3)
                for _ in range(8):
                    await asyncio.sleep(0)
                pending_before_exit = not second.done()
                if cancel:
                    first.cancel()
                else:
                    release.set()
                outcomes = await asyncio.gather(first, return_exceptions=True)
            finally:
                release.set()
                await asyncio.wait_for(returned.wait(), timeout=3)
                if second is not None:
                    await asyncio.wait_for(second, timeout=3)
            self.assertIsNotNone(waiter)
            self.assertTrue(pending_before_exit)
            self.assertTrue(waiter["event"].is_set())
            self.assertNotIn(key, bot._direct_payload_capture_waiters)
            self.assertNotIn(key, bot._direct_payload_sessions)
            self.assertEqual([str(item[1]) for item in bot._channel_buffers[channel.id]], ["Test Subject"])
            provider.assert_not_awaited()
            self.assertIsInstance(outcomes[0], asyncio.CancelledError if cancel else RuntimeError)

    async def test_capture_failure_releases_same_user_followup(self):
        await self._capture_exit_releases_followup(cancel=False)

    async def test_capture_cancellation_releases_same_user_followup(self):
        await self._capture_exit_releases_followup(cancel=True)

    async def test_early_request_await_cannot_lose_rapid_nomention_payload(self):
        channel = self._channel(991652)
        request = existing.FakeMessage(channel, "BNL, tell me something about each of these people")
        payload = existing.FakeMessage(channel, "Test Subject", author=request.author)
        entered, release, looked_up = asyncio.Event(), asyncio.Event(), asyncio.Event()
        real_key = bot._direct_session_key

        async def pause_first_await(message, _content):
            if message is request:
                entered.set()
                await release.wait()
            return False

        def observe_key(message):
            if message is payload:
                looked_up.set()
            return real_key(message)

        with (
            self._on_message_runtime(channel.id, followup_candidate=True),
            mock.patch.object(bot, "maybe_handle_declared_canon_command", side_effect=pause_first_await),
            mock.patch.object(bot, "_direct_session_key", side_effect=observe_key),
            mock.patch.object(bot, "_direct_session_timer", new=mock.AsyncMock(return_value=None)),
        ):
            first = asyncio.create_task(bot.on_message(request))
            second = None
            try:
                await asyncio.wait_for(entered.wait(), timeout=3)
                second = asyncio.create_task(bot.on_message(payload))
                await asyncio.wait_for(looked_up.wait(), timeout=3)
            finally:
                release.set()
                await asyncio.wait_for(first, timeout=3)
                if second is not None:
                    await asyncio.wait_for(second, timeout=3)
        key = real_key(request)
        self.assertEqual(bot._direct_payload_sessions[key]["payload_lines"], ["Test Subject"])
        self.assertEqual(list(bot._channel_buffers[channel.id]), [])
        self.assertNotIn(key, bot._direct_payload_capture_waiters)
        self.assertIs(bot.client.on_message, bot.on_message)
        await self._deliver_nomention_payload(key, request)

    async def _deliver_nomention_payload(self, key, request):
        answer = "Test Subject: a quiet thought with good drums."
        provider = mock.AsyncMock(return_value=answer)
        with (
            mock.patch.object(bot, "build_room_first_direct_context", return_value=""),
            mock.patch.object(bot, "maybe_build_bnl_read_model_context", return_value=""),
            mock.patch.object(bot, "maybe_build_source_context_for_direct_message", new=mock.AsyncMock(return_value="")),
            mock.patch.object(bot, "build_user_aware_prompt", return_value=("Synthetic payload task", False, "balanced")),
            mock.patch.object(bot, "log_response_style"),
            mock.patch.object(bot, "_apply_direct_response_pacing", new=mock.AsyncMock()),
            mock.patch.object(bot, "get_gemini_response_with_optional_typing", new=provider),
            mock.patch.object(bot, "suppress_stale_media_fallback", side_effect=lambda response, **_kw: response),
            mock.patch.object(bot, "apply_guarded_response_regeneration", new=mock.AsyncMock(return_value=(answer, {"suppressed": False}))),
            mock.patch.object(bot, "is_privileged_member", return_value=False),
            mock.patch.object(bot, "DIRECT_PRE_SEND_GRACE_SECONDS", 0),
            mock.patch.object(bot, "exact_quote_presend_failure", new=mock.AsyncMock(return_value="")),
            mock.patch.object(bot, "prompt_source_basis_failure", return_value=""),
            mock.patch.object(bot, "model_response_persistence_allowed_with_website_context", return_value=False),
            mock.patch.object(bot, "persist_bnl_self_name_decision_after_send_async", new=mock.AsyncMock()),
            mock.patch.object(bot, "record_unified_response_assessment_shadow_after_send", new=mock.AsyncMock()),
        ):
            await bot._generate_direct_payload_session(key, "quiet_timeout")
        provider.assert_awaited_once()
        self.assertIn("Test Subject", provider.await_args.args[1])
        self.assertEqual(request.replies, [answer])
        self.assertEqual(bot._direct_payload_sessions[key]["last_committed_payload_count"], 1)
        self.assertFalse(bot._direct_payload_sessions[key]["generating"])
        self.assertEqual(bot._direct_payload_capture_waiters, {})

    async def test_raw_controls_do_not_wait_or_reserve_a_payload_handoff(self):
        channel = self._channel(991657)
        request = existing.FakeMessage(channel, "BNL, tell me something about each of these people")
        controls = [existing.FakeMessage(channel, text, author=request.author) for text in (
            '!bnl canon declare {"claim":"tell me something about each of these people"}',
            "!bnl journal test", "/debug following names",
        )]
        entered, release = asyncio.Event(), asyncio.Event()

        async def handler(message, raw):
            self.assertEqual(raw, message.content)
            if message is request:
                entered.set()
                await release.wait()
                return False
            return True

        with (
            self._on_message_runtime(channel.id, followup_candidate=True),
            mock.patch.object(bot, "maybe_handle_declared_canon_command", side_effect=handler),
            mock.patch.object(bot, "_direct_session_timer", new=mock.AsyncMock(return_value=None)),
        ):
            first = asyncio.create_task(bot.on_message(request))
            try:
                await asyncio.wait_for(entered.wait(), timeout=3)
                await asyncio.wait_for(asyncio.gather(*(bot.on_message(m) for m in controls)), timeout=3)
                self.assertFalse(first.done())
                self.assertEqual(len(bot._direct_payload_capture_waiters), 1)
            finally:
                release.set()
                await asyncio.wait_for(first, timeout=3)
        self.assertEqual(bot._direct_payload_capture_waiters, {})

    async def test_new_explicit_request_keeps_its_following_payload_order(self):
        channel = self._channel(991658)
        author = existing.FakeAuthor()
        bot_mention = existing.SimpleNamespace(id=999, display_name="BNL-01", bot=True)
        first_request = existing.FakeMessage(channel, "BNL, tell me something about each of these people", author=author)
        replacement = existing.FakeMessage(channel, "<@999> tell me something about each of these names", author=author, mentions=[bot_mention])
        payload = existing.FakeMessage(channel, "Test Subject", author=author)
        entered, replacement_entered = asyncio.Event(), asyncio.Event()
        release, replacement_release = asyncio.Event(), asyncio.Event()

        async def pause_requests(message, _raw):
            if message is first_request:
                entered.set()
                await release.wait()
            elif message is replacement:
                replacement_entered.set()
                await replacement_release.wait()
            return False

        with (
            self._on_message_runtime(channel.id, followup_candidate=True),
            mock.patch.object(bot, "maybe_handle_declared_canon_command", side_effect=pause_requests),
            mock.patch.object(bot, "_direct_session_timer", new=mock.AsyncMock(return_value=None)),
        ):
            first = asyncio.create_task(bot.on_message(first_request))
            second = third = None
            try:
                await asyncio.wait_for(entered.wait(), timeout=3)
                second = asyncio.create_task(bot.on_message(replacement))
                third = asyncio.create_task(bot.on_message(payload))
                release.set()
                await asyncio.wait_for(replacement_entered.wait(), timeout=3)
                self.assertFalse(third.done())
            finally:
                release.set()
                replacement_release.set()
                await asyncio.wait_for(first, timeout=3)
                if second is not None:
                    await asyncio.wait_for(second, timeout=3)
                if third is not None:
                    await asyncio.wait_for(third, timeout=3)
        session = bot._direct_payload_sessions[bot._direct_session_key(replacement)]
        self.assertEqual(session["anchor_message_id"], replacement.id)
        self.assertEqual(session["payload_lines"], ["Test Subject"])
        self.assertEqual(bot._direct_payload_capture_waiters, {})

    async def test_nonactive_and_unconfigured_routes_use_the_same_deferred_owner(self):
        for active_id in (8000, None):
            with self.subTest(active_id=active_id):
                channel = self._channel(991660 if active_id is None else 991659)
                request = existing.FakeMessage(channel, "<@999> tell me something about each of these people",
                    mentions=[existing.SimpleNamespace(id=999, display_name="BNL-01", bot=True)])
                payload = existing.FakeMessage(channel, "Test Subject", author=request.author)
                with (
                    self._on_message_runtime(channel.id, followup_candidate=True),
                    mock.patch.object(bot, "get_guild_config", return_value=active_id),
                    mock.patch.object(bot, "resolve_channel_policy", return_value="public_context"),
                    mock.patch.object(bot, "_direct_session_timer", new=mock.AsyncMock(return_value=None)),
                ):
                    await bot.on_message(request)
                    await bot.on_message(payload)
                session = bot._direct_payload_sessions[bot._direct_session_key(request)]
                self.assertEqual(session["payload_lines"], ["Test Subject"])
                self.assertEqual(request.replies + payload.replies, [])
                self.assertEqual(bot._direct_payload_capture_waiters, {})

    async def test_recall_denial_releases_followup_before_denial_delivery(self):
        channel = self._channel(991662)
        request = existing.BlockingReplyMessage(channel, "BNL, summarize each of these people")
        payload = existing.FakeMessage(channel, "Test Subject", author=request.author)
        boundary = "Synthetic privacy boundary"
        with (
            self._on_message_runtime(channel.id, followup_candidate=True),
            mock.patch.object(bot, "get_conversation_recall_guard_response", return_value=boundary),
            mock.patch.object(bot, "_reset_debounce"),
        ):
            first = asyncio.create_task(bot.on_message(request))
            try:
                await asyncio.wait_for(request.reply_started.wait(), timeout=5)
                self.assertFalse(first.done())
                self.assertEqual(bot._direct_payload_capture_waiters, {})
                await asyncio.wait_for(bot.on_message(payload), timeout=5)
                self.assertNotIn(bot._direct_session_key(request), bot._direct_payload_sessions)
            finally:
                request.release_reply.set()
                await asyncio.wait_for(first, timeout=5)
        self.assertEqual(request.replies, [boundary])
        self.assertEqual([str(item[1]) for item in bot._channel_buffers[channel.id]], ["Test Subject"])

    async def test_ordinary_and_inline_requests_do_not_reserve_or_wait(self):
        for content in ("BNL, what's up?", "BNL, tell me something about each of these people: Test Subject"):
            with self.subTest(content=content):
                channel = self._channel(991664 if ":" in content else 991663)
                request = existing.FakeMessage(channel, content)
                other = existing.FakeMessage(channel, "Another ordinary turn", author=request.author)
                entered, release = asyncio.Event(), asyncio.Event()

                async def early_control(message, _raw):
                    if message is request:
                        entered.set()
                        await release.wait()
                        return True
                    return False

                with (
                    self._on_message_runtime(channel.id, followup_candidate=True),
                    mock.patch.object(bot, "maybe_handle_declared_canon_command", side_effect=early_control),
                    mock.patch.object(bot, "_reset_debounce"),
                ):
                    first = asyncio.create_task(bot.on_message(request))
                    try:
                        await asyncio.wait_for(entered.wait(), timeout=5)
                        self.assertEqual(bot._direct_payload_capture_waiters, {})
                        await asyncio.wait_for(bot.on_message(other), timeout=5)
                        self.assertFalse(first.done())
                    finally:
                        release.set()
                        await asyncio.wait_for(first, timeout=5)
                self.assertEqual(bot._direct_payload_capture_waiters, {})

    async def test_early_capture_handoff_is_isolated_by_guild_channel_and_user(self):
        channel = self._channel(991653)
        request = existing.FakeMessage(channel, "BNL, tell me something about each of these people")
        entered, release = asyncio.Event(), asyncio.Event()

        async def pause_first_await(message, _content):
            if message is request:
                entered.set()
                await release.wait()
            return False

        other_channel = self._channel(991654)
        foreign = existing.FakeChannel(channel.id, guild=existing.FakeGuild(7701))
        others = (
            existing.FakeMessage(channel, "Other member payload", author=existing.FakeAuthor(101)),
            existing.FakeMessage(other_channel, "Other channel payload", author=request.author),
            existing.FakeMessage(foreign, "Other guild payload", author=request.author),
        )
        with (
            self._on_message_runtime(channel.id, followup_candidate=True),
            mock.patch.object(bot, "maybe_handle_declared_canon_command", side_effect=pause_first_await),
            mock.patch.object(bot, "_reset_debounce"),
            mock.patch.object(bot, "_direct_session_timer", new=mock.AsyncMock(return_value=None)),
        ):
            first = asyncio.create_task(bot.on_message(request))
            try:
                await asyncio.wait_for(entered.wait(), timeout=3)
                await asyncio.wait_for(asyncio.gather(*(bot.on_message(m) for m in others)), timeout=3)
                self.assertFalse(first.done())
                self.assertEqual(len(bot._direct_payload_capture_waiters), 1)
            finally:
                release.set()
                await asyncio.wait_for(first, timeout=3)
        self.assertEqual(bot._direct_payload_sessions[bot._direct_session_key(request)]["payload_lines"], [])
        self.assertEqual(bot._direct_payload_capture_waiters, {})

    async def test_early_command_denial_and_cancellation_release_followup(self):
        for cancel in (False, True):
            with self.subTest(cancel=cancel):
                channel = self._channel(991656 if cancel else 991655)
                request = existing.FakeMessage(channel, "BNL, tell me something about each of these people")
                payload = existing.FakeMessage(channel, "Test Subject", author=request.author)
                entered, release, looked_up = asyncio.Event(), asyncio.Event(), asyncio.Event()
                real_key = bot._direct_session_key

                async def deny_after_pause(message, _content):
                    if message is request:
                        entered.set()
                        await release.wait()
                        return True
                    return False

                def observe_key(message):
                    if message is payload:
                        looked_up.set()
                    return real_key(message)

                with (
                    self._on_message_runtime(channel.id, followup_candidate=True),
                    mock.patch.object(bot, "maybe_handle_declared_canon_command", side_effect=deny_after_pause),
                    mock.patch.object(bot, "_direct_session_key", side_effect=observe_key),
                    mock.patch.object(bot, "_reset_debounce"),
                ):
                    first = asyncio.create_task(bot.on_message(request))
                    second = None
                    try:
                        await asyncio.wait_for(entered.wait(), timeout=3)
                        second = asyncio.create_task(bot.on_message(payload))
                        await asyncio.wait_for(looked_up.wait(), timeout=3)
                        if cancel:
                            first.cancel()
                        else:
                            release.set()
                        result = await asyncio.gather(first, return_exceptions=True)
                    finally:
                        release.set()
                        if second is not None:
                            await asyncio.wait_for(second, timeout=3)
                self.assertIsInstance(result[0], asyncio.CancelledError) if cancel else self.assertIsNone(result[0])
                self.assertNotIn(real_key(request), bot._direct_payload_sessions)
                self.assertEqual(bot._direct_payload_capture_waiters, {})
                self.assertEqual([str(item[1]) for item in bot._channel_buffers[channel.id]], ["Test Subject"])
