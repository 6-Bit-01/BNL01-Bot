"""Exercise the existing deferred owner through its real timer and ingress.

Only existing context/provider/Discord fixture boundaries are substituted.
Capture, exact source bases and their revalidation use real SQLite owners.
Time advances deterministically; no 10/40-second sleeps are required.
"""
from __future__ import annotations

import asyncio
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import sqlite3
import unittest
from unittest import mock

import test_conversation_batching as existing
import test_payload_description_handoff as handoff


bot = existing.bnl01_bot
REAL_TIMER = bot._direct_session_timer
NAMES = handoff.NAMES


class AckMessage(existing.FakeMessage):
    def __init__(self, *args, hold=False, fail=False, **kwargs):
        super().__init__(*args, **kwargs)
        self.ack_attempts = 0
        self.ack_started = asyncio.Event()
        self.ack_release = asyncio.Event()
        self.hold_ack = hold
        self.fail_ack = fail

    async def reply(self, text, **kwargs):
        if "send the list/items" in text:
            self.ack_attempts += 1
            self.ack_started.set()
            if self.hold_ack:
                await self.ack_release.wait()
            if self.fail_ack:
                raise RuntimeError("neutral acknowledgment transport failure")
        return await super().reply(text, **kwargs)


class DirectPayloadWaitLifecycleTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = existing.ConversationBatchCoordinatorTests.asyncSetUp
    asyncTearDown = existing.ConversationBatchCoordinatorTests.asyncTearDown
    _channel = existing.ConversationBatchCoordinatorTests._channel
    _on_message_runtime = existing.ConversationBatchCoordinatorTests._on_message_runtime
    _clear_capture_waiters = handoff.PayloadDescriptionHandoffTests._clear_capture_waiters
    _capture_runtime = handoff.PayloadDescriptionHandoffTests._capture_runtime
    _durable_rows = handoff.PayloadDescriptionHandoffTests._durable_rows

    def setUp(self):
        self.addCleanup(self._clear_capture_waiters)

    def _request(self, channel, *, hold=False, fail=False):
        mention = existing.SimpleNamespace(id=999, display_name="BNL-01", bot=True)
        return AckMessage(
            channel, "<@999> " + handoff.FAILED_PHRASE,
            mentions=[mention], hold=hold, fail=fail,
        )

    @contextmanager
    def _at(self, now):
        class ControlledClock(datetime):
            @classmethod
            def now(cls, tz=None):
                return now.astimezone(tz) if tz else now.replace(tzinfo=None)

        with mock.patch.object(bot, "datetime", ControlledClock):
            yield

    async def _until(self, predicate):
        for _ in range(300):
            if predicate():
                return
            await asyncio.sleep(0.01)
        self.fail("bounded neutral owner did not reach the expected state")

    async def _pulse(self, key, now, predicate):
        actual_generation = bot._generate_direct_payload_session
        generation_started, generation_finished = asyncio.Event(), asyncio.Event()

        async def observe_generation(*args, **kwargs):
            generation_started.set()
            try:
                return await actual_generation(*args, **kwargs)
            finally:
                generation_finished.set()

        with self._at(now), mock.patch.object(bot, "_generate_direct_payload_session", side_effect=observe_generation):
            timer = asyncio.create_task(REAL_TIMER(key))
            try:
                await self._until(predicate)
                # Cancel only the idle timer after the real generation owner
                # finishes its post-ACK/source checks. Cancellation during
                # those checks is tested explicitly below.
                if generation_started.is_set():
                    await asyncio.wait_for(generation_finished.wait(), 3)
                await asyncio.sleep(0)
            finally:
                timer.cancel()
                try:
                    await timer
                except asyncio.CancelledError:
                    pass

    def _generation_runtime(self, answer):
        async def guarded(response, **_kwargs):
            return response, {"suppressed": False}

        from contextlib import ExitStack
        stack = ExitStack()
        provider = mock.AsyncMock(return_value=answer)
        for patcher in (
            mock.patch.object(bot, "build_room_first_direct_context", return_value=""),
            mock.patch.object(bot, "maybe_build_bnl_read_model_context", return_value=""),
            mock.patch.object(bot, "maybe_build_source_context_for_direct_message", new=mock.AsyncMock(return_value="")),
            mock.patch.object(bot, "build_user_aware_prompt", side_effect=lambda *_args, **_kw: (_args[3], False, "balanced")),
            mock.patch.object(bot, "log_response_style"),
            mock.patch.object(bot, "_apply_direct_response_pacing", new=mock.AsyncMock()),
            mock.patch.object(bot, "get_gemini_response_with_optional_typing", new=provider),
            mock.patch.object(bot, "suppress_stale_media_fallback", side_effect=lambda response, **_kw: response),
            mock.patch.object(bot, "apply_guarded_response_regeneration", new=mock.AsyncMock(side_effect=guarded)),
            mock.patch.object(bot, "is_privileged_member", return_value=False),
            mock.patch.object(bot, "DIRECT_PRE_SEND_GRACE_SECONDS", 0),
            mock.patch.object(bot, "exact_quote_presend_failure", new=mock.AsyncMock(return_value="")),
            mock.patch.object(bot, "model_response_persistence_allowed_with_website_context", return_value=False),
            mock.patch.object(bot, "persist_bnl_self_name_decision_after_send_async", new=mock.AsyncMock()),
            mock.patch.object(bot, "record_unified_response_assessment_shadow_after_send", new=mock.AsyncMock()),
        ):
            stack.enter_context(patcher)
        return stack, provider

    def _change_original(self, message, change):
        conn = sqlite3.connect(bot.DB_FILE)
        try:
            if change == "delete":
                conn.execute("DELETE FROM conversations WHERE message_id=?", (message.id,))
            elif change == "edit":
                conn.execute("UPDATE conversations SET content=? WHERE message_id=?", ("Corrected synthetic request", message.id))
            elif change == "privacy":
                conn.execute("UPDATE conversations SET channel_policy='protected_system' WHERE message_id=?", (message.id,))
            elif change == "retract":
                conn.execute("UPDATE memory_ledger_entries SET lifecycle_status='retracted' WHERE source_message_id=? AND source_table='conversations'", (message.id,))
            else:
                conn.execute(
                    "INSERT INTO memory_ledger_lineage(entry_id,guild_id,lineage_type,target_entry_id,created_at) "
                    "SELECT ?,guild_id,'correction_of',entry_id,? FROM memory_ledger_entries "
                    "WHERE source_message_id=? AND source_table='conversations'",
                    ("synthetic-anchor-correction", datetime.now(timezone.utc).isoformat(), message.id),
                )
            conn.commit()
        finally:
            conn.close()

    async def test_timeout_ack_then_late_payload_completes_same_request_once(self):
        channel = self._channel(996100)
        request = self._request(channel)
        payload = existing.FakeMessage(channel, "\n".join(NAMES), author=request.author)
        key = bot._direct_session_key(request)
        answer = "\n".join(name + ": a synthetic joke." for name in NAMES)
        with self._capture_runtime(channel.id):
            await bot.on_message(request)
            session = bot._direct_payload_sessions[key]
            self.assertEqual(session["anchor_source_basis"].source_row_ids, (self._durable_rows(request)[0][0][0],))
            created = session["created_at"]
            await self._pulse(key, created + timedelta(seconds=10.25), lambda: request.ack_attempts == 1)
            self.assertIs(bot._direct_payload_sessions.get(key), session)
            self.assertFalse(session["generating"])
            with self._at(created + timedelta(seconds=12.8)):
                await bot.on_message(payload)
        self.assertEqual(session["payload_lines"], [payload.content])
        self.assertEqual(list(bot._channel_buffers[channel.id]), [])
        for message in (request, payload):
            self.assertEqual(tuple(map(len, self._durable_rows(message))), (1, 1))
        self.assertEqual(self._durable_rows(payload)[0][0][4], bot.ROUTE_MODE_DIRECT_PAYLOAD)
        runtime, provider = self._generation_runtime(answer)
        with runtime:
            await self._pulse(key, session["last_payload_at"] + timedelta(seconds=3.6), lambda: session["last_committed_payload_count"] == 1)
            await self._pulse(key, session["hard_deadline"] + timedelta(seconds=0.1), lambda: not session["generating"])
        provider.assert_awaited_once()
        for name in NAMES:
            self.assertIn(name, provider.await_args.args[1])
        self.assertEqual(request.replies, ["I can do that—send the list/items and I’ll run it.", answer])
        self.assertEqual(request.ack_attempts, 1)

    async def test_repeated_empty_timer_ticks_ack_once_then_expire_at_existing_lifetime(self):
        channel = self._channel(996101)
        request = self._request(channel)
        key = bot._direct_session_key(request)
        with self._capture_runtime(channel.id):
            await bot.on_message(request)
            session = bot._direct_payload_sessions[key]
            created = session["created_at"]
            await self._pulse(key, created + timedelta(seconds=10.25), lambda: request.ack_attempts == 1)
            for elapsed in (20, 39.9):
                await self._pulse(key, created + timedelta(seconds=elapsed), lambda: not session["generating"])
                self.assertIs(bot._direct_payload_sessions.get(key), session)
            await self._pulse(key, created + timedelta(seconds=40), lambda: key not in bot._direct_payload_sessions)
            payload = existing.FakeMessage(channel, "\n".join(NAMES), author=request.author)
            with mock.patch.object(bot, "_schedule_flush", new=mock.AsyncMock()):
                await bot.on_message(payload)
        self.assertEqual(request.ack_attempts, 1)
        self.assertEqual(session["payload_lines"], [])
        self.assertNotIn(key, bot._direct_payload_sessions)
        self.assertEqual(tuple(map(len, self._durable_rows(payload))), (1, 1))
        self.assertEqual(self._durable_rows(payload)[0][0][4], bot.ROUTE_MODE_NORMAL_CHAT)

    async def test_payload_during_ack_delivery_survives_existing_revision_barrier(self):
        channel = self._channel(996102)
        request = self._request(channel, hold=True)
        payload = existing.FakeMessage(channel, "\n".join(NAMES), author=request.author)
        key = bot._direct_session_key(request)
        with self._capture_runtime(channel.id):
            await bot.on_message(request)
            session = bot._direct_payload_sessions[key]
            with self._at(session["created_at"] + timedelta(seconds=10.25)):
                timer = asyncio.create_task(REAL_TIMER(key))
                try:
                    await asyncio.wait_for(request.ack_started.wait(), 2)
                    await bot.on_message(payload)
                    self.assertEqual(session["revision"], 1)
                    request.ack_release.set()
                    await self._until(lambda: not session["generating"])
                finally:
                    timer.cancel()
                    try:
                        await timer
                    except asyncio.CancelledError:
                        pass
        self.assertIs(bot._direct_payload_sessions.get(key), session)
        self.assertEqual(session["payload_lines"], [payload.content])
        self.assertEqual(tuple(map(len, self._durable_rows(payload))), (1, 1))
        self.assertNotIn(key, bot._direct_payload_capture_waiters)

    async def test_replacement_during_ack_delivery_is_never_removed(self):
        channel = self._channel(996103)
        request = self._request(channel, hold=True)
        replacement = self._request(channel)
        key = bot._direct_session_key(request)
        with self._capture_runtime(channel.id):
            await bot.on_message(request)
            original = bot._direct_payload_sessions[key]
            with self._at(original["created_at"] + timedelta(seconds=10.25)):
                timer = asyncio.create_task(REAL_TIMER(key))
                try:
                    await asyncio.wait_for(request.ack_started.wait(), 2)
                    await bot.on_message(replacement)
                    current = bot._direct_payload_sessions[key]
                    self.assertIsNot(current, original)
                    request.ack_release.set()
                    await asyncio.wait_for(timer, 2)
                    self.assertFalse(original["generating"])
                finally:
                    timer.cancel()
                    try:
                        await timer
                    except asyncio.CancelledError:
                        pass
        self.assertIs(bot._direct_payload_sessions.get(key), current)
        self.assertEqual(tuple(map(len, self._durable_rows(replacement))), (1, 1))

    async def test_ack_transport_error_keeps_captured_followup_usable(self):
        channel = self._channel(996104)
        request = self._request(channel, fail=True)
        payload = existing.FakeMessage(channel, "\n".join(NAMES), author=request.author)
        key = bot._direct_session_key(request)
        with self._capture_runtime(channel.id):
            await bot.on_message(request)
            session = bot._direct_payload_sessions[key]
            await self._pulse(key, session["created_at"] + timedelta(seconds=10.25), lambda: request.ack_attempts == 1)
            self.assertFalse(session["generating"])
            self.assertIs(bot._direct_payload_sessions.get(key), session)
            await bot.on_message(payload)
        self.assertEqual(session["payload_lines"], [payload.content])
        self.assertEqual(tuple(map(len, self._durable_rows(payload))), (1, 1))
        self.assertEqual(request.ack_attempts, 1)
        answer = "\n".join(name + ": a synthetic joke." for name in NAMES)
        runtime, provider = self._generation_runtime(answer)
        with runtime:
            await self._pulse(
                key, session["last_payload_at"] + timedelta(seconds=3.6),
                lambda: session["last_committed_payload_count"] == 1,
            )
        provider.assert_awaited_once()
        self.assertEqual(request.replies, [answer])

    async def test_cancelled_ack_retires_only_old_owner_and_preserves_original(self):
        for index, replace in enumerate((False, True)):
            with self.subTest(replace=replace):
                channel = self._channel(996107 + index)
                request = self._request(channel, hold=True)
                key = bot._direct_session_key(request)
                with self._capture_runtime(channel.id):
                    await bot.on_message(request)
                    original = bot._direct_payload_sessions[key]
                    with self._at(original["created_at"] + timedelta(seconds=10.25)):
                        timer = asyncio.create_task(REAL_TIMER(key))
                        await asyncio.wait_for(request.ack_started.wait(), 2)
                        if replace:
                            replacement = self._request(channel)
                            await bot.on_message(replacement)
                            current = bot._direct_payload_sessions[key]
                            self.assertIsNot(current, original)
                        timer.cancel()
                        with self.assertRaises(asyncio.CancelledError):
                            await timer
                    self.assertEqual(tuple(map(len, self._durable_rows(request))), (1, 1))
                    self.assertFalse(original["generating"])
                    self.assertTrue(original["completed"])
                    if replace:
                        self.assertIs(bot._direct_payload_sessions.get(key), current)
                    else:
                        self.assertNotIn(key, bot._direct_payload_sessions)
                        payload = existing.FakeMessage(channel, "\n".join(NAMES), author=request.author)
                        with mock.patch.object(bot, "_schedule_flush", new=mock.AsyncMock()):
                            await bot.on_message(payload)
                        self.assertNotIn(key, bot._direct_payload_sessions)
                        self.assertEqual(self._durable_rows(payload)[0][0][4], bot.ROUTE_MODE_NORMAL_CHAT)
                    self.assertEqual(request.replies, [])

    async def test_changed_private_payload_source_still_closes_retained_request(self):
        channel = self._channel(996106)
        request = self._request(channel)
        payload = existing.FakeMessage(channel, "\n".join(NAMES), author=request.author)
        key = bot._direct_session_key(request)
        with self._capture_runtime(channel.id):
            await bot.on_message(request)
            session = bot._direct_payload_sessions[key]
            await self._pulse(key, session["created_at"] + timedelta(seconds=10.25), lambda: request.ack_attempts == 1)
            await bot.on_message(payload)
        conn = sqlite3.connect(bot.DB_FILE)
        try:
            conn.execute("UPDATE conversations SET channel_policy='protected_system' WHERE message_id=?", (payload.id,))
            conn.commit()
        finally:
            conn.close()
        runtime, provider = self._generation_runtime("A stale answer must not be sent.")
        with runtime:
            await self._pulse(key, session["last_payload_at"] + timedelta(seconds=3.6), lambda: key not in bot._direct_payload_sessions)
        provider.assert_not_awaited()
        self.assertEqual(len(request.replies), 1)
        self.assertEqual(session["last_committed_payload_count"], 0)

    async def test_anchor_changes_after_ack_block_stale_request_and_preserve_late_payload(self):
        for index, change in enumerate(("edit", "delete", "privacy", "retract", "correction")):
            with self.subTest(change=change):
                channel = self._channel(996120 + index)
                request = self._request(channel)
                payload = existing.FakeMessage(channel, "\n".join(NAMES), author=request.author)
                key = bot._direct_session_key(request)
                with self._capture_runtime(channel.id):
                    await bot.on_message(request)
                    session = bot._direct_payload_sessions[key]
                    await self._pulse(key, session["created_at"] + timedelta(seconds=10.25), lambda: request.ack_attempts == 1)
                    self._change_original(request, change)
                    await bot.on_message(payload)
                self.assertEqual(tuple(map(len, self._durable_rows(payload))), (1, 1))
                runtime, provider = self._generation_runtime("The retired request must not be answered.")
                with runtime:
                    await self._pulse(key, session["last_payload_at"] + timedelta(seconds=3.6), lambda: key not in bot._direct_payload_sessions)
                provider.assert_not_awaited()
                self.assertTrue(session["completed"])
                self.assertEqual(session["last_committed_payload_count"], 0)
                self.assertEqual(len(request.replies), 1)

    async def test_anchor_changes_before_ack_never_prompt_for_retired_request(self):
        for index, change in enumerate(("edit", "delete", "privacy")):
            with self.subTest(change=change):
                channel = self._channel(996170 + index)
                request = self._request(channel)
                key = bot._direct_session_key(request)
                with self._capture_runtime(channel.id):
                    await bot.on_message(request)
                    session = bot._direct_payload_sessions[key]
                    self._change_original(request, change)
                    await self._pulse(key, session["created_at"] + timedelta(seconds=10.25), lambda: key not in bot._direct_payload_sessions)
                self.assertTrue(session["completed"])
                self.assertEqual(request.replies, [])
                self.assertEqual(request.ack_attempts, 0)

    async def test_anchor_edit_during_ack_closes_only_its_retained_request(self):
        channel = self._channel(996130)
        request = self._request(channel, hold=True)
        key = bot._direct_session_key(request)
        with self._capture_runtime(channel.id):
            await bot.on_message(request)
            session = bot._direct_payload_sessions[key]
            with self._at(session["created_at"] + timedelta(seconds=10.25)):
                timer = asyncio.create_task(REAL_TIMER(key, session))
                try:
                    await asyncio.wait_for(request.ack_started.wait(), 2)
                    self._change_original(request, "edit")
                    request.ack_release.set()
                    await asyncio.wait_for(timer, 2)
                finally:
                    timer.cancel()
                    try:
                        await timer
                    except asyncio.CancelledError:
                        pass
        self.assertNotIn(key, bot._direct_payload_sessions)
        self.assertTrue(session["completed"])
        self.assertEqual(tuple(map(len, self._durable_rows(request))), (1, 1))
        self.assertEqual(session["last_committed_payload_count"], 0)

    async def test_anchor_changes_during_provider_fail_final_exact_source_check(self):
        for index, change in enumerate(("edit", "delete", "privacy", "correction")):
            with self.subTest(change=change):
                channel = self._channel(996140 + index)
                request = self._request(channel)
                payload = existing.FakeMessage(channel, "\n".join(NAMES), author=request.author)
                key = bot._direct_session_key(request)
                with self._capture_runtime(channel.id):
                    await bot.on_message(request)
                    session = bot._direct_payload_sessions[key]
                    await self._pulse(key, session["created_at"] + timedelta(seconds=10.25), lambda: request.ack_attempts == 1)
                    await bot.on_message(payload)
                answer = "\n".join(name + ": a synthetic joke." for name in NAMES)
                runtime, provider = self._generation_runtime(answer)

                async def changed_before_provider_returns(*_args, **_kwargs):
                    self._change_original(request, change)
                    return answer

                provider.side_effect = changed_before_provider_returns
                with runtime:
                    await self._pulse(key, session["last_payload_at"] + timedelta(seconds=3.6), lambda: key not in bot._direct_payload_sessions)
                provider.assert_awaited_once()
                self.assertEqual(len(request.replies), 1)
                self.assertEqual(session["last_committed_payload_count"], 0)

    async def test_old_timer_after_ack_idle_does_not_adopt_replacement(self):
        channel = self._channel(996150)
        request = self._request(channel)
        replacement = self._request(channel)
        key = bot._direct_session_key(request)
        with self._capture_runtime(channel.id):
            await bot.on_message(request)
            original = bot._direct_payload_sessions[key]
            with self._at(original["created_at"] + timedelta(seconds=10.25)):
                timer = asyncio.create_task(REAL_TIMER(key, original))
                try:
                    await self._until(lambda: request.ack_attempts == 1 and not original["generating"])
                    await bot.on_message(replacement)
                    current = bot._direct_payload_sessions[key]
                    self.assertIsNot(current, original)
                    await asyncio.wait_for(timer, 2)
                finally:
                    timer.cancel()
                    try:
                        await timer
                    except asyncio.CancelledError:
                        pass
        self.assertIs(bot._direct_payload_sessions.get(key), current)
        self.assertEqual(replacement.ack_attempts, 0)
        self.assertEqual(tuple(map(len, self._durable_rows(replacement))), (1, 1))

    async def test_cancelled_anchor_validation_retires_exact_timer_request(self):
        actual_validation = bot.prompt_source_basis_failure_async
        for index, held_check in enumerate((1, 2)):
            with self.subTest(phase="before_ack" if held_check == 1 else "after_ack"):
                channel = self._channel(996160 + index)
                request = self._request(channel)
                key = bot._direct_session_key(request)
                entered, release = asyncio.Event(), asyncio.Event()
                checks = 0

                async def held_validation(*args, **kwargs):
                    nonlocal checks
                    result = await actual_validation(*args, **kwargs)
                    checks += 1
                    if checks == held_check:
                        entered.set()
                        await release.wait()
                    return result

                with self._capture_runtime(channel.id):
                    await bot.on_message(request)
                    session = bot._direct_payload_sessions[key]
                    with self._at(session["created_at"] + timedelta(seconds=10.25)), mock.patch.object(
                        bot, "prompt_source_basis_failure_async", side_effect=held_validation,
                    ):
                        timer = asyncio.create_task(REAL_TIMER(key, session))
                        try:
                            await asyncio.wait_for(entered.wait(), 2)
                            timer.cancel()
                            with self.assertRaises(asyncio.CancelledError):
                                await timer
                        finally:
                            release.set()
                            timer.cancel()
                            try:
                                await timer
                            except asyncio.CancelledError:
                                pass
                self.assertNotIn(key, bot._direct_payload_sessions)
                self.assertTrue(session["completed"])
                self.assertFalse(session["generating"])
                self.assertEqual(tuple(map(len, self._durable_rows(request))), (1, 1))
                self.assertEqual(request.ack_attempts, held_check - 1)


if __name__ == "__main__":
    unittest.main()
