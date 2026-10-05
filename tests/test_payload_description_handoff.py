"""Description-only list requests must reach the real deferred payload owner.

The parser, payload count, planner, ingress guard, SQLite originals, governed
roots, session handoff and source revalidation are production functions.
Context, transport, scheduled timers and generation use the established
coordinator fixture; no synthetic payload count is supplied.
"""

import asyncio
from datetime import datetime, timedelta, timezone
import sqlite3
import threading
import unittest
from unittest import mock

import test_conversation_batching as existing
import test_deferred_capture_ordering as ordering


bot = existing.bnl01_bot
FAILED_PHRASE = "Make one short joke for each of these fictional names."
NAMES = ("Captain Teacup", "Professor Moonboots", "Velvet Turnip")


def actual_plan(text):
    expected, _ = bot._detect_request_payload_expectation(text)
    items = bot._collect_inline_direct_payload_items(text)
    plan = bot.plan_conversation_response(
        text, "sealed_test", route_mode=bot.ROUTE_MODE_NORMAL_CHAT,
        active_channel=True, real_direct_target=True,
        channel_allows_conversation=True, batching_enabled=True,
        payload_expected=expected, payload_count=len(items),
        conversation_surface=bot.CONVERSATION_SURFACE_FREE_SPEAK_SEALED_MIRROR,
    )
    return items, plan


class PayloadDescriptionPlanTests(unittest.TestCase):
    def test_description_variants_use_actual_empty_payload_and_deferred_plan(self):
        for text in (
            FAILED_PHRASE,
            "Make one short joke for each of these musician names.",
            "Tell me something about each of those fictional characters.",
            "Write one line for each of the following made-up names.",
            "Tell me one joke for each of these fictional names; I will send the names next.",
            "Make one short joke for each of these tracks.",
            "Make one short joke for each of those songs.",
            "Make one short joke for each of these R&B songs.",
            "Make one short joke for each of these AI/ML characters.",
            "Make one joke for each of these 7:30 tracks.",
            "Make one joke for each of these https://example.invalid tracks.",
            "Tell me about these names please.",
            "Tell me about these names thanks.",
        ):
            with self.subTest(text=text):
                items, plan = actual_plan(text)
                self.assertEqual(items, [])
                self.assertTrue(plan.should_reply)
                self.assertEqual(plan.response_timing, bot.RESPONSE_TIMING_DEFERRED_PAYLOAD_SESSION)
                self.assertTrue(plan.direct_session_used)

    def test_actual_inline_names_are_preserved_and_do_not_defer(self):
        for text, expected in (
            ("Tell me about these names: Captain Teacup", ["Captain Teacup"]),
            ("Tell me about these people: Captain Teacup and Professor Moonboots", list(NAMES[:2])),
            (FAILED_PHRASE + "\n" + "\n".join(NAMES), list(NAMES)),
            ("Make one short joke for each of these fictional names: " + ", ".join(NAMES), list(NAMES)),
            ("Tell me about these names: These Fictional Names", ["These Fictional Names"]),
            ("Tell me about these names:\nThese Names\nThanks", ["These Names", "Thanks"]),
            ("Tell me about these people Chris and Pat", ["Chris", "Pat"]),
            ("Tell me about these names: Captain Teacup, Professor Moonboots", list(NAMES[:2])),
            ('Remember these names for "These Dreams"', ['"These Dreams"']),
            ("Tell me about these names Captain Teacup and Those Magnificent Men", ["Captain Teacup", "Those Magnificent Men"]),
            ("Tell me about these names Captain Teacup, These Broken Stars", ["Captain Teacup", "These Broken Stars"]),
            ("Tell me about these names These Broken Stars, Captain Teacup", ["These Broken Stars", "Captain Teacup"]),
            ("Tell me about these names Those Magnificent Men and Captain Teacup", ["Those Magnificent Men", "Captain Teacup"]),
            ("Make one short joke for each of these very strange fictional project names, These Broken Stars, Captain Teacup", ["These Broken Stars", "Captain Teacup"]),
            ("Make one joke for each of these fictional names:Captain Teacup, Professor Moonboots", list(NAMES[:2])),
        ):
            with self.subTest(text=text):
                items, plan = actual_plan(text)
                self.assertEqual(items, expected)
                self.assertNotEqual(plan.response_timing, bot.RESPONSE_TIMING_DEFERRED_PAYLOAD_SESSION)

    def test_ordinary_question_does_not_create_a_payload_waiter(self):
        for text in ("What's up?", "Why do fictional names sound funny?", "Tell me about Captain Teacup"):
            with self.subTest(text=text):
                items, plan = actual_plan(text)
                self.assertEqual(items, [])
                self.assertNotEqual(plan.response_timing, bot.RESPONSE_TIMING_DEFERRED_PAYLOAD_SESSION)


class PayloadDescriptionHandoffTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = existing.ConversationBatchCoordinatorTests.asyncSetUp
    asyncTearDown = existing.ConversationBatchCoordinatorTests.asyncTearDown
    _channel = existing.ConversationBatchCoordinatorTests._channel
    _on_message_runtime = existing.ConversationBatchCoordinatorTests._on_message_runtime
    _clear_capture_waiters = ordering.DeferredCaptureOrderingTests._clear_capture_waiters

    def setUp(self):
        self.addCleanup(self._clear_capture_waiters)

    def _durable_rows(self, message):
        conn = sqlite3.connect(bot.DB_FILE)
        try:
            originals = conn.execute(
                "SELECT id,content,channel_policy,channel_id,route_mode FROM conversations WHERE message_id=? AND role='user'",
                (message.id,),
            ).fetchall()
            roots = conn.execute(
                "SELECT source_row_id,normalized_value,channel_policy,channel_id,visibility,public_usable,derived,projection "
                "FROM memory_ledger_entries WHERE source_message_id=? AND source_table='conversations' AND source_role='user'",
                (message.id,),
            ).fetchall()
            return originals, roots
        finally:
            conn.close()

    def _capture_runtime(self, channel_id, *, policy="sealed_test", capture=None):
        actual_capture = capture or bot.save_user_message
        actual_target = bot.is_direct_bnl_target
        stack = self._on_message_runtime(channel_id)
        stack.enter_context(mock.patch.object(bot, "is_direct_bnl_target", side_effect=actual_target))
        stack.enter_context(mock.patch.object(bot, "resolve_channel_policy", return_value=policy))
        stack.enter_context(mock.patch.object(bot, "save_user_message", side_effect=actual_capture))
        stack.enter_context(mock.patch.object(bot, "memory_ledger_shadow_enabled", return_value=True))
        stack.enter_context(mock.patch.object(bot, "_direct_session_timer", new=mock.AsyncMock()))
        provider = mock.AsyncMock(side_effect=AssertionError("ingress must not call provider"))
        stack.enter_context(mock.patch.object(bot, "get_gemini_response", new=provider))
        stack.enter_context(mock.patch.object(bot, "get_gemini_response_with_optional_typing", new=provider))
        return stack

    def _request_and_payload(self, channel_id):
        channel = self._channel(channel_id)
        mention = existing.SimpleNamespace(id=999, display_name="BNL-01", bot=True)
        request = existing.FakeMessage(channel, "<@999> " + FAILED_PHRASE, mentions=[mention])
        payload = existing.FakeMessage(channel, "\n".join(NAMES), author=request.author)
        return channel, request, payload, bot._direct_session_key(request)

    async def test_followup_original_and_root_are_captured_once_by_existing_owner(self):
        channel = self._channel(992006)
        request = existing.FakeMessage(channel, "BNL, " + FAILED_PHRASE)
        payload = existing.FakeMessage(channel, "\n".join(NAMES), author=request.author)
        with self._capture_runtime(channel.id):
            await bot.on_message(request)
            await bot.on_message(payload)
        anchor_rows, anchor_roots = self._durable_rows(request)
        payload_rows, payload_roots = self._durable_rows(payload)
        self.assertEqual((len(anchor_rows), len(anchor_roots)), (1, 1))
        self.assertEqual((len(payload_rows), len(payload_roots)), (1, 1))
        self.assertEqual(payload_rows[0][1:], (payload.content, "sealed_test", channel.id, bot.ROUTE_MODE_DIRECT_PAYLOAD))
        self.assertEqual(payload_roots[0][0], str(payload_rows[0][0]))
        self.assertEqual(payload_roots[0][1:4], (payload.content, "sealed_test", channel.id))
        self.assertEqual(payload_roots[0][5:], (0, 0, 0))

    async def test_followups_keep_existing_current_turn_policy_and_root_visibility(self):
        for index, policy in enumerate(("public_home", "sealed_test", "unknown", "internal_controlled")):
            with self.subTest(policy=policy):
                channel, request, payload, key = self._request_and_payload(992040 + index)
                with self._capture_runtime(channel.id, policy=policy):
                    await bot.on_message(request)
                    await bot.on_message(payload)
                rows, roots = self._durable_rows(payload)
                self.assertEqual(len(rows), 1)
                if policy in {"public_home", "sealed_test"}:
                    self.assertEqual(len(roots), 1)
                else:
                    self.assertLessEqual(len(roots), 1)
                self.assertEqual(rows[0][1:], (payload.content, policy, channel.id, bot.ROUTE_MODE_DIRECT_PAYLOAD))
                for root in roots:
                    self.assertEqual(root[0], str(rows[0][0]))
                    self.assertEqual(root[1:4], (payload.content, policy, channel.id))
                    self.assertEqual(root[5], int(policy == "public_home"))
                    self.assertEqual(root[6:], (0, 0))
                self.assertEqual(bot._direct_payload_sessions[key]["payload_lines"], [payload.content])

    async def test_public_session_cannot_accept_now_sealed_or_protected_followup(self):
        for index, policy in enumerate(("sealed_test", "protected_system")):
            with self.subTest(policy=policy):
                channel, request, payload, key = self._request_and_payload(992022 + index)
                with self._capture_runtime(channel.id, policy="public_home"):
                    await bot.on_message(request)
                    old = bot._direct_payload_sessions[key]
                    with mock.patch.object(bot, "resolve_channel_policy", return_value=policy):
                        await bot.on_message(payload)
                rows, roots = self._durable_rows(payload)
                if policy == "sealed_test":
                    self.assertEqual((len(rows), len(roots)), (1, 1))
                    self.assertEqual(rows[0][2], policy)
                    self.assertEqual(roots[0][2], policy)
                    self.assertEqual(roots[0][5], 0)
                else:
                    self.assertEqual((rows, roots), ([], []))
                self.assertEqual(old["payload_lines"], [])
                self.assertNotIn(key, bot._direct_payload_sessions)
                self.assertEqual(channel.sent + request.replies + payload.replies, [])

    async def test_followup_message_id_deduplicates_but_identical_text_new_id_is_a_new_original(self):
        channel, request, payload, key = self._request_and_payload(992007)
        distinct = existing.FakeMessage(channel, payload.content, author=payload.author)
        with self._capture_runtime(channel.id):
            await bot.on_message(request)
            await bot.on_message(request)
            await bot.on_message(payload)
            await bot.on_message(payload)
            await bot.on_message(distinct)
        for item in (request, payload, distinct):
            rows, roots = self._durable_rows(item)
            self.assertEqual((len(rows), len(roots)), (1, 1))
        self.assertEqual(bot._direct_payload_sessions[key]["payload_lines"], [payload.content, distinct.content])
        self.assertEqual(channel.sent + request.replies, [])

    async def test_followup_capture_failure_does_not_accept_or_commit_payload(self):
        channel, request, payload, key = self._request_and_payload(992008)
        with self._capture_runtime(channel.id):
            await bot.on_message(request)
            conn = sqlite3.connect(bot.DB_FILE)
            try:
                conn.execute(
                    "CREATE TRIGGER neutral_payload_rejection BEFORE INSERT ON conversations "
                    f"WHEN NEW.message_id={int(payload.id)} BEGIN SELECT RAISE(ABORT, 'neutral payload rejection'); END"
                )
                conn.commit()
            finally:
                conn.close()
            try:
                await bot.on_message(payload)
            except sqlite3.DatabaseError:
                pass  # Existing transport may propagate the real write error.
        rows, roots = self._durable_rows(payload)
        self.assertEqual((rows, roots), ([], []))
        session = bot._direct_payload_sessions.get(key)
        if session is not None:
            self.assertEqual(session["payload_lines"], [])
            self.assertEqual(session["last_committed_payload_count"], 0)
        self.assertNotIn(key, bot._direct_payload_capture_waiters)
        self.assertEqual(channel.sent + request.replies + payload.replies, [])

    async def test_new_request_after_postcommit_error_reuses_original_without_duplicate(self):
        channel, request, payload, key = self._request_and_payload(992024)
        actual_capture = bot.save_user_message
        failed = False

        def fail_once_after_real_commit(*args, **kwargs):
            nonlocal failed
            result = actual_capture(*args, **kwargs)
            if kwargs.get("message_id") == payload.id and not failed:
                failed = True
                raise sqlite3.OperationalError("neutral failure after completed capture")
            return result

        with self._capture_runtime(channel.id, capture=fail_once_after_real_commit):
            await bot.on_message(request)
            try:
                await bot.on_message(payload)
            except sqlite3.DatabaseError:
                pass
            self.assertTrue(failed)
            self.assertNotIn(key, bot._direct_payload_sessions)
            self.assertEqual(tuple(map(len, self._durable_rows(payload))), (1, 1))
            replacement = existing.FakeMessage(channel, request.content, author=request.author, mentions=request.mentions)
            await bot.on_message(replacement)
            await bot.on_message(payload)
        self.assertEqual(tuple(map(len, self._durable_rows(payload))), (1, 1))
        self.assertEqual(bot._direct_payload_sessions[key]["payload_lines"], [payload.content])
        self.assertEqual(request.replies + payload.replies + channel.sent, [])

    async def test_timer_waits_for_real_followup_capture_before_generation(self):
        channel, request, payload, key = self._request_and_payload(992009)
        entered, release = asyncio.Event(), threading.Event()
        loop = asyncio.get_running_loop()
        actual_capture, actual_timer = bot.save_user_message, bot._direct_session_timer

        def held_capture(*args, **kwargs):
            if kwargs.get("message_id") == payload.id:
                loop.call_soon_threadsafe(entered.set)
                if not release.wait(5):
                    raise TimeoutError("bounded followup capture hold")
            return actual_capture(*args, **kwargs)

        generation = mock.AsyncMock(side_effect=asyncio.CancelledError)
        with self._capture_runtime(channel.id, capture=held_capture):
            await bot.on_message(request)
            followup = asyncio.create_task(bot.on_message(payload))
            timer = None
            try:
                await asyncio.wait_for(entered.wait(), 3)
                session = bot._direct_payload_sessions[key]
                self.assertEqual(session["payload_lines"], [])
                self.assertEqual(self._durable_rows(payload), ([], []))
                session["hard_deadline"] = datetime.now(timezone.utc) - timedelta(seconds=1)
                with mock.patch.object(bot, "_generate_direct_payload_session", new=generation):
                    timer = asyncio.create_task(actual_timer(key))
                    for _ in range(8):
                        await asyncio.sleep(0)
                    generation.assert_not_awaited()
                    self.assertFalse(timer.done())
                    release.set()
                    await asyncio.wait_for(followup, 3)
                    bot._direct_payload_sessions[key]["hard_deadline"] = datetime.now(timezone.utc) - timedelta(seconds=1)
                    with self.assertRaises(asyncio.CancelledError):
                        await asyncio.wait_for(timer, 3)
            finally:
                release.set()
                await asyncio.wait_for(followup, 3)
                if timer is not None and not timer.done():
                    timer.cancel()
        generation.assert_awaited_once()
        self.assertEqual(tuple(map(len, self._durable_rows(payload))), (1, 1))
        self.assertEqual(bot._direct_payload_sessions[key]["payload_lines"], [payload.content])

    async def test_completed_capture_cannot_append_to_replaced_session(self):
        channel, request, payload, key = self._request_and_payload(992010)
        replacement = existing.FakeMessage(channel, "BNL, tell me about these names", author=request.author)
        entered, release = asyncio.Event(), threading.Event()
        loop, actual_capture = asyncio.get_running_loop(), bot.save_user_message

        def held_capture(*args, **kwargs):
            if kwargs.get("message_id") == payload.id:
                loop.call_soon_threadsafe(entered.set)
                if not release.wait(5):
                    raise TimeoutError("bounded replacement capture hold")
            return actual_capture(*args, **kwargs)

        with self._capture_runtime(channel.id, capture=held_capture):
            await bot.on_message(request)
            old_session = bot._direct_payload_sessions[key]
            followup = asyncio.create_task(bot.on_message(payload))
            try:
                await asyncio.wait_for(entered.wait(), 3)
                # Use the existing replacement owner rather than substituting
                # a hand-made session dictionary while the worker is running.
                new_session = bot._start_direct_payload_session(
                    replacement, channel_policy="sealed_test",
                    request_text=replacement.content, current_turn_context="",
                )
            finally:
                release.set()
                await asyncio.wait_for(followup, 3)
        self.assertIs(bot._direct_payload_sessions[key], new_session)
        self.assertEqual(old_session["payload_lines"], [])
        self.assertEqual(new_session["payload_lines"], [])
        self.assertEqual(tuple(map(len, self._durable_rows(payload))), (1, 1))
        self.assertEqual(channel.sent + request.replies + replacement.replies, [])

    async def test_cancelled_followup_drains_real_capture_without_accepting_payload(self):
        channel, request, payload, key = self._request_and_payload(992011)
        entered, finished, release = asyncio.Event(), asyncio.Event(), threading.Event()
        loop, actual_capture = asyncio.get_running_loop(), bot.save_user_message

        def held_capture(*args, **kwargs):
            if kwargs.get("message_id") != payload.id:
                return actual_capture(*args, **kwargs)
            loop.call_soon_threadsafe(entered.set)
            try:
                if not release.wait(5):
                    raise TimeoutError("bounded cancelled capture hold")
                return actual_capture(*args, **kwargs)
            finally:
                loop.call_soon_threadsafe(finished.set)

        with self._capture_runtime(channel.id, capture=held_capture):
            await bot.on_message(request)
            followup = asyncio.create_task(bot.on_message(payload))
            try:
                await asyncio.wait_for(entered.wait(), 3)
                followup.cancel()
                for _ in range(8):
                    await asyncio.sleep(0)
                self.assertFalse(followup.done(), "capture handoff must not release a still-running writer")
            finally:
                release.set()
                await asyncio.wait_for(finished.wait(), 3)
                await asyncio.gather(followup, return_exceptions=True)
        self.assertTrue(followup.cancelled())
        session = bot._direct_payload_sessions.get(key)
        if session is not None:
            self.assertEqual(session["payload_lines"], [])
            self.assertEqual(session["last_committed_payload_count"], 0)
        self.assertEqual(tuple(map(len, self._durable_rows(payload))), (1, 1))
        self.assertNotIn(key, bot._direct_payload_capture_waiters)
        self.assertEqual(channel.sent + request.replies + payload.replies, [])

    async def test_failed_phrase_real_capture_creates_session_and_collects_same_author_list(self):
        await self._capture_and_deliver_tagged_request(992001, FAILED_PHRASE)

    async def test_prior_future_clause_fixture_also_hands_off_and_delivers_once(self):
        await self._capture_and_deliver_tagged_request(
            992005,
            "Tell me one joke for each of these fictional names; I will send the names next.",
        )

    async def _capture_and_deliver_tagged_request(self, channel_id, phrase):
        channel = self._channel(channel_id)
        mention = existing.SimpleNamespace(id=999, display_name="BNL-01", bot=True)
        request = existing.FakeMessage(channel, "<@999> " + phrase, mentions=[mention])
        payload = existing.FakeMessage(channel, "\n".join(NAMES), author=request.author)
        key = bot._direct_session_key(request)
        real_capture = bot.save_user_message
        real_direct_target = bot.is_direct_bnl_target
        provider = mock.AsyncMock(side_effect=AssertionError("generation must wait for session payload"))
        with (
            self._on_message_runtime(channel.id, followup_candidate=True),
            mock.patch.object(bot, "is_direct_bnl_target", side_effect=real_direct_target),
            mock.patch.object(bot, "save_user_message", side_effect=real_capture),
            mock.patch.object(bot, "_direct_session_timer", new=mock.AsyncMock()),
            mock.patch.object(bot, "get_gemini_response", new=provider),
            mock.patch.object(bot, "get_gemini_response_with_optional_typing", new=provider),
        ):
            await bot.on_message(request)
            self.assertIn(key, bot._direct_payload_sessions)
            self.assertEqual(bot._direct_payload_sessions[key]["payload_lines"], [])
            await bot.on_message(payload)
        self.assertEqual(bot._direct_payload_sessions[key]["payload_lines"], [payload.content])
        self.assertEqual(list(bot._channel_buffers[channel.id]), [])
        self.assertNotIn(key, bot._direct_payload_capture_waiters)
        self.assertEqual(channel.sent + request.replies + payload.replies, [])
        provider.assert_not_awaited()
        conn = sqlite3.connect(bot.DB_FILE)
        try:
            captured = conn.execute("SELECT content,route_mode FROM conversations WHERE message_id=?", (request.id,)).fetchall()
        finally:
            conn.close()
        self.assertEqual(captured, [(phrase, bot.ROUTE_MODE_DIRECT_PAYLOAD)])
        await self._deliver_collected_names_once(key, request)

    async def _deliver_collected_names_once(self, key, request, *, before_provider_return=None, expect_delivery=True):
        answer = "\n".join(name + ": a synthetic joke." for name in NAMES)
        async def provider_answer(*_args, **_kwargs):
            if before_provider_return is not None:
                await asyncio.sleep(0)
                before_provider_return()
            return answer
        provider = mock.AsyncMock(side_effect=provider_answer)
        session = bot._direct_payload_sessions[key]
        with (
            mock.patch.object(bot, "build_room_first_direct_context", return_value=""),
            mock.patch.object(bot, "maybe_build_bnl_read_model_context", return_value=""),
            mock.patch.object(bot, "maybe_build_source_context_for_direct_message", new=mock.AsyncMock(return_value="")),
            mock.patch.object(bot, "build_user_aware_prompt", side_effect=lambda *_args, **_kw: (_args[3], False, "balanced")) as prompt_builder,
            mock.patch.object(bot, "log_response_style"),
            mock.patch.object(bot, "_apply_direct_response_pacing", new=mock.AsyncMock()),
            mock.patch.object(bot, "get_gemini_response_with_optional_typing", new=provider),
            mock.patch.object(bot, "suppress_stale_media_fallback", side_effect=lambda response, **_kw: response),
            mock.patch.object(bot, "apply_guarded_response_regeneration", new=mock.AsyncMock(return_value=(answer, {"suppressed": False}))),
            mock.patch.object(bot, "is_privileged_member", return_value=False),
            mock.patch.object(bot, "DIRECT_PRE_SEND_GRACE_SECONDS", 0),
            mock.patch.object(bot, "exact_quote_presend_failure", new=mock.AsyncMock(return_value="")),
            mock.patch.object(bot, "model_response_persistence_allowed_with_website_context", return_value=False),
            mock.patch.object(bot, "persist_bnl_self_name_decision_after_send_async", new=mock.AsyncMock()),
            mock.patch.object(bot, "record_unified_response_assessment_shadow_after_send", new=mock.AsyncMock()),
        ):
            await bot._generate_direct_payload_session(key, "quiet_timeout")
            if not expect_delivery:
                provider.assert_awaited_once()
                self.assertEqual(request.replies + request.channel.sent, [])
                self.assertEqual(session["last_committed_payload_count"], 0)
                return
            self.assertEqual(session["last_committed_payload_count"], 1)
            self.assertFalse(session["generating"])
            # The real timer must recognize its committed snapshot at the next
            # hard deadline, without invoking generation/delivery a second time.
            session["hard_deadline"] = datetime.now(timezone.utc) - timedelta(seconds=1)
            with mock.patch.object(bot.asyncio, "sleep", side_effect=asyncio.CancelledError):
                with self.assertRaises(asyncio.CancelledError):
                    await bot._direct_session_timer(key)
        provider.assert_awaited_once()
        prompt_builder.assert_called_once()
        self.assertIn(bot._direct_payload_sessions[key]["original_request_text"], prompt_builder.call_args.args[3])
        for name in NAMES:
            self.assertIn(name, provider.await_args.args[1])
        self.assertIn(bot._direct_payload_sessions[key]["original_request_text"], provider.await_args.args[1])
        self.assertEqual(request.replies, [answer])
        self.assertEqual(request.channel.sent, [])

    async def test_each_captured_payload_source_revalidates_even_when_room_context_is_empty(self):
        for index, change in enumerate(("delete", "edit", "privacy", "retract", "correction")):
            with self.subTest(change=change):
                channel, request, payload, key = self._request_and_payload(992050 + index)
                with self._capture_runtime(channel.id):
                    await bot.on_message(request)
                    await bot.on_message(payload)
                self.assertEqual(tuple(map(len, self._durable_rows(payload))), (1, 1))

                def mutate_original():
                    conn = sqlite3.connect(bot.DB_FILE)
                    try:
                        if change == "delete":
                            conn.execute("DELETE FROM conversations WHERE message_id=?", (payload.id,))
                        elif change == "edit":
                            conn.execute("UPDATE conversations SET content=? WHERE message_id=?", ("Corrected synthetic payload", payload.id))
                        elif change == "privacy":
                            conn.execute("UPDATE conversations SET channel_policy='protected_system' WHERE message_id=?", (payload.id,))
                        elif change == "retract":
                            conn.execute("UPDATE memory_ledger_entries SET lifecycle_status='retracted' WHERE source_message_id=? AND source_table='conversations'", (payload.id,))
                        else:
                            conn.execute(
                                "INSERT INTO memory_ledger_lineage(entry_id,guild_id,lineage_type,target_entry_id,created_at) "
                                "SELECT ?,guild_id,'correction_of',entry_id,? FROM memory_ledger_entries "
                                "WHERE source_message_id=? AND source_table='conversations'",
                                ("synthetic-correction", datetime.now(timezone.utc).isoformat(), payload.id),
                            )
                        conn.commit()
                    finally:
                        conn.close()

                # The existing context fixture deliberately selects no room
                # rows. Every captured payload still needs its own exact basis.
                await self._deliver_collected_names_once(
                    key, request, before_provider_return=mutate_original,
                    expect_delivery=False,
                )

    async def test_new_capture_at_final_source_check_aborts_old_send_without_losing_session(self):
        channel, request, payload, key = self._request_and_payload(992034)
        later = existing.FakeMessage(channel, "Captain Sundial", author=request.author)
        entered, release = asyncio.Event(), threading.Event()
        loop = asyncio.get_running_loop()
        actual_capture = bot.save_user_message
        actual_validation = bot.prompt_source_basis_failure_async
        followup = None
        seen_captured_checks = 0

        def held_capture(*args, **kwargs):
            if kwargs.get("message_id") == later.id:
                loop.call_soon_threadsafe(entered.set)
                if not release.wait(5):
                    raise TimeoutError("bounded final-send capture hold")
            return actual_capture(*args, **kwargs)

        with self._capture_runtime(channel.id, capture=held_capture):
            await bot.on_message(request)
            await bot.on_message(payload)
            session = bot._direct_payload_sessions[key]
            bases = tuple(session["payload_source_bases"])
            self.assertTrue(bases)

            async def validate_then_interleave(candidate_bases, *args, **kwargs):
                nonlocal followup, seen_captured_checks
                result = await actual_validation(candidate_bases, *args, **kwargs)
                if tuple(candidate_bases) == bases:
                    seen_captured_checks += 1
                    if seen_captured_checks == 2:
                        followup = asyncio.create_task(bot.on_message(later))
                        await asyncio.wait_for(entered.wait(), 3)
                return result

            try:
                with mock.patch.object(bot, "prompt_source_basis_failure_async", side_effect=validate_then_interleave):
                    await self._deliver_collected_names_once(key, request, expect_delivery=False)
                self.assertEqual(seen_captured_checks, 2)
                self.assertIs(bot._direct_payload_sessions[key], session)
                self.assertEqual(session["payload_lines"], [payload.content])
            finally:
                release.set()
                if followup is not None:
                    await asyncio.wait_for(followup, 3)
        self.assertEqual(session["payload_lines"], [payload.content, later.content])
        self.assertEqual(session["last_committed_payload_count"], 0)
        self.assertEqual(tuple(map(len, self._durable_rows(later))), (1, 1))
        self.assertEqual(channel.sent + request.replies, [])

    async def test_real_description_parser_reserves_ingress_while_capture_is_in_flight(self):
        channel = self._channel(992002)
        request = existing.FakeMessage(channel, "BNL, " + FAILED_PHRASE)
        payload = existing.FakeMessage(channel, "\n".join(NAMES), author=request.author)
        key = bot._direct_session_key(request)
        entered, looked_up = asyncio.Event(), asyncio.Event()
        release = threading.Event()
        loop = asyncio.get_running_loop()
        real_capture, real_key = bot.save_user_message, bot._direct_session_key

        def delayed_capture(*args, **kwargs):
            result = real_capture(*args, **kwargs)
            if kwargs.get("message_id") == request.id:
                loop.call_soon_threadsafe(entered.set)
                if not release.wait(5):
                    raise TimeoutError("bounded neutral capture wait")
            return result

        def observed_key(message):
            if message is payload:
                looked_up.set()
            return real_key(message)

        with (
            self._on_message_runtime(channel.id, followup_candidate=True),
            mock.patch.object(bot, "save_user_message", side_effect=delayed_capture),
            mock.patch.object(bot, "_direct_session_key", side_effect=observed_key),
            mock.patch.object(bot, "_direct_session_timer", new=mock.AsyncMock()),
            mock.patch.object(bot, "get_gemini_response", new=mock.AsyncMock(return_value="Synthetic baseline failure response")),
            mock.patch.object(bot, "get_gemini_response_with_optional_typing", new=mock.AsyncMock(return_value="Synthetic baseline failure response")),
        ):
            first = asyncio.create_task(bot.on_message(request))
            second = None
            try:
                await asyncio.wait_for(entered.wait(), 3)
                self.assertIn(key, bot._direct_payload_capture_waiters)
                self.assertNotIn(key, bot._direct_payload_sessions)
                second = asyncio.create_task(bot.on_message(payload))
                await asyncio.wait_for(looked_up.wait(), 3)
                for _ in range(8):
                    await asyncio.sleep(0)
                self.assertFalse(second.done())
            finally:
                release.set()
                await asyncio.wait_for(first, 3)
                if second is not None:
                    await asyncio.wait_for(second, 3)
        self.assertEqual(bot._direct_payload_sessions[key]["payload_lines"], [payload.content])
        self.assertNotIn(key, bot._direct_payload_capture_waiters)

    async def test_other_author_and_channel_cannot_supply_requesters_payload(self):
        channel, other_channel = self._channel(992003), self._channel(992004)
        request = existing.FakeMessage(channel, "BNL, " + FAILED_PHRASE)
        outsiders = (
            existing.FakeMessage(channel, "Other Author Name", author=existing.FakeAuthor(101, "Other Member")),
            existing.FakeMessage(other_channel, "Other Channel Name", author=request.author),
        )
        own = existing.FakeMessage(channel, "\n".join(NAMES), author=request.author)
        key = bot._direct_session_key(request)
        with self._capture_runtime(channel.id):
            await bot.on_message(request)
            self.assertIn(key, bot._direct_payload_sessions)
            for outsider in outsiders:
                await bot.on_message(outsider)
                self.assertEqual(bot._direct_payload_sessions[key]["payload_lines"], [])
            await bot.on_message(own)
        self.assertEqual(bot._direct_payload_sessions[key]["payload_lines"], [own.content])
        self.assertEqual(request.replies + own.replies, [])


if __name__ == "__main__":
    unittest.main()
