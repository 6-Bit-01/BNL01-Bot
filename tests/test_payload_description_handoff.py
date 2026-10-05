"""Description-only list requests must reach the real deferred payload owner.

The parser, payload count, planner, ingress guard, SQLite anchor capture and
session handoff are production functions. The existing active-session owner
collects followups in the session before the ordinary durable capture branch.
Context, transport, timers and generation use the established coordinator
fixture; no synthetic payload count is supplied.
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

    async def _deliver_collected_names_once(self, key, request):
        answer = "\n".join(name + ": a synthetic joke." for name in NAMES)
        provider = mock.AsyncMock(return_value=answer)
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
            mock.patch.object(bot, "prompt_source_basis_failure", return_value=""),
            mock.patch.object(bot, "model_response_persistence_allowed_with_website_context", return_value=False),
            mock.patch.object(bot, "persist_bnl_self_name_decision_after_send_async", new=mock.AsyncMock()),
            mock.patch.object(bot, "record_unified_response_assessment_shadow_after_send", new=mock.AsyncMock()),
        ):
            await bot._generate_direct_payload_session(key, "quiet_timeout")
            session = bot._direct_payload_sessions[key]
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
        with (
            self._on_message_runtime(channel.id, followup_candidate=True),
            mock.patch.object(bot, "_direct_session_timer", new=mock.AsyncMock()),
            mock.patch.object(bot, "get_gemini_response", new=mock.AsyncMock(return_value="Synthetic baseline failure response")),
            mock.patch.object(bot, "get_gemini_response_with_optional_typing", new=mock.AsyncMock(return_value="Synthetic baseline failure response")),
        ):
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
