"""Completed follow-up interpretation retains Discord scope and fails quietly."""
import asyncio
from contextlib import ExitStack
from types import SimpleNamespace
import unittest
from unittest import mock

import test_ordinary_addressed_burst_ingress as fixtures

bot = fixtures.bot


class CompletedFollowupScopeTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.guild = SimpleNamespace(id=42)
        self.author = SimpleNamespace(id=7)
        self.channel = SimpleNamespace(id=100, guild=self.guild)
        self.message = SimpleNamespace(id=30, guild=self.guild, channel=self.channel,
                                       author=self.author, content="Could you explain that answer?")
        self.stack.enter_context(mock.patch.object(bot, "client", SimpleNamespace(user=SimpleNamespace(id=999))))
        self.stack.enter_context(mock.patch.object(bot, "_conversation_continuation_state", {}))
        self.stack.enter_context(mock.patch.object(bot, "_recent_direct_response_window", {}))
        self.policy = self.stack.enter_context(mock.patch.object(bot, "resolve_channel_policy", return_value="public_context"))
        self.stack.enter_context(mock.patch.object(bot, "_completed_exchange_originals_available", return_value=True))
        self.sources = {
            10: self.source(10, 7, "Please explain the fictional recording."),
            20: self.source(20, 999, "The fictional recording contains two sections."),
        }
        self.channel.fetch_message = mock.AsyncMock(side_effect=lambda mid: self.sources[mid])
        self.mark()
        self.key = bot._conversation_state_key(42, 100, 7)
        self.state = bot._conversation_continuation_state[self.key]

    def source(self, message_id, author_id, content):
        return SimpleNamespace(id=message_id, author=SimpleNamespace(id=author_id),
                               channel=self.channel, guild=self.guild, content=content)

    def mark(self, request_id=10, reply_id=20):
        bot._mark_conversation_continuation_state(
            42, 100, 7, channel_policy="public_context",
            request_message_ids=(request_id,), reply_message_ids=(reply_id,),
        )

    async def resolve(self):
        return await bot._resolve_completed_followup_addressing(
            self.message, self.message.content, "public_context")

    async def test_loader_reads_only_exact_member_and_bot_exchange(self):
        exchange = await bot._load_completed_followup_exchange(self.message, self.state)
        self.assertEqual(exchange, {
            "previous_user": "Please explain the fictional recording.",
            "bnl_reply": "The fictional recording contains two sections.",
        })
        self.assertEqual(self.channel.fetch_message.await_args_list, [mock.call(10), mock.call(20)])

    async def test_semantic_result_carries_the_validated_exchange_without_another_call(self):
        output = {}
        with mock.patch.object(bot, "_classify_completed_followup_exchange", new=mock.AsyncMock(return_value=True)) as classifier:
            self.assertTrue(await bot._resolve_completed_followup_addressing(
                self.message, self.message.content, "public_context", result_out=output))
        classifier.assert_awaited_once()
        exchange = output["exchange"]
        self.assertEqual(exchange.request_message_ids, (10,))
        self.assertEqual(exchange.reply_message_ids, (20,))
        self.assertEqual(exchange.user_id, 7)
        self.assertEqual(exchange.channel_policy, "public_context")

    async def test_ineligible_original_does_not_reach_semantic_provider(self):
        with mock.patch.object(bot, "_completed_exchange_originals_available", return_value=False):
            with mock.patch.object(bot, "_classify_completed_followup_exchange", new=mock.AsyncMock()) as classifier:
                self.assertFalse(await self.resolve())
        classifier.assert_not_awaited()

    async def test_selected_continuation_keeps_bounded_root_and_answer_lineage(self):
        for index in range(1, 7):
            state = bot._conversation_continuation_state[self.key]
            bot._mark_conversation_continuation_state(
                42, 100, 7, channel_policy="public_context",
                request_message_ids=(10 + index,), reply_message_ids=(20 + index,),
                continuation_request_message_ids=state.get("request_lineage_message_ids", (10,)),
                continuation_reply_message_ids=state.get("reply_lineage_message_ids", (20,)),
            )
        state = bot._conversation_continuation_state[self.key]
        self.assertEqual(state["request_lineage_message_ids"], (10, 14, 15, 16))
        self.assertEqual(state["reply_lineage_message_ids"], (20, 24, 25, 26))
        self.assertEqual(state["request_message_ids"], (16,))
        self.mark(99, 199)
        self.assertEqual(state["request_lineage_message_ids"], (99,))
        self.assertEqual(state["reply_lineage_message_ids"], (199,))

    async def test_initial_oversized_exchange_keeps_original_and_last_three_lineage(self):
        bot._mark_conversation_continuation_state(
            42, 100, 7, channel_policy="public_context",
            request_message_ids=(11, 12, 13, 14, 15, 16),
            reply_message_ids=(21, 22, 23, 24, 25, 26),
        )
        state = bot._conversation_continuation_state[self.key]
        self.assertEqual(state["request_message_ids"], (13, 14, 15, 16))
        self.assertEqual(state["reply_message_ids"], (23, 24, 25, 26))
        self.assertEqual(state["request_lineage_message_ids"], (11, 14, 15, 16))
        self.assertEqual(state["reply_lineage_message_ids"], (21, 24, 25, 26))

    async def test_initial_oversized_exchange_keeps_no_store_and_hashes_by_lineage_id(self):
        bot._mark_conversation_continuation_state(
            42, 100, 7, channel_policy="public_context",
            request_message_ids=(11, 12, 13, 14, 15, 16),
            reply_message_ids=(21, 22, 23, 24, 25, 26),
            no_store_reply_message_ids=(21, 22, 23, 24, 25, 26),
            reply_message_digests=(
                (26, "digest-26"), (24, "digest-24"), (22, "digest-22"),
                (21, "digest-21"), (25, "digest-25"), (23, "digest-23"),
            ),
        )
        state = bot._conversation_continuation_state[self.key]
        self.assertEqual(state["no_store_reply_message_ids"], (21, 24, 25, 26))
        self.assertEqual(state["reply_message_digests"], (
            (21, "digest-21"), (24, "digest-24"),
            (25, "digest-25"), (26, "digest-26"),
        ))

    async def test_loader_rejects_wrong_author_channel_guild_or_message_id(self):
        for message_id, author_id in ((10, 7), (20, 999)):
            for field in ("author", "channel", "guild", "id"):
                with self.subTest(message_id=message_id, field=field):
                    original = self.sources[message_id]
                    altered = self.source(message_id, author_id, "Out-of-scope fictional text.")
                    setattr(altered, field, 12345 if field == "id" else SimpleNamespace(id=12345))
                    self.sources[message_id] = altered
                    try:
                        self.assertIsNone(await bot._load_completed_followup_exchange(self.message, self.state))
                    finally:
                        self.sources[message_id] = original

    async def test_missing_reference_ids_do_not_fetch_or_classify(self):
        classifier = self.stack.enter_context(mock.patch.object(
            bot, "_classify_completed_followup_exchange", new=mock.AsyncMock(return_value=True)))
        for field in ("request_message_ids", "reply_message_ids"):
            with self.subTest(field=field):
                saved = self.state[field]
                self.state[field] = ()
                self.assertFalse(await self.resolve())
                self.state[field] = saved
        self.channel.fetch_message.assert_not_awaited()
        classifier.assert_not_awaited()

    async def test_unavailable_discord_reference_fails_quiet_without_classifier(self):
        self.channel.fetch_message.side_effect = RuntimeError("Fictional reference unavailable")
        with mock.patch.object(bot, "_classify_completed_followup_exchange", new=mock.AsyncMock()) as classifier:
            self.assertFalse(await self.resolve())
        classifier.assert_not_awaited()

    async def test_same_state_new_completed_reply_invalidates_inflight_result(self):
        async def classify(_exchange, _content):
            self.mark(request_id=11, reply_id=21)
            return True

        with mock.patch.object(bot, "_classify_completed_followup_exchange", new=mock.AsyncMock(side_effect=classify)):
            self.assertFalse(await self.resolve())

    async def test_policy_change_during_reference_fetch_never_reaches_classifier(self):
        def fetch(message_id):
            if message_id == 20:
                self.policy.return_value = "sealed_test"
            return self.sources[message_id]

        self.channel.fetch_message.side_effect = fetch
        with mock.patch.object(bot, "_classify_completed_followup_exchange", new=mock.AsyncMock()) as classifier:
            self.assertFalse(await self.resolve())
        classifier.assert_not_awaited()

    async def test_replaced_state_invalidates_inflight_result(self):
        async def classify(_exchange, _content):
            bot._conversation_continuation_state[self.key] = dict(self.state)
            return True

        with mock.patch.object(bot, "_classify_completed_followup_exchange", new=mock.AsyncMock(side_effect=classify)):
            self.assertFalse(await self.resolve())

    async def test_edited_current_message_invalidates_inflight_result(self):
        async def classify(_exchange, _content):
            self.message.content = "This fictional question was withdrawn."
            return True

        with mock.patch.object(bot, "_classify_completed_followup_exchange", new=mock.AsyncMock(side_effect=classify)):
            self.assertFalse(await self.resolve())

    async def test_policy_change_invalidates_inflight_result(self):
        async def classify(_exchange, _content):
            self.policy.return_value = "sealed_test"
            return True

        with mock.patch.object(bot, "_classify_completed_followup_exchange", new=mock.AsyncMock(side_effect=classify)):
            self.assertFalse(await self.resolve())

    async def test_timeout_stays_quiet_and_preserves_single_pending_worker(self):
        release = asyncio.Event()
        entered = asyncio.Event()

        async def classify(_exchange, _content):
            entered.set()
            await release.wait()
            return True

        with mock.patch.object(bot, "_classify_completed_followup_exchange", new=mock.AsyncMock(side_effect=classify)) as classifier:
            with mock.patch.object(bot.asyncio, "wait_for", new=mock.AsyncMock(side_effect=asyncio.TimeoutError)) as deadline:
                self.assertFalse(await self.resolve())
                await asyncio.sleep(0)
                pending = self.state["followup_check_task"]
                try:
                    self.assertFalse(pending.done())
                    waiter = asyncio.create_task(entered.wait())
                    await asyncio.wait({waiter}, timeout=2)
                    if not entered.is_set():
                        waiter.cancel()
                    self.assertTrue(entered.is_set())
                    self.assertFalse(await self.resolve())
                    classifier.assert_awaited_once()
                    deadline.assert_awaited_once()
                finally:
                    release.set()
                    await pending
            await asyncio.sleep(0)
        self.assertNotIn("followup_check_task", self.state)


if __name__ == "__main__":
    unittest.main()
