"""Word-count evidence crosses the real direct and grouped send owners.

These local tests retain SQLite, packet assembly, tracked generation wrappers,
source refresh and delivery guards. Only website, provider and Discord
transport are replaced; fixed replies do not certify live model factuality.
"""

import itertools
import os
import sqlite3
import unittest
from types import SimpleNamespace
from unittest import mock

import test_requested_show_date_delivery as delivery
from bnl_journal_source_store import purge_user_bound_conversation_sources_on_connection
from test_conversation_batching import FakeAuthor, FakeChannel, FakeGuild, FakeMessage


bot = delivery.bot
REQUEST = (
    'BNL, how many times did TikTok chat say the word "panda" '
    'during the September 4, 2026 stream?'
)
GROUPED_PARTICIPANTS = (
    ("Test Member", REQUEST, 42),
    ("Test Member", "Please check that recorded chat total.", 42),
)
STALE_ANSWER = (
    "The September 4 captured TikTok chat has 38 whole-word panda "
    "occurrences across 19 matching messages."
)
FRESH_ANSWER = (
    "The September 4 captured TikTok chat has 36 whole-word panda "
    "occurrences across 18 matching messages."
)


class ShowWordCountDeliveryTests(unittest.IsolatedAsyncioTestCase):
    # Each fixture owns a distinct room, including the real transient cache.
    channel_ids = itertools.count(88900)

    async def asyncSetUp(self):
        await delivery.RequestedShowDateDeliveryTests.asyncSetUp(self)
        self._seed_repeated_words()
        self.initial_memory_counts = self._personal_memory_counts()
        # Exercise retained source ownership when website transport is down.
        self.fetch.return_value = {}

    def _seed_repeated_words(self):
        at = delivery.show_fixture.durable_events()[0]["occurred_at_ms"] + 7 * 24 * 60 * 60 * 1000
        for index in range(19):
            result = delivery.record_source_event(
                bot.DB_FILE, guild_id=77, source_kind="tiktok_live_chat",
                source_key="word-count-original-" + str(index),
                occurred_at_ms=at + index,
                raw_text="Panda panda!", sanitized_summary="Panda panda!",
                channel_policy="public_context",
                subject_ref="discord_user:4242" if index == 0 else "tiktok_handle:test.word" + str(index),
                private_display_name="Test Word Viewer",
                public_usable=True, metadata={"eventType": "comment", "handle": "test.word" + str(index)},
            )
            self.assertTrue(result.ok)
        result = delivery.show_fixture.sync_tiktok_show_evidence_ledgers(
            bot.DB_FILE, guild_id=77, read_model=self.read_model,
            artist_identity_index=delivery.show_fixture.artist_index(),
            environ=delivery.show_fixture.ENABLED_QUEUE_ENV,
        )
        self.assertEqual(result["projectionErrors"], 0)

    def _withdraw_repeated_original(self):
        with sqlite3.connect(bot.DB_FILE) as conn:
            self.assertEqual(
                purge_user_bound_conversation_sources_on_connection(conn, 77, 4242), 1,
            )

    def _packet_env(self, enabled, channel_id):
        return mock.patch.dict(os.environ, {
            "BNL_MEMORY_LEDGER_SHADOW_ENABLED": "true",
            "BNL_MOMENT_ENGINE_SHADOW_ENABLED": "true",
            "BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED": "true",
            "BNL_RELATIONSHIP_V2_SHADOW_ENABLED": "true",
            "BNL_UNIFIED_RESPONSE_ASSESSMENT_SHADOW_ENABLED": "true",
            "BNL_UNIFIED_INTELLIGENCE_PACKET_SHADOW_ENABLED": "true",
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_ENABLED": str(enabled).lower(),
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_GUILD_IDS": "77",
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_USER_IDS": "42",
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_PUBLIC_ENABLED": "false",
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_SCOPED_EXPANSION_ENABLED": "false",
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_CHANNEL_IDS": str(channel_id),
        })

    def _assert_packet_scope(self, enabled, channel_id):
        # The existing packet owner permits private one-member batching.
        # Validate its real admission decision before exercising transport.
        scope = bot.ordinary_chat_route_scope_decision(
            guild_id=77, user_id=42, channel_id=channel_id,
            route_mode=bot.ROUTE_MODE_NORMAL_CHAT, channel_policy="sealed_test",
            current_direct=True, user_text=REQUEST,
        )
        self.assertEqual(scope.effective, enabled, scope.reason)
        self.assertEqual(scope.eligible, enabled, scope.reason)
        if enabled:
            self.assertEqual(scope.reason, "eligible")

    def _show_basis(self, bases):
        selected = [basis for basis in bases if isinstance(basis, bot.FinalizedShowPromptSourceBasis)]
        self.assertEqual(len(selected), 1)
        self.assertEqual(selected[0].show_keys, ("show-attendance-september",))
        return selected[0]

    def _assert_count(self, prompt, occurrences, messages):
        self.assertIn(
            "occurrenceCount=" + str(occurrences) + "; matchingMessageCount=" + str(messages), prompt,
        )
        self.assertIn("2026-09-04", prompt)
        self.assertNotIn("Durable TikTok show analysis context:", prompt)

    def _personal_memory_counts(self):
        with sqlite3.connect(bot.DB_FILE) as conn:
            return {
                table: conn.execute("SELECT COUNT(*) FROM " + table).fetchone()[0]
                for table in ("memory_tiers", "user_memory_facts", "relationship_state", "relationship_journal")
            }

    def _assert_source_blocks_not_stored(self, channel_id, *, direct, answer):
        # Sealed user/reply continuity is intentionally retained by the real
        # writer. Injected count evidence must not become conversation content
        # or public/durable memory, and test replies never become public sources.
        with sqlite3.connect(bot.DB_FILE) as conn:
            rows = conn.execute(
                "SELECT role,content,channel_policy FROM conversations WHERE guild_id=77 AND channel_id=?",
                (channel_id,),
            ).fetchall()
            self.assertEqual(len(rows), 2 if direct else 1)
            self.assertTrue(all(row[2] == "sealed_test" for row in rows))
            self.assertEqual([row[1] for row in rows if row[0] == "model"], [answer])
            self.assertEqual(len([row for row in rows if row[0] == "user"]), 1 if direct else 0)
            for _role, content, _policy in rows:
                self.assertNotIn("occurrenceCount=", content)
                self.assertNotIn("matchingMessageCount=", content)
                self.assertNotIn("Durable BARCODE Radio show episode memory:", content)
                self.assertNotIn("Exact TikTok chat word frequency", content)
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM bnl_journal_source_events WHERE guild_id=77 "
                "AND source_kind='discord_message' AND channel_id=?",
                (channel_id,),
            ).fetchone()[0], 0)
        self.assertEqual(self._personal_memory_counts(), self.initial_memory_counts)

    def _ledger_json(self):
        with sqlite3.connect(bot.DB_FILE) as conn:
            return conn.execute(
                "SELECT ledger_json FROM tiktok_show_evidence_ledgers "
                "WHERE guild_id=77 AND show_key='show-attendance-september'",
            ).fetchone()[0]

    async def _direct(self, channel_id, answer):
        channel = FakeChannel(channel_id, name="bnl-testing", guild=FakeGuild(77))
        self.runtime.channel_ids.add(channel_id)
        fake_bot = SimpleNamespace(id=999, display_name="BNL-01")
        message = FakeMessage(
            channel, REQUEST, author=FakeAuthor(42, "Test Member"), mentions=[fake_bot],
        )
        generation = mock.AsyncMock(side_effect=answer)
        guard = mock.AsyncMock(wraps=bot.apply_guarded_response_regeneration)
        with (
            mock.patch.object(type(bot.client), "user", new_callable=mock.PropertyMock, return_value=fake_bot),
            mock.patch.object(bot, "get_guild_config", return_value=999999),
            mock.patch.object(bot, "resolve_channel_policy", return_value="sealed_test"),
            mock.patch.object(bot, "is_privileged_member", return_value=False),
            mock.patch.object(bot, "BNL_ACTIVE_BATCHING_ENABLED", False),
            mock.patch.object(bot, "maybe_build_source_context_for_direct_message", new=mock.AsyncMock(return_value="")),
            mock.patch.object(bot, "get_gemini_response", new=generation),
            mock.patch.object(bot, "apply_guarded_response_regeneration", new=guard),
        ):
            await bot.on_message(message)
        return message, generation, guard

    async def _count_delivery(self, *, direct, enabled):
        async def provider_answer(prompt, *_args, **kwargs):
            if kwargs.get("attempt_counter") is not None:
                kwargs["attempt_counter"].mark_started()
            return STALE_ANSWER

        channel_id = next(self.channel_ids)
        with self._packet_env(enabled, channel_id):
            self._assert_packet_scope(enabled, channel_id)
            if direct:
                message, generation, guard = await self._direct(channel_id, provider_answer)
                sent = message.replies + message.channel.sent
            else:
                channel, generation, guard = await self.runtime._batch(
                    "sealed_test", REQUEST, answer=provider_answer, privileged=False, channel_id=channel_id,
                    participants=GROUPED_PARTICIPANTS,
                )
                sent = channel.sent
        generation.assert_awaited_once()
        self.assertEqual(
            generation.await_args.kwargs["route"],
            bot.ORDINARY_CHAT_SINGLE_PACKET_ROUTE if enabled else "get_gemini_response",
        )
        self._assert_count(generation.await_args.args[0], 38, 19)
        self.assertEqual(sent, [STALE_ANSWER])
        self.assertTrue(guard.await_count)
        basis = self._show_basis(guard.await_args.kwargs["prompt_source_bases"])
        self._assert_count(basis.rendered_context, 38, 19)
        self._assert_source_blocks_not_stored(channel_id, direct=direct, answer=STALE_ANSWER)

    async def test_direct_handler_delivers_selected_word_count(self):
        await self._count_delivery(direct=True, enabled=False)

    async def test_direct_packet_handler_delivers_selected_word_count(self):
        await self._count_delivery(direct=True, enabled=True)

    async def test_grouped_handler_delivers_selected_word_count(self):
        await self._count_delivery(direct=False, enabled=False)

    async def test_grouped_packet_handler_delivers_selected_word_count(self):
        await self._count_delivery(direct=False, enabled=True)

    async def _withdrawal_delivery(self, *, direct, enabled):
        calls = []
        ledger_before = self._ledger_json()

        async def provider_answer(prompt, *_args, **kwargs):
            calls.append(prompt)
            if kwargs.get("attempt_counter") is not None:
                kwargs["attempt_counter"].mark_started()
            if len(calls) == 1:
                self._withdraw_repeated_original()
                return STALE_ANSWER
            return FRESH_ANSWER

        channel_id = next(self.channel_ids)
        with self._packet_env(enabled, channel_id):
            self._assert_packet_scope(enabled, channel_id)
            if direct:
                message, generation, _guard = await self._direct(channel_id, provider_answer)
                sent = message.replies + message.channel.sent
            else:
                channel, generation, _guard = await self.runtime._batch(
                    "sealed_test", REQUEST, answer=provider_answer, privileged=False, channel_id=channel_id,
                    participants=GROUPED_PARTICIPANTS,
                )
                sent = channel.sent
        self.assertEqual(generation.await_count, 2)
        self.assertEqual(
            generation.await_args_list[0].kwargs["route"],
            bot.ORDINARY_CHAT_SINGLE_PACKET_ROUTE if enabled else "get_gemini_response",
        )
        self.assertTrue(generation.await_args.kwargs["source_context_available"])
        self._assert_count(calls[0], 38, 19)
        self._assert_count(calls[-1], 36, 18)
        self.assertNotIn("occurrenceCount=38", calls[-1])
        self.assertTrue(
            "SOURCE LIFECYCLE UPDATE" in calls[-1] or "RESPONSE REWRITE REQUIRED" in calls[-1],
        )
        self.assertEqual(sent, [FRESH_ANSWER])
        self.assertNotIn(STALE_ANSWER, sent)
        self.assertEqual(self._ledger_json(), ledger_before)
        self._assert_source_blocks_not_stored(channel_id, direct=direct, answer=FRESH_ANSWER)

    async def test_direct_provider_await_withdrawal_refreshes_word_count_before_one_send(self):
        await self._withdrawal_delivery(direct=True, enabled=False)

    async def test_direct_packet_provider_await_withdrawal_refreshes_word_count_before_one_send(self):
        await self._withdrawal_delivery(direct=True, enabled=True)

    async def test_grouped_provider_await_withdrawal_refreshes_word_count_before_one_send(self):
        await self._withdrawal_delivery(direct=False, enabled=False)

    async def test_grouped_packet_provider_await_withdrawal_refreshes_word_count_before_one_send(self):
        await self._withdrawal_delivery(direct=False, enabled=True)


if __name__ == "__main__":
    unittest.main()
