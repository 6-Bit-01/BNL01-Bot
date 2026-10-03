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

    async def _direct(self, channel_id, answer, request=REQUEST):
        channel = FakeChannel(channel_id, name="bnl-testing", guild=FakeGuild(77))
        self.runtime.channel_ids.add(channel_id)
        fake_bot = SimpleNamespace(id=999, display_name="BNL-01")
        message = FakeMessage(
            channel, request, author=FakeAuthor(42, "Test Member"), mentions=[fake_bot],
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


    def _seed_goat_words(self):
        at = delivery.show_fixture.durable_events()[0]["occurred_at_ms"] + 7 * 24 * 60 * 60 * 1000
        for index in range(6):
            result = delivery.record_source_event(
                bot.DB_FILE, guild_id=77, source_kind="tiktok_live_chat",
                source_key="goat-count-original-" + str(index),
                occurred_at_ms=at + 100 + index,
                raw_text="Goat!", sanitized_summary="Goat!",
                channel_policy="public_context",
                subject_ref="discord_user:4343" if index == 0 else "tiktok_handle:test.goat" + str(index),
                private_display_name="Test Goat Viewer",
                public_usable=True, metadata={"eventType": "comment", "handle": "test.goat" + str(index)},
            )
            self.assertTrue(result.ok)
        result = delivery.show_fixture.sync_tiktok_show_evidence_ledgers(
            bot.DB_FILE, guild_id=77, read_model=self.read_model,
            artist_identity_index=delivery.show_fixture.artist_index(),
            environ=delivery.show_fixture.ENABLED_QUEUE_ENV,
        )
        self.assertEqual(result["projectionErrors"], 0)

    async def _corrected_word_delivery(self, *, enabled, withdraw):
        self._seed_goat_words()
        self.fetch.return_value = self.read_model
        channel_id = next(self.channel_ids)
        self.addCleanup(bot._recent_room_events.pop, (77, channel_id), None)
        ledger_before = self._ledger_json()
        followup = "BNL, I meant goat."
        goat_answer = (
            "The September 4 captured TikTok chat has 6 whole-word goat "
            "occurrences across 6 matching messages."
        )
        fresh_answer = goat_answer.replace("6 whole-word", "5 whole-word").replace("6 matching", "5 matching")
        calls = []
        website_contexts = []
        read_model_builder = bot.build_bnl_read_model_context

        def observe_website_context(*args, **kwargs):
            context = read_model_builder(*args, **kwargs)
            website_contexts.append(context)
            return context

        async def panda_provider(_prompt, *_args, **kwargs):
            if kwargs.get("attempt_counter") is not None:
                kwargs["attempt_counter"].mark_started()
            return STALE_ANSWER

        async def goat_provider(prompt, *_args, **kwargs):
            calls.append(prompt)
            if kwargs.get("attempt_counter") is not None:
                kwargs["attempt_counter"].mark_started()
            if withdraw and len(calls) == 1:
                with sqlite3.connect(bot.DB_FILE) as conn:
                    self.assertEqual(
                        purge_user_bound_conversation_sources_on_connection(conn, 77, 4343), 1,
                    )
                return goat_answer
            return fresh_answer if withdraw else goat_answer

        fence = mock.AsyncMock(wraps=bot.prompt_source_basis_failure_async)
        with (
            self._packet_env(enabled, channel_id),
            mock.patch.object(bot, "build_bnl_read_model_context", new=observe_website_context),
            mock.patch.object(bot, "prompt_source_basis_failure_async", new=fence),
        ):
            self._assert_packet_scope(enabled, channel_id)
            first, first_generation, first_guard = await self._direct(channel_id, panda_provider)
            first_generation.assert_awaited_once()
            self.assertEqual(first.replies + first.channel.sent, [STALE_ANSWER])
            first_basis = self._show_basis(first_guard.await_args.kwargs["prompt_source_bases"])
            self._assert_count(first_basis.rendered_context, 38, 19)
            second, generation, guard = await self._direct(
                channel_id, goat_provider, request=followup,
            )

        # The normal website renderer really had an independently rendered
        # count for the correction. The prompt must use its versioned local
        # counterpart so a post-generation source change can remove that count.
        self.assertTrue(any(
            "occurrenceCount=6; matchingMessageCount=6" in context
            and "Durable TikTok show analysis context:" in context
            for context in website_contexts
        ))
        self.assertEqual(generation.await_count, 2 if withdraw else 1)
        self._assert_count(calls[0], 6, 6)
        basis = self._show_basis(guard.await_args.kwargs["prompt_source_bases"])
        self.assertEqual(basis.show_keys, first_basis.show_keys)
        self.assertEqual(bot.requested_tiktok_show_word_count(basis.user_text), "goat")
        self._assert_count(basis.rendered_context, 6, 6)
        if withdraw:
            self._assert_count(calls[-1], 5, 5)
            self.assertNotIn("occurrenceCount=6", calls[-1])
            final_basis = self._show_basis(fence.await_args.args[0])
            self._assert_count(final_basis.rendered_context, 5, 5)
        else:
            final_basis = self._show_basis(fence.await_args.args[0])
            self._assert_count(final_basis.rendered_context, 6, 6)
        expected_answer = fresh_answer if withdraw else goat_answer
        self.assertEqual(second.replies + second.channel.sent, [expected_answer])
        self.assertEqual(self._ledger_json(), ledger_before)
        self.assertEqual(self._personal_memory_counts(), self.initial_memory_counts)
        with sqlite3.connect(bot.DB_FILE) as conn:
            rows = conn.execute(
                "SELECT role,content,channel_policy FROM conversations "
                "WHERE guild_id=77 AND channel_id=? ORDER BY id",
                (channel_id,),
            ).fetchall()
        self.assertEqual([row[1] for row in rows if row[0] == "user"], [REQUEST, followup])
        self.assertEqual([row[1] for row in rows if row[0] == "model"], [STALE_ANSWER, expected_answer])
        self.assertTrue(all(row[2] == "sealed_test" for row in rows))
        self.assertTrue(all(
            "occurrenceCount=" not in row[1] and "matchingMessageCount=" not in row[1]
            for row in rows
        ))

    async def test_direct_word_correction_retains_selected_show_and_typed_count_basis(self):
        await self._corrected_word_delivery(enabled=False, withdraw=False)

    async def test_packet_word_correction_retains_selected_show_and_typed_count_basis(self):
        await self._corrected_word_delivery(enabled=True, withdraw=False)

    async def test_direct_word_correction_withdrawal_refreshes_count_before_one_send(self):
        await self._corrected_word_delivery(enabled=False, withdraw=True)

    async def test_packet_word_correction_withdrawal_refreshes_count_before_one_send(self):
        await self._corrected_word_delivery(enabled=True, withdraw=True)



class ShowWordCountFollowupSourceOwnerTests(unittest.TestCase):
    def _read_followup(self, evidence_items):
        request = "BNL, I meant goat."
        basis = SimpleNamespace(
            guild_id=77, current_user_id=42, evidence_items=evidence_items,
            rendered_context="BNL/model: The September 18 show had 900 goat occurrences.",
        )
        selection = {}
        with (
            mock.patch.object(bot, "env_queue_production_enabled", return_value=True),
            mock.patch.object(bot, "_consented_tiktok_show_subject_user_id", return_value=0),
            mock.patch.object(bot, "build_tiktok_show_evidence_context", return_value="Retained show evidence") as reader,
        ):
            context = bot.build_tiktok_show_evidence_context_for_turn(
                guild_id=77, user_text=request, subject_user_id=42,
                conversation_basis=basis, selection_out=selection,
            )
        self.assertEqual(context, "Retained show evidence")
        reader.assert_called_once()
        return reader.call_args.kwargs, selection

    def test_multiline_requester_count_reaches_reader_as_resolved_correction(self):
        prior = REQUEST.replace("BNL, ", "BNL,\n")
        kwargs, selection = self._read_followup((
            SimpleNamespace(source_id=10, speaker_user_id=42, text=prior),
        ))
        self.assertEqual(
            kwargs["user_text"],
            " ".join(prior.split()) + "\nCurrent follow-up: BNL, I meant goat.",
        )
        self.assertEqual(bot.requested_tiktok_show_word_count(kwargs["user_text"]), "goat")
        self.assertIn("September 4, 2026", kwargs["user_text"])
        self.assertEqual(selection["user_text"], kwargs["user_text"])

    def test_other_speaker_and_bot_count_items_cannot_establish_requester_scope(self):
        other_request = REQUEST.replace("September 4", "September 11")
        bot_request = REQUEST.replace("September 4", "September 18")
        kwargs, selection = self._read_followup((
            SimpleNamespace(source_id=20, speaker_user_id=88, text=other_request),
            SimpleNamespace(source_id=30, speaker_user_id=0, text=bot_request),
        ))
        self.assertEqual(kwargs["user_text"], "BNL, I meant goat.")
        self.assertEqual(kwargs["selection_user_text"], "BNL, I meant goat.")
        self.assertEqual(bot.requested_tiktok_show_word_count(kwargs["user_text"]), "")
        self.assertEqual(selection["user_text"], kwargs["user_text"])


    def test_count_correction_does_not_expand_personal_scope(self):
        import bnl_tiktok_show_ledger as ledger_owner

        selected = {"showDate": "2026-09-04", "participants": []}
        cases = (
            (REQUEST, True),
            (REQUEST + "\nCurrent follow-up: BNL, I meant goat.", True),
            ('Count the word "panda" in my TikTok comments during the September 4, 2026 stream.', False),
            (REQUEST + "\nCurrent follow-up: BNL, I meant goat in my comments.", False),
            ('How many times did I say the word "panda" during the September 4, 2026 TikTok stream?', False),
            ("What did I say during the September 4, 2026 TikTok stream?", False),
        )
        for query, eligible in cases:
            with self.subTest(query=query):
                score, participants = ledger_owner._document_relevance(
                    selected, user_text=query, subject_ref="",
                    recency_rank=0, allow_direct_subject=False,
                    requested_dates=("2026-09-04",),
                )
                self.assertEqual(score > 0, eligible)
                self.assertEqual(participants, [])



    def test_counter_rejects_personal_message_scope_for_eligible_originals(self):
        import test_tiktok_show_word_frequency as frequency_fixture

        selected = frequency_fixture.show()
        at = frequency_fixture.stamp("2026-10-03T03:00:00Z")
        originals = [frequency_fixture.event(0, "Panda panda goat my my.", at)]
        prior = 'Count the word "panda" in TikTok chat during the October 2, 2026 show.'
        cases = (
            (prior, "complete", 2),
            (prior + "\nCurrent follow-up: BNL, I meant goat.", "complete", 1),
            ('Count the word "my" in TikTok chat', "complete", 2),
            ('Count the word "panda" in my TikTok comments', "unavailable", None),
            (prior + "\nCurrent follow-up: BNL, I meant goat in my comments.", "unavailable", None),
            ('How many times did I say the word "panda" during the TikTok stream?', "unavailable", None),
            ('How many times did Test Member say the word "panda" during the TikTok stream?', "unavailable", None),
        )
        for query, status, occurrences in cases:
            with self.subTest(query=query):
                result = frequency_fixture.count_tiktok_show_word_frequency(
                    selected, originals, query,
                )
                self.assertEqual(result["status"], status)
                self.assertEqual(result["occurrenceCount"], occurrences)
                if status == "unavailable":
                    self.assertEqual(result["reason"], "specific_speaker_scope_not_resolved")
                    self.assertIsNone(result["matchingMessageCount"])



if __name__ == "__main__":
    unittest.main()
