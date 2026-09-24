"""Public person/topic recall reaches normal Gemini from Discord and TikTok.

Readers, SQLite source lineage, conversation frames, packet decisions, and
source refresh are real. Discord cache/transport and the website/provider
transport are fixtures. Supported replies demonstrate delivery, not live model
factuality or production availability of any particular comment.
"""

import asyncio
import json
import os
import sqlite3
import threading
import unittest
from itertools import product
from types import SimpleNamespace
from unittest import mock

import test_public_network_knowledge as network_fixture
import test_tiktok_show_evidence_ledger as show_fixture
from bnl_journal_source_store import record_source_event
from bnl_memory_ledger import (
    shadow_conversation_row, shadow_tiktok_live_chat_event, shadow_memory_tier_row,
    attach_memory_tier_conversation_sources,
)


bot = network_fixture.bnl01_bot
REAL_ADAPTERS = {
    name: getattr(bot, name)
    for name in (
        "build_broadcast_memory_context", "build_queue_artist_memory_context",
        "build_tiktok_show_evidence_context_for_turn", "maybe_build_bnl_read_model_context",
        "choose_response_style", "should_allow_greeting",
    )
}
GUILD = 77
SUBJECT = 42
REQUESTER = 100
REQUEST = "What has Test Signal said about amber lanterns?"
DID_REQUEST = "What did Test Signal say about amber lanterns?"
DISCORD_COMMENT = "I keep the amber lanterns beside my mixing desk."
TIKTOK_COMMENT = "The amber lanterns look great beside the stage."
NEWER_COMMENT = "The silver drums sound crisp tonight."
OTHER_COMMENT = "My amber lanterns arrived in a blue box."
PRIVATE_COMMENT = "The private amber lanterns passphrase is violet thimble."
SEALED_COMMENT = "The sealed amber lanterns passphrase is copper button."
ANSWER = (
    'Test Signal said on Discord, "I keep the amber lanterns beside my mixing desk." '
    'In TikTok chat, they said, "The amber lanterns look great beside the stage."'
)


class CrossSourceMemoryDeliveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_clock_request_keeps_original_and_episode_times_through_delivery(self):
        original = "The copper keyboard arrived safely."
        local_stamp = "2026-08-28 17:02:21 PDT"
        with sqlite3.connect(bot.DB_FILE) as conn:
            conn.execute(
                "INSERT INTO conversations (id,user_id,user_name,guild_id,role,content,"
                "timestamp,channel_id,channel_name,channel_policy,route_mode) "
                "VALUES (7096,?,?,?,'user',?,'2026-08-29T00:02:21Z',9920,"
                "'public-lounge','public_home','normal_chat')",
                (SUBJECT, "Test Signal", GUILD, original),
            )
            # The live failure quoted a retained directed exchange. Seed its
            # linked reply so both existing readers own this same original.
            conn.execute(
                "INSERT INTO conversations (id,user_id,user_name,guild_id,role,content,"
                "timestamp,channel_id,channel_name,channel_policy,route_mode) "
                "VALUES (7095,?,'BNL-01',?,'model','The keyboard receipt is acknowledged.',"
                "'2026-08-29T00:02:25Z',9920,'public-lounge','public_home','normal_chat')",
                (SUBJECT, GUILD),
            )
        show_fixture.sync_tiktok_show_evidence_ledgers(
            bot.DB_FILE, guild_id=GUILD, read_model=self.read_model,
            artist_identity_index={}, environ=show_fixture.ENABLED_QUEUE_ENV,
        )
        request = (
            "Give me a public Discord comment from Test Signal on August 28, 2026. "
            "Quote it and include the Pacific time."
        )
        for policy, enabled in product(("public_home", "sealed_test"), (False, True)):
            with self.subTest(policy=policy, packet=enabled), self._packet_configuration(enabled, 8810):
                inputs = self.runtime._direct_prompt_inputs(policy, request, privileged=False)
                direct, *_ = await bot.build_user_aware_prompt_async(**inputs)

                async def provider(*_args, **kwargs):
                    counter = kwargs.get("attempt_counter")
                    if counter is not None:
                        counter.mark_started()
                    return original

                channel, generation, guard = await self.runtime._batch(
                    policy, request=request, answer=provider, privileged=False, channel_id=8810,
                )
                generation.assert_awaited_once()
                self.assertEqual(channel.sent, [original])
                for prompt, bases in (
                    (direct, inputs["prompt_metadata"]["prompt_source_bases"]),
                    (generation.await_args.args[0], guard.await_args.kwargs["prompt_source_bases"]),
                ):
                    originals = [basis for basis in bases
                                 if isinstance(basis, bot.ConversationPromptSourceBasis)
                                 and 7096 in basis.source_row_ids]
                    self.assertTrue(originals, "Clock-format request removed the original reader")
                    episodes = [basis for basis in bases
                                if isinstance(basis, bot.FinalizedShowPromptSourceBasis)
                                and original in basis.rendered_context]
                    self.assertTrue(episodes, "Existing-source episode evidence is missing")
                    for context in (prompt, *(basis.rendered_context for basis in originals + episodes)):
                        self.assertTrue(any(original in line and local_stamp in line
                                            for line in context.splitlines()), context)
                    self.assertIn("recurring show schedule is not a recorded start", prompt)

    async def test_dated_original_keeps_local_time_in_direct_and_delivered_batch_prompts(self):
        original = "The violet keyboard arrived just after midnight."
        with sqlite3.connect(bot.DB_FILE) as conn:
            for row_id, stamp, content in (
                (7097, "2026-05-08T06:59:59Z", "The previous day's violet keyboard update."),
                (7098, "2026-05-09T07:00:00Z", "The following day's violet keyboard update."),
                (7099, "2026-05-08T07:15:00Z", original),
            ):
                conn.execute(
                    "INSERT INTO conversations (id,user_id,user_name,guild_id,role,content,"
                    "timestamp,channel_id,channel_name,channel_policy,route_mode) "
                    "VALUES (?,?,?,?,'user',?,?,9920,'public-lounge','public_home','normal_chat')",
                    (row_id, SUBJECT, "Test Signal", GUILD, content, stamp),
                )
        request = "Give me a public Discord comment from Test Signal on May 8, 2026. Quote it."
        for policy, enabled in product(("public_home", "sealed_test"), (False, True)):
            with self.subTest(policy=policy, packet=enabled), self._packet_configuration(enabled, 8810):
                inputs = self.runtime._direct_prompt_inputs(policy, request, privileged=False)
                direct, *_ = await bot.build_user_aware_prompt_async(**inputs)

                async def provider(*_args, **kwargs):
                    counter = kwargs.get("attempt_counter")
                    if counter is not None:
                        counter.mark_started()
                    return original

                channel, generation, guard = await self.runtime._batch(
                    policy, request=request, answer=provider, privileged=False, channel_id=8810,
                )
                generation.assert_awaited_once()
                self.assertEqual(channel.sent, [original])
                for prompt, bases in (
                    (direct, inputs["prompt_metadata"]["prompt_source_bases"]),
                    (generation.await_args.args[0], guard.await_args.kwargs["prompt_source_bases"]),
                ):
                    self.assertIn(original, prompt)
                    self.assertIn("2026-05-08 00:15:00 PDT", prompt)
                    self.assertNotIn("previous day's violet", prompt)
                    self.assertNotIn("following day's violet", prompt)
                    selected = {row for basis in bases if isinstance(basis, bot.ConversationPromptSourceBasis)
                                for row in basis.source_row_ids if row in {7097, 7098, 7099}}
                    self.assertEqual(selected, {7099})

    async def test_live_recall_sequence_keeps_layers_through_the_final_provider_prompt(self):
        early = "My violet keyboard arrived in May."
        with sqlite3.connect(bot.DB_FILE) as conn:
            conn.execute(
                "INSERT INTO conversations (id,user_id,user_name,guild_id,role,content,"
                "timestamp,channel_id,channel_name,channel_policy,route_mode) "
                "VALUES (7099,?,?,?,'user',?,'2026-05-08T18:00:00+00:00',9920,"
                "'public-lounge','public_home','normal_chat')",
                (SUBJECT, "Test Signal", GUILD, early),
            )
        self._seed_member_tiers()
        cases = (
            ("Tell me about a recent public Discord conversation involving Test Signal.",
             "Test Signal keeps amber lanterns beside a mixing desk.", (7099, 7101), True),
            ("What were their exact words?",
             'Test Signal said on Discord, "' + DISCORD_COMMENT + '"', (7099, 7101), True),
            ("Switch to Test Other. Give me a recent example from public Discord and quote what Test Other said.",
             'Test Other said on Discord, "' + OTHER_COMMENT + '"', (7104,), False),
            ("Give me a public Discord comment from Test Signal on May 8, 2026. Quote it.",
             'On May 8, Test Signal said on Discord, "' + early + '"', (7099,), False),
            ("What do you remember about Test Other?",
             "Test Other mentioned amber lanterns arriving in a blue box.", (7104,), False),
            ("Look at my TikTok and Discord activity together. What do you see?",
             "I do not have a supported TikTok example for you here.", (), False),
            ("What has Test Signal said in the TikTok live chats and the Discord?",
             ANSWER, (7099, 7101), True),
        )
        for enabled in (False, True):
            with self.subTest(packet=enabled), self._packet_configuration(enabled, 8810):
                with sqlite3.connect(bot.DB_FILE) as conn:
                    conn.execute("DELETE FROM conversations WHERE channel_id=8810")
                for index, (request, answer, expected_rows, has_show) in enumerate(cases):
                    inputs = self.runtime._direct_prompt_inputs("sealed_test", request, privileged=False)
                    direct, *_ = await bot.build_user_aware_prompt_async(**inputs)
                    self.assertEqual(inputs["conversation_orchestration"].situation_frame.status, "resolved")
                    if index == 3:
                        self.assertEqual(inputs["conversation_context_result"].referent_status, "not_requested")
                    async def provider(*_args, **kwargs):
                        counter = kwargs.get("attempt_counter")
                        if counter is not None:
                            counter.mark_started()
                        return answer
                    channel, generation, guard = await self.runtime._batch(
                        "sealed_test", request=request, answer=provider,
                        privileged=False, channel_id=8810,
                    )
                    generation.assert_awaited_once()
                    self.assertEqual(channel.sent, [answer])
                    delivered = generation.await_args.args[0]
                    for prompt, bases in (
                        (direct, inputs["prompt_metadata"]["prompt_source_bases"]),
                        (delivered, guard.await_args.kwargs["prompt_source_bases"]),
                    ):
                        with self.subTest(turn=index, packet=enabled):
                            source_rows = tuple(sorted({
                                row for basis in bases if isinstance(basis, bot.ConversationPromptSourceBasis)
                                for row in basis.source_row_ids if row in {7099, 7101, 7104}
                            }))
                            self.assertEqual(source_rows, expected_rows)
                            originals = [excerpt.source_text for basis in bases
                                         if isinstance(basis, bot.FinalizedShowPromptSourceBasis)
                                         for excerpt in basis.authored_excerpts]
                            self.assertEqual(TIKTOK_COMMENT in originals, has_show)
                            self.assertNotIn("Exact-quote authority: unavailable", prompt)
                            self.assertNotIn("Never quote them", prompt)
                            self.assertIn("quote short exact spans from the supplied original", prompt)
                            if index in (0, 1, 6):
                                self.assertIn("amber lantern project began", prompt)
                    # Preserve a preceding refusal too: a saved BNL claim must
                    # not override original quotation authority on later turns.
                    self._capture_prior_person_request(request)
                    bot.save_model_message(
                        REQUESTER, GUILD,
                        "I cannot give exact words without an audit block." if index == 1 else answer,
                        channel_name="bnl-testing", channel_policy="sealed_test", channel_id=8810,
                        route_mode="normal_chat",
                    )

    async def test_self_public_activity_uses_only_the_requesters_originals_in_both_sources(self):
        self.runtime.user_id = SUBJECT
        request = "Look at my TikTok and Discord activity together. What do you see?"
        prompt, metadata = await self.runtime._direct_prompt_async("sealed_test", request=request, privileged=False)
        self.assertIn(DISCORD_COMMENT, prompt)
        self.assertIn(TIKTOK_COMMENT, prompt)
        source_text = "\n".join(getattr(b, "rendered_context", "") for b in metadata["prompt_source_bases"])
        self.assertNotIn(OTHER_COMMENT, source_text)
        self.assertTrue(any(isinstance(b, bot.ConversationPromptSourceBasis)
                            and b.participant_user_ids == (SUBJECT,) for b in metadata["prompt_source_bases"]))
        self.assertEqual(bot.prompt_source_basis_failure(metadata["prompt_source_bases"]), "")

    async def test_broad_discord_recall_and_quote_followup_keep_the_named_member(self):
        request = "Tell me about a recent public Discord conversation involving Test Signal."
        for enabled in (False, True):
            with self.subTest(packet=enabled), self._packet_configuration(enabled, 8810):
                for text in (request, "What were their exact words?"):
                    if text != request:
                        self._capture_prior_person_request(request)
                    prompt, metadata = await self.runtime._direct_prompt_async(
                        "sealed_test", request=text, privileged=False,
                    )
                    self.assertIn(DISCORD_COMMENT, prompt)
                    self.assertTrue(any(
                        isinstance(basis, bot.ConversationPromptSourceBasis)
                        and 7101 in basis.source_row_ids
                        for basis in metadata["prompt_source_bases"]
                    ))
                    for excluded in (OTHER_COMMENT, PRIVATE_COMMENT, SEALED_COMMENT):
                        self.assertNotIn(excluded, prompt)

    async def test_topic_search_reaches_older_originals_before_applying_recent_row_limit(self):
        with sqlite3.connect(bot.DB_FILE) as conn:
            conn.execute(
                "INSERT INTO conversations (id,user_id,user_name,guild_id,role,content,"
                "timestamp,channel_id,channel_name,channel_policy,route_mode) "
                "VALUES (7200,?,?,?,'user',?,'2026-09-22T15:00:00+00:00',9920,"
                "'public-lounge','public_home','normal_chat')",
                (SUBJECT, "Test Signal", GUILD, "The new silver drum kit arrived today."),
            )
        with mock.patch.object(bot, "CONVERSATION_ROWS_PER_USER_MAX", 1):
            prompt, metadata = await self.runtime._direct_prompt_async(
                "sealed_test", request=REQUEST, privileged=False,
            )
        self.assertIn(DISCORD_COMMENT, prompt)
        self.assertTrue(any(
            isinstance(basis, bot.ConversationPromptSourceBasis)
            and 7101 in basis.source_row_ids for basis in metadata["prompt_source_bases"]
        ))

    def _seed_member_tiers(self):
        with sqlite3.connect(bot.DB_FILE) as conn:
            for uid, tier, text, trust in (
                (SUBJECT, "short", "A recent discussion covered silver drums.", "source_safe_public"),
                (SUBJECT, "medium", "An earlier exchange explored amber lantern placement.", "source_safe_public_consolidated"),
                (SUBJECT, "long", "The amber lantern project began with a stage lighting experiment.", "source_safe_public_consolidated"),
                (SUBJECT, "long", "The hidden amber lantern access code is turquoise.", "legacy_unknown"),
                (43, "long", "The other member ordered amber lantern batteries.", "source_safe_public_consolidated"),
            ):
                bot._insert_memory_tier(
                    conn.cursor(), uid, GUILD, tier, text, 0.85,
                    source_role="user" if tier == "short" else "consolidation",
                    source_channel_policy="public_home", source_trust=trust,
                )
            conn.execute("UPDATE memory_tiers SET updated_at='2026-05-15T12:00:00+00:00' WHERE tier IN ('medium','long')")

    async def test_named_member_tiers_join_originals_and_revalidate_in_both_routes(self):
        self._seed_member_tiers()
        for enabled in (False, True):
            with self.subTest(packet=enabled), self._packet_configuration(enabled, 8810):
                prompt, metadata = await self.runtime._direct_prompt_async(
                    "sealed_test", request=REQUEST, privileged=False,
                )
                self.assertIn("earlier exchange explored amber lantern placement", prompt)
                self.assertIn("amber lantern project began", prompt)
                self.assertIn(DISCORD_COMMENT, prompt)
                self.assertIn(TIKTOK_COMMENT, prompt)
                self.assertNotIn("hidden amber lantern access code", prompt)
                self.assertNotIn("other member ordered amber lantern batteries", prompt)
                bases = tuple(basis for basis in metadata["prompt_source_bases"]
                              if isinstance(basis, bot.MemoryPromptSourceBasis)
                              and basis.user_id == SUBJECT)
                self.assertEqual(len(bases), 1)
                self.assertIn("not quote authority", bases[0].rendered_context)
                self.assertFalse(bot.refresh_prompt_source_basis(bases[0])[1])
        with sqlite3.connect(bot.DB_FILE) as conn:
            conn.execute("UPDATE memory_tiers SET source_trust='legacy_unknown' WHERE user_id=? AND tier='medium'", (SUBJECT,))
        fresh, changed = bot.refresh_prompt_source_basis(bases[0])
        self.assertTrue(changed)
        self.assertNotIn("earlier exchange explored amber lantern placement", fresh.rendered_context)

    async def test_unmatched_wording_keeps_scoped_evidence_through_both_delivery_routes(self):
        self._seed_member_tiers()
        # No content word here overlaps the authored lantern/drum evidence.
        # The model needs that evidence to judge meaning, including whether a
        # question is unsupported; the selector must not decide by vocabulary.
        requests = (
            "How does Test Signal set the mood? Keep your answer concise.",
            "Describe Test Signal's outlook. Use a couple of sentences.",
            "What has Test Signal said about submarines?",
        )
        for policy, enabled, request in product(
            ("public_home", "sealed_test"), (False, True), requests,
        ):
            with self.subTest(policy=policy, packet=enabled, request=request), self._packet_configuration(enabled, 8810):
                inputs = self.runtime._direct_prompt_inputs(policy, request, privileged=False)
                direct, *_ = await bot.build_user_aware_prompt_async(**inputs)
                channel, generation, guard = await self.runtime._batch(
                    policy, request=request, answer=self._provider_answer,
                    privileged=False, channel_id=8810,
                )
                generation.assert_awaited_once()
                self.assertEqual(channel.sent, [ANSWER])
                for prompt, bases in (
                    (direct, inputs["prompt_metadata"]["prompt_source_bases"]),
                    (generation.await_args.args[0], guard.await_args.kwargs["prompt_source_bases"]),
                ):
                    self._assert_sources(prompt, bases)
                    for summary in ("recent discussion covered silver drums",
                                    "earlier exchange explored amber lantern placement",
                                    "amber lantern project began"):
                        self.assertIn(summary, prompt)
                    self.assertNotIn("hidden amber lantern access code", prompt)
                    self.assertIn("background context does not prove a requested topic or date", prompt)
                    self.assertIn("bounded selection does not establish absence", prompt)
                    self.assertTrue(any(isinstance(b, bot.MemoryPromptSourceBasis)
                                        and b.user_id == SUBJECT for b in bases))

    async def test_dated_recall_keeps_memory_as_background_and_originals_in_date_scope(self):
        self._seed_member_tiers()
        for enabled in (False, True):
            with self.subTest(packet=enabled), self._packet_configuration(enabled, 8810):
                prompt, metadata = await self.runtime._direct_prompt_async(
                    "sealed_test", privileged=False,
                    request="Give me a public Discord comment from Test Signal on May 8, 2026.",
                )
                self.assertIn("amber lantern project began", prompt)
                self.assertNotIn(DISCORD_COMMENT, prompt)
                self.assertNotIn(TIKTOK_COMMENT, prompt)
                self.assertIn("background context does not prove a requested topic or date", prompt)
                bases = metadata["prompt_source_bases"]
                self.assertTrue(any(isinstance(b, bot.MemoryPromptSourceBasis)
                                    and b.user_id == SUBJECT and "not quote authority" in b.rendered_context
                                    for b in bases))
                self.assertFalse(any(isinstance(b, bot.ConversationPromptSourceBasis)
                                     and 7101 in b.source_row_ids for b in bases))

    async def test_broad_batch_delivers_originals_and_all_public_tiers_with_one_send(self):
        self._seed_member_tiers()
        request = "Tell me about a recent public Discord conversation involving Test Signal."
        for policy, enabled in product(("public_home", "sealed_test"), (False, True)):
            channel_id = 8811 + len(self.runtime.channel_ids)
            with self.subTest(policy=policy, packet=enabled), self._packet_configuration(enabled, channel_id):
                channel, generation, guard = await self.runtime._batch(
                    policy, request=request, answer=self._provider_answer, privileged=False,
                )
                generation.assert_awaited_once()
                self.assertEqual(channel.sent, [ANSWER])
                prompt = generation.await_args.args[0]
                for included in (DISCORD_COMMENT, "recent discussion covered silver drums",
                                 "earlier exchange explored amber lantern placement", "amber lantern project began"):
                    self.assertIn(included, prompt)
                self.assertNotIn("hidden amber lantern access code", prompt)
                self.assertTrue(any(isinstance(basis, bot.MemoryPromptSourceBasis)
                                    and basis.user_id == SUBJECT
                                    for basis in guard.await_args.kwargs["prompt_source_bases"]))

    async def test_broad_recall_uses_message_time_not_archive_insertion_order(self):
        with sqlite3.connect(bot.DB_FILE) as conn:
            for row_id, stamp, comment in (
                (7200, "2026-09-22T12:00:00+00:00", "A recent public note about green stage curtains."),
                (7300, "2026-05-15T12:00:00+00:00", "An older note imported after the recent discussion."),
            ):
                conn.execute(
                    "INSERT INTO conversations (id,user_id,user_name,guild_id,role,content,"
                    "timestamp,channel_id,channel_name,channel_policy,route_mode) "
                    "VALUES (?,?,?,?,'user',?,?,9920,'public-lounge','public_home','normal_chat')",
                    (row_id, SUBJECT, "Test Signal", GUILD, comment, stamp),
                )
        with mock.patch.object(bot, "CONVERSATION_ROWS_PER_USER_MAX", 1):
            _prompt, metadata = await self.runtime._direct_prompt_async(
                "sealed_test", request="Give me a recent public Discord example from Test Signal.", privileged=False,
            )
        historical = [basis for basis in metadata["prompt_source_bases"]
                      if isinstance(basis, bot.ConversationPromptSourceBasis)
                      and basis.current_user_id == 0]
        self.assertEqual(len(historical), 1)
        self.assertEqual(historical[0].source_row_ids, (7200,))

    async def test_named_tier_controls_and_original_retractions_invalidate_delivery(self):
        self._seed_member_tiers()
        with sqlite3.connect(bot.DB_FILE) as conn:
            for row_id, tier, summary, stamp in conn.execute(
                "SELECT id,tier,summary,updated_at FROM memory_tiers WHERE user_id=? AND tier IN ('medium','long')",
                (SUBJECT,),
            ).fetchall():
                if tier == "long":
                    attach_memory_tier_conversation_sources(
                        conn, guild_id=GUILD, tier_row_id=row_id, source_row_ids=(7101,),
                    )
                shadow_memory_tier_row(
                    conn, row_id=row_id, user_id=SUBJECT, guild_id=GUILD,
                    tier=tier, summary=summary, updated_at=stamp, channel_policy="public_home",
                )
        _prompt, metadata = await self.runtime._direct_prompt_async(
            "sealed_test", request=REQUEST, privileged=False,
        )
        basis = next(basis for basis in metadata["prompt_source_bases"]
                     if isinstance(basis, bot.MemoryPromptSourceBasis) and basis.user_id == SUBJECT)
        self.assertIn("earlier exchange explored amber lantern placement", basis.rendered_context)
        with sqlite3.connect(bot.DB_FILE) as conn:
            conn.execute("UPDATE memory_ledger_entries SET lifecycle_status='retracted' WHERE source_table='memory_tiers' AND normalized_value LIKE 'An earlier exchange%'")
        fresh, changed = bot.refresh_prompt_source_basis(basis)
        self.assertTrue(changed)
        self.assertNotIn("earlier exchange explored amber lantern placement", fresh.rendered_context)
        self.assertIn("amber lantern project began", fresh.rendered_context)
        with sqlite3.connect(bot.DB_FILE) as conn:
            conn.execute("UPDATE conversations SET content='A corrected public contribution.' WHERE id=7101")
        corrected, changed = bot.refresh_prompt_source_basis(fresh)
        self.assertTrue(changed)
        self.assertNotIn("amber lantern project began", corrected.rendered_context)

    async def test_person_switch_and_unknown_artist_do_not_inherit_another_members_tiers(self):
        self._seed_member_tiers()
        self._capture_prior_person_request()
        for request, expected in (
            ("Switch to Test Other. Give me a recent public Discord example and quote what Test Other said.", OTHER_COMMENT),
            ("Tell me about a recent public Discord conversation involving Test Stage Alias.", None),
        ):
            with self.subTest(request=request):
                prompt, metadata = await self.runtime._direct_prompt_async(
                    "sealed_test", request=request, privileged=False,
                )
                self.assertNotIn("earlier exchange explored amber lantern placement", prompt)
                self.assertNotIn("amber lantern project began", prompt)
                self.assertFalse(any(isinstance(basis, bot.MemoryPromptSourceBasis) and basis.user_id == SUBJECT
                                     for basis in metadata["prompt_source_bases"]))
                if expected:
                    self.assertIn(expected, prompt)

    async def test_person_topic_followup_uses_original_memory_with_or_without_saved_bot_reply(self):
        for policy, enabled, saved in product(("public_home", "sealed_test"), (False, True), (False, True)):
            channel_id = 8811 + len(self.runtime.channel_ids)
            with self.subTest(policy=policy, packet=enabled, saved=saved), self._packet_configuration(enabled, 8810):
                channel_name = "bnl-testing" if policy == "sealed_test" else "barcode-bot"
                with sqlite3.connect(bot.DB_FILE) as conn:
                    conn.execute("DELETE FROM conversations WHERE channel_id=8810")
                for target in (8810, channel_id):
                    bot.save_user_message(
                        REQUESTER, "Test Member", GUILD, REQUEST,
                        channel_name=channel_name, channel_policy=policy,
                        channel_id=target, message_id=target * 100,
                        route_mode="normal_chat", directed_to_bnl=True,
                    )
                    if saved:
                        with sqlite3.connect(bot.DB_FILE) as conn:
                            conn.execute(
                                "INSERT INTO conversations (user_id,user_name,guild_id,role,content,"
                                "timestamp,channel_id,channel_name,channel_policy,route_mode) "
                                "VALUES (?,?,?,'model',?,?,?,?,?,'normal_chat')",
                                (REQUESTER, "BNL-01", GUILD, "They discussed their stage decorations.",
                                 bot.datetime.now(bot.timezone.utc).isoformat(), target, channel_name, policy),
                            )
                followup = "What were their exact words?"
                prompt, metadata = await self.runtime._direct_prompt_async(
                    policy, request=followup, privileged=False,
                )
                self._assert_sources(prompt, metadata["prompt_source_bases"])
                self.assertEqual(metadata["ordinary_chat_single_packet_applied"], enabled,
                                 metadata["ordinary_chat_single_packet_scope"])
                with self._packet_configuration(enabled, channel_id):
                    channel, generation, guard = await self.runtime._batch(
                        policy, request=followup, answer=self._provider_answer, privileged=False,
                    )
                generation.assert_awaited_once()
                self.assertEqual(channel.sent, [ANSWER])
                self._assert_sources(generation.await_args.args[0], guard.await_args.kwargs["prompt_source_bases"])

    def _capture_prior_person_request(self, text=REQUEST):
        bot.save_user_message(
            REQUESTER, "Test Member", GUILD, text,
            channel_name="bnl-testing", channel_policy="sealed_test",
            channel_id=8810, message_id=881000,
            route_mode="normal_chat", directed_to_bnl=True,
        )

    async def test_new_person_and_new_topic_take_precedence_over_prior_memory_request(self):
        self._capture_prior_person_request()
        prompt, metadata = await self.runtime._direct_prompt_async(
            "sealed_test", request="No, what has Test Other said about amber lanterns?", privileged=False,
        )
        self.assertIn(OTHER_COMMENT, prompt)
        self.assertNotIn(DISCORD_COMMENT, prompt)
        self.assertNotIn(TIKTOK_COMMENT, prompt)
        prompt, _metadata = await self.runtime._direct_prompt_async(
            "sealed_test", request="New topic: what are their favorite instruments?", privileged=False,
        )
        self.assertNotIn(REQUEST, prompt)
        self.assertNotIn(DISCORD_COMMENT, prompt)
        self.assertNotIn(TIKTOK_COMMENT, prompt)

    async def test_intervening_unrelated_human_turn_does_not_revive_old_person(self):
        self._capture_prior_person_request()
        self._capture_prior_person_request("Explain the difference between a flute and a trumpet.")
        prompt, _metadata = await self.runtime._direct_prompt_async(
            "sealed_test", request="What were their exact words?", privileged=False,
        )
        self.assertNotIn(DISCORD_COMMENT, prompt)
        self.assertNotIn(TIKTOK_COMMENT, prompt)

    async def test_continuation_anchor_and_original_sources_remain_revalidatable(self):
        self._capture_prior_person_request()
        prompt, metadata = await self.runtime._direct_prompt_async(
            "sealed_test", request="What were their exact words?", privileged=False,
        )
        bases = tuple(metadata["prompt_source_bases"])
        self._assert_sources(prompt, bases)
        with sqlite3.connect(bot.DB_FILE) as conn:
            conn.execute("UPDATE conversations SET channel_policy='internal_controlled' WHERE id=7101")
        self.assertEqual(bot.prompt_source_basis_failure(bases), "conversation_source_changed")
        with sqlite3.connect(bot.DB_FILE) as conn:
            conn.execute("UPDATE conversations SET channel_policy='public_home' WHERE id=7101")
            conn.execute("DELETE FROM conversations WHERE channel_id=8810")
        self.assertEqual(bot.prompt_source_basis_failure(bases), "conversation_source_changed")

    async def asyncSetUp(self):
        self.runtime = network_fixture.PublicNetworkKnowledgeTests()
        await self.runtime.asyncSetUp()
        self.addAsyncCleanup(self.runtime.asyncTearDown)
        self.stack = self.runtime.stack
        self.runtime.guild_id = GUILD
        self.runtime.user_id = REQUESTER
        for name, implementation in REAL_ADAPTERS.items():
            self.stack.enter_context(mock.patch.object(bot, name, new=implementation))
        self.stack.enter_context(mock.patch.object(bot, "BNL_PRIMARY_GUILD_ID", GUILD))
        self.stack.enter_context(mock.patch.dict(os.environ, {
            **show_fixture.ENABLED_QUEUE_ENV,
            "BNL_MEMORY_LEDGER_SHADOW_ENABLED": "true",
            "BNL_MOMENT_ENGINE_SHADOW_ENABLED": "true",
            "BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED": "true",
            "BNL_RELATIONSHIP_V2_SHADOW_ENABLED": "true",
            "BNL_UNIFIED_RESPONSE_ASSESSMENT_SHADOW_ENABLED": "true",
            "BNL_UNIFIED_INTELLIGENCE_PACKET_SHADOW_ENABLED": "true",
            "BNL_CONVERSATION_ORCHESTRATION_INFLUENCE_ENABLED": "false",
            "BNL_CONVERSATION_ORCHESTRATION_SEALED_CANARY_ENABLED": "false",
        }))
        members = [
            SimpleNamespace(id=uid, display_name=label, name=label,
                            global_name=None, bot=False, roles=[],
                            guild_permissions=SimpleNamespace(administrator=False, manage_guild=False))
            for uid, label in ((SUBJECT, "Test Signal"), (43, "Test Other"),
                               (REQUESTER, "Test Member"))
        ]
        self.guild = SimpleNamespace(
            id=GUILD, members=members,
            get_member=lambda uid: next((member for member in members if member.id == uid), None),
        )
        self.stack.enter_context(mock.patch.object(bot.client, "get_guild", return_value=self.guild))
        for member in members:
            bot.upsert_user_profile(member.id, GUILD, member.display_name)
        self.addCleanup(bot.purge_member_memory_caches, SUBJECT, GUILD)
        self._seed_discord_sources()
        self._seed_tiktok_shows()
        self.stack.enter_context(mock.patch.object(bot, "fetch_bnl_read_model", return_value=self.read_model))

    def _seed_discord_sources(self):
        with sqlite3.connect(bot.DB_FILE) as conn:
            for row_id, uid, label, policy, text in (
                (7101, SUBJECT, "Test Signal", "public_home", DISCORD_COMMENT),
                (7102, SUBJECT, "Test Signal", "internal_controlled", PRIVATE_COMMENT),
                (7103, SUBJECT, "Test Signal", "sealed_test", SEALED_COMMENT),
                (7104, 43, "Test Other", "public_home", OTHER_COMMENT),
            ):
                timestamp = "2026-08-30T18:00:00+00:00"  # Outside either broadcast.
                channel_name = "public-lounge" if policy == "public_home" else "private-fixture"
                conn.execute(
                    "INSERT INTO conversations (id,user_id,user_name,guild_id,channel_name,"
                    "channel_policy,route_mode,role,content,timestamp,channel_id,message_id) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                    (row_id, uid, label, GUILD, channel_name, policy, "normal_chat", "user",
                     text, timestamp, 9002 + row_id, row_id + 10000),
                )
                result = shadow_conversation_row(
                    conn, row_id=row_id, user_id=uid, user_name=label, guild_id=GUILD,
                    role="user", content=text, channel_name=channel_name,
                    channel_policy=policy, channel_id=9002 + row_id,
                    message_id=row_id + 10000, route_mode="normal_chat", observed_at=timestamp,
                )
                self.assertEqual(result.outcome, "inserted")

    def _seed_tiktok_shows(self):
        august = show_fixture.archived_show()
        september = json.loads(
            json.dumps(august).replace("show-attendance-1", "show-topic-september")
            .replace("2026-08-28", "2026-09-04").replace("2026-08-29", "2026-09-05")
        )
        for key, date, uid, label, handle, text in (
            ("topic-august", "2026-08-29T00:02:00Z", SUBJECT, "Test Signal", "test.signal", TIKTOK_COMMENT),
            ("topic-other", "2026-08-29T00:03:00Z", 43, "Test Other", "test.other", OTHER_COMMENT),
            ("topic-september", "2026-09-05T00:02:00Z", SUBJECT, "Test Signal", "test.signal", NEWER_COMMENT),
        ):
            result = record_source_event(
                bot.DB_FILE, guild_id=GUILD, source_kind="tiktok_live_chat", source_key=key,
                occurred_at_ms=show_fixture.stamp(date), raw_text=text, sanitized_summary=text,
                channel_policy="public_context", subject_ref="discord_user:%s" % uid,
                private_display_name=label, public_usable=True,
                metadata={"eventType": "comment", "handle": handle,
                          "identityBindingBasis": "exact_source_owned_subject_reference"},
            )
            self.assertTrue(result.ok)
            with sqlite3.connect(bot.DB_FILE) as conn:
                result = shadow_tiktok_live_chat_event(
                    conn, guild_id=GUILD, event_id=key, subject_key="discord_user:%s" % uid,
                    subject_display_name=label, content=text, observed_at=date,
                    source_sequence=show_fixture.stamp(date),
                )
                self.assertEqual(result.outcome, "inserted")
        self.read_model = show_fixture.authorized_read_model({
            "currentShow": None, "latestShow": september, "shows": [august],
        })
        synced = show_fixture.sync_tiktok_show_evidence_ledgers(
            bot.DB_FILE, guild_id=GUILD, read_model=self.read_model,
            artist_identity_index={}, environ=show_fixture.ENABLED_QUEUE_ENV,
        )
        self.assertEqual(synced["showsFinalized"], 2)

    def _packet_configuration(self, enabled, channel_id):
        return mock.patch.dict(os.environ, {
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_ENABLED": str(enabled).lower(),
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_PUBLIC_ENABLED": str(enabled).lower(),
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_GUILD_IDS": str(GUILD),
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_USER_IDS": str(REQUESTER),
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_CHANNEL_IDS": str(channel_id),
        })

    def _assert_sources(self, prompt, bases):
        self.assertTrue(DISCORD_COMMENT in prompt, "Public Discord authored statement missing from provider prompt")
        self.assertTrue(TIKTOK_COMMENT in prompt, "Public TikTok authored statement missing from provider prompt")
        # Same-person background can remain beside a stronger topic match.
        # Another person's words or private sources must never fill that role.
        for excluded in (OTHER_COMMENT, PRIVATE_COMMENT, SEALED_COMMENT):
            self.assertNotIn(excluded, prompt)
        self.assertIn("Test Signal", prompt)
        self.assertIn("discord", prompt.casefold())
        self.assertIn("tiktok", prompt.casefold())
        self.assertNotIn("Third-party attribution mode: summarize", prompt)
        self.assertNotIn("Do not provide, reconstruct, or claim exact wording.", prompt)
        self.assertTrue(bases)
        source_context = "\n".join(getattr(basis, "rendered_context", "") for basis in bases)
        self.assertTrue(DISCORD_COMMENT in source_context, "Discord statement has no revalidatable source basis")
        self.assertTrue(TIKTOK_COMMENT in source_context, "TikTok statement has no revalidatable source basis")
        discord_bases = tuple(
            basis for basis in bases
            if isinstance(basis, bot.ConversationPromptSourceBasis)
            and 7101 in basis.source_row_ids
        )
        self.assertEqual(len(discord_bases), 1)
        self.assertEqual(discord_bases[0].participant_user_ids, (SUBJECT,))
        self.assertEqual(discord_bases[0].speaker_labels, ("Test Signal",))
        self.assertTrue(any(
            item.speaker_user_id == SUBJECT and item.text == DISCORD_COMMENT
            for item in discord_bases[0].evidence_items
        ))
        show_bases = tuple(
            basis for basis in bases if isinstance(basis, bot.FinalizedShowPromptSourceBasis)
        )
        self.assertTrue(any(
            excerpt.subject_ref == "discord_user:42"
            and excerpt.source_text == TIKTOK_COMMENT
            and excerpt.speaker_label == "Test Signal (@test.signal)"
            for basis in show_bases
            for excerpt in basis.authored_excerpts
        ))
        # The completed public send may legitimately update requester memory.
        # These independently authored source roots must remain valid.
        self.assertEqual(bot.prompt_source_basis_failure((*discord_bases, *show_bases)), "")

    @staticmethod
    async def _provider_answer(*_args, **kwargs):
        # The real packet owner counts physical attempts at the provider
        # boundary; a plain text AsyncMock otherwise falsely reports zero.
        counter = kwargs.get("attempt_counter")
        if counter is not None:
            counter.mark_started()
        return ANSWER

    async def test_real_direct_assembly_combines_named_public_sources_with_packet_on_and_off(self):
        for policy, enabled, request in product(
            ("public_home", "sealed_test"), (False, True), (REQUEST, DID_REQUEST),
        ):
            with self.subTest(policy=policy, packet_enabled=enabled, request=request), self._packet_configuration(enabled, 8810):
                self.assertEqual(bot.ordinary_chat_configuration()["effective"], enabled)
                prompt, metadata = await self.runtime._direct_prompt_async(
                    policy, request=request, privileged=False,
                )
                self._assert_sources(prompt, metadata["prompt_source_bases"])

    async def test_real_batch_combines_sources_and_sends_one_normal_response(self):
        for policy, enabled, request in product(
            ("public_home", "sealed_test"), (False, True), (REQUEST, DID_REQUEST),
        ):
            channel_id = 8811 + len(self.runtime.channel_ids)
            with self.subTest(policy=policy, packet_enabled=enabled, request=request), self._packet_configuration(enabled, channel_id):
                self.assertEqual(bot.ordinary_chat_configuration()["effective"], enabled)
                channel, generation, guard = await self.runtime._batch(
                    policy, request=request, answer=self._provider_answer, privileged=False,
                )
                generation.assert_awaited_once()
                guard.assert_awaited_once()
                self.assertEqual(channel.sent, [ANSWER])
                self._assert_sources(
                    generation.await_args.args[0], guard.await_args.kwargs["prompt_source_bases"],
                )

    async def test_batch_ledger_read_leaves_discord_event_loop_responsive(self):
        import bnl_memory_ledger as ledger

        loop = asyncio.get_running_loop()
        read_started = asyncio.Event()
        release_read = threading.Event()
        heartbeat_released_read = []
        real_read = ledger._main_public_assessment_occurrence_candidates

        def delayed_read(*args, **kwargs):
            if not heartbeat_released_read:
                loop.call_soon_threadsafe(read_started.set)
                # Only an event-loop callback can release the first read.
                # The timeout makes the old synchronous path fail safely.
                heartbeat_released_read.append(release_read.wait(timeout=1))
            return real_read(*args, **kwargs)

        async def heartbeat():
            await read_started.wait()
            release_read.set()

        pulse = asyncio.create_task(heartbeat())
        try:
            with self._packet_configuration(True, 8811), mock.patch.object(
                ledger, "_main_public_assessment_occurrence_candidates",
                side_effect=delayed_read,
            ):
                channel, generation, guard = await self.runtime._batch(
                    "sealed_test", request=REQUEST,
                    answer=self._provider_answer, privileged=False,
                )
        finally:
            release_read.set()
            pulse.cancel()
            await asyncio.gather(pulse, return_exceptions=True)

        self.assertEqual(heartbeat_released_read, [True])
        self.assertEqual(channel.sent, [ANSWER])
        generation.assert_awaited_once()
        self._assert_sources(
            generation.await_args.args[0],
            guard.await_args.kwargs["prompt_source_bases"],
        )

    async def test_batch_show_reads_and_final_validation_yield_to_event_loop(self):
        loop = asyncio.get_running_loop()
        reads = []
        names = (
            "maybe_build_bnl_read_model_context",
            "build_tiktok_show_evidence_context_for_turn",
            "build_named_public_conversation_context",
            "build_named_public_member_memory_context",
            "prompt_source_basis_failure",
        )
        originals = {name: getattr(bot, name) for name in names}

        def read(name, *args, **kwargs):
            release = threading.Event()
            # Only Discord's event loop can release this source read. The
            # timeout lets the old synchronous call site fail without hanging.
            loop.call_soon_threadsafe(release.set)
            reads.append((name, release.wait(timeout=1)))
            return originals[name](*args, **kwargs)

        with self._packet_configuration(True, 8811):
            from contextlib import ExitStack
            with ExitStack() as stack:
                for name in names:
                    stack.enter_context(mock.patch.object(
                        bot, name,
                        side_effect=lambda *args, _name=name, **kwargs: read(_name, *args, **kwargs),
                    ))
                channel, generation, guard = await self.runtime._batch(
                    "sealed_test", request=REQUEST,
                    answer=self._provider_answer, privileged=False,
                )

        self.assertEqual({name for name, _released in reads}, set(names))
        self.assertTrue(all(released for _name, released in reads), reads)
        self.assertEqual(channel.sent, [ANSWER])
        generation.assert_awaited_once()
        self._assert_sources(
            generation.await_args.args[0],
            guard.await_args.kwargs["prompt_source_bases"],
        )

    async def test_public_discord_source_scope_change_is_detected_before_send(self):
        prompt, metadata = await self.runtime._direct_prompt_async(
            "public_home", request=REQUEST, privileged=False,
        )
        bases = tuple(metadata["prompt_source_bases"])
        self._assert_sources(prompt, bases)
        with sqlite3.connect(bot.DB_FILE) as conn:
            conn.execute("UPDATE conversations SET channel_policy='internal_controlled' WHERE id=7101")
        self.assertEqual(bot.prompt_source_basis_failure(bases), "conversation_source_changed")
        _prompt, _fresh, changed, replacement_failed = bot.refresh_prompt_source_bases(prompt, bases)
        self.assertIn("conversation", changed)
        self.assertTrue(replacement_failed)  # Existing conversation source fence owns stale delivery.
        for basis in bases:
            if isinstance(basis, bot.FinalizedShowPromptSourceBasis):
                refreshed, changed = bot.refresh_prompt_source_basis(basis)
                self.assertFalse(changed)
                self.assertIn(TIKTOK_COMMENT, refreshed.rendered_context)

    async def test_discord_author_change_is_detected_before_send(self):
        prompt, metadata = await self.runtime._direct_prompt_async(
            "sealed_test", request=REQUEST, privileged=False,
        )
        bases = tuple(metadata["prompt_source_bases"])
        self._assert_sources(prompt, bases)
        with sqlite3.connect(bot.DB_FILE) as conn:
            conn.execute("UPDATE conversations SET user_name='Test Renamed' WHERE id=7101")
        self.assertEqual(bot.prompt_source_basis_failure(bases), "conversation_source_changed")

    async def test_batch_uses_two_fresh_show_reads_after_generation(self):
        for enabled in (False, True):
            channel_id = 8811 + len(self.runtime.channel_ids)
            with self.subTest(packet=enabled), self._packet_configuration(enabled, channel_id), mock.patch.object(
                bot, "refresh_prompt_source_basis", wraps=bot.refresh_prompt_source_basis,
            ) as refresh:
                channel, generation, _guard = await self.runtime._batch(
                    "sealed_test", request=REQUEST, answer=self._provider_answer,
                    privileged=False, channel_id=channel_id,
                )
            self.assertEqual(channel.sent, [ANSWER])
            generation.assert_awaited_once()
            show_reads = [call for call in refresh.call_args_list
                          if isinstance(call.args[0], bot.FinalizedShowPromptSourceBasis)]
            # One post-generation refresh and one fresh send check. The
            # response guard must not add a third full evidence reconstruction.
            self.assertEqual(len(show_reads), 2)

    async def _send_direct_source_reply(self):
        from test_conversation_batching import FakeChannel, FakeGuild, FakeMessage

        prompt, metadata = await self.runtime._direct_prompt_async(
            "sealed_test", request=REQUEST, privileged=False,
        )
        channel = FakeChannel(8810, name="bnl-testing", guild=FakeGuild(GUILD))
        message = FakeMessage(channel, REQUEST)
        plan = bot.plan_conversation_response(
            REQUEST, "sealed_test", route_mode=bot.ROUTE_MODE_NORMAL_CHAT,
            real_direct_target=True, batching_enabled=False,
            conversation_surface=bot.CONVERSATION_SURFACE_MENTION_OR_REPLY,
        )
        with mock.patch.object(bot, "_apply_direct_response_pacing", new=mock.AsyncMock()):
            await bot.send_planned_conversation_response(
                message, ANSWER, plan, prompt=prompt,
                source_context_available=metadata["source_context_available"],
                prompt_source_bases=metadata["prompt_source_bases"],
                mark_recent_direct=False,
            )
        return message

    async def test_direct_uses_two_fresh_show_reads_before_delivery(self):
        with mock.patch.object(
            bot, "refresh_prompt_source_basis", wraps=bot.refresh_prompt_source_basis,
        ) as refresh:
            message = await self._send_direct_source_reply()
        self.assertEqual(message.replies, [ANSWER])
        show_reads = [call for call in refresh.call_args_list
                      if isinstance(call.args[0], bot.FinalizedShowPromptSourceBasis)]
        self.assertEqual(len(show_reads), 2)

    async def test_batch_source_changes_after_guard_never_deliver_the_old_quote(self):
        real_stop = bot._stop_batch_typing
        corrected = "That earlier Discord quotation is no longer available."
        for column, value in (
            ("channel_policy", "internal_controlled"),
            ("content", "The corrected comment concerns blue drums."),
            ("user_id", 43),
        ):
            channel_id = 8811 + len(self.runtime.channel_ids)
            changed = False

            async def stop_typing(*args, **kwargs):
                nonlocal changed
                if kwargs.get("reason") == "response_ready" and not changed:
                    with sqlite3.connect(bot.DB_FILE) as conn:
                        conn.execute(f"UPDATE conversations SET {column}=? WHERE id=7101", (value,))
                    changed = True
                return await real_stop(*args, **kwargs)

            async def provider(*args, **kwargs):
                await self._provider_answer(*args, **kwargs)
                return corrected if changed else ANSWER

            with self.subTest(mutation=column), self._packet_configuration(True, channel_id), mock.patch.object(
                bot, "_stop_batch_typing", side_effect=stop_typing,
            ):
                channel, generation, _guard = await self.runtime._batch(
                    "sealed_test", request=REQUEST, answer=provider,
                    privileged=False, channel_id=channel_id,
                )
            self.assertTrue(changed)
            self.assertEqual(channel.sent, [corrected])
            self.assertEqual(generation.await_count, 2)
            with sqlite3.connect(bot.DB_FILE) as conn:
                saved = conn.execute(
                    "SELECT content FROM conversations WHERE role='model' AND channel_id=?",
                    (channel_id,),
                ).fetchall()
                conn.execute(
                    "UPDATE conversations SET channel_policy='public_home',content=?,user_id=? WHERE id=7101",
                    (DISCORD_COMMENT, SUBJECT),
                )
            self.assertEqual(saved, [(corrected,)])

    async def test_direct_source_privacy_change_after_guard_is_still_checked(self):
        changed = False
        real_quote_check = bot.exact_quote_presend_failure

        async def change_after_guard(*args, **kwargs):
            nonlocal changed
            with sqlite3.connect(bot.DB_FILE) as conn:
                conn.execute("UPDATE conversations SET channel_policy='internal_controlled' WHERE id=7101")
            changed = True
            return await real_quote_check(*args, **kwargs)

        corrected = "That earlier Discord quotation is no longer available."
        provider = mock.AsyncMock(return_value=bot.TrackedGenerationResponse(corrected, 1))
        with (
            mock.patch.object(bot, "get_tracked_gemini_response_with_optional_typing", new=provider),
            mock.patch.object(bot, "exact_quote_presend_failure", side_effect=change_after_guard),
        ):
            message = await self._send_direct_source_reply()
        self.assertTrue(changed)
        self.assertEqual(message.replies, [corrected])
        provider.assert_awaited_once()

    async def test_standalone_guard_keeps_its_final_source_check_after_repair(self):
        prompt, metadata = await self.runtime._direct_prompt_async(
            "sealed_test", request=REQUEST, privileged=False,
        )

        async def repair(*_args, **_kwargs):
            with sqlite3.connect(bot.DB_FILE) as conn:
                conn.execute("UPDATE conversations SET channel_policy='internal_controlled' WHERE id=7101")
            return ANSWER

        with mock.patch.object(bot, "get_gemini_response_with_optional_typing", side_effect=repair) as provider:
            response, diagnostics = await bot.apply_guarded_response_regeneration(
                "What do you need?", prompt=prompt, user_id=REQUESTER, guild_id=GUILD,
                route_mode=bot.ROUTE_MODE_NORMAL_CHAT, channel_policy="sealed_test",
                current_user_text=REQUEST, source_context_available=True,
                prompt_source_bases=metadata["prompt_source_bases"],
            )
        provider.assert_awaited_once()
        self.assertEqual(response, "")
        self.assertTrue(diagnostics["suppressed"])
        self.assertEqual(diagnostics["suppression_reason"], "conversation_source_changed_before_send")


if __name__ == "__main__":
    unittest.main()
