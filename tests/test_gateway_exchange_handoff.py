"""Validated Discord exchanges retain scope through the real packet route.

Only transport, external providers/source services, and pacing are fixtures.
Gateway addressing, Context, frame, native show selection, packet construction,
conversation persistence, and send-time source validation use their real owners.
"""

import asyncio
import copy
import hashlib
import json
import os
import sqlite3
import sys
import unittest
from contextlib import closing
from types import SimpleNamespace
from unittest import mock

import test_conversation_batching as discord_fixtures
import test_public_network_knowledge as fixtures
import test_tiktok_show_evidence_ledger as show_fixtures


bot = fixtures.bnl01_bot
ROOT = (
    "Which of my tracks have appeared in past shows? Give me the full artist "
    "credits and show dates, without submitter names."
)
SUBSET = "Which of those tracks includes a featured artist? Give me its show date too."
SAME_TRACK = "And for that same track, what is the full artist credit?"
SEMANTIC_SUBSET = (
    "Narrow this to the entry with a guest performer, including the performance date."
)
SEMANTIC_CREDIT = (
    "The credit for the one you just selected is the part I want expanded."
)
ROOT_REPLY = (
    "Neutral Signal by 6 Bit appeared on September 11, 2026. Neutral Collaboration "
    "by 6-Bit featuring Second Artist appeared on September 25, 2026."
)
SUBSET_REPLY = (
    "Neutral Collaboration by 6-Bit featuring Second Artist appeared on September 25, 2026."
)
CREDIT_REPLY = "Neutral Collaboration is credited to 6-Bit featuring Second Artist."
LONG_ROOT_REPLY = (ROOT_REPLY + "\n\n" + "Neutral delivery padding. " * 95).strip()
WEBSITE = (
    "Website public read model context:\naccessScope=public\nPublic archive view available."
)
NEW_REQUEST = (
    "For the September 11, 2026 show: which of my songs were submitted? "
    "Give me the full artist credits, without submitter names."
)
NEW_REPLY = "Neutral Signal is credited to 6 Bit in the September 11, 2026 show."


class GatewayChannel(discord_fixtures.FakeChannel):
    def __init__(self, channel_id):
        super().__init__(channel_id, name="barcode-bot", guild=discord_fixtures.FakeGuild(77))
        self.type = "text"
        self.guild.me = SimpleNamespace(id=999, bot=True)
        self.messages = {}
        self.next_reply_id = channel_id * 10

    def permissions_for(self, _member):
        return SimpleNamespace(
            view_channel=True, read_message_history=True, send_messages=True,
            add_reactions=True,
        )

    async def fetch_message(self, message_id):
        return self.messages[message_id]

    async def send(self, text, **kwargs):
        await super().send(text, **kwargs)
        return self.delivered_reply(text)

    def delivered_reply(self, text):
        self.next_reply_id += 1
        reply = SimpleNamespace(
            id=self.next_reply_id, content=text, channel=self, guild=self.guild,
            author=SimpleNamespace(id=999, display_name="BNL-01", bot=True),
        )
        self.messages[reply.id] = reply
        return reply


class GatewayMessage(discord_fixtures.FakeMessage):
    async def reply(self, text, **kwargs):
        self.replies.append(text)
        return self.channel.delivered_reply(text)


class GatewayExchangeHandoffTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.runtime = fixtures.PublicNetworkKnowledgeTests()
        await self.runtime.asyncSetUp()
        self.runtime._seed_finalized_show()
        self.stack = self.runtime.stack
        self.channel = GatewayChannel(897702)
        self.runtime.channel_ids.add(self.channel.id)
        self.author = discord_fixtures.FakeAuthor(42, "Test Member")
        self.contexts = []
        self.preparations = []
        self.generated_prompts = []
        self.reply = ROOT_REPLY
        self.followup_continues = True

        flags = {name: "true" for name in (
            "BNL_MEMORY_LEDGER_SHADOW_ENABLED", "BNL_MOMENT_ENGINE_SHADOW_ENABLED",
            "BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED", "BNL_RELATIONSHIP_V2_SHADOW_ENABLED",
            "BNL_UNIFIED_RESPONSE_ASSESSMENT_SHADOW_ENABLED",
            "BNL_UNIFIED_INTELLIGENCE_PACKET_SHADOW_ENABLED",
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_ENABLED",
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_PUBLIC_ENABLED",
        )}
        flags.update({
            "BNL_OWNER_USER_ID": "42", "BNL_PRIMARY_GUILD_ID": "77",
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_GUILD_IDS": "77",
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_USER_IDS": "42",
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_CHANNEL_IDS": str(self.channel.id),
        })
        self.stack.enter_context(mock.patch.dict(os.environ, flags))
        self.stack.enter_context(mock.patch.object(bot, "BNL_ACTIVE_BATCHING_ENABLED", False))
        self.stack.enter_context(mock.patch.object(bot, "_conversation_continuation_state", {}))
        self.stack.enter_context(mock.patch.object(bot, "_recent_direct_response_window", {}))
        self.stack.enter_context(mock.patch.dict(bot._recent_room_events, {}, clear=True))
        self.stack.enter_context(mock.patch.object(
            type(bot.client), "user", new_callable=mock.PropertyMock,
            return_value=SimpleNamespace(id=999, display_name="BNL-01", bot=True),
        ))
        self.stack.enter_context(mock.patch.object(
            bot.client, "get_channel", create=True,
            side_effect=lambda channel_id: self.channel if channel_id == self.channel.id else None,
        ))
        self.policy = self.stack.enter_context(mock.patch.object(
            bot, "resolve_channel_policy", return_value="public_context",
        ))
        self.stack.enter_context(mock.patch.object(bot, "get_guild_config", return_value=self.channel.id + 1000))
        self.stack.enter_context(mock.patch.object(bot, "_apply_direct_response_pacing", new=mock.AsyncMock()))
        self.stack.enter_context(mock.patch.object(bot.random, "random", return_value=1.0))
        self.stack.enter_context(mock.patch.object(
            bot, "maybe_build_source_context_for_direct_message", new=mock.AsyncMock(return_value=""),
        ))
        self.stack.enter_context(mock.patch.object(
            bot, "build_tiktok_show_evidence_context_for_turn", side_effect=fixtures.REAL_SHOW_CONTEXT_FOR_TURN,
        ))
        real_context = bot.build_conversation_context_v2_for_prompt
        real_generation = bot.maybe_generate_ordinary_chat_single_packet

        def capture_context(*args, **kwargs):
            rendered = real_context(*args, **kwargs)
            self.contexts.append((kwargs.get("result_out") or {}).get("result"))
            return rendered

        async def capture_generation(*args, **kwargs):
            preparation = dict(kwargs)
            self.preparations.append(preparation)
            execution = await real_generation(*args, **kwargs)
            preparation["final_prompt"] = execution.prompt if execution is not None else ""
            preparation["context_result"] = self.contexts[-1]
            return execution

        async def provider(_channel, prompt, *_args, **_kwargs):
            self.generated_prompts.append(prompt)
            return bot.TrackedGenerationResponse(self.reply, 1)

        async def legacy_provider(_channel, prompt, *_args, **_kwargs):
            self.generated_prompts.append(prompt)
            return self.reply

        async def followup_provider(_prompt, route, **_kwargs):
            if route != "conversation_followup_addressing":
                raise AssertionError("unexpected external generation route: " + route)
            return SimpleNamespace(
                success=True, text=json.dumps({"continue": self.followup_continues}),
            )

        self.stack.enter_context(mock.patch.object(bot, "build_conversation_context_v2_for_prompt", side_effect=capture_context))
        self.stack.enter_context(mock.patch.object(bot, "maybe_generate_ordinary_chat_single_packet", side_effect=capture_generation))
        self.stack.enter_context(mock.patch.object(bot, "get_tracked_gemini_response_with_optional_typing", side_effect=provider))
        self.stack.enter_context(mock.patch.object(bot, "get_gemini_response_with_optional_typing", side_effect=legacy_provider))
        self.followup_provider = self.stack.enter_context(mock.patch.object(
            bot, "_generate_gemini_content_result_async", side_effect=followup_provider,
        ))
        self._seed_artist_shows()

    async def asyncTearDown(self):
        await self.runtime.asyncTearDown()

    def _seed_artist_shows(self):
        shows = []
        for index, (date, next_date, title, credit) in enumerate((
            ("2026-09-11", "2026-09-12", "Neutral Signal", "6 Bit"),
            ("2026-09-25", "2026-09-26", "Neutral Collaboration", "6-Bit featuring Second Artist"),
        )):
            show = json.loads(json.dumps(show_fixtures.archived_show()).replace(
                "2026-08-28", date).replace("2026-08-29", next_date))
            show["sessionId"] = "gateway-exchange-%s" % index
            show["showDate"] = date
            show["trackRoster"] = [{
                "trackId": "gateway-exchange-track-%s" % index, "projectLabel": credit,
                "title": title, "submittedByTikTokHandle": "synthetic.submitter",
                "outcome": "finished", "lane": "regular", "submissionEventSequence": 1,
            }]
            shows.append(show)
        result = show_fixtures.sync_tiktok_show_evidence_ledgers(
            bot.DB_FILE, guild_id=77,
            read_model=show_fixtures.authorized_read_model({
                "currentShow": None, "latestShow": shows[-1], "shows": shows,
            }), artist_identity_index={}, environ=show_fixtures.ENABLED_QUEUE_ENV,
        )
        self.assertEqual(result["showsFinalized"], 2)

    async def _message(self, text, *, mentioned=False, answer=ROOT_REPLY, author=None):
        mention = SimpleNamespace(id=999, display_name="BNL-01", bot=True)
        message = GatewayMessage(
            self.channel, "<@999> " + text if mentioned else text,
            author=author or self.author, mentions=[mention] if mentioned else [],
        )
        self.channel.messages[message.id] = message
        self.latest_ingress_message = message
        self.reply = answer
        before = len(self.preparations)
        await asyncio.wait_for(bot.on_message(message), timeout=15)
        return message, self.preparations[before:]

    def _selected(self, preparation):
        native = [source for source in preparation["prompt_source_bases"]
                  if isinstance(source, bot.FinalizedShowPromptSourceBasis)]
        basis = preparation["basis"]
        self.assertIsNotNone(basis)
        return native, basis

    def _row_id(self, message):
        with closing(sqlite3.connect(bot.DB_FILE)) as conn:
            row = conn.execute(
                "SELECT id FROM conversations WHERE role='user' AND channel_id=? AND message_id=?",
                (self.channel.id, message.id),
            ).fetchone()
        self.assertIsNotNone(row)
        return row[0]

    def _assert_artist_scope(self, preparation, *human_scope):
        native, basis = self._selected(preparation)
        self.assertEqual(len(native), 1)
        self.assertIsNotNone(native[0].artist_identity_request)
        self.assertEqual(tuple(subject.user_id for subject in
                               native[0].artist_identity_request.frame_subjects), (42,))
        for text in human_scope:
            self.assertIn(text, native[0].selection_user_text)
            self.assertIn(text, preparation["final_prompt"])
        show_items = [item for item in basis.packet.items if item.lane == "show_episode"]
        self.assertEqual(len(show_items), 1)
        for source in (native[0].rendered_context, show_items[0].text):
            self.assertIn("Neutral Collaboration", source)
            self.assertIn("6-Bit featuring Second Artist", source)
            self.assertIn("2026-09-25", source)
            self.assertNotIn("Neon Fox", source)
            self.assertNotIn("Queue Light", source)
        self.assertFalse(basis.packet.diagnostics.invalid_invariants)
        frame = basis.assessment.situation_frame
        self.assertIsNotNone(frame)
        self.assertNotIn(frame.status, {"ambiguous", "unresolved"})
        self.assertIn("TURN RESPONSE PLAN:", preparation["final_prompt"])
        plan = preparation["final_prompt"].split("TURN RESPONSE PLAN:", 1)[1].split("VISIBLE RESPONSE CONTRACT:", 1)[0]
        self.assertIn("supportKind=packet", plan)
        self.assertNotIn("supportKind=clarify", plan)
        self.assertNotIn("supportKind=hold", plan)

    def _assert_transcript_only(self, preparation, *answers):
        native, basis = self._selected(preparation)
        context = preparation["context_result"]
        for answer in answers:
            self.assertIn(answer, context.rendered_context)
            self.assertIn(answer, preparation["final_prompt"])
            for source in native:
                self.assertNotIn(answer, source.selection_user_text)
                self.assertNotIn(answer, source.rendered_context)
            self.assertFalse(any(answer in item.text for item in basis.packet.items),
                             "BNL's earlier answer is transcript context, not factual packet support")

    async def _chain(self, *, store_answers, duplicate_root=False,
                     subset_text=SUBSET, same_text=SAME_TRACK):
        unanswered = None
        if duplicate_root:
            unanswered, ignored = await self._message(ROOT)
            self.assertEqual(ignored, [])
            self.assertEqual(unanswered.replies, [])
        with mock.patch.object(bot, "maybe_build_bnl_read_model_context", return_value="" if store_answers else WEBSITE):
            root, root_preparations = await self._message(ROOT, mentioned=True)
            subset, subset_preparations = await self._message(subset_text, answer=SUBSET_REPLY)
        same, same_preparations = await self._message(same_text, answer=CREDIT_REPLY)
        for message, preparations in ((root, root_preparations), (subset, subset_preparations), (same, same_preparations)):
            self.assertEqual(len(preparations), 1, "gateway must accept each directed or validated plain turn")
            self.assertTrue(message.replies, "the real send owner must commit the turn")
        self._assert_artist_scope(subset_preparations[0], ROOT)
        self._assert_artist_scope(same_preparations[0], ROOT, subset_text)
        subset_context = subset_preparations[0]["context_result"]
        same_context = same_preparations[0]["context_result"]
        self.assertEqual(subset_context.referent_status, "resolved")
        self.assertEqual(same_context.referent_status, "resolved")
        root_row_id, subset_row_id = self._row_id(root), self._row_id(subset)
        self.assertIn(root_row_id, subset_context.referent_request_row_ids)
        self.assertIn(root_row_id, same_context.referent_request_row_ids)
        self.assertIn(subset_row_id, same_context.referent_request_row_ids)
        if unanswered is not None:
            unanswered_row_id = self._row_id(unanswered)
            self.assertNotIn(unanswered_row_id, subset_context.referent_request_row_ids)
            self.assertNotIn(unanswered_row_id, same_context.referent_request_row_ids)
        self._assert_transcript_only(subset_preparations[0], ROOT_REPLY)
        self._assert_transcript_only(same_preparations[0], ROOT_REPLY, SUBSET_REPLY)
        with closing(sqlite3.connect(bot.DB_FILE)) as conn:
            rows = conn.execute(
                "SELECT role,content FROM conversations WHERE channel_id=? ORDER BY id",
                (self.channel.id,),
            ).fetchall()
        expected_roles = (["user"] if duplicate_root else []) + (
            ["user", "model", "user", "model", "user", "model"]
            if store_answers else ["user", "user", "user"]
        )
        self.assertEqual([role for role, _text in rows], expected_roles)
        if not store_answers:
            self.assertNotIn(ROOT_REPLY, [text for role, text in rows if role == "model"])
            self.assertNotIn(SUBSET_REPLY, [text for role, text in rows if role == "model"])
            self.assertNotIn(CREDIT_REPLY, [text for role, text in rows if role == "model"])

    async def test_unique_saved_exchange_retains_root_and_third_turn_lineage(self):
        await self._chain(store_answers=True)

    async def test_split_saved_answer_retains_its_full_original_model_owner(self):
        self.assertGreater(len(LONG_ROOT_REPLY), 2000)
        root, root_preparations = await self._message(ROOT, mentioned=True, answer=LONG_ROOT_REPLY)
        self.assertEqual(len(root_preparations), 1)
        self.assertTrue(root.replies)
        state = bot._conversation_continuation_state[
            bot._conversation_state_key(77, self.channel.id, 42)
        ]
        reply_ids = state["reply_message_ids"]
        self.assertGreater(len(reply_ids), 1, "the real send owner must split the long answer")
        self.assertEqual(state["no_store_reply_message_ids"], ())
        with closing(sqlite3.connect(bot.DB_FILE)) as conn:
            models = conn.execute(
                "SELECT id,content FROM conversations WHERE channel_id=? AND role='model'",
                (self.channel.id,),
            ).fetchall()
            links = conn.execute(
                "SELECT link.conversation_row_id,link.message_id FROM conversation_discord_message_links AS link "
                "JOIN conversations AS owner ON owner.id=link.conversation_row_id "
                "WHERE link.guild_id=? AND link.channel_id=? AND owner.role='model' ORDER BY link.message_id",
                (77, self.channel.id),
            ).fetchall()
        self.assertEqual(len(models), 1)
        owner_row_id, owner_text = models[0]
        self.assertEqual(owner_text, LONG_ROOT_REPLY)
        self.assertEqual(links, [(owner_row_id, message_id) for message_id in reply_ids])
        subset, preparations = await self._message(SUBSET, answer=SUBSET_REPLY)
        self.assertEqual(len(preparations), 1)
        self.assertTrue(subset.replies)
        self._assert_artist_scope(preparations[0], ROOT)
        context = preparations[0]["context_result"]
        exchange = context.active_exchange
        self.assertIsNotNone(exchange)
        self.assertEqual(exchange.reply_message_ids, reply_ids)
        self.assertEqual(exchange.reply_source_row_ids, (owner_row_id,) * len(reply_ids))
        self.assertEqual(exchange.reply_owner_texts, (LONG_ROOT_REPLY,) * len(reply_ids))
        self.assertEqual(context.selected_row_ids.count(owner_row_id), 1)
        self.assertEqual(exchange.unsaved_reply_message_ids, ())
        self.assertEqual(context.transient_referent_message_ids, ())
        self.assertIn(ROOT_REPLY, context.rendered_context)
        native, basis = self._selected(preparations[0])
        for source in native:
            self.assertNotIn(LONG_ROOT_REPLY, source.selection_user_text)
            self.assertNotIn("Neutral delivery padding.", source.rendered_context)
        self.assertFalse(any("Neutral delivery padding." in item.text for item in basis.packet.items),
                         "stored BNL prose remains transcript context rather than factual packet support")
        conversation = [source for source in preparations[0]["prompt_source_bases"]
                        if isinstance(source, bot.ConversationPromptSourceBasis)]
        self.assertEqual(len(conversation), 1)
        self.assertEqual(conversation[0].transient_referent_message_ids, ())
        self.assertEqual(conversation[0].transient_referent_texts, ())

    async def test_deleted_saved_model_owner_rejects_exchange_before_classification(self):
        root, root_preparations = await self._message(ROOT, mentioned=True)
        self.assertEqual(len(root_preparations), 1)
        self.assertTrue(root.replies)
        state = bot._conversation_continuation_state[
            bot._conversation_state_key(77, self.channel.id, 42)
        ]
        self.assertEqual(state["no_store_reply_message_ids"], ())
        with closing(sqlite3.connect(bot.DB_FILE)) as conn, conn:
            models = conn.execute(
                "SELECT id FROM conversations WHERE channel_id=? AND role='model'",
                (self.channel.id,),
            ).fetchall()
            self.assertEqual(len(models), 1)
            conn.execute("DELETE FROM conversations WHERE id=?", (models[0][0],))
        self.followup_provider.reset_mock()
        classifier = bot._classify_completed_followup_exchange
        with mock.patch.object(bot, "_classify_completed_followup_exchange", wraps=classifier) as classification:
            subset, preparations = await self._message(SUBSET, answer=SUBSET_REPLY)
        self.assertEqual(preparations, [])
        self.assertEqual(subset.replies, [])
        classification.assert_not_awaited()
        self.followup_provider.assert_not_awaited()

    async def _assert_batch_delivery_metadata(self, *, store_answer):
        mention = SimpleNamespace(id=999, display_name="BNL-01", bot=True)
        root = GatewayMessage(self.channel, "<@999> " + ROOT, author=self.author, mentions=[mention])
        self.channel.messages[root.id] = root
        self.reply = ROOT_REPLY
        with mock.patch.object(bot, "BNL_ACTIVE_BATCHING_ENABLED", True), mock.patch.object(
            bot, "BATCH_WINDOW_SECONDS", 0.01,
        ), mock.patch.object(bot, "BATCH_REPLY_COOLDOWN_SECONDS", 0), mock.patch.object(
            bot, "get_guild_config", return_value=self.channel.id,
        ), mock.patch.object(bot, "get_gemini_response", new=mock.AsyncMock(return_value=ROOT_REPLY)), mock.patch.object(
            bot, "maybe_build_bnl_read_model_context", return_value="" if store_answer else WEBSITE,
        ):
            await asyncio.wait_for(bot.on_message(root), timeout=15)
            for _ in range(4):
                task = bot._channel_tasks.get(self.channel.id)
                if task is None or task.done():
                    break
                await asyncio.wait_for(asyncio.shield(task), timeout=15)
        self.assertTrue(self.channel.sent, "the real batch send owner must deliver the synthetic answer")
        state = bot._conversation_continuation_state[
            bot._conversation_state_key(77, self.channel.id, 42)
        ]
        self.assertEqual(state["request_message_ids"], (root.id,))
        reply_ids = state["reply_message_ids"]
        self.assertTrue(reply_ids)
        self.assertEqual(
            state["reply_message_digests"],
            tuple((mid, hashlib.sha256(self.channel.messages[mid].content.encode("utf-8")).hexdigest())
                  for mid in reply_ids),
        )
        self.assertEqual(state["no_store_reply_message_ids"], () if store_answer else reply_ids)
        self.assertNotIn(ROOT_REPLY, str(state), "continuation metadata must retain references rather than response prose")
        with closing(sqlite3.connect(bot.DB_FILE)) as conn:
            models = conn.execute(
                "SELECT content FROM conversations WHERE channel_id=? AND role='model'",
                (self.channel.id,),
            ).fetchall()
        self.assertEqual(models, [(ROOT_REPLY,)] if store_answer else [])

    async def test_saved_batch_delivery_records_exact_reply_digests(self):
        await self._assert_batch_delivery_metadata(store_answer=True)

    async def test_no_store_batch_delivery_records_exact_reply_digests(self):
        await self._assert_batch_delivery_metadata(store_answer=False)

    async def test_unique_no_store_exchange_retains_transient_answers_and_lineage(self):
        await self._chain(store_answers=False)

    async def test_unanswered_duplicate_does_not_compete_with_answered_no_store_root(self):
        await self._chain(store_answers=False, duplicate_root=True)

    async def test_validated_semantic_continuation_does_not_require_starter_phrases(self):
        await self._chain(
            store_answers=False, duplicate_root=True,
            subset_text=SEMANTIC_SUBSET, same_text=SEMANTIC_CREDIT,
        )

    async def test_independent_direct_request_owns_current_selection(self):
        with mock.patch.object(bot, "maybe_build_bnl_read_model_context", return_value=WEBSITE):
            await self._message(ROOT, mentioned=True)
        current, preparations = await self._message(NEW_REQUEST, mentioned=True, answer=NEW_REPLY)
        self.assertEqual(len(preparations), 1)
        self.assertTrue(current.replies)
        native, basis = self._selected(preparations[0])
        self.assertEqual(len(native), 1)
        self.assertIn(NEW_REQUEST, native[0].selection_user_text)
        self.assertNotIn(ROOT, native[0].selection_user_text)
        self.assertIn("Neutral Signal", native[0].rendered_context)
        self.assertNotIn("Neutral Collaboration", native[0].rendered_context)
        self.assertFalse(basis.packet.diagnostics.invalid_invariants)

    async def test_other_member_cannot_inherit_requester_exchange(self):
        with mock.patch.object(bot, "maybe_build_bnl_read_model_context", return_value=WEBSITE):
            await self._message(ROOT, mentioned=True)
        other = discord_fixtures.FakeAuthor(43, "Other Test Member")
        message, preparations = await self._message(SUBSET, author=other)
        self.assertEqual(preparations, [])
        self.assertEqual(message.replies, [])

    async def test_invalid_delivered_author_cannot_establish_exchange_context(self):
        with mock.patch.object(bot, "maybe_build_bnl_read_model_context", return_value=WEBSITE):
            await self._message(ROOT, mentioned=True)
        state = bot._conversation_continuation_state[bot._conversation_state_key(77, self.channel.id, 42)]
        for reply_id in state["reply_message_ids"]:
            self.channel.messages[reply_id].author = SimpleNamespace(id=43, bot=False)
        message, preparations = await self._message(SUBSET)
        self.assertEqual(preparations, [])
        self.assertEqual(message.replies, [])

    async def test_policy_change_during_fetch_cannot_establish_exchange_context(self):
        with mock.patch.object(bot, "maybe_build_bnl_read_model_context", return_value=WEBSITE):
            await self._message(ROOT, mentioned=True)
        fetch = self.channel.fetch_message

        async def change_policy(message_id):
            source = await fetch(message_id)
            self.policy.return_value = "sealed_test"
            return source

        self.channel.fetch_message = change_policy
        message, preparations = await self._message(SUBSET)
        self.assertEqual(preparations, [])
        self.assertEqual(message.replies, [])

    async def _assert_exchange_invalidated_after_source_fence(
        self, mutation, *, root_answer=ROOT_REPLY, store_root=False,
    ):
        with mock.patch.object(bot, "maybe_build_bnl_read_model_context", return_value="" if store_root else WEBSITE):
            root, root_preparations = await self._message(ROOT, mentioned=True, answer=root_answer)
        self.assertEqual(len(root_preparations), 1)
        state_key = bot._conversation_state_key(77, self.channel.id, 42)
        prior_state = bot._conversation_continuation_state[state_key]
        prior_reply_ids = prior_state["reply_message_ids"]
        reply_id = prior_reply_ids[0]
        reply = self.channel.messages[reply_id]
        with closing(sqlite3.connect(bot.DB_FILE)) as conn:
            prior_model_rows = conn.execute(
                "SELECT content FROM conversations WHERE channel_id=? AND role='model' ORDER BY id",
                (self.channel.id,),
            ).fetchall()
        changed = False
        source_fence = bot.prompt_source_basis_failure_async

        async def invalidate_after_validation(*args, **kwargs):
            nonlocal changed
            failure = await source_fence(*args, **kwargs)
            if not changed and sys._getframe(1).f_code.co_name == "send_planned_conversation_response":
                mutation(root, reply)
                changed = True
            return failure

        with mock.patch.object(bot, "prompt_source_basis_failure_async", new=invalidate_after_validation):
            message, preparations = await self._message(SUBSET, answer=SUBSET_REPLY)
        self.assertEqual(len(preparations), 1, "the validated followup must reach generation before the exchange changes")
        self.assertTrue(changed, "the synthetic change must occur after the real presend validation await")
        self.assertEqual(message.replies, [], "stale exchange context must not reach Discord delivery")
        state = bot._conversation_continuation_state.get(state_key)
        if state is not None:
            self.assertNotIn(message.id, state["request_message_ids"])
            self.assertEqual(state["reply_message_ids"], prior_reply_ids)
        with closing(sqlite3.connect(bot.DB_FILE)) as conn:
            model_rows = conn.execute(
                "SELECT content FROM conversations WHERE channel_id=? AND role='model' ORDER BY id",
                (self.channel.id,),
            ).fetchall()
        self.assertEqual(model_rows, prior_model_rows, "a rejected delivery must not create a model conversation row")

    async def test_request_edit_after_final_source_await_rejects_stale_delivery(self):
        def edit(root, _reply):
            root.content = "This fictional recording request was withdrawn."

        await self._assert_exchange_invalidated_after_source_fence(edit)

    async def test_reply_edit_after_final_source_await_rejects_stale_delivery(self):
        def edit(_root, reply):
            reply.content = "This fictional answer has been corrected."

        await self._assert_exchange_invalidated_after_source_fence(edit)

    async def test_reply_deletion_after_final_source_await_rejects_stale_delivery(self):
        def delete(_root, reply):
            self.channel.messages.pop(reply.id)

        await self._assert_exchange_invalidated_after_source_fence(delete)

    async def test_secondary_chunk_link_deletion_after_final_source_await_rejects_stale_delivery(self):
        def delete(_root, _reply):
            state = bot._conversation_continuation_state[
                bot._conversation_state_key(77, self.channel.id, 42)
            ]
            self.assertGreater(len(state["reply_message_ids"]), 1)
            secondary_id = state["reply_message_ids"][1]
            with closing(sqlite3.connect(bot.DB_FILE)) as conn, conn:
                deleted = conn.execute(
                    "DELETE FROM conversation_discord_message_links WHERE guild_id=? AND channel_id=? AND message_id=?",
                    (77, self.channel.id, secondary_id),
                )
                self.assertEqual(deleted.rowcount, 1)

        await self._assert_exchange_invalidated_after_source_fence(
            delete, root_answer=LONG_ROOT_REPLY, store_root=True,
        )

    async def test_secondary_chunk_owner_reassignment_after_final_source_await_rejects_stale_delivery(self):
        def reassign(root, _reply):
            state = bot._conversation_continuation_state[
                bot._conversation_state_key(77, self.channel.id, 42)
            ]
            self.assertGreater(len(state["reply_message_ids"]), 1)
            secondary_id = state["reply_message_ids"][1]
            human_row_id = self._row_id(root)
            with closing(sqlite3.connect(bot.DB_FILE)) as conn, conn:
                changed = conn.execute(
                    "UPDATE conversation_discord_message_links SET conversation_row_id=? "
                    "WHERE guild_id=? AND channel_id=? AND message_id=?",
                    (human_row_id, 77, self.channel.id, secondary_id),
                )
                self.assertEqual(changed.rowcount, 1)

        await self._assert_exchange_invalidated_after_source_fence(
            reassign, root_answer=LONG_ROOT_REPLY, store_root=True,
        )

    async def test_policy_change_after_final_source_await_rejects_stale_delivery(self):
        def change(_root, _reply):
            self.policy.return_value = "sealed_test"

        await self._assert_exchange_invalidated_after_source_fence(change)

    async def test_fetched_current_edit_after_final_source_await_rejects_stale_delivery(self):
        def edit(_root, _reply):
            current = self.latest_ingress_message
            replacement = copy.copy(current)
            replacement.content = "The current fictional question has been replaced."
            self.channel.messages[current.id] = replacement
            self.assertEqual(current.content, SUBSET, "the cached gateway event remains unchanged")

        await self._assert_exchange_invalidated_after_source_fence(edit)

    async def test_fetched_current_deletion_after_final_source_await_rejects_stale_delivery(self):
        def delete(_root, _reply):
            current = self.latest_ingress_message
            self.channel.messages.pop(current.id)
            self.assertEqual(current.content, SUBSET, "the cached gateway event remains unchanged")

        await self._assert_exchange_invalidated_after_source_fence(delete)

    async def test_current_source_retraction_after_final_source_await_rejects_stale_delivery(self):
        def retract(_root, _reply):
            self._retire_root_source(self.latest_ingress_message, lineage=True)

        await self._assert_exchange_invalidated_after_source_fence(retract)

    async def _assert_current_source_changes_during_history_fetch(self, *, deleted):
        changed_during_fetch = False

        def install_fetch_change(root, _reply):
            fetch = self.channel.fetch_message

            async def change_current_after_history_fetch(message_id):
                nonlocal changed_during_fetch
                source = await fetch(message_id)
                if not changed_during_fetch and message_id == root.id:
                    current = self.latest_ingress_message
                    if deleted:
                        row_id = self._row_id(current)
                        with closing(sqlite3.connect(bot.DB_FILE)) as conn, conn:
                            conn.execute("DELETE FROM conversations WHERE id=?", (row_id,))
                    else:
                        self._retire_root_source(current, lineage=True)
                    changed_during_fetch = True
                return source

            self.channel.fetch_message = change_current_after_history_fetch

        await self._assert_exchange_invalidated_after_source_fence(install_fetch_change)
        self.assertTrue(changed_during_fetch, "current source control must change inside the historical fetch await")

    async def test_current_source_retraction_during_history_fetch_rejects_stale_delivery(self):
        await self._assert_current_source_changes_during_history_fetch(deleted=False)

    async def test_current_source_deletion_during_history_fetch_rejects_stale_delivery(self):
        await self._assert_current_source_changes_during_history_fetch(deleted=True)

    async def _assert_pre_ingress_exchange_rejected(self, mutation):
        with mock.patch.object(bot, "maybe_build_bnl_read_model_context", return_value=WEBSITE):
            root, preparations = await self._message(ROOT, mentioned=True)
        self.assertEqual(len(preparations), 1)
        self.assertTrue(root.replies)
        state = bot._conversation_continuation_state[
            bot._conversation_state_key(77, self.channel.id, 42)
        ]
        reply = self.channel.messages[state["reply_message_ids"][0]]
        mutation(root, reply)
        self.followup_provider.reset_mock()
        classifier = bot._classify_completed_followup_exchange
        with mock.patch.object(bot, "_classify_completed_followup_exchange", wraps=classifier) as classification:
            message, preparations = await self._message(SUBSET, answer=SUBSET_REPLY)
        self.assertEqual(preparations, [])
        self.assertEqual(message.replies, [])
        classification.assert_not_awaited()
        self.followup_provider.assert_not_awaited()

    def _retire_root_source(self, root, *, lineage=False):
        row_id = self._row_id(root)
        with closing(sqlite3.connect(bot.DB_FILE)) as conn, conn:
            entries = conn.execute(
                "SELECT entry_id FROM memory_ledger_entries WHERE guild_id=? "
                "AND source_table='conversations' AND source_row_id=? AND source_role='user'",
                (77, str(row_id)),
            ).fetchall()
            self.assertTrue(entries, "the real gateway must create the original source's ledger representation")
            for (entry_id,) in entries:
                if lineage:
                    conn.execute(
                        "INSERT INTO memory_ledger_lineage "
                        "(entry_id,guild_id,lineage_type,target_entry_id,created_at) "
                        "VALUES(?,?,'retracts',?,?)",
                        ("synthetic-exchange-retraction", 77, entry_id, "2026-10-09T12:00:00Z"),
                    )
                else:
                    conn.execute(
                        "UPDATE memory_ledger_entries SET lifecycle_status='forgotten' WHERE entry_id=?",
                        (entry_id,),
                    )

    async def test_request_edit_before_ingress_rejects_exchange_before_classification(self):
        def edit(root, _reply):
            root.content = "This fictional request has been replaced."

        await self._assert_pre_ingress_exchange_rejected(edit)

    async def test_reply_edit_before_ingress_rejects_exchange_before_classification(self):
        def edit(_root, reply):
            reply.content = "This fictional answer has been corrected."

        await self._assert_pre_ingress_exchange_rejected(edit)

    async def test_stored_request_edit_before_ingress_rejects_exchange_before_classification(self):
        def edit(root, _reply):
            row_id = self._row_id(root)
            with closing(sqlite3.connect(bot.DB_FILE)) as conn, conn:
                conn.execute(
                    "UPDATE conversations SET content=? WHERE id=?",
                    ("The stored fictional request was corrected.", row_id),
                )

        await self._assert_pre_ingress_exchange_rejected(edit)

    async def test_forgotten_root_before_ingress_rejects_exchange_before_classification(self):
        def forget(root, _reply):
            self._retire_root_source(root)

        await self._assert_pre_ingress_exchange_rejected(forget)

    async def test_retracted_root_before_ingress_rejects_exchange_before_classification(self):
        def retract(root, _reply):
            self._retire_root_source(root, lineage=True)

        await self._assert_pre_ingress_exchange_rejected(retract)


if __name__ == "__main__":
    unittest.main()
