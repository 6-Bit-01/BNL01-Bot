"""Normal Gemini inputs use public knowledge without experimental ownership.

The fixtures keep the real memory readers, Context/Frame builders, response
guards and source revalidation. Only external data/provider boundaries and
Discord transport are replaced. These are local delivery tests, not live
Gemini or production-memory receipts.
"""

import asyncio
import os
import sqlite3
import tempfile
import threading
import unittest
from contextlib import ExitStack
from types import SimpleNamespace
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl01_bot
import bnl_journal
import bnl_website_relay_state
import test_publication_read_adapters as publication_fixtures
from test_conversation_batching import FakeChannel, FakeGuild, FakeMessage


PUBLIC_MEMORY = "Test Member shared the Copper Kite instrumental with the room."
INTERNAL_MEMORY = "The private planning phrase is amber thimble."
SEALED_MEMORY = "The sealed rehearsal phrase is violet button."
REQUEST = "BNL, tell me about the Copper Kite instrumental?"
QUEUE_REQUEST = (
    "What did the Journal say about the queue, and is the queue open right now?"
)
QUEUE_CONTEXT = "Current public queue information: submissions are open."
JOURNAL_BODY = "The Copper Kite instrumental brought the room together."
RELAY_BODY = "The Copper Kite instrumental sparked a public listening exchange."


class PublicNetworkKnowledgeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.stack = ExitStack()
        self.tmp = self.stack.enter_context(tempfile.TemporaryDirectory())
        self.stack.enter_context(
            mock.patch.object(bnl01_bot, "DB_FILE", os.path.join(self.tmp, "memory.db"))
        )
        self.stack.enter_context(
            mock.patch.dict(
                os.environ,
                {
                    "BNL_ORDINARY_CHAT_SINGLE_PACKET_ENABLED": "false",
                    "BNL_SHARED_BRAIN_SYNTHESIS_CANARY_ENABLED": "false",
                    "BNL_PUBLIC_HOME_BROAD_RECALL_OWNER_ENABLED": "false",
                    "BNL_MEMORY_GOVERNANCE_LIVE_ENABLED": "false",
                    "BNL_RELATIONSHIP_V2_LIVE_ENABLED": "false",
                    "BNL_ACTIVE_ENGAGEMENT_V2_LIVE_ENABLED": "false",
                    "BNL_UNIFIED_MOMENT_CANARY_ENABLED": "false",
                    "BNL_CONVERSATION_CONTEXT_V2_ENABLED": "true",
                },
                clear=False,
            )
        )
        bnl01_bot.init_db()
        self.guild_id = 7700
        self.user_id = 100
        bnl01_bot.upsert_user_profile(self.user_id, self.guild_id, "Test Member")
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            for text, policy, trust in (
                (PUBLIC_MEMORY, "public_home", "source_safe_public"),
                (INTERNAL_MEMORY, "internal_controlled", "legacy_unknown"),
                (SEALED_MEMORY, "sealed_test", "legacy_unknown"),
            ):
                bnl01_bot._insert_memory_tier(
                    conn.cursor(),
                    self.user_id,
                    self.guild_id,
                    "long",
                    text,
                    0.95,
                    source_role="user",
                    source_channel_policy=policy,
                    source_trust=trust,
                    topic_key="music",
                )
        # These are adapter boundaries, not memory/Context/Frame substitutes.
        for name, value in (
            ("build_broadcast_memory_context", ""),
            ("build_queue_artist_memory_context", ""),
            ("build_tiktok_show_evidence_context_for_turn", ""),
            ("maybe_build_bnl_read_model_context", ""),
            ("choose_response_style", ("balanced", "Respond naturally.")),
            ("should_allow_greeting", False),
        ):
            self.stack.enter_context(mock.patch.object(bnl01_bot, name, return_value=value))
        self.channel_ids = set()

    async def asyncTearDown(self):
        for channel_id in self.channel_ids:
            task = bnl01_bot._channel_tasks.pop(channel_id, None)
            if task is not None and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            for name in (
                "_channel_buffers", "_channel_first_seen", "_channel_last_message_at",
                "_channel_last_reply_at", "_channel_generating", "_channel_generation_id",
                "_channel_preempted_generation_id", "_channel_message_interrupt_generation_id",
                "_channel_interrupt_handoff", "_channel_payload_wait_extended",
                "_channel_pending_request_intent", "_channel_pending_request_anchor",
                "_channel_generation_typing_pause_used", "_channel_typing_indicator_last_at",
                "_channel_batch_typing_sessions", "_channel_batch_typing_interrupt_revision",
                "_channel_batch_typing_applied_revision",
            ):
                getattr(bnl01_bot, name).pop(channel_id, None)
            for key in list(bnl01_bot._conversation_continuation_state):
                if len(key) >= 2 and key[1] == channel_id:
                    bnl01_bot._conversation_continuation_state.pop(key, None)
        bnl01_bot.purge_member_memory_caches(self.user_id, self.guild_id)
        self.stack.close()

    def _direct_prompt_inputs(self, policy, request=REQUEST, website_context="", privileged=True):
        metadata = {}
        channel_name = "bnl-testing" if policy == "sealed_test" else "barcode-bot"
        context_out = {}
        room_context = bnl01_bot.build_conversation_context_v2_for_prompt(
            guild_id=self.guild_id,
            current_user_id=self.user_id,
            channel_id=8810,
            channel_name=channel_name,
            channel_policy=policy,
            current_texts=[request],
            current_participants={self.user_id},
            is_direct_target=True,
            result_out=context_out,
        )
        orchestration = bnl01_bot.build_live_conversation_orchestration_decision(
            engagement_decision="answer",
            engagement_reason="direct_request",
            channel_policy=policy,
            addressings=(),
            context_result=context_out["result"],
            moment_situation=None,
            guild_id=self.guild_id,
            channel_id=8810,
            current_text=request,
            current_speaker_user_ids=(self.user_id,),
            current_speaker_labels=("Test Member",),
        )
        return dict(
            user_id=self.user_id,
            guild_id=self.guild_id,
            fallback_display_name="Test Member",
            clean_content=request,
            privileged=privileged,
            channel_policy=policy,
            channel_name=channel_name,
            channel_id=8810,
            route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
            is_direct_interaction=True,
            website_read_model_context=website_context,
            room_context=room_context,
            conversation_context_result=context_out["result"],
            conversation_orchestration=orchestration,
            prompt_metadata=metadata,
        )

    def _direct_prompt(self, policy, request=REQUEST, website_context="", privileged=True):
        inputs = self._direct_prompt_inputs(policy, request, website_context, privileged)
        prompt, *_ = bnl01_bot.build_user_aware_prompt(**inputs)
        return prompt, inputs["prompt_metadata"]

    async def _direct_prompt_async(self, policy, request=REQUEST, website_context="", privileged=True):
        inputs = self._direct_prompt_inputs(policy, request, website_context, privileged)
        prompt, *_ = await bnl01_bot.build_user_aware_prompt_async(**inputs)
        return prompt, inputs["prompt_metadata"]

    async def _batch(self, policy, request=REQUEST, answer=PUBLIC_MEMORY, privileged=True, participants=()):
        channel_id = 8811 + len(self.channel_ids)
        self.channel_ids.add(channel_id)
        channel = FakeChannel(
            channel_id,
            name="bnl-testing" if policy == "sealed_test" else "barcode-bot",
            guild=FakeGuild(self.guild_id),
        )
        now = bnl01_bot.datetime.now(bnl01_bot.PACIFIC_TZ)
        bnl01_bot._channel_buffers[channel.id].extend(
            participants or (("Test Member", request, self.user_id),)
        )
        bnl01_bot._channel_first_seen[channel.id] = now
        bnl01_bot._channel_last_message_at[channel.id] = now
        bnl01_bot._channel_last_reply_at[channel.id] = now - bnl01_bot.timedelta(hours=2)
        generation = (
            mock.AsyncMock(side_effect=answer) if callable(answer)
            else mock.AsyncMock(return_value=answer)
        )
        real_guard = bnl01_bot.apply_guarded_response_regeneration
        guard = mock.AsyncMock(wraps=real_guard)
        fake_bot = SimpleNamespace(id=999, display_name="BNL-01")
        with ExitStack() as runtime:
            runtime.enter_context(mock.patch.object(
                type(bnl01_bot.client), "user", new_callable=mock.PropertyMock,
                return_value=fake_bot,
            ))
            for name, value in (
                ("resolve_channel_policy", policy),
                ("get_guild_config", channel.id),
                ("is_privileged_member", privileged),
            ):
                runtime.enter_context(mock.patch.object(bnl01_bot, name, return_value=value))
            runtime.enter_context(mock.patch.object(bnl01_bot, "BNL_ACTIVE_BATCHING_ENABLED", True))
            runtime.enter_context(mock.patch.object(bnl01_bot, "POST_GENERATION_CAPTURE_GRACE_SECONDS", 0))
            runtime.enter_context(mock.patch.object(bnl01_bot, "get_gemini_response", new=generation))
            runtime.enter_context(mock.patch.object(bnl01_bot, "apply_guarded_response_regeneration", new=guard))
            await bnl01_bot._flush_channel_buffer(channel)
        return channel, generation, guard

    def _assert_public_memory_only(self, prompt):
        self.assertIn(PUBLIC_MEMORY, prompt)
        self.assertNotIn(INTERNAL_MEMORY, prompt)
        self.assertNotIn(SEALED_MEMORY, prompt)

    def _seed_publications(self, *, public_excluded=(), memory_excluded=()):
        bnl_journal.ensure_schema(bnl01_bot.DB_FILE)
        bnl_website_relay_state.ensure_schema(bnl01_bot.DB_FILE)
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            fixtures = SimpleNamespace(conn=conn)
            publication_fixtures.PublicationReadAdapterTests.add_journal(
                fixtures,
                "journal_copper_kite",
                title="Copper Kite Connections",
                excerpt=JOURNAL_BODY,
                body=JOURNAL_BODY,
            )
            publication_fixtures.PublicationReadAdapterTests.add_relay(
                fixtures,
                "relay_copper_kite",
                message=RELAY_BODY,
                directive="Keep the Copper Kite exchange going.",
            )
            conn.execute("UPDATE bnl_journal_entries SET guild_id=?", (self.guild_id,))
            conn.execute("UPDATE website_relay_history SET guild_id=?", (self.guild_id,))
        now = bnl01_bot.datetime.now(bnl01_bot.timezone.utc)
        snapshot = publication_fixtures.control_snapshot(
            public_excluded=public_excluded,
            memory_excluded=memory_excluded,
            observed_at=now.isoformat(),
            fresh_until=(now + bnl01_bot.timedelta(minutes=2)).isoformat(),
        )
        return self.stack.enter_context(mock.patch.object(
            bnl01_bot, "_journal_publication_control_snapshot_sync",
            return_value=(snapshot, "valid"),
        ))

    def _assert_publications_in_prompt_and_basis(self, prompt, bases):
        self.assertIn(JOURNAL_BODY, prompt)
        self.assertIn(RELAY_BODY, prompt)
        publication_bases = [
            b for b in bases if isinstance(b, bnl01_bot.PublicationPromptSourceBasis)
        ]
        self.assertEqual(len(publication_bases), 2)
        rendered = "\n".join(b.rendered_context for b in publication_bases)
        self.assertIn(JOURNAL_BODY, rendered)
        self.assertIn(RELAY_BODY, rendered)
        self.assertEqual(bnl01_bot.prompt_source_basis_failure(tuple(bases)), "")

    def test_privileged_direct_public_prompt_reads_only_public_memory(self):
        prompt, metadata = self._direct_prompt("public_home")
        self._assert_public_memory_only(prompt)
        bases = metadata["prompt_source_bases"]
        memory = next(b for b in bases if isinstance(b, bnl01_bot.MemoryPromptSourceBasis))
        self._assert_public_memory_only(memory.rendered_context)
        self.assertFalse(memory.is_owner_or_mod)
        self.assertEqual(bnl01_bot.prompt_source_basis_failure(bases), "")

    async def test_privileged_public_batch_preserves_direct_memory_boundary_through_send(self):
        direct_prompt, _ = self._direct_prompt("public_home")
        self._assert_public_memory_only(direct_prompt)
        channel, generation, guard = await self._batch("public_home")
        generation.assert_awaited_once()
        self._assert_public_memory_only(generation.await_args.args[0])
        guard.assert_awaited_once()
        memory = next(
            b for b in guard.await_args.kwargs["prompt_source_bases"]
            if isinstance(b, bnl01_bot.MemoryPromptSourceBasis)
        )
        self._assert_public_memory_only(memory.rendered_context)
        self.assertFalse(memory.is_owner_or_mod)
        self.assertEqual(channel.sent, [PUBLIC_MEMORY])

    def test_sealed_direct_can_read_public_memory_without_internal_or_sealed_tiers(self):
        for privileged in (False, True):
            with self.subTest(privileged=privileged):
                prompt, metadata = self._direct_prompt("sealed_test", privileged=privileged)
                self._assert_public_memory_only(prompt)
                bases = metadata["prompt_source_bases"]
                memory = next(b for b in bases if isinstance(b, bnl01_bot.MemoryPromptSourceBasis))
                self._assert_public_memory_only(memory.rendered_context)
                self.assertEqual(bnl01_bot.prompt_source_basis_failure(bases), "")

    async def test_sealed_batch_can_read_public_memory_and_send_normally(self):
        channel, generation, guard = await self._batch("sealed_test", privileged=False)
        generation.assert_awaited_once()
        self._assert_public_memory_only(generation.await_args.args[0])
        guard.assert_awaited_once()
        self.assertEqual(channel.sent, [PUBLIC_MEMORY])

    def test_mixed_publication_queue_direct_keeps_current_queue_with_packet_off(self):
        with mock.patch.object(
            bnl01_bot, "build_bnl_queue_packet_snapshot",
            return_value="Current queue snapshot: submissions open.",
        ):
            prompt, metadata = self._direct_prompt("sealed_test", QUEUE_REQUEST, QUEUE_CONTEXT)
        self.assertIn(QUEUE_CONTEXT, prompt)
        self.assertFalse(metadata["ordinary_chat_single_packet_applied"])
        self.assertEqual(metadata["ordinary_chat_single_packet_scope"].reason, "configuration_disabled")

    async def test_mixed_publication_queue_batch_keeps_current_queue_with_packet_off(self):
        with ExitStack() as sources:
            sources.enter_context(mock.patch.object(
                bnl01_bot, "maybe_build_bnl_read_model_context", return_value=QUEUE_CONTEXT,
            ))
            sources.enter_context(mock.patch.object(
                bnl01_bot, "build_bnl_queue_packet_snapshot",
                return_value="Current queue snapshot: submissions open.",
            ))
            channel, generation, guard = await self._batch(
                "sealed_test", QUEUE_REQUEST,
                "Submissions are open right now; I do not have a published Journal entry available here.",
            )
        generation.assert_awaited_once()
        self.assertIn(QUEUE_CONTEXT, generation.await_args.args[0])
        guard.assert_awaited_once()
        self.assertEqual(len(channel.sent), 1)

    async def test_relevant_publications_reach_normal_direct_prompt_guard_and_saved_reply(self):
        self._seed_publications()
        for policy in ("public_home", "sealed_test"):
            with self.subTest(policy=policy):
                prompt, metadata = await self._direct_prompt_async(policy, privileged=False)
                self._assert_publications_in_prompt_and_basis(prompt, metadata["prompt_source_bases"])
                self.assertTrue(metadata["source_context_available"])
                self.assertFalse(metadata["ordinary_chat_single_packet_applied"])
                answer = "The Journal described the room coming together around Copper Kite, and the Relay carried that listening exchange forward."
                channel = FakeChannel(8820, name="bnl-testing" if policy == "sealed_test" else "barcode-bot", guild=FakeGuild(self.guild_id))
                message = FakeMessage(channel, REQUEST)
                plan = bnl01_bot.plan_conversation_response(
                    REQUEST, policy, route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                    real_direct_target=True, batching_enabled=False,
                    conversation_surface=bnl01_bot.CONVERSATION_SURFACE_MENTION_OR_REPLY,
                )
                with mock.patch.object(bnl01_bot, "_apply_direct_response_pacing", new=mock.AsyncMock()):
                    decision = await bnl01_bot.send_planned_conversation_response(
                        message, answer, plan, prompt=prompt,
                        source_context_available=metadata["source_context_available"],
                        prompt_source_bases=metadata["prompt_source_bases"],
                        source_context_block="Current public publication context is present.",
                        mark_recent_direct=False,
                    )
                self.assertEqual(message.replies, [answer])
                self.assertTrue(decision.save_conversation)
                with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
                    row = conn.execute(
                        "SELECT content,channel_policy FROM conversations WHERE role='model' ORDER BY rowid DESC LIMIT 1"
                    ).fetchone()
                self.assertEqual(row, (answer, policy))


    async def test_direct_publication_fetch_leaves_discord_event_loop_responsive(self):
        controls = self._seed_publications()
        snapshot_result = controls.return_value
        fetch_started = threading.Event()
        loop_progress = threading.Event()
        release_fetch = threading.Event()
        fetch_saw_loop_progress = []

        def delayed_control_fetch(**_kwargs):
            fetch_started.set()
            release_fetch.wait(timeout=2)
            fetch_saw_loop_progress.append(loop_progress.is_set())
            return snapshot_result

        async def concurrent_discord_work():
            self.assertTrue(await asyncio.to_thread(fetch_started.wait, 2))
            loop_progress.set()
            release_fetch.set()

        controls.side_effect = delayed_control_fetch
        builder = asyncio.create_task(self._direct_prompt_async("public_home", privileged=False))
        concurrent_work = asyncio.create_task(concurrent_discord_work())
        try:
            prompt, metadata = await asyncio.wait_for(builder, timeout=5)
            await concurrent_work
        finally:
            release_fetch.set()
            await asyncio.gather(builder, concurrent_work, return_exceptions=True)

        self.assertEqual(fetch_saw_loop_progress, [True])
        self._assert_publications_in_prompt_and_basis(prompt, metadata["prompt_source_bases"])
        self.assertTrue(metadata["source_context_available"])

    async def test_relevant_publications_reach_untagged_sealed_batch_without_privileges(self):
        self._seed_publications()
        request = "Tell me about the Copper Kite instrumental?"
        answer = "The Journal described the room coming together around Copper Kite, and the Relay carried that listening exchange forward."
        channel, generation, guard = await self._batch(
            "sealed_test", request, answer, privileged=False,
        )
        generation.assert_awaited_once()
        guard.assert_awaited_once()
        self._assert_publications_in_prompt_and_basis(
            generation.await_args.args[0], guard.await_args.kwargs["prompt_source_bases"],
        )
        self.assertTrue(guard.await_args.kwargs["source_context_available"])
        self.assertEqual(channel.sent, [answer])

    def test_unrelated_general_knowledge_does_not_fetch_publication_controls(self):
        controls = self._seed_publications()
        prompt, metadata = self._direct_prompt(
            "sealed_test", "Briefly explain why a checksum can detect a corrupted file but cannot repair it.",
            privileged=False,
        )
        controls.assert_not_called()
        self.assertNotIn(JOURNAL_BODY, prompt)
        self.assertNotIn(RELAY_BODY, prompt)
        self.assertFalse(metadata["ordinary_chat_single_packet_applied"])

    async def test_unavailable_journal_controls_preserve_relay_queue_and_response(self):
        controls = self._seed_publications()
        controls.return_value = (None, "control_snapshot_unavailable")
        request = "What did the Journal say about Copper Kite, and is the queue open right now?"
        answer = "The Relay described the Copper Kite listening exchange, and submissions are open right now."
        with mock.patch.object(
            bnl01_bot, "maybe_build_bnl_read_model_context", return_value=QUEUE_CONTEXT,
        ):
            channel, generation, guard = await self._batch(
                "sealed_test", request, answer, privileged=False,
            )
        generation.assert_awaited_once()
        self.assertIn(RELAY_BODY, generation.await_args.args[0])
        self.assertIn(QUEUE_CONTEXT, generation.await_args.args[0])
        self.assertTrue(guard.await_args.kwargs["source_context_available"])
        self.assertEqual(channel.sent, [answer])

    def test_unchanged_publications_remain_valid_through_source_checks(self):
        controls = self._seed_publications()
        prompt, metadata = self._direct_prompt("public_home", privileged=False)
        self._assert_publications_in_prompt_and_basis(prompt, metadata["prompt_source_bases"])
        controls.reset_mock()
        refreshed_prompt, refreshed_bases, changed_kinds, failed = (
            bnl01_bot.refresh_prompt_source_bases(prompt, metadata["prompt_source_bases"])
        )
        self.assertEqual(refreshed_prompt, prompt)
        self.assertEqual(changed_kinds, ())
        self.assertFalse(failed)
        self.assertEqual(bnl01_bot.prompt_source_basis_failure(refreshed_bases), "")

    def test_changed_journal_revision_removes_only_its_old_prompt_block(self):
        controls = self._seed_publications()
        prompt, metadata = self._direct_prompt("public_home", privileged=False)
        self._assert_publications_in_prompt_and_basis(prompt, metadata["prompt_source_bases"])
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            publication_fixtures.PublicationReadAdapterTests.add_journal(
                SimpleNamespace(conn=conn),
                "journal_copper_kite",
                revision=2,
                title="Copper Kite Connections, Revised",
                excerpt="The Copper Kite exchange continued the next evening.",
                body="The Copper Kite exchange continued the next evening.",
                published_at="2026-09-06T01:00:00Z",
            )
            conn.execute("UPDATE bnl_journal_entries SET guild_id=?", (self.guild_id,))
        controls.reset_mock()
        refreshed_prompt, refreshed_bases, changed_kinds, failed = (
            bnl01_bot.refresh_prompt_source_bases(prompt, metadata["prompt_source_bases"])
        )
        self.assertNotIn(JOURNAL_BODY, refreshed_prompt)
        self.assertIn(RELAY_BODY, refreshed_prompt)
        self.assertIn(PUBLIC_MEMORY, refreshed_prompt)
        self.assertIn(REQUEST, refreshed_prompt)
        self.assertEqual(changed_kinds, ("publication",))
        self.assertFalse(failed)
        self.assertEqual(bnl01_bot.prompt_source_basis_failure(refreshed_bases), "")

    def test_full_controls_preserved_and_memory_exclusion_differs_from_exact_lookup(self):
        controls = self._seed_publications(
            public_excluded=("unrelated_hidden_entry",),
            memory_excluded=("journal_copper_kite", "unrelated_memory_entry"),
        )
        context_prompt, _ = self._direct_prompt("public_home", privileged=False)
        self.assertNotIn(JOURNAL_BODY, context_prompt)
        self.assertIn(RELAY_BODY, context_prompt)
        request = 'show the Journal titled "Copper Kite Connections"'
        prompt, metadata = self._direct_prompt("sealed_test", request, privileged=False)
        self.assertIn(JOURNAL_BODY, prompt)
        journal = next(b for b in metadata["prompt_source_bases"]
                       if isinstance(b, bnl01_bot.PublicationPromptSourceBasis) and b.source_kind == "journal")
        self.assertEqual(journal.journal_control_snapshot, controls.return_value[0])
        self.assertEqual(journal.journal_control_snapshot.public_excluded_entry_ids, ("unrelated_hidden_entry",))
        self.assertEqual(journal.journal_control_snapshot.memory_excluded_entry_ids,
                         ("journal_copper_kite", "unrelated_memory_entry"))
        self.assertEqual(bnl01_bot.prompt_source_basis_failure(metadata["prompt_source_bases"]), "")

    async def test_public_batch_stores_one_reply_with_all_participants(self):
        self._seed_publications()
        answer = "The Journal covered the Copper Kite listening exchange, and the Relay carried it forward."
        channel, generation, _ = await self._batch(
            "public_home", answer=answer,
            participants=(("Test Member", "We discussed the Copper Kite instrumental.", self.user_id),
                          ("Second Member", "BNL, what did the Journal and Relay say about Copper Kite?", 202)),
        )
        self.assertEqual(channel.sent, [answer])
        self.assertIn(JOURNAL_BODY, generation.await_args.args[0])
        self.assertIn(RELAY_BODY, generation.await_args.args[0])
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            rows = conn.execute("SELECT id,user_id,content,channel_policy FROM conversations WHERE role='model'").fetchall()
            self.assertEqual(len(rows), 1)
            row_id, user_id, stored, policy = rows[0]
            links = conn.execute("SELECT user_id FROM conversation_response_participants WHERE conversation_row_id=? ORDER BY user_id", (row_id,)).fetchall()
        self.assertEqual((user_id, stored, policy), (0, answer, "public_home"))
        self.assertEqual(links, [(self.user_id,), (202,)])
        self.assertNotIn("Published Journal context", stored)

    async def test_sealed_publication_reply_stays_out_of_public_recall_and_durable_memory(self):
        self._seed_publications()
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            tiers_before = conn.execute("SELECT COUNT(*) FROM memory_tiers").fetchone()[0]
            facts_before = conn.execute("SELECT COUNT(*) FROM user_memory_facts").fetchone()[0]
        answer = "The Copper Kite listening exchange felt like violet lanterns over an open room."
        channel, _, _ = await self._batch("sealed_test", answer=answer, privileged=False)
        self.assertEqual(channel.sent, [answer])
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            rows = conn.execute("SELECT content,channel_policy FROM conversations WHERE role='model'").fetchall()
            self.assertEqual(rows, [(answer, "sealed_test")])
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM memory_tiers").fetchone()[0], tiers_before)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM user_memory_facts").fetchone()[0], facts_before)
        public_prompt, _ = self._direct_prompt("public_home", privileged=False)
        self.assertNotIn(answer, public_prompt)

    async def test_journal_changes_during_generation_only_corrected_reply_is_delivered_and_saved(self):
        self._seed_publications()
        initial_answer = "The Journal said the Copper Kite instrumental brought the room together."
        final_answer = "The Relay described a public listening exchange around Copper Kite."
        calls = []
        async def generate(prompt, *_args, **kwargs):
            calls.append(prompt)
            if len(calls) == 1:
                with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
                    publication_fixtures.PublicationReadAdapterTests.add_journal(
                        SimpleNamespace(conn=conn), "journal_copper_kite", revision=2,
                        title="Copper Kite Correction", excerpt="The exchange moved to a later evening.",
                        body="The exchange moved to a later evening.", published_at="2026-09-06T01:00:00Z",
                    )
                    conn.execute("UPDATE bnl_journal_entries SET guild_id=?", (self.guild_id,))
                return initial_answer
            return final_answer
        channel, generation, _ = await self._batch("public_home", answer=generate)
        self.assertEqual(generation.await_count, 2)
        self.assertIn(JOURNAL_BODY, calls[0])
        self.assertNotIn(JOURNAL_BODY, calls[1])
        self.assertIn(RELAY_BODY, calls[1])
        self.assertEqual(channel.sent, [final_answer])
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            rows = conn.execute("SELECT content FROM conversations WHERE role='model'").fetchall()
        self.assertEqual(rows, [(final_answer,)])


if __name__ == "__main__":
    unittest.main()
