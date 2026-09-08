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
import bnl_memory_ledger as ledger
import bnl_moment_engine as moments
import bnl_website_relay_state
import test_publication_read_adapters as publication_fixtures
import test_tiktok_show_evidence_ledger as show_fixtures
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
REAL_SHOW_CONTEXT_FOR_TURN = bnl01_bot.build_tiktok_show_evidence_context_for_turn


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

    def _seed_finalized_show(self):
        fixture = show_fixtures.TikTokShowEvidenceLedgerTests()
        fixture.seed_source_and_memory(bnl01_bot.DB_FILE)
        result = show_fixtures.sync_tiktok_show_evidence_ledgers(
            bnl01_bot.DB_FILE, guild_id=77,
            read_model=show_fixtures.authorized_read_model({
                "currentShow": None, "latestShow": show_fixtures.archived_show(),
                "shows": [],
            }),
            artist_identity_index=show_fixtures.artist_index(),
            environ=show_fixtures.ENABLED_QUEUE_ENV,
        )
        self.assertEqual(result["showsFinalized"], 1)
        self.guild_id = 77
        self.user_id = 42
        self.stack.enter_context(mock.patch.dict(os.environ, show_fixtures.ENABLED_QUEUE_ENV))
        self.stack.enter_context(mock.patch.object(
            bnl01_bot, "build_tiktok_show_evidence_context_for_turn",
            side_effect=REAL_SHOW_CONTEXT_FOR_TURN,
        ))

    def _assert_show_source(self, prompt, bases):
        selected = tuple(b for b in bases if isinstance(b, bnl01_bot.FinalizedShowPromptSourceBasis))
        self.assertEqual(len(selected), 1)
        self.assertIn("Source-linked authored examples:", prompt)
        self.assertIn("the green visuals during this song are wild.", prompt)
        self.assertNotIn("This private row must never enter", prompt)
        self.assertEqual(bnl01_bot.prompt_source_basis_failure(selected), "")
        return selected

    async def test_finalized_show_authored_sources_reach_real_direct_and_batch_assembly(self):
        self._seed_finalized_show()
        request = "What did the chat say during the show on 2026-08-28?"
        for policy in ("public_home", "sealed_test"):
            with self.subTest(policy=policy, route="direct"):
                prompt, metadata = await self._direct_prompt_async(policy, request=request)
                self._assert_show_source(prompt, metadata["prompt_source_bases"])
            with self.subTest(policy=policy, route="batch"):
                _channel, generation, guard = await self._batch(policy, request=request)
                self.assertTrue(generation.await_count)
                self._assert_show_source(
                    generation.await_args.args[0], guard.await_args.kwargs["prompt_source_bases"],
                )

    async def test_finalized_source_disappearance_uses_existing_refresh_without_substitution(self):
        self._seed_finalized_show()
        prompt, metadata = await self._direct_prompt_async(
            "sealed_test", request="What did the chat say during the show on 2026-08-28?",
        )
        bases = self._assert_show_source(prompt, metadata["prompt_source_bases"])
        replacement_show = show_fixtures.archived_show()
        replacement_show["sessionId"] = "test-replacement-show"
        show_fixtures.sync_tiktok_show_evidence_ledgers(
            bnl01_bot.DB_FILE, guild_id=77,
            read_model=show_fixtures.authorized_read_model({
                "currentShow": None, "latestShow": replacement_show,
                "shows": [show_fixtures.archived_show()],
            }),
            artist_identity_index=show_fixtures.artist_index(),
            environ=show_fixtures.ENABLED_QUEUE_ENV,
        )
        self.assertEqual(bnl01_bot.prompt_source_basis_failure(bases), "")
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            conn.execute(
                "DELETE FROM tiktok_show_evidence_ledgers WHERE guild_id=77 AND show_key=?",
                (bases[0].show_keys[0],),
            )
            self.assertEqual(conn.execute(
                "SELECT count(*) FROM tiktok_show_evidence_ledgers WHERE guild_id=77",
            ).fetchone()[0], 1)
        self.assertEqual(bnl01_bot.prompt_source_basis_failure(bases), "show_episode_source_changed")
        refreshed_prompt, fresh, changed, failed = bnl01_bot.refresh_prompt_source_bases(prompt, bases)
        self.assertTrue(changed)
        self.assertFalse(failed)
        self.assertNotIn("the green visuals during this song are wild.", refreshed_prompt)
        self.assertFalse(fresh[0].rendered_context)

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


    def _seed_topic_recall_sources(self, *, cross_tier=False):
        """Known low-salience answers compete with generic recall wording."""
        targets = {
            100: "The paper comet melody has muted bells.",
            101: "The glass orchard arrangement has brushed cymbals.",
        }
        members = (
            ("Test Member", "BNL, what do you remember me telling you about the paper comet?", 100),
            ("Second Member", "BNL, what do you remember me telling you about the glass orchard?", 101),
        )
        target_ids = {}
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            conn.execute("DELETE FROM memory_tiers")
            for uid, text in targets.items():
                tier = "medium" if cross_tier and uid == 100 else "long"
                target_ids[uid] = bnl01_bot._insert_memory_tier(
                    conn.cursor(), uid, self.guild_id, tier, text, 0.1,
                    source_role="user", source_channel_policy="public_home",
                    source_trust="source_safe_public", topic_key="music",
                )
                for index in range(6):
                    for competitor_tier in (("short", tier) if cross_tier else (tier,)):
                        bnl01_bot._insert_memory_tier(
                            conn.cursor(), uid, self.guild_id, competitor_tier,
                            f"Remember the cedar studio session {index}: warm bass with steady drums.",
                            0.99 - index * 0.01,
                            source_role="user", source_channel_policy="public_home",
                            source_trust="source_safe_public", topic_key="music",
                        )
        return members, targets, target_ids

    async def test_targeted_recall_survives_salience_competitors_in_single_prompt(self):
        members, targets, _target_ids = self._seed_topic_recall_sources()
        channel, generation, guard = await self._batch(
            "sealed_test", request=members[0][1], answer=targets[100], privileged=False,
        )
        generation.assert_awaited_once()
        basis = next(b for b in guard.await_args.kwargs["prompt_source_bases"]
                     if isinstance(b, bnl01_bot.MemoryPromptSourceBasis))
        self.assertIn(targets[100], basis.rendered_context)
        self.assertIn(basis.rendered_context, generation.await_args.args[0])
        self.assertNotIn(targets[101], basis.rendered_context)
        self.assertEqual(channel.sent, [targets[100]])

    async def test_targeted_group_recall_preserves_each_topic_in_both_orders(self):
        members, targets, _target_ids = self._seed_topic_recall_sources()
        answer = "Test Member described muted bells; Second Member described brushed cymbals."
        for turns in (members, tuple(reversed(members))):
            with self.subTest(order=tuple(item[2] for item in turns)):
                channel, generation, guard = await self._batch(
                    "sealed_test", participants=turns, answer=answer, privileged=False,
                )
                generation.assert_awaited_once()
                bases = tuple(b for b in guard.await_args.kwargs["prompt_source_bases"]
                              if isinstance(b, bnl01_bot.MemoryPromptSourceBasis))
                self.assertEqual([basis.user_id for basis in bases], [item[2] for item in turns])
                for basis in bases:
                    self.assertIn(targets[basis.user_id], basis.rendered_context)
                    for other_uid, other_target in targets.items():
                        if other_uid != basis.user_id:
                            self.assertNotIn(other_target, basis.rendered_context)
                    self.assertIn(basis.rendered_context, generation.await_args.args[0])
                self.assertEqual(channel.sent, [answer])

    async def test_targeted_medium_and_long_recall_survive_group_compaction(self):
        members, targets, _target_ids = self._seed_topic_recall_sources(cross_tier=True)
        channel, generation, guard = await self._batch(
            "sealed_test", participants=members, privileged=False,
            answer="Test Member described muted bells; Second Member described brushed cymbals.",
        )
        generation.assert_awaited_once()
        bases = tuple(b for b in guard.await_args.kwargs["prompt_source_bases"]
                      if isinstance(b, bnl01_bot.MemoryPromptSourceBasis))
        self.assertEqual([basis.user_id for basis in bases], [100, 101])
        for basis, tier in zip(bases, ("medium", "long")):
            self.assertEqual(basis.member_budget_chars, 448)
            self.assertLessEqual(len(basis.rendered_context), basis.member_budget_chars)
            self.assertIn(targets[basis.user_id], basis.rendered_context)
            self.assertIn(f"[{tier} hint; not quote authority]", basis.rendered_context)
            self.assertIn(basis.rendered_context, generation.await_args.args[0])
        injected = "\n\n" + "\n\n".join(basis.rendered_context for basis in bases)
        self.assertLessEqual(len(injected), bnl01_bot.MEMORY_PROMPT_BUDGET_PUBLIC)
        self.assertEqual(len(channel.sent), 1)

    def test_repeated_request_wording_does_not_displace_distinctive_topic(self):
        target = "The saffron melody has muted bells."
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            conn.execute("DELETE FROM memory_tiers")
            bnl01_bot._insert_memory_tier(
                conn.cursor(), 100, self.guild_id, "short", target, 0.1,
                source_role="user", source_channel_policy="public_home",
                source_trust="source_safe_public", topic_key="music",
            )
            for index in range(6):
                bnl01_bot._insert_memory_tier(
                    conn.cursor(), 100, self.guild_id, "short",
                    f"I was telling you about cedar studio session {index} and warm bass.",
                    0.99, source_role="user", source_channel_policy="public_home",
                    source_trust="source_safe_public", topic_key="music",
                )
        metadata = {}
        context = bnl01_bot.build_user_memory_context(
            100, self.guild_id, route_mode="normal_chat", channel_policy="sealed_test",
            user_text="BNL, what was I telling you about saffron?",
            current_direct=True, source_metadata=metadata,
        )
        self.assertIn(target, context)
        self.assertIn(target, metadata["memory_context_units"][0].text)

    def test_broad_and_unmatched_recall_preserve_salience_order(self):
        _members, targets, _target_ids = self._seed_topic_recall_sources()
        for query in (
            "What do you remember about me?",
            "BNL, what do you remember about the saffron telescope?",
            "BNL, what do you remember about a historical member trace?",
        ):
            with self.subTest(query=query):
                metadata = {}
                context = bnl01_bot.build_user_memory_context(
                    100, self.guild_id, route_mode="normal_chat",
                    channel_policy="sealed_test", user_text=query,
                    current_direct=True, source_metadata=metadata,
                )
                units = metadata["memory_context_units"]
                self.assertEqual(len(units), 3)
                for unit, index in zip(units, range(3)):
                    self.assertIn(f"Remember the cedar studio session {index}:", unit.text)
                self.assertNotIn(targets[100], context)

    def test_excluded_matching_sources_cannot_change_eligible_recall_ranking(self):
        members, targets, _target_ids = self._seed_topic_recall_sources()
        before = {}
        read_args = dict(
            route_mode="normal_chat", channel_policy="sealed_test",
            user_text=members[0][1], current_direct=True,
        )
        bnl01_bot.build_user_memory_context(100, self.guild_id, source_metadata=before, **read_args)
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            for uid, guild, policy, trust in (
                (100, self.guild_id, "internal_controlled", "legacy_unknown"),
                (100, self.guild_id, "sealed_test", "legacy_unknown"),
                (100, self.guild_id, "public_home", "legacy_unknown"),
                (9999, self.guild_id, "public_home", "source_safe_public"),
                (100, self.guild_id + 1, "public_home", "source_safe_public"),
            ):
                for index in range(8):
                    bnl01_bot._insert_memory_tier(
                        conn.cursor(), uid, guild, "long",
                        f"The paper comet confidential variant {index} has a hidden melodic pattern.",
                        1.0, source_role="user", source_channel_policy=policy,
                        source_trust=trust, topic_key="music",
                    )
        after = {}
        context = bnl01_bot.build_user_memory_context(100, self.guild_id, source_metadata=after, **read_args)
        self.assertEqual(before["memory_context_units"], after["memory_context_units"])
        self.assertIn(targets[100], context)
        self.assertNotIn("confidential variant", context)

    async def test_matching_approved_color_survives_incidental_topic_under_group_budget(self):
        self._seed_group_member_sources(facts=True)
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            cursor = conn.execute(
                "INSERT INTO conversations (user_id,user_name,guild_id,channel_name,channel_policy,channel_id,role,content) VALUES (?,?,?,?,?,?,?,?)",
                (100, "Test Member", self.guild_id, "barcode-bot", "public_home", 8800, "user", "My favorite color is blue."),
            )
            source_row_id = cursor.lastrowid
            bnl01_bot._insert_memory_tier(
                conn.cursor(), 100, self.guild_id, "long",
                "The blue lantern workshop featured warm bass and crisp drums in a relaxed afternoon session.",
                0.99, source_role="user", source_channel_policy="public_home",
                source_trust="source_safe_public", topic_key="music",
            )
        bnl01_bot.upsert_user_fact(
            100, self.guild_id, "favorite_color", "blue",
            source_conversation_row_id=source_row_id,
            source_channel_policy="public_home", source_directed=True,
        )
        members = (
            ("Test Member", "BNL, what favorite color did I tell you, was it blue?", 100),
            ("Second Member", "BNL, what is my favorite color?", 101),
        )
        with mock.patch.object(bnl01_bot, "MEMORY_PROMPT_BUDGET_PUBLIC", 448):
            channel, generation, guard = await self._batch(
                "sealed_test", participants=members, privileged=False,
                answer="Test Member chose blue; Second Member chose violet.",
            )
        bases = tuple(b for b in guard.await_args.kwargs["prompt_source_bases"]
                      if isinstance(b, bnl01_bot.MemoryPromptSourceBasis))
        self.assertEqual([basis.user_id for basis in bases], [100, 101])
        for basis, color in zip(bases, ("blue", "violet")):
            self.assertIn(f"[changeable self-report] Favorite color: {color}", basis.rendered_context)
            self.assertIn(basis.rendered_context, generation.await_args.args[0])
            self.assertLessEqual(len(basis.rendered_context), 222)
        self.assertEqual(channel.sent, ["Test Member chose blue; Second Member chose violet."])

    async def test_selected_recall_changed_or_removed_before_send_is_revalidated(self):
        for mutation in ("change", "remove"):
            with self.subTest(mutation=mutation):
                members, targets, target_ids = self._seed_topic_recall_sources()
                replacement = "The glass orchard arrangement has bright triangles."
                final_answer = (
                    "Test Member described muted bells; Second Member now uses bright triangles."
                    if mutation == "change" else
                    "Test Member described muted bells; the other arrangement is not available now."
                )
                prompts = []

                async def generate(prompt, *_args, **_kwargs):
                    prompts.append(prompt)
                    if len(prompts) == 1:
                        self.assertIn(targets[101], prompt)
                        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
                            if mutation == "change":
                                conn.execute("UPDATE memory_tiers SET summary=? WHERE id=?", (replacement, target_ids[101]))
                            else:
                                conn.execute("DELETE FROM memory_tiers WHERE id=?", (target_ids[101],))
                        return "Test Member described muted bells; Second Member described brushed cymbals."
                    self.assertNotIn(targets[101], prompt)
                    if mutation == "change":
                        self.assertIn(replacement, prompt)
                    return final_answer

                channel, generation, _guard = await self._batch(
                    "sealed_test", participants=members, answer=generate, privileged=False,
                )
                self.assertEqual(generation.await_count, 2)
                self.assertEqual(channel.sent, [final_answer])
                with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
                    saved = conn.execute(
                        "SELECT content FROM conversations WHERE role='model' AND channel_id=?",
                        (channel.id,),
                    ).fetchall()
                self.assertEqual(saved, [(final_answer,)])

    def _seed_group_member_sources(self, *, facts=False, count=2):
        names = ("Amber Instrumentalist", "Violet Sound Designer", "Copper Percussionist", "Indigo Studio Artist", "Cyan Music Producer", "Green Sound Engineer", "Blue Session Artist", "Golden Instrumentalist")
        colors = ("amber", "violet", "copper", "indigo", "cyan", "green", "blue", "gold")
        members = []
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            conn.execute("DELETE FROM memory_tiers")
            for index in range(count):
                uid, name, color = 100 + index, names[index], colors[index]
                members.append((name, f"BNL, remember the {color} music project?", uid))
                if not facts:
                    for tier, part in (("short", "intro"), ("medium", "mix"), ("long", "release")):
                        bnl01_bot._insert_memory_tier(
                            conn.cursor(), uid, self.guild_id, tier,
                            f"The {color} project {part} has soft chords.", 0.99,
                            source_role="user", source_channel_policy="public_home",
                            source_trust="source_safe_public", topic_key="music",
                        )
                for policy, trust, summary in (
                    ("internal_controlled", "legacy_unknown", f"Private {color} infrastructure access."),
                    ("sealed_test", "legacy_unknown", f"Sealed {color} hidden rehearsal."),
                ):
                    bnl01_bot._insert_memory_tier(
                        conn.cursor(), uid, self.guild_id, "long", summary, 1.0,
                        source_role="user", source_channel_policy=policy, source_trust=trust,
                    )
            for uid, guild in ((9999, self.guild_id), (100, self.guild_id + 1)):
                bnl01_bot._insert_memory_tier(
                    conn.cursor(), uid, guild, "long", "Unrelated secret project marker.", 1.0,
                    source_role="user", source_channel_policy="public_home", source_trust="source_safe_public",
                )
        if facts:
            for index, (name, _request, uid) in enumerate(members):
                with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
                    cursor = conn.execute(
                        "INSERT INTO conversations (user_id,user_name,guild_id,channel_name,channel_policy,channel_id,role,content) VALUES (?,?,?,?,?,?,?,?)",
                        (uid, name, self.guild_id, "barcode-bot", "public_home", 8800, "user", f"My favorite color is {colors[index]}."),
                    )
                    source_row_id = cursor.lastrowid
                bnl01_bot.upsert_user_fact(
                    uid, self.guild_id, "favorite_color", colors[index],
                    source_conversation_row_id=source_row_id,
                    source_channel_policy="public_home", source_directed=True,
                )
        return tuple(members)

    async def test_group_memory_reaches_real_prompt_guard_and_neutral_storage(self):
        members = self._seed_group_member_sources()
        answer = "The amber project uses soft chords, and the violet project does too."
        for policy, enabled in (("public_home", "false"), ("sealed_test", "true")):
            channel_id = 8811 + len(self.channel_ids)
            with mock.patch.dict(os.environ, {
                "BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED": "true",
                "BNL_MEMORY_LEDGER_SHADOW_ENABLED": "true",
                "BNL_MOMENT_ENGINE_SHADOW_ENABLED": "true",
                "BNL_RELATIONSHIP_V2_SHADOW_ENABLED": "true",
                "BNL_UNIFIED_RESPONSE_ASSESSMENT_SHADOW_ENABLED": "true",
                "BNL_UNIFIED_INTELLIGENCE_PACKET_SHADOW_ENABLED": "true",
                "BNL_ORDINARY_CHAT_SINGLE_PACKET_ENABLED": enabled,
                "BNL_ORDINARY_CHAT_SINGLE_PACKET_GUILD_IDS": str(self.guild_id),
                "BNL_ORDINARY_CHAT_SINGLE_PACKET_USER_IDS": "100",
                "BNL_ORDINARY_CHAT_SINGLE_PACKET_CHANNEL_IDS": str(channel_id),
            }), mock.patch.object(
                bnl01_bot, "maybe_generate_ordinary_chat_single_packet",
                wraps=bnl01_bot.maybe_generate_ordinary_chat_single_packet,
            ) as ordinary:
                if enabled == "true":
                    self.assertTrue(bnl01_bot.ordinary_chat_configuration()["effective"], bnl01_bot.ordinary_chat_configuration())
                channel, generation, guard = await self._batch(
                    policy, answer=answer, participants=members, privileged=True,
                )
            generation.assert_awaited_once()
            prompt = generation.await_args.args[0]
            bases = tuple(b for b in guard.await_args.kwargs["prompt_source_bases"] if isinstance(b, bnl01_bot.MemoryPromptSourceBasis))
            self.assertEqual([basis.user_id for basis in bases], [100, 101])
            self.assertTrue(all(not basis.is_owner_or_mod for basis in bases))
            for basis, color in zip(bases, ("amber", "violet")):
                for part in ("intro", "mix", "release"):
                    self.assertIn(f"The {color} project {part} has soft chords.", basis.rendered_context)
                self.assertIn(basis.rendered_context, prompt)
            self.assertNotIn("Private", "\n".join(b.rendered_context for b in bases))
            self.assertNotIn("Sealed", "\n".join(b.rendered_context for b in bases))
            self.assertNotIn("Unrelated secret project marker", prompt)
            self.assertEqual(channel.sent, [answer])
            # The existing group boundary must not become a new canary gate.
            self.assertTrue(all(not call.kwargs["scope_applied"] for call in ordinary.await_args_list))
            with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
                row = conn.execute("SELECT id,user_id,content FROM conversations WHERE role='model' AND channel_id=?", (channel.id,)).fetchone()
                self.assertEqual(row[1:], (0, answer))
                links = conn.execute("SELECT user_id FROM conversation_response_participants WHERE conversation_row_id=? ORDER BY user_id", (row[0],)).fetchall()
                self.assertEqual(links, [(100,), (101,)])

    async def test_eight_group_members_share_one_budget_including_wrappers(self):
        members = self._seed_group_member_sources(facts=True, count=8)
        channel, generation, guard = await self._batch(
            "public_home", participants=members,
            answer="Those individual colors give this room a varied palette.",
        )
        bases = tuple(b for b in guard.await_args.kwargs["prompt_source_bases"] if isinstance(b, bnl01_bot.MemoryPromptSourceBasis))
        self.assertEqual(len(bases), 8)
        injected = "\n\n" + "\n\n".join(b.rendered_context for b in bases)
        self.assertLessEqual(len(injected), bnl01_bot.MEMORY_PROMPT_BUDGET_PUBLIC)
        self.assertIn(injected, generation.await_args.args[0])
        for basis, color in zip(bases, ("amber", "violet", "copper", "indigo", "cyan", "green", "blue", "gold")):
            self.assertIn(f"Favorite color: {color}", basis.rendered_context)
            fresh, changed = bnl01_bot.refresh_prompt_source_basis(basis)
            self.assertFalse(changed)
            self.assertLessEqual(len(fresh.rendered_context), basis.member_budget_chars)
        self.assertEqual(len(channel.sent), 1)

    def test_group_own_message_directness_and_duplicate_names_remain_distinct(self):
        members = self._seed_group_member_sources(facts=True)
        turns = []
        for index, (_name, text, uid) in enumerate(members):
            addressing = bnl01_bot.DiscordTurnAddressing(
                speaker="Test Member", explicit_tag_recipients=(), reply_target="none",
                explicitly_mentions_bnl=index == 1, reply_targets_bnl=False,
                directly_targets_bnl=index == 1, targets_other_human=False,
                plain_text_names_bnl=False, speaker_user_id=uid,
            )
            turns.append(bnl01_bot.BatchConversationTurn("Test Member", text, uid, addressing))
        context, bases, metadata = bnl01_bot.build_batch_member_memory_context(
            turns, guild_id=self.guild_id, channel_id=8810,
            channel_policy="public_home", route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
        )
        self.assertEqual([basis.current_direct for basis in bases], [False, True])
        self.assertEqual([basis.user_text for basis in bases], [item[1] for item in turns])
        transcript = bnl01_bot._format_batched_prompt(turns, "balanced", "")
        self.assertIn("speaker 1 - Test Member", transcript)
        self.assertIn("speaker 2 - Test Member", transcript)
        self.assertIn("Favorite color: amber", bases[0].rendered_context)
        self.assertNotIn("Favorite color: violet", bases[0].rendered_context)
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            conn.execute("UPDATE user_memory_facts SET fact_value='green' WHERE user_id=101")
        prompt, fresh, changed, failed = bnl01_bot.refresh_prompt_source_bases(context, bases)
        self.assertFalse(failed)
        self.assertEqual(changed, ("memory",))
        self.assertEqual(fresh[0], bases[0])
        self.assertIn("Favorite color: green", fresh[1].rendered_context)
        self.assertNotIn("Favorite color: violet", prompt)
        self.assertEqual(metadata["member_source_count"], 2)

    async def test_second_member_forget_during_generation_never_sends_stale_fact(self):
        members = self._seed_group_member_sources(facts=True)
        drafts = []

        async def generate(prompt, *_args, **_kwargs):
            drafts.append(prompt)
            if len(drafts) == 1:
                with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
                    conn.execute("UPDATE user_memory_facts SET lifecycle_status='forgotten',fact_value='' WHERE user_id=101")
                return "Amber Instrumentalist named amber, while Violet Sound Designer named violet."
            self.assertNotIn("Favorite color: violet", prompt)
            return "Amber Instrumentalist named amber; the other preference is not available now."

        channel, generation, _guard = await self._batch(
            "public_home", participants=members, answer=generate,
        )
        self.assertGreaterEqual(generation.await_count, 2)
        self.assertNotIn("while Violet Sound Designer named violet", "\n".join(channel.sent))
        self.assertEqual(channel.sent, ["Amber Instrumentalist named amber; the other preference is not available now."])
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            saved = conn.execute("SELECT content FROM conversations WHERE role='model'").fetchall()
        self.assertEqual(saved, [(channel.sent[0],)])

    def test_group_reader_preserves_read_snapshot_with_governance_shadows_on(self):
        self._seed_group_member_sources(facts=True)
        with mock.patch.dict(os.environ, {"BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED": "true"}):
            with bnl01_bot.closing(bnl01_bot._open_member_memory_read_connection()) as conn:
                conn.execute("BEGIN")
                statements = []
                conn.set_trace_callback(statements.append)
                context, _metadata = bnl01_bot._read_bounded_member_memory(
                    100, self.guild_id, speaker_label="speaker 1 - Test Member",
                    budget_chars=400, route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                    channel_policy="public_home", user_text="remember the amber project",
                    current_direct=True, governance_allowed=False, channel_id=8810,
                    connection=conn,
                )
                self.assertIn("Favorite color: amber", context)
                self.assertTrue(conn.in_transaction)
                self.assertFalse(any(sql.strip().upper().startswith(("COMMIT", "INSERT", "UPDATE", "DELETE")) for sql in statements))
                with self.assertRaises(sqlite3.OperationalError):
                    conn.execute("UPDATE user_memory_facts SET fact_value='forbidden write'")
        missing = os.path.join(self.tmp, "absent.db")
        with mock.patch.object(bnl01_bot, "DB_FILE", missing):
            with self.assertRaises(sqlite3.OperationalError):
                bnl01_bot._open_member_memory_read_connection()
        self.assertFalse(os.path.exists(missing))


    def test_group_member_governed_canary_retains_authority_and_revalidates(self):
        self._seed_group_member_sources(facts=True)
        expected = "The Copper Kite arrangement is the current music goal."
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            # Production enabled the Moment shadow before startup. Preserve
            # that initialized-schema prerequisite for the read-only canary.
            moments.ensure_moment_schema(conn)
            for uid, visibility, predicate, value in (
                (100, ledger.Visibility.PUBLIC_SAFE, "goal", expected),
                (100, ledger.Visibility.PRIVATE, "commitment", "Private infrastructure password marker."),
                (9999, ledger.Visibility.PUBLIC_SAFE, "goal", "Other member Copper Kite secret marker."),
            ):
                entry = ledger.LedgerEntry(
                    guild_id=self.guild_id, source_table="test_member_goal",
                    source_row_id=f"{uid}:{predicate}", source_role="member",
                    entry_type="goal", predicate_key=predicate,
                    subject_key=ledger.subject_key_for_user(uid), value=value,
                    source_class=ledger.SourceClass.FIRST_PARTY_RECORD,
                    visibility=visibility, confidence=ledger.Confidence.HIGH,
                    public_usable=visibility == ledger.Visibility.PUBLIC_SAFE,
                    channel_policy="public_home", route_mode="normal_chat", salience=0.9,
                )
                ledger.insert_ledger_entry(conn, entry)
                if value == expected:
                    entry_id = entry.entry_id
        items = (("Test Member", "What do you remember about me?", 100), ("Violet Sound Designer", "That violet music project is underway.", 101))
        with mock.patch.dict(os.environ, {
            "BNL_MEMORY_GOVERNANCE_CANARY_ENABLED": "true",
            "BNL_MEMORY_GOVERNANCE_CANARY_GUILD_IDS": str(self.guild_id),
            "BNL_MEMORY_GOVERNANCE_CANARY_USER_IDS": "100",
            "BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED": "true",
            "BNL_MEMORY_LEDGER_SHADOW_ENABLED": "true",
            "BNL_MOMENT_ENGINE_SHADOW_ENABLED": "true",
        }):
            context, bases, metadata = bnl01_bot.build_batch_member_memory_context(
                items, guild_id=self.guild_id, channel_id=8810,
                channel_policy="public_home", route_mode="normal_chat",
            )
            self.assertIn(expected, context)
            self.assertIn("governed first_party_record", context)
            self.assertNotIn("Private infrastructure", context)
            self.assertNotIn("Other member", context)
            basis = next(b for b in bases if b.user_id == 100)
            self.assertTrue(basis.source_safe_recall_synthesis)
            self.assertTrue(basis.governed_basis_digest)
            self.assertIn(entry_id, metadata["governed_entry_ids"])
            self.assertEqual(bnl01_bot.prompt_source_basis_failure(bases), "")
            with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
                conn.execute("UPDATE memory_ledger_entries SET normalized_value=? WHERE entry_id=?", ("The Silver Moth arrangement is the corrected music goal.", entry_id))
            self.assertEqual(bnl01_bot.prompt_source_basis_failure(bases), "memory_source_changed")


    async def test_personal_show_memory_composes_with_member_memory_in_direct_prompt(self):
        show_context = (
            "Durable BARCODE Radio show episode memory:\n"
            "Attributed public TikTok/Discord evidence:\n"
            "- Test Member asked BNL about the Copper Kite instrumental.\n"
        )
        for policy in ("public_home", "sealed_test"):
            with self.subTest(policy=policy), mock.patch.object(
                bnl01_bot, "build_tiktok_show_evidence_context_for_turn",
                return_value=show_context,
            ), mock.patch.object(
                bnl01_bot, "build_shared_brain_synthesis_basis",
                wraps=bnl01_bot.build_shared_brain_synthesis_basis,
            ) as synthesis:
                prompt, metadata = self._direct_prompt(
                    policy, request="What do you remember about me?",
                )
                self.assertIn(PUBLIC_MEMORY, prompt)
                self.assertIn(show_context, prompt)
                self.assertNotIn("Finalized BARCODE Radio episode priority:", prompt)
                self.assertTrue(metadata["source_context_available"])
                contexts = synthesis.call_args.kwargs["competing_factual_contexts"]
                self.assertTrue(any(PUBLIC_MEMORY in value for value in contexts))
                self.assertTrue(any(show_context in value for value in contexts))

    async def _assert_personal_show_memory_batch_composition(self, policy):
        show_context = (
            "Durable BARCODE Radio show episode memory:\n"
            "Attributed public TikTok/Discord evidence:\n"
            "- Test Member asked BNL about the Copper Kite instrumental.\n"
        )
        answer = "Test Member shared the Copper Kite instrumental and asked about it during the show."
        with mock.patch.object(
            bnl01_bot, "build_tiktok_show_evidence_context_for_turn",
            return_value=show_context,
        ), mock.patch.object(
            bnl01_bot, "build_shared_brain_synthesis_basis",
            wraps=bnl01_bot.build_shared_brain_synthesis_basis,
        ) as synthesis:
            # The sealed legacy broad-recall shortcut is a distinct owner;
            # this targeted wording enters its existing normal batch route.
            request = (
                "BNL, what do you remember me telling you about the Copper Kite instrumental?"
                if policy == "sealed_test"
                else "BNL, what do you remember about me?"
            )
            channel, generation, _guard = await self._batch(
                policy, request=request, answer=answer,
            )
            prompt = generation.call_args.args[0]
            self.assertIn(PUBLIC_MEMORY, prompt)
            self.assertIn(show_context, prompt)
            self.assertNotIn("Finalized BARCODE Radio episode priority:", prompt)
            synthesis.assert_called_once()
            contexts = synthesis.call_args.kwargs["competing_factual_contexts"]
            self.assertTrue(any(PUBLIC_MEMORY in value for value in contexts))
            self.assertTrue(any(show_context in value for value in contexts))
            self.assertEqual(generation.await_count, 1)
            self.assertEqual(channel.sent, [answer])
            with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
                self.assertEqual(conn.execute(
                    "SELECT COUNT(*) FROM conversations WHERE role='model' AND content=?",
                    (answer,),
                ).fetchone()[0], 1)

    async def test_personal_show_memory_composes_in_public_batch(self):
        await self._assert_personal_show_memory_batch_composition("public_home")

    async def test_personal_show_memory_composes_in_sealed_batch(self):
        await self._assert_personal_show_memory_batch_composition("sealed_test")

    async def test_explicit_show_request_retains_show_priority_in_direct_and_batch(self):
        show_context = (
            "Durable BARCODE Radio show episode memory:\n"
            "Attributed public TikTok/Discord evidence:\n"
            "- Test Member asked BNL about the Copper Kite instrumental.\n"
        )
        request = "What happened during the last show?"
        with mock.patch.object(
            bnl01_bot, "build_tiktok_show_evidence_context_for_turn", return_value=show_context,
        ):
            prompt, _metadata = self._direct_prompt("sealed_test", request=request)
            self.assertIn("Finalized BARCODE Radio episode priority:", prompt)
            channel, generation, _guard = await self._batch(
                "sealed_test", request=request,
                answer="Test Member asked BNL about the Copper Kite instrumental.",
            )
            self.assertIn("Finalized BARCODE Radio episode priority:", generation.call_args.args[0])
            self.assertEqual(generation.await_count, 1)
            self.assertEqual(len(channel.sent), 1)

if __name__ == "__main__":
    unittest.main()
