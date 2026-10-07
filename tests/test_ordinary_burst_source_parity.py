"""Existing source owners remain available when direct turns are collected."""
import asyncio
import os
import unittest
from contextlib import ExitStack
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest import mock

import test_conversation_batching as fixtures
import test_ordinary_addressed_burst_ingress as ingress
from test_queue_artist_memory import artist_memory_record, read_model
from bnl_queue_artist_memory import sync_queue_artist_memory_read_model

bot = fixtures.bnl01_bot
ANSWER = "A bounded answer to the current request."
BROADCAST = "The copper orchard is an established broadcast reference."


class OrdinaryBurstSourceParityTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = fixtures.ConversationBatchCoordinatorTests.asyncSetUp
    _channel = fixtures.ConversationBatchCoordinatorTests._channel
    _prime_flush = fixtures.ConversationBatchCoordinatorTests._prime_flush
    _flush_runtime = fixtures.ConversationBatchCoordinatorTests._flush_runtime

    async def asyncTearDown(self):
        await fixtures.ConversationBatchCoordinatorTests.asyncTearDown(self)
        for cid in self.channel_ids:
            bot._channel_addressed_generation.pop(cid, None)
            bot._show_state_topic_context.pop(bot._show_state_topic_key(7700, cid), None)

    def _seed_catalog(self, channel):
        sync_queue_artist_memory_read_model(
            bot.DB_FILE, guild_id=channel.guild.id,
            read_model=read_model(artist_memory_record()),
            environ={"BNL_QUEUE_PRODUCTION_ENABLED": "true"},
        )

    async def _collect(self, channel, request, *, broadcast="", queue_enabled=False,
                       packet=False, publication=False):
        self._prime_flush(channel, request)
        bot._channel_addressed_generation[channel.id] = {
            "guild_id": channel.guild.id, "channel_policy": "sealed_test",
            "generation_id": 0, "author_ids": (), "message_ids": (),
            "commit_started": False,
        }
        prompts, scopes, assessments = [], [], []
        actual_queue = bot.build_queue_artist_memory_context
        ordinary_basis = mock.Mock(return_value=object())
        packet_object, assessment_object = object(), object()
        execution = bot.OrdinaryChatSinglePacketExecution(
            decision=SimpleNamespace(candidate_selected=True), response=ANSWER,
            prompt="Synthetic packet expression", prompt_source_bases=(),
            candidate_active=True, provider_call_count=1, corrective_call_count=0,
        )

        async def generate(prompt, **_kwargs):
            prompts.append(prompt)
            return ANSWER

        def scope(**kwargs):
            scopes.append(kwargs)
            return SimpleNamespace(
                eligible=packet and not kwargs["specialized_owner_present"],
                reason="eligible" if packet else "disabled",
            )

        def assess(*_args, **kwargs):
            assessments.append(kwargs)
            if packet:
                kwargs["intelligence_packet_out"]["packet"] = packet_object
                return assessment_object
            return None

        with self._flush_runtime(channel.id, generate), ExitStack() as stack:
            for patcher in (
                mock.patch.dict(os.environ, {"BNL_QUEUE_PRODUCTION_ENABLED": str(queue_enabled).lower()}),
                mock.patch.object(bot, "build_broadcast_memory_context", return_value=broadcast),
                mock.patch.object(bot, "build_queue_artist_memory_context", wraps=actual_queue),
                mock.patch.object(bot, "build_tiktok_show_evidence_context_for_turn", return_value=""),
                mock.patch.object(bot, "maybe_build_bnl_read_model_context", return_value=""),
                mock.patch.object(bot, "build_user_memory_context", return_value=""),
                mock.patch.object(bot, "ordinary_chat_route_scope_decision", side_effect=scope),
                mock.patch.object(bot, "build_unified_response_assessment_shadow", side_effect=assess),
                mock.patch.object(bot, "publication_packet_owns_turn", return_value=publication),
                mock.patch.object(bot, "publication_packet_composes_current_queue", return_value=False),
                mock.patch.object(bot, "build_shared_brain_synthesis_basis", return_value=None),
                mock.patch.object(bot, "build_ordinary_chat_basis", new=ordinary_basis),
                mock.patch.object(bot, "maybe_generate_ordinary_chat_single_packet", new=mock.AsyncMock(return_value=execution if packet else None)),
                mock.patch.object(bot, "prompt_source_basis_failure", return_value=""),
                mock.patch.object(bot, "safely_finalize_shared_brain_synthesis", new=mock.AsyncMock(return_value=True)),
                mock.patch.object(bot, "record_unified_response_assessment_shadow_after_send", new=mock.AsyncMock()),
            ):
                stack.enter_context(patcher)
            await bot._flush_channel_buffer(channel)
            bot.build_queue_artist_memory_context.assert_called_once()
        self.assertEqual(channel.sent, [ANSWER])
        self.assertEqual(len(assessments), 1)
        return SimpleNamespace(prompts=prompts, scope=scopes[-1],
                               assessment=assessments[0], ordinary=ordinary_basis)

    async def test_catalog_bare_artist_name_preserves_exact_existing_source_owner(self):
        channel = self._channel(996101)
        self._seed_catalog(channel)
        result = await self._collect(channel, "BNL, who is Provider Signal?", queue_enabled=True)
        self.assertTrue(result.scope["specialized_owner_present"])
        self.assertIn("queue_artist_memory", result.assessment["prompt_lanes"])
        self.assertIn('artist="Provider Signal"; track="Provider Song"', result.prompts[0])
        self.assertIn("not a verified identity alias", result.prompts[0])

    async def test_catalog_disabled_and_unrelated_requests_do_not_create_an_owner(self):
        for index, (request, enabled) in enumerate((
            ("BNL, who is Provider Signal?", False),
            ("BNL, how is the weather today?", True),
        )):
            with self.subTest(request=request, enabled=enabled):
                channel = self._channel(996110 + index)
                self._seed_catalog(channel)
                result = await self._collect(channel, request, queue_enabled=enabled)
                self.assertFalse(result.scope["specialized_owner_present"])
                self.assertNotIn("queue_artist_memory", result.assessment["prompt_lanes"])
                self.assertNotIn("Provider Song", result.prompts[0])

    async def test_cached_show_and_generic_broadcast_remain_supporting_legacy_context(self):
        channel = self._channel(996120)
        bot._show_state_topic_context[bot._show_state_topic_key(channel.guild.id, channel.id)] = {
            "target_show_date": "2026-10-02", "cleaned_summary": "The copper orchard broadcast was postponed.",
            "created_at": datetime.now(timezone.utc), "last_user_id": 404,
            "last_bot_answer_type": "show_state",
        }
        request = "BNL, suggest a color for a very small room."
        result = await self._collect(channel, request, broadcast=BROADCAST)
        self.assertFalse(result.scope["specialized_owner_present"])
        self.assertIn(BROADCAST, result.prompts[0])
        self.assertIn("The copper orchard broadcast was postponed.", result.prompts[0])
        self.assertIn(request, result.prompts[0])
        self.assertTrue(result.assessment["show_state_present"])
        self.assertTrue(result.assessment["broadcast_memory_present"])

    async def test_generic_broadcast_is_competing_context_when_single_packet_owns_turn(self):
        channel = self._channel(996130)
        result = await self._collect(channel, "BNL, who is Copper Orchard?", broadcast=BROADCAST, packet=True)
        self.assertFalse(result.scope["specialized_owner_present"])
        self.assertFalse(result.assessment["broadcast_memory_present"])
        result.ordinary.assert_called_once()
        self.assertTrue(any(BROADCAST in block for block in result.ordinary.call_args.kwargs["competing_factual_contexts"]))

    async def test_publication_owner_excludes_catalog_and_broadcast_context(self):
        channel = self._channel(996140)
        self._seed_catalog(channel)
        result = await self._collect(channel, "BNL, who is Provider Signal?", broadcast=BROADCAST,
                                     queue_enabled=True, publication=True)
        self.assertFalse(result.scope["specialized_owner_present"])
        self.assertNotIn("queue_artist_memory", result.assessment["prompt_lanes"])
        self.assertNotIn(BROADCAST, result.prompts[0])
        self.assertNotIn("Provider Song", result.prompts[0])

    async def test_source_file_scope_and_subject_preserve_the_direct_owner(self):
        channel = self._channel(996150)
        message = fixtures.FakeMessage(channel, "what do we know about Emerald?")
        plan = SimpleNamespace(should_reply=True, route_mode=bot.ROUTE_MODE_NORMAL_CHAT,
                               response_timing=bot.RESPONSE_TIMING_PACED_DIRECT,
                               batch_behavior="debounce", channel_policy="sealed_test")
        with mock.patch.object(bot, "resolve_channel_policy", return_value="sealed_test"), \
             mock.patch.object(bot, "can_use_source_context_injection", return_value=True), \
             mock.patch.object(bot, "_enqueue_conversation_batch", new=mock.AsyncMock()) as enqueue:
            self.assertTrue(bot._direct_source_context_requested(message, message.content, "sealed_test"))
            self.assertFalse(await bot._maybe_enqueue_addressed_conversation(
                message, plan, message.content, SimpleNamespace(addresses_bnl=True)))
            enqueue.assert_not_awaited()
            self.assertFalse(bot._direct_source_context_requested(message, "tell me a joke about pizza", "sealed_test"))
            self.assertFalse(bot._direct_source_context_requested(message, message.content, "public_home"))
            with mock.patch.object(bot, "can_use_source_context_injection", return_value=False):
                self.assertFalse(bot._direct_source_context_requested(message, message.content, "sealed_test"))


class OrdinaryBurstSourceContinuationTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = ingress.OrdinaryAddressedBurstIngressTests.asyncSetUp
    asyncTearDown = ingress.OrdinaryAddressedBurstIngressTests.asyncTearDown
    _channel = ingress.OrdinaryAddressedBurstIngressTests._channel
    _on_message_runtime = ingress.OrdinaryAddressedBurstIngressTests._on_message_runtime
    _flush_runtime = ingress.OrdinaryAddressedBurstIngressTests._flush_runtime
    _runtime = ingress.OrdinaryAddressedBurstIngressTests._runtime
    _assert_originals_once = ingress.OrdinaryAddressedBurstIngressTests._assert_originals_once

    async def test_source_successor_cannot_cancel_send_already_started_with_pending_fragment(self):
        channel = self._channel(996162, blocking_send=True)
        mention = SimpleNamespace(id=999, display_name="BNL-01", bot=True)
        first = fixtures.FakeMessage(channel, "<@999> Can you help me review something?", mentions=[mention])
        pending = fixtures.FakeMessage(channel, "It concerns a very small room.", author=first.author)
        source_request = fixtures.FakeMessage(channel, "what do we know about Emerald?", author=first.author)
        prompts = []

        async def generate(prompt, **_kwargs):
            prompts.append(prompt)
            return ANSWER if len(prompts) == 1 else "Source-backed successor answer."

        with self._runtime(channel, generate), \
             mock.patch.object(bot, "can_use_source_context_injection", return_value=True), \
             mock.patch.object(bot, "maybe_build_source_context_for_direct_message", new=mock.AsyncMock(return_value="INTERNAL SOURCE CONTEXT")) as source:
            await bot.on_message(first)
            batch_task = bot._channel_tasks[channel.id]
            try:
                await asyncio.wait_for(channel.send_started.wait(), timeout=8)
                await bot.on_message(pending)
                self.assertEqual(len(bot._channel_buffers[channel.id]), 1)
                await bot.on_message(source_request)
            finally:
                channel.release_send.set()
                await asyncio.wait_for(asyncio.gather(batch_task, return_exceptions=True), timeout=8)
        source.assert_awaited_once()
        self.assertFalse(batch_task.cancelled())
        self.assertEqual(channel.sent, [ANSWER])
        self.assertEqual(source_request.replies, ["Source-backed successor answer."])
        self.assertEqual(self.model_save.call_count, 2)
        self._assert_originals_once(first, pending, source_request)
        self.assertNotIn(channel.id, bot._channel_addressed_generation)
        self.assertFalse(bot._channel_buffers[channel.id])
        self.assertNotIn(channel.id, bot._channel_interrupt_handoff)

    async def test_untagged_source_request_during_addressed_collection_reaches_source_owner(self):
        for index, phase in enumerate(("pending", "provider")):
            with self.subTest(phase=phase):
                channel = self._channel(996160 + index)
                mention = SimpleNamespace(id=999, display_name="BNL-01", bot=True)
                first = fixtures.FakeMessage(channel, "<@999> Can you help me review something?", mentions=[mention])
                second = fixtures.FakeMessage(channel, "what do we know about Emerald?", author=first.author)
                started, release = asyncio.Event(), asyncio.Event()
                prompts = []

                async def generate(prompt, **_kwargs):
                    prompts.append(prompt)
                    if phase == "provider" and len(prompts) == 1:
                        started.set()
                        await release.wait()
                        return "STALE ORDINARY DRAFT"
                    return ANSWER

                with self._runtime(channel, generate), \
                     mock.patch.object(bot, "can_use_source_context_injection", return_value=True), \
                     mock.patch.object(bot, "build_current_turn_addressing_context", wraps=bot.build_current_turn_addressing_context) as addressing_context, \
                     mock.patch.object(bot, "maybe_build_source_context_for_direct_message", new=mock.AsyncMock(return_value="INTERNAL SOURCE CONTEXT")) as source:
                    await bot.on_message(first)
                    batch_task = bot._channel_tasks[channel.id]
                    try:
                        if phase == "provider":
                            await asyncio.wait_for(started.wait(), timeout=8)
                        self.assertTrue(bot._ordinary_burst_continuation(second, "sealed_test"))
                        await bot.on_message(second)
                    finally:
                        release.set()
                        await asyncio.wait_for(asyncio.gather(batch_task, return_exceptions=True), timeout=8)
                source.assert_awaited_once()
                self.assertEqual(source.await_args.args[1], second.content)
                self.assertTrue(source.await_args.kwargs["direct_interaction"])
                handoff = addressing_context.call_args.kwargs
                self.assertTrue(handoff["established_bnl_followup"])
                self.assertTrue(handoff["addressing"].established_bnl_followup)
                self.assertFalse(handoff["addressing"].directly_targets_bnl)
                self.assertFalse(handoff["addressing"].explicitly_mentions_bnl)
                self.assertEqual(first.replies + second.replies + channel.sent, [ANSWER])
                self.assertEqual(self.frames[-1].source_message_ids, (second.id,))
                self.assertEqual(self.frames[-1].explicit_mention_count, 0)
                self.assertEqual(self.frames[-1].reply_message_ids, ())
                self.model_save.assert_called_once()
                self._assert_originals_once(first, second)
                self.assertNotIn(channel.id, bot._channel_addressed_generation)
                self.assertFalse(bot._channel_buffers[channel.id])
                self.assertNotIn(channel.id, bot._channel_interrupt_handoff)
