"""Ordinary addressed bursts enter the existing conversation owner intact.

Discord intake, actual mention addressing, planner, typed batch handoff and
original-message/ledger capture are real. External generation and delivery use
the established coordinator fixture. Generic text must not need a payload or
repair phrase to preserve a second contribution.
"""
import asyncio
from contextlib import ExitStack
import sqlite3
import threading
import unittest
from unittest import mock

import test_conversation_batching as existing

bot = existing.bnl01_bot
FIRST = "We are imagining a desk with a purple lamp. What belongs next to it?"
SECOND = "The room is very small and has no shelves."
ANSWER = "Keep the desk clear except for the lamp and a small notebook."


class OrdinaryAddressedBurstIngressTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = existing.ConversationBatchCoordinatorTests.asyncSetUp
    _on_message_runtime = existing.ConversationBatchCoordinatorTests._on_message_runtime
    _flush_runtime = existing.ConversationBatchCoordinatorTests._flush_runtime

    def _channel(self, channel_id, *, blocking_send=False):
        channel = existing.ConversationBatchCoordinatorTests._channel(
            self, channel_id, blocking_send=blocking_send,
        )
        channel.send = mock.AsyncMock(wraps=channel.send)
        return channel

    def _assert_reply_reference(self, channel, message):
        call = channel.send.await_args_list[0]
        reference = call.kwargs["reference"]
        self.assertEqual(reference.message_id, message.id)
        self.assertEqual(reference.channel_id, channel.id)
        self.assertEqual(reference.guild_id, channel.guild.id)
        self.assertFalse(reference.fail_if_not_exists)
        self.assertEqual(call.kwargs["allowed_mentions"].to_dict(), {"parse": []})

    async def asyncTearDown(self):
        await existing.ConversationBatchCoordinatorTests.asyncTearDown(self)
        for channel_id in self.channel_ids:
            getattr(bot, "_channel_addressed_generation", {}).pop(channel_id, None)

    def _messages(self, channel, *, second_author=None):
        mention = existing.SimpleNamespace(id=999, display_name="BNL-01", bot=True)
        first = existing.FakeMessage(channel, "<@999> " + FIRST, mentions=[mention])
        second = existing.FakeMessage(channel, SECOND, author=second_author or first.author)
        return first, second

    def _runtime(self, channel, generate, *, policy="sealed_test"):
        actual_capture = bot.save_user_message
        actual_save_model = bot.save_model_message
        actual_target = bot.is_direct_bnl_target
        actual_orchestration = bot.build_live_conversation_orchestration_decision
        self.frames = []
        self.model_save = mock.Mock(side_effect=actual_save_model)

        def frame(*args, **kwargs):
            decision = actual_orchestration(*args, **kwargs)
            self.frames.append(decision.situation_frame)
            return decision

        stack = ExitStack()
        stack.enter_context(self._on_message_runtime(channel.id, followup_candidate=False))
        stack.enter_context(self._flush_runtime(channel.id, generate))
        for patcher in (
            mock.patch.object(bot, "resolve_channel_policy", return_value=policy),
            mock.patch.object(bot, "get_guild_config", return_value=channel.id + 1000 if policy == "public_context" else channel.id),
            mock.patch.object(bot, "is_direct_bnl_target", side_effect=actual_target),
            mock.patch.object(bot, "save_user_message", side_effect=actual_capture),
            mock.patch.object(bot, "save_model_message", new=self.model_save),
            mock.patch.object(bot, "build_live_conversation_orchestration_decision", side_effect=frame),
            mock.patch.object(bot, "memory_ledger_shadow_enabled", return_value=True),
            mock.patch.object(bot, "maybe_build_bnl_read_model_context", return_value=""),
            mock.patch.object(bot, "maybe_build_source_context_for_direct_message", new=mock.AsyncMock(return_value="")),
            mock.patch.object(bot, "build_room_first_direct_context_async", new=mock.AsyncMock(return_value="")),
            mock.patch.object(bot, "build_user_aware_prompt", side_effect=lambda _uid, _gid, _name, text, **_kwargs: (text, False, "balanced")),
            mock.patch.object(bot, "_apply_direct_response_pacing", new=mock.AsyncMock()),
            mock.patch.object(bot, "is_privileged_member", return_value=False),
            mock.patch.object(bot, "build_show_state_override_context", return_value={}),
            mock.patch.object(bot, "_get_recent_show_state_topic_context", return_value={}),
        ):
            stack.enter_context(patcher)

        async def direct_generate(_channel, prompt, _uid, _gid, **kwargs):
            return await generate(prompt, **kwargs)

        stack.enter_context(mock.patch.object(bot, "get_gemini_response_with_optional_typing", new=mock.AsyncMock(side_effect=direct_generate)))
        return stack

    async def _drain(self, *ingress_tasks):
        await asyncio.wait_for(asyncio.gather(*ingress_tasks), timeout=4)
        for _ in range(4):
            tasks = [bot._channel_tasks.get(channel_id) for channel_id in self.channel_ids]
            pending = [task for task in tasks if task is not None and not task.done()]
            if not pending:
                return
            await asyncio.wait_for(asyncio.gather(*pending), timeout=12)
        self.fail("conversation coordinator did not settle within its bounded handoff")

    def _assert_originals_once(self, *messages):
        conn = sqlite3.connect(bot.DB_FILE)
        try:
            for message in messages:
                originals = conn.execute("SELECT id,channel_policy,channel_id FROM conversations WHERE message_id=? AND role='user'", (message.id,)).fetchall()
                roots = conn.execute("SELECT source_row_id,channel_policy,channel_id,public_usable FROM memory_ledger_entries WHERE source_message_id=? AND source_table='conversations' AND source_role='user'", (message.id,)).fetchall()
                self.assertEqual(len(originals), 1)
                self.assertEqual(len(roots), 1)
                self.assertEqual(roots[0][:3], (str(originals[0][0]), originals[0][1], originals[0][2]))
                self.assertEqual(roots[0][3], int(originals[0][1] in {"public_home", "public_context"}))
        finally:
            conn.close()

    def _assert_combined(self, channel, first, second, *, calls, expected_calls):
        self.assertEqual(first.replies + second.replies + channel.sent, [ANSWER])
        self._assert_reply_reference(channel, first)
        self.assertEqual(len(calls), expected_calls)
        self.assertIn(FIRST, calls[-1])
        self.assertIn(SECOND, calls[-1])
        self.assertEqual(set(self.frames[-1].source_message_ids), {first.id, second.id})
        self.assertEqual(self.frames[-1].route_mode, bot.ROUTE_MODE_NORMAL_CHAT)
        self._assert_originals_once(first, second)
        self.model_save.assert_called_once()
        self.assertEqual(self.model_save.call_args.args[2], ANSWER)
        self.assertNotIn(bot._direct_session_key(first), bot._direct_payload_sessions)

    async def test_generic_tagged_pair_before_preparation_uses_one_complete_request(self):
        for index, policy in enumerate(("sealed_test", "public_home", "public_context")):
            with self.subTest(policy=policy):
                channel = self._channel(995110 + index)
                first, second = self._messages(channel)
                prompts = []

                async def generate(prompt, **_kwargs):
                    prompts.append(prompt)
                    return ANSWER

                with self._runtime(channel, generate, policy=policy):
                    await bot.on_message(first)
                    await bot.on_message(second)
                    await self._drain()
                self._assert_combined(channel, first, second, calls=prompts, expected_calls=1)

    async def test_second_fragment_during_preparation_rebuilds_before_provider(self):
        channel = self._channel(995120)
        first, second = self._messages(channel)
        loop = asyncio.get_running_loop()
        started = asyncio.Event()
        release = threading.Event()
        prompts = []
        preparation_calls = []

        def prepare(*_args, **_kwargs):
            preparation_calls.append(1)
            if len(preparation_calls) == 1:
                loop.call_soon_threadsafe(started.set)
                if not release.wait(8):
                    raise AssertionError("synthetic preparation barrier timed out")
            return ""

        async def generate(prompt, **_kwargs):
            prompts.append(prompt)
            return ANSWER

        with self._runtime(channel, generate), mock.patch.object(bot, "build_tiktok_show_evidence_context_for_turn", side_effect=prepare):
            first_task = asyncio.create_task(bot.on_message(first))
            try:
                await asyncio.wait_for(started.wait(), timeout=8)
                self.assertEqual(prompts, [])
                await bot.on_message(second)
            finally:
                release.set()
            await self._drain(first_task)
        self._assert_combined(channel, first, second, calls=prompts, expected_calls=1)

    async def test_generic_tagged_fragment_during_provider_rebuilds_once_before_send(self):
        channel = self._channel(995101)
        first, second = self._messages(channel)
        started = asyncio.Event()
        release = asyncio.Event()
        prompts = []

        async def generate(prompt, **_kwargs):
            prompts.append(prompt)
            if len(prompts) == 1:
                started.set()
                await release.wait()
                return "STALE ORIGINAL DRAFT"
            self.assertIn(FIRST, prompt)
            self.assertIn(SECOND, prompt)
            return ANSWER

        self.assertFalse(bot._detect_request_payload_expectation(FIRST)[0])
        self.assertFalse(bot.is_conversational_repair_intent(FIRST))
        with self._runtime(channel, generate):
            first_task = asyncio.create_task(bot.on_message(first))
            try:
                await asyncio.wait_for(started.wait(), timeout=8)
                await bot.on_message(second)
            finally:
                release.set()
            await self._drain(first_task)
        self._assert_combined(channel, first, second, calls=prompts, expected_calls=2)

    async def test_lone_tag_retains_one_normal_chat_answer_without_payload_waiter(self):
        channel = self._channel(995130)
        first, _ = self._messages(channel)
        prompts = []

        async def generate(prompt, **_kwargs):
            prompts.append(prompt)
            return ANSWER

        with self._runtime(channel, generate):
            await bot.on_message(first)
            await self._drain()
        self.assertEqual(first.replies + channel.sent, [ANSWER])
        self._assert_reply_reference(channel, first)
        self.assertEqual(len(prompts), 1)
        self.assertEqual(self.frames[-1].source_message_ids, (first.id,))
        self.assertEqual(self.frames[-1].route_mode, bot.ROUTE_MODE_NORMAL_CHAT)
        self.model_save.assert_called_once()
        self._assert_originals_once(first)
        self.assertNotIn(bot._direct_session_key(first), bot._direct_payload_sessions)

    async def test_mention_only_pending_turn_does_not_admit_other_author_or_room(self):
        for index, different in enumerate(("author", "channel", "thread", "guild")):
            with self.subTest(different=different):
                channel = self._channel(995140 + index * 2)
                first, second = self._messages(channel)
                other_channel = channel
                if different == "author":
                    second.author = existing.FakeAuthor(200, "Other Test Member")
                else:
                    other_channel = self._channel(channel.id + 1)
                    if different == "thread":
                        other_channel.parent_id = channel.id
                    if different == "guild":
                        other_channel.guild = existing.FakeGuild(channel.guild.id + 1)
                    second = existing.FakeMessage(other_channel, SECOND, author=first.author)
                prompts = []

                async def generate(prompt, **_kwargs):
                    prompts.append(prompt)
                    return ANSWER

                with self._runtime(channel, generate, policy="public_context"):
                    await bot.on_message(first)
                    await bot.on_message(second)
                    await self._drain()
                delivered = first.replies + second.replies + channel.sent
                if other_channel is not channel:
                    delivered += other_channel.sent
                self.assertEqual(delivered, [ANSWER])
                self.assertEqual(len(prompts), 1)
                self.assertNotIn(SECOND, prompts[0])
                self.assertEqual(self.frames[-1].source_message_ids, (first.id,))
                self.model_save.assert_called_once()
                self._assert_originals_once(first)

    async def test_second_fragment_during_final_guard_cannot_send_stale_draft(self):
        channel = self._channel(995160)
        first, second = self._messages(channel)
        started = asyncio.Event()
        release = asyncio.Event()
        prompts = []
        guards = []

        async def generate(prompt, **_kwargs):
            prompts.append(prompt)
            return "STALE ORIGINAL DRAFT" if len(prompts) == 1 else ANSWER

        async def guard(response, **_kwargs):
            guards.append(response)
            if len(guards) == 1:
                started.set()
                await release.wait()
            return response, {"suppressed": False}

        with self._runtime(channel, generate), mock.patch.object(bot, "apply_guarded_response_regeneration", new=mock.AsyncMock(side_effect=guard)):
            first_task = asyncio.create_task(bot.on_message(first))
            try:
                await asyncio.wait_for(started.wait(), timeout=8)
                await bot.on_message(second)
            finally:
                release.set()
            await self._drain(first_task)
        self._assert_combined(channel, first, second, calls=prompts, expected_calls=2)

    async def test_arrival_after_send_started_is_preserved_as_next_turn(self):
        channel = self._channel(995170, blocking_send=True)
        first, _ = self._messages(channel)
        second = existing.FakeMessage(channel, SECOND, author=first.author)
        prompts = []
        second_answer = "Then I would use one small notebook that fits beside the lamp."

        async def generate(prompt, **_kwargs):
            prompts.append(prompt)
            return ANSWER if len(prompts) == 1 else second_answer

        with self._runtime(channel, generate, policy="public_context"):
            first_task = asyncio.create_task(bot.on_message(first))
            try:
                await asyncio.wait_for(channel.send_started.wait(), timeout=8)
                await bot.on_message(second)
            finally:
                channel.release_send.set()
            await self._drain(first_task)
        self.assertEqual(first.replies + second.replies + channel.sent, [ANSWER, second_answer])
        self.assertEqual(len(prompts), 2)
        self.assertEqual(self.model_save.call_count, 2)
        self.assertEqual(self.frames[-1].source_message_ids, (second.id,))
        self.assertEqual(self.frames[-1].route_mode, bot.ROUTE_MODE_NORMAL_CHAT)
        self.assertEqual(self.frames[-1].explicit_mention_count, 0)
        self.assertEqual(self.frames[-1].reply_message_ids, ())
        self.assertIn(SECOND, prompts[-1])
        self._assert_originals_once(first, second)

    async def test_native_reply_keeps_typed_anchor_and_normal_chat_context_boundary(self):
        channel = self._channel(995180)
        prior = existing.FakeMessage(channel, "A blue notebook would suit that desk.", author=existing.SimpleNamespace(id=999, display_name="BNL-01", bot=True))
        bot.save_model_message(100, channel.guild.id, prior.content, channel_name=channel.name, channel_policy="sealed_test", channel_id=channel.id, discord_message_ids=(prior.id,))
        request = existing.FakeMessage(channel, "Why would you put that there?")
        request.reference = existing.SimpleNamespace(message_id=prior.id, resolved=prior)
        prompts = []
        context = mock.Mock(return_value="")

        async def generate(prompt, **_kwargs):
            prompts.append(prompt)
            return ANSWER

        with self._runtime(channel, generate), mock.patch.object(bot, "conversation_context_v2_enabled", return_value=True), mock.patch.object(bot, "build_conversation_context_v2_for_prompt", new=context):
            await bot.on_message(request)
            await self._drain()
        self.assertEqual(request.replies + channel.sent, [ANSWER])
        self._assert_reply_reference(channel, request)
        self.assertEqual(len(prompts), 1)
        self.assertEqual(self.frames[-1].source_message_ids, (request.id,))
        self.assertEqual(self.frames[-1].reply_message_ids, (prior.id,))
        self.assertEqual(self.frames[-1].route_mode, bot.ROUTE_MODE_NORMAL_CHAT)
        self.assertTrue(context.called)
        call = context.call_args.kwargs
        self.assertEqual(call["current_message_ids"], {request.id})
        self.assertEqual(call["referenced_message_ids"], {prior.id})
        self.assertEqual(len(call["referenced_conversation_row_ids"]), 1)
        self.assertTrue(call["is_direct_target"])
        self.assertTrue(call["is_reply_to_bnl"])
        self.assertFalse(call["is_deferred_payload_session"])
        self.model_save.assert_called_once()
        self._assert_originals_once(request)

    async def test_passive_and_multiauthor_batches_keep_channel_delivery(self):
        for index, addressed in enumerate((False, True)):
            with self.subTest(addressed=addressed):
                channel = self._channel(995185 + index)
                mention = existing.SimpleNamespace(id=999, display_name="BNL-01", bot=True)
                first = existing.FakeMessage(
                    channel, ("<@999> " if addressed else "") + FIRST,
                    mentions=[mention] if addressed else [],
                )
                second = existing.FakeMessage(
                    channel, "<@999> " + SECOND,
                    author=existing.FakeAuthor(200, "Other Test Member"), mentions=[mention],
                )

                async def generate(_prompt, **_kwargs):
                    return ANSWER

                with self._runtime(channel, generate):
                    await bot.on_message(first)
                    if addressed:
                        await bot.on_message(second)
                    await self._drain()
                self.assertEqual(channel.sent, [ANSWER])
                self.assertNotIn("reference", channel.send.await_args.kwargs)
                self.assertEqual(channel.send.await_args.kwargs["allowed_mentions"].to_dict(), {"parse": []})

    async def test_duplicate_gateway_event_does_not_duplicate_turn_or_delivery(self):
        for index, phase in enumerate(("pending", "provider")):
            with self.subTest(phase=phase):
                channel = self._channel(995190 + index)
                first, _ = self._messages(channel)
                duplicate, _ = self._messages(channel)
                duplicate.id = first.id
                started = asyncio.Event()
                release = asyncio.Event()
                prompts = []

                async def generate(prompt, **_kwargs):
                    prompts.append(prompt)
                    started.set()
                    if phase == "provider":
                        await release.wait()
                    return ANSWER

                with self._runtime(channel, generate):
                    task = asyncio.create_task(bot.on_message(first))
                    try:
                        if phase == "provider":
                            await asyncio.wait_for(started.wait(), timeout=8)
                        else:
                            await asyncio.wait_for(task, timeout=4)
                        await bot.on_message(duplicate)
                    finally:
                        release.set()
                    await self._drain(task)
                self.assertEqual(first.replies + duplicate.replies + channel.sent, [ANSWER])
                self.assertEqual(len(prompts), 1)
                self.assertEqual(self.frames[-1].source_message_ids, (first.id,))
                self.model_save.assert_called_once()
                self._assert_originals_once(first)

    async def test_channel_policy_change_cannot_project_old_sealed_turn_publicly(self):
        for index, phase in enumerate(("pending", "provider", "capture")):
            with self.subTest(phase=phase):
                channel = self._channel(995200 + index)
                first, _ = self._messages(channel)
                second = existing.FakeMessage(channel, "What would you do with an empty notebook?", author=first.author)
                current_policy = ["sealed_test"]
                prompts = []
                started = asyncio.Event()
                release = asyncio.Event()
                capture_release = threading.Event()
                loop = asyncio.get_running_loop()
                actual_capture = bot.save_user_message

                def capture(*args, **kwargs):
                    result = actual_capture(*args, **kwargs)
                    if phase == "capture" and kwargs.get("message_id") == first.id:
                        loop.call_soon_threadsafe(started.set)
                        if not capture_release.wait(8):
                            raise AssertionError("synthetic capture barrier timed out")
                    return result

                async def generate(prompt, **_kwargs):
                    prompts.append((current_policy[0], prompt))
                    if current_policy[0] == "sealed_test":
                        started.set()
                        await release.wait()
                        return "SEALED DRAFT MUST NOT CROSS THE POLICY CHANGE"
                    self.assertNotIn(FIRST, prompt)
                    return ANSWER

                with self._runtime(channel, generate), mock.patch.object(bot, "resolve_channel_policy", side_effect=lambda _channel: current_policy[0]), mock.patch.object(bot, "save_user_message", side_effect=capture):
                    task = asyncio.create_task(bot.on_message(first))
                    try:
                        if phase in {"provider", "capture"}:
                            await asyncio.wait_for(started.wait(), timeout=8)
                        else:
                            await asyncio.wait_for(task, timeout=4)
                        current_policy[0] = "public_home"
                        if phase == "capture":
                            capture_release.set()
                            await asyncio.wait_for(task, timeout=4)
                        await bot.on_message(second)
                    finally:
                        capture_release.set()
                        release.set()
                    await self._drain(task)
                self.assertEqual(first.replies + second.replies + channel.sent, [ANSWER])
                self.assertEqual(self.frames[-1].channel_policy, "public_home")
                self.assertEqual(self.frames[-1].source_message_ids, (second.id,))
                self.assertNotIn(channel.id, getattr(bot, "_channel_addressed_generation", {}))
                self._assert_originals_once(first, second)


if __name__ == "__main__":
    unittest.main(verbosity=2)
