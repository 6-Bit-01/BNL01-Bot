import asyncio
import os
import subprocess
import sys
import time
import unittest
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl01_bot


class ConversationBatchConfigTests(unittest.TestCase):
    def test_batching_remains_default_off_in_a_clean_process(self):
        env = os.environ.copy()
        env.pop("BNL_ACTIVE_BATCHING_ENABLED", None)
        completed = subprocess.run(
            [
                sys.executable,
                "-c",
                "import bnl01_bot; print(int(bnl01_bot.BNL_ACTIVE_BATCHING_ENABLED))",
            ],
            cwd=Path(__file__).resolve().parents[1],
            env=env,
            check=True,
            capture_output=True,
            text=True,
        )
        self.assertEqual(completed.stdout.strip().splitlines()[-1], "0")


class FakeGuild:
    def __init__(self, guild_id=7700):
        self.id = guild_id

    def get_member(self, user_id):
        return SimpleNamespace(
            id=user_id,
            roles=[],
            guild_permissions=SimpleNamespace(administrator=False, manage_guild=False),
        )


class FakeChannel:
    def __init__(self, channel_id, *, name="bnl-testing", guild=None):
        self.id = channel_id
        self.name = name
        self.guild = guild or FakeGuild()
        self.sent = []

    async def send(self, text, **_kwargs):
        self.sent.append(text)


class FakeAuthor:
    def __init__(self, user_id=100, display_name="Jon"):
        self.id = user_id
        self.display_name = display_name
        self.bot = False


class FakeMessage:
    _next_id = 9000

    def __init__(self, channel, content, *, author=None, mentions=None):
        FakeMessage._next_id += 1
        self.id = FakeMessage._next_id
        self.channel = channel
        self.guild = channel.guild
        self.content = content
        self.author = author or FakeAuthor()
        self.mentions = list(mentions or [])
        self.raw_mentions = [member.id for member in self.mentions]
        self.role_mentions = []
        self.channel_mentions = []
        self.attachments = []
        self.embeds = []
        self.stickers = []
        self.reference = None
        self.replies = []
        self.reactions = []

    async def reply(self, text, **_kwargs):
        self.replies.append(text)

    async def add_reaction(self, emoji):
        self.reactions.append(emoji)


class BlockingSendChannel(FakeChannel):
    def __init__(self, channel_id, *, name="bnl-testing", guild=None):
        super().__init__(channel_id, name=name, guild=guild)
        self.send_started = asyncio.Event()
        self.release_send = asyncio.Event()

    async def send(self, text, **_kwargs):
        self.send_started.set()
        await self.release_send.wait()
        self.sent.append(text)


class ConversationBatchCoordinatorTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.channel_ids = set()
        self.original_batching_enabled = bnl01_bot.BNL_ACTIVE_BATCHING_ENABLED
        bnl01_bot.BNL_ACTIVE_BATCHING_ENABLED = True

    async def asyncTearDown(self):
        for channel_id in self.channel_ids:
            task = bnl01_bot._channel_tasks.get(channel_id)
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            for state in (
                bnl01_bot._channel_buffers,
                bnl01_bot._channel_tasks,
                bnl01_bot._channel_first_seen,
                bnl01_bot._channel_last_message_at,
                bnl01_bot._channel_last_reply_at,
                bnl01_bot._channel_generating,
                bnl01_bot._channel_generation_id,
                bnl01_bot._channel_preempted_generation_id,
                bnl01_bot._channel_message_interrupt_generation_id,
                bnl01_bot._channel_interrupt_handoff,
                bnl01_bot._channel_payload_wait_extended,
                bnl01_bot._channel_pending_request_intent,
                bnl01_bot._channel_pending_request_anchor,
                bnl01_bot._channel_generation_typing_pause_used,
            ):
                state.pop(channel_id, None)
            for key in list(bnl01_bot._conversation_continuation_state):
                if len(key) >= 2 and key[1] == channel_id:
                    bnl01_bot._conversation_continuation_state.pop(key, None)
            for key, session in list(bnl01_bot._direct_payload_sessions.items()):
                if len(key) >= 2 and key[1] == channel_id:
                    timer_task = session.get("timer_task")
                    if timer_task and not timer_task.done():
                        timer_task.cancel()
                        try:
                            await timer_task
                        except asyncio.CancelledError:
                            pass
                    bnl01_bot._direct_payload_sessions.pop(key, None)
        bnl01_bot.BNL_ACTIVE_BATCHING_ENABLED = self.original_batching_enabled

    def _channel(self, channel_id, *, blocking_send=False):
        self.channel_ids.add(channel_id)
        channel_type = BlockingSendChannel if blocking_send else FakeChannel
        return channel_type(channel_id)

    def _append(self, channel, text, *, user_id=100, name="Jon"):
        bnl01_bot._channel_buffers[channel.id].append((name, text, user_id))
        bnl01_bot._channel_last_message_at[channel.id] = bnl01_bot.datetime.now(bnl01_bot.PACIFIC_TZ)

    def _prime_flush(self, channel, *texts):
        now = bnl01_bot.datetime.now(bnl01_bot.PACIFIC_TZ)
        for text in texts:
            bnl01_bot._channel_buffers[channel.id].append(("Jon", text, 100))
        bnl01_bot._channel_first_seen[channel.id] = now
        bnl01_bot._channel_last_message_at[channel.id] = now
        bnl01_bot._channel_last_reply_at[channel.id] = now - bnl01_bot.timedelta(hours=2)

    def _flush_runtime(self, channel_id, generation_side_effect):
        async def guarded(response, **_kwargs):
            return response, {
                "suppressed": False,
                "scripted_mode_leak_guard_triggered": False,
                "source_grounding_guard_triggered": False,
                "regenerated_for_mode_leak": False,
            }

        stack = ExitStack()
        fake_bot = SimpleNamespace(id=999, display_name="BNL-01")
        stack.enter_context(
            mock.patch.object(type(bnl01_bot.client), "user", new_callable=mock.PropertyMock, return_value=fake_bot)
        )
        for patcher in (
            mock.patch.object(bnl01_bot, "resolve_channel_policy", return_value="sealed_test"),
            mock.patch.object(bnl01_bot, "get_sealed_test_recall_guard_response", return_value=None),
            mock.patch.object(bnl01_bot, "get_restricted_channel_recall_guard_response", return_value=None),
            mock.patch.object(bnl01_bot, "try_self_reflection_response", return_value=None),
            mock.patch.object(bnl01_bot, "try_repair_response", return_value=None),
            mock.patch.object(bnl01_bot, "resolve_recent_media_followup", return_value=None),
            mock.patch.object(bnl01_bot, "try_memory_recall_response", return_value=None),
            mock.patch.object(bnl01_bot, "choose_response_style", return_value=("balanced", "")),
            mock.patch.object(bnl01_bot, "log_response_style"),
            mock.patch.object(bnl01_bot, "conversation_context_v2_enabled", return_value=False),
            mock.patch.object(bnl01_bot, "build_recent_text_room_context_for_prompt", return_value=""),
            mock.patch.object(bnl01_bot, "current_batch_references_recent_media", return_value=False),
            mock.patch.object(
                bnl01_bot,
                "get_gemini_response",
                new=mock.AsyncMock(side_effect=generation_side_effect),
            ),
            mock.patch.object(
                bnl01_bot,
                "suppress_stale_media_fallback",
                side_effect=lambda response, **_kwargs: response,
            ),
            mock.patch.object(
                bnl01_bot,
                "apply_guarded_response_regeneration",
                new=mock.AsyncMock(side_effect=guarded),
            ),
            mock.patch.object(bnl01_bot, "_contains_unsupported_source_authority_claim", return_value=False),
            mock.patch.object(bnl01_bot, "save_model_message"),
            mock.patch.object(bnl01_bot, "mark_recent_media_events_response_state"),
            mock.patch.object(bnl01_bot, "update_last_route_debug"),
            mock.patch.object(bnl01_bot, "get_guild_config", return_value=channel_id),
            mock.patch.object(bnl01_bot, "POST_GENERATION_CAPTURE_GRACE_SECONDS", 0),
            mock.patch.object(bnl01_bot, "HARD_INTERRUPT_REEVALUATE_PAUSE_MIN_SECONDS", 0),
            mock.patch.object(bnl01_bot, "HARD_INTERRUPT_REEVALUATE_PAUSE_MAX_SECONDS", 0),
        ):
            stack.enter_context(patcher)
        return stack

    def _on_message_runtime(self, channel_id, *, followup_candidate=True):
        stack = ExitStack()
        fake_bot = SimpleNamespace(id=999, display_name="BNL-01")
        stack.enter_context(
            mock.patch.object(type(bnl01_bot.client), "user", new_callable=mock.PropertyMock, return_value=fake_bot)
        )
        async_false_handlers = (
            "maybe_handle_provider_status_command",
            "maybe_handle_debug_last_route_command",
            "maybe_handle_channel_audit_command",
            "maybe_handle_backfill_command",
            "maybe_handle_journal_command",
            "maybe_handle_population_scan_command",
            "maybe_handle_source_file_refresh_command",
            "maybe_handle_source_file_enrichment_command",
            "maybe_handle_source_file_lookup_command",
            "maybe_handle_entity_context_resolver_command",
            "maybe_handle_entity_activity_summary_command",
            "maybe_handle_dossier_recommendation_command",
        )
        for name in async_false_handlers:
            stack.enter_context(mock.patch.object(bnl01_bot, name, new=mock.AsyncMock(return_value=False)))
        for patcher in (
            mock.patch.object(bnl01_bot, "get_guild_config", return_value=channel_id),
            mock.patch.object(bnl01_bot, "resolve_channel_policy", return_value="sealed_test"),
            mock.patch.object(bnl01_bot, "upsert_user_profile"),
            mock.patch.object(bnl01_bot, "is_direct_bnl_target", return_value=False),
            mock.patch.object(
                bnl01_bot,
                "build_message_media_context",
                return_value={"present": False, "included": False, "count": 0, "items": []},
            ),
            mock.patch.object(bnl01_bot, "record_recent_room_event_from_message"),
            mock.patch.object(bnl01_bot, "_is_previous_message_request", return_value=False),
            mock.patch.object(bnl01_bot, "maybe_record_live_community_presence"),
            mock.patch.object(bnl01_bot, "_is_recent_direct_followup", return_value=followup_candidate),
            mock.patch.object(bnl01_bot, "_is_recent_conversation_continuation", return_value=False),
            mock.patch.object(bnl01_bot, "maybe_record_member_activity_event"),
            mock.patch.object(bnl01_bot, "is_internal_channel_redirect_candidate", return_value=False),
            mock.patch.object(bnl01_bot, "is_research_and_development_channel", return_value=False),
            mock.patch.object(bnl01_bot.random, "random", return_value=1.0),
            mock.patch.object(bnl01_bot, "record_passive_user_activity", return_value=False),
            mock.patch.object(bnl01_bot, "save_user_message", return_value=SimpleNamespace()),
        ):
            stack.enter_context(patcher)
        return stack

    async def test_quiet_timer_restarts_when_a_second_fragment_arrives(self):
        channel = self._channel(8101)
        flushed = asyncio.Event()
        snapshots = []
        flush_times = []

        async def fake_flush(_channel, scheduler_wait_state=None):
            snapshots.append(list(bnl01_bot._channel_buffers[channel.id]))
            flush_times.append(time.monotonic())
            flushed.set()

        wait_state = {
            "selected_wait_seconds": 0.8,
            "max_wait_seconds": 2.0,
            "payload_count": 0,
            "elapsed_seconds": 0,
            "request_anchor": False,
        }
        with (
            mock.patch.object(bnl01_bot, "_adaptive_batch_wait_seconds", return_value=wait_state),
            mock.patch.object(bnl01_bot, "_batch_max_wait_seconds", return_value=2.0),
            mock.patch.object(bnl01_bot, "_flush_channel_buffer", side_effect=fake_flush),
        ):
            self._append(channel, "first fragment")
            bnl01_bot._reset_debounce(channel)
            first_seen = bnl01_bot._channel_first_seen[channel.id]
            await asyncio.sleep(0.35)

            self._append(channel, "second fragment")
            second_arrived_at = time.monotonic()
            bnl01_bot._reset_debounce(channel)
            self.assertEqual(first_seen, bnl01_bot._channel_first_seen[channel.id])

            await asyncio.sleep(0.55)
            self.assertFalse(flushed.is_set())
            await asyncio.wait_for(flushed.wait(), timeout=0.5)

        self.assertEqual(len(snapshots), 1)
        self.assertEqual([item[1] for item in snapshots[0]], ["first fragment", "second fragment"])
        self.assertGreaterEqual(flush_times[0] - second_arrived_at, 0.70)
        self.assertLess(flush_times[0] - second_arrived_at, 1.20)

    async def test_hard_cap_remains_anchored_to_the_first_fragment(self):
        channel = self._channel(8102)
        flushed = asyncio.Event()
        batch_events = []
        flush_times = []
        started_at = time.monotonic()

        async def fake_flush(_channel, scheduler_wait_state=None):
            flush_times.append(time.monotonic())
            flushed.set()

        wait_state = {
            "selected_wait_seconds": 2.0,
            "max_wait_seconds": 0.8,
            "payload_count": 0,
            "elapsed_seconds": 0,
            "request_anchor": False,
        }
        with (
            mock.patch.object(bnl01_bot, "_adaptive_batch_wait_seconds", return_value=wait_state),
            mock.patch.object(bnl01_bot, "_batch_max_wait_seconds", return_value=0.8),
            mock.patch.object(bnl01_bot, "_flush_channel_buffer", side_effect=fake_flush),
            mock.patch.object(
                bnl01_bot,
                "_log_batch_event",
                side_effect=lambda _level, event, _guild_id, _channel_id, _count, reason: batch_events.append((event, reason)),
            ),
        ):
            self._append(channel, "one")
            bnl01_bot._reset_debounce(channel)
            first_seen = bnl01_bot._channel_first_seen[channel.id]
            await asyncio.sleep(0.20)
            self._append(channel, "two")
            bnl01_bot._reset_debounce(channel)
            self.assertEqual(first_seen, bnl01_bot._channel_first_seen[channel.id])
            await asyncio.sleep(0.20)
            self._append(channel, "three")
            bnl01_bot._reset_debounce(channel)
            self.assertEqual(first_seen, bnl01_bot._channel_first_seen[channel.id])
            await asyncio.wait_for(flushed.wait(), timeout=0.7)

        self.assertIn(("flush", "hard_deadline"), batch_events)
        elapsed = flush_times[0] - started_at
        self.assertGreaterEqual(elapsed, 0.65)
        self.assertLess(elapsed, 1.10)

    async def test_scheduler_flushes_two_fragments_with_one_generation_and_send(self):
        channel = self._channel(8103)
        prompts = []

        async def generate(prompt, **_kwargs):
            prompts.append(prompt)
            return "One combined response."

        self._prime_flush(channel, "BNL, why are routers weird?", "and why do they blink?")
        wait_state = {
            "selected_wait_seconds": 0.01,
            "max_wait_seconds": 1.0,
            "payload_count": 0,
            "elapsed_seconds": 0,
            "request_anchor": False,
        }
        with self._flush_runtime(channel.id, generate), mock.patch.object(
            bnl01_bot, "_adaptive_batch_wait_seconds", return_value=wait_state
        ):
            bnl01_bot._reset_debounce(channel)
            await asyncio.wait_for(bnl01_bot._channel_tasks[channel.id], timeout=0.5)

        self.assertEqual(len(prompts), 1)
        self.assertIn("BNL, why are routers weird?", prompts[0])
        self.assertIn("and why do they blink?", prompts[0])
        self.assertEqual(channel.sent, ["One combined response."])

    async def test_late_fragment_does_not_cancel_generation_and_rebuilds_once(self):
        channel = self._channel(8104)
        generation_started = asyncio.Event()
        release_generation = asyncio.Event()
        prompts = []

        async def generate(prompt, **_kwargs):
            prompts.append(prompt)
            if len(prompts) == 1:
                generation_started.set()
                await release_generation.wait()
                return "Stale first draft."
            return "Fresh combined response."

        self._prime_flush(channel, "BNL, why are routers weird?")
        with self._flush_runtime(channel.id, generate):
            task = asyncio.create_task(bnl01_bot._flush_channel_buffer(channel))
            bnl01_bot._channel_tasks[channel.id] = task
            await asyncio.wait_for(generation_started.wait(), timeout=0.5)

            generation_id = bnl01_bot._channel_generation_id[channel.id]
            bnl01_bot._channel_preempted_generation_id[channel.id] = generation_id
            bnl01_bot._channel_message_interrupt_generation_id[channel.id] = generation_id
            self._append(channel, "and why do they blink?")
            bnl01_bot._reset_debounce(channel)

            self.assertIs(bnl01_bot._channel_tasks[channel.id], task)
            self.assertFalse(task.cancelled())
            release_generation.set()
            await asyncio.wait_for(task, timeout=1)

        self.assertEqual(len(prompts), 2)
        self.assertNotIn("and why do they blink?", prompts[0])
        self.assertIn("and why do they blink?", prompts[1])
        self.assertEqual(channel.sent, ["Fresh combined response."])
        self.assertEqual(list(bnl01_bot._channel_buffers[channel.id]), [])

    async def test_interrupt_handoff_merges_with_newer_buffered_fragment(self):
        channel = self._channel(8107)
        prompts = []

        async def generate(prompt, **_kwargs):
            prompts.append(prompt)
            return "All three turns survived."

        now = bnl01_bot.datetime.now(bnl01_bot.PACIFIC_TZ)
        bnl01_bot._channel_interrupt_handoff[channel.id] = [
            ("Jon", "BNL, why are routers weird?", 100),
            ("Jon", "and why do they blink?", 100),
        ]
        bnl01_bot._channel_buffers[channel.id].append(("Jon", "also, why are they warm?", 100))
        bnl01_bot._channel_first_seen[channel.id] = now
        bnl01_bot._channel_last_message_at[channel.id] = now
        bnl01_bot._channel_last_reply_at[channel.id] = now - bnl01_bot.timedelta(hours=2)

        with self._flush_runtime(channel.id, generate):
            await bnl01_bot._flush_channel_buffer(channel)

        self.assertEqual(len(prompts), 1)
        self.assertIn("BNL, why are routers weird?", prompts[0])
        self.assertIn("and why do they blink?", prompts[0])
        self.assertIn("also, why are they warm?", prompts[0])
        self.assertEqual(channel.sent, ["All three turns survived."])
        self.assertEqual(list(bnl01_bot._channel_buffers[channel.id]), [])
        self.assertNotIn(channel.id, bnl01_bot._channel_interrupt_handoff)

    async def test_message_after_send_commit_is_rescheduled_instead_of_orphaned(self):
        channel = self._channel(8105, blocking_send=True)

        async def generate(_prompt, **_kwargs):
            return "Committed first response."

        self._prime_flush(channel, "BNL, why are routers weird?")
        with self._flush_runtime(channel.id, generate):
            first_task = asyncio.create_task(bnl01_bot._flush_channel_buffer(channel))
            bnl01_bot._channel_tasks[channel.id] = first_task
            await asyncio.wait_for(channel.send_started.wait(), timeout=0.5)

            generation_id = bnl01_bot._channel_generation_id[channel.id]
            bnl01_bot._channel_preempted_generation_id[channel.id] = generation_id
            bnl01_bot._channel_message_interrupt_generation_id[channel.id] = generation_id
            self._append(channel, "one more thing")
            bnl01_bot._reset_debounce(channel)
            self.assertIs(bnl01_bot._channel_tasks[channel.id], first_task)

            channel.release_send.set()
            await asyncio.wait_for(first_task, timeout=1)
            successor = bnl01_bot._channel_tasks[channel.id]

        self.assertIsNot(successor, first_task)
        self.assertFalse(successor.done())
        self.assertEqual([item[1] for item in bnl01_bot._channel_buffers[channel.id]], ["one more thing"])
        self.assertEqual(channel.sent, ["Committed first response."])

    async def test_deferred_session_cancels_an_active_batch_generation(self):
        channel = self._channel(8108)
        generation_started = asyncio.Event()
        never_release = asyncio.Event()

        async def generate(_prompt, **_kwargs):
            generation_started.set()
            await never_release.wait()
            return "must not send"

        self._prime_flush(channel, "BNL, why are routers weird?")
        with self._flush_runtime(channel.id, generate):
            task = asyncio.create_task(bnl01_bot._flush_channel_buffer(channel))
            bnl01_bot._channel_tasks[channel.id] = task
            await asyncio.wait_for(generation_started.wait(), timeout=0.5)
            self.assertTrue(bnl01_bot._channel_generating[channel.id])

            await bnl01_bot._preempt_pending_batch_for_deferred_session(channel)

        self.assertTrue(task.done())
        self.assertFalse(bnl01_bot._channel_generating[channel.id])
        self.assertNotIn(channel.id, bnl01_bot._channel_tasks)
        self.assertEqual(channel.sent, [])

    async def test_on_message_human_recipient_outranks_stale_followup(self):
        channel = self._channel(8109)
        author = FakeAuthor()
        miss_bit = SimpleNamespace(id=456, display_name="Miss Bit", bot=False)
        message = FakeMessage(
            channel,
            "<@456> what do you think about Friday's opener?",
            author=author,
            mentions=[miss_bit],
        )

        with self._on_message_runtime(channel.id, followup_candidate=True):
            await bnl01_bot.on_message(message)
            scheduled = bnl01_bot._channel_tasks[channel.id]
            scheduled.cancel()
            try:
                await scheduled
            except asyncio.CancelledError:
                pass
            bnl01_bot._channel_tasks.pop(channel.id, None)
            await bnl01_bot._flush_channel_buffer(channel)

        self.assertEqual(channel.sent, [])
        self.assertEqual(message.replies, [])
        self.assertEqual(list(bnl01_bot._channel_buffers[channel.id]), [])

    async def test_on_message_plain_name_request_preempts_batch_and_accepts_tag_payload(self):
        channel = self._channel(8110)
        author = FakeAuthor()
        miss_bit = SimpleNamespace(id=456, display_name="Miss Bit", bot=False)
        first = FakeMessage(channel, "one setup fragment", author=author)
        request = FakeMessage(
            channel,
            "BNL, tell me something about each of these people",
            author=author,
        )
        payload = FakeMessage(channel, "<@456>", author=author, mentions=[miss_bit])

        with self._on_message_runtime(channel.id, followup_candidate=True), mock.patch.object(
            bnl01_bot,
            "_direct_session_timer",
            new=mock.AsyncMock(return_value=None),
        ):
            await bnl01_bot.on_message(first)
            old_batch_task = bnl01_bot._channel_tasks[channel.id]
            self.assertEqual(len(bnl01_bot._channel_buffers[channel.id]), 1)

            await bnl01_bot.on_message(request)
            key = (channel.guild.id, channel.id, author.id)
            session = bnl01_bot._direct_payload_sessions[key]
            self.assertTrue(old_batch_task.done())
            self.assertEqual(list(bnl01_bot._channel_buffers[channel.id]), [])
            self.assertNotIn(channel.id, bnl01_bot._channel_tasks)

            await bnl01_bot.on_message(payload)

        self.assertEqual(session["payload_lines"], ["@Miss Bit"])
        self.assertEqual(len(session["payload_turn_contexts"]), 1)
        self.assertEqual(channel.sent, [])
        self.assertEqual(request.replies, [])
        self.assertEqual(payload.replies, [])

    async def test_buffer_max_never_starts_an_overlapping_flush(self):
        channel = self._channel(8106)
        bnl01_bot._channel_generating[channel.id] = True
        self._append(channel, "late fragment")

        with mock.patch.object(bnl01_bot, "_flush_channel_buffer", new=mock.AsyncMock()) as flush:
            flushed = await bnl01_bot._flush_channel_buffer_now(channel)

        self.assertFalse(flushed)
        flush.assert_not_awaited()

    async def test_buffer_max_cancels_waiter_and_flushes_once_when_idle(self):
        channel = self._channel(8111)
        waiter_block = asyncio.Event()
        scheduled = asyncio.create_task(waiter_block.wait())
        bnl01_bot._channel_tasks[channel.id] = scheduled
        self._append(channel, "full buffer fragment")

        with mock.patch.object(bnl01_bot, "_flush_channel_buffer", new=mock.AsyncMock()) as flush:
            flushed = await bnl01_bot._flush_channel_buffer_now(channel)

        self.assertTrue(flushed)
        self.assertTrue(scheduled.cancelled())
        flush.assert_awaited_once_with(channel)
        self.assertTrue(bnl01_bot._channel_tasks[channel.id].done())


if __name__ == "__main__":
    unittest.main()
