"""Mandatory source capture can wait without starving Discord's event loop."""

import asyncio
import threading
import unittest
from unittest import mock

from tests import test_conversation_batching as runtime


bot = runtime.bnl01_bot


class MessageCaptureAvailabilityTests(unittest.IsolatedAsyncioTestCase):
    async def _blocked_capture(self, *, active, passive=False, configured=True,
                               human_tag=False, text="What makes the opener work?"):
        channel = runtime.FakeChannel(8830, name="community")
        message = runtime.FakeMessage(channel, text)
        fixture = runtime.ConversationBatchCoordinatorTests()
        started, release = threading.Event(), threading.Event()
        main_thread = threading.get_ident()
        observed = []
        failure = RuntimeError("capture did not finish")

        def slow_capture(*args, **kwargs):
            observed.append((threading.get_ident(), args, kwargs))
            started.set()
            if not release.wait(2):
                raise AssertionError("Discord event loop could not release capture")
            raise failure

        target = "record_passive_user_activity" if passive else "save_user_message"
        active_channel = channel.id if active else (8831 if configured else None)
        with fixture._on_message_runtime(active_channel,
                                         followup_candidate=False), \
                mock.patch.object(bot, "resolve_channel_policy",
                                  return_value="public_home" if active else "public_context"), \
                mock.patch.object(bot, "is_direct_bnl_target",
                                  return_value=not active and not passive and not human_tag), \
                mock.patch.object(bot, "is_human_to_human_tag_only_turn",
                                  return_value=human_tag), \
                mock.patch.object(bot, target, side_effect=slow_capture) as capture, \
                mock.patch.object(bot, "get_gemini_response_with_optional_typing",
                                  new=mock.AsyncMock()) as generation, \
                mock.patch.object(bot, "send_planned_conversation_response",
                                  new=mock.AsyncMock()) as send:
            task = asyncio.create_task(bot.on_message(message))
            try:
                async def other_discord_event():
                    while not started.is_set():
                        await asyncio.sleep(0.001)
                    self.assertFalse(task.done())
                    generation.assert_not_awaited()
                    send.assert_not_awaited()
                    self.assertEqual(message.replies, [])
                    release.set()

                await asyncio.wait_for(other_discord_event(), timeout=1)
                with self.assertRaises(RuntimeError) as caught:
                    await task
                self.assertIs(caught.exception, failure)
            finally:
                release.set()
                if not task.done():
                    task.cancel()
                await asyncio.gather(task, return_exceptions=True)

            capture.assert_called_once()
            generation.assert_not_awaited()
            send.assert_not_awaited()
        self.assertNotEqual(observed[0][0], main_thread)
        if not passive:
            self.assertEqual(observed[0][1], (
                message.author.id, message.author.display_name,
                message.guild.id, message.content,
            ))
            self.assertEqual(observed[0][2]["message_id"], message.id)
            self.assertEqual(observed[0][2]["channel_id"], channel.id)

    async def test_main_channel_capture_wait_leaves_discord_events_running(self):
        await self._blocked_capture(active=True)

    async def test_tagged_channel_capture_wait_leaves_discord_events_running(self):
        await self._blocked_capture(active=False)

    async def test_passive_capture_wait_leaves_discord_events_running(self):
        await self._blocked_capture(active=False, passive=True)

    async def test_unconfigured_tagged_capture_wait_leaves_discord_events_running(self):
        await self._blocked_capture(active=False, configured=False)

    async def test_exact_name_capture_still_finishes_before_deterministic_reply(self):
        await self._blocked_capture(active=True, text=(
            "reply with exactly these two names, and no other words: "
            "Copper Kite, Signal Finch"))

    async def test_human_tag_observation_cannot_block_the_event_loop(self):
        await self._blocked_capture(active=False, human_tag=True)


if __name__ == "__main__":
    unittest.main()
