"""A grouped exact count omits only its competing historical website prose."""
import unittest
import gc
from unittest import mock

import test_conversation_batching as existing

bot = existing.bnl01_bot


class GroupedWordFrequencyContextTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = existing.ConversationBatchCoordinatorTests.asyncSetUp
    asyncTearDown = existing.ConversationBatchCoordinatorTests.asyncTearDown
    _channel = existing.ConversationBatchCoordinatorTests._channel
    _prime_flush = existing.ConversationBatchCoordinatorTests._prime_flush
    _flush_runtime = existing.ConversationBatchCoordinatorTests._flush_runtime

    async def test_exact_selected_episode_count_replaces_stale_grouped_readout(self):
        channel = self._channel(991661)
        lines = ("Current unrelated queue remains", "STALE_SELECTED_EPISODE_COUNT_11",
                 "OTHER_EPISODE_CONTEXT_RETAINED")
        website = bot.WebsiteReadModelContext("\n".join(lines), rendered_lines=lines,
            historical_sections=(("show-selected", (1,)), ("show-other", (2,))))
        current = "Finalized exact word count: occurrenceCount=67; matchingMessageCount=66"
        prompts = []

        async def generate(prompt, **_kwargs):
            prompts.append(prompt)
            return "The recorded count is 67 occurrences in 66 messages."

        def evidence(**kwargs):
            kwargs["selection_out"].update(
                source_refs=(("show-selected", "source-digest"),),
                user_text=kwargs["user_text"], subject_user_id=100,
                word_frequency_lookup=({"showKey": "show-selected"},))
            return current

        def fresh_evidence(_database, **kwargs):
            self.assertEqual(kwargs["pinned_show_keys"], ("show-selected",))
            return evidence(**kwargs)

        self._prime_flush(channel, "BNL, how many times did panda appear during the last TikTok stream?")
        with (
            self._flush_runtime(channel.id, generate),
            mock.patch.object(bot, "maybe_build_bnl_read_model_context", return_value=website),
            mock.patch.object(bot, "build_tiktok_show_evidence_context_for_turn", side_effect=evidence),
            mock.patch.object(bot, "build_tiktok_show_evidence_context", side_effect=fresh_evidence) as fresh_reader,
            mock.patch.object(bot, "env_queue_production_enabled", return_value=True),
        ):
            try:
                await bot._flush_channel_buffer(channel)
                self.assertEqual(len(prompts), 1)
                self.assertIn(current, prompts[0])
                self.assertNotIn("STALE_SELECTED_EPISODE_COUNT_11", prompts[0])
                self.assertIn("OTHER_EPISODE_CONTEXT_RETAINED", prompts[0])
                self.assertIn("Current unrelated queue remains", prompts[0])
                self.assertGreaterEqual(fresh_reader.call_count, 1)
                self.assertEqual(channel.sent, ["The recorded count is 67 occurrences in 66 messages."])
            finally:
                # Collect unreachable native SQLite handles after assertions,
                # before the existing temporary fixture directory is removed.
                gc.collect()
