"""Current screenshot query targets reach real retained-source readers.

Image contents and Gemini responses are fixtures. These tests establish query
handoff, original-row lookup, and refresh ownership, not live visual accuracy.
"""

import asyncio
import json
import time
import sqlite3
import unittest
from unittest import mock

import test_conversation_image_delivery as image_fixture
import test_public_network_knowledge as network_fixture
import test_tiktok_show_evidence_ledger as show_fixture
from bnl_journal_source_store import (
    purge_user_bound_conversation_sources_on_connection,
    record_source_event,
)


bot = image_fixture.bot
REQUEST = (
    "BNL, read your earlier reply in this screenshot. What did you claim, "
    "and what can you actually verify or correct from the original chat records?"
)
SEPTEMBER_COMMENT = "The amber lanterns are bright tonight."
SEPTEMBER_KEY = "show-image-september"
ORIGINAL_KEY = "image-query-september-original"
CONFLICTING_LINES = (
    "Website public read model context:", "accessScope=public",
    "Durable TikTok show analysis context:",
    "- Show=Earlier Broadcast; showDate=2026-08-28; selectedFrom=latestShow.",
)
CONFLICTING_CONTEXT = bot.WebsiteReadModelContext(
    "\n".join(CONFLICTING_LINES), rendered_lines=CONFLICTING_LINES,
    historical_sections=(("show-attendance-1", (2, 3)),),
)


class ImageShowQueryDeliveryTests(unittest.IsolatedAsyncioTestCase):
    # Reuse fixture methods rather than inheriting and rerunning its tests.
    clear_room_events = image_fixture.ConversationImageDeliveryTests.clear_room_events
    channel_fixture = image_fixture.ConversationImageDeliveryTests.channel_fixture
    message = image_fixture.ConversationImageDeliveryTests.message
    provider_parts = image_fixture.ConversationImageDeliveryTests.provider_parts

    async def asyncSetUp(self):
        await image_fixture.ConversationImageDeliveryTests.asyncSetUp(self)
        self.owners._seed_finalized_show()
        self.channel.guild = image_fixture.FakeGuild(self.owners.guild_id)
        self.stack.enter_context(mock.patch.object(bot, "BNL_PRIMARY_GUILD_ID", 77))
        self.stack.enter_context(mock.patch.object(
            bot, "maybe_build_bnl_read_model_context", return_value=CONFLICTING_CONTEXT,
        ))
        september_show = json.loads(
            json.dumps(show_fixture.archived_show())
            .replace("show-attendance-1", SEPTEMBER_KEY)
            .replace("2026-08-28", "2026-09-04")
            .replace("2026-08-29", "2026-09-05")
        )
        recorded = record_source_event(
            bot.DB_FILE, guild_id=77, source_kind="tiktok_live_chat",
            source_key=ORIGINAL_KEY,
            occurred_at_ms=show_fixture.stamp("2026-09-05T00:02:00Z"),
            raw_text=SEPTEMBER_COMMENT, sanitized_summary=SEPTEMBER_COMMENT,
            channel_policy="public_context", public_usable=True,
            subject_ref="discord_user:4242",
            private_display_name="Test September",
            metadata={"eventType": "comment", "handle": "test.september"},
        )
        self.assertTrue(recorded.ok)
        synced = show_fixture.sync_tiktok_show_evidence_ledgers(
            bot.DB_FILE, guild_id=77,
            read_model=show_fixture.authorized_read_model({
                "currentShow": None, "latestShow": september_show,
                "shows": [show_fixture.archived_show()],
            }),
            artist_identity_index=show_fixture.artist_index(),
            environ=show_fixture.ENABLED_QUEUE_ENV,
        )
        self.assertEqual(synced["showsSeen"], 2)

    def extraction(self, message):
        return image_fixture.provider_response(json.dumps({
            "images": [{
                "message_id": message.id,
                "attachment_id": message.attachments[0].id,
                "show_dates": ["2026-09-04"],
                "quote_literals": [SEPTEMBER_COMMENT],
            }],
        }))

    def assert_selected_original(self, text):
        self.assertIn("on 2026-09-04;", text)
        self.assertNotIn("on 2026-08-28;", text)
        self.assertIn(SEPTEMBER_COMMENT, text)
        self.assertIn("Test September", text)
        self.assertIn("matchedOriginalRows=1", text)
        self.assertIn("eligibleOriginalRowsChecked=1", text)

    async def check_current_image_source_delivery(self, *, tagged):
        # Both optional persona rewrite branches would fire without the
        # source-verification rule; no third provider call may appear here.
        self.stack.enter_context(mock.patch.object(bot.random, "random", return_value=0.0))
        message = self.message(REQUEST, tagged=tagged)
        answer = "Test September's retained comment is available."
        self.provider.side_effect = [
            self.extraction(message), image_fixture.provider_response(answer),
        ]
        if tagged:
            await bot.on_message(message)
            self.assertEqual(message.replies, [answer])
        else:
            with mock.patch.object(bot, "_reset_debounce"):
                await bot.on_message(message)
            self.assertEqual(self.provider.call_count, 0)
            message.attachments[0].read.assert_not_awaited()
            await bot._flush_channel_buffer(self.channel)
            self.assertEqual(self.channel.sent, [answer])

        self.assertEqual(self.provider.call_count, 2)
        extraction_text, extraction_images = self.provider_parts(0)
        self.assertEqual(len(extraction_images), 1)
        self.assertNotIn("Earlier Broadcast", extraction_text)
        self.assertNotIn("Test September", extraction_text)
        self.assertNotIn(SEPTEMBER_COMMENT, extraction_text)
        self.assertIn(f"message_id={message.id}", extraction_text)
        final_text, final_images = self.provider_parts(1)
        self.assert_selected_original(final_text)
        self.assertNotIn("showDate=2026-08-28", final_text)
        self.assertIn("accessScope=public", final_text)
        self.assertEqual(
            [(image.mime_type, image.data) for image in final_images],
            [(image.mime_type, image.data) for image in extraction_images],
        )
        message.attachments[0].read.assert_awaited_once_with(use_cached=False)

    async def test_batch_screenshot_date_and_literal_reach_original_reader(self):
        await self.check_current_image_source_delivery(tagged=False)

    async def test_direct_screenshot_date_and_literal_reach_original_reader(self):
        await self.check_current_image_source_delivery(tagged=True)

    async def test_overlapping_preparation_reuses_one_query_and_attachment_read(self):
        message = self.message(REQUEST)
        inputs = bot.capture_message_image_inputs(message)
        def provider(**kwargs):
            time.sleep(0.02)
            return self.extraction(message)
        self.provider.side_effect = provider
        results = await asyncio.gather(*(
            bot.prepare_current_image_show_queries(
                REQUEST, inputs, guild_id=77, channel_id=self.channel.id,
            ) for _ in range(2)
        ))
        self.assertEqual(results[0], results[1])
        self.assertEqual(results[0][0].show_dates, ("2026-09-04",))
        self.assertEqual(self.provider.call_count, 1)
        message.attachments[0].read.assert_awaited_once_with(use_cached=False)

    async def test_source_refresh_retains_current_cue_without_reading_image_again(self):
        message = self.message(REQUEST)
        inputs = bot.capture_message_image_inputs(message)
        self.provider.return_value = self.extraction(message)
        queries = await bot.prepare_current_image_show_queries(
            REQUEST, inputs, guild_id=77, channel_id=self.channel.id,
        )
        selection = {}
        context = network_fixture.REAL_SHOW_CONTEXT_FOR_TURN(
            guild_id=77, user_text=REQUEST,
            website_read_model_context=CONFLICTING_CONTEXT,
            image_queries=queries, selection_out=selection,
        )
        self.assert_selected_original(context)
        basis = bot.build_finalized_show_prompt_source_basis(
            context, guild_id=77, selection=selection,
        )
        self.assertIsNotNone(basis)
        self.assertEqual(basis.show_keys, (SEPTEMBER_KEY,))
        same_queries = await bot.prepare_current_image_show_queries(
            REQUEST, inputs, guild_id=77, channel_id=self.channel.id,
        )
        self.assertEqual(same_queries, queries)
        refreshed, changed = bot.refresh_prompt_source_basis(basis)
        self.assertFalse(changed)
        self.assert_selected_original(refreshed.rendered_context)
        with sqlite3.connect(bot.DB_FILE) as conn:
            purge_user_bound_conversation_sources_on_connection(conn, 77, 4242)
        withdrawn, changed = bot.refresh_prompt_source_basis(refreshed)
        self.assertTrue(changed)
        self.assertEqual(withdrawn.show_keys, (SEPTEMBER_KEY,))
        self.assertIn("matchedOriginalRows=0", withdrawn.rendered_context)
        self.assertNotIn("on 2026-08-28;", withdrawn.rendered_context)
        self.assertEqual(self.provider.call_count, 1)
        message.attachments[0].read.assert_awaited_once_with(use_cached=False)

    async def test_failed_or_malformed_current_extraction_does_not_reuse_prior_queries(self):
        previous = self.message(REQUEST, attachment_id=7301)
        previous_inputs = bot.capture_message_image_inputs(previous)
        self.provider.return_value = self.extraction(previous)
        previous_queries = await bot.prepare_current_image_show_queries(
            REQUEST, previous_inputs, guild_id=77, channel_id=self.channel.id,
        )
        self.assertEqual(previous_queries[0].show_dates, ("2026-09-04",))
        self.assertEqual(previous_queries[0].quote_literals, (SEPTEMBER_COMMENT,))
        for index, result in enumerate((None, image_fixture.provider_response("not json"))):
            with self.subTest(failed=result is None):
                current = self.message(REQUEST, attachment_id=7310 + index)
                current_inputs = bot.capture_message_image_inputs(current)
                extraction = mock.AsyncMock(return_value=result)
                with mock.patch.object(bot, "_generate_gemini_content_with_fallback_async", extraction):
                    queries = await bot.prepare_current_image_show_queries(
                        REQUEST, current_inputs, guild_id=77, channel_id=self.channel.id,
                    )
                    repeated = await bot.prepare_current_image_show_queries(
                        REQUEST, current_inputs, guild_id=77, channel_id=self.channel.id,
                    )
                self.assertEqual(repeated, queries)
                self.assertEqual(len(queries), 1)
                self.assertEqual(queries[0].message_id, current.id)
                self.assertEqual(queries[0].attachment_id, current.attachments[0].id)
                self.assertFalse(queries[0].show_dates)
                self.assertFalse(queries[0].quote_literals)
                self.assertEqual(extraction.await_count, 1)
                current.attachments[0].read.assert_awaited_once_with(use_cached=False)
        self.assertEqual(self.provider.call_count, 1)


if __name__ == "__main__":
    unittest.main()
