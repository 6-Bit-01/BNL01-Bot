"""Current Discord image delivery through the real conversation/provider owners.

Gemini and Discord transport are fixture boundaries. Provider text is fixed:
these tests establish delivery and attribution, not vision or factual accuracy.
"""

import asyncio
import base64
import os
import sqlite3
import unittest
from types import SimpleNamespace
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl01_bot as bot
import test_public_network_knowledge as public_fixtures
from test_conversation_batching import FakeAuthor, FakeChannel, FakeGuild, FakeMessage


PNG = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8"
    "/x8AAwMCAO+jB1kAAAAASUVORK5CYII="
)
# Distinct, valid 1x1 lossless WebP images generated once as fixtures. Discord
# can name these image.png while correctly declaring their image/webp MIME.
WEBP_FIRST = base64.b64decode("UklGRh4AAABXRUJQVlA4TBEAAAAvAAAAAAdQmVJUq/+BiOh/AAA=")
WEBP_SECOND = base64.b64decode("UklGRh4AAABXRUJQVlA4TBEAAAAvAAAAAAdQj0KVp/+BiOh/AAA=")
ANSWER = "The supplied screenshot is available for this conversation."


def image_attachment(attachment_id=7101, *, mime_type="image/png"):
    return SimpleNamespace(
        id=attachment_id, filename="conversation.png", content_type=mime_type,
        width=1, height=1, size=len(PNG),
        url="https://cdn.discordapp.com/attachments/private-fixture-signed-url",
        read=mock.AsyncMock(return_value=PNG),
    )


def provider_response(text=ANSWER):
    return SimpleNamespace(
        candidates=[SimpleNamespace(content=SimpleNamespace(parts=[SimpleNamespace(text=text)]))],
        usage_metadata=SimpleNamespace(total_token_count=9),
    )


class ConversationImageDeliveryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        # Reuse the existing real SQLite memory/Context/Frame fixture setup;
        # do not inherit its test methods or substitute its conversation owners.
        self.owners = public_fixtures.PublicNetworkKnowledgeTests()
        await self.owners.asyncSetUp()
        self.addAsyncCleanup(self.owners.asyncTearDown)
        self.stack = self.owners.stack
        self.channel = self.channel_fixture(8891)
        self.bot_user = SimpleNamespace(id=999, display_name="BNL-01", bot=True)
        self.provider = mock.Mock(return_value=provider_response())
        fake_client = SimpleNamespace(models=SimpleNamespace(generate_content=self.provider))
        self.stack.enter_context(mock.patch.object(
            type(bot.client), "user", new_callable=mock.PropertyMock,
            return_value=self.bot_user,
        ))
        for name, value in (
            ("get_gemini_client", fake_client),
            ("check_quota_availability", True),
            ("get_guild_config", self.channel.id),
            ("is_privileged_member", False),
        ):
            self.stack.enter_context(mock.patch.object(bot, name, return_value=value))
        self.stack.enter_context(mock.patch.object(
            bot, "resolve_channel_policy", side_effect=lambda channel: getattr(channel, "policy", "sealed_test"),
        ))
        # Accounting itself has separate image-budget tests; this fixture
        # exercises the physical provider contents and retry/delivery owners.
        self.stack.enter_context(mock.patch.object(
            bot, "reserve_local_model_budget", return_value=bot.LocalBudgetReservation(1, "ordinary"),
        ))
        self.stack.enter_context(mock.patch.object(bot, "release_local_model_budget"))
        self.stack.enter_context(mock.patch.object(bot, "record_generation_token_usage"))
        self.stack.enter_context(mock.patch.object(bot.random, "random", return_value=1.0))
        self.stack.enter_context(mock.patch.object(bot, "BNL_ACTIVE_BATCHING_ENABLED", True))
        self.stack.enter_context(mock.patch.object(bot, "BNL_TYPING_INDICATOR_ENABLED", False))
        self.stack.enter_context(mock.patch.object(bot, "POST_GENERATION_CAPTURE_GRACE_SECONDS", 0))
        self.stack.enter_context(mock.patch.object(
            bot, "maybe_build_source_context_for_direct_message", new=mock.AsyncMock(return_value=""),
        ))
        self.addCleanup(self.clear_room_events)

    def clear_room_events(self):
        for channel_id in self.owners.channel_ids:
            bot._recent_room_events.pop((self.owners.guild_id, channel_id), None)

    def channel_fixture(self, channel_id):
        self.owners.channel_ids.add(channel_id)
        return FakeChannel(channel_id, guild=FakeGuild(self.owners.guild_id))

    def message(self, text="BNL, what does this screenshot show?", *, user_id=100, attachment_id=7101, channel=None, tagged=False):
        message = FakeMessage(
            channel or self.channel,
            ("<@999> " if tagged else "") + text,
            author=FakeAuthor(user_id, "Test Member"),
            mentions=[self.bot_user] if tagged else [],
        )
        message.attachments = [image_attachment(attachment_id)]
        return message

    def provider_parts(self, call_index=-1):
        contents = self.provider.call_args_list[call_index].kwargs["contents"]
        if isinstance(contents, str):
            return contents, []
        text = []
        images = []
        for part in contents:
            if isinstance(part, str):
                text.append(part)
            elif getattr(part, "text", None):
                text.append(part.text)
            elif getattr(part, "inline_data", None):
                images.append(part.inline_data)
        return "\n".join(text), images

    def prime_batch(self, *messages):
        now = bot.datetime.now(bot.PACIFIC_TZ)
        for message in messages:
            content = bot.append_media_context_to_text(
                message.content, bot.build_message_media_context(message),
            )
            bot._channel_buffers[self.channel.id].append(
                bot.build_batched_conversation_turn(message, content),
            )
        bot._channel_first_seen[self.channel.id] = now
        bot._channel_last_message_at[self.channel.id] = now
        bot._channel_last_reply_at[self.channel.id] = now - bot.timedelta(hours=2)

    async def test_tagged_current_image_reaches_native_provider_and_direct_send(self):
        message = self.message(tagged=True)
        await bot.on_message(message)
        self.assertEqual(message.replies, [ANSWER])
        self.assertEqual(self.provider.call_count, 1)
        text, images = self.provider_parts()
        self.assertEqual([(part.mime_type, part.data) for part in images], [("image/png", PNG)])
        self.assertIn(f"message_id={message.id}", text)
        self.assertIn("submitting_user_id=100", text)
        message.attachments[0].read.assert_awaited_once_with(use_cached=False)
        self.assertNotIn(message.attachments[0].url, text)

    async def test_two_same_name_batch_speakers_keep_separate_image_origins(self):
        first = self.message(user_id=100, attachment_id=7101)
        second = self.message(user_id=101, attachment_id=7102)
        self.prime_batch(first, second)
        await bot._flush_channel_buffer(self.channel)
        self.assertEqual(self.channel.sent, [ANSWER])
        text, images = self.provider_parts()
        self.assertEqual(len(images), 2)
        for message in (first, second):
            self.assertIn(f"message_id={message.id}; submitting_user_id={message.author.id}", text)
            self.assertIn(f"attachment_id={message.attachments[0].id}", text)
            message.attachments[0].read.assert_awaited_once()
        with sqlite3.connect(bot.DB_FILE) as conn:
            stored = "\n".join(str(row[0]) for row in conn.execute("SELECT content FROM conversations"))
        self.assertNotIn(first.attachments[0].url, stored)
        self.assertNotIn(base64.b64encode(PNG).decode(), stored)

    async def test_observation_and_unsupported_gif_do_not_download(self):
        message = self.message()
        message.attachments[0].content_type = "image/gif"
        message.attachments[0].filename = "conversation.gif"
        with mock.patch.object(bot, "_reset_debounce"):
            await bot.on_message(message)
        self.provider.assert_not_called()
        message.attachments[0].read.assert_not_awaited()
        turn = bot._channel_buffers[self.channel.id][-1]
        self.assertEqual(turn.image_inputs, ())
        self.assertIn("gif attachment", turn.content)

    async def test_current_png_ingress_waits_for_batch_before_downloading(self):
        message = self.message()
        with mock.patch.object(bot, "_reset_debounce"):
            await bot.on_message(message)
        self.provider.assert_not_called()
        message.attachments[0].read.assert_not_awaited()
        turn = bot._channel_buffers[self.channel.id][-1]
        self.assertEqual(turn.image_inputs[0].message_id, message.id)
        await bot._flush_channel_buffer(self.channel)
        self.assertEqual(self.channel.sent, [ANSWER])
        _text, images = self.provider_parts()
        self.assertEqual([part.data for part in images], [PNG])
        message.attachments[0].read.assert_awaited_once()

    async def test_sequential_png_named_webp_screenshots_deliver_only_current_pixels(self):
        # Exercise the real Discord ingress, deferred batch, and native Gemini
        # parts together. A familiar filename/author must not replace the
        # current attachment's content with the previous screenshot's bytes.
        messages = []
        answers = ("First image fixture.", "Second image fixture.", "Current image unavailable.")
        self.provider.side_effect = [provider_response(answer) for answer in answers]
        for index, payload in enumerate((WEBP_FIRST, WEBP_SECOND, WEBP_SECOND)):
            message = self.message(attachment_id=7131 + index)
            attachment = message.attachments[0]
            attachment.filename = "image.png"
            attachment.content_type = "image/webp"
            attachment.size = len(payload)
            attachment.read.return_value = payload
            if index == 2:
                attachment.read.side_effect = OSError("fixture attachment unavailable")
            messages.append(message)

            with mock.patch.object(bot, "_reset_debounce"):
                await bot.on_message(message)
            attachment.read.assert_not_awaited()
            self.assertEqual(self.provider.call_count, index)
            bot._channel_last_reply_at[self.channel.id] = (
                bot.datetime.now(bot.PACIFIC_TZ) - bot.timedelta(hours=2)
            )
            await bot._flush_channel_buffer(self.channel)

            self.assertEqual(self.provider.call_count, index + 1)
            self.assertEqual(self.channel.sent, list(answers[:index + 1]))
            text, images = self.provider_parts()
            expected_images = [] if index == 2 else [("image/webp", payload)]
            self.assertEqual([(part.mime_type, part.data) for part in images], expected_images)
            attachment.read.assert_awaited_once_with(use_cached=False)
            origins = [line for line in text.splitlines() if "Current image attachment:" in line]
            self.assertTrue(origins)
            for origin in origins:
                self.assertIn(f"message_id={message.id}; submitting_user_id=100", origin)
                self.assertIn(f"attachment_id={attachment.id}", origin)
                for previous in messages[:-1]:
                    self.assertNotIn(f"attachment_id={previous.attachments[0].id}", origin)
            if index == 2:
                self.assertIn("Pixels unavailable in this request; metadata only.", text)
                self.assertNotIn("Pixels supplied in this request.", text)

        for message in messages:
            message.attachments[0].read.assert_awaited_once_with(use_cached=False)

    async def test_unaddressed_image_outside_free_speak_is_not_downloaded(self):
        other = self.channel_fixture(8893)
        other.policy = "public_context"
        message = self.message(text="A picture for the room.", channel=other)
        await bot.on_message(message)
        self.provider.assert_not_called()
        message.attachments[0].read.assert_not_awaited()

    async def test_cross_channel_image_reference_is_not_sent_or_read(self):
        message = self.message()
        inputs = bot.capture_message_image_inputs(message)
        other_channel = self.channel_fixture(8892)
        await bot.get_gemini_response_with_optional_typing(
            other_channel, "Describe the current image only.", 100,
            self.owners.guild_id, image_inputs=inputs,
        )
        message.attachments[0].read.assert_not_awaited()
        _text, images = self.provider_parts()
        self.assertEqual(images, [])

    async def test_guard_and_response_obligation_reuse_current_image_once(self):
        message = self.message()
        inputs = bot.capture_message_image_inputs(message)
        prompt = "Current user request: Describe the screenshot.\nCurrent channel policy: sealed_test"
        await bot.get_gemini_response_with_optional_typing(
            self.channel, prompt, 100, self.owners.guild_id, image_inputs=inputs,
        )
        _guarded, diagnostics = await bot.apply_guarded_response_regeneration(
            "What do you need?", prompt=prompt, user_id=100, guild_id=self.owners.guild_id,
            route_mode=bot.ROUTE_MODE_NORMAL_CHAT, channel_policy="sealed_test",
            current_user_text="Describe the screenshot.", has_media=True,
            channel=self.channel, image_inputs=inputs,
        )
        self.assertTrue(diagnostics["generic_non_answer_regenerated"])
        await bot.regenerate_ordinary_chat_response_obligation(
            channel=self.channel, prompt=prompt, reason="generic_non_answer",
            prompt_source_bases=(), user_id=100, guild_id=self.owners.guild_id,
            source_context_available=False, current_user_text="Describe the screenshot.",
            image_inputs=inputs,
        )
        self.assertGreaterEqual(self.provider.call_count, 3)
        for index in range(self.provider.call_count):
            _text, images = self.provider_parts(index)
            self.assertEqual([part.data for part in images], [PNG])
        message.attachments[0].read.assert_awaited_once_with(use_cached=False)

    async def test_batch_collapse_and_handoff_preserve_input_reference(self):
        message = self.message()
        turn = bot.build_batched_conversation_turn(message, message.content)
        following = bot.BatchConversationTurn(
            turn.name, "And compare it with this.", turn.user_id, turn.addressing,
        )
        collapsed = bot._collapse_consecutive_batch_fragments((turn, following))
        self.assertEqual(len(collapsed), 1)
        self.assertIs(collapsed[0].image_inputs[0], turn.image_inputs[0])
        bot._channel_interrupt_handoff[self.channel.id] = collapsed
        now = bot.datetime.now(bot.PACIFIC_TZ)
        bot._channel_last_message_at[self.channel.id] = now
        bot._channel_first_seen[self.channel.id] = now
        bot._channel_last_reply_at[self.channel.id] = now - bot.timedelta(hours=2)
        await bot._flush_channel_buffer(self.channel)
        self.assertEqual(self.channel.sent, [ANSWER])
        _text, images = self.provider_parts()
        self.assertEqual([part.data for part in images], [PNG])
        message.attachments[0].read.assert_awaited_once()

    async def test_reversed_same_name_batch_binds_image_to_transcript_speaker(self):
        # Creation order differs from transcript order; the first speaker's
        # message is text-only. Neither attachment order nor a shared display
        # name may supply the identity mapping.
        image_message = self.message(user_id=100, attachment_id=7111)
        text_message = self.message(user_id=101, attachment_id=7112)
        text_message.attachments = []
        self.prime_batch(text_message, image_message)
        await bot._flush_channel_buffer(self.channel)
        text, images = self.provider_parts()
        self.assertEqual(len(images), 1)
        self.assertIn("speaker 1 - Test Member", text)
        self.assertIn("speaker 2 - Test Member", text)
        image_line = next(
            line for line in text.splitlines()
            if "Current image attachment:" in line
            and f"attachment_id={image_message.attachments[0].id}" in line
        )
        self.assertIn("speaker 2 - Test Member", image_line)
        self.assertNotIn("speaker 1 - Test Member", image_line)

    async def test_late_image_coalescing_keeps_original_image_and_reads_each_once(self):
        first = self.message(user_id=100, attachment_id=7121)
        late = self.message(user_id=101, attachment_id=7122)
        self.prime_batch(first)
        loop = asyncio.get_running_loop()
        call_count = 0

        def generate_with_late_image(**_kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                loop.call_soon_threadsafe(self.prime_batch, late)
            return provider_response()

        self.provider.side_effect = generate_with_late_image
        await bot._flush_channel_buffer(self.channel)
        self.assertEqual(self.channel.sent, [ANSWER])
        self.assertEqual(self.provider.call_count, 2)
        first_text, first_images = self.provider_parts(0)
        final_text, final_images = self.provider_parts(1)
        self.assertEqual(len(first_images), 1)
        self.assertEqual(len(final_images), 2)
        self.assertNotIn(f"attachment_id={late.attachments[0].id}", first_text)
        self.assertIn(f"attachment_id={late.attachments[0].id}", final_text)
        for message in (first, late):
            message.attachments[0].read.assert_awaited_once()
