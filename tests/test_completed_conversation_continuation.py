"""Completed exchanges retain typed addressing without treating all chatter as owed.

Actual ingress, planner, addressing, capture and model persistence are exercised;
the established fixture substitutes Discord and provider/source boundaries.
"""
import unittest
from contextlib import ExitStack
from datetime import timedelta
from unittest import mock

import test_ordinary_addressed_burst_ingress as ingress

bot = ingress.bot
existing = ingress.existing
FIRST = "Can you find the earlier recordings by this fictional musician?"
CORRECTION = "That account is incomplete. Others sent my recordings earlier."


class CompletedConversationContinuationTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = ingress.OrdinaryAddressedBurstIngressTests.asyncSetUp
    asyncTearDown = ingress.OrdinaryAddressedBurstIngressTests.asyncTearDown
    _channel = ingress.OrdinaryAddressedBurstIngressTests._channel
    _on_message_runtime = ingress.OrdinaryAddressedBurstIngressTests._on_message_runtime
    _flush_runtime = ingress.OrdinaryAddressedBurstIngressTests._flush_runtime
    _runtime = ingress.OrdinaryAddressedBurstIngressTests._runtime
    _drain = ingress.OrdinaryAddressedBurstIngressTests._drain
    _assert_originals_once = ingress.OrdinaryAddressedBurstIngressTests._assert_originals_once

    def _completed_runtime(self, channel, generate, *, policy="sealed_test"):
        actual_direct = bot._is_recent_direct_followup
        actual_continuation = bot._is_recent_conversation_continuation
        stack = ExitStack()
        stack.enter_context(self._runtime(channel, generate, policy=policy))
        stack.enter_context(mock.patch.object(bot, "_is_recent_direct_followup", side_effect=actual_direct))
        stack.enter_context(mock.patch.object(bot, "_is_recent_conversation_continuation", side_effect=actual_continuation))
        stack.enter_context(mock.patch.object(bot, "BATCH_WINDOW_SECONDS", 0.01))
        stack.enter_context(mock.patch.object(bot, "BATCH_REPLY_COOLDOWN_SECONDS", 0))
        if getattr(self, "no_store_context", ""):
            stack.enter_context(mock.patch.object(bot, "maybe_build_bnl_read_model_context",
                return_value=self.no_store_context))
        return stack

    def _first(self, channel):
        mention = existing.SimpleNamespace(id=999, display_name="BNL-01", bot=True)
        return existing.FakeMessage(channel, "<@999> " + FIRST, mentions=[mention])

    async def test_correction_after_committed_answer_is_a_new_owed_turn(self):
        for offset, policy in enumerate(("sealed_test", "public_home", "public_context")):
            with self.subTest(policy=policy):
                channel = self._channel(996200 + offset)
                first = self._first(channel)
                second = existing.FakeMessage(channel, CORRECTION, author=first.author)
                prompts = []
                source_version = ["CURRENT NEUTRAL EVIDENCE ONE"]

                async def generate(prompt, **_kwargs):
                    prompts.append(prompt)
                    return "No relevant recording was found." if len(prompts) == 1 else "I will reconsider the available record."

                self.assertFalse(bot.is_conversational_repair_intent(CORRECTION))
                self.assertFalse(bot._detect_request_payload_expectation(CORRECTION)[0])
                with self._completed_runtime(channel, generate, policy=policy), mock.patch.object(
                    bot, "build_tiktok_show_evidence_context_for_turn",
                    side_effect=lambda **_kwargs: source_version[0],
                ) as source_reader:
                    await bot.on_message(first)
                    await self._drain()
                    self.assertEqual(len(channel.sent), 1)
                    source_version[0] = "CURRENT NEUTRAL EVIDENCE TWO"
                    await bot.on_message(second)
                    await self._drain()
                self.assertEqual(source_reader.call_count, 2)
                self.assertIn(source_version[0], prompts[-1])
                self.assertEqual(len(prompts), 2)
                self.assertEqual(len(channel.sent), 2)
                self.assertIn(CORRECTION, prompts[-1])
                self.assertEqual(self.frames[-1].source_message_ids, (second.id,))
                self.assertEqual(self.frames[-1].route_mode, bot.ROUTE_MODE_NORMAL_CHAT)
                self.assertEqual(self.frames[-1].explicit_mention_count, 0)
                self.assertEqual(self.model_save.call_count, 2)
                self._assert_originals_once(first, second)

    async def test_acknowledgments_after_answer_remain_quiet(self):
        for offset, text in enumerate(("thanks", "👍", "nice")):
            with self.subTest(text=text):
                channel = self._channel(996210 + offset)
                first = self._first(channel)
                prompts = []

                async def generate(prompt, **_kwargs):
                    prompts.append(prompt)
                    return "The available record is limited."

                with self._completed_runtime(channel, generate):
                    await bot.on_message(first)
                    await self._drain()
                    self.assertEqual(len(prompts), 1)
                    await bot.on_message(existing.FakeMessage(channel, text, author=first.author))
                    await self._drain()
                self.assertEqual(len(prompts), 1)
                self.assertEqual(len(channel.sent), 1)

    async def test_no_store_answer_keeps_public_followup_without_persisting_output(self):
        channel = self._channel(996260)
        first = self._first(channel)
        followup = existing.FakeMessage(channel,
            "Which of those did Test Submitter submit, and when was that show?",
            author=first.author)
        generate = mock.AsyncMock(return_value="The neutral record contains two entries.")
        website = "Website private queue read model context:\nNEUTRAL NO-STORE SOURCE"
        with self._completed_runtime(channel, generate, policy="public_context"), mock.patch.object(
            bot, "maybe_build_bnl_read_model_context", return_value=website,
        ):
            await bot.on_message(first)
            await self._drain()
            self.assertEqual(len(channel.sent), 1)
            self.model_save.assert_not_called()
            # This is beyond the legacy direct window but inside the ordinary
            # completed-exchange window, as in the public delivery regression.
            key = bot._conversation_state_key(first.guild.id, channel.id, first.author.id)
            state = bot._conversation_continuation_state.get(key)
            if state:
                state["last_bnl_reply_at"] -= timedelta(seconds=70)
                state["live_exchange_until"] -= timedelta(seconds=70)
            if (channel.id, first.author.id) in bot._recent_direct_response_window:
                bot._recent_direct_response_window[(channel.id, first.author.id)] -= timedelta(seconds=70)
            await bot.on_message(followup)
            await self._drain()
            self.assertEqual(generate.await_count, 2)
            self.assertEqual(len(channel.sent), 2)
            self.model_save.assert_not_called()
            state = bot._conversation_continuation_state[key]
            self.assertEqual(state["channel_policy"], "public_context")
            self.assertNotIn("NEUTRAL", str(state))
            await bot.on_message(existing.FakeMessage(channel, "thanks", author=first.author))
            await self._drain()
            self.assertEqual(generate.await_count, 2)
        self._assert_originals_once(first, followup)
        conn = ingress.sqlite3.connect(bot.DB_FILE)
        try:
            self.assertEqual(conn.execute("SELECT count(*) FROM conversations WHERE role='model'").fetchone()[0], 0)
            self.assertEqual(conn.execute("SELECT count(*) FROM memory_ledger_entries WHERE source_role='model'").fetchone()[0], 0)
            self.assertEqual(conn.execute("SELECT count(*) FROM conversations WHERE content LIKE '%NEUTRAL NO-STORE SOURCE%'").fetchone()[0], 0)
        finally:
            conn.close()

    async def test_no_store_failed_or_partial_send_does_not_open_followup(self):
        self.no_store_context = "Website private queue read model context:\nNEUTRAL NO-STORE SOURCE"
        for index, partial in enumerate((False, True)):
            with self.subTest(partial=partial):
                channel = self._channel(996270 + index)
                first = self._first(channel)
                answer = "A neutral record has one entry. " * (90 if partial else 1)
                generate = mock.AsyncMock(return_value=answer)
                outcomes = ([existing.SimpleNamespace(id=77)] if partial else []) + [RuntimeError("neutral send failure")]
                with self._completed_runtime(channel, generate, policy="public_context"), mock.patch.object(
                    channel, "send", new=mock.AsyncMock(side_effect=outcomes),
                ) as send:
                    await bot.on_message(first)
                    await self._drain()
                    self.assertEqual(send.await_count, 2 if partial else 1)
                    self.model_save.assert_not_called()
                    self.assertNotIn(bot._conversation_state_key(first.guild.id, channel.id, first.author.id),
                        bot._conversation_continuation_state)
                    self.assertNotIn((channel.id, first.author.id), bot._recent_direct_response_window)

    async def test_no_store_followup_retains_existing_identity_scope_and_expiry_guards(self):
        self.no_store_context = "Website private queue read model context:\nNEUTRAL NO-STORE SOURCE"
        await self.test_completed_followup_does_not_cross_identity_scope_or_expiry()
        self.model_save.assert_not_called()

    async def test_completed_followup_does_not_cross_identity_scope_or_expiry(self):
        for offset, boundary in enumerate(("author", "channel", "thread", "guild", "expiry", "policy", "other_person")):
            with self.subTest(boundary=boundary):
                channel = self._channel(996220 + offset * 2)
                first = self._first(channel)
                generate = mock.AsyncMock(return_value="No relevant recording was found.")
                with self._completed_runtime(channel, generate, policy="public_context"):
                    await bot.on_message(first)
                    await self._drain()
                    second = existing.FakeMessage(channel, CORRECTION, author=first.author)
                    if boundary == "author":
                        second.author = existing.FakeAuthor(200, "Another Fictional Member")
                    elif boundary in {"channel", "thread", "guild"}:
                        second.channel = self._channel(channel.id + 1)
                        if boundary == "thread":
                            second.channel.parent_id = channel.id
                        if boundary == "guild":
                            second.channel.guild = existing.SimpleNamespace(id=98765)
                        second.guild = second.channel.guild
                    elif boundary == "expiry":
                        key = bot._conversation_state_key(first.guild.id, channel.id, first.author.id)
                        state = bot._conversation_continuation_state[key]
                        state["live_exchange_until"] -= timedelta(days=1)
                        bot._recent_direct_response_window[(channel.id, first.author.id)] -= timedelta(days=1)
                    elif boundary == "policy":
                        bot.resolve_channel_policy.return_value = "sealed_test"
                    elif boundary == "other_person":
                        other = existing.FakeAuthor(200, "Another Fictional Member")
                        second = existing.FakeMessage(channel, "<@200> " + CORRECTION, author=first.author, mentions=[other])
                    await bot.on_message(second)
                    await self._drain()
                self.assertEqual(generate.await_count, 1)
                self.assertEqual(len(channel.sent), 1)

    async def test_answer_to_bnls_question_keeps_its_existing_obligation(self):
        channel = self._channel(996250)
        first = self._first(channel)
        prompts = []

        async def generate(prompt, **_kwargs):
            prompts.append(prompt)
            return "Would you like the earlier version?" if len(prompts) == 1 else "The earlier version is selected."

        with self._completed_runtime(channel, generate):
            await bot.on_message(first)
            await self._drain()
            second = existing.FakeMessage(channel, "yes", author=first.author)
            await bot.on_message(second)
            await self._drain()
        self.assertEqual(len(prompts), 2)
        self.assertEqual(len(channel.sent), 2)
        self.assertEqual(self.frames[-1].source_message_ids, (second.id,))

    async def test_explicit_payload_owner_is_not_discarded_as_generic_ack(self):
        channel = self._channel(996251)
        bot._mark_conversation_continuation_state(
            channel.guild.id, channel.id, 100, channel_policy="sealed_test",
        )
        packet = bot._build_active_response_packet(
            channel.id, [("Fictional Member", "Thanks", 100)],
            {"payload_expected": True},
            guild_id=channel.guild.id, channel_policy="sealed_test",
        )
        self.assertEqual(packet["decision"], "answer")
        self.assertEqual(packet["reason"], "pending_request_single_payload_continuation")


if __name__ == "__main__":
    unittest.main()
