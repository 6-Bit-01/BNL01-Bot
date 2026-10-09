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
# These are labelled provider outcomes for routing integration, not a lexical
# substitute for the semantic classifier. Provider quality is checked separately.
FOLLOWUP_DECISIONS = {
    CORRECTION: True,
    "Could you explain what you mean by those earlier recordings?": True,
    "The recordings by that musician were captured at a different show.": True,
    "He only gives us the buttons we are allowed to press": True,
    "The second one.": True,
    "Which of those did Test Submitter submit, and when was that show?": True,
    "yes": True,
    "BNL, can you help me choose a replacement bicycle tyre?": True,
    "Did you find those earlier recordings?": True,
    "I am making mushroom pasta for dinner tonight.": False,
    "Does anyone know where to buy a replacement bicycle tyre?": False,
    "Unrelated question: which keyboard should I buy?": False,
    "My bicycle needs new brakes before the weekend.": False,
    "What is everyone cooking for dinner?": False,
    "Has anyone watched the new cartoon series?": False,
    "Does anyone know a good bicycle repair shop?": False,
    "Does anyone have a good mushroom pasta recipe?": False,
    "BNL mentioned those earlier recordings in the archive.": False,
    "I told BNL that the earlier recordings were missing.": False,
    "thanks": False,
    "nice": False,
    "👍": False,
}


class CompletedConversationContinuationTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = ingress.OrdinaryAddressedBurstIngressTests.asyncSetUp
    asyncTearDown = ingress.OrdinaryAddressedBurstIngressTests.asyncTearDown
    _on_message_runtime = ingress.OrdinaryAddressedBurstIngressTests._on_message_runtime
    _flush_runtime = ingress.OrdinaryAddressedBurstIngressTests._flush_runtime
    _runtime = ingress.OrdinaryAddressedBurstIngressTests._runtime
    _drain = ingress.OrdinaryAddressedBurstIngressTests._drain
    _assert_originals_once = ingress.OrdinaryAddressedBurstIngressTests._assert_originals_once

    def _channel(self, channel_id, **kwargs):
        channel = ingress.OrdinaryAddressedBurstIngressTests._channel(self, channel_id, **kwargs)
        original_send = channel.send
        channel.completed_request_fixtures = {}
        channel.completed_reply_fixtures = {}
        reply_count = 0

        def reply_receipt(text):
            nonlocal reply_count
            reply_count += 1
            receipt = existing.SimpleNamespace(id=channel_id * 100 + reply_count)
            channel.completed_reply_fixtures[receipt.id] = text
            return receipt

        channel.completed_reply_receipt = reply_receipt

        async def send_with_receipt(text, **send_kwargs):
            await original_send(text, **send_kwargs)
            return reply_receipt(text)

        channel.send = mock.AsyncMock(side_effect=send_with_receipt)
        return channel

    def _completed_runtime(self, channel, generate, *, policy="sealed_test"):
        actual_direct = bot._is_recent_direct_followup
        actual_continuation = bot._is_recent_conversation_continuation
        stack = ExitStack()
        stack.enter_context(self._runtime(channel, generate, policy=policy))
        stack.enter_context(mock.patch.object(bot, "_is_recent_direct_followup", side_effect=actual_direct))
        stack.enter_context(mock.patch.object(bot, "_is_recent_conversation_continuation", side_effect=actual_continuation))
        stack.enter_context(mock.patch.object(bot, "BATCH_WINDOW_SECONDS", 0.01))
        stack.enter_context(mock.patch.object(bot, "BATCH_REPLY_COOLDOWN_SECONDS", 0))

        async def load_exchange(message, state, *, result_out=None):
            channel.completed_request_fixtures[message.id] = message
            request_ids = tuple(state.get("request_lineage_message_ids") or state.get("request_message_ids") or ())
            reply_ids = tuple(state.get("reply_lineage_message_ids") or state.get("reply_message_ids") or ())
            if not request_ids or not reply_ids:
                return None
            requests = tuple(channel.completed_request_fixtures.get(mid) for mid in request_ids)
            replies = tuple(channel.completed_reply_fixtures.get(mid) for mid in reply_ids)
            if any(source is None for source in requests) or any(text is None for text in replies):
                return None
            request_texts = tuple(
                bot.append_media_context_to_text(
                    bot.resolve_discord_user_mentions_for_conversation(
                        source, source.content, bot_user_id=999, remove_bot_mention=True,
                    ),
                    bot.build_message_media_context(source),
                )
                for source in requests
            )
            if result_out is not None:
                result_out["exchange"] = bot.CompletedConversationExchange(
                    guild_id=message.guild.id, channel_id=message.channel.id,
                    user_id=message.author.id, channel_policy=state.get("channel_policy", "unknown"),
                    request_message_ids=request_ids, request_texts=request_texts,
                    reply_message_ids=reply_ids, reply_texts=replies,
                    revision=bot._completed_exchange_revision(state),
                    request_source_digests=tuple(bot._prompt_source_digest(source.content) for source in requests),
                    reply_source_digests=tuple(bot._prompt_source_digest(text) for text in replies),
                    unsaved_reply_message_ids=(tuple(state["no_store_reply_message_ids"])
                                              if "no_store_reply_message_ids" in state else None),
                )
            return {"previous_user": "\n".join(request_texts)[-4000:],
                    "bnl_reply": "\n".join(replies)[-4000:]}

        async def classify_exchange(exchange, content):
            self.assertTrue(exchange["bnl_reply"])
            self.assertIn(content, FOLLOWUP_DECISIONS, "Add an explicit semantic fixture label")
            return FOLLOWUP_DECISIONS[content]

        self.exchange_loader = mock.AsyncMock(side_effect=load_exchange)
        self.followup_classifier = mock.AsyncMock(side_effect=classify_exchange)
        stack.enter_context(mock.patch.object(bot, "_load_completed_followup_exchange",
            new=self.exchange_loader, create=True))
        stack.enter_context(mock.patch.object(bot, "_classify_completed_followup_exchange",
            new=self.followup_classifier, create=True))
        # This routing fixture supplies Discord snapshots and substitutes Context;
        # the real gateway suite covers original-row and privacy validation.
        stack.enter_context(mock.patch.object(bot, "_completed_exchange_originals_available", return_value=True))
        if getattr(self, "no_store_context", ""):
            stack.enter_context(mock.patch.object(bot, "maybe_build_bnl_read_model_context",
                return_value=self.no_store_context))
        return stack

    def _first(self, channel):
        mention = existing.SimpleNamespace(id=999, display_name="BNL-01", bot=True)
        first = existing.FakeMessage(channel, "<@999> " + FIRST, mentions=[mention])
        channel.completed_request_fixture = first
        channel.completed_request_fixtures[first.id] = first
        original_reply = first.reply

        async def reply_with_receipt(text, **reply_kwargs):
            await original_reply(text, **reply_kwargs)
            return channel.completed_reply_receipt(text)

        first.reply = reply_with_receipt
        return first

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

    async def test_unrelated_public_messages_do_not_inherit_completed_addressing(self):
        for offset, text in enumerate((
            "I am making mushroom pasta for dinner tonight.",
            "Does anyone know where to buy a replacement bicycle tyre?",
            "Unrelated question: which keyboard should I buy?",
        )):
            with self.subTest(text=text):
                channel = self._channel(996300 + offset)
                first = self._first(channel)
                second = existing.FakeMessage(channel, text, author=first.author)
                generate = mock.AsyncMock(return_value="The earlier recordings are listed in the archive.")
                with self._completed_runtime(channel, generate, policy="public_context"):
                    await bot.on_message(first)
                    await self._drain()
                    self.assertEqual(generate.await_count, 1)
                    # Exercise the immediate legacy follow-up window as well as
                    # the completed-exchange state; neither proves relevance.
                    await bot.on_message(second)
                    await self._drain()
                self.assertEqual(generate.await_count, 1)
                self.assertEqual(len(channel.sent), 1)
                self.assertEqual(second.replies, [])

    async def test_outstanding_question_accepts_its_answer_but_not_unrelated_chatter(self):
        channel = self._channel(996310)
        first = self._first(channel)
        unrelated = existing.FakeMessage(channel,
            "My bicycle needs new brakes before the weekend.", author=first.author)
        answer = existing.FakeMessage(channel, "yes", author=first.author)
        later = existing.FakeMessage(channel,
            "What is everyone cooking for dinner?", author=first.author)
        generate = mock.AsyncMock(side_effect=(
            "Would you like the earlier version?",
            "The earlier version is selected.",
        ))
        with self._completed_runtime(channel, generate, policy="public_context"):
            await bot.on_message(first)
            await self._drain()
            await bot.on_message(unrelated)
            await self._drain()
            self.assertEqual(generate.await_count, 1)
            self.assertEqual(len(channel.sent), 1)
            await bot.on_message(answer)
            await self._drain()
            self.assertEqual(generate.await_count, 2)
            self.assertEqual(len(channel.sent), 2)
            await bot.on_message(later)
            await self._drain()
        self.assertEqual(generate.await_count, 2)
        self.assertEqual(len(channel.sent), 2)
        self.assertEqual(self.frames[-1].source_message_ids, (answer.id,))

    async def test_direct_mention_or_reply_can_change_the_completed_topic(self):
        for offset, target in enumerate(("mention", "reply")):
            with self.subTest(target=target):
                channel = self._channel(996320 + offset)
                first = self._first(channel)
                bnl = existing.SimpleNamespace(id=999, display_name="BNL-01", bot=True)
                text = "Can you help me choose a replacement bicycle tyre?"
                second = existing.FakeMessage(channel,
                    "<@999> " + text if target == "mention" else text,
                    author=first.author, mentions=[bnl] if target == "mention" else [])
                if target == "reply":
                    prior = existing.FakeMessage(channel,
                        "The earlier recordings are listed in the archive.", author=bnl)
                    second.reference = existing.SimpleNamespace(message_id=prior.id, resolved=prior)
                generate = mock.AsyncMock(return_value="The neutral request has been answered.")
                with self._completed_runtime(channel, generate, policy="public_context"):
                    await bot.on_message(first)
                    await self._drain()
                    await bot.on_message(second)
                    await self._drain()
                self.assertEqual(generate.await_count, 2)
                self.assertEqual(len(first.replies + second.replies + channel.sent), 2)
                self.assertEqual(self.frames[-1].source_message_ids, (second.id,))

    async def test_permissions_continuation_responds_but_room_wide_topic_change_does_not(self):
        for offset, policy in enumerate(("public_context", "public_selective")):
            with self.subTest(policy=policy):
                channel = self._channel(996370 + offset)
                first = self._first(channel)
                first.content = "<@999> Can you change channel permissions for us?"
                followup = existing.FakeMessage(channel,
                    "He only gives us the buttons we are allowed to press", author=first.author)
                generate = mock.AsyncMock(side_effect=(
                    "I cannot change channel permissions; access is controlled by the server.",
                    "Your access follows the permissions granted by the server.",
                ))
                with self._completed_runtime(channel, generate, policy=policy), mock.patch.object(
                    bot, "get_guild_config", return_value=channel.id + 1000,
                ):
                    await bot.on_message(first)
                    await self._drain()
                    self.assertEqual(generate.await_count, 1)
                    await bot.on_message(followup)
                    await self._drain()
                    self.assertEqual(generate.await_count, 2)
                    self.assertEqual(len(channel.sent), 2)
                    self.assertEqual(self.frames[-1].source_message_ids, (followup.id,))
                    for text in (
                        "Has anyone watched the new cartoon series?",
                        "Does anyone know a good bicycle repair shop?",
                    ):
                        unrelated = existing.FakeMessage(channel, text, author=first.author)
                        await bot.on_message(unrelated)
                        await self._drain()
                        self.assertEqual(generate.await_count, 2)
                        self.assertEqual(unrelated.replies, [])
                self.assertEqual(len(channel.sent), 2)

    async def test_choice_question_accepts_an_ordinal_answer(self):
        channel = self._channel(996380)
        first = self._first(channel)
        first.content = "<@999> Can you help me choose a version of this recording?"
        answer = existing.FakeMessage(channel, "The second one.", author=first.author)
        generate = mock.AsyncMock(side_effect=(
            "Do you prefer the acoustic or electric version?",
            "The electric version is selected.",
        ))
        with self._completed_runtime(channel, generate, policy="public_context"):
            await bot.on_message(first)
            await self._drain()
            await bot.on_message(answer)
            await self._drain()
        self.assertEqual(generate.await_count, 2)
        self.assertEqual(len(channel.sent), 2)
        self.assertEqual(self.frames[-1].source_message_ids, (answer.id,))

    async def test_contextual_public_followups_remain_addressed(self):
        for offset, text in enumerate((
            "Could you explain what you mean by those earlier recordings?",
            CORRECTION,
            "The recordings by that musician were captured at a different show.",
        )):
            with self.subTest(text=text):
                channel = self._channel(996330 + offset)
                first = self._first(channel)
                second = existing.FakeMessage(channel, text, author=first.author)
                generate = mock.AsyncMock(return_value="The earlier recordings are listed in the archive.")
                with self._completed_runtime(channel, generate, policy="public_context"):
                    await bot.on_message(first)
                    await self._drain()
                    await bot.on_message(second)
                    await self._drain()
                self.assertEqual(generate.await_count, 2)
                self.assertEqual(len(channel.sent), 2)
                self.assertEqual(self.frames[-1].source_message_ids, (second.id,))
                self.assertEqual(self.frames[-1].explicit_mention_count, 0)

    async def test_contextual_message_targeting_another_human_remains_quiet(self):
        for offset, target in enumerate(("mention", "reply")):
            with self.subTest(target=target):
                channel = self._channel(996340 + offset)
                first = self._first(channel)
                other = existing.FakeAuthor(200, "Another Fictional Member")
                text = "Did you find those earlier recordings?"
                second = existing.FakeMessage(channel,
                    "<@200> " + text if target == "mention" else text,
                    author=first.author, mentions=[other] if target == "mention" else [])
                if target == "reply":
                    prior = existing.FakeMessage(channel, "I can look for those recordings.", author=other)
                    second.reference = existing.SimpleNamespace(message_id=prior.id, resolved=prior)
                generate = mock.AsyncMock(return_value="The earlier recordings are listed in the archive.")
                with self._completed_runtime(channel, generate, policy="public_context"):
                    await bot.on_message(first)
                    await self._drain()
                    await bot.on_message(second)
                    await self._drain()
                self.assertEqual(generate.await_count, 1)
                self.assertEqual(len(channel.sent), 1)
                self.assertEqual(second.replies, [])

    async def test_canonical_bnl_salutation_is_distinct_from_third_person_mention(self):
        cases = (
            ("BNL, can you help me choose a replacement bicycle tyre?", True),
            ("BNL mentioned those earlier recordings in the archive.", False),
            ("I told BNL that the earlier recordings were missing.", False),
        )
        for offset, (text, should_reply) in enumerate(cases):
            with self.subTest(text=text):
                channel = self._channel(996350 + offset)
                first = self._first(channel)
                second = existing.FakeMessage(channel, text, author=first.author)
                generate = mock.AsyncMock(return_value="The earlier recordings are listed in the archive.")
                with self._completed_runtime(channel, generate, policy="public_context"):
                    await bot.on_message(first)
                    await self._drain()
                    await bot.on_message(second)
                    await self._drain()
                expected = 2 if should_reply else 1
                self.assertEqual(generate.await_count, expected)
                self.assertEqual(len(first.replies + second.replies + channel.sent), expected)

    async def test_batching_disabled_still_requires_relevant_public_followup(self):
        for offset, (text, should_reply) in enumerate((
            ("Does anyone have a good mushroom pasta recipe?", False),
            (CORRECTION, True),
        )):
            with self.subTest(text=text):
                channel = self._channel(996360 + offset)
                first = self._first(channel)
                second = existing.FakeMessage(channel, text, author=first.author)
                generate = mock.AsyncMock(return_value="The earlier recordings are listed in the archive.")
                with self._completed_runtime(channel, generate, policy="public_context"), mock.patch.object(
                    bot, "BNL_ACTIVE_BATCHING_ENABLED", False,
                ):
                    await bot.on_message(first)
                    await self._drain()
                    self.assertEqual(generate.await_count, 1)
                    await bot.on_message(second)
                    await self._drain()
                expected = 2 if should_reply else 1
                self.assertEqual(generate.await_count, expected)
                self.assertEqual(len(first.replies + second.replies + channel.sent), expected)

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
            self.assertNotIn("neutral", str(state).lower())
            self.assertNotIn(FIRST, str(state))
            self.assertNotIn("The neutral record contains two entries.", str(state))
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


class CompletedFollowupClassifierTests(unittest.IsolatedAsyncioTestCase):
    def test_request_timeout_meets_provider_minimum_without_sdk_retries(self):
        config = bot._generation_config_for_model("gemini-3.6-flash", "conversation_followup_addressing")
        self.assertEqual(config.http_options.timeout, 10000)
        self.assertEqual(config.http_options.retry_options.attempts, 1)
        self.assertEqual(config.max_output_tokens, 1024)
        self.assertEqual(config.response_mime_type, "application/json")

    async def test_classifier_accepts_only_the_exact_true_boolean_contract(self):
        exchange = {"previous_user": FIRST, "bnl_reply": "The earlier recordings are listed."}
        for text, expected in (
            ('{"continue": true}', True),
            ('{"continue": false}', False),
            ('{"continue": "true"}', False),
            ('{"continue": 1}', False),
            ('{"continue": true, "reason": "same topic"}', False),
            ('[true]', False),
            ('Yes, continue.', False),
            ('```json\n{"continue": true}\n```', False),
        ):
            with self.subTest(text=text), mock.patch.object(
                bot, "_generate_gemini_content_result_async",
                new=mock.AsyncMock(return_value=bot.GenerationResult(True, text)),
            ) as provider:
                self.assertIs(await bot._classify_completed_followup_exchange(exchange, CORRECTION), expected)
                provider.assert_awaited_once()
                self.assertEqual(provider.await_args.args[1], "conversation_followup_addressing")

    async def test_classifier_stays_quiet_when_budget_or_provider_is_unavailable(self):
        exchange = {"previous_user": FIRST, "bnl_reply": "The earlier recordings are listed."}
        for category in (
            bot.GENERATION_ERROR_LOCAL_MODEL_BUDGET,
            bot.GENERATION_ERROR_PROVIDER_TIMEOUT,
        ):
            with self.subTest(category=category), mock.patch.object(
                bot, "_generate_gemini_content_result_async",
                new=mock.AsyncMock(return_value=bot.GenerationResult(
                    False, '{"continue": true}', error_category=category,
                )),
            ) as provider:
                self.assertFalse(await bot._classify_completed_followup_exchange(exchange, CORRECTION))
                provider.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
