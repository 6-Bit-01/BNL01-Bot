"""Trusted Discord addressing and bounded answer windows survive batching."""

import os
import sqlite3
import unittest
from dataclasses import replace
from unittest import mock

import test_public_network_knowledge as network_fixture

bot = network_fixture.bnl01_bot


def addressing(target="none"):
    return bot.DiscordTurnAddressing(
        speaker="Test Member",
        explicit_tag_recipients=("BNL-01",) if target == "mention" else (),
        reply_target="BNL-01" if target == "reply" else "Test Guest" if target == "other" else "none",
        explicitly_mentions_bnl=target == "mention",
        reply_targets_bnl=target == "reply",
        directly_targets_bnl=target in {"mention", "reply"},
        targets_other_human=target == "other",
        plain_text_names_bnl=False,
    )


class AddressedContinuationClassificationTests(unittest.TestCase):
    def setUp(self):
        for name in ("_conversation_continuation_state", "_recent_direct_response_window"):
            patcher = mock.patch.dict(getattr(bot, name), {}, clear=True)
            patcher.start()
            self.addCleanup(patcher.stop)

    def packet(self, items, policy="public_home", guild_id=77, channel_id=9199):
        return bot._build_active_response_packet(
            channel_id, items, False, guild_id=guild_id, channel_policy=policy,
        )

    def test_trusted_mention_and_reply_do_not_depend_on_request_wording(self):
        for policy in ("public_home", "sealed_test"):
            for target in ("mention", "reply"):
                for text in ("Continue.", "Yes.", "3", "Copper skies."):
                    with self.subTest(policy=policy, target=target, text=text):
                        item = bot.BatchConversationTurn("Test Member", text, 42, addressing(target))
                        packet = self.packet([item], policy)
                        self.assertTrue(packet["response_obligation"])
                        self.assertEqual(packet["decision"], "answer")
                        decision, _reason, _diagnostics = bot._free_speak_ack_resolution(
                            packet["decision"], packet["reason"], [item], policy,
                        )
                        self.assertEqual(decision, "answer")

    def test_other_human_address_and_empty_turn_stay_excluded(self):
        other = bot.BatchConversationTurn("Test Member", "Continue.", 42, addressing("other"))
        packet = self.packet([other])
        self.assertEqual(packet["decision"], "observe")
        self.assertFalse(packet["response_obligation"])
        empty = bot.BatchConversationTurn("Test Member", " ", 42, addressing("mention"))
        self.assertEqual(self.packet([empty])["decision"], "skip")

    def test_exact_live_answer_window_accepts_short_answers_without_word_filter(self):
        bot._mark_conversation_continuation_state(77, 9199, 42, awaiting_answer=True)
        for policy in ("public_home", "sealed_test"):
            for text in ("Continue.", "Yes.", "No.", "3"):
                with self.subTest(policy=policy, text=text):
                    packet = self.packet([("Test Member", text, 42)], policy)
                    self.assertEqual(packet["decision"], "answer")
                    self.assertEqual(packet["reason"], "same_user_awaiting_answer")

    def test_answer_window_does_not_cross_speaker_room_guild_or_human_target(self):
        bot._mark_conversation_continuation_state(77, 9199, 42, awaiting_answer=True)
        scenarios = (
            ([('Test Guest', 'Continue.', 43)], 77, 9199),
            ([('Test Member', 'Continue.', 42)], 77, 9200),
            ([('Test Member', 'Continue.', 42)], 78, 9199),
            ([bot.BatchConversationTurn('Test Member', 'Continue.', 42, addressing('other'))], 77, 9199),
            ([('Test Member', 'Continue.', 42), ('Test Guest', 'Yes.', 43)], 77, 9199),
            ([('Test Member', 'Continue.', 42), ('Unknown', 'Yes.', 0)], 77, 9199),
        )
        for items, guild_id, channel_id in scenarios:
            with self.subTest(items=items, guild_id=guild_id, channel_id=channel_id):
                packet = self.packet(items, guild_id=guild_id, channel_id=channel_id)
                self.assertNotEqual(packet["decision"], "answer")

    def test_generic_recent_activity_expired_window_and_noise_do_not_gain_authority(self):
        item = [("Test Member", "Continue.", 42)]
        bot._mark_conversation_continuation_state(77, 9199, 42)
        self.assertNotEqual(self.packet(item)["decision"], "answer")
        bot._mark_conversation_continuation_state(77, 9199, 42, awaiting_answer=True)
        state = bot._conversation_continuation_state[(77, 9199, 42)]
        state["awaiting_answer_until"] = bot.datetime.now(bot.timezone.utc) - bot.timedelta(seconds=1)
        self.assertNotEqual(self.packet(item)["decision"], "answer")
        bot._mark_conversation_continuation_state(77, 9199, 42, awaiting_answer=True)
        for text in ("", " ", "...", "🎵"):
            self.assertNotEqual(self.packet([("Test Member", text, 42)])["decision"], "answer")
        self.assertNotEqual(self.packet(item, policy="public_context")["decision"], "answer")


class AddressedContinuationDeliveryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.runtime = network_fixture.PublicNetworkKnowledgeTests()
        await self.runtime.asyncSetUp()
        self.addAsyncCleanup(self.runtime.asyncTearDown)
        self.runtime._seed_finalized_show()
        self.runtime.stack.enter_context(mock.patch.dict(os.environ, {
            "BNL_CONVERSATION_ORCHESTRATION_INFLUENCE_ENABLED": "false",
            "BNL_CONVERSATION_ORCHESTRATION_SEALED_CANARY_ENABLED": "false",
        }))

    def _seed_exchange(self, channel_id, policy):
        now = bot.datetime.now(bot.timezone.utc)
        row_id = channel_id * 10
        reply_message_id = row_id * 10 + 2
        channel_name = "bnl-testing" if policy == "sealed_test" else "barcode-bot"
        with sqlite3.connect(bot.DB_FILE) as conn:
            conn.executemany(
                """INSERT INTO conversations
                (id,user_id,user_name,guild_id,channel_name,channel_policy,
                 route_mode,role,content,timestamp,channel_id,message_id)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                [
                    (row_id, 42, "Test Member", 77, channel_name, policy, "normal_chat", "user",
                     "Give me a recap of the 2026-08-28 show.",
                     (now - bot.timedelta(minutes=2)).isoformat(), channel_id, reply_message_id - 1),
                    (row_id + 1, 42, "BNL-01", 77, channel_name, policy, "normal_chat", "model",
                     "Would you like another comment from that show?",
                     (now - bot.timedelta(minutes=1)).isoformat(), channel_id, reply_message_id),
                ],
            )
            conn.executemany(
                """INSERT INTO conversation_discord_message_links
                (conversation_row_id,guild_id,channel_id,message_id) VALUES (?,?,?,?)""",
                [(row_id, 77, channel_id, reply_message_id - 1),
                 (row_id + 1, 77, channel_id, reply_message_id)],
            )
        return reply_message_id

    def _next_answer_window(self, policy):
        channel_id = 8811 + len(self.runtime.channel_ids)
        self._seed_exchange(channel_id, policy)
        bot._mark_conversation_continuation_state(77, channel_id, 42, awaiting_answer=True)
        return channel_id, bot._conversation_continuation_state[(77, channel_id, 42)]

    def _followup_packet(self, channel_id, policy, text="Thanks."):
        return bot._build_active_response_packet(
            channel_id, [("Test Member", text, 42)], False,
            guild_id=77, channel_policy=policy,
        )

    async def test_successful_nonquestion_consumes_answer_window_before_next_short_turn(self):
        for policy in ("public_home", "sealed_test"):
            with self.subTest(policy=policy):
                channel_id, state = self._next_answer_window(policy)
                answer = "Let's continue with the recap."
                channel, generation, guard = await self.runtime._batch(
                    policy, request="Continue.", answer=answer,
                )
                generation.assert_awaited_once()
                guard.assert_awaited_once()
                self.assertEqual(channel.sent, [answer])
                self.assertNotIn("awaiting_answer_until", state)
                # A normal recent exchange remains available to substantive
                # follow-ups, but the answered question cannot force another
                # response to a low-signal acknowledgement.
                self.assertTrue(bot._is_recent_conversation_continuation(77, channel_id, 42))
                packet = self._followup_packet(channel_id, policy)
                self.assertNotEqual(packet["decision"], "answer")
                self.assertNotEqual(packet["reason"], "same_user_awaiting_answer")

    async def test_fresh_delivered_question_refreshes_window_and_survives_retransmission_mark(self):
        for policy in ("public_home", "sealed_test"):
            for requests_retransmission in (False, True):
                with self.subTest(policy=policy, retransmission=requests_retransmission):
                    channel_id, state = self._next_answer_window(policy)
                    old_deadline = bot.datetime.now(bot.timezone.utc) + bot.timedelta(seconds=5)
                    state["awaiting_answer_until"] = old_deadline
                    answer = (
                        "The recap covers that show's visuals. Could you send it again?"
                        if requests_retransmission
                        else "The recap covers that show's visuals. Would you like another comment?"
                    )
                    channel, generation, guard = await self.runtime._batch(
                        policy, request="Continue.", answer=answer,
                    )
                    generation.assert_awaited_once()
                    guard.assert_awaited_once()
                    self.assertEqual(channel.sent, [answer])
                    self.assertGreater(state["awaiting_answer_until"], old_deadline)
                    self.assertEqual("awaiting_retransmission_until" in state, requests_retransmission)
                    packet = self._followup_packet(channel_id, policy, "Yes.")
                    self.assertEqual(packet["decision"], "answer")
                    self.assertEqual(packet["reason"], "same_user_awaiting_answer")

    async def test_failed_discord_send_preserves_answer_window_for_retry(self):
        for policy in ("public_home", "sealed_test"):
            with self.subTest(policy=policy):
                channel_id, state = self._next_answer_window(policy)
                old_deadline = state["awaiting_answer_until"]
                with mock.patch.object(
                    network_fixture.FakeChannel, "send",
                    new=mock.AsyncMock(side_effect=RuntimeError("test transport unavailable")),
                ) as send:
                    channel, generation, guard = await self.runtime._batch(
                        policy, request="Continue.", answer="Let's continue with the recap.",
                    )
                generation.assert_awaited_once()
                guard.assert_awaited_once()
                send.assert_awaited_once()
                self.assertEqual(channel.sent, [])
                self.assertEqual(state["awaiting_answer_until"], old_deadline)
                packet = self._followup_packet(channel_id, policy, "Continue.")
                self.assertEqual(packet["decision"], "answer")
                self.assertEqual(packet["reason"], "same_user_awaiting_answer")

    async def test_successful_no_store_delivery_only_consumes_existing_question_state(self):
        for policy in ("public_home", "sealed_test"):
            for has_existing_window in (False, True):
                with self.subTest(policy=policy, existing_window=has_existing_window):
                    channel_id = 8811 + len(self.runtime.channel_ids)
                    if has_existing_window:
                        channel_id, state = self._next_answer_window(policy)
                        previous_state = dict(state)
                        participants = (("Test Member", "Continue.", 42),)
                    else:
                        self._seed_exchange(channel_id, policy)
                        participants = (
                            bot.BatchConversationTurn("Test Member", "Continue.", 42, addressing("mention")),
                        )
                    answer = "The recap covers that show's visuals. Would you like another comment?"
                    with (
                        mock.patch.object(bot, "model_response_persistence_allowed_with_website_context", return_value=False),
                        mock.patch.object(bot, "save_model_message") as save,
                    ):
                        channel, generation, guard = await self.runtime._batch(
                            policy, request="Continue.", answer=answer, participants=participants,
                        )
                    generation.assert_awaited_once()
                    guard.assert_awaited_once()
                    self.assertEqual(channel.sent, [answer])
                    save.assert_not_called()
                    if has_existing_window:
                        previous_state.pop("awaiting_answer_until")
                        self.assertEqual(state, previous_state)
                    else:
                        self.assertNotIn((77, channel_id, 42), bot._conversation_continuation_state)
                    packet = self._followup_packet(channel_id, policy)
                    self.assertNotEqual(packet["decision"], "answer")
                    self.assertNotEqual(packet["reason"], "same_user_awaiting_answer")

    async def test_real_bare_continue_reaches_provider_with_its_existing_source_scope(self):
        # The fixture checks source handoff and delivery, not live semantic
        # quality. In particular, exact reply targets must not be broadened to
        # a different source merely to satisfy an inherited-show assertion.
        answer = "Let's continue with the recap."
        for policy in ("public_home", "sealed_test"):
            for target in ("mention", "reply", "awaiting_answer"):
                with self.subTest(policy=policy, target=target):
                    channel_id = 8811 + len(self.runtime.channel_ids)
                    reply_message_id = self._seed_exchange(channel_id, policy)
                    if target == "awaiting_answer":
                        bot._mark_conversation_continuation_state(77, channel_id, 42, awaiting_answer=True)
                        participants = (("Test Member", "Continue.", 42),)
                    else:
                        meta = addressing(target)
                        if target == "reply":
                            meta = replace(meta, reply_message_id=reply_message_id)
                        participants = (bot.BatchConversationTurn("Test Member", "Continue.", 42, meta),)
                    channel, generation, guard = await self.runtime._batch(
                        policy, request="Continue.", answer=answer, participants=participants,
                    )
                    generation.assert_awaited_once()
                    guard.assert_awaited_once()
                    self.assertEqual(channel.sent, [answer])
                    prompt = generation.await_args.args[0]
                    if target == "reply":
                        self.assertIn("BNL-01 (exact Discord reply source):", prompt)
                        self.assertIn("Would you like another comment from that show?", prompt)
                    else:
                        self.assertIn("Give me a recap of the 2026-08-28 show.", prompt)
                        self.assertIn("the green visuals during this song are wild.", prompt)


if __name__ == "__main__":
    unittest.main()
