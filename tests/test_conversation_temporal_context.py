"""Clock/calendar grounding through real conversation and packet assembly.

Provider replies are transport fixtures. These tests verify what Gemini is
given, not the quality of a live model's answer or any production show state.
"""
import os
import unittest
from datetime import datetime, timezone
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl01_bot as bot
import test_public_network_knowledge as network_fixture


class ConversationCalendarTests(unittest.TestCase):
    def test_utc_date_rollover_uses_pacific_date_and_weekday(self):
        context = bot.get_temporal_context(datetime(2026, 9, 26, 6, 30, tzinfo=timezone.utc))
        self.assertEqual(context["local_date"], "2026-09-25")
        self.assertEqual(context["weekday"], "Friday")
        self.assertEqual(context["now_iso"], "2026-09-25T23:30:00-07:00")
        self.assertTrue(context["is_regular_show_day"])
        self.assertEqual(context["next_regular_show_at"], "2026-10-02T19:00:00-07:00")

    def test_intake_does_not_roll_next_start_to_the_following_week(self):
        context = bot.get_temporal_context(datetime(2026, 9, 25, 18, 50))
        self.assertEqual(context["next_regular_intake_at"], "2026-09-25T18:40:00-07:00")
        self.assertEqual(context["next_regular_show_at"], "2026-09-25T19:00:00-07:00")
        self.assertEqual(context["next_regular_first_track_target_at"], "2026-09-25T19:05:00-07:00")
        self.assertEqual(context["show_phase"], "show_day")

    def test_next_start_boundary_and_year_rollover(self):
        for now, expected in (
            (datetime(2026, 12, 25, 18, 59), "2026-12-25T19:00:00-08:00"),
            (datetime(2026, 12, 25, 19), "2027-01-01T19:00:00-08:00"),
            (datetime(2026, 12, 31, 23, 59), "2027-01-01T19:00:00-08:00"),
        ):
            with self.subTest(now=now):
                self.assertEqual(bot.get_temporal_context(now)["next_regular_show_at"], expected)

    def test_next_friday_is_localized_for_its_own_dst_offset(self):
        for now, expected in (
            (datetime(2026, 3, 6, 20), "2026-03-13T19:00:00-07:00"),
            (datetime(2026, 10, 30, 20), "2026-11-06T19:00:00-08:00"),
        ):
            with self.subTest(now=now):
                self.assertEqual(bot.get_temporal_context(now)["next_regular_show_at"], expected)

    def test_friday_clock_never_asserts_actual_live_or_end_state(self):
        for now in (datetime(2026, 9, 25, 18, 40), datetime(2026, 9, 25, 23, 59), datetime(2026, 9, 26, 0, 30)):
            with self.subTest(now=now):
                context = bot.get_temporal_context(now)
                self.assertNotIn(context["show_phase"], ("live_now", "post_show"))
                block = bot.render_conversation_temporal_context(context)
                self.assertIn("does not establish live, ended, intake-open or playback state", block)
                self.assertIn("fresh authorized operational evidence", block)
                self.assertIn("Supplied scheduling changes take precedence", block)

    def test_calendar_reuses_existing_occasion_owner_without_claiming_publication(self):
        context = bot.get_temporal_context(datetime(2026, 12, 25, 9))
        self.assertEqual(context["occasions"], tuple(item.name for item in bot.calendar_occasions_on(datetime(2026, 12, 25).date())))
        self.assertTrue(context["occasions"])
        block = bot.render_conversation_temporal_context(context)
        self.assertIn(context["occasions"][0], block)
        self.assertIn("does not establish that an occasion post was published", block)


class ConversationClockDeliveryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.runtime = network_fixture.PublicNetworkKnowledgeTests()
        await self.runtime.asyncSetUp()
        self.addAsyncCleanup(self.runtime.asyncTearDown)
        self.now = datetime(2026, 9, 25, 18, 50)
        real_clock = bot.get_temporal_context
        self.runtime.stack.enter_context(mock.patch.object(
            bot, "get_temporal_context", side_effect=lambda: real_clock(self.now),
        ))
        self.fetch = self.runtime.stack.enter_context(mock.patch.object(
            bot, "fetch_bnl_read_model", return_value={},
        ))

    def _packet_flags(self, enabled, channel_id=8810):
        return mock.patch.dict(os.environ, {
            "BNL_MEMORY_LEDGER_SHADOW_ENABLED": "true",
            "BNL_MOMENT_ENGINE_SHADOW_ENABLED": "true",
            "BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED": "true",
            "BNL_RELATIONSHIP_V2_SHADOW_ENABLED": "true",
            "BNL_UNIFIED_RESPONSE_ASSESSMENT_SHADOW_ENABLED": "true",
            "BNL_UNIFIED_INTELLIGENCE_PACKET_SHADOW_ENABLED": "true",
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_ENABLED": str(enabled).lower(),
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_PUBLIC_ENABLED": str(enabled).lower(),
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_GUILD_IDS": str(self.runtime.guild_id),
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_USER_IDS": str(self.runtime.user_id),
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_CHANNEL_IDS": str(channel_id),
        })

    def _assert_clock(self, prompt, expected="2026-09-25T18:50:00-07:00"):
        self.assertEqual(prompt.count("Current network time:"), 1)
        self.assertIn(expected, prompt)
        self.assertIn("event time, recorded time and publication time distinct", prompt)
        self.assertIn("Keep the selected historical episode", prompt)
        self.assertNotIn("current show phase supports", prompt)

    async def test_direct_and_packet_prompts_receive_clock_without_website_access(self):
        for enabled, policy in ((False, "public_home"), (True, "public_home"), (False, "sealed_test"), (True, "sealed_test")):
            with self.subTest(packet=enabled, policy=policy), self._packet_flags(enabled):
                prompt, metadata = await self.runtime._direct_prompt_async(
                    policy, "What day is it, and when is the next BARCODE Radio show?",
                )
                self._assert_clock(prompt)
                if enabled:
                    basis = metadata["ordinary_chat_single_packet_basis"]
                    self.assertIsNotNone(basis)
                    assembled = bot.build_packet_owned_prompt(prompt, basis)
                    self.assertTrue(assembled.ready)
                    self._assert_clock(assembled.prompt)
                self.fetch.assert_not_called()

    async def test_batch_delivers_once_with_clock_in_both_generation_routes(self):
        async def answer(*_args, **kwargs):
            if kwargs.get("attempt_counter") is not None:
                kwargs["attempt_counter"].mark_started()
            return "It is Friday. The regular show starts at 7 PM Pacific."

        for enabled, policy in ((False, "public_home"), (True, "public_home"), (False, "sealed_test"), (True, "sealed_test")):
            channel_id = 8811 + len(self.runtime.channel_ids)
            with self.subTest(packet=enabled, policy=policy), self._packet_flags(enabled, channel_id):
                channel, generation, _guard = await self.runtime._batch(
                    policy, "What day is it, and when is the next BARCODE Radio show?",
                    answer, privileged=False,
                )
                generation.assert_awaited_once()
                self._assert_clock(generation.await_args.args[0])
                self.assertEqual(generation.await_args.kwargs["route"], bot.ORDINARY_CHAT_SINGLE_PACKET_ROUTE if enabled else "get_gemini_response")
                self.assertEqual(len(channel.sent), 1)

    async def test_each_turn_gets_fresh_clock_while_historical_sources_keep_their_dates(self):
        self.runtime._seed_finalized_show()
        self.runtime.stack.enter_context(mock.patch.object(
            bot, "build_tiktok_show_evidence_context_for_turn", new=network_fixture.REAL_SHOW_CONTEXT_FOR_TURN,
        ))
        self.now = datetime(2026, 9, 25, 23, 59)
        first, _ = await self.runtime._direct_prompt_async("sealed_test", "Recap the August 28, 2026 BARCODE Radio show.")
        self._assert_clock(first, "2026-09-25T23:59:00-07:00")
        self.now = datetime(2026, 9, 26, 0, 1)
        second, _ = await self.runtime._direct_prompt_async("sealed_test", "Recap the August 28, 2026 BARCODE Radio show.")
        self._assert_clock(second, "2026-09-26T00:01:00-07:00")
        self.assertNotIn("2026-09-25T23:59:00-07:00", second)
        for prompt in (first, second):
            self.assertIn("2026-08-28", prompt)
            self.assertIn("the green visuals during this song are wild.", prompt)


if __name__ == "__main__":
    unittest.main()
