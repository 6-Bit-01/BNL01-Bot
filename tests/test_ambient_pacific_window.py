"""Automatic Ambient delivery observes Pacific quiet hours at the send boundary."""
from contextlib import closing
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
import sqlite3
import unittest
from unittest import mock

import test_ambient_community_edition as editions
import test_dormant_signal_echo as echoes
import test_occasion_publishing as occasions

bot = editions.bot


def pacific(year=2026, month=9, day=11, hour=8, minute=0, second=0):
    return bot.PACIFIC_TZ.localize(datetime(year, month, day, hour, minute, second))


class AmbientWindowClockTests(unittest.TestCase):
    def test_window_includes_eight_am_and_excludes_eight_pm(self):
        for hour, minute, second, allowed in (
            (7, 59, 59, False), (8, 0, 0, True),
            (19, 59, 59, True), (20, 0, 0, False), (23, 59, 59, False),
        ):
            with self.subTest(hour=hour, minute=minute, second=second):
                now = pacific(hour=hour, minute=minute, second=second)
                self.assertEqual(bot.ambient_posting_window_open(now), allowed)

    def test_naive_values_are_pacific_and_aware_values_are_converted(self):
        self.assertTrue(bot.ambient_posting_window_open(datetime(2026, 7, 4, 8)))
        self.assertFalse(bot.ambient_posting_window_open(datetime(2026, 7, 4, 20)))
        for local in (pacific(hour=7), pacific(hour=8), pacific(hour=19), pacific(hour=20)):
            with self.subTest(local=local):
                self.assertEqual(bot.ambient_posting_window_open(local.astimezone(timezone.utc)),
                                 8 <= local.hour < 20)

    def test_next_opening_is_today_before_eight_and_tomorrow_after_close(self):
        for now, expected in (
            (pacific(hour=7, minute=59), pacific(hour=8)),
            (pacific(hour=8), pacific(hour=8)),
            (pacific(hour=14, minute=22), pacific(hour=14, minute=22)),
            (pacific(hour=20), pacific(day=12, hour=8)),
        ):
            with self.subTest(now=now):
                self.assertEqual(bot.next_ambient_window_time(now), expected)
        self.assertEqual(bot.next_ambient_window_time(datetime(2026, 9, 11, 7)), pacific(hour=8))
        self.assertEqual(bot.next_ambient_window_time(pacific(hour=20).astimezone(timezone.utc)),
                         pacific(day=12, hour=8))

    def test_next_opening_localizes_each_date_across_both_dst_changes(self):
        for month, day, next_day, expected_offset in ((3, 7, 8, -7), (10, 31, 1, -8)):
            with self.subTest(month=month):
                now = pacific(month=month, day=day, hour=20)
                expected_month = 11 if month == 10 else month
                expected = pacific(month=expected_month, day=next_day, hour=8)
                actual = bot.next_ambient_window_time(now)
                self.assertEqual(actual, expected)
                self.assertEqual(actual.hour, 8)
                self.assertEqual(actual.utcoffset(), timedelta(hours=expected_offset))

    def test_default_clock_is_read_at_each_check(self):
        clock = mock.Mock(wraps=datetime)
        clock.now.side_effect = [pacific(hour=19, minute=59, second=59), pacific(hour=20)]
        with mock.patch.object(bot, "datetime", clock):
            self.assertTrue(bot.ambient_posting_window_open())
            self.assertFalse(bot.ambient_posting_window_open())

    def test_all_existing_scheduling_paths_keep_next_opportunity_inside_window(self):
        for path, now, expected_time in (
            ("initial", pacific(hour=19, minute=59), (8, 0)),
            ("failure_retry", pacific(hour=19, minute=59), (8, 0)),
            ("next_day_random_max", pacific(hour=19, minute=59), (19, 59)),
            ("optional_second", pacific(hour=16), None),
        ):
            with self.subTest(path=path):
                clock = mock.Mock(wraps=datetime)
                clock.now.return_value = now
                with mock.patch.object(bot, "datetime", clock), \
                        mock.patch.object(bot.random, "uniform", return_value=4), \
                        mock.patch.object(bot.random, "randint", side_effect=lambda _low, high: high), \
                        mock.patch.object(bot, "ambient_daily_post_cap", return_value=2), \
                        mock.patch.object(bot, "update_guild_ambient_times") as update:
                    if path == "initial":
                        actual = bot._random_time_today_pacific()
                    elif path == "failure_retry":
                        bot._reschedule_ambient_soon(42, "prior message")
                        actual = datetime.fromisoformat(update.call_args.args[2])
                    elif path == "next_day_random_max":
                        actual = bot._random_next_day_ambient_time_pacific()
                    else:
                        actual = datetime.fromisoformat(bot.schedule_after_ambient_post(
                            42, "prior message", 1, now_pacific=now))
                self.assertEqual(actual.date(), pacific(day=12).date())
                self.assertTrue(bot.ambient_posting_window_open(actual))
                if expected_time is not None:
                    self.assertEqual((actual.hour, actual.minute), expected_time)


class AmbientWindowSchedulerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.edition = editions.CommunityEditionIntegrationTests()
        # The borrowed fixture has no async runner of its own. Register its
        # cleanup callbacks on this running test instead of calling doCleanups.
        self.edition.addCleanup = self.addCleanup
        self.edition.setUp()
        self.clock = self.edition.fixture

    def count_posts(self):
        return self.edition.execute("SELECT COUNT(*) FROM ambient_log")[0][0]

    async def test_old_due_slot_waits_at_0759_then_resumes_at_0800(self):
        self.clock.now = pacific(hour=7, minute=59)
        channel, _guild, _ = self.edition.scheduler()
        with mock.patch.object(bot, "generate_dynamic_ambient", new=mock.AsyncMock(return_value=editions.LEGACY_TEXT)) as generate, \
                mock.patch.object(bot, "revalidate_ambient_sources", new=mock.AsyncMock(return_value=True)):
            await bot.ambient_message_task.coro()
            generate.assert_not_awaited()
            channel.send.assert_not_awaited()
            self.assertEqual(self.count_posts(), 0)
            self.clock.now = pacific(hour=8)
            await bot.ambient_message_task.coro()
        generate.assert_awaited_once()
        channel.send.assert_awaited_once()
        self.assertEqual(self.count_posts(), 1)

    async def test_old_due_slot_at_2000_does_not_generate_or_deliver(self):
        self.clock.now = pacific(hour=20)
        channel, _guild, _ = self.edition.scheduler()
        await bot.ambient_message_task.coro()
        self.edition.provider.assert_not_awaited()
        channel.send.assert_not_awaited()
        self.assertEqual(self.count_posts(), 0)
        next_at = datetime.fromisoformat(self.edition.execute(
            "SELECT next_ambient_message_at FROM guild_configs WHERE guild_id=42")[0][0])
        self.assertTrue(bot.ambient_posting_window_open(next_at))
        self.assertEqual(next_at.date(), pacific(day=12).date())

    async def test_generation_crossing_close_never_sends_or_logs(self):
        self.clock.now = pacific(hour=19, minute=59, second=58)
        channel, _guild, _ = self.edition.scheduler()
        async def generate(*_args, **_kwargs):
            self.clock.now += timedelta(seconds=5)
            return editions.LEGACY_TEXT
        with mock.patch.object(bot, "generate_dynamic_ambient", new=mock.AsyncMock(side_effect=generate)), \
                mock.patch.object(bot, "revalidate_ambient_sources", new=mock.AsyncMock(return_value=True)):
            await bot.ambient_message_task.coro()
        channel.send.assert_not_awaited()
        self.assertEqual(self.count_posts(), 0)

    async def test_member_confirmation_crossing_close_never_sends_or_logs(self):
        self.clock.now = pacific(hour=19, minute=59, second=58)
        def after_close(_user_id):
            self.clock.now += timedelta(seconds=5)
            return SimpleNamespace(id=7, bot=False, guild=SimpleNamespace(id=42))
        channel, guild, _ = self.edition.scheduler(fetch_effect=after_close)
        await bot.ambient_message_task.coro()
        guild.fetch_member.assert_awaited_once()
        channel.send.assert_not_awaited()
        self.assertEqual(self.count_posts(), 0)


class DormantEchoWindowTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.echo = echoes.DormantSignalEchoTests()
        self.echo.setUp()
        self.addCleanup(self.echo.tearDown)
        self.prepared = {"status": "ready", "message": "A remembered synth harmonic fits this thread.",
                         "subjectUserId": 42, "subjectDisplayName": "Test Member", "basis": {}}

    async def publish(self, initial):
        return await bot.publish_prepared_dormant_echo(
            77, 222, self.echo.channel, self.prepared,
            capacity_used_before_send=0, now_pacific=initial)

    def assert_no_delivery(self):
        self.assertEqual(self.echo.channel.sent, [])
        with closing(sqlite3.connect(self.echo.db_path)) as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM ambient_log").fetchone()[0], 0)

    async def test_direct_echo_publisher_is_closed_before_eight_and_at_twenty(self):
        for hour, minute in ((7, 59), (20, 0)):
            with self.subTest(hour=hour):
                self.echo.now = pacific(hour=hour, minute=minute)
                with mock.patch.object(bot, "relationship_v2_proactive_consent_decision", return_value=(True, "allowed")):
                    result = await self.publish(self.echo.now)
                self.assertNotEqual(result.get("status"), "published")
                self.assert_no_delivery()

    async def test_echo_checks_fresh_time_after_consent_lookup(self):
        initial = self.echo.now = pacific(hour=19, minute=59, second=58)
        def consent(*_args, **_kwargs):
            self.echo.now += timedelta(seconds=5)
            return True, "allowed"
        with mock.patch.object(bot, "relationship_v2_proactive_consent_decision", side_effect=consent):
            result = await self.publish(initial)
        self.assertNotEqual(result.get("status"), "published")
        self.assert_no_delivery()


class OccasionWindowTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.occasion = occasions.OccasionBotPathTests()
        self.occasion.setUp()
        self.addCleanup(self.occasion.tearDown)
        self.now = pacific(month=7, day=4, hour=19)
        clock = mock.Mock(wraps=datetime)
        clock.now.side_effect = lambda _tz=None: self.now
        patcher = mock.patch.object(bot, "datetime", clock)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.channel = occasions.FakeChannel()
        client_patch = mock.patch.object(bot, "client", SimpleNamespace(user=SimpleNamespace(id=self.channel.bot_user_id)))
        client_patch.start()
        self.addCleanup(client_patch.stop)

    async def cycle(self, generator, initial=None):
        with mock.patch.object(bot, "generate_occasion_reflection", new=generator):
            return await bot.process_due_occasion_for_guild(
                1, self.channel.id, self.channel, now_pacific=initial or self.now)

    async def test_owed_occasion_waits_overnight_and_delivers_at_eight(self):
        self.now = pacific(month=7, day=4, hour=20)
        generator = mock.AsyncMock(return_value=(occasions.valid_reflection(), ""))
        await self.cycle(generator)
        generator.assert_not_awaited()
        self.assertEqual(self.channel.send_attempts, [])
        self.now = pacific(month=7, day=5, hour=7, minute=59)
        await self.cycle(generator)
        generator.assert_not_awaited()
        self.now = pacific(month=7, day=5, hour=8)
        result = await self.cycle(generator)
        self.assertEqual(result["status"], "published")
        self.assertEqual(len(self.channel.send_attempts), 1)
        self.assertIn("2026-07-04", result["occurrenceKey"])

    async def test_generation_crossing_close_preserves_payload_for_next_opening(self):
        initial = self.now = pacific(month=7, day=4, hour=19, minute=59, second=58)
        content = occasions.valid_reflection()
        async def generate(*_args, **_kwargs):
            self.now += timedelta(seconds=5)
            return content, ""
        generator = mock.AsyncMock(side_effect=generate)
        await self.cycle(generator, initial)
        self.assertEqual(self.channel.send_attempts, [])
        with closing(sqlite3.connect(self.occasion.db_path)) as conn:
            saved, state = conn.execute("SELECT canonical_content,state FROM bnl_occasion_occurrences").fetchone()
            self.assertEqual(saved, content)
            self.assertNotEqual(state, "published")
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM ambient_log").fetchone()[0], 0)
        self.now = pacific(month=7, day=5, hour=8)
        result = await self.cycle(generator)
        self.assertEqual(result["status"], "published")
        generator.assert_awaited_once()
        self.assertEqual(len(self.channel.send_attempts), 1)
        self.assertEqual(self.channel.send_attempts[0][0], content)


if __name__ == "__main__":
    unittest.main()
