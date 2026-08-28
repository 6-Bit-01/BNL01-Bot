import argparse
import unittest
from datetime import datetime, time as clock_time
from pathlib import Path
from zoneinfo import ZoneInfo

from bnl_tiktok_live_chat import LiveEvent
from scripts import tiktok_live_chat_shadow_window as shadow


class ShadowWindowTests(unittest.TestCase):
    def setUp(self):
        self.timezone = ZoneInfo("America/Los_Angeles")
        self.timestamp = datetime(2026, 8, 28, 23, 15, tzinfo=self.timezone).timestamp()

    def test_default_window_runs_until_two_am(self):
        self.assertEqual(shadow.DEFAULT_WINDOW_START, clock_time(18, 50))
        self.assertEqual(shadow.DEFAULT_WINDOW_END, clock_time(2, 0))

    def test_friday_evening_resolves_cross_midnight_window(self):
        now = datetime(2026, 8, 28, 19, 0, tzinfo=self.timezone)
        window = shadow.resolve_active_window(
            now,
            shadow.DEFAULT_WEEKDAY,
            shadow.DEFAULT_WINDOW_START,
            shadow.DEFAULT_WINDOW_END,
        )
        self.assertIsNotNone(window)
        self.assertEqual(window.start, datetime(2026, 8, 28, 18, 50, tzinfo=self.timezone))
        self.assertEqual(window.end, datetime(2026, 8, 29, 2, 0, tzinfo=self.timezone))

    def test_saturday_tail_remains_inside_until_two(self):
        now = datetime(2026, 8, 29, 1, 59, tzinfo=self.timezone)
        window = shadow.resolve_active_window(
            now,
            shadow.DEFAULT_WEEKDAY,
            shadow.DEFAULT_WINDOW_START,
            shadow.DEFAULT_WINDOW_END,
        )
        self.assertIsNotNone(window)
        self.assertEqual(window.start.date().isoformat(), "2026-08-28")
        self.assertEqual(window.end, datetime(2026, 8, 29, 2, 0, tzinfo=self.timezone))

    def test_two_am_is_outside_the_window(self):
        now = datetime(2026, 8, 29, 2, 0, tzinfo=self.timezone)
        self.assertIsNone(
            shadow.resolve_active_window(
                now,
                shadow.DEFAULT_WEEKDAY,
                shadow.DEFAULT_WINDOW_START,
                shadow.DEFAULT_WINDOW_END,
            )
        )

    def test_resolver_rejects_naive_datetime(self):
        with self.assertRaises(ValueError):
            shadow.resolve_active_window(
                datetime(2026, 8, 28, 19, 0),
                shadow.DEFAULT_WEEKDAY,
                shadow.DEFAULT_WINDOW_START,
                shadow.DEFAULT_WINDOW_END,
            )

    def test_comment_formatter_preserves_public_handle_and_mod_flag(self):
        event = LiveEvent(
            event_type="comment",
            event_id="event-1",
            room_id="room-1",
            observed_at=self.timestamp,
            source_at=self.timestamp,
            unique_id="test.mod",
            display_name="Test Mod",
            comment_text="BNL CHAT TEST 740",
            moderator_flag=True,
        )
        self.assertEqual(
            shadow.format_event(event, self.timezone),
            "23:15:00 @test.mod [MOD]: BNL CHAT TEST 740",
        )

    def test_telemetry_formatters_are_plain_and_testable(self):
        events = [
            LiveEvent("like", "1", "room", self.timestamp, source_at=self.timestamp, like_count=125, like_total=8420),
            LiveEvent("viewer_snapshot", "2", "room", self.timestamp, source_at=self.timestamp, viewer_count=37),
            LiveEvent("share", "3", "room", self.timestamp, source_at=self.timestamp, unique_id="sharer"),
            LiveEvent("follow", "4", "room", self.timestamp, source_at=self.timestamp, unique_id="follower"),
            LiveEvent("gift", "5", "room", self.timestamp, source_at=self.timestamp, unique_id="giver", gift_name="Rose", gift_count=3, diamond_total=3),
            LiveEvent("question", "6", "room", self.timestamp, source_at=self.timestamp, unique_id="asker", question_text="Where do I submit?"),
        ]
        lines = [shadow.format_event(event, self.timezone) for event in events]
        self.assertEqual(lines[0], "23:15:00 [TAPS] +125 (total 8,420)")
        self.assertEqual(lines[1], "23:15:00 [VIEWERS] 37")
        self.assertIn("@sharer shared", lines[2])
        self.assertIn("@follower followed", lines[3])
        self.assertIn("Rose x3 · 3 diamonds", lines[4])
        self.assertIn("Where do I submit?", lines[5])

    def test_summary_includes_all_current_show_metrics(self):
        health = {
            "comments_accepted": 12,
            "taps_observed": 8420,
            "latest_like_total": 9000,
            "peak_viewers": 41,
            "shares": 3,
            "follows": 2,
            "gift_events": 1,
            "gift_units": 3,
            "diamond_total": 3,
            "questions": 4,
            "joins": 27,
            "duplicate_count": 6,
            "reconnect_count": 1,
        }
        summary = shadow.format_summary(health, 2)
        for expected in (
            "comments=12",
            "taps=8,420",
            "peak_viewers=41",
            "shares=3",
            "follows=2",
            "gifts=1/3",
            "diamonds=3",
            "questions=4",
            "joins=27",
            "duplicates_suppressed=6",
            "cycles=2",
        ):
            self.assertIn(expected, summary)

    def test_transport_command_is_read_only_collector_contract(self):
        args = argparse.Namespace(
            python=Path("/isolated/python"),
            transport_script=Path("/repo/scripts/tiktok_live_chat_transport.py"),
            username="six.bit",
            cdn="us",
            max_retries=5,
            stale_timeout=60.0,
            transport_dedupe_capacity=20_000,
        )
        command = shadow.build_transport_command(args)
        self.assertEqual(command[0], "/isolated/python")
        self.assertIn("--username", command)
        self.assertIn("six.bit", command)
        self.assertIn("--dedupe-capacity", command)
        self.assertNotIn("post", " ".join(command).lower())
        self.assertNotIn("moderate", " ".join(command).lower())


if __name__ == "__main__":
    unittest.main()
