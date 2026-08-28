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
            observed_at=datetime(2026, 8, 28, 23, 15, tzinfo=self.timezone).timestamp(),
            unique_id="test.mod",
            display_name="Test Mod",
            comment_text="BNL CHAT TEST 740",
            moderator_flag=True,
        )
        self.assertEqual(
            shadow.format_event(event, self.timezone),
            "23:15:00 @test.mod [MOD]: BNL CHAT TEST 740",
        )

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
