"""Real rollback-journal contention must not leave a reply writer locked."""

import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl01_bot as bot


class ReplySQLiteContentionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = str(Path(self.tmp.name) / "reply.db")
        self.db_patch = mock.patch.object(bot, "DB_FILE", self.path)
        self.db_patch.start()
        self.addCleanup(self.db_patch.stop)
        bot.init_db()
        self.connect = sqlite3.connect
        self.connections = []
        self.addCleanup(self._close_connections)

    def _close_connections(self):
        for conn in self.connections:
            conn.close()

    def _short_connection(self, *args, **kwargs):
        kwargs["timeout"] = 0.02
        conn = self.connect(*args, **kwargs)
        self.connections.append(conn)
        return conn

    def _hold_read(self, table):
        conn = self.connect(self.path)
        self.connections.append(conn)
        conn.execute("BEGIN")
        conn.execute("SELECT * FROM " + table).fetchall()
        return conn

    def _assert_readable(self, table, expected_count=0):
        conn = self.connect(self.path, timeout=0.02)
        try:
            self.assertEqual(
                conn.execute("SELECT count(*) FROM " + table).fetchone()[0],
                expected_count,
            )
        finally:
            conn.close()

    def test_style_commit_failure_releases_pending_lock_before_reader_exits(self):
        reader = self._hold_read("response_style_log")
        retained_error = None
        with mock.patch.object(bot.sqlite3, "connect", self._short_connection):
            try:
                result = bot.log_response_style(77, 42, "steady_reply")
            except sqlite3.OperationalError as exc:
                # A failed asyncio task can retain this traceback indefinitely.
                retained_error = exc
                result = None
        self._assert_readable("response_style_log")
        self.assertIsNone(retained_error)
        self.assertIs(result, False)
        for conn in self.connections[1:]:
            with self.assertRaises(sqlite3.ProgrammingError):
                conn.execute("SELECT 1")
        reader.close()
        self.assertIs(bot.log_response_style(77, 42, "steady_reply"), True)
        self._assert_readable("response_style_log", 1)

    def test_model_commit_failure_closes_connection_without_hiding_failure(self):
        reader = self._hold_read("conversations")
        with mock.patch.object(bot.sqlite3, "connect", self._short_connection):
            with self.assertRaises(sqlite3.OperationalError) as retained:
                bot.save_model_message(
                    42, 77, "A delivered test reply.", channel_id=700,
                    channel_name="bnl-testing", channel_policy="sealed_test",
                )
        self.assertIsNotNone(retained.exception)
        self._assert_readable("conversations")
        for conn in self.connections[1:]:
            with self.assertRaises(sqlite3.ProgrammingError):
                conn.execute("SELECT 1")
        reader.close()
        writer = self.connect(self.path, timeout=0.02)
        try:
            writer.execute("BEGIN IMMEDIATE")
            writer.rollback()
        finally:
            writer.close()

    def test_style_history_lock_does_not_prevent_style_selection(self):
        writer = self.connect(self.path)
        self.connections.append(writer)
        writer.execute("BEGIN EXCLUSIVE")
        with mock.patch.object(bot.sqlite3, "connect", self._short_connection):
            self.assertEqual(bot.get_recent_response_styles(77, 42), [])
            style, rule = bot.choose_response_style(77, 42, 1, "Explain rhythm.")
        self.assertTrue(style)
        self.assertTrue(rule)
        writer.rollback()
        self._assert_readable("response_style_log")

    def test_successful_style_history_keeps_member_and_room_scope(self):
        for member, style in ((42, "steady_reply"), (0, "brief_ping"),
                              (99, "deep_focus")):
            bot.log_response_style(77, member, style)
        bot.log_response_style(88, 42, "analytic_mode")
        self.assertEqual(bot.get_recent_response_styles(77, 42),
                         ["brief_ping", "steady_reply"])
        self.assertEqual(bot.get_recent_response_styles(77, limit=1),
                         ["deep_focus"])


if __name__ == "__main__":
    unittest.main()
