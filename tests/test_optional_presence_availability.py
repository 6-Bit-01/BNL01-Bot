"""Optional scouting cannot abort mandatory capture or replay partial writes."""

import asyncio
import os
import sqlite3
import tempfile
import threading
import unittest
from contextlib import closing
from pathlib import Path
from unittest import mock

from tests import test_conversation_batching as runtime
import bnl_community_scouting as scouting


bot = runtime.bnl01_bot


class PresenceConnectionCleanupTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = str(Path(directory.name) / "presence.db")

    def test_schema_commit_failure_closes_connection(self):
        opened = []
        real_connect = sqlite3.connect

        class FailingCommit(sqlite3.Connection):
            def commit(self):
                raise sqlite3.OperationalError("database is locked")

        def connect(*args, **kwargs):
            conn = real_connect(*args, factory=FailingCommit, **kwargs)
            opened.append(conn)
            return conn

        with mock.patch.object(scouting.sqlite3, "connect", side_effect=connect), \
                self.assertRaises(sqlite3.OperationalError):
            scouting.ensure_community_presence_schema(self.path)
        self.assertEqual(len(opened), 1)
        with self.assertRaises(sqlite3.ProgrammingError):
            opened[0].execute("SELECT 1")

    def test_upsert_commit_failure_rolls_back_closes_and_releases_writer_lock(self):
        scouting.ensure_community_presence_schema(self.path)
        opened = []
        real_connect = sqlite3.connect

        class FailingCommit(sqlite3.Connection):
            def commit(self):
                if self.in_transaction:
                    raise sqlite3.OperationalError("database is locked")
                return super().commit()

        def connect(*args, **kwargs):
            conn = real_connect(*args, factory=FailingCommit, **kwargs)
            opened.append(conn)
            return conn

        with mock.patch.object(scouting.sqlite3, "connect", side_effect=connect), \
                self.assertRaises(sqlite3.OperationalError):
            scouting.upsert_community_presence_subject(self.path, 7700, "Copper Kite")
        self.assertEqual(len(opened), 2)
        for conn in opened:
            with self.assertRaises(sqlite3.ProgrammingError):
                conn.execute("SELECT 1")
        with closing(real_connect(self.path, timeout=0.01)) as conn, conn:
            conn.execute("BEGIN IMMEDIATE")
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM community_presence").fetchone()[0], 0)


class OptionalPresenceAvailabilityTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = str(Path(directory.name) / "presence.db")
        self.message = runtime.FakeMessage(
            runtime.FakeChannel(8810, name="community"),
            "artist Copper Kite is a collaborator",
            author=runtime.FakeAuthor(display_name="Signal Finch"),
        )
        self.env = mock.patch.dict(os.environ, {
            "BNL_COMMUNITY_SCOUTING_ENABLED": "true",
            "BNL_COMMUNITY_SCOUTING_CHANNEL_IDS": "8810",
        })
        self.env.start()
        self.addCleanup(self.env.stop)
        patch = mock.patch.object(bot, "DB_FILE", self.path)
        patch.start()
        self.addCleanup(patch.stop)

    async def test_optional_write_runs_off_event_loop_exactly_once(self):
        main_thread = threading.get_ident()
        called = []

        def record(*args):
            called.append((threading.get_ident(), args))

        with mock.patch.object(bot, "maybe_record_live_community_presence", side_effect=record) as recorder:
            await bot.maybe_record_live_community_presence_async(
                self.message, self.message.content, "public_home", True)
        recorder.assert_called_once_with(self.message, self.message.content, "public_home", True)
        self.assertNotEqual(called[0][0], main_thread)

    async def test_unapproved_channel_still_never_invokes_recorder(self):
        self.message.channel.id = 8811
        with mock.patch.object(bot, "record_community_presence_event") as recorder:
            await bot.maybe_record_live_community_presence_async(
                self.message, self.message.content, "public_home", True)
        recorder.assert_not_called()
        self.assertEqual(bot._last_community_presence_error_status, "channel_not_approved")

    async def test_partial_event_is_not_replayed_after_later_commit_failure(self):
        real_connect = sqlite3.connect
        opened, writes = [], []

        class LaterCommitFailure(sqlite3.Connection):
            def commit(self):
                if self.in_transaction:
                    writes.append(self)
                    if len(writes) == 2:
                        raise sqlite3.OperationalError("database is locked")
                return super().commit()

        def connect(*args, **kwargs):
            conn = real_connect(*args, factory=LaterCommitFailure, **kwargs)
            opened.append(conn)
            return conn

        with mock.patch.object(scouting.sqlite3, "connect", side_effect=connect), \
                self.assertLogs(level="WARNING") as recorded:
            await bot.maybe_record_live_community_presence_async(
                self.message, self.message.content, "public_home", True)
        self.assertEqual(len(writes), 2)
        self.assertEqual(bot._last_community_presence_error_status, "skipped_sqlite_busy")
        self.assertNotIn(self.message.content, " ".join(recorded.output))
        for conn in opened:
            with self.assertRaises(sqlite3.ProgrammingError):
                conn.execute("SELECT 1")
        with closing(real_connect(self.path, timeout=0.01)) as conn, conn:
            conn.execute("BEGIN IMMEDIATE")
            rows = conn.execute(
                "SELECT display_name, mention_count, direct_interaction_count FROM community_presence").fetchall()
        self.assertEqual(rows, [("Signal Finch", 1, 1)])

    async def test_busy_optional_hook_does_not_abort_actual_message_capture(self):
        fixture = runtime.ConversationBatchCoordinatorTests()
        original_hook = bot.maybe_record_live_community_presence
        capture_reached = RuntimeError("mandatory capture reached")
        locked = sqlite3.OperationalError("database is locked: private detail")
        locked.sqlite_errorcode = 5  # Stable SQLITE_BUSY value, including Python 3.9.
        with fixture._on_message_runtime(self.message.channel.id, followup_candidate=False), \
                mock.patch.object(bot, "resolve_channel_policy", return_value="public_home"), \
                mock.patch.object(bot, "maybe_record_live_community_presence", new=original_hook), \
                mock.patch.object(bot, "record_community_presence_event",
                                  side_effect=locked) as recorder, \
                mock.patch.object(bot, "save_user_message", side_effect=capture_reached) as capture, \
                self.assertLogs(level="WARNING") as recorded, \
                self.assertRaises(RuntimeError) as caught:
            await bot.on_message(self.message)
        self.assertIs(caught.exception, capture_reached)
        recorder.assert_called_once()
        capture.assert_called_once()
        self.assertEqual(bot._last_community_presence_error_status, "skipped_sqlite_busy")
        log = " ".join(recorded.output)
        self.assertNotIn("private detail", log)
        self.assertNotIn(self.message.content, log)
        self.assertIn("message_id=" + str(self.message.id), log)

    async def test_nonbusy_error_retains_failure_and_stops_before_capture(self):
        fixture = runtime.ConversationBatchCoordinatorTests()
        original_hook = bot.maybe_record_live_community_presence
        retained = sqlite3.OperationalError("no such table: missing")
        with fixture._on_message_runtime(self.message.channel.id, followup_candidate=False), \
                mock.patch.object(bot, "resolve_channel_policy", return_value="public_home"), \
                mock.patch.object(bot, "maybe_record_live_community_presence", new=original_hook), \
                mock.patch.object(bot, "record_community_presence_event", side_effect=retained) as recorder, \
                mock.patch.object(bot, "save_user_message") as capture, \
                self.assertRaises(sqlite3.OperationalError) as caught:
            await bot.on_message(self.message)
        self.assertIs(caught.exception, retained)
        recorder.assert_called_once()
        capture.assert_not_called()


if __name__ == "__main__":
    unittest.main()
