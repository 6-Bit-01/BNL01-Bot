"""Optional read-index preparation must never leave a partial migration."""
from contextlib import closing
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import MagicMock, patch

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl01_bot as bot
from bnl_memory_ledger import (
    GOVERNED_SUBJECT_READ_INDEX,
    ensure_memory_ledger_schema,
    governed_subject_read_index_ready,
)


class GovernedSubjectIndexStartupTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "neutral.sqlite"
        self.db_patch = patch.object(bot, "DB_FILE", str(self.path))
        self.db_patch.start()
        self.addCleanup(self.db_patch.stop)
        self.attempt_patch = patch.object(bot, "_governed_subject_index_attempted_paths", set())
        self.attempt_patch.start()
        self.addCleanup(self.attempt_patch.stop)
        with closing(sqlite3.connect(self.path)) as conn:
            ensure_memory_ledger_schema(conn)
            conn.commit()

    def ready(self):
        with closing(sqlite3.connect(self.path)) as conn:
            return governed_subject_read_index_ready(conn)

    def test_normal_database_initialization_installs_index(self):
        self.assertFalse(self.ready())
        bot.init_db()
        self.assertTrue(self.ready())
        self.assertEqual(bot._prepare_governed_subject_read_index(), "already_attempted")

    def test_existing_index_needs_no_write_lock(self):
        self.assertEqual(bot._prepare_governed_subject_read_index(), "created")
        bot._governed_subject_index_attempted_paths.clear()
        with closing(sqlite3.connect(self.path)) as writer:
            writer.execute("BEGIN IMMEDIATE")
            self.assertEqual(bot._prepare_governed_subject_read_index(), "ready")
            writer.rollback()

    def test_busy_build_defers_and_reconnect_does_not_repeat_it(self):
        with closing(sqlite3.connect(self.path)) as writer:
            writer.execute("BEGIN IMMEDIATE")
            self.assertEqual(bot._prepare_governed_subject_read_index(), "deferred")
            writer.rollback()
        self.assertFalse(self.ready())
        self.assertEqual(bot._prepare_governed_subject_read_index(), "already_attempted")
        self.assertFalse(self.ready())

    def test_error_after_create_rolls_back_ddl_and_closes_connection(self):
        original = bot.ensure_governed_subject_read_index

        def failed_build(conn):
            self.assertTrue(conn.in_transaction)
            self.assertTrue(original(conn))
            raise sqlite3.OperationalError("neutral build failure")

        with patch.object(bot, "ensure_governed_subject_read_index", failed_build):
            self.assertEqual(bot._prepare_governed_subject_read_index(), "deferred")
        self.assertFalse(self.ready())
        with closing(sqlite3.connect(self.path, timeout=0)) as writer:
            writer.execute("BEGIN EXCLUSIVE")
            writer.rollback()

    def test_progress_interrupt_clears_handler_before_rollback(self):
        original = bot.ensure_governed_subject_read_index
        now = [0.0]

        def interrupted_build(conn):
            self.assertTrue(original(conn))
            now[0] = 21.0
            conn.execute("""WITH RECURSIVE n(value) AS (
                VALUES(0) UNION ALL SELECT value+1 FROM n WHERE value<100000
            ) SELECT sum(value) FROM n""").fetchone()
            self.fail("expired progress callback should interrupt SQLite")

        with patch.object(bot.time, "monotonic", lambda: now[0]), patch.object(
            bot, "ensure_governed_subject_read_index", interrupted_build
        ):
            self.assertEqual(bot._prepare_governed_subject_read_index(), "deferred")
        self.assertFalse(self.ready())
        with closing(sqlite3.connect(self.path, timeout=0)) as writer:
            writer.execute("BEGIN EXCLUSIVE")
            writer.rollback()

    def test_incompatible_existing_index_is_preserved(self):
        with closing(sqlite3.connect(self.path)) as conn:
            conn.execute("CREATE INDEX %s ON memory_ledger_entries(guild_id)" %
                         GOVERNED_SUBJECT_READ_INDEX)
            conn.commit()
        self.assertEqual(bot._prepare_governed_subject_read_index(), "unavailable")
        with closing(sqlite3.connect(self.path)) as conn:
            columns = [row[2] for row in conn.execute(
                "PRAGMA index_info(%s)" % GOVERNED_SUBJECT_READ_INDEX)]
        self.assertEqual(columns, ["guild_id"])

    def test_missing_path_is_not_created(self):
        missing = Path(self.directory.name) / "missing.sqlite"
        with patch.object(bot, "DB_FILE", str(missing)):
            self.assertEqual(bot._prepare_governed_subject_read_index(), "deferred")
        self.assertFalse(missing.exists())

    def test_optional_cleanup_errors_do_not_abort_database_startup(self):
        for operation in ("rollback", "close"):
            with self.subTest(operation=operation):
                bot._governed_subject_index_attempted_paths.clear()
                conn = MagicMock(spec=sqlite3.Connection)
                getattr(conn, operation).side_effect = sqlite3.OperationalError("neutral cleanup failure")
                with patch.object(bot.sqlite3, "connect", return_value=conn), patch.object(
                    bot, "governed_subject_read_index_ready", return_value=True
                ), self.assertLogs(level="INFO") as logged:
                    self.assertEqual(bot._prepare_governed_subject_read_index(), "deferred")
                conn.close.assert_called_once()
                self.assertIn("cleanup_error_type=" + operation + ":OperationalError", " ".join(logged.output))


if __name__ == "__main__":
    unittest.main()
