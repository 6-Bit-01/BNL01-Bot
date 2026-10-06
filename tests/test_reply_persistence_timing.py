"""Privacy-safe phase timings must leave real SQLite transaction behavior intact.

Only existing synchronous owners are AST-loaded, following the lifetime tests.
The tracking subclass never overrides native transaction enter/exit or closes
implicitly. No bot startup, provider, network, credentials or live database.
"""

import ast
from contextlib import closing
from functools import lru_cache
import gc
from pathlib import Path
import sqlite3
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest import mock


SOURCE = Path(__file__).resolve().parents[1] / "bnl01_bot.py"


@lru_cache(maxsize=1)
def owners():
    tree = ast.parse(SOURCE.read_text(encoding="utf-8"))
    names = {"_sqlite_busy", "_persist_reply_transaction"}
    nodes = [node for node in tree.body
             if isinstance(node, ast.FunctionDef) and node.name in names]
    assert len(nodes) == len(names)
    return compile(ast.Module(body=nodes, type_ignores=[]), str(SOURCE), "exec")


class TrackedConnection(sqlite3.Connection):
    """Retain native transaction behavior, including failed implicit COMMIT."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sql = []
        self.set_trace_callback(self.sql.append)


class ReplyPersistenceTimingTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="reply-timing-neutral-")
        self.path = str(Path(self.directory.name) / "neutral.sqlite")
        self.connections = []
        self.readers = []
        self.retained = []
        self.sleeps = []
        self.logger = mock.Mock()
        self.sleep_hook = lambda seconds: None
        self.namespace = {
            "sqlite3": SimpleNamespace(
                connect=self.connect, OperationalError=sqlite3.OperationalError,
                DatabaseError=sqlite3.DatabaseError,
            ),
            "closing": closing, "DB_FILE": self.path, "logging": self.logger,
            "time": SimpleNamespace(perf_counter=time.perf_counter, sleep=self.sleep),
        }
        exec(owners(), self.namespace)
        with closing(sqlite3.connect(self.path)) as conn, conn:
            self.assertEqual(conn.execute("PRAGMA journal_mode=DELETE").fetchone()[0], "delete")
            conn.execute("CREATE TABLE receipts (value TEXT NOT NULL)")

    def tearDown(self):
        self.release_readers()
        for conn in self.connections:
            conn.close()
        self.retained.clear()
        gc.collect()
        self.directory.cleanup()

    def connect(self, *args, **kwargs):
        self.assertEqual(args[0], self.path)
        kwargs["factory"] = TrackedConnection
        conn = sqlite3.connect(*args, **kwargs)
        self.connections.append(conn)
        return conn

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.sleep_hook(seconds)

    def hold_reader(self):
        conn = sqlite3.connect(self.path)
        conn.execute("BEGIN")
        conn.execute("SELECT COUNT(*) FROM receipts").fetchone()
        self.readers.append(conn)

    def release_readers(self):
        while self.readers:
            self.readers.pop().close()

    def persist(self, callback, operation="intelligence_packet"):
        return self.namespace["_persist_reply_transaction"](
            callback, operation=operation, timeout=.02,
        )

    def timing(self):
        calls = [call for call in self.logger.info.call_args_list
                 if call.args[0].startswith("reply_persistence_timing ")]
        self.assertEqual(len(calls), 1)
        return dict(piece.split("=", 1) for piece in
                    (calls[0].args[0] % calls[0].args[1:]).split()[1:])

    def retry_fields(self):
        return [dict(piece.split("=", 1) for piece in
                     (call.args[0] % call.args[1:]).split()[1:])
                for call in self.logger.warning.call_args_list]

    def assert_closed_and_durable(self, count):
        for conn in self.connections:
            with self.assertRaises(sqlite3.ProgrammingError):
                conn.execute("SELECT 1")
        self.release_readers()
        with closing(sqlite3.connect(self.path, timeout=.1)) as conn, conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM receipts").fetchone()[0], count)
            # A writer must commit after owner cleanup even with exception/cursor refs retained.
            conn.execute("INSERT INTO receipts VALUES ('cleanup_probe')")

    def test_three_named_owners_report_completed_static_numeric_fields(self):
        for operation in ("intelligence_packet", "single_packet_begin", "single_packet_evaluation"):
            with self.subTest(operation=operation):
                self.logger.reset_mock()
                value = "neutral_private_source_do_not_log"
                def write(conn):
                    conn.execute("INSERT INTO receipts VALUES (?)", (value,))
                    return value
                self.assertEqual(self.persist(write, operation), value)
                fields = self.timing()
                self.assertEqual(set(fields), {
                    "operation", "status", "phase", "total_ms", "callback_ms",
                    "commit_ms", "builds", "attempts", "error_category",
                })
                self.assertEqual(fields["operation"], operation)
                self.assertEqual(fields["status"], "completed")
                self.assertEqual(fields["phase"], "commit")
                self.assertEqual(fields["builds"], "1")
                self.assertEqual(fields["attempts"], "1")
                self.assertEqual(fields["error_category"], "none")
                for key in ("total_ms", "callback_ms", "commit_ms"):
                    self.assertGreaterEqual(int(fields[key]), 0)
                rendered = " ".join(str(call) for call in self.logger.mock_calls)
                self.assertNotIn(value, rendered)
                self.assertNotIn("INSERT", rendered)
                self.assertNotIn(self.path, rendered)

    def test_native_failed_commit_rebuilds_then_commits_once(self):
        builds = []
        def write(conn):
            cursor = conn.execute("INSERT INTO receipts VALUES ('neutral')")
            self.retained.append(cursor)
            builds.append(1)
            if len(builds) == 1:
                self.hold_reader()
            return len(builds)
        self.sleep_hook = lambda seconds: self.release_readers()
        self.assertEqual(self.persist(write), 2)
        self.assertEqual(self.sleeps, [.1])
        self.assertEqual(self.retry_fields(), [{
            "operation": "intelligence_packet", "attempt": "1", "phase": "commit", "retained": "0",
        }])
        fields = self.timing()
        self.assertEqual(fields["builds"], "2")
        self.assertEqual(fields["attempts"], "2")
        self.assertGreater(int(fields["commit_ms"]), 0)
        self.assert_closed_and_durable(1)

    def test_callback_busy_is_write_phase_and_retries_fresh(self):
        blocker = sqlite3.connect(self.path)
        self.addCleanup(blocker.close)
        blocker.execute("BEGIN IMMEDIATE")
        def write(conn):
            return conn.execute("INSERT INTO receipts VALUES ('neutral')").rowcount
        def release_writer(seconds):
            blocker.rollback()
            blocker.close()
        self.sleep_hook = release_writer
        self.assertEqual(self.persist(write), 1)
        self.assertEqual(self.retry_fields()[0]["phase"], "write")
        self.assertEqual(self.retry_fields()[0]["retained"], "0")
        fields = self.timing()
        self.assertEqual(fields["builds"], "2")
        self.assertEqual(fields["attempts"], "2")
        self.assertGreater(int(fields["callback_ms"]), 0)
        self.assert_closed_and_durable(1)

    def test_commit_exhaustion_reports_failure_and_releases_native_locks(self):
        self.hold_reader()
        def write(conn):
            self.retained.append(conn.execute("INSERT INTO receipts VALUES ('neutral')"))
        try:
            self.persist(write)
        except sqlite3.OperationalError as error:
            self.retained.append(error)
        else:
            self.fail("commit_should_be_busy")
        self.assertEqual(self.sleeps, [.1, .2])
        self.assertEqual([row["phase"] for row in self.retry_fields()], ["commit", "commit"])
        fields = self.timing()
        self.assertEqual(fields["status"], "failed")
        self.assertEqual(fields["phase"], "commit")
        self.assertEqual(fields["error_category"], "busy_or_locked")
        self.assertEqual(fields["builds"], "3")
        self.assertEqual(fields["attempts"], "3")
        self.assert_closed_and_durable(0)

    def test_nonbusy_sqlite_error_has_no_retry_and_no_raw_error(self):
        def write(conn):
            conn.execute("INSERT INTO receipts VALUES ('neutral')")
            conn.execute("SELECT * FROM neutral_missing_table")
        with self.assertRaises(sqlite3.OperationalError) as caught:
            self.persist(write)
        self.retained.append(caught.exception)
        self.assertEqual(self.sleeps, [])
        fields = self.timing()
        self.assertEqual(fields["error_category"], "sqlite_other")
        self.assertEqual(fields["phase"], "write")
        self.assertNotIn("neutral_missing_table", str(self.logger.mock_calls))
        self.assert_closed_and_durable(0)

    def test_callback_baseexception_rolls_back_and_reports_write_failure(self):
        def write(conn):
            conn.execute("INSERT INTO receipts VALUES ('neutral')")
            raise KeyboardInterrupt("neutral_private_error_do_not_log")
        with self.assertRaises(KeyboardInterrupt) as caught:
            self.persist(write)
        self.retained.append(caught.exception)
        fields = self.timing()
        self.assertEqual(fields["phase"], "write")
        self.assertEqual(fields["error_category"], "non_sqlite")
        self.assertNotIn("neutral_private_error_do_not_log", str(self.logger.mock_calls))
        self.assert_closed_and_durable(0)

    def test_interrupted_backoff_keeps_prior_connection_closed(self):
        self.hold_reader()
        def write(conn):
            conn.execute("INSERT INTO receipts VALUES ('neutral')")
        def stop(seconds):
            raise KeyboardInterrupt("neutral_interrupt")
        self.sleep_hook = stop
        with self.assertRaises(KeyboardInterrupt) as caught:
            self.persist(write)
        self.retained.append(caught.exception)
        fields = self.timing()
        self.assertEqual(fields["phase"], "backoff")
        self.assertEqual(fields["status"], "failed")
        self.assertEqual(fields["builds"], "1")
        self.assert_closed_and_durable(0)

    def test_default_owner_keeps_original_retry_logs_and_no_timing(self):
        builds = []
        def write(conn):
            conn.execute("INSERT INTO receipts VALUES ('neutral')")
            builds.append(1)
            if len(builds) == 1:
                self.hold_reader()
            return len(builds)
        self.sleep_hook = lambda seconds: self.release_readers()
        self.assertEqual(self.persist(write, "model_conversation"), 2)
        self.assertEqual(self.retry_fields(), [{"operation": "model_conversation", "attempt": "1"}])
        self.logger.info.assert_not_called()
        self.assert_closed_and_durable(1)


if __name__ == "__main__":
    unittest.main()
