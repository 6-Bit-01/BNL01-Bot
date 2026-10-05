"""Real owner transactions must release locks even on old Python SQLite exits.

Compile only the existing owners, as in the budget/Relay AST tests: no bot import,
credentials, provider, Discord, or application database. Unlike a closing test
connection, the tracking subclass keeps native __enter__/__exit__ unchanged.
This module also runs standalone with the deployed Python 3.9.5 stdlib.
"""

import ast
from contextlib import closing
from datetime import datetime, timezone
from functools import lru_cache
import logging
from pathlib import Path
import sqlite3
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest import mock


SOURCE = Path(__file__).resolve().parents[1] / "bnl01_bot.py"
OWNERS = {
    "LocalModelBudgetExhausted", "LocalBudgetReservation",
    "_add_column_if_missing", "_ensure_token_usage_schema", "_usage_int",
    "_protected_usage_lane", "_generation_lane_usage_on_connection",
    "_release_dollar_budget", "reserve_local_model_budget",
    "renew_show_update_claim", "_persist_reply_transaction", "_sqlite_busy",
}


@lru_cache(maxsize=2)
def compiled_owners(source):
    parsed = ast.parse(source.read_text(encoding="utf-8"))
    selected = [node for node in parsed.body
                if isinstance(node, (ast.FunctionDef, ast.ClassDef))
                and node.name in OWNERS]
    found = {node.name for node in selected}
    if found != OWNERS:
        raise AssertionError("Missing lifetime owners: %s" % (OWNERS - found))
    module = ast.Module(body=[ast.ImportFrom(
        module="__future__", names=[ast.alias(name="annotations")], level=0,
    ), *selected], type_ignores=[])
    return compile(ast.fix_missing_locations(module), str(source), "exec")


def load_owners(namespace):
    exec(compiled_owners(SOURCE), namespace)
    return namespace


class TrackedConnection(sqlite3.Connection):
    """Track completion without repairing native transaction/close behavior."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.trace = []
        self.set_trace_callback(self.trace.append)


class SQLiteConnectionLifetimeTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory(prefix="bnl-lifetime-test-")
        self.addCleanup(directory.cleanup)
        self.path = str(Path(directory.name) / "neutral.sqlite")
        self.connections = []
        # Keep references, including exception tracebacks, until every lock
        # assertion finishes. Collection must never serve as transaction cleanup.
        self.exceptions = []
        self.retained = []
        self.addCleanup(self.close_tracked)
        self.dollars = mock.Mock(return_value=("neutral-dollar-reservation", 123))
        self.ns = load_owners({
            "sqlite3": SimpleNamespace(
                connect=self.tracked_connect,
                OperationalError=sqlite3.OperationalError,
            ),
            "closing": closing, "DB_FILE": self.path,
            "datetime": datetime, "timezone": timezone,
            "logging": logging, "time": time,
            "_token_budget_reservation_lock": threading.Lock(),
            "_token_budget_reserved_tokens": 0,
            "_token_budget_reserved_by_lane": {},
            "_estimated_generation_reservation": lambda *_args: 7,
            "get_usage_stats": lambda: (0, "2026-10-05"),
            "policy_for_route": lambda route: SimpleNamespace(
                journal_protected=route == "journal",
                relay_protected=route == "relay",
            ),
            "budget_ceiling_for_route": lambda *_args, **_kwargs: 100,
            "DAILY_TOKEN_LIMIT": 100,
            "_reserve_dollar_budget": self.dollars,
        })
        with closing(sqlite3.connect(self.path)) as conn, conn:
            self.assertEqual(conn.execute("PRAGMA journal_mode=DELETE").fetchone()[0], "delete")
            self.ns["_ensure_token_usage_schema"](conn.cursor())
            conn.execute("UPDATE token_usage SET tokens_used_today=11, last_reset_date='2026-10-05'")
            conn.execute("INSERT INTO gemini_budget_reservations VALUES(?,?,?,?,?,?,?,?)",
                         ("keep-until-committed", "2026-10-05T00:00:00Z", "2026-11-01T00:00:00Z",
                          "2026-10-05", "2026-10", "normal_chat", "ordinary", 123))
            conn.execute("CREATE TABLE friday_show_update_claims (guild_id INTEGER, show_date TEXT, phase_key TEXT, claim_token TEXT, claimed_at TEXT)")
            conn.execute("INSERT INTO friday_show_update_claims VALUES(1,'2026-10-09','start','owned-claim','original')")

    def tracked_connect(self, *args, **kwargs):
        self.assertEqual(str(args[0]), self.path)
        kwargs["timeout"] = 0.02
        kwargs["factory"] = TrackedConnection
        conn = sqlite3.connect(*args, **kwargs)
        self.connections.append(conn)
        return conn

    def close_tracked(self):
        for conn in self.connections:
            conn.close()

    def rows(self, query):
        with closing(sqlite3.connect(self.path, timeout=0.02)) as conn:
            return conn.execute(query).fetchall()

    def hold_reader(self):
        conn = sqlite3.connect(self.path, timeout=0.02)
        self.addCleanup(conn.close)
        conn.execute("BEGIN")
        conn.execute("SELECT * FROM token_usage").fetchall()
        return conn

    def assert_owner_closed(self):
        self.assertTrue(self.connections)
        for conn in self.connections:
            with self.assertRaises(sqlite3.ProgrammingError):
                conn.execute("SELECT 1")

    def capture_busy(self, operation):
        try:
            operation()
        except sqlite3.OperationalError as exc:
            self.assertIn("locked", str(exc).lower())
            self.exceptions.append(exc)
        else:
            self.fail("fixture reader must force the real owner commit to fail")
        self.assertTrue(any("COMMIT" == sql for conn in self.connections for sql in conn.trace))

    def assert_no_in_memory_reservation(self):
        self.assertEqual(self.ns["_token_budget_reserved_tokens"], 0)
        self.assertEqual(self.ns["_token_budget_reserved_by_lane"], {})
        self.assertFalse(self.ns["_token_budget_reservation_lock"].locked())
        self.dollars.assert_not_called()

    def test_budget_release_failed_exit_commit_does_not_leave_database_locked(self):
        reader = self.hold_reader()
        self.capture_busy(lambda: self.ns["_release_dollar_budget"]("keep-until-committed"))
        reader.close()
        # On unpatched Python 3.9.5 with the old owner, this fresh read is still
        # blocked despite releasing the original reader and retaining no worker.
        self.assertEqual(self.rows("SELECT reservation_id FROM gemini_budget_reservations"),
                         [("keep-until-committed",)])
        self.assert_owner_closed()

    def test_pre_provider_lane_read_failed_exit_commit_releases_lock_and_reserves_nothing(self):
        # Schema's real INSERT OR IGNORE is intentionally exercised. Delete its
        # seed first so rollback also has a visible, testable state difference.
        with closing(sqlite3.connect(self.path)) as conn, conn:
            conn.execute("DELETE FROM token_usage")
        reader = self.hold_reader()
        self.capture_busy(lambda: self.ns["reserve_local_model_budget"]("neutral request", "normal_chat"))
        reader.close()
        self.assertEqual(self.rows("SELECT id FROM token_usage"), [])
        self.assertEqual(self.rows("SELECT reservation_id FROM gemini_budget_reservations"),
                         [("keep-until-committed",)])
        self.assert_no_in_memory_reservation()
        self.assert_owner_closed()

    def test_successful_budget_release_commits_and_closes(self):
        self.ns["_release_dollar_budget"]("keep-until-committed")
        self.assertEqual(self.rows("SELECT reservation_id FROM gemini_budget_reservations"), [])
        self.assert_owner_closed()

    def test_successful_pre_provider_reservation_keeps_lane_accounting(self):
        result = self.ns["reserve_local_model_budget"]("neutral request", "journal")
        self.assertEqual(int(result), 7)
        self.assertEqual(result.lane, "journal")
        self.assertEqual(result.cost_reservation_id, "neutral-dollar-reservation")
        self.assertEqual(result.estimated_cost_nanos, 123)
        self.assertEqual(self.ns["_token_budget_reserved_tokens"], 7)
        self.assertEqual(self.ns["_token_budget_reserved_by_lane"], {"journal": 7})
        self.dollars.assert_called_once_with("neutral request", "journal")
        self.assert_owner_closed()

    def test_budget_release_body_exception_rolls_back_and_closes(self):
        real_schema = self.ns["_ensure_token_usage_schema"]
        error = ValueError("neutral body failure")
        def failed_schema(cursor):
            real_schema(cursor)
            cursor.execute("UPDATE token_usage SET tokens_used_today=999")
            raise error
        self.ns["_ensure_token_usage_schema"] = failed_schema
        try:
            self.ns["_release_dollar_budget"]("keep-until-committed")
        except ValueError as exc:
            self.exceptions.append(exc)
            self.assertIs(exc, error)
        else:
            self.fail("body failure must propagate")
        self.assertEqual(self.rows("SELECT tokens_used_today FROM token_usage"), [(11,)])
        self.assertEqual(self.rows("SELECT reservation_id FROM gemini_budget_reservations"),
                         [("keep-until-committed",)])
        self.assert_owner_closed()

    def test_show_claim_early_return_commits_and_closes(self):
        self.assertTrue(self.ns["renew_show_update_claim"](1, "2026-10-09", "start", "owned-claim"))
        self.assertNotEqual(self.rows("SELECT claimed_at FROM friday_show_update_claims"), [("original",)])
        self.assert_owner_closed()

    def test_reply_transaction_closes_with_completed_cursor_and_result_retained(self):
        def read(conn):
            cursor = conn.execute("SELECT 1 UNION ALL SELECT 2 FROM token_usage")
            self.retained.append(cursor)
            # Real bookkeeping callers return completed values. An unfinished
            # cursor is deliberately not an allowed escape from this owner.
            return cursor.fetchall()
        result = self.ns["_persist_reply_transaction"](read, operation="neutral_cursor")
        self.assertEqual(result, [(1,), (2,)])
        self.assert_owner_closed()
        # A fresh writer proves the retained completed cursor and connection
        # cannot retain a shared lock after their owner closes.
        with closing(sqlite3.connect(self.path, timeout=0.02)) as conn, conn:
            conn.execute("UPDATE token_usage SET tokens_used_today=12")
        self.assertEqual(self.rows("SELECT tokens_used_today FROM token_usage"), [(12,)])
        with self.assertRaises(sqlite3.ProgrammingError):
            self.retained[0].fetchone()


if __name__ == "__main__":
    unittest.main(verbosity=2)
