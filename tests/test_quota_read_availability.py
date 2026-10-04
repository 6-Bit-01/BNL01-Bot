"""Existing quota rules stay readable while another SQLite writer is reserved.

Load only the actual quota owners and async conversation function, isolating
Discord/import startup. SQLite schema, counters, lane queries and route ceilings
remain real; no provider call is made by these availability checks.
"""

import ast
import asyncio
import logging
import os
from pathlib import Path
import sqlite3
import tempfile
import threading
from contextlib import closing
from types import SimpleNamespace
import unittest
from unittest import mock

from bnl_gemini_routing import budget_ceiling_for_route, policy_for_route


FUNCTIONS = {
    "_add_column_if_missing", "_ensure_token_usage_schema",
    "_reset_token_counter_if_needed", "check_and_reset_daily_counters",
    "check_quota_availability", "_generation_lane_usage_on_connection",
    "_protected_usage_lane", "_usage_int", "get_gemini_response",
}
SOURCE = Path(__file__).resolve().parents[1] / "bnl01_bot.py"
TREE = ast.parse(SOURCE.read_text(encoding="utf-8"))
OWNER_NODES = [node for node in TREE.body
               if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
               and node.name in FUNCTIONS]
OWNER_MODULE = ast.Module(body=[ast.ImportFrom(
    module="__future__", names=[ast.alias(name="annotations")], level=0,
), *OWNER_NODES], type_ignores=[])
OWNER_CODE = compile(ast.fix_missing_locations(OWNER_MODULE), str(SOURCE), "exec")


class TrackedConnection(sqlite3.Connection):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.closed = False
        self.statements = []
        self.set_trace_callback(self.statements.append)

    def close(self):
        self.closed = True
        return super().close()


def quota_namespace(path, connect):
    namespace = dict(
        sqlite3=SimpleNamespace(connect=connect,
                                OperationalError=sqlite3.OperationalError),
        closing=closing, logging=logging, asyncio=asyncio,
        DB_FILE=path, DAILY_TOKEN_LIMIT=1000,
        _pacific_usage_date=lambda: "2026-10-03",
        _token_budget_reservation_lock=threading.Lock(),
        _token_budget_reserved_tokens=0, _token_budget_reserved_by_lane={},
        budget_ceiling_for_route=budget_ceiling_for_route,
        policy_for_route=policy_for_route,
        ORDINARY_CHAT_SINGLE_PACKET_ROUTE="ordinary_chat_single_packet_canary",
        ConversationImageInput=type("ConversationImageInput", (), {}),
        GenerationResult=lambda *args: SimpleNamespace(success=args[0]),
        GENERATION_ERROR_LOCAL_MODEL_BUDGET="local_model_budget_exhausted",
        GENERATION_ERROR_PROVIDER_UNKNOWN="provider_unknown",
        GEMINI_MODEL="test-model", record_generation_result_status=lambda _result: None,
        BackgroundGenerationUnavailable=type("BackgroundGenerationUnavailable", (Exception,), {}),
    )
    exec(OWNER_CODE, namespace)
    return namespace


class QuotaReadAvailabilityTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = str(Path(directory.name) / "quota.db")
        self.connections = []
        self.ns = quota_namespace(self.path, self.connect)
        self.environment = mock.patch.dict(os.environ, {
            "BNL_GEMINI_JOURNAL_PROTECTED_TOKENS": "100",
            "BNL_GEMINI_RELAY_PROTECTED_TOKENS": "100",
        })
        self.environment.start()
        self.addCleanup(self.environment.stop)
        self.addCleanup(self.close_connections)
        self.seed(tokens=200)

    def connect(self, *args, **kwargs):
        kwargs["timeout"] = 0.03
        kwargs["factory"] = TrackedConnection
        conn = sqlite3.connect(*args, **kwargs)
        self.connections.append(conn)
        return conn

    def close_connections(self):
        # Failure cleanup only; assertions independently require production
        # owners to close every connection before the fixture cleans up.
        for conn in self.connections:
            conn.close()

    def seed(self, *, tokens=0, date="2026-10-03"):
        with closing(sqlite3.connect(self.path)) as conn, conn:
            self.ns["_ensure_token_usage_schema"](conn.cursor())
            conn.execute("UPDATE token_usage SET tokens_used_today=?,last_reset_date=? WHERE id=1",
                         (tokens, date))

    def counter(self):
        with closing(sqlite3.connect(self.path)) as conn:
            return conn.execute("SELECT tokens_used_today,last_reset_date FROM token_usage WHERE id=1").fetchone()

    def assert_owned_connections_closed(self):
        self.assertTrue(self.connections)
        for conn in self.connections:
            self.assertTrue(conn.closed)

    def statements(self):
        return [sql.strip().upper() for conn in self.connections for sql in conn.statements]

    def test_full_current_day_quota_reads_under_reserved_writer(self):
        self.ns["_token_budget_reserved_tokens"] = 599
        with closing(sqlite3.connect(self.path)) as writer:
            writer.execute("BEGIN IMMEDIATE")
            self.assertTrue(self.ns["check_quota_availability"]("normal_chat"))
            self.ns["_token_budget_reserved_tokens"] = 600
            self.assertFalse(self.ns["check_quota_availability"]("normal_chat"))
            writer.rollback()
        self.assertEqual(self.counter(), (200, "2026-10-03"))
        self.assertFalse(any(sql.startswith(("INSERT", "UPDATE", "DELETE", "CREATE", "ALTER", "BEGIN IMMEDIATE"))
                             for sql in self.statements()))
        self.assert_owned_connections_closed()

    def test_current_day_lane_use_and_inflight_reserves_keep_real_ceilings(self):
        self.seed(tokens=870)
        with closing(sqlite3.connect(self.path)) as conn, conn:
            conn.execute("INSERT INTO token_usage_events(usage_date,recorded_at,route,model,total_tokens) VALUES (?,?,?,?,?)",
                         ("2026-10-03", "2026-10-03T12:00:00Z", "journal_generation", "test", 50))
        self.assertFalse(self.ns["check_quota_availability"]("normal_chat"))
        self.assertTrue(self.ns["check_quota_availability"]("journal_generation"))
        self.ns["_token_budget_reserved_by_lane"]["journal"] = 50
        self.assertTrue(self.ns["check_quota_availability"]("normal_chat"))
        self.ns["_token_budget_reserved_tokens"] = 30
        self.assertFalse(self.ns["check_quota_availability"]("normal_chat"))
        self.assert_owned_connections_closed()

    def test_missing_event_schema_bootstraps_without_false_allowance(self):
        with closing(sqlite3.connect(self.path)) as conn, conn:
            conn.execute("DROP TABLE token_usage_events")
        self.seed(tokens=1000)
        with closing(sqlite3.connect(self.path)) as conn, conn:
            conn.execute("DROP TABLE token_usage_events")
        self.assertFalse(self.ns["check_quota_availability"]("normal_chat"))
        self.assertEqual(self.counter(), (1000, "2026-10-03"))
        with closing(sqlite3.connect(self.path)) as conn:
            self.assertIsNotNone(conn.execute("SELECT 1 FROM sqlite_master WHERE name='token_usage_events'").fetchone())
        self.assert_owned_connections_closed()

    def test_missing_counter_row_is_initialized_and_pacific_reset(self):
        with closing(sqlite3.connect(self.path)) as conn, conn:
            conn.execute("DELETE FROM token_usage WHERE id=1")
        self.assertTrue(self.ns["check_quota_availability"]("normal_chat"))
        self.assertEqual(self.counter(), (0, "2026-10-03"))
        self.assert_owned_connections_closed()

    def test_new_database_uses_existing_bootstrap_owner(self):
        self.ns["DB_FILE"] = self.path + ".new"
        self.assertTrue(self.ns["check_quota_availability"]("normal_chat"))
        with closing(sqlite3.connect(self.ns["DB_FILE"])) as conn:
            self.assertEqual(conn.execute("SELECT tokens_used_today,last_reset_date FROM token_usage WHERE id=1").fetchone(),
                             (0, "2026-10-03"))
            self.assertIsNotNone(conn.execute("SELECT 1 FROM sqlite_master WHERE name='gemini_budget_reservations'").fetchone())
        self.assert_owned_connections_closed()

    def test_malformed_existing_counter_schema_stays_a_visible_error(self):
        with closing(sqlite3.connect(self.path)) as conn, conn:
            conn.execute("DROP TABLE token_usage")
            conn.execute("CREATE TABLE token_usage(id INTEGER PRIMARY KEY,tokens_used_today INTEGER)")
        with self.assertRaisesRegex(sqlite3.OperationalError, "no such column"):
            self.ns["check_quota_availability"]("normal_chat")
        self.assert_owned_connections_closed()

    def test_rollover_resets_once_and_preserves_new_day_usage(self):
        self.seed(tokens=960, date="2026-10-02")
        self.assertTrue(self.ns["check_quota_availability"]("normal_chat"))
        self.assertEqual(self.counter(), (0, "2026-10-03"))
        with closing(sqlite3.connect(self.path)) as conn, conn:
            conn.execute("UPDATE token_usage SET tokens_used_today=tokens_used_today+73 WHERE id=1")
        self.assertTrue(self.ns["check_quota_availability"]("normal_chat"))
        self.assertEqual(self.counter(), (73, "2026-10-03"))
        resets = [sql for sql in self.statements() if sql.startswith("UPDATE TOKEN_USAGE")]
        self.assertEqual(len(resets), 1)
        self.assert_owned_connections_closed()

    def test_waiting_rollover_rechecks_after_other_owner_records_usage(self):
        self.seed(tokens=960, date="2026-10-02")
        initial = self.connect
        advanced = []

        def connect_after_rollover(*args, **kwargs):
            # Both owners first observed yesterday. Before this owner obtains
            # its write transaction, an independent writer completes rollover
            # and records current-day usage on the real database.
            if len(self.connections) == 1 and not advanced:
                with closing(sqlite3.connect(self.path)) as conn, conn:
                    conn.execute("BEGIN IMMEDIATE")
                    conn.execute("UPDATE token_usage SET tokens_used_today=73,last_reset_date='2026-10-03' WHERE id=1")
                advanced.append(True)
            return initial(*args, **kwargs)

        self.ns["sqlite3"].connect = connect_after_rollover
        self.ns["check_and_reset_daily_counters"]()
        self.assertEqual(advanced, [True])
        self.assertEqual(self.counter(), (73, "2026-10-03"))
        self.assertFalse(any(sql.startswith("UPDATE TOKEN_USAGE") for sql in self.statements()))
        self.assert_owned_connections_closed()

    def test_rollover_lock_is_visible_and_does_not_grant_quota(self):
        self.seed(tokens=960, date="2026-10-02")
        with closing(sqlite3.connect(self.path)) as writer:
            writer.execute("BEGIN IMMEDIATE")
            with self.assertRaises(sqlite3.OperationalError):
                self.ns["check_quota_availability"]("normal_chat")
            writer.rollback()
        self.assertEqual(self.counter(), (960, "2026-10-02"))
        self.assert_owned_connections_closed()


class AsyncQuotaAvailabilityTests(unittest.IsolatedAsyncioTestCase):
    async def test_blocked_real_conversation_quota_keeps_event_loop_running(self):
        started, release = threading.Event(), threading.Event()
        main_thread = threading.get_ident()
        observed = []
        ns = quota_namespace("unused.db", sqlite3.connect)

        def blocked_quota(route):
            observed.append((threading.get_ident(), route))
            started.set()
            if not release.wait(1):
                raise AssertionError("quota blocked the Discord event loop")
            return False

        ns["check_quota_availability"] = blocked_quota
        ns["_generate_gemini_content_result_async"] = mock.AsyncMock()
        task = asyncio.create_task(ns["get_gemini_response"]("A current request", 7, 1))
        try:
            async def heartbeat():
                while not started.is_set():
                    await asyncio.sleep(0.001)
                self.assertFalse(task.done())
                release.set()

            await asyncio.wait_for(heartbeat(), timeout=0.5)
            self.assertEqual(await task, "")
        finally:
            release.set()
            await asyncio.gather(task, return_exceptions=True)
        self.assertNotEqual(observed[0][0], main_thread)
        self.assertEqual(observed[0][1], "get_gemini_response")
        ns["_generate_gemini_content_result_async"].assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
