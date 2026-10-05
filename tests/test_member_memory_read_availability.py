"""Real SQLite acquisition tests; no providers or full bot initialization.

The unchanged function bodies are loaded from the product module. A neutral SQL
context reducer exposes transaction/retry behavior; existing full memory tests
remain responsible for complete governed context and prompt parity.
"""
from __future__ import annotations

import ast
import asyncio
import json
import logging
import sqlite3
import tempfile
import threading
import time
import types
import unittest
from collections import Counter
from contextlib import closing, nullcontext
from pathlib import Path
from unittest import mock


SOURCE = Path(__file__).resolve().parents[1] / "bnl01_bot.py"
CONNECT = sqlite3.connect


class TrackedConnection(sqlite3.Connection):
    closed_by_owner = False

    def close(self):
        super().close()
        self.closed_by_owner = True


class MemberMemoryAvailabilityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        tree = ast.parse(SOURCE.read_text(encoding="utf-8"))
        names = {
            "_sqlite_busy", "_member_memory_read_error_category",
            "_member_memory_read_sqlite_code", "_log_member_memory_read_result",
            "_open_member_memory_read_connection", "_read_user_memory_snapshot",
            "_read_bounded_member_memory", "build_user_memory_context_async",
            "build_named_public_member_memory_context", "build_batch_member_memory_context",
        }
        cls.nodes = [n for n in tree.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
                     and n.name in names]
        if {n.name for n in cls.nodes} != names:
            raise AssertionError("Missing actual memory reader function")
        cls.markers = next(ast.literal_eval(n.value) for n in tree.body
                           if isinstance(n, ast.Assign)
                           and any(isinstance(t, ast.Name) and t.id == "_SOURCE_BEARING_MEMORY_MARKERS"
                                   for t in n.targets))

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="bnl-memory-availability-")
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "neutral.sqlite"
        with closing(CONNECT(self.path)) as conn, conn:
            self.assertEqual(conn.execute("PRAGMA journal_mode=DELETE").fetchone()[0], "delete")
            conn.execute("CREATE TABLE evidence(value TEXT, eligible INTEGER)")
            conn.execute("INSERT INTO evidence VALUES('neutral-original',1)")
        self.opened = []
        self.reducer_calls = 0
        self.pauses = []
        self.retry_hook = None

        def tracked_connect(*args, **kwargs):
            self.assertTrue(kwargs["uri"])
            self.assertIn("?mode=ro", args[0])
            self.assertEqual(kwargs["timeout"], .25)
            kwargs["factory"] = TrackedConnection
            conn = CONNECT(*args, **kwargs)
            self.opened.append(conn)
            return conn

        def sleep(delay):
            self.pauses.append(delay)
            self.assert_closed()
            if self.retry_hook:
                self.retry_hook()
            time.sleep(delay)

        self.ns = {
            "sqlite3": types.SimpleNamespace(
                connect=tracked_connect, Connection=sqlite3.Connection,
                OperationalError=sqlite3.OperationalError, DatabaseError=sqlite3.DatabaseError),
            "Path": Path, "DB_FILE": str(self.path), "closing": closing,
            "nullcontext": nullcontext, "logging": logging, "asyncio": asyncio, "json": json,
            "time": types.SimpleNamespace(sleep=sleep, monotonic=time.monotonic),
            "Counter": Counter, "_SOURCE_BEARING_MEMORY_MARKERS": self.markers,
            "build_user_memory_context": self.reducer,
            "_bounded_member_memory_context": lambda metadata, **_kw: metadata["fixture_context"],
            "_user_memory_skip_reason": lambda *_a: "", "ROUTE_MODE_NORMAL_CHAT": "normal_chat",
            "memory_governance_live_enabled": lambda: False, "MEMORY_PROMPT_BUDGET_PUBLIC": 1024,
            "_named_public_recall_scope": lambda **_kw: (
                (types.SimpleNamespace(user_id=1, label_hint="Test Member"),), "neutral query", None),
            "_safe_prompt_display_label": lambda label, _default: label,
            "build_memory_prompt_source_basis": lambda context, **kw: (
                types.SimpleNamespace(user_id=kw["user_id"]) if context else None),
            "_batch_member_speaker_labels": lambda items: {item[2]: "Test Member" for item in items},
            "PUBLIC_CHAT_POLICIES": {"public_home", "public_context", "public_selective"},
            "DiscordTurnAddressing": types.SimpleNamespace,
            "is_broad_personal_recall_request": lambda _text: False,
        }
        exec(compile(ast.Module(body=self.nodes, type_ignores=[]), str(SOURCE), "exec"), self.ns)

    def reducer(self, _user, _guild, **kwargs):
        self.reducer_calls += 1
        conn = kwargs["connection"]
        self.assertTrue(conn.in_transaction)
        self.assertTrue(kwargs["read_only"])
        self.assertEqual(kwargs["source_metadata"], {})
        values = conn.execute("SELECT value FROM evidence WHERE eligible=1").fetchall()
        context = "Approved direct self-reports:\n" + values[0][0] if values else ""
        kwargs["source_metadata"].update(
            fixture_context=context, moment_gist_rendered=False, memory_context_units=(),
            eligible_count=len(values),
        )
        return context

    def assert_closed(self):
        for conn in self.opened:
            self.assertTrue(conn.closed_by_owner)
            with self.assertRaises(sqlite3.ProgrammingError):
                conn.execute("SELECT 1")

    def pending_writer(self, *, eligible=1):
        reader = CONNECT(self.path)
        reader.execute("BEGIN")
        reader.execute("SELECT value FROM evidence").fetchall()
        started = threading.Event()
        errors = []

        def write():
            try:
                with closing(CONNECT(self.path, timeout=5)) as conn, conn:
                    conn.execute("BEGIN IMMEDIATE")
                    conn.execute("UPDATE evidence SET value='neutral-corrected',eligible=?", (eligible,))
                    started.set()
                    conn.commit()
            except BaseException as exc:
                errors.append(exc)
                started.set()

        thread = threading.Thread(target=write, daemon=True)
        thread.start()
        self.assertTrue(started.wait(1))
        until = time.monotonic() + 1
        while time.monotonic() < until:
            try:
                with closing(CONNECT(self.path, timeout=.005)) as probe:
                    probe.execute("SELECT 1 FROM sqlite_master LIMIT 1").fetchone()
            except sqlite3.OperationalError as exc:
                self.assertTrue(self.ns["_sqlite_busy"](exc))
                break
            time.sleep(.005)
        else:
            self.fail("Real pending writer was not observed")

        def release():
            nonlocal reader
            if reader is not None:
                reader.rollback()
                reader.close()
                reader = None
            thread.join(2)
            self.assertFalse(thread.is_alive())
            if errors:
                raise errors[0]

        self.addCleanup(release)
        return release

    def call_owner(self, name):
        options = dict(route_mode="normal_chat", channel_policy="sealed_test", user_text="neutral query",
                       current_direct=True, governance_allowed=False, channel_id=7)
        if name == "user":
            return self.ns["_read_user_memory_snapshot"](1, 2, **options)[0]
        if name == "bounded":
            return self.ns["_read_bounded_member_memory"](
                1, 2, speaker_label="Test Member", budget_chars=1024, **options)[0]
        if name == "named":
            return self.ns["build_named_public_member_memory_context"](
                situation_frame=None, guild_id=2, route_mode="normal_chat", channel_policy="sealed_test",
                user_text="neutral query", channel_id=7)[0]
        return self.ns["build_batch_member_memory_context"](
            (("Test Member", "neutral query", 1),), guild_id=2, channel_id=7,
            channel_policy="sealed_test", route_mode="normal_chat")[0]

    def test_factory_returns_pinned_readonly_snapshot_and_missing_path_never_created(self):
        conn = self.ns["_open_member_memory_read_connection"]()
        try:
            self.assertTrue(conn.in_transaction)
            with self.assertRaises(sqlite3.OperationalError):
                conn.execute("UPDATE evidence SET eligible=0")
        finally:
            conn.close()
        self.assert_closed()
        missing = self.path.parent / "missing.sqlite"
        self.ns["DB_FILE"] = str(missing)
        with self.assertRaises(sqlite3.OperationalError):
            self.ns["_open_member_memory_read_connection"]()
        self.assertFalse(missing.exists())
        self.assertEqual(self.pauses, [])

    def test_owned_paths_share_fresh_acquisition_grace_without_replaying_source_work(self):
        for name in ("user", "bounded", "named", "multi"):
            with self.subTest(owner=name):
                self.opened = []
                self.reducer_calls = 0
                self.pauses = []
                with closing(CONNECT(self.path)) as conn, conn:
                    conn.execute("UPDATE evidence SET value='neutral-original',eligible=1")
                release = self.pending_writer()
                self.retry_hook = release
                context = self.call_owner(name)
                self.assertIn("neutral-corrected", context)
                self.assertNotIn("neutral-original", context)
                self.assertEqual((len(self.opened), self.reducer_calls, self.pauses), (2, 1, [.05]))
                self.assert_closed()
                release()

    def test_fresh_acquisition_observes_privacy_or_withdrawal_before_source_work(self):
        release = self.pending_writer(eligible=0)
        self.retry_hook = release
        context, metadata = self.ns["_read_user_memory_snapshot"](1, 2, channel_id=7)
        self.assertEqual(context, "")
        self.assertEqual(metadata["eligible_count"], 0)
        self.assertNotIn("neutral-original", repr(metadata))
        self.assertEqual(self.reducer_calls, 1)
        self.assert_closed()

    def test_exhaustion_closes_three_acquisitions_without_partial_context(self):
        release = self.pending_writer()
        with self.assertLogs(level="WARNING") as logs, self.assertRaises(sqlite3.OperationalError) as caught:
            self.ns["_read_user_memory_snapshot"](1, 2, channel_id=7)
        self.assertTrue(self.ns["_sqlite_busy"](caught.exception))
        self.assertEqual((len(self.opened), self.reducer_calls, self.pauses), (3, 0, [.05, .1]))
        self.assert_closed()
        self.assertIn("error_category=busy_or_locked", "\n".join(logs.output))
        release()

    def test_borrowed_connection_keeps_its_snapshot_and_ownership(self):
        with closing(CONNECT(self.path)) as conn:
            conn.execute("BEGIN")
            conn.execute("SELECT value FROM evidence").fetchall()
            with mock.patch.dict(self.ns, {"_open_member_memory_read_connection": mock.Mock(side_effect=AssertionError)}):
                context, _metadata = self.ns["_read_bounded_member_memory"](
                    1, 2, speaker_label="Test Member", budget_chars=1024,
                    route_mode="normal_chat", channel_policy="sealed_test", user_text="neutral query",
                    current_direct=True, governance_allowed=False, channel_id=7, connection=conn)
            self.assertIn("neutral-original", context)
            self.assertTrue(conn.in_transaction)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM evidence").fetchone()[0], 1)
        self.assertEqual(self.opened, [])

    def test_default_mid_read_busy_remains_visible_without_replaying_assembly(self):
        def fail(_user, _guild, **kwargs):
            self.reducer_calls += 1
            kwargs["source_metadata"]["abandoned"] = True
            raise sqlite3.OperationalError("database is locked")

        self.ns["build_user_memory_context"] = fail
        with self.assertRaises(sqlite3.OperationalError):
            self.ns["_read_user_memory_snapshot"](1, 2, channel_id=7)
        self.assertEqual((self.reducer_calls, len(self.opened), self.pauses), (1, 1, []))
        self.assert_closed()

    def test_direct_existing_whole_read_retry_has_no_nested_acquisition_multiplier(self):
        def fail_then_read(user, guild, **kwargs):
            if not self.reducer_calls:
                self.reducer_calls += 1
                kwargs["source_metadata"]["abandoned"] = True
                raise sqlite3.OperationalError("database is locked")
            return self.reducer(user, guild, **kwargs)

        self.ns["build_user_memory_context"] = fail_then_read
        context, metadata = self.ns["_read_user_memory_snapshot"](1, 2, busy_retries=2, channel_id=7)
        self.assertIn("neutral-original", context)
        self.assertNotIn("abandoned", metadata)
        self.assertEqual((self.reducer_calls, len(self.opened), self.pauses), (2, 2, [.05]))
        self.assert_closed()

    def test_real_nonbusy_sql_error_is_visible_and_safe_diagnostics_contain_no_message(self):
        def invalid(_user, _guild, **kwargs):
            self.reducer_calls += 1
            return kwargs["connection"].execute("SELECT * FROM private_fixture_marker_missing_table").fetchall()

        self.ns["build_user_memory_context"] = invalid
        with self.assertLogs(level="INFO") as logs, self.assertRaises(sqlite3.OperationalError) as caught:
            self.ns["_read_user_memory_snapshot"](1, 2, busy_retries=2, channel_id=7)
        self.assertFalse(self.ns["_sqlite_busy"](caught.exception))
        self.assertEqual((self.reducer_calls, len(self.opened), self.pauses), (1, 1, []))
        text = "\n".join(logs.output)
        self.assertIn("status=failed", text)
        self.assertIn("error_category=sqlite_other", text)
        self.assertNotIn("private_fixture_marker", text)
        self.assert_closed()

    def test_acquisition_nonbusy_failures_close_connected_handles_and_never_log_error_text(self):
        connect = self.ns["sqlite3"].connect

        def deny_schema(*args, **kwargs):
            conn = connect(*args, **kwargs)
            conn.set_authorizer(lambda operation, *_rest: (
                sqlite3.SQLITE_DENY if operation == sqlite3.SQLITE_READ else sqlite3.SQLITE_OK))
            return conn

        with mock.patch.object(self.ns["sqlite3"], "connect", side_effect=deny_schema), \
                self.assertLogs(level="WARNING") as logs, self.assertRaises(sqlite3.DatabaseError):
            self.ns["_open_member_memory_read_connection"]()
        self.assertEqual(len(self.opened), 1)
        self.assertEqual(self.pauses, [])
        self.assert_closed()
        self.assertIn("error_category=sqlite_other", "\n".join(logs.output))

        with mock.patch.object(self.ns["sqlite3"], "connect",
                               side_effect=OSError("private_fixture_marker")), \
                self.assertLogs(level="WARNING") as logs, self.assertRaises(OSError):
            self.ns["_open_member_memory_read_connection"]()
        text = "\n".join(logs.output)
        self.assertIn("error_category=non_sqlite", text)
        self.assertNotIn("private_fixture_marker", text)
        self.assertEqual(self.pauses, [])

    def test_async_cancel_is_explicit_and_cannot_donate_late_metadata(self):
        release = self.pending_writer()
        metadata = {"previous": True}

        async def run():
            task = asyncio.create_task(self.ns["build_user_memory_context_async"](
                1, 2, channel_id=7, source_metadata=metadata))
            while not self.opened:
                await asyncio.sleep(.005)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            release()

        with self.assertLogs(level="INFO") as logs:
            asyncio.run(run())
        self.assertEqual(metadata, {"previous": True})
        self.assertIn("status=cancelled", "\n".join(logs.output))
        self.assert_closed()


if __name__ == "__main__":
    unittest.main()
