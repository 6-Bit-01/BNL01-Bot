"""Mandatory guild configuration stays fresh under bounded SQLite contention.

Execute the actual configuration owner and Discord intake function with real
SQLite locks. Later capture, generation and delivery are isolated boundaries;
an unavailable mandatory read must never reach them or invent configuration.
"""

import ast
import asyncio
import logging
import re
from functools import wraps
from pathlib import Path
import sqlite3
import tempfile
import threading
import time
from contextlib import closing
from types import SimpleNamespace
import unittest
from unittest import mock


SOURCE = Path(__file__).resolve().parents[1] / "bnl01_bot.py"
TREE = ast.parse(SOURCE.read_text(encoding="utf-8"))
FUNCTIONS = {
    "get_guild_config", "_sqlite_busy", "on_message",
    "_guard_direct_payload_capture_ingress", "_direct_session_key",
    "_finish_direct_payload_capture_handoff", "_declared_canon_command_match",
}
OWNERS = [node for node in TREE.body
          if (
              isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
              and node.name in FUNCTIONS
          ) or (
              isinstance(node, ast.Assign)
              and any(isinstance(target, ast.Name) and (
                  target.id == "SOURCE_INTERNAL_MODES"
                  or (target.id.startswith("ROUTE_MODE_") and isinstance(node.value, ast.Constant))
              ) for target in node.targets)
          )]
MODULE = ast.Module(body=[ast.ImportFrom(
    module="__future__", names=[ast.alias(name="annotations")], level=0,
), *OWNERS], type_ignores=[])
CODE = compile(ast.fix_missing_locations(MODULE), str(SOURCE), "exec")


class TrackedConnection(sqlite3.Connection):
    closed_by_owner = False

    def close(self):
        super().close()
        self.closed_by_owner = True


class ConfigReadFinished(Exception):
    """Stop intake after its real config read and routing decision."""


class GuildConfigReadAvailabilityTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = str(Path(directory.name) / "intake.db")
        with closing(sqlite3.connect(self.path)) as conn, conn:
            conn.execute("CREATE TABLE guild_configs(guild_id INTEGER PRIMARY KEY, active_channel_id INTEGER)")
            conn.execute("INSERT INTO guild_configs VALUES(7700,8811)")
        self.opened = []
        self.timeouts = []
        self.retry_seen = threading.Event()
        self.first_retry_release = None
        self.worker_threads = []
        self.delays = []
        self.ns = dict(
            sqlite3=SimpleNamespace(connect=self.connect,
                                    OperationalError=sqlite3.OperationalError),
            closing=closing, asyncio=asyncio, logging=logging,
            re=re, wraps=wraps,
            _direct_payload_capture_waiters={},
            # Pure route/payload inputs are outside this configuration-owner
            # fixture. The actual capture wrapper and cleanup owner execute.
            classify_route_mode=mock.Mock(return_value="normal_chat"),
            _detect_request_payload_expectation=mock.Mock(return_value=(False, "")),
            _collect_inline_direct_payload_items=mock.Mock(return_value=[]),
            time=SimpleNamespace(sleep=self.pause), DB_FILE=self.path,
            client=SimpleNamespace(user=object(), event=lambda handler: handler),
            channel_observation_expected=lambda _channel: False,
            is_community_image_channel=lambda _channel: False,
            maybe_handle_declared_canon_command=mock.AsyncMock(return_value=False),
            _parse_journal_command=lambda _text: (False, {}, None),
            _register_direct_conversation_ingress=mock.Mock(return_value={}),
            resolve_channel_policy=lambda _channel: "public_home",
            upsert_user_profile=mock.Mock(),
            conversation_surface_for_channel_policy=mock.Mock(side_effect=ConfigReadFinished),
            record_recent_room_event_from_message=mock.Mock(),
            save_user_message=mock.Mock(),
            record_passive_user_activity=mock.Mock(),
            get_gemini_response_with_optional_typing=mock.AsyncMock(),
            send_planned_conversation_response=mock.AsyncMock(),
        )
        exec(CODE, self.ns)
        self.message = SimpleNamespace(
            id=9001, content="BNL, answer this request.",
            guild=SimpleNamespace(id=7700),
            channel=SimpleNamespace(id=8810),
            author=SimpleNamespace(id=100, display_name="Test Member", bot=False),
            replies=[],
        )

    def connect(self, *args, **kwargs):
        kwargs["factory"] = TrackedConnection
        conn = sqlite3.connect(*args, **kwargs)
        self.opened.append(conn)
        self.timeouts.append(kwargs.get("timeout"))
        self.worker_threads.append(threading.get_ident())
        return conn

    def pause(self, delay):
        self.delays.append(delay)
        self.retry_seen.set()
        if self.first_retry_release is not None and len(self.delays) == 1:
            # Let the async observer release the real lock before this worker
            # resumes. Scheduler load must not decide whether the lock fixture
            # outlives the production retry window.
            if not self.first_retry_release.wait(10):
                raise AssertionError("Discord event loop could not release config retry")
        time.sleep(delay)

    def assert_owned_reads_closed(self):
        self.assertTrue(self.opened)
        self.assertTrue(all(conn.closed_by_owner for conn in self.opened))

    def assert_no_capture_generation_or_delivery(self):
        for name in ("record_recent_room_event_from_message", "save_user_message",
                     "record_passive_user_activity"):
            self.ns[name].assert_not_called()
        self.ns["get_gemini_response_with_optional_typing"].assert_not_awaited()
        self.ns["send_planned_conversation_response"].assert_not_awaited()
        self.assertEqual(self.message.replies, [])

    async def test_actual_intake_recovers_released_lock_and_uses_new_configuration(self):
        main_thread = threading.get_ident()
        self.first_retry_release = threading.Event()
        with closing(sqlite3.connect(self.path)) as writer:
            writer.execute("BEGIN EXCLUSIVE")
            writer.execute("UPDATE guild_configs SET active_channel_id=8810 WHERE guild_id=7700")
            task = asyncio.create_task(self.ns["on_message"](self.message))
            ticks = 0
            try:
                async def observe_retry():
                    nonlocal ticks
                    while not self.retry_seen.is_set() and not task.done():
                        await asyncio.sleep(0.01)
                    # This independent async observer must execute while the
                    # retry worker remains paused, even when the worker reached
                    # its barrier before this observer was first scheduled.
                    ticks += 1

                await asyncio.wait_for(observe_retry(), timeout=10)
                self.assertFalse(task.done(), "mandatory intake failed instead of retrying its config read")
                self.assert_owned_reads_closed()
                self.assertGreaterEqual(ticks, 1, "config wait blocked other Discord events")
                writer.commit()
                self.first_retry_release.set()
                with self.assertRaises(ConfigReadFinished):
                    await asyncio.wait_for(task, timeout=10)
            finally:
                writer.rollback()
                self.first_retry_release.set()
                await asyncio.gather(task, return_exceptions=True)
        self.ns["conversation_surface_for_channel_policy"].assert_called_once_with("public_home", True)
        self.ns["upsert_user_profile"].assert_called_once_with(100, 7700, "Test Member")
        self.assertEqual(len(self.opened), 2)
        self.assertEqual(self.timeouts, [0.01, 0.01])
        self.assertEqual(self.delays, [0.05])
        self.assertTrue(all(thread_id != main_thread for thread_id in self.worker_threads))
        self.assert_owned_reads_closed()
        self.assert_no_capture_generation_or_delivery()

    async def test_actual_intake_sustained_lock_fails_closed_with_no_later_work(self):
        with closing(sqlite3.connect(self.path)) as writer:
            writer.execute("BEGIN EXCLUSIVE")
            with self.assertRaises(sqlite3.OperationalError) as caught:
                await asyncio.wait_for(self.ns["on_message"](self.message), timeout=10)
            writer.rollback()
        self.assertTrue(self.ns["_sqlite_busy"](caught.exception))
        self.assertEqual(len(self.opened), 3)
        self.assertEqual(self.timeouts, [0.01, 0.01, 5])
        self.assertEqual(self.delays, [0.05, 0.1])
        self.ns["upsert_user_profile"].assert_not_called()
        self.ns["conversation_surface_for_channel_policy"].assert_not_called()
        self.assert_owned_reads_closed()
        self.assert_no_capture_generation_or_delivery()

    async def test_intake_survives_reader_and_pending_commit_beyond_quick_retry_window(self):
        # A long background read can block another worker's COMMIT. In DELETE
        # mode that pending writer also blocks new readers, including intake.
        # Exercise that actual interaction without pausing/mocking the reader
        # owner, its retry clock, SQLite, or the Discord event loop.
        staged = threading.Event()
        committed = threading.Event()
        writer_errors = []

        def write_new_configuration():
            try:
                with closing(sqlite3.connect(self.path, timeout=8)) as writer:
                    writer.execute("BEGIN IMMEDIATE")
                    writer.execute("UPDATE guild_configs SET active_channel_id=8810 WHERE guild_id=7700")
                    staged.set()
                    writer.commit()
                    committed.set()
            except BaseException as exc:
                writer_errors.append(exc)
                staged.set()

        def pending_commit_blocks_new_reader():
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                try:
                    with closing(sqlite3.connect(self.path, timeout=0)) as probe:
                        probe.execute("SELECT active_channel_id FROM guild_configs").fetchall()
                except sqlite3.OperationalError as exc:
                    if self.ns["_sqlite_busy"](exc):
                        return True
                    raise
                time.sleep(0.005)
            return False

        task = None
        with closing(sqlite3.connect(self.path)) as background_reader:
            self.assertEqual(background_reader.execute("PRAGMA journal_mode").fetchone()[0], "delete")
            background_reader.execute("BEGIN")
            self.assertEqual(background_reader.execute(
                "SELECT active_channel_id FROM guild_configs WHERE guild_id=7700"
            ).fetchone()[0], 8811)
            worker = threading.Thread(target=write_new_configuration)
            worker.start()
            try:
                self.assertTrue(await asyncio.to_thread(staged.wait, 5))
                self.assertFalse(writer_errors)
                self.assertTrue(await asyncio.to_thread(pending_commit_blocks_new_reader))
                task = asyncio.create_task(self.ns["on_message"](self.message))

                async def observe_pending_commit_retry():
                    while not self.retry_seen.is_set() and not task.done():
                        await asyncio.sleep(0.005)

                await asyncio.wait_for(observe_pending_commit_retry(), 5)
                self.assertTrue(self.retry_seen.is_set())

                async def observe_final_read_attempt():
                    while len(self.opened) < 3 and not task.done():
                        await asyncio.sleep(0.005)

                await asyncio.wait_for(observe_final_read_attempt(), 5)
                self.assertEqual(len(self.opened), 3)
                # Exceed the old nominal 180ms retry budget. Native SQLite
                # waits and OS scheduling may add platform-dependent latency.
                await asyncio.sleep(0.35)
                self.assertFalse(task.done(), "a temporary pending commit discarded the message")
                self.assertFalse(committed.is_set())
                self.assertEqual(background_reader.execute(
                    "SELECT active_channel_id FROM guild_configs WHERE guild_id=7700"
                ).fetchone()[0], 8811)
                background_reader.rollback()
                with self.assertRaises(ConfigReadFinished):
                    await asyncio.wait_for(task, 8)
            finally:
                background_reader.rollback()
                await asyncio.to_thread(worker.join, 9)
                if task is not None:
                    await asyncio.gather(task, return_exceptions=True)
        self.assertFalse(worker.is_alive())
        self.assertFalse(writer_errors)
        self.assertTrue(committed.is_set())
        self.ns["conversation_surface_for_channel_policy"].assert_called_once_with("public_home", True)
        self.ns["upsert_user_profile"].assert_called_once_with(100, 7700, "Test Member")
        self.assertEqual(self.timeouts, [0.01, 0.01, 5])
        self.assert_owned_reads_closed()
        self.assert_no_capture_generation_or_delivery()

    async def test_unrelated_sql_error_is_visible_without_retry_or_later_work(self):
        with closing(sqlite3.connect(self.path)) as conn, conn:
            conn.execute("DROP TABLE guild_configs")
        with self.assertRaisesRegex(sqlite3.OperationalError, "no such table: guild_configs"):
            await self.ns["on_message"](self.message)
        self.assertEqual(len(self.opened), 1)
        self.assertEqual(self.delays, [])
        self.ns["upsert_user_profile"].assert_not_called()
        self.assert_owned_reads_closed()
        self.assert_no_capture_generation_or_delivery()

    async def test_corrupt_database_is_fatal_without_retry_or_later_work(self):
        Path(self.path).write_bytes(b"invalid fixture database")
        with self.assertRaisesRegex(sqlite3.DatabaseError, "file is not a database"):
            await self.ns["on_message"](self.message)
        self.assertEqual(len(self.opened), 1)
        self.assertEqual(self.delays, [])
        self.ns["upsert_user_profile"].assert_not_called()
        self.assert_owned_reads_closed()
        self.assert_no_capture_generation_or_delivery()

    async def test_unconfigured_guild_remains_distinct_from_unavailable_read(self):
        self.assertIsNone(self.ns["get_guild_config"](9900))
        self.assertEqual(self.ns["get_guild_config"](7700), 8811)
        self.assertEqual(len(self.opened), 2)
        self.assertEqual(self.delays, [])
        self.assertEqual(self.timeouts, [5, 5])
        self.assert_owned_reads_closed()

    async def test_deferred_capture_reservation_is_released_after_fatal_config_read(self):
        self.ns["_detect_request_payload_expectation"].return_value = (True, "people")
        self.message.content = "BNL, tell me something about each of these people"
        entered = []
        handler = self.ns["maybe_handle_declared_canon_command"]

        async def observe_reservation(*_args):
            entered.extend(self.ns["_direct_payload_capture_waiters"].values())
            return False

        handler.side_effect = observe_reservation
        Path(self.path).write_bytes(b"invalid fixture database")
        with self.assertRaisesRegex(sqlite3.DatabaseError, "file is not a database"):
            await self.ns["on_message"](self.message)
        self.assertEqual(len(entered), 1)
        self.assertTrue(entered[0]["event"].is_set())
        self.assertEqual(self.ns["_direct_payload_capture_waiters"], {})
        self.assert_owned_reads_closed()
        self.assert_no_capture_generation_or_delivery()


if __name__ == "__main__":
    unittest.main()
