"""Mandatory guild configuration stays fresh under bounded SQLite contention.

Execute the actual configuration owner and Discord intake function with real
SQLite locks. Later capture, generation and delivery are isolated boundaries;
an unavailable mandatory read must never reach them or invent configuration.
"""

import ast
import asyncio
import logging
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
FUNCTIONS = {"get_guild_config", "_sqlite_busy", "on_message"}
OWNERS = [node for node in TREE.body
          if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
          and node.name in FUNCTIONS]
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
        self.assertEqual(self.timeouts, [0.01, 0.01, 0.01])
        self.assertEqual(self.delays, [0.05, 0.1])
        self.ns["upsert_user_profile"].assert_not_called()
        self.ns["conversation_surface_for_channel_policy"].assert_not_called()
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


if __name__ == "__main__":
    unittest.main()
