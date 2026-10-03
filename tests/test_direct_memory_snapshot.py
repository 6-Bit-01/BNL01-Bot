"""Direct prompts retain the existing private memory reader under short locks."""

import os
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest import mock

from tests import test_public_network_knowledge as network


bot = network.bnl01_bot


class CloseTrackedConnection(sqlite3.Connection):
    """Observe native close success without cross-thread queries or cleanup."""
    closed_by_owner = False

    def close(self):
        super().close()
        self.closed_by_owner = True


class DirectMemorySnapshotTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.runtime = network.PublicNetworkKnowledgeTests()
        setup_connections = []
        connect = sqlite3.connect

        def fixture_connect(*args, **kwargs):
            conn = connect(*args, **kwargs)
            setup_connections.append(conn)
            return conn

        # Reused schema/tier fixture owners commit with Connection.__exit__,
        # which does not close SQLite handles. Finish only fixture setup's
        # ownership here; tested prompt/snapshot handles are never swept up.
        try:
            with mock.patch.object(sqlite3, "connect", side_effect=fixture_connect):
                await self.runtime.asyncSetUp()
        finally:
            for conn in setup_connections:
                conn.close()
        # This fixture isolates the real memory/Relationship owner. Publication
        # freshness has separate integration coverage and remains in full CI.
        self.runtime.stack.enter_context(mock.patch.object(
            bot, "build_publication_prompt_source_bases", return_value=()))

        def tracked_connect(*args, **kwargs):
            kwargs.setdefault("factory", CloseTrackedConnection)
            return connect(*args, **kwargs)

        # The actual reader keeps its URI, timeout and thread rules. Only
        # successful native closure is observed; tests do not close its handles.
        self.runtime.stack.enter_context(mock.patch.object(
            sqlite3, "connect", side_effect=tracked_connect))
        self.addAsyncCleanup(self.runtime.asyncTearDown)

    async def test_direct_private_relationship_reader_has_one_read_only_snapshot(self):
        original = bot.private_conversation_sources
        snapshots = []

        def sources(conn, **kwargs):
            if kwargs["channel_id"] > 0:
                # This is the real Relationship/habit caller, which previously
                # reopened private sources without owning a read transaction.
                if not conn.in_transaction:
                    raise sqlite3.OperationalError("database is locked")
                snapshots.append(conn)
                with self.assertRaises(sqlite3.OperationalError):
                    conn.execute("CREATE TABLE forbidden_memory_write(value TEXT)")
            return original(conn, **kwargs)

        inputs = self.runtime._direct_prompt_inputs("sealed_test", privileged=True)
        with mock.patch.object(bot, "private_conversation_sources", side_effect=sources):
            prompt, *_ = await bot.build_user_aware_prompt_async(**inputs)
        self.assertIn(network.PUBLIC_MEMORY, prompt)
        self.assertNotIn(network.INTERNAL_MEMORY, prompt)
        self.assertNotIn(network.SEALED_MEMORY, prompt)
        self.assertGreaterEqual(len(snapshots), 2)
        self.assertTrue(all(conn is snapshots[0] for conn in snapshots))
        memory_bases = [basis for basis in inputs["prompt_metadata"]["prompt_source_bases"]
                        if isinstance(basis, bot.MemoryPromptSourceBasis)]
        self.assertEqual(len(memory_bases), 1)
        self.assertEqual(memory_bases[0].channel_policy, "sealed_test")
        self.assertEqual(memory_bases[0].channel_id, inputs["channel_id"])
        self.assertTrue(memory_bases[0].current_direct)
        self.assertFalse(memory_bases[0].is_owner_or_mod)
        self.assertTrue(snapshots[0].closed_by_owner)

    async def test_exhausted_direct_read_does_not_publish_partial_metadata(self):
        inputs = self.runtime._direct_prompt_inputs("sealed_test", privileged=False)
        inputs["prompt_metadata"].update(previous="untouched")
        retained = sqlite3.OperationalError("database is locked")
        connections = []

        def fail(*_args, **kwargs):
            connections.append(kwargs["connection"])
            kwargs["source_metadata"].update(governed_basis_digest="abandoned")
            raise retained

        with mock.patch.object(bot, "build_user_memory_context", side_effect=fail) as reader, \
                mock.patch.object(bot.time, "sleep") as pause, \
                self.assertRaises(sqlite3.OperationalError) as caught:
            await bot.build_user_aware_prompt_async(**inputs)
        self.assertIs(caught.exception, retained)
        self.assertEqual(reader.call_count, 3)
        self.assertEqual(pause.call_count, 2)
        self.assertEqual(inputs["prompt_metadata"], {"previous": "untouched"})
        self.assertEqual(len({id(conn) for conn in connections}), 3)
        for conn in connections:
            self.assertTrue(conn.closed_by_owner)


class WholeMemoryReadRetryTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = os.path.join(directory.name, "memory.db")
        patch = mock.patch.object(bot, "DB_FILE", self.path)
        patch.start()
        self.addCleanup(patch.stop)
        with closing(sqlite3.connect(self.path)) as conn, conn:
            conn.execute("CREATE TABLE evidence(value TEXT)")
            conn.execute("INSERT INTO evidence VALUES('old')")

    def test_whole_read_retry_closes_failed_snapshot_and_discards_partial_metadata(self):
        connections = []

        def read(_user, _guild, **kwargs):
            conn = kwargs["connection"]
            connections.append(conn)
            self.assertTrue(conn.in_transaction)
            self.assertTrue(kwargs["read_only"])
            self.assertEqual(kwargs["source_metadata"], {})
            value = conn.execute("SELECT value FROM evidence").fetchone()[0]
            kwargs["source_metadata"].update(governed_basis_digest=value)
            if len(connections) == 1:
                kwargs["source_metadata"]["partial"] = "must disappear"
                raise sqlite3.OperationalError("database is locked")
            return "Memory: " + value

        def released_before_pause(_delay):
            with self.assertRaises(sqlite3.ProgrammingError):
                connections[-1].execute("SELECT 1")
            with closing(sqlite3.connect(self.path, timeout=0.01)) as writer, writer:
                writer.execute("UPDATE evidence SET value='corrected'")

        with mock.patch.object(bot, "build_user_memory_context", side_effect=read) as reader, \
                mock.patch.object(bot.time, "sleep", side_effect=released_before_pause) as pause:
            context, metadata = bot._read_user_memory_snapshot(42, 1, busy_retries=2,
                channel_policy="sealed_test", channel_id=7, current_direct=True)
        self.assertEqual(context, "Memory: corrected")
        self.assertEqual(metadata, {"governed_basis_digest": "corrected"})
        self.assertEqual(reader.call_count, 2)
        self.assertEqual(pause.call_count, 1)
        self.assertEqual(len({id(conn) for conn in connections}), 2)
        for conn in connections:
            with self.assertRaises(sqlite3.ProgrammingError):
                conn.execute("SELECT 1")

    def test_retry_can_recover_initial_real_exclusive_lock_without_reading_empty_memory(self):
        opened = []
        writer = sqlite3.connect(self.path)
        self.addCleanup(writer.close)
        writer.execute("BEGIN EXCLUSIVE")

        def open_read():
            conn = sqlite3.connect(Path(self.path).resolve().as_uri() + "?mode=ro",
                                  uri=True, timeout=0.01)
            opened.append(conn)
            return conn

        def release(_delay):
            with self.assertRaises(sqlite3.ProgrammingError):
                opened[-1].execute("SELECT 1")
            writer.rollback()

        def read(_user, _guild, **kwargs):
            return kwargs["connection"].execute("SELECT value FROM evidence").fetchone()[0]

        with mock.patch.object(bot, "_open_member_memory_read_connection", side_effect=open_read), \
                mock.patch.object(bot, "build_user_memory_context", side_effect=read) as reader, \
                mock.patch.object(bot.time, "sleep", side_effect=release):
            context, metadata = bot._read_user_memory_snapshot(
                42, 1, busy_retries=2, channel_policy="public_home")
        self.assertEqual((context, metadata), ("old", {}))
        self.assertEqual(len(opened), 2)
        reader.assert_called_once()
        for conn in opened:
            with self.assertRaises(sqlite3.ProgrammingError):
                conn.execute("SELECT 1")

    def test_busy_exhaustion_preserves_error_and_closes_every_attempt(self):
        retained = sqlite3.OperationalError("database is locked")
        opened = []

        def fail(_user, _guild, **kwargs):
            opened.append(kwargs["connection"])
            raise retained

        with mock.patch.object(bot, "build_user_memory_context", side_effect=fail) as reader, \
                mock.patch.object(bot.time, "sleep") as pause, \
                self.assertRaises(sqlite3.OperationalError) as caught:
            bot._read_user_memory_snapshot(
                42, 1, busy_retries=2, channel_policy="public_home")
        self.assertIs(caught.exception, retained)
        self.assertEqual(reader.call_count, 3)
        self.assertEqual(pause.call_count, 2)
        for conn in opened:
            with self.assertRaises(sqlite3.ProgrammingError):
                conn.execute("SELECT 1")

    def test_nonbusy_errors_are_never_retried(self):
        for retained in (sqlite3.OperationalError("fixture unavailable"),
                         sqlite3.DatabaseError("fixture corrupted"), ValueError("fixture invalid")):
            with self.subTest(error=type(retained).__name__), \
                    mock.patch.object(bot, "build_user_memory_context", side_effect=retained) as reader, \
                    mock.patch.object(bot.time, "sleep") as pause, \
                    self.assertRaises(type(retained)) as caught:
                bot._read_user_memory_snapshot(
                    42, 1, busy_retries=2, channel_policy="public_home")
            self.assertIs(caught.exception, retained)
            reader.assert_called_once()
            pause.assert_not_called()

    def test_existing_snapshot_callers_keep_one_attempt_by_default(self):
        retained = sqlite3.OperationalError("database is locked")
        with mock.patch.object(bot, "build_user_memory_context", side_effect=retained) as reader, \
                mock.patch.object(bot.time, "sleep") as pause, \
                self.assertRaises(sqlite3.OperationalError) as caught:
            bot._read_user_memory_snapshot(42, 1, channel_policy="public_home")
        self.assertIs(caught.exception, retained)
        reader.assert_called_once()
        pause.assert_not_called()

    def test_intentionally_skipped_routes_never_open_a_database(self):
        cases = [(bot.ROUTE_MODE_SIMPLE_GREETING, "public_home", "simple greeting")]
        cases.extend((route, "public_home", "No route-safe")
                     for route in bot.SOURCE_INTERNAL_MODES)
        cases.extend((bot.ROUTE_MODE_NORMAL_CHAT, policy, "No route-safe")
                     for policy in ("unknown", "protected_system", "broadcast_memory",
                                    "reference_canon", "ai_image_tool"))
        for route, policy, expected in cases:
            with self.subTest(route=route, policy=policy), \
                    mock.patch.object(bot, "_open_member_memory_read_connection") as opened, \
                    mock.patch.object(bot.time, "sleep") as pause:
                context, metadata = bot._read_user_memory_snapshot(
                    42, 1, busy_retries=2, skip_read_if_unused=True,
                    route_mode=route, channel_policy=policy)
            self.assertIn(expected, context)
            self.assertFalse(metadata["legacy_memory_present"])
            self.assertEqual(metadata["prompt_budget"], 0)
            opened.assert_not_called()
            pause.assert_not_called()


if __name__ == "__main__":
    unittest.main()
