"""Direct prompts retain the existing private memory reader under short locks."""

import asyncio
import os
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest import mock

from tests import test_public_network_knowledge as network
import bnl_memory_governance as governance
import bnl_memory_ledger as ledger


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

    def _seed_source(self, text="Copper Kite sounds funny tonight, lol?",
                     *, user_id=None, guild_id=None, channel_id=8810,
                     policy="sealed_test", role="user", audience=()):
        user_id = self.runtime.user_id if user_id is None else user_id
        guild_id = self.runtime.guild_id if guild_id is None else guild_id
        stamp = bot.datetime.now(bot.PACIFIC_TZ).isoformat()
        with closing(sqlite3.connect(bot.DB_FILE)) as conn, conn:
            ledger.ensure_memory_ledger_schema(conn)
            row_id = conn.execute(
                "INSERT INTO conversations (user_id,user_name,guild_id,channel_id,channel_policy,"
                "route_mode,role,content,timestamp) VALUES(?,?,?,?,?,?,?,?,?)",
                (user_id, "Test Member", guild_id, channel_id, policy,
                 bot.ROUTE_MODE_NORMAL_CHAT, role, text, stamp)).lastrowid
            root = ledger.shadow_conversation_row(conn, row_id=row_id,
                user_id=user_id, user_name="Test Member", guild_id=guild_id,
                channel_id=channel_id, channel_policy=policy,
                route_mode=bot.ROUTE_MODE_NORMAL_CHAT, role=role, content=text,
                observed_at=stamp, conversation_target_user_ids=audience,
                environ={"BNL_MEMORY_LEDGER_SHADOW_ENABLED": "1"}).entry_id
            for target in audience:
                conn.execute("INSERT INTO conversation_response_participants "
                    "(conversation_row_id,guild_id,user_id) VALUES(?,?,?)", (row_id, guild_id, target))
            if role == "user":
                bot._insert_memory_tier(conn.cursor(), user_id, guild_id, "long",
                    "Test Member remembers cobalt blue and the Copper Kite joke.", .95,
                    source_role="user", source_channel_policy=policy,
                    source_trust="source_safe_private", topic_key="music",
                    source_conversation_row_ids=(row_id,), source_lineage_complete=True)
        return row_id, root

    def _memory_kwargs(self):
        return dict(route_mode=bot.ROUTE_MODE_NORMAL_CHAT, channel_policy="sealed_test",
            channel_id=8810, user_text="What do you remember about Copper Kite and my favorite color?",
            current_direct=True, record_operational_diagnostics=False,
            environ={"BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED": "true"})

    def _compare_original_reader_assembly(self, conn):
        """The original default owners remain the independent assembly oracle."""
        original_relation = bot.get_relationship_state
        original_habits = bot.get_user_habits
        original_governed = bot.build_governed_context

        def defaults(owner, keyword):
            def read(*args, **kwargs):
                kwargs.pop(keyword, None)
                return owner(*args, **kwargs)
            return read

        def build():
            metadata = {}
            context = bot.build_user_memory_context(self.runtime.user_id, self.runtime.guild_id,
                connection=conn, read_only=True, source_metadata=metadata, **self._memory_kwargs())
            return context, metadata

        old_sources, new_sources, old_candidates, new_candidates = [], [], [], []

        def record(owner, outputs, *, private=False):
            def read(*args, **kwargs):
                result = owner(*args, **kwargs)
                if not private or kwargs["channel_id"] > 0:
                    outputs.append(result)
                return result
            return read

        with mock.patch.object(bot, "get_relationship_state", side_effect=defaults(original_relation, "_private_source_reader")), \
                mock.patch.object(bot, "get_user_habits", side_effect=defaults(original_habits, "_private_source_reader")), \
                mock.patch.object(bot, "build_governed_context", side_effect=defaults(original_governed, "_sealed_candidate_reader")), \
                mock.patch.object(bot, "private_conversation_sources", side_effect=record(bot.private_conversation_sources, old_sources, private=True)) as old_private, \
                mock.patch.object(bot, "sealed_tier_candidates", side_effect=record(bot.sealed_tier_candidates, old_candidates)) as old_tier, \
                mock.patch.object(governance, "sealed_tier_candidates", side_effect=record(governance.sealed_tier_candidates, old_candidates)) as old_gov_tier:
            original = build()
        with mock.patch.object(bot, "private_conversation_sources", side_effect=record(bot.private_conversation_sources, new_sources, private=True)) as new_private, \
                mock.patch.object(bot, "sealed_tier_candidates", side_effect=record(bot.sealed_tier_candidates, new_candidates)) as new_tier, \
                mock.patch.object(governance, "sealed_tier_candidates", side_effect=record(governance.sealed_tier_candidates, new_candidates)) as new_gov_tier:
            reused = build()
        self.assertEqual(original, reused)  # Complete text, selected DTOs, exclusions and basis digest.
        positive_calls = lambda reader: [call for call in reader.call_args_list if call.kwargs["channel_id"] > 0]
        self.assertEqual(len(positive_calls(old_private)), 2)
        self.assertEqual(len(positive_calls(new_private)), 1)
        self.assertEqual(old_tier.call_count + old_gov_tier.call_count, 2)
        self.assertEqual(new_tier.call_count + new_gov_tier.call_count, 1)
        self.assertEqual(old_sources, new_sources * 2)
        self.assertEqual(old_candidates, new_candidates * 2)
        self.validated_sources = new_sources[0]
        # Adaptive sizing must still use its original public-baseline readers.
        self.assertEqual([call.kwargs["channel_id"] for call in old_private.call_args_list].count(0), 2)
        self.assertEqual([call.kwargs["channel_id"] for call in new_private.call_args_list].count(0), 2)
        return reused

    async def test_complete_assembly_matches_original_readers_with_one_validated_read_each(self):
        row_id, root = self._seed_source()
        self._seed_source("My favorite color is cobalt blue.")
        self._seed_source(user_id=101)
        self._seed_source(guild_id=7701)
        self._seed_source(channel_id=8811)
        self._seed_source(policy="public_home")
        self._seed_source(role="model", audience=(self.runtime.user_id,))
        with closing(bot._open_member_memory_read_connection()) as conn:
            conn.execute("BEGIN")
            context, metadata = self._compare_original_reader_assembly(conn)
        self.assertIn("cobalt blue", context)
        self.assertTrue(metadata["governed_basis_digest"])
        self.assertIn(root, [source["entry_id"] for source in self.validated_sources])
        self.assertEqual(len(self.validated_sources), 2)  # This member's user and single-audience model only.
        self.assertTrue(metadata["legacy_relationship_present"])

    async def test_fresh_assembly_rechecks_correction_deletion_privacy_audience_and_revision(self):
        def snapshot():
            with closing(bot._open_member_memory_read_connection()) as conn:
                conn.execute("BEGIN")
                return self._compare_original_reader_assembly(conn)
        for name in ("revision", "audience", "correction", "privacy", "delete"):
            row_id, root = self._seed_source(role="model" if name == "audience" else "user",
                audience=(self.runtime.user_id,) if name == "audience" else ())
            previous = snapshot()
            self.assertIn(root, [source["entry_id"] for source in self.validated_sources])
            await asyncio.sleep(0)  # No build-local tuple may survive this fresh external read.
            with closing(sqlite3.connect(bot.DB_FILE)) as conn, conn:
                if name == "revision":
                    cursor = conn.execute("SELECT * FROM memory_ledger_entries WHERE entry_id=?", (root,))
                    values = dict(zip((column[0] for column in cursor.description), cursor.fetchone()))
                    values.update(entry_id="aaa-revision", source_revision="fresh-revision")
                    conn.execute("INSERT INTO memory_ledger_entries (" + ",".join(values) + ") VALUES ("
                        + ",".join("?" for _ in values) + ")", tuple(values.values()))
                elif name == "audience":
                    conn.execute("INSERT INTO conversation_response_participants "
                        "(conversation_row_id,guild_id,user_id) VALUES(?,?,?)", (row_id, self.runtime.guild_id, 101))
                elif name == "correction":
                    conn.execute("INSERT INTO memory_ledger_lineage VALUES(?,?,?,?,?)",
                        ("correction", self.runtime.guild_id, "correction_of", root, "now"))
                elif name == "privacy":
                    conn.execute("UPDATE conversations SET channel_policy='internal_controlled' WHERE id=?", (row_id,))
                else:
                    conn.execute("DELETE FROM conversations WHERE id=?", (row_id,))
            with self.subTest(mutation=name):
                current = snapshot()
                self.assertNotIn(root, [source["entry_id"] for source in self.validated_sources])
                if name == "revision":
                    self.assertIn("fresh-revision", [source["ledger"]["source_revision"] for source in self.validated_sources])
                if name == "audience":
                    self.assertNotEqual(previous[0], current[0])

    async def test_scope_and_transaction_mismatches_use_original_readers(self):
        self._seed_source()
        captured = {}
        original_relation = bot.get_relationship_state
        original_governed = bot.build_governed_context

        def relation(*args, **kwargs):
            if "_private_source_reader" in kwargs:
                captured["private"] = kwargs["_private_source_reader"]
            return original_relation(*args, **kwargs)

        def governed(*args, **kwargs):
            captured["tier"] = kwargs["_sealed_candidate_reader"]
            return original_governed(*args, **kwargs)

        with closing(sqlite3.connect(bot.DB_FILE)) as conn, \
                mock.patch.object(bot, "get_relationship_state", side_effect=relation), \
                mock.patch.object(bot, "build_governed_context", side_effect=governed):
            conn.execute("BEGIN")
            bot.build_user_memory_context(self.runtime.user_id, self.runtime.guild_id,
                connection=conn, read_only=True, **self._memory_kwargs())
            private = captured["private"]
            tier = captured["tier"]
            scope = dict(guild_id=self.runtime.guild_id, user_id=self.runtime.user_id, channel_id=8810)
            request = bot.GovernanceRequest(self.runtime.guild_id, self.runtime.user_id,
                bot.ROUTE_MODE_NORMAL_CHAT, "test", channel_id=8810, channel_policy="sealed_test")
            with mock.patch.object(bot, "private_conversation_sources", wraps=bot.private_conversation_sources) as real_private, \
                    mock.patch.object(bot, "sealed_tier_candidates", wraps=bot.sealed_tier_candidates) as real_tier:
                private(conn, **scope)
                tier(conn, request, bot.extract_user_facts)
                real_private.assert_not_called()
                real_tier.assert_not_called()
                for key in scope:
                    private(conn, **dict(scope, **{key: scope[key] + 1}))
                from dataclasses import replace
                for field, value in (("guild_id", 7701), ("subject_user_id", 101),
                                     ("channel_id", 8811), ("channel_policy", "public_home")):
                    tier(conn, replace(request, **{field: value}), bot.extract_user_facts)
                tier(conn, request, lambda text: bot.extract_user_facts(text))
                self.assertEqual(real_private.call_count, 3)
                self.assertEqual(real_tier.call_count, 5)
                with closing(sqlite3.connect(bot.DB_FILE)) as other:
                    private(other, **scope)
                    tier(other, request, bot.extract_user_facts)
                self.assertEqual(real_private.call_count, 4)
                self.assertEqual(real_tier.call_count, 6)
                conn.rollback()
                private(conn, **scope)  # Loss of the pinned transaction permanently invalidates both tuples.
                conn.execute("BEGIN")
                private(conn, **scope)
                tier(conn, request, bot.extract_user_facts)
                self.assertEqual(real_private.call_count, 6)
                self.assertEqual(real_tier.call_count, 7)

    async def test_own_write_and_failed_read_never_supply_previous_tuple(self):
        row_id, _root = self._seed_source()
        original_sources = bot.private_conversation_sources
        original_journal = bot.get_relationship_journal
        calls = []

        def sources(conn, **kwargs):
            if kwargs["channel_id"] > 0:
                calls.append(kwargs)
                if len(calls) == 1:
                    raise sqlite3.OperationalError("fixture read failure")
            return original_sources(conn, **kwargs)

        with closing(bot._open_member_memory_read_connection()) as conn, \
                mock.patch.object(bot, "private_conversation_sources", side_effect=sources):
            conn.execute("BEGIN")
            bot.build_user_memory_context(self.runtime.user_id, self.runtime.guild_id,
                connection=conn, read_only=True, **self._memory_kwargs())
        self.assertEqual(len(calls), 2)  # The failed Relationship read is not cached for habits.

        def revoke_between_reducers(*args, **kwargs):
            kwargs["connection"].execute("UPDATE conversations SET channel_policy='internal_controlled' WHERE id=?", (row_id,))
            return original_journal(*args, **kwargs)

        with closing(sqlite3.connect(bot.DB_FILE)) as conn, \
                mock.patch.object(bot, "get_relationship_journal", side_effect=revoke_between_reducers), \
                mock.patch.object(bot, "private_conversation_sources", wraps=original_sources) as real_private, \
                mock.patch.object(bot, "sealed_tier_candidates", wraps=bot.sealed_tier_candidates) as real_tier:
            conn.execute("BEGIN")
            bot.build_user_memory_context(self.runtime.user_id, self.runtime.guild_id,
                connection=conn, read_only=True, **self._memory_kwargs())
            positive = [call for call in real_private.call_args_list if call.kwargs["channel_id"] > 0]
            self.assertEqual(len(positive), 2)
            self.assertEqual(real_tier.call_count, 2)
            self.assertEqual(original_sources(conn, guild_id=self.runtime.guild_id,
                user_id=self.runtime.user_id, channel_id=8810), ())

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
        self.assertEqual(len(snapshots), 1)
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
