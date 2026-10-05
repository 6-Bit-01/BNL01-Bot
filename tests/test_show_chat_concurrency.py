"""Real chat capture must not wait for detached show CPU assembly.

Only temporary fixture databases are used. The real capture transaction,
source readers, builder, seal, graph projector and sync owner all execute.
Events hold CPU assembly without simulating SQLite lock outcomes.
"""

from __future__ import annotations

import copy
import gc
import os
import sqlite3
import threading
import unittest
from contextlib import closing
from pathlib import Path
from unittest import mock

import test_conversation_batching as chat_fixtures
import test_show_sync_transactions as show_fixtures
import bnl_tiktok_show_ledger as shows
import bnl_memory_governance as governance


bot = chat_fixtures.bnl01_bot


class ShowChatConcurrencyTests(unittest.TestCase):
    def setUp(self):
        self.fixture = show_fixtures.ShowSyncTransactionTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.db = self.fixture.db
        patcher = mock.patch.object(bot, "DB_FILE", self.db)
        patcher.start()
        self.addCleanup(patcher.stop)
        # Install the actual conversation schema owner before either actor
        # starts. Setup handles are explicitly closed for Windows fixtures.
        connect = sqlite3.connect
        setup_handles = []

        def setup_connect(*args, **kwargs):
            conn = connect(*args, **kwargs)
            setup_handles.append(conn)
            return conn

        try:
            with mock.patch.object(sqlite3, "connect", side_effect=setup_connect):
                bot.init_db()
        finally:
            for conn in setup_handles:
                conn.close()
        if os.name == "nt":
            self.addCleanup(gc.collect)
        self.fixture.sync()
        self.previous = self.show_graph()
        self.model = copy.deepcopy(self.fixture.model)
        self.model["sections"]["archive"]["latestShow"]["title"] = "Corrected fictional episode"

    def capture(self, message_id=88001):
        writer = bot.save_user_message
        if os.name == "nt":
            # Native Linux CI exercises the real file privacy fence. Windows
            # still runs the original SQLite capture/projection writer body.
            writer = writer.__wrapped__
        result = writer(
            42, "Test Member", 77, "BNL, which track was just played?",
            channel_name="barcode-bot", channel_policy="public_home",
            channel_id=9001, message_id=message_id,
            route_mode="channel_observation", directed_to_bnl=True,
            source_observed_at="2026-08-29T00:05:11+00:00",
        )
        self.assertTrue(result.save_conversation)
        with closing(sqlite3.connect(self.db)) as conn:
            rows = conn.execute(
                "SELECT id FROM conversations WHERE guild_id=77 AND message_id=?",
                (message_id,),
            ).fetchall()
            self.assertEqual(len(rows), 1)
            return rows[0][0]

    def show_graph(self, path=None):
        with closing(sqlite3.connect(path or self.db)) as conn:
            ledger = tuple(conn.execute(
                "SELECT guild_id,show_key,source_digest,lifecycle_status,ledger_json "
                "FROM tiktok_show_evidence_ledgers ORDER BY guild_id,show_key"
            ))
            # Compare every graph field except administrative creation/update
            # clocks; entry IDs, source revisions, values and controls remain.
            graph = []
            for table, where, order in (
                ("memory_ledger_entries", "source_table='tiktok_show_evidence'", "entry_id"),
                ("memory_ledger_lineage", "entry_id IN (SELECT entry_id FROM memory_ledger_entries "
                 "WHERE source_table='tiktok_show_evidence')", "entry_id,lineage_type,target_entry_id"),
            ):
                columns = tuple(row[1] for row in conn.execute("PRAGMA table_info(" + table + ")")
                                if row[1] not in {"created_at", "updated_at"})
                graph.append((columns, tuple(conn.execute(
                    "SELECT " + ",".join(columns) + " FROM " + table + " WHERE " + where + " ORDER BY " + order
                ))))
            entries, lineage = graph
            return ledger, entries, lineage

    def copy_oracle(self):
        oracle = str(Path(self.fixture.directory.name) / "oracle.db")
        with closing(sqlite3.connect(self.db)) as source, closing(sqlite3.connect(oracle)) as target:
            source.backup(target)
        return oracle

    def run_oracle(self, oracle):
        return shows.sync_tiktok_show_evidence_ledgers(
            oracle, guild_id=77, read_model=self.model,
            artist_identity_index=show_fixtures.artist_index(),
            environ=show_fixtures.ENABLED_QUEUE_ENV,
        )

    def assert_quiet_rerun_matches_fresh_oracle(self):
        oracle = self.copy_oracle()
        expected = self.run_oracle(oracle)
        actual = self.fixture.sync(self.model)
        self.assertEqual(actual["showsWritten"], expected["showsWritten"])
        self.assertEqual(actual["projectionInserted"], expected["projectionInserted"])
        self.assertEqual(self.show_graph(), self.show_graph(oracle))

    def test_chat_original_commits_while_show_assembly_is_held_and_retry_is_fresh(self):
        assembly_entered = threading.Event()
        release_assembly = threading.Event()
        capture_done = threading.Event()
        outcome = {}
        original_builder = shows.build_tiktok_show_evidence_ledger
        builder_calls = []

        def held_builder(*args, **kwargs):
            result = original_builder(*args, **kwargs)
            builder_calls.append(result)
            if len(builder_calls) == 1:
                assembly_entered.set()
                if not release_assembly.wait(5):
                    raise AssertionError("show assembly fixture not released")
            return result

        def sync_actor():
            try:
                outcome["sync"] = self.fixture.sync(self.model)
            except BaseException as exc:
                outcome["sync_error"] = exc

        def capture_actor():
            try:
                outcome["captured_row"] = self.capture()
            except BaseException as exc:
                outcome["capture_error"] = exc
            finally:
                capture_done.set()

        with mock.patch.object(shows, "build_tiktok_show_evidence_ledger", side_effect=held_builder):
            sync_thread = threading.Thread(target=sync_actor)
            capture_thread = threading.Thread(target=capture_actor)
            sync_thread.start()
            self.assertTrue(assembly_entered.wait(3), "real source reading did not reach assembly")
            capture_thread.start()
            try:
                self.assertTrue(capture_done.wait(2), "chat capture blocked by show's CPU assembly snapshot")
                self.assertNotIn("capture_error", outcome)
                self.assertEqual(self.show_graph(), self.previous, "held assembly published a partial graph")
                # This oracle has fresh sources but only the old show graph.
                # Comparing the candidate's FIRST stored graph prevents a
                # later quiet repair from concealing a stale initial publish.
                oracle = self.copy_oracle()
            finally:
                release_assembly.set()
                sync_thread.join(8)
                capture_thread.join(8)
            self.assertFalse(sync_thread.is_alive())
            self.assertFalse(capture_thread.is_alive())
        self.assertNotIn("sync_error", outcome)
        self.assertGreaterEqual(len(builder_calls), 2, "changed sources did not invalidate assembly")
        self.assertEqual(outcome["sync"]["showsWritten"], 1)
        document = self.fixture.stored()[1]
        self.assertIn(outcome["captured_row"], [row["conversationRowId"]
                      for exchange in document["discordInteractions"] for row in exchange["userMessages"]])
        self.run_oracle(oracle)
        self.assertEqual(self.show_graph(), self.show_graph(oracle))
        self.assert_quiet_rerun_matches_fresh_oracle()

    def test_repeated_source_change_defers_without_publishing_then_quiet_rerun_matches(self):
        original_builder = shows.build_tiktok_show_evidence_ledger
        calls = []

        def change_each_assembly(*args, **kwargs):
            result = original_builder(*args, **kwargs)
            calls.append(result)
            if len(calls) == 1:
                self.capture(88002)
            else:
                with closing(sqlite3.connect(self.db, timeout=0.1)) as writer:
                    writer.execute("UPDATE conversations SET content=? WHERE id=101",
                                   ("Corrected fictional track question",))
                    writer.commit()
            return result

        with mock.patch.object(shows, "build_tiktok_show_evidence_ledger", side_effect=change_each_assembly):
            result = self.fixture.sync(self.model)
        self.assertEqual(len(calls), 2)
        self.assertEqual((result["status"], result["reason"]), ("deferred", "source_changed"))
        self.assertEqual((result["showsWritten"], result["projectionInserted"]), (0, 0))
        self.assertEqual(self.show_graph(), self.previous)
        self.assert_quiet_rerun_matches_fresh_oracle()

    def test_deleted_or_newly_private_original_is_not_in_retried_graph(self):
        original_builder = shows.build_tiktok_show_evidence_ledger
        for mutation in ("private", "delete"):
            with self.subTest(mutation=mutation):
                calls = []

                def mutate_once(*args, **kwargs):
                    result = original_builder(*args, **kwargs)
                    calls.append(result)
                    if len(calls) == 1:
                        with closing(sqlite3.connect(self.db, timeout=0.1)) as writer:
                            if mutation == "delete":
                                # Use the complete deletion owner, including
                                # its show-parent purge. Ordinary row pruning
                                # intentionally retains authorized history.
                                delete = governance.complete_delete_member_data
                                if os.name == "nt":
                                    delete = governance._complete_delete_member_data
                                deleted = delete(writer, guild_id=77, user_id=42,
                                                 confirmation="DELETE MY BNL DATA 77")
                                self.assertTrue(deleted["ok"])
                            else:
                                writer.execute("UPDATE conversations SET channel_policy='sealed_test' WHERE id=101")
                            writer.commit()
                    return result

                with mock.patch.object(shows, "build_tiktok_show_evidence_ledger", side_effect=mutate_once):
                    result = self.fixture.sync(self.model)
                self.assertEqual(len(calls), 2)
                self.assertEqual(result["showsWritten"], 1)
                document = self.fixture.stored()[1]
                rows = [row["conversationRowId"] for exchange in document["discordInteractions"]
                        for row in exchange["userMessages"]]
                self.assertNotIn(101, rows)
                self.assert_quiet_rerun_matches_fresh_oracle()


if __name__ == "__main__":
    unittest.main()
