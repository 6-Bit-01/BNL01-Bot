"""Response-path regressions for large lineage tables and SQLite lifetimes."""

import os
import sqlite3
import tempfile
import unittest
from contextlib import closing
from types import SimpleNamespace
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-token")

import bnl01_bot as bot
import bnl_moment_engine as moments


class CaptureLineageLookupTests(unittest.TestCase):
    def test_exact_source_checks_do_not_scan_unrelated_guild_edges(self):
        with closing(sqlite3.connect(":memory:")) as conn:
            conn.executescript("""
                CREATE TABLE memory_ledger_lineage (
                    entry_id TEXT, guild_id INTEGER, lineage_type TEXT,
                    target_entry_id TEXT, created_at TEXT,
                    PRIMARY KEY(entry_id,lineage_type,target_entry_id));
                CREATE INDEX idx_mll_guild ON memory_ledger_lineage
                    (guild_id,lineage_type,target_entry_id);
                CREATE TABLE memory_ledger_entries
                    (entry_id TEXT PRIMARY KEY, source_row_id TEXT);
            """)
            conn.executemany("INSERT INTO memory_ledger_lineage VALUES(?,1,?,?, '')", (
                ("other-%s" % i, kind, "root-%s" % i)
                for kind in ("supersedes", "reply_to") for i in range(20000)
            ))
            conn.executemany("INSERT INTO memory_ledger_lineage VALUES(?,?,?,?, '')", (
                ("current", 1, "correction_of", "corrected-root"),
                ("current", 1, "supersedes", "superseded-root"),
                ("current", 1, "retracts", "retracted-root"),
                ("current", 2, "correction_of", "other-guild-root"),
                ("current", 1, "derived_from", "non-correction-root"),
            ))
            # A VM instruction budget avoids machine-dependent timing. The old
            # guild/type plan exhausts it on unrelated history before replying.
            calls = [0]
            def bound_work():
                calls[0] += 1
                return calls[0] > 20
            conn.set_progress_handler(bound_work, 100)
            source = SimpleNamespace(guild_id=1, entry_id="current",
                                     is_human=True, normalized_value="Tell me more")
            with mock.patch.object(moments, "handle_source_correction", return_value=1) as correct:
                self.assertEqual(moments._mark_targets_for_correction(conn, source), 3)
                self.assertEqual({c.args[1] for c in correct.call_args_list},
                                 {"corrected-root", "superseded-root", "retracted-root"})
                self.assertTrue(all(c.kwargs == {"guild_id": 1} for c in correct.call_args_list))
            self.assertEqual(moments._reply_target_moment(conn, source), "")
            conn.set_progress_handler(None, 0)


class ResponseSnapshotTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = os.path.join(directory.name, "fixture.db")
        self.patch = mock.patch.object(bot, "DB_FILE", self.path)
        self.patch.start()
        self.addCleanup(self.patch.stop)
        with closing(sqlite3.connect(self.path)) as conn, conn:
            conn.execute("CREATE TABLE evidence(value TEXT)")
            conn.executemany("INSERT INTO evidence VALUES(?)", [("old",), ("other",)])

    def test_memory_recheck_uses_read_only_snapshot_and_sees_corrections(self):
        held = []
        def read(_user, _guild, **kwargs):
            conn = kwargs["connection"]
            held.append(conn)
            # This must be enforced by SQLite, not a convention in each reader.
            with self.assertRaises(sqlite3.OperationalError):
                conn.execute("UPDATE evidence SET value='unwanted write'")
            value = conn.execute("SELECT value FROM evidence ORDER BY rowid LIMIT 1").fetchone()[0]
            kwargs["source_metadata"].update(governed_basis_digest=value)
            return "Memory: " + value
        basis = bot.MemoryPromptSourceBasis(
            expected_digest=bot._prompt_source_digest("Memory: old"),
            rendered_context="Memory: old", user_id=42, guild_id=1,
            route_mode="normal_chat", channel_policy="sealed_test",
            user_text="What do you remember?", is_owner_or_mod=False,
            current_direct=True, governance_allowed=True, channel_id=7,
            moment_attribution_target_user_id=0, governed_basis_digest="old",
        )
        with mock.patch.object(bot, "build_user_memory_context", side_effect=read):
            _, changed = bot.refresh_prompt_source_basis(basis)
            self.assertFalse(changed)
            with closing(sqlite3.connect(self.path, timeout=0.01)) as writer, writer:
                writer.execute("UPDATE evidence SET value='corrected' WHERE rowid=1")
            fresh, changed = bot.refresh_prompt_source_basis(basis)
            self.assertTrue(changed)
            self.assertEqual(fresh.rendered_context, "Memory: corrected")
            self.assertEqual(fresh.governed_basis_digest, "corrected")
        for conn in held:
            with self.assertRaises(sqlite3.ProgrammingError):
                conn.execute("SELECT 1")

    def test_memory_read_failure_closes_snapshot_and_remains_an_error(self):
        held = []
        def fail(_user, _guild, **kwargs):
            held.append(kwargs["connection"])
            raise sqlite3.OperationalError("fixture unavailable")
        with mock.patch.object(bot, "build_user_memory_context", side_effect=fail):
            with self.assertRaises(sqlite3.OperationalError):
                bot._read_user_memory_snapshot(42, 1)
        with self.assertRaises(sqlite3.ProgrammingError):
            held[0].execute("SELECT 1")

    def test_receipt_commit_or_rollback_always_closes_connection(self):
        for fail in (False, True):
            held = []
            def record(conn, _decision, **kwargs):
                held.append(conn)
                conn.execute("UPDATE evidence SET value='receipt' WHERE rowid=1")
                if fail:
                    raise sqlite3.OperationalError("fixture failure")
                return "recorded"
            with mock.patch.object(bot, "record_single_packet_review", side_effect=record):
                if fail:
                    with self.assertRaises(sqlite3.OperationalError):
                        bot._record_ordinary_chat_single_packet_review(None, reason="fixture")
                else:
                    self.assertEqual(bot._record_ordinary_chat_single_packet_review(None, reason="fixture"), "recorded")
            with self.assertRaises(sqlite3.ProgrammingError):
                held[0].execute("SELECT 1")
            with closing(sqlite3.connect(self.path, timeout=0.01)) as writer, writer:
                self.assertEqual(writer.execute("SELECT value FROM evidence WHERE rowid=1").fetchone()[0],
                                 "old" if fail else "receipt")
                writer.execute("UPDATE evidence SET value='old' WHERE rowid=1")


if __name__ == "__main__":
    unittest.main()
