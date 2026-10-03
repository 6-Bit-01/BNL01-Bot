"""Parity and query bounds for governed source controls, using real SQLite."""
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import bnl_memory_governance as governance
from bnl_memory_ledger import subject_key_for_user
from tests.test_memory_governance_v1 import insert, make_conn, req


def legacy_controls(conn, guild_id, rows):
    """Execute the original per-row checks as an independent parity oracle."""
    incoming = set()
    forgotten = {}
    for row in rows:
        entry_id = str(row.get("entry_id") or "")
        if not governance._entry_current(conn, guild_id, entry_id):
            incoming.add(entry_id)
        key = (row.get("source_table"), row.get("source_row_id"),
               row.get("predicate_key"))
        ids = conn.execute(
            """SELECT entry_id FROM memory_ledger_entries
            WHERE guild_id=? AND subject_key=? AND source_table=?
              AND source_row_id=? AND predicate_key=?
              AND lifecycle_status='forgotten' AND entry_id<>?""",
            (guild_id, row.get("subject_key"), *key, entry_id),
        ).fetchall()
        if ids:
            forgotten.setdefault(key, set()).update(str(value[0]) for value in ids)
    return incoming, forgotten


class BatchedGovernanceControlsTests(unittest.TestCase):
    def setUp(self):
        self.conn = make_conn()
        self.addCleanup(self.conn.close)

    def add(self, entry_id, **kwargs):
        kwargs.setdefault("vis", "public_safe")
        kwargs.setdefault("public", 1)
        kwargs.setdefault("pred", "favorite_movie")
        insert(self.conn, eid=entry_id, **kwargs)

    def edge(self, origin, target, kind="correction_of", guild=1):
        self.conn.execute(
            "INSERT INTO memory_ledger_lineage VALUES (?,?,?,?,?)",
            (origin, guild, kind, target, "2026-07-15T00:00:00+00:00"),
        )
        self.conn.commit()

    def result(self, snapshot=True, **kwargs):
        owns_snapshot = snapshot and not self.conn.in_transaction
        if owns_snapshot:
            self.conn.execute("BEGIN")
        try:
            return governance.build_governed_context(
                self.conn, req(**kwargs), initialize_schema=False,
            )
        finally:
            if owns_snapshot:
                self.conn.rollback()

    def test_full_result_matches_per_row_controls_for_mixed_sources(self):
        self.add("valid", value="likes modular synths")
        self.add("corrected", value="old synth preference")
        self.edge("correction", "corrected")
        self.add("blocked", life="forgotten")
        self.add("tombstone", life="forgotten", source_revision="old")
        self.add("reintroduced", source_revision="new")
        self.conn.execute(
            "UPDATE memory_ledger_entries SET source_row_id='shared' "
            "WHERE entry_id IN ('tombstone','reintroduced')"
        )
        self.add("private", vis="private", public=0)
        self.add("wrong_user", user=11)
        self.add("wrong_guild", guild=2)
        self.add("other_guild_edge")
        self.edge("other_control", "other_guild_edge", guild=2)
        self.add("inert", pred="conversation", etype="observation")
        self.edge("inert_correction", "inert")
        self.add("derived", source="derived_summary", derived=1)
        self.edge("derived", "valid", "derived_from")
        self.add("wrong_route")
        self.conn.execute(
            "UPDATE memory_ledger_entries SET channel_policy='sealed_test' "
            "WHERE entry_id='wrong_route'"
        )
        self.conn.commit()
        for visibility in ("public_safe", "private"):
            with self.subTest(visibility=visibility):
                optimized = self.result(visibility_allowance=visibility)
                with patch.object(governance, "_subject_entry_controls", legacy_controls):
                    original = self.result(visibility_allowance=visibility)
                self.assertEqual(optimized, original)
        reasons = {item.entry_id: item.reason for item in optimized.exclusions}
        self.assertEqual(reasons["corrected"], "superseded_or_retracted")
        self.assertEqual(reasons["reintroduced"], "forgotten_source_tombstone")
        self.assertEqual(reasons["inert"], "superseded_or_retracted")

    def test_control_queries_are_bounded_with_many_transient_and_derived_rows(self):
        self.add("root", value="likes modular synths")
        for index in range(800):
            self.add(f"transient-{index}", pred="conversation", etype="observation")
        for index in range(201):
            entry_id = f"derived-{index}"
            self.add(entry_id, source="derived_summary", derived=1)
            self.edge(entry_id, "root", "derived_from")
        self.edge("late-control", "transient-799", "retracts")
        statements = []
        self.conn.set_trace_callback(statements.append)
        optimized = self.result()
        self.conn.set_trace_callback(None)
        incoming_queries = [sql for sql in statements
                            if "SELECT DISTINCT target_entry_id" in sql]
        forgotten_queries = [sql for sql in statements
                             if "lifecycle_status='forgotten' AND entry_id<>" in sql]
        self.assertEqual(len(incoming_queries), 3)
        self.assertEqual(forgotten_queries, [])
        with patch.object(governance, "_subject_entry_controls", legacy_controls):
            self.assertEqual(optimized, self.result())

    def test_later_calls_revalidate_new_corrections_and_tombstones(self):
        self.add("current", value="likes modular synths")
        self.assertEqual([item.entry_id for item in self.result().selected], ["current"])
        self.edge("new-correction", "current", "supersedes")
        self.assertEqual(self.result().selected, ())
        self.add("restored", value="likes modular synths", source_revision="new")
        self.add("forgotten", life="forgotten", source_revision="old")
        self.conn.execute(
            "UPDATE memory_ledger_entries SET source_row_id='same-source' "
            "WHERE entry_id IN ('restored','forgotten')"
        )
        self.conn.commit()
        self.assertEqual(self.result().selected, ())

    def test_null_predicate_matches_sql_and_other_subject_tombstone_is_isolated(self):
        self.add("public", value="likes modular synths")
        self.add("other-subject-forgotten", user=11, life="forgotten", source_revision="old")
        self.conn.execute(
            "UPDATE memory_ledger_entries SET source_row_id='isolated-source' "
            "WHERE entry_id IN ('public','other-subject-forgotten')"
        )
        self.conn.commit()
        columns = [row[1] for row in self.conn.execute("PRAGMA table_info(memory_ledger_entries)")]
        rows = [dict(zip(columns, row)) for row in self.conn.execute(
            "SELECT * FROM memory_ledger_entries WHERE guild_id=1 AND subject_key=?",
            (subject_key_for_user(10),),
        )]
        # A partially populated legacy DTO must not turn SQL NULL equality
        # into a matching tombstone. Current production keys are NOT NULL.
        rows.append({"entry_id": "null-tombstone", "subject_key": subject_key_for_user(10),
                     "source_table": "test", "source_row_id": "missing",
                     "predicate_key": None, "lifecycle_status": "forgotten"})
        self.assertEqual(governance._subject_entry_controls(self.conn, 1, rows),
                         legacy_controls(self.conn, 1, rows))
        self.assertIn("public", [item.entry_id for item in self.result().selected])

    def test_unavailable_incoming_controls_remain_unsafe_and_unselected(self):
        self.add("public", value="likes modular synths")

        def deny_controls(action, table, _column, _database, _trigger):
            if action == sqlite3.SQLITE_READ and table == "memory_ledger_lineage":
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        self.conn.set_authorizer(deny_controls)
        try:
            result = self.result()
        finally:
            self.conn.set_authorizer(None)
        self.assertEqual(result.selected, ())
        self.assertTrue(result.diagnostics.processing_errors)
        self.assertTrue(governance.assess_governance_result_safety(result).unsafe)

    def test_unsnapshotted_read_observes_controls_inserted_after_candidate_read(self):
        self.add("corrected-live", value="likes modular synths")
        self.add("forgotten-live", value="likes modular synths")
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "controls.sqlite")
            reader = sqlite3.connect(path)
            writer = sqlite3.connect(path)
            try:
                self.conn.backup(reader)
                reader.commit()
                original_current = governance._entry_current
                inserted = False

                def insert_live_controls(conn, guild_id, entry_id):
                    nonlocal inserted
                    if not inserted:
                        inserted = True
                        writer.execute(
                            "INSERT INTO memory_ledger_lineage VALUES (?,?,?,?,?)",
                            ("live-correction", 1, "correction_of", "corrected-live", "now"),
                        )
                        insert(writer, eid="late-tombstone", life="forgotten",
                               pred="favorite_movie", source_revision="old")
                        writer.execute(
                            "UPDATE memory_ledger_entries SET source_row_id='forgotten-live' "
                            "WHERE entry_id='late-tombstone'"
                        )
                        writer.commit()
                    return original_current(conn, guild_id, entry_id)

                with patch.object(governance, "_entry_current", insert_live_controls), patch.object(
                    governance, "_subject_entry_controls", side_effect=AssertionError("no caller snapshot"),
                ):
                    result = governance.build_governed_context(reader, req(), initialize_schema=False)
                self.assertTrue(inserted)
                self.assertFalse(reader.in_transaction)
                self.assertEqual(result.selected, ())
                self.assertEqual({item.entry_id: item.reason for item in result.exclusions}, {
                    "corrected-live": "superseded_or_retracted",
                    "forgotten-live": "forgotten_source_tombstone",
                })
            finally:
                writer.close()
                reader.close()


if __name__ == "__main__":
    unittest.main()
