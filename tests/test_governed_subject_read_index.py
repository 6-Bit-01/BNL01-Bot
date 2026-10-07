"""Physical read-index parity, migration and lifecycle checks using real SQLite."""
from __future__ import annotations

from dataclasses import asdict
import sqlite3
from typing import Any, Dict, List
import unittest
from unittest.mock import patch

import bnl_memory_governance as governance
import bnl_memory_ledger as ledger
from bnl_memory_governance import NON_LIVE_PREDICATES, PROJECTION_CLASSES
from tests.test_memory_governance_v1 import make_conn, insert, req

_cols = governance._cols
# Exact PR665 loader retained from a93c0de; only its Python function name changes.
# Original function SHA256: 97d84a0315c1e5d1b1b4a1ed168209e25f77ff72729d28d38121b92fe8abd47d
def baseline_subject_rows(
    conn: sqlite3.Connection, guild_id: int, subject: str,
) -> List[Dict[str, Any]]:
    """Read every subject row without hydrating text the selector never uses."""
    available = _cols(conn, "memory_ledger_entries")
    columns = tuple(column for column in (
        "entry_id", "guild_id", "subject_key", "entry_type", "predicate_key",
        "normalized_value", "source_class", "source_table", "source_row_id",
        "route_mode", "channel_policy", "visibility", "confidence",
        "public_usable", "derived", "projection", "salience", "observed_at",
        "valid_from", "valid_until", "lifecycle_status",
    ) if column in available)
    skipped_text = []
    parameters: List[Any] = []
    if "normalized_value" in available:
        if "predicate_key" in available:
            predicates = sorted(NON_LIVE_PREDICATES)
            skipped_text.append("predicate_key COLLATE BINARY IN (%s)" % ",".join("?" for _ in predicates))
            parameters.extend(predicates)
        if "entry_type" in available:
            skipped_text.append("entry_type COLLATE BINARY='model_output'")
        for flag in ("derived", "projection"):
            if flag in available:
                skipped_text.append(flag + "=1")
        if "source_class" in available:
            classes = sorted(PROJECTION_CLASSES)
            skipped_text.append("source_class COLLATE BINARY IN (%s)" % ",".join("?" for _ in classes))
            parameters.extend(classes)
    # Use canonical values only. Other spellings keep their original payload
    # and still follow the selector's existing Python normalization/error path.
    # Every matched row continues before reading normalized_value; retain all
    # its metadata for controls, projection ancestry and exclusion diagnostics.
    projection = [
        "CASE WHEN %s THEN NULL ELSE normalized_value END AS normalized_value"
        % " OR ".join(skipped_text)
        if column == "normalized_value" and skipped_text else column
        for column in columns
    ]
    rows = conn.execute(
        "SELECT %s FROM memory_ledger_entries WHERE guild_id=? AND subject_key=?"
        % ",".join(projection),
        (*parameters, guild_id, subject),
    ).fetchall()
    return [dict(zip(columns, row)) for row in rows]


class GovernedSubjectReadIndexTests(unittest.TestCase):
    def setUp(self):
        self.conn = make_conn()
        self.addCleanup(self.conn.close)

    def add(self, entry_id, **changes):
        changes.setdefault("value", "favorite movie is Modular Synths")
        changes.setdefault("pred", "favorite_movie")
        changes.setdefault("vis", "public_safe")
        changes.setdefault("public", 1)
        insert(self.conn, eid=entry_id, **changes)

    def result(self, baseline=False):
        self.conn.execute("BEGIN")
        try:
            with patch.object(governance, "_governed_subject_rows",
                              baseline_subject_rows if baseline else governance._governed_subject_rows):
                return governance.build_governed_context(
                    self.conn, req(visibility_allowance="public_safe", broad_recall=True),
                    initialize_schema=False)
        finally:
            self.conn.rollback()

    def install(self):
        self.assertTrue(ledger.ensure_governed_subject_read_index(self.conn))
        self.conn.commit()
        self.assertTrue(ledger.governed_subject_read_index_ready(self.conn))

    def assert_parity(self):
        baseline = self.result(baseline=True)
        current = self.result()
        self.assertEqual(asdict(current), asdict(baseline))
        return current

    def test_reader_and_generic_schema_do_not_create_index(self):
        self.add("neutral")
        ledger.ensure_memory_ledger_schema(self.conn)
        self.assertFalse(ledger.governed_subject_read_index_ready(self.conn))
        def authorize(action, _table, _column, _db, _trigger):
            return sqlite3.SQLITE_DENY if action == sqlite3.SQLITE_CREATE_INDEX else sqlite3.SQLITE_OK
        self.conn.set_authorizer(authorize)
        before = self.conn.total_changes
        try:
            self.assert_parity()
        finally:
            # Python 3.9 does not support clearing this callback with None.
            self.conn.set_authorizer(lambda *_args: sqlite3.SQLITE_OK)
        self.assertEqual(before, self.conn.total_changes)
        self.assertFalse(ledger.governed_subject_read_index_ready(self.conn))

    def test_full_result_and_original_row_order_with_mixed_controls(self):
        self.add("z-current")
        self.add("x-conversation", pred="conversation", etype="observation", value="N" * 1000)
        self.add("q-upper", pred="CONVERSATION", etype="observation")
        self.add("t-private", vis="private", public=0)
        self.add("m-corrected")
        self.add("j-forgotten", life="forgotten")
        self.add("g-reintroduced", source_revision="new")
        self.add("d-derived", source="derived_summary", derived=1)
        self.add("c-other-flag", derived=2)
        self.add("b-route")
        self.add("a-after", pred="favorite_color", value="favorite color is green")
        self.add("foreign-subject", user=11)
        self.add("foreign-guild", guild=2)
        self.conn.execute("UPDATE memory_ledger_entries SET channel_policy='sealed_test' WHERE entry_id='b-route'")
        self.conn.execute("UPDATE memory_ledger_entries SET source_row_id='shared' WHERE entry_id IN ('j-forgotten','g-reintroduced')")
        self.conn.executemany("INSERT INTO memory_ledger_lineage VALUES (?,?,?,?,?)", [
            ("control", 1, "correction_of", "m-corrected", "now"),
            ("d-derived", 1, "derived_from", "z-current", "now"),
            ("c-other-flag", 1, "derived_from", "z-current", "now"),
        ])
        self.conn.commit()
        expected = self.result(baseline=True)
        expected_rows = baseline_subject_rows(self.conn, 1, "discord_user:10")
        self.install()
        self.assertEqual(governance._governed_subject_rows(self.conn, 1, "discord_user:10"), expected_rows)
        self.assertEqual(asdict(self.assert_parity()), asdict(expected))
        self.assertFalse(expected.diagnostics.processing_errors)

    def test_midstream_malformed_flag_preserves_partial_result_and_error_prefix(self):
        self.add("z-before")
        self.add("m-malformed", pred="favorite_color", value="favorite color is green")
        self.add("a-after", pred="preferred_name", value="preferred name is Test Member")
        self.conn.execute("UPDATE memory_ledger_entries SET derived='invalid' WHERE entry_id='m-malformed'")
        self.conn.commit()
        expected = self.result(baseline=True)
        self.assertEqual(expected.diagnostics.processing_errors, ["ValueError"])
        self.assertEqual([c.entry_id for c in expected.selected], ["z-before"])
        self.install()
        self.assertEqual(asdict(self.assert_parity()), asdict(expected))

    def test_installation_is_idempotent_and_caller_can_roll_back(self):
        self.add("neutral")
        self.conn.execute("BEGIN")
        self.assertTrue(ledger.ensure_governed_subject_read_index(self.conn))
        self.assertTrue(self.conn.in_transaction)
        self.conn.rollback()
        self.assertFalse(ledger.governed_subject_read_index_ready(self.conn))
        self.install()
        version = self.conn.execute("PRAGMA schema_version").fetchone()[0]
        self.assertTrue(ledger.ensure_governed_subject_read_index(self.conn))
        self.assertEqual(self.conn.execute("PRAGMA schema_version").fetchone()[0], version)
        names = {row[1] for row in self.conn.execute("PRAGMA index_list(memory_ledger_entries)")}
        self.assertIn("idx_mle_subject", names)
        self.conn.execute("PRAGMA query_only=ON")
        try:
            self.assertEqual([c.entry_id for c in self.assert_parity().selected], ["neutral"])
        finally:
            self.conn.execute("PRAGMA query_only=OFF")

    def test_wrong_partial_or_collated_index_falls_back_without_repair(self):
        self.add("neutral")
        name = ledger.GOVERNED_SUBJECT_READ_INDEX
        columns = ",".join(ledger.GOVERNED_SUBJECT_READ_COLUMNS)
        for body, where in (("guild_id,subject_key", ""), (columns, " WHERE public_usable=1"),
                            (columns.replace("source_class", "source_class COLLATE NOCASE"), "")):
            with self.subTest(body=body, where=where):
                self.conn.execute("CREATE INDEX %s ON memory_ledger_entries (%s)%s" % (name, body, where))
                self.conn.commit()
                self.assertFalse(ledger.governed_subject_read_index_ready(self.conn))
                self.assertFalse(ledger.ensure_governed_subject_read_index(self.conn))
                self.assert_parity()
                self.conn.execute("DROP INDEX " + name)
                self.conn.commit()

    def test_reduced_without_rowid_and_shadowed_rowid_schemas_keep_fallback(self):
        full = {column: "TEXT" for column in (*ledger.GOVERNED_SUBJECT_READ_COLUMNS, "normalized_value")}
        full.update(guild_id="INTEGER", entry_id="TEXT PRIMARY KEY")
        variants = [({"entry_id": "TEXT", "guild_id": "INTEGER", "subject_key": "TEXT", "normalized_value": "TEXT"}, ""),
                    (full, " WITHOUT ROWID"), ({**full, "rowid": "INTEGER"}, "")]
        for columns, suffix in variants:
            with self.subTest(suffix=suffix, columns=tuple(columns)), sqlite3.connect(":memory:") as conn:
                conn.execute("CREATE TABLE memory_ledger_entries (%s)%s" % (
                    ",".join(name + " " + kind for name, kind in columns.items()), suffix))
                conn.execute("INSERT INTO memory_ledger_entries (entry_id,guild_id,subject_key,normalized_value) VALUES ('neutral',1,'discord_user:10','neutral')")
                self.assertFalse(ledger.ensure_governed_subject_read_index(conn))
                self.assertFalse(ledger.governed_subject_read_index_ready(conn))
                self.assertEqual(governance._governed_subject_rows(conn, 1, "discord_user:10"),
                                 baseline_subject_rows(conn, 1, "discord_user:10"))

    def test_fresh_calls_observe_text_privacy_correction_tombstone_and_delete(self):
        self.add("current")
        self.install()
        self.assertEqual([c.entry_id for c in self.assert_parity().selected], ["current"])
        for assignment, expected in [("normalized_value='favorite movie is New Synths'", "New Synths"),
                                     ("visibility='private',public_usable=0", ""),
                                     ("visibility='public_safe',public_usable=1", "New Synths")]:
            self.conn.execute("UPDATE memory_ledger_entries SET " + assignment + " WHERE entry_id='current'")
            self.conn.commit()
            result = self.assert_parity()
            self.assertIn(expected, result.rendered_context)
            if not expected:
                self.assertEqual(result.selected, ())
        self.conn.execute("INSERT INTO memory_ledger_lineage VALUES ('correction',1,'supersedes','current','now')")
        self.conn.commit()
        self.assertEqual(self.assert_parity().selected, ())
        self.add("restored", source_revision="new")
        self.assertEqual([c.entry_id for c in self.assert_parity().selected], ["restored"])
        self.add("tombstone", source_revision="old", life="forgotten")
        self.conn.execute("UPDATE memory_ledger_entries SET source_row_id='same' WHERE entry_id IN ('restored','tombstone')")
        self.conn.commit()
        self.assertEqual(self.assert_parity().selected, ())
        self.conn.execute("DELETE FROM memory_ledger_entries WHERE entry_id='restored'")
        self.conn.commit()
        self.assertEqual(self.assert_parity().selected, ())
        self.conn.execute("DROP INDEX " + ledger.GOVERNED_SUBJECT_READ_INDEX)
        self.conn.commit()
        self.assertFalse(ledger.governed_subject_read_index_ready(self.conn))
        self.assert_parity()


if __name__ == "__main__":
    unittest.main()
