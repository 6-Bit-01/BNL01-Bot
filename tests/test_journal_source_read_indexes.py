"""Existing source readers keep their ordered originals with less index work."""
from __future__ import annotations

import ast
import gc
import hashlib
import inspect
import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

import bnl_journal_source_store as sources
import bnl_tiktok_show_ledger as shows
from bnl_tiktok_live_memory import archive_record


AUTHORED_INDEX = "idx_bnl_journal_sources_authored_window"
KIND_INDEX = "idx_bnl_journal_sources_kind_window"
START = 1_800_000_000_000
END = START + 10_000


def _reader_sql(owner, fragment):
    """Measure the SQL the actual reader owns, without a duplicate query oracle."""
    matches = [node.value for node in ast.walk(ast.parse(inspect.getsource(owner)))
               if isinstance(node, ast.Constant) and isinstance(node.value, str)
               and fragment in node.value]
    if len(matches) != 1:
        raise AssertionError("source reader query changed: %r" % matches)
    return matches[0]


class JournalSourceReadIndexTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.addCleanup(gc.collect)
        self.db = str(Path(directory.name) / "sources.sqlite3")
        sources.ensure_schema(self.db)
        self._seed_controls()

    def _row(self, key, kind="discord_message", *, guild=77, occurred=START + 5_000,
             public=1, policy="public_context", subject="", name="", corrupt=False):
        raw = "Synthetic source " + key
        metadata = {}
        if kind == "tiktok_live_engagement":
            record = archive_record({"event_type": "like", "event_id": key,
                "observed_at": occurred / 1000, "room_id": "test-room",
                "like_count": 2, "like_total": 100})
            self.assertIsNotNone(record)
            raw = json.dumps(record, sort_keys=True, separators=(",", ":"))
            metadata = {"engagement": record}
        return (guild, kind, key, occurred, occurred, 10, policy, subject, name,
                raw, "Synthetic summary", "invalid" if corrupt else hashlib.sha256(raw.encode()).hexdigest(),
                public, json.dumps(metadata, sort_keys=True, separators=(",", ":")))

    def _insert(self, rows):
        with closing(sqlite3.connect(self.db)) as conn, conn:
            conn.executemany("""INSERT INTO bnl_journal_source_events(
                guild_id,source_kind,source_key,occurred_at_ms,ingested_at_ms,channel_id,
                channel_policy,subject_ref,private_display_name,raw_text,sanitized_summary,
                content_hash,public_usable,metadata_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", rows)

    def _seed_controls(self):
        rows = [self._row("chat-%s" % i, kind=kind)
                for i, kind in enumerate(("tiktok_live_chat", "discord_message") * 5)]
        rows += [self._row("metric-%s" % i, kind="tiktok_live_engagement") for i in range(10)]
        rows += [
            self._row("private-authored", public=0, policy="sealed_test"),
            self._row("disallowed-authored", policy="sealed_test"),
            self._row("invalid-authored", corrupt=True),
            self._row("foreign-authored", guild=78),
            self._row("private-metric", kind="tiktok_live_engagement", public=0, policy="sealed_test"),
            self._row("foreign-metric", kind="tiktok_live_engagement", guild=78),
            self._row("end-exclusive", kind="tiktok_live_engagement", occurred=END),
            self._row("start-inclusive", kind="tiktok_live_engagement", occurred=START),
            self._row("irrelevant-publication", kind="website_relay"),
        ]
        self._insert(rows)

    def _drop_read_indexes(self, *names):
        with closing(sqlite3.connect(self.db)) as conn, conn:
            for name in names or (AUTHORED_INDEX, KIND_INDEX):
                conn.execute("DROP INDEX IF EXISTS " + name)

    def _measure(self, sql, parameters):
        with closing(sqlite3.connect(self.db)) as conn:
            plan = tuple(row[3] for row in conn.execute("EXPLAIN QUERY PLAN " + sql, parameters))
            steps = [0]

            def count_step():
                steps[0] += 1
                return 0

            conn.set_progress_handler(count_step, 1)
            try:
                rows = conn.execute(sql, parameters).fetchall()
            finally:
                conn.set_progress_handler(None, 0)
        return rows, steps[0], plan

    def _reader_query(self, owner, fragment):
        if owner is not shows._load_show_related_sources:
            return _reader_sql(owner, fragment)
        captured = []
        with closing(sqlite3.connect(self.db)) as conn:
            conn.execute("PRAGMA query_only=ON")
            conn.execute("BEGIN")

            class QueryRecorder:
                def execute(self, statement, parameters=()):
                    if fragment in statement:
                        captured.append(statement)
                    return conn.execute(statement, parameters)

            # Capture the actual owner's installed-index or legacy SQL branch.
            shows._load_show_related_sources(QueryRecorder(), guild_id=77)
        self.assertEqual(len(captured), 1)
        return captured[0]

    def _assert_index_cost(self, *, owner, fragment, parameters, expected_index, other_index):
        # Dense unrelated public records share the selected time range. The
        # baseline time index must visit them before filtering source_kind.
        self._insert(self._row("noise-%s" % i, kind="website_relay",
                               occurred=START + i % 9_000) for i in range(30_000))
        self._drop_read_indexes()
        before_sql = self._reader_query(owner, fragment)
        before_rows, before_steps, _before_plan = self._measure(before_sql, parameters)
        sources.ensure_schema(self.db)
        # Prove this index's independent benefit, without the sibling index.
        self._drop_read_indexes(other_index)
        after_sql = self._reader_query(owner, fragment)
        after_rows, after_steps, after_plan = self._measure(after_sql, parameters)
        if owner is shows._load_show_related_sources:
            self.assertNotIn("INDEXED BY", before_sql)
            self.assertIn("INDEXED BY " + expected_index, after_sql)
        self.assertEqual(before_rows, after_rows)
        self.assertLess(after_steps, 2_000, "plan=%r" % (after_plan,))
        self.assertGreater(before_steps, 50_000)
        self.assertGreater(before_steps, after_steps * 20)
        self.assertTrue(any(expected_index in detail for detail in after_plan), after_plan)
        return after_rows

    def test_authored_query_bounds_unrelated_records_with_exact_descending_ties(self):
        rows = self._assert_index_cost(owner=shows._load_show_related_sources,
            fragment="source_kind IN ('tiktok_live_chat','discord_message')",
            parameters=(77, 50_001), expected_index=AUTHORED_INDEX, other_index=KIND_INDEX)
        keys = [row[1] for row in rows]
        self.assertNotIn("private-authored", keys)
        self.assertNotIn("foreign-authored", keys)
        # The SQL still donates disallowed/invalid rows to the existing validator.
        self.assertIn("disallowed-authored", keys)
        self.assertIn("invalid-authored", keys)
        self.assertEqual(keys[:3], ["invalid-authored", "disallowed-authored", "chat-9"])

    def test_engagement_query_bounds_unrelated_records_with_exact_ascending_ties(self):
        rows = self._assert_index_cost(owner=shows.read_tiktok_engagement_evidence,
            fragment="source_kind='tiktok_live_engagement'",
            parameters=(77, START, END, 50_001), expected_index=KIND_INDEX, other_index=AUTHORED_INDEX)
        keys = [row[0] for row in rows]
        self.assertEqual(keys, ["start-inclusive"] + ["metric-%s" % i for i in range(10)])
        self.assertNotIn("private-metric", keys)
        self.assertNotIn("foreign-metric", keys)
        self.assertNotIn("end-exclusive", keys)

    def _actual_readers(self):
        with closing(sqlite3.connect(self.db)) as conn:
            conn.execute("PRAGMA query_only=ON")
            conn.execute("BEGIN")
            authored = shows._load_show_related_sources(conn, guild_id=77)
            engagement = shows.read_tiktok_engagement_evidence(
                conn, guild_id=77, source_window_ms=(START, END))
            self.assertEqual(conn.total_changes, 0)
            return authored, engagement

    def test_complete_reader_outputs_and_digests_preserve_eligibility(self):
        self._drop_read_indexes()
        before = self._actual_readers()
        sources.ensure_schema(self.db)
        self.assertEqual(before, self._actual_readers())
        authored, engagement = before
        self.assertEqual({row["eventId"] for row in authored[0]},
                         {"chat-%s" % i if i % 2 == 0 else "discord_source:chat-%s" % i
                          for i in range(10)})
        self.assertEqual(authored[1]["invalid"], 2)
        self.assertEqual(engagement["metrics"]["likes"]["capturedTapIncrements"], 22)
        self.assertEqual(len(engagement["originalSourceRefs"]), 11)

        # Public flags alone do not make private, identified or corrupt metrics
        # eligible. The actual validator/digest behavior remains exactly equal.
        self._insert([self._row("identified-metric", kind="tiktok_live_engagement", subject="discord_user:7"),
                      self._row("disallowed-metric", kind="tiktok_live_engagement", policy="sealed_test"),
                      self._row("invalid-metric", kind="tiktok_live_engagement", corrupt=True)])
        after = self._actual_readers()
        self._drop_read_indexes()
        self.assertEqual(after, self._actual_readers())
        self.assertEqual(after[1]["status"], "partial")
        self.assertEqual(after[1]["metrics"], {})
        self.assertEqual(after[1]["coverage"]["rejectedOriginalCount"], 3)

    def test_additive_idempotent_upgrade_preserves_originals_and_write_controls(self):
        self._drop_read_indexes()
        with closing(sqlite3.connect(self.db)) as conn:
            before = conn.execute("SELECT * FROM bnl_journal_source_events ORDER BY event_seq").fetchall()
        sources.ensure_schema(self.db)
        sources.ensure_schema(self.db)
        with closing(sqlite3.connect(self.db)) as conn:
            self.assertEqual(before, conn.execute("SELECT * FROM bnl_journal_source_events ORDER BY event_seq").fetchall())
            indexes = {row[1] for row in conn.execute("PRAGMA index_list(bnl_journal_source_events)")}
            self.assertTrue({AUTHORED_INDEX, KIND_INDEX,
                "idx_bnl_journal_sources_window", "idx_bnl_journal_sources_public_window"} <= indexes)
            with self.assertRaises(sqlite3.IntegrityError):
                conn.execute("UPDATE bnl_journal_source_events SET raw_text='changed' WHERE source_key='chat-0'")
            with self.assertRaises(sqlite3.IntegrityError):
                conn.execute("DELETE FROM bnl_journal_source_events WHERE source_key='chat-0'")


if __name__ == "__main__":
    unittest.main()
