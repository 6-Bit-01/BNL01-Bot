"""Same show evidence and controls with bounded lineage-count work."""

from contextlib import closing
from datetime import datetime
from pathlib import Path
import shutil
import sqlite3
import tempfile
import unittest
from unittest import mock

import bnl_memory_ledger as memory
import bnl_tiktok_show_ledger as shows
from bnl_journal_source_store import purge_user_bound_conversation_sources_on_connection
import test_tiktok_show_evidence_ledger as fixtures


NOW = "2026-09-01T12:00:00+00:00"
CONTROL_SELECT = (
    "SELECT source_table,source_row_id,lifecycle_status,public_usable, "
    "entry_id FROM memory_ledger_entries"
)


class _FrozenDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        value = datetime.fromisoformat(NOW)
        return value if tz else value.replace(tzinfo=None)


class _UnusedValueCursor:
    """Legacy SQL still reads the unused cell; its consumer discards it."""
    def __init__(self, cursor):
        self.cursor = cursor

    def __iter__(self):
        for row in self.cursor:
            yield (*row[:4], row[5])


class _MeasuredConnection(sqlite3.Connection):
    legacy_reads = False

    def set_progress_handler(self, callback, instructions):
        self.progress_handler = (callback, instructions)
        return super().set_progress_handler(callback, instructions)

    def execute(self, sql, parameters=(), /):
        normalized = " ".join(sql.split())
        control = normalized.startswith(CONTROL_SELECT)
        lineage = "FROM memory_ledger_lineage AS lineage" in normalized
        if self.legacy_reads:
            if control:
                sql = sql.replace("entry_id FROM memory_ledger_entries",
                                  "normalized_value,entry_id FROM memory_ledger_entries", 1)
            if lineage:
                sql = sql.replace(" AS entry INDEXED BY idx_mle_source", " AS entry", 1)
        if not lineage:
            cursor = super().execute(sql, parameters)
            return _UnusedValueCursor(cursor) if control and self.legacy_reads else cursor
        work = [0]

        def step():
            work[0] += 1
            return 0

        previous_handler = getattr(self, "progress_handler", (None, 0))
        super().set_progress_handler(step, 1)
        try:
            cursor = super().execute(sql, parameters)
        finally:
            super().set_progress_handler(*previous_handler)
        self.lineage_reads.append((sql, tuple(parameters), work[0]))
        return cursor


class _LegacyConnection(_MeasuredConnection):
    legacy_reads = True


class ShowSourceReadEfficiencyTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.template = str(Path(self.directory.name) / "template.db")
        opened = []
        connect = sqlite3.connect

        def fixture_connect(*args, **kwargs):
            conn = connect(*args, **kwargs)
            opened.append(conn)
            return conn

        # Source-store fixture context managers commit but do not close handles.
        try:
            with mock.patch.object(sqlite3, "connect", side_effect=fixture_connect):
                fixtures.TikTokShowEvidenceLedgerTests().seed_source_and_memory(self.template)
        finally:
            for conn in opened:
                conn.close()
        self.model = fixtures.authorized_read_model({
            "currentShow": None, "latestShow": fixtures.archived_show(), "shows": [],
        })

    def clone(self, name):
        path = str(Path(self.directory.name) / (name + ".db"))
        shutil.copyfile(self.template, path)
        return path

    def sync(self, path, *, legacy=False):
        connect = sqlite3.connect
        reads = []

        def measured_connect(*args, **kwargs):
            conn = connect(*args, factory=_LegacyConnection if legacy else _MeasuredConnection,
                           **kwargs)
            conn.lineage_reads = reads
            return conn

        with mock.patch.object(shows.sqlite3, "connect", side_effect=measured_connect), \
                mock.patch.object(shows, "datetime", _FrozenDateTime), \
                mock.patch.object(memory, "_now", return_value=NOW):
            result = shows.sync_tiktok_show_evidence_ledgers(
                path, guild_id=77, read_model=self.model,
                artist_identity_index=fixtures.artist_index(), environ=fixtures.ENABLED_QUEUE_ENV,
            )
        self.assertEqual(result["status"], "completed")
        return result, reads

    def related(self, path, *, legacy=False):
        with closing(sqlite3.connect(path, factory=_LegacyConnection if legacy else _MeasuredConnection)) as conn:
            conn.lineage_reads = []
            return shows._load_show_related_sources(conn, guild_id=77)

    def graph(self, path):
        with closing(sqlite3.connect(path)) as conn:
            return {table: conn.execute("SELECT * FROM " + table + " ORDER BY " + order).fetchall()
                    for table, order in (
                        ("tiktok_show_evidence_ledgers", "guild_id,show_key"),
                        ("memory_ledger_entries", "entry_id"),
                        ("memory_ledger_lineage", "entry_id,lineage_type,target_entry_id"),
                        ("memory_ledger_participants", "entry_id,participant_key,participant_role"),
                    )}

    def test_related_read_never_reads_unused_normalized_text(self):
        with closing(sqlite3.connect(self.template)) as conn:
            conn.execute("UPDATE memory_ledger_entries SET normalized_value=?",
                         ("unused historical projection " * 10000,))
            conn.commit()
            expected = shows._load_show_related_sources(conn, guild_id=77)
            denied_reads = []

            def authorize(action, table, column, database, trigger):
                if action == sqlite3.SQLITE_READ and table == "memory_ledger_entries" and column == "normalized_value":
                    denied_reads.append(column)
                    return sqlite3.SQLITE_DENY
                return sqlite3.SQLITE_OK

            conn.set_authorizer(authorize)
            actual = shows._load_show_related_sources(conn, guild_id=77)
            self.assertEqual(actual, expected)
            self.assertEqual(denied_reads, [])

    def test_ordered_sources_preserve_privacy_and_correction_controls(self):
        original = self.related(self.template)
        self.assertEqual(original, self.related(self.template, legacy=True))
        self.assertEqual([r["eventId"] for r in original[0]],
                         [r["eventId"] for r in sorted(original[0], key=lambda r: (r["occurredAtMs"], r["eventId"]))])
        with closing(sqlite3.connect(self.template)) as conn:
            # An extant private original outranks its older public Journal copy.
            conn.execute("UPDATE conversations SET channel_policy='sealed_test' WHERE id=101")
            # A current source edit remains authoritative over the copied text.
            conn.execute("UPDATE conversations SET content='Current public correction' WHERE id=103")
            conn.execute("UPDATE memory_ledger_entries SET lifecycle_status='retracted' "
                         "WHERE source_table='tiktok_live_chat' AND source_row_id='event-alex-1'")
            target = conn.execute("SELECT entry_id FROM memory_ledger_entries "
                                  "WHERE source_table='tiktok_live_chat' AND source_row_id='event-nova-1'").fetchone()[0]
            conn.execute("INSERT INTO memory_ledger_lineage VALUES (?,?,?,?,?)",
                         ("fixture-correction", 77, "correction_of", target, NOW))
            conn.commit()
        actual = self.related(self.template)
        self.assertEqual(actual, self.related(self.template, legacy=True))
        events = {r["eventId"]: r for r in actual[0]}
        self.assertNotIn("discord_conversation:101", events)
        self.assertNotIn("discord_source:7001", events)
        self.assertNotIn("event-alex-1", events)
        self.assertNotIn("event-nova-1", events)
        self.assertEqual(events["discord_conversation:103"]["text"], "Current public correction")
        self.assertNotEqual(actual, original)

    def test_complete_graph_matches_legacy_reads_and_repairs_missing_lineage(self):
        current, legacy = self.clone("current"), self.clone("legacy")
        self.assertEqual(self.sync(current)[0], self.sync(legacy, legacy=True)[0])
        self.assertEqual(self.graph(current), self.graph(legacy))
        before = self.graph(current)
        for path in (current, legacy):
            with closing(sqlite3.connect(path)) as conn:
                conn.execute("DELETE FROM memory_ledger_lineage WHERE lineage_type='derived_from' "
                             "AND entry_id IN (SELECT entry_id FROM memory_ledger_entries "
                             "WHERE source_table='tiktok_show_evidence')")
                conn.commit()
        candidate, baseline = self.sync(current)[0], self.sync(legacy, legacy=True)[0]
        self.assertEqual(candidate, baseline)
        self.assertGreater(candidate["projectionDeduplicated"], 0)
        self.assertEqual(self.graph(current), before)
        self.assertEqual(self.graph(current), self.graph(legacy))

    def test_corrected_and_privacy_withdrawn_sources_keep_complete_graph_parity(self):
        current, legacy = self.clone("revised-current"), self.clone("revised-legacy")
        self.sync(current)
        old_graph = self.graph(current)
        with closing(sqlite3.connect(current)) as conn:
            conn.execute("UPDATE conversations SET channel_policy='sealed_test' WHERE user_id=42")
            purge_user_bound_conversation_sources_on_connection(conn, 77, 42)
            conn.execute("UPDATE conversations SET content='Current public correction' WHERE id=103")
            conn.commit()
        # Both real owners start with exactly the same corrected source state,
        # including the existing privacy owner's invalidation of prior graphs.
        shutil.copyfile(current, legacy)
        self.assertEqual(self.sync(current)[0], self.sync(legacy, legacy=True)[0])
        graph = self.graph(current)
        self.assertEqual(graph, self.graph(legacy))
        self.assertNotEqual(graph, old_graph)
        with closing(sqlite3.connect(current)) as conn:
            related, coverage = shows._load_show_related_sources(conn, guild_id=77)
        self.assertTrue(coverage["complete"])
        events = {r["eventId"]: r for r in related}
        self.assertNotIn("event-alex-1", events)
        self.assertNotIn("event-alex-2", events)
        self.assertNotIn("discord_conversation:101", events)
        self.assertEqual(events["discord_conversation:103"]["text"], "Current public correction")

    def test_lineage_count_work_is_independent_of_unrelated_active_guild_roots(self):
        path = self.clone("large")
        with closing(sqlite3.connect(path)) as conn:
            # Many active roots reproduce the production lifecycle-index choice.
            # They are neither show evidence nor authored source candidates.
            columns = [row[1] for row in conn.execute("PRAGMA table_info(memory_ledger_entries)")]
            template = conn.execute("SELECT * FROM memory_ledger_entries LIMIT 1").fetchone()
            rows = []
            for number in range(25000):
                values = dict(zip(columns, template))
                values.update(entry_id="unrelated-" + str(number), source_table="fixture_unrelated",
                              source_row_id=str(number), source_event_key="unrelated",
                              entry_type="event", normalized_value="unrelated",
                              lifecycle_status="active")
                rows.append(tuple(values[name] for name in columns))
            conn.executemany("INSERT INTO memory_ledger_entries VALUES (" + ",".join("?" for _ in columns) + ")", rows)
            conn.commit()
        self.sync(path)
        before = self.graph(path)
        result, reads = self.sync(path)
        self.assertEqual(result["projectionInserted"], 0)
        self.assertEqual(self.graph(path), before)
        self.assertEqual(len(reads), 1)
        query, parameters, steps = reads[0]
        self.assertLess(steps, 1000, "unchanged-show count scanned unrelated active roots")
        with closing(sqlite3.connect(path)) as conn:
            expected = conn.execute(query, parameters).fetchone()[0]
            # Pin the verified older planner's lifecycle choice for a stable
            # counterfactual across SQLite versions; all predicates stay exact.
            old_plan = query.replace("INDEXED BY idx_mle_source", "INDEXED BY idx_mle_lifecycle")
            work = [0]

            def step():
                work[0] += 1
                return 0

            conn.set_progress_handler(step, 1)
            try:
                old_count = conn.execute(old_plan, parameters).fetchone()[0]
            finally:
                conn.set_progress_handler(None, 0)
            self.assertEqual(old_count, expected)
            self.assertGreater(expected, 0)
            self.assertGreater(work[0], 100000)
            self.assertGreater(work[0], steps * 100)
            lifecycle_steps = work[0]
            work[0] = 0
            legacy_query = query.replace(" AS entry INDEXED BY idx_mle_source", " AS entry")
            conn.set_progress_handler(step, 1)
            try:
                legacy_count = conn.execute(legacy_query, parameters).fetchone()[0]
            finally:
                conn.set_progress_handler(None, 0)
            self.assertEqual(legacy_count, expected)
            self.work_receipt = {
                "unrelated_active_roots": 25000,
                "derived_from_edge_count": expected,
                "candidate_vm_steps_before_fetch": steps,
                "legacy_unhinted_vm_steps": work[0],
                "verified_lifecycle_plan_vm_steps": lifecycle_steps,
                "legacy_plan": [row[3] for row in conn.execute("EXPLAIN QUERY PLAN " + legacy_query, parameters)],
                "candidate_plan": [row[3] for row in conn.execute("EXPLAIN QUERY PLAN " + query, parameters)],
                "same_count_and_complete_graph": True,
            }

    def test_sync_reinstalls_source_index_before_hinted_count(self):
        with closing(sqlite3.connect(self.template)) as conn:
            conn.execute("DROP INDEX idx_mle_source")
            conn.commit()
        self.sync(self.template)
        with closing(sqlite3.connect(self.template)) as conn:
            self.assertIsNotNone(conn.execute("SELECT 1 FROM sqlite_master "
                                             "WHERE type='index' AND name='idx_mle_source'").fetchone())


if __name__ == "__main__":
    unittest.main()
