"""Show revisions follow eligible sources, not unrelated archive activity."""

import copy
import hashlib
import json
import sqlite3
import tempfile
import time
import unittest
from contextlib import closing
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import bnl_tiktok_show_ledger as shows
import test_tiktok_show_evidence_ledger as fixtures
from bnl_journal_source_store import purge_user_bound_conversation_sources_on_connection
from test_tiktok_show_evidence_ledger import (
    ENABLED_QUEUE_ENV,
    archived_show,
    artist_index,
    authorized_read_model,
)


class ShowSyncTransactionTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.db = str(Path(self.directory.name) / "show.db")
        setup_connections = []
        connect = sqlite3.connect

        def fixture_connect(*args, **kwargs):
            conn = connect(*args, **kwargs)
            setup_connections.append(conn)
            return conn

        # Reused source-store setup commits via __exit__, which does not close
        # SQLite handles. Close only fixture setup's handles, never sync reads.
        try:
            with mock.patch.object(sqlite3, "connect", side_effect=fixture_connect):
                fixtures.TikTokShowEvidenceLedgerTests().seed_source_and_memory(self.db)
        finally:
            for conn in setup_connections:
                conn.close()
        self.model = authorized_read_model({
            "currentShow": None, "latestShow": archived_show(), "shows": [],
        })

    def sync(self, model=None, **kwargs):
        return shows.sync_tiktok_show_evidence_ledgers(
            self.db, guild_id=77, read_model=model or self.model,
            artist_identity_index=artist_index(), environ=ENABLED_QUEUE_ENV,
            **kwargs,
        )

    def stored(self, key="show-attendance-1"):
        with closing(sqlite3.connect(self.db)) as conn:
            row = conn.execute(
                "SELECT source_digest,ledger_json FROM tiktok_show_evidence_ledgers "
                "WHERE guild_id=77 AND show_key=?", (key,),
            ).fetchone()
            return (row[0], json.loads(row[1])) if row else None

    def counts(self):
        with closing(sqlite3.connect(self.db)) as conn:
            return tuple(conn.execute("SELECT COUNT(*) FROM " + table).fetchone()[0]
                         for table in ("memory_ledger_entries", "memory_ledger_lineage"))

    def test_archive_revision_and_digest_changes_do_not_rewrite_unchanged_show(self):
        self.sync()
        before, counts = self.stored(), self.counts()
        for digest in ("a" * 64, "b" * 64):
            with self.subTest(digest=digest):
                changed = copy.deepcopy(self.model)
                changed["sections"]["archive"].update(
                    sourceRevision=100, sourceDigest=digest,
                )
                with mock.patch.object(shows, "_project_finalized_show",
                                       wraps=shows._project_finalized_show) as project:
                    result = self.sync(changed)
                self.assertEqual(result["showsWritten"], 0)
                self.assertEqual(result["showsUnchanged"], 1)
                project.assert_not_called()
                self.assertEqual(self.stored(), before)
                self.assertEqual(self.counts(), counts)

    def test_real_show_change_still_creates_a_new_sealed_source_revision(self):
        self.sync()
        old_digest = self.stored()[0]
        changed = copy.deepcopy(self.model)
        changed["sections"]["archive"]["latestShow"]["title"] = "Corrected episode title"
        changed["sections"]["archive"].update(sourceRevision=100, sourceDigest="b" * 64)
        result = self.sync(changed)
        digest, document = self.stored()
        self.assertEqual(result["showsWritten"], 1)
        self.assertGreater(result["projectionInserted"], 0)
        self.assertNotEqual(digest, old_digest)
        self.assertEqual(document["sourceAuthorization"]["archiveSourceRevision"], 100)
        self.assertIsNotNone(shows._safe_document(document))
        with closing(sqlite3.connect(self.db)) as conn:
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM memory_ledger_entries "
                "WHERE source_table='tiktok_show_evidence' AND source_revision=? "
                "AND lifecycle_status='active'", (old_digest,),
            ).fetchone()[0], 0)

    def test_private_only_queue_revision_preserves_independent_public_history(self):
        model = copy.deepcopy(self.model)
        history = model["sections"]["archive"].copy()
        history.update(schemaVersion="queue_bnl_public_history_v1",
                       source="queue_bnl_public_history_projection", publicOnly=True,
                       mutationAllowed=False, currentSessionId=None,
                       shows=[archived_show()])
        history.pop("latestShow")
        payload = {key: history[key] for key in (
            "schemaVersion", "historyCoverageStartedAt", "currentSessionId", "shows",
        )}
        history["sourceDigest"] = hashlib.sha256(json.dumps(
            payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        ).encode()).hexdigest()
        model["sections"]["publicHistory"] = history
        self.sync(model)
        before, counts = self.stored(), self.counts()
        model.update(publicOnly=False, accessScope="private")
        model["sections"]["archive"].update(
            accessScope="private", visibility="bnl_private_safe",
        )
        history["sourceRevision"] += 1
        result = self.sync(model)
        self.assertEqual(result["showsWritten"], 0)
        self.assertEqual(self.stored(), before)
        self.assertEqual(self.counts(), counts)

    def test_changed_authority_profile_cannot_reuse_historical_receipt(self):
        self.sync()
        before = self.stored()[0]
        changed = copy.deepcopy(self.model)
        changed["sections"]["archive"]["historyCoverageStartedAt"] = "2026-08-25"
        result = self.sync(changed)
        self.assertEqual(result["showsWritten"], 1)
        self.assertNotEqual(self.stored()[0], before)
        self.assertEqual(self.stored()[1]["sourceAuthorization"]["historyCoverageStartedAt"],
                         "2026-08-25")

    def test_invalid_prior_seal_is_rebuilt_instead_of_retained(self):
        self.sync()
        with closing(sqlite3.connect(self.db)) as conn:
            digest, document = self.stored()
            document["sourceAuthorization"]["accessScope"] = "private"
            conn.execute("UPDATE tiktok_show_evidence_ledgers SET ledger_json=?",
                         (json.dumps(document),))
            conn.commit()
        result = self.sync()
        self.assertEqual(result["showsWritten"], 1)
        self.assertEqual(self.stored()[0], digest)
        self.assertIsNotNone(shows._safe_document(self.stored()[1]))

    def test_current_authorization_failure_cannot_use_prior_public_seal(self):
        self.sync()
        before, counts = self.stored(), self.counts()
        changed = copy.deepcopy(self.model)
        changed["capabilities"]["queueProduction"] = False
        with mock.patch.object(shows, "_load_show_related_sources") as read:
            result = self.sync(changed)
        self.assertEqual(result["status"], "skipped")
        self.assertFalse(result["authorizationEligible"])
        read.assert_not_called()
        self.assertEqual(self.stored(), before)
        self.assertEqual(self.counts(), counts)

    def test_deleted_original_changes_revision_and_removes_its_evidence(self):
        self.sync()
        before = self.stored()[0]
        with closing(sqlite3.connect(self.db)) as conn:
            conn.execute("BEGIN IMMEDIATE")
            purge_user_bound_conversation_sources_on_connection(conn, 77, 42)
            conn.commit()
        result = self.sync()
        digest, document = self.stored()
        self.assertEqual(result["showsWritten"], 1)
        self.assertNotEqual(digest, before)
        self.assertTrue({"event-alex-1", "event-alex-2"}.isdisjoint(
            {item["eventId"] for item in document["messages"]}))

    def test_edited_conversation_changes_revision_and_current_attributed_text(self):
        self.sync()
        before = self.stored()[0]
        changed_text = "BNL, which track just finished?"
        with closing(sqlite3.connect(self.db)) as conn:
            conn.execute("UPDATE conversations SET content=? WHERE id=101", (changed_text,))
            conn.commit()
        result = self.sync()
        digest, document = self.stored()
        self.assertEqual(result["showsWritten"], 1)
        self.assertNotEqual(digest, before)
        authored = [message for exchange in document["discordInteractions"]
                    for message in exchange["userMessages"]
                    if message["conversationRowId"] == 101]
        self.assertEqual([message["text"] for message in authored], [changed_text])

    def test_unchanged_show_still_repairs_missing_source_lineage(self):
        self.sync()
        before = self.stored()
        with closing(sqlite3.connect(self.db)) as conn:
            conn.execute("DELETE FROM memory_ledger_lineage WHERE lineage_type='derived_from' "
                         "AND entry_id IN (SELECT entry_id FROM memory_ledger_entries "
                         "WHERE source_table='tiktok_show_evidence')")
            conn.commit()
        result = self.sync()
        self.assertEqual(result["showsWritten"], 0)
        self.assertGreater(result["projectionDeduplicated"], 0)
        self.assertEqual(self.stored(), before)
        with closing(sqlite3.connect(self.db)) as conn:
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM memory_ledger_lineage l JOIN memory_ledger_entries e "
                "ON e.entry_id=l.entry_id WHERE e.source_table='tiktok_show_evidence' "
                "AND l.lineage_type='derived_from'",
            ).fetchone()[0], 14)

    def test_failed_show_rolls_back_its_document_and_complete_graph(self):
        before = self.counts()
        original = shows._project_finalized_show

        def fail_after_graph(conn, **kwargs):
            original(conn, **kwargs)
            raise RuntimeError("synthetic projection failure")

        with mock.patch.object(shows, "_project_finalized_show", side_effect=fail_after_graph):
            with self.assertRaisesRegex(RuntimeError, "synthetic projection failure"):
                self.sync()
        self.assertIsNone(self.stored())
        self.assertEqual(self.counts(), before)

    def test_competing_privacy_writer_cannot_publish_a_stale_show_snapshot(self):
        self.sync()
        before, counts = self.stored(), self.counts()
        changed = copy.deepcopy(self.model)
        changed["sections"]["archive"]["latestShow"]["title"] = "Revised episode title"
        original = shows._seal_authorized_show_ledger
        with closing(sqlite3.connect(self.db, timeout=0.1)) as writer:
            def reserve_privacy_write(*args, **kwargs):
                writer.execute("BEGIN IMMEDIATE")
                writer.execute("UPDATE conversations SET channel_policy='sealed_test' WHERE id=101")
                return original(*args, **kwargs)

            with mock.patch.object(shows, "_seal_authorized_show_ledger",
                                   side_effect=reserve_privacy_write):
                with self.assertRaises(sqlite3.OperationalError) as failure:
                    self.sync(changed)
            self.assertEqual(getattr(failure.exception, "sqlite_errorcode", 5) & 0xFF, 5)
            # The failed show reader has closed, so the competing privacy
            # owner can finish rather than being held by a partial graph.
            writer.commit()
        self.assertEqual(self.stored(), before)
        self.assertEqual(self.counts(), counts)
        with closing(sqlite3.connect(self.db)) as conn:
            self.assertEqual(conn.execute("SELECT channel_policy FROM conversations WHERE id=101")
                             .fetchone()[0], "sealed_test")

    def test_unavailable_source_releases_snapshot_before_next_show(self):
        second = copy.deepcopy(archived_show())
        second.update(sessionId="second-episode", showDate="2026-08-29")
        model = copy.deepcopy(self.model)
        model["sections"]["archive"]["shows"] = [second]
        original = shows._load_show_source_events
        observed = []

        def unavailable_first(conn, **kwargs):
            observed.append(conn.in_transaction)
            return None if len(observed) == 1 else original(conn, **kwargs)

        with mock.patch.object(shows, "_load_show_source_events", side_effect=unavailable_first):
            result = self.sync(model)
        self.assertEqual(observed, [True, True])
        self.assertEqual(result["showsWritten"], 1)

    def test_previous_show_commits_before_next_show_source_read(self):
        second = copy.deepcopy(archived_show())
        second.update(sessionId="second-episode", title="Second episode", showDate="2026-08-29")
        model = copy.deepcopy(self.model)
        model["sections"]["archive"]["shows"] = [second]
        original = shows._load_show_source_events
        first_key = []

        def inspect_next(conn, **kwargs):
            key = kwargs["show"]["sessionId"]
            if first_key:
                with closing(sqlite3.connect(self.db, timeout=0.1)) as observer:
                    row = observer.execute("SELECT show_key FROM tiktok_show_evidence_ledgers "
                                           "WHERE show_key=?", (first_key[0],)).fetchone()
                self.assertIsNotNone(row, "prior complete show remains uncommitted")
                raise RuntimeError("synthetic second source failure")
            first_key.append(key)
            return original(conn, **kwargs)

        with mock.patch.object(shows, "_load_show_source_events", side_effect=inspect_next):
            with self.assertRaisesRegex(RuntimeError, "synthetic second source failure"):
                self.sync(model)
        self.assertIsNotNone(self.stored(first_key[0]))
        with closing(sqlite3.connect(self.db)) as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM tiktok_show_evidence_ledgers")
                             .fetchone()[0], 1)

    def test_each_show_refreshes_related_sources_in_its_owned_transaction(self):
        second = copy.deepcopy(archived_show())
        second.update(sessionId="second-episode", showDate="2026-08-29")
        model = copy.deepcopy(self.model)
        model["sections"]["archive"]["shows"] = [second]
        original = shows._load_show_related_sources
        transactions = []

        def inspect_snapshot(conn, **kwargs):
            transactions.append(conn.in_transaction)
            return original(conn, **kwargs)

        with mock.patch.object(shows, "_load_show_related_sources", side_effect=inspect_snapshot):
            self.sync(model)
        self.assertEqual(transactions, [True, True])

    def two_shows(self):
        second = copy.deepcopy(archived_show())
        second.update(sessionId="second-episode", showDate="2026-08-29")
        model = copy.deepcopy(self.model)
        model["sections"]["archive"]["shows"] = [second]
        return model

    def test_unchanged_shows_share_one_source_scan_but_keep_separate_snapshots(self):
        model = self.two_shows()
        self.sync(model)
        original = shows._load_show_source_events
        transactions = []

        def inspect_snapshot(conn, **kwargs):
            transactions.append(conn.in_transaction)
            return original(conn, **kwargs)

        with mock.patch.object(shows, "_load_show_related_sources",
                               wraps=shows._load_show_related_sources) as sources, \
                mock.patch.object(shows, "_load_show_source_events",
                                  side_effect=inspect_snapshot):
            result = self.sync(model)
        self.assertEqual(result["showsUnchanged"], 2)
        self.assertEqual(transactions, [True, True])
        self.assertEqual(sources.call_count, 1)

    def test_committed_source_edits_or_privacy_changes_reload_between_show_snapshots(self):
        model = self.two_shows()
        self.sync(model)
        real_connect = sqlite3.connect
        original = shows._load_show_related_sources

        for column, value in (("content", "A corrected source message"),
                              ("channel_policy", "sealed_test"),
                              ("delete_subject", None)):
            with self.subTest(column=column):
                captured = []

                class BetweenShowConnection(sqlite3.Connection):
                    commits = 0

                    def commit(conn):
                        super().commit()
                        conn.commits += 1
                        if conn.commits == 2:
                            with closing(real_connect(self.db, timeout=0.1)) as writer:
                                if column == "delete_subject":
                                    purge_user_bound_conversation_sources_on_connection(writer, 77, 42)
                                else:
                                    writer.execute("UPDATE conversations SET " + column + "=? WHERE id=101",
                                                   (value,))
                                writer.commit()

                def connect(*args, **kwargs):
                    return real_connect(*args, **{**kwargs, "factory": BetweenShowConnection})

                def capture_sources(conn, **kwargs):
                    loaded = original(conn, **kwargs)
                    captured.append(copy.deepcopy(loaded[0]))
                    return loaded

                with mock.patch.object(sqlite3, "connect", side_effect=connect), \
                        mock.patch.object(shows, "_load_show_related_sources", side_effect=capture_sources):
                    self.sync(model)
                self.assertEqual(len(captured), 2)
                current = [record for record in captured[-1]
                           if record.get("conversationRowId") == 101]
                if column == "content":
                    self.assertEqual([record["text"] for record in current], [value])
                else:
                    self.assertEqual(current, [])
                    if column == "delete_subject":
                        self.assertTrue({"event-alex-1", "event-alex-2"}.isdisjoint(
                            {record["eventId"] for record in captured[-1]}))

    def test_own_graph_writes_invalidate_the_cycle_source_cache(self):
        model = self.two_shows()
        with mock.patch.object(shows, "_load_show_related_sources",
                               wraps=shows._load_show_related_sources) as sources:
            result = self.sync(model)
        self.assertEqual(result["showsWritten"], 2)
        self.assertEqual(sources.call_count, 2)

    def test_sql_deadline_rolls_back_changed_show_graph_and_releases_writer(self):
        self.sync()
        before, counts = self.stored(), self.counts()
        changed = copy.deepcopy(self.model)
        changed["sections"]["archive"]["latestShow"]["title"] = "Changed show"
        original = shows._project_finalized_show
        graph_started = []
        clock = [0.0]

        def slow_after_graph(conn, **kwargs):
            original(conn, **kwargs)
            graph_started.append(True)
            # Expire only after real graph writes. The production SQLite
            # progress callback still interrupts a real long-running query;
            # fixture setup speed on a loaded host does not choose the stage.
            clock[0] = 2.0
            conn.execute("WITH RECURSIVE work(n) AS (SELECT 1 UNION ALL "
                         "SELECT n+1 FROM work WHERE n<100000000) SELECT SUM(n) FROM work").fetchone()

        with mock.patch.object(shows, "time", SimpleNamespace(monotonic=lambda: clock[0])), \
                mock.patch.object(shows, "_project_finalized_show", side_effect=slow_after_graph):
            with self.assertRaisesRegex(TimeoutError, "tiktok_show_sync_deadline_exceeded"):
                self.sync(changed, max_seconds=1.0)
        self.assertTrue(graph_started)
        self.assertEqual(self.stored(), before)
        self.assertEqual(self.counts(), counts)
        # A real independent writer must acquire and commit immediately after
        # interruption; neither the snapshot nor its partial graph can linger.
        with closing(sqlite3.connect(self.db, timeout=0.1)) as writer:
            writer.execute("BEGIN EXCLUSIVE")
            writer.execute("UPDATE conversations SET content=content WHERE id=101")
            writer.commit()

    def test_cycle_deadline_preserves_previously_committed_complete_show(self):
        model = self.two_shows()
        original = shows._load_show_source_events
        first_key = []
        clock = [0.0]

        def slow_second_source(conn, **kwargs):
            if first_key:
                # Reaching the next source read proves the preceding show
                # committed. Expire here, then use real SQL interruption.
                clock[0] = 2.0
                conn.execute("WITH RECURSIVE work(n) AS (SELECT 1 UNION ALL "
                             "SELECT n+1 FROM work WHERE n<100000000) SELECT SUM(n) FROM work").fetchone()
            first_key.append(kwargs["show"]["sessionId"])
            return original(conn, **kwargs)

        with mock.patch.object(shows, "time", SimpleNamespace(monotonic=lambda: clock[0])), \
                mock.patch.object(shows, "_load_show_source_events", side_effect=slow_second_source):
            with self.assertRaisesRegex(TimeoutError, "tiktok_show_sync_deadline_exceeded"):
                self.sync(model, max_seconds=1.0)
        self.assertEqual(len(first_key), 1)
        self.assertIsNotNone(self.stored(first_key[0]))
        with closing(sqlite3.connect(self.db, timeout=0.1)) as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM tiktok_show_evidence_ledgers")
                             .fetchone()[0], 1)
            conn.execute("BEGIN EXCLUSIVE")
            conn.commit()

    def test_existing_busy_writer_has_short_wait_and_keeps_old_ledger(self):
        self.sync()
        before, counts = self.stored(), self.counts()
        with closing(sqlite3.connect(self.db, timeout=0.1)) as writer:
            writer.execute("BEGIN EXCLUSIVE")
            started = time.monotonic()
            with self.assertRaises(sqlite3.OperationalError) as caught:
                self.sync()
            self.assertEqual(getattr(caught.exception, "sqlite_errorcode", 5) & 0xFF, 5)
            self.assertLess(time.monotonic() - started, 1.5)
            writer.rollback()
        self.assertEqual(self.stored(), before)
        self.assertEqual(self.counts(), counts)


if __name__ == "__main__":
    unittest.main()
