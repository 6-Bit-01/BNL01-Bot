import asyncio
import json
import os
import sqlite3
import tempfile
import unittest
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl_source_file_refresh as refresh
import bnl01_bot
from bnl_entity_evidence import upsert_entity_evidence_event


class FakeResponse:
    def __init__(self, payload, status=200):
        self.payload = payload
        self.status = status

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def getcode(self):
        return self.status

    def read(self):
        import json
        return json.dumps(self.payload).encode("utf-8")


class FakeOpener:
    def __init__(self, get_payload):
        self.get_payload = get_payload
        self.posts = []

    def open(self, request, timeout=0):
        if request.get_method() == "POST":
            import json
            self.posts.append(json.loads((request.data or b"{}").decode("utf-8")))
            return FakeResponse({"ok": True})
        return FakeResponse(self.get_payload)


class FakeRefreshNowRequest:
    def __init__(self, payload=None, headers=None, json_exc=None):
        self._payload = payload
        self.headers = headers or {}
        self._json_exc = json_exc

    async def json(self):
        if self._json_exc:
            raise self._json_exc
        return self._payload


def decode_json_response(response):
    return json.loads(response.text)


class SourceFileRefreshTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False)
        self.tmp.close()
        self.db = self.tmp.name
        refresh.ensure_source_file_refresh_db(self.db)

    def tearDown(self):
        os.unlink(self.db)

    def rows(self):
        return refresh.list_source_file_refresh_queue(self.db, guild_id=1, limit=20)

    def all_queue_rows(self):
        conn = sqlite3.connect(self.db)
        conn.row_factory = sqlite3.Row
        try:
            refresh.ensure_source_file_refresh_schema(conn)
            return [dict(row) for row in conn.execute(f"SELECT * FROM {refresh.QUEUE_TABLE} ORDER BY id").fetchall()]
        finally:
            conn.close()

    def test_meaningful_subject_authored_evidence_creates_queue_row(self):
        result = refresh.mark_subject_dirty_for_evidence(
            self.db,
            guild_id=1,
            subject_name="Signal Fox",
            evidence_source="conversations",
            content="New track link posted for the project https://example.com/track",
            channel_policy="public_home",
            source_scope="subject_authored",
            visibility="public_safe",
            evidence_type="music",
            relation_to_subject="authored",
        )
        self.assertTrue(result["queued"])
        rows = self.rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["subject_key"], "signal-fox")
        self.assertEqual(rows[0]["status"], "queued")

    def test_generic_small_talk_does_not_queue_refresh(self):
        result = refresh.mark_subject_dirty_for_evidence(
            self.db,
            guild_id=1,
            subject_name="Signal Fox",
            evidence_source="conversations",
            content="hello",
            channel_policy="public_home",
            source_scope="subject_authored",
            visibility="public_safe",
            relation_to_subject="authored",
        )
        self.assertFalse(result["queued"])
        self.assertEqual(self.rows(), [])

    def test_dedupe_updates_existing_subject_queue_row(self):
        for text in (
            "Project link https://example.com/one from subject",
            "Collaboration discussion with a new platform link https://example.com/two",
        ):
            refresh.mark_subject_dirty_for_evidence(
                self.db,
                guild_id=1,
                subject_name="Signal Fox",
                evidence_source="conversations",
                content=text,
                channel_policy="public_home",
                source_scope="subject_authored",
                visibility="public_safe",
                evidence_type="music",
                relation_to_subject="authored",
            )
        rows = self.rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["evidence_count"], 2)
        self.assertIn("subject_activity_link_signal", rows[0]["reason"])

    def test_existing_queue_schema_migrates_candidate_id_without_data_loss(self):
        old = tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False)
        old.close()
        try:
            conn = sqlite3.connect(old.name)
            conn.execute(
                f"""CREATE TABLE {refresh.QUEUE_TABLE} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id INTEGER,
                    subject_key TEXT NOT NULL, subject_name TEXT NOT NULL,
                    reason TEXT NOT NULL, evidence_source TEXT,
                    evidence_count INTEGER NOT NULL DEFAULT 1,
                    priority INTEGER NOT NULL DEFAULT 50,
                    status TEXT NOT NULL DEFAULT 'queued', queued_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL, not_before_at TEXT, last_attempt_at TEXT,
                    attempts INTEGER NOT NULL DEFAULT 0, last_error TEXT,
                    refresh_mode TEXT NOT NULL DEFAULT 'automatic', created_by TEXT
                )"""
            )
            conn.execute(
                f"INSERT INTO {refresh.QUEUE_TABLE} (guild_id, subject_key, subject_name, reason, queued_at, updated_at) VALUES (1, 'signal-fox', 'Signal Fox', 'old', 'now', 'now')"
            )
            refresh.ensure_source_file_refresh_schema(conn)
            refresh.ensure_source_file_refresh_schema(conn)
            conn.commit()
            columns = {row[1] for row in conn.execute(f"PRAGMA table_info({refresh.QUEUE_TABLE})")}
            row = conn.execute(f"SELECT subject_name, candidate_id FROM {refresh.QUEUE_TABLE}").fetchone()
            conn.close()
            self.assertIn("candidate_id", columns)
            self.assertEqual(row, ("Signal Fox", None))
        finally:
            os.unlink(old.name)

    def test_existing_state_schema_migrates_candidate_id_without_data_loss(self):
        old = tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False)
        old.close()
        try:
            conn = sqlite3.connect(old.name)
            conn.execute(
                f"""CREATE TABLE {refresh.STATE_TABLE} (
                    guild_id INTEGER, subject_key TEXT NOT NULL,
                    subject_name TEXT NOT NULL, last_refresh_status TEXT,
                    last_recommendation_id TEXT, cooldown_until TEXT,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (guild_id, subject_key)
                )"""
            )
            conn.execute(
                f"INSERT INTO {refresh.STATE_TABLE} (guild_id, subject_key, subject_name, last_refresh_status, last_recommendation_id, cooldown_until, updated_at) VALUES (1, 'signal-fox', 'Signal Fox', 'succeeded', 'rec_old', '2999-01-01T00:00:00+00:00', 'now')"
            )
            refresh.ensure_source_file_refresh_schema(conn)
            refresh.ensure_source_file_refresh_schema(conn)
            conn.commit()
            columns = {row[1] for row in conn.execute(f"PRAGMA table_info({refresh.STATE_TABLE})")}
            row = conn.execute(f"SELECT subject_name, last_recommendation_id, candidate_id FROM {refresh.STATE_TABLE}").fetchone()
            conn.close()
            self.assertIn("candidate_id", columns)
            self.assertEqual(row, ("Signal Fox", "rec_old", None))
        finally:
            os.unlink(old.name)

    def test_candidate_identity_survives_dedupe_and_upgrades_failed_row(self):
        refresh.enqueue_source_file_refresh(self.db, guild_id=1, subject_name="Signal Fox", reason="automatic", evidence_source="test")
        conn = sqlite3.connect(self.db)
        conn.execute(f"UPDATE {refresh.QUEUE_TABLE} SET status='failed' WHERE subject_key='signal-fox'")
        conn.commit()
        conn.close()
        upgraded = refresh.enqueue_source_file_refresh(
            self.db,
            guild_id=1,
            subject_name="Signal Fox",
            reason="site retry",
            evidence_source="site_refresh_request",
            candidate_id="cand_exact",
        )
        self.assertTrue(upgraded["deduped"])
        self.assertEqual(self.rows()[0]["candidate_id"], "cand_exact")
        refresh.enqueue_source_file_refresh(self.db, guild_id=1, subject_name="Signal Fox", reason="later evidence", evidence_source="test")
        rows = self.all_queue_rows()
        self.assertEqual(len(rows), 2)
        self.assertEqual({row["candidate_id"] for row in rows}, {"cand_exact", None})

    def test_conflicting_candidate_identities_are_never_merged_or_replaced(self):
        refresh.enqueue_source_file_refresh(
            self.db,
            guild_id=1,
            subject_name="Signal Fox",
            reason="first exact request",
            evidence_source="site_refresh_request",
            candidate_id="cand_a",
        )
        refresh.enqueue_source_file_refresh(
            self.db,
            guild_id=1,
            subject_name="Signal Fox",
            reason="different exact request",
            evidence_source="site_refresh_request",
            candidate_id="cand_b",
        )
        rows = self.all_queue_rows()
        self.assertEqual(len(rows), 2)
        self.assertEqual({row["candidate_id"] for row in rows}, {"cand_a", "cand_b"})
        self.assertEqual({row["evidence_count"] for row in rows}, {1})

    def test_entity_evidence_upsert_marks_subject_dirty(self):
        conn = sqlite3.connect(self.db)
        conn.row_factory = sqlite3.Row
        try:
            status = upsert_entity_evidence_event(
                conn,
                guild_id=1,
                subject_name="Signal Fox",
                source_type="manual_note",
                source_table="operator_notes",
                source_row_id="note-1",
                source_label="operator note",
                channel_policy="internal_controlled",
                visibility="review_only",
                authority="operator_note",
                relation_to_subject="subject_keyed",
                evidence_kind="community_signal",
                safe_summary="Operator note records a new project/collaboration signal.",
            )
            conn.commit()
        finally:
            conn.close()
        self.assertEqual(status, "created")
        rows = self.rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["evidence_source"], "entity_evidence_events")

    def test_refresh_context_skips_self_generated_dirty_mark(self):
        with refresh.refresh_generation_context("Signal Fox"):
            result = refresh.mark_subject_dirty_for_evidence(
                self.db,
                guild_id=1,
                subject_name="Signal Fox",
                evidence_source="conversations",
                content="New project link https://example.com/track from the subject",
                channel_policy="public_home",
                source_scope="subject_authored",
                visibility="public_safe",
                evidence_type="music",
                relation_to_subject="authored",
            )
        self.assertFalse(result["queued"])
        self.assertEqual(result["reason"], "refresh_self_generated_evidence")
        self.assertEqual(self.rows(), [])

    def test_refresh_context_does_not_suppress_different_subject(self):
        with refresh.refresh_generation_context("Signal Fox"):
            result = refresh.mark_subject_dirty_for_evidence(
                self.db,
                guild_id=1,
                subject_name="Other Artist",
                evidence_source="conversations",
                content="Other artist shared a new project link https://example.com/track",
                channel_policy="public_home",
                source_scope="subject_authored",
                visibility="public_safe",
                evidence_type="music",
                relation_to_subject="authored",
            )
        self.assertTrue(result["queued"])
        rows = self.rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["subject_key"], "other-artist")

    def test_future_evidence_after_refresh_context_queues_normally(self):
        with refresh.refresh_generation_context("Signal Fox"):
            skipped = refresh.mark_subject_dirty_for_evidence(
                self.db,
                guild_id=1,
                subject_name="Signal Fox",
                evidence_source="conversations",
                content="Self generated project link https://example.com/generated",
                channel_policy="public_home",
                source_scope="subject_authored",
                visibility="public_safe",
                evidence_type="music",
                relation_to_subject="authored",
            )
        self.assertFalse(skipped["queued"])
        result = refresh.mark_subject_dirty_for_evidence(
            self.db,
            guild_id=1,
            subject_name="Signal Fox",
            evidence_source="conversations",
            content="Later real user activity with project link https://example.com/later",
            channel_policy="public_home",
            source_scope="subject_authored",
            visibility="public_safe",
            evidence_type="music",
            relation_to_subject="authored",
        )
        self.assertTrue(result["queued"])
        self.assertEqual(self.rows()[0]["subject_key"], "signal-fox")

    def test_entity_evidence_self_generated_does_not_queue_same_subject(self):
        conn = sqlite3.connect(self.db)
        conn.row_factory = sqlite3.Row
        try:
            with refresh.refresh_generation_context("Signal Fox"):
                status = upsert_entity_evidence_event(
                    conn,
                    guild_id=1,
                    subject_name="Signal Fox",
                    source_type="entity_intelligence",
                    source_table="entity_intelligence",
                    source_row_id="refresh-1",
                    source_label="refresh evidence",
                    channel_policy="public_home",
                    visibility="review_only",
                    authority="local_observed",
                    relation_to_subject="subject_keyed",
                    evidence_kind="community_signal",
                    safe_summary="Refresh summarized a project/collaboration signal.",
                )
            conn.commit()
        finally:
            conn.close()
        self.assertEqual(status, "created")
        self.assertEqual(self.rows(), [])

    def test_cooldown_defers_automatic_refresh(self):
        refresh.enqueue_source_file_refresh(self.db, guild_id=1, subject_name="Signal Fox", reason="test", evidence_source="test")
        conn = sqlite3.connect(self.db)
        try:
            refresh.ensure_source_file_refresh_schema(conn)
            conn.execute(
                f"INSERT OR REPLACE INTO {refresh.STATE_TABLE} (guild_id, subject_key, subject_name, cooldown_until, updated_at) VALUES (1, 'signal-fox', 'Signal Fox', '2999-01-01T00:00:00+00:00', 'now')"
            )
            conn.commit()
        finally:
            conn.close()
        summary = refresh.process_source_file_refresh_queue(self.db, guild_id=1, environ={"BNL_DOSSIER_INGEST_TOKEN": "x"}, sender=lambda payload: {"ok": True})
        self.assertEqual(summary["processed"], 0)
        self.assertEqual(summary["skipped"], 1)
        self.assertEqual(self.rows()[0]["status"], "cooldown")

    def test_current_archive_missing_case_report_is_refreshable(self):
        self.assertTrue(refresh.source_file_report_missing({"ok": True, "found": True, "latestArchive": {"sourceCounts": {"conversations": 1}}}))
        self.assertFalse(refresh.source_file_report_missing({"ok": True, "found": True, "latestArchive": {"sourceCounts": {"conversations": 1}, "subjectMemoryPacketV1": {"subjectName": "Signal Fox"}, "sourceFileCaseReportV1": {"subjectName": "Signal Fox"}}}))

    def test_cooldown_does_not_block_case_report_missing_backfill(self):
        refresh.enqueue_source_file_refresh(self.db, guild_id=1, subject_name="Signal Fox", reason="test", evidence_source="test")
        conn = sqlite3.connect(self.db)
        try:
            refresh.ensure_source_file_refresh_schema(conn)
            conn.execute(
                f"INSERT OR REPLACE INTO {refresh.STATE_TABLE} (guild_id, subject_key, subject_name, last_refresh_completed_at, last_refresh_status, cooldown_until, updated_at) VALUES (1, 'signal-fox', 'Signal Fox', '2026-01-01T00:00:00+00:00', 'succeeded', '2999-01-01T00:00:00+00:00', 'now')"
            )
            conn.commit()
        finally:
            conn.close()
        lookup = lambda query: {"ok": True, "found": True, "sourceFile": {"subjectName": "Signal Fox"}, "latestArchive": {"sourceCounts": {"conversations": 1}}}
        result_packet = {"sent": True, "recommendationSent": True, "archiveSent": True, "sendResult": {"recommendationId": "rec_backfill", "ok": True}, "archiveResult": {"archiveId": "arc_backfill", "ok": True, "status": 200}, "sourceCounts": {"conversations": 1}, "sourceTypes": ["conversations"], "subject": "Signal Fox", "qualityScore": 88}
        with mock.patch.object(refresh, "run_source_file_enrichment", return_value=result_packet) as mocked:
            summary = refresh.process_source_file_refresh_queue(self.db, guild_id=1, lookup_func=lookup, environ={"BNL_DOSSIER_INGEST_TOKEN": "x"})
        self.assertEqual(mocked.call_count, 1)
        self.assertEqual(summary["processed"], 1)
        self.assertEqual(summary["skipped"], 0)
        self.assertEqual(summary["items"][0]["status"], "succeeded")
        self.assertEqual(summary["items"][0]["reason"], "case_report_backfill")
        self.assertTrue(summary["items"][0]["caseReportBackfill"])

    def test_queue_refresh_proceeds_when_case_report_missing(self):
        refresh.enqueue_source_file_refresh(self.db, guild_id=1, subject_name="Signal Fox", reason="background", evidence_source="test")
        lookup = lambda query: {"ok": True, "found": True, "sourceFile": {"subjectName": "Signal Fox"}, "archivePayload": {"sourceCounts": {"conversations": 1}, "subjectMemoryPacketV1": {"subjectName": "Signal Fox"}}}
        result_packet = {"sent": True, "recommendationSent": True, "archiveSent": True, "sendResult": {"recommendationId": "rec_queue", "ok": True}, "archiveResult": {"archiveId": "arc_queue", "ok": True, "status": 200}, "sourceCounts": {"conversations": 1}, "sourceTypes": ["conversations"], "subject": "Signal Fox", "qualityScore": 88}
        with mock.patch.object(refresh, "run_source_file_enrichment", return_value=result_packet) as mocked:
            summary = refresh.process_source_file_refresh_queue(self.db, guild_id=1, lookup_func=lookup, environ={"BNL_DOSSIER_INGEST_TOKEN": "x"})
        self.assertEqual(mocked.call_count, 1)
        self.assertEqual(summary["items"][0]["reason"], "case_report_backfill")

    def test_archive_success_compact_failure_remains_partial_success_for_backfill(self):
        refresh.enqueue_source_file_refresh(self.db, guild_id=1, subject_name="Signal Fox", reason="test", evidence_source="test")
        lookup = lambda query: {"ok": True, "found": True, "sourceFile": {"subjectName": "Signal Fox"}, "latestArchive": {"sourceCounts": {"conversations": 1}}}
        result_packet = {"sent": False, "recommendationSent": False, "archiveSent": True, "sendResult": {"ok": False, "error": "compact rejected"}, "archiveResult": {"archiveId": "arc_partial", "ok": True, "status": 200}, "status": "partial_success", "sourceCounts": {"conversations": 1}, "subject": "Signal Fox"}
        with mock.patch.object(refresh, "run_source_file_enrichment", return_value=result_packet):
            summary = refresh.process_source_file_refresh_queue(self.db, guild_id=1, lookup_func=lookup, environ={"BNL_DOSSIER_INGEST_TOKEN": "x"})
        self.assertEqual(summary["items"][0]["status"], "partial_success")
        self.assertEqual(summary["items"][0]["reason"], "compact_recommendation_rejected")
        self.assertTrue(summary["items"][0]["caseReportBackfill"])


    def test_site_request_requires_case_report_backfill_flags_and_reason(self):
        self.assertTrue(refresh.site_request_requires_case_report_backfill({"caseReportMissing": True}))
        self.assertTrue(refresh.site_request_requires_case_report_backfill({"requiresCaseReportBackfill": True}))
        self.assertTrue(refresh.site_request_requires_case_report_backfill({"reason": "case_report_missing_after_refresh"}))
        self.assertTrue(refresh.site_request_requires_case_report_backfill({"reason": "operator_case_report_backfill_retry"}))
        self.assertFalse(refresh.site_request_requires_case_report_backfill({"caseReportMissing": False, "reason": "opened_source_file"}))

    def test_refresh_now_passes_trusted_callback_base_url_to_enrichment(self):
        opener = FakeOpener({"ok": True, "requests": []})
        lookup = lambda query: {"ok": True, "found": True, "sourceFile": {"subjectName": "Signal Fox"}, "latestArchive": {"sourceCounts": {"conversations": 1}}}
        result_packet = {
            "sent": True,
            "recommendationSent": True,
            "archiveSent": True,
            "sendResult": {"recommendationId": "rec_preview", "ok": True},
            "archiveResult": {"archiveId": "arc_preview", "ok": True, "status": 200},
            "archivePayload": {"subjectMemoryPacketV1": {"subjectName": "Signal Fox"}, "sourceFileCaseReportV1": {"subjectName": "Signal Fox"}},
            "sourceCounts": {"conversations": 1},
            "subject": "Signal Fox",
        }
        with mock.patch.object(refresh, "run_source_file_enrichment", return_value=result_packet) as mocked:
            result = refresh.process_source_file_refresh_now(
                self.db,
                {
                    "requestId": "req_preview",
                    "subjectName": "Signal Fox",
                    "normalizedSubjectKey": "signal-fox",
                    "caseReportMissing": True,
                    "source": "admin_source_file_open",
                    "requestingSiteOrigin": "https://barcode-network-site-git-case-report-6-bit-01.vercel.app",
                },
                guild_id=1,
                environ={"BNL_SOURCE_FILE_READ_TOKEN": "read", "BNL_WEBSITE_BASE_URL": "https://www.barcode-network.com", "BNL_DOSSIER_INGEST_TOKEN": "ingest"},
                opener=opener,
                lookup_func=lookup,
            )
        self.assertEqual(mocked.call_args.kwargs.get("callback_base_url"), "https://barcode-network-site-git-case-report-6-bit-01.vercel.app")
        self.assertEqual(result["callbackBaseSource"], "requesting_site_origin")
        self.assertEqual(result["callbackBaseHost"], "barcode-network-site-git-case-report-6-bit-01.vercel.app")
        self.assertTrue(result["caseReportGenerated"])
        self.assertTrue(result["subjectMemoryPacketGenerated"])
        self.assertTrue(result["caseReportBackfill"])
        self.assertEqual(result["caseReportBackfillTrigger"], "explicit_site_request")

    def test_refresh_now_ignores_untrusted_callback_base_url(self):
        opener = FakeOpener({"ok": True, "requests": []})
        lookup = lambda query: {"ok": True, "found": True, "sourceFile": {"subjectName": "Signal Fox"}, "latestArchive": {"sourceCounts": {"conversations": 1}}}
        with mock.patch.object(refresh, "run_source_file_enrichment", return_value={"sent": True, "recommendationSent": True, "archiveSent": True, "sendResult": {"recommendationId": "rec_default", "ok": True}, "archiveResult": {"archiveId": "arc_default", "ok": True, "status": 200}, "sourceCounts": {"conversations": 1}, "subject": "Signal Fox"}) as mocked:
            result = refresh.process_source_file_refresh_now(
                self.db,
                {"requestId": "req_bad", "subjectName": "Signal Fox", "normalizedSubjectKey": "signal-fox", "caseReportMissing": True, "requestingSiteOrigin": "https://evil.example.test"},
                guild_id=1,
                environ={"BNL_SOURCE_FILE_READ_TOKEN": "read", "BNL_WEBSITE_BASE_URL": "https://www.barcode-network.com", "BNL_DOSSIER_INGEST_TOKEN": "ingest"},
                opener=opener,
                lookup_func=lookup,
            )
        self.assertIsNone(mocked.call_args.kwargs.get("callback_base_url"))
        self.assertEqual(result["callbackBaseSource"], "env_default")
        self.assertEqual(result["callbackBaseHost"], "www.barcode-network.com")

    def test_refresh_now_explicit_case_report_missing_forces_backfill_without_archive_fields(self):
        opener = FakeOpener({"ok": True, "requests": []})
        lookup = lambda query: {"ok": True, "found": True, "sourceFile": {"subjectName": "Signal Fox"}}
        result_packet = {"sent": True, "recommendationSent": True, "archiveSent": True, "sendResult": {"recommendationId": "rec_explicit", "ok": True}, "archiveResult": {"archiveId": "arc_explicit", "ok": True, "status": 200}, "sourceCounts": {"conversations": 1}, "sourceTypes": ["conversations"], "subject": "Signal Fox", "qualityScore": 88}
        with self.assertLogs(level="INFO") as logs, mock.patch.object(refresh, "run_source_file_enrichment", return_value=result_packet) as mocked:
            result = refresh.process_source_file_refresh_now(
                self.db,
                {"requestId": "req_explicit", "subjectName": "Signal Fox", "normalizedSubjectKey": "signal-fox", "caseReportMissing": True, "reason": "case_report_missing_after_refresh", "source": "admin_source_file_open"},
                guild_id=1,
                environ={"BNL_SOURCE_FILE_READ_TOKEN": "read", "BNL_WEBSITE_BASE_URL": "https://site.test", "BNL_DOSSIER_INGEST_TOKEN": "ingest"},
                opener=opener,
                lookup_func=lookup,
            )
        self.assertEqual(mocked.call_count, 1)
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["local"]["items"][0]["reason"], "case_report_backfill")
        self.assertTrue(result["local"]["items"][0]["caseReportBackfill"])
        info_text = "\n".join(logs.output)
        self.assertIn("source_case_report_backfill_requested", info_text)
        self.assertIn("trigger=explicit_site_request", info_text)
        self.assertIn("source_case_report_backfill_started", info_text)
        self.assertIn("source_case_report_backfill_succeeded", info_text)

    def test_refresh_now_explicit_case_report_backfill_bypasses_current_cooldown(self):
        conn = sqlite3.connect(self.db)
        try:
            refresh.ensure_source_file_refresh_schema(conn)
            conn.execute(
                f"INSERT OR REPLACE INTO {refresh.STATE_TABLE} (guild_id, subject_key, subject_name, last_refresh_completed_at, last_refresh_status, last_recommendation_id, cooldown_until, updated_at) VALUES (1, 'signal-fox', 'Signal Fox', '2026-01-01T00:00:00+00:00', 'succeeded', 'rec_old', '2999-01-01T00:00:00+00:00', 'now')"
            )
            conn.commit()
        finally:
            conn.close()
        opener = FakeOpener({"ok": True, "requests": []})
        lookup = lambda query: {"ok": True, "found": True, "sourceFile": {"subjectName": "Signal Fox"}}
        result_packet = {"sent": True, "recommendationSent": True, "archiveSent": True, "sendResult": {"recommendationId": "rec_new", "ok": True}, "archiveResult": {"archiveId": "arc_new", "ok": True, "status": 200}, "sourceCounts": {"conversations": 1}, "sourceTypes": ["conversations"], "subject": "Signal Fox", "qualityScore": 88}
        with mock.patch.object(refresh, "run_source_file_enrichment", return_value=result_packet) as mocked:
            result = refresh.process_source_file_refresh_now(
                self.db,
                {"requestId": "req_backfill", "subjectName": "Signal Fox", "normalizedSubjectKey": "signal-fox", "requiresCaseReportBackfill": True, "reason": "case_report_backfill", "source": "admin_source_file_open"},
                guild_id=1,
                environ={"BNL_SOURCE_FILE_READ_TOKEN": "read", "BNL_WEBSITE_BASE_URL": "https://site.test", "BNL_DOSSIER_INGEST_TOKEN": "ingest"},
                opener=opener,
                lookup_func=lookup,
            )
        self.assertEqual(mocked.call_count, 1)
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["recommendationId"], "rec_new")
        self.assertNotEqual(result["local"]["items"][0]["reason"], "cooldown_current")

    def test_site_polling_explicit_case_report_backfill_without_archive_fields(self):
        opener = FakeOpener({"ok": True, "requests": [{"id": "req_explicit_poll", "subjectName": "Signal Fox", "normalizedSubjectKey": "signal-fox", "status": "pending", "requiresCaseReportBackfill": True, "reason": "case_report_backfill"}]})
        lookup = lambda query: {"ok": True, "found": True, "sourceFile": {"subjectName": "Signal Fox"}}
        result_packet = {"sent": True, "recommendationSent": True, "archiveSent": True, "sendResult": {"recommendationId": "rec_poll", "ok": True}, "archiveResult": {"archiveId": "arc_poll", "ok": True, "status": 200}, "sourceCounts": {"conversations": 1}, "sourceTypes": ["conversations"], "subject": "Signal Fox", "qualityScore": 88}
        with mock.patch.object(refresh, "run_source_file_enrichment", return_value=result_packet) as mocked:
            summary = refresh.process_site_refresh_requests(
                self.db,
                guild_id=1,
                environ={"BNL_SOURCE_FILE_READ_TOKEN": "read", "BNL_WEBSITE_BASE_URL": "https://site.test", "BNL_DOSSIER_INGEST_TOKEN": "ingest"},
                opener=opener,
                lookup_func=lookup,
            )
        self.assertEqual(mocked.call_count, 1)
        self.assertEqual(summary["processed"], 1)
        self.assertEqual(summary["items"][0]["reason"], "case_report_backfill")
        self.assertTrue(summary["items"][0]["caseReportBackfill"])

    def test_manual_refresh_can_queue_and_process_subject(self):
        with mock.patch.object(refresh, "run_source_file_enrichment", return_value={"sent": True, "sendResult": {"recommendationId": "rec_1"}, "sourceCounts": {"conversations": 1}, "sourceTypes": ["conversations"], "subject": "Signal Fox", "qualityScore": 80}):
            summary = refresh.process_single_source_file_refresh(self.db, guild_id=1, subject_name="Signal Fox", environ={"BNL_DOSSIER_INGEST_TOKEN": "x"})
        self.assertEqual(summary["processed"], 1)
        state = refresh.get_refresh_state(self.db, 1, "signal-fox")
        self.assertEqual(state["last_refresh_status"], "succeeded")
        self.assertEqual(state["last_recommendation_id"], "rec_1")

    def test_no_target_local_queue_is_terminal_and_not_retried(self):
        refresh.enqueue_source_file_refresh(self.db, guild_id=1, subject_name="Signal Fox", reason="test", evidence_source="test")
        no_target = {"sent": False, "status": "no_target", "subject": "Signal Fox", "sendResult": {}, "sourceCounts": {}, "sourceTypes": []}
        with mock.patch.object(refresh, "run_source_file_enrichment", return_value=no_target) as mocked:
            first = refresh.process_source_file_refresh_queue(self.db, guild_id=1, environ={"BNL_DOSSIER_INGEST_TOKEN": "x"})
            second = refresh.process_source_file_refresh_queue(self.db, guild_id=1, environ={"BNL_DOSSIER_INGEST_TOKEN": "x"})
        self.assertEqual(first["skipped"], 1)
        self.assertEqual(first["items"][0]["reason"], "no_target")
        self.assertEqual(second["processed"], 0)
        self.assertEqual(second["skipped"], 0)
        self.assertEqual(mocked.call_count, 1)
        self.assertEqual(self.rows(), [])
        terminal = self.all_queue_rows()[0]
        self.assertEqual(terminal["status"], "skipped")
        self.assertEqual(terminal["last_error"], "no_target")
        state = refresh.get_refresh_state(self.db, 1, "signal-fox")
        self.assertEqual(state["last_refresh_status"], "no_target")
        self.assertEqual(state["last_error"], "no_target")

    def test_dry_run_process_does_not_send_to_site(self):
        refresh.enqueue_source_file_refresh(self.db, guild_id=1, subject_name="Signal Fox", reason="test", evidence_source="test")
        with mock.patch.object(refresh, "run_source_file_enrichment") as mocked:
            summary = refresh.process_source_file_refresh_queue(self.db, guild_id=1, dry_run=True)
        mocked.assert_not_called()
        self.assertEqual(summary["processed"], 1)
        self.assertEqual(self.rows()[0]["status"], "queued")

    def test_worker_send_failure_records_attempt_and_error(self):
        refresh.enqueue_source_file_refresh(self.db, guild_id=1, subject_name="Signal Fox", reason="test", evidence_source="test")
        with mock.patch.object(refresh, "run_source_file_enrichment", return_value={"sent": False, "sendResult": {"error": "site rejected"}, "status": "send_failed"}):
            summary = refresh.process_source_file_refresh_queue(self.db, guild_id=1, environ={"BNL_DOSSIER_INGEST_TOKEN": "x"})
        self.assertEqual(summary["failed"], 1)
        rows = self.rows()
        self.assertEqual(rows[0]["status"], "failed")
        self.assertEqual(rows[0]["attempts"], 1)
        self.assertIn("site rejected", rows[0]["last_error"])
        state = refresh.get_refresh_state(self.db, 1, "signal-fox")
        self.assertEqual(state["last_refresh_status"], "failed")

    def test_refresh_module_has_no_subject_specific_fixture_names(self):
        with open("bnl_source_file_refresh.py", encoding="utf-8") as handle:
            source = handle.read().lower()
        for forbidden in ("hellcat", "emerald", "crow"):
            self.assertNotIn(forbidden, source)

    def test_parse_operator_commands(self):
        matched, options, error = refresh.parse_source_refresh_command("!bnl source refresh process | dry_run=true")
        self.assertTrue(matched)
        self.assertFalse(error)
        self.assertEqual(options["action"], "process")
        self.assertTrue(options["dry_run"])
        matched, options, error = refresh.parse_source_refresh_command("!bnl source refresh clear Signal Fox")
        self.assertTrue(matched)
        self.assertEqual(options["action"], "clear")
        self.assertEqual(options["subject"], "Signal Fox")


    def test_site_polling_dry_run_does_not_mutate_or_post(self):
        opener = FakeOpener({"ok": True, "requests": [{"id": "req_1", "subjectName": "Signal Fox", "status": "pending"}]})
        with mock.patch.object(refresh, "run_source_file_enrichment") as mocked:
            summary = refresh.process_site_refresh_requests(
                self.db,
                guild_id=1,
                dry_run=True,
                environ={"BNL_SOURCE_FILE_READ_TOKEN": "read", "BNL_WEBSITE_BASE_URL": "https://site.test"},
                opener=opener,
            )
        mocked.assert_not_called()
        self.assertEqual(summary["processed"], 1)
        self.assertEqual(opener.posts, [])
        self.assertEqual(self.rows(), [])

    def test_site_polling_real_success_claims_processes_and_completes(self):
        opener = FakeOpener({"ok": True, "requests": [{"id": "req_1", "subjectName": "Signal Fox", "status": "pending"}]})
        lookup = lambda query: {"ok": True, "found": True, "sourceFile": {"subjectName": "Signal Fox"}}
        with mock.patch.object(refresh, "run_source_file_enrichment", return_value={"sent": True, "sendResult": {"recommendationId": "rec_1"}, "sourceCounts": {"conversations": 1}, "sourceTypes": ["conversations"], "subject": "Signal Fox", "qualityScore": 80}):
            summary = refresh.process_site_refresh_requests(
                self.db,
                guild_id=1,
                environ={"BNL_SOURCE_FILE_READ_TOKEN": "read", "BNL_WEBSITE_BASE_URL": "https://site.test", "BNL_DOSSIER_INGEST_TOKEN": "ingest"},
                opener=opener,
                lookup_func=lookup,
            )
        self.assertEqual(summary["processed"], 1)
        self.assertEqual([p["status"] for p in opener.posts], ["claimed", "completed"])
        self.assertEqual(opener.posts[-1]["completedByRecommendationId"], "rec_1")
        state = refresh.get_refresh_state(self.db, 1, "signal-fox")
        self.assertEqual(state["last_refresh_status"], "succeeded")
        self.assertEqual(self.rows(), [])

    def test_site_polling_uses_candidate_identity_for_preflight_and_worker(self):
        opener = FakeOpener({"ok": True, "requests": [{"id": "req_1", "candidateId": "cand_exact", "subjectName": "Signal Fox", "status": "pending"}]})
        queries = []
        lookup = lambda query: queries.append(query) or {"ok": True, "found": True, "sourceFile": {"candidateId": "cand_exact", "subjectName": "Signal Fox"}}
        result = {"sent": True, "sendResult": {"recommendationId": "rec_1"}, "sourceCounts": {}, "subject": "Signal Fox"}
        with mock.patch.object(refresh, "run_source_file_enrichment", return_value=result) as mocked:
            summary = refresh.process_site_refresh_requests(
                self.db,
                guild_id=1,
                environ={"BNL_SOURCE_FILE_READ_TOKEN": "read", "BNL_WEBSITE_BASE_URL": "https://site.test", "BNL_DOSSIER_INGEST_TOKEN": "ingest"},
                opener=opener,
                lookup_func=lookup,
            )
        self.assertEqual(summary["processed"], 1)
        self.assertTrue(queries)
        self.assertTrue(all(query["lookupKey"] == "candidateId" for query in queries))
        self.assertEqual(mocked.call_args.kwargs["lookup_key"], "candidateId")
        self.assertEqual(mocked.call_args.kwargs["lookup_value"], "cand_exact")

    def test_site_candidate_identity_is_opaque_and_preserved_to_200_characters(self):
        candidate_id = "cand_" + ("x" * 145) + "_123456789012345678"
        self.assertEqual(len(candidate_id), 169)
        lookup_key, lookup_value = refresh._site_refresh_lookup_parts({"candidateId": candidate_id, "subjectName": "Signal Fox"})
        self.assertEqual(lookup_key, "candidateId")
        self.assertEqual(lookup_value, candidate_id)
        queued = refresh.enqueue_source_file_refresh(
            self.db,
            guild_id=1,
            subject_name="Signal Fox",
            reason="exact site request",
            evidence_source="site_refresh_request",
            candidate_id=candidate_id,
        )
        self.assertTrue(queued["queued"])
        self.assertEqual(self.rows()[0]["candidate_id"], candidate_id)

    def test_site_polling_failure_marks_failed(self):
        opener = FakeOpener({"ok": True, "requests": [{"id": "req_1", "subjectName": "Signal Fox", "status": "pending"}]})
        lookup = lambda query: {"ok": True, "found": True, "sourceFile": {"subjectName": "Signal Fox"}}
        with mock.patch.object(refresh, "run_source_file_enrichment", return_value={"sent": False, "sendResult": {"error": "site rejected"}, "status": "send_failed"}):
            summary = refresh.process_site_refresh_requests(
                self.db,
                guild_id=1,
                environ={"BNL_SOURCE_FILE_READ_TOKEN": "read", "BNL_WEBSITE_BASE_URL": "https://site.test", "BNL_DOSSIER_INGEST_TOKEN": "ingest"},
                opener=opener,
                lookup_func=lookup,
            )
        self.assertEqual(summary["failed"], 1)
        self.assertEqual([p["status"] for p in opener.posts], ["claimed", "failed"])
        self.assertIn("site rejected", opener.posts[-1]["failureReason"])

    def test_site_request_no_target_marks_failed(self):
        opener = FakeOpener({"ok": True, "requests": [{"id": "req_1", "subjectName": "Signal Fox", "status": "pending"}]})
        lookup = lambda query: {"ok": True, "found": False}
        with mock.patch.object(refresh, "run_source_file_enrichment") as mocked:
            summary = refresh.process_site_refresh_requests(
                self.db,
                guild_id=1,
                environ={"BNL_SOURCE_FILE_READ_TOKEN": "read", "BNL_WEBSITE_BASE_URL": "https://site.test", "BNL_DOSSIER_INGEST_TOKEN": "ingest"},
                opener=opener,
                lookup_func=lookup,
            )
        mocked.assert_not_called()
        self.assertEqual(summary["failed"], 1)
        self.assertEqual([p["status"] for p in opener.posts], ["claimed", "failed"])
        self.assertEqual(opener.posts[-1]["failureReason"], "no_target")
        state = refresh.get_refresh_state(self.db, 1, "signal-fox")
        self.assertEqual(state["last_refresh_status"], "no_target")
        self.assertEqual(state["last_error"], "no_target")
        self.assertEqual(self.rows(), [])

    def test_subject_dry_run_does_not_mutate_existing_queue_or_state(self):
        refresh.enqueue_source_file_refresh(self.db, guild_id=1, subject_name="Other Subject", reason="test", evidence_source="test")
        before_rows = self.rows()
        before_state = refresh.get_refresh_state(self.db, 1, "signal-fox")
        with mock.patch.object(refresh, "run_source_file_enrichment") as mocked:
            summary = refresh.process_single_source_file_refresh(self.db, guild_id=1, subject_name="Signal Fox", dry_run=True)
        mocked.assert_not_called()
        self.assertEqual(self.rows(), before_rows)
        self.assertEqual(refresh.get_refresh_state(self.db, 1, "signal-fox"), before_state)
        self.assertEqual(summary["items"][0]["subject"], "Signal Fox")

    def test_process_dry_run_does_not_change_cooldown_status_or_send(self):
        refresh.enqueue_source_file_refresh(self.db, guild_id=1, subject_name="Signal Fox", reason="test", evidence_source="test")
        conn = sqlite3.connect(self.db)
        try:
            refresh.ensure_source_file_refresh_schema(conn)
            conn.execute(
                f"INSERT OR REPLACE INTO {refresh.STATE_TABLE} (guild_id, subject_key, subject_name, cooldown_until, updated_at) VALUES (1, 'signal-fox', 'Signal Fox', '2999-01-01T00:00:00+00:00', 'now')"
            )
            conn.commit()
        finally:
            conn.close()
        before_rows = self.rows()
        with mock.patch.object(refresh, "run_source_file_enrichment") as mocked:
            summary = refresh.process_source_file_refresh_queue(self.db, guild_id=1, dry_run=True)
        mocked.assert_not_called()
        self.assertEqual(summary["items"][0]["status"], "dry_run")
        self.assertEqual(self.rows(), before_rows)

    def test_refresh_now_missing_token_rejected(self):
        self.assertFalse(refresh.is_source_refresh_now_token_valid(None, environ={"BNL_SOURCE_FILE_REFRESH_TOKEN": "secret"}))

    def test_refresh_now_invalid_token_rejected(self):
        self.assertFalse(refresh.is_source_refresh_now_token_valid("wrong", environ={"BNL_SOURCE_FILE_REFRESH_TOKEN": "secret"}))
        self.assertTrue(refresh.is_source_refresh_now_token_valid("secret", environ={"BNL_SOURCE_FILE_REFRESH_TOKEN": "secret"}))

    def test_refresh_now_valid_request_processes_immediately_and_completes(self):
        opener = FakeOpener({"ok": True, "requests": []})
        lookup = lambda query: {"ok": True, "found": True, "sourceFile": {"subjectName": "Signal Fox"}}
        with mock.patch.object(refresh, "run_source_file_enrichment", return_value={"sent": True, "recommendationSent": True, "archiveSent": True, "sendResult": {"recommendationId": "rec_now", "ok": True}, "archiveResult": {"archiveId": "arc_now", "ok": True, "status": 200}, "sourceCounts": {"conversations": 1}, "sourceTypes": ["conversations"], "subject": "Signal Fox", "qualityScore": 88}) as mocked:
            result = refresh.process_source_file_refresh_now(
                self.db,
                {"requestId": "req_now", "candidateId": "cand_1", "subjectName": "Signal Fox", "normalizedSubjectKey": "signal-fox", "reason": "opened_source_file", "source": "admin_source_file_open"},
                guild_id=1,
                environ={"BNL_SOURCE_FILE_READ_TOKEN": "read", "BNL_WEBSITE_BASE_URL": "https://site.test", "BNL_DOSSIER_INGEST_TOKEN": "ingest"},
                opener=opener,
                lookup_func=lookup,
            )
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["recommendationId"], "rec_now")
        self.assertTrue(result["archiveSent"])
        self.assertEqual(result["archiveId"], "arc_now")
        self.assertEqual(mocked.call_count, 1)
        self.assertEqual(mocked.call_args.kwargs["lookup_key"], "candidateId")
        self.assertEqual(mocked.call_args.kwargs["lookup_value"], "cand_1")
        self.assertEqual([p["status"] for p in opener.posts], ["claimed", "completed"])
        self.assertEqual(opener.posts[-1]["completedByRecommendationId"], "rec_now")

    def test_refresh_now_targets_exact_candidate_row_and_ignores_subject_cooldown(self):
        refresh.enqueue_source_file_refresh(
            self.db,
            guild_id=1,
            subject_name="Signal Fox",
            reason="older exact request",
            evidence_source="site_refresh_request",
            priority=100,
            candidate_id="cand_a",
        )
        conn = sqlite3.connect(self.db)
        try:
            conn.execute(
                f"INSERT OR REPLACE INTO {refresh.STATE_TABLE} (guild_id, subject_key, subject_name, last_refresh_completed_at, last_refresh_status, last_recommendation_id, cooldown_until, candidate_id, updated_at) VALUES (1, 'signal-fox', 'Signal Fox', '2026-07-21T00:00:00+00:00', 'succeeded', 'rec_a', '2999-01-01T00:00:00+00:00', 'cand_a', '2026-07-21T00:00:00+00:00')"
            )
            conn.commit()
        finally:
            conn.close()
        opener = FakeOpener({"ok": True, "requests": []})
        lookup = lambda query: {"ok": True, "found": True, "sourceFile": {"candidateId": query["lookupValue"], "subjectName": "Signal Fox"}}
        result = {"sent": True, "sendResult": {"recommendationId": "rec_b"}, "sourceCounts": {}, "subject": "Signal Fox"}

        with mock.patch.object(refresh, "run_source_file_enrichment", return_value=result) as mocked:
            response = refresh.process_source_file_refresh_now(
                self.db,
                {"requestId": "req_b", "candidateId": "cand_b", "subjectName": "Signal Fox"},
                guild_id=1,
                environ={"BNL_SOURCE_FILE_READ_TOKEN": "read", "BNL_WEBSITE_BASE_URL": "https://site.test", "BNL_DOSSIER_INGEST_TOKEN": "ingest"},
                opener=opener,
                lookup_func=lookup,
            )

        self.assertEqual(response["status"], "success")
        self.assertEqual(response["recommendationId"], "rec_b")
        self.assertEqual(mocked.call_args.kwargs["lookup_value"], "cand_b")
        rows = self.all_queue_rows()
        self.assertEqual({row["candidate_id"]: row["status"] for row in rows}, {"cand_a": "queued", "cand_b": "succeeded"})
        self.assertEqual([post["status"] for post in opener.posts], ["claimed", "completed"])

    def test_refresh_now_current_marks_only_its_exact_candidate_row_terminal(self):
        refresh.enqueue_source_file_refresh(
            self.db,
            guild_id=1,
            subject_name="Signal Fox",
            reason="different exact request",
            evidence_source="source_refresh_now",
            priority=100,
            candidate_id="cand_b",
        )
        conn = sqlite3.connect(self.db)
        try:
            conn.execute(
                f"INSERT OR REPLACE INTO {refresh.STATE_TABLE} (guild_id, subject_key, subject_name, last_refresh_completed_at, last_refresh_status, last_recommendation_id, cooldown_until, candidate_id, updated_at) VALUES (1, 'signal-fox', 'Signal Fox', '2026-07-21T00:00:00+00:00', 'succeeded', 'rec_a', '2999-01-01T00:00:00+00:00', 'cand_a', '2026-07-21T00:00:00+00:00')"
            )
            conn.commit()
        finally:
            conn.close()
        opener = FakeOpener({"ok": True, "requests": []})
        lookup = lambda query: {"ok": True, "found": True, "sourceFile": {"candidateId": query["lookupValue"], "subjectName": "Signal Fox"}}

        with mock.patch.object(refresh, "run_source_file_enrichment") as mocked:
            response = refresh.process_source_file_refresh_now(
                self.db,
                {"requestId": "req_a", "candidateId": "cand_a", "subjectName": "Signal Fox"},
                guild_id=1,
                environ={"BNL_SOURCE_FILE_READ_TOKEN": "read", "BNL_WEBSITE_BASE_URL": "https://site.test", "BNL_DOSSIER_INGEST_TOKEN": "ingest"},
                opener=opener,
                lookup_func=lookup,
            )

        mocked.assert_not_called()
        self.assertEqual(response["status"], "success")
        self.assertEqual(response["recommendationId"], "rec_a")
        rows = self.all_queue_rows()
        self.assertEqual({row["candidate_id"]: row["status"] for row in rows}, {"cand_b": "queued", "cand_a": "skipped"})
        self.assertEqual([post["status"] for post in opener.posts], ["claimed", "completed"])

    def test_site_polling_targets_the_row_enqueued_for_each_candidate_request(self):
        refresh.enqueue_source_file_refresh(
            self.db,
            guild_id=1,
            subject_name="Signal Fox",
            reason="older exact request",
            evidence_source="site_refresh_request",
            priority=95,
            candidate_id="cand_a",
        )
        opener = FakeOpener({"ok": True, "requests": [{"id": "req_b", "candidateId": "cand_b", "subjectName": "Signal Fox", "status": "pending"}]})
        lookup = lambda query: {"ok": True, "found": True, "sourceFile": {"candidateId": query["lookupValue"], "subjectName": "Signal Fox"}}
        result = {"sent": True, "sendResult": {"recommendationId": "rec_b"}, "sourceCounts": {}, "subject": "Signal Fox"}

        with mock.patch.object(refresh, "run_source_file_enrichment", return_value=result) as mocked:
            summary = refresh.process_site_refresh_requests(
                self.db,
                guild_id=1,
                environ={"BNL_SOURCE_FILE_READ_TOKEN": "read", "BNL_WEBSITE_BASE_URL": "https://site.test", "BNL_DOSSIER_INGEST_TOKEN": "ingest"},
                opener=opener,
                lookup_func=lookup,
            )

        self.assertEqual(summary["processed"], 1)
        self.assertEqual(mocked.call_args.kwargs["lookup_value"], "cand_b")
        rows = self.all_queue_rows()
        self.assertEqual({row["candidate_id"]: row["status"] for row in rows}, {"cand_a": "queued", "cand_b": "succeeded"})
        self.assertEqual([post["status"] for post in opener.posts], ["claimed", "completed"])

    def test_worker_reuses_persisted_candidate_identity_after_database_reopen(self):
        refresh.enqueue_source_file_refresh(
            self.db,
            guild_id=1,
            subject_name="Signal Fox",
            reason="site retry",
            evidence_source="site_refresh_request",
            candidate_id="cand_restart",
        )
        lookup = lambda query: {"ok": True, "found": True, "sourceFile": {"candidateId": "cand_restart"}}
        result = {"sent": True, "sendResult": {"recommendationId": "rec_restart"}, "sourceCounts": {}, "subject": "Signal Fox"}
        with mock.patch.object(refresh, "run_source_file_enrichment", return_value=result) as mocked:
            summary = refresh.process_source_file_refresh_queue(
                self.db,
                guild_id=1,
                lookup_func=lookup,
                environ={"BNL_DOSSIER_INGEST_TOKEN": "ingest"},
            )
        self.assertEqual(summary["processed"], 1)
        self.assertEqual(mocked.call_args.kwargs["lookup_key"], "candidateId")
        self.assertEqual(mocked.call_args.kwargs["lookup_value"], "cand_restart")

    def test_worker_without_candidate_identity_uses_subject_lookup(self):
        refresh.enqueue_source_file_refresh(self.db, guild_id=1, subject_name="Signal Fox", reason="automatic", evidence_source="test")
        lookup = lambda query: {"ok": True, "found": True, "sourceFile": {"subjectName": "Signal Fox"}}
        result = {"sent": True, "sendResult": {"recommendationId": "rec_subject"}, "sourceCounts": {}, "subject": "Signal Fox"}
        with mock.patch.object(refresh, "run_source_file_enrichment", return_value=result) as mocked:
            refresh.process_source_file_refresh_queue(self.db, guild_id=1, lookup_func=lookup, environ={"BNL_DOSSIER_INGEST_TOKEN": "ingest"})
        self.assertEqual(mocked.call_args.kwargs["lookup_key"], "subject")
        self.assertEqual(mocked.call_args.kwargs["lookup_value"], "Signal Fox")

    def test_refresh_now_failed_refresh_marks_site_failed(self):
        opener = FakeOpener({"ok": True, "requests": []})
        lookup = lambda query: {"ok": True, "found": True, "sourceFile": {"subjectName": "Signal Fox"}}
        with mock.patch.object(refresh, "run_source_file_enrichment", return_value={"sent": False, "sendResult": {"error": "site rejected"}, "status": "send_failed"}):
            result = refresh.process_source_file_refresh_now(
                self.db,
                {"requestId": "req_now", "subjectName": "Signal Fox", "normalizedSubjectKey": "signal-fox", "reason": "manual_retry", "source": "admin_manual_retry"},
                guild_id=1,
                environ={"BNL_SOURCE_FILE_READ_TOKEN": "read", "BNL_WEBSITE_BASE_URL": "https://site.test", "BNL_DOSSIER_INGEST_TOKEN": "ingest"},
                opener=opener,
                lookup_func=lookup,
            )
        self.assertEqual(result["status"], "failed")
        self.assertEqual([p["status"] for p in opener.posts], ["claimed", "failed"])
        self.assertIn("site rejected", opener.posts[-1]["failureReason"])

    def test_refresh_now_archive_failure_still_reports_failure(self):
        opener = FakeOpener({"ok": True, "requests": []})
        lookup = lambda query: {"ok": True, "found": True, "sourceFile": {"subjectName": "Signal Fox"}}
        with mock.patch.object(refresh, "run_source_file_enrichment", return_value={"sent": False, "recommendationSent": True, "archiveSent": False, "sendResult": {"recommendationId": "rec_partial", "ok": True}, "archiveResult": {"ok": False, "error": "archive rejected", "status": 500}, "archiveError": "archive rejected", "status": "archive_send_failed"}):
            result = refresh.process_source_file_refresh_now(
                self.db,
                {"requestId": "req_archive", "subjectName": "Signal Fox", "normalizedSubjectKey": "signal-fox", "reason": "manual_retry", "source": "admin_manual_retry"},
                guild_id=1,
                environ={"BNL_SOURCE_FILE_READ_TOKEN": "read", "BNL_WEBSITE_BASE_URL": "https://site.test", "BNL_DOSSIER_INGEST_TOKEN": "ingest"},
                opener=opener,
                lookup_func=lookup,
            )
        self.assertEqual(result["status"], "failed")
        self.assertFalse(result["archiveSent"])
        self.assertIn("archive rejected", result["archiveError"])
        self.assertEqual([p["status"] for p in opener.posts], ["claimed", "failed"])
        self.assertIn("archive rejected", opener.posts[-1]["failureReason"])

    def test_refresh_now_compact_recommendation_failure_reports_partial_success(self):
        opener = FakeOpener({"ok": True, "requests": []})
        lookup = lambda query: {"ok": True, "found": True, "sourceFile": {"subjectName": "Signal Fox"}}
        with mock.patch.object(refresh, "run_source_file_enrichment", return_value={"sent": False, "recommendationSent": False, "archiveSent": True, "sendResult": {"ok": False, "error": "compact rejected"}, "archiveResult": {"ok": True, "archiveId": "arc_partial", "status": 200}, "status": "partial_success", "partialSuccess": True, "partialSuccessReason": "compact_recommendation_rejected"}):
            result = refresh.process_source_file_refresh_now(
                self.db,
                {"requestId": "req_compact", "subjectName": "Signal Fox", "normalizedSubjectKey": "signal-fox", "reason": "manual_retry", "source": "admin_manual_retry"},
                guild_id=1,
                environ={"BNL_SOURCE_FILE_READ_TOKEN": "read", "BNL_WEBSITE_BASE_URL": "https://site.test", "BNL_DOSSIER_INGEST_TOKEN": "ingest"},
                opener=opener,
                lookup_func=lookup,
            )
        self.assertEqual(result["status"], "partial_success")
        self.assertTrue(result["ok"])
        self.assertTrue(result["archiveSent"])
        self.assertEqual(result["archiveId"], "arc_partial")
        self.assertEqual(result["failureReason"], "compact_recommendation_rejected")
        self.assertEqual([p["status"] for p in opener.posts], ["claimed", "completed"])

    def test_refresh_now_no_target_does_not_stay_claimed(self):
        opener = FakeOpener({"ok": True, "requests": []})
        lookup = lambda query: {"ok": True, "found": False}
        with mock.patch.object(refresh, "run_source_file_enrichment") as mocked:
            result = refresh.process_source_file_refresh_now(
                self.db,
                {"requestId": "req_now", "subjectName": "Signal Fox", "normalizedSubjectKey": "signal-fox", "reason": "opened_source_file", "source": "admin_source_file_open"},
                guild_id=1,
                environ={"BNL_SOURCE_FILE_READ_TOKEN": "read", "BNL_WEBSITE_BASE_URL": "https://site.test", "BNL_DOSSIER_INGEST_TOKEN": "ingest"},
                opener=opener,
                lookup_func=lookup,
            )
        mocked.assert_not_called()
        self.assertEqual(result["status"], "skipped")
        self.assertEqual(result["failureReason"], "no_target")
        self.assertEqual([p["status"] for p in opener.posts], ["claimed", "skipped"])
        self.assertEqual(opener.posts[-1]["failureReason"], "no_target")
        self.assertEqual(self.rows(), [])

    def test_refresh_now_fresh_cooldown_is_current_not_failed_for_admin_open(self):
        opener = FakeOpener({"ok": True, "requests": []})
        lookup = lambda query: {"ok": True, "found": True, "sourceFile": {"subjectName": "Signal Fox"}}
        with mock.patch.object(refresh, "run_source_file_enrichment", return_value={"sent": True, "recommendationSent": True, "archiveSent": True, "sendResult": {"recommendationId": "rec_first", "ok": True}, "archiveResult": {"archiveId": "arc_first", "ok": True, "status": 200}, "sourceCounts": {"conversations": 1}, "sourceTypes": ["conversations"], "subject": "Signal Fox", "qualityScore": 88}) as mocked:
            first = refresh.process_source_file_refresh_now(
                self.db,
                {"requestId": "req_first", "subjectName": "Signal Fox", "normalizedSubjectKey": "signal-fox", "reason": "opened_source_file", "source": "admin_source_file_open"},
                guild_id=1,
                environ={"BNL_SOURCE_FILE_READ_TOKEN": "read", "BNL_WEBSITE_BASE_URL": "https://site.test", "BNL_DOSSIER_INGEST_TOKEN": "ingest"},
                opener=opener,
                lookup_func=lookup,
            )
            with mock.patch.object(refresh.logging, "warning") as warning_mock:
                second = refresh.process_source_file_refresh_now(
                    self.db,
                    {"requestId": "req_second", "subjectName": "Signal Fox", "normalizedSubjectKey": "signal-fox", "reason": "opened_source_file", "source": "admin_source_file_open"},
                    guild_id=1,
                    environ={"BNL_SOURCE_FILE_READ_TOKEN": "read", "BNL_WEBSITE_BASE_URL": "https://site.test", "BNL_DOSSIER_INGEST_TOKEN": "ingest"},
                    opener=opener,
                    lookup_func=lookup,
                )
        self.assertEqual(first["status"], "success")
        self.assertEqual(first["recommendationId"], "rec_first")
        self.assertTrue(first["archiveSent"])
        self.assertEqual(first["archiveId"], "arc_first")
        self.assertEqual(second["status"], "success")
        self.assertEqual(second["failureReason"], "")
        self.assertEqual(second["local"]["items"][0]["reason"], "cooldown_current")
        self.assertEqual(second["recommendationId"], "rec_first")
        self.assertEqual(mocked.call_count, 1)
        self.assertEqual([p["status"] for p in opener.posts], ["claimed", "completed", "claimed", "completed"])
        warning_text = " ".join(str(call) for call in warning_mock.call_args_list)
        self.assertNotIn("source_refresh_now_failed", warning_text)
        self.assertEqual(self.rows(), [])

    def test_refresh_now_cooldown_after_failure_is_not_reported_current(self):
        conn = sqlite3.connect(self.db)
        try:
            refresh.ensure_source_file_refresh_schema(conn)
            conn.execute(
                f"INSERT OR REPLACE INTO {refresh.STATE_TABLE} (guild_id, subject_key, subject_name, last_refresh_status, last_failure_at, last_error, cooldown_until, updated_at) VALUES (1, 'signal-fox', 'Signal Fox', 'failed', 'now', 'site rejected', '2999-01-01T00:00:00+00:00', 'now')"
            )
            conn.commit()
        finally:
            conn.close()
        opener = FakeOpener({"ok": True, "requests": []})
        lookup = lambda query: {"ok": True, "found": True, "sourceFile": {"subjectName": "Signal Fox"}}
        with mock.patch.object(refresh, "run_source_file_enrichment") as mocked:
            result = refresh.process_source_file_refresh_now(
                self.db,
                {"requestId": "req_now", "subjectName": "Signal Fox", "normalizedSubjectKey": "signal-fox", "reason": "opened_source_file", "source": "admin_source_file_open"},
                guild_id=1,
                environ={"BNL_SOURCE_FILE_READ_TOKEN": "read", "BNL_WEBSITE_BASE_URL": "https://site.test", "BNL_DOSSIER_INGEST_TOKEN": "ingest"},
                opener=opener,
                lookup_func=lookup,
            )
        mocked.assert_not_called()
        self.assertEqual(result["status"], "skipped")
        self.assertEqual(result["failureReason"], "cooldown")
        self.assertEqual([p["status"] for p in opener.posts], ["claimed", "skipped"])
        self.assertEqual(opener.posts[-1]["failureReason"], "cooldown")

    def test_site_polling_cooldown_does_not_leave_request_claimed(self):
        conn = sqlite3.connect(self.db)
        try:
            refresh.ensure_source_file_refresh_schema(conn)
            conn.execute(
                f"INSERT OR REPLACE INTO {refresh.STATE_TABLE} (guild_id, subject_key, subject_name, cooldown_until, updated_at) VALUES (1, 'signal-fox', 'Signal Fox', '2999-01-01T00:00:00+00:00', 'now')"
            )
            conn.commit()
        finally:
            conn.close()
        opener = FakeOpener({"ok": True, "requests": [{"id": "req_1", "subjectName": "Signal Fox", "status": "pending", "reason": "stale_file", "source": "background"}]})
        lookup = lambda query: {"ok": True, "found": True, "sourceFile": {"subjectName": "Signal Fox"}}
        with mock.patch.object(refresh, "run_source_file_enrichment") as mocked:
            summary = refresh.process_site_refresh_requests(
                self.db,
                guild_id=1,
                environ={"BNL_SOURCE_FILE_READ_TOKEN": "read", "BNL_WEBSITE_BASE_URL": "https://site.test", "BNL_DOSSIER_INGEST_TOKEN": "ingest"},
                opener=opener,
                lookup_func=lookup,
            )
        mocked.assert_not_called()
        self.assertEqual(summary["skipped"], 1)
        self.assertEqual([p["status"] for p in opener.posts], ["claimed", "skipped"])
        self.assertEqual(opener.posts[-1]["failureReason"], "cooldown_deferred")


    def test_refresh_now_existing_future_not_before_recent_success_returns_current(self):
        conn = sqlite3.connect(self.db)
        try:
            refresh.ensure_source_file_refresh_schema(conn)
            conn.execute(
                f"INSERT INTO {refresh.QUEUE_TABLE} (guild_id, subject_key, subject_name, reason, evidence_source, evidence_count, priority, status, queued_at, updated_at, not_before_at, attempts, refresh_mode, created_by) VALUES (1, 'signal-fox', 'Signal Fox', 'old cooldown', 'source_refresh_now', 1, 10, 'cooldown', 'now', 'now', '2999-01-01T00:00:00+00:00', 0, 'site_open_request', 'test')"
            )
            conn.execute(
                f"INSERT OR REPLACE INTO {refresh.STATE_TABLE} (guild_id, subject_key, subject_name, last_refresh_completed_at, last_refresh_status, last_recommendation_id, cooldown_until, updated_at) VALUES (1, 'signal-fox', 'Signal Fox', '2026-01-01T00:00:00+00:00', 'succeeded', 'rec_recent', '2999-01-01T00:00:00+00:00', 'now')"
            )
            conn.commit()
        finally:
            conn.close()
        opener = FakeOpener({"ok": True, "requests": []})
        lookup = lambda query: {"ok": True, "found": True, "sourceFile": {"subjectName": "Signal Fox"}}
        with mock.patch.object(refresh, "run_source_file_enrichment") as mocked, mock.patch.object(refresh.logging, "warning") as warning_mock:
            result = refresh.process_source_file_refresh_now(
                self.db,
                {"requestId": "req_recent", "subjectName": "Signal Fox", "normalizedSubjectKey": "signal-fox", "reason": "opened_source_file", "source": "admin_source_file_open"},
                guild_id=1,
                environ={"BNL_SOURCE_FILE_READ_TOKEN": "read", "BNL_WEBSITE_BASE_URL": "https://site.test", "BNL_DOSSIER_INGEST_TOKEN": "ingest"},
                opener=opener,
                lookup_func=lookup,
            )
        mocked.assert_not_called()
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["recommendationId"], "rec_recent")
        self.assertEqual(result["local"]["items"][0]["reason"], "cooldown_current")
        self.assertEqual([p["status"] for p in opener.posts], ["claimed", "completed"])
        warning_text = " ".join(str(call) for call in warning_mock.call_args_list)
        self.assertNotIn("source_refresh_now_failed", warning_text)

    def test_manual_refresh_now_proceeds_when_case_report_missing_despite_current_cooldown(self):
        conn = sqlite3.connect(self.db)
        try:
            refresh.ensure_source_file_refresh_schema(conn)
            conn.execute(
                f"INSERT OR REPLACE INTO {refresh.STATE_TABLE} (guild_id, subject_key, subject_name, last_refresh_completed_at, last_refresh_status, last_recommendation_id, cooldown_until, updated_at) VALUES (1, 'signal-fox', 'Signal Fox', '2026-01-01T00:00:00+00:00', 'succeeded', 'rec_old', '2999-01-01T00:00:00+00:00', 'now')"
            )
            conn.commit()
        finally:
            conn.close()
        opener = FakeOpener({"ok": True, "requests": []})
        lookup = lambda query: {"ok": True, "found": True, "sourceFile": {"subjectName": "Signal Fox"}, "latestArchive": {"sourceCounts": {"conversations": 1}}}
        result_packet = {"sent": True, "recommendationSent": True, "archiveSent": True, "sendResult": {"recommendationId": "rec_new", "ok": True}, "archiveResult": {"archiveId": "arc_new", "ok": True, "status": 200}, "sourceCounts": {"conversations": 1}, "sourceTypes": ["conversations"], "subject": "Signal Fox", "qualityScore": 88}
        with mock.patch.object(refresh, "run_source_file_enrichment", return_value=result_packet) as mocked, mock.patch.object(refresh.logging, "info") as info_mock:
            result = refresh.process_source_file_refresh_now(
                self.db,
                {"requestId": "req_missing", "subjectName": "Signal Fox", "normalizedSubjectKey": "signal-fox", "reason": "opened_source_file", "source": "admin_source_file_open"},
                guild_id=1,
                environ={"BNL_SOURCE_FILE_READ_TOKEN": "read", "BNL_WEBSITE_BASE_URL": "https://site.test", "BNL_DOSSIER_INGEST_TOKEN": "ingest"},
                opener=opener,
                lookup_func=lookup,
            )
        self.assertEqual(mocked.call_count, 1)
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["recommendationId"], "rec_new")
        self.assertEqual(result["local"]["items"][0]["reason"], "case_report_backfill")
        self.assertEqual([p["status"] for p in opener.posts], ["claimed", "completed"])
        info_text = " ".join(str(call) for call in info_mock.call_args_list)
        self.assertIn("source_case_report_missing", info_text)
        self.assertIn("source_case_report_backfill_started", info_text)
        self.assertIn("source_case_report_backfill_succeeded", info_text)

    def test_existing_archive_with_case_report_present_remains_current(self):
        conn = sqlite3.connect(self.db)
        try:
            refresh.ensure_source_file_refresh_schema(conn)
            conn.execute(
                f"INSERT OR REPLACE INTO {refresh.STATE_TABLE} (guild_id, subject_key, subject_name, last_refresh_completed_at, last_refresh_status, last_recommendation_id, cooldown_until, updated_at) VALUES (1, 'signal-fox', 'Signal Fox', '2026-01-01T00:00:00+00:00', 'succeeded', 'rec_recent', '2999-01-01T00:00:00+00:00', 'now')"
            )
            conn.commit()
        finally:
            conn.close()
        opener = FakeOpener({"ok": True, "requests": []})
        lookup = lambda query: {"ok": True, "found": True, "sourceFile": {"subjectName": "Signal Fox"}, "latestArchive": {"sourceCounts": {"conversations": 1}, "subjectMemoryPacketV1": {"subjectName": "Signal Fox"}, "sourceFileCaseReportV1": {"subjectName": "Signal Fox"}}}
        with mock.patch.object(refresh, "run_source_file_enrichment") as mocked:
            result = refresh.process_source_file_refresh_now(
                self.db,
                {"requestId": "req_current", "subjectName": "Signal Fox", "normalizedSubjectKey": "signal-fox", "reason": "opened_source_file", "source": "admin_source_file_open"},
                guild_id=1,
                environ={"BNL_SOURCE_FILE_READ_TOKEN": "read", "BNL_WEBSITE_BASE_URL": "https://site.test", "BNL_DOSSIER_INGEST_TOKEN": "ingest"},
                opener=opener,
                lookup_func=lookup,
            )
        mocked.assert_not_called()
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["local"]["items"][0]["reason"], "cooldown_current")

    def test_refresh_now_existing_future_not_before_without_recent_success_attempts_or_fails_truthfully(self):
        conn = sqlite3.connect(self.db)
        try:
            refresh.ensure_source_file_refresh_schema(conn)
            conn.execute(
                f"INSERT INTO {refresh.QUEUE_TABLE} (guild_id, subject_key, subject_name, reason, evidence_source, evidence_count, priority, status, queued_at, updated_at, not_before_at, attempts, refresh_mode, created_by) VALUES (1, 'signal-fox', 'Signal Fox', 'old cooldown', 'source_refresh_now', 1, 10, 'cooldown', 'now', 'now', '2999-01-01T00:00:00+00:00', 0, 'site_open_request', 'test')"
            )
            conn.commit()
        finally:
            conn.close()
        opener = FakeOpener({"ok": True, "requests": []})
        lookup = lambda query: {"ok": True, "found": True, "sourceFile": {"subjectName": "Signal Fox"}}
        with mock.patch.object(refresh, "run_source_file_enrichment", return_value={"sent": False, "sendResult": {"error": "compact rejected"}, "status": "recommendation_send_failed"}) as mocked:
            result = refresh.process_source_file_refresh_now(
                self.db,
                {"requestId": "req_fail", "subjectName": "Signal Fox", "normalizedSubjectKey": "signal-fox", "reason": "opened_source_file", "source": "admin_source_file_open"},
                guild_id=1,
                environ={"BNL_SOURCE_FILE_READ_TOKEN": "read", "BNL_WEBSITE_BASE_URL": "https://site.test", "BNL_DOSSIER_INGEST_TOKEN": "ingest"},
                opener=opener,
                lookup_func=lookup,
            )
        self.assertEqual(mocked.call_count, 1)
        self.assertEqual(result["status"], "failed")
        self.assertIn("compact rejected", result["failureReason"])
        self.assertEqual([p["status"] for p in opener.posts], ["claimed", "failed"])

    def test_refresh_now_manual_retry_existing_future_not_before_bypasses_and_attempts(self):
        conn = sqlite3.connect(self.db)
        try:
            refresh.ensure_source_file_refresh_schema(conn)
            conn.execute(
                f"INSERT INTO {refresh.QUEUE_TABLE} (guild_id, subject_key, subject_name, reason, evidence_source, evidence_count, priority, status, queued_at, updated_at, not_before_at, attempts, refresh_mode, created_by) VALUES (1, 'signal-fox', 'Signal Fox', 'old cooldown', 'source_refresh_now', 1, 10, 'cooldown', 'now', 'now', '2999-01-01T00:00:00+00:00', 0, 'site_open_request', 'test')"
            )
            conn.commit()
        finally:
            conn.close()
        opener = FakeOpener({"ok": True, "requests": []})
        lookup = lambda query: {"ok": True, "found": True, "sourceFile": {"subjectName": "Signal Fox"}}
        with mock.patch.object(refresh, "run_source_file_enrichment", return_value={"sent": True, "recommendationSent": True, "archiveSent": True, "sendResult": {"recommendationId": "rec_retry", "ok": True}, "archiveResult": {"archiveId": "arc_retry", "ok": True, "status": 200}, "sourceCounts": {"conversations": 1}, "subject": "Signal Fox"}) as mocked:
            result = refresh.process_source_file_refresh_now(
                self.db,
                {"requestId": "req_retry", "subjectName": "Signal Fox", "normalizedSubjectKey": "signal-fox", "reason": "manual_retry", "source": "admin_manual_retry"},
                guild_id=1,
                environ={"BNL_SOURCE_FILE_READ_TOKEN": "read", "BNL_WEBSITE_BASE_URL": "https://site.test", "BNL_DOSSIER_INGEST_TOKEN": "ingest"},
                opener=opener,
                lookup_func=lookup,
            )
        self.assertEqual(mocked.call_count, 1)
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["recommendationId"], "rec_retry")
        self.assertEqual([p["status"] for p in opener.posts], ["claimed", "completed"])

    def test_refresh_now_dry_run_behavior_unchanged(self):
        opener = FakeOpener({"ok": True, "requests": []})
        with mock.patch.object(refresh, "run_source_file_enrichment") as mocked:
            result = refresh.process_source_file_refresh_now(
                self.db,
                {"requestId": "req_now", "subjectName": "Signal Fox", "normalizedSubjectKey": "signal-fox", "reason": "opened_source_file", "source": "admin_source_file_open"},
                guild_id=1,
                dry_run=True,
                environ={"BNL_SOURCE_FILE_READ_TOKEN": "read", "BNL_WEBSITE_BASE_URL": "https://site.test"},
                opener=opener,
            )
        mocked.assert_not_called()
        self.assertEqual(result["status"], "dry_run")
        self.assertEqual(opener.posts, [])
        self.assertEqual(self.rows(), [])

    def test_refresh_now_does_not_add_public_publishing_queue_payment_or_identity_flags(self):
        with open("bnl_source_file_refresh.py", encoding="utf-8") as handle:
            source = handle.read()
        for forbidden in ("publish_public", "queue_payment", "artist_account", "canonical_profile_merge", "identity_alias"):
            self.assertNotIn(forbidden, source)


class SourceFileRefreshNowRouteTests(unittest.TestCase):
    def call_handler(self, result):
        request = FakeRefreshNowRequest(
            {
                "requestId": "req_route",
                "subjectName": "Crow",
                "normalizedSubjectKey": "crow",
                "reason": "opened_source_file",
                "source": "admin_source_file_open",
            },
            headers={bnl01_bot.SOURCE_REFRESH_NOW_TOKEN_HEADER: "secret"},
        )
        with mock.patch.object(bnl01_bot, "is_source_refresh_now_token_valid", return_value=True), \
             mock.patch.object(bnl01_bot, "_resolve_force_pull_guild", return_value=123), \
             mock.patch.object(bnl01_bot, "process_source_file_refresh_now", return_value=result):
            return asyncio.run(bnl01_bot._handle_source_file_refresh_now(request))

    def test_route_returns_200_for_partial_success_with_failure_reason(self):
        result = {
            "ok": True,
            "status": "partial_success",
            "failureReason": "compact_recommendation_rejected",
            "reason": "compact_recommendation_rejected",
            "recommendation_id": None,
            "archive_id": "arc_crow",
            "archiveSent": True,
        }

        response = self.call_handler(result)
        body = decode_json_response(response)

        self.assertEqual(response.status, 200)
        self.assertEqual(body["status"], "partial_success")
        self.assertEqual(body["failureReason"], "compact_recommendation_rejected")
        self.assertEqual(body["reason"], "compact_recommendation_rejected")
        self.assertIsNone(body["recommendation_id"])
        self.assertEqual(body["archive_id"], "arc_crow")

    def test_route_still_returns_500_for_failed_status(self):
        response = self.call_handler({"ok": False, "status": "failed", "failureReason": "archive_rejected"})
        body = decode_json_response(response)

        self.assertEqual(response.status, 500)
        self.assertEqual(body["status"], "failed")
        self.assertEqual(body["failureReason"], "archive_rejected")


if __name__ == "__main__":
    unittest.main()
