import os
import sqlite3
import tempfile
import unittest
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl_source_file_refresh as refresh
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
        with mock.patch.object(refresh, "run_source_file_enrichment", return_value={"sent": True, "sendResult": {"recommendationId": "rec_now"}, "sourceCounts": {"conversations": 1}, "sourceTypes": ["conversations"], "subject": "Signal Fox", "qualityScore": 88}) as mocked:
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
        self.assertEqual(mocked.call_count, 1)
        self.assertEqual([p["status"] for p in opener.posts], ["claimed", "completed"])
        self.assertEqual(opener.posts[-1]["completedByRecommendationId"], "rec_now")

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

    def test_refresh_now_cooldown_bypassed_for_admin_open(self):
        conn = sqlite3.connect(self.db)
        try:
            refresh.ensure_source_file_refresh_schema(conn)
            conn.execute(
                f"INSERT OR REPLACE INTO {refresh.STATE_TABLE} (guild_id, subject_key, subject_name, cooldown_until, updated_at) VALUES (1, 'signal-fox', 'Signal Fox', '2999-01-01T00:00:00+00:00', 'now')"
            )
            conn.commit()
        finally:
            conn.close()
        opener = FakeOpener({"ok": True, "requests": []})
        lookup = lambda query: {"ok": True, "found": True, "sourceFile": {"subjectName": "Signal Fox"}}
        with mock.patch.object(refresh, "run_source_file_enrichment", return_value={"sent": True, "sendResult": {"recommendationId": "rec_fresh"}, "sourceCounts": {"conversations": 1}, "sourceTypes": ["conversations"], "subject": "Signal Fox", "qualityScore": 88}):
            result = refresh.process_source_file_refresh_now(
                self.db,
                {"requestId": "req_now", "subjectName": "Signal Fox", "normalizedSubjectKey": "signal-fox", "reason": "opened_source_file", "source": "admin_source_file_open"},
                guild_id=1,
                environ={"BNL_SOURCE_FILE_READ_TOKEN": "read", "BNL_WEBSITE_BASE_URL": "https://site.test", "BNL_DOSSIER_INGEST_TOKEN": "ingest"},
                opener=opener,
                lookup_func=lookup,
            )
        self.assertEqual(result["status"], "success")
        self.assertEqual([p["status"] for p in opener.posts], ["claimed", "completed"])

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


if __name__ == "__main__":
    unittest.main()
