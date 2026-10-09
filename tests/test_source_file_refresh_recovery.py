import asyncio
import json
import os
import sqlite3
import tempfile
import threading
import unittest
from unittest import mock

import bnl_source_file_refresh as refresh


class RefreshInterruption(BaseException):
    pass


class SourceFileRefreshRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="source-refresh-recovery-")
        self.db = os.path.join(self.root, "fixture.sqlite")
        self.env = {"BNL_DOSSIER_INGEST_TOKEN": "fixture-token"}
        self.job = refresh.enqueue_source_file_refresh(
            self.db, guild_id=1, subject_name="Test Member", reason="fixture",
            evidence_source="test", candidate_id="candidate-one",
        )["id"]

    def row(self):
        with sqlite3.connect(self.db) as conn:
            conn.row_factory = sqlite3.Row
            return dict(conn.execute(
                f"SELECT * FROM {refresh.QUEUE_TABLE} WHERE id=?", (self.job,)
            ).fetchone())

    def process(self, **kwargs):
        return refresh.process_source_file_refresh_queue(
            self.db, guild_id=1, environ=self.env, queue_id=self.job,
            lookup_func=lambda query: {"ok": True, "found": True}, **kwargs,
        )

    def success(self):
        return {"sent": True, "status": "sent", "recommendationSent": True,
                "archiveSent": True, "sendResult": {"ok": True, "recommendationId": "rec-one"},
                "archiveResult": {"ok": True, "archiveId": "archive-one", "status": 200},
                "sourceCounts": {"conversations": 2}, "sourceTypes": ["conversations"],
                "subject": "Test Member", "qualityScore": 80}

    def interrupt(self, side_effect):
        with mock.patch.object(refresh, "run_source_file_enrichment", side_effect=side_effect):
            with self.assertRaises(RefreshInterruption):
                self.process()
        self.assertEqual(self.row()["status"], "running")

    def test_disk_reopen_recovers_pre_delivery_without_new_job_identity(self):
        self.interrupt(RefreshInterruption())
        with mock.patch.object(refresh, "run_source_file_enrichment", return_value=self.success()) as build:
            result = self.process()
        self.assertEqual(result["processed"], 1)
        self.assertEqual(self.row()["attempts"], 2)
        self.assertEqual(self.row()["status"], "succeeded")
        self.assertEqual(self.row()["candidate_id"], "candidate-one")
        self.assertEqual(build.call_args.kwargs["lookup_value"], "candidate-one")

    def test_inflight_archive_is_held_and_manual_enqueue_cannot_bypass(self):
        def interrupted(*args, **kwargs):
            observer = kwargs.get("effect_observer")
            if observer:
                observer("archive", "before")
            raise RefreshInterruption()

        self.interrupt(interrupted)
        with mock.patch.object(refresh, "run_source_file_enrichment") as build:
            result = self.process(force=True, bypass_cooldown=True)
            queued = refresh.enqueue_source_file_refresh(
                self.db, guild_id=1, subject_name="Test Member", reason="manual",
                refresh_mode="operator_requested", candidate_id="candidate-one",
            )
            self.process(force=True, bypass_cooldown=True)
        self.assertEqual(self.row()["status"], "recovery_required")
        self.assertEqual(result["items"][0]["status"], "recovery_required")
        self.assertEqual(queued["id"], self.job)
        self.assertEqual(self.row()["attempts"], 1)
        build.assert_not_called()

    def test_partial_receipt_is_preserved_without_duplicate_delivery(self):
        def interrupted(*args, **kwargs):
            observer = kwargs.get("effect_observer")
            if observer:
                observer("archive", "before")
                observer("archive", "after", {"ok": True, "archiveId": "archive-one", "status": 200})
            raise RefreshInterruption()

        self.interrupt(interrupted)
        with mock.patch.object(refresh, "run_source_file_enrichment") as build:
            self.process()
        self.assertEqual(self.row()["status"], "recovery_required")
        effects = json.loads(self.row()["delivery_receipts_json"])
        self.assertEqual(effects["archive"]["archiveId"], "archive-one")
        self.assertTrue(effects["archive"]["ok"])
        build.assert_not_called()

    def test_completed_result_recovers_without_resend_after_terminal_write_failure(self):
        original = refresh._state_upsert

        def interrupted_state(conn, **kwargs):
            if kwargs["fields"].get("last_refresh_status") == "succeeded":
                raise RefreshInterruption()
            return original(conn, **kwargs)

        with mock.patch.object(refresh, "_state_upsert", side_effect=interrupted_state):
            self.interrupt(lambda *args, **kwargs: self.success())
        with mock.patch.object(refresh, "run_source_file_enrichment") as build:
            result = self.process()
        self.assertEqual(self.row()["status"], "succeeded")
        self.assertEqual(self.row()["attempts"], 1)
        state = refresh.get_refresh_state(self.db, 1, "test-member")
        self.assertEqual(state["last_recommendation_id"], "rec-one")
        self.assertEqual(state["last_evidence_count"], 2)
        build.assert_not_called()
        self.assertEqual(result["processed"], 1)

    def test_legacy_running_attempt_is_not_reset_or_replayed(self):
        with sqlite3.connect(self.db) as conn:
            conn.execute(f"UPDATE {refresh.QUEUE_TABLE} SET status='running', attempts=4 WHERE id=?", (self.job,))
        before = self.row()
        with mock.patch.object(refresh, "run_source_file_enrichment") as build:
            result = self.process(force=True, bypass_cooldown=True)
        self.assertEqual(self.row(), before)
        self.assertTrue(result["items"], "Legacy uncertainty must be reported, not silently ignored")
        self.assertEqual(result["items"][0]["status"], "recovery_required")
        build.assert_not_called()

    def test_two_threads_do_not_claim_or_clear_active_work(self):
        entered, release = threading.Event(), threading.Event()
        failures = []

        def slow(*args, **kwargs):
            entered.set()
            if not release.wait(5):
                raise AssertionError("fixture worker did not release")
            return self.success()

        def first():
            try:
                self.process()
            except BaseException as exc:
                failures.append(exc)

        with mock.patch.object(refresh, "run_source_file_enrichment", side_effect=slow) as build:
            worker = threading.Thread(target=first)
            worker.start()
            try:
                self.assertTrue(entered.wait(5))
                second = self.process(force=True, bypass_cooldown=True)
                cleared = refresh.clear_source_file_refresh(self.db, guild_id=1, subject_name="Test Member")
            finally:
                release.set()
                worker.join(5)
        self.assertFalse(worker.is_alive())
        self.assertEqual(failures, [])
        self.assertEqual(second["processed"], 0)
        self.assertEqual(cleared, 0)
        self.assertEqual(self.row()["attempts"], 1)
        self.assertEqual(self.row()["status"], "succeeded")
        self.assertEqual(build.call_count, 1)

    def test_stale_completion_cannot_overwrite_replacement_claim(self):
        def revoked(*args, **kwargs):
            with sqlite3.connect(self.db) as conn:
                conn.execute(f"UPDATE {refresh.QUEUE_TABLE} SET claim_token='replacement' WHERE id=?", (self.job,))
                conn.execute(f"UPDATE {refresh.STATE_TABLE} SET active_claim_token='replacement', last_refresh_status='newer' WHERE subject_key='test-member'")
            return self.success()

        with mock.patch.object(refresh, "run_source_file_enrichment", side_effect=revoked):
            self.process()
        self.assertEqual(self.row()["status"], "running")
        self.assertEqual(self.row()["claim_token"], "replacement")
        self.assertEqual(refresh.get_refresh_state(self.db, 1, "test-member")["last_refresh_status"], "newer")

    def test_partial_delivery_failure_is_not_retryable_by_force(self):
        def partial(*args, **kwargs):
            observer = kwargs.get("effect_observer")
            if observer:
                observer("archive", "before")
                observer("archive", "after", {"ok": False, "status": 503})
                observer("recommendation", "before")
                observer("recommendation", "after", {"ok": True, "recommendationId": "rec-one"})
            result = self.success()
            result.update(sent=False, archiveSent=False, status="archive_send_failed")
            return result

        with mock.patch.object(refresh, "run_source_file_enrichment", side_effect=partial) as build:
            self.process()
            self.process(force=True, bypass_cooldown=True)
        self.assertEqual(self.row()["status"], "recovery_required")
        self.assertEqual(build.call_count, 1)

    def test_dry_run_does_not_recover_or_change_claims(self):
        self.interrupt(RefreshInterruption())
        before = self.row()
        with mock.patch.object(refresh, "run_source_file_enrichment") as build:
            self.process(dry_run=True)
        self.assertEqual(self.row(), before)
        build.assert_not_called()

    def test_enqueue_cannot_overwrite_new_claim_from_stale_status_read(self):
        selected, resume = threading.Event(), threading.Event()
        original_connect = sqlite3.connect
        outcomes = []

        class PausedCursor:
            def __init__(self, cursor):
                self.cursor = cursor

            def fetchone(self):
                row = self.cursor.fetchone()
                selected.set()
                if not resume.wait(5):
                    raise AssertionError("enqueue fixture did not resume")
                return row

        class PausedConnection(sqlite3.Connection):
            def execute(self, sql, parameters=()):
                cursor = super().execute(sql, parameters)
                if threading.current_thread().name == "fixture-enqueue" and sql.startswith("SELECT * FROM source_file_refresh_queue"):
                    return PausedCursor(cursor)
                return cursor

        def connect(*args, **kwargs):
            return original_connect(*args, factory=PausedConnection, **kwargs)

        def enqueue():
            outcomes.append(refresh.enqueue_source_file_refresh(
                self.db, guild_id=1, subject_name="Test Member", reason="new signal",
                candidate_id="candidate-one",
            ))

        def interrupted(*args, **kwargs):
            kwargs["effect_observer"]("archive", "before")
            raise RefreshInterruption()

        with mock.patch.object(refresh.sqlite3, "connect", side_effect=connect):
            worker = threading.Thread(target=enqueue, name="fixture-enqueue")
            worker.start()
            try:
                self.assertTrue(selected.wait(5))
                self.interrupt(interrupted)
            finally:
                resume.set()
                worker.join(5)
        self.assertFalse(worker.is_alive())
        self.assertEqual(self.row()["status"], "running")
        self.assertEqual(outcomes[0]["status"], "running")
        with mock.patch.object(refresh, "run_source_file_enrichment") as build:
            self.process()
        self.assertEqual(self.row()["status"], "recovery_required")
        build.assert_not_called()

    def test_malformed_receipt_shape_is_not_proof_of_no_delivery(self):
        self.interrupt(RefreshInterruption())
        with sqlite3.connect(self.db) as conn:
            conn.execute(f"UPDATE {refresh.QUEUE_TABLE} SET delivery_receipts_json='null' WHERE id=?", (self.job,))
        with mock.patch.object(refresh, "run_source_file_enrichment") as build:
            self.process()
        self.assertEqual(self.row()["status"], "recovery_required")
        build.assert_not_called()

    def test_recovered_archive_only_result_preserves_partial_success(self):
        original = refresh._state_upsert

        def interrupted_state(conn, **kwargs):
            if kwargs["fields"].get("last_refresh_status") == "partial_success":
                raise RefreshInterruption()
            return original(conn, **kwargs)

        def partial(*args, **kwargs):
            observe = kwargs["effect_observer"]
            observe("archive", "before")
            observe("archive", "after", {"ok": True, "archiveId": "archive-one"})
            result = self.success()
            result.update(sent=False, recommendationSent=False, status="partial_success", sendResult={"ok": False})
            return result

        with mock.patch.object(refresh, "_state_upsert", side_effect=interrupted_state):
            self.interrupt(partial)
        with mock.patch.object(refresh, "run_source_file_enrichment") as build:
            result = self.process()
        self.assertEqual(result["items"][0]["status"], "partial_success")
        self.assertTrue(result["items"][0]["archiveSent"])
        self.assertFalse(result["items"][0]["recommendationSent"])
        self.assertEqual(result["items"][0]["archiveId"], "archive-one")
        build.assert_not_called()

    def test_refresh_now_does_not_report_old_success_for_busy_worker(self):
        with sqlite3.connect(self.db) as conn:
            refresh._state_upsert(conn, guild_id=1, subject_key="test-member", subject_name="Test Member", fields={"last_refresh_status": "succeeded", "last_refresh_completed_at": "2026-01-01T00:00:00+00:00", "last_recommendation_id": "old-rec", "cooldown_until": "2999-01-01T00:00:00+00:00", "candidate_id": "candidate-one"})
        with mock.patch.object(refresh, "process_source_file_refresh_queue", return_value={"ok": True, "items": [], "reason": "worker_busy"}):
            result = refresh.process_source_file_refresh_now(
                self.db, {"subjectName": "Test Member", "candidateId": "candidate-one", "source": "admin_manual_retry"},
                guild_id=1, environ=self.env, lookup_func=lambda query: {"ok": True, "found": True},
            )
        self.assertNotEqual(result["status"], "current")
        self.assertEqual(result["failureReason"], "worker_busy")

    def test_failed_intent_write_does_not_allow_an_external_effect(self):
        self.interrupt(RefreshInterruption())
        claim = self.row()["claim_token"]
        with sqlite3.connect(self.db) as conn:
            proxy = mock.Mock(wraps=conn)

            def execute(sql, parameters=()):
                if sql.startswith("UPDATE source_file_refresh_queue SET delivery_receipts_json="):
                    parameters = list(parameters)
                    parameters[2] = "not-the-current-claim"
                return conn.execute(sql, parameters)

            proxy.execute.side_effect = execute
            with self.assertRaises(refresh.SourceRefreshClaimLost):
                refresh._record_refresh_effect(proxy, self.job, claim, "archive", "before")

    def test_subject_scoped_manual_refresh_reports_legacy_uncertainty(self):
        with sqlite3.connect(self.db) as conn:
            conn.execute(f"UPDATE {refresh.QUEUE_TABLE} SET status='running', attempts=4 WHERE id=?", (self.job,))
        with mock.patch.object(refresh, "run_source_file_enrichment") as build:
            result = refresh.process_single_source_file_refresh(self.db, guild_id=1, subject_name="Test Member", environ=self.env)
        self.assertTrue(result["items"])
        self.assertEqual(result["items"][0]["status"], "recovery_required")
        build.assert_not_called()

    def test_restart_preserves_trusted_callback_destination(self):
        preview = "https://barcode-network-fixture.vercel.app"
        with mock.patch.object(refresh, "run_source_file_enrichment", side_effect=RefreshInterruption()):
            with self.assertRaises(RefreshInterruption):
                self.process(callback_base_url=preview)
        with mock.patch.object(refresh, "run_source_file_enrichment", return_value=self.success()) as build:
            self.process()
        self.assertEqual(build.call_args.kwargs["callback_base_url"], preview)

    def test_restart_holds_if_delivery_destination_configuration_changes(self):
        self.interrupt(RefreshInterruption())
        changed_env = dict(self.env, BNL_DOSSIER_INGEST_URL="https://barcode-network-fixture.vercel.app/api/bnl/dossier-recommendations")
        with mock.patch.object(refresh, "run_source_file_enrichment") as build:
            refresh.process_source_file_refresh_queue(self.db, guild_id=1, queue_id=self.job, environ=changed_env)
        self.assertEqual(self.row()["status"], "recovery_required")
        build.assert_not_called()

    def test_unclaimed_state_write_cannot_overwrite_concurrent_claim(self):
        with sqlite3.connect(self.db) as conn:
            refresh._state_upsert(conn, guild_id=1, subject_key="test-member", subject_name="Test Member", fields={"last_refresh_status": "succeeded"})
            conn.commit()
            proxy = mock.Mock(wraps=conn)

            def execute(sql, parameters=()):
                cursor = conn.execute(sql, parameters)
                if sql.startswith("SELECT active_claim_token FROM"):
                    row = cursor.fetchone()
                    with sqlite3.connect(self.db) as other:
                        other.execute(f"UPDATE {refresh.STATE_TABLE} SET active_claim_token='newer', last_refresh_status='running' WHERE subject_key='test-member'")
                    return mock.Mock(fetchone=mock.Mock(return_value=row))
                return cursor

            proxy.execute.side_effect = execute
            refresh._state_upsert(proxy, guild_id=1, subject_key="test-member", subject_name="Test Member", fields={"last_refresh_status": "no_target"})
        self.assertEqual(refresh.get_refresh_state(self.db, 1, "test-member")["last_refresh_status"], "running")

    def test_malformed_saved_result_is_held_without_blocking_unrelated_work(self):
        self.interrupt(RefreshInterruption())
        malformed = {"version": 1, "queue_id": self.job, "claim_token": self.row()["claim_token"], "status": "succeeded", "fields": {}, "item": {}}
        with sqlite3.connect(self.db) as conn:
            conn.execute(f"UPDATE {refresh.QUEUE_TABLE} SET result_receipt_json=? WHERE id=?", (json.dumps(malformed), self.job))
        other_job = refresh.enqueue_source_file_refresh(self.db, guild_id=1, subject_name="Second Test Member", reason="fixture")["id"]
        failure = None
        try:
            with mock.patch.object(refresh, "run_source_file_enrichment", return_value=self.success()):
                refresh.process_source_file_refresh_queue(self.db, guild_id=1, environ=self.env, lookup_func=lambda query: {"ok": True, "found": True})
        except Exception as exc:
            failure = type(exc).__name__
        self.assertIsNone(failure, "Malformed receipts must be quarantined, not stop the worker")
        self.assertEqual(self.row()["status"], "recovery_required")
        with sqlite3.connect(self.db) as conn:
            self.assertEqual(conn.execute(f"SELECT status FROM {refresh.QUEUE_TABLE} WHERE id=?", (other_job,)).fetchone()[0], "succeeded")

    def test_blocked_subject_siblings_do_not_starve_ready_subjects(self):
        with sqlite3.connect(self.db) as conn:
            conn.execute(f"UPDATE {refresh.QUEUE_TABLE} SET status='running', attempts=1 WHERE id=?", (self.job,))
        refresh.enqueue_source_file_refresh(self.db, guild_id=1, subject_name="Test Member", reason="new candidate", candidate_id="candidate-two", priority=100)
        other_job = refresh.enqueue_source_file_refresh(self.db, guild_id=1, subject_name="Second Test Member", reason="ready", priority=1)["id"]
        with mock.patch.object(refresh, "run_source_file_enrichment", return_value=self.success()) as build:
            result = refresh.process_source_file_refresh_queue(self.db, guild_id=1, environ=self.env, max_items=1, lookup_func=lambda query: {"ok": True, "found": True})
        self.assertEqual(result["processed"], 1)
        self.assertEqual(build.call_count, 1)
        with sqlite3.connect(self.db) as conn:
            self.assertEqual(conn.execute(f"SELECT status FROM {refresh.QUEUE_TABLE} WHERE id=?", (other_job,)).fetchone()[0], "succeeded")

    def test_operator_clear_does_not_claim_success_when_nothing_cleared(self):
        import bnl01_bot as bot

        message = mock.Mock()
        message.reply = mock.AsyncMock()
        with mock.patch.object(bot, "can_send_dossier_recommendation", return_value=True), mock.patch.object(bot, "resolve_channel_policy", return_value="internal_controlled"), mock.patch.object(bot, "is_public_prompt_context", return_value=False), mock.patch.object(bot, "clear_source_file_refresh", return_value=0):
            handled = asyncio.run(bot.maybe_handle_source_file_refresh_command(message, "!bnl source refresh clear Test Member"))
        self.assertTrue(handled)
        reply = message.reply.call_args.args[0]
        self.assertNotIn("queue cleared", reply)
        self.assertIn("left untouched", reply)

    def test_mismatched_subject_claim_blocks_effect_and_preserves_newer_state(self):
        self.interrupt(RefreshInterruption())
        claim = self.row()["claim_token"]
        with sqlite3.connect(self.db) as conn:
            conn.execute(f"UPDATE {refresh.STATE_TABLE} SET active_claim_token='newer', last_refresh_status='newer' WHERE subject_key='test-member'")
            conn.commit()
            with self.assertRaises(refresh.SourceRefreshClaimLost):
                refresh._record_refresh_effect(conn, self.job, claim, "archive", "before")
        with mock.patch.object(refresh, "run_source_file_enrichment") as build:
            self.process()
        self.assertEqual(self.row()["status"], "recovery_required")
        self.assertEqual(refresh.get_refresh_state(self.db, 1, "test-member")["last_refresh_status"], "newer")
        build.assert_not_called()

    def test_incomplete_success_receipt_is_not_a_completed_delivery(self):
        self.interrupt(RefreshInterruption())
        incomplete = {"version": 1, "queue_id": self.job, "claim_token": self.row()["claim_token"], "status": "succeeded", "error": None,
                      "fields": {"candidate_id": "candidate-one", "active_claim_token": None, "last_refresh_status": "succeeded"},
                      "item": {"status": "succeeded", "recommendationId": "", "recommendationSent": False, "archiveSent": False, "archiveId": ""}}
        with sqlite3.connect(self.db) as conn:
            conn.execute(f"UPDATE {refresh.QUEUE_TABLE} SET result_receipt_json=? WHERE id=?", (json.dumps(incomplete), self.job))
        with mock.patch.object(refresh, "run_source_file_enrichment") as build:
            self.process()
        self.assertEqual(self.row()["status"], "recovery_required")
        build.assert_not_called()

    def test_targeted_sibling_hold_cannot_fall_back_to_old_current_success(self):
        with sqlite3.connect(self.db) as conn:
            conn.execute(f"UPDATE {refresh.QUEUE_TABLE} SET status='running', attempts=1 WHERE id=?", (self.job,))
            refresh._state_upsert(conn, guild_id=1, subject_key="test-member", subject_name="Test Member", fields={"last_refresh_status": "succeeded", "last_refresh_completed_at": "2026-01-01T00:00:00+00:00", "last_recommendation_id": "old-rec", "cooldown_until": "2999-01-01T00:00:00+00:00", "candidate_id": "candidate-two"})
        with mock.patch.object(refresh, "run_source_file_enrichment") as build:
            result = refresh.process_source_file_refresh_now(
                self.db, {"subjectName": "Test Member", "candidateId": "candidate-two", "requiresCaseReportBackfill": True},
                guild_id=1, environ=self.env, lookup_func=lambda query: {"ok": True, "found": True},
            )
        self.assertEqual(result["localStatus"], "recovery_required")
        self.assertFalse(result["ok"])
        build.assert_not_called()

    def test_unsupported_saved_status_is_held_not_a_worker_exception(self):
        self.interrupt(RefreshInterruption())
        unsupported = {"version": 1, "queue_id": self.job, "claim_token": self.row()["claim_token"], "status": "unknown", "error": None, "fields": {}, "item": {}}
        with sqlite3.connect(self.db) as conn:
            conn.execute(f"UPDATE {refresh.QUEUE_TABLE} SET result_receipt_json=? WHERE id=?", (json.dumps(unsupported), self.job))
        failure = None
        try:
            with mock.patch.object(refresh, "run_source_file_enrichment") as build:
                self.process()
        except Exception as exc:
            failure = type(exc).__name__
        self.assertIsNone(failure)
        self.assertEqual(self.row()["status"], "recovery_required")
        build.assert_not_called()


if __name__ == "__main__":
    unittest.main()
