import asyncio
import json
import multiprocessing
import os
import sqlite3
import tempfile
import threading
import unittest
from unittest import mock

import bnl_source_file_refresh as refresh


def _lookup(query):
    return {"ok": True, "found": True}


def _success():
    return {
        "sent": True,
        "status": "sent",
        "recommendationSent": True,
        "archiveSent": True,
        "sendResult": {"ok": True, "recommendationId": "rec_test_member"},
        "archiveResult": {"ok": True, "archiveId": "arc_test_member", "status": 200},
        "sourceCounts": {"conversations": 1},
        "sourceTypes": ["conversations"],
        "subject": "Test Member",
        "qualityScore": 80,
    }


def _process(db_path, job_id, **kwargs):
    return refresh.process_source_file_refresh_queue(
        db_path,
        guild_id=1,
        queue_id=job_id,
        environ={"BNL_DOSSIER_INGEST_TOKEN": "fixture-token"},
        lookup_func=_lookup,
        **kwargs,
    )


def _exit_before_effects(db_path, job_id):
    with mock.patch.object(
        refresh, "run_source_file_enrichment", side_effect=lambda *args, **kwargs: os._exit(23),
    ):
        _process(db_path, job_id)


def _probe_active_fence(db_path, job_id, result_channel):
    calls = []

    def enrichment(*args, **kwargs):
        calls.append(1)
        return _success()

    try:
        with mock.patch.object(refresh, "run_source_file_enrichment", side_effect=enrichment):
            summary = _process(db_path, job_id, force=True, bypass_cooldown=True)
            cleared = refresh.clear_source_file_refresh(
                db_path, guild_id=1, subject_name="Test Member",
            )
        result_channel.send({
            "summary": summary, "cleared": cleared, "enrichment_calls": len(calls),
        })
    except BaseException as exc:
        result_channel.send({"error_type": type(exc).__name__, "error": str(exc)})
    finally:
        result_channel.close()


@unittest.skipUnless(os.name == "posix" and "fork" in multiprocessing.get_all_start_methods(), "Linux process fence required")
class SourceFileRefreshFenceTests(unittest.TestCase):
    def setUp(self):
        scratch = next((os.environ[name] for name in ("TMPDIR", "TEMP", "TMP") if os.environ.get(name)), None)
        self.assertTrue(scratch and os.path.isabs(scratch), "An absolute task scratch directory is required")
        self.root = tempfile.mkdtemp(prefix="source-refresh-fence-", dir=scratch)
        self.db = os.path.join(self.root, "fixture.sqlite")
        self.job = refresh.enqueue_source_file_refresh(
            self.db,
            guild_id=1,
            subject_name="Test Member",
            reason="fixture",
            evidence_source="test",
            candidate_id="candidate_test_member",
        )["id"]

    def row(self):
        with sqlite3.connect(self.db) as conn:
            conn.row_factory = sqlite3.Row
            return dict(conn.execute(
                f"SELECT * FROM {refresh.QUEUE_TABLE} WHERE id=?", (self.job,),
            ).fetchone())

    def _join_process(self, process):
        process.join(10)
        if process.is_alive():
            process.terminate()
            process.join(5)
            self.fail("Synthetic child process did not finish")

    def _probe_from_new_process(self):
        context = multiprocessing.get_context("spawn")
        reader, writer = context.Pipe(duplex=False)
        process = context.Process(target=_probe_active_fence, args=(self.db, self.job, writer))
        process.start()
        writer.close()
        try:
            self.assertTrue(reader.poll(10), "Synthetic fence probe did not respond")
            result = reader.recv()
            self._join_process(process)
            self.assertEqual(process.exitcode, 0)
            self.assertNotIn("error_type", result)
            return result
        finally:
            reader.close()
            if process.is_alive():
                process.terminate()
                process.join(5)

    def test_process_exit_releases_fence_and_recovers_original_pre_delivery_job(self):
        context = multiprocessing.get_context("fork")
        process = context.Process(target=_exit_before_effects, args=(self.db, self.job))
        process.start()
        self._join_process(process)

        self.assertEqual(process.exitcode, 23)
        interrupted = self.row()
        self.assertEqual(interrupted["id"], self.job)
        self.assertEqual(interrupted["status"], "running")
        self.assertEqual(interrupted["attempts"], 1)
        self.assertEqual(json.loads(interrupted["delivery_receipts_json"]), {})
        self.assertIsNone(interrupted["result_receipt_json"])
        calls = []

        def enrichment(*args, **kwargs):
            calls.append(1)
            return _success()

        with mock.patch.object(refresh, "run_source_file_enrichment", side_effect=enrichment):
            summary = _process(self.db, self.job)

        completed = self.row()
        self.assertEqual(calls, [1])
        self.assertEqual(summary["processed"], 1)
        self.assertEqual(completed["id"], self.job)
        self.assertEqual(completed["status"], "succeeded")
        self.assertEqual(completed["attempts"], 2)
        self.assertEqual(completed["candidate_id"], "candidate_test_member")
        with sqlite3.connect(self.db) as conn:
            self.assertEqual(conn.execute(f"SELECT COUNT(*) FROM {refresh.QUEUE_TABLE}").fetchone()[0], 1)

    def test_cancelled_awaiter_keeps_thread_fence_until_worker_completion(self):
        entered, release, finished = threading.Event(), threading.Event(), threading.Event()
        calls, completions, failures = [], [], []

        def paused_enrichment(*args, **kwargs):
            calls.append(1)
            entered.set()
            if not release.wait(15):
                raise AssertionError("Synthetic worker was not released")
            return _success()

        def worker():
            try:
                completions.append(_process(self.db, self.job))
            except BaseException as exc:
                failures.append(exc)
            finally:
                finished.set()

        async def cancel_and_probe():
            awaiter = asyncio.create_task(asyncio.to_thread(worker))
            try:
                self.assertTrue(await asyncio.to_thread(entered.wait, 5), "Synthetic worker did not start")
                awaiter.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await awaiter
                self.assertFalse(finished.is_set())
                self.assertEqual(self.row()["status"], "running")
                self.assertEqual(self.row()["attempts"], 1)

                probe = await asyncio.to_thread(self._probe_from_new_process)

                self.assertEqual(probe["summary"]["reason"], "worker_busy")
                self.assertEqual(probe["summary"]["processed"], 0)
                self.assertEqual(probe["enrichment_calls"], 0)
                self.assertEqual(probe["cleared"], 0)
                self.assertFalse(finished.is_set())
                self.assertEqual(self.row()["status"], "running")
                self.assertEqual(self.row()["attempts"], 1)
            finally:
                release.set()
                self.assertTrue(await asyncio.to_thread(finished.wait, 10), "Synthetic worker did not complete")
                if not awaiter.done():
                    awaiter.cancel()
                try:
                    await awaiter
                except asyncio.CancelledError:
                    pass

        with mock.patch.object(refresh, "run_source_file_enrichment", side_effect=paused_enrichment):
            asyncio.run(cancel_and_probe())

        self.assertEqual(failures, [])
        self.assertEqual(calls, [1])
        self.assertEqual(len(completions), 1)
        self.assertEqual(completions[0]["processed"], 1)
        self.assertEqual(self.row()["status"], "succeeded")
        self.assertEqual(self.row()["attempts"], 1)


if __name__ == "__main__":
    unittest.main()
