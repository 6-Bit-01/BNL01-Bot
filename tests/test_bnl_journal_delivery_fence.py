import json
import os
import sqlite3
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

os.environ.setdefault("GEMINI_API_KEY", "test-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-token")

import bnl_journal as journal
import bnl_journal_automation as automation
import bnl_journal_source_store as source_store


class AcceptedResponse:
    status = 200

    def __init__(self, request):
        submitted = json.loads(request.data.decode("utf-8"))["entry"]
        self._body = json.dumps(
            {
                "ok": True,
                "persisted": True,
                "idempotent": False,
                "entry": {
                    "entryId": submitted["entryId"],
                    "revision": submitted["revision"],
                    "contentHash": submitted["contentHash"],
                    "publishedAt": "2026-07-23T01:00:00Z",
                },
            }
        ).encode("utf-8")

    def read(self):
        return self._body

    def getcode(self):
        return self.status

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


class JournalDeliveryFenceTests(unittest.TestCase):
    def setUp(self):
        handle = tempfile.NamedTemporaryFile(delete=False)
        self.db = handle.name
        handle.close()
        journal.ensure_schema(self.db)
        automation.ensure_schema(self.db)
        self.guild_id = 1
        self.entry_id = "journal_daily_2026-07-21_fenced"
        self.revision = 1
        self.run_id = "jrun_delivery_fence_test"
        self.delivery_epoch = 7
        self.canonical = self._stage_approved_revision_and_run()

    def tearDown(self):
        for path in (self.db, self.db + ".journal-privacy.lock"):
            try:
                os.unlink(path)
            except FileNotFoundError:
                pass

    @staticmethod
    def _iso(value):
        return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace(
            "+00:00", "Z"
        )

    def _stage_approved_revision_and_run(self):
        packet = {
            "entryKind": "daily",
            "sourceWindowStart": "2026-07-21T02:00:00Z",
            "sourceWindowEnd": "2026-07-22T02:00:00Z",
            "privateSources": [],
            "history": {},
            "aggregateCounts": {},
        }
        article = {
            "title": "Fenced Delivery",
            "excerpt": "A prepared Journal payload used to exercise one delivery owner.",
            "sections": [
                {
                    "heading": "Signal",
                    "body": "The prepared signal is released exactly once by its durable owner.",
                    "sourceRefIds": [],
                }
            ],
            "sourceRefIds": {"Signal": []},
            "metadata": {},
        }
        entry_row, metadata_row, draft = journal._draft_records(
            self.guild_id,
            packet,
            article,
            entry_id=self.entry_id,
            revision=self.revision,
        )
        with sqlite3.connect(self.db) as conn:
            journal._insert_draft_rows(conn, entry_row, metadata_row)
        approved = journal.approve_draft(
            self.db,
            self.guild_id,
            self.entry_id,
            draft.content_hash,
            revision=self.revision,
        )
        self.assertTrue(approved.ok, approved)
        now = datetime.now(timezone.utc)
        with sqlite3.connect(self.db) as conn:
            conn.execute(
                "INSERT INTO bnl_journal_automation_runs("
                "run_id,guild_id,cadence,source_window_start,source_window_end,lifecycle_state,reason,"
                "journal_entry_id,journal_revision,aggregate_counts_json,attempt_count,delivery_epoch,"
                "delivery_lease_expires_at,created_at,updated_at) "
                "VALUES(?,?,?,?,?,'delivering','delivery_in_progress',?,?, '{}',1,?,?,?,?)",
                (
                    self.run_id,
                    self.guild_id,
                    "daily",
                    packet["sourceWindowStart"],
                    packet["sourceWindowEnd"],
                    self.entry_id,
                    self.revision,
                    self.delivery_epoch,
                    self._iso(now + timedelta(minutes=2)),
                    self._iso(now),
                    self._iso(now),
                ),
            )
            canonical = conn.execute(
                "SELECT canonical_payload_bytes FROM bnl_journal_entries "
                "WHERE guild_id=? AND entry_id=? AND revision=?",
                (self.guild_id, self.entry_id, self.revision),
            ).fetchone()[0]
        return bytes(canonical)

    def _states(self):
        with sqlite3.connect(self.db) as conn:
            entry_state = conn.execute(
                "SELECT lifecycle_state FROM bnl_journal_entries "
                "WHERE guild_id=? AND entry_id=? AND revision=?",
                (self.guild_id, self.entry_id, self.revision),
            ).fetchone()[0]
            metadata_state = conn.execute(
                "SELECT lifecycle_state FROM bnl_journal_private_metadata "
                "WHERE guild_id=? AND entry_id=? AND revision=?",
                (self.guild_id, self.entry_id, self.revision),
            ).fetchone()[0]
        return entry_state, metadata_state

    def test_live_matching_fence_posts_and_updates_revision_atomically(self):
        posts = []

        def opener(request, timeout=10):
            posts.append(request.data)
            return AcceptedResponse(request)

        result = journal.deliver_approved(
            self.db,
            self.guild_id,
            self.entry_id,
            "https://site.example",
            "key",
            opener=opener,
            revision=self.revision,
            delivery_fence=(self.run_id, self.delivery_epoch),
        )

        self.assertTrue(result.ok, result)
        self.assertEqual("published", result.status)
        self.assertEqual([self.canonical], posts)
        self.assertEqual(("published", "published"), self._states())

    def test_superseded_or_expired_fence_makes_zero_posts(self):
        posts = []

        def forbidden_opener(request, timeout=10):
            posts.append(request.data)
            raise AssertionError("lost delivery owner reached the network")

        superseded = journal.deliver_approved(
            self.db,
            self.guild_id,
            self.entry_id,
            "https://site.example",
            "key",
            opener=forbidden_opener,
            revision=self.revision,
            delivery_fence=(self.run_id, self.delivery_epoch - 1),
        )
        with sqlite3.connect(self.db) as conn:
            conn.execute(
                "UPDATE bnl_journal_automation_runs SET delivery_lease_expires_at=? WHERE run_id=?",
                ("2000-01-01T00:00:00Z", self.run_id),
            )
        expired = journal.deliver_approved(
            self.db,
            self.guild_id,
            self.entry_id,
            "https://site.example",
            "key",
            opener=forbidden_opener,
            revision=self.revision,
            delivery_fence=(self.run_id, self.delivery_epoch),
        )

        self.assertEqual("delivery_epoch_lost", superseded.reason)
        self.assertEqual("delivery_epoch_lost", expired.reason)
        self.assertEqual([], posts)
        self.assertEqual(
            ("approved_pending_delivery", "approved_pending_delivery"),
            self._states(),
        )

    def test_unfenced_manual_delivery_keeps_existing_behavior(self):
        posts = []

        def opener(request, timeout=10):
            posts.append(request.data)
            return AcceptedResponse(request)

        result = journal.deliver_approved(
            self.db,
            self.guild_id,
            self.entry_id,
            "https://site.example",
            "key",
            opener=opener,
            revision=self.revision,
        )

        self.assertTrue(result.ok, result)
        self.assertEqual([self.canonical], posts)
        self.assertEqual(("published", "published"), self._states())

    def test_privacy_purge_waits_for_journal_gate_while_unrelated_writes_continue(self):
        subject_ref = "discord_user:100"
        source_store.record_source_event(
            self.db,
            guild_id=self.guild_id,
            source_kind="discord_message",
            source_key="delivery-fence-privacy-source",
            occurred_at_ms=1_753_159_200_000,
            raw_text="A member shared a public music observation.",
            sanitized_summary="A member shared a public music observation.",
            channel_policy="public_home",
            subject_ref=subject_ref,
            public_usable=True,
        )
        with sqlite3.connect(self.db) as conn:
            conn.execute(
                "CREATE TABLE unrelated_delivery_fence_writes "
                "(id INTEGER PRIMARY KEY, note TEXT NOT NULL)"
            )
            metadata_raw = conn.execute(
                "SELECT metadata_json FROM bnl_journal_private_metadata "
                "WHERE guild_id=? AND entry_id=? AND revision=?",
                (self.guild_id, self.entry_id, self.revision),
            ).fetchone()[0]
            metadata = json.loads(metadata_raw)
            metadata.update(
                {
                    "subjectRefs": [subject_ref],
                    "supportingConversationRefs": [
                        {"refId": "fresh:1", "subjectRef": subject_ref}
                    ],
                    "sourceSummaries": [
                        {
                            "refId": "fresh:1",
                            "summary": "A member shared a public music observation.",
                        }
                    ],
                }
            )
            conn.execute(
                "UPDATE bnl_journal_private_metadata SET metadata_json=? "
                "WHERE guild_id=? AND entry_id=? AND revision=?",
                (
                    json.dumps(metadata, sort_keys=True, separators=(",", ":")),
                    self.guild_id,
                    self.entry_id,
                    self.revision,
                ),
            )
            public_before = conn.execute(
                "SELECT title,excerpt,sections_json,public_payload_json,canonical_payload_bytes,content_hash "
                "FROM bnl_journal_entries WHERE guild_id=? AND entry_id=? AND revision=?",
                (self.guild_id, self.entry_id, self.revision),
            ).fetchone()

        post_entered = threading.Event()
        allow_response = threading.Event()
        purge_started = threading.Event()
        purge_acquired = threading.Event()
        posts = []

        def blocking_opener(request, timeout=10):
            posts.append(request.data)
            post_entered.set()
            self.assertTrue(allow_response.wait(timeout=5), "test never released website response")
            return AcceptedResponse(request)

        def purge_member_sources_and_derivatives():
            purge_started.set()
            with source_store.journal_release_privacy_fence(self.db):
                purge_acquired.set()
                with sqlite3.connect(self.db, timeout=10) as conn:
                    conn.execute("BEGIN IMMEDIATE")
                    removed = source_store.purge_user_discord_sources_on_connection(
                        conn,
                        guild_id=self.guild_id,
                        user_id=100,
                    )
                    counts = journal.purge_user_journal_derivatives_on_connection(
                        conn,
                        guild_id=self.guild_id,
                        user_id=100,
                    )
                    conn.commit()
            return removed, counts

        with ThreadPoolExecutor(max_workers=2) as pool:
            delivery_future = pool.submit(
                journal.deliver_approved,
                self.db,
                self.guild_id,
                self.entry_id,
                "https://site.example",
                "key",
                blocking_opener,
                10,
                self.revision,
                delivery_fence=(self.run_id, self.delivery_epoch),
            )
            self.assertTrue(post_entered.wait(timeout=5), "fenced delivery never reached POST")
            purge_future = pool.submit(purge_member_sources_and_derivatives)
            self.assertTrue(purge_started.wait(timeout=5), "privacy purge thread never started")
            self.assertFalse(
                purge_acquired.wait(timeout=0.1),
                "privacy purge crossed the Journal gate before delivery finished",
            )
            self.assertFalse(purge_future.done())
            with sqlite3.connect(self.db, timeout=1) as conn:
                conn.execute(
                    "INSERT INTO unrelated_delivery_fence_writes(note) VALUES('continued')"
                )
            with sqlite3.connect(self.db) as conn:
                self.assertEqual(
                    1,
                    conn.execute(
                        "SELECT COUNT(*) FROM unrelated_delivery_fence_writes"
                    ).fetchone()[0],
                )
            allow_response.set()
            delivered = delivery_future.result(timeout=5)
            removed, counts = purge_future.result(timeout=5)

        self.assertTrue(delivered.ok, delivered)
        self.assertEqual("published", delivered.status)
        self.assertEqual([self.canonical], posts)
        self.assertEqual(1, removed)
        self.assertEqual(1, counts.get("bnl_journal_published_metadata_scrubbed"))
        self.assertTrue(purge_acquired.is_set())
        with sqlite3.connect(self.db) as conn:
            entry_after = conn.execute(
                "SELECT lifecycle_state,title,excerpt,sections_json,public_payload_json,"
                "canonical_payload_bytes,content_hash FROM bnl_journal_entries "
                "WHERE guild_id=? AND entry_id=? AND revision=?",
                (self.guild_id, self.entry_id, self.revision),
            ).fetchone()
            metadata_state, metadata_raw = conn.execute(
                "SELECT lifecycle_state,metadata_json FROM bnl_journal_private_metadata "
                "WHERE guild_id=? AND entry_id=? AND revision=?",
                (self.guild_id, self.entry_id, self.revision),
            ).fetchone()
        self.assertEqual("published", entry_after[0])
        self.assertEqual(tuple(public_before), tuple(entry_after[1:]))
        self.assertEqual("published", metadata_state)
        scrubbed = json.loads(metadata_raw)
        self.assertTrue(scrubbed.get("privacyScrubbed"))
        self.assertNotIn(subject_ref, json.dumps(scrubbed, sort_keys=True))


if __name__ == "__main__":
    unittest.main()
