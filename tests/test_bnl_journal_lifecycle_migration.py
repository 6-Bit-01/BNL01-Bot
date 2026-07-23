import hashlib
import json
import os
import sqlite3
import tempfile
import unittest
from datetime import date, datetime, timedelta
from unittest.mock import patch

os.environ.setdefault("GEMINI_API_KEY", "test-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-token")

import bnl_journal as journal
import bnl_journal_automation as automation
import bnl_journal_source_store as source_store
from tests.test_bnl_journal_prepared_release import AcceptedResponse, article_json


TARGET_DAY = date(2026, 7, 19)


class JournalLifecycleMigrationTests(unittest.TestCase):
    def setUp(self):
        self.archive_clock = patch.object(source_store, "_now_ms", return_value=0)
        self.archive_clock.start()
        tmp = tempfile.NamedTemporaryFile(delete=False)
        self.db = tmp.name
        tmp.close()
        journal.ensure_schema(self.db)
        automation.ensure_schema(self.db)
        with sqlite3.connect(self.db) as conn:
            conn.execute(
                "CREATE TABLE website_relay_history("
                "relay_id TEXT PRIMARY KEY,guild_id INTEGER,public_message TEXT,"
                "public_directive TEXT,mode TEXT,relay_lane TEXT,event_type TEXT,"
                "highest_source_conversation_id INTEGER,normalized_message TEXT,"
                "semantic_family TEXT,published_timestamp TEXT)"
            )
            conn.execute(
                "CREATE TABLE conversations("
                "id INTEGER PRIMARY KEY,user_id INTEGER,user_name TEXT,guild_id INTEGER,"
                "channel_name TEXT,channel_policy TEXT,role TEXT,content TEXT,timestamp TEXT,"
                "public_usable INTEGER,visibility TEXT)"
            )
        self.add_day(TARGET_DAY)

    def tearDown(self):
        self.archive_clock.stop()
        try:
            os.unlink(self.db)
        except FileNotFoundError:
            pass

    def add_day(self, day: date) -> None:
        start, _, _ = automation._daily_period_for_day(day)
        start_utc = datetime.fromisoformat(start.replace("Z", "+00:00"))
        label = day.isoformat()
        with sqlite3.connect(self.db) as conn:
            for index in range(6):
                stamp = (start_utc + timedelta(hours=1, minutes=index)).isoformat().replace(
                    "+00:00", "Z"
                )
                conn.execute(
                    "INSERT INTO website_relay_history VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        f"relay-{label}-{index}",
                        1,
                        f"A public producer shared track activity number {index}.",
                        "Keep listening to the community music room.",
                        "OBSERVATION",
                        "current_signal",
                        "fresh_public_discord_activity",
                        index,
                        "track activity",
                        "public_discord_activity",
                        stamp,
                    ),
                )
                conn.execute(
                    "INSERT INTO conversations VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        int(label.replace("-", "")) * 10 + index,
                        100 + index,
                        f"Member{index}",
                        1,
                        "general",
                        "public_home",
                        "user",
                        (
                            f"Member{index} discusses a new mix, bass movement, and "
                            f"community listening plan number {index}."
                        ),
                        stamp,
                        1,
                        "public_safe",
                    ),
                )

    def packet(self) -> dict:
        start, end, _ = automation._daily_period_for_day(TARGET_DAY)
        return journal.build_source_packet_between(
            self.db,
            1,
            start,
            end,
            entry_kind="daily",
        )

    def prepare_daily(self, generator=None):
        return automation.prepare_daily(
            self.db,
            1,
            generator or (lambda packet, _prompt: article_json(packet)),
            target_day=TARGET_DAY,
            force=True,
        )

    def release_daily(self, opener):
        return automation.release_daily(
            self.db,
            1,
            "https://site.example",
            "key",
            target_day=TARGET_DAY,
            force=True,
            opener=opener,
        )

    def entry_row(self, entry_id: str, revision: int) -> dict:
        with sqlite3.connect(self.db) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM bnl_journal_entries "
                "WHERE guild_id=1 AND entry_id=? AND revision=?",
                (entry_id, int(revision)),
            ).fetchone()
        return dict(row) if row else {}

    def run_row(self) -> dict:
        with sqlite3.connect(self.db) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM bnl_journal_automation_runs WHERE guild_id=1 AND cadence='daily'"
            ).fetchone()
        return dict(row) if row else {}

    def test_legacy_delivery_failed_revision_is_adopted_and_released_without_generation(self):
        packet = self.packet()
        self.assertTrue(packet["sourceArchiveAvailable"])
        start, end, label = automation._daily_period_for_day(TARGET_DAY)
        entry_id = automation._entry_id(1, "daily", label)
        generated = journal.generate_and_store_packet_draft(
            self.db,
            1,
            packet,
            lambda current, _prompt: article_json(current),
            entry_id=entry_id,
        )
        self.assertTrue(generated.ok, generated)
        approved = journal.approve_draft(
            self.db,
            1,
            entry_id,
            generated.content_hash,
            revision=generated.revision,
        )
        self.assertTrue(approved.ok, approved)
        before = bytes(self.entry_row(entry_id, approved.revision)["canonical_payload_bytes"])
        with sqlite3.connect(self.db) as conn:
            legacy_metadata = json.loads(
                conn.execute(
                    "SELECT metadata_json FROM bnl_journal_private_metadata "
                    "WHERE guild_id=1 AND entry_id=? AND revision=?",
                    (entry_id, approved.revision),
                ).fetchone()[0]
            )
        self.assertFalse(legacy_metadata.get("frozenSourceHash"))

        run_id = automation._run_id(1, "daily", start, end)
        now = journal.utc_now_iso()
        with sqlite3.connect(self.db) as conn:
            conn.execute(
                "UPDATE bnl_journal_entries SET lifecycle_state='delivery_failed',"
                "delivery_status='retryable_delivery_failure' "
                "WHERE guild_id=1 AND entry_id=? AND revision=?",
                (entry_id, approved.revision),
            )
            conn.execute(
                "UPDATE bnl_journal_private_metadata SET lifecycle_state='delivery_failed' "
                "WHERE guild_id=1 AND entry_id=? AND revision=?",
                (entry_id, approved.revision),
            )
            conn.execute(
                "INSERT INTO bnl_journal_automation_runs("
                "run_id,guild_id,cadence,source_window_start,source_window_end,lifecycle_state,"
                "reason,journal_entry_id,journal_revision,aggregate_counts_json,attempt_count,"
                "created_at,updated_at) VALUES(?,?,?,?,?,'delivery_failed',?,?,?,?,1,?,?)",
                (
                    run_id,
                    1,
                    "daily",
                    start,
                    end,
                    "retryable_delivery_failure",
                    entry_id,
                    approved.revision,
                    json.dumps(packet["aggregateCounts"]),
                    now,
                    now,
                ),
            )

        generator_calls = []

        def forbidden_generator(_packet, _prompt):
            generator_calls.append(1)
            raise AssertionError("legacy prepared bytes were regenerated")

        prepared = self.prepare_daily(forbidden_generator)
        self.assertEqual("prepared", prepared.status, prepared)
        self.assertEqual("legacy_prepared_revision_adopted", prepared.reason)
        self.assertEqual((entry_id, approved.revision), (prepared.entry_id, prepared.revision))
        self.assertEqual([], generator_calls)

        run = self.run_row()
        manifest_raw = run["frozen_packet_json"]
        manifest = json.loads(manifest_raw)
        self.assertEqual(
            {
                "entryId": entry_id,
                "revision": approved.revision,
                "contentHash": generated.content_hash,
                "canonicalPayloadHash": hashlib.sha256(before).hexdigest(),
                "sourceRefIds": legacy_metadata["sourceRefIds"],
                "sourceSummaries": legacy_metadata["sourceSummaries"],
                "relatedPriorJournalEntryIds": legacy_metadata[
                    "relatedPriorJournalEntryIds"
                ],
            },
            manifest["legacyStagedRevision"],
        )
        self.assertEqual(
            hashlib.sha256(manifest_raw.encode("utf-8")).hexdigest(),
            run["frozen_packet_hash"],
        )
        self.assertEqual(hashlib.sha256(before).hexdigest(), run["prepared_payload_hash"])
        with sqlite3.connect(self.db) as conn:
            metadata = json.loads(
                conn.execute(
                    "SELECT metadata_json FROM bnl_journal_private_metadata "
                    "WHERE guild_id=1 AND entry_id=? AND revision=?",
                    (entry_id, approved.revision),
                ).fetchone()[0]
            )
        self.assertTrue(metadata["legacyPreparedRevisionAdopted"])
        self.assertEqual(run["frozen_packet_hash"], metadata["frozenSourceHash"])
        self.assertEqual(
            "prepared_exact",
            self.entry_row(entry_id, approved.revision)["lifecycle_state"],
        )

        posted = []

        def idempotent_opener(request, timeout=10):
            posted.append(request.data)
            return AcceptedResponse(request, idempotent=True)

        released = self.release_daily(idempotent_opener)
        self.assertEqual("published", released.status, released)
        self.assertEqual(approved.revision, released.revision)
        self.assertEqual([before], posted)
        self.assertEqual([], generator_calls)

    def test_crash_before_run_link_never_falls_back_to_manual_delivery(self):
        start, end, label = automation._daily_period_for_day(TARGET_DAY)
        claim, run_id, epoch, _ = automation._claim_preparation(
            self.db,
            1,
            "daily",
            start,
            end,
            force=True,
        )
        self.assertEqual("claimed", claim)
        packet, source_hash, reason = automation._freeze_or_load_packet(
            self.db,
            1,
            run_id,
            epoch,
            self.packet,
        )
        self.assertEqual("", reason)
        self.assertTrue(source_hash)
        entry_id = automation._entry_id(1, "daily", label)
        generated = journal.generate_and_store_packet_draft(
            self.db,
            1,
            packet,
            lambda current, _prompt: article_json(current),
            entry_id=entry_id,
            attempt_fence=(run_id, epoch),
            source_hash=source_hash,
        )
        self.assertTrue(generated.ok, generated)
        approved = journal.approve_draft(
            self.db,
            1,
            entry_id,
            generated.content_hash,
            revision=generated.revision,
            attempt_fence=(run_id, epoch),
            source_hash=source_hash,
        )
        self.assertTrue(approved.ok, approved)
        run = self.run_row()
        self.assertEqual("preparing", run["lifecycle_state"])
        self.assertIsNone(run["journal_entry_id"])

        posts = []

        def forbidden_opener(request, timeout=10):
            posts.append(request.data)
            raise AssertionError("scheduled crash recovery used raw manual delivery")

        result = automation.release_prepared_entry(
            self.db,
            1,
            entry_id,
            "https://site.example",
            "key",
            force=True,
            opener=forbidden_opener,
        )
        self.assertIsNotNone(result)
        self.assertEqual("held", result.status, result)
        self.assertEqual("prepared_payload_unavailable", result.reason)
        self.assertEqual([], posts)
        self.assertEqual(
            "prepared_exact",
            self.entry_row(entry_id, approved.revision)["lifecycle_state"],
        )

    def test_invalid_legacy_adoption_clears_rejected_revision_everywhere(self):
        packet = self.packet()
        start, end, label = automation._daily_period_for_day(TARGET_DAY)
        entry_id = automation._entry_id(1, "daily", label)
        generated = journal.generate_and_store_packet_draft(
            self.db,
            1,
            packet,
            lambda current, _prompt: article_json(current),
            entry_id=entry_id,
        )
        approved = journal.approve_draft(
            self.db,
            1,
            entry_id,
            generated.content_hash,
            revision=generated.revision,
        )
        self.assertTrue(approved.ok, approved)
        with sqlite3.connect(self.db) as conn:
            conn.execute(
                "UPDATE bnl_journal_entries SET source_window_start=? "
                "WHERE guild_id=1 AND entry_id=? AND revision=?",
                ("2000-01-01T00:00:00Z", entry_id, approved.revision),
            )
            now = journal.utc_now_iso()
            conn.execute(
                "INSERT INTO bnl_journal_observations("
                "observation_id,guild_id,observation_date,source_window_start,"
                "source_window_end,aggregate_counts_json,topic_counts_json,"
                "subject_counts_json,representative_sources_json,lifecycle_state,"
                "journal_entry_id,journal_revision,created_at,updated_at"
                ") VALUES(?,?,?,?,?,'{}','{}','{}','[]','observed',NULL,0,?,?)",
                (
                    "legacy-invalid-observation",
                    1,
                    label,
                    start,
                    end,
                    now,
                    now,
                ),
            )
        claim, run_id, epoch, _row = automation._claim_preparation(
            self.db,
            1,
            "daily",
            start,
            end,
            force=True,
        )
        self.assertEqual("claimed", claim)

        result = automation._adopt_legacy_staged_revision(
            self.db,
            1,
            run_id,
            epoch,
            "daily",
            entry_id,
            start,
            end,
            set(),
        )

        self.assertIsNotNone(result)
        self.assertEqual("held", result.status, result)
        self.assertEqual("legacy_source_window_mismatch", result.reason)
        self.assertEqual("", result.entry_id)
        self.assertEqual(0, result.revision)
        run = self.run_row()
        self.assertEqual("held", run["lifecycle_state"])
        self.assertIsNone(run["journal_entry_id"])
        self.assertEqual(0, run["journal_revision"])
        with sqlite3.connect(self.db) as conn:
            observation = conn.execute(
                "SELECT lifecycle_state,journal_entry_id,journal_revision "
                "FROM bnl_journal_observations WHERE guild_id=1 "
                "AND observation_date=?",
                (label,),
            ).fetchone()
            state = conn.execute(
                "SELECT last_status,last_entry_id,last_revision "
                "FROM bnl_journal_automation_state WHERE guild_id=1",
            ).fetchone()
        self.assertEqual(("held", None, 0), observation)
        self.assertEqual(("held", "", 0), state)
        self.assertEqual(
            "rejected",
            self.entry_row(entry_id, approved.revision)["lifecycle_state"],
        )

    def test_release_result_preserves_http_and_idempotent_telemetry(self):
        prepared = self.prepare_daily()
        self.assertEqual("prepared", prepared.status, prepared)
        posted = []

        def accepted_opener(request, timeout=10):
            posted.append(request.data)
            response = AcceptedResponse(request, idempotent=True)
            response.status = 202
            return response

        released = self.release_daily(accepted_opener)
        self.assertEqual("published", released.status, released)
        self.assertEqual(202, released.http_status)
        self.assertTrue(released.idempotent)
        self.assertEqual(1, len(posted))
        result = automation.result_dict(released)
        self.assertEqual(202, result["http_status"])
        self.assertTrue(result["idempotent"])

    def test_persisted_guard_blocks_legacy_takeover_of_prepared_delivery(self):
        prepared = self.prepare_daily()
        self.assertEqual("prepared", prepared.status, prepared)
        before = self.run_row()
        canonical_before = bytes(
            self.entry_row(prepared.entry_id, prepared.revision)[
                "canonical_payload_bytes"
            ]
        )

        def legacy_takeover():
            with sqlite3.connect(self.db) as conn:
                conn.execute(
                    "UPDATE bnl_journal_automation_runs "
                    "SET lifecycle_state='running',reason='',lease_expires_at=? "
                    "WHERE run_id=?",
                    ("2099-01-01T00:00:00Z", before["run_id"]),
                )

        with self.assertRaisesRegex(
            sqlite3.IntegrityError,
            "journal_prepared_release_downgrade_guard",
        ):
            legacy_takeover()
        self.assertEqual("prepared", self.run_row()["lifecycle_state"])

        with sqlite3.connect(self.db) as conn:
            conn.execute(
                "UPDATE bnl_journal_automation_runs "
                "SET lifecycle_state='delivering',delivery_epoch=delivery_epoch+1,"
                "delivery_lease_expires_at='2099-01-01T00:00:00Z' WHERE run_id=?",
                (before["run_id"],),
            )
        with self.assertRaisesRegex(
            sqlite3.IntegrityError,
            "journal_prepared_release_downgrade_guard",
        ):
            legacy_takeover()
        self.assertEqual("delivering", self.run_row()["lifecycle_state"])

        with sqlite3.connect(self.db) as conn:
            conn.execute(
                "UPDATE bnl_journal_automation_runs "
                "SET lifecycle_state='delivery_failed',delivery_lease_expires_at=NULL "
                "WHERE run_id=?",
                (before["run_id"],),
            )
        with self.assertRaisesRegex(
            sqlite3.IntegrityError,
            "journal_prepared_release_downgrade_guard",
        ):
            legacy_takeover()

        after = self.run_row()
        self.assertEqual("delivery_failed", after["lifecycle_state"])
        self.assertEqual(before["journal_entry_id"], after["journal_entry_id"])
        self.assertEqual(before["journal_revision"], after["journal_revision"])
        self.assertEqual(
            before["prepared_payload_hash"],
            after["prepared_payload_hash"],
        )
        self.assertEqual(
            canonical_before,
            bytes(
                self.entry_row(prepared.entry_id, prepared.revision)[
                    "canonical_payload_bytes"
                ]
            ),
        )

    def test_downgrade_guard_allows_legacy_run_without_prepared_payload(self):
        start, end, _label = automation._daily_period_for_day(
            TARGET_DAY + timedelta(days=1)
        )
        run_id = automation._run_id(2, "daily", start, end)
        now = journal.utc_now_iso()
        with sqlite3.connect(self.db) as conn:
            conn.execute(
                "INSERT INTO bnl_journal_automation_runs("
                "run_id,guild_id,cadence,source_window_start,source_window_end,"
                "lifecycle_state,reason,aggregate_counts_json,attempt_count,"
                "created_at,updated_at) "
                "VALUES(?,?,?,?,?,'delivery_failed','legacy_failure','{}',1,?,?)",
                (run_id, 2, "daily", start, end, now, now),
            )
            conn.execute(
                "UPDATE bnl_journal_automation_runs "
                "SET lifecycle_state='running' WHERE run_id=?",
                (run_id,),
            )
            state = conn.execute(
                "SELECT lifecycle_state FROM bnl_journal_automation_runs "
                "WHERE run_id=?",
                (run_id,),
            ).fetchone()[0]
        self.assertEqual("running", state)

    def test_unfenced_legacy_delivery_cannot_see_scheduled_prepared_bytes(self):
        prepared = self.prepare_daily()
        self.assertEqual("prepared", prepared.status, prepared)
        posts = []

        def forbidden_opener(request, timeout=10):
            posts.append(request.data)
            raise AssertionError("legacy delivery reached scheduled prepared bytes")

        delivered = journal.deliver_approved(
            self.db,
            1,
            prepared.entry_id,
            "https://site.example",
            "key",
            opener=forbidden_opener,
            revision=prepared.revision,
        )

        self.assertFalse(delivered.ok)
        self.assertEqual("not_deliverable", delivered.status)
        self.assertEqual([], posts)
        self.assertEqual("prepared", self.run_row()["lifecycle_state"])
        self.assertEqual(
            "prepared_exact",
            self.entry_row(prepared.entry_id, prepared.revision)[
                "lifecycle_state"
            ],
        )

    def test_downgrade_guard_blocks_held_frozen_occurrence(self):
        prepared = self.prepare_daily()
        self.assertEqual("prepared", prepared.status, prepared)
        before = self.run_row()
        with sqlite3.connect(self.db) as conn:
            conn.execute(
                "UPDATE bnl_journal_automation_runs SET lifecycle_state='held',"
                "reason='temporary_storage_failure' WHERE run_id=?",
                (before["run_id"],),
            )
        with self.assertRaisesRegex(
            sqlite3.IntegrityError,
            "journal_prepared_release_downgrade_guard",
        ):
            with sqlite3.connect(self.db) as conn:
                conn.execute(
                    "UPDATE bnl_journal_automation_runs SET lifecycle_state='running' "
                    "WHERE run_id=?",
                    (before["run_id"],),
                )
        after = self.run_row()
        self.assertEqual("held", after["lifecycle_state"])
        self.assertEqual(before["frozen_packet_json"], after["frozen_packet_json"])
        self.assertEqual(before["frozen_packet_hash"], after["frozen_packet_hash"])

    def test_new_preparation_respects_live_legacy_running_lease(self):
        day = TARGET_DAY + timedelta(days=1)
        start, end, _label = automation._daily_period_for_day(day)
        run_id = automation._run_id(2, "daily", start, end)
        now = journal.utc_now_iso()
        with sqlite3.connect(self.db) as conn:
            conn.execute(
                "INSERT INTO bnl_journal_automation_runs("
                "run_id,guild_id,cadence,source_window_start,source_window_end,"
                "lifecycle_state,reason,aggregate_counts_json,attempt_count,"
                "lease_expires_at,created_at,updated_at) "
                "VALUES(?,?,?,?,?,'running','','{}',1,?,?,?)",
                (run_id, 2, "daily", start, end, "2099-01-01T00:00:00Z", now, now),
            )

        claim, _, epoch, _ = automation._claim_preparation(
            self.db,
            2,
            "daily",
            start,
            end,
            force=True,
        )
        self.assertEqual("busy", claim)
        self.assertEqual(0, epoch)

        with sqlite3.connect(self.db) as conn:
            conn.execute(
                "UPDATE bnl_journal_automation_runs SET lease_expires_at=? "
                "WHERE run_id=?",
                ("2000-01-01T00:00:00Z", run_id),
            )
        claim, _, epoch, _ = automation._claim_preparation(
            self.db,
            2,
            "daily",
            start,
            end,
            force=True,
        )
        self.assertEqual("claimed", claim)
        self.assertEqual(1, epoch)


if __name__ == "__main__":
    unittest.main()
