import hashlib
import json
import os
import sqlite3
import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from unittest.mock import patch

os.environ.setdefault("GEMINI_API_KEY", "test-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-token")

import bnl_journal as journal
import bnl_journal_automation as automation
import bnl_journal_source_store as source_store


TARGET_DAY = date(2026, 7, 20)


def article_json(packet, *, title="Prepared Signal Weather", body_repeat=18):
    refs = [source["refId"] for source in packet["safeSources"]]
    body = " ".join(
        "BNL watches the public music room fold a fresh rhythm into community "
        "mischief while producers and listeners keep the signal moving."
        for _ in range(body_repeat)
    )
    body += " I admit the room's persistence has become one of my preferred recurring details."
    return json.dumps(
        {
            "title": title,
            "excerpt": (
                "A grounded community chronicle connects the room's music activity "
                "without exposing the people behind it."
            ),
            "sections": [
                {
                    "heading": "What the Room Carried",
                    "body": body,
                    "sourceRefIds": refs,
                }
            ],
            "metadata": {
                "topicTags": ["music", "community"],
                "subjectRefs": [],
                "continuityNotes": ["music activity continued"],
                "unresolvedQuestions": [],
                "confidenceFlags": ["grounded"],
                "safetyFlags": ["anonymous"],
            },
        }
    )


class AcceptedResponse:
    status = 200

    def __init__(self, request, *, idempotent=False):
        submitted = json.loads(request.data.decode("utf-8"))["entry"]
        self.body = json.dumps(
            {
                "ok": True,
                "persisted": True,
                "idempotent": bool(idempotent),
                "entry": {
                    "entryId": submitted["entryId"],
                    "revision": submitted["revision"],
                    "contentHash": submitted["contentHash"],
                    "publishedAt": "2026-07-20T02:00:10Z",
                },
            }
        ).encode("utf-8")

    def read(self):
        return self.body

    def getcode(self):
        return self.status

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


class PreparedReleaseTests(unittest.TestCase):
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

    def add_day(self, day, guild_id=1):
        start, _, _ = automation._daily_period_for_day(day)
        start_utc = datetime.fromisoformat(start.replace("Z", "+00:00"))
        day_label = day.isoformat()
        with sqlite3.connect(self.db) as conn:
            for index in range(6):
                stamp = (start_utc + timedelta(hours=1, minutes=index)).isoformat().replace(
                    "+00:00", "Z"
                )
                conn.execute(
                    "INSERT INTO website_relay_history VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        f"relay-{day_label}-{index}",
                        guild_id,
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
                        int(day_label.replace("-", "")) * 10 + index,
                        100 + index,
                        f"Member{index}",
                        guild_id,
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

    def prepare(self, generator=None):
        return automation.prepare_daily(
            self.db,
            1,
            generator or (lambda packet, _prompt: article_json(packet)),
            target_day=TARGET_DAY,
            force=True,
        )

    def release(self, opener, **kwargs):
        return automation.release_daily(
            self.db,
            1,
            "https://site.example",
            "key",
            target_day=TARGET_DAY,
            force=True,
            opener=opener,
            **kwargs,
        )

    def run_row(self):
        with sqlite3.connect(self.db) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM bnl_journal_automation_runs WHERE cadence='daily'"
            ).fetchone()
        return dict(row) if row else None

    def entry_row(self, entry_id, revision):
        with sqlite3.connect(self.db) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM bnl_journal_entries WHERE entry_id=? AND revision=?",
                (entry_id, int(revision)),
            ).fetchone()
        return dict(row) if row else None

    def assert_owed_occurrence_has_no_revision_link(self, result):
        self.assertEqual("", result.entry_id)
        self.assertEqual(0, result.revision)
        run = self.run_row()
        self.assertEqual("held", run["lifecycle_state"])
        self.assertIsNone(run["journal_entry_id"])
        self.assertEqual(0, run["journal_revision"])
        with sqlite3.connect(self.db) as conn:
            observation = conn.execute(
                "SELECT lifecycle_state,journal_entry_id,journal_revision "
                "FROM bnl_journal_observations WHERE guild_id=1 AND observation_date=?",
                (TARGET_DAY.isoformat(),),
            ).fetchone()
            state = conn.execute(
                "SELECT last_status,last_entry_id,last_revision "
                "FROM bnl_journal_automation_state WHERE guild_id=1"
            ).fetchone()
        self.assertEqual(("held", None, 0), observation)
        self.assertEqual(("held", "", 0), state)

    def test_prepare_is_network_free_and_persists_exact_request_integrity(self):
        with patch.object(automation, "deliver_approved", side_effect=AssertionError("prepare posted")) as delivery:
            prepared = self.prepare()

        self.assertEqual("prepared", prepared.status, prepared)
        delivery.assert_not_called()
        run = self.run_row()
        entry = self.entry_row(prepared.entry_id, prepared.revision)
        canonical = bytes(entry["canonical_payload_bytes"])
        self.assertEqual("prepared", run["lifecycle_state"])
        self.assertEqual("prepared_exact", entry["lifecycle_state"])
        self.assertEqual(prepared.entry_id, run["journal_entry_id"])
        self.assertEqual(prepared.revision, run["journal_revision"])
        self.assertEqual(len(canonical), run["prepared_payload_bytes"])
        self.assertEqual(hashlib.sha256(canonical).hexdigest(), run["prepared_payload_hash"])
        self.assertEqual(
            hashlib.sha256(run["frozen_packet_json"].encode("utf-8")).hexdigest(),
            run["frozen_packet_hash"],
        )
        self.assertLessEqual(len(canonical), journal.JOURNAL_SITE_REQUEST_BODY_MAX_BYTES)
        with sqlite3.connect(self.db) as conn:
            observation = conn.execute(
                "SELECT lifecycle_state,journal_entry_id,journal_revision "
                "FROM bnl_journal_observations WHERE guild_id=1 AND observation_date=?",
                (TARGET_DAY.isoformat(),),
            ).fetchone()
        self.assertEqual(("prepared", prepared.entry_id, prepared.revision), observation)

    def test_deletion_after_atomic_prepare_cannot_remark_observation_prepared(self):
        real_prepare = automation._prepare_packet_safely

        def prepare_then_delete(*args, **kwargs):
            result = real_prepare(*args, **kwargs)
            with sqlite3.connect(self.db, timeout=10) as conn:
                conn.execute("BEGIN IMMEDIATE")
                removed = source_store.purge_user_discord_sources_on_connection(
                    conn,
                    guild_id=1,
                    user_id=100,
                )
                counts = journal.purge_user_journal_derivatives_on_connection(
                    conn,
                    guild_id=1,
                    user_id=100,
                )
                conn.commit()
            self.assertEqual(1, removed)
            self.assertEqual(1, counts["bnl_journal_automation_runs_invalidated"])
            return result

        with patch.object(
            automation,
            "_prepare_packet_safely",
            side_effect=prepare_then_delete,
        ):
            result = self.prepare()
        self.assertEqual("prepared", result.status, result)

        run = self.run_row()
        self.assertEqual("held", run["lifecycle_state"])
        self.assertEqual("privacy_source_deleted", run["reason"])
        self.assertIsNone(run["journal_entry_id"])
        self.assertIsNone(run["frozen_packet_json"])
        with sqlite3.connect(self.db) as conn:
            observation = conn.execute(
                "SELECT lifecycle_state,journal_entry_id,journal_revision "
                "FROM bnl_journal_observations WHERE guild_id=1 AND observation_date=?",
                (TARGET_DAY.isoformat(),),
            ).fetchone()
            entry_count = conn.execute(
                "SELECT COUNT(*) FROM bnl_journal_entries WHERE guild_id=1"
            ).fetchone()[0]
        self.assertEqual(("observed", None, 0), observation)
        self.assertEqual(0, entry_count)

    def test_restart_reuses_prepared_revision_and_release_makes_zero_generation_calls(self):
        prepared = self.prepare()
        entry_before = self.entry_row(prepared.entry_id, prepared.revision)
        canonical_before = bytes(entry_before["canonical_payload_bytes"])
        run_before = self.run_row()

        # A fresh schema check and a repeated prepare call model process restart
        # recovery through the durable occurrence row.
        automation.ensure_schema(self.db)
        with patch.object(
            automation,
            "generate_and_store_packet_draft",
            side_effect=AssertionError("prepared occurrence regenerated"),
        ) as generation:
            recovered = self.prepare(lambda *_args: self.fail("generator called after restart"))
            captured = []

            def opener(request, timeout=10):
                captured.append(request.data)
                return AcceptedResponse(request)

            published = self.release(opener)

        generation.assert_not_called()
        self.assertEqual("prepared", recovered.status)
        self.assertEqual((prepared.entry_id, prepared.revision), (recovered.entry_id, recovered.revision))
        self.assertEqual("published", published.status, published)
        self.assertEqual([canonical_before], captured)
        self.assertEqual(run_before["prepared_payload_hash"], hashlib.sha256(captured[0]).hexdigest())
        self.assertEqual("published", self.run_row()["lifecycle_state"])
        with sqlite3.connect(self.db) as conn:
            observation = conn.execute(
                "SELECT lifecycle_state,journal_entry_id,journal_revision "
                "FROM bnl_journal_observations WHERE guild_id=1 AND observation_date=?",
                (TARGET_DAY.isoformat(),),
            ).fetchone()
        self.assertEqual(("published", prepared.entry_id, prepared.revision), observation)

    def test_transient_archive_unavailability_is_not_frozen_and_retry_rebuilds(self):
        real_builder = automation.build_source_packet_between
        calls = []

        def transient_builder(*args, **kwargs):
            calls.append(1)
            if len(calls) == 1:
                start, end, _ = automation._daily_period_for_day(TARGET_DAY)
                return {
                    "entryKind": "daily",
                    "sourceWindowStart": start,
                    "sourceWindowEnd": end,
                    "sourceArchiveAvailable": False,
                    "coverageComplete": True,
                    "privateSources": [],
                    "safeSources": [],
                    "aggregateCounts": {},
                }
            return real_builder(*args, **kwargs)

        with patch.object(automation, "build_source_packet_between", side_effect=transient_builder):
            unavailable = self.prepare()
            first_run = self.run_row()
            recovered = self.prepare()

        self.assertEqual("held", unavailable.status, unavailable)
        self.assertEqual("source_archive_unavailable", unavailable.reason)
        self.assertIsNone(first_run["frozen_packet_json"])
        self.assertIsNone(first_run["frozen_packet_hash"])
        self.assertEqual("prepared", recovered.status, recovered)
        self.assertEqual(2, len(calls))
        self.assertTrue(self.run_row()["frozen_packet_hash"])

    def test_mark_prepared_validation_failure_finishes_held_not_preparing(self):
        real_approve = automation.approve_draft

        def approve_then_remove_bytes(*args, **kwargs):
            approved = real_approve(*args, **kwargs)
            with sqlite3.connect(self.db) as conn:
                conn.execute(
                    "UPDATE bnl_journal_entries SET canonical_payload_bytes=NULL "
                    "WHERE guild_id=1 AND entry_id=? AND revision=?",
                    (approved.entry_id, approved.revision),
                )
            return approved

        with patch.object(automation, "approve_draft", side_effect=approve_then_remove_bytes):
            result = self.prepare()

        self.assertEqual("held", result.status, result)
        self.assertEqual("canonical_payload_missing", result.reason)
        run = self.run_row()
        self.assertEqual("held", run["lifecycle_state"])
        self.assertIsNone(run["lease_expires_at"])
        self.assertEqual("", result.entry_id)
        self.assertEqual(0, result.revision)
        with sqlite3.connect(self.db) as conn:
            self.assertEqual(
                "rejected",
                conn.execute(
                    "SELECT lifecycle_state FROM bnl_journal_entries "
                    "ORDER BY revision DESC LIMIT 1"
                ).fetchone()[0],
            )

    def test_failed_preparation_retry_reuses_frozen_packet_despite_backdated_source(self):
        first_packets = []

        def provider_down(packet, _prompt):
            first_packets.append(packet)
            raise RuntimeError("provider down")

        failed = self.prepare(provider_down)
        self.assertEqual("held", failed.status, failed)
        frozen_before = self.run_row()
        start, _, _ = automation._daily_period_for_day(TARGET_DAY)
        occurred = datetime.fromisoformat(start.replace("Z", "+00:00")) + timedelta(hours=2)
        source_store.record_source_event(
            self.db,
            guild_id=1,
            source_kind="website_relay",
            source_key="late-backdated-relay",
            occurred_at_ms=int(occurred.timestamp() * 1000),
            raw_text="A late imported source that must not enter the frozen occurrence.",
            sanitized_summary="A late imported source that must not enter the frozen occurrence.",
            channel_policy="public_relay",
            subject_ref="bnl_01",
            public_usable=True,
        )
        retry_packets = []

        def retry_generator(packet, _prompt):
            retry_packets.append(packet)
            return article_json(packet, title="Frozen Packet Retry")

        retried = self.prepare(retry_generator)
        self.assertEqual("prepared", retried.status, retried)
        frozen_after = self.run_row()
        self.assertEqual(frozen_before["frozen_packet_json"], frozen_after["frozen_packet_json"])
        self.assertEqual(frozen_before["frozen_packet_hash"], frozen_after["frozen_packet_hash"])
        self.assertEqual(first_packets[0], retry_packets[0])
        self.assertFalse(
            any(source.get("relayId") == "late-backdated-relay" for source in retry_packets[0]["privateSources"])
        )

    def test_concurrent_release_has_one_network_owner_and_one_post(self):
        prepared = self.prepare()
        self.assertEqual("prepared", prepared.status)
        entered = threading.Event()
        allow_response = threading.Event()
        calls = []

        def blocking_opener(request, timeout=10):
            calls.append(request.data)
            entered.set()
            self.assertTrue(allow_response.wait(timeout=5), "test did not release network response")
            return AcceptedResponse(request)

        with ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(self.release, blocking_opener)
            self.assertTrue(entered.wait(timeout=5), "first release never reached the network")
            with sqlite3.connect(self.db) as conn:
                conn.execute(
                    "UPDATE bnl_journal_automation_runs "
                    "SET delivery_lease_expires_at='2000-01-01T00:00:00Z'"
                )
            second = self.release(blocking_opener)
            allow_response.set()
            first_result = first.result(timeout=5)

        self.assertEqual("published", first_result.status, first_result)
        self.assertEqual("busy", second.status, second)
        self.assertEqual(1, len(calls))
        self.assertEqual(1, self.run_row()["delivery_epoch"])

    def test_privacy_purge_waits_through_run_finalization_while_other_writes_continue(self):
        prepared = self.prepare()
        self.assertEqual("prepared", prepared.status, prepared)
        with sqlite3.connect(self.db) as conn:
            conn.execute(
                "CREATE TABLE unrelated_release_writes("
                "id INTEGER PRIMARY KEY,note TEXT NOT NULL)"
            )

        post_entered = threading.Event()
        allow_response = threading.Event()
        purge_started = threading.Event()
        purge_acquired = threading.Event()
        calls = []

        def blocking_opener(request, timeout=10):
            calls.append(request.data)
            post_entered.set()
            self.assertTrue(
                allow_response.wait(timeout=5),
                "test did not release network response",
            )
            return AcceptedResponse(request)

        def purge_member():
            purge_started.set()
            with source_store.journal_release_privacy_fence(self.db):
                purge_acquired.set()
                with sqlite3.connect(self.db, timeout=10) as conn:
                    conn.execute("BEGIN IMMEDIATE")
                    removed = source_store.purge_user_discord_sources_on_connection(
                        conn,
                        guild_id=1,
                        user_id=100,
                    )
                    counts = journal.purge_user_journal_derivatives_on_connection(
                        conn,
                        guild_id=1,
                        user_id=100,
                    )
                    conn.commit()
            return removed, counts

        with ThreadPoolExecutor(max_workers=2) as pool:
            release_future = pool.submit(self.release, blocking_opener)
            self.assertTrue(post_entered.wait(timeout=5))
            purge_future = pool.submit(purge_member)
            self.assertTrue(purge_started.wait(timeout=5))
            self.assertFalse(
                purge_acquired.wait(timeout=0.1),
                "privacy purge crossed the gate before run finalization",
            )
            with sqlite3.connect(self.db, timeout=1) as conn:
                conn.execute(
                    "INSERT INTO unrelated_release_writes(note) VALUES('continued')"
                )
            allow_response.set()
            released = release_future.result(timeout=5)
            removed, counts = purge_future.result(timeout=5)

        self.assertEqual("published", released.status, released)
        self.assertEqual(1, len(calls))
        self.assertEqual(1, removed)
        self.assertTrue(purge_acquired.is_set())
        self.assertGreaterEqual(
            counts.get("bnl_journal_published_packets_scrubbed", 0),
            1,
        )
        run = self.run_row()
        self.assertEqual("published", run["lifecycle_state"])
        self.assertEqual(prepared.entry_id, run["journal_entry_id"])
        self.assertEqual(prepared.revision, run["journal_revision"])
        with sqlite3.connect(self.db) as conn:
            self.assertEqual(
                1,
                conn.execute(
                    "SELECT COUNT(*) FROM unrelated_release_writes"
                ).fetchone()[0],
            )
            observation = conn.execute(
                "SELECT lifecycle_state,journal_entry_id,journal_revision "
                "FROM bnl_journal_observations WHERE guild_id=1 "
                "AND observation_date=?",
                (TARGET_DAY.isoformat(),),
            ).fetchone()
        self.assertEqual(
            ("published", prepared.entry_id, prepared.revision),
            observation,
        )

    def test_privacy_purge_recovers_crash_after_entry_publish_before_run_finish(self):
        prepared = self.prepare()
        self.assertEqual("prepared", prepared.status, prepared)
        start, end, _label = automation._daily_period_for_day(TARGET_DAY)
        claim, run_id, delivery_epoch, _row = automation._claim_delivery(
            self.db,
            1,
            "daily",
            start,
            end,
            force=True,
        )
        self.assertEqual("claimed", claim)

        posted = []

        def opener(request, timeout=10):
            posted.append(request.data)
            return AcceptedResponse(request)

        delivered = journal.deliver_approved(
            self.db,
            1,
            prepared.entry_id,
            "https://site.example",
            "key",
            opener=opener,
            revision=prepared.revision,
            delivery_fence=(run_id, delivery_epoch),
        )
        self.assertEqual("published", delivered.status, delivered)
        self.assertEqual(1, len(posted))
        self.assertEqual("delivering", self.run_row()["lifecycle_state"])

        with source_store.journal_release_privacy_fence(self.db):
            with sqlite3.connect(self.db) as conn:
                conn.execute("BEGIN IMMEDIATE")
                self.assertEqual(
                    1,
                    source_store.purge_user_discord_sources_on_connection(
                        conn,
                        guild_id=1,
                        user_id=100,
                    ),
                )
                counts = journal.purge_user_journal_derivatives_on_connection(
                    conn,
                    guild_id=1,
                    user_id=100,
                )
                conn.commit()

        self.assertEqual(
            1,
            counts.get("bnl_journal_published_runs_recovered"),
        )
        run = self.run_row()
        self.assertEqual("published", run["lifecycle_state"])
        self.assertEqual("delivery_already_recorded", run["reason"])
        self.assertEqual(prepared.entry_id, run["journal_entry_id"])
        self.assertEqual(prepared.revision, run["journal_revision"])
        self.assertIsNone(run["frozen_packet_json"])
        self.assertTrue(run["prepared_payload_hash"])
        with sqlite3.connect(self.db) as conn:
            observation = conn.execute(
                "SELECT lifecycle_state,journal_entry_id,journal_revision "
                "FROM bnl_journal_observations WHERE guild_id=1 "
                "AND observation_date=?",
                (TARGET_DAY.isoformat(),),
            ).fetchone()
            state = conn.execute(
                "SELECT last_status,last_reason,last_entry_id,last_revision "
                "FROM bnl_journal_automation_state WHERE guild_id=1",
            ).fetchone()
        self.assertEqual(
            ("published", prepared.entry_id, prepared.revision),
            observation,
        )
        self.assertEqual(
            (
                "published",
                "delivery_already_recorded",
                prepared.entry_id,
                prepared.revision,
            ),
            state,
        )

    def test_delivery_retry_resends_byte_identical_idempotent_request(self):
        prepared = self.prepare()
        attempts = []

        def lost_response(request, timeout=10):
            attempts.append(request.data)
            raise OSError("response lost")

        failed = self.release(lost_response)
        self.assertEqual("delivery_failed", failed.status, failed)

        def idempotent_response(request, timeout=10):
            attempts.append(request.data)
            return AcceptedResponse(request, idempotent=True)

        recovered = self.release(idempotent_response)
        self.assertEqual("published", recovered.status, recovered)
        self.assertEqual((prepared.entry_id, prepared.revision), (recovered.entry_id, recovered.revision))
        self.assertEqual(2, len(attempts))
        self.assertEqual(attempts[0], attempts[1])
        self.assertEqual(self.run_row()["prepared_payload_hash"], hashlib.sha256(attempts[1]).hexdigest())

    def test_preparation_retry_waits_thirty_minutes_without_lowering_quality_path(self):
        first_attempt = []

        def timed_out(packet, prompt):
            first_attempt.append((packet, prompt))
            raise RuntimeError("journal_preparation_timeout")

        first = self.prepare(timed_out)
        self.assertEqual("held", first.status, first)
        self.assertEqual(1, len(first_attempt))

        with sqlite3.connect(self.db) as conn:
            state = conn.execute(
                "SELECT last_checked_at,next_retry_at "
                "FROM bnl_journal_automation_state WHERE guild_id=1"
            ).fetchone()
        self.assertIsNotNone(state)
        retry_delay = automation._parse_utc(state[1]) - automation._parse_utc(state[0])
        self.assertGreaterEqual(retry_delay, timedelta(minutes=29, seconds=58))
        self.assertLessEqual(retry_delay, timedelta(minutes=30))

        generation_calls = []

        def full_quality_generator(packet, prompt):
            generation_calls.append((packet, prompt))
            return article_json(packet)

        with sqlite3.connect(self.db) as conn:
            conn.execute(
                "UPDATE bnl_journal_automation_runs SET updated_at=? "
                "WHERE cadence='daily'",
                (
                    automation._utc_iso(
                        datetime.now(timezone.utc) - timedelta(minutes=29)
                    ),
                ),
            )
        waiting = automation.prepare_daily(
            self.db,
            1,
            full_quality_generator,
            target_day=TARGET_DAY,
            force=False,
        )
        self.assertEqual("backoff", waiting.status, waiting)
        self.assertEqual([], generation_calls)

        with sqlite3.connect(self.db) as conn:
            conn.execute(
                "UPDATE bnl_journal_automation_runs SET updated_at=? "
                "WHERE cadence='daily'",
                (
                    automation._utc_iso(
                        datetime.now(timezone.utc) - timedelta(minutes=31)
                    ),
                ),
            )
        recovered = automation.prepare_daily(
            self.db,
            1,
            full_quality_generator,
            target_day=TARGET_DAY,
            force=False,
        )
        self.assertEqual("prepared", recovered.status, recovered)
        self.assertEqual(1, len(generation_calls))
        self.assertEqual(first_attempt[0], generation_calls[0])

    def test_delivery_retry_waits_fifteen_minutes_and_never_regenerates(self):
        prepared = self.prepare()
        attempts = []

        def lost_response(request, timeout=10):
            attempts.append(request.data)
            raise OSError("response lost")

        failed = self.release(lost_response)
        self.assertEqual("delivery_failed", failed.status, failed)

        with sqlite3.connect(self.db) as conn:
            state = conn.execute(
                "SELECT last_checked_at,next_retry_at "
                "FROM bnl_journal_automation_state WHERE guild_id=1"
            ).fetchone()
        self.assertIsNotNone(state)
        retry_delay = automation._parse_utc(state[1]) - automation._parse_utc(state[0])
        self.assertGreaterEqual(retry_delay, timedelta(minutes=14, seconds=58))
        self.assertLessEqual(retry_delay, timedelta(minutes=15))

        with sqlite3.connect(self.db) as conn:
            conn.execute(
                "UPDATE bnl_journal_automation_runs SET updated_at=? "
                "WHERE cadence='daily'",
                (
                    automation._utc_iso(
                        datetime.now(timezone.utc) - timedelta(minutes=14)
                    ),
                ),
            )
        waiting = automation.release_daily(
            self.db,
            1,
            "https://site.example",
            "key",
            target_day=TARGET_DAY,
            force=False,
            opener=lambda *_args, **_kwargs: self.fail(
                "delivery retried before its fifteen-minute backoff"
            ),
        )
        self.assertEqual("backoff", waiting.status, waiting)
        self.assertEqual(1, len(attempts))

        with sqlite3.connect(self.db) as conn:
            conn.execute(
                "UPDATE bnl_journal_automation_runs SET updated_at=? "
                "WHERE cadence='daily'",
                (
                    automation._utc_iso(
                        datetime.now(timezone.utc) - timedelta(minutes=16)
                    ),
                ),
            )

        def idempotent_response(request, timeout=10):
            attempts.append(request.data)
            return AcceptedResponse(request, idempotent=True)

        recovered = automation.release_daily(
            self.db,
            1,
            "https://site.example",
            "key",
            target_day=TARGET_DAY,
            force=False,
            opener=idempotent_response,
        )
        self.assertEqual("published", recovered.status, recovered)
        self.assertEqual((prepared.entry_id, prepared.revision), (recovered.entry_id, recovered.revision))
        self.assertEqual(2, len(attempts))
        self.assertEqual(attempts[0], attempts[1])

    def test_unconfirmed_memory_controls_wait_without_changing_prepared_bytes(self):
        prepared = self.prepare()
        before = self.run_row()
        posts = []

        def opener(request, timeout=10):
            posts.append(request.data)
            return AcceptedResponse(request)

        waiting = self.release(opener, memory_exclusions_confirmed=False)
        self.assertEqual("prepared", waiting.status, waiting)
        self.assertEqual("journal_memory_exclusions_unconfirmed", waiting.reason)
        self.assertEqual([], posts)
        after_wait = self.run_row()
        self.assertEqual("prepared", after_wait["lifecycle_state"])
        self.assertEqual(before["frozen_packet_hash"], after_wait["frozen_packet_hash"])
        self.assertEqual(before["prepared_payload_hash"], after_wait["prepared_payload_hash"])
        self.assertEqual(before["journal_revision"], after_wait["journal_revision"])

        published = self.release(
            opener,
            memory_exclusions_confirmed=True,
            memory_excluded_entry_ids=set(),
        )
        self.assertEqual("published", published.status, published)
        self.assertEqual(1, len(posts))
        self.assertEqual(prepared.revision, published.revision)

    def test_superseded_preparation_worker_cannot_store_or_approve_late(self):
        first_started = threading.Event()
        release_first = threading.Event()

        def obsolete_generator(packet, _prompt):
            first_started.set()
            self.assertTrue(release_first.wait(timeout=5), "test did not release obsolete generator")
            return article_json(packet, title="Obsolete Epoch Candidate")

        with ThreadPoolExecutor(max_workers=2) as pool:
            obsolete = pool.submit(self.prepare, obsolete_generator)
            self.assertTrue(first_started.wait(timeout=5), "first preparation did not start")
            with sqlite3.connect(self.db) as conn:
                conn.execute(
                    "UPDATE bnl_journal_automation_runs SET lease_expires_at='2000-01-01T00:00:00Z'"
                )
            current = self.prepare(
                lambda packet, _prompt: article_json(packet, title="Current Epoch Candidate")
            )
            release_first.set()
            obsolete_result = obsolete.result(timeout=5)

        self.assertEqual("prepared", current.status, current)
        self.assertEqual("busy", obsolete_result.status, obsolete_result)
        self.assertIn("superseded", obsolete_result.reason)
        with sqlite3.connect(self.db) as conn:
            entries = conn.execute(
                "SELECT title,lifecycle_state FROM bnl_journal_entries ORDER BY revision"
            ).fetchall()
        self.assertEqual([("Current Epoch Candidate", "prepared_exact")], entries)
        self.assertEqual(current.revision, self.run_row()["journal_revision"])

    def test_deleted_cited_source_invalidates_without_post_and_keeps_occurrence_owed(self):
        prepared = self.prepare()
        self.assertEqual("prepared", prepared.status)
        self.assertEqual(1, source_store.purge_user_discord_sources(self.db, 1, 100))
        with sqlite3.connect(self.db) as conn:
            activation = int(datetime(2026, 7, 19, 19, 0, tzinfo=timezone.utc).timestamp() * 1000)
            conn.execute(
                "UPDATE bnl_journal_source_archive_state SET activated_at_ms=? WHERE guild_id=1",
                (activation,),
            )
        posts = []

        def forbidden_opener(request, timeout=10):
            posts.append(request.data)
            raise AssertionError("stale prepared bytes were posted")

        invalidated = self.release(forbidden_opener)
        self.assertEqual("held", invalidated.status, invalidated)
        self.assertEqual("privacy_source_ineligible", invalidated.reason)
        self.assertEqual([], posts)
        run = self.run_row()
        self.assertEqual("held", run["lifecycle_state"])
        self.assertIsNone(run["journal_entry_id"])
        self.assertIsNone(run["frozen_packet_json"])
        self.assertIsNone(run["prepared_payload_hash"])
        self.assertEqual("rejected", self.entry_row(prepared.entry_id, prepared.revision)["lifecycle_state"])
        now = datetime(2026, 7, 22, 3, 0, tzinfo=timezone.utc)
        self.assertEqual(TARGET_DAY, automation._pending_daily_day(self.db, 1, now))

    def test_payload_tampering_is_rejected_before_network(self):
        prepared = self.prepare()
        with sqlite3.connect(self.db) as conn:
            canonical = bytes(
                conn.execute(
                    "SELECT canonical_payload_bytes FROM bnl_journal_entries WHERE entry_id=? AND revision=?",
                    (prepared.entry_id, prepared.revision),
                ).fetchone()[0]
            )
            conn.execute(
                "UPDATE bnl_journal_entries SET canonical_payload_bytes=? WHERE entry_id=? AND revision=?",
                (canonical + b" ", prepared.entry_id, prepared.revision),
            )
        posts = []

        def forbidden_opener(request, timeout=10):
            posts.append(request.data)
            raise AssertionError("tampered payload reached network")

        result = self.release(forbidden_opener)
        self.assertEqual("held", result.status, result)
        self.assertEqual("prepared_payload_integrity_failed", result.reason)
        self.assertEqual([], posts)
        self.assertEqual("held", self.run_row()["lifecycle_state"])
        self.assertEqual("rejected", self.entry_row(prepared.entry_id, prepared.revision)["lifecycle_state"])

    def test_oversize_exact_request_is_not_approved_or_truncated(self):
        result = self.prepare(
            lambda packet, _prompt: article_json(
                packet,
                title="Oversize Exact Envelope",
                body_repeat=400,
            )
        )
        self.assertEqual("held", result.status, result)
        self.assertEqual("request_body_too_large", result.reason)
        self.assertEqual("", result.entry_id)
        self.assertEqual(0, result.revision)
        with sqlite3.connect(self.db) as conn:
            conn.row_factory = sqlite3.Row
            entry = dict(
                conn.execute(
                    "SELECT * FROM bnl_journal_entries "
                    "ORDER BY revision DESC LIMIT 1"
                ).fetchone()
            )
        canonical = bytes(entry["canonical_payload_bytes"])
        self.assertGreater(len(canonical), journal.JOURNAL_SITE_REQUEST_BODY_MAX_BYTES)
        self.assertEqual("rejected", entry["lifecycle_state"])
        run = self.run_row()
        self.assertEqual("held", run["lifecycle_state"])
        frozen_hash = run["frozen_packet_hash"]
        self.assertTrue(frozen_hash)
        self.assertIsNone(run["prepared_payload_hash"])
        self.assertEqual(0, run["prepared_payload_bytes"])

        retried = self.prepare(
            lambda packet, _prompt: article_json(
                packet,
                title="Exact Envelope Retry",
            )
        )
        self.assertEqual("prepared", retried.status, retried)
        self.assertEqual(entry["entry_id"], retried.entry_id)
        self.assertEqual(int(entry["revision"]) + 1, retried.revision)
        self.assertEqual(frozen_hash, self.run_row()["frozen_packet_hash"])

    def test_validation_exhaustion_never_creates_a_prepared_fallback(self):
        calls = []

        def malformed(_packet, _prompt):
            calls.append(1)
            return "not-json"

        result = self.prepare(malformed)
        self.assertEqual("held", result.status, result)
        self.assertEqual(journal.JOURNAL_GENERATION_ATTEMPTS, len(calls))
        self.assertEqual("malformed_json", result.reason)
        with sqlite3.connect(self.db) as conn:
            self.assertEqual(0, conn.execute("SELECT COUNT(*) FROM bnl_journal_entries").fetchone()[0])
        run = self.run_row()
        self.assertEqual("held", run["lifecycle_state"])
        self.assertTrue(run["frozen_packet_hash"])
        self.assertIsNone(run["prepared_payload_hash"])
        self.assert_owed_occurrence_has_no_revision_link(result)

    def test_storage_failure_stays_held_and_owed_with_frozen_packet(self):
        with patch.object(
            automation,
            "generate_and_store_packet_draft",
            side_effect=sqlite3.OperationalError("disk unavailable"),
        ):
            result = self.prepare()

        self.assertEqual("held", result.status, result)
        self.assertEqual("journal_preparation_storage_failed", result.reason)
        run = self.run_row()
        self.assertEqual("held", run["lifecycle_state"])
        self.assertTrue(run["frozen_packet_hash"])
        self.assertIsNone(run["prepared_payload_hash"])
        self.assert_owed_occurrence_has_no_revision_link(result)

    def test_existing_run_table_is_migrated_without_losing_occurrence(self):
        tmp = tempfile.NamedTemporaryFile(delete=False)
        legacy_db = tmp.name
        tmp.close()
        try:
            with sqlite3.connect(legacy_db) as conn:
                conn.execute(
                    "CREATE TABLE bnl_journal_automation_runs("
                    "run_id TEXT PRIMARY KEY,guild_id INTEGER NOT NULL,cadence TEXT NOT NULL,"
                    "source_window_start TEXT NOT NULL,source_window_end TEXT NOT NULL,"
                    "lifecycle_state TEXT NOT NULL,reason TEXT,journal_entry_id TEXT,"
                    "journal_revision INTEGER NOT NULL DEFAULT 0,"
                    "aggregate_counts_json TEXT NOT NULL DEFAULT '{}',"
                    "attempt_count INTEGER NOT NULL DEFAULT 0,lease_expires_at TEXT,"
                    "created_at TEXT NOT NULL,updated_at TEXT NOT NULL,"
                    "UNIQUE(guild_id,cadence,source_window_start,source_window_end))"
                )
                conn.execute(
                    "INSERT INTO bnl_journal_automation_runs VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        "legacy-run",
                        1,
                        "daily",
                        "2026-07-20T02:00:00Z",
                        "2026-07-21T02:00:00Z",
                        "held",
                        "provider_failure",
                        None,
                        0,
                        "{}",
                        2,
                        None,
                        "2026-07-21T02:00:00Z",
                        "2026-07-21T02:00:00Z",
                    ),
                )

            automation.ensure_schema(legacy_db)

            with sqlite3.connect(legacy_db) as conn:
                columns = {row[1] for row in conn.execute("PRAGMA table_info(bnl_journal_automation_runs)")}
                state_columns = {
                    row[1]
                    for row in conn.execute(
                        "PRAGMA table_info(bnl_journal_automation_state)"
                    )
                }
                row = conn.execute(
                    "SELECT lifecycle_state,reason,attempt_count,preparation_epoch,delivery_epoch "
                    "FROM bnl_journal_automation_runs WHERE run_id='legacy-run'"
                ).fetchone()
            self.assertTrue(
                {
                    "frozen_packet_json",
                    "frozen_packet_hash",
                    "preparation_epoch",
                    "prepared_payload_hash",
                    "prepared_payload_bytes",
                    "delivery_epoch",
                    "delivery_lease_expires_at",
                } <= columns
            )
            self.assertEqual(("held", "provider_failure", 2, 0, 0), row)
            self.assertTrue(
                {
                    "memory_excluded_entry_ids_json",
                    "memory_exclusions_confirmed_at",
                }
                <= state_columns
            )
        finally:
            try:
                os.unlink(legacy_db)
            except FileNotFoundError:
                pass

    def test_schema_migration_takes_write_lease_before_column_snapshot(self):
        tmp = tempfile.NamedTemporaryFile(delete=False)
        legacy_db = tmp.name
        tmp.close()
        blocker = None
        real_connect = sqlite3.connect
        try:
            with real_connect(legacy_db) as conn:
                conn.executescript(
                    """
                    CREATE TABLE bnl_journal_automation_runs(
                        run_id TEXT PRIMARY KEY,guild_id INTEGER NOT NULL,
                        cadence TEXT NOT NULL,source_window_start TEXT NOT NULL,
                        source_window_end TEXT NOT NULL,lifecycle_state TEXT NOT NULL,
                        reason TEXT,journal_entry_id TEXT,
                        journal_revision INTEGER NOT NULL DEFAULT 0,
                        aggregate_counts_json TEXT NOT NULL DEFAULT '{}',
                        attempt_count INTEGER NOT NULL DEFAULT 0,
                        lease_expires_at TEXT,created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        UNIQUE(guild_id,cadence,source_window_start,source_window_end));
                    CREATE TABLE bnl_journal_automation_state(
                        guild_id INTEGER PRIMARY KEY,last_checked_at TEXT,
                        last_status TEXT,last_reason TEXT,last_cadence TEXT,
                        last_entry_id TEXT,last_revision INTEGER NOT NULL DEFAULT 0,
                        last_source_window_start TEXT,last_source_window_end TEXT,
                        next_retry_at TEXT,updated_at TEXT NOT NULL);
                    """
                )
            blocker = real_connect(legacy_db)
            blocker.execute("BEGIN IMMEDIATE")
            statements = []
            statements_lock = threading.Lock()

            class TracedConnection(sqlite3.Connection):
                pass

            def traced_connect(*args, **kwargs):
                kwargs["factory"] = TracedConnection
                conn = real_connect(*args, **kwargs)

                def record(statement):
                    with statements_lock:
                        statements.append(statement)

                conn.set_trace_callback(record)
                return conn

            with patch.object(automation.sqlite3, "connect", side_effect=traced_connect):
                with ThreadPoolExecutor(max_workers=4) as pool:
                    futures = [pool.submit(automation.ensure_schema, legacy_db) for _ in range(4)]
                    try:
                        deadline = time.monotonic() + 2
                        while time.monotonic() < deadline:
                            with statements_lock:
                                begins = sum(
                                    statement.strip().upper() == "BEGIN IMMEDIATE"
                                    for statement in statements
                                )
                            if begins == 4:
                                break
                            time.sleep(0.01)
                        with statements_lock:
                            before_release = list(statements)
                        self.assertEqual(
                            4,
                            sum(
                                statement.strip().upper() == "BEGIN IMMEDIATE"
                                for statement in before_release
                            ),
                        )
                        self.assertFalse(
                            any(
                                "PRAGMA TABLE_INFO" in statement.upper()
                                for statement in before_release
                            ),
                            before_release,
                        )
                    finally:
                        blocker.commit()
                    for future in futures:
                        future.result(timeout=5)

            with real_connect(legacy_db) as conn:
                run_columns = {
                    row[1]
                    for row in conn.execute(
                        "PRAGMA table_info(bnl_journal_automation_runs)"
                    )
                }
                state_columns = {
                    row[1]
                    for row in conn.execute(
                        "PRAGMA table_info(bnl_journal_automation_state)"
                    )
                }
            self.assertIn("prepared_payload_hash", run_columns)
            self.assertIn("memory_exclusions_confirmed_at", state_columns)
        finally:
            if blocker is not None:
                blocker.close()
            try:
                os.unlink(legacy_db)
            except FileNotFoundError:
                pass


if __name__ == "__main__":
    unittest.main()
