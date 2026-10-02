import gc
import json
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from bnl_journal_source_store import record_tiktok_engagement_event


def metric(event_id="tap-1", **changes):
    value = {
        "event_type": "like", "event_id": event_id, "room_id": "test-room",
        "observed_at": 1_800_000_010.0, "source_at": 1_800_000_000.0,
        "like_count": 23, "like_total": 700,
        "unique_id": "test.viewer", "display_name": "Test Viewer",
    }
    value.update(changes)
    return value


class TikTokEngagementArchiveTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.addCleanup(gc.collect)
        self.db = str(Path(self.directory.name) / "bnl.db")

    def test_measurements_land_anonymously_in_the_original_source_archive(self):
        result = record_tiktok_engagement_event(self.db, guild_id=77, record=metric())
        self.assertTrue(result.ok)
        with sqlite3.connect(self.db) as conn:
            row = conn.execute(
                "SELECT source_kind,occurred_at_ms,raw_text,metadata_json,"
                "subject_ref,private_display_name,channel_policy,public_usable "
                "FROM bnl_journal_source_events"
            ).fetchone()
            tables = {item[0] for item in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        self.assertEqual(row[0], "tiktok_live_engagement")
        self.assertEqual(row[1], 1_800_000_000_000)
        record = json.loads(row[2])
        self.assertEqual(record["like_count"], 23)
        self.assertEqual(record["like_total"], 700)
        self.assertEqual(json.loads(row[3])["engagement"], record)
        self.assertEqual(row[4:], ("", "", "public_context", 1))
        self.assertNotIn("test.viewer", row[2] + row[3])
        self.assertNotIn("Test Viewer", row[2] + row[3])
        self.assertNotIn("memory_ledger_entries", tables)
        self.assertNotIn("moments", tables)

    def test_reconnection_replay_preserves_first_receipt_without_counting_twice(self):
        first = record_tiktok_engagement_event(self.db, guild_id=77, record=metric())
        replay = record_tiktok_engagement_event(
            self.db, guild_id=77, record=metric(observed_at=1_800_000_030.0),
        )
        self.assertEqual(replay.status, "idempotent")
        self.assertEqual((first.event_seq, first.content_hash), (replay.event_seq, replay.content_hash))
        with sqlite3.connect(self.db) as conn:
            rows = conn.execute("SELECT raw_text FROM bnl_journal_source_events").fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(json.loads(rows[0][0])["observed_at"], 1_800_000_010.0)

    def test_receipt_only_events_keep_the_original_interval_on_restart_replay(self):
        first = record_tiktok_engagement_event(self.db, guild_id=77, record=metric(source_at=None))
        replay = record_tiktok_engagement_event(
            self.db, guild_id=77, record=metric(source_at=None, observed_at=1_800_000_100.0),
        )
        self.assertTrue(first.ok and replay.ok)
        with sqlite3.connect(self.db) as conn:
            self.assertEqual(conn.execute("SELECT occurred_at_ms FROM bnl_journal_source_events").fetchone()[0],
                             1_800_000_010_000)

    def test_changed_measurements_or_source_time_conflict_instead_of_overwriting(self):
        first = record_tiktok_engagement_event(self.db, guild_id=77, record=metric())
        for changed in (metric(like_count=99), metric(like_total=900), metric(source_at=1_800_000_001.0)):
            with self.subTest(changed=changed):
                result = record_tiktok_engagement_event(self.db, guild_id=77, record=changed)
                self.assertFalse(result.ok)
                self.assertEqual(result.status, "conflict")
                self.assertEqual(result.content_hash, first.content_hash)
        with sqlite3.connect(self.db) as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM bnl_journal_source_events").fetchone()[0], 1)

    def test_replay_cannot_restore_withdrawn_source_eligibility(self):
        record_tiktok_engagement_event(self.db, guild_id=77, record=metric())
        with sqlite3.connect(self.db) as conn:
            conn.execute("DROP TRIGGER trg_bnl_journal_sources_no_update")
            conn.execute("UPDATE bnl_journal_source_events SET public_usable=0")
        result = record_tiktok_engagement_event(self.db, guild_id=77, record=metric())
        self.assertTrue(result.ok)
        with sqlite3.connect(self.db) as conn:
            self.assertEqual(conn.execute("SELECT public_usable FROM bnl_journal_source_events").fetchone()[0], 0)

    def test_invalid_data_and_chat_cannot_enter_the_metric_path(self):
        for invalid in (metric(like_count=-1), metric(like_count=True), metric(event_type="comment", comment_text="Hi"),
                        metric(event_type="gift", gift_count=3, combo=True, streak_over=False)):
            self.assertFalse(record_tiktok_engagement_event(self.db, guild_id=77, record=invalid).ok)
        self.assertFalse(Path(self.db).exists())


class TikTokEngagementIngestionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
        os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")
        import bnl01_bot
        cls.bot = bnl01_bot

    def test_restart_replay_and_conflict_hold_use_the_existing_ingestion_task(self):
        directory_owner = tempfile.TemporaryDirectory()
        self.addCleanup(directory_owner.cleanup)
        self.addCleanup(gc.collect)
        directory = directory_owner.name
        db = str(Path(directory) / "bnl.db")
        spool = Path(directory) / "public-conversation.ndjson"
        spool.write_text(json.dumps(metric()) + "\n", encoding="utf-8")
        with mock.patch.object(self.bot, "DB_FILE", db), \
             mock.patch.object(self.bot, "_known_discord_identities_for_tiktok") as identities, \
             mock.patch.object(self.bot, "_shadow_memory_ledger_write") as memory:
            first = self.bot.ingest_tiktok_live_memory_once(77, path=str(spool))
            restart = self.bot.ingest_tiktok_live_memory_once(77, path=str(spool))
            self.assertTrue(first["ok"] and restart["ok"])
            self.assertGreater(first["offset"], 0)
            identities.assert_not_called()
            memory.assert_not_called()
            with spool.open("a", encoding="utf-8") as output:
                output.write(json.dumps(metric(like_count=99)) + "\n")
            conflict = self.bot.ingest_tiktok_live_memory_once(
                77, path=str(spool), offset=first["offset"], spool_identity=first["spoolIdentity"],
            )
            self.assertFalse(conflict["ok"])
            self.assertEqual(conflict["offset"], first["offset"])
        with sqlite3.connect(db) as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM bnl_journal_source_events").fetchone()[0], 1)


if __name__ == "__main__":
    unittest.main()
