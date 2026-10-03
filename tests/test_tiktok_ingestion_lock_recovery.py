import gc
import json
import os
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest import mock

import bnl_journal_source_store as source_store


def tap(event_id="tap-1"):
    return {"event_type": "like", "event_id": event_id, "room_id": "test-room",
            "observed_at": 1_800_000_010.0, "source_at": 1_800_000_000.0,
            "like_count": 23, "like_total": 700}


def chat(**changes):
    value = {"event_type": "comment", "event_id": "chat-1", "room_id": "test-room",
             "observed_at": 1_800_000_010.0, "source_at": None,
             "unique_id": "test.viewer", "display_name": "Test Viewer",
             "comment_text": "That bass sounds great", "moderator_flag": False}
    value.update(changes)
    return value


class TikTokIngestionLockRecoveryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
        os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")
        import bnl01_bot
        cls.bot = bnl01_bot

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.addCleanup(gc.collect)
        self.db = str(Path(self.directory.name) / "bnl.db")
        self.spool = Path(self.directory.name) / "public-conversation.ndjson"
        self.patch = mock.patch.object(self.bot, "DB_FILE", self.db)
        self.patch.start()
        self.addCleanup(self.patch.stop)
        memory_patch = mock.patch.object(self.bot, "_shadow_memory_ledger_write")
        self.memory = memory_patch.start()
        self.addCleanup(memory_patch.stop)

    def write(self, *records):
        self.spool.write_text("".join(json.dumps(r) + "\n" for r in records), encoding="utf-8")

    def rows(self):
        with closing(sqlite3.connect(self.db)) as conn:
            return conn.execute("SELECT * FROM bnl_journal_source_events ORDER BY event_seq").fetchall()

    def test_mixed_batch_prepares_schema_once_and_archives_every_original(self):
        self.write(tap(), chat(), tap("tap-2"))
        prepare = mock.Mock(wraps=source_store.ensure_schema)
        with mock.patch.object(self.bot, "ensure_journal_source_schema", prepare), \
             mock.patch.object(source_store, "ensure_schema", prepare), \
             mock.patch.object(self.bot, "_known_discord_identities_for_tiktok", return_value={}):
            result = self.bot.ingest_tiktok_live_memory_once(77, path=str(self.spool))
        self.assertTrue(result["ok"])
        self.assertEqual(result["ingested"], 3)
        self.assertEqual(prepare.call_count, 1)
        self.assertEqual(len(self.rows()), 3)

    def test_receipt_and_hint_replay_preserve_chat_and_allow_following_metrics(self):
        self.write(chat())
        with mock.patch.object(self.bot, "_known_discord_identities_for_tiktok", return_value={99: ("Test Viewer",)}):
            first = self.bot.ingest_tiktok_live_memory_once(77, path=str(self.spool))
        original = self.rows()[0]
        self.assertTrue(first["ok"])
        self.assertEqual(original[8], "discord_user:99")
        self.write(chat(observed_at=1_800_000_030.0), tap())
        with mock.patch.object(self.bot, "_known_discord_identities_for_tiktok", return_value={}):
            replay = self.bot.ingest_tiktok_live_memory_once(77, path=str(self.spool))
        self.assertTrue(replay["ok"])
        self.assertEqual(self.rows()[0], original)
        self.assertEqual(len(self.rows()), 2)
        self.assertEqual(self.memory.call_count, 1)

    def test_platform_clock_replay_keeps_first_binding_and_real_changes_hold_cursor(self):
        value = chat(source_at=1_800_000_000.0)
        self.write(value)
        with mock.patch.object(self.bot, "_known_discord_identities_for_tiktok", return_value={99: ("Test Viewer",)}):
            first = self.bot.ingest_tiktok_live_memory_once(77, path=str(self.spool))
        original = self.rows()[0]
        self.write(chat(source_at=1_800_000_000.0, observed_at=1_800_000_030.0), tap())
        with mock.patch.object(self.bot, "_known_discord_identities_for_tiktok", return_value={}):
            replay = self.bot.ingest_tiktok_live_memory_once(77, path=str(self.spool))
        self.assertTrue(first["ok"] and replay["ok"])
        self.assertEqual(self.rows()[0], original)
        self.write(chat(source_at=1_800_000_001.0), tap("tap-2"))
        with mock.patch.object(self.bot, "_known_discord_identities_for_tiktok", return_value={}):
            conflict = self.bot.ingest_tiktok_live_memory_once(77, path=str(self.spool))
        self.assertFalse(conflict["ok"])
        self.assertEqual(conflict["offset"], 0)
        self.assertEqual(self.rows()[0], original)
        self.assertEqual(len(self.rows()), 2)

    def test_writer_lock_preserves_cursor_until_release_and_exact_retry(self):
        self.write(tap())
        source_store.ensure_schema(self.db)
        connect = sqlite3.connect

        def short_connect(*args, **kwargs):
            kwargs["timeout"] = 0.02
            return connect(*args, **kwargs)

        with closing(connect(self.db)) as writer:
            writer.execute("BEGIN IMMEDIATE")
            with mock.patch.object(source_store.sqlite3, "connect", side_effect=short_connect):
                failed = self.bot.ingest_tiktok_live_memory_once(77, path=str(self.spool))
            self.assertFalse(failed["ok"])
            self.assertEqual(failed["errorType"], "OperationalError")
            self.assertEqual(failed["offset"], 0)
            self.assertEqual(self.rows(), [])
            writer.rollback()
        recovered = self.bot.ingest_tiktok_live_memory_once(77, path=str(self.spool))
        replay = self.bot.ingest_tiktok_live_memory_once(77, path=str(self.spool))
        self.assertTrue(recovered["ok"] and replay["ok"])
        self.assertEqual(len(self.rows()), 1)

    def test_locked_partial_batch_replays_committed_prefix_without_counting_twice(self):
        self.write(tap(), tap("tap-2"))
        source_store.ensure_schema(self.db)
        connect = sqlite3.connect
        record = self.bot.record_tiktok_engagement_event
        calls = 0

        def short_connect(*args, **kwargs):
            kwargs["timeout"] = 0.02
            return connect(*args, **kwargs)

        with closing(connect(self.db)) as writer:
            def archive_then_lock(*args, **kwargs):
                nonlocal calls
                result = record(*args, **kwargs)
                calls += 1
                if calls == 1:
                    writer.execute("BEGIN IMMEDIATE")
                return result
            with mock.patch.object(source_store.sqlite3, "connect", side_effect=short_connect), \
                 mock.patch.object(self.bot, "record_tiktok_engagement_event", side_effect=archive_then_lock):
                failed = self.bot.ingest_tiktok_live_memory_once(77, path=str(self.spool))
            self.assertFalse(failed["ok"])
            self.assertEqual(failed["ingested"], 1)
            self.assertEqual(failed["offset"], 0)
            self.assertEqual(len(self.rows()), 1)
            writer.rollback()
        recovered = self.bot.ingest_tiktok_live_memory_once(77, path=str(self.spool))
        self.assertTrue(recovered["ok"])
        self.assertEqual(recovered["ingested"], 2)
        self.assertEqual(len(self.rows()), 2)
        self.assertGreater(recovered["offset"], 0)


if __name__ == "__main__":
    unittest.main()
