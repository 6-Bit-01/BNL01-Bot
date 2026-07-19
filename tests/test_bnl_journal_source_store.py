import sqlite3
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import bnl_journal_source_store as store


class JournalSourceStoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db = str(Path(self.temp.name) / "journal.db")

    def tearDown(self):
        self.temp.cleanup()

    def _record(self, key, occurred=1_000, **overrides):
        values = {
            "guild_id": 1,
            "source_kind": "discord_message",
            "source_key": str(key),
            "occurred_at_ms": occurred,
            "raw_text": "A public source message",
            "sanitized_summary": "A public source message",
            "channel_id": 10,
            "channel_policy": "public_home",
            "subject_ref": "discord_user:7",
            "private_display_name": "KnownUser",
            "public_usable": True,
            "metadata": {"messageId": str(key)},
            "ingested_at_ms": 2_000,
        }
        values.update(overrides)
        return store.record_source_event(self.db, **values)

    def test_exact_replay_is_idempotent_and_rows_are_immutable(self):
        first = self._record("123")
        replay = self._record("123", ingested_at_ms=9_999)
        conflict = self._record("123", raw_text="Changed source text", sanitized_summary="Changed source text")

        self.assertTrue(first.ok)
        self.assertEqual("inserted", first.status)
        self.assertTrue(replay.ok)
        self.assertEqual("idempotent", replay.status)
        self.assertEqual(first.event_seq, replay.event_seq)
        self.assertFalse(conflict.ok)
        self.assertEqual("conflict", conflict.status)
        with sqlite3.connect(self.db) as conn:
            self.assertEqual(1, conn.execute("SELECT COUNT(*) FROM bnl_journal_source_events").fetchone()[0])
            with self.assertRaises(sqlite3.IntegrityError):
                conn.execute("UPDATE bnl_journal_source_events SET raw_text='mutated'")
            with self.assertRaises(sqlite3.IntegrityError):
                conn.execute("DELETE FROM bnl_journal_source_events")
            with self.assertRaises(sqlite3.IntegrityError):
                conn.execute(
                    """
                    INSERT OR REPLACE INTO bnl_journal_source_events(
                        event_seq,guild_id,source_kind,source_key,occurred_at_ms,ingested_at_ms,
                        channel_id,channel_policy,subject_ref,private_display_name,raw_text,
                        sanitized_summary,content_hash,public_usable,metadata_json
                    )
                    SELECT event_seq,guild_id,source_kind,source_key,occurred_at_ms+1,ingested_at_ms,
                           channel_id,channel_policy,subject_ref,private_display_name,'replaced',
                           sanitized_summary,content_hash,public_usable,metadata_json
                    FROM bnl_journal_source_events WHERE source_key='123'
                    """
                )

    def test_concurrent_exact_replays_return_inserted_then_idempotent(self):
        store.ensure_schema(self.db)

        def record_once(_index):
            return self._record("same-concurrent-key")

        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(record_once, range(24)))

        self.assertEqual(1, sum(result.status == "inserted" for result in results))
        self.assertEqual(23, sum(result.status == "idempotent" for result in results))
        self.assertTrue(all(result.ok for result in results))
        self.assertEqual(1, len({result.event_seq for result in results}))

    def test_query_is_exact_unbounded_deterministic_and_fully_counted(self):
        self._record("before", occurred=999)
        self._record("end", occurred=2_000)
        for index in reversed(range(85)):
            self._record(
                "%03d" % index,
                occurred=1_500 + (index % 3),
                source_kind="website_relay" if index % 2 else "discord_message",
                channel_id=10 if index % 4 else 11,
                channel_policy="public_home" if index % 3 else "public_context",
                subject_ref="discord_user:%d" % (index % 5),
                public_usable=index % 7 != 0,
            )

        result = store.query_source_events(self.db, 1, 1_000, 2_000)

        self.assertEqual(85, len(result.events))
        ordering = [(e["occurred_at_ms"], e["source_kind"], e["source_key"], e["event_seq"]) for e in result.events]
        self.assertEqual(sorted(ordering), ordering)
        self.assertEqual(85, result.counts["total"])
        self.assertEqual(43, result.counts["bySourceKind"]["discord_message"])
        self.assertEqual(42, result.counts["bySourceKind"]["website_relay"])
        self.assertEqual(72, result.counts["publicUsable"])
        self.assertEqual(13, result.counts["notPublicUsable"])
        self.assertEqual(56, result.counts["byChannelPolicy"]["public_home"])
        self.assertEqual(29, result.counts["byChannelPolicy"]["public_context"])
        self.assertEqual(5, result.counts["uniqueSubjects"])
        self.assertEqual(2, result.counts["uniqueChannels"])
        self.assertFalse(result.counts["coverageComplete"])
        activated_at = result.counts["archiveActivatedAtMs"]
        self.assertIsInstance(activated_at, int)
        covered = store.query_source_events(self.db, 1, activated_at, activated_at + 1)
        self.assertTrue(covered.counts["coverageComplete"])
        self.assertNotIn("before", {e["source_key"] for e in result.events})
        self.assertNotIn("end", {e["source_key"] for e in result.events})

    def test_legacy_backfill_uses_real_message_ids_filters_and_is_idempotent(self):
        with sqlite3.connect(self.db) as conn:
            conn.execute(
                "CREATE TABLE website_relay_history("
                "relay_id TEXT PRIMARY KEY,guild_id INTEGER,public_message TEXT,public_directive TEXT,"
                "mode TEXT,relay_lane TEXT,event_type TEXT,published_timestamp TEXT)"
            )
            conn.executemany(
                "INSERT INTO website_relay_history VALUES (?,?,?,?,?,?,?,?)",
                [
                    ("relay-b", 1, "Second relay", "Follow the hook", "OBS", "signal", "accepted", "2026-07-18T01:00:00Z"),
                    ("relay-a", 1, "First relay", "Watch the room", "OBS", "signal", "accepted", "2026-07-18 00:00:00"),
                    ("other-guild", 2, "Ignore", "Ignore", "OBS", "signal", "accepted", "2026-07-18T00:00:00Z"),
                ],
            )
            conn.execute(
                "CREATE TABLE conversations("
                "id INTEGER PRIMARY KEY,user_id INTEGER,user_name TEXT,guild_id INTEGER,channel_name TEXT,"
                "channel_policy TEXT,channel_id INTEGER,message_id INTEGER,role TEXT,content TEXT,timestamp TEXT,"
                "public_usable INTEGER,visibility TEXT)"
            )
            conn.executemany(
                "INSERT INTO conversations VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                [
                    (1, 7, "KnownUser", 1, "general-chat", "public_home", 10, 777001, "user", "KnownUser shared a https://example.com hook", "2026-07-18T02:00:00+00:00", 1, "public_safe"),
                    (2, 8, "OtherUser", 1, "wips", "public_context", 11, None, "user", "OtherUser answered KnownUser", "2026-07-18 02:01:00", 1, "public"),
                    (3, 9, "PrivateUser", 1, "staff", "internal_controlled", 12, 777003, "user", "private", "2026-07-18T02:02:00Z", 1, "public_safe"),
                    (4, 10, "Bot", 1, "general-chat", "public_home", 10, 777004, "model", "model output", "2026-07-18T02:03:00Z", 1, "public_safe"),
                    (5, 11, "Unsafe", 1, "general-chat", "public_home", 10, 777005, "user", "unsafe", "2026-07-18T02:04:00Z", 0, "public_safe"),
                    (6, 12, "Hidden", 1, "general-chat", "public_home", 10, 777006, "user", "hidden", "2026-07-18T02:05:00Z", 1, "internal"),
                    (7, 13, "Broken", 1, "general-chat", "public_home", 10, 777007, "user", "bad time", "not-a-time", 1, "public_safe"),
                    (8, 14, "MalformedId", 1, "general-chat", "public_home", 10, "not-a-message-id", "user", "valid public content", "2026-07-18T02:06:00Z", 1, "public_safe"),
                ],
            )

        first = store.backfill_legacy_sources(self.db, 1)
        second = store.backfill_legacy_sources(self.db, 1)
        result = store.query_source_events(self.db, 1, 0, 9_999_999_999_999)

        self.assertEqual(5, first.inserted)
        self.assertEqual(0, first.idempotent)
        self.assertEqual(2, first.relay_rows_scanned)
        self.assertEqual(4, first.conversation_rows_scanned)
        self.assertEqual(1, first.invalid_timestamps)
        self.assertEqual(1, first.skipped)
        self.assertEqual(0, second.inserted)
        self.assertEqual(0, second.idempotent)
        self.assertEqual(0, second.relay_rows_scanned)
        self.assertEqual(0, second.conversation_rows_scanned)
        self.assertEqual(5, result.counts["total"])
        discord = {e["source_key"]: e for e in result.events if e["source_kind"] == "discord_message"}
        self.assertIn("777001", discord)
        self.assertIn("legacy_row:2", discord)
        self.assertIn("legacy_row:8", discord)
        self.assertEqual("KnownUser shared a https://example.com hook", discord["777001"]["raw_text"])
        self.assertNotIn("KnownUser", discord["777001"]["sanitized_summary"])
        self.assertNotIn("example.com", discord["777001"]["sanitized_summary"])
        self.assertEqual(777001, discord["777001"]["metadata"]["legacyMessageId"])

    def test_legacy_relay_backfill_recovers_late_insert_with_older_timestamp(self):
        with sqlite3.connect(self.db) as conn:
            conn.execute(
                "CREATE TABLE website_relay_history("
                "relay_id TEXT PRIMARY KEY,guild_id INTEGER,public_message TEXT,public_directive TEXT,"
                "mode TEXT,relay_lane TEXT,event_type TEXT,published_timestamp TEXT)"
            )
            conn.execute(
                "INSERT INTO website_relay_history VALUES (?,?,?,?,?,?,?,?)",
                ("newer", 1, "Newer relay", "", "OBS", "signal", "accepted", "2026-07-18T02:00:00Z"),
            )

        first = store.backfill_legacy_sources(self.db, 1)
        with sqlite3.connect(self.db) as conn:
            conn.execute(
                "INSERT INTO website_relay_history VALUES (?,?,?,?,?,?,?,?)",
                ("late-old", 1, "Late hydrated relay", "", "OBS", "hydrated", "accepted", "2026-07-18T01:00:00Z"),
            )
        second = store.backfill_legacy_sources(self.db, 1)
        result = store.query_source_events(self.db, 1, 0, 9_999_999_999_999)

        self.assertEqual(1, first.inserted)
        self.assertEqual(1, second.relay_rows_scanned)
        self.assertEqual(1, second.inserted)
        self.assertEqual({"newer", "late-old"}, {event["source_key"] for event in result.events})
        with sqlite3.connect(self.db) as conn:
            self.assertEqual(
                2,
                conn.execute(
                    "SELECT legacy_relay_cursor_rowid FROM bnl_journal_source_archive_state WHERE guild_id=1"
                ).fetchone()[0],
            )

    def test_explicit_user_and_guild_purges_preserve_normal_immutability(self):
        self._record("user-7", subject_ref="discord_user:7")
        self._record("user-8", subject_ref="discord_user:8")
        self._record("other-guild", guild_id=2, subject_ref="discord_user:7")
        self._record(
            "relay-1",
            source_kind="website_relay",
            subject_ref="bnl_01",
            channel_policy="public_relay",
        )
        with sqlite3.connect(self.db) as conn:
            with self.assertRaises(sqlite3.IntegrityError):
                conn.execute("DELETE FROM bnl_journal_source_events WHERE source_key='user-7'")

        self.assertEqual(1, store.purge_user_discord_sources(self.db, 1, 7))
        guild_one = store.query_source_events(self.db, 1, 0, 9_999_999_999_999)
        self.assertEqual({"user-8", "relay-1"}, {event["source_key"] for event in guild_one.events})

        self.assertEqual(1, store.purge_guild_discord_sources(self.db, 1))
        guild_one = store.query_source_events(self.db, 1, 0, 9_999_999_999_999)
        guild_two = store.query_source_events(self.db, 2, 0, 9_999_999_999_999)
        self.assertEqual({"relay-1"}, {event["source_key"] for event in guild_one.events})
        self.assertEqual({"other-guild"}, {event["source_key"] for event in guild_two.events})
        with sqlite3.connect(self.db) as conn:
            with self.assertRaises(sqlite3.IntegrityError):
                conn.execute("DELETE FROM bnl_journal_source_events WHERE source_key='relay-1'")

    def test_legacy_backfill_supports_current_minimal_conversation_schema(self):
        with sqlite3.connect(self.db) as conn:
            conn.execute(
                "CREATE TABLE conversations("
                "id INTEGER PRIMARY KEY,user_id INTEGER,user_name TEXT,guild_id INTEGER,channel_name TEXT,"
                "channel_policy TEXT,role TEXT,content TEXT,timestamp TEXT)"
            )
            conn.execute(
                "INSERT INTO conversations VALUES (1,7,'User',1,'general-chat','public_home','user','hello','2026-07-18 00:00:00')"
            )

        backfill = store.backfill_legacy_sources(self.db, 1)
        result = store.query_source_events(self.db, 1, 0, 9_999_999_999_999)

        self.assertEqual(1, backfill.inserted)
        self.assertEqual("legacy_row:1", result.events[0]["source_key"])
        self.assertIsNone(result.events[0]["channel_id"])


if __name__ == "__main__":
    unittest.main()
