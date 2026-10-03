import gc
import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest import mock

import bnl_journal_source_store as source_store


def chat_values(**changes):
    values = {
        "guild_id": 77,
        "source_kind": "tiktok_live_chat",
        "source_key": "chat-1",
        "occurred_at_ms": 1_800_000_000_000,
        "raw_text": "That bass sounds great",
        "sanitized_summary": "That bass sounds great",
        "channel_policy": "public_context",
        "subject_ref": "discord_user:99",
        "private_display_name": "Test Viewer",
        "public_usable": True,
        "metadata": {
            "platform": "tiktok", "eventType": "comment", "eventId": "chat-1",
            "roomId": "test-room", "handle": "test.viewer", "moderator": False,
            "identityPolicy": "handle_display_correlated_v1",
            "identityBindingBasis": "handle_display_correlation",
            "boundDiscordUserId": 99, "memoryPlacement": "above_community_canon",
        },
    }
    values.update(changes)
    return values


def metric():
    return {
        "event_type": "like", "event_id": "tap-1", "room_id": "test-room",
        "observed_at": 1_800_000_010.0, "source_at": 1_800_000_000.0,
        "like_count": 23, "like_total": 700,
    }


class TikTokIngestionRetryStoreTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.addCleanup(gc.collect)
        self.db = str(Path(self.directory.name) / "bnl.db")

    def snapshot(self):
        with closing(sqlite3.connect(self.db)) as conn:
            return conn.execute(
                "SELECT * FROM bnl_journal_source_events ORDER BY event_seq"
            ).fetchall()

    def record_chat(self, **changes):
        return source_store.record_source_event(self.db, **chat_values(**changes))

    def replay_metadata(self):
        metadata = chat_values()["metadata"]
        metadata.update(identityBindingBasis="tiktok_handle_identity", boundDiscordUserId=None)
        return metadata

    def test_default_callers_still_prepare_the_schema(self):
        with mock.patch.object(source_store, "ensure_schema", wraps=source_store.ensure_schema) as prepare:
            self.assertTrue(self.record_chat().ok)
            self.assertTrue(source_store.record_tiktok_engagement_event(
                self.db, guild_id=77, record=metric()).ok)
        self.assertEqual(prepare.call_count, 2)

    def test_prepared_batch_helpers_do_not_reacquire_the_schema_lease(self):
        source_store.ensure_schema(self.db)
        with mock.patch.object(source_store, "ensure_schema", side_effect=AssertionError("redundant schema lease")):
            self.assertTrue(self.record_chat(prepare_schema=False).ok)
            self.assertTrue(source_store.record_tiktok_engagement_event(
                self.db, guild_id=77, record=metric(), prepare_schema=False).ok)
        self.assertEqual(len(self.snapshot()), 2)

    def test_receipt_only_replay_preserves_first_clock_binding_and_entire_row(self):
        first = self.record_chat()
        original = self.snapshot()
        replay = self.record_chat(
            occurred_at_ms=1_800_000_030_000, subject_ref="tiktok_user:test.viewer",
            metadata=self.replay_metadata(), tiktok_chat_replay=True,
            tiktok_receipt_only_replay=True)
        self.assertEqual(replay.status, "idempotent")
        self.assertEqual((replay.event_seq, replay.content_hash), (first.event_seq, first.content_hash))
        self.assertEqual(self.snapshot(), original)

    def test_platform_timed_replay_preserves_binding_but_rejects_changed_platform_clock(self):
        self.record_chat()
        original = self.snapshot()
        common = {
            "subject_ref": "tiktok_user:test.viewer", "metadata": self.replay_metadata(),
            "tiktok_chat_replay": True,
        }
        self.assertEqual(self.record_chat(**common).status, "idempotent")
        self.assertEqual(self.record_chat(occurred_at_ms=1_800_000_030_000, **common).status, "conflict")
        self.assertEqual(self.snapshot(), original)

    def test_receipt_flag_without_chat_opt_in_does_not_relax_exact_replay(self):
        self.record_chat()
        result = self.record_chat(occurred_at_ms=1_800_000_030_000, tiktok_receipt_only_replay=True)
        self.assertFalse(result.ok)
        self.assertEqual(result.status, "conflict")

    def test_other_source_kinds_keep_exact_timestamp_and_binding_replay(self):
        self.record_chat(source_kind="discord_message")
        result = self.record_chat(
            source_kind="discord_message", occurred_at_ms=1_800_000_030_000,
            subject_ref="tiktok_user:test.viewer", metadata=self.replay_metadata(),
            tiktok_chat_replay=True, tiktok_receipt_only_replay=True)
        self.assertFalse(result.ok)
        self.assertEqual(result.status, "conflict")

    def test_matching_replay_cannot_restore_withdrawn_source_eligibility(self):
        self.record_chat()
        with closing(sqlite3.connect(self.db)) as conn, conn:
            conn.execute("DROP TRIGGER trg_bnl_journal_sources_no_update")
            conn.execute("UPDATE bnl_journal_source_events SET public_usable=0")
        withdrawn = self.snapshot()
        result = self.record_chat(
            occurred_at_ms=1_800_000_030_000, subject_ref="tiktok_user:test.viewer",
            metadata=self.replay_metadata(), tiktok_chat_replay=True,
            tiktok_receipt_only_replay=True)
        self.assertEqual(result.status, "idempotent")
        self.assertEqual(self.snapshot(), withdrawn)

    def test_changed_captured_content_and_publication_fields_remain_conflicts(self):
        self.record_chat()
        original = self.snapshot()
        for changes in (
            {"raw_text": "Changed text"},
            {"sanitized_summary": "Changed summary"},
            {"channel_policy": "public_home"},
            {"channel_id": 123},
            {"private_display_name": "Someone Else"},
        ):
            with self.subTest(changes=changes):
                result = self.record_chat(
                    **changes, tiktok_chat_replay=True, tiktok_receipt_only_replay=True)
                self.assertFalse(result.ok)
                self.assertEqual(result.status, "conflict")
        self.assertEqual(self.snapshot(), original)

    def test_changed_platform_metadata_remains_a_conflict(self):
        self.record_chat()
        original = self.snapshot()
        for key, value in (
            ("platform", "other"), ("eventType", "question"), ("eventId", "other-id"),
            ("roomId", "other-room"), ("handle", "other.viewer"), ("moderator", True),
            ("identityPolicy", "different"), ("memoryPlacement", "different"),
            ("unexpectedField", "new"),
        ):
            with self.subTest(field=key):
                metadata = self.replay_metadata()
                metadata[key] = value
                result = self.record_chat(
                    metadata=metadata, tiktok_chat_replay=True,
                    tiktok_receipt_only_replay=True)
                self.assertFalse(result.ok)
                self.assertEqual(result.status, "conflict")
        self.assertEqual(self.snapshot(), original)

    def test_receipt_replay_cannot_certify_a_corrupted_original_hash(self):
        self.record_chat()
        with closing(sqlite3.connect(self.db)) as conn, conn:
            conn.execute("DROP TRIGGER trg_bnl_journal_sources_no_update")
            conn.execute("UPDATE bnl_journal_source_events SET content_hash='invalid'")
        corrupted = self.snapshot()
        result = self.record_chat(
            occurred_at_ms=1_800_000_030_000, tiktok_chat_replay=True,
            tiktok_receipt_only_replay=True)
        self.assertFalse(result.ok)
        self.assertEqual(result.status, "conflict")
        self.assertEqual(self.snapshot(), corrupted)

    def test_genuine_writer_contention_rolls_back_then_retries_exactly_once(self):
        source_store.ensure_schema(self.db)
        sqlite_connect = sqlite3.connect

        def short_connect(*args, **kwargs):
            kwargs["timeout"] = 0.02
            return sqlite_connect(*args, **kwargs)

        with closing(sqlite_connect(self.db)) as blocking_writer:
            blocking_writer.execute("BEGIN IMMEDIATE")
            with mock.patch.object(source_store.sqlite3, "connect", side_effect=short_connect):
                with self.assertRaisesRegex(sqlite3.OperationalError, "locked"):
                    source_store.record_tiktok_engagement_event(
                        self.db, guild_id=77, record=metric(), prepare_schema=False)
            self.assertEqual(self.snapshot(), [])
            blocking_writer.rollback()
        first = source_store.record_tiktok_engagement_event(
            self.db, guild_id=77, record=metric(), prepare_schema=False)
        replay = source_store.record_tiktok_engagement_event(
            self.db, guild_id=77, record=metric(), prepare_schema=False)
        self.assertEqual((first.status, replay.status), ("inserted", "idempotent"))
        self.assertEqual((first.event_seq, first.content_hash), (replay.event_seq, replay.content_hash))
        self.assertEqual(len(self.snapshot()), 1)


if __name__ == "__main__":
    unittest.main()
