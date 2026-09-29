"""Ballads read the whole authorized episode through current source owners."""
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from bnl_journal_source_store import record_source_event, purge_user_bound_conversation_sources_on_connection
from bnl_tiktok_show_ledger import build_broadcast_ballad_evidence, sync_tiktok_show_evidence_ledgers
from tests import test_tiktok_show_evidence_ledger as fixture


class BalladEpisodeCoverageTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db = str(Path(self.tmp.name) / "episode.db")
        fixture.TikTokShowEvidenceLedgerTests().seed_source_and_memory(self.db)
        self.show = fixture.archived_show()
        self.sync()

    def sync(self):
        sync_tiktok_show_evidence_ledgers(self.db, guild_id=77,
            read_model=fixture.authorized_read_model({"currentShow": None, "latestShow": self.show, "shows": []}),
            artist_identity_index=fixture.artist_index(), environ=fixture.ENABLED_QUEUE_ENV)

    def read(self):
        return build_broadcast_ballad_evidence(self.db, 77, "show-attendance-1")

    def test_full_episode_includes_thousands_of_original_messages_and_late_quiet_speaker(self):
        # Independent originals spanning the window, not a seeded selected recap.
        for i in range(2063):
            record_source_event(self.db, guild_id=77, source_kind="tiktok_live_chat",
                source_key=f"full-{i}", occurred_at_ms=fixture.stamp("2026-08-29T00:00:00Z") + i * 200,
                raw_text=f"Scene {i}: a different detail in the room.", sanitized_summary="",
                channel_policy="public_context", subject_ref=f"tiktok_user:member{i % 40}",
                private_display_name=f"Test Member {i % 40}", public_usable=True,
                metadata={"eventType": "comment", "handle": f"member{i % 40}"})
        text, digest = self.read()  # Fresh originals must work before periodic ledger sync.
        for i in range(2063):
            self.assertIn(f"Scene {i}:", text)
        self.assertIn('"eligibleTikTokMessages":2067', text)
        self.assertIn('Test Member 39', text)
        self.assertEqual(len(digest), 64)

    def test_old_bot_totals_and_private_rows_are_not_show_facts(self):
        with sqlite3.connect(self.db) as conn:
            conn.execute("UPDATE conversations SET content=? WHERE role='model'",
                         ("123,000 taps and 25 active tracks. A BNL-invented scene.",))
        text, _ = self.read()
        self.assertNotIn("123,000", text)
        self.assertNotIn("BNL-invented", text)
        self.assertNotIn("This private row", text)
        self.assertIn('"finalTapTotal":null', text)
        self.assertIn('"rosteredSubmissions":2', text)
        self.assertIn('"finished":2', text)
        self.assertIn("First Signal", text)
        self.assertIn("Queue Light", text)

    def test_current_withdrawal_and_new_sources_override_cached_episode(self):
        text, before = self.read()
        self.assertIn("the green visuals during this song are wild.", text)
        with sqlite3.connect(self.db) as conn:
            purge_user_bound_conversation_sources_on_connection(conn, 77, 42)
        text, after = self.read()
        self.assertNotIn("the green visuals during this song are wild.", text)
        self.assertNotEqual(before, after)
        record_source_event(self.db, guild_id=77, source_kind="tiktok_live_chat",
            source_key="new-comment", occurred_at_ms=fixture.stamp("2026-08-29T00:02:00Z"),
            raw_text="A new public observation.", sanitized_summary="", channel_policy="public_context",
            subject_ref="tiktok_user:test", private_display_name="Test Member", public_usable=True,
            metadata={"eventType": "comment", "handle": "test"})
        text, changed = self.read()
        self.assertIn("A new public observation.", text)
        self.assertNotEqual(changed, after)

    def test_missing_originals_do_not_resurrect_retained_chat(self):
        with sqlite3.connect(self.db) as conn:
            conn.execute("DROP TABLE bnl_journal_source_events")
        self.assertEqual(self.read(), ("", ""))

    def test_exact_show_and_guild_scope_remain_required(self):
        self.assertEqual(build_broadcast_ballad_evidence(self.db, 88, "show-attendance-1"), ("", ""))
        self.assertEqual(build_broadcast_ballad_evidence(self.db, 77, "private-rehearsal"), ("", ""))

    def test_unavailable_discord_is_labeled_without_discarding_tiktok(self):
        with mock.patch("bnl_tiktok_show_ledger._load_show_discord_exchanges", return_value=None):
            text, _ = self.read()
        self.assertIn('"discordWindowReadComplete":false', text)
        self.assertIn("the green visuals", text)

    def test_digest_and_order_are_stable_for_unchanged_sources(self):
        self.assertEqual(self.read(), self.read())
