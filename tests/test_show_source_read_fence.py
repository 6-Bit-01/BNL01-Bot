"""Optional creative consumers fence retained show views against current originals."""
import gc
import hashlib
import inspect
import sqlite3
import tempfile
import unittest
from pathlib import Path

import bnl_tiktok_show_ledger as shows
from tests import test_tiktok_show_evidence_ledger as fixture


class ShowSourceReadFenceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.addCleanup(gc.collect)
        self.db = str(Path(self.tmp.name) / "show-read.db")
        fixture.TikTokShowEvidenceLedgerTests().seed_source_and_memory(self.db)
        self.show = fixture.archived_show()
        self.sync()
        self.query = "The last 3 shows community chat timeline"
        self.now = "2026-08-29T12:00:00-07:00"

    def sync(self, extra=()):
        shows.sync_tiktok_show_evidence_ledgers(self.db, guild_id=77,
            read_model=fixture.authorized_read_model({
                "currentShow": None, "latestShow": self.show, "shows": list(extra),
            }), artist_identity_index=fixture.artist_index(), environ=fixture.ENABLED_QUEUE_ENV)

    def read(self, *, current=True, max_shows=8):
        # A pre-feature reader still runs the real stale-view behavior, so red
        # verifies the safety regression rather than merely an unknown argument.
        options = {"max_shows": max_shows}
        if "require_current_originals" in inspect.signature(shows.select_tiktok_show_episode_context_items).parameters:
            options["require_current_originals"] = current
        with sqlite3.connect(self.db) as conn:
            return shows.select_tiktok_show_episode_context_items(conn, guild_id=77,
                user_text=self.query, subject_user_id=0, now=self.now, **options)

    def versions(self, *, max_shows=8):
        options = {}
        parameters = inspect.signature(shows.tiktok_show_episode_context_item_versions).parameters
        if "require_current_originals" in parameters:
            options["require_current_originals"] = True
        if "max_shows" in parameters:
            options["max_shows"] = max_shows
        with sqlite3.connect(self.db) as conn:
            return shows.tiktok_show_episode_context_item_versions(conn, guild_id=77,
                user_text=self.query, subject_user_id=0, now=self.now, **options)

    def change_source(self, sql, values=()):
        # Only this disposable fixture is changed; archived originals remain
        # immutable in production and use their existing privacy owners.
        with sqlite3.connect(self.db) as conn:
            conn.execute("DROP TRIGGER trg_bnl_journal_sources_no_update")
            conn.execute(sql, values)

    def seed_passive_discord_row(self):
        # Arrives after the retained episode was captured, with no BNL reply
        # or source-event projection. Its original owns the fresh timeline.
        text = "The paper lantern went sideways."
        with sqlite3.connect(self.db) as conn:
            conn.execute("""INSERT INTO conversations
                (id,user_id,user_name,guild_id,channel_name,channel_policy,
                 route_mode,role,content,timestamp,channel_id,message_id)
                VALUES (203,43,'Pat',77,'barcode-bot','public_home',
                        'normal_chat','user',?,'2026-08-29T00:05:30+00:00',9001,7002)""", (text,))
            result = fixture.shadow_conversation_row(conn, row_id=203,
                user_id=43, user_name="Pat", guild_id=77, role="user",
                content=text, channel_name="barcode-bot", channel_policy="public_home",
                channel_id=9001, message_id=7002, route_mode="normal_chat",
                observed_at="2026-08-29T00:05:30+00:00")
            self.assertEqual(result.outcome, "inserted")
        return text, result.entry_id

    def test_withdrawal_removes_cached_human_views_but_preserves_operations(self):
        initial = self.read()
        self.assertEqual({item.kind for item in initial}, {"community", "dialogue", "operations"})
        self.change_source("UPDATE bnl_journal_source_events SET public_usable=0 WHERE source_key='event-nova-1'")
        current = self.read()
        self.assertEqual({item.kind for item in current}, {"operations"})
        self.assertIn("First Signal", current[0].text)
        self.assertEqual({item.kind for item in self.read(current=False)}, {"community", "dialogue", "operations"})

    def test_correction_invalidates_selected_human_view_version(self):
        selected = self.read()
        human = next(item for item in selected if item.kind == "dialogue")
        text = "The public observation has been corrected."
        self.change_source("UPDATE bnl_journal_source_events SET raw_text=?,content_hash=? WHERE source_key='event-nova-1'",
            (text, hashlib.sha256(text.encode()).hexdigest()))
        self.assertNotIn(human.source_ref, self.versions())
        self.assertEqual({item.kind for item in self.read()}, {"operations"})

    def test_unavailable_originals_never_revive_cached_human_views(self):
        with sqlite3.connect(self.db) as conn:
            conn.execute("ALTER TABLE bnl_journal_source_events RENAME TO fixture_unavailable_sources")
        self.assertEqual({item.kind for item in self.read()}, {"operations"})

    def test_eligible_source_revision_is_bound_to_human_views(self):
        initial = self.read()
        human = next(item for item in initial if item.kind == "community")
        operations = next(item for item in initial if item.kind == "operations")
        self.change_source("UPDATE bnl_journal_source_events SET event_seq=event_seq+100 WHERE source_key='event-nova-1'")
        current = self.versions()
        self.assertIn(human.source_ref, current)
        self.assertNotEqual(current[human.source_ref], human.source_digest)
        self.assertEqual(current[operations.source_ref], operations.source_digest)

    def test_revalidation_preserves_requested_show_limit(self):
        extra = []
        for index in (2, 3):
            value = fixture.archived_show()
            value["sessionId"] = "show-bounded-%s" % index
            extra.append(value)
        self.sync(extra)
        selected = self.read(max_shows=2)
        community = next(item for item in selected if item.kind == "community")
        self.assertEqual(len(community.show_keys), 2)
        self.assertEqual(self.versions(max_shows=2).get(community.source_ref), community.source_digest)

    def test_source_fence_is_read_only(self):
        with sqlite3.connect("file:%s?mode=ro" % self.db, uri=True) as conn:
            options = {}
            if "require_current_originals" in inspect.signature(shows.select_tiktok_show_episode_context_items).parameters:
                options["require_current_originals"] = True
            items = shows.select_tiktok_show_episode_context_items(conn, guild_id=77,
                user_text=self.query, subject_user_id=0, now=self.now, **options)
        self.assertEqual({item.kind for item in items}, {"community", "dialogue", "operations"})

    def test_nonshow_topic_cannot_reuse_discord_privacy_changed_after_capture(self):
        self.query = "community green visuals"
        self.assertEqual({item.kind for item in self.read()}, {"community", "dialogue"})
        with sqlite3.connect(self.db) as conn:
            conn.execute("UPDATE conversations SET channel_policy='internal_controlled' WHERE id=101")
        self.assertFalse(any(item.kind in {"community", "dialogue"} for item in self.read()))

    def test_discord_correction_invalidates_cached_community_version(self):
        initial = self.read()
        community = next(item for item in initial if item.kind == "community")
        with sqlite3.connect(self.db) as conn:
            conn.execute("UPDATE conversations SET content='The public detail has been corrected.' WHERE id=101")
        self.assertNotIn(community.source_ref, self.versions())
        self.assertEqual({item.kind for item in self.read()}, {"operations"})

    def test_passive_discord_withdrawal_invalidates_fresh_dialogue_version(self):
        text, entry_id = self.seed_passive_discord_row()
        initial = self.read()
        dialogue = next(item for item in initial if item.kind == "dialogue")
        operations = next(item for item in initial if item.kind == "operations")
        self.assertIn(text, dialogue.text)
        with sqlite3.connect(self.db) as conn:
            conn.execute("""UPDATE memory_ledger_entries SET
                lifecycle_status='superseded',public_usable=0 WHERE entry_id=?""", (entry_id,))
        current = self.read()
        self.assertFalse(any(text in item.text for item in current))
        versions = self.versions()
        self.assertNotEqual(versions.get(dialogue.source_ref), dialogue.source_digest)
        self.assertEqual(versions.get(operations.source_ref), operations.source_digest)
        self.assertTrue(any(text in item.text for item in self.read(current=False)))

    def test_passive_discord_incoming_correction_invalidates_fresh_dialogue_version(self):
        text, entry_id = self.seed_passive_discord_row()
        initial = self.read()
        dialogue = next(item for item in initial if item.kind == "dialogue")
        operations = next(item for item in initial if item.kind == "operations")
        self.assertIn(text, dialogue.text)
        with sqlite3.connect(self.db) as conn:
            correction_entry = conn.execute("""SELECT entry_id FROM memory_ledger_entries
                WHERE guild_id=77 AND source_table='conversations' AND source_row_id='101'
                  AND entry_type='observation'""").fetchone()[0]
            conn.execute("""INSERT INTO memory_ledger_lineage
                (entry_id,guild_id,lineage_type,target_entry_id,created_at)
                VALUES (?,77,'correction_of',?,'2026-08-29T00:08:00+00:00')""",
                (correction_entry, entry_id))
            # Prove the incoming edge itself fences an otherwise active row.
            conn.execute("UPDATE memory_ledger_entries SET lifecycle_status='active',public_usable=1 WHERE entry_id=?",
                (entry_id,))
        current = self.read()
        self.assertFalse(any(text in item.text for item in current))
        versions = self.versions()
        self.assertNotEqual(versions.get(dialogue.source_ref), dialogue.source_digest)
        self.assertEqual(versions.get(operations.source_ref), operations.source_digest)

    def test_passive_discord_topic_recall_revalidates_current_original(self):
        self.query = "community paper lantern"
        text, entry_id = self.seed_passive_discord_row()
        legacy = self.read(current=False)
        initial = next(item for item in self.read() if item.kind == "dialogue")
        self.assertIn(text, initial.text)
        with sqlite3.connect(self.db) as conn:
            conn.execute("""UPDATE memory_ledger_entries SET
                lifecycle_status='superseded',public_usable=0 WHERE entry_id=?""", (entry_id,))
        self.assertFalse(any(text in item.text for item in self.read()))
        self.assertNotEqual(self.versions().get(initial.source_ref), initial.source_digest)
        self.assertEqual(self.read(current=False), legacy)


if __name__ == "__main__":
    unittest.main()
