"""The existing public owners supply art; previous pictures are only fiction."""
import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sqlite3
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

import test_own_art_preview as fixtures
from test_own_art_preview import CONCEPT, PNG
import bnl_own_art as art
import bnl_ambient_art as ambient
import test_publication_read_adapters as publications


def packet():
    return {"sourceWindowStart": "2026-09-20T00:00:00Z", "sourceWindowEnd": "2026-09-23T00:00:00Z",
            "safeSources": [{"refId": "fresh:" + str(n), "summary": "A public musical contribution " + str(n),
                             "observedAt": "2026-09-22T19:00:00Z", "sourceKind": "conversation",
                             "publicSpeakerName": "Test Member", "participantAlias": "participant-fixture",
                             "conversationSurface": "tiktok_live_chat" if n == 29 else "discord"}
                            for n in range(30)]}


class ArtContinuityTests(unittest.TestCase):
    def test_journal_continuity_survives_next_daily_but_not_withdrawal_or_revision(self):
        fixture = publications.PublicationReadAdapterTests()
        fixture.setUp()
        self.addCleanup(fixture.tearDown)
        fixture.add_journal("original_art_inspiration")
        fixture.conn.commit()
        now = datetime.now(timezone.utc)
        snapshot = publications.control_snapshot(observed_at=now.isoformat(),
                                                fresh_until=(now + timedelta(minutes=2)).isoformat())
        fake = SimpleNamespace(DB_FILE=fixture.db_path,
            _journal_publication_control_snapshot_sync=mock.Mock(return_value=(snapshot, "valid")))
        root = art.journal_art_basis(fake, 1, [{"entryId": "original_art_inspiration", "revision": 1}], snapshot)
        fixture.add_journal("new_daily", published_at="2026-08-03T01:00:00Z")
        fixture.conn.commit()
        self.assertTrue(art.art_sources_current(fake, 1, [root]))
        for field in ("public_excluded_entry_ids", "memory_excluded_entry_ids"):
            fake._journal_publication_control_snapshot_sync.return_value = (
                replace(snapshot, **{field: ("original_art_inspiration",)}), "valid")
            self.assertFalse(art.art_sources_current(fake, 1, [root]))
        fake._journal_publication_control_snapshot_sync.return_value = (snapshot, "valid")
        fixture.add_journal("original_art_inspiration", revision=2, body="A corrected public account.")
        fixture.conn.commit()
        self.assertFalse(art.art_sources_current(fake, 1, [root]))

    def test_development_fetches_surrounding_exchange_and_tracks_new_sources(self):
        value = packet()
        expanded = packet()
        expanded["safeSources"].append({"refId": "fresh:99", "summary": "The answer explaining the musical joke",
                                        "observedAt": "2026-09-22T19:00:02Z", "sourceKind": "conversation"})
        expanded["safeSources"] = expanded["safeSources"][-2:]
        expanded["privateSources"] = [{"summary": "PRIVATE_NEIGHBOR"}]
        fake = fixtures.OwnArtPreviewTests().fake_bot("unused")
        proposal = {**CONCEPT, "inspirationRefs": ["fresh:29"]}
        developed = {**CONCEPT, "title": "A newly understood discovery", "inspirationRefs": ["fresh:99"]}
        fake._extract_text_and_tokens.return_value = (json.dumps(developed), 5)
        context = {"sources": art.art_source_records(value), "sourceBases": [art.art_source_basis(value)], "continuity": []}
        with mock.patch.object(art, "build_source_packet_between", return_value=expanded) as read, \
             mock.patch.object(art, "art_context_current", return_value=True):
            result = art.develop_art_concept(fake, 123, proposal, context)
        read.assert_called_once()
        self.assertFalse(read.call_args.kwargs["prepare_schema"])
        prompt = fake._generate_gemini_content_with_fallback.call_args.args[0]
        self.assertIn("The answer explaining the musical joke", prompt)
        self.assertIn("A public musical contribution 0", prompt)
        self.assertNotIn("PRIVATE_NEIGHBOR", prompt)
        self.assertEqual(result["title"], developed["title"])
        self.assertIn("fresh:99", {s["ref"] for s in context["sources"]})
        self.assertEqual(len(context["sourceBases"]), 2)

    def test_development_rechecks_sources_before_its_provider_call(self):
        fake = fixtures.OwnArtPreviewTests().fake_bot("unused")
        value = packet()
        context = {"sources": art.art_source_records(value), "sourceBases": [art.art_source_basis(value)], "continuity": []}
        with mock.patch.object(art, "build_source_packet_between", return_value=value), \
             mock.patch.object(art, "art_context_current", return_value=False):
            with self.assertRaisesRegex(ValueError, "sources_changed"):
                art.develop_art_concept(fake, 123, {**CONCEPT, "inspirationRefs": ["fresh:29"]}, context)
        fake._generate_gemini_content_with_fallback.assert_not_called()

    def test_shared_projection_keeps_later_exchange_speakers_and_platforms(self):
        value = packet()
        value["privateSources"] = [{"summary": "PRIVATE_ARCHIVE_VALUE"}]
        value["safeSources"][0]["rawSummary"] = "PRIVATE_ARCHIVE_VALUE"
        prompt, refs = art.build_own_art_brief(value)
        self.assertEqual(len(refs), 30)
        self.assertIn("A public musical contribution 29", prompt)
        self.assertIn("Test Member", prompt)
        self.assertIn("tiktok_live_chat", prompt)
        self.assertNotIn("PRIVATE_ARCHIVE_VALUE", prompt)

    def test_source_owner_changes_withdraw_context_without_writing(self):
        value = packet()
        basis = art.art_source_basis(value)
        fake = SimpleNamespace(DB_FILE="unused")
        with mock.patch.object(art, "build_source_packet_between", return_value=value) as read:
            self.assertTrue(art.art_sources_current(fake, 42, [basis]))
            self.assertFalse(read.call_args.kwargs["prepare_schema"])
            value["safeSources"].append({"refId": "fresh:100", "summary": "Unrelated later arrival"})
            self.assertTrue(art.art_sources_current(fake, 42, [basis]))
            value["safeSources"][0]["summary"] = "Corrected musical contribution"
            self.assertFalse(art.art_sources_current(fake, 42, [basis]))
            value["safeSources"].pop(0)
            self.assertFalse(art.art_sources_current(fake, 42, [basis]))

    def test_history_requires_confirmed_delivery_same_guild_and_current_roots(self):
        with tempfile.TemporaryDirectory() as folder:
            fake = SimpleNamespace(DB_FILE=str(Path(folder) / "db"), BNL_PRIMARY_GUILD_ID=42)
            self.assertEqual(art.public_creative_history(fake, 42), [])
            with sqlite3.connect(fake.DB_FILE) as conn:
                self.assertEqual(conn.execute("SELECT name FROM sqlite_master").fetchall(), [])
            ambient._db(fake).close()
            saved = {"version": 1, "guildId": 42, "sourceBases": [art.art_source_basis(packet())],
                     "imagePrompt": "An imagined musical observatory"}
            meta = {"title": "The First Glimpse", "meaning": "A fictional discovery", "privateCreativeContinuity": saved}
            with sqlite3.connect(fake.DB_FILE) as conn:
                for day, status, guild, message in ((1, "discord_confirmed", 42, "1"),
                    (2, "draft_ready", 42, ""), (3, "discord_confirmed", 99, "3"),
                    (4, "withdrawn_before_delivery", 42, "4"), (5, "discord_confirmed", 42, "")):
                    conn.execute("INSERT INTO bnl_own_art_delivery(pacific_day,art_id,guild_id,status,discord_message_id,metadata_json) VALUES(?,?,?,?,?,?)",
                                 (str(day), str(day), guild, status, message, json.dumps(meta)))
            with mock.patch.object(art, "art_sources_current", return_value=True):
                history = art.public_creative_history(fake, 42)
            self.assertEqual([p["ref"] for p in history], ["art:1"])
            prompt, refs = art.build_own_art_brief(packet(), continuity=history)
            self.assertIn("art:1", refs)
            self.assertIn("creative_fiction", prompt)
            self.assertNotIn("sourceBases", prompt)
            with mock.patch.object(art, "art_sources_current", return_value=False):
                self.assertEqual(art.public_creative_history(fake, 42), [])

    def test_publication_reuse_change_withdraws_even_with_unchanged_community_sources(self):
        fake = SimpleNamespace(_build_publication_prompt_source_basis=mock.Mock(
            return_value=SimpleNamespace(expected_digest="original")))
        basis = {"publication": {"kind": "journal", "query": "latest journal", "digest": "original"}}
        self.assertTrue(art.art_sources_current(fake, 42, [basis]))
        fake._build_publication_prompt_source_basis.return_value = None
        self.assertFalse(art.art_sources_current(fake, 42, [basis]))

    def test_private_preview_is_standalone_and_never_enters_public_history(self):
        with tempfile.TemporaryDirectory() as folder:
            db = Path(folder) / "db"
            db.touch()
            fake = fixtures.OwnArtPreviewTests().fake_bot(str(db))
            value = packet()
            concept = {**CONCEPT, "inspirationRefs": ["fresh:29"]}
            fake._extract_text_and_tokens.return_value = (json.dumps(concept), 5)
            with mock.patch.object(art, "build_source_packet", return_value=value), \
                 mock.patch.object(art, "build_source_packet_between", return_value=value), \
                 mock.patch.object(art, "generate_private_image", return_value=(PNG, {"mimeType": "image/png", "sha256": "image-fixture"})):
                receipt = art.prepare_private_preview(fake, str(Path(folder) / "independent"), generate=True)
            self.assertEqual(receipt["status"], "private_draft_ready")
            self.assertFalse(receipt["published"])
            self.assertNotIn("study", receipt)
            self.assertEqual(art.public_creative_history(fake, 123), [])

    def test_art_has_existing_public_world_knowledge_without_restricted_lore(self):
        prompt, _ = art.build_own_art_brief({})
        for fact in ("BARCODE Radio", "Sheila", "Cliff", "Studio Rats", "BARCODE Vol. 0", "BARCODE Vol. 1"):
            self.assertIn(fact, prompt)
        self.assertNotIn("9 Bit", prompt)
        self.assertIn("claymation", prompt)
        self.assertIn("video-game", prompt)

    def test_private_art_reuses_public_ambient_memory_and_broadcast_readers(self):
        fake = fixtures.OwnArtPreviewTests().fake_bot("unused")
        def memories(guild_id, *, source_basis):
            source_basis["rows"] = {"memory_tiers": {1: "fixture"}}
            return "self_directed", "A remembered public music collaboration."
        fake.build_dynamic_curiosity_payload = memories
        fake.build_scoped_broadcast_memory_context = mock.Mock(return_value="A recorded BARCODE Radio show moment.")
        context = art.build_art_context(fake, 123, packet=packet())
        refs = {s["ref"] for s in context["sources"]}
        self.assertIn("ambient:memory_cues", refs)
        self.assertIn("ambient:broadcast_history", refs)
        self.assertTrue(fake.build_scoped_broadcast_memory_context.call_args.kwargs["public_only"])
        fake.revalidate_ambient_local_sources = mock.Mock(return_value=False)
        with mock.patch.object(art, "build_source_packet_between", return_value=packet()):
            self.assertFalse(art.art_context_current(fake, 123, context))

    def test_saved_ambient_roots_survive_json_and_use_existing_privacy_owner(self):
        fake = SimpleNamespace(revalidate_ambient_local_sources=mock.Mock(return_value=True))
        roots = [{"ambient": {"guild_id": 42, "rows": {"conversations": {"1": "hash"}},
                              "tier_sources": {"2": [1]}}}]
        self.assertTrue(art.art_sources_current(fake, 42, roots))
        observed = fake.revalidate_ambient_local_sources.call_args.args[1]
        self.assertEqual(observed["rows"]["conversations"], {1: "hash"})
        self.assertEqual(observed["tier_sources"], {2: (1,)})
        fake.revalidate_ambient_local_sources.return_value = False
        self.assertFalse(art.art_sources_current(fake, 42, roots))
