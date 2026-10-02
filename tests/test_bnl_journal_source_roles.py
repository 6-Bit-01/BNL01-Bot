"""Retain public lore while distinguishing remembered events and BNL speech."""
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

import bnl_journal as journal
import bnl_journal_source_store as sources


class JournalSourceRoleTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.db = str(Path(directory.name) / "journal.db")
        journal.ensure_schema(self.db)
        sources.ensure_schema(self.db)
        self.message = "An audio link prompted questions about its silver antenna sound."
        self.invitation = "Check its creator and origin before drawing conclusions."
        self.original = self.message + " " + self.invitation
        with sqlite3.connect(self.db) as conn:
            conn.execute("CREATE TABLE website_relay_history("
                         "relay_id TEXT PRIMARY KEY,guild_id INTEGER,public_message TEXT,"
                         "public_directive TEXT,event_type TEXT,published_timestamp TEXT,source_basis_json TEXT)")
            conn.execute("INSERT INTO website_relay_history VALUES(?,?,?,?,?,?,?)", (
                "relay-1", 1, self.message, self.invitation, "fresh_public_discord_activity",
                "2026-10-01T16:00:00Z", "[]",
            ))
        sources.record_source_event(
            self.db, guild_id=1, source_kind="website_relay", source_key="relay-1",
            occurred_at_ms=sources.timestamp_to_epoch_ms("2026-10-01T16:00:00Z"),
            raw_text=self.original, sanitized_summary=self.original,
            channel_policy="public_relay", subject_ref="bnl_01", private_display_name="BNL",
            public_usable=True, metadata={"event_type": "fresh_public_discord_activity"},
        )
        with sqlite3.connect(self.db) as conn:
            conn.execute("UPDATE bnl_journal_source_archive_state SET activated_at_ms=0 WHERE guild_id=1")

    def packet(self):
        return journal.build_source_packet_between(
            self.db, 1, "2026-10-01T00:00:00Z", "2026-10-02T00:00:00Z",
            entry_kind="daily", prepare_schema=False,
        )

    def relay_source(self, packet, ref_id=None):
        self.assertFalse(any(source.get("sourceKind") == "relay" for source in packet["safeSources"]))
        return next(source for source in packet["reflectionBasis"]
                    if source.get("basisKind") == "accepted_relay_continuity"
                    and (ref_id is None or source["refId"] == ref_id))

    def test_archive_path_preserves_text_and_lineage_but_separates_speech_roles(self):
        before = hashlib.sha256(Path(self.db).read_bytes()).hexdigest()
        packet = self.packet()
        self.assertTrue(packet["sourceArchiveAvailable"])
        source = self.relay_source(packet)
        self.assertEqual(self.original, source["summary"])
        self.assertTrue(source["refId"].startswith("reflection:event:"))
        lineage = next(item for item in packet["privateReflectionBasisProvenance"]["historicalSourceEvents"]
                       if item["refId"] == source["refId"])
        self.assertEqual("relay-1", lineage["sourceKey"])
        self.assertEqual(f"fresh:{lineage['eventSeq']}", lineage["originalRefId"])
        speech = source["relaySpeech"]
        self.assertEqual(self.message, speech["publicMessage"])
        self.assertEqual(self.invitation, speech["publicInvitation"])
        self.assertEqual("BNL", speech["speaker"])
        self.assertEqual("speech_and_interpretation_not_independent_corroboration", speech["authority"])
        self.assertEqual(before, hashlib.sha256(Path(self.db).read_bytes()).hexdigest())
        self.assertIn("a requested check is not a completed check", journal.build_generation_prompt(packet))

    def test_legacy_reader_uses_same_role_projection_without_backfilling_archive(self):
        with sqlite3.connect(self.db) as conn:
            relays = journal.accepted_relays(conn, 1, "2026-10-01T00:00:00Z", "2026-10-02T00:00:00Z")
        packet = journal.build_packet_from_sources(
            self.db, 1, "2026-10-01T00:00:00Z", "2026-10-02T00:00:00Z", relays, [], prepare_schema=False,
        )
        source = self.relay_source(packet)
        self.assertEqual(relays[0]["summary"], source["summary"])
        self.assertEqual(self.invitation, source["relaySpeech"]["publicInvitation"])

    def test_changed_or_missing_owner_cannot_replace_archived_evidence(self):
        for replacement in ("Different public message with an invented completed check.", None):
            with self.subTest(replacement=replacement), sqlite3.connect(self.db) as conn:
                if replacement is None:
                    conn.execute("DELETE FROM website_relay_history WHERE relay_id='relay-1'")
                else:
                    conn.execute("UPDATE website_relay_history SET public_message=? WHERE relay_id='relay-1'", (replacement,))
            packet = self.packet()
            source = self.relay_source(packet)
            self.assertEqual(self.original, source["summary"])
            self.assertEqual("unavailable_in_archive", source["relaySpeech"]["partition"])
            self.assertNotIn("publicInvitation", source["relaySpeech"])
            self.assertNotIn("invented completed", json.dumps(source))

    def test_role_projection_cannot_append_unarchived_suffix_or_other_guild_speech(self):
        source = {"refId": "fresh:1", "sourceKind": "relay", "relayId": "relay-1", "summary": self.message[:32]}
        with sqlite3.connect(self.db) as conn:
            parts = journal._relay_speech_parts(conn, 1, [source])["fresh:1"]
            isolated = journal._relay_speech_parts(conn, 2, [source])["fresh:1"]
        self.assertEqual(source["summary"].strip(), parts["publicMessage"])
        self.assertEqual("", parts["publicInvitation"])
        self.assertEqual("unavailable_in_archive", isolated["partition"])

    def test_relay_speech_parts_use_existing_private_identity_projection(self):
        message = "Private Name asked about the silver antenna sound."
        original = message + " " + self.invitation
        with sqlite3.connect(self.db) as conn:
            conn.execute("CREATE TABLE user_profiles(guild_id INTEGER,display_name TEXT)")
            conn.execute("INSERT INTO user_profiles VALUES(1,'Private Name')")
            conn.execute("INSERT INTO website_relay_history VALUES(?,?,?,?,?,?,?)", (
                "relay-2", 1, message, self.invitation, "fresh_public_discord_activity",
                "2026-10-01T16:00:00Z", "[]",
            ))
        recorded = sources.record_source_event(
            self.db, guild_id=1, source_kind="website_relay", source_key="relay-2",
            occurred_at_ms=sources.timestamp_to_epoch_ms("2026-10-01T16:00:00Z"),
            raw_text=original, sanitized_summary=original,
            channel_policy="public_relay", subject_ref="bnl_01", private_display_name="BNL",
            public_usable=True, metadata={"event_type": "fresh_public_discord_activity"},
        )
        self.assertTrue(recorded.ok, recorded.reason)
        self.assertEqual("inserted", recorded.status)
        source = self.relay_source(self.packet(), f"reflection:event:{recorded.event_seq}")
        self.assertNotIn("Private Name", json.dumps(source))
        self.assertIn("someone", source["relaySpeech"]["publicMessage"])

    def test_retrospective_relay_keeps_speech_roles_without_changing_original_version(self):
        with sqlite3.connect(self.db) as conn:
            relays = journal.accepted_relays(conn, 1, "2026-10-01T00:00:00Z", "2026-10-02T00:00:00Z")
            relays[0]["eventType"] = "public_moment"
            original, _ = journal._relay_reflection_basis(conn, 1, relays[0])
        packet = journal.build_packet_from_sources(
            self.db, 1, "2026-10-01T00:00:00Z", "2026-10-02T00:00:00Z", relays, [], prepare_schema=False,
        )
        basis = self.relay_source(packet)
        self.assertEqual(original["sourceVersion"], basis["sourceVersion"])
        self.assertEqual(original["summary"], basis["summary"])
        self.assertEqual(self.invitation, basis["relaySpeech"]["publicInvitation"])

    def test_old_lore_keeps_dates_and_does_not_gain_current_authority_from_keyword_match(self):
        with sqlite3.connect(self.db) as conn:
            conn.execute("CREATE TABLE broadcast_memory(id INTEGER PRIMARY KEY,guild_id INTEGER,episode_date TEXT,"
                         "cleaned_summary TEXT,status TEXT,public_safe INTEGER,usage_scope TEXT,created_at TEXT)")
            conn.execute("INSERT INTO broadcast_memory VALUES(?,?,?,?,?,?,?,?)", (
                1, 1, "2026-05-29", "The silver antenna dossier describes a Network anomaly of unknown origin.",
                "active", 1, "ambient", "2026-05-30T10:00:00Z",
            ))
        # The current human contribution supplies the present association;
        # BNL's Relay retelling cannot create fresh evidence by keyword overlap.
        contribution = sources.record_source_event(
            self.db, guild_id=1, source_kind="discord_message", source_key="message-1",
            occurred_at_ms=sources.timestamp_to_epoch_ms("2026-10-01T15:00:00Z"),
            raw_text="This silver antenna sound sends my mind back to older transmissions.",
            channel_id=42, channel_policy="public_home", subject_ref="discord_user:10",
            private_display_name="Test Listener", public_usable=True,
        )
        self.assertTrue(contribution.ok, contribution.reason)
        self.assertEqual("inserted", contribution.status)
        packet = self.packet()
        lane = packet["generationContextLanes"]["establishedBroadcastMemory"][0]
        self.assertEqual("2026-05-29", lane["episodeDate"])
        self.assertEqual("2026-05-30T10:00:00Z", lane["recordedAt"])
        self.assertEqual("remembered_history_not_current_activity", lane["temporalScope"])
        self.assertEqual("topic_similarity_only", lane["matchAuthority"])
        self.assertEqual([f"fresh:{contribution.event_seq}"], lane["matchedFreshSourceRefIds"])
        self.relay_source(packet)
        self.assertIn("dossier", lane["summary"])
        self.assertIn("naturally locate them in remembered history", journal.build_generation_prompt(packet))

    def test_public_dossier_word_is_not_private_authority_or_identifier_permission(self):
        article = {"title": "An Old Dossier Returns to Mind", "excerpt": "A remembered Network curiosity.",
                   "sections": [{"heading": "An association", "body": "The public dossier is an old story that still amuses me."}]}
        packet = {"safeSources": [], "privateSources": [], "privateWindowDisplayNames": ["Private Name"]}
        self.assertEqual("", journal._article_privacy_reason(article, packet))
        for content, reason in (
            ("The private member dossier says more.", "public_leak_pattern"),
            ("A sealed dossier supplies the answer.", "public_leak_pattern"),
            ("An admin-only relationship dossier explains it.", "public_leak_pattern"),
            ("The memory:123 record supplies the answer.", "source_ref_leak"),
            ("The dossier belongs to <@123456789012345678>.", "public_leak_pattern"),
            ("The dossier belongs to Private Name.", "community_name_leak"),
            ("The private_metadata confirms this.", "public_leak_pattern"),
        ):
            with self.subTest(content=content):
                article["sections"][0]["body"] = content
                self.assertEqual(reason, journal._article_privacy_reason(article, packet))


if __name__ == "__main__":
    unittest.main()
