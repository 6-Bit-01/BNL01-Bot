import gc
import hashlib
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

import bnl_journal as journal
import bnl_journal_source_store as store


START = "2026-09-30T01:30:00Z"
END = "2026-10-01T01:30:00Z"


class JournalSourceContextTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.cleanup_database)
        self.db = str(Path(self.temp.name) / "journal.db")
        journal.ensure_schema(self.db)
        store.ensure_schema(self.db)

    def cleanup_database(self):
        # Existing schema owners use sqlite context managers; collect their
        # released connections before Windows removes the fixture directory.
        gc.collect()
        self.temp.cleanup()

    def record(self, key, text, observed="2026-09-30T16:25:45Z", **overrides):
        values = {
            "guild_id": 1, "source_kind": "discord_message", "source_key": str(key),
            "occurred_at_ms": store.timestamp_to_epoch_ms(observed),
            "raw_text": text, "sanitized_summary": text, "channel_id": 123456789012345678,
            "channel_policy": "public_home", "subject_ref": "discord_user:7",
            "private_display_name": "Test Host", "public_usable": True,
            "metadata": {"messageId": str(key)},
        }
        values.update(overrides)
        result = store.record_source_event(self.db, **values)
        self.assertTrue(result.ok, result)
        return f"fresh:{result.event_seq}"

    def packet(self):
        return journal.build_source_packet_between(self.db, 1, START, END, prepare_schema=False)

    def test_original_room_time_and_address_survive_real_archive_projection(self):
        addressed = self.record("101", "Is your simulated fussiness satisfied?", metadata={
            "directedToBnl": True, "channelName": "private-looking-room-name",
            "roomRef": "forged-room", "replyToMessageId": "unverified-target",
        })
        release = self.record("102", "Two newly finished works, one folk and one futuristic.",
                              "2026-09-30T16:28:48Z", channel_id=223456789012345678,
                              subject_ref="discord_user:8", private_display_name="Test Artist")
        sticker = self.record("103", "I got a sticker for it.", "2026-09-30T16:29:03Z",
                              subject_ref="discord_user:8", private_display_name="Test Artist")
        before = Path(self.db).read_bytes()
        packet = self.packet()
        self.assertEqual(before, Path(self.db).read_bytes(), "read-only projection must not migrate or save")
        sources = {source["refId"]: source for source in packet["safeSources"]}
        first, music, last = [sources[ref] for ref in (addressed, release, sticker)]
        self.assertTrue(first["directedToBnl"])
        self.assertNotIn("directedToBnl", music)
        self.assertNotIn("directedToBnl", last)
        self.assertEqual(first["roomRef"], last["roomRef"])
        self.assertNotEqual(first["roomRef"], music["roomRef"])
        self.assertEqual("2026-09-30T16:25:45Z", first["observedAt"])
        self.assertEqual("2026-09-30T09:25:45-07:00", first["observedAtPacific"])
        self.assertEqual("America/Los_Angeles", first["observedTimeZone"])
        self.assertEqual("original_contribution", first["sourceRole"])
        safe_text = json.dumps(packet["safeSources"])
        for hidden in ("123456789012345678", "223456789012345678", "discord_user:",
                       "private-looking-room-name", "forged-room", "replyToMessageId", "unverified-target"):
            self.assertNotIn(hidden, safe_text)

    def test_room_context_is_scoped_by_surface_guild_and_actual_room(self):
        def room(guild, surface, identifier):
            return journal._captured_conversation_context(guild, surface, identifier)["roomRef"]
        self.assertEqual(room(1, "discord", 100), room(1, "discord", "100"))
        rooms = {room(1, "discord", 100), room(1, "discord", 101),
                 room(2, "discord", 100), room(1, "tiktok_live_chat", "100")}
        self.assertEqual(4, len(rooms))
        for ref in rooms:
            self.assertRegex(ref, r"^room:[0-9a-f]{24}$")

    def test_tiktok_room_comes_from_captured_room_not_discord_or_claimed_room(self):
        ref = self.record("201", "A public live-chat contribution.", source_kind="tiktok_live_chat",
                          subject_ref="tiktok_user:test", private_display_name="Test Viewer",
                          metadata={"roomId": "live-session-a", "roomRef": "forged", "directedToBnl": True})
        source = next(source for source in self.packet()["safeSources"] if source["refId"] == ref)
        expected = journal._captured_conversation_context(1, "tiktok_live_chat", "live-session-a")["roomRef"]
        self.assertEqual(expected, source["roomRef"])
        self.assertNotIn("directedToBnl", source)
        self.assertNotIn("live-session-a", json.dumps(source))

    def test_absent_room_and_non_boolean_address_are_unknown(self):
        for missing in (None, "", 0, "0", True, {}, "not-a-discord-room"):
            self.assertNotIn("roomRef", journal._captured_conversation_context(1, "discord", missing))
        for value in (None, "true", "false", 1, 0, [], {}):
            self.assertNotIn("directedToBnl", journal._captured_conversation_context(
                1, "discord", 100, {"directedToBnl": value}))
        for value in (True, False):
            context = journal._captured_conversation_context(1, "discord", None, {"directedToBnl": value})
            source = journal._source_for_prompt({"sourceKind": "conversation", "conversationSurface": "discord", **context})
            self.assertIs(value, source["directedToBnl"])

    def test_pacific_time_respects_dst_and_leaves_unknown_time_unknown(self):
        for observed, expected in (("2026-09-30T16:25:45Z", "2026-09-30T09:25:45-07:00"),
                                   ("2026-01-20T17:25:45Z", "2026-01-20T09:25:45-08:00"),
                                   ("2026-10-01T01:15:00Z", "2026-09-30T18:15:00-07:00")):
            source = journal._source_for_prompt({"sourceKind": "conversation", "observedAt": observed,
                                                 "observedAtPacific": "forged", "observedTimeZone": "wrong"})
            self.assertEqual(observed, source["observedAt"])
            self.assertEqual(expected, source["observedAtPacific"])
        for observed in (None, "", "2026-09-30", "2026-09-30T99:99:99Z", "unknown"):
            source = journal._source_for_prompt({"sourceKind": "conversation", "observedAt": observed})
            self.assertNotIn("observedAtPacific", source)
            self.assertNotIn("observedTimeZone", source)

    def test_roles_are_owner_derived_and_relay_cannot_claim_original_authority(self):
        for kind, role in (("conversation", "original_contribution"), ("finalized_show", "recorded_event"),
                           ("relay", "bnl_interpretation")):
            source = journal._source_for_prompt({"sourceKind": kind, "sourceRole": "made_up_authority"})
            self.assertEqual(role, source["sourceRole"])
        relay_ref = self.record("relay-1", "Evaluate the new sound design.", source_kind="website_relay",
                                channel_id=None, channel_policy="public_relay", subject_ref="",
                                private_display_name="", metadata={"eventType": "accepted"})
        relay = next(source for source in self.packet()["safeSources"] if source["refId"] == relay_ref)
        self.assertEqual("bnl_interpretation", relay["sourceRole"])
        self.assertNotIn("roomRef", relay)
        self.assertNotIn("directedToBnl", relay)

    def test_retrospective_relay_keeps_interpretation_role(self):
        with sqlite3.connect(self.db) as conn:
            basis, _ = journal._relay_reflection_basis(conn, 1, {
                "refId": "fresh:12", "relayId": "prior-relay", "sourceKind": "relay",
                "summary": "A prior interpretation.", "observedAt": "2026-09-30T16:00:00Z",
            })
        self.assertEqual("bnl_interpretation", basis["sourceRole"])
        self.assertEqual("2026-09-30T16:00:00Z", basis["relayPublishedAt"])

    def test_historical_originals_retain_context_without_becoming_fresh(self):
        ref = self.record("history-1", "Did that satisfy your imaginary sorting circuit?",
                          observed="2026-09-29T16:25:45Z", metadata={"directedToBnl": True})
        basis, _ = journal._historical_source_reflection_basis(self.db, 1, START, END)
        item = next(item for item in basis if item["refId"] == ref.replace("fresh:", "reflection:event:"))
        self.assertEqual("public_source_history", item["basisKind"])
        self.assertEqual("original_contribution", item["sourceRole"])
        self.assertEqual("2026-09-29T16:25:45Z", item["sourceObservedAt"])
        self.assertEqual("2026-09-29T09:25:45-07:00", item["observedAtPacific"])
        self.assertTrue(item["directedToBnl"])
        self.assertRegex(item["roomRef"], r"^room:[0-9a-f]{24}$")
        self.assertNotIn("discord_user:", json.dumps(basis))

    def test_safe_projection_drops_raw_and_unverified_context(self):
        source = journal._source_for_prompt({
            "refId": "fresh:1", "sourceKind": "conversation", "summary": "A public remark.",
            "roomRef": "123456789012345678", "channel_id": 123456789012345678,
            "subjectRef": "discord_user:123", "displayName": "Private Label", "rawSummary": "private",
            "metadata": {"replyToRefId": "fresh:9"}, "replyToRefId": "fresh:9",
            "sourceRole": "invented", "observedAtPacific": "forged",
        })
        self.assertEqual({"refId": "fresh:1", "sourceKind": "conversation", "summary": "A public remark.",
                          "sourceRole": "original_contribution"}, source)

    def test_context_changes_are_bound_by_existing_frozen_packet_hash(self):
        def frozen_hash(room, directed):
            source = journal._source_for_prompt({"refId": "fresh:1", "sourceKind": "conversation",
                "conversationSurface": "discord", "summary": "A remark.", "observedAt": "2026-09-30T16:25:45Z",
                **journal._captured_conversation_context(1, "discord", room, {"directedToBnl": directed})})
            raw = json.dumps({"safeSources": [source]}, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            return hashlib.sha256(raw.encode("utf-8")).hexdigest()
        self.assertEqual(3, len({frozen_hash(100, True), frozen_hash(101, True), frozen_hash(100, False)}))

    def test_room_identifier_cannot_escape_into_public_article(self):
        room = journal._captured_conversation_context(1, "discord", 100)["roomRef"]
        article = {"title": "A community day", "excerpt": "A new tune.",
                   "sections": [{"heading": "Music", "body": "We gathered in " + room}], "metadata": {}}
        self.assertEqual("public_leak_pattern", journal._article_privacy_reason(article, {}, []))

    def test_history_preserves_source_window_separately_from_later_publication(self):
        for index, day in enumerate((27, 28), 1):
            entry_id = f"journal-test-{index}"
            published = f"2026-09-{day + 1}T02:00:00Z"
            with sqlite3.connect(self.db) as conn:
                conn.execute("""INSERT INTO bnl_journal_entries
                    (entry_id,revision,guild_id,lifecycle_state,title,excerpt,sections_json,content_hash,
                     source_window_start,source_window_end,authored_at,published_at,created_at,updated_at)
                    VALUES(?,1,1,'published','Test music','A public song.','[]','hash',?,?,?,?,?,?)""",
                    (entry_id, f"2026-09-{day}T01:30:00Z", f"2026-09-{day + 1}T01:30:00Z",
                     published, published, published, published))
                conn.execute("""INSERT INTO bnl_journal_private_metadata VALUES(?,1,1,?,'hash','published',?,?)""",
                             (entry_id, json.dumps({"topicTags": ["music"]}), published, published))
        history = journal.retrieve_history(self.db, 1, {"candidateTopicTags": ["music"]}, prepare_schema=False)
        compact = journal._bounded_history_for_prompt(history)
        latest = compact["previousEntry"]
        self.assertEqual("2026-09-29T02:00:00Z", latest["publishedAt"])
        self.assertEqual("2026-09-28T01:30:00Z", latest["sourceWindowStart"])
        self.assertEqual("2026-09-29T01:30:00Z", latest["sourceWindowEnd"])
        self.assertEqual("2026-09-27T01:30:00Z", compact["relevantOlderEntries"][0]["sourceWindowStart"])
        legacy = journal._bounded_history_for_prompt({"previousEntry": {"entry_id": "old", "published_at": "2026-01-01"}})
        self.assertIsNone(legacy["previousEntry"]["sourceWindowStart"])
        self.assertIsNone(legacy["previousEntry"]["sourceWindowEnd"])

    def test_legacy_reader_adds_only_available_room_and_preserves_public_filters(self):
        with sqlite3.connect(self.db) as conn:
            conn.execute("""CREATE TABLE conversations(id INTEGER PRIMARY KEY,user_id INTEGER,user_name TEXT,
                guild_id INTEGER,channel_name TEXT,channel_policy TEXT,role TEXT,content TEXT,timestamp TEXT,
                public_usable INTEGER,visibility TEXT,channel_id INTEGER)""")
            rows = [
                (1, 7, "Test Host", 1, "lobby", "public_home", "user", "Public remark.", "2026-09-30T16:25:45Z", 1, "public", 100),
                (2, 7, "Test Host", 1, "private", "internal_controlled", "user", "Private remark.", "2026-09-30T16:26:00Z", 1, "public", 200),
                (3, 7, "Test Host", 1, "lobby", "public_home", "user", "Withdrawn remark.", "2026-09-30T16:26:00Z", 0, "public", 100),
                (4, 7, "Test Host", 2, "lobby", "public_home", "user", "Other guild.", "2026-09-30T16:26:00Z", 1, "public", 100),
                (5, 7, "Test Host", 1, "lobby", "public_home", "model", "Model output.", "2026-09-30T16:26:00Z", 1, "public", 100),
                (6, 7, "Test Host", 1, "lobby", "public_home", "user", "Private visibility.", "2026-09-30T16:26:00Z", 1, "private", 100),
            ]
            conn.executemany("INSERT INTO conversations VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", rows)
            sources = journal.public_conversations(conn, 1, START, END)
            self.assertEqual(1, len(sources))
            self.assertIn("roomRef", journal._source_for_prompt(sources[0]))
            self.assertNotIn("directedToBnl", sources[0])
            conn.execute("ALTER TABLE conversations RENAME TO conversations_with_room")
            conn.execute("""CREATE TABLE conversations AS
                SELECT id,user_id,user_name,guild_id,channel_name,channel_policy,role,content,
                       timestamp,public_usable,visibility FROM conversations_with_room""")
            conn.execute("DROP TABLE conversations_with_room")
            legacy = journal.public_conversations(conn, 1, START, END)
            self.assertEqual(1, len(legacy))
            self.assertNotIn("roomRef", legacy[0])


if __name__ == "__main__":
    unittest.main()
