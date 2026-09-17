import json
import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

import bnl_journal as journal
import bnl_journal_automation as automation
from bnl_journal_source_store import record_source_event, timestamp_to_epoch_ms


class PublicJournalPeopleTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.db = self.temp.name + "/journal.db"
        journal.ensure_schema(self.db)
        with sqlite3.connect(self.db) as conn:
            conn.execute("""CREATE TABLE user_memory_facts (
                id INTEGER PRIMARY KEY, guild_id INTEGER, user_id INTEGER,
                fact_key TEXT, fact_value TEXT, lifecycle_status TEXT,
                source_directed INTEGER, source_kind TEXT, source_conversation_row_id INTEGER,
                source_channel_policy TEXT, source_control_ref TEXT, updated_at TEXT)""")
            conn.execute("CREATE TABLE user_profiles(guild_id INTEGER,user_id INTEGER,display_name TEXT,preferred_name TEXT)")

    def fact(self, user=10, name="Test Cadence", guild=1, directed=1, state="active"):
        with sqlite3.connect(self.db) as conn:
            conn.execute("INSERT INTO user_memory_facts VALUES(NULL,?,?,?,?,?,?,?,?,?,?,?)", (
                guild, user, "preferred_name", name, state, directed, "member_control", 0,
                "member_control", "discord_interaction:test", "2026-09-17T01:00:00Z"))

    def source(self, user=10, name="Test Composer", text="I made the chorus."):
        record_source_event(self.db, guild_id=1, source_kind="discord_message",
                            source_key=str(user), occurred_at_ms=timestamp_to_epoch_ms("2026-09-17T02:00:00Z"),
                            raw_text=text, sanitized_summary="someone made the chorus.",
                            subject_ref=f"discord_user:{user}", private_display_name=name,
                            channel_policy="public_home", public_usable=True)

    def packet(self):
        return journal.build_source_packet_between(self.db, 1, "2026-09-17T00:00:00Z", "2026-09-18T00:00:00Z", entry_kind="manual")

    def test_confirmed_name_is_bound_to_author_and_mentions_keep_other_person(self):
        self.fact()
        self.source(text="Test Listener tested my chorus; <@20> suggested a quieter ending.")
        self.source(user=20, name="Test Listener", text="I only tested the demo, I did not compose it.")
        packet = self.packet()
        sources = packet["safeSources"]
        self.assertEqual({"Test Cadence", "Test Listener"}, {s["publicSpeakerName"] for s in sources})
        composer = next(s for s in sources if s["publicSpeakerName"] == "Test Cadence")
        self.assertEqual(2, composer["summary"].count("Test Listener"))
        self.assertEqual(2, len({s["participantAlias"] for s in sources}))
        prompt = journal.build_generation_prompt(packet)
        self.assertIn("not automatically the person described", prompt)
        self.assertNotIn("discord_user:", prompt)
        self.assertNotIn("<@", prompt)
        self.assertNotIn("rawSummary", json.dumps(packet))

    def test_profile_guess_other_guild_and_inactive_fact_cannot_choose_nickname(self):
        self.source()
        with sqlite3.connect(self.db) as conn:
            conn.execute("INSERT INTO user_profiles VALUES(1,10,'Test Composer','Test Guess')")
        self.fact(guild=2, name="Test Other Guild")
        self.fact(name="Test Inferred", directed=0)
        self.fact(name="Test Retired", state="forgotten")
        packet = self.packet()
        self.assertEqual("Test Composer", packet["safeSources"][0]["publicSpeakerName"])
        prompt = journal.build_generation_prompt(packet)
        for value in ("Test Guess", "Test Other Guild", "Test Inferred", "Test Retired"):
            self.assertNotIn(value, prompt)

    def test_identical_names_never_merge_authors(self):
        self.source(name="Test Echo", text="Test Echo wrote the hook.")
        self.source(user=20, name="Test Echo", text="I only tested the hook.")
        packet = self.packet()
        self.assertEqual([], packet["privatePublicPeople"])
        self.assertEqual(2, len({s["participantAlias"] for s in packet["safeSources"]}))
        self.assertNotIn("Test Echo", journal.build_generation_prompt(packet))

    def test_owner_public_canon_wins_over_profile_and_chosen_name(self):
        self.source(name="Test Account Label", text="Test Account Label shared a chorus.")
        self.fact(name="Test Alternate Label")
        with patch.dict(os.environ, {"BNL_OWNER_USER_ID": "10"}):
            packet = self.packet()
        prompt = journal.build_generation_prompt(packet)
        self.assertEqual("6 Bit", packet["safeSources"][0]["publicSpeakerName"])
        self.assertNotIn("Test Account Label", prompt)
        self.assertNotIn("Test Alternate Label", prompt)

    def test_changed_nickname_invalidates_frozen_and_prepared_identity_basis(self):
        self.fact()
        self.source()
        packet = self.packet()
        with sqlite3.connect(self.db) as conn:
            self.assertEqual("", automation._frozen_packet_invalidation_reason(conn, 1, packet))
            conn.execute("UPDATE user_memory_facts SET lifecycle_status='forgotten'")
            self.assertEqual("privacy_memory_ineligible", automation._frozen_packet_invalidation_reason(conn, 1, packet))
            self.assertEqual("privacy_memory_ineligible", automation._prepared_invalidation_reason(
                conn, 1, {"publicPeople": packet["privatePublicPeople"]}, set()))

    def test_legacy_frozen_packet_does_not_gain_name_permission_from_current_facts(self):
        self.fact()
        packet = {"safeSources": [], "privateSources": [{"displayName": "Test Composer"}]}
        prompt = journal.build_generation_prompt(packet)
        self.assertIn("Describe anonymous humans", prompt)
        self.assertNotIn("Test Cadence", prompt)


if __name__ == "__main__":
    unittest.main()
