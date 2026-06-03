import asyncio
import json
import os
import sqlite3
import tempfile
import unittest
from types import SimpleNamespace
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl_entity_activity_summary as entity
import bnl01_bot


class EntityActivitySummaryBuilderTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False)
        self.tmp.close()
        self.db = self.tmp.name
        self.conn = sqlite3.connect(self.db)
        self._schema()

    def tearDown(self):
        self.conn.close()
        os.unlink(self.db)

    def _schema(self):
        c = self.conn.cursor()
        c.execute("CREATE TABLE user_profiles (user_id INTEGER, guild_id INTEGER, display_name TEXT, preferred_name TEXT, last_seen TEXT, last_greeting_at TEXT)")
        c.execute("CREATE TABLE user_memory_facts (id INTEGER PRIMARY KEY, user_id INTEGER, guild_id INTEGER, fact_key TEXT, fact_value TEXT, confidence REAL, is_core INTEGER, updated_at TEXT)")
        c.execute("CREATE TABLE user_habits (user_id INTEGER, guild_id INTEGER, total_messages INTEGER, question_messages INTEGER, humor_messages INTEGER, late_night_messages INTEGER, avg_length REAL, last_topic TEXT, updated_at TEXT)")
        c.execute("CREATE TABLE relationship_state (user_id INTEGER, guild_id INTEGER, interaction_count INTEGER, affinity_score REAL, trust_stage TEXT, social_stance TEXT, last_topic TEXT, updated_at TEXT)")
        c.execute("CREATE TABLE relationship_journal (id INTEGER PRIMARY KEY, user_id INTEGER, guild_id INTEGER, entry_type TEXT, summary TEXT, timestamp TEXT)")
        c.execute("CREATE TABLE conversations (id INTEGER PRIMARY KEY, user_id INTEGER, user_name TEXT, guild_id INTEGER, channel_name TEXT, channel_policy TEXT, role TEXT, content TEXT, timestamp TEXT)")
        c.execute("CREATE TABLE memory_tiers (id INTEGER PRIMARY KEY, user_id INTEGER, guild_id INTEGER, tier TEXT, summary TEXT, salience REAL, mentions INTEGER, updated_at TEXT)")
        c.execute("CREATE TABLE broadcast_memory (id INTEGER PRIMARY KEY, guild_id INTEGER, episode_date TEXT, cleaned_summary TEXT, entry_type TEXT, public_safe INTEGER, usage_scope TEXT, status TEXT)")
        self.conn.commit()

    def _summary(self, subject="Crow"):
        return entity.build_entity_activity_summary(self.db, subject, guild_id=1)

    def test_entity_summary_finds_local_profile_match(self):
        self.conn.execute("INSERT INTO user_profiles VALUES (42,1,'Crow','Crow',NULL,NULL)")
        self.conn.commit()

        summary = self._summary()

        self.assertEqual(summary["subjectName"], "Crow")
        self.assertEqual(summary["subjectKey"], "crow")
        self.assertIn("Crow", summary["matchedNames"])
        self.assertEqual(summary["identityConfidence"], "medium")
        self.assertTrue(any("Local profile match" in item for item in summary["knownContext"]))

    def test_relationship_rows_become_review_only_not_public_facts(self):
        c = self.conn.cursor()
        c.execute("INSERT INTO user_profiles VALUES (42,1,'Crow','Crow',NULL,NULL)")
        c.execute("INSERT INTO relationship_state VALUES (42,1,8,0.4,'known','steady','Crow discussed music support requests','now')")
        c.execute("INSERT INTO relationship_journal VALUES (NULL,42,1,'note','Crow has review-only relationship context around help requests.','now')")
        self.conn.commit()

        summary = self._summary()

        self.assertTrue(summary["relationshipSignals"])
        self.assertTrue(summary["privateOnlyNotes"])
        self.assertEqual(summary["publicSafePossibilities"], ["No public-safe facts confirmed yet."])
        self.assertNotIn("relationship", " ".join(summary["publicSafePossibilities"]).lower())

    def test_public_safe_conversation_requires_public_channel_policy(self):
        self.conn.execute("INSERT INTO conversations VALUES (NULL,7,'User',1,'general','public_home','user','Crow appears in music community context.','now')")
        self.conn.commit()

        summary = self._summary()

        self.assertTrue(any("Public-side conversation context exists" in item for item in summary["publicSafePossibilities"]))
        self.assertTrue(any("public-side" in item for item in summary["channelActivity"]))

    def test_public_conversation_rows_produce_concrete_channels_highlights_and_topics(self):
        c = self.conn.cursor()
        c.execute("INSERT INTO user_profiles VALUES (42,1,'Crow','Crow',NULL,NULL)")
        c.execute("INSERT INTO conversations VALUES (NULL,42,'Crow',1,'finished-tracks','public_home','user','Can BNL explain dossier source-file behavior for my new track and radio show context?','2026-06-01')")
        c.execute("INSERT INTO conversations VALUES (NULL,7,'Other',1,'barcode-bot','public_context','user','Crow was mentioned as a possible source-file candidate in community discussion.','2026-06-02')")
        self.conn.commit()

        summary = self._summary()

        self.assertTrue(any("#finished-tracks" in item and "authored" in item for item in summary["observedChannels"]))
        self.assertTrue(any("#barcode-bot" in item and "mentioned" in item for item in summary["observedChannels"]))
        self.assertTrue(any("Subject authored" in item and "dossier/source-file" in item for item in summary["conversationHighlights"]))
        self.assertTrue(any("Subject was mentioned" in item for item in summary["conversationHighlights"]))
        self.assertTrue(any("music" in item.lower() for item in summary["topicBreakdown"]))
        self.assertTrue(any("source-file/dossier" in item.lower() for item in summary["topicBreakdown"]))
        self.assertTrue(any("music" in item.lower() for item in summary["musicSignals"]))
        self.assertTrue(any("community" in item.lower() for item in summary["communitySignals"]))
        self.assertTrue(any("BNL-related" in item for item in summary["bnlInteractionSignals"]))

    def test_internal_rows_keep_highlights_review_only_without_private_channel_names(self):
        self.conn.execute("INSERT INTO conversations VALUES (NULL,7,'User',1,'research-and-development','internal_controlled','user','Crow asked about a private source file review.','now')")
        self.conn.commit()

        summary = self._summary()

        self.assertTrue(any("review-only" in item for item in summary["observedChannels"]))
        self.assertFalse(any("research-and-development" in item for item in summary["observedChannels"]))
        self.assertTrue(any("review-only" in item for item in summary["conversationHighlights"]))
        self.assertTrue(summary["reviewOnlyEvidence"])

    def test_unknown_sealed_internal_and_source_blind_remain_review_only(self):
        c = self.conn.cursor()
        c.execute("INSERT INTO conversations VALUES (NULL,7,'User',1,'mystery',NULL,'user','Crow appears in unknown legacy chatter.','now')")
        c.execute("INSERT INTO conversations VALUES (NULL,7,'User',1,'bnl-testing','sealed_test','user','Crow appears in sealed test context.','now')")
        c.execute("INSERT INTO conversations VALUES (NULL,7,'User',1,'research-and-development','internal_controlled','user','Crow appears in internal notes.','now')")
        c.execute("INSERT INTO memory_tiers VALUES (NULL,7,1,'long','Crow appears in source-blind legacy memory.',0.9,1,'now')")
        self.conn.commit()

        summary = self._summary()

        self.assertEqual(summary["publicSafePossibilities"], ["No public-safe facts confirmed yet."])
        self.assertTrue(any("Non-public" in item for item in summary["privateOnlyNotes"]))
        self.assertIn(entity.SOURCE_BLIND_REVIEW_NOTE, summary["privateOnlyNotes"])
        self.assertTrue(any("Source-blind memory cannot" in item for item in summary["notPublicYet"]))

    def test_raw_labels_stay_out_of_normal_fields_but_remain_in_raw_provenance(self):
        c = self.conn.cursor()
        c.execute("INSERT INTO user_profiles VALUES (42,1,'Crow','Crow',NULL,NULL)")
        c.execute("INSERT INTO conversations VALUES (NULL,42,'User',1,'general','public_home','user','Crow is tied to music context and help requests.','now')")
        c.execute("INSERT INTO memory_tiers VALUES (NULL,42,1,'long','Crow source blind memory trace with EDGE_SESSION help_signal text.',0.9,1,'now')")
        self.conn.commit()

        summary = self._summary()
        normal = dict(summary)
        normal.pop("rawProvenance")
        normal_text = json.dumps(normal)

        for banned in (
            "user_profiles/local_profile_observed",
            "conversations/public_discord_observed",
            "memory_tiers/source_blind_memory_trace",
            "EDGE_SESSION",
            "help_signal",
        ):
            self.assertNotIn(banned, normal_text)
        raw = summary["rawProvenance"]
        self.assertIn("user_profiles/local_profile_observed", raw["sourceLabels"])
        self.assertIn("conversations/public_discord_observed", raw["sourceLabels"])
        self.assertIn("memory_tiers/source_blind_memory_trace", raw["sourceLabels"])
        self.assertEqual(raw["sourceCounts"]["user_profiles"], 1)
        self.assertTrue(any(fragment.get("snippet") for fragment in raw["rawFragments"]))

    def test_no_queue_submission_counts_are_invented(self):
        self.conn.execute("INSERT INTO user_profiles VALUES (42,1,'Crow','Crow',NULL,NULL)")
        self.conn.commit()

        summary = self._summary()
        formatted = entity.format_entity_activity_summary_response(summary)

        self.assertIn(entity.QUEUE_NOT_CONNECTED_NOTE, summary["notPublicYet"])
        self.assertIn(entity.QUEUE_NOT_CONNECTED_NOTE, formatted)
        self.assertNotRegex(json.dumps(summary), r"submitted\s+\d+|\d+\s+songs")


class FakeAuthor:
    def __init__(self, user_id=99):
        self.id = user_id
        self.display_name = "Operator"
        self.bot = False


class FakeGuild:
    def __init__(self, owner_id=99):
        self.id = 1
        self.owner_id = owner_id

    def get_member(self, user_id):
        return None


class FakeChannel:
    def __init__(self, name):
        self.name = name
        self.id = 123
        self.guild = SimpleNamespace(id=1)
        self.parent = None


class FakeMessage:
    def __init__(self, content, channel_name="research-and-development", author_id=99, owner_id=99):
        self.content = content
        self.author = FakeAuthor(author_id)
        self.guild = FakeGuild(owner_id)
        self.channel = FakeChannel(channel_name)
        self.replies = []

    async def reply(self, text):
        self.replies.append(text)


class EntityActivitySummaryCommandTests(unittest.TestCase):
    def test_parse_entity_summary_command(self):
        matched, options, error = entity.parse_entity_activity_summary_command("!bnl entity summary Crow")
        self.assertTrue(matched)
        self.assertFalse(error)
        self.assertEqual(options["subjectName"], "Crow")
        self.assertTrue(entity.parse_entity_activity_summary_command("!bnl source summary Crow")[0])

    def test_command_is_owner_gated_and_uses_internal_channel_only(self):
        denied = FakeMessage("!bnl entity summary Crow", author_id=100, owner_id=99)
        handled = asyncio.run(bnl01_bot.maybe_handle_entity_activity_summary_command(denied, denied.content))
        self.assertTrue(handled)
        self.assertIn("operator-only", denied.replies[0])

        public = FakeMessage("!bnl entity summary Crow", channel_name="general", author_id=99, owner_id=99)
        with mock.patch.object(bnl01_bot, "BNL_OWNER_USER_ID", 99):
            handled = asyncio.run(bnl01_bot.maybe_handle_entity_activity_summary_command(public, public.content))
        self.assertTrue(handled)
        self.assertIn("restricted", public.replies[0])

    def test_command_does_not_create_recommendations_dossiers_or_website_ingest(self):
        allowed = FakeMessage("!bnl entity summary Crow", channel_name="bnl-testing", author_id=99, owner_id=99)
        summary = entity.build_entity_activity_summary.__name__
        with mock.patch.object(bnl01_bot, "BNL_OWNER_USER_ID", 99), \
             mock.patch.object(bnl01_bot, "build_entity_activity_summary", return_value={"subjectName": "Crow", "rawProvenance": {"rawFragments": []}, "missingInfo": []}) as builder, \
             mock.patch.object(bnl01_bot, "send_dossier_recommendation") as sender:
            handled = asyncio.run(bnl01_bot.maybe_handle_entity_activity_summary_command(allowed, allowed.content))
        self.assertTrue(handled)
        builder.assert_called_once()
        sender.assert_not_called()
        self.assertIn("BNL Entity Activity Summary", allowed.replies[0])
        self.assertEqual(summary, "build_entity_activity_summary")


if __name__ == "__main__":
    unittest.main()
