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
import bnl_entity_evidence as evidence
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
        c.execute("CREATE TABLE community_presence (guild_id INTEGER, subject_key TEXT, display_name TEXT, first_seen_at TEXT, last_seen_at TEXT, mention_count INTEGER, direct_interaction_count INTEGER, operator_mention_count INTEGER, connection_notes TEXT, evidence_snippets TEXT)")
        self.conn.commit()

    def _summary(self, subject="Crow"):
        return entity.build_entity_activity_summary(self.db, subject, guild_id=1)


    def test_entity_evidence_schema_created_safely(self):
        evidence.ensure_entity_evidence_schema(self.conn)
        cols = {row[1] for row in self.conn.execute("PRAGMA table_info(entity_evidence_events)")}

        self.assertIn("safe_summary", cols)
        self.assertIn("raw_ref_json", cols)
        self.assertIn("public_safe_candidate", cols)

    def test_derives_structured_evidence_from_existing_sources_idempotently(self):
        c = self.conn.cursor()
        c.execute("INSERT INTO user_profiles VALUES (42,1,'Crow','Crow',NULL,NULL)")
        c.execute("INSERT INTO conversations VALUES (1,42,'Crow',1,'finished-tracks','public_home','user','Can BNL explain dossier source-file behavior for my new track and radio show context?','2026-06-01')")
        c.execute("INSERT INTO conversations VALUES (2,7,'Other',1,'barcode-bot','public_context','user','Crow was mentioned as a possible source-file candidate in community discussion.','2026-06-02')")
        c.execute("INSERT INTO conversations VALUES (3,7,'Other',1,'research-and-development','internal_controlled','user','Crow appears in internal private notes with 123456789012345678.','2026-06-03')")
        c.execute("INSERT INTO relationship_state VALUES (42,1,8,0.4,'known','steady','Crow discussed music support requests','now')")
        c.execute("INSERT INTO relationship_journal VALUES (4,42,1,'note','Crow has review-only relationship context around help requests.','now')")
        c.execute("INSERT INTO memory_tiers VALUES (5,42,1,'long','Crow source blind memory trace with EDGE_SESSION help_signal text.',0.9,1,'now')")
        c.execute("INSERT INTO broadcast_memory VALUES (6,1,'2026-06-01','Crow appears in a public-safe broadcast music context.','show_note',1,'public','active')")
        c.execute("INSERT INTO broadcast_memory VALUES (7,1,'2026-06-02','Crow appears in inactive broadcast context.','show_note',0,'internal','draft')")
        c.execute("INSERT INTO community_presence VALUES (1,'crow','Crow','now','now',3,1,0,'Crow has community presence notes.','Crow appeared in approved public chat.')")
        self.conn.commit()

        first = evidence.derive_entity_evidence_for_subject(self.db, "Crow", guild_id=1)
        second = evidence.derive_entity_evidence_for_subject(self.db, "Crow", guild_id=1)
        rows = list(self.conn.execute("SELECT * FROM entity_evidence_events ORDER BY evidence_kind, source_row_id"))
        kinds = {row[16] for row in rows}

        self.assertGreaterEqual(first["createdCount"], 9)
        self.assertEqual(second["createdCount"], 0)
        self.assertIn("profile_match", kinds)
        self.assertIn("authored_public_conversation", kinds)
        self.assertIn("mentioned_public_conversation", kinds)
        self.assertIn("mentioned_review_only_conversation", kinds)
        self.assertIn("relationship_context_review_only", kinds)
        self.assertIn("source_blind_memory_review_only", kinds)
        self.assertIn("broadcast_memory_signal", kinds)
        self.assertIn("community_presence_signal", kinds)
        self.assertTrue(any(row[18] == 1 for row in rows))
        self.assertTrue(any(row[19] == 1 for row in rows))
        normal_safe_text = " ".join(row[17] or "" for row in rows)
        self.assertNotIn("research-and-development", normal_safe_text)
        self.assertNotIn("123456789012345678", normal_safe_text)
        raw_refs = [row[0] or "" for row in self.conn.execute("SELECT raw_ref_json FROM entity_evidence_events")]
        self.assertTrue(any("research-and-development" in raw_ref for raw_ref in raw_refs))

    def test_entity_activity_summary_prefers_structured_evidence_events(self):
        evidence.ensure_entity_evidence_schema(self.conn)
        evidence.upsert_entity_evidence_event(
            self.conn, guild_id=1, subject_name="Crow", source_type="conversation", source_table="conversations",
            source_row_id="99", source_label="conversations/public_discord_observed", channel_name="finished-tracks",
            channel_policy="public_home", visibility="public_side", authority="channel_policy_observed", confidence=0.7,
            relation_to_subject="authored", topic="source-file/dossier planning context", evidence_kind="authored_public_conversation",
            safe_summary="Subject authored approved public-side conversation about dossier/source-file behavior.",
            public_safe_candidate=True, review_only=False, music_signal=True, community_signal=True, bnl_interaction=True,
            dossier_relevance="candidate_after_owner_review", raw_ref_json={"table": "conversations", "row_id": 99, "content": "raw private-ish transcript 123456789012345678"},
            observed_at="now",
        )
        self.conn.execute("INSERT INTO conversations VALUES (100,7,'Other',1,'general','public_home','user','Crow loose fallback text should not appear when structured evidence exists.','now')")
        self.conn.commit()

        summary = self._summary()
        normal = dict(summary)
        normal.pop("rawProvenance")

        self.assertTrue(any("approved public-side conversation" in item for item in summary["conversationHighlights"]))
        self.assertNotIn("loose fallback", json.dumps(normal))
        self.assertIn("conversations/public_discord_observed", summary["rawProvenance"]["sourceLabels"])
        self.assertTrue(any(fragment.get("rawRefJson") for fragment in summary["rawProvenance"]["rawFragments"]))
        self.assertIn(entity.QUEUE_NOT_CONNECTED_NOTE, summary["notPublicYet"])

    def test_ranked_evidence_prioritizes_public_conversation_over_bulk_source_blind(self):
        evidence.ensure_entity_evidence_schema(self.conn)
        for idx in range(12):
            evidence.upsert_entity_evidence_event(
                self.conn, guild_id=1, subject_name="Crow", source_type="source_blind_memory", source_table="memory_tiers",
                source_row_id=f"legacy-{idx}", source_label="memory_tiers/source_blind_memory_trace", channel_policy="source_blind",
                visibility="review_only", authority="source_blind_legacy_memory", confidence=0.35,
                relation_to_subject="source_blind_memory", topic="local context", evidence_kind="source_blind_memory_review_only",
                safe_summary="Source-blind legacy memory exists and requires review.",
                public_safe_candidate=False, review_only=True, raw_ref_json={"snippet": f"legacy raw {idx}"}, observed_at=f"2026-06-03T00:{idx:02d}:00",
            )
        evidence.upsert_entity_evidence_event(
            self.conn, guild_id=1, subject_name="Crow", source_type="conversation", source_table="conversations",
            source_row_id="older-public", source_label="conversations/public_discord_observed", channel_name="barcode-bot",
            channel_policy="public_context", visibility="public_side", authority="channel_policy_observed", confidence=0.72,
            relation_to_subject="authored", topic="source-file/dossier planning context", evidence_kind="authored_public_conversation",
            safe_summary="Subject authored approved public-side conversation about BNL/source-file/dossier handling.",
            public_safe_candidate=True, review_only=False, bnl_interaction=True, raw_ref_json={"content": "raw transcript 123456789012345678"}, observed_at="2026-06-01T00:00:00",
        )
        self.conn.commit()

        ranked = evidence.get_ranked_entity_evidence_for_subject(self.conn, "Crow", guild_id=1, limit=3)

        self.assertEqual(ranked[0]["evidence_kind"], "authored_public_conversation")
        self.assertEqual(ranked[0]["source_row_id"], "older-public")

    def test_source_blind_is_capped_deduped_and_best_review_uses_safe_summaries(self):
        evidence.ensure_entity_evidence_schema(self.conn)
        evidence.upsert_entity_evidence_event(
            self.conn, guild_id=1, subject_name="Crow", source_type="conversation", source_table="conversations",
            source_row_id="public-1", source_label="conversations/public_discord_observed", channel_name="barcode-bot",
            channel_policy="public_context", visibility="public_side", authority="channel_policy_observed", confidence=0.8,
            relation_to_subject="mentioned", topic="source-file/dossier planning context", evidence_kind="mentioned_public_conversation",
            safe_summary="Subject was mentioned in approved public-side conversation as a possible source-file candidate.",
            public_safe_candidate=True, review_only=False, community_signal=True, raw_ref_json={"content": "VERY RAW TRANSCRIPT 123456789012345678"}, observed_at="2026-06-02",
        )
        for idx in range(6):
            evidence.upsert_entity_evidence_event(
                self.conn, guild_id=1, subject_name="Crow", source_type="source_blind_memory", source_table="memory_tiers",
                source_row_id=f"blind-{idx}", source_label="memory_tiers/source_blind_memory_trace", channel_policy="source_blind",
                visibility="review_only", authority="source_blind_legacy_memory", confidence=0.35,
                relation_to_subject="source_blind_memory", topic="local context", evidence_kind="source_blind_memory_review_only",
                safe_summary="Source-blind legacy memory exists and requires review.",
                public_safe_candidate=False, review_only=True, raw_ref_json={"snippet": f"VERY RAW MEMORY {idx} 123456789012345678"}, observed_at=f"2026-06-03-{idx}",
            )
        self.conn.commit()

        summary = self._summary()
        formatted = entity.format_entity_activity_summary_response(summary)
        normal_text = json.dumps({k: v for k, v in summary.items() if k != "rawProvenance"}) + formatted

        self.assertLessEqual(sum(1 for item in summary["reviewOnlyEvidence"] if "Source-blind" in item), 2)
        self.assertIn("Best evidence to review:", formatted)
        self.assertTrue(any("possible source-file candidate" in item for item in summary["bestEvidenceToReview"]))
        self.assertNotIn("VERY RAW TRANSCRIPT", normal_text)
        self.assertNotIn("123456789012345678", normal_text)
        self.assertIn("Public-safe candidate events are review candidates, not confirmed public facts", formatted)
        self.assertNotRegex(formatted, r"\b6 public-safe candidates\b|\b6 usable dossier facts\b")
        raw_text = json.dumps(summary["rawProvenance"])
        self.assertIn("rawRefJson", raw_text)

    def test_normal_readout_excludes_private_channel_names_and_queue_stays_unconnected(self):
        c = self.conn.cursor()
        c.execute("INSERT INTO conversations VALUES (NULL,7,'User',1,'research-and-development','internal_controlled','user','Crow private mod note with 123456789012345678 raw transcript.','now')")
        c.execute("INSERT INTO conversations VALUES (NULL,7,'User',1,'barcode-bot','public_context','user','Crow was mentioned in BARCODE community source-file discussion.','now')")
        self.conn.commit()

        summary = self._summary()
        formatted = entity.format_entity_activity_summary_response(summary)

        self.assertNotIn("research-and-development", formatted)
        self.assertNotIn("123456789012345678", formatted)
        self.assertIn("#barcode-bot", formatted)
        self.assertEqual(summary["queueSubmissionStatus"], "not_connected")
        self.assertIn(entity.QUEUE_NOT_CONNECTED_NOTE, formatted)
        for forbidden in ("websiteIngest", "dossierCreated", "published", "payment", "artistAccount", "ambient", "publicDiscordPost"):
            self.assertNotIn(forbidden, json.dumps(summary))

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
        self.assertTrue(any("Subject authored" in item and "BNL source-file and dossier" in item for item in summary["conversationHighlights"]))
        self.assertTrue(any("Subject was mentioned" in item for item in summary["conversationHighlights"]))
        self.assertTrue(any("music" in item.lower() for item in summary["topicBreakdown"]))
        self.assertTrue(any("source-file and dossier" in item.lower() for item in summary["topicBreakdown"]))
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


    def test_detail_extraction_frequency_recency_and_safe_paraphrases(self):
        c = self.conn.cursor()
        c.execute("INSERT INTO user_profiles VALUES (42,1,'Crow','Crow',NULL,NULL)")
        c.execute("INSERT INTO conversations VALUES (NULL,42,'Crow',1,'finished-tracks','public_home','user','Crow shared a finished track demo and asked for listens. RAW TRACK TITLE SHOULD NOT LEAK','2026-06-01T10:00:00')")
        c.execute("INSERT INTO conversations VALUES (NULL,42,'Crow',1,'barcode-bot','public_context','user','Crow asked how BNL source files and dossier review work. RAW SOURCE QUESTION SHOULD NOT LEAK','2026-06-02T10:00:00')")
        c.execute("INSERT INTO conversations VALUES (NULL,42,'Crow',1,'barcode-bot','public_context','user','Crow needs help because the bot command has an issue. RAW HELP TEXT SHOULD NOT LEAK','2026-06-03T10:00:00')")
        c.execute("INSERT INTO conversations VALUES (NULL,7,'Other',1,'general-chat','public_home','user','Crow joined the BARCODE community chat and people welcomed them. RAW WELCOME SHOULD NOT LEAK','2026-06-04T10:00:00')")
        c.execute("INSERT INTO conversations VALUES (NULL,7,'Other',1,'research-and-development','internal_controlled','user','Crow private internal operator review note in research-and-development.','2026-06-05T10:00:00')")
        c.execute("INSERT INTO memory_tiers VALUES (NULL,42,1,'long','Crow source-blind memory says EDGE_SESSION private raw memory should not dominate.',0.9,1,'2026-06-06T10:00:00')")
        self.conn.commit()

        summary = self._summary()
        normal = dict(summary)
        normal.pop("rawProvenance")
        normal_text = json.dumps(normal)

        self.assertEqual(summary["activityFrequencySummary"]["approvedPublicAuthoredRows"], 3)
        self.assertEqual(summary["activityFrequencySummary"]["approvedPublicMentionedRows"], 1)
        self.assertEqual(summary["activityFrequencySummary"]["reviewOnlyEvidenceCount"], 1)
        self.assertIn("2026-06-05T10:00:00", summary["recentActivitySummary"])
        self.assertTrue(any(item.get("channel") == "#barcode-bot" and item.get("count") == 2 for item in summary["topChannels"]))
        topic_text = json.dumps(summary["topTopicDetails"] + summary["topicBreakdown"])
        self.assertIn("music and track-sharing classification", topic_text)
        self.assertIn("BNL source-file and dossier classification", topic_text)
        self.assertIn("help and support classification", topic_text)
        self.assertIn("community and server participation classification", topic_text)
        self.assertNotIn("Music/track-sharing", topic_text)
        self.assertNotIn("BNL/source-file/dossier", topic_text)
        self.assertIn("#finished-tracks", json.dumps(summary["representativeEvidence"]))
        self.assertIn("#barcode-bot", json.dumps(summary["representativeEvidence"]))
        self.assertNotIn("research-and-development", normal_text)
        self.assertNotIn("RAW TRACK TITLE SHOULD NOT LEAK", normal_text)
        self.assertNotIn("RAW SOURCE QUESTION SHOULD NOT LEAK", normal_text)
        self.assertNotIn("EDGE_SESSION", normal_text)
        self.assertEqual(summary["queueSubmissionStatus"], "not_connected")
        self.assertNotRegex(normal_text, r"submitted\s+\d+|\d+\s+songs|Priority|payment")

    def test_website_safe_topic_labels_cover_slash_taxonomy(self):
        cases = {
            "BNL/source-file/dossier discussion": "BNL source-file and dossier classification",
            "Music/track-sharing discussion": "music and track-sharing classification",
            "WIP/demo discussion": "WIP and demo classification",
            "Community/server participation": "community and server participation classification",
            "Event/riddle/engagement activity": "event, riddle, or engagement classification",
            "Help/support requests": "help and support classification",
        }
        for raw, safe in cases.items():
            with self.subTest(raw=raw):
                self.assertEqual(evidence._website_safe_topic_label(raw), safe)
                self.assertNotIn("/", safe)

    def test_representative_evidence_line_is_website_safe_and_not_subject_claim(self):
        line = evidence._representative_evidence_line({
            "publicSafe": True,
            "relation": "authored",
            "topic": "BNL/source-file/dossier discussion",
            "channel": "#barcode-bot",
        })
        self.assertNotIn("/", line)
        self.assertNotIn("authored", line.lower())
        self.assertIn("BNL classified this approved public-context item", line)
        self.assertIn("review before treating it as a subject claim", line)


    def test_review_safe_fact_extraction_surfaces_named_topics_tools_and_bnl_patterns(self):
        self.conn.execute("INSERT INTO user_profiles VALUES (42,1,'Crow','Crow',NULL,NULL)")
        rows = [
            (42, 'Crow', 1, 'barcode-bot', 'public_home', 'user', 'Crow asks BNL for help reviewing Orion and mentions Suno song ideas without confirmed queue data.', '2026-06-01'),
            (42, 'Crow', 1, 'barcode-bot', 'public_home', 'user', 'Crow follows up with BNL about Orion support and Suno track language in community context.', '2026-06-02'),
        ]
        self.conn.executemany("INSERT INTO conversations (user_id,user_name,guild_id,channel_name,channel_policy,role,content,timestamp) VALUES (?,?,?,?,?,?,?,?)", rows)
        self.conn.commit()

        summary = self._summary()
        text = json.dumps({k: v for k, v in summary.items() if k != "rawProvenance"})

        self.assertIn("Recurring named topic: Orion", text)
        self.assertIn("Tool/platform mention: Suno", text)
        self.assertIn("BNL interaction pattern: Crow appears in repeated approved public-context exchanges involving BNL.", text)
        self.assertIn("Possible music/submission-related language appears in reviewed evidence, but queue/submission identity is not connected yet.", text)
        self.assertEqual(summary["queueSubmissionStatus"], "not_connected")
        self.assertIn("Queue/submission history is not connected yet; source-file memory cannot confirm submitted songs, source type, play status, or priority history.", summary["missingInfo"])
        self.assertNotRegex(text, r"submitted\s+\d+|played\s+\d+|Priority|payment|submitted songs from Suno")

    def test_review_only_named_topics_stay_in_review_only_evidence(self):
        self.conn.execute("INSERT INTO user_profiles VALUES (42,1,'Crow','Crow',NULL,NULL)")
        self.conn.execute("INSERT INTO relationship_journal VALUES (NULL,42,1,'note','Crow has internal relationship context repeatedly mentioning Orion and private planning notes.','now')")
        self.conn.commit()

        summary = self._summary()
        public_text = json.dumps({key: summary.get(key) for key in ("conversationHighlights", "topicBreakdown", "evidenceDetails", "bestEvidenceToReview", "publicUseCandidates")})
        review_text = json.dumps(summary["reviewOnlyEvidence"])

        self.assertNotIn("Review-only recurring topic: Orion", public_text)
        self.assertIn("Review-only recurring topic: Orion appears in internal context connected to Crow.", review_text)
        self.assertIn("Review-only relationship/context evidence exists and must stay internal.", review_text)

    def test_normal_fact_fields_do_not_expose_raw_ids_or_private_channels(self):
        self.conn.execute("INSERT INTO user_profiles VALUES (42,1,'Crow','Crow',NULL,NULL)")
        self.conn.execute("INSERT INTO conversations VALUES (NULL,42,'Crow',1,'research-and-development','private_internal','user','Crow internal note about Orion with 123456789012345678 and private transcript.','now')")
        self.conn.commit()

        summary = self._summary()
        normal_text = json.dumps({k: v for k, v in summary.items() if k != "rawProvenance"})

        self.assertNotIn("research-and-development", normal_text)
        self.assertNotIn("123456789012345678", normal_text)
        self.assertIn("Review-only recurring topic: Orion", normal_text)

    def test_no_queue_submission_counts_are_invented(self):
        self.conn.execute("INSERT INTO user_profiles VALUES (42,1,'Crow','Crow',NULL,NULL)")
        self.conn.commit()

        summary = self._summary()
        formatted = entity.format_entity_activity_summary_response(summary)

        self.assertIn(entity.QUEUE_NOT_CONNECTED_NOTE, summary["notPublicYet"])
        self.assertIn(entity.QUEUE_NOT_CONNECTED_NOTE, formatted)
        self.assertNotRegex(json.dumps(summary), r"submitted\s+\d+|\d+\s+songs")


    def test_titlecase_sentence_starters_and_fillers_are_not_named_topics(self):
        filler_rows = []
        fillers = ["The", "For", "And", "There", "This", "With", "From", "Good", "Maybe"]
        for idx in range(6):
            filler_rows.append({
                "text": " ".join(f"{word} sentence starter repeats as grammar." for word in fillers),
                "publicSafe": True,
                "source": "conversations",
            })
        intel = entity.extract_recurring_subject_intelligence(filler_rows, "Crow")
        extracted = set((intel["publicSubjects"] + intel["reviewOnlySubjects"]).keys())

        for word in fillers:
            self.assertNotIn(word, extracted)
        self.assertFalse(any(word in extracted for word in ("The", "For", "And", "Maybe")))

    def test_explicit_one_word_names_multiword_names_and_code_markers_survive_filtering(self):
        rows = [
            {"text": "Orion through Crow notes Atlas Bloom project marker 0x9A for BNL.", "publicSafe": True, "source": "conversations"},
            {"text": "Again Orion through Crow repeats Atlas Bloom project marker 0x9A with BNL.", "publicSafe": True, "source": "conversations"},
        ]
        intel = entity.extract_recurring_subject_intelligence(rows, "Crow")
        extracted = set(intel["publicSubjects"].keys())

        self.assertIn("Orion", extracted)
        self.assertIn("Atlas Bloom", extracted)
        self.assertIn("0x9A", extracted)
        reasons = intel["acceptedSubjectReasons"]
        self.assertEqual(reasons["Orion"], "explicit_through_pattern")
        self.assertEqual(reasons["0x9A"], "code_marker")

    def test_full_conversation_content_powers_recurring_intelligence_not_short_snippet(self):
        self.conn.execute("INSERT INTO user_profiles VALUES (42,1,'Crow','Crow',NULL,NULL)")
        self.conn.execute("INSERT INTO conversations VALUES (NULL,42,'Crow',1,'general','public_home','user',?, '2026-06-01')", (
            "Crow short opener. Deep in the full row Orion through Crow discusses BNL presence threshold behavior, liaison interface node language, sync convergence 0x9A, operational boundaries, and https://suno.com/song/alpha.",
        ))
        self.conn.execute("INSERT INTO conversations VALUES (NULL,42,'Crow',1,'general','public_home','user',?, '2026-06-02')", (
            "Crow second opener. Again Orion through Crow discusses BNL presence threshold behavior, liaison interface node language, sync convergence 0x9A, operational boundaries, and https://suno.com/song/beta.",
        ))
        self.conn.commit()

        summary = self._summary()
        normal_text = json.dumps({k: v for k, v in summary.items() if k != "rawProvenance"})

        self.assertIn("Recurring named topic: Orion appears in reviewed evidence connected to Crow", normal_text)
        for key in ("topicBreakdown", "conversationHighlights", "bestEvidenceToReview"):
            self.assertTrue(any("Recurring named topic: Orion" in item for item in summary[key]), key)
        self.assertTrue(any("Recurring named topic: Orion" in item for item in summary["knownContext"] + summary["usefulEvidence"]))
        self.assertIn("Recurring conversation pattern", normal_text)
        self.assertIn("Conversation theme: Crow repeatedly discusses BNL presence, threshold behavior, liaison/interface/node language, sync/convergence markers, operational boundaries", normal_text)
        self.assertIn("Activity pattern: Crow repeatedly relays messages framed as", normal_text)
        self.assertIn("Orion through Crow", normal_text)
        self.assertIn("Evidence digest: 2 Crow-linked evidence items were scanned", normal_text)
        self.assertIn("BNL interaction pattern", normal_text)
        self.assertIn("Tool/platform mention: Suno appears in reviewed evidence connected to Crow", normal_text)
        self.assertIn("Queue/submission history is not connected yet", normal_text)
        self.assertEqual(summary["queueSubmissionStatus"], "not_connected")
        self.assertGreaterEqual(summary["subjectIntelligenceDiagnostics"]["rowsScanned"], 2)


    def test_subject_intelligence_priority_survives_generic_field_caps(self):
        self.conn.execute("INSERT INTO user_profiles VALUES (42,1,'Crow','Crow',NULL,NULL)")
        for idx in range(12):
            self.conn.execute("INSERT INTO conversations VALUES (NULL,42,'Crow',1,'general','public_home','user',?, ?)", (
                f"Crow generic source-file and BARCODE context filler {idx} before the recurring intelligence.",
                f"2026-05-{idx + 1:02d}",
            ))
        self.conn.execute("INSERT INTO conversations VALUES (NULL,42,'Crow',1,'general','public_home','user','Orion through Crow asks BNL about Suno links: https://suno.com/song/cap-a', '2026-06-01')")
        self.conn.execute("INSERT INTO conversations VALUES (NULL,42,'Crow',1,'general','public_home','user','Orion through Crow repeats BNL Suno link context: https://suno.com/song/cap-b', '2026-06-02')")
        self.conn.commit()

        summary = self._summary()

        self.assertTrue(any("Recurring named topic: Orion" in item for item in summary["conversationHighlights"][:6]))
        self.assertTrue(any("Recurring named topic: Orion" in item for item in summary["topicBreakdown"][:8]))
        self.assertTrue(any("Recurring named topic: Orion" in item for item in summary["bestEvidenceToReview"][:6]))
        self.assertTrue(any("Tool/platform mention: Suno" in item for item in summary["musicSignals"][:5]))

    def test_raw_ref_json_conversation_pointer_rehydrates_full_row_for_intelligence(self):
        self.conn.execute("INSERT INTO user_profiles VALUES (42,1,'Crow','Crow',NULL,NULL)")
        self.conn.execute("INSERT INTO conversations VALUES (576,42,'Crow',1,'general','public_home','user',?, '2026-06-01')", (
            "Crow says tiny snippet is not enough. Orion through Crow repeats the full-row-only Nebula Forge project with BNL liaison interface language and https://suno.com/song/rehydrated.",
        ))
        evidence.ensure_entity_evidence_schema(self.conn)
        evidence.upsert_entity_evidence_event(
            self.conn, guild_id=1, subject_name="Crow", source_type="conversation", source_table="conversations",
            source_row_id="576", source_label="conversations/public_discord_observed", channel_name="general",
            channel_policy="public_home", visibility="public_side", authority="channel_policy_observed", confidence=0.7,
            relation_to_subject="authored", topic="community context", evidence_kind="authored_public_conversation",
            safe_summary="Subject authored approved public-side conversation. Possible Reviewed Evidence Tool Platform Source Context.",
            public_safe_candidate=True, review_only=False, music_signal=False, community_signal=True, bnl_interaction=True,
            dossier_relevance="candidate_after_owner_review", raw_ref_json={"table": "conversations", "row_id": 576, "snippet": "Orion here"},
            observed_at="now",
        )
        self.conn.commit()

        summary = self._summary()
        normal_text = json.dumps({k: v for k, v in summary.items() if k != "rawProvenance"})

        self.assertIn("Orion", normal_text)
        self.assertIn("Nebula Forge", normal_text)
        for garbage in ("Possible appears", "Reviewed appears", "Evidence appears", "Tool appears", "Platform appears", "Source appears", "Context appears"):
            self.assertNotIn(garbage, normal_text)
        self.assertNotIn("raw_ref_json", normal_text)
        self.assertNotIn("source_row_id", normal_text)
        self.assertNotIn("576", normal_text)

    def test_generic_non_orion_recurring_subjects_without_hardcoding(self):
        self.conn.execute("INSERT INTO user_profiles VALUES (88,1,'Mira','Mira',NULL,NULL)")
        self.conn.execute("INSERT INTO conversations VALUES (NULL,88,'Mira',1,'general','public_home','user','Mira says Atlas Bloom through Mira is the project name for BNL review and public context.', '2026-06-01')")
        self.conn.execute("INSERT INTO conversations VALUES (NULL,88,'Mira',1,'general','public_home','user','Mira repeats Atlas Bloom through Mira with interface language for BNL review and public context.', '2026-06-02')")
        self.conn.commit()

        summary = self._summary("Mira")
        normal_text = json.dumps({k: v for k, v in summary.items() if k != "rawProvenance"})

        self.assertIn("Atlas Bloom", normal_text)
        self.assertIn("Recurring named topic: Atlas Bloom appears in reviewed evidence connected to Mira", normal_text)
        self.assertNotIn("Orion appears", normal_text)

    def test_review_only_and_mixed_subject_intelligence_split(self):
        self.conn.execute("INSERT INTO user_profiles VALUES (42,1,'Crow','Crow',NULL,NULL)")
        self.conn.execute("INSERT INTO relationship_journal VALUES (NULL,42,1,'note','Crow internal note repeats Vega Signal through Crow in relationship memory.', 'now')")
        self.conn.execute("INSERT INTO memory_tiers VALUES (NULL,42,1,'long','Crow source-blind note repeats Vega Signal through Crow in memory.',0.9,1,'now')")
        self.conn.commit()

        review_only_summary = self._summary()
        public_text = json.dumps({key: review_only_summary.get(key) for key in ("conversationHighlights", "topicBreakdown", "bestEvidenceToReview", "publicUseCandidates")})
        review_text = json.dumps(review_only_summary["reviewOnlyEvidence"])
        self.assertNotIn("Recurring named topic: Vega Signal", public_text)
        self.assertIn("Review-only recurring subject: Vega Signal", review_text)

        self.conn.execute("INSERT INTO conversations VALUES (NULL,42,'Crow',1,'general','public_home','user','Crow public note repeats Vega Signal through Crow with BNL interaction.', '2026-06-03')")
        self.conn.execute("INSERT INTO conversations VALUES (NULL,42,'Crow',1,'general','public_home','user','Crow public note again repeats Vega Signal through Crow with BNL interaction.', '2026-06-04')")
        self.conn.commit()

        mixed_summary = self._summary()
        mixed_text = json.dumps({k: v for k, v in mixed_summary.items() if k != "rawProvenance"})
        self.assertIn("Recurring named topic: Vega Signal appears in reviewed evidence connected to Crow", mixed_text)
        self.assertIn("Additional review-only context also exists", mixed_text)

    def test_normal_payload_fields_do_not_expose_full_transcripts_private_names_or_internal_provenance(self):
        self.conn.execute("INSERT INTO user_profiles VALUES (42,1,'Crow','Crow',NULL,NULL)")
        secret = "PRIVATE_CHANNEL_ALPHA raw transcript 123456789012345678 raw_ref_json conversations rowid should not leak"
        self.conn.execute("INSERT INTO conversations VALUES (NULL,42,'Crow',1,'PRIVATE_CHANNEL_ALPHA','private_internal','user',?, 'now')", (f"Crow internal Orion through Crow note. {secret}",))
        self.conn.commit()

        summary = self._summary()
        normal_text = json.dumps({k: v for k, v in summary.items() if k not in {"rawProvenance", "subjectIntelligenceDiagnostics"}})

        self.assertNotIn("PRIVATE_CHANNEL_ALPHA", normal_text)
        self.assertNotIn("123456789012345678", normal_text)
        self.assertNotIn("raw_ref_json", normal_text)
        self.assertNotIn("raw transcript", normal_text)
        self.assertNotIn("conversations rowid", normal_text)
        self.assertNotIn("source_table", normal_text)


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
        matched, options, error = entity.parse_entity_activity_summary_command("!bnl entity evidence refresh Crow")
        self.assertTrue(matched)
        self.assertFalse(error)
        self.assertTrue(options["refreshEvidence"])

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
             mock.patch.object(bnl01_bot, "refresh_entity_evidence_for_subject") as refresher, \
             mock.patch.object(bnl01_bot, "send_dossier_recommendation") as sender:
            handled = asyncio.run(bnl01_bot.maybe_handle_entity_activity_summary_command(allowed, allowed.content))
        self.assertTrue(handled)
        builder.assert_called_once()
        sender.assert_not_called()
        refresher.assert_not_called()
        self.assertIn("BNL Entity Activity Summary", allowed.replies[0])
        self.assertEqual(summary, "build_entity_activity_summary")

    def test_evidence_refresh_command_is_gated_internal_and_does_not_publish(self):
        denied_public = FakeMessage("!bnl entity evidence refresh Crow", channel_name="general", author_id=99, owner_id=99)
        with mock.patch.object(bnl01_bot, "BNL_OWNER_USER_ID", 99), \
             mock.patch.object(bnl01_bot, "refresh_entity_evidence_for_subject") as refresher:
            handled = asyncio.run(bnl01_bot.maybe_handle_entity_activity_summary_command(denied_public, denied_public.content))
        self.assertTrue(handled)
        self.assertIn("restricted", denied_public.replies[0])
        refresher.assert_not_called()

        allowed = FakeMessage("!bnl entity evidence refresh Crow", channel_name="research-and-development", author_id=99, owner_id=99)
        with mock.patch.object(bnl01_bot, "BNL_OWNER_USER_ID", 99), \
             mock.patch.object(bnl01_bot, "refresh_entity_evidence_for_subject", return_value={"subjectName": "Crow", "createdCount": 1, "updatedCount": 0, "unchangedCount": 0, "sourceTypes": {"profile": 1}, "reviewOnlyCount": 1, "publicSafeCandidateCount": 0}) as refresher, \
             mock.patch.object(bnl01_bot, "build_entity_activity_summary", return_value={"subjectName": "Crow", "rawProvenance": {"rawFragments": []}, "missingInfo": [], "notPublicYet": [entity.QUEUE_NOT_CONNECTED_NOTE]}) as builder, \
             mock.patch.object(bnl01_bot, "send_dossier_recommendation") as sender:
            handled = asyncio.run(bnl01_bot.maybe_handle_entity_activity_summary_command(allowed, allowed.content))
        self.assertTrue(handled)
        refresher.assert_called_once()
        builder.assert_called_once()
        sender.assert_not_called()
        self.assertIn("BNL Entity Evidence Refresh", allowed.replies[0])
        self.assertIn("source types covered", allowed.replies[0])


if __name__ == "__main__":
    unittest.main()
