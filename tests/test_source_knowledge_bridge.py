import json
import os
import sqlite3
import tempfile
import unittest

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl_source_knowledge_bridge as bridge


class SourceKnowledgeBridgeTests(unittest.TestCase):
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
        c.execute("CREATE TABLE member_activity_events (id INTEGER PRIMARY KEY, guild_id INTEGER, member_name TEXT, raw_excerpt TEXT)")
        self.conn.commit()

    def _collect(self):
        return bridge.collect_knowledge_bridge_candidates(self.db, 1, limit=50)

    def _payload_by_name(self, result, name):
        for payload in result["payloads"]:
            if payload["subjectName"].lower() == name.lower():
                return payload
        self.fail(f"missing payload for {name}: {[p['subjectName'] for p in result['payloads']]}")

    def test_extracts_candidates_from_profiles_facts_relationships_conversations_and_memory(self):
        c = self.conn.cursor()
        c.execute("INSERT INTO user_profiles VALUES (123456789012345678,1,'Emerald','Emerald',NULL,NULL)")
        c.execute("INSERT INTO user_memory_facts VALUES (NULL,123456789012345678,1,'known_subject','Hellcat is a recurring artist candidate.',0.9,1,'now')")
        c.execute("INSERT INTO relationship_state VALUES (123456789012345678,1,5,0.2,'known','warm','Crow is a known recurring community subject','now')")
        c.execute("INSERT INTO relationship_journal VALUES (NULL,123456789012345678,1,'note','Orion via Crow came up as a possible connection, not identity.','now')")
        c.execute("INSERT INTO conversations VALUES (NULL,123456789012345678,'raw user',1,'general','public_home','user','Mac Modem is a recurring BARCODE concept candidate.','now')")
        c.execute("INSERT INTO conversations VALUES (NULL,123456789012345678,'raw user',1,'research-and-development','internal_controlled','user','Shadow Guest is a private internal subject candidate.','now')")
        c.execute("INSERT INTO memory_tiers VALUES (NULL,123456789012345678,1,'long','Memory says Signal Witch is a recurring entity.',0.9,1,'now')")
        self.conn.commit()

        result = self._collect()
        names = {p["subjectName"] for p in result["payloads"]}
        for expected in {"Emerald", "Hellcat", "Crow", "Orion", "Mac Modem", "Shadow Guest", "Signal Witch"}:
            self.assertIn(expected, names)
        public_payload = self._payload_by_name(result, "Mac Modem")
        self.assertIn("public_safe_candidate", public_payload["visibilityLabels"])
        internal_payload = self._payload_by_name(result, "Shadow Guest")
        self.assertIn("internal_only", internal_payload["visibilityLabels"])
        memory_payload = self._payload_by_name(result, "Signal Witch")
        self.assertIn("source_blind_review_required", memory_payload["visibilityLabels"])
        self.assertIn(bridge.SOURCE_BLIND_WARNING, memory_payload["safetyWarnings"])
        self.assertNotIn("public_safe_candidate", memory_payload["visibilityLabels"])
        orion = self._payload_by_name(result, "Orion")
        self.assertEqual(orion["type"], "possible_connection_review")
        self.assertIn("not confirmed identity", orion["reason"])

    def test_explicit_alias_language_maps_to_identity_link(self):
        self.conn.execute("INSERT INTO user_memory_facts VALUES (NULL,1,1,'alias','Emerald aka Green Signal is explicit alias language for review.',0.8,1,'now')")
        self.conn.commit()
        payload = self._payload_by_name(self._collect(), "Emerald")
        self.assertEqual(payload["type"], "identity_link")


    def test_meaning_first_source_packet_separates_raw_provenance(self):
        c = self.conn.cursor()
        c.execute("INSERT INTO user_profiles VALUES (1,1,'Crow','Crow',NULL,NULL)")
        c.execute("INSERT INTO relationship_journal VALUES (NULL,1,1,'note','help_signal: EDGE_SESSION Crow relationship_journal/local_relationship_trace raw detail','now')")
        c.execute("INSERT INTO conversations VALUES (NULL,1,'Crow',1,'general','public_home','user','Crow is a recurring community subject candidate.','now')")
        self.conn.commit()

        payload = self._payload_by_name(self._collect(), "Crow")

        self.assertIn("BNL found an internal local profile match for Crow.", payload["knownContext"])
        self.assertTrue(any("prior relationship/context notes" in item for item in payload["relationshipSignals"]))
        self.assertTrue(any("private relationship context" in item for item in payload["relationshipSignals"]))
        self.assertTrue(any("approved public-side conversation context" in item for item in payload["publicSafePossibilities"]))
        self.assertTrue(any("Keep this internal" in item or "Owner review" in item for item in payload["privateOnlyNotes"]))
        self.assertEqual(payload["recommendedAction"], payload["suggestedAction"])
        self.assertIn("sourceAuthority", payload)

        normal_text = json.dumps({
            "reason": payload.get("reason"),
            "evidenceSummary": payload.get("evidenceSummary"),
            "knownContext": payload.get("knownContext"),
            "usefulEvidence": payload.get("usefulEvidence"),
            "relationshipSignals": payload.get("relationshipSignals"),
            "publicSafePossibilities": payload.get("publicSafePossibilities"),
            "privateOnlyNotes": payload.get("privateOnlyNotes"),
            "notPublicYet": payload.get("notPublicYet"),
            "sourceAuthority": payload.get("sourceAuthority"),
        })
        for raw in (
            "user_profiles/local_profile_observed",
            "relationship_journal/local_relationship_trace",
            "conversations/public_discord_observed",
            "source lane mapping",
            "Source lanes: unknown",
            "unknown -> unknown",
            "help_signal:",
            "EDGE_SESSION",
            "memory_tiers",
        ):
            self.assertNotIn(raw, normal_text)

        provenance = payload["rawProvenance"]
        self.assertIn("user_profiles/local_profile_observed", provenance["sourceLabels"])
        self.assertIn("relationship_journal/local_relationship_trace", provenance["sourceLabels"])
        self.assertIn("conversations/public_discord_observed", provenance["sourceLabels"])
        self.assertEqual(provenance["sourceLaneMapping"].get("relationship_journal"), "unknown")
        self.assertTrue(any("help_signal:" in fragment["snippet"] for fragment in provenance["rawFragments"]))


    def test_entity_summary_enriches_bridge_packet_with_privacy_boundaries_and_provenance(self):
        c = self.conn.cursor()
        c.execute("INSERT INTO user_profiles VALUES (1,1,'Crow','Crow',NULL,NULL)")
        c.execute("INSERT INTO relationship_journal VALUES (NULL,1,1,'note','help_signal: EDGE_SESSION Crow relationship_journal/local_relationship_trace private note','now')")
        c.execute("INSERT INTO conversations VALUES (NULL,1,'Crow',1,'general','public_home','user','Crow is a recurring community subject candidate.','now')")
        c.execute("INSERT INTO memory_tiers VALUES (NULL,1,1,'long','Crow source-blind legacy memory trace.',0.9,1,'now')")
        self.conn.commit()

        payload = self._payload_by_name(self._collect(), "Crow")

        self.assertIn("BNL found an internal local profile match for Crow.", payload["knownContext"])
        self.assertEqual(payload["knownContext"].count("BNL found an internal local profile match for Crow."), 1)
        self.assertTrue(any("relationship/context" in item for item in payload["relationshipSignals"]))
        self.assertTrue(any("prior relationship/context notes" in item for item in payload["privateOnlyNotes"]))
        self.assertEqual(sum(1 for item in payload["privateOnlyNotes"] if "prior relationship/context notes" in item), 1)
        self.assertFalse(any("relationship" in item.lower() for item in payload["publicSafePossibilities"]))
        self.assertTrue(any("Public-side conversation context exists" in item for item in payload["publicSafePossibilities"]))
        self.assertTrue(any("Source-blind legacy memory exists" in item for item in payload["privateOnlyNotes"]))
        self.assertTrue(any("Source-blind memory cannot" in item for item in payload["notPublicYet"]))
        self.assertNotEqual(payload["recommendedAction"], "Review source context.")
        self.assertIn("queue/submission identity is not connected yet", payload["recommendedAction"])

        normal_text = json.dumps({
            "knownContext": payload.get("knownContext"),
            "usefulEvidence": payload.get("usefulEvidence"),
            "relationshipSignals": payload.get("relationshipSignals"),
            "publicSafePossibilities": payload.get("publicSafePossibilities"),
            "privateOnlyNotes": payload.get("privateOnlyNotes"),
            "notPublicYet": payload.get("notPublicYet"),
            "recommendedAction": payload.get("recommendedAction"),
            "sourceAuthority": payload.get("sourceAuthority"),
        })
        for raw in (
            "user_profiles/local_profile_observed",
            "relationship_journal/local_relationship_trace",
            "conversations/public_discord_observed",
            "memory_tiers/source_blind_memory_trace",
            "help_signal",
            "EDGE_SESSION",
            "unknown -> unknown",
        ):
            self.assertNotIn(raw, normal_text)

        provenance = payload["rawProvenance"]
        self.assertIn("user_profiles/local_profile_observed", provenance["sourceLabels"])
        self.assertIn("conversations/public_discord_observed", provenance["sourceLabels"])
        self.assertIn("memory_tiers/source_blind_memory_trace", provenance["sourceLabels"])
        self.assertIn("relationship_journal", provenance["sourceCounts"])
        self.assertIn("relationship_journal", provenance["entitySourceCounts"])
        self.assertEqual(provenance["channelPolicyCounts"].get("public_home"), 1)
        self.assertTrue(any(fragment.get("origin") == "entity_activity_summary" for fragment in provenance["rawFragments"] if isinstance(fragment, dict)))

    def test_unknown_conversation_is_internal_review_not_public_safe(self):
        self.conn.execute("INSERT INTO conversations VALUES (NULL,1,'User',1,'mystery',NULL,'user','Hellcat is a recurring candidate from unknown policy.','now')")
        self.conn.commit()
        payload = self._payload_by_name(self._collect(), "Hellcat")
        self.assertIn("internal_only", payload["visibilityLabels"])
        self.assertNotIn("public_safe_candidate", payload["visibilityLabels"])

    def test_does_not_use_member_activity_events_by_default(self):
        self.conn.execute("INSERT INTO member_activity_events VALUES (NULL,1,'Activity Only','Activity Phantom is a recurring candidate.')")
        self.conn.commit()
        result = self._collect()
        self.assertNotIn("Activity Phantom", {p["subjectName"] for p in result["payloads"]})

    def test_deterministic_ingest_keys_and_dedupe_repeated_subjects(self):
        self.conn.execute("INSERT INTO user_profiles VALUES (1,1,'Emerald','Emerald',NULL,NULL)")
        self.conn.execute("INSERT INTO user_memory_facts VALUES (NULL,1,1,'note','Emerald is a recurring community subject.',0.7,1,'now')")
        self.conn.commit()
        first = self._payload_by_name(self._collect(), "Emerald")
        second = self._payload_by_name(self._collect(), "Emerald")
        self.assertEqual(first["ingestKey"], second["ingestKey"])
        names = [p["subjectName"] for p in self._collect()["payloads"] if p["subjectName"] == "Emerald"]
        self.assertEqual(names, ["Emerald"])

    def test_dry_run_does_not_send_and_failed_sends_are_counted(self):
        self.conn.execute("INSERT INTO user_profiles VALUES (1,1,'Emerald','Emerald',NULL,NULL)")
        self.conn.commit()
        calls = []
        dry = bridge.run_source_knowledge_bridge(self.db, 1, dry_run=True, sender=lambda payload: calls.append(payload) or {"ok": True})
        self.assertEqual(calls, [])
        self.assertEqual(dry["sentCount"], 0)
        real = bridge.run_source_knowledge_bridge(self.db, 1, dry_run=False, sender=lambda payload: {"ok": False, "status": 500})
        self.assertEqual(real["failedCount"], 1)
        self.assertEqual(real["errorStatus"], "failed:500")

    def test_discord_response_is_safe_summary_only(self):
        result = {
            "dryRun": True,
            "sendableRecommendationCount": 1,
            "duplicateCount": 0,
            "withheldCount": 0,
            "sourceCounts": {"user_profiles": 1},
            "warningCounts": {bridge.SOURCE_BLIND_WARNING: 1},
            "candidates": [{"subjectName": "Emerald", "payload": {"evidenceSummary": "raw 123456789012345678 transcript"}}],
        }
        response = bridge.format_source_knowledge_bridge_response(result)
        self.assertIn("Emerald", response)
        self.assertNotIn("123456789012345678", response)
        self.assertNotIn("transcript", response.lower())
        self.assertNotIn("payload", response.lower())


    def test_classifier_rejects_generic_conversational_fragments(self):
        for term in ("what", "there", "such", "alot", "actually", "message"):
            self.assertFalse(bridge.is_valid_bridge_subject(term, source_type="conversations"), term)
        self.assertFalse(bridge.is_valid_bridge_subject("what Emerald", source_type="conversations"))
        self.assertFalse(bridge.is_valid_bridge_subject("Emerald there", source_type="conversations"))
        self.assertFalse(bridge.is_valid_bridge_subject("normal", source_type="conversations"))
        self.assertFalse(bridge.is_valid_bridge_subject("123456789012345678", source_type="conversations"))
        self.assertFalse(bridge.is_valid_bridge_subject("!!!", source_type="conversations"))

    def test_trusted_display_names_are_accepted_and_generic_names_withheld(self):
        c = self.conn.cursor()
        for name in ("Hellcat", "Emerald", "Crow", "Shortbabe"):
            c.execute("INSERT INTO community_presence VALUES (1,?,?,?,?,?,?,?,?,?)", (name.lower(), name, "now", "now", 3, 1, 1, json.dumps([]), json.dumps(["approved-channel presence"])))
        c.execute("INSERT INTO user_profiles VALUES (2,1,'what','what',NULL,NULL)")
        self.conn.commit()
        result = self._collect()
        names = {p["subjectName"] for p in result["payloads"]}
        for expected in {"Hellcat", "Emerald", "Crow", "Shortbabe"}:
            self.assertIn(expected, names)
        self.assertNotIn("what", {name.lower() for name in names})
        self.assertGreaterEqual(result["topWithheldReasons"].get("weak_generic_term", 0), 1)

    def test_known_name_registry_boosts_names_found_in_conversations_and_memory_supports(self):
        self.conn.execute("INSERT INTO community_presence VALUES (1,'emerald','Emerald','now','now',4,0,0,?,?)", (json.dumps([]), json.dumps([])))
        self.conn.execute("INSERT INTO conversations VALUES (NULL,1,'User',1,'general','public_home','user','I was talking with emerald about art yesterday.','now')")
        self.conn.execute("INSERT INTO memory_tiers VALUES (NULL,1,1,'long','Emerald came up in older source-blind memory notes.',0.7,1,'now')")
        self.conn.commit()
        payload = self._payload_by_name(self._collect(), "Emerald")
        self.assertIn("conversations", payload["sourceTypes"])
        self.assertIn("memory_tiers", payload["sourceTypes"])
        self.assertIn(bridge.SOURCE_BLIND_WARNING, payload["safetyWarnings"])

    def test_source_blind_memory_does_not_create_uncorroborated_junk_candidate(self):
        self.conn.execute("INSERT INTO memory_tiers VALUES (NULL,1,1,'long','what there such alot message conversation reply',0.9,1,'now')")
        self.conn.commit()
        result = self._collect()
        lower_names = {p["subjectName"].lower() for p in result["payloads"]}
        for blocked in {"what", "there", "such", "alot"}:
            self.assertNotIn(blocked, lower_names)
        self.assertEqual(result["sendableRecommendationCount"], 0)

    def test_modem_and_orion_connection_review_in_explicit_context(self):
        self.conn.execute("INSERT INTO user_profiles VALUES (10,1,'Modem','Modem',NULL,NULL)")
        self.conn.execute("INSERT INTO conversations VALUES (NULL,10,'Modem',1,'general','public_home','user','source file for Mac Modem; Orion via Crow is a possible connection review.','now')")
        self.conn.commit()
        result = self._collect()
        names = {p["subjectName"] for p in result["payloads"]}
        self.assertIn("Modem", names)
        self.assertIn("Mac Modem", names)
        orion = self._payload_by_name(result, "Orion")
        self.assertEqual(orion["type"], "possible_connection_review")
        self.assertIn("not confirmed identity", orion["reason"])

    def test_dry_run_reports_withheld_reasons_and_does_not_send(self):
        self.conn.execute("INSERT INTO user_profiles VALUES (1,1,'Emerald','Emerald',NULL,NULL)")
        self.conn.execute("INSERT INTO conversations VALUES (NULL,1,'User',1,'general','public_home','user','source file for What. subject: There. known as Such. artist Alot.','now')")
        self.conn.commit()
        calls = []
        result = bridge.run_source_knowledge_bridge(self.db, 1, dry_run=True, sender=lambda payload: calls.append(payload) or {"ok": True})
        response = bridge.format_source_knowledge_bridge_response(result)
        self.assertEqual(calls, [])
        self.assertIn("topWithheldReasons", result)
        self.assertIn("Top withheld reasons", response)
        self.assertGreater(result["withheldCount"], 0)
        self.assertGreater(result["junkTermsBlockedCount"], 0)

    def test_real_run_sends_valid_recommendations(self):
        self.conn.execute("INSERT INTO user_profiles VALUES (1,1,'Emerald','Emerald',NULL,NULL)")
        self.conn.commit()
        calls = []
        result = bridge.run_source_knowledge_bridge(self.db, 1, dry_run=False, sender=lambda payload: calls.append(payload) or {"ok": True})
        self.assertEqual(result["sentCount"], 1)
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0]["subjectName"], "Emerald")

    def test_parse_command_aliases(self):
        for command in ("!bnl source bridge knowledge | limit=2 | dry_run=false", "!bnl source bridge candidates", "!bnl dossier bridge knowledge"):
            matched, options, error = bridge.parse_source_knowledge_bridge_command(command)
            self.assertTrue(matched)
            self.assertFalse(error)
            self.assertIn("limit", options)


if __name__ == "__main__":
    unittest.main()
