import asyncio
import json
import os
import sqlite3
import tempfile
import unittest
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl_source_file_enrichment as enrich
import bnl01_bot


class SourceFileEnrichmentTests(unittest.TestCase):
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
        c.execute("CREATE TABLE memory_tiers (id INTEGER PRIMARY KEY, user_id INTEGER, guild_id INTEGER, tier TEXT, summary TEXT, salience REAL, mentions INTEGER, updated_at TEXT, source_trust TEXT, source_channel_policy TEXT)")
        c.execute("CREATE TABLE broadcast_memory (id INTEGER PRIMARY KEY, guild_id INTEGER, episode_date TEXT, cleaned_summary TEXT, entry_type TEXT, public_safe INTEGER, usage_scope TEXT, status TEXT, created_at TEXT)")
        c.execute("CREATE TABLE community_presence (guild_id INTEGER, subject_key TEXT, display_name TEXT, first_seen_at TEXT, last_seen_at TEXT, source_lanes TEXT, approved_channel_labels TEXT, mention_count INTEGER, direct_interaction_count INTEGER, operator_mention_count INTEGER, active_windows TEXT, connection_notes TEXT, evidence_snippets TEXT, category TEXT, last_error_status TEXT)")
        self.conn.commit()

    def _lookup(self, status="active"):
        return lambda query: {
            "ok": True,
            "found": True,
            "matchKind": "exact",
            "data": {"sourceFile": {"id": "sf_1", "name": query["lookupValue"], "status": status, "candidateType": "entity"}},
        }

    def test_parses_source_enrich_dry_run(self):
        matched, options, error = enrich.parse_source_enrichment_command("!bnl source enrich Hellcat | dry_run=true")
        self.assertTrue(matched)
        self.assertFalse(error)
        self.assertEqual(options["subject"], "Hellcat")
        self.assertTrue(options["dry_run"])
        matched, options, error = enrich.parse_source_enrichment_command("!bnl source enrich Hellcat | dry_run=false | force=true")
        self.assertTrue(matched)
        self.assertFalse(error)
        self.assertTrue(options["force"])
        for command in ("!bnl enrich source Emerald", "!bnl source file enrich Mac Modem | dry_run=false"):
            matched, options, error = enrich.parse_source_enrichment_command(command)
            self.assertTrue(matched)
            self.assertFalse(error)


    def test_parses_enrich_lookup_modes(self):
        cases = (
            ("!bnl source enrich candidateId=sf_123 | dry_run=true", "candidateId", "sf_123"),
            ("!bnl source enrich alias=Hellcat | dry_run=true", "alias", "Hellcat"),
            ("!bnl source enrich normalizedName=hellcatnz | dry_run=true", "normalizedName", "hellcatnz"),
            ("!bnl source enrich subject=Hellcat | dry_run=true", "subject", "Hellcat"),
        )
        for command, key, value in cases:
            matched, options, error = enrich.parse_source_enrichment_command(command)
            self.assertTrue(matched)
            self.assertFalse(error)
            self.assertEqual(options["lookupKey"], key)
            self.assertEqual(options["lookupValue"], value)
            self.assertTrue(options["dry_run"])

    def test_possible_single_match_is_review_only_and_does_not_send(self):
        calls = []
        result = enrich.run_source_file_enrichment(
            self.db,
            1,
            "Hellcat",
            dry_run=True,
            lookup_func=lambda query: {
                "ok": True,
                "found": False,
                "data": {"possibleMatches": [{"name": "HellcatNZ", "matchKind": "partial_name", "candidateId": "sf_hellcatnz"}]},
            },
            sender=lambda payload: calls.append(payload) or {"ok": True},
        )
        response = enrich.format_source_enrichment_response(result)
        self.assertEqual(result["status"], "possible_match_review")
        self.assertEqual(result["resolutionMode"], "possible_match_review")
        self.assertEqual(result["possibleMatchCount"], 1)
        self.assertFalse(result["sent"])
        self.assertEqual(calls, [])
        self.assertIn("Possible match found: HellcatNZ", response)
        self.assertIn("Match kind: partial_name", response)
        self.assertIn("Use candidateId=sf_hellcatnz or exact name", response)
        self.assertIn("No notes were sent", response)
        self.assertNotIn("no target found", response.lower())

    def test_multiple_possible_matches_asks_operator_to_narrow(self):
        result = enrich.run_source_file_enrichment(
            self.db,
            1,
            "Modem",
            dry_run=True,
            lookup_func=lambda query: {
                "ok": True,
                "found": False,
                "data": {
                    "possibleMatches": [
                        {"name": "Mac Modem", "matchKind": "partial_name", "candidateId": "sf_mac_modem"},
                        {"name": "Modem Ghost", "matchKind": "compact_name", "candidateId": "sf_modem_ghost"},
                    ]
                },
            },
        )
        response = enrich.format_source_enrichment_response(result)
        self.assertEqual(result["status"], "possible_match_review")
        self.assertIn("Possible matches (narrow", response)
        self.assertIn("Mac Modem — partial_name — candidateId=sf_mac_modem", response)
        self.assertIn("Use exact name or candidateId", response)
        self.assertFalse(result["sent"])

    def test_candidate_id_lookup_proceeds_with_enrichment(self):
        queries = []
        result = enrich.run_source_file_enrichment(
            self.db,
            1,
            "sf_hellcatnz",
            dry_run=True,
            lookup_key="candidateId",
            lookup_value="sf_hellcatnz",
            lookup_func=lambda query: queries.append(query) or {
                "ok": True,
                "found": True,
                "matchKind": "candidateId",
                "data": {"sourceFile": {"id": "sf_hellcatnz", "name": "HellcatNZ", "status": "active"}},
            },
        )
        self.assertEqual(queries[0]["lookupKey"], "candidateId")
        self.assertEqual(result["status"], "dry_run")
        self.assertEqual(result["resolutionMode"], "candidateId")
        self.assertEqual(result["subject"], "HellcatNZ")

    def test_alias_confirmed_proceeds_but_unconfirmed_alias_is_review_only(self):
        confirmed = enrich.run_source_file_enrichment(
            self.db,
            1,
            "Hellcat",
            dry_run=True,
            lookup_key="alias",
            lookup_value="Hellcat",
            lookup_func=lambda query: {
                "ok": True,
                "found": True,
                "matchKind": "confirmed alias",
                "data": {"matchedAlias": "Hellcat", "sourceFile": {"name": "HellcatNZ", "status": "active"}},
            },
        )
        self.assertEqual(confirmed["status"], "dry_run")
        self.assertEqual(confirmed["resolutionMode"], "alias")

        calls = []
        unconfirmed = enrich.run_source_file_enrichment(
            self.db,
            1,
            "Hellcat",
            dry_run=False,
            lookup_key="alias",
            lookup_value="Hellcat",
            lookup_func=lambda query: {
                "ok": True,
                "found": False,
                "data": {"possibleMatches": [{"name": "HellcatNZ", "matchKind": "unconfirmed_alias", "candidateId": "sf_hellcatnz"}]},
            },
            sender=lambda payload: calls.append(payload) or {"ok": True},
        )
        response = enrich.format_source_enrichment_response(unconfirmed)
        self.assertEqual(unconfirmed["status"], "possible_match_review")
        self.assertEqual(calls, [])
        self.assertIn("Use candidateId=sf_hellcatnz", response)

    def test_exact_name_lookup_still_proceeds_with_enrichment(self):
        result = enrich.run_source_file_enrichment(self.db, 1, "HellcatNZ", dry_run=True, lookup_func=self._lookup("active"))
        self.assertEqual(result["status"], "dry_run")
        self.assertEqual(result["matchKind"], "active_source_file")
        self.assertEqual(result["resolutionMode"], "exact")

    def test_enriches_active_source_file_with_structured_sections(self):
        c = self.conn.cursor()
        c.execute("INSERT INTO user_profiles VALUES (10,1,'Hellcat','Hellcat',NULL,NULL)")
        c.execute("INSERT INTO user_memory_facts VALUES (NULL,10,1,'role','Hellcat is a recurring artist candidate.',0.9,1,'now')")
        c.execute("INSERT INTO broadcast_memory VALUES (NULL,1,'2026-05-30','Hellcat was referenced in a public-safe BARCODE Radio note.','show_note',1,'ambient,direct','active','now')")
        self.conn.commit()
        calls = []
        result = enrich.run_source_file_enrichment(self.db, 1, "Hellcat", dry_run=False, lookup_func=self._lookup("active"), sender=lambda payload: calls.append(payload) or {"ok": True, "status": 200})
        self.assertTrue(result["sent"])
        self.assertEqual(result["matchKind"], "active_source_file")
        self.assertIn("Subject Overview", result["sections"])
        self.assertIn("Known Facts", result["sections"])
        self.assertIn("Public-Safe Notes", result["sections"])
        self.assertEqual(calls[0]["type"], "modify_existing_dossier")
        self.assertIn("review-only", calls[0]["reason"].lower())

    def test_candidate_intake_and_existing_dossier_are_labeled(self):
        candidate = enrich.run_source_file_enrichment(self.db, 1, "Emerald", dry_run=True, lookup_func=self._lookup("candidate_intake"))
        self.assertEqual(candidate["matchKind"], "candidate_intake")
        self.assertIn("Candidate Intake", enrich.format_source_enrichment_response(candidate))
        dossier_lookup = lambda query: {"ok": True, "found": True, "data": {"sourceFile": {"name": "Crow", "type": "public_dossier", "targetDossierId": "d1"}}}
        dossier = enrich.run_source_file_enrichment(self.db, 1, "Crow", dry_run=True, lookup_func=dossier_lookup)
        self.assertEqual(dossier["matchKind"], "existing_dossier_update")
        self.assertIn("Existing Dossier Update", enrich.format_source_enrichment_response(dossier))

    def test_no_match_returns_bounded_no_target_response(self):
        result = enrich.run_source_file_enrichment(self.db, 1, "Unknown", dry_run=True, lookup_func=lambda query: {"ok": True, "found": False, "data": {}})
        response = enrich.format_source_enrichment_response(result)
        self.assertEqual(result["status"], "no_target")
        self.assertIn("create/promote", response)
        self.assertNotIn("payload", response.lower())

    def test_source_blind_memory_review_only_and_alias_not_confirmed(self):
        c = self.conn.cursor()
        c.execute("INSERT INTO memory_tiers VALUES (NULL,10,1,'long','Hellcat may be connected to Emerald in old source-blind memory.',0.9,1,'now','legacy_unknown','legacy_unknown')")
        c.execute("INSERT INTO conversations VALUES (NULL,10,'User',1,'general','public_home','user','Hellcat aka Emerald is a possible connection review, not confirmed identity. 123456789012345678','now')")
        self.conn.commit()
        result = enrich.run_source_file_enrichment(self.db, 1, "Hellcat", dry_run=True, lookup_func=self._lookup("active"))
        self.assertIn(enrich.SOURCE_BLIND_WARNING, result["warnings"])
        self.assertIn("Internal-Only / Review-Only Notes", result["sections"])
        alias_text = json.dumps(result["sections"].get("Possible Aliases / Connections", []))
        self.assertIn("not confirmed", alias_text.lower())
        response = enrich.format_source_enrichment_response(result)
        self.assertNotIn("123456789012345678", response)
        self.assertNotIn("payload", response.lower())


    def test_case_file_language_rewrites_raw_fragments(self):
        c = self.conn.cursor()
        c.execute("INSERT INTO user_profiles VALUES (10,1,'HellcatNZ','HellcatNZ',NULL,NULL)")
        c.execute("INSERT INTO relationship_state VALUES (10,1,8,0.7,'steady','friendly','HellcatNZ radio planning','now')")
        c.execute("INSERT INTO community_presence VALUES (1,'hellcatnz','HellcatNZ','now','now','discord','general',4,2,1,'','','','artist','')")
        self.conn.commit()
        result = enrich.run_source_file_enrichment(self.db, 1, "HellcatNZ", dry_run=True, lookup_func=self._lookup("active"))
        evidence = result["payload"]["evidenceSummary"]
        self.assertIn("already known to the BARCODE Network workflow", evidence)
        self.assertIn("prior interaction history", evidence)
        self.assertIn("community-side activity", evidence)
        self.assertNotIn("Local profile/display-name match", evidence)
        self.assertNotIn("Relationship state:", evidence)
        self.assertNotIn("interactions; stage=", evidence)
        self.assertNotIn("mention/direct/operator signals", evidence)

    def test_known_facts_exclude_weak_and_source_blind_claims(self):
        c = self.conn.cursor()
        c.execute("INSERT INTO user_memory_facts VALUES (NULL,10,1,'role','HellcatNZ might be a collaborator.',0.4,0,'now')")
        c.execute("INSERT INTO memory_tiers VALUES (NULL,10,1,'long','HellcatNZ is allegedly tied to a private old note.',0.9,1,'now','legacy_unknown','legacy_unknown')")
        self.conn.commit()
        result = enrich.run_source_file_enrichment(self.db, 1, "HellcatNZ", dry_run=True, lookup_func=self._lookup("active"))
        known = json.dumps(result["sections"].get("Known Facts", []))
        inferred = json.dumps(result["sections"].get("Claimed or Inferred Notes", []))
        internal = json.dumps(result["sections"].get("Internal-Only / Review-Only Notes", []))
        self.assertNotIn("might be a collaborator", known)
        self.assertNotIn("private old note", known)
        self.assertIn("might be a collaborator", inferred)
        self.assertIn("Review-only legacy memory", internal)
        self.assertIn(enrich.SOURCE_BLIND_WARNING, result["warnings"])

    def test_identity_resolution_builds_alias_variants_and_counts_profiles(self):
        c = self.conn.cursor()
        c.execute("INSERT INTO user_profiles VALUES (10,1,'Hellcat NZ','HellcatNZ',NULL,NULL)")
        self.conn.commit()
        lookup = {
            "ok": True,
            "found": True,
            "matchKind": "confirmed alias",
            "data": {
                "matchedAlias": "Hellcat",
                "confirmedAliases": ["Hellcat NZ"],
                "sourceFile": {"id": "sf_hellcatnz", "name": "HellcatNZ", "status": "active", "identityLinks": ["Hellcat"]},
            },
        }
        identity = enrich.resolve_enrichment_subject_identity("HellcatNZ", lookup, self.db, 1)
        self.assertEqual(identity["matchedUserIdCount"], 1)
        self.assertEqual(identity["matchedUserProfileCount"], 1)
        self.assertIn("HellcatNZ", identity["aliasLabels"])
        self.assertIn("Hellcat", identity["aliasLabels"])
        self.assertNotIn("10", json.dumps({k: v for k, v in identity.items() if not k.startswith("_")}))

    def test_author_conversation_activity_uses_identity_without_subject_text_and_excludes_private(self):
        c = self.conn.cursor()
        c.execute("INSERT INTO user_profiles VALUES (10,1,'HellcatNZ','HellcatNZ',NULL,NULL)")
        c.execute("INSERT INTO conversations VALUES (NULL,10,'HellcatNZ',1,'general','public_home','user','talking about the new mix without self naming','now')")
        c.execute("INSERT INTO conversations VALUES (NULL,10,'HellcatNZ',1,'secret','private','user','private raw transcript should not appear','now')")
        c.execute("INSERT INTO conversations VALUES (NULL,10,'HellcatNZ',1,'dm','dm','user','dm raw transcript should not appear','now')")
        self.conn.commit()
        result = enrich.run_source_file_enrichment(self.db, 1, "HellcatNZ", dry_run=True, lookup_func=self._lookup("active"))
        self.assertIn("conversations_by_author", result["sourceTypes"])
        self.assertTrue(result["diagnostics"]["channelEvidenceFound"])
        observed = "\n".join(result["sections"].get("Observed Patterns", []))
        self.assertIn("internal source context, not public dossier copy", observed)
        self.assertIn("review-only internal context", observed)
        self.assertIn("No meaningful repeated theme has been extracted yet", observed)
        response = enrich.format_source_enrichment_response(result)
        self.assertIn("conversations_by_author yes", response)
        self.assertNotIn("talking about the new mix", response)
        self.assertNotIn("private raw transcript", response)
        self.assertNotIn("dm raw transcript", response)
        self.assertNotIn("10", response)

    def test_identity_linked_user_tables_contribute_to_patterns_and_history(self):
        c = self.conn.cursor()
        c.execute("INSERT INTO user_profiles VALUES (10,1,'HellcatNZ','HellcatNZ',NULL,NULL)")
        c.execute("INSERT INTO user_habits VALUES (10,1,12,3,2,1,20.0,'mix planning','now')")
        c.execute("INSERT INTO relationship_state VALUES (10,1,5,0.7,'steady','friendly','last check in','now')")
        c.execute("INSERT INTO relationship_journal VALUES (NULL,10,1,'check_in','helped BNL test a community workflow','now')")
        self.conn.commit()
        result = enrich.run_source_file_enrichment(self.db, 1, "HellcatNZ", dry_run=True, lookup_func=self._lookup("active"))
        self.assertIn("user_habits", result["sourceTypes"])
        self.assertIn("relationship_state", result["sourceTypes"])
        self.assertIn("relationship_journal", result["sourceTypes"])
        self.assertIn("recurring Discord-side activity", "\n".join(result["sections"].get("Observed Patterns", [])))
        history = "\n".join(result["sections"].get("History With BARCODE / BNL / Discord / BARCODE Radio", []))
        self.assertIn("prior interaction history", history)
        self.assertIn("Recent BNL relationship note", history)

    def test_community_presence_display_name_and_existing_dossier_awareness(self):
        c = self.conn.cursor()
        c.execute("INSERT INTO community_presence VALUES (1,'hellcatnz','Hellcat NZ','now','now','[\"community_presence\"]','[\"general\"]',4,1,0,'[]','[]','[]','artist','none')")
        self.conn.commit()
        def lookup(query):
            return {
                "ok": True,
                "found": True,
                "data": {
                    "existingDossierMatch": {"targetDossierId": "dossier_hellcat", "title": "HellcatNZ"},
                    "sourceFile": {"name": "HellcatNZ", "type": "public_dossier", "targetDossierId": "dossier_hellcat"},
                },
            }
        result = enrich.run_source_file_enrichment(self.db, 1, "HellcatNZ", dry_run=True, lookup_func=lookup)
        self.assertEqual(result["matchKind"], "existing_dossier_update")
        self.assertIn("community_presence", result["sourceTypes"])
        self.assertTrue(result["diagnostics"]["publicDossierMatchFound"])
        self.assertTrue(result["diagnostics"]["existingDossierUpdateLane"])
        history = "\n".join(result["sections"].get("History With BARCODE / BNL / Discord / BARCODE Radio", []))
        self.assertIn("proposed update material", history)
        self.assertIn("public dossier content was not changed", history)
        response = enrich.format_source_enrichment_response(result)
        self.assertIn("public_dossier_match yes", response)
        self.assertIn("existing dossier update lane: yes", response)


    def test_backfilled_channel_activity_enriches_source_evidence_without_overclaiming(self):
        c = self.conn.cursor()
        c.execute("INSERT INTO user_profiles VALUES (10,1,'HellcatNZ','HellcatNZ',NULL,NULL)")
        c.execute("INSERT INTO conversations VALUES (NULL,10,'HellcatNZ',1,'hellcat-nz','public_selective','user','safe activity one','2026-05-30T01:00:00+00:00')")
        c.execute("INSERT INTO conversations VALUES (NULL,10,'HellcatNZ',1,'hellcat-nz','public_selective','user','safe activity two','2026-05-30T02:00:00+00:00')")
        c.execute("INSERT INTO conversations VALUES (NULL,10,'HellcatNZ',1,'hellcat-nz','public_selective','user','safe activity three','2026-05-30T03:00:00+00:00')")
        c.execute("INSERT INTO community_presence VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (1, "hellcatnz", "HellcatNZ", "now", "now", json.dumps(["community_presence"]), json.dumps(["public_selective"]), 3, 0, 0, json.dumps([]), json.dumps([]), json.dumps(["approved-channel presence"]), "community_regular_candidate", "none"))
        self.conn.commit()
        result = enrich.run_source_file_enrichment(self.db, 1, "HellcatNZ", dry_run=True, lookup_func=self._lookup("active"))
        observed = "\n".join(result["sections"].get("Observed Patterns", []))
        self.assertIn("recurring activity in #hellcat-nz", observed)
        self.assertIn("review-only internal context", observed)
        self.assertIn("no specific public-safe role has been confirmed", observed)
        response = enrich.format_source_enrichment_response(result)
        self.assertIn("conversations_by_author yes", response)
        self.assertIn("community_presence yes", response)
        self.assertNotIn("safe activity one", response)

    def test_source_coverage_diagnostics_and_review_boundaries_are_safe(self):
        c = self.conn.cursor()
        c.execute("INSERT INTO user_profiles VALUES (123456789012345678,1,'HellcatNZ','HellcatNZ',NULL,NULL)")
        c.execute("INSERT INTO memory_tiers VALUES (NULL,123456789012345678,1,'long','HellcatNZ appears in source-blind memory only.',0.9,1,'now','legacy_unknown','legacy_unknown')")
        self.conn.commit()
        result = enrich.run_source_file_enrichment(self.db, 1, "HellcatNZ", dry_run=True, lookup_func=self._lookup("active"))
        self.assertIn("sourceTypesChecked", result)
        self.assertIn("sourceTypesSkipped", result)
        self.assertIn("memory_tiers", result["sourceTypesChecked"])
        response = enrich.format_source_enrichment_response(result)
        self.assertIn("Source coverage:", response)
        self.assertIn("source-blind memory", "\n".join(result["sections"].get("Internal-Only / Review-Only Notes", [])).lower())
        self.assertNotIn("123456789012345678", response)
        self.assertNotIn("raw transcript", response.lower())

    def test_missing_info_and_next_action_are_actionable(self):
        result = enrich.run_source_file_enrichment(self.db, 1, "HellcatNZ", dry_run=True, lookup_func=self._lookup("active"))
        missing = "\n".join(result["sections"].get("Missing Info", []))
        action = "\n".join(result["sections"].get("Suggested Next Action", []))
        self.assertIn("Confirm the public role/category", missing)
        self.assertIn("source-safe summary", missing)
        self.assertIn("Confirm possible aliases", missing)
        self.assertIn("approve a public-safe summary", action)

    def test_dry_run_preview_has_useful_bullets_without_raw_ids(self):
        c = self.conn.cursor()
        c.execute("INSERT INTO user_profiles VALUES (10,1,'HellcatNZ','HellcatNZ',NULL,NULL)")
        c.execute("INSERT INTO user_memory_facts VALUES (NULL,10,1,'role','HellcatNZ is a recurring artist candidate.',0.9,1,'now')")
        c.execute("INSERT INTO conversations VALUES (NULL,10,'User',1,'general','public_home','user','HellcatNZ discussed without exposing 123456789012345678 as raw ID.','now')")
        self.conn.commit()
        result = enrich.run_source_file_enrichment(self.db, 1, "HellcatNZ", dry_run=True, lookup_func=self._lookup("active"))
        response = enrich.format_source_enrichment_response(result)
        self.assertIn("Target lane: active Source File", response)
        self.assertIn("Useful preview bullets", response)
        self.assertIn("Would send as BNL Source File Enrichment", response)
        self.assertNotIn("123456789012345678", response)
        self.assertGreaterEqual(result["previewBulletsCount"], 3)

    def test_thin_enrichment_suppressed_unless_forced(self):
        calls = []
        thin = enrich.run_source_file_enrichment(
            self.db,
            1,
            "HellcatNZ",
            dry_run=False,
            lookup_func=self._lookup("active"),
            sender=lambda payload: calls.append(payload) or {"ok": True},
        )
        self.assertEqual(thin["status"], "suppressed_too_thin")
        self.assertFalse(thin["sent"])
        self.assertEqual(calls, [])
        self.assertEqual(thin["qualityStatus"], "too_thin")
        self.assertIn("too thin", enrich.format_source_enrichment_response(thin).lower())

        forced = enrich.run_source_file_enrichment(
            self.db,
            1,
            "HellcatNZ",
            dry_run=False,
            force=True,
            lookup_func=self._lookup("active"),
            sender=lambda payload: calls.append(payload) or {"ok": True},
        )
        self.assertTrue(forced["sent"])
        self.assertEqual(forced["qualityStatus"], "forced_low_confidence")
        self.assertTrue(calls[-1]["forced"])
        self.assertEqual(calls[-1]["confidence"], "low")
        self.assertIn("Forced low-confidence", enrich.format_source_enrichment_response(forced))

    def test_payload_keeps_machine_metadata(self):
        result = enrich.run_source_file_enrichment(self.db, 1, "HellcatNZ", dry_run=True, lookup_func=self._lookup("active"))
        payload = result["payload"]
        for key in ("sourceCounts", "warningCounts", "sourceTypes", "ingestSource", "ingestKey", "publicSafetyNotes", "missingInfo", "doNotSay"):
            self.assertIn(key, payload)

    def test_dry_run_does_not_send_and_ingest_key_is_deterministic(self):
        calls = []
        first = enrich.run_source_file_enrichment(self.db, 1, "Hellcat", dry_run=True, lookup_func=self._lookup("active"), sender=lambda payload: calls.append(payload) or {"ok": True})
        second = enrich.run_source_file_enrichment(self.db, 1, "Hellcat", dry_run=True, lookup_func=self._lookup("active"), sender=lambda payload: calls.append(payload) or {"ok": True})
        self.assertEqual(calls, [])
        self.assertEqual(first["ingestKey"], second["ingestKey"])
        self.assertEqual(first["ingestSource"], enrich.FALLBACK_INGEST_SOURCE)

    def test_does_not_invent_unsupported_sections(self):
        result = enrich.run_source_file_enrichment(self.db, 1, "Mac Modem", dry_run=True, lookup_func=self._lookup("active"))
        self.assertNotIn("Observed Patterns", result["sections"])
        self.assertIn("Missing Info", result["sections"])


class SourceFileEnrichmentBotTests(unittest.TestCase):
    class Perms:
        administrator = False
        manage_guild = False
        manage_messages = False
        kick_members = False
        ban_members = False

    class User:
        def __init__(self, user_id):
            self.id = user_id
            self.guild_permissions = SourceFileEnrichmentBotTests.Perms()
            self.roles = []

    class Guild:
        id = 1
        owner_id = 999
        def get_member(self, user_id):
            return None

    class Channel:
        id = 10
        parent = None
        def __init__(self, name):
            self.name = name

    class Message:
        def __init__(self, user_id=999, channel_name="research-and-development"):
            self.author = SourceFileEnrichmentBotTests.User(user_id)
            self.guild = SourceFileEnrichmentBotTests.Guild()
            self.channel = SourceFileEnrichmentBotTests.Channel(channel_name)
            self.replies = []
        async def reply(self, text):
            self.replies.append(text)

    def test_rejects_unauthorized_user(self):
        bnl01_bot.BNL_OWNER_USER_ID = 999
        message = self.Message(user_id=111)
        with mock.patch.object(bnl01_bot, "run_source_file_enrichment") as run_mock:
            handled = asyncio.run(bnl01_bot.maybe_handle_source_file_enrichment_command(message, "!bnl source enrich Hellcat | dry_run=true"))
        self.assertTrue(handled)
        run_mock.assert_not_called()
        self.assertIn("operator-only", message.replies[-1])

    def test_rejects_public_channel_use(self):
        bnl01_bot.BNL_OWNER_USER_ID = 999
        message = self.Message(user_id=999, channel_name="general")
        with mock.patch.object(bnl01_bot, "resolve_channel_policy", return_value="public_home"), mock.patch.object(bnl01_bot, "run_source_file_enrichment") as run_mock:
            handled = asyncio.run(bnl01_bot.maybe_handle_source_file_enrichment_command(message, "!bnl source enrich Hellcat | dry_run=true"))
        self.assertTrue(handled)
        run_mock.assert_not_called()
        self.assertIn("restricted", message.replies[-1])

    def test_real_run_reports_send_failure_safely_and_diagnostics_update(self):
        bnl01_bot.BNL_OWNER_USER_ID = 999
        message = self.Message(user_id=999)
        fake = {"ok": True, "dryRun": False, "subject": "Hellcat", "status": "send_failed", "matchKind": "active_source_file", "sections": {"Subject Overview": ["x"]}, "sourceCounts": {"source_file_lookup": 1}, "warningCounts": {}, "sourceTypes": ["source_file_lookup"], "warnings": [], "sent": False, "sendResult": {"error": "site support missing"}}
        with mock.patch.object(bnl01_bot, "run_source_file_enrichment", return_value=fake):
            handled = asyncio.run(bnl01_bot.maybe_handle_source_file_enrichment_command(message, "!bnl source enrich Hellcat"))
        self.assertTrue(handled)
        self.assertIn("not sent", message.replies[-1])
        diag = bnl01_bot.build_dossier_recommendation_diagnostics(1)
        self.assertTrue(diag["source_enrichment_available"])
        self.assertEqual(diag["source_enrichment_last_subject"], "Hellcat")
        self.assertEqual(diag["source_enrichment_last_match_kind"], "active_source_file")
        self.assertIn("source_enrichment_last_lookup_found", diag)
        self.assertIn("source_enrichment_last_possible_match_count", diag)
        self.assertIn("source_enrichment_last_possible_match_names", diag)
        self.assertIn("source_enrichment_last_resolution_mode", diag)
        self.assertIn("source_enrichment_last_quality_score", diag)
        self.assertIn("source_enrichment_last_quality_status", diag)
        self.assertIn("source_enrichment_last_suppressed_reason", diag)
        self.assertIn("source_enrichment_last_preview_bullets_count", diag)


if __name__ == "__main__":
    unittest.main()
