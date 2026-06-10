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
        matched, options, error = enrich.parse_source_enrichment_command("!bnl source enrich Hellcat | dry_run=false | force=true | diagnostics=true")
        self.assertTrue(matched)
        self.assertFalse(error)
        self.assertTrue(options["force"])
        self.assertTrue(options["diagnostics"])
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


    def test_dry_run_preview_quarantines_subject_intelligence_diagnostic_lists(self):
        result = {
            "subject": "Crow",
            "matchKind": "active_source_file",
            "dryRun": True,
            "qualityStatus": "ok",
            "sourceTypes": ["source_file_lookup", "conversations_by_author"],
            "subjectIntelligenceDiagnostics": {
                "rowsScanned": 4,
                "rowsBySource": {"conversations_by_author": 3, "community_presence": 1},
                "topRecurringSubjects": ["Orion"],
                "acceptedRecurringSubjects": [
                    {"label": "Orion", "reason": "repeated liaison context"},
                    {"label": "Suno", "reason": "recurring tool mention"},
                ],
                "rejectedGarbageCandidates": ["BNL", "Discord"],
                "topRecurringThemes": ["liaison interface"],
                "topConversationClusters": ["repeated BNL questions", "music tool links"],
                "topActivityPatterns": ["asks about source thresholds", "actual music platform links"],
                "topDomains": ["suno.com"],
                "publicSafeRows": 2,
                "reviewOnlyRows": 2,
            },
        }

        formatted = enrich.format_source_enrichment_response(result)

        self.assertIn("Legacy recurring-subject diagnostics:", formatted)
        self.assertIn("advisory only", formatted)
        self.assertNotIn("Top recurring subjects:", formatted)
        self.assertNotIn("Accepted recurring subjects with reason:", formatted)
        result["diagnosticsRequested"] = True
        diagnostic_formatted = enrich.format_source_enrichment_response(result)
        self.assertIn("Legacy recurring-subject diagnostics only. These are not Source File claims.", diagnostic_formatted)
        self.assertIn("Accepted recurring subjects with reason (diagnostic only)", diagnostic_formatted)
        self.assertIn("Orion: repeated liaison context", diagnostic_formatted)
        self.assertIn("Rejected legacy candidates", diagnostic_formatted)
        self.assertIn("BNL, Discord", diagnostic_formatted)

    def test_dry_run_preview_includes_subject_intelligence_diagnostics(self):
        self.conn.execute("INSERT INTO user_profiles VALUES (42,1,'Crow','Crow',NULL,NULL)")
        self.conn.execute("INSERT INTO conversations VALUES (NULL,42,'Crow',1,'general','public_home','user','You and Your Network BNL-01 language should not become topics. Orion through Crow asks BNL about Suno links, BARCODE Network behavior, BNL presence, and threshold behavior: https://suno.com/song/a', '2026-06-01')")
        self.conn.execute("INSERT INTO conversations VALUES (NULL,42,'Crow',1,'general','public_home','user','Your Network and BNL-01 repeat as system context while Orion through Crow repeats BNL liaison interface language, sync convergence markers, operational boundaries, and Suno links: https://suno.com/song/b', '2026-06-02')")
        self.conn.commit()

        result = enrich.run_source_file_enrichment(self.db, 1, "Crow", dry_run=True, lookup_func=self._lookup("active"))
        formatted = enrich.format_source_enrichment_response(result)

        self.assertIn("Subject intelligence rows scanned:", formatted)
        self.assertIn("Subject intelligence rows by source:", formatted)
        self.assertIn("Legacy recurring-subject diagnostics:", formatted)
        self.assertNotIn("Top recurring subjects:", formatted)
        self.assertNotIn("Accepted recurring subjects with reason:", formatted)
        self.assertIn("Orion", formatted)
        self.assertNotIn("Recurring named topic: You", formatted)
        self.assertNotIn("Recurring named topic: BNL-01", formatted)
        self.assertIn("Public-safe vs review-only intelligence rows:", formatted)
        payload = result["payload"]
        payload_text = json.dumps({
            key: payload.get(key)
            for key in ("knownContext", "usefulEvidence", "topicBreakdown", "conversationHighlights", "bestEvidenceToReview", "publicUseCandidates", "bnlInteractionSignals", "musicSignals", "evidenceDetails")
        })
        for garbage in ("You", "Your", "Network", "BNL-01"):
            self.assertNotIn(f"Recurring named topic: {garbage}", payload_text)
        self.assertNotIn("Recurring named topic:", payload_text)
        self.assertIn("Orion: speaks_through", payload_text)
        self.assertIn("Conversation theme:", payload_text)
        self.assertIn("Actual music platform links", payload_text)
        self.assertEqual(payload["queueSubmissionStatus"], "not_connected")
        self.assertNotRegex(payload_text, r"submitted\s+\d+|play count|source type:|Priority|payment|rawRefJson|sourceRowId|research-and-development")
        self.assertLessEqual(len(formatted), 1900)

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


    def test_profile_only_classification_unknown_needs_review(self):
        c = self.conn.cursor()
        c.execute("INSERT INTO user_profiles VALUES (10,1,'Profile Only','Profile Only',NULL,NULL)")
        self.conn.commit()
        result = enrich.run_source_file_enrichment(self.db, 1, "Profile Only", dry_run=True, lookup_func=self._lookup("active"))
        classification = result["classification"]
        self.assertEqual(classification["primaryRole"], "unknown")
        self.assertIn(classification["activityLevel"], {"thin", "none"})
        self.assertIn(classification["dossierUse"], {"not_ready", "source_file_only"})
        self.assertIn("needs owner/admin review", classification["roleRead"])

    def test_backfilled_activity_classifies_active_recurring_without_artist_overclaim(self):
        c = self.conn.cursor()
        c.execute("INSERT INTO user_profiles VALUES (10,1,'HellcatNZ','HellcatNZ',NULL,NULL)")
        c.execute("INSERT INTO conversations VALUES (NULL,10,'HellcatNZ',1,'hellcat-nz','public_selective','user','just hanging in community chat','2026-05-29T01:00:00+00:00')")
        c.execute("INSERT INTO conversations VALUES (NULL,10,'HellcatNZ',1,'hellcat-nz','public_selective','user','more community chat, not a submission claim','2026-05-30T01:00:00+00:00')")
        c.execute("INSERT INTO conversations VALUES (NULL,10,'HellcatNZ',1,'hellcat-nz','public_selective','user','community chat with no artist proof','2026-05-30T02:00:00+00:00')")
        self.conn.commit()
        result = enrich.run_source_file_enrichment(self.db, 1, "HellcatNZ", dry_run=True, lookup_func=self._lookup("active"))
        classification = result["classification"]
        self.assertEqual(classification["primaryRole"], "active_community_member")
        self.assertEqual(classification["activityLevel"], "recurring")
        self.assertNotIn("artist", [classification["primaryRole"]] + classification["secondaryRoles"])
        self.assertEqual(classification["dossierUse"], "source_file_only")
        response = enrich.format_source_enrichment_response(result)
        self.assertIn("Role read: Active community member; artist status unconfirmed", response)
        self.assertIn("Activity read: Recurring approved-channel activity found", response)
        self.assertIn("Dossier use: Source File only", response)
        self.assertIn("## Internal Classification", result["payload"]["evidenceSummary"])
        self.assertIn("Role read: Active community member", result["payload"]["evidenceSummary"])
        self.assertNotIn("just hanging", response)

    def test_submitter_moderator_team_entity_and_sponsor_require_source_evidence(self):
        chatter = enrich.classify_entity_activity({}, {"sourceCounts": {"conversations_by_author": 3}, "channelActivity": {"approvedMessageCount": 3, "approvedDayCount": 2, "themeTexts": ["I am an artist mod sponsor in chat"]}})
        self.assertEqual(chatter["primaryRole"], "active_community_member")
        self.assertNotIn("artist", [chatter["primaryRole"]] + chatter["secondaryRoles"])
        supported = enrich.classify_entity_activity({}, {"sourceCounts": {"source_file_lookup": 1}, "sourceFile": {"summary": "BARCODE team moderator and lore entity sponsor; submitted a track as an artist collaborator."}, "sections": {"Public-Safe Notes": ["Owner-safe context confirms public role."]}})
        roles = [supported["primaryRole"]] + supported["secondaryRoles"]
        for role in ("sponsor", "barcode_team", "moderator", "barcode_entity", "submitter", "artist", "collaborator"):
            self.assertIn(role, roles)
        self.assertIn(supported["dossierUse"], {"possible_future_dossier", "draftable_after_review"})

    def test_existing_dossier_and_relationship_classification_stays_cautious(self):
        result = enrich.classify_entity_activity(
            {"existingDossierUpdateLane": True},
            {"matchKind": "existing_dossier_update", "sourceCounts": {"relationship_state": 1}, "diagnostics": {"existingDossierUpdateLane": True}, "sections": {"Possible Aliases / Connections": ["possible connection only"]}},
        )
        self.assertEqual(result["dossierUse"], "existing_dossier_update")
        self.assertIn(result["relationshipUse"], {"possible_connections", "needs_owner_review"})
        self.assertNotIn("confirmed", result["relationshipRead"].lower())

    def test_topic_extraction_labels_without_raw_transcript_quotes(self):
        result = enrich.classify_entity_activity({}, {"sourceCounts": {"conversations_by_author": 2}, "channelActivity": {"approvedMessageCount": 2, "themeTexts": ["working on the mix and master", "new mix needs production help"]}})
        self.assertIn("production", result["topicThemes"])
        self.assertNotIn("working on the mix", json.dumps(result))


    def test_entity_summary_called_with_structured_evidence_lane(self):
        fake_summary = {
            "knownContext": ["Structured context exists."],
            "rawProvenance": {"sourceLabels": ["entity_evidence_events/structured"], "sourceCounts": {"entity_evidence_events": 1}, "rawFragments": [{"table": "entity_evidence_events"}], "channelPolicies": {}},
            "missingInfo": [],
            "sourceAuthority": [],
        }
        with mock.patch.object(enrich, "refresh_entity_evidence_for_subject", return_value={"ok": True}) as refresh_mock, \
             mock.patch.object(enrich, "build_entity_activity_summary", return_value=fake_summary) as builder:
            result = enrich.collect_source_enrichment_evidence(self.db, 1, "HellcatNZ", lookup_result=self._lookup("active")({"lookupValue": "HellcatNZ"}))
        self.assertIn("entity_activity_summary", result["sourceTypes"])
        refresh_mock.assert_called_once_with(self.db, "HellcatNZ", guild_id=1, limit=50)
        lanes = builder.call_args.args[3]
        for lane in ("entity_evidence_events", "user_profiles", "user_memory_facts", "user_habits", "relationship_state", "relationship_journal", "memory_tiers", "conversations", "broadcast_memory", "community_presence", "rd_context"):
            self.assertIn(lane, lanes)

    def test_structured_entity_evidence_fields_flow_to_result_and_payload_privately(self):
        import bnl_entity_evidence as entity_evidence
        conn = sqlite3.connect(self.db)
        try:
            entity_evidence.ensure_entity_evidence_schema(conn)
            entity_evidence.upsert_entity_evidence_event(
                conn,
                guild_id=1,
                subject_name="HellcatNZ",
                matched_user_id=123456789012345678,
                source_type="discord",
                source_table="conversations",
                source_row_id="42",
                source_label="conversations/public_selective",
                channel_name="finished-tracks",
                channel_policy="public_selective",
                visibility="approved_public_side",
                authority="local_observed",
                confidence=0.8,
                relation_to_subject="authored",
                topic="music/community context",
                evidence_kind="authored_public_conversation",
                safe_summary="Approved public-side activity indicates music/community context for review.",
                public_safe_candidate=True,
                review_only=False,
                music_signal=True,
                community_signal=True,
                bnl_interaction=True,
                raw_ref_json={"content": "raw transcript with 123456789012345678 should stay provenance-only", "channel_name": "finished-tracks"},
            )
            entity_evidence.upsert_entity_evidence_event(
                conn,
                guild_id=1,
                subject_name="HellcatNZ",
                source_type="discord",
                source_table="conversations",
                source_row_id="43",
                source_label="conversations/private",
                channel_name="research-and-development",
                channel_policy="private_internal",
                visibility="review_only",
                authority="local_observed",
                confidence=0.6,
                relation_to_subject="mentioned",
                topic="source-file/dossier planning context",
                evidence_kind="mentioned_review_only_conversation",
                safe_summary="Review-only planning context exists for owner review.",
                public_safe_candidate=False,
                review_only=True,
                raw_ref_json={"content": "private raw transcript", "channel_name": "research-and-development"},
            )
            conn.commit()
        finally:
            conn.close()
        with mock.patch.object(enrich, "refresh_entity_evidence_for_subject", return_value={"ok": True}):
            result = enrich.run_source_file_enrichment(self.db, 1, "HellcatNZ", dry_run=True, lookup_func=self._lookup("active"))
        payload = result["payload"]
        for key in ("observedChannels", "conversationHighlights", "representativeEvidence", "topicBreakdown", "topChannels", "topTopicDetails", "activityFrequencySummary", "recentActivitySummary", "authoredVsMentionedSummary", "bestEvidenceToReview", "bnlInteractionSignals", "musicSignals", "communitySignals", "sourceCoverage", "evidenceDetails", "publicUseCandidates", "reviewOnlyEvidence", "queueSubmissionStatus", "queueSubmissionNote"):
            self.assertIn(key, result)
            self.assertIn(key, payload)
        self.assertIsInstance(payload["sourceCoverage"], list)
        self.assertTrue(any(item.get("source") == "conversation evidence" for item in payload["sourceCoverage"] if isinstance(item, dict)))
        self.assertEqual(payload["queueSubmissionStatus"], "not_connected")
        self.assertEqual(payload["queueSubmissionNote"], enrich.QUEUE_NOT_CONNECTED_NOTE)
        self.assertTrue(any("pending owner and admin review" in item for item in payload["publicUseCandidates"]))
        self.assertTrue(any("Review-only planning context" in item for item in payload["reviewOnlyEvidence"]))
        public_normal_text = json.dumps({key: payload.get(key) for key in ("observedChannels", "conversationHighlights", "representativeEvidence", "evidenceDetails", "bestEvidenceToReview", "publicUseCandidates")})
        self.assertNotIn("Review-only planning context", public_normal_text)
        normal = dict(payload)
        raw = normal.pop("rawProvenance")
        normal_text = json.dumps(normal)
        self.assertNotIn("private raw transcript", normal_text)
        self.assertNotIn("123456789012345678", normal_text)
        self.assertNotIn("research-and-development", normal_text)
        self.assertIn("rawFragments", raw)


    def test_source_enrich_payload_includes_real_review_safe_fact_extraction(self):
        self.conn.execute("INSERT INTO user_profiles VALUES (42,1,'Crow','Crow',NULL,NULL)")
        rows = [
            (42, 'Crow', 1, 'barcode-bot', 'public_home', 'user', 'Crow asks BNL for help with Orion and Suno song language; queue is not confirmed.', '2026-06-01'),
            (42, 'Crow', 1, 'barcode-bot', 'public_home', 'user', 'Crow follows up with BNL support around Orion and Suno track ideas in BARCODE community context.', '2026-06-02'),
            (42, 'Crow', 1, 'research-and-development', 'private_internal', 'user', 'Crow private relationship note mentions Orion and raw id 123456789012345678.', '2026-06-03'),
        ]
        self.conn.executemany("INSERT INTO conversations (user_id,user_name,guild_id,channel_name,channel_policy,role,content,timestamp) VALUES (?,?,?,?,?,?,?,?)", rows)
        self.conn.commit()

        with mock.patch.object(enrich, "refresh_entity_evidence_for_subject", return_value={"ok": True}):
            result = enrich.run_source_file_enrichment(self.db, 1, "Crow", dry_run=True, lookup_func=self._lookup("active"), force=True)
        payload = result["payload"]
        normal = dict(payload)
        raw = normal.pop("rawProvenance")
        normal_text = json.dumps(normal)

        self.assertNotIn("Recurring named topic", json.dumps(payload["topicBreakdown"] + payload["evidenceDetails"] + payload["bestEvidenceToReview"]))
        self.assertNotIn("Recurring named topic", normal_text)
        self.assertIn("Music discussion only", normal_text)
        self.assertIn("Conversation theme", json.dumps(payload["conversationHighlights"]))
        self.assertEqual(payload["queueSubmissionStatus"], "not_connected")
        self.assertIn(enrich.QUEUE_NOT_CONNECTED_NOTE, payload["queueSubmissionNote"])
        self.assertNotRegex(normal_text, r"submitted\s+\d+|played\s+\d+|Priority|payment|submitted songs from Suno")
        self.assertNotIn("123456789012345678", normal_text)
        self.assertNotIn("research-and-development", normal_text)
        self.assertIn("rawFragments", raw)

    def test_build_enrichment_payload_includes_new_fields_and_keeps_raw_provenance_separate(self):
        packet = {
            "subject": "HellcatNZ",
            "matchKind": "active_source_file",
            "sourceFile": {"id": "sf_1", "name": "HellcatNZ"},
            "sections": {"Public-Safe Notes": ["No public copy yet."], "Do Not Say": ["Do not publish raw notes."], "Missing Info": ["public links"], "Suggested Next Action": ["owner review"]},
            "sourceTypes": ["entity_activity_summary"],
            "sourceCounts": {"entity_activity_summary": 2},
            "sourceCoverage": [{"source": "entity_evidence_events", "count": 2, "status": "found"}],
            "knownContext": ["Structured context exists."],
            "observedChannels": ["#finished-tracks — approved public-side; subject authored."],
            "conversationHighlights": ["Sanitized highlight."],
            "topicBreakdown": ["Music/track-sharing discussion: 1 public-side authored row(s), mostly in #finished-tracks."],
            "representativeEvidence": ["#finished-tracks: authored music/track-sharing discussion."],
            "topChannels": [{"channel": "#finished-tracks", "count": 1}],
            "topTopicDetails": [{"topic": "Music/track-sharing discussion", "count": 1}],
            "activityFrequencySummary": {"approvedPublicAuthoredRows": 1, "approvedPublicMentionedRows": 0, "reviewOnlyEvidenceCount": 1, "mostRecentObservedAt": "2026-06-01"},
            "recentActivitySummary": "Most recent observed evidence timestamp: 2026-06-01.",
            "authoredVsMentionedSummary": "1 approved public-side authored row(s); 0 approved public-side mentioned row(s).",
            "bestEvidenceToReview": ["discord: authored_public_conversation — Sanitized highlight."],
            "bnlInteractionSignals": ["BNL-related interaction context exists."],
            "musicSignals": ["Music/radio/show context exists; queue identity still needs review."],
            "communitySignals": ["Community context exists; owner review must confirm wording."],
            "evidenceDetails": ["Sanitized detail."],
            "publicUseCandidates": ["Possible community/source-file candidate, pending owner/admin review."],
            "reviewOnlyEvidence": ["Review-only note."],
            "queueSubmissionStatus": "not_connected",
            "queueSubmissionNote": enrich.QUEUE_NOT_CONNECTED_NOTE,
            "rawProvenance": {"rawFragments": [{"rawRefJson": "raw transcript"}]},
        }
        payload = enrich.build_enrichment_recommendation_payload(packet, environ={})
        passthrough_keys = ("observedChannels", "conversationHighlights", "topChannels", "recentActivitySummary", "bnlInteractionSignals", "communitySignals", "evidenceDetails", "reviewOnlyEvidence", "queueSubmissionStatus", "queueSubmissionNote")
        for key in passthrough_keys:
            self.assertEqual(payload[key], packet[key])
        self.assertEqual(payload["activityFrequencySummary"]["postedCount"], 1)
        self.assertEqual(payload["activityFrequencySummary"]["mentionedCount"], 0)
        self.assertIn("2026-06-01", payload["activityFrequencySummary"]["recency"])
        self.assertIn("Review-only evidence count: 1", payload["activityFrequencySummary"]["context"])
        self.assertNotIn("approvedPublicAuthoredRows", json.dumps(payload))
        self.assertEqual(payload["authoredVsMentionedSummary"], "1 approved public-side posted item(s); 0 approved public-side mentioned item(s).")
        self.assertEqual(payload["sourceCoverage"], [{"source": "entity evidence events", "count": 2, "status": "found"}])
        self.assertEqual(payload["topicBreakdown"], ["music and track-sharing classification: 1 public-side authored item(s), mostly in #finished-tracks."])
        self.assertEqual(payload["representativeEvidence"], ["#finished-tracks: approved public-context item classified under music and track-sharing; review before using as a subject claim."])
        self.assertEqual(payload["topTopicDetails"], [{"topic": "music and track-sharing classification", "count": 1}])
        self.assertEqual(payload["bestEvidenceToReview"], ["discord: authored public conversation classification — Sanitized highlight."])
        self.assertEqual(payload["musicSignals"], ["Music and radio and show context exists; queue identity still needs review."])
        self.assertEqual(payload["publicUseCandidates"], ["Possible community and source-file candidate, pending owner and admin review."])
        self.assertEqual(payload["rawProvenance"], packet["rawProvenance"])
        normal = dict(payload)
        normal.pop("rawProvenance")
        self.assertNotIn("raw transcript", json.dumps(normal))

    def test_representative_evidence_payload_validation_catches_raw_metadata(self):
        issues = enrich.validate_enrichment_payload_for_site({
            "evidenceSummary": "summary",
            "rawProvenance": {},
            "representativeEvidence": [
                "#barcode-bot: authored_public_conversation row id=123456789012345678 BNL/source-file/dossier [object Object] profile_match"
            ],
        })
        self.assertTrue(any("representativeEvidence contains" in issue for issue in issues))

    def test_representative_evidence_and_topic_fields_are_sanitized_before_send(self):
        packet = {
            "subject": "Crow",
            "matchKind": "active_source_file",
            "sourceFile": {"id": "sf_1", "name": "Crow"},
            "sections": {"Public-Safe Notes": ["No public copy yet."], "Do Not Say": ["Do not publish raw notes."], "Missing Info": ["public links"], "Suggested Next Action": ["owner review"]},
            "topicBreakdown": ["BNL/source-file/dossier discussion: 1 public-side authored row(s), mostly in #barcode-bot."],
            "representativeEvidence": ["#barcode-bot: authored bnl/source-file/dossier discussion."],
            "topTopicDetails": [{"topic": "Event/riddle/engagement activity", "count": 2}],
            "queueSubmissionStatus": "not_connected",
            "queueSubmissionNote": enrich.QUEUE_NOT_CONNECTED_NOTE,
            "rawProvenance": {"sourceLabels": ["entity_evidence_events/structured"], "rawFragments": [{"rawRefJson": "raw BNL/source-file/dossier transcript", "sourceRowId": "99"}]},
        }
        payload = enrich.build_enrichment_recommendation_payload(packet, environ={})
        normal = dict(payload)
        raw = normal.pop("rawProvenance")
        normal_text = json.dumps(normal)
        rep_text = json.dumps(payload["representativeEvidence"])
        self.assertNotIn("/", rep_text)
        self.assertNotIn("authored", rep_text.lower())
        self.assertIn("BNL classified this approved public-context item", rep_text)
        self.assertIn("BNL source-file and dossier classification", json.dumps(payload["topicBreakdown"]))
        self.assertIn("event, riddle, or engagement classification", json.dumps(payload["topTopicDetails"]))
        self.assertNotIn("BNL/source-file/dossier", normal_text)
        self.assertNotIn("row(s)", normal_text)
        self.assertNotIn("sourceRowId", normal_text)
        self.assertIn("rawFragments", raw)
        self.assertEqual(payload["queueSubmissionStatus"], "not_connected")
        self.assertNotIn("siteValidationIssues", payload)

    def test_enrichment_payload_is_compacted_to_site_limits(self):
        long_item = "approved evidence " + ("x" * 3000) + " 123456789012345678"
        packet = {
            "subject": "HellcatNZ",
            "matchKind": "active_source_file",
            "sourceFile": {"id": "sf_1", "name": "HellcatNZ"},
            "sections": {
                "Subject Overview": [long_item for _ in range(20)],
                "History With BARCODE / BNL / Discord / BARCODE Radio": ["prior history", "prior interaction history with review context"],
                "Observed Patterns": ["community-side activity"],
                "Missing Info": ["public links"],
                "Suggested Next Action": ["owner review"],
            },
            "classification": {"primaryRole": "active_community_member", "activityLevel": "recurring", "dossierUse": "source_file_only"},
            "bestEvidenceToReview": [long_item for _ in range(40)],
            "observedChannels": ["#research-and-development 123456789012345678 " + ("x" * 2000) for _ in range(40)],
            "conversationHighlights": ["highlight " + ("x" * 2000) for _ in range(40)],
            "musicSignals": ["music signal"],
            "communitySignals": ["community signal"],
            "reviewOnlyEvidence": ["review-only note"],
            "sourceCoverage": [
                "entity evidence events checked",
                {"source": "conversations", "count": 30, "counts": {"public": 20, "review": 10}, "status": "found", "notes": long_item},
            ],
            "queueSubmissionStatus": "not_connected",
            "queueSubmissionNote": enrich.QUEUE_NOT_CONNECTED_NOTE,
            "rawProvenance": {
                "sourceLabels": ["entity_evidence_events/structured"],
                "sourceCounts": {"entity_evidence_events": 40},
                "channelPolicyCounts": {"public_safe": 20},
                "sourceAuthority": ["local_observed"],
                "rawFragments": [
                    {"table": "conversations", "snippet": "full raw transcript " + ("x" * 10000), "rawRefJson": {"content": "full raw transcript " + ("y" * 10000), "channel_name": "research-and-development"}}
                    for _ in range(20)
                ],
            },
        }
        payload = enrich.build_enrichment_recommendation_payload(packet, environ={})
        self.assertLessEqual(len(payload["evidenceSummary"]), 2000)
        self.assertLess(len(json.dumps(payload["rawProvenance"], sort_keys=True)), 20000)
        self.assertLessEqual(len(payload["rawProvenance"].get("rawFragments", [])), 4)
        for key in ("bestEvidenceToReview", "observedChannels", "conversationHighlights", "musicSignals", "communitySignals", "reviewOnlyEvidence", "sourceCoverage"):
            self.assertIn(key, payload)
            self.assertLessEqual(len(payload[key]), 25)
        self.assertEqual(payload["sourceCoverage"][0], "entity evidence events checked")
        self.assertEqual(payload["sourceCoverage"][1]["source"], "conversation evidence")
        self.assertEqual(payload["sourceCoverage"][1]["count"], 30)
        self.assertEqual(payload["sourceCoverage"][1]["counts"], {"public": 20, "review": 10})
        self.assertEqual(payload["sourceCoverage"][1]["status"], "found")
        self.assertNotIn("notes", payload["sourceCoverage"][1])
        self.assertEqual(payload["queueSubmissionStatus"], "not_connected")
        self.assertEqual(payload["queueSubmissionNote"], enrich.QUEUE_NOT_CONNECTED_NOTE)
        normal = dict(payload)
        normal.pop("rawProvenance", None)
        normal_text = json.dumps(normal)
        self.assertNotIn("123456789012345678", normal_text)
        self.assertNotIn("research-and-development", normal_text)
        self.assertNotIn("[object Object]", normal_text)
        self.assertFalse(enrich.validate_enrichment_payload_for_site(payload))

    def test_all_structured_readout_fields_normalize_to_website_contract(self):
        payload = {
            "evidenceSummary": "summary",
            "representativeEvidence": ["#barcode-bot: authored music/track-sharing discussion."],
            "activityFrequencySummary": {
                "approvedPublicAuthoredRows": 12,
                "approvedPublicMentionedRows": 3,
                "reviewOnlyEvidenceCount": 7,
                "mostRecentObservedAt": "2026-06-03T21:24:00",
            },
            "topChannels": [
                {"channel": "#barcode-bot", "channelId": "123456789012345678", "count": 6},
                {"channel": "research-and-development", "count": 2},
            ],
            "topTopicDetails": [{"topic": "Music/track-sharing discussion", "rawTaxonomy": "Music/track-sharing", "count": 4}],
            "recentActivitySummary": {"mostRecentObservedAt": "2026-06-03T21:24:00", "sourceTable": "entity_evidence_events"},
            "authoredVsMentionedSummary": {"approvedPublicAuthoredRows": 12, "approvedPublicMentionedRows": 3},
            "conversationHighlights": [{"summary": "Highlight only.", "sourceRowId": "123456789012345678"}],
            "topicBreakdown": ["BNL/source-file/dossier discussion: 4 items."],
            "evidenceDetails": [{"detail": "Safe detail.", "sourceLabel": "entity_evidence_events/structured"}],
            "bestEvidenceToReview": [{"summary": "Safe review detail.", "source_table": "conversations"}],
            "musicSignals": [{"summary": "Music/radio/show context exists."}],
            "communitySignals": [{"summary": "Community/server participation exists."}],
            "bnlInteractionSignals": [{"summary": "BNL/source-file/dossier context exists."}],
            "sourceCoverage": ["entity_evidence_events checked", {"source": "conversations", "count": 2, "status": "found"}],
            "queueSubmissionStatus": "not_connected",
            "rawProvenance": {"sourceLabels": ["entity_evidence_events/structured"], "rawFragments": [{"sourceRowId": "123456789012345678", "channel_name": "research-and-development"}]},
        }
        compact = enrich.compact_enrichment_payload_for_site(payload)
        normal = dict(compact)
        raw = normal.pop("rawProvenance")
        normal_text = json.dumps(normal)
        for raw_key in ("approvedPublicAuthoredRows", "approvedPublicMentionedRows", "reviewOnlyEvidenceCount", "mostRecentObservedAt", "sourceRowId", "sourceLabel", "sourceTable", "channelId"):
            self.assertNotIn(raw_key, normal_text)
        self.assertEqual(compact["activityFrequencySummary"]["postedCount"], 12)
        self.assertEqual(compact["activityFrequencySummary"]["mentionedCount"], 3)
        self.assertIn("Review-only evidence count: 7", compact["activityFrequencySummary"]["context"])
        self.assertIn("2026-06-03T21:24:00", compact["activityFrequencySummary"]["recency"])
        self.assertEqual(compact["authoredVsMentionedSummary"], {"summary": "Posted and mentioned balance.", "postedCount": 12, "mentionedCount": 3})
        self.assertTrue(all(set(item) <= enrich.SITE_READOUT_ALLOWED_OBJECT_KEYS for item in compact["topChannels"] if isinstance(item, dict)))
        self.assertTrue(all(set(item) <= enrich.SITE_READOUT_ALLOWED_OBJECT_KEYS for item in compact["topTopicDetails"] if isinstance(item, dict)))
        self.assertNotIn("Music/track-sharing", normal_text)
        self.assertNotIn("BNL/source-file/dossier", normal_text)
        self.assertNotIn("research-and-development", normal_text)
        self.assertNotIn("entity_evidence_events", normal_text)
        self.assertNotIn("conversations", normal_text)
        self.assertFalse(enrich.validate_enrichment_payload_for_site(compact))
        self.assertIn("rawFragments", raw)
        self.assertEqual(compact["queueSubmissionStatus"], "not_connected")

    def test_readout_validation_flags_unsupported_keys_across_contract_fields(self):
        for field in enrich.SITE_CONTRACT_READOUT_FIELDS:
            if field == "sourceCoverage":
                payload = {"evidenceSummary": "summary", "rawProvenance": {}, field: [{"source": "conversation evidence", "count": 1, "unsupported": "x"}]}
            elif field in {"activityFrequencySummary", "recentActivitySummary", "authoredVsMentionedSummary"}:
                payload = {"evidenceSummary": "summary", "rawProvenance": {}, field: {"summary": "safe", "unsupported": "x"}}
            else:
                payload = {"evidenceSummary": "summary", "rawProvenance": {}, field: [{"summary": "safe", "unsupported": "x"}]}
            issues = enrich.validate_enrichment_payload_for_site(payload)
            self.assertTrue(any("unsupported object keys" in issue for issue in issues), field)

    def test_source_coverage_compactor_matches_site_contract(self):
        payload = {
            "evidenceSummary": "compact",
            "sourceCoverage": [
                "source file lookup evidence checked",
                {"source": "conversations", "count": 3, "status": "found", "notes": "drop me"},
                {"source": "entity_evidence_events", "counts": {"public": 2, "review_only": 1}, "status": "found", "rawTranscript": "drop me too"},
            ],
            "rawProvenance": {},
        }
        compact = enrich.compact_enrichment_payload_for_site(payload)
        self.assertEqual(compact["sourceCoverage"][0], "source file lookup evidence checked")
        self.assertEqual(compact["sourceCoverage"][1], {"source": "conversation evidence", "count": 3, "status": "found"})
        self.assertEqual(compact["sourceCoverage"][2], {"source": "entity evidence events", "counts": {"public": 2, "review only": 1}, "status": "found"})
        self.assertNotIn("notes", compact["sourceCoverage"][1])
        self.assertNotIn("rawTranscript", compact["sourceCoverage"][2])
        for item in compact["sourceCoverage"]:
            if isinstance(item, dict):
                self.assertLessEqual(set(item), {"source", "count", "counts", "status"})
        self.assertFalse(enrich.validate_enrichment_payload_for_site(compact))
        self.assertNotIn("[object Object]", json.dumps(compact))

    def test_source_coverage_validation_flags_uncompacted_contract_violations(self):
        payload = {
            "evidenceSummary": "compact",
            "sourceCoverage": [
                {"source": "conversations", "count": 3, "status": "found", "notes": "unsupported"},
                {"source": "bad", "counts": {"private": float("inf")}, "status": "found"},
            ],
            "rawProvenance": {},
        }
        issues = enrich.validate_enrichment_payload_for_site(payload)
        self.assertTrue(any("unsupported object keys" in issue for issue in issues))
        count_issues = enrich.validate_enrichment_payload_for_site({"evidenceSummary": "compact", "sourceCoverage": [{"source": "bad", "count": float("inf"), "status": "found"}], "rawProvenance": {}})
        self.assertTrue(any("count must be finite" in issue for issue in count_issues))
        counts_issues = enrich.validate_enrichment_payload_for_site({"evidenceSummary": "compact", "sourceCoverage": [{"source": "bad", "counts": {"private": float("inf")}, "status": "found"}], "rawProvenance": {}})
        self.assertTrue(any("counts values must be finite" in issue for issue in counts_issues))
        compact = enrich.compact_enrichment_payload_for_site(payload)
        self.assertFalse(enrich.validate_enrichment_payload_for_site(compact))
        self.assertNotIn("notes", compact["sourceCoverage"][0])
        self.assertNotIn("counts", compact["sourceCoverage"][1])

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

    def test_source_file_payload_dedupes_link_signal_categories_without_queue_claims(self):
        self.conn.execute("INSERT INTO user_profiles VALUES (77,1,'Antigrain','Antigrain',NULL,NULL)")
        links = [
            "https://youtu.be/8C4lK41SX-Q?si=one",
            "https://youtu.be/PBwAxmrE194?si=two",
            "https://youtu.be/rrbFQEcpJ3A?si=three",
        ]
        for link in links:
            self.conn.execute(
                "INSERT INTO conversations VALUES (NULL,77,'Antigrain',1,'barcode-bot','public_home','user',?,'now')",
                (f"Antigrain shared a YouTube link {link}",),
            )
            self.conn.execute(
                "INSERT INTO memory_tiers VALUES (NULL,77,1,'long',?,0.9,1,'now','public_discord_observed','public_home')",
                (f"Antigrain local stored context repeats {link}",),
            )
        self.conn.commit()
        with mock.patch.object(enrich, "refresh_entity_evidence_for_subject", return_value={"ok": True}):
            result = enrich.run_source_file_enrichment(self.db, 1, "Antigrain", dry_run=True, lookup_func=self._lookup("active"), force=True)
        payload = result["payload"]
        diagnostics = result.get("linkSignalDiagnostics") or {}
        text = json.dumps({key: payload.get(key) for key in ("musicSignals", "communitySignals", "usefulEvidence", "evidenceDetails", "reviewOnlyEvidence", "recommendedAction", "queueSubmissionStatus")})
        self.assertEqual(diagnostics.get("videoPlatformLinkCount"), 3)
        self.assertEqual(diagnostics.get("musicPlatformLinkCount"), 0)
        self.assertEqual(diagnostics.get("derivedDuplicateLinkCount"), 3)
        self.assertIn("Video platform links: 3", text)
        self.assertIn("Derived duplicate references suppressed: 3", text)
        self.assertNotIn("shares music links", text)
        self.assertNotIn("submitted songs", text.lower())
        self.assertEqual(payload.get("queueSubmissionStatus"), "not_connected")


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

    def test_classification_diagnostics_update_from_source_enrichment(self):
        bnl01_bot.BNL_OWNER_USER_ID = 999
        message = self.Message(user_id=999)
        fake = {
            "ok": True,
            "dryRun": True,
            "subject": "HellcatNZ",
            "status": "dry_run",
            "matchKind": "active_source_file",
            "sections": {"Subject Overview": ["x"]},
            "sourceCounts": {"source_file_lookup": 1, "conversations_by_author": 3},
            "warningCounts": {},
            "sourceTypes": ["source_file_lookup", "conversations_by_author"],
            "warnings": [],
            "classification": {
                "primaryRole": "active_community_member",
                "activityLevel": "recurring",
                "dossierUse": "source_file_only",
                "sourceConfidence": "low-medium",
                "missingInfo": ["confirm public role"],
            },
        }
        with mock.patch.object(bnl01_bot, "run_source_file_enrichment", return_value=fake):
            handled = asyncio.run(bnl01_bot.maybe_handle_source_file_enrichment_command(message, "!bnl source enrich HellcatNZ | dry_run=true"))
        self.assertTrue(handled)
        diag = bnl01_bot.build_dossier_recommendation_diagnostics(1)
        self.assertTrue(diag["classification_available"])
        self.assertEqual(diag["classification_last_subject"], "HellcatNZ")
        self.assertEqual(diag["classification_last_primary_role"], "active_community_member")
        self.assertEqual(diag["classification_last_activity_level"], "recurring")
        self.assertEqual(diag["classification_last_dossier_use"], "source_file_only")
        self.assertEqual(diag["classification_last_confidence"], "low-medium")
        self.assertEqual(diag["classification_last_missing_info_count"], 1)



    def test_entity_context_resolver_parser_accepts_testing_forms(self):
        self.assertEqual(bnl01_bot._parse_rd_entity_context_subject("entity intelligence for Crow"), "Crow")
        self.assertEqual(bnl01_bot._parse_rd_entity_context_subject("entity context Lost Marbles"), "Lost Marbles")
        self.assertEqual(bnl01_bot._parse_rd_entity_context_subject("context resolver for Antigrain"), "Antigrain")
        response = bnl01_bot._format_rd_entity_context_response("Crow", {
            "allowedRoles": [{"label": "artist"}],
            "allowedRelationships": [{"objectLabel": "Orion", "relationType": "speaks_through"}],
            "allowedActivity": [{"label": "frequent BNL-facing conversation"}],
            "allowedActionItems": [],
            "missingInfo": ["queue submission bridge not_connected"],
        })
        self.assertIn("Entity context resolver", response)
        self.assertIn("Orion: speaks_through", response)
        self.assertNotIn("suspicious blinking light", response.lower())

    def test_source_payload_validation_removes_blocked_bad_labels(self):
        packet = {
            "subject": "Crow",
            "matchKind": "active_source_file",
            "sourceFile": {"id": "sf_1", "name": "Crow"},
            "sections": {"Public-Safe Notes": ["review only"], "Do Not Say": ["bad labels"], "Missing Info": ["missing"], "Suggested Next Action": ["review"]},
            "sourceTypes": ["entity_activity_summary"],
            "sourceCounts": {},
            "warningCounts": {},
            "warnings": [],
            "qualityScore": 80,
            "qualityStatus": "sendable",
            "relationshipSignals": ["Detected relationship facet: Orion: speaks_through", "Detected relationship facet: not: speaks_through"],
            "topicBreakdown": ["entire civilizations communicate purely"],
            "bestEvidenceToReview": ["Confirm whether The tradeoff relationship/context can be public."],
            "publicUseCandidates": ["Precision matters"],
            "reviewOnlyEvidence": ["trap"],
            "conversationHighlights": ["Conversation theme: safe music"],
            "evidenceDetails": ["The value"],
            "knownContext": ["Detected role facet: artist"],
            "usefulEvidence": ["sustained analytical input"],
        }
        payload = enrich.build_enrichment_recommendation_payload(packet)
        blocked = ("entire civilizations communicate purely", "not: speaks_through", "The tradeoff", "Precision matters", "The value", "trap", "sustained analytical input")
        payload_text = json.dumps({key: payload.get(key) for key in ("topicBreakdown", "bestEvidenceToReview", "relationshipSignals", "publicUseCandidates", "reviewOnlyEvidence", "conversationHighlights", "evidenceDetails", "knownContext", "usefulEvidence")})
        for label in blocked:
            self.assertNotIn(label, payload_text)
        self.assertIn("Orion: speaks_through", payload_text)


if __name__ == "__main__":
    unittest.main()


class SourceFileEntityIntelligenceIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False)
        self.tmp.close()
        self.db = self.tmp.name
        self.conn = sqlite3.connect(self.db)
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

    def tearDown(self):
        self.conn.close()
        os.unlink(self.db)

    def lookup(self, query):
        return {"ok": True, "found": True, "matchKind": "exact", "data": {"sourceFile": {"id": "sf_1", "name": query["lookupValue"], "status": "active", "candidateType": "entity"}}}

    def test_source_file_payload_includes_entity_intelligence_without_raw_material_or_queue_claims(self):
        c = self.conn.cursor()
        c.execute("INSERT INTO user_profiles VALUES (21,1,'Crow','Crow',NULL,NULL)")
        c.execute("INSERT INTO conversations VALUES (NULL,21,'Crow',1,'finished-tracks','public_music','user','Orion through Crow. Crow created Orion. BNL source-file boundaries and Suno music link https://suno.com/song/raw-path should be reviewed, not a submitted song. 123456789012345678','now')")
        c.execute("INSERT INTO conversations VALUES (NULL,21,'Crow',1,'contest-room','public_home','user','Crow helps with community event contest rules and asks for feedback on a track.','now')")
        self.conn.commit()
        result = enrich.run_source_file_enrichment(self.db, 1, "Crow", dry_run=True, lookup_func=self.lookup)
        payload = result["payload"]
        profile = payload.get("entityIntelligenceProfile") or {}
        public_payload = dict(payload)
        public_payload.pop("rawProvenance", None)
        joined = json.dumps(public_payload, sort_keys=True)
        self.assertTrue(profile)
        self.assertIn("Detected role facet", json.dumps(payload.get("knownContext", [])))
        self.assertIn("Orion", json.dumps(payload.get("relationshipSignals", [])))
        self.assertIn("Suno", json.dumps(payload.get("musicSignals", [])))
        self.assertIn("BNL", json.dumps(payload.get("bnlInteractionSignals", [])))
        self.assertIn("Conversation theme", json.dumps(payload.get("conversationHighlights", [])))
        self.assertIn("contest", json.dumps(profile.get("eventContest", [])).lower())
        self.assertIn("Confirm", json.dumps(payload.get("bestEvidenceToReview", [])))
        self.assertIn("official_public_dossier source not connected yet", json.dumps(payload.get("missingInfo", [])))
        self.assertEqual(payload.get("queueSubmissionStatus"), "not_connected")
        self.assertNotIn("123456789012345678", joined)
        self.assertNotIn("rawRefJson", joined)
        self.assertNotIn("sourceRowId", joined)
        self.assertNotIn("entity_evidence_events", joined)
        self.assertNotIn("/song/raw-path", joined)
        self.assertNotIn("[object Object]", joined)
        self.assertNotIn("payment", joined.lower())
        self.assertNotIn("priority", joined.lower())
        self.assertNotIn("confirmed submission", joined.lower())
        self.assertTrue(result.get("subjectIntelligenceDiagnostics", {}).get("entityIntelligence"))
        response = enrich.format_source_enrichment_response(result)
        self.assertIn("Entity intelligence ledger/profile", response)
        self.assertIn("Resolver surface: source_file", response)
        self.assertLessEqual(len(response), 1900)

    def test_source_file_archive_payload_includes_memory_packet_and_case_report(self):
        packet = {
            "subject": "Signal Fox",
            "sourceFile": {"name": "Signal Fox", "candidateId": "sf_signal_fox"},
            "sections": {"Subject Overview": ["Signal Fox has review-only source evidence."]},
            "sourceCounts": {"conversations": 1},
            "sourceTypes": ["conversations"],
            "warningCounts": {},
            "warnings": [],
            "qualityScore": 88,
            "qualityStatus": "sendable",
            "matchKind": "exact",
            "runTime": "2026-06-10T00:00:00+00:00",
        }
        archive = enrich.build_source_file_archive_payload(packet)
        self.assertIsInstance(archive.get("subjectMemoryPacketV1"), dict)
        self.assertIsInstance(archive.get("sourceFileCaseReportV1"), dict)
        self.assertEqual(archive["subjectMemoryPacketV1"].get("subjectName"), "Signal Fox")

    def test_compact_recommendation_payload_excludes_full_memory_packet_and_case_report(self):
        payload = {
            "subjectName": "Signal Fox",
            "evidenceSummary": "short summary",
            "subjectMemoryPacketV1": {"full": "memory"},
            "sourceFileCaseReportV1": {"full": "case report"},
        }
        compact = enrich.sanitize_compact_recommendation_payload(payload, packet={"subject": "Signal Fox", "matchKind": "exact"}, archive_id="arc_1")
        self.assertNotIn("subjectMemoryPacketV1", compact)
        self.assertNotIn("sourceFileCaseReportV1", compact)
        self.assertIn("compactRecommendationNotice", compact)
