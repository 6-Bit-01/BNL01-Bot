import json
import logging
import os
import sqlite3
import tempfile
import unittest

from bnl_population_recommender import (
    POPULATION_RECOMMENDATION_SCHEMA_FIELDS,
    build_population_recommendation,
    build_population_recommendation_payload,
    collect_shared_memory_population_records,
    format_population_scan_response,
    parse_population_scan_command,
    run_population_scan,
)


class PopulationRecommenderTests(unittest.TestCase):
    def mid_records(self, name="Signal Weaver"):
        return [
            {"subjectName": name, "memoryTier": "mid_term", "sourceLane": "broadcast_memory", "summary": "Recurring broadcast contribution signal", "sourceEvidenceRef": "broadcast:1", "rawEvidenceRef": "broadcast_memory:1"},
            {"subjectName": name, "memoryTier": "mid_term", "sourceLane": "rd_context", "summary": "Repeated R&D review mention", "sourceEvidenceRef": "rd:2", "rawEvidenceRef": "rd_context:2"},
        ]

    def test_schema_exists(self):
        expected = {"recommendationId", "generatedAt", "generatedBy", "subjectName", "normalizedSubjectKey", "recommendedLane", "recommendedAction", "confidence", "sourceEvidenceRefs", "rawEvidenceRefs", "adminSummary", "doNotPublishReason"}
        self.assertTrue(expected.issubset(set(POPULATION_RECOMMENDATION_SCHEMA_FIELDS)))

    def test_new_subject_candidate_from_repeated_mid_term(self):
        rec = build_population_recommendation(self.mid_records())
        self.assertEqual(rec["recommendedLane"], "candidate_intake")
        self.assertEqual(rec["recommendedAction"], "create_source_file_candidate")
        self.assertEqual(rec["confidence"], "medium")

    def test_routes_to_existing_source_file_exact_key(self):
        rec = build_population_recommendation(self.mid_records("Known Artist"), existing_candidates=[{"id": "sf-1", "subjectKey": "known-artist", "name": "Known Artist"}])
        self.assertEqual(rec["recommendedLane"], "active_source_file")
        self.assertEqual(rec["recommendedAction"], "attach_to_existing_source_file")
        self.assertEqual(rec["matchedExistingCandidateId"], "sf-1")
        self.assertEqual(rec["confidence"], "high")

    def test_routes_to_existing_dossier_update_workspace_by_public_id(self):
        records = self.mid_records("Dossier Subject")
        records[0]["publicDossierId"] = "pd-1"
        rec = build_population_recommendation(records, public_dossiers=[{"id": "pd-1", "name": "Dossier Subject"}], dossier_update_workspaces=[{"id": "du-1", "publicDossierId": "pd-1", "name": "Dossier Subject"}])
        self.assertEqual(rec["recommendedLane"], "existing_dossier_update")
        self.assertEqual(rec["recommendedAction"], "attach_to_existing_dossier_update")
        self.assertEqual(rec["matchedPublicDossierId"], "pd-1")

    def test_recommends_dossier_update_workspace_when_signal_has_no_workspace(self):
        records = self.mid_records("Dossier Subject")
        records[0].update({"publicDossierId": "pd-2", "publicDossierUpdateSignal": True})
        rec = build_population_recommendation(records, public_dossiers=[{"id": "pd-2", "name": "Dossier Subject"}], dossier_update_workspaces=[])
        self.assertEqual(rec["recommendedLane"], "public_dossier_update_signal")
        self.assertEqual(rec["recommendedAction"], "create_dossier_update_workspace")

    def test_confirmed_alias_avoids_duplicate_source_file(self):
        rec = build_population_recommendation(self.mid_records("Internal Alias"), existing_candidates=[{"id": "sf-2", "subjectKey": "canonical-subject", "name": "Canonical Subject"}], confirmed_aliases={"internal-alias": {"candidateId": "sf-2", "canonicalName": "Canonical Subject"}})
        self.assertEqual(rec["recommendedLane"], "identity_review")
        self.assertEqual(rec["recommendedAction"], "suggest_identity_link")
        self.assertNotEqual(rec["recommendedAction"], "create_source_file_candidate")

    def test_similar_name_needs_review_not_auto_merge(self):
        rec = build_population_recommendation(self.mid_records("Signal Weaver Jr"), existing_candidates=[{"id": "sf-3", "subjectKey": "signal-weaver", "name": "Signal Weaver"}])
        self.assertEqual(rec["recommendedLane"], "needs_population_review")
        self.assertEqual(rec["recommendedAction"], "admin_review_required")
        self.assertEqual(rec["confidence"], "low")

    def test_conflicting_public_dossier_matches_blocked(self):
        rec = build_population_recommendation(self.mid_records("Mirror Name"), public_dossiers=[{"id": "pd-a", "subjectKey": "mirror-name", "name": "Mirror Name A"}, {"id": "pd-b", "subjectKey": "mirror-name", "name": "Mirror Name B"}])
        self.assertEqual(rec["confidence"], "blocked")
        self.assertEqual(rec["recommendedAction"], "admin_review_required")

    def test_diagnostic_artifacts_classified(self):
        rec = build_population_recommendation([{"subjectName": "Fixture Subject", "memoryTier": "short_term", "sourceLane": "diagnostic_test", "summary": "diagnostic fixture", "sourceEvidenceRef": "test:1", "rawEvidenceRef": "test:1"}])
        self.assertEqual(rec["recommendedLane"], "diagnostic_artifact")
        self.assertEqual(rec["recommendedAction"], "archive_diagnostic")

    def test_preserves_refs_and_includes_admin_summary(self):
        rec = build_population_recommendation(self.mid_records())
        self.assertIn("broadcast:1", rec["sourceEvidenceRefs"])
        self.assertIn("broadcast memory:1", " ".join(rec["rawEvidenceRefs"]).replace("_", " "))
        self.assertIsInstance(rec["adminSummary"], dict)

    def test_admin_summary_sanitizes_raw_blobs_and_unknown_chains(self):
        records = self.mid_records("Raw Blob")
        records[0]["summary"] = "relationship_journal unknown -> unknown rawRefJson alias: secret-name"
        rec = build_population_recommendation(records)
        summary_text = json.dumps(rec["adminSummary"], sort_keys=True)
        self.assertNotIn("relationship_journal", summary_text)
        self.assertNotIn("rawRefJson", summary_text)
        self.assertNotIn("unknown -> unknown", summary_text.lower())
        self.assertNotIn("secret-name", summary_text)

    def test_internal_alias_not_exposed_as_public_fact(self):
        rec = build_population_recommendation(self.mid_records("Alias Subject"), confirmed_aliases={"alias-subject": {"candidateId": "sf-4", "canonicalName": "Canonical", "internalAlias": "Secret Alias"}})
        text = json.dumps(rec, sort_keys=True)
        self.assertNotIn("Secret Alias", text)
        self.assertIn("Do not expose internal aliases", json.dumps(rec["adminSummary"], sort_keys=True))

    def test_private_admin_evidence_marked_do_not_publish(self):
        records = self.mid_records("Private Subject")
        records[0]["visibility"] = "admin_private"
        rec = build_population_recommendation(records)
        self.assertEqual(rec["confidence"], "blocked")
        self.assertIn("private/admin/test memory", rec["doNotPublishReason"])

    def test_does_not_publish_or_edit_public_text(self):
        rec = build_population_recommendation(self.mid_records())
        payload = build_population_recommendation_payload(rec)
        self.assertNotIn("publish", payload.get("suggestedAction", "").lower())
        self.assertIn("populationRecommendation", payload)
        self.assertIn("Do not publish", json.dumps(payload))

    def test_dry_run_logs_and_does_not_send(self):
        calls = []
        def sender(payload, **kwargs):
            calls.append(payload)
            return {"ok": True}
        with self.assertLogs(level="INFO") as cm:
            result = run_population_scan(memory_records=self.mid_records(), dry_run=True, sender=sender)
        self.assertEqual(calls, [])
        self.assertEqual(result["counts"]["dry_run"], 1)
        self.assertIn("population_recommendation_dry_run", "\n".join(cm.output))

    def test_non_dry_run_uses_sender_pipeline(self):
        calls = []
        def sender(payload, **kwargs):
            calls.append(payload)
            return {"ok": True, "recommendationId": "site-1"}
        result = run_population_scan(memory_records=self.mid_records(), dry_run=False, sender=sender)
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0]["type"], "population_recommendation")
        self.assertEqual(result["counts"]["sent_to_site"], 1)

    def test_logs_counts(self):
        with self.assertLogs(level="INFO") as cm:
            result = run_population_scan(memory_records=self.mid_records(), dry_run=True)
        self.assertEqual(result["counts"]["scanned"], 2)
        logs = "\n".join(cm.output)
        self.assertIn("population_scan_started", logs)
        self.assertIn("population_scan_completed", logs)

    def test_parse_population_scan_defaults_safe(self):
        matched, options, error = parse_population_scan_command("!bnl population scan now max=10 min=medium subject=Signal")
        self.assertTrue(matched)
        self.assertEqual(error, "")
        self.assertTrue(options["dry_run"])
        self.assertEqual(options["max_items"], 10)
        self.assertEqual(options["min_confidence"], "medium")

    def _temp_db(self):
        tmp = tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False)
        tmp.close()
        self.addCleanup(lambda: os.path.exists(tmp.name) and os.unlink(tmp.name))
        return tmp.name

    def test_collection_failure_logs_and_returns_failed_summary(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with self.assertLogs(level="ERROR") as cm:
                result = run_population_scan(db_path=tmpdir, dry_run=True)
        logs = "\n".join(cm.output)
        self.assertFalse(result["ok"])
        self.assertEqual(result["error"], "population_scan_collect_failed")
        self.assertIn("population_scan_collect_failed", logs)
        self.assertIn("exception_type=", logs)
        self.assertIn("exception_message=", logs)
        self.assertIn("db_path=", logs)
        self.assertIn("guild_id=", logs)
        self.assertIn("source_lanes=", logs)
        self.assertIn("subject_filter=", logs)
        response = format_population_scan_response(result)
        self.assertIn("failed during memory collection", response)
        self.assertIn("No site records were created", response)
        self.assertNotIn("complete", response.splitlines()[0].lower())

    def test_empty_collection_returns_ok_warning(self):
        db = self._temp_db()
        result = run_population_scan(db_path=db, dry_run=True)
        self.assertTrue(result["ok"])
        self.assertEqual(result["counts"]["errors"], 0)
        self.assertEqual(result["warning"], "no eligible memory records found")
        self.assertIn("No eligible memory records found", format_population_scan_response(result))

    def test_missing_optional_tables_and_columns_do_not_fail(self):
        db = self._temp_db()
        conn = sqlite3.connect(db)
        conn.execute("CREATE TABLE broadcast_memory (id INTEGER PRIMARY KEY, guild_id INTEGER, status TEXT)")
        conn.execute("INSERT INTO broadcast_memory VALUES (1,1,'approved')")
        conn.commit(); conn.close()
        with self.assertLogs(level="INFO") as cm:
            result = run_population_scan(db_path=db, guild_id=1, dry_run=True, diagnostics=True)
        self.assertTrue(result["ok"])
        logs = "\n".join(cm.output)
        self.assertIn("population_scan_table_checked table=source_file_notes exists=0", logs)
        self.assertIn("population_scan_table_checked table=broadcast_memory exists=1", logs)

    def test_collects_real_memory_source_tables(self):
        db = self._temp_db()
        conn = sqlite3.connect(db)
        conn.execute("CREATE TABLE user_profiles (user_id INTEGER, guild_id INTEGER, display_name TEXT, preferred_name TEXT)")
        conn.execute("INSERT INTO user_profiles VALUES (42,1,'Crow','Crow')")
        conn.execute("CREATE TABLE conversations (id INTEGER, guild_id INTEGER, channel_id INTEGER, channel_name TEXT, channel_policy TEXT, user_id INTEGER, user_name TEXT, role TEXT, content TEXT, timestamp TEXT)")
        conn.execute("INSERT INTO conversations VALUES (1,1,10,'home','public_home',42,'Crow','user','helpful public context','2026-06-01T00:00:00+00:00')")
        conn.execute("INSERT INTO conversations VALUES (2,1,10,'admin','private_admin',42,'Crow','user','private admin text','2026-06-01T00:00:00+00:00')")
        conn.execute("INSERT INTO conversations VALUES (3,1,10,'sealed','sealed_test',42,'Crow','user','sealed text','2026-06-01T00:00:00+00:00')")
        conn.execute("CREATE TABLE user_memory_facts (id INTEGER, user_id INTEGER, guild_id INTEGER, fact_key TEXT, fact_value TEXT, confidence TEXT, is_core INTEGER, updated_at TEXT)")
        conn.execute("INSERT INTO user_memory_facts VALUES (4,42,1,'skill','maps','high',1,'2026-06-01')")
        conn.execute("CREATE TABLE relationship_journal (id INTEGER, user_id INTEGER, guild_id INTEGER, entry_type TEXT, summary TEXT, timestamp TEXT)")
        conn.execute("INSERT INTO relationship_journal VALUES (5,42,1,'note','relationship_journal rawRefJson alias: secret-name unknown -> unknown', '2026-06-01')")
        conn.execute("CREATE TABLE relationship_state (user_id INTEGER, guild_id INTEGER, interaction_count INTEGER, affinity_score REAL, trust_stage TEXT, social_stance TEXT, last_topic TEXT, updated_at TEXT)")
        conn.execute("INSERT INTO relationship_state VALUES (42,1,3,0.5,'known','friendly','maps','2026-06-01')")
        conn.execute("CREATE TABLE broadcast_memory (id INTEGER, guild_id INTEGER, cleaned_summary TEXT, status TEXT, public_safe INTEGER, usage_scope TEXT)")
        conn.execute("INSERT INTO broadcast_memory VALUES (6,1,'broadcast signal','approved',1,'review_only')")
        conn.execute("CREATE TABLE memory_tiers (id INTEGER, guild_id INTEGER, subject_name TEXT, tier TEXT, summary TEXT)")
        conn.execute("INSERT INTO memory_tiers VALUES (7,1,'Crow','mid_term','tier signal')")
        conn.execute("CREATE TABLE community_presence (guild_id INTEGER, subject_key TEXT, display_name TEXT, last_topic TEXT)")
        conn.execute("INSERT INTO community_presence VALUES (1,'crow','Crow','presence signal')")
        conn.commit(); conn.close()

        records = collect_shared_memory_population_records(db, 1, max_items=50)
        lanes = [r["sourceLane"] for r in records]
        self.assertIn("conversations", lanes)
        self.assertIn("user_memory_facts", lanes)
        self.assertIn("relationship_journal", lanes)
        self.assertIn("relationship_state", lanes)
        self.assertIn("broadcast_memory", lanes)
        self.assertIn("memory_tiers", lanes)
        self.assertIn("community_presence", lanes)
        self.assertNotIn("private admin text", json.dumps(records))
        self.assertNotIn("sealed text", json.dumps(records))
        rec = build_population_recommendation([r for r in records if r["sourceLane"] == "relationship_journal"] + self.mid_records("Crow"))
        self.assertNotIn("secret-name", json.dumps(rec["adminSummary"]))
        self.assertNotIn("unknown -> unknown", json.dumps(rec["adminSummary"]).lower())

        sealed = collect_shared_memory_population_records(db, 1, max_items=50, allow_sealed_test=True)
        self.assertIn("sealed text", json.dumps(sealed))

    def test_dry_run_counts_and_send_mode_pathway(self):
        calls = []
        result = run_population_scan(memory_records=self.mid_records(), dry_run=True, sender=lambda payload, **kw: calls.append(payload) or {"ok": True})
        self.assertEqual(result["counts"]["scanned"], 2)
        self.assertEqual(result["counts"]["dry_run"], 1)
        self.assertEqual(result["counts"]["sent_to_site"], 0)
        self.assertEqual(calls, [])
        sent = run_population_scan(memory_records=self.mid_records(), dry_run=False, sender=lambda payload, **kw: calls.append(payload) or {"ok": True})
        self.assertEqual(sent["counts"]["sent_to_site"], 1)

    def test_parse_diagnostics_command(self):
        matched, options, error = parse_population_scan_command("!bnl population scan now diagnostics")
        self.assertTrue(matched)
        self.assertEqual(error, "")
        self.assertTrue(options["diagnostics"])
        self.assertTrue(options["dry_run"])


    def _quota_db(self):
        db = self._temp_db()
        conn = sqlite3.connect(db)
        conn.execute("CREATE TABLE user_profiles (user_id INTEGER, guild_id INTEGER, display_name TEXT)")
        conn.execute("INSERT INTO user_profiles VALUES (42,1,'Profile Crow')")
        conn.execute("CREATE TABLE conversations (id INTEGER, guild_id INTEGER, channel_policy TEXT, user_id INTEGER, user_name TEXT, role TEXT, content TEXT, timestamp TEXT)")
        for i in range(20):
            conn.execute("INSERT INTO conversations VALUES (?,?,?,?,?,?,?,?)", (i + 1, 1, 'public_home', 42, 'Profile Crow', 'user', f'conversation {i}', '2026-06-01T00:00:00+00:00'))
        conn.execute("CREATE TABLE entity_evidence_events (id INTEGER, guild_id INTEGER, channel_name TEXT, channel_policy TEXT, author TEXT, matched_user_id INTEGER, evidence_kind TEXT, dossier_relevance TEXT, confidence TEXT, community_signal TEXT, music_signal TEXT, relation_type TEXT, observed_at TEXT, created_at TEXT, public_safe_candidate INTEGER, raw_ref_json TEXT)")
        for i in range(8):
            conn.execute("INSERT INTO entity_evidence_events VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (100 + i, 1, 'signals', 'public_home', 'Fallback Author', 42, 'community', 'high', 'high', 'present', 'none', 'member', '2026-06-02', '2026-06-02', 1, json.dumps({'displayLabel': 'Raw Label'})))
        conn.commit(); conn.close()
        return db

    def test_scan_uses_read_model_public_dossiers_when_available(self):
        read_model = {"ok": True, "version": 1, "publicOnly": True, "sections": {"dossiers": {"items": [{"id": "pd-6", "name": "6 Bit", "slug": "6-bit"}]}}}
        result = run_population_scan(memory_records=self.mid_records("6 Bit"), dry_run=True, diagnostics=True, read_model_loader=lambda: read_model)
        self.assertTrue(result["diagnostics"]["site_context_loaded"])
        self.assertEqual(result["diagnostics"]["public_dossiers"], 1)
        rec = result["recommendations"][0]
        self.assertEqual(rec["recommendedLane"], "public_dossier_update_signal")
        self.assertEqual(rec["recommendedAction"], "create_dossier_update_workspace")
        self.assertEqual(rec["matchedPublicDossierId"], "pd-6")

    def test_site_context_unavailable_continues_memory_only(self):
        result = run_population_scan(memory_records=self.mid_records("Memory Only"), dry_run=True, diagnostics=True, read_model_loader=lambda: {})
        self.assertTrue(result["ok"])
        self.assertFalse(result["diagnostics"]["site_context_loaded"])
        self.assertEqual(result["recommendations"][0]["recommendedLane"], "candidate_intake")

    def test_existing_source_file_candidate_from_read_model_attaches(self):
        read_model = {"ok": True, "version": 1, "publicOnly": True, "sections": {"sourceContext": {"sourceFiles": [{"id": "sf-known", "name": "Known Subject", "subjectKey": "known-subject"}]}}}
        result = run_population_scan(memory_records=self.mid_records("Known Subject"), dry_run=True, read_model_loader=lambda: read_model)
        rec = result["recommendations"][0]
        self.assertEqual(rec["recommendedAction"], "attach_to_existing_source_file")
        self.assertEqual(rec["matchedExistingCandidateId"], "sf-known")

    def test_repeated_evidence_without_target_creates_candidate(self):
        rec = build_population_recommendation(self.mid_records("New Repeated"), public_dossiers=[], existing_candidates=[])
        self.assertEqual(rec["recommendedLane"], "candidate_intake")
        self.assertEqual(rec["recommendedAction"], "create_source_file_candidate")

    def test_balanced_quotas_include_entity_events_when_conversations_are_many(self):
        db = self._quota_db()
        diagnostics = {}
        records = collect_shared_memory_population_records(db, 1, max_items=10, diagnostics=diagnostics)
        counts = {lane: sum(1 for r in records if r["sourceLane"] == lane) for lane in {r["sourceLane"] for r in records}}
        self.assertLessEqual(counts.get("conversations", 0), 6)
        self.assertGreater(counts.get("entity_evidence_events", 0), 0)
        self.assertGreater(diagnostics["tables"]["conversations"]["skipped_due_to_quota"], 0)
        self.assertGreater(diagnostics["tables"]["entity_evidence_events"]["attempted_collection"], 0)

    def test_entity_evidence_uses_user_profile_display_name(self):
        db = self._quota_db()
        records = collect_shared_memory_population_records(db, 1, max_items=10)
        entity = [r for r in records if r["sourceLane"] == "entity_evidence_events"][0]
        self.assertEqual(entity["subjectName"], "Profile Crow")
        self.assertEqual(entity["sourceEvidenceRef"], "entity_evidence_events:100")
        self.assertEqual(entity["memoryTier"], "long_term")

    def test_entity_evidence_skips_numeric_and_redacted_subjects(self):
        db = self._temp_db()
        conn = sqlite3.connect(db)
        conn.execute("CREATE TABLE entity_evidence_events (id INTEGER, guild_id INTEGER, author TEXT, evidence_kind TEXT, confidence TEXT, created_at TEXT, raw_ref_json TEXT)")
        conn.execute("INSERT INTO entity_evidence_events VALUES (1,1,'123456789012345678','community','high','2026-06-01',?)", (json.dumps({'displayLabel': '[redacted-id]'}),))
        conn.execute("INSERT INTO entity_evidence_events VALUES (2,1,'[redacted-id]','community','high','2026-06-01',?)", (json.dumps({'displayLabel': '7890'}),))
        conn.commit(); conn.close()
        diagnostics = {}
        records = collect_shared_memory_population_records(db, 1, max_items=10, diagnostics=diagnostics)
        self.assertEqual(records, [])
        self.assertEqual(diagnostics["tables"]["entity_evidence_events"]["skipped_due_to_missing_subject_name"], 2)

    def test_redacted_id_not_emitted_as_recommendation_subject(self):
        result = run_population_scan(memory_records=[{"subjectName": "[redacted-id]", "memoryTier": "mid_term", "sourceLane": "entity_evidence_events", "summary": "diagnostic fixture", "sourceEvidenceRef": "x", "rawEvidenceRef": "x"}], dry_run=True)
        self.assertEqual(result["recommendations"], [])
        self.assertEqual(result["counts"]["skipped"], 1)

    def test_diagnostic_requires_strong_evidence_not_test_channel_alone(self):
        rec = build_population_recommendation([{"subjectName": "Crow", "memoryTier": "short_term", "sourceLane": "conversations", "visibility": "sealed_test", "summary": "human-readable community note", "sourceEvidenceRef": "c:1", "rawEvidenceRef": "c:1"}])
        self.assertNotEqual(rec["recommendedLane"], "diagnostic_artifact")
        strong = build_population_recommendation([{"subjectName": "Fixture Subject", "memoryTier": "short_term", "sourceLane": "conversations", "visibility": "sealed_test", "summary": "diagnostic fixture marker", "sourceEvidenceRef": "c:2", "rawEvidenceRef": "c:2"}])
        self.assertEqual(strong["recommendedLane"], "diagnostic_artifact")

    def test_diagnostics_response_includes_context_and_skips(self):
        result = run_population_scan(memory_records=self.mid_records("6 Bit"), dry_run=True, diagnostics=True, public_dossiers=[{"id": "pd-6", "name": "6 Bit"}])
        response = format_population_scan_response(result)
        self.assertIn("Context: site_context_loaded=False, public_dossiers=1", response)
        self.assertIn("site_context_loaded", response)


if __name__ == "__main__":
    unittest.main()
