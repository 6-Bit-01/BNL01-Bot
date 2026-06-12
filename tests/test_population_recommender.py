import json
import logging
import unittest

from bnl_population_recommender import (
    POPULATION_RECOMMENDATION_SCHEMA_FIELDS,
    build_population_recommendation,
    build_population_recommendation_payload,
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


if __name__ == "__main__":
    unittest.main()
