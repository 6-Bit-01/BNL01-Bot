import unittest

from bnl_admin_summaries import (
    ADMIN_SUMMARY_SCHEMA_FIELDS,
    DOSSIER_UPDATE_SUMMARY_FIELDS,
    RECOMMENDATION_CLUSTER_SUMMARY_FIELDS,
    build_admin_summary,
    build_dossier_update_summary,
    build_recommendation_cluster_summary,
    mark_summary_stale_if_evidence_changed,
)
from bnl_dossier_candidate_discovery import discover_candidate_recommendations
from bnl_source_file_enrichment import build_enrichment_recommendation_payload, build_source_file_archive_payload


class AdminSummaryTests(unittest.TestCase):
    def sample_record(self):
        return {
            "subjectName": "Signal Fox",
            "subjectKey": "signal-fox",
            "matchKind": "existing_dossier_update",
            "confidence": "medium",
            "sourceLanes": ["source_file_notes", "relationship_journal", "local_profile_observed"],
            "sourceTypes": ["conversations"],
            "sourceCounts": {"conversations": 2},
            "knownContext": ["Signal Fox appears in BARCODE Radio planning notes as a recurring review subject."],
            "usefulEvidence": ["Admin note says Signal Fox may need a Source File follow-up after repeated BARCODE mentions."],
            "relationshipSignals": ["relationship_journal: unknown -> unknown via local_profile_observed blob"],
            "missingInfo": ["Confirm role and whether any public links are owned by the subject."],
            "publicSafetyNotes": ["Keep internal aliases private until owner/admin confirmation."],
            "doNotSay": ["Do not publish alias: Fox Internal Handle as a public fact."],
            "rawEvidenceRefs": ["relationship_journal/42", "local_profile_observed/7"],
            "reason": "Repeated review-only BARCODE context; not public copy.",
        }

    def test_admin_summary_schema_exists(self):
        self.assertIn("operatorSummary", ADMIN_SUMMARY_SCHEMA_FIELDS)
        self.assertIn("rawEvidenceRefs", ADMIN_SUMMARY_SCHEMA_FIELDS)
        self.assertIn("inputHash", ADMIN_SUMMARY_SCHEMA_FIELDS)
        self.assertIn("updateSummary", DOSSIER_UPDATE_SUMMARY_FIELDS)
        self.assertIn("clusterSummary", RECOMMENDATION_CLUSTER_SUMMARY_FIELDS)

    def test_source_file_refresh_payload_can_produce_admin_summary_and_preserve_refs(self):
        record = self.sample_record()
        archive = build_source_file_archive_payload({
            "subject": "Signal Fox",
            "matchKind": "existing_dossier_update",
            "sections": {"Missing Info": ["Confirm role."], "Do Not Say": ["Do not publish aliases."], "Public-Safe Notes": ["Review first."]},
            **record,
        })
        self.assertIn("adminSummary", archive)
        self.assertEqual(archive["adminSummary"]["generatedBy"], "BNL")
        self.assertFalse(archive["adminSummary"]["stale"])
        self.assertTrue(archive["adminSummary"]["rawEvidenceRefs"])
        self.assertIn("sourcePackage", archive)

    def test_dossier_update_workspace_refresh_can_produce_update_summary(self):
        summary = build_dossier_update_summary(self.sample_record())
        self.assertEqual(summary["summaryVersion"], "dossier_update_summary_v1")
        self.assertIn("possible Dossier Update", summary["updateSummary"])
        self.assertIn(summary["safePublicUpdateCandidate"], {"yes", "no", "maybe"})
        archive = build_source_file_archive_payload({"subject": "Signal Fox", "matchKind": "existing_dossier_update", "sections": {}, **self.sample_record()})
        self.assertIn("updateSummary", archive)
        self.assertIn("adminSummary", archive)

    def test_recommendation_cluster_summary_can_be_produced_from_stored_evidence(self):
        summary = build_recommendation_cluster_summary({
            "subjectName": "Signal Fox",
            "reason": "BNL dynamic discovery found Signal Fox in approved source-safe lanes as a new source file candidate.",
            "evidenceSummary": "R&D context: Signal Fox should have a source file for recurring BARCODE mentions.",
            "sourceLanes": ["rd_context"],
            "confidence": "medium",
        })
        self.assertEqual(summary["summaryVersion"], "recommendation_cluster_summary_v1")
        self.assertEqual(summary["recommendedDisposition"], "source_file_review")
        self.assertEqual(summary["appearsToBelongTo"], "Signal Fox")

    def test_discovery_payload_includes_recommendation_cluster_summary(self):
        result = discover_candidate_recommendations(
            123,
            ["rd_context"],
            rd_context=[{"main_subject": "Signal Fox", "user_request": "Signal Fox needs a source file", "response_summary": "recurring BARCODE review subject"}],
        )
        self.assertGreaterEqual(result["candidateCount"], 1)
        payload = result["payloads"][0]
        self.assertIn("recommendationClusterSummary", payload)
        self.assertEqual(payload["recommendationClusterSummary"]["generatedBy"], "BNL")

    def test_raw_blobs_unknown_chains_and_internal_alias_values_not_dumped(self):
        summary = build_admin_summary(self.sample_record())
        text = " ".join([
            summary["operatorSummary"],
            summary["whyTheyMatter"],
            " ".join(summary["knownContext"]),
            " ".join(summary["relationshipToBARCODE"]),
            " ".join(summary["doNotSay"]),
        ]).lower()
        self.assertNotIn("relationship_journal", text)
        self.assertNotIn("local_profile_observed", text)
        self.assertNotIn("unknown -> unknown", text)
        self.assertNotIn("fox internal handle", text)
        self.assertIn("unconfirmed", summary["operatorSummary"].lower())

    def test_missing_evidence_creates_insufficient_summary_instead_of_hallucination(self):
        summary = build_admin_summary({"subjectName": "Empty Subject", "sourceLanes": []})
        self.assertEqual(summary["operatorSummary"], "BNL summary needed: insufficient evidence.")
        self.assertEqual(summary["confidence"], "low")
        self.assertIn("Collect or attach evidence", summary["recommendedNextAction"])

    def test_summary_becomes_stale_when_source_evidence_changes_and_can_regenerate(self):
        record = self.sample_record()
        summary = build_admin_summary(record)
        changed = {**record, "usefulEvidence": record["usefulEvidence"] + ["New admin evidence was added."]}
        stale = mark_summary_stale_if_evidence_changed(summary, changed)
        self.assertTrue(stale["stale"])
        regenerated = build_admin_summary(changed)
        self.assertFalse(regenerated["stale"])
        self.assertNotEqual(summary["inputHash"], regenerated["inputHash"])

    def test_public_copy_and_publish_actions_are_not_emitted(self):
        archive = build_source_file_archive_payload({"subject": "Signal Fox", "matchKind": "none", "sections": {}, **self.sample_record()})
        serialized = str(archive).lower()
        self.assertNotIn("publishnow", serialized)
        self.assertNotIn("publicdossiertext", serialized)
        self.assertIn("internal/review-only", archive["sourcePackage"]["archiveNotice"].lower())


if __name__ == "__main__":
    unittest.main()
