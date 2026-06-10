import json
import logging
import unittest
from unittest import mock

import bnl_dossier_recommendations as recs
import bnl_source_file_enrichment as enrichment


class FakeResponse:
    def __init__(self, payload, status=200):
        self.payload = payload
        self.status = status

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def getcode(self):
        return self.status

    def read(self):
        return json.dumps(self.payload).encode("utf-8")


class CapturingOpener:
    def __init__(self, payload=None, status=200):
        self.payload = payload if payload is not None else {"ok": True, "archiveId": "arc_1"}
        self.status = status
        self.requests = []

    def open(self, request, timeout=0):
        self.requests.append(request)
        return FakeResponse(self.payload, status=self.status)


class SourceFileArchiveTests(unittest.TestCase):
    def large_packet(self):
        evidence = [f"6-bit recurring source intelligence item {i} with meaningful context" for i in range(80)]
        return {
            "ok": True,
            "subject": "6-bit",
            "matchKind": "active_source_file",
            "sourceFile": {"candidateId": "cand_6bit", "name": "6-bit", "discord_user_id": "123456789012345678"},
            "sections": {
                "Missing Info": ["owner confirmation still needed"],
                "Public-Safe Notes": ["public-safe possibility after review"],
                "Do Not Say": ["do not claim identity merge"],
            },
            "sourceCounts": {"conversations": 80, "entity_evidence_events": 25},
            "sourceTypes": ["conversations", "entity_evidence_events"],
            "warningCounts": {"review_only": 2},
            "warnings": ["review-only archive"],
            "qualityScore": 95,
            "qualityStatus": "sendable",
            "publicSafePossibilities": ["Possible public-safe 6-bit summary after admin review"],
            "bestEvidenceToReview": evidence,
            "conversationHighlights": evidence,
            "rawProvenance": {
                "rawFragments": [
                    {"sourceTable": "conversations", "sourceRowId": f"row_{i}", "safeSummary": item, "user_id": "123456789012345678"}
                    for i, item in enumerate(evidence)
                ]
            },
            "runTime": "2026-06-08T00:00:00+00:00",
            "ingestKey": "bnl:source-enrichment:6-bit:test",
        }

    def test_archive_url_and_token_config_with_dossier_fallback(self):
        self.assertEqual(
            recs.get_source_file_archive_url({}),
            "https://www.barcode-network.com/api/bnl/source-file-enrichments",
        )
        self.assertEqual(recs.get_source_file_archive_url({"BNL_SOURCE_FILE_ARCHIVE_URL": "https://site.test/archive"}), "https://site.test/archive")
        self.assertEqual(recs.get_source_file_archive_token({"BNL_DOSSIER_INGEST_TOKEN": "dossier-token"}), "dossier-token")
        self.assertEqual(
            recs.get_source_file_archive_token({"BNL_SOURCE_FILE_ARCHIVE_TOKEN": "archive-token", "BNL_DOSSIER_INGEST_TOKEN": "dossier-token"}),
            "archive-token",
        )

    def test_archive_sender_posts_to_archive_endpoint_without_logging_token_or_payload(self):
        opener = CapturingOpener({"ok": True, "archiveId": "arc_safe"})
        payload = enrichment.build_source_file_archive_payload(self.large_packet())
        with mock.patch.object(logging, "info") as info_mock, mock.patch.object(logging, "warning") as warning_mock:
            result = recs.send_source_file_archive_enrichment(
                payload,
                environ={"BNL_SOURCE_FILE_ARCHIVE_URL": "https://site.test/archive", "BNL_SOURCE_FILE_ARCHIVE_TOKEN": "secret-archive-token"},
                opener=opener,
            )
        self.assertTrue(result["ok"])
        self.assertEqual(result["archiveId"], "arc_safe")
        request = opener.requests[0]
        self.assertEqual(request.full_url, "https://site.test/archive")
        self.assertEqual(request.headers["Authorization"], "Bearer secret-archive-token")
        log_text = " ".join(str(call) for call in [*info_mock.call_args_list, *warning_mock.call_args_list])
        self.assertNotIn("secret-archive-token", log_text)
        self.assertNotIn("6-bit recurring source intelligence item 79", log_text)

    def test_large_source_package_is_preserved_in_archive_not_compact_recommendation_fields(self):
        packet = self.large_packet()
        archive_payload = enrichment.build_source_file_archive_payload(packet)
        compact_payload = enrichment.build_enrichment_recommendation_payload(packet)
        archive_body = json.dumps(archive_payload, sort_keys=True)
        compact_body = json.dumps(compact_payload, sort_keys=True)
        self.assertIn("6-bit recurring source intelligence item 79", archive_body)
        self.assertIn("sourcePackage", archive_payload)
        self.assertNotIn("6-bit recurring source intelligence item 79", compact_body)
        self.assertLess(len(compact_payload["evidenceSummary"]), 2000)

    def test_archive_payload_redacts_discord_payment_and_secret_material(self):
        packet = self.large_packet()
        packet["sourceFile"]["payment_customer_id"] = "cus_123"
        packet["sourceFile"]["apiToken"] = "token-value"
        archive_payload = enrichment.build_source_file_archive_payload(packet)
        body = json.dumps(archive_payload, sort_keys=True)
        self.assertNotIn("123456789012345678", body)
        self.assertNotIn("cus_123", body)
        self.assertNotIn("token-value", body)
        self.assertIn("[redacted]", body)

    def test_run_enrichment_distinguishes_archive_and_recommendation_results(self):
        packet = self.large_packet()
        with mock.patch.object(enrichment, "collect_source_enrichment_evidence", return_value={
                 "sections": packet["sections"], "sourceCounts": packet["sourceCounts"], "warningCounts": packet["warningCounts"],
                 "sourceTypes": packet["sourceTypes"], "warnings": [], "diagnostics": {}, "classification": {},
                 "bestEvidenceToReview": packet["bestEvidenceToReview"], "conversationHighlights": packet["conversationHighlights"],
                 "publicSafePossibilities": packet["publicSafePossibilities"], "rawProvenance": packet["rawProvenance"],
             }), \
             mock.patch.object(enrichment, "build_entity_intelligence_profile", return_value={"diagnostics": {}}), \
             mock.patch.object(enrichment, "resolve_entity_context_for_surface", return_value={}):
            archive_calls = []
            rec_calls = []
            result = enrichment.run_source_file_enrichment(
                ":memory:", 1, "6-bit", force=True,
                lookup_func=lambda query: {"ok": True, "found": True, "sourceFile": packet["sourceFile"], "matchKind": "active_source_file"},
                environ={"BNL_SOURCE_FILE_ARCHIVE_TOKEN": "archive-token"},
                archive_sender=lambda payload: archive_calls.append(payload) or {"ok": True, "archiveId": "arc_1", "status": 200},
                sender=lambda payload: rec_calls.append(payload) or {"ok": False, "error": "compact too long", "status": 413},
            )
        self.assertTrue(result["archiveSent"])
        self.assertFalse(result["recommendationSent"])
        self.assertFalse(result["sent"])
        self.assertEqual(result["status"], "partial_success")
        self.assertTrue(result["partialSuccess"])
        self.assertEqual(result["partialSuccessReason"], "compact_recommendation_rejected")
        self.assertIn("sourcePackage", archive_calls[0])
        self.assertNotIn("sourcePackage", rec_calls[0])

    def test_compact_success_archive_failure_is_not_fully_sent(self):
        packet = self.large_packet()
        payload = enrichment.build_enrichment_recommendation_payload(packet)
        archive_payload = enrichment.build_source_file_archive_payload(packet)
        archive_result = {"ok": False, "error": "archive rejected", "status": 500}
        rec_result = {"ok": True, "recommendationId": "rec_1", "status": 200}
        self.assertFalse(archive_result["ok"])
        self.assertTrue(rec_result["ok"])
        self.assertIn("sourcePackage", archive_payload)
        self.assertNotIn("sourcePackage", payload)

    def test_no_public_publish_or_identity_merge_flags_introduced(self):
        packet = self.large_packet()
        archive_payload = enrichment.build_source_file_archive_payload(packet)
        body = json.dumps(archive_payload, sort_keys=True).lower()
        for forbidden in ("publish_public", "draft_create", "tag_create", "identity_merge", "queue_payment", "alias_confirm"):
            self.assertNotIn(forbidden, body)

    def test_oversized_compact_recommendation_fields_are_capped_with_archive_preserved(self):
        packet = self.large_packet()
        huge = "\n".join(f"full evidence dump row {i} " + ("x" * 200) for i in range(200))
        packet["entityIntelligenceProfile"] = {"subjectKey": "6-bit", "body": huge, "diagnostics": {"rowScopeCounts": {"subject_owned": 200}}}
        packet["bestEvidenceToReview"] = [huge for _ in range(20)]
        packet["conversationHighlights"] = [huge for _ in range(20)]
        archive_payload = enrichment.build_source_file_archive_payload(packet)
        compact_payload = enrichment.build_enrichment_recommendation_payload(packet)
        archive_body = json.dumps(archive_payload, sort_keys=True)
        compact_body = json.dumps(compact_payload, sort_keys=True)
        self.assertIn("full evidence dump row 13", archive_body)
        self.assertNotIn("full evidence dump row 199", compact_body)
        self.assertLessEqual(len(compact_payload["evidenceSummary"]), enrichment.COMPACT_RECOMMENDATION_TEXT_FIELD_LIMITS["evidenceSummary"])
        self.assertLessEqual(len(json.dumps(compact_payload["entityIntelligenceProfile"], sort_keys=True)), enrichment.COMPACT_RECOMMENDATION_JSON_FIELD_LIMITS["entityIntelligenceProfile"])
        self.assertIn("Full Source File archive available internally", compact_payload["compactRecommendationNotice"])

    def test_compact_summary_can_include_archive_id_after_archive_send(self):
        packet = self.large_packet()
        payload = enrichment.build_enrichment_recommendation_payload(packet)
        compact = enrichment.sanitize_compact_recommendation_payload(payload, packet=packet, archive_id="arc_6bit")
        self.assertIn("arc_6bit", compact["evidenceSummary"])
