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

    def test_archive_payload_includes_readable_source_file_brief_v2(self):
        packet = self.large_packet()
        packet["subjectIdentity"] = {"aliasLabels": ["6 Bit", "6 Bit"], "matchedUserProfileCount": 1, "workflowLane": "active_source_file"}
        packet["bestEvidenceToReview"] = [
            "BNL has seen this subject connected to BARCODE community activity.",
            "BNL has seen this subject connected to BARCODE community activity.",
            {"claim": "Profile matched identity signal exists", "evidenceSummary": "A local profile match needs admin review.", "sourceLane": "identity", "confidence": "medium", "publicSafe": False},
        ]
        payload = enrichment.build_source_file_archive_payload(packet)
        brief = payload["sourceFileBriefV2"]
        expected_keys = {
            "version", "subjectName", "subjectKey", "oneLineSummary", "adminSummary", "whatBnlKnows", "whyThisMatters",
            "evidenceToReview", "identityContext", "publicSafeDraftingNotes", "internalOnlyNotes", "dossierQuestions",
            "recommendedNextAction", "qualityWarnings", "archiveReferences",
        }
        self.assertEqual(set(brief), expected_keys)
        self.assertEqual(brief["version"], "2")
        self.assertEqual(brief["subjectName"], "6-bit")
        self.assertEqual(brief["subjectKey"], "6-bit")
        self.assertIn("sourcePackage", payload)
        self.assertIn("rawProvenance", payload["sourcePackage"])
        self.assertLessEqual(len(brief["oneLineSummary"]), 220)
        self.assertLessEqual(len(brief["adminSummary"]), 900)
        for list_key in ("whatBnlKnows", "whyThisMatters", "evidenceToReview", "publicSafeDraftingNotes", "internalOnlyNotes", "dossierQuestions", "qualityWarnings"):
            self.assertLessEqual(len(brief[list_key]), 6)
        self.assertEqual(
            sum(1 for item in brief["evidenceToReview"] if item.get("claim") == "BNL has seen this subject connected to BARCODE community activity"),
            1,
        )

    def test_source_file_brief_v2_readable_fields_avoid_raw_json_and_giant_arrays(self):
        packet = self.large_packet()
        raw_dump = '{"sourceTable":"entity_evidence_events","sourceRowId":"row_123","rawRefJson":{"message":"x"}}'
        packet["bestEvidenceToReview"] = [raw_dump for _ in range(30)]
        packet["conversationHighlights"] = [raw_dump for _ in range(30)]
        payload = enrichment.build_source_file_archive_payload(packet)
        brief = payload["sourceFileBriefV2"]
        brief_body = json.dumps(brief, sort_keys=True)
        self.assertNotIn("sourceTable", brief_body)
        self.assertNotIn("sourceRowId", brief_body)
        self.assertNotIn("rawRefJson", brief_body)
        self.assertNotIn("entity_evidence_events", brief_body)
        self.assertLessEqual(len(brief["evidenceToReview"]), 6)
        self.assertLess(len(brief_body), 8000)

    def test_compact_recommendation_does_not_receive_full_brief_body(self):
        packet = self.large_packet()
        archive_payload = enrichment.build_source_file_archive_payload(packet)
        compact_payload = enrichment.build_enrichment_recommendation_payload(packet)
        self.assertIn("sourceFileBriefV2", archive_payload)
        self.assertNotIn("sourceFileBriefV2", compact_payload)
        self.assertNotIn("adminSummary", json.dumps(compact_payload, sort_keys=True))

    def test_archive_preserves_full_evidence_when_brief_is_concise(self):
        packet = self.large_packet()
        payload = enrichment.build_source_file_archive_payload(packet)
        body = json.dumps(payload, sort_keys=True)
        self.assertIn("6-bit recurring source intelligence item 79", body)
        self.assertNotIn("6-bit recurring source intelligence item 79", json.dumps(payload["sourceFileBriefV2"], sort_keys=True))
        self.assertLessEqual(len(payload["sourceFileBriefV2"]["evidenceToReview"]), 6)

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
    def test_archive_includes_subject_memory_packet_and_case_report_only_in_full_archive(self):
        packet = self.large_packet()
        packet.update({
            "subject": "Crow",
            "matchKind": "active_source_file",
            "musicSignals": ["music discussion only: 22"],
            "communitySignals": ["Crow asked BNL Source File questions in #barcode-bot during admin review."],
            "relationshipSignals": ["Orion appears in relationship/context evidence for Crow"],
            "queueSubmissionStatus": "not_connected",
            "queueSubmissionNote": "Queue/submission memory is not connected to this report. Do not claim submissions, song counts, play history, payment, or Priority status.",
            "subjectIdentity": {"aliasLabels": ["Crow"], "workflowLane": "identity_check_review"},
            "representativeEvidence": [
                {
                    "summary": "Crow asked BNL Source File questions in #barcode-bot, supporting BNL/source-file workflow activity.",
                    "sourceType": "Discord conversation",
                    "channelName": "#barcode-bot",
                    "visibility": "internal_review_only",
                    "occurredAt": "2026-06-08T00:00:00+00:00",
                }
            ],
        })
        archive_payload = enrichment.build_source_file_archive_payload(packet)
        compact_payload = enrichment.build_enrichment_recommendation_payload(packet)

        self.assertIn("subjectMemoryPacketV1", archive_payload)
        self.assertIn("sourceFileCaseReportV1", archive_payload)
        self.assertNotIn("subjectMemoryPacketV1", compact_payload)
        self.assertNotIn("sourceFileCaseReportV1", compact_payload)
        self.assertIn("sourcePackage", archive_payload)
        self.assertIn("rawProvenance", archive_payload["sourcePackage"])

        memory = archive_payload["subjectMemoryPacketV1"]
        report = archive_payload["sourceFileCaseReportV1"]
        self.assertEqual(memory["version"], "1")
        self.assertEqual(report["version"], "1")
        self.assertEqual(report["subjectKey"], memory["subjectKey"])
        self.assertEqual(report["generatedAt"], memory["generatedAt"])
        self.assertEqual(report["memoryCoverage"], memory["memoryCoverage"])
        self.assertLessEqual(len(memory["representativeMoments"]), 8)
        moment = memory["representativeMoments"][0]
        self.assertEqual(moment["channelName"], "#barcode-bot")
        self.assertEqual(moment["sourceType"], "Discord conversation")
        self.assertIn("source-file workflow", moment["summary"])

        report_body = json.dumps(report, sort_keys=True).lower()
        for forbidden in (
            "global_mixed",
            "source_blind",
            "source rows",
            "internal classification",
            "automated topic label",
            "approved approved",
            "relationship facet",
            "music discussion exists",
            "creative language observed",
            "relationship signals found",
            "community activity detected",
        ):
            self.assertNotIn(forbidden, report_body)
        self.assertIn("does not expose the specific songs", report_body)
        self.assertIn("orion", report_body)
        self.assertIn("review-only", report_body)
        self.assertIn("queue/submission memory is not connected", report_body)
        self.assertIn("does not auto-confirm identity", report_body)
        coverage = json.dumps(memory["memoryCoverage"], sort_keys=True).lower()
        self.assertIn("queue/submission data", coverage)
        self.assertIn("short/mid/long-term subject memory lanes are not exposed separately", coverage)


    def test_subject_intelligence_brief_surfaces_crow_like_patterns(self):
        packet = self.large_packet()
        packet.update({
            "subject": "Crow",
            "matchKind": "active_source_file",
            "sourceCounts": {"conversations": 9, "entity_evidence_events": 2},
            "activityFrequencySummary": {
                "approvedPublicAuthoredRows": 6,
                "approvedPublicMentionedRows": 2,
                "reviewOnlyEvidenceCount": 1,
                "latestObservedAt": "2026-06-09T12:00:00+00:00",
            },
            "topChannels": [
                {"channel": "#barcode-bot", "count": 5, "summary": "Crow repeatedly addresses BNL/source-file workflow."},
                {"channel": "#finished-tracks", "count": 1, "summary": "One music/link-adjacent public signal."},
            ],
            "topTopicDetails": [
                {"topic": "BNL source-file and dossier classification", "count": 5, "summary": "Crow asks about BNL, Source Files, dossier review, thresholds, and operational boundaries."},
                {"topic": "interface / lore convergence", "count": 3, "summary": "Crow uses sync, convergence, Network, EDGE, and Orion-linked framing."},
                {"topic": "music and link-sharing classification", "count": 1, "summary": "A Suno link appears, but ownership/submission status is not connected."},
            ],
            "topicBreakdown": ["BNL source-file and dossier classification", "interface / lore convergence", "music and link-sharing classification"],
            "conversationThemes": ["BNL-facing interaction", "threshold behavior", "Orion-linked framing"],
            "knownContext": ["Crow talks with BNL-01 about Source Files, thresholds, sync, convergence, BARCODE, EDGE, Network, and Orion."],
            "usefulEvidence": ["Crow repeatedly asks BNL-01 how a Source File becomes safe for dossier work."],
            "bestEvidenceToReview": ["Crow shared a Suno link while discussing BNL boundaries; this does not prove it was Crow's submission."],
            "bnlInteractionSignals": ["Crow addresses BNL-01 directly and tests operational boundaries around Source File review."],
            "musicSignals": ["Suno link surfaced near Crow; treat as a link signal, not a confirmed owned track or queue submission."],
            "communitySignals": ["Crow appears in BARCODE-facing public context and talks about the Network and Lardcode."],
            "relationshipSignals": ["Orion appears in Crow context; meaning unconfirmed."],
            "queueSubmissionStatus": "not_connected",
            "queueSubmissionNote": "Queue/submission memory is not connected to this report. Do not claim submissions, song counts, play history, payment, or Priority status.",
            "representativeEvidence": [
                {
                    "summary": "Crow asked BNL-01 Source File threshold questions in #barcode-bot with Orion, Network, EDGE, and BARCODE language.",
                    "sourceType": "Discord conversation",
                    "channelName": "#barcode-bot",
                    "visibility": "public_context",
                    "occurredAt": "2026-06-09T12:00:00+00:00",
                }
            ],
        })
        report = enrichment.build_source_file_archive_payload(packet)["sourceFileCaseReportV1"]
        brief = report.get("subjectIntelligenceBriefV1")
        self.assertIsInstance(brief, dict)
        self.assertIn("BNL-facing", brief["subjectRead"])
        self.assertIn("BNL currently reads Crow", brief["bnlTake"])
        self.assertIn("not connected", brief["queueSubmissionRead"].lower())
        self.assertEqual(brief["activityProfile"]["totalApprovedPublicAuthoredItems"], 6)
        self.assertTrue(any(item.get("channelName") == "#barcode-bot" and item.get("count") == 5 for item in brief["channelBreakdown"]))
        self.assertTrue(any("BNL source-file and dossier" in item.get("topic", "") and item.get("strength") == "strong" for item in brief["topicBuckets"]))
        anchors = {item.get("name") for item in brief["namedAnchors"]}
        self.assertIn("Orion", anchors)
        self.assertIn("BNL-01", anchors)
        self.assertIn("BARCODE", anchors)
        self.assertFalse({"You", "Your", "The", "For"}.intersection(anchors))
        self.assertTrue(any("Suno" in item for item in brief["musicAndLinkSignals"]))
        self.assertTrue(all("review-only" in item.lower() or "unconfirmed" in item.lower() for item in brief["relationshipSignals"]))
        self.assertTrue(any("display name" in item.lower() for item in brief["sourceFileGaps"]))
        self.assertTrue(any("Suno" in item or "music links" in item for item in brief["sourceFileGaps"]))
        body = json.dumps(brief, sort_keys=True)
        for forbidden in ("entity_evidence_events", "rawRefJson", "sourceRowId", "user_id", "channel_id", "research-and-development"):
            self.assertNotIn(forbidden, body)
        self.assertNotIn("confirmed submission", body.lower())

    def test_case_report_is_built_from_subject_memory_packet_not_raw_packet_labels(self):
        memory = {
            "version": "1",
            "generatedAt": "2026-06-08T00:00:00+00:00",
            "subjectName": "Signal Fox",
            "subjectKey": "signal-fox",
            "memoryCoverage": {"summary": "Short/mid/long-term subject memory lanes are not exposed separately to this report yet; this report uses available conversation, entity, and Source File archive data.", "lanes": []},
            "identitySignals": ["Identity review is handled through the site Identity Check when links are available. No identity is auto-confirmed by this report."],
            "communityContext": ["Signal Fox discussed source-file review in #barcode-bot."],
            "musicCreativeContext": ["The archive flags music/creative context, but the current evidence packet does not expose the specific songs, collaborators, messages, channels, platforms, or links behind that label."],
            "relationshipContext": ["Orion appears in relationship/context evidence for Signal Fox. Treat the relationship/context meaning as review-only and unconfirmed."],
            "queueSubmissionContext": ["Queue/submission memory is not connected to this report. Do not claim submissions, song counts, play history, payment, or Priority status."],
            "publicSafeCandidateFacts": ["No public-safe fact is confirmed by this packet; owner review is required before public use."],
            "representativeMoments": [{"summary": "Signal Fox asked Source File questions in #barcode-bot.", "sourceType": "Discord conversation", "channelName": "#barcode-bot", "visibility": "internal_review_only", "publicSafe": False}],
            "openQuestions": ["Confirm public-safe display name."],
            "internalWarnings": ["Do not publish or merge identities."],
            "evidenceGaps": ["The current memory path exposes only category labels for music discussion; raw representative music details are not available to the report generator yet."],
        }
        report = enrichment.build_source_file_case_report_v1(memory)
        body = json.dumps(report, sort_keys=True).lower()
        self.assertIn("signal fox asked source file questions", body)
        self.assertIn("does not expose the specific songs", body)
        self.assertNotIn("music discussion exists", body)
        self.assertNotIn("relationship facet", body)
        self.assertIn("queue/submission memory is not connected", body)
        self.assertIn("does not auto-confirm identity", body)

