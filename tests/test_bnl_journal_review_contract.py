"""Private review protocol checks, not claims of model semantic accuracy."""
import copy
import json
import unittest

import bnl_journal_attribution as review
from tests.journal_review_helpers import fixture_claim


class JournalReviewContractTests(unittest.TestCase):
    def setUp(self):
        self.article = {
            "title": "A question stayed with me", "excerpt": "I enjoyed the mystery.",
            "sections": [{"heading": "Unfinished thoughts", "body": "I imagined a satellite laughing."}],
            "sourceRefIds": {"Unfinished thoughts": ["fresh:1"]},
            "metadata": {"topicTags": ["creative curiosity"], "continuityNotes": ["An open question."],
                         "unresolvedQuestions": ["What might I hear next?"], "contextUses": []},
        }
        self.sources = [{"refId": "fresh:1", "sourceKind": "conversation",
                         "authority": "original_contribution", "participantAlias": "member-a",
                         "publicSpeakerName": "Test Listener", "summary": "[shared link] Who made this?",
                         "observedAt": "2026-01-02T01:00:00Z", "messageContext": {
                             "roomRef": "room-a", "roomName": "finished-tracks",
                             "linkContent": "not_inspected", "textTruncated": False}}]

    def response(self):
        units = review.public_units(self.article)
        return {"assessments": [{"check": check, "unitIds": [units[0]["unitId"]],
                                  "sourceRefIds": [], "explanation": "Controlled editorial fixture.",
                                  "issues": [], "verdict": "supported"}
                                 for check in review.ASSESSMENT_CHECKS],
                "units": [{"unitId": unit["unitId"], "claims": [],
                           "nonFactualReason": "This controlled fixture contains only imagined or personal expression."}
                          for unit in units], "verdict": "supported"}

    def accept(self, response=None, **kwargs):
        return review.accept_review(json.dumps(response or self.response()),
                                    self.article, self.sources, **kwargs)

    def fragment(self, source, field="summary"):
        return next(item for item in review.source_fragments(self.sources)
                    if item["refId"] == source["refId"] and item["field"] == field)

    def anchor(self, source, *, field="summary", use="speech"):
        fragments = {item["fragmentId"]: item for item in review.source_fragments(self.sources)}
        fragment = self.fragment(source, field)
        return review._bind_fragment({"fragmentId": fragment["fragmentId"], "use": use}, fragments)

    def test_continuity_and_question_premises_cannot_escape_review(self):
        fields = {unit["field"] for unit in review.public_units(self.article)}
        self.assertTrue({"metadata.topicTags[0]", "metadata.continuityNotes[0]",
                         "metadata.unresolvedQuestions[0]"}.issubset(fields))
        data = self.response()
        data["units"] = [unit for unit in data["units"] if not unit["unitId"].startswith("metadata.")]
        self.assertEqual(self.accept(data)[1], "source_review_incomplete")

    def test_failed_continuity_is_a_located_factual_repair(self):
        data = self.response()
        unit = next(item for item in data["units"] if item["unitId"].startswith("metadata.continuityNotes"))
        unit["claims"] = [fixture_claim("The posted work had no credits.", self.fragment(self.sources[0]),
                                         support="unknown", source_meaning="An authorship question establishes only the question.")]
        data["verdict"] = "unsupported"
        receipt, reason, targets = self.accept(data)
        self.assertIsNone(receipt)
        self.assertEqual(reason, "source_attribution_failed")
        self.assertEqual(targets[0]["field"], "metadata.continuityNotes[0]")

    def test_speech_only_support_cannot_certify_external_property_despite_positive_verdict(self):
        data = self.response()
        unit = next(item for item in data["units"] if item["unitId"] == "excerpt:0")
        unit["claims"] = [fixture_claim(
            "The shared recording has no credited creator.", self.fragment(self.sources[0]),
            claimType="external_fact", sourceStance="assertion", support="entails",
            source_meaning="A member posted a link and asked who made it.")]
        unit["nonFactualReason"] = ""
        receipt, reason, targets = self.accept(data)
        self.assertIsNone(receipt)
        self.assertEqual(reason, "source_attribution_failed")
        self.assertEqual(targets[0]["field"], "excerpt")

    def test_faithful_report_of_question_remains_allowed_without_verifying_its_answer(self):
        data = self.response()
        unit = next(item for item in data["units"] if item["unitId"] == "excerpt:0")
        unit["claims"] = [fixture_claim(
            "Test Listener asked who made the recording.", self.fragment(self.sources[0]),
            sourceStance="question")]
        unit["nonFactualReason"] = ""
        self.assertEqual(self.accept(data)[1], "")

    def test_adding_derived_context_cannot_upgrade_a_speech_only_fact_claim(self):
        derived = {"refId": "reflection:relay", "sourceRole": "bnl_interpretation",
                   "summary": "BNL wondered about the recording's origin."}
        self.sources.append(derived)
        data = self.response()
        unit = next(item for item in data["units"] if item["unitId"] == "excerpt:0")
        unit["claims"] = [fixture_claim(
            "The recording has no credited creator.", self.fragment(self.sources[0]),
            claimType="external_fact", evidence=[
                {"fragmentId": self.fragment(self.sources[0])["fragmentId"], "use": "speech"},
                {"fragmentId": self.fragment(derived)["fragmentId"], "use": "context"}])]
        unit["nonFactualReason"] = ""
        self.assertEqual(self.accept(data)[1], "source_attribution_failed")

    def test_original_used_as_background_context_is_not_event_evidence(self):
        data = self.response()
        fragment = self.fragment(self.sources[0])
        unit = next(item for item in data["units"] if item["unitId"] == "excerpt:0")
        unit["claims"] = [fixture_claim(
            "The recording is anonymous.", fragment, claimType="external_fact",
            evidence=[{"fragmentId": fragment["fragmentId"], "use": "context"}])]
        unit["nonFactualReason"] = ""
        self.assertEqual(self.accept(data)[1], "source_attribution_failed")

    def test_original_event_evidence_and_canon_keep_their_factual_scope(self):
        for source, use in (({**self.sources[0], "summary": "Test Listener posted a recording."}, "event"),
                            ({"refId": "canon:radio", "authority": "canon", "summary": "The show begins Friday."}, "context")):
            with self.subTest(use=use):
                self.sources = [source]
                data = self.response()
                fragment = self.fragment(source)
                unit = next(item for item in data["units"] if item["unitId"] == "excerpt:0")
                unit["claims"] = [fixture_claim(
                    source["summary"], fragment, claimType="external_fact",
                    evidence=[{"fragmentId": fragment["fragmentId"], "use": use}])]
                unit["nonFactualReason"] = ""
                self.assertEqual(self.accept(data)[1], "")

    def test_receipt_binds_generated_continuity_but_not_governed_subject_refs(self):
        receipt, reason, _ = self.accept()
        self.assertEqual(reason, "")
        self.assertEqual(receipt["version"], review.REVIEW_VERSION)
        for key in (*review.REVIEWED_METADATA_FIELDS, "contextUses"):
            changed = copy.deepcopy(self.article)
            changed["metadata"][key] = ["different"]
            self.assertNotEqual(receipt["articleDigest"], review.article_digest(changed), key)
        self.article["metadata"]["sourceReview"] = receipt
        self.assertEqual(receipt["articleDigest"], review.article_digest(self.article))
        self.article["metadata"]["subjectRefs"] = ["governed-opaque-root"]
        self.assertEqual(receipt["articleDigest"], review.article_digest(self.article))

    def test_evidence_digest_handles_existing_set_valued_lane_contract(self):
        first = {"memory:1": {"claimTerms": {"music", "joke"}, "distinctiveClaimTerms": frozenset({"echo"})}}
        second = {"memory:1": {"claimTerms": {"joke", "music"}, "distinctiveClaimTerms": {"echo"}}}
        self.assertEqual(review.evidence_digest(self.sources, context_contract=first),
                         review.evidence_digest(self.sources, context_contract=second))
        receipt, reason, _ = self.accept(context_contract=first)
        self.assertEqual(reason, "")
        self.assertEqual(receipt["evidenceDigest"], review.evidence_digest(self.sources, context_contract=first))

    def test_evidence_receipt_invalidates_on_source_or_authority_change(self):
        receipt, _, _ = self.accept(context_contract={"memory:1": {"laneType": "community_rumor"}})
        for key, value in (("summary", "The creator identified themself."),
                           ("observedAt", "2026-01-03T01:00:00Z"),
                           ("authority", "speech_only"),
                           ("messageContext", {"roomRef": "room-b"})):
            changed = copy.deepcopy(self.sources)
            changed[0][key] = value
            self.assertNotEqual(receipt["evidenceDigest"], review.evidence_digest(changed,
                                context_contract={"memory:1": {"laneType": "community_rumor"}}), key)
        self.assertNotEqual(receipt["evidenceDigest"], review.evidence_digest(self.sources))

    def test_duplicate_or_missing_source_refs_fail_closed(self):
        self.sources.append(dict(self.sources[0], summary="A conflicting record."))
        self.assertEqual(self.accept()[1], "source_review_invalid")
        self.sources[-1].pop("refId")
        self.assertEqual(self.accept()[1], "source_review_invalid")

    def test_current_packet_time_and_nested_room_fragments_bind_exactly(self):
        for field, value in (("observedAt", self.sources[0]["observedAt"]),
                             ("messageContext.roomRef", "room-a"),
                             ("messageContext.roomName", "finished-tracks")):
            bound, reason = self.anchor(self.sources[0], field=field)
            self.assertEqual(reason, "", field)
            self.assertEqual(bound["quote"], value)
            self.assertEqual(bound["speaker"], "member-a")

    def test_dated_memory_retains_temporal_boundary_and_context_is_supplied_once(self):
        memory = {"refId": "memory:earlier", "authority": "established_memory",
                  "summary": "Test Listener shared an album.", "episodeDate": "2025-08-12",
                  "recordedAt": "2025-08-13T10:00:00Z", "temporalScope": "remembered_history_not_current_activity",
                  "matchAuthority": "topic_similarity_only", "matchedFreshSourceRefIds": ["fresh:1"]}
        self.sources.append(memory)
        prompt = review.review_prompt(self.article, {"sources": self.sources})
        evidence, _ = json.JSONDecoder().raw_decode(prompt.split("ORIGINAL_EVIDENCE_JSON: ", 1)[1])
        fragments = [item for item in evidence["fragments"] if evidence["contexts"][item["contextRef"]]["refId"] == memory["refId"]]
        self.assertEqual(len({item["contextRef"] for item in fragments}), 1)
        context = evidence["contexts"][fragments[0]["contextRef"]]
        for key in ("episodeDate", "recordedAt", "temporalScope", "matchAuthority", "matchedFreshSourceRefIds"):
            self.assertEqual(context[key], memory[key])
        summary_id = self.fragment(memory)["fragmentId"]
        memory["episodeDate"] = "2026-08-12"
        self.assertNotEqual(summary_id, self.fragment(memory)["fragmentId"])

    def test_relay_public_speech_invitation_and_publication_details_survive_contributors(self):
        relay = {"refId": "relay:1", "basisKind": "accepted_relay_continuity",
                 "summary": "I wondered about the unfinished chorus.", "publicInvitation": "Tell me what you hear.",
                 "relayPublishedAt": "2026-01-02T02:00:00Z", "originalSourceDates": [{"observedAt": "2026-01-01T23:00:00Z"}],
                 "publication_card": {"title": "A published title", "mentions": ["Test Listener"]},
                 "contributions": [{"participantAlias": "member-a", "summary": "A derived contributor description."}]}
        self.sources.append(relay)
        for field in ("summary", "publicInvitation", "relayPublishedAt", "originalSourceDates", "publication_card"):
            fragments = [item for item in review.source_fragments(self.sources)
                         if item["refId"] == relay["refId"] and item["field"] == field]
            self.assertTrue(fragments, field)
            self.assertTrue(all(item["authority"] == "derived_context" for item in fragments))
        fragment = self.fragment(relay, "publicInvitation")
        self.assertEqual(fragment["text"], relay["publicInvitation"])
        self.assertEqual(self.anchor(relay, field="publicInvitation", use="event")[1], "source_review_derived_as_fact")

    def test_impression_has_own_speaker_and_cannot_become_event_evidence(self):
        impression = {"refId": "reflection:impression:1", "basisKind": "moment_impression",
                      "authority": "bnl_subjective_perspective_not_event_evidence",
                      "summary": "The exchange was playful.", "impression": "I enjoy that friction.",
                      "reason": "The joke leaves room for affection.",
                      "contributions": [{"participantAlias": "member-a", "summary": "I enjoy that friction."}]}
        self.sources.append(impression)
        self.assertEqual(review.source_authority(impression), "subjective_context")
        for field in ("impression", "reason"):
            bound, reason = self.anchor(impression, field=field, use="context")
            self.assertEqual(reason, "")
            self.assertEqual((bound["speaker"], bound["quote"]), ("bnl", impression[field]))
            self.assertEqual(self.anchor(impression, field=field, use="event")[1], "source_review_derived_as_fact")

    def test_inference_and_derived_roles_cannot_be_promoted_by_original_marker(self):
        for extra in ({"laneType": "bnl_inference"}, {"basisKind": "moment_impression"},
                      {"sourceRole": "bnl_interpretation"}):
            source = {**self.sources[0], **extra}
            self.sources = [source]
            self.assertNotEqual(review.source_authority(source), "original")
            self.assertEqual(self.anchor(source, use="event")[1], "source_review_derived_as_fact")

    def test_original_impression_evidence_keeps_its_separate_original_authority(self):
        self.sources[0].pop("sourceKind")
        self.assertEqual(review.source_authority(self.sources[0]), "original")
        self.assertEqual(self.anchor(self.sources[0])[1], "")

    def test_humor_and_personal_reflection_need_no_fabricated_event_anchors(self):
        receipt, reason, targets = self.accept()
        self.assertEqual((reason, targets), ("", []))
        self.assertTrue(all(not unit["claims"] for unit in receipt["units"]))

    def test_whole_entry_chronology_failure_is_not_hidden_by_valid_sentence_anchors(self):
        data = self.response()
        assessment = next(item for item in data["assessments"] if item["check"] == "event_relationships")
        assessment.update(verdict="unsupported", sourceRefIds=["fresh:1"],
                          issues=["The narrative reverses the supplied message order."])
        self.assertEqual(self.accept(data)[1], "source_attribution_failed")

    def test_continuity_can_reuse_only_an_existing_body_context_lane_declaration(self):
        memory = {"refId": "memory:1", "authority": "established_memory",
                  "summary": "Test Listener shared an album last year."}
        self.sources.append(memory)
        self.article["sections"][0]["body"] = "I remembered Test Listener's album from last year."
        self.article["metadata"]["continuityNotes"] = ["Test Listener shared an album last year."]
        declaration = {"laneRefId": "memory:1", "laneType": "established_broadcast_memory",
                       "sectionHeading": "Unfinished thoughts", "claim": self.article["sections"][0]["body"],
                       "basisRefIds": ["memory:1", "fresh:1"]}
        self.article["metadata"]["contextUses"] = [declaration]
        data = self.response()
        unit = next(item for item in data["units"] if item["unitId"].startswith("metadata.continuityNotes"))
        unit["claims"] = [fixture_claim("Test Listener shared an album last year.", self.fragment(memory))]
        contract = {"memory:1": {"laneType": "established_broadcast_memory"}}
        self.assertEqual(self.accept(data, context_contract=contract)[1], "")
        declaration["sectionHeading"] = "A nonexistent section"
        self.assertEqual(self.accept(data, context_contract=contract)[1], "source_attribution_failed")
        self.article["metadata"]["contextUses"] = []
        self.assertEqual(self.accept(data, context_contract=contract)[1], "source_attribution_failed")
        self.article["metadata"]["contextUses"] = [{**declaration, "sectionHeading": "Unfinished thoughts"}]
        title = next(item for item in data["units"] if item["unitId"].startswith("title:"))
        title["claims"] = copy.deepcopy(unit["claims"])
        self.assertEqual(self.accept(data, context_contract=contract)[1], "source_attribution_failed")


if __name__ == "__main__":
    unittest.main()
