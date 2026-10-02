"""Focused audit protocol regressions, not semantic model-quality assertions."""
import copy
import json
import unittest

import bnl_journal_attribution as review
from tests.journal_review_helpers import fixture_issue


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
        return {"reviewedUnitIds": [unit["unitId"] for unit in review.public_units(self.article)],
                "issues": [], "verdict": "supported"}

    def accept(self, response=None, **kwargs):
        return review.accept_review(json.dumps(self.response() if response is None else response),
                                    self.article, self.sources, **kwargs)

    def fragment(self):
        return next(item for item in review.source_fragments(self.sources) if item["field"] == "summary")

    def issue(self, unit_ids=None, fragments=None):
        return fixture_issue(unit_ids or ["sections[0].body:0"],
                             [self.fragment()] if fragments is None else fragments,
                             source_meaning="The member asked who made the recording.",
                             added_premise="The linked recording has no creator credit.",
                             repair="Keep the curiosity without asserting unseen page contents.")

    def test_receipt_covers_prose_and_future_continuity_without_sentence_exemptions(self):
        receipt, reason, targets = self.accept()
        self.assertEqual((reason, targets), ("", []))
        self.assertEqual(receipt["version"], 8)
        self.assertEqual(receipt["reviewedUnitIds"], self.response()["reviewedUnitIds"])
        self.assertEqual(receipt["issues"], [])
        self.assertEqual(receipt["assessments"], [])
        self.assertNotIn("units", receipt)
        fields = {unit["field"] for unit in review.public_units(self.article)}
        self.assertTrue({"title", "excerpt", "sections[0].body", "metadata.topicTags[0]",
                         "metadata.continuityNotes[0]", "metadata.unresolvedQuestions[0]"}.issubset(fields))

    def test_blank_partial_duplicate_unknown_or_nonstring_coverage_fails_closed(self):
        complete = self.response()["reviewedUnitIds"]
        for coverage in (None, "all", [], [""], complete[:-1], complete + [complete[0]],
                         [*complete[:-1], complete[0]], [*complete[:-1], "unknown"],
                         [*complete[:-1], {}], [*complete[:-1], None]):
            with self.subTest(coverage=coverage):
                data = self.response()
                data["reviewedUnitIds"] = coverage
                self.assertEqual(self.accept(data), (None, "source_review_incomplete", []))

    def test_coverage_requires_continuity_and_questions_too(self):
        data = self.response()
        data["reviewedUnitIds"] = [unit for unit in data["reviewedUnitIds"] if not unit.startswith("metadata.")]
        self.assertEqual(self.accept(data)[1], "source_review_incomplete")

    def test_valid_issue_beats_every_global_verdict_even_for_reflective_prose(self):
        # The issue is a test-supplied semantic finding, not an automated oracle.
        for verdict in ("supported", "unsupported", "uncertain"):
            data = self.response()
            data.update(verdict=verdict, issues=[self.issue()])
            receipt, reason, targets = self.accept(data)
            self.assertIsNone(receipt)
            self.assertEqual(reason, "source_attribution_failed")
            self.assertEqual(targets[0]["check"], "source_entailment")
            self.assertEqual(targets[0]["claim"], data["issues"][0]["addedPremise"])
            self.assertEqual(targets[0]["sourceMeaning"], data["issues"][0]["sourceMeaning"])
            self.assertEqual(targets[0]["explanation"], data["issues"][0]["repair"])

    def test_metaphor_label_cannot_remove_a_located_external_premise(self):
        data = self.response()
        data["issues"] = [self.issue()]
        for key in ("nonFactualReason", "claimType", "use", "authority", "evidenceKind"):
            altered = copy.deepcopy(data)
            altered["issues"][0][key] = "metaphor"
            self.assertEqual(self.accept(altered), (None, "source_review_invalid_grounding", []))
        data["nonFactualReason"] = "Everything is figurative."
        self.assertEqual(self.accept(data), (None, "source_review_invalid", []))

    def test_legacy_per_unit_certificates_cannot_approve_any_candidate(self):
        for old in ({"units": [], "assessments": [], "verdict": "supported"},
                    {**self.response(), "units": [{"unitId": "title:0", "claims": [],
                      "nonFactualReason": "A title cannot have a factual premise."}]}):
            self.assertEqual(self.accept(old), (None, "source_review_invalid", []))

    def test_issue_locations_cover_exact_fields_including_private_continuity(self):
        units = ["excerpt:0", "sections[0].body:0", "metadata.continuityNotes[0]:0"]
        data = self.response()
        data["issues"] = [self.issue(units)]
        _, reason, targets = self.accept(data)
        self.assertEqual(reason, "source_attribution_failed")
        self.assertEqual(targets[0]["unitIds"], units)
        self.assertEqual(targets[0]["fieldPaths"], ["excerpt", "sections[0].body", "metadata.continuityNotes[0]"])
        self.assertEqual(targets[0]["field"], "excerpt")

    def test_issue_requires_specific_source_meaning_addition_and_repair(self):
        for field in ("sourceMeaning", "addedPremise", "repair"):
            for value in ("", " ", [], {}, None, 3):
                with self.subTest(field=field, value=value):
                    data = self.response()
                    data["issues"] = [{**self.issue(), field: value}]
                    self.assertEqual(self.accept(data)[1], "source_review_invalid_grounding")
        data = self.response()
        data["issues"] = [self.issue()]
        del data["issues"][0]["repair"]
        self.assertEqual(self.accept(data)[1], "source_review_invalid_grounding")

    def test_issue_locations_are_nonempty_valid_and_unique(self):
        for unit_ids in ([], None, {}, ["unknown"], ["title:0", "title:0"], [{}]):
            data = self.response()
            data["issues"] = [{**self.issue(), "unitIds": unit_ids}]
            self.assertEqual(self.accept(data), (None, "source_review_incomplete", []))

    def test_missing_evidence_can_be_located_without_inventing_an_anchor(self):
        data = self.response()
        data["issues"] = [self.issue(fragments=[])]
        data["issues"][0]["sourceMeaning"] = "No queue event is supplied."
        _, reason, targets = self.accept(data)
        self.assertEqual(reason, "source_attribution_failed")
        self.assertEqual(targets[0]["evidence"], [])
        self.assertEqual(targets[0]["sourceRefIds"], [])

    def test_negative_global_verdict_needs_a_concrete_located_issue(self):
        for verdict in ("unsupported", "uncertain"):
            data = self.response()
            data["verdict"] = verdict
            self.assertEqual(self.accept(data), (None, "source_review_invalid", []))

    def test_fragment_ids_cannot_be_forged_retyped_or_duplicated(self):
        fragment_id = self.fragment()["fragmentId"]
        for ids in (["f:unknown"], [fragment_id, fragment_id], [None], [{}], [1], None, "all"):
            data = self.response()
            data["issues"] = [{**self.issue(), "fragmentIds": ids}]
            self.assertEqual(self.accept(data), (None, "source_review_invalid_anchor", []))

    def test_source_edits_invalidate_selected_fragment_identity(self):
        data = self.response()
        data["issues"] = [self.issue()]
        original = copy.deepcopy(self.sources)
        for mutate in (
            lambda s: s.update(summary="A different original."),
            lambda s: s.update(participantAlias="another-member"),
            lambda s: s.update(observedAt="2026-01-03T01:00:00Z"),
            lambda s: s["messageContext"].update(linkContent="inspected"),
            lambda s: s.update(sourceRole="bnl_interpretation"),
        ):
            self.sources = copy.deepcopy(original)
            mutate(self.sources[0])
            self.assertEqual(self.accept(data)[1], "source_review_invalid_anchor")

    def test_evidence_digest_binds_roles_dates_privacy_limits_and_context_contract(self):
        contract = {"memory:1": {"laneType": "established_broadcast_memory"}}
        receipt, _, _ = self.accept(context_contract=contract)
        self.assertEqual(receipt["evidenceDigest"], review.evidence_digest(self.sources, context_contract=contract))
        for update in ({"sourceRole": "bnl_interpretation"}, {"observedAt": "2025-01-01"},
                       {"messageContext": {"linkContent": "inspected"}}, {"eligible": False}):
            changed = copy.deepcopy(self.sources)
            changed[0].update(update)
            self.assertNotEqual(receipt["evidenceDigest"], review.evidence_digest(changed, context_contract=contract))
        self.assertNotEqual(receipt["evidenceDigest"], review.evidence_digest(self.sources))

    def test_exact_candidate_receipt_changes_with_prose_citations_and_context_uses(self):
        receipt, _, _ = self.accept()
        for mutate in (
            lambda a: a.update(title="Another title"),
            lambda a: a["sections"][0].update(body="Another thought."),
            lambda a: a["sections"][0].update(sourceRefIds=["another"]),
            lambda a: a["sourceRefIds"].update({"Unfinished thoughts": ["another"]}),
            lambda a: a["metadata"].update(continuityNotes=["Another conclusion."]),
            lambda a: a["metadata"].update(contextUses=[{"laneRefId": "memory:1"}]),
        ):
            changed = copy.deepcopy(self.article)
            mutate(changed)
            self.assertNotEqual(receipt["articleDigest"], review.article_digest(changed))

    def test_duplicate_missing_or_invalid_source_refs_and_contract_fail_closed(self):
        for sources in (None, {}, [{}], [{"refId": ""}], [{"refId": None}], self.sources * 2):
            self.assertEqual(review.accept_review(json.dumps(self.response()), self.article, sources)[1],
                             "source_review_invalid")
        self.assertEqual(self.accept(context_contract=[])[1], "source_review_invalid")

    def test_success_fixture_does_not_claim_to_verify_actual_model_semantics(self):
        # No deterministic pronoun or sentiment quota: this semantic judgment is
        # supplied by a fixture. Live evaluations must establish writing quality.
        self.article["sections"][0]["body"] = "That question amused me. What if satellites argued over stickers?"
        self.assertEqual(self.accept()[1], "")

    def test_schema_has_no_pass_certificates_or_model_assigned_source_permissions(self):
        schema = review.response_schema()
        self.assertEqual(schema["required"], ["reviewedUnitIds", "issues", "verdict"])
        self.assertEqual(schema["propertyOrdering"], schema["required"])
        issue = schema["properties"]["issues"]["items"]
        self.assertEqual(issue["required"], ["unitIds", "fragmentIds", "sourceMeaning", "addedPremise", "repair"])
        for obsolete in ("nonFactualReason", "claimType", "sourceStance", '"use"', "assessments"):
            self.assertNotIn(obsolete, json.dumps(schema))


if __name__ == "__main__":
    unittest.main()
