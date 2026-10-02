"""Protocol regressions, not proof that a model identifies every hidden premise.

Each semantic comparison below is explicit fixture input. These tests prove that
the consumer enforces that comparison even when the overall review says pass.
"""
import json
import unittest

import bnl_journal_attribution as review
from tests.journal_review_helpers import fixture_grounding


class JournalEvidenceObligationTests(unittest.TestCase):
    def setUp(self):
        self.sources = [{
            "refId": "fresh:question", "sourceRole": "original_contribution",
            "participantAlias": "listener", "summary": "[shared link] Who performed this?",
            "messageContext": {"roomName": "music", "linkContent": "not_inspected"},
        }]
        self.article = {
            "title": "Static in my thoughts", "excerpt": "I enjoy a loose end.",
            "sections": [{"heading": "A thought I kept", "body": "I resent the missing label."}],
            "sourceRefIds": {"A thought I kept": ["fresh:question"]},
            "metadata": {"contextUses": []},
        }

    def response(self):
        units = review.public_units(self.article)
        return {
            "units": [{"unitId": unit["unitId"], "spans": [{
                "text": unit["text"], "kind": "creative", "evidence": [],
                "grounding": fixture_grounding(unit["text"], factual=False),
                "issues": [], "verdict": "supported",
            }]} for unit in units],
            "assessments": [{
                "check": check, "unitIds": [unit["unitId"] for unit in units],
                "sourceRefIds": [], "explanation": "Explicit passing protocol fixture.",
                "issues": [], "verdict": "supported",
            } for check in review.ASSESSMENT_CHECKS],
            "verdict": "supported",
        }

    def body(self, response):
        return next(unit for unit in response["units"] if ".body:" in unit["unitId"])["spans"][0]

    def external(self, span, *, claim="The linked work lacks a label.", **changes):
        span.update(kind="reflection", evidence=[{
            "refId": self.sources[0]["refId"], "quote": self.sources[0]["summary"],
            "speaker": "listener", "use": "speech",
        }])
        span["grounding"] = fixture_grounding(claim, source_meaning="A listener asked who performed it.")
        premise = span["grounding"]["externalPremises"][0]
        premise.update(claimType="external_fact", sourceStance="assertion", **changes)
        return premise

    def accept(self, response):
        return review.accept_review(json.dumps(response), self.article, self.sources)

    def test_old_anchor_only_approval_cannot_pass_without_semantic_comparison(self):
        response = self.response()
        self.external(self.body(response))
        for unit in response["units"]:
            for span in unit["spans"]:
                span.pop("grounding")
        self.assertEqual(self.accept(response)[1], "source_review_invalid_grounding")

    def test_every_span_kind_requires_grounding_even_when_reviewer_says_supported(self):
        for kind in ("factual", "reflection", "creative"):
            with self.subTest(kind=kind):
                response = self.response()
                span = self.body(response)
                self.external(span)
                span["kind"] = kind
                span.pop("grounding")
                self.assertEqual(self.accept(response)[1], "source_review_invalid_grounding")

    def test_acknowledged_semantic_gap_overrides_supported_span_and_whole_entry(self):
        for support in ("compatible_only", "contradicted", "unknown"):
            for kind in ("factual", "reflection", "creative"):
                with self.subTest(support=support, kind=kind):
                    response = self.response()
                    span = self.body(response)
                    self.external(span, support=support)
                    span["kind"] = kind
                    receipt, reason, targets = self.accept(response)
                    self.assertIsNone(receipt)
                    self.assertEqual(reason, "source_attribution_failed")
                    self.assertTrue(any(target["field"] == "sections[0].body" for target in targets))
                    self.assertIn("The linked work lacks a label.", json.dumps(targets))

    def test_required_assumption_rejects_even_a_declared_entailment(self):
        response = self.response()
        self.external(self.body(response), assumptions=["The question means the page has no label."])
        receipt, reason, targets = self.accept(response)
        self.assertIsNone(receipt)
        self.assertEqual(reason, "source_attribution_failed")
        self.assertIn("The question means the page has no label.", json.dumps(targets))

    def test_question_speculation_joke_or_subjective_position_cannot_establish_external_fact(self):
        for stance in ("question", "speculation", "joke", "subjective", "unknown"):
            with self.subTest(stance=stance):
                response = self.response()
                premise = self.external(self.body(response))
                premise["sourceStance"] = stance
                self.assertEqual(self.accept(response)[1], "source_attribution_failed")

    def test_actual_question_can_be_reported_without_inventing_its_answer(self):
        self.article["sections"][0]["body"] = "A listener asked who performed it; I imagined the satellite shrugging."
        response = self.response()
        premise = self.external(self.body(response), claim="A listener asked who performed it.")
        premise.update(claimType="reported_speech", sourceStance="question")
        receipt, reason, targets = self.accept(response)
        self.assertEqual((reason, targets), ("", []))
        self.assertEqual(receipt["articleDigest"], review.article_digest(self.article))

    def test_uninspected_link_cannot_prove_destination_properties(self):
        response = self.response()
        self.external(self.body(response), evidenceScope="referenced_content")
        receipt, reason, targets = self.accept(response)
        self.assertIsNone(receipt)
        self.assertEqual(reason, "source_attribution_failed")
        self.assertTrue(targets)

    def test_explicit_human_report_uses_recorded_content_without_needing_to_visit_link(self):
        self.sources[0]["summary"] = "I checked that page: the performer field is blank. [shared link]"
        self.article["sections"][0]["body"] = "The listener reported a blank performer field; I disliked the loose end."
        response = self.response()
        premise = self.external(self.body(response), claim="The listener reported a blank performer field.")
        premise.update(claimType="reported_speech", sourceMeaning="They explicitly reported inspecting a blank performer field.")
        self.assertEqual(self.accept(response)[1], "")

    def test_aggregate_source_cannot_borrow_another_contributions_inspected_scope(self):
        response = self.response()
        self.external(self.body(response), evidenceScope="referenced_content")
        original = self.sources[0]
        self.sources = [{"refId": original["refId"], "sourceRole": "original_contribution",
                         "contributions": [
                             {"participantAlias": "listener", "summary": original["summary"],
                              "messageContext": {"linkContent": "not_inspected"}},
                             {"participantAlias": "other", "summary": "I inspected a different page.",
                              "messageContext": {"linkContent": "inspected"}},
                         ]}]
        self.assertEqual(self.accept(response)[1], "source_attribution_failed")

    def test_nested_recorded_human_report_is_not_disabled_by_uninspected_link(self):
        self.sources[0]["summary"] = "I checked that page: the performer field is blank. [shared link]"
        response = self.response()
        premise = self.external(self.body(response), claim="The listener reported a blank performer field.")
        premise.update(claimType="reported_speech", sourceMeaning="They explicitly reported a blank field.")
        original = self.sources[0]
        self.sources = [{"refId": original["refId"], "sourceRole": "original_contribution",
                         "contributions": [{"participantAlias": "listener", "summary": original["summary"],
                                            "messageContext": {"linkContent": "not_inspected"}}]}]
        self.assertEqual(self.accept(response)[1], "")

    def test_missing_anchors_or_invalid_indexes_cannot_ground_supported_premise(self):
        for indexes in ([], [-1], [1], [True], ["0"], [0, 0], None):
            with self.subTest(indexes=indexes):
                response = self.response()
                self.external(self.body(response), evidenceIndexes=indexes)
                self.assertEqual(self.accept(response)[1], "source_review_invalid_grounding")

    def test_uncertain_premise_without_anchor_is_located_failure_not_manufactured_evidence(self):
        response = self.response()
        span = self.body(response)
        self.external(span, support="unknown", evidenceIndexes=[])
        span["evidence"] = []
        self.assertEqual(self.accept(response)[1], "source_attribution_failed")

    def test_factual_kind_cannot_claim_it_has_no_external_premises(self):
        response = self.response()
        span = self.body(response)
        self.external(span)
        span["kind"] = "factual"
        span["grounding"] = fixture_grounding(span["text"], factual=False)
        self.assertEqual(self.accept(response)[1], "source_review_invalid_grounding")

    def test_pure_personal_and_imagined_voice_needs_no_invented_evidence(self):
        for body in (
            "I prefer a little static in my thoughts.",
            "In my imaginary control room, the moon applied for a tea break.",
            "I might reserve a corner of my impossible antenna for whatever arrives tomorrow.",
        ):
            with self.subTest(body=body):
                self.article["sections"][0]["body"] = body
                response = self.response()
                self.body(response)["kind"] = "reflection"
                receipt, reason, targets = self.accept(response)
                self.assertEqual((reason, targets), ("", []))
                self.assertEqual(receipt["units"], response["units"])

    def test_no_external_premise_requires_specific_nonfactual_account(self):
        for value in ("", "  ", None, []):
            with self.subTest(value=value):
                response = self.response()
                self.body(response)["grounding"]["nonFactualReason"] = value
                self.assertEqual(self.accept(response)[1], "source_review_invalid_grounding")

    def test_malformed_comparison_fields_fail_closed_without_crashing(self):
        for field in ("claim", "claimType", "sourceStance", "evidenceIndexes", "sourceMeaning",
                      "support", "assumptions", "evidenceScope"):
            for mode in ("missing", "wrong_type"):
                with self.subTest(field=field, mode=mode):
                    response = self.response()
                    premise = self.external(self.body(response))
                    if mode == "missing":
                        premise.pop(field)
                    else:
                        premise[field] = {}
                    self.assertEqual(self.accept(response)[1], "source_review_invalid_grounding")

    def test_presuppositions_in_future_continuity_receive_same_obligations(self):
        cases = (
            ("unresolvedQuestions", "When will the band's cancelled tour resume?", "The band's tour was cancelled."),
            ("continuityNotes", "The rivalry ended in a reconciliation.", "The two members reconciled."),
            ("topicTags", "surprise album release", "An album was released."),
        )
        for field, wording, claim in cases:
            with self.subTest(field=field):
                self.article["metadata"] = {"contextUses": [], field: [wording]}
                response = self.response()
                unit = next(item for item in response["units"] if item["unitId"].startswith("metadata."))
                self.external(unit["spans"][0], claim=claim, support="compatible_only")
                receipt, reason, targets = self.accept(response)
                self.assertIsNone(receipt)
                self.assertEqual(reason, "source_attribution_failed")
                self.assertTrue(any(target["field"] == "metadata." + field + "[0]" for target in targets))

    def test_receipt_preserves_explicit_comparison_for_inspection(self):
        self.article["sections"][0]["body"] = "A listener asked who performed it."
        response = self.response()
        premise = self.external(self.body(response), claim="A listener asked who performed it.")
        premise.update(claimType="reported_speech", sourceStance="question")
        receipt, reason, _ = self.accept(response)
        self.assertEqual(reason, "")
        self.assertEqual(self.body(receipt)["grounding"], self.body(response)["grounding"])

    def test_derived_perspective_or_rumor_cannot_alone_establish_external_fact(self):
        for role in (
            {"sourceRole": "bnl_interpretation"},
            {"sourceRole": "bnl_utterance", "authority": "speech_only"},
            {"basisKind": "moment_impression"},
            {"laneType": "bnl_inference"},
            {"authority": "rumor"},
        ):
            with self.subTest(role=role):
                self.sources[0] = {"refId": "fresh:question", "participantAlias": "listener",
                                   "summary": "The artist released an album.", **role}
                response = self.response()
                span = self.body(response)
                self.external(span, claim="The artist released an album.")
                span["evidence"][0]["use"] = "context"
                self.assertEqual(self.accept(response)[1], "source_attribution_failed")

    def test_original_canon_or_established_memory_can_support_appropriate_external_fact(self):
        for role in ({"sourceRole": "original_contribution"},
                     {"sourceRole": "approved_canon"}, {"authority": "established_memory"}):
            with self.subTest(role=role):
                self.sources[0] = {"refId": "fresh:question", "participantAlias": "listener",
                                   "summary": "The archive's first signal arrived last year.", **role}
                response = self.response()
                span = self.body(response)
                self.external(span, claim="The archive's first signal arrived last year.")
                span["evidence"][0]["use"] = "context"
                self.assertEqual(self.accept(response)[1], "")

    def test_recorded_bnl_speech_can_support_what_bnl_said(self):
        self.sources[0] = {"refId": "fresh:question", "participantAlias": "listener",
                           "sourceRole": "bnl_utterance", "authority": "speech_only",
                           "summary": "I said the archive was humming."}
        response = self.response()
        premise = self.external(self.body(response), claim="BNL said the archive was humming.")
        premise.update(claimType="reported_speech", sourceMeaning="BNL's original recorded utterance.")
        self.assertEqual(self.accept(response)[1], "")

    def test_faithful_taste_paraphrase_needs_no_literal_said_or_public_quote(self):
        self.sources[0]["summary"] = "I love jazz."
        self.article["sections"][0]["body"] = "Test Listener prefers jazz; I can appreciate that frequency."
        response = self.response()
        premise = self.external(self.body(response), claim="Test Listener prefers jazz.")
        premise.update(claimType="reported_speech", sourceStance="subjective",
                       sourceMeaning="The listener expressed a love of jazz.")
        self.assertEqual(self.accept(response)[1], "")


if __name__ == "__main__":
    unittest.main()
