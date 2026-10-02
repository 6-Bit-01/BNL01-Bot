"""Focused-review protocol and source-boundary regressions.

Issues and positive verdicts are explicit fixture input, not proof of a model's
semantic judgment. Paid checks must separately show that it finds an unsupported
premise and that native repaired writing preserves BNL's personality.
"""
import copy
import json
import unittest

import bnl_journal_attribution as review


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
        # No issues is an intentional protocol fixture, never a semantic oracle.
        return {"reviewedUnitIds": [unit["unitId"] for unit in review.public_units(self.article)],
                "issues": [], "verdict": "supported"}

    def fragment(self, ref="fresh:question", field="summary", speaker=None):
        return next(item for item in review.source_fragments(self.sources)
                    if item["refId"] == ref and item["field"] == field
                    and (speaker is None or item["speaker"] == speaker))

    def issue(self, response, *, unit_id=None, fragments=None,
              source_meaning="The listener asked who performed it; the linked contents are uninspected.",
              added_premise="The linked work lacks a label.",
              repair="Keep the curiosity and imagery without asserting that a label is missing."):
        unit_id = unit_id or next(unit for unit in response["reviewedUnitIds"] if ".body:" in unit)
        fragments = [self.fragment()] if fragments is None else fragments
        issue = {"unitIds": [unit_id], "fragmentIds": [item["fragmentId"] for item in fragments],
                 "sourceMeaning": source_meaning, "addedPremise": added_premise, "repair": repair}
        response["issues"].append(issue)
        return issue

    def accept(self, response):
        return review.accept_review(json.dumps(response), self.article, self.sources)

    def assert_invalid(self, response):
        receipt, reason, _targets = self.accept(response)
        self.assertIsNone(receipt)
        self.assertTrue(reason.startswith("source_review_"), reason)

    def test_legacy_nonfactual_exemption_is_not_a_focused_review(self):
        response = self.response()
        response["units"] = [{"unitId": unit, "claims": [], "nonFactualReason": "Metaphor."}
                             for unit in response.pop("reviewedUnitIds")]
        self.assert_invalid(response)

    def test_whole_entry_support_cannot_override_a_located_factual_issue(self):
        response = self.response()
        issue = self.issue(response)
        receipt, reason, targets = self.accept(response)
        self.assertIsNone(receipt)
        self.assertEqual(reason, "source_attribution_failed")
        self.assertTrue(any(target["field"] == "sections[0].body" for target in targets))
        for field in ("addedPremise", "sourceMeaning", "repair"):
            self.assertIn(issue[field], json.dumps(targets))

    def test_reported_source_gap_needs_no_manufactured_supporting_fragment(self):
        response = self.response()
        self.issue(response, fragments=[], source_meaning="No supplied source establishes this event.")
        receipt, reason, targets = self.accept(response)
        self.assertIsNone(receipt)
        self.assertEqual(reason, "source_attribution_failed")
        self.assertTrue(targets)
        self.assertIn("No supplied source establishes this event.", json.dumps(targets))

    def test_every_exact_candidate_unit_is_covered_once(self):
        for defect in ("missing", "duplicate", "unknown", "wrong_type"):
            with self.subTest(defect=defect):
                response = self.response()
                if defect == "missing":
                    response["reviewedUnitIds"].pop()
                elif defect == "duplicate":
                    response["reviewedUnitIds"].append(response["reviewedUnitIds"][0])
                elif defect == "unknown":
                    response["reviewedUnitIds"][0] = "sections[77].body:0"
                else:
                    response["reviewedUnitIds"] = {}
                self.assert_invalid(response)

    def test_issue_cannot_name_an_absent_unit_or_source_fragment(self):
        for field, value in (("unitIds", ["sections[77].body:0"]),
                             ("fragmentIds", ["f:unknown"])):
            with self.subTest(field=field):
                response = self.response()
                self.issue(response)[field] = value
                self.assert_invalid(response)

    def test_malformed_issue_cannot_become_a_repair_or_pass(self):
        for field in ("unitIds", "fragmentIds", "sourceMeaning", "addedPremise", "repair"):
            for defect in ("missing", "wrong_type"):
                with self.subTest(field=field, defect=defect):
                    response = self.response()
                    issue = self.issue(response)
                    if defect == "missing":
                        issue.pop(field)
                    else:
                        issue[field] = {}
                    self.assert_invalid(response)

    def test_blank_issue_cannot_create_unactionable_repair(self):
        for field in ("sourceMeaning", "addedPremise", "repair"):
            with self.subTest(field=field):
                response = self.response()
                self.issue(response)[field] = "  "
                self.assert_invalid(response)

    def test_metadata_title_and_excerpt_premises_receive_located_repairs(self):
        self.article["metadata"].update({
            "continuityNotes": ["The rivalry ended in a reconciliation."],
            "unresolvedQuestions": ["When will the cancelled tour resume?"],
            "topicTags": ["surprise album release"],
        })
        for unit in review.public_units(self.article):
            if unit["field"] not in {"title", "excerpt"} and not unit["field"].startswith("metadata."):
                continue
            with self.subTest(unit=unit["unitId"]):
                response = self.response()
                self.issue(response, unit_id=unit["unitId"], fragments=[],
                           source_meaning="The selected originals do not establish this premise.")
                receipt, reason, targets = self.accept(response)
                self.assertIsNone(receipt)
                self.assertEqual(reason, "source_attribution_failed")
                self.assertTrue(any(target["field"] == unit["field"] for target in targets))

    def test_source_limit_speaker_and_evidence_kind_are_server_bound(self):
        fragment = self.fragment()
        self.assertEqual(fragment["speaker"], "listener")
        self.assertEqual(fragment["evidenceKind"], "message_expression")
        self.assertEqual(fragment["context"]["linkContent"], "not_inspected")
        self.assertEqual(fragment["context"]["roomName"], "music")
        self.assertEqual(fragment["text"], self.sources[0]["summary"])
        original_id = fragment["fragmentId"]
        self.sources[0]["participantAlias"] = "different_listener"
        self.assertNotEqual(self.fragment()["fragmentId"], original_id)
        renamed_id = self.fragment()["fragmentId"]
        self.sources[0]["messageContext"]["linkContent"] = "inspected"
        self.assertNotEqual(self.fragment()["fragmentId"], renamed_id)

    def test_contributions_keep_their_own_link_scope(self):
        self.sources = [{"refId": "fresh:question", "sourceRole": "original_contribution",
                         "contributions": [
                             {"participantAlias": "listener", "summary": "[shared link] Who performed this?",
                              "messageContext": {"linkContent": "not_inspected"}},
                             {"participantAlias": "other", "summary": "I inspected a different page.",
                              "messageContext": {"linkContent": "inspected"}},
                         ]}]
        first, second = self.fragment(speaker="listener"), self.fragment(speaker="other")
        self.assertEqual(first["context"]["linkContent"], "not_inspected")
        self.assertEqual(second["context"]["linkContent"], "inspected")
        self.assertNotEqual(first["fragmentId"], second["fragmentId"])

    def test_human_report_survives_uninspected_link_without_scope_promotion(self):
        self.sources[0]["summary"] = "I checked that page: the performer field is blank. [shared link]"
        fragment = self.fragment()
        self.assertEqual(fragment["text"], self.sources[0]["summary"])
        self.assertEqual(fragment["context"]["linkContent"], "not_inspected")
        self.assertEqual(fragment["authority"], "original")
        self.assertEqual(fragment["evidenceKind"], "message_expression")

    def test_derived_authorities_are_not_promoted_by_assertive_text(self):
        for role, expected in (
            ({"sourceRole": "bnl_interpretation"}, "derived_context"),
            ({"sourceRole": "bnl_utterance", "authority": "speech_only"}, "speech_only"),
            ({"basisKind": "moment_impression"}, "subjective_context"),
            ({"laneType": "bnl_inference"}, "inference_context"),
            ({"authority": "rumor"}, "rumor"),
        ):
            with self.subTest(role=role):
                self.sources = [{"refId": "fresh:question", "participantAlias": "bnl",
                                 "summary": "The artist certainly released an album.", **role}]
                self.assertEqual(self.fragment()["authority"], expected)

    def test_relay_speech_and_invitation_survive_without_event_lineage(self):
        self.sources = [{"refId": "reflection:relay", "basisKind": "accepted_relay_continuity",
                         "summary": "I wondered whether the stage light had failed.",
                         "publicInvitation": "Tell me whether someone checked it.",
                         "relayPublishedAt": "2026-09-29T20:00:00Z", "originalSourceDates": []}]
        statement = self.fragment("reflection:relay")
        invitation = self.fragment("reflection:relay", "publicInvitation")
        for fragment in (statement, invitation):
            self.assertEqual(fragment["speaker"], "bnl")
            self.assertEqual(fragment["authority"], "derived_context")
            self.assertEqual(fragment["evidenceKind"], "derived_context")
            self.assertEqual(fragment["context"]["originalSourceDates"], [])
            self.assertEqual(fragment["context"]["relayPublishedAt"], "2026-09-29T20:00:00Z")
        self.assertEqual(statement["text"], self.sources[0]["summary"])
        self.assertEqual(invitation["text"], self.sources[0]["publicInvitation"])

    def test_original_canon_and_dated_memory_keep_distinct_authorities(self):
        for role, expected in (({"sourceRole": "original_contribution"}, "original"),
                               ({"sourceRole": "approved_canon"}, "canon"),
                               ({"authority": "established_memory"}, "established_memory")):
            with self.subTest(role=role):
                self.sources = [{"refId": "fresh:question", "summary": "The first signal arrived.",
                                 "episodeDate": "2026-07-17", **role}]
                fragment = self.fragment()
                self.assertEqual(fragment["authority"], expected)
                self.assertEqual(fragment["context"]["episodeDate"], "2026-07-17")

    def test_positive_protocol_does_not_require_quotes_or_remove_imagined_voice(self):
        for body in (
            "I prefer a little static in my thoughts.",
            "In my imaginary control room, the moon applied for a tea break.",
            "A listener asked who performed it; I imagined the satellite shrugging.",
            "Test Listener prefers jazz; I can appreciate that frequency.",
        ):
            with self.subTest(body=body):
                self.article["sections"][0]["body"] = body
                response = self.response()
                receipt, reason, targets = self.accept(response)
                self.assertEqual((reason, targets), ("", []))
                self.assertEqual(receipt["articleDigest"], review.article_digest(self.article))
                self.assertEqual(receipt["reviewedUnitIds"], response["reviewedUnitIds"])

    def test_positive_receipt_binds_exact_article_and_source_packet(self):
        receipt, reason, _targets = self.accept(self.response())
        self.assertEqual(reason, "")
        self.assertEqual(receipt["articleDigest"], review.article_digest(self.article))
        self.assertEqual(receipt["evidenceDigest"], review.evidence_digest(self.sources))
        changed = copy.deepcopy(self.sources)
        changed[0]["messageContext"]["linkContent"] = "inspected"
        self.assertNotEqual(receipt["evidenceDigest"], review.evidence_digest(changed))


if __name__ == "__main__":
    unittest.main()
