"""Selective repair flow with explicit editor fixtures, not model-quality proof."""
import copy
import json
import unittest
from unittest.mock import Mock

import bnl_journal as journal
import bnl_journal_attribution as attribution
from tests.journal_review_helpers import fixture_claim, review_inputs, supported_review


class JournalSelectiveOmissionTests(unittest.TestCase):
    def setUp(self):
        self.sources = [
            {"refId": "fresh:1", "sourceKind": "conversation",
             "summary": "I shortened the bridge to eight bars.",
             "observedAt": "2026-09-29T10:00:00Z", "channelPolicy": "public_home",
             "participantAlias": "participant-11111111"},
            {"refId": "fresh:2", "sourceKind": "conversation",
             "summary": "Could those two members make something together?",
             "observedAt": "2026-09-29T11:00:00Z", "channelPolicy": "public_home",
             "participantAlias": "participant-22222222"},
        ]
        self.impression = {
            "refId": "reflection:impression:arrangement", "basisKind": "moment_impression",
            "scope": journal.JOURNAL_REFLECTION_SCOPE, "publicSafe": True, "reuseEligible": True,
            "sourceVersion": "fixture-v1", "sourceObservedAt": "2026-09-29T10:00:00Z",
            "authority": "bnl_subjective_perspective_not_event_evidence",
            "summary": "A member described shortening the bridge.",
            "impression": "I like the room a shorter bridge leaves around a chorus.",
            "reason": "Removing a little can make the return more satisfying.",
            "evidence": [copy.deepcopy(self.sources[0])],
        }
        self.packet = {
            "entryKind": "daily", "sourceWindowStart": "2026-09-29T00:00:00Z",
            "sourceWindowEnd": "2026-09-30T00:00:00Z",
            "editorialVersion": journal.JOURNAL_EDITORIAL_VERSION,
            "safeSources": self.sources, "reflectionBasis": [self.impression],
            "evidenceCoverageContract": {
                "minimumDistinctFreshSources": 0, "requiredSourceKinds": [],
                "minimumDistinctParticipants": 0, "minimumDistinctWindowSegments": 0,
                "minimumDistinctFreshSourcesByKind": {}, "subjectiveSelectionMode": True,
            },
            "history": {},
        }

    def draft(self, *, include_unsupported_story):
        article = {
            "title": "The Space Around Eight Bars",
            "excerpt": "A shorter bridge gives me something to think about.",
            "sections": [{
                "heading": "Room for the Chorus",
                "body": "A member said they shortened the bridge to eight bars. "
                        "I like the room that leaves around a chorus; my imaginary antenna has room to lean.",
                "sourceRefIds": ["fresh:1"],
            }],
            "metadata": {"topicTags": ["arrangement"], "subjectRefs": [],
                         "continuityNotes": ["A member described an eight-bar bridge."],
                         "unresolvedQuestions": [], "contextUses": []},
        }
        if include_unsupported_story:
            article["title"] = "Eight Bars and a Secret Collaboration"
            article["excerpt"] += " A secret collaboration has arrived."
            article["sections"].append({
                "heading": "A Secret Collaboration",
                "body": "Two members released their secret collaboration tonight. "
                        "I find their surprise partnership intriguing.",
                "sourceRefIds": ["fresh:2"],
            })
            article["metadata"]["topicTags"].append("secret collaboration")
            article["metadata"]["subjectRefs"].append("secret collaboration")
            article["metadata"]["continuityNotes"].append("The secret collaboration was released.")
            article["metadata"]["unresolvedQuestions"].append("What follows the secret collaboration?")
        return json.dumps(article)

    def rejected_optional_story(self, prompt):
        response = json.loads(supported_review(prompt))
        units, evidence = review_inputs(prompt)
        original = next(fragment for fragment in evidence["fragments"]
                        if fragment["refId"] == "fresh:2" and fragment["field"] == "summary")
        target = next(unit for unit in units
                      if unit["field"] == "sections[1].body" and "released" in unit["text"])
        review = next(unit for unit in response["units"] if unit["unitId"] == target["unitId"])
        review["claims"] = [fixture_claim(
            "Two members released a collaboration.", original,
            claimType="external_fact", sourceStance="question", support="unknown",
            assumptions=["A proposed collaboration occurred and was released."],
        )]
        return json.dumps(response)

    def test_uncertain_framing_does_not_bypass_the_source_reviewer(self):
        raw = json.loads(self.draft(include_unsupported_story=True))
        raw["sections"][1]["body"] = "Apparently " + raw["sections"][1]["body"]
        outputs = iter([json.dumps(raw), self.rejected_optional_story])

        def generate(_packet, prompt):
            output = next(outputs)
            return output(prompt) if callable(output) else output

        generator = Mock(side_effect=generate)
        guard = Mock(return_value="")
        packet_before = copy.deepcopy(self.packet)
        article, reason, advisory = journal._generate_article_with_repairs(
            self.packet, generator, [], max_attempts=2, generation_guard=guard,
        )
        self.assertEqual(generator.call_count, 2)
        self.assertEqual(guard.call_count, 4)
        self.assertIsNone(article)
        self.assertEqual(reason, "source_attribution_failed")
        self.assertFalse(advisory)
        self.assertTrue(generator.call_args_list[1].args[1].startswith(attribution.REVIEW_PREFIX))
        self.assertEqual(self.packet, packet_before)

    def test_unsupported_selected_story_can_be_omitted_and_exact_remainder_is_reviewed(self):
        first = self.draft(include_unsupported_story=True)
        revised = self.draft(include_unsupported_story=False)
        outputs = iter([first, self.rejected_optional_story, revised, supported_review])

        def generate(_packet, prompt):
            output = next(outputs)
            return output(prompt) if callable(output) else output

        generator = Mock(side_effect=generate)
        guard = Mock(return_value="")
        packet_before = copy.deepcopy(self.packet)
        article, reason, advisory = journal._generate_article_with_repairs(
            self.packet, generator, [], generation_guard=guard,
        )
        self.assertEqual(generator.call_count, 4)
        self.assertEqual(guard.call_count, 8)
        self.assertEqual(reason, "")
        self.assertFalse(advisory)
        expected = journal.parse_generated_json(revised)
        receipt = article["metadata"].pop("sourceReview")
        self.assertEqual(article, expected)
        self.assertEqual(receipt["articleDigest"], attribution.article_digest(expected))
        self.assertEqual(self.packet, packet_before)
        self.assertEqual(journal._article_cited_refs(article), {"fresh:1"})
        self.assertNotIn("collaboration", json.dumps(article).lower())
        self.assertEqual(journal.validate_article(article, self.packet), "")

        first_units, first_evidence = review_inputs(generator.call_args_list[1].args[1])
        final_units, final_evidence = review_inputs(generator.call_args_list[3].args[1])
        self.assertNotEqual(first_units, final_units)
        self.assertEqual(final_units, attribution.public_units(expected))
        # Original evidence stays available; omission does not hide a source
        # from the reviewer or mutate the source-selection owner.
        self.assertEqual(first_evidence, final_evidence)
        self.assertTrue(any(fragment["refId"] == "fresh:2" for fragment in final_evidence["fragments"]))
        repair_prompt = generator.call_args_list[2].args[1]
        self.assertIn("including the affected story already selected", repair_prompt)
        self.assertIn("never remove a qualification while keeping the claim it qualifies", repair_prompt)
        self.assertIn("Remove its dependent wording", repair_prompt)
        self.assertIn("unresolved questions and other metadata", repair_prompt)
        self.assertIn("Complete previous draft (not evidence)", repair_prompt)

    def test_selection_does_not_relax_current_activity_or_reference_checks(self):
        article = journal.parse_generated_json(self.draft(include_unsupported_story=False))
        heading = article["sections"][0]["heading"]
        article["sourceRefIds"][heading] = [self.impression["refId"]]
        article["sections"][0]["body"] = "Tonight a member released a new track."
        self.assertEqual(journal.validate_article(article, self.packet), "current_activity_without_fresh_source")
        article["sourceRefIds"][heading] = ["fresh:missing"]
        self.assertEqual(journal.validate_article(article, self.packet), "invalid_section_source_refs")

    def test_repair_can_keep_the_uncertain_topic_as_a_question_and_personal_reaction(self):
        first = self.draft(include_unsupported_story=True)
        revised = json.loads(self.draft(include_unsupported_story=False))
        revised["sections"].append({
            "heading": "A Possibility Worth Keeping",
            "body": "A member asked whether two others could make something together. "
                    "I enjoyed the possibility; my imaginary antenna tilted toward that question.",
            "sourceRefIds": ["fresh:2"],
        })
        revised["metadata"]["continuityNotes"].append("A member raised the possibility of a collaboration.")
        revised["metadata"]["unresolvedQuestions"] = ["What might they make together?"]
        raw = json.dumps(revised)
        outputs = iter([first, self.rejected_optional_story, raw, supported_review])

        def generate(_packet, prompt):
            value = next(outputs)
            return value(prompt) if callable(value) else value

        generator = Mock(side_effect=generate)
        before = copy.deepcopy(self.packet)
        article, reason, _ = journal._generate_article_with_repairs(self.packet, generator, [])
        self.assertEqual(reason, "")
        self.assertEqual(generator.call_count, 4)
        expected = journal.parse_generated_json(raw)
        receipt = article["metadata"].pop("sourceReview")
        self.assertEqual(article, expected)
        self.assertEqual(receipt["articleDigest"], attribution.article_digest(expected))
        self.assertEqual(journal._article_cited_refs(article), {"fresh:1", "fresh:2"})
        self.assertEqual(self.packet, before)
        self.assertIn("uncertainty alone is no reason to discard", generator.call_args_list[0].args[1])
        self.assertIn("An original question or uncertainty may remain as such", generator.call_args_list[2].args[1])


if __name__ == "__main__":
    unittest.main()
