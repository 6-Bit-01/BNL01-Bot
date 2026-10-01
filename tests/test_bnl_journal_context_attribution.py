"""Memory attribution must identify claim content, not grammar or a date alone."""
import copy
import json
import unittest

import bnl_journal as journal
from tests.journal_review_helpers import is_source_review, supported_review_with_anchor


class JournalContextAttributionTests(unittest.TestCase):
    def setUp(self):
        self.packet = {
            "entryKind": "manual",
            "editorialVersion": journal.JOURNAL_EDITORIAL_VERSION,
            "sourceWindowStart": "2026-09-28T00:00:00Z",
            "sourceWindowEnd": "2026-09-29T00:00:00Z",
            "safeSources": [
                {"refId": "fresh:1", "sourceKind": "conversation",
                 "summary": "I watched last week's show quietly while getting used to the room.",
                 "publicSpeakerName": "Test Listener", "participantAlias": "participant-listener",
                 "sourceRole": "original_contribution"},
                {"refId": "fresh:2", "sourceKind": "conversation",
                 "summary": "The fragmented instances are returning.",
                 "publicSpeakerName": "Test Signal", "participantAlias": "participant-signal",
                 "sourceRole": "original_contribution"},
                {"refId": "fresh:3", "sourceKind": "conversation",
                 "summary": "Do you remember the tangled broadcast cables?",
                 "publicSpeakerName": "Test Listener", "participantAlias": "participant-listener",
                 "sourceRole": "original_contribution"},
            ],
            "privatePublicPeople": [
                {"publicName": "Test Listener", "participantAlias": "participant-listener",
                 "sourceRefIds": ["fresh:1", "fresh:3"]},
                {"publicName": "Test Signal", "participantAlias": "participant-signal", "sourceRefIds": ["fresh:2"]},
            ],
            "generationContextLanes": {
                "establishedBroadcastMemory": [
                    {"laneRefId": "memory:cables", "epistemicStatus": "established_network_record",
                     "summary": "Someone said that the cables for the show were tangled and a technician was trying to fix them.",
                     "matchedFreshSourceRefIds": ["fresh:3"]},
                    {"laneRefId": "memory:transmission", "epistemicStatus": "established_network_record",
                     "summary": "An imaginary pirate hacked the feed last week and escaped a prison.",
                     "matchedFreshSourceRefIds": ["fresh:2"]},
                ],
            },
            "history": {},
        }

    @staticmethod
    def article(body):
        return {
            "title": "Another Listen", "excerpt": "A little room for a listener.",
            "sections": [{"heading": "Room to Listen", "body": body}],
            "sourceRefIds": {"Room to Listen": ["fresh:1", "fresh:2", "fresh:3"]},
            "metadata": {"contextUses": []},
        }

    def review_with_anchor(self, article, source_ref, *, unit_id="sections[0].body:0", packet=None):
        packet = self.packet if packet is None else packet
        evidence = journal._source_review_evidence(packet)
        prompt = journal.attribution.review_prompt(article, evidence)
        verdict = supported_review_with_anchor(prompt, unit_id=unit_id, source_ref=source_ref)
        return journal.attribution.accept_review(
            verdict, article, evidence["sources"], context_contract=journal._context_lane_ref_contract(packet))

    def test_personal_reflection_does_not_borrow_a_memory_through_pronouns_and_grammar(self):
        article = self.article(
            "I find a quiet comfort in watching them explore electronic textures while Test Signal warned "
            "that fragmented instances were returning.")
        details = []
        self.assertEqual(journal.validate_article(article, self.packet, [], repair_details=details), "")
        self.assertEqual(details, [])
        self.assertEqual(article["metadata"]["contextUses"], [])

    def test_fresh_supported_paraphrases_do_not_claim_unrelated_last_week_memory(self):
        for body in (
            "Test Listener clarified that they quietly watched last week's show while getting used to the room.",
            "Test Listener explained that they silently observed last week's show to get comfortable with the room.",
            "Test Listener was getting used to the room after quietly listening to last week's show.",
        ):
            with self.subTest(body=body):
                self.assertEqual(journal.validate_article(self.article(body), self.packet, []), "")

    def test_relative_time_without_an_event_does_not_become_distinctive_among_lanes(self):
        packet = copy.deepcopy(self.packet)
        packet["generationContextLanes"]["establishedBroadcastMemory"] = [{
            "laneRefId": "memory:time", "summary": "That was last week.",
            "matchedFreshSourceRefIds": ["fresh:1"],
        }]
        contract = journal._context_lane_ref_contract(packet)["memory:time"]
        self.assertEqual(contract["claimTerms"], set())
        self.assertEqual(contract["distinctiveClaimTerms"], set())
        self.assertEqual(journal.validate_article(self.article(
            "Test Listener watched last week's show quietly."), packet, []), "")

    def test_distinctive_historical_fact_still_requires_memory_declaration(self):
        article = self.article(
            "During the earlier show, tangled cables sent a technician scrambling to repair the feed.")
        self.assertEqual(journal.validate_article(article, self.packet, []), "")
        receipt, reason, details = self.review_with_anchor(article, "memory:cables")
        self.assertIsNone(receipt)
        self.assertEqual(reason, "source_attribution_failed")
        self.assertTrue(any(item.get("laneRefId") == "memory:cables"
                            and item.get("check") == "missing_context_declaration" for item in details))

    def test_relative_date_does_not_exempt_an_actual_historical_event(self):
        article = self.article("Last week, an imaginary pirate hacked the feed and escaped a prison.")
        self.assertEqual(journal.validate_article(article, self.packet, []), "")
        self.assertEqual(self.review_with_anchor(article, "memory:transmission")[1], "source_attribution_failed")

    def test_correctly_declared_historical_fact_remains_usable(self):
        claim = "During the earlier show, tangled cables sent a technician scrambling to repair the feed."
        article = self.article(claim)
        article["metadata"]["contextUses"] = [{
            "laneType": "established_broadcast_memory", "laneRefId": "memory:cables",
            "sectionHeading": "Room to Listen", "claim": claim,
            "basisRefIds": ["memory:cables", "fresh:3"],
        }]
        self.assertEqual(journal.validate_article(article, self.packet, []), "")
        self.assertEqual(self.review_with_anchor(article, "memory:cables")[1], "")

    def test_two_incidental_distinctive_words_do_not_spend_a_repair_before_original_review(self):
        packet = copy.deepcopy(self.packet)
        fresh = "I am taking time to get used to this room over the coming month."
        packet["safeSources"][0]["summary"] = fresh
        packet["generationContextLanes"]["establishedBroadcastMemory"] = [{
            "laneRefId": "memory:unrelated", "epistemicStatus": "established_network_record",
            "summary": "The fictional tower became unstable over time.",
            "matchedFreshSourceRefIds": ["fresh:2"],
        }]
        article = self.article(fresh)
        details = []
        self.assertEqual(journal.validate_article(article, packet, [], repair_details=details), "")
        self.assertEqual(details, [])
        self.assertEqual(self.review_with_anchor(article, "fresh:1", packet=packet)[1], "")
        calls = []
        generated = copy.deepcopy(article)
        for section in generated["sections"]:
            section["sourceRefIds"] = generated["sourceRefIds"][section["heading"]]
        del generated["sourceRefIds"]

        def generator(_packet, prompt):
            calls.append(prompt)
            return (supported_review_with_anchor(prompt, unit_id="sections[0].body:0", source_ref="fresh:1")
                    if is_source_review(prompt) else json.dumps(generated))

        accepted, reason, advisory = journal._generate_article_with_repairs(packet, generator, [])
        self.assertIsNotNone(accepted, reason)
        self.assertEqual(reason, "")
        self.assertFalse(advisory)
        self.assertEqual(len(calls), 2)

    def test_explicit_inference_and_rumor_checks_remain_enforced(self):
        for body in (
            "I suspect the satellite chorus belongs to a broader plan.",
            "Apparently a satellite chorus is being prepared for a broadcast.",
        ):
            with self.subTest(body=body):
                self.assertEqual(journal.validate_article(self.article(body), self.packet, []),
                                 "undeclared_context_use")

    def test_public_privacy_check_is_not_weakened_by_fresh_source_attribution(self):
        article = self.article("Test Listener watched last week's show. See https://example.test/private.")
        self.assertEqual(journal.validate_article(article, self.packet, []), "public_leak_pattern")


if __name__ == "__main__":
    unittest.main()
