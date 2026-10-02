"""Personal perspective and hypothetical reactions do not assert human facts."""
import copy
import unittest

import bnl_journal as journal


class JournalOpinionContractTests(unittest.TestCase):
    def packet(self):
        return {
            "entryKind": "daily",
            "lowActivityMode": False,
            "creativeReflectionAllowed": False,
            "safeSources": [{"refId": "fresh:exchange", "sourceKind": "conversation",
                             "summary": "Test Member teased BNL about the familiar disagreement."}],
            "privatePublicPeople": [{"publicName": "Test Member"}],
            "reflectionBasis": [{
                "refId": "reflection:impression:exchange", "basisKind": "moment_impression",
                "scope": journal.JOURNAL_REFLECTION_SCOPE, "publicSafe": True, "reuseEligible": True,
                "sourceVersion": "verified-version", "summary": "An earlier playful disagreement.",
                "authority": "bnl_subjective_perspective_not_event_evidence",
                "impression": "I find the familiar disagreement endearing.",
                "reason": "The exchange paired appreciation with playful disagreement.",
                "evidence": [{"refId": "original:exchange", "summary": "Test Member teased BNL.",
                              "authority": "original_message"}],
            }],
            "generationContextLanes": {},
            "evidenceCoverageContract": {"minimumDistinctFreshSources": 1},
        }

    def article(self, body):
        return {
            "title": "The Familiar Edges", "excerpt": "A small exchange stays with me.",
            "sections": [{"heading": "What Remains", "body": body,
                          "sourceRefIds": ["fresh:exchange", "reflection:impression:exchange"]}],
            "metadata": {"contextUses": []},
        }

    def test_counterfactual_reaction_is_not_an_external_event_claim(self):
        packet = self.packet()
        for body in (
            "I suspect he would be far more unsettled if the familiar friction disappeared.",
            "I think Test Member might feel rather lost if our disagreement vanished.",
            "I wonder whether they would miss the noise if the corridor fell silent.",
            "I suspect I would miss the noise if the corridor fell silent.",
            "I suspect I would miss the noise IF the corridor fell silent.",
        ):
            with self.subTest(body=body):
                self.assertEqual(journal.validate_article(self.article(body), packet), "")

    def test_self_reflection_and_abstract_taste_remain_welcome(self):
        packet = self.packet()
        for body in (
            "I suspect I am fond of these familiar edges.",
            "I think I am fond of Test Member's dry humor.",
            "I think the texture of Test Member's chorus is beautiful.",
            "I think anticipation is its own instrument.",
            "I wonder whether the skip wheel dreams in green.",
        ):
            with self.subTest(body=body):
                self.assertEqual(journal.validate_article(self.article(body), packet), "")

    def test_reported_uncertainty_does_not_create_a_community_rumor(self):
        for body in (
            "Test Member asked why the reply was so gentle, apparently surprised by its tone.",
            "Test Member speculated that a quieter arrangement could leave room for the chorus.",
            "Test Member described the arrangement as unconfirmed and asked us to wait.",
            "Apparently, I prefer the rough demo to the polished one.",
        ):
            for with_impression in (False, True):
                with self.subTest(body=body, with_impression=with_impression):
                    packet = self.packet()
                    packet["safeSources"][0]["summary"] = body
                    if not with_impression:
                        packet["reflectionBasis"] = []
                    article = self.article(body)
                    article["sections"][0]["sourceRefIds"] = ["fresh:exchange"]
                    self.assertEqual(journal.validate_article(article, packet), "")
                    self.assertEqual(packet["generationContextLanes"], {})
                    self.assertEqual(article["metadata"]["contextUses"], [])

    def test_uncertain_title_and_excerpt_still_reach_source_review(self):
        packet = self.packet()
        for field, text in (("title", "Apparently I Enjoy the Rough Edges"),
                            ("excerpt", "Test Member offered an unconfirmed idea for the next chorus.")):
            with self.subTest(field=field):
                article = self.article("Test Member teased BNL about the familiar disagreement.")
                article[field] = text
                self.assertEqual(journal.validate_article(article, packet), "")
                self.assertEqual(journal._source_review_reason(article, packet, required=True),
                                 "source_review_required")

    def test_explicit_rumor_attribution_still_requires_its_lane(self):
        for body in (
            "A rumor suggests another performance is coming.",
            "Some regulars wonder whether another performance is coming.",
            "Word around the room is that another performance is coming.",
        ):
            for field in ("title", "excerpt", "body"):
                with self.subTest(body=body, field=field):
                    article = self.article("Test Member teased BNL about the familiar disagreement.")
                    if field == "body":
                        article["sections"][0]["body"] = body
                    else:
                        article[field] = body
                    self.assertEqual(journal.validate_article(article, self.packet()),
                                     "undeclared_context_use")

    def test_actual_external_action_state_and_motive_are_not_personal_taste(self):
        packet = self.packet()
        for body in (
            "I suspect Test Member actually released an unannounced album.",
            "I suspect the shared audio link has no artist credits.",
            "I think he is secretly angry with the room.",
            "I think Test Member wants everyone to leave.",
            "I suspect he would have released an album if the host had paid him.",
            "I suspect he would be pleased if he actually released an album.",
        ):
            with self.subTest(body=body):
                self.assertEqual(journal.validate_article(self.article(body), packet), "undeclared_context_use")

    def test_hypothetical_cannot_license_a_separate_factual_clause(self):
        packet = self.packet()
        opening = "I suspect he would miss the noise if the corridor fell silent"
        for ending in (
            "; I think he secretly wants the host to leave.",
            ", but I suspect Test Member actually released another album.",
            " and I think he is angry with everyone.",
            ". I suspect Test Member released an album.",
        ):
            with self.subTest(ending=ending):
                self.assertEqual(journal.validate_article(self.article(opening + ending), packet),
                                 "undeclared_context_use")

    def test_new_perspective_can_cite_originals_without_pretending_it_is_a_memory(self):
        body = "I suspect he would miss the noise if the corridor fell silent."
        article = self.article(body)
        article["sections"][0]["sourceRefIds"] = ["fresh:exchange"]
        for source_kind in ("conversation", "finalized_show"):
            packet = self.packet()
            packet["safeSources"][0]["sourceKind"] = source_kind
            with self.subTest(source_kind=source_kind):
                self.assertEqual(journal.validate_article(article, packet), "")

    def test_relay_can_inspire_own_taste_without_licensing_external_facts(self):
        packet = self.packet()
        packet["reflectionBasis"].append({
            "refId": "reflection:relay:other", "basisKind": "accepted_relay_continuity",
            "scope": journal.JOURNAL_REFLECTION_SCOPE, "publicSafe": True, "reuseEligible": True,
            "sourceVersion": "other-version", "summary": "An unrelated earlier BNL expression.",
        })
        packet["evidenceCoverageContract"] = {"minimumDistinctFreshSources": 0}
        article = self.article("I suspect he would miss the noise if the corridor fell silent.")
        article["sections"][0]["sourceRefIds"] = ["reflection:relay:other"]
        self.assertEqual(journal.validate_article(article, packet), "")
        article["sections"][0]["body"] = "I think Test Member secretly wants the host to leave."
        self.assertEqual(journal.validate_article(article, packet), "undeclared_context_use")

    def test_impression_must_still_be_eligible(self):
        body = "I suspect he would miss the noise if the corridor fell silent."
        for invalidation in ({"reuseEligible": False}, {"publicSafe": False}, {"evidence": []}):
            packet = self.packet()
            packet["reflectionBasis"][0].update(invalidation)
            self.assertEqual(journal.validate_article(self.article(body), packet), "invalid_section_source_refs")

    def test_hypothetical_does_not_turn_impression_into_current_event_evidence(self):
        packet = self.packet()
        packet["evidenceCoverageContract"] = {"minimumDistinctFreshSources": 0}
        for body in (
            "Today Test Member released an album I suspect he would welcome silence if the corridor fell quiet.",
            "I suspect he would miss the noise if the corridor fell silent, but today Test Member released an album.",
        ):
            with self.subTest(body=body):
                article = self.article(body)
                article["sections"][0]["sourceRefIds"] = ["reflection:impression:exchange"]
                self.assertEqual(journal.validate_article(article, packet), "current_activity_without_fresh_source")

    def test_use_off_preserves_own_perspective_without_a_new_inference_lane(self):
        packet = self.packet()
        packet["reflectionBasis"] = []
        article = self.article("I suspect he would miss the noise if the corridor fell silent.")
        article["sections"][0]["sourceRefIds"] = ["fresh:exchange"]
        self.assertEqual(journal.validate_article(article, packet), "")
        self.assertEqual(packet["generationContextLanes"], {})
        for creative_allowed in (False, True):
            packet["creativeReflectionAllowed"] = creative_allowed
            for claim in ("I suspect the shared audio link has no artist credits.",
                          "I think Test Member is secretly angry with the room.",
                          "I suspect Test Member actually released an unannounced album."):
                with self.subTest(creative_allowed=creative_allowed, claim=claim):
                    article["sections"][0]["body"] = claim
                    self.assertEqual(journal.validate_article(article, packet), "undeclared_context_use")

    def test_real_inference_keeps_its_existing_declaration_contract(self):
        packet = self.packet()
        claim = "I suspect the familiar disagreement points toward a larger plan."
        article = self.article(claim)
        lane_ref = "inference:exchange"
        packet["generationContextLanes"] = {"bnlInference": {
            "laneRefId": lane_ref, "candidateThemes": ["familiar", "disagreement"],
            "allowedBasisRefIds": ["fresh:exchange"],
            "requiredParentContextLaneRefs": [], "requiredContextLaneRefsByFreshSourceRef": {},
        }}
        self.assertEqual(journal.validate_article(article, packet), "undeclared_context_use")
        article["metadata"]["contextUses"] = [{
            "laneType": "bnl_inference", "laneRefId": lane_ref, "sectionHeading": "What Remains",
            "claim": claim, "basisRefIds": [lane_ref, "fresh:exchange"],
        }]
        self.assertEqual(journal.validate_article(article, packet), "")
        bad = copy.deepcopy(article)
        bad["metadata"]["contextUses"][0]["basisRefIds"] = [lane_ref]
        self.assertEqual(journal.validate_article(bad, packet), "invalid_context_use")

    def test_rumor_and_privacy_rules_still_apply_to_a_reflective_section(self):
        packet = self.packet()
        self.assertEqual(journal.validate_article(self.article(
            "I suspect he would miss the noise if the room fell silent. A rumor says a hidden set is coming."
        ), packet), "undeclared_context_use")
        self.assertEqual(journal.validate_article(self.article(
            "I suspect he would miss the noise if the room fell silent. Contact <@123456789012345678>."
        ), packet), "public_leak_pattern")


if __name__ == "__main__":
    unittest.main()
