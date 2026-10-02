"""The Journal sees original experience and distinct reflection, not extra witnesses."""
import copy
import json
import unittest

import bnl_journal as journal


END = "2026-10-01T23:48:43Z"


def reflection(ref, kind, summary, **fields):
    return {"refId": ref, "basisKind": kind, "summary": summary,
            "scope": journal.JOURNAL_REFLECTION_SCOPE, "publicSafe": True,
            "reuseEligible": True, "sourceVersion": "version-" + ref,
            "sourceObservedAt": "2026-10-01T15:00:00Z", **fields}


def roots(revision="1"):
    return [{"sourceTable": "conversations", "sourceRowId": "71", "sourceRevision": revision}]


class JournalPromptProjectionTests(unittest.TestCase):
    def setUp(self):
        self.original = {"refId": "original:question", "participantAlias": "listener",
                         "authority": "original_contribution", "role": "user",
                         "summary": "[shared link] Who played this?",
                         "messageContext": {"roomName": "music-room", "linkContent": "not_inspected"}}
        impression = reflection("reflection:impression:1", "moment_impression", "A listener asked about a performance.",
                                authority="bnl_subjective_perspective_not_event_evidence",
                                impression="I enjoy the question's open ending.",
                                reason="Curiosity matters more to me than a tidy label.", evidence=[self.original])
        gist = reflection("reflection:moment:1", "public_moment", impression["summary"])
        relay = reflection("reflection:event:2", "accepted_relay_continuity",
                           "I kept thinking about that question. Tell me what you hear.",
                           relaySpeech={"partition": "matched_public_relay_fields",
                                        "publicMessage": "I kept thinking about that question.",
                                        "publicInvitation": "Tell me what you hear."})
        self.packet = {"entryKind": "daily", "sourceWindowEnd": END, "safeSources": [],
                       "reflectionBasis": [gist, relay, impression],
                       "privateSharedSourceProvenance": [
                           {"refId": item["refId"], "originalSourceRefs": roots()}
                           for item in (gist, impression)],
                       "privateReflectionBasisProvenance": {"historicalSourceEvents": [
                           {"refId": relay["refId"], "originalSources": [{"originalSourceRefs": roots()}]}]}}

    def test_same_root_gist_collapses_but_distinct_later_reflection_and_original_survive(self):
        before = copy.deepcopy(self.packet)
        result = journal._journal_prompt_projection(self.packet)
        by_ref = {item["refId"]: item for item in result["reflectionBasis"]}
        self.assertNotIn("reflection:moment:1", by_ref)
        self.assertEqual({"reflection:impression:1", "reflection:event:2"}, set(by_ref))
        self.assertEqual([self.original], by_ref["reflection:impression:1"]["evidence"])
        self.assertEqual("I enjoy the question's open ending.", by_ref["reflection:impression:1"]["impression"])
        self.assertEqual("I kept thinking about that question.", by_ref["reflection:event:2"]["summary"])
        self.assertEqual("Tell me what you hear.", by_ref["reflection:event:2"]["publicInvitation"])
        self.assertEqual("derived_context", by_ref["reflection:event:2"]["authority"])
        self.assertEqual(1, len(result["experienceGroups"]))
        self.assertEqual(["original:question"], result["experienceGroups"][0]["originalMessageRefIds"])
        self.assertFalse(result["experienceGroups"][0]["interpretationsAreIndependentEvidence"])
        self.assertEqual(before, self.packet)
        self.assertNotIn("sourceRowId", json.dumps(result))

    def test_same_words_with_different_revision_or_unknown_lineage_do_not_merge(self):
        self.packet["privateSharedSourceProvenance"][0]["originalSourceRefs"] = roots("2")
        self.packet["privateReflectionBasisProvenance"]["historicalSourceEvents"][0]["originalSources"] = []
        result = journal._journal_prompt_projection(self.packet)
        self.assertEqual(3, len(result["reflectionBasis"]))
        self.assertEqual(3, len(result["experienceGroups"]))
        relay_group = next(group for group in result["experienceGroups"]
                           if "reflection:event:2" in group["reflectionRefIds"])
        self.assertEqual([], relay_group["originalMessageRefIds"])
        self.assertFalse(relay_group["originalMessagesSupplied"])

    def test_same_summary_does_not_erase_distinct_participant_interpretation(self):
        self.packet["reflectionBasis"][0]["contributions"] = [
            {"participantAlias": "listener", "summary": "A distinct account of the exchange."}]
        result = journal._journal_prompt_projection(self.packet)
        self.assertIn("reflection:moment:1", {item["refId"] for item in result["reflectionBasis"]})

    def test_withdrawn_impression_cannot_supply_originals_or_hide_remaining_gist(self):
        self.packet["reflectionBasis"][-1]["reuseEligible"] = False
        result = journal._journal_prompt_projection(self.packet)
        self.assertEqual({"reflection:moment:1", "reflection:event:2"},
                         {item["refId"] for item in result["reflectionBasis"]})
        self.assertFalse(any(group["originalMessagesSupplied"] for group in result["experienceGroups"]))
        self.assertNotIn("original:question", json.dumps(result))

    def test_writer_reviewer_and_citations_share_projected_set(self):
        self.packet["privatePublicPeople"] = [{"participantAlias": "listener", "publicName": "Test Listener",
                                              "sourceRefIds": ["reflection:moment:1", "reflection:impression:1"]}]
        safe = json.loads(journal.build_generation_prompt(self.packet).split("Generation-safe packet:\n", 1)[1])
        evidence = journal._source_review_evidence(self.packet)
        refs = {item["refId"] for item in evidence["sources"]}
        self.assertNotIn("reflection:moment:1", refs)
        self.assertIn("original:question", refs)
        self.assertEqual(safe["experienceGroups"], evidence["experienceGroups"])
        self.assertEqual(["reflection:impression:1"], safe["publicPeople"][0]["sourceRefIds"])
        for item in safe["reflectionBasis"]:
            self.assertEqual(item, next(source for source in evidence["sources"] if source["refId"] == item["refId"]))
        article = {"title": "A question", "excerpt": "I kept thinking.",
                   "sections": [{"heading": "What stayed", "body": "I kept thinking.",
                                 "sourceRefIds": ["reflection:moment:1"]}]}
        self.assertEqual("invalid_section_source_refs", journal.validate_article(article, self.packet))

    def test_sources_outside_writer_bound_cannot_authorize_reviewer_or_citation(self):
        self.packet["safeSources"] = [{"refId": "fresh:%s" % i, "sourceKind": "conversation",
                                       "summary": "A shared musical thought."}
                                      for i in range(journal.MAX_PROMPT_SOURCES + 1)]
        omitted = "fresh:%s" % journal.MAX_PROMPT_SOURCES
        self.assertNotIn(omitted, {item["refId"] for item in journal._source_review_evidence(self.packet)["sources"]})
        article = {"title": "A question", "excerpt": "I kept thinking.",
                   "sections": [{"heading": "What stayed", "body": "I kept thinking.", "sourceRefIds": [omitted]}]}
        self.assertEqual("invalid_section_source_refs", journal.validate_article(article, self.packet))

    def test_frozen_packet_round_trip_preserves_exact_generation_prompt(self):
        before = journal.build_generation_prompt(self.packet)
        restored = json.loads(json.dumps(self.packet, sort_keys=True))
        self.assertEqual(before, journal.build_generation_prompt(restored))


if __name__ == "__main__":
    unittest.main()
