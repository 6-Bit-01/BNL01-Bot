"""Protocol regressions; mocked comparisons are not semantic model quality proof."""
import copy
import json
import os
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import bnl_journal_attribution as review
from tests.journal_review_helpers import fixture_issue, review_inputs


class JournalAttributionTests(unittest.TestCase):
    def setUp(self):
        self.article = {"title": "The Sticker Disagreement", "excerpt": "Two different reactions.",
                        "sections": [{"heading": "Who Said What", "body":
                            "Test Listener said the bot invented it. Test Host later explained the sticker joke. "
                            "I am fond of how a tiny sticker became the center of my afternoon."}],
                        "sourceRefIds": {"Who Said What": ["fresh:1", "fresh:2"]},
                        "metadata": {"contextUses": []}}
        self.sources = [
            {"refId": "fresh:1", "participantAlias": "listener", "summary": "The bot invented it.",
             "publicSpeakerName": "Test Listener", "sourceRole": "original_contribution"},
            {"refId": "fresh:2", "participantAlias": "host", "summary": "It was the sticker joke.",
             "publicSpeakerName": "Test Host", "sourceRole": "original_contribution"},
            {"refId": "speech:3", "participantAlias": "bnl", "summary": "I already answered that.",
             "authority": "speech_only", "sourceRole": "bnl_utterance"},
        ]

    def fragment(self, ref, field="summary"):
        return next(item for item in review.source_fragments(self.sources)
                    if item["refId"] == ref and item["field"] == field)

    def verdict(self):
        return {"reviewedUnitIds": [unit["unitId"] for unit in review.public_units(self.article)],
                "issues": [], "verdict": "supported"}

    def accept(self, value):
        return review.accept_review(json.dumps(value), self.article, self.sources)

    def issue(self, fragment, units=None):
        return fixture_issue(units or ["sections[0].body:0"], [fragment],
                             source_meaning=fragment["text"],
                             added_premise="The candidate assigns this statement to the wrong speaker.")

    def test_canonical_speaker_quote_and_kind_come_only_from_selected_fragment(self):
        data = self.verdict()
        data["issues"] = [self.issue(self.fragment(ref)) for ref in ("fresh:1", "fresh:2")]
        receipt, reason, targets = self.accept(data)
        self.assertIsNone(receipt)
        self.assertEqual(reason, "source_attribution_failed")
        anchors = [target["evidence"][0] for target in targets]
        self.assertEqual([item["speaker"] for item in anchors], ["listener", "host"])
        self.assertEqual([item["quote"] for item in anchors], [source["summary"] for source in self.sources[:2]])
        self.assertTrue(all(item["evidenceKind"] == "message_expression" for item in anchors))
        for key in ("speaker", "quote", "refId", "field", "use", "evidenceKind"):
            altered = copy.deepcopy(data)
            altered["issues"][0][key] = "invented"
            self.assertEqual(self.accept(altered)[1], "source_review_invalid_grounding")

    def test_fragment_identity_is_stable_across_source_order_but_keeps_people_distinct(self):
        first = review.source_fragments(self.sources)
        self.assertEqual(first, review.source_fragments(list(reversed(self.sources))))
        self.sources.append({**self.sources[0], "refId": "fresh:other", "participantAlias": "other"})
        self.assertNotEqual(self.fragment("fresh:1")["fragmentId"], self.fragment("fresh:other")["fragmentId"])
        self.assertEqual(self.fragment("fresh:1")["speaker"], "listener")
        self.assertEqual(self.fragment("fresh:other")["speaker"], "other")

    def test_nested_contributions_keep_their_own_author_time_room_and_limits(self):
        self.sources = [{"refId": "exchange:1", "sourceRole": "original_contribution", "contributions": [
            {"participantAlias": "a", "summary": "A question?", "observedAt": "2026-01-01T10:00:00Z",
             "messageContext": {"roomName": "first", "linkContent": "not_inspected"}},
            {"participantAlias": "b", "summary": "A reply.", "observedAt": "2026-01-01T11:00:00Z",
             "messageContext": {"roomName": "second", "linkContent": "inspected"}},
        ]}]
        for fragment in review.source_fragments(self.sources):
            expected = self.sources[0]["contributions"][fragment["speaker"] == "b"]
            self.assertEqual(fragment["context"]["observedAt"], expected["observedAt"])
            self.assertEqual(fragment["context"]["roomName"], expected["messageContext"]["roomName"])
            self.assertEqual(fragment["context"]["linkContent"], expected["messageContext"]["linkContent"])

    def test_original_envelope_message_expression_and_recorded_event_are_distinct(self):
        self.sources[0].update(observedAt="2026-01-01T10:00:00Z", roomRef="room:a")
        self.sources.append({"refId": "show:1", "sourceKind": "finalized_show",
                             "summary": "Forty tracks played.", "episodeDate": "2026-01-01"})
        self.assertEqual(self.fragment("fresh:1")["evidenceKind"], "message_expression")
        for field in ("observedAt", "roomRef"):
            self.assertEqual(self.fragment("fresh:1", field)["evidenceKind"], "message_envelope")
        self.assertEqual(self.fragment("show:1")["evidenceKind"], "recorded_event")
        self.assertEqual(self.fragment("show:1", "episodeDate")["evidenceKind"], "recorded_event_context")
        self.assertEqual(self.fragment("speech:3")["evidenceKind"], "message_expression")
        self.assertEqual(self.fragment("speech:3")["authority"], "speech_only")

    def test_impression_reason_is_not_replaced_by_impression_or_contributor_gist(self):
        self.sources.append({"refId": "impression:1", "basisKind": "moment_impression",
                             "impression": "I enjoy their friction.", "reason": "The later joke made me reconsider.",
                             "contributions": [{"participantAlias": "listener", "summary": "Their derived gist."}]})
        fragment = self.fragment("impression:1", "reason")
        data = self.verdict()
        data["issues"] = [self.issue(fragment)]
        _, reason, targets = self.accept(data)
        self.assertEqual(reason, "source_attribution_failed")
        anchor = targets[0]["evidence"][0]
        self.assertEqual((anchor["field"], anchor["quote"], anchor["speaker"], anchor["evidenceKind"]),
                         ("reason", "The later joke made me reconsider.", "bnl", "subjective_context"))
        self.assertNotEqual(fragment["fragmentId"], self.fragment("impression:1", "impression")["fragmentId"])

    def test_derived_contributions_keep_their_origin_without_becoming_originals(self):
        for basis in ("public_moment", "published_journal", "published_ballad", "accepted_relay_continuity"):
            source = {"refId": "reflection:" + basis, "basisKind": basis,
                      "sourceRole": "original_contribution", "authority": "original",
                      "contributions": [copy.deepcopy(self.sources[0])]}
            self.sources.append(source)
            fragment = self.fragment(source["refId"])
            self.assertEqual(fragment["speaker"], "listener")
            self.assertEqual(fragment["authority"], "derived_context")
            self.assertEqual(fragment["evidenceKind"], "derived_context")

    def test_relay_owned_expression_and_member_contribution_keep_distinct_speakers(self):
        self.sources.append({"refId": "relay:1", "basisKind": "accepted_relay_continuity",
                             "summary": "I wondered about that question.", "publicInvitation": "Who has a theory?",
                             "contributions": [copy.deepcopy(self.sources[0])],
                             "relayPublishedAt": "2026-02-01", "originalSourceDates": []})
        fragments = [item for item in review.source_fragments(self.sources) if item["refId"] == "relay:1"]
        own = [item for item in fragments if item["text"] in {"I wondered about that question.", "Who has a theory?"}]
        self.assertEqual(len(own), 2)
        self.assertTrue(all(item["speaker"] == "bnl" and item["authority"] == "derived_context" for item in own))
        member = next(item for item in fragments if item["text"] == "The bot invented it.")
        self.assertEqual(member["speaker"], "listener")
        self.assertEqual(member["publicSpeakerName"], "Test Listener")

    def test_bnl_owned_expression_cannot_inherit_a_human_display_name(self):
        source = {"refId": "relay:conflict", "basisKind": "accepted_relay_continuity",
                  "summary": "I wondered about the recording.", "participantAlias": "listener",
                  "publicSpeakerName": "Test Listener"}
        fragments = review.source_fragments([source])
        self.assertTrue(fragments)
        self.assertTrue(all(item["speaker"] == "bnl" and item["publicSpeakerName"] == "BNL"
                            and item["authority"] == "derived_context" for item in fragments))

    def test_source_authority_uses_owner_role_not_confident_wording(self):
        for source, expected in ((self.sources[0], "original"), (self.sources[2], "speech_only"),
                                 ({"sourceRole": "recorded_event"}, "original"),
                                 ({"basisKind": "public_source_history"}, "original"),
                                 ({"sourceRole": "approved_canon"}, "canon"),
                                 ({"epistemicStatus": "established_network_record"}, "established_memory"),
                                 ({"sourceRole": "unconfirmed_rumor"}, "rumor"),
                                 ({"basisKind": "public_moment", "sourceRole": "original_contribution"}, "derived_context"),
                                 ({"sourceRole": "bnl_interpretation", "authority": "original"}, "derived_context")):
            self.assertEqual(review.source_authority(source), expected)

    def test_prompt_orders_originals_before_candidate_and_does_not_grade_voice(self):
        self.article["sections"][0]["body"] += "\n\nI imagined a singing satellite."
        self.article["metadata"].update(privateFixture="never project this", contextUses=[{
            "laneRefId": "memory:1", "laneType": "established_broadcast_memory", "sectionHeading": "Who Said What",
            "claim": "A declared claim.", "basisRefIds": ["fresh:1"], "privateFixture": "never project nested"}])
        evidence = {"sources": self.sources, "experienceGroups": [{"originalMessageRefIds": ["fresh:1"],
                      "reflectionRefIds": [], "interpretationsAreIndependentEvidence": False}]}
        original = copy.deepcopy(evidence)
        prompt = review.review_prompt(self.article, evidence)
        units, projected = review_inputs(prompt)
        self.assertEqual(evidence, original)
        self.assertEqual(projected["experienceGroups"], evidence["experienceGroups"])
        self.assertNotIn("CANDIDATE_ARTICLE_JSON", prompt)
        self.assertNotIn("never project", prompt)
        self.assertIn("Do not grade style", prompt)
        self.assertIn("faithful everyday paraphrases", prompt)
        self.assertIn("Avoid literalizing obvious banter", prompt)
        self.assertLess(prompt.index("ORIGINAL_EVIDENCE_JSON:"), prompt.index("CANDIDATE_UNITS_JSON:"))
        declarations, _ = json.JSONDecoder().raw_decode(prompt.split("CANDIDATE_CONTEXT_USES_JSON: ")[1])
        self.assertEqual(declarations[0]["basisRefIds"], ["fresh:1"])
        self.assertNotIn("privateFixture", declarations[0])
        self.assertEqual(next(item for item in units if item["text"] == "I imagined a singing satellite.")["paragraphIndex"], 1)
        self.assertTrue(projected["evidenceGroups"]["originalRecords"])

    def test_cross_sentence_issue_preserves_all_locations_and_originals(self):
        # A controlled discrepancy tests transport only, not semantic accuracy.
        data = self.verdict()
        issue = fixture_issue(["sections[0].body:0", "sections[0].body:1"],
                              [self.fragment("fresh:1"), self.fragment("fresh:2")],
                              source_meaning="The statements were made in separate rooms.",
                              added_premise="The first statement directly prompted the second.")
        data["issues"] = [issue]
        _, reason, targets = self.accept(data)
        self.assertEqual(reason, "source_attribution_failed")
        self.assertEqual(targets[0]["unitIds"], issue["unitIds"])
        self.assertEqual(targets[0]["sourceRefIds"], ["fresh:1", "fresh:2"])
        self.assertEqual(targets[0]["fieldPaths"], ["sections[0].body"])

    def test_raw_or_single_complete_json_fence_preserves_review_checks(self):
        raw = json.dumps(self.verdict())
        for value in (raw, " \n" + raw + "\n ", "```json\n" + raw + "\n```",
                      "```JSON\r\n" + raw + "\r\n```", "```\n" + raw + "\n```"):
            receipt, reason, _ = review.accept_review(value, self.article, self.sources)
            self.assertEqual(reason, "")
            self.assertEqual(receipt["articleDigest"], review.article_digest(self.article))
        for value in ("Here is a review.\n" + raw, raw + raw, "```json\n" + raw,
                      raw + "\n```", "```python\n" + raw + "\n```", '[]', 'null'):
            self.assertEqual(review.accept_review(value, self.article, self.sources)[1], "source_review_invalid")

    def test_malformed_verdict_and_issue_containers_fail_closed(self):
        for bad in ([], {}, None, 42):
            data = self.verdict()
            data["verdict"] = bad
            self.assertEqual(self.accept(data)[1], "source_review_invalid")
        for bad in ({}, None, 42, "none"):
            data = self.verdict()
            data["issues"] = bad
            self.assertEqual(self.accept(data)[1], "source_review_invalid")


class JournalReviewTransportTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ.setdefault("GEMINI_API_KEY", "test-key")
        os.environ.setdefault("DISCORD_BOT_TOKEN", "test-token")
        import bnl01_bot
        cls.bot = bnl01_bot

    def test_editor_uses_existing_route_without_writing_persona(self):
        response = SimpleNamespace(candidates=[SimpleNamespace(finish_reason="STOP")])
        prompt = review.REVIEW_PREFIX + "review fixture"
        with patch.object(self.bot, "check_quota_availability", return_value=True) as quota, \
             patch.object(self.bot, "_generate_gemini_content_with_fallback", return_value=response) as provider, \
             patch.object(self.bot, "_extract_text_and_tokens", return_value=('{}', 1)):
            self.assertEqual(self.bot._generate_journal_json_sync({}, prompt), '{}')
        quota.assert_called_once_with(self.bot.JOURNAL_ROUTE)
        provider.assert_called_once_with(prompt, self.bot.JOURNAL_ROUTE)

    def test_partial_editor_response_cannot_authorize_a_candidate(self):
        response = SimpleNamespace(candidates=[SimpleNamespace(finish_reason="MAX_TOKENS")])
        with patch.object(self.bot, "check_quota_availability", return_value=True), \
             patch.object(self.bot, "_generate_gemini_content_with_fallback", return_value=response):
            with self.assertRaisesRegex(RuntimeError, "journal_source_review_incomplete"):
                self.bot._generate_journal_json_sync({}, review.REVIEW_PREFIX + "fixture")

    def test_normal_journal_keeps_bnl_persona(self):
        response = SimpleNamespace(candidates=[SimpleNamespace(finish_reason="STOP")])
        with patch.object(self.bot, "check_quota_availability", return_value=True), \
             patch.object(self.bot, "_generate_gemini_content_with_fallback", return_value=response) as provider, \
             patch.object(self.bot, "_extract_text_and_tokens", return_value=('{}', 1)):
            self.bot._generate_journal_json_sync({}, "write a Journal")
        self.assertEqual(provider.call_args.args,
                         (self.bot.BNL01_SYSTEM_PROMPT + "\n\nwrite a Journal", self.bot.JOURNAL_ROUTE))


if __name__ == "__main__":
    unittest.main()

