"""Protocol regressions; mocked comparisons are not semantic model quality proof."""
import copy
import json
import os
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import bnl_journal_attribution as review
from tests.journal_review_helpers import fixture_claim, review_inputs


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

    def fragment(self, ref, field="summary", sources=None):
        return next(item for item in review.source_fragments(sources or self.sources)
                    if item["refId"] == ref and item["field"] == field)

    def verdict(self):
        units = []
        for unit in review.public_units(self.article):
            claims = []
            if unit["text"].startswith("Test "):
                ref = "fresh:1" if unit["text"].startswith("Test Listener") else "fresh:2"
                claims = [fixture_claim(unit["text"], self.fragment(ref))]
            units.append({"unitId": unit["unitId"], "claims": claims,
                          "nonFactualReason": "A personal title or imaginative reaction in this controlled fixture." if not claims else ""})
        return {"assessments": [
            {"check": check, "unitIds": [unit["unitId"] for unit in units],
             "sourceRefIds": ["fresh:1", "fresh:2"],
             "explanation": "Controlled passing " + check + " fixture; not a live quality verdict.",
             "issues": [], "verdict": "supported"} for check in review.ASSESSMENT_CHECKS
        ], "units": units, "verdict": "supported"}

    @staticmethod
    def claims(data):
        return [claim for unit in data["units"] for claim in unit["claims"]]

    def accept(self, value):
        return review.accept_review(json.dumps(value), self.article, self.sources)

    def test_complete_review_binds_prose_paragraphs_citations_and_continuity(self):
        receipt, reason, targets = self.accept(self.verdict())
        self.assertEqual((reason, targets), ("", []))
        self.assertEqual(receipt["version"], review.REVIEW_VERSION)
        self.assertEqual(receipt["articleDigest"], review.article_digest(self.article))
        for mutate in (
            lambda a: a.update(title="An Unchecked Different Story"),
            lambda a: a["sections"][0].update(body="Test Host accused the bot instead."),
            lambda a: a["sourceRefIds"].update({"Who Said What": ["fresh:2"]}),
            lambda a: a["metadata"].update(contextUses=[{"claim": "An unchecked memory"}]),
            lambda a: a["sections"][0].update(body=a["sections"][0]["body"].replace(". ", ".\n\n", 1)),
        ):
            changed = copy.deepcopy(self.article)
            mutate(changed)
            self.assertNotEqual(receipt["articleDigest"], review.article_digest(changed))

    def test_empty_duplicate_missing_and_unknown_units_cannot_pass(self):
        for mode in ("empty", "duplicate", "missing", "unknown"):
            data = self.verdict()
            if mode == "empty": data["units"] = []
            elif mode == "duplicate": data["units"][-1] = data["units"][0]
            elif mode == "missing": data["units"].pop()
            else: data["units"][-1]["unitId"] = "not-a-candidate-unit"
            self.assertEqual(self.accept(data)[1], "source_review_incomplete", mode)

    def test_canonical_speaker_and_quote_come_only_from_selected_fragment(self):
        receipt, reason, _ = self.accept(self.verdict())
        self.assertEqual(reason, "")
        anchors = [claim["evidence"][0] for claim in self.claims(receipt)]
        self.assertEqual([item["speaker"] for item in anchors], ["listener", "host"])
        self.assertEqual([item["quote"] for item in anchors], [source["summary"] for source in self.sources[:2]])
        for key, wrong in (("speaker", "host"), ("quote", "An invented action."),
                           ("refId", "fresh:2"), ("field", "reason")):
            data = self.verdict()
            self.claims(data)[0]["evidence"][0][key] = wrong
            self.assertEqual(self.accept(data)[1], "source_review_invalid_anchor", key)

    def test_unknown_changed_or_duplicate_fragment_cannot_pass(self):
        for fragment_id in ("f:missing", {}, None, 42):
            data = self.verdict()
            self.claims(data)[0]["evidence"][0]["fragmentId"] = fragment_id
            self.assertEqual(self.accept(data)[1], "source_review_invalid_anchor")
        data = self.verdict()
        self.claims(data)[0]["evidence"] *= 2
        self.assertEqual(self.accept(data)[1], "source_review_invalid_anchor")
        data = self.verdict()
        self.sources[0]["summary"] = "An updated original."
        self.assertEqual(self.accept(data)[1], "source_review_invalid_anchor")

    def test_fragment_identity_is_stable_across_source_order_but_keeps_people_distinct(self):
        first = review.source_fragments(self.sources)
        self.assertEqual(first, review.source_fragments(list(reversed(self.sources))))
        same_words = {**self.sources[0], "refId": "fresh:other", "participantAlias": "other"}
        self.sources.append(same_words)
        self.assertNotEqual(self.fragment("fresh:1")["fragmentId"], self.fragment("fresh:other")["fragmentId"])
        # Public name collision cannot change a server-bound original author.
        self.sources[-1]["publicSpeakerName"] = "Test Listener"
        self.assertEqual(self.fragment("fresh:1")["speaker"], "listener")
        self.assertEqual(self.fragment("fresh:other")["speaker"], "other")

    def test_nested_contributions_keep_their_own_author_time_room_and_limits(self):
        self.sources = [{"refId": "exchange:1", "sourceRole": "original_contribution", "contributions": [
            {"participantAlias": "a", "summary": "A question?", "observedAt": "2026-01-01T10:00:00Z",
             "messageContext": {"roomName": "first", "linkContent": "not_inspected"}},
            {"participantAlias": "b", "summary": "A reply.", "observedAt": "2026-01-01T11:00:00Z",
             "messageContext": {"roomName": "second", "linkContent": "inspected"}},
        ]}]
        fragments = review.source_fragments(self.sources)
        for fragment in fragments:
            expected = self.sources[0]["contributions"][fragment["speaker"] == "b"]
            self.assertEqual(fragment["context"]["observedAt"], expected["observedAt"])
            self.assertEqual(fragment["context"]["roomName"], expected["messageContext"]["roomName"])
            self.assertEqual(fragment["context"]["linkContent"], expected["messageContext"]["linkContent"])

    def test_metadata_and_impression_reason_are_distinct_exact_fragments(self):
        self.sources[0].update(observedAtPacific="2026-06-09T19:30:00-07:00", roomRef="room:a")
        self.sources.append({"refId": "impression:1", "basisKind": "moment_impression",
                             "impression": "I enjoy their friction.", "reason": "The later joke made me reconsider.",
                             "contributions": [{"participantAlias": "listener", "summary": "Their derived gist."}]})
        for field in ("observedAtPacific", "roomRef"):
            self.assertEqual(self.fragment("fresh:1", field)["text"], self.sources[0][field])
        for field in ("impression", "reason"):
            fragment = self.fragment("impression:1", field)
            self.assertEqual(fragment["text"], self.sources[-1][field])
            self.assertEqual(fragment["speaker"], "bnl")
        self.assertNotEqual(self.fragment("impression:1", "reason")["fragmentId"],
                            self.fragment("impression:1", "impression")["fragmentId"])

    def test_r6_reason_selection_cannot_accidentally_bind_to_impression(self):
        # Synthetic analogue of the saved R6 field-copy failure; not a semantic oracle.
        self.sources.append({"refId": "impression:1", "basisKind": "moment_impression",
                             "impression": "I enjoy their friction.", "reason": "The later joke made me reconsider."})
        data = self.verdict()
        fragment = self.fragment("impression:1", "reason")
        self.claims(data)[0]["evidence"] = [{"fragmentId": fragment["fragmentId"], "use": "context"}]
        receipt, reason, _ = self.accept(data)
        self.assertEqual(reason, "")
        anchor = self.claims(receipt)[0]["evidence"][0]
        self.assertEqual((anchor["field"], anchor["quote"], anchor["speaker"]),
                         ("reason", "The later joke made me reconsider.", "bnl"))

    def test_missing_claim_account_or_legacy_spans_cannot_approve(self):
        for mode in ("missing", "empty_reason", "legacy", "extra_copied_text"):
            data = self.verdict()
            unit = data["units"][0]
            if mode == "missing": unit.pop("claims")
            elif mode == "empty_reason": unit["nonFactualReason"] = " "
            elif mode == "legacy": unit.update(spans=[{"text": "Old protocol", "verdict": "supported"}])
            else: unit["text"] = "A made-up replacement"
            self.assertEqual(self.accept(data)[1], "source_review_invalid_grounding", mode)

    def test_bnl_speech_and_derived_contributions_cannot_masquerade_as_events(self):
        data = self.verdict()
        self.claims(data)[0]["evidence"] = [{"fragmentId": self.fragment("speech:3")["fragmentId"], "use": "event"}]
        self.assertEqual(self.accept(data)[1], "source_review_derived_as_fact")
        self.claims(data)[0]["evidence"][0]["use"] = "speech"
        self.assertEqual(self.accept(data)[1], "")
        for kind in ("public_moment", "published_journal", "published_ballad", "accepted_relay_continuity"):
            source = {"refId": "reflection:" + kind, "basisKind": kind,
                      "contributions": [copy.deepcopy(self.sources[0])]}
            self.sources.append(source)
            data = self.verdict()
            anchor = {"fragmentId": self.fragment(source["refId"])["fragmentId"], "use": "speech"}
            self.claims(data)[0]["evidence"] = [anchor]
            for use in ("speech", "event"):
                anchor["use"] = use
                self.assertEqual(self.accept(data)[1], "source_review_derived_as_fact")
            anchor["use"] = "context"
            self.assertEqual(self.accept(data)[1], "")

    def test_memory_and_rumor_require_matching_body_section_declaration(self):
        for lane_type, role in (("established_broadcast_memory", "established_memory"), ("community_rumor", "rumor")):
            source = {"refId": "lane:test", "summary": "An earlier sticker discussion.", "authority": role}
            sources = [*self.sources, source]
            contract = {"lane:test": {"laneType": lane_type}}
            data = self.verdict()
            self.claims(data)[0]["evidence"] = [{"fragmentId": self.fragment("lane:test", sources=sources)["fragmentId"], "use": "context"}]
            declaration = {"laneRefId": "lane:test", "laneType": lane_type, "sectionHeading": "Who Said What"}
            for wrong in ([], [{**declaration, "sectionHeading": "Another Section"}],
                          [{**declaration, "laneRefId": "lane:other"}], [{**declaration, "laneType": "bnl_inference"}]):
                self.article["metadata"]["contextUses"] = wrong
                _, reason, targets = review.accept_review(json.dumps(data), self.article, sources, context_contract=contract)
                self.assertEqual(reason, "source_attribution_failed")
                self.assertEqual(targets[0]["check"], "missing_context_declaration")
            self.article["metadata"]["contextUses"] = [declaration]
            self.assertEqual(review.accept_review(json.dumps(data), self.article, sources, context_contract=contract)[1], "")

    def test_same_fresh_wording_does_not_imply_use_of_unrelated_memory(self):
        self.sources.append({"refId": "memory:old", "summary": "The bot invented it during an older unrelated joke.",
                             "epistemicStatus": "established_network_record"})
        contract = {"memory:old": {"laneType": "established_broadcast_memory"}}
        self.assertEqual(review.accept_review(json.dumps(self.verdict()), self.article, self.sources,
                                             context_contract=contract)[1], "")

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

    def test_prompt_has_one_ordered_candidate_and_no_private_metadata_or_duplicate_article(self):
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
        self.assertEqual(projected["fragments"], review.source_fragments(self.sources))
        self.assertNotIn("CANDIDATE_ARTICLE_JSON", prompt)
        self.assertNotIn("CANDIDATE_CONTEXT_USES_JSON", prompt)
        self.assertNotIn("never project", prompt)
        self.assertEqual(next(item for item in units if item["text"] == "I imagined a singing satellite.")["paragraphIndex"], 1)
        self.assertTrue(any(item.get("contextUses") for item in units if item["field"] == "sections[0].body"))
        self.assertFalse(any(item.get("contextUses") for item in units if item["field"] in {"title", "excerpt"}))

    def test_missing_duplicate_and_unknown_whole_entry_checks_cannot_approve(self):
        for mode in ("v2", "empty", "missing", "duplicate", "unknown"):
            with self.subTest(mode=mode):
                data = self.verdict()
                if mode == "v2": data.pop("assessments")
                elif mode == "empty": data["assessments"] = []
                elif mode == "missing": data["assessments"].pop()
                elif mode == "duplicate": data["assessments"][-1] = data["assessments"][0]
                else: data["assessments"][-1]["check"] = "invented_check"
                self.assertEqual(self.accept(data), (None, "source_review_incomplete", []))

    def test_whole_entry_assessments_require_bound_locations_and_specific_explanation(self):
        for key, value in (("unitIds", []), ("unitIds", ["missing:0"]),
                           ("unitIds", ["title:0", "title:0"]), ("unitIds", [{}]),
                           ("sourceRefIds", ["fresh:missing"]),
                           ("sourceRefIds", ["fresh:1", "fresh:1"]),
                           ("sourceRefIds", [{}]), ("sourceRefIds", None),
                           ("explanation", " "), ("explanation", []),
                           ("issues", [""]), ("issues", {}), ("verdict", [])):
            with self.subTest(key=key, value=value):
                data = self.verdict()
                data["assessments"][0][key] = value
                self.assertEqual(self.accept(data), (None, "source_review_invalid", []))

    def test_individually_supported_spans_do_not_overrule_cross_sentence_factual_findings(self):
        # The supplied negative verdict is a controlled editor finding, not a
        # deterministic assertion that the protocol can understand these claims.
        for check, issue in (
            ("event_relationships", "The reaction is in another room; two true statements do not establish a reply."),
            ("attribution_stance", "The allegation remains disputed after the host's later explanation."),
        ):
            with self.subTest(check=check):
                data = self.verdict()
                assessment = next(item for item in data["assessments"] if item["check"] == check)
                assessment.update(unitIds=["sections[0].body:0", "sections[0].body:1"],
                                  explanation=issue, issues=[issue], verdict="unsupported")
                self.assertTrue(all(claim["support"] == "entails" for claim in self.claims(data)))
                receipt, reason, targets = self.accept(data)
                self.assertIsNone(receipt)
                self.assertEqual(reason, "source_attribution_failed")
                self.assertEqual(len(targets), 1)
                self.assertEqual(targets[0]["field"], "sections[0].body")
                self.assertEqual(targets[0]["check"], check)
                self.assertEqual(targets[0]["sourceRefIds"], ["fresh:1", "fresh:2"])
                self.assertEqual(targets[0]["issues"], [issue])

    def test_recap_with_reaction_and_lost_detail_require_editorial_repair(self):
        for check, issue in (
            ("journal_perspective", "The entry inventories events and appends fondness without developing BNL's thought."),
            ("detail_retention", "The selected dispute loses the later explanation that changes its meaning."),
        ):
            for verdict in ("unsupported", "uncertain", "supported"):
                with self.subTest(check=check, verdict=verdict):
                    data = self.verdict()
                    assessment = next(item for item in data["assessments"] if item["check"] == check)
                    assessment.update(unitIds=["sections[0].body:0", "sections[0].body:2"],
                                      explanation=issue, issues=[issue], verdict=verdict)
                    receipt, reason, targets = self.accept(data)
                    self.assertIsNone(receipt)
                    self.assertEqual(reason, "journal_editorial_failed")
                    self.assertEqual(targets[0]["check"], check)
                    self.assertEqual(targets[0]["field"], "sections[0].body")

    def test_factual_failure_takes_priority_and_preserves_editorial_repair_targets(self):
        data = self.verdict()
        for assessment in data["assessments"]:
            assessment.update(unitIds=["sections[0].body:0"], verdict="unsupported",
                              issues=["Controlled specific defect for " + assessment["check"]])
        receipt, reason, targets = self.accept(data)
        self.assertIsNone(receipt)
        self.assertEqual(reason, "source_attribution_failed")
        self.assertEqual([target["check"] for target in targets], list(review.ASSESSMENT_CHECKS))

    def test_whole_entry_findings_cannot_crowd_later_checks_out_of_repair_budget(self):
        self.article["sections"] = [
            {"heading": "Part " + str(index),
             "body": "Test Listener said the bot invented it. I am fond of this small disagreement."}
            for index in range(3)
        ]
        data = self.verdict()
        for assessment in data["assessments"]:
            assessment.update(verdict="unsupported", issues=["Controlled whole-entry finding."])
        _, reason, targets = self.accept(data)
        self.assertEqual(reason, "source_attribution_failed")
        self.assertEqual(len(targets), 4)
        self.assertEqual([target["check"] for target in targets[:12]], list(review.ASSESSMENT_CHECKS))
        expected_units = [unit["unitId"] for unit in review.public_units(self.article)]
        expected_fields = list(dict.fromkeys(unit["field"] for unit in review.public_units(self.article)))
        self.assertEqual(len(expected_fields), 8)
        for target in targets:
            self.assertEqual(target["unitIds"], expected_units)
            self.assertEqual(target["fieldPaths"], expected_fields)
            self.assertEqual(target["field"], expected_fields[0])
            self.assertEqual(target["sourceRefIds"], ["fresh:1", "fresh:2"])

    def test_grounded_personal_comparison_and_selective_detail_can_pass_without_quotas(self):
        data = self.verdict()
        explanations = {
            "event_relationships": "The two comments are attributed separately; BNL's sticker metaphor does not claim a shared event.",
            "attribution_stance": "The allegation remains the listener's statement, while the host's later explanation has its own attribution.",
            "journal_perspective": "The controlled fixture accepts BNL's comic personal investment in a tiny object; no required pronoun or emotional quota.",
            "detail_retention": "The selected disagreement preserves both stances and the sticker detail; unrelated sources need not be inventoried.",
        }
        for assessment in data["assessments"]:
            assessment["explanation"] = explanations[assessment["check"]]
            if assessment["check"] == "journal_perspective":
                assessment["sourceRefIds"] = []
        receipt, reason, targets = self.accept(data)
        self.assertEqual((reason, targets), ("", []))
        self.assertEqual(receipt["assessments"], data["assessments"])
        self.assertEqual(receipt["verdict"], "supported")

    def test_negative_overall_verdict_without_located_findings_is_protocol_failure(self):
        data = self.verdict()
        data["verdict"] = "uncertain"
        self.assertEqual(self.accept(data), (None, "source_review_invalid", []))

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

    def test_malformed_enum_values_fail_closed_instead_of_crashing(self):
        for bad in ([], {}, None, 42):
            data = self.verdict()
            data["verdict"] = bad
            self.assertEqual(self.accept(data)[1], "source_review_invalid")
            data = self.verdict()
            self.claims(data)[0]["evidence"][0]["use"] = bad
            self.assertEqual(self.accept(data)[1], "source_review_invalid_anchor")

    def test_compact_response_schema_names_claims_and_server_bound_fragments(self):
        schema = review.response_schema()
        self.assertEqual(schema["propertyOrdering"], ["units", "assessments", "verdict"])
        unit = schema["properties"]["units"]["items"]
        self.assertEqual(unit["propertyOrdering"], ["unitId", "claims", "nonFactualReason"])
        claim = unit["properties"]["claims"]["items"]
        anchor = claim["properties"]["evidence"]["items"]
        self.assertEqual(set(anchor["properties"]), {"fragmentId", "use"})
        self.assertLess(claim["propertyOrdering"].index("evidence"), claim["propertyOrdering"].index("support"))


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
