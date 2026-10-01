"""Protocol regressions; mocked verdicts are not semantic model quality proof."""
import copy
import json
import os
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import bnl_journal_attribution as review


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

    def verdict(self):
        units = []
        for unit in review.public_units(self.article):
            item = {"text": unit["text"], "kind": "creative", "verdict": "supported",
                    "evidence": [], "issues": []}
            if unit["text"].startswith("Test "):
                source = self.sources[0 if unit["text"].startswith("Test Listener") else 1]
                item.update(kind="factual", evidence=[{"refId": source["refId"],
                    "quote": source["summary"], "speaker": source["participantAlias"], "use": "speech"}])
            if unit["text"].startswith("I am"):
                item["kind"] = "reflection"
            units.append({"unitId": unit["unitId"], "spans": [item]})
        return {"assessments": [
            {"check": check, "unitIds": [unit["unitId"] for unit in units],
             "sourceRefIds": ["fresh:1", "fresh:2"],
             "explanation": "Controlled passing " + check + " fixture; not a live quality verdict.",
             "issues": [], "verdict": "supported"} for check in review.ASSESSMENT_CHECKS
        ], "units": units, "verdict": "supported"}

    @staticmethod
    def spans(data):
        return [span for unit in data["units"] for span in unit["spans"]]

    def accept(self, value):
        return review.accept_review(json.dumps(value), self.article, self.sources)

    def test_complete_source_review_bound_to_exact_candidate(self):
        receipt, reason, targets = self.accept(self.verdict())
        self.assertEqual((reason, targets), ("", []))
        self.assertEqual(receipt["version"], 3)
        self.assertEqual(receipt["assessments"], self.verdict()["assessments"])
        self.assertEqual(receipt["articleDigest"], review.article_digest(self.article))
        for mutate in (
            lambda a: a.update(title="An Unchecked Different Story"),
            lambda a: a["sections"][0].update(body="Test Host accused the bot instead."),
            lambda a: a["sourceRefIds"].update({"Who Said What": ["fresh:2"]}),
            lambda a: a["metadata"].update(contextUses=[{"claim": "An unchecked memory"}]),
            lambda a: a["sections"][0].update(body=a["sections"][0]["body"].replace(". ", ".\n\n", 1)),
        ):
            edited = copy.deepcopy(self.article)
            mutate(edited)
            self.assertNotEqual(receipt["articleDigest"], review.article_digest(edited))

    def test_empty_duplicate_missing_and_unknown_units_cannot_pass(self):
        for mode in ("empty", "duplicate", "missing", "unknown"):
            data = self.verdict()
            if mode == "empty": data["units"] = []
            elif mode == "duplicate": data["units"][-1] = data["units"][0]
            elif mode == "missing": data["units"].pop()
            else: data["units"][-1]["unitId"] = "not-a-candidate-unit"
            self.assertEqual(self.accept(data)[1], "source_review_incomplete", mode)

    def test_actual_author_binding_rejects_recipient_or_swapped_speaker(self):
        data = self.verdict()
        anchor = next(u for u in self.spans(data) if u["kind"] == "factual")["evidence"][0]
        anchor["speaker"] = "host"
        self.assertEqual(self.accept(data)[1], "source_review_invalid_anchor")

    def test_unique_public_speaker_name_is_bound_and_receipt_uses_canonical_alias(self):
        data = self.verdict()
        for item in self.spans(data):
            for anchor in item["evidence"]:
                source = next(s for s in self.sources if s["refId"] == anchor["refId"])
                anchor["speaker"] = source["publicSpeakerName"]
        # Repeated contributions by the same named person do not create ambiguity.
        self.sources.append(dict(self.sources[0], refId="fresh:other"))
        receipt, reason, targets = self.accept(data)
        self.assertEqual((reason, targets), ("", []))
        self.assertEqual([a["speaker"] for u in self.spans(receipt) for a in u["evidence"]],
                         ["listener", "host"])

    def test_public_speaker_name_collision_swap_and_unknown_name_are_rejected(self):
        for name in ("Test Host", "Test Stranger", "test listener", "Test Listener"):
            with self.subTest(name=name):
                data = self.verdict()
                anchor = next(u for u in self.spans(data) if u["kind"] == "factual")["evidence"][0]
                anchor["speaker"] = name
                if name == "Test Listener":
                    self.sources.append({"refId": "fresh:collision", "participantAlias": "other",
                                         "publicSpeakerName": name, "summary": "An unrelated message."})
                self.assertEqual(self.accept(data)[1], "source_review_invalid_anchor")

    def test_public_name_must_bind_to_quoted_contribution_within_the_cited_source(self):
        data = self.verdict()
        anchor = next(u for u in self.spans(data) if u["kind"] == "factual")["evidence"][0]
        self.sources.append({"refId": "fresh:exchange", "sourceRole": "original_contribution",
                             "contributions": copy.deepcopy(self.sources[:2])})
        anchor.update(refId="fresh:exchange", speaker="Test Listener")
        self.assertEqual(self.accept(data)[1], "")
        anchor["speaker"] = "Test Host"
        self.assertEqual(self.accept(data)[1], "source_review_invalid_anchor")
        anchor["speaker"] = "Test Listener"
        anchor["quote"] = "It was the sticker joke."
        self.assertEqual(self.accept(data)[1], "source_review_invalid_anchor")

    def test_exact_alias_cannot_be_reinterpreted_as_another_persons_public_name(self):
        data = self.verdict()
        self.sources[0]["publicSpeakerName"] = "host"
        anchor = next(u for u in self.spans(data) if u["kind"] == "factual")["evidence"][0]
        anchor["speaker"] = "host"
        self.assertEqual(self.accept(data)[1], "source_review_invalid_anchor")

    def test_unknown_ref_or_invented_source_quote_cannot_pass(self):
        for field, value in (("refId", "fresh:missing"), ("quote", "It happened before BNL replied.")):
            data = self.verdict()
            anchor = next(u for u in self.spans(data) if u["kind"] == "factual")["evidence"][0]
            anchor[field] = value
            self.assertEqual(self.accept(data)[1], "source_review_invalid_anchor")

    def test_factual_unit_requires_evidence_even_with_supported_verdict(self):
        data = self.verdict()
        next(u for u in self.spans(data) if u["kind"] == "factual")["evidence"] = []
        self.assertEqual(self.accept(data)[1], "source_review_missing_evidence")

    def test_mixed_reflection_has_separately_anchored_factual_clause_and_located_defect(self):
        self.article["sections"][0]["body"] = (
            "I liked that Test Listener called the bridge unfinished, though I imagined the ceiling applauding.")
        self.sources[0]["summary"] = "The bridge is unfinished."
        data = self.verdict()
        body = next(u for u in data["units"] if ".body:" in u["unitId"])
        body["spans"] = [
            {"text": "I liked that ", "kind": "reflection", "evidence": [],
             "issues": [], "verdict": "supported"},
            {"text": "Test Listener called the bridge unfinished", "kind": "factual",
             "evidence": [{"refId": "fresh:1", "quote": "The bridge is unfinished.",
                           "speaker": "Test Listener", "use": "speech"}],
             "issues": [], "verdict": "supported"},
            {"text": ", though I imagined the ceiling applauding.", "kind": "creative",
             "evidence": [], "issues": [], "verdict": "supported"},
        ]
        self.assertEqual(self.accept(data)[1], "")
        claim = body["spans"][1]
        claim["evidence"] = []
        self.assertEqual(self.accept(data)[1], "source_review_missing_evidence")
        claim.update(verdict="unsupported", issues=["The later original clarifies a different subject."])
        receipt, reason, targets = self.accept(data)
        self.assertIsNone(receipt)
        self.assertEqual(reason, "source_attribution_failed")
        self.assertEqual(targets, [{"field": "sections[0].body", "check": "source_attribution",
                                  "unitId": body["unitId"], "spanIndex": 1,
                                  "claim": claim["text"], "issues": claim["issues"]}])

    def test_span_coverage_rejects_omission_invention_reordering_and_v1_verdict(self):
        for mode in ("omitted", "invented", "reordered", "duplicated", "empty", "v1"):
            with self.subTest(mode=mode):
                data = self.verdict()
                unit = next(u for u in data["units"] if ".body:" in u["unitId"])
                original = unit["spans"][0]
                if mode == "omitted": original["text"] = "Test Listener"
                elif mode == "invented": original["text"] = "Test Listener definitely said the bot invented it."
                elif mode == "reordered":
                    unit["spans"] = [dict(original, text="said the bot invented it."),
                                     dict(original, text="Test Listener")]
                elif mode == "duplicated": unit["spans"].append(copy.deepcopy(original))
                elif mode == "empty": unit["spans"] = []
                else:
                    unit.pop("spans")
                    unit.update(kind="factual", evidence=original["evidence"], issues=[], verdict="supported")
                self.assertEqual(self.accept(data)[1], "source_review_incomplete")

    def test_span_coverage_permits_only_whitespace_normalization(self):
        data = self.verdict()
        span = next(u for u in self.spans(data) if u["kind"] == "factual")
        span["text"] = "\n Test   Listener said\n the bot invented it. "
        self.assertEqual(self.accept(data)[1], "")
        span["text"] = "TestListener said the bot invented it."
        self.assertEqual(self.accept(data)[1], "source_review_incomplete")

    def test_metadata_anchors_require_exact_value_on_the_same_original_contribution(self):
        self.sources[0].update(observedAtPacific="2026-06-09T19:30:00-07:00", roomRef="room:fictional-a")
        for field in ("observedAtPacific", "roomRef"):
            with self.subTest(field=field):
                data = self.verdict()
                anchor = next(u for u in self.spans(data) if u["kind"] == "factual")["evidence"][0]
                anchor.update(field=field, quote=self.sources[0][field], speaker="Test Listener", use="context")
                self.assertEqual(self.accept(data)[1], "")
                anchor["quote"] = self.sources[0][field][:-1]
                self.assertEqual(self.accept(data)[1], "source_review_invalid_anchor")
                anchor.update(quote=self.sources[0][field], refId="fresh:2", speaker="Test Host")
                self.assertEqual(self.accept(data)[1], "source_review_invalid_anchor")
        # Aggregate metadata cannot masquerade as an original contributor's time.
        self.sources.append({"refId": "reflection:group", "observedAtPacific": "2026-06-09T19:30:00-07:00",
                             "contributions": [dict(self.sources[1])]})
        anchor.update(refId="reflection:group", field="observedAtPacific",
                      quote="2026-06-09T19:30:00-07:00", speaker="Test Host")
        self.assertEqual(self.accept(data)[1], "source_review_invalid_anchor")

    def test_schema_orders_evidence_and_issues_before_any_verdict(self):
        schema = review.response_schema()
        self.assertEqual(schema["propertyOrdering"], ["assessments", "units", "verdict"])
        assessment = schema["properties"]["assessments"]["items"]
        self.assertEqual(assessment["propertyOrdering"],
                         ["check", "unitIds", "sourceRefIds", "explanation", "issues", "verdict"])
        unit = schema["properties"]["units"]["items"]
        self.assertEqual(unit["required"], ["unitId", "spans"])
        span = unit["properties"]["spans"]["items"]
        self.assertEqual(span["propertyOrdering"], ["text", "kind", "evidence", "issues", "verdict"])
        anchor = span["properties"]["evidence"]["items"]
        self.assertNotIn("field", anchor["required"])
        self.assertEqual(anchor["properties"]["field"]["enum"], ["summary", "observedAtPacific", "roomRef"])

    def test_negative_uncertain_and_unresolved_issues_return_exact_repair_target(self):
        for verdict in ("unsupported", "uncertain", "supported"):
            data = self.verdict()
            target = next(u for u in self.spans(data) if u["kind"] == "factual")
            target.update(verdict=verdict, issues=["The accusation belongs to the listener, not its recipient."])
            receipt, reason, targets = self.accept(data)
            self.assertIsNone(receipt)
            self.assertEqual(reason, "source_attribution_failed")
            self.assertEqual(targets[0]["claim"], "Test Listener said the bot invented it.")
            self.assertEqual(targets[0]["field"], "sections[0].body")

    def test_bnl_speech_is_not_corroboration_of_external_event(self):
        data = self.verdict()
        target = next(u for u in self.spans(data) if u["kind"] == "factual")
        target["evidence"] = [{"refId": "speech:3", "quote": "I already answered that.",
                               "speaker": "bnl", "use": "event"}]
        self.assertEqual(self.accept(data)[1], "source_review_derived_as_fact")
        target["evidence"][0]["use"] = "speech"
        self.assertEqual(self.accept(data)[1], "")

    def test_derived_contributor_prose_cannot_masquerade_as_original_speech(self):
        for kind in ("public_moment", "published_journal", "published_ballad", "accepted_relay_continuity"):
            with self.subTest(kind=kind):
                self.sources.append({"refId": "reflection:" + kind, "basisKind": kind,
                                     "contributions": [copy.deepcopy(self.sources[0])]})
                data = self.verdict()
                anchor = next(s for s in self.spans(data) if s["kind"] == "factual")["evidence"][0]
                anchor["refId"] = "reflection:" + kind
                for use in ("speech", "event"):
                    anchor["use"] = use
                    self.assertEqual(self.accept(data)[1], "source_review_derived_as_fact")
                # It remains available as interpretation/publication context.
                anchor["use"] = "context"
                self.assertEqual(self.accept(data)[1], "")

    def test_same_fresh_wording_does_not_imply_use_of_unrelated_memory(self):
        self.sources.append({"refId": "memory:old", "summary": "The bot invented it during an older unrelated joke.",
                             "epistemicStatus": "established_network_record"})
        contract = {"memory:old": {"laneType": "established_broadcast_memory"}}
        data = self.verdict()
        self.assertEqual(review.accept_review(json.dumps(data), self.article, self.sources,
                                             context_contract=contract)[1], "")

    def test_memory_and_rumor_anchor_require_exact_section_and_lane_declaration(self):
        for lane_type, role in (("established_broadcast_memory", "established_memory"),
                                ("community_rumor", "rumor")):
            with self.subTest(lane_type=lane_type):
                source = {"refId": "lane:test", "summary": "An earlier discussion about the sticker.",
                          "authority": role}
                sources = [*self.sources, source]
                contract = {"lane:test": {"laneType": lane_type}}
                data = self.verdict()
                body = next(unit for unit in data["units"] if ".body:" in unit["unitId"])
                body["spans"][0]["evidence"] = [
                    {"refId": "lane:test", "quote": source["summary"], "speaker": "", "use": "context"}]
                self.article["metadata"]["contextUses"] = []
                receipt, reason, targets = review.accept_review(json.dumps(data), self.article, sources,
                                                                context_contract=contract)
                self.assertIsNone(receipt)
                self.assertEqual(reason, "source_attribution_failed")
                self.assertEqual(targets[0]["check"], "missing_context_declaration")
                self.assertEqual(targets[0]["field"], "sections[0].body")
                self.assertEqual(targets[0]["laneRefId"], "lane:test")
                self.assertEqual(targets[0]["laneType"], lane_type)
                declaration = {"laneRefId": "lane:test", "laneType": lane_type,
                               "sectionHeading": self.article["sections"][0]["heading"]}
                for key, wrong in (("sectionHeading", "Another Section"), ("laneRefId", "lane:other"),
                                   ("laneType", "bnl_inference")):
                    self.article["metadata"]["contextUses"] = [dict(declaration, **{key: wrong})]
                    self.assertEqual(review.accept_review(json.dumps(data), self.article, sources,
                                                         context_contract=contract)[1], "source_attribution_failed")
                self.article["metadata"]["contextUses"] = [declaration]
                receipt, reason, _ = review.accept_review(json.dumps(data), self.article, sources,
                                                         context_contract=contract)
                self.assertEqual(reason, "")
                self.assertEqual(len(receipt["assessments"]), 4)
                self.assertEqual(receipt["articleDigest"], review.article_digest(self.article))

    def test_title_or_excerpt_cannot_borrow_body_memory_declaration(self):
        source = {"refId": "memory:earlier", "summary": "An earlier sticker discussion.",
                  "authority": "established_memory"}
        contract = {source["refId"]: {"laneType": "established_broadcast_memory"}}
        self.article["metadata"]["contextUses"] = [{"laneRefId": source["refId"],
            "laneType": "established_broadcast_memory", "sectionHeading": "Who Said What"}]
        for field in ("title", "excerpt"):
            with self.subTest(field=field):
                data = self.verdict()
                unit = next(unit for unit in data["units"] if unit["unitId"] == field + ":0")
                unit["spans"][0]["evidence"] = [
                    {"refId": source["refId"], "quote": source["summary"], "speaker": "", "use": "context"}]
                _, reason, targets = review.accept_review(json.dumps(data), self.article, [*self.sources, source],
                                                          context_contract=contract)
                self.assertEqual(reason, "source_attribution_failed")
                self.assertEqual(targets[0]["field"], field)

    def test_source_authority_uses_owner_role_not_confident_derived_wording(self):
        self.assertEqual(review.source_authority(self.sources[0]), "original")
        self.assertEqual(review.source_authority(self.sources[2]), "speech_only")
        self.assertEqual(review.source_authority({"sourceRole": "recorded_event"}), "original")
        self.assertEqual(review.source_authority({"basisKind": "public_source_history"}), "original")
        self.assertEqual(review.source_authority({"sourceRole": "approved_canon"}), "canon")
        self.assertEqual(review.source_authority({"epistemicStatus": "established_network_record"}), "established_memory")
        self.assertEqual(review.source_authority({"sourceRole": "unconfirmed_rumor"}), "rumor")
        for source in ({"basisKind": "public_moment", "sourceRole": "original_contribution"},
                       {"sourceRole": "bnl_interpretation", "authority": "original"},
                       {"summary": "The truth has been confirmed by all participants."}):
            self.assertEqual(review.source_authority(source), "derived_context")

    def test_reviewer_receives_primary_records_first_and_only_declared_context_metadata(self):
        derived = {"refId": "reflection:earlier", "basisKind": "public_moment",
                   "summary": "The earlier automated description was invented."}
        evidence = {"sources": [derived, self.sources[2], *self.sources[:2]], "contextLanes": {}}
        original = copy.deepcopy(evidence)
        self.article["metadata"]["privateFixture"] = "never project this field"
        self.article["metadata"]["contextUses"] = [{"laneRefId": "memory:earlier", "laneType": "established_broadcast_memory",
                                                  "sectionHeading": "Who Said What", "claim": "A declared public claim.",
                                                  "basisRefIds": ["exchange:original"],
                                                  "privateFixture": "never project this nested field"}]
        prompt = review.review_prompt(self.article, evidence)
        projected, _ = json.JSONDecoder().raw_decode(prompt.split("ORIGINAL_EVIDENCE_JSON: ", 1)[1])
        self.assertEqual([s["authority"] for s in projected["sources"]],
                         ["speech_only", "original", "original", "derived_context"])
        self.assertEqual(evidence, original)
        declarations, _ = json.JSONDecoder().raw_decode(prompt.split("CANDIDATE_CONTEXT_USES_JSON: ", 1)[1])
        self.assertEqual(declarations, [{key: value for key, value in self.article["metadata"]["contextUses"][0].items()
                                        if key != "privateFixture"}])
        self.assertNotIn("never project this field", prompt)
        self.assertNotIn("never project this nested field", prompt)
        for text in ("An allegation cannot erase the utterance", "contributor summaries are still derived prose",
                     "Shared words or themes", "Fresh evidence can support its own wording"):
            self.assertIn(text, prompt)

    def test_genuine_personal_reflection_does_not_need_invented_external_anchor(self):
        data = self.verdict()
        self.assertTrue(any(u["kind"] == "reflection" and not u["evidence"] for u in self.spans(data)))
        self.assertEqual(self.accept(data)[1], "")

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
                self.assertTrue(all(span["verdict"] == "supported" for span in self.spans(data)))
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

    def test_raw_or_single_complete_json_fence_preserves_all_review_checks(self):
        data = self.verdict()
        raw = json.dumps(data)
        for value in (raw, " \n" + raw + "\n ", "```json\n" + raw + "\n```",
                      "```JSON\r\n" + raw + "\r\n```", "```\n" + raw + "\n```"):
            receipt, reason, _ = review.accept_review(value, self.article, self.sources)
            self.assertEqual(reason, "")
            self.assertEqual(receipt["articleDigest"], review.article_digest(self.article))
        factual = next(u for u in self.spans(data) if u["kind"] == "factual")
        factual.update(verdict="unsupported", evidence=[], issues=["The original says otherwise."])
        self.assertEqual(review.accept_review("```json\n" + json.dumps(data) + "\n```",
                                            self.article, self.sources)[1], "source_attribution_failed")

    def test_surplus_prose_multiple_documents_or_incomplete_fences_are_rejected(self):
        raw = json.dumps(self.verdict())
        fenced = "```json\n" + raw + "\n```"
        for value in ("Here is the review.\n" + fenced, fenced + "\nApproved.",
                      raw + raw, fenced + "\n" + fenced, "```json\n" + raw,
                      raw + "\n```", "```python\n" + raw + "\n```"):
            self.assertEqual(review.accept_review(value, self.article, self.sources)[1],
                             "source_review_invalid")

    def test_truncated_or_nonobject_review_is_withheld(self):
        for raw in ('{"verdict":"supported"', '```json\n{}\n```', '[]', 'null'):
            self.assertEqual(review.accept_review(raw, self.article, self.sources)[1], "source_review_invalid")

    def test_malformed_enum_values_consume_a_slot_instead_of_crashing(self):
        for bad in ([], {}, None, 42):
            data = self.verdict()
            data["verdict"] = bad
            self.assertEqual(self.accept(data)[1], "source_review_invalid")
            for key in ("verdict", "kind"):
                data = self.verdict()
                data["units"][0]["spans"][0][key] = bad
                self.assertEqual(self.accept(data)[1], "source_review_invalid")
            data = self.verdict()
            next(u for u in self.spans(data) if u["evidence"])["evidence"][0]["use"] = bad
            self.assertEqual(self.accept(data)[1], "source_review_invalid_anchor")

    def test_prompt_requires_original_speakers_later_clarification_and_factual_clauses(self):
        prompt = review.review_prompt(self.article, {"sources": self.sources})
        self.assertTrue(prompt.startswith(review.REVIEW_PREFIX))
        for text in ("do not rewrite", "later clarifications", "recipient", "reply order", "mixed sentence",
                     "personal voice", "EVERY unit", "never a member's biography", "evidence-first",
                     "atomic external factual clause", "contradictory", "ordered verbatim spans",
                     "ACROSS sentences", "A generic statement", "allegation", "complete writing",
                     "not completeness, a roll call", "First read the complete article"):
            self.assertIn(text, prompt)
        article, _ = json.JSONDecoder().raw_decode(prompt.split("CANDIDATE_ARTICLE_JSON: ", 1)[1])
        self.assertEqual(article["sections"], review._candidate_article(self.article)["sections"])
        self.assertEqual(article["title"], self.article["title"])
        self.assertNotIn("metadata", article)


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
                         (self.bot.BNL01_JOURNAL_SYSTEM_PROMPT + "\n\nwrite a Journal", self.bot.JOURNAL_ROUTE))


if __name__ == "__main__":
    unittest.main()
