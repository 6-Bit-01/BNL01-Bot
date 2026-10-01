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
        return {"units": units, "verdict": "supported"}

    @staticmethod
    def spans(data):
        return [span for unit in data["units"] for span in unit["spans"]]

    def accept(self, value):
        return review.accept_review(json.dumps(value), self.article, self.sources)

    def test_complete_source_review_bound_to_exact_candidate(self):
        receipt, reason, targets = self.accept(self.verdict())
        self.assertEqual((reason, targets), ("", []))
        self.assertEqual(receipt["version"], 2)
        self.assertEqual(receipt["articleDigest"], review.article_digest(self.article))
        for mutate in (
            lambda a: a.update(title="An Unchecked Different Story"),
            lambda a: a["sections"][0].update(body="Test Host accused the bot instead."),
            lambda a: a["sourceRefIds"].update({"Who Said What": ["fresh:2"]}),
            lambda a: a["metadata"].update(contextUses=[{"claim": "An unchecked memory"}]),
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
        self.sources.append({"refId": "fresh:exchange", "contributions": copy.deepcopy(self.sources[:2])})
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
        self.assertEqual(schema["propertyOrdering"], ["units", "verdict"])
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

    def test_genuine_personal_reflection_does_not_need_invented_external_anchor(self):
        data = self.verdict()
        self.assertTrue(any(u["kind"] == "reflection" and not u["evidence"] for u in self.spans(data)))
        self.assertEqual(self.accept(data)[1], "")

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
                     "atomic external factual clause", "contradictory", "ordered verbatim spans"):
            self.assertIn(text, prompt)


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
