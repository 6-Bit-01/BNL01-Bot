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
            item = {"unitId": unit["unitId"], "kind": "creative", "verdict": "supported",
                    "evidence": [], "issues": []}
            if unit["text"].startswith("Test "):
                source = self.sources[0 if unit["text"].startswith("Test Listener") else 1]
                item.update(kind="factual", evidence=[{"refId": source["refId"],
                    "quote": source["summary"], "speaker": source["participantAlias"], "use": "speech"}])
            if unit["text"].startswith("I am"):
                item["kind"] = "reflection"
            units.append(item)
        return {"verdict": "supported", "units": units}

    def accept(self, value):
        return review.accept_review(json.dumps(value), self.article, self.sources)

    def test_complete_source_review_bound_to_exact_candidate(self):
        receipt, reason, targets = self.accept(self.verdict())
        self.assertEqual((reason, targets), ("", []))
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
        anchor = next(u for u in data["units"] if u["kind"] == "factual")["evidence"][0]
        anchor["speaker"] = "host"
        self.assertEqual(self.accept(data)[1], "source_review_invalid_anchor")

    def test_unknown_ref_or_invented_source_quote_cannot_pass(self):
        for field, value in (("refId", "fresh:missing"), ("quote", "It happened before BNL replied.")):
            data = self.verdict()
            anchor = next(u for u in data["units"] if u["kind"] == "factual")["evidence"][0]
            anchor[field] = value
            self.assertEqual(self.accept(data)[1], "source_review_invalid_anchor")

    def test_factual_unit_requires_evidence_even_with_supported_verdict(self):
        data = self.verdict()
        next(u for u in data["units"] if u["kind"] == "factual")["evidence"] = []
        self.assertEqual(self.accept(data)[1], "source_review_missing_evidence")

    def test_negative_uncertain_and_unresolved_issues_return_exact_repair_target(self):
        for verdict in ("unsupported", "uncertain", "supported"):
            data = self.verdict()
            target = next(u for u in data["units"] if u["kind"] == "factual")
            target.update(verdict=verdict, issues=["The accusation belongs to the listener, not its recipient."])
            receipt, reason, targets = self.accept(data)
            self.assertIsNone(receipt)
            self.assertEqual(reason, "source_attribution_failed")
            self.assertEqual(targets[0]["claim"], "Test Listener said the bot invented it.")
            self.assertEqual(targets[0]["field"], "sections[0].body")

    def test_bnl_speech_is_not_corroboration_of_external_event(self):
        data = self.verdict()
        target = next(u for u in data["units"] if u["kind"] == "factual")
        target["evidence"] = [{"refId": "speech:3", "quote": "I already answered that.",
                               "speaker": "bnl", "use": "event"}]
        self.assertEqual(self.accept(data)[1], "source_review_derived_as_fact")
        target["evidence"][0]["use"] = "speech"
        self.assertEqual(self.accept(data)[1], "")

    def test_genuine_personal_reflection_does_not_need_invented_external_anchor(self):
        data = self.verdict()
        self.assertTrue(any(u["kind"] == "reflection" and not u["evidence"] for u in data["units"]))
        self.assertEqual(self.accept(data)[1], "")

    def test_truncated_fenced_or_nonobject_review_is_withheld(self):
        for raw in ('{"verdict":"supported"', '```json\n{}\n```', '[]', 'null'):
            self.assertEqual(review.accept_review(raw, self.article, self.sources)[1], "source_review_invalid")

    def test_malformed_enum_values_consume_a_slot_instead_of_crashing(self):
        for bad in ([], {}, None, 42):
            data = self.verdict()
            data["verdict"] = bad
            self.assertEqual(self.accept(data)[1], "source_review_invalid")
            for key in ("verdict", "kind"):
                data = self.verdict()
                data["units"][0][key] = bad
                self.assertEqual(self.accept(data)[1], "source_review_invalid")
            data = self.verdict()
            next(u for u in data["units"] if u["evidence"])["evidence"][0]["use"] = bad
            self.assertEqual(self.accept(data)[1], "source_review_invalid_anchor")

    def test_prompt_requires_original_speakers_later_clarification_and_factual_clauses(self):
        prompt = review.review_prompt(self.article, {"sources": self.sources})
        self.assertTrue(prompt.startswith(review.REVIEW_PREFIX))
        for text in ("do not rewrite", "later clarifications", "recipient", "reply order", "mixed sentence",
                     "personal voice", "EVERY unit", "never a member's biography"):
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
