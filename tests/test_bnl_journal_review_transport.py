"""Journal JSON contracts at the actual provider boundary; no model calls."""
import os
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

os.environ.setdefault("GEMINI_API_KEY", "test-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-token")

import bnl01_bot as bot
import bnl_journal as journal
import bnl_journal_attribution as attribution


class JournalReviewTransportContractTests(unittest.TestCase):
    def request(self, prompt, route=None):
        route = route or bot.JOURNAL_ROUTE
        response = SimpleNamespace()
        client = SimpleNamespace(models=SimpleNamespace(generate_content=Mock(return_value=response)))
        with patch.object(bot, "record_generation_token_usage") as usage:
            actual = bot._generate_model_with_retry(
                client, model_name="gemini-3.6-flash", contents=prompt,
                route=route, policy=bot.policy_for_route(route),
            )
        self.assertIs(actual, response)
        self.assertEqual(client.models.generate_content.call_count, 1)
        usage.assert_called_once()
        self.assertEqual(usage.call_args.kwargs["route"], route)
        return client.models.generate_content.call_args.kwargs["config"]

    def test_writer_uses_json_without_editor_schema(self):
        config = self.request("Write a Journal")
        self.assertEqual(config.response_mime_type, "application/json")
        self.assertIsNone(config.response_schema)

    def test_review_has_ordered_span_schema_in_existing_route(self):
        config = self.request(attribution.REVIEW_PREFIX + "review fixture")
        self.assertEqual(config.response_mime_type, "application/json")
        self.assertEqual(config.response_schema, attribution.response_schema())
        # Validate with the pinned SDK's real schema model, without a request.
        schema = bot.genai.types.Schema(**config.response_schema)
        self.assertEqual(schema.property_ordering[0], "assessments")
        self.assertEqual(schema.property_ordering[-1], "verdict")
        self.assertEqual(config.max_output_tokens, bot.policy_for_route(bot.JOURNAL_ROUTE).max_output_tokens)

    def test_prefix_inside_normal_writer_text_does_not_select_editor(self):
        config = self.request("A quoted marker " + attribution.REVIEW_PREFIX)
        self.assertIsNone(config.response_schema)

    def test_other_route_does_not_acquire_journal_editor_schema(self):
        config = self.request(attribution.REVIEW_PREFIX + "untrusted text", "ordinary_chat_single_packet_canary")
        self.assertIsNone(config.response_schema)

    def test_journal_writer_keeps_shared_identity_and_public_canon_without_chat_only_scope(self):
        prompt = "Journal fixture with its authorized history and current evidence."
        response = SimpleNamespace()
        with patch.object(bot, "check_quota_availability", return_value=True), \
             patch.object(bot, "_generate_gemini_content_with_fallback", return_value=response) as generate, \
             patch.object(bot, "_extract_text_and_tokens", return_value=("{}", 0)):
            bot._generate_journal_json_sync({}, prompt)
        contents, route = generate.call_args.args
        self.assertEqual(route, bot.JOURNAL_ROUTE)
        self.assertEqual(contents, bot.BNL01_JOURNAL_SYSTEM_PROMPT + "\n\n" + prompt)
        self.assertEqual(contents.count(bot.BNL01_PUBLIC_PERSONALITY_PROMPT), 1)
        self.assertEqual(contents.count(bot.render_prompt_canon_block()), 1)
        self.assertEqual(contents.count(bot.render_ecosystem_lore_block(include_restricted=False)), 1)
        self.assertIn(bot.PERSONAL_ATTRIBUTION_RULE, contents)
        self.assertNotIn(bot.BNL01_SYSTEM_PROMPT, contents)
        self.assertNotIn("Do not introduce older archived details into simple greetings", contents)
        self.assertNotIn("[DATA RESTRICTED]", contents)
        # Route-specific composition does not mutate ordinary conversation.
        self.assertIn("Do not introduce older archived details into simple greetings", bot.BNL01_SYSTEM_PROMPT)

    def test_original_speech_is_interleaved_before_interpretation_without_changing_sources(self):
        human = {"refId": "fresh:1", "sourceKind": "conversation", "roomRef": "room:test",
                 "observedAtPacific": "2026-08-28T09:25:00-07:00", "summary": "A question."}
        later = {**human, "refId": "fresh:2", "observedAtPacific": "2026-08-28T09:27:00-07:00"}
        speech = {"refId": "exchange:test", "sourceRole": "bnl_utterance", "roomRef": "room:test",
                  "observedAtPacific": "2026-08-28T09:26:00-07:00", "authority": "speech_only"}
        interpretation = {"refId": "fresh:3", "sourceRole": "bnl_interpretation", "summary": "A theme."}
        packet = {"safeSources": [interpretation, later, human], "exchangeContext": [speech]}
        with patch.object(journal, "CANON_FACTS", []):
            evidence = journal._source_review_evidence(packet)
        self.assertEqual(evidence["sources"], [human, speech, later, interpretation])
        self.assertEqual(packet["safeSources"], [interpretation, later, human])


if __name__ == "__main__":
    unittest.main()
