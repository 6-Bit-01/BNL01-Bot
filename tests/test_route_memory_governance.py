import os
import tempfile
import unittest

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl01_bot
from bnl_source_file_enrichment import parse_source_enrichment_command


class RouteModeGovernanceTests(unittest.TestCase):
    def test_simple_greeting_maps_to_simple_greeting(self):
        for text in ("Hi BNL", "hey BNL", "yo BNL", "hello BNL", "good morning BNL", "BNL?"):
            self.assertTrue(bnl01_bot.is_simple_greeting_to_bnl(text), text)
            self.assertEqual(
                bnl01_bot.classify_route_mode(text, "sealed_test", real_direct_target=True),
                bnl01_bot.ROUTE_MODE_SIMPLE_GREETING,
            )

    def test_source_enrich_and_backfill_commands_do_not_map_to_normal_chat(self):
        self.assertEqual(
            bnl01_bot.classify_route_mode("!bnl source enrich HellcatNZ | dry_run=true", "internal_controlled", real_direct_target=True),
            bnl01_bot.ROUTE_MODE_SOURCE_ENRICHMENT,
        )
        self.assertEqual(
            bnl01_bot.classify_route_mode("!bnl backfill channel hellcat-nz | limit=20 | dry_run=true", "internal_controlled", real_direct_target=True),
            bnl01_bot.ROUTE_MODE_APPROVED_BACKFILL,
        )

    def test_public_direct_mention_maps_to_normal_chat(self):
        self.assertEqual(
            bnl01_bot.classify_route_mode("BNL what is Barcode doing?", "public_context", real_direct_target=True),
            bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
        )

    def test_route_contract_contains_expected_modes_and_policies(self):
        self.assertIn(bnl01_bot.ROUTE_MODE_NORMAL_CHAT, bnl01_bot.ROUTE_MODE_CONTRACTS)
        self.assertIn("public_context", bnl01_bot.ROUTE_MODE_CONTRACTS[bnl01_bot.ROUTE_MODE_NORMAL_CHAT].allowed_channel_policies)
        self.assertFalse(bnl01_bot.CHANNEL_POLICY_CONTRACTS["unknown"]["memory_tiers"])


class SubjectExtractionBoundaryTests(unittest.TestCase):
    def test_questions_are_not_subject_candidates(self):
        for text in ("Is BARCODE back this week?", "baking. Is BARCODE back this week", "What are we doing today?"):
            self.assertFalse(
                bnl01_bot.is_valid_subject_candidate_for_memory_or_scouting(
                    text,
                    bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                    "public_context",
                ),
                text,
            )

    def test_explicit_source_command_still_parses_subject(self):
        matched, options, error = parse_source_enrichment_command("!bnl source enrich HellcatNZ | dry_run=true")
        self.assertTrue(matched, error)
        self.assertEqual(options["subject"], "HellcatNZ")
        self.assertTrue(
            bnl01_bot.is_valid_subject_candidate_for_memory_or_scouting(
                "HellcatNZ",
                bnl01_bot.ROUTE_MODE_SOURCE_ENRICHMENT,
                "internal_controlled",
            )
        )


class MemoryPolicyGovernanceTests(unittest.TestCase):
    def test_unknown_policy_model_save_does_not_write_durable_memory(self):
        decision = bnl01_bot.decide_memory_write_policy(
            bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
            "unknown",
            "model",
            "Records are thin, but here is a reply.",
            True,
        )
        self.assertTrue(decision.save_conversation)
        self.assertFalse(decision.write_memory_tier)
        self.assertFalse(decision.update_relationship)

    def test_sealed_and_internal_do_not_write_public_durable_memory(self):
        for policy in ("sealed_test", "internal_controlled"):
            decision = bnl01_bot.decide_memory_write_policy(
                bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                policy,
                "user",
                "I have a durable preference that would normally matter.",
                False,
            )
            self.assertFalse(decision.write_memory_tier)
            self.assertFalse(decision.update_habits)

    def test_throwaway_public_chatter_does_not_become_memory_tier(self):
        decision = bnl01_bot.decide_memory_write_policy(
            bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
            "public_context",
            "user",
            "lol thanks BNL",
            False,
        )
        self.assertTrue(decision.save_conversation)
        self.assertFalse(decision.write_memory_tier)

    def test_quality_public_statement_can_write_memory_tier(self):
        decision = bnl01_bot.decide_memory_write_policy(
            bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
            "public_context",
            "user",
            "I prefer BARCODE updates in concise bullet points on show days.",
            False,
        )
        self.assertTrue(decision.write_memory_tier)


class MemoryInjectionGovernanceTests(unittest.TestCase):
    def test_simple_greeting_memory_context_is_skipped(self):
        context = bnl01_bot.build_user_memory_context(1, 1, route_mode=bnl01_bot.ROUTE_MODE_SIMPLE_GREETING, channel_policy="sealed_test")
        self.assertIn("skipped", context.lower())

    def test_normal_public_prompt_has_normal_chat_contract(self):
        old_db = bnl01_bot.DB_FILE
        try:
            with tempfile.NamedTemporaryFile(delete=True) as tmp:
                bnl01_bot.DB_FILE = tmp.name
                bnl01_bot.init_db()
                prompt, _allow, _style = bnl01_bot.build_user_aware_prompt(
                    1,
                    1,
                    "6 Bit",
                    "I had to visit Lardcode radio. Is Barcode back this week?",
                    channel_name="general",
                    channel_policy="public_context",
                    route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                )
        finally:
            bnl01_bot.DB_FILE = old_db
        self.assertIn("Mode contract: normal_chat", prompt)
        self.assertIn("Do not classify the user's phrasing as a subject/entity", prompt)


class LeakGuardTests(unittest.TestCase):
    def test_normal_chat_catches_scripted_language(self):
        for text in ("Records are thin", "one suspicious blinking light", "preliminary analysis", "data stream"):
            self.assertTrue(bnl01_bot.detect_scripted_mode_leak(text, bnl01_bot.ROUTE_MODE_NORMAL_CHAT), text)

    def test_guard_does_not_fire_for_source_mode_or_operator_mode(self):
        self.assertFalse(bnl01_bot.detect_scripted_mode_leak("Classification: candidate", bnl01_bot.ROUTE_MODE_SOURCE_ENRICHMENT))
        self.assertFalse(bnl01_bot.detect_scripted_mode_leak("Records are thin", bnl01_bot.ROUTE_MODE_OPERATOR_COMMAND))


class RegressionExamplesTests(unittest.TestCase):
    def test_lardcode_prompt_not_subject_candidate(self):
        text = "I had to go to another dimension on Friday where they have Lardcode radio. It's like Barcode but all songs are about baking. Is Barcode back this week?"
        self.assertFalse(bnl01_bot.is_valid_subject_candidate_for_memory_or_scouting(text, bnl01_bot.ROUTE_MODE_NORMAL_CHAT, "public_context"))
        self.assertEqual(bnl01_bot.classify_route_mode(text, "public_context", real_direct_target=True), bnl01_bot.ROUTE_MODE_NORMAL_CHAT)

    def test_simple_greeting_response_is_plain(self):
        response = bnl01_bot.build_simple_greeting_response("6 Bit")
        self.assertIn("I’m here", response)
        self.assertFalse(bnl01_bot.detect_scripted_mode_leak(response, bnl01_bot.ROUTE_MODE_SIMPLE_GREETING))


if __name__ == "__main__":
    unittest.main()
