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


class ConversationPlannerTests(unittest.TestCase):
    def test_simple_greeting_creates_paced_planned_template_response(self):
        route_mode = bnl01_bot.classify_route_mode("Hi BNL", "sealed_test", real_direct_target=True)
        plan = bnl01_bot.plan_conversation_response(
            "Hi BNL",
            "sealed_test",
            route_mode=route_mode,
            real_direct_target=True,
            batching_enabled=True,
        )
        self.assertEqual(plan.route_mode, bnl01_bot.ROUTE_MODE_SIMPLE_GREETING)
        self.assertEqual(plan.response_timing, bnl01_bot.RESPONSE_TIMING_PACED_DIRECT)
        self.assertEqual(plan.response_generation, bnl01_bot.RESPONSE_GENERATION_DETERMINISTIC_TEMPLATE)
        self.assertEqual(plan.pacing_behavior, bnl01_bot.PACING_BEHAVIOR_MIN_DELAY)
        self.assertEqual(plan.memory_injection, "minimal_display_name_only")
        self.assertEqual(plan.batch_behavior, bnl01_bot.BATCH_BEHAVIOR_DO_NOT_BATCH)
        self.assertIn("simple_greeting", plan.batch_bypass_reason)

    def test_direct_mention_creates_paced_direct_plan(self):
        plan = bnl01_bot.plan_conversation_response(
            "BNL what are we doing?",
            "public_context",
            route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
            real_direct_target=True,
            batching_enabled=True,
        )
        self.assertEqual(plan.response_timing, bnl01_bot.RESPONSE_TIMING_PACED_DIRECT)
        self.assertEqual(plan.response_generation, bnl01_bot.RESPONSE_GENERATION_GEMINI_NORMAL_CHAT)
        self.assertEqual(plan.batch_behavior, bnl01_bot.BATCH_BEHAVIOR_PREEMPT_BATCH)

    def test_non_direct_active_message_batches_when_batching_enabled(self):
        plan = bnl01_bot.plan_conversation_response(
            "room chatter continuing",
            "public_context",
            route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
            active_channel=True,
            real_direct_target=False,
            followup_candidate=False,
            batching_enabled=True,
        )
        self.assertEqual(plan.response_timing, bnl01_bot.RESPONSE_TIMING_BATCHED_CHANNEL)
        self.assertEqual(plan.batch_behavior, bnl01_bot.BATCH_BEHAVIOR_ENTER_BATCH)
        self.assertFalse(plan.should_reply)

    def test_non_direct_active_message_silent_when_batching_disabled(self):
        plan = bnl01_bot.plan_conversation_response(
            "room chatter continuing",
            "public_context",
            route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
            active_channel=True,
            real_direct_target=False,
            followup_candidate=False,
            batching_enabled=False,
        )
        self.assertEqual(plan.response_timing, bnl01_bot.RESPONSE_TIMING_SILENT_OBSERVE)
        self.assertEqual(plan.response_generation, bnl01_bot.RESPONSE_GENERATION_NONE)

    def test_operator_and_source_commands_remain_command_routes(self):
        operator_plan = bnl01_bot.plan_conversation_response(
            "!bnl debug last route",
            "sealed_test",
            route_mode=bnl01_bot.ROUTE_MODE_OPERATOR_COMMAND,
            operator_command=True,
        )
        self.assertEqual(operator_plan.response_timing, bnl01_bot.RESPONSE_TIMING_IMMEDIATE_COMMAND)
        self.assertEqual(operator_plan.response_generation, bnl01_bot.RESPONSE_GENERATION_OPERATOR_COMMAND_HANDLER)

        source_plan = bnl01_bot.plan_conversation_response(
            "!bnl source enrich HellcatNZ | dry_run=true",
            "internal_controlled",
            route_mode=bnl01_bot.ROUTE_MODE_SOURCE_ENRICHMENT,
            source_command=True,
        )
        self.assertEqual(source_plan.route_mode, bnl01_bot.ROUTE_MODE_SOURCE_ENRICHMENT)
        self.assertTrue(source_plan.source_context_allowed)

    def test_direct_payload_session_plan_is_deferred(self):
        plan = bnl01_bot.plan_conversation_response(
            "BNL make a list from the next lines",
            "public_context",
            route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
            real_direct_target=True,
            payload_expected=True,
            payload_count=0,
        )
        self.assertEqual(plan.route_mode, bnl01_bot.ROUTE_MODE_DIRECT_PAYLOAD)
        self.assertEqual(plan.response_timing, bnl01_bot.RESPONSE_TIMING_DEFERRED_PAYLOAD_SESSION)
        self.assertTrue(plan.direct_session_used)


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
        for text in ("Records are thin", "one suspicious blinking light", "preliminary analysis", "data stream", "Functioning within optimal parameters", "ambient signal patterns"):
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
        responses = {bnl01_bot.build_simple_greeting_response("6 Bit") for _ in range(20)}
        self.assertGreaterEqual(len(responses), 2)
        for response in responses:
            self.assertIn("6 Bit", response)
            self.assertFalse(bnl01_bot.detect_scripted_mode_leak(response, bnl01_bot.ROUTE_MODE_SIMPLE_GREETING))

    def test_normal_chat_contract_blocks_system_boilerplate(self):
        contract = bnl01_bot.normal_chat_prompt_contract(bnl01_bot.ROUTE_MODE_NORMAL_CHAT)
        self.assertIn("answer naturally", contract.lower())
        self.assertIn("optimal parameters", contract.lower())


if __name__ == "__main__":
    unittest.main()
