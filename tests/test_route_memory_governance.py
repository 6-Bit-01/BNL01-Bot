import os
import tempfile
import unittest
from unittest import mock

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

    def test_conversation_surface_mapping_keeps_policy_separate(self):
        cases = {
            "public_home": bnl01_bot.CONVERSATION_SURFACE_FREE_SPEAK_PUBLIC_HOME,
            "sealed_test": bnl01_bot.CONVERSATION_SURFACE_FREE_SPEAK_SEALED_MIRROR,
            "public_context": bnl01_bot.CONVERSATION_SURFACE_MENTION_OR_REPLY,
            "public_selective": bnl01_bot.CONVERSATION_SURFACE_MENTION_OR_REPLY,
            "internal_controlled": bnl01_bot.CONVERSATION_SURFACE_COMMAND_ONLY,
            "broadcast_memory": bnl01_bot.CONVERSATION_SURFACE_COMMAND_ONLY,
            "reference_canon": bnl01_bot.CONVERSATION_SURFACE_PROTECTED_OR_SILENT,
            "protected_system": bnl01_bot.CONVERSATION_SURFACE_PROTECTED_OR_SILENT,
            "ai_image_tool": bnl01_bot.CONVERSATION_SURFACE_PROTECTED_OR_SILENT,
            "unknown": bnl01_bot.CONVERSATION_SURFACE_PROTECTED_OR_SILENT,
        }
        for policy, surface in cases.items():
            self.assertEqual(bnl01_bot.conversation_surface_for_channel_policy(policy), surface, policy)
        self.assertTrue(bnl01_bot.conversation_surface_allows_free_speak(bnl01_bot.conversation_surface_for_channel_policy("public_home")))
        self.assertTrue(bnl01_bot.conversation_surface_allows_free_speak(bnl01_bot.conversation_surface_for_channel_policy("sealed_test")))
        for policy in ("public_context", "internal_controlled", "reference_canon", "protected_system", "unknown"):
            self.assertFalse(bnl01_bot.conversation_surface_allows_free_speak(bnl01_bot.conversation_surface_for_channel_policy(policy)), policy)


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

    def test_simple_greeting_direct_exceptions_stay_paced_direct(self):
        mention = bnl01_bot.plan_conversation_response(
            "Hey BNL",
            "public_home",
            route_mode=bnl01_bot.ROUTE_MODE_SIMPLE_GREETING,
            real_direct_target=True,
            plain_text_name_seen=True,
            batching_enabled=True,
        )
        self.assertTrue(mention.should_reply)
        self.assertEqual(mention.directness, "real_mention")
        self.assertEqual(mention.response_timing, bnl01_bot.RESPONSE_TIMING_PACED_DIRECT)
        self.assertEqual(mention.response_generation, bnl01_bot.RESPONSE_GENERATION_DETERMINISTIC_TEMPLATE)

        reply = bnl01_bot.plan_conversation_response(
            "hey",
            "sealed_test",
            route_mode=bnl01_bot.ROUTE_MODE_SIMPLE_GREETING,
            reply_to_bot=True,
            batching_enabled=True,
        )
        self.assertTrue(reply.should_reply)
        self.assertEqual(reply.directness, "reply_to_bot")
        self.assertEqual(reply.response_timing, bnl01_bot.RESPONSE_TIMING_PACED_DIRECT)
        self.assertEqual(reply.response_generation, bnl01_bot.RESPONSE_GENERATION_DETERMINISTIC_TEMPLATE)

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

    def test_sealed_test_free_speak_replies_when_batching_disabled(self):
        for text, plain_seen in (("Hi BNL", True), ("How's it going?", False), ("BNL?", True)):
            route_mode = bnl01_bot.classify_route_mode(text, "sealed_test", active_channel=True)
            plan = bnl01_bot.plan_conversation_response(
                text,
                "sealed_test",
                route_mode=route_mode,
                active_channel=True,
                plain_text_name_seen=plain_seen,
                channel_allows_conversation=True,
                batching_enabled=False,
            )
            self.assertTrue(plan.should_reply, text)
            self.assertEqual(plan.response_timing, bnl01_bot.RESPONSE_TIMING_PACED_DIRECT)
            self.assertNotEqual(plan.reason, "active_batching_disabled")
            self.assertFalse(plan.batching_disabled_affects_reply)
            self.assertIn(plan.directness, {"plain_name_call", "free_speak"})

    def test_public_home_plain_name_replies_when_batching_disabled(self):
        for text in ("Hi BNL", "BNL how's it going?"):
            route_mode = bnl01_bot.classify_route_mode(text, "public_home", active_channel=True)
            if text == "BNL how's it going?":
                self.assertEqual(route_mode, bnl01_bot.ROUTE_MODE_NORMAL_CHAT)
            plan = bnl01_bot.plan_conversation_response(
                text,
                "public_home",
                route_mode=route_mode,
                active_channel=True,
                plain_text_name_seen=True,
                channel_allows_conversation=True,
                batching_enabled=False,
            )
            self.assertTrue(plan.should_reply, text)
            self.assertEqual(plan.directness, "plain_name_call")
            self.assertEqual(plan.response_timing, bnl01_bot.RESPONSE_TIMING_PACED_DIRECT)
            self.assertNotEqual(plan.reason, "active_batching_disabled")

    def test_free_speak_surfaces_share_batching_disabled_paced_fallback(self):
        for policy in ("public_home", "sealed_test"):
            plan = bnl01_bot.plan_conversation_response(
                "room chatter continuing",
                policy,
                route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                active_channel=False,
                channel_allows_conversation=True,
                batching_enabled=False,
            )
            self.assertTrue(plan.should_reply, policy)
            self.assertFalse(plan.batch_allowed, policy)
            self.assertEqual(plan.response_timing, bnl01_bot.RESPONSE_TIMING_PACED_DIRECT, policy)
            self.assertEqual(plan.response_generation, bnl01_bot.RESPONSE_GENERATION_GEMINI_NORMAL_CHAT, policy)
            self.assertNotEqual(plan.reason, "public_home_passive_observe_batching_disabled", policy)
            self.assertEqual(plan.policy_reply_class, "free_speak_conversation", policy)

    def test_free_speak_surfaces_share_batching_enabled_debounce(self):
        plans = []
        for policy in ("public_home", "sealed_test"):
            plan = bnl01_bot.plan_conversation_response(
                "room chatter continuing",
                policy,
                route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                active_channel=False,
                channel_allows_conversation=True,
                batching_enabled=True,
            )
            plans.append(plan)
            self.assertFalse(plan.should_reply, policy)
            self.assertTrue(plan.batch_allowed, policy)
            self.assertEqual(plan.response_timing, bnl01_bot.RESPONSE_TIMING_BATCHED_CHANNEL, policy)
            self.assertEqual(plan.batch_behavior, bnl01_bot.BATCH_BEHAVIOR_ENTER_BATCH, policy)
        self.assertEqual(plans[0].response_timing, plans[1].response_timing)
        self.assertEqual(plans[0].response_generation, plans[1].response_generation)
        self.assertEqual(plans[0].batch_behavior, plans[1].batch_behavior)

    def test_public_home_and_sealed_test_free_speak_parity_examples(self):
        cases = [
            ("Hey", bnl01_bot.ROUTE_MODE_NORMAL_CHAT, False),
            ("Hey BNL", bnl01_bot.ROUTE_MODE_SIMPLE_GREETING, True),
            ("whats up", bnl01_bot.ROUTE_MODE_NORMAL_CHAT, False),
            ("BNL how’s it going?", bnl01_bot.ROUTE_MODE_NORMAL_CHAT, True),
        ]
        for text, route_mode, plain_seen in cases:
            public_plan = bnl01_bot.plan_conversation_response(
                text,
                "public_home",
                route_mode=route_mode,
                plain_text_name_seen=plain_seen,
                channel_allows_conversation=True,
                batching_enabled=True,
            )
            sealed_plan = bnl01_bot.plan_conversation_response(
                text,
                "sealed_test",
                route_mode=route_mode,
                plain_text_name_seen=plain_seen,
                channel_allows_conversation=True,
                batching_enabled=True,
            )
            self.assertEqual(public_plan.should_reply, sealed_plan.should_reply, text)
            self.assertEqual(public_plan.response_timing, sealed_plan.response_timing, text)
            self.assertEqual(public_plan.response_generation, sealed_plan.response_generation, text)
            self.assertEqual(public_plan.batch_behavior, sealed_plan.batch_behavior, text)

    def test_plain_name_calls_batch_on_both_free_speak_surfaces_when_enabled(self):
        cases = [
            ("Hey BNL", bnl01_bot.ROUTE_MODE_SIMPLE_GREETING),
            ("Hi BNL", bnl01_bot.ROUTE_MODE_SIMPLE_GREETING),
            ("BNL?", bnl01_bot.ROUTE_MODE_SIMPLE_GREETING),
            ("BNL how's it going?", bnl01_bot.ROUTE_MODE_NORMAL_CHAT),
        ]
        for policy in ("public_home", "sealed_test"):
            for text, route_mode in cases:
                plan = bnl01_bot.plan_conversation_response(
                    text,
                    policy,
                    route_mode=route_mode,
                    plain_text_name_seen=True,
                    channel_allows_conversation=True,
                    batching_enabled=True,
                )
                self.assertFalse(plan.should_reply, (policy, text))
                self.assertEqual(plan.directness, "plain_name_call", (policy, text))
                self.assertEqual(plan.policy_reply_class, "batched_plain_name", (policy, text))
                self.assertEqual(plan.response_timing, bnl01_bot.RESPONSE_TIMING_BATCHED_CHANNEL, (policy, text))
                self.assertEqual(plan.batch_behavior, bnl01_bot.BATCH_BEHAVIOR_ENTER_BATCH, (policy, text))
                self.assertEqual(plan.response_generation, bnl01_bot.RESPONSE_GENERATION_GEMINI_NORMAL_CHAT, (policy, text))
                self.assertIn("plain_name_call_entered_batch", plan.reply_reason, (policy, text))

    def test_plain_name_calls_work_on_both_free_speak_surfaces_when_batching_disabled(self):
        for policy in ("public_home", "sealed_test"):
            greeting = bnl01_bot.plan_conversation_response(
                "Hi BNL",
                policy,
                route_mode=bnl01_bot.ROUTE_MODE_SIMPLE_GREETING,
                plain_text_name_seen=True,
                channel_allows_conversation=True,
                batching_enabled=False,
            )
            self.assertTrue(greeting.should_reply, policy)
            self.assertEqual(greeting.directness, "plain_name_call", policy)
            self.assertEqual(greeting.response_timing, bnl01_bot.RESPONSE_TIMING_PACED_DIRECT, policy)
            self.assertEqual(greeting.response_generation, bnl01_bot.RESPONSE_GENERATION_DETERMINISTIC_TEMPLATE, policy)

            normal = bnl01_bot.plan_conversation_response(
                "BNL how's it going?",
                policy,
                route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                plain_text_name_seen=True,
                channel_allows_conversation=True,
                batching_enabled=False,
            )
            self.assertTrue(normal.should_reply, policy)
            self.assertEqual(normal.route_mode, bnl01_bot.ROUTE_MODE_NORMAL_CHAT, policy)
            self.assertEqual(normal.response_timing, bnl01_bot.RESPONSE_TIMING_PACED_DIRECT, policy)
            self.assertEqual(normal.response_generation, bnl01_bot.RESPONSE_GENERATION_GEMINI_NORMAL_CHAT, policy)

    def test_public_context_matrix(self):
        mention = bnl01_bot.plan_conversation_response(
            "actual mention",
            "public_context",
            route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
            real_direct_target=True,
            batching_enabled=False,
        )
        self.assertTrue(mention.should_reply)
        self.assertEqual(mention.directness, "real_mention")

        reply = bnl01_bot.plan_conversation_response(
            "replying back",
            "public_context",
            route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
            real_direct_target=True,
            reply_to_bot=True,
            batching_enabled=False,
        )
        self.assertTrue(reply.should_reply)
        self.assertEqual(reply.directness, "reply_to_bot")

        passive = bnl01_bot.plan_conversation_response(
            "random public chatter",
            "public_context",
            route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
            active_channel=False,
            batching_enabled=False,
        )
        self.assertFalse(passive.should_reply)
        self.assertEqual(passive.response_timing, bnl01_bot.RESPONSE_TIMING_BLOCKED)

        plain_name = bnl01_bot.plan_conversation_response(
            "Hi BNL",
            "public_context",
            route_mode=bnl01_bot.ROUTE_MODE_SIMPLE_GREETING,
            plain_text_name_seen=True,
            batching_enabled=False,
        )
        self.assertFalse(plain_name.should_reply)
        self.assertEqual(plain_name.reason, "public_context_plain_name_call_not_direct")

    def test_public_selective_internal_and_protected_passive_stay_quiet(self):
        for policy in ("public_selective", "internal_controlled", "reference_canon", "unknown"):
            plan = bnl01_bot.plan_conversation_response(
                "passive message",
                policy,
                route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                batching_enabled=False,
            )
            self.assertFalse(plan.should_reply, policy)
            self.assertEqual(plan.response_generation, bnl01_bot.RESPONSE_GENERATION_NONE)

    def test_active_batching_disabled_never_denies_direct_or_allowed_free_speak(self):
        cases = [
            dict(text="mention", policy="public_context", real_direct_target=True),
            dict(text="reply", policy="public_context", real_direct_target=True, reply_to_bot=True),
            dict(text="Hi BNL", policy="public_home", plain_text_name_seen=True, active_channel=True, channel_allows_conversation=True),
            dict(text="How's it going?", policy="sealed_test", active_channel=True, channel_allows_conversation=True),
        ]
        for case in cases:
            plan = bnl01_bot.plan_conversation_response(
                case.pop("text"),
                case.pop("policy"),
                route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                batching_enabled=False,
                **case,
            )
            self.assertTrue(plan.should_reply)
            self.assertNotEqual(plan.reason, "active_batching_disabled")
            self.assertFalse(plan.batching_disabled_affects_reply)

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

    def test_sealed_test_keeps_private_visibility_and_no_public_signals(self):
        decision = bnl01_bot.decide_memory_write_policy(
            bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
            "sealed_test",
            "user",
            "I prefer BARCODE updates in concise bullet points on show days.",
            False,
        )
        self.assertTrue(decision.save_conversation)
        self.assertFalse(decision.write_memory_tier)
        self.assertFalse(decision.record_community_presence)
        self.assertEqual(decision.visibility, "test_only_no_public_relay")
        self.assertEqual(bnl01_bot.website_relay_eligibility("sealed_test"), "no")
        self.assertFalse(bnl01_bot.CHANNEL_POLICY_CONTRACTS["sealed_test"]["presence"])
        self.assertFalse(bnl01_bot.CHANNEL_POLICY_CONTRACTS["sealed_test"]["subject_signals"])

    def test_public_home_memory_behavior_remains_public_home(self):
        decision = bnl01_bot.decide_memory_write_policy(
            bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
            "public_home",
            "user",
            "I prefer BARCODE updates in concise bullet points on show days.",
            False,
        )
        self.assertTrue(decision.save_conversation)
        self.assertTrue(decision.write_memory_tier)
        self.assertTrue(decision.record_community_presence)
        self.assertEqual(decision.visibility, "public_context_allowed")
        self.assertEqual(bnl01_bot.website_relay_eligibility("public_home"), "yes")


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
        self.assertIn("Answer the user's actual message", prompt)
        self.assertIn("Do not expose source-file, dossier, classification, candidate", prompt)
        self.assertIn("unless explicitly asked", prompt)


class LeakGuardTests(unittest.TestCase):
    def test_normal_chat_catches_structural_scripted_language(self):
        leaked_texts = (
            "Records are thin",
            "Lardcode Radio: Records are thin",
            "one suspicious blinking light",
            "preliminary analysis",
            "data stream",
            "source coverage",
            "existing dossier update",
            "public dossier update lane",
            "source-file enrichment",
            "candidate intake",
            "Classification: candidate",
            "source classification: internal",
        )
        for text in leaked_texts:
            self.assertTrue(bnl01_bot.detect_scripted_mode_leak(text, bnl01_bot.ROUTE_MODE_NORMAL_CHAT), text)

    def test_guard_preserves_bnl_voice_phrases(self):
        for text in ("Functioning within optimal parameters", "ambient signal patterns are calm"):
            self.assertFalse(bnl01_bot.detect_scripted_mode_leak(text, bnl01_bot.ROUTE_MODE_NORMAL_CHAT), text)

    def test_guard_does_not_fire_for_ordinary_classification_discussion(self):
        text = "We talked about genre classification in the archive yesterday."
        self.assertFalse(bnl01_bot.detect_scripted_mode_leak(text, bnl01_bot.ROUTE_MODE_NORMAL_CHAT))

    def test_guard_does_not_fire_for_source_mode_or_operator_mode(self):
        self.assertFalse(bnl01_bot.detect_scripted_mode_leak("Classification: candidate", bnl01_bot.ROUTE_MODE_SOURCE_ENRICHMENT))
        self.assertFalse(bnl01_bot.detect_scripted_mode_leak("Records are thin", bnl01_bot.ROUTE_MODE_OPERATOR_COMMAND))

    def test_mode_leak_fallback_is_not_meta_instructional(self):
        fallback = bnl01_bot.safe_fallback_response_for_mode_leak(
            "6 Bit",
            bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
            "How’s it going?",
        )
        lowered = fallback.lower()
        self.assertIn("i’m good", lowered)
        self.assertNotIn("ask me that normally", lowered)
        self.assertNotIn("i’ll keep it conversational", lowered)
        self.assertNotIn("please rephrase", lowered)
        for forbidden in ("guard", "prompt", "mode", "rules"):
            self.assertNotIn(forbidden, lowered)

    def test_guard_suppresses_normal_chat_leak_instead_of_generic_fallback(self):
        response, guard_triggered = bnl01_bot.apply_response_mode_contamination_guard(
            "Records are thin",
            bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
            "Crow",
            "What does this mean for the show?",
        )
        self.assertTrue(guard_triggered)
        self.assertEqual("", response)

    def test_substantive_request_and_generic_non_answer_detection(self):
        self.assertTrue(bnl01_bot.is_substantive_current_request("Crow asks: what happened with BARCODE this week?"))
        self.assertTrue(bnl01_bot.is_generic_non_answer_response("I’m here, Crow. What do you need?", "Crow"))
        self.assertTrue(bnl01_bot.is_generic_non_answer_response("What can I help with?"))
        self.assertFalse(bnl01_bot.is_generic_non_answer_response("Yeah — BARCODE is back this week, but the slot still needs confirmation."))

    def test_media_only_text_is_not_substantive(self):
        text = bnl01_bot.append_media_context_to_text("", {"items": ["gif embed"], "prompt_text": "- gif embed"})
        self.assertFalse(bnl01_bot.is_substantive_current_request(text, has_media=True))


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

    def test_normal_chat_contract_is_positive_and_not_voice_censorship(self):
        contract = bnl01_bot.normal_chat_prompt_contract(bnl01_bot.ROUTE_MODE_NORMAL_CHAT)
        lowered = contract.lower()
        self.assertIn("answer the user's actual message", lowered)
        self.assertIn("keep bnl's voice", lowered)
        self.assertIn("do not expose source-file, dossier, classification, candidate", lowered)
        self.assertIn("unless explicitly asked", lowered)
        self.assertNotIn("optimal parameters", lowered)
        self.assertNotIn("ambient signal patterns", lowered)
        self.assertNotIn("ask me that normally", lowered)

    def test_check_in_fallback_makes_sense_as_answer(self):
        fallback = bnl01_bot.safe_fallback_response_for_mode_leak(
            "6 Bit",
            bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
            "How’s it going?",
        )
        lowered = fallback.lower()
        self.assertIn("i’m good", lowered)
        self.assertIn("systems are steady", lowered)
        self.assertNotIn("ask me that normally", lowered)
        self.assertNotIn("please rephrase", lowered)


class MediaAwareBatchAckTests(unittest.TestCase):
    def _media_items(self, label="gif embed (provider=Tenor; preview=yes)", text=""):
        return [("Crow", bnl01_bot.append_media_context_to_text(text, {"items": [label], "prompt_text": f"- {label}"}), 101)]

    def test_media_only_gif_public_home_resolves_to_generation_not_observe(self):
        items = self._media_items()
        decision, reason = bnl01_bot._classify_batch_engagement(items)
        self.assertEqual(decision, "acknowledge")
        self.assertEqual(bnl01_bot._build_acknowledgement_response(items), "")
        resolved_decision, resolved_reason, diag = bnl01_bot._free_speak_ack_resolution(decision, reason, items, "public_home")
        self.assertEqual(resolved_decision, "answer")
        self.assertEqual(resolved_reason, "free_speak_media_generation")
        self.assertTrue(diag["canned_ack_suppressed"])
        self.assertTrue(diag["ack_escalated_to_generation"])
        self.assertFalse(diag["ack_converted_to_observe"])

    def test_media_only_gif_sealed_test_resolves_to_generation_not_observe(self):
        items = self._media_items()
        resolved_decision, resolved_reason, diag = bnl01_bot._free_speak_ack_resolution("acknowledge", "light_media_reaction_cluster", items, "sealed_test")
        self.assertEqual(resolved_decision, "answer")
        self.assertEqual(resolved_reason, "free_speak_media_generation")
        self.assertTrue(diag["ack_escalated_to_generation"])
        self.assertFalse(diag["ack_converted_to_observe"])

    def test_media_only_image_public_home_resolves_to_generation(self):
        items = self._media_items("image attachment (filename=meme.png; type=image/png)")
        resolved_decision, resolved_reason, diag = bnl01_bot.resolve_batch_acknowledgement_decision(
            "acknowledge", "light_media_reaction_cluster", items, "public_home", guild_id=1, channel_id=2, message_count=1
        )
        self.assertEqual(resolved_decision, "answer")
        self.assertEqual(resolved_reason, "free_speak_media_generation")
        self.assertTrue(diag["media_present"])

    def test_media_only_video_sealed_test_resolves_to_generation(self):
        items = self._media_items("video attachment (filename=signal.mp4; type=video/mp4)")
        resolved_decision, resolved_reason, diag = bnl01_bot.resolve_batch_acknowledgement_decision(
            "acknowledge", "light_media_reaction_cluster", items, "sealed_test", guild_id=1, channel_id=2, message_count=1
        )
        self.assertEqual(resolved_decision, "answer")
        self.assertEqual(resolved_reason, "free_speak_media_generation")
        self.assertEqual(diag["media_item_count"], 1)

    def test_original_acknowledge_media_escalates_with_diagnostics_and_logging(self):
        items = self._media_items()
        with mock.patch.object(bnl01_bot, "_build_acknowledgement_response", wraps=bnl01_bot._build_acknowledgement_response) as ack_builder:
            with mock.patch.object(bnl01_bot, "_log_batch_event") as batch_log:
                resolved_decision, resolved_reason, diag = bnl01_bot.resolve_batch_acknowledgement_decision(
                    "acknowledge",
                    "light_media_reaction_cluster",
                    items,
                    "sealed_test",
                    guild_id=1,
                    channel_id=2,
                    message_count=len(items),
                )
        self.assertEqual(resolved_decision, "answer")
        self.assertEqual(resolved_reason, "free_speak_media_generation")
        self.assertEqual(diag["original_decision"], "acknowledge")
        self.assertEqual(diag["resolved_decision"], "answer")
        self.assertTrue(diag["ack_escalated_to_generation"])
        self.assertFalse(diag["ack_converted_to_observe"])
        ack_builder.assert_not_called()
        logged_events = [call.args[1] for call in batch_log.call_args_list]
        self.assertIn("free_speak_media_resolution", logged_events)
        self.assertIn("free_speak_ack_resolution", logged_events)
        joined_details = " ".join(call.args[5] for call in batch_log.call_args_list)
        self.assertIn("original_decision=acknowledge", joined_details)
        self.assertIn("resolved_decision=answer", joined_details)
        self.assertIn("resolved_reason=free_speak_media_generation", joined_details)
        self.assertIn("ack_escalated_to_generation=1", joined_details)
        self.assertIn("media_present=1", joined_details)
        self.assertIn("media_context_included=1", joined_details)
        self.assertIn("recent_media_context_found=", joined_details)
        self.assertIn("channel_policy=sealed_test", joined_details)

    def test_isolated_free_speak_media_does_not_resolve_to_observe(self):
        items = self._media_items()
        resolved_decision, _resolved_reason, diag = bnl01_bot.resolve_batch_acknowledgement_decision(
            "acknowledge", "light_media_reaction_cluster", items, "public_home", guild_id=1, channel_id=2, message_count=1,
            recent_bnl_reply_context=False,
        )
        self.assertEqual(resolved_decision, "answer")
        self.assertEqual(diag["distinct_user_count"], 1)
        self.assertEqual(diag["batch_item_count"], 1)
        self.assertFalse(diag["recent_bnl_reply_context"])
        self.assertFalse(diag["recent_room_context"])

    def test_text_plus_media_same_batch_answers(self):
        items = self._media_items("gif embed (provider=Tenor; preview=yes)", text="this is the room energy")
        packet = bnl01_bot._build_active_response_packet(123, items, pending_state=False)
        self.assertEqual(packet["decision"], "answer")
        self.assertTrue(packet["media_present"])

    def test_media_after_text_answers(self):
        items = [("Crow", "this is the room energy", 101)] + self._media_items()
        packet = bnl01_bot._build_active_response_packet(123, items, pending_state=False)
        resolved_decision, resolved_reason, _diag = bnl01_bot.resolve_batch_acknowledgement_decision(
            packet["decision"], packet["reason"], packet["collapsed_items"], "public_home", guild_id=1, channel_id=2, message_count=2
        )
        self.assertEqual(resolved_decision, "answer")
        self.assertEqual(resolved_reason, "free_speak_media_generation")

    def test_media_after_bnl_response_answers(self):
        items = self._media_items("gif embed (provider=Tenor; title=Spock Logical; preview=yes)")
        resolved_decision, resolved_reason, diag = bnl01_bot.resolve_batch_acknowledgement_decision(
            "acknowledge",
            "light_media_reaction_cluster",
            items,
            "sealed_test",
            guild_id=1,
            channel_id=2,
            message_count=1,
            recent_bnl_reply_context=True,
        )
        self.assertEqual(resolved_decision, "answer")
        self.assertEqual(resolved_reason, "free_speak_media_generation")
        self.assertTrue(diag["recent_bnl_reply_context"])

    def test_media_context_is_represented_in_batch_prompt_and_packet_metadata(self):
        media_text = bnl01_bot.append_media_context_to_text(
            "BNL what do you make of this?",
            {"items": ["image attachment (filename=meme.png; type=image/png)"], "prompt_text": "- image attachment (filename=meme.png; type=image/png)"},
        )
        items = [("Crow", media_text, 101)]
        packet = bnl01_bot._build_active_response_packet(123, items, pending_state=False)
        prompt = bnl01_bot._format_batched_prompt([("Crow", media_text)], "steady", "")
        self.assertTrue(packet["media_present"])
        self.assertTrue(packet["media_context_included"])
        self.assertGreaterEqual(packet["media_item_count"], 1)
        self.assertIn("Current message media context", prompt)
        self.assertIn("image attachment", prompt)

    def test_direct_mention_with_media_prompt_preserves_media_context(self):
        direct_content = bnl01_bot.append_media_context_to_text(
            "BNL thoughts?",
            {"items": ["gif link preview (host=tenor.com)"], "prompt_text": "- gif link preview (host=tenor.com)"},
        )
        old_db = bnl01_bot.DB_FILE
        with tempfile.NamedTemporaryFile(delete=True) as tmp:
            bnl01_bot.DB_FILE = tmp.name
            bnl01_bot.init_db()
            try:
                prompt, _allow, _style = bnl01_bot.build_user_aware_prompt(
                    1,
                    1,
                    "Crow",
                    direct_content,
                    channel_name="barcode-bot",
                    channel_policy="public_home",
                    route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                )
            finally:
                bnl01_bot.DB_FILE = old_db
        self.assertIn("Current message media context", prompt)
        self.assertIn("Media context rule", prompt)

    def test_resolve_batch_acknowledgement_escalates_structured_free_speak_context(self):
        items = [("Crow", "testing diagnostics with enough detail to require acknowledgement", 101)]
        with mock.patch.object(bnl01_bot, "_log_batch_event") as batch_log:
            resolved_decision, resolved_reason, diag = bnl01_bot.resolve_batch_acknowledgement_decision(
                "acknowledge",
                "light_fragment_or_test_cluster",
                items,
                "public_home",
                has_structured_intent=True,
                guild_id=1,
                channel_id=2,
                message_count=len(items),
            )
        self.assertEqual(resolved_decision, "answer")
        self.assertEqual(resolved_reason, "free_speak_ack_structured_context_generation")
        self.assertTrue(diag["canned_ack_suppressed"])
        self.assertTrue(diag["ack_escalated_to_generation"])
        self.assertFalse(diag["ack_converted_to_observe"])
        batch_log.assert_not_called()

    def test_non_free_speak_command_acknowledgement_still_available(self):
        items = [("Ops", "testing diagnostics with enough detail to require acknowledgement from the utility path", 101), ("Ops", "second testing diagnostic line with enough detail", 101)]
        decision, reason = bnl01_bot._classify_batch_engagement(items)
        self.assertEqual(decision, "acknowledge")
        resolved_decision, _resolved_reason, diag = bnl01_bot._free_speak_ack_resolution(decision, reason, items, "internal_controlled")
        self.assertEqual(resolved_decision, "acknowledge")
        self.assertEqual(bnl01_bot._build_acknowledgement_response(items), "Received.")
        self.assertFalse(diag["canned_ack_suppressed"])

    def test_non_free_speak_media_does_not_become_free_speak(self):
        items = self._media_items()
        for policy in ("public_context", "public_selective", "internal_controlled", "reference", "protected", "unknown"):
            with self.subTest(policy=policy):
                resolved_decision, _reason, diag = bnl01_bot.resolve_batch_acknowledgement_decision(
                    "acknowledge", "light_media_reaction_cluster", items, policy, guild_id=1, channel_id=2, message_count=1
                )
                self.assertEqual(resolved_decision, "acknowledge")
                self.assertFalse(diag["canned_ack_suppressed"])


class RecentMediaRoomContextTests(unittest.TestCase):
    def setUp(self):
        bnl01_bot._recent_room_events.clear()

    def test_media_message_records_short_term_room_event(self):
        event = bnl01_bot.record_recent_media_event(
            guild_id=1,
            channel_id=2,
            author_id=101,
            author_display_name="Crow",
            text=bnl01_bot.append_media_context_to_text("", {"items": ["gif embed (provider=Tenor; title=Spock Logical; preview=yes)"], "prompt_text": "- gif embed (provider=Tenor; title=Spock Logical; preview=yes)"}),
            media_context={"items": ["gif embed (provider=Tenor; title=Spock Logical; preview=yes)"]},
            channel_policy="sealed_test",
            conversation_surface=bnl01_bot.CONVERSATION_SURFACE_FREE_SPEAK_SEALED_MIRROR,
            response_state="observed",
        )
        self.assertTrue(event["media_present"])
        self.assertEqual(event["visibility"], "sealed_private")
        self.assertEqual(len(bnl01_bot.get_recent_room_events(1, 2, channel_policy="sealed_test", media_only=True)), 1)

    def test_recent_media_followup_uses_matching_author_metadata(self):
        bnl01_bot.record_recent_media_event(
            guild_id=1, channel_id=2, author_id=101, author_display_name="Crow",
            text="", media_context={"items": ["gif embed (provider=Tenor; title=Spock Logical; preview=yes)"]},
            channel_policy="public_home", conversation_surface="free_speak_public_home", response_state="observed",
        )
        response = bnl01_bot.resolve_recent_media_followup(202, 1, 2, "public_home", "what did Crow post?")
        self.assertIn("Crow", response)
        self.assertIn("Spock Logical", response)

    def test_recent_media_followup_thin_metadata_is_honest(self):
        bnl01_bot.record_recent_media_event(
            guild_id=1, channel_id=2, author_id=101, author_display_name="Crow",
            text="", media_context={"items": ["gif embed (provider=Tenor; preview=yes)"]},
            channel_policy="public_home", conversation_surface="free_speak_public_home", response_state="observed",
        )
        response = bnl01_bot.resolve_recent_media_followup(101, 1, 2, "public_home", "what was my GIF of?")
        self.assertIn("Tenor", response)
        self.assertIn("do not have a detailed visual description", response)

    def test_public_home_media_context_is_transient_not_durable_memory(self):
        event = bnl01_bot.record_recent_media_event(
            guild_id=1, channel_id=2, author_id=101, author_display_name="Crow",
            text="", media_context={"items": ["image attachment (filename=meme.png; type=image/png)"]},
            channel_policy="public_home", conversation_surface="free_speak_public_home", response_state="observed",
        )
        self.assertEqual(event["visibility"], "public_home_transient")
        self.assertNotIn("durable", event["visibility"])


    def test_bare_media_fallback_detection_and_suppression_without_current_media(self):
        fallback = "I saw your recent gif as gif embed (embed_type=gifv; provider=Tenor; preview=yes); gif link preview (host=tenor.com), but I do not have a detailed visual description stored for that one."
        self.assertTrue(bnl01_bot.is_bare_media_fallback_text(fallback))
        self.assertEqual(
            bnl01_bot.suppress_stale_media_fallback(
                fallback,
                current_text="what is BARCODE Radio doing next?",
                current_has_media=False,
            ),
            "",
        )

    def test_current_media_reference_can_acknowledge_once_but_not_repeat(self):
        fallback = "I saw your recent gif as gif embed (provider=Tenor; preview=yes), but I do not have a detailed visual description stored for that one."
        old_db = bnl01_bot.DB_FILE
        with tempfile.NamedTemporaryFile(delete=True) as tmp:
            bnl01_bot.DB_FILE = tmp.name
            bnl01_bot.init_db()
            try:
                self.assertEqual(
                    bnl01_bot.suppress_stale_media_fallback(
                        fallback,
                        current_text="what was that gif?",
                        current_has_media=False,
                        user_id=101,
                        guild_id=1,
                        channel_id=2,
                    ),
                    fallback,
                )
                with bnl01_bot.sqlite3.connect(bnl01_bot.DB_FILE) as conn:
                    conn.execute(
                        "INSERT INTO conversations (user_id, user_name, guild_id, channel_id, role, content) VALUES (?, ?, ?, ?, ?, ?)",
                        (101, "BNL-01", 1, 2, "model", fallback),
                    )
                self.assertEqual(
                    bnl01_bot.suppress_stale_media_fallback(
                        fallback,
                        current_text="what was that gif?",
                        current_has_media=False,
                        user_id=101,
                        guild_id=1,
                        channel_id=2,
                    ),
                    "",
                )
            finally:
                bnl01_bot.DB_FILE = old_db

    def test_bare_media_fallback_model_rows_excluded_from_history_and_room_context(self):
        fallback = "I saw your recent gif as gif embed (provider=Tenor; preview=yes), but I do not have a detailed visual description stored for that one."
        old_db = bnl01_bot.DB_FILE
        with tempfile.NamedTemporaryFile(delete=True) as tmp:
            bnl01_bot.DB_FILE = tmp.name
            bnl01_bot.init_db()
            try:
                bnl01_bot.save_user_message(101, "Crow", 1, "what is the current signal?", channel_name="general", channel_policy="public_home", channel_id=2)
                bnl01_bot.save_model_message(101, 1, fallback, channel_name="general", channel_policy="public_home", channel_id=2)
                history_text = "\n".join(part for msg in bnl01_bot.get_conversation_history(101, 1) for part in msg.get("parts", []))
                room_rows = bnl01_bot.get_recent_channel_context(1, 2, channel_name="general", channel_policy="public_home")
                self.assertNotIn("recent gif", history_text.lower())
                self.assertFalse(any("recent gif" in row["content"].lower() for row in room_rows))
            finally:
                bnl01_bot.DB_FILE = old_db

    def test_recent_media_prompt_context_requires_current_media_or_reference(self):
        bnl01_bot.record_recent_media_event(
            guild_id=1, channel_id=2, author_id=101, author_display_name="Crow",
            text="", media_context={"items": ["gif embed (provider=Tenor; preview=yes)"]},
            channel_policy="public_home", conversation_surface="free_speak_public_home", response_state="observed",
        )
        self.assertFalse(bnl01_bot.current_batch_references_recent_media("what time is the show?"))
        self.assertTrue(bnl01_bot.current_batch_references_recent_media("what was that gif?"))
        old_db = bnl01_bot.DB_FILE
        with tempfile.NamedTemporaryFile(delete=True) as tmp:
            bnl01_bot.DB_FILE = tmp.name
            bnl01_bot.init_db()
            try:
                no_ref = bnl01_bot.build_room_first_direct_context(1, 2, "general", "public_home", "Crow", current_text="what time is the show?")
                with_ref = bnl01_bot.build_room_first_direct_context(1, 2, "general", "public_home", "Crow", current_text="what was that gif?")
            finally:
                bnl01_bot.DB_FILE = old_db
        self.assertNotIn("Recent media context", no_ref)
        self.assertIn("Recent media context", with_ref)

    def test_repeat_guard_replaces_non_media_duplicate(self):
        old_db = bnl01_bot.DB_FILE
        with tempfile.NamedTemporaryFile(delete=True) as tmp:
            bnl01_bot.DB_FILE = tmp.name
            bnl01_bot.init_db()
            try:
                previous = "The relay is stable; answer the text normally."
                with bnl01_bot.sqlite3.connect(bnl01_bot.DB_FILE) as conn:
                    conn.execute(
                        "INSERT INTO conversations (user_id, user_name, guild_id, channel_id, role, content) VALUES (?, ?, ?, ?, ?, ?)",
                        (101, "BNL-01", 1, 2, "model", previous),
                    )
                replacement = bnl01_bot.suppress_stale_media_fallback(previous, current_text="repeat?", user_id=101, guild_id=1, channel_id=2)
                self.assertNotEqual(replacement, previous)
                self.assertNotIn("gif", replacement.lower())
            finally:
                bnl01_bot.DB_FILE = old_db

    def test_active_media_reaction_after_other_user_text_escalates(self):
        bnl01_bot.record_recent_room_event_from_message(
            guild_id=1, channel_id=2, author_id=202, author_display_name="Six", text="that plan is extremely logical",
            media_context={}, channel_policy="public_home", conversation_surface="free_speak_public_home", response_state="ignored",
        )
        items = [("Crow", bnl01_bot.append_media_context_to_text("", {"items": ["gif embed (provider=Tenor; title=Spock Logical; preview=yes)"], "prompt_text": "- gif embed (provider=Tenor; title=Spock Logical; preview=yes)"}), 101)]
        resolved_decision, resolved_reason, diag = bnl01_bot.resolve_batch_acknowledgement_decision(
            "acknowledge", "light_media_reaction_cluster", items, "public_home", guild_id=1, channel_id=2, message_count=1
        )
        self.assertEqual(resolved_decision, "answer")
        self.assertEqual(resolved_reason, "free_speak_media_generation")
        self.assertTrue(diag["ack_escalated_to_generation"])
        self.assertTrue(diag["recent_room_context"])

    def test_media_reaction_after_recent_bnl_reply_can_escalate_when_meaningful(self):
        items = [("Crow", bnl01_bot.append_media_context_to_text("", {"items": ["gif embed (provider=Tenor; title=Spock Logical; preview=yes)"], "prompt_text": "- gif embed (provider=Tenor; title=Spock Logical; preview=yes)"}), 101)]
        resolved_decision, resolved_reason, diag = bnl01_bot.resolve_batch_acknowledgement_decision(
            "acknowledge",
            "light_media_reaction_cluster",
            items,
            "sealed_test",
            guild_id=1,
            channel_id=2,
            message_count=1,
            recent_bnl_reply_context=True,
        )
        self.assertEqual(resolved_decision, "answer")
        self.assertEqual(resolved_reason, "free_speak_media_generation")
        self.assertTrue(diag["recent_bnl_reply_context"])

    def test_non_free_speak_media_does_not_become_free_speak(self):
        items = [("Crow", bnl01_bot.append_media_context_to_text("", {"items": ["gif embed (provider=Tenor; preview=yes)"], "prompt_text": "- gif embed (provider=Tenor; preview=yes)"}), 101)]
        resolved_decision, _reason, diag = bnl01_bot.resolve_batch_acknowledgement_decision(
            "acknowledge", "light_media_reaction_cluster", items, "public_context", guild_id=1, channel_id=2, message_count=1
        )
        self.assertEqual(resolved_decision, "acknowledge")
        self.assertFalse(diag["canned_ack_suppressed"])


class GuardedResponseRegenerationTests(unittest.IsolatedAsyncioTestCase):
    async def test_mode_leak_substantive_request_regenerates_without_generic_fallback(self):
        async def fake_regen(channel, prompt, user_id, guild_id, route="get_gemini_response"):
            self.assertIn("previous draft tripped the mode-leak guard", prompt)
            return "Crow, the show note points to a normal BARCODE answer instead of an internal classification."

        with mock.patch.object(bnl01_bot, "get_gemini_response_with_optional_typing", side_effect=fake_regen):
            response, diagnostics = await bnl01_bot.apply_guarded_response_regeneration(
                "Classification: candidate",
                prompt="CURRENT USER MESSAGE: What happened with BARCODE?",
                user_id=101,
                guild_id=1,
                route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                channel_policy="public_home",
                directness="real_direct_target",
                user_display_name="Crow",
                current_user_text="What happened with BARCODE?",
            )
        self.assertFalse(diagnostics["suppressed"])
        self.assertTrue(diagnostics["regenerated_for_mode_leak"])
        self.assertNotEqual("I’m here, Crow. What do you need?", response)
        self.assertFalse(bnl01_bot.is_generic_non_answer_response(response, "Crow"))

    async def test_mode_leak_retry_failure_suppresses(self):
        async def fake_regen(channel, prompt, user_id, guild_id, route="get_gemini_response"):
            return "I’m here, Crow. What do you need?"

        with mock.patch.object(bnl01_bot, "get_gemini_response_with_optional_typing", side_effect=fake_regen):
            response, diagnostics = await bnl01_bot.apply_guarded_response_regeneration(
                "Records are thin",
                prompt="CURRENT USER MESSAGE: What happened with BARCODE?",
                user_id=101,
                guild_id=1,
                route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                channel_policy="public_home",
                user_display_name="Crow",
                current_user_text="What happened with BARCODE?",
            )
        self.assertEqual("", response)
        self.assertTrue(diagnostics["suppressed"])
        self.assertEqual("scripted_mode_leak_after_retry", diagnostics["suppression_reason"])

    async def test_generic_non_answer_to_substantive_request_regenerates(self):
        async def fake_regen(channel, prompt, user_id, guild_id, route="get_gemini_response"):
            self.assertIn("failed to answer the current user message", prompt)
            return "The schedule is uncertain; I can only confirm the public room has no locked broadcast status yet."

        with mock.patch.object(bnl01_bot, "get_gemini_response_with_optional_typing", side_effect=fake_regen):
            response, diagnostics = await bnl01_bot.apply_guarded_response_regeneration(
                "I’m here, Crow. What do you need?",
                prompt="CURRENT USER MESSAGE: Is BARCODE back this week?",
                user_id=101,
                guild_id=1,
                route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                channel_policy="public_home",
                user_display_name="Crow",
                current_user_text="Is BARCODE back this week?",
            )
        self.assertFalse(diagnostics["suppressed"])
        self.assertTrue(diagnostics["generic_non_answer_triggered"])
        self.assertFalse(bnl01_bot.is_generic_non_answer_response(response, "Crow"))

    async def test_media_only_generic_response_suppresses(self):
        media_text = bnl01_bot.append_media_context_to_text("", {"items": ["gif embed"], "prompt_text": "- gif embed"})
        response, diagnostics = await bnl01_bot.apply_guarded_response_regeneration(
            "I’m here, Crow. What do you need?",
            prompt="CURRENT USER MESSAGE: [media]",
            user_id=101,
            guild_id=1,
            route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
            channel_policy="public_home",
            user_display_name="Crow",
            current_user_text=media_text,
            has_media=True,
        )
        self.assertEqual("", response)
        self.assertTrue(diagnostics["suppressed"])
        self.assertEqual("media_only_no_text", diagnostics["suppression_reason"])


if __name__ == "__main__":
    unittest.main()

class SealedConversationPersistenceTests(unittest.TestCase):
    def test_sealed_user_and_model_rows_are_conversation_only_and_pairable_once(self):
        import sqlite3
        old_db = bnl01_bot.DB_FILE
        with tempfile.NamedTemporaryFile(delete=True) as tmp:
            bnl01_bot.DB_FILE = tmp.name
            bnl01_bot.init_db()
            try:
                user_decision = bnl01_bot.save_user_message(
                    101, "Crow", 1, "remember this number: 8",
                    channel_name="bnl-testing", channel_policy="sealed_test", channel_id=2,
                    message_id=7001, route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                )
                model_decision = bnl01_bot.save_model_message(
                    101, 1, "You told me to remember 8.",
                    channel_name="bnl-testing", channel_policy="sealed_test", channel_id=2,
                    route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                )
                self.assertTrue(user_decision.save_conversation)
                self.assertTrue(model_decision.save_conversation)
                self.assertFalse(model_decision.write_memory_tier)
                self.assertFalse(model_decision.update_relationship)
                conn = sqlite3.connect(tmp.name)
                cur = conn.cursor()
                cur.execute("SELECT role, channel_policy, channel_id, content FROM conversations ORDER BY id")
                rows = cur.fetchall()
                self.assertEqual(len(rows), 2)
                self.assertEqual(rows[0][0], "user")
                self.assertEqual(rows[1][0], "model")
                self.assertTrue(all(r[1] == "sealed_test" and r[2] == 2 for r in rows))
                for table in ("memory_tiers", "user_memory_facts", "relationship_state", "relationship_journal"):
                    cur.execute(f"SELECT COUNT(*) FROM {table}")
                    self.assertEqual(cur.fetchone()[0], 0, table)
                conn.close()
            finally:
                bnl01_bot.DB_FILE = old_db

    def test_empty_or_excluded_model_response_is_not_saved(self):
        old_db = bnl01_bot.DB_FILE
        with tempfile.NamedTemporaryFile(delete=True) as tmp:
            bnl01_bot.DB_FILE = tmp.name
            bnl01_bot.init_db()
            try:
                decision = bnl01_bot.save_model_message(
                    101, 1, "", channel_name="bnl-testing", channel_policy="sealed_test", channel_id=2
                )
                self.assertFalse(decision.save_conversation)
            finally:
                bnl01_bot.DB_FILE = old_db

class SendThenSaveOrderingTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.old_db = bnl01_bot.DB_FILE
        self.tmp = tempfile.NamedTemporaryFile(delete=True)
        bnl01_bot.DB_FILE = self.tmp.name
        bnl01_bot.init_db()

    def tearDown(self):
        bnl01_bot.DB_FILE = self.old_db
        self.tmp.close()

    def message(self, *, fail_reply=False):
        msg = mock.Mock()
        msg.author = mock.Mock(id=101, display_name="Crow")
        msg.guild = mock.Mock(id=1)
        msg.channel = mock.Mock(id=2, name="bnl-testing")
        msg.content = "what number did i tell you to remember?"
        if fail_reply:
            msg.reply = mock.AsyncMock(side_effect=RuntimeError("discord down"))
        else:
            msg.reply = mock.AsyncMock()
        msg.channel.send = mock.AsyncMock()
        return msg

    def model_count(self):
        import sqlite3
        conn = sqlite3.connect(bnl01_bot.DB_FILE)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM conversations WHERE role='model'")
        count = cur.fetchone()[0]
        conn.close()
        return count

    async def test_successful_send_writes_one_model_row_after_delivery(self):
        msg = self.message()
        await bnl01_bot.send_reply_then_save_model(
            msg, "You told me to remember 8.", user_id=101, guild_id=1,
            channel_name="bnl-testing", channel_policy="sealed_test", channel_id=2,
        )
        msg.reply.assert_awaited_once()
        self.assertEqual(self.model_count(), 1)

    async def test_failed_send_writes_zero_model_rows(self):
        msg = self.message(fail_reply=True)
        with self.assertRaises(RuntimeError):
            await bnl01_bot.send_reply_then_save_model(
                msg, "You told me to remember 8.", user_id=101, guild_id=1,
                channel_name="bnl-testing", channel_policy="sealed_test", channel_id=2,
            )
        self.assertEqual(self.model_count(), 0)

    async def test_multi_chunk_successful_send_writes_one_complete_model_row(self):
        channel = mock.Mock(id=2, name="bnl-testing")
        channel.send = mock.AsyncMock()
        long_response = "chunk " * 900
        await bnl01_bot.send_channel_then_save_model(
            channel, long_response, user_id=101, guild_id=1,
            channel_name="bnl-testing", channel_policy="sealed_test", channel_id=2,
        )
        self.assertGreater(channel.send.await_count, 1)
        self.assertEqual(self.model_count(), 1)
        import sqlite3
        conn = sqlite3.connect(bnl01_bot.DB_FILE)
        cur = conn.cursor()
        cur.execute("SELECT content FROM conversations WHERE role='model'")
        self.assertEqual(cur.fetchone()[0], long_response)
        conn.close()

    async def test_repeated_helper_calls_do_not_double_save_within_one_execution(self):
        msg = self.message()
        await bnl01_bot.send_reply_then_save_model(
            msg, "You told me to remember 8.", user_id=101, guild_id=1,
            channel_name="bnl-testing", channel_policy="sealed_test", channel_id=2,
        )
        self.assertEqual(self.model_count(), 1)

    async def test_sealed_success_forms_pair_and_recall_repairs_without_archive_framing(self):
        import sqlite3
        bnl01_bot.save_user_message(101, "Crow", 1, "remember this number: 8", channel_name="bnl-testing", channel_policy="sealed_test", channel_id=2, route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT)
        msg = self.message()
        await bnl01_bot.send_reply_then_save_model(
            msg, "I tucked 8 into the sealed corner.", user_id=101, guild_id=1,
            channel_name="bnl-testing", channel_policy="sealed_test", channel_id=2,
        )
        rows = bnl01_bot.get_conversation_context_v2_rows(1, limit=20, current_user_id=101, channel_id=2, channel_name="bnl-testing", channel_policy="sealed_test")
        from bnl_conversation_context_v2 import ConversationContextRequest, assemble_conversation_context_v2
        req = ConversationContextRequest(guild_id=1, current_user_id=101, channel_id=2, channel_name="bnl-testing", channel_policy="sealed_test", route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT, current_texts=("what number did i tell you to remember?",), is_direct_target=True)
        res = assemble_conversation_context_v2(rows, req)
        self.assertEqual(res.same_room_paired_turn_count, 1)
        prompt = "Current channel policy: sealed_test\n" + res.rendered_context + "\nCurrent user request: what number did i tell you to remember?\n"
        repaired = bnl01_bot._repair_unsupported_authority_with_conversation_context("Records indicate the number was 8.", prompt)
        self.assertEqual(repaired, "You told me to remember 8.")
        self.assertNotRegex(repaired.lower(), r"archive|database|dossier|records indicate")
        conn = sqlite3.connect(bnl01_bot.DB_FILE); cur = conn.cursor()
        for table in ("memory_tiers", "user_memory_facts", "relationship_state", "relationship_journal"):
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            self.assertEqual(cur.fetchone()[0], 0, table)
        conn.close()

    async def test_sealed_failed_send_leaves_unpaired_user_turn(self):
        bnl01_bot.save_user_message(101, "Crow", 1, "remember this number: 8", channel_name="bnl-testing", channel_policy="sealed_test", channel_id=2, route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT)
        msg = self.message(fail_reply=True)
        with self.assertRaises(RuntimeError):
            await bnl01_bot.send_reply_then_save_model(
                msg, "I tucked 8 into the sealed corner.", user_id=101, guild_id=1,
                channel_name="bnl-testing", channel_policy="sealed_test", channel_id=2,
            )
        rows = bnl01_bot.get_conversation_context_v2_rows(1, limit=20, current_user_id=101, channel_id=2, channel_name="bnl-testing", channel_policy="sealed_test")
        from bnl_conversation_context_v2 import ConversationContextRequest, assemble_conversation_context_v2
        req = ConversationContextRequest(guild_id=1, current_user_id=101, channel_id=2, channel_name="bnl-testing", channel_policy="sealed_test", route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT, current_texts=("what number did i tell you to remember?",), is_direct_target=True)
        res = assemble_conversation_context_v2(rows, req)
        self.assertEqual(res.same_room_paired_turn_count, 0)
        self.assertEqual(self.model_count(), 0)
