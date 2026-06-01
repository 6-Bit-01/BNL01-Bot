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

    def test_guard_replaces_leak_with_context_safe_check_in_answer(self):
        response, guard_triggered = bnl01_bot.apply_response_mode_contamination_guard(
            "Records are thin",
            bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
            "6 Bit",
            "Hows it going?",
        )
        self.assertTrue(guard_triggered)
        self.assertIn("Systems are steady", response)
        lowered = response.lower()
        self.assertNotIn("ask me that normally", lowered)
        self.assertNotIn("i’ll keep it conversational", lowered)
        self.assertNotIn("please rephrase", lowered)


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


if __name__ == "__main__":
    unittest.main()
