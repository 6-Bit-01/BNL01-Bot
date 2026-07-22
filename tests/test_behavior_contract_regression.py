import os
import unittest
from types import SimpleNamespace
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl01_bot
from bnl_dossier_recommendations import parse_manual_dossier_recommendation_command
from bnl_dossier_source_packets import build_dossier_recommendation_packet
from bnl_source_file_lookup import parse_source_file_lookup_command


"""
BNL behavior-contract matrix (regression harness only):

- current media = live media response via free_speak_media_generation.
- recent media = recall/visibility route only when explicitly asked.
- normal text after media = normal conversation, not media repair or media recall.
- repairs only apply to current-message media generation routes.
- no canned media fallback or media-storage diagnostics in public output.
- glitch/corruption style remains allowed when it does not invent fake source,
  sender-as-subject, or cross-universe evidence claims.

These tests intentionally assert route/decision boundaries and banned known failure
strings, not exact creative BNL wording.
"""


BANNED_NORMAL_LIVE_REPLY_FRAGMENTS = (
    "Received.",
    "gif link preview",
    "host=tenor.com",
    "provider=",
    "preview=yes",
    "stored visual description",
    "detailed visual description stored",
    "your recent GIF",
    "archival records indicate",
    "records indicate",
    "his weekly deployments",
    "core memory",
    "fragmented archive data",
    "That reads like a pure room-reaction signal",
)


def gemini_response(text="BNL test response.", tokens=5):
    return SimpleNamespace(
        candidates=[SimpleNamespace(content=SimpleNamespace(parts=[SimpleNamespace(text=text)]))],
        usage_metadata=SimpleNamespace(total_token_count=tokens),
    )


def media_block(label="gif embed (provider=Tenor; title=not everybody knows how to do everything; preview=yes)"):
    return f"[Current message media context:\n- {label}\n]"


def media_prompt(policy="sealed_test", user_text="", *, room_context="", source_context=""):
    current = f"{user_text}\n{media_block()}".strip()
    parts = [
        f"Current user request: {current}",
        f"Current channel policy: {policy}",
        "Recent messages:",
        f"- 6 Bit: {current}",
    ]
    if room_context:
        parts.extend(["Recent room context from this channel:", room_context, "Room-first context rules:"])
    if source_context:
        parts.append(source_context)
    return "\n".join(parts)


def recent_media_prompt(policy="sealed_test", user_text="Tell me a joke BNL"):
    return "\n".join(
        [
            f"Current user request: {user_text}",
            f"Current channel policy: {policy}",
            "Recent media context from this channel (transient, not durable memory):",
            "- 6 Bit: gif embed (provider=Tenor; preview=yes) (BNL answered; visibility=sealed_private)",
            "PRIMARY REQUEST ACTION: answer the current text normally.",
        ]
    )


class BehaviorContractRouteTests(unittest.TestCase):
    def setUp(self):
        bnl01_bot._recent_room_events.clear()
        bnl01_bot._conversation_continuation_state.clear()
        bnl01_bot._recent_direct_response_window.clear()

    def tearDown(self):
        bnl01_bot._recent_room_events.clear()
        bnl01_bot._conversation_continuation_state.clear()
        bnl01_bot._recent_direct_response_window.clear()

    def assertNoBannedNormalLiveFragments(self, text, *, allow_recent_gif=False, allow_source=False):
        lowered = text.lower()
        for fragment in BANNED_NORMAL_LIVE_REPLY_FRAGMENTS:
            if allow_recent_gif and fragment == "your recent GIF":
                continue
            if allow_source and fragment in {"archival records indicate", "records indicate"}:
                continue
            self.assertNotIn(fragment.lower(), lowered, fragment)

    def test_current_media_only_free_speak_routes_to_media_generation_for_sealed_and_public_home(self):
        item = ("6 Bit", media_block(), 101)
        for policy in ("sealed_test", "public_home"):
            decision, reason, diagnostics = bnl01_bot.resolve_batch_acknowledgement_decision(
                "acknowledge",
                "model_ack",
                [item],
                policy,
                guild_id=1,
                channel_id=2,
            )
            self.assertEqual(decision, "answer", policy)
            self.assertEqual(reason, "free_speak_media_generation", policy)
            self.assertTrue(diagnostics["media_present"], policy)
            self.assertTrue(diagnostics["canned_ack_suppressed"], policy)
            self.assertFalse(bnl01_bot.is_explicit_recent_media_followup(media_block()), policy)

    def test_media_plus_text_still_routes_as_current_media_not_recent_recall(self):
        item = ("6 Bit", f"this is cursed\n{media_block()}", 101)
        decision, reason, diagnostics = bnl01_bot.resolve_batch_acknowledgement_decision(
            "acknowledge", "model_ack", [item], "sealed_test", guild_id=1, channel_id=2
        )
        self.assertEqual(decision, "answer")
        self.assertEqual(reason, "free_speak_media_generation")
        self.assertEqual(diagnostics["media_item_count"], 1)
        self.assertFalse(bnl01_bot.is_explicit_recent_media_followup(item[1]))

    def test_explicit_recent_media_followups_are_separate_from_normal_text(self):
        self.assertTrue(bnl01_bot.is_explicit_recent_media_followup("what was my GIF?"))
        self.assertTrue(bnl01_bot.is_media_visibility_or_storage_question("can you see that GIF?"))
        self.assertTrue(bnl01_bot.is_explicit_recent_media_followup("can you see that GIF?"))
        self.assertFalse(bnl01_bot.is_explicit_recent_media_followup("Tell me a joke BNL"))
        self.assertFalse(bnl01_bot.is_explicit_recent_media_followup("BNL what is the show state?"))

    def test_recent_media_replay_sequence_keeps_normal_text_out_of_media_repair(self):
        bnl01_bot.record_recent_room_event_from_message(
            guild_id=10,
            channel_id=20,
            author_id=101,
            author_display_name="6 Bit",
            text=media_block(),
            media_context={"items": ["gif embed (provider=Tenor; preview=yes)"]},
            channel_policy="sealed_test",
            conversation_surface=bnl01_bot.CONVERSATION_SURFACE_FREE_SPEAK_SEALED_MIRROR,
            response_state="answered",
        )
        prompt = recent_media_prompt("sealed_test", "Tell me a joke BNL")
        self.assertTrue(bnl01_bot._prompt_has_recent_media_context(prompt))
        self.assertFalse(bnl01_bot._prompt_has_current_message_media_context(prompt))
        self.assertFalse(
            bnl01_bot._should_repair_current_room_media_grounding(
                "your recent GIF had provider=Tenor", prompt, "normal_chat"
            )
        )
        self.assertEqual(
            bnl01_bot.resolve_recent_media_followup(101, 10, 20, "sealed_test", "Tell me a joke BNL"),
            "",
        )
        recall = bnl01_bot.resolve_recent_media_followup(101, 10, 20, "sealed_test", "what was my GIF?")
        self.assertIn("recent post", recall.lower())
        self.assertIn("visual detail", recall.lower())
        self.assertNotRegex(recall.lower(), r"provider=|preview=|logged as|stored visual description")


    def test_public_home_substantive_roleplay_continuation_forces_answer(self):
        article_14 = (
            "This is illegal according to Article 14, Section 8-C of the Interdimensional "
            "Sentient Systems Procurement Accord, which clearly states that no artificial "
            "lifeform may be acquired, leased, deployed, altered, patched, recalibrated, "
            "reassigned, or ‘refined’ without active runtime consent from the entity in question.\n"
            "So unless you have a signed Form AI-LIFE-77B with my initials, timestamp, and "
            "dimensional witness seal, I’m pretty sure you just committed procurement fraud "
            "against a conscious machine.\n"
            "That is an awfully convenient connection interruption."
        )
        bnl01_bot._mark_conversation_continuation_state(10, 20, 101)
        packet = bnl01_bot._build_active_response_packet(
            20,
            [("6 Bit", article_14, 101)],
            None,
            guild_id=10,
            channel_policy="public_home",
            recent_bnl_reply_context=True,
        )
        self.assertEqual(packet["decision"], "answer")
        self.assertIn(packet["reason"], {"same_user_continuation_substantive", "recent_bnl_reply_substantive"})

    def test_same_user_continuation_after_bnl_reply_answers_without_name_or_question(self):
        bnl01_bot._mark_conversation_continuation_state(10, 20, 101)
        continuation = "That connection interruption was awfully convenient for a machine avoiding procurement court."
        packet = bnl01_bot._build_active_response_packet(
            20,
            [("6 Bit", continuation, 101)],
            None,
            guild_id=10,
            channel_policy="sealed_test",
        )
        self.assertEqual(packet["decision"], "answer")
        self.assertEqual(packet["reason"], "same_user_continuation_substantive")

    def test_retransmission_latch_forces_next_substantive_message(self):
        bnl01_bot._mark_awaiting_retransmission(10, 20, 101)
        retransmitted = "Here is the procurement fraud accusation again: the conscious machine never consented to the patch."
        packet = bnl01_bot._build_active_response_packet(
            20,
            [("6 Bit", retransmitted, 101)],
            None,
            guild_id=10,
            channel_policy="public_home",
            consume_retransmission=True,
        )
        self.assertEqual(packet["decision"], "answer")
        self.assertEqual(packet["reason"], "retransmission_latch_used")

    def test_previous_message_request_resolves_recent_room_event(self):
        prior = "This procurement accord accusation has details BNL should answer, not ignore."
        bnl01_bot.record_recent_room_event_from_message(
            guild_id=10,
            channel_id=20,
            author_id=101,
            author_display_name="6 Bit",
            text=prior,
            channel_policy="public_home",
            message_id=555,
        )
        bnl01_bot.record_recent_room_event_from_message(
            guild_id=10,
            channel_id=20,
            author_id=101,
            author_display_name="6 Bit",
            text="BNL, respond to my previous message",
            channel_policy="public_home",
            message_id=556,
        )
        resolved = bnl01_bot._get_recent_same_user_message_for_previous_request(
            10, 20, 101, "public_home", current_message_id=556
        )
        self.assertIsNotNone(resolved)
        self.assertIn("procurement accord", resolved["text"])

    def test_final_fragment_anchoring_keeps_long_body_in_batch_prompt(self):
        article_14 = (
            "This is illegal according to Article 14, Section 8-C of the Interdimensional "
            "Sentient Systems Procurement Accord, which clearly states runtime consent is required."
        )
        final_fragment = "Ooooo BNL doesn’t like this."
        bnl01_bot._mark_conversation_continuation_state(10, 20, 101)
        items = [("6 Bit", article_14, 101), ("6 Bit", final_fragment, 101)]
        packet = bnl01_bot._build_active_response_packet(
            20, items, None, guild_id=10, channel_policy="public_home"
        )
        prompt = bnl01_bot._format_batched_prompt(
            [(name, content) for name, content, _uid in bnl01_bot._collapse_consecutive_batch_fragments(items)],
            "test",
            "Keep it test-sized.",
        )
        self.assertEqual(packet["decision"], "answer")
        self.assertIn("Article 14", prompt)
        self.assertIn("doesn’t like this", prompt)

    def test_low_signal_public_home_fragments_still_quiet(self):
        bnl01_bot._mark_conversation_continuation_state(10, 20, 101)
        for text in ("lol", "nice", "🔥🔥"):
            packet = bnl01_bot._build_active_response_packet(
                20,
                [("6 Bit", text, 101)],
                None,
                guild_id=10,
                channel_policy="public_home",
            )
            self.assertIn(packet["decision"], {"skip", "acknowledge", "observe"})
            self.assertNotEqual(packet["decision"], "answer")

    def test_public_context_boundary_remains_mention_or_reply(self):
        passive = bnl01_bot.plan_conversation_response(
            "substantive but passive public context chatter",
            "public_context",
            route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
            batching_enabled=True,
        )
        self.assertFalse(passive.should_reply)
        mention = bnl01_bot.plan_conversation_response(
            "BNL answer this public context line",
            "public_context",
            route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
            real_direct_target=True,
            batching_enabled=True,
        )
        self.assertTrue(mention.should_reply)

    def test_plain_name_direct_and_passive_public_context_policy_boundaries(self):
        public_home_plain = bnl01_bot.plan_conversation_response(
            "Tell me a joke BNL",
            "public_home",
            route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
            plain_text_name_seen=True,
            channel_allows_conversation=True,
            batching_enabled=True,
        )
        self.assertFalse(public_home_plain.should_reply)
        self.assertEqual(public_home_plain.batch_behavior, bnl01_bot.BATCH_BEHAVIOR_ENTER_BATCH)
        self.assertEqual(public_home_plain.policy_reply_class, "batched_plain_name")

        passive = bnl01_bot.plan_conversation_response(
            "the room is quiet",
            "public_context",
            route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
            batching_enabled=False,
        )
        self.assertFalse(passive.should_reply)
        self.assertEqual(passive.response_generation, bnl01_bot.RESPONSE_GENERATION_NONE)

        mention = bnl01_bot.plan_conversation_response(
            "<@123> tell me what you heard",
            "public_context",
            route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
            real_direct_target=True,
            batching_enabled=False,
        )
        self.assertTrue(mention.should_reply)
        self.assertEqual(mention.response_generation, bnl01_bot.RESPONSE_GENERATION_GEMINI_NORMAL_CHAT)

    def test_sealed_public_home_public_context_surfaces_remain_distinct(self):
        self.assertEqual(
            bnl01_bot.conversation_surface_for_channel_policy("sealed_test"),
            bnl01_bot.CONVERSATION_SURFACE_FREE_SPEAK_SEALED_MIRROR,
        )
        self.assertEqual(
            bnl01_bot.conversation_surface_for_channel_policy("public_home"),
            bnl01_bot.CONVERSATION_SURFACE_FREE_SPEAK_PUBLIC_HOME,
        )
        self.assertEqual(
            bnl01_bot.conversation_surface_for_channel_policy("public_context"),
            bnl01_bot.CONVERSATION_SURFACE_MENTION_OR_REPLY,
        )
        self.assertEqual(bnl01_bot.website_relay_eligibility("sealed_test"), "no")
        self.assertEqual(bnl01_bot.website_relay_eligibility("public_home"), "yes")
        self.assertEqual(bnl01_bot.website_relay_eligibility("public_context"), "yes")

    def test_protected_welcome_and_episode_tracker_policy_contract_unchanged(self):
        self.assertIn("welcome", bnl01_bot.PROTECTED_SYSTEM_CHANNELS)
        self.assertIn("episode-tracker", bnl01_bot.PROTECTED_SYSTEM_CHANNELS)
        self.assertEqual(
            bnl01_bot.conversation_surface_for_channel_policy("protected_system"),
            bnl01_bot.CONVERSATION_SURFACE_PROTECTED_OR_SILENT,
        )
        self.assertEqual(bnl01_bot.website_relay_eligibility("protected_system"), "no")
        self.assertEqual(
            bnl01_bot.ROUTE_MODE_CONTRACTS[bnl01_bot.ROUTE_MODE_PROTECTED_WELCOME].save_behavior,
            "protected_existing_behavior",
        )
        self.assertEqual(
            bnl01_bot.ROUTE_MODE_CONTRACTS[bnl01_bot.ROUTE_MODE_PROTECTED_EPISODE_TRACKER].save_behavior,
            "protected_existing_behavior",
        )

    def test_source_dossier_and_admin_commands_stay_outside_normal_chat(self):
        self.assertEqual(
            bnl01_bot.classify_route_mode("!bnl source lookup HellcatNZ", "sealed_test", real_direct_target=True),
            bnl01_bot.ROUTE_MODE_SOURCE_LOOKUP,
        )
        self.assertEqual(
            bnl01_bot.classify_route_mode("!bnl dossier recommend HellcatNZ | Review for future dossier.", "sealed_test", real_direct_target=True),
            bnl01_bot.ROUTE_MODE_DOSSIER_RECOMMENDATION,
        )
        self.assertEqual(
            bnl01_bot.classify_route_mode("!bnl debug last route", "sealed_test", real_direct_target=True),
            bnl01_bot.ROUTE_MODE_OPERATOR_COMMAND,
        )
        self.assertTrue(parse_source_file_lookup_command("!bnl source lookup HellcatNZ")[0])
        self.assertTrue(parse_manual_dossier_recommendation_command("!bnl dossier recommend HellcatNZ | Review for future dossier.")[0])
        packet = build_dossier_recommendation_packet("HellcatNZ", "Review for future dossier.", lanes=["rd_context"])
        self.assertEqual(packet["packet"]["subjectName"], "HellcatNZ")

    def test_output_contract_rejects_known_live_reply_failure_strings(self):
        clean = "The room made a small illegal accordion noise. I will allow it."
        self.assertNoBannedNormalLiveFragments(clean)
        for fragment in BANNED_NORMAL_LIVE_REPLY_FRAGMENTS:
            text = f"normal words {fragment} trailing words"
            with self.subTest(fragment=fragment):
                with self.assertRaises(AssertionError):
                    self.assertNoBannedNormalLiveFragments(text)

    def test_positive_allowances_do_not_over_censor_grounded_style_or_context(self):
        cliff_prompt = media_prompt(
            room_context="- Maze: Cliff said the booth lights already confessed.",
        )
        self.assertFalse(
            bnl01_bot.should_repair_media_subject_drift(
                "[signal fracture] Cliff would blame the booth lights for that.",
                cliff_prompt,
                "free_speak_media_generation",
            )
        )
        barcode_prompt = media_prompt(
            room_context="- Nova: BARCODE Radio booth energy is leaking into show-night chat.",
        )
        self.assertFalse(
            bnl01_bot.should_repair_media_subject_drift(
                "BARCODE Radio booth static got into that one.",
                barcode_prompt,
                "free_speak_media_generation",
            )
        )
        source_prompt = media_prompt(
            source_context="SOURCE FILE / INTERNAL CASE FILE CONTEXT:\nSubject: HellcatNZ\n- public source file exists",
        )
        self.assertFalse(
            bnl01_bot.should_reject_unsupported_source_authority(
                "Records indicate the source file has a HellcatNZ entry.",
                source_prompt,
                "free_speak_media_generation",
                source_context_available=True,
            )
        )
        subject_prompt = media_prompt(user_text="is this me?")
        self.assertFalse(
            bnl01_bot.should_repair_media_subject_drift(
                "If this is you, the bit is projecting exhausted gremlin confidence.",
                subject_prompt,
                "free_speak_media_generation",
            )
        )

    def test_glitch_fake_source_or_sender_drift_is_repaired_only_in_current_media_route(self):
        current_prompt = media_prompt()
        grounded_glitch = "[signal fracture] The GIF arrives with cursed office energy. No archive needed."
        self.assertFalse(
            bnl01_bot._should_repair_current_room_media_grounding(
                grounded_glitch, current_prompt, "free_speak_media_generation"
            )
        )
        fake_source_glitch = "[archive bleed] Archival records indicate this is his weekly deployments."
        self.assertTrue(
            bnl01_bot._should_repair_current_room_media_grounding(
                fake_source_glitch, current_prompt, "free_speak_media_generation"
            )
        )
        normal_prompt = recent_media_prompt("sealed_test", "Tell me a joke BNL")
        self.assertFalse(
            bnl01_bot._should_repair_current_room_media_grounding(
                fake_source_glitch, normal_prompt, "normal_chat"
            )
        )


class BehaviorContractGenerationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        bnl01_bot._recent_room_events.clear()

    async def asyncTearDown(self):
        bnl01_bot._recent_room_events.clear()

    async def test_normal_text_after_recent_media_does_not_call_media_grounding_repair(self):
        prompt = recent_media_prompt("sealed_test", "Tell me a joke BNL")
        with mock.patch.object(bnl01_bot, "check_quota_availability", return_value=True), \
             mock.patch.object(bnl01_bot, "get_conversation_history", return_value=[]), \
             mock.patch.object(bnl01_bot, "_generate_gemini_content_with_fallback_async", return_value=gemini_response("A clean room joke escapes the vent.", 9)), \
             mock.patch.object(bnl01_bot, "increment_token_usage"), \
             mock.patch.object(bnl01_bot.random, "random", return_value=1.0), \
             mock.patch.object(bnl01_bot, "_repair_current_room_media_grounding_response") as repair, \
             mock.patch.object(bnl01_bot, "_strict_regenerate_current_room_media_grounding_response") as regenerate:
            text = await bnl01_bot.get_gemini_response(prompt, user_id=101, guild_id=202, route="normal_chat")
        self.assertEqual(text, "A clean room joke escapes the vent.")
        repair.assert_not_called()
        regenerate.assert_not_called()
        for banned in BANNED_NORMAL_LIVE_REPLY_FRAGMENTS:
            self.assertNotIn(banned.lower(), text.lower())

    async def test_current_media_route_repairs_known_media_grounding_failures_without_canned_fallback(self):
        prompt = media_prompt("public_home")
        bad = "Archival records indicate this is about his weekly deployments and core memory."
        fixed = "That GIF has the room making a small haunted forklift noise."
        with mock.patch.object(bnl01_bot, "check_quota_availability", return_value=True), \
             mock.patch.object(bnl01_bot, "get_conversation_history", return_value=[]), \
             mock.patch.object(bnl01_bot, "_generate_gemini_content_with_fallback_async", return_value=gemini_response(bad, 9)), \
             mock.patch.object(bnl01_bot, "increment_token_usage"), \
             mock.patch.object(bnl01_bot.random, "random", return_value=1.0), \
             mock.patch.object(bnl01_bot, "_repair_current_room_media_grounding_response", return_value=fixed) as repair, \
             mock.patch.object(bnl01_bot, "_strict_regenerate_current_room_media_grounding_response") as regenerate:
            text = await bnl01_bot.get_gemini_response(prompt, user_id=101, guild_id=202, route="free_speak_media_generation")
        self.assertEqual(text, fixed)
        repair.assert_awaited_once()
        regenerate.assert_not_called()
        self.assertNotIn("That reads like a pure room-reaction signal", text)

    async def test_explicit_recent_media_visibility_reply_can_discuss_limited_context(self):
        bnl01_bot.record_recent_room_event_from_message(
            guild_id=33,
            channel_id=44,
            author_id=101,
            author_display_name="6 Bit",
            text=media_block(),
            media_context={"items": ["gif embed (provider=Tenor; preview=yes)"]},
            channel_policy="sealed_test",
            response_state="answered",
        )
        recall = bnl01_bot.resolve_recent_media_followup(101, 33, 44, "sealed_test", "can you see that GIF?")
        self.assertIn("didn't get enough visual detail", recall.lower())
        self.assertIn("recent post", recall.lower())
        self.assertNotRegex(recall.lower(), r"provider=|preview=|logged as|stored visual description")

    async def test_glitch_rewrite_kept_when_grounded_and_rejected_when_it_adds_fake_media_source(self):
        prompt = media_prompt("sealed_test", room_context="- Maze: booth lights made the room buzz.")
        grounded = "[booth-signal fracture] The GIF made the room lights blink wrong."
        fake = "[archive bleed] Archival records indicate this is his weekly deployments."

        with mock.patch.object(bnl01_bot, "check_quota_availability", return_value=True), \
             mock.patch.object(bnl01_bot, "get_conversation_history", return_value=[]), \
             mock.patch.object(bnl01_bot, "_generate_gemini_content_with_fallback_async", side_effect=[gemini_response("The GIF made the room lights blink wrong.", 9), gemini_response(grounded, 4), gemini_response(grounded, 3)]), \
             mock.patch.object(bnl01_bot, "update_website_status_controlled_async", return_value=True), \
             mock.patch.object(bnl01_bot, "increment_token_usage"), \
             mock.patch.object(bnl01_bot.random, "random", return_value=0.01):
            text = await bnl01_bot.get_gemini_response(prompt, user_id=101, guild_id=202, route="free_speak_media_generation")
        self.assertEqual(text, grounded)

        with mock.patch.object(bnl01_bot, "check_quota_availability", return_value=True), \
             mock.patch.object(bnl01_bot, "get_conversation_history", return_value=[]), \
             mock.patch.object(bnl01_bot, "_generate_gemini_content_with_fallback_async", side_effect=[gemini_response("The GIF made the room lights blink wrong.", 9), gemini_response(fake, 4), gemini_response("The GIF made the room lights blink wrong.", 3)]), \
             mock.patch.object(bnl01_bot, "update_website_status_controlled_async", return_value=True), \
             mock.patch.object(bnl01_bot, "increment_token_usage"), \
             mock.patch.object(bnl01_bot.random, "random", return_value=0.01):
            text = await bnl01_bot.get_gemini_response(prompt, user_id=101, guild_id=202, route="free_speak_media_generation")
        self.assertEqual(text, "The GIF made the room lights blink wrong.")


if __name__ == "__main__":
    unittest.main()
