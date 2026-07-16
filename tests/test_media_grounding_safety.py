import os
import unittest
from types import SimpleNamespace
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl01_bot


def gemini_response(text="BNL relay response.", tokens=7):
    return SimpleNamespace(
        candidates=[SimpleNamespace(content=SimpleNamespace(parts=[SimpleNamespace(text=text)]))],
        usage_metadata=SimpleNamespace(total_token_count=tokens),
    )


class SourceAuthorityGroundingTests(unittest.TestCase):
    def test_source_authority_phrases_are_detected(self):
        self.assertTrue(bnl01_bot.contains_unsupported_source_authority_claim("Archival records indicate this is fine."))
        self.assertTrue(bnl01_bot.contains_unsupported_source_authority_claim("Records indicate the relay blinked."))
        self.assertTrue(bnl01_bot.contains_unsupported_source_authority_claim("His weekly broadcast deployments prove the point."))

    def test_metaphorical_archive_flavor_is_not_source_authority_claim(self):
        self.assertFalse(bnl01_bot.contains_unsupported_source_authority_claim("That joke belongs in the little archive of cursed buttons."))
        self.assertFalse(bnl01_bot.contains_unsupported_source_authority_claim("The room just filed that one under haunted office supplies."))

    def test_source_authority_requires_source_context(self):
        room_media_prompt = "Current channel policy: sealed_test\nRecent media context from this channel (transient, not durable memory):\n- 6 Bit: gif embed (BNL observed)"
        self.assertTrue(
            bnl01_bot.should_reject_unsupported_source_authority(
                "Archival records indicate the poster does this weekly.", room_media_prompt, "free_speak_media_generation"
            )
        )

    def test_broadcast_memory_context_allows_broadcast_memory_authority(self):
        prompt = "Broadcast memory context:\n- BARCODE Radio notes say the show is paused.\nUser message: what does memory say?"
        self.assertFalse(
            bnl01_bot.should_reject_unsupported_source_authority(
                "Broadcast memory indicates the show is paused.", prompt, "normal_chat"
            )
        )

    def test_source_context_allows_source_file_authority(self):
        prompt = "SOURCE FILE / INTERNAL CASE FILE CONTEXT:\nSubject: HellcatNZ\nUser message: summarize this source"
        self.assertFalse(
            bnl01_bot.should_reject_unsupported_source_authority(
                "The source file indicates HellcatNZ is associated with the packet.", prompt, "normal_chat"
            )
        )

    def test_show_state_context_allows_status_source_phrasing(self):
        prompt = "Current BARCODE Radio scheduling context:\nEpisode status: paused\nUser message: is the show live?"
        self.assertFalse(
            bnl01_bot.should_reject_unsupported_source_authority(
                "Records show the show is paused tonight.", prompt, "show_state_status"
            )
        )


class SubjectAndTopicDriftTests(unittest.TestCase):
    def media_only_prompt(self):
        return (
            "Current user request: [Current message media context:\n- gif embed (provider=Tenor; title=not everybody knows how to do everything; preview=yes)\n]\n"
            "Current channel policy: sealed_test\n"
            "Recent messages:\n- 6 Bit: [Current message media context:\n- gif embed (provider=Tenor; title=not everybody knows how to do everything; preview=yes)\n]\n"
        )

    def test_media_only_post_does_not_support_poster_broadcast_biography(self):
        prompt = self.media_only_prompt()
        text = "Archival records indicate this is about his weekly broadcast deployments."
        self.assertTrue(bnl01_bot.contains_unsupported_subject_attribution(text, prompt))
        self.assertTrue(bnl01_bot.should_repair_media_subject_drift(text, prompt, "free_speak_media_generation"))

    def test_random_meme_does_not_support_barcode_radio_topic_jump(self):
        prompt = self.media_only_prompt()
        self.assertTrue(
            bnl01_bot.should_repair_media_subject_drift(
                "BARCODE Radio clearly has a show-schedule problem here.", prompt, "free_speak_media_generation"
            )
        )

    def test_explicit_barcode_or_6bit_question_allows_relevant_reference(self):
        prompt = "Current user request: Is 6 Bit handling BARCODE Radio tonight?\n[Current message media context:\n- gif embed (provider=Tenor)\n]\nUser message: Is 6 Bit handling BARCODE Radio tonight?"
        self.assertFalse(
            bnl01_bot.should_repair_media_subject_drift(
                "6 Bit and BARCODE Radio are the topic you asked about, so the broadcast reference is relevant.",
                prompt,
                "free_speak_media_generation",
            )
        )


    def test_recent_room_context_cliff_allows_natural_cliff_riff(self):
        prompt = (
            self.media_only_prompt()
            + "Recent room context from this channel:\n"
            + "- Maze: Cliff already blamed the booth lights for that one.\n"
            + "Room-first context rules:\n"
        )
        self.assertTrue(bnl01_bot._prompt_has_barcode_topic_basis(prompt))
        self.assertFalse(
            bnl01_bot.should_repair_media_subject_drift(
                "Cliff would call that a booth light with commitment issues.",
                prompt,
                "free_speak_media_generation",
            )
        )

    def test_recent_room_context_barcode_show_booth_allows_natural_radio_riff(self):
        prompt = (
            self.media_only_prompt()
            + "Recent room context from this channel:\n"
            + "- Nova: BARCODE Radio show-night booth energy is already leaking through the queue.\n"
            + "Room-first context rules:\n"
        )
        self.assertTrue(bnl01_bot._prompt_has_barcode_topic_basis(prompt))
        self.assertFalse(
            bnl01_bot.should_repair_media_subject_drift(
                "That has BARCODE Radio booth energy: one blinking light, three mods pretending it is fine.",
                prompt,
                "free_speak_media_generation",
            )
        )

    def test_media_only_gif_without_nearby_barcode_context_repairs_random_show_jump(self):
        prompt = self.media_only_prompt()
        self.assertFalse(bnl01_bot._prompt_has_barcode_topic_basis(prompt))
        self.assertTrue(
            bnl01_bot.should_repair_media_subject_drift(
                "That is obviously BARCODE Radio show-night booth energy.",
                prompt,
                "free_speak_media_generation",
            )
        )

    def test_recent_media_author_name_alone_does_not_create_barcode_topic_basis(self):
        prompt = (
            self.media_only_prompt()
            + "Recent media context from this channel (transient, not durable memory):\n"
            + "- 6 Bit: gif embed (provider=Tenor; preview=yes) (BNL observed; visibility=transient)\n"
        )
        self.assertFalse(bnl01_bot._prompt_has_barcode_topic_basis(prompt))
        self.assertTrue(
            bnl01_bot.should_repair_media_subject_drift(
                "BARCODE Radio clearly has a show-schedule problem here.",
                prompt,
                "free_speak_media_generation",
            )
        )

    def test_source_evidence_plus_poster_deployment_report_still_repairs(self):
        prompt = self.media_only_prompt()
        text = "Archival records indicate his weekly broadcast deployments explain the GIF."
        self.assertTrue(bnl01_bot.should_reject_unsupported_source_authority(text, prompt, "free_speak_media_generation"))
        self.assertTrue(bnl01_bot.should_repair_media_subject_drift(text, prompt, "free_speak_media_generation"))

    def test_natural_barcode_radio_joke_allowed_when_room_context_supports_it(self):
        prompt = (
            self.media_only_prompt()
            + "Recent room context from this channel:\n"
            + "- Operator: The BARCODE Radio booth and the mods are already arguing with the queue.\n"
            + "Room-first context rules:\n"
        )
        text = "That is BARCODE Radio booth energy: the queue coughed once and the mods heard a prophecy."
        self.assertFalse(bnl01_bot.should_repair_media_subject_drift(text, prompt, "free_speak_media_generation"))
        self.assertFalse(bnl01_bot.should_reject_unsupported_source_authority(text, prompt, "free_speak_media_generation"))


    def test_media_only_sender_emotional_resonance_subject_drift_repairs(self):
        prompt = self.media_only_prompt()
        text = "It is unusual for him to project such emotional resonance from a GIF."
        self.assertTrue(bnl01_bot.contains_unsupported_subject_attribution(text, prompt))
        self.assertTrue(bnl01_bot.should_repair_media_subject_drift(text, prompt, "free_speak_media_generation"))

    def test_media_only_core_memory_weekly_deployments_repairs(self):
        prompt = self.media_only_prompt()
        text = "Perhaps a core memory from before his weekly deployments."
        self.assertTrue(bnl01_bot.contains_unsupported_subject_attribution(text, prompt))
        self.assertTrue(bnl01_bot.contains_unsupported_media_grounding_basis(text))
        self.assertTrue(bnl01_bot.should_reject_unsupported_source_authority(text, prompt, "free_speak_media_generation"))
        self.assertTrue(bnl01_bot.should_repair_media_subject_drift(text, prompt, "free_speak_media_generation"))

    def test_media_only_fragmented_archive_data_explanation_repairs_without_source_context(self):
        prompt = self.media_only_prompt()
        text = "That GIF is probably fragmented archive data trying to explain itself."
        self.assertTrue(bnl01_bot.contains_unsupported_media_grounding_basis(text))
        self.assertTrue(bnl01_bot.should_reject_unsupported_source_authority(text, prompt, "free_speak_media_generation"))

    def test_source_context_allows_media_grounding_basis_language(self):
        prompt = self.media_only_prompt() + "Broadcast memory context:\n- A source note explicitly discusses fragmented archive data.\n"
        text = "The fragmented archive data matches the supplied source note."
        self.assertTrue(bnl01_bot.contains_unsupported_media_grounding_basis(text))
        self.assertFalse(bnl01_bot.should_reject_unsupported_source_authority(text, prompt, "free_speak_media_generation"))

    def test_explicit_user_subject_question_allows_pronoun_subject_framing(self):
        prompt = (
            "Current user request: is this me?\n"
            "[Current message media context:\n- gif embed (provider=Tenor)\n]\n"
            "User message: is this me?"
        )
        text = "It might be about him if he is asking whether the GIF describes him."
        self.assertTrue(bnl01_bot.prompt_has_subject_basis(prompt))
        self.assertFalse(bnl01_bot.should_repair_media_subject_drift(text, prompt, "free_speak_media_generation"))

    def test_batched_media_prompt_includes_grounding_rules(self):
        prompt = bnl01_bot._format_batched_prompt(
            [("6 Bit", "[Current message media context:\n- gif embed (provider=Tenor)\n]")], "steady", ""
        )
        self.assertIn("Do not assume the poster is the subject of a meme", prompt)
        self.assertIn("Do not turn a meme into an archive/source report", prompt)


class MediaGroundingRepairTests(unittest.IsolatedAsyncioTestCase):
    async def test_free_speak_media_source_authority_is_repaired_not_dry_refusal(self):
        prompt = (
            "Current channel policy: sealed_test\n"
            "Current user request: [Current message media context:\n- gif embed (title=not everybody knows how to do everything)\n]\n"
            "User message: [Current message media context:\n- gif embed (title=not everybody knows how to do everything)\n]"
        )
        responses = [
            gemini_response("Archival records indicate this is about his weekly broadcast deployments.", 10),
            gemini_response("Not everybody knows how to do everything. The Network can respect that without turning it into a case file.", 8),
        ]

        async def fake_generate(_contents, _route):
            return responses.pop(0)

        with mock.patch.object(bnl01_bot, "check_quota_availability", return_value=True), \
             mock.patch.object(bnl01_bot, "get_conversation_history", return_value=[]), \
             mock.patch.object(bnl01_bot, "_generate_gemini_content_with_fallback_async", side_effect=fake_generate) as generate, \
             mock.patch.object(bnl01_bot, "increment_token_usage"), \
             mock.patch.object(bnl01_bot.random, "random", return_value=1.0):
            text = await bnl01_bot.get_gemini_response(prompt, user_id=123, guild_id=456, route="free_speak_media_generation")

        self.assertEqual(text, "Not everybody knows how to do everything. The Network can respect that without turning it into a case file.")
        self.assertNotIn("I don’t have enough current room context", text)
        self.assertEqual([call.args[1] for call in generate.await_args_list], ["free_speak_media_generation", "media_response_grounding_repair"])

    async def test_free_speak_media_subject_drift_is_repaired(self):
        prompt = (
            "Current channel policy: sealed_test\n"
            "Current user request: [Current message media context:\n- gif embed (title=not everybody knows how to do everything)\n]\n"
        )
        responses = [
            gemini_response("That is his weekly broadcast deployment energy.", 10),
            gemini_response("That is pure ‘not everybody knows how to do everything’ energy. The panel blinks; the room understands.", 8),
        ]

        async def fake_generate(_contents, _route):
            return responses.pop(0)

        with mock.patch.object(bnl01_bot, "check_quota_availability", return_value=True), \
             mock.patch.object(bnl01_bot, "get_conversation_history", return_value=[]), \
             mock.patch.object(bnl01_bot, "_generate_gemini_content_with_fallback_async", side_effect=fake_generate), \
             mock.patch.object(bnl01_bot, "increment_token_usage"), \
             mock.patch.object(bnl01_bot.random, "random", return_value=1.0):
            text = await bnl01_bot.get_gemini_response(prompt, user_id=123, guild_id=456, route="free_speak_media_generation")

        self.assertIn("not everybody knows how to do everything", text.lower())
        self.assertNotIn("weekly broadcast", text.lower())

    def recent_media_only_prompt(self, request="Tell me a joke BNL"):
        return (
            "Current channel policy: sealed_test\n"
            f"Current user request: {request}\n"
            f"User message: {request}\n"
            "Recent media context from this channel (transient, not durable memory):\n"
            "- 6 Bit: gif embed (provider=Tenor; preview=yes) (BNL observed; visibility=transient)\n"
        )

    async def test_normal_text_after_recent_media_does_not_run_media_repair_or_canned_fallback(self):
        prompt = self.recent_media_only_prompt()
        generated = "Perhaps a core memory from before his weekly deployments. Anyway, the joke relay coughed."

        async def fake_generate(_contents, _route):
            return gemini_response(generated, 10)

        with mock.patch.object(bnl01_bot, "check_quota_availability", return_value=True), \
             mock.patch.object(bnl01_bot, "get_conversation_history", return_value=[]), \
             mock.patch.object(bnl01_bot, "_generate_gemini_content_with_fallback_async", side_effect=fake_generate) as generate, \
             mock.patch.object(bnl01_bot, "increment_token_usage"), \
             mock.patch.object(bnl01_bot.random, "random", return_value=1.0):
            text = await bnl01_bot.get_gemini_response(prompt, user_id=123, guild_id=456, route="get_gemini_response")

        self.assertEqual(text, generated)
        self.assertNotIn("pure room-reaction signal", text.lower())
        self.assertEqual([call.args[1] for call in generate.await_args_list], ["get_gemini_response"])

    def test_normal_text_route_without_current_media_does_not_need_current_media_repair(self):
        prompt = self.recent_media_only_prompt("Tell me a joke BNL")
        self.assertFalse(
            bnl01_bot._should_repair_current_room_media_grounding(
                "Perhaps a core memory from before his weekly deployments.",
                prompt,
                "get_gemini_response",
            )
        )

    async def test_current_media_repair_failure_uses_strict_regeneration_when_valid(self):
        prompt = (
            "Current channel policy: sealed_test\n"
            "Current user request: [Current message media context:\n- gif embed (title=confused office)\n]\n"
        )
        responses = [
            gemini_response("Archival records indicate this is about his weekly broadcast deployments.", 10),
            gemini_response("Still his weekly deployments inside the records.", 8),
            gemini_response("That GIF walks in wearing a confused office badge. The red light approves; no dossier required.", 8),
        ]

        async def fake_generate(_contents, _route):
            return responses.pop(0)

        with mock.patch.object(bnl01_bot, "check_quota_availability", return_value=True), \
             mock.patch.object(bnl01_bot, "get_conversation_history", return_value=[]), \
             mock.patch.object(bnl01_bot, "_generate_gemini_content_with_fallback_async", side_effect=fake_generate) as generate, \
             mock.patch.object(bnl01_bot, "increment_token_usage"), \
             mock.patch.object(bnl01_bot.random, "random", return_value=1.0):
            text = await bnl01_bot.get_gemini_response(prompt, user_id=123, guild_id=456, route="free_speak_media_generation")

        self.assertEqual(text, "That GIF walks in wearing a confused office badge. The red light approves; no dossier required.")
        self.assertNotIn("pure room-reaction signal", text.lower())
        self.assertEqual(
            [call.args[1] for call in generate.await_args_list],
            ["free_speak_media_generation", "media_response_grounding_repair", "media_grounding_strict_regeneration"],
        )

    async def test_current_media_repair_and_strict_regeneration_failure_suppresses_response(self):
        prompt = (
            "Current channel policy: sealed_test\n"
            "Current user request: [Current message media context:\n- gif embed (title=confused office)\n]\n"
        )
        responses = [
            gemini_response("Archival records indicate this is about his weekly broadcast deployments.", 10),
            gemini_response("Still his weekly deployments inside the records.", 8),
            gemini_response("The core memory confirms his weekly deployments.", 8),
        ]

        async def fake_generate(_contents, _route):
            return responses.pop(0)

        with mock.patch.object(bnl01_bot, "check_quota_availability", return_value=True), \
             mock.patch.object(bnl01_bot, "get_conversation_history", return_value=[]), \
             mock.patch.object(bnl01_bot, "_generate_gemini_content_with_fallback_async", side_effect=fake_generate) as generate, \
             mock.patch.object(bnl01_bot, "increment_token_usage"), \
             mock.patch.object(bnl01_bot.random, "random", return_value=1.0):
            text = await bnl01_bot.get_gemini_response(prompt, user_id=123, guild_id=456, route="free_speak_media_generation")

        self.assertEqual(text, "")
        self.assertEqual(
            [call.args[1] for call in generate.await_args_list],
            ["free_speak_media_generation", "media_response_grounding_repair", "media_grounding_strict_regeneration"],
        )

    def test_canned_media_fallback_string_is_not_publicly_returned(self):
        banned_prefix = "That reads like a pure room-reaction signal"
        self.assertNotIn(banned_prefix, bnl01_bot._safe_current_room_media_grounding_response("Current channel policy: sealed_test"))

    def test_glitch_cross_universe_output_is_allowed_when_grounded_and_repaired_when_not(self):
        prompt = (
            "Current channel policy: sealed_test\n"
            "Current user request: [Current message media context:\n- gif embed (title=confused office)\n]\n"
        )
        self.assertFalse(
            bnl01_bot.should_repair_media_subject_drift(
                "The red panel hiccups sideways and the room receives one cursed office memo.",
                prompt,
                "free_speak_media_generation",
            )
        )
        self.assertTrue(
            bnl01_bot.should_reject_unsupported_source_authority(
                "host=tenor.com says his weekly deployments are a core memory in the fake records.",
                prompt,
                "free_speak_media_generation",
            )
        )


    async def test_free_speak_media_core_memory_deployments_regression_is_repaired(self):
        prompt = (
            "Current channel policy: sealed_test\n"
            "Current user request: [Current message media context:\n- gif embed (title=not everybody knows how to do everything)\n]\n"
        )
        responses = [
            gemini_response("Perhaps a core memory from before his weekly deployments.", 10),
            gemini_response("That is the GIF doing its tiny public-service announcement. No case file required.", 8),
        ]

        async def fake_generate(_contents, _route):
            return responses.pop(0)

        with mock.patch.object(bnl01_bot, "check_quota_availability", return_value=True), \
             mock.patch.object(bnl01_bot, "get_conversation_history", return_value=[]), \
             mock.patch.object(bnl01_bot, "_generate_gemini_content_with_fallback_async", side_effect=fake_generate), \
             mock.patch.object(bnl01_bot, "increment_token_usage"), \
             mock.patch.object(bnl01_bot.random, "random", return_value=1.0):
            text = await bnl01_bot.get_gemini_response(prompt, user_id=123, guild_id=456, route="free_speak_media_generation")

        self.assertEqual(text, "That is the GIF doing its tiny public-service announcement. No case file required.")
        self.assertNotIn("core memory", text.lower())
        self.assertNotIn("weekly deployments", text.lower())

    async def test_cross_universe_bleed_rejects_sender_subject_drift(self):
        prompt = (
            "Current channel policy: sealed_test\n"
            "Current user request: [Current message media context:\n- gif embed (title=not everybody knows how to do everything)\n]\n"
        )
        responses = [
            gemini_response("That lands like a cursed office-training plaque.", 10),
            gemini_response("That lands like a cursed office-training plaque, maybe from his weekly deployments.", 8),
        ]

        async def fake_generate(_contents, _route):
            return responses.pop(0)

        with mock.patch.object(bnl01_bot, "check_quota_availability", return_value=True), \
             mock.patch.object(bnl01_bot, "get_conversation_history", return_value=[]), \
             mock.patch.object(bnl01_bot, "_generate_gemini_content_with_fallback_async", side_effect=fake_generate) as generate, \
             mock.patch.object(bnl01_bot, "increment_token_usage"), \
             mock.patch.object(bnl01_bot.random, "random", side_effect=[1.0, 0.0]):
            text = await bnl01_bot.get_gemini_response(prompt, user_id=123, guild_id=456, route="free_speak_media_generation")

        self.assertEqual(text, "That lands like a cursed office-training plaque.")
        self.assertNotIn("weekly deployments", text.lower())
        self.assertEqual([call.args[1] for call in generate.await_args_list], ["free_speak_media_generation", "cross_universe_bleed"])

    async def test_glitch_rewrite_rejects_added_source_authority(self):
        responses = [
            gemini_response("That lands like a cursed office-training plaque.", 10),
            gemini_response("Archival records indicate that lands like a cursed office-training plaque.", 8),
        ]

        async def fake_generate(_contents, _route):
            return responses.pop(0)

        with mock.patch.object(bnl01_bot, "check_quota_availability", return_value=True), \
             mock.patch.object(bnl01_bot, "get_conversation_history", return_value=[]), \
             mock.patch.object(bnl01_bot, "_generate_gemini_content_with_fallback_async", side_effect=fake_generate), \
             mock.patch.object(bnl01_bot, "update_website_status_controlled_async", return_value=True), \
             mock.patch.object(bnl01_bot, "increment_token_usage"), \
             mock.patch.object(bnl01_bot.random, "random", side_effect=[0.01, 1.0]):
            text = await bnl01_bot.get_gemini_response("hello", user_id=0, guild_id=456, route="normal_chat")

        self.assertEqual(text, "That lands like a cursed office-training plaque.")


class MediaMemoryRecallLeakTests(unittest.IsolatedAsyncioTestCase):
    def live_media_prompt(self, text=""):
        request = (text + "\n" if text else "") + "[Current message media context:\n- gif link preview (host=tenor.com)\n]"
        return (
            "Current channel policy: sealed_test\n"
            f"Current user request: {request}\n"
            f"User message: {request}"
        )

    async def _run_with_generated(self, prompt, generated, repaired="That lands like confusion entering the room with a tiny red clipboard."):
        responses = [gemini_response(generated, 10), gemini_response(repaired, 8)]

        async def fake_generate(_contents, _route):
            return responses.pop(0)

        with mock.patch.object(bnl01_bot, "check_quota_availability", return_value=True), \
             mock.patch.object(bnl01_bot, "get_conversation_history", return_value=[]), \
             mock.patch.object(bnl01_bot, "_generate_gemini_content_with_fallback_async", side_effect=fake_generate) as generate, \
             mock.patch.object(bnl01_bot, "increment_token_usage"), \
             mock.patch.object(bnl01_bot.random, "random", return_value=1.0):
            text = await bnl01_bot.get_gemini_response(prompt, user_id=123, guild_id=456, route="free_speak_media_generation")
        return text, generate

    def test_explicit_recent_media_followup_detector_ignores_media_only_post(self):
        self.assertFalse(bnl01_bot.is_explicit_recent_media_followup("[Current message media context:\n- gif link preview (host=tenor.com)\n]"))
        self.assertFalse(bnl01_bot.is_explicit_recent_media_followup("done to us?\n[Current message media context:\n- gif embed (provider=Tenor)\n]"))
        self.assertTrue(bnl01_bot.is_explicit_recent_media_followup("what was my GIF?"))
        self.assertTrue(bnl01_bot.is_explicit_recent_media_followup("what did Crow post?"))
        self.assertTrue(bnl01_bot.is_media_visibility_or_storage_question("can you see that GIF?"))
        self.assertTrue(bnl01_bot.is_media_visibility_or_storage_question("what do you have stored for that media?"))

    async def test_live_media_recent_gif_storage_language_is_repaired(self):
        prompt = self.live_media_prompt()
        generated = "I saw your recent gif as gif link preview (host=tenor.com), but I do not have a detailed visual description stored for that one."
        text, generate = await self._run_with_generated(prompt, generated)
        self.assertEqual(text, "That lands like confusion entering the room with a tiny red clipboard.")
        self.assertNotIn("recent gif", text.lower())
        self.assertNotIn("link preview", text.lower())
        self.assertEqual([call.args[1] for call in generate.await_args_list], ["free_speak_media_generation", "media_response_grounding_repair"])

    async def test_live_media_provider_host_storage_variants_are_repaired(self):
        prompt = self.live_media_prompt()
        generated = "That link preview is provider=Tenor, host=tenor.com, and the visual description is stored for that one only as preview=yes."
        text, _generate = await self._run_with_generated(prompt, generated, "Thin signal, but it walks in like the room just tripped over a reaction wire.")
        lowered = text.lower()
        self.assertNotIn("provider=", lowered)
        self.assertNotIn("host=", lowered)
        self.assertNotIn("stored for that one", lowered)

    async def test_live_media_thin_metadata_repair_stays_conversational(self):
        prompt = self.live_media_prompt()
        generated = "I saw your recent gif as gif link preview (host=tenor.com), but I do not have a detailed visual description stored for that one."
        repaired = "I can’t pull much detail from that one, but the reaction itself lands like confusion entering the room."
        text, _generate = await self._run_with_generated(prompt, generated, repaired)
        self.assertEqual(text, repaired)
        self.assertNotIn("host=", text.lower())
        self.assertNotIn("stored", text.lower())

    async def test_explicit_followup_allows_limited_stored_context_language(self):
        prompt = self.live_media_prompt("what was my GIF?")
        generated = "I saw your recent gif as gif link preview (host=tenor.com), but I do not have a detailed visual description stored for that one."

        async def fake_generate(_contents, _route):
            return gemini_response(generated, 10)

        with mock.patch.object(bnl01_bot, "check_quota_availability", return_value=True), \
             mock.patch.object(bnl01_bot, "get_conversation_history", return_value=[]), \
             mock.patch.object(bnl01_bot, "_generate_gemini_content_with_fallback_async", side_effect=fake_generate) as generate, \
             mock.patch.object(bnl01_bot, "increment_token_usage"), \
             mock.patch.object(bnl01_bot.random, "random", return_value=1.0):
            text = await bnl01_bot.get_gemini_response(prompt, user_id=123, guild_id=456, route="free_speak_media_generation")

        self.assertEqual(text, generated)
        self.assertEqual([call.args[1] for call in generate.await_args_list], ["free_speak_media_generation"])

    async def test_explicit_visibility_question_allows_limited_context_language(self):
        prompt = self.live_media_prompt("can you see that GIF?")
        generated = "I can see limited media context for that GIF, but the stored visual description is thin."

        async def fake_generate(_contents, _route):
            return gemini_response(generated, 10)

        with mock.patch.object(bnl01_bot, "check_quota_availability", return_value=True), \
             mock.patch.object(bnl01_bot, "get_conversation_history", return_value=[]), \
             mock.patch.object(bnl01_bot, "_generate_gemini_content_with_fallback_async", side_effect=fake_generate) as generate, \
             mock.patch.object(bnl01_bot, "increment_token_usage"), \
             mock.patch.object(bnl01_bot.random, "random", return_value=1.0):
            text = await bnl01_bot.get_gemini_response(prompt, user_id=123, guild_id=456, route="free_speak_media_generation")

        self.assertEqual(text, generated)
        self.assertEqual([call.args[1] for call in generate.await_args_list], ["free_speak_media_generation"])

    async def test_current_media_plus_normal_text_does_not_allow_diagnostic_leak(self):
        prompt = self.live_media_prompt("done to us?")
        generated = "I saw your recent gif as gif link preview (host=tenor.com), but I do not have a detailed visual description stored for that one."
        text, _generate = await self._run_with_generated(prompt, generated, "Done to us? Probably. The room has entered its small, blinking evidence phase.")
        self.assertIn("done to us", text.lower())
        self.assertNotIn("recent gif", text.lower())



if __name__ == "__main__":
    unittest.main()

class ConversationContinuityGroundingTests(unittest.TestCase):
    def v2_prompt(self):
        return (
            "Current channel policy: sealed_test\n"
            "Conversation continuity (bounded; continuity-only, not canon/current-state evidence):\n"
            "- Prior BNL replies here are conversational continuity only. They do not prove canon, live show state, queue state, dossiers, payments, Priority, Wheel, or third-party facts.\n"
            "User/member: remember this number: 8\n"
            "BNL-01: Locked in, tiny cursed digit and all.\n"
            "Current user request: what number did i tell you to remember?\n"
        )

    def test_conversation_continuity_does_not_count_as_source_authority(self):
        prompt = self.v2_prompt()
        self.assertTrue(bnl01_bot.prompt_has_conversation_continuity_context(prompt))
        self.assertTrue(
            bnl01_bot.should_reject_unsupported_source_authority(
                "Records indicate the number was 8.", prompt, "get_gemini_response"
            )
        )

    def test_grounded_records_claim_repairs_to_direct_contextual_answer(self):
        prompt = self.v2_prompt()
        repaired = bnl01_bot._repair_unsupported_authority_with_conversation_context(
            "Records indicate the number was 8.", prompt, "get_gemini_response"
        )
        self.assertEqual(repaired, "You told me to remember 8.")
        self.assertFalse(bnl01_bot.contains_unsupported_source_authority_claim(repaired))

    def test_unsupported_records_claim_without_context_remains_rejected(self):
        prompt = "Current channel policy: sealed_test\nCurrent user request: what number?\n"
        self.assertEqual(
            bnl01_bot._repair_unsupported_authority_with_conversation_context(
                "Records indicate the number was 8.", prompt, "get_gemini_response"
            ),
            "",
        )
        self.assertTrue(
            bnl01_bot.should_reject_unsupported_source_authority(
                "Records indicate the number was 8.", prompt, "get_gemini_response"
            )
        )

    def test_v2_safe_fallback_extracts_number_without_archive_framing(self):
        fallback = bnl01_bot._safe_uncertain_response_from_prompt(self.v2_prompt())
        self.assertIn("8", fallback)
        self.assertNotRegex(fallback.lower(), r"archive|database|dossier|records indicate")

    def test_legacy_and_broadcast_fallbacks_still_work(self):
        legacy = bnl01_bot._safe_uncertain_response_from_prompt(
            "Current channel policy: sealed_test\nRecent room context from this channel:\n- Maze: Crow said hello.\nRoom-first context rules:\n"
        )
        self.assertIn("current room context", legacy)
        broadcast = bnl01_bot._safe_uncertain_response_from_prompt(
            "Broadcast memory context (cleaned summaries only):\n- Show paused.\nBroadcast-memory usage guidance:\n"
        )
        self.assertIn("broadcast-memory context", broadcast)

class ConversationContinuityRepairSafetyTests(unittest.TestCase):
    def prompt(self, user_line="User/member: remember this number: 8", model_line="BNL-01: Sure, 8 is in the little sealed box."):
        return (
            "Current channel policy: sealed_test\n"
            "Conversation continuity (bounded; continuity-only, not canon/current-state evidence):\n"
            f"{user_line}\n"
            f"{model_line}\n"
            "Current user request: what number did i tell you to remember?\n"
        )

    def test_unrelated_generic_claim_is_not_stripped_and_returned(self):
        repaired = bnl01_bot._repair_unsupported_authority_with_conversation_context(
            "Records indicate Pluto is made of cheese.", self.prompt(), "get_gemini_response"
        )
        self.assertEqual(repaired, "")

    def test_absent_person_fact_or_number_cannot_survive_stripping(self):
        for candidate in (
            "Records indicate Maze is the mayor of the moon.",
            "Records indicate the number was 42.",
            "Records indicate Crow owns a haunted queue slot.",
        ):
            with self.subTest(candidate=candidate):
                self.assertEqual(
                    bnl01_bot._repair_unsupported_authority_with_conversation_context(candidate, self.prompt(), "get_gemini_response"),
                    "",
                )

    def test_different_candidate_number_than_user_continuity_is_rejected(self):
        self.assertEqual(
            bnl01_bot._repair_unsupported_authority_with_conversation_context(
                "Records indicate the number was 9.", self.prompt(), "get_gemini_response"
            ),
            "",
        )

    def test_model_authored_text_alone_cannot_establish_user_fact(self):
        prompt = self.prompt(user_line="User/member: what was it?", model_line="BNL-01: You told me 8 earlier.")
        self.assertEqual(
            bnl01_bot._repair_unsupported_authority_with_conversation_context(
                "Records indicate the number was 8.", prompt, "get_gemini_response"
            ),
            "",
        )
