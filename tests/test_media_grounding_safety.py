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


if __name__ == "__main__":
    unittest.main()
