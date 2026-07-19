import os
import unittest
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl01_bot


def read_model(queue_production: bool, *, radio_summary: str = "") -> dict:
    source_context = []
    if radio_summary:
        source_context.append({
            "id": "barcode_radio",
            "title": "BARCODE Radio",
            "summary": radio_summary,
        })
    return {
        "ok": True,
        "version": 1,
        "source": "barcode-network-site",
        "publicOnly": True,
        "capabilities": {"queueProduction": queue_production},
        "sections": {
            "sourceContext": source_context,
            "queue": {
                "nowPlaying": {"title": "PRIVATE_TEST_QUEUE_TITLE"},
            },
        },
    }


class ShowdayQueueAlignmentTests(unittest.IsolatedAsyncioTestCase):
    def test_native_announcement_canon_requires_local_and_site_gates(self):
        remote_true = read_model(True)
        remote_false = read_model(False)

        with mock.patch.dict(os.environ, {"BNL_QUEUE_PRODUCTION_ENABLED": "false"}, clear=False):
            self.assertEqual(bnl01_bot.showday_submission_canon(remote_true)["mode"], "public_intake")
        with mock.patch.dict(os.environ, {"BNL_QUEUE_PRODUCTION_ENABLED": "true"}, clear=False):
            self.assertEqual(bnl01_bot.showday_submission_canon(remote_false)["mode"], "public_intake")
            self.assertEqual(bnl01_bot.showday_submission_canon(remote_true)["mode"], "native_queue")

    def test_all_stock_showday_copy_removes_auxchord_and_current_state_overclaims(self):
        stock = [
            *bnl01_bot.SHOWDAY_INTAKE_FALLBACKS["public_intake"],
            *bnl01_bot.SHOWDAY_INTAKE_FALLBACKS["native_queue"],
            *bnl01_bot.SHOWDAY_FALLBACKS["show_live"],
            *bnl01_bot.SHOWDAY_FALLBACKS["sponsor_window"],
        ]
        self.assertTrue(stock)
        self.assertNotIn("auxchord", "\n".join(stock).lower())

        for message in bnl01_bot.SHOWDAY_FALLBACKS["show_live"]:
            self.assertTrue(bnl01_bot._showday_output_matches_canon(message, "show_live"))
        for message in bnl01_bot.SHOWDAY_FALLBACKS["sponsor_window"]:
            self.assertTrue(bnl01_bot._showday_output_matches_canon(message, "sponsor_window"))

        self.assertFalse(bnl01_bot._showday_output_matches_canon(
            "BARCODE Radio is now live and 6 Bit is on-air.",
            "show_live",
        ))
        self.assertFalse(bnl01_bot._showday_output_matches_canon(
            "The BARCODE Radio broadcast is live.",
            "show_live",
        ))
        self.assertFalse(bnl01_bot._showday_output_matches_canon(
            "Sponsor transmissions are due and must process now.",
            "sponsor_window",
        ))
        self.assertFalse(bnl01_bot._showday_output_matches_canon(
            "The commercial break is required and has been called.",
            "sponsor_window",
        ))

    def test_intake_copy_cannot_claim_bnl_operates_submissions(self):
        model = read_model(True)
        with mock.patch.dict(os.environ, {"BNL_QUEUE_PRODUCTION_ENABLED": "true"}, clear=False):
            self.assertFalse(bnl01_bot._showday_output_matches_canon(
                "BNL routes the native queue and accepts submissions.",
                "submissions_open",
                model,
            ))
            self.assertFalse(bnl01_bot._showday_output_matches_canon(
                "BNL manages and controls the native queue.",
                "submissions_open",
                model,
            ))

    def test_public_intake_copy_rejects_all_queue_specific_generated_wording(self):
        model = read_model(False)
        with mock.patch.dict(os.environ, {"BNL_QUEUE_PRODUCTION_ENABLED": "true"}, clear=False):
            self.assertFalse(bnl01_bot._showday_output_matches_canon(
                "Use the BARCODE Radio queue during the Friday intake window.",
                "submissions_open",
                model,
            ))
            self.assertTrue(bnl01_bot._showday_output_matches_canon(
                "Use the current public submission route during the Friday intake window.",
                "submissions_open",
                model,
            ))

    def test_public_site_canon_survives_while_live_queue_values_remain_stripped(self):
        model = read_model(
            True,
            radio_summary="Public submissions enter through the native BARCODE Radio queue.",
        )
        with mock.patch.dict(os.environ, {"BNL_QUEUE_PRODUCTION_ENABLED": "false"}, clear=False):
            context = bnl01_bot.build_bnl_read_model_context(
                model,
                "what does the website say about BARCODE Radio?",
                "public_home",
            )

        self.assertIn("Public site canon:", context)
        self.assertIn("Public submissions enter through the native BARCODE Radio queue.", context)
        self.assertNotIn("PRIVATE_TEST_QUEUE_TITLE", context)
        self.assertNotIn("Now playing:", context)

    def test_unrelated_chat_does_not_fetch_or_inject_queue_context(self):
        with mock.patch.dict(os.environ, {"BNL_QUEUE_PRODUCTION_ENABLED": "true"}, clear=False), \
             mock.patch.object(bnl01_bot, "fetch_bnl_read_model", return_value=read_model(True)) as fetch:
            context = bnl01_bot.maybe_build_bnl_read_model_context(
                "How are you doing today?",
                "public_home",
            )

        self.assertEqual(context, "")
        fetch.assert_not_called()

    async def test_auxchord_generation_is_rejected_for_native_intake(self):
        generated = (
            "Auxchord channels are accepting submissions now.\n"
            "Auxchord is the active Friday route. Submit there while BNL routes the queue."
        )
        with mock.patch.dict(os.environ, {"BNL_QUEUE_PRODUCTION_ENABLED": "true"}, clear=False), \
             mock.patch.object(bnl01_bot, "fetch_bnl_read_model", return_value=read_model(True)), \
             mock.patch.object(bnl01_bot, "get_recent_signal_summary", return_value=""), \
             mock.patch.object(bnl01_bot, "get_gemini_response", new=mock.AsyncMock(return_value=generated)):
            discord_message, website_message = await bnl01_bot.generate_showday_messages(1, "submissions_open")

        combined = f"{discord_message}\n{website_message}".lower()
        self.assertNotIn("auxchord", combined)
        self.assertIn("native", combined)
        self.assertIn("queue", combined)

    async def test_generation_prompts_carry_each_showday_truth_boundary(self):
        prompts = []

        async def capture(prompt, **_kwargs):
            prompts.append(prompt)
            return ""

        with mock.patch.dict(os.environ, {"BNL_QUEUE_PRODUCTION_ENABLED": "true"}, clear=False), \
             mock.patch.object(bnl01_bot, "fetch_bnl_read_model", return_value=read_model(True)), \
             mock.patch.object(bnl01_bot, "get_recent_signal_summary", return_value=""), \
             mock.patch.object(bnl01_bot, "get_gemini_response", new=capture):
            await bnl01_bot.generate_showday_messages(1, "submissions_open")
            await bnl01_bot.generate_showday_messages(1, "show_live")
            await bnl01_bot.generate_showday_messages(1, "sponsor_window")

        self.assertEqual(len(prompts), 3)
        self.assertIn("native BARCODE Radio queue", prompts[0])
        self.assertIn("BNL observes it but does not operate", prompts[0])
        self.assertIn("scheduled broadcast window, not proof of current live state", prompts[1])
        self.assertIn("optional later-show sponsor reminder", prompts[2])
        self.assertIn("timing remains host-controlled", prompts[2])


if __name__ == "__main__":
    unittest.main()
