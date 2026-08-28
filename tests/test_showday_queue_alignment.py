import os
import json
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


def private_read_model() -> dict:
    model = read_model(True)
    model["publicOnly"] = False
    model["accessScope"] = "private"
    model["sections"]["sourceContext"] = [{
        "id": "public_site_summary",
        "title": "Public Site Summary",
        "summary": "PUBLIC_SITE_CONTEXT_REMAINS_AVAILABLE",
    }]
    model["sections"]["queue"] = {
        "available": True,
        "accessScope": "private",
        "nowPlaying": {"title": "PRIVATE_TEST_QUEUE_TITLE"},
        "queue": [{"title": "PRIVATE_SIMULATION_TRACK", "isSimulation": True}],
    }
    model["sections"]["archive"] = {
        "available": True,
        "latestTitle": "PRIVATE_TEST_ARCHIVE_TITLE",
    }
    return model


class ShowdayQueueAlignmentTests(unittest.IsolatedAsyncioTestCase):
    def test_authenticated_private_read_model_is_accepted_and_sends_existing_api_key(self):
        payload = private_read_model()
        captured = {}

        class Response:
            status = 200

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def getcode(self):
                return 200

            def read(self):
                return json.dumps(payload).encode("utf-8")

        def open_request(request, timeout):
            captured["request"] = request
            captured["timeout"] = timeout
            return Response()

        with mock.patch.object(bnl01_bot, "BNL_READ_MODEL_ENABLED", True), \
             mock.patch.object(bnl01_bot, "BNL_READ_MODEL_URL", "https://example.test/api/bnl/read-model"), \
             mock.patch.object(bnl01_bot, "BNL_API_KEY", "shared-bnl-key"), \
             mock.patch.object(bnl01_bot.urllib.request, "urlopen", side_effect=open_request), \
             mock.patch.object(bnl01_bot, "_bnl_read_model_cache", None), \
             mock.patch.object(bnl01_bot, "_bnl_read_model_cached_at", None):
            result = bnl01_bot.fetch_bnl_read_model(force=True)

        self.assertEqual(result["accessScope"], "private")
        self.assertEqual(captured["request"].get_header("X-api-key"), "shared-bnl-key")
        self.assertEqual(captured["timeout"], 3)

    def test_private_read_model_is_rejected_without_service_key(self):
        payload = private_read_model()

        class Response:
            status = 200

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def getcode(self):
                return 200

            def read(self):
                return json.dumps(payload).encode("utf-8")

        with mock.patch.object(bnl01_bot, "BNL_READ_MODEL_ENABLED", True), \
             mock.patch.object(bnl01_bot, "BNL_READ_MODEL_URL", "https://example.test/api/bnl/read-model"), \
             mock.patch.object(bnl01_bot, "BNL_API_KEY", ""), \
             mock.patch.object(bnl01_bot.urllib.request, "urlopen", return_value=Response()), \
             mock.patch.object(bnl01_bot, "_bnl_read_model_cache", None), \
             mock.patch.object(bnl01_bot, "_bnl_read_model_cached_at", None):
            result = bnl01_bot.fetch_bnl_read_model(force=True)

        self.assertEqual(result, {})

    def test_private_queue_data_is_visible_only_in_private_test_and_operator_channels(self):
        model = private_read_model()
        with mock.patch.dict(os.environ, {"BNL_QUEUE_PRODUCTION_ENABLED": "true"}, clear=False):
            public_contexts = [
                bnl01_bot.build_bnl_read_model_context(
                    model,
                    "what is playing right now?",
                    policy,
                )
                for policy in (
                    "public_home",
                    "public_context",
                    "public_selective",
                    "broadcast_memory",
                )
            ]
            sealed_context = bnl01_bot.build_bnl_read_model_context(
                model,
                "what is playing right now?",
                "sealed_test",
            )
            operator_context = bnl01_bot.build_bnl_read_model_context(
                model,
                "what is playing right now?",
                "internal_controlled",
            )

        for public_context in public_contexts:
            self.assertNotIn("PRIVATE_TEST_QUEUE_TITLE", public_context)
            self.assertNotIn("PRIVATE_SIMULATION_TRACK", public_context)
            self.assertNotIn("PRIVATE_TEST_ARCHIVE_TITLE", public_context)
            self.assertIn("PUBLIC_SITE_CONTEXT_REMAINS_AVAILABLE", public_context)
        self.assertIn("PRIVATE_TEST_QUEUE_TITLE", sealed_context)
        self.assertIn("PRIVATE_SIMULATION_TRACK", sealed_context)
        self.assertIn("PRIVATE_TEST_QUEUE_TITLE", operator_context)
        self.assertIn("private", operator_context.lower())
        self.assertIn(
            "Answer queue-related questions from any participant who can speak here",
            sealed_context,
        )
        self.assertNotIn("private owner/admin test", sealed_context)
        self.assertIn("Do not use this queue data in public output", operator_context)

    def test_private_queue_cannot_drive_public_current_state_or_showday_output(self):
        model = private_read_model()
        with mock.patch.dict(os.environ, {"BNL_QUEUE_PRODUCTION_ENABLED": "true"}, clear=False), \
             mock.patch.object(bnl01_bot, "fetch_bnl_read_model", return_value=model):
            public_context = bnl01_bot.maybe_build_bnl_read_model_context(
                "what is playing right now?",
                "public_home",
            )
            operator_context = bnl01_bot.maybe_build_bnl_read_model_context(
                "what is playing right now?",
                "internal_controlled",
            )
            intake = bnl01_bot.showday_submission_canon(model)

        self.assertNotIn("PRIVATE_TEST_QUEUE_TITLE", public_context)
        self.assertNotIn("PRIVATE_SIMULATION_TRACK", public_context)
        self.assertNotIn("PRIVATE_TEST_ARCHIVE_TITLE", public_context)
        self.assertIn("PUBLIC_SITE_CONTEXT_REMAINS_AVAILABLE", public_context)
        self.assertIn("PRIVATE_TEST_QUEUE_TITLE", operator_context)
        self.assertEqual(intake["mode"], "public_intake")

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

    def test_current_queue_owner_fails_closed_until_both_production_gates_are_ready(self):
        questions = (
            "is the Barcode Radio queue open right now?",
            "What's the status of the Barcode Radio queue?",
            "What is the current state of the Barcode Radio queue?",
            "Is Barcode Radio accepting submissions?",
            "Can I submit a track right now?",
            "Is the intake open for tracks?",
        )
        for question in questions:
            with self.subTest(question=question), mock.patch.dict(
                os.environ,
                {"BNL_QUEUE_PRODUCTION_ENABLED": "false"},
                clear=False,
            ), mock.patch.object(
                bnl01_bot,
                "fetch_bnl_read_model",
                return_value=read_model(True),
            ):
                local_off = bnl01_bot.maybe_build_bnl_read_model_context(
                    question,
                    "public_home",
                )
            with mock.patch.dict(
                os.environ,
                {"BNL_QUEUE_PRODUCTION_ENABLED": "true"},
                clear=False,
            ), mock.patch.object(
                bnl01_bot,
                "fetch_bnl_read_model",
                return_value=read_model(False),
            ):
                site_off = bnl01_bot.maybe_build_bnl_read_model_context(
                    question,
                    "public_home",
                )

            self.assertTrue(bnl01_bot._current_queue_state_query(question))
            self.assertEqual(local_off, "")
            self.assertEqual(site_off, "")
            self.assertNotIn("auxchord", (local_off + site_off).lower())

    def test_current_queue_owner_uses_only_gated_native_read_model_context(self):
        questions = (
            "is the Barcode Radio queue open right now?",
            "What's the status of the Barcode Radio queue?",
            "What is the current state of the Barcode Radio queue?",
            "Is Barcode Radio accepting submissions?",
            "Can I submit a track right now?",
            "Is the intake open for tracks?",
        )
        for question in questions:
            with self.subTest(question=question), mock.patch.dict(
                os.environ,
                {"BNL_QUEUE_PRODUCTION_ENABLED": "true"},
                clear=False,
            ), mock.patch.object(
                bnl01_bot,
                "fetch_bnl_read_model",
                return_value=read_model(True),
            ):
                context = bnl01_bot.maybe_build_bnl_read_model_context(
                    question,
                    "public_home",
                )

            self.assertIn("Queue:", context)
            self.assertIn("PRIVATE_TEST_QUEUE_TITLE", context)
            self.assertNotIn("auxchord", context.lower())

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
