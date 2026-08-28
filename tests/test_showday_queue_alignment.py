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
                "queueUrl": "https://www.barcode-network.com/queue",
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
        "queueUrl": "https://www.barcode-network.com/queue",
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
        self.assertEqual(captured["request"].get_header("Cache-control"), "no-cache")
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
                    "is PRIVATE_SIMULATION_TRACK in the queue?",
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
                "is PRIVATE_SIMULATION_TRACK in the queue?",
                "sealed_test",
            )
            operator_context = bnl01_bot.build_bnl_read_model_context(
                model,
                "is PRIVATE_SIMULATION_TRACK in the queue?",
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

    def test_private_queue_context_exposes_rehearsal_position_duration_and_empty_now_playing(self):
        model = private_read_model()
        model["sections"]["queue"] = {
            "available": True,
            "accessScope": "private",
            "session": {
                "title": "Friend rehearsal",
                "status": "open",
                "queueOpen": True,
                "broadcastPhase": "submission_window",
            },
            "status": {"activeCount": 3, "capacity": 44, "pressure": "low"},
            "nowPlaying": None,
            "upNext": {
                "submittedArtistName": "6 Bit",
                "submittedSongTitle": "Node Link Established",
                "lane": "regular",
                "sourceType": "upload",
                "queuePosition": 1,
                "durationLabel": "3:11",
                "durationIsEstimate": False,
            },
            "queue": [
                {
                    "submittedArtistName": "LostMarbles",
                    "submittedSongTitle": "Still Me",
                    "lane": "regular",
                    "sourceType": "youtube",
                    "queuePosition": 2,
                    "durationLabel": "est. 4:02",
                    "durationIsEstimate": True,
                },
                {
                    "submittedArtistName": "WittyF0x",
                    "submittedSongTitle": "The Man Inside the Tree",
                    "lane": "regular",
                    "sourceType": "youtube",
                    "queuePosition": 3,
                    "detectedDurationSeconds": 219,
                    "durationIsEstimate": False,
                },
            ],
        }

        with mock.patch.dict(
            os.environ,
            {"BNL_QUEUE_PRODUCTION_ENABLED": "true"},
            clear=False,
        ):
            context = bnl01_bot.build_bnl_read_model_context(
                model,
                "How many left before my song?",
                "sealed_test",
            )

        self.assertIn("Now playing: none", context)
        self.assertIn("6 Bit — Node Link Established", context)
        self.assertIn("queuePosition=1", context)
        self.assertIn("duration=3:11", context)
        self.assertIn("LostMarbles — Still Me", context)
        self.assertIn("queuePosition=2", context)
        self.assertIn("duration=est. 4:02", context)
        self.assertIn("duration=3:39", context)
        self.assertIn("position N has N-1 queued tracks ahead", context)
        self.assertIn("visible display name", context)
        self.assertIn("Never claim the lineup is hidden", context)

        crowded_model = json.loads(json.dumps(model))
        crowded_model["sections"]["sourceContext"] = [
            {
                "id": f"source-{index}",
                "title": f"Source {index}",
                "summary": "Public source context",
            }
            for index in range(6)
        ]
        crowded_model["sections"]["queue"]["queue"] = [
            {
                "submittedArtistName": f"Queued Artist {index}",
                "submittedSongTitle": f"Queued Track {index}",
                "queuePosition": index + 2,
                "durationLabel": "3:00",
            }
            for index in range(8)
        ]
        crowded_model["sections"]["queue"]["completed"] = [
            {
                "submittedArtistName": f"Completed Artist {index}",
                "submittedSongTitle": f"Completed Track {index}",
            }
            for index in range(5)
        ]
        crowded_model["sections"]["artists"] = {
            "items": [
                {"name": f"Artist {index}", "tracks": [f"Track {index}"]}
                for index in range(8)
            ]
        }
        crowded_model["sections"]["dossiers"] = {
            "items": [
                {"name": f"Dossier {index}", "summary": "Public summary"}
                for index in range(8)
            ]
        }
        crowded_model["sections"]["operatorLanes"] = {
            "doNotStore": [f"Guardrail {index}" for index in range(5)]
        }
        with mock.patch.dict(
            os.environ,
            {"BNL_QUEUE_PRODUCTION_ENABLED": "true"},
            clear=False,
        ):
            crowded_context = bnl01_bot.build_bnl_read_model_context(
                crowded_model,
                "queue status and list public dossiers",
                "sealed_test",
            )
        self.assertLessEqual(len(crowded_context.splitlines()), 80)
        self.assertIn("Do not write, merge, promote, or persist", crowded_context)
        self.assertIn("Never claim the lineup is hidden", crowded_context)

    def test_natural_now_playing_and_wheel_phrases_activate_queue_context(self):
        phrases = (
            "whats playing right now",
            "what song is on",
            "what are we listening to",
            "what did you pull up",
            "what's pulled up in now playing",
            "whats next",
            "who won the wheel",
        )
        for phrase in phrases:
            with self.subTest(phrase=phrase):
                self.assertTrue(bnl01_bot._queue_read_model_query(phrase))

    def test_complete_queue_is_searched_beyond_position_eight_without_dumping_it(self):
        model = private_read_model()
        model["sections"]["queue"] = {
            "available": True,
            "accessScope": "private",
            "queueUrl": "https://www.barcode-network.com/queue",
            "revision": 88,
            "session": {
                "title": "Deep queue rehearsal",
                "status": "open",
                "queueOpen": True,
                "broadcastPhase": "broadcast_active",
                "wheelSpinsOwed": 1,
            },
            "status": {"activeCount": 44, "capacity": 44, "pressure": "max"},
            "nowPlaying": {
                "submittedArtistName": "Loaded Artist",
                "submittedSongTitle": "Loaded Track",
            },
            "upNext": {
                "submittedArtistName": "Ordered Artist 1",
                "submittedSongTitle": "Ordered Track 1",
                "queuePosition": 1,
            },
            "queue": [
                {
                    "submittedArtistName": f"Ordered Artist {position}",
                    "submittedSongTitle": f"Ordered Track {position}",
                    "queuePosition": position,
                    "lane": "regular",
                }
                for position in range(2, 45)
            ],
        }

        with mock.patch.dict(
            os.environ,
            {"BNL_QUEUE_PRODUCTION_ENABLED": "true"},
            clear=False,
        ):
            lookup = bnl01_bot.build_bnl_read_model_context(
                model,
                "Where is Ordered Artist 44 in the queue?",
                "sealed_test",
            )
            full_lineup = bnl01_bot.build_bnl_read_model_context(
                model,
                "Show me the whole queue",
                "sealed_test",
            )
            natural_full_lineup = bnl01_bot.build_bnl_read_model_context(
                model,
                "What tracks are queued?",
                "sealed_test",
            )

        self.assertIn("Ordered Artist 44 — Ordered Track 44", lookup)
        self.assertIn("queuePosition=44", lookup)
        self.assertNotIn("Ordered Artist 8 — Ordered Track 8", lookup)
        self.assertIn("Full-lineup request", full_lineup)
        self.assertIn("https://www.barcode-network.com/queue", full_lineup)
        self.assertNotIn("Ordered Artist 44 — Ordered Track 44", full_lineup)
        self.assertIn("Full-lineup request", natural_full_lineup)
        self.assertIn("https://www.barcode-network.com/queue", natural_full_lineup)
        self.assertNotIn("Ordered Artist 44 — Ordered Track 44", natural_full_lineup)

    def test_operational_queue_questions_force_a_fresh_snapshot_and_keep_answers_focused(self):
        model = private_read_model()
        model["sections"]["queue"] = {
            "available": True,
            "accessScope": "private",
            "queueUrl": "https://www.barcode-network.com/queue",
            "session": {"title": "Live test", "status": "open", "queueOpen": True},
            "status": {"activeCount": 2, "capacity": 44, "pressure": "low"},
            "nowPlaying": {
                "submittedArtistName": "Current Artist",
                "submittedSongTitle": "Current Track",
            },
            "upNext": {
                "submittedArtistName": "Next Artist",
                "submittedSongTitle": "Next Track",
                "queuePosition": 1,
            },
            "queue": [{
                "submittedArtistName": "Waiting Artist",
                "submittedSongTitle": "Waiting Track",
                "queuePosition": 2,
            }],
        }

        with mock.patch.dict(
            os.environ,
            {"BNL_QUEUE_PRODUCTION_ENABLED": "true"},
            clear=False,
        ), mock.patch.object(
            bnl01_bot,
            "fetch_bnl_read_model",
            return_value=model,
        ) as fetch:
            context = bnl01_bot.maybe_build_bnl_read_model_context(
                "whats playing right now",
                "sealed_test",
            )

        fetch.assert_called_once_with(force=True)
        self.assertIn("Current Artist — Current Track", context)
        self.assertIn("Now-playing request", context)
        self.assertNotIn("Waiting Artist — Waiting Track", context)
        self.assertIn("cannot move tracks", context)
        self.assertIn("Never deflect", context)

    def test_wheel_question_uses_confirmed_winner_bound_to_current_position(self):
        model = private_read_model()
        model["sections"]["queue"] = {
            "available": True,
            "accessScope": "private",
            "queueUrl": "https://www.barcode-network.com/queue",
            "session": {"title": "Wheel test", "status": "open", "wheelSpinsOwed": 2},
            "status": {"activeCount": 1, "capacity": 44, "pressure": "low"},
            "nowPlaying": None,
            "upNext": None,
            "queue": [],
            "wheel": {
                "spinsOwed": 2,
                "status": "confirmed",
                "lastConfirmedWinner": {
                    "trackId": "winner-track",
                    "artist": "Wheel Artist",
                    "title": "Wheel Track",
                    "currentQueuePosition": 3,
                    "currentLane": "wheel",
                    "occurredAt": "2026-08-28T20:00:00.000Z",
                },
                "recentEvents": [{
                    "eventType": "wheel_confirmed",
                    "occurredAt": "2026-08-28T20:00:00.000Z",
                    "track": {
                        "trackId": "winner-track",
                        "artist": "Wheel Artist",
                        "title": "Wheel Track",
                        "currentQueuePosition": 3,
                        "currentLane": "wheel",
                    },
                    "details": {"wheelCandidateCount": 12},
                }],
            },
        }

        with mock.patch.dict(
            os.environ,
            {"BNL_QUEUE_PRODUCTION_ENABLED": "true"},
            clear=False,
        ):
            context = bnl01_bot.build_bnl_read_model_context(
                model,
                "Who won the wheel?",
                "sealed_test",
            )

        self.assertIn("Wheel ceremony status: confirmed", context)
        self.assertIn("Latest confirmed Wheel winner: Wheel Artist — Wheel Track", context)
        self.assertIn("queuePosition=3", context)
        self.assertIn("wheel_confirmed", context)
        self.assertIn("candidates=12", context)

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
