import os
import unittest
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl01_bot as bot


REQUEST = (
    "Read the current private rehearsal titled BARCODE Radio [09-15-2026]. "
    "Give the submitted artist and song titles for B2 Complete and B3 Partial Priority. "
    "Distinguish actual playback from tracks only loaded, queued or removed. "
    "If that session is unavailable, say so; do not guess or use a different show."
)


def rehearsal_model():
    def track(track_id, title, stage, **playback):
        return {
            "id": track_id, "submittedArtistName": "Test Artist B",
            "submittedSongTitle": title, "detectedArtistName": "Wrong Uploader",
            "detectedSongTitle": "Wrong Filename", "stage": stage,
            "playedAt": "2026-09-16T05:00:00Z" if stage == "completed" else None,
            "playback": playback,
        }

    def event(track_id, event_type, sequence):
        return {
            "sessionId": "private-rehearsal", "eventType": event_type,
            "sequence": sequence, "occurredAt": "2026-09-16T05:00:00Z",
            "track": {"trackId": track_id},
        }

    return {
        "ok": True, "version": 1, "publicOnly": False, "accessScope": "private",
        "capabilities": {"queueProduction": True},
        "sections": {
            "queue": {
                "available": True, "accessScope": "private",
                "session": {
                    "sessionId": "private-rehearsal",
                    "title": "BARCODE Radio [09-15-2026]", "showDate": "2026-09-15",
                    "status": "open", "broadcastPhase": "broadcast_active",
                },
                "completed": [
                    track("a1", "A1 Loaded Only", "completed", outcome="finished", endedNaturally=False),
                    track("b2", "B2 Complete", "completed", outcome="finished", endedNaturally=True),
                    track("b3", "B3 Partial Priority", "completed", outcome="finished", endedNaturally=False, earlyCutoff=True, endPositionSeconds=6),
                ],
                "queue": [track("waiting", "Waiting Song", "queued")],
                "removed": [track("removed", "Removed Song", "removed")],
                # Starts have already rolled out of the bounded recent queue events.
                "recentEvents": [event("b3", "track_finished", 8)],
            },
            "archive": {
                "available": True,
                "currentShow": {
                    "sessionId": "private-rehearsal",
                    "milestones": [
                        event("a1", "track_loaded", 1),
                        event("a1", "track_finished", 2),
                        event("b2", "track_play_started", 3),
                        event("b2", "track_finished", 4),
                        event("b3", "track_play_started", 5),
                        event("b3", "track_paused", 6),
                        event("b3", "track_resumed", 7),
                        event("b3", "track_finished", 8),
                    ],
                },
            },
        },
    }


class RehearsalReadModelTests(unittest.TestCase):
    def read(self, request=REQUEST, *, model=None, policy="sealed_test", gate="true"):
        model = rehearsal_model() if model is None else model
        with mock.patch.dict(os.environ, {"BNL_QUEUE_PRODUCTION_ENABLED": gate}), \
             mock.patch.object(bot, "fetch_bnl_read_model", return_value=model) as fetch:
            context = bot.maybe_build_bnl_read_model_context(request, policy)
        return context, fetch

    def test_operator_request_fetches_named_rehearsal_and_playback_evidence(self):
        context, fetch = self.read()
        fetch.assert_called_once_with(force=True)
        self.assertIn("sessionId=private-rehearsal", context)
        self.assertIn("BARCODE Radio [09-15-2026]", context)
        self.assertIn("Test Artist B — B2 Complete", context)
        self.assertIn("Test Artist B — B3 Partial Priority", context)
        self.assertEqual(context.count("actualPlayback=confirmed"), 2)
        self.assertIn("track_resumed=1", context)
        self.assertIn("earlyCutoff=True", context)
        b3 = next(line for line in context.splitlines() if "B3 Partial Priority" in line)
        self.assertIn("track_finished=1", b3)
        self.assertNotIn("Wrong Uploader", context)
        self.assertNotIn("Wrong Filename", context)
        self.assertNotIn("A1 Loaded Only", context)
        self.assertIn("do not substitute another session", context)

    def test_read_intent_does_not_require_the_word_queue(self):
        for request in (
            "Check B2 Complete's playback in our private rehearsal.",
            "Who submitted B2 Complete for the current BARCODE Radio session?",
            "Give me the submitted song titles for the current rehearsal.",
            "Was B3 Partial Priority actually played during this rehearsal?",
        ):
            with self.subTest(request=request):
                _, fetch = self.read(request)
                fetch.assert_called_once_with(force=True)

    def test_loaded_finished_queued_and_removed_are_not_playback_proof(self):
        context, _ = self.read(
            "Read the private rehearsal playback for A1 Loaded Only, "
            "Waiting Song and Removed Song."
        )
        self.assertIn("A1 Loaded Only", context)
        self.assertIn("Waiting Song", context)
        self.assertIn("Removed Song", context)
        self.assertEqual(sum(" | actualPlayback=not_evidenced" in line for line in context.splitlines()), 3)
        self.assertNotIn("actualPlayback=confirmed", context)
        self.assertIn("track_loaded=1", context)
        self.assertIn("track_finished=1", context)

    def test_other_session_events_cannot_confirm_current_playback(self):
        model = rehearsal_model()
        model["sections"]["archive"]["currentShow"]["sessionId"] = "different-show"
        context, _ = self.read(model=model)
        b3 = next(line for line in context.splitlines() if "B3 Partial Priority" in line)
        self.assertIn("actualPlayback=not_evidenced", b3)
        self.assertNotIn("track_resumed=", context)

    def test_existing_access_boundaries_still_apply(self):
        for policy in ("public_home", "public_context", "public_selective", "broadcast_memory"):
            with self.subTest(policy=policy):
                context, _ = self.read(policy=policy)
                self.assertNotIn("B2 Complete", context)
                self.assertNotIn("private-rehearsal", context)
        for policy in ("sealed_test", "internal_controlled"):
            with self.subTest(policy=policy):
                context, _ = self.read(policy=policy)
                self.assertIn("B2 Complete", context)
                self.assertIn("Do not treat this as durable memory", context)
        context, _ = self.read(gate="false")
        self.assertNotIn("B2 Complete", context)
        model = rehearsal_model()
        model["capabilities"]["queueProduction"] = False
        context, _ = self.read(model=model)
        self.assertNotIn("B2 Complete", context)

    def test_unavailable_feed_and_unrelated_chat_do_not_invent_a_session(self):
        context, fetch = self.read(model={})
        fetch.assert_called_once_with(force=True)
        self.assertEqual(context, "")
        for request in ("I enjoyed rehearsal.", "My song has a rehearsal-room sound.", "Hello BNL"):
            with self.subTest(request=request):
                context, fetch = self.read(request)
                fetch.assert_not_called()
                self.assertEqual(context, "")


if __name__ == "__main__":
    unittest.main()
