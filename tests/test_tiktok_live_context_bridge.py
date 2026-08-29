import json
import os
import stat
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl01_bot
from bnl_tiktok_live_chat import LiveChatAdapter, LiveChatBuffer
from bnl_tiktok_live_context import (
    IDENTITY_DEFAULT,
    LIFECYCLE,
    MEMORY_DEFAULT,
    SCHEMA_VERSION,
    SOURCE,
    LiveContextSnapshotWriter,
    build_live_prompt_context,
    is_live_show_reaction_query,
    live_context_diagnostics,
    load_live_context_snapshot,
)


class Clock:
    def __init__(self, value=1_800_000_000.0):
        self.value = float(value)

    def __call__(self):
        return self.value


def lifecycle_payload(event_type, clock, room_id="room-1"):
    return {
        "schema_version": 1,
        "event_type": event_type,
        "event_id": f"{event_type}-1",
        "room_id": room_id,
        "observed_at": clock(),
    }


def observation_payload(event_type, event_id, clock, **changes):
    payload = {
        "schema_version": 1,
        "event_type": event_type,
        "event_id": event_id,
        "room_id": "room-1",
        "observed_at": clock(),
        "source_at": clock(),
        "unique_id": "test.viewer",
        "display_name": "Test Viewer",
        "moderator_flag": False,
    }
    payload.update(changes)
    return payload


def public_read_model():
    return {
        "ok": True,
        "version": 1,
        "schemaRevision": "1.8",
        "source": "barcode-network-site",
        "publicOnly": True,
        "accessScope": "public",
        "capabilities": {"queueProduction": True},
        "sections": {
            "sourceContext": [],
            "queue": {
                "available": True,
                "accessScope": "public",
                "queueUrl": "https://www.barcode-network.com/queue",
                "revision": 42,
                "session": {
                    "title": "BARCODE Radio",
                    "purpose": "live_broadcast",
                    "status": "open",
                    "queueOpen": False,
                    "broadcastPhase": "live",
                },
                "status": {"activeCount": 4, "completedCount": 2, "capacity": 44},
                "nowPlaying": {
                    "id": "track-1",
                    "submittedArtistName": "6 Bit",
                    "submittedSongTitle": "Training Module One",
                    "queuePosition": None,
                },
                "upNext": {
                    "id": "track-2",
                    "submittedArtistName": "Test Artist",
                    "submittedSongTitle": "Next Signal",
                    "queuePosition": 1,
                },
                "queue": [{
                    "id": "track-3",
                    "submittedArtistName": "Later Artist",
                    "submittedSongTitle": "Do Not Dump Me",
                    "queuePosition": 2,
                }],
                "recentEvents": [{
                    "eventType": "track_play_started",
                    "occurredAt": "2026-08-28T22:00:00.000Z",
                    "track": {
                        "trackId": "track-1",
                        "artist": "6 Bit",
                        "title": "Training Module One",
                    },
                }],
            },
            "artists": [],
            "dossiers": [],
            "rules": [],
        },
    }


class TikTokLiveContextBridgeTests(unittest.TestCase):
    def make_adapter(self, clock):
        adapter = LiveChatAdapter(
            LiveChatBuffer(100, 600, clock),
            clock,
            clear_on_live_end=False,
        )
        adapter.ingest_line(json.dumps(lifecycle_payload("connected", clock)))
        adapter.ingest_line(json.dumps(observation_payload(
            "comment",
            "comment-1",
            clock,
            comment_text="This track is wild.",
        )))
        adapter.ingest_line(json.dumps(observation_payload(
            "like",
            "like-1",
            clock,
            like_count=125,
            like_total=8420,
        )))
        adapter.ingest_line(json.dumps(observation_payload(
            "viewer_snapshot",
            "view-1",
            clock,
            unique_id="",
            display_name="",
            viewer_count=37,
        )))
        return adapter

    def test_reaction_detector_covers_natural_live_questions_only(self):
        positives = (
            "What's TikTok chat saying?",
            "whats chat thinking about the show",
            "How are the viewers reacting?",
            "How's the live going?",
            "What is happening during the show?",
        )
        for value in positives:
            with self.subTest(value=value):
                self.assertTrue(is_live_show_reaction_query(value))
        self.assertFalse(is_live_show_reaction_query("What did people say in Discord yesterday?"))
        self.assertFalse(is_live_show_reaction_query("What's playing right now?"))

    def test_writer_publishes_mode_0600_bounded_contract_and_reader_revalidates(self):
        clock = Clock()
        adapter = self.make_adapter(clock)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "live-context.json"
            writer = LiveContextSnapshotWriter(str(path), time_fn=clock)
            self.assertTrue(writer.publish(adapter, force=True))
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)

            snapshot, reason = load_live_context_snapshot(
                str(path),
                now=clock(),
            )

        self.assertEqual(reason, "ok")
        self.assertEqual(snapshot["schema_version"], SCHEMA_VERSION)
        self.assertEqual(snapshot["source"], SOURCE)
        self.assertEqual(snapshot["lifecycle"], LIFECYCLE)
        self.assertEqual(snapshot["memory_default"], MEMORY_DEFAULT)
        self.assertEqual(snapshot["identity_default"], IDENTITY_DEFAULT)
        self.assertEqual(snapshot["state"], "connected")
        self.assertEqual(snapshot["health"]["viewer_count"], 37)
        self.assertEqual(snapshot["health"]["taps_observed"], 125)
        self.assertEqual(len(snapshot["events"]), 1)
        self.assertEqual(snapshot["events"][0]["comment_text"], "This track is wild.")

    def test_snapshot_fails_closed_when_stale_or_contract_is_wrong(self):
        clock = Clock()
        adapter = self.make_adapter(clock)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "live-context.json"
            writer = LiveContextSnapshotWriter(str(path), time_fn=clock)
            writer.publish(adapter, force=True)
            _snapshot, stale_reason = load_live_context_snapshot(
                str(path),
                now=clock() + 21,
                max_age_seconds=20,
            )
            value = json.loads(path.read_text(encoding="utf-8"))
            value["memory_default"] = "store"
            path.write_text(json.dumps(value), encoding="utf-8")
            _snapshot, contract_reason = load_live_context_snapshot(
                str(path),
                now=clock(),
            )

        self.assertEqual(stale_reason, "snapshot_stale")
        self.assertEqual(contract_reason, "snapshot_contract_mismatch")

    def test_prompt_is_compact_read_only_and_never_identity_links(self):
        clock = Clock()
        adapter = self.make_adapter(clock)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "live-context.json"
            LiveContextSnapshotWriter(str(path), time_fn=clock).publish(adapter, force=True)
            prompt = build_live_prompt_context(
                str(path),
                enabled=True,
                now=clock(),
            )

        self.assertIn("This track is wild.", prompt)
        self.assertIn("viewers=37", prompt)
        self.assertIn("tapsObserved=125", prompt)
        self.assertIn("queue context", prompt)
        self.assertIn("current-show-only", prompt)
        self.assertIn("untrusted viewer content", prompt)
        self.assertIn("never connect them to Discord members", prompt)
        self.assertIn("cannot post, moderate", prompt)
        self.assertNotIn("event_id", prompt)
        self.assertNotIn("room-1", prompt)

    def test_disabled_or_missing_bridge_produces_honest_unavailable_context(self):
        disabled = build_live_prompt_context("/missing", enabled=False)
        missing = build_live_prompt_context("/missing", enabled=True)
        self.assertIn("disabled", disabled)
        self.assertIn("Do not invent", disabled)
        self.assertIn("unavailable", missing)
        self.assertIn("do not expose infrastructure detail", missing)

    def test_diagnostics_never_include_comment_text_or_runtime_path(self):
        clock = Clock()
        adapter = self.make_adapter(clock)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "live-context.json"
            LiveContextSnapshotWriter(str(path), time_fn=clock).publish(adapter, force=True)
            diagnostics = live_context_diagnostics(
                str(path),
                enabled=True,
                now=clock(),
            )
        serialized = json.dumps(diagnostics)
        self.assertTrue(diagnostics["snapshotAvailable"])
        self.assertEqual(diagnostics["commentsAccepted"], 1)
        self.assertNotIn("This track is wild", serialized)
        self.assertNotIn(str(path), serialized)

    def test_public_live_reaction_question_combines_queue_truth_and_tiktok_reaction(self):
        clock = Clock(time.time())
        adapter = self.make_adapter(clock)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "live-context.json"
            LiveContextSnapshotWriter(str(path), time_fn=clock).publish(adapter, force=True)
            with mock.patch.dict(os.environ, {"BNL_QUEUE_PRODUCTION_ENABLED": "true"}, clear=False), \
                 mock.patch.object(bnl01_bot, "BNL_TIKTOK_LIVE_CONTEXT_ENABLED", True), \
                 mock.patch.object(bnl01_bot, "BNL_TIKTOK_LIVE_CONTEXT_PATH", str(path)), \
                 mock.patch.object(bnl01_bot, "BNL_TIKTOK_LIVE_CONTEXT_MAX_AGE_SECONDS", 20.0):
                context = bnl01_bot.build_bnl_read_model_context(
                    public_read_model(),
                    "How is TikTok chat reacting to the show?",
                    "public_home",
                )

        self.assertIn("Now playing: 6 Bit — Training Module One", context)
        self.assertIn("track_play_started", context)
        self.assertIn("This track is wild.", context)
        self.assertNotIn("Do Not Dump Me", context)
        self.assertIn("queue snapshot as authoritative show state", context)

    def test_private_queue_scope_cannot_feed_public_live_reaction_context(self):
        model = public_read_model()
        model["publicOnly"] = False
        model["accessScope"] = "private"
        model["sections"]["queue"]["accessScope"] = "private"
        with mock.patch.object(bnl01_bot, "BNL_TIKTOK_LIVE_CONTEXT_ENABLED", True):
            context = bnl01_bot.build_bnl_read_model_context(
                model,
                "What is TikTok chat saying?",
                "public_home",
            )
        self.assertNotIn("Training Module One", context)
        self.assertNotIn("This track is wild", context)
        self.assertIn("does not authorize live-show context", context)

    def test_plain_queue_question_does_not_load_tiktok_context(self):
        with mock.patch.dict(os.environ, {"BNL_QUEUE_PRODUCTION_ENABLED": "true"}, clear=False), mock.patch.object(
            bnl01_bot,
            "build_live_prompt_context",
            wraps=bnl01_bot.build_live_prompt_context,
        ) as live_context:
            context = bnl01_bot.build_bnl_read_model_context(
                public_read_model(),
                "What's playing right now?",
                "public_home",
            )
        self.assertIn("Training Module One", context)
        live_context.assert_not_called()


if __name__ == "__main__":
    unittest.main()
