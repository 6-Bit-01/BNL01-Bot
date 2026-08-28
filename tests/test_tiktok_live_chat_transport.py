import argparse
import io
import json
import sys
import unittest
from contextlib import redirect_stderr, redirect_stdout
from types import ModuleType, SimpleNamespace
from unittest import mock

from scripts import tiktok_live_chat_transport as transport


class TransportCliTests(unittest.TestCase):
    def test_username_validation(self):
        self.assertEqual(transport.normalize_username("@six.bit"), "six.bit")
        with self.assertRaises(argparse.ArgumentTypeError):
            transport.normalize_username("six bit; rm -rf")

    def test_missing_dependency_returns_bounded_error(self):
        args = argparse.Namespace(
            username="six.bit",
            cdn="us",
            max_retries=0,
            stale_timeout=60.0,
            dedupe_capacity=20_000,
        )
        stdout = io.StringIO()
        stderr = io.StringIO()
        with mock.patch.dict(sys.modules, {"piratetok_live": None}):
            with redirect_stdout(stdout), redirect_stderr(stderr):
                result = transport.run_transport(args)
        self.assertEqual(result, 2)
        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["event_type"], "transport_error")
        self.assertEqual(payload["error_code"], "missing_dependency")
        self.assertEqual(stderr.getvalue().strip(), "missing_dependency")

    def test_run_transport_emits_public_telemetry_and_stops_at_live_end(self):
        class FakeEventType:
            connected = "connected"
            chat = "chat"
            like = "like"
            room_user_seq = "room_user_seq"
            share = "share"
            follow = "follow"
            gift = "gift"
            question_new = "question_new"
            join = "join"
            reconnecting = "reconnecting"
            live_ended = "live_ended"
            disconnected = "disconnected"

        class FakeClient:
            last_instance = None

            def __init__(self, username):
                self.username = username
                self.listeners = {}
                self.settings = {}
                FakeClient.last_instance = self

            def cdn_us(self):
                self.settings["cdn"] = "us"
                return self

            def cdn_eu(self):
                self.settings["cdn"] = "eu"
                return self

            def max_retries(self, value):
                self.settings["max_retries"] = value
                return self

            def stale_timeout(self, value):
                self.settings["stale_timeout"] = value
                return self

            def on(self, event_type):
                def register(callback):
                    self.listeners.setdefault(event_type, []).append(callback)
                    return callback
                return register

            def disconnect(self):
                self.settings["disconnected"] = True

            def emit(self, event_type, event):
                for callback in self.listeners.get(event_type, []):
                    callback(event)

            def run(self):
                common = lambda msg: {"msgId": msg}
                self.emit("connected", SimpleNamespace(room_id="room", data={}))
                comment = SimpleNamespace(
                    room_id="room",
                    data={
                        "common": common("comment-1"),
                        "content": "hello",
                        "user": {"uniqueId": "viewer", "nickname": "Viewer"},
                    },
                )
                self.emit("chat", comment)
                self.emit("chat", comment)
                self.emit(
                    "like",
                    SimpleNamespace(
                        room_id="room",
                        data={"common": common("like-1"), "count": 25, "total": 500},
                    ),
                )
                self.emit(
                    "room_user_seq",
                    SimpleNamespace(
                        room_id="room",
                        data={"common": common("view-1"), "viewerCount": 33},
                    ),
                )
                social = SimpleNamespace(
                    room_id="room",
                    data={
                        "common": common("social-1"),
                        "user": {"uniqueId": "social", "nickname": "Social"},
                    },
                )
                self.emit("share", social)
                social2 = SimpleNamespace(
                    room_id="room",
                    data={
                        "common": common("social-2"),
                        "user": {"uniqueId": "follower", "nickname": "Follower"},
                    },
                )
                self.emit("follow", social2)
                self.emit(
                    "gift",
                    SimpleNamespace(
                        room_id="room",
                        data={
                            "common": common("gift-progress"),
                            "is_combo": True,
                            "is_streak_over": False,
                            "repeatCount": 2,
                            "gift": {"name": "Rose", "diamondCount": 1},
                        },
                    ),
                )
                self.emit(
                    "gift",
                    SimpleNamespace(
                        room_id="room",
                        data={
                            "common": common("gift-final"),
                            "is_combo": True,
                            "is_streak_over": True,
                            "repeatCount": 2,
                            "diamond_total": 2,
                            "gift": {"name": "Rose", "diamondCount": 1},
                            "user": {"uniqueId": "giver", "nickname": "Giver"},
                        },
                    ),
                )
                self.emit(
                    "question_new",
                    SimpleNamespace(
                        room_id="room",
                        data={
                            "common": common("question-1"),
                            "details": {
                                "questionId": "7",
                                "questionText": "Where do I submit?",
                                "user": {"uniqueId": "asker", "nickname": "Asker"},
                            },
                        },
                    ),
                )
                self.emit(
                    "join",
                    SimpleNamespace(
                        room_id="room",
                        data={
                            "common": common("join-1"),
                            "user": {"uniqueId": "new.viewer", "nickname": "New Viewer"},
                        },
                    ),
                )
                self.emit("live_ended", SimpleNamespace(room_id="room", data={}))
                self.emit("reconnecting", SimpleNamespace(room_id="room", data={}))
                self.emit("chat", comment)
                self.emit("disconnected", SimpleNamespace(room_id="room", data={}))

        fake_module = ModuleType("piratetok_live")
        fake_module.EventType = FakeEventType
        fake_module.TikTokLiveClient = FakeClient
        args = argparse.Namespace(
            username="six.bit",
            cdn="us",
            max_retries=4,
            stale_timeout=75.0,
            dedupe_capacity=20_000,
        )
        stdout = io.StringIO()
        with mock.patch.dict(sys.modules, {"piratetok_live": fake_module}):
            with redirect_stdout(stdout):
                result = transport.run_transport(args)

        self.assertEqual(result, 0)
        emitted = [json.loads(line) for line in stdout.getvalue().splitlines()]
        self.assertEqual(
            [event["event_type"] for event in emitted],
            [
                "connected",
                "comment",
                "like",
                "viewer_snapshot",
                "share",
                "follow",
                "gift",
                "question",
                "join",
                "live_ended",
                "disconnected",
            ],
        )
        self.assertTrue(FakeClient.last_instance.settings["disconnected"])
        self.assertEqual(FakeClient.last_instance.settings["cdn"], "us")
        self.assertEqual(FakeClient.last_instance.settings["max_retries"], 4)
        self.assertEqual(FakeClient.last_instance.settings["stale_timeout"], 75.0)
        for event_name in (
            "like",
            "room_user_seq",
            "share",
            "follow",
            "gift",
            "question_new",
            "join",
        ):
            self.assertIn(event_name, FakeClient.last_instance.listeners)
        self.assertNotIn("post", FakeClient.last_instance.listeners)
        self.assertNotIn("moderate", FakeClient.last_instance.listeners)


if __name__ == "__main__":
    unittest.main()
