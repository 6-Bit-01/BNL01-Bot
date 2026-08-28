import argparse
import io
import json
import sys
import unittest
from contextlib import redirect_stderr, redirect_stdout
from types import ModuleType, SimpleNamespace
from unittest import mock

from scripts import tiktok_live_chat_transport as transport


class TransportPayloadTests(unittest.TestCase):
    def test_comment_payload_contains_only_normalized_comment_fields(self):
        event = SimpleNamespace(
            room_id="room-1",
            data={
                "common": {"msgId": "message-9"},
                "content": "  hello\u0000\nworld  ",
                "user": {
                    "uniqueId": "@test.viewer",
                    "nickname": "Test Viewer",
                    "avatarThumb": {"urlList": ["private-avatar-url"]},
                    "bioDescription": "should not leave transport",
                    "identity": {"isModeratorOfAnchor": True},
                },
            },
        )
        payload = transport.build_comment_payload(event)

        self.assertEqual(payload["event_type"], "comment")
        self.assertEqual(payload["event_id"], "tiktok:room-1:message-9")
        self.assertEqual(payload["comment_text"], "hello world")
        self.assertEqual(payload["unique_id"], "test.viewer")
        self.assertTrue(payload["moderator_flag"])
        serialized = json.dumps(payload)
        self.assertNotIn("avatar", serialized.lower())
        self.assertNotIn("bioDescription", serialized)
        self.assertNotIn("private-avatar-url", serialized)

    def test_empty_comment_is_not_emitted(self):
        event = SimpleNamespace(room_id="room", data={"content": "\u0000\n"})
        self.assertIsNone(transport.build_comment_payload(event))

    def test_fallback_event_id_uses_stable_source_time(self):
        event = SimpleNamespace(
            room_id="room-1",
            data={
                "common": {"createTime": "1787931000000"},
                "content": "same replayed comment",
                "user": {"uniqueId": "viewer", "nickname": "Viewer"},
            },
        )
        with mock.patch.object(
            transport,
            "_utc_now",
            side_effect=["2026-08-28T23:30:00Z", "2026-08-28T23:31:00Z"],
        ):
            first = transport.build_comment_payload(event)
            second = transport.build_comment_payload(event)
        self.assertEqual(first["event_id"], second["event_id"])
        self.assertNotEqual(first["observed_at"], second["observed_at"])

    def test_transport_error_uses_class_only(self):
        payload = transport.build_transport_error_payload(
            RuntimeError("https://example.invalid/?token=secret")
        )
        self.assertEqual(payload["error_code"], "RuntimeError")
        self.assertNotIn("secret", json.dumps(payload))
        self.assertNotIn("example.invalid", json.dumps(payload))

    def test_reconnecting_payload_bounds_numeric_fields(self):
        event = SimpleNamespace(
            room_id="room",
            data={"attempt": "3", "max_retries": "5", "delay": "8"},
        )
        payload = transport.build_lifecycle_payload("reconnecting", event)
        self.assertEqual(payload["attempt"], 3)
        self.assertEqual(payload["max_retries"], 5)
        self.assertEqual(payload["delay_seconds"], 8)

    def test_emit_payload_is_one_compact_json_line(self):
        output = io.StringIO()
        with redirect_stdout(output):
            transport.emit_payload({"schema_version": 1, "event_type": "connected"})
        lines = output.getvalue().splitlines()
        self.assertEqual(len(lines), 1)
        self.assertEqual(json.loads(lines[0])["event_type"], "connected")

    def test_event_id_deduper_is_bounded(self):
        deduper = transport.EventIdDeduper(capacity=2)
        self.assertTrue(deduper.accept("one"))
        self.assertFalse(deduper.accept("one"))
        self.assertTrue(deduper.accept("two"))
        self.assertTrue(deduper.accept("three"))
        self.assertTrue(deduper.accept("one"))
        self.assertEqual(deduper.duplicates, 1)


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

    def test_run_transport_stops_at_live_end_and_suppresses_replay(self):
        class FakeEventType:
            connected = "connected"
            chat = "chat"
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
                connected = SimpleNamespace(room_id="room", data={"room_id": "room"})
                comment = SimpleNamespace(
                    room_id="room",
                    data={
                        "common": {"msgId": "1"},
                        "content": "hello",
                        "user": {"uniqueId": "viewer", "nickname": "Viewer"},
                    },
                )
                self.emit("connected", connected)
                self.emit("chat", comment)
                self.emit("chat", comment)
                self.emit("live_ended", SimpleNamespace(room_id="room", data={}))
                # Simulate the upstream library trying to deliver a stale frame after end.
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
            ["connected", "comment", "live_ended", "disconnected"],
        )
        self.assertTrue(FakeClient.last_instance.settings["disconnected"])
        self.assertEqual(FakeClient.last_instance.settings["cdn"], "us")
        self.assertEqual(FakeClient.last_instance.settings["max_retries"], 4)
        self.assertEqual(FakeClient.last_instance.settings["stale_timeout"], 75.0)
        self.assertNotIn("gift", FakeClient.last_instance.listeners)
        self.assertNotIn("follow", FakeClient.last_instance.listeners)
        self.assertNotIn("join", FakeClient.last_instance.listeners)


if __name__ == "__main__":
    unittest.main()
