import argparse
import io
import json
import sys
import unittest
from contextlib import redirect_stderr, redirect_stdout
from types import ModuleType, SimpleNamespace
from unittest import mock

from scripts import tiktok_live_chat_transport as transport
from scripts import tiktok_live_telemetry_common as telemetry_common


class TransportPayloadTests(unittest.TestCase):
    def test_comment_payload_contains_only_normalized_comment_fields(self):
        event = SimpleNamespace(
            room_id="room-1",
            data={
                "common": {"msgId": "message-9", "createTime": "1787931000000"},
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
        self.assertTrue(payload["source_at"].startswith("2026-"))
        serialized = json.dumps(payload)
        self.assertNotIn("avatar", serialized.lower())
        self.assertNotIn("bioDescription", serialized)
        self.assertNotIn("private-avatar-url", serialized)

    def test_empty_comment_is_not_emitted(self):
        event = SimpleNamespace(room_id="room", data={"content": "\u0000\n"})
        self.assertIsNone(transport.build_comment_payload(event))

    def test_like_and_viewer_payloads_are_bounded_metrics(self):
        like = transport.build_like_payload(
            SimpleNamespace(
                room_id="room",
                data={
                    "common": {"msgId": "like-1"},
                    "count": "42",
                    "total": "9001",
                    "user": {"uniqueId": "tapper", "nickname": "Tapper"},
                },
            )
        )
        viewers = transport.build_viewer_snapshot_payload(
            SimpleNamespace(
                room_id="room",
                data={"common": {"msgId": "view-1"}, "viewerCount": "37"},
            )
        )
        self.assertEqual(like["like_count"], 42)
        self.assertEqual(like["like_total"], 9001)
        self.assertEqual(like["unique_id"], "tapper")
        self.assertEqual(viewers["viewer_count"], 37)

    def test_follow_share_and_question_payloads_keep_public_fields_only(self):
        social = SimpleNamespace(
            room_id="room",
            data={
                "common": {"msgId": "social-1"},
                "action": "2",
                "shareType": "3",
                "shareTarget": "should-not-leave",
                "user": {
                    "uniqueId": "viewer",
                    "nickname": "Viewer",
                    "bioDescription": "private-profile-text",
                },
            },
        )
        share = transport.build_social_payload("share", social)
        follow = transport.build_social_payload("follow", social)
        question = transport.build_question_payload(
            SimpleNamespace(
                room_id="room",
                data={
                    "common": {"msgId": "question-1"},
                    "details": {
                        "questionId": "77",
                        "questionText": "Where do I submit?",
                        "answerStatus": "0",
                        "user": {"uniqueId": "asker", "nickname": "Asker"},
                    },
                },
            )
        )
        self.assertEqual(share["event_type"], "share")
        self.assertEqual(share["share_type"], 3)
        self.assertEqual(follow["event_type"], "follow")
        self.assertEqual(question["question_text"], "Where do I submit?")
        serialized = json.dumps([share, follow, question])
        self.assertNotIn("private-profile-text", serialized)
        self.assertNotIn("shareTarget", serialized)

    def test_gift_emits_only_completed_combo_and_never_currency(self):
        base = {
            "common": {"msgId": "gift-1"},
            "giftId": "5655",
            "repeatCount": "3",
            "gift": {"id": "5655", "name": "Rose", "diamondCount": "1"},
            "user": {"uniqueId": "giver", "nickname": "Giver"},
            "is_combo": True,
            "diamond_total": 3,
        }
        in_progress = transport.build_gift_payload(
            SimpleNamespace(room_id="room", data={**base, "is_streak_over": False})
        )
        completed = transport.build_gift_payload(
            SimpleNamespace(room_id="room", data={**base, "is_streak_over": True})
        )
        self.assertIsNone(in_progress)
        self.assertEqual(completed["gift_name"], "Rose")
        self.assertEqual(completed["gift_count"], 3)
        self.assertEqual(completed["diamond_total"], 3)
        self.assertNotIn("usd", json.dumps(completed).lower())
        self.assertNotIn("dollar", json.dumps(completed).lower())

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
            telemetry_common,
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


