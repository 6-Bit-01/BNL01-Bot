import json
import unittest

from bnl_tiktok_live_chat import (
    AUTHORITY,
    IDENTITY_DEFAULT,
    MEMORY_DEFAULT,
    SOURCE,
    VISIBILITY,
    LiveChatAdapter,
    LiveChatBuffer,
    ProtocolError,
    parse_line,
)


class Clock:
    def __init__(self):
        self.value = 1_700_000_000.0

    def __call__(self):
        return self.value


def payload(**changes):
    value = {
        "schema_version": 1,
        "event_type": "comment",
        "event_id": "message-1",
        "room_id": "room-1",
        "observed_at": 1_700_000_000.0,
        "unique_id": "test.viewer",
        "display_name": "Test Viewer",
        "comment_text": "This track is wild.",
        "moderator_flag": False,
    }
    value.update(changes)
    return value


class ParseTests(unittest.TestCase):
    def test_adapter_owns_source_and_no_store_semantics(self):
        event = parse_line(
            json.dumps(payload(source="spoof", memory_default="store")),
            now=1_700_000_000.0,
        )
        record = event.context_record()
        self.assertEqual(record["source"], SOURCE)
        self.assertEqual(record["visibility"], VISIBILITY)
        self.assertEqual(record["authority"], AUTHORITY)
        self.assertEqual(record["memory_default"], MEMORY_DEFAULT)
        self.assertEqual(record["identity_default"], IDENTITY_DEFAULT)

    def test_sanitizes_and_bounds_comment(self):
        event = parse_line(
            json.dumps(payload(comment_text="hello\u0000\nworld " + "x" * 2000)),
            now=1_700_000_000.0,
        )
        self.assertTrue(event.comment_text.startswith("hello world"))
        self.assertLessEqual(len(event.comment_text), 1000)

    def test_failures_are_bounded(self):
        with self.assertRaises(ProtocolError) as invalid:
            parse_line("{secret-token", now=1.0)
        self.assertEqual(invalid.exception.code, "invalid_json")
        self.assertNotIn("secret-token", str(invalid.exception))
        with self.assertRaises(ProtocolError) as handle:
            parse_line(json.dumps(payload(unique_id="bad handle;secret")), now=1.0)
        self.assertEqual(handle.exception.code, "invalid_unique_id")

    def test_missing_id_gets_deterministic_fallback(self):
        value = json.dumps(payload(event_id=""))
        first = parse_line(value, now=1_700_000_000.0)
        second = parse_line(value, now=1_700_000_000.0)
        self.assertEqual(first.event_id, second.event_id)
        self.assertTrue(first.event_id.startswith("local:"))


class BufferTests(unittest.TestCase):
    def setUp(self):
        self.clock = Clock()
        self.buffer = LiveChatBuffer(2, 30, self.clock)
        self.adapter = LiveChatAdapter(self.buffer, self.clock)

    def add(self, event_id, text="comment"):
        return self.adapter.ingest_line(
            json.dumps(
                payload(
                    event_id=event_id,
                    comment_text=text,
                    observed_at=self.clock(),
                )
            )
        )

    def test_dedupes_and_caps(self):
        self.assertIsNotNone(self.add("a"))
        self.assertIsNone(self.add("a", "replay"))
        self.add("b")
        self.add("c")
        self.assertEqual(
            [row["event_id"] for row in self.adapter.context_snapshot()], ["b", "c"]
        )
        health = self.adapter.health_snapshot()
        self.assertEqual(health["duplicate_count"], 1)
        self.assertEqual(health["overflow_count"], 1)

    def test_seen_ids_are_bounded_separately_from_comment_buffer(self):
        buffer = LiveChatBuffer(2, 300, self.clock, max_seen_events=3)
        adapter = LiveChatAdapter(buffer, self.clock)
        for event_id in ("a", "b", "c", "d"):
            adapter.ingest_line(
                json.dumps(
                    payload(
                        event_id=event_id,
                        comment_text=event_id,
                        observed_at=self.clock(),
                    )
                )
            )
        health = adapter.health_snapshot()
        self.assertEqual(health["comments_buffered"], 2)
        self.assertEqual(health["seen_event_ids"], 3)

    def test_expires_and_clears_at_live_end(self):
        self.add("a")
        self.clock.value += 31
        self.assertEqual(self.adapter.context_snapshot(), [])
        self.add("b")
        self.adapter.ingest_line(
            json.dumps(
                {
                    "schema_version": 1,
                    "event_type": "live_ended",
                    "event_id": "end",
                    "room_id": "room-1",
                    "observed_at": self.clock(),
                }
            )
        )
        self.assertEqual(self.adapter.context_snapshot(), [])
        self.assertEqual(self.adapter.health_snapshot()["state"], "ended")

    def test_invalid_line_changes_health_not_storage(self):
        self.assertIsNone(self.adapter.ingest_line("not-json secret"))
        health = self.adapter.health_snapshot()
        self.assertEqual(health["invalid_lines"], 1)
        self.assertEqual(health["last_error_code"], "invalid_json")
        self.assertNotIn("secret", json.dumps(health))


if __name__ == "__main__":
    unittest.main()
