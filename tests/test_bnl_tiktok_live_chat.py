import json
import unittest

from bnl_tiktok_live_chat import (
    AUTHORITY,
    COMMENT_AUTHORITY,
    IDENTITY_DEFAULT,
    INTERACTION_AUTHORITY,
    MEMORY_DEFAULT,
    METRIC_AUTHORITY,
    METRIC_MEMORY,
    PUBLIC_TEXT_MEMORY,
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
        "source_at": 1_700_000_000.0,
        "unique_id": "test.viewer",
        "display_name": "Test Viewer",
        "comment_text": "This track is wild.",
        "moderator_flag": False,
    }
    value.update(changes)
    return value


class ParseTests(unittest.TestCase):
    def test_adapter_owns_source_and_source_aware_memory_semantics(self):
        event = parse_line(
            json.dumps(payload(source="spoof", memory_default="store")),
            now=1_700_000_000.0,
        )
        record = event.context_record()
        self.assertEqual(record["source"], SOURCE)
        self.assertEqual(record["visibility"], VISIBILITY)
        self.assertEqual(record["authority"], AUTHORITY)
        self.assertEqual(record["authority"], COMMENT_AUTHORITY)
        self.assertEqual(MEMORY_DEFAULT, "source_aware")
        self.assertEqual(record["memory_default"], PUBLIC_TEXT_MEMORY)
        self.assertEqual(record["identity_default"], IDENTITY_DEFAULT)

    def test_sanitizes_and_bounds_comment(self):
        event = parse_line(
            json.dumps(payload(comment_text="hello\u0000\nworld " + "x" * 2000)),
            now=1_700_000_000.0,
        )
        self.assertTrue(event.comment_text.startswith("hello world"))
        self.assertLessEqual(len(event.comment_text), 1000)

    def test_parses_all_shadow_telemetry_types(self):
        cases = [
            (payload(event_type="like", comment_text=None, like_count="25", like_total="500"), "like"),
            (payload(event_type="viewer_snapshot", comment_text=None, unique_id="", display_name="", viewer_count="37"), "viewer_snapshot"),
            (payload(event_type="share", comment_text=None, share_type="2"), "share"),
            (payload(event_type="follow", comment_text=None), "follow"),
            (payload(event_type="gift", comment_text=None, gift_id="10", gift_name="Rose", gift_count="3", diamond_count="1", diamond_total="3", combo=True, streak_over=True), "gift"),
            (payload(event_type="question", comment_text=None, question_id="7", question_text="Where do I submit?", answer_status="0"), "question"),
            (payload(event_type="join", comment_text=None, join_count="1"), "join"),
        ]
        parsed = [parse_line(json.dumps(value), now=1_700_000_000.0) for value, _ in cases]
        self.assertEqual([event.event_type for event in parsed], [expected for _, expected in cases])
        self.assertEqual(parsed[0].like_count, 25)
        self.assertEqual(parsed[1].viewer_count, 37)
        self.assertEqual(parsed[4].diamond_total, 3)
        self.assertEqual(parsed[5].question_text, "Where do I submit?")
        self.assertEqual(parsed[0].telemetry_record()["authority"], INTERACTION_AUTHORITY)
        self.assertEqual(parsed[1].telemetry_record()["authority"], METRIC_AUTHORITY)
        self.assertEqual(parsed[5].telemetry_record()["authority"], COMMENT_AUTHORITY)
        self.assertEqual(parsed[0].telemetry_record()["memory_default"], METRIC_MEMORY)
        self.assertEqual(parsed[5].telemetry_record()["memory_default"], PUBLIC_TEXT_MEMORY)

    def test_source_timestamp_is_preserved_separately_from_receipt_time(self):
        event = parse_line(
            json.dumps(payload(observed_at=1_700_000_010.0, source_at=1_700_000_000.0)),
            now=1_700_000_010.0,
        )
        self.assertEqual(event.observed_at, 1_700_000_010.0)
        self.assertEqual(event.source_at, 1_700_000_000.0)
        self.assertEqual(event.event_time, 1_700_000_000.0)

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
        self.buffer = LiveChatBuffer(3, 30, self.clock)
        self.adapter = LiveChatAdapter(self.buffer, self.clock)

    def add(self, event_id, text="comment"):
        return self.adapter.ingest_line(
            json.dumps(
                payload(
                    event_id=event_id,
                    comment_text=text,
                    observed_at=self.clock(),
                    source_at=self.clock(),
                )
            )
        )

    def ingest(self, **changes):
        return self.adapter.ingest_line(
            json.dumps(
                payload(
                    observed_at=self.clock(),
                    source_at=self.clock(),
                    **changes,
                )
            )
        )

    def test_dedupes_and_caps(self):
        self.assertIsNotNone(self.add("a"))
        self.assertIsNone(self.add("a", "replay"))
        self.add("b")
        self.add("c")
        self.add("d")
        self.assertEqual(
            [row["event_id"] for row in self.adapter.context_snapshot()],
            ["b", "c", "d"],
        )
        health = self.adapter.health_snapshot()
        self.assertEqual(health["duplicate_count"], 1)
        self.assertEqual(health["overflow_count"], 1)
        self.assertEqual(health["comments_accepted"], 4)

    def test_seen_ids_are_bounded_separately_from_event_buffer(self):
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
        self.assertEqual(health["events_buffered"], 2)
        self.assertEqual(health["seen_event_ids"], 3)

    def test_metrics_are_deduped_and_aggregated_without_join_retention(self):
        self.ingest(event_type="like", event_id="like-1", comment_text=None, like_count=100, like_total=1000)
        self.assertIsNone(
            self.ingest(event_type="like", event_id="like-1", comment_text=None, like_count=100, like_total=1000)
        )
        self.ingest(event_type="viewer_snapshot", event_id="view-1", comment_text=None, unique_id="", display_name="", viewer_count=25)
        self.ingest(event_type="viewer_snapshot", event_id="view-2", comment_text=None, unique_id="", display_name="", viewer_count=41)
        self.ingest(event_type="share", event_id="share-1", comment_text=None)
        self.ingest(event_type="follow", event_id="follow-1", comment_text=None)
        self.ingest(event_type="gift", event_id="gift-1", comment_text=None, gift_name="Rose", gift_count=3, diamond_total=3, streak_over=True)
        self.ingest(event_type="question", event_id="question-1", comment_text=None, question_text="Where?", question_id="1")
        self.ingest(event_type="join", event_id="join-1", comment_text=None, join_count=1)

        health = self.adapter.health_snapshot()
        self.assertEqual(health["taps_observed"], 100)
        self.assertEqual(health["latest_like_total"], 1000)
        self.assertEqual(health["viewer_count"], 41)
        self.assertEqual(health["peak_viewers"], 41)
        self.assertEqual(health["shares"], 1)
        self.assertEqual(health["follows"], 1)
        self.assertEqual(health["gift_events"], 1)
        self.assertEqual(health["gift_units"], 3)
        self.assertEqual(health["diamond_total"], 3)
        self.assertEqual(health["questions"], 1)
        self.assertEqual(health["joins"], 1)
        self.assertEqual(health["duplicate_count"], 1)
        self.assertFalse(any(row["event_type"] == "join" for row in self.adapter.telemetry_snapshot()))
        self.assertEqual(self.adapter.context_snapshot(), [])

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
