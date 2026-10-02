import asyncio
import json
import os
import stat
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from zoneinfo import ZoneInfo

from bnl_tiktok_live_chat import LiveChatAdapter, LiveChatBuffer, parse_line, ProtocolError
from bnl_tiktok_live_memory import (
    ARCHIVE_SCHEMA_VERSION, ENGAGEMENT_ARCHIVE_POLICY,
    TikTokPublicConversationSpoolWriter, archive_record, collector_boundary_record,
    public_conversation_record, read_public_conversation_spool,
)
from scripts.tiktok_live_shadow_model import CycleState
from scripts.tiktok_live_shadow_runtime import _consume_stdout, run_transport_cycle


STAMP = datetime(2026, 10, 2, 3, 0, tzinfo=timezone.utc).timestamp()


def event(kind, **fields):
    return {"event_type": kind, "event_id": "test:%s" % kind,
            "room_id": "test-room", "observed_at": STAMP,
            "source_at": STAMP - 2, **fields}


def engagement_events():
    return [
        event("like", like_count=25, like_total=500),
        event("viewer_snapshot", viewer_count=17),
        event("share", share_type=1), event("follow"),
        event("gift", gift_id=7, gift_name="Test Flower", gift_count=3,
              diamond_count=2, diamond_total=6, combo=True, streak_over=True),
        event("join", join_count=1),
    ]


class EngagementRecordTests(unittest.TestCase):
    def test_all_engagement_types_keep_both_clocks_without_participant_identity(self):
        for value in engagement_events():
            with self.subTest(kind=value["event_type"]):
                value.update(unique_id="test.viewer", display_name="Test Member",
                             moderator_flag=True, boundDiscordUserId=71,
                             biography="private", authorization="secret",
                             checkout={"private": "value"})
                record = archive_record(value)
                self.assertIsNotNone(record)
                self.assertEqual(record["archive_schema_version"], ARCHIVE_SCHEMA_VERSION)
                self.assertEqual(record["archive_policy"], ENGAGEMENT_ARCHIVE_POLICY)
                self.assertEqual(record["observed_at"], STAMP)
                self.assertEqual(record["source_at"], STAMP - 2)
                for private_key in ("unique_id", "display_name", "moderator_flag",
                                    "boundDiscordUserId", "biography", "authorization", "checkout"):
                    self.assertNotIn(private_key, record)

    def test_legacy_text_contract_remains_text_only(self):
        comment = event("comment", comment_text="A real public comment.",
                        unique_id="test.member", display_name="Test Member")
        self.assertEqual(archive_record(comment), public_conversation_record(comment))
        self.assertIsNone(public_conversation_record(engagement_events()[0]))
        # The old incomplete nontext fixture remains rejected by the writer.
        self.assertIsNone(archive_record(event("like", like_count=25)))

    def test_numeric_signals_reject_coercions_and_out_of_range_values(self):
        for bad in (True, False, -1, 2.5, "25", float("nan"), 10**9 + 1):
            with self.subTest(value=bad):
                self.assertIsNone(archive_record(event("like", like_count=bad, like_total=500)))
        self.assertIsNone(archive_record(event("like", like_count=0, like_total=0)))
        self.assertIsNone(archive_record(event("join", join_count=0)))
        self.assertIsNone(archive_record(event("viewer_snapshot", viewer_count=10**9 + 1)))

    def test_timestamps_reject_boolean_nonfinite_and_excessive_source_skew(self):
        for field, bad in (("observed_at", True), ("observed_at", float("nan")),
                           ("source_at", False), ("source_at", float("inf")),
                           ("source_at", STAMP + 86401)):
            with self.subTest(field=field, bad=bad):
                value = event("follow")
                value[field] = bad
                self.assertIsNone(archive_record(value))
                self.assertIsNone(archive_record({**value, "event_type": "comment",
                                                  "comment_text": "Test comment."}))
        value = event("follow")
        value.pop("source_at")
        self.assertIsNone(archive_record(value)["source_at"])

    def test_completed_gifts_only_and_platform_units_preserved(self):
        gift = engagement_events()[4]
        record = archive_record(gift)
        self.assertEqual(record["gift_count"], 3)
        self.assertEqual(record["diamond_total"], 6)
        for fields in ({"streak_over": False}, {"gift_count": 0},
                       {"combo": "true"}, {"diamond_total": -1}):
            with self.subTest(fields=fields):
                self.assertIsNone(archive_record({**gift, **fields}))

    def test_lifecycle_records_allow_only_safe_fields_and_error_codes(self):
        for kind in ("connected", "reconnecting", "disconnected", "live_ended"):
            with self.subTest(kind=kind):
                value = event(kind, unique_id="test.member", display_name="Test Member",
                              cookies="private", url="private", raw_exception="private")
                record = archive_record(value)
                self.assertEqual(record["event_type"], kind)
                self.assertNotIn("unique_id", record)
                self.assertNotIn("url", record)
                self.assertNotIn("raw_exception", record)
        self.assertEqual(archive_record(event("transport_error", error_code="ConnectionError"))["error_code"],
                         "ConnectionError")
        self.assertIsNone(archive_record(event("transport_error", error_code="https://private.invalid/secret")))

    def test_collection_boundary_does_not_claim_a_platform_end(self):
        record = collector_boundary_record("cycle_stopped", STAMP, room_id="test-room",
                                           reason="process_exit", return_code=-9)
        self.assertEqual(record["event_type"], "collector_boundary")
        self.assertEqual(record["return_code"], -9)
        self.assertEqual(archive_record(record), record)
        with self.assertRaises(ProtocolError):
            parse_line(json.dumps({**record, "schema_version": 1}), now=STAMP)
        self.assertIsNone(archive_record({**record, "reason": ["untrusted"]}))


class EngagementSpoolReaderTests(unittest.TestCase):
    def test_mixed_records_report_bad_lines_and_preserve_partial_tail(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "public-conversation.ndjson"
            values = [event("comment", comment_text="Test comment."), *engagement_events(),
                      collector_boundary_record("window_stopped", STAMP, reason="window_closed")]
            complete = "\n".join(json.dumps(value) for value in values) + "\nnot-json\n"
            path.write_bytes((complete + '{"incomplete":').encode("utf-8"))
            result = read_public_conversation_spool(str(path))
            self.assertEqual(len(result.records), len(values))
            self.assertEqual(result.invalid_lines, 1)
            self.assertEqual(result.next_offset, len(complete.encode("utf-8")))
            self.assertEqual(read_public_conversation_spool(str(path), offset=result.next_offset).reason,
                             "partial_line_waiting")

    def test_replaced_larger_spool_resets_using_file_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "public-conversation.ndjson"
            path.write_text(json.dumps(event("follow")) + "\n", encoding="utf-8")
            first = read_public_conversation_spool(str(path))
            replacement = Path(directory) / "replacement.ndjson"
            replacement.write_text("\n".join(json.dumps(value) for value in engagement_events()) + "\n",
                                   encoding="utf-8")
            os.replace(replacement, path)
            result = read_public_conversation_spool(str(path), offset=first.next_offset,
                                                    expected_spool_identity=first.spool_identity)
            self.assertTrue(result.reset)
            self.assertEqual(len(result.records), 6)

    @unittest.skipUnless(hasattr(os, "fchmod"), "requires the production Unix file permission API")
    def test_existing_writer_roundtrips_text_metrics_and_boundaries_with_private_permissions(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "public-conversation.ndjson"
            writer = TikTokPublicConversationSpoolWriter(str(path))
            values = [event("comment", comment_text="Test comment."), *engagement_events(),
                      collector_boundary_record("window_started", STAMP)]
            for value in values:
                self.assertTrue(writer.append(value))
            self.assertFalse(writer.append(event("like", like_count=25)))
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
            self.assertEqual(len(read_public_conversation_spool(str(path)).records), len(values))


class EngagementCollectorTests(unittest.IsolatedAsyncioTestCase):
    async def test_unexpected_process_exit_archives_a_collector_boundary(self):
        from scripts import tiktok_live_shadow_runtime as runtime
        stdout, stderr = asyncio.StreamReader(), asyncio.StreamReader()
        stdout.feed_eof()
        stderr.feed_eof()
        process = SimpleNamespace(stdout=stdout, stderr=stderr, returncode=3,
                                  wait=AsyncMock(return_value=3))
        records = []

        class Writer:
            def append(self, value):
                records.append(archive_record(value))
                return records[-1] is not None

        with patch.object(runtime, "build_transport_command", return_value=["unused-test-command"]), \
                patch.object(runtime.asyncio, "create_subprocess_exec", AsyncMock(return_value=process)), \
                patch("builtins.print"):
            result = await run_transport_cycle(SimpleNamespace(), LiveChatAdapter(), ZoneInfo("UTC"),
                                              set(), asyncio.Event(), datetime.now(timezone.utc) + timedelta(seconds=1),
                                              archive_writer=Writer())
        self.assertEqual(result.stop_reason, "process_exit")
        self.assertEqual(records[0]["event_type"], "collector_boundary")
        self.assertEqual(records[0]["boundary"], "cycle_stopped")
        self.assertEqual(records[0]["reason"], "process_exit")
        self.assertEqual(records[0]["return_code"], 3)

    async def test_capture_precedes_join_and_unchanged_viewer_filters_and_includes_lifecycle(self):
        reader = asyncio.StreamReader()
        values = [event("connected"), event("comment", comment_text="Test comment."),
                  *engagement_events(), event("viewer_snapshot", event_id="test:viewer:2", viewer_count=17),
                  event("reconnecting"), event("disconnected"),
                  event("transport_error", error_code="ConnectionError")]
        for value in [*values, values[2]]:
            reader.feed_data((json.dumps({"schema_version": 1, **value}) + "\n").encode("utf-8"))
        reader.feed_eof()
        records = []

        class Writer:
            def append(self, value):
                record = archive_record(value)
                if record is not None:
                    records.append(record)
                return record is not None

        adapter = LiveChatAdapter(buffer=LiveChatBuffer(time_fn=lambda: STAMP), time_fn=lambda: STAMP)
        with patch("builtins.print"):
            await _consume_stdout(reader, SimpleNamespace(returncode=0), adapter, ZoneInfo("UTC"),
                                  set(), CycleState(), archive_writer=Writer())
        self.assertEqual([r["event_id"] for r in records], [v["event_id"] for v in values])
        self.assertEqual(sum(r["event_type"] == "join" for r in records), 1)
        self.assertEqual(sum(r["event_type"] == "viewer_snapshot" for r in records), 2)
        self.assertEqual(adapter.health["taps_observed"], 25)


if __name__ == "__main__":
    unittest.main()
