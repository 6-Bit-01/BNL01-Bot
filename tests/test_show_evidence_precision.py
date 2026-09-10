"""Original show evidence keeps its count scope and event-time basis in prompts."""

import hashlib
import json
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from bnl_journal_source_store import ensure_schema, record_source_event
from bnl_tiktok_live_context import (
    build_durable_show_prompt_context,
    build_tiktok_show_evidence_ledger,
)
from bnl_tiktok_show_ledger import (
    TIKTOK_SHOW_EVIDENCE_TABLE,
    build_tiktok_show_evidence_context,
    sync_tiktok_show_evidence_ledgers,
)
from tests.test_tiktok_show_evidence_ledger import authorized_read_model


START = datetime(2026, 9, 5, 2, tzinfo=timezone.utc)
QUERY = "What stood out in TikTok chat during the September 4, 2026 show? Give me actual comments."
LOADED_QUOTE = "The queue lights changed."
SECOND_QUOTE = "The queue lights are bright."
WHEEL_QUOTE = "BNL, the wheel lights changed."


def stamp(milliseconds):
    return int(START.timestamp() * 1000) + milliseconds


def iso(milliseconds):
    return (START + timedelta(milliseconds=milliseconds)).isoformat()


def show_archive(*, playback_started=False, reload_after_start=False):
    track = {"trackId": "test-track", "projectLabel": "Test Artist", "title": "Test Signal"}
    milestones = [
        ("broadcast_started", 0, None),
        ("track_loaded", 60000, track),
        ("track_submitted", 80000, None),
        ("track_finished", 180000, track),
        ("wheel_launched", 240000, None),
        ("wheel_spun", 246872, None),
        ("wheel_confirmed", 255000, None),
        ("session_archived", 360000, None),
    ]
    if playback_started:
        milestones.append(("track_play_started", 85000, track))
    if reload_after_start:
        milestones.append(("track_loaded", 95000, track))
    milestones.sort(key=lambda item: item[1])
    show = {
        "sessionId": "test-show-precision", "title": "Test Broadcast",
        "showDate": "2026-09-04", "status": "archived",
        "trackRoster": [track],
        "milestones": [
            {"sequence": index, "eventType": event, "occurredAt": iso(offset), "track": item}
            for index, (event, offset, item) in enumerate(milestones, start=1)
        ],
    }
    return {"currentShow": None, "latestShow": show, "shows": []}


def source_events():
    # Two handle keys are already bound to the same source-owned subject.
    # Counting those keys as people creates the known count discrepancy.
    return [
        {
            "event_id": "test-loaded-comment", "occurred_at_ms": stamp(90000),
            "subject_ref": "discord_user:42", "private_display_name": "Test Member",
            "raw_text": LOADED_QUOTE,
            "metadata": {"eventType": "comment", "handle": "test.member.one"},
        },
        {
            "event_id": "test-second-comment", "occurred_at_ms": stamp(110000),
            "subject_ref": "discord_user:42", "private_display_name": "Test Member",
            "raw_text": SECOND_QUOTE,
            "metadata": {"eventType": "comment", "handle": "test.member.two"},
        },
        {
            "event_id": "test-wheel-comment", "occurred_at_ms": stamp(241793),
            "subject_ref": "tiktok_user:test.guest", "private_display_name": "Test Guest",
            "raw_text": WHEEL_QUOTE,
            "metadata": {"eventType": "comment", "handle": "test.guest"},
        },
    ]


class ShowEvidencePrecisionTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.db_file = str(Path(directory.name) / "precision.db")
        ensure_schema(self.db_file)
        for event in source_events():
            result = record_source_event(
                self.db_file, guild_id=77, source_kind="tiktok_live_chat",
                source_key=event["event_id"], occurred_at_ms=event["occurred_at_ms"],
                raw_text=event["raw_text"], sanitized_summary=event["raw_text"],
                subject_ref=event["subject_ref"], private_display_name=event["private_display_name"],
                channel_policy="public_context", public_usable=True, metadata=event["metadata"],
            )
            self.assertTrue(result.ok)
        with sqlite3.connect(self.db_file) as conn:
            conn.execute("""
                CREATE TABLE conversations (
                    id INTEGER PRIMARY KEY, user_id INTEGER, user_name TEXT,
                    guild_id INTEGER, role TEXT, content TEXT, timestamp TEXT,
                    channel_id INTEGER, channel_name TEXT, channel_policy TEXT,
                    route_mode TEXT, message_id INTEGER
                )
            """)
            conn.execute("""
                INSERT INTO conversations VALUES
                    (103,43,'Test Discord Member',77,'user',?, ?,9001,
                     'barcode-bot','public_home','normal_chat',7003)
            """, ("BNL, did the queue open?", iso(200000)))
        result = record_source_event(
            self.db_file, guild_id=77, source_kind="discord_message", source_key="7003",
            occurred_at_ms=stamp(200000), raw_text="BNL, did the queue open?",
            sanitized_summary="BNL, did the queue open?", subject_ref="discord_user:43",
            private_display_name="Test Discord Member", channel_id=9001,
            channel_policy="public_home", public_usable=True,
            metadata={"messageId": 7003, "conversationRowId": 103, "routeMode": "normal_chat",
                      "directedToBnl": True, "channelName": "barcode-bot"},
        )
        self.assertTrue(result.ok)

    def contexts(self, *, playback_started=False, reload_after_start=False):
        archive = show_archive(
            playback_started=playback_started, reload_after_start=reload_after_start,
        )
        result = sync_tiktok_show_evidence_ledgers(
            self.db_file, guild_id=77, read_model=authorized_read_model(archive),
            environ={"BNL_QUEUE_PRODUCTION_ENABLED": "true"},
        )
        self.assertEqual(result["status"], "completed")
        selection = {}
        finalized = build_tiktok_show_evidence_context(
            self.db_file, guild_id=77, user_text=QUERY, selection_out=selection,
        )
        website = build_durable_show_prompt_context(archive, source_events(), QUERY)
        return {"website": website, "finalized": finalized}, selection

    def quote_line(self, context, quote):
        return next(line for line in context.splitlines() if json.dumps(quote) in line)

    def store_historical_fixture(self, ledger):
        # Keep the fixture a valid persisted source document under the existing
        # integrity contract; changing an aggregate must not bypass validation.
        ledger.pop("sourceDigest", None)
        ledger["sourceDigest"] = hashlib.sha256(json.dumps(
            ledger, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        ).encode("utf-8")).hexdigest()
        historical_json = json.dumps(ledger)
        with sqlite3.connect(self.db_file) as conn:
            conn.execute(
                f"UPDATE {TIKTOK_SHOW_EVIDENCE_TABLE} SET source_digest=?,ledger_json=?",
                (ledger["sourceDigest"], historical_json),
            )
        return historical_json

    def test_count_scope_uses_existing_tiktok_subjects_and_separates_discord(self):
        contexts, _ = self.contexts()
        with sqlite3.connect(self.db_file) as conn:
            ledger = json.loads(conn.execute(
                f"SELECT ledger_json FROM {TIKTOK_SHOW_EVIDENCE_TABLE}"
            ).fetchone()[0])
        self.assertEqual(ledger["coverage"]["participantCount"], 2)
        self.assertEqual(ledger["coverage"]["distinctSubjectCount"], 3)
        self.assertEqual(ledger["coverage"]["discordParticipantCount"], 1)
        for source, context in contexts.items():
            with self.subTest(source=source):
                prefix = "- Evidence:" if source == "website" else "Show episode:"
                total_line = next(line for line in context.splitlines() if line.startswith(prefix))
                self.assertIn("2 TikTok participants", total_line)
                self.assertNotIn("3 unique chatters", total_line)
                if source == "finalized":
                    self.assertIn("1 Discord participant", total_line)

    def test_both_sources_keep_next_wheel_event_and_exact_signed_gaps(self):
        contexts, _ = self.contexts()
        ledger = build_tiktok_show_evidence_ledger(show_archive()["latestShow"], source_events())
        wheel_message = next(item for item in ledger["messages"] if item["text"] == WHEEL_QUOTE)
        context = wheel_message["operationalContext"]
        self.assertEqual(context["lastOperationalEventType"], "wheel_launched")
        self.assertEqual(context["nextOperationalEventType"], "wheel_spun")
        self.assertEqual(context["wheelState"], "launched")
        for source, context in contexts.items():
            with self.subTest(source=source):
                line = self.quote_line(context, WHEEL_QUOTE)
                self.assertIn("wheel launched (wheel_launched) 1.793s before comment", line)
                self.assertIn("wheel spun (wheel_spun) 5.079s after comment", line)
                self.assertIn("wheel state at comment=launched", line)

    def test_loaded_track_window_does_not_imply_playback(self):
        contexts, _ = self.contexts()
        for source, context in contexts.items():
            with self.subTest(source=source):
                line = self.quote_line(context, LOADED_QUOTE)
                self.assertIn("Test Artist", line)
                self.assertIn("track loaded", line)
                self.assertIn("playback unconfirmed", line)

    def test_recorded_play_start_remains_positive_evidence(self):
        contexts, _ = self.contexts(playback_started=True)
        for source, context in contexts.items():
            with self.subTest(source=source):
                line = self.quote_line(context, LOADED_QUOTE)
                self.assertIn("track play started", line)
                self.assertNotIn("playback unconfirmed", line)

    def test_reloading_same_track_does_not_reuse_earlier_play_start(self):
        contexts, _ = self.contexts(playback_started=True, reload_after_start=True)
        for source, context in contexts.items():
            with self.subTest(source=source):
                first_line = self.quote_line(context, LOADED_QUOTE)
                reloaded_line = self.quote_line(context, SECOND_QUOTE)
                self.assertIn("track play started", first_line)
                self.assertNotIn("playback unconfirmed", first_line)
                self.assertIn("track loaded", reloaded_line)
                self.assertIn("playback unconfirmed", reloaded_line)

    def test_stored_ledger_recovers_timing_without_context_snapshot(self):
        contexts, selection = self.contexts()

        def remove_context_snapshots(value):
            if isinstance(value, dict):
                value.pop("operationalContext", None)
                for item in value.values():
                    remove_context_snapshots(item)
            elif isinstance(value, list):
                for item in value:
                    remove_context_snapshots(item)

        with sqlite3.connect(self.db_file) as conn:
            ledger = json.loads(conn.execute(
                f"SELECT ledger_json FROM {TIKTOK_SHOW_EVIDENCE_TABLE}"
            ).fetchone()[0])
        remove_context_snapshots(ledger)
        historical_json = self.store_historical_fixture(ledger)
        historical_selection = {}
        historical_context = build_tiktok_show_evidence_context(
            self.db_file, guild_id=77, user_text=QUERY, selection_out=historical_selection,
        )
        self.assertEqual(historical_context, contexts["finalized"])
        # The fixture was legitimately resealed, so its root digest changes;
        # selected original event, speaker, subject, and text must stay the same.
        self.assertEqual(
            [item[:1] + item[2:] for item in historical_selection["authored_excerpts"]],
            [item[:1] + item[2:] for item in selection["authored_excerpts"]],
        )
        with sqlite3.connect(self.db_file) as conn:
            self.assertEqual(conn.execute(
                f"SELECT ledger_json FROM {TIKTOK_SHOW_EVIDENCE_TABLE}"
            ).fetchone()[0], historical_json)

    def test_old_track_moment_handle_count_is_recomputed_from_authored_subjects(self):
        self.contexts()
        with sqlite3.connect(self.db_file) as conn:
            ledger = json.loads(conn.execute(
                f"SELECT ledger_json FROM {TIKTOK_SHOW_EVIDENCE_TABLE}"
            ).fetchone()[0])
            track = ledger["trackMoments"][0]
            self.assertEqual(track["messageCount"], 2)
            # The deployed builder counted two handles for this one subject.
            track["participantCount"] = 2
        historical_json = self.store_historical_fixture(ledger)
        context = build_tiktok_show_evidence_context(self.db_file, guild_id=77, user_text=QUERY)
        track_line = next(line for line in context.splitlines() if "participants while active" in line)
        self.assertIn("2 messages / 1 participants while active", track_line)
        with sqlite3.connect(self.db_file) as conn:
            self.assertEqual(conn.execute(
                f"SELECT ledger_json FROM {TIKTOK_SHOW_EVIDENCE_TABLE}"
            ).fetchone()[0], historical_json)

    def test_selected_authored_evidence_keeps_original_event_speaker_and_text(self):
        contexts, selection = self.contexts()
        self.assertEqual([item[0] for item in selection["source_refs"]], ["test-show-precision"])
        authored = {item[2]: (item[3], item[4], item[5], item[6])
                    for item in selection["authored_excerpts"]}
        for event in source_events():
            handle = event["metadata"]["handle"]
            self.assertEqual(authored[event["event_id"]], (
                event["subject_ref"], f'{event["private_display_name"]} (@{handle})',
                event["raw_text"], "tiktok",
            ))
            for context in contexts.values():
                self.assertIn(json.dumps(event["raw_text"]), context)


if __name__ == "__main__":
    unittest.main()
