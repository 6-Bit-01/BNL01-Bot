"""Source-window coverage must survive the actual packet render boundary."""

import hashlib
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from datetime import datetime, timezone
from unittest import mock

from bnl_shared_brain_synthesis import render_packet_context
from bnl_tiktok_live_context import (
    SHOW_INTERVAL_CONTEXT_MAX_CHARS,
    build_durable_show_prompt_context,
    build_show_interval_conversation,
    build_show_timeline,
    build_tiktok_show_evidence_ledger,
    show_conversation_scope,
)
from bnl_tiktok_show_ledger import (
    _authored_show_messages,
    build_tiktok_show_evidence_context,
    select_tiktok_show_episode_context_items,
    sync_tiktok_show_evidence_ledgers,
    load_show_timeline_discord_messages,
)
from bnl_unified_intelligence_packet import IntelligencePacketRequest, build_packet, revalidate_packet
import test_tiktok_show_evidence_ledger as fixtures
from test_tiktok_show_evidence_ledger import (
    ENABLED_QUEUE_ENV, archived_show,
    artist_index, authorized_read_model, discord_exchanges, durable_events, stamp,
)


QUERY = "For the August 28, 2026 BARCODE Radio show, what did TikTok chat say during Neon Fox — First Signal?"
PACKET_ENV = {
    **ENABLED_QUEUE_ENV,
    "BNL_MEMORY_LEDGER_SHADOW_ENABLED": "true",
    "BNL_MOMENT_ENGINE_SHADOW_ENABLED": "true",
    "BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED": "true",
    "BNL_RELATIONSHIP_V2_SHADOW_ENABLED": "true",
    "BNL_UNIFIED_INTELLIGENCE_PACKET_SHADOW_ENABLED": "true",
}


def interval_events(count=26):
    # Most messages never name the track, address BNL, or ask a question.
    # A busy room's later remarks must not disappear behind early examples.
    base = durable_events()[0]
    return [{
        **base, "event_id": f"interval-{index:03d}",
        "occurred_at_ms": stamp("2026-08-29T00:01:15Z") + index * 5000,
        "subject_ref": f"tiktok_user:fixture-{index % 6}",
        "private_display_name": f"Test Listener {index % 6}",
        "metadata": {"eventType": "comment"},
        "raw_text": f"Observation {index:03d}: the green lights keep shifting and the room is trading running jokes.",
    } for index in range(count)]


class ShowIntervalConversationTests(unittest.TestCase):
    def ledger(self, events=None, show=None):
        return build_tiktok_show_evidence_ledger(
            show or archived_show(), events if events is not None else interval_events() + durable_events()[2:],
            artist_identity_index=artist_index(), discord_exchanges=discord_exchanges(),
        )

    def test_all_twenty_six_utterances_survive_artist_bias_and_old_excerpt_limits(self):
        ledger = self.ledger()
        result = build_show_interval_conversation(ledger, QUERY, messages=_authored_show_messages(ledger))
        self.assertEqual(result["event_ids"], tuple(f"interval-{i:03d}" for i in range(26)))
        self.assertEqual(result["rendered_event_ids"], result["event_ids"])
        self.assertEqual(len(result["participants"]), 6)
        self.assertTrue(result["complete"])
        self.assertGreater(len(result["text"]), 2800)
        self.assertIn("Observation 025:", result["text"])
        self.assertNotIn("The green visuals made that moment hit.", result["text"])
        self.assertNotIn("Did the Wheel put Queue Light", result["text"])

    def test_title_wins_over_other_tracks_by_the_same_artist(self):
        show = archived_show()
        show["trackRoster"][1]["projectLabel"] = "Neon Fox"
        for event in show["milestones"]:
            if (event.get("track") or {}).get("trackId") == "track-queue-light":
                event["track"]["projectLabel"] = "Neon Fox"
        result = build_show_interval_conversation(self.ledger(show=show), QUERY)
        self.assertEqual(result["message_count"], 26)
        self.assertEqual(result["track_keys"], ("track_id:track-first-signal",))

    def test_relative_previous_uses_completed_playback_not_loudest_or_latest_submission(self):
        show = archived_show()
        show["status"] = "open"
        show["milestones"] = show["milestones"][:10]
        show["_evidenceObservedThroughMs"] = stamp("2026-08-29T00:07:00Z")
        ledger = self.ledger(show=show)
        previous = build_show_interval_conversation(ledger, "What did TikTok chat say during the last track?")
        current = build_show_interval_conversation(ledger, "What is TikTok chat saying during this track?")
        self.assertEqual(previous["message_count"], 26)
        self.assertEqual(previous["labels"], ("Neon Fox — First Signal",))
        self.assertEqual(current["labels"], ("Second Artist — Queue Light",))
        self.assertIn("The green visuals made that moment hit.", current["text"])
        self.assertIn("provisional observation", current["text"])
        self.assertNotIn("Observation 025:", current["text"])

    def test_archived_last_track_and_current_track_have_distinct_meanings(self):
        ledger = self.ledger()
        previous = build_show_interval_conversation(ledger, "What did TikTok chat say during the last track?")
        current = build_show_interval_conversation(ledger, "What is TikTok chat saying during this track?")
        self.assertEqual(previous["labels"], ("Second Artist — Queue Light",))
        self.assertEqual(current["status"], "unresolved")
        self.assertFalse(current["complete"])

    def test_explicit_show_interval_uses_half_open_time_and_keeps_platform_authorship(self):
        ledger = self.ledger()
        result = build_show_interval_conversation(
            ledger, "What did TikTok and Discord chat say between minute 5 and 6?",
            messages=_authored_show_messages(ledger),
        )
        self.assertIn("Did the Wheel put Queue Light", result["text"])
        self.assertNotIn("Yes—the Wheel confirmed", result["text"])
        self.assertNotIn("The green visuals made that moment hit.", result["text"])
        self.assertNotIn("This private row", result["text"])

    def test_unknown_window_does_not_substitute_an_artist_or_other_track(self):
        result = build_show_interval_conversation(self.ledger(), QUERY.replace("First Signal", "Missing Title").replace("Neon Fox", "Missing Artist"))
        self.assertEqual(result["status"], "unresolved")
        self.assertFalse(result["complete"])
        self.assertEqual(result["event_ids"], ())
        self.assertIn("does not establish zero chat", result["text"])

    def test_retained_text_omission_is_reported_without_cutting_an_utterance(self):
        ledger = self.ledger()
        ledger["messages"][0]["textDigest"] = hashlib.sha256(b"A longer original source text").hexdigest()
        result = build_show_interval_conversation(ledger, QUERY)
        self.assertFalse(result["complete"])
        self.assertEqual(result["source_text_partial"], 1)
        self.assertEqual(result["rendered_count"], 26)

    def test_oversized_scope_counts_all_rows_and_discloses_transcript_omission(self):
        ledger = self.ledger()
        first = ledger["messages"][0]
        ledger["messages"] = [{**first, "eventId": f"dense-{i}", "text": "x" * 1000,
                               "textDigest": hashlib.sha256(("x" * 1000).encode()).hexdigest()}
                              for i in range(150)]
        result = build_show_interval_conversation(ledger, QUERY)
        self.assertEqual(result["message_count"], 150)
        self.assertLess(result["rendered_count"], 150)
        self.assertFalse(result["complete"])
        self.assertLessEqual(len(result["text"]), SHOW_INTERVAL_CONTEXT_MAX_CHARS)
        self.assertIn("complete=no", result["text"])

    def test_whole_show_person_recall_keeps_its_existing_owner(self):
        for query in ("What did Neon Fox say during the show?", "What did 6 Bit say during yesterday's show?",
                      "What did Neon Fox say during the session?"):
            with self.subTest(query=query):
                self.assertIsNone(show_conversation_scope(self.ledger(), query))

    def test_live_and_archived_builders_share_the_complete_interval_view(self):
        prompt = build_durable_show_prompt_context({"latestShow": archived_show()}, interval_events(), QUERY)
        result = build_show_interval_conversation(self.ledger(interval_events()), QUERY)
        self.assertEqual(prompt, "Durable TikTok show analysis context:\n" + result["text"])

    def test_real_database_packet_default_renderer_and_native_reader_keep_all_rows(self):
        from bnl_journal_source_store import record_source_event

        with tempfile.TemporaryDirectory() as directory:
            db = str(Path(directory) / "bnl.db")
            fixture = fixtures.TikTokShowEvidenceLedgerTests()
            fixture.seed_source_and_memory(db)
            for event in interval_events():
                result = record_source_event(
                    db, guild_id=77, source_kind="tiktok_live_chat", source_key=event["event_id"],
                    occurred_at_ms=event["occurred_at_ms"], raw_text=event["raw_text"],
                    sanitized_summary=event["raw_text"], subject_ref=event["subject_ref"],
                    private_display_name=event["private_display_name"], public_usable=True, metadata=event["metadata"],
                )
                self.assertTrue(result.ok)
            sync_tiktok_show_evidence_ledgers(
                db, guild_id=77, read_model=authorized_read_model({"latestShow": archived_show()}),
                artist_identity_index=artist_index(), environ=ENABLED_QUEUE_ENV,
            )
            conn = sqlite3.connect(db)
            request = IntelligencePacketRequest(
                guild_id=77, subject_user_id=0, channel_id=9001, channel_policy="public_home",
                route_mode="normal_chat", conversation_surface="mention_or_reply", visibility_allowance="public_safe",
                user_text=QUERY, direct_state="direct", now="2026-08-29T12:00:00-07:00",
            )
            packet = build_packet(conn, request, persist=True, environ=PACKET_ENV)
            self.assertIsNotNone(packet)
            self.assertEqual(packet.diagnostics.invalid_invariants, [])
            rendered, _lanes, count, _digests = render_packet_context(packet)
            self.assertGreater(count, 0)
            for i in range(26):
                self.assertIn(f"Observation {i:03d}:", rendered)
            self.assertIn("Transcript coverage: 28/28", rendered)
            self.assertNotIn("The green visuals made that moment hit.", rendered)
            self.assertTrue(revalidate_packet(conn, packet, environ=ENABLED_QUEUE_ENV).valid)
            self.assertFalse(revalidate_packet(conn, packet, environ={}).valid)
            selection = {}
            native = build_tiktok_show_evidence_context(db, guild_id=77, user_text=QUERY, selection_out=selection)
            self.assertEqual(len(selection["authored_excerpts"]), 28)
            self.assertTrue(selection["interval_coverage"]["complete"])
            self.assertIn("Observation 025:", native)
            conn.commit()
            record_source_event(
                db, guild_id=77, source_kind="tiktok_live_chat", source_key="late-arriving-original",
                occurred_at_ms=stamp("2026-08-29T00:03:40Z"), raw_text="One more source observation.",
                sanitized_summary="One more source observation.", subject_ref="tiktok_user:fixture-0",
                private_display_name="Test Listener 0", public_usable=True, metadata={"eventType": "comment"},
            )
            sync_tiktok_show_evidence_ledgers(
                db, guild_id=77, read_model=authorized_read_model({"latestShow": archived_show()}),
                artist_identity_index=artist_index(), environ=ENABLED_QUEUE_ENV,
            )
            self.assertFalse(revalidate_packet(conn, packet, environ=ENABLED_QUEUE_ENV).valid)
            conn.close()

    def test_authorized_live_read_model_includes_chat_after_latest_track_operation(self):
        import bnl01_bot as bot
        from test_bnl_live_context_bridge import public_read_model

        show = archived_show()
        show["status"] = "open"
        show["milestones"] = show["milestones"][:10]
        model = public_read_model()
        model["sections"]["archive"] = authorized_read_model({"currentShow": show})["sections"]["archive"]
        model["sections"]["queue"]["session"]["id"] = show["sessionId"]
        query = "What is TikTok chat saying during this track?"
        with mock.patch.object(bot, "_load_durable_tiktok_show_events", return_value=durable_events()), \
             mock.patch.dict(bot.os.environ, ENABLED_QUEUE_ENV), \
             mock.patch.object(bot, "_bnl_read_model_cached_at", datetime(2026, 8, 29, 0, 7, tzinfo=timezone.utc)):
            text = bot.build_bnl_read_model_context(model, query, "public_home")
        self.assertIn("The green visuals made that moment hit.", text)
        self.assertIn("Transcript coverage: 2/2", text)
        self.assertIn("provisional observation", text)

    def test_interval_public_speaker_boundary_uses_only_configured_owner_identity(self):
        ledger = self.ledger()
        ledger["messages"][0].update(subjectRef="discord_user:123", speakerLabel="Test Member")
        with mock.patch.dict("os.environ", {"BNL_OWNER_USER_ID": "123"}):
            result = build_show_interval_conversation(ledger, QUERY)
        self.assertIn("6 Bit:", result["text"])
        self.assertNotIn("Test Member", result["text"])

    def test_timeline_keeps_every_operation_and_labels_model_words_separately(self):
        ledger = self.ledger()
        rows = _authored_show_messages(ledger)
        rows.append({"eventId": "discord_conversation:999", "role": "model", "surface": "discord",
                     "occurredAtMs": stamp("2026-08-29T00:02:00Z"), "text": "Test model interpretation."})
        timeline = build_show_timeline(ledger, rows)
        self.assertEqual(len(timeline), len(ledger["operationalEvents"]) + len(rows))
        times = [row["occurredAtMs"] for row in timeline]
        self.assertEqual(times, sorted(times))
        self.assertIn("wheel_confirmed", {row.get("eventType") for row in timeline})
        self.assertIn("track_finished", {row.get("eventType") for row in timeline})
        result = build_show_interval_conversation(ledger, QUERY.replace("TikTok", "TikTok and Discord"), messages=rows)
        self.assertEqual(result["message_count"], 26)
        self.assertIn("[recorded operation] track_finished", result["text"])
        self.assertIn("model's own words; not audience evidence", result["text"])
        self.assertNotIn("discord_conversation:999", result["event_ids"])

    def test_unanswered_public_discord_chatter_is_read_without_backfill_or_source_mutation(self):
        from bnl_journal_source_store import record_source_event

        with tempfile.TemporaryDirectory() as directory:
            db = str(Path(directory) / "bnl.db")
            fixtures.TikTokShowEvidenceLedgerTests().seed_source_and_memory(db)
            sync_tiktok_show_evidence_ledgers(
                db, guild_id=77, read_model=authorized_read_model({"latestShow": archived_show()}),
                artist_identity_index=artist_index(), environ=ENABLED_QUEUE_ENV,
            )
            with sqlite3.connect(db) as conn:
                for i in range(20):
                    conn.execute("""INSERT INTO conversations
                        (id,user_id,user_name,guild_id,channel_name,channel_policy,route_mode,role,content,timestamp,channel_id,message_id)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (200+i, 45, "Test Listener", 77, "barcode-bot", "public_home", "normal_chat", "user",
                         f"Unanswered room observation {i:02d}.", "2026-08-29T00:02:30Z", 9001, 8000+i))
            result = record_source_event(
                db, guild_id=77, source_kind="discord_message", source_key="source-only-passive",
                occurred_at_ms=stamp("2026-08-29T00:02:40Z"), raw_text="Source-only ordinary room chatter.",
                sanitized_summary="Source-only ordinary room chatter.", subject_ref="discord_user:46",
                private_display_name="Test Member", public_usable=True, channel_id=9001, channel_policy="public_home",
                metadata={"directedToBnl": False, "messageId": 8300, "channelName": "barcode-bot"},
            )
            self.assertTrue(result.ok)
            before = hashlib.sha256(Path(db).read_bytes()).hexdigest()
            with sqlite3.connect("file:%s?mode=ro" % db, uri=True) as conn:
                items = select_tiktok_show_episode_context_items(conn, guild_id=77, user_text=QUERY.replace("TikTok", "Discord"))
                text = "\n".join(item.text for item in items)
            for i in range(20):
                self.assertIn(f"Unanswered room observation {i:02d}.", text)
            self.assertIn("Source-only ordinary room chatter.", text)
            self.assertIn("Transcript coverage: 21/21", text)
            self.assertNotIn("This private row", text)
            self.assertNotIn("Yes—the Wheel confirmed", text)
            self.assertEqual(before, hashlib.sha256(Path(db).read_bytes()).hexdigest())
            rows, complete = load_show_timeline_discord_messages(db, guild_id=77, show=archived_show())
            self.assertTrue(complete)
            self.assertTrue(any(row.get("role") == "model" for row in rows))
            self.assertTrue(any(row.get("text") == "Unanswered room observation 19." for row in rows))

    def test_live_date_scope_does_not_recall_a_different_finalized_show(self):
        from dataclasses import replace

        with tempfile.TemporaryDirectory() as directory:
            db = str(Path(directory) / "bnl.db")
            fixtures.TikTokShowEvidenceLedgerTests().seed_source_and_memory(db)
            sync_tiktok_show_evidence_ledgers(db, guild_id=77,
                read_model=authorized_read_model({"latestShow": archived_show()}), environ=ENABLED_QUEUE_ENV)
            with sqlite3.connect(db) as conn:
                request = IntelligencePacketRequest(guild_id=77, subject_user_id=0,
                    route_mode="normal_chat", conversation_surface="mention_or_reply", channel_policy="public_home",
                    user_text="What did TikTok chat say during the last track?", show_episode_dates=("2026-08-29",))
                packet = build_packet(conn, request, persist=True, environ=PACKET_ENV)
                self.assertFalse(any(item.lane == "show_episode" for item in packet.items))
                exact = build_packet(conn, replace(request, user_text=QUERY), persist=True, environ=PACKET_ENV)
                self.assertTrue(any(item.lane == "show_episode" for item in exact.items))

    def test_operation_moment_has_its_own_chat_and_exact_end_boundary(self):
        ledger = self.ledger()
        operations = ledger["operationalEvents"]
        operations.extend([
            {"eventId": "wheel-start", "eventType": "wheel_launched", "occurredAtMs": stamp("2026-08-29T00:04:21Z")},
        ])
        row = ledger["messages"][0]
        ledger["messages"].extend([
            {**row, "eventId": "wheel-chat", "occurredAtMs": stamp("2026-08-29T00:04:26Z"),
             "text": "That wheel suspense is getting me.", "textDigest": ""},
            {**row, "eventId": "after-wheel", "occurredAtMs": stamp("2026-08-29T00:04:30Z"),
             "text": "After the result.", "textDigest": ""},
        ])
        result = build_show_interval_conversation(ledger, "What did TikTok chat say during the last wheel spin?")
        self.assertEqual(result["event_ids"], ("wheel-chat",))
        self.assertIn("[recorded operation] wheel_confirmed", result["text"])
        self.assertNotIn("After the result.", result["text"])

    def test_missing_discord_read_does_not_claim_complete_cross_platform_coverage(self):
        ledger = self.ledger()
        result = build_show_interval_conversation(ledger, QUERY.replace("TikTok", "TikTok and Discord"), discord_complete=False)
        self.assertFalse(result["complete"])
        self.assertEqual(result["message_count"], 26)
        self.assertIn("Discord source coverage is incomplete", result["text"])

    def test_whole_show_timeline_keeps_pre_broadcast_operations_and_all_broadcast_chat(self):
        ledger = self.ledger()
        ledger["operationalEvents"].append({"eventId": "pre-intake", "eventType": "track_submitted",
            "occurredAtMs": ledger["startedAtMs"] - 60000, "trackLabel": "Test Artist — Early Entry"})
        result = build_show_interval_conversation(ledger, "Give me the timeline of the August 28 radio show.")
        self.assertEqual(result["basis"], "recorded_show_timeline")
        self.assertEqual(result["message_count"], len(ledger["messages"]))
        self.assertEqual(result["timeline_event_count"], len(ledger["messages"]) + len(ledger["operationalEvents"]))
        self.assertIn("Test Artist — Early Entry", result["text"])
        self.assertIn("earlier or later session chat has not been established", result["text"])

    def test_whole_show_timeline_is_selected_by_the_actual_packet_owner(self):
        with tempfile.TemporaryDirectory() as directory:
            db = str(Path(directory) / "bnl.db")
            fixtures.TikTokShowEvidenceLedgerTests().seed_source_and_memory(db)
            sync_tiktok_show_evidence_ledgers(db, guild_id=77,
                read_model=authorized_read_model({"latestShow": archived_show()}), environ=ENABLED_QUEUE_ENV)
            with sqlite3.connect(db) as conn:
                items = select_tiktok_show_episode_context_items(conn, guild_id=77,
                    user_text="Give me the timeline of the August 28, 2026 BARCODE Radio show.")
            timeline = next(item for item in items if item.usage == "scoped_show_conversation")
            self.assertTrue(any(item.source_class == "first_party_record" for item in items))
            self.assertIn("[recorded operation] wheel_confirmed", timeline.text)
            self.assertIn("[recorded operation] session_archived", timeline.text)

    def test_enumerated_show_timeline_keeps_all_operations_through_native_and_packet_readers(self):
        import bnl01_bot as bot

        # The deployed request listed event categories. Those words must not
        # turn a whole-show timeline into a wheel-only interval.
        queries = (
            "Give me the August 28, 2026 BARCODE Radio show timeline in order: "
            "session start, submissions, wheel spins, track starts and stops, removals, and show end.",
            "Give me the August 28, 2026 BARCODE Radio show timeline for "
            "track starts and stops, wheel spins, removals, and show end.",
            "Give me the August 28, 2026 BARCODE Radio show chronology of "
            "song starts and stops, wheel spins, submissions, and show end.",
            "Give me the August 28, 2026 BARCODE Radio show timeline for "
            "wheel spins, sponsor breaks, track starts and stops, and show end.",
        )
        with tempfile.TemporaryDirectory() as directory, mock.patch.dict(os.environ, PACKET_ENV):
            db = str(Path(directory) / "bnl.db")
            fixtures.TikTokShowEvidenceLedgerTests().seed_source_and_memory(db)
            model = authorized_read_model({"latestShow": archived_show()})
            sync_tiktok_show_evidence_ledgers(db, guild_id=77, read_model=model, environ=ENABLED_QUEUE_ENV)
            for query in queries:
                native = build_tiktok_show_evidence_context(db, guild_id=77, user_text=query)
                with mock.patch.multiple(bot, DB_FILE=db, BNL_PRIMARY_GUILD_ID=77), \
                     mock.patch.object(bot, "_load_durable_tiktok_show_events", return_value=interval_events()):
                    website = bot.build_bnl_read_model_context(model, query, "sealed_test")
                with sqlite3.connect(db) as conn:
                    packet = build_packet(conn, IntelligencePacketRequest(
                        guild_id=77, subject_user_id=0, channel_id=9001, channel_policy="sealed_test",
                        route_mode="normal_chat", conversation_surface="free_speak_sealed_mirror",
                        visibility_allowance="public_safe", user_text=query, direct_state="direct",
                        now="2026-08-29T12:00:00-07:00"), persist=True, environ=PACKET_ENV)
                    rendered = render_packet_context(packet)[0]
                for reader in (native, website, rendered):
                    with self.subTest(query=query, reader=reader[:65]):
                        self.assertIn("window basis=recorded_show_timeline", reader)
                        for operation in self.ledger()["operationalEvents"]:
                            self.assertIn("[recorded operation] " + operation["eventType"], reader)

    def test_timeline_of_a_specific_interval_does_not_expand_to_the_show(self):
        ledger = self.ledger()
        ledger["operationalEvents"].append({"eventId": "wheel-start", "eventType": "wheel_launched",
            "occurredAtMs": stamp("2026-08-29T00:04:21Z")})
        for query, basis in (
            ("Give me the show timeline during Neon Fox — First Signal.", "named_track"),
            ("Give me the show timeline for Neon Fox — First Signal.", "named_track"),
            ("Give me the show timeline for the last track.", "latest_completed_playback_window"),
            ("Give me the show timeline during the last wheel spin.", "recorded_operation_interval"),
            ("Give me the timeline of the last wheel spin in the show.", "recorded_operation_interval"),
            ("Give me the show timeline from minute 1 to minute 2.", "explicit_show_offsets"),
            ("Give me the show timeline during an unknown track.", "named_track"),
        ):
            with self.subTest(query=query):
                scope = show_conversation_scope(ledger, query)
                self.assertEqual(scope["basis"], basis)
                self.assertNotEqual(scope["basis"], "recorded_show_timeline")

    def test_busy_show_timeline_keeps_operations_before_sampling_chat(self):
        ledger = self.ledger()
        first = ledger["messages"][0]
        ledger["messages"] = [{**first, "eventId": f"dense-{i}", "text": "x" * 1000,
                               "textDigest": hashlib.sha256(("x" * 1000).encode()).hexdigest()}
                              for i in range(150)]
        result = build_show_interval_conversation(ledger, "Give me the timeline of the radio show.")
        for operation in ledger["operationalEvents"]:
            self.assertIn("[recorded operation] " + operation["eventType"], result["text"])
        self.assertEqual(result["rendered_operation_count"], len(ledger["operationalEvents"]))
        self.assertEqual(result["operation_count"], len(ledger["operationalEvents"]))
        self.assertFalse(result["complete"])
        self.assertLess(result["rendered_count"], 150)
        self.assertLessEqual(len(result["text"]), SHOW_INTERVAL_CONTEXT_MAX_CHARS)

    def test_timeline_operations_remain_available_when_tiktok_archive_read_fails(self):
        prompt = build_durable_show_prompt_context({"latestShow": archived_show()}, None,
            "Give me the radio show timeline: submissions, wheel spins, track starts and stops, and show end.")
        for operation in self.ledger()["operationalEvents"]:
            self.assertIn("[recorded operation] " + operation["eventType"], prompt)
        self.assertIn("TikTok source coverage is unavailable", prompt)
        self.assertIn("complete=no", prompt)


if __name__ == "__main__":
    unittest.main()
