"""Measured platform evidence stays original, bounded and separate from people."""
import copy
import gc
import json
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import bnl_journal as journal
import bnl_journal_source_store as sources
import bnl_tiktok_show_ledger as shows
from bnl_ambient_edition_sources import evidence_role
from bnl_shared_brain_synthesis import render_packet_context
from bnl_unified_intelligence_packet import IntelligencePacketRequest, build_packet, revalidate_packet
from bnl_website_relay_state import select_shared_relay_sources_on_connection, shared_relay_source_failure
from tests import test_tiktok_show_evidence_ledger as fixture


START = fixture.stamp("2026-08-28T23:30:00Z")
END = fixture.stamp("2026-08-29T00:30:00Z")


class TikTokEngagementReaderTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.db = str(Path(directory.name) / "evidence.db")
        clock = mock.patch.object(sources, "_now_ms", return_value=0)
        clock.start()
        self.addCleanup(clock.stop)
        sources.ensure_schema(self.db)
        self.addCleanup(gc.collect)

    def add(self, kind, minute=31, room="room-one", **fields):
        value = {"event_type": kind, "event_id": "%s:%s:%s" % (kind, minute, room),
                 "observed_at": (START + minute * 60_000) / 1000, "room_id": room, **fields}
        result = sources.record_tiktok_engagement_event(self.db, guild_id=77, record=value)
        self.assertTrue(result.ok, result)
        return value["event_id"]

    def read(self, **kwargs):
        with sqlite3.connect(self.db) as conn:
            return shows.read_tiktok_engagement_evidence(conn, guild_id=77,
                source_window_ms=(START, END), **kwargs)

    def sync_show(self, show=None):
        result = shows.sync_tiktok_show_evidence_ledgers(self.db, guild_id=77,
            read_model=fixture.authorized_read_model({"currentShow": None,
                "latestShow": show or fixture.archived_show(), "shows": []}),
            environ=fixture.ENABLED_QUEUE_ENV)
        self.assertEqual(result["showsWritten"], 1)

    def test_counts_snapshots_gifts_and_original_hashes_have_distinct_meaning(self):
        self.add("like", 31, like_count=12, like_total=100)
        self.add("like", 32, like_count=3, like_total=103)
        self.add("viewer_snapshot", 31, viewer_count=40)
        self.add("viewer_snapshot", 33, viewer_count=12)
        self.add("gift", 34, gift_id=1, gift_name="Rose", gift_count=3,
                 diamond_count=2, diamond_total=6, combo=True, streak_over=True)
        self.add("share", 35, share_type=0)
        self.add("follow", 36)
        self.add("join", 37, join_count=8)
        evidence = self.read()
        self.assertEqual(evidence["metrics"]["likes"]["capturedTapIncrements"], 15)
        self.assertEqual(evidence["metrics"]["likes"]["lastObservedPlatformTotal"], 103)
        self.assertEqual(evidence["metrics"]["viewers"]["peakObservedViewers"], 40)
        self.assertEqual(evidence["metrics"]["viewers"]["lastObservedViewers"], 12)
        self.assertEqual(evidence["metrics"]["gifts"]["capturedGiftUnits"], 3)
        self.assertEqual(evidence["metrics"]["gifts"]["capturedDiamondTotal"], 6)
        self.assertEqual(evidence["metrics"]["join"]["capturedJoinCount"], 8)
        self.assertEqual(len(evidence["originalSourceRefs"]), 8)
        self.assertTrue(all(len(ref["contentHash"]) == 64 for ref in evidence["originalSourceRefs"]))
        self.assertFalse(evidence["coverage"]["fullPlatformCoverage"])
        self.assertNotIn("participants", evidence)
        self.assertEqual(self.read()["sourceDigest"], evidence["sourceDigest"])

    def test_missing_cumulative_total_never_becomes_zero_or_false_counter_reset(self):
        self.add("like", 31, like_count=4, like_total=0)
        evidence = self.read()
        self.assertIsNone(evidence["metrics"]["likes"]["lastObservedPlatformTotal"])
        self.add("like", 32, like_count=3, like_total=100)
        self.add("like", 33, like_count=2, like_total=0)
        evidence = self.read()
        self.assertEqual(evidence["metrics"]["likes"]["lastObservedPlatformTotal"], 100)
        self.assertEqual(evidence["metrics"]["likes"]["lastObservedAtMs"], START + 32 * 60_000)
        self.assertFalse(evidence["metrics"]["likes"]["cumulativeCounterDecreased"])
        self.add("like", 34, room="room-two", like_count=1, like_total=5)
        evidence = self.read()
        self.assertIsNone(evidence["metrics"]["likes"]["lastObservedPlatformTotal"])
        self.assertEqual(len(evidence["metrics"]["likes"]["roomSnapshots"]), 2)
        self.assertFalse(evidence["metrics"]["likes"]["cumulativeCounterDecreased"])

    def test_collector_stop_closes_all_rooms_without_fabricating_platform_end(self):
        self.add("connected", 30)
        self.add("connected", 31, room="room-two")
        self.add("disconnected", 32)
        self.add("connected", 33)
        self.add("transport_error", 34, room="", error_code="socket_closed")
        self.add("connected", 35)
        self.add("collector_boundary", 38, room="", boundary="cycle_stopped", reason="process_exit")
        evidence = self.read()
        self.assertEqual(len(evidence["coverage"]["connectionSpans"]), 4)
        self.assertEqual(evidence["coverage"]["openConnections"], [])
        self.assertNotIn("live_ended", [item["eventType"] for item in evidence["coverage"]["collectorBoundaries"]])
        self.assertEqual(evidence["metrics"], {})

    def test_partial_corrupt_or_missing_evidence_never_reports_zero_totals(self):
        self.assertEqual(self.read()["status"], "unavailable")
        self.add("like", 31, like_count=8, like_total=80)
        self.add("like", 32, like_count=2, like_total=82)
        limited = self.read(limit=1)
        self.assertEqual(limited["status"], "partial")
        self.assertEqual(limited["metrics"], {})
        with sqlite3.connect(self.db) as conn:
            conn.execute("DROP TRIGGER trg_bnl_journal_sources_no_update")
            conn.execute("UPDATE bnl_journal_source_events SET raw_text='tampered'")
        corrupt = self.read()
        self.assertEqual(corrupt["status"], "partial")
        self.assertEqual(corrupt["metrics"], {})
        self.assertEqual(corrupt["coverage"]["rejectedOriginalCount"], 2)

    def test_reader_is_read_only_and_window_and_guild_are_exact(self):
        self.add("like", 31, like_count=8, like_total=80)
        self.add("like", 60, like_count=99, like_total=179)
        with sqlite3.connect(Path(self.db).as_uri() + "?mode=ro", uri=True) as conn:
            conn.execute("PRAGMA query_only=ON")
            evidence = shows.read_tiktok_engagement_evidence(conn, guild_id=77, source_window_ms=(START, END))
            self.assertEqual(evidence["metrics"]["likes"]["capturedTapIncrements"], 8)
            other = shows.read_tiktok_engagement_evidence(conn, guild_id=78, source_window_ms=(START, END))
            self.assertEqual(other["metrics"], {})
            self.assertEqual(conn.total_changes, 0)
        with sqlite3.connect(":memory:") as conn:
            conn.execute("PRAGMA query_only=ON")
            self.assertEqual(shows.read_tiktok_engagement_evidence(conn, guild_id=77,
                source_window_ms=(START, END))["reason"], "archive_unavailable")
            self.assertEqual(conn.execute("SELECT count(*) FROM sqlite_master").fetchone()[0], 0)

    def test_show_metrics_include_recorded_intake_and_withdrawal_rebuilds_fresh(self):
        self.add("like", 15, like_count=5, like_total=50)
        late_key = self.add("like", 32, like_count=3, like_total=53)
        show = copy.deepcopy(fixture.archived_show())
        show["milestones"].insert(0, {"sequence": 0, "eventType": "submissions_opened",
            "occurredAt": "2026-08-28T23:40:00Z", "track": None})
        self.sync_show(show)
        with sqlite3.connect(self.db) as conn:
            row = conn.execute("SELECT ledger_json FROM tiktok_show_evidence_ledgers").fetchone()
            ledger = json.loads(row[0])
            self.assertEqual(ledger["engagement"]["metrics"]["likes"]["capturedTapIncrements"], 8)
            self.assertEqual(ledger["coverage"]["eligibleMessageCount"], 0)
            self.assertEqual(ledger["participants"], [])
            selected = shows.select_tiktok_show_episode_context_items(conn, guild_id=77,
                user_text="What were the TikTok taps during the 2026-08-28 show?")
            metric = next(item for item in selected if item.kind == "engagement")
            self.assertEqual(metric.source_class, "evidence_projection")
            self.assertEqual(metric.participants, ())
            self.assertIn("not guaranteed final show totals", metric.text)
            conn.execute("DROP TRIGGER trg_bnl_journal_sources_no_update")
            conn.execute("UPDATE bnl_journal_source_events SET public_usable=0 WHERE source_key=?", (late_key,))
            current = shows.tiktok_show_episode_context_item_version(conn, guild_id=77,
                user_text="What were the TikTok taps during the 2026-08-28 show?",
                subject_user_id=0, source_ref=metric.source_ref)
            self.assertNotEqual(current, metric.source_digest)
            new = next(item for item in shows.select_tiktok_show_episode_context_items(conn, guild_id=77,
                user_text="What were the TikTok taps during the 2026-08-28 show?") if item.kind == "engagement")
            self.assertIn("Captured tap increments: 5.", new.text)
            self.assertEqual(len(new.original_source_refs), 1)

    def test_generic_gift_likes_or_programming_requests_do_not_select_metrics(self):
        for text in ("How many gifts should I buy for a birthday?", "How many likes on this post?",
                     "Show me programming metrics", "What are my Discord likes?", "I like this song"):
            with self.subTest(text=text), sqlite3.connect(self.db) as conn:
                self.assertEqual(shows.select_tiktok_engagement_context_items(conn,
                    guild_id=77, user_text=text, now=END), ())

    def test_current_temporal_queries_never_select_previous_finalized_show_metrics(self):
        self.add("like", 32, like_count=7, like_total=207)
        self.sync_show()
        self.add("like", 24 * 60 + 32, room="room-current", like_count=99, like_total=999)
        with sqlite3.connect(self.db) as conn:
            for query in ("How many taps in this show?", "What are this broadcast's TikTok likes?",
                          "How many TikTok taps tonight?", "What are today's live tap counts?"):
                with self.subTest(query=query):
                    selected = shows.select_tiktok_engagement_context_items(conn, guild_id=77,
                        user_text=query, now="2026-08-30T00:30:00Z", allow_show_linkage=True)
                    self.assertEqual(len(selected), 1)
                    self.assertEqual(selected[0].show_keys, ())
                    self.assertEqual(selected[0].lifecycle, "observed")
                    self.assertIn("Captured tap increments: 99.", selected[0].text)
                    self.assertNotIn("Captured tap increments: 7.", selected[0].text)

    def test_standalone_metric_packet_works_with_queue_off_and_revalidates_at_send(self):
        self.add("like", 32, like_count=7, like_total=207)
        self.sync_show()
        request = IntelligencePacketRequest(guild_id=77, subject_user_id=0, route_mode="normal_chat",
            conversation_surface="mention_or_reply", channel_id=9001,
            channel_policy="public_home", visibility_allowance="public_safe",
            user_text="How many TikTok taps have we captured right now?", direct_state="direct",
            now="2026-08-29T00:30:00Z", budget_chars=6000)
        with sqlite3.connect(self.db) as conn:
            packet = build_packet(conn, request, persist=False, environ={
                "BNL_MEMORY_LEDGER_SHADOW_ENABLED": "true",
                "BNL_MOMENT_ENGINE_SHADOW_ENABLED": "true",
                "BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED": "true",
                "BNL_RELATIONSHIP_V2_SHADOW_ENABLED": "true",
                "BNL_UNIFIED_INTELLIGENCE_PACKET_SHADOW_ENABLED": "true"})
            self.assertIsNotNone(packet)
            selected = [item for item in packet.items if item.source_type == "barcode_show_engagement_projection"]
            self.assertEqual(len(selected), 1)
            self.assertEqual(selected[0].lifecycle, "observed")
            self.assertEqual(selected[0].attribution_mode, "measured_platform_projection")
            self.assertNotIn("barcode_show_operations", {item.source_type for item in packet.items})
            self.assertFalse(selected[0].canon_status)
            self.assertEqual(packet.diagnostics.invalid_invariants, [])
            self.assertTrue(revalidate_packet(conn, packet, environ={}).valid)
            conn.execute("DROP TRIGGER trg_bnl_journal_sources_no_update")
            conn.execute("UPDATE bnl_journal_source_events SET channel_policy='sealed_test'")
            self.assertFalse(revalidate_packet(conn, packet, environ={}).valid)

    def test_shared_period_reaches_journal_relay_and_ambient_without_human_or_show_counts(self):
        key = self.add("like", 32, like_count=7, like_total=207)
        journal.ensure_schema(self.db)
        start, end = shows._utc_iso_from_ms(START), shows._utc_iso_from_ms(END)
        packet = journal.build_packet_from_sources(self.db, 77, start, end, [], [], prepare_schema=False)
        measured = [item for item in packet["safeSources"] if item["sourceKind"] == "tiktok_live_engagement"]
        self.assertEqual(len(measured), 1)
        self.assertEqual(measured[0]["sourceType"], "engagement")
        self.assertEqual(packet["aggregateCounts"]["eligibleConversations"], 0)
        self.assertEqual(packet["aggregateCounts"]["participants"], 0)
        self.assertFalse(journal.journal_source_packet_has_meaningful_activity(packet))
        self.assertEqual(sum(segment.get("finalizedShows", 0) for segment in packet["windowSegmentActivity"]), 0)
        self.assertEqual(evidence_role({"kind": "tiktok_live_engagement"}), "recorded_event")
        with sqlite3.connect(self.db) as conn:
            self.assertTrue(journal.journal_shared_source_provenance_is_current(conn, 77,
                packet["privateSharedSourceProvenance"]))
            relays = select_shared_relay_sources_on_connection(conn, guild_id=77, topic_text="TikTok taps", now=end)
            measured_relay = next(item for item in relays if item.source_class == "tiktok_live_engagement")
            basis = measured_relay.metadata["shared_source_provenance"]
            self.assertEqual(shared_relay_source_failure(conn, 77, basis), "")
            self.assertTrue(basis[0]["originalSourceRefs"])
            conn.execute("DROP TRIGGER trg_bnl_journal_sources_no_update")
            conn.execute("UPDATE bnl_journal_source_events SET public_usable=0 WHERE source_key=?", (key,))
            self.assertFalse(journal.journal_shared_source_provenance_is_current(conn, 77,
                packet["privateSharedSourceProvenance"]))
            self.assertEqual(shared_relay_source_failure(conn, 77, basis), "relay_source_changed")

    def test_render_keeps_gift_units_and_coverage_after_multiple_room_snapshots(self):
        for index in range(4):
            self.add("like", 31 + index, room="room-" + str(index),
                     like_count=2, like_total=100 + index)
        self.add("gift", 36, gift_id=1, gift_name="Rose", gift_count=13,
                 diamond_count=2, diamond_total=26, combo=True, streak_over=True)
        request = IntelligencePacketRequest(guild_id=77, subject_user_id=0, route_mode="normal_chat",
            conversation_surface="mention_or_reply", channel_id=9001,
            channel_policy="public_home", visibility_allowance="public_safe",
            user_text="How many TikTok gifts and taps have we captured right now?", direct_state="direct",
            now="2026-08-29T00:30:00Z", budget_chars=6000)
        with sqlite3.connect(self.db) as conn:
            packet = build_packet(conn, request, persist=False, environ={
                "BNL_MEMORY_LEDGER_SHADOW_ENABLED": "true",
                "BNL_MOMENT_ENGINE_SHADOW_ENABLED": "true",
                "BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED": "true",
                "BNL_RELATIONSHIP_V2_SHADOW_ENABLED": "true",
                "BNL_UNIFIED_INTELLIGENCE_PACKET_SHADOW_ENABLED": "true"})
        text = render_packet_context(packet, max_chars=700)[0]
        self.assertIn("Captured finalized gifts: 1 events, 13 units, 26 reported diamonds.", text)
        self.assertIn("not guaranteed final show totals", text)
        self.assertNotIn("originalSourceRefs", text)


if __name__ == "__main__":
    unittest.main()
