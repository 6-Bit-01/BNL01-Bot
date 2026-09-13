"""Show preparation stays under the show owner, with fresh original sources."""

import copy
import json
import os
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from dataclasses import replace
from pathlib import Path
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl01_bot as bot
import bnl_memory_ledger as memory
import bnl_moment_engine as moments
import bnl_tiktok_show_ledger as shows
from bnl_journal_source_store import record_source_event
from bnl_shared_brain_synthesis import render_packet_context
from bnl_unified_intelligence_packet import IntelligencePacketRequest, PacketConversationEvidence, build_packet, revalidate_packet
import test_tiktok_show_evidence_ledger as fixtures
from test_tiktok_show_evidence_ledger import archived_show, authorized_read_model, stamp
from test_show_interval_conversation import PACKET_ENV

QUERY = "What happened in preparation for the August 28, 2026 BARCODE Radio show?"
MIXED_REQUESTS = tuple(
    "For the August 28, 2026 BARCODE Radio show, " + text
    for text in (
        "what happened in preparation and during the session?",
        "connect preparation with what happened on air and how the session ended.",
        "connect what we sorted out beforehand with the on-air discussion and how we wrapped up.",
    )
)


class ShowPreparationTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.db = str(Path(temp.name) / "bnl.db")
        env = mock.patch.dict(os.environ, PACKET_ENV, clear=False)
        env.start()
        self.addCleanup(env.stop)
        fixtures.TikTokShowEvidenceLedgerTests().seed_source_and_memory(self.db)
        self.show = archived_show()
        self.show["milestones"].insert(0, {
            "sequence": 0, "eventType": "session_created",
            "occurredAt": "2026-08-28T23:30:00Z", "track": None,
        })

    def add_discord(self, row_id, text, *, at="2026-08-27T10:00:00Z", user_id=901,
                    policy="public_home", observe=False):
        with sqlite3.connect(self.db) as conn:
            conn.execute(
                """INSERT INTO conversations(id,user_id,user_name,guild_id,channel_name,
                   channel_policy,route_mode,role,content,timestamp,channel_id,message_id)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                (row_id, user_id, "Test Technician", 77, "show-room", policy,
                 "normal_chat", "user", text, at, 9001, row_id + 10000),
            )
            result = memory.shadow_conversation_row(
                conn, row_id=row_id, user_id=user_id, user_name="Test Technician",
                guild_id=77, role="user", content=text, channel_name="show-room",
                channel_policy=policy, route_mode="normal_chat", channel_id=9001,
                message_id=row_id + 10000, observed_at=at,
            )
            if observe:
                moments.observe_ledger_entry(conn, result.entry_id)
        return result.entry_id

    def add_tiktok(self, event_id, text, *, at="2026-08-28T23:45:00Z", metadata=None):
        result = record_source_event(
            self.db, guild_id=77, source_kind="tiktok_live_chat", source_key=event_id,
            occurred_at_ms=stamp(at), raw_text=text, subject_ref="tiktok_user:test-listener",
            private_display_name="Test Listener", channel_policy="public_context",
            public_usable=True, metadata=metadata or {"eventType": "comment", "roomId": "test-room"},
        )
        self.assertTrue(result.ok)

    def sync(self, more=()):
        return shows.sync_tiktok_show_evidence_ledgers(
            self.db, guild_id=77,
            read_model=authorized_read_model({"latestShow": self.show, "shows": list(more)}),
            environ=PACKET_ENV,
        )

    def parent(self):
        with sqlite3.connect(self.db) as conn:
            row = conn.execute(
                "SELECT ledger_json FROM tiktok_show_evidence_ledgers WHERE show_key=?",
                (self.show["sessionId"],),
            ).fetchone()
        return json.loads(row[0])

    def request(self):
        return IntelligencePacketRequest(
            guild_id=77, subject_user_id=0, channel_id=9001, channel_policy="public_home",
            route_mode="normal_chat", conversation_surface="mention_or_reply",
            visibility_allowance="public_safe", user_text=QUERY, direct_state="direct",
            now="2026-08-29T12:00:00-07:00",
        )

    def test_single_preflight_report_before_session_forms_linked_preparation(self):
        text = ("For the August 28, 2026 BARCODE Radio show, the preflight audio check passed. "
                "The left and right channels are balanced; the backup cable still needs replacing.")
        self.add_discord(2001, text)
        self.sync()
        parent = self.parent()
        prep = parent["preparationMoment"]
        self.assertEqual(prep["momentId"], self.show["sessionId"] + ":preparation")
        report = next(r for r in prep["messages"] if r["conversationRowId"] == 2001)
        self.assertEqual(report["text"], text)
        self.assertEqual(report["occurredAtMs"], stamp("2026-08-27T10:00:00Z"))
        self.assertEqual(report["associationReason"], "explicit_show_date")
        self.assertEqual(report["phase"], "pre_show")
        self.assertNotIn(report["eventId"], parent["coverage"]["sourceEventIds"])
        with sqlite3.connect(self.db) as conn:
            self.assertFalse(conn.execute(
                "SELECT 1 FROM sqlite_master WHERE name='memory_moment_windows'").fetchone())

    def test_all_captured_pre_show_chat_is_connected_without_tiktok_moment_admission(self):
        for index in range(26):
            self.add_tiktok(f"prep-{index}", f"Unaddressed room remark {index:02d}: blue curtains again.")
        self.add_discord(2001, "The kettle finally boiled.", at="2026-08-28T23:50:00Z")
        self.sync()
        prep = self.parent()["preparationMoment"]
        self.assertEqual(len([r for r in prep["messages"] if r["surface"] == "tiktok"]), 26)
        self.assertIn("The kettle finally boiled.", [r["text"] for r in prep["messages"]])
        self.assertTrue(all(r["associationReason"] == "session_time_context" for r in prep["messages"]))
        with sqlite3.connect(self.db) as conn:
            self.assertFalse(conn.execute(
                "SELECT 1 FROM sqlite_master WHERE name='memory_moment_windows'").fetchone())

    def test_unrelated_earlier_conversation_and_other_show_are_not_attached(self):
        self.add_discord(2001, "I checked the blue cables yesterday.")
        self.add_discord(2002, "For the August 29, 2026 BARCODE Radio show the blue cables passed.",
                         at="2026-08-28T23:45:00Z")
        self.sync()
        self.assertFalse(self.parent()["preparationMoment"]["messages"])

    def test_existing_discord_moment_keeps_identity_time_and_contributions(self):
        texts = (
            "For the August 28, 2026 BARCODE Radio show I checked the audio cables and speaker routing.",
            "I checked the audio cables and prefer the balanced speaker routing.",
            "We checked the audio cables and agreed the balanced speaker routing works.",
        )
        for index, text in enumerate(texts):
            self.add_discord(2001 + index, text,
                at=f"2026-08-27T10:00:0{index}+00:00", user_id=901 + index, observe=True)
        with sqlite3.connect(self.db) as conn:
            moments.sweep_expired_windows(conn, now="2026-08-27T10:03:00+00:00")
            before = conn.execute("SELECT * FROM memory_moment_windows").fetchall()
            mid = conn.execute(
                "SELECT moment_id FROM memory_moment_windows WHERE lifecycle_status='finalized'").fetchone()[0]
        self.sync()
        prep = self.parent()["preparationMoment"]
        self.assertEqual([m["momentId"] for m in prep["linkedDiscordMoments"]], [mid])
        self.assertEqual(len(prep["messages"]), 3)
        with sqlite3.connect(self.db) as conn:
            self.assertEqual(conn.execute("SELECT * FROM memory_moment_windows").fetchall(), before)
            gists = moments.select_public_situation_moment_gists(
                conn, guild_id=77, topic_text="audio cables speaker routing",
                allowed_channel_policies=("public_home",), token_budget=500)
            self.assertIn(mid, [g.moment_id for g in gists])

    def test_same_date_needs_exact_session_identity(self):
        self.add_discord(2001, "For the August 28, 2026 BARCODE Radio show the cable check passed.")
        self.add_discord(2002, "For show-attendance-1 the cable check passed.")
        second = copy.deepcopy(self.show)
        second["sessionId"] = "test-second-show"
        self.sync((second,))
        prep = self.parent()["preparationMoment"]
        self.assertEqual([r["conversationRowId"] for r in prep["messages"]], [2002])

    def test_raw_sources_survive_packet_render_and_native_recall(self):
        for index in range(26):
            self.add_tiktok(f"prep-{index}", f"Preparation remark {index:02d}: an ordinary room observation with no artist keyword.")
        self.sync()
        with sqlite3.connect(self.db) as conn:
            packet = build_packet(conn, self.request(), environ=PACKET_ENV)
            self.assertEqual(packet.diagnostics.invalid_invariants, [])
            rendered = render_packet_context(packet)[0]
            for index in range(26):
                self.assertIn(f"Preparation remark {index:02d}:", rendered)
            self.assertTrue(revalidate_packet(conn, packet, environ=PACKET_ENV).valid)
        selection = {}
        native = shows.build_tiktok_show_evidence_context(self.db, guild_id=77, user_text=QUERY, selection_out=selection)
        self.assertIn("Preparation remark 25:", native)
        basis = bot.build_finalized_show_prompt_source_basis(native, guild_id=77, selection=selection)
        self.assertIsNotNone(basis)
        self.assertEqual(sum(excerpt.source_text.startswith("Preparation remark ")
                             for excerpt in basis.authored_excerpts), 26)

    def test_combined_request_composes_preparation_moments_timeline_chat_and_conversation(self):
        texts = (
            "For the August 28, 2026 BARCODE Radio show I checked the audio cables and speaker routing.",
            "I checked the audio cables and prefer the balanced speaker routing.",
            "We checked the audio cables and agreed the balanced speaker routing works.",
        )
        for index, text in enumerate(texts):
            self.add_discord(2001 + index, text,
                at=f"2026-08-27T10:00:0{index}+00:00", user_id=901 + index, observe=True)
        self.add_tiktok("prep-kettle", "The kettle finally boiled.")
        self.add_tiktok("show-room-topic", "The room is talking about green lighting.", at="2026-08-29T00:02:00Z")
        with sqlite3.connect(self.db) as conn:
            moments.sweep_expired_windows(conn, now="2026-08-27T10:03:00+00:00")
            moment_id = conn.execute("SELECT moment_id FROM memory_moment_windows WHERE lifecycle_status='finalized'").fetchone()[0]
        self.sync()
        correction = "Please keep room banter separate from actual equipment-check results."
        self.add_discord(2010, correction, at="2026-08-29T12:00:00Z")
        query = ("Give me the August 28, 2026 BARCODE Radio show timeline: session start, submissions, "
                 "wheel spins, track starts and stops, removals, and show end. Include linked preparation "
                 "and what TikTok and Discord chat discussed during the show.")
        selection = {}
        native = shows.build_tiktok_show_evidence_context(self.db, guild_id=77, user_text=query, selection_out=selection)
        with mock.patch.multiple(bot, DB_FILE=self.db, BNL_PRIMARY_GUILD_ID=77):
            website = bot.build_bnl_read_model_context(
                fixtures.authorized_read_model({"latestShow": self.show}), query, "sealed_test")
        with sqlite3.connect(self.db) as conn:
            request = replace(self.request(), user_text=query, conversation_evidence=(
                PacketConversationEvidence(text=correction, source_id=2010, speaker_user_id=901, speaker_label="Test Technician"),))
            packet = build_packet(conn, request, persist=True, environ=PACKET_ENV)
            self.assertEqual(packet.diagnostics.invalid_invariants, [])
            rendered = render_packet_context(packet)[0]
            self.assertIn("conversation_context", {item.lane for item in packet.items})
            self.assertIn(correction, rendered)
            items = [item for item in packet.items if item.lane == "show_episode"]
            self.assertEqual(len({item.source_ref for item in items}), len(items))
            self.assertTrue({"show_linked_preparation", "scoped_show_conversation"} <= {item.usage for item in items})
            self.assertTrue(any(item.source_class == "first_party_record" for item in items))
            self.assertTrue(revalidate_packet(conn, packet, environ=PACKET_ENV).valid)
            conn.execute("UPDATE conversations SET content=? WHERE id=2001", ("The audio cables still need checking.",))
            self.assertFalse(revalidate_packet(conn, packet, environ=PACKET_ENV).valid)
        for reader in (native, website, rendered):
            self.assertIn("Show-linked preparation Moment:", reader)
            self.assertIn(moment_id, reader)
            self.assertIn("The kettle finally boiled.", reader)
            self.assertIn("[recorded operation] session_archived", reader)
            self.assertIn("The room is talking about green lighting.", reader)
            self.assertIn("window basis=recorded_show_timeline", reader)
        self.assertTrue(bot.finalized_show_packet_owner_requested(query, native))
        self.assertTrue(bot.finalized_show_packet_owner_requested(QUERY, native))

    def test_session_follow_on_composes_preparation_and_on_air_evidence(self):
        preparation = "We reserved the spare mixer for the August 28, 2026 BARCODE Radio show."
        on_air = "The room is talking about green lighting."
        self.add_discord(2001, preparation)
        self.add_tiktok("show-room-topic", on_air, at="2026-08-29T00:02:00Z")
        self.sync()
        model = authorized_read_model({"latestShow": self.show})
        requests = (
            "what happened in preparation and during the session?",
            "what happened in preparation and throughout the session?",
            "what happened in preparation and after the session?",
            "what did TikTok chat discuss in preparation and during the session?",
            "what happened in preparation and during the entire BARCODE Radio show?",
            "what happened in preparation and throughout the whole TikTok live stream?",
            "what happened in preparation and after yesterday's full radio session?",
            "connect preparation with what happened on air and how the session ended.",
            "connect what we sorted out beforehand with the on-air discussion and how we wrapped up.",
            "what did TikTok chat say during Neon Fox — First Signal, what preparation was linked, "
            "and how did the session end?",
        )
        for follow_on in requests:
            query = "For the August 28, 2026 BARCODE Radio show, " + follow_on
            native = shows.build_tiktok_show_evidence_context(self.db, guild_id=77, user_text=query)
            with mock.patch.multiple(bot, DB_FILE=self.db, BNL_PRIMARY_GUILD_ID=77):
                website = bot.build_bnl_read_model_context(model, query, "sealed_test")
            with sqlite3.connect(self.db) as conn:
                packet = build_packet(conn, replace(self.request(), user_text=query), environ=PACKET_ENV)
                rendered = render_packet_context(packet)[0]
                self.assertTrue(revalidate_packet(conn, packet, environ=PACKET_ENV).valid)
            for reader in (native, website, rendered):
                with self.subTest(follow_on=follow_on, reader=reader[:65]):
                    self.assertIn(preparation, reader)
                    self.assertIn(on_air, reader)
                    self.assertRegex(reader, r"session[_ ]archived")
        # The question determines the answer's focus. Available linked evidence
        # must not be removed to force a short answer; delivery is tested below.
        native = shows.build_tiktok_show_evidence_context(self.db, guild_id=77, user_text=QUERY)
        with mock.patch.multiple(bot, DB_FILE=self.db, BNL_PRIMARY_GUILD_ID=77):
            website = bot.build_bnl_read_model_context(model, QUERY, "sealed_test")
        with sqlite3.connect(self.db) as conn:
            packet = build_packet(conn, self.request(), environ=PACKET_ENV)
            rendered = render_packet_context(packet)[0]
        for reader in (native, website, rendered):
            self.assertIn(preparation, reader)
            self.assertIn(on_air, reader)

    def test_preparation_survives_ordinary_conversation_pruning(self):
        text = "For the August 28, 2026 BARCODE Radio show we reserved the spare mixer."
        self.add_discord(2001, text)
        self.sync()
        with sqlite3.connect(self.db) as conn:
            conn.execute("DELETE FROM conversations WHERE id=2001")
        self.sync()
        self.assertEqual(self.parent()["preparationMoment"]["messages"][0]["text"], text)
        native = shows.build_tiktok_show_evidence_context(self.db, guild_id=77, user_text=QUERY)
        self.assertIn(text, native)
        with mock.patch.multiple(bot, DB_FILE=self.db, BNL_PRIMARY_GUILD_ID=77):
            website = bot.build_bnl_read_model_context(
                authorized_read_model({"latestShow": self.show}), QUERY, "public_home")
        self.assertIn(text, website)

    def test_two_show_comparison_keeps_each_preparation_dialogue_and_chronology(self):
        second = json.loads(json.dumps(self.show).replace("2026-08-29", "2026-08-30")
                            .replace("2026-08-28", "2026-08-29"))
        second["sessionId"] = "test-second-show"
        evidence = []
        for index, day, air_day in ((0, "28", "29"), (1, "29", "30")):
            preparation = f"We reserved mixer {index} for the August {day}, 2026 BARCODE Radio show."
            discussion = f"The room is discussing lighting design {index}."
            self.add_discord(2001 + index, preparation)
            self.add_tiktok(f"compare-chat-{index}", discussion, at=f"2026-08-{air_day}T00:02:00Z")
            evidence.extend((preparation, discussion))
        self.sync((second,))
        query = "Compare the August 28, 2026 and August 29, 2026 BARCODE Radio shows, from preparation through the on-air lighting discussion and closing."
        model = authorized_read_model({"latestShow": second, "shows": [self.show]})
        with mock.patch.multiple(bot, DB_FILE=self.db, BNL_PRIMARY_GUILD_ID=77):
            website = bot.build_bnl_read_model_context(model, query, "public_home")
            # A resolved earlier-show follow-up must not acquire preparation
            # from the newest archive merely because its own words omit a date.
            followup = bot.build_bnl_read_model_context(
                model, "Connect what we sorted out beforehand with how it wrapped up.", "public_home",
                tiktok_show_analysis_request=MIXED_REQUESTS[2],
            )
        self.assertIn(evidence[0], followup)
        self.assertNotIn(evidence[2], followup)
        for text in evidence:
            self.assertIn(text, website)
        # A fresh original-quote view supersedes the matching website show
        # block, including its preparation, without discarding the other show.
        replaced = website.for_original_quote_lookup((self.show["sessionId"],))
        self.assertNotIn(evidence[0], replaced)
        self.assertIn(evidence[2], replaced)
        with sqlite3.connect(self.db) as conn:
            packet = build_packet(conn, replace(self.request(), user_text=query), environ=PACKET_ENV)
            rendered = render_packet_context(packet)[0]
            for text in evidence:
                self.assertIn(text, rendered)
            items = [item for item in packet.items if item.lane == "show_episode"]
            self.assertEqual(len(items), 4)
            operations = next(item for item in items if item.usage == "authoritative_show_chronology")
            self.assertEqual(operations.source_class, "first_party_record")
            self.assertEqual(operations.text.count("[session archived]"), 2)
            self.assertTrue(revalidate_packet(conn, packet, environ=PACKET_ENV).valid)
            conn.execute("UPDATE conversations SET content='The mixer is unavailable.' WHERE id=2002")
            self.assertFalse(revalidate_packet(conn, packet, environ=PACKET_ENV).valid)

    def test_metadata_session_link_is_retained_before_session_creation(self):
        self.add_tiktok("early-preflight", "The spare mixer is reserved.",
                        at="2026-08-20T10:00:00Z", metadata={"sessionId": self.show["sessionId"]})
        self.sync()
        self.sync()
        prep = self.parent()["preparationMoment"]
        self.assertEqual(prep["messages"][0]["text"], "The spare mixer is reserved.")
        self.assertEqual(prep["messages"][0]["associationReason"], "explicit_session")
        self.assertEqual(prep["messages"][0]["explicitSessionId"], self.show["sessionId"])
        self.assertIn("The spare mixer is reserved.",
                      shows.build_tiktok_show_evidence_context(self.db, guild_id=77, user_text=QUERY))

    def test_journal_copy_cannot_restore_private_or_edited_original(self):
        text = "For the August 28, 2026 BARCODE Radio show the spare mixer is reserved."
        self.add_discord(2001, text)
        result = record_source_event(
            self.db, guild_id=77, source_kind="discord_message", source_key="12001",
            occurred_at_ms=stamp("2026-08-27T10:00:00Z"), raw_text=text,
            subject_ref="discord_user:901", private_display_name="Test Technician",
            channel_policy="public_home", public_usable=True,
            metadata={"conversationRowId": 2001, "messageId": 12001},
        )
        self.assertTrue(result.ok)
        self.sync()
        self.assertEqual(len(self.parent()["preparationMoment"]["messages"]), 1)
        with sqlite3.connect(self.db) as conn:
            conn.execute("UPDATE conversations SET channel_policy='sealed_test' WHERE id=2001")
        self.sync()
        self.assertFalse(self.parent()["preparationMoment"]["messages"])
        with sqlite3.connect(self.db) as conn:
            conn.execute("UPDATE conversations SET channel_policy='public_home',content=? WHERE id=2001",
                         ("For the August 28, 2026 BARCODE Radio show the spare mixer is unavailable.",))
        self.sync()
        texts = [r["text"] for r in self.parent()["preparationMoment"]["messages"]]
        self.assertEqual(len(texts), 1)
        self.assertNotIn(text, texts)
        self.assertIn("is unavailable", texts[0])

    def test_legacy_journal_copy_keeps_original_retraction_authority(self):
        text = "For the August 28, 2026 BARCODE Radio show the spare mixer is reserved."
        root = self.add_discord(2001, text)
        result = record_source_event(
            self.db, guild_id=77, source_kind="discord_message", source_key="12001",
            occurred_at_ms=stamp("2026-08-27T10:00:00Z"), raw_text=text,
            subject_ref="discord_user:901", private_display_name="Test Technician",
            channel_policy="public_home", public_usable=True,
            metadata={"legacyTable": "conversations", "legacyRowId": 2001, "legacyMessageId": 12001},
        )
        self.assertTrue(result.ok)
        with sqlite3.connect(self.db) as conn:
            conn.execute("DELETE FROM conversations WHERE id=2001")
        self.sync()
        self.assertEqual(self.parent()["preparationMoment"]["messages"][0]["ledgerEntryId"], root)
        with sqlite3.connect(self.db) as conn:
            conn.execute("UPDATE memory_ledger_entries SET lifecycle_status='retracted',public_usable=0 WHERE entry_id=?", (root,))
        self.sync()
        self.assertFalse(self.parent()["preparationMoment"]["messages"])

    def test_malformed_source_metadata_does_not_hide_other_preparation(self):
        self.add_tiktok("invalid-prep", "Invalid source.", metadata={"conversationRowId": "invalid"})
        self.add_tiktok("valid-prep", "A usable preparation observation.")
        self.sync()
        prep = self.parent()["preparationMoment"]
        self.assertEqual([r["text"] for r in prep["messages"]], ["A usable preparation observation."])
        self.assertEqual(prep["coverage"]["invalid"], 1)
        self.assertFalse(prep["coverage"]["complete"])

    def test_exact_show_identity_selects_native_and_packet_preparation(self):
        self.add_discord(2001, "For show-attendance-1 the blue mixer is ready.")
        self.add_discord(2002, "For test-second-show the red mixer is ready.")
        second = copy.deepcopy(self.show)
        second["sessionId"] = "test-second-show"
        self.sync((second,))
        query = "What preparation happened for show-attendance-1?"
        native = shows.build_tiktok_show_evidence_context(self.db, guild_id=77, user_text=query)
        with sqlite3.connect(self.db) as conn:
            items = shows.select_tiktok_show_episode_context_items(conn, guild_id=77, user_text=query)
        packet_text = "\n".join(item.text for item in items)
        for rendered in (native, packet_text):
            self.assertIn("blue mixer", rendered)
            self.assertNotIn("red mixer", rendered)

    def test_complete_delete_covers_preparation_only_participant(self):
        from bnl_memory_governance import complete_delete_member_data
        self.add_discord(2001, "For the August 28, 2026 BARCODE Radio show we checked the mixer.")
        self.sync()
        with sqlite3.connect(self.db) as conn:
            result = complete_delete_member_data(conn, guild_id=77, user_id=901,
                                                confirmation="DELETE MY BNL DATA 77")
            self.assertTrue(result["ok"])
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM tiktok_show_evidence_ledgers").fetchone()[0], 0)

    def test_changed_private_original_cannot_resurrect_from_parent(self):
        text = "For the August 28, 2026 BARCODE Radio show we checked the mixer."
        self.add_discord(2001, text)
        self.sync()
        with sqlite3.connect(self.db) as conn:
            conn.execute("UPDATE conversations SET channel_policy='sealed_test' WHERE id=2001")
        self.sync()
        self.assertFalse(self.parent()["preparationMoment"]["messages"])

    def test_active_preparation_native_read_uses_accepted_snapshot_time(self):
        self.add_discord(2001, "The mixer is ready.", at="2026-08-28T23:50:00Z")
        show = copy.deepcopy(self.show)
        show["status"] = "prepared"
        show["milestones"] = [show["milestones"][0]]
        model = authorized_read_model({"currentShow": show})
        model["sections"]["queue"] = {"available": True, "session": {
            "id": show["sessionId"], "status": "prepared", "broadcastPhase": "warmup"}}
        with mock.patch.multiple(bot, DB_FILE=self.db, BNL_PRIMARY_GUILD_ID=77,
                _bnl_read_model_cached_at=datetime(2026, 8, 28, 23, 55, tzinfo=timezone.utc)):
            context = bot.build_bnl_read_model_context(model, "What is our preflight status?", "public_home")
        self.assertIn("The mixer is ready.", context)
        self.assertIn("precede on-air playback", context)

    def test_source_correction_invalidates_existing_packet_without_stale_parent_fallback(self):
        text = "For the August 28, 2026 BARCODE Radio show the preflight passed."
        root = self.add_discord(2001, text)
        self.sync()
        with sqlite3.connect(self.db) as conn:
            packet = build_packet(conn, self.request(), environ=PACKET_ENV)
            self.assertIn(text, render_packet_context(packet)[0])
            conn.execute("UPDATE memory_ledger_entries SET lifecycle_status='retracted',public_usable=0 WHERE entry_id=?", (root,))
            self.assertFalse(revalidate_packet(conn, packet, environ=PACKET_ENV).valid)
            items = shows.select_tiktok_show_episode_context_items(conn, guild_id=77, user_text=QUERY)
            self.assertNotIn(text, "\n".join(item.text for item in items))

    def test_private_preflight_does_not_enter_public_show(self):
        self.add_discord(2001, "For the August 28, 2026 BARCODE Radio show private preflight.", policy="sealed_test")
        self.sync()
        self.assertFalse(self.parent()["preparationMoment"]["messages"])

    def test_parent_refresh_is_idempotent_and_preserves_show_counts(self):
        self.add_tiktok("prep-a", "The room is getting ready.")
        first = self.sync()
        before = self.parent()
        second = self.sync()
        self.assertEqual(self.parent(), before)
        self.assertEqual(second["showsWritten"], 0)
        self.assertEqual(first["sourceEvents"], second["sourceEvents"])

    def test_explicit_preparation_has_no_artificial_day_lookback(self):
        self.add_discord(2001, "For the August 28, 2026 BARCODE Radio show we reserved the spare mixer.",
                         at="2026-07-01T10:00:00Z")
        self.sync()
        self.assertEqual(len(self.parent()["preparationMoment"]["messages"]), 1)

    def test_source_limits_are_visible_instead_of_claiming_full_coverage(self):
        self.add_tiktok("prep-a", "A source observation.")
        self.sync()
        with sqlite3.connect(self.db) as conn, mock.patch.object(shows, "TIKTOK_SHOW_EVIDENCE_MAX_SOURCE_EVENTS", 1):
            view = shows._show_preparation_view(conn, guild_id=77, ledger=self.parent())
        self.assertFalse(view["coverage"]["complete"])
        self.assertIn("journal_source_rows", view["coverage"]["limited"])
        self.assertIn("not proof of complete platform capture", shows._render_show_preparation(view))


class LightShowAwarenessTests(unittest.TestCase):
    def setUp(self):
        env = mock.patch.dict(os.environ, {"BNL_QUEUE_PRODUCTION_ENABLED": "true"})
        env.start()
        self.addCleanup(env.stop)

    def model(self, *, phase="broadcast_active", status="open", private=False):
        return {
            "ok": True, "version": 1, "source": "barcode-network-site",
            "publicOnly": not private, "accessScope": "private" if private else "public",
            "capabilities": {"queueProduction": True},
            "sections": {"queue": {
                "available": True, "accessScope": "private" if private else "public",
                "session": {"id": "test-show", "title": "Test Broadcast", "showDate": "2026-09-12",
                            "status": status, "broadcastPhase": phase},
                "queueUrl": "https://example.test/queue",
                "nowPlaying": {"artist": "Test Artist", "title": "Test Track",
                               "publicSourceUrl": "https://example.test/song",
                               "uploadedFileUrl": "https://private.example.test/upload"},
            }},
        }

    def test_ordinary_conversation_gets_small_current_context_from_existing_owner(self):
        with mock.patch.object(bot, "BNL_PRIMARY_GUILD_ID", 77), \
             mock.patch.object(bot, "_bnl_read_model_cache", self.model()), \
             mock.patch.object(bot, "BNL_QUEUE_PRODUCTION_ENABLED", True), \
             mock.patch.object(bot, "fetch_bnl_read_model", return_value=self.model()) as fetch:
            context = bot.maybe_build_bnl_read_model_context(
                "I love this song.", "public_home", guild_id=77)
        fetch.assert_called_once_with()
        self.assertIn("Test Artist", context)
        self.assertIn("https://example.test/song", context)
        self.assertNotIn("private.example", context)
        self.assertLess(len(context), 1500)
        self.assertFalse(bot._sealed_test_queue_response_required("I love this song.", "sealed_test"))

    def test_ordinary_public_reply_keeps_capture_policy(self):
        with mock.patch.object(bot, "BNL_QUEUE_PRODUCTION_ENABLED", True):
            context = bot.build_light_show_awareness(self.model(), "public_home")
        self.assertTrue(context)
        self.assertTrue(bot.model_response_persistence_allowed_with_website_context(
            "I love this song.", "public_home", context))
        self.assertFalse(bot.model_response_persistence_allowed_with_website_context(
            "I love this song.", "public_home", str(context)))

    def test_no_cross_guild_or_private_room_background_fetch(self):
        with mock.patch.object(bot, "BNL_PRIMARY_GUILD_ID", 77), \
             mock.patch.object(bot, "fetch_bnl_read_model") as fetch:
            for guild, policy in ((88, "public_home"), (77, "private"), (0, "public_home")):
                self.assertEqual(bot.maybe_build_bnl_read_model_context(
                    "I like these colors.", policy, guild_id=guild), "")
            fetch.assert_not_called()

    def test_inactive_private_and_unavailable_sources_do_not_activate_nudge(self):
        with mock.patch.object(bot, "BNL_QUEUE_PRODUCTION_ENABLED", True):
            for model in ({}, self.model(phase="ended", status="archived"), self.model(private=True)):
                self.assertEqual(bot.build_light_show_awareness(model, "public_home"), "")

    def test_preparation_is_not_described_as_playing_and_does_not_require_live_phase(self):
        with mock.patch.object(bot, "BNL_QUEUE_PRODUCTION_ENABLED", True):
            context = bot.build_light_show_awareness(self.model(phase="warmup", status="prepared"), "public_home")
        self.assertTrue(context)
        self.assertNotIn("Now playing:", context)
        self.assertIn("Preparation is not on-air", context)
        self.assertIn("corrections take precedence", context)

    def test_stale_cache_is_not_used_to_keep_show_active(self):
        from test_read_model_refresh_recovery import Response
        real_now = datetime.now(timezone.utc)
        with mock.patch.multiple(bot, BNL_READ_MODEL_ENABLED=True, BNL_QUEUE_PRODUCTION_ENABLED=True,
                BNL_PRIMARY_GUILD_ID=77, BNL_READ_MODEL_URL="https://example.test/read-model", BNL_API_KEY="",
                _bnl_read_model_cache=None, _bnl_read_model_cached_at=None, _bnl_read_model_cache_scope=None), \
             mock.patch.object(bot.urllib.request, "urlopen", return_value=Response(self.model())) as http:
            bot.fetch_bnl_read_model(force=True)
            self.assertTrue(bot.maybe_build_bnl_read_model_context("These colors look good.", "public_home", guild_id=77))
            bot._bnl_read_model_cached_at = real_now - timedelta(seconds=30)
            http.side_effect = TimeoutError("fixture timeout")
            self.assertEqual(bot.maybe_build_bnl_read_model_context("These colors look good.", "public_home", guild_id=77), "")


class ShowCompositionDeliveryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        import test_public_network_knowledge as network

        self.real_generate = bot.get_gemini_response
        self.real_website = bot.maybe_build_bnl_read_model_context
        self.network = network.PublicNetworkKnowledgeTests()
        await self.network.asyncSetUp()
        self.addAsyncCleanup(self.network.asyncTearDown)
        self.network._seed_finalized_show()
        self.sources = ShowPreparationTests()
        self.sources.db = bot.DB_FILE
        self.sources.show = archived_show()
        self.sources.show["milestones"].insert(0, {
            "sequence": 0, "eventType": "session_created",
            "occurredAt": "2026-08-28T23:30:00Z", "track": None,
        })
        self.preparation = "We reserved the spare mixer for the August 28, 2026 BARCODE Radio show."
        self.on_air = "The room is talking about green lighting."
        self.sources.add_discord(2001, self.preparation)
        self.sources.add_tiktok("composition-room-topic", self.on_air, at="2026-08-29T00:02:00Z")
        self.sources.sync()
        self.model = authorized_read_model({"latestShow": self.sources.show})
        self.network.stack.enter_context(mock.patch.dict(os.environ, {
            **PACKET_ENV,
            "BNL_UNIFIED_RESPONSE_ASSESSMENT_SHADOW_ENABLED": "true",
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_ENABLED": "true",
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_PUBLIC_ENABLED": "true",
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_GUILD_IDS": "77",
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_USER_IDS": "42",
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_CHANNEL_IDS": "8810",
        }))
        self.network.stack.enter_context(mock.patch.object(bot, "BNL_PRIMARY_GUILD_ID", 77))
        # Keep the affected show readers, packet assembly, provider preparation,
        # refresh and send real. Website HTTP, Gemini and Discord are fixtures.
        self.network.stack.enter_context(mock.patch.object(bot, "fetch_bnl_read_model", return_value=self.model))
        self.network.stack.enter_context(mock.patch.object(bot, "maybe_build_bnl_read_model_context", new=self.real_website))
        self.network.stack.enter_context(mock.patch.object(bot, "check_quota_availability", return_value=True))

    def assert_composed(self, prompt):
        self.assertIn(self.preparation, prompt)
        self.assertIn(self.on_air, prompt)
        self.assertRegex(prompt, r"session[_ ]archived")
        self.assertIn("Show-linked preparation Moment:", prompt)
        self.assertIn("Recorded BARCODE Radio chronology", prompt)
        self.assertNotIn("This private row must never enter", prompt)

    def provider(self, answer):
        async def generate(contents, route, *, attempt_counter=None):
            if attempt_counter is not None:
                attempt_counter.mark_started()
            return bot.GenerationResult(True, answer, route=route)
        return mock.AsyncMock(side_effect=generate)

    async def test_equivalent_mixed_requests_reach_actual_direct_and_batch_provider_inputs(self):
        answer = "The spare mixer was reserved beforehand, the room discussed green lighting on air, and the session was archived."
        for policy in ("public_home", "sealed_test"):
            for request in MIXED_REQUESTS:
                for route in ("direct", "batch"):
                    with self.subTest(policy=policy, request=request, route=route):
                        provider = self.provider(answer)
                        channel_id = 8810 if route == "direct" else 8811 + len(self.network.channel_ids)
                        with mock.patch.object(bot, "_generate_gemini_content_result_async", new=provider), \
                             mock.patch.dict(os.environ, {"BNL_ORDINARY_CHAT_SINGLE_PACKET_CHANNEL_IDS": str(channel_id)}):
                            if route == "direct":
                                prompt, metadata = await self.network._direct_prompt_async(policy, request=request)
                                self.assertTrue(metadata["ordinary_chat_single_packet_applied"])
                                basis = metadata["ordinary_chat_single_packet_basis"]
                                self.assertIsNotNone(basis)
                                execution = await bot.maybe_generate_ordinary_chat_single_packet(
                                    channel=None, prompt=prompt, basis=basis,
                                    scope_applied=True,
                                    preflight_reason=metadata["ordinary_chat_single_packet_preflight_reason"],
                                    situation_frame=metadata["situation_frame_shadow"],
                                    situation_frame_current_text=request,
                                    route_mode="normal_chat", channel_policy=policy,
                                    conversation_surface=metadata["situation_frame_shadow"].conversation_surface,
                                    user_id=42, guild_id=77, user_display_name="Test Member",
                                    source_context_available=metadata["source_context_available"],
                                    prompt_source_bases=metadata["prompt_source_bases"],
                                )
                                self.assertIsNotNone(execution)
                                self.assertEqual(execution.response, answer)
                            else:
                                channel, generation, _guard = await self.network._batch(
                                    policy, request=request, answer=self.real_generate,
                                )
                                generation.assert_awaited_once()
                                self.assertEqual(channel.sent, [answer])
                        provider.assert_awaited_once()
                        self.assert_composed(provider.await_args.args[0])

    async def test_simple_preparation_question_delivers_one_sentence_without_reciting_other_layers(self):
        answer = "The spare mixer was reserved."
        provider = self.provider(answer)
        with mock.patch.object(bot, "_generate_gemini_content_result_async", new=provider), \
             mock.patch.dict(os.environ, {"BNL_ORDINARY_CHAT_SINGLE_PACKET_CHANNEL_IDS": "8811"}):
            channel, generation, _guard = await self.network._batch(
                "public_home", request=QUERY, answer=self.real_generate,
            )
        generation.assert_awaited_once()
        provider.assert_awaited_once()
        self.assert_composed(provider.await_args.args[0])
        self.assertEqual(channel.sent, [answer])


if __name__ == "__main__":
    unittest.main()
