import json
import os
import sqlite3
import tempfile
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl01_bot
import bnl_journal
from bnl_tiktok_live_chat import LiveChatAdapter, LiveChatBuffer
from bnl_tiktok_live_context import LiveContextSnapshotWriter
from bnl_tiktok_live_memory import TikTokPublicConversationSpoolWriter


class Clock:
    def __init__(self, value=None):
        self.value = float(value if value is not None else time.time())

    def __call__(self):
        return self.value


def lifecycle_payload(event_type, clock, room_id="room-1"):
    return {
        "schema_version": 1,
        "event_type": event_type,
        "event_id": f"{event_type}-1",
        "room_id": room_id,
        "observed_at": clock(),
    }


def observation_payload(event_type, event_id, clock, **changes):
    payload = {
        "schema_version": 1,
        "event_type": event_type,
        "event_id": event_id,
        "room_id": "room-1",
        "observed_at": clock(),
        "source_at": clock(),
        "unique_id": "test.viewer",
        "display_name": "Test Viewer",
        "moderator_flag": False,
    }
    payload.update(changes)
    return payload


def public_read_model():
    return {
        "ok": True,
        "version": 1,
        "schemaRevision": "1.8",
        "source": "barcode-network-site",
        "publicOnly": True,
        "accessScope": "public",
        "capabilities": {"queueProduction": True},
        "sections": {
            "sourceContext": [],
            "queue": {
                "available": True,
                "accessScope": "public",
                "queueUrl": "https://www.barcode-network.com/queue",
                "revision": 42,
                "session": {
                    "title": "BARCODE Radio",
                    "purpose": "live_broadcast",
                    "status": "open",
                    "queueOpen": False,
                    "broadcastPhase": "live",
                },
                "status": {"activeCount": 4, "completedCount": 2, "capacity": 44},
                "nowPlaying": {
                    "id": "track-1",
                    "submittedArtistName": "6 Bit",
                    "submittedSongTitle": "Training Module One",
                    "queuePosition": None,
                },
                "upNext": {
                    "id": "track-2",
                    "submittedArtistName": "Test Artist",
                    "submittedSongTitle": "Next Signal",
                    "queuePosition": 1,
                },
                "queue": [{
                    "id": "track-3",
                    "submittedArtistName": "Later Artist",
                    "submittedSongTitle": "Do Not Dump Me",
                    "queuePosition": 2,
                }],
                "recentEvents": [{
                    "eventType": "track_play_started",
                    "occurredAt": "2026-08-28T22:00:00.000Z",
                    "track": {
                        "trackId": "track-1",
                        "artist": "6 Bit",
                        "title": "Training Module One",
                    },
                }],
            },
            "artists": [],
            "dossiers": [],
            "rules": [],
        },
    }


def public_read_model_with_show_archive():
    model = public_read_model()
    model["sections"]["archive"] = {
        "available": True,
        "currentShow": {
            "sessionId": "show-1",
            "title": "BARCODE Radio",
            "showDate": "2026-08-28",
            "status": "open",
            "milestones": [
                {"sequence": 1, "eventType": "broadcast_started", "occurredAt": "2026-08-29T00:00:00Z", "track": None},
                {"sequence": 2, "eventType": "track_loaded", "occurredAt": "2026-08-29T00:01:00Z", "track": {"projectLabel": "First Artist", "title": "First Track"}},
                {"sequence": 3, "eventType": "track_finished", "occurredAt": "2026-08-29T00:04:00Z", "track": {"projectLabel": "First Artist", "title": "First Track"}},
                {"sequence": 4, "eventType": "track_loaded", "occurredAt": "2026-08-29T00:04:00Z", "track": {"projectLabel": "Winning Artist", "title": "Winning Track"}},
                {"sequence": 5, "eventType": "track_finished", "occurredAt": "2026-08-29T00:08:00Z", "track": {"projectLabel": "Winning Artist", "title": "Winning Track"}},
                {"sequence": 6, "eventType": "session_archived", "occurredAt": "2026-08-29T00:09:00Z", "track": None},
            ],
        },
        "latestShow": None,
        "shows": [],
    }
    return model


class BNLLiveContextBridgeTests(unittest.TestCase):
    def make_adapter(self, clock):
        adapter = LiveChatAdapter(
            LiveChatBuffer(100, 600, clock),
            clock,
            clear_on_live_end=False,
        )
        adapter.ingest_line(json.dumps(lifecycle_payload("connected", clock)))
        adapter.ingest_line(json.dumps(observation_payload(
            "comment",
            "comment-1",
            clock,
            comment_text="This track is wild.",
        )))
        adapter.ingest_line(json.dumps(observation_payload(
            "viewer_snapshot",
            "view-1",
            clock,
            unique_id="",
            display_name="",
            viewer_count=37,
        )))
        return adapter

    def test_public_live_reaction_combines_queue_truth_and_tiktok_reaction(self):
        clock = Clock()
        adapter = self.make_adapter(clock)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "live-context.json"
            LiveContextSnapshotWriter(str(path), time_fn=clock).publish(adapter, force=True)
            with mock.patch.dict(os.environ, {"BNL_QUEUE_PRODUCTION_ENABLED": "true"}, clear=False), \
                 mock.patch.object(bnl01_bot, "BNL_TIKTOK_LIVE_CONTEXT_ENABLED", True), \
                 mock.patch.object(bnl01_bot, "BNL_TIKTOK_LIVE_CONTEXT_PATH", str(path)), \
                 mock.patch.object(bnl01_bot, "BNL_TIKTOK_LIVE_CONTEXT_MAX_AGE_SECONDS", 20.0):
                context = bnl01_bot.build_bnl_read_model_context(
                    public_read_model(),
                    "How is TikTok chat reacting to the show?",
                    "public_home",
                )
                exact_live_chat_context = bnl01_bot.build_bnl_read_model_context(
                    public_read_model(),
                    "What did TikTok chat just say?",
                    "public_home",
                )

        self.assertIn("Now playing: 6 Bit — Training Module One", context)
        self.assertIn("track_play_started", context)
        self.assertIn("This track is wild.", context)
        self.assertNotIn("Do Not Dump Me", context)
        self.assertIn("queue snapshot as authoritative show state", context)
        self.assertIn("above Community Canon", context)
        self.assertIn("This track is wild.", exact_live_chat_context)
        self.assertIn(
            "Current TikTok LIVE public reaction context",
            exact_live_chat_context,
        )

    def test_private_queue_scope_cannot_feed_public_live_reaction_context(self):
        model = public_read_model()
        model["publicOnly"] = False
        model["accessScope"] = "private"
        model["sections"]["queue"]["accessScope"] = "private"
        with mock.patch.object(bnl01_bot, "BNL_TIKTOK_LIVE_CONTEXT_ENABLED", True):
            context = bnl01_bot.build_bnl_read_model_context(
                model,
                "What is TikTok chat saying?",
                "public_home",
            )
        self.assertNotIn("Training Module One", context)
        self.assertNotIn("This track is wild", context)
        self.assertIn("does not authorize live-show context", context)

    def test_plain_queue_question_does_not_load_tiktok_context(self):
        with mock.patch.dict(os.environ, {"BNL_QUEUE_PRODUCTION_ENABLED": "true"}, clear=False), mock.patch.object(
            bnl01_bot,
            "build_live_prompt_context",
            wraps=bnl01_bot.build_live_prompt_context,
        ) as live_context:
            context = bnl01_bot.build_bnl_read_model_context(
                public_read_model(),
                "What's playing right now?",
                "public_home",
            )
        self.assertIn("Training Module One", context)
        live_context.assert_not_called()

    def test_post_show_question_uses_durable_archive_not_expired_live_buffer(self):
        question = "BNL, which songs tonight got the most TikTok chat engagement?"
        with tempfile.TemporaryDirectory() as directory:
            db_path = str(Path(directory) / "bnl.db")
            bnl01_bot.ensure_journal_source_schema(db_path)

            def stamp(value):
                return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000)

            messages = (
                ("first-1", "2026-08-29T00:02:00Z", "one", "First reaction."),
                ("win-1", "2026-08-29T00:04:30Z", "one", "Winning reaction one."),
                ("win-2", "2026-08-29T00:05:30Z", "two", "Winning reaction two."),
                ("win-3", "2026-08-29T00:06:30Z", "three", "Winning reaction three."),
            )
            for event_id, occurred_at, handle, text in messages:
                result = bnl01_bot.record_journal_source_event(
                    db_path,
                    guild_id=77,
                    source_kind="tiktok_live_chat",
                    source_key=event_id,
                    occurred_at_ms=stamp(occurred_at),
                    raw_text=text,
                    sanitized_summary=text,
                    channel_policy="public_context",
                    subject_ref=f"tiktok_handle:{handle}",
                    private_display_name=f"@{handle}",
                    public_usable=True,
                    metadata={"eventType": "comment", "handle": handle},
                )
                self.assertTrue(result.ok)

            with mock.patch.dict(os.environ, {"BNL_QUEUE_PRODUCTION_ENABLED": "true"}, clear=False), \
                 mock.patch.object(bnl01_bot, "DB_FILE", db_path), \
                 mock.patch.object(bnl01_bot, "BNL_PRIMARY_GUILD_ID", 77), \
                 mock.patch.object(bnl01_bot, "BNL_TIKTOK_LIVE_CONTEXT_PATH", "/missing-live-context"):
                context = bnl01_bot.build_bnl_read_model_context(
                    public_read_model_with_show_archive(),
                    question,
                    "public_home",
                )

        self.assertIn("Durable TikTok show analysis context", context)
        self.assertIn("1. Winning Artist — Winning Track: 3 messages", context)
        self.assertIn("2. First Artist — First Track: 1 messages", context)
        self.assertNotIn("snapshot_missing", context)
        self.assertNotIn("live TikTok reaction data is not currently available", context)
        self.assertTrue(
            bnl01_bot.public_tiktok_interaction_memory_allowed(
                question,
                "public_home",
                context,
            )
        )

    def test_post_show_question_reports_durable_archive_read_failure_honestly(self):
        question = "BNL, which songs tonight got the most TikTok chat engagement?"
        with mock.patch.dict(os.environ, {"BNL_QUEUE_PRODUCTION_ENABLED": "true"}, clear=False), \
             mock.patch.object(bnl01_bot, "DB_FILE", "/missing-bnl-archive.db"), \
             mock.patch.object(bnl01_bot, "BNL_PRIMARY_GUILD_ID", 77):
            context = bnl01_bot.build_bnl_read_model_context(
                public_read_model_with_show_archive(),
                question,
                "public_home",
            )
        self.assertIn("durable TikTok event archive could not be read", context)
        self.assertIn("Do not report zero engagement", context)
        self.assertNotIn("No durable public TikTok comments", context)

    def test_contextual_followup_reloads_durable_chat_and_ignores_prior_bnl_claims(self):
        initial_question = "BNL, which songs tonight got the most TikTok chat engagement?"
        followup = "Awesome. Any recurring topics or anything of note?"
        room_context = (
            "Recent room context from this channel:\n"
            f"User/member (display name “6 Bit”): {initial_question}\n"
            "BNL-01: The room discussed imaginary mercury organs.\n"
            f"User/member (current payload fragment): {followup}"
        )
        resolved = bnl01_bot.resolve_tiktok_show_analysis_request(
            followup,
            room_context,
        )
        self.assertIn(initial_question, resolved)
        self.assertIn(f"Current follow-up: {followup}", resolved)
        self.assertNotIn("mercury organs", resolved)

        with tempfile.TemporaryDirectory() as directory:
            db_path = str(Path(directory) / "bnl.db")
            bnl01_bot.ensure_journal_source_schema(db_path)

            def stamp(value):
                return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000)

            messages = (
                ("topic-1", "2026-08-29T00:02:00Z", "one", "The green visuals are wild."),
                ("topic-2", "2026-08-29T00:03:00Z", "two", "Those green visuals look incredible."),
                ("topic-3", "2026-08-29T00:05:00Z", "three", "The green visuals changed again."),
                ("isolated", "2026-08-29T00:06:00Z", "four", "Wheel chaos tonight."),
            )
            for event_id, occurred_at, handle, text in messages:
                result = bnl01_bot.record_journal_source_event(
                    db_path,
                    guild_id=77,
                    source_kind="tiktok_live_chat",
                    source_key=event_id,
                    occurred_at_ms=stamp(occurred_at),
                    raw_text=text,
                    sanitized_summary=text,
                    channel_policy="public_context",
                    subject_ref=f"tiktok_handle:{handle}",
                    private_display_name=f"@{handle}",
                    public_usable=True,
                    metadata={"eventType": "comment", "handle": handle},
                )
                self.assertTrue(result.ok)

            with mock.patch.dict(os.environ, {"BNL_QUEUE_PRODUCTION_ENABLED": "true"}, clear=False), \
                 mock.patch.object(bnl01_bot, "DB_FILE", db_path), \
                 mock.patch.object(bnl01_bot, "BNL_PRIMARY_GUILD_ID", 77), \
                 mock.patch.object(
                     bnl01_bot,
                     "fetch_bnl_read_model",
                     return_value=public_read_model_with_show_archive(),
                 ):
                context = bnl01_bot.maybe_build_bnl_read_model_context(
                    followup,
                    "public_home",
                    conversation_context=room_context,
                )

        self.assertIn("Durable TikTok show analysis context", context)
        self.assertIn('"green visuals": 3 messages / 3 unique chatters', context)
        self.assertIn("The green visuals are wild.", context)
        self.assertIn("Wheel chaos tonight.", context)
        self.assertIn("BNL's earlier replies are not evidence", context)
        self.assertNotIn("mercury organs", context)
        self.assertTrue(
            bnl01_bot.public_tiktok_interaction_memory_allowed(
                followup,
                "public_home",
                context,
            )
        )

    def test_contextual_followup_does_not_self_anchor_or_jump_unrelated_human_turn(self):
        followup = "Why?"
        self.assertEqual(
            bnl01_bot.resolve_tiktok_show_analysis_request(
                followup,
                "BNL-01: TikTok chat was very active tonight.",
            ),
            "",
        )
        shifted_context = (
            "User/member: Which tracks had the most TikTok chat engagement tonight?\n"
            "BNL-01: The second track ranked first.\n"
            "User/member: What is the queue capacity?\n"
            "BNL-01: The capacity is 44."
        )
        self.assertEqual(
            bnl01_bot.resolve_tiktok_show_analysis_request(
                followup,
                shifted_context,
            ),
            "",
        )
        with mock.patch.object(bnl01_bot, "fetch_bnl_read_model") as fetch:
            context = bnl01_bot.maybe_build_bnl_read_model_context(
                followup,
                "public_home",
                conversation_context="BNL-01: What TikTok viewers discussed.",
            )
        self.assertEqual(context, "")
        fetch.assert_not_called()

    def test_public_tiktok_exchange_uses_normal_memory_but_queue_only_does_not(self):
        public_context = (
            "Website public read model context:\n"
            "Source: barcode-network-site / publicOnly=true / "
            "accessScope=public / version=1\n"
            "Current TikTok LIVE public reaction context:\n"
            "- @viewer: good track"
        )
        self.assertTrue(bnl01_bot.public_tiktok_interaction_memory_allowed(
            "What is TikTok chat saying?",
            "public_home",
            public_context,
        ))
        self.assertFalse(bnl01_bot.public_tiktok_interaction_memory_allowed(
            "What's playing?",
            "public_home",
            public_context,
        ))
        self.assertFalse(bnl01_bot.public_tiktok_interaction_memory_allowed(
            "What is TikTok chat saying?",
            "sealed_test",
            public_context,
        ))

    def test_spool_ingest_archives_pr0x_as_owner_and_feeds_surface_lore(self):
        clock = Clock()
        with tempfile.TemporaryDirectory() as directory:
            db_path = str(Path(directory) / "bnl.db")
            spool_path = str(Path(directory) / "public-conversation.ndjson")
            writer = TikTokPublicConversationSpoolWriter(spool_path)
            writer.append(observation_payload(
                "comment",
                "comment-owner-1",
                clock,
                unique_id="pr0x60",
                display_name="PR0X",
                moderator_flag=True,
                comment_text="The room is locked in tonight.",
            ))
            with mock.patch.object(bnl01_bot, "DB_FILE", db_path), \
                 mock.patch.object(bnl01_bot, "BNL_OWNER_USER_ID", 601), \
                 mock.patch.object(bnl01_bot, "BNL_TIKTOK_OWNER_HANDLES", ("pr0x60",)), \
                 mock.patch.object(bnl01_bot, "memory_ledger_shadow_enabled", return_value=True), \
                 mock.patch.object(bnl01_bot, "form_atomic_candidate_from_ledger_entry", return_value=None):
                result = bnl01_bot.ingest_tiktok_live_memory_once(
                    77,
                    path=spool_path,
                )

            self.assertTrue(result["ok"])
            self.assertEqual(result["ingested"], 1)
            with sqlite3.connect(db_path) as conn:
                source = conn.execute(
                    "SELECT source_kind,subject_ref,raw_text,metadata_json "
                    "FROM bnl_journal_source_events WHERE source_key=?",
                    ("comment-owner-1",),
                ).fetchone()
                ledger = conn.execute(
                    "SELECT source_table,subject_key,normalized_value,freshness "
                    "FROM memory_ledger_entries WHERE source_row_id=?",
                    ("comment-owner-1",),
                ).fetchone()

            self.assertEqual(source[0], "tiktok_live_chat")
            self.assertEqual(source[1], "discord_user:601")
            self.assertEqual(source[2], "The room is locked in tonight.")
            metadata = json.loads(source[3])
            self.assertEqual(
                metadata["identityBindingBasis"],
                "owner_declared_exact_tiktok_handle",
            )
            self.assertTrue(metadata["moderator"])
            self.assertEqual(ledger[0], "tiktok_live_chat")
            self.assertEqual(ledger[1], "discord_user:601")
            self.assertEqual(ledger[2], "The room is locked in tonight.")
            self.assertEqual(ledger[3], "surface_lore_input")

            start = datetime.fromtimestamp(clock() - 60, tz=timezone.utc).isoformat()
            end = datetime.fromtimestamp(clock() + 60, tz=timezone.utc).isoformat()
            packet = bnl_journal.build_source_packet_between(
                db_path,
                77,
                start,
                end,
                entry_kind="manual",
            )
            tiktok_sources = [
                source
                for source in packet.get("safeSources", [])
                if source.get("conversationSurface") == "tiktok_live_chat"
            ]
            self.assertEqual(len(tiktok_sources), 1)
            self.assertEqual(tiktok_sources[0]["sourceKind"], "conversation")


if __name__ == "__main__":
    unittest.main()
