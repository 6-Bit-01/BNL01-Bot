import hashlib
import json
import os
import sqlite3
import tempfile
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
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

    def test_whole_live_rundown_is_explicit_and_reloads_without_room_context(self):
        question = (
            "Give me a quick rundown on what people talked about "
            "throughout the live"
        )
        with mock.patch.dict(
            os.environ,
            {"BNL_QUEUE_PRODUCTION_ENABLED": "true"},
            clear=False,
        ), mock.patch.object(
            bnl01_bot,
            "fetch_bnl_read_model",
            return_value=public_read_model_with_show_archive(),
        ) as fetch, mock.patch.object(
            bnl01_bot,
            "_load_durable_tiktok_show_events",
            return_value=[],
        ) as archive_load:
            context = bnl01_bot.maybe_build_bnl_read_model_context(
                question,
                "public_home",
            )

        fetch.assert_called_once_with(force=True)
        archive_load.assert_called_once()
        self.assertIn("Analysis intent=chat_topics", context)
        self.assertIn("Full-archive coverage", context)
        self.assertNotIn("Now playing:", context)
        self.assertNotIn("Ranking by public chat messages", context)

    def test_historical_multisource_timeline_reloads_durable_show_owner(self):
        question = (
            "BNL, what happened during yesterday's BARCODE Radio show? "
            "Give me a chronological timeline using the show chat, queue "
            "events, tracks, and your Discord conversations."
        )
        with mock.patch.dict(
            os.environ,
            {"BNL_QUEUE_PRODUCTION_ENABLED": "true"},
            clear=False,
        ), mock.patch.object(
            bnl01_bot,
            "fetch_bnl_read_model",
            return_value=public_read_model_with_show_archive(),
        ) as fetch, mock.patch.object(
            bnl01_bot,
            "_load_durable_tiktok_show_events",
            return_value=[],
        ) as archive_load:
            context = bnl01_bot.maybe_build_bnl_read_model_context(
                question,
                "public_home",
            )

        fetch.assert_called_once_with(force=True)
        archive_load.assert_called_once()
        self.assertIn("Durable TikTok show analysis context", context)
        self.assertIn("Analysis intent=show_recap", context)

    def test_each_explicit_chat_analysis_turn_reloads_its_durable_owner(self):
        questions = (
            "Any recurring topics from TikTok chat during the show?",
            "Give me a quick rundown on what people talked about throughout the live",
        )
        with mock.patch.dict(
            os.environ,
            {"BNL_QUEUE_PRODUCTION_ENABLED": "true"},
            clear=False,
        ), mock.patch.object(
            bnl01_bot,
            "fetch_bnl_read_model",
            return_value=public_read_model_with_show_archive(),
        ) as fetch, mock.patch.object(
            bnl01_bot,
            "_load_durable_tiktok_show_events",
            return_value=[],
        ) as archive_load:
            contexts = [
                bnl01_bot.maybe_build_bnl_read_model_context(
                    question,
                    "public_home",
                )
                for question in questions
            ]

        self.assertEqual(fetch.call_count, 2)
        self.assertEqual(archive_load.call_count, 2)
        self.assertTrue(
            all("Durable TikTok show analysis context" in value for value in contexts)
        )

    def test_public_tiktok_reply_can_persist_without_storing_injected_context(self):
        durable_context = (
            "Website public read model context:\n"
            "Source: barcode-network-site / publicOnly=true / "
            "accessScope=public / version=1\n"
            "Durable TikTok show analysis context:\n"
            "- Analysis intent=chat_topics."
        )
        self.assertTrue(
            bnl01_bot.model_response_persistence_allowed_with_website_context(
                "Give me a rundown of what people discussed throughout the live",
                "public_home",
                durable_context,
            )
        )
        self.assertFalse(
            bnl01_bot.model_response_persistence_allowed_with_website_context(
                "What's playing right now?",
                "public_home",
                "Website public read model context:\naccessScope=public",
            )
        )

    def test_durable_tiktok_turn_contract_uses_room_context_only_as_framing(self):
        contract = bnl01_bot.build_tiktok_show_analysis_turn_contract(
            "Durable TikTok show analysis context:\n"
            "- Analysis intent=chat_topics."
        )
        self.assertIn("full eligible archive", contract)
        self.assertIn("cannot supply claims about what TikTok viewers said", contract)

    def test_sealed_show_continuity_preserves_private_and_transient_exclusions(self):
        public_context = (
            "Website public read model context:\n"
            "Source: barcode-network-site / publicOnly=true / accessScope=public / version=1\n"
            "Durable TikTok show analysis context:\n"
            "- Analysis intent=chat_topics."
        )
        request = "What stood out in the show conversation?"
        allowed = bnl01_bot.model_response_persistence_allowed_with_website_context
        self.assertTrue(allowed(request, "sealed_test", public_context))
        self.assertFalse(bnl01_bot.public_tiktok_interaction_memory_allowed(
            request, "sealed_test", public_context,
        ))
        self.assertFalse(allowed(request, "internal_controlled", public_context))
        for context in (
            public_context.replace("Website public read model context:",
                                   "Website private queue read model context:")
                          .replace("publicOnly=true", "publicOnly=false")
                          .replace("accessScope=public", "accessScope=private"),
            "Website private queue read model context:\naccessScope=private\n"
            "Untrusted source quotation: " + public_context,
            "Website public read model context:\naccessScope=public\n"
            "Current queue: submissions are open.",
        ):
            with self.subTest(context=context):
                self.assertFalse(allowed(request, "sealed_test", context))
        transient = type("TransientBasis", (), {"transient_referent_message_ids": (123,)})()
        self.assertFalse(allowed(request, "sealed_test", public_context,
                                 prompt_source_bases=(transient,)))

    def test_tiktok_topic_contract_preserves_source_authority_without_output_quota(self):
        prompt = (
            "Durable TikTok show analysis context:\n"
            "- Analysis intent=chat_topics.\n"
            "- Signal \"green visuals\": 3 messages / 3 unique chatters.\n"
            "  Support t+2.0m | First Track | @one: "
            "\"The green visuals are wild.\""
        )
        contract = bnl01_bot.build_tiktok_show_analysis_turn_contract(prompt)
        self.assertIn("full eligible archive", contract)
        self.assertIn("cannot supply claims about what TikTok viewers said", contract)
        self.assertNotIn("three to five", contract)

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


class BNLLiveContextGuardTests(unittest.IsolatedAsyncioTestCase):
    async def test_topic_evidence_allows_natural_synonyms_without_a_rewrite(self):
        prompt = (
            "Current user request: Any recurring topics from chat tonight?\n"
            "Durable TikTok show analysis context:\n"
            "- Analysis intent=chat_topics.\n"
            "- Signal \"green visuals\": 3 messages / 3 unique chatters.\n"
            "  Support t+2.0m | First Track | @one: "
            "\"The green visuals are wild.\""
        )
        answer = (
            "Three different viewers commented on the green look, "
            "one remark apiece. One described it as wild during First Track."
        )
        with mock.patch.object(
            bnl01_bot,
            "get_gemini_response_with_optional_typing",
            new=mock.AsyncMock(),
        ) as regenerate:
            response, diagnostics = (
                await bnl01_bot.apply_guarded_response_regeneration(
                    answer,
                    prompt=prompt,
                    user_id=1,
                    guild_id=77,
                    route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                    channel_policy="public_home",
                    current_user_text=(
                        "Any recurring topics from TikTok chat during the show?"
                    ),
                    source_context_available=True,
                )
            )

        self.assertEqual(response, answer)
        self.assertFalse(
            diagnostics["tiktok_show_analysis_guard_triggered"]
        )
        self.assertFalse(diagnostics["tiktok_show_analysis_regenerated"])
        self.assertEqual(diagnostics["tiktok_show_analysis_guard_reason"], "")
        self.assertFalse(diagnostics["suppressed"])
        regenerate.assert_not_awaited()

    async def test_direct_public_tiktok_reply_is_saved_as_conversation_not_snapshot(self):
        question = (
            "Give me a quick rundown on what people talked about throughout the live"
        )
        website_context = (
            "Website public read model context:\n"
            "Source: barcode-network-site / publicOnly=true / "
            "accessScope=public / version=1\n"
            "Durable TikTok show analysis context:\n"
            "- Analysis intent=chat_topics.\n"
            "- Signal \"green visuals\": 3 messages / 3 unique chatters."
        )
        sent = SimpleNamespace(id=991)
        channel = SimpleNamespace(id=81, name="barcode-bot")
        message = SimpleNamespace(
            content=question,
            author=SimpleNamespace(id=1, display_name="6 Bit"),
            guild=SimpleNamespace(id=77),
            channel=channel,
            reply=mock.AsyncMock(return_value=sent),
        )
        plan = bnl01_bot.plan_conversation_response(
            question,
            "public_home",
            route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
            active_channel=True,
            real_direct_target=True,
            batching_enabled=True,
        )
        save = mock.Mock(
            return_value=SimpleNamespace(
                save_conversation=True,
                reason="saved",
            )
        )
        with mock.patch.object(
            bnl01_bot,
            "_apply_direct_response_pacing",
            new=mock.AsyncMock(),
        ), mock.patch.object(
            bnl01_bot,
            "maybe_generate_shared_brain_synthesis_canary",
            new=mock.AsyncMock(return_value=None),
        ), mock.patch.object(
            bnl01_bot,
            "apply_guarded_response_regeneration",
            new=mock.AsyncMock(
                return_value=(
                    "Green visuals were the strongest recurring subject.",
                    {"suppressed": False},
                )
            ),
        ), mock.patch.object(
            bnl01_bot,
            "build_message_media_context",
            return_value={"present": False},
        ), mock.patch.object(
            bnl01_bot,
            "save_model_message",
            new=save,
        ), mock.patch.object(
            bnl01_bot,
            "_mark_conversation_continuation_state",
        ), mock.patch.object(
            bnl01_bot,
            "record_unified_response_assessment_shadow_after_send",
            new=mock.AsyncMock(),
        ):
            await bnl01_bot.send_planned_conversation_response(
                message,
                "Green visuals were the strongest recurring subject.",
                plan,
                website_read_model_context=website_context,
                source_context_available=True,
                prompt=website_context,
                mark_recent_direct=False,
            )

        save.assert_called_once()
        self.assertEqual(
            save.call_args.args[2],
            "Green visuals were the strongest recurring subject.",
        )
        self.assertNotIn(
            "Durable TikTok show analysis context",
            save.call_args.args[2],
        )



class BnlShowWordFrequencyIntegrationTests(unittest.TestCase):
    """The ordinary context builder supplies originals to the frequency owner."""

    def setUp(self):
        from tests.test_tiktok_show_evidence_ledger import authorized_read_model

        self.authorized_read_model = authorized_read_model
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.db_path = str(Path(directory.name) / "frequency.db")
        bnl01_bot.ensure_journal_source_schema(self.db_path)
        self.observed_at = datetime(2026, 10, 3, 4, 18, 14, tzinfo=timezone.utc)
        self.current = self._show("current-frequency", "2026-10-02", "2026-10-03", active=True)
        self.latest = self._show("latest-frequency", "2026-09-25", "2026-09-26")
        self.older = self._show("older-frequency", "2026-09-18", "2026-09-19")
        for event_id, instant, text in (
            ("current-pre", "2026-10-03T02:00:00Z", "panda " * 20),
            ("current-one", "2026-10-03T02:06:00Z", "Panda panda!"),
            ("current-neutral", "2026-10-03T02:08:00Z", "Good drums"),
            ("current-two", "2026-10-03T03:40:00Z", "PANDA!"),
            ("current-three", "2026-10-03T04:05:00Z", "panda"),
            ("current-after-cutoff", "2026-10-03T04:19:00Z", "panda " * 50),
            ("latest-one", "2026-09-26T03:00:00Z", "panda " * 8),
            ("older-one", "2026-09-19T03:00:00Z", "panda " * 31),
        ):
            result = bnl01_bot.record_journal_source_event(
                self.db_path, guild_id=77, source_kind="tiktok_live_chat",
                source_key=event_id, occurred_at_ms=self._stamp(instant),
                ingested_at_ms=self._stamp(instant),
                raw_text=text, sanitized_summary=text, channel_policy="public_context",
                subject_ref="tiktok_handle:" + event_id, private_display_name="@" + event_id,
                public_usable=True, metadata={"eventType": "comment", "handle": event_id},
            )
            self.assertTrue(result.ok)

    @staticmethod
    def _stamp(value):
        return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000)

    @staticmethod
    def _show(session_id, day, next_day, *, active=False):
        milestones = [
            {"eventType": "broadcast_started", "occurredAt": next_day + "T02:05:35.254Z"},
            {"eventType": "track_loaded", "occurredAt": next_day + "T03:00:00Z"},
        ]
        if not active:
            milestones.append({"eventType": "session_archived", "occurredAt": next_day + "T08:08:03.054Z"})
        return {
            "sessionId": session_id, "title": "BARCODE Radio", "showDate": day,
            "status": "open" if active else "archived", "milestones": milestones,
        }

    def _model(self, *, include_current=True):
        archive = {"latestShow": self.latest, "shows": [self.latest, self.older]}
        if include_current:
            archive["currentShow"] = self.current
        model = self.authorized_read_model(archive)
        if include_current:
            # The real bot freezes an active window only when the queue and
            # archive identify the same currently broadcasting session.
            model["sections"]["queue"] = {
                "available": True, "accessScope": "public",
                "session": {
                    "id": self.current["sessionId"], "showDate": self.current["showDate"],
                    "title": "BARCODE Radio", "status": "open", "broadcastPhase": "live",
                },
            }
        return model

    def _empty_source_database(self, label):
        # Each immutable source receipt is created once in a fresh fixture DB.
        self.db_path = str(Path(self.db_path).with_name("frequency-%s.db" % label))
        bnl01_bot.ensure_journal_source_schema(self.db_path)

    def _context(self, question, *, conversation_context="", include_current=True, read_model=None):
        with mock.patch.dict(os.environ, {"BNL_QUEUE_PRODUCTION_ENABLED": "true"}, clear=False), \
             mock.patch.object(bnl01_bot, "DB_FILE", self.db_path), \
             mock.patch.object(bnl01_bot, "BNL_PRIMARY_GUILD_ID", 77), \
             mock.patch.object(bnl01_bot, "_bnl_read_model_cached_at", self.observed_at), \
             mock.patch.object(bnl01_bot, "BNL_TIKTOK_LIVE_CONTEXT_PATH", "/missing-frequency-live-context"), \
             mock.patch.object(bnl01_bot, "fetch_bnl_read_model",
                               return_value=read_model if read_model is not None else self._model(include_current=include_current)), \
             mock.patch.object(bnl01_bot, "load_tiktok_show_source_events",
                               wraps=bnl01_bot.load_tiktok_show_source_events) as originals, \
             mock.patch.object(bnl01_bot, "build_durable_show_prompt_context",
                               wraps=bnl01_bot.build_durable_show_prompt_context) as renderer, \
             mock.patch.object(bnl01_bot, "build_live_prompt_context",
                               wraps=bnl01_bot.build_live_prompt_context) as live:
            context = bnl01_bot.maybe_build_bnl_read_model_context(
                question, "public_home", conversation_context=conversation_context,
            )
        originals.assert_called_once()
        renderer.assert_called_once()
        live.assert_not_called()
        self.assertEqual(originals.call_args.args[0], self.db_path)
        self.assertEqual(originals.call_args.kwargs["guild_id"], 77)
        self.assertTrue(originals.call_args.kwargs["word_frequency"])
        return context, originals.call_args.kwargs["show"], renderer.call_args.args

    def _assert_current_originals(self, context, selected_show, rendered_args):
        self.assertEqual(selected_show["sessionId"], self.current["sessionId"])
        self.assertEqual(
            selected_show["_evidenceObservedThroughMs"],
            int(self.observed_at.timestamp() * 1000),
        )
        self.assertEqual(
            {event["event_id"] for event in rendered_args[1]},
            {"current-one", "current-neutral", "current-two", "current-three"},
        )
        self.assertIn("occurrenceCount=4; matchingMessageCount=3; matchingSpeakerCount=3", context)
        self.assertIn("eligibleCapturedMessagesChecked=4", context)
        self.assertNotIn("occurrenceCount=8", context)
        self.assertNotIn("occurrenceCount=31", context)
        self.assertNotIn("snapshot_missing", context)

    def test_current_word_count_receives_the_complete_frozen_original_window(self):
        for question in (
            "Count the word panda in TikTok chat tonight",
            "How many times did TikTok chat say panda in this TikTok live?",
        ):
            with self.subTest(question=question):
                context, selected_show, rendered_args = self._context(question)
                self._assert_current_originals(context, selected_show, rendered_args)
                self.assertEqual(rendered_args[2], question)

    def test_last_stream_and_explicit_date_keep_their_own_originals(self):
        for question, session_id, event_id, count in (
            ("How many times did people say the word panda during the last stream?",
             "latest-frequency", "latest-one", 8),
            ("Count the word panda during the 2026-09-18 stream",
             "older-frequency", "older-one", 31),
        ):
            with self.subTest(question=question):
                context, selected_show, rendered_args = self._context(question)
                self.assertEqual(selected_show["sessionId"], session_id)
                self.assertEqual({event["event_id"] for event in rendered_args[1]}, {event_id})
                self.assertIn("occurrenceCount=%s; matchingMessageCount=1" % count, context)
                self.assertNotIn("occurrenceCount=4", context)
                self.assertEqual(rendered_args[2], question)

    def test_targetless_whole_stream_correction_uses_the_human_count_chain(self):
        correction = "Not now, in the whole stream"
        conversation = (
            "User/member: TikTok chat tonight\n"
            "BNL-01: A previous show had 31 pandas.\n"
            'User/member: How many times did they say the word "panda"?\n'
            "BNL-01: I only checked a rolling buffer and guessed 99.\n"
            "User/member (current payload fragment): " + correction
        )
        context, selected_show, rendered_args = self._context(
            correction, conversation_context=conversation,
        )
        self._assert_current_originals(context, selected_show, rendered_args)
        self.assertIn('Prior follow-up: How many times did they say the word "panda"?', rendered_args[2])
        self.assertIn("Current follow-up: " + correction, rendered_args[2])
        self.assertNotIn("31 pandas", rendered_args[2])
        self.assertNotIn("guessed 99", rendered_args[2])


    def test_bare_count_and_target_corrections_keep_the_actual_human_show_chain(self):
        conversation = (
            "User/member: TikTok chat tonight\n"
            'User/member: How many times did they say the word "panda" in this TikTok live?\n'
            "BNL-01: A previous show had 31 pandas.\n"
        )
        for question, word, count, matching in (
            ("count panda", "panda", 4, 3),
            ("count 'panda'", "panda", 4, 3),
            ("I meant goat", "goat", 0, 0),
            ("not panda, goat", "goat", 0, 0),
            ("I meant 'red panda'", "red panda", None, None),
        ):
            with self.subTest(question=question):
                context, selected, rendered = self._context(
                    question,
                    conversation_context=conversation + "User/member (current payload fragment): " + question,
                )
                self.assertEqual(selected["sessionId"], self.current["sessionId"])
                self.assertEqual({event["event_id"] for event in rendered[1]}, {
                    "current-one", "current-neutral", "current-two", "current-three",
                })
                if count is None:
                    self.assertIn("Coverage=unavailable", context)
                    self.assertIn("unsupported_word_target", context)
                    self.assertNotIn("occurrenceCount=4", context)
                    self.assertNotIn("occurrenceCount=0", context)
                else:
                    self.assertIn('- Word "%s": occurrenceCount=%s; matchingMessageCount=%s' %
                                  (word, count, matching), context)
                self.assertNotIn("occurrenceCount=31", context)
                self.assertNotIn("31 pandas", rendered[2])

    def test_incoming_active_observation_marker_requires_owned_live_queue(self):
        cutoff = int(self.observed_at.timestamp() * 1000)
        for session_id, phase in (
            ("different-frequency", "live"),
            (self.current["sessionId"], "ended"),
            ("different-frequency", "ended"),
        ):
            with self.subTest(session_id=session_id, phase=phase):
                model = self._model()
                incoming = model["sections"]["archive"]["currentShow"]
                incoming["_evidenceObservedThroughMs"] = cutoff
                model["sections"]["queue"]["session"].update(id=session_id, broadcastPhase=phase)
                context, selected, rendered = self._context(
                    "Count the word panda in this TikTok live", read_model=model,
                )
                self.assertNotIn("_evidenceObservedThroughMs", selected)
                self.assertIsNone(rendered[1])
                self.assertIn("Coverage=unavailable", context)
                self.assertIn("active_observation_bound_unavailable", context)
                self.assertNotIn("occurrenceCount=4", context)
                # Cleaning the local read must not mutate the incoming model.
                self.assertEqual(incoming["_evidenceObservedThroughMs"], cutoff)

    def test_owned_live_queue_replaces_incoming_marker_with_frozen_read_clock(self):
        model = self._model()
        model["sections"]["archive"]["currentShow"]["_evidenceObservedThroughMs"] = (
            int(self.observed_at.timestamp() * 1000) + 60_000
        )
        context, selected, rendered = self._context(
            "Count the word panda in this TikTok live", read_model=model,
        )
        self._assert_current_originals(context, selected, rendered)

    def test_first_receipt_after_frozen_cutoff_cannot_create_an_exact_match(self):
        cutoff = int(self.observed_at.timestamp() * 1000)
        self._empty_source_database("first-receipt-cutoff")
        for event_id, occurred, ingested, text in (
            ("quiet-control", cutoff - 2000, cutoff - 1000, "Good drums"),
            ("late-first-receipt", cutoff - 1000, cutoff + 60_000, "panda"),
        ):
            self.assertTrue(bnl01_bot.record_journal_source_event(
                self.db_path, guild_id=77, source_kind="tiktok_live_chat",
                source_key=event_id, occurred_at_ms=occurred, ingested_at_ms=ingested,
                raw_text=text, channel_policy="public_context",
                subject_ref="tiktok_handle:test.viewer",
                metadata={"eventType": "comment", "handle": "test.viewer"},
            ).ok)
        context, selected, rendered = self._context(
            "Count the word panda in this TikTok live",
        )
        self.assertEqual([event["event_id"] for event in rendered[1]], ["quiet-control"])
        self.assertEqual(rendered[1][0]["ingested_at_ms"], cutoff - 1000)
        self.assertIn("Coverage=complete", context)
        self.assertIn("occurrenceCount=0; matchingMessageCount=0", context)
        self.assertIn("eligibleCapturedMessagesChecked=1", context)

    def test_invalid_first_receipt_cannot_certify_an_exact_count(self):
        cutoff = int(self.observed_at.timestamp() * 1000)
        for index, receipt in enumerate((0, 1.5, "invalid-receipt")):
            with self.subTest(receipt=receipt):
                self._empty_source_database("invalid-receipt-%s" % index)
                self.assertTrue(bnl01_bot.record_journal_source_event(
                    self.db_path, guild_id=77, source_kind="tiktok_live_chat",
                    source_key="quiet-control", occurred_at_ms=cutoff - 2000,
                    ingested_at_ms=cutoff - 1000, raw_text="Good drums",
                    channel_policy="public_context", subject_ref="tiktok_handle:test.viewer",
                    metadata={"eventType": "comment", "handle": "test.viewer"},
                ).ok)
                # The normal writer coerces receipt values to integers. Insert
                # malformed fixture values once under its unchanged SQL schema.
                with sqlite3.connect(self.db_path) as conn:
                    conn.execute(
                        "INSERT INTO bnl_journal_source_events "
                        "(guild_id,source_kind,source_key,occurred_at_ms,ingested_at_ms,"
                        "channel_policy,subject_ref,raw_text,sanitized_summary,content_hash,"
                        "public_usable,metadata_json) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                        (77, "tiktok_live_chat", "invalid-receipt", cutoff - 1000, receipt,
                         "public_context", "tiktok_handle:test.viewer", "panda", "panda",
                         hashlib.sha256(b"panda").hexdigest(), 1,
                         json.dumps({"eventType": "comment", "handle": "test.viewer"})),
                    )
                context, _selected, rendered = self._context(
                    "Count the word panda in this TikTok live",
                )
                self.assertIn("invalid-receipt", {event["event_id"] for event in rendered[1]})
                self.assertIn("Coverage=partial", context)
                self.assertNotIn("occurrenceCount=1", context)
                self.assertNotIn("occurrenceCount=0", context)

    def test_archived_count_keeps_retained_originals_received_after_show_end(self):
        self._empty_source_database("archived-late-receipt")
        self.assertTrue(bnl01_bot.record_journal_source_event(
            self.db_path, guild_id=77, source_kind="tiktok_live_chat",
            source_key="latest-one", occurred_at_ms=self._stamp("2026-09-26T03:00:00Z"),
            ingested_at_ms=int(self.observed_at.timestamp() * 1000),
            raw_text="panda " * 8, channel_policy="public_context",
            subject_ref="tiktok_handle:test.viewer",
            metadata={"eventType": "comment", "handle": "test.viewer"},
        ).ok)
        model = self._model(include_current=False)
        for candidate in (
            model["sections"]["archive"]["latestShow"],
            *model["sections"]["archive"]["shows"],
        ):
            candidate["_evidenceObservedThroughMs"] = int(self.observed_at.timestamp() * 1000)
        context, selected, rendered = self._context(
            "Count the word panda during the last stream", include_current=False, read_model=model,
        )
        self.assertNotIn("_evidenceObservedThroughMs", selected)
        self.assertEqual([event["event_id"] for event in rendered[1]], ["latest-one"])
        self.assertEqual(rendered[1][0]["ingested_at_ms"], int(self.observed_at.timestamp() * 1000))
        self.assertIn("occurrenceCount=8; matchingMessageCount=1", context)
        self.assertIn("2026-09-26T08:08:03.054000+00:00 inclusive", context)

    def test_recorded_intake_extends_only_the_count_window_for_live_show(self):
        from bnl_tiktok_live_context import show_timeline_bounds_ms
        self.current["milestones"].insert(0, {
            "eventType": "submissions_opened", "occurredAt": "2026-10-03T01:41:01.622Z",
        })
        context, selected, rendered = self._context(
            "Count the word panda in this TikTok live",
        )
        self.assertEqual(show_timeline_bounds_ms(selected)[0], self._stamp("2026-10-03T02:05:35.254Z"))
        self.assertEqual({event["event_id"] for event in rendered[1]}, {
            "current-pre", "current-one", "current-neutral", "current-two", "current-three",
        })
        self.assertIn("occurrenceCount=24; matchingMessageCount=4", context)
        self.assertIn("eligibleCapturedMessagesChecked=5", context)
        self.assertIn("windowUTC=2026-10-03T01:41:01.622000+00:00", context)

    def test_recorded_intake_extends_only_the_count_window_for_archived_show(self):
        from bnl_tiktok_live_context import show_timeline_bounds_ms
        self.latest["milestones"].insert(0, {
            "eventType": "session_created", "occurredAt": "2026-09-26T01:41:01.622Z",
        })
        self.assertTrue(bnl01_bot.record_journal_source_event(
            self.db_path, guild_id=77, source_kind="tiktok_live_chat",
            source_key="latest-intake", occurred_at_ms=self._stamp("2026-09-26T02:00:00Z"),
            ingested_at_ms=self._stamp("2026-09-26T02:00:01Z"),
            raw_text="Panda panda", channel_policy="public_context",
            subject_ref="tiktok_handle:test.viewer",
            metadata={"eventType": "comment", "handle": "test.viewer"},
        ).ok)
        context, selected, rendered = self._context(
            "Count the word panda during the last stream", include_current=False,
        )
        self.assertEqual(show_timeline_bounds_ms(selected)[0], self._stamp("2026-09-26T02:05:35.254Z"))
        self.assertEqual({event["event_id"] for event in rendered[1]}, {"latest-intake", "latest-one"})
        self.assertIn("occurrenceCount=10; matchingMessageCount=2", context)
        self.assertIn("windowUTC=2026-09-26T01:41:01.622000+00:00", context)

    def _seed_tonight_word_count_originals(self):
        self._empty_source_database("tonight-native-session")
        texts = (
            "Butt butt!", "butt", "BUTT?", "butt, butts", "no butt?",
            "butt... butt", "butts", "BUTTS!", "butter", "about", "butt",
        )
        for index, text in enumerate(texts):
            event_id = "tonight-original-%02d" % index
            occurred_at = self._stamp("2026-10-03T03:30:00Z") + index * 1000
            self.assertTrue(bnl01_bot.record_journal_source_event(
                self.db_path, guild_id=77, source_kind="tiktok_live_chat",
                source_key=event_id, occurred_at_ms=occurred_at,
                ingested_at_ms=occurred_at + 100,
                raw_text=text, sanitized_summary=text, channel_policy="public_context",
                subject_ref="tiktok_handle:" + event_id, private_display_name="@" + event_id,
                public_usable=True, metadata={
                    "eventType": "comment", "handle": event_id,
                    "sessionId": self.current["sessionId"],
                },
            ).ok)
        # These originals belong to other sessions, outside the selected window.
        # Their distinct totals expose either a latest-show or date-only fallback.
        for event_id, instant, session_id, count in (
            ("older-butt-original", "2026-09-26T03:00:00Z", self.latest["sessionId"], 31),
            ("rehearsal-butt-original", "2026-10-02T20:10:00Z", "same-date-rehearsal", 23),
        ):
            occurred_at = self._stamp(instant)
            text = "butt " * count
            self.assertTrue(bnl01_bot.record_journal_source_event(
                self.db_path, guild_id=77, source_kind="tiktok_live_chat",
                source_key=event_id, occurred_at_ms=occurred_at,
                ingested_at_ms=occurred_at + 100,
                raw_text=text, sanitized_summary=text, channel_policy="public_context",
                subject_ref="tiktok_handle:" + event_id, private_display_name="@" + event_id,
                public_usable=True, metadata={
                    "eventType": "comment", "handle": event_id, "sessionId": session_id,
                },
            ).ok)

    def _tonight_native_session_model(self, *, archived):
        selected_show = self._show(
            self.current["sessionId"], "2026-10-02", "2026-10-03", active=not archived,
        )
        rehearsal = {
            "sessionId": "same-date-rehearsal", "title": "BARCODE rehearsal",
            "showDate": "2026-10-02", "status": "archived", "milestones": [
                {"eventType": "broadcast_started", "occurredAt": "2026-10-02T20:00:00Z"},
                {"eventType": "session_archived", "occurredAt": "2026-10-02T20:30:00Z"},
            ],
        }
        archive = {
            "latestShow": rehearsal if archived else self.latest,
            "shows": [rehearsal, self.latest, self.older],
        }
        if archived:
            # The canonical queue id must find this archived node even when
            # another same-date session occupies latestShow and the first slot.
            archive["shows"].append(selected_show)
        else:
            archive["currentShow"] = selected_show
        model = self.authorized_read_model(archive)
        model["sections"]["queue"] = {
            "available": True, "accessScope": "public", "session": {
                "id": self.current["sessionId"], "showDate": "2026-10-02",
                "title": "BARCODE Radio", "purpose": "live_broadcast",
                "status": "archived" if archived else "open",
                "broadcastPhase": "ended" if archived else "live",
            },
        }
        return model

    def _tonight_word_count_context(self, question, *, model, conversation_context=""):
        # Freeze the existing calendar owner alongside the bridge observation
        # clock, so this test remains deterministic when run on another day.
        with mock.patch(
            "bnl_tiktok_live_context._pacific_show_date",
            return_value=self.observed_at.astimezone(bnl01_bot.PACIFIC_TZ).date(),
        ):
            return self._context(
                question, conversation_context=conversation_context, read_model=model,
            )

    def _assert_tonight_word_count(self, context, selected, rendered, *, word, archived):
        self.assertEqual(selected["sessionId"], self.current["sessionId"])
        self.assertEqual(selected["showDate"], "2026-10-02")
        if archived:
            self.assertNotIn("_evidenceObservedThroughMs", selected)
        else:
            self.assertEqual(selected["_evidenceObservedThroughMs"],
                             int(self.observed_at.timestamp() * 1000))
        self.assertEqual({event["event_id"] for event in rendered[1]}, {
            "tonight-original-%02d" % index for index in range(11)
        })
        self.assertTrue(all(
            event["metadata"]["sessionId"] == self.current["sessionId"]
            for event in rendered[1]
        ))
        count, matching = (9, 7) if word == "butt" else (3, 3)
        self.assertIn('- Word "%s": occurrenceCount=%s; matchingMessageCount=%s; matchingSpeakerCount=%s' %
                      (word, count, matching, matching), context)
        self.assertIn("eligibleCapturedMessagesChecked=11", context)
        self.assertNotIn("occurrenceCount=31", context)
        self.assertNotIn("occurrenceCount=23", context)
        self.assertNotIn("Coverage=unavailable", context)

    def test_tonight_native_queue_session_survives_midnight_and_immediate_archival(self):
        self._seed_tonight_word_count_originals()
        for instant, archived in (
            ("2026-10-03T06:59:00Z", False),
            ("2026-10-03T07:01:00Z", False),
            ("2026-10-03T08:10:00Z", True),
        ):
            self.observed_at = datetime.fromisoformat(instant.replace("Z", "+00:00"))
            for question, word in (
                ("BNL butt word count. Tonight’s show. Go", "butt"),
                ("BNL butts word count. Tonight’s show. Go", "butts"),
                ("Count butt in the current TikTok stream", "butt"),
            ):
                with self.subTest(instant=instant, archived=archived, question=question):
                    context, selected, rendered = self._tonight_word_count_context(
                        question, model=self._tonight_native_session_model(archived=archived),
                    )
                    self._assert_tonight_word_count(
                        context, selected, rendered, word=word, archived=archived,
                    )

    def test_same_user_current_stream_correction_retains_word_across_archival(self):
        self._seed_tonight_word_count_originals()
        correction = "Current stream not last stream"
        conversation = (
            "User/member: BNL butt word count. Last stream. Go\n"
            "BNL-01: The older stream had 31; my guessed word was panda.\n"
            "User/member (current payload fragment): " + correction
        )
        for instant, archived in (
            ("2026-10-03T06:59:00Z", False),
            ("2026-10-03T07:01:00Z", False),
            ("2026-10-03T08:10:00Z", True),
        ):
            with self.subTest(instant=instant, archived=archived):
                self.observed_at = datetime.fromisoformat(instant.replace("Z", "+00:00"))
                context, selected, rendered = self._tonight_word_count_context(
                    correction, conversation_context=conversation,
                    model=self._tonight_native_session_model(archived=archived),
                )
                self._assert_tonight_word_count(
                    context, selected, rendered, word="butt", archived=archived,
                )
                self.assertIn("Current follow-up: " + correction, rendered[2])
                self.assertNotIn("guessed word", rendered[2])
                self.assertNotIn("31;", rendered[2])

    def test_missing_stale_or_mismatched_native_queue_identity_keeps_counts_unavailable(self):
        self._seed_tonight_word_count_originals()
        self.observed_at = datetime(2026, 10, 3, 8, 10, tzinfo=timezone.utc)
        for archived in (False, True):
            for failure in ("missing", "stale", "mismatched", "conflicting_alias"):
                for question in (
                    "BNL butt word count. Tonight’s show. Go",
                    "Count butt in the current TikTok stream",
                ):
                    with self.subTest(archived=archived, failure=failure, question=question):
                        model = self._tonight_native_session_model(archived=archived)
                        queue = model["sections"]["queue"]
                        if failure == "missing":
                            queue["session"].pop("id")
                        elif failure == "stale":
                            queue.update(available=False, reason="stale")
                        elif failure == "mismatched":
                            queue["session"]["id"] = "unmatched-current-session"
                        else:
                            queue["session"]["sessionId"] = self.latest["sessionId"]
                        context, _selected, rendered = self._tonight_word_count_context(
                            question, model=model,
                        )
                        self.assertIsNone(rendered[1])
                        self.assertNotIn("occurrenceCount=9", context)
                        self.assertNotIn("occurrenceCount=31", context)
                        self.assertNotIn("occurrenceCount=23", context)
                        self.assertNotIn("occurrenceCount=0", context)
                        self.assertTrue(
                            "Coverage=unavailable" in context
                            or "no public show timeline was selected" in context,
                            context,
                        )

    def test_missing_current_tiktok_live_does_not_count_an_archived_show(self):
        question = "How many times did TikTok chat say panda in this TikTok live?"
        context, selected_show, rendered_args = self._context(question, include_current=False)
        self.assertEqual(selected_show, {})
        self.assertIsNone(rendered_args[1])
        self.assertNotIn("occurrenceCount=8", context)
        self.assertNotIn("occurrenceCount=31", context)
        self.assertIn("no public show timeline was selected", context)




class BnlWordFrequencyRepairBasisTests(unittest.TestCase):
    @staticmethod
    def _basis(text, digest, count):
        return bnl01_bot.FinalizedShowPromptSourceBasis(
            expected_digest=digest,
            rendered_context="TikTok show word frequency: occurrenceCount=%s" % count,
            guild_id=77, user_text=text, selection_user_text=text,
            subject_user_id=0, show_keys=("test-frequency-show",),
        )

    def test_corrected_word_basis_is_retained_only_after_stable_revalidation(self):
        question = "Count the word panda during the last stream"
        stale = self._basis(question, "stale", 38)
        fresh = self._basis(question, "fresh", 36)
        with mock.patch.object(
            bnl01_bot, "refresh_prompt_source_basis",
            side_effect=((fresh, True), (fresh, False)),
        ) as refresh:
            prompt, bases, source_neutral = bnl01_bot.build_ordinary_chat_response_repair_prompt(
                stale.rendered_context, reason="show_episode_source_changed",
                prompt_source_bases=(stale,), current_user_text=question,
            )
        self.assertEqual(refresh.call_count, 2)
        self.assertEqual(bases, (fresh,))
        self.assertFalse(source_neutral)
        self.assertIn("occurrenceCount=36", prompt)
        self.assertNotIn("occurrenceCount=38", prompt)

    def test_corrected_word_basis_that_changes_again_is_not_retained(self):
        question = "Count the word panda during the last stream"
        stale = self._basis(question, "stale", 38)
        fresh = self._basis(question, "fresh", 36)
        with mock.patch.object(
            bnl01_bot, "refresh_prompt_source_basis", return_value=(fresh, True),
        ) as refresh:
            prompt, bases, source_neutral = bnl01_bot.build_ordinary_chat_response_repair_prompt(
                stale.rendered_context, reason="show_episode_source_changed",
                prompt_source_bases=(stale,), current_user_text=question,
            )
        self.assertEqual(refresh.call_count, 2)
        self.assertEqual(bases, ())
        self.assertTrue(source_neutral)
        self.assertNotIn("occurrenceCount=36", prompt)
        self.assertNotIn("occurrenceCount=38", prompt)

    def test_changed_non_count_show_basis_keeps_existing_omission_rule(self):
        question = "Recap the last stream"
        stale = self._basis(question, "stale", 38)
        fresh = self._basis(question, "fresh", 36)
        with mock.patch.object(
            bnl01_bot, "refresh_prompt_source_basis", return_value=(fresh, True),
        ) as refresh:
            prompt, bases, source_neutral = bnl01_bot.build_ordinary_chat_response_repair_prompt(
                stale.rendered_context, reason="show_episode_source_changed",
                prompt_source_bases=(stale,), current_user_text=question,
            )
        refresh.assert_called_once()
        self.assertEqual(bases, ())
        self.assertTrue(source_neutral)
        self.assertNotIn("occurrenceCount=36", prompt)

if __name__ == "__main__":
    unittest.main()
