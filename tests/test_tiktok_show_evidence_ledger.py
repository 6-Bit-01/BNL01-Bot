import json
import sqlite3
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from bnl_journal_source_store import (
    ensure_schema as ensure_journal_source_schema,
    record_source_event,
)
from bnl_memory_ledger import (
    ensure_memory_ledger_schema,
    shadow_conversation_row,
    shadow_tiktok_live_chat_event,
)
from bnl_memory_governance import (
    complete_delete_member_data,
    ensure_governance_schema,
)
from bnl_tiktok_live_context import build_tiktok_show_evidence_ledger
from bnl_tiktok_show_ledger import (
    TIKTOK_SHOW_EVIDENCE_TABLE,
    build_tiktok_show_evidence_context,
    sync_tiktok_show_evidence_ledgers,
)


def stamp(value: str) -> int:
    return int(
        datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
        * 1000
    )


def archived_show():
    return {
        "sessionId": "show-attendance-1",
        "title": "BARCODE Radio",
        "showDate": "2026-08-28",
        "status": "archived",
        "trackRoster": [
            {
                "trackId": "track-first-signal",
                "projectLabel": "Neon Fox",
                "title": "First Signal",
                "submittedByTikTokHandle": "neon.fox",
                "lane": "regular",
                "outcome": "finished",
                "submittedAt": "2026-08-28T23:45:00Z",
                "resolvedAt": "2026-08-29T00:04:00Z",
                "wheelChosen": False,
                "submissionEventSequence": 1,
                "outcomeEventSequence": 4,
            },
            {
                "trackId": "track-queue-light",
                "projectLabel": "Second Artist",
                "title": "Queue Light",
                "submittedByTikTokHandle": "second.artist",
                "lane": "wheel",
                "outcome": "finished",
                "submittedAt": "2026-08-28T23:50:00Z",
                "resolvedAt": "2026-08-29T00:08:00Z",
                "wheelChosen": True,
                "submissionEventSequence": 2,
                "outcomeEventSequence": 12,
            },
        ],
        "milestones": [
            {
                "sequence": 1,
                "eventType": "broadcast_started",
                "occurredAt": "2026-08-29T00:00:00Z",
                "track": None,
            },
            {
                "sequence": 2,
                "eventType": "track_loaded",
                "occurredAt": "2026-08-29T00:01:00Z",
                "track": {
                    "trackId": "track-first-signal",
                    "projectLabel": "Neon Fox",
                    "title": "First Signal",
                    "submittedByTikTokHandle": "neon.fox",
                    "lane": "regular",
                    "outcome": "finished",
                    "submissionOrder": 1,
                    "playedOrder": 1,
                },
            },
            {
                "sequence": 3,
                "eventType": "track_play_started",
                "occurredAt": "2026-08-29T00:01:10Z",
                "track": {
                    "trackId": "track-first-signal",
                    "projectLabel": "Neon Fox",
                    "title": "First Signal",
                    "submittedByTikTokHandle": "neon.fox",
                    "lane": "regular",
                    "outcome": "finished",
                    "submissionOrder": 1,
                    "playedOrder": 1,
                },
            },
            {
                "sequence": 4,
                "eventType": "track_finished",
                "occurredAt": "2026-08-29T00:04:00Z",
                "track": {
                    "trackId": "track-first-signal",
                    "projectLabel": "Neon Fox",
                    "title": "First Signal",
                    "submittedByTikTokHandle": "neon.fox",
                    "lane": "regular",
                    "outcome": "finished",
                    "submissionOrder": 1,
                    "playedOrder": 1,
                },
            },
            {
                "sequence": 5,
                "eventType": "sponsor_break_started",
                "occurredAt": "2026-08-29T00:04:05Z",
                "track": None,
                "headline": "Sponsor break started",
                "detail": "The sponsor break is running.",
            },
            {
                "sequence": 6,
                "eventType": "sponsor_break_completed",
                "occurredAt": "2026-08-29T00:04:20Z",
                "track": None,
                "headline": "Sponsor break completed",
                "detail": "The show returned from sponsor break.",
            },
            {
                "sequence": 7,
                "eventType": "wheel_spin_unlocked",
                "occurredAt": "2026-08-29T00:04:25Z",
                "track": None,
                "headline": "Wheel spin unlocked",
                "detail": "A Wheel spin became available.",
                "details": {
                    "wheelSpinsAdded": 1,
                    "wheelSpinsOwed": 1,
                },
            },
            {
                "sequence": 8,
                "eventType": "wheel_confirmed",
                "occurredAt": "2026-08-29T00:04:30Z",
                "track": {
                    "trackId": "track-queue-light",
                    "projectLabel": "Second Artist",
                    "title": "Queue Light",
                    "submittedByTikTokHandle": "second.artist",
                    "lane": "wheel",
                    "outcome": "finished",
                    "submissionOrder": 2,
                    "playedOrder": 2,
                },
                "headline": "Wheel result confirmed",
                "detail": "Second Artist — Queue Light",
            },
            {
                "sequence": 9,
                "eventType": "track_loaded",
                "occurredAt": "2026-08-29T00:04:40Z",
                "track": {
                    "trackId": "track-queue-light",
                    "projectLabel": "Second Artist",
                    "title": "Queue Light",
                    "submittedByTikTokHandle": "second.artist",
                    "lane": "wheel",
                    "outcome": "finished",
                    "submissionOrder": 2,
                    "playedOrder": 2,
                },
            },
            {
                "sequence": 10,
                "eventType": "track_play_started",
                "occurredAt": "2026-08-29T00:04:45Z",
                "track": {
                    "trackId": "track-queue-light",
                    "projectLabel": "Second Artist",
                    "title": "Queue Light",
                    "submittedByTikTokHandle": "second.artist",
                    "lane": "wheel",
                    "outcome": "finished",
                    "submissionOrder": 2,
                    "playedOrder": 2,
                },
            },
            {
                "sequence": 11,
                "eventType": "track_signal_hold_applied",
                "occurredAt": "2026-08-29T00:05:30Z",
                "track": {
                    "trackId": "track-queue-light",
                    "projectLabel": "Second Artist",
                    "title": "Queue Light",
                    "submittedByTikTokHandle": "second.artist",
                    "lane": "wheel",
                    "outcome": "finished",
                    "submissionOrder": 2,
                    "playedOrder": 2,
                },
                "headline": "Signal hold applied",
                "detail": "Queue Light moved into the priority lane.",
                "details": {
                    "signalHoldPreviousLane": "wheel",
                    "signalHoldApplicationCount": 1,
                },
            },
            {
                "sequence": 12,
                "eventType": "track_finished",
                "occurredAt": "2026-08-29T00:08:00Z",
                "track": {
                    "trackId": "track-queue-light",
                    "projectLabel": "Second Artist",
                    "title": "Queue Light",
                    "submittedByTikTokHandle": "second.artist",
                    "lane": "wheel",
                    "outcome": "finished",
                    "submissionOrder": 2,
                    "playedOrder": 2,
                },
            },
            {
                "sequence": 13,
                "eventType": "session_archived",
                "occurredAt": "2026-08-29T00:09:00Z",
                "track": None,
            },
        ],
    }


def discord_exchanges():
    return [
        {
            "exchangeId": "discord:102:42",
            "subjectRef": "discord_user:42",
            "speakerLabel": "Alex",
            "channelId": 9001,
            "channelName": "barcode-bot",
            "channelPolicy": "public_home",
            "userMessages": [
                {
                    "conversationRowId": 101,
                    "messageId": 7001,
                    "occurredAtMs": stamp("2026-08-29T00:05:10Z"),
                    "text": "Did the Wheel put Queue Light up next, BNL?",
                    "channelId": 9001,
                    "channelName": "barcode-bot",
                    "channelPolicy": "public_home",
                    "routeMode": "normal_chat",
                }
            ],
            "bnlResponse": {
                "conversationRowId": 102,
                "messageIds": [7002],
                "occurredAtMs": stamp("2026-08-29T00:05:20Z"),
                "text": "Yes—the Wheel confirmed Queue Light, and it is playing now.",
                "channelId": 9001,
                "channelName": "barcode-bot",
                "channelPolicy": "public_home",
                "routeMode": "normal_chat",
            },
        }
    ]


def durable_events():
    return [
        {
            "event_id": "event-alex-1",
            "occurred_at_ms": stamp("2026-08-29T00:02:00Z"),
            "subject_ref": "discord_user:42",
            "private_display_name": "Alex",
            "raw_text": "BNL, the green visuals during this song are wild.",
            "metadata": {
                "eventType": "comment",
                "handle": "alex.signal",
                "identityBindingBasis": "handle_display_correlation",
            },
        },
        {
            "event_id": "event-nova-1",
            "occurred_at_ms": stamp("2026-08-29T00:03:00Z"),
            "subject_ref": "tiktok_user:nova",
            "private_display_name": "Nova",
            "raw_text": "Those green visuals changed again.",
            "metadata": {"eventType": "comment", "handle": "nova"},
        },
        {
            "event_id": "event-alex-2",
            "occurred_at_ms": stamp("2026-08-29T00:05:00Z"),
            "subject_ref": "discord_user:42",
            "private_display_name": "Alex",
            "raw_text": "BNL, is my song still in the queue?",
            "metadata": {
                "eventType": "question",
                "handle": "alex.signal",
                "identityBindingBasis": "handle_display_correlation",
            },
        },
        {
            "event_id": "event-artist-1",
            "occurred_at_ms": stamp("2026-08-29T00:06:00Z"),
            "subject_ref": "tiktok_user:neon.fox",
            "private_display_name": "Neon Fox",
            "raw_text": "The green visuals made that moment hit.",
            "metadata": {"eventType": "comment", "handle": "neon.fox"},
        },
    ]


def artist_index():
    return {
        "neon.fox": (
            {
                "artistName": "Neon Fox",
                "identityKey": "queue:submission:neon-fox",
                "identityBasis": "submitted_tiktok_attribution",
                "recordId": "queue-record-neon-fox",
            },
        )
    }


class TikTokShowEvidenceLedgerTests(unittest.TestCase):
    def test_builder_accounts_for_people_messages_topics_and_show_moments(self):
        ledger = build_tiktok_show_evidence_ledger(
            archived_show(),
            durable_events(),
            artist_identity_index=artist_index(),
            discord_exchanges=discord_exchanges(),
        )

        self.assertEqual(ledger["lifecycle"], "finalized")
        self.assertEqual(ledger["coverage"]["eligibleMessageCount"], 4)
        self.assertEqual(ledger["coverage"]["accountedEventCount"], 4)
        self.assertTrue(ledger["coverage"]["allEligibleMessagesAccounted"])
        self.assertEqual(
            ledger["coverage"]["sourceEventIds"],
            [
                "event-alex-1",
                "event-nova-1",
                "event-alex-2",
                "event-artist-1",
            ],
        )
        self.assertEqual(ledger["interactions"]["bnlAddressCount"], 2)
        self.assertEqual(ledger["interactions"]["queueReferenceCount"], 1)
        self.assertEqual(ledger["interactions"]["discordExchangeCount"], 1)
        self.assertEqual(ledger["interactions"]["allQuestionCount"], 2)
        self.assertEqual(len(ledger["messages"]), 4)
        self.assertEqual(len(ledger["trackRoster"]), 2)
        self.assertEqual(len(ledger["operationalEvents"]), 13)
        self.assertEqual(len(ledger["discordInteractions"]), 1)
        self.assertEqual(len(ledger["discordParticipants"]), 1)
        self.assertEqual(len(ledger["crossSourceBindings"]), 1)
        self.assertEqual(
            ledger["crossSourceBindings"][0]["basis"],
            "exact source-owned subject reference",
        )
        self.assertEqual(
            ledger["messages"][0]["speakerLabel"],
            "Alex (@alex.signal)",
        )
        self.assertEqual(
            ledger["messages"][0]["trackLabel"],
            "Neon Fox — First Signal",
        )
        self.assertEqual(
            ledger["messages"][0]["operationalContext"][
                "lastOperationalEventType"
            ],
            "track_play_started",
        )
        topic = next(
            item for item in ledger["topics"] if item["term"] == "green visuals"
        )
        self.assertEqual(topic["messageCount"], 3)
        self.assertEqual(topic["participantCount"], 3)
        alex = next(
            item
            for item in ledger["participants"]
            if item["subjectRef"] == "discord_user:42"
        )
        self.assertEqual(alex["messageCount"], 2)
        self.assertEqual(alex["bnlAddressCount"], 2)
        neon = next(
            item
            for item in ledger["participants"]
            if item["handle"] == "neon.fox"
        )
        self.assertEqual(
            neon["artistAttributions"][0]["artistName"],
            "Neon Fox",
        )
        queue_light = next(
            item
            for item in ledger["trackRoster"]
            if item["trackId"] == "track-queue-light"
        )
        self.assertEqual(queue_light["submissionOrder"], 2)
        self.assertEqual(queue_light["playedOrder"], 2)
        discord_message = ledger["discordInteractions"][0]["userMessages"][0]
        self.assertEqual(discord_message["trackLabel"], "Second Artist — Queue Light")
        self.assertEqual(
            discord_message["operationalContext"]["wheelState"],
            "confirmed",
        )

    def seed_source_and_memory(
        self,
        db_file: str,
        *,
        include_shadow_memory: bool = True,
    ) -> None:
        ensure_journal_source_schema(db_file)
        for event in durable_events():
            result = record_source_event(
                db_file,
                guild_id=77,
                source_kind="tiktok_live_chat",
                source_key=event["event_id"],
                occurred_at_ms=event["occurred_at_ms"],
                raw_text=event["raw_text"],
                sanitized_summary=event["raw_text"],
                channel_policy="public_context",
                subject_ref=event["subject_ref"],
                private_display_name=event["private_display_name"],
                public_usable=True,
                metadata=event["metadata"],
            )
            self.assertTrue(result.ok)
        self.seed_conversations(db_file)
        if include_shadow_memory:
            self.seed_shadow_memory(db_file)

    def seed_conversations(self, db_file: str) -> None:
        conn = sqlite3.connect(db_file)
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS conversations (
                id INTEGER PRIMARY KEY,
                user_id INTEGER NOT NULL,
                user_name TEXT NOT NULL,
                guild_id INTEGER NOT NULL,
                channel_name TEXT,
                channel_policy TEXT,
                route_mode TEXT NOT NULL DEFAULT 'unknown',
                role TEXT NOT NULL,
                content TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                channel_id INTEGER,
                message_id INTEGER
            );
            CREATE TABLE IF NOT EXISTS conversation_response_participants (
                conversation_row_id INTEGER NOT NULL,
                guild_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                PRIMARY KEY (conversation_row_id,guild_id,user_id)
            );
            CREATE TABLE IF NOT EXISTS conversation_discord_message_links (
                conversation_row_id INTEGER NOT NULL,
                guild_id INTEGER NOT NULL,
                channel_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                PRIMARY KEY (guild_id,message_id)
            );
            """
        )
        conn.executemany(
            """
            INSERT OR REPLACE INTO conversations (
                id,user_id,user_name,guild_id,channel_name,channel_policy,
                route_mode,role,content,timestamp,channel_id,message_id
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            [
                (
                    101,
                    42,
                    "Alex",
                    77,
                    "barcode-bot",
                    "public_home",
                    "normal_chat",
                    "user",
                    "Did the Wheel put Queue Light up next, BNL?",
                    "2026-08-29T00:05:10+00:00",
                    9001,
                    7001,
                ),
                (
                    102,
                    42,
                    "BNL-01",
                    77,
                    "barcode-bot",
                    "public_home",
                    "normal_chat",
                    "model",
                    "Yes—the Wheel confirmed Queue Light, and it is playing now.",
                    "2026-08-29T00:05:20+00:00",
                    9001,
                    7002,
                ),
                (
                    103,
                    43,
                    "Jordan",
                    77,
                    "barcode-bot",
                    "public_home",
                    "normal_chat",
                    "user",
                    "BNL, did the sponsor break finish?",
                    "2026-08-29T00:06:00+00:00",
                    9001,
                    7003,
                ),
                (
                    104,
                    42,
                    "Alex",
                    77,
                    "private-room",
                    "sealed_test",
                    "normal_chat",
                    "user",
                    "This private row must never enter the public show episode.",
                    "2026-08-29T00:06:10+00:00",
                    9999,
                    7004,
                ),
            ],
        )
        conn.execute(
            """
            INSERT OR REPLACE INTO conversation_discord_message_links
              (conversation_row_id,guild_id,channel_id,message_id)
            VALUES (102,77,9001,7002)
            """
        )
        conn.commit()
        conn.close()
        direct_result = record_source_event(
            db_file,
            guild_id=77,
            source_kind="discord_message",
            source_key="7003",
            occurred_at_ms=stamp("2026-08-29T00:06:00Z"),
            raw_text="BNL, did the sponsor break finish?",
            sanitized_summary="BNL, did the sponsor break finish?",
            channel_id=9001,
            channel_policy="public_home",
            subject_ref="discord_user:43",
            private_display_name="Jordan",
            public_usable=True,
            metadata={
                "messageId": 7003,
                "conversationRowId": 103,
                "routeMode": "normal_chat",
                "directedToBnl": True,
                "channelName": "barcode-bot",
            },
        )
        self.assertTrue(direct_result.ok)
        paired_source_result = record_source_event(
            db_file,
            guild_id=77,
            source_kind="discord_message",
            source_key="7001",
            occurred_at_ms=stamp("2026-08-29T00:05:10Z"),
            raw_text="Did the Wheel put Queue Light up next, BNL?",
            sanitized_summary="Did the Wheel put Queue Light up next, BNL?",
            channel_id=9001,
            channel_policy="public_home",
            subject_ref="discord_user:42",
            private_display_name="Alex",
            public_usable=True,
            metadata={
                "messageId": 7001,
                "conversationRowId": 101,
                "routeMode": "normal_chat",
                "directedToBnl": True,
                "channelName": "barcode-bot",
            },
        )
        self.assertTrue(paired_source_result.ok)

    def seed_shadow_memory(self, db_file: str) -> None:
        conn = sqlite3.connect(db_file)
        ensure_memory_ledger_schema(conn)
        for event in durable_events():
            shadow_tiktok_live_chat_event(
                conn,
                guild_id=77,
                event_id=event["event_id"],
                subject_key=event["subject_ref"],
                subject_display_name=event["private_display_name"],
                content=event["raw_text"],
                observed_at=datetime.fromtimestamp(
                    event["occurred_at_ms"] / 1000
                ).astimezone().isoformat(),
                source_sequence=event["occurred_at_ms"],
            )
        conversation_rows = conn.execute(
            """
            SELECT id,user_id,user_name,role,content,channel_name,
                   channel_policy,channel_id,message_id,route_mode,timestamp
            FROM conversations
            WHERE id IN (101,102,103)
            ORDER BY id
            """
        ).fetchall()
        for row in conversation_rows:
            shadow_conversation_row(
                conn,
                row_id=int(row[0]),
                user_id=int(row[1]),
                user_name=str(row[2]),
                guild_id=77,
                role=str(row[3]),
                content=str(row[4]),
                channel_name=str(row[5]),
                channel_policy=str(row[6]),
                channel_id=int(row[7]),
                message_id=int(row[8]),
                route_mode=str(row[9]),
                observed_at=str(row[10]),
                conversation_target_user_ids=(42,) if row[3] == "model" else (),
            )
        conn.commit()
        conn.close()

    def test_sync_projects_finalized_episode_with_raw_event_lineage(self):
        with tempfile.TemporaryDirectory() as directory:
            db_file = str(Path(directory) / "bnl.db")
            self.seed_source_and_memory(db_file)
            read_model = {
                "ok": True,
                "version": 1,
                "sections": {
                    "archive": {
                        "currentShow": None,
                        "latestShow": archived_show(),
                        "shows": [],
                    }
                },
            }
            first = sync_tiktok_show_evidence_ledgers(
                db_file,
                guild_id=77,
                read_model=read_model,
                artist_identity_index=artist_index(),
            )
            self.assertEqual(first["status"], "completed")
            self.assertEqual(first["showsWritten"], 1)
            self.assertEqual(first["showsFinalized"], 1)
            self.assertEqual(first["sourceEvents"], 4)
            self.assertEqual(first["participants"], 4)
            self.assertEqual(first["operationalEvents"], 13)
            self.assertEqual(first["trackRoster"], 2)
            self.assertEqual(first["discordInteractions"], 2)
            self.assertEqual(first["discordExchanges"], 1)
            self.assertEqual(first["discordParticipants"], 2)
            self.assertEqual(first["discordConversationRows"], 3)
            self.assertEqual(first["projectionInserted"], 6)
            self.assertEqual(first["projectionErrors"], 0)

            conn = sqlite3.connect(db_file)
            row = conn.execute(
                f"SELECT ledger_json FROM {TIKTOK_SHOW_EVIDENCE_TABLE}"
            ).fetchone()
            projected = conn.execute(
                """
                SELECT entry_type,subject_key,predicate_key,derived,projection,
                       public_usable,lifecycle_status
                FROM memory_ledger_entries
                WHERE source_table='tiktok_show_evidence'
                ORDER BY entry_type,subject_key
                """
            ).fetchall()
            lineage_count = conn.execute(
                """
                SELECT COUNT(*) FROM memory_ledger_lineage l
                JOIN memory_ledger_entries e ON e.entry_id=l.entry_id
                WHERE e.source_table='tiktok_show_evidence'
                  AND l.lineage_type='derived_from'
                """
            ).fetchone()[0]
            conn.close()
            stored = json.loads(row[0])
            self.assertEqual(len(stored["messages"]), 4)
            self.assertEqual(len(stored["operationalEvents"]), 13)
            self.assertEqual(len(stored["trackRoster"]), 2)
            self.assertEqual(len(stored["discordInteractions"]), 2)
            self.assertIn("did the sponsor break finish?", row[0])
            self.assertNotIn("private row", row[0])
            self.assertEqual(len(projected), 6)
            self.assertTrue(all(item[3] == 1 for item in projected))
            self.assertTrue(all(item[4] == 1 for item in projected))
            self.assertTrue(all(item[5] == 1 for item in projected))
            self.assertTrue(all(item[6] == "active" for item in projected))
            self.assertEqual(
                {item[2] for item in projected},
                {
                    "barcode_radio.show_episode",
                    "barcode_radio.show_participation",
                },
            )
            self.assertEqual(lineage_count, 14)

            second = sync_tiktok_show_evidence_ledgers(
                db_file,
                guild_id=77,
                read_model=read_model,
                artist_identity_index=artist_index(),
            )
            self.assertEqual(second["showsWritten"], 0)
            self.assertEqual(second["showsUnchanged"], 1)
            self.assertEqual(second["projectionDeduplicated"], 0)

    def test_sync_backfills_lineage_when_raw_memory_shadows_arrive_later(self):
        with tempfile.TemporaryDirectory() as directory:
            db_file = str(Path(directory) / "bnl.db")
            self.seed_source_and_memory(
                db_file,
                include_shadow_memory=False,
            )
            read_model = {
                "sections": {
                    "archive": {
                        "currentShow": archived_show(),
                        "latestShow": None,
                        "shows": [],
                    }
                }
            }
            first = sync_tiktok_show_evidence_ledgers(
                db_file,
                guild_id=77,
                read_model=read_model,
                artist_identity_index=artist_index(),
            )
            self.assertEqual(first["projectionInserted"], 6)
            conn = sqlite3.connect(db_file)
            self.assertEqual(
                conn.execute(
                    """
                    SELECT COUNT(*) FROM memory_ledger_lineage AS lineage
                    JOIN memory_ledger_entries AS entry
                      ON entry.entry_id=lineage.entry_id
                    WHERE entry.source_table='tiktok_show_evidence'
                      AND lineage.lineage_type='derived_from'
                    """
                ).fetchone()[0],
                0,
            )
            conn.close()

            self.seed_shadow_memory(db_file)
            second = sync_tiktok_show_evidence_ledgers(
                db_file,
                guild_id=77,
                read_model=read_model,
                artist_identity_index=artist_index(),
            )
            self.assertEqual(second["showsUnchanged"], 1)
            self.assertEqual(second["projectionInserted"], 0)
            self.assertEqual(second["projectionDeduplicated"], 6)
            conn = sqlite3.connect(db_file)
            lineage_count = conn.execute(
                """
                SELECT COUNT(*) FROM memory_ledger_lineage AS lineage
                JOIN memory_ledger_entries AS entry
                  ON entry.entry_id=lineage.entry_id
                WHERE entry.source_table='tiktok_show_evidence'
                  AND lineage.lineage_type='derived_from'
                """
            ).fetchone()[0]
            conn.close()
            self.assertEqual(lineage_count, 14)

    def test_recall_connects_member_artist_topic_queue_and_track_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            db_file = str(Path(directory) / "bnl.db")
            self.seed_source_and_memory(db_file)
            sync_tiktok_show_evidence_ledgers(
                db_file,
                guild_id=77,
                read_model={
                    "sections": {
                        "archive": {
                            "currentShow": archived_show(),
                            "latestShow": None,
                            "shows": [],
                        }
                    }
                },
                artist_identity_index=artist_index(),
            )

            broad = build_tiktok_show_evidence_context(
                db_file,
                guild_id=77,
                user_text="What recurring topics came up throughout the show?",
            )
            self.assertIn("complete eligible TikTok chat ledger", broad)
            self.assertIn('"green visuals": 3 messages / 3 participants', broad)
            self.assertIn("Alex (@alex.signal)", broad)
            self.assertIn("queue/wheel", broad)

            topic_excerpt = build_tiktok_show_evidence_context(
                db_file,
                guild_id=77,
                user_text="What recurring topics came up throughout the show?",
                message_limit=1,
            )
            self.assertIn(
                "the green visuals during this song are wild.",
                topic_excerpt,
            )

            track_excerpt = build_tiktok_show_evidence_context(
                db_file,
                guild_id=77,
                user_text=(
                    "What happened during Second Artist — Queue Light?"
                ),
                message_limit=2,
            )
            self.assertIn("is my song still in the queue?", track_excerpt)
            self.assertIn("Did the Wheel put Queue Light up next, BNL?", track_excerpt)
            self.assertNotIn(
                "the green visuals during this song are wild.",
                track_excerpt,
            )

            member = build_tiktok_show_evidence_context(
                db_file,
                guild_id=77,
                user_text="Hey BNL, remember me?",
                subject_user_id=42,
            )
            self.assertIn("Alex (@alex.signal)", member)
            self.assertIn("is my song still in the queue?", member)
            self.assertIn("Did the Wheel put Queue Light up next, BNL?", member)
            self.assertIn(
                "Yes—the Wheel confirmed Queue Light, and it is playing now.",
                member,
            )

            operational = build_tiktok_show_evidence_context(
                db_file,
                guild_id=77,
                user_text=(
                    "What happened with the Wheel and signal hold during Queue Light?"
                ),
            )
            self.assertIn("Authoritative queue/broadcast events", operational)
            self.assertIn("[wheel confirmed]", operational)
            self.assertIn("[track signal hold applied]", operational)
            self.assertIn("played order 2", operational)

            asked = build_tiktok_show_evidence_context(
                db_file,
                guild_id=77,
                user_text="What did I ask BNL during the live?",
                subject_user_id=42,
            )
            self.assertIn("Public Discord interactions with BNL", asked)
            self.assertIn("Did the Wheel put Queue Light up next, BNL?", asked)
            self.assertIn("BNL replied", asked)

            unanswered = build_tiktok_show_evidence_context(
                db_file,
                guild_id=77,
                user_text="What did Jordan ask BNL during the live?",
            )
            self.assertIn("BNL, did the sponsor break finish?", unanswered)
            self.assertIn("No BNL response row is linked", unanswered)

            artist = build_tiktok_show_evidence_context(
                db_file,
                guild_id=77,
                user_text="What did Neon Fox say during the show?",
            )
            self.assertIn("Exact queue-submitted TikTok attribution", artist)
            self.assertIn("The green visuals made that moment hit.", artist)
            self.assertIn("source correlation only, not Discord identity", artist)

            unrelated = build_tiktok_show_evidence_context(
                db_file,
                guild_id=77,
                user_text="How do I make pasta?",
            )
            self.assertEqual(unrelated, "")

            wrong_episode = build_tiktok_show_evidence_context(
                db_file,
                guild_id=77,
                user_text="What happened in chat on 2026-08-27?",
            )
            self.assertEqual(wrong_episode, "")

    def test_complete_delete_removes_bound_sources_and_invalidates_episode(self):
        with tempfile.TemporaryDirectory() as directory:
            db_file = str(Path(directory) / "bnl.db")
            self.seed_source_and_memory(db_file)
            sync_tiktok_show_evidence_ledgers(
                db_file,
                guild_id=77,
                read_model={
                    "sections": {
                        "archive": {
                            "currentShow": archived_show(),
                            "latestShow": None,
                            "shows": [],
                        }
                    }
                },
                artist_identity_index=artist_index(),
            )
            conn = sqlite3.connect(db_file)
            ensure_governance_schema(conn)
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS conversations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    user_name TEXT NOT NULL DEFAULT '',
                    guild_id INTEGER NOT NULL,
                    role TEXT NOT NULL DEFAULT 'user',
                    content TEXT NOT NULL DEFAULT ''
                )
                """
            )
            conn.commit()

            result = complete_delete_member_data(
                conn,
                guild_id=77,
                user_id=42,
                confirmation="DELETE MY BNL DATA 77",
            )
            self.assertTrue(result["ok"])
            remaining_show_ledgers = conn.execute(
                f"SELECT COUNT(*) FROM {TIKTOK_SHOW_EVIDENCE_TABLE}"
            ).fetchone()[0]
            remaining_bound_sources = conn.execute(
                """
                SELECT COUNT(*) FROM bnl_journal_source_events
                WHERE source_kind='tiktok_live_chat'
                  AND subject_ref='discord_user:42'
                """
            ).fetchone()[0]
            remaining_other_sources = conn.execute(
                """
                SELECT COUNT(*) FROM bnl_journal_source_events
                WHERE source_kind='tiktok_live_chat'
                """
            ).fetchone()[0]
            remaining_projections = conn.execute(
                """
                SELECT COUNT(*) FROM memory_ledger_entries
                WHERE source_table='tiktok_show_evidence'
                """
            ).fetchone()[0]
            conn.close()

            self.assertEqual(remaining_show_ledgers, 0)
            self.assertEqual(remaining_bound_sources, 0)
            self.assertEqual(remaining_other_sources, 2)
            self.assertEqual(remaining_projections, 0)


if __name__ == "__main__":
    unittest.main()
