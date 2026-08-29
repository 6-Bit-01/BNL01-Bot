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
                    "projectLabel": "Neon Fox",
                    "title": "First Signal",
                },
            },
            {
                "sequence": 3,
                "eventType": "track_finished",
                "occurredAt": "2026-08-29T00:04:00Z",
                "track": {
                    "projectLabel": "Neon Fox",
                    "title": "First Signal",
                },
            },
            {
                "sequence": 4,
                "eventType": "track_loaded",
                "occurredAt": "2026-08-29T00:04:00Z",
                "track": {
                    "projectLabel": "Second Artist",
                    "title": "Queue Light",
                },
            },
            {
                "sequence": 5,
                "eventType": "track_finished",
                "occurredAt": "2026-08-29T00:08:00Z",
                "track": {
                    "projectLabel": "Second Artist",
                    "title": "Queue Light",
                },
            },
            {
                "sequence": 6,
                "eventType": "session_archived",
                "occurredAt": "2026-08-29T00:09:00Z",
                "track": None,
            },
        ],
    }


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
        self.assertEqual(len(ledger["messages"]), 4)
        self.assertEqual(
            ledger["messages"][0]["speakerLabel"],
            "Alex (@alex.signal)",
        )
        self.assertEqual(
            ledger["messages"][0]["trackLabel"],
            "Neon Fox — First Signal",
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
        if include_shadow_memory:
            self.seed_shadow_memory(db_file)

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
            self.assertEqual(first["participants"], 3)
            self.assertEqual(first["projectionInserted"], 4)
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
            self.assertEqual(len(projected), 4)
            self.assertTrue(all(item[3] == 1 for item in projected))
            self.assertTrue(all(item[4] == 1 for item in projected))
            self.assertTrue(all(item[5] == 1 for item in projected))
            self.assertTrue(all(item[6] == "active" for item in projected))
            self.assertEqual(lineage_count, 8)

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
            self.assertEqual(first["projectionInserted"], 4)
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
            self.assertEqual(second["projectionDeduplicated"], 4)
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
            self.assertEqual(lineage_count, 8)

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
            self.assertIn("every eligible message", broad)
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
                message_limit=1,
            )
            self.assertIn("is my song still in the queue?", track_excerpt)
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
