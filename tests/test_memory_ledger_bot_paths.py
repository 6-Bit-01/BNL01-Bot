import asyncio
import os
import sqlite3
import tempfile
import unittest
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl01_bot
import bnl_memory_ledger as ledger
import bnl_moment_engine as moments


class _Author:
    id = 42
    display_name = "Crow"

class _Message:
    def __init__(self, content="note"):
        self.content = content
        self.author = _Author()
        self.replies = []
    async def reply(self, text, **kwargs):
        self.replies.append(text)

class _FailMessage(_Message):
    async def reply(self, text, **kwargs):
        raise RuntimeError("send failed")

class MemoryLedgerBotPathTests(unittest.TestCase):
    def setUp(self):
        self.old_db = bnl01_bot.DB_FILE
        self.tmp = tempfile.NamedTemporaryFile(delete=False)
        self.tmp.close()
        bnl01_bot.DB_FILE = self.tmp.name
        self.env = mock.patch.dict(os.environ, {}, clear=False)
        self.env.start()
        os.environ.pop("BNL_MEMORY_LEDGER_SHADOW_ENABLED", None)
        bnl01_bot.init_db()

    def tearDown(self):
        self.env.stop()
        bnl01_bot.DB_FILE = self.old_db
        try:
            os.unlink(self.tmp.name)
        except OSError:
            pass

    def rows(self, sql, params=()):
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            return conn.execute(sql, params).fetchall()

    def enable(self):
        os.environ["BNL_MEMORY_LEDGER_SHADOW_ENABLED"] = "1"

    def test_group_model_reply_is_one_room_source_with_all_participants(self):
        self.enable()

        decision = bnl01_bot.save_model_message(
            42,
            1,
            "One answer to the room.",
            channel_name="barcode-bot",
            channel_policy="public_home",
            channel_id=10,
            conversation_target_user_ids=(42, 43),
        )

        self.assertTrue(decision.save_conversation)
        self.assertEqual(
            self.rows(
                """
                SELECT user_id, role, content
                FROM conversations
                WHERE guild_id=1 AND role='model'
                """
            ),
            [(0, "model", "One answer to the room.")],
        )
        self.assertEqual(
            self.rows(
                """
                SELECT user_id
                FROM conversation_response_participants
                WHERE guild_id=1
                ORDER BY user_id
                """
            ),
            [(42,), (43,)],
        )
        ledger_rows = self.rows(
            """
            SELECT entry_id
            FROM memory_ledger_entries
            WHERE guild_id=1 AND source_table='conversations'
              AND source_role='model'
            """
        )
        self.assertEqual(len(ledger_rows), 1)
        self.assertEqual(
            self.rows(
                """
                SELECT participant_key, participant_role
                FROM memory_ledger_participants
                WHERE entry_id=?
                ORDER BY order_index
                """,
                (ledger_rows[0][0],),
            ),
            [
                ("bnl_01", "author"),
                ("discord_user:42", "conversation_target"),
                ("discord_user:43", "conversation_target"),
            ],
        )
        self.assertEqual(
            self.rows(
                "SELECT user_id FROM relationship_state WHERE guild_id=1"
            ),
            [],
        )
        self.assertEqual(
            self.rows(
                "SELECT user_id FROM memory_tiers WHERE guild_id=1"
            ),
            [],
        )
        self.assertEqual(bnl01_bot.get_conversation_history(42, 1), [])
        self.assertEqual(bnl01_bot.get_conversation_history(43, 1), [])

    def test_complete_delete_removes_group_response_and_all_associations_without_ledger(self):
        bnl01_bot.save_model_message(
            42,
            1,
            "Shared answer involving Crow.",
            channel_name="barcode-bot",
            channel_policy="public_home",
            channel_id=10,
            conversation_target_user_ids=(42, 43),
        )
        bnl01_bot.save_model_message(
            43,
            1,
            "Separate shared answer.",
            channel_name="barcode-bot",
            channel_policy="public_home",
            channel_id=10,
            conversation_target_user_ids=(43, 44),
        )
        target_row_id = self.rows(
            "SELECT id FROM conversations WHERE content='Shared answer involving Crow.'"
        )[0][0]
        other_row_id = self.rows(
            "SELECT id FROM conversations WHERE content='Separate shared answer.'"
        )[0][0]

        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            result = bnl01_bot.complete_delete_member_data(
                conn,
                guild_id=1,
                user_id=42,
                confirmation="DELETE MY BNL DATA 1",
            )

        self.assertTrue(result["ok"])
        self.assertEqual(
            result["row_counts"]["conversation_group_model_responses"],
            1,
        )
        self.assertEqual(
            result["row_counts"]["conversation_response_participants"],
            2,
        )
        self.assertEqual(
            self.rows(
                "SELECT id FROM conversations WHERE id IN (?,?) ORDER BY id",
                (target_row_id, other_row_id),
            ),
            [(other_row_id,)],
        )
        self.assertEqual(
            self.rows(
                """
                SELECT conversation_row_id, user_id
                FROM conversation_response_participants
                ORDER BY conversation_row_id, user_id
                """
            ),
            [(other_row_id, 43), (other_row_id, 44)],
        )

    def test_complete_delete_group_response_cleanup_is_atomic_with_ledger(self):
        self.enable()
        bnl01_bot.save_model_message(
            42,
            1,
            "Shared answer involving Crow.",
            channel_name="barcode-bot",
            channel_policy="public_home",
            channel_id=10,
            conversation_target_user_ids=(42, 43),
        )
        row_id = self.rows(
            "SELECT id FROM conversations WHERE content='Shared answer involving Crow.'"
        )[0][0]
        ledger_entry_id = self.rows(
            """
            SELECT entry_id
            FROM memory_ledger_entries
            WHERE guild_id=1 AND source_table='conversations'
              AND source_row_id=? AND source_role='model'
            """,
            (str(row_id),),
        )[0][0]

        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            with self.assertRaises(RuntimeError):
                bnl01_bot.complete_delete_member_data(
                    conn,
                    guild_id=1,
                    user_id=42,
                    confirmation="DELETE MY BNL DATA 1",
                    inject_failure=True,
                )

        self.assertEqual(
            self.rows("SELECT COUNT(*) FROM conversations WHERE id=?", (row_id,))[0][0],
            1,
        )
        self.assertEqual(
            self.rows(
                """
                SELECT COUNT(*)
                FROM conversation_response_participants
                WHERE conversation_row_id=?
                """,
                (row_id,),
            )[0][0],
            2,
        )
        self.assertEqual(
            self.rows(
                "SELECT COUNT(*) FROM memory_ledger_entries WHERE entry_id=?",
                (ledger_entry_id,),
            )[0][0],
            1,
        )

        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            result = bnl01_bot.complete_delete_member_data(
                conn,
                guild_id=1,
                user_id=42,
                confirmation="DELETE MY BNL DATA 1",
            )

        self.assertTrue(result["ok"])
        self.assertEqual(
            self.rows("SELECT COUNT(*) FROM conversations WHERE id=?", (row_id,))[0][0],
            0,
        )
        self.assertEqual(
            self.rows(
                """
                SELECT COUNT(*)
                FROM conversation_response_participants
                WHERE conversation_row_id=?
                """,
                (row_id,),
            )[0][0],
            0,
        )
        self.assertEqual(
            self.rows(
                "SELECT COUNT(*) FROM memory_ledger_entries WHERE entry_id=?",
                (ledger_entry_id,),
            )[0][0],
            0,
        )

    def test_clearhistory_and_prune_remove_group_response_associations(self):
        bnl01_bot.save_model_message(
            42,
            1,
            "Shared answer involving Crow.",
            channel_name="barcode-bot",
            channel_policy="public_home",
            channel_id=10,
            conversation_target_user_ids=(42, 43),
        )
        bnl01_bot.save_model_message(
            43,
            1,
            "Older unrelated shared answer.",
            channel_name="barcode-bot",
            channel_policy="public_home",
            channel_id=10,
            conversation_target_user_ids=(43, 44),
        )
        target_row_id = self.rows(
            "SELECT id FROM conversations WHERE content='Shared answer involving Crow.'"
        )[0][0]
        older_other_row_id = self.rows(
            "SELECT id FROM conversations WHERE content='Older unrelated shared answer.'"
        )[0][0]

        self.assertEqual(bnl01_bot.clear_user_history(42, 1), 1)
        self.assertEqual(
            self.rows("SELECT COUNT(*) FROM conversations WHERE id=?", (target_row_id,))[0][0],
            0,
        )
        self.assertEqual(
            self.rows(
                """
                SELECT COUNT(*)
                FROM conversation_response_participants
                WHERE conversation_row_id=?
                """,
                (target_row_id,),
            )[0][0],
            0,
        )
        self.assertEqual(
            self.rows(
                """
                SELECT user_id
                FROM conversation_response_participants
                WHERE conversation_row_id=?
                ORDER BY user_id
                """,
                (older_other_row_id,),
            ),
            [(43,), (44,)],
        )

        bnl01_bot.save_model_message(
            43,
            1,
            "Newer unrelated shared answer.",
            channel_name="barcode-bot",
            channel_policy="public_home",
            channel_id=10,
            conversation_target_user_ids=(43, 44),
        )
        newer_other_row_id = self.rows(
            "SELECT id FROM conversations WHERE content='Newer unrelated shared answer.'"
        )[0][0]
        bnl01_bot.prune_conversation_history(0, 1, max_rows=1)
        self.assertEqual(
            self.rows(
                "SELECT id FROM conversations WHERE user_id=0 ORDER BY id"
            ),
            [(newer_other_row_id,)],
        )
        self.assertEqual(
            self.rows(
                """
                SELECT conversation_row_id, user_id
                FROM conversation_response_participants
                ORDER BY conversation_row_id, user_id
                """
            ),
            [(newer_other_row_id, 43), (newer_other_row_id, 44)],
        )

    def test_single_speaker_model_reply_keeps_personal_storage_contract(self):
        self.enable()

        bnl01_bot.save_model_message(
            42,
            1,
            "One answer to Crow.",
            channel_name="barcode-bot",
            channel_policy="public_home",
            channel_id=10,
            conversation_target_user_ids=(42,),
        )

        self.assertEqual(
            self.rows(
                """
                SELECT user_id, role, content
                FROM conversations
                WHERE guild_id=1 AND role='model'
                """
            ),
            [(42, "model", "One answer to Crow.")],
        )
        self.assertEqual(
            bnl01_bot.get_conversation_history(42, 1),
            [{"role": "model", "parts": ["One answer to Crow."]}],
        )

    def _conversation_id(self, *, guild_id, user_id, content):
        rows = self.rows(
            """
            SELECT id
            FROM conversations
            WHERE guild_id=? AND user_id=? AND content=?
            ORDER BY id DESC
            LIMIT 1
            """,
            (guild_id, user_id, content),
        )
        self.assertEqual(len(rows), 1)
        return int(rows[0][0])

    def _attach_test_moment(self, *, guild_id, source_row_id):
        moment_id = f"mom_lifecycle_{guild_id}_{source_row_id}"
        timestamp = "2026-07-23T12:00:00+00:00"
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            moments.ensure_moment_schema(conn)
            raw_rows = conn.execute(
                """
                SELECT entry_id
                FROM memory_ledger_entries
                WHERE guild_id=? AND source_table='conversations'
                  AND source_row_id=?
                  AND predicate_key='conversation'
                  AND entry_type='observation'
                """,
                (guild_id, str(source_row_id)),
            ).fetchall()
            self.assertEqual(len(raw_rows), 1)
            raw_entry_id = str(raw_rows[0][0])
            canonical = ledger.insert_ledger_entry(
                conn,
                ledger.LedgerEntry(
                    guild_id=guild_id,
                    source_table="memory_moment_windows",
                    source_row_id=moment_id,
                    source_role="derived_assessment",
                    entry_type="shared_moment",
                    subject_key=f"moment:{moment_id}",
                    predicate_key="shared_moment",
                    value='{"summary":"source-bound test moment"}',
                    source_class=ledger.SourceClass.DERIVED_SUMMARY,
                    visibility=ledger.Visibility.PRIVATE,
                    confidence=ledger.Confidence.HIGH,
                    derived=True,
                    projection=True,
                    observed_at=timestamp,
                    lineage=(("derived_from", raw_entry_id),),
                ),
            )
            self.assertTrue(canonical.entry_id)
            conn.execute(
                """
                INSERT INTO memory_moment_windows (
                    moment_id, guild_id, channel_id, channel_name,
                    channel_policy, route_mode, topic_key,
                    window_started_at, last_activity_at,
                    qualification_reason, lifecycle_status, visibility,
                    public_usable, salience, human_entry_count,
                    model_entry_count, participant_count, summary,
                    created_at, updated_at, canonical_ledger_entry_id
                )
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    moment_id,
                    guild_id,
                    10,
                    "barcode-bot",
                    "public_home",
                    bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                    "topic_lifecycle_test",
                    timestamp,
                    timestamp,
                    "qualified_test_fixture",
                    "finalized",
                    "private",
                    1,
                    0.8,
                    1,
                    0,
                    1,
                    "source-bound test moment",
                    timestamp,
                    timestamp,
                    canonical.entry_id,
                ),
            )
            conn.execute(
                """
                INSERT INTO memory_moment_members (
                    moment_id, ledger_entry_id, source_sequence,
                    observed_at, membership_role, created_at
                )
                VALUES (?,?,?,?,?,?)
                """,
                (
                    moment_id,
                    raw_entry_id,
                    source_row_id,
                    timestamp,
                    "human",
                    timestamp,
                ),
            )
            conn.execute(
                """
                INSERT INTO memory_moment_participants (
                    moment_id, participant_key, safe_display_name,
                    participant_role, first_seen_at, last_seen_at,
                    authored_entry_count, participation_order,
                    created_at, updated_at
                )
                VALUES (?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    moment_id,
                    "discord_user:42",
                    "Crow",
                    "author",
                    timestamp,
                    timestamp,
                    1,
                    0,
                    timestamp,
                    timestamp,
                ),
            )
            conn.commit()
        return moment_id, raw_entry_id, canonical.entry_id

    def test_disabled_gate_save_user_message_creates_no_ledger(self):
        bnl01_bot.save_user_message(42, "Crow", 1, "remember this number: 8", channel_policy="sealed_test")
        self.assertEqual(self.rows("SELECT COUNT(*) FROM conversations")[0][0], 1)
        self.assertEqual(self.rows("SELECT COUNT(*) FROM memory_ledger_entries")[0][0], 0)

    def test_enabled_gate_save_user_message_inserts_and_dedup_receipts(self):
        self.enable()
        bnl01_bot.save_user_message(42, "Crow", 1, "remember this number: 8", channel_policy="sealed_test")
        self.assertEqual(
            self.rows(
                "SELECT predicate_key, normalized_value FROM memory_ledger_entries"
            )[0],
            ("conversation", "remember this number: 8"),
        )
        self.assertEqual(
            self.rows(
                "SELECT COUNT(*) FROM memory_ledger_entries WHERE predicate_key='remembered_number'"
            )[0][0],
            0,
        )
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            result = ledger.shadow_conversation_row(conn, row_id=1, user_id=42, user_name="Crow", guild_id=1, role="user", content="remember this number: 8", channel_policy="sealed_test")
            ledger.record_shadow_receipt(conn, guild_id=1, writer="test", source_table=result.source_table, source_row_id=result.source_row_id, source_revision=result.source_revision, outcome=result.outcome, reason_code=result.reason_code, entry_id=result.entry_id)
            conn.commit()
        outcomes = dict(self.rows("SELECT outcome, COUNT(*) FROM memory_ledger_shadow_receipts GROUP BY outcome"))
        self.assertEqual(outcomes.get("inserted"), 1)
        self.assertEqual(outcomes.get("deduplicated"), 1)

    def test_shadow_failure_keeps_legacy_conversation_and_records_error(self):
        self.enable()
        with mock.patch("bnl01_bot.shadow_conversation_row", side_effect=RuntimeError("boom")):
            bnl01_bot.save_user_message(42, "Crow", 1, "hello", channel_policy="public_home")
        self.assertEqual(self.rows("SELECT COUNT(*) FROM conversations")[0][0], 1)
        self.assertEqual(self.rows("SELECT outcome, reason_code FROM memory_ledger_shadow_receipts")[0], ("error", "shadow_exception"))

    def test_model_send_after_delivery_subject_and_participants(self):
        self.enable()
        asyncio.run(bnl01_bot.send_reply_then_save_model(_Message(), "I remember 8.", user_id=42, guild_id=1, channel_policy="sealed_test"))
        entry = self.rows("SELECT subject_key, source_role, derived, projection, public_usable FROM memory_ledger_entries")[0]
        self.assertEqual(entry, ("bnl_01", "model", 1, 1, 0))
        parts = self.rows("SELECT participant_key, participant_role FROM memory_ledger_participants ORDER BY order_index")
        self.assertEqual(parts[0], ("bnl_01", "author"))
        self.assertEqual(parts[1], ("discord_user:42", "conversation_target"))

    def test_failed_send_creates_no_model_conversation_or_ledger(self):
        self.enable()
        with self.assertRaises(RuntimeError):
            asyncio.run(bnl01_bot.send_reply_then_save_model(_FailMessage(), "No row", user_id=42, guild_id=1, channel_policy="sealed_test"))
        self.assertEqual(self.rows("SELECT COUNT(*) FROM conversations WHERE role='model'")[0][0], 0)
        self.assertEqual(self.rows("SELECT COUNT(*) FROM memory_ledger_entries")[0][0], 0)

    def test_direct_public_fact_is_source_linked_repetition_is_noop_and_change_supersedes(self):
        self.enable()
        bnl01_bot.save_user_message(
            42,
            "Crow",
            1,
            "my favorite color is green",
            channel_name="barcode-bot",
            channel_policy="public_home",
            channel_id=10,
            message_id=101,
            directed_to_bnl=True,
        )
        bnl01_bot.save_user_message(
            42,
            "Crow",
            1,
            "my favorite color is violet",
            channel_name="barcode-bot",
            channel_policy="public_home",
            channel_id=10,
            message_id=102,
            directed_to_bnl=True,
        )
        bnl01_bot.save_user_message(
            42,
            "Crow",
            1,
            "my favorite color is violet",
            channel_name="barcode-bot",
            channel_policy="public_home",
            channel_id=10,
            message_id=103,
            directed_to_bnl=True,
        )

        live_fact = self.rows(
            """
            SELECT fact_key, fact_value, is_core, source_conversation_row_id,
                   source_message_id, source_channel_policy, source_channel_id,
                   source_route_mode, source_kind, source_directed,
                   source_ledger_entry_id, lifecycle_status
            FROM user_memory_facts
            WHERE user_id=42 AND guild_id=1 AND fact_key='favorite_color'
            """
        )
        self.assertEqual(len(live_fact), 1)
        self.assertEqual(live_fact[0][:10], (
            "favorite_color",
            "violet",
            0,
            2,
            102,
            "public_home",
            10,
            bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
            "member_self_report",
            1,
        ))
        self.assertTrue(live_fact[0][10].startswith("mle_"))
        self.assertEqual(live_fact[0][11], "active")

        entries = self.rows(
            """
            SELECT entry_id, normalized_value, lifecycle_status, source_row_id,
                   source_message_id, public_usable
            FROM memory_ledger_entries
            WHERE source_role='member_self_report'
              AND predicate_key='favorite_color'
            ORDER BY source_sequence
            """
        )
        self.assertEqual(len(entries), 2)
        original_id, replacement_id = entries[0][0], entries[1][0]
        self.assertEqual(entries[0][1:], ("green", "superseded", "1", 101, 1))
        self.assertEqual(entries[1][1:], ("violet", "active", "2", 102, 1))
        self.assertEqual(
            set(self.rows(
                "SELECT lineage_type, target_entry_id FROM memory_ledger_lineage WHERE entry_id=?",
                (replacement_id,),
            )),
            {
                ("correction_of", original_id),
                ("supersedes", original_id),
            },
        )
        self.assertEqual(live_fact[0][10], replacement_id)

    def test_discord_queue_and_payment_discussion_is_not_content_excluded(self):
        self.enable()
        samples = [
            "is the queue open?",
            "payment is pending",
            "what's your availability?",
            "the current session was fun",
            "we were talking about the queue",
            "the DJ queue is full tonight",
        ]
        for idx, text in enumerate(samples, start=1):
            bnl01_bot.save_user_message(42, "Crow", 1, text, channel_policy="public_home", message_id=idx)
        rows = self.rows("SELECT normalized_value FROM memory_ledger_entries WHERE source_table='conversations' ORDER BY source_row_id")
        self.assertEqual([r[0] for r in rows], samples)
        self.assertEqual(self.rows("SELECT COUNT(*) FROM memory_ledger_shadow_receipts WHERE reason_code='queue_operational_state_excluded'")[0][0], 0)

    def test_partial_shadow_failure_rolls_back_and_records_safe_error(self):
        self.enable()
        def partial_then_fail(conn):
            ledger.shadow_conversation_row(conn, row_id=1, user_id=42, user_name="Crow", guild_id=1, role="user", content="remember this number: 8", channel_policy="sealed_test")
            raise RuntimeError("boom with PRIVATE CONTENT")
        bnl01_bot.save_user_message(42, "Crow", 1, "legacy survives", channel_policy="sealed_test")
        bnl01_bot._shadow_memory_ledger_write("partial_failure_test", partial_then_fail, guild_id=1, source_table="conversations", source_row_id=999, source_revision="999")
        self.assertEqual(self.rows("SELECT COUNT(*) FROM conversations")[0][0], 1)
        self.assertEqual(self.rows("SELECT COUNT(*) FROM memory_ledger_entries WHERE normalized_value='remember this number: 8'")[0][0], 0)
        self.assertEqual(self.rows("SELECT outcome, reason_code FROM memory_ledger_shadow_receipts WHERE writer='partial_failure_test'")[0], ("error", "shadow_exception"))
        self.assertNotIn("PRIVATE CONTENT", str(self.rows("SELECT * FROM memory_ledger_shadow_receipts")))

    def test_broadcast_insert_cleaned_summary_and_raw_absent_from_receipts(self):
        self.enable()
        msg = _Message("RAW SECRET note")
        new_id = bnl01_bot.add_broadcast_memory_entry(1, msg, {"cleaned_summary": "Clean show summary.", "entry_type": "show_note", "public_safe": True, "usage_scope": "public"})
        self.assertGreater(new_id, 0)
        self.assertEqual(self.rows("SELECT normalized_value FROM memory_ledger_entries WHERE source_table='broadcast_memory'")[0][0], "Clean show summary.")
        dump = str(self.rows("SELECT * FROM memory_ledger_shadow_receipts"))
        self.assertNotIn("RAW SECRET", dump)

    def test_broadcast_replacement_has_one_revision_and_real_lineage(self):
        self.enable()
        original_id = bnl01_bot.add_broadcast_memory_entry(
            1,
            _Message("RAW original"),
            {
                "cleaned_summary": "Original safe summary.",
                "entry_type": "show_note",
                "public_safe": True,
                "usage_scope": "ambient,direct",
            },
        )
        replacement_id = bnl01_bot.add_broadcast_memory_entry(
            1,
            _Message("RAW replacement"),
            {
                "cleaned_summary": "Replacement safe summary.",
                "entry_type": "show_note",
                "public_safe": True,
                "usage_scope": "ambient,direct",
                "supersedes_id": original_id,
            },
        )
        updated_at = "2026-07-16T00:00:00-07:00"
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            conn.execute(
                "UPDATE broadcast_memory SET status='superseded', superseded_by_id=?, updated_at=? WHERE id=?",
                (replacement_id, updated_at, original_id),
            )
            conn.commit()
        bnl01_bot._shadow_memory_ledger_write(
            "broadcast_memory_status",
            lambda ledger_conn: ledger.shadow_broadcast_status_event(
                ledger_conn,
                row_id=original_id,
                guild_id=1,
                status="superseded",
                updated_at=updated_at,
                actor_id=42,
                actor_name="Crow",
                superseded_by_id=replacement_id,
            ),
            guild_id=1,
            source_table="broadcast_memory",
            source_row_id=original_id,
            source_revision=f"event:status:superseded:{updated_at.lower()}",
            source_event_key="status:superseded",
        )
        primary_rows = self.rows(
            """
            SELECT source_row_id, entry_id, normalized_value
            FROM memory_ledger_entries
            WHERE source_table='broadcast_memory' AND source_role='broadcast_memory'
            ORDER BY CAST(source_row_id AS INTEGER)
            """
        )
        self.assertEqual(len(primary_rows), 2)
        original_entry = primary_rows[0][1]
        replacement_entry = primary_rows[1][1]
        self.assertEqual(primary_rows[0][0], str(original_id))
        self.assertEqual(primary_rows[1][0], str(replacement_id))
        self.assertEqual(primary_rows[1][2], "Replacement safe summary.")
        self.assertEqual(
            self.rows(
                "SELECT COUNT(*) FROM memory_ledger_entries WHERE source_table='broadcast_memory' AND source_role='broadcast_memory' AND source_row_id=?",
                (str(replacement_id),),
            )[0][0],
            1,
        )
        self.assertEqual(
            self.rows(
                "SELECT COUNT(*) FROM memory_ledger_entries WHERE source_table='broadcast_memory' AND source_role='broadcast_memory_status' AND source_row_id=?",
                (str(original_id),),
            )[0][0],
            1,
        )
        self.assertEqual(
            set(self.rows("SELECT lineage_type, target_entry_id FROM memory_ledger_lineage WHERE entry_id=?", (replacement_entry,))),
            {("correction_of", original_entry), ("supersedes", original_entry)},
        )
        self.assertEqual(
            self.rows(
                """
                SELECT COUNT(*)
                FROM memory_ledger_lineage l
                LEFT JOIN memory_ledger_entries e
                  ON e.guild_id=l.guild_id AND e.entry_id=l.target_entry_id
                WHERE e.entry_id IS NULL
                """
            )[0][0],
            0,
        )
        effective_active = self.rows(
            """
            SELECT entry_id
            FROM memory_ledger_entries
            WHERE source_table='broadcast_memory'
              AND source_role='broadcast_memory'
              AND lifecycle_status='active'
              AND entry_id NOT IN (
                SELECT target_entry_id FROM memory_ledger_lineage
                WHERE lineage_type IN ('supersedes', 'retracts')
              )
            """
        )
        self.assertEqual(effective_active, [(replacement_entry,)])

    def test_prune_purges_exact_raw_source_and_moment_before_transcript_delete(self):
        self.enable()
        bnl01_bot.save_user_message(
            42,
            "Crow",
            1,
            "my favorite color is green",
            channel_name="barcode-bot",
            channel_policy="public_home",
            channel_id=10,
            message_id=101,
            directed_to_bnl=True,
        )
        old_row_id = self._conversation_id(
            guild_id=1,
            user_id=42,
            content="my favorite color is green",
        )
        bnl01_bot.save_user_message(
            42,
            "Crow",
            1,
            "newer target row",
            channel_policy="sealed_test",
        )
        keep_row_id = self._conversation_id(
            guild_id=1,
            user_id=42,
            content="newer target row",
        )
        bnl01_bot.save_user_message(
            43,
            "Other",
            1,
            "same guild other member",
            channel_policy="sealed_test",
        )
        other_member_row_id = self._conversation_id(
            guild_id=1,
            user_id=43,
            content="same guild other member",
        )
        bnl01_bot.save_user_message(
            42,
            "Crow",
            2,
            "same member other guild",
            channel_policy="sealed_test",
        )
        other_guild_row_id = self._conversation_id(
            guild_id=2,
            user_id=42,
            content="same member other guild",
        )
        moment_id, raw_entry_id, canonical_entry_id = self._attach_test_moment(
            guild_id=1,
            source_row_id=old_row_id,
        )
        scalar_entry_id = self.rows(
            """
            SELECT entry_id
            FROM memory_ledger_entries
            WHERE guild_id=1
              AND source_table='conversations'
              AND source_row_id=?
              AND predicate_key='favorite_color'
              AND entry_type='preference'
            """,
            (str(old_row_id),),
        )[0][0]
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            legacy_raw = ledger.insert_ledger_entry(
                conn,
                ledger.LedgerEntry(
                    guild_id=1,
                    source_table="conversations",
                    source_row_id=old_row_id,
                    source_revision=str(old_row_id),
                    source_role="user",
                    entry_type="observation",
                    subject_key=ledger.subject_key_for_user(42),
                    predicate_key="remembered_number",
                    value="1234",
                    source_class=ledger.SourceClass.PUBLIC_OBSERVATION,
                    visibility=ledger.Visibility.PUBLIC_SAFE,
                    confidence=ledger.Confidence.MEDIUM,
                    public_usable=False,
                    observed_at="2026-07-23T00:00:00+00:00",
                ),
            )
            conn.commit()
        self.assertTrue(legacy_raw.entry_id)

        real_purge = bnl01_bot.purge_conversation_ledger_sources
        observed = {}

        def inspect_then_purge(conn, *, guild_id, source_row_ids, reason):
            exact_ids = tuple(sorted(int(row_id) for row_id in source_row_ids))
            placeholders = ",".join("?" for _ in exact_ids)
            observed["guild_id"] = guild_id
            observed["source_row_ids"] = exact_ids
            observed["reason"] = reason
            observed["transcripts_before"] = tuple(
                int(row[0])
                for row in conn.execute(
                    f"""
                    SELECT id FROM conversations
                    WHERE guild_id=? AND id IN ({placeholders})
                    ORDER BY id
                    """,
                    (guild_id, *exact_ids),
                ).fetchall()
            )
            result = real_purge(
                conn,
                guild_id=guild_id,
                source_row_ids=source_row_ids,
                reason=reason,
            )
            observed["raw_after_purge"] = conn.execute(
                "SELECT COUNT(*) FROM memory_ledger_entries WHERE entry_id=?",
                (raw_entry_id,),
            ).fetchone()[0]
            observed["legacy_raw_after_purge"] = conn.execute(
                "SELECT COUNT(*) FROM memory_ledger_entries WHERE entry_id=?",
                (legacy_raw.entry_id,),
            ).fetchone()[0]
            observed["moment_after_purge"] = conn.execute(
                """
                SELECT lifecycle_status, summary, public_usable
                FROM memory_moment_windows
                WHERE moment_id=?
                """,
                (moment_id,),
            ).fetchone()
            return result

        with mock.patch(
            "bnl01_bot.purge_conversation_ledger_sources",
            side_effect=inspect_then_purge,
        ):
            bnl01_bot.prune_conversation_history(42, 1, max_rows=1)

        self.assertEqual(observed["guild_id"], 1)
        self.assertEqual(observed["source_row_ids"], (old_row_id,))
        self.assertEqual(observed["transcripts_before"], (old_row_id,))
        self.assertEqual(observed["reason"], "bounded_conversation_prune")
        self.assertEqual(observed["raw_after_purge"], 0)
        self.assertEqual(observed["legacy_raw_after_purge"], 0)
        self.assertEqual(observed["moment_after_purge"], ("retracted", "", 0))
        self.assertEqual(
            self.rows(
                "SELECT id FROM conversations WHERE guild_id=1 AND user_id=42 ORDER BY id"
            ),
            [(keep_row_id,)],
        )
        self.assertEqual(
            self.rows(
                "SELECT id FROM conversations WHERE id IN (?,?) ORDER BY id",
                (other_member_row_id, other_guild_row_id),
            ),
            [(other_member_row_id,), (other_guild_row_id,)],
        )
        self.assertEqual(
            self.rows(
                """
                SELECT fact_value, source_conversation_row_id,
                       source_ledger_entry_id, lifecycle_status
                FROM user_memory_facts
                WHERE guild_id=1 AND user_id=42 AND fact_key='favorite_color'
                """
            ),
            [("green", old_row_id, scalar_entry_id, "active")],
        )
        self.assertEqual(
            self.rows(
                """
                SELECT normalized_value, lifecycle_status
                FROM memory_ledger_entries
                WHERE entry_id=?
                """,
                (scalar_entry_id,),
            ),
            [("green", "active")],
        )
        self.assertEqual(
            self.rows(
                """
                SELECT normalized_value, lifecycle_status, public_usable
                FROM memory_ledger_entries
                WHERE entry_id=?
                """,
                (canonical_entry_id,),
            ),
            [("", "retracted", 0)],
        )
        self.assertEqual(
            self.rows(
                "SELECT COUNT(*) FROM memory_moment_members WHERE moment_id=?",
                (moment_id,),
            )[0][0],
            0,
        )

    def test_clear_user_and_guild_purge_exact_rows_before_delete_and_preserve_isolation(self):
        self.enable()
        fixtures = (
            (42, "Crow", 1, "target one"),
            (42, "Crow", 1, "target two"),
            (43, "Other", 1, "same guild survivor"),
            (42, "Crow", 2, "cross guild survivor"),
        )
        row_ids = {}
        for user_id, user_name, guild_id, content in fixtures:
            bnl01_bot.save_user_message(
                user_id,
                user_name,
                guild_id,
                content,
                channel_policy="sealed_test",
            )
            row_ids[content] = self._conversation_id(
                guild_id=guild_id,
                user_id=user_id,
                content=content,
            )

        real_purge = bnl01_bot.purge_conversation_ledger_sources
        calls = []

        def inspect_then_purge(conn, *, guild_id, source_row_ids, reason):
            exact_ids = tuple(sorted(int(row_id) for row_id in source_row_ids))
            placeholders = ",".join("?" for _ in exact_ids)
            present = tuple(
                int(row[0])
                for row in conn.execute(
                    f"""
                    SELECT id FROM conversations
                    WHERE guild_id=? AND id IN ({placeholders})
                    ORDER BY id
                    """,
                    (guild_id, *exact_ids),
                ).fetchall()
            )
            calls.append((reason, guild_id, exact_ids, present))
            return real_purge(
                conn,
                guild_id=guild_id,
                source_row_ids=source_row_ids,
                reason=reason,
            )

        with mock.patch(
            "bnl01_bot.purge_conversation_ledger_sources",
            side_effect=inspect_then_purge,
        ):
            self.assertEqual(bnl01_bot.clear_user_history(42, 1), 2)
            self.assertEqual(
                self.rows(
                    "SELECT id FROM conversations WHERE guild_id=1 ORDER BY id"
                ),
                [(row_ids["same guild survivor"],)],
            )
            self.assertEqual(
                self.rows(
                    """
                    SELECT source_row_id
                    FROM memory_ledger_entries
                    WHERE guild_id=1 AND source_table='conversations'
                      AND predicate_key='conversation'
                    ORDER BY CAST(source_row_id AS INTEGER)
                    """
                ),
                [(str(row_ids["same guild survivor"]),)],
            )
            self.assertEqual(bnl01_bot.clear_guild_history(1), 1)

        target_ids = tuple(sorted((row_ids["target one"], row_ids["target two"])))
        self.assertEqual(
            calls[0],
            ("clear_user_history", 1, target_ids, target_ids),
        )
        survivor_id = row_ids["same guild survivor"]
        self.assertEqual(
            calls[1],
            ("clear_guild_history", 1, (survivor_id,), (survivor_id,)),
        )
        cross_guild_id = row_ids["cross guild survivor"]
        self.assertEqual(
            self.rows(
                "SELECT guild_id, user_id, id FROM conversations ORDER BY id"
            ),
            [(2, 42, cross_guild_id)],
        )
        self.assertEqual(
            self.rows(
                """
                SELECT guild_id, source_row_id
                FROM memory_ledger_entries
                WHERE source_table='conversations'
                  AND predicate_key='conversation'
                ORDER BY guild_id, CAST(source_row_id AS INTEGER)
                """
            ),
            [(2, str(cross_guild_id))],
        )

    def test_prune_lifecycle_failure_rolls_back_ledger_moment_and_transcript(self):
        self.enable()
        bnl01_bot.save_user_message(
            42,
            "Crow",
            1,
            "old row must survive failed purge",
            channel_policy="sealed_test",
        )
        old_row_id = self._conversation_id(
            guild_id=1,
            user_id=42,
            content="old row must survive failed purge",
        )
        bnl01_bot.save_user_message(
            42,
            "Crow",
            1,
            "new row also survives",
            channel_policy="sealed_test",
        )
        keep_row_id = self._conversation_id(
            guild_id=1,
            user_id=42,
            content="new row also survives",
        )
        moment_id, raw_entry_id, canonical_entry_id = self._attach_test_moment(
            guild_id=1,
            source_row_id=old_row_id,
        )
        real_purge = bnl01_bot.purge_conversation_ledger_sources
        observed = {}

        def partial_then_fail(conn, *, guild_id, source_row_ids, reason):
            observed["transcript_before"] = conn.execute(
                "SELECT COUNT(*) FROM conversations WHERE guild_id=? AND id=?",
                (guild_id, old_row_id),
            ).fetchone()[0]
            real_purge(
                conn,
                guild_id=guild_id,
                source_row_ids=source_row_ids,
                reason=reason,
            )
            observed["raw_removed_inside_transaction"] = conn.execute(
                "SELECT COUNT(*) FROM memory_ledger_entries WHERE entry_id=?",
                (raw_entry_id,),
            ).fetchone()[0]
            raise RuntimeError("injected lifecycle failure")

        with mock.patch(
            "bnl01_bot.purge_conversation_ledger_sources",
            side_effect=partial_then_fail,
        ):
            bnl01_bot.prune_conversation_history(42, 1, max_rows=1)

        self.assertEqual(observed["transcript_before"], 1)
        self.assertEqual(observed["raw_removed_inside_transaction"], 0)
        self.assertEqual(
            self.rows(
                "SELECT id FROM conversations WHERE guild_id=1 AND user_id=42 ORDER BY id"
            ),
            [(old_row_id,), (keep_row_id,)],
        )
        self.assertEqual(
            self.rows(
                "SELECT normalized_value, lifecycle_status FROM memory_ledger_entries WHERE entry_id=?",
                (raw_entry_id,),
            ),
            [("old row must survive failed purge", "active")],
        )
        self.assertEqual(
            self.rows(
                """
                SELECT lifecycle_status, summary, public_usable
                FROM memory_moment_windows WHERE moment_id=?
                """,
                (moment_id,),
            ),
            [("finalized", "source-bound test moment", 1)],
        )
        self.assertEqual(
            self.rows(
                "SELECT normalized_value, lifecycle_status FROM memory_ledger_entries WHERE entry_id=?",
                (canonical_entry_id,),
            ),
            [('{"summary":"source-bound test moment"}', "active")],
        )
        self.assertEqual(
            self.rows(
                "SELECT ledger_entry_id FROM memory_moment_members WHERE moment_id=?",
                (moment_id,),
            ),
            [(raw_entry_id,)],
        )

    def test_init_db_reconciles_preexisting_raw_orphan_without_deleting_scalar_or_live_rows(self):
        self.enable()
        bnl01_bot.save_user_message(
            42,
            "Crow",
            1,
            "my favorite color is green",
            channel_name="barcode-bot",
            channel_policy="public_home",
            channel_id=10,
            message_id=101,
            directed_to_bnl=True,
        )
        orphan_source_row_id = self._conversation_id(
            guild_id=1,
            user_id=42,
            content="my favorite color is green",
        )
        bnl01_bot.save_user_message(
            43,
            "Other",
            1,
            "live same guild row",
            channel_policy="sealed_test",
        )
        live_same_guild_row_id = self._conversation_id(
            guild_id=1,
            user_id=43,
            content="live same guild row",
        )
        bnl01_bot.save_user_message(
            42,
            "Crow",
            2,
            "live cross guild row",
            channel_policy="sealed_test",
        )
        live_cross_guild_row_id = self._conversation_id(
            guild_id=2,
            user_id=42,
            content="live cross guild row",
        )
        moment_id, orphan_raw_entry_id, canonical_entry_id = (
            self._attach_test_moment(
                guild_id=1,
                source_row_id=orphan_source_row_id,
            )
        )
        scalar_entry_id = self.rows(
            """
            SELECT source_ledger_entry_id
            FROM user_memory_facts
            WHERE guild_id=1 AND user_id=42 AND fact_key='favorite_color'
            """
        )[0][0]
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            legacy_orphan = ledger.insert_ledger_entry(
                conn,
                ledger.LedgerEntry(
                    guild_id=1,
                    source_table="conversations",
                    source_row_id=orphan_source_row_id,
                    source_revision=str(orphan_source_row_id),
                    source_role="user",
                    entry_type="observation",
                    subject_key=ledger.subject_key_for_user(42),
                    predicate_key="remembered_number",
                    value="1234",
                    source_class=ledger.SourceClass.PUBLIC_OBSERVATION,
                    visibility=ledger.Visibility.PUBLIC_SAFE,
                    confidence=ledger.Confidence.MEDIUM,
                    public_usable=False,
                    observed_at="2026-07-23T00:00:00+00:00",
                ),
            )
            conn.execute(
                "DELETE FROM conversations WHERE guild_id=1 AND id=?",
                (orphan_source_row_id,),
            )
            conn.commit()
        self.assertTrue(legacy_orphan.entry_id)
        self.assertEqual(
            self.rows(
                "SELECT COUNT(*) FROM memory_ledger_entries WHERE entry_id=?",
                (orphan_raw_entry_id,),
            )[0][0],
            1,
        )
        self.assertEqual(
            self.rows(
                "SELECT COUNT(*) FROM memory_ledger_entries WHERE entry_id=?",
                (legacy_orphan.entry_id,),
            )[0][0],
            1,
        )

        bnl01_bot.init_db()

        self.assertEqual(
            self.rows(
                "SELECT COUNT(*) FROM memory_ledger_entries WHERE entry_id=?",
                (orphan_raw_entry_id,),
            )[0][0],
            0,
        )
        self.assertEqual(
            self.rows(
                "SELECT COUNT(*) FROM memory_ledger_entries WHERE entry_id=?",
                (legacy_orphan.entry_id,),
            )[0][0],
            0,
        )
        self.assertEqual(
            self.rows(
                """
                SELECT fact_value, source_conversation_row_id,
                       source_ledger_entry_id, lifecycle_status
                FROM user_memory_facts
                WHERE guild_id=1 AND user_id=42 AND fact_key='favorite_color'
                """
            ),
            [("green", orphan_source_row_id, scalar_entry_id, "active")],
        )
        self.assertEqual(
            self.rows(
                "SELECT normalized_value, lifecycle_status FROM memory_ledger_entries WHERE entry_id=?",
                (scalar_entry_id,),
            ),
            [("green", "active")],
        )
        self.assertEqual(
            self.rows(
                """
                SELECT guild_id, source_row_id
                FROM memory_ledger_entries
                WHERE source_table='conversations'
                  AND predicate_key='conversation'
                ORDER BY guild_id, CAST(source_row_id AS INTEGER)
                """
            ),
            [
                (1, str(live_same_guild_row_id)),
                (2, str(live_cross_guild_row_id)),
            ],
        )
        self.assertEqual(
            self.rows(
                """
                SELECT lifecycle_status, summary, public_usable
                FROM memory_moment_windows WHERE moment_id=?
                """,
                (moment_id,),
            ),
            [("retracted", "", 0)],
        )
        self.assertEqual(
            self.rows(
                """
                SELECT normalized_value, lifecycle_status, public_usable
                FROM memory_ledger_entries WHERE entry_id=?
                """,
                (canonical_entry_id,),
            ),
            [("", "retracted", 0)],
        )

        stable_snapshot = (
            self.rows(
                "SELECT entry_id, lifecycle_status FROM memory_ledger_entries ORDER BY entry_id"
            ),
            self.rows(
                """
                SELECT moment_id, lifecycle_status, summary
                FROM memory_moment_windows ORDER BY moment_id
                """
            ),
        )
        bnl01_bot.init_db()
        self.assertEqual(
            (
                self.rows(
                    "SELECT entry_id, lifecycle_status FROM memory_ledger_entries ORDER BY entry_id"
                ),
                self.rows(
                    """
                    SELECT moment_id, lifecycle_status, summary
                    FROM memory_moment_windows ORDER BY moment_id
                    """
                ),
            ),
            stable_snapshot,
        )

    def test_report_is_guild_scoped_and_counts_real_outcomes(self):
        self.enable()
        bnl01_bot.save_user_message(42, "Crow", 1, "remember this number: 8", channel_policy="sealed_test")
        bnl01_bot.save_user_message(43, "Other", 2, "remember this number: 9", channel_policy="sealed_test")
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            r1 = ledger.build_memory_ledger_evaluation(conn, guild_id=1)
            r2 = ledger.build_memory_ledger_evaluation(conn, guild_id=2)
        self.assertEqual(r1["eligibleLegacyWrites"], 1)
        self.assertEqual(r2["eligibleLegacyWrites"], 1)
        self.assertEqual(r1["insertedLedgerEntries"], 1)

if __name__ == "__main__":
    unittest.main()
