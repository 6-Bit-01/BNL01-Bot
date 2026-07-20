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

    def test_disabled_gate_save_user_message_creates_no_ledger(self):
        bnl01_bot.save_user_message(42, "Crow", 1, "remember this number: 8", channel_policy="sealed_test")
        self.assertEqual(self.rows("SELECT COUNT(*) FROM conversations")[0][0], 1)
        self.assertEqual(self.rows("SELECT COUNT(*) FROM memory_ledger_entries")[0][0], 0)

    def test_enabled_gate_save_user_message_inserts_and_dedup_receipts(self):
        self.enable()
        bnl01_bot.save_user_message(42, "Crow", 1, "remember this number: 8", channel_policy="sealed_test")
        self.assertEqual(self.rows("SELECT normalized_value FROM memory_ledger_entries")[0][0], "8")
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

    def test_mutable_fact_update_creates_second_revision_but_same_revision_is_idempotent(self):
        self.enable()
        bnl01_bot.upsert_user_fact(42, 1, "favorite_number", "8", 0.8)
        bnl01_bot.upsert_user_fact(42, 1, "favorite_number", "9", 0.8)
        values = self.rows("SELECT normalized_value FROM memory_ledger_entries WHERE source_table='user_memory_facts' ORDER BY source_revision")
        self.assertEqual([v[0] for v in values], ["8", "9"])

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
        self.assertEqual(self.rows("SELECT COUNT(*) FROM memory_ledger_entries WHERE normalized_value='8'")[0][0], 0)
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
