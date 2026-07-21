import os
import sqlite3
import unittest
from unittest import mock

import bnl_memory_ledger as ledger


class MemoryLedgerV1Tests(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        ledger.ensure_memory_ledger_schema(self.conn)

    def tearDown(self):
        self.conn.close()

    def count_entries(self):
        return self.conn.execute("SELECT COUNT(*) FROM memory_ledger_entries").fetchone()[0]

    def test_schema_idempotent_and_stable_id(self):
        ledger.ensure_memory_ledger_schema(self.conn)
        one = ledger.stable_entry_id(guild_id=1, source_table="conversations", source_row_id=500, entry_type="observation", subject_key="discord_user:7", predicate_key="remembered_number")
        two = ledger.stable_entry_id(guild_id=1, source_table="conversations", source_row_id=500, entry_type="observation", subject_key="discord_user:7", predicate_key="remembered_number")
        self.assertEqual(one, two)

    def test_exact_source_dedup_but_separate_rows_survive(self):
        kwargs = dict(user_id=7, user_name="Crow", guild_id=1, role="user", content="remember this number: 8", channel_policy="sealed_test")
        first = ledger.shadow_conversation_row(self.conn, row_id=500, **kwargs)
        duplicate = ledger.shadow_conversation_row(self.conn, row_id=500, **kwargs)
        second = ledger.shadow_conversation_row(self.conn, row_id=501, **kwargs)
        self.conn.commit()
        self.assertEqual(first.entry_id, duplicate.entry_id)
        self.assertEqual(duplicate.outcome, "deduplicated")
        self.assertNotEqual(first.entry_id, second.entry_id)
        self.assertEqual(self.count_entries(), 2)

    def test_number_sequence_is_additive_not_supersession(self):
        ledger.shadow_conversation_row(self.conn, row_id=1, user_id=7, user_name="Crow", guild_id=1, role="user", content="remember this number: 731946", channel_policy="sealed_test", observed_at="2026-01-01T00:00:01+00:00")
        ledger.shadow_conversation_row(self.conn, row_id=2, user_id=7, user_name="Crow", guild_id=1, role="user", content="remember this number: 284517", channel_policy="sealed_test", observed_at="2026-01-01T00:00:02+00:00")
        rows = self.conn.execute("SELECT normalized_value, lifecycle_status FROM memory_ledger_entries ORDER BY source_sequence").fetchall()
        self.assertEqual(rows, [("731946", "active"), ("284517", "active")])
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM memory_ledger_lineage WHERE lineage_type IN ('supersedes','correction_of')").fetchone()[0], 0)

    def test_model_row_is_derived_and_cannot_establish_user_fact(self):
        ledger.shadow_conversation_row(self.conn, row_id=3, user_id=7, user_name="BNL-01", guild_id=1, role="model", content="I remember 284517.", channel_policy="sealed_test")
        row = self.conn.execute("SELECT entry_type, source_role, derived, projection, predicate_key, public_usable FROM memory_ledger_entries").fetchone()
        self.assertEqual(row, ("derived_summary", "model", 1, 1, "model_output", 0))

    def test_participants_are_queryable_and_ordered(self):
        entry = ledger.LedgerEntry(guild_id=1, source_table="manual", source_row_id=1, source_role="operator", entry_type="shared_moment", subject_key="moment:1", predicate_key="manual_shared_moment", participants=(ledger.LedgerParticipant("discord_user:2", "B", "participant", 1), ledger.LedgerParticipant("discord_user:1", "A", "participant", 0)))
        ledger.insert_ledger_entry(self.conn, entry)
        rows = self.conn.execute("SELECT participant_key FROM memory_ledger_participants WHERE entry_id=? ORDER BY order_index", (entry.entry_id,)).fetchall()
        self.assertEqual([r[0] for r in rows], ["discord_user:1", "discord_user:2"])

    def test_broadcast_uses_cleaned_summary_not_raw_note(self):
        ledger.shadow_broadcast_memory_row(self.conn, row_id=9, guild_id=1, cleaned_summary="Safe show summary", entry_type="show_note", public_safe=True, status="active", usage_scope="ambient,direct")
        row = self.conn.execute("SELECT normalized_value, public_usable, visibility FROM memory_ledger_entries").fetchone()
        self.assertEqual(row, ("Safe show summary", 1, "public_safe"))

    def test_queue_discussion_is_conversation_not_website_state(self):
        ledger.shadow_conversation_row(self.conn, row_id=10, user_id=7, user_name="Crow", guild_id=1, role="user", content="We joked that the queue is haunted.", channel_policy="public_home")
        row = self.conn.execute("SELECT source_table, predicate_key FROM memory_ledger_entries").fetchone()
        self.assertEqual(row, ("conversations", "conversation"))


    def test_single_and_twelve_digit_numbers_are_supported(self):
        r1 = ledger.shadow_conversation_row(self.conn, row_id=20, user_id=7, user_name="Crow", guild_id=1, role="user", content="Remember this number: 8", channel_policy="sealed_test")
        r2 = ledger.shadow_conversation_row(self.conn, row_id=21, user_id=7, user_name="Crow", guild_id=1, role="user", content="remember this number: 123456789012", channel_policy="sealed_test")
        self.assertEqual((r1.outcome, r2.outcome), ("inserted", "inserted"))
        values = [r[0] for r in self.conn.execute("SELECT normalized_value FROM memory_ledger_entries ORDER BY source_sequence").fetchall()]
        self.assertEqual(values, ["8", "123456789012"])

    def test_bare_remember_number_is_durable_but_declarative_and_questions_are_not(self):
        ledger.shadow_conversation_row(self.conn, row_id=22, user_id=7, user_name="Crow", guild_id=1, role="user", content="remember 8", channel_policy="sealed_test")
        ledger.shadow_conversation_row(self.conn, row_id=23, user_id=7, user_name="Crow", guild_id=1, role="user", content="do you remember 9?", channel_policy="sealed_test")
        ledger.shadow_conversation_row(self.conn, row_id=24, user_id=7, user_name="Crow", guild_id=1, role="user", content="I remember 10 people", channel_policy="sealed_test")
        ledger.shadow_conversation_row(self.conn, row_id=25, user_id=7, user_name="Crow", guild_id=1, role="user", content="I do not remember 11", channel_policy="sealed_test")
        ledger.shadow_conversation_row(self.conn, row_id=26, user_id=7, user_name="Crow", guild_id=1, role="user", content="what's up? / remember 12", channel_policy="sealed_test")
        ledger.shadow_conversation_row(self.conn, row_id=27, user_id=7, user_name="Crow", guild_id=1, role="user", content="remember 13 / what's up?", channel_policy="sealed_test")
        ledger.shadow_conversation_row(self.conn, row_id=28, user_id=7, user_name="Crow", guild_id=1, role="user", content="hey BNL, please remember the number 14", channel_policy="sealed_test")
        ledger.shadow_conversation_row(self.conn, row_id=29, user_id=7, user_name="Crow", guild_id=1, role="user", content="remember 15 for me", channel_policy="sealed_test")
        ledger.shadow_conversation_row(self.conn, row_id=30, user_id=7, user_name="Crow", guild_id=1, role="user", content=", remember 16", channel_policy="sealed_test")
        ledger.shadow_conversation_row(self.conn, row_id=31, user_id=7, user_name="Crow", guild_id=1, role="user", content="Hey, BNL, remember 17", channel_policy="sealed_test")
        ledger.shadow_conversation_row(self.conn, row_id=32, user_id=7, user_name="Crow", guild_id=1, role="user", content="bnlremember 18", channel_policy="sealed_test")
        ledger.shadow_conversation_row(self.conn, row_id=33, user_id=7, user_name="Crow", guild_id=1, role="user", content="bnlplease remember 19", channel_policy="sealed_test")
        rows = self.conn.execute(
            "SELECT predicate_key, normalized_value FROM memory_ledger_entries ORDER BY source_sequence"
        ).fetchall()
        self.assertEqual(rows[0], ("remembered_number", "8"))
        self.assertEqual(rows[1], ("conversation", "do you remember 9?"))
        self.assertEqual(rows[2], ("conversation", "I remember 10 people"))
        self.assertEqual(rows[3], ("conversation", "I do not remember 11"))
        self.assertEqual(rows[4], ("remembered_number", "12"))
        self.assertEqual(rows[5], ("remembered_number", "13"))
        self.assertEqual(rows[6], ("remembered_number", "14"))
        self.assertEqual(rows[7], ("remembered_number", "15"))
        self.assertEqual(rows[8], ("remembered_number", "16"))
        self.assertEqual(rows[9], ("remembered_number", "17"))
        self.assertEqual(rows[10], ("conversation", "bnlremember 18"))
        self.assertEqual(rows[11], ("conversation", "bnlplease remember 19"))

    def test_explicit_unique_correction_links_and_ambiguous_does_not(self):
        ledger.shadow_conversation_row(self.conn, row_id=30, user_id=7, user_name="Crow", guild_id=1, role="user", content="remember this number: 731946", channel_policy="sealed_test")
        linked = ledger.shadow_conversation_row(self.conn, row_id=31, user_id=7, user_name="Crow", guild_id=1, role="user", content="Correction: the number is 284517, not 731946", channel_policy="sealed_test")
        self.assertEqual(linked.reason_code, "explicit_correction_linked")
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM memory_ledger_lineage WHERE lineage_type='correction_of'").fetchone()[0], 1)
        ledger.shadow_conversation_row(self.conn, row_id=32, user_id=7, user_name="Crow", guild_id=1, role="user", content="remember this number: 111", channel_policy="sealed_test")
        ledger.shadow_conversation_row(self.conn, row_id=33, user_id=7, user_name="Crow", guild_id=1, role="user", content="remember this number: 111", channel_policy="sealed_test")
        unresolved = ledger.shadow_conversation_row(self.conn, row_id=34, user_id=7, user_name="Crow", guild_id=1, role="user", content="replace 111 with 222", channel_policy="sealed_test")
        self.assertEqual(unresolved.reason_code, "unresolved_correction_target")


    def test_explicit_correction_retires_old_value_from_effective_active_count(self):
        ledger.shadow_conversation_row(self.conn, row_id=40, user_id=7, user_name="Crow", guild_id=1, role="user", content="remember this number: 8", channel_policy="sealed_test")
        linked = ledger.shadow_conversation_row(self.conn, row_id=41, user_id=7, user_name="Crow", guild_id=1, role="user", content="replace 8 with 9", channel_policy="sealed_test")
        self.assertEqual(linked.reason_code, "explicit_correction_linked")
        edges = set(self.conn.execute("SELECT lineage_type FROM memory_ledger_lineage WHERE entry_id=?", (linked.entry_id,)).fetchall())
        self.assertEqual(edges, {("correction_of",), ("supersedes",)})
        report = ledger.build_memory_ledger_evaluation(self.conn, guild_id=1)
        self.assertEqual(report["entriesWithMultipleActiveValues"], 0)
        self.assertEqual(report["explicitCorrectionCounts"], 1)

    def test_all_lineage_targets_resolve_to_entries(self):
        ledger.shadow_conversation_row(self.conn, row_id=50, user_id=7, user_name="Crow", guild_id=1, role="user", content="remember this number: 8", channel_policy="sealed_test")
        ledger.shadow_conversation_row(self.conn, row_id=51, user_id=7, user_name="Crow", guild_id=1, role="user", content="Correction: the number is 9, not 8", channel_policy="sealed_test")
        missing = self.conn.execute("""
            SELECT COUNT(*) FROM memory_ledger_lineage l
            LEFT JOIN memory_ledger_entries e ON e.guild_id=l.guild_id AND e.entry_id=l.target_entry_id
            WHERE e.entry_id IS NULL
        """).fetchone()[0]
        self.assertEqual(missing, 0)

    def test_broadcast_usage_scopes_public_usability(self):
        cases = [("ambient,direct", 1, "public_safe"), ("show_status", 1, "public_safe"), ("relay", 1, "public_safe"), ("internal", 0, "internal"), ("ambient", 0, "internal")]
        for idx, (scope, expected_public, expected_visibility) in enumerate(cases, start=60):
            ledger.shadow_broadcast_memory_row(self.conn, row_id=idx, guild_id=1, cleaned_summary=f"Summary {idx}", entry_type="show_note", public_safe=(idx != 64), status="active", usage_scope=scope)
            row = self.conn.execute("SELECT public_usable, visibility FROM memory_ledger_entries WHERE source_row_id=?", (str(idx),)).fetchone()
            self.assertEqual(row, (expected_public, expected_visibility))

    def test_evaluation_detects_missing_success_receipt_entry_and_dangling_lineage(self):
        ledger.record_shadow_receipt(self.conn, guild_id=1, writer="test", source_table="conversations", source_row_id=999, outcome="inserted", reason_code="ok", entry_id="missing")
        self.conn.execute("INSERT INTO memory_ledger_lineage(entry_id, guild_id, lineage_type, target_entry_id, created_at) VALUES ('source', 1, 'supersedes', 'missing-target', 'now')")
        report = ledger.build_memory_ledger_evaluation(self.conn, guild_id=1)
        self.assertGreaterEqual(report["legacyToLedgerParityMismatches"], 2)
        self.assertEqual(report["danglingLineageTargets"], 1)

    def test_shadow_gate_default_disabled(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertFalse(ledger.shadow_enabled())


if __name__ == "__main__":
    unittest.main()
