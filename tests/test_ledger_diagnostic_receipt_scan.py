import sqlite3
import unittest

from bnl_memory_ledger import (
    build_memory_ledger_evaluation,
    ensure_memory_ledger_schema,
)


class LedgerDiagnosticReceiptScanTests(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        ensure_memory_ledger_schema(self.conn)
        self.addCleanup(self.conn.close)

    def entry(self, entry_id, guild_id=1):
        self.conn.execute(
            """
            INSERT INTO memory_ledger_entries (
                entry_id,schema_version,guild_id,subject_key,entry_type,
                predicate_key,source_class,source_table,source_row_id,
                source_role,visibility,confidence,lifecycle_status,
                created_at,updated_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                entry_id,"test",guild_id,"test", "test", "test", "test",
                "test",str(entry_id),"user","public","high","active",
                "2026-01-01","2026-01-01",
            ),
        )

    def receipt(self, entry_id, guild_id=1, outcome="inserted"):
        self.conn.execute(
            """
            INSERT INTO memory_ledger_shadow_receipts (
                guild_id,writer,source_table,source_row_id,attempted_at,
                outcome,reason_code,entry_id
            ) VALUES (?,?,?,?,?,?,?,?)
            """,
            (guild_id,"test","test",str(entry_id),"2026-01-01",
             outcome,"test",entry_id),
        )

    def original_parity_count(self, guild_id):
        scope = "AND e.guild_id=?" if guild_id is not None else ""
        params = (guild_id,) if guild_id is not None else ()
        entries_without_receipts = self.conn.execute(
            f"""
            SELECT COUNT(*) FROM memory_ledger_entries e
            WHERE NOT EXISTS (
                SELECT 1 FROM memory_ledger_shadow_receipts r
                WHERE r.guild_id=e.guild_id AND r.entry_id=e.entry_id
                  AND r.outcome IN ('inserted','deduplicated')
            ) {scope}
            """,
            params,
        ).fetchone()[0]
        receipt_scope = "AND guild_id=?" if guild_id is not None else ""
        missing_receipt_entries = self.conn.execute(
            f"""
            SELECT COUNT(*) FROM memory_ledger_shadow_receipts
            WHERE outcome IN ('inserted','deduplicated') {receipt_scope}
              AND (entry_id='' OR entry_id NOT IN (
                SELECT entry_id FROM memory_ledger_entries
                WHERE 1=1 {receipt_scope}
              ))
            """,
            params + params,
        ).fetchone()[0]
        write_errors = self.conn.execute(
            "SELECT COUNT(*) FROM memory_ledger_shadow_receipts "
            f"WHERE outcome='error' {receipt_scope}",
            params,
        ).fetchone()[0]
        return entries_without_receipts + missing_receipt_entries + write_errors

    def test_matches_existing_counts_for_duplicate_cross_guild_and_empty_receipts(self):
        for entry_id in ("duplicate", "deduplicated", "failed", "missing", "cross-guild"):
            self.entry(entry_id)
        self.entry("other-guild", guild_id=2)
        self.receipt("duplicate")
        self.receipt("duplicate", outcome="deduplicated")
        self.receipt("deduplicated", outcome="deduplicated")
        self.receipt("failed", outcome="error")
        self.receipt("failed", outcome="skipped")
        self.receipt("cross-guild", guild_id=2)
        self.receipt("other-guild", guild_id=2)
        self.receipt("")
        self.receipt(None)
        self.receipt("orphan")
        self.conn.commit()
        before = self.conn.total_changes
        for guild_id in (1, 2, 3, None):
            with self.subTest(guild_id=guild_id):
                expected = self.original_parity_count(guild_id)
                report = build_memory_ledger_evaluation(
                    self.conn, guild_id=guild_id, prepare_schema=False,
                )
                self.assertEqual(report["legacyToLedgerParityMismatches"], expected)
        self.assertEqual(self.conn.total_changes, before)
        self.assertFalse(self.conn.in_transaction)

    def test_empty_database_has_zero_parity_mismatches(self):
        for guild_id in (1, None):
            report = build_memory_ledger_evaluation(
                self.conn, guild_id=guild_id, prepare_schema=False,
            )
            self.assertEqual(report["legacyToLedgerParityMismatches"], 0)

    def test_diagnostic_avoids_quadratic_receipt_scan(self):
        for index in range(2000):
            entry_id = f"test-{index}"
            self.entry(entry_id)
            self.receipt(entry_id)
        self.conn.commit()
        # Count SQLite work, not elapsed time. The old correlated receipt
        # lookup alone exceeds 16 million VM steps for this fixture.
        for guild_id in (1, None):
            with self.subTest(guild_id=guild_id):
                steps = 0

                def progress():
                    nonlocal steps
                    steps += 1000
                    return int(steps > 2_000_000)

                self.conn.set_progress_handler(progress, 1000)
                try:
                    report = build_memory_ledger_evaluation(
                        self.conn, guild_id=guild_id, prepare_schema=False,
                    )
                finally:
                    self.conn.set_progress_handler(None, 0)
                self.assertEqual(report["legacyToLedgerParityMismatches"], 0)
                self.assertLessEqual(steps, 2_000_000)


if __name__ == "__main__":
    unittest.main()
