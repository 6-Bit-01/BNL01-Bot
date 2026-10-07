"""Measure actual SQLite reads, including on the deployed legacy runtime."""
from contextlib import closing
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest

@unittest.skipUnless(sys.platform == "linux", "Linux process I/O counters required")
class GovernedSubjectReadIOTests(unittest.TestCase):
    def test_skipped_payload_pages_are_not_read_through_metadata(self):
        from bnl_memory_governance import _governed_subject_rows
        from bnl_memory_ledger import ensure_governed_subject_read_index, ensure_memory_ledger_schema

        print("governed subject read I/O: SQLite " + sqlite3.sqlite_version, flush=True)
        # rchar counts bytes returned by read/pread even when the host page
        # cache is warm. This checks physical query access, not elapsed time,
        # interpreter object size, planner labels or an exact SQLite opcode.
        def read_bytes():
            fields = dict(line.split(":", 1) for line in Path("/proc/self/io").read_text().splitlines())
            return int(fields["rchar"])

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "neutral.sqlite"
            with closing(sqlite3.connect(path)) as conn:
                ensure_memory_ledger_schema(conn)
                for ordinal in range(512):
                    # IDs deliberately disagree with insertion/rowid order.
                    entry = "neutral-%04d" % (511 - ordinal)
                    conn.execute("""INSERT INTO memory_ledger_entries (
                        entry_id,schema_version,guild_id,subject_key,entry_type,
                        predicate_key,normalized_value,source_class,source_table,
                        source_row_id,source_role,route_mode,channel_policy,
                        visibility,confidence,public_usable,derived,projection,
                        salience,observed_at,lifecycle_status,created_at,updated_at
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
                        entry, "memory_ledger_v1", 1, "discord_user:10", "observation",
                        "conversation", "N" * 1000, "public_observation", "neutral_source",
                        str(ordinal), "user", "normal_chat", "public_context", "public_safe",
                        "high", 1, 0, 0, 0.7, "2026-10-01", "active", "now", "now",
                    ))
                conn.commit()

            def measure():
                with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as conn:
                    conn.execute("PRAGMA cache_size=-64")
                    conn.execute("PRAGMA mmap_size=0")
                    conn.execute("PRAGMA query_only=ON")
                    before = read_bytes()
                    rows = _governed_subject_rows(conn, 1, "discord_user:10")
                    return rows, read_bytes() - before

            baseline, old_reads = measure()
            with closing(sqlite3.connect(path)) as conn:
                conn.execute("BEGIN IMMEDIATE")
                self.assertTrue(ensure_governed_subject_read_index(conn))
                conn.commit()
            indexed, new_reads = measure()
            self.assertEqual(indexed, baseline)
            self.assertEqual(len(indexed), 512)
            self.assertTrue(all(row["normalized_value"] is None for row in indexed))
            self.assertGreater(old_reads, 128 * 1024)
            self.assertLess(new_reads, old_reads * 0.6,
                            "discarded payload pages still dominate the governed read")


if __name__ == "__main__":
    unittest.main()
