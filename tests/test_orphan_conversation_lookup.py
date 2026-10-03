"""Startup orphan detection must use row IDs without relaxing source identity."""
import unittest
from unittest.mock import patch

import bnl_memory_governance as governance
from bnl_memory_ledger import subject_key_for_user
from tests.test_memory_governance_v1 import (
    ensure_test_contribution_schema,
    insert,
    insert_test_contribution,
    make_conn,
)


class OrphanConversationLookupTests(unittest.TestCase):
    def setUp(self):
        self.conn = make_conn()
        self.addCleanup(self.conn.close)
        self.conn.execute(
            "CREATE TABLE conversations (id INTEGER PRIMARY KEY, guild_id INTEGER, user_id INTEGER)"
        )
        self.conn.execute("CREATE INDEX idx_conversations_guild ON conversations(guild_id)")
        self.conn.executemany("INSERT INTO conversations VALUES (?,?,?)", [
            (0, 1, 10), (1, 1, 10), (-3, 1, 10), (2, 2, 10),
            (9223372036854775807, 1, 10), (-9223372036854775808, 1, 10),
        ])
        self.conn.commit()

    def raw(self, entry_id, source_row_id, guild=1):
        insert(self.conn, eid=entry_id, guild=guild, source="public_observation",
               value="Test Member source", pred="conversation", etype="observation",
               source_table="conversations")
        self.conn.execute(
            "UPDATE memory_ledger_entries SET source_row_id=?,source_role='user' WHERE entry_id=?",
            (source_row_id, entry_id),
        )
        self.conn.commit()

    def test_exact_ids_and_guilds_match_original_detection_including_noncanonical_ids(self):
        canonical = ["0", "1", "-3", "9223372036854775807", "-9223372036854775808"]
        noncanonical = ["01", "+1", "1.0", "1x", " 1", "1 ", "1e0", "-03", "-0",
                        "9223372036854775808", "-9223372036854775809", "", "unknown", "999"]
        for index, row_id in enumerate(canonical + noncanonical):
            self.raw(f"raw-{index}", row_id)
        self.raw("wrong-guild", "2")
        self.raw("other-guild-existing", "2", guild=2)
        original_orphans = set(self.conn.execute(
            """SELECT e.guild_id,e.source_row_id FROM memory_ledger_entries e
            LEFT JOIN conversations c ON c.guild_id=e.guild_id AND CAST(c.id AS TEXT)=e.source_row_id
            WHERE e.source_table='conversations' AND c.id IS NULL"""
        ))
        # Check this query's exact input to the existing purge owner. Purge has
        # its own normalization contract; its effects are tested separately.
        with patch.object(governance, "purge_conversation_ledger_sources", return_value={}) as purge:
            result = governance.reconcile_orphaned_conversation_ledger_sources(self.conn)
        detected_orphans = {(call.kwargs["guild_id"], row_id)
                            for call in purge.call_args_list
                            for row_id in call.kwargs["source_row_ids"]}
        self.assertEqual(detected_orphans, original_orphans)
        self.assertEqual(result["orphan_source_rows"], len(noncanonical) + 1)
        self.assertEqual(detected_orphans, {(1, row_id) for row_id in noncanonical} | {(1, "2")})

    def test_actual_reconciliation_query_uses_integer_primary_key_lookup(self):
        self.raw("retained", "1")
        self.raw("orphan", "missing")
        statements = []
        self.conn.set_trace_callback(statements.append)
        try:
            with self.conn:
                governance.reconcile_orphaned_conversation_ledger_sources(self.conn, guild_id=1)
        finally:
            self.conn.set_trace_callback(None)
        actual_query = next(sql for sql in statements
                            if "SELECT DISTINCT e.guild_id, e.source_row_id" in sql)
        plan = self.conn.execute("EXPLAIN QUERY PLAN " + actual_query).fetchall()
        self.assertTrue(any("SEARCH c USING INTEGER PRIMARY KEY (rowid=?)" in row[3]
                            for row in plan), plan)
        original_query = actual_query.replace("AND c.id=CAST(e.source_row_id AS INTEGER)", "")
        original_plan = self.conn.execute("EXPLAIN QUERY PLAN " + original_query).fetchall()
        self.assertFalse(any("SEARCH c USING INTEGER PRIMARY KEY" in row[3]
                             for row in original_plan), original_plan)

    def test_orphan_retraction_propagates_and_preserves_existing_source_and_other_guild(self):
        self.raw("retained", "1")
        self.raw("orphan", "999")
        self.raw("other-guild-orphan", "999", guild=2)
        self.conn.execute("""CREATE TABLE memory_moment_windows (
            moment_id TEXT PRIMARY KEY, guild_id INTEGER, lifecycle_status TEXT,
            summary TEXT, updated_at TEXT)""")
        ensure_test_contribution_schema(self.conn)
        self.conn.execute(
            "INSERT INTO memory_moment_windows VALUES ('orphan-moment',1,'finalized','Test Member gist','now')"
        )
        insert_test_contribution(self.conn, "orphan-moment", subject_key_for_user(10),
                                 "Test Member gist", "orphan")
        self.conn.commit()
        with self.conn:
            result = governance.reconcile_orphaned_conversation_ledger_sources(self.conn, guild_id=1)
        self.assertEqual(result["raw_ledger_entries"], 1)
        self.assertEqual(self.conn.execute(
            "SELECT contribution_gist,lifecycle_status,public_usable "
            "FROM memory_moment_contributions WHERE moment_id='orphan-moment'"
        ).fetchone(), ("", "retracted", 0))
        self.assertEqual(set(self.conn.execute("SELECT entry_id FROM memory_ledger_entries")),
                         {("retained",), ("other-guild-orphan",)})
        with self.conn:
            repeated = governance.reconcile_orphaned_conversation_ledger_sources(self.conn, guild_id=1)
        self.assertEqual(repeated["orphan_source_rows"], 0)
        self.assertEqual(repeated["raw_ledger_entries"], 0)


if __name__ == "__main__":
    unittest.main()
