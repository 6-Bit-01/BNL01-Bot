"""Private source reads must retain exact identity without scanning chat history."""
import os
import re
import sqlite3
import unittest
from unittest.mock import patch

import bnl_memory_ledger as ledger
import bnl_relationship_engine as rel


class PrivateConversationSourceLookupTests(unittest.TestCase):
    def setUp(self):
        env = patch.dict(os.environ, {"BNL_MEMORY_LEDGER_SHADOW_ENABLED": "1"})
        env.start()
        self.addCleanup(env.stop)
        self.conn = sqlite3.connect(":memory:")
        self.addCleanup(self.conn.close)
        ledger.ensure_memory_ledger_schema(self.conn)
        self.conn.execute("""CREATE TABLE conversations (
            id INTEGER PRIMARY KEY,user_id INTEGER,guild_id INTEGER,channel_id INTEGER,
            channel_policy TEXT,route_mode TEXT,role TEXT,content TEXT,timestamp TEXT)""")
        self.conn.execute("""CREATE TABLE conversation_response_participants (
            conversation_row_id INTEGER,guild_id INTEGER,user_id INTEGER)""")

    def source(self, row_id, *, user_id=2, guild_id=1, channel_id=99,
               policy="sealed_test", role="user", route="normal_chat"):
        text = f"Test Member contribution {row_id % 10000}."
        stamp = "2026-10-03T00:00:00+00:00"
        self.conn.execute("INSERT INTO conversations VALUES (?,?,?,?,?,?,?,?,?)",
                          (row_id, user_id, guild_id, channel_id, policy, route, role, text, stamp))
        return ledger.shadow_conversation_row(self.conn, row_id=row_id,
            user_id=user_id, user_name="Test Member", guild_id=guild_id, role=role,
            content=text, channel_id=channel_id, channel_policy=policy,
            route_mode=route, observed_at=stamp).entry_id

    def clone_root(self, root, entry_id, **changes):
        cursor = self.conn.execute("SELECT * FROM memory_ledger_entries WHERE entry_id=?", (root,))
        values = dict(zip((column[0] for column in cursor.description), cursor.fetchone()))
        values.update(entry_id=entry_id, source_revision=entry_id)
        values.update(changes)
        self.conn.execute("INSERT INTO memory_ledger_entries (" + ",".join(values) + ") VALUES ("
                          + ",".join("?" for _ in values) + ")", tuple(values.values()))

    def sources(self):
        return rel.private_conversation_sources(self.conn, guild_id=1, user_id=2, channel_id=99)

    def root_query(self):
        statements = []
        self.conn.set_trace_callback(statements.append)
        try:
            self.sources()
        finally:
            self.conn.set_trace_callback(None)
        return next(sql for sql in statements
                    if "SELECT e.entry_id FROM memory_ledger_entries e" in sql)

    @staticmethod
    def original_query(query):
        return re.sub(r"c\.id=CAST\(e\.source_row_id AS INTEGER\)\s+AND\s+", "", query)

    def test_actual_root_query_uses_primary_key_and_avoids_unrelated_conversations(self):
        self.conn.executemany("INSERT INTO conversations VALUES (?,?,?,?,?,?,?,?,?)",
            ((row_id, 3, 1, 10, "public_home", "normal_chat", "user", "Other member history.",
              "2026-10-03T00:00:00+00:00") for row_id in range(1, 10001)))
        roots = [self.source(row_id) for row_id in range(10001, 10011)]
        query = self.root_query()
        plan = self.conn.execute("EXPLAIN QUERY PLAN " + query).fetchall()
        self.assertTrue(any("c USING INTEGER PRIMARY KEY (rowid=?)" in row[3]
                            for row in plan), plan)
        original = self.original_query(query)
        original_plan = self.conn.execute("EXPLAIN QUERY PLAN " + original).fetchall()
        self.assertFalse(any("c USING INTEGER PRIMARY KEY" in row[3]
                             for row in original_plan), original_plan)
        steps = 0

        def budget():
            nonlocal steps
            steps += 1000
            return int(steps > 15000)

        self.conn.set_progress_handler(budget, 1000)
        try:
            self.assertEqual([source["entry_id"] for source in self.sources()], roots)
        finally:
            self.conn.set_progress_handler(None, 0)

    def test_integer_lookup_preserves_exact_text_ids_and_matches_original_root_selection(self):
        root = self.source(1)
        largest_root = self.source(9223372036854775807)
        invalid = ("01", "+1", "1.0", "1x", " 1", "1 ", "1e0", "", "unknown",
                   "9223372036854775808", "-9223372036854775809")
        for index, row_id in enumerate(invalid):
            self.clone_root(root, f"invalid-{index}", source_row_id=row_id)
        query = self.root_query()
        original = self.original_query(query)
        self.assertEqual(self.conn.execute(query).fetchall(), self.conn.execute(original).fetchall())
        self.assertEqual([source["entry_id"] for source in self.sources()], [root, largest_root])

    def test_source_table_guild_member_channel_and_policy_stay_isolated(self):
        root = self.source(1)
        self.source(2, user_id=3)
        self.source(3, guild_id=2)
        self.source(4, channel_id=100)
        self.source(5, policy="public_home")
        self.clone_root(root, "wrong-ledger-guild", guild_id=2)
        self.clone_root(root, "wrong-source-table", source_table="other_sources")
        self.assertEqual([source["entry_id"] for source in self.sources()], [root])

    def test_edited_deleted_and_revoked_roots_are_freshly_rejected(self):
        mutations = ("edit", "delete", "privacy", "public_usable", "derived", "projection",
                     "lifecycle", "subject", "role", "channel", "policy", "route")
        for index, mutation in enumerate(mutations, 1):
            with self.subTest(mutation=mutation):
                root = self.source(index)
                self.assertIn(root, [source["entry_id"] for source in self.sources()])
                if mutation == "edit":
                    self.conn.execute("UPDATE conversations SET content='Edited original.' WHERE id=?", (index,))
                elif mutation == "delete":
                    self.conn.execute("DELETE FROM conversations WHERE id=?", (index,))
                else:
                    column, value = {
                        "privacy": ("visibility", "private"), "public_usable": ("public_usable", 1),
                        "derived": ("derived", 1), "projection": ("projection", 1),
                        "lifecycle": ("lifecycle_status", "retracted"),
                        "subject": ("subject_key", ledger.subject_key_for_user(3)),
                        "role": ("source_role", "model"), "channel": ("channel_id", 100),
                        "policy": ("channel_policy", "public_home"), "route": ("route_mode", "ambient"),
                    }[mutation]
                    self.conn.execute("UPDATE memory_ledger_entries SET " + column + "=? WHERE entry_id=?",
                                      (value, root))
                self.assertNotIn(root, [source["entry_id"] for source in self.sources()])

    def test_correction_withdrawal_is_guild_scoped_and_rechecked_each_read(self):
        root = self.source(1)
        self.conn.execute("INSERT INTO memory_ledger_lineage VALUES (?,?,?,?,?)",
                          ("foreign-correction", 2, "correction_of", root, "now"))
        self.assertEqual([source["entry_id"] for source in self.sources()], [root])
        self.conn.execute("INSERT INTO memory_ledger_lineage VALUES (?,?,?,?,?)",
                          ("correction", 1, "correction_of", root, "now"))
        self.assertEqual(self.sources(), ())

    def test_model_context_retains_only_exact_subject_and_response_audience(self):
        user_root = self.source(1)
        model_root = self.source(2, role="model")
        self.conn.execute("INSERT INTO conversation_response_participants VALUES (?,?,?)", (2, 1, 2))
        self.assertEqual([source["entry_id"] for source in self.sources()], [user_root, model_root])
        self.conn.execute("INSERT INTO conversation_response_participants VALUES (?,?,?)", (2, 1, 3))
        self.assertEqual([source["entry_id"] for source in self.sources()], [user_root])
        self.conn.execute("DELETE FROM conversation_response_participants WHERE user_id=3")
        self.conn.execute("UPDATE memory_ledger_entries SET subject_key=? WHERE entry_id=?",
                          (ledger.subject_key_for_user(2), model_root))
        self.assertEqual([source["entry_id"] for source in self.sources()], [user_root])

    def test_order_and_revision_deduplication_preserve_original_selection(self):
        later = self.source(3)
        earlier = self.source(1)
        middle = self.source(2)
        self.clone_root(middle, "aaa-revision")
        self.clone_root(middle, "zzz-revision")
        self.assertEqual([(source["row_id"], source["entry_id"]) for source in self.sources()],
                         [(1, earlier), (2, "aaa-revision"), (3, later)])

    def test_no_channel_or_ledger_yields_no_private_sources(self):
        self.source(1)
        self.assertEqual(rel.private_conversation_sources(self.conn, guild_id=1,
                         user_id=2, channel_id=0), ())
        with sqlite3.connect(":memory:") as empty:
            self.assertEqual(rel.private_conversation_sources(empty, guild_id=1,
                             user_id=2, channel_id=99), ())


if __name__ == "__main__":
    unittest.main()
