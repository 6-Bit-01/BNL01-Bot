"""Same-result source indexes reduce reads without replacing source owners."""

from __future__ import annotations

import ast
import json
import sqlite3
import unittest
from pathlib import Path

import bnl_tiktok_show_ledger as shows
import test_private_conversation_source_lookup as source_fixture_tests


def _index_owner_and_init_prefix():
    """Load the real setup owner without importing the network-facing bot."""
    tree = ast.parse((Path(__file__).resolve().parents[1] / "bnl01_bot.py").read_text(encoding="utf-8"))
    functions = {node.name: node for node in tree.body if isinstance(node, ast.FunctionDef)}
    setup = functions["init_db"]
    prefix = []
    for statement in setup.body:
        prefix.append(statement)
        if any(isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
               and node.func.id == "ensure_conversation_read_indexes"
               for node in ast.walk(statement)):
            break
    else:
        raise AssertionError("init_db must call the index owner")
    setup.body = prefix
    namespace = {"sqlite3": sqlite3}
    module = ast.fix_missing_locations(ast.Module(body=[
        functions["_try_alter"], functions["ensure_conversation_read_indexes"], setup,
    ], type_ignores=[]))
    exec(compile(module, "<conversation-schema-owner>", "exec"), namespace)
    return namespace


def _steps(conn, read):
    total = [0]

    def count():
        total[0] += 100
        return 0

    conn.set_progress_handler(count, 100)
    try:
        return read(), total[0]
    finally:
        conn.set_progress_handler(None, 0)


def _digest(value):
    # Compare complete ordered DTOs, including coverage, without dumping source
    # payloads into assertion diagnostics.
    import hashlib
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


class ConversationReadIndexTests(unittest.TestCase):
    def setUp(self):
        self.namespace = _index_owner_and_init_prefix()
        self.ensure = self.namespace["ensure_conversation_read_indexes"]

    def connection(self):
        conn = sqlite3.connect(":memory:")
        self.addCleanup(conn.close)
        return conn

    def source_fixture(self):
        fixture = source_fixture_tests.PrivateConversationSourceLookupTests()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        return fixture

    def test_setup_is_additive_idempotent_and_owned_by_the_callers_transaction(self):
        conn = self.connection()
        conn.execute("""CREATE TABLE conversations (
            id INTEGER PRIMARY KEY,guild_id INTEGER,user_id INTEGER,channel_id INTEGER,
            message_id INTEGER,channel_policy TEXT,role TEXT,timestamp TEXT,content TEXT)""")
        conn.execute("INSERT INTO conversations VALUES (1,1,2,99,1001,'sealed_test','user',"
                     "'2026-10-03T00:00:00+00:00','Fictional source.')")
        before = conn.execute("SELECT * FROM conversations").fetchall()
        self.assertTrue(conn.in_transaction)
        self.ensure(conn)
        self.ensure(conn)
        self.assertTrue(conn.in_transaction)
        self.assertEqual(conn.execute("SELECT * FROM conversations").fetchall(), before)
        self.assertEqual({row[1] for row in conn.execute("PRAGMA index_list(conversations)")}, {
            "idx_conversations_guild_message", "idx_conversations_public_time",
            "idx_conversations_member_scope",
        })
        conn.rollback()
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM conversations").fetchone()[0], 0)
        self.assertEqual(conn.execute("PRAGMA index_list(conversations)").fetchall(), [])

    def test_legacy_optional_columns_and_missing_table_are_safe(self):
        conn = self.connection()
        self.ensure(conn)
        conn.execute("CREATE TABLE conversations(id INTEGER PRIMARY KEY,guild_id INTEGER,"
                     "role TEXT,timestamp TEXT,channel_policy TEXT)")
        self.ensure(conn)
        self.assertEqual([row[1] for row in conn.execute("PRAGMA index_list(conversations)")],
                         ["idx_conversations_public_time"])

    def test_real_init_prefix_migrates_old_columns_before_installing_indexes(self):
        conn = self.connection()
        conn.execute("""CREATE TABLE conversations (
            id INTEGER PRIMARY KEY,user_id INTEGER NOT NULL,user_name TEXT NOT NULL,
            guild_id INTEGER NOT NULL,role TEXT NOT NULL,content TEXT NOT NULL,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)""")
        conn.execute("INSERT INTO conversations VALUES(1,2,'Test Member',1,'user',"
                     "'Fictional retained source.','2026-10-03 00:00:00')")
        before = conn.execute("SELECT id,user_id,user_name,guild_id,role,content,timestamp "
                              "FROM conversations").fetchall()
        # Only the DB factory and independent schemas are fixture inputs; the
        # real initialization statements and migration/index owners execute.
        from unittest.mock import patch
        self.namespace.update(DB_FILE=":memory:", ensure_journal_schema=lambda path: None,
                              ensure_occasion_schema=lambda path: None)
        with patch.object(sqlite3, "connect", return_value=conn):
            self.namespace["init_db"]()
        self.assertEqual(conn.execute("SELECT id,user_id,user_name,guild_id,role,content,timestamp "
                                      "FROM conversations").fetchall(), before)
        self.assertEqual(len(conn.execute("PRAGMA index_list(conversations)").fetchall()), 3)

    def test_actual_private_reader_avoids_unrelated_history_with_full_dto_parity(self):
        fixture = self.source_fixture()
        conn = fixture.conn
        conn.executemany("INSERT INTO conversations VALUES (?,?,?,?,?,?,?,?,?)", (
            (row_id, 3, 1, 10, "public_home", "normal_chat", "user",
             "Fictional unrelated record.", "2026-10-03T00:00:00+00:00")
            for row_id in range(1, 50001)
        ))
        for row_id in range(50001, 50031):
            fixture.source(row_id)
        query = fixture.root_query()
        before, old_work = _steps(conn, fixture.sources)
        old_plan = [row[3] for row in conn.execute("EXPLAIN QUERY PLAN " + query)]
        self.assertTrue(any("SCAN c" in line for line in old_plan), old_plan)
        self.ensure(conn)
        after, new_work = _steps(conn, fixture.sources)
        plan = [row[3] for row in conn.execute("EXPLAIN QUERY PLAN " + query)]
        self.assertEqual(before, after)
        self.assertEqual(len(after), 30)
        self.assertTrue(any("COVERING INDEX idx_conversations_member_scope" in line
                            for line in plan), plan)
        self.assertLess(new_work, old_work // 4)

    def test_private_correction_deletion_privacy_and_identity_controls_stay_fresh(self):
        # Run the existing independent source-control assertions against the
        # indexed reader. Their mutations exercise real validators/reducers,
        # rather than comparing two copies of the index SQL.
        for name in (
            "test_integer_lookup_preserves_exact_text_ids_and_matches_original_root_selection",
            "test_source_table_guild_member_channel_and_policy_stay_isolated",
            "test_edited_deleted_and_revoked_roots_are_freshly_rejected",
            "test_correction_withdrawal_is_guild_scoped_and_rechecked_each_read",
            "test_model_context_retains_only_exact_subject_and_response_audience",
            "test_order_and_revision_deduplication_preserve_original_selection",
        ):
            with self.subTest(control=name):
                fixture = self.source_fixture()
                self.ensure(fixture.conn)
                getattr(fixture, name)()

    def test_actual_show_reader_keeps_all_results_and_privacy_fence_with_less_work(self):
        conn = self.connection()
        conn.execute("""CREATE TABLE conversations (
            id INTEGER PRIMARY KEY,user_id INTEGER,user_name TEXT,guild_id INTEGER,
            channel_id INTEGER,channel_policy TEXT,route_mode TEXT,role TEXT,
            content TEXT,timestamp TEXT,message_id INTEGER)""")
        stamps = ("2026-10-03T00:00:00+00:00", "2026-10-02T17:00:00-07:00",
                  "2026-10-03 00:00:00", "invalid-date", None)
        policies = ("public_home", "public_context", "public_selective")
        conn.executemany("INSERT INTO conversations VALUES (?,?,?,?,?,?,?,?,?,?,?)", (
            (row_id, 20 + row_id % 4, "Test Member", 2 if row_id <= 5000 else 1,
             99, "sealed_test" if 5000 < row_id <= 25000 else policies[row_id % 3],
             "normal_chat", "user" if row_id % 2 else "model",
             "Fictional source record %s." % row_id, stamps[row_id % 5], 100000 + row_id)
            for row_id in range(1, 50001)
        ))
        statements = []
        conn.set_trace_callback(statements.append)
        before, old_work = _steps(conn, lambda: shows._load_show_related_sources(conn, guild_id=1))
        conn.set_trace_callback(None)
        public = next(sql for sql in statements if "ORDER BY datetime(timestamp) DESC,id DESC" in sql)
        identity = next(sql for sql in statements if sql.startswith("SELECT id,message_id FROM conversations"))
        old_plan = [row[3] for row in conn.execute("EXPLAIN QUERY PLAN " + public)]
        self.assertTrue(any("USE TEMP B-TREE FOR ORDER BY" in line for line in old_plan), old_plan)
        identities_before = set(conn.execute(identity))
        self.ensure(conn)
        after, new_work = _steps(conn, lambda: shows._load_show_related_sources(conn, guild_id=1))
        public_plan = [row[3] for row in conn.execute("EXPLAIN QUERY PLAN " + public)]
        identity_plan = [row[3] for row in conn.execute("EXPLAIN QUERY PLAN " + identity)]
        self.assertEqual(_digest(before), _digest(after))
        self.assertTrue(before == after)
        self.assertGreater(len(after[0]), 10000)
        self.assertTrue(any("idx_conversations_public_time" in line for line in public_plan), public_plan)
        self.assertFalse(any("TEMP B-TREE" in line for line in public_plan), public_plan)
        self.assertTrue(any("COVERING INDEX idx_conversations_guild_message" in line
                            for line in identity_plan), identity_plan)
        self.assertEqual(set(conn.execute(identity)), identities_before)
        self.assertEqual(len(identities_before), 45000)
        self.assertIn((6000, 106000), identities_before)  # Private original remains in the fence.
        self.assertLess(new_work, old_work // 2)


if __name__ == "__main__":
    unittest.main()
