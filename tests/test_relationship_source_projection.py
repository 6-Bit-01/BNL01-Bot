"""Source validation keeps its full DTO while unused columns stay unread."""

from __future__ import annotations

import ast
import inspect
import sqlite3
import textwrap
import unittest
from unittest import mock

import bnl_relationship_engine as relationship
from tests import test_private_conversation_source_lookup as source_lookup


def original_source_owner():
    # Restore the two original SELECT * statements independently of the new
    # projection. All downstream source fences and DTO construction are real.
    tree = ast.parse(textwrap.dedent(inspect.getsource(relationship._meaning_source)))

    class OriginalQueries(ast.NodeTransformer):
        replacements = 0

        def visit_Call(self, node):
            node = self.generic_visit(node)
            if (isinstance(node.func, ast.Attribute) and node.func.attr == "execute"
                    and node.args and isinstance(node.args[0], ast.Constant)
                    and isinstance(node.args[0].value, str)):
                sql = " ".join(node.args[0].value.split())
                for tail in (
                    "FROM memory_ledger_entries WHERE entry_id=? AND guild_id=?",
                    "FROM conversations WHERE id=? AND guild_id=? AND user_id=?",
                ):
                    if sql.startswith("SELECT ") and sql.endswith(tail):
                        node.args[0] = ast.copy_location(ast.Constant("SELECT * " + tail), node.args[0])
                        self.replacements += 1
            return node

    rewrite = OriginalQueries()
    tree = ast.fix_missing_locations(rewrite.visit(tree))
    if rewrite.replacements != 2:
        raise AssertionError("source owner query sites changed")
    namespace = dict(vars(relationship))
    exec(compile(tree, "<original-relationship-source-owner>", "exec"), namespace)
    return namespace["_meaning_source"]


class RelationshipSourceProjectionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.original = staticmethod(original_source_owner())

    def setUp(self):
        fixture = source_lookup.PrivateConversationSourceLookupTests()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        self.fixture = fixture
        self.conn = fixture.conn

    def sources(self, owner=relationship._meaning_source):
        with mock.patch.object(relationship, "_meaning_source", owner):
            return self.fixture.sources()

    def assert_parity(self):
        old = self.sources(self.original)
        new = self.sources()
        self.assertEqual(new, old)
        return new

    def test_large_unused_payloads_and_identity_labels_are_not_retrieved(self):
        roots = [self.fixture.source(row_id) for row_id in range(1, 33)]
        self.conn.execute("ALTER TABLE memory_ledger_entries ADD COLUMN unused_fixture_payload TEXT")
        self.conn.execute("ALTER TABLE conversations ADD COLUMN unused_fixture_payload TEXT")
        self.conn.execute("ALTER TABLE conversations ADD COLUMN user_name TEXT")
        payload = "Fictional unused archive payload. " * 2048
        self.conn.execute("UPDATE memory_ledger_entries SET unused_fixture_payload=?,subject_display_name='Test Member'", (payload,))
        self.conn.execute("UPDATE conversations SET unused_fixture_payload=?,user_name='Test Member'", (payload,))
        self.conn.commit()
        self.conn.execute("PRAGMA query_only=1")
        self.conn.execute("BEGIN")
        expected = self.sources(self.original)
        denied = []

        def forbid_unused(action, table, column, _database, _trigger):
            if action == sqlite3.SQLITE_READ and (
                column == "unused_fixture_payload"
                or (table == "memory_ledger_entries" and column == "subject_display_name")
                or (table == "conversations" and column == "user_name")
            ):
                denied.append((table, column))
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        self.conn.set_authorizer(forbid_unused)
        try:
            # This fails the old owner at SQLite's read boundary, proving the
            # values are omitted before Python hydration rather than discarded.
            with self.assertRaises(sqlite3.DatabaseError):
                self.sources(self.original)
            self.assertTrue(denied)
            denied.clear()
            current = self.sources()
            self.assertEqual(current, expected)
            self.assertEqual([item["entry_id"] for item in current], roots)
            self.assertEqual(denied, [])
        finally:
            # sqlite3 on supported Python 3.9 cannot clear with None.
            self.conn.set_authorizer(lambda *_: sqlite3.SQLITE_OK)
            self.conn.rollback()

    def test_complete_dto_and_order_match_for_revisions_roles_and_scopes(self):
        first = self.fixture.source(1)
        model = self.fixture.source(2, role="model")
        self.conn.execute("INSERT INTO conversation_response_participants VALUES(?,?,?)", (2, 1, 2))
        self.fixture.clone_root(first, "aaa-valid-revision")
        self.fixture.clone_root(first, "aaa-invalid-role", source_role="model")
        self.fixture.clone_root(first, "coercible-source-id", source_row_id="01")
        self.fixture.clone_root(first, "foreign-ledger-guild", guild_id=2)
        self.fixture.source(3, user_id=3)
        self.fixture.source(4, channel_id=100)
        self.fixture.source(5, policy="public_home")
        rows = self.assert_parity()
        self.assertEqual([(row["row_id"], row["entry_id"]) for row in rows],
                         [(1, "aaa-valid-revision"), (2, model)])
        self.assertEqual(set(rows[0]), {"entry_id", "row_id", "role", "text", "timestamp", "channel_id", "ledger"})
        self.assertEqual(set(rows[0]["ledger"]), {
            "subject_key", "source_revision", "source_role", "channel_policy",
            "route_mode", "visibility", "public_usable", "derived", "projection",
            "lifecycle_status", "observed_at",
        })

    def test_same_connection_rechecks_edits_deletion_privacy_and_controls(self):
        for row_id, mutation in enumerate(("edit", "delete", "privacy", "channel", "lifecycle",
                                           "correction_of", "supersedes", "retracts"), 1):
            with self.subTest(mutation=mutation):
                root = self.fixture.source(row_id)
                self.assertIn(root, [row["entry_id"] for row in self.assert_parity()])
                if mutation == "edit":
                    self.conn.execute("UPDATE conversations SET content='Edited fictional source.' WHERE id=?", (row_id,))
                elif mutation == "delete":
                    self.conn.execute("DELETE FROM conversations WHERE id=?", (row_id,))
                elif mutation in {"privacy", "channel", "lifecycle"}:
                    column, value = {"privacy": ("visibility", "private"),
                                     "channel": ("channel_id", 100),
                                     "lifecycle": ("lifecycle_status", "retracted")}[mutation]
                    self.conn.execute("UPDATE memory_ledger_entries SET " + column + "=? WHERE entry_id=?", (value, root))
                else:
                    self.conn.execute("INSERT INTO memory_ledger_lineage VALUES(?,?,?,?,?)",
                                      ("control-" + mutation, 1, mutation, root, "now"))
                self.assertNotIn(root, [row["entry_id"] for row in self.assert_parity()])

    def test_revision_and_grouped_model_audience_are_rechecked_without_reuse(self):
        root = self.fixture.source(1, role="model")
        self.conn.execute("INSERT INTO conversation_response_participants VALUES(?,?,?)", (1, 1, 2))
        rows = self.assert_parity()
        self.assertEqual([row["entry_id"] for row in rows], [root])
        self.conn.execute("UPDATE memory_ledger_entries SET source_revision='fresh-revision',observed_at='fresh-observation' WHERE entry_id=?", (root,))
        rows = self.assert_parity()
        self.assertEqual(rows[0]["ledger"]["source_revision"], "fresh-revision")
        self.assertEqual(rows[0]["ledger"]["observed_at"], "fresh-observation")
        self.conn.execute("INSERT INTO conversation_response_participants VALUES(?,?,?)", (1, 1, 3))
        self.assertEqual(self.assert_parity(), ())


if __name__ == "__main__":
    unittest.main()
