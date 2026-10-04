"""Subject metadata retains selection while ignored archive text stays unread."""

import ast
from dataclasses import asdict
import inspect
import sqlite3
import textwrap
import unittest

import bnl_memory_governance as governance
from tests.test_memory_governance_v1 import insert, make_conn, req


# Exact subject-loading statements from the pre-change owner. Reconstruct its
# selector AST with this independent loader so all downstream results compare,
# including exclusions, control batching, selected lineage and diagnostics.
_ORIGINAL_SUBJECT_LOAD = """
cols = [c[1] for c in conn.execute("PRAGMA table_info(memory_ledger_entries)").fetchall()]
subject_rows = [dict(zip(cols, row)) for row in conn.execute(
    "SELECT * FROM memory_ledger_entries WHERE guild_id=? AND subject_key=?",
    (req.guild_id, subject),
).fetchall()]
"""


def original_selector():
    tree = ast.parse(textwrap.dedent(inspect.getsource(governance.build_governed_context)))

    class OriginalLoader(ast.NodeTransformer):
        replacements = 0

        def visit_Assign(self, node):
            if (any(isinstance(target, ast.Name) and target.id == "subject_rows"
                    for target in node.targets)
                    and isinstance(node.value, ast.Call)
                    and isinstance(node.value.func, ast.Name)
                    and node.value.func.id == "_governed_subject_rows"):
                self.replacements += 1
                return ast.parse(_ORIGINAL_SUBJECT_LOAD).body
            return self.generic_visit(node)

    loader = OriginalLoader()
    tree = ast.fix_missing_locations(loader.visit(tree))
    if loader.replacements != 1:
        raise AssertionError("original owner subject-loading site changed")
    namespace = dict(vars(governance))
    exec(compile(tree, "<original-governed-subject-owner>", "exec"), namespace)
    return namespace["build_governed_context"]


class GovernedSubjectProjectionTests(unittest.TestCase):
    def setUp(self):
        self.conn = make_conn()
        self.addCleanup(self.conn.close)
        self.original = original_selector()

    def add(self, entry_id, **changes):
        changes.setdefault("value", "favorite movie is Modular Synths")
        changes.setdefault("pred", "favorite_movie")
        changes.setdefault("vis", "public_safe")
        changes.setdefault("public", 1)
        insert(self.conn, eid=entry_id, **changes)

    def edge(self, origin, target, kind="derived_from", guild=1):
        self.conn.execute("INSERT INTO memory_ledger_lineage VALUES (?,?,?,?,?)",
                          (origin, guild, kind, target, "now"))
        self.conn.commit()

    def result(self, owner, request=None):
        self.conn.execute("BEGIN")
        try:
            return owner(self.conn, request or req(visibility_allowance="public_safe"),
                         initialize_schema=False)
        finally:
            self.conn.rollback()

    def assert_parity(self, request=None):
        old = self.result(self.original, request)
        new = self.result(governance.build_governed_context, request)
        self.assertEqual(asdict(new), asdict(old))
        return new

    def test_all_rows_controls_exclusion_order_and_selected_lineage_match_original(self):
        self.add("current")
        self.add("raw-conversation", pred="conversation", etype="observation")
        self.add("raw-output", etype="model_output")
        self.add("raw-assistant", pred="assistant_response")
        self.add("raw-message", pred="raw_message")
        self.add("upper-predicate", pred="CONVERSATION", etype="observation")
        self.add("projection", source="derived_summary", derived=1, projection=1)
        self.add("projection-class", source="legacy_source_blind")
        self.add("derived-flag", derived=1)
        self.add("projection-flag", projection=1)
        self.add("private", vis="private", public=0)
        self.add("retracted", life="retracted")
        self.add("wrong-route")
        self.add("forgotten-original", source_table="same")
        self.add("forgotten-control", source_table="same", life="forgotten",
                 source_revision="tombstone")
        self.add("foreign-subject", user=11)
        self.add("foreign-guild", guild=2)
        self.edge("projection", "current")
        self.edge("derived-flag", "current")
        self.edge("projection-flag", "current")
        self.edge("current", "history", "reply_to")
        self.conn.execute("UPDATE memory_ledger_entries SET route_mode='operator_command' "
                          "WHERE entry_id='wrong-route'")
        self.conn.execute("UPDATE memory_ledger_entries SET source_row_id='shared-source' "
                          "WHERE entry_id IN ('forgotten-original','forgotten-control')")
        self.conn.commit()
        result = self.assert_parity()
        self.assertEqual([candidate.entry_id for candidate in result.selected], ["current"])
        self.assertEqual(result.selected[0].lineage, (("reply_to", "history"),))
        self.assertEqual(result.diagnostics.excluded_by_reason["forgotten_source_tombstone"], 1)
        self.assertFalse(result.diagnostics.processing_errors)

    def test_large_ignored_payload_is_not_returned_but_every_row_and_result_remain(self):
        self.add("current")
        self.add("template", pred="conversation", etype="observation")
        cursor = self.conn.execute("SELECT * FROM memory_ledger_entries WHERE entry_id='template'")
        columns = [column[0] for column in cursor.description]
        original = dict(zip(columns, cursor.fetchone()))
        # Fictional source text stays in an isolated in-memory database. Eight
        # thousand rows model the measured subject size without provider calls.
        payload = "Test Member fictional archive text. " * 32
        self.conn.executemany(
            "INSERT INTO memory_ledger_entries (" + ",".join(columns) + ") VALUES ("
            + ",".join("?" for _ in columns) + ")",
            (tuple(dict(original, entry_id=f"archive-{index}", source_row_id=str(index),
                        normalized_value=payload)[column] for column in columns)
             for index in range(8000)),
        )
        self.conn.commit()
        subject = governance.subject_key_for_user(10)
        rows = governance._governed_subject_rows(self.conn, 1, subject)
        old_rows = [dict(zip(columns, row)) for row in self.conn.execute(
            "SELECT * FROM memory_ledger_entries WHERE guild_id=? AND subject_key=?",
            (1, subject))]
        self.assertEqual([row["entry_id"] for row in rows],
                         [row["entry_id"] for row in old_rows])
        self.assertEqual(len(rows), 8002)
        old_payload_bytes = sum(len(str(row.get("normalized_value") or "").encode())
                                for row in old_rows)
        new_payload_bytes = sum(len(str(row.get("normalized_value") or "").encode())
                                for row in rows)
        self.assertGreater(old_payload_bytes - new_payload_bytes, 8_000_000)
        self.assertLess(new_payload_bytes, 100)
        self.assertTrue(all("subject_display_name" not in row for row in rows))
        self.assertEqual(self.assert_parity().diagnostics.excluded_by_reason[
            "non_durable_conversation"], 8001)

    def test_current_text_correction_privacy_and_deletion_are_fresh_on_same_connection(self):
        self.add("current")
        self.assertEqual([candidate.entry_id for candidate in self.assert_parity().selected],
                         ["current"])
        self.conn.execute("UPDATE memory_ledger_entries SET normalized_value="
                          "'favorite movie is Modular Synths Again' WHERE entry_id='current'")
        self.conn.commit()
        self.assertIn("Again", self.assert_parity().rendered_context)
        self.edge("owner-control", "current", "correction_of")
        self.assertEqual(self.assert_parity().selected, ())
        self.conn.execute("DELETE FROM memory_ledger_lineage WHERE entry_id='owner-control'")
        self.conn.execute("UPDATE memory_ledger_entries SET visibility='private',public_usable=0 "
                          "WHERE entry_id='current'")
        self.conn.commit()
        self.assertEqual(self.assert_parity().selected, ())
        self.conn.execute("UPDATE memory_ledger_entries SET visibility='public_safe',public_usable=1 "
                          "WHERE entry_id='current'")
        self.conn.commit()
        self.assertEqual(len(self.assert_parity().selected), 1)
        self.conn.execute("DELETE FROM memory_ledger_entries WHERE entry_id='current'")
        self.conn.commit()
        self.assertEqual(self.assert_parity().selected, ())

    def test_projection_ancestry_missing_cycle_and_root_lifecycle_remain_fresh(self):
        self.add("root")
        self.add("projection", source="derived_summary", derived=1, projection=1)
        result = self.assert_parity()
        self.assertIn(("projection", "projection_lineage"),
                      [(item.entry_id, item.reason) for item in result.exclusions])
        self.edge("projection", "projection")
        self.assert_parity()
        self.edge("projection", "root")
        result = self.assert_parity()
        self.assertIn(("projection", "projection_shadow_only"),
                      [(item.entry_id, item.reason) for item in result.exclusions])
        self.conn.execute("UPDATE memory_ledger_entries SET lifecycle_status='retracted' "
                          "WHERE entry_id='root'")
        self.conn.commit()
        result = self.assert_parity()
        self.assertIn(("projection", "projection_lineage"),
                      [(item.entry_id, item.reason) for item in result.exclusions])
        self.conn.execute("DELETE FROM memory_ledger_entries WHERE entry_id='root'")
        self.conn.commit()
        self.assert_parity()

    def test_noncanonical_values_keep_the_original_normalization_and_error_path(self):
        self.add("upper", pred="CONVERSATION", etype="observation")
        self.add("other-flag", derived=2)
        self.add("malformed-flag")
        self.conn.execute("UPDATE memory_ledger_entries SET derived='invalid' "
                          "WHERE entry_id='malformed-flag'")
        self.conn.commit()
        rows = governance._governed_subject_rows(
            self.conn, 1, governance.subject_key_for_user(10))
        self.assertTrue(all(row["normalized_value"] for row in rows))
        result = self.assert_parity()
        self.assertEqual(result.diagnostics.processing_errors, ["ValueError"])

    def test_missing_optional_columns_retain_existing_defaults(self):
        with sqlite3.connect(":memory:") as conn:
            conn.executescript("""
                CREATE TABLE memory_ledger_entries (
                    entry_id TEXT,guild_id INTEGER,subject_key TEXT,entry_type TEXT,
                    predicate_key TEXT,normalized_value TEXT,source_class TEXT,
                    source_table TEXT,source_row_id TEXT,lifecycle_status TEXT,
                    visibility TEXT,public_usable INTEGER);
                CREATE TABLE memory_ledger_lineage (
                    entry_id TEXT,guild_id INTEGER,lineage_type TEXT,target_entry_id TEXT,
                    PRIMARY KEY(entry_id,lineage_type,target_entry_id));
                INSERT INTO memory_ledger_entries VALUES (
                    'current',1,'discord_user:10','claim','favorite_movie',
                    'favorite movie is Modular Synths','first_party_record',
                    'test','current','active','public_safe',1);
            """)
            conn.execute("BEGIN")
            old = self.original(conn, req(visibility_allowance="public_safe"),
                                initialize_schema=False)
            new = governance.build_governed_context(
                conn, req(visibility_allowance="public_safe"), initialize_schema=False)
            self.assertEqual(asdict(new), asdict(old))
            self.assertEqual(len(new.selected), 1)

    def test_missing_normalized_value_retains_defaults_and_fresh_exclusions(self):
        with sqlite3.connect(":memory:") as conn:
            conn.executescript("""
                CREATE TABLE memory_ledger_entries (
                    entry_id TEXT,guild_id INTEGER,subject_key TEXT,entry_type TEXT,
                    predicate_key TEXT,source_class TEXT,source_table TEXT,
                    source_row_id TEXT,lifecycle_status TEXT,visibility TEXT,
                    public_usable INTEGER);
                CREATE TABLE memory_ledger_lineage (
                    entry_id TEXT,guild_id INTEGER,lineage_type TEXT,target_entry_id TEXT,
                    PRIMARY KEY(entry_id,lineage_type,target_entry_id));
                INSERT INTO memory_ledger_entries VALUES (
                    'current',1,'discord_user:10','claim','favorite_movie',
                    'first_party_record','test','current','active','public_safe',1);
            """)
            request = req(visibility_allowance="public_safe")

            def compare(reason):
                conn.execute("BEGIN")
                try:
                    old = self.original(conn, request, initialize_schema=False)
                    new = governance.build_governed_context(
                        conn, request, initialize_schema=False)
                    self.assertEqual(asdict(new), asdict(old))
                    self.assertEqual(new.selected, ())
                    self.assertEqual(new.diagnostics.processing_errors, [])
                    self.assertEqual([(item.entry_id, item.reason)
                                      for item in new.exclusions], [("current", reason)])
                finally:
                    conn.rollback()

            compare("current_message_or_empty")
            conn.execute("UPDATE memory_ledger_entries SET predicate_key='conversation'")
            conn.commit()
            compare("non_durable_conversation")
            conn.execute("UPDATE memory_ledger_entries SET predicate_key='favorite_movie',"
                         "visibility='private',public_usable=0")
            conn.commit()
            compare("visibility")

    def test_case_payload_elision_stays_exact_with_a_legacy_nocase_column(self):
        with sqlite3.connect(":memory:") as conn:
            conn.executescript("""
                CREATE TABLE memory_ledger_entries (
                    entry_id TEXT,guild_id INTEGER,subject_key TEXT,entry_type TEXT,
                    predicate_key TEXT,normalized_value TEXT,
                    source_class TEXT COLLATE NOCASE,source_table TEXT,
                    source_row_id TEXT,lifecycle_status TEXT,visibility TEXT,
                    public_usable INTEGER);
                CREATE TABLE memory_ledger_lineage (
                    entry_id TEXT,guild_id INTEGER,lineage_type TEXT,target_entry_id TEXT,
                    PRIMARY KEY(entry_id,lineage_type,target_entry_id));
                INSERT INTO memory_ledger_entries VALUES (
                    'current',1,'discord_user:10','claim','favorite_movie',
                    'favorite movie is Modular Synths','DERIVED_SUMMARY',
                    'test','current','active','public_safe',1);
            """)
            request = req(visibility_allowance="public_safe",
                          allowed_source_classes=("DERIVED_SUMMARY",))
            conn.execute("BEGIN")
            old = self.original(conn, request, initialize_schema=False)
            new = governance.build_governed_context(conn, request, initialize_schema=False)
            self.assertEqual(asdict(new), asdict(old))
            self.assertEqual(len(new.selected), 1)


if __name__ == "__main__":
    unittest.main()
