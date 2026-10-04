"""Moment diagnostics preserve exact membership without guild-wide join work.

Use the real schema owners and the saved governance helper. The original query
is an independent result oracle; SQLite VM work, rather than elapsed time,
detects scanning unrelated members on machines with different loads.
"""

import ast
from contextlib import closing
from pathlib import Path
import sqlite3
from types import SimpleNamespace
import unittest

from bnl_memory_ledger import subject_key_for_user
from bnl_moment_engine import ensure_moment_schema


SOURCE = Path(__file__).resolve().parents[1] / "bnl_memory_governance.py"
TREE = ast.parse(SOURCE.read_text(encoding="utf-8"))
OWNERS = [node for node in TREE.body
          if isinstance(node, ast.FunctionDef)
          and node.name in {"_table_exists", "_moment_counts"}]
MODULE = ast.Module(body=[ast.ImportFrom(
    module="__future__", names=[ast.alias(name="annotations")], level=0,
), *OWNERS], type_ignores=[])
NAMESPACE = {"sqlite3": sqlite3, "subject_key_for_user": subject_key_for_user}
exec(compile(ast.fix_missing_locations(MODULE), str(SOURCE), "exec"), NAMESPACE)
MOMENT_COUNTS = NAMESPACE["_moment_counts"]

ORIGINAL_QUERY = """
SELECT COUNT(DISTINCT w.moment_id)
FROM memory_moment_windows w
LEFT JOIN memory_moment_participants p ON p.moment_id=w.moment_id
WHERE w.guild_id=? AND (
    p.participant_key=? OR w.canonical_ledger_entry_id IN (
        SELECT entry_id FROM memory_ledger_entries
        WHERE guild_id=? AND subject_key=?
    )
)
"""
GUILD = 7700
MEMBER = 100
SUBJECT = subject_key_for_user(MEMBER)
OTHER_SUBJECT = subject_key_for_user(200)
STAMP = "2026-10-01T00:00:00+00:00"
WINDOW_INSERT = """
INSERT INTO memory_moment_windows(
    moment_id,guild_id,channel_id,topic_key,window_started_at,
    last_activity_at,lifecycle_status,created_at,updated_at,
    canonical_ledger_entry_id
) VALUES(?,?,?,?,?,?,?,?,?,?)
"""
PARTICIPANT_INSERT = """
INSERT INTO memory_moment_participants(
    moment_id,participant_key,participant_role,created_at,updated_at
) VALUES(?,?,?,?,?)
"""
ENTRY_INSERT = """
INSERT INTO memory_ledger_entries(
    entry_id,schema_version,guild_id,subject_key,entry_type,predicate_key,
    source_class,source_table,source_row_id,source_role,visibility,
    confidence,lifecycle_status,created_at,updated_at
) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
"""


class CandidateBudgetConnection(sqlite3.Connection):
    candidate_budget = None

    def execute(self, sql, parameters=(), /):
        if self.candidate_budget is None or "memory_moment_participants" not in sql:
            return super().execute(sql, parameters)
        self.candidate_queries.append(sql)

        def progress():
            self.candidate_steps += 100
            return int(self.candidate_steps > self.candidate_budget)

        # Arm before SQLite executes. Installing a handler from a trace
        # callback can be too late for the statement being traced.
        self.set_progress_handler(progress, 100)
        try:
            return super().execute(sql, parameters)
        finally:
            self.set_progress_handler(None, 0)


class MomentDiagnosticCountTests(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:", factory=CandidateBudgetConnection)
        self.addCleanup(self.conn.close)
        ensure_moment_schema(self.conn)

    def window(self, moment, *, guild=GUILD, canonical="", status="finalized"):
        self.conn.execute(WINDOW_INSERT, (
            moment, guild, 8800, "neutral", STAMP, STAMP,
            status, STAMP, STAMP, canonical,
        ))

    def participant(self, moment, *, subject=SUBJECT, role="human"):
        self.conn.execute(PARTICIPANT_INSERT, (moment, subject, role, STAMP, STAMP))

    def entry(self, entry, *, guild=GUILD, subject=SUBJECT):
        self.conn.execute(ENTRY_INSERT, self.entry_values(entry, guild, subject))

    @staticmethod
    def entry_values(entry, guild, subject):
        return (entry, "fixture_v1", guild, subject, "shared_moment", "neutral",
                "first_party_record", "fixture", entry, "user", "public",
                "high", "active", STAMP, STAMP)

    def diagnostics(self, *, guild=GUILD, member=MEMBER):
        diag = SimpleNamespace(moment_candidate_count=0,
                               moment_needs_review_excluded=0,
                               processing_errors=[])
        MOMENT_COUNTS(self.conn, SimpleNamespace(guild_id=guild,
                                                subject_user_id=member), diag)
        return diag

    def assert_original_count(self, expected, *, guild=GUILD, member=MEMBER):
        subject = subject_key_for_user(member)
        oracle = self.conn.execute(ORIGINAL_QUERY,
                                   (guild, subject, guild, subject)).fetchone()[0]
        self.assertEqual(oracle, expected)
        diag = self.diagnostics(guild=guild, member=member)
        self.assertEqual(diag.processing_errors, [])
        self.assertEqual(diag.moment_candidate_count, oracle)
        return diag

    def test_all_roles_and_canonical_only_roots_are_distinct_within_guild(self):
        self.entry("own-canonical")
        self.entry("other-canonical", subject=OTHER_SUBJECT)
        self.entry("foreign-canonical", guild=GUILD + 1)
        self.window("all-roles", canonical="own-canonical", status="needs_review")
        for role in ("human", "model", "mentioned", "future-role", None, None):
            self.participant("all-roles", role=role)
        self.window("canonical-only", canonical="own-canonical", status="retracted")
        self.window("canonical-with-other-member", canonical="own-canonical")
        self.participant("canonical-with-other-member", subject=OTHER_SUBJECT)
        self.window("participant-only", canonical="other-canonical", status="open")
        self.participant("participant-only", role="context")
        self.window("foreign-window", guild=GUILD + 1, canonical="own-canonical")
        self.participant("foreign-window")
        self.window("wrong-canonical-guild", canonical="foreign-canonical")
        self.window("other-member", canonical="other-canonical")
        self.participant("other-member", subject=OTHER_SUBJECT)
        self.participant("dangling-participant")
        diag = self.assert_original_count(4)
        self.assertEqual(diag.moment_needs_review_excluded, 1)
        self.assert_original_count(1, guild=GUILD + 1)

    def test_empty_null_and_dangling_references_keep_original_semantics(self):
        for moment, canonical in (("empty", ""), ("null", None), ("dangling", "missing")):
            self.window(moment, canonical=canonical)
        self.participant("missing-window")
        self.assert_original_count(0)
        # An empty but real ledger ID is still a match in the original contract.
        self.entry("")
        # SQLite's TEXT PRIMARY KEY permits NULL; COUNT(DISTINCT moment_id)
        # excluded it even when its canonical root matched.
        self.window(None, canonical="")
        self.assert_original_count(1)

    def test_needs_review_diagnostic_counts_every_guild_window(self):
        self.window("member-review", status="needs_review")
        self.participant("member-review")
        self.window("unrelated-review", status="needs_review")
        self.window("foreign-review", guild=GUILD + 1, status="needs_review")
        self.window("member-finalized")
        self.participant("member-finalized")
        diag = self.assert_original_count(2)
        self.assertEqual(diag.moment_needs_review_excluded, 2)

    def test_each_call_observes_current_participants_canonical_subject_and_guild(self):
        self.entry("canonical")
        self.window("canonical-only", canonical="canonical")
        self.window("participant-only")
        self.participant("participant-only")
        self.assert_original_count(2)
        self.conn.execute("DELETE FROM memory_moment_participants")
        self.assert_original_count(1)
        self.conn.execute("UPDATE memory_ledger_entries SET subject_key=?", (OTHER_SUBJECT,))
        self.assert_original_count(0)
        self.conn.execute("UPDATE memory_ledger_entries SET subject_key=?,guild_id=?",
                          (SUBJECT, GUILD + 1))
        self.assert_original_count(0)
        self.conn.execute("UPDATE memory_ledger_entries SET guild_id=?", (GUILD,))
        self.assert_original_count(1)
        self.conn.execute("UPDATE memory_moment_windows SET guild_id=?", (GUILD + 1,))
        self.assert_original_count(0)
        self.assert_original_count(0, guild=GUILD + 1)
        self.conn.execute("UPDATE memory_ledger_entries SET guild_id=?", (GUILD + 1,))
        self.assert_original_count(1, guild=GUILD + 1)
        self.conn.execute("DELETE FROM memory_ledger_entries")
        self.assert_original_count(0, guild=GUILD + 1)

    def test_missing_moment_table_skips_and_sql_failure_remains_visible(self):
        with closing(sqlite3.connect(":memory:")) as conn:
            diag = SimpleNamespace(moment_candidate_count=0,
                                   moment_needs_review_excluded=0,
                                   processing_errors=[])
            MOMENT_COUNTS(conn, SimpleNamespace(guild_id=GUILD, subject_user_id=MEMBER), diag)
            self.assertEqual(diag.processing_errors, [])
        self.conn.execute("DROP TABLE memory_moment_participants")
        self.assertEqual(self.diagnostics().processing_errors, ["moment:OperationalError"])

    def test_candidate_vm_work_is_bounded_by_member_links(self):
        self.entry("target-canonical")
        self.window("target-participant", canonical="target-canonical")
        self.participant("target-participant")
        self.window("target-canonical-only", canonical="target-canonical")
        self.window("foreign-target", guild=GUILD + 1)
        self.participant("foreign-target")
        # Many unrelated windows, participant roles and ledger subjects must not
        # become candidate-query work. Matching ledger roots with no Moment also
        # exercise the scoped subject lookup rather than a global ledger scan.
        self.conn.executemany(WINDOW_INSERT, (
            (f"unrelated-{i}", GUILD if i < 4000 else GUILD + 1, 8800,
             "neutral", STAMP, STAMP, "finalized", STAMP, STAMP, "")
            for i in range(6000)
        ))
        self.conn.executemany(PARTICIPANT_INSERT, (
            (f"unrelated-{i}", subject_key_for_user(200 + role),
             f"role-{role}", STAMP, STAMP)
            for i in range(6000) for role in range(8)
        ))
        self.conn.executemany(ENTRY_INSERT, (
            self.entry_values(f"unrelated-ledger-{i}", GUILD, OTHER_SUBJECT)
            for i in range(20000)
        ))
        self.conn.executemany(ENTRY_INSERT, (
            self.entry_values(f"unlinked-member-ledger-{i}", GUILD, SUBJECT)
            for i in range(200)
        ))
        oracle = self.conn.execute(ORIGINAL_QUERY,
                                   (GUILD, SUBJECT, GUILD, SUBJECT)).fetchone()[0]
        self.assertEqual(oracle, 2)
        self.conn.candidate_steps = 0
        self.conn.candidate_queries = []
        self.conn.candidate_budget = 10000
        try:
            diag = self.diagnostics()
        finally:
            self.conn.candidate_budget = None
            self.conn.set_progress_handler(None, 0)
        self.assertEqual(len(self.conn.candidate_queries), 1)
        self.assertEqual(diag.processing_errors, [],
                         "candidate query exceeded 10,000 SQLite VM steps "
                         f"({self.conn.candidate_steps})")
        self.assertEqual(diag.moment_candidate_count, oracle)
        self.assertLessEqual(self.conn.candidate_steps, 10000)


if __name__ == "__main__":
    unittest.main()
