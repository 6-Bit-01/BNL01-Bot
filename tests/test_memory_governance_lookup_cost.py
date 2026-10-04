"""Fresh governed memory reads must not revisit unrelated archive lineage."""

import sqlite3
import unittest
from contextlib import contextmanager

import bnl_memory_governance as governance
from tests.test_memory_governance_v1 import insert, make_conn, req


class MemoryGovernanceLookupCostTests(unittest.TestCase):
    def setUp(self):
        self.conn = make_conn()
        self.addCleanup(self.conn.close)

    def add(self, entry_id, **kwargs):
        kwargs.setdefault("vis", "public_safe")
        kwargs.setdefault("public", 1)
        kwargs.setdefault("pred", "favorite_movie")
        insert(self.conn, eid=entry_id, **kwargs)

    def edge(self, origin, target, kind="derived_from", guild=1):
        self.conn.execute(
            "INSERT INTO memory_ledger_lineage VALUES (?,?,?,?,?)",
            (origin, guild, kind, target, "2026-07-15T00:00:00+00:00"),
        )
        self.conn.commit()

    def unrelated_archive(self):
        # Both guild-only and guild/type plans previously looked at this whole
        # history for every exact entry. Include the type used by root probes
        # as well as other outgoing relationships in the same large guild.
        self.conn.executemany(
            "INSERT INTO memory_ledger_lineage VALUES (?,?,?,?,?)",
            (
                (f"archive-{index}", 1, kind, f"archive-root-{index}", "now")
                for kind in ("derived_from", "supersedes", "reply_to")
                for index in range(20000)
            ),
        )
        self.conn.commit()

    @contextmanager
    def bounded_work(self, max_steps=10000):
        # VM work is deterministic across slow hosts. This measures actual SQL
        # execution, without making elapsed time or planner text the assertion.
        steps = [0]

        def progress():
            steps[0] += 100
            return int(steps[0] > max_steps)

        self.conn.set_progress_handler(progress, 100)
        try:
            yield
        finally:
            self.conn.set_progress_handler(None, 0)

    def result(self):
        self.conn.execute("BEGIN")
        try:
            return governance.build_governed_context(
                self.conn, req(visibility_allowance="public_safe"),
                initialize_schema=False,
            )
        finally:
            self.conn.rollback()

    def test_exact_lineage_read_is_bounded_and_retains_guild_type_order(self):
        self.unrelated_archive()
        self.edge("current", "later", "supersedes")
        self.edge("current", "first", "derived_from")
        self.edge("current", "foreign", "derived_from", guild=2)
        with self.bounded_work(1000):
            self.assertEqual(governance._lineage(self.conn, 1, "current"),
                             (("derived_from", "first"), ("supersedes", "later")))
            self.assertEqual(governance._lineage(self.conn, 1, "missing"), ())

    def test_projection_root_read_is_bounded_across_archive_revisions(self):
        self.unrelated_archive()
        self.add("root")
        self.add("projection", source="derived_summary", derived=1, projection=1)
        self.edge("projection", "root")
        self.edge("projection", "not-a-root", "reply_to")
        self.edge("projection", "foreign", guild=2)
        with self.bounded_work(1000):
            self.assertTrue(governance._has_eligible_projection_root(
                self.conn, 1, "projection"))
            self.assertFalse(governance._has_eligible_projection_root(
                self.conn, 2, "projection"))

    def test_full_selection_is_bounded_and_keeps_projections_shadow_only(self):
        self.unrelated_archive()
        self.add("root", value="favorite movie is Modular Synths")
        self.add("projection", value="derived synths", source="derived_summary",
                 derived=1, projection=1)
        self.add("nested", value="nested synths", source="derived_summary",
                 derived=1, projection=1)
        self.edge("projection", "root")
        self.edge("nested", "projection")
        with self.bounded_work():
            result = self.result()
        self.assertEqual([item.entry_id for item in result.selected], ["root"])
        self.assertFalse(result.diagnostics.processing_errors)
        self.assertEqual({item.entry_id: item.reason for item in result.exclusions}, {
            "projection": "projection_shadow_only", "nested": "projection_shadow_only",
        })

    def test_same_connection_rechecks_correction_privacy_edit_and_deletion(self):
        self.add("current", value="favorite movie is Modular Synths")
        self.assertEqual([item.entry_id for item in self.result().selected], ["current"])
        self.conn.execute(
            "UPDATE memory_ledger_entries SET normalized_value='favorite movie is Synths Again' "
            "WHERE entry_id='current'"
        )
        self.conn.commit()
        self.assertIn("Synths Again", self.result().rendered_context)
        self.conn.execute(
            "UPDATE memory_ledger_entries SET visibility='private',public_usable=0 "
            "WHERE entry_id='current'"
        )
        self.conn.commit()
        self.assertEqual(self.result().selected, ())
        self.conn.execute(
            "UPDATE memory_ledger_entries SET visibility='public_safe',public_usable=1 "
            "WHERE entry_id='current'"
        )
        self.conn.commit()
        self.edge("new-correction", "current", "correction_of")
        self.assertEqual(self.result().selected, ())
        self.conn.execute("DELETE FROM memory_ledger_entries WHERE entry_id='current'")
        self.conn.commit()
        self.assertEqual(self.result().selected, ())
        # No cached lineage is allowed, even on a reused connection.
        self.conn.execute("DELETE FROM memory_ledger_lineage WHERE entry_id='new-correction'")
        self.conn.commit()
        self.assertEqual(governance._lineage(self.conn, 1, "new-correction"), ())

    def test_projection_cycles_missing_and_retracted_roots_are_freshly_rejected(self):
        for entry_id in ("cycle-a", "cycle-b"):
            self.add(entry_id, source="derived_summary", derived=1, projection=1)
        self.edge("cycle-a", "cycle-b")
        self.edge("cycle-b", "cycle-a")
        self.assertFalse(governance._has_eligible_projection_root(self.conn, 1, "cycle-a"))
        self.add("root")
        self.edge("cycle-b", "root")
        self.assertTrue(governance._has_eligible_projection_root(self.conn, 1, "cycle-a"))
        self.conn.execute(
            "UPDATE memory_ledger_entries SET lifecycle_status='retracted' WHERE entry_id='root'"
        )
        self.conn.commit()
        self.assertFalse(governance._has_eligible_projection_root(self.conn, 1, "cycle-a"))
        self.conn.execute("DELETE FROM memory_ledger_entries WHERE entry_id='root'")
        self.conn.commit()
        self.assertFalse(governance._has_eligible_projection_root(self.conn, 1, "cycle-a"))


if __name__ == "__main__":
    unittest.main()
