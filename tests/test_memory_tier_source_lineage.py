import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import bnl_memory_ledger as ledger
from bnl_memory_governance import purge_conversation_ledger_sources


class MemoryTierSourceLineageTests(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.executescript("""
            CREATE TABLE conversations (
              id INTEGER PRIMARY KEY, guild_id INTEGER, user_id INTEGER,
              role TEXT, channel_policy TEXT, content TEXT);
            CREATE TABLE memory_tiers (
              id INTEGER PRIMARY KEY, guild_id INTEGER, user_id INTEGER,
              tier TEXT, summary TEXT, source_role TEXT,
              source_channel_policy TEXT);
        """)
        ledger.ensure_memory_tier_source_schema(self.conn)

    def tearDown(self):
        self.conn.close()

    def source(self, row_id, *, guild=1, user=7, role="user", policy="public_home"):
        self.conn.execute("INSERT INTO conversations VALUES(?,?,?,?,?,?)", (row_id, guild, user, role, policy, "A meaningful source for the bean recipe."))

    def tier(self, row_id, *, guild=1, user=7, tier="short", role="user", policy="public_home", complete=0):
        self.conn.execute("INSERT INTO memory_tiers VALUES(?,?,?,?,?,?,?,?)", (row_id, guild, user, tier, "The remembered bean recipe.", role, policy, complete))

    def link(self, tier_id, *sources, guild=1):
        ledger.attach_memory_tier_conversation_sources(self.conn, guild_id=guild, tier_row_id=tier_id, source_row_ids=sources)

    def root(self, source_id, *, scalar=False):
        ledger.ensure_memory_ledger_schema(self.conn)
        return ledger.insert_ledger_entry(self.conn, ledger.LedgerEntry(
            guild_id=1, source_table="conversations", source_row_id=source_id,
            source_role="user", source_revision="1", subject_key="discord_user:7",
            entry_type="preference" if scalar else "observation",
            predicate_key="favorite_color" if scalar else "conversation",
            value="green" if scalar else "A meaningful source for the bean recipe.",
            source_class=ledger.SourceClass.FIRST_PARTY_RECORD,
            visibility=ledger.Visibility.PUBLIC_SAFE,
            channel_policy="public_home", public_usable=True,
        )).entry_id

    def projection(self, tier_id, tier="short", revision="2026-09-11T00:00:00Z"):
        return ledger.shadow_memory_tier_row(self.conn, row_id=tier_id, user_id=7, guild_id=1, tier=tier, summary="The remembered bean recipe.", updated_at=revision)

    def test_attach_validates_entire_batch_and_exact_owner_scope(self):
        self.source(1)
        self.source(2, user=8)
        self.source(3, guild=2)
        self.source(4, role="model")
        self.source(5, policy="sealed_test")
        self.tier(10)
        for bad in (2, 3, 4, 5, 99, True, "1 OR 1=1"):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                self.link(10, 1, bad)
            self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM memory_tier_conversation_sources").fetchone()[0], 0)
        self.link(10, 1, 1)
        self.assertEqual(ledger.retained_tier_conversation_sources(self.conn, guild_id=1, source_row_ids=[1, 2]), {1})
        self.assertEqual(ledger.unresolved_memory_tier_sources_count(self.conn, guild_id=1, user_id=7), 0)

    def test_promotions_and_existing_destination_keep_all_original_leaves(self):
        for source_id in (1, 2, 3):
            self.source(source_id)
            self.tier(10 + source_id)
            self.link(10 + source_id, source_id)
        self.tier(20, tier="medium", role="consolidation", complete=1)
        ledger.carry_memory_tier_conversation_sources(self.conn, guild_id=1, target_tier_row_id=20, source_tier_row_ids=[11, 12])
        self.conn.execute("DELETE FROM memory_tiers WHERE id IN (11,12)")
        ledger.carry_memory_tier_conversation_sources(self.conn, guild_id=1, target_tier_row_id=20, source_tier_row_ids=[13])
        self.conn.execute("DELETE FROM memory_tiers WHERE id=13")
        self.tier(30, tier="long", role="consolidation", complete=1)
        ledger.carry_memory_tier_conversation_sources(self.conn, guild_id=1, target_tier_row_id=30, source_tier_row_ids=[20])
        self.conn.execute("DELETE FROM memory_tiers WHERE id=20")
        self.assertEqual(ledger.retained_tier_conversation_sources(self.conn, guild_id=1, source_row_ids=[1, 2, 3]), {1, 2, 3})
        self.assertEqual(self.conn.execute("SELECT DISTINCT tier_row_id FROM memory_tier_conversation_sources").fetchall(), [(30,)])
        self.conn.execute("DELETE FROM memory_tiers WHERE id=30")
        self.assertEqual(ledger.retained_tier_conversation_sources(self.conn, guild_id=1, source_row_ids=[1, 2, 3]), set())

    def test_mixed_legacy_consolidation_remains_unresolved_without_invented_links(self):
        self.source(1)
        self.tier(11)
        self.link(11, 1)
        self.tier(20, tier="medium", role="consolidation")
        ledger.carry_memory_tier_conversation_sources(self.conn, guild_id=1, target_tier_row_id=20, source_tier_row_ids=[11])
        self.assertEqual(ledger.unresolved_memory_tier_sources_count(self.conn, guild_id=1, user_id=7), 1)
        self.tier(30, tier="long", role="consolidation", complete=1)
        ledger.carry_memory_tier_conversation_sources(self.conn, guild_id=1, target_tier_row_id=30, source_tier_row_ids=[20])
        self.assertEqual(self.conn.execute("SELECT source_lineage_complete FROM memory_tiers WHERE id=30").fetchone(), (0,))
        self.assertEqual(self.conn.execute("SELECT conversation_row_id FROM memory_tier_conversation_sources WHERE tier_row_id=30").fetchall(), [(1,)])

    def test_carry_rejects_cross_member_or_missing_parent_before_writing(self):
        self.source(1, user=8)
        self.tier(11, user=8)
        self.link(11, 1)
        self.tier(20, tier="medium", role="consolidation", complete=1)
        for parent in (11, 99):
            with self.assertRaises(ValueError):
                ledger.carry_memory_tier_conversation_sources(self.conn, guild_id=1, target_tier_row_id=20, source_tier_row_ids=[parent])
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM memory_tier_conversation_sources WHERE tier_row_id=20").fetchone()[0], 0)

    def test_source_delete_invalidates_entire_summary_and_projection_only(self):
        self.source(1)
        self.source(2)
        self.source(3, user=8)
        self.tier(10, role="consolidation")
        self.link(10, 1, 2)
        self.tier(11, user=8)
        self.link(11, 3)
        self.root(1)
        projection = self.projection(10)
        self.conn.execute("DELETE FROM conversations WHERE id=1")
        self.assertEqual(self.conn.execute("SELECT id FROM memory_tiers").fetchall(), [(11,)])
        self.assertEqual(self.conn.execute("SELECT normalized_value,lifecycle_status FROM memory_ledger_entries WHERE entry_id=?", (projection.entry_id,)).fetchone(), ("", "retracted"))
        self.assertEqual(ledger.retained_tier_conversation_sources(self.conn, guild_id=1, source_row_ids=[2, 3]), {3})

    def test_source_change_and_no_op_respect_exact_source_state(self):
        self.source(1)
        self.tier(10)
        self.link(10, 1)
        self.conn.execute("UPDATE conversations SET content=content WHERE id=1")
        self.assertIsNotNone(self.conn.execute("SELECT id FROM memory_tiers WHERE id=10").fetchone())
        self.conn.execute("UPDATE conversations SET channel_policy='sealed_test' WHERE id=1")
        self.assertIsNone(self.conn.execute("SELECT id FROM memory_tiers WHERE id=10").fetchone())

    def test_scalar_source_lifecycle_invalidates_and_cannot_be_reattached(self):
        self.source(1)
        self.tier(10)
        self.link(10, 1)
        scalar = self.root(1, scalar=True)
        self.conn.execute("UPDATE memory_ledger_entries SET lifecycle_status='forgotten',normalized_value='' WHERE entry_id=?", (scalar,))
        self.assertIsNone(self.conn.execute("SELECT id FROM memory_tiers WHERE id=10").fetchone())
        self.tier(11)
        with self.assertRaisesRegex(ValueError, "source_retired"):
            self.link(11, 1)
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM memory_tier_conversation_sources").fetchone()[0], 0)

    def test_raw_ledger_deletion_invalidates_even_before_transcript_removal(self):
        self.source(1)
        self.tier(10)
        self.link(10, 1)
        root = self.root(1)
        self.conn.execute("DELETE FROM memory_ledger_entries WHERE entry_id=?", (root,))
        self.assertIsNotNone(self.conn.execute("SELECT id FROM conversations WHERE id=1").fetchone())
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM memory_tiers").fetchone()[0], 0)

    def test_explicit_purge_without_ledger_or_raw_copy_invalidates_tiers(self):
        for initialize_ledger in (False, True):
            with self.subTest(initialize_ledger=initialize_ledger):
                self.source(1)
                self.tier(10)
                self.link(10, 1)
                if initialize_ledger:
                    ledger.ensure_memory_ledger_schema(self.conn)
                result = purge_conversation_ledger_sources(self.conn, guild_id=1, source_row_ids=[1], reason="clear_user_history")
                self.assertEqual(result["memory_tiers_invalidated"], 1)
                self.assertEqual(result["raw_ledger_entries"], 0)
                self.conn.execute("DELETE FROM conversations")

    def test_original_root_lineage_survives_ambiguous_parent_revisions(self):
        self.source(1)
        self.tier(10)
        self.link(10, 1)
        root = self.root(1)
        first = self.projection(10)
        self.projection(10, revision="2026-09-11T00:01:00Z")
        self.tier(20, tier="medium", role="consolidation", complete=1)
        ledger.carry_memory_tier_conversation_sources(self.conn, guild_id=1, target_tier_row_id=20, source_tier_row_ids=[10])
        self.conn.execute("DELETE FROM memory_tiers WHERE id=10")
        middle = ledger.shadow_memory_tier_row(self.conn, row_id=20, user_id=7, guild_id=1, tier="medium", summary="The remembered bean recipe.", derived_from_source_row_ids=(10,))
        self.assertEqual(self.conn.execute("SELECT target_entry_id FROM memory_ledger_lineage WHERE entry_id=? AND lineage_type='derived_from'", (middle.entry_id,)).fetchall(), [(root,)])
        self.assertNotEqual(self.conn.execute("SELECT normalized_value FROM memory_ledger_entries WHERE entry_id=?", (first.entry_id,)).fetchone()[0], "")
        self.assertEqual(self.projection(10).reason_code, "tier_source_retired")
        self.assertEqual(ledger.shadow_memory_tier_row(self.conn, row_id=20, user_id=7, guild_id=1, tier="medium", summary="Stale summary.").reason_code, "tier_source_changed")
        self.conn.execute("DELETE FROM conversations WHERE id=1")
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM memory_ledger_entries WHERE source_table='memory_tiers' AND normalized_value<>''").fetchone()[0], 0)

    def test_source_deleted_after_projection_read_cannot_be_rehydrated(self):
        self.source(1)
        self.tier(10)
        self.link(10, 1)
        self.root(1)
        self.conn.commit()
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "memory.sqlite"
            with sqlite3.connect(path) as setup:
                self.conn.backup(setup)
                setup.execute("PRAGMA journal_mode=WAL")
            original = ledger._entry_ids_for_source_rows
            def delete_after_read(conn, **kwargs):
                self.assertTrue(conn.in_transaction)
                with sqlite3.connect(path, timeout=0.1) as remove:
                    remove.execute("DELETE FROM conversations WHERE id=1")
                return original(conn, **kwargs)
            with sqlite3.connect(path, timeout=0.1) as projection_conn:
                with mock.patch.object(ledger, "_entry_ids_for_source_rows", side_effect=delete_after_read):
                    with self.assertRaises(sqlite3.OperationalError):
                        ledger.shadow_memory_tier_row(projection_conn, row_id=10, user_id=7, guild_id=1, tier="short", summary="The remembered bean recipe.")
                projection_conn.rollback()
            with sqlite3.connect(path) as check:
                self.assertEqual(check.execute("SELECT COUNT(*) FROM memory_tiers").fetchone()[0], 0)
                self.assertEqual(check.execute("SELECT COUNT(*) FROM memory_ledger_entries WHERE source_table='memory_tiers'").fetchone()[0], 0)

    def test_retention_read_is_read_only_chunked_and_ignores_phantom_pins(self):
        for source_id in range(1, 502):
            self.source(source_id)
        self.tier(10, role="consolidation")
        self.link(10, *range(1, 502))
        self.conn.execute("INSERT INTO memory_tier_conversation_sources VALUES(1,999,999)")
        changes = self.conn.total_changes
        self.conn.execute("PRAGMA query_only=ON")
        self.assertEqual(ledger.retained_tier_conversation_sources(self.conn, guild_id=1, source_row_ids=range(1, 1000)), set(range(1, 502)))
        self.assertEqual(ledger.retained_tier_conversation_sources(self.conn, guild_id=2, source_row_ids=[1]), set())
        self.assertEqual(self.conn.total_changes, changes)

    def test_source_invalidation_rolls_back_with_caller_transaction(self):
        self.source(1)
        self.tier(10)
        self.link(10, 1)
        self.root(1)
        projection = self.projection(10)
        self.conn.commit()
        with self.assertRaises(RuntimeError):
            with self.conn:
                self.conn.execute("DELETE FROM conversations WHERE id=1")
                raise RuntimeError("simulated source transaction failure")
        self.assertEqual(ledger.retained_tier_conversation_sources(self.conn, guild_id=1, source_row_ids=[1]), {1})
        self.assertEqual(self.conn.execute("SELECT lifecycle_status FROM memory_ledger_entries WHERE entry_id=?", (projection.entry_id,)).fetchone()[0], "review_only")

    def test_isolated_ledger_schema_does_not_invent_tier_tables(self):
        with sqlite3.connect(":memory:") as isolated:
            ledger.ensure_memory_ledger_schema(isolated)
            self.assertIsNone(isolated.execute("SELECT name FROM sqlite_master WHERE name='memory_tier_conversation_sources'").fetchone())
            self.assertEqual(ledger.retained_tier_conversation_sources(isolated, guild_id=1, source_row_ids=[1]), set())

    def test_unrelated_history_does_not_expand_source_invalidation_work(self):
        self.source(1)
        ledger.ensure_memory_ledger_schema(self.conn)
        rows = []
        for source_id in range(1, 10002):
            rows.append(("raw_%s" % source_id, "memory_ledger_v1", 1,
                         "discord_user:7", "observation", "conversation",
                         "An unrelated source.", "first_party_record", "conversations",
                         str(source_id), "user", "public_safe", "high", "active", "", ""))
            if source_id > 1:
                rows.append(("tier_%s" % source_id, "memory_ledger_v1", 1,
                             "discord_user:7", "derived_summary", "memory_tier:short",
                             "An unrelated historical summary.", "derived_summary", "memory_tiers",
                             str(source_id), "derived_projection", "private", "low", "review_only", "", ""))
        self.conn.executemany("""
            INSERT INTO memory_ledger_entries (
              entry_id,schema_version,guild_id,subject_key,entry_type,predicate_key,
              normalized_value,source_class,source_table,source_row_id,source_role,
              visibility,confidence,lifecycle_status,created_at,updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, rows)
        self.conn.executemany("INSERT INTO memory_ledger_lineage VALUES(?,?,?,?,?)", [
            ("tier_%s" % source_id, 1, "derived_from", "raw_%s" % source_id, "")
            for source_id in range(2, 10002)
        ])
        self.conn.commit()
        operations = (
            lambda: ledger.invalidate_memory_tiers_for_conversation_sources(self.conn, guild_id=1, source_row_ids=[1]),
            lambda: self.conn.execute("DELETE FROM conversations WHERE id=1"),
            lambda: self.conn.execute("DELETE FROM memory_ledger_entries WHERE entry_id='raw_1'"),
        )
        for index, operation in enumerate(operations):
            steps = [0]
            def count_steps():
                steps[0] += 100
                return 0
            self.conn.set_progress_handler(count_steps, 100)
            try:
                operation()
            finally:
                self.conn.set_progress_handler(None, 0)
            with self.subTest(operation=index):
                self.assertLess(steps[0], 10000)
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM memory_ledger_entries WHERE source_table='memory_tiers' AND normalized_value<>''").fetchone()[0], 10000)


if __name__ == "__main__":
    unittest.main()
