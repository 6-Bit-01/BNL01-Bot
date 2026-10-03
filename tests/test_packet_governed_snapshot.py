"""The real packet adapter preserves governed batching and transaction ownership."""
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import bnl_memory_governance as governance
import bnl_moment_engine as moments
import bnl_relationship_engine as relationships
import bnl_unified_intelligence_packet as packets
from tests.test_memory_governance_v1 import insert, make_conn


class PacketGovernedSnapshotTests(unittest.TestCase):
    def setUp(self):
        self.conn = make_conn()
        self.addCleanup(self.conn.close)
        moments.ensure_moment_schema(self.conn)
        relationships.ensure_relationship_v2_schema(self.conn)
        packets.ensure_schema(self.conn)
        self.conn.execute("CREATE TABLE pending_work (value TEXT)")
        self.conn.commit()
        self.request = packets.IntelligencePacketRequest(
            guild_id=1, subject_user_id=10, route_mode="normal_chat",
            conversation_surface="discord_conversation", channel_id=70,
            channel_policy="public_home", visibility_allowance="public_safe",
            user_text="What is my favorite movie?", now="2026-07-16T00:00:00+00:00",
        )
        self.flags = {
            "BNL_MEMORY_LEDGER_SHADOW_ENABLED": "true",
            "BNL_MOMENT_ENGINE_SHADOW_ENABLED": "true",
            "BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED": "true",
            "BNL_RELATIONSHIP_V2_SHADOW_ENABLED": "true",
            "BNL_UNIFIED_INTELLIGENCE_PACKET_SHADOW_ENABLED": "true",
        }

    def add(self, entry_id="current", **kwargs):
        kwargs.setdefault("vis", "public_safe")
        kwargs.setdefault("public", 1)
        kwargs.setdefault("pred", "favorite_movie")
        kwargs.setdefault("value", "favorite movie is Alien")
        insert(self.conn, eid=entry_id, **kwargs)
        self.conn.execute(
            "UPDATE memory_ledger_entries SET route_mode='normal_chat',"
            "channel_policy='public_home' WHERE entry_id=?", (entry_id,),
        )
        self.conn.commit()

    def select(self, conn=None):
        diagnostics = packets.IntelligencePacketDiagnostics()
        exclusions = []
        items = packets._governed_items(
            conn or self.conn, self.request, diagnostics, exclusions, broad=False,
        )
        return items, diagnostics

    def clone_connections(self, directory):
        path = str(Path(directory) / "packet.sqlite")
        selector = sqlite3.connect(path, timeout=0.01)
        observer = sqlite3.connect(path, timeout=0.01)
        self.conn.backup(selector)
        selector.commit()
        return selector, observer

    def test_production_packet_path_batches_controls_before_receipt(self):
        self.add()
        self.conn.executemany(
            """INSERT INTO memory_ledger_entries
            (entry_id,schema_version,guild_id,subject_key,entry_type,predicate_key,
             source_class,source_table,source_row_id,source_role,visibility,confidence,
             lifecycle_status,created_at,updated_at)
            VALUES (?,'memory_ledger_v1',1,'discord_user:10','observation',
                    'conversation','public_observation','test',?,'user','public_safe',
                    'high','active','now','now')""",
            [(f"transient-{index}", f"transient-{index}") for index in range(1001)],
        )
        self.conn.commit()
        statements = []
        self.conn.set_trace_callback(statements.append)
        try:
            with mock.patch.object(
                governance, "_subject_entry_controls", wraps=governance._subject_entry_controls,
            ) as controls:
                packet = packets.build_packet(self.conn, self.request, environ=self.flags)
        finally:
            self.conn.set_trace_callback(None)
        self.assertIsNotNone(packet)
        controls.assert_called_once()
        self.assertEqual(len(controls.call_args.args[2]), 1002)
        self.assertEqual(sum("SELECT DISTINCT target_entry_id" in sql for sql in statements), 3)
        self.assertFalse(any("lifecycle_status='forgotten' AND entry_id<>" in sql for sql in statements))
        self.assertIn("ledger:current", [item.source_ref for item in packet.items])
        self.assertEqual(packet.diagnostics.processing_errors, [])
        self.assertEqual(packet.diagnostics.revalidation_status, "passed")
        self.assertTrue(self.conn.execute(
            "SELECT 1 FROM memory_governance_intelligence_packet_runs WHERE packet_id=?",
            (packet.packet_id,),
        ).fetchone())

    def test_owned_scope_prepares_schemas_then_reads_and_versions_without_commits(self):
        self.add()
        original_governed = packets.build_governed_context
        original_digest = packets._ledger_entry_digest
        observed = []

        def governed(conn, request, **kwargs):
            observed.append("selection")
            self.assertTrue(conn.in_transaction)
            self.assertIs(kwargs["initialize_schema"], False)
            return original_governed(conn, request, **kwargs)

        def digest(conn, entry_id):
            observed.append("fingerprint")
            self.assertTrue(conn.in_transaction)
            return original_digest(conn, entry_id)

        with mock.patch.object(packets, "build_governed_context", side_effect=governed), mock.patch.object(
            packets, "_ledger_entry_digest", side_effect=digest,
        ):
            items, diagnostics = self.select()
        self.assertEqual(observed, ["selection", "fingerprint"])
        self.assertEqual(len(items), 1)
        self.assertEqual(diagnostics.processing_errors, [])
        self.assertFalse(self.conn.in_transaction)

    def test_caller_pending_work_remains_uncommitted_and_transaction_remains_owned(self):
        self.add()
        with tempfile.TemporaryDirectory() as directory:
            caller, observer = self.clone_connections(directory)
            try:
                caller.execute("INSERT INTO pending_work VALUES ('keep pending')")
                with mock.patch.object(
                    packets, "ensure_governance_schema", side_effect=AssertionError("caller owns work"),
                ), mock.patch.object(
                    packets, "ensure_moment_schema", side_effect=AssertionError("caller owns work"),
                ):
                    items, diagnostics = self.select(caller)
                self.assertEqual(len(items), 1)
                self.assertEqual(diagnostics.processing_errors, [])
                self.assertTrue(caller.in_transaction)
                self.assertEqual(observer.execute("SELECT count(*) FROM pending_work").fetchone()[0], 0)
                caller.rollback()
                self.assertEqual(caller.execute("SELECT count(*) FROM pending_work").fetchone()[0], 0)
            finally:
                observer.close()
                caller.close()

    def test_later_selection_revalidates_privacy_corrections_and_forgetting(self):
        self.add()
        self.assertEqual([item.source_ref for item in self.select()[0]], ["ledger:current"])
        self.conn.execute("UPDATE memory_ledger_entries SET visibility='private',public_usable=0")
        self.conn.commit()
        self.assertEqual(self.select()[0], [])
        self.conn.execute("UPDATE memory_ledger_entries SET visibility='public_safe',public_usable=1")
        self.conn.commit()
        self.conn.execute(
            "INSERT INTO memory_ledger_lineage VALUES ('correction',1,'correction_of','current','now')"
        )
        self.conn.commit()
        self.assertEqual(self.select()[0], [])
        self.conn.execute("DELETE FROM memory_ledger_lineage")
        self.conn.commit()
        self.assertEqual(len(self.select()[0]), 1)
        self.add("tombstone", life="forgotten", source_revision="old")
        self.conn.execute("UPDATE memory_ledger_entries SET source_row_id='current' WHERE entry_id='tombstone'")
        self.conn.commit()
        items, diagnostics = self.select()
        self.assertEqual(items, [])
        self.assertEqual(diagnostics.excluded_by_reason["governance:forgotten_source_tombstone"], 1)

    def test_source_fingerprint_stays_on_selection_snapshot_and_later_correction_is_fresh(self):
        self.add()
        with tempfile.TemporaryDirectory() as directory:
            selector, writer = self.clone_connections(directory)
            selector.execute("PRAGMA journal_mode=WAL")
            original_governed = packets.build_governed_context
            before_digest = packets._ledger_entry_digest(writer, "current")

            def try_concurrent_correction(conn, request, **kwargs):
                result = original_governed(conn, request, **kwargs)
                writer.execute(
                    "INSERT INTO memory_ledger_lineage VALUES ('correction',1,'correction_of','current','now')"
                )
                writer.commit()
                self.assertTrue(conn.in_transaction)
                return result

            try:
                with mock.patch.object(packets, "build_governed_context", side_effect=try_concurrent_correction):
                    items, diagnostics = self.select(selector)
                self.assertEqual(len(items), 1)
                self.assertEqual(diagnostics.processing_errors, [])
                self.assertFalse(selector.in_transaction)
                self.assertEqual(items[0].source_digest, before_digest)
                self.assertNotEqual(items[0].source_digest, packets._ledger_entry_digest(writer, "current"))
                self.assertEqual(self.select(selector)[0], [])
            finally:
                writer.close()
                selector.close()

    def test_fresh_packet_revalidation_rejects_privacy_and_correction_changes(self):
        self.add()
        prepared = packets.build_packet(self.conn, self.request, environ=self.flags)
        self.assertEqual(prepared.diagnostics.revalidation_status, "passed")
        self.conn.execute("UPDATE memory_ledger_entries SET visibility='private',public_usable=0")
        self.conn.commit()
        self.assertFalse(packets.revalidate_packet(self.conn, prepared, environ=self.flags).valid)
        self.conn.execute("UPDATE memory_ledger_entries SET visibility='public_safe',public_usable=1")
        self.conn.commit()
        prepared = packets.build_packet(self.conn, self.request, environ=self.flags)
        self.assertEqual(prepared.diagnostics.revalidation_status, "passed")
        self.conn.execute(
            "INSERT INTO memory_ledger_lineage VALUES ('correction',1,'correction_of','current','now')"
        )
        self.conn.commit()
        self.assertFalse(packets.revalidate_packet(self.conn, prepared, environ=self.flags).valid)

    def test_owned_read_scope_closes_on_failure_without_ending_caller_scope(self):
        self.add()
        with mock.patch.object(packets, "_governed_items_in_snapshot", side_effect=ValueError("test failure")):
            with self.assertRaises(ValueError):
                self.select()
            self.assertFalse(self.conn.in_transaction)
            self.conn.execute("BEGIN")
            with self.assertRaises(ValueError):
                self.select()
            self.assertTrue(self.conn.in_transaction)
            self.conn.rollback()

    def test_schema_preparation_failure_releases_own_work_and_keeps_other_lanes(self):
        self.add()

        def fail_preparation(conn):
            conn.execute("INSERT INTO pending_work VALUES ('failed preparation')")
            raise sqlite3.OperationalError("database is locked")

        with mock.patch.object(packets, "ensure_moment_schema", side_effect=fail_preparation), mock.patch.object(
            packets, "_episode_items", wraps=packets._episode_items,
        ) as episodes:
            items, diagnostics = self.select()
            self.assertEqual(items, [])
            self.assertIn("governance:OperationalError", diagnostics.processing_errors)
            self.assertFalse(self.conn.in_transaction)
            packet = packets.build_packet(self.conn, self.request, environ=self.flags)
        episodes.assert_called_once()
        self.assertIn("governance:OperationalError", packet.diagnostics.processing_errors)
        self.assertFalse(any(item.source_ref == "ledger:current" for item in packet.items))
        self.assertEqual(self.conn.execute("SELECT count(*) FROM pending_work").fetchone()[0], 0)


if __name__ == "__main__":
    unittest.main()
