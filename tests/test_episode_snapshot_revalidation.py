"""Episode reads share one validation snapshot, never a later validation."""
from dataclasses import asdict, replace
import sqlite3
import unittest
from unittest import mock

import bnl_unified_intelligence_packet as packet_module
from tests import test_unified_intelligence_packet as fixtures


class EpisodeSnapshotRevalidationTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.UnifiedIntelligencePacketTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        self.conn = self.fixture.conn
        self.fixture.add_public_moment()
        self.fixture.add_followup_public_moment()
        self.request = replace(
            self.fixture.public_request(text="BNL, the synth chorus is interesting."),
            frame_schema_version="situation_frame_v1",
            frame_revision="sf_neutral_episode",
            frame_input_evidence_digest="c" * 64,
            frame_status="resolved",
            frame_subject_requirement="not_applicable",
        )
        self.packet = packet_module.build_packet(
            self.conn, self.request, persist=False, environ=self.fixture.flags,
        )
        self.conn.commit()
        self.episodes = tuple(i for i in self.packet.items if i.lane == "episode")
        self.assertEqual(len(self.episodes), 2)
        self.assertEqual(
            len([i for i in self.packet.validation_items if i.lane == "episode"]), 2,
        )

    def validate(self, packet=None):
        return packet_module.revalidate_packet(
            self.conn, packet or self.packet, environ=self.fixture.flags,
        )

    def scalar_result(self, packet=None):
        # The default _episode_version path remains the original scalar owner.
        original = packet_module._episode_version
        with mock.patch.object(
            packet_module, "_episode_version",
            side_effect=lambda conn, value, item, **_: original(conn, value, item),
        ):
            return self.validate(packet)

    def test_real_two_episode_result_matches_scalar_with_one_read_each_fresh_pass(self):
        original_packet = asdict(self.packet)
        changes = self.conn.total_changes
        with mock.patch.object(packet_module, "_episode_rows", wraps=packet_module._episode_rows) as reads:
            scalar = self.scalar_result()
        self.assertEqual(reads.call_count, 2)
        self.assertTrue(scalar.valid)
        for _ in range(2):
            with mock.patch.object(packet_module, "_episode_rows", wraps=packet_module._episode_rows) as reads:
                actual = self.validate()
            self.assertEqual(asdict(actual), asdict(scalar))
            self.assertEqual(reads.call_count, 1)
            self.assertEqual(reads.call_args.args[1].now, self.request.now)
            self.assertFalse(self.conn.in_transaction)
        self.assertEqual(asdict(self.packet), original_packet)
        self.assertEqual(self.conn.total_changes, changes)

    def test_fresh_pass_rejects_changed_projection_privacy_retraction_and_deleted_source(self):
        for update in (
            "UPDATE memory_moment_windows SET summary=summary || ' A changed projection.'",
            "UPDATE memory_ledger_entries SET visibility='private', public_usable=0 WHERE source_table='conversations'",
            "UPDATE memory_ledger_entries SET lifecycle_status='retracted' WHERE source_table='conversations'",
            "DELETE FROM memory_ledger_entries WHERE source_table='conversations'",
        ):
            with self.subTest(update=update):
                self.assertTrue(self.validate().valid)
                self.conn.execute("SAVEPOINT change_sources")
                try:
                    self.conn.execute(update)
                    expected = self.scalar_result()
                    actual = self.validate()
                    self.assertEqual(asdict(actual), asdict(expected))
                    self.assertFalse(actual.valid)
                    self.assertGreater(actual.changed_source_count, 0)
                finally:
                    self.conn.execute("ROLLBACK TO change_sources")
                    self.conn.execute("RELEASE change_sources")

    def test_new_correction_lineage_is_seen_on_next_fresh_pass(self):
        ledger = fixtures.ledger
        self.assertTrue(self.validate().valid)
        target = self.conn.execute(
            "SELECT ledger_entry_id FROM memory_moment_members ORDER BY ledger_entry_id LIMIT 1"
        ).fetchone()[0]
        result = ledger.insert_ledger_entry(
            self.conn,
            ledger.LedgerEntry(
                guild_id=1, source_table="member_memory_controls",
                source_row_id=500, source_revision="500", source_role="member_control",
                entry_type="boundary", subject_key="discord_user:7",
                subject_display_name="Crow", predicate_key="source_correction",
                value="The earlier source was corrected by its author.",
                source_class=ledger.SourceClass.FIRST_PARTY_RECORD,
                route_mode="normal_chat", channel_id=10, channel_name="barcode-bot",
                channel_policy="sealed_test", visibility=ledger.Visibility.SEALED_TEST,
                confidence=ledger.Confidence.HIGH, observed_at=self.request.now,
                source_sequence=500, lineage=(("correction_of", target), ("supersedes", target)),
            ),
        )
        self.assertEqual(result.outcome, "inserted")
        self.conn.commit()
        expected = self.scalar_result()
        actual = self.validate()
        self.assertEqual(asdict(actual), asdict(expected))
        self.assertFalse(actual.valid)
        self.assertGreater(actual.changed_source_count, 0)

    def test_same_connection_write_between_items_forces_new_selection(self):
        original = packet_module._episode_version
        seen = []

        def mutate_after_first(conn, value, item, **kwargs):
            result = original(conn, value, item, **kwargs)
            seen.append(item.source_ref)
            if len(seen) == 1:
                conn.execute(
                    "UPDATE memory_ledger_entries SET public_usable=0 WHERE source_table='conversations'"
                )
            return result

        self.conn.execute("BEGIN")
        try:
            with mock.patch.object(packet_module, "_episode_version", side_effect=mutate_after_first):
                with mock.patch.object(packet_module, "_episode_rows", wraps=packet_module._episode_rows) as reads:
                    actual = self.validate()
            self.assertEqual(len(seen), 2)
            self.assertEqual(reads.call_count, 2)
            self.assertFalse(actual.valid)
            self.assertGreater(actual.changed_source_count, 0)
            self.assertTrue(self.conn.in_transaction)
        finally:
            self.conn.rollback()

    def test_selector_error_is_not_reused_as_empty_or_success(self):
        original = packet_module._episode_rows
        calls = []

        def fail_first(*args):
            calls.append(args[2])
            if len(calls) == 1:
                raise sqlite3.OperationalError("neutral first selection failure")
            return original(*args)

        with mock.patch.object(packet_module, "_episode_rows", side_effect=fail_first):
            result = self.validate()
        self.assertEqual(len(calls), 2)
        self.assertEqual(result.status, "processing_error")
        self.assertEqual(result.processing_error_count, 1)
        self.assertEqual(result.changed_source_count, 0)
        self.assertTrue(self.validate().valid)

    def test_different_participant_keys_do_not_share_real_selector_results(self):
        changed = tuple(
            replace(item, source_type="participant_episode_gist", subject_key=f"discord_user:{7 + index}")
            for index, item in enumerate(self.episodes)
        )
        # Mixed or malformed caller packets must still evaluate each exact scope.
        value = replace(self.packet, items=changed, validation_items=changed)
        expected = self.scalar_result(value)
        with mock.patch.object(packet_module, "_episode_rows", wraps=packet_module._episode_rows) as reads:
            actual = self.validate(value)
        self.assertEqual(asdict(actual), asdict(expected))
        self.assertEqual([call.args[2] for call in reads.call_args_list], ["discord_user:7", "discord_user:8"])

    def test_each_component_owns_its_selection_even_with_same_participant_key(self):
        components = (self.packet, replace(self.packet, packet_id="neutral_second_component"))
        combined = replace(
            self.packet,
            items=packet_module._merge_packet_items(components),
            validation_items=packet_module._merge_packet_items(components, validation=True),
            subject_resolution=replace(self.packet.subject_resolution, status="multi_resolved"),
            subject_resolutions=tuple(c.subject_resolution for c in components),
            component_packets=components,
        )
        expected = self.scalar_result(combined)
        with mock.patch.object(packet_module, "_episode_rows", wraps=packet_module._episode_rows) as reads:
            actual = self.validate(combined)
        self.assertTrue(actual.valid)
        self.assertEqual(asdict(actual), asdict(expected))
        self.assertEqual(reads.call_count, 2)

    def test_implicit_clock_is_coherent_within_pass_then_expired_next_pass(self):
        value = replace(self.packet, request=replace(self.request, now=""))
        original = packet_module._episode_version
        with mock.patch.object(packet_module, "_now", return_value=self.request.now) as clock:
            def advance_after_item(*args, **kwargs):
                result = original(*args, **kwargs)
                clock.return_value = "2050-01-01T00:00:00+00:00"
                return result

            with mock.patch.object(packet_module, "_episode_version", side_effect=advance_after_item):
                with mock.patch.object(packet_module, "_episode_rows", wraps=packet_module._episode_rows) as reads:
                    first = self.validate(value)
                    second = self.validate(value)
        self.assertTrue(first.valid)
        self.assertFalse(second.valid)
        self.assertEqual(second.changed_source_count, 2)
        self.assertEqual(reads.call_count, 2)
        self.assertEqual(clock.call_count, 2)
        self.assertEqual(value.request.now, "")

    def test_explicit_clock_is_preserved_without_reading_current_clock(self):
        with mock.patch.object(packet_module, "_now", side_effect=AssertionError("explicit time must be preserved")):
            self.assertTrue(self.validate().valid)

    def test_direct_unpinned_validation_keeps_independent_scalar_reads(self):
        self.assertFalse(self.conn.in_transaction)
        with mock.patch.object(packet_module, "_episode_rows", wraps=packet_module._episode_rows) as reads:
            result = packet_module._revalidate_packet_in_snapshot(
                self.conn, self.packet, environ=self.fixture.flags,
            )
        self.assertTrue(result.valid)
        self.assertEqual(reads.call_count, 2)
        self.assertFalse(self.conn.in_transaction)


if __name__ == "__main__":
    unittest.main()
