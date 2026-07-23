import os
import sqlite3
import tempfile
import unittest

os.environ.setdefault("GEMINI_API_KEY", "test-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-token")

import bnl_journal as journal
import bnl_journal_automation as automation
import bnl_journal_source_store as source_store


class JournalFreezeFenceTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.NamedTemporaryFile(delete=False)
        self.db = tmp.name
        tmp.close()
        journal.ensure_schema(self.db)
        automation.ensure_schema(self.db)
        source_store.ensure_schema(self.db)
        recorded = source_store.record_source_event(
            self.db,
            guild_id=1,
            source_kind="discord_message",
            source_key="message:100",
            occurred_at_ms=1_000,
            raw_text="A public-safe source that may later be deleted.",
            sanitized_summary="A public-safe source that may later be deleted.",
            channel_id=10,
            channel_policy="public_home",
            subject_ref="discord_user:42",
            private_display_name="Member",
            public_usable=True,
        )
        self.event_seq = recorded.event_seq
        self.start = "2026-07-20T02:00:00Z"
        self.end = "2026-07-21T02:00:00Z"

    def tearDown(self):
        try:
            os.unlink(self.db)
        except FileNotFoundError:
            pass

    def claim(self):
        claim, run_id, epoch, _row = automation._claim_preparation(
            self.db,
            1,
            "daily",
            self.start,
            self.end,
            force=True,
        )
        self.assertEqual("claimed", claim)
        return run_id, epoch

    def packet(self, **updates):
        source = {
            "refId": f"fresh:{self.event_seq}",
            "sourceKind": "conversation",
            "subjectRef": "discord_user:42",
            "summary": "A public-safe source that may later be deleted.",
        }
        packet = {
            "entryKind": "daily",
            "sourceArchiveAvailable": True,
            "coverageComplete": True,
            "sourceWindowStart": self.start,
            "sourceWindowEnd": self.end,
            "aggregateCounts": {"eligibleConversations": 1},
            "privateSources": [source],
            "safeSources": [dict(source)],
            "privateContextLaneProvenance": {},
        }
        packet.update(updates)
        return packet

    def frozen_fields(self, run_id):
        with sqlite3.connect(self.db) as conn:
            return conn.execute(
                "SELECT frozen_packet_json,frozen_packet_hash,packet_frozen_at "
                "FROM bnl_journal_automation_runs WHERE run_id=?",
                (run_id,),
            ).fetchone()

    def observation_count(self):
        with sqlite3.connect(self.db) as conn:
            return int(conn.execute("SELECT COUNT(*) FROM bnl_journal_observations").fetchone()[0])

    def test_builder_deletion_race_cannot_freeze_deleted_source(self):
        run_id, epoch = self.claim()

        def build_after_deletion():
            self.assertEqual(1, source_store.purge_user_discord_sources(self.db, 1, 42))
            return self.packet()

        packet, packet_hash, reason = automation._freeze_or_load_packet(
            self.db,
            1,
            run_id,
            epoch,
            build_after_deletion,
        )

        self.assertIsNone(packet)
        self.assertEqual("", packet_hash)
        self.assertEqual("privacy_source_ineligible", reason)
        self.assertEqual((None, None, None), self.frozen_fields(run_id))

    def test_recovery_clears_packet_whose_source_was_deleted(self):
        run_id, epoch = self.claim()
        packet = self.packet()
        frozen, packet_hash, reason = automation._freeze_or_load_packet(
            self.db, 1, run_id, epoch, lambda: packet
        )
        self.assertEqual(packet, frozen)
        self.assertTrue(packet_hash)
        self.assertEqual("", reason)
        self.assertEqual(1, source_store.purge_user_discord_sources(self.db, 1, 42))

        builder_called = False

        def should_not_build():
            nonlocal builder_called
            builder_called = True
            return packet

        recovered, recovered_hash, recovered_reason = automation._freeze_or_load_packet(
            self.db, 1, run_id, epoch, should_not_build
        )

        self.assertIsNone(recovered)
        self.assertEqual("", recovered_hash)
        self.assertEqual("privacy_source_ineligible", recovered_reason)
        self.assertFalse(builder_called)
        self.assertEqual((None, None, None), self.frozen_fields(run_id))

    def test_superseded_worker_cannot_insert_observation(self):
        run_id, epoch = self.claim()
        packet = self.packet()
        automation._freeze_or_load_packet(self.db, 1, run_id, epoch, lambda: packet)
        with sqlite3.connect(self.db) as conn:
            conn.execute(
                "UPDATE bnl_journal_automation_runs SET lifecycle_state='held',"
                "preparation_epoch=preparation_epoch+1,lease_expires_at=NULL WHERE run_id=?",
                (run_id,),
            )

        observation_id, reason = automation._store_observation(
            self.db,
            1,
            "2026-07-20",
            packet,
            run_id=run_id,
            preparation_epoch=epoch,
        )

        self.assertEqual("", observation_id)
        self.assertEqual("preparation_epoch_superseded", reason)
        self.assertEqual(0, self.observation_count())

    def test_deletion_before_observation_blocks_insert_and_clears_packet(self):
        run_id, epoch = self.claim()
        packet = self.packet()
        automation._freeze_or_load_packet(self.db, 1, run_id, epoch, lambda: packet)
        self.assertEqual(1, source_store.purge_user_discord_sources(self.db, 1, 42))

        observation_id, reason = automation._store_observation(
            self.db,
            1,
            "2026-07-20",
            packet,
            run_id=run_id,
            preparation_epoch=epoch,
        )

        self.assertEqual("", observation_id)
        self.assertEqual("privacy_source_ineligible", reason)
        self.assertEqual(0, self.observation_count())
        self.assertEqual((None, None, None), self.frozen_fields(run_id))

    def test_transient_ineligible_packet_is_not_frozen(self):
        run_id, epoch = self.claim()
        packet = self.packet(sourceArchiveAvailable=False)

        returned, packet_hash, reason = automation._freeze_or_load_packet(
            self.db, 1, run_id, epoch, lambda: packet
        )

        self.assertEqual(packet, returned)
        self.assertEqual("", packet_hash)
        self.assertEqual("source_packet_ineligible", reason)
        self.assertEqual((None, None, None), self.frozen_fields(run_id))


if __name__ == "__main__":
    unittest.main()
