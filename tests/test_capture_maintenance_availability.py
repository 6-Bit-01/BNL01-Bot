"""Committed source capture must survive optional retention contention."""

import sqlite3
import time
import unittest
from contextlib import closing
from unittest import mock

from tests import test_original_capture_transaction as capture_fixture
import bnl01_bot as bot


REAL_ADAPTIVE_LIMITS = bot.calculate_adaptive_memory_limits


class CaptureMaintenanceAvailabilityTests(unittest.TestCase):
    def setUp(self):
        self.capture = capture_fixture.OriginalCaptureTransactionTests()
        self.capture.setUp()
        self.addCleanup(self.capture.doCleanups)

    def original(self):
        return self.capture.rows(
            "SELECT id,channel_policy,message_id FROM conversations ORDER BY id"
        )

    def test_busy_retention_limits_do_not_abort_committed_public_source_or_later_facts(self):
        with mock.patch.object(
            bot, "calculate_adaptive_memory_limits",
            side_effect=sqlite3.OperationalError("database is locked"),
        ), mock.patch.object(
            bot, "extract_user_facts", return_value=[("genre", "synthwave", 0.9)]
        ), mock.patch.object(bot, "upsert_user_fact") as fact, \
                mock.patch.object(bot, "_persist_reply_transaction",
                                  wraps=bot._persist_reply_transaction) as persist:
            decision = self.capture.save()
        self.assertTrue(decision.save_conversation)
        rows = self.original()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][1:], ("public_home", 901))
        self.assertEqual(self.capture.rows(
            "SELECT conversation_row_id FROM memory_tier_conversation_sources"
        ), [(rows[0][0],)])
        persist.assert_called_once()
        self.capture.hooks["record_journal_source_event"].assert_called_once()
        self.capture.hooks["_shadow_memory_ledger_write"].assert_called_once()
        self.capture.hooks["prune_conversation_history"].assert_not_called()
        fact.assert_called_once()
        self.assertEqual(fact.call_args.kwargs["source_conversation_row_id"], rows[0][0])

    def test_busy_retention_limits_keep_private_original_and_lineage_isolated(self):
        with mock.patch.object(
            bot, "calculate_adaptive_memory_limits",
            side_effect=sqlite3.OperationalError("database is locked"),
        ), mock.patch.object(bot, "_persist_reply_transaction",
                             wraps=bot._persist_reply_transaction) as persist:
            decision = self.capture.save(
                channel_name="bnl-testing", channel_policy="sealed_test"
            )
        self.assertTrue(decision.save_conversation)
        rows = self.original()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][1:], ("sealed_test", 901))
        self.assertEqual(self.capture.rows(
            "SELECT conversation_row_id FROM memory_tier_conversation_sources"
        ), [(rows[0][0],)])
        self.assertEqual(self.capture.rows("SELECT source_trust FROM memory_tiers"),
                         [("sealed_test",)])
        persist.assert_called_once()
        self.capture.hooks["record_journal_source_event"].assert_not_called()
        self.capture.hooks["mark_subject_dirty_for_evidence"].assert_not_called()
        self.capture.hooks["_shadow_memory_ledger_write"].assert_called_once()

    def test_locked_prune_is_deferred_and_next_capture_uses_same_maintenance_owner(self):
        prune = self.capture.hooks["prune_conversation_history"]
        prune.side_effect = [sqlite3.OperationalError("database is locked"), None]
        with mock.patch.object(bot, "_persist_reply_transaction",
                               wraps=bot._persist_reply_transaction) as persist, \
                self.assertLogs(level="WARNING") as logged:
            self.assertTrue(self.capture.save().save_conversation)
            self.assertTrue(self.capture.save(message_id=902).save_conversation)
        self.assertEqual([row[2] for row in self.original()], [901, 902])
        self.assertEqual(persist.call_count, 2)
        self.assertEqual(prune.call_count, 2)
        self.assertEqual(self.capture.hooks["_consolidate_memory_tiers"].call_count, 2)
        self.assertEqual(sum("conversation_retention_deferred" in line
                             for line in logged.output), 1)

    def test_non_lock_maintenance_error_stays_visible_without_outer_capture_retry(self):
        error = sqlite3.OperationalError("no such table: fixture_unavailable")
        self.capture.hooks["prune_conversation_history"].side_effect = error
        with mock.patch.object(bot, "_persist_reply_transaction",
                               wraps=bot._persist_reply_transaction) as persist:
            with self.assertRaises(sqlite3.OperationalError) as caught:
                self.capture.save()
        self.assertIs(caught.exception, error)
        self.assertEqual(len(self.original()), 1)
        persist.assert_called_once()

    def test_actual_postcommit_sqlite_lock_defers_real_maintenance_read(self):
        observed_originals = []
        maintenance_failures = []
        maintenance_elapsed = []
        with closing(self.capture.connect(self.capture.path)) as blocker:
            def lock_after_original_commit(*_args, **_kwargs):
                # This independent connection sees both canonical records
                # before it takes an exclusive lock against the real reader.
                original = blocker.execute(
                    "SELECT id,message_id FROM conversations"
                ).fetchall()
                lineage = blocker.execute(
                    "SELECT conversation_row_id FROM memory_tier_conversation_sources"
                ).fetchall()
                self.assertEqual(len(original), 1)
                self.assertEqual(original[0][1], 901)
                self.assertEqual(lineage, [(original[0][0],)])
                observed_originals.extend(original)
                blocker.execute("BEGIN EXCLUSIVE")

            def read_real_limits(*args, **kwargs):
                started = time.monotonic()
                try:
                    return REAL_ADAPTIVE_LIMITS(*args, **kwargs)
                except sqlite3.OperationalError as exc:
                    maintenance_failures.append(exc)
                    raise
                finally:
                    maintenance_elapsed.append(time.monotonic() - started)

            self.capture.hooks["_shadow_stored_memory_tier"].side_effect = (
                lock_after_original_commit
            )
            with mock.patch.object(bot.sqlite3, "connect",
                                   side_effect=self.capture.tracked_connect), \
                    mock.patch.object(bot, "calculate_adaptive_memory_limits",
                                      side_effect=read_real_limits) as limits, \
                    mock.patch.object(bot, "extract_user_facts",
                                      return_value=[("genre", "synthwave", 0.9)]), \
                    mock.patch.object(bot, "upsert_user_fact") as fact, \
                    mock.patch.object(bot, "_persist_reply_transaction",
                                      wraps=bot._persist_reply_transaction) as persist, \
                    self.assertLogs(level="WARNING") as logged:
                try:
                    decision = self.capture.save()
                finally:
                    blocker.rollback()

        self.assertTrue(decision.save_conversation)
        self.assertEqual(len(maintenance_failures), 1)
        self.assertTrue(bot._sqlite_busy(maintenance_failures[0]))
        self.assertLess(maintenance_elapsed[0], 1)
        limits.assert_called_once()
        persist.assert_called_once()
        self.capture.assert_closed()
        rows = self.original()
        self.assertEqual(rows, [(observed_originals[0][0], "public_home", 901)])
        self.assertEqual(self.capture.rows(
            "SELECT conversation_row_id FROM memory_tier_conversation_sources"
        ), [(rows[0][0],)])
        self.capture.hooks["_consolidate_memory_tiers"].assert_not_called()
        self.capture.hooks["prune_conversation_history"].assert_not_called()
        fact.assert_called_once()
        self.assertEqual(fact.call_args.kwargs["source_conversation_row_id"], rows[0][0])
        self.assertEqual(sum("conversation_retention_deferred" in line
                             for line in logged.output), 1)


if __name__ == "__main__":
    unittest.main()
