from contextlib import closing
import gc
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import bnl_journal as journal
import bnl_journal_automation as automation
import bnl_journal_source_store as source_store


START = "2026-09-30T01:30:00Z"
END = "2026-10-01T01:30:00Z"
OBSERVED = "2026-09-30T16:25:45Z"
TEXT = "A public discussion about an unfinished melody."


class JournalCorrectionSourceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.cleanup)
        self.db = str(Path(self.temp.name) / "journal.db")
        journal.ensure_schema(self.db)
        source_store.ensure_schema(self.db)
        with closing(sqlite3.connect(self.db)) as conn, conn:
            conn.execute("""CREATE TABLE conversations(
                id INTEGER PRIMARY KEY,guild_id INTEGER,user_id INTEGER,user_name TEXT,
                role TEXT,content TEXT,channel_policy TEXT,channel_id INTEGER,
                public_usable INTEGER,visibility TEXT,timestamp TEXT)""")
            conn.execute("INSERT INTO conversations VALUES(10,1,7,'Test Listener','user',?,'public_home',100,1,'public',?)",
                         (TEXT, OBSERVED))
        self.event = self.record("source-10", OBSERVED, row_id=10)
        self.packet = journal.build_source_packet_between(self.db, 1, START, END, prepare_schema=False)

    def cleanup(self):
        gc.collect()
        self.temp.cleanup()

    def record(self, key, observed, row_id=None, **overrides):
        values = dict(guild_id=1, source_kind="discord_message", source_key=key,
                      occurred_at_ms=source_store.timestamp_to_epoch_ms(observed),
                      raw_text=TEXT, sanitized_summary=TEXT, channel_id=100,
                      channel_policy="public_home", subject_ref="discord_user:7",
                      private_display_name="Test Listener", public_usable=True,
                      metadata={"conversationRowId": row_id} if row_id else {})
        values.update(overrides)
        result = source_store.record_source_event(self.db, **values)
        self.assertTrue(result.ok)
        return result

    def mutate(self, assignments, args=()):
        with closing(sqlite3.connect(self.db)) as conn, conn:
            conn.execute("UPDATE conversations SET " + assignments + " WHERE id=10", args)

    def guard(self, packet=None, controls=None):
        return automation._generation_guard_for_packet(
            self.db, 1, packet or self.packet, validate_original_sources=True,
            original_source_controls=controls,
        )

    def test_current_original_is_valid_and_check_is_read_only(self):
        before = Path(self.db).read_bytes()
        guard = self.guard()
        self.assertEqual("", guard())
        self.assertEqual("", guard())
        self.assertEqual(before, Path(self.db).read_bytes())

    def test_edit_after_first_check_blocks_the_same_correction(self):
        guard = self.guard()
        self.assertEqual("", guard())
        self.mutate("content=?", ("That was a question, not a finished release.",))
        self.assertEqual("journal_original_source_changed", guard())

    def test_known_private_policy_visibility_or_use_withdrawal_blocks(self):
        for assignment in ("channel_policy='sealed_test'", "channel_policy='internal_controlled'",
                           "public_usable=0", "visibility='private'"):
            with self.subTest(assignment=assignment):
                self.mutate("channel_policy='public_home',public_usable=1,visibility='public'")
                self.mutate(assignment)
                self.assertEqual("privacy_source_ineligible", self.guard()())

    def test_author_guild_or_role_rebinding_cannot_look_like_retention(self):
        for assignment in ("user_id=8", "guild_id=2", "role='model'"):
            with self.subTest(assignment=assignment):
                self.mutate("user_id=7,guild_id=1,role='user'")
                self.mutate(assignment)
                self.assertEqual("privacy_source_ineligible", self.guard()())

    def test_room_public_policy_and_observation_changes_are_detected(self):
        for assignment in ("channel_id=101", "channel_policy='public_context'",
                           "timestamp='2026-09-30T18:25:45Z'"):
            with self.subTest(assignment=assignment):
                self.mutate("channel_id=100,channel_policy='public_home',timestamp=?", (OBSERVED,))
                self.mutate(assignment)
                self.assertEqual("journal_original_source_changed", self.guard()())

    def test_retention_pruned_conversation_keeps_eligible_archive(self):
        guard = self.guard()
        with closing(sqlite3.connect(self.db)) as conn, conn:
            conn.execute("DELETE FROM conversations WHERE id=10")
        self.assertEqual("", guard())

    def test_archive_purge_blocks_even_when_conversation_still_exists(self):
        guard = self.guard()
        source_store.purge_user_discord_sources(self.db, 1, 7)
        self.assertEqual("privacy_source_ineligible", guard())

    def test_existing_governance_owner_applies_even_after_retention(self):
        controls = Mock(return_value=("current-controls", frozenset()))
        with closing(sqlite3.connect(self.db)) as conn, conn:
            conn.execute("CREATE TABLE memory_ledger_entries(entry_id TEXT)")
            conn.execute("DELETE FROM conversations WHERE id=10")
        guard = self.guard(controls=controls)
        self.assertEqual("", guard())
        self.assertEqual({"guild_id": 1, "source_users": {10: 7}, "source_table": "conversations"},
                         controls.call_args.kwargs)
        controls.return_value = ("corrected-controls", frozenset({10}))
        self.assertEqual("privacy_source_ineligible", guard())

    def test_existing_controls_without_owner_callback_fail_closed(self):
        with closing(sqlite3.connect(self.db)) as conn, conn:
            conn.execute("CREATE TABLE memory_ledger_entries(entry_id TEXT)")
        self.assertEqual("journal_original_controls_unavailable", self.guard()())

    def test_final_transaction_reuses_the_same_guard_without_another_connection(self):
        with closing(sqlite3.connect(self.db)) as conn, conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute("UPDATE conversations SET visibility='private' WHERE id=10")
            self.assertEqual("privacy_source_ineligible", automation._frozen_packet_invalidation_reason(
                conn, 1, self.packet, validate_original_sources=True))
            conn.rollback()
        self.assertEqual("", self.guard()())

    def test_historical_reflection_original_is_checked_too(self):
        old = "2026-09-29T16:25:45Z"
        with closing(sqlite3.connect(self.db)) as conn, conn:
            conn.execute("INSERT INTO conversations VALUES(11,1,7,'Test Listener','user',?,'public_home',100,1,'public',?)",
                         (TEXT, old))
        event = self.record("older-11", old, row_id=11)
        packet = journal.build_source_packet_between(self.db, 1, START, END, prepare_schema=False)
        self.assertIn(f"reflection:event:{event.event_seq}", {source["refId"] for source in packet["reflectionBasis"]})
        guard = self.guard(packet)
        self.assertEqual("", guard())
        with closing(sqlite3.connect(self.db)) as conn, conn:
            conn.execute("UPDATE conversations SET content='Revised earlier contribution.' WHERE id=11")
        self.assertEqual("journal_original_source_changed", guard())

    def test_legacy_row_key_binds_but_discord_message_id_is_not_a_row_id(self):
        source_store.purge_user_discord_sources(self.db, 1, 7)
        self.record("legacy_row:10", OBSERVED)
        packet = journal.build_source_packet_between(self.db, 1, START, END, prepare_schema=False)
        self.mutate("content='Changed original.'")
        self.assertEqual("journal_original_source_changed", self.guard(packet)())
        source_store.purge_user_discord_sources(self.db, 1, 7)
        self.record("10", OBSERVED, metadata={"messageId": 10})
        packet = journal.build_source_packet_between(self.db, 1, START, END, prepare_schema=False)
        self.assertEqual("", self.guard(packet)())

    def test_new_check_is_opt_in_and_does_not_change_normal_generation(self):
        self.mutate("content='Changed original.'")
        normal = automation._generation_guard_for_packet(self.db, 1, self.packet)
        self.assertEqual("", normal())
        self.assertEqual("journal_original_source_changed", self.guard()())


class JournalCorrectionRoutingTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.cleanup)
        self.db = str(Path(self.temp.name) / "journal.db")
        journal.ensure_schema(self.db)
        automation.ensure_schema(self.db)
        self.context = {
            "correction": {"previousRevision": 1, "previousContentHash": "published-hash", "note": "Clarify the source."},
            "originalPublishedAt": "2026-10-01T02:00:00Z",
            "sourceWindowStart": START, "sourceWindowEnd": END, "entryKind": "daily",
        }
        with closing(sqlite3.connect(self.db)) as conn, conn:
            conn.execute("""INSERT INTO bnl_journal_entries(
                entry_id,revision,guild_id,lifecycle_state,title,excerpt,sections_json,content_hash,
                source_window_start,source_window_end,authored_at,published_at,created_at,updated_at)
                VALUES('journal-test',1,1,'published','Original title','An account.','[]','published-hash',?,?,?,?,?,?)""",
                (START, END, END, self.context["originalPublishedAt"], END, END))
            conn.execute("UPDATE bnl_journal_entries SET public_payload_json=? WHERE revision=1",
                         (json.dumps({"entry": {"entryKind": "daily"}}),))
            conn.execute("""INSERT INTO bnl_journal_automation_runs(
                run_id,guild_id,cadence,source_window_start,source_window_end,lifecycle_state,
                journal_entry_id,journal_revision,created_at,updated_at)
                VALUES('original-run',1,'daily',?,?,'published','journal-test',1,?,?)""", (START, END, END, END))

    def cleanup(self):
        gc.collect()
        self.temp.cleanup()

    def candidate(self, *, metadata=None, state="approved_pending_delivery"):
        metadata = {"publishedCorrection": self.context} if metadata is None else metadata
        with closing(sqlite3.connect(self.db)) as conn, conn:
            conn.execute("""INSERT INTO bnl_journal_entries(
                entry_id,revision,guild_id,lifecycle_state,title,excerpt,sections_json,content_hash,
                source_window_start,source_window_end,authored_at,created_at,updated_at)
                VALUES('journal-test',2,1,?,'Corrected title','A clarified account.','[]','candidate-hash',?,?,?,?,?)""",
                (state, START, END, END, END, END))
            conn.execute("""INSERT INTO bnl_journal_private_metadata VALUES(
                'journal-test',2,1,?,'candidate-hash',?,?,?)""", (json.dumps(metadata), state, END, END))

    def run_row(self):
        with closing(sqlite3.connect(self.db)) as conn:
            return conn.execute("SELECT * FROM bnl_journal_automation_runs WHERE run_id='original-run'").fetchone()

    def test_reviewed_correction_routes_to_explicit_delivery_without_reopening_schedule(self):
        self.candidate()
        before = self.run_row()
        with patch.object(automation, "release_occurrence") as release:
            result = automation.release_prepared_entry(self.db, 1, "journal-test", "unused", "unused", force=True)
        self.assertIsNone(result)
        release.assert_not_called()
        self.assertEqual(before, self.run_row())
        with closing(sqlite3.connect(self.db)) as conn:
            self.assertEqual("approved_pending_delivery", conn.execute(
                "SELECT lifecycle_state FROM bnl_journal_entries WHERE revision=2").fetchone()[0])

    def test_unapproved_correction_does_not_get_automatic_scheduled_release(self):
        self.candidate(state="draft")
        before = self.run_row()
        with patch.object(automation, "release_occurrence") as release:
            self.assertIsNone(automation.release_prepared_entry(self.db, 1, "journal-test", "unused", "unused"))
        release.assert_not_called()
        self.assertEqual(before, self.run_row())

    def test_published_correction_retry_reports_its_revision_without_old_dispatch(self):
        self.candidate(state="published")
        before = self.run_row()
        with patch.object(automation, "release_occurrence") as release:
            result = automation.release_prepared_entry(self.db, 1, "journal-test", "unused", "unused")
        self.assertTrue(result.ok)
        self.assertTrue(result.idempotent)
        self.assertEqual("already_published", result.reason)
        self.assertEqual(2, result.revision)
        self.assertEqual(START, result.source_window_start)
        self.assertEqual(END, result.source_window_end)
        release.assert_not_called()
        self.assertEqual(before, self.run_row())

    def test_unmarked_draft_cannot_bypass_the_scheduled_owner(self):
        self.candidate(metadata={})
        expected = automation.AutomationResult(True, "daily", "published", "already_published", "journal-test", 1)
        with patch.object(automation, "release_occurrence", return_value=expected) as release:
            self.assertIs(expected, automation.release_prepared_entry(self.db, 1, "journal-test", "unused", "unused"))
        release.assert_called_once()

    def test_invalid_correction_marker_holds_without_dispatching_predecessor(self):
        self.context["correction"]["previousContentHash"] = "not-the-published-base"
        self.candidate()
        before = self.run_row()
        with patch.object(automation, "release_occurrence") as release:
            result = automation.release_prepared_entry(self.db, 1, "journal-test", "unused", "unused")
        self.assertFalse(result.ok)
        self.assertEqual("correction_base_changed", result.reason)
        release.assert_not_called()
        self.assertEqual(before, self.run_row())


if __name__ == "__main__":
    unittest.main()
