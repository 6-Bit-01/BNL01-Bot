"""Mandatory capture retries only its atomic original and memory trace."""

import os
import sqlite3
import tempfile
import unittest
from contextlib import closing
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-token")

import bnl01_bot as bot


class CaptureCursor(sqlite3.Cursor):
    def execute(self, sql, parameters=()):
        if self.connection.fail_timestamp and sql.startswith(
            "SELECT timestamp FROM conversations WHERE id="
        ):
            raise sqlite3.OperationalError("fixture timestamp unavailable")
        return super().execute(sql, parameters)


class CaptureConnection(sqlite3.Connection):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.closed = False
        self.fail_timestamp = False
        self.statements = []
        self.set_trace_callback(self.statements.append)

    def cursor(self, *args, **kwargs):
        kwargs.setdefault("factory", CaptureCursor)
        return super().cursor(*args, **kwargs)

    def close(self):
        self.closed = True
        return super().close()


class OriginalCaptureTransactionTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = os.path.join(directory.name, "capture.db")
        self.connect = sqlite3.connect
        for patch in (
            mock.patch.object(bot, "DB_FILE", self.path),
            mock.patch.dict(os.environ, {
                "BNL_MEMORY_LEDGER_SHADOW_ENABLED": "0",
                "BNL_MOMENT_ENGINE_SHADOW_ENABLED": "0",
                "BNL_RELATIONSHIP_V2_SHADOW_ENABLED": "0",
            }),
        ):
            patch.start()
            self.addCleanup(patch.stop)
        bot.init_db()
        self.hooks = {}
        for name in (
            "record_journal_source_event", "_shadow_memory_ledger_write",
            "mark_subject_dirty_for_evidence", "update_relationship_state",
            "update_user_habits", "_shadow_stored_memory_tier",
            "_consolidate_memory_tiers", "prune_conversation_history",
            "add_relationship_journal",
        ):
            patch = mock.patch.object(bot, name)
            self.hooks[name] = patch.start()
            self.addCleanup(patch.stop)
        for patch in (
            mock.patch.object(bot, "relationship_v2_shadow_enabled", return_value=False),
            mock.patch.object(bot, "extract_user_facts", return_value=[]),
            mock.patch.object(bot, "calculate_adaptive_memory_limits", return_value={}),
        ):
            patch.start()
            self.addCleanup(patch.stop)
        self.attempts = []
        self.fail_timestamp = False

    def tracked_connect(self, *args, **kwargs):
        kwargs["timeout"] = 0.01
        kwargs["factory"] = CaptureConnection
        conn = self.connect(*args, **kwargs)
        conn.fail_timestamp = self.fail_timestamp
        self.attempts.append(conn)
        return conn

    def save(self, **overrides):
        parameters = dict(
            channel_name="BARCODE-BOT", channel_policy="public_home",
            channel_id=10, message_id=901, route_mode="normal_chat",
            directed_to_bnl=True,
        )
        parameters.update(overrides)
        return bot.save_user_message(
            42, "Test Member", 1,
            "Remember this amber synth arrangement for the upcoming album.",
            **parameters,
        )

    def rows(self, sql):
        with closing(self.connect(self.path)) as conn:
            return conn.execute(sql).fetchall()

    def assert_closed(self):
        for conn in self.attempts:
            self.assertTrue(conn.closed)
            with self.assertRaises(sqlite3.ProgrammingError):
                conn.execute("SELECT 1")

    def assert_no_capture_or_hooks(self):
        self.assertEqual(self.rows("SELECT id FROM conversations"), [])
        self.assertEqual(self.rows("SELECT id FROM memory_tiers"), [])
        self.assertEqual(self.rows("SELECT tier_row_id FROM memory_tier_conversation_sources"), [])
        for hook in self.hooks.values():
            hook.assert_not_called()

    def assert_one_capture_and_hooks(self):
        rows = self.rows(
            "SELECT id,user_id,user_name,guild_id,channel_name,channel_policy,"
            "channel_id,message_id,route_mode,role FROM conversations"
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][1:], (
            42, "Test Member", 1, "barcode-bot", "public_home", 10, 901,
            "normal_chat", "user",
        ))
        tiers = self.rows("SELECT id,source_channel_policy,source_trust FROM memory_tiers")
        self.assertEqual(len(tiers), 1)
        self.assertEqual(tiers[0][1:], ("public_home", "source_safe_public"))
        self.assertEqual(self.rows(
            "SELECT tier_row_id,conversation_row_id FROM memory_tier_conversation_sources"
        ), [(tiers[0][0], rows[0][0])])
        for name, hook in self.hooks.items():
            if name != "add_relationship_journal":
                hook.assert_called_once()
        self.hooks["add_relationship_journal"].assert_not_called()
        self.assertEqual(
            self.hooks["record_journal_source_event"].call_args.kwargs["metadata"]["conversationRowId"],
            rows[0][0],
        )

    def test_insert_lock_retries_closed_attempt_and_saves_once(self):
        with closing(self.connect(self.path)) as blocker:
            blocker.execute("BEGIN IMMEDIATE")
            with mock.patch.object(bot.sqlite3, "connect", side_effect=self.tracked_connect), \
                    mock.patch.object(bot.time, "sleep", side_effect=lambda _seconds: blocker.rollback()) as sleep:
                self.assertTrue(self.save().save_conversation)
        self.assertEqual(len(self.attempts), 2)
        sleep.assert_called_once_with(0.1)
        self.assert_closed()
        self.assert_one_capture_and_hooks()

    def test_commit_lock_rolls_back_original_and_trace_then_retries_once(self):
        with closing(self.connect(self.path)) as reader:
            reader.execute("BEGIN")
            reader.execute("SELECT COUNT(*) FROM conversations").fetchone()
            # A separate connection in this hook can see the saved original
            # only after the retry transaction has successfully committed.
            self.hooks["record_journal_source_event"].side_effect = (
                lambda *_args, **_kwargs: self.assertEqual(
                    len(self.rows("SELECT id FROM conversations")), 1
                )
            )
            with mock.patch.object(bot.sqlite3, "connect", side_effect=self.tracked_connect), \
                    mock.patch.object(bot.time, "sleep", side_effect=lambda _seconds: reader.rollback()) as sleep:
                self.assertTrue(self.save().save_conversation)
        self.assertEqual(len(self.attempts), 2)
        self.assertIn("COMMIT", self.attempts[0].statements)
        self.assertIn("ROLLBACK", self.attempts[0].statements)
        self.assertTrue(any(sql.startswith("INSERT INTO memory_tiers")
                            for sql in self.attempts[0].statements))
        sleep.assert_called_once_with(0.1)
        self.assert_closed()
        self.assert_one_capture_and_hooks()

    def test_exhausted_insert_lock_remains_visible_without_projection(self):
        with closing(self.connect(self.path)) as blocker:
            blocker.execute("BEGIN IMMEDIATE")
            with mock.patch.object(bot.sqlite3, "connect", side_effect=self.tracked_connect), \
                    mock.patch.object(bot.time, "sleep") as sleep:
                with self.assertRaisesRegex(sqlite3.OperationalError, "locked"):
                    self.save()
            blocker.rollback()
        self.assertEqual(len(self.attempts), 3)
        self.assertEqual([call.args for call in sleep.call_args_list], [(0.1,), (0.2,)])
        self.assert_closed()
        self.assert_no_capture_or_hooks()

    def test_timestamp_read_failure_rolls_back_closes_and_is_not_retried(self):
        self.fail_timestamp = True
        with mock.patch.object(bot.sqlite3, "connect", side_effect=self.tracked_connect), \
                mock.patch.object(bot.time, "sleep") as sleep:
            with self.assertRaisesRegex(sqlite3.OperationalError, "fixture timestamp unavailable"):
                self.save()
        self.assertEqual(len(self.attempts), 1)
        sleep.assert_not_called()
        self.assert_closed()
        self.assert_no_capture_or_hooks()

    def test_trace_failure_rolls_back_both_rows_and_is_not_retried(self):
        trace = bot.maybe_add_memory_trace
        def fail_after_trace(*args, **kwargs):
            self.assertTrue(trace(*args, **kwargs))
            raise sqlite3.OperationalError("fixture trace unavailable")
        with mock.patch.object(bot.sqlite3, "connect", side_effect=self.tracked_connect), \
                mock.patch.object(bot, "maybe_add_memory_trace", side_effect=fail_after_trace), \
                mock.patch.object(bot.time, "sleep") as sleep:
            with self.assertRaisesRegex(sqlite3.OperationalError, "fixture trace unavailable"):
                self.save()
        self.assertEqual(len(self.attempts), 1)
        sleep.assert_not_called()
        self.assert_closed()
        self.assert_no_capture_or_hooks()

    def test_invalid_observation_timestamp_closes_and_does_not_retry(self):
        with mock.patch.object(bot.sqlite3, "connect", side_effect=self.tracked_connect), \
                mock.patch.object(bot.time, "sleep") as sleep:
            with self.assertRaisesRegex(ValueError, "invalid_observation_timestamp"):
                self.save(source_observed_at="invalid timestamp")
        self.assertEqual(len(self.attempts), 1)
        sleep.assert_not_called()
        self.assert_closed()
        self.assert_no_capture_or_hooks()

    def test_private_capture_preserves_historical_time_and_stays_private(self):
        with mock.patch.object(bot.sqlite3, "connect", side_effect=self.tracked_connect):
            self.assertTrue(self.save(
                channel_policy="sealed_test", channel_name="BNL-TESTING",
                source_observed_at="2026-09-25T18:00:00Z",
            ).save_conversation)
        self.assert_closed()
        self.assertEqual(self.rows(
            "SELECT channel_name,channel_policy,channel_id,message_id,route_mode,timestamp FROM conversations"
        ), [("bnl-testing", "sealed_test", 10, 901, "normal_chat", "2026-09-25 18:00:00")])
        self.assertEqual(self.rows("SELECT source_trust FROM memory_tiers"), [("sealed_test",)])
        self.hooks["record_journal_source_event"].assert_not_called()
        self.hooks["mark_subject_dirty_for_evidence"].assert_not_called()
        self.hooks["_shadow_memory_ledger_write"].assert_called_once()


if __name__ == "__main__":
    unittest.main()
