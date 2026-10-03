"""A completed privacy action cannot be followed by late source projections."""

import os
import sqlite3
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, closing, contextmanager
from pathlib import Path
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-token")

import bnl01_bot as bot
import bnl_memory_governance as governance


@unittest.skipIf(os.name == "nt", "Requires the existing native POSIX privacy fence")
class SourceCapturePrivacyOrderingTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = str(Path(directory.name) / "capture.db")
        self.connect = sqlite3.connect
        self.connections = []
        self.connection_lock = threading.Lock()
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        # Existing schema/clear fixtures have transaction-context handles.
        # Track their test ownership so cleanup does not rely on collection.
        self.addCleanup(self.close_connections)
        self.stack.enter_context(mock.patch.object(bot, "DB_FILE", self.path))
        self.stack.enter_context(mock.patch.dict(os.environ, {
            "BNL_MEMORY_LEDGER_SHADOW_ENABLED": "1",
            "BNL_MOMENT_ENGINE_SHADOW_ENABLED": "0",
            "BNL_RELATIONSHIP_V2_SHADOW_ENABLED": "1",
            "BNL_RELATIONSHIP_V2_MEANING_SHADOW_ENABLED": "0",
            "BNL_CONVERSATION_MOTIF_FORMATION_SHADOW_ENABLED": "0",
            "BNL_LIVING_CANON_V1_FORMATION_SHADOW_ENABLED": "0",
        }))
        self.stack.enter_context(mock.patch.object(
            bot.sqlite3, "connect", side_effect=self.tracked_connect))
        self.stack.enter_context(mock.patch.object(bot, "mark_subject_dirty_for_evidence"))
        bot.init_db()

    def tracked_connect(self, *args, **kwargs):
        # Workers use their own connections; only final fixture cleanup may
        # close their already-idle handles from the test thread.
        kwargs["check_same_thread"] = False
        conn = self.connect(*args, **kwargs)
        with self.connection_lock:
            self.connections.append(conn)
        return conn

    def close_connections(self):
        for conn in self.connections:
            conn.close()

    def count(self, table, where="", params=()):
        with closing(self.connect(self.path)) as conn:
            return conn.execute("SELECT COUNT(*) FROM " + table + where, params).fetchone()[0]

    def save(self, user=42):
        return bot.save_user_message(
            user, "Test Member", 1,
            "Thanks BNL for helping me learn this amber synth pattern.",
            channel_name="community", channel_policy="public_home", channel_id=10,
            message_id=900 + user, route_mode="normal_chat", directed_to_bnl=True,
        )

    def capture_then_delete(self, *, complete):
        # An unrelated member remains intact under either privacy operation.
        self.save(user=43)
        paused, release, delete_at_fence, delete_finished = (
            threading.Event() for _ in range(4))
        archive = bot.record_journal_source_event
        fence = bot.journal_release_privacy_fence
        archive_calls = []
        delete_thread = []

        @contextmanager
        def observed_fence(*args, **kwargs):
            if delete_thread and threading.get_ident() == delete_thread[0]:
                delete_at_fence.set()
            with fence(*args, **kwargs) as acquired:
                yield acquired

        def pause_after_original_commit(*args, **kwargs):
            archive_calls.append(kwargs["metadata"]["conversationRowId"])
            self.assertEqual(self.count(
                "conversations", " WHERE guild_id=1 AND user_id=42"), 1)
            self.assertEqual(self.count(
                "memory_tier_conversation_sources", " WHERE guild_id=1 AND conversation_row_id=?",
                (archive_calls[-1],)), 1)
            paused.set()
            if not release.wait(5):
                raise AssertionError("capture privacy race was not released")
            return archive(*args, **kwargs)

        def delete():
            delete_thread.append(threading.get_ident())
            try:
                if complete:
                    return bot._complete_delete_member_data_sync(
                        1, 42, "DELETE MY BNL DATA 1")
                return bot.clear_user_history(42, 1)
            finally:
                delete_finished.set()

        with mock.patch.object(bot, "record_journal_source_event", side_effect=pause_after_original_commit), \
                mock.patch.object(bot, "journal_release_privacy_fence", new=observed_fence), \
                mock.patch.object(governance, "journal_release_privacy_fence", new=observed_fence), \
                mock.patch.object(bot, "_persist_reply_transaction",
                                  wraps=bot._persist_reply_transaction) as persist, \
                ThreadPoolExecutor(max_workers=2) as workers:
            capture_future = workers.submit(self.save)
            delete_future = None
            try:
                self.assertTrue(paused.wait(3), "original capture did not reach its committed boundary")
                delete_future = workers.submit(delete)
                self.assertTrue(delete_at_fence.wait(3), "deletion did not reach the privacy fence")
                # Deletion must wait for all projections belonging to the
                # committed source, rather than removing only an early subset.
                self.assertFalse(delete_finished.wait(0.2))
            finally:
                release.set()
            self.assertTrue(capture_future.result(timeout=5).save_conversation)
            result = delete_future.result(timeout=5)
            self.assertTrue(result["ok"] if complete else result == 1)
            self.assertEqual(persist.call_count, 1)
        self.assertEqual(len(archive_calls), 1)
        self.assertTrue(delete_finished.is_set())
        self.assertEqual(self.count("conversations", " WHERE guild_id=1 AND user_id=42"), 0)
        self.assertEqual(self.count("memory_tiers", " WHERE guild_id=1 AND user_id=42"), 0)
        self.assertEqual(self.count(
            "bnl_journal_source_events", " WHERE guild_id=1 AND subject_ref='discord_user:42'"), 0)
        self.assertEqual(self.count(
            "memory_ledger_entries", " WHERE guild_id=1 AND source_table='conversations' AND source_row_id=?",
            (str(archive_calls[0]),)), 0)
        self.assertEqual(self.count("conversations", " WHERE guild_id=1 AND user_id=43"), 1)
        if complete:
            for table in ("relationship_events_v2", "relationship_state_v2",
                          "relationship_observation_diagnostics_v2"):
                self.assertEqual(self.count(table, " WHERE guild_id=1 AND subject_user_id=42"), 0)
            self.assertGreater(self.count(
                "relationship_events_v2", " WHERE guild_id=1 AND subject_user_id=43"), 0)

    def test_clear_waits_for_original_archive_and_ledger_capture_to_finish(self):
        self.capture_then_delete(complete=False)

    def test_complete_delete_waits_for_relationship_and_all_source_capture_to_finish(self):
        self.capture_then_delete(complete=True)

    def test_failed_capture_releases_privacy_fence_without_retrying_outer_owner(self):
        failure = sqlite3.DatabaseError("fixture capture unavailable")
        with mock.patch.object(bot, "_persist_reply_transaction", side_effect=failure) as persist:
            with self.assertRaises(sqlite3.DatabaseError) as caught:
                self.save()
        self.assertIs(caught.exception, failure)
        persist.assert_called_once()
        with ThreadPoolExecutor(max_workers=1) as worker:
            self.assertEqual(worker.submit(bot.clear_user_history, 42, 1).result(timeout=3), 0)


if __name__ == "__main__":
    unittest.main()
