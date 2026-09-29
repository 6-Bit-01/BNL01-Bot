"""Real rollback-journal contention must not leave a reply writer locked."""

import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl01_bot as bot


class ReplySQLiteContentionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = str(Path(self.tmp.name) / "reply.db")
        self.db_patch = mock.patch.object(bot, "DB_FILE", self.path)
        self.db_patch.start()
        self.addCleanup(self.db_patch.stop)
        bot.init_db()
        self.connect = sqlite3.connect
        self.connections = []
        self.addCleanup(self._close_connections)

    def _close_connections(self):
        for conn in self.connections:
            conn.close()

    def _short_connection(self, *args, **kwargs):
        kwargs["timeout"] = 0.02
        conn = self.connect(*args, **kwargs)
        self.connections.append(conn)
        return conn

    def _hold_read(self, table):
        conn = self.connect(self.path)
        self.connections.append(conn)
        conn.execute("BEGIN")
        conn.execute("SELECT * FROM " + table).fetchall()
        return conn

    def _assert_readable(self, table, expected_count=0):
        conn = self.connect(self.path, timeout=0.02)
        try:
            self.assertEqual(
                conn.execute("SELECT count(*) FROM " + table).fetchone()[0],
                expected_count,
            )
        finally:
            conn.close()

    def test_style_commit_failure_releases_pending_lock_before_reader_exits(self):
        reader = self._hold_read("response_style_log")
        retained_error = None
        with mock.patch.object(bot.sqlite3, "connect", self._short_connection):
            try:
                result = bot.log_response_style(77, 42, "steady_reply")
            except sqlite3.OperationalError as exc:
                # A failed asyncio task can retain this traceback indefinitely.
                retained_error = exc
                result = None
        self._assert_readable("response_style_log")
        self.assertIsNone(retained_error)
        self.assertIs(result, False)
        for conn in self.connections[1:]:
            with self.assertRaises(sqlite3.ProgrammingError):
                conn.execute("SELECT 1")
        reader.close()
        self.assertIs(bot.log_response_style(77, 42, "steady_reply"), True)
        self._assert_readable("response_style_log", 1)

    def test_model_commit_failure_closes_connection_without_hiding_failure(self):
        reader = self._hold_read("conversations")
        with mock.patch.object(bot.sqlite3, "connect", self._short_connection):
            with self.assertRaises(sqlite3.OperationalError) as retained:
                bot.save_model_message(
                    42, 77, "A delivered test reply.", channel_id=700,
                    channel_name="bnl-testing", channel_policy="sealed_test",
                )
        self.assertIsNotNone(retained.exception)
        self._assert_readable("conversations")
        for conn in self.connections[1:]:
            with self.assertRaises(sqlite3.ProgrammingError):
                conn.execute("SELECT 1")
        reader.close()
        writer = self.connect(self.path, timeout=0.02)
        try:
            writer.execute("BEGIN IMMEDIATE")
            writer.rollback()
        finally:
            writer.close()

    def test_model_commit_retries_after_reader_releases_without_duplicate_rows(self):
        reader = self._hold_read("conversations")
        with (
            mock.patch.object(bot.sqlite3, "connect", self._short_connection),
            mock.patch.object(bot.time, "sleep", side_effect=lambda _delay: reader.rollback()) as backoff,
            mock.patch.object(bot, "_shadow_memory_ledger_write") as shadow,
            mock.patch.object(bot, "relationship_v2_shadow_enabled", return_value=False),
        ):
            bot.save_model_message(
                42, 77, "A delivered private reply.", channel_id=700,
                channel_name="bnl-testing", channel_policy="sealed_test",
                discord_message_ids=(9001, 9002),
            )
        backoff.assert_called_once()
        shadow.assert_called_once()
        with self.connect(self.path) as conn:
            self.assertEqual(conn.execute(
                "SELECT user_id,channel_id,channel_policy,content FROM conversations"
            ).fetchall(), [(42, 700, "sealed_test", "A delivered private reply.")])
            self.assertEqual(conn.execute(
                "SELECT channel_id,message_id FROM conversation_discord_message_links ORDER BY message_id"
            ).fetchall(), [(700, 9001), (700, 9002)])

    def test_model_insert_retries_after_writer_releases(self):
        writer = self.connect(self.path)
        self.connections.append(writer)
        writer.execute("BEGIN EXCLUSIVE")
        with (
            mock.patch.object(bot.sqlite3, "connect", self._short_connection),
            mock.patch.object(bot.time, "sleep", side_effect=lambda _delay: writer.rollback()) as backoff,
            mock.patch.object(bot, "_shadow_memory_ledger_write"),
            mock.patch.object(bot, "relationship_v2_shadow_enabled", return_value=False),
        ):
            bot.save_model_message(42, 77, "Another delivered reply.",
                                   channel_id=700, channel_policy="sealed_test")
        backoff.assert_called_once()
        self._assert_readable("conversations", 1)

    def test_synthesis_receipt_retries_only_its_rolled_back_transaction(self):
        reader = self._hold_read("response_style_log")

        def finalize(conn, *_args, **_kwargs):
            conn.execute("INSERT INTO response_style_log (guild_id,user_id,style_key,timestamp) VALUES (77,42,'steady_reply','now')")
            return True

        with (
            mock.patch.object(bot.sqlite3, "connect", self._short_connection),
            mock.patch.object(bot.time, "sleep", side_effect=lambda _delay: reader.rollback()) as backoff,
            mock.patch.object(bot, "finalize_shared_brain_synthesis_run", side_effect=finalize) as finalize_run,
        ):
            result = bot._finalize_shared_brain_synthesis_receipt(
                object(), final_response="Already delivered.", response_sent=True,
                candidate_live=True, guard_status="sent",
            )
        self.assertTrue(result)
        backoff.assert_called_once()
        self.assertEqual(finalize_run.call_count, 2)
        self._assert_readable("response_style_log", 1)

    def test_non_lock_database_failure_is_not_retried(self):
        with (
            mock.patch.object(bot.sqlite3, "connect", self._short_connection),
            mock.patch.object(bot.time, "sleep") as backoff,
            mock.patch.object(bot, "finalize_shared_brain_synthesis_run",
                              side_effect=sqlite3.OperationalError("no such table: missing")),
        ):
            with self.assertRaisesRegex(sqlite3.OperationalError, "no such table"):
                bot._finalize_shared_brain_synthesis_receipt(
                    object(), final_response="Already delivered.", response_sent=True,
                    candidate_live=True, guard_status="sent",
                )
        backoff.assert_not_called()

    def test_style_history_lock_does_not_prevent_style_selection(self):
        writer = self.connect(self.path)
        self.connections.append(writer)
        writer.execute("BEGIN EXCLUSIVE")
        with mock.patch.object(bot.sqlite3, "connect", self._short_connection):
            self.assertEqual(bot.get_recent_response_styles(77, 42), [])
            style, rule = bot.choose_response_style(77, 42, 1, "Explain rhythm.")
        self.assertTrue(style)
        self.assertTrue(rule)
        writer.rollback()
        self._assert_readable("response_style_log")

    def test_successful_style_history_keeps_member_and_room_scope(self):
        for member, style in ((42, "steady_reply"), (0, "brief_ping"),
                              (99, "deep_focus")):
            bot.log_response_style(77, member, style)
        bot.log_response_style(88, 42, "analytic_mode")
        self.assertEqual(bot.get_recent_response_styles(77, 42),
                         ["brief_ping", "steady_reply"])
        self.assertEqual(bot.get_recent_response_styles(77, limit=1),
                         ["deep_focus"])

    def _packet(self, evidence_items=()):
        with mock.patch.dict(os.environ, {
            "BNL_MEMORY_LEDGER_SHADOW_ENABLED": "true",
            "BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED": "true",
            "BNL_MOMENT_ENGINE_SHADOW_ENABLED": "true",
            "BNL_RELATIONSHIP_V2_SHADOW_ENABLED": "true",
            "BNL_UNIFIED_INTELLIGENCE_PACKET_SHADOW_ENABLED": "true",
            "BNL_MEMORY_GOVERNANCE_LIVE_ENABLED": "false",
            "BNL_RELATIONSHIP_V2_LIVE_ENABLED": "false",
            "BNL_ACTIVE_ENGAGEMENT_V2_LIVE_ENABLED": "false",
        }):
            return bot._build_unified_intelligence_packet_shadow(
                guild_id=77, route_mode="normal_chat", channel_policy="sealed_test",
                conversation_surface="free_speak_sealed_mirror", channel_id=700,
                current_text="Explain rhythm.", current_speaker_user_ids=(42,),
                current_speaker_labels=("Test Member",), target_user_ids=(),
                participant_user_ids=(42,), conversation_evidence_items=evidence_items,
                source_context_snapshot="", source_context_authorized=False,
                operational_context_snapshot="", operational_context_authorized=False,
                current_direct=True,
            )

    def test_packet_commit_recovers_after_reader_releases_with_one_receipt(self):
        reader = self._hold_read("response_style_log")
        with (
            mock.patch.object(bot.sqlite3, "connect", self._short_connection),
            mock.patch.object(bot.time, "sleep", side_effect=lambda _delay: reader.rollback()) as backoff,
        ):
            packet = self._packet()
        self.assertIsNotNone(packet)
        backoff.assert_called_once()
        self.assertTrue(packet.diagnostics.receipt_run_id)
        with self.connect(self.path) as conn:
            rows = conn.execute("SELECT run_id FROM memory_governance_intelligence_packet_runs").fetchall()
        self.assertEqual(rows, [(packet.diagnostics.receipt_run_id,)])
        for conn in self.connections[1:]:
            with self.assertRaises(sqlite3.ProgrammingError):
                conn.execute("SELECT 1")

    def test_packet_retry_rebuilds_from_sources_after_a_concurrent_deletion(self):
        source_text = "The green lights follow the rhythm."
        with self.connect(self.path) as conn:
            conn.execute("""INSERT INTO conversations
                (id,user_id,user_name,guild_id,channel_id,channel_policy,route_mode,role,content,timestamp)
                VALUES (900,42,'Test Member',77,700,'sealed_test','normal_chat','user',?,'2026-09-29T12:00:00+00:00')""", (source_text,))
        evidence = bot.ConversationEvidenceItem(
            source_id=900, speaker_user_id=42, speaker_label="Test Member",
            text=source_text, current_turn=True, semantic_roles=(), option_anchors=(),
            criterion_positive_terms=(), criterion_negative_terms=(),
        )
        reader = self._hold_read("conversations")
        built = []
        real_build = bot.build_unified_intelligence_packet

        def capture(conn, request, **kwargs):
            result = real_build(conn, request, **kwargs)
            built.append(result)
            return result

        def withdraw(_delay):
            reader.rollback()
            with self.connect(self.path) as conn:
                conn.execute("DELETE FROM conversations WHERE id=900")

        with (
            mock.patch.object(bot.sqlite3, "connect", self._short_connection),
            mock.patch.object(bot.time, "sleep", side_effect=withdraw),
            mock.patch.object(bot, "build_unified_intelligence_packet", side_effect=capture),
        ):
            packet = self._packet((evidence,))
        self.assertEqual(len(built), 2)
        self.assertTrue(any(source_text in item.text for item in built[0].items))
        self.assertIs(packet, built[1])
        self.assertFalse(any(source_text in item.text for item in packet.items))
        self._assert_readable("memory_governance_intelligence_packet_runs", 1)

    def test_packet_read_recovers_after_exclusive_writer_releases(self):
        writer = self.connect(self.path)
        self.connections.append(writer)
        writer.execute("BEGIN EXCLUSIVE")
        with (
            mock.patch.object(bot.sqlite3, "connect", self._short_connection),
            mock.patch.object(bot.time, "sleep", side_effect=lambda _delay: writer.rollback()) as backoff,
        ):
            packet = self._packet()
        self.assertIsNotNone(packet)
        backoff.assert_called_once()
        self._assert_readable("memory_governance_intelligence_packet_runs", 1)

    def test_packet_exhaustion_stays_unavailable_without_retaining_pending_lock(self):
        self._hold_read("response_style_log")
        with (
            mock.patch.object(bot.sqlite3, "connect", self._short_connection),
            mock.patch.object(bot.time, "sleep") as backoff,
            self.assertLogs(level="WARNING") as logs,
        ):
            self.assertIsNone(self._packet())
        self.assertEqual(backoff.call_count, 2)
        self._assert_readable("memory_governance_intelligence_packet_runs")
        self.assertTrue(any("sqlite_busy=1" in line for line in logs.output))

    def test_packet_non_lock_failure_is_diagnosed_without_retry_or_error_content(self):
        detail = "no such table: private_fixture_detail"
        with (
            mock.patch.object(bot, "build_unified_intelligence_packet",
                              side_effect=sqlite3.OperationalError(detail)),
            mock.patch.object(bot.time, "sleep") as backoff,
            self.assertLogs(level="WARNING") as logs,
        ):
            self.assertIsNone(self._packet())
        backoff.assert_not_called()
        self.assertTrue(any("sqlite_busy=0" in line for line in logs.output))
        self.assertFalse(any("private_fixture_detail" in line for line in logs.output))

    def test_style_commit_recovers_after_reader_releases_without_duplicate(self):
        reader = self._hold_read("response_style_log")
        with (
            mock.patch.object(bot.sqlite3, "connect", self._short_connection),
            mock.patch.object(bot.time, "sleep", side_effect=lambda _delay: reader.rollback()) as backoff,
        ):
            self.assertTrue(bot.log_response_style(77, 42, "steady_reply"))
        backoff.assert_called_once()
        self._assert_readable("response_style_log", 1)


if __name__ == "__main__":
    unittest.main()
