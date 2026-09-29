"""Review retries reuse the generated reply and recheck current evidence."""

import os
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl01_bot as bot
import test_ordinary_chat_single_packet_canary as packet_fixture


class ReviewSQLiteContentionTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.fixture = packet_fixture.OrdinaryChatSinglePacketCanaryTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        self.run = self.fixture._begin()
        self.fixture.conn.commit()
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = str(Path(directory.name) / "review.db")
        with closing(sqlite3.connect(self.path)) as conn:
            self.fixture.conn.backup(conn)
        patch = mock.patch.object(bot, "DB_FILE", self.path)
        patch.start()
        self.addCleanup(patch.stop)
        self.connect = sqlite3.connect
        self.connections = []
        self.addCleanup(lambda: [conn.close() for conn in self.connections])

    def _short_connection(self, *args, **kwargs):
        kwargs.update(timeout=0.02, check_same_thread=False)
        conn = self.connect(*args, **kwargs)
        self.connections.append(conn)
        return conn

    def _reader(self):
        conn = self.connect(self.path, check_same_thread=False)
        self.connections.append(conn)
        conn.execute("BEGIN")
        conn.execute("SELECT * FROM memory_governance_shared_brain_synthesis_runs").fetchall()
        return conn

    def _evaluate(self):
        return bot._evaluate_ordinary_chat_single_packet_receipt(
            self.run, response="Your favorite movie is Arrival.",
            provider_call_count=1, corrective_call_count=0,
            generation_latency_ms=12, total_tokens=321,
            prompt_tokens=200, output_tokens=100, thought_tokens=21,
            estimated_cost_nanos=123456, cost_priced=True,
        )

    def _receipt(self, run_id=None):
        with closing(self.connect(self.path, timeout=0.02)) as conn:
            return conn.execute("""SELECT candidate_selected,provider_call_count,
                corrective_call_count,candidate_total_tokens,candidate_estimated_cost_nanos,
                source_revalidation_status,processing_error_count
                FROM memory_governance_shared_brain_synthesis_runs WHERE run_id=?""",
                (run_id or self.run.run_id,)).fetchone()

    def test_begin_retries_without_duplicate_receipts(self):
        reader = self._reader()
        with (
            mock.patch.object(bot.sqlite3, "connect", self._short_connection),
            mock.patch.object(bot.time, "sleep", side_effect=lambda _: reader.rollback()) as backoff,
        ):
            run = bot._begin_ordinary_chat_single_packet_receipt(
                self.fixture.basis, prompt_ready=True, prompt_failure_reason="",
                frame_revalidation_status="valid",
            )
        self.assertTrue(run.prompt_applied)
        backoff.assert_called_once()
        with closing(self.connect(self.path)) as conn:
            self.assertEqual(conn.execute(
                "SELECT run_id FROM memory_governance_shared_brain_synthesis_runs ORDER BY run_id"
            ).fetchall(), sorted([(self.run.run_id,), (run.run_id,)]))

    def test_evaluation_retries_commit_with_one_accounted_generation(self):
        reader = self._reader()
        with (
            mock.patch.object(bot.sqlite3, "connect", self._short_connection),
            mock.patch.object(bot.time, "sleep", side_effect=lambda _: reader.rollback()) as backoff,
        ):
            decision = self._evaluate()
        self.assertTrue(decision.candidate_selected)
        backoff.assert_called_once()
        self.assertEqual(self._receipt(), (1, 1, 0, 321, 123456, "passed", 0))
        for conn in self.connections[1:]:
            with self.assertRaisesRegex(sqlite3.ProgrammingError, "closed"):
                conn.execute("SELECT 1")

    def test_evaluation_retries_after_exclusive_writer_releases(self):
        writer = self.connect(self.path)
        self.connections.append(writer)
        writer.execute("BEGIN EXCLUSIVE")
        with (
            mock.patch.object(bot.sqlite3, "connect", self._short_connection),
            mock.patch.object(bot.time, "sleep", side_effect=lambda _: writer.rollback()) as backoff,
        ):
            decision = self._evaluate()
        backoff.assert_called_once()
        self.assertTrue(decision.candidate_selected)
        self.assertEqual(self._receipt(), (1, 1, 0, 321, 123456, "passed", 0))

    def test_retry_revalidates_source_deletion_instead_of_reusing_approval(self):
        reader = self._reader()

        def withdraw(_delay):
            reader.rollback()
            with closing(self.connect(self.path)) as conn, conn:
                conn.execute("DELETE FROM conversations WHERE id=900")

        with (
            mock.patch.object(bot.sqlite3, "connect", self._short_connection),
            mock.patch.object(bot.time, "sleep", side_effect=withdraw) as backoff,
            mock.patch.object(bot, "_shared_brain_journal_revalidation_snapshot",
                              wraps=bot._shared_brain_journal_revalidation_snapshot) as snapshots,
        ):
            decision = self._evaluate()
        backoff.assert_called_once()
        self.assertEqual(snapshots.call_count, 2)
        self.assertFalse(decision.candidate_selected)
        self.assertTrue(decision.fallback_reason.startswith("post_generation_"))
        self.assertNotEqual(self._receipt()[5], "passed")
        self.assertEqual(self._receipt()[1:5], (1, 0, 321, 123456))

    def test_review_retry_rolls_back_error_increment_and_keeps_call_counts(self):
        decision = self._evaluate()
        reader = self._reader()
        with (
            mock.patch.object(bot.sqlite3, "connect", self._short_connection),
            mock.patch.object(bot.time, "sleep", side_effect=lambda _: reader.rollback()) as backoff,
        ):
            reviewed = bot._record_ordinary_chat_single_packet_review(
                decision, reason="fixture_review", provider_call_count=1,
                corrective_call_count=0, processing_error=True,
            )
        backoff.assert_called_once()
        self.assertFalse(reviewed.candidate_selected)
        self.assertEqual(self._receipt()[1:5], (1, 0, 321, 123456))
        self.assertEqual(self._receipt()[6], 1)

    def test_exhausted_review_closes_transactions_and_does_not_approve(self):
        reader = self._reader()
        with (
            mock.patch.object(bot.sqlite3, "connect", self._short_connection),
            mock.patch.object(bot.time, "sleep") as backoff,
        ):
            with self.assertRaises(sqlite3.OperationalError):
                self._evaluate()
        self.assertEqual(backoff.call_count, 2)
        self.assertEqual(self._receipt()[:5], (0, 0, 0, 0, 0))
        reader.rollback()
        self.assertTrue(self._evaluate().candidate_selected)

    def test_non_lock_review_failure_is_not_retried(self):
        with (
            mock.patch.object(bot, "evaluate_single_packet_response",
                              side_effect=sqlite3.OperationalError("no such table: fixture")) as evaluate,
            mock.patch.object(bot.time, "sleep") as backoff,
        ):
            with self.assertRaisesRegex(sqlite3.OperationalError, "no such table"):
                self._evaluate()
        evaluate.assert_called_once()
        backoff.assert_not_called()

    async def test_transient_review_contention_does_not_repeat_paid_generation(self):
        holder = []

        async def generate(*_args, **_kwargs):
            holder.append(self._reader())
            return bot.TrackedGenerationResponse(
                text="Your favorite movie is Arrival.", provider_call_count=1,
                total_tokens=321, estimated_cost_nanos=123456, cost_priced=True,
            )

        with (
            mock.patch.object(bot.sqlite3, "connect", self._short_connection),
            mock.patch.object(bot.time, "sleep", side_effect=lambda _: holder[0].rollback()),
            mock.patch.object(bot, "get_tracked_gemini_response_with_optional_typing",
                              new=mock.AsyncMock(side_effect=generate)) as provider,
        ):
            execution = await bot.maybe_generate_ordinary_chat_single_packet(
                channel=None, prompt="Current user request: What do you remember about me?",
                basis=self.fixture.basis, scope_applied=True, preflight_reason="",
                situation_frame=self.fixture.frame, situation_frame_current_text=self.fixture.text,
                route_mode="normal_chat", channel_policy="public_context",
                conversation_surface="mention_or_reply", user_id=7, guild_id=1,
                user_display_name="Test Member", source_context_available=True,
            )
        provider.assert_awaited_once()
        self.assertIsNotNone(execution)
        self.assertTrue(execution.candidate_active)
        self.assertEqual(execution.response, "Your favorite movie is Arrival.")
        self.assertEqual(execution.review_reason, "")
        self.assertEqual(execution.corrective_call_count, 0)
        self.assertEqual(self._receipt(execution.decision.run.run_id)[:5],
                         (1, 1, 0, 321, 123456))


if __name__ == "__main__":
    unittest.main()
