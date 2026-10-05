from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
import sqlite3
import threading
from types import SimpleNamespace
import unittest
from unittest import mock

import test_token_budget_accounting as accounting_fixture

bot = accounting_fixture.bnl01_bot
provider_response = accounting_fixture.provider_response


class AtomicGenerationAccountingTests(unittest.TestCase):
    def setUp(self):
        accounting_fixture.TokenBudgetAccountingTests.setUp(self)
        real_connect = sqlite3.connect
        fixture_thread = threading.get_ident()
        self.fixture_connections = []

        def connect(*args, **kwargs):
            conn = real_connect(*args, **kwargs)
            if threading.get_ident() == fixture_thread:
                self.fixture_connections.append(conn)
            return conn

        # Retain native transaction/connection behavior throughout each test.
        # SQLite's context manager does not close handles; finish only fixture
        # ownership at teardown so Windows can remove the temporary database.
        self.connection_tracker = mock.patch.object(sqlite3, "connect", side_effect=connect)
        self.connection_tracker.start()

    def tearDown(self):
        self.connection_tracker.stop()
        try:
            for conn in self.fixture_connections:
                conn.close()
        finally:
            accounting_fixture.TokenBudgetAccountingTests.tearDown(self)

    def initialize(self, path=None, *, diagnostic_failure=""):
        with closing(sqlite3.connect(path or self.db_path)) as conn, conn:
            bot._ensure_token_usage_schema(conn.cursor())
            if diagnostic_failure:
                conn.execute(
                    "CREATE TRIGGER fail_diagnostic BEFORE INSERT ON "
                    "model_generation_attempts BEGIN SELECT RAISE("
                    + diagnostic_failure
                    + ", 'fixture diagnostic failure'); END"
                )

    def persisted(self, path=None):
        with closing(sqlite3.connect(path or self.db_path)) as conn:
            return (
                conn.execute("SELECT * FROM token_usage_events ORDER BY id").fetchall(),
                conn.execute("SELECT * FROM model_generation_attempts ORDER BY id").fetchall(),
                conn.execute("SELECT * FROM token_usage ORDER BY id").fetchall(),
            )

    def test_complete_rows_and_return_match_two_transaction_accounting(self):
        cases = (
            (provider_response(), "gemini-3.6-flash", 0),
            (SimpleNamespace(candidates=[]), "gemini-3.6-flash", 2500),
            (provider_response(), "future-unpriced-model", 0),
            (provider_response(total=0, prompt=0, candidate=0, thought=0, cached=0),
             "gemini-3.6-flash", 0),
        )
        frozen_now = datetime(2026, 10, 4, 19, 0, tzinfo=timezone.utc)
        for index, (response, model, fallback_total) in enumerate(cases):
            with self.subTest(case=index):
                reference = str(Path(self.tempdir.name) / f"reference-{index}.sqlite")
                candidate = str(Path(self.tempdir.name) / f"candidate-{index}.sqlite")
                self.initialize(reference)
                self.initialize(candidate)
                route = "normal_chat.fixture_safe"
                with (
                    mock.patch.object(bot, "_pacific_usage_date", return_value="2026-10-04"),
                    mock.patch.object(bot, "datetime", wraps=datetime) as clock,
                ):
                    clock.now.return_value = frozen_now
                    breakdown = bot._extract_token_usage_breakdown(response)
                    accounting_route = route
                    if not breakdown.total_tokens and fallback_total:
                        breakdown = bot.TokenUsageBreakdown(total_tokens=fallback_total)
                        accounting_route += ".usage_estimated"
                    with mock.patch.object(bot, "DB_FILE", reference):
                        expected_total = bot.record_token_usage(
                            breakdown, route=accounting_route, model=model,
                            reservation_id="fixture-reservation",
                        )
                        bot._record_model_generation_attempt(
                            breakdown, route=route, model=model, outcome="success",
                            attempt_number=2, is_retry=True, is_fallback=True,
                            reservation_id="fixture-reservation",
                        )
                    with mock.patch.object(bot, "DB_FILE", candidate):
                        actual_total = bot.record_generation_token_usage(
                            response, route=route, model=model,
                            fallback_total=fallback_total, attempt_number=2,
                            is_retry=True, is_fallback=True,
                            reservation_id="fixture-reservation",
                        )
                self.assertEqual(actual_total, expected_total)
                # Includes IDs, timestamps, every token/cost/pricing field,
                # route, status and retry/fallback metadata, and the counter.
                self.assertEqual(self.persisted(candidate), self.persisted(reference))

    def test_success_owns_one_writer_lease_and_commits_both_rows_together(self):
        self.initialize()
        real_connect = sqlite3.connect
        connections = []
        trace = []
        competitor_result = []
        observer_result = []

        def compete():
            with closing(real_connect(self.db_path, timeout=.05)) as other:
                try:
                    other.execute("BEGIN IMMEDIATE")
                except sqlite3.OperationalError:
                    competitor_result.append("busy")
                else:
                    competitor_result.append("entered")
                    other.rollback()

        def traced(sql):
            trace.append(sql.strip().upper())
            if sql.lstrip().upper().startswith("INSERT INTO MODEL_GENERATION_ATTEMPTS"):
                # Canonical usage is still uncommitted when its success
                # receipt is inserted; another writer cannot enter the gap.
                with closing(real_connect(self.db_path)) as observer:
                    observer_result.append(observer.execute(
                        "SELECT COUNT(*) FROM token_usage_events"
                    ).fetchone()[0])
                thread = threading.Thread(target=compete)
                thread.start()
                thread.join(timeout=2)
                self.assertFalse(thread.is_alive())

        def connect(*args, **kwargs):
            conn = real_connect(*args, **kwargs)
            conn.set_trace_callback(traced)
            connections.append(conn)
            return conn

        try:
            with mock.patch.object(bot.sqlite3, "connect", side_effect=connect):
                total = bot.record_generation_token_usage(
                    provider_response(), route="normal_chat", model="gemini-3.6-flash",
                )
        finally:
            for conn in connections:
                conn.close()
        self.assertEqual(total, 150)
        self.assertEqual(trace.count("BEGIN IMMEDIATE"), 1)
        self.assertEqual(trace.count("COMMIT"), 1)
        self.assertEqual(competitor_result, ["busy"])
        self.assertEqual(observer_result, [0])
        events, attempts, _ = self.persisted()
        self.assertEqual((len(events), len(attempts)), (1, 1))

    def test_diagnostic_failure_keeps_charge_and_reports_after_commit(self):
        self.initialize(diagnostic_failure="FAIL")
        with self.assertLogs(level="INFO") as logs:
            with self.assertRaisesRegex(sqlite3.IntegrityError, "fixture diagnostic failure"):
                bot.record_generation_token_usage(
                    provider_response(), route="normal_chat", model="gemini-3.6-flash",
                )
        events, attempts, counter = self.persisted()
        self.assertEqual((len(events), len(attempts)), (1, 0))
        self.assertEqual(counter[0][1], 150)
        self.assertTrue(any("model_token_usage " in line for line in logs.output))
        self.assertFalse(any("model_generation_attempt " in line for line in logs.output))

    def test_success_wrapper_retains_lease_on_diagnostic_failure_without_provider_retry(self):
        self.initialize(diagnostic_failure="FAIL")
        response = provider_response()
        generate = mock.Mock(return_value=response)
        reservation = bot.LocalBudgetReservation(1, "ordinary")
        with (
            mock.patch.object(bot, "gemini_client", SimpleNamespace(
                models=SimpleNamespace(generate_content=generate))),
            mock.patch.object(bot, "reserve_local_model_budget", return_value=reservation),
            mock.patch.object(bot, "release_local_model_budget") as release,
            self.assertLogs(level="INFO") as logs,
        ):
            result = bot._generate_gemini_content_with_fallback("fixture request", "normal_chat")
        self.assertIs(result.raw_response, response)
        self.assertEqual(generate.call_count, 1)
        self.assertFalse(result.fallback_used)
        release.assert_called_once_with(reservation, retain_cost_reservation=True)
        self.assertEqual(len(self.persisted()[0]), 1)
        self.assertTrue(any("model_success_accounting_failed " in line for line in logs.output))

    def test_transaction_abort_retains_unaccounted_lease_and_emits_no_charge_receipt(self):
        self.initialize(diagnostic_failure="ROLLBACK")
        response = provider_response()
        generate = mock.Mock(return_value=response)
        reservation = bot.LocalBudgetReservation(1, "ordinary")
        with (
            mock.patch.object(bot, "gemini_client", SimpleNamespace(
                models=SimpleNamespace(generate_content=generate))),
            mock.patch.object(bot, "reserve_local_model_budget", return_value=reservation),
            mock.patch.object(bot, "release_local_model_budget") as release,
            self.assertLogs(level="INFO") as logs,
        ):
            result = bot._generate_gemini_content_with_fallback("fixture request", "normal_chat")
        self.assertIs(result.raw_response, response)
        self.assertEqual(generate.call_count, 1)
        release.assert_called_once_with(reservation, retain_cost_reservation=True)
        events, attempts, counter = self.persisted()
        self.assertEqual((len(events), len(attempts), counter[0][1]), (0, 0, 0))
        self.assertFalse(any("model_token_usage " in line for line in logs.output))
        self.assertFalse(any("model_generation_attempt " in line for line in logs.output))
        self.assertTrue(any("model_success_accounting_failed " in line for line in logs.output))

    def test_usage_and_success_attempt_share_one_accounting_date(self):
        with mock.patch.object(
            bot, "_pacific_usage_date", side_effect=["2026-10-04", "2026-10-05"],
        ) as usage_date:
            total = bot.record_generation_token_usage(
                provider_response(), route="normal_chat", model="gemini-3.6-flash",
            )
        events, attempts, counter = self.persisted()
        self.assertEqual(total, 150)
        self.assertEqual(usage_date.call_count, 1)
        self.assertEqual((events[0][1], attempts[0][1], counter[0][2]),
                         ("2026-10-04", "2026-10-04", "2026-10-04"))

    def test_commit_failure_never_emits_successful_accounting_receipts(self):
        self.initialize()
        real_connect = sqlite3.connect
        connections = []

        class FailingCommitConnection(sqlite3.Connection):
            def commit(self):
                raise sqlite3.OperationalError("fixture commit failure")

        def connect(*args, **kwargs):
            conn = real_connect(*args, **kwargs, factory=FailingCommitConnection)
            connections.append(conn)
            return conn

        try:
            with (
                mock.patch.object(bot.sqlite3, "connect", side_effect=connect),
                mock.patch.object(bot.logging, "info") as log_info,
            ):
                with self.assertRaisesRegex(sqlite3.OperationalError, "fixture commit failure"):
                    bot.record_generation_token_usage(
                        provider_response(), route="normal_chat", model="gemini-3.6-flash",
                    )
        finally:
            for conn in connections:
                conn.close()
        events, attempts, counter = self.persisted()
        self.assertEqual((len(events), len(attempts), counter[0][1]), (0, 0, 0))
        logged_templates = [str(call.args[0]) for call in log_info.call_args_list]
        self.assertFalse(any("model_token_usage " in line for line in logged_templates))
        self.assertFalse(any("model_generation_attempt " in line for line in logged_templates))

    def test_charged_failure_and_later_success_keep_attempt_and_retry_metadata(self):
        self.initialize()
        failure = RuntimeError("503 service unavailable")
        failure.response = provider_response()
        with mock.patch.object(bot, "_pacific_usage_date", return_value="2026-10-04"):
            failed_tokens = bot.record_failed_generation_attempt(
                failure, route="website_relay_event", model="gemini-3.6-flash",
                attempt_number=1, reservation_id="fixture-shared-reservation",
            )
            total = bot.record_generation_token_usage(
                provider_response(), route="website_relay_event", model="gemini-3.6-flash",
                attempt_number=2, is_retry=True, is_fallback=True,
                reservation_id="fixture-shared-reservation",
            )
        with closing(sqlite3.connect(self.db_path)) as conn:
            events = conn.execute(
                "SELECT route,total_tokens FROM token_usage_events ORDER BY id"
            ).fetchall()
            attempts = conn.execute(
                "SELECT outcome,error_category,provider_status_code,total_tokens,"
                "attempt_number,is_retry,is_fallback FROM model_generation_attempts ORDER BY id"
            ).fetchall()
        self.assertEqual((failed_tokens, total), (150, 300))
        self.assertEqual(events, [("website_relay_event.failed_server", 150),
                                  ("website_relay_event", 150)])
        self.assertEqual(attempts, [("failure", "server", 503, 150, 1, 0, 0),
                                    ("success", "", 0, 150, 2, 1, 1)])


if __name__ == "__main__":
    unittest.main()
