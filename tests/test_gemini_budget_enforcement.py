import asyncio
import concurrent.futures
from datetime import datetime
from decimal import Decimal
import os
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest
from types import SimpleNamespace
from unittest import mock
from zoneinfo import ZoneInfo

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl01_bot


PACIFIC = ZoneInfo("America/Los_Angeles")


def provider_response():
    return SimpleNamespace(
        candidates=[
            SimpleNamespace(
                content=SimpleNamespace(
                    parts=[SimpleNamespace(text="generated")]
                )
            )
        ],
        usage_metadata=SimpleNamespace(
            total_token_count=1_400,
            prompt_token_count=1_000,
            candidates_token_count=100,
            thoughts_token_count=300,
            cached_content_token_count=200,
        ),
    )


class GeminiBudgetEnforcementTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.tempdir.name) / "usage.sqlite")
        self.original_db = bnl01_bot.DB_FILE
        bnl01_bot.DB_FILE = self.db_path
        with bnl01_bot._token_budget_reservation_lock:
            bnl01_bot._token_budget_reserved_tokens = 0
            bnl01_bot._token_budget_reserved_by_lane.clear()
        self.now = datetime(2026, 8, 31, 12, 0, tzinfo=PACIFIC)
        self.clock_patch = mock.patch.object(
            bnl01_bot,
            "_pacific_now",
            return_value=self.now,
        )
        self.clock_patch.start()

    def tearDown(self):
        self.clock_patch.stop()
        bnl01_bot.DB_FILE = self.original_db
        with bnl01_bot._token_budget_reservation_lock:
            bnl01_bot._token_budget_reserved_tokens = 0
            bnl01_bot._token_budget_reserved_by_lane.clear()
        self.tempdir.cleanup()

    @staticmethod
    def default_budget_env(**overrides):
        values = {
            "BNL_GEMINI_MONTHLY_TARGET_USD": "20.00",
            "BNL_GEMINI_MONTHLY_HARD_LIMIT_USD": "24.00",
            "BNL_GEMINI_DAILY_SOFT_LIMIT_USD": "0.65",
            "BNL_GEMINI_BUDGET_ENFORCEMENT_ENABLED": "true",
            "BNL_GEMINI_BILLING_LAG_BUFFER_USD": "0.50",
            "BNL_GEMINI_JOURNAL_RESERVE_USD": "1.00",
            "BNL_GEMINI_INTERACTIVE_RESERVE_USD": "2.00",
        }
        values.update(overrides)
        return values

    def decision(self, route, *, request, month, today, active=0):
        return bnl01_bot._dollar_budget_decision(
            route=route,
            request_nanos=bnl01_bot._usd_to_nanos(Decimal(request)),
            month_nanos=bnl01_bot._usd_to_nanos(Decimal(month)),
            today_nanos=bnl01_bot._usd_to_nanos(Decimal(today)),
            active_month_nanos=bnl01_bot._usd_to_nanos(Decimal(active)),
            active_today_nanos=bnl01_bot._usd_to_nanos(Decimal(active)),
            unpriced_calls=0,
            unpriced_month_guardrail_nanos=0,
            unpriced_today_guardrail_nanos=0,
            now_pacific=self.now,
        )

    def test_provider_client_has_one_sdk_attempt_and_timeout_below_lease(self):
        original_client = bnl01_bot.gemini_client
        sentinel = object()
        constructor = mock.Mock(return_value=sentinel)
        try:
            bnl01_bot.gemini_client = None
            with (
                mock.patch.object(bnl01_bot.genai, "Client", constructor),
                mock.patch.dict(
                    os.environ,
                    {"BNL_GEMINI_PROVIDER_TIMEOUT_SECONDS": "9999"},
                    clear=False,
                ),
            ):
                self.assertIs(bnl01_bot.get_gemini_client(), sentinel)
            options = constructor.call_args.kwargs["http_options"]
            self.assertEqual(options.timeout, 240_000)
            self.assertEqual(options.retry_options.attempts, 1)
        finally:
            bnl01_bot.gemini_client = original_client

    def test_daily_soft_limit_stops_optional_background_work(self):
        with mock.patch.dict(os.environ, self.default_budget_env(), clear=False):
            allowed, reason = self.decision(
                "website_relay_event",
                request="0.02",
                month="19.99",
                today="0.64",
            )
        self.assertFalse(allowed)
        self.assertEqual(reason, "daily_soft_limit")

    def test_monthly_pace_stops_background_but_not_interactive(self):
        with mock.patch.dict(os.environ, self.default_budget_env(), clear=False):
            background = self.decision(
                "ambient_generation",
                request="0.01",
                month="20.01",
                today="0.10",
            )
            interactive = self.decision(
                "normal_chat",
                request="0.01",
                month="20.01",
                today="0.10",
            )
        self.assertEqual(background, (False, "monthly_target_pace"))
        self.assertEqual(interactive, (True, "interactive_available"))

    def test_background_yields_to_interactive_and_journal_reserves(self):
        with mock.patch.dict(os.environ, self.default_budget_env(), clear=False):
            background = self.decision(
                "website_relay_event",
                request="0.01",
                month="20.50",
                today="0.10",
            )
            interactive = self.decision(
                "normal_chat",
                request="0.01",
                month="20.50",
                today="0.10",
            )
            journal = self.decision(
                bnl01_bot.JOURNAL_ROUTE,
                request="0.01",
                month="20.50",
                today="0.10",
            )
        self.assertEqual(
            background,
            (False, "interactive_and_journal_reserve"),
        )
        self.assertTrue(interactive[0])
        self.assertTrue(journal[0])

    def test_journal_reserve_outlives_ordinary_chat(self):
        with mock.patch.dict(os.environ, self.default_budget_env(), clear=False):
            interactive = self.decision(
                "ordinary_chat_single_packet_canary",
                request="0.10",
                month="22.45",
                today="0.10",
            )
            journal = self.decision(
                bnl01_bot.JOURNAL_ROUTE,
                request="0.10",
                month="22.45",
                today="0.10",
            )
        self.assertEqual(interactive, (False, "journal_reserve"))
        self.assertEqual(journal, (True, "journal_protected"))

    def test_effective_hard_limit_stops_every_route_with_lag_buffer(self):
        with mock.patch.dict(os.environ, self.default_budget_env(), clear=False):
            for route in (
                "website_relay_event",
                "normal_chat",
                bnl01_bot.JOURNAL_ROUTE,
            ):
                with self.subTest(route=route):
                    self.assertEqual(
                        self.decision(
                            route,
                            request="0.02",
                            month="23.49",
                            today="0.10",
                        ),
                        (False, "monthly_hard_limit"),
                    )

    def test_unpriced_history_restricts_background_without_monthlong_journal_outage(self):
        with mock.patch.dict(os.environ, self.default_budget_env(), clear=False):
            journal = bnl01_bot._dollar_budget_decision(
                route=bnl01_bot.JOURNAL_ROUTE,
                request_nanos=1,
                month_nanos=0,
                today_nanos=0,
                active_month_nanos=0,
                active_today_nanos=0,
                unpriced_calls=1,
                unpriced_month_guardrail_nanos=9_000_000,
                unpriced_today_guardrail_nanos=9_000_000,
                now_pacific=self.now,
            )
            background = bnl01_bot._dollar_budget_decision(
                route="website_relay_event",
                request_nanos=1,
                month_nanos=0,
                today_nanos=0,
                active_month_nanos=0,
                active_today_nanos=0,
                unpriced_calls=1,
                unpriced_month_guardrail_nanos=9_000_000,
                unpriced_today_guardrail_nanos=9_000_000,
                now_pacific=self.now,
            )
        self.assertEqual(journal, (True, "journal_protected"))
        self.assertEqual(background, (False, "unpriced_monthly_usage"))

    def test_concurrent_sqlite_reservations_cannot_oversubscribe(self):
        env = self.default_budget_env(
            BNL_GEMINI_MONTHLY_TARGET_USD="0.006",
            BNL_GEMINI_MONTHLY_HARD_LIMIT_USD="0.006",
            BNL_GEMINI_DAILY_SOFT_LIMIT_USD="0.006",
            BNL_GEMINI_BILLING_LAG_BUFFER_USD="0",
            BNL_GEMINI_JOURNAL_RESERVE_USD="0",
            BNL_GEMINI_INTERACTIVE_RESERVE_USD="0",
            BNL_GEMINI_BACKGROUND_MAX_OUTPUT_TOKENS="1024",
        )
        barrier = threading.Barrier(2)

        def reserve():
            barrier.wait(timeout=5)
            try:
                return bnl01_bot._reserve_dollar_budget(
                    "x",
                    "website_relay_event",
                )[0]
            except bnl01_bot.LocalModelBudgetExhausted:
                return ""

        with mock.patch.dict(os.environ, env, clear=False):
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                reservations = list(pool.map(lambda _index: reserve(), range(2)))

        claimed = [item for item in reservations if item]
        self.assertEqual(len(claimed), 1)
        with sqlite3.connect(self.db_path) as conn:
            count = conn.execute(
                "SELECT COUNT(*) FROM gemini_budget_reservations"
            ).fetchone()[0]
        self.assertEqual(count, 1)
        bnl01_bot._release_dollar_budget(claimed[0])

    def test_reservation_ttl_cannot_be_shorter_than_retry_envelope(self):
        env = self.default_budget_env(
            BNL_GEMINI_BUDGET_RESERVATION_TTL_MINUTES="5",
        )
        with mock.patch.dict(os.environ, env, clear=False):
            reservation_id, _cost = bnl01_bot._reserve_dollar_budget(
                "x",
                "normal_chat",
            )
        with sqlite3.connect(self.db_path) as conn:
            created_at, expires_at = conn.execute(
                """
                SELECT created_at, expires_at
                FROM gemini_budget_reservations
                WHERE reservation_id=?
                """,
                (reservation_id,),
            ).fetchone()
        lease_minutes = (
            datetime.fromisoformat(expires_at)
            - datetime.fromisoformat(created_at)
        ).total_seconds() / 60
        self.assertEqual(lease_minutes, 30)
        bnl01_bot._release_dollar_budget(reservation_id)

    def test_budget_exhaustion_returns_clean_empty_background_result(self):
        fake_client = SimpleNamespace(
            models=SimpleNamespace(generate_content=mock.Mock())
        )
        env = self.default_budget_env(
            BNL_GEMINI_MONTHLY_TARGET_USD="0.001",
            BNL_GEMINI_MONTHLY_HARD_LIMIT_USD="0.001",
            BNL_GEMINI_DAILY_SOFT_LIMIT_USD="0.001",
            BNL_GEMINI_BILLING_LAG_BUFFER_USD="0",
            BNL_GEMINI_JOURNAL_RESERVE_USD="0",
            BNL_GEMINI_INTERACTIVE_RESERVE_USD="0",
        )
        with (
            mock.patch.dict(os.environ, env, clear=False),
            mock.patch.object(bnl01_bot, "gemini_client", fake_client),
        ):
            result = asyncio.run(
                bnl01_bot.get_gemini_response(
                    "optional relay",
                    user_id=0,
                    guild_id=42,
                    route="website_relay_event",
                )
            )
        self.assertEqual(result, "")
        fake_client.models.generate_content.assert_not_called()

    def test_hard_limit_single_packet_uses_existing_visible_local_fallback(self):
        basis = SimpleNamespace(
            packet=SimpleNamespace(source_snapshot_digest="source-digest")
        )
        run = SimpleNamespace(
            prompt_applied=True,
            fallback_reason="",
            revalidation_status="passed",
            basis=basis,
        )
        decision = SimpleNamespace(
            candidate_selected=False,
            fallback_reason="provider_call_count_invalid",
            run=run,
        )
        fake_client = SimpleNamespace(
            models=SimpleNamespace(generate_content=mock.Mock())
        )
        env = self.default_budget_env(
            BNL_GEMINI_MONTHLY_TARGET_USD="0",
            BNL_GEMINI_MONTHLY_HARD_LIMIT_USD="0",
            BNL_GEMINI_DAILY_SOFT_LIMIT_USD="0",
            BNL_GEMINI_BILLING_LAG_BUFFER_USD="0",
            BNL_GEMINI_JOURNAL_RESERVE_USD="0",
            BNL_GEMINI_INTERACTIVE_RESERVE_USD="0",
        )
        with (
            mock.patch.dict(os.environ, env, clear=False),
            mock.patch.object(bnl01_bot, "gemini_client", fake_client),
            mock.patch.object(
                bnl01_bot,
                "BNL_TYPING_INDICATOR_ENABLED",
                False,
            ),
            mock.patch.object(
                bnl01_bot,
                "build_packet_owned_prompt",
                return_value=SimpleNamespace(
                    ready=True,
                    prompt="packet-owned prompt",
                    reason="",
                ),
            ),
            mock.patch.object(
                bnl01_bot,
                "revalidate_situation_frame",
                return_value=SimpleNamespace(status="valid"),
            ),
            mock.patch.object(
                bnl01_bot,
                "_begin_ordinary_chat_single_packet_receipt",
                return_value=run,
            ),
            mock.patch.object(
                bnl01_bot,
                "_evaluate_ordinary_chat_single_packet_receipt",
                return_value=decision,
            ),
        ):
            execution = asyncio.run(
                bnl01_bot.maybe_generate_ordinary_chat_single_packet(
                    channel=None,
                    prompt="base prompt",
                    basis=basis,
                    scope_applied=True,
                    preflight_block_reason="",
                    situation_frame=SimpleNamespace(),
                    situation_frame_current_text="Answer this.",
                    route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                    channel_policy="public_context",
                    conversation_surface="mention_or_reply",
                    user_id=7,
                    guild_id=1,
                    user_display_name="Test Member",
                    source_context_available=True,
                )
            )

        self.assertFalse(execution.candidate_active)
        self.assertEqual(execution.provider_call_count, 0)
        self.assertTrue(execution.response)
        self.assertEqual(
            execution.response,
            bnl01_bot._ordinary_chat_single_packet_block_response(
                "provider_call_count_invalid"
            ),
        )
        fake_client.models.generate_content.assert_not_called()

    def test_unknown_active_model_is_unpriced_and_never_called(self):
        fake_client = SimpleNamespace(
            models=SimpleNamespace(generate_content=mock.Mock())
        )
        with (
            mock.patch.dict(os.environ, self.default_budget_env(), clear=False),
            mock.patch.object(bnl01_bot, "GEMINI_MODEL", "future-unknown-model"),
            mock.patch.object(bnl01_bot, "gemini_client", fake_client),
        ):
            result = asyncio.run(
                bnl01_bot.get_gemini_response(
                    "optional relay",
                    user_id=0,
                    guild_id=42,
                    route="website_relay_event",
                )
            )
        self.assertEqual(result, "")
        fake_client.models.generate_content.assert_not_called()

    def test_successful_provider_response_survives_accounting_failure(self):
        response = provider_response()
        generate = mock.Mock(return_value=response)
        fake_client = SimpleNamespace(
            models=SimpleNamespace(generate_content=generate)
        )
        reservation = bnl01_bot.LocalBudgetReservation(1, "ordinary")
        with (
            mock.patch.object(bnl01_bot, "gemini_client", fake_client),
            mock.patch.object(
                bnl01_bot,
                "reserve_local_model_budget",
                return_value=reservation,
            ),
            mock.patch.object(
                bnl01_bot,
                "release_local_model_budget",
            ) as release,
            mock.patch.object(
                bnl01_bot,
                "record_generation_token_usage",
                side_effect=sqlite3.OperationalError("ledger unavailable"),
            ),
        ):
            routed = bnl01_bot._generate_gemini_content_with_fallback(
                "hello",
                "normal_chat",
            )
        self.assertIs(routed.raw_response, response)
        self.assertFalse(routed.fallback_used)
        self.assertEqual(generate.call_count, 1)
        release.assert_called_once_with(
            reservation,
            retain_cost_reservation=True,
        )

    def test_accounting_failure_retains_conservative_sqlite_lease(self):
        response = provider_response()
        fake_client = SimpleNamespace(
            models=SimpleNamespace(
                generate_content=mock.Mock(return_value=response)
            )
        )
        budget_env = self.default_budget_env(
            BNL_GEMINI_MONTHLY_TARGET_USD="0.15",
            BNL_GEMINI_MONTHLY_HARD_LIMIT_USD="0.15",
            BNL_GEMINI_DAILY_SOFT_LIMIT_USD="0.15",
            BNL_GEMINI_BILLING_LAG_BUFFER_USD="0",
            BNL_GEMINI_JOURNAL_RESERVE_USD="0",
            BNL_GEMINI_INTERACTIVE_RESERVE_USD="0",
        )
        with (
            mock.patch.dict(os.environ, budget_env, clear=False),
            mock.patch.object(bnl01_bot, "gemini_client", fake_client),
            mock.patch.object(
                bnl01_bot,
                "record_generation_token_usage",
                side_effect=sqlite3.OperationalError("ledger unavailable"),
            ),
        ):
            routed = bnl01_bot._generate_gemini_content_with_fallback(
                "hello",
                "normal_chat",
            )

        self.assertIs(routed.raw_response, response)
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                """
                SELECT reservation_id, expires_at
                FROM gemini_budget_reservations
                """
            ).fetchall()
        self.assertEqual(len(rows), 1)
        expected_expiry = bnl01_bot.pacific_budget_clock(
            self.now
        ).next_monthly_reset_at.astimezone(ZoneInfo("UTC"))
        self.assertGreaterEqual(
            datetime.fromisoformat(rows[0][1]),
            expected_expiry,
        )
        self.assertEqual(bnl01_bot._token_budget_reserved_tokens, 0)
        with mock.patch.dict(os.environ, budget_env, clear=False):
            with self.assertRaises(bnl01_bot.LocalModelBudgetExhausted):
                bnl01_bot._reserve_dollar_budget("hello", "normal_chat")
        bnl01_bot._release_dollar_budget(rows[0][0])

    def test_successful_accounting_releases_sqlite_lease(self):
        response = provider_response()
        fake_client = SimpleNamespace(
            models=SimpleNamespace(
                generate_content=mock.Mock(return_value=response)
            )
        )
        with (
            mock.patch.dict(os.environ, self.default_budget_env(), clear=False),
            mock.patch.object(bnl01_bot, "gemini_client", fake_client),
        ):
            routed = bnl01_bot._generate_gemini_content_with_fallback(
                "hello",
                "normal_chat",
            )

        self.assertIs(routed.raw_response, response)
        with sqlite3.connect(self.db_path) as conn:
            lease_count = conn.execute(
                "SELECT COUNT(*) FROM gemini_budget_reservations"
            ).fetchone()[0]
            usage_count = conn.execute(
                "SELECT COUNT(*) FROM token_usage_events"
            ).fetchone()[0]
        self.assertEqual(lease_count, 0)
        self.assertEqual(usage_count, 1)

    def test_background_provider_failure_has_no_retry_or_fallback(self):
        generate = mock.Mock(side_effect=RuntimeError("503 service unavailable"))
        fake_client = SimpleNamespace(
            models=SimpleNamespace(generate_content=generate)
        )
        reservation = bnl01_bot.LocalBudgetReservation(1, "relay")
        with (
            mock.patch.object(bnl01_bot, "gemini_client", fake_client),
            mock.patch.object(
                bnl01_bot,
                "reserve_local_model_budget",
                return_value=reservation,
            ),
            mock.patch.object(bnl01_bot, "release_local_model_budget"),
            mock.patch.object(bnl01_bot, "record_failed_generation_attempt"),
        ):
            with self.assertRaisesRegex(RuntimeError, "503"):
                bnl01_bot._generate_gemini_content_with_fallback(
                    "optional",
                    "website_relay_event",
                )
        self.assertEqual(generate.call_count, 1)
        self.assertEqual(
            generate.call_args.kwargs["model"],
            "models/gemini-3.6-flash",
        )

    def test_failed_attempt_usage_cost_retry_and_fallback_are_reported(self):
        error = RuntimeError("503 service unavailable")
        error.response = provider_response()
        with mock.patch.dict(os.environ, self.default_budget_env(), clear=False):
            recorded = bnl01_bot.record_failed_generation_attempt(
                error,
                route="normal_chat",
                model="gemini-3.5-flash",
                attempt_number=2,
                is_retry=True,
                is_fallback=True,
            )
            diagnostics = bnl01_bot.get_usage_breakdown()
        self.assertEqual(recorded, 1_400)
        self.assertEqual(diagnostics["retry_count_month"], 1)
        self.assertEqual(diagnostics["fallback_count_month"], 1)
        self.assertEqual(diagnostics["failed_attempt_count_month"], 1)
        self.assertGreater(diagnostics["failed_attempt_cost_usd"], Decimal("0"))
        self.assertEqual(diagnostics["cost_routes"][0]["calls"], 1)

    def test_new_month_ignores_prior_month_cost(self):
        with sqlite3.connect(self.db_path) as conn:
            bnl01_bot._ensure_token_usage_schema(conn.cursor())
            conn.execute(
                """
                INSERT INTO token_usage_events (
                    usage_date, recorded_at, route, model, total_tokens,
                    estimated_cost_nanos, cost_priced, pricing_version
                ) VALUES (?, ?, ?, ?, ?, ?, 1, ?)
                """,
                (
                    "2026-08-31",
                    "2026-09-01T06:59:00Z",
                    "normal_chat",
                    "gemini-3.6-flash",
                    1,
                    bnl01_bot._usd_to_nanos(Decimal("23.40")),
                    bnl01_bot.PRICING_VERSION,
                ),
            )
        september = datetime(2026, 9, 1, 0, 1, tzinfo=PACIFIC)
        with (
            mock.patch.object(
                bnl01_bot,
                "_pacific_now",
                return_value=september,
            ),
            mock.patch.dict(os.environ, self.default_budget_env(), clear=False),
        ):
            reservation_id, _cost = bnl01_bot._reserve_dollar_budget(
                "journal",
                bnl01_bot.JOURNAL_ROUTE,
            )
        self.assertTrue(reservation_id)
        bnl01_bot._release_dollar_budget(reservation_id)

    def test_cost_schema_migration_is_backward_compatible_and_idempotent(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE token_usage_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    usage_date TEXT NOT NULL,
                    recorded_at TEXT NOT NULL,
                    route TEXT NOT NULL,
                    model TEXT NOT NULL,
                    prompt_tokens INTEGER NOT NULL DEFAULT 0,
                    candidate_tokens INTEGER NOT NULL DEFAULT 0,
                    thought_tokens INTEGER NOT NULL DEFAULT 0,
                    cached_tokens INTEGER NOT NULL DEFAULT 0,
                    total_tokens INTEGER NOT NULL DEFAULT 0
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE model_generation_attempts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    usage_date TEXT NOT NULL,
                    recorded_at TEXT NOT NULL,
                    route TEXT NOT NULL,
                    model TEXT NOT NULL,
                    outcome TEXT NOT NULL,
                    error_category TEXT NOT NULL DEFAULT '',
                    provider_status_code INTEGER NOT NULL DEFAULT 0,
                    prompt_tokens INTEGER NOT NULL DEFAULT 0,
                    candidate_tokens INTEGER NOT NULL DEFAULT 0,
                    thought_tokens INTEGER NOT NULL DEFAULT 0,
                    cached_tokens INTEGER NOT NULL DEFAULT 0,
                    total_tokens INTEGER NOT NULL DEFAULT 0
                )
                """
            )
            bnl01_bot._ensure_token_usage_schema(conn.cursor())
            bnl01_bot._ensure_token_usage_schema(conn.cursor())
            event_columns = {
                row[1]
                for row in conn.execute(
                    "PRAGMA table_info(token_usage_events)"
                ).fetchall()
            }
            attempt_columns = {
                row[1]
                for row in conn.execute(
                    "PRAGMA table_info(model_generation_attempts)"
                ).fetchall()
            }
        self.assertIn("estimated_cost_nanos", event_columns)
        self.assertIn("pricing_version", event_columns)
        self.assertIn("attempt_number", attempt_columns)
        self.assertIn("is_retry", attempt_columns)
        self.assertIn("is_fallback", attempt_columns)


if __name__ == "__main__":
    unittest.main()
