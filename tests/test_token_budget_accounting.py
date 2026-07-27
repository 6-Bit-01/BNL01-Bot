import os
from pathlib import Path
import sqlite3
import tempfile
import unittest
from types import SimpleNamespace
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl01_bot


def provider_response(
    *,
    text="generated",
    total=150,
    prompt=100,
    candidate=20,
    thought=30,
    cached=10,
):
    return SimpleNamespace(
        candidates=[
            SimpleNamespace(
                content=SimpleNamespace(
                    parts=[SimpleNamespace(text=text)]
                )
            )
        ],
        usage_metadata=SimpleNamespace(
            total_token_count=total,
            prompt_token_count=prompt,
            candidates_token_count=candidate,
            thoughts_token_count=thought,
            cached_content_token_count=cached,
        ),
    )


class TokenBudgetAccountingTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.tempdir.name) / "usage.sqlite")
        self.original_db_file = bnl01_bot.DB_FILE
        bnl01_bot.DB_FILE = self.db_path
        with bnl01_bot._token_budget_reservation_lock:
            bnl01_bot._token_budget_reserved_tokens = 0

    def tearDown(self):
        bnl01_bot.DB_FILE = self.original_db_file
        with bnl01_bot._token_budget_reservation_lock:
            bnl01_bot._token_budget_reserved_tokens = 0
        self.tempdir.cleanup()

    def test_provider_metadata_is_recorded_once_with_route_breakdown(self):
        with mock.patch.object(
            bnl01_bot,
            "_pacific_usage_date",
            return_value="2026-07-26",
        ):
            total = bnl01_bot.record_generation_token_usage(
                provider_response(),
                route="bnl_memory_preview_candidate",
                model="gemini-3.6-flash",
            )
            diagnostics = bnl01_bot.get_usage_breakdown()

        self.assertEqual(total, 150)
        self.assertEqual(diagnostics["total_tokens"], 150)
        self.assertEqual(diagnostics["tracked_calls"], 1)
        self.assertEqual(diagnostics["prompt_tokens"], 100)
        self.assertEqual(diagnostics["candidate_tokens"], 20)
        self.assertEqual(diagnostics["thought_tokens"], 30)
        self.assertEqual(diagnostics["cached_tokens"], 10)
        self.assertEqual(diagnostics["unattributed_tokens"], 0)
        self.assertEqual(
            diagnostics["routes"],
            [
                {
                    "route": "bnl_memory_preview_candidate",
                    "calls": 1,
                    "total_tokens": 150,
                }
            ],
        )
        with sqlite3.connect(self.db_path) as conn:
            event = conn.execute(
                """
                SELECT usage_date, route, model, prompt_tokens,
                       candidate_tokens, thought_tokens, cached_tokens,
                       total_tokens
                FROM token_usage_events
                """
            ).fetchone()
        self.assertEqual(
            event,
            (
                "2026-07-26",
                "bnl_memory_preview_candidate",
                "gemini-3.6-flash",
                100,
                20,
                30,
                10,
                150,
            ),
        )

    def test_pre_ledger_daily_total_remains_visible_as_unattributed(self):
        with sqlite3.connect(self.db_path) as conn:
            bnl01_bot._ensure_token_usage_schema(conn.cursor())
            conn.execute(
                """
                UPDATE token_usage
                SET tokens_used_today = ?,
                    last_reset_date = ?
                WHERE id = 1
                """,
                (1_000, "2026-07-26"),
            )
        with mock.patch.object(
            bnl01_bot,
            "_pacific_usage_date",
            return_value="2026-07-26",
        ):
            bnl01_bot.record_token_usage(
                bnl01_bot.TokenUsageBreakdown(total_tokens=25),
                route="new_route",
                model="test-model",
            )
            diagnostics = bnl01_bot.get_usage_breakdown()

        self.assertEqual(diagnostics["total_tokens"], 1_025)
        self.assertEqual(diagnostics["tracked_total_tokens"], 25)
        self.assertEqual(diagnostics["unattributed_tokens"], 1_000)

    def test_midnight_pacific_reset_preserves_prior_event_history(self):
        with mock.patch.object(
            bnl01_bot,
            "_pacific_usage_date",
            return_value="2026-07-25",
        ):
            bnl01_bot.record_token_usage(
                bnl01_bot.TokenUsageBreakdown(total_tokens=90),
                route="before_midnight",
                model="test-model",
            )
        with mock.patch.object(
            bnl01_bot,
            "_pacific_usage_date",
            return_value="2026-07-26",
        ):
            bnl01_bot.record_token_usage(
                bnl01_bot.TokenUsageBreakdown(total_tokens=10),
                route="after_midnight",
                model="test-model",
            )
            diagnostics = bnl01_bot.get_usage_breakdown()

        self.assertEqual(diagnostics["usage_date"], "2026-07-26")
        self.assertEqual(diagnostics["total_tokens"], 10)
        self.assertEqual(diagnostics["tracked_total_tokens"], 10)
        with sqlite3.connect(self.db_path) as conn:
            events = conn.execute(
                """
                SELECT usage_date, route, total_tokens
                FROM token_usage_events
                ORDER BY id
                """
            ).fetchall()
        self.assertEqual(
            events,
            [
                ("2026-07-25", "before_midnight", 90),
                ("2026-07-26", "after_midnight", 10),
            ],
        )

    def test_generation_wrapper_applies_limits_and_centrally_accounts(self):
        response = provider_response(total=75, prompt=40, candidate=15, thought=20)
        fake_client = SimpleNamespace(
            models=SimpleNamespace(
                generate_content=mock.Mock(return_value=response)
            )
        )
        with (
            mock.patch.object(bnl01_bot, "gemini_client", fake_client),
            mock.patch.object(
                bnl01_bot,
                "_pacific_usage_date",
                return_value="2026-07-26",
            ),
        ):
            returned = bnl01_bot._generate_gemini_content_with_fallback(
                "bounded prompt",
                "normal_chat",
            )
            diagnostics = bnl01_bot.get_usage_breakdown()

        self.assertIs(returned.raw_response, response)
        self.assertEqual(returned.model_name, bnl01_bot.GEMINI_MODEL)
        self.assertFalse(returned.fallback_used)
        call = fake_client.models.generate_content.call_args
        config = call.kwargs["config"]
        self.assertEqual(
            config.max_output_tokens,
            bnl01_bot.policy_for_route("normal_chat").max_output_tokens,
        )
        self.assertIsNone(config.thinking_config)
        self.assertEqual(diagnostics["total_tokens"], 75)
        self.assertEqual(diagnostics["tracked_calls"], 1)
        self.assertEqual(diagnostics["routes"][0]["route"], "normal_chat")
        self.assertEqual(bnl01_bot._token_budget_reserved_tokens, 0)

    def test_missing_provider_usage_is_conservatively_identified(self):
        response = SimpleNamespace(
            candidates=[
                SimpleNamespace(
                    content=SimpleNamespace(
                        parts=[SimpleNamespace(text="generated")]
                    )
                )
            ]
        )
        with mock.patch.object(
            bnl01_bot,
            "_pacific_usage_date",
            return_value="2026-07-26",
        ):
            bnl01_bot.record_generation_token_usage(
                response,
                route="normal_chat",
                model="gemini-3.6-flash",
                fallback_total=2_500,
            )
            diagnostics = bnl01_bot.get_usage_breakdown()

        self.assertEqual(diagnostics["total_tokens"], 2_500)
        self.assertEqual(
            diagnostics["routes"][0]["route"],
            "normal_chat.usage_estimated",
        )

    def test_reservation_blocks_new_call_before_soft_counter_is_crossed(self):
        today = "2026-07-26"
        with sqlite3.connect(self.db_path) as conn:
            bnl01_bot._ensure_token_usage_schema(conn.cursor())
            conn.execute(
                """
                UPDATE token_usage
                SET tokens_used_today = ?,
                    last_reset_date = ?
                WHERE id = 1
                """,
                (bnl01_bot.DAILY_TOKEN_LIMIT - 100, today),
            )
        with mock.patch.object(
            bnl01_bot,
            "_pacific_usage_date",
            return_value=today,
        ):
            self.assertTrue(
                bnl01_bot.check_quota_availability(bnl01_bot.JOURNAL_ROUTE)
            )
            self.assertFalse(
                bnl01_bot.check_quota_availability("normal_chat")
            )
            with self.assertRaises(
                bnl01_bot.LocalModelBudgetExhausted
            ):
                bnl01_bot.reserve_local_model_budget("small prompt", "normal_chat")
        self.assertEqual(bnl01_bot._token_budget_reserved_tokens, 0)

    def test_inflight_reservation_prevents_concurrent_oversubscription(self):
        prompt = "small prompt"
        reservation = bnl01_bot._estimated_generation_reservation(prompt, "normal_chat")
        with (
            mock.patch.object(
                bnl01_bot,
                "_pacific_usage_date",
                return_value="2026-07-26",
            ),
            mock.patch.object(
                bnl01_bot,
                "DAILY_TOKEN_LIMIT",
                reservation * 2 - 1,
            ),
        ):
            first = bnl01_bot.reserve_local_model_budget(prompt, "normal_chat")
            with self.assertRaises(
                bnl01_bot.LocalModelBudgetExhausted
            ):
                bnl01_bot.reserve_local_model_budget(prompt, "normal_chat")
            bnl01_bot.release_local_model_budget(first)

        self.assertEqual(bnl01_bot._token_budget_reserved_tokens, 0)


    def test_failed_provider_attempt_is_ledgered_without_incrementing_zero_usage(self):
        error = Exception("503 service unavailable")
        with mock.patch.object(
            bnl01_bot,
            "_pacific_usage_date",
            return_value="2026-07-26",
        ):
            recorded = bnl01_bot.record_failed_generation_attempt(
                error,
                route="normal_chat",
                model="gemini-3.6-flash",
            )
            diagnostics = bnl01_bot.get_usage_breakdown()
        self.assertEqual(recorded, 0)
        self.assertEqual(diagnostics["total_tokens"], 0)
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                """
                SELECT route, model, outcome, error_category,
                       provider_status_code, total_tokens
                FROM model_generation_attempts
                """
            ).fetchone()
        self.assertEqual(
            row,
            (
                "normal_chat",
                "gemini-3.6-flash",
                "failure",
                "server",
                503,
                0,
            ),
        )

    def test_journal_capacity_is_protected_from_ordinary_routes(self):
        today = "2026-07-26"
        ordinary_ceiling = bnl01_bot.budget_ceiling_for_route(
            bnl01_bot.DAILY_TOKEN_LIMIT,
            "normal_chat",
        )
        with sqlite3.connect(self.db_path) as conn:
            bnl01_bot._ensure_token_usage_schema(conn.cursor())
            conn.execute(
                """
                UPDATE token_usage
                SET tokens_used_today = ?, last_reset_date = ?
                WHERE id = 1
                """,
                (ordinary_ceiling - 10, today),
            )
        with mock.patch.object(
            bnl01_bot,
            "_pacific_usage_date",
            return_value=today,
        ):
            with self.assertRaises(bnl01_bot.LocalModelBudgetExhausted):
                bnl01_bot.reserve_local_model_budget(
                    "small prompt",
                    "normal_chat",
                )
            journal_reservation = bnl01_bot.reserve_local_model_budget(
                "small prompt",
                bnl01_bot.JOURNAL_ROUTE,
            )
            bnl01_bot.release_local_model_budget(journal_reservation)
        self.assertEqual(bnl01_bot._token_budget_reserved_tokens, 0)

    def test_gemini3_journal_config_preserves_large_output_without_legacy_cap(self):
        config = bnl01_bot._generation_config_for_model(
            "gemini-3.6-flash",
            bnl01_bot.JOURNAL_ROUTE,
        )
        self.assertEqual(
            config.max_output_tokens,
            bnl01_bot.policy_for_route(
                bnl01_bot.JOURNAL_ROUTE
            ).max_output_tokens,
        )
        self.assertIsNone(config.thinking_config)


if __name__ == "__main__":
    unittest.main()
