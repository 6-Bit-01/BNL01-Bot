import json
import unittest
from datetime import date, datetime, timezone
from decimal import Decimal

import bnl_gemini_cost as cost


class GeminiPricingTests(unittest.TestCase):
    def test_pricing_table_has_a_stable_audit_version(self):
        self.assertEqual(
            cost.PRICING_VERSION,
            "gemini_standard_2026-08-22",
        )

    def test_current_gemini_36_standard_rates(self):
        price = cost.price_for_model(
            "gemini-3.6-flash",
            at=date(2026, 8, 22),
            environ={},
        )

        self.assertIsNotNone(price)
        self.assertEqual(price.input_usd_per_million, Decimal("0.75"))
        self.assertEqual(price.output_usd_per_million, Decimal("3.75"))
        self.assertEqual(
            price.cached_input_usd_per_million,
            Decimal("0.075"),
        )
        self.assertEqual(price.effective_through, date(2026, 12, 31))

    def test_gemini_36_published_2027_rate_transition(self):
        before = cost.price_for_model(
            "gemini-3.6-flash",
            at=date(2026, 12, 31),
            environ={},
        )
        after = cost.price_for_model(
            "gemini-3.6-flash",
            at=date(2027, 1, 1),
            environ={},
        )

        self.assertEqual(before.input_usd_per_million, Decimal("0.75"))
        self.assertEqual(after.input_usd_per_million, Decimal("1.50"))
        self.assertEqual(after.output_usd_per_million, Decimal("7.50"))
        self.assertEqual(
            after.cached_input_usd_per_million,
            Decimal("0.15"),
        )

    def test_gemini_35_standard_rates(self):
        price = cost.price_for_model(
            "gemini-3.5-flash",
            at=date(2026, 8, 22),
            environ={},
        )

        self.assertEqual(price.input_usd_per_million, Decimal("1.50"))
        self.assertEqual(price.output_usd_per_million, Decimal("9.00"))
        self.assertEqual(
            price.cached_input_usd_per_million,
            Decimal("0.15"),
        )

    def test_models_resource_prefix_is_normalized(self):
        estimate = cost.estimate_gemini_cost(
            " models/GEMINI-3.6-FLASH ",
            prompt_tokens=1_000,
            at=date(2026, 8, 22),
            environ={},
        )

        self.assertTrue(estimate.priced)
        self.assertEqual(estimate.normalized_model, "gemini-3.6-flash")
        self.assertEqual(estimate.estimated_cost_usd, Decimal("0.00075"))

    def test_input_tokens_use_input_rate(self):
        estimate = cost.estimate_gemini_cost(
            "gemini-3.6-flash",
            prompt_tokens=10_000,
            total_tokens=10_000,
            at=date(2026, 8, 22),
            environ={},
        )

        self.assertEqual(estimate.input_cost_usd, Decimal("0.0075"))
        self.assertEqual(estimate.estimated_cost_usd, Decimal("0.0075"))
        self.assertEqual(estimate.estimated_cost_nanos, 7_500_000)

    def test_candidate_tokens_use_output_rate(self):
        estimate = cost.estimate_gemini_cost(
            "gemini-3.6-flash",
            candidate_tokens=1_000,
            total_tokens=1_000,
            at=date(2026, 8, 22),
            environ={},
        )

        self.assertEqual(estimate.candidate_cost_usd, Decimal("0.00375"))
        self.assertEqual(estimate.estimated_cost_usd, Decimal("0.00375"))

    def test_thinking_tokens_use_output_rate(self):
        estimate = cost.estimate_gemini_cost(
            "gemini-3.6-flash",
            thought_tokens=2_000,
            total_tokens=2_000,
            at=date(2026, 8, 22),
            environ={},
        )

        self.assertEqual(estimate.thinking_cost_usd, Decimal("0.00750"))
        self.assertEqual(estimate.estimated_cost_usd, Decimal("0.00750"))

    def test_cached_tokens_are_subtracted_from_prompt_and_priced_separately(self):
        estimate = cost.estimate_gemini_cost(
            "gemini-3.6-flash",
            prompt_tokens=1_000,
            cached_tokens=400,
            total_tokens=1_000,
            at=date(2026, 8, 22),
            environ={},
        )

        self.assertEqual(estimate.uncached_prompt_tokens, 600)
        self.assertEqual(estimate.billable_cached_tokens, 400)
        self.assertEqual(estimate.input_cost_usd, Decimal("0.00045"))
        self.assertEqual(
            estimate.cached_input_cost_usd,
            Decimal("0.0000300"),
        )
        self.assertEqual(estimate.estimated_cost_usd, Decimal("0.0004800"))

    def test_failed_attempt_usage_can_be_priced_without_special_casing(self):
        failed_usage = cost.estimate_gemini_cost(
            "gemini-3.5-flash",
            prompt_tokens=2_000,
            candidate_tokens=100,
            thought_tokens=300,
            total_tokens=2_400,
            at=date(2026, 8, 22),
            environ={},
        )

        self.assertTrue(failed_usage.priced)
        self.assertEqual(failed_usage.input_cost_usd, Decimal("0.00300"))
        self.assertEqual(failed_usage.candidate_cost_usd, Decimal("0.000900"))
        self.assertEqual(failed_usage.thinking_cost_usd, Decimal("0.002700"))
        self.assertEqual(failed_usage.estimated_cost_usd, Decimal("0.006600"))

    def test_supplied_production_snapshot_reconciles_to_current_estimate(self):
        estimate = cost.estimate_gemini_cost(
            "gemini-3.6-flash",
            prompt_tokens=282_387,
            candidate_tokens=3_896,
            thought_tokens=87_123,
            cached_tokens=0,
            total_tokens=373_406,
            at=date(2026, 8, 22),
            environ={},
        )

        self.assertEqual(
            estimate.estimated_cost_usd,
            Decimal("0.5531115"),
        )

    def test_unknown_model_is_unpriced_not_zero_and_retains_raw_usage(self):
        estimate = cost.estimate_gemini_cost(
            "gemini-future-unknown",
            prompt_tokens=111,
            candidate_tokens=22,
            thought_tokens=33,
            cached_tokens=44,
            total_tokens=170,
            at=date(2026, 8, 22),
            environ={},
        )

        self.assertFalse(estimate.priced)
        self.assertIsNone(estimate.price)
        self.assertIsNone(estimate.estimated_cost_usd)
        self.assertIsNone(estimate.estimated_cost_nanos)
        self.assertEqual(
            estimate.usage,
            cost.GeminiTokenUsage(
                prompt_tokens=111,
                candidate_tokens=22,
                thought_tokens=33,
                cached_tokens=44,
                total_tokens=170,
            ),
        )
        self.assertEqual(estimate.unclassified_tokens, 4)

    def test_unclassified_reported_tokens_are_conservatively_priced(self):
        estimate = cost.estimate_gemini_cost(
            "gemini-3.6-flash",
            prompt_tokens=100,
            total_tokens=110,
            at=date(2026, 8, 22),
            environ={},
        )

        self.assertEqual(estimate.unclassified_tokens, 10)
        self.assertEqual(
            estimate.unclassified_cost_usd,
            Decimal("0.0000375"),
        )
        self.assertEqual(estimate.usage.total_tokens, 110)

    def test_complete_environment_override_replaces_builtin_or_prices_new_model(self):
        override = {
            "models/gemini-3.6-flash": {
                "input_usd_per_million": "2.0",
                "output_usd_per_million": "4.0",
                "cached_input_usd_per_million": "0.2",
            },
            "gemini-future-model": {
                "input_usd_per_million": "1.25",
                "output_usd_per_million": "5.5",
                "cached_input_usd_per_million": "0.125",
            },
        }
        environ = {
            cost.PRICING_OVERRIDES_ENV: json.dumps(override),
        }

        replaced = cost.price_for_model(
            "gemini-3.6-flash",
            at=date(2026, 8, 22),
            environ=environ,
        )
        added = cost.price_for_model(
            "models/gemini-future-model",
            at=date(2026, 8, 22),
            environ=environ,
        )

        self.assertEqual(replaced.input_usd_per_million, Decimal("2.0"))
        self.assertEqual(replaced.source, "environment")
        self.assertEqual(added.output_usd_per_million, Decimal("5.5"))
        self.assertEqual(added.source, "environment")

    def test_malformed_override_fails_safely(self):
        incomplete = {
            "gemini-future-model": {
                "input_usd_per_million": "1.0",
            }
        }
        environ = {cost.PRICING_OVERRIDES_ENV: json.dumps(incomplete)}

        self.assertIsNone(
            cost.price_for_model(
                "gemini-future-model",
                at=date(2026, 8, 22),
                environ=environ,
            )
        )
        built_in = cost.price_for_model(
            "gemini-3.6-flash",
            at=date(2026, 8, 22),
            environ={cost.PRICING_OVERRIDES_ENV: "not-json"},
        )
        self.assertEqual(built_in.input_usd_per_million, Decimal("0.75"))

    def test_zero_rate_override_is_rejected_instead_of_pricing_calls_at_zero(self):
        override = {
            "gemini-future-model": {
                "input_usd_per_million": "0",
                "output_usd_per_million": "0",
                "cached_input_usd_per_million": "0",
            }
        }
        environ = {
            cost.PRICING_OVERRIDES_ENV: json.dumps(override),
        }

        self.assertIsNone(
            cost.price_for_model(
                "gemini-future-model",
                at=date(2026, 8, 22),
                environ=environ,
            )
        )

    def test_unpriced_guardrail_reserve_uses_conservative_nonzero_rate(self):
        self.assertEqual(
            cost.conservative_unpriced_guardrail_cost_nanos(
                1_000_000,
                environ={},
            ),
            9_000_000_000,
        )
        self.assertEqual(
            cost.conservative_unpriced_guardrail_cost_nanos(
                1_000_000,
                environ={cost.UNPRICED_GUARDRAIL_RATE_ENV: "0"},
            ),
            9_000_000_000,
        )


class GeminiBudgetCalendarTests(unittest.TestCase):
    def test_budget_config_defaults(self):
        config = cost.load_budget_config({})

        self.assertEqual(config.monthly_target_usd, Decimal("20.00"))
        self.assertEqual(config.monthly_hard_limit_usd, Decimal("24.00"))
        self.assertEqual(config.daily_soft_limit_usd, Decimal("0.65"))
        self.assertTrue(config.enforcement_enabled)

    def test_budget_config_environment_override_and_safe_clamping(self):
        config = cost.load_budget_config(
            {
                "BNL_GEMINI_MONTHLY_TARGET_USD": "30",
                "BNL_GEMINI_MONTHLY_HARD_LIMIT_USD": "22",
                "BNL_GEMINI_DAILY_SOFT_LIMIT_USD": "99",
                "BNL_GEMINI_BUDGET_ENFORCEMENT_ENABLED": "off",
            }
        )

        self.assertEqual(config.monthly_target_usd, Decimal("22"))
        self.assertEqual(config.monthly_hard_limit_usd, Decimal("22"))
        self.assertEqual(config.daily_soft_limit_usd, Decimal("22"))
        self.assertFalse(config.enforcement_enabled)

    def test_zero_target_and_hard_limit_are_valid_emergency_stop_values(self):
        config = cost.load_budget_config(
            {
                "BNL_GEMINI_MONTHLY_TARGET_USD": "0",
                "BNL_GEMINI_MONTHLY_HARD_LIMIT_USD": "0",
                "BNL_GEMINI_DAILY_SOFT_LIMIT_USD": "0",
            }
        )

        self.assertEqual(config.monthly_target_usd, Decimal("0"))
        self.assertEqual(config.monthly_hard_limit_usd, Decimal("0"))
        self.assertEqual(config.daily_soft_limit_usd, Decimal("0"))

    def test_pacific_daily_reset_occurs_at_midnight_not_utc_midnight(self):
        before = cost.pacific_budget_clock(
            datetime(2027, 1, 1, 7, 59, tzinfo=timezone.utc)
        )
        after = cost.pacific_budget_clock(
            datetime(2027, 1, 1, 8, 0, tzinfo=timezone.utc)
        )

        self.assertEqual(before.usage_date, date(2026, 12, 31))
        self.assertEqual(before.month_key, "2026-12")
        self.assertEqual(after.usage_date, date(2027, 1, 1))
        self.assertEqual(after.month_key, "2027-01")

    def test_monthly_reset_rolls_december_into_next_year(self):
        clock = cost.pacific_budget_clock(date(2026, 12, 31))

        self.assertEqual(clock.month_start, date(2026, 12, 1))
        self.assertEqual(clock.next_month_start, date(2027, 1, 1))
        self.assertEqual(
            clock.next_monthly_reset_at,
            datetime(2027, 1, 1, 0, 0, tzinfo=cost.PACIFIC_TZ),
        )
        self.assertEqual(clock.days_remaining_including_today, 1)

    def test_february_and_leap_year_month_lengths(self):
        ordinary = cost.pacific_budget_clock(date(2027, 2, 14))
        leap = cost.pacific_budget_clock(date(2028, 2, 14))

        self.assertEqual(ordinary.days_in_month, 28)
        self.assertEqual(ordinary.next_month_start, date(2027, 3, 1))
        self.assertEqual(leap.days_in_month, 29)
        self.assertEqual(leap.next_month_start, date(2028, 3, 1))

    def test_projection_uses_actual_active_month_length(self):
        february_projection = cost.projected_month_end_cost(
            "10",
            at=date(2027, 2, 14),
        )
        leap_projection = cost.projected_month_end_cost(
            "10",
            at=date(2028, 2, 14),
        )

        self.assertEqual(february_projection, Decimal("20"))
        self.assertEqual(
            leap_projection,
            Decimal("20.714285714285714285714285714286"),
        )

    def test_monthly_pace_banks_underspend_and_reports_restriction_signals(self):
        config = cost.GeminiBudgetConfig(
            monthly_target_usd=Decimal("20"),
            monthly_hard_limit_usd=Decimal("24"),
            daily_soft_limit_usd=Decimal("0.65"),
            enforcement_enabled=True,
        )
        pace = cost.calculate_monthly_budget_pace(
            "8",
            "0.20",
            config=config,
            at=date(2027, 2, 14),
        )

        self.assertEqual(pace.expected_cost_to_date_usd, Decimal("10"))
        self.assertEqual(pace.remaining_to_current_pace_usd, Decimal("2"))
        self.assertEqual(
            pace.remaining_daily_soft_limit_usd,
            Decimal("0.45"),
        )
        self.assertEqual(pace.projected_month_end_cost_usd, Decimal("16"))
        self.assertEqual(pace.remaining_target_usd, Decimal("12"))
        self.assertEqual(pace.remaining_hard_limit_usd, Decimal("16"))
        self.assertFalse(pace.over_target_pace)
        self.assertFalse(pace.target_exhausted)
        self.assertFalse(pace.hard_limit_exhausted)

    def test_monthly_pace_marks_target_and_hard_limit_exhaustion(self):
        target = cost.calculate_monthly_budget_pace(
            "21",
            "1",
            at=date(2026, 8, 22),
            environ={},
        )
        hard = cost.calculate_monthly_budget_pace(
            "24",
            "1",
            at=date(2026, 8, 22),
            environ={},
        )

        self.assertTrue(target.target_exhausted)
        self.assertFalse(target.hard_limit_exhausted)
        self.assertTrue(target.over_target_pace)
        self.assertTrue(hard.hard_limit_exhausted)
        self.assertEqual(hard.remaining_hard_limit_usd, Decimal("0"))


if __name__ == "__main__":
    unittest.main()
