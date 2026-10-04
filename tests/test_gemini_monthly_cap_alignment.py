"""Exercise the saved budget owners without importing or starting the bot."""

import ast
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from functools import partial
import logging
import os
from pathlib import Path
import sqlite3 as native_sqlite
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock
import uuid

import bnl_gemini_cost as cost
from bnl_gemini_routing import budget_ceiling_for_route, policy_for_route


class ClosingConnection(native_sqlite.Connection):
    def __exit__(self, *args):
        # Preserve SQLite's real commit/rollback behavior, and deterministically
        # close temporary-file connections on Windows instead of awaiting GC.
        try:
            return super().__exit__(*args)
        finally:
            self.close()


sqlite3 = SimpleNamespace(
    connect=partial(native_sqlite.connect, factory=ClosingConnection),
    OperationalError=native_sqlite.OperationalError,
)


SOURCE = Path(__file__).resolve().parents[1] / "bnl01_bot.py"
TREE = ast.parse(SOURCE.read_text(encoding="utf-8"))
NAMES = {
    "LocalModelBudgetExhausted", "TokenUsageBreakdown", "_usage_int",
    "_usd_to_nanos", "_nanos_to_usd", "_budget_env_usd", "_bounded_env_int",
    "_add_column_if_missing", "_ensure_token_usage_schema", "_protected_usage_lane",
    "_generation_lane_usage_on_connection", "_budget_month_event_scope",
    "_budget_reservation_expiry_scope", "_active_dollar_budget_reservations",
    "_event_cost_rollup", "_provider_attempt_route_for_usage_route",
    "_dollar_budget_decision", "_reserve_dollar_budget",
    "_retain_dollar_budget_through_month", "get_usage_breakdown",
}
OWNERS = [node for node in TREE.body
          if isinstance(node, (ast.FunctionDef, ast.ClassDef)) and node.name in NAMES]
MODULE = ast.Module(body=[ast.ImportFrom(
    module="__future__", names=[ast.alias(name="annotations")], level=0,
), *OWNERS], type_ignores=[])
CODE = compile(ast.fix_missing_locations(MODULE), str(SOURCE), "exec")
CAP_ENV = {
    "BNL_GEMINI_MONTHLY_CAP_ONLY": "true",
    "BNL_GEMINI_MONTHLY_HARD_LIMIT_USD": "30",
    "BNL_GEMINI_MONTHLY_TARGET_USD": "28",
    "BNL_GEMINI_DAILY_SOFT_LIMIT_USD": "0.65",
    "BNL_GEMINI_BILLING_LAG_BUFFER_USD": "0.50",
    "BNL_GEMINI_JOURNAL_RESERVE_USD": "1.00",
    "BNL_GEMINI_INTERACTIVE_RESERVE_USD": "2.00",
    "BNL_GEMINI_BUDGET_ENFORCEMENT_ENABLED": "true",
}


def utc(text):
    return datetime.fromisoformat(text).replace(tzinfo=timezone.utc)


class MonthlyCapAlignmentTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = str(Path(directory.name) / "budget.db")
        self.now = utc("2026-10-01T12:00:00")
        self.request_nanos = 20_000_000
        owner = self

        class BudgetDateTime(datetime):
            @classmethod
            def now(cls, tz=None):
                return owner.now.astimezone(tz) if tz else owner.now.replace(tzinfo=None)

        self.ns = dict(
            __name__=__name__, dataclass=dataclass, sqlite3=sqlite3,
            datetime=BudgetDateTime, date=date, timedelta=timedelta, timezone=timezone,
            Decimal=Decimal, os=os, uuid=uuid, logging=logging, DB_FILE=self.path,
            _NANODOLLARS_PER_USD=Decimal("1000000000"),
            load_budget_config=cost.load_budget_config,
            budget_month_window=cost.budget_month_window,
            pacific_budget_clock=cost.pacific_budget_clock,
            calculate_monthly_budget_pace=cost.calculate_monthly_budget_pace,
            conservative_unpriced_guardrail_cost_nanos=cost.conservative_unpriced_guardrail_cost_nanos,
            policy_for_route=policy_for_route,
            budget_ceiling_for_route=budget_ceiling_for_route,
            _pacific_now=lambda: self.now.astimezone(cost.PACIFIC_TZ),
            _estimated_request_cost_nanos=lambda _contents, _route: self.request_nanos,
            GEMINI_MODEL="gemini-3.6-flash", DAILY_TOKEN_LIMIT=1_000_000,
            BNL_GEMINI_JOURNAL_PROTECTED_TOKENS=250_000,
            BNL_GEMINI_RELAY_PROTECTED_TOKENS=100_000,
            ORDINARY_CHAT_SINGLE_PACKET_ROUTE="ordinary_chat_single_packet_canary",
            JOURNAL_ROUTE="journal_generation", get_usage_stats=lambda: (0, self.now.astimezone(cost.PACIFIC_TZ).date().isoformat()),
        )
        exec(CODE, self.ns)
        self.ns["_estimate_breakdown_cost"] = lambda usage, model, day: cost.estimate_gemini_cost(
            model, total_tokens=usage.total_tokens, prompt_tokens=usage.prompt_tokens,
            candidate_tokens=usage.candidate_tokens, thought_tokens=usage.thought_tokens,
            cached_tokens=usage.cached_tokens, at=date.fromisoformat(day),
        )
        with sqlite3.connect(self.path) as conn:
            self.ns["_ensure_token_usage_schema"](conn.cursor())
        patch = mock.patch.dict(os.environ, CAP_ENV, clear=True)
        patch.start()
        self.addCleanup(patch.stop)

    def window(self):
        return cost.budget_month_window(self.now)

    def event(self, when, dollars, *, route="ambient_generation.community_edition", attempt=False):
        timestamp = utc(when)
        usage_date = timestamp.astimezone(cost.PACIFIC_TZ).date().isoformat()
        with sqlite3.connect(self.path) as conn:
            conn.execute("""INSERT INTO token_usage_events
                (usage_date,recorded_at,route,model,total_tokens,estimated_cost_nanos,cost_priced)
                VALUES(?,?,?,?,?,?,1)""",
                (usage_date, timestamp.isoformat(), route, "gemini-3.6-flash", 100,
                 int(Decimal(dollars) * Decimal("1000000000"))))
            if attempt:
                conn.execute("""INSERT INTO model_generation_attempts
                    (usage_date,recorded_at,route,model,outcome,total_tokens,estimated_cost_nanos,cost_priced)
                    VALUES(?,?,?,?,?,?,?,1)""",
                    (usage_date, timestamp.isoformat(), route, "gemini-3.6-flash", "success",
                     100, int(Decimal(dollars) * Decimal("1000000000"))))

    def lease(self, name, created, expires, dollars, *, usage_month=None):
        timestamp = utc(created)
        usage_date = timestamp.astimezone(cost.PACIFIC_TZ).date().isoformat()
        with sqlite3.connect(self.path) as conn:
            conn.execute("INSERT INTO gemini_budget_reservations VALUES(?,?,?,?,?,?,?,?)",
                         (name, timestamp.isoformat(), utc(expires).isoformat(), usage_date,
                          usage_month or usage_date[:7], "ambient_generation", "background",
                          int(Decimal(dollars) * Decimal("1000000000"))))

    def rollup(self):
        window = self.window()
        with sqlite3.connect(self.path) as conn:
            return self.ns["_event_cost_rollup"](
                conn, window.month_start.isoformat(), window.next_month_start.isoformat(),
                month_window=window,
            )

    def decision(self, route="ambient_generation", *, month="5.6988", today="1.90",
                 active="0.2536", request="0.02", unpriced_calls=0, unpriced="0"):
        nanos = lambda value: int(Decimal(value) * Decimal("1000000000"))
        return self.ns["_dollar_budget_decision"](
            route=route, request_nanos=nanos(request), month_nanos=nanos(month),
            today_nanos=nanos(today), active_month_nanos=nanos(active),
            active_today_nanos=nanos(active), unpriced_calls=unpriced_calls,
            unpriced_month_guardrail_nanos=nanos(unpriced),
            unpriced_today_guardrail_nanos=nanos(unpriced), now_pacific=self.now.astimezone(cost.PACIFIC_TZ),
        )

    def test_explicit_mode_aligns_target_to_configured_cap_and_defaults_remain_paced(self):
        aligned = cost.load_budget_config(CAP_ENV)
        self.assertTrue(aligned.monthly_cap_only)
        self.assertEqual(aligned.monthly_target_usd, Decimal("30"))
        self.assertEqual(aligned.monthly_hard_limit_usd, Decimal("30"))
        default = cost.load_budget_config({})
        self.assertFalse(default.monthly_cap_only)
        self.assertEqual(default.monthly_target_usd, Decimal("20"))
        self.assertEqual(default.monthly_hard_limit_usd, Decimal("24"))
        invalid = cost.load_budget_config({"BNL_GEMINI_MONTHLY_CAP_ONLY": "invalid"})
        self.assertFalse(invalid.monthly_cap_only)

    def test_month_resets_at_fixed_pst_in_summer_and_winter_daily_clock_stays_la(self):
        for before, after, old_month, new_month in (
            ("2026-11-01T07:59:59", "2026-11-01T08:00:00", "2026-10", "2026-11"),
            ("2026-07-01T07:59:59", "2026-07-01T08:00:00", "2026-06", "2026-07"),
            ("2027-01-01T07:59:59", "2027-01-01T08:00:00", "2026-12", "2027-01"),
        ):
            with self.subTest(before=before):
                self.assertEqual(cost.budget_month_window(utc(before)).month_key, old_month)
                self.assertEqual(cost.budget_month_window(utc(after)).month_key, new_month)
        self.assertEqual(cost.budget_month_window(date(2026, 10, 1)).month_key, "2026-10")
        daily = cost.pacific_budget_clock(utc("2026-11-01T07:30:00"))
        self.assertEqual(daily.usage_date, date(2026, 11, 1))
        with mock.patch.dict(os.environ, {"BNL_GEMINI_MONTHLY_CAP_ONLY": "false"}):
            self.assertEqual(cost.budget_month_window(utc("2026-11-01T07:30:00")).month_key, "2026-11")

    def test_canonical_timestamp_month_includes_last_pdt_hour_without_rewriting_dates(self):
        self.now = utc("2026-10-15T12:00:00")
        for when, dollars in (("2026-10-01T07:59:59", "1"),
                              ("2026-10-01T08:00:00", "2"),
                              ("2026-11-01T07:59:59", "3"),
                              ("2026-11-01T08:00:00", "4")):
            self.event(when, dollars)
        self.assertEqual(self.rollup()["estimated_cost_nanos"], 5_000_000_000)
        with sqlite3.connect(self.path) as conn:
            dates = conn.execute("SELECT usage_date FROM token_usage_events ORDER BY id").fetchall()
            clause, params = self.ns["_budget_month_event_scope"](self.window())
            plan = conn.execute("EXPLAIN QUERY PLAN SELECT id FROM token_usage_events WHERE " + clause, params).fetchall()
        self.assertEqual(dates, [("2026-10-01",), ("2026-10-01",), ("2026-11-01",), ("2026-11-01",)])
        self.assertTrue(any("idx_token_usage_events_date_route" in row[3] for row in plan), plan)
        with mock.patch.dict(os.environ, {"BNL_GEMINI_MONTHLY_CAP_ONLY": "false"}):
            self.assertEqual(self.rollup()["estimated_cost_nanos"], 3_000_000_000)

    def test_early_month_background_runs_without_legacy_pace_but_safeguards_still_win(self):
        for route in ("ambient_generation", "ambient_generation.community_edition",
                      "ambient_generation.community_edition_repair"):
            with self.subTest(route=route):
                self.assertEqual(self.decision(route), (True, "monthly_cap_available"))
        self.assertEqual(self.decision("website_relay_event"), (True, "relay_protected"))
        self.assertEqual(self.decision(month="26.49", active="0", request="0.02"),
                         (False, "interactive_and_journal_reserve"))
        self.assertEqual(self.decision(month="29.49", active="0", request="0.02"),
                         (False, "monthly_hard_limit"))
        self.assertEqual(self.decision("normal_chat", month="28.49", active="0", request="0.02"),
                         (False, "journal_reserve"))
        self.assertEqual(self.decision("journal_generation", month="28.49", active="0", request="0.02"),
                         (True, "journal_protected"))
        self.assertEqual(self.decision(unpriced_calls=1), (False, "unpriced_monthly_usage"))
        self.assertEqual(self.decision(month="25.99", active="0.50", request="0.02"),
                         (False, "interactive_and_journal_reserve"))
        self.assertEqual(self.decision(month="29", active="0", unpriced="0.50"),
                         (False, "monthly_hard_limit"))
        with mock.patch.dict(os.environ, {"BNL_GEMINI_MONTHLY_CAP_ONLY": "false"}):
            self.assertEqual(self.decision(), (False, "monthly_target_pace"))

    def test_relay_allowance_and_show_protection_remain_enforced_in_cap_mode(self):
        # Day-one shared spend exceeds the retained Relay pace+5.50 allowance,
        # while the shared cap/reserves still admit generic Ambient and shows.
        self.assertEqual(self.decision("website_relay_event", month="6.45", active="0"),
                         (False, "relay_pace_allowance"))
        self.assertEqual(self.decision(month="6.45", active="0"),
                         (True, "monthly_cap_available"))
        for route in ("showday_generation", "broadcast_ballad_background"):
            with self.subTest(route=route):
                self.assertEqual(self.decision(route, month="6.45", active="0"),
                                 (True, "showday_protected"))
                self.assertEqual(self.decision(route, month="26.49", active="0"),
                                 (False, "interactive_and_journal_reserve"))
        # At LA November 1, the selected PST month remains October until 08 UTC.
        self.now = utc("2026-11-01T07:30:00")
        self.assertEqual(self.decision("website_relay_event", month="10", active="0"),
                         (True, "relay_protected"))
        self.now = utc("2026-11-01T08:00:00")
        self.assertEqual(self.decision("website_relay_event", month="10", active="0"),
                         (False, "relay_pace_allowance"))

    def test_actual_admission_waits_for_pst_reset_even_when_la_daily_month_has_reset(self):
        self.event("2026-10-31T20:00:00", "29.49")
        self.now = utc("2026-11-01T07:30:00")
        with self.assertRaisesRegex(self.ns["LocalModelBudgetExhausted"], "monthly_hard_limit"):
            self.ns["_reserve_dollar_budget"]("Test Member request", "ambient_generation")
        with sqlite3.connect(self.path) as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM gemini_budget_reservations").fetchone()[0], 0)
        self.now = utc("2026-11-01T08:00:00")
        lease, amount = self.ns["_reserve_dollar_budget"]("Test Member request", "ambient_generation")
        self.assertEqual(amount, self.request_nanos)
        with sqlite3.connect(self.path) as conn:
            self.assertEqual(conn.execute("SELECT usage_month FROM gemini_budget_reservations WHERE reservation_id=?", (lease,)).fetchone(), ("2026-11",))

    def test_old_retained_hold_survives_purge_until_pst_reset_and_ordinary_ttl_does_not(self):
        self.event("2026-10-31T20:00:00", "26.30")
        self.lease("retained", "2026-10-03T01:44:00", "2026-11-01T07:00:00", "0.25")
        self.lease("expired-ttl", "2026-10-31T23:00:00", "2026-10-31T23:30:00", "0.10")
        self.now = utc("2026-11-01T07:30:00")
        with sqlite3.connect(self.path) as conn:
            active = self.ns["_active_dollar_budget_reservations"](conn, self.window(), "2026-11-01", self.now)
        self.assertEqual(active, (250_000_000, 0))
        with self.assertRaisesRegex(self.ns["LocalModelBudgetExhausted"], "interactive_and_journal_reserve"):
            self.ns["_reserve_dollar_budget"]("Test Member request", "ambient_generation")
        with sqlite3.connect(self.path) as conn:
            self.assertEqual(conn.execute("SELECT reservation_id FROM gemini_budget_reservations").fetchall(), [("retained",)])
        with mock.patch.dict(os.environ, {"BNL_GEMINI_MONTHLY_CAP_ONLY": "false"}):
            with sqlite3.connect(self.path) as conn:
                self.assertEqual(self.ns["_active_dollar_budget_reservations"](conn, self.window(), "2026-11-01", self.now), (0, 0))
        self.now = utc("2026-11-01T08:00:00")
        lease, _ = self.ns["_reserve_dollar_budget"]("Test Member request", "ambient_generation")
        with sqlite3.connect(self.path) as conn:
            self.assertEqual(conn.execute("SELECT reservation_id FROM gemini_budget_reservations").fetchall(), [(lease,)])

    def test_current_leases_use_timestamps_across_legacy_month_keys_and_retention_uses_pst(self):
        self.now = utc("2026-11-01T07:30:00")
        self.lease("new-hour", "2026-11-01T07:10:00", "2026-11-01T07:40:00", "0.20")
        with sqlite3.connect(self.path) as conn:
            self.assertEqual(self.ns["_active_dollar_budget_reservations"](conn, self.window(), "2026-11-01", self.now), (200_000_000, 200_000_000))
        self.ns["_retain_dollar_budget_through_month"]("new-hour")
        with sqlite3.connect(self.path) as conn:
            self.assertEqual(conn.execute("SELECT expires_at FROM gemini_budget_reservations").fetchone()[0], "2026-11-01T08:00:00+00:00")

    def test_prior_month_inflight_lease_remains_outside_new_month_as_before(self):
        self.lease("prior-inflight", "2026-11-01T07:59:00", "2026-11-01T08:29:00", "0.20", usage_month="2026-10")
        self.now = utc("2026-11-01T08:00:00")
        with sqlite3.connect(self.path) as conn:
            self.assertEqual(self.ns["_active_dollar_budget_reservations"](conn, self.window(), "2026-11-01", self.now), (0, 0))

    def test_concurrent_actual_reservations_cannot_oversubscribe_background_reserves(self):
        self.event("2026-10-01T09:00:00", "26.10")
        self.request_nanos = 300_000_000

        def reserve(_index):
            try:
                return self.ns["_reserve_dollar_budget"]("Test Member request", "ambient_generation")
            except self.ns["LocalModelBudgetExhausted"]:
                return None

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(reserve, range(2)))
        self.assertEqual(sum(result is not None for result in results), 1)
        with sqlite3.connect(self.path) as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*),SUM(estimated_cost_nanos) FROM gemini_budget_reservations").fetchone(), (1, 300_000_000))

    def test_usage_diagnostics_share_the_selected_cost_attempt_and_hold_window(self):
        self.event("2026-10-01T07:59:59", "1", attempt=True)
        self.event("2026-10-01T08:00:00", "2", attempt=True)
        self.event("2026-11-01T07:10:00", "3", attempt=True)
        self.lease("retained", "2026-10-03T01:44:00", "2026-11-01T07:00:00", "0.25")
        self.now = utc("2026-11-01T07:30:00")
        diag = self.ns["get_usage_breakdown"]()
        self.assertTrue(diag["monthly_cap_only"])
        self.assertEqual(diag["budget_month"], "2026-10")
        self.assertEqual(diag["monthly_budget_timezone"], "PST (UTC-08:00)")
        self.assertEqual(diag["next_monthly_reset_at"], "2026-11-01T00:00:00-08:00")
        self.assertEqual(diag["estimated_cost_month_usd"], Decimal("5"))
        self.assertEqual(diag["estimated_cost_today_usd"], Decimal("3"))
        self.assertEqual(diag["active_reservations_month_usd"], Decimal("0.25"))
        self.assertEqual(diag["physical_attempts_month"], 2)
        self.assertEqual(diag["route_restrictions"]["background"]["reason"], "monthly_cap_available")


if __name__ == "__main__":
    unittest.main()
