"""Pure Gemini pricing and Pacific budget-calendar helpers for BNL-01.

The provider usage metadata reports prompt tokens inclusive of cached input.
Cost calculation therefore prices only ``prompt - cached`` at the normal input
rate, the cached subset at the cached-input rate, and candidate plus thinking
tokens at the output rate.

Pricing overrides may be supplied through ``BNL_GEMINI_PRICING_OVERRIDES_JSON``::

    {
      "gemini-3.6-flash": {
        "input_usd_per_million": "0.75",
        "output_usd_per_million": "3.75",
        "cached_input_usd_per_million": "0.075"
      }
    }

An override is exact-model only after removing an optional ``models/`` prefix.
Malformed model entries are ignored. Unknown models remain explicitly unpriced
unless a complete environment override provides all three rates.
"""

from __future__ import annotations

import calendar
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from decimal import Decimal, InvalidOperation, ROUND_CEILING, localcontext
import json
import os
from typing import Mapping, Optional, Union
from zoneinfo import ZoneInfo


PACIFIC_TIMEZONE_NAME = "America/Los_Angeles"
PACIFIC_TZ = ZoneInfo(PACIFIC_TIMEZONE_NAME)
PRICING_VERSION = "gemini_standard_2026-08-22"
PRICING_OVERRIDES_ENV = "BNL_GEMINI_PRICING_OVERRIDES_JSON"
UNPRICED_GUARDRAIL_RATE_ENV = (
    "BNL_GEMINI_UNPRICED_GUARDRAIL_USD_PER_MILLION"
)
DEFAULT_UNPRICED_GUARDRAIL_USD_PER_MILLION = Decimal("9.00")
TOKENS_PER_MILLION = Decimal("1000000")
NANODOLLARS_PER_USD = Decimal("1000000000")


@dataclass(frozen=True)
class GeminiModelPrice:
    """Standard paid-tier token rates in USD per one million tokens."""

    model: str
    input_usd_per_million: Decimal
    output_usd_per_million: Decimal
    cached_input_usd_per_million: Decimal
    effective_from: Optional[date] = None
    effective_through: Optional[date] = None
    source: str = "built_in"

    def applies_on(self, usage_date: date) -> bool:
        return bool(
            (self.effective_from is None or usage_date >= self.effective_from)
            and (
                self.effective_through is None
                or usage_date <= self.effective_through
            )
        )


@dataclass(frozen=True)
class GeminiTokenUsage:
    """Raw provider token fields retained independently from priced fields."""

    prompt_tokens: int = 0
    candidate_tokens: int = 0
    thought_tokens: int = 0
    cached_tokens: int = 0
    total_tokens: int = 0

    @property
    def accounted_total_tokens(self) -> int:
        return self.prompt_tokens + self.candidate_tokens + self.thought_tokens


@dataclass(frozen=True)
class GeminiCostEstimate:
    """A component-level cost estimate, or an explicit unpriced result."""

    requested_model: str
    normalized_model: str
    usage: GeminiTokenUsage
    priced: bool
    price: Optional[GeminiModelPrice] = None
    uncached_prompt_tokens: int = 0
    billable_cached_tokens: int = 0
    unclassified_tokens: int = 0
    input_cost_usd: Optional[Decimal] = None
    candidate_cost_usd: Optional[Decimal] = None
    thinking_cost_usd: Optional[Decimal] = None
    cached_input_cost_usd: Optional[Decimal] = None
    unclassified_cost_usd: Optional[Decimal] = None
    estimated_cost_usd: Optional[Decimal] = None

    @property
    def estimated_cost_nanos(self) -> Optional[int]:
        """Return a conservative integer snapshot suitable for a DB ledger."""

        if self.estimated_cost_usd is None:
            return None
        return int(
            (self.estimated_cost_usd * NANODOLLARS_PER_USD).to_integral_value(
                rounding=ROUND_CEILING
            )
        )


@dataclass(frozen=True)
class GeminiBudgetConfig:
    monthly_target_usd: Decimal
    monthly_hard_limit_usd: Decimal
    daily_soft_limit_usd: Decimal
    enforcement_enabled: bool


@dataclass(frozen=True)
class PacificBudgetClock:
    at_pacific: datetime
    usage_date: date
    month_key: str
    month_start: date
    next_month_start: date
    days_in_month: int
    day_of_month: int
    days_remaining_including_today: int
    next_daily_reset_at: datetime
    next_monthly_reset_at: datetime


@dataclass(frozen=True)
class MonthlyBudgetPace:
    clock: PacificBudgetClock
    month_cost_usd: Decimal
    today_cost_usd: Decimal
    expected_cost_to_date_usd: Decimal
    average_cost_per_elapsed_day_usd: Decimal
    projected_month_end_cost_usd: Decimal
    remaining_target_usd: Decimal
    remaining_hard_limit_usd: Decimal
    remaining_to_current_pace_usd: Decimal
    remaining_daily_soft_limit_usd: Decimal
    over_target_pace: bool
    target_exhausted: bool
    hard_limit_exhausted: bool


# Google Gemini Developer API Standard paid-tier rates. Gemini 3.6 Flash has a
# published price transition on 2027-01-01; retaining both periods prevents a
# silent stale-price estimate when a record crosses that boundary.
BUILT_IN_STANDARD_PRICING = {
    "gemini-3.6-flash": (
        GeminiModelPrice(
            model="gemini-3.6-flash",
            input_usd_per_million=Decimal("0.75"),
            output_usd_per_million=Decimal("3.75"),
            cached_input_usd_per_million=Decimal("0.075"),
            effective_through=date(2026, 12, 31),
        ),
        GeminiModelPrice(
            model="gemini-3.6-flash",
            input_usd_per_million=Decimal("1.50"),
            output_usd_per_million=Decimal("7.50"),
            cached_input_usd_per_million=Decimal("0.15"),
            effective_from=date(2027, 1, 1),
        ),
    ),
    "gemini-3.5-flash": (
        GeminiModelPrice(
            model="gemini-3.5-flash",
            input_usd_per_million=Decimal("1.50"),
            output_usd_per_million=Decimal("9.00"),
            cached_input_usd_per_million=Decimal("0.15"),
        ),
    ),
}


def normalize_model_name(model: object) -> str:
    """Normalize only the provider's optional resource-name prefix."""

    normalized = str(model or "").strip().lower()
    if normalized.startswith("models/"):
        normalized = normalized[len("models/") :]
    return normalized


def _nonnegative_int(value: object) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError, OverflowError):
        return 0


def _nonnegative_decimal(value: object) -> Optional[Decimal]:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    if not parsed.is_finite() or parsed < 0:
        return None
    return parsed


def _coerce_pacific_datetime(
    at: Optional[Union[date, datetime]] = None,
) -> datetime:
    if at is None:
        return datetime.now(PACIFIC_TZ)
    if isinstance(at, datetime):
        if at.tzinfo is None:
            return at.replace(tzinfo=PACIFIC_TZ)
        return at.astimezone(PACIFIC_TZ)
    if isinstance(at, date):
        return datetime.combine(at, time.min, tzinfo=PACIFIC_TZ)
    raise TypeError("at must be a date, datetime, or None")


def _coerce_pacific_date(
    at: Optional[Union[date, datetime]] = None,
) -> date:
    return _coerce_pacific_datetime(at).date()


def _environment(environ: Optional[Mapping[str, str]]) -> Mapping[str, str]:
    return os.environ if environ is None else environ


def _pricing_overrides(
    environ: Optional[Mapping[str, str]] = None,
) -> Mapping[str, GeminiModelPrice]:
    raw = str(_environment(environ).get(PRICING_OVERRIDES_ENV, "") or "").strip()
    if not raw:
        return {}
    try:
        payload = json.loads(raw)
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    if not isinstance(payload, dict):
        return {}

    overrides = {}
    for raw_model, raw_rates in payload.items():
        model = normalize_model_name(raw_model)
        if not model or not isinstance(raw_rates, dict):
            continue
        input_rate = _nonnegative_decimal(
            raw_rates.get("input_usd_per_million")
        )
        output_rate = _nonnegative_decimal(
            raw_rates.get("output_usd_per_million")
        )
        cached_rate = _nonnegative_decimal(
            raw_rates.get("cached_input_usd_per_million")
        )
        # A zero-rate override would make a configured model look priced while
        # silently disabling its guardrail. Provider prices can be changed by
        # environment, but every component must remain explicitly positive.
        if (
            input_rate is None
            or output_rate is None
            or cached_rate is None
            or input_rate <= 0
            or output_rate <= 0
            or cached_rate <= 0
        ):
            continue
        overrides[model] = GeminiModelPrice(
            model=model,
            input_usd_per_million=input_rate,
            output_usd_per_million=output_rate,
            cached_input_usd_per_million=cached_rate,
            source="environment",
        )
    return overrides


def price_for_model(
    model: object,
    *,
    at: Optional[Union[date, datetime]] = None,
    environ: Optional[Mapping[str, str]] = None,
) -> Optional[GeminiModelPrice]:
    """Return an exact-model Standard price or ``None`` when unpriced."""

    normalized = normalize_model_name(model)
    override = _pricing_overrides(environ).get(normalized)
    if override is not None:
        return override
    usage_date = _coerce_pacific_date(at)
    for price in BUILT_IN_STANDARD_PRICING.get(normalized, ()):
        if price.applies_on(usage_date):
            return price
    return None


def _component_cost(tokens: int, rate_per_million: Decimal) -> Decimal:
    with localcontext() as context:
        context.prec = 32
        return Decimal(tokens) * rate_per_million / TOKENS_PER_MILLION


def conservative_unpriced_guardrail_cost_nanos(
    total_tokens: object,
    *,
    environ: Optional[Mapping[str, str]] = None,
) -> int:
    """Reserve a conservative amount for historical unpriced usage.

    The usage remains explicitly *unpriced* in diagnostics. This separate
    guardrail amount prevents those raw tokens from being treated as free while
    avoiding a month-long interactive outage caused by one legacy model name.
    """

    raw_rate = _environment(environ).get(
        UNPRICED_GUARDRAIL_RATE_ENV,
        str(DEFAULT_UNPRICED_GUARDRAIL_USD_PER_MILLION),
    )
    rate = _nonnegative_decimal(raw_rate)
    if rate is None or rate <= 0:
        rate = DEFAULT_UNPRICED_GUARDRAIL_USD_PER_MILLION
    cost = _component_cost(_nonnegative_int(total_tokens), rate)
    return int(
        (cost * NANODOLLARS_PER_USD).to_integral_value(
            rounding=ROUND_CEILING
        )
    )


def estimate_gemini_cost(
    model: object,
    *,
    prompt_tokens: object = 0,
    candidate_tokens: object = 0,
    thought_tokens: object = 0,
    cached_tokens: object = 0,
    total_tokens: object = 0,
    at: Optional[Union[date, datetime]] = None,
    environ: Optional[Mapping[str, str]] = None,
) -> GeminiCostEstimate:
    """Estimate Standard API cost while retaining every raw token component.

    Any positive difference between the reported total and its documented
    prompt/candidate/thought components is conservatively charged at the higher
    of the selected input and output rates. This avoids silently underpricing
    malformed or newly extended provider metadata.
    """

    usage = GeminiTokenUsage(
        prompt_tokens=_nonnegative_int(prompt_tokens),
        candidate_tokens=_nonnegative_int(candidate_tokens),
        thought_tokens=_nonnegative_int(thought_tokens),
        cached_tokens=_nonnegative_int(cached_tokens),
        total_tokens=_nonnegative_int(total_tokens),
    )
    normalized = normalize_model_name(model)
    billable_cached = min(usage.prompt_tokens, usage.cached_tokens)
    uncached_prompt = max(0, usage.prompt_tokens - billable_cached)
    unclassified = max(0, usage.total_tokens - usage.accounted_total_tokens)
    price = price_for_model(model, at=at, environ=environ)
    if price is None:
        return GeminiCostEstimate(
            requested_model=str(model or ""),
            normalized_model=normalized,
            usage=usage,
            priced=False,
            uncached_prompt_tokens=uncached_prompt,
            billable_cached_tokens=billable_cached,
            unclassified_tokens=unclassified,
        )

    input_cost = _component_cost(
        uncached_prompt,
        price.input_usd_per_million,
    )
    candidate_cost = _component_cost(
        usage.candidate_tokens,
        price.output_usd_per_million,
    )
    thinking_cost = _component_cost(
        usage.thought_tokens,
        price.output_usd_per_million,
    )
    cached_cost = _component_cost(
        billable_cached,
        price.cached_input_usd_per_million,
    )
    conservative_rate = max(
        price.input_usd_per_million,
        price.output_usd_per_million,
    )
    unclassified_cost = _component_cost(unclassified, conservative_rate)
    total_cost = (
        input_cost
        + candidate_cost
        + thinking_cost
        + cached_cost
        + unclassified_cost
    )
    return GeminiCostEstimate(
        requested_model=str(model or ""),
        normalized_model=normalized,
        usage=usage,
        priced=True,
        price=price,
        uncached_prompt_tokens=uncached_prompt,
        billable_cached_tokens=billable_cached,
        unclassified_tokens=unclassified,
        input_cost_usd=input_cost,
        candidate_cost_usd=candidate_cost,
        thinking_cost_usd=thinking_cost,
        cached_input_cost_usd=cached_cost,
        unclassified_cost_usd=unclassified_cost,
        estimated_cost_usd=total_cost,
    )


def _budget_decimal(
    environ: Mapping[str, str],
    name: str,
    default: str,
    *,
    strictly_positive: bool = False,
) -> Decimal:
    parsed = _nonnegative_decimal(environ.get(name, default))
    if parsed is None or (strictly_positive and parsed <= 0):
        return Decimal(default)
    return parsed


def _environment_bool(
    environ: Mapping[str, str],
    name: str,
    default: bool,
) -> bool:
    raw = str(environ.get(name, "true" if default else "false") or "").strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return default


def load_budget_config(
    environ: Optional[Mapping[str, str]] = None,
) -> GeminiBudgetConfig:
    """Load fail-safe dollar guardrail settings without touching global state."""

    source = _environment(environ)
    hard_limit = _budget_decimal(
        source,
        "BNL_GEMINI_MONTHLY_HARD_LIMIT_USD",
        "24.00",
    )
    target = _budget_decimal(
        source,
        "BNL_GEMINI_MONTHLY_TARGET_USD",
        "20.00",
    )
    daily_soft = _budget_decimal(
        source,
        "BNL_GEMINI_DAILY_SOFT_LIMIT_USD",
        "0.65",
    )
    # Invalid relationships clamp toward less spend, never above the hard cap.
    target = min(target, hard_limit)
    daily_soft = min(daily_soft, hard_limit)
    return GeminiBudgetConfig(
        monthly_target_usd=target,
        monthly_hard_limit_usd=hard_limit,
        daily_soft_limit_usd=daily_soft,
        enforcement_enabled=_environment_bool(
            source,
            "BNL_GEMINI_BUDGET_ENFORCEMENT_ENABLED",
            True,
        ),
    )


def pacific_budget_clock(
    at: Optional[Union[date, datetime]] = None,
) -> PacificBudgetClock:
    """Describe current Pacific daily/monthly budget periods and resets."""

    current = _coerce_pacific_datetime(at)
    usage_day = current.date()
    month_start = usage_day.replace(day=1)
    days = calendar.monthrange(usage_day.year, usage_day.month)[1]
    if usage_day.month == 12:
        next_month_start = date(usage_day.year + 1, 1, 1)
    else:
        next_month_start = date(usage_day.year, usage_day.month + 1, 1)
    next_daily_date = usage_day + timedelta(days=1)
    return PacificBudgetClock(
        at_pacific=current,
        usage_date=usage_day,
        month_key=usage_day.strftime("%Y-%m"),
        month_start=month_start,
        next_month_start=next_month_start,
        days_in_month=days,
        day_of_month=usage_day.day,
        days_remaining_including_today=days - usage_day.day + 1,
        next_daily_reset_at=datetime.combine(
            next_daily_date,
            time.min,
            tzinfo=PACIFIC_TZ,
        ),
        next_monthly_reset_at=datetime.combine(
            next_month_start,
            time.min,
            tzinfo=PACIFIC_TZ,
        ),
    )


def projected_month_end_cost(
    month_cost_usd: object,
    *,
    at: Optional[Union[date, datetime]] = None,
) -> Decimal:
    """Project spend using elapsed Pacific calendar days including today."""

    spent = _nonnegative_decimal(month_cost_usd) or Decimal("0")
    clock = pacific_budget_clock(at)
    with localcontext() as context:
        context.prec = 32
        return spent * Decimal(clock.days_in_month) / Decimal(clock.day_of_month)


def calculate_monthly_budget_pace(
    month_cost_usd: object,
    today_cost_usd: object,
    *,
    config: Optional[GeminiBudgetConfig] = None,
    at: Optional[Union[date, datetime]] = None,
    environ: Optional[Mapping[str, str]] = None,
) -> MonthlyBudgetPace:
    """Calculate a cumulative monthly pace without assuming a 31-day month."""

    active_config = config or load_budget_config(environ)
    month_cost = _nonnegative_decimal(month_cost_usd) or Decimal("0")
    today_cost = _nonnegative_decimal(today_cost_usd) or Decimal("0")
    clock = pacific_budget_clock(at)
    with localcontext() as context:
        context.prec = 32
        expected = (
            active_config.monthly_target_usd
            * Decimal(clock.day_of_month)
            / Decimal(clock.days_in_month)
        )
        average = month_cost / Decimal(clock.day_of_month)
        projected = average * Decimal(clock.days_in_month)
    remaining_target = max(
        Decimal("0"),
        active_config.monthly_target_usd - month_cost,
    )
    remaining_hard = max(
        Decimal("0"),
        active_config.monthly_hard_limit_usd - month_cost,
    )
    return MonthlyBudgetPace(
        clock=clock,
        month_cost_usd=month_cost,
        today_cost_usd=today_cost,
        expected_cost_to_date_usd=expected,
        average_cost_per_elapsed_day_usd=average,
        projected_month_end_cost_usd=projected,
        remaining_target_usd=remaining_target,
        remaining_hard_limit_usd=remaining_hard,
        remaining_to_current_pace_usd=max(Decimal("0"), expected - month_cost),
        remaining_daily_soft_limit_usd=max(
            Decimal("0"),
            active_config.daily_soft_limit_usd - today_cost,
        ),
        over_target_pace=month_cost > expected,
        target_exhausted=month_cost >= active_config.monthly_target_usd,
        hard_limit_exhausted=(
            month_cost >= active_config.monthly_hard_limit_usd
        ),
    )
