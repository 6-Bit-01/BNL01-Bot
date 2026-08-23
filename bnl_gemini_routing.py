"""Route-aware Gemini model, budget, retry, and fallback policy for BNL-01.

This module contains no provider client calls. It keeps routing decisions small,
testable, and independent from Discord, SQLite, and Journal implementation code.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import os
import re


DEFAULT_PRIMARY_MODEL = "gemini-3.6-flash"
DEFAULT_FALLBACK_MODEL = "gemini-3.5-flash"


class ProviderFailureKind(str, Enum):
    RATE_LIMITED = "rate_limited"
    SERVER = "server"
    MODEL_UNAVAILABLE = "model_unavailable"
    INVALID_REQUEST = "invalid_request"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class GeminiRoutePolicy:
    lane: str
    max_output_tokens: int
    legacy_thinking_budget: int
    provider_retries: int
    allow_fallback: bool
    journal_protected: bool = False
    relay_protected: bool = False


def _bounded_env_int(
    name: str,
    default: int,
    *,
    minimum: int,
    maximum: int,
) -> int:
    try:
        value = int(os.getenv(name, str(default)) or default)
    except (TypeError, ValueError):
        value = default
    return max(minimum, min(maximum, value))


def journal_protected_tokens(daily_limit: int) -> int:
    configured = _bounded_env_int(
        "BNL_GEMINI_JOURNAL_PROTECTED_TOKENS",
        250_000,
        minimum=0,
        maximum=max(0, int(daily_limit)),
    )
    return min(configured, max(0, int(daily_limit) // 2))


def relay_protected_tokens(daily_limit: int) -> int:
    configured = _bounded_env_int(
        "BNL_GEMINI_RELAY_PROTECTED_TOKENS",
        100_000,
        minimum=0,
        maximum=max(0, int(daily_limit)),
    )
    return min(configured, max(0, int(daily_limit) // 2))


def _route_lane(route: str) -> str:
    normalized = re.sub(r"[^a-z0-9_:-]+", "_", str(route or "").lower())
    if normalized == "bnl_journal_generation" or "journal" in normalized:
        return "journal"
    background_markers = (
        "website",
        "relay",
        "ambient",
        "occasion",
        "community_scouting",
        "curiosity",
        "heartbeat",
        "showday",
        "background",
    )
    automatic_background_work = bool(
        "automatic" in normalized
        and any(
            marker in normalized
            for marker in ("enrichment", "source_file_refresh", "source_refresh")
        )
    )
    if automatic_background_work:
        return "background"
    protected_markers = (
        "memory_preview",
        "memory_governance",
        "relationship",
        "shared_brain",
        "source_file",
        "source_enrichment",
        "dossier",
        "population",
        "entity_intelligence",
        "canon",
        "single_packet",
    )
    if any(marker in normalized for marker in protected_markers):
        return "protected"
    if any(marker in normalized for marker in background_markers):
        return "background"
    return "conversation"


def policy_for_route(route: str) -> GeminiRoutePolicy:
    lane = _route_lane(route)
    normalized_route = re.sub(
        r"[^a-z0-9_:-]+",
        "_",
        str(route or "").lower(),
    )
    retries = _bounded_env_int(
        "BNL_GEMINI_PROVIDER_RETRIES",
        1,
        minimum=0,
        maximum=2,
    )
    if normalized_route == "ordinary_chat_single_packet_canary":
        # The accepted cutover path is one logical and physical provider
        # attempt: no retry multiplication and no model fallback.
        return GeminiRoutePolicy(
            lane="protected",
            max_output_tokens=_bounded_env_int(
                "BNL_GEMINI_CONVERSATION_MAX_OUTPUT_TOKENS",
                4_096,
                minimum=1_024,
                maximum=16_384,
            ),
            legacy_thinking_budget=_bounded_env_int(
                "BNL_GEMINI_CONVERSATION_LEGACY_THINKING_BUDGET",
                2_048,
                minimum=0,
                maximum=8_192,
            ),
            provider_retries=0,
            allow_fallback=False,
        )
    if lane == "journal":
        return GeminiRoutePolicy(
            lane=lane,
            max_output_tokens=_bounded_env_int(
                "BNL_GEMINI_JOURNAL_MAX_OUTPUT_TOKENS",
                16_384,
                minimum=4_096,
                maximum=32_768,
            ),
            legacy_thinking_budget=_bounded_env_int(
                "BNL_GEMINI_JOURNAL_LEGACY_THINKING_BUDGET",
                8_192,
                minimum=0,
                maximum=24_576,
            ),
            # The Journal already has four validator-guided generation
            # attempts. Retrying each provider call underneath that loop
            # multiplies shared-project pressure without adding a new repair.
            provider_retries=_bounded_env_int(
                "BNL_GEMINI_JOURNAL_PROVIDER_RETRIES",
                0,
                minimum=0,
                maximum=1,
            ),
            allow_fallback=False,
            journal_protected=True,
        )
    if lane == "protected":
        return GeminiRoutePolicy(
            lane=lane,
            max_output_tokens=_bounded_env_int(
                "BNL_GEMINI_PROTECTED_MAX_OUTPUT_TOKENS",
                8_192,
                minimum=2_048,
                maximum=24_576,
            ),
            legacy_thinking_budget=_bounded_env_int(
                "BNL_GEMINI_PROTECTED_LEGACY_THINKING_BUDGET",
                4_096,
                minimum=0,
                maximum=16_384,
            ),
            provider_retries=retries,
            allow_fallback=False,
        )
    if lane == "background":
        return GeminiRoutePolicy(
            lane=lane,
            max_output_tokens=_bounded_env_int(
                "BNL_GEMINI_BACKGROUND_MAX_OUTPUT_TOKENS",
                4_096,
                minimum=1_024,
                maximum=16_384,
            ),
            legacy_thinking_budget=_bounded_env_int(
                "BNL_GEMINI_BACKGROUND_LEGACY_THINKING_BUDGET",
                1_024,
                minimum=0,
                maximum=8_192,
            ),
            # Optional scheduled work skips cleanly when the provider is
            # unavailable. It must not amplify one background job into retry
            # or fallback-model spend, regardless of the interactive retry
            # policy.
            provider_retries=0,
            allow_fallback=False,
            relay_protected="relay" in normalized_route,
        )
    return GeminiRoutePolicy(
        lane=lane,
        max_output_tokens=_bounded_env_int(
            "BNL_GEMINI_CONVERSATION_MAX_OUTPUT_TOKENS",
            4_096,
            minimum=1_024,
            maximum=16_384,
        ),
        legacy_thinking_budget=_bounded_env_int(
            "BNL_GEMINI_CONVERSATION_LEGACY_THINKING_BUDGET",
            2_048,
            minimum=0,
            maximum=8_192,
        ),
        provider_retries=retries,
        allow_fallback=True,
    )


def single_attempt_reservation(contents: str, policy: GeminiRoutePolicy) -> int:
    prompt_tokens = max(1, (len(str(contents or "")) + 2) // 3)
    return prompt_tokens + int(policy.max_output_tokens)


def estimated_generation_reservation(
    contents: str,
    policy: GeminiRoutePolicy,
) -> int:
    model_count = 2 if policy.allow_fallback else 1
    attempts_per_model = 1 + max(0, int(policy.provider_retries))
    return single_attempt_reservation(contents, policy) * model_count * attempts_per_model


def budget_ceiling_for_route(
    daily_limit: int,
    route: str,
    *,
    journal_used: int = 0,
    relay_used: int = 0,
) -> int:
    """Return the shared-total ceiling while preserving unused protected lanes.

    The reserves are mutual rather than one-way. A Journal call leaves only the
    *unused* Relay reserve untouched, a Relay call leaves only the unused
    Journal reserve untouched, and ordinary calls leave both. Once a protected
    lane has used its own reserve, those tokens no longer need to be held back
    from the other lane.
    """
    limit = max(0, int(daily_limit))
    policy = policy_for_route(route)
    journal_remaining = max(
        0,
        journal_protected_tokens(limit) - max(0, int(journal_used)),
    )
    relay_remaining = max(
        0,
        relay_protected_tokens(limit) - max(0, int(relay_used)),
    )
    if policy.journal_protected:
        protected_remaining = relay_remaining
    elif policy.relay_protected:
        protected_remaining = journal_remaining
    else:
        protected_remaining = journal_remaining + relay_remaining
    return max(0, limit - min(limit, protected_remaining))


def provider_status_code(exc: Exception | None) -> int:
    for attr in ("status_code", "code"):
        value = getattr(exc, attr, None)
        try:
            if value is not None:
                return int(value)
        except (TypeError, ValueError):
            pass
    match = re.search(r"\b(400|404|429|500|502|503|504)\b", str(exc or ""))
    return int(match.group(1)) if match else 0


def provider_failure_kind(exc: Exception | None) -> ProviderFailureKind:
    status = provider_status_code(exc)
    text = str(exc or "").lower()
    if status == 429 or any(
        marker in text
        for marker in ("resource_exhausted", "rate limit", "rate_limit", "quota exceeded")
    ):
        return ProviderFailureKind.RATE_LIMITED
    if status in {500, 502, 503, 504} or any(
        marker in text
        for marker in ("service unavailable", "temporarily unavailable", "server error")
    ):
        return ProviderFailureKind.SERVER
    if status == 404 or any(
        marker in text
        for marker in ("model is no longer available", "model not found", "not_found")
    ):
        return ProviderFailureKind.MODEL_UNAVAILABLE
    if status == 400 or "invalid_argument" in text or "invalid argument" in text:
        return ProviderFailureKind.INVALID_REQUEST
    return ProviderFailureKind.UNKNOWN


def retryable_failure(kind: ProviderFailureKind) -> bool:
    return kind in {ProviderFailureKind.RATE_LIMITED, ProviderFailureKind.SERVER}


def fallback_eligible_failure(kind: ProviderFailureKind) -> bool:
    return kind in {
        ProviderFailureKind.RATE_LIMITED,
        ProviderFailureKind.SERVER,
        ProviderFailureKind.MODEL_UNAVAILABLE,
    }


def retry_delay_seconds(attempt_index: int) -> float:
    return min(2.0, 0.5 * (2 ** max(0, int(attempt_index))))
