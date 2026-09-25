"""Route-aware Gemini model, budget, retry, and fallback policy for BNL-01.

This module contains no provider client calls. It keeps routing decisions small,
testable, and independent from Discord, SQLite, and Journal implementation code.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
import os
import re


DEFAULT_PRIMARY_MODEL = "gemini-3.6-flash"
DEFAULT_FALLBACK_MODEL = "gemini-3.5-flash"


@dataclass(frozen=True)
class GeminiImagePart:
    """One admitted current-message image, kept transient through generation."""

    data: bytes = field(repr=False)
    mime_type: str
    source_label: str
    estimated_tokens: int

    def __post_init__(self) -> None:
        if not isinstance(self.data, bytes) or not self.data:
            raise ValueError("gemini_image_requires_nonempty_bytes")
        if not isinstance(self.mime_type, str) or not self.mime_type:
            raise ValueError("gemini_image_requires_mime_type")
        if not isinstance(self.source_label, str) or not self.source_label:
            raise ValueError("gemini_image_requires_source_label")
        if type(self.estimated_tokens) is not int or self.estimated_tokens < 1:
            raise ValueError("gemini_image_requires_positive_token_estimate")


@dataclass(frozen=True)
class GeminiImageRequest:
    """Text and its scoped images; diagnostic text never includes image bytes."""

    text: str
    images: tuple[GeminiImagePart, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.text, str):
            raise ValueError("gemini_image_request_requires_text")
        if not isinstance(self.images, tuple) or any(
            not isinstance(image, GeminiImagePart) for image in self.images
        ):
            raise ValueError("gemini_image_request_requires_immutable_image_parts")

    def __str__(self) -> str:
        return self.text


def estimate_gemini_prompt_tokens(
    contents: str | GeminiImageRequest,
    *,
    conservative_utf8: bool = False,
) -> int:
    """Apply the existing text estimate and include admitted image token bounds.

    UTF-8 counting is the existing conservative dollar-reservation rule. Daily
    token reservations use the existing character estimate. Neither path may
    count a byte representation in place of the actual image token allowance.
    """

    def text_tokens(value: str) -> int:
        if conservative_utf8:
            return max(1, len(value.encode("utf-8")))
        return max(1, (len(value) + 2) // 3)

    if not isinstance(contents, GeminiImageRequest):
        return text_tokens(str(contents or ""))
    return text_tokens(contents.text) + sum(
        text_tokens(image.source_label) + image.estimated_tokens
        for image in contents.images
    )


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
    showday_protected: bool = False
    fallback_status_codes: tuple[int, ...] = ()


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
    if normalized_route == 'moment_meaning_background':
        return GeminiRoutePolicy(
            lane='background', max_output_tokens=2048, legacy_thinking_budget=512,
            provider_retries=0, allow_fallback=False,
        )
    if normalized_route in {"broadcast_ballad_manual", "broadcast_ballad_background"}:
        # A full song plus metadata and model thinking needs its own allowance.
        # Existing token/dollar reservations price this bound before one call.
        return GeminiRoutePolicy(
            lane=lane,
            max_output_tokens=_bounded_env_int(
                "BNL_GEMINI_BALLAD_MAX_OUTPUT_TOKENS", 16_384,
                minimum=4_096, maximum=32_768,
            ),
            legacy_thinking_budget=2_048,
            provider_retries=0, allow_fallback=False,
            showday_protected=normalized_route == "broadcast_ballad_background",
        )
    if normalized_route == "ordinary_chat_single_packet_canary":
        # Direct chat uses one physical attempt, without model fallback.
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
            # Relay may recover from provider overload with one backup call.
            # No per-model retries: both attempts are reserved up front by
            # the existing token/dollar guards. Other background work skips.
            provider_retries=0,
            allow_fallback=normalized_route == "website_relay_event",
            fallback_status_codes=(503,) if normalized_route == "website_relay_event" else (),
            relay_protected="relay" in normalized_route,
            # A single automatic draft for a finalized show is scheduled show
            # work. It retains the hard ceiling and both dollar reserves.
            showday_protected="showday" in normalized_route or normalized_route == "broadcast_ballad_background",
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


def single_attempt_reservation(
    contents: str | GeminiImageRequest,
    policy: GeminiRoutePolicy,
) -> int:
    prompt_tokens = estimate_gemini_prompt_tokens(contents)
    return prompt_tokens + int(policy.max_output_tokens)


def estimated_generation_reservation(
    contents: str | GeminiImageRequest,
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


def provider_server_diagnostics(exc: Exception, *, secrets: tuple[str, ...] = ()) -> dict:
    """Bounded private SDK error fields; never stringify the exception or body.

    google-genai exposes message/status separately from ``details``, whose
    arbitrary payload and ErrorInfo metadata must not enter diagnostics.
    These fields describe the provider response, not a verified root cause.
    """
    result = {}

    def safe_token(value, pattern, limit=160):
        if (isinstance(value, str) and len(value) <= limit
                and re.fullmatch(pattern, value)
                and not any(secret and secret in value for secret in secrets)):
            return value
        return ""

    status = safe_token(getattr(exc, "status", None), r"[A-Z][A-Z0-9_]*", 64)
    if status:
        result["status"] = status
    message = getattr(exc, "message", None)
    if isinstance(message, str) and message:
        if len(message) > 8192:
            result["message_omitted"] = "oversized"
        elif re.search(r'''(?i)\b(?:contents?|prompts?|parts|inline_data|system_instruction)["']?\s*[:=]''', message):
            result["message_omitted"] = "request_payload_marker"
        else:
            for secret in secrets:
                if secret:
                    message = message.replace(secret, "[redacted]")
            message = re.sub(r'''(?i)https?://[^\s<>"']+''', "[url omitted]", message)
            message = re.sub(r"(?i)\bBearer\s+[A-Za-z0-9._~+/=-]+", "Bearer [redacted]", message)
            message = re.sub(
                r'''(?i)\b(?:api[_ -]?key|x-goog-api-key|access[_ -]?token|token|secret|password|authorization)["']?\s*[:=]\s*(?:"[^"\r\n]*"|'[^'\r\n]*'|[^\s,;]+)''',
                "credential=[redacted]", message,
            )
            message = re.sub(r"[\x00-\x1f\x7f-\x9f]", " ", message)
            message = " ".join(message.split())
            result["message"] = message[:600]
            result["message_truncated"] = len(message) > 600

    details = getattr(exc, "details", None)
    if isinstance(details, dict):
        error = details.get("error", details)
        entries = error.get("details", []) if isinstance(error, dict) else []
        if isinstance(entries, list):
            reasons = []
            for entry in entries[:8]:
                if not isinstance(entry, dict) or entry.get("@type") != "type.googleapis.com/google.rpc.ErrorInfo":
                    continue
                reason = safe_token(entry.get("reason"), r"[A-Z][A-Z0-9_]*", 100)
                domain = safe_token(entry.get("domain"), r"[a-z0-9.-]+", 100)
                if reason:
                    reasons.append({"reason": reason, "domain": domain})
            if reasons:
                result["error_info"] = reasons

    headers = getattr(getattr(exc, "response", None), "headers", None)
    if headers is not None and callable(getattr(headers, "get", None)):
        for name in ("x-request-id", "x-goog-request-id", "x-guploader-uploadid"):
            value = safe_token(headers.get(name), r"[A-Za-z0-9._:/=-]+")
            if value:
                result[name] = value
        retry_after = safe_token(headers.get("retry-after"), r"[0-9]{1,8}", 8)
        if retry_after:
            result["retry_after_seconds"] = int(retry_after)
    if not result:
        result["details_available"] = False
    return result


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
