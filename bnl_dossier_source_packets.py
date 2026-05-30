"""Approved source-lane packet builder for BNL dossier recommendations.

This module is deliberately deterministic and bot-internal. It does not scan
Discord history, create public content, or submit recommendations by itself; it
only builds a sanitized packet/payload for an explicitly-triggered operator path.
"""

from __future__ import annotations

import re
from typing import Any, Callable

from bnl_dossier_recommendations import build_dossier_recommendation_payload

APPROVED_SOURCE_LANES = {"rd_context", "broadcast_memory"}
FUTURE_SOURCE_LANES = {"read_model", "public_read_model", "safe_rd_notes"}
DEFAULT_MISSING_INFO = [
    "preferred display name",
    "public link",
    "public-safe role/description",
]
DEFAULT_PUBLIC_SAFETY_NOTES = [
    "Do not expose private Discord identity without owner approval.",
    "Verify public-safe wording before publishing.",
]
DEFAULT_SUGGESTED_ACTION = "Review source context and decide whether to attach or convert into a BNL Source File."
MAX_EVIDENCE_MATCHES = 5
MAX_SNIPPET_LENGTH = 220
MAX_EVIDENCE_SUMMARY_LENGTH = 700
_SAFE_LANE_PATTERN = re.compile(r"[^a-z0-9_-]+")
_WORD_PATTERN = re.compile(r"[a-z0-9]+")
_DISCORD_MENTION_PATTERN = re.compile(r"<[@#!&]?[0-9]{5,}>")
_LONG_ID_PATTERN = re.compile(r"\b\d{15,22}\b")
_broadcast_memory_reader: Callable[..., Any] | None = None


def set_broadcast_memory_reader(reader: Callable[..., Any] | None) -> None:
    """Register the existing bot broadcast-memory reader, if available."""

    global _broadcast_memory_reader
    _broadcast_memory_reader = reader


def is_broadcast_memory_reader_available() -> bool:
    """Return whether the packet builder has a reader for existing memory."""

    return callable(_broadcast_memory_reader)


def normalize_subject_name(subject_name: str) -> str:
    """Normalize display whitespace without guessing identity aliases."""

    return re.sub(r"\s+", " ", (subject_name or "").strip())


def subject_key(subject_name: str) -> str:
    """Return the stable subject key used by dossier ingest payloads."""

    normalized = re.sub(r"[^a-z0-9]+", "-", normalize_subject_name(subject_name).lower())
    return normalized.strip("-") or "unknown-subject"


def compact_subject_text(value: str) -> str:
    """Return compact lowercase alphanumeric text for conservative matching."""

    return "".join(_WORD_PATTERN.findall((value or "").lower()))


def contains_exact_subject_mention(text: str, subject_name: str) -> bool:
    """Case-insensitive exact phrase match with non-word boundaries."""

    subject = normalize_subject_name(subject_name)
    if not subject:
        return False
    pattern = re.compile(rf"(?<![A-Za-z0-9]){re.escape(subject)}(?![A-Za-z0-9])", re.IGNORECASE)
    return bool(pattern.search(text or ""))


def contains_compact_subject_mention(text: str, subject_name: str) -> bool:
    """Match spacing/punctuation variants only; no broad fuzzy matching."""

    compact_subject = compact_subject_text(subject_name)
    if not compact_subject or len(compact_subject) < 3:
        return False
    compact_text = compact_subject_text(text)
    return compact_subject in compact_text


def contains_subject_mention(text: str, subject_name: str, aliases: list[str] | None = None) -> bool:
    """Return true for conservative subject/explicit-alias mention matches."""

    candidates = [subject_name] + [alias for alias in (aliases or []) if alias]
    return any(
        contains_exact_subject_mention(text, candidate) or contains_compact_subject_mention(text, candidate)
        for candidate in candidates
    )


def normalize_source_lanes(lanes: Any) -> list[str]:
    """Normalize and allow only approved v1 lanes for packet collection."""

    if isinstance(lanes, str):
        raw_lanes = re.split(r"[,\s]+", lanes)
    elif isinstance(lanes, (list, tuple, set)):
        raw_lanes = list(lanes)
    else:
        raw_lanes = []

    normalized: list[str] = []
    seen = set()
    for lane in raw_lanes:
        clean = _SAFE_LANE_PATTERN.sub("_", str(lane or "").strip().lower()).strip("_")
        if clean in {"manual", "operator", "manual_operator", "r_d", "rnd", "rd"}:
            clean = "rd_context"
        if clean not in APPROVED_SOURCE_LANES or clean in seen:
            continue
        seen.add(clean)
        normalized.append(clean)
    return normalized or ["rd_context"]


def _safe_snippet(text: str, max_length: int = MAX_SNIPPET_LENGTH) -> str:
    cleaned = _DISCORD_MENTION_PATTERN.sub("[redacted-mention]", str(text or ""))
    cleaned = _LONG_ID_PATTERN.sub("[redacted-id]", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    if len(cleaned) <= max_length:
        return cleaned
    return cleaned[: max(0, max_length - 1)].rstrip() + "…"


def _broadcast_row_to_text(row: Any) -> str:
    if isinstance(row, dict):
        return str(row.get("cleaned_summary") or row.get("summary") or row.get("text") or row.get("raw_note") or "")
    if isinstance(row, (list, tuple)):
        # get_recent_broadcast_memory returns cleaned_summary at index 1.
        if len(row) > 1 and row[1]:
            return str(row[1])
        return " ".join(str(part or "") for part in row)
    return str(row or "")


def _broadcast_row_episode(row: Any) -> str:
    if isinstance(row, dict):
        return str(row.get("episode_date") or row.get("created_at") or "unknown-date")
    if isinstance(row, (list, tuple)) and row:
        return str(row[0] or "unknown-date")
    return "unknown-date"


def build_manual_operator_evidence(subject: str, reason: str, source_lanes: Any = None) -> dict[str, Any]:
    """Build explicit operator/R&D evidence from the command reason."""

    # The command reason is always an explicit operator/R&D note, even when
    # additional lanes are requested for lookup.
    return {
        "lane": "rd_context",
        "label": "R&D/operator note",
        "summary": _safe_snippet(reason, MAX_SNIPPET_LENGTH),
        "subjectName": normalize_subject_name(subject),
    }


def collect_broadcast_memory_evidence(
    guild_id: int | None,
    subject_name: str,
    limit: int = 500,
    *,
    reader: Callable[..., Any] | None = None,
    aliases: list[str] | None = None,
) -> list[dict[str, Any]]:
    """Collect small labeled snippets from existing broadcast memory records."""

    memory_reader = reader or _broadcast_memory_reader
    if not guild_id or not callable(memory_reader):
        return []

    try:
        rows = memory_reader(guild_id, public_only=False, limit=max(1, min(int(limit or 1), 500)))
    except Exception:
        return []

    evidence: list[dict[str, Any]] = []
    for row in rows or []:
        text = _broadcast_row_to_text(row)
        if not contains_subject_mention(text, subject_name, aliases=aliases):
            continue
        snippet = _safe_snippet(text)
        if not snippet:
            continue
        evidence.append(
            {
                "lane": "broadcast_memory",
                "label": "Broadcast memory",
                "summary": snippet,
                "episodeDate": _broadcast_row_episode(row),
            }
        )
        if len(evidence) >= MAX_EVIDENCE_MATCHES:
            break
    return evidence


def _evidence_summary(evidence: list[dict[str, Any]]) -> str:
    pieces = []
    for item in evidence:
        lane = item.get("lane") or "source"
        summary = _safe_snippet(item.get("summary") or "", 180)
        if summary:
            pieces.append(f"{lane}: {summary}")
    joined = " | ".join(pieces)
    if len(joined) > MAX_EVIDENCE_SUMMARY_LENGTH:
        return joined[: MAX_EVIDENCE_SUMMARY_LENGTH - 1].rstrip() + "…"
    return joined


def _confidence_for_evidence(actual_lanes: list[str], evidence: list[dict[str, Any]]) -> str:
    non_manual_lanes = {item.get("lane") for item in evidence if item.get("lane") != "rd_context"}
    if len(non_manual_lanes) >= 2 and len(evidence) >= 3:
        return "high"
    if non_manual_lanes or len(actual_lanes) > 1:
        return "medium"
    return "low"


def build_dossier_recommendation_packet(
    subject: str,
    reason: str,
    lanes: Any = None,
    guild_id: int | None = None,
    options: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build an internal source packet and sender-compatible payload."""

    opts = options or {}
    subject_name = normalize_subject_name(subject)
    requested_lanes = normalize_source_lanes(lanes or ["rd_context"])
    aliases = opts.get("aliases") if isinstance(opts.get("aliases"), list) else None

    evidence = [build_manual_operator_evidence(subject_name, reason, requested_lanes)]
    if "broadcast_memory" in requested_lanes:
        evidence.extend(
            collect_broadcast_memory_evidence(
                guild_id,
                subject_name,
                reader=opts.get("broadcast_memory_reader"),
                aliases=aliases,
            )
        )

    actual_lanes: list[str] = []
    for item in evidence:
        lane = item.get("lane")
        if lane and lane not in actual_lanes:
            actual_lanes.append(lane)
    if not actual_lanes:
        actual_lanes = ["rd_context"]

    packet = {
        "subjectName": subject_name,
        "subjectKey": subject_key(subject_name),
        "sourceLanes": actual_lanes,
        "reason": _safe_snippet(reason, 300),
        "evidenceSummary": _evidence_summary(evidence),
        "missingInfo": list(DEFAULT_MISSING_INFO),
        "publicSafetyNotes": list(DEFAULT_PUBLIC_SAFETY_NOTES),
        "doNotSay": [],
        "recommendedTags": [],
        "confidence": _confidence_for_evidence(actual_lanes, evidence),
        "suggestedAction": DEFAULT_SUGGESTED_ACTION,
        "evidence": evidence,
    }
    payload = build_dossier_recommendation_payload(
        {
            "type": "new_subject",
            "subjectName": packet["subjectName"],
            "subjectKey": packet["subjectKey"],
            "reason": packet["reason"],
            "evidenceSummary": packet["evidenceSummary"],
            "missingInfo": packet["missingInfo"],
            "publicSafetyNotes": packet["publicSafetyNotes"],
            "doNotSay": packet["doNotSay"],
            "recommendedTags": packet["recommendedTags"],
            "confidence": packet["confidence"],
            "sourceLanes": packet["sourceLanes"],
            "suggestedAction": packet["suggestedAction"],
            "createdBy": "bnl",
        }
    )
    return {"packet": packet, "payload": payload}
