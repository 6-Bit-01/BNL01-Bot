"""Approved source-lane packet builder for BNL dossier recommendations.

This module is deliberately deterministic and bot-internal. It does not scan
Discord history, create public content, or submit recommendations by itself; it
only builds a sanitized packet/payload for an explicitly-triggered operator path.
"""

from __future__ import annotations

import re
from typing import Any, Callable

from bnl_dossier_recommendations import build_dossier_recommendation_payload

APPROVED_SOURCE_LANES = {"rd_context", "broadcast_memory", "community_presence"}
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

_RAW_NORMAL_FIELD_MARKERS = (
    "user_profiles/local_profile_observed",
    "relationship_journal/local_relationship_trace",
    "relationship_state/local_relationship_trace",
    "conversations/public_discord_observed",
    "memory_tiers/source_blind_memory_trace",
    "source lane mapping",
    "unknown -> unknown",
    "help_signal",
    "EDGE_SESSION",
)
MAX_ENTITY_RAW_FRAGMENTS = 8


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
            if lane == "broadcast_memory":
                pieces.append(f"BARCODE Radio context: {summary}")
            elif lane == "rd_context":
                pieces.append(f"Operator review context: {summary}")
            else:
                pieces.append(f"{lane} review context: {summary}")
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


def _normal_text_key(value: Any) -> str:
    text = re.sub(r"[^a-z0-9]+", " ", str(value or "").lower()).strip()
    replacements = {
        "local profile match found for": "bnl found an internal local profile match for",
        "review only relationship context signal exists for this subject": "bnl found prior relationship context notes connected to subject",
        "public side conversation context exists but owner review is needed before stating it as public fact": "bnl found approved public side conversation context connected to subject",
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
    return re.sub(r"\s+", " ", text)


def _looks_like_raw_label(value: Any) -> bool:
    text = str(value or "")
    lowered = text.lower()
    if any(marker.lower() in lowered for marker in _RAW_NORMAL_FIELD_MARKERS):
        return True
    if re.search(r"\b[a-z]+(?:_[a-z]+)+/[a-z]+(?:_[a-z]+)+\b", text):
        return True
    if re.search(r"\b\d{15,22}\b", text):
        return True
    return False


def _merge_unique(target: list[Any], values: Any, *, limit: int | None = None) -> None:
    if not isinstance(values, list):
        return
    seen = {_normal_text_key(item) for item in target}
    for value in values:
        if isinstance(value, dict):
            marker = _normal_text_key(value)
            if marker and marker not in seen:
                target.append(value)
                seen.add(marker)
        else:
            clean = _safe_snippet(str(value or ""), 260)
            key = _normal_text_key(clean)
            if not clean or _looks_like_raw_label(clean) or key in seen:
                continue
            target.append(clean)
            seen.add(key)
        if limit and len(target) >= limit:
            break


def _dedupe_same_observation(values: list[Any]) -> list[Any]:
    seen: set[str] = set()
    out: list[Any] = []
    for value in values:
        if isinstance(value, dict):
            key = _normal_text_key(value)
        else:
            text = str(value or "")
            observation = re.split(r"\b(?:Keep this internal|Owner review|Not public|Treat this as|It may inform|Source-blind memory requires)\b", text, maxsplit=1)[0]
            key = _normal_text_key(observation or text)
        if key and key in seen:
            continue
        if key:
            seen.add(key)
        out.append(value)
    return out


def _has_entity_evidence(entity_summary: dict[str, Any] | None) -> bool:
    raw = (entity_summary or {}).get("rawProvenance") or {}
    return bool(raw.get("rawFragments"))


def _entity_source_authority_items(entity_summary: dict[str, Any], confidence: str) -> list[dict[str, str]]:
    items: list[dict[str, str]] = []
    for value in entity_summary.get("sourceAuthority") or []:
        if not value or _looks_like_raw_label(value):
            continue
        items.append({"source": "Entity Activity Summary", "boundary": _safe_snippet(str(value), 160), "confidence": confidence})
    return items


def merge_entity_activity_summary_into_packet(packet: dict[str, Any], entity_summary: dict[str, Any] | None) -> dict[str, Any]:
    """Merge shared entity-summary context into a source packet without changing privacy boundaries."""

    if not _has_entity_evidence(entity_summary):
        return packet

    summary = entity_summary or {}
    confidence = str(packet.get("confidence") or summary.get("confidence") or "low")
    _merge_unique(packet.setdefault("knownContext", []), summary.get("knownContext"), limit=8)
    useful_from_summary: list[str] = []
    useful_from_summary.extend(summary.get("activitySignals") or [])
    useful_from_summary.extend(summary.get("channelActivity") or [])
    useful_from_summary.extend(summary.get("recurringTopics") or [])
    if summary.get("matchedNames"):
        useful_from_summary.insert(0, f"BNL found an internal local profile match for {packet.get('subjectName') or summary.get('subjectName') or 'this subject'}.")
    _merge_unique(packet.setdefault("usefulEvidence", []), useful_from_summary, limit=8)
    _merge_unique(packet.setdefault("relationshipSignals", []), summary.get("relationshipSignals"), limit=6)

    current_public = packet.setdefault("publicSafePossibilities", [])
    summary_public = [
        item for item in (summary.get("publicSafePossibilities") or [])
        if item != "No public-safe facts confirmed yet." or not any("public-safe" not in str(existing).lower() for existing in current_public)
    ]
    if any(item != "No public-safe facts confirmed yet." for item in summary_public):
        current_public[:] = [item for item in current_public if item != "No public-safe fact is confirmed by this packet; owner review is required before public use."]
    _merge_unique(current_public, summary_public, limit=6)
    if not current_public:
        current_public.append("No public-safe fact is confirmed by this packet; owner review is required before public use.")

    _merge_unique(packet.setdefault("privateOnlyNotes", []), summary.get("privateOnlyNotes"), limit=8)
    packet["privateOnlyNotes"] = _dedupe_same_observation(packet.get("privateOnlyNotes") or [])[:8]
    _merge_unique(packet.setdefault("notPublicYet", []), summary.get("notPublicYet"), limit=8)
    packet["notPublicYet"] = _dedupe_same_observation(packet.get("notPublicYet") or [])[:8]
    _merge_unique(packet.setdefault("missingInfo", []), summary.get("missingInfo"), limit=8)
    _merge_unique(packet.setdefault("sourceAuthority", []), _entity_source_authority_items(summary, confidence), limit=10)

    summary_confidence = str(summary.get("confidence") or "none")
    if packet.get("confidence") == "low" and summary_confidence in {"medium", "high"}:
        packet["confidence"] = "medium" if summary_confidence == "medium" else "high"

    if packet.get("relationshipSignals") or any("relationship" in str(item).lower() for item in packet.get("privateOnlyNotes") or []):
        action = "Separate private relationship/context notes from any public-safe community description before drafting; queue/submission identity is not connected yet."
    elif any(item != "No public-safe facts confirmed yet." and "no public-safe fact" not in str(item).lower() for item in packet.get("publicSafePossibilities") or []):
        action = "Review this subject as a possible community/source-file candidate; owner review must confirm public-safe identity, role, and wording before any public use."
    else:
        action = "Keep this as internal Source File review material until an owner confirms public-safe identity, role, and wording; queue/submission identity is not connected yet."
    packet["recommendedAction"] = action
    packet["suggestedAction"] = action

    raw = packet.setdefault("rawProvenance", {})
    entity_raw = summary.get("rawProvenance") or {}
    direct_labels = list(raw.get("sourceLabels") or [])
    entity_labels = list(entity_raw.get("sourceLabels") or [])
    raw["sourceLabels"] = list(dict.fromkeys(direct_labels + entity_labels))
    raw["sourceCounts"] = dict(raw.get("sourceCounts") or {})
    raw["entitySourceCounts"] = dict(entity_raw.get("sourceCounts") or {})
    if entity_raw.get("channelPolicies"):
        raw["channelPolicyCounts"] = dict(entity_raw.get("channelPolicies") or {})
    raw["entitySourceAuthority"] = list(summary.get("sourceAuthority") or [])[:5]
    fragments = list(raw.get("rawFragments") or [])
    for fragment in list(entity_raw.get("rawFragments") or [])[:MAX_ENTITY_RAW_FRAGMENTS]:
        if isinstance(fragment, dict):
            merged = {"origin": "entity_activity_summary", **fragment}
        else:
            merged = {"origin": "entity_activity_summary", "snippet": _safe_snippet(str(fragment))}
        fragments.append(merged)
    raw["rawFragments"] = fragments[: MAX_EVIDENCE_MATCHES + MAX_ENTITY_RAW_FRAGMENTS]
    packet["entityActivitySummaryIncluded"] = True
    return packet

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

    entity_summary = opts.get("entity_activity_summary") if isinstance(opts.get("entity_activity_summary"), dict) else None
    db_path = opts.get("db_path") or opts.get("entity_db_path")
    if entity_summary is None and db_path:
        try:
            from bnl_entity_activity_summary import build_entity_activity_summary

            entity_summary = build_entity_activity_summary(
                str(db_path),
                subject_name,
                guild_id,
                [
                    "user_profiles",
                    "user_memory_facts",
                    "user_habits",
                    "relationship_state",
                    "relationship_journal",
                    "memory_tiers",
                    "conversations",
                    "broadcast_memory",
                    "rd_context",
                ],
                "admin_internal",
                50,
                opts.get("rd_context"),
            )
        except Exception:
            entity_summary = None

    actual_lanes: list[str] = []
    for item in evidence:
        lane = item.get("lane")
        if lane and lane not in actual_lanes:
            actual_lanes.append(lane)
    if not actual_lanes:
        actual_lanes = ["rd_context"]

    confidence = _confidence_for_evidence(actual_lanes, evidence)
    known_context = [f"BNL has operator-provided review context for {subject_name}."]
    useful_evidence = ["Operator supplied R&D context for review."]
    public_safe_possibilities: list[str] = []
    private_only_notes = ["Treat operator/R&D context as internal until owner review confirms public-safe wording."]
    if any(item.get("lane") == "broadcast_memory" for item in evidence):
        known_context.append(f"BNL found BARCODE Radio/show-history context connected to {subject_name}.")
        useful_evidence.append("BARCODE Radio memory can support show-history review after owner approval.")
        public_safe_possibilities.append("BARCODE Radio context may inform public wording only after owner review.")
    if not public_safe_possibilities:
        public_safe_possibilities.append("No public-safe fact is confirmed by this packet; owner review is required before public use.")
    not_public_yet = ["Do not publish, merge identities, or expose private Discord identity from this packet without owner approval."]
    raw_provenance = {
        "sourceLabels": [f"{item.get('lane') or 'source'}/{item.get('label') or 'review'}" for item in evidence],
        "sourceLaneMapping": {lane: lane for lane in actual_lanes},
        "rawFragments": evidence,
        "sourceCounts": {lane: sum(1 for item in evidence if item.get("lane") == lane) for lane in actual_lanes},
    }

    packet = {
        "subjectName": subject_name,
        "subjectKey": subject_key(subject_name),
        "sourceLanes": actual_lanes,
        "reason": _safe_snippet(reason, 300),
        "evidenceSummary": _evidence_summary(evidence),
        "knownContext": known_context,
        "usefulEvidence": useful_evidence,
        "relationshipSignals": [],
        "publicSafePossibilities": public_safe_possibilities,
        "privateOnlyNotes": private_only_notes,
        "missingInfo": list(DEFAULT_MISSING_INFO),
        "notPublicYet": not_public_yet,
        "publicSafetyNotes": list(DEFAULT_PUBLIC_SAFETY_NOTES),
        "doNotSay": [],
        "recommendedTags": [],
        "confidence": confidence,
        "sourceAuthority": [{"source": "Operator R&D context", "boundary": "owner_review_required", "confidence": confidence}],
        "rawProvenance": raw_provenance,
        "suggestedAction": DEFAULT_SUGGESTED_ACTION,
        "recommendedAction": DEFAULT_SUGGESTED_ACTION,
        "evidence": evidence,
    }
    merge_entity_activity_summary_into_packet(packet, entity_summary)
    payload = build_dossier_recommendation_payload(
        {
            "type": "new_subject",
            "subjectName": packet["subjectName"],
            "subjectKey": packet["subjectKey"],
            "reason": packet["reason"],
            "evidenceSummary": packet["evidenceSummary"],
            "knownContext": packet["knownContext"],
            "usefulEvidence": packet["usefulEvidence"],
            "relationshipSignals": packet["relationshipSignals"],
            "publicSafePossibilities": packet["publicSafePossibilities"],
            "privateOnlyNotes": packet["privateOnlyNotes"],
            "missingInfo": packet["missingInfo"],
            "notPublicYet": packet["notPublicYet"],
            "publicSafetyNotes": packet["publicSafetyNotes"],
            "doNotSay": packet["doNotSay"],
            "recommendedTags": packet["recommendedTags"],
            "confidence": packet["confidence"],
            "sourceLanes": packet["sourceLanes"],
            "sourceAuthority": packet["sourceAuthority"],
            "rawProvenance": packet["rawProvenance"],
            "suggestedAction": packet["suggestedAction"],
            "recommendedAction": packet["recommendedAction"],
            "createdBy": "bnl",
        }
    )
    return {"packet": packet, "payload": payload}
