"""Shared BNL entity activity summary builder.

This module builds a structured, review-safe readout from approved local BNL
memory lanes. It is intentionally backend-only: it does not write memory, create
Source Files/dossiers, call website ingest, publish, or fetch Discord history.
"""

from __future__ import annotations

import re
import sqlite3
from collections import Counter
from typing import Any

from bnl_dossier_source_packets import normalize_subject_name, subject_key

DEFAULT_ENTITY_SUMMARY_LIMIT = 50
MAX_ENTITY_SUMMARY_LIMIT = 200
DEFAULT_ALLOWED_LANES = {
    "user_profiles",
    "user_memory_facts",
    "user_habits",
    "relationship_state",
    "relationship_journal",
    "memory_tiers",
    "conversations",
    "broadcast_memory",
}
PUBLIC_CONVERSATION_POLICIES = {"public_home", "public_context", "public_selective", "broadcast_memory"}
SOURCE_BLIND_REVIEW_NOTE = "Source-blind legacy memory exists; review source and visibility before use."
QUEUE_NOT_CONNECTED_NOTE = "Queue/submission history is not connected to BNL entity summaries yet."
MAX_SNIPPET_LENGTH = 180
_DISCORD_MENTION_PATTERN = re.compile(r"<[@#!&]?[0-9]{5,}>")
_LONG_ID_PATTERN = re.compile(r"\b\d{15,22}\b")
_URL_PATTERN = re.compile(r"https?://\S+", re.I)
_WORD_PATTERN = re.compile(r"[a-z0-9]+")
_TOPIC_PATTERNS = [
    ("music/community context", re.compile(r"\b(music|song|track|artist|radio|show|broadcast|album|playlist|dj|beat)\b", re.I)),
    ("help/support requests", re.compile(r"\b(help|support|assist|fix|issue|question|request|need)\b", re.I)),
    ("creative/project context", re.compile(r"\b(project|design|art|write|story|video|website|source file|dossier)\b", re.I)),
    ("community/event context", re.compile(r"\b(community|server|event|chat|room|crowd|member)\b", re.I)),
]



def parse_entity_activity_summary_command(content: str) -> tuple[bool, dict[str, Any] | None, str]:
    """Parse `!bnl entity summary <subject>` operator readout commands."""

    text = (content or "").strip()
    match = re.match(r"^!bnl\s+(?:entity\s+summary|source\s+summary)\s+(.+?)\s*$", text, flags=re.I | re.S)
    if not match:
        return False, None, "not_an_entity_activity_summary_command"
    subject = normalize_subject_name(match.group(1))
    if not subject:
        return True, None, "Use: `!bnl entity summary <subject>`"
    return True, {"subjectName": subject}, ""


def compact_subject_text(value: str) -> str:
    return "".join(_WORD_PATTERN.findall((value or "").lower()))


def contains_subject_mention(text: str, subject: str, aliases: list[str] | None = None) -> bool:
    candidates = [subject] + [alias for alias in (aliases or []) if alias]
    haystack = text or ""
    compact_haystack = compact_subject_text(haystack)
    for candidate in candidates:
        name = normalize_subject_name(candidate)
        if not name:
            continue
        if re.search(rf"(?<![A-Za-z0-9]){re.escape(name)}(?![A-Za-z0-9])", haystack, flags=re.I):
            return True
        compact = compact_subject_text(name)
        if compact and len(compact) >= 3 and compact in compact_haystack:
            return True
    return False


def _safe_snippet(text: Any, max_length: int = MAX_SNIPPET_LENGTH) -> str:
    cleaned = _URL_PATTERN.sub("[redacted-url]", str(text or ""))
    cleaned = _DISCORD_MENTION_PATTERN.sub("[redacted-mention]", cleaned)
    cleaned = _LONG_ID_PATTERN.sub("[redacted-id]", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    if len(cleaned) <= max_length:
        return cleaned
    return cleaned[: max(0, max_length - 1)].rstrip() + "…"


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    cur = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,))
    return cur.fetchone() is not None


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    if not _table_exists(conn, table):
        return set()
    return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}


def _where_guild(cols: set[str], guild_id: int | None) -> tuple[str, list[Any]]:
    if guild_id is not None and "guild_id" in cols:
        return " WHERE guild_id=?", [guild_id]
    return "", []


def _fetch_rows(conn: sqlite3.Connection, table: str, guild_id: int | None, limit: int) -> list[sqlite3.Row]:
    cols = _columns(conn, table)
    if not cols:
        return []
    where, params = _where_guild(cols, guild_id)
    order = ""
    for candidate in ("updated_at", "timestamp", "last_seen_at", "created_at", "id"):
        if candidate in cols:
            order = f" ORDER BY {candidate} DESC"
            break
    return list(conn.execute(f"SELECT * FROM {table}{where}{order} LIMIT ?", [*params, max(1, limit)]))


def _row_text(row: sqlite3.Row, fields: list[str]) -> str:
    data = dict(row)
    return " ".join(str(data.get(field) or "") for field in fields if field in data)


def _add_unique(target: list[str], value: str) -> None:
    clean = re.sub(r"\s+", " ", str(value or "")).strip()
    if clean and clean not in target:
        target.append(clean)


def _source_label(table: str, quality: str) -> str:
    return f"{table}/{quality}"


def _record_raw(raw: dict[str, Any], table: str, quality: str, snippet: str, **extra: Any) -> None:
    label = _source_label(table, quality)
    raw["sourceLabels"].append(label)
    raw["sourceCounts"][table] += 1
    fragment = {"table": table, "sourceLabel": label, "sourceQuality": quality, "snippet": snippet}
    fragment.update({k: v for k, v in extra.items() if v is not None and v != ""})
    raw["rawFragments"].append(fragment)


def _note_topics(counter: Counter, text: str) -> None:
    for label, pattern in _TOPIC_PATTERNS:
        if pattern.search(text or ""):
            counter[label] += 1


def _normalize_allowed_lanes(allowed_lanes: Any) -> set[str]:
    if allowed_lanes is None:
        return set(DEFAULT_ALLOWED_LANES)
    if isinstance(allowed_lanes, str):
        raw = re.split(r"[,\s]+", allowed_lanes)
    elif isinstance(allowed_lanes, (list, tuple, set)):
        raw = list(allowed_lanes)
    else:
        raw = []
    lanes = {re.sub(r"[^a-z0-9_]+", "_", str(lane or "").strip().lower()).strip("_") for lane in raw}
    return {lane for lane in lanes if lane in DEFAULT_ALLOWED_LANES or lane == "rd_context"} or set(DEFAULT_ALLOWED_LANES)


def build_entity_activity_summary(
    db_path: str,
    subject_name: str,
    guild_id: int | None = None,
    allowed_lanes: Any = None,
    output_mode: str = "admin_internal",
    limit: int = DEFAULT_ENTITY_SUMMARY_LIMIT,
    rd_context: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build a structured, privacy-bounded BNL entity/person activity summary."""

    subject = normalize_subject_name(subject_name)
    key = subject_key(subject)
    lanes = _normalize_allowed_lanes(allowed_lanes)
    max_rows = max(1, min(int(limit or DEFAULT_ENTITY_SUMMARY_LIMIT), MAX_ENTITY_SUMMARY_LIMIT))
    summary: dict[str, Any] = {
        "subjectName": subject,
        "subjectKey": key,
        "matchedNames": [],
        "identityConfidence": "none",
        "knownContext": [],
        "activitySignals": [],
        "channelActivity": [],
        "recurringTopics": [],
        "interactionSignals": [],
        "relationshipSignals": [],
        "publicSafePossibilities": [],
        "privateOnlyNotes": [],
        "notPublicYet": [],
        "missingInfo": ["public role", "public links", "confirmed display name", "queue/submission identity"],
        "sourceAuthority": [],
        "recommendedAction": "Keep internal and review before drafting public dossier copy.",
        "confidence": "low",
        "rawProvenance": {"sourceLabels": [], "sourceCounts": Counter(), "rawFragments": [], "channelPolicies": Counter()},
    }
    topic_counts: Counter = Counter()
    matched_user_ids: set[Any] = set()
    aliases: list[str] = []

    if not subject:
        summary["privateOnlyNotes"].append("No subject supplied for summary.")
        summary["rawProvenance"]["sourceCounts"] = {}
        summary["rawProvenance"]["channelPolicies"] = {}
        return summary

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        if "user_profiles" in lanes:
            for row in _fetch_rows(conn, "user_profiles", guild_id, max_rows):
                data = dict(row)
                names = [data.get("display_name"), data.get("preferred_name")]
                if any(contains_subject_mention(str(name or ""), subject) or contains_subject_mention(subject, str(name or "")) for name in names if name):
                    for name in names:
                        if name:
                            clean_name = normalize_subject_name(str(name))
                            _add_unique(summary["matchedNames"], clean_name)
                            aliases.append(clean_name)
                    if "user_id" in data:
                        matched_user_ids.add(data.get("user_id"))
                    _add_unique(summary["knownContext"], f"Local profile match found for {subject}.")
                    _record_raw(summary["rawProvenance"], "user_profiles", "local_profile_observed", f"Local profile observed for {subject}.")

        aliases = list(dict.fromkeys([a for a in aliases if a and a.lower() != subject.lower()]))

        def row_matches(row: sqlite3.Row, fields: list[str]) -> bool:
            data = dict(row)
            if data.get("user_id") in matched_user_ids:
                return True
            return contains_subject_mention(_row_text(row, fields), subject, aliases=aliases)

        if "user_memory_facts" in lanes:
            for row in _fetch_rows(conn, "user_memory_facts", guild_id, max_rows):
                if not row_matches(row, ["fact_key", "fact_value"]):
                    continue
                text = _row_text(row, ["fact_key", "fact_value"])
                _add_unique(summary["activitySignals"], "Local memory notes mention this subject; treat as review-only until source authority is confirmed.")
                _add_unique(summary["privateOnlyNotes"], "Local memory facts require owner review before public use.")
                _note_topics(topic_counts, text)
                _record_raw(summary["rawProvenance"], "user_memory_facts", "local_memory_fact", _safe_snippet(text))

        if "user_habits" in lanes:
            for row in _fetch_rows(conn, "user_habits", guild_id, max_rows):
                if not row_matches(row, ["last_topic"]):
                    continue
                text = _row_text(row, ["last_topic"])
                _add_unique(summary["activitySignals"], "Habit/activity traces mention this subject; use only as observed context, not personality facts.")
                _note_topics(topic_counts, text)
                _record_raw(summary["rawProvenance"], "user_habits", "local_behavior_trace", _safe_snippet(text))

        for table in ("relationship_state", "relationship_journal"):
            if table not in lanes:
                continue
            fields = ["last_topic", "trust_stage", "social_stance", "summary", "entry_type"]
            for row in _fetch_rows(conn, table, guild_id, max_rows):
                if not row_matches(row, fields):
                    continue
                text = _row_text(row, fields)
                _add_unique(summary["relationshipSignals"], "Review-only relationship/context signal exists for this subject.")
                _add_unique(summary["privateOnlyNotes"], "Relationship/context notes are private/internal and require owner review before any public wording.")
                _note_topics(topic_counts, text)
                _record_raw(summary["rawProvenance"], table, "local_relationship_trace", _safe_snippet(text))

        if "conversations" in lanes:
            for row in _fetch_rows(conn, "conversations", guild_id, max_rows):
                fields = ["user_name", "content", "channel_name", "channel_policy"]
                if not row_matches(row, fields):
                    continue
                data = dict(row)
                policy = str(data.get("channel_policy") or "unknown").strip().lower() or "unknown"
                text = _row_text(row, ["content"])
                summary["rawProvenance"]["channelPolicies"][policy] += 1
                _note_topics(topic_counts, text)
                if policy in PUBLIC_CONVERSATION_POLICIES:
                    _add_unique(summary["publicSafePossibilities"], "Public-side conversation context exists, but owner review is needed before stating it as public fact.")
                    _add_unique(summary["channelActivity"], "Appears in public-side or broadcast-memory context.")
                    quality = "public_discord_observed"
                else:
                    _add_unique(summary["privateOnlyNotes"], "Non-public, unknown, internal, or sealed conversation context remains review-only.")
                    _add_unique(summary["notPublicYet"], "Conversation context from unknown/internal/sealed lanes is not public-safe evidence.")
                    _add_unique(summary["channelActivity"], "Seen in review-only channel context.")
                    quality = "internal_context_review_only"
                _record_raw(summary["rawProvenance"], "conversations", quality, _safe_snippet(text), channelPolicy=policy)

        if "memory_tiers" in lanes:
            for row in _fetch_rows(conn, "memory_tiers", guild_id, max_rows):
                if not row_matches(row, ["summary", "tier"]):
                    continue
                text = _row_text(row, ["summary", "tier"])
                _add_unique(summary["privateOnlyNotes"], SOURCE_BLIND_REVIEW_NOTE)
                _add_unique(summary["notPublicYet"], "Source-blind memory cannot be used as a public fact without review.")
                _add_unique(summary["channelActivity"], "Seen in source-blind legacy memory; review required.")
                _note_topics(topic_counts, text)
                _record_raw(summary["rawProvenance"], "memory_tiers", "source_blind_memory_trace", _safe_snippet(text))

        if "broadcast_memory" in lanes:
            for row in _fetch_rows(conn, "broadcast_memory", guild_id, max_rows):
                if not row_matches(row, ["cleaned_summary", "summary", "raw_note", "entry_type"]):
                    continue
                data = dict(row)
                text = _row_text(row, ["cleaned_summary", "summary", "raw_note", "entry_type"])
                public_safe = bool(data.get("public_safe")) if "public_safe" in data else False
                status = str(data.get("status") or "active")
                if public_safe and status == "active":
                    _add_unique(summary["publicSafePossibilities"], "Broadcast-memory context may support public wording after owner review.")
                    _add_unique(summary["channelActivity"], "Appears in broadcast memory.")
                else:
                    _add_unique(summary["privateOnlyNotes"], "Broadcast-memory context is not public-safe unless active and marked public-safe.")
                _note_topics(topic_counts, text)
                _record_raw(summary["rawProvenance"], "broadcast_memory", "broadcast_memory_trace", _safe_snippet(text))

        if "rd_context" in lanes and rd_context:
            for item in rd_context[:max_rows]:
                text = " ".join(str(item.get(k) or "") for k in ("text", "summary", "content", "user_request", "response_summary"))
                if not contains_subject_mention(text, subject, aliases=aliases):
                    continue
                _add_unique(summary["privateOnlyNotes"], "Operator-supplied R&D context is internal until reviewed.")
                _add_unique(summary["channelActivity"], "Appears in approved R&D/operator context.")
                _note_topics(topic_counts, text)
                _record_raw(summary["rawProvenance"], "rd_context", "operator_supplied_context", _safe_snippet(text))

    finally:
        conn.close()

    if summary["matchedNames"]:
        summary["identityConfidence"] = "medium"
    elif summary["rawProvenance"]["rawFragments"]:
        summary["identityConfidence"] = "low"
    else:
        summary["identityConfidence"] = "none"
        _add_unique(summary["knownContext"], f"No approved local BNL memory match found for {subject}.")

    if summary["rawProvenance"]["rawFragments"] and not summary["knownContext"]:
        _add_unique(summary["knownContext"], f"Internal BNL context exists for {subject}.")
    if summary["publicSafePossibilities"]:
        _add_unique(summary["notPublicYet"], "Public-safe possibilities are candidates only until owner review confirms wording.")
    if not summary["publicSafePossibilities"]:
        _add_unique(summary["publicSafePossibilities"], "No public-safe facts confirmed yet.")
    _add_unique(summary["notPublicYet"], QUEUE_NOT_CONNECTED_NOTE)

    summary["recurringTopics"] = [f"Observed topic pattern: {topic}." for topic, count in topic_counts.most_common(5) if count > 0]
    source_count = len(summary["rawProvenance"]["rawFragments"])
    if source_count >= 4 and len(summary["rawProvenance"]["sourceCounts"]) >= 2:
        summary["confidence"] = "medium"
    elif source_count:
        summary["confidence"] = "low"
    else:
        summary["confidence"] = "none"
    summary["sourceAuthority"] = [
        "Local profile matches support internal identity matching only." if summary["matchedNames"] else "No local profile authority found.",
        "Public-safe claims require public/broadcast source policy plus owner review.",
        "Relationship, internal, sealed, unknown, and source-blind lanes are review-only.",
    ]

    raw = summary["rawProvenance"]
    raw["sourceCounts"] = dict(raw["sourceCounts"])
    raw["channelPolicies"] = dict(raw["channelPolicies"])
    if output_mode != "admin_internal":
        # Raw provenance remains separate, but public-facing callers should not receive snippets.
        raw["rawFragments"] = [{k: v for k, v in frag.items() if k != "snippet"} for frag in raw["rawFragments"]]
    return summary


def format_entity_activity_summary_response(summary: dict[str, Any]) -> str:
    """Format a concise operator-only Discord readout without raw provenance labels."""

    subject = summary.get("subjectName") or "subject"
    known = "Internal BNL context exists." if (summary.get("rawProvenance") or {}).get("rawFragments") else "No approved local context found."
    useful_bits = []
    if summary.get("matchedNames"):
        useful_bits.append("local profile match")
    if summary.get("relationshipSignals"):
        useful_bits.append("relationship/context review signal")
    if summary.get("channelActivity"):
        useful_bits.append("channel activity signal")
    useful = ", ".join(useful_bits) + " found." if useful_bits else "No strong reusable evidence found yet."
    public_safe = (summary.get("publicSafePossibilities") or ["No public-safe facts confirmed yet."])[0]
    private = (summary.get("privateOnlyNotes") or ["No private-only notes found in approved lanes."])[0]
    missing = ", ".join((summary.get("missingInfo") or [])[:4]) or "none listed"
    action = summary.get("recommendedAction") or "Keep internal and review before public use."
    return (
        f"BNL Entity Activity Summary — {subject}\n\n"
        f"* Current read: {known}\n"
        f"* Useful evidence: {useful}\n"
        f"* Public-safe: {public_safe}\n"
        f"* Private/review-only: {private}\n"
        f"* Missing: {missing}.\n"
        f"* Queue/submission: {QUEUE_NOT_CONNECTED_NOTE}\n"
        f"* Next action: {action}"
    )
