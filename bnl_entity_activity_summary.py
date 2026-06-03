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
from bnl_entity_evidence import (
    ENTITY_EVIDENCE_TABLE,
    build_conversation_safe_summary,
    derive_entity_evidence_for_subject,
    extract_conversation_topic_details,
    get_entity_evidence_for_subject,
    group_entity_evidence_details,
    table_exists as _evidence_table_exists,
)

DEFAULT_ENTITY_SUMMARY_LIMIT = 50
MAX_ENTITY_SUMMARY_LIMIT = 200
DEFAULT_ALLOWED_LANES = {
    "entity_evidence_events",
    "user_profiles",
    "user_memory_facts",
    "user_habits",
    "relationship_state",
    "relationship_journal",
    "memory_tiers",
    "conversations",
    "broadcast_memory",
    "community_presence",
}
PUBLIC_CONVERSATION_POLICIES = {"public_home", "public_context", "public_selective", "broadcast_memory"}
SOURCE_BLIND_REVIEW_NOTE = "Source-blind legacy memory exists; review source and visibility before use."
QUEUE_NOT_CONNECTED_NOTE = "Queue/submission identity is not connected yet."
MAX_SNIPPET_LENGTH = 180
_DISCORD_MENTION_PATTERN = re.compile(r"<[@#!&]?[0-9]{5,}>")
_LONG_ID_PATTERN = re.compile(r"\b\d{15,22}\b")
_URL_PATTERN = re.compile(r"https?://\S+", re.I)
_WORD_PATTERN = re.compile(r"[a-z0-9]+")
_TOPIC_PATTERNS = [
    ("music/community context", re.compile(r"\b(music|song|track|artist|radio|show|broadcast|album|playlist|dj|beat|demo|performance|finished tracks?|wips?|collab(?:oration)?s?)\b", re.I)),
    ("help/support requests", re.compile(r"\b(help|support|assist|fix|issue|question|request|need)\b", re.I)),
    ("source-file/dossier planning context", re.compile(r"\b(source files?|dossiers?|draft|candidate|owner review|public copy|public-safe)\b", re.I)),
    ("creative/project context", re.compile(r"\b(project|design|art|write|story|video|website)\b", re.I)),
    ("community/event context", re.compile(r"\b(community|server|event|chat|room|crowd|member|barcode)\b", re.I)),
]
_MUSIC_PATTERN = re.compile(r"\b(artist|track|song|submission|playlist|queue|radio|show|performance|beat|demo|finished tracks?|wips?|collab(?:oration)?s?|music)\b", re.I)
_COMMUNITY_PATTERN = re.compile(r"\b(community|server|barcode|member|public|conversation|channel|chat|finished tracks?|wips?)\b", re.I)
_BNL_INTERACTION_PATTERN = re.compile(r"\b(bnl|bot|source files?|dossiers?|asked|question|help|review)\b", re.I)



def parse_entity_activity_summary_command(content: str) -> tuple[bool, dict[str, Any] | None, str]:
    """Parse operator entity summary/evidence refresh readout commands."""

    text = (content or "").strip()
    refresh_match = re.match(r"^!bnl\s+entity\s+evidence\s+refresh\s+(.+?)\s*$", text, flags=re.I | re.S)
    if refresh_match:
        subject = normalize_subject_name(refresh_match.group(1))
        if not subject:
            return True, None, "Use: `!bnl entity evidence refresh <subject>`"
        return True, {"subjectName": subject, "refreshEvidence": True}, ""
    match = re.match(r"^!bnl\s+(?:entity\s+summary|source\s+summary)\s+(.+?)\s*$", text, flags=re.I | re.S)
    if not match:
        return False, None, "not_an_entity_activity_summary_command"
    subject_text = match.group(1)
    refresh = False
    parts = [piece.strip() for piece in subject_text.split("|") if piece.strip()]
    if len(parts) > 1:
        subject_text = parts[0]
        for part in parts[1:]:
            key_match = re.match(r"^([a-zA-Z_]+)\s*=\s*(.+)$", part)
            if not key_match:
                return True, None, "Use: `!bnl entity summary <subject> [| refresh=true]`"
            key = key_match.group(1).strip().lower()
            value = key_match.group(2).strip().lower()
            if key == "refresh":
                refresh = value in {"true", "1", "yes", "on"}
            else:
                return True, None, "Supported option: refresh."
    subject = normalize_subject_name(subject_text)
    if not subject:
        return True, None, "Use: `!bnl entity summary <subject>`"
    return True, {"subjectName": subject, "refreshEvidence": refresh}, ""


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


def _channel_display_name(channel_name: Any, policy: str) -> str:
    name = re.sub(r"[^A-Za-z0-9 _-]+", "", str(channel_name or "")).strip().replace(" ", "-").lower()
    if policy in PUBLIC_CONVERSATION_POLICIES and name:
        return f"#{name[:60]}"
    if policy in PUBLIC_CONVERSATION_POLICIES:
        return "approved public-side channel"
    return "review-only channel"


def _topic_labels_for_text(text: str, *, review_only: bool = False) -> list[str]:
    return extract_conversation_topic_details(text, review_only=review_only)


def _conversation_highlight(text: str, *, authored: bool, public_safe: bool, channel_name: Any = None, channel_policy: str = "unknown") -> str:
    return build_conversation_safe_summary(
        text=text,
        authored=authored,
        public_safe=public_safe,
        channel_name=channel_name,
        channel_policy=channel_policy,
    )

def _source_coverage_item(source: str, count: int, status: str = "found") -> dict[str, Any]:
    return {"source": source, "count": int(count or 0), "status": status}


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


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


def _resolve_existing_enrichment_identity(db_path: str, subject: str, guild_id: int | None) -> dict[str, Any]:
    """Reuse Source File enrichment identity matching when available."""

    try:
        from bnl_source_file_enrichment import resolve_enrichment_subject_identity

        return resolve_enrichment_subject_identity(subject, None, db_path, guild_id) or {}
    except Exception:
        return {}



def refresh_entity_evidence_for_subject(db_path: str, subject_name: str, guild_id: int | None = None, limit: int = MAX_ENTITY_SUMMARY_LIMIT) -> dict[str, Any]:
    """Operator/source-path helper to derive structured evidence for one subject."""

    return derive_entity_evidence_for_subject(db_path, subject_name, guild_id=guild_id, limit=limit)


def _evidence_review_label(data: dict[str, Any], channel_display: str, safe_summary: str) -> str:
    relation = str(data.get("relation_to_subject") or "mentioned")
    topic = str(data.get("topic") or "local context")
    public_candidate = bool(data.get("public_safe_candidate"))
    channel_part = f" in {channel_display}" if public_candidate and channel_display and channel_display != "review-only channel" else ""
    return _safe_snippet(f"Review candidate ({relation}, {topic}){channel_part} — {safe_summary}", 220)


def _append_best_evidence(summary: dict[str, Any], data: dict[str, Any], channel_display: str, safe_summary: str) -> None:
    if len(summary.setdefault("bestEvidenceToReview", [])) >= 8:
        return
    # Raw refs/transcripts/IDs stay in rawProvenance; this queue uses only sanitized labels and summaries.
    _add_unique(summary["bestEvidenceToReview"], _evidence_review_label(data, channel_display, safe_summary))


def _cap_review_only_evidence(summary: dict[str, Any], source_blind_limit: int = 2, total_limit: int = 8) -> None:
    capped: list[str] = []
    source_blind_seen = 0
    for item in summary.get("reviewOnlyEvidence") or []:
        is_source_blind = "source-blind" in item.lower() or "source blind" in item.lower()
        if is_source_blind:
            source_blind_seen += 1
            if source_blind_seen > source_blind_limit:
                continue
        _add_unique(capped, item)
        if len(capped) >= total_limit:
            break
    summary["reviewOnlyEvidence"] = capped


def _apply_structured_evidence(summary: dict[str, Any], rows: list[sqlite3.Row], topic_counts: Counter) -> None:
    """Populate summary fields from ranked entity_evidence_events rows."""

    source_blind_review_added = 0
    for row in rows:
        data = dict(row)
        source_table = str(data.get("source_table") or "entity_evidence_events")
        source_label = str(data.get("source_label") or f"{source_table}/structured_evidence")
        kind = str(data.get("evidence_kind") or "")
        safe_summary = _safe_snippet(data.get("safe_summary") or "Structured entity evidence exists for owner review.")
        relation = str(data.get("relation_to_subject") or "mentioned")
        policy = str(data.get("channel_policy") or "unknown")
        topic = str(data.get("topic") or "local context")
        public_candidate = bool(data.get("public_safe_candidate"))
        review_only = bool(data.get("review_only"))
        channel_name = data.get("channel_name") if not review_only else None
        channel_display = _channel_display_name(channel_name, policy) if channel_name else ("approved public-side channel" if public_candidate else "review-only channel")

        summary["rawProvenance"]["sourceLabels"].append(source_label)
        summary["rawProvenance"]["sourceCounts"][source_table] += 1
        summary["rawProvenance"]["channelPolicies"][policy] += 1
        summary["rawProvenance"]["rawFragments"].append({
            "table": source_table,
            "sourceLabel": source_label,
            "sourceQuality": "structured_entity_evidence",
            "evidenceKind": kind,
            "rowId": data.get("source_row_id"),
            "rawRefJson": data.get("raw_ref_json"),
        })
        if topic:
            topic_counts[topic] += 1
        _add_unique(summary["evidenceDetails"], safe_summary)
        if kind != "source_blind_memory_review_only":
            _append_best_evidence(summary, data, channel_display, safe_summary)

        if kind == "profile_match":
            _add_unique(summary["matchedNames"], normalize_subject_name(data.get("subject_name") or summary.get("subjectName") or ""))
            _add_unique(summary["knownContext"], f"Local profile match found for {summary.get('subjectName')}." )
            continue

        if kind in {"authored_public_conversation", "mentioned_public_conversation"}:
            _add_unique(summary["publicSafePossibilities"], "Public-side conversation context exists in approved Discord lanes and may support community/source-file wording after owner review.")
            _add_unique(summary["publicUseCandidates"], "Possible community/source-file candidate based on approved public-side structured evidence, pending owner review.")
            _add_unique(summary["observedChannels"], f"{channel_display} — approved public-side; subject {relation}.")
            _add_unique(summary["channelActivity"], f"Appears in approved public-side structured conversation evidence as {relation} context.")
            _add_unique(summary["conversationHighlights"], safe_summary)
        elif "conversation" in kind:
            _add_unique(summary["privateOnlyNotes"], "Non-public, unknown, internal, or sealed conversation context remains review-only.")
            _add_unique(summary["reviewOnlyEvidence"], safe_summary)
            _add_unique(summary["notPublicYet"], "Conversation context from unknown/internal/sealed lanes is not public-safe evidence.")
            _add_unique(summary["observedChannels"], f"review-only channel — {policy}; subject {relation}.")
            _add_unique(summary["channelActivity"], f"Seen in review-only structured conversation evidence as {relation} context.")
            _add_unique(summary["conversationHighlights"], safe_summary)
        elif kind == "relationship_context_review_only":
            _add_unique(summary["relationshipSignals"], safe_summary)
            _add_unique(summary["privateOnlyNotes"], "Relationship/context notes are private/internal and require owner review before any public wording.")
            _add_unique(summary["reviewOnlyEvidence"], safe_summary)
        elif kind == "source_blind_memory_review_only":
            _add_unique(summary["privateOnlyNotes"], SOURCE_BLIND_REVIEW_NOTE)
            if source_blind_review_added < 2:
                _add_unique(summary["reviewOnlyEvidence"], safe_summary)
                source_blind_review_added += 1
            _add_unique(summary["notPublicYet"], "Source-blind memory cannot be used as a public fact without review.")
        elif kind == "broadcast_memory_signal":
            if public_candidate:
                _add_unique(summary["publicSafePossibilities"], "Broadcast-memory context may support public wording after owner review.")
                _add_unique(summary["publicUseCandidates"], "Active public-safe broadcast memory may support public wording after owner review.")
                _add_unique(summary["observedChannels"], "broadcast memory — active/public-safe; owner review still required.")
            else:
                _add_unique(summary["privateOnlyNotes"], "Broadcast-memory context is not public-safe unless active and marked public-safe.")
                _add_unique(summary["reviewOnlyEvidence"], safe_summary)
        elif kind == "community_presence_signal":
            _add_unique(summary["communitySignals"], safe_summary)
            _add_unique(summary["publicUseCandidates"], "Community presence can inform a possible public role only after owner review confirms identity and wording.")
            _add_unique(summary["publicSafePossibilities"], "Community presence may support public-safe wording after owner review; it is not a confirmed public fact by itself.")
        elif review_only:
            _add_unique(summary["reviewOnlyEvidence"], safe_summary)
            _add_unique(summary["privateOnlyNotes"], safe_summary)

        if data.get("music_signal"):
            _add_unique(summary["musicSignals"], "Structured evidence connects this subject to music/radio/show context; queue identity still needs review.")
        if data.get("community_signal"):
            _add_unique(summary["communitySignals"], "Structured evidence places this subject in BARCODE/community context; owner review must confirm public wording.")
        if data.get("bnl_interaction"):
            _add_unique(summary["bnlInteractionSignals"], f"Structured evidence shows BNL-related interaction context; subject was {relation}.")


def _structured_evidence_rows(conn: sqlite3.Connection, subject: str, guild_id: int | None, lanes: set[str], max_rows: int) -> list[sqlite3.Row]:
    if "entity_evidence_events" not in lanes or not _evidence_table_exists(conn, ENTITY_EVIDENCE_TABLE):
        return []
    return get_entity_evidence_for_subject(conn, subject, guild_id=guild_id, limit=max_rows)

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
        "observedChannels": [],
        "channelActivity": [],
        "conversationHighlights": [],
        "representativeEvidence": [],
        "activityFrequencySummary": {},
        "topChannels": [],
        "topTopicDetails": [],
        "recentActivitySummary": "",
        "authoredVsMentionedSummary": "",
        "topicBreakdown": [],
        "recurringTopics": [],
        "interactionSignals": [],
        "bnlInteractionSignals": [],
        "musicSignals": [],
        "communitySignals": [],
        "sourceCoverage": [],
        "evidenceDetails": [],
        "bestEvidenceToReview": [],
        "relationshipSignals": [],
        "publicSafePossibilities": [],
        "publicUseCandidates": [],
        "privateOnlyNotes": [],
        "reviewOnlyEvidence": [],
        "notPublicYet": [],
        "missingInfo": ["public role", "public links", "confirmed display name", "queue/submission identity"],
        "sourceAuthority": [],
        "recommendedAction": "Review the best public-side conversation and community/music signals before drafting public dossier copy.",
        "confidence": "low",
        "queueSubmissionStatus": "not_connected",
        "queueSubmissionNote": QUEUE_NOT_CONNECTED_NOTE,
        "rawProvenance": {"sourceLabels": [], "sourceCounts": Counter(), "rawFragments": [], "channelPolicies": Counter()},
    }
    topic_counts: Counter = Counter()
    matched_user_ids: set[Any] = set()
    aliases: list[str] = []
    fallback_activity_rows: list[dict[str, Any]] = []

    if not subject:
        summary["privateOnlyNotes"].append("No subject supplied for summary.")
        summary["rawProvenance"]["sourceCounts"] = {}
        summary["rawProvenance"]["channelPolicies"] = {}
        return summary

    enrichment_identity = _resolve_existing_enrichment_identity(db_path, subject, guild_id)
    for user_id in enrichment_identity.get("_matchedUserIds") or []:
        matched_user_ids.add(user_id)
    for label in enrichment_identity.get("matchedIdentityLabels") or enrichment_identity.get("aliasLabels") or []:
        clean_label = normalize_subject_name(str(label or ""))
        if clean_label:
            aliases.append(clean_label)
            _add_unique(summary["matchedNames"], clean_label)
    if enrichment_identity.get("matchedUserIdCount"):
        _add_unique(summary["evidenceDetails"], "Existing Source File enrichment identity matching linked this subject to local profile/community identity context for review.")

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        structured_rows = _structured_evidence_rows(conn, subject, guild_id, lanes, max_rows)
        if structured_rows:
            _apply_structured_evidence(summary, structured_rows, topic_counts)
            grouped_details = group_entity_evidence_details(structured_rows)
            for item in grouped_details.get("topicBreakdown") or []:
                _add_unique(summary["topicBreakdown"], item)
            for item in grouped_details.get("representativeEvidence") or []:
                _add_unique(summary["representativeEvidence"], item)
                _add_unique(summary["conversationHighlights"], item)
                _add_unique(summary["evidenceDetails"], item)
            summary["activityFrequencySummary"] = grouped_details.get("activityFrequencySummary") or {}
            summary["topChannels"] = grouped_details.get("topChannels") or []
            summary["topTopicDetails"] = grouped_details.get("topTopicDetails") or []
            summary["recentActivitySummary"] = grouped_details.get("recentActivitySummary") or ""
            summary["authoredVsMentionedSummary"] = grouped_details.get("authoredVsMentionedSummary") or ""
            matched_user_ids.update(row["matched_user_id"] for row in structured_rows if "matched_user_id" in row.keys() and row["matched_user_id"] not in (None, ""))
            for row in structured_rows:
                if row["evidence_kind"] == "profile_match":
                    clean_name = normalize_subject_name(row["subject_name"] or subject)
                    _add_unique(summary["matchedNames"], clean_name)
                    aliases.append(clean_name)
            lanes = set(lanes) - {
                "user_profiles",
                "user_memory_facts",
                "user_habits",
                "relationship_state",
                "relationship_journal",
                "memory_tiers",
                "conversations",
                "broadcast_memory",
                "community_presence",
            }

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
                    _add_unique(summary["evidenceDetails"], f"BNL matched {subject} to an existing local profile label for internal review.")
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
                _add_unique(summary["evidenceDetails"], "Local memory notes mention this subject, but the original source must be reviewed before reuse.")
                _add_unique(summary["privateOnlyNotes"], "Local memory facts require owner review before public use.")
                _add_unique(summary["reviewOnlyEvidence"], "Local memory facts require owner review before public use.")
                _note_topics(topic_counts, text)
                _record_raw(summary["rawProvenance"], "user_memory_facts", "local_memory_fact", _safe_snippet(text))

        if "user_habits" in lanes:
            for row in _fetch_rows(conn, "user_habits", guild_id, max_rows):
                if not row_matches(row, ["last_topic"]):
                    continue
                text = _row_text(row, ["last_topic"])
                _add_unique(summary["activitySignals"], "Habit/activity traces mention this subject; use only as observed context, not personality facts.")
                _add_unique(summary["evidenceDetails"], "Habit/activity traces mention this subject as context only, not as a public trait or role.")
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
                relationship_note = "BNL found prior relationship/context notes, but they are private review-only."
                _add_unique(summary["relationshipSignals"], relationship_note)
                _add_unique(summary["privateOnlyNotes"], "Relationship/context notes are private/internal and require owner review before any public wording.")
                _add_unique(summary["reviewOnlyEvidence"], relationship_note)
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
                channel_name = data.get("channel_name") or ""
                channel_display = _channel_display_name(channel_name, policy)
                authored = data.get("user_id") in matched_user_ids or contains_subject_mention(str(data.get("user_name") or ""), subject, aliases=aliases)
                relation = "authored" if authored else "mentioned"
                timestamp = str(data.get("timestamp") or "")
                topics = _topic_labels_for_text(text, review_only=policy not in PUBLIC_CONVERSATION_POLICIES)
                summary["rawProvenance"]["channelPolicies"][policy] += 1
                _note_topics(topic_counts, text)
                if policy in PUBLIC_CONVERSATION_POLICIES:
                    fallback_activity_rows.append({
                        "public_safe_candidate": True,
                        "review_only": False,
                        "relation_to_subject": relation,
                        "topic": topics[0] if topics else "Community/server participation",
                        "channel_name": channel_name,
                        "channel_policy": policy,
                        "safe_summary": _conversation_highlight(text, authored=authored, public_safe=True, channel_name=channel_name, channel_policy=policy),
                        "observed_at": timestamp,
                        "evidence_kind": f"{relation}_public_conversation",
                    })
                    _add_unique(summary["publicSafePossibilities"], "Public-side conversation context exists in approved Discord lanes and may support community/source-file wording after owner review.")
                    _add_unique(summary["publicUseCandidates"], "Possible community/source-file candidate based on approved public-side context, pending owner review.")
                    _add_unique(summary["channelActivity"], f"Appears in approved public-side conversation context in {channel_display} as {relation} context.")
                    _add_unique(summary["observedChannels"], f"{channel_display} — approved public-side; subject {relation}.")
                    _add_unique(summary["conversationHighlights"], _conversation_highlight(text, authored=authored, public_safe=True, channel_name=channel_name, channel_policy=policy))
                    _add_unique(summary["evidenceDetails"], f"{channel_display}: subject {relation} approved public-side {topics[0].replace(' discussion', '').lower()} discussion.")
                    if _MUSIC_PATTERN.search(text):
                        _add_unique(summary["musicSignals"], "Approved context connects this subject to music, track, queue, radio, or show discussion; queue identity still needs review.")
                    if _COMMUNITY_PATTERN.search(text):
                        _add_unique(summary["communitySignals"], "Approved context places this subject in BARCODE/community conversation; owner review must confirm public wording.")
                    if _BNL_INTERACTION_PATTERN.search(text):
                        _add_unique(summary["bnlInteractionSignals"], f"BNL-related discussion appears in approved public-side context; subject was {relation}.")
                    quality = "public_discord_observed"
                else:
                    fallback_activity_rows.append({
                        "public_safe_candidate": False,
                        "review_only": True,
                        "relation_to_subject": relation,
                        "topic": topics[0] if topics else "Local context",
                        "channel_name": None,
                        "channel_policy": policy,
                        "safe_summary": _conversation_highlight(text, authored=authored, public_safe=False, channel_name=channel_name, channel_policy=policy),
                        "observed_at": timestamp,
                        "evidence_kind": f"{relation}_review_only_conversation",
                    })
                    _add_unique(summary["privateOnlyNotes"], "Non-public, unknown, internal, or sealed conversation context remains review-only.")
                    _add_unique(summary["reviewOnlyEvidence"], "Internal/unknown/sealed conversation context remains review-only and cannot become a public-safe candidate.")
                    _add_unique(summary["notPublicYet"], "Conversation context from unknown/internal/sealed lanes is not public-safe evidence.")
                    _add_unique(summary["channelActivity"], f"Seen in review-only channel context as {relation} context.")
                    _add_unique(summary["observedChannels"], f"review-only channel — {policy}; subject {relation}.")
                    _add_unique(summary["conversationHighlights"], _conversation_highlight(text, authored=authored, public_safe=False, channel_name=channel_name, channel_policy=policy))
                    quality = "internal_context_review_only"
                for topic in topics:
                    _add_unique(summary["topicBreakdown"], f"{topic}: observed in conversation context.")
                _record_raw(summary["rawProvenance"], "conversations", quality, _safe_snippet(text), channelPolicy=policy, channelName=channel_name, subjectRelation=relation, timestamp=timestamp)

        if "memory_tiers" in lanes:
            for row in _fetch_rows(conn, "memory_tiers", guild_id, max_rows):
                if not row_matches(row, ["summary", "tier"]):
                    continue
                text = _row_text(row, ["summary", "tier"])
                _add_unique(summary["privateOnlyNotes"], SOURCE_BLIND_REVIEW_NOTE)
                _add_unique(summary["reviewOnlyEvidence"], SOURCE_BLIND_REVIEW_NOTE)
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
                    _add_unique(summary["publicUseCandidates"], "Active public-safe broadcast memory may support public wording after owner review.")
                    _add_unique(summary["channelActivity"], "Appears in broadcast memory.")
                    _add_unique(summary["observedChannels"], "broadcast memory — active/public-safe; owner review still required.")
                    if _MUSIC_PATTERN.search(text):
                        _add_unique(summary["musicSignals"], "Broadcast memory connects this subject to radio/show/music context; owner review must confirm wording.")
                else:
                    _add_unique(summary["privateOnlyNotes"], "Broadcast-memory context is not public-safe unless active and marked public-safe.")
                    _add_unique(summary["reviewOnlyEvidence"], "Broadcast-memory context is review-only unless active and marked public-safe.")
                _note_topics(topic_counts, text)
                _record_raw(summary["rawProvenance"], "broadcast_memory", "broadcast_memory_trace", _safe_snippet(text))

        if "community_presence" in lanes:
            for row in _fetch_rows(conn, "community_presence", guild_id, max_rows):
                if not row_matches(row, ["subject_key", "display_name", "connection_notes", "evidence_snippets"]):
                    continue
                data = dict(row)
                text = _row_text(row, ["display_name", "connection_notes", "evidence_snippets"])
                mention_count = data.get("mention_count") if "mention_count" in data else None
                direct_count = data.get("direct_interaction_count") if "direct_interaction_count" in data else None
                last_seen = data.get("last_seen_at") if "last_seen_at" in data else None
                detail = "Community presence record exists for this subject"
                if mention_count not in (None, ""):
                    detail += f" with {_safe_int(mention_count)} observed mention(s)"
                if direct_count not in (None, ""):
                    detail += f" and {_safe_int(direct_count)} direct interaction marker(s)"
                if last_seen:
                    detail += f"; latest local presence marker {last_seen}"
                detail += "."
                _add_unique(summary["communitySignals"], detail)
                _add_unique(summary["evidenceDetails"], detail)
                _add_unique(summary["publicUseCandidates"], "Community presence can inform a possible public role only after owner review confirms identity and wording.")
                _add_unique(summary["publicSafePossibilities"], "Community presence may support public-safe wording after owner review; it is not a confirmed public fact by itself.")
                _note_topics(topic_counts, text)
                _record_raw(summary["rawProvenance"], "community_presence", "community_presence_trace", _safe_snippet(text), lastSeenAt=last_seen)

        if "rd_context" in lanes and rd_context:
            for item in rd_context[:max_rows]:
                text = " ".join(str(item.get(k) or "") for k in ("text", "summary", "content", "user_request", "response_summary"))
                if not contains_subject_mention(text, subject, aliases=aliases):
                    continue
                _add_unique(summary["privateOnlyNotes"], "Operator-supplied R&D context is internal until reviewed.")
                _add_unique(summary["reviewOnlyEvidence"], "Operator-supplied R&D context is internal until reviewed.")
                _add_unique(summary["channelActivity"], "Appears in approved R&D/operator context.")
                _note_topics(topic_counts, text)
                _record_raw(summary["rawProvenance"], "rd_context", "operator_supplied_context", _safe_snippet(text))

    finally:
        conn.close()

    if fallback_activity_rows and not summary.get("activityFrequencySummary"):
        grouped_details = group_entity_evidence_details(fallback_activity_rows)
        for item in grouped_details.get("topicBreakdown") or []:
            _add_unique(summary["topicBreakdown"], item)
        for item in grouped_details.get("representativeEvidence") or []:
            _add_unique(summary["representativeEvidence"], item)
            _add_unique(summary["conversationHighlights"], item)
            _add_unique(summary["evidenceDetails"], item)
        summary["activityFrequencySummary"] = grouped_details.get("activityFrequencySummary") or {}
        summary["topChannels"] = grouped_details.get("topChannels") or []
        summary["topTopicDetails"] = grouped_details.get("topTopicDetails") or []
        summary["recentActivitySummary"] = grouped_details.get("recentActivitySummary") or ""
        summary["authoredVsMentionedSummary"] = grouped_details.get("authoredVsMentionedSummary") or ""

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
        _add_unique(summary["notPublicYet"], "Public-safe possibilities are review candidates, not confirmed public facts or ready dossier copy.")
        _add_unique(summary["notPublicYet"], "Owner/admin review is still required; many candidate rows may only be supporting evidence.")
    if not summary["publicSafePossibilities"]:
        _add_unique(summary["publicSafePossibilities"], "No public-safe facts confirmed yet.")
    if not summary["publicUseCandidates"] and any(item != "No public-safe facts confirmed yet." for item in summary["publicSafePossibilities"]):
        _add_unique(summary["publicUseCandidates"], "Possible community/source-file candidate, pending owner review.")
    _add_unique(summary["notPublicYet"], QUEUE_NOT_CONNECTED_NOTE)

    summary["recurringTopics"] = [f"Observed topic pattern: {topic}." for topic, count in topic_counts.most_common(5) if count > 0]
    has_detail_breakdown = any("public-side authored" in item or "public-side mentioned" in item for item in summary["topicBreakdown"])
    if not has_detail_breakdown:
        for topic, count in topic_counts.most_common(8):
            if count > 0:
                _add_unique(summary["topicBreakdown"], f"{topic}: {count} approved/reviewed source row(s).")
    if not summary["musicSignals"]:
        _add_unique(summary["musicSignals"], "No queue/submission identity is connected yet; do not claim music submissions or counts from this summary.")
    if not summary["communitySignals"] and summary["rawProvenance"]["rawFragments"]:
        _add_unique(summary["communitySignals"], "Internal context exists, but community role or public identity still needs owner review.")
    for note in summary["privateOnlyNotes"]:
        _add_unique(summary["reviewOnlyEvidence"], note)
    _cap_review_only_evidence(summary)
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
    summary["sourceCoverage"] = [
        _source_coverage_item(source, count)
        for source, count in Counter(raw["sourceCounts"]).most_common()
    ]
    if raw["channelPolicies"]:
        summary["sourceCoverage"].append({"source": "channel_policy", "counts": dict(raw["channelPolicies"]), "status": "found"})
    raw["sourceCounts"] = dict(raw["sourceCounts"])
    raw["channelPolicies"] = dict(raw["channelPolicies"])
    if output_mode != "admin_internal":
        # Raw provenance remains separate, but public-facing callers should not receive snippets.
        raw["rawFragments"] = [{k: v for k, v in frag.items() if k != "snippet"} for frag in raw["rawFragments"]]
    return summary


def format_entity_activity_summary_response(summary: dict[str, Any]) -> str:
    """Format a concise operator-only Discord readout without raw provenance labels."""

    subject = summary.get("subjectName") or "subject"
    has_context = bool((summary.get("rawProvenance") or {}).get("rawFragments"))
    if has_context:
        if summary.get("publicSafePossibilities") and summary.get("publicSafePossibilities") != ["No public-safe facts confirmed yet."]:
            known = f"Internal BNL context exists for {subject}. Structured evidence exists, but public role and queue/submission identity are not confirmed."
        else:
            known = f"Internal BNL context exists for {subject}. Public role and queue/submission identity are not confirmed."
    else:
        known = f"No approved local context found for {subject}."

    useful_bits: list[str] = []
    if summary.get("matchedNames"):
        useful_bits.append("Local profile match found.")
    useful_bits.extend((summary.get("representativeEvidence") or summary.get("conversationHighlights") or [])[:3])
    if summary.get("authoredVsMentionedSummary"):
        useful_bits.append(summary["authoredVsMentionedSummary"])
    useful_bits.extend([item for item in (summary.get("observedChannels") or []) if "#" in item][:2])
    useful_bits.extend((summary.get("musicSignals") or [])[:1])
    useful_bits.extend((summary.get("communitySignals") or [])[:2])
    useful_bits.extend((summary.get("bnlInteractionSignals") or [])[:1])
    if summary.get("relationshipSignals"):
        useful_bits.append("Relationship/context notes exist but are private review-only.")
    useful_bits = useful_bits[:7] or ["No strong reusable evidence found yet."]

    best_review = (summary.get("bestEvidenceToReview") or summary.get("topicBreakdown") or summary.get("recurringTopics") or ["No clear evidence review queue yet."])[:6]
    public_safe = (summary.get("publicUseCandidates") or summary.get("publicSafePossibilities") or ["No public-safe facts confirmed yet."])[:3]
    candidate_note = "Public-safe candidate events are review candidates, not confirmed public facts; owner/admin review is required before reuse."
    if candidate_note not in public_safe:
        public_safe.append(candidate_note)
    private = (summary.get("reviewOnlyEvidence") or summary.get("privateOnlyNotes") or ["No private-only notes found in approved lanes."])[:6]
    missing_items = (summary.get("missingInfo") or [])[:6]
    action = summary.get("recommendedAction") or "Keep internal and review before public use."

    def bullets(items: list[str]) -> str:
        return "\n".join(f"- {item}" for item in items)

    return (
        f"BNL Entity Activity Summary — {subject}\n\n"
        f"Current read:\n{known}\n\n"
        f"Useful evidence:\n{bullets(useful_bits)}\n\n"
        f"Best evidence to review:\n{bullets(best_review)}\n\n"
        f"Public-safe candidates:\n{bullets(public_safe)}\n\n"
        f"Review-only:\n{bullets(private)}\n\n"
        f"Missing:\n{bullets(missing_items or ['none listed'])}\n\n"
        f"Queue/submission:\n{QUEUE_NOT_CONNECTED_NOTE}\n\n"
        f"Next action:\n{action}"
    )
