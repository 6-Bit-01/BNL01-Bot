"""Shared BNL entity activity summary builder.

This module builds a structured, review-safe readout from approved local BNL
memory lanes. It is intentionally backend-only: it does not write memory, create
Source Files/dossiers, call website ingest, publish, or fetch Discord history.
"""

from __future__ import annotations

import json
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
    _website_safe_topic_label,
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




_TOOL_OR_PLATFORM_TERMS = {
    "suno": "Suno",
    "udio": "Udio",
    "ableton": "Ableton",
    "fl studio": "FL Studio",
    "bandcamp": "Bandcamp",
    "soundcloud": "SoundCloud",
    "spotify": "Spotify",
    "youtube": "YouTube",
    "discord": "Discord",
}
_NAMED_TOPIC_ALLOWLIST = {
    "orion": "Orion",
}
_NAMED_TOPIC_STOPWORDS = {
    "approved", "subject", "review", "context", "community", "source", "file", "dossier", "music",
    "conversation", "public", "internal", "private", "unknown", "queue", "submission", "identity",
    "owner", "admin", "discord", "channel", "support", "help", "possible", "recurring", "tool", "bnl",
}

_ENTITY_NAME_PATTERN = re.compile(r"(?<![A-Za-z0-9])(?:[A-Z][A-Za-z0-9]*(?:[-'’][A-Za-z0-9]+)?)(?:\s+(?:[A-Z][A-Za-z0-9]*(?:[-'’][A-Za-z0-9]+)?))*")
_MUSIC_SUBMISSION_LANGUAGE_PATTERN = re.compile(r"\b(song|songs|track|tracks|submitted|submission|queue|played|priority|payment|paid|suno|udio|music)\b", re.I)



_SUBJECT_INTELLIGENCE_STOPWORDS = {
    "possible", "reviewed", "evidence", "tool", "platform", "queue", "submission", "source", "context",
    "confirm", "public", "private", "review", "owner", "admin", "music", "community", "pattern",
    "activity", "signal", "history", "this", "subject", "channel", "conversation", "dossier", "file",
    "approved", "internal", "candidate", "notes", "known", "facts", "relationship", "memory", "recent",
    "user", "users", "member", "members", "someone", "something", "anyone", "everyone", "everything",
    "hey", "hi", "hello", "still", "speaking", "transmitted", "directly", "operating", "continuity",
    "pm", "am", "pacific", "friday", "saturday", "sunday", "monday", "tuesday", "wednesday", "thursday",
    "tonight", "today", "tomorrow", "yesterday", "morning", "afternoon", "evening", "night", "bit", "bits",
    "entire", "civilizations", "communicate", "purely", "language", "sustained", "analytical", "input",
    "precision", "matters", "tradeoff", "value", "trap", "incidental",
    # Title-case sentence starters / filler words that should never become named topics by repetition.
    "the", "a", "an", "and", "or", "but", "if", "then", "there", "that", "these", "those",
    "with", "without", "from", "to", "for", "of", "in", "on", "at", "by", "as",
    "is", "are", "was", "were", "be", "been", "being", "not", "no", "yes",
    "good", "bad", "still", "just", "now", "also", "because", "before", "after", "again", "maybe",
}
_PRONOUN_OR_ADDRESS_TERMS = {
    "i", "me", "my", "mine", "we", "us", "our", "ours", "you", "your", "yours",
    "he", "him", "his", "she", "her", "hers", "they", "them", "their", "theirs",
    "it", "its", "this", "that", "these", "those", "someone", "something", "anyone", "everyone", "everything",
    "user", "users", "member", "members", "hey", "hi", "hello",
}
_SYSTEM_OR_PERSONA_TERMS = {
    "bnl", "bnl-01", "bot", "network", "barcode network", "barcode radio", "discord", "server", "channel",
    "source file", "dossier", "admin", "owner", "public", "private", "review", "barcode", "6 bit", "six bit", "bit", "bit's",
}
_GRAMMAR_OR_FILLER_TERMS = {
    "the", "a", "an", "and", "or", "but", "if", "then", "there", "with", "without",
    "from", "to", "for", "of", "in", "on", "at", "by", "as", "is", "are", "was", "were",
    "be", "been", "being", "not", "no", "yes", "good", "bad", "still", "just", "now", "also",
    "speaking", "transmitted", "directly", "operating", "continuity", "pm", "am", "pacific",
    "because", "before", "after", "again", "maybe", "possible", "reviewed", "evidence", "context",
    "conversation", "community", "activity", "pattern", "candidate", "known", "facts", "source", "file",
    "entire", "civilizations", "communicate", "purely", "language", "sustained", "analytical", "input",
    "precision", "matters", "tradeoff", "value", "trap", "incidental",
}
_SUBJECT_INTELLIGENCE_STOPWORDS.update(_PRONOUN_OR_ADDRESS_TERMS | _SYSTEM_OR_PERSONA_TERMS | _GRAMMAR_OR_FILLER_TERMS)
_NAMED_TOPIC_BLOCKED_TERMS = _PRONOUN_OR_ADDRESS_TERMS | _SYSTEM_OR_PERSONA_TERMS | _GRAMMAR_OR_FILLER_TERMS | _SUBJECT_INTELLIGENCE_STOPWORDS | _NAMED_TOPIC_STOPWORDS
_SUBJECT_INTELLIGENCE_PRIORITY_PREFIXES = (
    "Recurring named topic:",
    "Conversation theme:",
    "Activity pattern:",
    "Evidence digest:",
    "Recurring conversation pattern:",
    "BNL interaction pattern:",
    "Tool/platform mention:",
)
_RECURRING_PHRASE_STOPWORDS = _SUBJECT_INTELLIGENCE_STOPWORDS | {"appears", "mentioned", "connected", "review", "before", "after", "with", "from", "about"}
_DOMAIN_PATTERN = re.compile(r"(?:https?://)?(?:www\.)?([a-z0-9][a-z0-9-]*(?:\.[a-z0-9][a-z0-9-]*)+)(?:/[^\s<>()\[\]{}]*)?", re.I)
_CODE_MARKER_PATTERN = re.compile(r"\b(?:0x[A-Fa-f0-9]{2,}|[A-Z][A-Z0-9]{2,}(?:-[A-Z0-9]+)*)\b")
_THROUGH_PATTERN = re.compile(r"\b([A-Z][A-Za-z0-9_-]{2,}(?:\s+[A-Z][A-Za-z0-9_-]{2,}){0,3})\s+(?:through|via|speaking\s+through|relayed\s+through)\s+([A-Z][A-Za-z0-9_-]{2,}(?:\s+[A-Z][A-Za-z0-9_-]{2,}){0,2})\b", re.I)
_HERE_PATTERN = re.compile(r"\b([A-Z][A-Za-z0-9_-]{2,}(?:\s+[A-Z][A-Za-z0-9_-]{2,}){0,1})\s+here\b", re.I)
_THROUGH_TRAILING_ACTION_PATTERN = re.compile(r"(?:\s+(?:speaking(?:\s+directly)?|talking|transmitted|transmitting|relayed|operating|active))+$", re.I)
_SAID_PATTERN = re.compile(r"\b([A-Z][A-Za-z0-9_-]{2,})\s+(?:said|says|asks|asked)\b")


def _safe_int_value(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _row_value(row: sqlite3.Row | dict[str, Any], key: str, default: Any = None) -> Any:
    if isinstance(row, sqlite3.Row):
        return row[key] if key in row.keys() else default
    return row.get(key, default)


def _select_existing_columns(conn: sqlite3.Connection, table: str, wanted: list[str]) -> list[str]:
    cols = _columns(conn, table)
    return [col for col in wanted if col in cols]


def _safe_table_row_text(conn: sqlite3.Connection, table: str, row_id: Any) -> str:
    """Rehydrate local source-row text for extraction only; never expose it directly."""

    if table != "conversations" or not _table_exists(conn, table):
        return ""
    row_int = _safe_int_value(row_id)
    if row_int is None:
        return ""
    cols = _select_existing_columns(conn, table, ["content"])
    if not cols:
        return ""
    row = conn.execute("SELECT content FROM conversations WHERE rowid=? OR id=? LIMIT 1", (row_int, row_int)).fetchone()
    return str(row["content"] or "") if row else ""


def _parse_raw_ref(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    try:
        parsed = json.loads(str(value or "{}"))
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def _is_public_intelligence_row(source: str, data: dict[str, Any]) -> bool:
    if source == "conversations":
        return str(data.get("channel_policy") or "").lower() in PUBLIC_CONVERSATION_POLICIES
    if source == "entity_evidence_events":
        return bool(data.get("public_safe_candidate")) and not bool(data.get("review_only"))
    if source == "broadcast_memory":
        return bool(data.get("public_safe")) and str(data.get("status") or "active").lower() == "active"
    if source == "community_presence":
        return True
    return False


def extract_full_text_from_source_row(conn: sqlite3.Connection, source: str, row: sqlite3.Row | dict[str, Any]) -> str:
    """Return fuller local text for internal scoring, preferring rehydrated conversation content."""

    data = dict(row) if isinstance(row, sqlite3.Row) else dict(row or {})
    if source == "entity_evidence_events":
        raw_ref = _parse_raw_ref(data.get("raw_ref_json"))
        table = str(raw_ref.get("table") or raw_ref.get("sourceTable") or "")
        row_id = raw_ref.get("row_id", raw_ref.get("sourceRowId"))
        rehydrated = _safe_table_row_text(conn, table, row_id)
        if rehydrated:
            return rehydrated
        return " ".join(str(v or "") for v in (data.get("safe_summary"), raw_ref.get("snippet"), raw_ref.get("summary"), data.get("topic"))).strip()
    fields_by_source = {
        "conversations": ["content"],
        "relationship_journal": ["entry_type", "summary"],
        "relationship_state": ["trust_stage", "social_stance", "last_topic"],
        "user_memory_facts": ["fact_key", "fact_value"],
        "memory_tiers": ["tier", "summary"],
        "broadcast_memory": ["entry_type", "cleaned_summary", "summary", "raw_note"],
        "community_presence": ["display_name", "connection_notes", "evidence_snippets", "category"],
    }
    return " ".join(str(data.get(field) or "") for field in fields_by_source.get(source, [])).strip()


def collect_subject_intelligence_rows(
    conn: sqlite3.Connection,
    subject: str,
    guild_id: int | None,
    *,
    aliases: list[str] | None = None,
    matched_user_ids: set[Any] | None = None,
    structured_rows: list[sqlite3.Row] | None = None,
    max_rows: int = 200,
) -> list[dict[str, Any]]:
    """Collect selected subject-linked local rows for internal recurring-subject scoring."""

    rows: list[dict[str, Any]] = []
    matched_ids = {_safe_int_value(v) for v in (matched_user_ids or set())}
    matched_ids.discard(None)
    try:
        from bnl_entity_intelligence import _known_entity_names, _subject_row_scope, subject_key as _ei_subject_key
        scoped_known_names = _known_entity_names(conn, guild_id, subject)
        scoped_subject_key = _ei_subject_key(subject)
    except Exception:
        _subject_row_scope = None
        scoped_known_names = set()
        scoped_subject_key = ""

    def scoped_accept(source: str, data: dict[str, Any], text: str, *, authored: bool = False) -> tuple[bool, str]:
        if _subject_row_scope is None:
            return authored or contains_subject_mention(text, subject, aliases=aliases), "legacy_unscoped"
        scope, _reason = _subject_row_scope(source, data, subject, scoped_subject_key, {int(v) for v in matched_ids if v is not None}, scoped_known_names)
        # Legacy recurring subjects are diagnostic-only unless the row is strongly subject-owned/keyed/authored.
        return scope in {"subject_owned", "subject_keyed", "subject_authored", "subject_direct_evidence"}, scope

    def matches_text(text: str) -> bool:
        return contains_subject_mention(text, subject, aliases=aliases)

    def add(source: str, row: sqlite3.Row | dict[str, Any], relation: str = "mentioned") -> None:
        data = dict(row) if isinstance(row, sqlite3.Row) else dict(row or {})
        text = extract_full_text_from_source_row(conn, source, data)
        if not text:
            return
        accepted, scope = scoped_accept(source, data, text, authored=relation in {"authored", "matched"})
        rows.append({"source": source, "text": text, "publicSafe": _is_public_intelligence_row(source, data) and accepted, "relation": relation, "scope": scope, "legacyDiagnosticOnly": not accepted})

    for row in structured_rows or []:
        add("entity_evidence_events", row, str(_row_value(row, "relation_to_subject", "matched")))

    if _table_exists(conn, "conversations"):
        select_cols = _select_existing_columns(conn, "conversations", ["id", "user_id", "author_id", "discord_user_id", "member_id", "user_name", "author_name", "channel_policy", "content", "timestamp"])
        if select_cols:
            order_col = "timestamp" if "timestamp" in _columns(conn, "conversations") else "id" if "id" in _columns(conn, "conversations") else "rowid"
            query = f"SELECT rowid AS _rowid, {', '.join(select_cols)} FROM conversations WHERE guild_id=? ORDER BY {order_col} DESC LIMIT ?" if guild_id is not None else f"SELECT rowid AS _rowid, {', '.join(select_cols)} FROM conversations ORDER BY {order_col} DESC LIMIT ?"
            params = (guild_id, max_rows * 8) if guild_id is not None else (max_rows * 8,)
            for row in conn.execute(query, params):
                data = dict(row)
                author_id_match = any(_safe_int_value(data.get(col)) in matched_ids for col in ("user_id", "author_id", "discord_user_id", "member_id"))
                author_name_match = any(contains_subject_mention(str(data.get(col) or ""), subject, aliases=aliases) for col in ("user_name", "author_name"))
                content_match = matches_text(str(data.get("content") or ""))
                if author_id_match or author_name_match or content_match:
                    relation = "authored" if author_id_match or author_name_match else "mentioned"
                    if relation == "authored" or scoped_accept("conversations", data, str(data.get("content") or ""), authored=False)[0]:
                        add("conversations", data, relation)
                if len(rows) >= max_rows:
                    break

    table_fields = {
        "relationship_journal": ["user_id", "entry_type", "summary"],
        "relationship_state": ["user_id", "trust_stage", "social_stance", "last_topic"],
        "user_memory_facts": ["user_id", "fact_key", "fact_value"],
        "memory_tiers": ["user_id", "tier", "summary"],
        "broadcast_memory": ["cleaned_summary", "summary", "raw_note", "entry_type", "public_safe", "status"],
        "community_presence": ["subject_key", "display_name", "connection_notes", "evidence_snippets", "category", "user_id", "discord_user_id", "member_id"],
    }
    for table, wanted in table_fields.items():
        if not _table_exists(conn, table):
            continue
        select_cols = _select_existing_columns(conn, table, wanted)
        if not select_cols:
            continue
        where = " WHERE guild_id=?" if guild_id is not None and "guild_id" in _columns(conn, table) else ""
        params: tuple[Any, ...] = (guild_id,) if where else ()
        for row in conn.execute(f"SELECT {', '.join(select_cols)} FROM {table}{where} LIMIT ?", (*params, max_rows * 4)):
            data = dict(row)
            id_match = any(_safe_int_value(data.get(col)) in matched_ids for col in ("user_id", "discord_user_id", "member_id"))
            text = extract_full_text_from_source_row(conn, table, data)
            if id_match or matches_text(text):
                relation = "matched" if id_match else "mentioned"
                if id_match or scoped_accept(table, data, text, authored=False)[0]:
                    add(table, data, relation)
            if len(rows) >= max_rows * 2:
                break
    return rows[: max_rows * 2]


def build_subject_intelligence_corpus(rows: list[dict[str, Any]]) -> dict[str, Any]:
    public = [str(row.get("text") or "") for row in rows if row.get("publicSafe")]
    review = [str(row.get("text") or "") for row in rows if not row.get("publicSafe")]
    counts = Counter(str(row.get("source") or "unknown") for row in rows)
    return {"publicTexts": public, "reviewOnlyTexts": review, "sourceCounts": counts, "rowCount": len(rows)}


def _clean_intelligence_label(label: str, subject: str = "") -> str:
    label = re.sub(r"\s+", " ", str(label or "")).strip(" -:.,;!?()[]{}\"'“”‘’")
    label = _THROUGH_TRAILING_ACTION_PATTERN.sub("", label).strip()
    if not label or len(label) < 3 or len(label) > 60:
        return ""
    lowered = label.lower()
    if lowered == normalize_subject_name(subject).lower():
        return ""
    words = re.findall(r"[a-z0-9]+", lowered)
    if not words:
        return ""
    return label


def _is_sentence_start_match(text: str, start: int) -> bool:
    prefix = text[:start].rstrip()
    return not prefix or prefix[-1] in ".!?\n"


def _has_relationship_context(text: str, start: int, end: int) -> bool:
    window = text[max(0, start - 60): min(len(text), end + 60)].lower()
    return bool(re.search(r"\b(through|with|from|asks?|asked|says?|said|project|song|artist|tool|link|topic|via)\b", window))


def _add_candidate_reason(candidates: dict[str, set[str]], label: str, reason: str, subject: str) -> None:
    clean = _clean_intelligence_label(label, subject)
    if clean:
        candidates.setdefault(clean, set()).add(reason)


def _subject_candidate_reasons_from_text(text: str, subject: str) -> tuple[dict[str, set[str]], Counter, Counter]:
    candidates: dict[str, set[str]] = {}
    sentence_start_counts: Counter = Counter()
    non_sentence_start_counts: Counter = Counter()

    explicit_patterns = (
        (_THROUGH_PATTERN, "explicit_through_pattern"),
        (_HERE_PATTERN, "explicit_here_pattern"),
        (_SAID_PATTERN, "explicit_said_pattern"),
        (re.compile(r"\btopic\s*:\s*([A-Z][A-Za-z0-9_-]{2,}(?:\s+[A-Z][A-Za-z0-9_-]{2,}){0,3})\b"), "explicit_said_pattern"),
        (re.compile(r"\bRecurring\s+named\s+topic\s*:\s*([A-Z][A-Za-z0-9_-]{2,}(?:\s+[A-Z][A-Za-z0-9_-]{2,}){0,3})\b"), "explicit_said_pattern"),
    )
    for pattern, reason in explicit_patterns:
        for match in pattern.finditer(text):
            for group in match.groups():
                clean = _clean_intelligence_label(group, subject)
                if clean:
                    _add_candidate_reason(candidates, clean, reason, subject)
                raw_parts = re.findall(r"[A-Z][A-Za-z0-9_-]{2,}", str(group or ""))
                for suffix_len in (1, 2):
                    if len(raw_parts) >= suffix_len:
                        _add_candidate_reason(candidates, " ".join(raw_parts[-suffix_len:]), reason, subject)

    for match in _ENTITY_NAME_PATTERN.finditer(text):
        raw = match.group(0)
        clean = _clean_intelligence_label(raw, subject)
        if clean and len(clean.split()) <= 4:
            reason = "phrase_cluster" if len(clean.split()) > 1 else "observed_capitalized"
            if _has_relationship_context(text, match.start(), match.end()):
                reason = "repeated_non_sentence_start" if reason == "observed_capitalized" else reason
            _add_candidate_reason(candidates, clean, reason, subject)
            if _is_sentence_start_match(text, match.start()):
                sentence_start_counts[clean] += 1
            else:
                non_sentence_start_counts[clean] += 1
            for token in re.findall(r"[A-Z][A-Za-z0-9_-]{0,}", raw):
                token_clean = _clean_intelligence_label(token, subject)
                if token_clean and _candidate_entity_type(token_clean, {"observed_capitalized"}) in {"pronoun_or_address", "system_label", "grammar_or_filler", "rejected"}:
                    _add_candidate_reason(candidates, token_clean, "component_blocked_candidate", subject)
                    if _is_sentence_start_match(text, match.start()):
                        sentence_start_counts[token_clean] += 1
                    else:
                        non_sentence_start_counts[token_clean] += 1
            if raw[:1] in {'"', "'", '“', '‘'} or text[max(0, match.start()-1):match.start()] in {'"', "'", '“', '‘'}:
                _add_candidate_reason(candidates, clean, "quoted_title", subject)

    for match in _CODE_MARKER_PATTERN.finditer(text):
        clean = _clean_intelligence_label(match.group(0), subject)
        if clean:
            _add_candidate_reason(candidates, clean, "code_marker", subject)
            non_sentence_start_counts[clean] += 1

    lowered = text.lower()
    for raw, label in _TOOL_OR_PLATFORM_TERMS.items():
        if re.search(rf"(?<![a-z0-9]){re.escape(raw)}(?![a-z0-9])", lowered):
            _add_candidate_reason(candidates, label, "domain_tool", subject)
            non_sentence_start_counts[label] += 1
    for domain in _extract_domains(text):
        _add_candidate_reason(candidates, _domain_tool_label(domain), "domain_tool", subject)
    return candidates, sentence_start_counts, non_sentence_start_counts


def _subject_candidates_from_text(text: str, subject: str) -> set[str]:
    """Backward-compatible candidate set; acceptance filtering happens in extract_recurring_subject_intelligence."""

    candidates, _sentence_start, _non_sentence_start = _subject_candidate_reasons_from_text(text, subject)
    return set(candidates)

def _extract_domains(text: str) -> set[str]:
    domains: set[str] = set()
    for match in _DOMAIN_PATTERN.finditer(text):
        domain = match.group(1).lower().strip("./")
        if domain and "." in domain and not domain.endswith((".png", ".jpg", ".gif")):
            domains.add(domain.removeprefix("www."))
    return domains


def _theme_labels_for_text(text: str) -> set[str]:
    t = str(text or "")
    lowered = t.lower()
    labels: set[str] = set()
    if re.search(r"\b[A-Z][A-Za-z0-9_-]{2,}(?:\s+[A-Z][A-Za-z0-9_-]{2,}){0,2}\s+(?:through|via|speaking\s+through|relayed\s+through)\s+[A-Z][A-Za-z0-9_-]{2,}(?:\s+[A-Z][A-Za-z0-9_-]{2,}){0,2}\b", t):
        labels.add("messages framed through another named participant")
    if re.search(r"\bbnl\b|\bsource files?\b|\bdossiers?\b|\bbot\b", lowered):
        labels.add("BNL interaction")
    if re.search(r"\bliaison\b|\binterface\b|\bnode\b", lowered):
        labels.add("liaison/interface language")
    if re.search(r"\bbnl\b", lowered) and re.search(r"\bpresence\b", lowered):
        labels.add("BNL presence")
    elif re.search(r"\bpresence\b", lowered):
        labels.add("presence language")
    if re.search(r"\bthreshold\b", lowered):
        labels.add("threshold behavior")
    if re.search(r"\bsync\b|\bconvergence\b|\b0x[a-f0-9]{2,}\b", lowered):
        labels.add("sync/convergence markers")
    if re.search(r"\bboundar(?:y|ies)\b|\boperational\b", lowered):
        labels.add("operational boundaries")
    if re.search(r"\bbarcode\b", lowered):
        labels.add("BARCODE Network/community behavior")
    if re.search(r"\bsuno\b|\bmusic\b|\bsong\b|\btrack\b|\blink\b|\bshared?\b", lowered):
        labels.add("music-sharing or link language")
    return labels


def _recurring_phrases(text: str) -> set[str]:
    tokens = re.findall(r"[A-Za-z0-9][A-Za-z0-9'-]*", text)
    phrases: set[str] = set()
    for n in range(2, 6):
        for i in range(0, max(0, len(tokens) - n + 1)):
            phrase = " ".join(tokens[i:i+n])
            lower_words = [w.lower() for w in tokens[i:i+n]]
            if any(w in _RECURRING_PHRASE_STOPWORDS for w in lower_words):
                continue
            if not any(w[:1].isupper() or re.match(r"0x", w, re.I) for w in tokens[i:i+n]):
                continue
            phrases.add(phrase[:80])
    return phrases


def _candidate_words(label: str) -> list[str]:
    return re.findall(r"[A-Za-z0-9]+", str(label or ""))


def _candidate_key(label: str) -> str:
    return re.sub(r"\s+", " ", str(label or "").strip().lower())


def _is_system_or_pronoun_candidate(label: str) -> bool:
    """Return True for pronouns/address words and BNL/system/persona labels."""

    key = _candidate_key(label)
    words = [word.lower() for word in _candidate_words(label)]
    if not key or key in _PRONOUN_OR_ADDRESS_TERMS or key in _SYSTEM_OR_PERSONA_TERMS:
        return True
    if len(words) == 1 and words[0] in (_PRONOUN_OR_ADDRESS_TERMS | _SYSTEM_OR_PERSONA_TERMS):
        return True
    return bool(words) and all(word in (_PRONOUN_OR_ADDRESS_TERMS | _SYSTEM_OR_PERSONA_TERMS) for word in words)


def _candidate_entity_type(label: str, reasons: set[str] | None = None) -> str:
    """Classify a weak extracted string before it can become a recurring named topic."""

    reasons = reasons or set()
    key = _candidate_key(label)
    words = [word.lower() for word in _candidate_words(label)]
    if not key or not words:
        return "rejected"
    if key in _PRONOUN_OR_ADDRESS_TERMS or (len(words) == 1 and words[0] in _PRONOUN_OR_ADDRESS_TERMS):
        return "pronoun_or_address"
    if key.replace("'", " ") in {"bit", "bit s", "hey b", "pm pacific time", "pacific time", "continuity structure", "continuity structure speaking", "continuity structure operating", "speaking directly"}:
        return "rejected"
    if key in _SYSTEM_OR_PERSONA_TERMS or (len(words) == 1 and words[0] in _SYSTEM_OR_PERSONA_TERMS):
        return "system_label"
    if key in _TOOL_OR_PLATFORM_TERMS or "domain_tool" in reasons:
        return "tool_or_platform"
    if re.fullmatch(r"0x[A-Fa-f0-9]{2,}", str(label or "")):
        return "code_marker"
    if "code_marker" in reasons and not _is_system_or_pronoun_candidate(label):
        raw = str(label or "")
        if re.search(r"\d", raw) or "-" in raw:
            return "code_marker"
    if words and all(word in _GRAMMAR_OR_FILLER_TERMS for word in words):
        return "grammar_or_filler"
    if any(word in {"speaking", "transmitted", "directly", "operating", "still", "communicate", "communicates", "communicating"} for word in words):
        return "rejected"
    if words[0] in (_PRONOUN_OR_ADDRESS_TERMS | _GRAMMAR_OR_FILLER_TERMS | _SYSTEM_OR_PERSONA_TERMS | {"the", "a", "an", "not", "are", "is", "was", "were", "can", "could", "would", "should", "entire", "precision", "tradeoff", "value"}):
        return "rejected"
    blocked_count = sum(1 for word in words if word in _NAMED_TOPIC_BLOCKED_TERMS)
    if len(words) > 1 and blocked_count > 0:
        if key not in {"vega signal", "signal witch"}:
            return "rejected"
    if key in {"bnl 01", "barcode network", "barcode radio", "the network", "source file", "public context", "for review", "hey b", "pm pacific time", "pacific time", "continuity structure", "continuity structure speaking", "continuity structure operating", "6 bit", "six bit", "bit s"}:
        return "system_label"
    if re.fullmatch(r"(?:\d{1,2}\s*)?(?:am|pm)(?:\s+pacific(?:\s+time)?)?", key) or key in {"monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday", "tonight", "today", "tomorrow", "yesterday", "morning", "afternoon", "evening", "night"}:
        return "schedule_or_time"
    if any(word in {"presence", "threshold", "liaison", "interface", "sync", "convergence", "boundaries", "operational"} for word in words):
        return "conversation_theme"
    return "person_or_project"


def _is_valid_named_topic_candidate(label: str, reasons: set[str] | None = None) -> bool:
    return _candidate_entity_type(label, reasons) in {"person_or_project", "code_marker"}


def _should_promote_named_topic(label: str, count: int, reasons: set[str], sentence_starts: int, non_sentence_starts: int) -> tuple[bool, str]:
    """Apply the entity-quality gate for recurring named topics."""

    words = _candidate_words(label)
    entity_type = _candidate_entity_type(label, reasons)
    if entity_type not in {"person_or_project", "code_marker"}:
        return False, entity_type
    if entity_type == "code_marker":
        return True, "code_marker"

    key = _candidate_key(label)
    if key in _NAMED_TOPIC_BLOCKED_TERMS or any(word.lower() in _PRONOUN_OR_ADDRESS_TERMS for word in words):
        return False, "blocked_term"
    explicit_reasons = {"explicit_through_pattern", "explicit_here_pattern", "explicit_said_pattern", "quoted_title"}
    if reasons & explicit_reasons:
        raw = str(label or "")
        title_or_known = key in _NAMED_TOPIC_ALLOWLIST or bool(re.fullmatch(r"[A-Z][A-Za-z0-9_-]{2,}(?:\s+[A-Z][A-Za-z0-9_-]{2,}){0,2}", raw)) or bool(re.fullmatch(r"0x[A-Fa-f0-9]{2,}", raw))
        if not title_or_known:
            return False, "explicit_sentence_fragment"
        return True, next(reason for reason in ("explicit_through_pattern", "explicit_here_pattern", "explicit_said_pattern", "quoted_title") if reason in reasons)
    if len(words) > 1:
        lowered_words = [word.lower() for word in words]
        if lowered_words[0] in _NAMED_TOPIC_BLOCKED_TERMS:
            return False, "blocked_prefix"
        if sum(1 for word in lowered_words if word in _NAMED_TOPIC_BLOCKED_TERMS) >= len(lowered_words):
            return False, "mostly_blocked_words"
        if count >= 2:
            return True, "phrase_cluster"
        return False, "single_phrase_without_explicit_pattern"
    if len(words) == 1 and count >= 2 and non_sentence_starts >= 2:
        return True, "repeated_non_sentence_start"
    return False, "sentence_start_or_weak_signal"


def _acceptable_recurring_subject(label: str, count: int, reasons: set[str], sentence_starts: int, non_sentence_starts: int) -> tuple[bool, str]:
    return _should_promote_named_topic(label, count, reasons, sentence_starts, non_sentence_starts)

def _theme_sort_key(item: tuple[str, int]) -> tuple[int, int, str]:
    label, count = item
    preferred = [
        "BNL presence", "threshold behavior", "liaison/interface language",
        "sync/convergence markers", "operational boundaries", "BNL interaction",
        "music-sharing or link language", "BARCODE Network/community behavior",
    ]
    try:
        rank = preferred.index(label)
    except ValueError:
        rank = len(preferred)
    return (rank, -count, label)


def extract_conversation_theme_clusters(intelligence: dict[str, Any], *, public_only: bool = True) -> list[str]:
    themes: Counter = intelligence.get("publicThemes") or Counter()
    phrases: Counter = intelligence.get("publicPhrases") or Counter()
    domains: Counter = intelligence.get("publicDomains") or Counter()
    if not public_only:
        themes = themes + (intelligence.get("reviewOnlyThemes") or Counter())
        phrases = phrases + (intelligence.get("reviewOnlyPhrases") or Counter())
        domains = domains + (intelligence.get("reviewOnlyDomains") or Counter())
    clusters: list[str] = []
    for phrase, count in phrases.most_common(10):
        phrase_text = str(phrase or "")
        if count < 2:
            continue
        if " through " in phrase_text.lower():
            if not re.fullmatch(r"[A-Z][A-Za-z0-9_-]*(?:\s+[A-Z][A-Za-z0-9_-]*)?\s+through\s+[A-Z][A-Za-z0-9_-]*(?:\s+[A-Z][A-Za-z0-9_-]*)?", phrase_text):
                continue
            clusters.append(phrase_text.replace(" through ", "-through-") + " framing")
        elif re.fullmatch(r"0x[A-Fa-f0-9]{2,}", phrase_text):
            clusters.append(f"{phrase_text} marker")
    for label, count in sorted(themes.items(), key=_theme_sort_key):
        if count >= 2:
            clusters.append(label)
    if any(count >= 1 and _domain_tool_label(domain) == "Suno" for domain, count in domains.items()):
        clusters.append("Suno links")
    deduped: list[str] = []
    for cluster in clusters:
        clean = _safe_snippet(cluster, 80)
        if clean and clean not in deduped:
            deduped.append(clean)
    return deduped[:12]


def score_recurring_conversation_phrases(intelligence: dict[str, Any]) -> list[tuple[str, int]]:
    phrases: Counter = (intelligence.get("publicPhrases") or Counter()) + (intelligence.get("reviewOnlyPhrases") or Counter())
    return [(phrase, count) for phrase, count in phrases.most_common(8) if count >= 2]


def build_subject_topic_clusters(subject: str, intelligence: dict[str, Any]) -> dict[str, Any]:
    clusters = extract_conversation_theme_clusters(intelligence, public_only=True)
    phrase_scores = score_recurring_conversation_phrases(intelligence)
    public_domains: Counter = intelligence.get("publicDomains") or Counter()
    rows_scanned = int(intelligence.get("rowsScanned") or 0)
    activity: list[str] = []
    through_phrase = next((phrase for phrase, count in phrase_scores if " through " in phrase.lower()), "")
    if through_phrase:
        activity.append(f"Activity pattern: {subject} repeatedly relays messages framed as “{_safe_snippet(through_phrase, 90)}” in approved public context.")
    if public_domains:
        tools = [_domain_tool_label(domain) for domain, count in public_domains.most_common(4)]
        if any(tool == "Suno" for tool in tools):
            activity.append(f"Activity pattern: {subject} shares Suno links in reviewed evidence; queue/submission history is still not connected.")
        elif tools:
            activity.append(f"Activity pattern: {subject} repeatedly shares links in reviewed evidence.")
    public_themes: Counter = intelligence.get("publicThemes") or Counter()
    if public_themes.get("BNL interaction", 0) >= 2:
        activity.append(f"Activity pattern: {subject} has repeated BNL-facing exchanges in approved review context.")
    if public_themes.get("music-sharing or link language", 0) >= 2 and not any("shares" in line for line in activity):
        activity.append(f"Activity pattern: {subject} appears mainly in music-sharing context.")
    if public_themes.get("BARCODE Network/community behavior", 0) >= 2:
        activity.append(f"Activity pattern: {subject} appears mainly in community conversation context.")
    if public_themes.get("BNL interaction", 0) >= 2 and not any("BNL-facing" in line for line in activity):
        activity.append(f"Activity pattern: {subject} repeatedly asks BNL/source-file related questions.")
    theme_line = ""
    if clusters:
        theme_line = f"Conversation theme: {subject} repeatedly discusses {', '.join(clusters[:10])}. Review before public use."
    digest = ""
    if rows_scanned and clusters:
        digest = f"Evidence digest: {rows_scanned} {subject}-linked evidence items were scanned. The strongest recurring clusters were {', '.join(clusters[:10])}."
    return {"clusters": clusters, "activityPatterns": activity[:5], "conversationThemeLine": theme_line, "evidenceDigestLine": digest}



def _clean_through_left_intelligence_label(label: str, subject: str = "") -> str:
    clean = _clean_intelligence_label(label, subject)
    if not clean:
        return ""
    accepted, _ = _acceptable_recurring_subject(clean, 1, {"explicit_through_pattern"}, 0, 1)
    if accepted:
        return clean
    raw_parts = re.findall(r"[A-Z][A-Za-z0-9_-]{2,}", str(label or ""))
    for suffix_len in (2, 1):
        if len(raw_parts) >= suffix_len:
            suffix = _clean_intelligence_label(" ".join(raw_parts[-suffix_len:]), subject)
            accepted, _ = _acceptable_recurring_subject(suffix, 1, {"explicit_through_pattern"}, 0, 1)
            if accepted:
                return suffix
    return ""

def extract_recurring_subject_intelligence(rows: list[dict[str, Any]], subject: str) -> dict[str, Any]:
    """Score recurring names/themes/domains from fuller local rows, split by public-safe vs review-only."""

    public_subjects: Counter = Counter(); review_subjects: Counter = Counter()
    public_themes: Counter = Counter(); review_themes: Counter = Counter()
    public_domains: Counter = Counter(); review_domains: Counter = Counter()
    public_phrases: Counter = Counter(); review_phrases: Counter = Counter()
    reason_map: dict[str, set[str]] = {}
    accepted_reasons: dict[str, str] = {}
    rejected_candidates: Counter = Counter()
    sentence_starts: Counter = Counter(); non_sentence_starts: Counter = Counter()
    provisional_public: Counter = Counter(); provisional_review: Counter = Counter()
    for row in rows:
        text = str(row.get("text") or "")
        candidates, sentence_counts, non_sentence_counts = _subject_candidate_reasons_from_text(text, subject)
        for label, count in sentence_counts.items():
            sentence_starts[label] += count
        for label, count in non_sentence_counts.items():
            non_sentence_starts[label] += count
        target_subjects = provisional_public if row.get("publicSafe") else provisional_review
        target_themes = public_themes if row.get("publicSafe") else review_themes
        target_domains = public_domains if row.get("publicSafe") else review_domains
        target_phrases = public_phrases if row.get("publicSafe") else review_phrases
        for label, reasons in candidates.items():
            target_subjects[label] += 1
            reason_map.setdefault(label, set()).update(reasons)
        for domain in _extract_domains(text):
            target_domains[domain] += 1
        for match in _THROUGH_PATTERN.finditer(text):
            left = _clean_through_left_intelligence_label(match.group(1), subject)
            right = _clean_intelligence_label(match.group(2), subject) or normalize_subject_name(subject)
            subject_display = normalize_subject_name(subject)
            if right.lower().startswith(subject_display.lower() + " "):
                right = subject_display
            if left and right:
                target_phrases[f"{left} through {right}"] += 1
        for theme in _theme_labels_for_text(text):
            target_themes[theme] += 1
        for phrase in _recurring_phrases(text):
            target_phrases[phrase] += 1

    for label, count in (provisional_public + provisional_review).items():
        accepted, reason = _acceptable_recurring_subject(label, count, reason_map.get(label, set()), sentence_starts[label], non_sentence_starts[label])
        if accepted:
            accepted_reasons[label] = reason
        else:
            rejected_candidates[label] += count
    for label, count in provisional_public.items():
        if label in accepted_reasons:
            public_subjects[label] = count
    for label, count in provisional_review.items():
        if label in accepted_reasons:
            review_subjects[label] = count

    source_counts = Counter(str(row.get("source") or "unknown") for row in rows)
    intelligence = {
        "rowsScanned": len(rows),
        "sourceCounts": source_counts,
        "publicSubjects": public_subjects,
        "reviewOnlySubjects": review_subjects,
        "publicThemes": public_themes,
        "reviewOnlyThemes": review_themes,
        "publicDomains": public_domains,
        "reviewOnlyDomains": review_domains,
        "publicPhrases": public_phrases,
        "reviewOnlyPhrases": review_phrases,
        "publicRows": sum(1 for row in rows if row.get("publicSafe")),
        "reviewOnlyRows": sum(1 for row in rows if not row.get("publicSafe")),
        "acceptedSubjectReasons": accepted_reasons,
        "rejectedSubjectCandidates": rejected_candidates,
    }
    intelligence["topicClusters"] = build_subject_topic_clusters(subject, intelligence)
    return intelligence

def _domain_tool_label(domain: str) -> str:
    base = domain.lower().removeprefix("www.")
    known = {
        "suno.com": "Suno", "youtube.com": "YouTube", "youtu.be": "YouTube", "soundcloud.com": "SoundCloud",
        "spotify.com": "Spotify", "bandcamp.com": "Bandcamp", "tiktok.com": "TikTok", "instagram.com": "Instagram",
    }
    return known.get(base, base)


def _prepend_unique(target: list[str], value: str) -> None:
    clean = re.sub(r"\s+", " ", str(value or "")).strip()
    if not clean:
        return
    try:
        target.remove(clean)
    except ValueError:
        pass
    target.insert(0, clean)


def _add_priority_unique(summary: dict[str, Any], key: str, value: str) -> None:
    target = summary.setdefault(key, [])
    if isinstance(target, list):
        _prepend_unique(target, value)


def _subject_intelligence_priority_rank(value: Any) -> int:
    text = str(value or "")
    for index, prefix in enumerate(_SUBJECT_INTELLIGENCE_PRIORITY_PREFIXES):
        if text.startswith(prefix):
            return index
    return len(_SUBJECT_INTELLIGENCE_PRIORITY_PREFIXES)


def _compact_subject_intelligence_priorities(summary: dict[str, Any]) -> None:
    for key, value in list(summary.items()):
        if not isinstance(value, list):
            continue
        priority = [item for item in value if _subject_intelligence_priority_rank(item) < len(_SUBJECT_INTELLIGENCE_PRIORITY_PREFIXES)]
        if not priority:
            continue
        rest = [item for item in value if item not in priority]
        priority.sort(key=_subject_intelligence_priority_rank)
        summary[key] = priority + rest


def _promote_subject_intelligence_readouts(summary: dict[str, Any], intelligence: dict[str, Any]) -> None:
    subject = summary.get("subjectName") or "this subject"
    public_subjects: Counter = intelligence.get("publicSubjects") or Counter()
    review_subjects: Counter = intelligence.get("reviewOnlySubjects") or Counter()
    public_themes: Counter = intelligence.get("publicThemes") or Counter()
    review_themes: Counter = intelligence.get("reviewOnlyThemes") or Counter()
    public_domains: Counter = intelligence.get("publicDomains") or Counter()
    review_domains: Counter = intelligence.get("reviewOnlyDomains") or Counter()
    public_phrases: Counter = intelligence.get("publicPhrases") or Counter()
    topic_clusters: dict[str, Any] = intelligence.get("topicClusters") or {}

    conversation_theme_line = str(topic_clusters.get("conversationThemeLine") or "")
    if conversation_theme_line:
        for key in ("conversationHighlights", "topicBreakdown", "evidenceDetails"):
            _add_priority_unique(summary, key, conversation_theme_line)
        if len(topic_clusters.get("clusters") or []) >= 3:
            _add_priority_unique(summary, "bestEvidenceToReview", conversation_theme_line)

    evidence_digest_line = str(topic_clusters.get("evidenceDigestLine") or "")
    if evidence_digest_line:
        _add_priority_unique(summary, "evidenceDetails", evidence_digest_line)

    for activity_line in topic_clusters.get("activityPatterns") or []:
        for key in ("conversationHighlights", "usefulEvidence"):
            _add_priority_unique(summary, key, activity_line)
        lowered = activity_line.lower()
        if "bnl" in lowered:
            _add_priority_unique(summary, "bnlInteractionSignals", activity_line)
        if "suno" in lowered or "music" in lowered or "links" in lowered:
            _add_priority_unique(summary, "musicSignals", activity_line)
        if "community" in lowered:
            _add_priority_unique(summary, "communitySignals", activity_line)

    accepted_reason_map = intelligence.get("acceptedSubjectReasons") or {}
    for label, count in public_subjects.most_common(5):
        if count < 2 or not re.search(r"[A-Za-z]", str(label or "")):
            continue
        if accepted_reason_map.get(label) == "domain_tool":
            continue
        caution = " Additional review-only context also exists." if review_subjects.get(label) else ""
        normalized_line = f"Recurring named topic: {label} appears in reviewed evidence connected to {subject}. Review before public use.{caution}"
        review_line = f"Recurring named topic: {label}. Review selected evidence before using this publicly."
        candidate_line = f"Recurring named topic: {label} may inform public-safe wording only after owner/admin review."
        for key in ("knownContext", "usefulEvidence", "topicBreakdown", "conversationHighlights", "publicUseCandidates"):
            _add_priority_unique(summary, key, normalized_line if key != "publicUseCandidates" else candidate_line)
        _add_priority_unique(summary, "bestEvidenceToReview", review_line)
    for label, count in review_subjects.most_common(5):
        if count >= 1 and label not in public_subjects:
            _add_unique(summary["reviewOnlyEvidence"], f"Review-only recurring subject: {label} appears in internal context connected to {subject}. Do not use publicly without owner/admin review.")

    if public_phrases:
        for phrase, count in public_phrases.most_common(3):
            phrase_text = _safe_snippet(phrase, 80)
            if count >= 2 and (" through " in phrase.lower() or " via " in phrase.lower()):
                line = f"Recurring conversation pattern: messages are repeatedly framed as “{phrase_text}.” Review before public use."
                for key in ("conversationHighlights", "bnlInteractionSignals", "evidenceDetails"):
                    _add_priority_unique(summary, key, line)
                break

    theme_labels = [theme for theme, count in public_themes.most_common(8) if count >= 2]
    if theme_labels:
        _add_unique(summary["topicBreakdown"], "Recurring discussion themes: " + ", ".join(theme_labels[:6]) + ".")
        if "BNL interaction" in theme_labels:
            line = f"BNL interaction pattern: {subject} has repeated reviewed exchanges involving BNL. Review before public use."
            for key in ("bnlInteractionSignals", "conversationHighlights", "evidenceDetails"):
                _add_priority_unique(summary, key, line)
    for theme, count in review_themes.most_common(5):
        if count >= 1 and theme not in public_themes:
            _add_unique(summary["reviewOnlyEvidence"], f"Review-only recurring theme: {theme} appears in internal context; do not use publicly without owner/admin review.")

    for domain, count in reversed(public_domains.most_common(6)):
        if count < 1:
            continue
        tool = _domain_tool_label(domain)
        if tool == "Suno":
            line = f"Tool/platform mention: Suno appears in reviewed evidence connected to {subject}. Confirm through queue/submission data before claiming submitted songs or source type."
        else:
            line = f"Tool/platform mention: {tool} appears in reviewed evidence connected to {subject}. Confirm platform context before public use."
        for key in ("musicSignals", "usefulEvidence", "topicBreakdown", "evidenceDetails"):
            _add_priority_unique(summary, key, line)
    for domain, count in review_domains.most_common(6):
        tool = _domain_tool_label(domain)
        if domain not in public_domains:
            _add_unique(summary["reviewOnlyEvidence"], f"Review-only tool/link signal: {tool} appears in internal context; do not use publicly without owner/admin review.")

    summary["subjectIntelligenceDiagnostics"] = {
        "rowsScanned": int(intelligence.get("rowsScanned") or 0),
        "rowsBySource": dict((intelligence.get("sourceCounts") or Counter()).most_common()),
        "topRecurringSubjects": [label for label, count in (public_subjects + review_subjects).most_common(6) if count >= 1],
        "acceptedRecurringSubjects": [
            {"label": label, "reason": accepted_reason_map.get(label, "")}
            for label, count in (public_subjects + review_subjects).most_common(8) if count >= 1
        ],
        "rejectedGarbageCandidates": [label for label, count in (intelligence.get("rejectedSubjectCandidates") or Counter()).most_common(8)],
        "topRecurringThemes": [label for label, count in (public_themes + review_themes).most_common(6) if count >= 1],
        "topConversationClusters": list((topic_clusters.get("clusters") or [])[:8]),
        "topActivityPatterns": list((topic_clusters.get("activityPatterns") or [])[:5]),
        "topDomains": [domain for domain, count in (public_domains + review_domains).most_common(6) if count >= 1],
        "publicSafeRows": int(intelligence.get("publicRows") or 0),
        "reviewOnlyRows": int(intelligence.get("reviewOnlyRows") or 0),
    }


def _clean_fact_text(text: Any) -> str:
    return _safe_snippet(text, 500)


def _count_term_mentions(texts: list[str], terms: dict[str, str]) -> Counter:
    counts: Counter = Counter()
    for text in texts:
        lowered = f" {str(text or '').lower()} "
        for raw, label in terms.items():
            if re.search(rf"(?<![a-z0-9]){re.escape(raw)}(?![a-z0-9])", lowered, flags=re.I):
                counts[label] += 1
    return counts


def extract_tool_or_platform_mentions(texts: list[str]) -> Counter:
    """Count known creative/community tools or platforms in already-selected evidence text."""

    return _count_term_mentions(texts, _TOOL_OR_PLATFORM_TERMS)


def extract_named_topic_facts(texts: list[str], subject: str = "") -> Counter:
    """Extract bounded named recurring subjects from safe/public or reviewable snippets."""

    provisional: Counter = Counter()
    reasons_by_label: dict[str, set[str]] = {}
    sentence_starts: Counter = Counter()
    non_sentence_starts: Counter = Counter()
    for text in texts:
        candidates, sentence_counts, non_sentence_counts = _subject_candidate_reasons_from_text(str(text or ""), subject)
        for label, reasons in candidates.items():
            if _candidate_entity_type(label, reasons) == "tool_or_platform":
                continue
            provisional[label] += 1
            reasons_by_label.setdefault(label, set()).update(reasons)
        sentence_starts.update(sentence_counts)
        non_sentence_starts.update(non_sentence_counts)

    counts: Counter = Counter()
    for label, count in provisional.items():
        accepted, _reason = _should_promote_named_topic(label, count, reasons_by_label.get(label, set()), sentence_starts[label], non_sentence_starts[label])
        if accepted:
            counts[label] = count
    return counts

def extract_bnl_interaction_facts(texts: list[str]) -> dict[str, int]:
    """Return repeated BNL/help interaction counts from already-approved evidence text."""

    bnl_count = 0
    help_count = 0
    for text in texts:
        if re.search(r"\b(BNL|bot|source files?|dossiers?)\b", str(text or ""), flags=re.I):
            bnl_count += 1
        if re.search(r"\b(help|support|question|request|fix|how do I|bot-use|bot use)\b", str(text or ""), flags=re.I):
            help_count += 1
    return {"bnl": bnl_count, "help": help_count}


def extract_recurring_people_mentions(texts: list[str], subject: str = "") -> Counter:
    """Alias for named person/community-member style recurring topic extraction."""

    return extract_named_topic_facts(texts, subject=subject)


def extract_public_conversation_patterns(texts: list[str]) -> dict[str, int]:
    """Count broad but review-safe patterns in approved public-context evidence text."""

    return {
        "music_or_submission_language": sum(1 for text in texts if _MUSIC_SUBMISSION_LANGUAGE_PATTERN.search(str(text or ""))),
        "community_context": sum(1 for text in texts if _COMMUNITY_PATTERN.search(str(text or ""))),
        "bnl_interaction": extract_bnl_interaction_facts(texts).get("bnl", 0),
    }


def extract_review_safe_entity_facts(public_texts: list[str], review_only_texts: list[str], subject: str) -> dict[str, Any]:
    """Build review-safe fact readouts without exposing raw transcripts or source IDs."""

    public_clean = [_clean_fact_text(text) for text in public_texts if _clean_fact_text(text)]
    review_clean = [_clean_fact_text(text) for text in review_only_texts if _clean_fact_text(text)]
    return {
        "publicNamedTopics": extract_named_topic_facts(public_clean, subject=subject),
        "reviewOnlyNamedTopics": extract_named_topic_facts(review_clean, subject=subject),
        "publicTools": extract_tool_or_platform_mentions(public_clean),
        "reviewOnlyTools": extract_tool_or_platform_mentions(review_clean),
        "publicBnlInteraction": extract_bnl_interaction_facts(public_clean),
        "reviewOnlyBnlInteraction": extract_bnl_interaction_facts(review_clean),
        "publicPatterns": extract_public_conversation_patterns(public_clean),
        "hasPublicMusicSubmissionLanguage": any(_MUSIC_SUBMISSION_LANGUAGE_PATTERN.search(text) for text in public_clean),
        "hasReviewOnlyRelationshipContext": bool(review_clean),
    }


def _append_review_safe_fact_readouts(summary: dict[str, Any], facts: dict[str, Any]) -> None:
    subject = summary.get("subjectName") or "This subject"
    public_topics: Counter = facts.get("publicNamedTopics") or Counter()
    review_topics: Counter = facts.get("reviewOnlyNamedTopics") or Counter()
    public_tools: Counter = facts.get("publicTools") or Counter()
    review_tools: Counter = facts.get("reviewOnlyTools") or Counter()

    # PR #237: legacy named-topic facts are advisory diagnostics only. Do not promote
    # them into Source File readout/payload fields; entity-ledger facets are authoritative.
    diagnostics = summary.setdefault("subjectIntelligenceDiagnostics", {})
    legacy_topics = diagnostics.setdefault("legacyNamedTopicDiagnostics", [])
    for topic, count in public_topics.most_common(6):
        if count >= 2 or topic in {"Orion"}:
            legacy_topics.append({"label": topic, "kind": "public_named_topic", "accepted": False, "reason": "legacy_named_topic_advisory_only", "sourceScope": "reviewed_public", "sourceTable": "activity_summary", "evidenceCount": int(count)})
    for topic, count in review_topics.most_common(6):
        if count >= 1 and topic not in public_topics:
            legacy_topics.append({"label": topic, "kind": "review_only_named_topic", "accepted": False, "reason": "legacy_named_topic_advisory_only", "sourceScope": "review_only", "sourceTable": "activity_summary", "evidenceCount": int(count)})

    for tool, count in public_tools.most_common(5):
        line = f"Tool/platform mention: {tool} appears in reviewed evidence connected to {subject}. Confirm whether it relates to submissions before using as public copy."
        _add_unique(summary["musicSignals"], line)
        _add_unique(summary["evidenceDetails"], line)
        _add_unique(summary["topicBreakdown"], f"Tool/platform mention: {tool} appears in reviewed evidence; queue/submission status remains not connected.")
    for tool, count in review_tools.most_common(5):
        if tool not in public_tools:
            _add_unique(summary["reviewOnlyEvidence"], f"Review-only tool/platform mention: {tool} appears in internal context connected to {subject}. Do not use publicly without owner/admin review.")

    public_bnl = facts.get("publicBnlInteraction") or {}
    if int(public_bnl.get("bnl") or 0) >= 2:
        _add_unique(summary["bnlInteractionSignals"], f"BNL interaction pattern: {subject} appears in repeated approved public-context exchanges involving BNL.")
        _add_unique(summary["conversationHighlights"], f"{subject} has repeated BNL interaction evidence in approved public context.")
        _add_unique(summary["evidenceDetails"], f"Confirmed internal evidence: repeated approved public-context BNL interaction pattern found for {subject}.")
    if int(public_bnl.get("help") or 0) >= 2:
        _add_unique(summary["bnlInteractionSignals"], f"BNL/help pattern: {subject} has recurring help/support or bot-use context in approved public evidence.")

    patterns = facts.get("publicPatterns") or {}
    if int(patterns.get("music_or_submission_language") or 0) > 0:
        _add_unique(summary["musicSignals"], "Possible music/submission-related language appears in reviewed evidence, but queue/submission identity is not connected yet.")
    if int(patterns.get("community_context") or 0) >= 2:
        _add_unique(summary["communitySignals"], f"Public-context activity: {subject} appears in repeated approved BARCODE/community conversation patterns; owner/admin review must confirm wording.")
    if facts.get("hasReviewOnlyRelationshipContext"):
        _add_unique(summary["reviewOnlyEvidence"], "Review-only relationship/context evidence exists and must stay internal.")

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
        "usefulEvidence": [],
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
    public_fact_texts: list[str] = []
    review_only_fact_texts: list[str] = []
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
            for row in structured_rows:
                data = dict(row)
                text = str(data.get("safe_summary") or "")
                topic = str(data.get("topic") or "")
                if topic:
                    text = f"{text} {topic}".strip()
                if not text:
                    continue
                if bool(data.get("public_safe_candidate")) and not bool(data.get("review_only")):
                    public_fact_texts.append(text)
                else:
                    review_only_fact_texts.append(text)
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

        intelligence_rows = collect_subject_intelligence_rows(
            conn,
            subject,
            guild_id,
            aliases=aliases,
            matched_user_ids=matched_user_ids,
            structured_rows=structured_rows,
            max_rows=max_rows,
        )
        if intelligence_rows:
            intelligence = extract_recurring_subject_intelligence(intelligence_rows, subject)
            # PR #237: legacy recurring-subject extraction is diagnostic/advisory only.
            # Entity Intelligence Ledger facets are authoritative for Source File claims/readouts.
            existing_legacy = (summary.get("subjectIntelligenceDiagnostics") or {}).get("legacyNamedTopicDiagnostics") or []
            accepted_reason_map = intelligence.get("acceptedSubjectReasons") or {}
            public_subjects = intelligence.get("publicSubjects") or Counter()
            review_subjects = intelligence.get("reviewOnlySubjects") or Counter()
            public_themes = intelligence.get("publicThemes") or Counter()
            review_themes = intelligence.get("reviewOnlyThemes") or Counter()
            public_domains = intelligence.get("publicDomains") or Counter()
            review_domains = intelligence.get("reviewOnlyDomains") or Counter()
            topic_clusters = intelligence.get("topicClusters") or {}
            summary["subjectIntelligenceDiagnostics"] = {
                "rowsScanned": int(intelligence.get("rowsScanned") or 0),
                "rowsBySource": dict((intelligence.get("sourceCounts") or Counter()).most_common()),
                "topRecurringSubjects": [label for label, count in (public_subjects + review_subjects).most_common(6) if count >= 1],
                "acceptedRecurringSubjects": [
                    {"label": label, "reason": accepted_reason_map.get(label, ""), "advisoryOnly": True}
                    for label, count in (public_subjects + review_subjects).most_common(8) if count >= 1
                ],
                "rejectedGarbageCandidates": [label for label, count in (intelligence.get("rejectedSubjectCandidates") or Counter()).most_common(8)],
                "topRecurringThemes": [label for label, count in (public_themes + review_themes).most_common(6) if count >= 1],
                "topConversationClusters": list((topic_clusters.get("clusters") or [])[:8]),
                "topActivityPatterns": list((topic_clusters.get("activityPatterns") or [])[:5]),
                "topDomains": [domain for domain, count in (public_domains + review_domains).most_common(6) if count >= 1],
                "publicSafeRows": int(intelligence.get("publicRows") or 0),
                "reviewOnlyRows": int(intelligence.get("reviewOnlyRows") or 0),
                "legacyAdvisoryOnly": True,
                "legacyDiagnosticNote": "Legacy recurring-subject diagnostics only. These are not Source File claims.",
                "acceptedRecurringSubjectCount": len(accepted_reason_map),
                "rejectedRecurringSubjectCount": sum((intelligence.get("rejectedSubjectCandidates") or Counter()).values()),
                "legacyNamedTopicDiagnostics": existing_legacy,
            }

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
                review_only_fact_texts.append(text)
                _record_raw(summary["rawProvenance"], "user_memory_facts", "local_memory_fact", _safe_snippet(text))

        if "user_habits" in lanes:
            for row in _fetch_rows(conn, "user_habits", guild_id, max_rows):
                if not row_matches(row, ["last_topic"]):
                    continue
                text = _row_text(row, ["last_topic"])
                _add_unique(summary["activitySignals"], "Habit/activity traces mention this subject; use only as observed context, not personality facts.")
                _add_unique(summary["evidenceDetails"], "Habit/activity traces mention this subject as context only, not as a public trait or role.")
                _note_topics(topic_counts, text)
                review_only_fact_texts.append(text)
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
                review_only_fact_texts.append(text)
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
                    safe_topic = _website_safe_topic_label(topics[0])
                    _add_unique(summary["evidenceDetails"], f"{channel_display}: approved public-side item classified under {safe_topic}; review before treating it as a subject claim.")
                    if _MUSIC_PATTERN.search(text):
                        _add_unique(summary["musicSignals"], "Approved context connects this subject to music, track, queue, radio, or show discussion; queue identity still needs review.")
                    if _COMMUNITY_PATTERN.search(text):
                        _add_unique(summary["communitySignals"], "Approved context places this subject in BARCODE/community conversation; owner review must confirm public wording.")
                    if _BNL_INTERACTION_PATTERN.search(text):
                        _add_unique(summary["bnlInteractionSignals"], f"BNL-related discussion appears in approved public-side context; subject was {relation}.")
                    public_fact_texts.append(text)
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
                    review_only_fact_texts.append(text)
                    quality = "internal_context_review_only"
                for topic in topics:
                    _add_unique(summary["topicBreakdown"], f"{_website_safe_topic_label(topic)}: observed in conversation context.")
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
                review_only_fact_texts.append(text)
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
                if public_safe and status == "active":
                    public_fact_texts.append(text)
                else:
                    review_only_fact_texts.append(text)
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
                public_fact_texts.append(text)
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
                review_only_fact_texts.append(text)
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

    fact_readouts = extract_review_safe_entity_facts(public_fact_texts, review_only_fact_texts, subject)
    _append_review_safe_fact_readouts(summary, fact_readouts)

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
    _add_unique(summary["missingInfo"], "Queue/submission history is not connected yet; source-file memory cannot confirm submitted songs, source type, play status, or priority history.")
    _add_unique(summary["missingInfo"], "Reception and co-participant analysis is not available from the current evidence packet.")

    summary["recurringTopics"] = [f"Observed topic classification: {_website_safe_topic_label(topic)}." for topic, count in topic_counts.most_common(5) if count > 0]
    has_detail_breakdown = any("public-side authored" in item or "public-side mentioned" in item for item in summary["topicBreakdown"])
    if not has_detail_breakdown:
        for topic, count in topic_counts.most_common(8):
            if count > 0:
                _add_unique(summary["topicBreakdown"], f"{_website_safe_topic_label(topic)}: {count} approved/reviewed source item(s).")
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

    _compact_subject_intelligence_priorities(summary)

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
