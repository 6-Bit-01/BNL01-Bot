"""Source-aware Entity Intelligence Ledger and Context Resolver v1.

This module intentionally uses only local stored rows. It does not fetch live
Discord history, does not publish, and does not treat Discord music links as
queue/submission/payment facts. Future website public dossier reads should feed
``official_public_dossier`` authority facts here; future queue integrations
should feed ``queue_submission_confirmed`` facts here.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Any

ENTITY_SOURCE_AUTHORITY_ORDER = (
    "owner_confirmed",
    "official_public_dossier",
    "broadcast_memory",
    "mod_confirmed",
    "queue_submission_confirmed",
    "public_discord_observed",
    "internal_observation",
    "bnl_inferred",
    "source_blind_memory",
    "unknown",
)
ENTITY_VISIBILITY_VALUES = (
    "public",
    "public_safe_candidate",
    "review_only",
    "mod_only",
    "admin_only",
    "internal_only",
    "source_blind",
    "account_private",
    "unknown",
)
SURFACES = {
    "public_conversation",
    "ambient_public",
    "website_relay",
    "rd_mod_ops",
    "source_file",
    "dossier_draft",
    "owner_review",
    "internal_diagnostics",
}
PUBLIC_POLICIES = {"public", "public_home", "public_discussion", "public_music", "public_show", "community_public"}
PRIVATE_POLICY_RE = re.compile(r"(?i)(?:private|internal|admin|mod|staff|dm|direct)")
URL_RE = re.compile(r"https?://\S+", re.I)
MENTION_RE = re.compile(r"<@!?\d+>|<@&\d+>|<#\d+>")
LONG_ID_RE = re.compile(r"\b\d{15,22}\b")
EMAIL_RE = re.compile(r"\b[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}\b")

ROLE_PATTERNS = [
    ("moderator", re.compile(r"\b(?:mod|mods|moderator|staff|admin|operator)\b", re.I), "mod/staff language observed"),
    ("artist", re.compile(r"\b(?:artist|producer|musician|songwriter|music maker|track|song|album|suno|udio|soundcloud|spotify|bandcamp)\b", re.I), "creative/music language observed"),
    ("collaborator", re.compile(r"\b(?:collab\w*|collaborat\w*|feature\s+with|work\s+with|team\s+up)\b", re.I), "collaboration language observed"),
    ("contest operator", re.compile(r"\b(?:contest|competition|event|rules|deadline|submit entries|winner|prize|competition challenge|contest challenge|challenge entries)\b", re.I), "contest/event language observed"),
    ("support/helper", re.compile(r"\b(?:helping|helper|support|answers?|guides?|welcome|onboard)\b", re.I), "community support language observed"),
    ("lore/entity/persona/project creator", re.compile(r"\b(?:created|made|built|my ai|my bot|my project|my persona|my character|my entity)\b", re.I), "entity/persona/project creation language observed"),
    ("antagonist/challenger", re.compile(r"\b(?:challenge|challenging|argue|arguing|bait|teas|hostile|antagonistic|fight me|prove it|you can't)\b", re.I), "challenging language observed"),
]
THEME_PATTERNS = [
    ("BNL behavior/boundaries", re.compile(r"\b(?:BNL|bot|boundary|boundaries|behavior|rules|what can you say)\b", re.I)),
    ("source files/dossiers", re.compile(r"\b(?:source files?|dossiers?|case file|public dossier)\b", re.I)),
    ("music/track sharing", re.compile(r"\b(?:music|track|song|suno|udio|youtube|soundcloud|spotify|bandcamp|listen|demo|wip)\b", re.I)),
    ("collaboration", re.compile(r"\b(?:collab\w*|collaborat\w*|feature|work together|open verse)\b", re.I)),
    ("contests/events", re.compile(r"\b(?:contest|competition|event|rules|deadline|entries|winner|prize|submission window|bracket|round|judge|competition challenge|contest challenge|challenge entries)\b", re.I)),
    ("show operations", re.compile(r"\b(?:BARCODE Radio|show|episode|broadcast|queue)\b", re.I)),
    ("moderation/community support", re.compile(r"\b(?:mod|moderator|staff|admin|help|support|welcome|community)\b", re.I)),
    ("strange/anomaly conversations", re.compile(r"\b(?:anomaly|strange|weird|theory|signal|glitch|lore|cipher|riddle)\b", re.I)),
    ("AI/persona/project interaction", re.compile(r"\b(?:AI|bot|persona|entity|project|created|speaking through|relayed through|my ai|my bot|my persona|my character|my entity)\b", re.I)),
    ("public links/socials", re.compile(r"\b(?:https?://|youtube|suno|udio|soundcloud|spotify|bandcamp|instagram|tiktok)\b", re.I)),
    ("BARCODE Network behavior", re.compile(r"\b(?:BARCODE|BNL|Barcode Network)\b", re.I)),
]
MUSIC_RE = re.compile(r"\b(?:suno|udio|youtube|youtu\.be|soundcloud|spotify|bandcamp|track|song|demo|wip|finished[- ]tracks?)\b|https?://", re.I)
BNL_RE = re.compile(r"\b(?:BNL|bot|source files?|dossiers?|BARCODE)\b", re.I)
ANTAGONISTIC_RE = re.compile(r"\b(?:challenge|challenging|argu|bait|teas|hostile|antagon|prove it|wrong|you can't|fight me)\b", re.I)
ANOMALY_RE = re.compile(r"\b(?:anomaly|strange|weird|theory|signal|glitch|lore|cipher|riddle)\b", re.I)
CONTEST_RE = re.compile(r"\b(?:contest|competition|event|rules|deadline|entries|winner|prize|submission\s+window|bracket|round|judge|competition\s+challenge|contest\s+challenge|challenge\s+entries)\b", re.I)
CHALLENGE_CONTEST_RE = re.compile(r"\b(?:competition|contest)\s+challenge\b|\bchallenge\s+(?:entries|round|bracket|deadline|prize|winner|submission)\b", re.I)
COLLAB_RE = re.compile(r"\b(?:collab\w*|collaborat\w*|feature|work together|open verse|team up)\b", re.I)
CREATED_RE = re.compile(r"\b([A-Z][A-Za-z0-9_-]{2,}(?:\s+[A-Z][A-Za-z0-9_-]{2,}){0,2})\s+(?:created|made|built)\s+([A-Z][A-Za-z0-9_-]{2,}(?:\s+[A-Z][A-Za-z0-9_-]{2,}){0,2})\b")
CREATED_MY_RE = re.compile(r"\b([A-Z][A-Za-z0-9_-]{2,}(?:\s+[A-Z][A-Za-z0-9_-]{2,}){0,2})\s+is\s+my\s+(?:AI|bot|project|persona|character|entity)\b", re.I)
THROUGH_RE = re.compile(r"\b([A-Z][A-Za-z0-9_-]{2,}(?:\s+[A-Z][A-Za-z0-9_-]{2,}){0,3})\s+(?:through|via|speaking\s+through|relayed\s+through)\s+([A-Z][A-Za-z0-9_-]{2,}(?:\s+[A-Z][A-Za-z0-9_-]{2,}){0,2})\b", re.I)
HERE_RE = re.compile(r"\b([A-Z][A-Za-z0-9_-]{2,}(?:\s+[A-Z][A-Za-z0-9_-]{2,}){0,1})\s+here\b", re.I)


SUBJECT_SCOPE_VALUES = {
    "subject_owned",
    "subject_keyed",
    "subject_authored",
    "subject_profile_matched",
    "subject_direct_evidence",
    "subject_co_mention",
    "global_mixed_memory",
    "source_blind_global",
    "rejected",
}
STRONG_SUBJECT_SCOPES = {"subject_owned", "subject_keyed", "subject_authored", "subject_profile_matched", "subject_direct_evidence"}
EXTRA_ENTITY_NAME_STOPWORDS = {
    "BNL", "BARCODE", "Barcode Network", "BARCODE Radio", "Discord", "Suno", "Udio", "YouTube", "SoundCloud",
    "Spotify", "Bandcamp", "Source File", "Source Files", "Candidate Intake", "Dossier", "Dossiers", "Owner", "Admin",
    "Moderator", "Staff", "Public", "Review", "Unknown", "Entity", "AI", "Bot", "Crowd",
    "User", "Users", "Member", "Members", "Someone", "Something", "Anyone", "Everyone", "Everything",
    "Hey", "Hey B", "Hi", "Hello", "Still", "Speaking", "Transmitted", "Directly", "Operating",
    "Continuity", "Continuity Structure", "Continuity Structure Speaking", "Continuity Structure Operating",
    "PM", "AM", "Pacific", "Pacific Time", "PM Pacific Time", "Friday", "Saturday", "Sunday", "Monday",
    "Tuesday", "Wednesday", "Thursday", "Tonight", "Today", "Tomorrow", "Yesterday", "Morning", "Afternoon",
    "Evening", "Night", "Bit", "Bit's", "Bits",
}
_LABEL_BLOCKED_KEYS = {re.sub(r"[^a-z0-9]+", " ", label.lower()).strip() for label in EXTRA_ENTITY_NAME_STOPWORDS}
_SCHEDULE_OR_TIME_RE = re.compile(r"\b(?:(?:\d{1,2}(?::\d{2})?\s*)?(?:am|pm)(?:\s+pacific(?:\s+time)?)?|pacific(?:\s+time)?|monday|tuesday|wednesday|thursday|friday|saturday|sunday|tonight|today|tomorrow|yesterday|morning|afternoon|evening|night)\b", re.I)
_GREETING_OR_ADDRESS_RE = re.compile(r"\b(?:hey(?:\s+b)?|hi|hello|yo|dear|user|users|member|members|someone|something|anyone|everyone|everything)\b", re.I)
_ACTION_OR_VERB_FRAGMENT_RE = re.compile(r"\b(?:speaking(?:\s+directly)?|talking|transmitted|transmitting|relayed|operating|active|directly|still|communicate|communicates|communicating)\b", re.I)
_THROUGH_LEFT_TRAILING_ACTION_RE = re.compile(r"(?:\s+(?:speaking(?:\s+directly)?|talking|transmitted|transmitting|relayed|operating|active))+$", re.I)
_RELATIONSHIP_LABEL_BLOCKED_PREFIXES = {
    "the", "a", "an", "not", "are", "is", "was", "were", "can", "could", "would", "should",
    "entire", "precision", "tradeoff", "value", "still", "anyone", "user", "users",
}
_COMMON_PROSE_WORDS = _LABEL_BLOCKED_KEYS | {
    "entire", "civilizations", "communicate", "purely", "language", "sustained", "analytical", "input",
    "tradeoff", "precision", "matters", "value", "trap", "incidental", "here", "through", "speaking",
    "operating", "structure", "continuity", "still", "anyone", "directly", "transmitted",
}
_PLATFORM_OR_TOOL_KEYS = {"discord", "suno", "udio", "youtube", "soundcloud", "spotify", "bandcamp", "instagram", "tiktok"}
_SYSTEM_OR_CHANNEL_KEYS = {"source file", "source files", "candidate intake", "dossier", "dossiers", "owner", "admin", "moderator", "staff", "public", "review", "unknown", "entity", "ai", "bot", "server", "channel", "welcome", "episode tracker"}
_THEME_ONLY_KEYS = {"continuity", "continuity structure", "conversation", "community", "source", "file", "dossier", "anomaly", "lore", "signal", "challenge", "rules language"}
_COMMON_PROSE_WORDS.update(_THEME_ONLY_KEYS)
_CORE_BARCODE_ALIASES = {
    "6 bit": "6 Bit", "six bit": "6 Bit", "bit": "6 Bit", "bit s": "6 Bit", "bits": "6 Bit",
    "bnl": "BNL", "bnl 01": "BNL-01", "barcode": "BARCODE", "barcode network": "BARCODE Network", "barcode radio": "BARCODE Radio",
}
_NAMED_ENTITY_ALLOWLIST_KEYS = {"orion", "signal witch", "vega signal"}


def _label_key(label: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(label or "").lower()).strip()


def _normalize_entity_label(label: str) -> str:
    text = str(label or "")[:90].strip(" .,:;!?'\"“”‘’()[]{}")
    text = MENTION_RE.sub(" ", text)
    text = LONG_ID_RE.sub(" ", text)
    text = EMAIL_RE.sub(" ", text)
    text = URL_RE.sub(" ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _canonical_entity_label(label: str) -> str:
    normalized = _normalize_entity_label(label)
    key = _label_key(normalized)
    if key in _CORE_BARCODE_ALIASES:
        return _CORE_BARCODE_ALIASES[key]
    return normalized


def _is_core_barcode_alias(label: str) -> bool:
    return _label_key(label) in _CORE_BARCODE_ALIASES


def _is_schedule_or_time_label(label: str) -> bool:
    key = _label_key(label)
    return bool(key and (_SCHEDULE_OR_TIME_RE.fullmatch(str(label or "").strip()) or _SCHEDULE_OR_TIME_RE.fullmatch(key)))


def _is_greeting_or_address_label(label: str) -> bool:
    key = _label_key(label)
    return bool(key and (_GREETING_OR_ADDRESS_RE.fullmatch(str(label or "").strip()) or key in _LABEL_BLOCKED_KEYS and any(word in key.split() for word in {"hey", "hi", "hello", "user", "users", "member", "members"})))


def _is_noise_entity_label(label: str) -> bool:
    normalized = _normalize_entity_label(label)
    key = _label_key(normalized)
    if not key or key in _LABEL_BLOCKED_KEYS:
        return True
    words = key.split()
    if all(word in _LABEL_BLOCKED_KEYS for word in words):
        return True
    return bool(_ACTION_OR_VERB_FRAGMENT_RE.fullmatch(normalized) or _ACTION_OR_VERB_FRAGMENT_RE.fullmatch(key))


def _looks_like_entity_marker(label: str) -> bool:
    raw = str(label or "").strip()
    normalized = _normalize_entity_label(raw)
    key = _label_key(normalized)
    if not key:
        return False
    if key in _NAMED_ENTITY_ALLOWLIST_KEYS or _is_core_barcode_alias(normalized):
        return True
    if re.search(r"\b0x[A-Fa-f0-9]{2,}\b|[A-Z0-9]{3,}-[A-Z0-9-]+", raw):
        return True
    if raw[:1] in {'"', "'", "“", "‘"} and raw[-1:] in {'"', "'", "”", "’"}:
        return True
    words = normalized.split()
    if not words:
        return False
    title_words = [w for w in words if re.match(r"^[A-Z][A-Za-z0-9_-]{2,}$", w)]
    return bool(title_words) and len(title_words) == len(words)


def _strict_relationship_label_rejection_reason(label: str) -> str:
    normalized = _normalize_entity_label(label)
    key = _label_key(normalized)
    if not key:
        return "empty"
    words = key.split()
    if words and words[0] in _RELATIONSHIP_LABEL_BLOCKED_PREFIXES:
        return "blocked_prefix"
    if key in {"trap", "speaking", "transmitted", "directly", "operating", "not incidental", "are still", "sustained analytical input", "entire civilizations communicate purely"}:
        return "blocked_fragment"
    if any(word in {"speaking", "transmitted", "directly", "operating", "communicate", "communicates", "communicating"} for word in words) and not _looks_like_entity_marker(normalized):
        return "verb_fragment"
    if len(words) > 1 and all(word in _COMMON_PROSE_WORDS for word in words) and not _looks_like_entity_marker(normalized):
        return "common_prose"
    if len(words) == 1 and words[0] in _COMMON_PROSE_WORDS and key not in _NAMED_ENTITY_ALLOWLIST_KEYS:
        return "common_word"
    return ""


def _route_label_kind(label: str) -> str:
    normalized = _normalize_entity_label(label)
    key = _label_key(normalized)
    if not key:
        return "rejected"
    strict_reason = _strict_relationship_label_rejection_reason(normalized)
    if strict_reason:
        return strict_reason
    if _is_greeting_or_address_label(normalized):
        return "greeting_or_address"
    if _is_schedule_or_time_label(normalized):
        return "schedule_or_time"
    if _ACTION_OR_VERB_FRAGMENT_RE.fullmatch(normalized) or _ACTION_OR_VERB_FRAGMENT_RE.fullmatch(key):
        return "action_or_verb_fragment"
    if key in {"user", "users", "member", "members", "someone", "something", "anyone", "everyone", "everything"}:
        return "generic_speaker_label"
    if _is_core_barcode_alias(normalized):
        return "core_barcode_entity"
    if key in _PLATFORM_OR_TOOL_KEYS:
        return "platform_or_tool"
    if key in _SYSTEM_OR_CHANNEL_KEYS:
        return "system_or_channel_label"
    if key in _THEME_ONLY_KEYS or key.startswith("continuity structure"):
        return "theme_only"
    if _is_noise_entity_label(normalized):
        return "rejected"
    if len(key) <= 2 and key not in {"ai"}:
        return "rejected"
    return "person_or_project"


def _is_valid_entity_label(label: str) -> bool:
    return _route_label_kind(label) == "person_or_project"


def _is_valid_relationship_object_label(label: str, *, extraction: str = "") -> bool:
    if _route_label_kind(label) != "person_or_project":
        return False
    if extraction in {"here", "through"} and not _looks_like_entity_marker(label):
        return False
    return True


def _clean_through_left_label(label: str) -> str:
    cleaned = _normalize_entity_label(label)
    previous = None
    while previous != cleaned:
        previous = cleaned
        cleaned = _THROUGH_LEFT_TRAILING_ACTION_RE.sub("", cleaned).strip()
    return _canonical_entity_label(cleaned) if cleaned else ""


def _has_contest_event_signal(text: str) -> bool:
    if not CONTEST_RE.search(text):
        return False
    if re.search(r"\b(?:contest|competition|event|rules|deadline|entries|winner|prize|submission\s+window|bracket|round|judge)\b", text, re.I):
        return True
    return bool(CHALLENGE_CONTEST_RE.search(text))


def _positive_counter_items(counter: Counter[str]):
    return ((k, v) for k, v in counter.items() if int(v) > 0)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def subject_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", str(value or "").strip().lower()).strip("-") or "unknown-subject"


def safe_text(value: Any, limit: int = 220) -> str:
    text = str(value or "")
    text = MENTION_RE.sub("[redacted-mention]", text)
    text = LONG_ID_RE.sub("[redacted-id]", text)
    text = EMAIL_RE.sub("[redacted-email]", text)
    text = URL_RE.sub("[redacted-url]", text)
    text = re.sub(r"\s+", " ", text).strip()
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 1)].rstrip() + "…"


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone() is not None


def columns(conn: sqlite3.Connection, table: str) -> set[str]:
    if not table_exists(conn, table):
        return set()
    return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}


def ensure_entity_intelligence_schema(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS entity_intelligence_facts (
            id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id INTEGER, subject_key TEXT NOT NULL,
            subject_name TEXT, fact_type TEXT NOT NULL, fact_label TEXT NOT NULL, fact_value TEXT,
            source_type TEXT, source_table TEXT, source_row_id TEXT, source_channel_name TEXT,
            source_channel_policy TEXT, visibility TEXT NOT NULL, authority TEXT NOT NULL,
            confidence REAL NOT NULL DEFAULT 0.5, public_safe INTEGER NOT NULL DEFAULT 0,
            review_only INTEGER NOT NULL DEFAULT 1, needs_owner_review INTEGER NOT NULL DEFAULT 0,
            first_seen_at TEXT, last_seen_at TEXT, evidence_count INTEGER NOT NULL DEFAULT 1,
            mention_count INTEGER NOT NULL DEFAULT 1, status TEXT NOT NULL DEFAULT 'active',
            UNIQUE(guild_id, subject_key, fact_type, fact_label, fact_value, authority, visibility)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS entity_intelligence_edges (
            id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id INTEGER, subject_key TEXT NOT NULL,
            object_key TEXT NOT NULL, object_label TEXT NOT NULL, relation_type TEXT NOT NULL,
            source_type TEXT, source_channel_policy TEXT, visibility TEXT NOT NULL,
            authority TEXT NOT NULL, confidence REAL NOT NULL DEFAULT 0.5, first_seen_at TEXT,
            last_seen_at TEXT, evidence_count INTEGER NOT NULL DEFAULT 1, status TEXT NOT NULL DEFAULT 'active',
            UNIQUE(guild_id, subject_key, object_key, relation_type, authority, visibility)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS entity_activity_rollups (
            id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id INTEGER, subject_key TEXT NOT NULL,
            channel_name TEXT, channel_policy TEXT, activity_type TEXT NOT NULL, first_seen_at TEXT,
            last_seen_at TEXT, authored_count INTEGER NOT NULL DEFAULT 0, mentioned_count INTEGER NOT NULL DEFAULT 0,
            link_count INTEGER NOT NULL DEFAULT 0, bnl_interaction_count INTEGER NOT NULL DEFAULT 0,
            music_signal_count INTEGER NOT NULL DEFAULT 0, contest_signal_count INTEGER NOT NULL DEFAULT 0,
            collaboration_signal_count INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL DEFAULT 'active',
            UNIQUE(guild_id, subject_key, channel_name, channel_policy, activity_type)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS entity_open_questions (
            id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id INTEGER, subject_key TEXT NOT NULL,
            question TEXT NOT NULL, reason TEXT, source_type TEXT, visibility TEXT NOT NULL DEFAULT 'review_only',
            priority INTEGER NOT NULL DEFAULT 50, status TEXT NOT NULL DEFAULT 'open', created_at TEXT, updated_at TEXT,
            UNIQUE(guild_id, subject_key, question)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS entity_profile_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id INTEGER, subject_key TEXT NOT NULL,
            subject_name TEXT, summary_json TEXT NOT NULL, source_hash TEXT, created_at TEXT, updated_at TEXT,
            UNIQUE(guild_id, subject_key)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS entity_scouting_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id INTEGER, subject_key TEXT NOT NULL,
            subject_name TEXT, reason TEXT, source TEXT, last_seen_at TEXT, status TEXT NOT NULL DEFAULT 'stale',
            created_at TEXT, updated_at TEXT, UNIQUE(guild_id, subject_key, reason, source)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_entity_facts_subject ON entity_intelligence_facts(guild_id, subject_key, status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_entity_edges_subject ON entity_intelligence_edges(guild_id, subject_key, status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_entity_queue_status ON entity_scouting_queue(guild_id, status, updated_at)")
    conn.commit()


def authority_rank(authority: str) -> int:
    try:
        return ENTITY_SOURCE_AUTHORITY_ORDER.index(str(authority or "unknown"))
    except ValueError:
        return len(ENTITY_SOURCE_AUTHORITY_ORDER) - 1


def _visibility_for_source(source: str, row: dict[str, Any]) -> tuple[str, str, bool, bool]:
    policy = str(row.get("channel_policy") or row.get("source_channel_policy") or "").lower()
    channel = str(row.get("channel_name") or row.get("source_channel_name") or "")
    if source == "broadcast_memory" and bool(row.get("public_safe")) and str(row.get("status") or "active").lower() == "active":
        return "public", "broadcast_memory", True, False
    if source == "entity_evidence_events":
        authority = str(row.get("authority") or "public_discord_observed")
        if bool(row.get("public_safe_candidate")) and not bool(row.get("review_only")):
            return "public_safe_candidate", authority, True, False
        return "review_only", authority, False, True
    if source == "memory_tiers":
        trust = str(row.get("source_trust") or "").lower()
        if not trust or trust in {"legacy_unknown", "unknown", "source_blind"}:
            return "source_blind", "source_blind_memory", False, True
    if PRIVATE_POLICY_RE.search(policy) or PRIVATE_POLICY_RE.search(channel):
        return "internal_only", "internal_observation", False, True
    if source == "conversations" and (policy in PUBLIC_POLICIES or policy.startswith("public")):
        return "public_safe_candidate", "public_discord_observed", True, False
    if source == "community_presence":
        return "public_safe_candidate", "public_discord_observed", True, False
    return "review_only", "internal_observation", False, True


def _row_text(source: str, row: dict[str, Any]) -> str:
    fields = {
        "conversations": ["content"],
        "entity_evidence_events": ["safe_summary", "topic", "dossier_relevance", "raw_ref_json"],
        "relationship_journal": ["entry_type", "summary"],
        "relationship_state": ["trust_stage", "social_stance", "last_topic"],
        "memory_tiers": ["tier", "summary"],
        "user_memory_facts": ["fact_key", "fact_value"],
        "broadcast_memory": ["entry_type", "cleaned_summary", "summary", "raw_note"],
        "community_presence": ["display_name", "connection_notes", "evidence_snippets", "category"],
    }.get(source, [])
    return " ".join(str(row.get(f) or "") for f in fields).strip()


def _normalize_subject_label(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(value or "").strip().lower()).strip()


def _subject_terms(subject: str) -> set[str]:
    skey = subject_key(subject)
    return {term for term in {str(subject or "").strip().lower(), skey.replace("-", " ").lower(), _normalize_subject_label(subject)} if term}


def _has_subject_text_mention(text: Any, subject: str) -> bool:
    normalized = f" {_normalize_subject_label(text)} "
    return any(f" {term} " in normalized for term in _subject_terms(subject))


def _exact_subject_name_match(value: Any, subject: str) -> bool:
    return _normalize_subject_label(value) in _subject_terms(subject)


def _safe_int(value: Any) -> int | None:
    try:
        if value is None or str(value).strip() == "":
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _known_entity_names(conn: sqlite3.Connection, guild_id: int | None, subject: str, *, limit: int = 800) -> set[str]:
    names = {safe_text(subject, 90)}
    sources = {
        "user_profiles": ["display_name", "preferred_name"],
        "community_presence": ["display_name"],
        "entity_evidence_events": ["subject_name"],
    }
    for table, fields in sources.items():
        if not table_exists(conn, table):
            continue
        cols = columns(conn, table)
        select = [c for c in fields if c in cols]
        if not select:
            continue
        where = " WHERE guild_id=?" if guild_id is not None and "guild_id" in cols else ""
        params: tuple[Any, ...] = (guild_id,) if where else ()
        try:
            for row in conn.execute(f"SELECT {', '.join(select)} FROM {table}{where} LIMIT ?", (*params, limit)):
                for col in select:
                    name = safe_text(row[col], 90).strip()
                    if name:
                        names.add(name)
        except sqlite3.Error:
            continue
    # Local proper-name fallback catches entity names that only appear in older mixed memory rows.
    for table, fields in {
        "memory_tiers": ["summary"],
        "relationship_journal": ["summary"],
        "broadcast_memory": ["cleaned_summary", "summary", "raw_note"],
    }.items():
        if not table_exists(conn, table):
            continue
        cols = columns(conn, table)
        select = [c for c in fields if c in cols]
        if not select:
            continue
        where = " WHERE guild_id=?" if guild_id is not None and "guild_id" in cols else ""
        params = (guild_id,) if where else ()
        try:
            for row in conn.execute(f"SELECT {', '.join(select)} FROM {table}{where} LIMIT ?", (*params, min(limit, 200))):
                text = " ".join(str(row[col] or "") for col in select)
                for match in re.finditer(r"\b[A-Z][A-Za-z0-9_-]{2,}(?:\s+[A-Z][A-Za-z0-9_-]{2,}){0,2}\b", text):
                    candidate = _canonical_entity_label(match.group(0))
                    if candidate and _route_label_kind(candidate) in {"person_or_project", "core_barcode_entity"}:
                        names.add(candidate)
        except sqlite3.Error:
            continue
    return {n for n in names if n and _normalize_subject_label(n)}


def _mentioned_known_entities(text: Any, known_names: set[str]) -> set[str]:
    normalized = f" {_normalize_subject_label(text)} "
    mentioned: set[str] = set()
    for name in known_names:
        n = _normalize_subject_label(name)
        if n and f" {n} " in normalized:
            mentioned.add(name)
    return mentioned


def _memory_tier_has_source_metadata(row: dict[str, Any]) -> bool:
    trust = str(row.get("source_trust") or "").strip().lower()
    policy = str(row.get("source_channel_policy") or "").strip().lower()
    blind_values = {"", "legacy_unknown", "unknown", "source_blind", "none", "null"}
    return bool(trust not in blind_values and policy not in blind_values)


def _subject_row_scope(table: str, row: dict[str, Any], subject: str, skey: str, matched_ids: set[int], known_names: set[str]) -> tuple[str, str]:
    text = _row_text(table, row)
    subject_in_text = _has_subject_text_mention(text, subject)
    mentioned_entities = _mentioned_known_entities(text, known_names)
    id_match = any((_safe_int(row.get(c)) in matched_ids) for c in ("user_id", "author_id", "discord_user_id", "member_id"))
    key_match = str(row.get("subject_key") or "").strip() == skey
    exact_author_name = any(_exact_subject_name_match(row.get(c), subject) for c in ("user_name", "author_name"))
    exact_display_name = any(_exact_subject_name_match(row.get(c), subject) for c in ("display_name", "preferred_name"))
    exact_subject_name = _exact_subject_name_match(row.get("subject_name"), subject)
    mixed_unowned = len(mentioned_entities) >= 2 and not (id_match or key_match or exact_author_name or exact_subject_name)

    if table == "conversations":
        if id_match or exact_author_name:
            return "subject_authored", "conversation author identity matched subject"
        if subject_in_text:
            return ("global_mixed_memory", "conversation co-mentioned multiple known entities") if mixed_unowned else ("subject_co_mention", "conversation text mentions subject but another author owns row")
        return "rejected", "conversation not subject-owned and no subject mention"

    if table == "entity_evidence_events":
        if key_match:
            return "subject_keyed", "entity evidence subject_key matched"
        if exact_subject_name:
            return "subject_direct_evidence", "entity evidence subject_name exactly matched"
        if subject_in_text:
            return ("global_mixed_memory", "entity evidence text mixed multiple subjects without keyed ownership") if mixed_unowned else ("subject_co_mention", "entity evidence text-only subject mention")
        return "rejected", "entity evidence did not match subject identity"

    if table == "memory_tiers":
        if subject_in_text and len(mentioned_entities) >= 2 and not id_match:
            return "global_mixed_memory", "source-blind/memory text mentions multiple known entities"
        if not _memory_tier_has_source_metadata(row):
            return ("source_blind_global", "memory_tiers lacks source metadata/source policy") if subject_in_text or id_match else ("rejected", "source-blind memory without subject signal")
        if id_match:
            return "subject_profile_matched", "memory_tiers user_id matched subject profile"
        if subject_in_text:
            return "subject_co_mention", "memory_tiers text-only subject mention"
        return "rejected", "memory_tiers did not match subject identity"

    if table == "relationship_journal":
        if id_match:
            return "subject_profile_matched", "relationship_journal user_id matched subject profile"
        if subject_in_text:
            return ("global_mixed_memory", "relationship_journal mentions multiple entities without ownership") if mixed_unowned else ("subject_co_mention", "relationship_journal text-only subject mention")
        return "rejected", "relationship_journal did not match subject identity"

    if table in {"user_memory_facts", "user_habits", "relationship_state"}:
        if id_match or exact_display_name:
            return "subject_profile_matched", f"{table} subject identity matched"
        return "rejected", f"{table} requires user_id or exact subject identity match"

    if table == "broadcast_memory":
        public_safe_active = bool(row.get("public_safe")) and str(row.get("status") or "active").lower() == "active"
        if subject_in_text and public_safe_active and not mixed_unowned:
            return "subject_direct_evidence", "broadcast_memory public-safe active exact subject mention"
        if subject_in_text:
            return ("global_mixed_memory", "broadcast_memory mixed multiple names without subject ownership") if mixed_unowned else ("subject_co_mention", "broadcast_memory subject mention is background only")
        return "rejected", "broadcast_memory did not explicitly mention subject"

    if table == "community_presence":
        if key_match:
            return "subject_keyed", "community_presence subject_key matched"
        if exact_display_name:
            return "subject_direct_evidence", "community_presence display_name exactly matched"
        return "rejected", "community_presence requires subject_key or exact display_name"

    if id_match:
        return "subject_owned", "row user identity matched subject"
    if subject_in_text:
        return ("global_mixed_memory", "row mentions multiple known entities without ownership") if mixed_unowned else ("subject_co_mention", "text-only mention")
    return "rejected", "no subject signal"


def _is_subject_owned_row(row: dict[str, Any]) -> bool:
    return row.get("scope") in STRONG_SUBJECT_SCOPES


def _is_subject_co_mention_row(row: dict[str, Any]) -> bool:
    return row.get("scope") == "subject_co_mention"


def _is_global_or_mixed_memory_row(row: dict[str, Any]) -> bool:
    return row.get("scope") in {"global_mixed_memory", "source_blind_global"}


def _should_extract_relationships_from_row(row: dict[str, Any]) -> bool:
    scope = row.get("scope")
    if scope not in {"subject_owned", "subject_keyed", "subject_authored", "subject_direct_evidence"}:
        return False
    if row.get("source") == "relationship_journal" and scope not in {"subject_owned", "subject_keyed", "subject_authored"}:
        return False
    return not _is_global_or_mixed_memory_row(row)


def _should_extract_named_topics_from_row(row: dict[str, Any]) -> bool:
    if _is_global_or_mixed_memory_row(row) or _is_subject_co_mention_row(row):
        return False
    if row.get("source") == "memory_tiers" and row.get("scope") == "subject_profile_matched" and not row.get("source_metadata_present"):
        return False
    return row.get("scope") in STRONG_SUBJECT_SCOPES


def _collect_rows(conn: sqlite3.Connection, subject: str, guild_id: int | None, max_rows: int) -> list[dict[str, Any]]:
    skey = subject_key(subject)
    rows: list[dict[str, Any]] = []
    matched_ids: set[int] = set()
    scope_counts: Counter[str] = Counter()

    if table_exists(conn, "user_profiles"):
        cols = columns(conn, "user_profiles")
        select = [c for c in ("user_id", "guild_id", "display_name", "preferred_name") if c in cols]
        where = " WHERE guild_id=?" if guild_id is not None and "guild_id" in cols else ""
        params = (guild_id,) if where else ()
        for r in conn.execute(f"SELECT {', '.join(select)} FROM user_profiles{where} LIMIT ?", (*params, max_rows * 3)):
            d = dict(r)
            if any(_exact_subject_name_match(d.get(k), subject) for k in ("display_name", "preferred_name")):
                uid = _safe_int(d.get("user_id"))
                if uid is not None:
                    matched_ids.add(uid)
    known_names = _known_entity_names(conn, guild_id, subject)
    table_cols = {
        "conversations": ["id", "rowid", "user_id", "author_id", "discord_user_id", "member_id", "user_name", "author_name", "guild_id", "channel_name", "channel_policy", "role", "content", "timestamp"],
        "entity_evidence_events": ["id", "guild_id", "subject_key", "subject_name", "source_type", "source_table", "source_row_id", "channel_name", "channel_policy", "visibility", "authority", "safe_summary", "topic", "dossier_relevance", "public_safe_candidate", "review_only", "raw_ref_json", "created_at", "observed_at"],
        "relationship_journal": ["id", "user_id", "guild_id", "entry_type", "summary", "timestamp"],
        "relationship_state": ["user_id", "guild_id", "trust_stage", "social_stance", "last_topic", "updated_at"],
        "memory_tiers": ["id", "user_id", "guild_id", "tier", "summary", "mentions", "updated_at", "source_trust", "source_channel_policy"],
        "user_memory_facts": ["id", "user_id", "guild_id", "fact_key", "fact_value", "confidence", "updated_at"],
        "broadcast_memory": ["id", "guild_id", "entry_type", "cleaned_summary", "summary", "raw_note", "public_safe", "status", "created_at"],
        "community_presence": ["guild_id", "subject_key", "display_name", "connection_notes", "evidence_snippets", "category", "mention_count", "direct_interaction_count", "last_seen_at"],
    }
    for table, wanted in table_cols.items():
        if not table_exists(conn, table):
            continue
        cols = columns(conn, table)
        select = [c for c in wanted if c in cols or c == "rowid"]
        if not select:
            continue
        select_sql = ", ".join("rowid AS rowid" if c == "rowid" else c for c in select)
        where = " WHERE guild_id=?" if guild_id is not None and "guild_id" in cols else ""
        params = (guild_id,) if where else ()
        order = " ORDER BY " + ("timestamp" if "timestamp" in cols else "updated_at" if "updated_at" in cols else "created_at" if "created_at" in cols else "rowid") + " DESC"
        for r in conn.execute(f"SELECT {select_sql} FROM {table}{where}{order} LIMIT ?", (*params, max_rows * 6)):
            d = dict(r)
            text = _row_text(table, d)
            scope, scope_reason = _subject_row_scope(table, d, subject, skey, matched_ids, known_names)
            scope_counts[scope] += 1
            if scope == "rejected":
                continue
            visibility, authority, public_safe, review_only = _visibility_for_source(table, d)
            if scope in {"global_mixed_memory", "source_blind_global"}:
                visibility, authority, public_safe, review_only = "source_blind", "source_blind_memory", False, True
            elif scope == "subject_co_mention":
                public_safe, review_only = False, True
                if visibility == "public_safe_candidate":
                    visibility = "review_only"
            rows.append({
                "source": table,
                "row_id": d.get("id") or d.get("rowid"),
                "text": text,
                "channel_name": d.get("channel_name"),
                "channel_policy": d.get("channel_policy") or d.get("source_channel_policy"),
                "observed_at": d.get("timestamp") or d.get("updated_at") or d.get("created_at") or d.get("last_seen_at") or now_iso(),
                "relation": "authored" if scope == "subject_authored" else "mentioned",
                "scope": scope,
                "scopeReason": scope_reason,
                "source_metadata_present": _memory_tier_has_source_metadata(d) if table == "memory_tiers" else True,
                "visibility": visibility,
                "authority": authority,
                "public_safe": public_safe,
                "review_only": review_only,
            })
            if len(rows) >= max_rows:
                rows[0]["_scope_counts"] = dict(scope_counts)
                return rows
    if rows:
        rows[0]["_scope_counts"] = dict(scope_counts)
    return rows[:max_rows]

def _item(label: str, value: str = "", *, visibility: str, authority: str, confidence: float = 0.65, public_safe: bool = False, review_only: bool = True, needs_owner_review: bool = False, evidence_count: int = 1) -> dict[str, Any]:
    return {
        "label": label,
        "value": value or label,
        "visibility": visibility,
        "authority": authority,
        "confidence": confidence,
        "publicSafe": bool(public_safe),
        "reviewOnly": bool(review_only),
        "needsOwnerReview": bool(needs_owner_review),
        "evidenceCount": int(evidence_count),
    }


def _classify(subject: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    role_counts: Counter[str] = Counter()
    role_reasons: dict[str, str] = {}
    themes: Counter[str] = Counter()
    behaviors: Counter[str] = Counter()
    music = Counter()
    events = Counter()
    activity = Counter()
    edges: dict[tuple[str, str], dict[str, Any]] = {}
    label_traces: list[dict[str, Any]] = []
    facts: list[dict[str, Any]] = []
    skey = subject_key(subject)
    source_counts = Counter(r["source"] for r in rows)
    scope_counts = Counter(str(r.get("scope") or "rejected") for r in rows)
    if rows and isinstance(rows[0].get("_scope_counts"), dict):
        scope_counts.update({k: int(v) for k, v in rows[0]["_scope_counts"].items() if k == "rejected"})
    public_rows = sum(1 for r in rows if r.get("public_safe"))
    review_rows = len(rows) - public_rows
    extraction_rows = [r for r in rows if _should_extract_named_topics_from_row(r)]

    def trace_label(label: str, kind: str, accepted: bool, reason: str, row: dict[str, Any]) -> None:
        label_traces.append({
            "label": safe_text(label, 90),
            "kind": kind,
            "accepted": bool(accepted),
            "reason": reason,
            "sourceScope": row.get("scope") or "unknown",
            "sourceTable": row.get("source") or "unknown",
            "evidenceCount": 1,
        })

    def add_edge(obj: str, rel: str, row: dict[str, Any], confidence: float = 0.62, extraction: str = "") -> None:
        obj = _canonical_entity_label(obj)
        if not obj or subject_key(obj) == skey:
            return
        if not _is_valid_relationship_object_label(obj, extraction=extraction):
            trace_label(obj, f"relationship:{rel}", False, _route_label_kind(obj), row)
            return
        key = (subject_key(obj), rel)
        existing = edges.get(key)
        if existing:
            existing["evidenceCount"] += 1
            existing["confidence"] = min(0.95, max(existing["confidence"], confidence) + 0.05)
            existing["publicSafe"] = existing["publicSafe"] and bool(row.get("public_safe"))
            existing["reviewOnly"] = existing["reviewOnly"] or bool(row.get("review_only"))
            trace_label(obj, f"relationship:{rel}", True, "merged_existing_edge", row)
            return
        trace_label(obj, f"relationship:{rel}", True, "validated_entity_label", row)
        edges[key] = {
            "objectKey": subject_key(obj), "objectLabel": obj, "relationType": rel,
            "visibility": "review_only" if row.get("review_only") else "public_safe_candidate",
            "authority": row.get("authority") or "bnl_inferred", "confidence": confidence,
            "publicSafe": bool(row.get("public_safe")) and rel in {"collaborates_with", "offers_collaboration", "associated_with"},
            "reviewOnly": True if rel in {"speaks_through", "created", "created_by", "co_mentioned_only"} else bool(row.get("review_only")),
            "needsOwnerReview": rel in {"speaks_through", "created", "created_by", "co_mentioned_only"},
            "evidenceCount": 1,
        }

    for row in extraction_rows:
        text = str(row.get("text") or "")
        low = text.lower()
        for role, pat, reason in ROLE_PATTERNS:
            if role == "contest operator" and not _has_contest_event_signal(text):
                continue
            if pat.search(text):
                role_counts[role] += 1
                role_reasons.setdefault(role, reason)
        for theme, pat in THEME_PATTERNS:
            if pat.search(text):
                themes[theme] += 1
        if BNL_RE.search(text):
            activity["frequent BNL-facing conversation"] += 1
        if re.search(r"\bsource files?|dossiers?\b", text, re.I):
            activity["asks BNL Source File questions"] += 1
        if re.search(r"\bboundar|behavior|what can you say|liaison|interface\b", text, re.I):
            activity["discusses BNL boundaries/behavior"] += 1
        if MUSIC_RE.search(text):
            music["shares music links"] += 1
            if re.search(r"\b(?:suno|udio|youtube|youtu\.be|soundcloud|spotify|bandcamp)\b|https?://", text, re.I):
                music["shares Suno/Udio/YouTube/SoundCloud/Spotify/Bandcamp links"] += 1
        if "finished-tracks" in low:
            music["posts in finished-tracks"] += 1
        if "wips-and-demos" in low or "wip" in low or "demo" in low:
            music["posts in wips-and-demos"] += 1
        if "collaboration-hub" in low:
            music["posts in collaboration-hub"] += 1
        if COLLAB_RE.search(text):
            music["offers collaboration"] += 1
            activity["collaboration-offer facet"] += 1
        if re.search(r"\bfeedback\b", text, re.I):
            music["asks for feedback"] += 1
        if ANTAGONISTIC_RE.search(text):
            behaviors["antagonistic/challenging"] += 1
            if BNL_RE.search(text):
                activity["challenges BNL"] += 1
        if ANOMALY_RE.search(text):
            behaviors["theory/anomaly-heavy"] += 1
        if _has_contest_event_signal(text):
            behaviors["contest-focused"] += 1
            events["contest/event activity"] += 1
            if re.search(r"\b(?:rules|instructions|deadline|entries|submission\s+window|bracket|round|judge)\b", text, re.I):
                events["rules/instructions poster"] += 1
            if re.search(r"\b(?:organize|run|host|operator|promote)\b", text, re.I):
                events["contest organizer"] += 1
        if re.search(r"\b(?:help|support|welcome|community)\b", text, re.I):
            behaviors["helpful/supportive"] += 1
        if re.search(r"\b(?:playful|chaotic|teas)\b", text, re.I):
            behaviors["playful/chaotic"] += 1
        if re.search(r"\b(?:lore|persona|entity|cipher|riddle)\b", text, re.I):
            behaviors["lore-heavy"] += 1
        # relationship semantics; only strong subject-owned/keyed/authored evidence can create edges.
        if not _should_extract_relationships_from_row(row):
            continue
        for m in THROUGH_RE.finditer(text):
            a, b = _clean_through_left_label(m.group(1)), _canonical_entity_label(m.group(2))
            if subject_key(b).startswith(f"{skey}-"):
                b = subject
            if not a or not _is_valid_relationship_object_label(a, extraction="through"):
                if a:
                    trace_label(a, "relationship:speaks_through", False, _route_label_kind(a), row)
                continue
            if subject_key(b) == skey:
                add_edge(a, "speaks_through", row, 0.72, extraction="through")
            elif subject_key(a) == skey and _is_valid_relationship_object_label(b, extraction="through"):
                add_edge(b, "relayed_through", row, 0.66, extraction="through")
        for m in CREATED_RE.finditer(text):
            a, b = _canonical_entity_label(m.group(1)), _canonical_entity_label(m.group(2))
            if subject_key(a) == skey and _is_valid_relationship_object_label(b):
                add_edge(b, "created", row, 0.78)
            elif subject_key(b) == skey and _is_valid_relationship_object_label(a):
                add_edge(a, "created_by", row, 0.78)
        for m in CREATED_MY_RE.finditer(text):
            obj = _canonical_entity_label(m.group(1))
            if row.get("scope") in {"subject_authored", "subject_keyed", "subject_direct_evidence"} and _is_valid_relationship_object_label(obj):
                add_edge(obj, "created", row, 0.70)
        for m in HERE_RE.finditer(text):
            obj = _canonical_entity_label(m.group(1))
            if subject_key(obj) != skey and _is_valid_relationship_object_label(obj, extraction="here"):
                add_edge(obj, "associated_with", row, 0.50, extraction="here")
            elif subject_key(obj) != skey:
                trace_label(obj, "relationship:associated_with", False, _route_label_kind(obj), row)
        cm = re.search(r"\bcollab(?:orate)?\s+with\s+([A-Z][A-Za-z0-9_-]{2,}(?:\s+[A-Z][A-Za-z0-9_-]{2,}){0,2})", text, re.I)
        if cm:
            add_edge(_canonical_entity_label(cm.group(1)), "offers_collaboration", row, 0.68)

    source_blind_count = sum(1 for row in rows if row.get("authority") == "source_blind_memory" or row.get("visibility") == "source_blind")
    global_mixed_count = sum(1 for row in rows if row.get("scope") == "global_mixed_memory")
    source_blind_facts = []
    if source_blind_count:
        source_blind_facts.append(_item("source-blind or mixed memory mentions this subject", "Source-blind or mixed memory mentions this subject; review original source before use.", visibility="source_blind", authority="source_blind_memory", confidence=0.4, public_safe=False, review_only=True, needs_owner_review=True, evidence_count=source_blind_count) | {"type": "source_warning"})
    if not role_counts:
        role_counts["unknown"] = 1
        role_reasons["unknown"] = "not enough structured role evidence yet"
    if len(extraction_rows) >= 8:
        behaviors["frequent poster"] += len(extraction_rows)
    elif extraction_rows:
        behaviors["occasional/low activity"] += len(extraction_rows)
    for label, count in _positive_counter_items(role_counts):
        # moderator/contest roles are review-needed until public role is confirmed.
        review_needed = label in {"moderator", "contest operator", "lore/entity/persona/project creator", "antagonist/challenger"}
        facts.append(_item(label, role_reasons.get(label, label), visibility="review_only" if review_needed else "public_safe_candidate", authority="bnl_inferred", confidence=min(0.9, 0.55 + count * 0.08), public_safe=not review_needed and label != "unknown", review_only=review_needed or label == "unknown", needs_owner_review=review_needed, evidence_count=count) | {"type": "role"})
    roles = facts[:]
    creative = [_item(k, k, visibility="public_safe_candidate", authority="public_discord_observed", confidence=min(0.9, .55 + v*.08), public_safe=True, review_only=False, needs_owner_review=k.startswith("shares") and "links" in k, evidence_count=v) | {"type": "creative_music"} for k, v in _positive_counter_items(music)]
    bnl = [_item(k, k, visibility="review_only" if "challenges" in k else "public_safe_candidate", authority="bnl_inferred", confidence=min(.9,.55+v*.08), public_safe="challenges" not in k, review_only="challenges" in k, needs_owner_review="challenges" in k, evidence_count=v) | {"type": "bnl_interaction"} for k, v in _positive_counter_items(activity) if "BNL" in k or "source-file" in k or "boundaries" in k or "challenges" in k]
    behavior = [_item(k, k, visibility="review_only" if k in {"antagonistic/challenging", "theory/anomaly-heavy", "lore-heavy"} else "public_safe_candidate", authority="bnl_inferred", confidence=min(.9,.55+v*.06), public_safe=k not in {"antagonistic/challenging", "theory/anomaly-heavy", "lore-heavy"}, review_only=k in {"antagonistic/challenging", "theory/anomaly-heavy", "lore-heavy"}, needs_owner_review=k in {"antagonistic/challenging", "theory/anomaly-heavy", "lore-heavy"}, evidence_count=v) | {"type": "behavior_posture"} for k, v in _positive_counter_items(behaviors)]
    theme_items = [_item(k, k, visibility="review_only" if k in {"strange/anomaly conversations", "AI/persona/project interaction"} else "public_safe_candidate", authority="bnl_inferred", confidence=min(.9,.55+v*.06), public_safe=k not in {"strange/anomaly conversations", "AI/persona/project interaction"}, review_only=k in {"strange/anomaly conversations", "AI/persona/project interaction"}, needs_owner_review=k in {"strange/anomaly conversations", "AI/persona/project interaction"}, evidence_count=v) | {"type": "conversation_theme"} for k, v in _positive_counter_items(themes)]
    event_items = [_item(k, k, visibility="review_only" if k != "contest/event activity" else "public_safe_candidate", authority="bnl_inferred", confidence=min(.9,.55+v*.08), public_safe=k == "contest/event activity", review_only=k != "contest/event activity", needs_owner_review=True, evidence_count=v) | {"type": "event_contest"} for k, v in _positive_counter_items(events)]
    action_items = []
    missing = []
    if edges:
        for edge in edges.values():
            if edge.get("needsOwnerReview"):
                action_items.append(_item(f"Confirm whether {edge['objectLabel']} relationship/context can be public", edge["relationType"], visibility="review_only", authority="bnl_inferred", public_safe=False, review_only=True, needs_owner_review=True, confidence=.7) | {"type": "action_item"})
    if music:
        action_items.append(_item("Confirm public artist links and whether music links are portfolio material, not submissions", "queue submission bridge not connected", visibility="review_only", authority="bnl_inferred", public_safe=False, review_only=True, needs_owner_review=True) | {"type": "action_item"})
        missing.append("public artist links / public music role")
    if role_counts.get("moderator"):
        action_items.append(_item("Confirm public-safe mod/staff role wording", "mod role should not be published without confirmation", visibility="review_only", authority="bnl_inferred", public_safe=False, review_only=True, needs_owner_review=True) | {"type": "action_item"})
        missing.append("public-safe role confirmation")
    if events:
        action_items.append(_item("Confirm contest name, rules, channel, dates, and public link", "contest/event evidence needs owner/mod confirmation", visibility="review_only", authority="bnl_inferred", public_safe=False, review_only=True, needs_owner_review=True) | {"type": "action_item"})
        missing.append("contest name/rules/channel/dates/public link")
    if behaviors.get("antagonistic/challenging"):
        action_items.append(_item("Keep antagonistic/challenging posture review-only; do not produce defamatory public copy", "public-safety caution", visibility="review_only", authority="bnl_inferred", public_safe=False, review_only=True, needs_owner_review=True) | {"type": "warning"})
    if behaviors.get("theory/anomaly-heavy") or themes.get("strange/anomaly conversations"):
        action_items.append(_item("Determine whether anomaly/lore context is public-safe lore or internal-only", "lore/anomaly context needs review", visibility="review_only", authority="bnl_inferred", public_safe=False, review_only=True, needs_owner_review=True) | {"type": "action_item"})
        missing.append("lore/anomaly public-safety status")
    missing.extend(["official_public_dossier source not connected yet", "queue submission bridge not_connected"])
    dossier = []
    if extraction_rows:
        dossier.append(_item("active community source file candidate", "subject-owned/keyed local evidence exists", visibility="review_only", authority="bnl_inferred", public_safe=False, review_only=True, needs_owner_review=True, evidence_count=len(extraction_rows)) | {"type": "dossier_usefulness"})
    if source_blind_count:
        dossier.append(_item("source-blind memory present", "source_blind_memory review-only; do not use publicly without owner/admin review", visibility="source_blind", authority="source_blind_memory", confidence=0.4, public_safe=False, review_only=True, needs_owner_review=True, evidence_count=source_blind_count) | {"type": "dossier_usefulness"})
    if missing:
        dossier.append(_item("needs owner approval", "; ".join(sorted(set(missing))[:4]), visibility="review_only", authority="bnl_inferred", public_safe=False, review_only=True, needs_owner_review=True) | {"type": "dossier_usefulness"})
    return {
        "facts": facts + source_blind_facts + creative + bnl + behavior + theme_items + event_items + dossier,
        "roles": roles,
        "relationships": list(edges.values()),
        "creativeMusic": creative,
        "bnlInteraction": bnl,
        "behaviorPosture": behavior,
        "conversationThemes": theme_items,
        "eventContest": event_items,
        "dossierUsefulness": dossier,
        "actionItems": action_items,
        "missingInfo": sorted(set(missing)),
        "labelEvidenceTraces": label_traces[:80],
        "warnings": [i for i in action_items if i.get("type") == "warning"],
        "sourceCounts": dict(source_counts),
        "scopeCounts": dict(scope_counts),
        "extractableRows": len(extraction_rows),
        "publicSafeRows": public_rows,
        "reviewOnlyRows": review_rows,
        "globalMixedRows": global_mixed_count,
        "sourceBlindRows": source_blind_count,
    }


def _upsert_fact(conn: sqlite3.Connection, guild_id: int | None, skey: str, subject: str, fact_type: str, fact: dict[str, Any]) -> None:
    ts = now_iso()
    conn.execute("""
        INSERT INTO entity_intelligence_facts (guild_id, subject_key, subject_name, fact_type, fact_label, fact_value, source_type, source_table, source_row_id, source_channel_name, source_channel_policy, visibility, authority, confidence, public_safe, review_only, needs_owner_review, first_seen_at, last_seen_at, evidence_count, mention_count, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active')
        ON CONFLICT(guild_id, subject_key, fact_type, fact_label, fact_value, authority, visibility)
        DO UPDATE SET last_seen_at=excluded.last_seen_at, evidence_count=entity_intelligence_facts.evidence_count + excluded.evidence_count, mention_count=entity_intelligence_facts.mention_count + excluded.mention_count, confidence=max(entity_intelligence_facts.confidence, excluded.confidence), status='active'
    """, (guild_id, skey, subject, fact_type, fact.get("label"), fact.get("value"), fact.get("sourceType") or "entity_intelligence_engine", fact.get("sourceTable"), None, None, None, fact.get("visibility") or "review_only", fact.get("authority") or "bnl_inferred", float(fact.get("confidence") or .5), int(bool(fact.get("publicSafe"))), int(bool(fact.get("reviewOnly"))), int(bool(fact.get("needsOwnerReview"))), ts, ts, int(fact.get("evidenceCount") or 1), int(fact.get("evidenceCount") or 1)))


def _upsert_edge(conn: sqlite3.Connection, guild_id: int | None, skey: str, edge: dict[str, Any]) -> None:
    ts = now_iso()
    conn.execute("""
        INSERT INTO entity_intelligence_edges (guild_id, subject_key, object_key, object_label, relation_type, source_type, source_channel_policy, visibility, authority, confidence, first_seen_at, last_seen_at, evidence_count, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active')
        ON CONFLICT(guild_id, subject_key, object_key, relation_type, authority, visibility)
        DO UPDATE SET last_seen_at=excluded.last_seen_at, evidence_count=entity_intelligence_edges.evidence_count + excluded.evidence_count, confidence=max(entity_intelligence_edges.confidence, excluded.confidence), status='active'
    """, (guild_id, skey, edge.get("objectKey"), edge.get("objectLabel"), edge.get("relationType"), "entity_intelligence_engine", None, edge.get("visibility") or "review_only", edge.get("authority") or "bnl_inferred", float(edge.get("confidence") or .5), ts, ts, int(edge.get("evidenceCount") or 1)))


def _upsert_question(conn: sqlite3.Connection, guild_id: int | None, skey: str, question: str, reason: str, priority: int = 50) -> None:
    ts = now_iso()
    conn.execute("""
        INSERT INTO entity_open_questions (guild_id, subject_key, question, reason, source_type, visibility, priority, status, created_at, updated_at)
        VALUES (?, ?, ?, ?, 'entity_intelligence_engine', 'review_only', ?, 'open', ?, ?)
        ON CONFLICT(guild_id, subject_key, question) DO UPDATE SET reason=excluded.reason, priority=excluded.priority, updated_at=excluded.updated_at, status='open'
    """, (guild_id, skey, question, reason, priority, ts, ts))


def build_entity_intelligence_profile(db_path: str, guild_id: int | None, subject: str, *, refresh: bool = False, max_rows: int = 240) -> dict[str, Any]:
    subject = safe_text(subject, 90) or "Unknown subject"
    skey = subject_key(subject)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        ensure_entity_intelligence_schema(conn)
        logging.info("entity_intelligence_profile_started subject_key=%s", skey)
        rows = _collect_rows(conn, subject, guild_id, max_rows)
        classified = _classify(subject, rows)
        scope_counts = classified.get("scopeCounts") or {}
        logging.info(
            "entity_intelligence_rows_collected subject_key=%s owned=%s keyed=%s authored=%s profile_matched=%s direct=%s co_mention=%s global_mixed=%s source_blind=%s rejected=%s",
            skey,
            int(scope_counts.get("subject_owned") or 0),
            int(scope_counts.get("subject_keyed") or 0),
            int(scope_counts.get("subject_authored") or 0),
            int(scope_counts.get("subject_profile_matched") or 0),
            int(scope_counts.get("subject_direct_evidence") or 0),
            int(scope_counts.get("subject_co_mention") or 0),
            int(scope_counts.get("global_mixed_memory") or 0),
            int(scope_counts.get("source_blind_global") or 0),
            int(scope_counts.get("rejected") or 0),
        )
        logging.info(
            "entity_intelligence_scope_filtered subject_key=%s rejected_global=%s rejected_source_blind=%s",
            skey, int(scope_counts.get("global_mixed_memory") or 0), int(scope_counts.get("source_blind_global") or 0),
        )
        for fact in classified["facts"]:
            _upsert_fact(conn, guild_id, skey, subject, fact.get("type") or "fact", fact)
        for edge in classified["relationships"]:
            _upsert_edge(conn, guild_id, skey, edge)
        for item in classified["actionItems"]:
            _upsert_question(conn, guild_id, skey, str(item.get("label") or "Confirm context"), str(item.get("value") or item.get("label") or ""), 70 if item.get("needsOwnerReview") else 50)
        # simple activity rollups by safe channel label/policy, never exposed as raw provenance
        roll: dict[tuple[str, str], Counter] = defaultdict(Counter)
        for row in [r for r in rows if _should_extract_named_topics_from_row(r)]:
            chan = safe_text(row.get("channel_name") or "local stored context", 80)
            pol = safe_text(row.get("channel_policy") or row.get("visibility") or "unknown", 80)
            key = (chan, pol)
            if row.get("relation") == "authored": roll[key]["authored"] += 1
            else: roll[key]["mentioned"] += 1
            if URL_RE.search(str(row.get("text") or "")): roll[key]["links"] += 1
            if BNL_RE.search(str(row.get("text") or "")): roll[key]["bnl"] += 1
            if MUSIC_RE.search(str(row.get("text") or "")): roll[key]["music"] += 1
            if CONTEST_RE.search(str(row.get("text") or "")): roll[key]["contest"] += 1
            if COLLAB_RE.search(str(row.get("text") or "")): roll[key]["collab"] += 1
        ts = now_iso()
        for (chan, pol), cnt in roll.items():
            conn.execute("""
                INSERT INTO entity_activity_rollups (guild_id, subject_key, channel_name, channel_policy, activity_type, first_seen_at, last_seen_at, authored_count, mentioned_count, link_count, bnl_interaction_count, music_signal_count, contest_signal_count, collaboration_signal_count, status)
                VALUES (?, ?, ?, ?, 'stored_context', ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active')
                ON CONFLICT(guild_id, subject_key, channel_name, channel_policy, activity_type)
                DO UPDATE SET last_seen_at=excluded.last_seen_at, authored_count=excluded.authored_count, mentioned_count=excluded.mentioned_count, link_count=excluded.link_count, bnl_interaction_count=excluded.bnl_interaction_count, music_signal_count=excluded.music_signal_count, contest_signal_count=excluded.contest_signal_count, collaboration_signal_count=excluded.collaboration_signal_count, status='active'
            """, (guild_id, skey, chan, pol, ts, ts, cnt["authored"], cnt["mentioned"], cnt["links"], cnt["bnl"], cnt["music"], cnt["contest"], cnt["collab"]))
        diagnostics = {
            "ledgerStatus": "ready",
            "profileStatus": "refreshed" if refresh else "built",
            "rowsScanned": len(rows),
            "rowsBySource": classified["sourceCounts"],
            "rowScopeCounts": classified.get("scopeCounts") or {},
            "extractableRows": classified.get("extractableRows", 0),
            "publicSafeRows": classified["publicSafeRows"],
            "reviewOnlyRows": classified["reviewOnlyRows"],
            "globalMixedRows": classified.get("globalMixedRows", 0),
            "sourceBlindRows": classified.get("sourceBlindRows", 0),
            "officialPublicDossierConnected": False,
            "officialPublicDossierNote": "official_public_dossier source not connected yet",
            "queueSubmissionStatus": "not_connected",
            "labelEvidenceTraces": classified.get("labelEvidenceTraces") or [],
        }
        profile = {
            "subjectKey": skey,
            "subjectName": subject,
            "authorityOrder": list(ENTITY_SOURCE_AUTHORITY_ORDER),
            "visibilityValues": list(ENTITY_VISIBILITY_VALUES),
            "roles": classified["roles"],
            "relationships": classified["relationships"],
            "creativeMusic": classified["creativeMusic"],
            "bnlInteraction": classified["bnlInteraction"],
            "behaviorPosture": classified["behaviorPosture"],
            "conversationThemes": classified["conversationThemes"],
            "eventContest": classified["eventContest"],
            "dossierUsefulness": classified["dossierUsefulness"],
            "actionItems": classified["actionItems"],
            "missingInfo": classified["missingInfo"],
            "warnings": classified["warnings"],
            "sourceCoverage": [{"source": k, "count": v, "status": "used"} for k, v in sorted(classified["sourceCounts"].items())],
            "diagnostics": diagnostics,
        }
        source_hash = hashlib.sha256(json.dumps(profile, sort_keys=True, default=str).encode("utf-8")).hexdigest()
        conn.execute("""
            INSERT INTO entity_profile_snapshots (guild_id, subject_key, subject_name, summary_json, source_hash, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(guild_id, subject_key) DO UPDATE SET subject_name=excluded.subject_name, summary_json=excluded.summary_json, source_hash=excluded.source_hash, updated_at=excluded.updated_at
        """, (guild_id, skey, subject, json.dumps(profile, sort_keys=True, default=str), source_hash, ts, ts))
        logging.info(
            "entity_intelligence_profile_completed subject_key=%s roles=%s relationships=%s themes=%s",
            skey, len(profile.get("roles") or []), len(profile.get("relationships") or []), len(profile.get("conversationThemes") or []),
        )
        conn.commit()
        return profile
    finally:
        conn.close()


def _safe_item_for_output(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "label": safe_text(item.get("label") or item.get("objectLabel") or item.get("relationType") or "", 160),
        "value": safe_text(item.get("value") or item.get("relationType") or "", 180),
        "visibility": item.get("visibility") or "unknown",
        "authority": item.get("authority") or "unknown",
        "confidence": round(float(item.get("confidence") or 0), 2),
        "reviewOnly": bool(item.get("reviewOnly")),
        "publicSafe": bool(item.get("publicSafe")),
        "needsOwnerReview": bool(item.get("needsOwnerReview")),
        "evidenceCount": int(item.get("evidenceCount") or 1),
    }


def _edge_for_output(edge: dict[str, Any]) -> dict[str, Any]:
    out = _safe_item_for_output(edge)
    out.update({"objectLabel": safe_text(edge.get("objectLabel"), 120), "relationType": safe_text(edge.get("relationType"), 80)})
    return out


def _allow_item(item: dict[str, Any], surface: str) -> tuple[bool, str]:
    visibility = str(item.get("visibility") or "unknown")
    authority = str(item.get("authority") or "unknown")
    public_safe = bool(item.get("publicSafe")) or visibility == "public"
    review_only = bool(item.get("reviewOnly")) or visibility in {"review_only", "mod_only", "admin_only", "internal_only", "source_blind", "account_private"}
    if authority == "source_blind_memory" and surface not in {"rd_mod_ops", "source_file", "owner_review", "internal_diagnostics"}:
        return False, "source_blind_memory"
    if surface in {"public_conversation", "ambient_public", "website_relay", "dossier_draft"}:
        if review_only or not public_safe:
            return False, "not_public_safe"
        if visibility not in {"public", "public_safe_candidate"} and authority not in {"owner_confirmed", "official_public_dossier", "broadcast_memory"}:
            return False, "visibility_restricted"
    if surface == "dossier_draft" and authority not in {"owner_confirmed", "official_public_dossier", "broadcast_memory", "public_discord_observed"}:
        return False, "authority_too_low"
    if surface in {"ambient_public", "website_relay"} and item.get("needsOwnerReview"):
        return False, "needs_owner_review"
    if surface == "owner_review" and visibility in {"account_private", "source_blind"}:
        return False, "unsafe_for_owner_public_copy"
    return True, "allowed"


def resolve_entity_context_for_surface(profile: dict[str, Any], surface: str, *, channel_policy: str | None = None, viewer_role: str | None = None, max_items: int = 12) -> dict[str, Any]:
    if surface not in SURFACES:
        raise ValueError(f"unsupported entity context surface: {surface}")
    excluded: Counter[str] = Counter()

    def filter_items(items: list[dict[str, Any]], *, edge: bool = False) -> list[dict[str, Any]]:
        allowed = []
        for item in items or []:
            ok, reason = _allow_item(item, surface)
            if ok:
                allowed.append(_edge_for_output(item) if edge else _safe_item_for_output(item))
            else:
                excluded[reason] += 1
        return allowed[:max_items]

    if surface == "internal_diagnostics":
        diag = dict(profile.get("diagnostics") or {})
        return {
            "surface": surface,
            "allowedFacts": [], "allowedRoles": [], "allowedRelationships": [], "allowedThemes": [], "allowedActivity": [],
            "allowedWarnings": [], "allowedActionItems": [], "missingInfo": [],
            "excludedReasonCounts": {},
            "sourceAuthority": list(ENTITY_SOURCE_AUTHORITY_ORDER),
            "diagnostics": {
                "ledgerStatus": diag.get("ledgerStatus"), "rowsScanned": int(diag.get("rowsScanned") or 0),
                "rowsBySource": dict(diag.get("rowsBySource") or {}),
                "publicSafeRows": int(diag.get("publicSafeRows") or 0), "reviewOnlyRows": int(diag.get("reviewOnlyRows") or 0),
                "officialPublicDossierConnected": bool(diag.get("officialPublicDossierConnected")),
                "queueSubmissionStatus": diag.get("queueSubmissionStatus") or "not_connected",
            },
        }

    roles = filter_items(profile.get("roles") or [])
    rels = filter_items(profile.get("relationships") or [], edge=True)
    themes = filter_items(profile.get("conversationThemes") or [])
    activity = []
    for key in ("creativeMusic", "bnlInteraction", "behaviorPosture", "eventContest", "dossierUsefulness"):
        activity.extend(filter_items(profile.get(key) or []))
    actions = filter_items(profile.get("actionItems") or []) if surface in {"rd_mod_ops", "source_file", "owner_review"} else []
    warnings = filter_items(profile.get("warnings") or []) if surface in {"rd_mod_ops", "source_file", "owner_review"} else []
    missing = [safe_text(x, 180) for x in (profile.get("missingInfo") or [])]
    if surface in {"public_conversation", "ambient_public", "website_relay"}:
        missing = []
    elif surface == "dossier_draft":
        missing = [x for x in missing if "not_connected" not in x][:max_items]
    context = {
        "surface": surface,
        "allowedFacts": (roles + themes + activity)[:max_items],
        "allowedRoles": roles,
        "allowedRelationships": rels,
        "allowedThemes": themes,
        "allowedActivity": activity[:max_items],
        "allowedWarnings": warnings,
        "allowedActionItems": actions,
        "missingInfo": missing[:max_items],
        "excludedReasonCounts": dict(excluded),
        "sourceAuthority": list(ENTITY_SOURCE_AUTHORITY_ORDER),
        "officialPublicDossierConnected": bool((profile.get("diagnostics") or {}).get("officialPublicDossierConnected")),
        "queueSubmissionStatus": (profile.get("diagnostics") or {}).get("queueSubmissionStatus") or "not_connected",
    }
    if surface in {"ambient_public", "website_relay"}:
        # Keep these surfaces broad: strip individual relationship objects unless official/public.
        context["allowedRelationships"] = [r for r in context["allowedRelationships"] if r.get("authority") in {"owner_confirmed", "official_public_dossier", "broadcast_memory"}][:max_items]
    return context


def build_entity_context_for_public_conversation(profile: dict[str, Any], *, max_lines: int = 4) -> list[str]:
    context = resolve_entity_context_for_surface(profile, "public_conversation", max_items=max_lines)
    lines = []
    for item in (context.get("allowedRoles") or []) + (context.get("allowedThemes") or []) + (context.get("allowedActivity") or []):
        label = safe_text(item.get("label"), 100)
        if label and label not in lines:
            lines.append(label)
        if len(lines) >= max_lines:
            break
    return lines


def build_entity_context_for_ambient_public(profile: dict[str, Any], *, max_items: int = 6) -> dict[str, Any]:
    return resolve_entity_context_for_surface(profile, "ambient_public", max_items=max_items)


def build_entity_context_for_website_relay(profile: dict[str, Any], *, max_items: int = 6) -> dict[str, Any]:
    return resolve_entity_context_for_surface(profile, "website_relay", max_items=max_items)


def build_entity_context_for_rd_mod_ops(db_path: str, guild_id: int | None, subject: str, *, max_items: int = 8) -> dict[str, Any]:
    profile = build_entity_intelligence_profile(db_path, guild_id, subject, refresh=True)
    return resolve_entity_context_for_surface(profile, "rd_mod_ops", max_items=max_items)


def mark_entity_scouting_stale(conn: sqlite3.Connection, guild_id: int | None, subject: str, *, reason: str, source: str, last_seen_at: str | None = None) -> None:
    ensure_entity_intelligence_schema(conn)
    ts = now_iso()
    conn.execute("""
        INSERT INTO entity_scouting_queue (guild_id, subject_key, subject_name, reason, source, last_seen_at, status, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, 'stale', ?, ?)
        ON CONFLICT(guild_id, subject_key, reason, source) DO UPDATE SET last_seen_at=excluded.last_seen_at, status='stale', updated_at=excluded.updated_at
    """, (guild_id, subject_key(subject), safe_text(subject, 90), safe_text(reason, 180), safe_text(source, 80), last_seen_at or ts, ts, ts))
    conn.commit()


def resolve_authoritative_fact(facts: list[dict[str, Any]]) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    """Return highest-authority fact and owner-review conflicts for same labels."""
    if not facts:
        return None, []
    ordered = sorted(facts, key=lambda f: (authority_rank(str(f.get("authority") or "unknown")), -float(f.get("confidence") or 0)))
    winner = ordered[0]
    conflicts = []
    for fact in ordered[1:]:
        if safe_text(fact.get("label")).lower() == safe_text(winner.get("label")).lower() and safe_text(fact.get("value")).lower() != safe_text(winner.get("value")).lower():
            if authority_rank(str(fact.get("authority") or "unknown")) <= authority_rank("public_discord_observed"):
                conflicts.append(_item("Owner review needed for conflicting high-authority entity fact", f"{winner.get('value')} vs {fact.get('value')}", visibility="review_only", authority="bnl_inferred", public_safe=False, review_only=True, needs_owner_review=True) | {"type": "action_item"})
    return winner, conflicts
