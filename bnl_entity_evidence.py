"""Structured entity/source evidence events for BNL internal source workflows.

This is a conservative evidence layer over existing local memory/source stores. It
stores short review-safe summaries and provenance pointers; it does not create
canonical profiles, website dossiers, Source Files, queue links, or public output.
"""

from __future__ import annotations

import json
import logging
import re
import sqlite3
from collections import Counter
from datetime import datetime, timezone
from typing import Any

from bnl_dossier_source_packets import normalize_subject_name, subject_key

ENTITY_EVIDENCE_TABLE = "entity_evidence_events"
PUBLIC_CONVERSATION_POLICIES = {"public_home", "public_context", "public_selective", "broadcast_memory"}
QUEUE_NOT_CONNECTED_NOTE = "Queue/submission identity is not connected yet."
MAX_SAFE_SUMMARY_LENGTH = 220
MAX_REF_SNIPPET_LENGTH = 160

ALLOWED_EVIDENCE_KINDS = {
    "profile_match",
    "authored_public_conversation",
    "mentioned_public_conversation",
    "authored_review_only_conversation",
    "mentioned_review_only_conversation",
    "music_or_show_signal",
    "community_signal",
    "bnl_interaction",
    "relationship_context_review_only",
    "source_blind_memory_review_only",
    "broadcast_memory_signal",
    "r_and_d_context_review_only",
    "community_presence_signal",
}

_DISCORD_MENTION_PATTERN = re.compile(r"<[@#!&]?[0-9]{5,}>")
_LONG_ID_PATTERN = re.compile(r"\b\d{15,22}\b")
_URL_PATTERN = re.compile(r"https?://\S+", re.I)
_DOMAIN_PATTERN = re.compile(r"https?://(?:www\.)?([^/\s?#]+)", re.I)
_WORD_PATTERN = re.compile(r"[a-z0-9]+")
_MUSIC_PATTERN = re.compile(r"\b(artist|track|song|playlist|radio|show|performance|beat|demo|finished tracks?|wips?|collab(?:oration)?s?|music)\b", re.I)
_COMMUNITY_PATTERN = re.compile(r"\b(community|server|barcode|member|public|conversation|channel|chat|finished tracks?|wips?)\b", re.I)
_BNL_PATTERN = re.compile(r"\b(bnl|bot|source files?|dossiers?|owner review|public-safe|asked|question|help|review)\b", re.I)
_TOPIC_PATTERNS = [
    ("BNL/source-file/dossier discussion", re.compile(r"\b(bnl|source files?|dossiers?|draft|candidate|owner review|public copy|public-safe|source packet)\b", re.I)),
    ("Music/track-sharing discussion", re.compile(r"\b(finished tracks?|share(?:d|ing)?\s+(?:a\s+)?(?:track|song|music)|new track|track|song|beat|music)\b", re.I)),
    ("WIP/demo discussion", re.compile(r"\b(wips?|work in progress|demo|rough mix|preview|snippet)\b", re.I)),
    ("Collaboration discussion", re.compile(r"\b(collab(?:oration)?s?|feature|work together|producer|vocalist|remix)\b", re.I)),
    ("BARCODE Radio/show discussion", re.compile(r"\b(barcode radio|radio|show|episode|broadcast|live set|performance|playlist)\b", re.I)),
    ("Help/support requests", re.compile(r"\b(help|support|assist|fix|issue|question|request|need|how do|can bnl|explain)\b|\?\s*$", re.I)),
    ("Community/server participation", re.compile(r"\b(community|server|barcode|member|public|conversation|channel|chat|general chat)\b", re.I)),
    ("Introductions/social links", re.compile(r"\b(intro(?:duce|duction)?|hello|hi everyone|socials?|linktree|instagram|soundcloud|spotify|youtube)\b", re.I)),
    ("Event/riddle/engagement activity", re.compile(r"\b(event|riddle|game|contest|challenge|engagement|poll|vote)\b", re.I)),
    ("Admin/operator review context", re.compile(r"\b(admin|operator|moderation|mod|staff|review-only|internal|private)\b", re.I)),
]

_SLASH_TOPIC_LABEL_REPLACEMENTS = {
    "bnl/source-file/dossier discussion": "BNL source-file and dossier classification",
    "bnl/source-file/dossier": "BNL source-file and dossier classification",
    "music/track-sharing discussion": "music and track-sharing classification",
    "music/track-sharing": "music and track-sharing classification",
    "wip/demo discussion": "WIP and demo classification",
    "wip/demo": "WIP and demo classification",
    "community/server participation": "community and server participation classification",
    "community/server": "community and server classification",
    "event/riddle/engagement activity": "event, riddle, or engagement classification",
    "event/riddle/engagement": "event, riddle, or engagement classification",
    "help/support requests": "help and support classification",
    "help/support": "help and support classification",
}
_CLASSIFICATION_SUFFIX_RE = re.compile(r"\s+(?:discussion|activity|requests?|context)\s*$", re.I)


def _join_topic_parts_for_payload(parts: list[str]) -> str:
    cleaned = [part.strip() for part in parts if part and part.strip()]
    if not cleaned:
        return "local context"
    if len(cleaned) == 1:
        return cleaned[0]
    if len(cleaned) == 2:
        return f"{cleaned[0]} and {cleaned[1]}"
    return f"{', '.join(cleaned[:-1])}, or {cleaned[-1]}"


def _website_safe_topic_label(value: Any) -> str:
    """Convert internal slash taxonomy into website-safe classification wording."""

    raw = re.sub(r"\s+", " ", str(value or "")).strip()
    if not raw:
        return "local context classification"
    lowered = raw.lower()
    if lowered in _SLASH_TOPIC_LABEL_REPLACEMENTS:
        return _SLASH_TOPIC_LABEL_REPLACEMENTS[lowered]
    if "/" not in raw:
        return raw
    first, *tail = raw.split(" ")
    taxonomy = first if "/" in first else raw
    suffix = " ".join(tail) if taxonomy == first else ""
    if taxonomy != first:
        taxonomy = _CLASSIFICATION_SUFFIX_RE.sub("", taxonomy)
    pieces = taxonomy.split("/")
    normalized_parts: list[str] = []
    for piece in pieces:
        cleaned = re.sub(r"[^A-Za-z0-9 -]+", " ", piece).strip()
        if cleaned.upper() in {"BNL", "WIP"}:
            cleaned = cleaned.upper()
        elif cleaned:
            cleaned = cleaned[:1].lower() + cleaned[1:]
        if cleaned:
            normalized_parts.append(cleaned)
    suffix = _CLASSIFICATION_SUFFIX_RE.sub("", suffix).strip()
    if suffix and suffix.lower() == "participation":
        normalized_parts.append("participation")
    label = _join_topic_parts_for_payload(normalized_parts)
    return f"{label} classification"


def _website_safe_payload_text(value: Any) -> str:
    """Sanitize known internal topic taxonomy embedded in normal website fields."""

    text = str(value or "")
    protected_slashes = {
        "Tool/platform": "Tool__BNL_SLASH__platform",
        "tool/platform": "tool__BNL_SLASH__platform",
        "queue/submission": "queue__BNL_SLASH__submission",
        "Queue/submission": "Queue__BNL_SLASH__submission",
    }
    for raw, placeholder in protected_slashes.items():
        text = text.replace(raw, placeholder)
    for raw_label in sorted(_SLASH_TOPIC_LABEL_REPLACEMENTS, key=len, reverse=True):
        safe_label = _SLASH_TOPIC_LABEL_REPLACEMENTS[raw_label]
        text = re.sub(re.escape(raw_label), safe_label, text, flags=re.I)
    text = text.replace("/", " and ")
    for raw, placeholder in protected_slashes.items():
        text = text.replace(placeholder, raw)
    return text


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_text(value: Any, max_length: int = MAX_REF_SNIPPET_LENGTH) -> str:
    cleaned = _URL_PATTERN.sub("[redacted-url]", str(value or ""))
    cleaned = _DISCORD_MENTION_PATTERN.sub("[redacted-mention]", cleaned)
    cleaned = _LONG_ID_PATTERN.sub("[redacted-id]", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    if len(cleaned) <= max_length:
        return cleaned
    return cleaned[: max(0, max_length - 1)].rstrip() + "…"


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


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone() is not None


def columns(conn: sqlite3.Connection, table: str) -> set[str]:
    if not table_exists(conn, table):
        return set()
    return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}


def ensure_entity_evidence_schema(conn: sqlite3.Connection) -> None:
    """Create the v1 structured entity/source evidence table and indexes."""

    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {ENTITY_EVIDENCE_TABLE} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            guild_id INTEGER,
            subject_key TEXT NOT NULL,
            subject_name TEXT NOT NULL,
            matched_user_id INTEGER,
            source_type TEXT NOT NULL,
            source_table TEXT NOT NULL,
            source_row_id TEXT,
            source_label TEXT NOT NULL,
            channel_name TEXT,
            channel_policy TEXT,
            visibility TEXT NOT NULL,
            authority TEXT NOT NULL,
            confidence REAL NOT NULL DEFAULT 0.5,
            relation_to_subject TEXT NOT NULL DEFAULT 'mentioned',
            topic TEXT,
            evidence_kind TEXT NOT NULL,
            safe_summary TEXT NOT NULL,
            public_safe_candidate INTEGER NOT NULL DEFAULT 0,
            review_only INTEGER NOT NULL DEFAULT 1,
            music_signal INTEGER NOT NULL DEFAULT 0,
            community_signal INTEGER NOT NULL DEFAULT 0,
            bnl_interaction INTEGER NOT NULL DEFAULT 0,
            dossier_relevance TEXT,
            raw_ref_json TEXT,
            created_at TEXT NOT NULL,
            observed_at TEXT,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        f"""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_entity_evidence_unique_source
        ON {ENTITY_EVIDENCE_TABLE} (
            guild_id,
            subject_key,
            source_type,
            source_table,
            coalesce(source_row_id, ''),
            evidence_kind,
            relation_to_subject
        )
        """
    )
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_entity_evidence_subject ON {ENTITY_EVIDENCE_TABLE} (guild_id, subject_key, updated_at)")
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_entity_evidence_kind ON {ENTITY_EVIDENCE_TABLE} (evidence_kind, review_only, public_safe_candidate)")


def _coerce_bool(value: Any) -> int:
    return 1 if bool(value) else 0


def _clean_safe_summary(summary: str) -> str:
    cleaned = safe_text(summary, MAX_SAFE_SUMMARY_LENGTH)
    # Evidence summaries are labels, not transcript excerpts; remove accidental IDs.
    return cleaned or "Structured entity evidence exists for owner review."


def _raw_ref_json(raw_ref: dict[str, Any] | None) -> str:
    safe_ref: dict[str, Any] = {}
    for key, value in (raw_ref or {}).items():
        if value is None:
            continue
        if key in {"snippet", "content", "summary", "raw_excerpt", "channel_name", "source_label"}:
            safe_ref[key] = safe_text(value)
        else:
            safe_ref[key] = value
    return json.dumps(safe_ref, sort_keys=True)


def upsert_entity_evidence_event(conn: sqlite3.Connection, **event: Any) -> str:
    """Insert/update one evidence event. Returns created, updated, or unchanged."""

    ensure_entity_evidence_schema(conn)
    subject = normalize_subject_name(event.get("subject_name") or event.get("subjectName") or "")
    key = event.get("subject_key") or subject_key(subject)
    evidence_kind = str(event.get("evidence_kind") or "").strip()
    if evidence_kind not in ALLOWED_EVIDENCE_KINDS:
        raise ValueError(f"Unsupported entity evidence kind: {evidence_kind}")
    source_type = str(event.get("source_type") or event.get("source_table") or "unknown")[:80]
    source_table = str(event.get("source_table") or source_type)[:80]
    source_row_id = None if event.get("source_row_id") in (None, "") else str(event.get("source_row_id"))[:80]
    relation = str(event.get("relation_to_subject") or "mentioned")[:80]
    guild_id = event.get("guild_id")
    now = now_iso()
    values = {
        "guild_id": guild_id,
        "subject_key": key,
        "subject_name": subject,
        "matched_user_id": event.get("matched_user_id"),
        "source_type": source_type,
        "source_table": source_table,
        "source_row_id": source_row_id,
        "source_label": str(event.get("source_label") or f"{source_table}/structured_evidence")[:120],
        "channel_name": event.get("channel_name") if event.get("channel_name") else None,
        "channel_policy": str(event.get("channel_policy") or "unknown")[:80],
        "visibility": str(event.get("visibility") or "review_only")[:80],
        "authority": str(event.get("authority") or "local_observed")[:80],
        "confidence": float(event.get("confidence") if event.get("confidence") is not None else 0.5),
        "relation_to_subject": relation,
        "topic": safe_text(event.get("topic") or "", 120) or None,
        "evidence_kind": evidence_kind,
        "safe_summary": _clean_safe_summary(str(event.get("safe_summary") or "")),
        "public_safe_candidate": _coerce_bool(event.get("public_safe_candidate")),
        "review_only": _coerce_bool(event.get("review_only", True)),
        "music_signal": _coerce_bool(event.get("music_signal")),
        "community_signal": _coerce_bool(event.get("community_signal")),
        "bnl_interaction": _coerce_bool(event.get("bnl_interaction")),
        "dossier_relevance": safe_text(event.get("dossier_relevance") or "", 120) or None,
        "raw_ref_json": _raw_ref_json(event.get("raw_ref_json") or {}),
        "observed_at": str(event.get("observed_at") or now)[:80],
        "updated_at": now,
    }
    existing = conn.execute(
        f"""
        SELECT * FROM {ENTITY_EVIDENCE_TABLE}
        WHERE (guild_id IS ? OR guild_id=?) AND subject_key=? AND source_type=? AND source_table=?
          AND coalesce(source_row_id, '')=coalesce(?, '') AND evidence_kind=? AND relation_to_subject=?
        LIMIT 1
        """,
        (guild_id, guild_id, key, source_type, source_table, source_row_id, evidence_kind, relation),
    ).fetchone()
    if existing is None:
        insert_values = dict(values)
        insert_values["created_at"] = now
        columns_sql = ", ".join(insert_values.keys())
        placeholders = ", ".join(["?"] * len(insert_values))
        conn.execute(f"INSERT INTO {ENTITY_EVIDENCE_TABLE} ({columns_sql}) VALUES ({placeholders})", list(insert_values.values()))
        _maybe_mark_source_refresh_dirty(conn, insert_values)
        return "created"
    current = dict(existing) if isinstance(existing, sqlite3.Row) else {desc[0]: existing[idx] for idx, desc in enumerate(conn.execute(f"SELECT * FROM {ENTITY_EVIDENCE_TABLE} LIMIT 0").description or [])}
    comparable = {k: v for k, v in values.items() if k != "updated_at"}
    if all(str(current.get(k) or "") == str(v or "") for k, v in comparable.items()):
        return "unchanged"
    assignments = ", ".join(f"{k}=?" for k in values.keys())
    conn.execute(f"UPDATE {ENTITY_EVIDENCE_TABLE} SET {assignments} WHERE id=?", [*values.values(), current["id"]])
    _maybe_mark_source_refresh_dirty(conn, values)
    return "updated"



def _maybe_mark_source_refresh_dirty(conn: sqlite3.Connection, values: dict[str, Any]) -> None:
    """Best-effort bridge from durable entity evidence to Source File freshness.

    This deliberately uses only scoped evidence metadata and safe summaries; no raw
    transcripts are logged or sent by the dirty marker. It writes through the
    existing connection so evidence creation and dirty marking stay atomic.
    """

    try:
        from bnl_source_file_refresh import (
            QUEUE_TABLE,
            classify_refresh_evidence,
            ensure_source_file_refresh_schema,
            source_refresh_subject_key,
            utc_now,
        )
        from bnl_source_refresh_context import is_refresh_generation_subject

        subject = str(values.get("subject_name") or "").strip()
        skey = source_refresh_subject_key(subject)
        if is_refresh_generation_subject(skey):
            logging.info("source_refresh_skipped subject_key=%s reason=refresh_self_generated_evidence", skey)
            return
        decision = classify_refresh_evidence(
            subject_name=subject,
            evidence_source="entity_evidence_events",
            content=str(values.get("safe_summary") or values.get("topic") or ""),
            channel_policy=str(values.get("channel_policy") or "unknown"),
            source_scope="subject_keyed",
            authority=str(values.get("authority") or "local_observed"),
            visibility=str(values.get("visibility") or "review_only"),
            evidence_type=str(values.get("evidence_kind") or "structured_evidence"),
            relation_to_subject=str(values.get("relation_to_subject") or "subject_keyed"),
        )
        if not decision.get("meaningful"):
            logging.info("source_refresh_skipped subject_key=%s reason=%s", skey, decision.get("reason"))
            return
        ensure_source_file_refresh_schema(conn)
        now = utc_now()
        existing = conn.execute(
            f"SELECT * FROM {QUEUE_TABLE} WHERE (guild_id IS ? OR guild_id=?) AND subject_key=? AND status IN ('queued','deferred','cooldown','failed','running') ORDER BY id DESC LIMIT 1",
            (values.get("guild_id"), values.get("guild_id"), skey),
        ).fetchone()
        reason = str(decision.get("reason") or "entity_evidence")[:260]
        priority = int(decision.get("priority") or 80)
        logging.info("source_refresh_mark_dirty subject_key=%s reason=%s", skey, reason)
        if existing:
            row_id = existing["id"] if isinstance(existing, sqlite3.Row) else existing[0]
            evidence_count = int((existing["evidence_count"] if isinstance(existing, sqlite3.Row) else existing[6]) or 0) + 1
            old_reason = (existing["reason"] if isinstance(existing, sqlite3.Row) else existing[4]) or ""
            conn.execute(
                f"UPDATE {QUEUE_TABLE} SET reason=?, evidence_source='entity_evidence_events', evidence_count=?, priority=max(priority, ?), status='queued', updated_at=?, refresh_mode='automatic', created_by='entity_evidence_events' WHERE id=?",
                ((old_reason + '; ' + reason)[:260], evidence_count, priority, now, row_id),
            )
        else:
            conn.execute(
                f"INSERT INTO {QUEUE_TABLE} (guild_id, subject_key, subject_name, reason, evidence_source, evidence_count, priority, status, queued_at, updated_at, attempts, refresh_mode, created_by) VALUES (?, ?, ?, ?, 'entity_evidence_events', 1, ?, 'queued', ?, ?, 0, 'automatic', 'entity_evidence_events')",
                (values.get("guild_id"), skey, subject[:90], reason, priority, now, now),
            )
        logging.info("source_refresh_queued subject_key=%s priority=%s", skey, priority)
    except Exception as exc:
        logging.debug("source_refresh_dirty_hook_failed source=entity_evidence_events error=%s", exc)

def _fetch_rows(conn: sqlite3.Connection, table: str, guild_id: int | None, limit: int) -> list[sqlite3.Row]:
    cols = columns(conn, table)
    if not cols:
        return []
    where = ""
    params: list[Any] = []
    if guild_id is not None and "guild_id" in cols:
        where = " WHERE guild_id=?"
        params.append(guild_id)
    order = ""
    for candidate in ("updated_at", "timestamp", "last_seen_at", "created_at", "id"):
        if candidate in cols:
            order = f" ORDER BY {candidate} DESC"
            break
    return list(conn.execute(f"SELECT * FROM {table}{where}{order} LIMIT ?", [*params, max(1, limit)]))


def _row_text(row: sqlite3.Row, fields: list[str]) -> str:
    data = dict(row)
    return " ".join(str(data.get(field) or "") for field in fields if field in data)


def _topic(text: str) -> str:
    labels = [label for label, pattern in _TOPIC_PATTERNS if pattern.search(text or "")]
    return labels[0] if labels else "local context"


def _conversation_match_labels(subject_name: str, subject_key_value: str = "", confirmed_aliases: list[Any] | None = None) -> set[str]:
    """Return compact labels that may identify the subject as conversation author."""

    labels: set[str] = set()

    def add(value: Any) -> None:
        if value in (None, ""):
            return
        raw = str(value).strip()
        if not raw:
            return
        labels.add(compact_subject_text(raw))
        labels.add(compact_subject_text(normalize_subject_name(raw)))
        labels.add(compact_subject_text(raw.replace("-", " ").replace("_", " ")))

    add(subject_name)
    add(subject_key_value)
    for alias in confirmed_aliases or []:
        if isinstance(alias, dict):
            use = alias.get("useForMatching")
            if use is False or str(use).lower() == "false":
                continue
            add(alias.get("name") or alias.get("alias") or alias.get("label") or alias.get("value"))
        else:
            add(alias)
    return {label for label in labels if len(label) >= 3}


def _conversation_author_values(data: dict[str, Any]) -> list[str]:
    values: list[str] = []
    for key in ("user_name", "author_name", "display_name", "preferred_name", "username", "global_name"):
        value = data.get(key)
        if value not in (None, ""):
            values.append(str(value))
    return values


def _authored_conversation_music_signal(text: str) -> bool:
    lowered = str(text or "").lower()
    if re.search(r"\b(?:no|not|without|lacks?)\b.{0,40}\b(?:artist|submission|music|track|song|proof|claim)\b", lowered):
        return bool(_DOMAIN_PATTERN.search(text or ""))
    return bool(_MUSIC_PATTERN.search(text or "") or _DOMAIN_PATTERN.search(text or ""))


def _is_bnl_conversation_author(data: dict[str, Any]) -> bool:
    role = str(data.get("role") or "").strip().lower()
    if role in {"assistant", "model", "bot", "system"}:
        return True
    for value in _conversation_author_values(data):
        compact = compact_subject_text(value)
        if compact in {"bnl", "bnl01", "bnlbot", "barcodebot", "barcodebnl", "bnl01bot"}:
            return True
    return False


def _conversation_author_matches_subject(data: dict[str, Any], match_labels: set[str], matched_user_ids: set[Any] | None = None) -> bool:
    for key in ("user_id", "author_id", "discord_user_id", "member_id"):
        if data.get(key) in (matched_user_ids or set()):
            return True
        if str(data.get(key) or "") in {str(v) for v in (matched_user_ids or set()) if v not in (None, "")}:
            return True
    for value in _conversation_author_values(data):
        compact = compact_subject_text(value)
        if compact and compact in match_labels:
            return True
    return False


def extract_authored_conversation_topic(text: str) -> str:
    """Extract a concrete website-safe topic from a subject-authored public row."""

    raw = str(text or "")
    bits: list[str] = []
    domains = []
    for match in _DOMAIN_PATTERN.finditer(raw):
        domain = match.group(1).lower().removeprefix("www.")
        if domain and domain not in domains:
            domains.append(domain)
    platform_map = {
        "youtube.com": "YouTube link",
        "youtu.be": "YouTube link",
        "soundcloud.com": "SoundCloud link",
        "spotify.com": "Spotify link",
        "suno.com": "Suno link",
        "bandcamp.com": "Bandcamp link",
    }
    for domain in domains[:2]:
        label = next((name for suffix, name in platform_map.items() if domain == suffix or domain.endswith("." + suffix)), None)
        bits.append(label or f"{domain} link")
    lowered = raw.lower()
    negated_music = bool(re.search(r"\b(?:no|not|without|lacks?)\b.{0,40}\b(?:artist|submission|music|track|song|proof|claim)\b", lowered))
    if not negated_music and re.search(r"\b(?:track|song|music|artist|cream|playlist|album|video)\b", lowered):
        bits.append("music/video reference")
    if re.search(r"\b(?:wip|demo|rough mix|preview|finished track|finished tracks|snippet)\b", lowered):
        bits.append("WIP/demo or finished-track reference")
    if re.search(r"\b(?:bnl|bnl-01|bot)\b", lowered) and "?" in raw:
        bits.append("BNL-facing question")
    elif re.search(r"\b(?:bnl|bnl-01|bot)\b", lowered):
        bits.append("BNL-facing exchange")
    weird_matches = []
    for phrase in ("vibrating beds", "electric sheep"):
        if phrase in lowered:
            weird_matches.append(phrase)
    if weird_matches:
        bits.append("weird concrete subject: " + ", ".join(weird_matches[:2]))
    if re.search(r"\b(?:orion|cliff|network|barcode|lore|archive|edge)\b", lowered):
        found = []
        for term in ("Orion", "Cliff", "Network", "BARCODE", "lore", "archive", "EDGE"):
            if re.search(rf"\b{re.escape(term)}\b", raw, flags=re.I):
                found.append(term)
        if found:
            bits.append("show/lore terms: " + ", ".join(found[:3]))
    if "?" in raw or re.search(r"\b(?:how do|how to|can you|could you|help|support|fix|bring back)\b", lowered):
        bits.append("question or support request")
    # Preserve one distinctive noun phrase when available, without exposing URLs/IDs.
    clean = safe_text(raw, 120)
    candidates = re.findall(r"\b[A-Z][A-Za-z0-9_-]{2,}(?:\s+[A-Z][A-Za-z0-9_-]{2,}){0,2}\b", clean)
    for candidate in candidates:
        if candidate.lower() not in {"http", "https", "bnl"} and candidate not in bits:
            bits.append(candidate)
            break
    if not bits:
        details = extract_conversation_topic_details(raw, review_only=False)
        bits.extend(details[:1])
    seen: set[str] = set()
    out: list[str] = []
    for bit in bits:
        safe = _website_safe_payload_text(safe_text(bit, 80))
        key = safe.lower()
        if safe and key not in seen:
            seen.add(key)
            out.append(safe)
    return "; ".join(out[:4]) or "Community/server participation"


def build_authored_conversation_safe_summary(*, text: str, channel_name: Any = None, channel_policy: str = "unknown") -> str:
    """Build a concrete paraphrase for subject-authored public conversation evidence."""

    topic = extract_authored_conversation_topic(text)
    channel = _safe_channel_label(channel_name, channel_policy)
    lowered = str(text or "").lower()
    if "youtube" in topic.lower() or "youtu.be" in lowered:
        action = "shared a YouTube link"
    elif " link" in topic.lower():
        action = "shared a public link"
    elif "bnl-facing question" in topic.lower() or "?" in str(text or ""):
        action = "asked a public question"
    elif "wip" in topic.lower() or "demo" in topic.lower() or "finished-track" in topic.lower():
        action = "discussed a WIP, demo, or finished-track reference"
    else:
        action = "authored public conversation context"
    return _clean_safe_summary(f"Subject {action} about {topic} in {channel}.")



def extract_conversation_topic_details(text: str, *, review_only: bool = False) -> list[str]:
    """Return specific, review-safe conversation detail labels without raw quotes."""

    labels: list[str] = []
    for label, pattern in _TOPIC_PATTERNS:
        if pattern.search(text or ""):
            if label == "Admin/operator review context" and not review_only:
                continue
            if label not in labels:
                labels.append(label)
    if not labels:
        labels.append("Community/server participation" if _COMMUNITY_PATTERN.search(text or "") else "Local context")
    return labels[:4]


def _safe_channel_label(channel_name: Any, policy: str) -> str:
    if policy not in PUBLIC_CONVERSATION_POLICIES:
        return "approved public-side channel"
    name = re.sub(r"[^A-Za-z0-9 _-]+", "", str(channel_name or "")).strip().replace(" ", "-").lower()
    return f"#{name[:60]}" if name else "approved public-side channel"


def build_conversation_safe_summary(*, text: str, authored: bool, public_safe: bool, channel_name: Any = None, channel_policy: str = "unknown") -> str:
    """Build a short paraphrase for conversation evidence; never include raw text."""

    relation = "Subject authored" if authored else "Subject was mentioned in"
    if not public_safe:
        role = "authored" if authored else "was mentioned in"
        return f"Subject {role} review-only conversation context; private/internal channel details require owner review."
    labels = extract_conversation_topic_details(text, review_only=False)
    primary = _website_safe_topic_label(labels[0]).removesuffix(" classification")
    if primary == "Help/support requests":
        action = "help/support request"
    elif primary == "Community/server participation":
        action = "community discussion"
    else:
        action = f"discussion about {primary}"
    channel = _safe_channel_label(channel_name, channel_policy)
    return f"{relation} approved public-side {action} in {channel}."


def extract_entity_activity_details(row: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
    data = dict(row) if not isinstance(row, dict) else dict(row)
    public_candidate = bool(data.get("public_safe_candidate")) and not bool(data.get("review_only"))
    policy = str(data.get("channel_policy") or "unknown").lower()
    relation = str(data.get("relation_to_subject") or "mentioned")
    topic = str(data.get("topic") or "Local context")
    channel = _safe_channel_label(data.get("channel_name"), policy) if public_candidate else "review-only channel"
    summary = safe_text(data.get("safe_summary") or "Structured entity evidence exists for owner review.", MAX_SAFE_SUMMARY_LENGTH)
    return {
        "topic": topic,
        "relation": relation,
        "publicSafe": public_candidate,
        "reviewOnly": bool(data.get("review_only")),
        "channel": channel,
        "safeSummary": summary,
        "observedAt": data.get("observed_at") or data.get("updated_at"),
        "evidenceKind": data.get("evidence_kind"),
    }


def _representative_evidence_line(detail: dict[str, Any]) -> str:
    if not detail.get("publicSafe"):
        return "Review-only evidence exists; private/internal details require owner review."
    channel = _website_safe_payload_text(detail.get("channel") or "approved public-side channel")
    topic = _website_safe_topic_label(detail.get("topic") or "Community/server participation")
    topic_context = topic.removesuffix(" classification")
    if topic_context.lower().startswith("bnl source-file and dossier"):
        return f"{channel}: BNL classified this approved public-context item under {topic_context} context; review before treating it as a subject claim."
    return f"{channel}: approved public-context item classified under {topic_context}; review before using as a subject claim."


def group_entity_evidence_details(rows: list[sqlite3.Row | dict[str, Any]]) -> dict[str, Any]:
    """Aggregate structured evidence into website-friendly counts and paraphrases."""

    details = [extract_entity_activity_details(row) for row in rows]
    public_details = [item for item in details if item["publicSafe"]]
    topic_counts = Counter(item["topic"] for item in public_details)
    channel_counts = Counter(item["channel"] for item in public_details if str(item.get("channel") or "").startswith("#"))
    topic_channel_counts: dict[str, Counter] = {}
    topic_relation_counts: dict[str, Counter] = {}
    for item in public_details:
        topic_channel_counts.setdefault(item["topic"], Counter())[item["channel"]] += 1
        topic_relation_counts.setdefault(item["topic"], Counter())[item["relation"]] += 1
    authored_count = sum(1 for item in public_details if item["relation"] == "authored")
    mentioned_count = sum(1 for item in public_details if item["relation"] != "authored")
    review_only_count = sum(1 for item in details if item["reviewOnly"] or not item["publicSafe"])
    observed_values = [str(item.get("observedAt") or "") for item in details if item.get("observedAt")]
    most_recent = max(observed_values) if observed_values else ""

    topic_breakdown: list[str] = []
    for topic, count in topic_counts.most_common(8):
        relations = topic_relation_counts.get(topic, Counter())
        rel_bits = []
        if relations.get("authored"):
            rel_bits.append(f"{relations['authored']} public-side authored item(s)")
        mentioned = count - relations.get("authored", 0)
        if mentioned:
            rel_bits.append(f"{mentioned} public-side mentioned item(s)")
        safe_topic = _website_safe_topic_label(topic)
        line = f"{safe_topic}: {', '.join(rel_bits) or f'{count} public-side item(s)'}"
        channel, channel_count = (topic_channel_counts.get(topic, Counter()).most_common(1) or [("", 0)])[0]
        if channel.startswith("#") and channel_count >= max(1, count // 2):
            line += f", mostly in {channel}"
        topic_breakdown.append(line + ".")

    representative: list[str] = []
    seen_topics: set[str] = set()
    for item in public_details:
        if item["topic"] in seen_topics:
            continue
        representative.append(_representative_evidence_line(item))
        seen_topics.add(item["topic"])
        if len(representative) >= 5:
            break
    if len(public_details) >= 3 and len(channel_counts) >= 2:
        representative.append("Community activity appears repeated across approved public-side channels.")

    return {
        "activityFrequencySummary": {
            "approvedPublicAuthoredRows": authored_count,
            "approvedPublicMentionedRows": mentioned_count,
            "reviewOnlyEvidenceCount": review_only_count,
            "mostRecentObservedAt": most_recent,
        },
        "authoredVsMentionedSummary": f"{authored_count} approved public-side authored item(s); {mentioned_count} approved public-side mentioned item(s).",
        "recentActivitySummary": f"Most recent observed evidence timestamp: {most_recent}." if most_recent else "No observed timestamp available in structured evidence.",
        "topChannels": [{"channel": channel, "count": count} for channel, count in channel_counts.most_common(5)],
        "topTopicDetails": [{"topic": _website_safe_topic_label(topic), "count": count} for topic, count in topic_counts.most_common(8)],
        "topicBreakdown": topic_breakdown,
        "representativeEvidence": representative[:6],
    }

def _source_row_id(data: dict[str, Any]) -> str | None:
    value = data.get("id")
    return None if value in (None, "") else str(value)


def backfill_subject_authored_conversation_evidence(
    conn,
    *,
    guild_id: int,
    subject_name: str,
    subject_key: str,
    confirmed_aliases: list[str] | None = None,
    limit: int = 75,
) -> dict[str, Any]:
    """Backfill subject-authored public conversation rows into entity evidence events.

    This only accepts rows authored by the subject in approved public-side policies.
    It does not infer ownership from content mentions or from model/BNL rows.
    """

    ensure_entity_evidence_schema(conn)
    diagnostics: dict[str, Any] = {
        "attempted": True,
        "matchedRows": 0,
        "createdEvents": 0,
        "updatedEvents": 0,
        "skippedRows": 0,
        "skippedReasons": [],
    }

    def skip(reason: str) -> None:
        diagnostics["skippedRows"] += 1
        if reason not in diagnostics["skippedReasons"]:
            diagnostics["skippedReasons"].append(reason)

    if not table_exists(conn, "conversations"):
        skip("conversations_table_missing")
        return diagnostics
    cols = columns(conn, "conversations")
    required = {"role", "guild_id", "channel_policy"}
    if not required.issubset(cols):
        skip("conversations_required_columns_missing")
        return diagnostics

    subject = normalize_subject_name(subject_name)
    skey = subject_key or globals()["subject_key"](subject)
    match_labels = _conversation_match_labels(subject, skey, confirmed_aliases)
    if not match_labels:
        skip("missing_subject_match_labels")
        return diagnostics

    select_cols = ["rowid"] + sorted(cols)
    order_col = "timestamp" if "timestamp" in cols else "created_at" if "created_at" in cols else "id" if "id" in cols else "rowid"
    rows = conn.execute(
        f"SELECT {', '.join('rowid AS _rowid' if c == 'rowid' else c for c in select_cols)} FROM conversations WHERE guild_id=? ORDER BY {order_col} DESC LIMIT ?",
        (guild_id, max(1, limit)),
    ).fetchall()
    for row in rows:
        data = dict(row)
        role = str(data.get("role") or "").strip().lower()
        if role != "user":
            skip("non_user_role")
            continue
        if _is_bnl_conversation_author(data):
            skip("bnl_or_model_author")
            continue
        policy = str(data.get("channel_policy") or "unknown").strip().lower() or "unknown"
        if policy not in PUBLIC_CONVERSATION_POLICIES:
            skip("non_public_channel_policy")
            continue
        if not _conversation_author_matches_subject(data, match_labels):
            skip("author_identity_not_subject")
            continue
        text = _row_text(row, ["content"])
        if not text.strip():
            skip("empty_content")
            continue
        diagnostics["matchedRows"] += 1
        source_id = _source_row_id(data) or str(data.get("_rowid") or "")
        topic = extract_authored_conversation_topic(text)
        safe_summary = build_authored_conversation_safe_summary(text=text, channel_name=data.get("channel_name"), channel_policy=policy)
        status = upsert_entity_evidence_event(
            conn,
            guild_id=data.get("guild_id", guild_id),
            subject_key=skey,
            subject_name=subject,
            matched_user_id=data.get("user_id") or data.get("author_id") or data.get("discord_user_id") or data.get("member_id"),
            source_type="conversation",
            source_table="conversations",
            source_row_id=source_id,
            source_label="conversations/public_discord_observed",
            channel_name=safe_text(data.get("channel_name") or "", 80) or None,
            channel_policy=policy,
            visibility="public_side",
            authority="public_discord_observed",
            confidence=0.72,
            relation_to_subject="authored",
            topic=topic,
            evidence_kind="authored_public_conversation",
            safe_summary=safe_summary,
            public_safe_candidate=True,
            review_only=False,
            music_signal=_authored_conversation_music_signal(text),
            community_signal=True,
            bnl_interaction=bool(_BNL_PATTERN.search(text)),
            dossier_relevance="candidate_after_owner_review",
            raw_ref_json={
                "table": "conversations",
                "row_id": source_id,
                "safe_snippet": safe_text(text),
                "channel_name": data.get("channel_name"),
                "channel_policy": policy,
            },
            observed_at=data.get("timestamp") or data.get("created_at"),
        )
        if status == "created":
            diagnostics["createdEvents"] += 1
        elif status == "updated":
            diagnostics["updatedEvents"] += 1
    return diagnostics


def derive_entity_evidence_from_conversation_row(
    conn: sqlite3.Connection,
    row: sqlite3.Row,
    subject_name: str,
    *,
    guild_id: int | None = None,
    matched_user_ids: set[Any] | None = None,
    aliases: list[str] | None = None,
) -> str | None:
    """Derive one conservative conversation evidence event for a matched row."""

    data = dict(row)
    subject = normalize_subject_name(subject_name)
    text = _row_text(row, ["content"])
    haystack = _row_text(row, ["user_name", "content", "channel_name", "channel_policy"])
    authored = data.get("user_id") in (matched_user_ids or set()) or contains_subject_mention(str(data.get("user_name") or ""), subject, aliases)
    if not authored and not contains_subject_mention(haystack, subject, aliases):
        return None
    policy = str(data.get("channel_policy") or "unknown").strip().lower() or "unknown"
    public = policy in PUBLIC_CONVERSATION_POLICIES
    relation = "authored" if authored else "mentioned"
    kind = f"{relation}_{'public' if public else 'review_only'}_conversation"
    if public:
        safe_summary = build_authored_conversation_safe_summary(text=text, channel_name=data.get("channel_name"), channel_policy=policy) if authored else build_conversation_safe_summary(text=text, authored=authored, public_safe=True, channel_name=data.get("channel_name"), channel_policy=policy)
        channel_name = safe_text(data.get("channel_name") or "", 80) or None
    else:
        safe_summary = f"Subject {relation} review-only conversation context; private/internal channel details require owner review."
        channel_name = None
    return upsert_entity_evidence_event(
        conn,
        guild_id=guild_id if guild_id is not None else data.get("guild_id"),
        subject_name=subject,
        matched_user_id=data.get("user_id") if authored else None,
        source_type="conversation",
        source_table="conversations",
        source_row_id=_source_row_id(data),
        source_label="conversations/public_discord_observed" if public else "conversations/internal_context_review_only",
        channel_name=channel_name,
        channel_policy=policy,
        visibility="public_side" if public else "review_only",
        authority="public_discord_observed" if public and authored else ("channel_policy_observed" if public else "internal_review_only"),
        confidence=0.72 if public else 0.5,
        relation_to_subject=relation,
        topic=extract_authored_conversation_topic(text) if authored and public else (extract_conversation_topic_details(text, review_only=not public) or [_topic(text)])[0],
        evidence_kind=kind,
        safe_summary=safe_summary,
        public_safe_candidate=public,
        review_only=not public,
        music_signal=_authored_conversation_music_signal(text) if authored and public else bool(_MUSIC_PATTERN.search(text)),
        community_signal=bool(_COMMUNITY_PATTERN.search(text)),
        bnl_interaction=bool(_BNL_PATTERN.search(text)),
        dossier_relevance="candidate_after_owner_review" if public else "review_only_context",
        raw_ref_json={"table": "conversations", "row_id": data.get("id"), "channel_policy": policy, "channel_name": data.get("channel_name"), "snippet": text},
        observed_at=data.get("timestamp"),
    )


def derive_entity_evidence_for_subject(db_path: str, subject_name: str, guild_id: int | None = None, limit: int = 200, confirmed_aliases: list[str] | None = None) -> dict[str, Any]:
    """Derive/update evidence events for one subject from existing local stores only."""

    subject = normalize_subject_name(subject_name)
    key = subject_key(subject)
    result = {"subjectName": subject, "subjectKey": key, "createdCount": 0, "updatedCount": 0, "unchangedCount": 0, "sourceTypes": Counter(), "reviewOnlyCount": 0, "publicSafeCandidateCount": 0, "status": "ok", "conversationBackfill": {"attempted": False, "matchedRows": 0, "createdEvents": 0, "updatedEvents": 0, "skippedRows": 0, "skippedReasons": []}}
    if not subject:
        result["status"] = "missing_subject"
        result["sourceTypes"] = {}
        return result
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        ensure_entity_evidence_schema(conn)
        matched_user_ids: set[Any] = set()
        aliases: list[str] = []

        def note(status: str | None, source_type: str, review_only: bool, public_candidate: bool) -> None:
            if not status:
                return
            result[f"{status}Count"] += 1
            result["sourceTypes"][source_type] += 1
            if review_only:
                result["reviewOnlyCount"] += 1
            if public_candidate:
                result["publicSafeCandidateCount"] += 1

        for row in _fetch_rows(conn, "user_profiles", guild_id, limit):
            data = dict(row)
            names = [data.get("display_name"), data.get("preferred_name")]
            if not any(contains_subject_mention(str(name or ""), subject) or contains_subject_mention(subject, str(name or "")) for name in names if name):
                continue
            for name in names:
                clean = normalize_subject_name(str(name or ""))
                if clean:
                    aliases.append(clean)
            matched_user_ids.add(data.get("user_id"))
            status = upsert_entity_evidence_event(
                conn,
                guild_id=data.get("guild_id", guild_id),
                subject_name=subject,
                matched_user_id=data.get("user_id"),
                source_type="profile",
                source_table="user_profiles",
                source_row_id=str(data.get("user_id") or ""),
                source_label="user_profiles/local_profile_observed",
                channel_policy="none",
                visibility="internal_identity_match",
                authority="local_profile_label",
                confidence=0.8,
                relation_to_subject="profile_match",
                topic="identity matching context",
                evidence_kind="profile_match",
                safe_summary="Local profile label matches this subject for internal identity review.",
                public_safe_candidate=False,
                review_only=True,
                dossier_relevance="identity_match_review",
                raw_ref_json={"table": "user_profiles", "user_id": data.get("user_id"), "display_name": data.get("display_name"), "preferred_name": data.get("preferred_name")},
                observed_at=data.get("last_seen") or data.get("last_greeting_at"),
            )
            note(status, "profile", True, False)

        aliases = list(dict.fromkeys([a for a in [*(confirmed_aliases or []), *aliases] if a and str(a).lower() != subject.lower()]))

        if guild_id is not None:
            backfill = backfill_subject_authored_conversation_evidence(
                conn,
                guild_id=int(guild_id),
                subject_name=subject,
                subject_key=key,
                confirmed_aliases=aliases,
                limit=min(max(1, limit), 75),
            )
            result["conversationBackfill"] = backfill
            for status_name, count_key in (("created", "createdEvents"), ("updated", "updatedEvents")):
                count = int(backfill.get(count_key) or 0)
                if count:
                    result[f"{status_name}Count"] += count
                    result["sourceTypes"]["conversation"] += count
                    result["publicSafeCandidateCount"] += count

        def row_matches(row: sqlite3.Row, fields: list[str]) -> bool:
            data = dict(row)
            if data.get("user_id") in matched_user_ids:
                return True
            return contains_subject_mention(_row_text(row, fields), subject, aliases=aliases)

        for row in _fetch_rows(conn, "conversations", guild_id, limit):
            if not row_matches(row, ["user_name", "content", "channel_name", "channel_policy"]):
                continue
            data = dict(row)
            status = derive_entity_evidence_from_conversation_row(conn, row, subject, guild_id=guild_id, matched_user_ids=matched_user_ids, aliases=aliases)
            policy = str(data.get("channel_policy") or "unknown").strip().lower() or "unknown"
            note(status, "conversation", policy not in PUBLIC_CONVERSATION_POLICIES, policy in PUBLIC_CONVERSATION_POLICIES)

        for table in ("relationship_state", "relationship_journal"):
            for row in _fetch_rows(conn, table, guild_id, limit):
                if not row_matches(row, ["last_topic", "trust_stage", "social_stance", "summary", "entry_type"]):
                    continue
                data = dict(row)
                text = _row_text(row, ["last_topic", "summary", "entry_type"])
                status = upsert_entity_evidence_event(
                    conn,
                    guild_id=data.get("guild_id", guild_id),
                    subject_name=subject,
                    matched_user_id=data.get("user_id"),
                    source_type="relationship_context",
                    source_table=table,
                    source_row_id=_source_row_id(data) or str(data.get("user_id") or ""),
                    source_label=f"{table}/local_relationship_trace",
                    channel_policy="source_blind_private",
                    visibility="review_only",
                    authority="private_relationship_context",
                    confidence=0.45,
                    relation_to_subject="relationship_context",
                    topic=_topic(text),
                    evidence_kind="relationship_context_review_only",
                    safe_summary="Relationship/context notes exist but are private review-only.",
                    public_safe_candidate=False,
                    review_only=True,
                    dossier_relevance="review_only_context",
                    raw_ref_json={"table": table, "row_id": data.get("id"), "user_id": data.get("user_id"), "snippet": text},
                    observed_at=data.get("updated_at") or data.get("timestamp"),
                )
                note(status, "relationship_context", True, False)

        for row in _fetch_rows(conn, "memory_tiers", guild_id, limit):
            if not row_matches(row, ["summary", "tier"]):
                continue
            data = dict(row)
            text = _row_text(row, ["summary", "tier"])
            status = upsert_entity_evidence_event(
                conn,
                guild_id=data.get("guild_id", guild_id),
                subject_name=subject,
                matched_user_id=data.get("user_id"),
                source_type="source_blind_memory",
                source_table="memory_tiers",
                source_row_id=_source_row_id(data),
                source_label="memory_tiers/source_blind_memory_trace",
                channel_policy=str(data.get("source_channel_policy") or "source_blind"),
                visibility="review_only",
                authority="source_blind_legacy_memory",
                confidence=0.35,
                relation_to_subject="source_blind_memory",
                topic=_topic(text),
                evidence_kind="source_blind_memory_review_only",
                safe_summary="Source-blind legacy memory exists and requires review.",
                public_safe_candidate=False,
                review_only=True,
                music_signal=bool(_MUSIC_PATTERN.search(text)),
                community_signal=bool(_COMMUNITY_PATTERN.search(text)),
                dossier_relevance="requires_source_review",
                raw_ref_json={"table": "memory_tiers", "row_id": data.get("id"), "snippet": text},
                observed_at=data.get("updated_at"),
            )
            note(status, "source_blind_memory", True, False)

        for row in _fetch_rows(conn, "broadcast_memory", guild_id, limit):
            if not row_matches(row, ["cleaned_summary", "summary", "raw_note", "entry_type"]):
                continue
            data = dict(row)
            text = _row_text(row, ["cleaned_summary", "summary", "raw_note", "entry_type"])
            public_candidate = bool(data.get("public_safe")) and str(data.get("status") or "active") == "active"
            status = upsert_entity_evidence_event(
                conn,
                guild_id=data.get("guild_id", guild_id),
                subject_name=subject,
                source_type="broadcast_memory",
                source_table="broadcast_memory",
                source_row_id=_source_row_id(data),
                source_label="broadcast_memory/broadcast_memory_trace",
                channel_policy="broadcast_memory",
                visibility="public_safe_candidate" if public_candidate else "review_only",
                authority="broadcast_memory_public_safe" if public_candidate else "broadcast_memory_review_only",
                confidence=0.68 if public_candidate else 0.45,
                relation_to_subject="broadcast_memory_context",
                topic=_topic(text),
                evidence_kind="broadcast_memory_signal",
                safe_summary="Broadcast-memory context may support public wording after owner review." if public_candidate else "Broadcast-memory context exists but is review-only unless active and public-safe.",
                public_safe_candidate=public_candidate,
                review_only=not public_candidate,
                music_signal=bool(_MUSIC_PATTERN.search(text)),
                community_signal=True,
                dossier_relevance="candidate_after_owner_review" if public_candidate else "review_only_context",
                raw_ref_json={"table": "broadcast_memory", "row_id": data.get("id"), "status": data.get("status"), "public_safe": data.get("public_safe"), "snippet": text},
                observed_at=data.get("episode_date"),
            )
            note(status, "broadcast_memory", not public_candidate, public_candidate)

        for row in _fetch_rows(conn, "community_presence", guild_id, limit):
            if not row_matches(row, ["subject_key", "display_name", "connection_notes", "evidence_snippets"]):
                continue
            data = dict(row)
            text = _row_text(row, ["display_name", "connection_notes", "evidence_snippets"])
            status = upsert_entity_evidence_event(
                conn,
                guild_id=data.get("guild_id", guild_id),
                subject_name=subject,
                source_type="community_presence",
                source_table="community_presence",
                source_row_id=str(data.get("subject_key") or key),
                source_label="community_presence/community_presence_trace",
                channel_policy="community_presence_approved_sources",
                visibility="review_only",
                authority="community_presence_trace",
                confidence=0.62,
                relation_to_subject="community_presence",
                topic=_topic(text),
                evidence_kind="community_presence_signal",
                safe_summary="Community presence record exists and can inform public wording only after owner review.",
                public_safe_candidate=False,
                review_only=True,
                community_signal=True,
                dossier_relevance="candidate_after_owner_review",
                raw_ref_json={"table": "community_presence", "subject_key": data.get("subject_key"), "mention_count": data.get("mention_count"), "direct_interaction_count": data.get("direct_interaction_count"), "snippet": text},
                observed_at=data.get("last_seen_at"),
            )
            note(status, "community_presence", True, False)

        conn.commit()
    finally:
        conn.close()
    result["sourceTypes"] = dict(result["sourceTypes"])
    return result


def _row_value(row: sqlite3.Row | dict[str, Any], key: str, default: Any = None) -> Any:
    data = dict(row) if not isinstance(row, dict) else row
    return data.get(key, default)


def evidence_event_priority(row: sqlite3.Row | dict[str, Any]) -> int:
    """Return a usefulness priority for entity evidence rows; lower is stronger."""

    kind = str(_row_value(row, "evidence_kind", "") or "")
    source_type = str(_row_value(row, "source_type", "") or "")
    public_candidate = bool(_row_value(row, "public_safe_candidate", False))
    if kind == "profile_match":
        return 10
    if kind == "authored_public_conversation":
        return 20
    if kind == "mentioned_public_conversation":
        return 30
    if kind == "broadcast_memory_signal" and public_candidate:
        return 40
    if kind == "music_or_show_signal" or bool(_row_value(row, "music_signal", False)):
        return 50
    if bool(_row_value(row, "bnl_interaction", False)):
        return 60
    if kind == "community_presence_signal":
        return 70
    if kind == "relationship_context_review_only":
        return 80
    if kind == "source_blind_memory_review_only" or source_type == "source_blind_memory":
        return 90
    if bool(_row_value(row, "review_only", True)):
        return 100
    return 110


def _timestamp_rank(value: str) -> float:
    text = str(value or "").strip()
    if not text:
        return 0.0
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return float(sum((idx + 1) * ord(ch) for idx, ch in enumerate(text[:80])))


def _entity_evidence_sort_key(row: sqlite3.Row | dict[str, Any]) -> tuple[Any, ...]:
    kind = str(_row_value(row, "evidence_kind", "") or "")
    source_type = str(_row_value(row, "source_type", "") or "")
    relation = str(_row_value(row, "relation_to_subject", "") or "")
    topic = str(_row_value(row, "topic", "") or "")
    safe_summary = str(_row_value(row, "safe_summary", "") or "")
    confidence = float(_row_value(row, "confidence", 0) or 0)
    observed = str(_row_value(row, "observed_at", "") or "")
    updated = str(_row_value(row, "updated_at", "") or "")
    row_id = int(_row_value(row, "id", 0) or 0)
    source_rank = {"conversation": 0, "broadcast_memory": 1, "community_presence": 2, "user_profiles": 3, "relationship_context": 4, "source_blind_memory": 9}.get(source_type, 5)
    return (
        evidence_event_priority(row),
        0 if bool(_row_value(row, "public_safe_candidate", False)) else 1,
        0 if relation == "authored" or kind.startswith("authored_") else 1,
        -confidence,
        0 if topic and safe_summary else 1,
        source_rank,
        -_timestamp_rank(observed or updated),
        -row_id,
    )


def sort_entity_evidence_rows(rows: list[sqlite3.Row]) -> list[sqlite3.Row]:
    """Sort structured evidence by operator usefulness instead of insertion recency."""

    return sorted(rows, key=_entity_evidence_sort_key)


def get_ranked_entity_evidence_for_subject(conn: sqlite3.Connection, subject_name: str, guild_id: int | None = None, limit: int = 200) -> list[sqlite3.Row]:
    if not table_exists(conn, ENTITY_EVIDENCE_TABLE):
        return []
    key = subject_key(normalize_subject_name(subject_name))
    where = "subject_key=?"
    params: list[Any] = [key]
    if guild_id is not None:
        where += " AND guild_id=?"
        params.append(guild_id)
    cur = conn.cursor()
    cur.row_factory = sqlite3.Row
    rows = list(cur.execute(f"SELECT * FROM {ENTITY_EVIDENCE_TABLE} WHERE {where}", params))
    return sort_entity_evidence_rows(rows)[: max(1, limit)]


def get_entity_evidence_for_subject(conn: sqlite3.Connection, subject_name: str, guild_id: int | None = None, limit: int = 200) -> list[sqlite3.Row]:
    """Return ranked entity evidence for summaries/readouts.

    This deliberately does not simply read newest rows because bulk legacy/source-blind
    refreshes can otherwise bury stronger public-side conversation evidence.
    """

    return get_ranked_entity_evidence_for_subject(conn, subject_name, guild_id=guild_id, limit=limit)
