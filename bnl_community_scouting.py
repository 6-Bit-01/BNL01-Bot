"""Safe community-presence scouting helpers for BNL dossier discovery.

This module only handles lightweight, review-only signals from approved live
community contexts supplied by the bot. It does not fetch Discord history, read
DMs, merge identities, infer real-world identity, create drafts, publish, or
send recommendations by itself.
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
from datetime import datetime, timezone
from typing import Any

from bnl_dossier_source_packets import MAX_SNIPPET_LENGTH, subject_key

COMMUNITY_PRESENCE_LANE = "community_presence"
COMMUNITY_SCOUTING_MIN_SIGNALS_DEFAULT = 2
MAX_COMMUNITY_EVIDENCE_SNIPPETS = 3
MAX_CONNECTION_NOTES = 4
MAX_CHANNEL_LABELS = 6
_DISCORD_MENTION_PATTERN = re.compile(r"<[@#!&]?[0-9]{5,}>")
_LONG_ID_PATTERN = re.compile(r"\b\d{15,22}\b")
_EMAIL_PATTERN = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.I)
_URL_PATTERN = re.compile(r"https?://\S+", re.I)
_SUBJECT_WORD = r"[A-Z0-9][A-Za-z0-9'_-]*"
_SUBJECT_PHRASE = rf"{_SUBJECT_WORD}(?:\s+{_SUBJECT_WORD}){{0,3}}"
WEAK_COMMUNITY_TERMS = {
    "everyone", "somebody", "someone", "anyone", "today", "tonight", "yesterday", "tomorrow",
    "chat", "thread", "post", "comment", "reply", "song", "track", "live", "show", "server", "discord",
    "message", "channel", "admin", "operator", "owner", "mod", "bot", "user", "member", "people",
    "thing", "stuff", "voice", "role", "rules", "welcome", "announcement", "question", "answer",
}


def community_scouting_enabled(environ: dict[str, str] | None = None) -> bool:
    env = environ if environ is not None else os.environ
    return (env.get("BNL_COMMUNITY_SCOUTING_ENABLED") or "false").strip().lower() in {"1", "true", "yes", "on"}


def configured_channel_ids(environ: dict[str, str] | None = None) -> set[int]:
    env = environ if environ is not None else os.environ
    ids: set[int] = set()
    for part in re.split(r"[,\s]+", (env.get("BNL_COMMUNITY_SCOUTING_CHANNEL_IDS") or "").strip()):
        if not part:
            continue
        try:
            ids.add(int(part))
        except ValueError:
            continue
    return ids


def community_min_signals(environ: dict[str, str] | None = None) -> int:
    env = environ if environ is not None else os.environ
    try:
        return max(1, min(int(env.get("BNL_COMMUNITY_SCOUTING_MIN_SIGNALS") or COMMUNITY_SCOUTING_MIN_SIGNALS_DEFAULT), 10))
    except ValueError:
        return COMMUNITY_SCOUTING_MIN_SIGNALS_DEFAULT


def is_community_presence_channel_allowed(channel_id: int | None, channel_policy: str, environ: dict[str, str] | None = None) -> bool:
    configured = configured_channel_ids(environ)
    if configured:
        return bool(channel_id and int(channel_id) in configured)
    return (channel_policy or "").strip().lower() in {"public_home", "public_context", "public_selective"}


def safe_community_snippet(text: str, max_length: int = MAX_SNIPPET_LENGTH) -> str:
    cleaned = _DISCORD_MENTION_PATTERN.sub("[redacted-mention]", str(text or ""))
    cleaned = _LONG_ID_PATTERN.sub("[redacted-id]", cleaned)
    cleaned = _EMAIL_PATTERN.sub("[redacted-email]", cleaned)
    cleaned = _URL_PATTERN.sub("[redacted-url]", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    if len(cleaned) <= max_length:
        return cleaned
    return cleaned[: max(0, max_length - 1)].rstrip() + "…"


def clean_subject_name(candidate: str) -> str:
    cleaned = re.sub(r"\s+", " ", (candidate or "").strip(" .,:;!?()[]{}\"'"))
    cleaned = re.sub(r"^(?:the|a|an)\s+", "", cleaned, flags=re.I).strip()
    return cleaned[:80]


def is_weak_community_subject(candidate: str) -> bool:
    subject = clean_subject_name(candidate)
    normalized = subject.lower()
    if not subject or len(subject) < 3:
        return True
    if re.fullmatch(r"\d+", subject) or not re.search(r"[A-Za-z]", subject):
        return True
    words = re.findall(r"[A-Za-z0-9]+", normalized)
    if normalized in WEAK_COMMUNITY_TERMS:
        return True
    if words and all(word in WEAK_COMMUNITY_TERMS for word in words):
        return True
    if words and words[0] in WEAK_COMMUNITY_TERMS:
        return True
    if normalized in {"bnl", "bnl 01", "barcode", "barcode network", "barcode radio"}:
        return True
    return False


def _json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if not value:
        return []
    try:
        parsed = json.loads(value)
    except Exception:
        return []
    return parsed if isinstance(parsed, list) else []


def _append_unique(items: list[Any], value: Any, limit: int) -> list[Any]:
    if value in (None, "", []):
        return items[:limit]
    if value not in items:
        items.append(value)
    return items[-limit:]


def ensure_community_presence_schema(db_path: str) -> None:
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS community_presence (
            guild_id INTEGER NOT NULL,
            subject_key TEXT NOT NULL,
            display_name TEXT NOT NULL,
            first_seen_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            source_lanes TEXT DEFAULT '["community_presence"]',
            approved_channel_labels TEXT DEFAULT '[]',
            mention_count INTEGER DEFAULT 0,
            direct_interaction_count INTEGER DEFAULT 0,
            operator_mention_count INTEGER DEFAULT 0,
            active_windows TEXT DEFAULT '[]',
            connection_notes TEXT DEFAULT '[]',
            evidence_snippets TEXT DEFAULT '[]',
            category TEXT DEFAULT 'community_regular_candidate',
            last_error_status TEXT DEFAULT 'none',
            PRIMARY KEY (guild_id, subject_key)
        )
        """
    )
    conn.commit()
    conn.close()


def _extract_connection_subjects(content: str) -> list[tuple[str, str, str]]:
    rows: list[tuple[str, str, str]] = []
    patterns = (
        rf"\b({_SUBJECT_PHRASE})\s+(?:via|through|introduced\s+through|introduced\s+by)\s+({_SUBJECT_PHRASE})\b",
        rf"\b(?:introduced|introducing|mention(?:ed)?|brought\s+up)\s+({_SUBJECT_PHRASE})\s+(?:via|through|by)\s+({_SUBJECT_PHRASE})\b",
    )
    for pattern in patterns:
        for match in re.finditer(pattern, content or ""):
            subject = clean_subject_name(match.group(1))
            connector = clean_subject_name(match.group(2))
            if subject and connector and not is_weak_community_subject(subject) and not is_weak_community_subject(connector):
                rows.append((subject, connector, f"mentioned through {connector}"))
    return rows


def extract_community_subjects(content: str) -> list[str]:
    subjects: list[str] = []
    explicit_patterns = (
        rf"\b(?:source file|dossier|community candidate|community subject|review)\s+(?:for|on|about)\s+({_SUBJECT_PHRASE})\b",
        rf"\b({_SUBJECT_PHRASE})\s+(?:is|was|seems like|feels like)\s+(?:a\s+)?(?:regular|recurring|community|artist|collaborator|moderator|mod|support|participant|candidate)\b",
        rf"\b(?:artist|collaborator|regular|participant|moderator|mod)\s+({_SUBJECT_PHRASE})\b",
    )
    for pattern in explicit_patterns:
        for match in re.finditer(pattern, content or "", flags=re.I):
            subject = clean_subject_name(match.group(1))
            if subject and not is_weak_community_subject(subject):
                subjects.append(subject)
    for subject, _connector, _note in _extract_connection_subjects(content):
        subjects.append(subject)
    deduped: list[str] = []
    for subject in subjects:
        key = subject_key(subject)
        if key not in {subject_key(item) for item in deduped}:
            deduped.append(subject)
    return deduped


def _category_for_signal(subject: str, content: str, *, connection_note: str = "", operator_mention: bool = False) -> str:
    text = f"{subject} {content} {connection_note}".lower()
    if re.search(r"\b(alias|aka|same as|duplicate|identity link|identity review)\b", text):
        return "possible_alias_review"
    if connection_note or re.search(r"\b(via|through|introduced by|introduced through|connected to)\b", text):
        return "introduced_subject_candidate"
    if re.search(r"\b(mod|moderator|support|staff)\b", text) and operator_mention:
        return "mod_support_candidate"
    if re.search(r"\b(artist|collaborator|producer|dj|vocalist|musician)\b", text):
        return "artist_or_collaborator_candidate"
    return "community_regular_candidate"


def upsert_community_presence_subject(
    db_path: str,
    guild_id: int,
    subject_name: str,
    *,
    channel_label: str = "approved community channel",
    mention_increment: int = 1,
    direct_interaction_increment: int = 0,
    operator_mention_increment: int = 0,
    connection_note: str = "",
    evidence_snippet: str = "",
    category: str = "community_regular_candidate",
    now: datetime | None = None,
) -> bool:
    subject = clean_subject_name(subject_name)
    if is_weak_community_subject(subject):
        return False
    ensure_community_presence_schema(db_path)
    now_dt = now or datetime.now(timezone.utc)
    now_text = now_dt.isoformat()
    day = now_dt.date().isoformat()
    key = subject_key(subject)
    lane = COMMUNITY_PRESENCE_LANE
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("SELECT * FROM community_presence WHERE guild_id=? AND subject_key=?", (guild_id, key))
    row = cur.fetchone()
    columns = [desc[0] for desc in cur.description] if cur.description else []
    existing = dict(zip(columns, row)) if row else {}
    if existing:
        channels = _append_unique(_json_list(existing.get("approved_channel_labels")), channel_label, MAX_CHANNEL_LABELS)
        windows = _append_unique(_json_list(existing.get("active_windows")), day, 30)
        notes = _append_unique(_json_list(existing.get("connection_notes")), safe_community_snippet(connection_note, 120), MAX_CONNECTION_NOTES) if connection_note else _json_list(existing.get("connection_notes"))[:MAX_CONNECTION_NOTES]
        snippets = _append_unique(_json_list(existing.get("evidence_snippets")), safe_community_snippet(evidence_snippet, 160), MAX_COMMUNITY_EVIDENCE_SNIPPETS) if evidence_snippet else _json_list(existing.get("evidence_snippets"))[:MAX_COMMUNITY_EVIDENCE_SNIPPETS]
        old_category = existing.get("category") or "community_regular_candidate"
        new_category = category if old_category == "community_regular_candidate" or category != "community_regular_candidate" else old_category
        cur.execute(
            """
            UPDATE community_presence
            SET display_name=?, last_seen_at=?, source_lanes=?, approved_channel_labels=?,
                mention_count=mention_count+?, direct_interaction_count=direct_interaction_count+?,
                operator_mention_count=operator_mention_count+?, active_windows=?, connection_notes=?,
                evidence_snippets=?, category=?, last_error_status='none'
            WHERE guild_id=? AND subject_key=?
            """,
            (
                subject, now_text, json.dumps([lane]), json.dumps(channels), int(mention_increment),
                int(direct_interaction_increment), int(operator_mention_increment), json.dumps(windows),
                json.dumps(notes), json.dumps(snippets), new_category, guild_id, key,
            ),
        )
    else:
        cur.execute(
            """
            INSERT INTO community_presence (
                guild_id, subject_key, display_name, first_seen_at, last_seen_at, source_lanes,
                approved_channel_labels, mention_count, direct_interaction_count, operator_mention_count,
                active_windows, connection_notes, evidence_snippets, category, last_error_status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'none')
            """,
            (
                guild_id, key, subject, now_text, now_text, json.dumps([lane]),
                json.dumps([channel_label] if channel_label else []), int(mention_increment),
                int(direct_interaction_increment), int(operator_mention_increment), json.dumps([day]),
                json.dumps([safe_community_snippet(connection_note, 120)] if connection_note else []),
                json.dumps([safe_community_snippet(evidence_snippet, 160)] if evidence_snippet else []),
                category,
            ),
        )
    conn.commit()
    conn.close()
    return True


def record_community_presence_event(
    db_path: str,
    guild_id: int,
    author_display_name: str,
    content: str,
    *,
    channel_id: int | None = None,
    channel_name: str = "",
    channel_policy: str = "unknown",
    direct_interaction: bool = False,
    operator_mention: bool = False,
    environ: dict[str, str] | None = None,
) -> dict[str, Any]:
    if not community_scouting_enabled(environ):
        return {"ok": False, "status": "disabled", "stored": 0}
    if not is_community_presence_channel_allowed(channel_id, channel_policy, environ):
        return {"ok": False, "status": "channel_not_approved", "stored": 0}
    if not guild_id:
        return {"ok": False, "status": "missing_guild", "stored": 0}

    channel_label = (channel_policy or "approved community channel").strip()[:40]
    safe_snippet = safe_community_snippet(content, 160)
    stored = 0

    if author_display_name and upsert_community_presence_subject(
        db_path,
        guild_id,
        author_display_name,
        channel_label=channel_label,
        mention_increment=1,
        direct_interaction_increment=1 if direct_interaction else 0,
        operator_mention_increment=0,
        evidence_snippet="direct BNL interaction in approved community context" if direct_interaction else "approved-channel presence",
        category="community_regular_candidate",
    ):
        stored += 1

    connection_subjects = _extract_connection_subjects(content)
    connection_keys = {subject_key(subject) for subject, _connector, _note in connection_subjects}
    for subject, _connector, note in connection_subjects:
        category = _category_for_signal(subject, content, connection_note=note, operator_mention=operator_mention)
        if upsert_community_presence_subject(
            db_path,
            guild_id,
            subject,
            channel_label=channel_label,
            mention_increment=1,
            operator_mention_increment=1 if operator_mention else 0,
            connection_note=note,
            evidence_snippet=f"connection phrase: {note}",
            category=category,
        ):
            stored += 1

    for subject in extract_community_subjects(content):
        if subject_key(subject) in connection_keys:
            continue
        category = _category_for_signal(subject, content, operator_mention=operator_mention)
        if upsert_community_presence_subject(
            db_path,
            guild_id,
            subject,
            channel_label=channel_label,
            mention_increment=1,
            operator_mention_increment=1 if operator_mention else 0,
            evidence_snippet=("operator/mod mention in approved community context" if operator_mention else safe_snippet),
            category=category,
        ):
            stored += 1
    return {"ok": True, "status": "ok", "stored": stored}


def get_community_presence_candidates(db_path: str, guild_id: int | None, *, limit: int = 500, min_signals: int | None = None) -> list[dict[str, Any]]:
    if not guild_id:
        return []
    ensure_community_presence_schema(db_path)
    min_required = max(1, int(min_signals or community_min_signals()))
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(
        """
        SELECT * FROM community_presence
        WHERE guild_id=?
        ORDER BY (mention_count + direct_interaction_count + operator_mention_count) DESC, last_seen_at DESC
        LIMIT ?
        """,
        (guild_id, max(1, min(int(limit or 500), 500))),
    )
    rows = cur.fetchall()
    conn.close()
    results: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        signals = int(item.get("mention_count") or 0) + int(item.get("direct_interaction_count") or 0) + int(item.get("operator_mention_count") or 0) + len(_json_list(item.get("active_windows"))) - 1
        if signals < min_required and not int(item.get("operator_mention_count") or 0) and not int(item.get("direct_interaction_count") or 0):
            continue
        results.append(
            {
                "subjectName": item.get("display_name") or item.get("subject_key") or "unknown subject",
                "subjectKey": item.get("subject_key") or subject_key(item.get("display_name") or "unknown subject"),
                "firstSeenAt": item.get("first_seen_at"),
                "lastSeenAt": item.get("last_seen_at"),
                "sourceLanes": _json_list(item.get("source_lanes")) or [COMMUNITY_PRESENCE_LANE],
                "approvedChannelLabels": _json_list(item.get("approved_channel_labels")),
                "mentionCount": int(item.get("mention_count") or 0),
                "directInteractionCount": int(item.get("direct_interaction_count") or 0),
                "operatorMentionCount": int(item.get("operator_mention_count") or 0),
                "daysActive": len(_json_list(item.get("active_windows"))),
                "connectionNotes": _json_list(item.get("connection_notes"))[:MAX_CONNECTION_NOTES],
                "evidenceSnippets": _json_list(item.get("evidence_snippets"))[:MAX_COMMUNITY_EVIDENCE_SNIPPETS],
                "category": item.get("category") or "community_regular_candidate",
                "signalCount": signals,
            }
        )
    return results


def build_community_presence_diagnostics(db_path: str, guild_id: int | None = None, environ: dict[str, str] | None = None, *, last_candidate_count: int = 0, last_withheld_count: int = 0, last_top_withheld_reason: str = "none", last_error_status: str = "none") -> dict[str, Any]:
    enabled = community_scouting_enabled(environ)
    configured = bool(configured_channel_ids(environ))
    item_count = 0
    last_seen = "none"
    candidate_count = 0
    try:
        ensure_community_presence_schema(db_path)
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        if guild_id:
            cur.execute("SELECT COUNT(*), MAX(last_seen_at) FROM community_presence WHERE guild_id=?", (guild_id,))
            row = cur.fetchone()
            item_count = int(row[0] or 0) if row else 0
            last_seen = str(row[1] or "none") if row else "none"
            candidate_count = len(get_community_presence_candidates(db_path, guild_id, min_signals=community_min_signals(environ)))
        else:
            cur.execute("SELECT COUNT(*), MAX(last_seen_at) FROM community_presence")
            row = cur.fetchone()
            item_count = int(row[0] or 0) if row else 0
            last_seen = str(row[1] or "none") if row else "none"
        conn.close()
    except Exception:
        last_error_status = "diagnostic_error"
    return {
        "community_scouting_available": True,
        "community_scouting_enabled": enabled,
        "community_scouting_allowed_channels_configured": configured,
        "community_presence_item_count": item_count,
        "community_presence_last_seen_at": last_seen,
        "community_presence_candidate_count": candidate_count,
        "community_presence_last_candidate_count": int(last_candidate_count or 0),
        "community_presence_last_withheld_count": int(last_withheld_count or 0),
        "community_presence_last_top_withheld_reason": str(last_top_withheld_reason or "none")[:80],
        "community_presence_last_error_status": str(last_error_status or "none")[:80],
    }


def community_category_taxonomy(category: str) -> dict[str, Any]:
    cat = category or "community_regular_candidate"
    if cat == "possible_alias_review":
        return {"type": "identity_link"}
    if cat == "artist_or_collaborator_candidate":
        return {"type": "new_subject", "recommendedCategory": "Personnel", "recommendedKind": "collaborator", "recommendedIdentityAuthority": "mixed_or_unclear"}
    if cat == "mod_support_candidate":
        return {"type": "new_subject", "recommendedCategory": "Personnel", "recommendedKind": "moderator", "recommendedIdentityAuthority": "community_owned"}
    if cat in {"introduced_subject_candidate", "possible_connection_review"}:
        return {"type": "new_subject", "recommendedCategory": "Personnel", "recommendedIdentityAuthority": "mixed_or_unclear"}
    return {"type": "new_subject", "recommendedCategory": "Personnel", "recommendedKind": "community_member", "recommendedIdentityAuthority": "community_owned"}
