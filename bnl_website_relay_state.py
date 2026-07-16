from __future__ import annotations

import difflib
import hashlib
import re
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

MAX_HISTORY = 25
MAX_ATTEMPTS_PER_GUILD = 100
STOCK_FAMILIES = {
    "waiting_standby": (
        "waiting", "standing by", "standby", "awaiting signal", "awaiting fresh", "remains online",
        "until fresh", "until clearer", "no activity", "no public activity",
    ),
    "quiet_signal": (
        "quiet", "thin signal", "weak signal", "low signal", "signal is thin", "channels are quiet",
        "corridor is quiet", "public signal is thin", "nothing fresh", "no fresh", "has cleared",
    ),
    "observation_posture": (
        "observing", "observation posture", "monitor", "monitoring", "watching", "listening posture",
        "listening window", "remains operational", "remains active",
    ),
    "bridge_active": (
        "bridge active", "bridge is active", "relay active", "corridor remains open", "public access corridor",
        "outer channel remains live", "broadcast aperture is open",
    ),
}

@dataclass
class RelaySourceDecision:
    source_class: str
    context: str
    aggregate_counts: dict[str, int] = field(default_factory=dict)
    source_conversation_ids: list[int] = field(default_factory=list)
    source_cursor: int = 0
    highest_eligible_conversation_id: int = 0
    skip_reason: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class WebsiteRelayDecision:
    publish: bool
    skipReason: str = ""
    eventType: str = ""
    sourceConversationIds: list[int] = field(default_factory=list)
    sourceCursor: int = 0
    message: str = ""
    directive: str = ""
    mode: str = "OBSERVATION"
    relayLane: str = "current_signal"
    metadata: dict[str, Any] = field(default_factory=dict)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def ensure_schema(db_path: str) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS website_relay_state (
            guild_id INTEGER PRIMARY KEY,
            last_published_conversation_cursor INTEGER NOT NULL DEFAULT 0,
            last_publication_timestamp TEXT
        )
        """)
        conn.execute("""
        CREATE TABLE IF NOT EXISTS website_relay_history (
            relay_id TEXT PRIMARY KEY,
            guild_id INTEGER NOT NULL,
            public_message TEXT NOT NULL,
            public_directive TEXT NOT NULL,
            mode TEXT NOT NULL,
            relay_lane TEXT NOT NULL,
            event_type TEXT NOT NULL,
            highest_source_conversation_id INTEGER NOT NULL,
            normalized_message TEXT NOT NULL,
            semantic_family TEXT NOT NULL,
            published_timestamp TEXT NOT NULL
        )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_website_relay_history_guild_time ON website_relay_history(guild_id, published_timestamp DESC)")

        conn.execute("""
        CREATE TABLE IF NOT EXISTS website_relay_attempts (
            attempt_id TEXT PRIMARY KEY,
            guild_id INTEGER NOT NULL,
            trigger TEXT NOT NULL,
            source_class TEXT NOT NULL,
            started_at TEXT NOT NULL,
            completed_at TEXT,
            outcome TEXT NOT NULL,
            reason TEXT,
            aggregate_source_counts TEXT NOT NULL DEFAULT '{}',
            cursor INTEGER NOT NULL DEFAULT 0,
            highest_eligible_conversation_id INTEGER NOT NULL DEFAULT 0,
            accepted_relay_id TEXT
        )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_website_relay_attempts_guild_time ON website_relay_attempts(guild_id, started_at DESC)")
        cols = {r[1] for r in conn.execute("PRAGMA table_info(website_relay_attempts)").fetchall()}
        for name, ddl in {
            "prepared_relay_id": "ALTER TABLE website_relay_attempts ADD COLUMN prepared_relay_id TEXT",
            "website_published_at": "ALTER TABLE website_relay_attempts ADD COLUMN website_published_at TEXT",
            "idempotent": "ALTER TABLE website_relay_attempts ADD COLUMN idempotent INTEGER NOT NULL DEFAULT 0",
        }.items():
            if name not in cols:
                conn.execute(ddl)

        conn.execute("""
        CREATE TABLE IF NOT EXISTS website_relay_pending_v2 (
            guild_id INTEGER PRIMARY KEY,
            relay_id TEXT NOT NULL,
            message TEXT NOT NULL,
            current_directive TEXT NOT NULL,
            source_class TEXT NOT NULL,
            trigger TEXT NOT NULL,
            source_cursor INTEGER NOT NULL DEFAULT 0,
            source_conversation_fingerprint TEXT NOT NULL,
            canonical_json TEXT NOT NULL,
            prepared_at TEXT NOT NULL,
            mode TEXT NOT NULL DEFAULT 'OBSERVATION',
            relay_lane TEXT NOT NULL DEFAULT 'current_signal',
            event_type TEXT NOT NULL DEFAULT '',
            aggregate_source_counts TEXT NOT NULL DEFAULT '{}',
            highest_eligible_conversation_id INTEGER NOT NULL DEFAULT 0
        )
        """)


def get_cursor(db_path: str, guild_id: int) -> int | None:
    ensure_schema(db_path)
    with sqlite3.connect(db_path) as conn:
        row = conn.execute("SELECT last_published_conversation_cursor FROM website_relay_state WHERE guild_id=?", (guild_id,)).fetchone()
    return int(row[0]) if row else None


def bootstrap_cursor(db_path: str, guild_id: int, cursor: int) -> None:
    ensure_schema(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute("INSERT OR REPLACE INTO website_relay_state(guild_id,last_published_conversation_cursor,last_publication_timestamp) VALUES(?,?,NULL)", (guild_id, int(cursor or 0)))


def normalize_text(text: str) -> str:
    text = (text or "").lower().replace("—", " ").replace("–", " ")
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _has_stock_marker(norm: str, marker: str) -> bool:
    marker_norm = normalize_text(marker)
    if not marker_norm:
        return False
    if " " in marker_norm:
        return re.search(r"(?<![a-z0-9])" + re.escape(marker_norm) + r"(?![a-z0-9])", norm) is not None
    return re.search(r"(?<![a-z0-9])" + re.escape(marker_norm) + r"(?![a-z0-9])", norm) is not None


def semantic_family(text: str) -> str:
    norm = normalize_text(text)
    for family, markers in STOCK_FAMILIES.items():
        if any(_has_stock_marker(norm, marker) for marker in markers):
            return "non_event_stock"
    if any(k in norm for k in ("track", "song", "submission", "broadcast", "show", "question", "asked", "discuss")):
        return "public_discord_activity"
    return "general_public_signal"


STOCK_DIRECTIVE_MARKERS = (
    "continue monitoring", "await further activity", "await fresh", "review fresh context",
    "review fresh public discord context", "maintain observation posture", "stand by",
    "standing by", "monitor until", "await clearer", "waiting for", "remain online",
    "hold the relay", "passive listen mode", "refresh once clear public context returns",
    "clear public context returns",
)


def stock_directive_reason(directive: str) -> str:
    norm = normalize_text(directive)
    if not norm:
        return "empty_directive"
    if any(_has_stock_marker(norm, marker) for marker in STOCK_DIRECTIVE_MARKERS):
        return "stock_directive_rejected"
    # A useful current directive should be specific enough to not fit every relay.
    if len(norm.split()) < 5:
        return "directive_too_thin"
    return ""


def recent_history(db_path: str, guild_id: int, limit: int = MAX_HISTORY) -> list[dict[str, Any]]:
    ensure_schema(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM website_relay_history WHERE guild_id=? ORDER BY published_timestamp DESC LIMIT ?",
            (guild_id, limit),
        ).fetchall()
    return [dict(r) for r in rows]


def reject_reason_for_candidate(db_path: str, guild_id: int, message: str, directive: str = "", *, threshold: float = 0.92) -> str:
    norm = normalize_text(message)
    dir_norm = normalize_text(directive)
    if not norm:
        return "empty_output"
    fam = semantic_family(message)
    if fam == "non_event_stock":
        return "stock_family_rejected"
    for rec in recent_history(db_path, guild_id):
        old = rec.get("normalized_message") or normalize_text(rec.get("public_message", ""))
        old_directive = normalize_text(rec.get("public_directive", ""))
        # Directive equality alone is not a duplicate: only the public message, or
        # the complete message/directive pair, can block a fresh relay.
        if norm == old or (norm == old and dir_norm == old_directive):
            return "exact_duplicate"
        if old and difflib.SequenceMatcher(None, norm, old).ratio() >= threshold:
            return "near_duplicate"
    return ""


def get_pending_v2_publication(db_path: str, guild_id: int) -> dict[str, Any]:
    ensure_schema(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM website_relay_pending_v2 WHERE guild_id=?", (guild_id,)).fetchone()
    return dict(row) if row else {}


def save_pending_v2_publication(
    db_path: str,
    guild_id: int,
    *,
    relay_id: str,
    message: str,
    current_directive: str,
    source_class: str,
    trigger: str,
    source_cursor: int,
    source_conversation_fingerprint: str,
    canonical_json: str,
    mode: str = "OBSERVATION",
    relay_lane: str = "current_signal",
    event_type: str = "",
    aggregate_source_counts: dict[str, int] | None = None,
    highest_eligible_conversation_id: int = 0,
) -> None:
    ensure_schema(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute("""
        INSERT OR REPLACE INTO website_relay_pending_v2(
            guild_id,relay_id,message,current_directive,source_class,trigger,source_cursor,source_conversation_fingerprint,
            canonical_json,prepared_at,mode,relay_lane,event_type,aggregate_source_counts,highest_eligible_conversation_id
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            guild_id, relay_id, message, current_directive, source_class, trigger, int(source_cursor or 0),
            source_conversation_fingerprint or "", canonical_json, utc_now_iso(), mode or "OBSERVATION", relay_lane or "current_signal",
            event_type or "", __import__('json').dumps(aggregate_source_counts or {}, sort_keys=True), int(highest_eligible_conversation_id or 0),
        ))


def clear_pending_v2_publication(db_path: str, guild_id: int, relay_id: str = "") -> None:
    ensure_schema(db_path)
    with sqlite3.connect(db_path) as conn:
        if relay_id:
            conn.execute("DELETE FROM website_relay_pending_v2 WHERE guild_id=? AND relay_id=?", (guild_id, relay_id))
        else:
            conn.execute("DELETE FROM website_relay_pending_v2 WHERE guild_id=?", (guild_id,))


def record_publication(db_path: str, guild_id: int, *, message: str, directive: str, mode: str, relay_lane: str, event_type: str, source_cursor: int, published_timestamp: str | None = None, relay_id: str | None = None) -> str:
    ensure_schema(db_path)
    ts = published_timestamp or utc_now_iso()
    norm = normalize_text(message)
    fam = semantic_family(message)
    relay_id = relay_id or hashlib.sha256(f"{guild_id}|{source_cursor}|{norm}|{ts}".encode()).hexdigest()[:24]
    with sqlite3.connect(db_path) as conn:
        existing = conn.execute("SELECT public_message, public_directive FROM website_relay_history WHERE relay_id=?", (relay_id,)).fetchone()
        if existing:
            if existing[0] != message or existing[1] != directive:
                raise ValueError("local_relay_id_conflict")
            conn.execute("INSERT OR REPLACE INTO website_relay_state(guild_id,last_published_conversation_cursor,last_publication_timestamp) VALUES(?,?,?)", (guild_id, int(source_cursor or 0), ts))
            conn.execute("""
            UPDATE website_relay_history
            SET guild_id=?, mode=?, relay_lane=?, event_type=?, highest_source_conversation_id=?, normalized_message=?, semantic_family=?, published_timestamp=?
            WHERE relay_id=?
            """, (guild_id, mode, relay_lane, event_type, int(source_cursor or 0), norm, fam, ts, relay_id))
        else:
            conn.execute("INSERT OR REPLACE INTO website_relay_state(guild_id,last_published_conversation_cursor,last_publication_timestamp) VALUES(?,?,?)", (guild_id, int(source_cursor or 0), ts))
            conn.execute("""
            INSERT INTO website_relay_history(relay_id,guild_id,public_message,public_directive,mode,relay_lane,event_type,highest_source_conversation_id,normalized_message,semantic_family,published_timestamp)
            VALUES(?,?,?,?,?,?,?,?,?,?,?)
            """, (relay_id, guild_id, message, directive, mode, relay_lane, event_type, int(source_cursor or 0), norm, fam, ts))
        old = conn.execute("SELECT relay_id FROM website_relay_history WHERE guild_id=? ORDER BY published_timestamp DESC LIMIT -1 OFFSET ?", (guild_id, MAX_HISTORY)).fetchall()
        if old:
            conn.executemany("DELETE FROM website_relay_history WHERE relay_id=?", [(r[0],) for r in old])
    return relay_id


def _prune_attempts(conn: sqlite3.Connection, guild_id: int) -> None:
    old = conn.execute(
        "SELECT attempt_id FROM website_relay_attempts WHERE guild_id=? ORDER BY started_at DESC LIMIT -1 OFFSET ?",
        (guild_id, MAX_ATTEMPTS_PER_GUILD),
    ).fetchall()
    if old:
        conn.executemany("DELETE FROM website_relay_attempts WHERE attempt_id=?", [(r[0],) for r in old])


def begin_attempt(db_path: str, attempt_id: str, guild_id: int, trigger: str, source_class: str = "pending", *, cursor: int = 0, highest_eligible_conversation_id: int = 0, aggregate_source_counts: dict[str, int] | None = None) -> None:
    ensure_schema(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute("""
        INSERT OR REPLACE INTO website_relay_attempts(attempt_id,guild_id,trigger,source_class,started_at,outcome,aggregate_source_counts,cursor,highest_eligible_conversation_id)
        VALUES(?,?,?,?,?,?,?,?,?)
        """, (attempt_id, guild_id, trigger, source_class, utc_now_iso(), "accepted", __import__('json').dumps(aggregate_source_counts or {}, sort_keys=True), int(cursor or 0), int(highest_eligible_conversation_id or 0)))
        _prune_attempts(conn, guild_id)


def prepare_attempt_relay(db_path: str, attempt_id: str, prepared_relay_id: str) -> None:
    ensure_schema(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute("UPDATE website_relay_attempts SET prepared_relay_id=? WHERE attempt_id=?", (prepared_relay_id or "", attempt_id))

def hydrate_publication(db_path: str, guild_id: int, *, relay_id: str, message: str, directive: str, source_class: str, trigger: str, published_timestamp: str) -> bool:
    ensure_schema(db_path)
    norm = normalize_text(message)
    fam = semantic_family(message)
    with sqlite3.connect(db_path) as conn:
        existing = conn.execute("SELECT public_message, public_directive, published_timestamp FROM website_relay_history WHERE relay_id=?", (relay_id,)).fetchone()
        if existing:
            return False
        conn.execute("""
        INSERT INTO website_relay_history(relay_id,guild_id,public_message,public_directive,mode,relay_lane,event_type,highest_source_conversation_id,normalized_message,semantic_family,published_timestamp)
        VALUES(?,?,?,?,?,?,?,?,?,?,?)
        """, (relay_id, guild_id, message, directive, "OBSERVATION", "hydrated", source_class or trigger or "hydrated", 0, norm, fam, published_timestamp))
        old = conn.execute("SELECT relay_id FROM website_relay_history WHERE guild_id=? ORDER BY published_timestamp DESC LIMIT -1 OFFSET ?", (guild_id, MAX_HISTORY)).fetchall()
        if old:
            conn.executemany("DELETE FROM website_relay_history WHERE relay_id=?", [(r[0],) for r in old])
    return True

def complete_attempt(db_path: str, attempt_id: str, *, source_class: str, outcome: str, reason: str = "", aggregate_source_counts: dict[str, int] | None = None, cursor: int = 0, highest_eligible_conversation_id: int = 0, accepted_relay_id: str = "", prepared_relay_id: str = "", website_published_at: str = "", idempotent: bool = False) -> None:
    ensure_schema(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute("""
        UPDATE website_relay_attempts
        SET source_class=?, completed_at=?, outcome=?, reason=?, aggregate_source_counts=?, cursor=?, highest_eligible_conversation_id=?, accepted_relay_id=?, prepared_relay_id=COALESCE(NULLIF(?, ''), prepared_relay_id), website_published_at=?, idempotent=?
        WHERE attempt_id=?
        """, (source_class or "none", utc_now_iso(), outcome, reason or "", __import__('json').dumps(aggregate_source_counts or {}, sort_keys=True), int(cursor or 0), int(highest_eligible_conversation_id or 0), accepted_relay_id or "", prepared_relay_id or "", website_published_at or "", 1 if idempotent else 0, attempt_id))


def get_attempt(db_path: str, attempt_id: str) -> dict[str, Any]:
    ensure_schema(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM website_relay_attempts WHERE attempt_id=?", (attempt_id,)).fetchone()
    return dict(row) if row else {}


def last_attempt(db_path: str, guild_id: int) -> dict[str, Any]:
    ensure_schema(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM website_relay_attempts WHERE guild_id=? ORDER BY started_at DESC LIMIT 1", (guild_id,)).fetchone()
    return dict(row) if row else {}
