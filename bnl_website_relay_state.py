from __future__ import annotations

import difflib
import hashlib
import re
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

MAX_HISTORY = 25
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


def semantic_family(text: str) -> str:
    norm = normalize_text(text)
    for family, markers in STOCK_FAMILIES.items():
        if any(marker in norm for marker in markers):
            return "non_event_stock"
    if any(k in norm for k in ("track", "song", "submission", "broadcast", "show", "question", "asked", "discuss")):
        return "public_discord_activity"
    return "general_public_signal"


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
    fam = semantic_family(f"{message} {directive}")
    if fam == "non_event_stock":
        return "stock_family_rejected"
    for rec in recent_history(db_path, guild_id):
        old = rec.get("normalized_message") or normalize_text(rec.get("public_message", ""))
        if norm == old or (dir_norm and dir_norm == normalize_text(rec.get("public_directive", ""))):
            return "exact_duplicate"
        if old and difflib.SequenceMatcher(None, norm, old).ratio() >= threshold:
            return "near_duplicate"
        if rec.get("semantic_family") == fam and fam != "public_discord_activity":
            return "repeated_semantic_family"
    return ""


def record_publication(db_path: str, guild_id: int, *, message: str, directive: str, mode: str, relay_lane: str, event_type: str, source_cursor: int, published_timestamp: str | None = None) -> str:
    ensure_schema(db_path)
    ts = published_timestamp or utc_now_iso()
    norm = normalize_text(message)
    fam = semantic_family(f"{message} {directive}")
    relay_id = hashlib.sha256(f"{guild_id}|{source_cursor}|{norm}|{ts}".encode()).hexdigest()[:24]
    with sqlite3.connect(db_path) as conn:
        conn.execute("INSERT OR REPLACE INTO website_relay_state(guild_id,last_published_conversation_cursor,last_publication_timestamp) VALUES(?,?,?)", (guild_id, int(source_cursor or 0), ts))
        conn.execute("""
        INSERT INTO website_relay_history(relay_id,guild_id,public_message,public_directive,mode,relay_lane,event_type,highest_source_conversation_id,normalized_message,semantic_family,published_timestamp)
        VALUES(?,?,?,?,?,?,?,?,?,?,?)
        """, (relay_id, guild_id, message, directive, mode, relay_lane, event_type, int(source_cursor or 0), norm, fam, ts))
        old = conn.execute("SELECT relay_id FROM website_relay_history WHERE guild_id=? ORDER BY published_timestamp DESC LIMIT -1 OFFSET ?", (guild_id, MAX_HISTORY)).fetchall()
        if old:
            conn.executemany("DELETE FROM website_relay_history WHERE relay_id=?", [(r[0],) for r in old])
    return relay_id
