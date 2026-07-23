"""Append-only source archive for BNL Journal generation.

This module intentionally has no dependency on the live Journal producer, bot
message flow, or website relay state.  It provides a durable landing table that
those systems can dual-write to in later integration changes.
"""
from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple


PUBLIC_POLICIES = ("public_context", "public_home", "public_selective")
SOURCE_TABLE = "bnl_journal_source_events"
STATE_TABLE = "bnl_journal_source_archive_state"
DELETE_TRIGGER = "trg_bnl_journal_sources_no_delete"


@dataclass(frozen=True)
class SourceRecordResult:
    ok: bool
    status: str
    event_seq: int = 0
    content_hash: str = ""
    reason: str = ""


@dataclass(frozen=True)
class SourceQueryResult:
    events: Tuple[Dict[str, Any], ...]
    counts: Dict[str, Any]


@dataclass(frozen=True)
class LegacyBackfillResult:
    inserted: int = 0
    idempotent: int = 0
    conflicts: int = 0
    skipped: int = 0
    relay_rows_scanned: int = 0
    conversation_rows_scanned: int = 0
    invalid_timestamps: int = 0


def _now_ms() -> int:
    return int(time.time() * 1000)


def _content_hash(raw_text: str) -> str:
    return hashlib.sha256((raw_text or "").encode("utf-8")).hexdigest()


def _canonical_json(value: Optional[Mapping[str, Any]]) -> str:
    return json.dumps(dict(value or {}), ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def timestamp_to_epoch_ms(value: Any) -> Optional[int]:
    """Normalize legacy SQLite/ISO timestamps to UTC epoch milliseconds."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        number = float(value)
        if number < 0:
            return None
        return int(number if number >= 1_000_000_000_000 else number * 1000)
    raw = str(value).strip()
    if not raw:
        return None
    if re.fullmatch(r"\d+(?:\.\d+)?", raw):
        return timestamp_to_epoch_ms(float(raw))
    try:
        parsed = datetime.fromisoformat(raw[:-1] + "+00:00" if raw.endswith("Z") else raw)
    except ValueError:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                parsed = datetime.strptime(raw, fmt)
                break
            except ValueError:
                parsed = None
        if parsed is None:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.astimezone(timezone.utc).timestamp() * 1000)


def sanitize_summary(text: str, private_names: Optional[Sequence[str]] = None, limit: int = 1000) -> str:
    """Build prompt-safe source text while leaving the immutable raw text private."""
    clean = re.sub(r"https?://\S+", "", text or "")
    clean = re.sub(r"<@!?\d+>|@\w+", "someone", clean)
    clean = re.sub(r"\b\d{12,}\b", "", clean)
    for name in sorted({str(item).strip() for item in (private_names or ()) if str(item).strip()}, key=len, reverse=True):
        clean = re.sub(r"\b" + re.escape(name) + r"\b", "someone", clean, flags=re.IGNORECASE)
    clean = re.sub(r"[\"“”‘’]", "", clean)
    clean = re.sub(r"\s+", " ", clean).strip()
    return clean[: max(1, int(limit))]


def ensure_schema(db_path: str) -> None:
    with sqlite3.connect(db_path, timeout=30) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS bnl_journal_source_archive_state (
                guild_id INTEGER PRIMARY KEY,
                activated_at_ms INTEGER NOT NULL CHECK(activated_at_ms >= 0),
                created_at_ms INTEGER NOT NULL CHECK(created_at_ms >= 0),
                legacy_backfill_completed_at_ms INTEGER,
                legacy_conversation_cursor INTEGER NOT NULL DEFAULT 0,
                legacy_relay_cursor_rowid INTEGER NOT NULL DEFAULT 0,
                legacy_relay_cursor_timestamp TEXT NOT NULL DEFAULT '',
                legacy_relay_cursor_id TEXT NOT NULL DEFAULT ''
            )
            """
        )
        state_columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(bnl_journal_source_archive_state)")}
        if "legacy_backfill_completed_at_ms" not in state_columns:
            conn.execute("ALTER TABLE bnl_journal_source_archive_state ADD COLUMN legacy_backfill_completed_at_ms INTEGER")
        if "legacy_conversation_cursor" not in state_columns:
            conn.execute("ALTER TABLE bnl_journal_source_archive_state ADD COLUMN legacy_conversation_cursor INTEGER NOT NULL DEFAULT 0")
        if "legacy_relay_cursor_rowid" not in state_columns:
            conn.execute("ALTER TABLE bnl_journal_source_archive_state ADD COLUMN legacy_relay_cursor_rowid INTEGER NOT NULL DEFAULT 0")
        if "legacy_relay_cursor_timestamp" not in state_columns:
            conn.execute("ALTER TABLE bnl_journal_source_archive_state ADD COLUMN legacy_relay_cursor_timestamp TEXT NOT NULL DEFAULT ''")
        if "legacy_relay_cursor_id" not in state_columns:
            conn.execute("ALTER TABLE bnl_journal_source_archive_state ADD COLUMN legacy_relay_cursor_id TEXT NOT NULL DEFAULT ''")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS bnl_journal_source_events (
                event_seq INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id INTEGER NOT NULL,
                source_kind TEXT NOT NULL,
                source_key TEXT NOT NULL,
                occurred_at_ms INTEGER NOT NULL CHECK(occurred_at_ms >= 0),
                ingested_at_ms INTEGER NOT NULL CHECK(ingested_at_ms >= 0),
                channel_id INTEGER,
                channel_policy TEXT NOT NULL DEFAULT '',
                subject_ref TEXT NOT NULL DEFAULT '',
                private_display_name TEXT NOT NULL DEFAULT '',
                raw_text TEXT NOT NULL,
                sanitized_summary TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                public_usable INTEGER NOT NULL CHECK(public_usable IN (0, 1)),
                metadata_json TEXT NOT NULL DEFAULT '{}',
                UNIQUE(guild_id, source_kind, source_key)
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_bnl_journal_sources_window "
            "ON bnl_journal_source_events(guild_id, occurred_at_ms, source_kind, source_key)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_bnl_journal_sources_public_window "
            "ON bnl_journal_source_events(guild_id, public_usable, occurred_at_ms)"
        )
        conn.execute(
            """
            CREATE TRIGGER IF NOT EXISTS trg_bnl_journal_sources_no_duplicate_insert
            BEFORE INSERT ON bnl_journal_source_events
            WHEN EXISTS (
                SELECT 1 FROM bnl_journal_source_events
                WHERE event_seq=NEW.event_seq
                   OR (
                        guild_id=NEW.guild_id
                        AND source_kind=NEW.source_kind
                        AND source_key=NEW.source_key
                   )
            )
            BEGIN
                SELECT RAISE(ABORT, 'bnl_journal_source_events_duplicate_identity');
            END
            """
        )
        conn.execute(
            """
            CREATE TRIGGER IF NOT EXISTS trg_bnl_journal_sources_no_update
            BEFORE UPDATE ON bnl_journal_source_events
            BEGIN
                SELECT RAISE(ABORT, 'bnl_journal_source_events_immutable');
            END
            """
        )
        _create_delete_trigger(conn)


def _create_delete_trigger(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TRIGGER IF NOT EXISTS trg_bnl_journal_sources_no_delete
        BEFORE DELETE ON bnl_journal_source_events
        BEGIN
            SELECT RAISE(ABORT, 'bnl_journal_source_events_immutable');
        END
        """
    )


def _validate_identity(guild_id: int, source_kind: str, source_key: str, occurred_at_ms: int) -> Tuple[int, str, str, int]:
    guild = int(guild_id)
    kind = str(source_kind or "").strip().lower()
    key = str(source_key or "").strip()
    occurred = int(occurred_at_ms)
    if guild <= 0:
        raise ValueError("invalid_guild_id")
    if not re.fullmatch(r"[a-z0-9_:-]{1,64}", kind):
        raise ValueError("invalid_source_kind")
    if not key or len(key) > 240:
        raise ValueError("invalid_source_key")
    if occurred < 0:
        raise ValueError("invalid_occurred_at_ms")
    return guild, kind, key, occurred


def _record_on_connection(
    conn: sqlite3.Connection,
    *,
    guild_id: int,
    source_kind: str,
    source_key: str,
    occurred_at_ms: int,
    raw_text: str,
    sanitized_summary: Optional[str] = None,
    channel_id: Optional[int] = None,
    channel_policy: str = "",
    subject_ref: str = "",
    private_display_name: str = "",
    public_usable: bool = True,
    metadata: Optional[Mapping[str, Any]] = None,
    ingested_at_ms: Optional[int] = None,
) -> SourceRecordResult:
    guild, kind, key, occurred = _validate_identity(guild_id, source_kind, source_key, occurred_at_ms)
    raw = str(raw_text or "")
    summary = str(sanitized_summary if sanitized_summary is not None else sanitize_summary(raw))
    digest = _content_hash(raw)
    metadata_json = _canonical_json(metadata)
    immutable_values = (
        occurred,
        int(channel_id) if channel_id not in (None, "") else None,
        str(channel_policy or "")[:80],
        str(subject_ref or "")[:160],
        str(private_display_name or "")[:160],
        raw,
        summary,
        digest,
        1 if public_usable else 0,
        metadata_json,
    )
    existing = conn.execute(
        """
        SELECT event_seq, occurred_at_ms, channel_id, channel_policy, subject_ref,
               private_display_name, raw_text, sanitized_summary, content_hash,
               public_usable, metadata_json
        FROM bnl_journal_source_events
        WHERE guild_id=? AND source_kind=? AND source_key=?
        """,
        (guild, kind, key),
    ).fetchone()
    if existing:
        if tuple(existing[1:]) == immutable_values:
            return SourceRecordResult(True, "idempotent", int(existing[0]), digest)
        return SourceRecordResult(False, "conflict", int(existing[0]), str(existing[8]), "immutable_source_conflict")
    cursor = conn.execute(
        """
        INSERT INTO bnl_journal_source_events(
            guild_id, source_kind, source_key, occurred_at_ms, ingested_at_ms,
            channel_id, channel_policy, subject_ref, private_display_name,
            raw_text, sanitized_summary, content_hash, public_usable, metadata_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            guild,
            kind,
            key,
            occurred,
            int(ingested_at_ms) if ingested_at_ms is not None else _now_ms(),
            *immutable_values[1:],
        ),
    )
    return SourceRecordResult(True, "inserted", int(cursor.lastrowid or 0), digest)


def record_source_event(
    db_path: str,
    *,
    guild_id: int,
    source_kind: str,
    source_key: str,
    occurred_at_ms: int,
    raw_text: str,
    sanitized_summary: Optional[str] = None,
    channel_id: Optional[int] = None,
    channel_policy: str = "",
    subject_ref: str = "",
    private_display_name: str = "",
    public_usable: bool = True,
    metadata: Optional[Mapping[str, Any]] = None,
    ingested_at_ms: Optional[int] = None,
) -> SourceRecordResult:
    """Insert one immutable event, returning idempotent for an exact replay."""
    ensure_schema(db_path)
    with sqlite3.connect(db_path, timeout=30) as conn:
        # Serialize the identity check and insert.  Without this write lock, two
        # processes can both observe a missing identity and one can leak a
        # UNIQUE/locking error instead of returning an idempotent result.
        conn.execute("BEGIN IMMEDIATE")
        now_ms = _now_ms()
        conn.execute(
            "INSERT OR IGNORE INTO bnl_journal_source_archive_state(guild_id,activated_at_ms,created_at_ms) VALUES(?,?,?)",
            (int(guild_id), now_ms, now_ms),
        )
        return _record_on_connection(
            conn,
            guild_id=guild_id,
            source_kind=source_kind,
            source_key=source_key,
            occurred_at_ms=occurred_at_ms,
            raw_text=raw_text,
            sanitized_summary=sanitized_summary,
            channel_id=channel_id,
            channel_policy=channel_policy,
            subject_ref=subject_ref,
            private_display_name=private_display_name,
            public_usable=public_usable,
            metadata=metadata,
            ingested_at_ms=ingested_at_ms,
        )


def query_source_events(db_path: str, guild_id: int, start_ms: int, end_ms: int) -> SourceQueryResult:
    """Return every event in the exact half-open interval ``[start_ms, end_ms)``."""
    start = int(start_ms)
    end = int(end_ms)
    if start < 0 or end < start:
        raise ValueError("invalid_source_window")
    ensure_schema(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        state = conn.execute(
            "SELECT activated_at_ms FROM bnl_journal_source_archive_state WHERE guild_id=?",
            (int(guild_id),),
        ).fetchone()
        rows = conn.execute(
            """
            SELECT * FROM bnl_journal_source_events
            WHERE guild_id=? AND occurred_at_ms>=? AND occurred_at_ms<?
            ORDER BY occurred_at_ms ASC, source_kind ASC, source_key ASC, event_seq ASC
            """,
            (int(guild_id), start, end),
        ).fetchall()
    events: List[Dict[str, Any]] = []
    by_kind: Dict[str, int] = {}
    by_policy: Dict[str, int] = {}
    subjects = set()
    channels = set()
    public_count = 0
    for row in rows:
        item = dict(row)
        item["public_usable"] = bool(item["public_usable"])
        try:
            item["metadata"] = json.loads(item.pop("metadata_json") or "{}")
        except (TypeError, json.JSONDecodeError):
            item["metadata"] = {}
        events.append(item)
        kind = str(item.get("source_kind") or "")
        policy = str(item.get("channel_policy") or "")
        by_kind[kind] = by_kind.get(kind, 0) + 1
        by_policy[policy] = by_policy.get(policy, 0) + 1
        if item.get("subject_ref"):
            subjects.add(item["subject_ref"])
        if item.get("channel_id") not in (None, 0):
            channels.add(int(item["channel_id"]))
        public_count += 1 if item["public_usable"] else 0
    counts = {
        "total": len(events),
        "publicUsable": public_count,
        "notPublicUsable": len(events) - public_count,
        "bySourceKind": dict(sorted(by_kind.items())),
        "byChannelPolicy": dict(sorted(by_policy.items())),
        "uniqueSubjects": len(subjects),
        "uniqueChannels": len(channels),
        "archiveActivatedAtMs": int(state[0]) if state else None,
        "coverageComplete": bool(state and start >= int(state[0])),
    }
    return SourceQueryResult(tuple(events), counts)


def _purge_discord_source_events_on_connection(
    conn: sqlite3.Connection,
    guild_id: int,
    user_id: Optional[int] = None,
) -> int:
    """Purge a Discord archive scope inside the caller's active transaction.

    This is the controlled exception to the archive's normal immutability rule.
    Callers must own the surrounding transaction so the trigger change, delete,
    and any related governance deletion commit or roll back together.
    """
    guild = int(guild_id)
    if guild <= 0:
        raise ValueError("invalid_guild_id")
    subject_ref = ""
    if user_id is not None:
        user = int(user_id)
        if user <= 0:
            raise ValueError("invalid_user_id")
        subject_ref = "discord_user:%s" % user

    table_exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (SOURCE_TABLE,),
    ).fetchone()
    if not table_exists:
        return 0

    conn.execute("DROP TRIGGER IF EXISTS %s" % DELETE_TRIGGER)
    try:
        if subject_ref:
            cursor = conn.execute(
                "DELETE FROM bnl_journal_source_events "
                "WHERE guild_id=? AND source_kind='discord_message' AND subject_ref=?",
                (guild, subject_ref),
            )
        else:
            cursor = conn.execute(
                "DELETE FROM bnl_journal_source_events "
                "WHERE guild_id=? AND source_kind='discord_message'",
                (guild,),
            )
        return int(cursor.rowcount or 0)
    finally:
        _create_delete_trigger(conn)


def purge_user_discord_sources_on_connection(conn: sqlite3.Connection, guild_id: int, user_id: int) -> int:
    """Purge one user's raw Discord Journal sources in a caller transaction."""
    return _purge_discord_source_events_on_connection(conn, guild_id, user_id)


def purge_guild_discord_sources_on_connection(conn: sqlite3.Connection, guild_id: int) -> int:
    """Purge one guild's raw Discord Journal sources in a caller transaction."""
    return _purge_discord_source_events_on_connection(conn, guild_id)


def _purge_discord_source_events(db_path: str, guild_id: int, user_id: Optional[int] = None) -> int:
    """Delete an explicitly scoped Discord archive while keeping normal rows immutable.

    The exclusive transaction makes the temporary trigger removal invisible to
    concurrent writers. If any statement fails, SQLite rolls the schema change
    and deletion back together, so the immutability guard remains installed.
    """
    ensure_schema(db_path)
    with sqlite3.connect(db_path, timeout=30) as conn:
        conn.execute("BEGIN EXCLUSIVE")
        try:
            removed = _purge_discord_source_events_on_connection(conn, guild_id, user_id)
            conn.commit()
            return removed
        except Exception:
            conn.rollback()
            raise


def purge_guild_discord_sources(db_path: str, guild_id: int) -> int:
    """Purge durable Discord source rows for one guild, preserving relay history."""
    return _purge_discord_source_events(db_path, guild_id)


def purge_user_discord_sources(db_path: str, guild_id: int, user_id: int) -> int:
    """Purge durable Discord source rows authored by one user in one guild."""
    return _purge_discord_source_events(db_path, guild_id, user_id)


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return bool(conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone())


def _columns(conn: sqlite3.Connection, table: str) -> set:
    return {str(row[1]) for row in conn.execute("PRAGMA table_info(%s)" % table).fetchall()}


def _result_tally(result: SourceRecordResult, totals: Dict[str, int]) -> None:
    if result.status == "inserted":
        totals["inserted"] += 1
    elif result.status == "idempotent":
        totals["idempotent"] += 1
    elif result.status == "conflict":
        totals["conflicts"] += 1
    else:
        totals["skipped"] += 1


def backfill_legacy_sources(db_path: str, guild_id: int) -> LegacyBackfillResult:
    """Idempotently import all currently retained eligible legacy source rows."""
    ensure_schema(db_path)
    totals = {
        "inserted": 0,
        "idempotent": 0,
        "conflicts": 0,
        "skipped": 0,
        "relay_rows_scanned": 0,
        "conversation_rows_scanned": 0,
        "invalid_timestamps": 0,
    }
    with sqlite3.connect(db_path, timeout=30) as conn:
        conn.execute("BEGIN IMMEDIATE")
        now_ms = _now_ms()
        conn.execute(
            "INSERT OR IGNORE INTO bnl_journal_source_archive_state(guild_id,activated_at_ms,created_at_ms) VALUES(?,?,?)",
            (int(guild_id), now_ms, now_ms),
        )
        state = conn.execute(
            "SELECT legacy_conversation_cursor,legacy_relay_cursor_rowid,legacy_relay_cursor_timestamp,legacy_relay_cursor_id "
            "FROM bnl_journal_source_archive_state WHERE guild_id=?",
            (int(guild_id),),
        ).fetchone()
        conversation_cursor = int(state[0] or 0)
        relay_cursor_rowid = int(state[1] or 0)
        relay_cursor_timestamp = str(state[2] or "")
        relay_cursor_id = str(state[3] or "")
        if _table_exists(conn, "website_relay_history"):
            relay_cols = _columns(conn, "website_relay_history")
            required = {"relay_id", "guild_id", "public_message", "public_directive", "published_timestamp"}
            if required.issubset(relay_cols):
                optional = [name for name in ("event_type", "mode", "relay_lane") if name in relay_cols]
                select_names = ["legacy_rowid", "relay_id", "public_message", "public_directive", "published_timestamp"] + optional
                select_sql = ["rowid"] + select_names[1:]
                rows = conn.execute(
                    "SELECT %s FROM website_relay_history WHERE guild_id=? AND rowid>? "
                    "ORDER BY rowid ASC" % ",".join(select_sql),
                    (int(guild_id), relay_cursor_rowid),
                ).fetchall()
                totals["relay_rows_scanned"] += len(rows)
                for row in rows:
                    values = dict(zip(select_names, row))
                    occurred = timestamp_to_epoch_ms(values.get("published_timestamp"))
                    relay_id = str(values.get("relay_id") or "").strip()
                    if occurred is None or not relay_id:
                        totals["invalid_timestamps"] += 1 if occurred is None else 0
                        totals["skipped"] += 1
                        continue
                    message = str(values.get("public_message") or "").strip()
                    directive = str(values.get("public_directive") or "").strip()
                    raw = message + (("\n" + directive) if directive else "")
                    metadata = {"legacyTable": "website_relay_history"}
                    for name in optional:
                        if values.get(name) not in (None, ""):
                            metadata[name] = values[name]
                    result = _record_on_connection(
                        conn,
                        guild_id=guild_id,
                        source_kind="website_relay",
                        source_key=relay_id,
                        occurred_at_ms=occurred,
                        raw_text=raw,
                        sanitized_summary=sanitize_summary(raw),
                        channel_policy="public_relay",
                        subject_ref="bnl_01",
                        private_display_name="BNL-01",
                        public_usable=True,
                        metadata=metadata,
                    )
                    _result_tally(result, totals)
                if rows:
                    last_values = dict(zip(select_names, rows[-1]))
                    relay_cursor_rowid = int(last_values.get("legacy_rowid") or relay_cursor_rowid)
                    relay_cursor_timestamp = str(last_values.get("published_timestamp") or relay_cursor_timestamp)
                    relay_cursor_id = str(last_values.get("relay_id") or relay_cursor_id)

        if _table_exists(conn, "conversations"):
            cols = _columns(conn, "conversations")
            required = {"id", "user_id", "user_name", "guild_id", "channel_policy", "content", "timestamp"}
            if required.issubset(cols):
                expressions = [
                    "id",
                    "user_id",
                    "user_name",
                    "channel_policy",
                    "content",
                    "timestamp",
                    "message_id" if "message_id" in cols else "NULL AS message_id",
                    "channel_id" if "channel_id" in cols else "NULL AS channel_id",
                    "channel_name" if "channel_name" in cols else "'' AS channel_name",
                ]
                predicates = [
                    "guild_id=?",
                    "id>?",
                    "channel_policy IN (?,?,?)",
                ]
                args: List[Any] = [int(guild_id), conversation_cursor, *PUBLIC_POLICIES]
                if "role" in cols:
                    predicates.append("role='user'")
                if "public_usable" in cols:
                    predicates.append("public_usable=1")
                if "visibility" in cols:
                    predicates.append("visibility IN ('public','public_safe')")
                rows = conn.execute(
                    "SELECT %s FROM conversations WHERE %s ORDER BY timestamp ASC, id ASC"
                    % (",".join(expressions), " AND ".join(predicates)),
                    tuple(args),
                ).fetchall()
                totals["conversation_rows_scanned"] += len(rows)
                names = [str(row[2] or "").strip() for row in rows if str(row[2] or "").strip()]
                for row in rows:
                    row_id, user_id, user_name, policy, content, timestamp, message_id, channel_id, channel_name = row
                    occurred = timestamp_to_epoch_ms(timestamp)
                    if occurred is None:
                        totals["invalid_timestamps"] += 1
                        totals["skipped"] += 1
                        continue
                    try:
                        message_id_text = str(message_id or "").strip()
                        real_message_id = int(message_id_text) if re.fullmatch(r"\d+", message_id_text) else 0
                        if real_message_id <= 0:
                            real_message_id = 0
                    except (TypeError, ValueError, OverflowError):
                        real_message_id = 0
                    source_key = str(real_message_id) if real_message_id else "legacy_row:%s" % int(row_id)
                    raw = str(content or "")
                    result = _record_on_connection(
                        conn,
                        guild_id=guild_id,
                        source_kind="discord_message",
                        source_key=source_key,
                        occurred_at_ms=occurred,
                        raw_text=raw,
                        sanitized_summary=sanitize_summary(raw, names),
                        channel_id=int(channel_id) if channel_id not in (None, "") else None,
                        channel_policy=str(policy or ""),
                        subject_ref="discord_user:%s" % int(user_id or 0),
                        private_display_name=str(user_name or ""),
                        public_usable=True,
                        metadata={
                            "legacyTable": "conversations",
                            "legacyRowId": int(row_id),
                            "legacyMessageId": real_message_id or None,
                            "channelName": str(channel_name or ""),
                        },
                    )
                    _result_tally(result, totals)
                if rows:
                    conversation_cursor = max(int(row[0]) for row in rows)
        conn.execute(
            """UPDATE bnl_journal_source_archive_state SET
                legacy_backfill_completed_at_ms=?,legacy_conversation_cursor=?,
                legacy_relay_cursor_rowid=?,legacy_relay_cursor_timestamp=?,legacy_relay_cursor_id=?
                WHERE guild_id=?""",
            (_now_ms(), conversation_cursor, relay_cursor_rowid, relay_cursor_timestamp, relay_cursor_id, int(guild_id)),
        )
    return LegacyBackfillResult(**totals)
