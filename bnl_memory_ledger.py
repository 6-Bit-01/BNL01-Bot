"""Unified Memory Ledger v1 shadow schema and write adapters.

The ledger is append-oriented shadow infrastructure only. Legacy memory remains the
production source of truth; this module never participates in prompt assembly or
retrieval.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
import os
import re
import sqlite3
from typing import Any

from bnl_canon_source_contract import (
    Confidence,
    SourceClass,
    SourceClaim,
    SubjectIdentity,
    Visibility,
    has_explicit_channel_policy_mapping,
    has_explicit_route_source_mapping,
    is_public_usable,
    map_channel_policy_visibility,
    map_route_source_label,
)

MEMORY_LEDGER_SCHEMA_VERSION = "memory_ledger_v1"
MEMORY_LEDGER_SHADOW_ENV = "BNL_MEMORY_LEDGER_SHADOW_ENABLED"
BNL_SUBJECT_KEY = "bnl_01"

ENTRY_TYPES = frozenset({
    "observation", "claim", "event", "preference", "boundary", "goal", "open_loop", "commitment",
    "shared_moment", "relationship_event", "canon_reference", "show_event", "unresolved_question", "derived_summary",
})
LINEAGE_TYPES = frozenset({"derived_from", "correction_of", "supersedes", "retracts", "duplicate_of", "part_of_moment"})
OUTCOMES = frozenset({"inserted", "deduplicated", "skipped", "error"})
ACTIVE_LIFECYCLE = "active"
REVIEW_ONLY_LIFECYCLE = "review_only"
RESOLVED_LIFECYCLE = "resolved"
REJECTED_LIFECYCLE = "rejected"

_NUMBER_REMEMBER_RE = re.compile(r"\bremember\b[^\n]{0,80}?\bnumber\b\D+(\d{1,12})(?!\d)", re.I)
_REPLACE_NUMBER_RE = re.compile(r"\b(?:replace|supersede)\s+(\d{1,12})\s+\bwith\b\s+(\d{1,12})(?!\d)", re.I)
_CORRECTION_NUMBER_RE = re.compile(r"\b(?:correction|actually|correcting|i meant)\b[^\n]{0,80}?\b(?:number\s+)?(?:is|was)?\s*(\d{1,12})\s*,?\s+not\s+(\d{1,12})(?!\d)", re.I)
_QUEUE_OPERATIONAL_RE = re.compile(
    r"\b(?:website|site|current|live)\s+queue\s+(?:is|was|currently|status|open|closed|active|available|full)\b|\bqueue\s+(?:status|open|closed)\b"
    r"|\bqueue\s+(?:is\s+)?currently\s+(?:open|closed|active|available|full)\b"
    r"|\bpayment\s+(?:cleared|pending|paid|failed|confirmed)\b"
    r"|\bnow\s*playing\b|\bup\s*next\b|\bcurrent\s+session\b|\bavailability\b"
    r"|\bpriority\s+(?:signal|slot|enabled|upgrade)\b|\bwheel\s+(?:spin|owed|enabled)\b"
    r"|\bqueue[-_ ]derived\s+artist\b|\bqueueOpen\b|\bcurrentTrack\b|\bupNext\b|\bqueuedTracks\b",
    re.I,
)

@dataclass(frozen=True)
class LedgerWriteResult:
    entry_id: str = ""
    outcome: str = "skipped"
    reason_code: str = "not_attempted"
    source_table: str = ""
    source_row_id: str = ""
    source_revision: str = ""
    source_event_key: str = ""
    guild_id: int = 0

    def __post_init__(self):
        if self.outcome not in OUTCOMES:
            object.__setattr__(self, "outcome", "error")

    def __str__(self) -> str:
        return self.entry_id

    def __bool__(self) -> bool:
        return self.outcome in {"inserted", "deduplicated"} and bool(self.entry_id)


def shadow_enabled(environ: dict[str, str] | None = None) -> bool:
    value = (environ or os.environ).get(MEMORY_LEDGER_SHADOW_ENV, "")
    return str(value).strip().lower() in {"1", "true", "yes", "on", "enabled"}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _canon(value: Any) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value).strip().lower())


def subject_key_for_user(user_id: int | str | None) -> str:
    return f"discord_user:{int(user_id or 0)}"


def source_revision_for(row_id: int | str, updated_at: str | None = None, event: str | None = None) -> str:
    if event:
        return f"event:{_canon(event)}"
    if updated_at:
        return f"rev:{row_id}:{_canon(updated_at)}"
    return str(row_id or "0")


def stable_entry_id(*, guild_id: int | str | None, source_table: str, source_row_id: int | str, entry_type: str, subject_key: str, predicate_key: str, source_revision: str = "") -> str:
    parts = [MEMORY_LEDGER_SCHEMA_VERSION, str(guild_id or 0), _canon(source_table), str(source_row_id), _canon(source_revision or source_row_id), _canon(entry_type), _canon(subject_key), _canon(predicate_key)]
    return "mle_" + hashlib.sha256("\x1f".join(parts).encode("utf-8")).hexdigest()[:40]


@dataclass(frozen=True)
class LedgerParticipant:
    participant_key: str
    display_name: str = ""
    role: str = "participant"
    order_index: int = 0


@dataclass(frozen=True)
class LedgerEntry:
    guild_id: int
    source_table: str
    source_row_id: int | str
    source_role: str
    entry_type: str
    subject_key: str
    subject_display_name: str = ""
    predicate_key: str = "conversation"
    value: str = ""
    source_class: SourceClass = SourceClass.LEGACY_SOURCE_BLIND
    route_mode: str = "unknown"
    channel_id: int = 0
    channel_name: str = ""
    channel_policy: str = "unknown"
    source_message_id: int | None = None
    source_revision: str = ""
    source_event_key: str = ""
    visibility: Visibility = Visibility.UNKNOWN
    confidence: Confidence = Confidence.UNKNOWN
    public_usable: bool = False
    derived: bool = False
    projection: bool = False
    salience: float = 0.0
    observed_at: str = ""
    source_sequence: int | None = None
    valid_from: str = ""
    valid_until: str = ""
    freshness: str = ""
    lifecycle_status: str = ACTIVE_LIFECYCLE
    participants: tuple[LedgerParticipant, ...] = field(default_factory=tuple)
    lineage: tuple[tuple[str, str], ...] = field(default_factory=tuple)

    @property
    def entry_id(self) -> str:
        return stable_entry_id(guild_id=self.guild_id, source_table=self.source_table, source_row_id=self.source_row_id, entry_type=self.entry_type, subject_key=self.subject_key, predicate_key=self.predicate_key, source_revision=self.source_revision)


def ensure_memory_ledger_schema(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS memory_ledger_entries (
            entry_id TEXT PRIMARY KEY, schema_version TEXT NOT NULL, guild_id INTEGER NOT NULL,
            subject_key TEXT NOT NULL, subject_display_name TEXT, entry_type TEXT NOT NULL,
            predicate_key TEXT NOT NULL, normalized_value TEXT, source_class TEXT NOT NULL,
            source_table TEXT NOT NULL, source_row_id TEXT NOT NULL, source_revision TEXT DEFAULT '', source_event_key TEXT DEFAULT '',
            source_role TEXT NOT NULL, route_mode TEXT, channel_id INTEGER, channel_name TEXT, channel_policy TEXT,
            source_message_id INTEGER, visibility TEXT NOT NULL, confidence TEXT NOT NULL,
            public_usable INTEGER DEFAULT 0, derived INTEGER DEFAULT 0, projection INTEGER DEFAULT 0,
            salience REAL DEFAULT 0.0, observed_at TEXT, source_sequence INTEGER,
            valid_from TEXT, valid_until TEXT, freshness TEXT, lifecycle_status TEXT NOT NULL,
            created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
            UNIQUE(schema_version, guild_id, source_table, source_row_id, source_revision, entry_type, subject_key, predicate_key)
        )
    """)
    for sql in (
        "ALTER TABLE memory_ledger_entries ADD COLUMN source_revision TEXT DEFAULT ''",
        "ALTER TABLE memory_ledger_entries ADD COLUMN source_event_key TEXT DEFAULT ''",
    ):
        try:
            cur.execute(sql)
        except sqlite3.OperationalError:
            pass
    cur.execute("""
        CREATE TABLE IF NOT EXISTS memory_ledger_lineage (
            entry_id TEXT NOT NULL, guild_id INTEGER NOT NULL DEFAULT 0, lineage_type TEXT NOT NULL, target_entry_id TEXT NOT NULL,
            created_at TEXT NOT NULL, PRIMARY KEY(entry_id, lineage_type, target_entry_id)
        )
    """)
    try:
        cur.execute("ALTER TABLE memory_ledger_lineage ADD COLUMN guild_id INTEGER NOT NULL DEFAULT 0")
    except sqlite3.OperationalError:
        pass
    cur.execute("""
        CREATE TABLE IF NOT EXISTS memory_ledger_participants (
            entry_id TEXT NOT NULL, guild_id INTEGER NOT NULL, participant_key TEXT NOT NULL,
            display_name TEXT, participant_role TEXT, order_index INTEGER DEFAULT 0, created_at TEXT NOT NULL,
            PRIMARY KEY(entry_id, participant_key, participant_role)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS memory_ledger_shadow_receipts (
            id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id INTEGER NOT NULL, writer TEXT NOT NULL,
            source_table TEXT NOT NULL, source_row_id TEXT NOT NULL, source_revision TEXT DEFAULT '', source_event_key TEXT DEFAULT '',
            attempted_at TEXT NOT NULL, outcome TEXT NOT NULL, reason_code TEXT NOT NULL, entry_id TEXT DEFAULT ''
        )
    """)
    for sql in [
        "CREATE INDEX IF NOT EXISTS idx_mle_guild ON memory_ledger_entries(guild_id)",
        "CREATE INDEX IF NOT EXISTS idx_mle_subject ON memory_ledger_entries(guild_id, subject_key)",
        "CREATE INDEX IF NOT EXISTS idx_mle_type ON memory_ledger_entries(guild_id, entry_type)",
        "CREATE INDEX IF NOT EXISTS idx_mle_source ON memory_ledger_entries(guild_id, source_table, source_row_id, source_revision)",
        "CREATE INDEX IF NOT EXISTS idx_mle_lifecycle ON memory_ledger_entries(guild_id, lifecycle_status)",
        "CREATE INDEX IF NOT EXISTS idx_mle_visibility ON memory_ledger_entries(guild_id, visibility)",
        "CREATE INDEX IF NOT EXISTS idx_mle_predicate ON memory_ledger_entries(guild_id, predicate_key)",
        "CREATE INDEX IF NOT EXISTS idx_mle_observed ON memory_ledger_entries(guild_id, observed_at)",
        "CREATE INDEX IF NOT EXISTS idx_mll_guild ON memory_ledger_lineage(guild_id, lineage_type, target_entry_id)",
        "CREATE INDEX IF NOT EXISTS idx_mlp_participant ON memory_ledger_participants(guild_id, participant_key, order_index)",
        "CREATE INDEX IF NOT EXISTS idx_mlr_guild ON memory_ledger_shadow_receipts(guild_id, writer, outcome, reason_code)",
    ]:
        cur.execute(sql)


def record_shadow_receipt(conn: sqlite3.Connection, *, guild_id: int, writer: str, source_table: str, source_row_id: int | str, source_revision: str = "", source_event_key: str = "", outcome: str, reason_code: str, entry_id: str = "") -> None:
    ensure_memory_ledger_schema(conn)
    conn.execute(
        "INSERT INTO memory_ledger_shadow_receipts (guild_id, writer, source_table, source_row_id, source_revision, source_event_key, attempted_at, outcome, reason_code, entry_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (int(guild_id or 0), (writer or "unknown")[:80], (source_table or "unknown")[:80], str(source_row_id or ""), (source_revision or "")[:160], (source_event_key or "")[:160], _now(), outcome if outcome in OUTCOMES else "error", (reason_code or "unknown")[:120], entry_id or ""),
    )


def skipped_result(*, guild_id: int, source_table: str, source_row_id: int | str, reason_code: str, source_revision: str = "", source_event_key: str = "") -> LedgerWriteResult:
    return LedgerWriteResult(outcome="skipped", reason_code=reason_code, source_table=source_table, source_row_id=str(source_row_id), source_revision=source_revision, source_event_key=source_event_key, guild_id=int(guild_id or 0))


def insert_ledger_entry(conn: sqlite3.Connection, entry: LedgerEntry) -> LedgerWriteResult:
    if entry.entry_type not in ENTRY_TYPES:
        return LedgerWriteResult(outcome="error", reason_code="unsupported_entry_type", source_table=entry.source_table, source_row_id=str(entry.source_row_id), source_revision=entry.source_revision, source_event_key=entry.source_event_key, guild_id=entry.guild_id)
    ensure_memory_ledger_schema(conn)
    now = _now()
    cur = conn.cursor()
    cur.execute("""
        INSERT OR IGNORE INTO memory_ledger_entries (
            entry_id, schema_version, guild_id, subject_key, subject_display_name, entry_type, predicate_key,
            normalized_value, source_class, source_table, source_row_id, source_revision, source_event_key, source_role, route_mode, channel_id,
            channel_name, channel_policy, source_message_id, visibility, confidence, public_usable, derived,
            projection, salience, observed_at, source_sequence, valid_from, valid_until, freshness,
            lifecycle_status, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (entry.entry_id, MEMORY_LEDGER_SCHEMA_VERSION, entry.guild_id, entry.subject_key, entry.subject_display_name, entry.entry_type, entry.predicate_key, entry.value[:1000], entry.source_class.value, entry.source_table, str(entry.source_row_id), entry.source_revision, entry.source_event_key, entry.source_role, entry.route_mode, int(entry.channel_id or 0), entry.channel_name[:120], entry.channel_policy[:80], entry.source_message_id, entry.visibility.value, entry.confidence.value, 1 if entry.public_usable else 0, 1 if entry.derived else 0, 1 if entry.projection else 0, float(entry.salience or 0.0), entry.observed_at, entry.source_sequence, entry.valid_from, entry.valid_until, entry.freshness, entry.lifecycle_status, now, now))
    outcome = "inserted" if cur.rowcount else "deduplicated"
    if outcome == "inserted":
        for idx, p in enumerate(sorted(entry.participants, key=lambda x: (x.order_index, x.participant_key))):
            cur.execute("INSERT OR IGNORE INTO memory_ledger_participants VALUES (?, ?, ?, ?, ?, ?, ?)", (entry.entry_id, entry.guild_id, p.participant_key, p.display_name[:120], p.role[:40], idx, now))
        for lineage_type, target in entry.lineage:
            if lineage_type in LINEAGE_TYPES and target:
                cur.execute("INSERT OR IGNORE INTO memory_ledger_lineage VALUES (?, ?, ?, ?, ?)", (entry.entry_id, entry.guild_id, lineage_type, target, now))
    return LedgerWriteResult(entry.entry_id, outcome, "ok" if outcome == "inserted" else "exact_source_duplicate", entry.source_table, str(entry.source_row_id), entry.source_revision, entry.source_event_key, entry.guild_id)


def _visibility(policy: str) -> Visibility:
    return map_channel_policy_visibility(policy) if has_explicit_channel_policy_mapping(policy) else Visibility.UNKNOWN


def _source_class(route: str, fallback: SourceClass) -> SourceClass:
    return map_route_source_label(route) if has_explicit_route_source_mapping(route) else fallback


def _public_ok(subject_key: str, predicate: str, value: str, source_class: SourceClass, visibility: Visibility, confidence: Confidence, *, valid: bool = True, projection: bool = False) -> bool:
    claim = SourceClaim(stable_entry_id(guild_id=0, source_table="eval", source_row_id=0, entry_type="claim", subject_key=subject_key, predicate_key=predicate), SubjectIdentity(subject_key, subject_key), predicate, value, source_class, visibility, confidence, valid=valid, projection=projection)
    return is_public_usable(claim)


def is_queue_operational_state(text: str) -> bool:
    return bool(_QUEUE_OPERATIONAL_RE.search(text or ""))


def _unique_active_value_target(conn: sqlite3.Connection, *, guild_id: int, subject_key: str, predicate_key: str, old_value: str) -> str:
    ensure_memory_ledger_schema(conn)
    rows = conn.execute(
        """
        SELECT entry_id FROM memory_ledger_entries
        WHERE guild_id=? AND subject_key=? AND predicate_key=? AND normalized_value=? AND lifecycle_status='active'
          AND entry_id NOT IN (SELECT target_entry_id FROM memory_ledger_lineage WHERE guild_id=? AND lineage_type IN ('supersedes','retracts'))
        """,
        (guild_id, subject_key, predicate_key, old_value, guild_id),
    ).fetchall()
    return rows[0][0] if len(rows) == 1 else ""


def _conversation_fact(text: str, conn: sqlite3.Connection, guild_id: int, subject_key: str) -> tuple[str, str, tuple[tuple[str, str], ...], str, str]:
    remember = _NUMBER_REMEMBER_RE.search(text or "")
    if remember:
        return "remembered_number", remember.group(1), (), ACTIVE_LIFECYCLE, "ok"
    replacement = _REPLACE_NUMBER_RE.search(text or "")
    if replacement:
        old, new = replacement.group(1), replacement.group(2)
        target = _unique_active_value_target(conn, guild_id=guild_id, subject_key=subject_key, predicate_key="remembered_number", old_value=old)
        if target:
            return "remembered_number", new, (("supersedes", target), ("correction_of", target)), ACTIVE_LIFECYCLE, "explicit_correction_linked"
        return "remembered_number", new, (), REVIEW_ONLY_LIFECYCLE, "unresolved_correction_target"
    correction = _CORRECTION_NUMBER_RE.search(text or "")
    if correction:
        new, old = correction.group(1), correction.group(2)
        target = _unique_active_value_target(conn, guild_id=guild_id, subject_key=subject_key, predicate_key="remembered_number", old_value=old)
        if target:
            return "remembered_number", new, (("correction_of", target),), ACTIVE_LIFECYCLE, "explicit_correction_linked"
        return "remembered_number", new, (), REVIEW_ONLY_LIFECYCLE, "unresolved_correction_target"
    if "?" in (text or ""):
        return "question", "", (), ACTIVE_LIFECYCLE, "ok"
    return "conversation", (text or "")[:500], (), ACTIVE_LIFECYCLE, "ok"


def shadow_conversation_row(conn: sqlite3.Connection, *, row_id: int, user_id: int, user_name: str, guild_id: int, role: str, content: str, channel_name: str = "", channel_policy: str = "unknown", channel_id: int = 0, message_id: int | None = None, route_mode: str = "unknown", observed_at: str = "") -> LedgerWriteResult:
    role_norm = (role or "").lower()
    if role_norm == "user" and is_queue_operational_state(content):
        return skipped_result(guild_id=guild_id, source_table="conversations", source_row_id=row_id, reason_code="queue_operational_state_excluded", source_revision=str(row_id))
    visibility = _visibility(channel_policy)
    if role_norm != "user":
        subject_key = BNL_SUBJECT_KEY
        participants = (LedgerParticipant(BNL_SUBJECT_KEY, "BNL-01", "author", 0), LedgerParticipant(subject_key_for_user(user_id), user_name or "", "conversation_target", 1))
        entry = LedgerEntry(guild_id=guild_id, source_table="conversations", source_row_id=row_id, source_revision=str(row_id), source_role="model", entry_type="derived_summary", subject_key=BNL_SUBJECT_KEY, subject_display_name="BNL-01", predicate_key="model_output", value=(content or "")[:500], source_class=SourceClass.DERIVED_SUMMARY, route_mode=route_mode, channel_id=channel_id, channel_name=channel_name, channel_policy=channel_policy, source_message_id=message_id, visibility=visibility, confidence=Confidence.LOW, public_usable=False, derived=True, projection=True, salience=0.1, observed_at=observed_at or _now(), source_sequence=row_id, participants=participants)
        return insert_ledger_entry(conn, entry)
    subject_key = subject_key_for_user(user_id)
    predicate, value, lineage, lifecycle, reason = _conversation_fact(content, conn, guild_id, subject_key)
    source_class = _source_class("conversation_continuity", SourceClass.PUBLIC_OBSERVATION)
    public_ok = _public_ok(subject_key, predicate, value, source_class, visibility, Confidence.MEDIUM) if lifecycle == ACTIVE_LIFECYCLE else False
    result = insert_ledger_entry(conn, LedgerEntry(guild_id=guild_id, source_table="conversations", source_row_id=row_id, source_revision=str(row_id), source_role="user", entry_type="observation", subject_key=subject_key, subject_display_name=user_name or "", predicate_key=predicate, value=value, source_class=source_class, route_mode=route_mode, channel_id=channel_id, channel_name=channel_name, channel_policy=channel_policy, source_message_id=message_id, visibility=visibility, confidence=Confidence.MEDIUM, public_usable=public_ok, salience=0.2, observed_at=observed_at or _now(), source_sequence=row_id, lifecycle_status=lifecycle, participants=(LedgerParticipant(subject_key, user_name or "", "author", 0),), lineage=lineage))
    if reason != "ok" and result.outcome == "inserted":
        return LedgerWriteResult(result.entry_id, result.outcome, reason, result.source_table, result.source_row_id, result.source_revision, result.source_event_key, result.guild_id)
    return result


def shadow_user_fact_row(conn: sqlite3.Connection, *, row_id: int, user_id: int, guild_id: int, fact_key: str, fact_value: str, confidence: float = 0.7, updated_at: str = "") -> LedgerWriteResult:
    rev = source_revision_for(row_id, updated_at)
    return insert_ledger_entry(conn, LedgerEntry(guild_id=guild_id, source_table="user_memory_facts", source_row_id=row_id, source_revision=rev, source_role="legacy_source_blind", entry_type="claim", subject_key=subject_key_for_user(user_id), predicate_key=fact_key or "legacy_fact", value=(fact_value or "")[:500], source_class=SourceClass.LEGACY_SOURCE_BLIND, visibility=Visibility.PRIVATE, confidence=Confidence.LOW, public_usable=False, observed_at=updated_at or _now(), source_sequence=int(row_id or 0), lifecycle_status=REVIEW_ONLY_LIFECYCLE, participants=(LedgerParticipant(subject_key_for_user(user_id), "", "subject", 0),)))


def shadow_memory_tier_row(conn: sqlite3.Connection, *, row_id: int, user_id: int, guild_id: int, tier: str, summary: str, salience: float = 0.5, channel_policy: str = "legacy_unknown", topic_key: str = "", updated_at: str = "", derived_from_entry_ids: tuple[str, ...] = ()) -> LedgerWriteResult:
    rev = source_revision_for(row_id, updated_at)
    lineage = tuple(("derived_from", eid) for eid in sorted(set(derived_from_entry_ids)) if eid)
    return insert_ledger_entry(conn, LedgerEntry(guild_id=guild_id, source_table="memory_tiers", source_row_id=row_id, source_revision=rev, source_role="derived_projection", entry_type="derived_summary", subject_key=subject_key_for_user(user_id), predicate_key=topic_key or f"memory_tier:{tier}", value=(summary or "")[:500], source_class=SourceClass.DERIVED_SUMMARY, visibility=Visibility.PRIVATE, confidence=Confidence.LOW, public_usable=False, derived=True, projection=True, salience=salience, observed_at=updated_at or _now(), source_sequence=int(row_id or 0), lifecycle_status=REVIEW_ONLY_LIFECYCLE, participants=(LedgerParticipant(subject_key_for_user(user_id), "", "subject", 0),), lineage=lineage))


def shadow_relationship_journal_row(conn: sqlite3.Connection, *, row_id: int, user_id: int, guild_id: int, entry_type: str, summary: str, timestamp: str = "") -> LedgerWriteResult:
    return insert_ledger_entry(conn, LedgerEntry(guild_id=guild_id, source_table="relationship_journal", source_row_id=row_id, source_revision=str(row_id), source_role="internal", entry_type="relationship_event", subject_key=subject_key_for_user(user_id), predicate_key=entry_type or "relationship_event", value=(summary or "")[:500], source_class=SourceClass.DERIVED_SUMMARY, visibility=Visibility.INTERNAL, confidence=Confidence.LOW, public_usable=False, derived=True, projection=True, salience=0.3, observed_at=timestamp or _now(), source_sequence=int(row_id or 0), lifecycle_status=REVIEW_ONLY_LIFECYCLE, participants=(LedgerParticipant(subject_key_for_user(user_id), "", "subject", 0),)))


def shadow_broadcast_memory_row(conn: sqlite3.Connection, *, row_id: int, guild_id: int, cleaned_summary: str, entry_type: str, public_safe: bool, status: str, usage_scope: str, submitted_by_user_id: int | None = None, submitted_by_name: str = "", created_at: str = "", updated_at: str = "", supersedes_id: int | None = None) -> LedgerWriteResult:
    if not cleaned_summary:
        return skipped_result(guild_id=guild_id, source_table="broadcast_memory", source_row_id=row_id, reason_code="empty_cleaned_summary", source_revision=source_revision_for(row_id, updated_at or created_at))
    lifecycle = ACTIVE_LIFECYCLE if str(status or "active").lower() == "active" else (RESOLVED_LIFECYCLE if str(status or "").lower() == "resolved" else REVIEW_ONLY_LIFECYCLE)
    public_ok = bool(public_safe and lifecycle == ACTIVE_LIFECYCLE and any(scope in {"public", "public_context", "recap", "show", "website", "broadcast", "ambient", "direct", "relay"} for scope in re.split(r"[,\s]+", usage_scope or "")))
    target = ""
    if supersedes_id:
        target = stable_entry_id(guild_id=guild_id, source_table="broadcast_memory", source_row_id=supersedes_id, source_revision=str(supersedes_id), entry_type="show_event" if "show" in (entry_type or "") else "event", subject_key="barcode_radio", predicate_key=entry_type or "broadcast_memory")
    lineage = (("supersedes", target), ("correction_of", target)) if target else ()
    rev = source_revision_for(row_id, updated_at or created_at)
    return insert_ledger_entry(conn, LedgerEntry(guild_id=guild_id, source_table="broadcast_memory", source_row_id=row_id, source_revision=rev, source_role="broadcast_memory", entry_type="show_event" if "show" in (entry_type or "") else "event", subject_key="barcode_radio", subject_display_name="BARCODE Radio", predicate_key=entry_type or "broadcast_memory", value=cleaned_summary[:500], source_class=SourceClass.FIRST_PARTY_RECORD, visibility=Visibility.PUBLIC_SAFE if public_ok else Visibility.INTERNAL, confidence=Confidence.HIGH if public_ok else Confidence.MEDIUM, public_usable=public_ok, salience=0.5, observed_at=created_at or updated_at or _now(), source_sequence=int(row_id or 0), freshness=usage_scope or "", lifecycle_status=lifecycle, participants=tuple([LedgerParticipant(f"discord_user:{submitted_by_user_id}", submitted_by_name or "", "submitter", 0)] if submitted_by_user_id else ()), lineage=lineage))


def shadow_broadcast_status_event(conn: sqlite3.Connection, *, row_id: int, guild_id: int, status: str, updated_at: str, actor_id: int | None = None, actor_name: str = "", superseded_by_id: int | None = None) -> LedgerWriteResult:
    rev = source_revision_for(row_id, updated_at, event=f"status:{status}:{updated_at}")
    target = stable_entry_id(guild_id=guild_id, source_table="broadcast_memory", source_row_id=row_id, source_revision=str(row_id), entry_type="event", subject_key="barcode_radio", predicate_key="broadcast_status")
    lineage = ()
    lifecycle = RESOLVED_LIFECYCLE if status == "resolved" else REVIEW_ONLY_LIFECYCLE
    predicate = f"broadcast_status:{status or 'unknown'}"
    if status == "superseded" and superseded_by_id:
        lifecycle = REVIEW_ONLY_LIFECYCLE
    return insert_ledger_entry(conn, LedgerEntry(guild_id=guild_id, source_table="broadcast_memory", source_row_id=row_id, source_revision=rev, source_event_key=f"status:{status}", source_role="broadcast_memory_status", entry_type="event", subject_key="barcode_radio", subject_display_name="BARCODE Radio", predicate_key=predicate, value=status or "unknown", source_class=SourceClass.FIRST_PARTY_RECORD, visibility=Visibility.INTERNAL, confidence=Confidence.HIGH, public_usable=False, observed_at=updated_at or _now(), source_sequence=int(row_id or 0), lifecycle_status=lifecycle, participants=tuple([LedgerParticipant(f"discord_user:{actor_id}", actor_name or "", "correction_actor", 0)] if actor_id else ()), lineage=lineage))


def shadow_canon_reference(conn: sqlite3.Connection, *, guild_id: int, canon_id: str, subject_key: str, subject_display_name: str, predicate_key: str, value: str, observed_at: str = "") -> LedgerWriteResult:
    if not canon_id or not subject_key or not predicate_key:
        return skipped_result(guild_id=guild_id, source_table="approved_canon", source_row_id=canon_id or "", reason_code="missing_canon_source_identity")
    return insert_ledger_entry(conn, LedgerEntry(guild_id=guild_id, source_table="approved_canon", source_row_id=canon_id, source_revision=str(canon_id), source_role="approved_canon", entry_type="canon_reference", subject_key=subject_key, subject_display_name=subject_display_name, predicate_key=predicate_key, value=(value or "")[:500], source_class=SourceClass.APPROVED_CANON, visibility=Visibility.REFERENCE_CANON, confidence=Confidence.APPROVED, public_usable=True, observed_at=observed_at or _now(), lifecycle_status=ACTIVE_LIFECYCLE))


def build_memory_ledger_evaluation(conn: sqlite3.Connection, *, guild_id: int | None = None) -> dict[str, Any]:
    ensure_memory_ledger_schema(conn)
    params: list[Any] = []
    where = ""
    if guild_id is not None:
        where = " WHERE guild_id=?"
        params.append(guild_id)
    cur = conn.cursor()
    report: dict[str, Any] = {"schemaVersion": MEMORY_LEDGER_SCHEMA_VERSION}
    cur.execute(f"SELECT COUNT(*) FROM memory_ledger_shadow_receipts{where}", params)
    report["eligibleLegacyWrites"] = int(cur.fetchone()[0] or 0)
    for outcome, key in (("inserted", "insertedLedgerEntries"), ("deduplicated", "exactSourceDeduplications"), ("error", "shadowWriteErrors")):
        cur.execute(f"SELECT COUNT(*) FROM memory_ledger_shadow_receipts{where + (' AND' if where else ' WHERE')} outcome=?", params + [outcome])
        report[key] = int(cur.fetchone()[0] or 0)
    cur.execute(f"SELECT reason_code, COUNT(*) FROM memory_ledger_shadow_receipts{where + (' AND' if where else ' WHERE')} outcome='skipped' GROUP BY reason_code", params)
    report["skippedWrites"] = dict(cur.fetchall())
    cur.execute(f"SELECT source_table, COUNT(*) FROM memory_ledger_entries{where} GROUP BY source_table", params)
    report["countsBySourceLane"] = dict(cur.fetchall())
    for key, col in (("countsByEntryType", "entry_type"), ("countsByVisibility", "visibility"), ("countsByLifecycle", "lifecycle_status")):
        cur.execute(f"SELECT {col}, COUNT(*) FROM memory_ledger_entries{where} GROUP BY {col}", params)
        report[key] = dict(cur.fetchall())
    cur.execute(f"SELECT COUNT(*) FROM memory_ledger_entries{where + (' AND' if where else ' WHERE')} (source_class='legacy_source_blind' OR visibility='unknown')", params)
    report["missingUnmappedProvenance"] = int(cur.fetchone()[0] or 0)
    cur.execute(f"SELECT COUNT(*) FROM memory_ledger_entries{where + (' AND' if where else ' WHERE')} public_usable=0", params)
    report["publicUsabilityRejections"] = int(cur.fetchone()[0] or 0)
    cur.execute(f"SELECT COUNT(*) FROM memory_ledger_entries{where + (' AND' if where else ' WHERE')} entry_id NOT IN (SELECT target_entry_id FROM memory_ledger_lineage WHERE {'guild_id=? AND ' if guild_id is not None else ''} lineage_type IN ('supersedes','retracts')) AND lifecycle_status='active' GROUP BY subject_key, predicate_key HAVING COUNT(*) > 1", params + ([guild_id] if guild_id is not None else []))
    report["entriesWithMultipleActiveValues"] = len(cur.fetchall())
    cur.execute(f"SELECT lineage_type, COUNT(*) FROM memory_ledger_lineage{where} GROUP BY lineage_type", params)
    lineage_counts = dict(cur.fetchall())
    report["explicitCorrectionCounts"] = int(lineage_counts.get("correction_of", 0) + lineage_counts.get("supersedes", 0))
    cur.execute(f"SELECT COUNT(*) FROM memory_ledger_entries{where + (' AND' if where else ' WHERE')} lifecycle_status='review_only' AND predicate_key='remembered_number'", params)
    report["unresolvedCorrectionAttempts"] = int(cur.fetchone()[0] or 0)
    report["legacyToLedgerParityMismatches"] = 0
    report["implicitSupersessionsBlocked"] = int(report.get("entriesWithMultipleActiveValues", 0))
    return report
