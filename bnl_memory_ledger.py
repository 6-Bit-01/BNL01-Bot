"""Unified Memory Ledger v1 shadow schema and write adapters.

The ledger is append-oriented shadow infrastructure. Legacy memory remains the
default production source of truth; separately gated governance and Moment
adapters may consume only revalidated, route-safe projections.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
import json
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
ATOMIC_KNOWLEDGE_SCHEMA_VERSION = "memory_ledger_atomic_knowledge_v1"
ATOMIC_KNOWLEDGE_BACKFILL = "atomic_knowledge_backfill_v1"
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

KNOWLEDGE_CANDIDATE_TYPES = frozenset(
    {
        "topic_or_motif",
        "person_role_fact",
        "project_event_or_milestone",
        "open_loop_or_question",
        "inference_or_contested_claim",
    }
)
KNOWLEDGE_EPISTEMIC_STATUSES = frozenset(
    {
        "stated",
        "observed",
        "source_abstraction",
        "inference",
        "contested",
        "question",
    }
)
KNOWLEDGE_CURRENTNESS = frozenset(
    {"current", "historical", "open", "unresolved", "uncertain"}
)
KNOWLEDGE_CANDIDATE_STATES = frozenset(
    {"candidate", "contested", "superseded", "invalidated"}
)
KNOWLEDGE_TERMINAL_ROOT_LIFECYCLES = frozenset(
    {
        "corrected",
        "superseded",
        "retracted",
        "expired",
        "quarantined",
        "needs_review",
        "forgotten",
        "deleted",
        "rejected",
        "unresolved",
    }
)
KNOWLEDGE_DERIVATIVE_SOURCE_CLASSES = frozenset(
    {
        SourceClass.DERIVED_SUMMARY.value,
        SourceClass.EVIDENCE_PROJECTION.value,
        SourceClass.SOURCE_FILE_PROJECTION.value,
        SourceClass.DOSSIER_PROJECTION.value,
        SourceClass.ENTITY_EVIDENCE_PROJECTION.value,
        SourceClass.LEGACY_SOURCE_BLIND.value,
    }
)
_KNOWLEDGE_AUTHORITY_RANK = {
    SourceClass.LEGACY_SOURCE_BLIND.value: 0,
    SourceClass.DERIVED_SUMMARY.value: 1,
    SourceClass.ENTITY_EVIDENCE_PROJECTION.value: 1,
    SourceClass.DOSSIER_PROJECTION.value: 1,
    SourceClass.SOURCE_FILE_PROJECTION.value: 1,
    SourceClass.EVIDENCE_PROJECTION.value: 1,
    SourceClass.PUBLIC_OBSERVATION.value: 2,
    SourceClass.RUNTIME_OBSERVATION.value: 3,
    SourceClass.FIRST_PARTY_RECORD.value: 4,
    SourceClass.APPROVED_CANON.value: 5,
    SourceClass.OWNER_CORRECTION.value: 6,
}
_KNOWLEDGE_RESTRICTED_VISIBILITIES = frozenset(
    {
        Visibility.SEALED_TEST.value,
        Visibility.PROTECTED.value,
        Visibility.AI_IMAGE_TOOL.value,
        Visibility.UNKNOWN.value,
    }
)
_KNOWLEDGE_TEST_OR_OPERATIONAL_RE = re.compile(
    r"\b(?:queue|rehearsal|synthetic[-\s]?artist|test[-\s]?payment|"
    r"payment[-\s]?test|queue[-\s]?simulation|simulation[-\s]?queue|"
    r"wheel(?:\s+spin)?|now[-\s]?playing|up[-\s]?next|priority[-\s]?signal|"
    r"show[-\s]?test|showtest)\b",
    re.I,
)
_KNOWLEDGE_TEST_OR_OPERATIONAL_SOURCE_RE = re.compile(
    r"(?:queue|payment|wheel|rehearsal|show[_-]?test|simulation)",
    re.I,
)

APPROVED_SELF_AUTHORED_FACT_KEYS = frozenset({
    "preferred_name",
    "pronouns",
    "favorite_color",
    "favorite_movie",
})
_CONVERSATION_CORRECTION_RE = re.compile(
    r"\b(?:actually|correction|correcting|i meant|instead|not that|"
    r"that's wrong|that is wrong|replace|swap|change)\b",
    re.I,
)
_CORRECTION_TOPIC_STOPWORDS = frozenset(
    {
        "actually",
        "and",
        "change",
        "correcting",
        "correction",
        "for",
        "from",
        "instead",
        "into",
        "meant",
        "not",
        "replace",
        "swap",
        "that",
        "the",
        "this",
        "to",
        "use",
        "with",
        "wrong",
    }
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


@dataclass(frozen=True)
class AtomicKnowledgeProposal:
    candidate_type: str
    subject_key: str
    predicate_key: str
    meaning: str
    root_entry_ids: tuple[str, ...]
    derivative_entry_ids: tuple[str, ...] = field(default_factory=tuple)
    subject_display_name: str = ""
    participant_keys: tuple[str, ...] = field(default_factory=tuple)
    epistemic_status: str = "stated"
    uncertainty_note: str = ""
    currentness: str = "current"
    contradiction_key: str = ""
    retrieval_tags: tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class AtomicKnowledgeResult:
    candidate_id: str = ""
    outcome: str = "rejected"
    reason_code: str = "not_attempted"
    candidate_type: str = ""
    root_count: int = 0

    def __bool__(self) -> bool:
        return self.outcome in {"created", "matched_existing"} and bool(
            self.candidate_id
        )


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
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS memory_ledger_knowledge_candidates (
            candidate_id TEXT PRIMARY KEY,
            schema_version TEXT NOT NULL,
            guild_id INTEGER NOT NULL,
            candidate_type TEXT NOT NULL,
            subject_key TEXT NOT NULL,
            subject_display_name TEXT DEFAULT '',
            predicate_key TEXT NOT NULL,
            normalized_value TEXT NOT NULL,
            value_digest TEXT NOT NULL,
            epistemic_status TEXT NOT NULL,
            uncertainty_note TEXT DEFAULT '',
            currentness TEXT NOT NULL,
            candidate_state TEXT NOT NULL,
            contradiction_key TEXT NOT NULL,
            supersedes_candidate_id TEXT DEFAULT '',
            visibility TEXT NOT NULL,
            authority_class TEXT NOT NULL,
            confidence_class TEXT NOT NULL,
            route_scope_json TEXT NOT NULL,
            participant_scope_digest TEXT NOT NULL,
            first_seen_at TEXT DEFAULT '',
            last_seen_at TEXT DEFAULT '',
            retrieval_tags_json TEXT NOT NULL,
            root_digest TEXT NOT NULL,
            independent_root_count INTEGER NOT NULL DEFAULT 0,
            derivative_root_count INTEGER NOT NULL DEFAULT 0,
            candidate_eligible INTEGER NOT NULL DEFAULT 1,
            live_eligible INTEGER NOT NULL DEFAULT 0,
            promotion_status TEXT NOT NULL DEFAULT 'unpromoted',
            invalidated_reason TEXT DEFAULT '',
            invalidated_at TEXT DEFAULT '',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(
                schema_version, guild_id, candidate_type, subject_key,
                predicate_key, contradiction_key, root_digest
            )
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS memory_ledger_knowledge_roots (
            candidate_id TEXT NOT NULL,
            guild_id INTEGER NOT NULL,
            root_entry_id TEXT NOT NULL,
            root_kind TEXT NOT NULL,
            is_independent INTEGER NOT NULL DEFAULT 0,
            source_class TEXT NOT NULL,
            source_table TEXT NOT NULL,
            source_row_id TEXT NOT NULL,
            source_revision TEXT DEFAULT '',
            source_role TEXT NOT NULL,
            visibility TEXT NOT NULL,
            confidence TEXT NOT NULL,
            lifecycle_status TEXT NOT NULL,
            root_status TEXT NOT NULL,
            root_digest TEXT NOT NULL,
            lineage_path_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY(candidate_id, root_entry_id)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS memory_ledger_knowledge_participants (
            candidate_id TEXT NOT NULL,
            guild_id INTEGER NOT NULL,
            participant_key TEXT NOT NULL,
            participant_role TEXT NOT NULL DEFAULT 'subject',
            created_at TEXT NOT NULL,
            PRIMARY KEY(candidate_id, participant_key, participant_role)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS memory_ledger_knowledge_receipts (
            receipt_id TEXT PRIMARY KEY,
            guild_id INTEGER NOT NULL,
            candidate_id TEXT DEFAULT '',
            event_type TEXT NOT NULL,
            reason_code TEXT NOT NULL,
            candidate_type TEXT DEFAULT '',
            root_count INTEGER NOT NULL DEFAULT 0,
            occurred_at TEXT NOT NULL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS memory_ledger_knowledge_backfill (
            migration_key TEXT PRIMARY KEY,
            phase TEXT NOT NULL,
            cursor_value TEXT DEFAULT '',
            completed INTEGER NOT NULL DEFAULT 0,
            counts_json TEXT NOT NULL DEFAULT '{}',
            updated_at TEXT NOT NULL
        )
        """
    )
    for sql in (
        "ALTER TABLE memory_ledger_knowledge_candidates ADD COLUMN confidence_class TEXT NOT NULL DEFAULT 'unknown'",
        "ALTER TABLE memory_ledger_knowledge_roots ADD COLUMN confidence TEXT NOT NULL DEFAULT 'unknown'",
    ):
        try:
            cur.execute(sql)
        except sqlite3.OperationalError:
            pass
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
        "CREATE INDEX IF NOT EXISTS idx_mlkc_guild_type ON memory_ledger_knowledge_candidates(guild_id, candidate_type, candidate_state)",
        "CREATE INDEX IF NOT EXISTS idx_mlkc_subject ON memory_ledger_knowledge_candidates(guild_id, subject_key, candidate_state)",
        "CREATE INDEX IF NOT EXISTS idx_mlkc_contradiction ON memory_ledger_knowledge_candidates(guild_id, contradiction_key, candidate_state)",
        "CREATE INDEX IF NOT EXISTS idx_mlkc_visibility ON memory_ledger_knowledge_candidates(guild_id, visibility, authority_class)",
        "CREATE INDEX IF NOT EXISTS idx_mlkr_root ON memory_ledger_knowledge_roots(guild_id, root_entry_id, root_status)",
        "CREATE INDEX IF NOT EXISTS idx_mlkp_participant ON memory_ledger_knowledge_participants(guild_id, participant_key)",
        "CREATE INDEX IF NOT EXISTS idx_mlkreceipt_event ON memory_ledger_knowledge_receipts(guild_id, event_type, reason_code)",
    ]:
        cur.execute(sql)
    cur.execute(
        """
        CREATE TRIGGER IF NOT EXISTS trg_atomic_knowledge_root_delete
        AFTER DELETE ON memory_ledger_entries
        BEGIN
          INSERT OR IGNORE INTO memory_ledger_knowledge_receipts(
            receipt_id,guild_id,candidate_id,event_type,reason_code,
            candidate_type,root_count,occurred_at
          )
          SELECT
            'trigger:root_deleted:' || c.candidate_id || ':' || OLD.entry_id,
            c.guild_id,c.candidate_id,'invalidated','root_deleted',
            c.candidate_type,c.independent_root_count + c.derivative_root_count,
            CURRENT_TIMESTAMP
          FROM memory_ledger_knowledge_candidates c
          JOIN memory_ledger_knowledge_roots r
            ON r.candidate_id=c.candidate_id AND r.guild_id=c.guild_id
          WHERE r.root_entry_id=OLD.entry_id;

          UPDATE memory_ledger_knowledge_candidates
          SET normalized_value='',candidate_state='invalidated',
              candidate_eligible=0,live_eligible=0,
              invalidated_reason='root_deleted',
              invalidated_at=CURRENT_TIMESTAMP,updated_at=CURRENT_TIMESTAMP
          WHERE candidate_id IN (
            SELECT candidate_id FROM memory_ledger_knowledge_roots
            WHERE root_entry_id=OLD.entry_id
          );

          UPDATE memory_ledger_knowledge_roots
          SET lifecycle_status='deleted',root_status='deleted',
              updated_at=CURRENT_TIMESTAMP
          WHERE root_entry_id=OLD.entry_id;
        END
        """
    )
    cur.execute(
        """
        CREATE TRIGGER IF NOT EXISTS trg_atomic_knowledge_root_change
        AFTER UPDATE OF lifecycle_status,normalized_value,public_usable,
          visibility,source_class,confidence,subject_key,channel_policy,
          route_mode
        ON memory_ledger_entries
        WHEN
          NEW.lifecycle_status IS NOT OLD.lifecycle_status
          OR NEW.normalized_value IS NOT OLD.normalized_value
          OR NEW.public_usable IS NOT OLD.public_usable
          OR NEW.visibility IS NOT OLD.visibility
          OR NEW.source_class IS NOT OLD.source_class
          OR NEW.confidence IS NOT OLD.confidence
          OR NEW.subject_key IS NOT OLD.subject_key
          OR NEW.channel_policy IS NOT OLD.channel_policy
          OR NEW.route_mode IS NOT OLD.route_mode
        BEGIN
          INSERT OR IGNORE INTO memory_ledger_knowledge_receipts(
            receipt_id,guild_id,candidate_id,event_type,reason_code,
            candidate_type,root_count,occurred_at
          )
          SELECT
            'trigger:root_changed:' || c.candidate_id || ':' ||
              NEW.entry_id || ':' || NEW.lifecycle_status,
            c.guild_id,c.candidate_id,
            CASE
              WHEN NEW.lifecycle_status IN ('corrected','superseded')
                THEN 'superseded'
              ELSE 'invalidated'
            END,
            CASE
              WHEN NEW.lifecycle_status IN ('corrected','superseded')
                THEN 'root_superseded'
              WHEN NEW.lifecycle_status IN ('forgotten','deleted','retracted')
                THEN 'root_privacy_or_deletion'
              WHEN NEW.public_usable IS NOT OLD.public_usable
                OR NEW.visibility IS NOT OLD.visibility
                OR NEW.source_class IS NOT OLD.source_class
                OR NEW.subject_key IS NOT OLD.subject_key
                OR NEW.channel_policy IS NOT OLD.channel_policy
                OR NEW.route_mode IS NOT OLD.route_mode
                THEN 'root_privacy_or_provenance_changed'
              WHEN NEW.confidence IS NOT OLD.confidence
                THEN 'root_confidence_changed'
              ELSE 'root_changed'
            END,
            c.candidate_type,c.independent_root_count + c.derivative_root_count,
            CURRENT_TIMESTAMP
          FROM memory_ledger_knowledge_candidates c
          JOIN memory_ledger_knowledge_roots r
            ON r.candidate_id=c.candidate_id AND r.guild_id=c.guild_id
          WHERE r.root_entry_id=NEW.entry_id;

          UPDATE memory_ledger_knowledge_candidates
          SET
            normalized_value=CASE
              WHEN NEW.lifecycle_status IN ('forgotten','deleted','retracted')
                OR COALESCE(NEW.normalized_value,'')=''
                OR NEW.public_usable IS NOT OLD.public_usable
                OR NEW.visibility IS NOT OLD.visibility
                OR NEW.source_class IS NOT OLD.source_class
                OR NEW.subject_key IS NOT OLD.subject_key
                OR NEW.channel_policy IS NOT OLD.channel_policy
                OR NEW.route_mode IS NOT OLD.route_mode
                THEN ''
              ELSE normalized_value
            END,
            candidate_state=CASE
              WHEN NEW.lifecycle_status IN ('corrected','superseded')
                THEN 'superseded'
              ELSE 'invalidated'
            END,
            candidate_eligible=0,live_eligible=0,
            invalidated_reason=CASE
              WHEN NEW.lifecycle_status IN ('corrected','superseded')
                THEN 'root_superseded'
              WHEN NEW.lifecycle_status IN ('forgotten','deleted','retracted')
                THEN 'root_privacy_or_deletion'
              WHEN NEW.public_usable IS NOT OLD.public_usable
                OR NEW.visibility IS NOT OLD.visibility
                OR NEW.source_class IS NOT OLD.source_class
                OR NEW.subject_key IS NOT OLD.subject_key
                OR NEW.channel_policy IS NOT OLD.channel_policy
                OR NEW.route_mode IS NOT OLD.route_mode
                THEN 'root_privacy_or_provenance_changed'
              WHEN NEW.confidence IS NOT OLD.confidence
                THEN 'root_confidence_changed'
              ELSE 'root_changed'
            END,
            invalidated_at=CURRENT_TIMESTAMP,updated_at=CURRENT_TIMESTAMP
          WHERE candidate_id IN (
            SELECT candidate_id FROM memory_ledger_knowledge_roots
            WHERE root_entry_id=NEW.entry_id
          );

          UPDATE memory_ledger_knowledge_roots
          SET lifecycle_status=NEW.lifecycle_status,
              source_class=NEW.source_class,
              visibility=NEW.visibility,
              confidence=NEW.confidence,
              root_status=CASE
                WHEN NEW.lifecycle_status IN ('active','review_only')
                  THEN 'changed'
                ELSE NEW.lifecycle_status
              END,
              updated_at=CURRENT_TIMESTAMP
          WHERE root_entry_id=NEW.entry_id;
        END
        """
    )
    cur.execute(
        """
        CREATE TRIGGER IF NOT EXISTS trg_atomic_knowledge_participant_delete
        AFTER DELETE ON memory_ledger_participants
        BEGIN
          INSERT OR IGNORE INTO memory_ledger_knowledge_receipts(
            receipt_id,guild_id,candidate_id,event_type,reason_code,
            candidate_type,root_count,occurred_at
          )
          SELECT
            'trigger:participant_deleted:' || c.candidate_id || ':' ||
              OLD.entry_id || ':' || OLD.participant_key,
            c.guild_id,c.candidate_id,'invalidated','participant_deleted',
            c.candidate_type,c.independent_root_count + c.derivative_root_count,
            CURRENT_TIMESTAMP
          FROM memory_ledger_knowledge_candidates c
          JOIN memory_ledger_knowledge_roots r
            ON r.candidate_id=c.candidate_id AND r.guild_id=c.guild_id
          WHERE r.root_entry_id=OLD.entry_id;

          UPDATE memory_ledger_knowledge_candidates
          SET normalized_value='',candidate_state='invalidated',
              candidate_eligible=0,live_eligible=0,
              invalidated_reason='participant_deleted',
              invalidated_at=CURRENT_TIMESTAMP,updated_at=CURRENT_TIMESTAMP
          WHERE candidate_id IN (
            SELECT candidate_id FROM memory_ledger_knowledge_roots
            WHERE root_entry_id=OLD.entry_id
          );
        END
        """
    )
    cur.execute(
        """
        CREATE TRIGGER IF NOT EXISTS trg_atomic_knowledge_lineage_change
        AFTER INSERT ON memory_ledger_lineage
        WHEN NEW.lineage_type IN ('correction_of','supersedes','retracts')
        BEGIN
          INSERT OR IGNORE INTO memory_ledger_knowledge_receipts(
            receipt_id,guild_id,candidate_id,event_type,reason_code,
            candidate_type,root_count,occurred_at
          )
          SELECT
            'trigger:lineage:' || NEW.lineage_type || ':' ||
              c.candidate_id || ':' || NEW.entry_id,
            c.guild_id,c.candidate_id,
            CASE
              WHEN NEW.lineage_type='supersedes' THEN 'superseded'
              WHEN NEW.lineage_type='correction_of' THEN 'contested'
              ELSE 'invalidated'
            END,
            'root_' || NEW.lineage_type,
            c.candidate_type,c.independent_root_count + c.derivative_root_count,
            CURRENT_TIMESTAMP
          FROM memory_ledger_knowledge_candidates c
          JOIN memory_ledger_knowledge_roots r
            ON r.candidate_id=c.candidate_id AND r.guild_id=c.guild_id
          WHERE r.root_entry_id=NEW.target_entry_id;

          UPDATE memory_ledger_knowledge_candidates
          SET
            normalized_value=CASE
              WHEN NEW.lineage_type='retracts' THEN ''
              ELSE normalized_value
            END,
            candidate_state=CASE
              WHEN NEW.lineage_type='supersedes' THEN 'superseded'
              WHEN NEW.lineage_type='correction_of' THEN 'contested'
              ELSE 'invalidated'
            END,
            candidate_eligible=0,live_eligible=0,
            invalidated_reason='root_' || NEW.lineage_type,
            invalidated_at=CURRENT_TIMESTAMP,updated_at=CURRENT_TIMESTAMP
          WHERE candidate_id IN (
            SELECT candidate_id FROM memory_ledger_knowledge_roots
            WHERE root_entry_id=NEW.target_entry_id
          );
        END
        """
    )


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


def _knowledge_text(value: Any, limit: int = 1000) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()[:limit]


def _knowledge_tag(value: Any) -> str:
    return re.sub(
        r"[^a-z0-9_.:-]+",
        "_",
        str(value or "").strip().lower(),
    ).strip("_")[:64]


def _knowledge_digest(*parts: Any) -> str:
    return hashlib.sha256(
        "\x1f".join(str(part or "") for part in parts).encode("utf-8")
    ).hexdigest()


def _stable_knowledge_candidate_id(
    *,
    guild_id: int,
    candidate_type: str,
    subject_key: str,
    predicate_key: str,
    contradiction_key: str,
    root_entry_ids: tuple[str, ...],
) -> str:
    digest = _knowledge_digest(
        ATOMIC_KNOWLEDGE_SCHEMA_VERSION,
        int(guild_id or 0),
        candidate_type,
        subject_key,
        predicate_key,
        contradiction_key,
        *sorted(set(root_entry_ids)),
    )
    return "mlkc_" + digest[:40]


def _knowledge_receipt_id(
    *,
    event_type: str,
    reason_code: str,
    candidate_id: str,
    candidate_type: str,
    root_entry_ids: tuple[str, ...],
    proposal_digest: str = "",
) -> str:
    return "mlkrec_" + _knowledge_digest(
        event_type,
        reason_code,
        candidate_id,
        candidate_type,
        proposal_digest,
        *sorted(set(root_entry_ids)),
    )[:40]


def _record_knowledge_receipt(
    conn: sqlite3.Connection,
    *,
    guild_id: int,
    event_type: str,
    reason_code: str,
    candidate_id: str = "",
    candidate_type: str = "",
    root_entry_ids: tuple[str, ...] = (),
    proposal_digest: str = "",
) -> None:
    ensure_memory_ledger_schema(conn)
    conn.execute(
        """
        INSERT OR IGNORE INTO memory_ledger_knowledge_receipts(
          receipt_id,guild_id,candidate_id,event_type,reason_code,
          candidate_type,root_count,occurred_at
        ) VALUES(?,?,?,?,?,?,?,?)
        """,
        (
            _knowledge_receipt_id(
                event_type=event_type,
                reason_code=reason_code,
                candidate_id=candidate_id,
                candidate_type=candidate_type,
                root_entry_ids=root_entry_ids,
                proposal_digest=proposal_digest,
            ),
            int(guild_id or 0),
            candidate_id,
            event_type[:40],
            reason_code[:120],
            candidate_type[:80],
            len(set(root_entry_ids)),
            _now(),
        ),
    )


def record_atomic_knowledge_processing_error(
    conn: sqlite3.Connection,
    *,
    guild_id: int,
    reason_code: str = "processing_error",
    candidate_type: str = "",
    root_entry_ids: tuple[str, ...] = (),
) -> None:
    _record_knowledge_receipt(
        conn,
        guild_id=guild_id,
        event_type="error",
        reason_code=reason_code,
        candidate_type=candidate_type,
        root_entry_ids=root_entry_ids,
    )


def _reject_atomic_knowledge(
    conn: sqlite3.Connection,
    proposal: AtomicKnowledgeProposal,
    *,
    guild_id: int,
    reason_code: str,
    root_entry_ids: tuple[str, ...],
) -> AtomicKnowledgeResult:
    if not int(guild_id or 0) and root_entry_ids:
        placeholders = ",".join("?" for _entry_id in root_entry_ids)
        guild_rows = conn.execute(
            """
            SELECT DISTINCT guild_id
            FROM memory_ledger_entries
            WHERE entry_id IN (%s)
            """ % placeholders,
            tuple(root_entry_ids),
        ).fetchall()
        if len(guild_rows) == 1:
            guild_id = int(guild_rows[0][0] or 0)
    _record_knowledge_receipt(
        conn,
        guild_id=guild_id,
        event_type="rejected",
        reason_code=reason_code,
        candidate_type=proposal.candidate_type,
        root_entry_ids=root_entry_ids,
        proposal_digest=_knowledge_digest(
            proposal.subject_key,
            proposal.predicate_key,
            _canon(proposal.meaning),
        ),
    )
    return AtomicKnowledgeResult(
        outcome="rejected",
        reason_code=reason_code,
        candidate_type=proposal.candidate_type,
        root_count=len(set(root_entry_ids)),
    )


def _knowledge_entry_rows(
    conn: sqlite3.Connection,
    entry_ids: tuple[str, ...],
) -> dict[str, dict[str, Any]]:
    if not entry_ids:
        return {}
    placeholders = ",".join("?" for _entry_id in entry_ids)
    rows = conn.execute(
        """
        SELECT
          entry_id,guild_id,subject_key,subject_display_name,entry_type,
          predicate_key,normalized_value,source_class,source_table,
          source_row_id,source_revision,source_role,route_mode,channel_id,
          channel_name,channel_policy,visibility,confidence,public_usable,
          derived,projection,observed_at,lifecycle_status
        FROM memory_ledger_entries
        WHERE entry_id IN (%s)
        """ % placeholders,
        tuple(entry_ids),
    ).fetchall()
    by_id: dict[str, dict[str, Any]] = {}
    for row in rows:
        by_id[str(row[0])] = {
            "entry_id": str(row[0]),
            "guild_id": int(row[1] or 0),
            "subject_key": str(row[2] or ""),
            "subject_display_name": str(row[3] or ""),
            "entry_type": str(row[4] or ""),
            "predicate_key": str(row[5] or ""),
            "normalized_value": str(row[6] or ""),
            "source_class": str(row[7] or ""),
            "source_table": str(row[8] or ""),
            "source_row_id": str(row[9] or ""),
            "source_revision": str(row[10] or ""),
            "source_role": str(row[11] or ""),
            "route_mode": str(row[12] or ""),
            "channel_id": int(row[13] or 0),
            "channel_name": str(row[14] or ""),
            "channel_policy": str(row[15] or ""),
            "visibility": str(row[16] or ""),
            "confidence": str(row[17] or "unknown"),
            "public_usable": bool(row[18]),
            "derived": bool(row[19]),
            "projection": bool(row[20]),
            "observed_at": str(row[21] or ""),
            "lifecycle_status": str(row[22] or ""),
        }
    if by_id:
        placeholders = ",".join("?" for _entry_id in by_id)
        for entry_id, participant_key, participant_role in conn.execute(
            """
            SELECT entry_id,participant_key,participant_role
            FROM memory_ledger_participants
            WHERE entry_id IN (%s)
            ORDER BY entry_id,order_index,participant_key
            """ % placeholders,
            tuple(by_id),
        ).fetchall():
            by_id[str(entry_id)].setdefault("participants", []).append(
                (str(participant_key or ""), str(participant_role or "participant"))
            )
    for row in by_id.values():
        row.setdefault("participants", [])
    return by_id


def _derivation_paths(
    conn: sqlite3.Connection,
    derivative_entry_id: str,
    *,
    max_depth: int = 8,
    max_nodes: int = 128,
) -> dict[str, tuple[str, ...]]:
    """Return bounded derived-from paths without treating them as authority."""
    paths: dict[str, tuple[str, ...]] = {}
    frontier: list[tuple[str, tuple[str, ...]]] = [
        (derivative_entry_id, (derivative_entry_id,))
    ]
    visited = {derivative_entry_id}
    while frontier and len(visited) <= max_nodes:
        current, path = frontier.pop(0)
        if len(path) > max_depth:
            continue
        targets = conn.execute(
            """
            SELECT target_entry_id
            FROM memory_ledger_lineage
            WHERE entry_id=? AND lineage_type='derived_from'
            ORDER BY target_entry_id
            """,
            (current,),
        ).fetchall()
        for (target_raw,) in targets:
            target = str(target_raw or "")
            if not target:
                continue
            candidate_path = (*path, target)
            paths.setdefault(target, candidate_path)
            if target not in visited:
                visited.add(target)
                frontier.append((target, candidate_path))
    return paths


def _knowledge_visibility(values: set[str]) -> tuple[str, str]:
    values = {str(value or "unknown") for value in values}
    if values.intersection(_KNOWLEDGE_RESTRICTED_VISIBILITIES):
        return "", "restricted_or_unknown_visibility"
    nonpublic = values.intersection({"internal", "private", "mod"})
    if len(nonpublic) > 1:
        return "", "ambiguous_nonpublic_visibility"
    if nonpublic:
        return next(iter(nonpublic)), ""
    if values == {Visibility.REFERENCE_CANON.value}:
        return Visibility.REFERENCE_CANON.value, ""
    if values.issubset(
        {
            Visibility.PUBLIC.value,
            Visibility.PUBLIC_SAFE.value,
            Visibility.REFERENCE_CANON.value,
        }
    ):
        return (
            Visibility.PUBLIC.value
            if values == {Visibility.PUBLIC.value}
            else Visibility.PUBLIC_SAFE.value
        ), ""
    return "", "ambiguous_visibility"


def _knowledge_route_visibility_is_explicit(entry: dict[str, Any]) -> bool:
    policy = str(entry.get("channel_policy") or "").strip()
    visibility = str(entry.get("visibility") or "").strip()
    if policy == "member_control":
        return visibility in {
            Visibility.PRIVATE.value,
            Visibility.PUBLIC_SAFE.value,
        }
    if not has_explicit_channel_policy_mapping(policy):
        return False
    return map_channel_policy_visibility(policy).value == visibility


def _knowledge_is_derivative(entry: dict[str, Any]) -> bool:
    return bool(
        entry.get("derived")
        or entry.get("projection")
        or entry.get("source_class") in KNOWLEDGE_DERIVATIVE_SOURCE_CLASSES
        or str(entry.get("source_role") or "").lower() in {"model", "assistant"}
    )


def _knowledge_operational_or_test_source(
    entry: dict[str, Any],
) -> bool:
    metadata = " ".join(
        str(entry.get(key) or "")
        for key in (
            "source_table",
            "source_role",
            "entry_type",
            "predicate_key",
            "route_mode",
            "channel_name",
            "channel_policy",
        )
    )
    if _KNOWLEDGE_TEST_OR_OPERATIONAL_SOURCE_RE.search(metadata):
        return True
    return bool(
        _KNOWLEDGE_TEST_OR_OPERATIONAL_RE.search(
            str(entry.get("normalized_value") or "")
        )
    )


def _knowledge_participant_scope(
    proposal: AtomicKnowledgeProposal,
    independent_entries: list[dict[str, Any]],
) -> tuple[tuple[str, ...], str]:
    scopes: list[tuple[str, ...]] = []
    for entry in independent_entries:
        scope = tuple(
            sorted(
                {
                    str(participant_key or "")
                    for participant_key, _role in entry.get("participants", [])
                    if str(participant_key or "")
                }
            )
        )
        scopes.append(scope)
    if scopes and any(scope != scopes[0] for scope in scopes[1:]):
        return (), "participant_scope_mismatch"
    derived_scope = scopes[0] if scopes else ()
    requested = tuple(
        sorted(
            {
                str(participant_key or "")
                for participant_key in proposal.participant_keys
                if str(participant_key or "")
            }
        )
    )
    if requested and requested != derived_scope:
        return (), "participant_scope_mismatch"
    participants = requested or derived_scope
    if proposal.subject_key.startswith("discord_user:"):
        if proposal.subject_key not in participants:
            return (), "ambiguous_subject_participant_identity"
    return participants, ""


def form_atomic_knowledge_candidate(
    conn: sqlite3.Connection,
    proposal: AtomicKnowledgeProposal,
) -> AtomicKnowledgeResult:
    """Atomically form one candidate without owning the caller transaction."""
    ensure_memory_ledger_schema(conn)
    savepoint = f"atomic_knowledge_{id(proposal):x}"
    conn.execute(f"SAVEPOINT {savepoint}")
    try:
        result = _form_atomic_knowledge_candidate_impl(conn, proposal)
    except Exception:
        conn.execute(f"ROLLBACK TO {savepoint}")
        conn.execute(f"RELEASE {savepoint}")
        raise
    conn.execute(f"RELEASE {savepoint}")
    return result


def _form_atomic_knowledge_candidate_impl(
    conn: sqlite3.Connection,
    proposal: AtomicKnowledgeProposal,
) -> AtomicKnowledgeResult:
    """Create one unpromoted candidate from exact revalidated Ledger roots."""
    candidate_type = _canon(proposal.candidate_type)
    subject_key = str(proposal.subject_key or "").strip()
    predicate_key = _knowledge_tag(proposal.predicate_key)
    meaning = _knowledge_text(proposal.meaning)
    independent_ids = tuple(
        sorted(
            {
                str(entry_id or "").strip()
                for entry_id in proposal.root_entry_ids
                if str(entry_id or "").strip()
            }
        )
    )
    derivative_ids = tuple(
        sorted(
            {
                str(entry_id or "").strip()
                for entry_id in proposal.derivative_entry_ids
                if str(entry_id or "").strip()
            }
        )
    )
    all_ids = tuple(sorted(set(independent_ids + derivative_ids)))
    guild_hint = 0
    if candidate_type not in KNOWLEDGE_CANDIDATE_TYPES:
        return _reject_atomic_knowledge(
            conn,
            proposal,
            guild_id=guild_hint,
            reason_code="unsupported_candidate_type",
            root_entry_ids=all_ids,
        )
    if (
        not subject_key
        or subject_key == "unknown"
        or subject_key.startswith("moment:")
    ):
        return _reject_atomic_knowledge(
            conn,
            proposal,
            guild_id=guild_hint,
            reason_code="ambiguous_subject_identity",
            root_entry_ids=all_ids,
        )
    if not predicate_key or not meaning:
        return _reject_atomic_knowledge(
            conn,
            proposal,
            guild_id=guild_hint,
            reason_code="missing_candidate_meaning",
            root_entry_ids=all_ids,
        )
    if not independent_ids:
        return _reject_atomic_knowledge(
            conn,
            proposal,
            guild_id=guild_hint,
            reason_code="derivative_only_no_independent_root",
            root_entry_ids=all_ids,
        )
    if len(independent_ids) > 16 or len(derivative_ids) > 4:
        return _reject_atomic_knowledge(
            conn,
            proposal,
            guild_id=guild_hint,
            reason_code="root_set_too_large",
            root_entry_ids=all_ids,
        )
    if set(independent_ids).intersection(derivative_ids):
        return _reject_atomic_knowledge(
            conn,
            proposal,
            guild_id=guild_hint,
            reason_code="ambiguous_root_role",
            root_entry_ids=all_ids,
        )
    if proposal.epistemic_status not in KNOWLEDGE_EPISTEMIC_STATUSES:
        return _reject_atomic_knowledge(
            conn,
            proposal,
            guild_id=guild_hint,
            reason_code="ambiguous_epistemic_status",
            root_entry_ids=all_ids,
        )
    if proposal.currentness not in KNOWLEDGE_CURRENTNESS:
        return _reject_atomic_knowledge(
            conn,
            proposal,
            guild_id=guild_hint,
            reason_code="ambiguous_currentness",
            root_entry_ids=all_ids,
        )
    if (
        candidate_type == "inference_or_contested_claim"
        and proposal.epistemic_status not in {"inference", "contested"}
    ):
        return _reject_atomic_knowledge(
            conn,
            proposal,
            guild_id=guild_hint,
            reason_code="inference_label_required",
            root_entry_ids=all_ids,
        )

    entries = _knowledge_entry_rows(conn, all_ids)
    if len(entries) != len(all_ids):
        present_guilds = {
            int(entry.get("guild_id") or 0) for entry in entries.values()
        }
        guild_hint = next(iter(present_guilds)) if len(present_guilds) == 1 else 0
        return _reject_atomic_knowledge(
            conn,
            proposal,
            guild_id=guild_hint,
            reason_code="missing_or_orphaned_root",
            root_entry_ids=all_ids,
        )
    guilds = {int(entry.get("guild_id") or 0) for entry in entries.values()}
    if len(guilds) != 1 or not next(iter(guilds)):
        return _reject_atomic_knowledge(
            conn,
            proposal,
            guild_id=0,
            reason_code="cross_guild_or_ambiguous_provenance",
            root_entry_ids=all_ids,
        )
    guild_id = next(iter(guilds))
    guild_hint = guild_id
    independent_entries = [entries[entry_id] for entry_id in independent_ids]
    derivative_entries = [entries[entry_id] for entry_id in derivative_ids]

    if any(
        entry.get("source_class") == SourceClass.LEGACY_SOURCE_BLIND.value
        for entry in entries.values()
    ):
        return _reject_atomic_knowledge(
            conn,
            proposal,
            guild_id=guild_id,
            reason_code="source_blind_provenance",
            root_entry_ids=all_ids,
        )
    if any(_knowledge_is_derivative(entry) for entry in independent_entries):
        return _reject_atomic_knowledge(
            conn,
            proposal,
            guild_id=guild_id,
            reason_code="derivative_misclassified_as_independent",
            root_entry_ids=all_ids,
        )
    if any(not _knowledge_is_derivative(entry) for entry in derivative_entries):
        return _reject_atomic_knowledge(
            conn,
            proposal,
            guild_id=guild_id,
            reason_code="ambiguous_derivative_provenance",
            root_entry_ids=all_ids,
        )
    if any(
        _knowledge_operational_or_test_source(entry)
        for entry in entries.values()
    ):
        return _reject_atomic_knowledge(
            conn,
            proposal,
            guild_id=guild_id,
            reason_code="operational_or_rehearsal_source_excluded",
            root_entry_ids=all_ids,
        )
    if any(
        entry.get("lifecycle_status") != ACTIVE_LIFECYCLE
        for entry in independent_entries
    ):
        return _reject_atomic_knowledge(
            conn,
            proposal,
            guild_id=guild_id,
            reason_code="ineligible_root_lifecycle",
            root_entry_ids=all_ids,
        )
    if any(
        entry.get("lifecycle_status")
        not in {ACTIVE_LIFECYCLE, REVIEW_ONLY_LIFECYCLE}
        for entry in derivative_entries
    ):
        return _reject_atomic_knowledge(
            conn,
            proposal,
            guild_id=guild_id,
            reason_code="ineligible_derivative_lifecycle",
            root_entry_ids=all_ids,
        )
    if conn.execute(
        """
        SELECT 1
        FROM memory_ledger_lineage
        WHERE target_entry_id IN (%s)
          AND lineage_type IN ('supersedes','retracts')
        LIMIT 1
        """ % ",".join("?" for _entry_id in independent_ids),
        independent_ids,
    ).fetchone():
        return _reject_atomic_knowledge(
            conn,
            proposal,
            guild_id=guild_id,
            reason_code="root_superseded_or_retracted",
            root_entry_ids=all_ids,
        )
    if any(
        not str(entry.get("route_mode") or "").strip()
        or str(entry.get("route_mode") or "").strip() == "unknown"
        or not str(entry.get("channel_policy") or "").strip()
        or str(entry.get("channel_policy") or "").strip() == "unknown"
        for entry in entries.values()
    ):
        return _reject_atomic_knowledge(
            conn,
            proposal,
            guild_id=guild_id,
            reason_code="ambiguous_route_or_policy",
            root_entry_ids=all_ids,
        )
    if any(
        not _knowledge_route_visibility_is_explicit(entry)
        for entry in entries.values()
    ):
        return _reject_atomic_knowledge(
            conn,
            proposal,
            guild_id=guild_id,
            reason_code="route_visibility_contract_mismatch",
            root_entry_ids=all_ids,
        )

    root_subjects = {
        str(entry.get("subject_key") or "") for entry in independent_entries
    }
    if root_subjects != {subject_key}:
        return _reject_atomic_knowledge(
            conn,
            proposal,
            guild_id=guild_id,
            reason_code="subject_root_isolation_failure",
            root_entry_ids=all_ids,
        )
    participants, participant_reason = _knowledge_participant_scope(
        proposal,
        independent_entries,
    )
    if participant_reason:
        return _reject_atomic_knowledge(
            conn,
            proposal,
            guild_id=guild_id,
            reason_code=participant_reason,
            root_entry_ids=all_ids,
        )
    for derivative in derivative_entries:
        derivative_participants = {
            participant_key
            for participant_key, _role in derivative.get("participants", [])
            if participant_key
        }
        if derivative_participants and subject_key not in derivative_participants:
            return _reject_atomic_knowledge(
                conn,
                proposal,
                guild_id=guild_id,
                reason_code="derivative_participant_isolation_failure",
                root_entry_ids=all_ids,
            )

    derivation_paths: dict[str, dict[str, tuple[str, ...]]] = {}
    for derivative_id in derivative_ids:
        paths = _derivation_paths(conn, derivative_id)
        derivation_paths[derivative_id] = paths
        if any(root_id not in paths for root_id in independent_ids):
            return _reject_atomic_knowledge(
                conn,
                proposal,
                guild_id=guild_id,
                reason_code="derivative_lineage_incomplete",
                root_entry_ids=all_ids,
            )

    visibility, visibility_reason = _knowledge_visibility(
        {str(entry.get("visibility") or "unknown") for entry in entries.values()}
    )
    if visibility_reason:
        return _reject_atomic_knowledge(
            conn,
            proposal,
            guild_id=guild_id,
            reason_code=visibility_reason,
            root_entry_ids=all_ids,
        )
    if (
        visibility
        in {
            Visibility.PUBLIC.value,
            Visibility.PUBLIC_SAFE.value,
            Visibility.REFERENCE_CANON.value,
        }
        and any(not entry.get("public_usable") for entry in independent_entries)
    ):
        return _reject_atomic_knowledge(
            conn,
            proposal,
            guild_id=guild_id,
            reason_code="public_source_not_usable",
            root_entry_ids=all_ids,
        )
    if any(
        entry.get("public_usable")
        and str(entry.get("visibility") or "")
        not in {
            Visibility.PUBLIC.value,
            Visibility.PUBLIC_SAFE.value,
            Visibility.REFERENCE_CANON.value,
        }
        for entry in entries.values()
    ):
        return _reject_atomic_knowledge(
            conn,
            proposal,
            guild_id=guild_id,
            reason_code="nonpublic_source_marked_public_usable",
            root_entry_ids=all_ids,
        )

    authority_values = {
        str(entry.get("source_class") or "") for entry in independent_entries
    }
    if any(value not in _KNOWLEDGE_AUTHORITY_RANK for value in authority_values):
        return _reject_atomic_knowledge(
            conn,
            proposal,
            guild_id=guild_id,
            reason_code="ambiguous_authority",
            root_entry_ids=all_ids,
        )
    authority_class = min(
        authority_values,
        key=lambda value: (_KNOWLEDGE_AUTHORITY_RANK[value], value),
    )
    confidence_rank = {
        Confidence.UNKNOWN.value: 0,
        Confidence.LOW.value: 1,
        Confidence.MEDIUM.value: 2,
        Confidence.HIGH.value: 3,
        Confidence.APPROVED.value: 4,
    }
    confidence_values = {
        str(entry.get("confidence") or Confidence.UNKNOWN.value)
        for entry in independent_entries
    }
    confidence_class = min(
        confidence_values,
        key=lambda value: (confidence_rank.get(value, 0), value),
    )
    contradiction_key = _knowledge_tag(
        proposal.contradiction_key
        or f"{subject_key}:{predicate_key}"
    )
    if not contradiction_key:
        return _reject_atomic_knowledge(
            conn,
            proposal,
            guild_id=guild_id,
            reason_code="ambiguous_contradiction_scope",
            root_entry_ids=all_ids,
        )
    tags = tuple(
        sorted(
            {
                tag
                for tag in (
                    _knowledge_tag(raw_tag)
                    for raw_tag in proposal.retrieval_tags
                )
                if tag
            }
        )[:16]
    )
    routes = sorted(
        {
            (
                str(entry.get("route_mode") or ""),
                str(entry.get("channel_policy") or ""),
                int(entry.get("channel_id") or 0),
                str(entry.get("channel_name") or "")[:120],
            )
            for entry in entries.values()
        }
    )
    route_scope_json = json.dumps(
        [
            {
                "route_mode": route_mode,
                "channel_policy": channel_policy,
                "channel_id": channel_id,
                "channel_name": channel_name,
            }
            for route_mode, channel_policy, channel_id, channel_name in routes
        ],
        sort_keys=True,
        separators=(",", ":"),
    )
    root_digest = _knowledge_digest(*independent_ids)
    participant_scope_digest = _knowledge_digest(*participants)
    candidate_id = _stable_knowledge_candidate_id(
        guild_id=guild_id,
        candidate_type=candidate_type,
        subject_key=subject_key,
        predicate_key=predicate_key,
        contradiction_key=contradiction_key,
        root_entry_ids=independent_ids,
    )
    value_digest = _knowledge_digest(_canon(meaning))
    first_seen = min(
        (str(entry.get("observed_at") or "") for entry in independent_entries),
        default="",
    )
    last_seen = max(
        (str(entry.get("observed_at") or "") for entry in independent_entries),
        default="",
    )
    existing = conn.execute(
        """
        SELECT normalized_value,candidate_state,candidate_eligible
        FROM memory_ledger_knowledge_candidates
        WHERE candidate_id=?
        """,
        (candidate_id,),
    ).fetchone()
    if existing:
        if _canon(existing[0]) != _canon(meaning):
            conn.execute(
                """
                UPDATE memory_ledger_knowledge_candidates
                SET candidate_state='contested',candidate_eligible=0,
                    live_eligible=0,invalidated_reason='same_roots_meaning_mismatch',
                    invalidated_at=?,updated_at=?
                WHERE candidate_id=?
                """,
                (_now(), _now(), candidate_id),
            )
            _record_knowledge_receipt(
                conn,
                guild_id=guild_id,
                event_type="contested",
                reason_code="same_roots_meaning_mismatch",
                candidate_id=candidate_id,
                candidate_type=candidate_type,
                root_entry_ids=all_ids,
                proposal_digest=value_digest,
            )
            return AtomicKnowledgeResult(
                candidate_id,
                "contested",
                "same_roots_meaning_mismatch",
                candidate_type,
                len(all_ids),
            )
        conn.execute(
            """
            UPDATE memory_ledger_knowledge_candidates
            SET last_seen_at=CASE
                  WHEN ? > COALESCE(last_seen_at,'') THEN ?
                  ELSE last_seen_at
                END,
                updated_at=?
            WHERE candidate_id=?
            """,
            (last_seen, last_seen, _now(), candidate_id),
        )
        _record_knowledge_receipt(
            conn,
            guild_id=guild_id,
            event_type="matched_existing",
            reason_code=(
                "matched_terminal_candidate"
                if str(existing[1]) != "candidate" or not bool(existing[2])
                else "exact_candidate_match"
            ),
            candidate_id=candidate_id,
            candidate_type=candidate_type,
            root_entry_ids=all_ids,
            proposal_digest=value_digest,
        )
        return AtomicKnowledgeResult(
            candidate_id,
            "matched_existing",
            (
                "matched_terminal_candidate"
                if str(existing[1]) != "candidate" or not bool(existing[2])
                else "exact_candidate_match"
            ),
            candidate_type,
            len(all_ids),
        )

    conflicts = conn.execute(
        """
        SELECT candidate_id,normalized_value,candidate_state,
               candidate_eligible
        FROM memory_ledger_knowledge_candidates
        WHERE guild_id=? AND subject_key=? AND candidate_type=?
          AND contradiction_key=? AND candidate_id<>?
        ORDER BY candidate_id
        """,
        (
            guild_id,
            subject_key,
            candidate_type,
            contradiction_key,
            candidate_id,
        ),
    ).fetchall()
    conflicting_rows = [
        (
            str(row[0]),
            str(row[2] or ""),
            bool(row[3]),
        )
        for row in conflicts
        if _canon(row[1]) != _canon(meaning)
    ]
    explicitly_superseded: list[str] = []
    unresolved_conflicts: list[str] = []
    for conflict_id, conflict_state, conflict_eligible in conflicting_rows:
        old_roots = tuple(
            str(row[0])
            for row in conn.execute(
                """
                SELECT root_entry_id
                FROM memory_ledger_knowledge_roots
                WHERE candidate_id=? AND is_independent=1
                ORDER BY root_entry_id
                """,
                (conflict_id,),
            ).fetchall()
        )
        if old_roots and conn.execute(
            """
            SELECT 1 FROM memory_ledger_lineage
            WHERE entry_id IN (%s)
              AND target_entry_id IN (%s)
              AND lineage_type='supersedes'
            LIMIT 1
            """
            % (
                ",".join("?" for _entry_id in independent_ids),
                ",".join("?" for _entry_id in old_roots),
            ),
            tuple(independent_ids + old_roots),
        ).fetchone():
            explicitly_superseded.append(conflict_id)
        elif conflict_state == "candidate" and conflict_eligible:
            unresolved_conflicts.append(conflict_id)

    initial_state = (
        "contested"
        if proposal.epistemic_status == "contested" or unresolved_conflicts
        else "candidate"
    )
    now = _now()
    conn.execute(
        """
        INSERT INTO memory_ledger_knowledge_candidates(
          candidate_id,schema_version,guild_id,candidate_type,subject_key,
          subject_display_name,predicate_key,normalized_value,value_digest,
          epistemic_status,uncertainty_note,currentness,candidate_state,
          contradiction_key,supersedes_candidate_id,visibility,
          authority_class,confidence_class,route_scope_json,
          participant_scope_digest,first_seen_at,last_seen_at,
          retrieval_tags_json,root_digest,independent_root_count,
          derivative_root_count,candidate_eligible,live_eligible,
          promotion_status,invalidated_reason,invalidated_at,created_at,
          updated_at
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            candidate_id,
            ATOMIC_KNOWLEDGE_SCHEMA_VERSION,
            guild_id,
            candidate_type,
            subject_key,
            _knowledge_text(proposal.subject_display_name, 120),
            predicate_key,
            meaning,
            value_digest,
            proposal.epistemic_status,
            _knowledge_text(proposal.uncertainty_note, 240),
            proposal.currentness,
            initial_state,
            contradiction_key,
            explicitly_superseded[0] if len(explicitly_superseded) == 1 else "",
            visibility,
            authority_class,
            confidence_class,
            route_scope_json,
            participant_scope_digest,
            first_seen,
            last_seen,
            json.dumps(tags, separators=(",", ":")),
            root_digest,
            len(independent_ids),
            len(derivative_ids),
            1 if initial_state == "candidate" else 0,
            0,
            "unpromoted",
            "unresolved_contradiction" if initial_state == "contested" else "",
            now if initial_state == "contested" else "",
            now,
            now,
        ),
    )
    for participant_key in participants:
        conn.execute(
            """
            INSERT OR IGNORE INTO memory_ledger_knowledge_participants(
              candidate_id,guild_id,participant_key,participant_role,created_at
            ) VALUES(?,?,?,?,?)
            """,
            (
                candidate_id,
                guild_id,
                participant_key,
                "subject" if participant_key == subject_key else "participant",
                now,
            ),
        )
    for entry_id in all_ids:
        entry = entries[entry_id]
        independent = entry_id in independent_ids
        if independent:
            root_kind = (
                "human_source"
                if str(entry.get("source_role") or "").lower()
                in {
                    "user",
                    "member_self_report",
                    "member_control",
                    "owner",
                    "operator",
                }
                else "source_record"
            )
            paths: list[list[str]] = [[entry_id]]
        else:
            root_kind = "bnl_derivative"
            paths = [
                list(path)
                for path in derivation_paths.get(entry_id, {}).values()
                if path[-1] in independent_ids
            ]
        entry_digest = _knowledge_digest(
            entry_id,
            entry.get("source_revision"),
            _canon(entry.get("normalized_value")),
        )
        conn.execute(
            """
            INSERT INTO memory_ledger_knowledge_roots(
              candidate_id,guild_id,root_entry_id,root_kind,is_independent,
              source_class,source_table,source_row_id,source_revision,
              source_role,visibility,confidence,lifecycle_status,root_status,
              root_digest,lineage_path_json,created_at,updated_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                candidate_id,
                guild_id,
                entry_id,
                root_kind,
                1 if independent else 0,
                entry.get("source_class"),
                entry.get("source_table"),
                entry.get("source_row_id"),
                entry.get("source_revision"),
                entry.get("source_role"),
                entry.get("visibility"),
                entry.get("confidence"),
                entry.get("lifecycle_status"),
                "eligible",
                entry_digest,
                json.dumps(paths, sort_keys=True, separators=(",", ":")),
                now,
                now,
            ),
        )

    for superseded_id in explicitly_superseded:
        conn.execute(
            """
            UPDATE memory_ledger_knowledge_candidates
            SET candidate_state='superseded',candidate_eligible=0,
                live_eligible=0,invalidated_reason='explicit_root_supersession',
                invalidated_at=?,updated_at=?
            WHERE candidate_id=?
            """,
            (now, now, superseded_id),
        )
        _record_knowledge_receipt(
            conn,
            guild_id=guild_id,
            event_type="superseded",
            reason_code="explicit_root_supersession",
            candidate_id=superseded_id,
            candidate_type=candidate_type,
            root_entry_ids=independent_ids,
        )
    if unresolved_conflicts:
        placeholders = ",".join("?" for _candidate_id in unresolved_conflicts)
        conn.execute(
            """
            UPDATE memory_ledger_knowledge_candidates
            SET candidate_state='contested',candidate_eligible=0,
                live_eligible=0,invalidated_reason='unresolved_contradiction',
                invalidated_at=?,updated_at=?
            WHERE candidate_id IN (%s)
            """ % placeholders,
            tuple([now, now] + unresolved_conflicts),
        )
        for conflict_id in unresolved_conflicts:
            _record_knowledge_receipt(
                conn,
                guild_id=guild_id,
                event_type="contested",
                reason_code="unresolved_contradiction",
                candidate_id=conflict_id,
                candidate_type=candidate_type,
                root_entry_ids=independent_ids,
            )
    event_type = "contested" if initial_state == "contested" else "created"
    reason_code = (
        "unresolved_contradiction"
        if initial_state == "contested"
        else "source_linked_candidate_created"
    )
    _record_knowledge_receipt(
        conn,
        guild_id=guild_id,
        event_type=event_type,
        reason_code=reason_code,
        candidate_id=candidate_id,
        candidate_type=candidate_type,
        root_entry_ids=all_ids,
        proposal_digest=value_digest,
    )
    return AtomicKnowledgeResult(
        candidate_id,
        event_type,
        reason_code,
        candidate_type,
        len(all_ids),
    )


def form_atomic_candidate_from_ledger_entry(
    conn: sqlite3.Connection,
    entry_id: str,
) -> AtomicKnowledgeResult | None:
    """Map only already-typed durable Ledger entries; raw chat stays raw."""
    ensure_memory_ledger_schema(conn)
    rows = _knowledge_entry_rows(conn, (entry_id,))
    entry = rows.get(entry_id)
    if not entry:
        proposal = AtomicKnowledgeProposal(
            "person_role_fact",
            "unknown",
            "missing",
            "",
            (entry_id,),
        )
        return _reject_atomic_knowledge(
            conn,
            proposal,
            guild_id=0,
            reason_code="missing_or_orphaned_root",
            root_entry_ids=(entry_id,),
        )
    entry_type = str(entry.get("entry_type") or "")
    if entry_type not in {
        "claim",
        "preference",
        "boundary",
        "goal",
        "open_loop",
        "commitment",
        "unresolved_question",
        "event",
        "canon_reference",
    }:
        return None
    subject_key = str(entry.get("subject_key") or "")
    predicate_key = str(entry.get("predicate_key") or "")
    if entry_type in {"open_loop", "unresolved_question"}:
        candidate_type = "open_loop_or_question"
        currentness = "open"
        epistemic = "question" if entry_type == "unresolved_question" else "stated"
    elif entry_type == "event":
        candidate_type = "project_event_or_milestone"
        currentness = "historical"
        epistemic = "observed"
    elif (
        subject_key.startswith("discord_user:")
        or any(
            token in predicate_key
            for token in (
                "name",
                "pronoun",
                "role",
                "identity",
                "favorite",
                "preference",
                "boundary",
            )
        )
    ):
        candidate_type = "person_role_fact"
        currentness = "current"
        epistemic = "stated"
    elif entry_type == "canon_reference":
        candidate_type = "project_event_or_milestone"
        currentness = "current"
        epistemic = "stated"
    else:
        candidate_type = "open_loop_or_question"
        currentness = "open"
        epistemic = "stated"
    if str(entry.get("lifecycle_status") or "") != ACTIVE_LIFECYCLE:
        proposal = AtomicKnowledgeProposal(
            candidate_type=candidate_type,
            subject_key=subject_key,
            subject_display_name=str(entry.get("subject_display_name") or ""),
            predicate_key=predicate_key,
            meaning=str(entry.get("normalized_value") or ""),
            root_entry_ids=(entry_id,),
            epistemic_status=epistemic,
            currentness=currentness,
        )
        return _reject_atomic_knowledge(
            conn,
            proposal,
            guild_id=int(entry.get("guild_id") or 0),
            reason_code="ineligible_root_lifecycle",
            root_entry_ids=(entry_id,),
        )
    participant_keys = tuple(
        sorted(
            {
                participant_key
                for participant_key, _role in entry.get("participants", [])
                if participant_key
            }
        )
    )
    return form_atomic_knowledge_candidate(
        conn,
        AtomicKnowledgeProposal(
            candidate_type=candidate_type,
            subject_key=subject_key,
            subject_display_name=str(entry.get("subject_display_name") or ""),
            predicate_key=predicate_key,
            meaning=str(entry.get("normalized_value") or ""),
            root_entry_ids=(entry_id,),
            participant_keys=participant_keys,
            epistemic_status=epistemic,
            currentness=currentness,
            contradiction_key=f"{subject_key}:{predicate_key}",
            retrieval_tags=(
                candidate_type,
                predicate_key,
                str(entry.get("source_table") or ""),
            ),
        ),
    )


def form_atomic_candidates_from_moment(
    conn: sqlite3.Connection,
    moment_id: str,
) -> list[AtomicKnowledgeResult]:
    """Project participant gists while retaining every actual human root."""
    ensure_memory_ledger_schema(conn)
    required_tables = {
        "memory_moment_windows",
        "memory_moment_contributions",
        "memory_moment_contribution_sources",
    }
    existing_tables = {
        str(row[0])
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    if not required_tables.issubset(existing_tables):
        return []
    window = conn.execute(
        """
        SELECT guild_id,topic_key,topic_family,lifecycle_status,
               canonical_ledger_entry_id,last_activity_at,channel_policy,
               route_mode,visibility
        FROM memory_moment_windows WHERE moment_id=?
        """,
        (moment_id,),
    ).fetchone()
    if not window or str(window[3] or "") != "finalized":
        return []
    canonical_entry_id = str(window[4] or "")
    if not canonical_entry_id:
        return []
    results: list[AtomicKnowledgeResult] = []
    contributions = conn.execute(
        """
        SELECT participant_key,contribution_gist,frame_type,lifecycle_status,
               public_usable
        FROM memory_moment_contributions
        WHERE moment_id=?
        ORDER BY participant_key
        """,
        (moment_id,),
    ).fetchall()
    for participant_key, gist, frame_type, lifecycle_status, public_usable in contributions:
        participant_key = str(participant_key or "")
        if (
            not participant_key
            or not str(gist or "").strip()
            or str(lifecycle_status or "") != REVIEW_ONLY_LIFECYCLE
            or not bool(public_usable)
        ):
            continue
        root_ids = tuple(
            str(row[0])
            for row in conn.execute(
                """
                SELECT ledger_entry_id
                FROM memory_moment_contribution_sources
                WHERE moment_id=? AND participant_key=?
                ORDER BY ledger_entry_id
                """,
                (moment_id, participant_key),
            ).fetchall()
        )
        frame_type = str(frame_type or "observation")
        if frame_type in {"question", "question_thread", "plan", "proposal", "conditional_plan"}:
            candidate_type = "open_loop_or_question"
            currentness = "open"
            epistemic = "question" if frame_type.startswith("question") else "source_abstraction"
        elif frame_type in {
            "correction",
            "correction_replacement",
            "replacement",
            "disagreement",
            "rejection",
        }:
            candidate_type = "inference_or_contested_claim"
            currentness = "uncertain"
            epistemic = "contested"
        elif frame_type == "preference":
            candidate_type = "person_role_fact"
            currentness = "current"
            epistemic = "source_abstraction"
        else:
            candidate_type = "topic_or_motif"
            currentness = "historical"
            epistemic = "source_abstraction"
        result = form_atomic_knowledge_candidate(
            conn,
            AtomicKnowledgeProposal(
                candidate_type=candidate_type,
                subject_key=participant_key,
                predicate_key=f"moment_{frame_type}_{window[1]}",
                meaning=str(gist or ""),
                root_entry_ids=root_ids,
                derivative_entry_ids=(canonical_entry_id,),
                participant_keys=(participant_key,),
                epistemic_status=epistemic,
                uncertainty_note=(
                    "Participant-specific Moment abstraction; not an exact quote."
                ),
                currentness=currentness,
                contradiction_key=f"{participant_key}:{frame_type}:{window[1]}",
                retrieval_tags=(
                    str(window[1] or ""),
                    str(window[2] or ""),
                    frame_type,
                    candidate_type,
                ),
            ),
        )
        results.append(result)
    return results


def _merge_backfill_count(counts: dict[str, int], outcome: str) -> None:
    key = str(outcome or "unknown")
    counts[key] = int(counts.get(key, 0) or 0) + 1


def backfill_atomic_knowledge_candidates(
    conn: sqlite3.Connection,
    *,
    batch_size: int = 250,
) -> dict[str, Any]:
    """Run one bounded, resumable historical pass; ordinary writes stay live."""
    ensure_memory_ledger_schema(conn)
    safe_batch = max(1, min(int(batch_size or 250), 500))
    state = conn.execute(
        """
        SELECT phase,cursor_value,completed,counts_json
        FROM memory_ledger_knowledge_backfill
        WHERE migration_key=?
        """,
        (ATOMIC_KNOWLEDGE_BACKFILL,),
    ).fetchone()
    if state:
        phase = str(state[0] or "entries")
        cursor = str(state[1] or "")
        completed = bool(state[2])
        try:
            counts = {
                str(key): int(value or 0)
                for key, value in json.loads(str(state[3] or "{}")).items()
            }
        except (TypeError, ValueError, json.JSONDecodeError):
            counts = {}
    else:
        phase, cursor, completed, counts = "entries", "", False, {}
        conn.execute(
            """
            INSERT INTO memory_ledger_knowledge_backfill(
              migration_key,phase,cursor_value,completed,counts_json,updated_at
            ) VALUES(?,?,?,?,?,?)
            """,
            (
                ATOMIC_KNOWLEDGE_BACKFILL,
                phase,
                cursor,
                0,
                "{}",
                _now(),
            ),
        )
    if completed:
        return {
            "migration": ATOMIC_KNOWLEDGE_BACKFILL,
            "phase": phase,
            "completed": True,
            "counts": counts,
        }

    if phase == "entries":
        rows = conn.execute(
            """
            SELECT entry_id
            FROM memory_ledger_entries
            WHERE entry_id>?
              AND entry_type IN (
                'claim','preference','boundary','goal','open_loop',
                'commitment','unresolved_question','event','canon_reference'
              )
            ORDER BY entry_id
            LIMIT ?
            """,
            (cursor, safe_batch),
        ).fetchall()
        for (entry_id,) in rows:
            result = form_atomic_candidate_from_ledger_entry(
                conn,
                str(entry_id),
            )
            _merge_backfill_count(
                counts,
                result.outcome if result is not None else "not_candidate",
            )
        if rows:
            cursor = str(rows[-1][0] or "")
        if len(rows) < safe_batch:
            phase, cursor = "moments", ""

    if phase == "moments":
        if conn.execute(
            """
            SELECT 1 FROM sqlite_master
            WHERE type='table' AND name='memory_moment_windows'
            """
        ).fetchone():
            rows = conn.execute(
                """
                SELECT moment_id
                FROM memory_moment_windows
                WHERE moment_id>? AND lifecycle_status='finalized'
                ORDER BY moment_id
                LIMIT ?
                """,
                (cursor, safe_batch),
            ).fetchall()
        else:
            rows = []
        for (moment_id,) in rows:
            results = form_atomic_candidates_from_moment(
                conn,
                str(moment_id),
            )
            if not results:
                _merge_backfill_count(counts, "moment_without_candidate")
            for result in results:
                _merge_backfill_count(counts, result.outcome)
        if rows:
            cursor = str(rows[-1][0] or "")
        if len(rows) < safe_batch:
            phase, cursor, completed = "complete", "", True

    conn.execute(
        """
        UPDATE memory_ledger_knowledge_backfill
        SET phase=?,cursor_value=?,completed=?,counts_json=?,updated_at=?
        WHERE migration_key=?
        """,
        (
            phase,
            cursor,
            1 if completed else 0,
            json.dumps(counts, sort_keys=True),
            _now(),
            ATOMIC_KNOWLEDGE_BACKFILL,
        ),
    )
    return {
        "migration": ATOMIC_KNOWLEDGE_BACKFILL,
        "phase": phase,
        "completed": bool(completed),
        "counts": counts,
    }


def purge_atomic_knowledge_for_subject(
    conn: sqlite3.Connection,
    *,
    guild_id: int,
    subject_key: str,
) -> dict[str, int]:
    """Complete-delete helper; ordinary forget uses lifecycle triggers."""
    ensure_memory_ledger_schema(conn)
    candidate_ids = {
        str(row[0])
        for row in conn.execute(
            """
            SELECT candidate_id
            FROM memory_ledger_knowledge_candidates
            WHERE guild_id=? AND subject_key=?
            UNION
            SELECT candidate_id
            FROM memory_ledger_knowledge_participants
            WHERE guild_id=? AND participant_key=?
            """,
            (guild_id, subject_key, guild_id, subject_key),
        ).fetchall()
    }
    counts = {
        "memory_ledger_knowledge_candidates": 0,
        "memory_ledger_knowledge_roots": 0,
        "memory_ledger_knowledge_participants": 0,
        "memory_ledger_knowledge_receipts": 0,
    }
    if not candidate_ids:
        return counts
    placeholders = ",".join("?" for _candidate_id in candidate_ids)
    params = tuple(sorted(candidate_ids))
    counts["memory_ledger_knowledge_receipts"] = conn.execute(
        """
        DELETE FROM memory_ledger_knowledge_receipts
        WHERE candidate_id IN (%s)
        """ % placeholders,
        params,
    ).rowcount
    counts["memory_ledger_knowledge_participants"] = conn.execute(
        """
        DELETE FROM memory_ledger_knowledge_participants
        WHERE candidate_id IN (%s)
        """ % placeholders,
        params,
    ).rowcount
    counts["memory_ledger_knowledge_roots"] = conn.execute(
        """
        DELETE FROM memory_ledger_knowledge_roots
        WHERE candidate_id IN (%s)
        """ % placeholders,
        params,
    ).rowcount
    counts["memory_ledger_knowledge_candidates"] = conn.execute(
        """
        DELETE FROM memory_ledger_knowledge_candidates
        WHERE candidate_id IN (%s)
        """ % placeholders,
        params,
    ).rowcount
    return counts


def _visibility(policy: str) -> Visibility:
    return map_channel_policy_visibility(policy) if has_explicit_channel_policy_mapping(policy) else Visibility.UNKNOWN


def _source_class(route: str, fallback: SourceClass) -> SourceClass:
    return map_route_source_label(route) if has_explicit_route_source_mapping(route) else fallback


def _conversation_correction_topic_tokens(value: str) -> set[str]:
    return {
        token
        for token in re.findall(r"[a-z0-9][a-z0-9'-]{2,40}", _canon(value))
        if token not in _CORRECTION_TOPIC_STOPWORDS
    }


def _finalized_conversation_correction_resolution(
    conn: sqlite3.Connection,
    *,
    guild_id: int,
    subject_key: str,
    correction_value: str,
    channel_policy: str,
    current_entry_id: str,
) -> tuple[str, tuple[str, ...]]:
    """Resolve only one same-author finalized source; ambiguity never guesses."""
    if (
        not _CONVERSATION_CORRECTION_RE.search(correction_value or "")
        or _canon(channel_policy) not in {"public_home", "public_context"}
        or not conn.execute(
            """
            SELECT 1 FROM sqlite_master
            WHERE type='table' AND name='memory_moment_windows'
            """
        ).fetchone()
    ):
        return "", ()
    correction_tokens = _conversation_correction_topic_tokens(
        correction_value
    )
    if not correction_tokens:
        return "", ()
    rows = conn.execute(
        """
        SELECT DISTINCT e.entry_id,e.normalized_value
        FROM memory_ledger_entries e
        JOIN memory_moment_members m
          ON m.ledger_entry_id=e.entry_id
        JOIN memory_moment_windows w
          ON w.moment_id=m.moment_id
        WHERE e.guild_id=? AND e.subject_key=?
          AND e.entry_id<>?
          AND e.source_table='conversations'
          AND e.source_role='user'
          AND e.entry_type='observation'
          AND e.lifecycle_status='active'
          AND e.public_usable=1
          AND e.channel_policy IN ('public_home','public_context')
          AND w.guild_id=e.guild_id
          AND w.lifecycle_status='finalized'
          AND w.public_usable=1
          AND NOT EXISTS (
            SELECT 1 FROM memory_ledger_lineage l
            WHERE l.guild_id=e.guild_id
              AND l.target_entry_id=e.entry_id
              AND l.lineage_type IN (
                'correction_of','supersedes','retracts'
              )
          )
        ORDER BY e.observed_at DESC,e.source_sequence DESC,e.entry_id DESC
        LIMIT 40
        """,
        (int(guild_id or 0), subject_key, current_entry_id),
    ).fetchall()
    ranked: list[tuple[int, str]] = []
    for entry_id, value in rows:
        overlap = len(
            correction_tokens
            & _conversation_correction_topic_tokens(str(value or ""))
        )
        if overlap > 0:
            ranked.append((overlap, str(entry_id or "")))
    if not ranked:
        return "", ()
    highest = max(score for score, _entry_id in ranked)
    # One shared word is not enough to connect an ordinary correction to an
    # older finalized Moment.  A mistaken supersession is worse than leaving
    # the correction unlinked for later context-aware review.
    if highest < 2:
        return "", ()
    strongest = {
        entry_id
        for score, entry_id in ranked
        if score == highest and entry_id
    }
    if len(strongest) == 1:
        return next(iter(strongest)), ()
    return "", tuple(sorted(strongest))


def _public_ok(subject_key: str, predicate: str, value: str, source_class: SourceClass, visibility: Visibility, confidence: Confidence, *, valid: bool = True, projection: bool = False) -> bool:
    claim = SourceClaim(stable_entry_id(guild_id=0, source_table="eval", source_row_id=0, entry_type="claim", subject_key=subject_key, predicate_key=predicate), SubjectIdentity(subject_key, subject_key), predicate, value, source_class, visibility, confidence, valid=valid, projection=projection)
    return is_public_usable(claim)



def _current_first_party_fact(
    conn: sqlite3.Connection,
    *,
    guild_id: int,
    subject_key: str,
    predicate_key: str,
) -> tuple[str, str]:
    ensure_memory_ledger_schema(conn)
    rows = conn.execute(
        """
        SELECT entry_id, normalized_value
        FROM memory_ledger_entries
        WHERE guild_id=? AND subject_key=? AND predicate_key=?
          AND source_class IN ('first_party_record','owner_correction')
          AND lifecycle_status='active'
          AND entry_id NOT IN (SELECT target_entry_id FROM memory_ledger_lineage WHERE guild_id=? AND lineage_type IN ('supersedes','retracts'))
        ORDER BY observed_at DESC, created_at DESC, entry_id DESC
        """,
        (guild_id, subject_key, predicate_key, guild_id),
    ).fetchall()
    if not rows:
        return "", ""
    return str(rows[0][0] or ""), str(rows[0][1] or "")


def shadow_conversation_row(
    conn: sqlite3.Connection,
    *,
    row_id: int,
    user_id: int,
    user_name: str,
    guild_id: int,
    role: str,
    content: str,
    channel_name: str = "",
    channel_policy: str = "unknown",
    channel_id: int = 0,
    message_id: int | None = None,
    route_mode: str = "unknown",
    observed_at: str = "",
    conversation_target_user_ids: tuple[int, ...] = (),
) -> LedgerWriteResult:
    role_norm = (role or "").lower()
    visibility = _visibility(channel_policy)
    if role_norm != "user":
        subject_key = BNL_SUBJECT_KEY
        target_user_ids = tuple(
            sorted(
                {
                    int(target_user_id)
                    for target_user_id in (
                        conversation_target_user_ids
                        or ((user_id,) if int(user_id or 0) > 0 else ())
                    )
                    if int(target_user_id or 0) > 0
                }
            )
        )
        participants = (
            LedgerParticipant(BNL_SUBJECT_KEY, "BNL-01", "author", 0),
            *tuple(
                LedgerParticipant(
                    subject_key_for_user(target_user_id),
                    "",
                    "conversation_target",
                    index,
                )
                for index, target_user_id in enumerate(
                    target_user_ids,
                    start=1,
                )
            ),
        )
        entry = LedgerEntry(guild_id=guild_id, source_table="conversations", source_row_id=row_id, source_revision=str(row_id), source_role="model", entry_type="derived_summary", subject_key=BNL_SUBJECT_KEY, subject_display_name="BNL-01", predicate_key="model_output", value=(content or "")[:500], source_class=SourceClass.DERIVED_SUMMARY, route_mode=route_mode, channel_id=channel_id, channel_name=channel_name, channel_policy=channel_policy, source_message_id=message_id, visibility=visibility, confidence=Confidence.LOW, public_usable=False, derived=True, projection=True, salience=0.1, observed_at=observed_at or _now(), source_sequence=row_id, participants=participants)
        return insert_ledger_entry(conn, entry)
    subject_key = subject_key_for_user(user_id)
    source_class = _source_class("conversation_continuity", SourceClass.PUBLIC_OBSERVATION)
    value = (content or "")[:500]
    public_ok = _public_ok(
        subject_key,
        "conversation",
        value,
        source_class,
        visibility,
        Confidence.MEDIUM,
    )
    result = insert_ledger_entry(
        conn,
        LedgerEntry(
            guild_id=guild_id,
            source_table="conversations",
            source_row_id=row_id,
            source_revision=str(row_id),
            source_role="user",
            entry_type="observation",
            subject_key=subject_key,
            subject_display_name=user_name or "",
            predicate_key="conversation",
            value=value,
            source_class=source_class,
            route_mode=route_mode,
            channel_id=channel_id,
            channel_name=channel_name,
            channel_policy=channel_policy,
            source_message_id=message_id,
            visibility=visibility,
            confidence=Confidence.MEDIUM,
            public_usable=public_ok,
            salience=0.2,
            observed_at=observed_at or _now(),
            source_sequence=row_id,
            participants=(LedgerParticipant(subject_key, user_name or "", "author", 0),),
        ),
    )
    if result.outcome == "inserted":
        (
            correction_target,
            ambiguous_correction_targets,
        ) = _finalized_conversation_correction_resolution(
            conn,
            guild_id=guild_id,
            subject_key=subject_key,
            correction_value=value,
            channel_policy=channel_policy,
            current_entry_id=result.entry_id,
        )
        if correction_target:
            now = _now()
            for lineage_type in ("correction_of", "supersedes"):
                conn.execute(
                    """
                    INSERT OR IGNORE INTO memory_ledger_lineage
                      (entry_id,guild_id,lineage_type,target_entry_id,created_at)
                    VALUES (?,?,?,?,?)
                    """,
                    (
                        result.entry_id,
                        int(guild_id or 0),
                        lineage_type,
                        correction_target,
                        now,
                    ),
                )
        elif ambiguous_correction_targets:
            placeholders = ",".join(
                "?" for _target in ambiguous_correction_targets
            )
            conn.execute(
                f"""
                UPDATE memory_moment_windows
                SET lifecycle_status='needs_review',updated_at=?
                WHERE guild_id=? AND lifecycle_status='finalized'
                  AND moment_id IN (
                    SELECT moment_id FROM memory_moment_members
                    WHERE ledger_entry_id IN ({placeholders})
                  )
                """,
                (
                    _now(),
                    int(guild_id or 0),
                    *ambiguous_correction_targets,
                ),
            )
    return result


def shadow_first_party_user_fact(
    conn: sqlite3.Connection,
    *,
    row_id: int,
    user_id: int,
    user_name: str,
    guild_id: int,
    fact_key: str,
    fact_value: str,
    channel_name: str = "",
    channel_policy: str = "unknown",
    channel_id: int = 0,
    message_id: int | None = None,
    route_mode: str = "unknown",
    observed_at: str = "",
) -> LedgerWriteResult:
    """Project one approved direct self-report from its conversation source.

    This remains a shadow write. The source conversation row is the authority;
    legacy ``user_memory_facts`` is retained only for production compatibility.
    Repetition does not create a stronger entry, while a later direct value
    supersedes the prior current value for that same field.
    """
    key = _canon(fact_key)
    value = re.sub(r"\s+", " ", str(fact_value or "")).strip()[:500]
    if key not in APPROVED_SELF_AUTHORED_FACT_KEYS:
        return skipped_result(
            guild_id=guild_id,
            source_table="conversations",
            source_row_id=row_id,
            source_revision=str(row_id),
            reason_code="self_authored_fact_not_allowlisted",
        )
    if not value:
        return skipped_result(
            guild_id=guild_id,
            source_table="conversations",
            source_row_id=row_id,
            source_revision=str(row_id),
            reason_code="empty_self_authored_fact",
        )

    subject_key = subject_key_for_user(user_id)
    prior_entry_id, prior_value = _current_first_party_fact(
        conn,
        guild_id=guild_id,
        subject_key=subject_key,
        predicate_key=key,
    )
    if prior_entry_id and _canon(prior_value) == _canon(value):
        return skipped_result(
            guild_id=guild_id,
            source_table="conversations",
            source_row_id=row_id,
            source_revision=str(row_id),
            reason_code="repeated_self_authored_value",
        )

    visibility = _visibility(channel_policy)
    source_class = SourceClass.FIRST_PARTY_RECORD
    public_ok = _public_ok(
        subject_key,
        key,
        value,
        source_class,
        visibility,
        Confidence.HIGH,
    )
    lineage = (
        (("supersedes", prior_entry_id), ("correction_of", prior_entry_id))
        if prior_entry_id
        else ()
    )
    result = insert_ledger_entry(
        conn,
        LedgerEntry(
            guild_id=guild_id,
            source_table="conversations",
            source_row_id=row_id,
            source_revision=str(row_id),
            source_role="member_self_report",
            entry_type="preference",
            subject_key=subject_key,
            subject_display_name=user_name or "",
            predicate_key=key,
            value=value,
            source_class=source_class,
            route_mode=route_mode,
            channel_id=channel_id,
            channel_name=channel_name,
            channel_policy=channel_policy,
            source_message_id=message_id,
            visibility=visibility,
            confidence=Confidence.HIGH,
            public_usable=public_ok,
            salience=0.45,
            observed_at=observed_at or _now(),
            source_sequence=row_id,
            participants=(LedgerParticipant(subject_key, user_name or "", "author", 0),),
            lineage=lineage,
        ),
    )
    if result.outcome == "inserted" and prior_entry_id:
        conn.execute(
            """
            UPDATE memory_ledger_entries
            SET lifecycle_status='superseded', updated_at=?
            WHERE guild_id=? AND entry_id=?
            """,
            (_now(), int(guild_id or 0), prior_entry_id),
        )
    return result


def shadow_member_control_fact(
    conn: sqlite3.Connection,
    *,
    row_id: int,
    user_id: int,
    guild_id: int,
    fact_key: str,
    fact_value: str,
    control_ref: str,
    observed_at: str = "",
) -> LedgerWriteResult:
    """Project an explicit member control as first-party authoritative input."""
    key = _canon(fact_key)
    value = re.sub(r"\s+", " ", str(fact_value or "")).strip()[:500]
    revision = "control:" + _canon(control_ref or f"fact:{row_id}")
    if key not in APPROVED_SELF_AUTHORED_FACT_KEYS:
        return skipped_result(
            guild_id=guild_id,
            source_table="user_memory_facts",
            source_row_id=row_id,
            source_revision=revision,
            reason_code="member_control_fact_not_allowlisted",
        )
    if not value:
        return skipped_result(
            guild_id=guild_id,
            source_table="user_memory_facts",
            source_row_id=row_id,
            source_revision=revision,
            reason_code="empty_member_control_fact",
        )
    subject_key = subject_key_for_user(user_id)
    prior_entry_id, prior_value = _current_first_party_fact(
        conn,
        guild_id=guild_id,
        subject_key=subject_key,
        predicate_key=key,
    )
    if prior_entry_id and _canon(prior_value) == _canon(value):
        return skipped_result(
            guild_id=guild_id,
            source_table="user_memory_facts",
            source_row_id=row_id,
            source_revision=revision,
            reason_code="repeated_member_control_value",
        )
    lineage = (
        (("supersedes", prior_entry_id), ("correction_of", prior_entry_id))
        if prior_entry_id
        else ()
    )
    result = insert_ledger_entry(
        conn,
        LedgerEntry(
            guild_id=guild_id,
            source_table="user_memory_facts",
            source_row_id=row_id,
            source_revision=revision,
            source_event_key=_canon(control_ref),
            source_role="member_control",
            entry_type="preference",
            subject_key=subject_key,
            predicate_key=key,
            value=value,
            source_class=SourceClass.FIRST_PARTY_RECORD,
            route_mode="member_control",
            channel_policy="member_control",
            visibility=Visibility.PUBLIC_SAFE,
            confidence=Confidence.HIGH,
            public_usable=True,
            salience=0.5,
            observed_at=observed_at or _now(),
            source_sequence=int(row_id or 0),
            participants=(
                LedgerParticipant(subject_key, "", "control_actor", 0),
            ),
            lineage=lineage,
        ),
    )
    if result.outcome == "inserted" and prior_entry_id:
        conn.execute(
            """
            UPDATE memory_ledger_entries
            SET lifecycle_status='superseded', updated_at=?
            WHERE guild_id=? AND entry_id=?
            """,
            (_now(), int(guild_id or 0), prior_entry_id),
        )
    return result


def shadow_user_fact_row(conn: sqlite3.Connection, *, row_id: int, user_id: int, guild_id: int, fact_key: str, fact_value: str, confidence: float = 0.7, updated_at: str = "") -> LedgerWriteResult:
    rev = source_revision_for(row_id, updated_at)
    return insert_ledger_entry(conn, LedgerEntry(guild_id=guild_id, source_table="user_memory_facts", source_row_id=row_id, source_revision=rev, source_role="legacy_source_blind", entry_type="claim", subject_key=subject_key_for_user(user_id), predicate_key=fact_key or "legacy_fact", value=(fact_value or "")[:500], source_class=SourceClass.LEGACY_SOURCE_BLIND, visibility=Visibility.PRIVATE, confidence=Confidence.LOW, public_usable=False, observed_at=updated_at or _now(), source_sequence=int(row_id or 0), lifecycle_status=REVIEW_ONLY_LIFECYCLE, participants=(LedgerParticipant(subject_key_for_user(user_id), "", "subject", 0),)))


def _entry_ids_for_source_rows(conn: sqlite3.Connection, *, guild_id: int, source_table: str, source_row_ids: tuple[int, ...]) -> tuple[str, ...]:
    ensure_memory_ledger_schema(conn)
    ids: list[str] = []
    for source_row_id in source_row_ids:
        rows = conn.execute(
            "SELECT entry_id FROM memory_ledger_entries WHERE guild_id=? AND source_table=? AND source_row_id=? ORDER BY created_at DESC",
            (guild_id, source_table, str(source_row_id)),
        ).fetchall()
        if len(rows) == 1:
            ids.append(rows[0][0])
    return tuple(sorted(set(ids)))


def shadow_memory_tier_row(conn: sqlite3.Connection, *, row_id: int, user_id: int, guild_id: int, tier: str, summary: str, salience: float = 0.5, channel_policy: str = "legacy_unknown", topic_key: str = "", updated_at: str = "", derived_from_entry_ids: tuple[str, ...] = (), derived_from_source_row_ids: tuple[int, ...] = ()) -> LedgerWriteResult:
    rev = source_revision_for(row_id, updated_at)
    real_source_ids = _entry_ids_for_source_rows(conn, guild_id=guild_id, source_table="memory_tiers", source_row_ids=derived_from_source_row_ids)
    lineage = tuple(("derived_from", eid) for eid in sorted(set(tuple(derived_from_entry_ids) + real_source_ids)) if eid)
    return insert_ledger_entry(conn, LedgerEntry(guild_id=guild_id, source_table="memory_tiers", source_row_id=row_id, source_revision=rev, source_role="derived_projection", entry_type="derived_summary", subject_key=subject_key_for_user(user_id), predicate_key=topic_key or f"memory_tier:{tier}", value=(summary or "")[:500], source_class=SourceClass.DERIVED_SUMMARY, visibility=Visibility.PRIVATE, confidence=Confidence.LOW, public_usable=False, derived=True, projection=True, salience=salience, observed_at=updated_at or _now(), source_sequence=int(row_id or 0), lifecycle_status=REVIEW_ONLY_LIFECYCLE, participants=(LedgerParticipant(subject_key_for_user(user_id), "", "subject", 0),), lineage=lineage))


def shadow_relationship_journal_row(conn: sqlite3.Connection, *, row_id: int, user_id: int, guild_id: int, entry_type: str, summary: str, timestamp: str = "") -> LedgerWriteResult:
    return insert_ledger_entry(conn, LedgerEntry(guild_id=guild_id, source_table="relationship_journal", source_row_id=row_id, source_revision=str(row_id), source_role="internal", entry_type="relationship_event", subject_key=subject_key_for_user(user_id), predicate_key=entry_type or "relationship_event", value=(summary or "")[:500], source_class=SourceClass.DERIVED_SUMMARY, visibility=Visibility.INTERNAL, confidence=Confidence.LOW, public_usable=False, derived=True, projection=True, salience=0.3, observed_at=timestamp or _now(), source_sequence=int(row_id or 0), lifecycle_status=REVIEW_ONLY_LIFECYCLE, participants=(LedgerParticipant(subject_key_for_user(user_id), "", "subject", 0),)))


def _unique_entry_for_source(conn: sqlite3.Connection, *, guild_id: int, source_table: str, source_row_id: int | str, preferred_lifecycle: str | None = None) -> str:
    ensure_memory_ledger_schema(conn)
    sql = "SELECT entry_id FROM memory_ledger_entries WHERE guild_id=? AND source_table=? AND source_row_id=?"
    params: list[Any] = [guild_id, source_table, str(source_row_id)]
    if preferred_lifecycle:
        sql += " AND lifecycle_status=?"
        params.append(preferred_lifecycle)
    rows = conn.execute(sql + " ORDER BY created_at DESC", params).fetchall()
    return rows[0][0] if len(rows) == 1 else ""


def shadow_broadcast_memory_row(conn: sqlite3.Connection, *, row_id: int, guild_id: int, cleaned_summary: str, entry_type: str, public_safe: bool, status: str, usage_scope: str, submitted_by_user_id: int | None = None, submitted_by_name: str = "", created_at: str = "", updated_at: str = "", supersedes_id: int | None = None) -> LedgerWriteResult:
    if not cleaned_summary:
        return skipped_result(guild_id=guild_id, source_table="broadcast_memory", source_row_id=row_id, reason_code="empty_cleaned_summary", source_revision=source_revision_for(row_id, updated_at or created_at))
    lifecycle = ACTIVE_LIFECYCLE if str(status or "active").lower() == "active" else (RESOLVED_LIFECYCLE if str(status or "").lower() == "resolved" else REVIEW_ONLY_LIFECYCLE)
    scopes = {scope.strip().lower() for scope in re.split(r"[,\s]+", usage_scope or "") if scope.strip()}
    public_ok = bool(public_safe and lifecycle == ACTIVE_LIFECYCLE and bool(scopes & {"ambient", "direct", "show_status", "relay"}))
    target = _unique_entry_for_source(conn, guild_id=guild_id, source_table="broadcast_memory", source_row_id=supersedes_id, preferred_lifecycle=ACTIVE_LIFECYCLE) if supersedes_id else ""
    lineage = (("supersedes", target), ("correction_of", target)) if target else ()
    rev = source_revision_for(row_id, updated_at or created_at)
    return insert_ledger_entry(conn, LedgerEntry(guild_id=guild_id, source_table="broadcast_memory", source_row_id=row_id, source_revision=rev, source_role="broadcast_memory", entry_type="show_event" if "show" in (entry_type or "") else "event", subject_key="barcode_radio", subject_display_name="BARCODE Radio", predicate_key=entry_type or "broadcast_memory", value=cleaned_summary[:500], source_class=SourceClass.FIRST_PARTY_RECORD, visibility=Visibility.PUBLIC_SAFE if public_ok else Visibility.INTERNAL, confidence=Confidence.HIGH if public_ok else Confidence.MEDIUM, public_usable=public_ok, salience=0.5, observed_at=created_at or updated_at or _now(), source_sequence=int(row_id or 0), freshness=usage_scope or "", lifecycle_status=lifecycle, participants=tuple([LedgerParticipant(f"discord_user:{submitted_by_user_id}", submitted_by_name or "", "submitter", 0)] if submitted_by_user_id else ()), lineage=lineage))


def _unique_broadcast_primary_entry(conn: sqlite3.Connection, *, guild_id: int, source_row_id: int | str) -> str:
    ensure_memory_ledger_schema(conn)
    rows = conn.execute(
        "SELECT entry_id FROM memory_ledger_entries WHERE guild_id=? AND source_table='broadcast_memory' AND source_row_id=? AND source_role='broadcast_memory' ORDER BY created_at DESC",
        (guild_id, str(source_row_id)),
    ).fetchall()
    return rows[0][0] if len(rows) == 1 else ""


def shadow_broadcast_status_event(conn: sqlite3.Connection, *, row_id: int, guild_id: int, status: str, updated_at: str, actor_id: int | None = None, actor_name: str = "", superseded_by_id: int | None = None) -> LedgerWriteResult:
    rev = source_revision_for(row_id, updated_at, event=f"status:{status}:{updated_at}")
    lineage = ()
    reason_override = "ok"
    if status == "superseded" and superseded_by_id:
        old_entry = _unique_broadcast_primary_entry(conn, guild_id=guild_id, source_row_id=row_id)
        replacement_entry = _unique_broadcast_primary_entry(conn, guild_id=guild_id, source_row_id=superseded_by_id)
        if old_entry and replacement_entry:
            lineage = (("derived_from", old_entry), ("derived_from", replacement_entry))
        else:
            reason_override = "unresolved_broadcast_status_lineage"
    lifecycle = RESOLVED_LIFECYCLE if status == "resolved" else REVIEW_ONLY_LIFECYCLE
    predicate = f"broadcast_status:{status or 'unknown'}"
    result = insert_ledger_entry(conn, LedgerEntry(guild_id=guild_id, source_table="broadcast_memory", source_row_id=row_id, source_revision=rev, source_event_key=f"status:{status}", source_role="broadcast_memory_status", entry_type="event", subject_key="barcode_radio", subject_display_name="BARCODE Radio", predicate_key=predicate, value=status or "unknown", source_class=SourceClass.FIRST_PARTY_RECORD, visibility=Visibility.INTERNAL, confidence=Confidence.HIGH, public_usable=False, observed_at=updated_at or _now(), source_sequence=int(row_id or 0), lifecycle_status=lifecycle, participants=tuple([LedgerParticipant(f"discord_user:{actor_id}", actor_name or "", "correction_actor", 0)] if actor_id else ()), lineage=lineage))
    if reason_override != "ok" and result.outcome == "inserted":
        return LedgerWriteResult(result.entry_id, result.outcome, reason_override, result.source_table, result.source_row_id, result.source_revision, result.source_event_key, result.guild_id)
    return result


def shadow_canon_reference(conn: sqlite3.Connection, *, guild_id: int, canon_id: str, subject_key: str, subject_display_name: str, predicate_key: str, value: str, observed_at: str = "") -> LedgerWriteResult:
    if not canon_id or not subject_key or not predicate_key:
        return skipped_result(guild_id=guild_id, source_table="approved_canon", source_row_id=canon_id or "", reason_code="missing_canon_source_identity")
    return insert_ledger_entry(conn, LedgerEntry(guild_id=guild_id, source_table="approved_canon", source_row_id=canon_id, source_revision=str(canon_id), source_role="approved_canon", entry_type="canon_reference", subject_key=subject_key, subject_display_name=subject_display_name, predicate_key=predicate_key, value=(value or "")[:500], source_class=SourceClass.APPROVED_CANON, route_mode="approved_canon", channel_policy="reference_canon", visibility=Visibility.REFERENCE_CANON, confidence=Confidence.APPROVED, public_usable=True, observed_at=observed_at or _now(), lifecycle_status=ACTIVE_LIFECYCLE))


def build_memory_ledger_evaluation(
    conn: sqlite3.Connection,
    *,
    guild_id: int | None = None,
    prepare_schema: bool = True,
) -> dict[str, Any]:
    if prepare_schema:
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
    cur.execute(f"SELECT COUNT(DISTINCT entry_id) FROM memory_ledger_lineage{where + (' AND' if where else ' WHERE')} lineage_type IN ('correction_of','supersedes')", params)
    report["explicitCorrectionCounts"] = int(cur.fetchone()[0] or 0)
    cur.execute(f"SELECT COUNT(*) FROM memory_ledger_entries{where + (' AND' if where else ' WHERE')} lifecycle_status='review_only' AND predicate_key='remembered_number'", params)
    report["unresolvedCorrectionAttempts"] = int(cur.fetchone()[0] or 0)
    cur.execute(f"SELECT COUNT(*) FROM memory_ledger_shadow_receipts{where + (' AND' if where else ' WHERE')} outcome IN ('inserted','deduplicated') AND (entry_id='' OR entry_id NOT IN (SELECT entry_id FROM memory_ledger_entries WHERE {'guild_id=? AND ' if guild_id is not None else ''} 1=1))", params + ([guild_id] if guild_id is not None else []))
    missing_receipt_entries = int(cur.fetchone()[0] or 0)
    cur.execute(f"SELECT COUNT(*) FROM memory_ledger_entries e{where.replace('WHERE', 'WHERE e.') if where else ''} AND NOT EXISTS (SELECT 1 FROM memory_ledger_shadow_receipts r WHERE r.guild_id=e.guild_id AND r.entry_id=e.entry_id AND r.outcome IN ('inserted','deduplicated'))" if where else "SELECT COUNT(*) FROM memory_ledger_entries e WHERE NOT EXISTS (SELECT 1 FROM memory_ledger_shadow_receipts r WHERE r.guild_id=e.guild_id AND r.entry_id=e.entry_id AND r.outcome IN ('inserted','deduplicated'))", params)
    entries_without_receipts = int(cur.fetchone()[0] or 0)
    cur.execute(f"SELECT COUNT(*) FROM memory_ledger_lineage l{where.replace('WHERE', 'WHERE l.') if where else ''} AND NOT EXISTS (SELECT 1 FROM memory_ledger_entries e WHERE e.guild_id=l.guild_id AND e.entry_id=l.target_entry_id)" if where else "SELECT COUNT(*) FROM memory_ledger_lineage l WHERE NOT EXISTS (SELECT 1 FROM memory_ledger_entries e WHERE e.guild_id=l.guild_id AND e.entry_id=l.target_entry_id)", params)
    dangling_lineage = int(cur.fetchone()[0] or 0)
    report["danglingLineageTargets"] = dangling_lineage
    report["legacyToLedgerParityMismatches"] = missing_receipt_entries + entries_without_receipts + dangling_lineage + int(report.get("shadowWriteErrors", 0)) + int(report.get("unresolvedCorrectionAttempts", 0))
    knowledge_tables_present = bool(
        cur.execute(
            """
            SELECT COUNT(*) FROM sqlite_master
            WHERE type='table' AND name IN (
              'memory_ledger_knowledge_candidates',
              'memory_ledger_knowledge_roots',
              'memory_ledger_knowledge_participants',
              'memory_ledger_knowledge_receipts'
            )
            """
        ).fetchone()[0]
        == 4
    )
    report["knowledgeCandidateSchemaVersion"] = (
        ATOMIC_KNOWLEDGE_SCHEMA_VERSION if knowledge_tables_present else "absent"
    )
    report["knowledgeCandidateTablesPresent"] = knowledge_tables_present
    report["knowledgeCandidateTotalsByType"] = {}
    report["knowledgeCandidateTotalsByState"] = {}
    report["knowledgeCandidateTotalsByVisibility"] = {}
    report["knowledgeCandidateTotalsByAuthority"] = {}
    report["knowledgeCandidateTotalsByConfidence"] = {}
    report["knowledgeCandidateTotalsByEpistemicStatus"] = {}
    report["knowledgeCandidateTotalsByCurrentness"] = {}
    report["knowledgeCandidateReceiptEvents"] = {}
    report["knowledgeCandidateRejectionsByReason"] = {}
    report["knowledgeCandidateInvalidationsByReason"] = {}
    report["knowledgeCandidateRootKinds"] = {}
    report["knowledgeCandidateRootCount"] = 0
    report["knowledgeCandidateIndependentRootCount"] = 0
    report["knowledgeCandidateDerivativeRootCount"] = 0
    report["knowledgeCandidateDerivativeOnlyRejections"] = 0
    report["knowledgeCandidateAmbiguousRejections"] = 0
    report["knowledgeCandidateOrphanedRoots"] = 0
    report["knowledgeCandidateMissingIndependentRoots"] = 0
    report["knowledgeCandidateParticipantIsolationViolations"] = 0
    report["knowledgeCandidateLiveEligibleCount"] = 0
    report["knowledgeCandidateProcessingErrors"] = 0
    report["knowledgeCandidateCorrectionDeletePrivacyInvalidations"] = 0
    report["knowledgeCandidateBackfill"] = {
        "phase": "not_started",
        "completed": False,
        "counts": {},
    }
    if knowledge_tables_present:
        candidate_where = ""
        candidate_params: list[Any] = []
        if guild_id is not None:
            candidate_where = " WHERE guild_id=?"
            candidate_params = [guild_id]
        for key, column in (
            ("knowledgeCandidateTotalsByType", "candidate_type"),
            ("knowledgeCandidateTotalsByState", "candidate_state"),
            ("knowledgeCandidateTotalsByVisibility", "visibility"),
            ("knowledgeCandidateTotalsByAuthority", "authority_class"),
            ("knowledgeCandidateTotalsByConfidence", "confidence_class"),
            (
                "knowledgeCandidateTotalsByEpistemicStatus",
                "epistemic_status",
            ),
            ("knowledgeCandidateTotalsByCurrentness", "currentness"),
        ):
            cur.execute(
                f"""
                SELECT {column},COUNT(*)
                FROM memory_ledger_knowledge_candidates
                {candidate_where}
                GROUP BY {column}
                """,
                candidate_params,
            )
            report[key] = dict(cur.fetchall())
        cur.execute(
            f"""
            SELECT event_type,COUNT(*)
            FROM memory_ledger_knowledge_receipts
            {candidate_where}
            GROUP BY event_type
            """,
            candidate_params,
        )
        report["knowledgeCandidateReceiptEvents"] = dict(cur.fetchall())
        rejection_suffix = " AND" if candidate_where else " WHERE"
        cur.execute(
            f"""
            SELECT reason_code,COUNT(*)
            FROM memory_ledger_knowledge_receipts
            {candidate_where}{rejection_suffix} event_type='rejected'
            GROUP BY reason_code
            """,
            candidate_params,
        )
        report["knowledgeCandidateRejectionsByReason"] = dict(cur.fetchall())
        cur.execute(
            f"""
            SELECT invalidated_reason,COUNT(*)
            FROM memory_ledger_knowledge_candidates
            {candidate_where}{rejection_suffix}
              COALESCE(invalidated_reason,'')<>''
            GROUP BY invalidated_reason
            """,
            candidate_params,
        )
        report["knowledgeCandidateInvalidationsByReason"] = dict(
            cur.fetchall()
        )
        cur.execute(
            f"""
            SELECT root_kind,COUNT(*)
            FROM memory_ledger_knowledge_roots
            {candidate_where}
            GROUP BY root_kind
            """,
            candidate_params,
        )
        report["knowledgeCandidateRootKinds"] = dict(cur.fetchall())
        cur.execute(
            f"""
            SELECT
              COUNT(*),
              COALESCE(SUM(CASE WHEN is_independent=1 THEN 1 ELSE 0 END),0),
              COALESCE(SUM(CASE WHEN is_independent=0 THEN 1 ELSE 0 END),0)
            FROM memory_ledger_knowledge_roots
            {candidate_where}
            """,
            candidate_params,
        )
        root_counts = cur.fetchone() or (0, 0, 0)
        report["knowledgeCandidateRootCount"] = int(root_counts[0] or 0)
        report["knowledgeCandidateIndependentRootCount"] = int(
            root_counts[1] or 0
        )
        report["knowledgeCandidateDerivativeRootCount"] = int(
            root_counts[2] or 0
        )
        cur.execute(
            f"""
            SELECT COUNT(*)
            FROM memory_ledger_knowledge_receipts
            {candidate_where}{rejection_suffix}
              event_type='rejected'
              AND reason_code LIKE 'derivative%'
            """,
            candidate_params,
        )
        report["knowledgeCandidateDerivativeOnlyRejections"] = int(
            cur.fetchone()[0] or 0
        )
        cur.execute(
            f"""
            SELECT COUNT(*)
            FROM memory_ledger_knowledge_receipts
            {candidate_where}{rejection_suffix}
              event_type='rejected'
              AND (
                reason_code LIKE 'ambiguous%'
                OR reason_code LIKE '%isolation%'
                OR reason_code='participant_scope_mismatch'
                OR reason_code='cross_guild_or_ambiguous_provenance'
              )
            """,
            candidate_params,
        )
        report["knowledgeCandidateAmbiguousRejections"] = int(
            cur.fetchone()[0] or 0
        )
        root_alias_where = ""
        root_alias_params: list[Any] = []
        if guild_id is not None:
            root_alias_where = " AND r.guild_id=?"
            root_alias_params = [guild_id]
        cur.execute(
            f"""
            SELECT COUNT(*)
            FROM memory_ledger_knowledge_roots r
            LEFT JOIN memory_ledger_entries e
              ON e.guild_id=r.guild_id AND e.entry_id=r.root_entry_id
            WHERE e.entry_id IS NULL
              AND r.root_status NOT IN (
                'deleted','forgotten','retracted','superseded','corrected'
              )
              {root_alias_where}
            """,
            root_alias_params,
        )
        report["knowledgeCandidateOrphanedRoots"] = int(
            cur.fetchone()[0] or 0
        )
        candidate_alias_where = ""
        candidate_alias_params: list[Any] = []
        if guild_id is not None:
            candidate_alias_where = " WHERE c.guild_id=?"
            candidate_alias_params = [guild_id]
        cur.execute(
            f"""
            SELECT COUNT(*)
            FROM memory_ledger_knowledge_candidates c
            {candidate_alias_where}
            {"AND" if candidate_alias_where else "WHERE"} NOT EXISTS (
              SELECT 1 FROM memory_ledger_knowledge_roots r
              WHERE r.candidate_id=c.candidate_id AND r.is_independent=1
            )
            """,
            candidate_alias_params,
        )
        report["knowledgeCandidateMissingIndependentRoots"] = int(
            cur.fetchone()[0] or 0
        )
        cur.execute(
            f"""
            SELECT COUNT(*)
            FROM memory_ledger_knowledge_candidates c
            {candidate_alias_where}
            {"AND" if candidate_alias_where else "WHERE"}
              c.subject_key LIKE 'discord_user:%'
              AND NOT EXISTS (
                SELECT 1 FROM memory_ledger_knowledge_participants p
                WHERE p.candidate_id=c.candidate_id
                  AND p.participant_key=c.subject_key
              )
            """,
            candidate_alias_params,
        )
        report["knowledgeCandidateParticipantIsolationViolations"] = int(
            cur.fetchone()[0] or 0
        )
        cur.execute(
            f"""
            SELECT COUNT(*)
            FROM memory_ledger_knowledge_candidates
            {candidate_where}{rejection_suffix} live_eligible<>0
            """,
            candidate_params,
        )
        report["knowledgeCandidateLiveEligibleCount"] = int(
            cur.fetchone()[0] or 0
        )
        if guild_id is None:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM memory_ledger_knowledge_receipts
                WHERE event_type='error'
                """
            )
        else:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM memory_ledger_knowledge_receipts
                WHERE guild_id IN (?,0) AND event_type='error'
                """,
                (guild_id,),
            )
        report["knowledgeCandidateProcessingErrors"] = int(
            cur.fetchone()[0] or 0
        )
        cur.execute(
            f"""
            SELECT COUNT(*)
            FROM memory_ledger_knowledge_receipts
            {candidate_where}{rejection_suffix}
              event_type IN ('invalidated','superseded')
              AND reason_code IN (
                'root_privacy_or_deletion','root_deleted','root_changed',
                'root_privacy_or_provenance_changed',
                'root_correction_of','root_supersedes','root_retracts',
                'explicit_root_supersession'
              )
            """,
            candidate_params,
        )
        report[
            "knowledgeCandidateCorrectionDeletePrivacyInvalidations"
        ] = int(cur.fetchone()[0] or 0)
        backfill = cur.execute(
            """
            SELECT phase,completed,counts_json
            FROM memory_ledger_knowledge_backfill
            WHERE migration_key=?
            """,
            (ATOMIC_KNOWLEDGE_BACKFILL,),
        ).fetchone()
        if backfill:
            try:
                backfill_counts = json.loads(str(backfill[2] or "{}"))
            except (TypeError, ValueError, json.JSONDecodeError):
                backfill_counts = {"invalid_json": 1}
            report["knowledgeCandidateBackfill"] = {
                "phase": str(backfill[0] or "unknown"),
                "completed": bool(backfill[1]),
                "counts": backfill_counts,
            }
    return report
