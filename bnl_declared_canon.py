"""Dependency-free Declared Canon lifecycle and Broadcast classification core.

This module owns no response route, projection, model call, or deployment gate.
It stores append-only owner declarations and typed metadata for rows whose
authoritative content remains in ``broadcast_memory``.

Trust boundary: ``actor_user_id`` must come from an authenticated Discord
message/interaction object.  This dependency-free core cannot authenticate a
network principal; it independently rechecks that opaque ID and the exact guild
against ``BNL_OWNER_USER_ID`` and ``BNL_PRIMARY_GUILD_ID`` on every public read
or mutation.  It accepts neither caller-built authority objects nor receipts.
Receipts are keyed integrity labels bound to the complete request and stored
revision.  The signing secret is runtime configuration, never caller input.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import hmac
import json
import os
import re
import sqlite3
from typing import Any, Iterable, Mapping, Sequence


DECLARED_CANON_CONTRACT_VERSION = "declared_canon_lifecycle_v1"
DECLARED_CANON_TABLE = "declared_canon_revisions"
GENERAL_DECLARATION_SOURCE = "general_declaration"
BROADCAST_MEMORY_SOURCE = "broadcast_memory"
BROADCAST_DECLARED_CANON_OWNER_ERA_CUTOFF = "2026-07-23T19:16:22+00:00"
BROADCAST_SOURCE_FINGERPRINT_VERSION = "broadcast_memory_complete_row_v2"
INTERNAL_AUTHORITY_RECEIPT_VERSION = "declared_canon_internal_receipt_v1"
STORED_AUTHORITY_BINDING_VERSION = "declared_canon_stored_payload_v1"
DECLARED_CANON_AUTHORITY_SECRET_ENV = "BNL_DECLARED_CANON_AUTHORITY_SECRET"
DECLARED_CANON_AUTHORITY_SECRET_MIN_BYTES = 32

# These fields are the minimum recognized Broadcast schema, not a fingerprint
# allowlist.  Every column returned by the authoritative source row, including
# unknown/future columns, is included in the v2 fingerprint.  Adding a column
# therefore invalidates prior review until the owner explicitly reclassifies it.
_BROADCAST_SOURCE_REQUIRED_FIELDS = (
    "id",
    "guild_id",
    "episode_date",
    "submitted_by_user_id",
    "submitted_by_name",
    "raw_note",
    "cleaned_summary",
    "entry_type",
    "importance",
    "public_safe",
    "affects_next_show",
    "usage_scope",
    "target_show_date",
    "valid_until",
    "override_span_count",
    "needs_clarification",
    "status",
    "created_at",
    "updated_at",
    "corrected_by_user_id",
    "corrected_by_name",
    "correction_reason",
    "supersedes_id",
    "superseded_by_id",
)

GENERAL_SUBJECT_TYPES = frozenset(
    {
        "entity",
        "person",
        "character",
        "project",
        "relationship",
        "broadcast",
        "event",
        "organization",
    }
)
CANON_DOMAINS = frozenset(
    {"real_community", "broadcast_history", "operational", "lore", "hybrid"}
)
CLAIM_KINDS = frozenset(
    {
        "identity",
        "role",
        "standing",
        "relationship",
        "contribution",
        "event",
        "current_state",
        "behavior_pattern",
        "tradition_or_joke",
        "world_rule",
        "other",
    }
)
VISIBILITIES = frozenset(
    {
        "public",
        "public_safe",
        "reference_canon",
        "internal",
        "private",
        "mod",
        "protected",
    }
)
PUBLIC_VISIBILITIES = frozenset({"public", "public_safe", "reference_canon"})
PUBLIC_ROUTES = frozenset(
    {
        "public_home",
        "public_context",
        "public_selective",
        "relay",
        "journal",
        "website",
    }
)
ALLOWED_ELIGIBLE_ROUTES = PUBLIC_ROUTES | frozenset(
    {
        "broadcast_memory",
        "declared_canon_review",
        "internal_controlled",
        "protected_system",
        "reference_canon",
        "sealed_test",
    }
)
GENERAL_LIFECYCLES = frozenset(
    {
        "established",
        "contested",
        "review_only",
        "resolved",
        "retired",
        "superseded",
    }
)
TERMINAL_LIFECYCLES = frozenset({"retired", "superseded"})

BROADCAST_TYPE_DEFAULTS = {
    "episode_arc": ("broadcast_history", "event"),
    "notable_moment": ("broadcast_history", "event"),
    "running_joke": ("hybrid", "tradition_or_joke"),
    "technical_issue": ("operational", "event"),
    "moderation_context": ("real_community", "event"),
    "show_state_override": ("operational", "current_state"),
}
BROADCAST_USAGE_SCOPES = frozenset(
    {"direct", "ambient", "show_status", "relay", "internal"}
)

_SOURCE_SYSTEMS = frozenset({GENERAL_DECLARATION_SOURCE, BROADCAST_MEMORY_SOURCE})
_MUTATION_OPERATIONS = frozenset(
    {"add", "correct", "supersede", "retire", "status", "classify_broadcast"}
)
_PREVIEW_OPERATIONS = frozenset({"preview_declared", "preview_broadcast"})
_AUTHORITY_NONCE_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{7,127}$")
_STABLE_ID_RE = re.compile(r"^[a-z0-9][a-z0-9_.:-]{1,119}$")
_PREDICATE_RE = re.compile(r"^[a-z][a-z0-9_.:-]{1,119}$")
_ROUTE_RE = re.compile(r"^[a-z][a-z0-9_.:-]{1,79}$")

BROADCAST_PUBLIC_ROUTE_SCOPES = {
    "public_home": frozenset({"ambient", "direct"}),
    "public_context": frozenset({"ambient", "direct"}),
    "public_selective": frozenset({"ambient", "direct"}),
    "relay": frozenset({"relay"}),
    "journal": frozenset({"ambient"}),
    "website": frozenset({"relay"}),
}


class DeclaredCanonError(ValueError):
    """Fail-closed Declared Canon contract error with a stable reason code."""

    def __init__(self, code: str):
        self.code = str(code or "declared_canon_error")
        super().__init__(self.code)


@dataclass(frozen=True)
class _VerifiedAuthority:
    actor_user_id: int
    guild_id: int
    operation: str
    request_fingerprint: str
    operation_id: str
    authority_actor: str
    authority_receipt: str


@dataclass(frozen=True)
class DeclaredCanonRevision:
    revision_id: str
    declaration_id: str
    revision_number: int
    guild_id: int
    source_system: str
    source_row_id: str
    source_fingerprint: str
    operation: str
    operation_id: str
    operation_reason: str
    classification_mode: str
    raw_declaration: str
    cleaned_summary: str
    subject_type: str
    subject_id: str
    object_subject_type: str
    object_subject_id: str
    predicate: str
    value_json: str
    domain: str
    claim_kind: str
    visibility: str
    eligible_routes_json: str
    valid_from: str
    valid_until: str
    lifecycle_status: str
    previous_revision_id: str
    correction_of_revision_id: str
    supersedes_declaration_id: str
    superseded_by_declaration_id: str
    derived_from_source_ref: str
    authority_request_fingerprint: str
    authority_actor: str
    authority_receipt: str
    authority_verified: bool
    contract_version: str
    created_at: str

    @property
    def eligible_routes(self) -> tuple[str, ...]:
        try:
            values = json.loads(self.eligible_routes_json or "[]")
        except (TypeError, json.JSONDecodeError):
            return ()
        if not isinstance(values, list):
            return ()
        return tuple(str(value) for value in values if str(value or ""))


@dataclass(frozen=True)
class MutationResult:
    operation_id: str
    revisions: tuple[DeclaredCanonRevision, ...]

    @property
    def primary(self) -> DeclaredCanonRevision:
        return self.revisions[0]


@dataclass(frozen=True)
class DeclaredPreviewItem:
    declaration_id: str
    revision_id: str
    revision_number: int
    source_system: str
    domain: str
    claim_kind: str
    visibility: str
    lifecycle_status: str
    classification_mode: str
    current_revision: bool


@dataclass(frozen=True)
class DeclaredPreview:
    contract_version: str
    guild_id: int
    authority_actor_ref: str
    authority_receipt_id: str
    items: tuple[DeclaredPreviewItem, ...]
    total_rows: int
    truncated: bool
    mutation_count: int


@dataclass(frozen=True)
class BroadcastPreviewItem:
    preview_item: str
    source_era: str
    entry_type: str
    status: str
    usage_scope: str
    public_safe: bool
    submitter_state: str
    validity_state: str
    derivation_state: str
    subject_link_state: str
    source_fingerprint_state: str
    classification_state: str
    disposition: str
    reason_codes: tuple[str, ...]


@dataclass(frozen=True)
class BroadcastHistoryPreview:
    contract_version: str
    guild_id: int
    authority_actor_ref: str
    authority_receipt_id: str
    owner_era_cutoff: str
    items: tuple[BroadcastPreviewItem, ...]
    total_rows: int
    truncated: bool
    counts_scope: str
    type_counts: Mapping[str, int]
    era_counts: Mapping[str, int]
    disposition_counts: Mapping[str, int]
    mutation_count: int


_REVISION_COLUMNS = (
    "revision_id",
    "declaration_id",
    "revision_number",
    "guild_id",
    "source_system",
    "source_row_id",
    "source_fingerprint",
    "operation",
    "operation_id",
    "operation_reason",
    "classification_mode",
    "raw_declaration",
    "cleaned_summary",
    "subject_type",
    "subject_id",
    "object_subject_type",
    "object_subject_id",
    "predicate",
    "value_json",
    "domain",
    "claim_kind",
    "visibility",
    "eligible_routes_json",
    "valid_from",
    "valid_until",
    "lifecycle_status",
    "previous_revision_id",
    "correction_of_revision_id",
    "supersedes_declaration_id",
    "superseded_by_declaration_id",
    "derived_from_source_ref",
    "authority_request_fingerprint",
    "authority_actor",
    "authority_receipt",
    "authority_verified",
    "contract_version",
    "created_at",
)

_DECLARED_CANON_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS main.declared_canon_revisions (
    revision_id TEXT PRIMARY KEY,
    declaration_id TEXT NOT NULL,
    revision_number INTEGER NOT NULL CHECK(revision_number > 0),
    guild_id INTEGER NOT NULL CHECK(guild_id > 0),
    source_system TEXT NOT NULL,
    source_row_id TEXT NOT NULL,
    source_fingerprint TEXT NOT NULL,
    operation TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    operation_reason TEXT NOT NULL DEFAULT '',
    classification_mode TEXT NOT NULL,
    raw_declaration TEXT NOT NULL DEFAULT '',
    cleaned_summary TEXT NOT NULL DEFAULT '',
    subject_type TEXT NOT NULL,
    subject_id TEXT NOT NULL,
    object_subject_type TEXT NOT NULL DEFAULT '',
    object_subject_id TEXT NOT NULL DEFAULT '',
    predicate TEXT NOT NULL,
    value_json TEXT NOT NULL DEFAULT '',
    domain TEXT NOT NULL,
    claim_kind TEXT NOT NULL,
    visibility TEXT NOT NULL,
    eligible_routes_json TEXT NOT NULL DEFAULT '[]',
    valid_from TEXT NOT NULL DEFAULT '',
    valid_until TEXT NOT NULL DEFAULT '',
    lifecycle_status TEXT NOT NULL,
    previous_revision_id TEXT NOT NULL DEFAULT '',
    correction_of_revision_id TEXT NOT NULL DEFAULT '',
    supersedes_declaration_id TEXT NOT NULL DEFAULT '',
    superseded_by_declaration_id TEXT NOT NULL DEFAULT '',
    derived_from_source_ref TEXT NOT NULL DEFAULT '',
    authority_request_fingerprint TEXT NOT NULL,
    authority_actor TEXT NOT NULL,
    authority_receipt TEXT NOT NULL,
    authority_verified INTEGER NOT NULL CHECK(authority_verified IN (0,1)),
    contract_version TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(guild_id, declaration_id, revision_number),
    UNIQUE(guild_id, declaration_id, operation_id)
)
"""

_DECLARED_CANON_INDEX_DDL = {
    "idx_declared_canon_source": """
        CREATE INDEX IF NOT EXISTS main.idx_declared_canon_source
        ON declared_canon_revisions(
            guild_id, source_system, source_row_id, revision_number
        )
    """,
    "idx_declared_canon_subject": """
        CREATE INDEX IF NOT EXISTS main.idx_declared_canon_subject
        ON declared_canon_revisions(
            guild_id, subject_type, subject_id, lifecycle_status
        )
    """,
}

_DECLARED_CANON_TRIGGER_DDL = {
    "trg_declared_canon_revisions_no_conflicting_insert": """
        CREATE TRIGGER IF NOT EXISTS main.trg_declared_canon_revisions_no_conflicting_insert
        BEFORE INSERT ON declared_canon_revisions
        WHEN EXISTS(
            SELECT 1 FROM declared_canon_revisions existing
            WHERE existing.revision_id=NEW.revision_id
               OR (
                   existing.guild_id=NEW.guild_id
                   AND existing.declaration_id=NEW.declaration_id
                   AND existing.revision_number=NEW.revision_number
               )
               OR (
                   existing.guild_id=NEW.guild_id
                   AND existing.declaration_id=NEW.declaration_id
                   AND existing.operation_id=NEW.operation_id
               )
        )
        BEGIN
            SELECT RAISE(ABORT, 'declared_canon_append_only_conflict');
        END
    """,
    "trg_declared_canon_revisions_no_update": """
        CREATE TRIGGER IF NOT EXISTS main.trg_declared_canon_revisions_no_update
        BEFORE UPDATE ON declared_canon_revisions
        BEGIN
            SELECT RAISE(ABORT, 'declared_canon_append_only_update');
        END
    """,
    "trg_declared_canon_revisions_no_delete": """
        CREATE TRIGGER IF NOT EXISTS main.trg_declared_canon_revisions_no_delete
        BEFORE DELETE ON declared_canon_revisions
        BEGIN
            SELECT RAISE(ABORT, 'declared_canon_append_only_delete');
        END
    """,
}

_DECLARED_CANON_UNIQUE_COLUMN_SETS = frozenset(
    {
        ("revision_id",),
        ("guild_id", "declaration_id", "revision_number"),
        ("guild_id", "declaration_id", "operation_id"),
    }
)


def _utc_now() -> str:
    # Broadcast intake stores microseconds. Keep equal precision so an
    # immediately classified row cannot appear fractionally future-dated.
    return datetime.now(timezone.utc).isoformat()


def _digest(*values: Any) -> str:
    payload = json.dumps(
        values,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _authority_secret() -> bytes:
    """Load the dedicated authority key without accepting a caller override."""

    raw = os.getenv(DECLARED_CANON_AUTHORITY_SECRET_ENV, "")
    if not raw:
        raise DeclaredCanonError("declared_canon_authority_secret_not_configured")
    encoded = raw.encode("utf-8")
    if len(encoded) < DECLARED_CANON_AUTHORITY_SECRET_MIN_BYTES:
        raise DeclaredCanonError("declared_canon_authority_secret_invalid")
    return encoded


def _authority_hmac(*values: Any) -> str:
    payload = json.dumps(
        values,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hmac.new(_authority_secret(), payload, hashlib.sha256).hexdigest()


def _strict_bool(value: Any) -> bool:
    if value is True:
        return True
    if value is False or value is None:
        return False
    if isinstance(value, int) and not isinstance(value, bool):
        return value == 1
    return False


def _configured_owner_user_id() -> int:
    """Read trusted runtime configuration for every public operation."""

    try:
        return int(os.getenv("BNL_OWNER_USER_ID", "0") or 0)
    except (TypeError, ValueError):
        return 0


def _configured_primary_guild_id() -> int:
    try:
        return int(os.getenv("BNL_PRIMARY_GUILD_ID", "0") or 0)
    except (TypeError, ValueError):
        return 0


def _require_configured_owner_actor(*, actor_user_id: int, guild_id: int) -> int:
    configured = _configured_owner_user_id()
    actor = int(actor_user_id or 0)
    target_guild = int(guild_id or 0)
    if configured <= 0:
        raise DeclaredCanonError("owner_user_id_not_configured")
    if actor <= 0 or actor != configured:
        raise DeclaredCanonError("configured_owner_required")
    configured_guild = _configured_primary_guild_id()
    if configured_guild <= 0:
        raise DeclaredCanonError("primary_guild_id_not_configured")
    if target_guild <= 0 or target_guild != configured_guild:
        raise DeclaredCanonError("configured_primary_guild_required")
    return actor


def _authorize_request(
    *,
    actor_user_id: int,
    guild_id: int,
    operation: str,
    authority_nonce: str,
    binding: Mapping[str, Any],
) -> _VerifiedAuthority:
    """Issue an internal receipt bound to the exact normalized request.

    Callers cannot supply a configured-owner value or a prebuilt receipt.  The
    actor is compared with ``BNL_OWNER_USER_ID`` at this boundary on every call.
    No caller-supplied secret or receipt participates. The internally computed,
    keyed and visibly versioned receipt commits to the operation, guild, actor,
    nonce, target, expected revision/source fingerprint, and normalized payload.
    """

    normalized_operation = str(operation or "").strip().casefold()
    if normalized_operation not in _MUTATION_OPERATIONS | _PREVIEW_OPERATIONS:
        raise DeclaredCanonError("invalid_authority_operation")
    actor = _require_configured_owner_actor(
        actor_user_id=actor_user_id, guild_id=guild_id
    )
    nonce = str(authority_nonce or "").strip()
    if not _AUTHORITY_NONCE_RE.fullmatch(nonce):
        raise DeclaredCanonError("invalid_authority_nonce")
    request_fingerprint = "req_" + _digest(
        DECLARED_CANON_CONTRACT_VERSION,
        normalized_operation,
        int(guild_id),
        actor,
        binding,
    )[:48]
    operation_id = "op_" + _digest(
        DECLARED_CANON_CONTRACT_VERSION,
        int(guild_id),
        actor,
        nonce,
    )[:32]
    receipt_digest = _authority_hmac(
        INTERNAL_AUTHORITY_RECEIPT_VERSION,
        "request_authority",
        DECLARED_CANON_CONTRACT_VERSION,
        normalized_operation,
        int(guild_id),
        actor,
        nonce,
        operation_id,
        request_fingerprint,
    )
    return _VerifiedAuthority(
        actor_user_id=actor,
        guild_id=int(guild_id),
        operation=normalized_operation,
        request_fingerprint=request_fingerprint,
        operation_id=operation_id,
        authority_actor="discord_user:%s" % actor,
        authority_receipt=(
            "owner_command:%s:%s:%s:%s"
            % (
                DECLARED_CANON_CONTRACT_VERSION,
                INTERNAL_AUTHORITY_RECEIPT_VERSION,
                normalized_operation,
                receipt_digest,
            )
        ),
    )


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    if table_name not in {DECLARED_CANON_TABLE, BROADCAST_MEMORY_SOURCE}:
        raise DeclaredCanonError("invalid_main_table_identifier")
    return bool(
        conn.execute(
            "SELECT 1 FROM main.sqlite_master WHERE type='table' AND name=?",
            (str(table_name or ""),),
        ).fetchone()
    )


def _table_columns(conn: sqlite3.Connection, table_name: str) -> tuple[str, ...]:
    if not _table_exists(conn, table_name):
        return ()
    return tuple(
        str(row[1])
        for row in conn.execute("PRAGMA main.table_info(%s)" % table_name)
    )


def _normalized_schema_sql(value: Any) -> str:
    normalized = " ".join(str(value or "").strip().casefold().split())
    normalized = normalized.replace(" if not exists ", " ")
    normalized = normalized.replace("main.", "")
    return normalized.rstrip(";")


def _main_schema_sql(
    conn: sqlite3.Connection, *, object_type: str, name: str
) -> str:
    row = conn.execute(
        "SELECT sql FROM main.sqlite_master WHERE type=? AND name=?",
        (str(object_type), str(name)),
    ).fetchone()
    return str(row[0] or "") if row else ""


def _main_index_columns(
    conn: sqlite3.Connection, index_name: str
) -> tuple[str, ...]:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", str(index_name or "")):
        raise DeclaredCanonError("declared_canon_schema_integrity_invalid")
    return tuple(
        str(row[2])
        for row in conn.execute("PRAGMA main.index_info(%s)" % index_name)
    )


@contextmanager
def _immediate_transaction(conn: sqlite3.Connection):
    if conn.in_transaction:
        raise DeclaredCanonError("transaction_already_active")
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield
    except Exception:
        conn.rollback()
        raise
    else:
        conn.commit()


@contextmanager
def _read_snapshot(conn: sqlite3.Connection):
    """Hold one coherent read snapshot without taking caller transaction ownership."""

    owns_transaction = not conn.in_transaction
    before = int(conn.total_changes or 0)
    if owns_transaction:
        conn.execute("BEGIN")
        # BEGIN is deferred in SQLite. This read pins the snapshot before any
        # validator query so a WAL writer cannot splice a newer revision/source
        # row into the remainder of the validation sequence.
        conn.execute("SELECT 1 FROM main.sqlite_schema LIMIT 1").fetchone()
    try:
        yield
        if int(conn.total_changes or 0) != before:
            raise DeclaredCanonError("read_validator_mutated_state")
    except Exception:
        if owns_transaction:
            conn.rollback()
        raise
    else:
        if owns_transaction:
            conn.commit()


def ensure_declared_canon_schema(conn: sqlite3.Connection) -> None:
    """Create the exact schema once; never auto-heal an existing damaged one."""

    with _immediate_transaction(conn):
        if _table_exists(conn, DECLARED_CANON_TABLE):
            _require_schema(conn)
            return
        conn.execute(_DECLARED_CANON_TABLE_DDL)
        for statement in _DECLARED_CANON_INDEX_DDL.values():
            conn.execute(statement)
        for statement in _DECLARED_CANON_TRIGGER_DDL.values():
            conn.execute(statement)
        _require_schema(conn)


def _require_schema(conn: sqlite3.Connection) -> None:
    if not _table_exists(conn, DECLARED_CANON_TABLE):
        raise DeclaredCanonError("declared_canon_schema_unavailable")
    if _table_columns(conn, DECLARED_CANON_TABLE) != _REVISION_COLUMNS:
        raise DeclaredCanonError("declared_canon_schema_integrity_invalid")
    actual_table_sql = _main_schema_sql(
        conn, object_type="table", name=DECLARED_CANON_TABLE
    )
    if _normalized_schema_sql(actual_table_sql) != _normalized_schema_sql(
        _DECLARED_CANON_TABLE_DDL
    ):
        raise DeclaredCanonError("declared_canon_schema_integrity_invalid")

    index_rows = conn.execute(
        "PRAGMA main.index_list(%s)" % DECLARED_CANON_TABLE
    ).fetchall()
    explicit_indexes = {
        str(row[1]): (_strict_bool(row[2]), _main_index_columns(conn, str(row[1])))
        for row in index_rows
        if str(row[3] or "") == "c"
    }
    expected_explicit_indexes = {
        "idx_declared_canon_source": (
            False,
            ("guild_id", "source_system", "source_row_id", "revision_number"),
        ),
        "idx_declared_canon_subject": (
            False,
            ("guild_id", "subject_type", "subject_id", "lifecycle_status"),
        ),
    }
    if explicit_indexes != expected_explicit_indexes:
        raise DeclaredCanonError("declared_canon_schema_integrity_invalid")
    for name, expected_sql in _DECLARED_CANON_INDEX_DDL.items():
        if _normalized_schema_sql(
            _main_schema_sql(conn, object_type="index", name=name)
        ) != _normalized_schema_sql(expected_sql):
            raise DeclaredCanonError("declared_canon_schema_integrity_invalid")

    unique_column_sets = frozenset(
        _main_index_columns(conn, str(row[1]))
        for row in index_rows
        if _strict_bool(row[2])
    )
    if unique_column_sets != _DECLARED_CANON_UNIQUE_COLUMN_SETS:
        raise DeclaredCanonError("declared_canon_schema_integrity_invalid")

    trigger_rows = conn.execute(
        "SELECT name,sql FROM main.sqlite_master "
        "WHERE type='trigger' AND tbl_name=?",
        (DECLARED_CANON_TABLE,),
    ).fetchall()
    actual_triggers = {str(row[0]): str(row[1] or "") for row in trigger_rows}
    if set(actual_triggers) != set(_DECLARED_CANON_TRIGGER_DDL):
        raise DeclaredCanonError("declared_canon_schema_integrity_invalid")
    for name, expected_sql in _DECLARED_CANON_TRIGGER_DDL.items():
        if _normalized_schema_sql(actual_triggers.get(name)) != _normalized_schema_sql(
            expected_sql
        ):
            raise DeclaredCanonError("declared_canon_schema_integrity_invalid")


def _bounded_text(value: Any, *, field: str, maximum: int, required: bool = True) -> str:
    text = str(value or "").strip()
    if required and not text:
        raise DeclaredCanonError("missing_%s" % field)
    if len(text) > maximum:
        raise DeclaredCanonError("%s_too_long" % field)
    return text


def _validate_stable_token(value: Any, *, field: str, pattern: re.Pattern[str]) -> str:
    token = str(value or "").strip()
    if not pattern.fullmatch(token):
        raise DeclaredCanonError("invalid_%s" % field)
    return token


def _canonical_value_json(value: Any) -> str:
    try:
        encoded = json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError):
        raise DeclaredCanonError("invalid_typed_value")
    if len(encoded) > 4000:
        raise DeclaredCanonError("typed_value_too_long")
    return encoded


def _canonical_routes(routes: Iterable[str], visibility: str) -> str:
    normalized = tuple(
        sorted(
            {
                str(route or "").strip().casefold()
                for route in routes
                if str(route or "").strip()
            }
        )
    )
    if any(not _ROUTE_RE.fullmatch(route) for route in normalized):
        raise DeclaredCanonError("invalid_eligible_route")
    if any(route not in ALLOWED_ELIGIBLE_ROUTES for route in normalized):
        raise DeclaredCanonError("unknown_eligible_route")
    if visibility not in PUBLIC_VISIBILITIES and PUBLIC_ROUTES.intersection(normalized):
        raise DeclaredCanonError("internal_visibility_public_route")
    return json.dumps(normalized, separators=(",", ":"))


def _parse_validity(value: Any, *, field: str) -> tuple[str, datetime | None]:
    text = str(value or "").strip()
    if not text:
        return "", None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        raise DeclaredCanonError("invalid_%s" % field)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return text, parsed.astimezone(timezone.utc)


def _validated_claim_fields(
    *,
    subject_type: Any,
    subject_id: Any,
    object_subject_type: Any,
    object_subject_id: Any,
    predicate: Any,
    value: Any,
    raw_declaration: Any,
    cleaned_summary: Any,
    domain: Any,
    claim_kind: Any,
    visibility: Any,
    eligible_routes: Iterable[str],
    valid_from: Any,
    valid_until: Any,
) -> dict[str, str]:
    normalized_subject_type = str(subject_type or "").strip().casefold()
    if normalized_subject_type not in GENERAL_SUBJECT_TYPES:
        raise DeclaredCanonError("invalid_subject_type")
    normalized_subject_id = _validate_stable_token(
        subject_id, field="subject_id", pattern=_STABLE_ID_RE
    )
    normalized_predicate = _validate_stable_token(
        predicate, field="predicate", pattern=_PREDICATE_RE
    )
    normalized_domain = str(domain or "").strip().casefold()
    if normalized_domain not in CANON_DOMAINS:
        raise DeclaredCanonError("invalid_domain")
    normalized_claim_kind = str(claim_kind or "").strip().casefold()
    if normalized_claim_kind not in CLAIM_KINDS:
        raise DeclaredCanonError("invalid_claim_kind")
    normalized_object_type = str(object_subject_type or "").strip().casefold()
    normalized_object_id = str(object_subject_id or "").strip()
    if normalized_claim_kind == "relationship":
        if normalized_object_type not in GENERAL_SUBJECT_TYPES:
            raise DeclaredCanonError("relationship_object_subject_type_required")
        normalized_object_id = _validate_stable_token(
            normalized_object_id,
            field="object_subject_id",
            pattern=_STABLE_ID_RE,
        )
    elif normalized_object_type or normalized_object_id:
        raise DeclaredCanonError("relationship_object_not_allowed")
    if normalized_subject_type == "relationship" and normalized_claim_kind != "relationship":
        raise DeclaredCanonError("relationship_claim_kind_required")
    normalized_visibility = str(visibility or "").strip().casefold()
    if normalized_visibility not in VISIBILITIES:
        raise DeclaredCanonError("invalid_visibility")
    normalized_valid_from, parsed_from = _parse_validity(
        valid_from, field="valid_from"
    )
    normalized_valid_until, parsed_until = _parse_validity(
        valid_until, field="valid_until"
    )
    if parsed_from and parsed_until and parsed_until < parsed_from:
        raise DeclaredCanonError("invalid_validity_window")
    return {
        "subject_type": normalized_subject_type,
        "subject_id": normalized_subject_id,
        "object_subject_type": normalized_object_type,
        "object_subject_id": normalized_object_id,
        "predicate": normalized_predicate,
        "value_json": _canonical_value_json(value),
        "raw_declaration": _bounded_text(
            raw_declaration, field="raw_declaration", maximum=4000
        ),
        "cleaned_summary": _bounded_text(
            cleaned_summary,
            field="cleaned_summary",
            maximum=1000,
            required=False,
        ),
        "domain": normalized_domain,
        "claim_kind": normalized_claim_kind,
        "visibility": normalized_visibility,
        "eligible_routes_json": _canonical_routes(
            eligible_routes, normalized_visibility
        ),
        "valid_from": normalized_valid_from,
        "valid_until": normalized_valid_until,
    }


def _operation_id(authority: _VerifiedAuthority) -> str:
    return authority.operation_id


def _opaque_actor_ref(authority: _VerifiedAuthority) -> str:
    return "actor_" + _digest(
        DECLARED_CANON_CONTRACT_VERSION,
        authority.guild_id,
        authority.actor_user_id,
    )[:20]


def _receipt_id(authority: _VerifiedAuthority) -> str:
    return "receipt_" + _digest(authority.authority_receipt)[:24]


def _general_fingerprint(fields: Mapping[str, str]) -> str:
    return "src_" + _digest(
        DECLARED_CANON_CONTRACT_VERSION,
        GENERAL_DECLARATION_SOURCE,
        fields.get("raw_declaration", ""),
        fields.get("cleaned_summary", ""),
        fields.get("subject_type", ""),
        fields.get("subject_id", ""),
        fields.get("object_subject_type", ""),
        fields.get("object_subject_id", ""),
        fields.get("predicate", ""),
        fields.get("value_json", ""),
        fields.get("domain", ""),
        fields.get("claim_kind", ""),
        fields.get("visibility", ""),
        fields.get("eligible_routes_json", ""),
        fields.get("valid_from", ""),
        fields.get("valid_until", ""),
    )[:40]


def _stored_authority_receipt(payload: Mapping[str, Any]) -> str:
    operation = str(payload.get("operation") or "")
    digest = _authority_hmac(
        INTERNAL_AUTHORITY_RECEIPT_VERSION,
        STORED_AUTHORITY_BINDING_VERSION,
        *(payload.get(column) for column in _REVISION_COLUMNS
          if column not in {"revision_id", "authority_receipt"}),
    )
    return "owner_command:%s:%s:%s:%s" % (
        DECLARED_CANON_CONTRACT_VERSION,
        INTERNAL_AUTHORITY_RECEIPT_VERSION,
        operation,
        digest,
    )


def _revision_payload(
    *,
    declaration_id: str,
    revision_number: int,
    guild_id: int,
    source_system: str,
    source_row_id: str,
    source_fingerprint: str,
    operation: str,
    operation_id: str,
    operation_reason: str,
    classification_mode: str,
    raw_declaration: str,
    cleaned_summary: str,
    subject_type: str,
    subject_id: str,
    object_subject_type: str,
    object_subject_id: str,
    predicate: str,
    value_json: str,
    domain: str,
    claim_kind: str,
    visibility: str,
    eligible_routes_json: str,
    valid_from: str,
    valid_until: str,
    lifecycle_status: str,
    previous_revision_id: str,
    correction_of_revision_id: str,
    supersedes_declaration_id: str,
    superseded_by_declaration_id: str,
    derived_from_source_ref: str,
    authority_request_fingerprint: str,
    authority_actor: str,
    created_at: str,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "revision_id": "",
        "declaration_id": declaration_id,
        "revision_number": int(revision_number),
        "guild_id": int(guild_id),
        "source_system": source_system,
        "source_row_id": source_row_id,
        "source_fingerprint": source_fingerprint,
        "operation": operation,
        "operation_id": operation_id,
        "operation_reason": operation_reason,
        "classification_mode": classification_mode,
        "raw_declaration": raw_declaration,
        "cleaned_summary": cleaned_summary,
        "subject_type": subject_type,
        "subject_id": subject_id,
        "object_subject_type": object_subject_type,
        "object_subject_id": object_subject_id,
        "predicate": predicate,
        "value_json": value_json,
        "domain": domain,
        "claim_kind": claim_kind,
        "visibility": visibility,
        "eligible_routes_json": eligible_routes_json,
        "valid_from": valid_from,
        "valid_until": valid_until,
        "lifecycle_status": lifecycle_status,
        "previous_revision_id": previous_revision_id,
        "correction_of_revision_id": correction_of_revision_id,
        "supersedes_declaration_id": supersedes_declaration_id,
        "superseded_by_declaration_id": superseded_by_declaration_id,
        "derived_from_source_ref": derived_from_source_ref,
        "authority_request_fingerprint": authority_request_fingerprint,
        "authority_actor": authority_actor,
        "authority_receipt": "",
        "authority_verified": 1,
        "contract_version": DECLARED_CANON_CONTRACT_VERSION,
        "created_at": created_at,
    }
    if not re.fullmatch(
        r"req_[0-9a-f]{48}",
        str(payload["authority_request_fingerprint"] or ""),
    ):
        raise DeclaredCanonError("authority_request_fingerprint_invalid")
    payload["authority_receipt"] = _stored_authority_receipt(payload)
    payload["revision_id"] = "drev_" + _digest(
        *(payload[column] for column in _REVISION_COLUMNS if column != "revision_id")
    )[:40]
    return payload


def _copy_revision_payload(
    prior: DeclaredCanonRevision,
    *,
    operation: str,
    operation_id: str,
    operation_reason: str,
    authority: _VerifiedAuthority,
    created_at: str,
    lifecycle_status: str | None = None,
    supersedes_declaration_id: str | None = None,
    superseded_by_declaration_id: str | None = None,
) -> dict[str, Any]:
    return _revision_payload(
        declaration_id=prior.declaration_id,
        revision_number=prior.revision_number + 1,
        guild_id=prior.guild_id,
        source_system=prior.source_system,
        source_row_id=prior.source_row_id,
        source_fingerprint=prior.source_fingerprint,
        operation=operation,
        operation_id=operation_id,
        operation_reason=operation_reason,
        classification_mode=prior.classification_mode,
        raw_declaration=prior.raw_declaration,
        cleaned_summary=prior.cleaned_summary,
        subject_type=prior.subject_type,
        subject_id=prior.subject_id,
        object_subject_type=prior.object_subject_type,
        object_subject_id=prior.object_subject_id,
        predicate=prior.predicate,
        value_json=prior.value_json,
        domain=prior.domain,
        claim_kind=prior.claim_kind,
        visibility=prior.visibility,
        eligible_routes_json=prior.eligible_routes_json,
        valid_from=prior.valid_from,
        valid_until=prior.valid_until,
        lifecycle_status=(
            lifecycle_status
            if lifecycle_status is not None
            else prior.lifecycle_status
        ),
        previous_revision_id=prior.revision_id,
        correction_of_revision_id="",
        supersedes_declaration_id=(
            supersedes_declaration_id
            if supersedes_declaration_id is not None
            else prior.supersedes_declaration_id
        ),
        superseded_by_declaration_id=(
            superseded_by_declaration_id
            if superseded_by_declaration_id is not None
            else prior.superseded_by_declaration_id
        ),
        derived_from_source_ref=prior.derived_from_source_ref,
        authority_request_fingerprint=authority.request_fingerprint,
        authority_actor=authority.authority_actor,
        created_at=created_at,
    )


def _insert_revision(conn: sqlite3.Connection, payload: Mapping[str, Any]) -> None:
    conn.execute(
        "INSERT INTO main.declared_canon_revisions (%s) VALUES (%s)"
        % (",".join(_REVISION_COLUMNS), ",".join("?" for _ in _REVISION_COLUMNS)),
        tuple(payload[column] for column in _REVISION_COLUMNS),
    )


def _row_to_revision(row: Sequence[Any]) -> DeclaredCanonRevision:
    values = dict(zip(_REVISION_COLUMNS, row))
    values["authority_verified"] = _strict_bool(values["authority_verified"])
    return DeclaredCanonRevision(**values)


def _validated_revision_chain(
    conn: sqlite3.Connection,
    *,
    guild_id: int,
    declaration_id: str,
) -> tuple[DeclaredCanonRevision, ...]:
    """Load and authenticate every revision before trusting the current head."""

    rows = conn.execute(
        "SELECT %s FROM main.declared_canon_revisions "
        "WHERE guild_id=? AND declaration_id=? ORDER BY revision_number"
        % ",".join(_REVISION_COLUMNS),
        (int(guild_id), str(declaration_id or "")),
    ).fetchall()
    revisions = tuple(_row_to_revision(row) for row in rows)
    prior: DeclaredCanonRevision | None = None
    for expected_number, revision in enumerate(revisions, start=1):
        if (
            int(revision.guild_id) != int(guild_id)
            or revision.declaration_id != str(declaration_id or "")
            or int(revision.revision_number) != expected_number
            or (
                not prior
                and bool(str(revision.previous_revision_id or ""))
            )
            or (
                prior is not None
                and revision.previous_revision_id != prior.revision_id
            )
            or (
                prior is not None
                and (
                    revision.source_system != prior.source_system
                    or revision.source_row_id != prior.source_row_id
                )
            )
        ):
            raise DeclaredCanonError("declared_canon_revision_chain_invalid")
        _require_trusted_stored_revision(revision)
        prior = revision
    return revisions


def _latest_revision(
    conn: sqlite3.Connection,
    *,
    guild_id: int,
    declaration_id: str,
) -> DeclaredCanonRevision | None:
    chain = _validated_revision_chain(
        conn, guild_id=int(guild_id), declaration_id=str(declaration_id or "")
    )
    return chain[-1] if chain else None


def _existing_operation(
    conn: sqlite3.Connection,
    *,
    authority: _VerifiedAuthority,
) -> tuple[DeclaredCanonRevision, ...] | None:
    rows = conn.execute(
        "SELECT %s FROM main.declared_canon_revisions "
        "WHERE guild_id=? AND operation_id=? ORDER BY rowid"
        % ",".join(_REVISION_COLUMNS),
        (authority.guild_id, authority.operation_id),
    ).fetchall()
    if not rows:
        return None
    revisions = tuple(_row_to_revision(row) for row in rows)
    if any(
        revision.operation != authority.operation
        or revision.authority_actor != authority.authority_actor
        or revision.authority_request_fingerprint
        != authority.request_fingerprint
        for revision in revisions
    ):
        raise DeclaredCanonError("authority_nonce_replay_mismatch")
    for declaration_id in {revision.declaration_id for revision in revisions}:
        _validated_revision_chain(
            conn, guild_id=authority.guild_id, declaration_id=declaration_id
        )
    return revisions


def _requested_and_created_at(now: str) -> tuple[str, str]:
    requested = ""
    if str(now or "").strip():
        requested = _parse_validity(now, field="created_at")[0]
    return requested, requested or _utc_now()


def _require_general_latest(
    conn: sqlite3.Connection,
    *,
    guild_id: int,
    declaration_id: str,
    allow_terminal: bool = False,
) -> DeclaredCanonRevision:
    normalized_id = _validate_stable_token(
        declaration_id, field="declaration_id", pattern=_STABLE_ID_RE
    )
    latest = _latest_revision(
        conn, guild_id=int(guild_id), declaration_id=normalized_id
    )
    if latest is None or latest.source_system != GENERAL_DECLARATION_SOURCE:
        raise DeclaredCanonError("general_declaration_not_found")
    _require_trusted_stored_revision(latest)
    if not allow_terminal and latest.lifecycle_status in TERMINAL_LIFECYCLES:
        raise DeclaredCanonError("declaration_terminal")
    return latest


def add_declared_canon(
    conn: sqlite3.Connection,
    *,
    actor_user_id: int,
    authority_nonce: str,
    guild_id: int,
    subject_type: str,
    subject_id: str,
    object_subject_type: str = "",
    object_subject_id: str = "",
    predicate: str,
    value: Any,
    raw_declaration: str,
    cleaned_summary: str = "",
    domain: str,
    claim_kind: str,
    visibility: str = "internal",
    eligible_routes: Iterable[str] = (),
    valid_from: str = "",
    valid_until: str = "",
    now: str = "",
) -> MutationResult:
    _require_configured_owner_actor(
        actor_user_id=actor_user_id, guild_id=guild_id
    )
    fields = _validated_claim_fields(
        subject_type=subject_type,
        subject_id=subject_id,
        object_subject_type=object_subject_type,
        object_subject_id=object_subject_id,
        predicate=predicate,
        value=value,
        raw_declaration=raw_declaration,
        cleaned_summary=cleaned_summary,
        domain=domain,
        claim_kind=claim_kind,
        visibility=visibility,
        eligible_routes=eligible_routes,
        valid_from=valid_from,
        valid_until=valid_until,
    )
    requested_now, created_at = _requested_and_created_at(now)
    authority = _authorize_request(
        actor_user_id=actor_user_id,
        guild_id=guild_id,
        operation="add",
        authority_nonce=authority_nonce,
        binding={"new_declaration": fields, "requested_now": requested_now},
    )
    operation_id = _operation_id(authority)
    declaration_id = "dcl_" + _digest(
        DECLARED_CANON_CONTRACT_VERSION,
        int(guild_id),
        authority.request_fingerprint,
        authority.authority_receipt,
    )[:32]
    payload = _revision_payload(
        declaration_id=declaration_id,
        revision_number=1,
        guild_id=int(guild_id),
        source_system=GENERAL_DECLARATION_SOURCE,
        source_row_id=declaration_id,
        source_fingerprint=_general_fingerprint(fields),
        operation="add",
        operation_id=operation_id,
        operation_reason="",
        classification_mode="owner_explicit",
        lifecycle_status="established",
        previous_revision_id="",
        correction_of_revision_id="",
        supersedes_declaration_id="",
        superseded_by_declaration_id="",
        derived_from_source_ref="",
        authority_request_fingerprint=authority.request_fingerprint,
        authority_actor=authority.authority_actor,
        created_at=created_at,
        **fields,
    )
    with _immediate_transaction(conn):
        _require_schema(conn)
        existing = _existing_operation(conn, authority=authority)
        if existing is not None:
            return MutationResult(operation_id, existing)
        _insert_revision(conn, payload)
    return MutationResult(operation_id, (_row_to_revision(tuple(payload[c] for c in _REVISION_COLUMNS)),))


def correct_declared_canon(
    conn: sqlite3.Connection,
    *,
    actor_user_id: int,
    authority_nonce: str,
    guild_id: int,
    declaration_id: str,
    expected_revision_id: str,
    subject_type: str,
    subject_id: str,
    object_subject_type: str = "",
    object_subject_id: str = "",
    predicate: str,
    value: Any,
    raw_declaration: str,
    cleaned_summary: str = "",
    domain: str,
    claim_kind: str,
    visibility: str = "internal",
    eligible_routes: Iterable[str] = (),
    valid_from: str = "",
    valid_until: str = "",
    reason: str = "",
    now: str = "",
) -> MutationResult:
    _require_configured_owner_actor(
        actor_user_id=actor_user_id, guild_id=guild_id
    )
    fields = _validated_claim_fields(
        subject_type=subject_type,
        subject_id=subject_id,
        object_subject_type=object_subject_type,
        object_subject_id=object_subject_id,
        predicate=predicate,
        value=value,
        raw_declaration=raw_declaration,
        cleaned_summary=cleaned_summary,
        domain=domain,
        claim_kind=claim_kind,
        visibility=visibility,
        eligible_routes=eligible_routes,
        valid_from=valid_from,
        valid_until=valid_until,
    )
    operation_reason = _bounded_text(
        reason, field="operation_reason", maximum=500, required=False
    )
    expected_revision = _validate_stable_token(
        expected_revision_id,
        field="expected_revision_id",
        pattern=_STABLE_ID_RE,
    )
    requested_now, created_at = _requested_and_created_at(now)
    authority = _authorize_request(
        actor_user_id=actor_user_id,
        guild_id=guild_id,
        operation="correct",
        authority_nonce=authority_nonce,
        binding={
            "declaration_id": str(declaration_id or ""),
            "expected_revision_id": expected_revision,
            "replacement": fields,
            "reason": operation_reason,
            "requested_now": requested_now,
        },
    )
    operation_id = _operation_id(authority)
    with _immediate_transaction(conn):
        _require_schema(conn)
        existing = _existing_operation(conn, authority=authority)
        if existing is not None:
            return MutationResult(operation_id, existing)
        prior = _require_general_latest(
            conn, guild_id=guild_id, declaration_id=declaration_id
        )
        if prior.revision_id != expected_revision:
            raise DeclaredCanonError("expected_revision_mismatch")
        payload = _revision_payload(
            declaration_id=prior.declaration_id,
            revision_number=prior.revision_number + 1,
            guild_id=int(guild_id),
            source_system=GENERAL_DECLARATION_SOURCE,
            source_row_id=prior.source_row_id,
            source_fingerprint=_general_fingerprint(fields),
            operation="correct",
            operation_id=operation_id,
            operation_reason=operation_reason,
            classification_mode="owner_explicit",
            lifecycle_status=prior.lifecycle_status,
            previous_revision_id=prior.revision_id,
            correction_of_revision_id=prior.revision_id,
            supersedes_declaration_id=prior.supersedes_declaration_id,
            superseded_by_declaration_id=prior.superseded_by_declaration_id,
            derived_from_source_ref=prior.derived_from_source_ref,
            authority_request_fingerprint=authority.request_fingerprint,
            authority_actor=authority.authority_actor,
            created_at=created_at,
            **fields,
        )
        _insert_revision(conn, payload)
    return MutationResult(operation_id, (_row_to_revision(tuple(payload[c] for c in _REVISION_COLUMNS)),))


def retire_declared_canon(
    conn: sqlite3.Connection,
    *,
    actor_user_id: int,
    authority_nonce: str,
    guild_id: int,
    declaration_id: str,
    expected_revision_id: str,
    reason: str = "",
    now: str = "",
) -> MutationResult:
    _require_configured_owner_actor(
        actor_user_id=actor_user_id, guild_id=guild_id
    )
    expected_revision = _validate_stable_token(
        expected_revision_id,
        field="expected_revision_id",
        pattern=_STABLE_ID_RE,
    )
    requested_now, created_at = _requested_and_created_at(now)
    operation_reason = _bounded_text(
        reason, field="operation_reason", maximum=500, required=False
    )
    authority = _authorize_request(
        actor_user_id=actor_user_id,
        guild_id=guild_id,
        operation="retire",
        authority_nonce=authority_nonce,
        binding={
            "declaration_id": str(declaration_id or ""),
            "expected_revision_id": expected_revision,
            "reason": operation_reason,
            "requested_now": requested_now,
        },
    )
    operation_id = _operation_id(authority)
    with _immediate_transaction(conn):
        _require_schema(conn)
        existing = _existing_operation(conn, authority=authority)
        if existing is not None:
            return MutationResult(operation_id, existing)
        prior = _require_general_latest(
            conn, guild_id=guild_id, declaration_id=declaration_id
        )
        if prior.revision_id != expected_revision:
            raise DeclaredCanonError("expected_revision_mismatch")
        payload = _copy_revision_payload(
            prior,
            operation="retire",
            operation_id=operation_id,
            operation_reason=operation_reason,
            authority=authority,
            created_at=created_at,
            lifecycle_status="retired",
        )
        _insert_revision(conn, payload)
    return MutationResult(operation_id, (_row_to_revision(tuple(payload[c] for c in _REVISION_COLUMNS)),))


def change_declared_canon_status(
    conn: sqlite3.Connection,
    *,
    actor_user_id: int,
    authority_nonce: str,
    guild_id: int,
    declaration_id: str,
    expected_revision_id: str,
    lifecycle_status: str,
    reason: str = "",
    now: str = "",
) -> MutationResult:
    _require_configured_owner_actor(
        actor_user_id=actor_user_id, guild_id=guild_id
    )
    normalized_status = str(lifecycle_status or "").strip().casefold()
    if normalized_status not in {"established", "contested", "resolved"}:
        raise DeclaredCanonError("invalid_status_change")
    expected_revision = _validate_stable_token(
        expected_revision_id,
        field="expected_revision_id",
        pattern=_STABLE_ID_RE,
    )
    requested_now, created_at = _requested_and_created_at(now)
    operation_reason = _bounded_text(
        reason, field="operation_reason", maximum=500, required=False
    )
    authority = _authorize_request(
        actor_user_id=actor_user_id,
        guild_id=guild_id,
        operation="status",
        authority_nonce=authority_nonce,
        binding={
            "declaration_id": str(declaration_id or ""),
            "expected_revision_id": expected_revision,
            "lifecycle_status": normalized_status,
            "reason": operation_reason,
            "requested_now": requested_now,
        },
    )
    operation_id = _operation_id(authority)
    with _immediate_transaction(conn):
        _require_schema(conn)
        existing = _existing_operation(conn, authority=authority)
        if existing is not None:
            return MutationResult(operation_id, existing)
        prior = _require_general_latest(
            conn, guild_id=guild_id, declaration_id=declaration_id
        )
        if prior.revision_id != expected_revision:
            raise DeclaredCanonError("expected_revision_mismatch")
        if prior.lifecycle_status == normalized_status:
            raise DeclaredCanonError("status_unchanged")
        payload = _copy_revision_payload(
            prior,
            operation="status",
            operation_id=operation_id,
            operation_reason=operation_reason,
            authority=authority,
            created_at=created_at,
            lifecycle_status=normalized_status,
        )
        _insert_revision(conn, payload)
    return MutationResult(operation_id, (_row_to_revision(tuple(payload[c] for c in _REVISION_COLUMNS)),))


def supersede_declared_canon(
    conn: sqlite3.Connection,
    *,
    actor_user_id: int,
    authority_nonce: str,
    guild_id: int,
    declaration_id: str,
    expected_revision_id: str,
    replacement_declaration_id: str,
    expected_replacement_revision_id: str,
    reason: str = "",
    now: str = "",
) -> MutationResult:
    """Atomically mark one declaration superseded and link its replacement."""

    _require_configured_owner_actor(
        actor_user_id=actor_user_id, guild_id=guild_id
    )
    normalized_declaration_id = _validate_stable_token(
        declaration_id, field="declaration_id", pattern=_STABLE_ID_RE
    )
    normalized_replacement_id = _validate_stable_token(
        replacement_declaration_id,
        field="replacement_declaration_id",
        pattern=_STABLE_ID_RE,
    )
    if normalized_declaration_id == normalized_replacement_id:
        raise DeclaredCanonError("self_supersession")
    expected_revision = _validate_stable_token(
        expected_revision_id,
        field="expected_revision_id",
        pattern=_STABLE_ID_RE,
    )
    expected_replacement_revision = _validate_stable_token(
        expected_replacement_revision_id,
        field="expected_replacement_revision_id",
        pattern=_STABLE_ID_RE,
    )
    requested_now, created_at = _requested_and_created_at(now)
    operation_reason = _bounded_text(
        reason, field="operation_reason", maximum=500, required=False
    )
    authority = _authorize_request(
        actor_user_id=actor_user_id,
        guild_id=guild_id,
        operation="supersede",
        authority_nonce=authority_nonce,
        binding={
            "declaration_id": normalized_declaration_id,
            "expected_revision_id": expected_revision,
            "replacement_declaration_id": normalized_replacement_id,
            "expected_replacement_revision_id": expected_replacement_revision,
            "reason": operation_reason,
            "requested_now": requested_now,
        },
    )
    operation_id = _operation_id(authority)
    with _immediate_transaction(conn):
        _require_schema(conn)
        existing = _existing_operation(conn, authority=authority)
        if existing is not None:
            by_declaration = {item.declaration_id: item for item in existing}
            return MutationResult(
                operation_id,
                (
                    by_declaration[normalized_declaration_id],
                    by_declaration[normalized_replacement_id],
                ),
            )
        prior = _require_general_latest(
            conn, guild_id=guild_id, declaration_id=normalized_declaration_id
        )
        replacement = _require_general_latest(
            conn, guild_id=guild_id, declaration_id=normalized_replacement_id
        )
        if prior.revision_id != expected_revision:
            raise DeclaredCanonError("expected_revision_mismatch")
        if replacement.revision_id != expected_replacement_revision:
            raise DeclaredCanonError("expected_replacement_revision_mismatch")
        replacement_payload = _copy_revision_payload(
            replacement,
            operation="supersede",
            operation_id=operation_id,
            operation_reason=operation_reason,
            authority=authority,
            created_at=created_at,
            supersedes_declaration_id=prior.declaration_id,
        )
        prior_payload = _copy_revision_payload(
            prior,
            operation="supersede",
            operation_id=operation_id,
            operation_reason=operation_reason,
            authority=authority,
            created_at=created_at,
            lifecycle_status="superseded",
            superseded_by_declaration_id=replacement.declaration_id,
        )
        _insert_revision(conn, replacement_payload)
        _insert_revision(conn, prior_payload)
    return MutationResult(
        operation_id,
        (
            _row_to_revision(tuple(prior_payload[c] for c in _REVISION_COLUMNS)),
            _row_to_revision(tuple(replacement_payload[c] for c in _REVISION_COLUMNS)),
        ),
    )


def _broadcast_row(
    conn: sqlite3.Connection,
    *,
    guild_id: int,
    row_id: int,
) -> dict[str, Any] | None:
    columns = _table_columns(conn, BROADCAST_MEMORY_SOURCE)
    required = set(_BROADCAST_SOURCE_REQUIRED_FIELDS)
    if not required.issubset(set(columns)):
        return None
    cursor = conn.execute(
        "SELECT * FROM main.broadcast_memory WHERE guild_id=? AND id=? LIMIT 1",
        (int(guild_id), int(row_id)),
    )
    row = cursor.fetchone()
    returned_columns = tuple(
        str(item[0] or "") for item in (cursor.description or ())
    )
    return dict(zip(returned_columns, row)) if row else None


def _canonical_broadcast_source_value(value: Any) -> tuple[str, Any]:
    """Give each SQLite scalar a deterministic, type-explicit encoding."""

    if value is None:
        return ("null", None)
    if isinstance(value, bool):
        return ("bool", bool(value))
    if isinstance(value, int):
        return ("int", str(value))
    if isinstance(value, float):
        return ("float", value.hex())
    if isinstance(value, str):
        return ("text", value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return ("blob", bytes(value).hex())
    raise DeclaredCanonError("broadcast_source_value_unversioned")


def broadcast_source_fingerprint(row: Mapping[str, Any]) -> str:
    """Bind classification to the complete versioned Broadcast source row.

    The minimum known schema must be present, but it is not an allowlist. Every
    returned column is sorted by name and included with a type-explicit scalar
    encoding. Unknown/future columns therefore stale an existing approval even
    when their current value is NULL or appears operationally unrelated.
    The sidecar stores only the hash; Broadcast remains the content authority.
    """

    required = _BROADCAST_SOURCE_REQUIRED_FIELDS
    if any(field not in row for field in required):
        raise DeclaredCanonError("broadcast_source_unversioned")
    normalized: list[tuple[str, tuple[str, Any]]] = []
    seen: set[str] = set()
    for raw_name in row:
        if not isinstance(raw_name, str) or not raw_name:
            raise DeclaredCanonError("broadcast_source_column_unversioned")
        name = str(raw_name)
        if name in seen:
            raise DeclaredCanonError("broadcast_source_column_ambiguous")
        seen.add(name)
        normalized.append((name, _canonical_broadcast_source_value(row[raw_name])))
    canonical_row = tuple(sorted(normalized, key=lambda item: item[0]))
    return "bsrc_" + _digest(
        DECLARED_CANON_CONTRACT_VERSION,
        BROADCAST_SOURCE_FINGERPRINT_VERSION,
        BROADCAST_MEMORY_SOURCE,
        canonical_row,
    )[:40]


def _scope_tokens(value: Any) -> frozenset[str]:
    return frozenset(
        token.strip().casefold().replace("-", "_")
        for token in re.split(r"[,/;\s]+", str(value or ""))
        if token.strip()
    )


def _validate_broadcast_public_routes(
    *, routes_json: str, source_scopes: frozenset[str]
) -> None:
    if source_scopes.difference(BROADCAST_USAGE_SCOPES):
        raise DeclaredCanonError("broadcast_usage_scope_unrecognized")
    try:
        decoded = json.loads(routes_json or "[]")
    except (TypeError, json.JSONDecodeError):
        raise DeclaredCanonError("invalid_eligible_routes_json")
    if not isinstance(decoded, list):
        raise DeclaredCanonError("invalid_eligible_routes_json")
    routes = tuple(str(route) for route in decoded)
    if any(route not in ALLOWED_ELIGIBLE_ROUTES for route in routes):
        raise DeclaredCanonError("unknown_eligible_route")
    for route in routes:
        required_scopes = BROADCAST_PUBLIC_ROUTE_SCOPES.get(str(route))
        if required_scopes is not None and not required_scopes.intersection(
            source_scopes
        ):
            raise DeclaredCanonError("broadcast_route_scope_widening")


def _broadcast_validity_state(row: Mapping[str, Any], now: datetime) -> str:
    return _validity_window_state(
        row.get("created_at"), row.get("valid_until"), now
    )


def _broadcast_lifecycle(row: Mapping[str, Any], now: datetime) -> str:
    status = str(row.get("status") or "").strip().casefold()
    if status not in {"active", "resolved", "superseded"}:
        raise DeclaredCanonError("broadcast_source_status_unrecognized")
    if row.get("superseded_by_id") and status != "superseded":
        raise DeclaredCanonError("broadcast_source_supersession_inconsistent")
    if status == "superseded":
        return "superseded"
    if status == "resolved":
        return "resolved"
    validity = _broadcast_validity_state(row, now)
    if validity == "invalid":
        raise DeclaredCanonError("broadcast_source_validity_invalid")
    return "established"


def classify_broadcast_memory(
    conn: sqlite3.Connection,
    *,
    actor_user_id: int,
    authority_nonce: str,
    guild_id: int,
    broadcast_row_id: int,
    expected_source_fingerprint: str,
    expected_revision_id: str = "",
    subject_type: str,
    subject_id: str,
    object_subject_type: str = "",
    object_subject_id: str = "",
    predicate: str = "",
    domain: str = "",
    claim_kind: str = "",
    visibility: str = "internal",
    eligible_routes: Iterable[str] = (),
    reason: str = "",
    now: str = "",
) -> MutationResult:
    """Append typed owner metadata for one Broadcast row without copying it.

    The six current parser types may use versioned defaults.  An exact legacy
    type remains zero-write review-only unless the owner supplies an explicit
    domain and claim kind; it is never coerced into a current type.
    """

    _require_configured_owner_actor(
        actor_user_id=actor_user_id, guild_id=guild_id
    )
    normalized_subject_type = str(subject_type or "").strip().casefold()
    if normalized_subject_type not in GENERAL_SUBJECT_TYPES:
        raise DeclaredCanonError("invalid_subject_type")
    normalized_subject_id = _validate_stable_token(
        subject_id, field="subject_id", pattern=_STABLE_ID_RE
    )
    expected_fingerprint = str(expected_source_fingerprint or "").strip()
    if not re.fullmatch(r"bsrc_[0-9a-f]{40}", expected_fingerprint):
        raise DeclaredCanonError("expected_source_fingerprint_required")
    expected_revision = str(expected_revision_id or "").strip()
    if expected_revision:
        expected_revision = _validate_stable_token(
            expected_revision,
            field="expected_revision_id",
            pattern=_STABLE_ID_RE,
        )
    normalized_visibility = str(visibility or "").strip().casefold()
    if normalized_visibility not in VISIBILITIES:
        raise DeclaredCanonError("invalid_visibility")
    routes_json = _canonical_routes(eligible_routes, normalized_visibility)
    requested_domain = str(domain or "").strip().casefold()
    if requested_domain and requested_domain not in CANON_DOMAINS:
        raise DeclaredCanonError("invalid_domain")
    requested_claim_kind = str(claim_kind or "").strip().casefold()
    if requested_claim_kind and requested_claim_kind not in CLAIM_KINDS:
        raise DeclaredCanonError("invalid_claim_kind")
    operation_reason = _bounded_text(
        reason, field="operation_reason", maximum=500, required=False
    )
    requested_now, created_at = _requested_and_created_at(now)
    parsed_now = _parse_validity(created_at, field="created_at")[1]
    assert parsed_now is not None
    request_binding = {
        "broadcast_row_id": int(broadcast_row_id or 0),
        "expected_source_fingerprint": expected_fingerprint,
        "expected_revision_id": expected_revision,
        "subject_type": normalized_subject_type,
        "subject_id": normalized_subject_id,
        "object_subject_type": str(object_subject_type or "").strip().casefold(),
        "object_subject_id": str(object_subject_id or "").strip(),
        "predicate": str(predicate or "").strip().casefold(),
        "requested_domain": requested_domain,
        "requested_claim_kind": requested_claim_kind,
        "visibility": normalized_visibility,
        "eligible_routes_json": routes_json,
        "reason": operation_reason,
        "requested_now": requested_now,
    }
    authority = _authorize_request(
        actor_user_id=actor_user_id,
        guild_id=guild_id,
        operation="classify_broadcast",
        authority_nonce=authority_nonce,
        binding=request_binding,
    )
    operation_id = _operation_id(authority)
    with _immediate_transaction(conn):
        _require_schema(conn)
        existing = _existing_operation(conn, authority=authority)
        if existing is not None:
            return MutationResult(operation_id, existing)
        source = _broadcast_row(
            conn, guild_id=int(guild_id), row_id=int(broadcast_row_id or 0)
        )
        if source is None:
            raise DeclaredCanonError("broadcast_source_not_found")
        source_fingerprint = broadcast_source_fingerprint(source)
        if source_fingerprint != expected_fingerprint:
            raise DeclaredCanonError("expected_source_fingerprint_mismatch")
        entry_type = str(source.get("entry_type") or "").strip()
        defaults = BROADCAST_TYPE_DEFAULTS.get(entry_type)
        if defaults is None:
            if not requested_domain or not requested_claim_kind:
                raise DeclaredCanonError("legacy_type_review_only")
            default_domain, default_claim_kind = (
                requested_domain,
                requested_claim_kind,
            )
        else:
            default_domain, default_claim_kind = defaults
        normalized_domain = requested_domain or default_domain
        normalized_claim_kind = requested_claim_kind or default_claim_kind
        normalized_predicate = str(predicate or entry_type).strip().casefold()
        normalized_predicate = _validate_stable_token(
            normalized_predicate, field="predicate", pattern=_PREDICATE_RE
        )
        normalized_object_type = str(object_subject_type or "").strip().casefold()
        normalized_object_id = str(object_subject_id or "").strip()
        if normalized_claim_kind == "relationship":
            if not predicate:
                raise DeclaredCanonError("relationship_predicate_required")
            if normalized_object_type not in GENERAL_SUBJECT_TYPES:
                raise DeclaredCanonError(
                    "relationship_object_subject_type_required"
                )
            normalized_object_id = _validate_stable_token(
                normalized_object_id,
                field="object_subject_id",
                pattern=_STABLE_ID_RE,
            )
        elif normalized_object_type or normalized_object_id:
            raise DeclaredCanonError("relationship_object_not_allowed")
        if normalized_subject_type == "relationship" and normalized_claim_kind != "relationship":
            raise DeclaredCanonError("relationship_claim_kind_required")
        scopes = _scope_tokens(source.get("usage_scope"))
        lifecycle = _broadcast_lifecycle(source, parsed_now)
        if entry_type == "moderation_context" and normalized_visibility in PUBLIC_VISIBILITIES:
            raise DeclaredCanonError("moderation_context_internal_only")
        if normalized_visibility in PUBLIC_VISIBILITIES:
            if lifecycle != "established" or _broadcast_validity_state(
                source, parsed_now
            ) not in {"unbounded", "current"}:
                raise DeclaredCanonError("broadcast_source_not_current_for_public")
            if not _strict_bool(source.get("public_safe")):
                raise DeclaredCanonError("broadcast_source_not_public_safe")
            if "internal" in scopes:
                raise DeclaredCanonError("broadcast_internal_scope_veto")
            _validate_broadcast_public_routes(
                routes_json=routes_json, source_scopes=scopes
            )
        declaration_id = "dcl_" + _digest(
            DECLARED_CANON_CONTRACT_VERSION,
            int(guild_id),
            BROADCAST_MEMORY_SOURCE,
            int(broadcast_row_id),
        )[:32]
        prior = _latest_revision(
            conn, guild_id=int(guild_id), declaration_id=declaration_id
        )
        if prior and (
            prior.source_system != BROADCAST_MEMORY_SOURCE
            or prior.source_row_id != str(int(broadcast_row_id))
        ):
            raise DeclaredCanonError("broadcast_declaration_identity_collision")
        if prior is not None:
            _require_trusted_stored_revision(prior)
        if prior is None and expected_revision:
            raise DeclaredCanonError("expected_revision_mismatch")
        if prior is not None and prior.revision_id != expected_revision:
            raise DeclaredCanonError("expected_revision_mismatch")
        valid_from = str(source.get("created_at") or "")
        valid_until = str(source.get("valid_until") or "")
        if valid_from:
            _parse_validity(valid_from, field="valid_from")
        if valid_until:
            _parse_validity(valid_until, field="valid_until")
        payload = _revision_payload(
            declaration_id=declaration_id,
            revision_number=(prior.revision_number + 1 if prior else 1),
            guild_id=int(guild_id),
            source_system=BROADCAST_MEMORY_SOURCE,
            source_row_id=str(int(broadcast_row_id)),
            source_fingerprint=source_fingerprint,
            operation="classify_broadcast",
            operation_id=operation_id,
            operation_reason=operation_reason,
            classification_mode=(
                "owner_explicit_legacy_mapping"
                if defaults is None
                else "owner_explicit_mapping_override"
                if requested_domain or requested_claim_kind or predicate
                else "owner_explicit_default_mapping"
            ),
            raw_declaration="",
            cleaned_summary="",
            subject_type=normalized_subject_type,
            subject_id=normalized_subject_id,
            object_subject_type=normalized_object_type,
            object_subject_id=normalized_object_id,
            predicate=normalized_predicate,
            value_json="",
            domain=normalized_domain,
            claim_kind=normalized_claim_kind,
            visibility=normalized_visibility,
            eligible_routes_json=routes_json,
            valid_from=valid_from,
            valid_until=valid_until,
            lifecycle_status=lifecycle,
            previous_revision_id=(prior.revision_id if prior else ""),
            correction_of_revision_id=(prior.revision_id if prior else ""),
            supersedes_declaration_id="",
            superseded_by_declaration_id="",
            derived_from_source_ref="",
            authority_request_fingerprint=authority.request_fingerprint,
            authority_actor=authority.authority_actor,
            created_at=created_at,
        )
        _insert_revision(conn, payload)
    return MutationResult(operation_id, (_row_to_revision(tuple(payload[c] for c in _REVISION_COLUMNS)),))


def _stored_authority_valid(revision: DeclaredCanonRevision) -> bool:
    try:
        payload = {
            column: getattr(revision, column)
            for column in _REVISION_COLUMNS
        }
        # SQLite stores the verification bit as integer 0/1; normalize the
        # dataclass bool back to that canonical storage representation before
        # recomputing either immutable digest.
        payload["authority_verified"] = (
            1 if _strict_bool(revision.authority_verified) else 0
        )
        operation = str(revision.operation or "")
        prefix = "owner_command:%s:%s:%s:" % (
            DECLARED_CANON_CONTRACT_VERSION,
            INTERNAL_AUTHORITY_RECEIPT_VERSION,
            operation,
        )
        configured_owner = _configured_owner_user_id()
        configured_guild = _configured_primary_guild_id()
        if not (
            configured_owner > 0
            and configured_guild > 0
            and _strict_bool(revision.authority_verified)
            and str(revision.contract_version or "")
            == DECLARED_CANON_CONTRACT_VERSION
            and int(revision.guild_id) == configured_guild
            and operation in _MUTATION_OPERATIONS
            and str(revision.operation_id or "").startswith("op_")
            and re.fullmatch(r"op_[0-9a-f]{32}", revision.operation_id or "")
            and re.fullmatch(
                r"req_[0-9a-f]{48}",
                revision.authority_request_fingerprint or "",
            )
            and str(revision.authority_actor or "")
            == "discord_user:%s" % configured_owner
            and str(revision.authority_receipt or "").startswith(prefix)
        ):
            return False
        expected_receipt = _stored_authority_receipt(payload)
        if not hmac.compare_digest(
            str(revision.authority_receipt or ""), expected_receipt
        ):
            return False
        expected_revision_id = "drev_" + _digest(
            *(payload[column] for column in _REVISION_COLUMNS
              if column != "revision_id")
        )[:40]
        return hmac.compare_digest(
            str(revision.revision_id or ""), expected_revision_id
        )
    except (DeclaredCanonError, AttributeError, TypeError, ValueError):
        return False


def _revision_subject_shape_valid(revision: DeclaredCanonRevision) -> bool:
    if (
        revision.subject_type not in GENERAL_SUBJECT_TYPES
        or not _STABLE_ID_RE.fullmatch(revision.subject_id)
        or not _PREDICATE_RE.fullmatch(revision.predicate)
    ):
        return False
    if revision.claim_kind == "relationship":
        return bool(
            revision.object_subject_type in GENERAL_SUBJECT_TYPES
            and _STABLE_ID_RE.fullmatch(revision.object_subject_id)
        )
    return not revision.object_subject_type and not revision.object_subject_id


def _require_revision_contract(revision: DeclaredCanonRevision) -> None:
    if revision.source_system not in _SOURCE_SYSTEMS:
        raise DeclaredCanonError("invalid_source_system")
    if revision.domain not in CANON_DOMAINS:
        raise DeclaredCanonError("invalid_domain")
    if revision.claim_kind not in CLAIM_KINDS:
        raise DeclaredCanonError("invalid_claim_kind")
    if revision.visibility not in VISIBILITIES:
        raise DeclaredCanonError("invalid_visibility")
    if revision.lifecycle_status not in GENERAL_LIFECYCLES:
        raise DeclaredCanonError("invalid_lifecycle_status")
    if not _revision_subject_shape_valid(revision):
        raise DeclaredCanonError("classification_subject_link_invalid")
    try:
        routes = json.loads(revision.eligible_routes_json or "[]")
    except (TypeError, json.JSONDecodeError):
        raise DeclaredCanonError("invalid_eligible_routes_json")
    if not isinstance(routes, list):
        raise DeclaredCanonError("invalid_eligible_routes_json")
    if _canonical_routes(routes, revision.visibility) != revision.eligible_routes_json:
        raise DeclaredCanonError("noncanonical_eligible_routes")
    _parse_validity(revision.valid_from, field="valid_from")
    _parse_validity(revision.valid_until, field="valid_until")
    if revision.source_system == GENERAL_DECLARATION_SOURCE:
        if revision.classification_mode != "owner_explicit":
            raise DeclaredCanonError("general_classification_mode_invalid")
        try:
            json.loads(revision.value_json)
        except (TypeError, json.JSONDecodeError):
            raise DeclaredCanonError("invalid_typed_value")
    else:
        if revision.classification_mode not in {
            "owner_explicit_default_mapping",
            "owner_explicit_mapping_override",
            "owner_explicit_legacy_mapping",
        }:
            raise DeclaredCanonError("broadcast_classification_mode_invalid")
        if (
            revision.raw_declaration
            or revision.cleaned_summary
            or revision.value_json
            or revision.derived_from_source_ref
        ):
            raise DeclaredCanonError("broadcast_sidecar_content_or_projection_invalid")


def _require_trusted_stored_revision(
    revision: DeclaredCanonRevision,
) -> None:
    """Reject any prior row that cannot safely anchor a new revision."""

    if not _stored_authority_valid(revision):
        raise DeclaredCanonError("stored_authority_invalid")
    _require_revision_contract(revision)
    if revision.source_system == GENERAL_DECLARATION_SOURCE:
        if revision.source_row_id != revision.declaration_id:
            raise DeclaredCanonError("general_source_identity_invalid")
        fields = {
            "raw_declaration": revision.raw_declaration,
            "cleaned_summary": revision.cleaned_summary,
            "subject_type": revision.subject_type,
            "subject_id": revision.subject_id,
            "object_subject_type": revision.object_subject_type,
            "object_subject_id": revision.object_subject_id,
            "predicate": revision.predicate,
            "value_json": revision.value_json,
            "domain": revision.domain,
            "claim_kind": revision.claim_kind,
            "visibility": revision.visibility,
            "eligible_routes_json": revision.eligible_routes_json,
            "valid_from": revision.valid_from,
            "valid_until": revision.valid_until,
        }
        if _general_fingerprint(fields) != revision.source_fingerprint:
            raise DeclaredCanonError("declared_source_fingerprint_invalid")
        return
    if revision.source_system == BROADCAST_MEMORY_SOURCE:
        try:
            source_row_id = int(revision.source_row_id)
        except (TypeError, ValueError):
            raise DeclaredCanonError("broadcast_source_identity_invalid")
        expected_declaration_id = "dcl_" + _digest(
            DECLARED_CANON_CONTRACT_VERSION,
            int(revision.guild_id),
            BROADCAST_MEMORY_SOURCE,
            source_row_id,
        )[:32]
        if revision.declaration_id != expected_declaration_id:
            raise DeclaredCanonError("broadcast_source_identity_invalid")
        if not re.fullmatch(
            r"bsrc_[0-9a-f]{40}",
            str(revision.source_fingerprint or ""),
        ):
            raise DeclaredCanonError("broadcast_source_fingerprint_invalid")
        return
    raise DeclaredCanonError("invalid_source_system")


def validate_declared_canon_read_boundary(
    conn: sqlite3.Connection, *, guild_id: int
) -> tuple[DeclaredCanonRevision, ...]:
    """Validate schema and every current chain inside a caller-owned snapshot.

    Internal inventory adapters use this boundary after opening their own read
    snapshot.  It authenticates stored authority without manufacturing a new
    owner request, nonce, or receipt.
    """

    if not conn.in_transaction:
        raise DeclaredCanonError("declared_canon_read_snapshot_required")
    configured_owner = _configured_owner_user_id()
    if configured_owner <= 0:
        raise DeclaredCanonError("owner_user_id_not_configured")
    configured_guild = _configured_primary_guild_id()
    if configured_guild <= 0:
        raise DeclaredCanonError("primary_guild_id_not_configured")
    if int(guild_id or 0) != configured_guild:
        raise DeclaredCanonError("configured_primary_guild_required")
    _authority_secret()
    _require_schema(conn)
    declaration_ids = tuple(
        str(row[0])
        for row in conn.execute(
            "SELECT DISTINCT declaration_id "
            "FROM main.declared_canon_revisions WHERE guild_id=? "
            "ORDER BY declaration_id",
            (configured_guild,),
        ).fetchall()
    )
    latest: list[DeclaredCanonRevision] = []
    for declaration_id in declaration_ids:
        chain = _validated_revision_chain(
            conn, guild_id=configured_guild, declaration_id=declaration_id
        )
        if not chain:
            raise DeclaredCanonError("declared_canon_revision_chain_invalid")
        latest.append(chain[-1])
    return tuple(latest)


def validate_current_declared_canon_revision(
    conn: sqlite3.Connection,
    *,
    actor_user_id: int,
    authority_nonce: str,
    guild_id: int,
    declaration_id: str,
    expected_revision_id: str,
    expected_source_fingerprint: str,
    now: str = "",
) -> DeclaredCanonRevision:
    """Validate one exact current revision inside a coherent read snapshot."""

    with _read_snapshot(conn):
        return _validate_current_declared_canon_revision(
            conn,
            actor_user_id=actor_user_id,
            authority_nonce=authority_nonce,
            guild_id=guild_id,
            declaration_id=declaration_id,
            expected_revision_id=expected_revision_id,
            expected_source_fingerprint=expected_source_fingerprint,
            now=now,
        )


def _validate_current_declared_canon_revision(
    conn: sqlite3.Connection,
    *,
    actor_user_id: int,
    authority_nonce: str,
    guild_id: int,
    declaration_id: str,
    expected_revision_id: str,
    expected_source_fingerprint: str,
    now: str = "",
) -> DeclaredCanonRevision:
    """Read-only foundation validator for a later Ledger integration.

    This function does not project or write anything.  The caller must be an
    already-authenticated Discord command boundary and must present the exact
    owner-reviewed revision and source fingerprint.  Only a current,
    source-intersecting ``established`` revision is returned.
    """

    _require_configured_owner_actor(
        actor_user_id=actor_user_id, guild_id=guild_id
    )
    before = int(conn.total_changes or 0)
    _require_schema(conn)
    normalized_declaration_id = _validate_stable_token(
        declaration_id, field="declaration_id", pattern=_STABLE_ID_RE
    )
    normalized_revision_id = _validate_stable_token(
        expected_revision_id,
        field="expected_revision_id",
        pattern=_STABLE_ID_RE,
    )
    normalized_fingerprint = str(expected_source_fingerprint or "").strip()
    if not re.fullmatch(r"(?:src|bsrc)_[0-9a-f]{40}", normalized_fingerprint):
        raise DeclaredCanonError("expected_source_fingerprint_required")
    requested_now, evaluation_now = _requested_and_created_at(now)
    parsed_now = _parse_validity(evaluation_now, field="validation_now")[1]
    assert parsed_now is not None
    _authorize_request(
        actor_user_id=actor_user_id,
        guild_id=guild_id,
        operation="preview_declared",
        authority_nonce=authority_nonce,
        binding={
            "validation": "current_established_revision",
            "declaration_id": normalized_declaration_id,
            "expected_revision_id": normalized_revision_id,
            "expected_source_fingerprint": normalized_fingerprint,
            "requested_now": requested_now,
        },
    )
    revision = _latest_revision(
        conn,
        guild_id=int(guild_id),
        declaration_id=normalized_declaration_id,
    )
    if revision is None or revision.revision_id != normalized_revision_id:
        raise DeclaredCanonError("expected_revision_mismatch")
    if revision.source_fingerprint != normalized_fingerprint:
        raise DeclaredCanonError("expected_source_fingerprint_mismatch")
    if not _stored_authority_valid(revision):
        raise DeclaredCanonError("stored_authority_invalid")
    if revision.lifecycle_status != "established":
        raise DeclaredCanonError("declared_revision_not_established")
    _require_revision_contract(revision)
    if _validity_window_state(
        revision.valid_from, revision.valid_until, parsed_now
    ) not in {"unbounded", "current"}:
        raise DeclaredCanonError("declared_revision_not_current")
    if revision.source_system == GENERAL_DECLARATION_SOURCE:
        if revision.source_row_id != revision.declaration_id:
            raise DeclaredCanonError("general_source_identity_invalid")
        fields = {
            "raw_declaration": revision.raw_declaration,
            "cleaned_summary": revision.cleaned_summary,
            "subject_type": revision.subject_type,
            "subject_id": revision.subject_id,
            "object_subject_type": revision.object_subject_type,
            "object_subject_id": revision.object_subject_id,
            "predicate": revision.predicate,
            "value_json": revision.value_json,
            "domain": revision.domain,
            "claim_kind": revision.claim_kind,
            "visibility": revision.visibility,
            "eligible_routes_json": revision.eligible_routes_json,
            "valid_from": revision.valid_from,
            "valid_until": revision.valid_until,
        }
        if _general_fingerprint(fields) != normalized_fingerprint:
            raise DeclaredCanonError("declared_source_fingerprint_invalid")
    elif revision.source_system == BROADCAST_MEMORY_SOURCE:
        try:
            source_row_id = int(revision.source_row_id)
        except (TypeError, ValueError):
            raise DeclaredCanonError("broadcast_source_identity_invalid")
        source = _broadcast_row(
            conn, guild_id=int(guild_id), row_id=source_row_id
        )
        if source is None:
            raise DeclaredCanonError("broadcast_source_not_found")
        if broadcast_source_fingerprint(source) != normalized_fingerprint:
            raise DeclaredCanonError("expected_source_fingerprint_mismatch")
        if _broadcast_lifecycle(source, parsed_now) != "established" or (
            _broadcast_validity_state(source, parsed_now)
            not in {"unbounded", "current"}
        ):
            raise DeclaredCanonError("broadcast_source_not_current")
        entry_type = str(source.get("entry_type") or "")
        if entry_type not in BROADCAST_TYPE_DEFAULTS and (
            revision.classification_mode != "owner_explicit_legacy_mapping"
        ):
            raise DeclaredCanonError("legacy_type_review_only")
        if revision.visibility in PUBLIC_VISIBILITIES:
            scopes = _scope_tokens(source.get("usage_scope"))
            if (
                not _strict_bool(source.get("public_safe"))
                or "internal" in scopes
                or entry_type == "moderation_context"
            ):
                raise DeclaredCanonError("broadcast_public_eligibility_invalid")
            _validate_broadcast_public_routes(
                routes_json=revision.eligible_routes_json,
                source_scopes=scopes,
            )
    else:
        raise DeclaredCanonError("invalid_source_system")
    if int(conn.total_changes or 0) != before:
        raise DeclaredCanonError("read_validator_mutated_state")
    return revision


def validate_latest_declared_canon_revision(
    conn: sqlite3.Connection,
    *,
    actor_user_id: int,
    authority_nonce: str,
    guild_id: int,
    declaration_id: str,
    expected_revision_id: str,
    expected_source_fingerprint: str,
    expected_lifecycle_status: str,
    now: str = "",
) -> DeclaredCanonRevision:
    """Validate one exact terminal revision inside a coherent read snapshot."""

    with _read_snapshot(conn):
        return _validate_latest_declared_canon_revision(
            conn,
            actor_user_id=actor_user_id,
            authority_nonce=authority_nonce,
            guild_id=guild_id,
            declaration_id=declaration_id,
            expected_revision_id=expected_revision_id,
            expected_source_fingerprint=expected_source_fingerprint,
            expected_lifecycle_status=expected_lifecycle_status,
            now=now,
        )


def _validate_latest_declared_canon_revision(
    conn: sqlite3.Connection,
    *,
    actor_user_id: int,
    authority_nonce: str,
    guild_id: int,
    declaration_id: str,
    expected_revision_id: str,
    expected_source_fingerprint: str,
    expected_lifecycle_status: str,
    now: str = "",
) -> DeclaredCanonRevision:
    """Validate an exact latest revision, including historical/terminal state.

    This read-only API exists so a later Ledger adapter can project an explicit
    retraction/supersession instead of leaving an older established projection
    effective.  It does not call terminal records current and does not write.
    """

    _require_configured_owner_actor(
        actor_user_id=actor_user_id, guild_id=guild_id
    )
    before = int(conn.total_changes or 0)
    _require_schema(conn)
    normalized_declaration_id = _validate_stable_token(
        declaration_id, field="declaration_id", pattern=_STABLE_ID_RE
    )
    normalized_revision_id = _validate_stable_token(
        expected_revision_id,
        field="expected_revision_id",
        pattern=_STABLE_ID_RE,
    )
    normalized_fingerprint = str(expected_source_fingerprint or "").strip()
    if not re.fullmatch(r"(?:src|bsrc)_[0-9a-f]{40}", normalized_fingerprint):
        raise DeclaredCanonError("expected_source_fingerprint_required")
    normalized_lifecycle = str(expected_lifecycle_status or "").strip().casefold()
    if normalized_lifecycle not in GENERAL_LIFECYCLES:
        raise DeclaredCanonError("expected_lifecycle_status_invalid")
    if normalized_lifecycle == "established":
        raise DeclaredCanonError("latest_validator_requires_noncurrent_lifecycle")
    requested_now, evaluation_now = _requested_and_created_at(now)
    parsed_now = _parse_validity(evaluation_now, field="validation_now")[1]
    assert parsed_now is not None
    _authorize_request(
        actor_user_id=actor_user_id,
        guild_id=guild_id,
        operation="preview_declared",
        authority_nonce=authority_nonce,
        binding={
            "validation": "exact_latest_revision",
            "declaration_id": normalized_declaration_id,
            "expected_revision_id": normalized_revision_id,
            "expected_source_fingerprint": normalized_fingerprint,
            "expected_lifecycle_status": normalized_lifecycle,
            "requested_now": requested_now,
        },
    )
    revision = _latest_revision(
        conn,
        guild_id=int(guild_id),
        declaration_id=normalized_declaration_id,
    )
    if revision is None or revision.revision_id != normalized_revision_id:
        raise DeclaredCanonError("expected_revision_mismatch")
    if revision.source_fingerprint != normalized_fingerprint:
        raise DeclaredCanonError("expected_source_fingerprint_mismatch")
    if revision.lifecycle_status != normalized_lifecycle:
        raise DeclaredCanonError("expected_lifecycle_status_mismatch")
    if not _stored_authority_valid(revision):
        raise DeclaredCanonError("stored_authority_invalid")
    _require_revision_contract(revision)
    if revision.source_system == GENERAL_DECLARATION_SOURCE:
        if revision.source_row_id != revision.declaration_id:
            raise DeclaredCanonError("general_source_identity_invalid")
        fields = {
            "raw_declaration": revision.raw_declaration,
            "cleaned_summary": revision.cleaned_summary,
            "subject_type": revision.subject_type,
            "subject_id": revision.subject_id,
            "object_subject_type": revision.object_subject_type,
            "object_subject_id": revision.object_subject_id,
            "predicate": revision.predicate,
            "value_json": revision.value_json,
            "domain": revision.domain,
            "claim_kind": revision.claim_kind,
            "visibility": revision.visibility,
            "eligible_routes_json": revision.eligible_routes_json,
            "valid_from": revision.valid_from,
            "valid_until": revision.valid_until,
        }
        if _general_fingerprint(fields) != normalized_fingerprint:
            raise DeclaredCanonError("declared_source_fingerprint_invalid")
    elif revision.source_system == BROADCAST_MEMORY_SOURCE:
        try:
            source_row_id = int(revision.source_row_id)
        except (TypeError, ValueError):
            raise DeclaredCanonError("broadcast_source_identity_invalid")
        source = _broadcast_row(
            conn, guild_id=int(guild_id), row_id=source_row_id
        )
        if source is None:
            raise DeclaredCanonError("broadcast_source_not_found")
        if broadcast_source_fingerprint(source) != normalized_fingerprint:
            raise DeclaredCanonError("expected_source_fingerprint_mismatch")
        if _broadcast_lifecycle(source, parsed_now) != normalized_lifecycle:
            raise DeclaredCanonError("broadcast_lifecycle_intersection_mismatch")
        entry_type = str(source.get("entry_type") or "")
        if entry_type not in BROADCAST_TYPE_DEFAULTS and (
            revision.classification_mode != "owner_explicit_legacy_mapping"
        ):
            raise DeclaredCanonError("legacy_type_review_only")
        if (
            normalized_lifecycle == "established"
            and revision.visibility in PUBLIC_VISIBILITIES
        ):
            scopes = _scope_tokens(source.get("usage_scope"))
            if (
                not _strict_bool(source.get("public_safe"))
                or "internal" in scopes
                or entry_type == "moderation_context"
            ):
                raise DeclaredCanonError("broadcast_public_eligibility_invalid")
            _validate_broadcast_public_routes(
                routes_json=revision.eligible_routes_json,
                source_scopes=scopes,
            )
    else:
        raise DeclaredCanonError("invalid_source_system")
    if int(conn.total_changes or 0) != before:
        raise DeclaredCanonError("read_validator_mutated_state")
    return revision


def preview_declared_canon(
    conn: sqlite3.Connection,
    *,
    actor_user_id: int,
    authority_nonce: str,
    guild_id: int,
    source_system: str = "",
    limit: int = 100,
) -> DeclaredPreview:
    """Return content-free revision metadata from one coherent read snapshot."""

    with _read_snapshot(conn):
        return _preview_declared_canon(
            conn,
            actor_user_id=actor_user_id,
            authority_nonce=authority_nonce,
            guild_id=guild_id,
            source_system=source_system,
            limit=limit,
        )


def _preview_declared_canon(
    conn: sqlite3.Connection,
    *,
    actor_user_id: int,
    authority_nonce: str,
    guild_id: int,
    source_system: str = "",
    limit: int = 100,
) -> DeclaredPreview:
    """Return content-free revision metadata without creating schema or rows."""

    before = int(conn.total_changes or 0)
    normalized_source = str(source_system or "").strip().casefold()
    if normalized_source and normalized_source not in _SOURCE_SYSTEMS:
        raise DeclaredCanonError("invalid_source_system")
    safe_limit = max(1, min(int(limit or 100), 500))
    authority = _authorize_request(
        actor_user_id=actor_user_id,
        guild_id=guild_id,
        operation="preview_declared",
        authority_nonce=authority_nonce,
        binding={"source_system": normalized_source, "limit": safe_limit},
    )
    if not _table_exists(conn, DECLARED_CANON_TABLE):
        return DeclaredPreview(
            DECLARED_CANON_CONTRACT_VERSION,
            int(guild_id),
            _opaque_actor_ref(authority),
            _receipt_id(authority),
            (),
            0,
            False,
            int(conn.total_changes or 0) - before,
        )
    validate_declared_canon_read_boundary(conn, guild_id=int(guild_id))
    where = ["guild_id=?"]
    params: list[Any] = [int(guild_id)]
    if normalized_source:
        where.append("source_system=?")
        params.append(normalized_source)
    total = int(
        conn.execute(
            "SELECT COUNT(*) FROM main.declared_canon_revisions WHERE %s"
            % " AND ".join(where),
            tuple(params),
        ).fetchone()[0]
        or 0
    )
    rows = conn.execute(
        """
        SELECT d.declaration_id,d.revision_id,d.revision_number,d.source_system,
               domain,claim_kind,visibility,lifecycle_status,classification_mode,
               CASE WHEN revision_number=(
                   SELECT MAX(r2.revision_number)
                   FROM main.declared_canon_revisions r2
                   WHERE r2.guild_id=d.guild_id
                     AND r2.declaration_id=d.declaration_id
               ) THEN 1 ELSE 0 END
        FROM main.declared_canon_revisions d
        WHERE %s
        ORDER BY declaration_id,revision_number DESC
        LIMIT ?
        """ % " AND ".join(where),
        (*params, safe_limit),
    ).fetchall()
    after = int(conn.total_changes or 0)
    return DeclaredPreview(
        DECLARED_CANON_CONTRACT_VERSION,
        int(guild_id),
        _opaque_actor_ref(authority),
        _receipt_id(authority),
        tuple(
            DeclaredPreviewItem(
                declaration_id=str(row[0]),
                revision_id=str(row[1]),
                revision_number=int(row[2]),
                source_system=str(row[3]),
                domain=str(row[4]),
                claim_kind=str(row[5]),
                visibility=str(row[6]),
                lifecycle_status=str(row[7]),
                classification_mode=str(row[8]),
                current_revision=bool(row[9]),
            )
            for row in rows
        ),
        total,
        total > len(rows),
        after - before,
    )


def _preview_validity_state(value: Any, now: datetime) -> str:
    raw = str(value or "").strip()
    if not raw:
        return "unbounded"
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return "invalid"
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return "expired" if parsed.astimezone(timezone.utc) < now else "current"


def _validity_window_state(
    valid_from: Any, valid_until: Any, now: datetime
) -> str:
    raw_from = str(valid_from or "").strip()
    raw_until = str(valid_until or "").strip()
    try:
        parsed_from = _parse_validity(raw_from, field="valid_from")[1]
        parsed_until = _parse_validity(raw_until, field="valid_until")[1]
    except DeclaredCanonError:
        return "invalid"
    if parsed_from and parsed_until and parsed_until < parsed_from:
        return "invalid"
    if parsed_from and parsed_from > now:
        return "not_started"
    if parsed_until and parsed_until < now:
        return "expired"
    if not raw_from and not raw_until:
        return "unbounded"
    return "current"


def preview_historical_broadcast_memory(
    conn: sqlite3.Connection,
    *,
    actor_user_id: int,
    authority_nonce: str,
    guild_id: int,
    limit: int = 200,
    offset: int = 0,
    now: str = "",
) -> BroadcastHistoryPreview:
    """Build a zero-write preview from one coherent read snapshot."""

    with _read_snapshot(conn):
        return _preview_historical_broadcast_memory(
            conn,
            actor_user_id=actor_user_id,
            authority_nonce=authority_nonce,
            guild_id=guild_id,
            limit=limit,
            offset=offset,
            now=now,
        )


def _preview_historical_broadcast_memory(
    conn: sqlite3.Connection,
    *,
    actor_user_id: int,
    authority_nonce: str,
    guild_id: int,
    limit: int = 200,
    offset: int = 0,
    now: str = "",
) -> BroadcastHistoryPreview:
    """Build a zero-write, content-free classification preview.

    Raw/clean content, source row IDs, stable source tokens, names, and account
    IDs are never returned.  Item keys are derived from this preview receipt and
    therefore cannot correlate rows across previews.  Era is review grouping,
    never authority.
    """

    before = int(conn.total_changes or 0)
    safe_limit = max(1, min(int(limit or 200), 500))
    safe_offset = max(0, int(offset or 0))
    cutoff_text, cutoff_value = _parse_validity(
        BROADCAST_DECLARED_CANON_OWNER_ERA_CUTOFF,
        field="owner_era_cutoff",
    )
    if cutoff_value is None:
        raise DeclaredCanonError("owner_era_cutoff_required")
    now_text, now_value = _parse_validity(now or _utc_now(), field="preview_now")
    assert now_value is not None
    authority = _authorize_request(
        actor_user_id=actor_user_id,
        guild_id=guild_id,
        operation="preview_broadcast",
        authority_nonce=authority_nonce,
        binding={
            "limit": safe_limit,
            "offset": safe_offset,
            "owner_era_cutoff": cutoff_text,
            "preview_now": now_text,
        },
    )
    receipt_id = _receipt_id(authority)
    columns = _table_columns(conn, BROADCAST_MEMORY_SOURCE)
    required = {"id", "guild_id", "entry_type", "status", "created_at"}
    if not required.issubset(set(columns)):
        return BroadcastHistoryPreview(
            contract_version=DECLARED_CANON_CONTRACT_VERSION,
            guild_id=int(guild_id),
            authority_actor_ref=_opaque_actor_ref(authority),
            authority_receipt_id=receipt_id,
            owner_era_cutoff=cutoff_text,
            items=(),
            total_rows=0,
            truncated=False,
            counts_scope="returned_page",
            type_counts={},
            era_counts={},
            disposition_counts={},
            mutation_count=int(conn.total_changes or 0) - before,
        )
    total = int(
        conn.execute(
            "SELECT COUNT(*) FROM main.broadcast_memory WHERE guild_id=?",
            (int(guild_id),),
        ).fetchone()[0]
        or 0
    )
    source_cursor = conn.execute(
        "SELECT * FROM main.broadcast_memory WHERE guild_id=? ORDER BY id LIMIT ? OFFSET ?",
        (int(guild_id), safe_limit, safe_offset),
    )
    returned_columns = tuple(
        str(item[0] or "") for item in (source_cursor.description or ())
    )
    raw_rows = source_cursor.fetchall()
    rows = tuple(dict(zip(returned_columns, row)) for row in raw_rows)
    classification_rows: dict[str, dict[str, str]] = {}
    if _table_exists(conn, DECLARED_CANON_TABLE):
        validate_declared_canon_read_boundary(conn, guild_id=int(guild_id))
        source_ids = tuple(str(row.get("id")) for row in rows)
        if source_ids:
            placeholders = ",".join("?" for _ in source_ids)
            selected = ",".join(
                "d.%s" % column for column in _REVISION_COLUMNS
            )
            for classified_row in conn.execute(
                """
                SELECT %s
                FROM main.declared_canon_revisions d
                JOIN (
                    SELECT declaration_id,MAX(revision_number) AS max_revision
                    FROM main.declared_canon_revisions
                    WHERE guild_id=? AND source_system='broadcast_memory'
                      AND source_row_id IN (%s)
                    GROUP BY declaration_id
                ) latest
                  ON latest.declaration_id=d.declaration_id
                 AND latest.max_revision=d.revision_number
                WHERE d.guild_id=? AND d.source_system='broadcast_memory'
                """ % (selected, placeholders),
                (int(guild_id), *source_ids, int(guild_id)),
            ).fetchall():
                item = dict(zip(_REVISION_COLUMNS, classified_row))
                item["_validated_revision"] = _row_to_revision(classified_row)
                classification_rows[str(item["source_row_id"])] = item
    items: list[BroadcastPreviewItem] = []
    type_counts: dict[str, int] = {}
    era_counts: dict[str, int] = {}
    disposition_counts: dict[str, int] = {}
    for position, row in enumerate(rows, start=1):
        row_id = int(row.get("id") or 0)
        entry_type = str(row.get("entry_type") or "")
        preview_entry_type = (
            entry_type
            if entry_type in BROADCAST_TYPE_DEFAULTS
            else "legacy_or_unrecognized"
        )
        status = str(row.get("status") or "").strip().casefold()
        preview_status = (
            status
            if status in {"active", "resolved", "superseded"}
            else "unrecognized"
        )
        scopes = _scope_tokens(row.get("usage_scope"))
        safe_scopes = scopes.intersection(BROADCAST_USAGE_SCOPES)
        unknown_scopes = scopes.difference(BROADCAST_USAGE_SCOPES)
        public_safe = _strict_bool(row.get("public_safe"))
        raw_submitter = row.get("submitted_by_user_id")
        submitter_malformed = False
        try:
            submitter_id = int(raw_submitter or 0)
        except (TypeError, ValueError, OverflowError):
            submitter_id = 0
            submitter_malformed = bool(str(raw_submitter or "").strip())
        submitter_matches = submitter_id == int(authority.actor_user_id)
        recognized = entry_type in BROADCAST_TYPE_DEFAULTS
        classified = classification_rows.get(str(row_id))
        legacy_explicit = bool(
            classified
            and classified.get("classification_mode")
            == "owner_explicit_legacy_mapping"
        )
        reasons: list[str] = []
        try:
            source_created = _parse_validity(
                row.get("created_at"), field="broadcast_created_at"
            )[1]
        except DeclaredCanonError:
            source_created = None
        if source_created is None:
            source_era = "unknown_era"
        elif source_created < cutoff_value:
            source_era = "pre_declared_canon_owner_era"
        else:
            source_era = "post_declared_canon_owner_era"
        reasons.append(source_era)
        if not recognized and not legacy_explicit:
            reasons.append("legacy_type_review_only")
        elif not recognized:
            reasons.append("legacy_type_explicitly_classified")
        if submitter_malformed:
            reasons.append("submitter_identity_malformed")
        if not submitter_matches:
            reasons.append("owner_authorship_unverified")
        if status not in {"active", "resolved", "superseded"}:
            reasons.append("status_unrecognized")
        if status in {"resolved", "superseded"}:
            reasons.append("historical_not_current")
        if "internal" in scopes:
            reasons.append("internal_scope")
        if unknown_scopes:
            reasons.append("usage_scope_unrecognized")
        validity_state = _broadcast_validity_state(row, now_value)
        if validity_state in {"expired", "invalid", "not_started"}:
            reasons.append("validity_%s" % validity_state)
        linked = bool(row.get("supersedes_id") or row.get("superseded_by_id"))
        if entry_type == "continuity_backreference":
            reasons.append("derived_relationship_ambiguous")
        try:
            current_source_fingerprint = broadcast_source_fingerprint(row)
            source_versioned = True
        except DeclaredCanonError:
            current_source_fingerprint = ""
            source_versioned = False
            reasons.append("broadcast_source_unversioned")
        classification_valid = False
        if not classified:
            classification_state = "unclassified"
            fingerprint_state = "unclassified"
            subject_link_state = "unclassified"
        else:
            stored_fingerprint = str(classified.get("source_fingerprint") or "")
            fingerprint_state = (
                "current"
                if source_versioned
                and stored_fingerprint == current_source_fingerprint
                else "stale_or_unversioned"
            )
            if fingerprint_state != "current":
                reasons.append("classification_source_stale")
            subject_type = str(classified.get("subject_type") or "")
            subject_id = str(classified.get("subject_id") or "")
            claim_kind = str(classified.get("claim_kind") or "")
            predicate = str(classified.get("predicate") or "")
            domain = str(classified.get("domain") or "")
            visibility = str(classified.get("visibility") or "")
            object_type = str(classified.get("object_subject_type") or "")
            object_id = str(classified.get("object_subject_id") or "")
            subject_valid = bool(
                subject_type in GENERAL_SUBJECT_TYPES
                and _STABLE_ID_RE.fullmatch(subject_id)
                and _PREDICATE_RE.fullmatch(predicate)
                and domain in CANON_DOMAINS
                and claim_kind in CLAIM_KINDS
                and visibility in VISIBILITIES
            )
            if claim_kind == "relationship":
                subject_valid = bool(
                    subject_valid
                    and object_type in GENERAL_SUBJECT_TYPES
                    and _STABLE_ID_RE.fullmatch(object_id)
                )
            elif object_type or object_id:
                subject_valid = False
            if classified.get("derived_from_source_ref"):
                subject_link_state = "derived_projection_not_authoritative"
                subject_valid = False
                reasons.append("derived_projection_not_supported")
            else:
                subject_link_state = (
                    "explicit_typed" if subject_valid else "invalid_or_missing"
                )
            if not subject_valid:
                reasons.append("classification_subject_link_invalid")
            authority_valid = _stored_authority_valid(
                classified.get("_validated_revision")
            )
            if not authority_valid:
                reasons.append("classification_authority_invalid")
            metadata_valid = True
            try:
                mode = str(classified.get("classification_mode") or "")
                if mode not in {
                    "owner_explicit_default_mapping",
                    "owner_explicit_mapping_override",
                    "owner_explicit_legacy_mapping",
                }:
                    raise DeclaredCanonError("broadcast_classification_mode_invalid")
                if recognized and mode == "owner_explicit_legacy_mapping":
                    raise DeclaredCanonError("broadcast_classification_mode_invalid")
                if not recognized and mode != "owner_explicit_legacy_mapping":
                    raise DeclaredCanonError("legacy_type_review_only")
                decoded_routes = json.loads(
                    str(classified.get("eligible_routes_json") or "[]")
                )
                if not isinstance(decoded_routes, list):
                    raise DeclaredCanonError("invalid_eligible_routes_json")
                if (
                    _canonical_routes(decoded_routes, visibility)
                    != str(classified.get("eligible_routes_json") or "[]")
                ):
                    raise DeclaredCanonError("noncanonical_eligible_routes")
            except (DeclaredCanonError, json.JSONDecodeError, TypeError):
                metadata_valid = False
                reasons.append("classification_metadata_invalid")
            lifecycle = str(classified.get("lifecycle_status") or "")
            lifecycle_intersects = (
                lifecycle == "established"
                and status == "active"
                and not row.get("superseded_by_id")
                and validity_state in {"unbounded", "current"}
            ) or (
                lifecycle in {"resolved", "superseded"}
                and status == lifecycle
            )
            if not lifecycle_intersects:
                reasons.append("classification_lifecycle_stale")
            public_intersects = True
            if visibility in PUBLIC_VISIBILITIES:
                if (
                    not public_safe
                    or "internal" in scopes
                    or entry_type == "moderation_context"
                    or lifecycle != "established"
                ):
                    public_intersects = False
                try:
                    _validate_broadcast_public_routes(
                        routes_json=str(
                            classified.get("eligible_routes_json") or "[]"
                        ),
                        source_scopes=scopes,
                    )
                except (DeclaredCanonError, json.JSONDecodeError, TypeError):
                    public_intersects = False
                if not public_intersects:
                    reasons.append("classification_public_eligibility_stale")
            classification_valid = bool(
                fingerprint_state == "current"
                and subject_valid
                and authority_valid
                and metadata_valid
                and lifecycle_intersects
                and public_intersects
                and (recognized or legacy_explicit)
            )
            classification_state = "%s:%s:%s" % (
                "current" if classification_valid else "not_current",
                lifecycle if lifecycle in GENERAL_LIFECYCLES else "invalid",
                str(classified.get("classification_mode") or "")
                if str(classified.get("classification_mode") or "")
                in {
                    "owner_explicit_default_mapping",
                    "owner_explicit_mapping_override",
                    "owner_explicit_legacy_mapping",
                }
                else "invalid",
            )
        if classification_valid and str(classified.get("lifecycle_status")) == "established":
            disposition = "declared_classification_current"
        elif classification_valid:
            disposition = "declared_classification_historical"
        elif classified:
            disposition = "stale_classification_review"
        else:
            disposition = "needs_owner_review"
        type_counts[preview_entry_type] = type_counts.get(preview_entry_type, 0) + 1
        era_counts[source_era] = era_counts.get(source_era, 0) + 1
        disposition_counts[disposition] = disposition_counts.get(disposition, 0) + 1
        items.append(
            BroadcastPreviewItem(
                preview_item="item_" + _digest(receipt_id, position)[:16],
                source_era=source_era,
                entry_type=preview_entry_type,
                status=preview_status,
                usage_scope=",".join(
                    (*sorted(safe_scopes),)
                    + (("unknown_scope_present",) if unknown_scopes else ())
                ),
                public_safe=public_safe,
                submitter_state=(
                    "configured_owner" if submitter_matches else "unverified"
                ),
                validity_state=validity_state,
                derivation_state="source_lineage_linked" if linked else "primary",
                subject_link_state=subject_link_state,
                source_fingerprint_state=fingerprint_state,
                classification_state=classification_state,
                disposition=disposition,
                reason_codes=tuple(sorted(set(reasons))),
            )
        )
    after = int(conn.total_changes or 0)
    return BroadcastHistoryPreview(
        contract_version=DECLARED_CANON_CONTRACT_VERSION,
        guild_id=int(guild_id),
        authority_actor_ref=_opaque_actor_ref(authority),
        authority_receipt_id=receipt_id,
        owner_era_cutoff=cutoff_text,
        items=tuple(items),
        total_rows=total,
        truncated=safe_offset + len(rows) < total,
        counts_scope="returned_page",
        type_counts=dict(sorted(type_counts.items())),
        era_counts=dict(sorted(era_counts.items())),
        disposition_counts=dict(sorted(disposition_counts.items())),
        mutation_count=after - before,
    )


__all__ = [
    "ALLOWED_ELIGIBLE_ROUTES",
    "BROADCAST_DECLARED_CANON_OWNER_ERA_CUTOFF",
    "BROADCAST_MEMORY_SOURCE",
    "BROADCAST_PUBLIC_ROUTE_SCOPES",
    "BROADCAST_SOURCE_FINGERPRINT_VERSION",
    "BROADCAST_TYPE_DEFAULTS",
    "BroadcastHistoryPreview",
    "BroadcastPreviewItem",
    "DECLARED_CANON_CONTRACT_VERSION",
    "DECLARED_CANON_AUTHORITY_SECRET_ENV",
    "DECLARED_CANON_AUTHORITY_SECRET_MIN_BYTES",
    "DECLARED_CANON_TABLE",
    "DeclaredCanonError",
    "DeclaredCanonRevision",
    "DeclaredPreview",
    "DeclaredPreviewItem",
    "GENERAL_DECLARATION_SOURCE",
    "INTERNAL_AUTHORITY_RECEIPT_VERSION",
    "MutationResult",
    "add_declared_canon",
    "broadcast_source_fingerprint",
    "change_declared_canon_status",
    "classify_broadcast_memory",
    "correct_declared_canon",
    "ensure_declared_canon_schema",
    "preview_declared_canon",
    "preview_historical_broadcast_memory",
    "retire_declared_canon",
    "supersede_declared_canon",
    "validate_current_declared_canon_revision",
    "validate_declared_canon_read_boundary",
    "validate_latest_declared_canon_revision",
]
