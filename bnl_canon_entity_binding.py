"""Owner-authorized, append-only Discord account-to-canon bindings.

This module completes the write side of the existing
``canon_entity_account_binding_v1`` read contract.  A binding links one
same-platform account to one already-known canon entity.  It never merges
entities, creates aliases, or declares a relationship between them.

The authenticated Discord actor and guild are rechecked against runtime
configuration on every operation.  Stored revisions are HMAC-bound to their
complete normalized payload and are immutable once written.
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
from typing import Any, Mapping, Sequence

from bnl_canon_source_contract import (
    CANON_ENTITY_IDENTITIES,
    ENTITY_ACCOUNT_BINDING_CONTRACT_VERSION,
    EntityAccountBinding,
)
from bnl_declared_canon import (
    DECLARED_CANON_AUTHORITY_SECRET_ENV,
    DECLARED_CANON_AUTHORITY_SECRET_MIN_BYTES,
)


BINDING_TABLE = "canon_entity_account_bindings"
BINDING_LIFECYCLE_VERSION = "canon_entity_account_binding_lifecycle_v1"
BINDING_AUTHORITY_RECEIPT_VERSION = "canon_entity_binding_receipt_v1"
_OPERATIONS = frozenset({"bind", "retire", "preview"})
_NONCE_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{7,127}$")
_DISCORD_ACCOUNT_RE = re.compile(r"^[1-9][0-9]{0,19}$")
_STABLE_ID_RE = re.compile(r"^[a-z0-9][a-z0-9_.:-]{1,119}$")
_ENTITY_IDS = frozenset(subject.key for subject in CANON_ENTITY_IDENTITIES)


class CanonEntityBindingError(ValueError):
    """Fail-closed binding error with a stable public reason code."""

    def __init__(self, code: str):
        self.code = str(code or "canon_entity_binding_error")
        super().__init__(self.code)


@dataclass(frozen=True)
class _VerifiedAuthority:
    actor_user_id: int
    guild_id: int
    operation: str
    request_fingerprint: str
    operation_id: str
    authority_actor: str


@dataclass(frozen=True)
class CanonEntityBindingRevision:
    binding_revision_id: str
    binding_id: str
    revision_number: int
    guild_id: int
    platform: str
    account_id: str
    entity_id: str
    operation: str
    operation_id: str
    operation_reason: str
    previous_revision_id: str
    authority_request_fingerprint: str
    authority_actor: str
    authority_receipt: str
    binding_version: str
    authority_verified: bool
    active: bool
    lifecycle_version: str
    created_at: str

    def as_contract_binding(self) -> EntityAccountBinding:
        return EntityAccountBinding(
            entity_id=self.entity_id,
            platform=self.platform,
            account_id=self.account_id,
            authority_receipt=self.authority_receipt,
            authority_actor=self.authority_actor,
            binding_version=self.binding_version,
            authority_verified=self.authority_verified,
            active=self.active,
        )


@dataclass(frozen=True)
class CanonEntityBindingMutation:
    operation_id: str
    revision: CanonEntityBindingRevision


@dataclass(frozen=True)
class CanonEntityBindingRead:
    status: str
    bindings: tuple[EntityAccountBinding, ...] = ()
    revisions: tuple[CanonEntityBindingRevision, ...] = ()


@dataclass(frozen=True)
class CanonEntityBindingPreview:
    lifecycle_version: str
    guild_id: int
    active_count: int
    retired_count: int
    entity_counts: Mapping[str, int]
    binding_refs: tuple[tuple[str, str, str], ...]
    mutation_count: int


_REVISION_COLUMNS = (
    "binding_revision_id",
    "binding_id",
    "revision_number",
    "guild_id",
    "platform",
    "account_id",
    "entity_id",
    "operation",
    "operation_id",
    "operation_reason",
    "previous_revision_id",
    "authority_request_fingerprint",
    "authority_actor",
    "authority_receipt",
    "binding_version",
    "authority_verified",
    "active",
    "lifecycle_version",
    "created_at",
)

_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS main.canon_entity_account_bindings (
    binding_revision_id TEXT PRIMARY KEY,
    binding_id TEXT NOT NULL,
    revision_number INTEGER NOT NULL CHECK(revision_number > 0),
    guild_id INTEGER NOT NULL CHECK(guild_id > 0),
    platform TEXT NOT NULL,
    account_id TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    operation TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    operation_reason TEXT NOT NULL DEFAULT '',
    previous_revision_id TEXT NOT NULL DEFAULT '',
    authority_request_fingerprint TEXT NOT NULL,
    authority_actor TEXT NOT NULL,
    authority_receipt TEXT NOT NULL,
    binding_version TEXT NOT NULL,
    authority_verified INTEGER NOT NULL CHECK(authority_verified IN (0,1)),
    active INTEGER NOT NULL CHECK(active IN (0,1)),
    lifecycle_version TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(guild_id, binding_id, revision_number),
    UNIQUE(guild_id, binding_id, operation_id)
)
"""

_INDEX_DDL = {
    "idx_canon_entity_binding_account": """
        CREATE INDEX IF NOT EXISTS main.idx_canon_entity_binding_account
        ON canon_entity_account_bindings(
            guild_id, platform, account_id, binding_id, revision_number
        )
    """,
    "idx_canon_entity_binding_entity": """
        CREATE INDEX IF NOT EXISTS main.idx_canon_entity_binding_entity
        ON canon_entity_account_bindings(
            guild_id, entity_id, active, revision_number
        )
    """,
}

_TRIGGER_DDL = {
    "trg_canon_entity_bindings_no_conflicting_insert": """
        CREATE TRIGGER IF NOT EXISTS main.trg_canon_entity_bindings_no_conflicting_insert
        BEFORE INSERT ON canon_entity_account_bindings
        WHEN EXISTS(
            SELECT 1 FROM canon_entity_account_bindings existing
            WHERE existing.binding_revision_id=NEW.binding_revision_id
               OR (
                   existing.guild_id=NEW.guild_id
                   AND existing.binding_id=NEW.binding_id
                   AND existing.revision_number=NEW.revision_number
               )
               OR (
                   existing.guild_id=NEW.guild_id
                   AND existing.binding_id=NEW.binding_id
                   AND existing.operation_id=NEW.operation_id
               )
        )
        BEGIN
            SELECT RAISE(ABORT, 'canon_entity_binding_append_only_conflict');
        END
    """,
    "trg_canon_entity_bindings_no_update": """
        CREATE TRIGGER IF NOT EXISTS main.trg_canon_entity_bindings_no_update
        BEFORE UPDATE ON canon_entity_account_bindings
        BEGIN
            SELECT RAISE(ABORT, 'canon_entity_binding_append_only_update');
        END
    """,
    "trg_canon_entity_bindings_no_delete": """
        CREATE TRIGGER IF NOT EXISTS main.trg_canon_entity_bindings_no_delete
        BEFORE DELETE ON canon_entity_account_bindings
        BEGIN
            SELECT RAISE(ABORT, 'canon_entity_binding_append_only_delete');
        END
    """,
}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _stable_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def _digest(*values: Any) -> str:
    payload = "\x1f".join(_stable_json(value) for value in values)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _authority_secret() -> bytes:
    raw = os.getenv(DECLARED_CANON_AUTHORITY_SECRET_ENV, "")
    encoded = raw.encode("utf-8")
    if not raw:
        raise CanonEntityBindingError(
            "declared_canon_authority_secret_not_configured"
        )
    if len(encoded) < DECLARED_CANON_AUTHORITY_SECRET_MIN_BYTES:
        raise CanonEntityBindingError(
            "declared_canon_authority_secret_invalid"
        )
    return encoded


def _authority_hmac(*values: Any) -> str:
    return hmac.new(
        _authority_secret(),
        _stable_json(values).encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def _configured_id(name: str) -> int:
    try:
        return int(os.getenv(name, "0") or 0)
    except (TypeError, ValueError):
        return 0


def _authorize(
    *,
    actor_user_id: int,
    guild_id: int,
    operation: str,
    authority_nonce: str,
    binding: Mapping[str, Any],
) -> _VerifiedAuthority:
    normalized_operation = str(operation or "").strip().casefold()
    if normalized_operation not in _OPERATIONS:
        raise CanonEntityBindingError("invalid_binding_authority_operation")
    actor = int(actor_user_id or 0)
    guild = int(guild_id or 0)
    configured_owner = _configured_id("BNL_OWNER_USER_ID")
    configured_guild = _configured_id("BNL_PRIMARY_GUILD_ID")
    if configured_owner <= 0:
        raise CanonEntityBindingError("owner_user_id_not_configured")
    if actor <= 0 or actor != configured_owner:
        raise CanonEntityBindingError("configured_owner_required")
    if configured_guild <= 0:
        raise CanonEntityBindingError("primary_guild_id_not_configured")
    if guild <= 0 or guild != configured_guild:
        raise CanonEntityBindingError("configured_primary_guild_required")
    nonce = str(authority_nonce or "").strip()
    if not _NONCE_RE.fullmatch(nonce):
        raise CanonEntityBindingError("invalid_binding_authority_nonce")
    request_fingerprint = "bindreq_" + _digest(
        BINDING_LIFECYCLE_VERSION,
        normalized_operation,
        guild,
        actor,
        binding,
    )[:48]
    operation_id = "bindop_" + _digest(
        BINDING_LIFECYCLE_VERSION,
        guild,
        actor,
        nonce,
    )[:32]
    # Force the configured signing boundary to be present before returning an
    # authority object, including for content-free previews.
    _authority_hmac(
        BINDING_AUTHORITY_RECEIPT_VERSION,
        normalized_operation,
        operation_id,
        request_fingerprint,
    )
    return _VerifiedAuthority(
        actor_user_id=actor,
        guild_id=guild,
        operation=normalized_operation,
        request_fingerprint=request_fingerprint,
        operation_id=operation_id,
        authority_actor="discord_user:%s" % actor,
    )


def _table_exists(conn: sqlite3.Connection) -> bool:
    return bool(
        conn.execute(
            "SELECT 1 FROM main.sqlite_master WHERE type='table' AND name=?",
            (BINDING_TABLE,),
        ).fetchone()
    )


def _columns(conn: sqlite3.Connection) -> tuple[str, ...]:
    if not _table_exists(conn):
        return ()
    return tuple(
        str(row[1] or "")
        for row in conn.execute(
            "PRAGMA main.table_info(%s)" % BINDING_TABLE
        ).fetchall()
    )


def _normalized_sql(value: Any) -> str:
    normalized = " ".join(str(value or "").strip().casefold().split())
    normalized = normalized.replace(" if not exists ", " ")
    normalized = normalized.replace("main.", "")
    return normalized.rstrip(";")


def _schema_sql(
    conn: sqlite3.Connection, *, object_type: str, name: str
) -> str:
    row = conn.execute(
        "SELECT sql FROM main.sqlite_master WHERE type=? AND name=?",
        (object_type, name),
    ).fetchone()
    return str(row[0] or "") if row else ""


def _require_schema(conn: sqlite3.Connection) -> None:
    if not _table_exists(conn):
        raise CanonEntityBindingError("canon_entity_binding_schema_unavailable")
    if _columns(conn) != _REVISION_COLUMNS:
        raise CanonEntityBindingError(
            "canon_entity_binding_schema_integrity_invalid"
        )
    if _normalized_sql(
        _schema_sql(conn, object_type="table", name=BINDING_TABLE)
    ) != _normalized_sql(_TABLE_DDL):
        raise CanonEntityBindingError(
            "canon_entity_binding_schema_integrity_invalid"
        )
    triggers = {
        str(row[0]): str(row[1] or "")
        for row in conn.execute(
            "SELECT name,sql FROM main.sqlite_master "
            "WHERE type='trigger' AND tbl_name=?",
            (BINDING_TABLE,),
        ).fetchall()
    }
    if set(triggers) != set(_TRIGGER_DDL):
        raise CanonEntityBindingError(
            "canon_entity_binding_schema_integrity_invalid"
        )
    for name, expected in _TRIGGER_DDL.items():
        if _normalized_sql(triggers.get(name, "")) != _normalized_sql(expected):
            raise CanonEntityBindingError(
                "canon_entity_binding_schema_integrity_invalid"
            )
    for name, expected in _INDEX_DDL.items():
        if _normalized_sql(
            _schema_sql(conn, object_type="index", name=name)
        ) != _normalized_sql(expected):
            raise CanonEntityBindingError(
                "canon_entity_binding_schema_integrity_invalid"
            )


@contextmanager
def _immediate_transaction(conn: sqlite3.Connection):
    owns = not conn.in_transaction
    if owns:
        conn.execute("BEGIN IMMEDIATE")
    try:
        yield
    except Exception:
        if owns:
            conn.rollback()
        raise
    else:
        if owns:
            conn.commit()


@contextmanager
def _read_snapshot(conn: sqlite3.Connection):
    owns = not conn.in_transaction
    before = int(conn.total_changes or 0)
    if owns:
        conn.execute("BEGIN")
    try:
        yield
        if int(conn.total_changes or 0) != before:
            raise CanonEntityBindingError("binding_read_mutated_state")
    except Exception:
        if owns:
            conn.rollback()
        raise
    else:
        if owns:
            conn.rollback()


def ensure_canon_entity_binding_schema(conn: sqlite3.Connection) -> None:
    """Create the exact append-only schema; never repair a damaged one."""

    with _immediate_transaction(conn):
        if _table_exists(conn):
            _require_schema(conn)
            return
        conn.execute(_TABLE_DDL)
        for statement in _INDEX_DDL.values():
            conn.execute(statement)
        for statement in _TRIGGER_DDL.values():
            conn.execute(statement)
        _require_schema(conn)


def _normalize_account_id(value: Any) -> str:
    account_id = str(value or "").strip()
    if not _DISCORD_ACCOUNT_RE.fullmatch(account_id):
        raise CanonEntityBindingError("discord_account_id_invalid")
    return account_id


def _normalize_entity_id(value: Any) -> str:
    entity_id = str(value or "").strip().casefold()
    if not _STABLE_ID_RE.fullmatch(entity_id) or entity_id not in _ENTITY_IDS:
        raise CanonEntityBindingError("canon_entity_id_unknown")
    return entity_id


def _normalize_reason(value: Any, *, required: bool) -> str:
    reason = " ".join(str(value or "").strip().split())
    if required and not reason:
        raise CanonEntityBindingError("binding_reason_required")
    if len(reason) > 500:
        raise CanonEntityBindingError("binding_reason_too_long")
    return reason


def _row_to_revision(row: Sequence[Any]) -> CanonEntityBindingRevision:
    values = dict(zip(_REVISION_COLUMNS, row))
    values["revision_number"] = int(values["revision_number"] or 0)
    values["guild_id"] = int(values["guild_id"] or 0)
    values["authority_verified"] = values["authority_verified"] is True or (
        isinstance(values["authority_verified"], int)
        and not isinstance(values["authority_verified"], bool)
        and values["authority_verified"] == 1
    )
    values["active"] = values["active"] is True or (
        isinstance(values["active"], int)
        and not isinstance(values["active"], bool)
        and values["active"] == 1
    )
    return CanonEntityBindingRevision(**values)


def _all_revisions(
    conn: sqlite3.Connection,
    *,
    guild_id: int,
    platform: str = "",
    account_id: str = "",
) -> tuple[CanonEntityBindingRevision, ...]:
    where = ["guild_id=?"]
    params: list[Any] = [int(guild_id or 0)]
    if platform:
        where.append("platform=?")
        params.append(str(platform))
    if account_id:
        where.append("account_id=?")
        params.append(str(account_id))
    rows = conn.execute(
        "SELECT %s FROM main.%s WHERE %s "
        "ORDER BY binding_id,revision_number"
        % (",".join(_REVISION_COLUMNS), BINDING_TABLE, " AND ".join(where)),
        tuple(params),
    ).fetchall()
    return tuple(_row_to_revision(row) for row in rows)


def _receipt_for_payload(payload: Mapping[str, Any]) -> str:
    signature = _authority_hmac(
        BINDING_AUTHORITY_RECEIPT_VERSION,
        "stored_binding_revision",
        tuple((column, payload[column]) for column in _REVISION_COLUMNS if column != "authority_receipt"),
    )
    return (
        "owner_command:%s:%s:%s"
        % (
            ENTITY_ACCOUNT_BINDING_CONTRACT_VERSION,
            BINDING_AUTHORITY_RECEIPT_VERSION,
            signature,
        )
    )


def _stored_revision_valid(revision: CanonEntityBindingRevision) -> bool:
    if not (
        revision.binding_revision_id
        and revision.binding_id
        and revision.revision_number > 0
        and revision.guild_id > 0
        and revision.platform == "discord"
        and _DISCORD_ACCOUNT_RE.fullmatch(revision.account_id)
        and revision.entity_id in _ENTITY_IDS
        and revision.operation in {"bind", "retire"}
        and revision.binding_version
        == ENTITY_ACCOUNT_BINDING_CONTRACT_VERSION
        and revision.lifecycle_version == BINDING_LIFECYCLE_VERSION
        and revision.authority_verified
        and re.fullmatch(r"discord_user:[1-9][0-9]{0,19}", revision.authority_actor)
        and revision.active == (revision.operation == "bind")
    ):
        return False
    payload = {
        column: getattr(revision, column)
        for column in _REVISION_COLUMNS
    }
    payload["authority_verified"] = int(revision.authority_verified)
    payload["active"] = int(revision.active)
    try:
        expected = _receipt_for_payload(payload)
    except CanonEntityBindingError:
        return False
    return hmac.compare_digest(expected, revision.authority_receipt)


def _current_revisions(
    revisions: Sequence[CanonEntityBindingRevision],
) -> tuple[CanonEntityBindingRevision, ...]:
    grouped: dict[str, list[CanonEntityBindingRevision]] = {}
    for revision in revisions:
        grouped.setdefault(revision.binding_id, []).append(revision)
    latest: list[CanonEntityBindingRevision] = []
    for binding_id, chain in grouped.items():
        ordered = sorted(chain, key=lambda item: item.revision_number)
        for index, revision in enumerate(ordered, start=1):
            previous = ordered[index - 2] if index > 1 else None
            if not _stored_revision_valid(revision):
                raise CanonEntityBindingError("binding_revision_authority_invalid")
            if (
                revision.binding_id != binding_id
                or revision.revision_number != index
                or revision.previous_revision_id
                != (previous.binding_revision_id if previous else "")
                or (
                    previous is not None
                    and (
                        revision.guild_id != previous.guild_id
                        or revision.platform != previous.platform
                        or revision.account_id != previous.account_id
                        or revision.entity_id != previous.entity_id
                    )
                )
            ):
                raise CanonEntityBindingError("binding_revision_chain_invalid")
        latest.append(ordered[-1])
    return tuple(sorted(latest, key=lambda item: item.binding_id))


def _existing_operation(
    conn: sqlite3.Connection,
    authority: _VerifiedAuthority,
) -> CanonEntityBindingRevision | None:
    row = conn.execute(
        "SELECT %s FROM main.%s WHERE guild_id=? AND operation_id=?"
        % (",".join(_REVISION_COLUMNS), BINDING_TABLE),
        (authority.guild_id, authority.operation_id),
    ).fetchone()
    if not row:
        return None
    revision = _row_to_revision(row)
    if (
        revision.authority_request_fingerprint
        != authority.request_fingerprint
        or not _stored_revision_valid(revision)
    ):
        raise CanonEntityBindingError("binding_operation_replay_conflict")
    return revision


def _insert_revision(
    conn: sqlite3.Connection,
    payload: Mapping[str, Any],
) -> CanonEntityBindingRevision:
    stored = dict(payload)
    stored["authority_receipt"] = _receipt_for_payload(stored)
    conn.execute(
        "INSERT INTO main.%s (%s) VALUES (%s)"
        % (
            BINDING_TABLE,
            ",".join(_REVISION_COLUMNS),
            ",".join("?" for _ in _REVISION_COLUMNS),
        ),
        tuple(stored[column] for column in _REVISION_COLUMNS),
    )
    return _row_to_revision(tuple(stored[column] for column in _REVISION_COLUMNS))


def bind_discord_account(
    conn: sqlite3.Connection,
    *,
    actor_user_id: int,
    authority_nonce: str,
    guild_id: int,
    account_id: Any,
    entity_id: Any,
    reason: Any,
) -> CanonEntityBindingMutation:
    """Bind one Discord account without merging or aliasing canon entities."""

    normalized_account = _normalize_account_id(account_id)
    normalized_entity = _normalize_entity_id(entity_id)
    normalized_reason = _normalize_reason(reason, required=True)
    authority = _authorize(
        actor_user_id=actor_user_id,
        guild_id=guild_id,
        operation="bind",
        authority_nonce=authority_nonce,
        binding={
            "platform": "discord",
            "account_id": normalized_account,
            "entity_id": normalized_entity,
            "reason": normalized_reason,
        },
    )
    ensure_canon_entity_binding_schema(conn)
    with _immediate_transaction(conn):
        existing = _existing_operation(conn, authority)
        if existing is not None:
            return CanonEntityBindingMutation(authority.operation_id, existing)
        revisions = _all_revisions(
            conn,
            guild_id=authority.guild_id,
            platform="discord",
            account_id=normalized_account,
        )
        current = _current_revisions(revisions)
        active = tuple(item for item in current if item.active)
        if active:
            if any(item.entity_id != normalized_entity for item in active):
                raise CanonEntityBindingError("account_binding_collision")
            raise CanonEntityBindingError("account_binding_already_active")
        same_chain = next(
            (item for item in current if item.entity_id == normalized_entity),
            None,
        )
        binding_id = (
            same_chain.binding_id
            if same_chain is not None
            else "binding_"
            + _digest(
                BINDING_LIFECYCLE_VERSION,
                authority.guild_id,
                "discord",
                normalized_account,
                normalized_entity,
            )[:40]
        )
        revision_number = (
            same_chain.revision_number + 1 if same_chain is not None else 1
        )
        previous_revision_id = (
            same_chain.binding_revision_id if same_chain is not None else ""
        )
        created_at = _utc_now()
        revision_id = "bindingrev_" + _digest(
            BINDING_LIFECYCLE_VERSION,
            binding_id,
            revision_number,
            authority.operation_id,
            authority.request_fingerprint,
            created_at,
        )[:48]
        payload = {
            "binding_revision_id": revision_id,
            "binding_id": binding_id,
            "revision_number": revision_number,
            "guild_id": authority.guild_id,
            "platform": "discord",
            "account_id": normalized_account,
            "entity_id": normalized_entity,
            "operation": "bind",
            "operation_id": authority.operation_id,
            "operation_reason": normalized_reason,
            "previous_revision_id": previous_revision_id,
            "authority_request_fingerprint": authority.request_fingerprint,
            "authority_actor": authority.authority_actor,
            "authority_receipt": "",
            "binding_version": ENTITY_ACCOUNT_BINDING_CONTRACT_VERSION,
            "authority_verified": 1,
            "active": 1,
            "lifecycle_version": BINDING_LIFECYCLE_VERSION,
            "created_at": created_at,
        }
        revision = _insert_revision(conn, payload)
        return CanonEntityBindingMutation(authority.operation_id, revision)


def retire_discord_account_binding(
    conn: sqlite3.Connection,
    *,
    actor_user_id: int,
    authority_nonce: str,
    guild_id: int,
    binding_id: Any,
    expected_revision_id: Any,
    reason: Any,
) -> CanonEntityBindingMutation:
    """Append one retirement revision; the historical binding stays intact."""

    normalized_binding_id = str(binding_id or "").strip()
    normalized_expected = str(expected_revision_id or "").strip()
    if not normalized_binding_id.startswith("binding_"):
        raise CanonEntityBindingError("binding_id_invalid")
    if not normalized_expected.startswith("bindingrev_"):
        raise CanonEntityBindingError("binding_expected_revision_invalid")
    normalized_reason = _normalize_reason(reason, required=True)
    authority = _authorize(
        actor_user_id=actor_user_id,
        guild_id=guild_id,
        operation="retire",
        authority_nonce=authority_nonce,
        binding={
            "binding_id": normalized_binding_id,
            "expected_revision_id": normalized_expected,
            "reason": normalized_reason,
        },
    )
    ensure_canon_entity_binding_schema(conn)
    with _immediate_transaction(conn):
        existing = _existing_operation(conn, authority)
        if existing is not None:
            return CanonEntityBindingMutation(authority.operation_id, existing)
        revisions = _all_revisions(conn, guild_id=authority.guild_id)
        latest = next(
            (
                item
                for item in _current_revisions(revisions)
                if item.binding_id == normalized_binding_id
            ),
            None,
        )
        if latest is None:
            raise CanonEntityBindingError("binding_not_found")
        if latest.binding_revision_id != normalized_expected:
            raise CanonEntityBindingError("binding_revision_conflict")
        if not latest.active:
            raise CanonEntityBindingError("binding_already_retired")
        created_at = _utc_now()
        revision_number = latest.revision_number + 1
        revision_id = "bindingrev_" + _digest(
            BINDING_LIFECYCLE_VERSION,
            latest.binding_id,
            revision_number,
            authority.operation_id,
            authority.request_fingerprint,
            created_at,
        )[:48]
        payload = {
            "binding_revision_id": revision_id,
            "binding_id": latest.binding_id,
            "revision_number": revision_number,
            "guild_id": latest.guild_id,
            "platform": latest.platform,
            "account_id": latest.account_id,
            "entity_id": latest.entity_id,
            "operation": "retire",
            "operation_id": authority.operation_id,
            "operation_reason": normalized_reason,
            "previous_revision_id": latest.binding_revision_id,
            "authority_request_fingerprint": authority.request_fingerprint,
            "authority_actor": authority.authority_actor,
            "authority_receipt": "",
            "binding_version": ENTITY_ACCOUNT_BINDING_CONTRACT_VERSION,
            "authority_verified": 1,
            "active": 0,
            "lifecycle_version": BINDING_LIFECYCLE_VERSION,
            "created_at": created_at,
        }
        revision = _insert_revision(conn, payload)
        return CanonEntityBindingMutation(authority.operation_id, revision)


def read_current_entity_account_bindings(
    conn: sqlite3.Connection,
    *,
    guild_id: int,
    platform: str,
    account_id: Any,
) -> CanonEntityBindingRead:
    """Read HMAC-verified latest revisions for one same-platform account."""

    if not _table_exists(conn):
        return CanonEntityBindingRead("binding_table_unavailable")
    _require_schema(conn)
    normalized_platform = str(platform or "").strip().casefold()
    if normalized_platform != "discord":
        return CanonEntityBindingRead("binding_platform_unsupported")
    try:
        normalized_account = _normalize_account_id(account_id)
    except CanonEntityBindingError:
        return CanonEntityBindingRead("binding_account_invalid")
    with _read_snapshot(conn):
        revisions = _all_revisions(
            conn,
            guild_id=int(guild_id or 0),
            platform=normalized_platform,
            account_id=normalized_account,
        )
        if not revisions:
            return CanonEntityBindingRead("no_binding")
        try:
            latest = _current_revisions(revisions)
        except CanonEntityBindingError as exc:
            return CanonEntityBindingRead(exc.code)
        active = tuple(item for item in latest if item.active)
        if not active:
            return CanonEntityBindingRead(
                "retired_account_binding",
                revisions=latest,
            )
        return CanonEntityBindingRead(
            "active",
            bindings=tuple(item.as_contract_binding() for item in active),
            revisions=active,
        )


def read_current_guild_entity_account_bindings(
    conn: sqlite3.Connection,
    *,
    guild_id: int,
) -> CanonEntityBindingRead:
    """Read every HMAC-verified latest binding in one configured guild."""

    if not _table_exists(conn):
        return CanonEntityBindingRead("binding_table_unavailable")
    _require_schema(conn)
    with _read_snapshot(conn):
        revisions = _all_revisions(conn, guild_id=int(guild_id or 0))
        if not revisions:
            return CanonEntityBindingRead("no_binding")
        try:
            latest = _current_revisions(revisions)
        except CanonEntityBindingError as exc:
            return CanonEntityBindingRead(exc.code)
        active = tuple(item for item in latest if item.active)
        return CanonEntityBindingRead(
            "active" if active else "retired_account_binding",
            bindings=tuple(item.as_contract_binding() for item in active),
            revisions=latest,
        )


def preview_canon_entity_bindings(
    conn: sqlite3.Connection,
    *,
    actor_user_id: int,
    authority_nonce: str,
    guild_id: int,
) -> CanonEntityBindingPreview:
    """Return identifiers and aggregate states, never account IDs or labels."""

    authority = _authorize(
        actor_user_id=actor_user_id,
        guild_id=guild_id,
        operation="preview",
        authority_nonce=authority_nonce,
        binding={"scope": "same_guild_discord_bindings"},
    )
    ensure_canon_entity_binding_schema(conn)
    with _read_snapshot(conn):
        before = int(conn.total_changes or 0)
        current = _current_revisions(
            _all_revisions(conn, guild_id=authority.guild_id)
        )
        active = tuple(item for item in current if item.active)
        retired = tuple(item for item in current if not item.active)
        entity_counts: dict[str, int] = {}
        for item in active:
            entity_counts[item.entity_id] = entity_counts.get(item.entity_id, 0) + 1
        return CanonEntityBindingPreview(
            lifecycle_version=BINDING_LIFECYCLE_VERSION,
            guild_id=authority.guild_id,
            active_count=len(active),
            retired_count=len(retired),
            entity_counts=dict(sorted(entity_counts.items())),
            binding_refs=tuple(
                (
                    item.binding_id,
                    item.binding_revision_id,
                    "active" if item.active else "retired",
                )
                for item in current
            ),
            mutation_count=int(conn.total_changes or 0) - before,
        )
