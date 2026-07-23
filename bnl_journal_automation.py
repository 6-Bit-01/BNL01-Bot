from __future__ import annotations

import hashlib
import json
import logging
import re
import sqlite3
from dataclasses import asdict, dataclass
from datetime import date, datetime, time as datetime_time, timedelta, timezone
from typing import Any, Callable, Optional

import pytz

from bnl_journal import (
    JOURNAL_SITE_REQUEST_BODY_MAX_BYTES,
    SCHEDULED_PREPARED_STATE,
    JournalResult,
    approve_draft,
    build_source_packet_between,
    canonical_payload_hash,
    deliver_approved,
    generate_and_store_packet_draft,
    journal_broadcast_memory_provenance_is_eligible,
    journal_topic_counts,
    utc_now_iso,
)
from bnl_journal_source_store import journal_release_privacy_fence

PACIFIC = pytz.timezone("US/Pacific")
DAILY_READY_HOUR = 19
DAILY_READY_MINUTE = 0
WEEKLY_READY_WEEKDAY = 0  # Monday, covering the prior Monday-Sunday week.
WEEKLY_READY_HOUR = 19
MIN_DAILY_SOURCE_COUNT = 5
MIN_WEEKLY_ACTIVE_DAYS = 1
LEASE_MINUTES = 30
DELIVERY_LEASE_MINUTES = 2
RETRY_MINUTES = 60
TERMINAL_RUN_STATES = {"published", "quiet", "incomplete"}
DELIVERY_PREFLIGHT_INVALIDATION_REASONS = frozenset({
    "prepared_revision_missing",
    "prepared_payload_integrity_failed",
    "source_packet_hash_mismatch",
    "privacy_eligibility_changed",
    "privacy_source_ineligible",
    "privacy_memory_ineligible",
})


@dataclass
class AutomationResult:
    ok: bool
    cadence: str
    status: str
    reason: str = ""
    entry_id: str = ""
    revision: int = 0
    source_window_start: str = ""
    source_window_end: str = ""
    aggregate_counts: Optional[dict[str, Any]] = None
    http_status: int = 0
    idempotent: bool = False


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _parse_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _utc_iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _pacific_journal_cutoff(day: date) -> datetime:
    return PACIFIC.localize(
        datetime.combine(day, datetime_time(DAILY_READY_HOUR, DAILY_READY_MINUTE))
    )


def _daily_period_for_day(day: date) -> tuple[str, str, str]:
    start_local = _pacific_journal_cutoff(day)
    end_local = _pacific_journal_cutoff(day + timedelta(days=1))
    return _utc_iso(start_local), _utc_iso(end_local), day.isoformat()


def daily_period(now_utc: Optional[datetime] = None) -> tuple[str, str, str]:
    now = (now_utc or datetime.now(timezone.utc)).astimezone(PACIFIC)
    start_day = now.date() - timedelta(days=1)
    if (now.hour, now.minute) < (DAILY_READY_HOUR, DAILY_READY_MINUTE):
        start_day -= timedelta(days=1)
    return _daily_period_for_day(start_day)


def _weekly_period_for_monday(monday: date) -> tuple[str, str, str]:
    start_local = _pacific_journal_cutoff(monday)
    end_local = _pacific_journal_cutoff(monday + timedelta(days=7))
    return _utc_iso(start_local), _utc_iso(end_local), monday.isoformat()


def weekly_period(now_utc: Optional[datetime] = None) -> tuple[str, str, str]:
    now = (now_utc or datetime.now(timezone.utc)).astimezone(PACIFIC)
    monday = now.date() - timedelta(days=now.weekday())
    if now.weekday() == WEEKLY_READY_WEEKDAY and now.hour < WEEKLY_READY_HOUR:
        monday -= timedelta(days=7)
    return _weekly_period_for_monday(monday - timedelta(days=7))


def daily_due(now_utc: Optional[datetime] = None) -> bool:
    local = (now_utc or datetime.now(timezone.utc)).astimezone(PACIFIC)
    return (local.hour, local.minute) >= (DAILY_READY_HOUR, DAILY_READY_MINUTE)


def weekly_due(now_utc: Optional[datetime] = None) -> bool:
    local = (now_utc or datetime.now(timezone.utc)).astimezone(PACIFIC)
    return local.weekday() == WEEKLY_READY_WEEKDAY and local.hour >= WEEKLY_READY_HOUR


def next_schedule_times(now_utc: Optional[datetime] = None) -> tuple[str, str]:
    local = (now_utc or datetime.now(timezone.utc)).astimezone(PACIFIC)
    daily_day = local.date()
    daily_local = PACIFIC.localize(
        datetime.combine(daily_day, datetime_time(DAILY_READY_HOUR, DAILY_READY_MINUTE))
    )
    if daily_local <= local:
        daily_local = PACIFIC.localize(
            datetime.combine(daily_day + timedelta(days=1), datetime_time(DAILY_READY_HOUR, DAILY_READY_MINUTE))
        )
    days_until_monday = (WEEKLY_READY_WEEKDAY - local.weekday()) % 7
    weekly_day = local.date() + timedelta(days=days_until_monday)
    weekly_local = PACIFIC.localize(
        datetime.combine(weekly_day, datetime_time(WEEKLY_READY_HOUR, 0))
    )
    if weekly_local <= local:
        weekly_local = PACIFIC.localize(
            datetime.combine(weekly_day + timedelta(days=7), datetime_time(WEEKLY_READY_HOUR, 0))
        )
    return _utc_iso(daily_local), _utc_iso(weekly_local)


def ensure_schema(db_path: str) -> None:
    with sqlite3.connect(db_path, timeout=30) as conn:
        # Serialize the legacy-column snapshot with every conditional ALTER.
        # Journal entrypoints can initialize concurrently during startup.
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("""CREATE TABLE IF NOT EXISTS bnl_journal_observations (
            observation_id TEXT PRIMARY KEY,
            guild_id INTEGER NOT NULL,
            observation_date TEXT NOT NULL,
            source_window_start TEXT NOT NULL,
            source_window_end TEXT NOT NULL,
            aggregate_counts_json TEXT NOT NULL,
            topic_counts_json TEXT NOT NULL,
            subject_counts_json TEXT NOT NULL,
            representative_sources_json TEXT NOT NULL,
            lifecycle_state TEXT NOT NULL,
            journal_entry_id TEXT,
            journal_revision INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(guild_id, observation_date))""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_bnl_journal_observation_window ON bnl_journal_observations(guild_id,source_window_start,source_window_end)")
        conn.execute("""CREATE TABLE IF NOT EXISTS bnl_journal_automation_runs (
            run_id TEXT PRIMARY KEY,
            guild_id INTEGER NOT NULL,
            cadence TEXT NOT NULL,
            source_window_start TEXT NOT NULL,
            source_window_end TEXT NOT NULL,
            lifecycle_state TEXT NOT NULL,
            reason TEXT,
            journal_entry_id TEXT,
            journal_revision INTEGER NOT NULL DEFAULT 0,
            aggregate_counts_json TEXT NOT NULL DEFAULT '{}',
            attempt_count INTEGER NOT NULL DEFAULT 0,
            lease_expires_at TEXT,
            frozen_packet_json TEXT,
            frozen_packet_hash TEXT,
            packet_frozen_at TEXT,
            preparation_epoch INTEGER NOT NULL DEFAULT 0,
            prepared_payload_hash TEXT,
            prepared_payload_bytes INTEGER NOT NULL DEFAULT 0,
            prepared_at TEXT,
            delivery_epoch INTEGER NOT NULL DEFAULT 0,
            delivery_lease_expires_at TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(guild_id,cadence,source_window_start,source_window_end))""")
        run_columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(bnl_journal_automation_runs)")}
        run_migrations = {
            "frozen_packet_json": "TEXT",
            "frozen_packet_hash": "TEXT",
            "packet_frozen_at": "TEXT",
            "preparation_epoch": "INTEGER NOT NULL DEFAULT 0",
            "prepared_payload_hash": "TEXT",
            "prepared_payload_bytes": "INTEGER NOT NULL DEFAULT 0",
            "prepared_at": "TEXT",
            "delivery_epoch": "INTEGER NOT NULL DEFAULT 0",
            "delivery_lease_expires_at": "TEXT",
        }
        for column, declaration in run_migrations.items():
            if column not in run_columns:
                conn.execute(f"ALTER TABLE bnl_journal_automation_runs ADD COLUMN {column} {declaration}")
        conn.execute(
            """CREATE TRIGGER IF NOT EXISTS trg_bnl_journal_runs_guard_legacy_running
               BEFORE UPDATE OF lifecycle_state ON bnl_journal_automation_runs
               WHEN NEW.lifecycle_state='running'
                AND (
                    OLD.lifecycle_state IN ('preparing','prepared','delivering')
                    OR COALESCE(OLD.preparation_epoch,0) > 0
                    OR COALESCE(OLD.delivery_epoch,0) > 0
                    OR OLD.frozen_packet_json IS NOT NULL
                    OR OLD.frozen_packet_hash IS NOT NULL
                    OR OLD.prepared_payload_hash IS NOT NULL
                )
               BEGIN
                   SELECT RAISE(ABORT,'journal_prepared_release_downgrade_guard');
               END"""
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_bnl_journal_runs_latest ON bnl_journal_automation_runs(guild_id,updated_at DESC)")
        conn.execute("""CREATE TABLE IF NOT EXISTS bnl_journal_automation_state (
            guild_id INTEGER PRIMARY KEY,
            last_checked_at TEXT,
            last_status TEXT,
            last_reason TEXT,
            last_cadence TEXT,
            last_entry_id TEXT,
            last_revision INTEGER NOT NULL DEFAULT 0,
            last_source_window_start TEXT,
            last_source_window_end TEXT,
            next_retry_at TEXT,
            memory_excluded_entry_ids_json TEXT NOT NULL DEFAULT '[]',
            memory_exclusions_confirmed_at TEXT,
            updated_at TEXT NOT NULL)""")
        state_columns = {
            str(row[1]) for row in conn.execute("PRAGMA table_info(bnl_journal_automation_state)")
        }
        state_migrations = {
            "memory_excluded_entry_ids_json": "TEXT NOT NULL DEFAULT '[]'",
            "memory_exclusions_confirmed_at": "TEXT",
        }
        for column, declaration in state_migrations.items():
            if column not in state_columns:
                conn.execute(
                    f"ALTER TABLE bnl_journal_automation_state ADD COLUMN {column} {declaration}"
                )
        if conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='bnl_journal_entries'"
        ).fetchone():
            # Hide already-staged occurrence payloads from pre-upgrade delivery
            # code before a rollback can observe the legacy lifecycle values.
            conn.execute(
                "UPDATE bnl_journal_entries SET lifecycle_state='prepared_exact' "
                "WHERE lifecycle_state IN ('approved_pending_delivery','delivery_failed') "
                "AND EXISTS ("
                "SELECT 1 FROM bnl_journal_automation_runs r "
                "WHERE r.guild_id=bnl_journal_entries.guild_id "
                "AND r.journal_entry_id=bnl_journal_entries.entry_id "
                "AND r.journal_revision=bnl_journal_entries.revision "
                "AND r.prepared_payload_hash IS NOT NULL"
                ")"
            )
            if conn.execute(
                "SELECT 1 FROM sqlite_master "
                "WHERE type='table' AND name='bnl_journal_private_metadata'"
            ).fetchone():
                conn.execute(
                    "UPDATE bnl_journal_private_metadata SET lifecycle_state='prepared_exact' "
                    "WHERE lifecycle_state IN ('approved_pending_delivery','delivery_failed') "
                    "AND EXISTS ("
                    "SELECT 1 FROM bnl_journal_automation_runs r "
                    "WHERE r.guild_id=bnl_journal_private_metadata.guild_id "
                    "AND r.journal_entry_id=bnl_journal_private_metadata.entry_id "
                    "AND r.journal_revision=bnl_journal_private_metadata.revision "
                    "AND r.prepared_payload_hash IS NOT NULL"
                    ")"
                )


def _load_journal_memory_exclusions_on_connection(
    conn: sqlite3.Connection,
    guild_id: int,
) -> tuple[set[str], bool]:
    row = conn.execute(
        "SELECT memory_excluded_entry_ids_json,memory_exclusions_confirmed_at "
        "FROM bnl_journal_automation_state WHERE guild_id=?",
        (int(guild_id),),
    ).fetchone()
    if not row or not str(row[1] or ""):
        return set(), False
    try:
        values = json.loads(str(row[0] or ""))
    except (TypeError, json.JSONDecodeError):
        return set(), False
    if not isinstance(values, list) or any(not isinstance(value, str) for value in values):
        return set(), False
    resolved = {
        value.strip()
        for value in values
        if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,159}", value.strip())
    }
    if len(resolved) != len(values):
        return set(), False
    return resolved, True


def store_journal_memory_exclusions(
    db_path: str,
    guild_id: int,
    entry_ids: set[str] | list[str],
) -> set[str]:
    """Persist one confirmed website memory-eligibility snapshot."""
    ensure_schema(db_path)
    resolved = {
        str(entry_id).strip()
        for entry_id in entry_ids
        if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,159}", str(entry_id).strip())
    }
    now = utc_now_iso()
    with journal_release_privacy_fence(db_path):
        with sqlite3.connect(db_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "INSERT INTO bnl_journal_automation_state("
                "guild_id,memory_excluded_entry_ids_json,memory_exclusions_confirmed_at,updated_at"
                ") VALUES(?,?,?,?) ON CONFLICT(guild_id) DO UPDATE SET "
                "memory_excluded_entry_ids_json=excluded.memory_excluded_entry_ids_json,"
                "memory_exclusions_confirmed_at=excluded.memory_exclusions_confirmed_at,"
                "updated_at=excluded.updated_at",
                (int(guild_id), _json(sorted(resolved)), now, now),
            )
            conn.commit()
    return resolved


def load_journal_memory_exclusions(
    db_path: str,
    guild_id: int,
) -> tuple[set[str], bool]:
    """Return the last confirmed exclusion set; never infer confirmed-empty."""
    ensure_schema(db_path)
    with sqlite3.connect(db_path) as conn:
        return _load_journal_memory_exclusions_on_connection(conn, guild_id)


def _run_id(guild_id: int, cadence: str, start: str, end: str) -> str:
    return "jrun_" + hashlib.sha256(f"{guild_id}|{cadence}|{start}|{end}".encode("utf-8")).hexdigest()[:20]


def _entry_id(guild_id: int, cadence: str, label: str) -> str:
    suffix = hashlib.sha256(str(guild_id).encode("utf-8")).hexdigest()[:8]
    return f"journal_{cadence}_{label}_{suffix}"


def _update_state(conn: sqlite3.Connection, guild_id: int, result: AutomationResult, *, next_retry_at: str = "") -> None:
    now = utc_now_iso()
    conn.execute("""INSERT INTO bnl_journal_automation_state(
        guild_id,last_checked_at,last_status,last_reason,last_cadence,last_entry_id,last_revision,
        last_source_window_start,last_source_window_end,next_retry_at,updated_at
    ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
    ON CONFLICT(guild_id) DO UPDATE SET
        last_checked_at=excluded.last_checked_at,last_status=excluded.last_status,last_reason=excluded.last_reason,
        last_cadence=excluded.last_cadence,last_entry_id=excluded.last_entry_id,last_revision=excluded.last_revision,
        last_source_window_start=excluded.last_source_window_start,last_source_window_end=excluded.last_source_window_end,
        next_retry_at=excluded.next_retry_at,updated_at=excluded.updated_at""", (
        guild_id, now, result.status, result.reason, result.cadence, result.entry_id, int(result.revision or 0),
        result.source_window_start, result.source_window_end, next_retry_at or None, now,
    ))


def _reconcile_daily_observation(
    conn: sqlite3.Connection,
    guild_id: int,
    result: AutomationResult,
) -> None:
    """Commit the Daily run and its Weekly prerequisite in one transaction."""
    if result.cadence != "daily" or not result.source_window_start or not result.source_window_end:
        return
    conn.execute(
        "UPDATE bnl_journal_observations SET lifecycle_state=?,journal_entry_id=?,"
        "journal_revision=?,updated_at=? WHERE guild_id=? AND source_window_start=? AND source_window_end=?",
        (
            result.status,
            result.entry_id or None,
            int(result.revision or 0),
            utc_now_iso(),
            guild_id,
            result.source_window_start,
            result.source_window_end,
        ),
    )


def _claim_preparation(
    db_path: str,
    guild_id: int,
    cadence: str,
    start: str,
    end: str,
    *,
    force: bool = False,
) -> tuple[str, str, int, dict[str, Any]]:
    ensure_schema(db_path)
    run_id = _run_id(guild_id, cadence, start, end)
    now = datetime.now(timezone.utc)
    lease = _utc_iso(now + timedelta(minutes=LEASE_MINUTES))
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute("SELECT * FROM bnl_journal_automation_runs WHERE run_id=?", (run_id,)).fetchone()
        if row:
            current = dict(row)
            if current["lifecycle_state"] in TERMINAL_RUN_STATES:
                conn.commit()
                return "complete", run_id, int(current.get("preparation_epoch") or 0), current
            if (
                current["lifecycle_state"] in {"prepared", "delivery_failed", "delivering"}
                and current.get("journal_entry_id")
                and int(current.get("journal_revision") or 0) > 0
                and current.get("prepared_payload_hash")
                and current.get("frozen_packet_hash")
            ):
                conn.commit()
                return "prepared", run_id, int(current.get("preparation_epoch") or 0), current
            if current["lifecycle_state"] in {"held", "delivery_failed", "deferred"} and not force:
                updated_at = str(current.get("updated_at") or "")
                if updated_at:
                    retry_at = _parse_utc(updated_at) + timedelta(minutes=RETRY_MINUTES)
                    if retry_at > now:
                        current["retry_at"] = _utc_iso(retry_at)
                        conn.commit()
                        return "backoff", run_id, int(current.get("preparation_epoch") or 0), current
            lease_expires = current.get("lease_expires_at") or ""
            # A manual Run Now may bypass retry backoff, but it must never
            # steal a live lease from the scheduler or another operator.
            if (
                current["lifecycle_state"] in {"running", "preparing"}
                and lease_expires
                and _parse_utc(lease_expires) > now
            ):
                conn.commit()
                return "busy", run_id, int(current.get("preparation_epoch") or 0), current
            epoch = int(current.get("preparation_epoch") or 0) + 1
            conn.execute("""UPDATE bnl_journal_automation_runs
                SET lifecycle_state='preparing',reason='',attempt_count=attempt_count+1,
                    preparation_epoch=?,lease_expires_at=?,delivery_lease_expires_at=NULL,updated_at=?
                WHERE run_id=?""", (epoch, lease, utc_now_iso(), run_id))
        else:
            now_iso = utc_now_iso()
            epoch = 1
            conn.execute("""INSERT INTO bnl_journal_automation_runs(
                run_id,guild_id,cadence,source_window_start,source_window_end,lifecycle_state,reason,
                aggregate_counts_json,attempt_count,lease_expires_at,preparation_epoch,created_at,updated_at
            ) VALUES(?,?,?,?,?,'preparing','', '{}',1,?,?,?,?)""", (
                run_id, guild_id, cadence, start, end, lease, epoch, now_iso, now_iso,
            ))
        conn.commit()
    return "claimed", run_id, epoch, {}


def _finish_preparation(
    db_path: str,
    run_id: str,
    preparation_epoch: int,
    result: AutomationResult,
) -> AutomationResult:
    retry_at = ""
    if result.status in {"held", "delivery_failed", "deferred"}:
        retry_at = _utc_iso(datetime.now(timezone.utc) + timedelta(minutes=RETRY_MINUTES))
    with sqlite3.connect(db_path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            "SELECT guild_id FROM bnl_journal_automation_runs WHERE run_id=? AND preparation_epoch=? AND lifecycle_state='preparing'",
            (run_id, int(preparation_epoch)),
        ).fetchone()
        if not row:
            conn.rollback()
            return AutomationResult(
                False,
                result.cadence,
                "busy",
                "preparation_epoch_superseded",
                source_window_start=result.source_window_start,
                source_window_end=result.source_window_end,
                aggregate_counts=result.aggregate_counts,
            )
        updated = conn.execute("""UPDATE bnl_journal_automation_runs SET lifecycle_state=?,reason=?,journal_entry_id=?,
                journal_revision=?,aggregate_counts_json=?,lease_expires_at=NULL,updated_at=?
                WHERE run_id=? AND preparation_epoch=? AND lifecycle_state='preparing'""", (
                result.status, result.reason, result.entry_id or None, int(result.revision or 0),
                _json(result.aggregate_counts or {}), utc_now_iso(), run_id, int(preparation_epoch),
            )).rowcount
        if updated != 1:
            conn.rollback()
            return AutomationResult(False, result.cadence, "busy", "preparation_epoch_superseded")
        _reconcile_daily_observation(conn, int(row[0]), result)
        _update_state(conn, int(row[0]), result, next_retry_at=retry_at)
        conn.commit()
    return result


def _result_from_existing(cadence: str, row: dict[str, Any], *, claim: str = "complete") -> AutomationResult:
    counts = json.loads(row.get("aggregate_counts_json") or "{}")
    status = str(row.get("lifecycle_state") or "unknown")
    reason = str(row.get("reason") or "already_processed")
    if claim == "busy":
        status = "busy"
        reason = "automation_run_in_progress"
    elif claim == "backoff":
        status = "backoff"
        reason = "retry_after_" + str(row.get("retry_at") or "later")
    elif claim == "prepared":
        status = "prepared"
        reason = "prepared_waiting_release"
    elif claim == "controls_unconfirmed":
        status = "prepared"
        reason = "journal_memory_exclusions_unconfirmed"
    return AutomationResult(
        status in {"published", "prepared"},
        cadence,
        status,
        reason,
        str(row.get("journal_entry_id") or ""),
        int(row.get("journal_revision") or 0),
        str(row.get("source_window_start") or ""),
        str(row.get("source_window_end") or ""),
        counts,
    )


def _fresh_event_sequences(refs: set[str]) -> set[int]:
    return {
        int(match.group(1))
        for ref in refs
        for match in [re.fullmatch(r"fresh:(\d+)", ref)]
        if match
    }


def _fresh_sequences_are_eligible(
    conn: sqlite3.Connection,
    guild_id: int,
    sequences: set[int],
) -> bool:
    if not sequences:
        return True
    if not conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='bnl_journal_source_events'"
    ).fetchone():
        return False
    placeholders = ",".join("?" for _ in sequences)
    eligible = {
        int(row[0])
        for row in conn.execute(
            f"SELECT event_seq FROM bnl_journal_source_events "
            f"WHERE guild_id=? AND public_usable=1 AND event_seq IN ({placeholders})",
            (guild_id, *sorted(sequences)),
        ).fetchall()
    }
    return eligible == sequences


def _frozen_packet_invalidation_reason(
    conn: sqlite3.Connection,
    guild_id: int,
    packet: dict[str, Any],
) -> str:
    refs = {
        str(source.get("refId") or "")
        for source in packet.get("privateSources", [])
        if isinstance(source, dict) and source.get("refId")
    }
    if not _fresh_sequences_are_eligible(conn, guild_id, _fresh_event_sequences(refs)):
        return "privacy_source_ineligible"
    provenance = packet.get("privateContextLaneProvenance")
    provenance = provenance if isinstance(provenance, dict) else {}
    memory_rows = provenance.get("establishedBroadcastMemory")
    memory_rows = memory_rows if isinstance(memory_rows, list) else []
    if not journal_broadcast_memory_provenance_is_eligible(
        conn,
        guild_id,
        memory_rows,
        str(packet.get("sourceWindowEnd") or ""),
    ):
        return "privacy_memory_ineligible"
    return ""


def _freeze_or_load_packet(
    db_path: str,
    guild_id: int,
    run_id: str,
    preparation_epoch: int,
    builder: Callable[[], Optional[dict[str, Any]]],
) -> tuple[Optional[dict[str, Any]], str, str]:
    """Persist one complete deterministic source packet before any model call."""
    ensure_schema(db_path)

    def owner_reason(current: Optional[sqlite3.Row | tuple[Any, ...]]) -> str:
        if (
            not current
            or str(current[0]) != "preparing"
            or int(current[1] or 0) != int(preparation_epoch)
        ):
            return "preparation_epoch_superseded"
        lease_raw = str(current[2] or "")
        try:
            live = bool(lease_raw) and _parse_utc(lease_raw) > datetime.now(timezone.utc)
        except ValueError:
            live = False
        return "" if live else "preparation_epoch_expired"

    def stored_packet_result(
        conn: sqlite3.Connection,
        current: sqlite3.Row | tuple[Any, ...],
    ) -> Optional[tuple[Optional[dict[str, Any]], str, str]]:
        if not current[3]:
            return None
        stored_raw = str(current[3])
        stored_hash = hashlib.sha256(stored_raw.encode("utf-8")).hexdigest()
        if stored_hash != str(current[4] or ""):
            return None, "", "frozen_packet_hash_mismatch"
        try:
            parsed = json.loads(stored_raw)
        except (TypeError, json.JSONDecodeError):
            return None, "", "frozen_packet_invalid"
        if not isinstance(parsed, dict):
            return None, "", "frozen_packet_invalid"
        stored = dict(parsed)
        if not stored.get("sourceArchiveAvailable", False) or not stored.get("coverageComplete", True):
            invalidation = "source_packet_ineligible"
        else:
            invalidation = _frozen_packet_invalidation_reason(conn, guild_id, stored)
        if invalidation:
            conn.execute(
                "UPDATE bnl_journal_automation_runs SET frozen_packet_json=NULL,frozen_packet_hash=NULL,"
                "packet_frozen_at=NULL,updated_at=? WHERE run_id=? AND preparation_epoch=? "
                "AND lifecycle_state='preparing'",
                (utc_now_iso(), run_id, int(preparation_epoch)),
            )
            return None, "", invalidation
        return stored, stored_hash, ""

    # Fast recovery still takes the write lock: a privacy deletion and a
    # preparation owner can never pass one another between validation/return.
    with sqlite3.connect(db_path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        current = conn.execute(
            "SELECT lifecycle_state,preparation_epoch,lease_expires_at,frozen_packet_json,frozen_packet_hash "
            "FROM bnl_journal_automation_runs WHERE run_id=?",
            (run_id,),
        ).fetchone()
        reason = owner_reason(current)
        if reason:
            conn.rollback()
            return None, "", reason
        stored_result = stored_packet_result(conn, current)
        if stored_result is not None:
            conn.commit()
            return stored_result
        conn.commit()

    # Packet construction may read several durable owners and therefore cannot
    # run under this table's write transaction. Recheck everything after it.
    packet = builder()
    with sqlite3.connect(db_path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        current = conn.execute(
            "SELECT lifecycle_state,preparation_epoch,lease_expires_at,frozen_packet_json,frozen_packet_hash "
            "FROM bnl_journal_automation_runs WHERE run_id=?",
            (run_id,),
        ).fetchone()
        reason = owner_reason(current)
        if reason:
            conn.rollback()
            return None, "", reason
        stored_result = stored_packet_result(conn, current)
        if stored_result is not None:
            conn.commit()
            return stored_result
        if not isinstance(packet, dict):
            conn.commit()
            return None, "", "source_packet_not_ready"
        # Archive-unavailable and pre-activation packets are not eligible source
        # packets. Keep the occurrence owed without binding retries to them.
        if not packet.get("sourceArchiveAvailable", False) or not packet.get("coverageComplete", True):
            conn.commit()
            return packet, "", "source_packet_ineligible"
        invalidation = _frozen_packet_invalidation_reason(conn, guild_id, packet)
        if invalidation:
            conn.commit()
            return None, "", invalidation
        raw = _json(packet)
        packet_hash = hashlib.sha256(raw.encode("utf-8")).hexdigest()
        updated = conn.execute(
            "UPDATE bnl_journal_automation_runs SET frozen_packet_json=?,frozen_packet_hash=?,packet_frozen_at=?,updated_at=? "
            "WHERE run_id=? AND preparation_epoch=? AND lifecycle_state='preparing'",
            (raw, packet_hash, utc_now_iso(), utc_now_iso(), run_id, int(preparation_epoch)),
        ).rowcount
        if updated != 1:
            conn.rollback()
            return None, "", "preparation_epoch_superseded"
        conn.commit()
    return packet, packet_hash, ""


def _latest_entry_row(db_path: str, guild_id: int, entry_id: str) -> Optional[dict[str, Any]]:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT e.revision,e.lifecycle_state,e.content_hash,e.canonical_payload_bytes,m.metadata_json "
            "FROM bnl_journal_entries e LEFT JOIN bnl_journal_private_metadata m "
            "ON m.entry_id=e.entry_id AND m.revision=e.revision AND m.guild_id=e.guild_id "
            "WHERE e.guild_id=? AND e.entry_id=? ORDER BY e.revision DESC LIMIT 1",
            (guild_id, entry_id),
        ).fetchone()
    return dict(row) if row else None


def _adopt_legacy_staged_revision(
    db_path: str,
    guild_id: int,
    run_id: str,
    preparation_epoch: int,
    cadence: str,
    entry_id: str,
    start: str,
    end: str,
    memory_excluded_entry_ids: Optional[set[str]],
) -> Optional[AutomationResult]:
    """Bind a pre-migration approved revision without regenerating its bytes.

    A delivery-failed response may have been lost after the website committed.
    Replacing that revision would risk a duplicate article, so one explicit
    migration manifest binds its already-persisted request bytes to the new
    occurrence lifecycle. Legacy drafts were never approved and are handled by
    the normal frozen-packet regeneration path instead.
    """
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("BEGIN IMMEDIATE")
        run = conn.execute(
            "SELECT lifecycle_state,preparation_epoch,lease_expires_at "
            "FROM bnl_journal_automation_runs WHERE run_id=?",
            (run_id,),
        ).fetchone()
        if (
            not run
            or str(run["lifecycle_state"]) != "preparing"
            or int(run["preparation_epoch"] or 0) != int(preparation_epoch)
        ):
            conn.rollback()
            return AutomationResult(
                False,
                cadence,
                "busy",
                "preparation_epoch_superseded",
                source_window_start=start,
                source_window_end=end,
            )
        lease_raw = str(run["lease_expires_at"] or "")
        if not lease_raw or _parse_utc(lease_raw) <= datetime.now(timezone.utc):
            conn.rollback()
            return AutomationResult(
                False,
                cadence,
                "busy",
                "preparation_epoch_expired",
                source_window_start=start,
                source_window_end=end,
            )
        row = conn.execute(
            "SELECT e.revision,e.lifecycle_state,e.canonical_payload_bytes,e.content_hash,"
            "e.source_window_start,e.source_window_end,m.metadata_json "
            "FROM bnl_journal_entries e JOIN bnl_journal_private_metadata m "
            "ON m.entry_id=e.entry_id AND m.revision=e.revision AND m.guild_id=e.guild_id "
            "WHERE e.guild_id=? AND e.entry_id=? ORDER BY e.revision DESC LIMIT 1",
            (guild_id, entry_id),
        ).fetchone()
        if not row or str(row["lifecycle_state"]) not in {
            "approved_pending_delivery",
            "delivery_failed",
        }:
            conn.rollback()
            return None
        try:
            metadata = json.loads(str(row["metadata_json"] or "{}"))
        except (TypeError, json.JSONDecodeError):
            metadata = {}
        if not isinstance(metadata, dict):
            metadata = {}
        # A source-bound revision belongs to the normal recovery path. Only
        # unbound revisions created before this migration are adopted here.
        if str(metadata.get("frozenSourceHash") or ""):
            conn.rollback()
            return None

        revision = int(row["revision"] or 0)
        canonical = bytes(row["canonical_payload_bytes"] or b"")
        request_hash = canonical_payload_hash(canonical) if canonical else ""
        failure = ""
        if str(row["source_window_start"] or "") != start or str(row["source_window_end"] or "") != end:
            failure = "legacy_source_window_mismatch"
        elif not canonical:
            failure = "canonical_payload_missing"
        elif len(canonical) > JOURNAL_SITE_REQUEST_BODY_MAX_BYTES:
            failure = "request_body_too_large"
        elif metadata.get("canonicalPayloadHash") and str(metadata.get("canonicalPayloadHash")) != request_hash:
            failure = "canonical_payload_hash_mismatch"
        elif not bool(metadata.get("coverageComplete", True)):
            failure = "incomplete_source_window"
        else:
            failure = _prepared_invalidation_reason(
                conn,
                guild_id,
                metadata,
                {str(value) for value in (memory_excluded_entry_ids or set()) if str(value)},
            )

        aggregate = metadata.get("aggregateCounts") if isinstance(metadata.get("aggregateCounts"), dict) else {}
        if failure:
            now = utc_now_iso()
            conn.execute(
                "UPDATE bnl_journal_entries SET lifecycle_state='rejected',review_reason=?,updated_at=? "
                "WHERE guild_id=? AND entry_id=? AND revision=? "
                "AND lifecycle_state IN ('approved_pending_delivery','delivery_failed')",
                (failure, now, guild_id, entry_id, revision),
            )
            conn.execute(
                "UPDATE bnl_journal_private_metadata SET lifecycle_state='rejected',updated_at=? "
                "WHERE guild_id=? AND entry_id=? AND revision=?",
                (now, guild_id, entry_id, revision),
            )
            result = AutomationResult(
                False,
                cadence,
                "held",
                failure,
                "",
                0,
                start,
                end,
                aggregate,
            )
            conn.execute(
                "UPDATE bnl_journal_automation_runs SET lifecycle_state='held',reason=?,"
                "journal_entry_id=NULL,journal_revision=0,lease_expires_at=NULL,"
                "prepared_payload_hash=NULL,prepared_payload_bytes=0,prepared_at=NULL,updated_at=? "
                "WHERE run_id=? AND preparation_epoch=? AND lifecycle_state='preparing'",
                (failure, now, run_id, int(preparation_epoch)),
            )
            _reconcile_daily_observation(conn, guild_id, result)
            _update_state(conn, guild_id, result)
            conn.commit()
            return result

        source_refs = sorted(_flatten_source_refs(metadata))
        used_provenance = metadata.get("usedContextLaneProvenance")
        used_provenance = used_provenance if isinstance(used_provenance, dict) else {}
        manifest = {
            "contractVersion": 1,
            "entryKind": cadence,
            "sourceWindowStart": start,
            "sourceWindowEnd": end,
            "sourceArchiveAvailable": True,
            "coverageComplete": True,
            "privateSources": [{"refId": ref_id} for ref_id in source_refs],
            "privateContextLaneProvenance": used_provenance,
            "aggregateCounts": aggregate,
            "legacyStagedRevision": {
                "entryId": entry_id,
                "revision": revision,
                "contentHash": str(row["content_hash"] or ""),
                "canonicalPayloadHash": request_hash,
                "sourceRefIds": metadata.get("sourceRefIds") or {},
                "sourceSummaries": metadata.get("sourceSummaries") or [],
                "relatedPriorJournalEntryIds": metadata.get("relatedPriorJournalEntryIds") or [],
            },
        }
        manifest_raw = _json(manifest)
        source_hash = hashlib.sha256(manifest_raw.encode("utf-8")).hexdigest()
        metadata.update(
            {
                "automationRunId": run_id,
                "automationPreparationEpoch": int(preparation_epoch),
                "frozenSourceHash": source_hash,
                "canonicalPayloadHash": request_hash,
                "canonicalPayloadBytes": len(canonical),
                "legacyPreparedRevisionAdopted": True,
            }
        )
        now = utc_now_iso()
        entry_changed = conn.execute(
            "UPDATE bnl_journal_entries SET lifecycle_state=?,updated_at=? "
            "WHERE guild_id=? AND entry_id=? AND revision=? "
            "AND lifecycle_state IN ('approved_pending_delivery','delivery_failed')",
            (
                SCHEDULED_PREPARED_STATE,
                now,
                guild_id,
                entry_id,
                revision,
            ),
        ).rowcount
        if entry_changed != 1:
            conn.rollback()
            return AutomationResult(
                False,
                cadence,
                "busy",
                "preparation_epoch_superseded",
            )
        conn.execute(
            "UPDATE bnl_journal_private_metadata SET metadata_json=?,lifecycle_state=?,updated_at=? "
            "WHERE guild_id=? AND entry_id=? AND revision=?",
            (
                _json(metadata),
                SCHEDULED_PREPARED_STATE,
                now,
                guild_id,
                entry_id,
                revision,
            ),
        )
        result = AutomationResult(
            True,
            cadence,
            "prepared",
            "legacy_prepared_revision_adopted",
            entry_id,
            revision,
            start,
            end,
            aggregate,
        )
        updated = conn.execute(
            "UPDATE bnl_journal_automation_runs SET lifecycle_state='prepared',reason=?,"
            "journal_entry_id=?,journal_revision=?,aggregate_counts_json=?,"
            "frozen_packet_json=?,frozen_packet_hash=?,packet_frozen_at=?,"
            "prepared_payload_hash=?,prepared_payload_bytes=?,prepared_at=?,"
            "lease_expires_at=NULL,delivery_lease_expires_at=NULL,updated_at=? "
            "WHERE run_id=? AND preparation_epoch=? AND lifecycle_state='preparing'",
            (
                result.reason,
                entry_id,
                revision,
                _json(aggregate),
                manifest_raw,
                source_hash,
                now,
                request_hash,
                len(canonical),
                now,
                now,
                run_id,
                int(preparation_epoch),
            ),
        ).rowcount
        if updated != 1:
            conn.rollback()
            return AutomationResult(False, cadence, "busy", "preparation_epoch_superseded")
        _reconcile_daily_observation(conn, guild_id, result)
        _update_state(conn, guild_id, result)
        conn.commit()
    logging.info(
        "journal_prepared run=%s cadence=%s entry=%s revision=%s source_hash=%s payload_hash=%s "
        "payload_bytes=%s migration=legacy model_calls=0",
        run_id,
        cadence,
        entry_id,
        revision,
        source_hash,
        request_hash,
        len(canonical),
    )
    return result


def _mark_prepared(
    db_path: str,
    guild_id: int,
    run_id: str,
    preparation_epoch: int,
    cadence: str,
    entry_id: str,
    revision: int,
    source_hash: str,
    packet: dict[str, Any],
) -> AutomationResult:
    def hold_invalid_revision(reason: str) -> AutomationResult:
        _reject_staged_revision_for_retry(
            db_path,
            guild_id,
            run_id,
            preparation_epoch,
            entry_id,
            revision,
            reason,
        )
        return _finish_preparation(
            db_path,
            run_id,
            preparation_epoch,
            AutomationResult(
                False,
                cadence,
                "held",
                reason,
                "",
                0,
                str(packet.get("sourceWindowStart") or ""),
                str(packet.get("sourceWindowEnd") or ""),
                packet.get("aggregateCounts") or {},
            ),
        )

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("BEGIN IMMEDIATE")
        run = conn.execute(
            "SELECT lifecycle_state,preparation_epoch,lease_expires_at FROM bnl_journal_automation_runs WHERE run_id=?",
            (run_id,),
        ).fetchone()
        if not run or str(run[0]) != "preparing" or int(run[1] or 0) != int(preparation_epoch):
            conn.rollback()
            return AutomationResult(False, cadence, "busy", "preparation_epoch_superseded")
        lease_raw = str(run[2] or "")
        if not lease_raw or _parse_utc(lease_raw) <= datetime.now(timezone.utc):
            conn.rollback()
            return AutomationResult(False, cadence, "busy", "preparation_epoch_expired")
        entry = conn.execute(
            "SELECT e.lifecycle_state,e.canonical_payload_bytes,e.content_hash,m.metadata_json "
            "FROM bnl_journal_entries e JOIN bnl_journal_private_metadata m "
            "ON m.entry_id=e.entry_id AND m.revision=e.revision AND m.guild_id=e.guild_id "
            "WHERE e.guild_id=? AND e.entry_id=? AND e.revision=?",
            (guild_id, entry_id, int(revision)),
        ).fetchone()
        if not entry or str(entry[0]) not in {
            SCHEDULED_PREPARED_STATE,
            "approved_pending_delivery",
            "delivery_failed",
        }:
            conn.rollback()
            return hold_invalid_revision("prepared_revision_missing")
        canonical = bytes(entry[1] or b"")
        if not canonical:
            conn.rollback()
            return hold_invalid_revision("canonical_payload_missing")
        if len(canonical) > JOURNAL_SITE_REQUEST_BODY_MAX_BYTES:
            conn.rollback()
            return hold_invalid_revision("request_body_too_large")
        request_hash = canonical_payload_hash(canonical)
        try:
            metadata = json.loads(str(entry[3] or "{}"))
        except (TypeError, json.JSONDecodeError):
            metadata = {}
        stored_source_hash = str(metadata.get("frozenSourceHash") or "") if isinstance(metadata, dict) else ""
        if stored_source_hash != source_hash:
            conn.rollback()
            return hold_invalid_revision("source_packet_hash_mismatch")
        now = utc_now_iso()
        if str(entry[0]) != SCHEDULED_PREPARED_STATE:
            entry_changed = conn.execute(
                "UPDATE bnl_journal_entries SET lifecycle_state=?,updated_at=? "
                "WHERE guild_id=? AND entry_id=? AND revision=? "
                "AND lifecycle_state IN ('approved_pending_delivery','delivery_failed')",
                (
                    SCHEDULED_PREPARED_STATE,
                    now,
                    guild_id,
                    entry_id,
                    int(revision),
                ),
            ).rowcount
            metadata_changed = conn.execute(
                "UPDATE bnl_journal_private_metadata SET lifecycle_state=?,updated_at=? "
                "WHERE guild_id=? AND entry_id=? AND revision=? "
                "AND lifecycle_state IN ('approved_pending_delivery','delivery_failed')",
                (
                    SCHEDULED_PREPARED_STATE,
                    now,
                    guild_id,
                    entry_id,
                    int(revision),
                ),
            ).rowcount
            if entry_changed != 1 or metadata_changed != 1:
                conn.rollback()
                return AutomationResult(
                    False,
                    cadence,
                    "busy",
                    "preparation_epoch_superseded",
                )
        result = AutomationResult(
            True,
            cadence,
            "prepared",
            "prepared_waiting_release",
            entry_id,
            int(revision),
            str(packet.get("sourceWindowStart") or ""),
            str(packet.get("sourceWindowEnd") or ""),
            packet.get("aggregateCounts") or {},
        )
        updated = conn.execute(
            "UPDATE bnl_journal_automation_runs SET lifecycle_state='prepared',reason=?,journal_entry_id=?,"
            "journal_revision=?,aggregate_counts_json=?,prepared_payload_hash=?,prepared_payload_bytes=?,prepared_at=?,"
            "lease_expires_at=NULL,delivery_lease_expires_at=NULL,updated_at=? "
            "WHERE run_id=? AND preparation_epoch=? AND lifecycle_state='preparing'",
            (
                result.reason,
                entry_id,
                int(revision),
                _json(result.aggregate_counts or {}),
                request_hash,
                len(canonical),
                now,
                now,
                run_id,
                int(preparation_epoch),
            ),
        ).rowcount
        if updated != 1:
            conn.rollback()
            return AutomationResult(False, cadence, "busy", "preparation_epoch_superseded")
        _reconcile_daily_observation(conn, guild_id, result)
        _update_state(conn, guild_id, result)
        conn.commit()
    logging.info(
        "journal_prepared run=%s cadence=%s entry=%s revision=%s source_hash=%s payload_hash=%s "
        "payload_bytes=%s",
        run_id,
        cadence,
        entry_id,
        revision,
        source_hash,
        request_hash,
        len(canonical),
    )
    return result


def _topic_counts(sources: list[dict[str, Any]], limit: int = 30) -> dict[str, int]:
    return journal_topic_counts(sources, limit=limit)


def _store_observation(
    db_path: str,
    guild_id: int,
    label: str,
    packet: dict[str, Any],
    state: str = "observed",
    *,
    run_id: str = "",
    preparation_epoch: Optional[int] = None,
) -> tuple[str, str]:
    observation_id = "jobs_" + hashlib.sha256(f"{guild_id}|{label}".encode("utf-8")).hexdigest()[:20]
    subject_counts: dict[str, int] = {}
    for source in packet.get("privateSources", []):
        subject = str(source.get("subjectRef") or "")
        if subject:
            subject_counts[subject] = subject_counts.get(subject, 0) + 1
    representative = [
        {key: source.get(key) for key in (
            "refId", "sourceKind", "relayId", "messageId", "subjectRef", "participantAlias", "channelPolicy", "summary", "observedAt", "eventType"
        ) if source.get(key) not in (None, "")}
        for source in packet.get("privateSources", [])
    ]
    now = utc_now_iso()
    with sqlite3.connect(db_path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        if run_id or preparation_epoch is not None:
            if not run_id or preparation_epoch is None:
                conn.rollback()
                return "", "preparation_epoch_superseded"
            run = conn.execute(
                "SELECT lifecycle_state,preparation_epoch,lease_expires_at,frozen_packet_json,frozen_packet_hash "
                "FROM bnl_journal_automation_runs WHERE run_id=?",
                (run_id,),
            ).fetchone()
            if (
                not run
                or str(run[0]) != "preparing"
                or int(run[1] or 0) != int(preparation_epoch)
            ):
                conn.rollback()
                return "", "preparation_epoch_superseded"
            lease_raw = str(run[2] or "")
            try:
                live = bool(lease_raw) and _parse_utc(lease_raw) > datetime.now(timezone.utc)
            except ValueError:
                live = False
            if not live:
                conn.rollback()
                return "", "preparation_epoch_expired"
            stored_raw = str(run[3] or "")
            stored_hash = str(run[4] or "")
            packet_raw = _json(packet)
            if (
                not stored_raw
                or hashlib.sha256(stored_raw.encode("utf-8")).hexdigest() != stored_hash
                or hashlib.sha256(packet_raw.encode("utf-8")).hexdigest() != stored_hash
            ):
                conn.rollback()
                return "", "frozen_packet_hash_mismatch"
            try:
                stored_packet = json.loads(stored_raw)
            except (TypeError, json.JSONDecodeError):
                conn.rollback()
                return "", "frozen_packet_invalid"
            if not isinstance(stored_packet, dict):
                conn.rollback()
                return "", "frozen_packet_invalid"
            if (
                not stored_packet.get("sourceArchiveAvailable", False)
                or not stored_packet.get("coverageComplete", True)
            ):
                invalidation = "source_packet_ineligible"
            else:
                invalidation = _frozen_packet_invalidation_reason(conn, guild_id, stored_packet)
            if invalidation:
                conn.execute(
                    "UPDATE bnl_journal_automation_runs SET frozen_packet_json=NULL,frozen_packet_hash=NULL,"
                    "packet_frozen_at=NULL,updated_at=? WHERE run_id=? AND preparation_epoch=? "
                    "AND lifecycle_state='preparing'",
                    (utc_now_iso(), run_id, int(preparation_epoch)),
                )
                conn.commit()
                return "", invalidation
        conn.execute("""INSERT INTO bnl_journal_observations(
            observation_id,guild_id,observation_date,source_window_start,source_window_end,
            aggregate_counts_json,topic_counts_json,subject_counts_json,representative_sources_json,
            lifecycle_state,journal_entry_id,journal_revision,created_at,updated_at
        ) VALUES(?,?,?,?,?,?,?,?,?,?,NULL,0,?,?)
        ON CONFLICT(guild_id,observation_date) DO UPDATE SET
            aggregate_counts_json=excluded.aggregate_counts_json,topic_counts_json=excluded.topic_counts_json,
            subject_counts_json=excluded.subject_counts_json,representative_sources_json=excluded.representative_sources_json,
            lifecycle_state=CASE WHEN bnl_journal_observations.lifecycle_state='published' THEN 'published' ELSE excluded.lifecycle_state END,
            updated_at=excluded.updated_at""", (
            observation_id, guild_id, label, packet["sourceWindowStart"], packet["sourceWindowEnd"],
            _json(packet.get("aggregateCounts") or {}), _json(_topic_counts(packet.get("privateSources", []))),
            _json(subject_counts), _json(representative), state, now, now,
        ))
        conn.commit()
    return observation_id, ""


def _mark_observation(
    db_path: str,
    guild_id: int,
    label: str,
    result: AutomationResult,
) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "UPDATE bnl_journal_observations SET lifecycle_state=?,"
            "journal_entry_id=?,journal_revision=?,updated_at=? "
            "WHERE guild_id=? AND observation_date=?",
            (
                result.status,
                result.entry_id or None,
                int(result.revision or 0),
                utc_now_iso(),
                guild_id,
                label,
            ),
        )


def _has_meaningful_daily_activity(packet: dict[str, Any]) -> bool:
    counts = packet.get("aggregateCounts") or {}
    total = int(counts.get("eligibleRelays") or 0) + int(counts.get("eligibleConversations") or 0)
    if total < MIN_DAILY_SOURCE_COUNT:
        return False
    if int(counts.get("eligibleConversations") or 0) > 0:
        return True
    quiet_markers = {"quiet", "quiet_source", "non_event_stock", "heartbeat", "hydrated"}
    return any(str(source.get("eventType") or "").lower() not in quiet_markers for source in packet.get("privateSources", []))


def _reject_staged_revision_for_retry(
    db_path: str,
    guild_id: int,
    run_id: str,
    preparation_epoch: int,
    entry_id: str,
    revision: int,
    reason: str,
) -> bool:
    """Reject only while the same live preparation epoch owns the revision."""
    now = utc_now_iso()
    with sqlite3.connect(db_path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        run = conn.execute(
            "SELECT lifecycle_state,preparation_epoch,lease_expires_at "
            "FROM bnl_journal_automation_runs WHERE run_id=?",
            (run_id,),
        ).fetchone()
        if (
            not run
            or str(run[0]) != "preparing"
            or int(run[1] or 0) != int(preparation_epoch)
            or not str(run[2] or "")
            or _parse_utc(str(run[2])) <= datetime.now(timezone.utc)
        ):
            conn.rollback()
            return False
        entry_changed = conn.execute(
            "UPDATE bnl_journal_entries SET lifecycle_state='rejected',review_reason=?,updated_at=? "
            "WHERE guild_id=? AND entry_id=? AND revision=? "
            "AND lifecycle_state IN ('draft','prepared_exact','approved_pending_delivery','delivery_failed')",
            (reason, now, guild_id, entry_id, int(revision)),
        ).rowcount
        metadata_changed = conn.execute(
            "UPDATE bnl_journal_private_metadata SET lifecycle_state='rejected',updated_at=? "
            "WHERE guild_id=? AND entry_id=? AND revision=? "
            "AND lifecycle_state IN ('draft','prepared_exact','approved_pending_delivery','delivery_failed')",
            (now, guild_id, entry_id, int(revision)),
        ).rowcount
        if entry_changed != 1 or metadata_changed != 1:
            conn.rollback()
            return False
        conn.commit()
    return True


def _prepare_packet(
    db_path: str,
    guild_id: int,
    cadence: str,
    label: str,
    run_id: str,
    preparation_epoch: int,
    packet: dict[str, Any],
    source_hash: str,
    generator: Callable[[dict[str, Any], str], str],
) -> AutomationResult:
    entry_id = _entry_id(guild_id, cadence, label)
    existing = _latest_entry_row(db_path, guild_id, entry_id)
    if existing and existing["lifecycle_state"] == "published":
        return _finish_preparation(
            db_path,
            run_id,
            preparation_epoch,
            AutomationResult(True, cadence, "published", "already_published", entry_id, int(existing["revision"]), packet["sourceWindowStart"], packet["sourceWindowEnd"], packet.get("aggregateCounts")),
        )
    existing_source_hash = ""
    if existing and existing["lifecycle_state"] in {
        "draft",
        SCHEDULED_PREPARED_STATE,
        "approved_pending_delivery",
        "delivery_failed",
    }:
        try:
            existing_metadata = json.loads(str(existing.get("metadata_json") or "{}"))
        except (TypeError, json.JSONDecodeError):
            existing_metadata = {}
        existing_source_hash = (
            str(existing_metadata.get("frozenSourceHash") or "")
            if isinstance(existing_metadata, dict)
            else ""
        )
        if not existing_source_hash or existing_source_hash != source_hash:
            reason = "source_packet_unbound" if not existing_source_hash else "source_packet_hash_mismatch"
            if not _reject_staged_revision_for_retry(
                db_path,
                guild_id,
                run_id,
                preparation_epoch,
                entry_id,
                int(existing["revision"]),
                reason,
            ):
                return AutomationResult(False, cadence, "busy", "preparation_epoch_superseded")
            existing = {**existing, "lifecycle_state": "rejected"}

    if existing and existing["lifecycle_state"] in {
        SCHEDULED_PREPARED_STATE,
        "approved_pending_delivery",
        "delivery_failed",
    }:
        return _mark_prepared(
            db_path,
            guild_id,
            run_id,
            preparation_epoch,
            cadence,
            entry_id,
            int(existing["revision"]),
            source_hash,
            packet,
        )
    if existing and existing["lifecycle_state"] == "draft":
        approved = approve_draft(
            db_path,
            guild_id,
            entry_id,
            str(existing["content_hash"]),
            revision=int(existing["revision"]),
            attempt_fence=(run_id, preparation_epoch),
            source_hash=source_hash,
        )
        if not approved.ok:
            if approved.reason == "preparation_epoch_lost":
                return AutomationResult(False, cadence, "busy", "preparation_epoch_superseded")
            if not _reject_staged_revision_for_retry(
                db_path,
                guild_id,
                run_id,
                preparation_epoch,
                entry_id,
                int(existing["revision"]),
                approved.reason,
            ):
                return AutomationResult(False, cadence, "busy", "preparation_epoch_superseded")
            return _finish_preparation(
                db_path,
                run_id,
                preparation_epoch,
                AutomationResult(
                    False,
                    cadence,
                    "held",
                    approved.reason,
                    "",
                    0,
                    packet["sourceWindowStart"],
                    packet["sourceWindowEnd"],
                    packet.get("aggregateCounts"),
                ),
            )
        return _mark_prepared(
            db_path,
            guild_id,
            run_id,
            preparation_epoch,
            cadence,
            entry_id,
            approved.revision,
            source_hash,
            packet,
        )

    revision = int(existing["revision"]) + 1 if existing else 1
    generated = generate_and_store_packet_draft(
        db_path,
        guild_id,
        packet,
        generator,
        entry_id=entry_id,
        revision=revision,
        attempt_fence=(run_id, preparation_epoch),
        source_hash=source_hash,
    )
    if not generated.ok:
        if generated.reason == "preparation_epoch_lost":
            return AutomationResult(False, cadence, "busy", "preparation_epoch_superseded")
        return _finish_preparation(
            db_path,
            run_id,
            preparation_epoch,
            AutomationResult(
                False,
                cadence,
                "held",
                generated.reason,
                "",
                0,
                packet["sourceWindowStart"],
                packet["sourceWindowEnd"],
                packet.get("aggregateCounts"),
            ),
        )
    approved = approve_draft(
        db_path,
        guild_id,
        entry_id,
        generated.content_hash,
        revision=generated.revision,
        attempt_fence=(run_id, preparation_epoch),
        source_hash=source_hash,
    )
    if not approved.ok:
        if approved.reason == "preparation_epoch_lost":
            return AutomationResult(False, cadence, "busy", "preparation_epoch_superseded")
        if not _reject_staged_revision_for_retry(
            db_path,
            guild_id,
            run_id,
            preparation_epoch,
            entry_id,
            generated.revision,
            approved.reason,
        ):
            return AutomationResult(False, cadence, "busy", "preparation_epoch_superseded")
        return _finish_preparation(
            db_path,
            run_id,
            preparation_epoch,
            AutomationResult(
                False,
                cadence,
                "held",
                approved.reason,
                "",
                0,
                packet["sourceWindowStart"],
                packet["sourceWindowEnd"],
                packet.get("aggregateCounts"),
            ),
        )
    return _mark_prepared(
        db_path,
        guild_id,
        run_id,
        preparation_epoch,
        cadence,
        entry_id,
        approved.revision,
        source_hash,
        packet,
    )


def _prepare_packet_safely(
    db_path: str,
    guild_id: int,
    cadence: str,
    label: str,
    run_id: str,
    preparation_epoch: int,
    packet: dict[str, Any],
    source_hash: str,
    generator: Callable[[dict[str, Any], str], str],
) -> AutomationResult:
    """Keep a storage failure nonterminal and visibly owed."""
    try:
        return _prepare_packet(
            db_path,
            guild_id,
            cadence,
            label,
            run_id,
            preparation_epoch,
            packet,
            source_hash,
            generator,
        )
    except sqlite3.Error:
        return _finish_preparation(
            db_path,
            run_id,
            preparation_epoch,
            AutomationResult(
                False,
                cadence,
                "held",
                "journal_preparation_storage_failed",
                "",
                0,
                str(packet.get("sourceWindowStart") or ""),
                str(packet.get("sourceWindowEnd") or ""),
                packet.get("aggregateCounts") or {},
            ),
        )


def _flatten_source_refs(metadata: dict[str, Any]) -> set[str]:
    refs: set[str] = set()
    source_map = metadata.get("sourceRefIds")
    if isinstance(source_map, dict):
        for values in source_map.values():
            if isinstance(values, list):
                refs.update(str(value) for value in values if str(value))
    return refs


def _prepared_invalidation_reason(
    conn: sqlite3.Connection,
    guild_id: int,
    metadata: dict[str, Any],
    memory_excluded_entry_ids: set[str],
) -> str:
    related = {
        str(value)
        for value in metadata.get("relatedPriorJournalEntryIds", [])
        if str(value)
    }
    if related & memory_excluded_entry_ids:
        return "privacy_eligibility_changed"
    fresh_sequences = {
        int(match.group(1))
        for ref in _flatten_source_refs(metadata)
        for match in [re.fullmatch(r"fresh:(\d+)", ref)]
        if match
    }
    if fresh_sequences:
        if not conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='bnl_journal_source_events'"
        ).fetchone():
            return "source_archive_unavailable"
        placeholders = ",".join("?" for _ in fresh_sequences)
        eligible = {
            int(row[0])
            for row in conn.execute(
                f"SELECT event_seq FROM bnl_journal_source_events WHERE guild_id=? AND public_usable=1 AND event_seq IN ({placeholders})",
                (guild_id, *sorted(fresh_sequences)),
            ).fetchall()
        }
        if eligible != fresh_sequences:
            return "privacy_source_ineligible"
    used_provenance = metadata.get("usedContextLaneProvenance")
    used_provenance = used_provenance if isinstance(used_provenance, dict) else {}
    memory_rows = used_provenance.get("establishedBroadcastMemory")
    memory_rows = memory_rows if isinstance(memory_rows, list) else []
    if not journal_broadcast_memory_provenance_is_eligible(
        conn,
        guild_id,
        memory_rows,
        utc_now_iso(),
    ):
        return "privacy_memory_ineligible"
    return ""


def _invalidate_prepared_in_transaction(
    conn: sqlite3.Connection,
    run_id: str,
    guild_id: int,
    entry_id: str,
    revision: int,
    reason: str,
    *,
    retire_packet: bool,
) -> None:
    now = utc_now_iso()
    run = conn.execute(
        "SELECT cadence,source_window_start,source_window_end,aggregate_counts_json "
        "FROM bnl_journal_automation_runs WHERE run_id=?",
        (run_id,),
    ).fetchone()
    conn.execute(
        "UPDATE bnl_journal_entries SET lifecycle_state='rejected',review_reason=?,updated_at=? "
        "WHERE guild_id=? AND entry_id=? AND revision=? "
        "AND lifecycle_state IN ('prepared_exact','approved_pending_delivery','delivery_failed')",
        (reason, now, guild_id, entry_id, int(revision)),
    )
    conn.execute(
        "UPDATE bnl_journal_private_metadata SET lifecycle_state='rejected',updated_at=? "
        "WHERE guild_id=? AND entry_id=? AND revision=? "
        "AND lifecycle_state IN ('prepared_exact','approved_pending_delivery','delivery_failed')",
        (now, guild_id, entry_id, int(revision)),
    )
    packet_reset = (
        "frozen_packet_json=NULL,frozen_packet_hash=NULL,packet_frozen_at=NULL,"
        if retire_packet
        else ""
    )
    conn.execute(
        "UPDATE bnl_journal_automation_runs SET lifecycle_state='held',reason=?,journal_entry_id=NULL,"
        "journal_revision=0," + packet_reset +
        "prepared_payload_hash=NULL,prepared_payload_bytes=0,prepared_at=NULL,lease_expires_at=NULL,"
        "delivery_lease_expires_at=NULL,preparation_epoch=preparation_epoch+1,delivery_epoch=delivery_epoch+1,updated_at=? "
        "WHERE run_id=?",
        (reason, now, run_id),
    )
    if run:
        try:
            aggregate = json.loads(str(run[3] or "{}"))
        except (TypeError, json.JSONDecodeError):
            aggregate = {}
        result = AutomationResult(
            False,
            str(run[0] or ""),
            "held",
            reason,
            "",
            0,
            str(run[1] or ""),
            str(run[2] or ""),
            aggregate if isinstance(aggregate, dict) else {},
        )
        _reconcile_daily_observation(conn, guild_id, result)
        _update_state(
            conn,
            guild_id,
            result,
            next_retry_at=_utc_iso(datetime.now(timezone.utc) + timedelta(minutes=RETRY_MINUTES)),
        )


def _claim_delivery(
    db_path: str,
    guild_id: int,
    cadence: str,
    start: str,
    end: str,
    *,
    force: bool = False,
    memory_excluded_entry_ids: Optional[set[str]] = None,
    memory_exclusions_confirmed: bool = True,
) -> tuple[str, str, int, dict[str, Any]]:
    ensure_schema(db_path)
    run_id = _run_id(guild_id, cadence, start, end)
    now_dt = datetime.now(timezone.utc)
    # A caller-level Journal fence serializes this claim with the complete
    # network/finalization interval. This read remains an inexpensive shortcut
    # for direct tests and defensive callers.
    try:
        with sqlite3.connect(db_path) as read_conn:
            read_conn.row_factory = sqlite3.Row
            active = read_conn.execute(
                "SELECT * FROM bnl_journal_automation_runs WHERE run_id=?",
                (run_id,),
            ).fetchone()
        if active:
            active_row = dict(active)
            active_lease = str(active_row.get("delivery_lease_expires_at") or "")
            if (
                active_row.get("lifecycle_state") == "delivering"
                and active_lease
                and _parse_utc(active_lease) > now_dt
            ):
                return (
                    "busy",
                    run_id,
                    int(active_row.get("delivery_epoch") or 0),
                    active_row,
                )
    except (sqlite3.Error, ValueError):
        pass
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("BEGIN IMMEDIATE")
        run = conn.execute("SELECT * FROM bnl_journal_automation_runs WHERE run_id=?", (run_id,)).fetchone()
        if not run:
            conn.rollback()
            return "not_prepared", run_id, 0, {}
        current = dict(run)
        if current["lifecycle_state"] == "published":
            conn.commit()
            return "complete", run_id, int(current.get("delivery_epoch") or 0), current
        if current["lifecycle_state"] == "delivery_failed" and not force:
            updated_at = str(current.get("updated_at") or "")
            if updated_at and _parse_utc(updated_at) + timedelta(minutes=RETRY_MINUTES) > now_dt:
                current["retry_at"] = _utc_iso(_parse_utc(updated_at) + timedelta(minutes=RETRY_MINUTES))
                conn.commit()
                return "backoff", run_id, int(current.get("delivery_epoch") or 0), current
        delivery_lease = str(current.get("delivery_lease_expires_at") or "")
        if current["lifecycle_state"] == "delivering" and delivery_lease and _parse_utc(delivery_lease) > now_dt:
            conn.commit()
            return "busy", run_id, int(current.get("delivery_epoch") or 0), current
        if current["lifecycle_state"] not in {"prepared", "delivery_failed", "delivering"}:
            conn.commit()
            return "not_prepared", run_id, int(current.get("delivery_epoch") or 0), current
        if not memory_exclusions_confirmed:
            current["reason"] = "journal_memory_exclusions_unconfirmed"
            conn.commit()
            return (
                "controls_unconfirmed",
                run_id,
                int(current.get("delivery_epoch") or 0),
                current,
            )
        entry_id = str(current.get("journal_entry_id") or "")
        revision = int(current.get("journal_revision") or 0)
        entry = conn.execute(
            "SELECT e.lifecycle_state,e.canonical_payload_bytes,m.metadata_json FROM bnl_journal_entries e "
            "JOIN bnl_journal_private_metadata m ON m.entry_id=e.entry_id AND m.revision=e.revision AND m.guild_id=e.guild_id "
            "WHERE e.guild_id=? AND e.entry_id=? AND e.revision=?",
            (guild_id, entry_id, revision),
        ).fetchone()
        if entry and str(entry[0]) == "published":
            recovered = AutomationResult(
                True,
                cadence,
                "published",
                "delivery_already_recorded",
                entry_id,
                revision,
                start,
                end,
                json.loads(current.get("aggregate_counts_json") or "{}"),
            )
            conn.execute(
                "UPDATE bnl_journal_automation_runs SET lifecycle_state='published',reason=?,"
                "delivery_lease_expires_at=NULL,lease_expires_at=NULL,updated_at=? WHERE run_id=?",
                (recovered.reason, utc_now_iso(), run_id),
            )
            _reconcile_daily_observation(conn, guild_id, recovered)
            _update_state(conn, guild_id, recovered)
            conn.commit()
            current.update({"lifecycle_state": "published", "reason": recovered.reason})
            return "complete", run_id, int(current.get("delivery_epoch") or 0), current
        if not entry or str(entry[0]) not in {
            SCHEDULED_PREPARED_STATE,
            "approved_pending_delivery",
            "delivery_failed",
        }:
            _invalidate_prepared_in_transaction(
                conn,
                run_id,
                guild_id,
                entry_id,
                revision,
                "prepared_revision_missing",
                retire_packet=False,
            )
            conn.commit()
            current["lifecycle_state"] = "held"
            current["reason"] = "prepared_revision_missing"
            return "invalidated", run_id, int(current.get("delivery_epoch") or 0), current
        canonical = bytes(entry[1] or b"")
        expected_hash = str(current.get("prepared_payload_hash") or "")
        if (
            not canonical
            or len(canonical) > JOURNAL_SITE_REQUEST_BODY_MAX_BYTES
            or len(canonical) != int(current.get("prepared_payload_bytes") or 0)
            or canonical_payload_hash(canonical) != expected_hash
        ):
            _invalidate_prepared_in_transaction(
                conn,
                run_id,
                guild_id,
                entry_id,
                revision,
                "prepared_payload_integrity_failed",
                retire_packet=False,
            )
            conn.commit()
            current["lifecycle_state"] = "held"
            current["reason"] = "prepared_payload_integrity_failed"
            return "invalidated", run_id, int(current.get("delivery_epoch") or 0), current
        try:
            metadata = json.loads(str(entry[2] or "{}"))
        except (TypeError, json.JSONDecodeError):
            metadata = {}
        if (
            not isinstance(metadata, dict)
            or str(metadata.get("frozenSourceHash") or "")
            != str(current.get("frozen_packet_hash") or "")
        ):
            _invalidate_prepared_in_transaction(
                conn,
                run_id,
                guild_id,
                entry_id,
                revision,
                "source_packet_hash_mismatch",
                retire_packet=False,
            )
            conn.commit()
            current["lifecycle_state"] = "held"
            current["reason"] = "source_packet_hash_mismatch"
            return "invalidated", run_id, int(current.get("delivery_epoch") or 0), current
        invalidation = _prepared_invalidation_reason(
            conn,
            guild_id,
            metadata if isinstance(metadata, dict) else {},
            {str(value) for value in (memory_excluded_entry_ids or set()) if str(value)},
        )
        if invalidation:
            _invalidate_prepared_in_transaction(
                conn,
                run_id,
                guild_id,
                entry_id,
                revision,
                invalidation,
                retire_packet=True,
            )
            conn.commit()
            current["lifecycle_state"] = "held"
            current["reason"] = invalidation
            return "invalidated", run_id, int(current.get("delivery_epoch") or 0), current
        epoch = int(current.get("delivery_epoch") or 0) + 1
        lease = _utc_iso(now_dt + timedelta(minutes=DELIVERY_LEASE_MINUTES))
        updated = conn.execute(
            "UPDATE bnl_journal_automation_runs SET lifecycle_state='delivering',reason='delivery_in_progress',"
            "delivery_epoch=?,delivery_lease_expires_at=?,updated_at=? WHERE run_id=? AND delivery_epoch=?",
            (epoch, lease, utc_now_iso(), run_id, int(current.get("delivery_epoch") or 0)),
        ).rowcount
        if updated != 1:
            conn.rollback()
            return "busy", run_id, int(current.get("delivery_epoch") or 0), current
        conn.commit()
        current.update({"journal_entry_id": entry_id, "journal_revision": revision})
    return "claimed", run_id, epoch, current


def _finish_delivery(
    db_path: str,
    run_id: str,
    delivery_epoch: int,
    result: AutomationResult,
) -> AutomationResult:
    retry_at = ""
    if result.status in {"held", "delivery_failed", "deferred"}:
        retry_at = _utc_iso(datetime.now(timezone.utc) + timedelta(minutes=RETRY_MINUTES))
    with sqlite3.connect(db_path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            "SELECT guild_id FROM bnl_journal_automation_runs WHERE run_id=? AND delivery_epoch=? AND lifecycle_state='delivering'",
            (run_id, int(delivery_epoch)),
        ).fetchone()
        if not row:
            conn.rollback()
            return AutomationResult(False, result.cadence, "busy", "delivery_epoch_superseded")
        updated = conn.execute(
            "UPDATE bnl_journal_automation_runs SET lifecycle_state=?,reason=?,journal_entry_id=?,journal_revision=?,"
            "aggregate_counts_json=?,delivery_lease_expires_at=NULL,updated_at=? "
            "WHERE run_id=? AND delivery_epoch=? AND lifecycle_state='delivering'",
            (
                result.status,
                result.reason,
                result.entry_id or None,
                int(result.revision or 0),
                _json(result.aggregate_counts or {}),
                utc_now_iso(),
                run_id,
                int(delivery_epoch),
            ),
        ).rowcount
        if updated != 1:
            conn.rollback()
            return AutomationResult(False, result.cadence, "busy", "delivery_epoch_superseded")
        _reconcile_daily_observation(conn, int(row[0]), result)
        _update_state(conn, int(row[0]), result, next_retry_at=retry_at)
        conn.commit()
    return result


def _finish_invalidated_delivery(
    db_path: str,
    run_id: str,
    delivery_epoch: int,
    result: AutomationResult,
) -> AutomationResult:
    """Invalidate a staged revision found ineligible at the final POST fence."""
    with sqlite3.connect(db_path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            "SELECT guild_id,journal_entry_id,journal_revision "
            "FROM bnl_journal_automation_runs "
            "WHERE run_id=? AND delivery_epoch=? AND lifecycle_state='delivering'",
            (run_id, int(delivery_epoch)),
        ).fetchone()
        if not row:
            conn.rollback()
            return AutomationResult(
                False,
                result.cadence,
                "busy",
                "delivery_epoch_superseded",
                result.entry_id,
                result.revision,
                result.source_window_start,
                result.source_window_end,
                result.aggregate_counts,
            )
        _invalidate_prepared_in_transaction(
            conn,
            run_id,
            int(row[0]),
            str(row[1] or result.entry_id),
            int(row[2] or result.revision),
            result.reason,
            retire_packet=result.reason.startswith("privacy_"),
        )
        conn.commit()
    return AutomationResult(
        False,
        result.cadence,
        "held",
        result.reason,
        "",
        0,
        result.source_window_start,
        result.source_window_end,
        result.aggregate_counts,
        result.http_status,
        result.idempotent,
    )


def _release_occurrence_under_fence(
    db_path: str,
    guild_id: int,
    cadence: str,
    start: str,
    end: str,
    base_url: str,
    api_key: str,
    *,
    force: bool = False,
    opener=None,
    memory_excluded_entry_ids: Optional[set[str]] = None,
    memory_exclusions_confirmed: bool = True,
) -> AutomationResult:
    claim, run_id, delivery_epoch, row = _claim_delivery(
        db_path,
        guild_id,
        cadence,
        start,
        end,
        force=force,
        memory_excluded_entry_ids=memory_excluded_entry_ids,
        memory_exclusions_confirmed=memory_exclusions_confirmed,
    )
    if claim != "claimed":
        if claim == "invalidated":
            return AutomationResult(False, cadence, "held", str(row.get("reason") or "prepared_payload_invalidated"), source_window_start=start, source_window_end=end, aggregate_counts=json.loads(row.get("aggregate_counts_json") or "{}"))
        if claim == "not_prepared":
            return AutomationResult(False, cadence, "held", "prepared_payload_unavailable", source_window_start=start, source_window_end=end)
        if claim == "controls_unconfirmed":
            logging.info(
                "journal_release_waiting run=%s cadence=%s entry=%s revision=%s "
                "source_hash=%s payload_hash=%s reason=journal_memory_exclusions_unconfirmed "
                "model_calls=0 post_attempts=0",
                run_id,
                cadence,
                str(row.get("journal_entry_id") or ""),
                int(row.get("journal_revision") or 0),
                str(row.get("frozen_packet_hash") or ""),
                str(row.get("prepared_payload_hash") or ""),
            )
        return _result_from_existing(cadence, row, claim=claim)
    entry_id = str(row.get("journal_entry_id") or "")
    revision = int(row.get("journal_revision") or 0)
    counts = json.loads(row.get("aggregate_counts_json") or "{}")
    payload_hash = str(row.get("prepared_payload_hash") or "")
    payload_bytes = int(row.get("prepared_payload_bytes") or 0)
    source_hash = str(row.get("frozen_packet_hash") or "")
    logging.info(
        "journal_release_started run=%s cadence=%s entry=%s revision=%s source_hash=%s "
        "payload_hash=%s model_calls=0",
        run_id,
        cadence,
        entry_id,
        revision,
        source_hash,
        payload_hash,
    )
    if not base_url or not api_key:
        delivered = JournalResult(False, "delivery_failed", "website_configuration_missing", entry_id, revision)
    else:
        excluded_entry_ids = {
            str(value)
            for value in (memory_excluded_entry_ids or set())
            if str(value)
        }

        def final_delivery_preflight(conn: sqlite3.Connection) -> str:
            durable_excluded_entry_ids, durable_exclusions_confirmed = (
                _load_journal_memory_exclusions_on_connection(conn, guild_id)
            )
            effective_excluded_entry_ids = (
                durable_excluded_entry_ids
                if durable_exclusions_confirmed
                else excluded_entry_ids
            )
            private = conn.execute(
                "SELECT m.metadata_json,e.canonical_payload_bytes "
                "FROM bnl_journal_private_metadata m "
                "JOIN bnl_journal_entries e "
                "ON e.guild_id=m.guild_id AND e.entry_id=m.entry_id AND e.revision=m.revision "
                "WHERE m.guild_id=? AND m.entry_id=? AND m.revision=? "
                "AND m.lifecycle_state IN ('prepared_exact','approved_pending_delivery','delivery_failed') "
                "AND e.lifecycle_state IN ('prepared_exact','approved_pending_delivery','delivery_failed')",
                (guild_id, entry_id, revision),
            ).fetchone()
            if not private:
                return "prepared_revision_missing"
            canonical = bytes(private[1] or b"")
            if (
                not canonical
                or len(canonical) > JOURNAL_SITE_REQUEST_BODY_MAX_BYTES
                or len(canonical) != payload_bytes
                or canonical_payload_hash(canonical) != payload_hash
            ):
                return "prepared_payload_integrity_failed"
            try:
                metadata = json.loads(str(private[0] or "{}"))
            except (TypeError, json.JSONDecodeError):
                return "prepared_revision_missing"
            if (
                not isinstance(metadata, dict)
                or str(metadata.get("frozenSourceHash") or "") != source_hash
            ):
                return "source_packet_hash_mismatch"
            return _prepared_invalidation_reason(
                conn,
                guild_id,
                metadata,
                effective_excluded_entry_ids,
            )

        try:
            delivered = deliver_approved(
                db_path,
                guild_id,
                entry_id,
                base_url,
                api_key,
                opener=opener,
                revision=revision,
                delivery_fence=(run_id, delivery_epoch),
                delivery_preflight=final_delivery_preflight,
            )
        except sqlite3.Error:
            delivered = JournalResult(
                False,
                "delivery_failed",
                "journal_delivery_storage_failed",
                entry_id,
                revision,
            )
    if delivered.reason == "delivery_epoch_lost":
        logging.info(
            "journal_release_suppressed run=%s cadence=%s entry=%s revision=%s "
            "payload_hash=%s reason=delivery_epoch_lost model_calls=0 post_attempts=0",
            run_id,
            cadence,
            entry_id,
            revision,
            payload_hash,
        )
        return AutomationResult(
            False,
            cadence,
            "busy",
            "delivery_epoch_superseded",
            entry_id,
            revision,
            start,
            end,
            counts,
        )
    status = delivered.status if delivered.status in {"published", "delivery_failed"} else "held"
    result = AutomationResult(
        delivered.ok,
        cadence,
        status,
        delivered.reason,
        entry_id,
        delivered.revision or revision,
        start,
        end,
        counts,
        int(delivered.http_status or 0),
        bool(delivered.idempotent),
    )
    if delivered.reason in DELIVERY_PREFLIGHT_INVALIDATION_REASONS:
        finished = _finish_invalidated_delivery(
            db_path,
            run_id,
            delivery_epoch,
            result,
        )
    else:
        finished = _finish_delivery(db_path, run_id, delivery_epoch, result)
    logging.info(
        "journal_release_finished run=%s cadence=%s entry=%s revision=%s source_hash=%s "
        "payload_hash=%s status=%s http=%s idempotent=%s model_calls=0",
        run_id,
        cadence,
        entry_id,
        revision,
        source_hash,
        payload_hash,
        finished.status,
        int(finished.http_status or 0),
        bool(finished.idempotent),
    )
    return finished


def release_occurrence(
    db_path: str,
    guild_id: int,
    cadence: str,
    start: str,
    end: str,
    base_url: str,
    api_key: str,
    *,
    force: bool = False,
    opener=None,
    memory_excluded_entry_ids: Optional[set[str]] = None,
    memory_exclusions_confirmed: bool = True,
) -> AutomationResult:
    """Own claim, POST, and durable finalization under one narrow gate."""
    with journal_release_privacy_fence(db_path, blocking=False) as acquired:
        if not acquired:
            return AutomationResult(
                False,
                cadence,
                "busy",
                "delivery_owner_in_progress",
                source_window_start=start,
                source_window_end=end,
            )
        return _release_occurrence_under_fence(
            db_path,
            guild_id,
            cadence,
            start,
            end,
            base_url,
            api_key,
            force=force,
            opener=opener,
            memory_excluded_entry_ids=memory_excluded_entry_ids,
            memory_exclusions_confirmed=memory_exclusions_confirmed,
        )


def release_prepared_entry(
    db_path: str,
    guild_id: int,
    entry_id: str,
    base_url: str,
    api_key: str,
    *,
    force: bool = False,
    opener=None,
    memory_excluded_entry_ids: Optional[set[str]] = None,
    memory_exclusions_confirmed: bool = True,
) -> Optional[AutomationResult]:
    """Release a scheduled entry through its occurrence-level network owner.

    Manual, non-scheduled drafts have no automation occurrence and return
    ``None`` so their existing explicit-review delivery path remains intact.
    """
    ensure_schema(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT cadence,source_window_start,source_window_end "
            "FROM bnl_journal_automation_runs "
            "WHERE guild_id=? AND journal_entry_id=? "
            "ORDER BY journal_revision DESC,updated_at DESC LIMIT 1",
            (guild_id, str(entry_id)),
        ).fetchone()
        automation_run_id = ""
        if not row:
            metadata_rows = conn.execute(
                "SELECT metadata_json FROM bnl_journal_private_metadata "
                "WHERE guild_id=? AND entry_id=? ORDER BY revision DESC",
                (guild_id, str(entry_id)),
            ).fetchall()
            for metadata_row in metadata_rows:
                try:
                    metadata = json.loads(str(metadata_row[0] or "{}"))
                except (TypeError, json.JSONDecodeError):
                    continue
                if isinstance(metadata, dict) and metadata.get("automationRunId"):
                    automation_run_id = str(metadata.get("automationRunId"))
                    break
            if automation_run_id:
                row = conn.execute(
                    "SELECT cadence,source_window_start,source_window_end "
                    "FROM bnl_journal_automation_runs WHERE guild_id=? AND run_id=?",
                    (guild_id, automation_run_id),
                ).fetchone()
    if not row:
        if automation_run_id:
            return AutomationResult(
                False,
                "scheduled",
                "held",
                "scheduled_occurrence_missing",
                str(entry_id),
            )
        return None
    return release_occurrence(
        db_path,
        guild_id,
        str(row["cadence"]),
        str(row["source_window_start"]),
        str(row["source_window_end"]),
        base_url,
        api_key,
        force=force,
        opener=opener,
        memory_excluded_entry_ids=memory_excluded_entry_ids,
        memory_exclusions_confirmed=memory_exclusions_confirmed,
    )


def prepare_daily(
    db_path: str,
    guild_id: int,
    generator: Callable[[dict[str, Any], str], str],
    *,
    now_utc: Optional[datetime] = None,
    target_day: Optional[date] = None,
    force: bool = False,
    memory_excluded_entry_ids: Optional[set[str]] = None,
) -> AutomationResult:
    ensure_schema(db_path)
    start, end, label = _daily_period_for_day(target_day) if target_day else daily_period(now_utc)
    if not force and target_day is None and not daily_due(now_utc):
        return AutomationResult(False, "daily", "not_due", "before_daily_publish_time", source_window_start=start, source_window_end=end)
    claim, run_id, preparation_epoch, row = _claim_preparation(db_path, guild_id, "daily", start, end, force=force)
    if claim != "claimed":
        return _result_from_existing("daily", row, claim=claim)
    legacy = _adopt_legacy_staged_revision(
        db_path,
        guild_id,
        run_id,
        preparation_epoch,
        "daily",
        _entry_id(guild_id, "daily", label),
        start,
        end,
        memory_excluded_entry_ids,
    )
    if legacy is not None:
        return legacy
    packet, source_hash, freeze_reason = _freeze_or_load_packet(
        db_path,
        guild_id,
        run_id,
        preparation_epoch,
        lambda: build_source_packet_between(
            db_path,
            guild_id,
            start,
            end,
            entry_kind="daily",
            excluded_history_entry_ids=memory_excluded_entry_ids,
        ),
    )
    if packet is None:
        if freeze_reason.startswith("preparation_epoch_"):
            return AutomationResult(False, "daily", "busy", freeze_reason, source_window_start=start, source_window_end=end)
        return _finish_preparation(
            db_path,
            run_id,
            preparation_epoch,
            AutomationResult(False, "daily", "held", freeze_reason or "source_packet_freeze_failed", source_window_start=start, source_window_end=end),
        )
    if source_hash:
        observation_id, observation_reason = _store_observation(
            db_path,
            guild_id,
            label,
            packet,
            run_id=run_id,
            preparation_epoch=preparation_epoch,
        )
        if not observation_id:
            if observation_reason.startswith("preparation_epoch_"):
                return AutomationResult(
                    False,
                    "daily",
                    "busy",
                    observation_reason,
                    source_window_start=start,
                    source_window_end=end,
                )
            return _finish_preparation(
                db_path,
                run_id,
                preparation_epoch,
                AutomationResult(
                    False,
                    "daily",
                    "held",
                    observation_reason or "observation_store_failed",
                    source_window_start=start,
                    source_window_end=end,
                    aggregate_counts=packet.get("aggregateCounts"),
                ),
            )
    if not packet.get("sourceArchiveAvailable", False):
        result = AutomationResult(False, "daily", "held", "source_archive_unavailable", source_window_start=start, source_window_end=end, aggregate_counts=packet.get("aggregateCounts"))
    elif not packet.get("coverageComplete", True):
        # Time cannot make a pre-archive window complete. Record it explicitly
        # and move on instead of blocking every later catch-up day forever.
        result = AutomationResult(False, "daily", "incomplete", "window_began_before_archive_activation", source_window_start=start, source_window_end=end, aggregate_counts=packet.get("aggregateCounts"))
    elif not _has_meaningful_daily_activity(packet):
        result = AutomationResult(True, "daily", "quiet", "insufficient_meaningful_activity", source_window_start=start, source_window_end=end, aggregate_counts=packet.get("aggregateCounts"))
    else:
        result = _prepare_packet_safely(
            db_path,
            guild_id,
            "daily",
            label,
            run_id,
            preparation_epoch,
            packet,
            source_hash,
            generator,
        )
        return result
    finished = _finish_preparation(db_path, run_id, preparation_epoch, result)
    return finished


def release_daily(
    db_path: str,
    guild_id: int,
    base_url: str,
    api_key: str,
    *,
    now_utc: Optional[datetime] = None,
    target_day: Optional[date] = None,
    force: bool = False,
    opener=None,
    memory_excluded_entry_ids: Optional[set[str]] = None,
    memory_exclusions_confirmed: bool = True,
) -> AutomationResult:
    start, end, label = _daily_period_for_day(target_day) if target_day else daily_period(now_utc)
    if not force and target_day is None and not daily_due(now_utc):
        return AutomationResult(False, "daily", "not_due", "before_daily_publish_time", source_window_start=start, source_window_end=end)
    result = release_occurrence(
        db_path,
        guild_id,
        "daily",
        start,
        end,
        base_url,
        api_key,
        force=force,
        opener=opener,
        memory_excluded_entry_ids=memory_excluded_entry_ids,
        memory_exclusions_confirmed=memory_exclusions_confirmed,
    )
    return result


def run_daily(
    db_path: str,
    guild_id: int,
    generator: Callable[[dict[str, Any], str], str],
    base_url: str,
    api_key: str,
    *,
    now_utc: Optional[datetime] = None,
    target_day: Optional[date] = None,
    force: bool = False,
    opener=None,
    memory_excluded_entry_ids: Optional[set[str]] = None,
    memory_exclusions_confirmed: bool = True,
) -> AutomationResult:
    prepared = prepare_daily(
        db_path,
        guild_id,
        generator,
        now_utc=now_utc,
        target_day=target_day,
        force=force,
        memory_excluded_entry_ids=memory_excluded_entry_ids,
    )
    if prepared.status != "prepared":
        return prepared
    return release_daily(
        db_path,
        guild_id,
        base_url,
        api_key,
        now_utc=now_utc,
        target_day=target_day,
        force=force,
        opener=opener,
        memory_excluded_entry_ids=memory_excluded_entry_ids,
        memory_exclusions_confirmed=memory_exclusions_confirmed,
    )


def _weekly_packet(
    db_path: str,
    guild_id: int,
    start: str,
    end: str,
    *,
    memory_excluded_entry_ids: Optional[set[str]] = None,
) -> tuple[Optional[dict[str, Any]], int, int]:
    ensure_schema(db_path)
    excluded = {str(entry_id) for entry_id in (memory_excluded_entry_ids or set()) if str(entry_id)}
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        observations = conn.execute("""SELECT * FROM bnl_journal_observations
            WHERE guild_id=? AND source_window_start>=? AND source_window_end<=?
            ORDER BY source_window_start ASC""", (guild_id, start, end)).fetchall()
        daily_entries = {
            row[0]: {"title": row[1], "excerpt": row[2]}
            for row in conn.execute("""SELECT entry_id,title,excerpt FROM bnl_journal_entries
                WHERE guild_id=? AND lifecycle_state='published' AND source_window_start>=? AND source_window_end<=?""", (guild_id, start, end)).fetchall()
        }
    complete = [row for row in observations if row["lifecycle_state"] in {"published", "quiet"}]
    active = [row for row in complete if row["lifecycle_state"] == "published"]
    # A weekly synthesis is only grounded once all seven daily windows have
    # reached a durable terminal state. Held/incomplete days are excluded.
    if len(complete) < 7 or len(active) < MIN_WEEKLY_ACTIVE_DAYS:
        return None, len(complete), len(active)
    observation_context: list[dict[str, Any]] = []
    for row in complete:
        counts = json.loads(row["aggregate_counts_json"] or "{}")
        topics = json.loads(row["topic_counts_json"] or "{}")
        daily_entry_id = str(row["journal_entry_id"] or "")
        entry = daily_entries.get(daily_entry_id, {}) if daily_entry_id not in excluded else {}
        observation_context.append({
            "date": row["observation_date"], "state": row["lifecycle_state"], "counts": counts,
            "topicCounts": topics, "dailyEntryId": "" if daily_entry_id in excluded else daily_entry_id,
            "dailyTitle": entry.get("title", ""), "dailyExcerpt": entry.get("excerpt", ""),
        })
    # Re-read the durable full-week archive and sample it exactly once. Reusing
    # already-sampled daily representatives caused busy weeks to be compressed
    # twice and discarded the private display-name set needed for the normal
    # cross-participant sanitization pass.
    packet = build_source_packet_between(
        db_path,
        guild_id,
        start,
        end,
        entry_kind="weekly",
        observation_context=observation_context,
        excluded_history_entry_ids=excluded,
    )
    packet.setdefault("aggregateCounts", {})["daysObserved"] = len(complete)
    packet["aggregateCounts"]["activeDays"] = len(active)
    return packet, len(complete), len(active)


def prepare_weekly(
    db_path: str,
    guild_id: int,
    generator: Callable[[dict[str, Any], str], str],
    *,
    now_utc: Optional[datetime] = None,
    target_monday: Optional[date] = None,
    force: bool = False,
    memory_excluded_entry_ids: Optional[set[str]] = None,
) -> AutomationResult:
    ensure_schema(db_path)
    start, end, label = _weekly_period_for_monday(target_monday) if target_monday else weekly_period(now_utc)
    if not force and target_monday is None and not weekly_due(now_utc):
        return AutomationResult(False, "weekly", "not_due", "before_weekly_publish_time", source_window_start=start, source_window_end=end)
    claim, run_id, preparation_epoch, row = _claim_preparation(
        db_path,
        guild_id,
        "weekly",
        start,
        end,
        force=force,
    )
    if claim != "claimed":
        return _result_from_existing("weekly", row, claim=claim)
    legacy = _adopt_legacy_staged_revision(
        db_path,
        guild_id,
        run_id,
        preparation_epoch,
        "weekly",
        _entry_id(guild_id, "weekly", label),
        start,
        end,
        memory_excluded_entry_ids,
    )
    if legacy is not None:
        return legacy

    readiness: dict[str, int] = {}

    def build_weekly_packet() -> Optional[dict[str, Any]]:
        packet, complete_days, active_days = _weekly_packet(
            db_path,
            guild_id,
            start,
            end,
            memory_excluded_entry_ids=memory_excluded_entry_ids,
        )
        readiness["completeDays"] = complete_days
        readiness["activeDays"] = active_days
        return packet

    packet, source_hash, freeze_reason = _freeze_or_load_packet(
        db_path,
        guild_id,
        run_id,
        preparation_epoch,
        build_weekly_packet,
    )
    if packet is not None:
        aggregate = packet.get("aggregateCounts") or {}
        complete_days = int(aggregate.get("daysObserved") or readiness.get("completeDays") or 0)
        active_days = int(aggregate.get("activeDays") or readiness.get("activeDays") or 0)
    else:
        complete_days = int(readiness.get("completeDays") or 0)
        active_days = int(readiness.get("activeDays") or 0)

    if packet is None and freeze_reason.startswith("preparation_epoch_"):
        return AutomationResult(
            False,
            "weekly",
            "busy",
            freeze_reason,
            source_window_start=start,
            source_window_end=end,
        )
    if packet is None and freeze_reason not in {"", "source_packet_not_ready"}:
        result = AutomationResult(
            False,
            "weekly",
            "held",
            freeze_reason,
            source_window_start=start,
            source_window_end=end,
            aggregate_counts={"completeDays": complete_days, "activeDays": active_days},
        )
    elif packet is None and complete_days == 7 and active_days == 0:
        result = AutomationResult(True, "weekly", "quiet", "no_meaningful_weekly_activity", source_window_start=start, source_window_end=end, aggregate_counts={"completeDays": complete_days, "activeDays": active_days})
    elif packet is None:
        result = AutomationResult(True, "weekly", "deferred", f"only_{complete_days}_complete_daily_observations", source_window_start=start, source_window_end=end, aggregate_counts={"completeDays": complete_days, "activeDays": active_days})
    elif not packet.get("sourceArchiveAvailable", False):
        result = AutomationResult(False, "weekly", "held", "source_archive_unavailable", source_window_start=start, source_window_end=end, aggregate_counts=packet.get("aggregateCounts"))
    elif not packet.get("coverageComplete", True):
        result = AutomationResult(False, "weekly", "incomplete", "window_began_before_archive_activation", source_window_start=start, source_window_end=end, aggregate_counts=packet.get("aggregateCounts"))
    else:
        return _prepare_packet_safely(
            db_path,
            guild_id,
            "weekly",
            label,
            run_id,
            preparation_epoch,
            packet,
            source_hash,
            generator,
        )
    return _finish_preparation(db_path, run_id, preparation_epoch, result)


def release_weekly(
    db_path: str,
    guild_id: int,
    base_url: str,
    api_key: str,
    *,
    now_utc: Optional[datetime] = None,
    target_monday: Optional[date] = None,
    force: bool = False,
    opener=None,
    memory_excluded_entry_ids: Optional[set[str]] = None,
    memory_exclusions_confirmed: bool = True,
) -> AutomationResult:
    start, end, _label = _weekly_period_for_monday(target_monday) if target_monday else weekly_period(now_utc)
    if not force and target_monday is None and not weekly_due(now_utc):
        return AutomationResult(False, "weekly", "not_due", "before_weekly_publish_time", source_window_start=start, source_window_end=end)
    return release_occurrence(
        db_path,
        guild_id,
        "weekly",
        start,
        end,
        base_url,
        api_key,
        force=force,
        opener=opener,
        memory_excluded_entry_ids=memory_excluded_entry_ids,
        memory_exclusions_confirmed=memory_exclusions_confirmed,
    )


def run_weekly(
    db_path: str,
    guild_id: int,
    generator: Callable[[dict[str, Any], str], str],
    base_url: str,
    api_key: str,
    *,
    now_utc: Optional[datetime] = None,
    target_monday: Optional[date] = None,
    force: bool = False,
    opener=None,
    memory_excluded_entry_ids: Optional[set[str]] = None,
    memory_exclusions_confirmed: bool = True,
) -> AutomationResult:
    prepared = prepare_weekly(
        db_path,
        guild_id,
        generator,
        now_utc=now_utc,
        target_monday=target_monday,
        force=force,
        memory_excluded_entry_ids=memory_excluded_entry_ids,
    )
    if prepared.status != "prepared":
        return prepared
    return release_weekly(
        db_path,
        guild_id,
        base_url,
        api_key,
        now_utc=now_utc,
        target_monday=target_monday,
        force=force,
        opener=opener,
        memory_excluded_entry_ids=memory_excluded_entry_ids,
        memory_exclusions_confirmed=memory_exclusions_confirmed,
    )


def _archive_activation_day(db_path: str, guild_id: int) -> Optional[date]:
    try:
        with sqlite3.connect(db_path) as conn:
            row = conn.execute(
                "SELECT activated_at_ms FROM bnl_journal_source_archive_state WHERE guild_id=?",
                (guild_id,),
            ).fetchone()
    except sqlite3.Error:
        return None
    if not row:
        return None
    activated = datetime.fromtimestamp(int(row[0]) / 1000.0, tz=timezone.utc).astimezone(PACIFIC)
    same_day_cutoff = _pacific_journal_cutoff(activated.date())
    # The first fully covered window starts at the first 7 PM cutoff at or
    # after activation. An activation later that evening must wait until the
    # following day's cutoff.
    if activated <= same_day_cutoff:
        return activated.date()
    return activated.date() + timedelta(days=1)


def _latest_daily_day(now_utc: Optional[datetime]) -> date:
    local = (now_utc or datetime.now(timezone.utc)).astimezone(PACIFIC)
    latest = local.date() - timedelta(days=1)
    if (local.hour, local.minute) < (DAILY_READY_HOUR, DAILY_READY_MINUTE):
        latest -= timedelta(days=1)
    return latest


def _pending_daily_day(db_path: str, guild_id: int, now_utc: Optional[datetime]) -> Optional[date]:
    first = _archive_activation_day(db_path, guild_id)
    latest = _latest_daily_day(now_utc)
    if first is None or first > latest:
        return None
    with sqlite3.connect(db_path) as conn:
        rows = {
            (str(row[0]), str(row[1]), str(row[2])): str(row[3])
            for row in conn.execute(
                "SELECT cadence,source_window_start,source_window_end,lifecycle_state FROM bnl_journal_automation_runs WHERE guild_id=? AND cadence='daily'",
                (guild_id,),
            ).fetchall()
        }
    # Always give the newly closed 7-to-7 window first priority. Once that
    # window is terminal, the interval worker walks backward through any
    # older backlog without letting an old held run starve today's Journal.
    current = latest
    while current >= first:
        start, end, _ = _daily_period_for_day(current)
        if rows.get(("daily", start, end)) not in TERMINAL_RUN_STATES:
            return current
        current -= timedelta(days=1)
    return None


def _latest_week_monday(now_utc: Optional[datetime]) -> date:
    local = (now_utc or datetime.now(timezone.utc)).astimezone(PACIFIC)
    current_monday = local.date() - timedelta(days=local.weekday())
    if local.weekday() == WEEKLY_READY_WEEKDAY and local.hour < WEEKLY_READY_HOUR:
        current_monday -= timedelta(days=7)
    return current_monday - timedelta(days=7)


def _pending_week_monday(db_path: str, guild_id: int, now_utc: Optional[datetime]) -> Optional[date]:
    first_day = _archive_activation_day(db_path, guild_id)
    if first_day is None:
        return None
    first_monday = first_day + timedelta(days=(7 - first_day.weekday()) % 7)
    latest = _latest_week_monday(now_utc)
    if first_monday > latest:
        return None
    with sqlite3.connect(db_path) as conn:
        rows = {
            (str(row[0]), str(row[1])): str(row[2])
            for row in conn.execute(
                "SELECT source_window_start,source_window_end,lifecycle_state FROM bnl_journal_automation_runs WHERE guild_id=? AND cadence='weekly'",
                (guild_id,),
            ).fetchall()
        }
    current = first_monday
    while current <= latest:
        start, end, _ = _weekly_period_for_monday(current)
        if rows.get((start, end)) not in TERMINAL_RUN_STATES:
            return current
        current += timedelta(days=7)
    return None


def run_scheduled(
    db_path: str,
    guild_id: int,
    generator: Callable[[dict[str, Any], str], str],
    base_url: str,
    api_key: str,
    flags: Optional[dict[str, Any]] = None,
    *,
    now_utc: Optional[datetime] = None,
    opener=None,
) -> list[AutomationResult]:
    controls = flags or {}
    memory_excluded_entry_ids = {
        str(entry_id)
        for entry_id in controls.get("journalMemoryExcludedEntryIds", [])
        if str(entry_id)
    }
    memory_exclusions_confirmed = bool(
        controls.get("journalMemoryExclusionsConfirmed", True)
    )
    if not bool(controls.get("journalAutoPublishEnabled", True)):
        return [AutomationResult(False, "all", "paused", "auto_publish_paused")]
    try:
        from bnl_journal_source_store import backfill_legacy_sources

        backfill_legacy_sources(db_path, guild_id)
    except (ImportError, sqlite3.Error, ValueError):
        return [AutomationResult(False, "all", "held", "source_archive_unavailable")]
    results: list[AutomationResult] = []
    if bool(controls.get("journalDailyEnabled", True)):
        target_day = _pending_daily_day(db_path, guild_id, now_utc)
        if target_day is not None:
            results.append(run_daily(db_path, guild_id, generator, base_url, api_key, now_utc=now_utc, target_day=target_day, opener=opener, memory_excluded_entry_ids=memory_excluded_entry_ids, memory_exclusions_confirmed=memory_exclusions_confirmed))
    if bool(controls.get("journalWeeklyEnabled", True)):
        target_monday = _pending_week_monday(db_path, guild_id, now_utc)
        if target_monday is not None:
            results.append(run_weekly(db_path, guild_id, generator, base_url, api_key, now_utc=now_utc, target_monday=target_monday, opener=opener, memory_excluded_entry_ids=memory_excluded_entry_ids, memory_exclusions_confirmed=memory_exclusions_confirmed))
    return results or [AutomationResult(False, "all", "not_due", "no_schedule_due")]


def automation_status(db_path: str, guild_id: int) -> dict[str, Any]:
    ensure_schema(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        state = conn.execute("SELECT * FROM bnl_journal_automation_state WHERE guild_id=?", (guild_id,)).fetchone()
        runs = conn.execute("""SELECT cadence,lifecycle_state,reason,journal_entry_id,journal_revision,
            source_window_start,source_window_end,aggregate_counts_json,updated_at
            FROM bnl_journal_automation_runs WHERE guild_id=? ORDER BY updated_at DESC LIMIT 10""", (guild_id,)).fetchall()
    now = datetime.now(timezone.utc)
    daily_start, daily_end, _ = daily_period(now)
    weekly_start, weekly_end, _ = weekly_period(now)
    next_daily_at, next_weekly_at = next_schedule_times(now)
    return {
        "contractVersion": 1,
        "guildId": guild_id,
        "schedulerState": "ready",
        "dailyWindow": {"start": daily_start, "end": daily_end},
        "weeklyWindow": {"start": weekly_start, "end": weekly_end},
        "nextDailyAt": next_daily_at,
        "nextWeeklyAt": next_weekly_at,
        "lastState": dict(state) if state else None,
        "recentRuns": [dict(row, aggregate_counts_json=None, aggregateCounts=json.loads(row["aggregate_counts_json"] or "{}")) for row in runs],
    }


def result_dict(result: AutomationResult) -> dict[str, Any]:
    return asdict(result)
