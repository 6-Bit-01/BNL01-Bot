from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import asdict, dataclass
from datetime import date, datetime, time as datetime_time, timedelta, timezone
from typing import Any, Callable, Optional

import pytz

from bnl_journal import (
    JournalResult,
    approve_draft,
    build_source_packet_between,
    deliver_approved,
    generate_and_store_packet_draft,
    journal_topic_counts,
    utc_now_iso,
)

PACIFIC = pytz.timezone("US/Pacific")
DAILY_READY_HOUR = 19
DAILY_READY_MINUTE = 0
WEEKLY_READY_WEEKDAY = 0  # Monday, covering the prior Monday-Sunday week.
WEEKLY_READY_HOUR = 19
MIN_DAILY_SOURCE_COUNT = 5
MIN_WEEKLY_ACTIVE_DAYS = 1
LEASE_MINUTES = 30
RETRY_MINUTES = 60
TERMINAL_RUN_STATES = {"published", "quiet", "incomplete"}


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
    with sqlite3.connect(db_path) as conn:
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
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(guild_id,cadence,source_window_start,source_window_end))""")
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
            updated_at TEXT NOT NULL)""")


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


def _claim_run(db_path: str, guild_id: int, cadence: str, start: str, end: str, *, force: bool = False) -> tuple[str, str, dict[str, Any]]:
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
                return "complete", run_id, current
            if current["lifecycle_state"] in {"held", "delivery_failed", "deferred"} and not force:
                updated_at = str(current.get("updated_at") or "")
                if updated_at:
                    retry_at = _parse_utc(updated_at) + timedelta(minutes=RETRY_MINUTES)
                    if retry_at > now:
                        current["retry_at"] = _utc_iso(retry_at)
                        conn.commit()
                        return "backoff", run_id, current
            lease_expires = current.get("lease_expires_at") or ""
            # A manual Run Now may bypass retry backoff, but it must never
            # steal a live lease from the scheduler or another operator.
            if current["lifecycle_state"] == "running" and lease_expires and _parse_utc(lease_expires) > now:
                conn.commit()
                return "busy", run_id, current
            conn.execute("""UPDATE bnl_journal_automation_runs
                SET lifecycle_state='running',reason='',attempt_count=attempt_count+1,lease_expires_at=?,updated_at=?
                WHERE run_id=?""", (lease, utc_now_iso(), run_id))
        else:
            now_iso = utc_now_iso()
            conn.execute("""INSERT INTO bnl_journal_automation_runs(
                run_id,guild_id,cadence,source_window_start,source_window_end,lifecycle_state,reason,
                aggregate_counts_json,attempt_count,lease_expires_at,created_at,updated_at
            ) VALUES(?,?,?,?,?,'running','', '{}',1,?,?,?)""", (
                run_id, guild_id, cadence, start, end, lease, now_iso, now_iso,
            ))
        conn.commit()
    return "claimed", run_id, {}


def _finish_run(db_path: str, run_id: str, result: AutomationResult) -> AutomationResult:
    retry_at = ""
    if result.status in {"held", "delivery_failed", "deferred"}:
        retry_at = _utc_iso(datetime.now(timezone.utc) + timedelta(minutes=RETRY_MINUTES))
    with sqlite3.connect(db_path) as conn:
        with conn:
            conn.execute("""UPDATE bnl_journal_automation_runs SET lifecycle_state=?,reason=?,journal_entry_id=?,
                journal_revision=?,aggregate_counts_json=?,lease_expires_at=NULL,updated_at=? WHERE run_id=?""", (
                result.status, result.reason, result.entry_id or None, int(result.revision or 0),
                _json(result.aggregate_counts or {}), utc_now_iso(), run_id,
            ))
            _update_state(conn, int(conn.execute("SELECT guild_id FROM bnl_journal_automation_runs WHERE run_id=?", (run_id,)).fetchone()[0]), result, next_retry_at=retry_at)
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
    return AutomationResult(
        status == "published",
        cadence,
        status,
        reason,
        str(row.get("journal_entry_id") or ""),
        int(row.get("journal_revision") or 0),
        str(row.get("source_window_start") or ""),
        str(row.get("source_window_end") or ""),
        counts,
    )


def _topic_counts(sources: list[dict[str, Any]], limit: int = 30) -> dict[str, int]:
    return journal_topic_counts(sources, limit=limit)


def _store_observation(db_path: str, guild_id: int, label: str, packet: dict[str, Any], state: str = "observed") -> str:
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
    return observation_id


def _mark_observation(db_path: str, guild_id: int, label: str, result: AutomationResult) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute("""UPDATE bnl_journal_observations SET lifecycle_state=?,journal_entry_id=?,journal_revision=?,updated_at=?
            WHERE guild_id=? AND observation_date=?""", (
            result.status, result.entry_id or None, int(result.revision or 0), utc_now_iso(), guild_id, label,
        ))


def _has_meaningful_daily_activity(packet: dict[str, Any]) -> bool:
    counts = packet.get("aggregateCounts") or {}
    total = int(counts.get("eligibleRelays") or 0) + int(counts.get("eligibleConversations") or 0)
    if total < MIN_DAILY_SOURCE_COUNT:
        return False
    if int(counts.get("eligibleConversations") or 0) > 0:
        return True
    quiet_markers = {"quiet", "quiet_source", "non_event_stock", "heartbeat", "hydrated"}
    return any(str(source.get("eventType") or "").lower() not in quiet_markers for source in packet.get("privateSources", []))


def _publish_packet(
    db_path: str,
    guild_id: int,
    cadence: str,
    label: str,
    packet: dict[str, Any],
    generator: Callable[[dict[str, Any], str], str],
    base_url: str,
    api_key: str,
    *,
    opener=None,
) -> AutomationResult:
    entry_id = _entry_id(guild_id, cadence, label)
    if not base_url or not api_key:
        return AutomationResult(False, cadence, "held", "website_configuration_missing", entry_id, 0, packet["sourceWindowStart"], packet["sourceWindowEnd"], packet.get("aggregateCounts"))
    with sqlite3.connect(db_path) as conn:
        existing = conn.execute("""SELECT revision,lifecycle_state,content_hash FROM bnl_journal_entries
            WHERE guild_id=? AND entry_id=? ORDER BY revision DESC LIMIT 1""", (guild_id, entry_id)).fetchone()
    if existing and existing[1] in {"approved_pending_delivery", "delivery_failed"}:
        delivered = deliver_approved(db_path, guild_id, entry_id, base_url, api_key, opener=opener)
        return AutomationResult(delivered.ok, cadence, delivered.status, delivered.reason, entry_id, delivered.revision, packet["sourceWindowStart"], packet["sourceWindowEnd"], packet.get("aggregateCounts"))
    if existing and existing[1] == "published":
        return AutomationResult(True, cadence, "published", "already_published", entry_id, int(existing[0]), packet["sourceWindowStart"], packet["sourceWindowEnd"], packet.get("aggregateCounts"))
    if existing and existing[1] == "draft":
        approved = approve_draft(db_path, guild_id, entry_id, existing[2], revision=int(existing[0]))
        if not approved.ok:
            return AutomationResult(False, cadence, "held", approved.reason, entry_id, int(existing[0]), packet["sourceWindowStart"], packet["sourceWindowEnd"], packet.get("aggregateCounts"))
        delivered = deliver_approved(db_path, guild_id, entry_id, base_url, api_key, opener=opener, revision=int(existing[0]))
        return AutomationResult(delivered.ok, cadence, delivered.status, delivered.reason, entry_id, delivered.revision, packet["sourceWindowStart"], packet["sourceWindowEnd"], packet.get("aggregateCounts"))
    generated = generate_and_store_packet_draft(db_path, guild_id, packet, generator, entry_id=entry_id)
    if not generated.ok:
        return AutomationResult(False, cadence, "held", generated.reason, entry_id, generated.revision, packet["sourceWindowStart"], packet["sourceWindowEnd"], packet.get("aggregateCounts"))
    approved = approve_draft(db_path, guild_id, entry_id, generated.content_hash, revision=generated.revision)
    if not approved.ok:
        return AutomationResult(False, cadence, "held", approved.reason, entry_id, generated.revision, packet["sourceWindowStart"], packet["sourceWindowEnd"], packet.get("aggregateCounts"))
    delivered = deliver_approved(db_path, guild_id, entry_id, base_url, api_key, opener=opener, revision=generated.revision)
    return AutomationResult(delivered.ok, cadence, delivered.status, delivered.reason, entry_id, delivered.revision, packet["sourceWindowStart"], packet["sourceWindowEnd"], packet.get("aggregateCounts"))


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
) -> AutomationResult:
    ensure_schema(db_path)
    start, end, label = _daily_period_for_day(target_day) if target_day else daily_period(now_utc)
    if not force and target_day is None and not daily_due(now_utc):
        return AutomationResult(False, "daily", "not_due", "before_daily_publish_time", source_window_start=start, source_window_end=end)
    claim, run_id, row = _claim_run(db_path, guild_id, "daily", start, end, force=force)
    if claim != "claimed":
        return _result_from_existing("daily", row, claim=claim)
    packet = build_source_packet_between(
        db_path,
        guild_id,
        start,
        end,
        entry_kind="daily",
        excluded_history_entry_ids=memory_excluded_entry_ids,
    )
    _store_observation(db_path, guild_id, label, packet)
    if not packet.get("sourceArchiveAvailable", False):
        result = AutomationResult(False, "daily", "held", "source_archive_unavailable", source_window_start=start, source_window_end=end, aggregate_counts=packet.get("aggregateCounts"))
    elif not packet.get("coverageComplete", True):
        # Time cannot make a pre-archive window complete. Record it explicitly
        # and move on instead of blocking every later catch-up day forever.
        result = AutomationResult(False, "daily", "incomplete", "window_began_before_archive_activation", source_window_start=start, source_window_end=end, aggregate_counts=packet.get("aggregateCounts"))
    elif not _has_meaningful_daily_activity(packet):
        result = AutomationResult(True, "daily", "quiet", "insufficient_meaningful_activity", source_window_start=start, source_window_end=end, aggregate_counts=packet.get("aggregateCounts"))
    else:
        result = _publish_packet(db_path, guild_id, "daily", label, packet, generator, base_url, api_key, opener=opener)
    _mark_observation(db_path, guild_id, label, result)
    return _finish_run(db_path, run_id, result)


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
) -> AutomationResult:
    ensure_schema(db_path)
    start, end, label = _weekly_period_for_monday(target_monday) if target_monday else weekly_period(now_utc)
    if not force and target_monday is None and not weekly_due(now_utc):
        return AutomationResult(False, "weekly", "not_due", "before_weekly_publish_time", source_window_start=start, source_window_end=end)
    claim, run_id, row = _claim_run(db_path, guild_id, "weekly", start, end, force=force)
    if claim != "claimed":
        return _result_from_existing("weekly", row, claim=claim)
    packet, complete_days, active_days = _weekly_packet(
        db_path,
        guild_id,
        start,
        end,
        memory_excluded_entry_ids=memory_excluded_entry_ids,
    )
    if packet is None and complete_days == 7 and active_days == 0:
        result = AutomationResult(True, "weekly", "quiet", "no_meaningful_weekly_activity", source_window_start=start, source_window_end=end, aggregate_counts={"completeDays": complete_days, "activeDays": active_days})
    elif packet is None:
        result = AutomationResult(True, "weekly", "deferred", f"only_{complete_days}_complete_daily_observations", source_window_start=start, source_window_end=end, aggregate_counts={"completeDays": complete_days, "activeDays": active_days})
    elif not packet.get("sourceArchiveAvailable", False):
        result = AutomationResult(False, "weekly", "held", "source_archive_unavailable", source_window_start=start, source_window_end=end, aggregate_counts=packet.get("aggregateCounts"))
    elif not packet.get("coverageComplete", True):
        result = AutomationResult(False, "weekly", "incomplete", "window_began_before_archive_activation", source_window_start=start, source_window_end=end, aggregate_counts=packet.get("aggregateCounts"))
    else:
        result = _publish_packet(db_path, guild_id, "weekly", label, packet, generator, base_url, api_key, opener=opener)
    return _finish_run(db_path, run_id, result)


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
            results.append(run_daily(db_path, guild_id, generator, base_url, api_key, now_utc=now_utc, target_day=target_day, opener=opener, memory_excluded_entry_ids=memory_excluded_entry_ids))
    if bool(controls.get("journalWeeklyEnabled", True)):
        target_monday = _pending_week_monday(db_path, guild_id, now_utc)
        if target_monday is not None:
            results.append(run_weekly(db_path, guild_id, generator, base_url, api_key, now_utc=now_utc, target_monday=target_monday, opener=opener, memory_excluded_entry_ids=memory_excluded_entry_ids))
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
