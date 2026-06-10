"""Durable Source File auto-refresh queue helpers for BNL.

The queue is review-only automation: it marks Source File subjects dirty when
meaningful local evidence changes, batches duplicate signals, and reuses the
existing Source File enrichment/send path under cooldown/rate-limit controls.
It does not publish, merge identities, connect queue payments, or create artist
accounts.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import re
import socket
import sqlite3
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from bnl_dossier_recommendations import (
    get_dossier_ingest_url,
    get_source_file_archive_url,
    is_dossier_ingest_token_configured,
    is_source_file_archive_token_configured,
    select_trusted_site_callback_base_url,
    send_dossier_recommendation,
    _safe_url_host as _safe_callback_host,
)
from bnl_source_file_enrichment import run_source_file_enrichment
from bnl_source_file_lookup import lookup_source_file
from bnl_source_refresh_context import is_refresh_generation_subject, refresh_generation_context

QUEUE_TABLE = "source_file_refresh_queue"
STATE_TABLE = "source_file_refresh_state"
ACTIVE_QUEUE_STATUSES = {"queued", "deferred", "cooldown", "running", "failed"}
TERMINAL_QUEUE_STATUSES = {"succeeded", "skipped"}
VALID_STATUSES = ACTIVE_QUEUE_STATUSES | TERMINAL_QUEUE_STATUSES
VALID_REFRESH_MODES = {"automatic", "manual", "dry_run", "operator_requested", "site_open_request", "site_manual_request"}
CASE_REPORT_MISSING_REASON = "case_report_missing"
CASE_REPORT_BACKFILL_REASON = "case_report_backfill"
DEFAULT_COOLDOWN_MINUTES = 30
DEFAULT_PROCESS_INTERVAL_MINUTES = 15
DEFAULT_MAX_AUTOMATIC_PER_CYCLE = 3
DEFAULT_MAX_SITE_REQUESTS_PER_CYCLE = 3
SITE_REFRESH_REQUEST_TIMEOUT_SECONDS = 8
SOURCE_REFRESH_NOW_PATH = "/internal/source-files/refresh-now"
SOURCE_REFRESH_NOW_TOKEN_HEADER = "X-BNL-REFRESH-TOKEN"
SITE_REFRESH_REQUEST_PATH = "/api/bnl/source-files/refresh-requests"
MEANINGFUL_MIN_TEXT_LENGTH = 16

_URL_RE = re.compile(r"https?://\S+|\b(?:soundcloud|spotify|bandcamp|youtube|youtu\.be|instagram|tiktok|linktree|discord\.gg)/", re.I)
_MEANINGFUL_TOPIC_RE = re.compile(
    r"\b(?:source\s*file|dossier|profile|artist|track|song|album|ep|mix|collab(?:oration)?|feature|project|contest|event|show|episode|lineup|submission|queue|persona|relationship|member|role|mod|admin|note|link|platform|release|bio)\b",
    re.I,
)
_GENERIC_SMALL_TALK_RE = re.compile(r"^(?:hi|hey|hello|yo|sup|thanks|thank you|ok|okay|lol|lmao|haha|good morning|good night|gn|gm|yes|no|nice|cool)[.!?\s]*$", re.I)
_PRIVATE_POLICIES = {"private", "dm", "sealed_test", "internal_controlled", "private_internal"}


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def parse_iso(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def source_refresh_subject_key(value: str) -> str:
    from bnl_source_refresh_context import source_refresh_subject_key as _context_subject_key

    return _context_subject_key(value)


def _safe_text(value: Any, limit: int = 180) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    text = re.sub(r"<@!?\d+>|<@&\d+>|<#\d+>", "[redacted-mention]", text)
    text = re.sub(r"\b\d{15,22}\b", "[redacted-id]", text)
    return text[:limit]


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}
    except Exception:
        return set()


def ensure_source_file_refresh_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {QUEUE_TABLE} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            guild_id INTEGER,
            subject_key TEXT NOT NULL,
            subject_name TEXT NOT NULL,
            reason TEXT NOT NULL,
            evidence_source TEXT,
            evidence_count INTEGER NOT NULL DEFAULT 1,
            priority INTEGER NOT NULL DEFAULT 50,
            status TEXT NOT NULL DEFAULT 'queued',
            queued_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            not_before_at TEXT,
            last_attempt_at TEXT,
            attempts INTEGER NOT NULL DEFAULT 0,
            last_error TEXT,
            refresh_mode TEXT NOT NULL DEFAULT 'automatic',
            created_by TEXT
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {STATE_TABLE} (
            guild_id INTEGER,
            subject_key TEXT NOT NULL,
            subject_name TEXT NOT NULL,
            last_refresh_started_at TEXT,
            last_refresh_completed_at TEXT,
            last_refresh_status TEXT,
            last_recommendation_id TEXT,
            last_evidence_hash TEXT,
            last_evidence_count INTEGER NOT NULL DEFAULT 0,
            last_failure_at TEXT,
            last_error TEXT,
            cooldown_until TEXT,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (guild_id, subject_key)
        )
        """
    )
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_source_refresh_queue_status ON {QUEUE_TABLE} (status, not_before_at, priority, queued_at)")
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_source_refresh_queue_subject ON {QUEUE_TABLE} (guild_id, subject_key, status)")
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_source_refresh_state_subject ON {STATE_TABLE} (guild_id, subject_key)")


def ensure_source_file_refresh_db(db_path: str) -> None:
    conn = sqlite3.connect(db_path)
    try:
        ensure_source_file_refresh_schema(conn)
        conn.commit()
    finally:
        conn.close()


def classify_refresh_evidence(*, subject_name: str | None, content: str = "", evidence_source: str = "", channel_policy: str = "unknown", source_scope: str = "", authority: str = "", visibility: str = "", evidence_type: str = "", relation_to_subject: str = "", is_bot_generated: bool = False, duplicate: bool = False) -> dict[str, Any]:
    subject = _safe_text(subject_name, 90)
    if not subject:
        return {"meaningful": False, "reason": "missing_subject"}
    if is_bot_generated:
        return {"meaningful": False, "reason": "bot_generated"}
    if duplicate:
        return {"meaningful": False, "reason": "duplicate"}
    text = str(content or "")
    if _GENERIC_SMALL_TALK_RE.match(text.strip()):
        return {"meaningful": False, "reason": "generic_small_talk"}
    policy = (channel_policy or "unknown").lower()
    vis = (visibility or "").lower()
    source = (evidence_source or "").lower()
    scope = (source_scope or "").lower()
    kind = (evidence_type or "").lower()
    relation = (relation_to_subject or "").lower()
    auth = (authority or "").lower()
    public_or_reviewable = policy not in _PRIVATE_POLICIES or vis in {"public", "public_safe", "review_only", "internal_review_only"} or auth in {"operator_note", "owner_admin", "local_observed"}
    direct_subject_basis = relation in {"authored", "self_reported", "direct", "subject_keyed"} or scope in {"subject_keyed", "subject_authored", "subject_direct_evidence", "subject_owned"}
    topic_signal = bool(_MEANINGFUL_TOPIC_RE.search(text) or _URL_RE.search(text) or kind in {"link", "platform", "relationship", "project", "music", "event", "contest", "bnl_interaction", "source_question", "manual_note", "community_presence"})
    if source == "entity_evidence_events" and subject and public_or_reviewable:
        return {"meaningful": True, "reason": f"entity_evidence:{kind or 'subject_keyed'}", "priority": 80}
    if source == "community_presence" and public_or_reviewable and topic_signal:
        return {"meaningful": True, "reason": "community_presence_threshold", "priority": 45}
    if direct_subject_basis and public_or_reviewable and topic_signal and len(text.strip()) >= MEANINGFUL_MIN_TEXT_LENGTH:
        if _URL_RE.search(text):
            return {"meaningful": True, "reason": "subject_activity_link_signal", "priority": 70}
        return {"meaningful": True, "reason": "subject_activity_meaningful_signal", "priority": 55}
    return {"meaningful": False, "reason": "insufficient_refresh_signal"}


def mark_subject_dirty_for_evidence(db_path: str, *, guild_id: int | None, subject_name: str, evidence_source: str, content: str = "", channel_policy: str = "unknown", source_scope: str = "", authority: str = "", visibility: str = "", evidence_type: str = "", relation_to_subject: str = "", created_by: str = "auto_evidence", not_before_at: str | None = None) -> dict[str, Any]:
    decision = classify_refresh_evidence(
        subject_name=subject_name,
        content=content,
        evidence_source=evidence_source,
        channel_policy=channel_policy,
        source_scope=source_scope,
        authority=authority,
        visibility=visibility,
        evidence_type=evidence_type,
        relation_to_subject=relation_to_subject,
    )
    skey = source_refresh_subject_key(subject_name)
    if is_refresh_generation_subject(skey):
        logging.info("source_refresh_skipped subject_key=%s reason=refresh_self_generated_evidence", skey)
        return {"ok": False, "queued": False, "status": "skipped", "reason": "refresh_self_generated_evidence", "subject_key": skey}
    if not decision.get("meaningful"):
        logging.info("source_refresh_skipped subject_key=%s reason=%s", skey, decision.get("reason"))
        return {"ok": False, "queued": False, "status": "skipped", "reason": decision.get("reason"), "subject_key": skey}
    logging.info("source_refresh_mark_dirty subject_key=%s reason=%s", skey, decision.get("reason"))
    return enqueue_source_file_refresh(
        db_path,
        guild_id=guild_id,
        subject_name=subject_name,
        reason=str(decision.get("reason") or "meaningful_evidence"),
        evidence_source=evidence_source,
        priority=int(decision.get("priority") or 50),
        refresh_mode="automatic",
        created_by=created_by,
        not_before_at=not_before_at,
    )


def enqueue_source_file_refresh(db_path: str, *, guild_id: int | None, subject_name: str, reason: str, evidence_source: str = "unknown", priority: int = 50, refresh_mode: str = "automatic", created_by: str = "system", not_before_at: str | None = None, evidence_count: int = 1) -> dict[str, Any]:
    subject = _safe_text(subject_name, 90)
    skey = source_refresh_subject_key(subject)
    mode = refresh_mode if refresh_mode in VALID_REFRESH_MODES else "automatic"
    now = utc_now()
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        ensure_source_file_refresh_schema(conn)
        existing = conn.execute(
            f"SELECT * FROM {QUEUE_TABLE} WHERE (guild_id IS ? OR guild_id=?) AND subject_key=? AND status IN ('queued','deferred','cooldown','failed','running') ORDER BY id DESC LIMIT 1",
            (guild_id, guild_id, skey),
        ).fetchone()
        if existing:
            new_count = int(existing["evidence_count"] or 0) + max(1, int(evidence_count or 1))
            new_priority = max(int(existing["priority"] or 0), int(priority or 0))
            merged_reason = _safe_text(f"{existing['reason']}; {reason}", 260)
            status = "queued" if existing["status"] in {"failed", "deferred", "cooldown"} else existing["status"]
            conn.execute(
                f"UPDATE {QUEUE_TABLE} SET reason=?, evidence_source=?, evidence_count=?, priority=?, status=?, updated_at=?, not_before_at=COALESCE(?, not_before_at), refresh_mode=?, created_by=? WHERE id=?",
                (merged_reason, _safe_text(evidence_source, 80), new_count, new_priority, status, now, not_before_at, mode, _safe_text(created_by, 80), existing["id"]),
            )
            conn.commit()
            logging.info("source_refresh_queued subject_key=%s priority=%s", skey, new_priority)
            return {"ok": True, "queued": True, "deduped": True, "id": existing["id"], "subject_key": skey, "status": status, "evidence_count": new_count}
        cur = conn.execute(
            f"""
            INSERT INTO {QUEUE_TABLE} (guild_id, subject_key, subject_name, reason, evidence_source, evidence_count, priority, status, queued_at, updated_at, not_before_at, attempts, refresh_mode, created_by)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'queued', ?, ?, ?, 0, ?, ?)
            """,
            (guild_id, skey, subject, _safe_text(reason, 260), _safe_text(evidence_source, 80), max(1, int(evidence_count or 1)), int(priority or 50), now, now, not_before_at, mode, _safe_text(created_by, 80)),
        )
        conn.commit()
        logging.info("source_refresh_queued subject_key=%s priority=%s", skey, int(priority or 50))
        return {"ok": True, "queued": True, "deduped": False, "id": cur.lastrowid, "subject_key": skey, "status": "queued", "evidence_count": max(1, int(evidence_count or 1))}
    finally:
        conn.close()


def list_source_file_refresh_queue(db_path: str, *, guild_id: int | None = None, limit: int = 20) -> list[dict[str, Any]]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        ensure_source_file_refresh_schema(conn)
        where = "WHERE status IN ('queued','deferred','cooldown','running','failed')"
        params: list[Any] = []
        if guild_id is not None:
            where += " AND guild_id=?"
            params.append(guild_id)
        rows = conn.execute(f"SELECT * FROM {QUEUE_TABLE} {where} ORDER BY priority DESC, queued_at ASC LIMIT ?", [*params, max(1, limit)]).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def get_refresh_state(db_path: str, guild_id: int | None, subject_key: str) -> dict[str, Any] | None:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        ensure_source_file_refresh_schema(conn)
        row = conn.execute(f"SELECT * FROM {STATE_TABLE} WHERE (guild_id IS ? OR guild_id=?) AND subject_key=? LIMIT 1", (guild_id, guild_id, subject_key)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def clear_source_file_refresh(db_path: str, *, guild_id: int | None, subject_name: str) -> int:
    skey = source_refresh_subject_key(subject_name)
    conn = sqlite3.connect(db_path)
    try:
        ensure_source_file_refresh_schema(conn)
        cur = conn.execute(f"UPDATE {QUEUE_TABLE} SET status='skipped', updated_at=?, last_error='cleared_by_operator' WHERE (guild_id IS ? OR guild_id=?) AND subject_key=? AND status IN ('queued','deferred','cooldown','failed','running')", (utc_now(), guild_id, guild_id, skey))
        conn.commit()
        logging.info("source_refresh_skipped subject_key=%s reason=cleared_by_operator", skey)
        return int(cur.rowcount or 0)
    finally:
        conn.close()



def _configured_site_refresh_requests_url(environ: dict[str, str] | None = None) -> str:
    """Return the configured site refresh-request endpoint, or an empty string.

    Polling is intentionally opt-in: unlike read-only lookup, the worker does
    not fall back to the production website unless a site endpoint/base URL is
    configured in the environment.
    """

    env = environ if environ is not None else os.environ
    explicit = (env.get("BNL_SOURCE_FILE_REFRESH_REQUESTS_URL") or "").strip()
    if explicit:
        return explicit
    base = (env.get("BNL_WEBSITE_BASE_URL") or env.get("BNL_SITE_BASE_URL") or "").strip()
    if base:
        return urllib.parse.urljoin(base.rstrip("/") + "/", SITE_REFRESH_REQUEST_PATH.lstrip("/"))
    read_url = (env.get("BNL_SOURCE_FILE_READ_URL") or "").strip()
    if read_url:
        parsed = urllib.parse.urlparse(read_url)
        root = urllib.parse.urlunparse((parsed.scheme, parsed.netloc, "", "", "", ""))
        return urllib.parse.urljoin(root.rstrip("/") + "/", SITE_REFRESH_REQUEST_PATH.lstrip("/"))
    return ""


def _site_refresh_request_id(request: dict[str, Any]) -> str:
    return _safe_text(request.get("id") or request.get("requestId") or request.get("request_id") or "", 120)


def _site_refresh_subject_name(request: dict[str, Any]) -> str:
    return _safe_text(request.get("subjectName") or request.get("subject_name") or request.get("subject") or request.get("normalizedSubjectKey") or request.get("subjectKey") or "", 90)


def _site_refresh_subject_key(request: dict[str, Any]) -> str:
    key = _safe_text(request.get("normalizedSubjectKey") or request.get("subjectKey") or request.get("subject_key") or "", 120)
    return source_refresh_subject_key(key or _site_refresh_subject_name(request))


def _site_refresh_mode(request: dict[str, Any]) -> str:
    raw = str(request.get("refreshMode") or request.get("refresh_mode") or request.get("requestType") or request.get("type") or "").lower()
    source = str(request.get("source") or "").lower()
    reason = str(request.get("reason") or "").lower()
    high_priority = bool(request.get("highPriority") or request.get("manual") or request.get("force"))
    if source in {"admin_manual_retry"} or reason in {"manual_retry", "file_not_updated"}:
        high_priority = True
    return "site_manual_request" if high_priority or "manual" in raw or "button" in raw else "site_open_request"



def is_source_refresh_now_token_valid(provided_token: str | None, *, environ: dict[str, str] | None = None) -> bool:
    """Return True only when the internal immediate-refresh token matches env config."""

    env = environ if environ is not None else os.environ
    expected = (env.get("BNL_SOURCE_FILE_REFRESH_TOKEN") or "").strip()
    provided = (provided_token or "").strip()
    if not expected or not provided:
        return False
    return hmac.compare_digest(provided, expected)


def _site_refresh_lookup_parts(site_request: dict[str, Any]) -> tuple[str, str]:
    candidate_id = _safe_text(site_request.get("candidateId") or site_request.get("candidate_id") or "", 120)
    subject = _site_refresh_subject_name(site_request)
    skey = _site_refresh_subject_key(site_request)
    if candidate_id:
        return "candidateId", candidate_id
    if (site_request.get("normalizedSubjectKey") or site_request.get("subjectKey")) and not site_request.get("subjectName"):
        return "normalizedName", str(site_request.get("normalizedSubjectKey") or site_request.get("subjectKey") or skey)
    return "subject", str(subject or skey)


def _site_status_for_local_item(item: dict[str, Any]) -> tuple[str, str]:
    status = str(item.get("status") or "").lower()
    if status in {"succeeded", "current", "already_current", "partial_success"}:
        reason = _safe_text(item.get("reason") or "", 240) if status == "partial_success" else ""
        return "completed", reason
    if status in {"skipped", "cooldown", "deferred", "no_target", "dry_run"}:
        return "skipped", _safe_text(item.get("reason") or item.get("error") or status, 240)
    return "failed", _safe_text(item.get("error") or item.get("reason") or status or "send_failed", 240)


def _mark_site_request_terminal(request_id: str, item: dict[str, Any], *, environ: dict[str, str], opener=None) -> dict[str, Any]:
    site_status, reason = _site_status_for_local_item(item)
    rec_id = _safe_text(item.get("recommendationId") or "", 120)
    result = update_site_refresh_request_status(
        request_id,
        site_status,
        environ=environ,
        opener=opener,
        completed_by_recommendation_id=rec_id if site_status == "completed" else None,
        failure_reason=reason if site_status != "completed" else None,
    )
    logging.info("source_refresh_now_site_status_updated request_id=%s status=%s ok=%s", _safe_text(request_id, 120), site_status, bool(result.get("ok")))
    return result


def _state_has_current_success(state: dict[str, Any] | sqlite3.Row | None) -> bool:
    if not state:
        return False
    if isinstance(state, sqlite3.Row):
        status_value = state["last_refresh_status"]
        completed_value = state["last_refresh_completed_at"]
        cooldown_value = state["cooldown_until"]
    else:
        status_value = state.get("last_refresh_status")
        completed_value = state.get("last_refresh_completed_at")
        cooldown_value = state.get("cooldown_until")
    status = str(status_value or "").lower()
    completed = parse_iso(completed_value)
    cooldown_until = parse_iso(cooldown_value)
    now_dt = datetime.now(timezone.utc).replace(microsecond=0)
    return status in {"succeeded", "partial_success"} and completed is not None and cooldown_until is not None and cooldown_until > now_dt


def _archive_candidate_dicts(value: Any) -> list[dict[str, Any]]:
    """Return archive/source-package dictionaries that are rich enough to test.

    Plain Source File identity objects are intentionally ignored. The backfill
    trigger is only for existing archive/source-package evidence that predates
    the case-report fields; a bare lookup match must not pretend report evidence
    exists or force regeneration by itself.
    """

    candidates: list[dict[str, Any]] = []
    seen: set[int] = set()
    archive_keys = {
        "archive",
        "latestArchive",
        "latest_archive",
        "archivePayload",
        "archive_payload",
        "sourcePackage",
        "source_package",
        "package",
        "enrichmentArchive",
        "sourceFileArchive",
    }
    archive_markers = {
        "compactSummary",
        "evidenceReceiptSummary",
        "sourceFileBriefV2",
        "sourceCounts",
        "sourceTypes",
        "qualityScore",
        "archiveNotice",
    }

    def add(candidate: dict[str, Any]) -> None:
        ident = id(candidate)
        if ident not in seen:
            seen.add(ident)
            candidates.append(candidate)

    def walk(node: Any, *, forced: bool = False) -> None:
        if isinstance(node, dict):
            if forced or archive_markers.intersection(node.keys()) or {"subjectMemoryPacketV1", "sourceFileCaseReportV1"}.intersection(node.keys()):
                add(node)
            for key, child in node.items():
                walk(child, forced=forced or str(key) in archive_keys)
        elif isinstance(node, list):
            for child in node:
                walk(child, forced=forced)

    walk(value)
    return candidates


def source_file_report_missing(latest_archive_or_lookup: Any) -> bool:
    """Detect an existing Source File archive/package missing report fields."""

    candidates = _archive_candidate_dicts(latest_archive_or_lookup)
    if not candidates:
        return False
    newest = candidates[0]
    missing = not isinstance(newest.get("subjectMemoryPacketV1"), dict) or not isinstance(newest.get("sourceFileCaseReportV1"), dict)
    return bool(missing)


def archive_missing_case_report(latest_archive_or_lookup: Any) -> bool:
    """Compatibility alias for tests/callers that name the archive helper."""

    return source_file_report_missing(latest_archive_or_lookup)


def _lookup_source_file_report_missing(
    row: sqlite3.Row,
    *,
    lookup_func: Callable[[dict[str, str]], dict[str, Any]],
) -> bool:
    try:
        lookup_result = lookup_func({"lookupKey": "subject", "lookupValue": row["subject_name"], "subject": row["subject_name"]})
    except Exception as exc:
        logging.warning("source_case_report_backfill_failed subject_key=%s error=%s", row["subject_key"], _safe_text(exc, 160))
        return False
    missing = source_file_report_missing(lookup_result)
    if missing:
        logging.info("source_case_report_missing subject_key=%s reason=%s", row["subject_key"], CASE_REPORT_MISSING_REASON)
    return missing


def site_request_requires_case_report_backfill(site_request: dict[str, Any] | None) -> bool:
    """Return True when the website explicitly asks BNL to backfill a case report."""

    request = site_request if isinstance(site_request, dict) else {}
    if request.get("caseReportMissing") is True or request.get("requiresCaseReportBackfill") is True:
        return True
    reason = str(request.get("reason") or "").lower()
    return CASE_REPORT_MISSING_REASON in reason or CASE_REPORT_BACKFILL_REASON in reason


def _queue_reason_has_case_report_missing(reason: Any) -> bool:
    return CASE_REPORT_MISSING_REASON in str(reason or "") or CASE_REPORT_BACKFILL_REASON in str(reason or "")


def _case_report_backfill_trigger(*, explicit_site_request: bool = False, archive_lookup_missing: bool = False, queue_reason: Any = None, evidence_source: Any = None) -> str:
    if explicit_site_request:
        return "explicit_site_request"
    if archive_lookup_missing:
        return "archive_lookup_missing"
    reason = str(queue_reason or "")
    if CASE_REPORT_BACKFILL_REASON in reason and str(evidence_source or "") in {"source_refresh_now", "site_refresh_request"}:
        return "explicit_site_request"
    if _queue_reason_has_case_report_missing(reason):
        return "queue_reason"
    return "none"


def _select_and_log_source_refresh_callback_base(site_request: dict[str, Any], *, environ: dict[str, str]) -> tuple[str, str]:
    """Select and log the safe site callback target for Source File sends."""

    callback_base_url, source = select_trusted_site_callback_base_url(site_request, environ=environ)
    selected = callback_base_url or get_source_file_archive_url(environ)
    logging.info(
        "source_refresh_callback_base_selected source=%s host=%s",
        source,
        _safe_callback_host(selected) or "none",
    )
    return callback_base_url, source



def _result_case_report_generated(result: dict[str, Any]) -> bool:
    if "caseReportGenerated" in result:
        return bool(result.get("caseReportGenerated"))
    archive_payload = result.get("archivePayload") if isinstance(result.get("archivePayload"), dict) else {}
    return bool(result.get("sourceFileCaseReportV1") or archive_payload.get("sourceFileCaseReportV1"))


def _result_subject_memory_packet_generated(result: dict[str, Any]) -> bool:
    if "subjectMemoryPacketGenerated" in result:
        return bool(result.get("subjectMemoryPacketGenerated"))
    archive_payload = result.get("archivePayload") if isinstance(result.get("archivePayload"), dict) else {}
    return bool(result.get("subjectMemoryPacketV1") or archive_payload.get("subjectMemoryPacketV1"))


def _item_generation_status(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "caseReportGenerated": bool(item.get("caseReportGenerated")),
        "subjectMemoryPacketGenerated": bool(item.get("subjectMemoryPacketGenerated")),
        "caseReportBackfill": bool(item.get("caseReportBackfill")),
        "caseReportBackfillTrigger": _safe_text(item.get("caseReportBackfillTrigger") or "none", 80),
    }


def _mark_current_cooldown_terminal(db_path: str, *, guild_id: int | None, subject_key: str, reason: str = "cooldown_current") -> None:
    conn = sqlite3.connect(db_path)
    try:
        ensure_source_file_refresh_schema(conn)
        conn.execute(
            f"UPDATE {QUEUE_TABLE} SET status='skipped', updated_at=?, last_error=? WHERE (guild_id IS ? OR guild_id=?) AND subject_key=? AND status IN ('queued','deferred','cooldown','failed') AND evidence_source='source_refresh_now'",
            (utc_now(), reason, guild_id, guild_id, source_refresh_subject_key(subject_key)),
        )
        conn.commit()
    finally:
        conn.close()

def process_source_file_refresh_now(db_path: str, payload: dict[str, Any], *, guild_id: int | None = None, dry_run: bool = False, lookup_func: Callable[[dict[str, str]], dict[str, Any]] = lookup_source_file, sender: Callable[[dict[str, Any]], dict[str, Any]] = send_dossier_recommendation, environ: dict[str, str] | None = None, opener=None) -> dict[str, Any]:
    """Process one immediate website Source File refresh request.

    This is the bot-side wake-up path for admin Source File opens/retries. The
    existing polling worker remains independent fallback; this helper only
    processes the supplied request and makes any claimed site request terminal or
    retry-safe on no_target, cooldown, deferred, or send failures.
    """

    env = environ if environ is not None else os.environ
    site_request = dict(payload or {})
    request_id = _site_refresh_request_id(site_request)
    subject = _site_refresh_subject_name(site_request)
    skey = _site_refresh_subject_key(site_request)
    candidate_id = _safe_text(site_request.get("candidateId") or site_request.get("candidate_id") or "", 120)
    mode = _site_refresh_mode(site_request)
    source = _safe_text(site_request.get("source") or "", 80)
    reason = _safe_text(site_request.get("reason") or "", 80)
    explicit_case_report_backfill = site_request_requires_case_report_backfill(site_request)
    callback_base_url, callback_source = _select_and_log_source_refresh_callback_base(site_request, environ=env)
    logging.info("source_refresh_now_started request_id=%s subject_key=%s source=%s reason=%s", request_id or "none", skey or "none", source or "none", reason or "none")
    if explicit_case_report_backfill:
        logging.info("source_case_report_backfill_requested subject_key=%s trigger=%s reason=%s", skey or "none", "explicit_site_request", reason or CASE_REPORT_BACKFILL_REASON)

    summary: dict[str, Any] = {"ok": True, "dryRun": dry_run, "requestId": request_id, "candidateId": candidate_id, "subject": subject or skey, "subjectKey": skey, "status": "skipped", "recommendationId": "", "recommendationSent": False, "archiveSent": False, "archiveStatus": None, "archiveId": "", "archiveError": "", "failureReason": "", "callbackBaseSource": callback_source, "callbackBaseHost": _safe_callback_host(callback_base_url or get_source_file_archive_url(env))}
    if dry_run:
        summary.update({"status": "dry_run", "processed": 1})
        return summary
    if not (candidate_id or subject or skey):
        summary.update({"ok": False, "status": "failed", "failureReason": "missing_target"})
        return summary

    if request_id:
        claim = update_site_refresh_request_status(request_id, "claimed", environ=env, opener=opener)
        if not claim.get("ok"):
            failure = _safe_text(claim.get("reason") or claim.get("error") or "claim_failed", 160)
            logging.warning("source_refresh_now_failed request_id=%s reason=%s", request_id, failure)
            summary.update({"ok": False, "status": "failed", "failureReason": failure})
            return summary

    lookup_key, lookup_value = _site_refresh_lookup_parts(site_request)
    report_missing = bool(explicit_case_report_backfill)
    try:
        target_check = lookup_func({"lookupKey": lookup_key, "lookupValue": lookup_value, lookup_key: lookup_value})
        lookup_report_missing = source_file_report_missing(target_check)
        report_missing = bool(explicit_case_report_backfill or lookup_report_missing)
        if lookup_report_missing:
            logging.info("source_case_report_missing subject_key=%s reason=%s trigger=%s", skey or "none", CASE_REPORT_MISSING_REASON, "archive_lookup_missing")
        found = bool(target_check.get("found") or target_check.get("sourceFile") or target_check.get("source_file") or target_check.get("candidate") or target_check.get("dossier"))
        if target_check.get("ok") is False or not found:
            failure = "lookup_failed" if target_check.get("ok") is False else "no_target"
            if failure == "no_target":
                conn = sqlite3.connect(db_path)
                try:
                    ensure_source_file_refresh_schema(conn)
                    _state_upsert(conn, guild_id=guild_id, subject_key=skey, subject_name=subject or lookup_value, fields={"last_refresh_completed_at": utc_now(), "last_refresh_status": "no_target", "last_failure_at": utc_now(), "last_error": "no_target"})
                    conn.commit()
                finally:
                    conn.close()
            item = {"subject": subject or lookup_value, "status": "skipped" if failure == "no_target" else "failed", "reason": failure, "error": failure}
            if request_id:
                _mark_site_request_terminal(request_id, item, environ=env, opener=opener)
            summary.update({"ok": failure == "no_target", "status": "skipped" if failure == "no_target" else "failed", "failureReason": failure})
            logging.warning("source_refresh_now_failed request_id=%s reason=%s", request_id or "none", failure)
            return summary
    except Exception as exc:
        failure = _safe_text(exc, 160) or "lookup_failed"
        item = {"subject": subject or skey, "status": "failed", "error": failure}
        if request_id:
            _mark_site_request_terminal(request_id, item, environ=env, opener=opener)
        summary.update({"ok": False, "status": "failed", "failureReason": failure})
        logging.warning("source_refresh_now_failed request_id=%s reason=%s", request_id or "none", failure)
        return summary

    immediate_not_before = utc_now()
    enqueue_source_file_refresh(
        db_path,
        guild_id=guild_id,
        subject_name=subject or lookup_value,
        reason=CASE_REPORT_BACKFILL_REASON if explicit_case_report_backfill else (CASE_REPORT_MISSING_REASON if report_missing else f"source_refresh_now:{request_id or candidate_id or skey}"),
        evidence_source="source_refresh_now",
        priority=100,
        refresh_mode=mode,
        created_by=source or "source_refresh_now",
        not_before_at=immediate_not_before,
        evidence_count=1,
    )
    local = process_source_file_refresh_queue(
        db_path,
        guild_id=guild_id,
        dry_run=False,
        max_items=1,
        lookup_func=lookup_func,
        sender=sender,
        environ=env,
        bypass_cooldown=explicit_case_report_backfill or report_missing or source in {"admin_manual_retry"} or reason in {"manual_retry", "file_not_updated"},
        refresh_mode=mode,
        subject_key=skey,
        callback_base_url=callback_base_url or None,
    )
    item = (local.get("items") or [{}])[0]
    if not item:
        state = get_refresh_state(db_path, guild_id, skey)
        if _state_has_current_success(state):
            _mark_current_cooldown_terminal(db_path, guild_id=guild_id, subject_key=skey, reason="cooldown_current_no_item")
            item = {
                "subject": subject or lookup_value,
                "status": "current",
                "reason": "cooldown_current_no_item",
                "recommendationId": _safe_text(state.get("last_recommendation_id") or "", 120),
                "recommendationSent": bool(state.get("last_recommendation_id")),
            }
            local["items"] = [item]
            logging.info("source_refresh_now_current request_id=%s subject_key=%s reason=cooldown_current_no_item", request_id or "none", skey or "none")
        else:
            item = {"subject": subject or lookup_value, "status": "failed", "error": "refresh_not_processed"}
            local["items"] = [item]
    if str(item.get("status") or "").lower() == "cooldown":
        state = get_refresh_state(db_path, guild_id, skey)
        if _state_has_current_success(state):
            _mark_current_cooldown_terminal(db_path, guild_id=guild_id, subject_key=skey, reason="cooldown_current")
            item = {
                **item,
                "status": "current",
                "reason": "cooldown_current",
                "recommendationId": _safe_text(state.get("last_recommendation_id") or "", 120),
                "recommendationSent": bool(state.get("last_recommendation_id")),
            }
            local["items"] = [item, *(local.get("items") or [])[1:]]
            logging.info("source_refresh_now_current request_id=%s subject_key=%s reason=cooldown_current", request_id or "none", skey or "none")
    site_status, terminal_reason = _site_status_for_local_item(item)
    if request_id:
        _mark_site_request_terminal(request_id, item, environ=env, opener=opener)
    rec_id = _safe_text(item.get("recommendationId") or "", 120)
    archive_id = _safe_text(item.get("archiveId") or "", 120)
    summary.update({
        "ok": site_status in {"completed", "skipped"},
        "status": "partial_success" if str(item.get("status") or "").lower() == "partial_success" else ("success" if site_status == "completed" else site_status),
        "recommendationId": rec_id,
        "recommendationSent": bool(item.get("recommendationSent") or rec_id),
        "archiveSent": bool(item.get("archiveSent")),
        "archiveStatus": item.get("archiveStatus"),
        "archiveId": archive_id,
        "archiveError": _safe_text(item.get("archiveError") or "", 180),
        "caseReportGenerated": bool(item.get("caseReportGenerated")),
        "subjectMemoryPacketGenerated": bool(item.get("subjectMemoryPacketGenerated")),
        "caseReportBackfill": bool(item.get("caseReportBackfill")),
        "caseReportBackfillTrigger": _safe_text(item.get("caseReportBackfillTrigger") or "none", 80),
        "failureReason": terminal_reason,
        "local": local,
    })
    if site_status == "completed":
        logging.info("source_refresh_now_completed request_id=%s recommendation_id=%s", request_id or "none", rec_id or "none")
    else:
        logging.warning("source_refresh_now_failed request_id=%s reason=%s", request_id or "none", terminal_reason or site_status)
    return summary


def fetch_site_refresh_requests(*, environ: dict[str, str] | None = None, opener=None, limit: int = DEFAULT_MAX_SITE_REQUESTS_PER_CYCLE) -> dict[str, Any]:
    """Fetch pending Source File refresh requests from the website."""

    env = environ if environ is not None else os.environ
    url = _configured_site_refresh_requests_url(env)
    token = (env.get("BNL_SOURCE_FILE_READ_TOKEN") or "").strip()
    if not url or not token:
        reason = "not_configured" if not url else "missing_read_token"
        logging.info("source_refresh_site_poll_skipped reason=%s", reason)
        return {"ok": False, "configured": False, "reason": reason, "requests": []}
    logging.info("source_refresh_site_poll_started")
    separator = "&" if urllib.parse.urlparse(url).query else "?"
    poll_url = f"{url}{separator}{urllib.parse.urlencode({'status': 'pending', 'limit': max(1, int(limit or DEFAULT_MAX_SITE_REQUESTS_PER_CYCLE))})}"
    request = urllib.request.Request(
        poll_url,
        headers={"Accept": "application/json", "x-bnl-source-file-read-token": token},
        method="GET",
    )
    urlopen = opener.open if opener is not None else urllib.request.urlopen
    try:
        with urlopen(request, timeout=SITE_REFRESH_REQUEST_TIMEOUT_SECONDS) as response:
            status = getattr(response, "status", None) or response.getcode()
            raw = response.read().decode("utf-8")
    except (urllib.error.URLError, TimeoutError, socket.timeout) as exc:
        logging.warning("source_refresh_site_poll_skipped reason=network_or_timeout")
        return {"ok": False, "configured": True, "reason": "network_or_timeout", "error": _safe_text(exc, 160), "requests": []}
    except Exception as exc:
        logging.warning("source_refresh_site_poll_skipped reason=unexpected_error")
        return {"ok": False, "configured": True, "reason": "unexpected_error", "error": _safe_text(exc, 160), "requests": []}
    try:
        parsed = json.loads(raw) if raw.strip() else {}
    except json.JSONDecodeError:
        logging.warning("source_refresh_site_poll_skipped reason=invalid_json")
        return {"ok": False, "configured": True, "reason": "invalid_json", "status": status, "requests": []}
    requests = parsed.get("requests") if isinstance(parsed, dict) else parsed
    if not isinstance(requests, list):
        requests = []
    pending = [r for r in requests if isinstance(r, dict) and str(r.get("status") or "pending").lower() in {"pending", "queued"}]
    limited = pending[: max(1, int(limit or DEFAULT_MAX_SITE_REQUESTS_PER_CYCLE))]
    logging.info("source_refresh_site_poll_received count=%s", len(limited))
    return {"ok": True, "configured": True, "status": status, "requests": limited}


def update_site_refresh_request_status(request_id: str, status: str, *, environ: dict[str, str] | None = None, opener=None, completed_by_recommendation_id: str | None = None, failure_reason: str | None = None) -> dict[str, Any]:
    """POST a safe status update for a site-side refresh request."""

    env = environ if environ is not None else os.environ
    url = _configured_site_refresh_requests_url(env)
    token = (env.get("BNL_SOURCE_FILE_READ_TOKEN") or "").strip()
    rid = _safe_text(request_id, 120)
    safe_status = _safe_text(status, 40)
    if not url or not token or not rid:
        logging.info("source_refresh_site_poll_skipped reason=not_configured")
        return {"ok": False, "reason": "not_configured"}
    payload: dict[str, Any] = {"id": rid, "requestId": rid, "status": safe_status}
    if completed_by_recommendation_id:
        payload["completedByRecommendationId"] = _safe_text(completed_by_recommendation_id, 120)
    if failure_reason:
        payload["failureReason"] = _safe_text(failure_reason, 240)
    body = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=body,
        headers={"Accept": "application/json", "Content-Type": "application/json", "x-bnl-source-file-read-token": token},
        method="POST",
    )
    urlopen = opener.open if opener is not None else urllib.request.urlopen
    try:
        with urlopen(request, timeout=SITE_REFRESH_REQUEST_TIMEOUT_SECONDS) as response:
            http_status = getattr(response, "status", None) or response.getcode()
            raw = response.read().decode("utf-8")
    except (urllib.error.URLError, TimeoutError, socket.timeout) as exc:
        return {"ok": False, "reason": "network_or_timeout", "error": _safe_text(exc, 160)}
    except Exception as exc:
        return {"ok": False, "reason": "unexpected_error", "error": _safe_text(exc, 160)}
    try:
        parsed = json.loads(raw) if raw.strip() else {}
    except json.JSONDecodeError:
        parsed = {}
    ok = 200 <= int(http_status or 0) < 300 and (not isinstance(parsed, dict) or parsed.get("ok", True) is not False)
    return {"ok": ok, "status": http_status, "response": parsed}

def _evidence_hash_for_result(result: dict[str, Any]) -> str:
    material = json.dumps({"sourceCounts": result.get("sourceCounts"), "sourceTypes": result.get("sourceTypes"), "qualityScore": result.get("qualityScore"), "subject": result.get("subject")}, sort_keys=True, default=str)
    return hashlib.sha256(material.encode("utf-8")).hexdigest()[:16]


def _cooldown_until_from(now_dt: datetime, minutes: int) -> str:
    return (now_dt + timedelta(minutes=max(DEFAULT_COOLDOWN_MINUTES, int(minutes or DEFAULT_COOLDOWN_MINUTES)))).replace(microsecond=0).isoformat()


def _state_upsert(conn: sqlite3.Connection, *, guild_id: int | None, subject_key: str, subject_name: str, fields: dict[str, Any]) -> None:
    now = utc_now()
    current = conn.execute(f"SELECT subject_key FROM {STATE_TABLE} WHERE (guild_id IS ? OR guild_id=?) AND subject_key=?", (guild_id, guild_id, subject_key)).fetchone()
    base = {"guild_id": guild_id, "subject_key": subject_key, "subject_name": subject_name, "updated_at": now}
    base.update(fields)
    if current:
        assignments = ", ".join(f"{k}=?" for k in base if k not in {"guild_id", "subject_key"})
        values = [base[k] for k in base if k not in {"guild_id", "subject_key"}]
        conn.execute(f"UPDATE {STATE_TABLE} SET {assignments} WHERE (guild_id IS ? OR guild_id=?) AND subject_key=?", [*values, guild_id, guild_id, subject_key])
    else:
        cols = ", ".join(base.keys())
        placeholders = ", ".join(["?"] * len(base))
        conn.execute(f"INSERT INTO {STATE_TABLE} ({cols}) VALUES ({placeholders})", list(base.values()))


def process_source_file_refresh_queue(db_path: str, *, guild_id: int | None = None, dry_run: bool = False, max_items: int = DEFAULT_MAX_AUTOMATIC_PER_CYCLE, cooldown_minutes: int = DEFAULT_COOLDOWN_MINUTES, lookup_func: Callable[[dict[str, str]], dict[str, Any]] = lookup_source_file, sender: Callable[[dict[str, Any]], dict[str, Any]] = send_dossier_recommendation, environ: dict[str, str] | None = None, force: bool = False, bypass_cooldown: bool = False, refresh_mode: str = "automatic", subject_key: str | None = None, callback_base_url: str | None = None) -> dict[str, Any]:
    now_dt = datetime.now(timezone.utc).replace(microsecond=0)
    now = now_dt.isoformat()
    env = environ if environ is not None else os.environ
    mode = "dry_run" if dry_run else (refresh_mode if refresh_mode in VALID_REFRESH_MODES else "automatic")
    summary: dict[str, Any] = {"ok": True, "dryRun": dry_run, "processed": 0, "skipped": 0, "failed": 0, "items": []}
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        ensure_source_file_refresh_schema(conn)
        where = "status IN ('queued','deferred','cooldown','failed') AND (not_before_at IS NULL OR not_before_at<=?)"
        params: list[Any] = [now]
        if guild_id is not None:
            where += " AND guild_id=?"
            params.append(guild_id)
        if subject_key:
            where += " AND subject_key=?"
            params.append(source_refresh_subject_key(subject_key))
        rows = conn.execute(f"SELECT * FROM {QUEUE_TABLE} WHERE {where} ORDER BY priority DESC, queued_at ASC LIMIT ?", [*params, max(1, int(max_items or 1))]).fetchall()
        if not rows:
            return summary
        if not dry_run and not (is_dossier_ingest_token_configured(env) or is_source_file_archive_token_configured(env)):
            for row in rows:
                conn.execute(f"UPDATE {QUEUE_TABLE} SET status='deferred', updated_at=?, last_error='missing_source_refresh_token' WHERE id=?", (now, row["id"]))
                logging.info("source_refresh_skipped subject_key=%s reason=missing_source_refresh_token", row["subject_key"])
                summary["skipped"] += 1
                summary["items"].append({"subject": row["subject_name"], "status": "deferred", "reason": "missing_source_refresh_token"})
            conn.commit()
            return summary
        for row in rows:
            if dry_run:
                summary["processed"] += 1
                summary["items"].append({"subject": row["subject_name"], "status": "dry_run", "reason": row["reason"], "evidenceCount": row["evidence_count"], "wouldSend": True})
                continue
            state = conn.execute(f"SELECT * FROM {STATE_TABLE} WHERE (guild_id IS ? OR guild_id=?) AND subject_key=? LIMIT 1", (row["guild_id"], row["guild_id"], row["subject_key"])).fetchone()
            cooldown_until = parse_iso(state["cooldown_until"] if state else None)
            queue_reason_missing = _queue_reason_has_case_report_missing(row["reason"])
            report_missing = queue_reason_missing
            backfill_trigger = _case_report_backfill_trigger(queue_reason=row["reason"], evidence_source=row["evidence_source"])
            if not report_missing:
                lookup_report_missing = _lookup_source_file_report_missing(row, lookup_func=lookup_func)
                report_missing = lookup_report_missing
                if lookup_report_missing:
                    backfill_trigger = _case_report_backfill_trigger(archive_lookup_missing=True)
                    merged_reason = _safe_text(f"{row['reason']}; {CASE_REPORT_MISSING_REASON}", 260)
                    conn.execute(f"UPDATE {QUEUE_TABLE} SET reason=?, updated_at=? WHERE id=?", (merged_reason, now, row["id"]))
                    conn.commit()
            if report_missing:
                logging.info("source_case_report_backfill_requested subject_key=%s trigger=%s reason=%s", row["subject_key"], backfill_trigger, CASE_REPORT_BACKFILL_REASON if queue_reason_missing else CASE_REPORT_MISSING_REASON)
            if cooldown_until and cooldown_until > now_dt and not (report_missing or bypass_cooldown or row["refresh_mode"] in {"manual", "operator_requested", "site_manual_request"}):
                conn.execute(f"UPDATE {QUEUE_TABLE} SET status='cooldown', updated_at=?, not_before_at=?, last_error='cooldown' WHERE id=?", (now, cooldown_until.isoformat(), row["id"]))
                conn.commit()
                logging.info("source_refresh_deferred subject_key=%s reason=cooldown", row["subject_key"])
                summary["skipped"] += 1
                summary["items"].append({"subject": row["subject_name"], "status": "cooldown", "reason": "cooldown", "cooldownUntil": cooldown_until.isoformat()})
                continue
            conn.execute(f"UPDATE {QUEUE_TABLE} SET status='running', updated_at=?, last_attempt_at=?, attempts=attempts+1, last_error=NULL WHERE id=?", (now, now, row["id"]))
            _state_upsert(conn, guild_id=row["guild_id"], subject_key=row["subject_key"], subject_name=row["subject_name"], fields={"last_refresh_started_at": now, "last_refresh_status": "running"})
            conn.commit()
            logging.info("source_refresh_started subject_key=%s mode=%s", row["subject_key"], mode)
            if report_missing:
                logging.info("source_case_report_backfill_started subject_key=%s reason=%s trigger=%s", row["subject_key"], CASE_REPORT_BACKFILL_REASON, backfill_trigger)
            try:
                with refresh_generation_context(row["subject_key"]):
                    result = run_source_file_enrichment(
                        db_path,
                        row["guild_id"],
                        row["subject_name"],
                        dry_run=False,
                        lookup_func=lookup_func,
                        sender=sender,
                        environ=env,
                        force=force,
                        diagnostics=False,
                        callback_base_url=callback_base_url or None,
                    )
                if result.get("status") == "no_target":
                    conn.execute(f"UPDATE {QUEUE_TABLE} SET status='skipped', updated_at=?, last_error='no_target' WHERE id=?", (utc_now(), row["id"]))
                    _state_upsert(conn, guild_id=row["guild_id"], subject_key=row["subject_key"], subject_name=row["subject_name"], fields={"last_refresh_completed_at": utc_now(), "last_refresh_status": "no_target", "last_failure_at": utc_now(), "last_error": "no_target"})
                    conn.commit()
                    logging.info("source_refresh_skipped subject_key=%s reason=no_target", row["subject_key"])
                    summary["skipped"] += 1
                    summary["items"].append({"subject": row["subject_name"], "status": "skipped", "reason": "no_target", "error": "no_target"})
                elif result.get("sent") or (result.get("archiveSent") and not result.get("recommendationSent") and result.get("status") == "partial_success"):
                    send_result = result.get("sendResult") or {}
                    recommendation_id = str(send_result.get("recommendationId") or send_result.get("recommendation_id") or result.get("recommendationId") or send_result.get("id") or "")[:120]
                    archive_result = result.get("archiveResult") or {}
                    archive_id = str(archive_result.get("archiveId") or result.get("archiveId") or "")[:120]
                    cooldown_until_text = _cooldown_until_from(now_dt, cooldown_minutes)
                    evidence_count = sum(int(v or 0) for v in (result.get("sourceCounts") or {}).values()) if isinstance(result.get("sourceCounts"), dict) else int(row["evidence_count"] or 0)
                    partial_success = bool(result.get("archiveSent") and not result.get("recommendationSent") and result.get("status") == "partial_success")
                    item_status = "partial_success" if partial_success else "succeeded"
                    partial_reason = "compact_recommendation_rejected" if partial_success else ""
                    item_reason = CASE_REPORT_BACKFILL_REASON if report_missing and not partial_reason else partial_reason
                    conn.execute(f"UPDATE {QUEUE_TABLE} SET status='succeeded', updated_at=?, last_error=NULL WHERE id=?", (utc_now(), row["id"]))
                    _state_upsert(conn, guild_id=row["guild_id"], subject_key=row["subject_key"], subject_name=row["subject_name"], fields={"last_refresh_completed_at": utc_now(), "last_refresh_status": item_status, "last_recommendation_id": recommendation_id, "last_evidence_hash": _evidence_hash_for_result(result), "last_evidence_count": evidence_count, "last_error": partial_reason or None, "cooldown_until": cooldown_until_text})
                    conn.commit()
                    if partial_success:
                        logging.warning("source_refresh_partial_success subject_key=%s archive_ok=true compact_recommendation_ok=false reason=compact_recommendation_rejected archive_id=%s", row["subject_key"], archive_id or "none")
                    else:
                        logging.info("source_refresh_succeeded subject_key=%s recommendation_id=%s archive_id=%s", row["subject_key"], recommendation_id or "none", archive_id or "none")
                    if report_missing:
                        logging.info("source_case_report_backfill_succeeded subject_key=%s status=%s archive_id=%s trigger=%s", row["subject_key"], item_status, archive_id or "none", backfill_trigger)
                    summary["processed"] += 1
                    summary["items"].append({"subject": row["subject_name"], "status": item_status, "reason": item_reason, "error": partial_reason, "partialSuccess": partial_success, "recommendationId": recommendation_id, "recommendationSent": bool(result.get("recommendationSent", bool(send_result.get("ok")))), "archiveSent": bool(result.get("archiveSent", bool(archive_result.get("ok")))), "archiveStatus": result.get("archiveStatus") or archive_result.get("status"), "archiveId": archive_id, "archiveError": _safe_text(result.get("archiveError") or archive_result.get("error") or "", 180), "caseReportBackfill": bool(report_missing), "caseReportBackfillTrigger": backfill_trigger, "caseReportGenerated": _result_case_report_generated(result), "subjectMemoryPacketGenerated": _result_subject_memory_packet_generated(result)})
                else:
                    archive_result = result.get("archiveResult") or {}
                    send_result = result.get("sendResult") or {}
                    if result.get("recommendationSent") and not result.get("archiveSent"):
                        err = _safe_text(result.get("archiveError") or archive_result.get("error") or "archive_send_failed", 240)
                    elif result.get("archiveSent") and not result.get("recommendationSent"):
                        err = _safe_text(send_result.get("error") or "recommendation_send_failed", 240)
                    else:
                        err = _safe_text(send_result.get("error") or archive_result.get("error") or result.get("suppressedReason") or result.get("status") or "send_failed", 240)
                    conn.execute(f"UPDATE {QUEUE_TABLE} SET status='failed', updated_at=?, last_error=? WHERE id=?", (utc_now(), err, row["id"]))
                    _state_upsert(conn, guild_id=row["guild_id"], subject_key=row["subject_key"], subject_name=row["subject_name"], fields={"last_refresh_status": "failed", "last_failure_at": utc_now(), "last_error": err})
                    conn.commit()
                    logging.warning("source_refresh_failed subject_key=%s error=%s", row["subject_key"], err)
                    if report_missing:
                        logging.warning("source_case_report_backfill_failed subject_key=%s error=%s trigger=%s", row["subject_key"], err, backfill_trigger)
                    summary["failed"] += 1
                    summary["items"].append({"subject": row["subject_name"], "status": "failed", "reason": CASE_REPORT_BACKFILL_REASON if report_missing else "", "error": err, "recommendationSent": bool(result.get("recommendationSent")), "archiveSent": bool(result.get("archiveSent")), "archiveStatus": result.get("archiveStatus") or archive_result.get("status"), "archiveId": str(archive_result.get("archiveId") or result.get("archiveId") or "")[:120], "archiveError": _safe_text(result.get("archiveError") or archive_result.get("error") or "", 180), "recommendationId": str(send_result.get("recommendationId") or result.get("recommendationId") or "")[:120], "caseReportBackfill": bool(report_missing), "caseReportBackfillTrigger": backfill_trigger, "caseReportGenerated": _result_case_report_generated(result), "subjectMemoryPacketGenerated": _result_subject_memory_packet_generated(result)})
            except Exception as exc:
                err = _safe_text(exc, 240)
                conn.execute(f"UPDATE {QUEUE_TABLE} SET status='failed', updated_at=?, last_error=? WHERE id=?", (utc_now(), err, row["id"]))
                _state_upsert(conn, guild_id=row["guild_id"], subject_key=row["subject_key"], subject_name=row["subject_name"], fields={"last_refresh_status": "failed", "last_failure_at": utc_now(), "last_error": err})
                conn.commit()
                logging.warning("source_refresh_failed subject_key=%s error=%s", row["subject_key"], err)
                if report_missing:
                    logging.warning("source_case_report_backfill_failed subject_key=%s error=%s trigger=%s", row["subject_key"], err, backfill_trigger)
                summary["failed"] += 1
                summary["items"].append({"subject": row["subject_name"], "status": "failed", "reason": CASE_REPORT_BACKFILL_REASON if report_missing else "", "error": err, "caseReportBackfill": bool(report_missing)})
    finally:
        conn.close()
    logging.info("source_refresh_worker_cycle processed=%s skipped=%s failed=%s", summary["processed"], summary["skipped"], summary["failed"])
    return summary


def process_single_source_file_refresh(db_path: str, *, guild_id: int | None, subject_name: str, dry_run: bool = False, lookup_func: Callable[[dict[str, str]], dict[str, Any]] = lookup_source_file, sender: Callable[[dict[str, Any]], dict[str, Any]] = send_dossier_recommendation, environ: dict[str, str] | None = None) -> dict[str, Any]:
    subject = _safe_text(subject_name, 90)
    skey = source_refresh_subject_key(subject)
    if dry_run:
        return {"ok": True, "dryRun": True, "processed": 1, "skipped": 0, "failed": 0, "items": [{"subject": subject, "status": "dry_run", "reason": "operator_requested_refresh", "evidenceCount": 1, "wouldSend": True}]}
    enqueue_source_file_refresh(db_path, guild_id=guild_id, subject_name=subject, reason="operator_requested_refresh", evidence_source="operator_command", priority=100, refresh_mode="operator_requested", created_by="operator", evidence_count=1)
    return process_source_file_refresh_queue(db_path, guild_id=guild_id, dry_run=False, max_items=1, lookup_func=lookup_func, sender=sender, environ=environ, bypass_cooldown=True, refresh_mode="operator_requested", subject_key=skey)



def process_site_refresh_requests(db_path: str, *, guild_id: int | None = None, dry_run: bool = False, max_requests: int = DEFAULT_MAX_SITE_REQUESTS_PER_CYCLE, lookup_func: Callable[[dict[str, str]], dict[str, Any]] = lookup_source_file, sender: Callable[[dict[str, Any]], dict[str, Any]] = send_dossier_recommendation, environ: dict[str, str] | None = None, opener=None) -> dict[str, Any]:
    """Poll, claim, and process pending website Source File refresh requests."""

    env = environ if environ is not None else os.environ
    fetched = fetch_site_refresh_requests(environ=env, opener=opener, limit=max_requests)
    summary: dict[str, Any] = {"ok": bool(fetched.get("ok")), "dryRun": dry_run, "processed": 0, "skipped": 0, "failed": 0, "items": [], "sitePoll": fetched}
    requests = list(fetched.get("requests") or [])
    if not requests:
        return summary
    for site_request in requests[: max(1, int(max_requests or DEFAULT_MAX_SITE_REQUESTS_PER_CYCLE))]:
        request_id = _site_refresh_request_id(site_request)
        subject = _site_refresh_subject_name(site_request)
        skey = _site_refresh_subject_key(site_request)
        mode = _site_refresh_mode(site_request)
        explicit_case_report_backfill = site_request_requires_case_report_backfill(site_request)
        if explicit_case_report_backfill:
            logging.info("source_case_report_backfill_requested subject_key=%s trigger=%s reason=%s", skey or "none", "explicit_site_request", _safe_text(site_request.get("reason") or CASE_REPORT_BACKFILL_REASON, 120))
        if dry_run:
            summary["processed"] += 1
            summary["items"].append({"subject": subject or skey, "subjectKey": skey, "requestId": request_id, "status": "dry_run", "wouldClaim": True, "wouldEnqueue": True, "wouldSend": True})
            continue
        if not request_id:
            summary["failed"] += 1
            summary["items"].append({"subject": subject or skey, "subjectKey": skey, "status": "failed", "error": "missing_request_id"})
            continue
        claim = update_site_refresh_request_status(request_id, "claimed", environ=env, opener=opener)
        if not claim.get("ok"):
            reason = _safe_text(claim.get("reason") or claim.get("error") or "claim_failed", 160)
            logging.warning("source_refresh_site_request_failed request_id=%s reason=%s", request_id, reason)
            summary["failed"] += 1
            summary["items"].append({"subject": subject or skey, "subjectKey": skey, "requestId": request_id, "status": "failed", "error": reason})
            continue
        logging.info("source_refresh_site_request_claimed request_id=%s", request_id)
        lookup_key = "normalizedName" if (site_request.get("normalizedSubjectKey") or site_request.get("subjectKey")) and not site_request.get("subjectName") else "subject"
        lookup_value = str(site_request.get("normalizedSubjectKey") or site_request.get("subjectKey") or subject or skey)
        report_missing = bool(explicit_case_report_backfill)
        try:
            target_check = lookup_func({"lookupKey": lookup_key, "lookupValue": lookup_value, lookup_key: lookup_value})
            lookup_report_missing = source_file_report_missing(target_check)
            report_missing = bool(explicit_case_report_backfill or lookup_report_missing)
            if lookup_report_missing:
                logging.info("source_case_report_missing subject_key=%s reason=%s trigger=%s", skey or "none", CASE_REPORT_MISSING_REASON, "archive_lookup_missing")
            found = bool(target_check.get("found") or target_check.get("sourceFile") or target_check.get("source_file") or target_check.get("candidate") or target_check.get("dossier"))
            if target_check.get("ok") is False or not found:
                reason = "no_target" if target_check.get("ok") is not False else "lookup_failed"
                if reason == "no_target":
                    conn = sqlite3.connect(db_path)
                    try:
                        ensure_source_file_refresh_schema(conn)
                        _state_upsert(conn, guild_id=guild_id, subject_key=skey, subject_name=subject or lookup_value, fields={"last_refresh_completed_at": utc_now(), "last_refresh_status": "no_target", "last_failure_at": utc_now(), "last_error": "no_target"})
                        conn.commit()
                    finally:
                        conn.close()
                    logging.info("source_refresh_skipped subject_key=%s reason=no_target", skey)
                update_site_refresh_request_status(request_id, "failed", environ=env, opener=opener, failure_reason=reason)
                logging.warning("source_refresh_site_request_failed request_id=%s reason=%s", request_id, reason)
                summary["failed"] += 1
                summary["items"].append({"subject": subject or lookup_value, "subjectKey": skey, "requestId": request_id, "status": "failed", "error": reason})
                continue
        except Exception as exc:
            reason = _safe_text(exc, 160) or "lookup_failed"
            update_site_refresh_request_status(request_id, "failed", environ=env, opener=opener, failure_reason=reason)
            logging.warning("source_refresh_site_request_failed request_id=%s reason=%s", request_id, reason)
            summary["failed"] += 1
            summary["items"].append({"subject": subject or skey, "subjectKey": skey, "requestId": request_id, "status": "failed", "error": reason})
            continue
        callback_base_url, callback_source = _select_and_log_source_refresh_callback_base(site_request, environ=env)
        enqueue_source_file_refresh(
            db_path,
            guild_id=guild_id,
            subject_name=subject or lookup_value,
            reason=CASE_REPORT_BACKFILL_REASON if explicit_case_report_backfill else (CASE_REPORT_MISSING_REASON if report_missing else f"site_refresh_request:{request_id}"),
            evidence_source="site_refresh_request",
            priority=95 if mode == "site_manual_request" else 70,
            refresh_mode=mode,
            created_by="site_refresh_request",
            evidence_count=1,
        )
        local = process_source_file_refresh_queue(
            db_path,
            guild_id=guild_id,
            dry_run=False,
            max_items=1,
            lookup_func=lookup_func,
            sender=sender,
            environ=env,
            bypass_cooldown=explicit_case_report_backfill or report_missing or (mode == "site_manual_request"),
            refresh_mode=mode,
            subject_key=skey,
            callback_base_url=callback_base_url or None,
        )
        item = (local.get("items") or [{}])[0]
        status = item.get("status")
        if status in {"succeeded", "partial_success"}:
            rec_id = _safe_text(item.get("recommendationId") or "", 120)
            update_site_refresh_request_status(request_id, "completed", environ=env, opener=opener, completed_by_recommendation_id=rec_id)
            logging.info("source_refresh_site_request_completed request_id=%s recommendation_id=%s", request_id, rec_id or "none")
            summary["processed"] += 1
            summary["items"].append({"subject": subject or item.get("subject") or skey, "subjectKey": skey, "requestId": request_id, "status": status, "reason": item.get("reason") or "", "recommendationId": rec_id, "caseReportBackfill": bool(item.get("caseReportBackfill"))})
        elif status == "cooldown":
            update_site_refresh_request_status(request_id, "skipped", environ=env, opener=opener, failure_reason="cooldown_deferred")
            logging.info("source_refresh_site_request_failed request_id=%s reason=cooldown_deferred", request_id)
            summary["skipped"] += 1
            summary["items"].append({"subject": subject or item.get("subject") or skey, "subjectKey": skey, "requestId": request_id, "status": "deferred", "reason": "cooldown"})
        else:
            reason = _safe_text(item.get("error") or item.get("reason") or "send_failed", 240)
            update_site_refresh_request_status(request_id, "failed", environ=env, opener=opener, failure_reason=reason)
            logging.warning("source_refresh_site_request_failed request_id=%s reason=%s", request_id, reason)
            summary["failed"] += 1
            summary["items"].append({"subject": subject or item.get("subject") or skey, "subjectKey": skey, "requestId": request_id, "status": "failed", "error": reason})
    return summary

def format_source_refresh_queue_response(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "Source refresh queue is empty."
    lines = ["Source refresh queue:"]
    for row in rows[:15]:
        lines.append(f"* {_safe_text(row.get('subject_name'), 70)} — {row.get('status')} — evidence {int(row.get('evidence_count') or 0)} — priority {int(row.get('priority') or 0)} — {_safe_text(row.get('reason'), 90)}")
    return "\n".join(lines)[:1900]


def format_source_refresh_process_response(summary: dict[str, Any]) -> str:
    header = "Dry-run source refresh process" if summary.get("dryRun") else "Source refresh process"
    lines = [f"{header}: processed={int(summary.get('processed') or 0)} skipped={int(summary.get('skipped') or 0)} failed={int(summary.get('failed') or 0)}"]
    for item in (summary.get("items") or [])[:12]:
        detail = item.get("reason") or item.get("error") or item.get("recommendationId") or "none"
        lines.append(f"* {_safe_text(item.get('subject'), 70)} — {item.get('status')} — {_safe_text(detail, 110)}")
    return "\n".join(lines)[:1900]


def parse_source_refresh_command(content: str) -> tuple[bool, dict[str, Any] | None, str]:
    text = (content or "").strip()
    match = re.match(r"^!bnl\s+source\s+refresh(?:\s+(.+))?$", text, flags=re.I | re.S)
    if not match:
        return False, None, "not_a_source_refresh_command"
    body = (match.group(1) or "").strip()
    if not body:
        return True, None, "Use: `!bnl source refresh queue|process|site|clear <subject>|<subject> [| dry_run=true]`."
    parts = [p.strip() for p in body.split("|")]
    head = re.sub(r"\s+", " ", parts[0]).strip()
    opts = {"dry_run": False}
    for extra in parts[1:]:
        opt = re.match(r"^([A-Za-z_]+)\s*=\s*(.+)$", extra, flags=re.S)
        if not opt:
            return True, None, "Supported option: dry_run=true|false."
        key = opt.group(1).strip().lower()
        val = opt.group(2).strip().lower()
        if key in {"dry_run", "dryrun"}:
            opts["dry_run"] = val in {"true", "1", "yes", "on"}
        else:
            return True, None, "Supported option: dry_run=true|false."
    low = head.lower()
    if low == "queue":
        return True, {"action": "queue", **opts}, ""
    if low == "process":
        return True, {"action": "process", **opts}, ""
    if low == "site":
        return True, {"action": "site", **opts}, ""
    if low.startswith("clear "):
        subject = head[6:].strip()
        if not subject:
            return True, None, "Subject is required for clear."
        return True, {"action": "clear", "subject": subject, **opts}, ""
    return True, {"action": "subject", "subject": head, **opts}, ""


def build_refresh_diagnostics(db_path: str, *, guild_id: int | None, subject_name: str, mode: str = "manual") -> list[str]:
    skey = source_refresh_subject_key(subject_name)
    state = get_refresh_state(db_path, guild_id, skey) or {}
    rows = [r for r in list_source_file_refresh_queue(db_path, guild_id=guild_id, limit=50) if r.get("subject_key") == skey]
    cooldown_until = state.get("cooldown_until") or "none"
    evidence_count = int(rows[0].get("evidence_count") or state.get("last_evidence_count") or 0) if rows else int(state.get("last_evidence_count") or 0)
    return [
        f"Refresh mode: {mode}.",
        f"Last refresh: {state.get('last_refresh_completed_at') or state.get('last_refresh_started_at') or 'none'} ({state.get('last_refresh_status') or 'none'}).",
        f"Evidence count changed/queued: {evidence_count}.",
        f"Cooldown until: {cooldown_until}.",
        f"Current queue state: {(rows[0].get('status') if rows else 'none')}.",
    ]
