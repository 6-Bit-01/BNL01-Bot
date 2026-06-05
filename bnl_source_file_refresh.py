"""Durable Source File auto-refresh queue helpers for BNL.

The queue is review-only automation: it marks Source File subjects dirty when
meaningful local evidence changes, batches duplicate signals, and reuses the
existing Source File enrichment/send path under cooldown/rate-limit controls.
It does not publish, merge identities, connect queue payments, or create artist
accounts.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from bnl_dossier_recommendations import is_dossier_ingest_token_configured, send_dossier_recommendation
from bnl_source_file_enrichment import run_source_file_enrichment
from bnl_source_file_lookup import lookup_source_file

QUEUE_TABLE = "source_file_refresh_queue"
STATE_TABLE = "source_file_refresh_state"
ACTIVE_QUEUE_STATUSES = {"queued", "deferred", "cooldown", "running", "failed"}
TERMINAL_QUEUE_STATUSES = {"succeeded", "skipped"}
VALID_STATUSES = ACTIVE_QUEUE_STATUSES | TERMINAL_QUEUE_STATUSES
VALID_REFRESH_MODES = {"automatic", "manual", "dry_run", "operator_requested"}
DEFAULT_COOLDOWN_MINUTES = 30
DEFAULT_PROCESS_INTERVAL_MINUTES = 15
DEFAULT_MAX_AUTOMATIC_PER_CYCLE = 3
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
    return re.sub(r"[^a-z0-9]+", "-", (value or "").strip().lower()).strip("-") or "unknown-subject"


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


def process_source_file_refresh_queue(db_path: str, *, guild_id: int | None = None, dry_run: bool = False, max_items: int = DEFAULT_MAX_AUTOMATIC_PER_CYCLE, cooldown_minutes: int = DEFAULT_COOLDOWN_MINUTES, lookup_func: Callable[[dict[str, str]], dict[str, Any]] = lookup_source_file, sender: Callable[[dict[str, Any]], dict[str, Any]] = send_dossier_recommendation, environ: dict[str, str] | None = None, force: bool = False, bypass_cooldown: bool = False, refresh_mode: str = "automatic") -> dict[str, Any]:
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
        rows = conn.execute(f"SELECT * FROM {QUEUE_TABLE} WHERE {where} ORDER BY priority DESC, queued_at ASC LIMIT ?", [*params, max(1, int(max_items or 1))]).fetchall()
        if not rows:
            return summary
        if not dry_run and not is_dossier_ingest_token_configured(env):
            for row in rows:
                conn.execute(f"UPDATE {QUEUE_TABLE} SET status='deferred', updated_at=?, last_error='missing_ingest_token' WHERE id=?", (now, row["id"]))
                logging.info("source_refresh_skipped subject_key=%s reason=missing_ingest_token", row["subject_key"])
                summary["skipped"] += 1
                summary["items"].append({"subject": row["subject_name"], "status": "deferred", "reason": "missing_ingest_token"})
            conn.commit()
            return summary
        for row in rows:
            state = conn.execute(f"SELECT * FROM {STATE_TABLE} WHERE (guild_id IS ? OR guild_id=?) AND subject_key=? LIMIT 1", (row["guild_id"], row["guild_id"], row["subject_key"])).fetchone()
            cooldown_until = parse_iso(state["cooldown_until"] if state else None)
            if cooldown_until and cooldown_until > now_dt and not (bypass_cooldown or row["refresh_mode"] in {"manual", "operator_requested"}):
                conn.execute(f"UPDATE {QUEUE_TABLE} SET status='cooldown', updated_at=?, not_before_at=?, last_error='cooldown' WHERE id=?", (now, cooldown_until.isoformat(), row["id"]))
                conn.commit()
                logging.info("source_refresh_deferred subject_key=%s reason=cooldown", row["subject_key"])
                summary["skipped"] += 1
                summary["items"].append({"subject": row["subject_name"], "status": "cooldown", "reason": "cooldown", "cooldownUntil": cooldown_until.isoformat()})
                continue
            if dry_run:
                summary["processed"] += 1
                summary["items"].append({"subject": row["subject_name"], "status": "dry_run", "reason": row["reason"], "evidenceCount": row["evidence_count"], "wouldSend": True})
                continue
            conn.execute(f"UPDATE {QUEUE_TABLE} SET status='running', updated_at=?, last_attempt_at=?, attempts=attempts+1, last_error=NULL WHERE id=?", (now, now, row["id"]))
            _state_upsert(conn, guild_id=row["guild_id"], subject_key=row["subject_key"], subject_name=row["subject_name"], fields={"last_refresh_started_at": now, "last_refresh_status": "running"})
            conn.commit()
            logging.info("source_refresh_started subject_key=%s mode=%s", row["subject_key"], mode)
            try:
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
                )
                if result.get("sent"):
                    send_result = result.get("sendResult") or {}
                    recommendation_id = str(send_result.get("recommendationId") or send_result.get("recommendation_id") or send_result.get("id") or "")[:120]
                    cooldown_until_text = _cooldown_until_from(now_dt, cooldown_minutes)
                    evidence_count = sum(int(v or 0) for v in (result.get("sourceCounts") or {}).values()) if isinstance(result.get("sourceCounts"), dict) else int(row["evidence_count"] or 0)
                    conn.execute(f"UPDATE {QUEUE_TABLE} SET status='succeeded', updated_at=?, last_error=NULL WHERE id=?", (utc_now(), row["id"]))
                    _state_upsert(conn, guild_id=row["guild_id"], subject_key=row["subject_key"], subject_name=row["subject_name"], fields={"last_refresh_completed_at": utc_now(), "last_refresh_status": "succeeded", "last_recommendation_id": recommendation_id, "last_evidence_hash": _evidence_hash_for_result(result), "last_evidence_count": evidence_count, "last_error": None, "cooldown_until": cooldown_until_text})
                    conn.commit()
                    logging.info("source_refresh_succeeded subject_key=%s recommendation_id=%s", row["subject_key"], recommendation_id or "none")
                    summary["processed"] += 1
                    summary["items"].append({"subject": row["subject_name"], "status": "succeeded", "recommendationId": recommendation_id})
                else:
                    err = _safe_text((result.get("sendResult") or {}).get("error") or result.get("suppressedReason") or result.get("status") or "send_failed", 240)
                    conn.execute(f"UPDATE {QUEUE_TABLE} SET status='failed', updated_at=?, last_error=? WHERE id=?", (utc_now(), err, row["id"]))
                    _state_upsert(conn, guild_id=row["guild_id"], subject_key=row["subject_key"], subject_name=row["subject_name"], fields={"last_refresh_status": "failed", "last_failure_at": utc_now(), "last_error": err})
                    conn.commit()
                    logging.warning("source_refresh_failed subject_key=%s error=%s", row["subject_key"], err)
                    summary["failed"] += 1
                    summary["items"].append({"subject": row["subject_name"], "status": "failed", "error": err})
            except Exception as exc:
                err = _safe_text(exc, 240)
                conn.execute(f"UPDATE {QUEUE_TABLE} SET status='failed', updated_at=?, last_error=? WHERE id=?", (utc_now(), err, row["id"]))
                _state_upsert(conn, guild_id=row["guild_id"], subject_key=row["subject_key"], subject_name=row["subject_name"], fields={"last_refresh_status": "failed", "last_failure_at": utc_now(), "last_error": err})
                conn.commit()
                logging.warning("source_refresh_failed subject_key=%s error=%s", row["subject_key"], err)
                summary["failed"] += 1
                summary["items"].append({"subject": row["subject_name"], "status": "failed", "error": err})
    finally:
        conn.close()
    logging.info("source_refresh_worker_cycle processed=%s skipped=%s failed=%s", summary["processed"], summary["skipped"], summary["failed"])
    return summary


def process_single_source_file_refresh(db_path: str, *, guild_id: int | None, subject_name: str, dry_run: bool = False, lookup_func: Callable[[dict[str, str]], dict[str, Any]] = lookup_source_file, sender: Callable[[dict[str, Any]], dict[str, Any]] = send_dossier_recommendation, environ: dict[str, str] | None = None) -> dict[str, Any]:
    enqueue_source_file_refresh(db_path, guild_id=guild_id, subject_name=subject_name, reason="operator_requested_refresh", evidence_source="operator_command", priority=100, refresh_mode="dry_run" if dry_run else "operator_requested", created_by="operator", evidence_count=1)
    return process_source_file_refresh_queue(db_path, guild_id=guild_id, dry_run=dry_run, max_items=1, lookup_func=lookup_func, sender=sender, environ=environ, bypass_cooldown=not dry_run, refresh_mode="operator_requested")


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
        return True, None, "Use: `!bnl source refresh queue|process|clear <subject>|<subject> [| dry_run=true]`."
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
