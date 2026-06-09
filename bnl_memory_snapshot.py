"""Read-only sanitized BNL memory snapshot utilities."""
from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SNAPSHOT_VERSION = 1
DEFAULT_LIMIT = 25
MAX_LIMIT = 100
TEXT_LIMIT = 700
SAMPLE_TEXT_LIMIT = 500

_LONG_ID_RE = re.compile(r"\b\d{15,22}\b")
_EMAIL_RE = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.I)
_URL_RE = re.compile(r"https?://\S+", re.I)
_MENTION_RE = re.compile(r"<@!?\d+>|<#\d+>|<@&\d+>")
_SECRET_ASSIGNMENT_RE = re.compile(
    r"(?i)\b([a-z0-9_]*(?:token|secret|api[_-]?key|authorization|password|passwd|credential)[a-z0-9_]*)\s*[:=]\s*([^\s,;\]}\)]+)"
)
_BEARER_RE = re.compile(r"(?i)\bBearer\s+[A-Za-z0-9._~+/=-]{8,}")
_DISCORD_TOKEN_RE = re.compile(r"\b[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{6,}\.[A-Za-z0-9_-]{20,}\b")

SECRET_FIELD_RE = re.compile(r"(?i)(token|secret|api[_-]?key|authorization|password|passwd|credential|raw_note|raw_transcript|payload_body)")
RAW_REF_FIELD_RE = re.compile(r"(?i)(raw|transcript|message|content|payload|body|authorization|token|secret|api[_-]?key|password)")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def clamp_limit(value: Any) -> int:
    try:
        limit = int(value)
    except Exception:
        return DEFAULT_LIMIT
    return min(MAX_LIMIT, max(1, limit))


def parse_bool(value: Any, *, default: bool = False) -> bool:
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def sanitize_database_path(db_path: str) -> str:
    return Path(db_path).name or "bnl01_conversations.db"


def redact_text(value: Any, *, limit: int = TEXT_LIMIT) -> str:
    text = str(value or "")
    text = _SECRET_ASSIGNMENT_RE.sub(lambda m: f"{m.group(1)}=[redacted-secret]", text)
    text = _BEARER_RE.sub("Bearer [redacted-secret]", text)
    text = _DISCORD_TOKEN_RE.sub("[redacted-secret]", text)
    text = _MENTION_RE.sub("[redacted-mention]", text)
    text = _LONG_ID_RE.sub("[redacted-id]", text)
    text = _EMAIL_RE.sub("[redacted-email]", text)
    text = _URL_RE.sub("[redacted-url]", text)
    text = re.sub(r"\s+", " ", text).strip()
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 1)].rstrip() + "…"


def redact_identifier(value: Any) -> str:
    raw = str(value or "")
    if not raw:
        return ""
    return "sha256:" + hashlib.sha256(raw.encode("utf-8", "ignore")).hexdigest()[:16]


def sanitize_value(key: str, value: Any, *, text_limit: int = TEXT_LIMIT, include_raw_refs: bool = False) -> Any:
    if value is None:
        return None
    if SECRET_FIELD_RE.search(key or "") and key not in {"raw_ref_json"}:
        return "[redacted]"
    if key == "matched_user_id":
        return redact_identifier(value)
    if key == "raw_ref_json":
        if not include_raw_refs:
            return None
        return compact_raw_ref_json(value, limit=350)
    if isinstance(value, (int, float)):
        return value
    if isinstance(value, bytes):
        return "[redacted-bytes]"
    return redact_text(value, limit=text_limit)



def compact_raw_ref_json(value: Any, *, limit: int = 350) -> Any:
    parsed = value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except Exception:
            return redact_text(value, limit=limit)
    return sanitize_raw_ref_json(parsed, limit=limit)


def sanitize_raw_ref_json(value: Any, *, limit: int = 350, depth: int = 0) -> Any:
    if depth > 3:
        return "[truncated-depth]"
    if isinstance(value, dict):
        out: dict[str, Any] = {}
        for idx, (k, v) in enumerate(value.items()):
            if idx >= 20:
                out["_truncatedKeys"] = True
                break
            key = str(k)
            if RAW_REF_FIELD_RE.search(key):
                out[key] = "[redacted-raw-ref]"
            else:
                out[key] = sanitize_raw_ref_json(v, limit=limit, depth=depth + 1)
        return out
    if isinstance(value, list):
        out = [sanitize_raw_ref_json(v, limit=limit, depth=depth + 1) for v in value[:10]]
        if len(value) > 10:
            out.append("[truncated-list]")
        return out
    if isinstance(value, (int, float, bool)) or value is None:
        return value
    return redact_text(value, limit=limit)

def compact_json(value: Any, *, limit: int = TEXT_LIMIT) -> Any:
    parsed = value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except Exception:
            return redact_text(value, limit=limit)
    return sanitize_json(parsed, limit=limit)


def sanitize_json(value: Any, *, limit: int = TEXT_LIMIT, depth: int = 0) -> Any:
    if depth > 4:
        return "[truncated-depth]"
    if isinstance(value, dict):
        out: dict[str, Any] = {}
        for idx, (k, v) in enumerate(value.items()):
            if idx >= 40:
                out["_truncatedKeys"] = True
                break
            key = str(k)
            if SECRET_FIELD_RE.search(key):
                out[key] = "[redacted]"
            else:
                out[key] = sanitize_json(v, limit=limit, depth=depth + 1)
        return out
    if isinstance(value, list):
        out = [sanitize_json(v, limit=limit, depth=depth + 1) for v in value[:25]]
        if len(value) > 25:
            out.append("[truncated-list]")
        return out
    if isinstance(value, (int, float, bool)) or value is None:
        return value
    return redact_text(value, limit=limit)


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone() is not None


def table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    if not table_exists(conn, table):
        return set()
    return {str(row[1]) for row in conn.execute(f'PRAGMA table_info("{table}")')}


def table_counts(conn: sqlite3.Connection) -> dict[str, int]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name").fetchall()
    counts: dict[str, int] = {}
    for row in rows:
        name = str(row[0])
        try:
            counts[name] = int(conn.execute(f'SELECT COUNT(*) FROM "{name}"').fetchone()[0])
        except Exception:
            counts[name] = -1
    return counts


def _order_column(cols: set[str]) -> str | None:
    for col in ("updated_at", "created_at", "observed_at", "last_seen_at", "timestamp", "queued_at", "id"):
        if col in cols:
            return col
    return None


def fetch_rows(conn: sqlite3.Connection, table: str, wanted: list[str], *, limit: int, subject_key: str | None = None, include_raw_refs: bool = False, text_limit: int = TEXT_LIMIT, where_extra: str = "", params_extra: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    cols = table_columns(conn, table)
    if not cols:
        return []
    select = [c for c in wanted if c in cols]
    if not select:
        return []
    where_parts: list[str] = []
    params: list[Any] = []
    if subject_key and "subject_key" in cols:
        where_parts.append("subject_key = ?")
        params.append(subject_key)
    if where_extra:
        where_parts.append(where_extra)
        params.extend(params_extra)
    where = " WHERE " + " AND ".join(where_parts) if where_parts else ""
    order_col = _order_column(cols)
    order = f' ORDER BY "{order_col}" DESC' if order_col else ""
    sql = f'SELECT {", ".join([f"\"{c}\"" for c in select])} FROM "{table}"{where}{order} LIMIT ?'
    rows = conn.execute(sql, (*params, limit)).fetchall()
    result: list[dict[str, Any]] = []
    for row in rows:
        item: dict[str, Any] = {}
        for key in select:
            val = row[key]
            if key in {"source_lanes", "approved_channel_labels", "active_windows", "evidence_snippets", "connection_notes"}:
                item[key] = compact_json(val, limit=180)
            else:
                sanitized = sanitize_value(key, val, text_limit=text_limit, include_raw_refs=include_raw_refs)
                if sanitized is not None:
                    item[key] = sanitized
        result.append(item)
    return result


def _profile_snapshots(conn: sqlite3.Connection, *, limit: int, subject_key: str | None) -> list[dict[str, Any]]:
    rows = fetch_rows(
        conn,
        "entity_profile_snapshots",
        ["guild_id", "subject_key", "subject_name", "display_name", "summary_json", "source_hash", "created_at", "updated_at"],
        limit=limit,
        subject_key=subject_key,
        text_limit=TEXT_LIMIT,
    )
    for row in rows:
        if "summary_json" in row:
            summary = compact_json(row["summary_json"], limit=TEXT_LIMIT)
            row["summary_json"] = summary
            if isinstance(summary, dict):
                for key in ("action_items", "actionItems", "open_questions", "openQuestions", "reviewOnly", "publicSafe", "public_safe"):
                    if key in summary:
                        row[key] = summary[key]
    return rows


def _memory_risk_summary(conn: sqlite3.Connection) -> dict[str, Any]:
    summary: dict[str, Any] = {"memoryTierRows": 0, "conversationRows": 0, "groups": []}
    if table_exists(conn, "memory_tiers"):
        cols = table_columns(conn, "memory_tiers")
        dims = [c for c in ("source_role", "source_channel_policy", "source_origin", "source_trust", "tier", "source_channel_name") if c in cols]
        summary["memoryTierRows"] = table_counts(conn).get("memory_tiers", 0)
        if dims:
            sql = f'SELECT {", ".join([f"\"{c}\"" for c in dims])}, COUNT(*) AS count FROM memory_tiers GROUP BY {", ".join([f"\"{c}\"" for c in dims])} ORDER BY count DESC LIMIT 100'
            summary["groups"] = [dict(row) for row in conn.execute(sql).fetchall()]
    if table_exists(conn, "conversations"):
        summary["conversationRows"] = int(conn.execute('SELECT COUNT(*) FROM "conversations"').fetchone()[0])
        cols = table_columns(conn, "conversations")
        dims = [c for c in ("role", "channel_policy", "channel_name") if c in cols]
        if dims:
            sql = f'SELECT {", ".join([f"\"{c}\"" for c in dims])}, COUNT(*) AS count FROM conversations GROUP BY {", ".join([f"\"{c}\"" for c in dims])} ORDER BY count DESC LIMIT 100'
            summary["conversationGroups"] = [dict(row) for row in conn.execute(sql).fetchall()]
    return sanitize_json(summary, limit=220)


def _memory_samples(conn: sqlite3.Connection, *, limit: int, include_samples: bool) -> list[dict[str, Any]]:
    if not include_samples or not table_exists(conn, "conversations"):
        return []
    return fetch_rows(
        conn,
        "conversations",
        ["user_name", "role", "content", "channel_policy", "channel_name", "timestamp"],
        limit=limit,
        text_limit=SAMPLE_TEXT_LIMIT,
    )


def _ambient_log(conn: sqlite3.Connection, *, limit: int, include_samples: bool) -> dict[str, Any]:
    result: dict[str, Any] = {"note": "ambient/context only; not dossier truth", "counts": {}, "samples": []}
    if not table_exists(conn, "ambient_log"):
        return result
    cols = table_columns(conn, "ambient_log")
    result["counts"]["total"] = int(conn.execute('SELECT COUNT(*) FROM "ambient_log"').fetchone()[0])
    if "source_type" in cols:
        result["counts"]["bySourceType"] = [dict(row) for row in conn.execute('SELECT source_type, COUNT(*) AS count FROM "ambient_log" GROUP BY source_type ORDER BY count DESC').fetchall()]
    if include_samples:
        result["samples"] = fetch_rows(conn, "ambient_log", ["guild_id", "channel_id", "message", "source_type", "posted_at", "timestamp"], limit=limit, text_limit=300)
    return sanitize_json(result, limit=300)


def _warnings(conn: sqlite3.Connection) -> list[str]:
    warnings: list[str] = []
    if table_exists(conn, "entity_evidence_events"):
        cols = table_columns(conn, "entity_evidence_events")
        if "authority" in cols:
            vals = [str(r[0] or "") for r in conn.execute('SELECT DISTINCT authority FROM "entity_evidence_events"')]
            if any(v in {"source_blind_memory", "source_blind_legacy_memory"} for v in vals):
                warnings.append("source_blind_memory present in entity evidence")
            if any("private" in v or "review" in v for v in vals):
                warnings.append("entity evidence includes review-only/private authority")
        if "review_only" in cols:
            count = conn.execute('SELECT COUNT(*) FROM "entity_evidence_events" WHERE review_only IN (1, "1", "true", "TRUE")').fetchone()[0]
            if int(count):
                warnings.append("entity evidence includes review_only rows")
    if table_exists(conn, "memory_tiers") and "source_trust" in table_columns(conn, "memory_tiers"):
        count = conn.execute('SELECT COUNT(*) FROM "memory_tiers" WHERE source_trust IN ("mixed_or_legacy_consolidated", "legacy_unknown", "source_blind_memory", "source_blind_legacy_memory")').fetchone()[0]
        if int(count):
            warnings.append("memory tiers include mixed_or_legacy_consolidated/source-blind/legacy source_trust")
    if table_exists(conn, "source_file_refresh_state"):
        cols = table_columns(conn, "source_file_refresh_state")
        if {"last_refresh_status", "last_error"}.issubset(cols):
            failures = conn.execute('SELECT COUNT(*) FROM "source_file_refresh_state" WHERE last_refresh_status IN ("failed", "error") OR COALESCE(last_error, "") != ""').fetchone()[0]
            if int(failures):
                warnings.append("source_file_refresh_state has failures")
            oversized = conn.execute('SELECT COUNT(*) FROM "source_file_refresh_state" WHERE LOWER(COALESCE(last_error, "")) LIKE "%text field is too long%" OR LOWER(COALESCE(last_error, "")) LIKE "%too long%"').fetchone()[0]
            if int(oversized):
                warnings.append("oversized compact payload failure appears in refresh state")
    if table_exists(conn, "source_file_refresh_queue"):
        cols = table_columns(conn, "source_file_refresh_queue")
        if "attempts" in cols:
            high = conn.execute('SELECT COUNT(*) FROM "source_file_refresh_queue" WHERE attempts >= 3').fetchone()[0]
            if int(high):
                warnings.append("source_file_refresh_queue has high attempt counts")
        if "last_error" in cols:
            oversized = conn.execute('SELECT COUNT(*) FROM "source_file_refresh_queue" WHERE LOWER(COALESCE(last_error, "")) LIKE "%text field is too long%" OR LOWER(COALESCE(last_error, "")) LIKE "%too long%"').fetchone()[0]
            if int(oversized):
                warnings.append("oversized compact payload failure appears in refresh queue")
    return warnings


def build_bnl_memory_snapshot(db_path: str, *, limit: int = DEFAULT_LIMIT, subject_key: str | None = None, include_samples: bool = True, include_raw_refs: bool = False) -> dict[str, Any]:
    """Build a read-only sanitized JSON-serializable snapshot from the existing SQLite DB."""
    bounded_limit = clamp_limit(limit)
    uri = f"file:{os.path.abspath(db_path)}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    try:
        counts = table_counts(conn)
        sections = {
            "communityPresence": fetch_rows(conn, "community_presence", ["guild_id", "subject_key", "display_name", "first_seen_at", "last_seen_at", "source_lanes", "approved_channel_labels", "mention_count", "direct_interaction_count", "operator_mention_count", "active_windows", "category", "last_error_status", "evidence_snippets"], limit=bounded_limit, subject_key=subject_key, text_limit=300),
            "entityProfileSnapshots": _profile_snapshots(conn, limit=bounded_limit, subject_key=subject_key),
            "entityEvidenceEvents": fetch_rows(conn, "entity_evidence_events", ["subject_key", "subject_name", "matched_user_id", "source_type", "source_table", "source_label", "channel_name", "channel_policy", "visibility", "authority", "confidence", "relation_to_subject", "topic", "evidence_kind", "safe_summary", "public_safe_candidate", "review_only", "music_signal", "community_signal", "bnl_interaction", "dossier_relevance", "created_at", "observed_at", "updated_at", "raw_ref_json"], limit=bounded_limit, subject_key=subject_key, include_raw_refs=include_raw_refs, text_limit=500),
            "entityIntelligenceEdges": fetch_rows(conn, "entity_intelligence_edges", ["subject_key", "object_key", "object_label", "relation_type", "source_type", "source_channel_policy", "visibility", "authority", "confidence", "first_seen_at", "last_seen_at", "evidence_count", "status"], limit=bounded_limit, subject_key=subject_key, text_limit=300),
            "entityOpenQuestions": fetch_rows(conn, "entity_open_questions", ["subject_key", "question", "reason", "source_type", "visibility", "priority", "status", "created_at", "updated_at"], limit=bounded_limit, subject_key=subject_key, where_extra='status = ?' if table_exists(conn, "entity_open_questions") and "status" in table_columns(conn, "entity_open_questions") else "", params_extra=("open",), text_limit=500),
            "memoryTierRiskSummary": _memory_risk_summary(conn),
            "memoryTierSamples": _memory_samples(conn, limit=bounded_limit, include_samples=include_samples),
            "sourceFileRefreshState": fetch_rows(conn, "source_file_refresh_state", ["subject_key", "subject_name", "last_refresh_started_at", "last_refresh_completed_at", "last_refresh_status", "last_failure_at", "last_error", "last_evidence_count", "cooldown_until", "updated_at"], limit=bounded_limit, subject_key=subject_key, text_limit=350),
            "sourceFileRefreshQueue": fetch_rows(conn, "source_file_refresh_queue", ["subject_key", "subject_name", "status", "attempts", "reason", "last_error", "queued_at", "updated_at", "not_before_at", "last_attempt_at", "refresh_mode", "created_by"], limit=bounded_limit, subject_key=subject_key, text_limit=350),
            "broadcastMemory": fetch_rows(conn, "broadcast_memory", ["episode_date", "cleaned_summary", "entry_type", "importance", "public_safe", "affects_next_show", "usage_scope", "status", "created_at", "updated_at"], limit=bounded_limit, text_limit=500),
            "ambientLog": _ambient_log(conn, limit=bounded_limit, include_samples=include_samples),
        }
        return {
            "snapshotVersion": SNAPSHOT_VERSION,
            "exportedAt": utc_now_iso(),
            "databaseName": sanitize_database_path(db_path),
            "tableCounts": counts,
            "rowLimits": {"default": bounded_limit, "max": MAX_LIMIT, "text": TEXT_LIMIT, "sampleText": SAMPLE_TEXT_LIMIT},
            "redactionPolicy": "Secrets, auth material, long Discord IDs, mentions, emails, URLs, raw refs, and transcripts are redacted/truncated; relationship/journal/source-blind material is review-only.",
            "filters": {"subject_key": subject_key or None, "include_samples": bool(include_samples), "include_raw_refs": bool(include_raw_refs)},
            "sections": sections,
            "warnings": _warnings(conn),
        }
    finally:
        conn.close()
