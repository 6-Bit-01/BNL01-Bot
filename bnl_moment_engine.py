"""Moment Engine v1 shadow infrastructure.

Builds bounded, auditable conversational moments from Unified Memory Ledger
conversation entries only. This module is deliberately not used by prompt assembly.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
import re
import sqlite3
from typing import Any

from bnl_canon_source_contract import Confidence, SourceClass, Visibility
from bnl_memory_ledger import (
    BNL_SUBJECT_KEY,
    LedgerEntry,
    LedgerParticipant,
    ensure_memory_ledger_schema,
    insert_ledger_entry,
    shadow_enabled as ledger_shadow_enabled,
)

MOMENT_ENGINE_SHADOW_ENV = "BNL_MOMENT_ENGINE_SHADOW_ENABLED"
MOMENT_SCHEMA_VERSION = "memory_moment_v1"
MAX_WINDOW_SECONDS = 5 * 60
INACTIVITY_SECONDS = 2 * 60

STOP = set("a an and are as at be but by for from how i in is it me my of on or our that the this to was we what when where who why with you your did do does about into can could would should just yep yes no ok okay hey hi hello thanks thank lol lmao got noted also ask asked".split())
LOW_SIGNAL = set("hi hey hello thanks thank you ok okay yep yes no lol lmao cool nice got it noted".split())
STRONG_MARKERS = ("remember", "recall", "what numbers", "correction", "actually", "replace", "commit", "promise", "boundary", "milestone", "follow up", "follow-up", "celebrate")
VIS_RANK = {"public": 0, "public_safe": 0, "reference_canon": 0, "internal": 2, "private": 3, "mod": 3, "sealed_test": 4, "protected": 4, "ai_image_tool": 4, "unknown": 5}


@dataclass(frozen=True)
class MomentObservationResult:
    outcome: str = "skipped"
    reason_code: str = "not_attempted"
    moment_id: str = ""
    ledger_entry_id: str = ""


@dataclass(frozen=True)
class SourceEntry:
    entry_id: str
    guild_id: int
    source_table: str
    source_role: str
    entry_type: str
    predicate_key: str
    normalized_value: str
    route_mode: str
    channel_id: int
    channel_name: str
    channel_policy: str
    visibility: str
    public_usable: bool
    observed_at: str
    source_sequence: int
    lifecycle_status: str
    subject_key: str
    subject_display_name: str

    @property
    def is_human(self) -> bool:
        return self.source_role == "user"

    @property
    def is_model(self) -> bool:
        return self.source_role != "user"


def shadow_enabled(environ: dict[str, str] | None = None) -> bool:
    return str((environ or os.environ).get(MOMENT_ENGINE_SHADOW_ENV, "")).strip().lower() in {"1", "true", "yes", "on", "enabled"}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_ts(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat((value or "").replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except Exception:
        return datetime.now(timezone.utc)


def _canon(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip().lower())


def _tokens(text: str) -> tuple[str, ...]:
    return tuple(t for t in re.findall(r"[a-z0-9]{2,}", _canon(text)) if t not in STOP)[:24]


def _topic_family(text: str, predicate_key: str) -> str:
    canon = _canon(text)
    toks = set(_tokens(canon))
    if predicate_key == "remembered_number" or (("number" in toks or "numbers" in toks) and ({"remember", "recall"} & toks or "what numbers" in canon)):
        return "remembered_number"
    families = (
        ("music_production", {"synth", "drum", "drums", "bass", "riff", "vocal", "mix", "patch", "bridge", "production"}),
        ("cooking", {"pizza", "oven", "dough", "sauce", "bake", "baking", "cheese"}),
        ("outdoors", {"hiking", "hike", "trail", "rain", "weather", "mountain", "boots", "conditions"}),
    )
    for name, markers in families:
        if toks & markers:
            return name
    if toks:
        return "topic:" + ":".join(sorted(toks)[:3])
    return "low_signal"


def _topic_signature(text: str, predicate_key: str) -> tuple[str, ...]:
    family = _topic_family(text, predicate_key)
    if family == "remembered_number":
        return ("remembered_number",)
    return tuple(sorted(set(_tokens(text))))[:16]


def _topic_key(family: str, signature: tuple[str, ...]) -> str:
    base = family + "|" + " ".join(signature)
    return "topic_" + hashlib.sha256(base.encode("utf-8")).hexdigest()[:16]


def stable_moment_id(guild_id: int, channel_id: int, topic_key: str, started_at: str) -> str:
    return "mom_" + hashlib.sha256(f"{MOMENT_SCHEMA_VERSION}\x1f{guild_id}\x1f{channel_id}\x1f{topic_key}\x1f{started_at}".encode("utf-8")).hexdigest()[:32]


def _meaningful(text: str, role: str, predicate_key: str) -> bool:
    canon = _canon(text)
    if role != "user":
        return False
    if predicate_key == "remembered_number" or any(marker in canon for marker in STRONG_MARKERS):
        return True
    if re.fullmatch(r"[!?.\s]+", canon or ""):
        return False
    toks = [t for t in _tokens(canon) if t not in LOW_SIGNAL]
    return len(toks) >= 2 or len(canon) >= 24


def _strong_marker(text: str, predicate_key: str) -> bool:
    canon = _canon(text)
    return predicate_key == "remembered_number" or any(marker in canon for marker in STRONG_MARKERS)


def _coherent(family: str, signature: tuple[str, ...], window_family: str, window_signature: tuple[str, ...]) -> bool:
    if not signature or family == "low_signal":
        return False
    if family == window_family and family != "topic":
        return True
    overlap = set(signature) & set(window_signature)
    if len(overlap) >= 2:
        return True
    denom = max(1, min(len(set(signature)), len(set(window_signature))))
    return (len(overlap) / denom) >= 0.5 and len(overlap) >= 1


def _json_sig(sig: tuple[str, ...]) -> str:
    return json.dumps(list(sig), sort_keys=True)


def _load_sig(raw: str) -> tuple[str, ...]:
    try:
        value = json.loads(raw or "[]")
        return tuple(str(v) for v in value if str(v))
    except Exception:
        return ()


def ensure_moment_schema(conn: sqlite3.Connection) -> None:
    ensure_memory_ledger_schema(conn)
    cur = conn.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS memory_moment_windows (
      moment_id TEXT PRIMARY KEY, guild_id INTEGER NOT NULL, channel_id INTEGER NOT NULL, channel_name TEXT,
      channel_policy TEXT, route_mode TEXT, topic_key TEXT NOT NULL, window_started_at TEXT NOT NULL,
      last_activity_at TEXT NOT NULL, finalized_at TEXT, qualification_type TEXT, qualification_reason TEXT,
      lifecycle_status TEXT NOT NULL, visibility TEXT, public_usable INTEGER DEFAULT 0, salience REAL DEFAULT 0,
      human_entry_count INTEGER DEFAULT 0, model_entry_count INTEGER DEFAULT 0, participant_count INTEGER DEFAULT 0,
      summary TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL)""")
    for sql in (
        "ALTER TABLE memory_moment_windows ADD COLUMN topic_family TEXT DEFAULT ''",
        "ALTER TABLE memory_moment_windows ADD COLUMN topic_signature TEXT DEFAULT '[]'",
        "ALTER TABLE memory_moment_windows ADD COLUMN canonical_ledger_entry_id TEXT DEFAULT ''",
    ):
        try:
            cur.execute(sql)
        except sqlite3.OperationalError:
            pass
    cur.execute("""CREATE TABLE IF NOT EXISTS memory_moment_members (
      moment_id TEXT NOT NULL, ledger_entry_id TEXT NOT NULL, source_sequence INTEGER DEFAULT 0, observed_at TEXT,
      membership_role TEXT, created_at TEXT NOT NULL, PRIMARY KEY(moment_id, ledger_entry_id))""")
    cur.execute("""CREATE TABLE IF NOT EXISTS memory_moment_participants (
      moment_id TEXT NOT NULL, participant_key TEXT NOT NULL, safe_display_name TEXT, participant_role TEXT,
      first_seen_at TEXT, last_seen_at TEXT, authored_entry_count INTEGER DEFAULT 0, participation_order INTEGER DEFAULT 0,
      created_at TEXT NOT NULL, updated_at TEXT NOT NULL, PRIMARY KEY(moment_id, participant_key, participant_role))""")
    cur.execute("""CREATE TABLE IF NOT EXISTS memory_moment_diagnostics (
      id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id INTEGER DEFAULT 0, moment_id TEXT DEFAULT '', event_type TEXT NOT NULL,
      reason_code TEXT DEFAULT '', ledger_entry_id TEXT DEFAULT '', created_at TEXT NOT NULL)""")
    for sql in [
        "CREATE INDEX IF NOT EXISTS idx_mmw_scope ON memory_moment_windows(guild_id, channel_id, lifecycle_status, last_activity_at)",
        "CREATE INDEX IF NOT EXISTS idx_mmw_canonical ON memory_moment_windows(guild_id, canonical_ledger_entry_id)",
        "CREATE INDEX IF NOT EXISTS idx_mmm_entry ON memory_moment_members(ledger_entry_id)",
        "CREATE INDEX IF NOT EXISTS idx_mmp_participant ON memory_moment_participants(participant_key, moment_id)",
        "CREATE INDEX IF NOT EXISTS idx_mmd_guild ON memory_moment_diagnostics(guild_id, event_type, reason_code)",
    ]:
        cur.execute(sql)


def _diag(conn: sqlite3.Connection, guild_id: int, event: str, reason: str = "", moment_id: str = "", entry_id: str = "") -> None:
    conn.execute(
        "INSERT INTO memory_moment_diagnostics(guild_id,moment_id,event_type,reason_code,ledger_entry_id,created_at) VALUES(?,?,?,?,?,?)",
        (int(guild_id or 0), moment_id or "", event[:80], reason[:120], entry_id or "", _now()),
    )


def _fetch_entry(conn: sqlite3.Connection, entry_id: str) -> SourceEntry | None:
    row = conn.execute(
        """
        SELECT entry_id,guild_id,source_table,source_role,entry_type,predicate_key,normalized_value,route_mode,
               channel_id,channel_name,channel_policy,visibility,public_usable,observed_at,source_sequence,
               lifecycle_status,subject_key,subject_display_name
        FROM memory_ledger_entries WHERE entry_id=?
        """,
        (entry_id,),
    ).fetchone()
    if not row:
        return None
    return SourceEntry(
        entry_id=row[0], guild_id=int(row[1] or 0), source_table=row[2] or "", source_role=row[3] or "",
        entry_type=row[4] or "", predicate_key=row[5] or "", normalized_value=row[6] or "", route_mode=row[7] or "unknown",
        channel_id=int(row[8] or 0), channel_name=row[9] or "", channel_policy=row[10] or "unknown", visibility=row[11] or "unknown",
        public_usable=bool(row[12]), observed_at=row[13] or _now(), source_sequence=int(row[14] or 0), lifecycle_status=row[15] or "",
        subject_key=row[16] or "", subject_display_name=row[17] or "",
    )


def _entries(conn: sqlite3.Connection, moment_id: str) -> list[SourceEntry]:
    rows = conn.execute(
        """
        SELECT e.entry_id,e.guild_id,e.source_table,e.source_role,e.entry_type,e.predicate_key,e.normalized_value,e.route_mode,
               e.channel_id,e.channel_name,e.channel_policy,e.visibility,e.public_usable,e.observed_at,e.source_sequence,
               e.lifecycle_status,e.subject_key,e.subject_display_name
        FROM memory_moment_members m JOIN memory_ledger_entries e ON e.entry_id=m.ledger_entry_id
        WHERE m.moment_id=? ORDER BY e.observed_at, e.source_sequence, e.entry_id
        """,
        (moment_id,),
    ).fetchall()
    return [SourceEntry(r[0], int(r[1] or 0), r[2] or "", r[3] or "", r[4] or "", r[5] or "", r[6] or "", r[7] or "unknown", int(r[8] or 0), r[9] or "", r[10] or "unknown", r[11] or "unknown", bool(r[12]), r[13] or _now(), int(r[14] or 0), r[15] or "", r[16] or "", r[17] or "") for r in rows]


def _mark_targets_for_correction(conn: sqlite3.Connection, source: SourceEntry) -> int:
    targets = [r[0] for r in conn.execute(
        "SELECT target_entry_id FROM memory_ledger_lineage WHERE guild_id=? AND entry_id=? AND lineage_type IN ('correction_of','supersedes','retracts')",
        (source.guild_id, source.entry_id),
    ).fetchall()]
    count = 0
    for target in targets:
        count += handle_source_correction(conn, target, guild_id=source.guild_id)
    return count


def observe_ledger_entry(conn: sqlite3.Connection, ledger_entry_id: str) -> MomentObservationResult:
    if not shadow_enabled():
        return MomentObservationResult(reason_code="moment_gate_disabled", ledger_entry_id=ledger_entry_id)
    if not ledger_shadow_enabled():
        ensure_moment_schema(conn)
        _diag(conn, 0, "moment_processing_skipped", "ledger_shadow_unavailable", entry_id=ledger_entry_id)
        return MomentObservationResult(reason_code="ledger_shadow_unavailable", ledger_entry_id=ledger_entry_id)
    ensure_moment_schema(conn)
    try:
        conn.execute("SAVEPOINT moment_observe")
        existing = conn.execute("SELECT moment_id FROM memory_moment_members WHERE ledger_entry_id=? ORDER BY created_at LIMIT 1", (ledger_entry_id,)).fetchone()
        if existing:
            conn.execute("RELEASE moment_observe")
            return MomentObservationResult("deduplicated", "exact_source_duplicate", existing[0], ledger_entry_id)
        source = _fetch_entry(conn, ledger_entry_id)
        if not source:
            conn.execute("RELEASE moment_observe")
            return MomentObservationResult(reason_code="missing_ledger_entry", ledger_entry_id=ledger_entry_id)
        if source.source_table != "conversations" or source.lifecycle_status not in {"active", "review_only"} or source.entry_type not in {"observation", "derived_summary"}:
            conn.execute("RELEASE moment_observe")
            return MomentObservationResult(reason_code="ineligible_source", ledger_entry_id=source.entry_id)
        _diag(conn, source.guild_id, "eligible_ledger_entry_observed", "ok", entry_id=source.entry_id)
        _mark_targets_for_correction(conn, source)
        family = _topic_family(source.normalized_value, source.predicate_key)
        signature = _topic_signature(source.normalized_value, source.predicate_key)
        meaningful = _meaningful(source.normalized_value, source.source_role, source.predicate_key)
        ts = _parse_ts(source.observed_at)
        chosen = ""
        open_rows = conn.execute(
            """
            SELECT moment_id,window_started_at,last_activity_at,channel_policy,visibility,topic_family,topic_signature
            FROM memory_moment_windows
            WHERE guild_id=? AND channel_id=? AND lifecycle_status='open'
            ORDER BY last_activity_at DESC, moment_id
            """,
            (source.guild_id, source.channel_id),
        ).fetchall()
        for row in open_rows:
            mid, started, last, policy, visibility, win_family, win_sig_raw = row
            expired = (ts - _parse_ts(last)).total_seconds() > INACTIVITY_SECONDS or (ts - _parse_ts(started)).total_seconds() > MAX_WINDOW_SECONDS
            incompatible = policy != source.channel_policy or visibility != source.visibility
            if expired or incompatible:
                finalize_moment(conn, mid)
                continue
            if source.is_model or not meaningful:
                if not chosen:
                    chosen = mid
                continue
            if _coherent(family, signature, win_family or "", _load_sig(win_sig_raw)):
                chosen = mid
                break
            finalize_moment(conn, mid)
            _diag(conn, source.guild_id, "window_split", "topic_coherence_mismatch", mid, source.entry_id)
        if not chosen:
            topic_key = _topic_key(family, signature)
            started = source.observed_at or _now()
            chosen = stable_moment_id(source.guild_id, source.channel_id, topic_key, started)
            conn.execute(
                """
                INSERT OR IGNORE INTO memory_moment_windows(
                    moment_id,guild_id,channel_id,channel_name,channel_policy,route_mode,topic_key,topic_family,topic_signature,
                    window_started_at,last_activity_at,lifecycle_status,visibility,public_usable,created_at,updated_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (chosen, source.guild_id, source.channel_id, source.channel_name, source.channel_policy, source.route_mode, topic_key, family, _json_sig(signature), started, started, "open", source.visibility, int(source.public_usable), _now(), _now()),
            )
            _diag(conn, source.guild_id, "window_opened", "ok", chosen, source.entry_id)
        _insert_membership(conn, chosen, source, meaningful, family, signature)
        conn.execute("RELEASE moment_observe")
        return MomentObservationResult("observed", "ok", chosen, source.entry_id)
    except Exception:
        try:
            conn.execute("ROLLBACK TO moment_observe")
            conn.execute("RELEASE moment_observe")
        except Exception:
            pass
        try:
            _diag(conn, 0, "moment_processing_error", "exception", entry_id=ledger_entry_id)
        except Exception:
            pass
        return MomentObservationResult("error", "exception", ledger_entry_id=ledger_entry_id)


def _insert_membership(conn: sqlite3.Connection, moment_id: str, source: SourceEntry, meaningful: bool, family: str, signature: tuple[str, ...]) -> None:
    role_name = "human_author" if source.is_human else "bnl_participant"
    before = conn.execute("SELECT COUNT(*) FROM memory_moment_members WHERE moment_id=? AND ledger_entry_id=?", (moment_id, source.entry_id)).fetchone()[0]
    conn.execute("INSERT OR IGNORE INTO memory_moment_members VALUES(?,?,?,?,?,?)", (moment_id, source.entry_id, source.source_sequence, source.observed_at, role_name, _now()))
    if before:
        _diag(conn, source.guild_id, "exact_source_duplicate_ignored", "duplicate", moment_id, source.entry_id)
        return
    pkey = source.subject_key if source.is_human else BNL_SUBJECT_KEY
    pname = source.subject_display_name if source.is_human else "BNL-01"
    existing = conn.execute("SELECT participation_order FROM memory_moment_participants WHERE moment_id=? AND participant_key=? AND participant_role=?", (moment_id, pkey, role_name)).fetchone()
    if existing:
        conn.execute(
            "UPDATE memory_moment_participants SET last_seen_at=?, authored_entry_count=authored_entry_count+?, updated_at=? WHERE moment_id=? AND participant_key=? AND participant_role=?",
            (source.observed_at, 1 if source.is_human else 0, _now(), moment_id, pkey, role_name),
        )
    else:
        order = conn.execute("SELECT COUNT(*) FROM memory_moment_participants WHERE moment_id=?", (moment_id,)).fetchone()[0]
        conn.execute("INSERT INTO memory_moment_participants VALUES(?,?,?,?,?,?,?,?,?,?)", (moment_id, pkey, (pname or "")[:120], role_name, source.observed_at, source.observed_at, 1 if source.is_human else 0, order, _now(), _now()))
    if source.is_human and meaningful:
        old = _load_sig(conn.execute("SELECT topic_signature FROM memory_moment_windows WHERE moment_id=?", (moment_id,)).fetchone()[0])
        merged = tuple(sorted(set(old) | set(signature)))[:24]
        conn.execute("UPDATE memory_moment_windows SET topic_family=?, topic_signature=? WHERE moment_id=?", (family, _json_sig(merged), moment_id))
    _recount(conn, moment_id)
    _diag(conn, source.guild_id, "window_extended", "ok", moment_id, source.entry_id)


def _recount(conn: sqlite3.Connection, moment_id: str) -> None:
    rows = _entries(conn, moment_id)
    humans = [r for r in rows if _meaningful(r.normalized_value, r.source_role, r.predicate_key)]
    models = [r for r in rows if r.is_model]
    parts = len({r.subject_key for r in humans})
    visibility = max([r.visibility for r in rows] or ["unknown"], key=lambda v: VIS_RANK.get(v, 5))
    public_usable = bool(rows) and all(r.public_usable for r in rows) and VIS_RANK.get(visibility, 5) <= VIS_RANK.get("public_safe", 0)
    conn.execute(
        """
        UPDATE memory_moment_windows
        SET human_entry_count=?, model_entry_count=?, participant_count=?, visibility=?, public_usable=?,
            last_activity_at=COALESCE((SELECT MAX(observed_at) FROM memory_moment_members WHERE moment_id=?), last_activity_at), updated_at=?
        WHERE moment_id=?
        """,
        (len(humans), len(models), parts, visibility, int(public_usable), moment_id, _now(), moment_id),
    )


def _qualify(rows: list[SourceEntry]) -> tuple[str, str, list[SourceEntry], list[SourceEntry]]:
    humans = [r for r in rows if _meaningful(r.normalized_value, r.source_role, r.predicate_key)]
    models = [r for r in rows if r.is_model]
    human_parts = {r.subject_key for r in humans}
    strong = any(_strong_marker(r.normalized_value, r.predicate_key) for r in humans)
    if len(human_parts) >= 2 and len(humans) >= 3:
        return "shared_activity", "two_humans_three_meaningful_entries", humans, models
    if len(human_parts) == 1 and models and ((len(humans) >= 2 and strong) or len(humans) >= 3):
        return "conversational", "one_human_bnl_continuity", humans, models
    return "", "low_signal_or_insufficient_continuity", humans, models


def _summary(rows: list[SourceEntry], qtype: str, reason: str) -> str:
    vals: list[str] = []
    for row in rows:
        if row.is_human and (row.predicate_key == "remembered_number" or _strong_marker(row.normalized_value, row.predicate_key)):
            vals.append(f"{row.predicate_key}={row.normalized_value[:80]}")
    detail = ("; ".join(dict.fromkeys(vals)) or "bounded coherent conversation activity")[:240]
    return f"Derived moment ({qtype}): {detail}; reason={reason}."


def _existing_moment_entry_id(conn: sqlite3.Connection, moment_id: str, guild_id: int | None = None) -> str:
    row = conn.execute("SELECT canonical_ledger_entry_id FROM memory_moment_windows WHERE moment_id=?", (moment_id,)).fetchone()
    if row and row[0] and conn.execute("SELECT 1 FROM memory_ledger_entries WHERE entry_id=?", (row[0],)).fetchone():
        return row[0]
    params: list[Any] = [str(moment_id)]
    where = "source_table='memory_moment_windows' AND source_row_id=? AND entry_type='shared_moment'"
    if guild_id is not None:
        where += " AND guild_id=?"
        params.append(guild_id)
    row = conn.execute(f"SELECT entry_id FROM memory_ledger_entries WHERE {where} ORDER BY created_at LIMIT 1", params).fetchone()
    return row[0] if row else ""


def finalize_moment(conn: sqlite3.Connection, moment_id: str) -> MomentObservationResult:
    ensure_moment_schema(conn)
    win = conn.execute(
        "SELECT guild_id,channel_id,channel_name,channel_policy,route_mode,topic_key,window_started_at,last_activity_at,visibility,public_usable,lifecycle_status,canonical_ledger_entry_id FROM memory_moment_windows WHERE moment_id=?",
        (moment_id,),
    ).fetchone()
    if not win:
        return MomentObservationResult(reason_code="missing_window", moment_id=moment_id)
    existing = _existing_moment_entry_id(conn, moment_id, int(win[0] or 0))
    if win[10] == "finalized" and existing:
        return MomentObservationResult("deduplicated", "already_finalized", moment_id, existing)
    rows = _entries(conn, moment_id)
    qtype, reason, humans, _models = _qualify(rows)
    if not qtype:
        conn.execute("UPDATE memory_moment_windows SET lifecycle_status='rejected', qualification_reason=?, finalized_at=?, updated_at=? WHERE moment_id=?", (reason, _now(), _now(), moment_id))
        _diag(conn, int(win[0] or 0), "window_rejected", reason, moment_id)
        return MomentObservationResult("rejected", reason, moment_id)
    source_ids = [r.entry_id for r in rows]
    if any(not conn.execute("SELECT 1 FROM memory_ledger_entries WHERE entry_id=?", (entry_id,)).fetchone() for entry_id in source_ids):
        _diag(conn, int(win[0] or 0), "lineage_validation_failure", "dangling_source", moment_id)
        return MomentObservationResult("error", "dangling_source", moment_id)
    participant_count = len({r.subject_key for r in humans})
    salience = min(1.0, 0.15 + 0.10 * len(humans) + 0.10 * participant_count + (0.08 if qtype == "conversational" else 0.12))
    summary = _summary(rows, qtype, reason)
    participants = [LedgerParticipant(p[0], p[1] or "", p[2], int(p[3] or 0)) for p in conn.execute("SELECT participant_key,safe_display_name,participant_role,participation_order FROM memory_moment_participants WHERE moment_id=? ORDER BY participation_order,participant_key", (moment_id,)).fetchall()]
    visibility = win[8] or "unknown"
    public_usable = bool(win[9]) and VIS_RANK.get(visibility, 5) <= VIS_RANK.get("public_safe", 0)
    value = json.dumps({
        "schema": MOMENT_SCHEMA_VERSION, "moment_id": moment_id, "summary": summary, "window_started_at": win[6], "last_activity_at": win[7],
        "topic_key": win[5], "qualification_type": qtype, "qualification_reason": reason, "salience": salience,
        "participants": [p.participant_key for p in participants], "public_usable": public_usable, "lifecycle_status": "finalized", "source_revision": "1",
    }, sort_keys=True)
    entry = LedgerEntry(
        guild_id=int(win[0] or 0), source_table="memory_moment_windows", source_row_id=moment_id, source_revision="1",
        source_role="derived_assessment", entry_type="shared_moment", subject_key=f"moment:{moment_id}", predicate_key="shared_moment",
        value=value, source_class=SourceClass.DERIVED_SUMMARY, route_mode=win[4] or "unknown", channel_id=int(win[1] or 0),
        channel_name=win[2] or "", channel_policy=win[3] or "", visibility=Visibility(visibility), confidence=Confidence.LOW,
        public_usable=public_usable, derived=True, projection=True, salience=salience, observed_at=win[7], source_sequence=0,
        valid_from=win[6], valid_until=win[7], freshness="shadow_moment_v1", lifecycle_status="review_only",
        participants=tuple(participants), lineage=tuple(("derived_from", source_id) for source_id in source_ids),
    )
    result = insert_ledger_entry(conn, entry)
    moment_entry_id = result.entry_id or _existing_moment_entry_id(conn, moment_id, int(win[0] or 0))
    for source_id in source_ids:
        conn.execute("INSERT OR IGNORE INTO memory_ledger_lineage VALUES(?,?,?,?,?)", (source_id, int(win[0] or 0), "part_of_moment", moment_entry_id, _now()))
    conn.execute(
        """
        UPDATE memory_moment_windows SET lifecycle_status='finalized', finalized_at=?, qualification_type=?, qualification_reason=?,
            salience=?, summary=?, canonical_ledger_entry_id=?, updated_at=? WHERE moment_id=?
        """,
        (_now(), qtype, reason, salience, summary, moment_entry_id, _now(), moment_id),
    )
    _diag(conn, int(win[0] or 0), "window_finalized", reason, moment_id, moment_entry_id)
    return MomentObservationResult(result.outcome, result.reason_code, moment_id, moment_entry_id)


def sweep_expired_windows(conn: sqlite3.Connection, *, guild_id: int | None = None, now: str | None = None) -> list[MomentObservationResult]:
    if not shadow_enabled() or not ledger_shadow_enabled():
        return []
    ensure_moment_schema(conn)
    base = _parse_ts(now or _now())
    params: list[Any] = []
    where = "lifecycle_status='open'"
    if guild_id is not None:
        where += " AND guild_id=?"
        params.append(guild_id)
    results: list[MomentObservationResult] = []
    for moment_id, last_activity_at in conn.execute(f"SELECT moment_id,last_activity_at FROM memory_moment_windows WHERE {where}", params).fetchall():
        if (base - _parse_ts(last_activity_at)).total_seconds() >= INACTIVITY_SECONDS:
            results.append(finalize_moment(conn, moment_id))
    return results


def handle_source_correction(conn: sqlite3.Connection, source_entry_id: str, *, guild_id: int | None = None) -> int:
    ensure_moment_schema(conn)
    params: list[Any] = [source_entry_id]
    sql = "SELECT DISTINCT w.moment_id,w.guild_id FROM memory_moment_members m JOIN memory_moment_windows w ON w.moment_id=m.moment_id WHERE m.ledger_entry_id=?"
    if guild_id is not None:
        sql += " AND w.guild_id=?"
        params.append(guild_id)
    rows = conn.execute(sql, params).fetchall()
    for moment_id, gid in rows:
        conn.execute("UPDATE memory_moment_windows SET lifecycle_status='needs_review', updated_at=? WHERE moment_id=? AND lifecycle_status IN ('open','finalized')", (_now(), moment_id))
        _diag(conn, int(gid or 0), "moment_awaiting_review", "source_corrected", moment_id, source_entry_id)
    return len(rows)


def render_shadow_moment_context(conn: sqlite3.Connection, *, guild_id: int, channel_id: int, participant_key: str = "", visibility: str = "public_safe", topic_text: str = "", token_budget: int = 120, freshness_days: int = 3650) -> str:
    ensure_moment_schema(conn)
    cutoff = (datetime.now(timezone.utc) - timedelta(days=freshness_days)).isoformat()
    family = _topic_family(topic_text, "conversation")
    signature = _topic_signature(topic_text, "conversation")
    lines: list[str] = []
    used = 0
    for row in conn.execute(
        """
        SELECT moment_id,summary,topic_family,topic_signature,visibility,last_activity_at,salience
        FROM memory_moment_windows
        WHERE guild_id=? AND channel_id=? AND lifecycle_status='finalized' AND last_activity_at>=?
        ORDER BY salience DESC,last_activity_at DESC
        """,
        (guild_id, channel_id, cutoff),
    ).fetchall():
        if VIS_RANK.get(row[4], 5) > VIS_RANK.get(visibility, 0):
            continue
        if participant_key and not conn.execute("SELECT 1 FROM memory_moment_participants WHERE moment_id=? AND participant_key=?", (row[0], participant_key)).fetchone():
            continue
        if signature and not _coherent(family, signature, row[2] or "", _load_sig(row[3])):
            continue
        line = f"[Derived moment context] {row[1]}"
        words = line.split()
        if used + len(words) > token_budget:
            if not lines:
                lines.append(" ".join(words[:max(1, token_budget)]))
            break
        lines.append(line)
        used += len(words)
    return "\n".join(lines)


def build_moment_evaluation_report(conn: sqlite3.Connection, *, guild_id: int | None = None) -> dict[str, Any]:
    ensure_moment_schema(conn)
    where = ""
    params: list[Any] = []
    if guild_id is not None:
        where = " WHERE guild_id=?"
        params = [guild_id]
    def one(sql: str, bind: list[Any] | None = None) -> Any:
        return conn.execute(sql, [] if bind is None else bind).fetchone()[0]
    scoped = "guild_id=? AND " if guild_id is not None else ""
    p = [guild_id] if guild_id is not None else []
    report = {
        "eligible_entries_observed": one(f"SELECT COUNT(*) FROM memory_moment_diagnostics{where + (' AND' if where else ' WHERE')} event_type='eligible_ledger_entry_observed'", params),
        "open_windows": one(f"SELECT COUNT(*) FROM memory_moment_windows{where + (' AND' if where else ' WHERE')} lifecycle_status='open'", params),
        "finalized_moments": one(f"SELECT COUNT(*) FROM memory_moment_windows{where + (' AND' if where else ' WHERE')} lifecycle_status='finalized'", params),
        "processing_errors": one(f"SELECT COUNT(*) FROM memory_moment_diagnostics{where + (' AND' if where else ' WHERE')} event_type='moment_processing_error'", params),
        "rejected_windows_by_reason": dict(conn.execute(f"SELECT qualification_reason,COUNT(*) FROM memory_moment_windows{where + (' AND' if where else ' WHERE')} lifecycle_status='rejected' GROUP BY qualification_reason", params).fetchall()),
        "moments_by_qualification_type": dict(conn.execute(f"SELECT qualification_type,COUNT(*) FROM memory_moment_windows{where} GROUP BY qualification_type", params).fetchall()),
        "moments_by_visibility": dict(conn.execute(f"SELECT visibility,COUNT(*) FROM memory_moment_windows{where} GROUP BY visibility", params).fetchall()),
        "moments_by_lifecycle": dict(conn.execute(f"SELECT lifecycle_status,COUNT(*) FROM memory_moment_windows{where} GROUP BY lifecycle_status", params).fetchall()),
        "one_human_conversational_moments": one(f"SELECT COUNT(*) FROM memory_moment_windows WHERE {scoped}qualification_type='conversational'", p),
        "multi_human_shared_moments": one(f"SELECT COUNT(*) FROM memory_moment_windows WHERE {scoped}qualification_type='shared_activity'", p),
        "average_participant_count": one(f"SELECT COALESCE(AVG(participant_count),0) FROM memory_moment_windows WHERE {scoped}lifecycle_status='finalized'", p),
        "duplicate_memberships": one(f"SELECT COUNT(*) FROM (SELECT m.moment_id,m.ledger_entry_id,COUNT(*) c FROM memory_moment_members m JOIN memory_moment_windows w ON w.moment_id=m.moment_id WHERE {scoped}1=1 GROUP BY m.moment_id,m.ledger_entry_id HAVING c>1)", p),
        "bnl_only_violations": one(f"SELECT COUNT(*) FROM memory_moment_windows WHERE {scoped}lifecycle_status='finalized' AND human_entry_count=0", p),
        "cross_guild_violations": one(f"SELECT COUNT(*) FROM memory_moment_members mm JOIN memory_moment_windows mw ON mw.moment_id=mm.moment_id JOIN memory_ledger_entries e ON e.entry_id=mm.ledger_entry_id WHERE {('mw.guild_id=? AND ' if guild_id is not None else '')}e.guild_id<>mw.guild_id", p),
        "cross_channel_violations": one(f"SELECT COUNT(*) FROM memory_moment_members mm JOIN memory_moment_windows mw ON mw.moment_id=mm.moment_id JOIN memory_ledger_entries e ON e.entry_id=mm.ledger_entry_id WHERE {('mw.guild_id=? AND ' if guild_id is not None else '')}e.channel_id<>mw.channel_id", p),
        "incompatible_visibility_violations": one(f"SELECT COUNT(*) FROM memory_moment_members mm JOIN memory_moment_windows mw ON mw.moment_id=mm.moment_id JOIN memory_ledger_entries e ON e.entry_id=mm.ledger_entry_id WHERE {('mw.guild_id=? AND ' if guild_id is not None else '')}e.visibility<>mw.visibility", p),
        "dangling_lineage_targets": one(f"SELECT COUNT(*) FROM memory_ledger_lineage l LEFT JOIN memory_ledger_entries e ON e.guild_id=l.guild_id AND e.entry_id=l.target_entry_id WHERE {('l.guild_id=? AND ' if guild_id is not None else '')}e.entry_id IS NULL", p),
        "affected_moments_awaiting_correction_review": one(f"SELECT COUNT(*) FROM memory_moment_windows{where + (' AND' if where else ' WHERE')} lifecycle_status='needs_review'", params),
        "finalization_latency": one(f"SELECT COALESCE(AVG(strftime('%s',finalized_at)-strftime('%s',last_activity_at)),0) FROM memory_moment_windows{where + (' AND' if where else ' WHERE')} finalized_at IS NOT NULL", params),
    }
    return report
