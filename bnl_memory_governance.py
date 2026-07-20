"""Deterministic governed durable-memory candidate assembly and member controls.

Scoring weights are fixed so stable inputs produce stable output. Candidate score =
explicit recall 2.0 + subject match 3.0 + topic overlap 0.45/term (max 2.7) +
open loop/commitment 1.0 + authority rank up to 2.8 + confidence rank up to 1.2
+ freshness up to 1.0 + salience up to 1.0 + participant continuity 0.8 +
correction state 2.5. Tie-break order: score desc, authority desc, confidence
desc, observed_at desc, source class, source ref, entry id.
"""
from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
import hashlib
import json
import os
import re
import sqlite3
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from bnl_dossier_source_packets import subject_key as normalized_subject_key
from bnl_journal import purge_user_journal_derivatives_on_connection
from bnl_journal_source_store import purge_user_discord_sources_on_connection
from bnl_memory_ledger import ensure_memory_ledger_schema, subject_key_for_user
from bnl_relationship_engine import complete_delete_relationship_v2, ensure_relationship_v2_schema, propagate_ledger_lifecycle

SHADOW_ENV = "BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED"
LIVE_ENV = "BNL_MEMORY_GOVERNANCE_LIVE_ENABLED"
BLOCKED_LIFECYCLES = {"corrected", "superseded", "retracted", "expired", "quarantined", "review_only", "needs_review", "forgotten", "deleted", "unresolved"}
PUBLIC_VIS = {"public", "public_safe", "reference_canon"}
INTERNAL_VIS = PUBLIC_VIS | {"internal", "private", "mod", "operator_only"}
DURABLE_ENTRY_TYPES = {"claim", "preference", "boundary", "goal", "open_loop", "commitment", "canon_reference", "unresolved_question", "event"}
DURABLE_PREDICATES = {"fact", "preference", "favorite", "boundary", "goal", "open_loop", "commitment", "canon_reference", "unresolved_question", "correction", "favorite_color", "favorite_food", "remembered_number"}
NON_LIVE_PREDICATES = {"conversation", "model_output", "assistant_response", "raw_message"}
PROJECTION_CLASSES = {"derived_summary", "evidence_projection", "source_file_projection", "dossier_projection", "entity_evidence_projection", "legacy_source_blind"}
AUTHORITY = {"legacy_source_blind": 0, "derived_summary": 1, "evidence_projection": 2, "entity_evidence_projection": 2, "dossier_projection": 2, "source_file_projection": 2, "public_observation": 3, "runtime_observation": 4, "first_party_record": 5, "approved_canon": 6, "owner_correction": 7}
CONF = {"unknown": 0, "low": 1, "medium": 2, "high": 3, "approved": 4}
BROAD_RECALL_RE = re.compile(r"\b(what do you (?:know|remember) about me|what do you remember|my memory|everything you know|recall my)\b", re.I)
RECALL_RE = re.compile(r"\b(remember|memory|recall|what do you know|what is my|what's my|my)\b", re.I)

@dataclass(frozen=True)
class GovernanceRequest:
    guild_id: int
    subject_user_id: int
    route_mode: str
    conversation_surface: str
    channel_id: int = 0
    channel_name: str = ""
    channel_policy: str = "unknown"
    visibility_allowance: str = "public_safe"
    user_text: str = ""
    topic_terms: Tuple[str, ...] = ()
    participants: Tuple[str, ...] = ()
    direct_state: str = "direct"
    budget_chars: int = 1200
    allowed_source_classes: Tuple[str, ...] = ()
    now: str = ""

@dataclass(frozen=True)
class MemoryCandidate:
    source_class: str
    source_type: str
    source_ref: str
    entry_id: str
    guild_id: int
    subject_key: str
    predicate_key: str
    entry_type: str
    text: str
    visibility: str
    confidence: str
    lifecycle: str
    authority: int
    salience: float = 0.0
    observed_at: str = ""
    valid_from: str = ""
    valid_until: str = ""
    derived: bool = False
    projection: bool = False
    participants: Tuple[str, ...] = ()
    lineage: Tuple[Tuple[str, str], ...] = ()
    score: float = 0.0
    eligible_root: bool = True

@dataclass(frozen=True)
class GovernanceExclusion:
    source_ref: str
    reason: str
    source_class: str = ""
    entry_id: str = ""

@dataclass
class GovernanceDiagnostics:
    route_policy: Dict[str, Any] = field(default_factory=dict)
    candidates_by_source: Dict[str, int] = field(default_factory=dict)
    selected_by_source: Dict[str, int] = field(default_factory=dict)
    selected_count: int = 0
    excluded_by_reason: Dict[str, int] = field(default_factory=dict)
    contradiction_resolutions: List[str] = field(default_factory=list)
    correction_supersession_exclusions: int = 0
    moment_candidate_count: int = 0
    moment_needs_review_excluded: int = 0
    visibility_exclusions: int = 0
    token_budget_exclusions: int = 0
    duplicate_suppression: int = 0
    invalid_invariants: List[str] = field(default_factory=list)
    rendered_size: int = 0
    rendered_hash: str = ""
    legacy_vs_governed: Dict[str, Any] = field(default_factory=dict)
    processing_errors: List[str] = field(default_factory=list)
    fallback_reason: str = ""

@dataclass(frozen=True)
class GovernanceResult:
    rendered_context: str
    selected: Tuple[MemoryCandidate, ...]
    exclusions: Tuple[GovernanceExclusion, ...]
    diagnostics: GovernanceDiagnostics

def gate_enabled(name: str, environ: Optional[Dict[str, str]] = None) -> bool:
    return str((environ or os.environ).get(name, "")).strip().lower() in {"1", "true", "yes", "on", "enabled"}

def shadow_enabled(environ: Optional[Dict[str, str]] = None) -> bool:
    return gate_enabled(SHADOW_ENV, environ)

def live_enabled(environ: Optional[Dict[str, str]] = None) -> bool:
    return shadow_enabled(environ) and gate_enabled(LIVE_ENV, environ)

def _terms(s: str) -> Set[str]:
    return {t for t in re.findall(r"[a-z0-9]{3,}", (s or "").lower()) if t not in {"what", "whats", "remember", "memory", "about", "know", "does", "this", "that", "queue"}}

def _hash(s: str) -> str:
    return hashlib.sha256((s or "").encode("utf-8")).hexdigest()[:16]

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()

def _parse_time(value: str) -> float:
    if not value:
        return 0.0
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
    except Exception:
        return 0.0

def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return bool(conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone())

def _cols(conn: sqlite3.Connection, table: str) -> Set[str]:
    return {r[1] for r in conn.execute("PRAGMA table_info(%s)" % table).fetchall()}

def _add(diag: GovernanceDiagnostics, key: str, n: int = 1) -> None:
    diag.excluded_by_reason[key] = diag.excluded_by_reason.get(key, 0) + n

def ensure_governance_schema(conn: sqlite3.Connection) -> None:
    ensure_memory_ledger_schema(conn)
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS memory_governance_receipts (receipt_id TEXT PRIMARY KEY, guild_id INTEGER NOT NULL, subject_hash TEXT NOT NULL, action TEXT NOT NULL, target_ref TEXT DEFAULT '', created_at TEXT NOT NULL, row_counts_json TEXT DEFAULT '{}')")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_mgr_subject ON memory_governance_receipts(guild_id,subject_hash,action)")
    cur.execute("CREATE TABLE IF NOT EXISTS memory_governance_shadow_runs (run_id TEXT PRIMARY KEY, guild_id INTEGER NOT NULL, subject_hash TEXT NOT NULL, route_mode TEXT, channel_policy TEXT, created_at TEXT NOT NULL, rendered_hash TEXT, rendered_size INTEGER, legacy_hash TEXT, legacy_size INTEGER, selected_count INTEGER, excluded_json TEXT, errors_json TEXT, diagnostics_json TEXT DEFAULT '{}')")
    if "diagnostics_json" not in {row[1] for row in cur.execute("PRAGMA table_info(memory_governance_shadow_runs)").fetchall()}:
        cur.execute("ALTER TABLE memory_governance_shadow_runs ADD COLUMN diagnostics_json TEXT DEFAULT '{}'")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_mgs_guild ON memory_governance_shadow_runs(guild_id, created_at)")
    conn.commit()

def _safe_sql_text_col(cols: Set[str], preferred: Iterable[str]) -> str:
    for c in preferred:
        if c in cols:
            return c
    return ""

def _lineage(conn: sqlite3.Connection, guild_id: int, entry_id: str) -> Tuple[Tuple[str, str], ...]:
    return tuple((str(a), str(b)) for a, b in conn.execute("SELECT lineage_type,target_entry_id FROM memory_ledger_lineage WHERE guild_id=? AND entry_id=? ORDER BY lineage_type,target_entry_id", (guild_id, entry_id)).fetchall())

def _has_eligible_projection_root(conn: sqlite3.Connection, guild_id: int, entry_id: str, seen: Optional[Set[str]] = None) -> bool:
    seen = seen or set()
    if entry_id in seen:
        return False
    seen.add(entry_id)
    rows = conn.execute("SELECT target_entry_id FROM memory_ledger_lineage WHERE guild_id=? AND entry_id=? AND lineage_type='derived_from'", (guild_id, entry_id)).fetchall()
    if not rows:
        return False
    for (target,) in rows:
        r = conn.execute("SELECT source_class,lifecycle_status,derived,projection,predicate_key,entry_type FROM memory_ledger_entries WHERE guild_id=? AND entry_id=?", (guild_id, target)).fetchone()
        if not r:
            continue
        sc, life, derived, projection, pred, etype = [str(x or "") for x in r]
        if life in BLOCKED_LIFECYCLES:
            continue
        if (not int(derived or 0) and not int(projection or 0) and sc not in PROJECTION_CLASSES and (pred in DURABLE_PREDICATES or etype in DURABLE_ENTRY_TYPES)):
            return True
        if _has_eligible_projection_root(conn, guild_id, target, seen):
            return True
    return False

def _entry_current(conn: sqlite3.Connection, guild_id: int, entry_id: str) -> bool:
    return not bool(conn.execute("SELECT 1 FROM memory_ledger_lineage WHERE guild_id=? AND target_entry_id=? AND lineage_type IN ('correction_of','supersedes','retracts')", (guild_id, entry_id)).fetchone())

def _valid_now(d: Dict[str, Any], now_ts: float) -> bool:
    start = _parse_time(str(d.get("valid_from") or "")); end = _parse_time(str(d.get("valid_until") or ""))
    return (not start or start <= now_ts) and (not end or end >= now_ts)

def _classify_kind(source_class: str, entry_type: str, derived: bool, lifecycle: str) -> str:
    if source_class == "owner_correction":
        return "member-authored correction"
    if entry_type in {"preference", "claim"} and source_class in {"first_party_record", "owner_correction"}:
        return "member-authored fact"
    if derived or lifecycle == "review_only":
        return "review-only derived assessment"
    return "observation"

def _is_broad_recall(text: str) -> bool:
    return bool(BROAD_RECALL_RE.search(text or ""))

def _explicit_recall(text: str) -> bool:
    return bool(RECALL_RE.search(text or ""))

def _relevance_ok(c: MemoryCandidate, request_terms: Set[str], broad: bool) -> bool:
    if broad:
        return True
    if c.predicate_key in {"open_loop", "commitment", "goal", "unresolved_question"}:
        return True
    if not request_terms:
        return False
    cand_terms = _terms(c.text) | _terms(c.predicate_key.replace("_", " "))
    overlap = request_terms & cand_terms
    return len(overlap) >= (2 if len(request_terms) >= 2 else 1)

def _freshness_score(observed_at: str, now_ts: float) -> float:
    ts = _parse_time(observed_at)
    if not ts or not now_ts:
        return 0.0
    age_days = max(0.0, (now_ts - ts) / 86400.0)
    return max(0.0, 1.0 - min(age_days, 365.0) / 365.0)

def _moment_counts(conn: sqlite3.Connection, req: GovernanceRequest, diag: GovernanceDiagnostics) -> None:
    if not _table_exists(conn, "memory_moment_windows"):
        return
    try:
        subject = subject_key_for_user(req.subject_user_id)
        diag.moment_candidate_count = conn.execute("SELECT COUNT(DISTINCT w.moment_id) FROM memory_moment_windows w LEFT JOIN memory_moment_participants p ON p.moment_id=w.moment_id WHERE w.guild_id=? AND (p.participant_key=? OR w.canonical_ledger_entry_id IN (SELECT entry_id FROM memory_ledger_entries WHERE guild_id=? AND subject_key=?))", (req.guild_id, subject, req.guild_id, subject)).fetchone()[0]
        diag.moment_needs_review_excluded = conn.execute("SELECT COUNT(*) FROM memory_moment_windows WHERE guild_id=? AND lifecycle_status='needs_review'", (req.guild_id,)).fetchone()[0]
    except Exception as e:
        diag.processing_errors.append("moment:" + type(e).__name__)

def build_governed_context(conn: sqlite3.Connection, req: GovernanceRequest, *, legacy_context: str = "", include_review_moments: bool = False) -> GovernanceResult:
    diag = GovernanceDiagnostics(route_policy={"route_mode": req.route_mode, "channel_policy": req.channel_policy, "visibility": req.visibility_allowance})
    try:
        ensure_governance_schema(conn)
    except Exception as e:
        diag.processing_errors.append(type(e).__name__)
        return GovernanceResult("", (), (), diag)
    exclusions: List[GovernanceExclusion] = []
    cands: List[MemoryCandidate] = []
    subject = subject_key_for_user(req.subject_user_id)
    request_terms = set(req.topic_terms) or _terms(req.user_text)
    broad = _is_broad_recall(req.user_text)
    now_ts = _parse_time(req.now or _now())
    if req.route_mode == "simple_greeting" or (len(request_terms) <= 1 and re.fullmatch(r"\s*(hi|hello|hey|yo|sup|gm|good morning)[!. ]*\s*", req.user_text or "", re.I)):
        diag.fallback_reason = "simple_greeting_skip"
        return GovernanceResult("", (), (), diag)
    allowed = set(req.allowed_source_classes or AUTHORITY.keys())
    public_route = req.visibility_allowance in {"public", "public_safe"}
    try:
        cols = [c[1] for c in conn.execute("PRAGMA table_info(memory_ledger_entries)").fetchall()]
        for row in conn.execute("SELECT * FROM memory_ledger_entries WHERE guild_id=? AND subject_key=?", (req.guild_id, subject)).fetchall():
            d = dict(zip(cols, row))
            entry_id = str(d.get("entry_id") or "")
            source_class = str(d.get("source_class") or "")
            source_ref = "ledger:%s" % entry_id
            diag.candidates_by_source[source_class] = diag.candidates_by_source.get(source_class, 0) + 1
            def exclude(reason: str) -> None:
                exclusions.append(GovernanceExclusion(source_ref, reason, source_class, entry_id)); _add(diag, reason)
            if source_class not in allowed:
                exclude("route_source_class"); continue
            source_route = str(d.get("route_mode") or "unknown").lower()
            source_policy = str(d.get("channel_policy") or "unknown").lower()
            restricted_policies = {"sealed_test", "protected_system", "internal_controlled", "reference_canon", "ai_image_tool"}
            if source_policy in restricted_policies and source_policy != str(req.channel_policy or "").lower():
                diag.invalid_invariants.append("invalid_route_channel_policy_selected")
                exclude("invalid_route_channel_policy"); continue
            if source_route in {"operator_command", "internal_control", "protected_system"} and source_route != str(req.route_mode or "").lower():
                diag.invalid_invariants.append("invalid_route_channel_policy_selected")
                exclude("invalid_route_channel_policy"); continue
            life = str(d.get("lifecycle_status") or "active").lower()
            if life in BLOCKED_LIFECYCLES:
                exclude("lifecycle"); diag.correction_supersession_exclusions += 1; continue
            if not _entry_current(conn, req.guild_id, entry_id):
                exclude("superseded_or_retracted"); diag.correction_supersession_exclusions += 1; continue
            if conn.execute("SELECT 1 FROM memory_ledger_entries WHERE guild_id=? AND subject_key=? AND source_table=? AND source_row_id=? AND lifecycle_status='forgotten' AND entry_id<>?", (req.guild_id, subject, d.get("source_table"), d.get("source_row_id"), entry_id)).fetchone():
                exclude("forgotten_source_tombstone"); continue
            if not _valid_now(d, now_ts):
                exclude("validity_window"); continue
            pred = str(d.get("predicate_key") or "").lower()
            etype = str(d.get("entry_type") or "").lower()
            if pred in NON_LIVE_PREDICATES or (pred == "conversation" and etype == "observation") or etype == "model_output":
                exclude("non_durable_conversation"); continue
            if pred not in DURABLE_PREDICATES and etype not in DURABLE_ENTRY_TYPES and source_class != "owner_correction":
                exclude("non_durable_predicate"); continue
            derived = bool(int(d.get("derived") or 0)); projection = bool(int(d.get("projection") or 0))
            if derived or projection or source_class in PROJECTION_CLASSES:
                if not _has_eligible_projection_root(conn, req.guild_id, entry_id):
                    exclude("projection_lineage"); continue
                # Safe v1: keep projections shadow-measured only, never live-rendered.
                exclude("projection_shadow_only"); continue
            vis = str(d.get("visibility") or "unknown")
            if public_route and (vis not in PUBLIC_VIS or not int(d.get("public_usable") or 0) or source_class == "legacy_source_blind"):
                exclude("visibility"); diag.visibility_exclusions += 1; continue
            if vis not in INTERNAL_VIS:
                exclude("visibility"); diag.visibility_exclusions += 1; continue
            text = str(d.get("normalized_value") or "").strip()
            if not text or text.lower().strip() == (req.user_text or "").lower().strip():
                exclude("current_message_or_empty"); continue
            c = MemoryCandidate(source_class, source_class, source_ref, entry_id, int(d.get("guild_id") or 0), subject, pred, etype, text, vis, str(d.get("confidence") or "unknown"), life, AUTHORITY.get(source_class, 0), float(d.get("salience") or 0), str(d.get("observed_at") or ""), str(d.get("valid_from") or ""), str(d.get("valid_until") or ""), derived, projection, (), _lineage(conn, req.guild_id, entry_id), 0.0, True)
            if not _relevance_ok(c, request_terms, broad):
                exclude("topic_relevance"); continue
            cands.append(c)
        if include_review_moments:
            _moment_counts(conn, req, diag)
    except Exception as e:
        diag.processing_errors.append(type(e).__name__)
    scored: List[MemoryCandidate] = []
    recall = _explicit_recall(req.user_text)
    participant_keys = set(req.participants)
    for c in cands:
        overlap = len(request_terms & (_terms(c.text) | _terms(c.predicate_key.replace("_", " "))))
        score = (2.0 if recall else 0.0) + 3.0 + min(2.7, 0.45 * overlap) + (1.0 if c.predicate_key in {"open_loop", "commitment", "goal", "unresolved_question"} else 0.0) + c.authority * 0.4 + CONF.get(c.confidence, 0) * 0.3 + min(1.0, max(0.0, c.salience)) + _freshness_score(c.observed_at, now_ts) + (0.8 if participant_keys & set(c.participants) else 0.0) + (2.5 if c.source_class == "owner_correction" else 0.0)
        scored.append(replace(c, score=round(score, 4)))
    ranked = sorted(scored, key=lambda c: (-c.score, -c.authority, -CONF.get(c.confidence, 0), -_parse_time(c.observed_at), c.source_class, c.source_ref, c.entry_id))
    # Resolve contradictions by predicate: additive/open-loop-like predicates may coexist; scalar predicates keep highest ranked current value.
    additive = {"open_loop", "commitment", "goal", "unresolved_question"}
    resolved: List[MemoryCandidate] = []
    seen_pred: Set[str] = set()
    for c in ranked:
        if c.predicate_key not in additive and c.predicate_key in seen_pred:
            diag.contradiction_resolutions.append(_hash(c.predicate_key) + ":lower_ranked_suppressed")
            _add(diag, "contradiction_resolution")
            continue
        seen_pred.add(c.predicate_key)
        resolved.append(c)
    selected: List[MemoryCandidate] = []
    seen_text: Set[str] = set()
    used = 0
    per_source: Dict[str, int] = {}
    for c in resolved:
        norm = re.sub(r"\W+", " ", c.text.lower()).strip()
        if norm in seen_text:
            diag.duplicate_suppression += 1; _add(diag, "duplicate"); continue
        if per_source.get(c.source_class, 0) >= 4:
            _add(diag, "per_source_cap"); continue
        line = "- %s" % c.text[:240]
        if used + len(line) > max(0, int(req.budget_chars or 0)):
            diag.token_budget_exclusions += 1; _add(diag, "budget"); continue
        selected.append(c); seen_text.add(norm); used += len(line); per_source[c.source_class] = per_source.get(c.source_class, 0) + 1
        diag.selected_by_source[c.source_class] = diag.selected_by_source.get(c.source_class, 0) + 1
    for c in selected:
        if c.guild_id != req.guild_id: diag.invalid_invariants.append("cross_guild_selected")
        if c.subject_key != subject: diag.invalid_invariants.append("cross_subject_selected")
        if c.lifecycle in BLOCKED_LIFECYCLES: diag.invalid_invariants.append("blocked_lifecycle_selected")
        if public_route and c.visibility not in PUBLIC_VIS: diag.invalid_invariants.append("visibility_selected")
    rendered = "" if not selected else "Durable memory (governed):\n" + "\n".join("- %s" % c.text[:240] for c in selected)
    diag.selected_count = len(selected); diag.rendered_size = len(rendered); diag.rendered_hash = _hash(rendered)
    diag.legacy_vs_governed = {"legacy_hash": _hash(legacy_context), "governed_hash": diag.rendered_hash, "same": _hash(legacy_context) == diag.rendered_hash, "legacy_size": len(legacy_context or ""), "governed_size": len(rendered)}
    return GovernanceResult(rendered, tuple(selected), tuple(exclusions), diag)

def persist_shadow_diagnostics(conn: sqlite3.Connection, req: GovernanceRequest, result: GovernanceResult, legacy_context: str) -> str:
    ensure_governance_schema(conn)
    now = _now(); run_id = "mgs_" + _hash("|".join([str(req.guild_id), subject_key_for_user(req.subject_user_id), now, result.diagnostics.rendered_hash]))
    invalid_invariant_counts: Dict[str, int] = {}
    for invariant in result.diagnostics.invalid_invariants:
        key = str(invariant or "unknown")[:120]
        invalid_invariant_counts[key] = invalid_invariant_counts.get(key, 0) + 1
    aggregate_diagnostics = {
        "selected_by_source": dict(result.diagnostics.selected_by_source),
        "invalid_invariant_counts": invalid_invariant_counts,
        "fallback_reason": str(result.diagnostics.fallback_reason or "")[:120],
        "visibility_exclusions": int(result.diagnostics.visibility_exclusions or 0),
        "token_budget_exclusions": int(result.diagnostics.token_budget_exclusions or 0),
        "duplicate_suppression": int(result.diagnostics.duplicate_suppression or 0),
        "contradiction_resolution_count": len(result.diagnostics.contradiction_resolutions),
        "moment_candidate_count": int(result.diagnostics.moment_candidate_count or 0),
        "moment_needs_review_excluded": int(result.diagnostics.moment_needs_review_excluded or 0),
        "empty_result": 0 if result.rendered_context else 1,
    }
    conn.execute(
        """
        INSERT OR REPLACE INTO memory_governance_shadow_runs (
            run_id, guild_id, subject_hash, route_mode, channel_policy, created_at,
            rendered_hash, rendered_size, legacy_hash, legacy_size, selected_count,
            excluded_json, errors_json, diagnostics_json
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            run_id, req.guild_id, _hash(subject_key_for_user(req.subject_user_id)), req.route_mode,
            req.channel_policy, now, result.diagnostics.rendered_hash, result.diagnostics.rendered_size,
            _hash(legacy_context), len(legacy_context or ""), result.diagnostics.selected_count,
            json.dumps(result.diagnostics.excluded_by_reason, sort_keys=True),
            json.dumps(result.diagnostics.processing_errors, sort_keys=True),
            json.dumps(aggregate_diagnostics, sort_keys=True),
        ),
    )
    conn.execute("DELETE FROM memory_governance_shadow_runs WHERE guild_id=? AND run_id NOT IN (SELECT run_id FROM memory_governance_shadow_runs WHERE guild_id=? ORDER BY created_at DESC, run_id DESC LIMIT 500)", (req.guild_id, req.guild_id))
    conn.commit(); return run_id

def build_evaluation_report(results: List[GovernanceResult], guild_id: int) -> Dict[str, Any]:
    report: Dict[str, Any] = {"guild_id": guild_id, "runs": len(results), "selected_total": 0, "excluded_total": 0, "selected_by_source": {}, "excluded_by_reason": {}, "visibility_violations": 0, "cross_member_violations": 0, "cross_guild_violations": 0, "invalid_route_channel_policy_selections": 0, "corrected_superseded_retracted_selected": 0, "review_only_or_needs_review_selected": 0, "projection_lineage_violations": 0, "duplicate_suppression": 0, "contradiction_resolutions": 0, "budget_overruns": 0, "empty_result_frequency": 0, "processing_errors": 0, "legacy_vs_governed": [], "rollback_readiness": "ready_env_disable_live"}
    for r in results:
        report["selected_total"] += len(r.selected); report["excluded_total"] += len(r.exclusions)
        for k, v in r.diagnostics.selected_by_source.items(): report["selected_by_source"][k] = report["selected_by_source"].get(k, 0) + v
        for k, v in r.diagnostics.excluded_by_reason.items(): report["excluded_by_reason"][k] = report["excluded_by_reason"].get(k, 0) + v
        report["visibility_violations"] += sum(1 for c in r.selected if c.visibility not in INTERNAL_VIS)
        report["cross_guild_violations"] += sum(1 for c in r.selected if c.guild_id != guild_id)
        report["corrected_superseded_retracted_selected"] += sum(1 for c in r.selected if c.lifecycle in {"corrected", "superseded", "retracted", "forgotten", "deleted"})
        report["review_only_or_needs_review_selected"] += sum(1 for c in r.selected if c.lifecycle in {"review_only", "needs_review"})
        report["projection_lineage_violations"] += sum(1 for c in r.selected if (c.derived or c.projection or c.source_class in PROJECTION_CLASSES) and not c.eligible_root)
        report["duplicate_suppression"] += r.diagnostics.duplicate_suppression
        report["contradiction_resolutions"] += len(r.diagnostics.contradiction_resolutions)
        report["budget_overruns"] += r.diagnostics.token_budget_exclusions
        report["empty_result_frequency"] += 1 if not r.rendered_context else 0
        report["processing_errors"] += len(r.diagnostics.processing_errors)
        report["legacy_vs_governed"].append(r.diagnostics.legacy_vs_governed)
        for inv in r.diagnostics.invalid_invariants:
            if inv == "cross_subject_selected": report["cross_member_violations"] += 1
            if inv == "cross_guild_selected": report["cross_guild_violations"] += 1
            if inv == "visibility_selected": report["visibility_violations"] += 1
            if inv == "invalid_route_channel_policy_selected": report["invalid_route_channel_policy_selections"] += 1
    if report["processing_errors"] or report["visibility_violations"] or report["cross_member_violations"] or report["cross_guild_violations"] or report["invalid_route_channel_policy_selections"]:
        report["rollback_readiness"] = "fallback_required_before_live"
    return report

def _receipt(action: str, guild_id: int, user_id: int, target: str = "") -> str:
    return "mgr_" + hashlib.sha256(("%s|%s|%s|%s" % (action, guild_id, subject_key_for_user(user_id), target)).encode("utf-8")).hexdigest()[:32]

def _subject_hash(user_id: int) -> str:
    return _hash(subject_key_for_user(user_id))

def view_member_memory(conn: sqlite3.Connection, *, guild_id: int, user_id: int, limit: int = 10) -> List[Dict[str, Any]]:
    ensure_governance_schema(conn); subject = subject_key_for_user(user_id); out: List[Dict[str, Any]] = []
    rows = conn.execute("SELECT entry_id,predicate_key,normalized_value,source_class,lifecycle_status,derived,entry_type,updated_at FROM memory_ledger_entries WHERE guild_id=? AND subject_key=? AND lifecycle_status NOT IN ('corrected','superseded','forgotten','deleted','retracted') ORDER BY updated_at DESC LIMIT ?", (guild_id, subject, limit)).fetchall()
    for eid, pred, val, sc, life, derived, etype, updated in rows:
        out.append({"ref": "mem_" + _hash(str(eid)), "kind": _classify_kind(str(sc), str(etype), bool(derived), str(life)), "summary": str(val or "")[:220], "status": life, "correctable": life not in {"corrected", "superseded", "forgotten", "deleted", "retracted"}, "_entry_id": eid})
    return out

def _resolve_ref(conn: sqlite3.Connection, guild_id: int, user_id: int, ref: str, include_historical: bool = False) -> str:
    if include_historical:
        subject = subject_key_for_user(user_id)
        rows = conn.execute("SELECT entry_id FROM memory_ledger_entries WHERE guild_id=? AND subject_key=?", (guild_id, subject)).fetchall()
        matches = [str(r[0]) for r in rows if "mem_" + _hash(str(r[0])) == ref]
    else:
        matches = [item["_entry_id"] for item in view_member_memory(conn, guild_id=guild_id, user_id=user_id, limit=500) if item["ref"] == ref]
    return matches[0] if len(matches) == 1 else ""

def _correction_chain(conn: sqlite3.Connection, guild_id: int, start_entry_id: str) -> Set[str]:
    seen: Set[str] = set()
    stack = [start_entry_id]
    while stack:
        eid = stack.pop()
        if not eid or eid in seen:
            continue
        seen.add(eid)
        for (child,) in conn.execute("SELECT entry_id FROM memory_ledger_lineage WHERE guild_id=? AND target_entry_id=? AND lineage_type IN ('correction_of','supersedes')", (guild_id, eid)).fetchall():
            if child not in seen:
                stack.append(str(child))
        for (parent,) in conn.execute("SELECT target_entry_id FROM memory_ledger_lineage WHERE guild_id=? AND entry_id=? AND lineage_type IN ('correction_of','supersedes')", (guild_id, eid)).fetchall():
            if parent not in seen:
                stack.append(str(parent))
    return seen

def _table_has_cols(conn: sqlite3.Connection, table: str, needed: Iterable[str]) -> bool:
    return _table_exists(conn, table) and set(needed).issubset(_cols(conn, table))

def _delete_where_col(conn: sqlite3.Connection, table: str, guild_id: int, col: str, value: Any) -> int:
    if not _table_exists(conn, table):
        return 0
    cols = _cols(conn, table)
    if col not in cols:
        return 0
    if "guild_id" in cols:
        return conn.execute("DELETE FROM %s WHERE guild_id=? AND %s=?" % (table, col), (guild_id, value)).rowcount
    return conn.execute("DELETE FROM %s WHERE %s=?" % (table, col), (value,)).rowcount

def _null_identity_cols(conn: sqlite3.Connection, table: str, guild_id: int, id_col: str, name_col: str, user_id: int) -> int:
    if not _table_exists(conn, table):
        return 0
    cols = _cols(conn, table)
    if id_col not in cols:
        return 0
    assignments = [id_col + "=NULL"]
    if name_col in cols:
        assignments.append(name_col + "=NULL")
    if "guild_id" in cols:
        return conn.execute("UPDATE %s SET %s WHERE guild_id=? AND CAST(%s AS TEXT)=?" % (table, ", ".join(assignments), id_col), (guild_id, str(user_id))).rowcount
    return conn.execute("UPDATE %s SET %s WHERE CAST(%s AS TEXT)=?" % (table, ", ".join(assignments), id_col), (str(user_id),)).rowcount

def _safe_text_values(conn: sqlite3.Connection, guild_id: int, user_id: int) -> Set[str]:
    values = {str(user_id), subject_key_for_user(user_id)}
    if _table_exists(conn, "user_profiles"):
        cols = _cols(conn, "user_profiles")
        select_cols = [c for c in ("display_name", "preferred_name") if c in cols]
        if select_cols:
            row = conn.execute("SELECT %s FROM user_profiles WHERE guild_id=? AND user_id=?" % ",".join(select_cols), (guild_id, user_id)).fetchone()
            if row:
                values.update(str(v) for v in row if v)
    return {v for v in values if v}

def _identity_subject_keys(conn: sqlite3.Connection, guild_id: int, user_id: int) -> Set[str]:
    """Return exact production subject keys for the member; no fuzzy alias matching."""
    keys = {subject_key_for_user(user_id)}
    if _table_exists(conn, "user_profiles"):
        cols = _cols(conn, "user_profiles")
        select_cols = [c for c in ("display_name", "preferred_name") if c in cols]
        if select_cols:
            row = conn.execute("SELECT %s FROM user_profiles WHERE guild_id=? AND user_id=?" % ",".join(select_cols), (guild_id, user_id)).fetchone()
            if row:
                for value in row:
                    if value:
                        key = normalized_subject_key(str(value))
                        if key and key != "unknown-subject":
                            keys.add(key)
    return {k for k in keys if k}

def _scrub_text(text: str, values: Set[str]) -> str:
    cleaned = text or ""
    for v in sorted(values, key=len, reverse=True):
        if v:
            cleaned = cleaned.replace(v, "")
    return cleaned

def _mark_dependents_needs_review(conn: sqlite3.Connection, guild_id: int, entry_id: str) -> None:
    now = _now()
    if _table_exists(conn, "memory_moment_members") and _table_exists(conn, "memory_moment_windows"):
        conn.execute("UPDATE memory_moment_windows SET lifecycle_status='needs_review', updated_at=? WHERE guild_id=? AND moment_id IN (SELECT moment_id FROM memory_moment_members WHERE ledger_entry_id=?)", (now, guild_id, entry_id))
    if _table_exists(conn, "memory_ledger_lineage"):
        derived = [r[0] for r in conn.execute("SELECT entry_id FROM memory_ledger_lineage WHERE guild_id=? AND target_entry_id=? AND lineage_type='derived_from'", (guild_id, entry_id)).fetchall()]
        for did in derived:
            conn.execute("UPDATE memory_ledger_entries SET lifecycle_status='needs_review', updated_at=? WHERE guild_id=? AND entry_id=? AND (derived=1 OR projection=1)", (now, guild_id, did))

def correct_member_memory(conn: sqlite3.Connection, *, guild_id: int, user_id: int, safe_ref: str, corrected_text: str) -> Dict[str, Any]:
    ensure_governance_schema(conn); target = _resolve_ref(conn, guild_id, user_id, safe_ref)
    if not target: return {"ok": False, "reason": "ambiguous_or_unauthorized_target"}
    if not (corrected_text or "").strip(): return {"ok": False, "reason": "empty_correction"}
    subject = subject_key_for_user(user_id); rid = _receipt("correct", guild_id, user_id, safe_ref + _hash(corrected_text)); now = _now()
    with conn:
        prior = conn.execute("SELECT predicate_key,lifecycle_status FROM memory_ledger_entries WHERE guild_id=? AND subject_key=? AND entry_id=?", (guild_id, subject, target)).fetchone()
        if not prior or prior[1] in {"corrected", "superseded", "forgotten", "deleted", "retracted"} or not _entry_current(conn, guild_id, target): return {"ok": False, "reason": "obsolete_or_missing"}
        if conn.execute("SELECT 1 FROM memory_governance_receipts WHERE receipt_id=?", (rid,)).fetchone(): return {"ok": True, "receipt": rid, "idempotent": True}
        entry_id = "mle_" + hashlib.sha256(("correction|%s|%s|%s|%s" % (guild_id, subject, target, _hash(corrected_text))).encode("utf-8")).hexdigest()[:40]
        conn.execute("INSERT OR IGNORE INTO memory_ledger_entries (entry_id,schema_version,guild_id,subject_key,subject_display_name,entry_type,predicate_key,normalized_value,source_class,source_table,source_row_id,source_role,visibility,confidence,public_usable,derived,projection,salience,observed_at,lifecycle_status,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (entry_id, "memory_ledger_v1", guild_id, subject, "", "claim", prior[0], corrected_text[:1000], "owner_correction", "member_memory_control", rid, "member_control", "private", "high", 0, 0, 0, 1.0, now, "active", now, now))
        conn.execute("INSERT OR IGNORE INTO memory_ledger_lineage VALUES (?,?,?,?,?)", (entry_id, guild_id, "correction_of", target, now)); conn.execute("INSERT OR IGNORE INTO memory_ledger_lineage VALUES (?,?,?,?,?)", (entry_id, guild_id, "supersedes", target, now))
        conn.execute("UPDATE memory_ledger_entries SET lifecycle_status='corrected', updated_at=? WHERE guild_id=? AND entry_id=?", (now, guild_id, target)); propagate_ledger_lifecycle(conn, guild_id=guild_id, ledger_entry_id=target, lifecycle="corrected"); _mark_dependents_needs_review(conn, guild_id, target)
        conn.execute("INSERT INTO memory_governance_receipts VALUES (?,?,?,?,?,?,?)", (rid, guild_id, _subject_hash(user_id), "correct", safe_ref, now, json.dumps({"ledger_entries": 1})))
    return {"ok": True, "receipt": rid, "ref": safe_ref}

def forget_member_memory(conn: sqlite3.Connection, *, guild_id: int, user_id: int, safe_ref: str) -> Dict[str, Any]:
    ensure_governance_schema(conn); target = _resolve_ref(conn, guild_id, user_id, safe_ref, include_historical=True); rid = _receipt("forget", guild_id, user_id, safe_ref); now = _now(); subject = subject_key_for_user(user_id)
    if not target:
        if conn.execute("SELECT 1 FROM memory_governance_receipts WHERE receipt_id=?", (rid,)).fetchone(): return {"ok": True, "receipt": rid, "idempotent": True}
        return {"ok": False, "reason": "ambiguous_or_unauthorized_target"}
    with conn:
        if conn.execute("SELECT 1 FROM memory_governance_receipts WHERE receipt_id=?", (rid,)).fetchone(): return {"ok": True, "receipt": rid, "idempotent": True}
        chain = _correction_chain(conn, guild_id, target)
        owned = [r[0] for r in conn.execute("SELECT entry_id FROM memory_ledger_entries WHERE guild_id=? AND subject_key=?", (guild_id, subject)).fetchall()]
        chain = {eid for eid in chain if eid in set(owned)}
        if not chain: return {"ok": False, "reason": "unauthorized"}
        source_rows = conn.execute("SELECT source_table,source_row_id FROM memory_ledger_entries WHERE guild_id=? AND entry_id IN (%s)" % ",".join("?" for _ in chain), tuple([guild_id] + sorted(chain))).fetchall()
        tomb_id = "mle_" + hashlib.sha256(("forget|%s|%s|%s" % (guild_id, subject, ":".join(sorted(chain)))).encode("utf-8")).hexdigest()[:40]
        conn.execute("INSERT OR IGNORE INTO memory_ledger_entries (entry_id,schema_version,guild_id,subject_key,subject_display_name,entry_type,predicate_key,normalized_value,source_class,source_table,source_row_id,source_revision,source_role,visibility,confidence,public_usable,derived,projection,salience,observed_at,lifecycle_status,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (tomb_id, "memory_ledger_v1", guild_id, subject, "", "claim", "retraction", "", "owner_correction", "member_memory_control", rid, "", "member_control", "private", "high", 0, 0, 0, 1.0, now, "active", now, now))
        for eid in chain:
            conn.execute("INSERT OR IGNORE INTO memory_ledger_lineage VALUES (?,?,?,?,?)", (tomb_id, guild_id, "retracts", eid, now))
            conn.execute("UPDATE memory_ledger_entries SET lifecycle_status='forgotten', normalized_value='', updated_at=? WHERE guild_id=? AND entry_id=?", (now, guild_id, eid))
            propagate_ledger_lifecycle(conn, guild_id=guild_id, ledger_entry_id=eid, lifecycle="forgotten")
            _mark_dependents_needs_review(conn, guild_id, eid)
        for table, row_id in source_rows:
            conn.execute("UPDATE memory_ledger_entries SET lifecycle_status='forgotten', normalized_value='', updated_at=? WHERE guild_id=? AND subject_key=? AND source_table=? AND source_row_id=?", (now, guild_id, subject, table, row_id))
        conn.execute("INSERT INTO memory_governance_receipts VALUES (?,?,?,?,?,?,?)", (rid, guild_id, _subject_hash(user_id), "forget", safe_ref, now, json.dumps({"ledger_entries": len(chain), "tombstones": 1})))
    return {"ok": True, "receipt": rid}

def _delete_by_user(conn: sqlite3.Connection, table: str, guild_id: int, user_id: int) -> int:
    if not _table_exists(conn, table): return 0
    cols = _cols(conn, table)
    if "guild_id" not in cols: return 0
    for col in ("user_id", "member_id", "discord_user_id"):
        if col in cols:
            return conn.execute("DELETE FROM %s WHERE guild_id=? AND %s=?" % (table, col), (guild_id, user_id)).rowcount
    return 0

def _moment_ids_for_guild(conn: sqlite3.Connection, guild_id: int, where_sql: str, params: Tuple[Any, ...]) -> Set[str]:
    if not _table_exists(conn, "memory_moment_windows"):
        return set()
    return {str(r[0]) for r in conn.execute("SELECT moment_id FROM memory_moment_windows WHERE guild_id=? AND " + where_sql, (guild_id,) + params).fetchall()}

def _cleanup_canonical_moment(conn: sqlite3.Connection, guild_id: int, moment_id: str, subject: str, scrub_values: Set[str], now: str, counts: Dict[str, int]) -> None:
    if not _table_exists(conn, "memory_moment_windows"):
        return
    cols = _cols(conn, "memory_moment_windows")
    if "canonical_ledger_entry_id" not in cols:
        return
    row = conn.execute("SELECT canonical_ledger_entry_id FROM memory_moment_windows WHERE guild_id=? AND moment_id=?", (guild_id, moment_id)).fetchone()
    if not row or not row[0]:
        return
    canonical = str(row[0])
    if _table_exists(conn, "memory_ledger_participants"):
        counts["canonical_ledger_participants"] = counts.get("canonical_ledger_participants", 0) + conn.execute("DELETE FROM memory_ledger_participants WHERE guild_id=? AND entry_id=? AND participant_key=?", (guild_id, canonical, subject)).rowcount
    remaining = 0
    if _table_exists(conn, "memory_moment_participants"):
        remaining = conn.execute("SELECT COUNT(*) FROM memory_moment_participants p JOIN memory_moment_windows w ON w.moment_id=p.moment_id WHERE w.guild_id=? AND p.moment_id=?", (guild_id, moment_id)).fetchone()[0]
    new_life = "needs_review" if remaining else "retracted"
    if _table_exists(conn, "memory_ledger_entries"):
        payload = conn.execute("SELECT normalized_value FROM memory_ledger_entries WHERE guild_id=? AND entry_id=?", (guild_id, canonical)).fetchone()
        scrubbed = ""
        counts["canonical_ledger_entries"] = counts.get("canonical_ledger_entries", 0) + conn.execute("UPDATE memory_ledger_entries SET normalized_value=?, lifecycle_status=?, updated_at=? WHERE guild_id=? AND entry_id=?", (scrubbed, new_life, now, guild_id, canonical)).rowcount
        # Remove lineage edges from deleted sources; keep canonical row reachable but not dangling to deleted member entries.
        counts["canonical_ledger_lineage"] = counts.get("canonical_ledger_lineage", 0) + conn.execute("DELETE FROM memory_ledger_lineage WHERE guild_id=? AND entry_id=? AND target_entry_id NOT IN (SELECT entry_id FROM memory_ledger_entries WHERE guild_id=?)", (guild_id, canonical, guild_id)).rowcount

def complete_delete_member_data(conn: sqlite3.Connection, *, guild_id: int, user_id: int, confirmation: str, inject_failure: bool = False) -> Dict[str, Any]:
    if confirmation != "DELETE MY BNL DATA %s" % guild_id: return {"ok": False, "reason": "confirmation_required"}
    ensure_governance_schema(conn); rid = _receipt("complete_delete", guild_id, user_id); now = _now(); counts: Dict[str, int] = {}; subject = subject_key_for_user(user_id); subject_hash = _subject_hash(user_id)
    scrub_values = _safe_text_values(conn, guild_id, user_id)
    identity_keys = _identity_subject_keys(conn, guild_id, user_id)
    with conn:
        # Core member-owned tables.
        for table in ("conversations", "user_profiles", "user_memory_facts", "user_habits", "relationship_state", "relationship_journal", "memory_tiers", "response_style_log"):
            counts[table] = _delete_by_user(conn, table, guild_id, user_id)
        ensure_relationship_v2_schema(conn)
        counts.update(complete_delete_relationship_v2(conn, guild_id=guild_id, user_id=user_id))
        # Community/member activity schemas use varied identity columns.
        if _table_exists(conn, "community_presence"):
            for col in ("user_id", "member_user_id", "subject_user_id", "discord_user_id"):
                counts["community_presence"] = counts.get("community_presence", 0) + _delete_where_col(conn, "community_presence", guild_id, col, user_id)
            cols = _cols(conn, "community_presence")
            for col in ("subject_key", "member_key"):
                if col in cols:
                    for key in identity_keys:
                        counts["community_presence"] = counts.get("community_presence", 0) + _delete_where_col(conn, "community_presence", guild_id, col, key)
        if _table_exists(conn, "member_activity_events"):
            cols = _cols(conn, "member_activity_events")
            if "member_user_id" in cols:
                counts["member_activity_events"] = conn.execute("DELETE FROM member_activity_events WHERE guild_id=? AND CAST(member_user_id AS TEXT)=?", (guild_id, str(user_id))).rowcount
            else:
                counts["member_activity_events"] = _delete_by_user(conn, "member_activity_events", guild_id, user_id)
        # Entity evidence/intelligence actual tables.
        if _table_exists(conn, "entity_evidence_events"):
            cols = _cols(conn, "entity_evidence_events")
            total = 0
            if "matched_user_id" in cols:
                total += conn.execute("DELETE FROM entity_evidence_events WHERE guild_id=? AND CAST(matched_user_id AS TEXT)=?", (guild_id, str(user_id))).rowcount
            for col in ("subject_key", "subject_id", "entity_key", "matched_subject_key"):
                if col in cols:
                    for key in identity_keys:
                        total += conn.execute("DELETE FROM entity_evidence_events WHERE guild_id=? AND %s=?" % col, (guild_id, key)).rowcount
            counts["entity_evidence_events"] = total
        for table in ("entity_intelligence_facts", "entity_activity_rollups", "entity_open_questions", "entity_profile_snapshots", "entity_scouting_queue"):
            if _table_exists(conn, table):
                total = 0; cols = _cols(conn, table)
                for col in ("subject_key",):
                    if col in cols:
                        for key in identity_keys:
                            total += conn.execute("DELETE FROM %s WHERE guild_id=? AND %s=?" % (table, col), (guild_id, key)).rowcount
                for col in ("user_id", "member_user_id", "matched_user_id"):
                    if col in cols:
                        total += conn.execute("DELETE FROM %s WHERE guild_id=? AND CAST(%s AS TEXT)=?" % (table, col), (guild_id, str(user_id))).rowcount
                counts[table] = total
        if _table_exists(conn, "entity_intelligence_edges"):
            total = 0; cols = _cols(conn, "entity_intelligence_edges")
            for col in ("subject_key", "object_key"):
                if col in cols:
                    for key in identity_keys:
                        total += conn.execute("DELETE FROM entity_intelligence_edges WHERE guild_id=? AND %s=?" % col, (guild_id, key)).rowcount
            counts["entity_intelligence_edges"] = total
        # Preserve broadcast/show content but anonymize matching submitter/corrector identities.
        counts["broadcast_memory_submitter_anonymized"] = _null_identity_cols(conn, "broadcast_memory", guild_id, "submitted_by_user_id", "submitted_by_name", user_id)
        counts["broadcast_memory_corrector_anonymized"] = _null_identity_cols(conn, "broadcast_memory", guild_id, "corrected_by_user_id", "corrected_by_name", user_id)
        affected_moments: Set[str] = set()
        # Guild-scoped moment participant lookup and deletion.
        if _table_exists(conn, "memory_moment_participants") and _table_exists(conn, "memory_moment_windows"):
            affected_moments.update(str(r[0]) for r in conn.execute("SELECT p.moment_id FROM memory_moment_participants p JOIN memory_moment_windows w ON w.moment_id=p.moment_id WHERE w.guild_id=? AND p.participant_key=?", (guild_id, subject)).fetchall())
            counts["memory_moment_participants"] = conn.execute("DELETE FROM memory_moment_participants WHERE participant_key=? AND moment_id IN (SELECT moment_id FROM memory_moment_windows WHERE guild_id=?)", (subject, guild_id)).rowcount
        ids: List[str] = []
        if _table_exists(conn, "memory_ledger_entries"):
            ids = [str(r[0]) for r in conn.execute("SELECT entry_id FROM memory_ledger_entries WHERE guild_id=? AND subject_key=?", (guild_id, subject)).fetchall()]
            if _table_exists(conn, "memory_moment_members") and _table_exists(conn, "memory_moment_windows") and ids:
                q = ",".join("?" for _ in ids)
                affected_moments.update(str(r[0]) for r in conn.execute("SELECT m.moment_id FROM memory_moment_members m JOIN memory_moment_windows w ON w.moment_id=m.moment_id WHERE w.guild_id=? AND m.ledger_entry_id IN (%s)" % q, tuple([guild_id] + ids)).fetchall())
                counts["memory_moment_members"] = conn.execute("DELETE FROM memory_moment_members WHERE ledger_entry_id IN (%s) AND moment_id IN (SELECT moment_id FROM memory_moment_windows WHERE guild_id=?)" % q, tuple(ids + [guild_id])).rowcount
            # Clean canonical moment copies before deleting member entries.
            for mid in sorted(affected_moments):
                _cleanup_canonical_moment(conn, guild_id, mid, subject, scrub_values, now, counts)
            for eid in ids:
                counts["memory_ledger_lineage"] = counts.get("memory_ledger_lineage", 0) + conn.execute("DELETE FROM memory_ledger_lineage WHERE guild_id=? AND (entry_id=? OR target_entry_id=?)", (guild_id, eid, eid)).rowcount
            counts["memory_ledger_participants"] = counts.get("memory_ledger_participants", 0) + conn.execute("DELETE FROM memory_ledger_participants WHERE guild_id=? AND participant_key=?", (guild_id, subject)).rowcount
            counts["memory_ledger_entries"] = conn.execute("DELETE FROM memory_ledger_entries WHERE guild_id=? AND subject_key=?", (guild_id, subject)).rowcount
            # Remove any now-dangling lineage in this guild.
            counts["memory_ledger_lineage_dangling"] = conn.execute("DELETE FROM memory_ledger_lineage WHERE guild_id=? AND (entry_id NOT IN (SELECT entry_id FROM memory_ledger_entries WHERE guild_id=?) OR target_entry_id NOT IN (SELECT entry_id FROM memory_ledger_entries WHERE guild_id=?))", (guild_id, guild_id, guild_id)).rowcount
        if _table_exists(conn, "memory_moment_windows"):
            cols = _cols(conn, "memory_moment_windows")
            for mid in sorted(affected_moments):
                remaining = conn.execute("SELECT COUNT(*) FROM memory_moment_participants p JOIN memory_moment_windows w ON w.moment_id=p.moment_id WHERE w.guild_id=? AND p.moment_id=?", (guild_id, mid)).fetchone()[0] if _table_exists(conn, "memory_moment_participants") else 0
                status = "needs_review" if remaining else "retracted"
                assignments = ["lifecycle_status=?", "updated_at=?"]
                params: List[Any] = [status, now]
                if "summary" in cols:
                    assignments.append("summary=?"); params.append("")
                params.extend([guild_id, mid])
                counts["memory_moment_windows_%s" % status] = counts.get("memory_moment_windows_%s" % status, 0) + conn.execute("UPDATE memory_moment_windows SET %s WHERE guild_id=? AND moment_id=?" % ", ".join(assignments), tuple(params)).rowcount
        # Delete shadow runs and prior receipts for this member only in this guild.
        counts["memory_governance_shadow_runs"] = conn.execute("DELETE FROM memory_governance_shadow_runs WHERE guild_id=? AND subject_hash=?", (guild_id, subject_hash)).rowcount if _table_exists(conn, "memory_governance_shadow_runs") else 0
        counts["prior_governance_receipts"] = conn.execute("DELETE FROM memory_governance_receipts WHERE guild_id=? AND subject_hash=?", (guild_id, subject_hash)).rowcount
        # The Journal source archive is normally immutable. Complete deletion is
        # the governed, transaction-scoped exception for this member's raw Discord
        # inputs; website relays and other members/guilds remain untouched.
        counts["bnl_journal_source_events"] = purge_user_discord_sources_on_connection(
            conn, guild_id=guild_id, user_id=user_id
        )
        counts.update(purge_user_journal_derivatives_on_connection(
            conn, guild_id=guild_id, user_id=user_id
        ))
        if inject_failure: raise RuntimeError("injected_complete_delete_failure")
        safe_counts = {k: int(v or 0) for k, v in counts.items() if int(v or 0)}
        conn.execute("INSERT INTO memory_governance_receipts VALUES (?,?,?,?,?,?,?)", (rid, guild_id, subject_hash, "complete_delete", "", now, json.dumps(safe_counts, sort_keys=True)))
    return {"ok": True, "receipt": rid, "row_counts": counts, "idempotent": not any(counts.values()), "limitation": "Discord-hosted messages are not deleted by this bot command."}
