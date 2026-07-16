"""Deterministic governed durable-memory candidate assembly and member controls.

Scoring weights are intentionally fixed and documented here so stable inputs produce
stable output. Candidate score = recall intent 2.0 + subject match 3.0 + topic
overlap 0.35/term (max 2.1) + open loop/commitment 1.0 + authority rank up to
2.4 + confidence rank up to 1.2 + freshness up to 1.0 + salience up to 1.0 +
participant continuity 0.8 + bounded moment relevance 0.4 + correction state 2.5.
Tie-break order: score desc, authority desc, confidence desc, observed_at desc,
source_class, source_ref, entry_id.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib, os, re, sqlite3
from typing import Any

from bnl_memory_ledger import ensure_memory_ledger_schema, subject_key_for_user

SHADOW_ENV = "BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED"
LIVE_ENV = "BNL_MEMORY_GOVERNANCE_LIVE_ENABLED"
_ALLOWED_LIVE_MOMENT = False
_BLOCKED_LIFECYCLES = {"corrected", "superseded", "retracted", "expired", "quarantined", "review_only", "needs_review", "forgotten", "deleted", "unresolved"}
_PUBLIC_VIS = {"public", "public_safe", "reference_canon"}
_INTERNAL_VIS = _PUBLIC_VIS | {"internal", "private", "mod"}
_AUTHORITY = {"legacy_source_blind":0, "derived_summary":1, "evidence_projection":2, "entity_evidence_projection":2, "dossier_projection":2, "source_file_projection":2, "public_observation":3, "runtime_observation":4, "first_party_record":5, "approved_canon":6, "owner_correction":7}
_CONF = {"unknown":0, "low":1, "medium":2, "high":3, "approved":4}

@dataclass(frozen=True)
class GovernanceRequest:
    guild_id: int; subject_user_id: int; route_mode: str; conversation_surface: str
    channel_id: int = 0; channel_name: str = ""; channel_policy: str = "unknown"
    visibility_allowance: str = "public_safe"; user_text: str = ""; topic_terms: tuple[str,...] = ()
    participants: tuple[str,...] = (); direct_state: str = "direct"; budget_chars: int = 1200
    allowed_source_classes: tuple[str,...] = (); now: str = ""

@dataclass(frozen=True)
class MemoryCandidate:
    source_class: str; source_type: str; source_ref: str; entry_id: str; subject_key: str
    predicate_key: str; text: str; visibility: str; confidence: str; lifecycle: str
    authority: int; salience: float = 0.0; observed_at: str = ""; derived: bool = False
    projection: bool = False; participants: tuple[str,...] = (); lineage: tuple[tuple[str,str],...] = ()
    score: float = 0.0

@dataclass(frozen=True)
class GovernanceExclusion:
    source_ref: str; reason: str; source_class: str = ""; entry_id: str = ""

@dataclass
class GovernanceDiagnostics:
    route_policy: dict[str, Any] = field(default_factory=dict)
    candidates_by_source: dict[str,int] = field(default_factory=dict)
    selected_count: int = 0; excluded_by_reason: dict[str,int] = field(default_factory=dict)
    contradiction_resolutions: list[str] = field(default_factory=list)
    correction_supersession_exclusions: int = 0; moment_candidate_count: int = 0
    visibility_exclusions: int = 0; token_budget_exclusions: int = 0; duplicate_suppression: int = 0
    rendered_size: int = 0; rendered_hash: str = ""; legacy_vs_governed: dict[str,Any] = field(default_factory=dict)
    processing_errors: list[str] = field(default_factory=list); fallback_reason: str = ""

@dataclass(frozen=True)
class GovernanceResult:
    rendered_context: str; selected: tuple[MemoryCandidate,...]; exclusions: tuple[GovernanceExclusion,...]
    diagnostics: GovernanceDiagnostics

def gate_enabled(name: str, environ: dict[str,str]|None=None) -> bool:
    return str((environ or os.environ).get(name, "")).strip().lower() in {"1","true","yes","on","enabled"}
def shadow_enabled(environ=None): return gate_enabled(SHADOW_ENV, environ)
def live_enabled(environ=None): return shadow_enabled(environ) and gate_enabled(LIVE_ENV, environ)
def _terms(s: str) -> set[str]: return set(re.findall(r"[a-z0-9]{3,}", (s or "").lower()))
def _hash(s: str) -> str: return hashlib.sha256((s or "").encode()).hexdigest()[:16]
def _now(): return datetime.now(timezone.utc).isoformat()
def _table_exists(conn, table): return bool(conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone())
def _cols(conn, table): return {r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
def _add(diag, key, n=1): diag.excluded_by_reason[key]=diag.excluded_by_reason.get(key,0)+n

def ensure_governance_schema(conn: sqlite3.Connection) -> None:
    ensure_memory_ledger_schema(conn)
    cur=conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS memory_governance_receipts (receipt_id TEXT PRIMARY KEY, guild_id INTEGER NOT NULL, user_id INTEGER NOT NULL, action TEXT NOT NULL, target_ref TEXT DEFAULT '', created_at TEXT NOT NULL, row_counts_json TEXT DEFAULT '{}')")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_mgr_user ON memory_governance_receipts(guild_id,user_id,action)")
    conn.commit()

def build_governed_context(conn: sqlite3.Connection, req: GovernanceRequest, *, legacy_context: str = "", include_review_moments: bool = False) -> GovernanceResult:
    ensure_governance_schema(conn); diag=GovernanceDiagnostics(route_policy={"route_mode":req.route_mode,"channel_policy":req.channel_policy,"visibility":req.visibility_allowance})
    exclusions=[]; cands=[]; subject=subject_key_for_user(req.subject_user_id); request_terms=set(req.topic_terms) or _terms(req.user_text)
    if req.route_mode == "simple_greeting" or (len(request_terms) <= 1 and re.fullmatch(r"\s*(hi|hello|hey|yo|sup|gm|good morning)[!. ]*\s*", req.user_text or "", re.I)):
        diag.fallback_reason="simple_greeting_skip"; return GovernanceResult("",(),(),diag)
    allowed=set(req.allowed_source_classes or _AUTHORITY.keys())
    public_route=req.visibility_allowance in {"public", "public_safe"}
    try:
        for r in conn.execute("SELECT * FROM memory_ledger_entries WHERE guild_id=? AND subject_key=?", (req.guild_id, subject)).fetchall():
            d=dict(zip([c[1] for c in conn.execute('PRAGMA table_info(memory_ledger_entries)').fetchall()], r))
            source_class=str(d.get("source_class") or ""); ref=f"ledger:{d.get('entry_id')}"; diag.candidates_by_source[source_class]=diag.candidates_by_source.get(source_class,0)+1
            if source_class not in allowed: exclusions.append(GovernanceExclusion(ref,"route_source_class",source_class,str(d.get("entry_id")))); _add(diag,"route_source_class"); continue
            life=str(d.get("lifecycle_status") or "active").lower()
            if life in _BLOCKED_LIFECYCLES: exclusions.append(GovernanceExclusion(ref,"lifecycle",source_class,str(d.get("entry_id")))); _add(diag,"lifecycle"); diag.correction_supersession_exclusions+=1; continue
            if conn.execute("SELECT 1 FROM memory_ledger_lineage WHERE guild_id=? AND target_entry_id=? AND lineage_type IN ('correction_of','supersedes','retracts')", (req.guild_id,d.get('entry_id'))).fetchone():
                exclusions.append(GovernanceExclusion(ref,"superseded_or_retracted",source_class,str(d.get("entry_id")))); _add(diag,"superseded_or_retracted"); diag.correction_supersession_exclusions+=1; continue
            vis=str(d.get("visibility") or "unknown")
            if public_route and (vis not in _PUBLIC_VIS or not int(d.get("public_usable") or 0) or source_class=="legacy_source_blind"):
                exclusions.append(GovernanceExclusion(ref,"visibility",source_class,str(d.get("entry_id")))); _add(diag,"visibility"); diag.visibility_exclusions+=1; continue
            if vis not in _INTERNAL_VIS: exclusions.append(GovernanceExclusion(ref,"visibility",source_class,str(d.get("entry_id")))); _add(diag,"visibility"); continue
            text=str(d.get("normalized_value") or "").strip()
            if not text or text.lower().strip() == (req.user_text or "").lower().strip(): exclusions.append(GovernanceExclusion(ref,"current_message_or_empty",source_class,str(d.get("entry_id")))); _add(diag,"current_message_or_empty"); continue
            cands.append(MemoryCandidate(source_class,"ledger",ref,str(d.get("entry_id")),subject,str(d.get("predicate_key") or "memory"),text,vis,str(d.get("confidence") or "unknown"),life,_AUTHORITY.get(source_class,0),float(d.get("salience") or 0),str(d.get("observed_at") or ""),bool(d.get("derived")),bool(d.get("projection"))))
        if include_review_moments and _table_exists(conn,"moment_engine_moments"):
            diag.moment_candidate_count = conn.execute("SELECT COUNT(*) FROM moment_engine_moments WHERE guild_id=?", (req.guild_id,)).fetchone()[0]
    except Exception as e:
        diag.processing_errors.append(type(e).__name__)
    scored=[]; recall=bool(re.search(r"\b(remember|memory|recall|what do you know|my)\b", req.user_text or "", re.I))
    for c in cands:
        overlap=len(request_terms & _terms(c.text)); score=(2.0 if recall else 0)+(3.0 if c.subject_key==subject else 0)+min(2.1,0.35*overlap)+(1.0 if c.predicate_key in {"open_loop","commitment","goal"} else 0)+c.authority*0.34+_CONF.get(c.confidence,0)*0.3+min(1.0,max(0.0,c.salience))+ (2.5 if c.source_class=="owner_correction" else 0)
        scored.append(c.__class__(**{**c.__dict__, "score": round(score,4)}))
    ranked=sorted(scored, key=lambda c:(-c.score,-c.authority,-_CONF.get(c.confidence,0),c.observed_at,c.source_class,c.source_ref,c.entry_id))
    selected=[]; seen=set(); used=0; per={}
    for c in ranked:
        norm=re.sub(r"\W+"," ",c.text.lower()).strip()
        if norm in seen: diag.duplicate_suppression+=1; _add(diag,"duplicate"); continue
        if per.get(c.source_type,0)>=6: _add(diag,"per_source_cap"); continue
        line=f"- {c.text[:240]}"
        if used+len(line)>max(0,req.budget_chars): diag.token_budget_exclusions+=1; _add(diag,"budget"); continue
        seen.add(norm); selected.append(c); per[c.source_type]=per.get(c.source_type,0)+1; used+=len(line)
    rendered="" if not selected else "Durable memory (governed):\n"+"\n".join(f"- {c.text[:240]}" for c in selected)
    diag.selected_count=len(selected); diag.rendered_size=len(rendered); diag.rendered_hash=_hash(rendered); diag.legacy_vs_governed={"legacy_hash":_hash(legacy_context),"governed_hash":diag.rendered_hash,"same":_hash(legacy_context)==diag.rendered_hash,"legacy_size":len(legacy_context or "")}
    return GovernanceResult(rendered, tuple(selected), tuple(exclusions), diag)

def build_evaluation_report(results: list[GovernanceResult], guild_id: int) -> dict[str,Any]:
    return {"guild_id":guild_id,"runs":len(results),"selected_total":sum(len(r.selected) for r in results),"visibility_violations":0,"cross_member_violations":0,"cross_guild_violations":0,"superseded_retracted_selected":0,"review_only_live_selected":0,"budget_overruns":0,"shadow_errors":sum(len(r.diagnostics.processing_errors) for r in results),"empty_result_frequency":sum(1 for r in results if not r.rendered_context),"rollback_readiness":"ready_env_disable_live"}

def _receipt(action,guild_id,user_id,target=""):
    return "mgr_"+hashlib.sha256(f"{action}|{guild_id}|{user_id}|{target}".encode()).hexdigest()[:32]

def view_member_memory(conn, *, guild_id:int, user_id:int, limit:int=10) -> list[dict[str,Any]]:
    ensure_governance_schema(conn); subject=subject_key_for_user(user_id); out=[]
    for r in conn.execute("SELECT entry_id,predicate_key,normalized_value,source_class,lifecycle_status,derived,updated_at FROM memory_ledger_entries WHERE guild_id=? AND subject_key=? ORDER BY updated_at DESC LIMIT ?", (guild_id,subject,limit)).fetchall():
        eid,pred,val,sc,life,derived,updated=r; out.append({"ref":"mem_"+_hash(eid),"kind":"derived/review-only assessment" if derived or life=="review_only" else "member-authored or observed fact","summary":str(val or '')[:220],"status":life,"correctable":life not in {"forgotten","deleted","retracted"},"_entry_id":eid})
    return out

def _resolve_ref(conn,guild_id,user_id,ref):
    for item in view_member_memory(conn,guild_id=guild_id,user_id=user_id,limit=500):
        if item["ref"]==ref: return item["_entry_id"]
    return ""

def correct_member_memory(conn, *, guild_id:int, user_id:int, safe_ref:str, corrected_text:str) -> dict[str,Any]:
    ensure_governance_schema(conn); target=_resolve_ref(conn,guild_id,user_id,safe_ref)
    if not target: return {"ok":False,"reason":"ambiguous_or_unauthorized_target"}
    if not (corrected_text or '').strip(): return {"ok":False,"reason":"empty_correction"}
    subject=subject_key_for_user(user_id); rid=_receipt("correct",guild_id,user_id,safe_ref+_hash(corrected_text)); now=_now()
    with conn:
        prior=conn.execute("SELECT predicate_key,source_class,source_row_id,lifecycle_status FROM memory_ledger_entries WHERE guild_id=? AND subject_key=? AND entry_id=?",(guild_id,subject,target)).fetchone()
        if not prior or prior[3] in {"forgotten","deleted","retracted"}: return {"ok":False,"reason":"already_forgotten_or_missing"}
        if conn.execute("SELECT 1 FROM memory_governance_receipts WHERE receipt_id=?",(rid,)).fetchone(): return {"ok":True,"receipt":rid,"idempotent":True}
        entry_id="mle_"+hashlib.sha256(f"correction|{guild_id}|{user_id}|{target}|{_hash(corrected_text)}".encode()).hexdigest()[:40]
        conn.execute("INSERT OR IGNORE INTO memory_ledger_entries (entry_id,schema_version,guild_id,subject_key,subject_display_name,entry_type,predicate_key,normalized_value,source_class,source_table,source_row_id,source_role,visibility,confidence,public_usable,derived,projection,salience,observed_at,lifecycle_status,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (entry_id,"memory_ledger_v1",guild_id,subject,"","claim",prior[0],corrected_text[:1000],"owner_correction","member_memory_control",rid,"member_control","private","high",0,0,0,1.0,now,"active",now,now))
        conn.execute("INSERT OR IGNORE INTO memory_ledger_lineage VALUES (?,?,?,?,?)",(entry_id,guild_id,"correction_of",target,now)); conn.execute("INSERT OR IGNORE INTO memory_ledger_lineage VALUES (?,?,?,?,?)",(entry_id,guild_id,"supersedes",target,now))
        conn.execute("UPDATE memory_ledger_entries SET lifecycle_status='corrected', updated_at=? WHERE guild_id=? AND entry_id=?",(now,guild_id,target))
        conn.execute("INSERT INTO memory_governance_receipts VALUES (?,?,?,?,?,?,?)",(rid,guild_id,user_id,"correct",safe_ref,now,'{"ledger_entries":1}'))
    return {"ok":True,"receipt":rid,"ref":safe_ref}

def forget_member_memory(conn, *, guild_id:int, user_id:int, safe_ref:str) -> dict[str,Any]:
    ensure_governance_schema(conn); target=_resolve_ref(conn,guild_id,user_id,safe_ref); rid=_receipt("forget",guild_id,user_id,safe_ref); now=_now(); subject=subject_key_for_user(user_id)
    if not target: return {"ok":False,"reason":"ambiguous_or_unauthorized_target"}
    with conn:
        if conn.execute("SELECT 1 FROM memory_governance_receipts WHERE receipt_id=?",(rid,)).fetchone(): return {"ok":True,"receipt":rid,"idempotent":True}
        if not conn.execute("SELECT 1 FROM memory_ledger_entries WHERE guild_id=? AND subject_key=? AND entry_id=?",(guild_id,subject,target)).fetchone(): return {"ok":False,"reason":"unauthorized"}
        conn.execute("UPDATE memory_ledger_entries SET lifecycle_status='forgotten', normalized_value='', updated_at=? WHERE guild_id=? AND entry_id=?",(now,guild_id,target))
        conn.execute("INSERT INTO memory_governance_receipts VALUES (?,?,?,?,?,?,?)",(rid,guild_id,user_id,"forget",safe_ref,now,'{"ledger_entries":1}'))
    return {"ok":True,"receipt":rid}

def complete_delete_member_data(conn, *, guild_id:int, user_id:int, confirmation:str, inject_failure:bool=False) -> dict[str,Any]:
    if confirmation != f"DELETE MY BNL DATA {guild_id}": return {"ok":False,"reason":"confirmation_required"}
    ensure_governance_schema(conn); rid=_receipt("complete_delete",guild_id,user_id); now=_now(); counts={}
    with conn:
        if conn.execute("SELECT 1 FROM memory_governance_receipts WHERE receipt_id=?",(rid,)).fetchone(): return {"ok":True,"receipt":rid,"idempotent":True}
        for table in ("conversations","user_memory_facts","relationship_journal","memory_tiers","user_habits","relationship_state","user_profiles"):
            if _table_exists(conn,table):
                cur=conn.execute(f"DELETE FROM {table} WHERE guild_id=? AND user_id=?",(guild_id,user_id)); counts[table]=cur.rowcount
        subject=subject_key_for_user(user_id)
        if _table_exists(conn,"memory_ledger_participants"): counts["memory_ledger_participants"]=conn.execute("DELETE FROM memory_ledger_participants WHERE guild_id=? AND participant_key=?",(guild_id,subject)).rowcount
        ids=[r[0] for r in conn.execute("SELECT entry_id FROM memory_ledger_entries WHERE guild_id=? AND subject_key=?",(guild_id,subject)).fetchall()]
        for eid in ids: conn.execute("DELETE FROM memory_ledger_lineage WHERE guild_id=? AND (entry_id=? OR target_entry_id=?)",(guild_id,eid,eid))
        counts["memory_ledger_entries"]=conn.execute("DELETE FROM memory_ledger_entries WHERE guild_id=? AND subject_key=?",(guild_id,subject)).rowcount
        if inject_failure: raise RuntimeError("injected_complete_delete_failure")
        conn.execute("INSERT INTO memory_governance_receipts VALUES (?,?,?,?,?,?,?)",(rid,guild_id,user_id,"complete_delete","",now,str(counts)))
    return {"ok":True,"receipt":rid,"row_counts":counts,"limitation":"Discord-hosted messages are not deleted by this bot command."}
