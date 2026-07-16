"""Moment Engine v1 shadow infrastructure.

Builds bounded, auditable conversational moments from Unified Memory Ledger
conversation entries only. This module is deliberately not used by prompt assembly.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
import hashlib, json, os, re, sqlite3
from typing import Any

from bnl_canon_source_contract import Confidence, SourceClass, Visibility
from bnl_memory_ledger import (
    BNL_SUBJECT_KEY, LedgerEntry, LedgerParticipant, LedgerWriteResult,
    ensure_memory_ledger_schema, insert_ledger_entry, shadow_enabled as ledger_shadow_enabled,
)

MOMENT_ENGINE_SHADOW_ENV = "BNL_MOMENT_ENGINE_SHADOW_ENABLED"
MOMENT_SCHEMA_VERSION = "memory_moment_v1"
MAX_WINDOW_SECONDS = 5 * 60
INACTIVITY_SECONDS = 2 * 60

STOP = set("a an and are as at be but by for from how i in is it me my of on or our that the this to was we what when where who why with you your did do does about into can could would should just yep yes no ok okay hey hi hello thanks thank lol lmao".split())
STRONG_MARKERS = ("remember", "recall", "what numbers", "correction", "actually", "replace", "commit", "promise", "boundary", "milestone", "follow up", "follow-up", "celebrate")
LOW_SIGNAL = set("hi hey hello thanks thank you ok okay yep yes no lol lmao cool nice got it noted".split())
VIS_RANK = {"public": 0, "public_safe": 0, "reference_canon": 0, "internal": 2, "private": 3, "mod": 3, "sealed_test": 4, "protected": 4, "ai_image_tool": 4, "unknown": 5}

@dataclass(frozen=True)
class MomentObservationResult:
    outcome: str = "skipped"
    reason_code: str = "not_attempted"
    moment_id: str = ""
    ledger_entry_id: str = ""


def shadow_enabled(environ: dict[str, str] | None = None) -> bool:
    return str((environ or os.environ).get(MOMENT_ENGINE_SHADOW_ENV, "")).strip().lower() in {"1", "true", "yes", "on", "enabled"}


def _now() -> str: return datetime.now(timezone.utc).isoformat()
def _parse_ts(v: str) -> datetime:
    try: return datetime.fromisoformat((v or "").replace("Z", "+00:00"))
    except Exception: return datetime.now(timezone.utc)
def _canon(v: Any) -> str: return re.sub(r"\s+", " ", str(v or "").strip().lower())
def _tokens(text: str) -> tuple[str, ...]: return tuple(t for t in re.findall(r"[a-z0-9]{2,}", _canon(text)) if t not in STOP)[:12]
def _topic_key(text: str, predicate: str = "") -> str:
    nums = re.findall(r"\b\d{2,12}\b", text or "")
    toks = list(_tokens(text)) + nums
    if predicate == "remembered_number": toks.append("remembered_number")
    if not toks: return "low_signal"
    return "topic_" + hashlib.sha256(" ".join(sorted(set(toks))[:10]).encode()).hexdigest()[:16]
def stable_moment_id(guild_id:int, channel_id:int, topic_key:str, started_at:str) -> str:
    return "mom_" + hashlib.sha256(f"{MOMENT_SCHEMA_VERSION}\x1f{guild_id}\x1f{channel_id}\x1f{topic_key}\x1f{started_at}".encode()).hexdigest()[:32]
def stable_moment_entry_id(moment_id: str) -> str: return "moment_entry_" + hashlib.sha256(moment_id.encode()).hexdigest()[:32]
def _meaningful(text: str, role: str, predicate: str) -> bool:
    c = _canon(text)
    if role != "user": return False
    if predicate == "remembered_number" or any(m in c for m in STRONG_MARKERS): return True
    toks = [t for t in _tokens(c) if t not in LOW_SIGNAL]
    if not toks: return False
    if re.fullmatch(r"[!?.\s]+", c or ""): return False
    return len(toks) >= 2 or len(c) >= 24

def ensure_moment_schema(conn: sqlite3.Connection) -> None:
    ensure_memory_ledger_schema(conn); cur=conn.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS memory_moment_windows (
      moment_id TEXT PRIMARY KEY, guild_id INTEGER NOT NULL, channel_id INTEGER NOT NULL, channel_name TEXT,
      channel_policy TEXT, route_mode TEXT, topic_key TEXT NOT NULL, window_started_at TEXT NOT NULL,
      last_activity_at TEXT NOT NULL, finalized_at TEXT, qualification_type TEXT, qualification_reason TEXT,
      lifecycle_status TEXT NOT NULL, visibility TEXT, public_usable INTEGER DEFAULT 0, salience REAL DEFAULT 0,
      human_entry_count INTEGER DEFAULT 0, model_entry_count INTEGER DEFAULT 0, participant_count INTEGER DEFAULT 0,
      summary TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL)""")
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
      "CREATE INDEX IF NOT EXISTS idx_mmm_entry ON memory_moment_members(ledger_entry_id)",
      "CREATE INDEX IF NOT EXISTS idx_mmp_participant ON memory_moment_participants(participant_key, moment_id)",
      "CREATE INDEX IF NOT EXISTS idx_mmd_guild ON memory_moment_diagnostics(guild_id, event_type, reason_code)"]:
        cur.execute(sql)

def _diag(conn, guild_id, event, reason="", moment_id="", entry_id=""):
    conn.execute("INSERT INTO memory_moment_diagnostics(guild_id,moment_id,event_type,reason_code,ledger_entry_id,created_at) VALUES(?,?,?,?,?,?)", (guild_id, moment_id, event, reason, entry_id, _now()))

def _fetch_entry(conn, entry_id):
    return conn.execute("SELECT entry_id,guild_id,source_table,source_role,entry_type,predicate_key,normalized_value,route_mode,channel_id,channel_name,channel_policy,visibility,public_usable,salience,observed_at,source_sequence,lifecycle_status,subject_key,subject_display_name FROM memory_ledger_entries WHERE entry_id=?", (entry_id,)).fetchone()

def observe_ledger_entry(conn: sqlite3.Connection, ledger_entry_id: str) -> MomentObservationResult:
    ensure_moment_schema(conn)
    if not shadow_enabled(): return MomentObservationResult(reason_code="moment_gate_disabled")
    if not ledger_shadow_enabled():
        _diag(conn, 0, "moment_processing_skipped", "ledger_shadow_unavailable", entry_id=ledger_entry_id); return MomentObservationResult(reason_code="ledger_shadow_unavailable")
    try:
        row=_fetch_entry(conn, ledger_entry_id)
        if not row: return MomentObservationResult(reason_code="missing_ledger_entry", ledger_entry_id=ledger_entry_id)
        (eid,guild,table,role,etype,pred,val,route,chan,cname,policy,vis,pub,sal,obs,seq,life,subj,disp)=row
        if table!="conversations" or life not in ("active","review_only") or etype not in ("observation","derived_summary"):
            return MomentObservationResult(reason_code="ineligible_source", ledger_entry_id=eid)
        _diag(conn,guild,"eligible_ledger_entry_observed","ok",entry_id=eid)
        topic=_topic_key(val or "", pred or "")
        ts=_parse_ts(obs); open_rows=conn.execute("SELECT moment_id,topic_key,window_started_at,last_activity_at,channel_policy,visibility FROM memory_moment_windows WHERE guild_id=? AND channel_id=? AND lifecycle_status='open' ORDER BY last_activity_at DESC",(guild,chan)).fetchall()
        chosen=None
        for m in open_rows:
            if (ts-_parse_ts(m[3])).total_seconds()>INACTIVITY_SECONDS or (ts-_parse_ts(m[2])).total_seconds()>MAX_WINDOW_SECONDS or m[4] != policy or m[5] != vis:
                finalize_moment(conn,m[0]); continue
            shift = "different" in _canon(val) or "unrelated" in _canon(val)
            if not shift and not chosen: chosen=m[0]
            else: finalize_moment(conn,m[0]); _diag(conn,guild,"window_split","topic_or_visibility_shift",m[0],eid)
        if not chosen:
            started=obs or _now(); chosen=stable_moment_id(guild,chan,topic,started)
            conn.execute("INSERT OR IGNORE INTO memory_moment_windows(moment_id,guild_id,channel_id,channel_name,channel_policy,route_mode,topic_key,window_started_at,last_activity_at,lifecycle_status,visibility,public_usable,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",(chosen,guild,chan,cname,policy,route,topic,started,started,"open",vis,int(bool(pub)),_now(),_now()))
            _diag(conn,guild,"window_opened","ok",chosen,eid)
        role_name = "human_author" if role=="user" else "bnl_participant"
        before=conn.execute("SELECT COUNT(*) FROM memory_moment_members WHERE moment_id=? AND ledger_entry_id=?",(chosen,eid)).fetchone()[0]
        conn.execute("INSERT OR IGNORE INTO memory_moment_members VALUES(?,?,?,?,?,?)",(chosen,eid,int(seq or 0),obs,role_name,_now()))
        if before: _diag(conn,guild,"exact_source_duplicate_ignored","duplicate",chosen,eid)
        pkey = subj if role=="user" else BNL_SUBJECT_KEY; pname = disp if role=="user" else "BNL-01"
        existing=conn.execute("SELECT authored_entry_count, participation_order FROM memory_moment_participants WHERE moment_id=? AND participant_key=? AND participant_role=?",(chosen,pkey,role_name)).fetchone()
        order=existing[1] if existing else conn.execute("SELECT COUNT(*) FROM memory_moment_participants WHERE moment_id=?",(chosen,)).fetchone()[0]
        if existing:
            conn.execute("UPDATE memory_moment_participants SET last_seen_at=?, authored_entry_count=authored_entry_count+?, updated_at=? WHERE moment_id=? AND participant_key=? AND participant_role=?",(obs,1 if role=="user" and not before else 0,_now(),chosen,pkey,role_name))
        else:
            conn.execute("INSERT INTO memory_moment_participants VALUES(?,?,?,?,?,?,?,?,?,?)",(chosen,pkey,(pname or "")[:120],role_name,obs,obs,1 if role=="user" else 0,order,_now(),_now()))
        _recount(conn, chosen); _diag(conn,guild,"window_extended","ok",chosen,eid)
        return MomentObservationResult("observed","ok",chosen,eid)
    except Exception:
        try: _diag(conn,0,"moment_processing_error","exception",entry_id=ledger_entry_id)
        except Exception: pass
        return MomentObservationResult("error","exception",ledger_entry_id=ledger_entry_id)

def _entries(conn, moment_id):
    return conn.execute("""SELECT e.* FROM memory_moment_members m JOIN memory_ledger_entries e ON e.entry_id=m.ledger_entry_id WHERE m.moment_id=? ORDER BY e.observed_at, e.source_sequence, e.entry_id""",(moment_id,)).fetchall()

def _recount(conn, moment_id):
    rows=_entries(conn,moment_id); humans=[r for r in rows if r[13]=="user" and _meaningful(r[7] or "", "user", r[6] or "")]; models=[r for r in rows if r[13]!="user"]
    parts=conn.execute("SELECT COUNT(DISTINCT participant_key) FROM memory_moment_participants WHERE moment_id=? AND participant_role='human_author'",(moment_id,)).fetchone()[0]
    vis=max([r[19] for r in rows] or ["unknown"], key=lambda v: VIS_RANK.get(v,5)); pub=all(bool(r[22]) for r in rows) if rows else False
    conn.execute("UPDATE memory_moment_windows SET human_entry_count=?, model_entry_count=?, participant_count=?, visibility=?, public_usable=?, last_activity_at=COALESCE((SELECT MAX(observed_at) FROM memory_moment_members WHERE moment_id=?), last_activity_at), updated_at=? WHERE moment_id=?",(len(humans),len(models),parts,vis,int(pub),moment_id,_now(),moment_id))

def _qualify(rows):
    humans=[r for r in rows if r[13]=="user" and _meaningful(r[7] or "", "user", r[6] or "")]; models=[r for r in rows if r[13]!="user"]
    human_parts={r[3] for r in humans}; text=" ".join((r[7] or "") for r in humans).lower(); strong=any(m in text for m in STRONG_MARKERS) or any(r[6]=="remembered_number" for r in humans)
    if len(human_parts)>=2 and len(humans)>=3: return "shared_activity", "two_humans_three_meaningful_entries"
    if len(human_parts)==1 and models and (len(humans)>=3 or strong): return "conversational", "one_human_bnl_meaningful_continuity_or_marker"
    if len(human_parts)==1 and strong and len(humans)>=1: return "conversational", "explicit_marker_continuity"
    return "", "low_signal_or_bnl_only"

def _summary(rows, qtype, reason):
    vals=[]
    for r in rows:
        if r[13]=="user" and (r[6]=="remembered_number" or any(m in (r[7] or "").lower() for m in STRONG_MARKERS)):
            vals.append(f"{r[6]}={str(r[7])[:80]}")
    detail=("; ".join(dict.fromkeys(vals)) or "bounded conversation activity")[:240]
    return f"Derived moment ({qtype}): {detail}; reason={reason}."

def finalize_moment(conn: sqlite3.Connection, moment_id: str) -> MomentObservationResult:
    ensure_moment_schema(conn); win=conn.execute("SELECT guild_id,channel_id,channel_name,channel_policy,route_mode,topic_key,window_started_at,last_activity_at,visibility,public_usable,lifecycle_status FROM memory_moment_windows WHERE moment_id=?",(moment_id,)).fetchone()
    if not win: return MomentObservationResult(reason_code="missing_window", moment_id=moment_id)
    if win[10]=="finalized": return MomentObservationResult("deduplicated","already_finalized",moment_id,stable_moment_entry_id(moment_id))
    rows=_entries(conn,moment_id); qtype, reason=_qualify(rows)
    if not qtype:
        conn.execute("UPDATE memory_moment_windows SET lifecycle_status='rejected', qualification_reason=?, finalized_at=?, updated_at=? WHERE moment_id=?",(reason,_now(),_now(),moment_id)); _diag(conn,win[0],"window_rejected",reason,moment_id); return MomentObservationResult("rejected",reason,moment_id)
    source_ids=[r[0] for r in rows]
    if any(not conn.execute("SELECT 1 FROM memory_ledger_entries WHERE entry_id=?",(x,)).fetchone() for x in source_ids):
        _diag(conn,win[0],"lineage_validation_failure","dangling_source",moment_id); return MomentObservationResult("error","dangling_source",moment_id)
    sal=min(1.0,0.2+0.12*len([r for r in rows if r[13]=="user"])+0.08*len({r[3] for r in rows if r[13]=="user"}))
    summary=_summary(rows,qtype,reason); participants=[]
    for p in conn.execute("SELECT participant_key,safe_display_name,participant_role,participation_order FROM memory_moment_participants WHERE moment_id=? ORDER BY participation_order,participant_key",(moment_id,)).fetchall(): participants.append(LedgerParticipant(p[0],p[1] or "",p[2],p[3]))
    value=json.dumps({"schema":MOMENT_SCHEMA_VERSION,"moment_id":moment_id,"summary":summary,"window_started_at":win[6],"last_activity_at":win[7],"topic_key":win[5],"qualification_type":qtype,"qualification_reason":reason,"salience":sal,"participants":[p.participant_key for p in participants],"public_usable":bool(win[9]),"lifecycle_status":"finalized"},sort_keys=True)
    entry=LedgerEntry(guild_id=win[0],source_table="memory_moment_windows",source_row_id=moment_id,source_revision="1",source_role="derived_assessment",entry_type="shared_moment",subject_key=f"moment:{moment_id}",predicate_key="shared_moment",value=value,source_class=SourceClass.DERIVED_SUMMARY,route_mode=win[4] or "unknown",channel_id=win[1],channel_name=win[2] or "",channel_policy=win[3] or "",visibility=Visibility(win[8] or "unknown"),confidence=Confidence.LOW,public_usable=bool(win[9]),derived=True,projection=True,salience=sal,observed_at=win[7],source_sequence=0,valid_from=win[6],valid_until=win[7],freshness="shadow_moment_v1",lifecycle_status="active",participants=tuple(participants),lineage=tuple(("derived_from",x) for x in source_ids))
    res=insert_ledger_entry(conn,entry)
    moment_entry=res.entry_id
    for sid in source_ids: conn.execute("INSERT OR IGNORE INTO memory_ledger_lineage VALUES(?,?,?,?,?)",(sid,win[0],"part_of_moment",moment_entry,_now()))
    conn.execute("UPDATE memory_moment_windows SET lifecycle_status='finalized', finalized_at=?, qualification_type=?, qualification_reason=?, salience=?, summary=?, updated_at=? WHERE moment_id=?",(_now(),qtype,reason,sal,summary,_now(),moment_id)); _diag(conn,win[0],"window_finalized",reason,moment_id,moment_entry)
    return MomentObservationResult(res.outcome,res.reason_code,moment_id,moment_entry)

def sweep_expired_windows(conn: sqlite3.Connection, *, guild_id:int|None=None, now:str|None=None):
    ensure_moment_schema(conn); base=_parse_ts(now or _now()); params=[]; where="lifecycle_status='open'"
    if guild_id is not None: where+=" AND guild_id=?"; params.append(guild_id)
    out=[]
    for mid,last in conn.execute(f"SELECT moment_id,last_activity_at FROM memory_moment_windows WHERE {where}",params).fetchall():
        if (base-_parse_ts(last)).total_seconds()>=INACTIVITY_SECONDS: out.append(finalize_moment(conn,mid))
    return out

def handle_source_correction(conn: sqlite3.Connection, source_entry_id: str) -> int:
    ensure_moment_schema(conn); rows=conn.execute("SELECT DISTINCT moment_id FROM memory_moment_members WHERE ledger_entry_id=?",(source_entry_id,)).fetchall()
    for (mid,) in rows: conn.execute("UPDATE memory_moment_windows SET lifecycle_status='needs_review', updated_at=? WHERE moment_id=?",(_now(),mid)); _diag(conn,0,"moment_awaiting_review","source_corrected",mid,source_entry_id)
    return len(rows)

def render_shadow_moment_context(conn: sqlite3.Connection, *, guild_id:int, channel_id:int, participant_key:str="", visibility:str="public_safe", topic_text:str="", token_budget:int=120, freshness_days:int=3650) -> str:
    ensure_moment_schema(conn); cutoff=(datetime.now(timezone.utc)-timedelta(days=freshness_days)).isoformat(); toks=set(_tokens(topic_text)); lines=[]; used=0
    for r in conn.execute("SELECT moment_id,summary,topic_key,visibility,last_activity_at,salience FROM memory_moment_windows WHERE guild_id=? AND channel_id=? AND lifecycle_status='finalized' AND last_activity_at>=? ORDER BY salience DESC,last_activity_at DESC",(guild_id,channel_id,cutoff)).fetchall():
        if VIS_RANK.get(r[3],5)>VIS_RANK.get(visibility,0): continue
        if participant_key and not conn.execute("SELECT 1 FROM memory_moment_participants WHERE moment_id=? AND participant_key=?",(r[0],participant_key)).fetchone(): continue
        if toks and not (toks & set(_tokens((r[1] or '')+' '+(r[2] or ''))) or ({"number","numbers"} & toks and "remembered_number" in (r[1] or ""))): continue
        line=f"[Derived moment context] {r[1]}"
        n=len(line.split())
        if used+n>token_budget:
            if not lines:
                line=" ".join(line.split()[:max(1, token_budget)])
                lines.append(line)
            break
        lines.append(line); used+=n
    return "\n".join(lines)

def build_moment_evaluation_report(conn: sqlite3.Connection, *, guild_id:int|None=None) -> dict[str,Any]:
    ensure_moment_schema(conn); where=""; params=[]
    if guild_id is not None: where=" WHERE guild_id=?"; params=[guild_id]
    def one(sql,p=None):
        bind = params if p is None and "?" in sql else ([] if p is None else p)
        return conn.execute(sql, bind).fetchone()[0]
    report={"eligible_entries_observed":one(f"SELECT COUNT(*) FROM memory_moment_diagnostics{where + (' AND' if where else ' WHERE')} event_type='eligible_ledger_entry_observed'"),"open_windows":one(f"SELECT COUNT(*) FROM memory_moment_windows{where + (' AND' if where else ' WHERE')} lifecycle_status='open'"),"finalized_moments":one(f"SELECT COUNT(*) FROM memory_moment_windows{where + (' AND' if where else ' WHERE')} lifecycle_status='finalized'"),"processing_errors":one(f"SELECT COUNT(*) FROM memory_moment_diagnostics{where + (' AND' if where else ' WHERE')} event_type='moment_processing_error'")}
    report["rejected_windows_by_reason"]=dict(conn.execute(f"SELECT qualification_reason,COUNT(*) FROM memory_moment_windows{where + (' AND' if where else ' WHERE')} lifecycle_status='rejected' GROUP BY qualification_reason",params).fetchall())
    report["moments_by_qualification_type"]=dict(conn.execute(f"SELECT qualification_type,COUNT(*) FROM memory_moment_windows{where} GROUP BY qualification_type",params).fetchall())
    report["moments_by_visibility"]=dict(conn.execute(f"SELECT visibility,COUNT(*) FROM memory_moment_windows{where} GROUP BY visibility",params).fetchall())
    report["moments_by_lifecycle"]=dict(conn.execute(f"SELECT lifecycle_status,COUNT(*) FROM memory_moment_windows{where} GROUP BY lifecycle_status",params).fetchall())
    prefix="m.guild_id=? AND " if guild_id is not None else ""
    p=[guild_id] if guild_id is not None else []
    report.update({
      "one_human_conversational_moments":one(f"SELECT COUNT(*) FROM memory_moment_windows m WHERE {prefix}qualification_type='conversational'",p),
      "multi_human_shared_moments":one(f"SELECT COUNT(*) FROM memory_moment_windows m WHERE {prefix}qualification_type='shared_activity'",p),
      "average_participant_count":one(f"SELECT COALESCE(AVG(participant_count),0) FROM memory_moment_windows m WHERE {prefix}lifecycle_status='finalized'",p),
      "duplicate_memberships":one("SELECT COUNT(*) FROM (SELECT moment_id,ledger_entry_id,COUNT(*) c FROM memory_moment_members GROUP BY moment_id,ledger_entry_id HAVING c>1)"),
      "bnl_only_violations":one(f"SELECT COUNT(*) FROM memory_moment_windows m WHERE {prefix}lifecycle_status='finalized' AND human_entry_count=0",p),
      "cross_guild_violations":one("SELECT COUNT(*) FROM memory_moment_members mm JOIN memory_moment_windows mw ON mw.moment_id=mm.moment_id JOIN memory_ledger_entries e ON e.entry_id=mm.ledger_entry_id WHERE e.guild_id<>mw.guild_id"),
      "cross_channel_violations":one("SELECT COUNT(*) FROM memory_moment_members mm JOIN memory_moment_windows mw ON mw.moment_id=mm.moment_id JOIN memory_ledger_entries e ON e.entry_id=mm.ledger_entry_id WHERE e.channel_id<>mw.channel_id"),
      "incompatible_visibility_violations":one("SELECT COUNT(*) FROM memory_moment_members mm JOIN memory_moment_windows mw ON mw.moment_id=mm.moment_id JOIN memory_ledger_entries e ON e.entry_id=mm.ledger_entry_id WHERE e.visibility<>mw.visibility"),
      "dangling_lineage_targets":one("SELECT COUNT(*) FROM memory_ledger_lineage l LEFT JOIN memory_ledger_entries e ON e.guild_id=l.guild_id AND e.entry_id=l.target_entry_id WHERE e.entry_id IS NULL"),
      "affected_moments_awaiting_correction_review":one(f"SELECT COUNT(*) FROM memory_moment_windows{where + (' AND' if where else ' WHERE')} lifecycle_status='needs_review'"),
      "finalization_latency":one(f"SELECT COALESCE(AVG(strftime('%s',finalized_at)-strftime('%s',last_activity_at)),0) FROM memory_moment_windows{where + (' AND' if where else ' WHERE')} finalized_at IS NOT NULL")})
    return report
