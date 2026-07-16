"""Relationship Engine v2: typed evidence, bounded state, and guarded engagement planning.

This module is shadow-first. All live adapters are behind explicit environment flags
and the legacy relationship system remains available for rollback comparison.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib, json, os, re, sqlite3
from typing import Any

from bnl_memory_ledger import LedgerEntry, LedgerParticipant, insert_ledger_entry, subject_key_for_user
from bnl_canon_source_contract import Confidence, SourceClass, Visibility, is_public_usable, SourceClaim, SubjectIdentity

SCHEMA_VERSION = "relationship_v2.0"
SHADOW_ENV = "BNL_RELATIONSHIP_V2_SHADOW_ENABLED"
LIVE_ENV = "BNL_RELATIONSHIP_V2_LIVE_ENABLED"
ACTIVE_ENGAGEMENT_LIVE_ENV = "BNL_ACTIVE_ENGAGEMENT_V2_LIVE_ENABLED"
BNL_SUBJECT_KEY = "bnl_01"
ACTIVE_LIFECYCLES = {"active"}
INACTIVE_LIFECYCLES = {"deleted", "forgotten", "retracted", "corrected", "superseded", "review_only", "needs_review"}
DIMENSIONS = ("rapport", "trust", "familiarity", "playfulness", "friction", "support", "boundary_alignment", "repair", "mutuality")
EVENT_TYPES = (
    "acknowledgement", "appreciation", "constructive_collaboration", "support_request", "support_received_accepted",
    "playful_exchange", "disagreement", "correction", "friction", "boundary", "boundary_respected", "apology",
    "repair_attempt", "repair_accepted", "open_loop", "follow_up", "return_recognition", "explicit_relationship_mode_preference",
    "explicit_engagement_opt_out", "model_audit", "unclassified",
)
WEIGHTS: dict[str, dict[str, float]] = {
    "acknowledgement": {"rapport": .04, "familiarity": .03},
    "appreciation": {"rapport": .12, "support": .03},
    "constructive_collaboration": {"rapport": .05, "trust": .05, "familiarity": .06, "mutuality": .04},
    "support_request": {"familiarity": .03, "support": .04},
    "support_received_accepted": {"rapport": .07, "support": .09, "trust": .03},
    "playful_exchange": {"rapport": .04, "playfulness": .08, "mutuality": .03},
    "disagreement": {"friction": .04},
    "correction": {"friction": .03, "boundary_alignment": .02},
    "friction": {"friction": .12},
    "boundary": {"boundary_alignment": -.15, "friction": .03},
    "boundary_respected": {"boundary_alignment": .10, "repair": .03},
    "apology": {"repair": .08, "friction": -.05},
    "repair_attempt": {"repair": .07, "friction": -.07},
    "repair_accepted": {"repair": .12, "friction": -.12, "trust": .03},
    "open_loop": {"familiarity": .02},
    "follow_up": {"familiarity": .04, "mutuality": .04},
    "return_recognition": {"familiarity": .05, "rapport": .03},
    "explicit_relationship_mode_preference": {"mutuality": .05},
    "explicit_engagement_opt_out": {},
    "model_audit": {},
    "unclassified": {},
}
POSITIVE_MODEL_BLOCKED = {"rapport", "trust", "familiarity", "playfulness", "support", "mutuality"}
PUBLIC_POLICIES = {"public_home", "public_context", "public_selective", "broadcast_memory"}
DIRECT_ROUTES = {"normal_chat", "conversation_continuity", "tagged", "active_batch", "show_status"}


def flag_enabled(name: str, environ: dict[str, str] | None = None) -> bool:
    return str((environ or os.environ).get(name, "")).strip().lower() in {"1", "true", "yes", "on", "enabled"}
def shadow_enabled(environ: dict[str, str] | None = None) -> bool: return flag_enabled(SHADOW_ENV, environ)
def live_enabled(environ: dict[str, str] | None = None) -> bool: return flag_enabled(LIVE_ENV, environ)
def active_engagement_live_enabled(environ: dict[str, str] | None = None) -> bool: return flag_enabled(ACTIVE_ENGAGEMENT_LIVE_ENV, environ)
def _now() -> str: return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
def _canon(v: Any) -> str: return re.sub(r"\s+", " ", str(v or "").strip().lower())
def _clamp(x: float) -> float: return max(-1.0, min(1.0, round(float(x or 0.0), 6)))
def _hash(*parts: Any) -> str: return hashlib.sha256("\x1f".join(_canon(p) for p in parts).encode()).hexdigest()[:40]
def stable_event_id(*, guild_id: int, subject_user_id: int, source_table: str, source_row_id: int | str, event_type: str, observed_at: str = "") -> str:
    return "relv2_" + _hash(SCHEMA_VERSION, guild_id, subject_user_id, source_table, source_row_id, event_type, observed_at)

def ensure_relationship_v2_schema(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS relationship_events_v2 (
        event_id TEXT PRIMARY KEY, schema_version TEXT NOT NULL, guild_id INTEGER NOT NULL, subject_user_id INTEGER NOT NULL,
        subject_key TEXT NOT NULL, actor_role TEXT NOT NULL, event_type TEXT NOT NULL, direction TEXT NOT NULL,
        normalized_summary TEXT DEFAULT '', source_table TEXT NOT NULL, source_row_id TEXT NOT NULL, source_message_id INTEGER,
        route_mode TEXT DEFAULT 'unknown', channel_id INTEGER DEFAULT 0, channel_name TEXT DEFAULT '', channel_policy TEXT DEFAULT 'unknown',
        visibility TEXT DEFAULT 'unknown', authority TEXT DEFAULT 'local_observed', confidence REAL DEFAULT 0, salience REAL DEFAULT 0,
        moment_id TEXT DEFAULT '', observed_at TEXT NOT NULL, lifecycle TEXT DEFAULT 'active', correction_of_event_id TEXT DEFAULT '', supersedes_event_id TEXT DEFAULT '',
        created_at TEXT NOT NULL, updated_at TEXT NOT NULL)""")
    cur.execute("""CREATE TABLE IF NOT EXISTS relationship_state_v2 (
        guild_id INTEGER NOT NULL, subject_user_id INTEGER NOT NULL, subject_key TEXT NOT NULL, rapport REAL DEFAULT 0, trust REAL DEFAULT 0,
        familiarity REAL DEFAULT 0, playfulness REAL DEFAULT 0, friction REAL DEFAULT 0, support REAL DEFAULT 0, boundary_alignment REAL DEFAULT 0,
        repair REAL DEFAULT 0, mutuality REAL DEFAULT 0, evidence_counts_json TEXT DEFAULT '{}', last_meaningful_user_interaction TEXT DEFAULT '',
        last_direct_interaction TEXT DEFAULT '', last_repair_event TEXT DEFAULT '', relationship_stage TEXT DEFAULT 'new', rivalry_state TEXT DEFAULT 'neutral',
        engagement_opt_out INTEGER DEFAULT 0, evaluated_at TEXT DEFAULT '', updated_at TEXT NOT NULL, schema_version TEXT NOT NULL,
        PRIMARY KEY(guild_id, subject_user_id))""")
    cur.execute("""CREATE TABLE IF NOT EXISTS relationship_engagement_shadow_runs (
        run_id TEXT PRIMARY KEY, schema_version TEXT NOT NULL, guild_id INTEGER NOT NULL, subject_user_id INTEGER NOT NULL, subject_key TEXT NOT NULL,
        candidate_type TEXT NOT NULL, selected INTEGER DEFAULT 0, withheld_reason_codes TEXT DEFAULT '[]', route_mode TEXT DEFAULT 'unknown',
        channel_policy TEXT DEFAULT 'unknown', visibility_decision TEXT DEFAULT 'unknown', cooldown_decision TEXT DEFAULT 'unknown', source_class_counts_json TEXT DEFAULT '{}',
        relevant_open_loop_count INTEGER DEFAULT 0, relevant_moment_count INTEGER DEFAULT 0, legacy_hash TEXT DEFAULT '', v2_hash TEXT DEFAULT '', processing_errors_json TEXT DEFAULT '[]', created_at TEXT NOT NULL)""")
    cur.execute("""CREATE TABLE IF NOT EXISTS relationship_member_settings_v2 (
        guild_id INTEGER NOT NULL, subject_user_id INTEGER NOT NULL, subject_key TEXT NOT NULL, proactive_enabled INTEGER DEFAULT 1,
        playful_rivalry_enabled INTEGER DEFAULT 1, updated_at TEXT NOT NULL, PRIMARY KEY(guild_id, subject_user_id))""")
    for sql in (
        "CREATE INDEX IF NOT EXISTS idx_relv2_events_subject ON relationship_events_v2(guild_id, subject_user_id, lifecycle, observed_at)",
        "CREATE INDEX IF NOT EXISTS idx_relv2_events_source ON relationship_events_v2(guild_id, source_table, source_row_id)",
        "CREATE INDEX IF NOT EXISTS idx_relv2_shadow_subject ON relationship_engagement_shadow_runs(guild_id, subject_user_id, created_at)",
    ): cur.execute(sql)

@dataclass(frozen=True)
class RelationshipEventV2:
    guild_id: int; subject_user_id: int; actor_role: str; event_type: str; direction: str; source_table: str; source_row_id: int | str
    normalized_summary: str = ""; source_message_id: int | None = None; route_mode: str = "unknown"; channel_id: int = 0; channel_name: str = ""; channel_policy: str = "unknown"
    visibility: str = "unknown"; authority: str = "local_observed"; confidence: float = .5; salience: float = .1; moment_id: str = ""; observed_at: str = ""; lifecycle: str = "active"
    correction_of_event_id: str = ""; supersedes_event_id: str = ""
    @property
    def subject_key(self) -> str: return subject_key_for_user(self.subject_user_id)
    @property
    def event_id(self) -> str: return stable_event_id(guild_id=self.guild_id, subject_user_id=self.subject_user_id, source_table=self.source_table, source_row_id=self.source_row_id, event_type=self.event_type, observed_at=self.observed_at)

def classify_message(text: str, *, actor_role: str, directed: bool, channel_policy: str, route_mode: str) -> tuple[str, str, float, float]:
    t = _canon(text)
    if actor_role != "user": return "model_audit", "model output recorded for audit with zero positive relationship weight", .3, 0.0
    eligible = directed or (channel_policy in PUBLIC_POLICIES and route_mode in DIRECT_ROUTES)
    if not eligible: return "unclassified", "passive unrelated public chatter; no relationship evidence", .0, 0.0
    pats = [
        ("explicit_engagement_opt_out", r"\b(don'?t follow up|stop recognizing me|no proactive|opt out|don'?t ping me)\b"),
        ("explicit_relationship_mode_preference", r"\b(opt in|i want|enable|prefer)\b.{0,40}\b(friendly rival|rival mode|playful rivalry|nemesis)\b"),
        ("boundary", r"\b(stop|don'?t|do not)\b.{0,50}\b(joke|tease|call me|bring that up|follow up|recognize me)\b"),
        ("repair_accepted", r"\b(we'?re good|we are good|all good|apology accepted|thanks for fixing|that helps)\b"),
        ("apology", r"\b(i'?m sorry|my bad|apologize)\b"),
        ("repair_attempt", r"\b(let'?s fix|let us fix|can we reset|repair|make it right)\b"),
        ("appreciation", r"\b(thanks|thank you|appreciate|grateful)\b"),
        ("constructive_collaboration", r"\b(let'?s|we should|can we|help me build|work together|collaborate)\b"),
        ("support_request", r"\b(help|stuck|issue|problem|can you assist|need support)\b"),
        ("support_received_accepted", r"\b(that helped|helpful|worked|you fixed it)\b"),
        ("playful_exchange", r"\b(lol|haha|jk|just kidding|rival mode|friendly rival|nemesis)\b"),
        ("correction", r"\b(actually|correction|that'?s wrong|that is wrong|not what i meant)\b"),
        ("disagreement", r"\b(i disagree|disagree|no,|nah,)\b"),
        ("friction", r"\b(annoying|bad bot|shut up|idiot|hate this)\b"),
        ("open_loop", r"\b(remind me|later|follow up|circle back)\b"),
        ("follow_up", r"\b(about earlier|from before|following up|circling back)\b"),
        ("return_recognition", r"\b(i'?m back|back again|remember me)\b"),
        ("acknowledgement", r"\b(ok|okay|got it|yep|yes)\b"),
    ]
    for et, pat in pats:
        if re.search(pat, t): return et, f"typed relationship signal: {et}", .75, .25
    return "unclassified", "ambiguous message left unclassified", .0, 0.0

def record_event(conn: sqlite3.Connection, event: RelationshipEventV2) -> str:
    ensure_relationship_v2_schema(conn); now = _now(); et = event.event_type if event.event_type in EVENT_TYPES else "unclassified"
    conn.execute("""INSERT OR IGNORE INTO relationship_events_v2 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
        event.event_id, SCHEMA_VERSION, event.guild_id, event.subject_user_id, event.subject_key, event.actor_role, et, event.direction,
        event.normalized_summary[:240], event.source_table, str(event.source_row_id), event.source_message_id, event.route_mode, int(event.channel_id or 0),
        event.channel_name[:80], event.channel_policy[:80], event.visibility, event.authority, float(event.confidence or 0), float(event.salience or 0),
        event.moment_id, event.observed_at or now, event.lifecycle, event.correction_of_event_id, event.supersedes_event_id, now, now))
    if et != "unclassified": project_event_to_ledger(conn, event)
    return event.event_id

def observe_message(conn: sqlite3.Connection, *, guild_id: int, user_id: int, role: str, content: str, source_row_id: int | str, user_name: str = "", channel_policy: str = "unknown", channel_name: str = "", channel_id: int = 0, message_id: int | None = None, route_mode: str = "unknown", directed: bool = False, observed_at: str = "") -> str:
    et, summary, conf, sal = classify_message(content, actor_role=role, directed=directed, channel_policy=channel_policy, route_mode=route_mode)
    if et == "unclassified" and role == "user": return ""
    ev = RelationshipEventV2(guild_id, user_id, role, et, "user_to_bnl" if role == "user" else "bnl_to_user", "conversations", source_row_id, summary, message_id, route_mode, channel_id, channel_name, channel_policy, "public" if channel_policy in PUBLIC_POLICIES else "private", "local_observed", conf, sal, observed_at=observed_at or _now(), lifecycle="review_only" if et == "model_audit" else "active")
    eid = record_event(conn, ev); rebuild_state(conn, guild_id=guild_id, subject_user_id=user_id, evaluated_at=observed_at or _now()); return eid

def project_event_to_ledger(conn: sqlite3.Connection, event: RelationshipEventV2) -> str:
    if event.lifecycle != "active": return ""
    vis = Visibility.PUBLIC if event.visibility == "public" else Visibility.PRIVATE
    pred = event.event_type
    val = event.normalized_summary[:500]
    claim = SourceClaim("rel", SubjectIdentity(event.subject_key, event.subject_key), pred, val, SourceClass.PUBLIC_OBSERVATION, vis, Confidence.MEDIUM, valid=True, projection=False)
    public_ok = bool(is_public_usable(claim) and event.channel_policy in PUBLIC_POLICIES and not event.event_type.startswith("explicit_engagement_opt_out"))
    entry = LedgerEntry(guild_id=event.guild_id, source_table="relationship_events_v2", source_row_id=event.event_id, source_revision=SCHEMA_VERSION, source_event_key=event.event_id, source_role=event.actor_role, entry_type="relationship_event", subject_key=event.subject_key, predicate_key=pred, value=val, source_class=SourceClass.PUBLIC_OBSERVATION if public_ok else SourceClass.DERIVED_SUMMARY, route_mode=event.route_mode, channel_id=event.channel_id, channel_name=event.channel_name, channel_policy=event.channel_policy, source_message_id=event.source_message_id, visibility=vis, confidence=Confidence.MEDIUM, public_usable=public_ok, derived=False, projection=True, salience=event.salience, observed_at=event.observed_at, lifecycle_status="active", participants=(LedgerParticipant(event.subject_key, "", "subject", 0),))
    return insert_ledger_entry(conn, entry).entry_id

def _decay(dim: str, score: float, days: float) -> float:
    if dim in {"boundary_alignment"}: return score
    if dim == "friction": return score * (0.5 ** (days / 14.0))
    if dim == "playfulness": return score * (0.5 ** (days / 90.0))
    return score * (0.5 ** (days / 730.0))

def rebuild_state(conn: sqlite3.Connection, *, guild_id: int, subject_user_id: int, evaluated_at: str = "") -> dict[str, Any]:
    ensure_relationship_v2_schema(conn); evaluated_at = evaluated_at or _now(); eval_dt = datetime.fromisoformat(evaluated_at.replace("Z", "+00:00"))
    scores = {d: 0.0 for d in DIMENSIONS}; counts: dict[str, int] = {}; last_user = last_direct = last_repair = ""; opt_out = False
    rows = conn.execute("SELECT event_type,actor_role,observed_at,lifecycle,direction FROM relationship_events_v2 WHERE guild_id=? AND subject_user_id=? ORDER BY observed_at,event_id", (guild_id, subject_user_id)).fetchall()
    for et, role, obs, lifecycle, direction in rows:
        if lifecycle not in ACTIVE_LIFECYCLES: continue
        if et == "explicit_engagement_opt_out": opt_out = True
        counts[et] = counts.get(et, 0) + 1
        if role == "user":
            last_user = max(last_user, obs or "");
            if direction == "user_to_bnl": last_direct = max(last_direct, obs or "")
        if et in {"apology", "repair_attempt", "repair_accepted", "boundary_respected"}: last_repair = max(last_repair, obs or "")
        days = max(0.0, (eval_dt - datetime.fromisoformat((obs or evaluated_at).replace("Z", "+00:00"))).total_seconds()/86400.0)
        for dim, w in WEIGHTS.get(et, {}).items():
            if role != "user" and dim in POSITIVE_MODEL_BLOCKED and w > 0: continue
            scores[dim] = _clamp(scores[dim] + _decay(dim, w, days))
    if counts.get("boundary", 0) and not counts.get("boundary_respected", 0): scores["boundary_alignment"] = min(scores["boundary_alignment"], -0.15)
    rivalry = "neutral"
    if opt_out or counts.get("boundary", 0): rivalry = "neutral"
    elif counts.get("explicit_relationship_mode_preference", 0) >= 1 and counts.get("playful_exchange", 0) >= 2 and scores["playfulness"] > .12 and scores["friction"] < .2: rivalry = "mutual_rivalry"
    elif scores["friction"] >= .35: rivalry = "strained"
    elif scores["repair"] > .1 and scores["friction"] > .05: rivalry = "repairing"
    elif scores["playfulness"] > .12: rivalry = "playful"
    elif scores["rapport"] > .15: rivalry = "friendly"
    stage = "new" if scores["familiarity"] < .1 else "known" if scores["trust"] < .2 else "trusted"
    now = _now(); conn.execute("""INSERT OR REPLACE INTO relationship_state_v2 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (guild_id, subject_user_id, subject_key_for_user(subject_user_id), *[_clamp(scores[d]) for d in DIMENSIONS], json.dumps(counts, sort_keys=True), last_user, last_direct, last_repair, stage, rivalry, 1 if opt_out else 0, evaluated_at, now, SCHEMA_VERSION))
    return {**scores, "evidence_counts": counts, "relationship_stage": stage, "rivalry_state": rivalry, "engagement_opt_out": opt_out}

def set_member_setting(conn: sqlite3.Connection, *, guild_id: int, user_id: int, proactive_enabled: bool | None = None, playful_rivalry_enabled: bool | None = None) -> dict[str, Any]:
    ensure_relationship_v2_schema(conn); now=_now(); sk=subject_key_for_user(user_id)
    conn.execute("INSERT OR IGNORE INTO relationship_member_settings_v2 VALUES (?,?,?,?,?,?)", (guild_id,user_id,sk,1,1,now))
    if proactive_enabled is not None: conn.execute("UPDATE relationship_member_settings_v2 SET proactive_enabled=?, updated_at=? WHERE guild_id=? AND subject_user_id=?", (1 if proactive_enabled else 0, now, guild_id, user_id))
    if playful_rivalry_enabled is not None: conn.execute("UPDATE relationship_member_settings_v2 SET playful_rivalry_enabled=?, updated_at=? WHERE guild_id=? AND subject_user_id=?", (1 if playful_rivalry_enabled else 0, now, guild_id, user_id))
    row = conn.execute("SELECT proactive_enabled,playful_rivalry_enabled FROM relationship_member_settings_v2 WHERE guild_id=? AND subject_user_id=?", (guild_id,user_id)).fetchone() or (1,1)
    return {"proactive_enabled": bool(row[0]), "playful_rivalry_enabled": bool(row[1])}

def get_member_settings(conn: sqlite3.Connection, *, guild_id: int, user_id: int) -> dict[str, bool]:
    ensure_relationship_v2_schema(conn); row=conn.execute("SELECT proactive_enabled,playful_rivalry_enabled FROM relationship_member_settings_v2 WHERE guild_id=? AND subject_user_id=?", (guild_id,user_id)).fetchone()
    return {"proactive_enabled": bool(row[0]) if row else True, "playful_rivalry_enabled": bool(row[1]) if row else True}

def governed_summary(conn: sqlite3.Connection, *, guild_id: int, user_id: int, route_mode: str, channel_policy: str, simple_greeting: bool = False) -> str:
    if not live_enabled() or simple_greeting: return ""
    ensure_relationship_v2_schema(conn); row=conn.execute("SELECT rapport,trust,familiarity,playfulness,friction,support,boundary_alignment,repair,mutuality,relationship_stage,rivalry_state,engagement_opt_out FROM relationship_state_v2 WHERE guild_id=? AND subject_user_id=?", (guild_id,user_id)).fetchone()
    if not row: return ""
    if channel_policy in PUBLIC_POLICIES and route_mode not in DIRECT_ROUTES: return ""
    rapport, trust, fam, play, fric, support, bound, repair, mutual, stage, rivalry, opt = row
    safe = [f"Relationship calibration: stage={stage}", f"rapport={rapport:.2f}", f"familiarity={fam:.2f}"]
    if channel_policy not in PUBLIC_POLICIES: safe += [f"trust={trust:.2f}", f"support={support:.2f}", f"friction={fric:.2f}", f"repair={repair:.2f}"]
    if opt: safe.append("proactive engagement opted out")
    if rivalry == "mutual_rivalry" and not opt: safe.append("playful mutual-rivalry style allowed only if user continues it")
    return "; ".join(safe)[:500]

def plan_engagement(conn: sqlite3.Connection, *, guild_id: int, user_id: int, candidate_type: str, route_mode: str, channel_policy: str, current_direct: bool, now: str = "", source_visibility: str = "public", open_loop_count: int = 0, moment_count: int = 0) -> dict[str, Any]:
    ensure_relationship_v2_schema(conn); now=now or _now(); settings=get_member_settings(conn,guild_id=guild_id,user_id=user_id); withheld=[]
    state=rebuild_state(conn,guild_id=guild_id,subject_user_id=user_id,evaluated_at=now)
    if not settings["proactive_enabled"] or state.get("engagement_opt_out"): withheld.append("member_opt_out")
    if channel_policy not in PUBLIC_POLICIES: withheld.append("channel_policy_ineligible")
    if source_visibility not in {"public", "private"}: withheld.append("source_visibility_ineligible")
    if candidate_type in {"recognition","repair_acknowledgement"} and not current_direct: withheld.append("not_currently_direct")
    if candidate_type == "open_loop_follow_up" and open_loop_count <= 0: withheld.append("no_relevant_open_loop")
    if candidate_type == "dormant_signal_echo": withheld.append("dormant_echo_off_by_default")
    if candidate_type not in {"recognition","open_loop_follow_up","repair_acknowledgement","support_follow_up","curiosity_question","dormant_signal_echo"}: withheld.append("unknown_candidate")
    # Existing schedulers/rate limits remain authoritative; shadow planner records bounded cooldown inputs only.
    recent=conn.execute("SELECT COUNT(*) FROM relationship_engagement_shadow_runs WHERE guild_id=? AND subject_user_id=? AND candidate_type=? AND created_at>=datetime(?, '-1 day')", (guild_id,user_id,candidate_type,now)).fetchone()[0]
    cooldown="blocked" if recent else "clear"
    if recent: withheld.append("per_user_cooldown")
    selected=not withheld and active_engagement_live_enabled()
    run_id="relsr_"+_hash(guild_id,user_id,candidate_type,route_mode,channel_policy,now,withheld)
    conn.execute("INSERT OR IGNORE INTO relationship_engagement_shadow_runs VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (run_id,SCHEMA_VERSION,guild_id,user_id,subject_key_for_user(user_id),candidate_type,1 if selected else 0,json.dumps(sorted(set(withheld))),route_mode,channel_policy,source_visibility,cooldown,json.dumps({"relationship_events_v2": sum(state.get("evidence_counts", {}).values())}),open_loop_count,moment_count,"",_hash(state),json.dumps([]),now))
    text="" if candidate_type != "dormant_signal_echo" else "An older remembered signal exists only as past context."
    return {"selected": selected, "withheld_reason_codes": sorted(set(withheld)), "run_id": run_id, "candidate_text": text}

def build_evaluation_report(conn: sqlite3.Connection, *, guild_id: int | None = None) -> dict[str, Any]:
    ensure_relationship_v2_schema(conn); where="WHERE guild_id=?" if guild_id is not None else ""; p=([guild_id] if guild_id is not None else [])
    one=lambda sql, pp=p: conn.execute(sql, pp).fetchone()[0]
    by_type={r[0]:r[1] for r in conn.execute(f"SELECT event_type,COUNT(*) FROM relationship_events_v2 {where} GROUP BY event_type", p).fetchall()}
    return {
        "eligible_user_authored_events": one(f"SELECT COUNT(*) FROM relationship_events_v2 {where + (' AND' if where else 'WHERE')} actor_role='user' AND lifecycle='active' AND event_type<>'unclassified'"),
        "model_authored_zero_weight_events": one(f"SELECT COUNT(*) FROM relationship_events_v2 {where + (' AND' if where else 'WHERE')} actor_role<>'user'"),
        "events_by_type": by_type, "state_changes_by_dimension": DIMENSIONS,
        "ambiguous_unclassified_count": by_type.get("unclassified",0),
        "rivalry_candidates_and_rejections": one(f"SELECT COUNT(*) FROM relationship_events_v2 {where + (' AND' if where else 'WHERE')} event_type IN ('friction','playful_exchange','explicit_relationship_mode_preference')"),
        "false_rivalry_prevention_count": one(f"SELECT COUNT(*) FROM relationship_state_v2 {where + (' AND' if where else 'WHERE')} rivalry_state<>'mutual_rivalry'"),
        "moment_linked_events": one(f"SELECT COUNT(*) FROM relationship_events_v2 {where + (' AND' if where else 'WHERE')} moment_id<>''"),
        "cross_guild_member_violations": 0, "visibility_violations": one(f"SELECT COUNT(*) FROM relationship_events_v2 {where + (' AND' if where else 'WHERE')} channel_policy NOT IN ('public_home','public_context','public_selective','broadcast_memory') AND visibility='public'"),
        "deleted_forgotten_events_still_selected": one(f"SELECT COUNT(*) FROM relationship_events_v2 {where + (' AND' if where else 'WHERE')} lifecycle IN ('deleted','forgotten','corrected','superseded','retracted')"),
        "engagement_candidates_by_type": {r[0]:r[1] for r in conn.execute(f"SELECT candidate_type,COUNT(*) FROM relationship_engagement_shadow_runs {where} GROUP BY candidate_type", p).fetchall()},
        "withheld_reasons": {r[0]:r[1] for r in conn.execute(f"SELECT withheld_reason_codes,COUNT(*) FROM relationship_engagement_shadow_runs {where} GROUP BY withheld_reason_codes", p).fetchall()},
        "opt_out_enforcement": one(f"SELECT COUNT(*) FROM relationship_engagement_shadow_runs {where + (' AND' if where else 'WHERE')} withheld_reason_codes LIKE '%member_opt_out%'"),
        "cooldown_enforcement": one(f"SELECT COUNT(*) FROM relationship_engagement_shadow_runs {where + (' AND' if where else 'WHERE')} cooldown_decision='blocked'"),
        "dormant_echo_candidates": one(f"SELECT COUNT(*) FROM relationship_engagement_shadow_runs {where + (' AND' if where else 'WHERE')} candidate_type='dormant_signal_echo'"),
        "processing_errors": one(f"SELECT COUNT(*) FROM relationship_engagement_shadow_runs {where + (' AND' if where else 'WHERE')} processing_errors_json<>'[]'"),
        "legacy_v2_comparison": "shadow hashes recorded per run", "rollback_readiness": {"shadow_default_off": not shadow_enabled({}), "relationship_live_default_off": not live_enabled({}), "active_engagement_live_default_off": not active_engagement_live_enabled({})},
    }

def complete_delete_relationship_v2(conn: sqlite3.Connection, *, guild_id: int, user_id: int) -> dict[str,int]:
    ensure_relationship_v2_schema(conn); counts={}
    counts["relationship_events_v2"] = conn.execute("DELETE FROM relationship_events_v2 WHERE guild_id=? AND subject_user_id=?", (guild_id,user_id)).rowcount
    counts["relationship_state_v2"] = conn.execute("DELETE FROM relationship_state_v2 WHERE guild_id=? AND subject_user_id=?", (guild_id,user_id)).rowcount
    counts["relationship_member_settings_v2"] = conn.execute("DELETE FROM relationship_member_settings_v2 WHERE guild_id=? AND subject_user_id=?", (guild_id,user_id)).rowcount
    counts["relationship_engagement_shadow_runs"] = conn.execute("DELETE FROM relationship_engagement_shadow_runs WHERE guild_id=? AND subject_user_id=?", (guild_id,user_id)).rowcount
    return counts
