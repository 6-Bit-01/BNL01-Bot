"""Relationship Engine v2: typed evidence, bounded state, and guarded engagement planning.

Shadow-first relationship intelligence for Discord interactions.  The module stores
private derived relationship evidence, reconstructs bounded state deterministically,
and exposes guarded adapters; it never sends Discord messages or enables live
behavior on its own.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib, json, os, re, sqlite3
from typing import Any, Mapping

from bnl_canon_source_contract import Confidence, SourceClass, Visibility
from bnl_memory_ledger import LedgerEntry, LedgerParticipant, insert_ledger_entry, subject_key_for_user

SCHEMA_VERSION = "relationship_v2.1"
SHADOW_ENV = "BNL_RELATIONSHIP_V2_SHADOW_ENABLED"
LIVE_ENV = "BNL_RELATIONSHIP_V2_LIVE_ENABLED"
ACTIVE_ENGAGEMENT_LIVE_ENV = "BNL_ACTIVE_ENGAGEMENT_V2_LIVE_ENABLED"
ACTIVE_LIFECYCLES = {"active"}
BLOCKED_LIFECYCLES = {"deleted", "forgotten", "retracted", "corrected", "superseded", "review_only", "needs_review"}
DIMENSIONS = ("rapport", "trust", "familiarity", "playfulness", "friction", "support", "boundary_alignment", "repair", "mutuality")
EVENT_TYPES = (
    "acknowledgement", "appreciation", "constructive_collaboration", "support_request", "support_received_accepted",
    "playful_exchange", "disagreement", "correction", "friction", "boundary", "boundary_respected", "apology",
    "repair_attempt", "repair_accepted", "open_loop", "follow_up", "return_recognition", "explicit_relationship_mode_preference",
    "explicit_engagement_opt_out", "explicit_engagement_opt_in", "model_audit", "model_playful_rivalry_acceptance", "unclassified",
)
WEIGHTS: dict[str, dict[str, float]] = {
    "acknowledgement": {"rapport": .04, "familiarity": .03},
    "appreciation": {"rapport": .12, "support": .03},
    "constructive_collaboration": {"rapport": .05, "trust": .05, "familiarity": .06, "mutuality": .04},
    "support_request": {"familiarity": .03, "support": .04},
    "support_received_accepted": {"rapport": .07, "support": .09, "trust": .03},
    "playful_exchange": {"rapport": .04, "playfulness": .08, "mutuality": .03},
    "disagreement": {"friction": .04}, "correction": {"friction": .03, "boundary_alignment": .02}, "friction": {"friction": .12},
    "boundary": {"boundary_alignment": -.15, "friction": .03}, "boundary_respected": {"boundary_alignment": .10, "repair": .03},
    "apology": {"repair": .08, "friction": -.05}, "repair_attempt": {"repair": .07, "friction": -.07},
    "repair_accepted": {"repair": .12, "friction": -.12, "trust": .03}, "open_loop": {"familiarity": .02},
    "follow_up": {"familiarity": .04, "mutuality": .04}, "return_recognition": {"familiarity": .05, "rapport": .03},
    "explicit_relationship_mode_preference": {}, "explicit_engagement_opt_out": {}, "explicit_engagement_opt_in": {},
    "model_audit": {}, "model_playful_rivalry_acceptance": {}, "unclassified": {},
}
POSITIVE_MODEL_BLOCKED = {"rapport", "trust", "familiarity", "playfulness", "support", "mutuality"}
PUBLIC_POLICIES = {"public_home", "public_context", "public_selective"}
ELIGIBLE_RELATIONSHIP_POLICIES = PUBLIC_POLICIES
RELATIONSHIP_LIVE_ROUTES = {"normal_chat", "tagged", "active_batch"}
DISALLOWED_RELATIONSHIP_ROUTES = {"relay", "website_relay_event", "ambient", "operator_command", "direct_payload_task", "show_status"}
SIMPLE_GREETING_RE = re.compile(r"^\s*(hi|hello|hey|yo|sup|gm|good morning)[!. ]*\s*$", re.I)


def flag_enabled(name: str, environ: Mapping[str, str] | None = None) -> bool:
    source = os.environ if environ is None else environ
    return str(source.get(name, "")).strip().lower() in {"1", "true", "yes", "on", "enabled"}
def shadow_enabled(environ: Mapping[str, str] | None = None) -> bool: return flag_enabled(SHADOW_ENV, environ)
def live_enabled(environ: Mapping[str, str] | None = None) -> bool: return flag_enabled(LIVE_ENV, environ)
def active_engagement_live_enabled(environ: Mapping[str, str] | None = None) -> bool: return flag_enabled(ACTIVE_ENGAGEMENT_LIVE_ENV, environ)
def _now() -> str: return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
def _canon(v: Any) -> str: return re.sub(r"\s+", " ", str(v or "").strip().lower())
def _clamp(x: float) -> float: return max(-1.0, min(1.0, round(float(x or 0.0), 6)))
def _hash(*parts: Any) -> str: return hashlib.sha256("\x1f".join(_canon(p) for p in parts).encode()).hexdigest()[:40]
def parse_utc(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
    text = str(value or "").strip()
    if not text: return datetime.now(timezone.utc).replace(microsecond=0)
    if text.endswith("Z"): text = text[:-1] + "+00:00"
    for fmt in (None, "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f"):
        try:
            dt = datetime.fromisoformat(text) if fmt is None else datetime.strptime(text, fmt)
            return dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return datetime(1970, 1, 1, tzinfo=timezone.utc)
def stable_event_id(*, guild_id: int, subject_user_id: int, source_table: str, source_row_id: int | str, event_type: str, observed_at: str = "") -> str:
    return "relv2_" + _hash(SCHEMA_VERSION, guild_id, subject_user_id, source_table, source_row_id, event_type, observed_at)


def ensure_relationship_v2_schema(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS relationship_events_v2 (
        event_id TEXT PRIMARY KEY, schema_version TEXT NOT NULL, guild_id INTEGER NOT NULL, subject_user_id INTEGER NOT NULL,
        subject_key TEXT NOT NULL, actor_role TEXT NOT NULL, event_type TEXT NOT NULL, direction TEXT NOT NULL,
        normalized_summary TEXT DEFAULT '', source_table TEXT NOT NULL, source_row_id TEXT NOT NULL, source_message_id INTEGER,
        route_mode TEXT DEFAULT 'unknown', channel_id INTEGER DEFAULT 0, channel_name TEXT DEFAULT '', channel_policy TEXT DEFAULT 'unknown',
        visibility TEXT DEFAULT 'private', authority TEXT DEFAULT 'derived_relationship', confidence REAL DEFAULT 0, salience REAL DEFAULT 0,
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
        candidate_type TEXT NOT NULL, policy_eligible INTEGER DEFAULT 0, would_select INTEGER DEFAULT 0, live_emission_allowed INTEGER DEFAULT 0, actual_emitted INTEGER DEFAULT 0,
        withheld_reason_codes TEXT DEFAULT '[]', route_mode TEXT DEFAULT 'unknown', channel_policy TEXT DEFAULT 'unknown', visibility_decision TEXT DEFAULT 'unknown',
        cooldown_decision TEXT DEFAULT 'unknown', simulated_cooldown_decision TEXT DEFAULT 'unknown', source_class_counts_json TEXT DEFAULT '{}',
        relevant_open_loop_count INTEGER DEFAULT 0, relevant_moment_count INTEGER DEFAULT 0, legacy_hash TEXT DEFAULT '', v2_hash TEXT DEFAULT '', processing_errors_json TEXT DEFAULT '[]', created_at TEXT NOT NULL)""")
    # v2.1 migrations for earlier shadow table.
    for sql in (
        "ALTER TABLE relationship_engagement_shadow_runs ADD COLUMN policy_eligible INTEGER DEFAULT 0",
        "ALTER TABLE relationship_engagement_shadow_runs ADD COLUMN would_select INTEGER DEFAULT 0",
        "ALTER TABLE relationship_engagement_shadow_runs ADD COLUMN live_emission_allowed INTEGER DEFAULT 0",
        "ALTER TABLE relationship_engagement_shadow_runs ADD COLUMN actual_emitted INTEGER DEFAULT 0",
        "ALTER TABLE relationship_engagement_shadow_runs ADD COLUMN simulated_cooldown_decision TEXT DEFAULT 'unknown'",
    ):
        try: cur.execute(sql)
        except sqlite3.OperationalError: pass
    cur.execute("""CREATE TABLE IF NOT EXISTS relationship_member_settings_v2 (
        guild_id INTEGER NOT NULL, subject_user_id INTEGER NOT NULL, subject_key TEXT NOT NULL, proactive_enabled INTEGER DEFAULT 1,
        playful_rivalry_enabled INTEGER DEFAULT 1, updated_at TEXT NOT NULL, PRIMARY KEY(guild_id, subject_user_id))""")
    cur.execute("""CREATE TABLE IF NOT EXISTS relationship_member_preferences_v2 (
        preference_id TEXT PRIMARY KEY, guild_id INTEGER NOT NULL, subject_user_id INTEGER NOT NULL, subject_key TEXT NOT NULL,
        preference_key TEXT NOT NULL, preference_value TEXT NOT NULL, source_event_id TEXT DEFAULT '', lifecycle TEXT DEFAULT 'active', created_at TEXT NOT NULL)""")
    cur.execute("""CREATE TABLE IF NOT EXISTS relationship_event_ledger_links_v2 (
        event_id TEXT NOT NULL, guild_id INTEGER NOT NULL, subject_user_id INTEGER NOT NULL, ledger_entry_id TEXT NOT NULL,
        created_at TEXT NOT NULL, PRIMARY KEY(event_id, ledger_entry_id))""")
    cur.execute("""CREATE TABLE IF NOT EXISTS relationship_observation_diagnostics_v2 (
        diagnostic_id TEXT PRIMARY KEY, schema_version TEXT NOT NULL, guild_id INTEGER NOT NULL, subject_user_id INTEGER NOT NULL,
        subject_key TEXT NOT NULL, actor_role TEXT NOT NULL, rejection_reason TEXT NOT NULL, route_mode TEXT DEFAULT 'unknown',
        channel_policy TEXT DEFAULT 'unknown', source_table TEXT DEFAULT 'unknown', source_row_id TEXT DEFAULT '', observed_at TEXT NOT NULL, created_at TEXT NOT NULL)""")
    cur.execute("""CREATE TABLE IF NOT EXISTS relationship_event_moment_links_v2 (
        event_id TEXT NOT NULL, moment_id TEXT NOT NULL, guild_id INTEGER NOT NULL, subject_user_id INTEGER NOT NULL, lifecycle TEXT DEFAULT 'active',
        created_at TEXT NOT NULL, updated_at TEXT NOT NULL, PRIMARY KEY(event_id, moment_id))""")
    for sql in (
        "CREATE INDEX IF NOT EXISTS idx_relv2_events_subject ON relationship_events_v2(guild_id, subject_user_id, lifecycle, observed_at)",
        "CREATE INDEX IF NOT EXISTS idx_relv2_events_source ON relationship_events_v2(guild_id, source_table, source_row_id)",
        "CREATE INDEX IF NOT EXISTS idx_relv2_shadow_subject ON relationship_engagement_shadow_runs(guild_id, subject_user_id, created_at)",
        "CREATE INDEX IF NOT EXISTS idx_relv2_links_subject ON relationship_event_ledger_links_v2(guild_id, subject_user_id)",
        "CREATE INDEX IF NOT EXISTS idx_relv2_diag_subject ON relationship_observation_diagnostics_v2(guild_id, subject_user_id, rejection_reason)",
        "CREATE INDEX IF NOT EXISTS idx_relv2_moment_subject ON relationship_event_moment_links_v2(guild_id, subject_user_id, lifecycle)",
    ): cur.execute(sql)

@dataclass(frozen=True)
class RelationshipEventV2:
    guild_id: int; subject_user_id: int; actor_role: str; event_type: str; direction: str; source_table: str; source_row_id: int | str
    normalized_summary: str = ""; source_message_id: int | None = None; route_mode: str = "unknown"; channel_id: int = 0; channel_name: str = ""; channel_policy: str = "unknown"
    visibility: str = "private"; authority: str = "derived_relationship"; confidence: float = .5; salience: float = .1; moment_id: str = ""; observed_at: str = ""; lifecycle: str = "active"
    correction_of_event_id: str = ""; supersedes_event_id: str = ""
    @property
    def subject_key(self) -> str: return subject_key_for_user(self.subject_user_id)
    @property
    def event_id(self) -> str: return stable_event_id(guild_id=self.guild_id, subject_user_id=self.subject_user_id, source_table=self.source_table, source_row_id=self.source_row_id, event_type=self.event_type, observed_at=self.observed_at)


def classify_message(text: str, *, actor_role: str, directed: bool, channel_policy: str, route_mode: str) -> tuple[str, str, float, float]:
    t = _canon(text)
    if actor_role != "user":
        return "model_audit", "model output recorded for audit with zero positive relationship weight", .3, 0.0
    if not directed or channel_policy not in ELIGIBLE_RELATIONSHIP_POLICIES or route_mode in DISALLOWED_RELATIONSHIP_ROUTES:
        return "unclassified", "passive or route-ineligible message; no relationship evidence", .0, 0.0
    pats = [
        ("explicit_engagement_opt_out", r"\b(don'?t follow up|stop recognizing me|no proactive|don'?t proactively|opt out|don'?t ping me)\b"),
        ("explicit_engagement_opt_in", r"\b(re-enable proactive|enable proactive|you can follow up again|opt me back in)\b"),
        ("explicit_relationship_mode_preference", r"\b(opt in|i want|enable|prefer)\b.{0,40}\b(friendly rival|rival mode|playful rivalry|nemesis)\b"),
        ("boundary", r"\b(stop|don'?t|do not)\b.{0,50}\b(joke|tease|call me|bring that up|follow up|recognize me|rival)\b"),
        ("boundary_respected", r"\b(you can|it is ok to|okay to|allowed to)\b.{0,50}\b(joke|tease|rival|follow up|recognize me)\b"),
        ("repair_accepted", r"\b(we'?re good|we are good|all good|apology accepted|thanks for fixing|that helps)\b"),
        ("apology", r"\b(i'?m sorry|my bad|apologize)\b"), ("repair_attempt", r"\b(let'?s fix|let us fix|can we reset|repair|make it right)\b"),
        ("appreciation", r"\b(thanks|thank you|appreciate|grateful)\b"), ("constructive_collaboration", r"\b(let'?s|we should|can we|help me build|work together|collaborate)\b"),
        ("support_request", r"\b(help|stuck|issue|problem|can you assist|need support)\b"), ("support_received_accepted", r"\b(that helped|helpful|worked|you fixed it)\b"),
        ("playful_exchange", r"\b(lol|haha|jk|just kidding|friendly rival|nemesis)\b"), ("correction", r"\b(actually|correction|that'?s wrong|that is wrong|not what i meant)\b"),
        ("disagreement", r"\b(i disagree|disagree|no,|nah,)\b"), ("friction", r"\b(annoying|bad bot|shut up|idiot|hate this)\b"),
        ("open_loop", r"\b(remind me|later|follow up|circle back)\b"), ("follow_up", r"\b(about earlier|from before|following up|circling back)\b"),
        ("return_recognition", r"\b(i'?m back|back again|remember me)\b"), ("acknowledgement", r"\b(ok|okay|got it|yep|yes)\b"),
    ]
    for et, pat in pats:
        if re.search(pat, t): return et, f"typed relationship signal: {et}", .75, .25
    return "unclassified", "ambiguous message left unclassified", .0, 0.0


def _latest_pref(conn: sqlite3.Connection, *, guild_id: int, user_id: int, key: str) -> str:
    row = conn.execute("SELECT preference_value FROM relationship_member_preferences_v2 WHERE guild_id=? AND subject_user_id=? AND preference_key=? AND lifecycle='active' ORDER BY created_at DESC LIMIT 1", (guild_id, user_id, key)).fetchone()
    return str(row[0]) if row else ""

def _set_pref(conn: sqlite3.Connection, *, guild_id: int, user_id: int, key: str, value: str, source_event_id: str = "") -> None:
    ensure_relationship_v2_schema(conn); now = _now(); sk = subject_key_for_user(user_id)
    conn.execute("UPDATE relationship_member_preferences_v2 SET lifecycle='superseded' WHERE guild_id=? AND subject_user_id=? AND preference_key=? AND lifecycle='active'", (guild_id, user_id, key))
    pid = "relpref_" + _hash(guild_id, user_id, key, value, source_event_id, now)
    conn.execute("INSERT INTO relationship_member_preferences_v2 VALUES (?,?,?,?,?,?,?,?,?)", (pid, guild_id, user_id, sk, key, value, source_event_id, "active", now))

def record_event(conn: sqlite3.Connection, event: RelationshipEventV2) -> str:
    ensure_relationship_v2_schema(conn); now = _now(); et = event.event_type if event.event_type in EVENT_TYPES else "unclassified"; observed = parse_utc(event.observed_at or now).isoformat()
    conn.execute("""INSERT OR IGNORE INTO relationship_events_v2 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
        event.event_id, SCHEMA_VERSION, event.guild_id, event.subject_user_id, event.subject_key, event.actor_role, et, event.direction,
        event.normalized_summary[:240], event.source_table, str(event.source_row_id), event.source_message_id, event.route_mode, int(event.channel_id or 0),
        event.channel_name[:80], event.channel_policy[:80], event.visibility or "private", event.authority, float(event.confidence or 0), float(event.salience or 0),
        event.moment_id, observed, event.lifecycle, event.correction_of_event_id, event.supersedes_event_id, now, now))
    if et == "explicit_engagement_opt_out": _set_pref(conn, guild_id=event.guild_id, user_id=event.subject_user_id, key="proactive", value="disabled", source_event_id=event.event_id)
    if et == "explicit_engagement_opt_in": _set_pref(conn, guild_id=event.guild_id, user_id=event.subject_user_id, key="proactive", value="enabled", source_event_id=event.event_id)
    if et == "explicit_relationship_mode_preference": _set_pref(conn, guild_id=event.guild_id, user_id=event.subject_user_id, key="playful_rivalry_opt_in", value="enabled", source_event_id=event.event_id)
    if event.actor_role == "user" and event.lifecycle == "active" and et not in {"unclassified"}: project_event_to_ledger(conn, event)
    return event.event_id

def record_observation_diagnostic(conn: sqlite3.Connection, *, guild_id: int, user_id: int, role: str, reason: str, source_row_id: int | str, route_mode: str, channel_policy: str, observed_at: str = "", source_table: str = "conversations") -> str:
    ensure_relationship_v2_schema(conn); obs = parse_utc(observed_at or _now()).isoformat(); did = "reldiag_" + _hash(guild_id, user_id, role, reason, source_table, source_row_id, obs)
    conn.execute("INSERT OR IGNORE INTO relationship_observation_diagnostics_v2 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", (did, SCHEMA_VERSION, guild_id, user_id, subject_key_for_user(user_id), role, reason, route_mode, channel_policy, source_table, str(source_row_id), obs, _now()))
    return did

def observe_message(conn: sqlite3.Connection, *, guild_id: int, user_id: int, role: str, content: str, source_row_id: int | str, user_name: str = "", channel_policy: str = "unknown", channel_name: str = "", channel_id: int = 0, message_id: int | None = None, route_mode: str = "unknown", directed: bool = False, observed_at: str = "") -> str:
    et, summary, conf, sal = classify_message(content, actor_role=role, directed=directed, channel_policy=channel_policy, route_mode=route_mode)
    if et == "unclassified" and role == "user":
        reason = "sealed_test" if channel_policy == "sealed_test" else ("passive" if not directed else "policy_or_route_or_ambiguous")
        record_observation_diagnostic(conn, guild_id=guild_id, user_id=user_id, role=role, reason=reason, source_row_id=source_row_id, route_mode=route_mode, channel_policy=channel_policy, observed_at=observed_at)
        return ""
    lifecycle = "review_only" if et == "model_audit" else "active"
    ev = RelationshipEventV2(guild_id, user_id, role, et, "user_to_bnl" if role == "user" else "bnl_to_user", "conversations", source_row_id, summary, message_id, route_mode, channel_id, channel_name, channel_policy, "private", "derived_relationship", conf, sal, observed_at=parse_utc(observed_at or _now()).isoformat(), lifecycle=lifecycle)
    eid = record_event(conn, ev); rebuild_state(conn, guild_id=guild_id, subject_user_id=user_id, evaluated_at=observed_at or _now()); return eid

def record_model_playful_rivalry_acceptance(conn: sqlite3.Connection, *, guild_id: int, user_id: int, source_row_id: int | str, route_mode: str = "normal_chat", channel_policy: str = "public_home", observed_at: str = "") -> str:
    ev = RelationshipEventV2(guild_id, user_id, "model", "model_playful_rivalry_acceptance", "bnl_to_user", "controlled_relationship_policy", source_row_id, "controlled policy acceptance provenance", route_mode=route_mode, channel_policy=channel_policy, visibility="private", authority="controlled_policy", confidence=.8, salience=0, observed_at=parse_utc(observed_at or _now()).isoformat(), lifecycle="active")
    return record_event(conn, ev)

def project_event_to_ledger(conn: sqlite3.Connection, event: RelationshipEventV2) -> str:
    # Relationship interpretations are derived personal state: never public usable, never live recall facts.
    entry = LedgerEntry(guild_id=event.guild_id, source_table="relationship_events_v2", source_row_id=event.event_id, source_revision=SCHEMA_VERSION, source_event_key=event.event_id, source_role=event.actor_role, entry_type="relationship_event", subject_key=event.subject_key, predicate_key=event.event_type, value=event.normalized_summary[:500], source_class=SourceClass.DERIVED_SUMMARY, route_mode=event.route_mode, channel_id=event.channel_id, channel_name=event.channel_name, channel_policy=event.channel_policy, source_message_id=event.source_message_id, visibility=Visibility.PRIVATE, confidence=Confidence.LOW, public_usable=False, derived=True, projection=True, salience=event.salience, observed_at=parse_utc(event.observed_at or _now()).isoformat(), lifecycle_status="review_only", participants=(LedgerParticipant(event.subject_key, "", "subject", 0),))
    result = insert_ledger_entry(conn, entry)
    if result.entry_id:
        conn.execute("INSERT OR IGNORE INTO relationship_event_ledger_links_v2 VALUES (?,?,?,?,?)", (event.event_id, event.guild_id, event.subject_user_id, result.entry_id, _now()))
    return result.entry_id


def _decay(dim: str, score: float, days: float) -> float:
    if dim == "boundary_alignment": return score
    if dim == "friction": return score * (0.5 ** (days / 14.0))
    if dim == "playfulness": return score * (0.5 ** (days / 90.0))
    return score * (0.5 ** (days / 730.0))

def get_member_settings(conn: sqlite3.Connection, *, guild_id: int, user_id: int) -> dict[str, bool]:
    ensure_relationship_v2_schema(conn); row = conn.execute("SELECT proactive_enabled,playful_rivalry_enabled FROM relationship_member_settings_v2 WHERE guild_id=? AND subject_user_id=?", (guild_id,user_id)).fetchone()
    return {"proactive_enabled": bool(row[0]) if row else True, "playful_rivalry_enabled": bool(row[1]) if row else True}

def rebuild_state(conn: sqlite3.Connection, *, guild_id: int, subject_user_id: int, evaluated_at: str = "") -> dict[str, Any]:
    ensure_relationship_v2_schema(conn); eval_dt = parse_utc(evaluated_at or _now())
    settings = get_member_settings(conn, guild_id=guild_id, user_id=subject_user_id)
    scores = {d: 0.0 for d in DIMENSIONS}; counts: dict[str, int] = {}; excluded: dict[str, int] = {}; last_user = last_direct = last_repair = ""
    opt_out = _latest_pref(conn, guild_id=guild_id, user_id=subject_user_id, key="proactive") == "disabled"
    rows = conn.execute("SELECT event_type,actor_role,observed_at,lifecycle,direction FROM relationship_events_v2 WHERE guild_id=? AND subject_user_id=? ORDER BY observed_at,event_id", (guild_id, subject_user_id)).fetchall()
    model_accept = 0
    for et, role, obs, lifecycle, direction in rows:
        if lifecycle not in ACTIVE_LIFECYCLES:
            excluded[lifecycle or "unknown"] = excluded.get(lifecycle or "unknown", 0) + 1; continue
        counts[et] = counts.get(et, 0) + 1
        if et == "model_playful_rivalry_acceptance": model_accept += 1
        if role == "user":
            last_user = max(last_user, parse_utc(obs).isoformat())
            if direction == "user_to_bnl": last_direct = max(last_direct, parse_utc(obs).isoformat())
        if et in {"apology", "repair_attempt", "repair_accepted", "boundary_respected"}: last_repair = max(last_repair, parse_utc(obs).isoformat())
        days = max(0.0, (eval_dt - parse_utc(obs)).total_seconds()/86400.0)
        for dim, w in WEIGHTS.get(et, {}).items():
            if role != "user" and dim in POSITIVE_MODEL_BLOCKED and w > 0: continue
            scores[dim] = _clamp(scores[dim] + _decay(dim, w, days))
    if counts.get("boundary", 0) and not counts.get("boundary_respected", 0): scores["boundary_alignment"] = min(scores["boundary_alignment"], -0.15)
    rivalry = "neutral"
    active_boundary = counts.get("boundary", 0) > counts.get("boundary_respected", 0)
    explicit_opt_in = _latest_pref(conn, guild_id=guild_id, user_id=subject_user_id, key="playful_rivalry_opt_in") == "enabled"
    if opt_out or active_boundary or not settings["playful_rivalry_enabled"]: rivalry = "neutral"
    elif explicit_opt_in and model_accept >= 1 and counts.get("playful_exchange", 0) >= 2 and scores["playfulness"] > .12 and scores["friction"] < .12: rivalry = "mutual_rivalry"
    elif scores["friction"] >= .35: rivalry = "strained"
    elif scores["repair"] > .1 and scores["friction"] > .05: rivalry = "repairing"
    elif scores["playfulness"] > .12: rivalry = "playful"
    elif scores["rapport"] > .15: rivalry = "friendly"
    stage = "new" if scores["familiarity"] < .1 else "known" if scores["trust"] < .2 else "trusted"
    now = _now(); conn.execute("""INSERT OR REPLACE INTO relationship_state_v2 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (guild_id, subject_user_id, subject_key_for_user(subject_user_id), *[_clamp(scores[d]) for d in DIMENSIONS], json.dumps(counts, sort_keys=True), last_user, last_direct, last_repair, stage, rivalry, 1 if opt_out else 0, eval_dt.isoformat(), now, SCHEMA_VERSION))
    return {**{d: _clamp(scores[d]) for d in DIMENSIONS}, "evidence_counts": counts, "excluded_counts": excluded, "relationship_stage": stage, "rivalry_state": rivalry, "engagement_opt_out": opt_out}

def set_member_setting(conn: sqlite3.Connection, *, guild_id: int, user_id: int, proactive_enabled: bool | None = None, playful_rivalry_enabled: bool | None = None) -> dict[str, Any]:
    ensure_relationship_v2_schema(conn); now=_now(); sk=subject_key_for_user(user_id)
    conn.execute("INSERT OR IGNORE INTO relationship_member_settings_v2 VALUES (?,?,?,?,?,?)", (guild_id,user_id,sk,1,1,now))
    if proactive_enabled is not None:
        conn.execute("UPDATE relationship_member_settings_v2 SET proactive_enabled=?, updated_at=? WHERE guild_id=? AND subject_user_id=?", (1 if proactive_enabled else 0, now, guild_id, user_id))
        _set_pref(conn, guild_id=guild_id, user_id=user_id, key="proactive", value="enabled" if proactive_enabled else "disabled", source_event_id="member_setting")
    if playful_rivalry_enabled is not None:
        conn.execute("UPDATE relationship_member_settings_v2 SET playful_rivalry_enabled=?, updated_at=? WHERE guild_id=? AND subject_user_id=?", (1 if playful_rivalry_enabled else 0, now, guild_id, user_id))
        if not playful_rivalry_enabled: _set_pref(conn, guild_id=guild_id, user_id=user_id, key="playful_rivalry_opt_in", value="disabled", source_event_id="member_setting")
    row = conn.execute("SELECT proactive_enabled,playful_rivalry_enabled FROM relationship_member_settings_v2 WHERE guild_id=? AND subject_user_id=?", (guild_id,user_id)).fetchone() or (1,1)
    return {"proactive_enabled": bool(row[0]), "playful_rivalry_enabled": bool(row[1]), "proactive_preference": _latest_pref(conn,guild_id=guild_id,user_id=user_id,key="proactive") or "enabled"}

def settings_summary(conn: sqlite3.Connection, *, guild_id: int, user_id: int) -> str:
    s = get_member_settings(conn, guild_id=guild_id, user_id=user_id)
    pref = _latest_pref(conn, guild_id=guild_id, user_id=user_id, key="proactive") or ("enabled" if s["proactive_enabled"] else "disabled")
    return f"Relationship v2 settings: proactive={'enabled' if pref == 'enabled' and s['proactive_enabled'] else 'disabled'}; playful_rivalry={'enabled' if s['playful_rivalry_enabled'] else 'disabled'}."

def governed_summary(conn: sqlite3.Connection, *, guild_id: int, user_id: int, route_mode: str, channel_policy: str, simple_greeting: bool = False, direct: bool = True, target_user_id: int | None = None, governance_allowed: bool = True) -> str:
    if not live_enabled() or not governance_allowed or simple_greeting or not direct: return ""
    if target_user_id is not None and int(target_user_id or 0) != int(user_id or 0): return ""
    if channel_policy not in PUBLIC_POLICIES or route_mode not in RELATIONSHIP_LIVE_ROUTES: return ""
    ensure_relationship_v2_schema(conn); settings = get_member_settings(conn, guild_id=guild_id, user_id=user_id)
    row=conn.execute("SELECT rapport,trust,familiarity,friction,support,repair,relationship_stage,rivalry_state,engagement_opt_out FROM relationship_state_v2 WHERE guild_id=? AND subject_user_id=?", (guild_id,user_id)).fetchone()
    if not row: return ""
    rapport, trust, fam, fric, support, repair, stage, rivalry, opt = row
    tone = "familiar" if fam >= .15 else "lightly familiar" if fam > .05 else "new/low-history"
    warmth = "warm" if rapport >= .15 else "neutral-warm" if rapport > .03 else "neutral"
    safe = [f"Private relationship calibration for current member only: use a {warmth}, {tone} tone.", "Do not mention internal relationship state, labels, scores, or evidence."]
    if opt or not settings["proactive_enabled"]: safe.append("Do not proactively recognize or follow up with this member.")
    if rivalry == "mutual_rivalry" and settings["playful_rivalry_enabled"] and not opt: safe.append("Playful rivalry is allowed only if the member continues it in the current turn.")
    if fric > .05 or repair > .05: safe.append("Prefer careful repair-aware wording; do not relitigate the old friction.")
    return " ".join(safe)[:500]


def _open_loop_count(conn: sqlite3.Connection, *, guild_id: int, user_id: int, channel_policy: str = "unknown") -> int:
    sk = subject_key_for_user(user_id)
    try:
        public = str(channel_policy or "").startswith("public_")
        if public:
            return int(conn.execute("""SELECT COUNT(*) FROM memory_ledger_entries WHERE guild_id=? AND subject_key=? AND entry_type IN ('open_loop','commitment','unresolved_question') AND lifecycle_status='active' AND derived=0 AND projection=0 AND visibility IN ('public','public_safe') AND public_usable=1""", (guild_id, sk)).fetchone()[0])
        return int(conn.execute("""SELECT COUNT(*) FROM memory_ledger_entries WHERE guild_id=? AND subject_key=? AND entry_type IN ('open_loop','commitment','unresolved_question') AND lifecycle_status='active' AND derived=0 AND projection=0""", (guild_id, sk)).fetchone()[0])
    except sqlite3.OperationalError:
        return 0

def _compatible_moment_count(conn: sqlite3.Connection, *, guild_id: int, user_id: int, channel_policy: str) -> int:
    sk = subject_key_for_user(user_id)
    try:
        return int(conn.execute("""SELECT COUNT(DISTINCT w.moment_id) FROM memory_moment_windows w JOIN memory_moment_participants p ON p.moment_id=w.moment_id WHERE w.guild_id=? AND p.participant_key=? AND w.lifecycle_status='finalized' AND w.visibility IN ('public','public_safe','private') AND (? NOT LIKE 'public_%' OR w.visibility IN ('public','public_safe'))""", (guild_id, sk, channel_policy)).fetchone()[0])
    except sqlite3.OperationalError:
        return 0

def plan_engagement(conn: sqlite3.Connection, *, guild_id: int, user_id: int, candidate_type: str, route_mode: str, channel_policy: str, current_direct: bool, now: str = "", source_visibility: str = "public", send_authorized: bool = False) -> dict[str, Any]:
    ensure_relationship_v2_schema(conn); now_dt=parse_utc(now or _now()); settings=get_member_settings(conn,guild_id=guild_id,user_id=user_id); withheld=[]; errors=[]
    state=rebuild_state(conn,guild_id=guild_id,subject_user_id=user_id,evaluated_at=now_dt.isoformat())
    open_loops = _open_loop_count(conn, guild_id=guild_id, user_id=user_id, channel_policy=channel_policy); moments = _compatible_moment_count(conn, guild_id=guild_id, user_id=user_id, channel_policy=channel_policy)
    if not settings["proactive_enabled"] or state.get("engagement_opt_out"): withheld.append("member_opt_out")
    if not settings["playful_rivalry_enabled"] and candidate_type == "playful_rivalry": withheld.append("playful_rivalry_disabled")
    if channel_policy not in PUBLIC_POLICIES: withheld.append("channel_policy_ineligible")
    if route_mode not in RELATIONSHIP_LIVE_ROUTES: withheld.append("route_ineligible")
    if source_visibility not in {"public", "private"}: withheld.append("source_visibility_ineligible")
    if candidate_type in {"recognition","repair_acknowledgement"} and not current_direct: withheld.append("not_currently_direct")
    if candidate_type == "open_loop_follow_up" and open_loops <= 0: withheld.append("no_governed_open_loop")
    if candidate_type == "dormant_signal_echo": withheld.append("dormant_echo_off_by_default")
    allowed_types={"recognition","open_loop_follow_up","repair_acknowledgement","support_follow_up","curiosity_question","dormant_signal_echo"}
    if candidate_type not in allowed_types: withheld.append("unknown_candidate")
    window_start = (now_dt.timestamp() - 86400.0)
    emitted_rows = conn.execute("SELECT created_at FROM relationship_engagement_shadow_runs WHERE guild_id=? AND subject_user_id=? AND candidate_type=? AND actual_emitted=1", (guild_id,user_id,candidate_type)).fetchall()
    recent_block = any(window_start <= parse_utc(created).timestamp() <= now_dt.timestamp() for (created,) in emitted_rows)
    if recent_block: withheld.append("per_user_live_cooldown")
    sim_recent = int(conn.execute("SELECT COUNT(*) FROM relationship_engagement_shadow_runs WHERE guild_id=? AND subject_user_id=? AND candidate_type=? AND actual_emitted=0 AND created_at<=? AND created_at>=?", (guild_id,user_id,candidate_type,now_dt.isoformat(), (now_dt.replace(hour=0, minute=0, second=0, microsecond=0)).isoformat())).fetchone()[0])
    policy_eligible = not withheld
    would_select = policy_eligible
    live_allowed = would_select and live_enabled() and active_engagement_live_enabled() and bool(send_authorized)
    actual_emitted = False
    run_id="relsr_"+_hash(guild_id,user_id,candidate_type,route_mode,channel_policy,now_dt.isoformat(),withheld,send_authorized)
    conn.execute("""INSERT OR IGNORE INTO relationship_engagement_shadow_runs (run_id,schema_version,guild_id,subject_user_id,subject_key,candidate_type,policy_eligible,would_select,live_emission_allowed,actual_emitted,withheld_reason_codes,route_mode,channel_policy,visibility_decision,cooldown_decision,simulated_cooldown_decision,source_class_counts_json,relevant_open_loop_count,relevant_moment_count,legacy_hash,v2_hash,processing_errors_json,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (run_id,SCHEMA_VERSION,guild_id,user_id,subject_key_for_user(user_id),candidate_type,1 if policy_eligible else 0,1 if would_select else 0,1 if live_allowed else 0,1 if actual_emitted else 0,json.dumps(sorted(set(withheld))),route_mode,channel_policy,source_visibility,"blocked" if recent_block else "clear","blocked" if sim_recent else "clear",json.dumps({"relationship_events_v2": sum(state.get("evidence_counts", {}).values())}),open_loops,moments,"not_collected",_hash(state),json.dumps(errors),now_dt.isoformat()))
    text="" if candidate_type != "dormant_signal_echo" else "An older remembered signal exists only as past context."
    return {"policy_eligible": policy_eligible, "would_select": would_select, "live_emission_allowed": live_allowed, "actual_emitted": actual_emitted, "withheld_reason_codes": sorted(set(withheld)), "run_id": run_id, "candidate_text": text, "relevant_open_loop_count": open_loops, "relevant_moment_count": moments}


def record_engagement_emission(conn: sqlite3.Connection, *, run_id: str, emitted_at: str = "") -> int:
    ensure_relationship_v2_schema(conn)
    return conn.execute("UPDATE relationship_engagement_shadow_runs SET actual_emitted=1, live_emission_allowed=1, created_at=? WHERE run_id=? AND live_emission_allowed=1", (parse_utc(emitted_at or _now()).isoformat(), run_id)).rowcount

def refresh_moment_links(conn: sqlite3.Connection, *, guild_id: int | None = None) -> int:
    ensure_relationship_v2_schema(conn); now=_now(); params=[]; where=""
    if guild_id is not None:
        where="AND e.guild_id=?"; params.append(guild_id)
    if guild_id is None:
        conn.execute("UPDATE relationship_event_moment_links_v2 SET lifecycle='retracted', updated_at=?", (now,))
    else:
        conn.execute("UPDATE relationship_event_moment_links_v2 SET lifecycle='retracted', updated_at=? WHERE guild_id=?", (now, guild_id))
    try:
        rows = conn.execute(f"""SELECT e.event_id,w.moment_id,e.guild_id,e.subject_user_id FROM relationship_events_v2 e
            JOIN memory_ledger_entries src ON src.guild_id=e.guild_id AND src.source_table=e.source_table AND src.source_row_id=CAST(e.source_row_id AS TEXT) AND src.source_role=e.actor_role
            JOIN memory_moment_members mm ON mm.ledger_entry_id=src.entry_id
            JOIN memory_moment_windows w ON w.moment_id=mm.moment_id AND w.guild_id=e.guild_id
            JOIN memory_moment_participants p ON p.moment_id=w.moment_id AND p.participant_key=e.subject_key
            WHERE e.lifecycle='active' AND e.actor_role='user' AND w.lifecycle_status='finalized'
              AND src.lifecycle_status IN ('active','review_only')
              AND (e.channel_policy NOT LIKE 'public_%' OR w.visibility IN ('public','public_safe')) {where}
        """, tuple(params)).fetchall()
    except sqlite3.OperationalError:
        return 0
    count=0
    for event_id, moment_id, gid, uid in rows:
        count += conn.execute("INSERT OR REPLACE INTO relationship_event_moment_links_v2 VALUES (?,?,?,?,?,?,?)", (event_id, moment_id, gid, uid, "active", now, now)).rowcount
    return count

def propagate_ledger_lifecycle(conn: sqlite3.Connection, *, guild_id: int, ledger_entry_id: str, lifecycle: str) -> int:
    ensure_relationship_v2_schema(conn); now=_now(); rows=conn.execute("SELECT event_id,subject_user_id FROM relationship_event_ledger_links_v2 WHERE guild_id=? AND ledger_entry_id=?", (guild_id, ledger_entry_id)).fetchall(); count=0
    for eid, uid in rows:
        count += conn.execute("UPDATE relationship_events_v2 SET lifecycle=?, updated_at=? WHERE guild_id=? AND event_id=?", (lifecycle, now, guild_id, eid)).rowcount
        conn.execute("UPDATE relationship_event_moment_links_v2 SET lifecycle='retracted', updated_at=? WHERE guild_id=? AND event_id=?", (now, guild_id, eid))
        rebuild_state(conn, guild_id=guild_id, subject_user_id=int(uid))
    return count

def build_evaluation_report(conn: sqlite3.Connection, *, guild_id: int | None = None, prepare_schema: bool = True) -> dict[str, Any]:
    if prepare_schema:
        ensure_relationship_v2_schema(conn)
    where="WHERE guild_id=?" if guild_id is not None else ""; p=([guild_id] if guild_id is not None else [])
    def one(sql, pp=p): return conn.execute(sql, pp).fetchone()[0]
    by_type={r[0]:r[1] for r in conn.execute(f"SELECT event_type,COUNT(*) FROM relationship_events_v2 {where} GROUP BY event_type", p).fetchall()}
    lifecycle={r[0]:r[1] for r in conn.execute(f"SELECT lifecycle,COUNT(*) FROM relationship_events_v2 {where} GROUP BY lifecycle", p).fetchall()}
    scoped = "guild_id=? AND " if guild_id is not None else ""
    if _table_exists(conn, "memory_ledger_entries"):
        ledger_link_integrity_violations = one(
            "SELECT COUNT(*) FROM relationship_event_ledger_links_v2 l "
            "LEFT JOIN relationship_events_v2 e ON e.event_id=l.event_id "
            "LEFT JOIN memory_ledger_entries m ON m.entry_id=l.ledger_entry_id "
            f"WHERE {('l.guild_id=? AND ' if guild_id is not None else '')}"
            "(e.event_id IS NULL OR e.guild_id<>l.guild_id "
            "OR e.subject_user_id<>l.subject_user_id "
            "OR m.entry_id IS NULL OR m.guild_id<>l.guild_id "
            "OR m.subject_key<>('discord_user:' || CAST(l.subject_user_id AS TEXT)))"
        )
    else:
        # A persisted link cannot be verified when its target table is absent.
        ledger_link_integrity_violations = one(
            f"SELECT COUNT(*) FROM relationship_event_ledger_links_v2 {where}"
        )
    if _table_exists(conn, "memory_moment_windows") and _table_exists(
        conn, "memory_moment_participants"
    ):
        moment_link_integrity_violations = one(
            "SELECT COUNT(*) FROM relationship_event_moment_links_v2 l "
            "LEFT JOIN relationship_events_v2 e ON e.event_id=l.event_id "
            "LEFT JOIN memory_moment_windows w ON w.moment_id=l.moment_id "
            f"WHERE {('l.guild_id=? AND ' if guild_id is not None else '')}"
            "(e.event_id IS NULL OR e.guild_id<>l.guild_id "
            "OR e.subject_user_id<>l.subject_user_id "
            "OR w.moment_id IS NULL OR w.guild_id<>l.guild_id "
            "OR NOT EXISTS ("
            "SELECT 1 FROM memory_moment_participants p "
            "WHERE p.moment_id=l.moment_id "
            "AND p.participant_key=('discord_user:' || CAST(l.subject_user_id AS TEXT))"
            "))"
        )
    else:
        # A persisted link cannot be verified when either target table is absent.
        moment_link_integrity_violations = one(
            f"SELECT COUNT(*) FROM relationship_event_moment_links_v2 {where}"
        )
    cross_guild_member_violations = sum([
        one(f"SELECT COUNT(*) FROM relationship_events_v2 WHERE {scoped}subject_key<>('discord_user:' || CAST(subject_user_id AS TEXT))"),
        one(f"SELECT COUNT(*) FROM relationship_state_v2 WHERE {scoped}subject_key<>('discord_user:' || CAST(subject_user_id AS TEXT))"),
        one(f"SELECT COUNT(*) FROM relationship_engagement_shadow_runs WHERE {scoped}subject_key<>('discord_user:' || CAST(subject_user_id AS TEXT))"),
        one(f"SELECT COUNT(*) FROM relationship_observation_diagnostics_v2 WHERE {scoped}subject_key<>('discord_user:' || CAST(subject_user_id AS TEXT))"),
        one(f"SELECT COUNT(*) FROM relationship_member_settings_v2 WHERE {scoped}subject_key<>('discord_user:' || CAST(subject_user_id AS TEXT))"),
        one(f"SELECT COUNT(*) FROM relationship_member_preferences_v2 WHERE {scoped}subject_key<>('discord_user:' || CAST(subject_user_id AS TEXT))"),
    ])
    withheld_reason_counts: dict[str, int] = {}
    for raw_reasons, count in conn.execute(f"SELECT withheld_reason_codes,COUNT(*) FROM relationship_engagement_shadow_runs {where} GROUP BY withheld_reason_codes", p).fetchall():
        try:
            parsed = json.loads(raw_reasons or "[]")
            reasons = parsed if isinstance(parsed, list) else []
        except (TypeError, ValueError, json.JSONDecodeError):
            reasons = []
        if raw_reasons and raw_reasons != "[]" and not reasons:
            reasons = ["malformed_withheld_reason_json"]
        for raw_reason in reasons:
            reason = re.sub(r"[^a-z0-9_:-]+", "_", str(raw_reason or "unknown").strip().lower())[:80] or "unknown"
            withheld_reason_counts[reason] = withheld_reason_counts.get(reason, 0) + int(count or 0)
    return {
        "eligible_user_authored_events": one(f"SELECT COUNT(*) FROM relationship_events_v2 {where + (' AND' if where else 'WHERE')} actor_role='user' AND lifecycle='active' AND event_type<>'unclassified'"),
        "rejected_or_unclassified_user_evidence": one(f"SELECT COUNT(*) FROM relationship_observation_diagnostics_v2 {where}"),
        "passive_message_rejections": one(f"SELECT COUNT(*) FROM relationship_observation_diagnostics_v2 {where + (' AND' if where else 'WHERE')} rejection_reason='passive'"),
        "sealed_test_rejections": one(f"SELECT COUNT(*) FROM relationship_observation_diagnostics_v2 {where + (' AND' if where else 'WHERE')} rejection_reason='sealed_test'"),
        "policy_route_ambiguity_rejections": one(f"SELECT COUNT(*) FROM relationship_observation_diagnostics_v2 {where + (' AND' if where else 'WHERE')} rejection_reason='policy_or_route_or_ambiguous'"),
        "model_audit_events": by_type.get("model_audit",0)+by_type.get("model_playful_rivalry_acceptance",0), "events_by_type": by_type, "lifecycle_exclusions": {k:v for k,v in lifecycle.items() if k in BLOCKED_LIFECYCLES},
        "moment_linked_events": one(f"SELECT COUNT(DISTINCT event_id) FROM relationship_event_moment_links_v2 {where + (' AND' if where else 'WHERE')} lifecycle='active'"),
        "cross_guild_member_violations": cross_guild_member_violations,
        "ledger_link_integrity_violations": ledger_link_integrity_violations,
        "moment_link_integrity_violations": moment_link_integrity_violations,
        "visibility_violations": one(f"SELECT COUNT(*) FROM relationship_events_v2 {where + (' AND' if where else 'WHERE')} visibility<>'private'"),
        "shadow_runs": one(f"SELECT COUNT(*) FROM relationship_engagement_shadow_runs {where}"),
        "policy_eligible_shadow_candidates": one(f"SELECT COUNT(*) FROM relationship_engagement_shadow_runs {where + (' AND' if where else 'WHERE')} policy_eligible=1"),
        "would_select": one(f"SELECT COUNT(*) FROM relationship_engagement_shadow_runs {where + (' AND' if where else 'WHERE')} would_select=1"),
        "live_emission_allowed": one(f"SELECT COUNT(*) FROM relationship_engagement_shadow_runs {where + (' AND' if where else 'WHERE')} live_emission_allowed=1"),
        "actual_live_emissions": one(f"SELECT COUNT(*) FROM relationship_engagement_shadow_runs {where + (' AND' if where else 'WHERE')} actual_emitted=1"),
        "withheld_reasons": withheld_reason_counts,
        "opt_out_rejections": one(f"SELECT COUNT(*) FROM relationship_engagement_shadow_runs {where + (' AND' if where else 'WHERE')} withheld_reason_codes LIKE '%member_opt_out%'"),
        "privacy_rejections": one(f"SELECT COUNT(*) FROM relationship_engagement_shadow_runs {where + (' AND' if where else 'WHERE')} (withheld_reason_codes LIKE '%visibility%' OR withheld_reason_codes LIKE '%policy%')"),
        "cooldown_rejections": one(f"SELECT COUNT(*) FROM relationship_engagement_shadow_runs {where + (' AND' if where else 'WHERE')} withheld_reason_codes LIKE '%cooldown%'"),
        "rivalry_rejections": one(f"SELECT COUNT(*) FROM relationship_engagement_shadow_runs {where + (' AND' if where else 'WHERE')} withheld_reason_codes LIKE '%rivalry%'"),
        "processing_errors": one(f"SELECT COUNT(*) FROM relationship_engagement_shadow_runs {where + (' AND' if where else 'WHERE')} processing_errors_json<>'[]'"),
        "legacy_v2_comparison": "not_collected", "rollback_readiness": {"shadow_default_off": not shadow_enabled({}), "relationship_live_default_off": not live_enabled({}), "active_engagement_live_default_off": not active_engagement_live_enabled({})},
    }

def complete_delete_relationship_v2(conn: sqlite3.Connection, *, guild_id: int, user_id: int) -> dict[str,int]:
    ensure_relationship_v2_schema(conn); counts={}; sk=subject_key_for_user(user_id)
    linked=[r[0] for r in conn.execute("SELECT ledger_entry_id FROM relationship_event_ledger_links_v2 WHERE guild_id=? AND subject_user_id=?", (guild_id,user_id)).fetchall()]
    for eid in linked:
        counts["memory_ledger_entries"] = counts.get("memory_ledger_entries",0) + conn.execute("DELETE FROM memory_ledger_entries WHERE guild_id=? AND entry_id=?", (guild_id,eid)).rowcount if _table_exists(conn,"memory_ledger_entries") else counts.get("memory_ledger_entries",0)
        if _table_exists(conn,"memory_ledger_lineage"): counts["memory_ledger_lineage"] = counts.get("memory_ledger_lineage",0) + conn.execute("DELETE FROM memory_ledger_lineage WHERE guild_id=? AND (entry_id=? OR target_entry_id=?)", (guild_id,eid,eid)).rowcount
        if _table_exists(conn,"memory_ledger_participants"): counts["memory_ledger_participants"] = counts.get("memory_ledger_participants",0) + conn.execute("DELETE FROM memory_ledger_participants WHERE guild_id=? AND entry_id=?", (guild_id,eid)).rowcount
    counts["relationship_event_moment_links_v2"] = conn.execute("DELETE FROM relationship_event_moment_links_v2 WHERE guild_id=? AND subject_user_id=?", (guild_id,user_id)).rowcount
    counts["relationship_observation_diagnostics_v2"] = conn.execute("DELETE FROM relationship_observation_diagnostics_v2 WHERE guild_id=? AND subject_user_id=?", (guild_id,user_id)).rowcount
    counts["relationship_event_ledger_links_v2"] = conn.execute("DELETE FROM relationship_event_ledger_links_v2 WHERE guild_id=? AND subject_user_id=?", (guild_id,user_id)).rowcount
    counts["relationship_events_v2"] = conn.execute("DELETE FROM relationship_events_v2 WHERE guild_id=? AND subject_user_id=?", (guild_id,user_id)).rowcount
    counts["relationship_state_v2"] = conn.execute("DELETE FROM relationship_state_v2 WHERE guild_id=? AND subject_user_id=?", (guild_id,user_id)).rowcount
    counts["relationship_member_settings_v2"] = conn.execute("DELETE FROM relationship_member_settings_v2 WHERE guild_id=? AND subject_user_id=?", (guild_id,user_id)).rowcount
    counts["relationship_member_preferences_v2"] = conn.execute("DELETE FROM relationship_member_preferences_v2 WHERE guild_id=? AND subject_user_id=?", (guild_id,user_id)).rowcount
    counts["relationship_engagement_shadow_runs"] = conn.execute("DELETE FROM relationship_engagement_shadow_runs WHERE guild_id=? AND subject_user_id=?", (guild_id,user_id)).rowcount
    return counts

def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    return bool(conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone())
