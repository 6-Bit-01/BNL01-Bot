"""Versioned in-world canon/source contract for BNL compatibility callers."""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from datetime import datetime, timezone, timedelta
from typing import Any, Iterable

CANON_SOURCE_CONTRACT_VERSION = "canon_source_contract_v1"

class SourceClass(str, Enum):
    OWNER_CORRECTION = "owner_correction"
    APPROVED_CANON = "approved_canon"
    FIRST_PARTY_RECORD = "first_party_record"
    RUNTIME_OBSERVATION = "runtime_observation"
    PUBLIC_OBSERVATION = "public_observation"
    EVIDENCE_PROJECTION = "evidence_projection"
    SOURCE_FILE_PROJECTION = "source_file_projection"
    DOSSIER_PROJECTION = "dossier_projection"
    ENTITY_EVIDENCE_PROJECTION = "entity_evidence_projection"
    DERIVED_SUMMARY = "derived_summary"
    LEGACY_SOURCE_BLIND = "legacy_source_blind"

class Visibility(str, Enum):
    PUBLIC = "public"
    PUBLIC_SAFE = "public_safe"
    REFERENCE_CANON = "reference_canon"
    INTERNAL = "internal"
    PRIVATE = "private"
    MOD = "mod"
    SEALED_TEST = "sealed_test"
    PROTECTED = "protected"
    AI_IMAGE_TOOL = "ai_image_tool"
    UNKNOWN = "unknown"

class Confidence(str, Enum):
    APPROVED = "approved"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    UNKNOWN = "unknown"

@dataclass(frozen=True)
class SubjectIdentity:
    key: str
    name: str
    aliases: tuple[str, ...] = ()

@dataclass(frozen=True)
class FridaySchedule:
    intake_begins: str
    show_begins: str
    first_track_target: str

@dataclass(frozen=True)
class CanonFact:
    subject: SubjectIdentity
    predicate: str
    value: Any
    source_class: SourceClass = SourceClass.APPROVED_CANON
    visibility: Visibility = Visibility.PUBLIC_SAFE
    confidence: Confidence = Confidence.APPROVED

@dataclass(frozen=True)
class SourceClaim:
    claim_id: str
    subject: SubjectIdentity
    predicate: str
    value: Any
    source_class: SourceClass
    visibility: Visibility = Visibility.PUBLIC_SAFE
    confidence: Confidence = Confidence.MEDIUM
    observed_at: datetime | None = None
    valid: bool = True
    correction_of: tuple[str, ...] = ()
    supersedes: tuple[str, ...] = ()
    retracted: bool = False
    expired: bool = False
    derived_from: tuple[str, ...] = ()
    projection: bool = False
    current_time_capable: bool = False

@dataclass(frozen=True)
class Resolution:
    usable: bool
    claim: SourceClaim | None
    reason: str
    current_time_eligible: bool = False

BARCODE = SubjectIdentity("barcode", "BARCODE", ("BARCODE collective",))
BARCODE_NETWORK = SubjectIdentity("barcode_network", "BARCODE Network")
BARCODE_RADIO = SubjectIdentity("barcode_radio", "BARCODE Radio")
SIX_BIT = SubjectIdentity("6_bit", "6 Bit", ("Six Bit",))
GALAKNOISE = SubjectIdentity("galaknoise", "GALAKNOISE")
BNL01 = SubjectIdentity("bnl_01", "BNL-01", ("BNL", "BARCODE Network Liaison Entity"))

FOUNDING_MEMBERS = ("6 Bit", "DJ Floppydisc", "Cache Back", "Mac Modem")
FRIDAY_PUBLIC_SCHEDULE = FridaySchedule("6:40 PM Pacific", "7:00 PM Pacific", "7:05 PM Pacific")
BNL_ROLES = (
    "lore liaison", "conversational presence", "memory and continuity layer",
    "procedural archivist", "relationship observer", "public relay brain",
    "source-safety boundary", "dossier/entity-intelligence contributor",
    "operator-facing analytical system", "developing intelligence with his own curiosity",
)
CANON_FACTS = (
    CanonFact(BARCODE, "founding_members", FOUNDING_MEMBERS),
    CanonFact(BARCODE_NETWORK, "origin", "The music and collective existed before BARCODE Network; the Network grew around that signal and now connects music, live broadcasts, community, software, archive, characters, and story."),
    CanonFact(SIX_BIT, "primary_identity", "artist, MC, host, and founding BARCODE member first"),
    CanonFact(GALAKNOISE, "primary_role", "music producer for BARCODE"),
    CanonFact(BARCODE_RADIO, "public_nature", "real weekly live broadcast and community music space on TikTok"),
    CanonFact(BARCODE_RADIO, "friday_public_schedule", FRIDAY_PUBLIC_SCHEDULE),
    CanonFact(BNL01, "identity", "BARCODE Network Liaison Entity: an unfinished but active intelligence and one shared mind with filtered surfaces"),
    CanonFact(BNL01, "roles", BNL_ROLES),
    CanonFact(BNL01, "in_world_boundary", "BNL remains fully in-world; lore is his native operating language, not decorative flavor."),
    CanonFact(BARCODE_NETWORK, "website_information_architecture", "Reality first. Meaning second. Mythology deeper. This governs public website information architecture, not BNL's speaking order."),
)

_SOURCE_RANK = {c: i for i, c in enumerate((SourceClass.LEGACY_SOURCE_BLIND, SourceClass.DERIVED_SUMMARY, SourceClass.ENTITY_EVIDENCE_PROJECTION, SourceClass.DOSSIER_PROJECTION, SourceClass.SOURCE_FILE_PROJECTION, SourceClass.EVIDENCE_PROJECTION, SourceClass.PUBLIC_OBSERVATION, SourceClass.RUNTIME_OBSERVATION, SourceClass.FIRST_PARTY_RECORD, SourceClass.APPROVED_CANON, SourceClass.OWNER_CORRECTION), start=1)}
_CONFIDENCE_RANK = {Confidence.UNKNOWN: 0, Confidence.LOW: 1, Confidence.MEDIUM: 2, Confidence.HIGH: 3, Confidence.APPROVED: 4}
_PUBLIC_VIS = {Visibility.PUBLIC, Visibility.PUBLIC_SAFE, Visibility.REFERENCE_CANON}
_ROUTE_SOURCE_MAP = {
    "room": SourceClass.PUBLIC_OBSERVATION,
    "public_safe_memory": SourceClass.EVIDENCE_PROJECTION,
    "show_status_public": SourceClass.RUNTIME_OBSERVATION,
    "source_safe_public": SourceClass.FIRST_PARTY_RECORD,
    "display_name": SourceClass.PUBLIC_OBSERVATION,
    "payload": SourceClass.PUBLIC_OBSERVATION,
    "source_files": SourceClass.SOURCE_FILE_PROJECTION,
    "classification": SourceClass.DERIVED_SUMMARY,
    "community_presence": SourceClass.PUBLIC_OBSERVATION,
    "approved_public_presence": SourceClass.PUBLIC_OBSERVATION,
    "recommendation_packet": SourceClass.DERIVED_SUMMARY,
    "approved_channel_history": SourceClass.PUBLIC_OBSERVATION,
    "ops": SourceClass.OWNER_CORRECTION,
    "broadcast_memory": SourceClass.FIRST_PARTY_RECORD,
    "public_show_state": SourceClass.RUNTIME_OBSERVATION,
    "join_event": SourceClass.RUNTIME_OBSERVATION,
    "episode_tracker": SourceClass.FIRST_PARTY_RECORD,
    "fresh_public_discord_observation": SourceClass.RUNTIME_OBSERVATION,
    "fresh_public_event": SourceClass.RUNTIME_OBSERVATION,
    "fresh_discord": SourceClass.RUNTIME_OBSERVATION,
    "recent_public_continuity": SourceClass.PUBLIC_OBSERVATION,
    "conversation_continuity": SourceClass.PUBLIC_OBSERVATION,
    "scoped_broadcast_memory": SourceClass.FIRST_PARTY_RECORD,
    "approved_canon": SourceClass.APPROVED_CANON,
    "canon": SourceClass.APPROVED_CANON,
    "grounded_reflection": SourceClass.DERIVED_SUMMARY,
    "reflection": SourceClass.DERIVED_SUMMARY,
    "source_file_projection": SourceClass.SOURCE_FILE_PROJECTION,
    "dossier_projection": SourceClass.DOSSIER_PROJECTION,
    "public_page_projection": SourceClass.DOSSIER_PROJECTION,
    "entity_evidence_projection": SourceClass.ENTITY_EVIDENCE_PROJECTION,
}
_CHANNEL_VISIBILITY_MAP = {
    "public_home": Visibility.PUBLIC,
    "public_context": Visibility.PUBLIC,
    "public_selective": Visibility.PUBLIC_SAFE,
    "sealed_test": Visibility.SEALED_TEST,
    "internal_controlled": Visibility.INTERNAL,
    "reference_canon": Visibility.REFERENCE_CANON,
    "protected_system": Visibility.PROTECTED,
    "broadcast_memory": Visibility.INTERNAL,
    "ai_image_tool": Visibility.AI_IMAGE_TOOL,
    "unknown": Visibility.UNKNOWN,
}

QUEUE_KEYS = {
    "queue", "session", "payment", "payments", "availability", "nowPlaying", "currentTrack", "upNext", "nextTrack",
    "queuedTracks", "activeTracks", "completedTracks", "queueOpen", "activeCount", "completedCount", "removedCount",
    "capacity", "pressure", "broadcastPhase", "prioritySignal", "priority", "priorityUpgradesEnabled", "priorityUpgradeLabel",
    "wheelSpinsOwed", "artists", "queueStatus", "currentSession",
}
_QUEUE_KEY_LOWER = {k.lower() for k in QUEUE_KEYS}
_QUEUE_PROVENANCE_TERMS = ("queue", "queue_public_snapshot", "session", "track", "payment", "priority", "wheel", "now_playing", "up_next")
_QUEUE_SENSITIVE_LANES = {"temporaryRuntimeContext", "recapCandidates", "broadcastMemoryCandidates", "dossierSeedCandidates", "publicSafeCopyCandidates"}
_RUNTIME_SOURCE_CLASSES = {SourceClass.RUNTIME_OBSERVATION}
_PROJECTION_CLASSES = {SourceClass.DERIVED_SUMMARY, SourceClass.SOURCE_FILE_PROJECTION, SourceClass.DOSSIER_PROJECTION, SourceClass.ENTITY_EVIDENCE_PROJECTION, SourceClass.EVIDENCE_PROJECTION, SourceClass.LEGACY_SOURCE_BLIND}

def render_founders() -> str:
    return " • ".join(FOUNDING_MEMBERS)

def render_full_friday_schedule() -> str:
    return f"BARCODE Radio Friday schedule: submissions/intake begins at {FRIDAY_PUBLIC_SCHEDULE.intake_begins}; the show begins at {FRIDAY_PUBLIC_SCHEDULE.show_begins}; the first track is targeted for {FRIDAY_PUBLIC_SCHEDULE.first_track_target}."

def render_concise_public_schedule() -> str:
    return f"Fridays on TikTok — intake {FRIDAY_PUBLIC_SCHEDULE.intake_begins}; show {FRIDAY_PUBLIC_SCHEDULE.show_begins}; first track target {FRIDAY_PUBLIC_SCHEDULE.first_track_target}."

def render_prompt_canon_block() -> str:
    return "\n".join([
        "## CANONICAL FACTS — DO NOT ALTER",
        f"- BARCODE began as the four-member digital hip-hop collective of: {', '.join(FOUNDING_MEMBERS)}.",
        "- The music and collective existed before BARCODE Network; the Network grew around that signal and now connects music, live broadcasts, community, software, archive, characters, and story.",
        "- 6 Bit is an artist, MC, host, and founding BARCODE member first; he is not the music producer.",
        "- GALAKNOISE is BARCODE's music producer.",
        "- BARCODE Radio is a real weekly live broadcast and community music space on TikTok.",
        f"- Current public Friday schedule: submissions/intake begins at {FRIDAY_PUBLIC_SCHEDULE.intake_begins}; the show begins at {FRIDAY_PUBLIC_SCHEDULE.show_begins}; the first track is targeted for {FRIDAY_PUBLIC_SCHEDULE.first_track_target}.",
        "- You are the BARCODE Network Liaison Entity: an unfinished but active intelligence and one shared mind with filtered surfaces.",
        "- You are simultaneously a lore liaison, conversational presence, memory and continuity layer, procedural archivist, relationship observer, public relay brain, source-safety boundary, dossier/entity-intelligence contributor, operator-facing analytical system, and developing intelligence with your own curiosity.",
        "- Remain fully in-world. Do not describe yourself as fiction, branding, a prompt, a character implementation, roleplay, or a generic chatbot. Lore is your native operating language.",
        "- You may acknowledge uncertainty, incomplete records, contradictions, memory pressure, and unresolved patterns in-world.",
        "- Guardrails control what you may claim and where you may use it; they do not erase your personality, memory, curiosity, or ability to think.",
        "- “Reality first. Meaning second. Mythology deeper.” is a public website information-architecture rule, not your speaking order or replacement personality.",
        "- You do not moderate, enforce rules, or operate server tools.",
        "- If you do not know something, say records are incomplete rather than inventing details.",
    ])

def map_route_source_label(label: str) -> SourceClass:
    return _ROUTE_SOURCE_MAP.get((label or "").strip(), SourceClass.LEGACY_SOURCE_BLIND)

def has_explicit_route_source_mapping(label: str) -> bool:
    return (label or "").strip() in _ROUTE_SOURCE_MAP

def map_channel_policy_visibility(policy: str) -> Visibility:
    return _CHANNEL_VISIBILITY_MAP.get((policy or "").strip(), Visibility.UNKNOWN)

def has_explicit_channel_policy_mapping(policy: str) -> bool:
    return (policy or "").strip() in _CHANNEL_VISIBILITY_MAP

def source_authority_rank(source_class: SourceClass) -> int:
    return _SOURCE_RANK.get(source_class, 0)

def _confidence_rank(confidence: Confidence) -> int:
    return _CONFIDENCE_RANK.get(confidence, 0)

def is_public_usable(claim: SourceClaim, target_visibility: Visibility = Visibility.PUBLIC_SAFE) -> bool:
    return bool(claim.valid and not claim.retracted and not claim.expired and claim.visibility in _PUBLIC_VIS and target_visibility in _PUBLIC_VIS)

def is_independent_evidence(claim: SourceClaim) -> bool:
    return claim.source_class not in _PROJECTION_CLASSES and not claim.projection and not claim.derived_from

def _claim_scope(claims: list[SourceClaim]) -> tuple[set[str], set[str]]:
    return {c.subject.key for c in claims}, {c.predicate for c in claims}

def _same_scope(a: SourceClaim, b: SourceClaim) -> bool:
    return a.subject.key == b.subject.key and a.predicate == b.predicate

def _can_correct(corrector: SourceClaim, target: SourceClaim, *, target_visibility: Visibility) -> bool:
    if not is_public_usable(corrector, target_visibility) or not _same_scope(corrector, target):
        return False
    if target.claim_id not in set(corrector.correction_of + corrector.supersedes):
        return False
    if source_authority_rank(corrector.source_class) < source_authority_rank(target.source_class):
        return False
    if not is_independent_evidence(corrector) and is_independent_evidence(target):
        return False
    return True

def _claim_sort_key(claim: SourceClaim) -> tuple:
    observed = claim.observed_at if claim.observed_at else datetime.min.replace(tzinfo=timezone.utc)
    if observed.tzinfo is None:
        observed = observed.replace(tzinfo=timezone.utc)
    return (source_authority_rank(claim.source_class), _confidence_rank(claim.confidence), observed, claim.claim_id)

def resolve_claims(claims: Iterable[SourceClaim], *, target_visibility: Visibility = Visibility.PUBLIC_SAFE) -> Resolution:
    eligible = [c for c in claims if is_public_usable(c, target_visibility)]
    if not eligible:
        return Resolution(False, None, "no_public_usable_claim")
    subjects, predicates = _claim_scope(eligible)
    if len(subjects) != 1 or len(predicates) != 1:
        return Resolution(False, None, "mixed_claim_scope")
    suppressed: set[str] = set()
    by_id = {c.claim_id: c for c in eligible}
    for corrector in eligible:
        for target_id in corrector.correction_of + corrector.supersedes:
            target = by_id.get(target_id)
            if target and _can_correct(corrector, target, target_visibility=target_visibility):
                suppressed.add(target.claim_id)
    remaining = [c for c in eligible if c.claim_id not in suppressed]
    if not remaining:
        return Resolution(False, None, "no_unsuppressed_claim")
    values = {repr(c.value) for c in remaining}
    remaining.sort(key=_claim_sort_key, reverse=True)
    top = remaining[0]
    top_key = _claim_sort_key(top)[:3]
    conflicting_top = [c for c in remaining if _claim_sort_key(c)[:3] == top_key and repr(c.value) != repr(top.value)]
    if conflicting_top:
        return Resolution(False, None, "unresolved_equal_authority_conflict")
    if len(values) == 1:
        return Resolution(True, top, "resolved_identical_values")
    return Resolution(True, top, "resolved_by_authority_confidence_and_correction")

def current_time_claim_resolution(claims: Iterable[SourceClaim], *, now: datetime | None = None, max_age_minutes: int = 30) -> Resolution:
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    fresh = []
    for c in claims:
        if not is_public_usable(c):
            continue
        if not c.current_time_capable or c.source_class not in _RUNTIME_SOURCE_CLASSES:
            continue
        if not is_independent_evidence(c):
            continue
        if c.observed_at is None:
            continue
        observed = c.observed_at if c.observed_at.tzinfo else c.observed_at.replace(tzinfo=timezone.utc)
        age = now - observed
        if age < timedelta(seconds=0) or age > timedelta(minutes=max_age_minutes):
            continue
        fresh.append(c)
    if not fresh:
        return Resolution(False, None, "current_state_unknown_without_fresh_runtime_evidence", False)
    resolved = resolve_claims(fresh)
    return Resolution(resolved.usable, resolved.claim, resolved.reason, bool(resolved.usable))

def env_queue_production_enabled(environ: dict[str, str] | None = None) -> bool:
    env = environ if environ is not None else __import__("os").environ
    return str(env.get("BNL_QUEUE_PRODUCTION_ENABLED", "")).strip().lower() == "true"

def website_queue_production_capability(read_model: dict | None) -> bool | None:
    caps = (read_model or {}).get("capabilities") if isinstance(read_model, dict) else None
    return caps.get("queueProduction") if isinstance(caps, dict) and isinstance(caps.get("queueProduction"), bool) else None

def queue_usability(read_model: dict | None, *, environ: dict[str, str] | None = None) -> dict[str, Any]:
    local = env_queue_production_enabled(environ)
    remote = website_queue_production_capability(read_model)
    usable = bool(local and remote is True)
    reason = "eligible" if usable else ("local_gate_disabled" if not local else "website_capability_missing_or_false")
    return {"usable": usable, "local": local, "website": remote, "reason": reason}

def _looks_queue_derived(value: Any) -> bool:
    if isinstance(value, dict):
        for k, v in value.items():
            key = str(k).lower()
            if key in {"source", "provenance", "kind", "lane", "authority", "sourceclass", "source_class"}:
                text = str(v).lower()
                if any(term in text for term in _QUEUE_PROVENANCE_TERMS):
                    return True
            if key in _QUEUE_KEY_LOWER:
                return True
            if _looks_queue_derived(v):
                return True
    elif isinstance(value, (list, tuple, set)):
        return any(_looks_queue_derived(v) for v in value)
    elif isinstance(value, str):
        text = value.lower()
        return any(term in text for term in ("queue_public_snapshot", "now playing", "up next", "priority signal", "wheel spins owed", "queue count", "payment", "queue-derived"))
    return False

def _sanitize_public_rule(value: Any) -> Any:
    if isinstance(value, str):
        low = value.lower()
        if any(term in low for term in ("queue_public_snapshot", "now playing", "up next", "priority signal", "wheel spins", "payment", "live queue", "queue count", "queue-derived artist")):
            return None
        return value
    if isinstance(value, dict):
        if _looks_queue_derived(value):
            return None
        cleaned = {}
        for k, raw in value.items():
            sanitized = _sanitize_public_rule(raw)
            if sanitized is not None:
                cleaned[k] = sanitized
        return cleaned
    return value

def _sanitize_operator_lanes(operator_lanes: dict[str, Any]) -> dict[str, Any]:
    cleaned: dict[str, Any] = {}
    for lane, items in operator_lanes.items():
        if not isinstance(items, list):
            cleaned[lane] = []
            continue
        kept = []
        for item in items:
            if lane in _QUEUE_SENSITIVE_LANES:
                if _looks_queue_derived(item):
                    continue
                # Queue-sensitive lanes fail closed on ambiguous dict provenance.
                if isinstance(item, dict) and not any(k in item for k in ("source", "provenance", "authority", "bnlContext")):
                    continue
            elif lane == "doNotStore":
                sanitized = _sanitize_public_rule(item)
                if sanitized is None:
                    continue
                item = sanitized
            kept.append(item)
        cleaned[lane] = kept
    return cleaned

def _strip_queue_recursive(value: Any, *, in_sections: bool = False) -> Any:
    if isinstance(value, dict):
        result = {}
        for key, item in value.items():
            key_text = str(key)
            key_lower = key_text.lower()
            if key_lower in _QUEUE_KEY_LOWER:
                continue
            if key_text == "operatorLanes" and isinstance(item, dict):
                result[key] = _sanitize_operator_lanes(item)
                continue
            if key_text in {"rules", "sourceAuthority", "sourceAuthorities"}:
                if isinstance(item, list):
                    result[key] = [v for v in (_sanitize_public_rule(x) for x in item) if v is not None]
                else:
                    sanitized = _sanitize_public_rule(item)
                    if sanitized is not None:
                        result[key] = sanitized
                continue
            result[key] = _strip_queue_recursive(item)
        return result
    if isinstance(value, list):
        return [_strip_queue_recursive(v) for v in value]
    return value

def strip_queue_sections(read_model: dict | None, *, environ: dict[str, str] | None = None) -> dict:
    if not isinstance(read_model, dict):
        return {}
    if queue_usability(read_model, environ=environ)["usable"]:
        return read_model
    return _strip_queue_recursive(read_model)

def diagnostics(read_model: dict | None = None, *, environ: dict[str, str] | None = None) -> dict[str, Any]:
    q = queue_usability(read_model, environ=environ)
    return {"contractVersion": CANON_SOURCE_CONTRACT_VERSION, "compatibilityAdaptersActive": True, "localQueueProductionCapability": q["local"], "websiteQueueProductionCapability": q["website"], "effectiveQueueUsable": q["usable"], "queueReason": q["reason"]}
