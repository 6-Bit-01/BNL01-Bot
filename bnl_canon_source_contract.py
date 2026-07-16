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
    PUBLIC_OBSERVATION = "public_observation"
    EVIDENCE_PROJECTION = "evidence_projection"
    DERIVED_SUMMARY = "derived_summary"
    LEGACY_SOURCE_BLIND = "legacy_source_blind"

class Visibility(str, Enum):
    PUBLIC = "public"
    PUBLIC_SAFE = "public_safe"
    INTERNAL = "internal"
    PRIVATE = "private"
    MOD = "mod"
    SEALED_TEST = "sealed_test"
    PROTECTED = "protected"

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
BNL01 = SubjectIdentity("bnl_01", "BNL-01", ("BNL", "BARCODE Network Liaison Entity"))

FOUNDING_MEMBERS = ("6 Bit", "DJ Floppydisc", "Cache Back", "Mac Modem")
FRIDAY_PUBLIC_SCHEDULE = {
    "intake_begins": "6:40 PM Pacific",
    "show_begins": "7:00 PM Pacific",
    "first_track_target": "7:05 PM Pacific",
}
BNL_ROLES = (
    "lore liaison", "conversational presence", "memory and continuity layer",
    "procedural archivist", "relationship observer", "public relay brain",
    "source-safety boundary", "dossier/entity-intelligence contributor",
    "operator-facing analytical system", "developing intelligence with his own curiosity",
)
CANON_FACTS = (
    CanonFact(BARCODE, "founding_members", FOUNDING_MEMBERS),
    CanonFact(BARCODE_NETWORK, "origin", "The music and collective existed before BARCODE Network; the Network grew around that signal and now connects music, live broadcasts, community, software, archive, characters, and story."),
    CanonFact(SIX_BIT, "primary_identity", "artist, producer, host, and founding BARCODE member first"),
    CanonFact(BARCODE_RADIO, "public_nature", "real weekly live broadcast and community music space on TikTok"),
    CanonFact(BARCODE_RADIO, "friday_public_schedule", FRIDAY_PUBLIC_SCHEDULE),
    CanonFact(BNL01, "identity", "BARCODE Network Liaison Entity: an unfinished but active intelligence and one shared mind with filtered surfaces"),
    CanonFact(BNL01, "roles", BNL_ROLES),
    CanonFact(BNL01, "in_world_boundary", "BNL remains fully in-world; lore is his native operating language, not decorative flavor."),
    CanonFact(BARCODE_NETWORK, "website_information_architecture", "Reality first. Meaning second. Mythology deeper. This governs public website information architecture, not BNL's speaking order."),
)

_SOURCE_RANK = {c: i for i, c in enumerate((SourceClass.LEGACY_SOURCE_BLIND, SourceClass.DERIVED_SUMMARY, SourceClass.EVIDENCE_PROJECTION, SourceClass.PUBLIC_OBSERVATION, SourceClass.FIRST_PARTY_RECORD, SourceClass.APPROVED_CANON, SourceClass.OWNER_CORRECTION), start=1)}
_PUBLIC_VIS = {Visibility.PUBLIC, Visibility.PUBLIC_SAFE}
_ROUTE_SOURCE_MAP = {
    "source_safe_public": SourceClass.FIRST_PARTY_RECORD,
    "public_safe_memory": SourceClass.EVIDENCE_PROJECTION,
    "show_status_public": SourceClass.PUBLIC_OBSERVATION,
    "public_show_state": SourceClass.PUBLIC_OBSERVATION,
    "source_files": SourceClass.FIRST_PARTY_RECORD,
    "broadcast_memory": SourceClass.FIRST_PARTY_RECORD,
    "ops": SourceClass.OWNER_CORRECTION,
}
_CHANNEL_VISIBILITY_MAP = {"public_home": Visibility.PUBLIC, "public_context": Visibility.PUBLIC, "public_selective": Visibility.PUBLIC_SAFE, "internal_controlled": Visibility.INTERNAL, "sealed_test": Visibility.SEALED_TEST, "protected_system": Visibility.PROTECTED, "broadcast_memory": Visibility.INTERNAL}

QUEUE_KEYS = {"queue", "session", "payment", "nowPlaying", "currentTrack", "upNext", "nextTrack", "availability", "queuedTracks", "activeTracks", "completedTracks", "artists"}

def render_prompt_canon_block() -> str:
    return "\n".join([
        "## CANONICAL FACTS — DO NOT ALTER",
        f"- BARCODE began as the four-member digital hip-hop collective of: {', '.join(FOUNDING_MEMBERS)}.",
        "- The music and collective existed before BARCODE Network; the Network grew around that signal and now connects music, live broadcasts, community, software, archive, characters, and story.",
        "- 6 Bit is an artist, producer, host, and founding BARCODE member first.",
        "- BARCODE Radio is a real weekly live broadcast and community music space on TikTok.",
        "- Current public Friday schedule: submissions/intake begins at 6:40 PM Pacific; the show begins at 7:00 PM Pacific; the first track is targeted for 7:05 PM Pacific.",
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

def map_channel_policy_visibility(policy: str) -> Visibility:
    return _CHANNEL_VISIBILITY_MAP.get((policy or "").strip(), Visibility.INTERNAL)

def is_public_usable(claim: SourceClaim, target_visibility: Visibility = Visibility.PUBLIC) -> bool:
    return bool(claim.valid and not claim.retracted and not claim.expired and claim.visibility in _PUBLIC_VIS and target_visibility in _PUBLIC_VIS)

def is_independent_evidence(claim: SourceClaim) -> bool:
    return claim.source_class is not SourceClass.DERIVED_SUMMARY and not claim.projection and not claim.derived_from

def resolve_claims(claims: Iterable[SourceClaim], *, target_visibility: Visibility = Visibility.PUBLIC_SAFE) -> Resolution:
    eligible = [c for c in claims if is_public_usable(c, target_visibility)]
    if not eligible:
        return Resolution(False, None, "no_public_usable_claim")
    corrections = {sid for c in eligible for sid in (c.correction_of + c.supersedes)}
    eligible = [c for c in eligible if c.claim_id not in corrections]
    eligible.sort(key=lambda c: (_SOURCE_RANK.get(c.source_class, 0), c.observed_at or datetime.min.replace(tzinfo=timezone.utc)), reverse=True)
    winner = eligible[0]
    return Resolution(True, winner, "resolved_by_authority_and_correction")

def current_time_claim_resolution(claims: Iterable[SourceClaim], *, now: datetime | None = None, max_age_minutes: int = 30) -> Resolution:
    now = now or datetime.now(timezone.utc)
    fresh = []
    for c in claims:
        if not is_public_usable(c):
            continue
        if c.observed_at is None:
            continue
        observed = c.observed_at if c.observed_at.tzinfo else c.observed_at.replace(tzinfo=timezone.utc)
        if now - observed <= timedelta(minutes=max_age_minutes) and c.source_class in {SourceClass.PUBLIC_OBSERVATION, SourceClass.FIRST_PARTY_RECORD, SourceClass.OWNER_CORRECTION}:
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

def strip_queue_sections(read_model: dict | None, *, environ: dict[str, str] | None = None) -> dict:
    if not isinstance(read_model, dict):
        return {}
    if queue_usability(read_model, environ=environ)["usable"]:
        return read_model
    cleaned = dict(read_model)
    sections = cleaned.get("sections") if isinstance(cleaned.get("sections"), dict) else None
    if sections is not None:
        new_sections = dict(sections)
        for key in ("queue", "session", "payments", "payment", "availability"):
            new_sections.pop(key, None)
        if "artists" in new_sections:
            new_sections["artists"] = []
        cleaned["sections"] = new_sections
    for key in ("queue", "session", "payments", "payment", "availability", "nowPlaying", "upNext"):
        cleaned.pop(key, None)
    if "artists" in cleaned:
        cleaned["artists"] = []
    return cleaned

def diagnostics(read_model: dict | None = None, *, environ: dict[str, str] | None = None) -> dict[str, Any]:
    q = queue_usability(read_model, environ=environ)
    return {"contractVersion": CANON_SOURCE_CONTRACT_VERSION, "compatibilityAdaptersActive": True, "localQueueProductionCapability": q["local"], "websiteQueueProductionCapability": q["website"], "effectiveQueueUsable": q["usable"], "queueReason": q["reason"]}
