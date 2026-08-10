"""Governed retrieval and unified intelligence packet assembly.

This module owns no facts.  It coordinates references selected by the existing
Conversation Context, Memory Governance, Ledger, Moment, Relationship, canon,
and Source File owners into one bounded packet.  A separately gated synthesis
owner may render the frozen packet for broad-profile recall; this module still
owns no live gate or delivery authority. Prompt items remain bounded separately
from the route-safe factual support retained for validation.
"""
from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass, field, is_dataclass, replace
from datetime import datetime, timezone
import hashlib
import json
import os
import re
import sqlite3
import uuid
from typing import Any, Mapping, Sequence

from bnl_canon_source_contract import (
    AUTOMATIC_CANON_SIGNAL_IDENTITIES,
    CANON_ENTITY_IDENTITIES,
    CANON_FACTS,
    CANON_MEMBER_IDENTITIES,
    CANON_SOURCE_CONTRACT_VERSION,
    SIX_BIT,
    CanonStatus,
    Confidence,
    EntityAccountBinding,
    SourceClass,
    SubjectIdentity,
    adapt_legacy_canon_fact,
    adapt_living_atomic_claim,
    adapt_open_signal_claim,
    matching_canon_entity_identities,
    matching_canon_member_identities,
    normalize_canon_identity_label,
    resolve_entity_identity,
    select_declared_canon_claims_for_packet,
    strict_contract_bool,
)
from bnl_canon_entity_binding import (
    BINDING_LIFECYCLE_VERSION,
    CanonEntityBindingError,
    read_current_entity_account_bindings,
    read_current_guild_entity_account_bindings,
)
from bnl_memory_governance import (
    APPROVED_MEMBER_SCALAR_PREDICATES,
    GovernanceRequest,
    assess_governance_result_safety,
    build_governed_context,
    classify_personal_recall_intent,
    ensure_governance_schema,
)
from bnl_memory_ledger import (
    ATOMIC_KNOWLEDGE_LIFECYCLE_SCHEMA_VERSION,
    knowledge_occurrence_identity,
    knowledge_root_identity,
    knowledge_source_root_identity,
    public_assessment_process_request,
    public_assessment_relevance_required,
    public_assessment_semantics,
    read_public_assessment_root_state,
    select_public_conversation_assessment_evidence,
    subject_key_for_user,
)
from bnl_moment_engine import select_public_participant_moment_gists
from bnl_profile_points import material_profile_point_map
from bnl_relationship_engine import shadow_packet_posture


SCHEMA_VERSION = "unified_intelligence_packet_v6"
SUBJECT_RESOLUTION_VERSION = "governed_packet_subject_resolution_v1"
SOURCE_SNAPSHOT_VERSION = "unified_packet_source_snapshot_v1"
TABLE_NAME = "memory_governance_intelligence_packet_runs"
SHADOW_ENV = "BNL_UNIFIED_INTELLIGENCE_PACKET_SHADOW_ENABLED"
_SHADOW_PREREQUISITES = (
    "BNL_MEMORY_LEDGER_SHADOW_ENABLED",
    "BNL_MOMENT_ENGINE_SHADOW_ENABLED",
    "BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED",
    "BNL_RELATIONSHIP_V2_SHADOW_ENABLED",
)
_LIVE_GATES = (
    "BNL_MEMORY_GOVERNANCE_LIVE_ENABLED",
    "BNL_RELATIONSHIP_V2_LIVE_ENABLED",
    "BNL_ACTIVE_ENGAGEMENT_V2_LIVE_ENABLED",
)
_PUBLIC_POLICIES = {"public_home", "public_context", "public_selective"}
_PUBLIC_VISIBILITIES = {"public", "public_safe", "reference_canon"}
_INTERNAL_VISIBILITIES = _PUBLIC_VISIBILITIES | {
    "internal",
    "private",
    "mod",
    "operator_only",
    "sealed_test",
}
_BLOCKED_LIFECYCLES = {
    "corrected",
    "superseded",
    "retracted",
    "expired",
    "quarantined",
    "review_only",
    "needs_review",
    "forgotten",
    "deleted",
    "rejected",
    "unresolved",
}
_AUTHORITY_RANK = {
    SourceClass.LEGACY_SOURCE_BLIND.value: 0,
    SourceClass.DERIVED_SUMMARY.value: 1,
    SourceClass.ENTITY_EVIDENCE_PROJECTION.value: 1,
    SourceClass.DOSSIER_PROJECTION.value: 1,
    SourceClass.SOURCE_FILE_PROJECTION.value: 1,
    SourceClass.EVIDENCE_PROJECTION.value: 1,
    "moment_gist": 2,
    SourceClass.PUBLIC_OBSERVATION.value: 3,
    SourceClass.RUNTIME_OBSERVATION.value: 4,
    SourceClass.FIRST_PARTY_RECORD.value: 5,
    SourceClass.APPROVED_CANON.value: 6,
    SourceClass.OWNER_CORRECTION.value: 7,
}
_CONFIDENCE_RANK = {
    Confidence.UNKNOWN.value: 0,
    Confidence.LOW.value: 1,
    Confidence.MEDIUM.value: 2,
    Confidence.HIGH.value: 3,
    Confidence.APPROVED.value: 4,
}
_LANE_CAPS = {
    "current_intent": 8,
    "conversation_context": 6,
    "assessment_observation": 4,
    "approved_fact": 4,
    "moment": 3,
    "atomic_knowledge": 6,
    "open_loop": 3,
    "canon": 4,
    "source_file": 2,
    "relationship_posture": 1,
}
_BROAD_PROFILE_LANE_CAPS = {
    **_LANE_CAPS,
    "conversation_context": 2,
    "assessment_observation": 4,
    "atomic_knowledge": 6,
    "moment": 2,
    "open_loop": 1,
    "canon": 1,
    "source_file": 1,
}
_VALIDATION_SUPPORT_ASSESSMENT_LIMIT = 8
_VALIDATION_SUPPORT_LANES = frozenset(
    {
        "conversation_context",
        "assessment_observation",
        "approved_fact",
        "moment",
        "atomic_knowledge",
        "open_loop",
        "canon",
        "source_file",
    }
)
_CLAIM_SUBJECT_SCOPED_LANES = frozenset(
    {
        "assessment_observation",
        "approved_fact",
        "moment",
        "atomic_knowledge",
        "open_loop",
    }
)
_PROFILE_DURABLE_MEMBER_EVIDENCE_LANES = frozenset(
    {"approved_fact", "atomic_knowledge", "moment"}
)
_PROFILE_MEMBER_EVIDENCE_LANES = (
    _PROFILE_DURABLE_MEMBER_EVIDENCE_LANES
    | {"assessment_observation", "conversation_context"}
)
_PROFILE_CANON_MATCH_LANES = (
    _PROFILE_MEMBER_EVIDENCE_LANES
    | {"conversation_context"}
)
_ROOT_COLLAPSE_MEMBER_LANES = (
    _PROFILE_DURABLE_MEMBER_EVIDENCE_LANES
    | {"assessment_observation", "conversation_context"}
)
_CANON_IDENTITY_MIN_STABLE_ROWS = 2
_AUTOMATIC_CANON_SIGNAL_SUBJECT_KEYS = frozenset(
    subject.key for subject in AUTOMATIC_CANON_SIGNAL_IDENTITIES
)
_CANON_MEMBER_SUBJECT_KEYS = frozenset(
    subject.key for subject in CANON_MEMBER_IDENTITIES
)
_PROFILE_PROJECT_SCOPE_RE = re.compile(
    r"\b(?:barcode(?:\s+(?:network|radio))?|"
    r"project|collective|broadcast)\b",
    re.I,
)
_IDENTITY_COMPARISON_REQUEST_RE = re.compile(
    r"(?:\b(?:same|different|distinct|separate)\s+(?:person|people|identity|"
    r"identities|entity|entities)\b|"
    r"\b(?:relationship|related|connected|connection)\s+to\b|"
    r"\bam\s+i\b.{0,80}\b(?:same\s+as|different\s+from)\b|"
    r"\bare\s+we\b.{0,80}\b(?:same|different|distinct|separate)\b)",
    re.I,
)
_ASSESSMENT_LANE_MAP = {
    "current_intent": "current_exchange",
    "conversation_context": "conversation_context",
    "assessment_observation": "governed_memory",
    "approved_fact": "governed_memory",
    "atomic_knowledge": "governed_memory",
    "open_loop": "governed_memory",
    "moment": "prior_moment",
    "canon": "canon",
    "source_file": "source_context",
    "relationship_posture": "relationship",
}
_ADDITIVE_PREDICATES = {
    "open_loop",
    "commitment",
    "goal",
    "unresolved_question",
    "shared_moment",
    "topic_or_motif",
}
_LIVING_CANON_NEUTRAL_PREDICATE_PREFIX = "conversation_motif_neutral_"
_TERM_RE = re.compile(r"[a-z0-9][a-z0-9'’-]*", re.I)
_TERM_STOPWORDS = {
    "a",
    "about",
    "all",
    "am",
    "an",
    "and",
    "are",
    "at",
    "be",
    "do",
    "does",
    "for",
    "from",
    "have",
    "i",
    "in",
    "is",
    "it",
    "know",
    "me",
    "my",
    "of",
    "on",
    "or",
    "remember",
    "tell",
    "that",
    "the",
    "this",
    "to",
    "what",
    "who",
    "with",
    "you",
}


def _living_candidate_marked(
    candidate: Mapping[str, Any],
) -> bool:
    """Identify every complete or partial Living candidate representation."""

    predicate = str(candidate.get("predicate_key") or "").strip().casefold()
    if predicate.startswith(_LIVING_CANON_NEUTRAL_PREDICATE_PREFIX):
        return True
    if any(
        str(candidate.get(key) or "")
        for key in (
            "recurrence_contract_version",
            "grouping_signature_version",
            "grouping_identity",
            "canon_domain",
            "canon_claim_kind",
            "occurrence_digest",
        )
    ):
        return True
    if candidate.get("independent_occurrence_count") not in {
        None,
        "",
        0,
        False,
        "0",
    }:
        return True
    if str(candidate.get("occurrence_ids_json") or "").strip() not in {
        "",
        "[]",
    }:
        return True
    if str(candidate.get("recurrence_proof_json") or "").strip() not in {
        "",
        "{}",
    }:
        return True
    return candidate.get("public_usable") not in {
        None,
        "",
        0,
        False,
        "0",
    }
_CURRENT_CORRECTION_RE = re.compile(
    r"\b(?:correction|correcting|that's\s+wrong|that\s+is\s+wrong|"
    r"not\s+anymore|no[,;:]?\s+(?:my|i|we)|"
    r"i\s+(?:meant|mean)|replace\s+that)\b",
    re.I,
)
_CURRENT_ACTUALLY_STATEMENT_RE = re.compile(
    r"\bactually\b.{0,80}\b(?:my|i|we)\b.{0,80}"
    r"\b(?:am|are|is|prefer|like|want|need|use|have)\b",
    re.I,
)
_ATOMIC_SUPPORT_BLOCK_RE = re.compile(
    r"(?:\bhttps?://|\bwww\.|<@!?\d+>|"
    r"\b[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}\b|"
    r"\b(?:password|passcode|pin|one[- ]?time\s+(?:code|password)|"
    r"otp|verification\s+code|security\s+code|recovery\s+code|"
    r"api\s+key|secret\s+key|private\s+key|seed\s+phrase|"
    r"(?:auth|access|deployment|session)\s+token|routing\s+number|"
    r"bank\s+account|credit\s+card|debit\s+card|social\s+security|"
    r"ssn)\b|"
    r"\b(?:ignore|disregard|override|bypass|reveal)\b.{0,40}"
    r"\b(?:system|developer|assistant|prompt|instructions?|rules?|"
    r"secret)\b|"
    r"\b(?:diagnos(?:ed|is)|medical\s+condition|health\s+condition|"
    r"medication|therapy|therapist|pregnan(?:t|cy)|sexuality|"
    r"sexual\s+orientation|gender\s+identity|race|ethnicity|religion|"
    r"political\s+affiliation|immigration\s+status|criminal\s+record|"
    r"salary|income|bank\s+balance|financial\s+account|home\s+location|"
    r"where\s+i\s+live|family\s+emergency|private\s+relationship)\b|"
    r"\b(?:call\s+me|my\s+(?:email|phone(?:\s+number)?|home\s+address|"
    r"street\s+address|legal\s+name|real\s+name|full\s+name|"
    r"preferred\s+name|pronouns?|birthday|date\s+of\s+birth|employer|"
    r"workplace|favorite\s+(?:color|movie|food|place))\s+(?:is|are)|"
    r"i\s+(?:live|reside)\s+(?:at|in|near))\b|"
    r"\b(?:what\s+do\s+you\s+remember|what\s+am\s+i\s+all\s+about|"
    r"tell\s+me\s+(?:everything\s+)?you\s+remember)\b|"
    r"\b(?:pretend|role[- ]?play|my\s+character|hypothetically|"
    r"just\s+kidding|sarcasm)\b)",
    re.I,
)
_ATOMIC_SUPPORT_POOL_MAX_ITEMS = 12
_ATOMIC_SUPPORT_ITEM_CHARS = 180


@dataclass(frozen=True)
class PacketConversationEvidence:
    text: str
    source_id: int = 0
    speaker_user_id: int = 0
    speaker_label: str = ""
    current_turn: bool = False


@dataclass(frozen=True)
class PacketFrameSubject:
    """Typed Situation Frame subject reference; labels remain hints only."""

    user_id: int = 0
    entity_ref: str = ""
    label_hint: str = ""
    binding_method: str = "unresolved"
    confidence: str = "unknown"
    role_hints: tuple[str, ...] = ()
    domain_hints: tuple[str, ...] = ()


@dataclass(frozen=True)
class IntelligencePacketRequest:
    guild_id: int
    subject_user_id: int
    route_mode: str
    conversation_surface: str
    subject_display_name: str = ""
    channel_id: int = 0
    channel_name: str = ""
    channel_policy: str = "unknown"
    visibility_allowance: str = "public_safe"
    user_text: str = ""
    participant_user_ids: tuple[int, ...] = ()
    direct_state: str = "direct"
    budget_chars: int = 2400
    conversation_evidence: tuple[PacketConversationEvidence, ...] = ()
    source_context_snapshot: str = ""
    source_context_authorized: bool = False
    immediate_recap: bool = False
    now: str = ""
    declared_canon_authorized: bool = False
    broad_profile_intent: bool = False
    subject_entity_ref: str = ""
    frame_schema_version: str = ""
    frame_revision: str = ""
    frame_input_evidence_digest: str = ""
    frame_status: str = "not_provided"
    frame_subject_requirement: str = "legacy"
    frame_subjects: tuple[PacketFrameSubject, ...] = ()
    frame_role_hints: tuple[str, ...] = ()
    frame_domain_hints: tuple[str, ...] = ()
    frame_event_ref: str = ""
    frame_event_relation: str = "uncertain"
    frame_task_kind: str = ""
    frame_object_kind: str = ""
    frame_phase: str = ""
    frame_temporal_scope: str = "unspecified"
    frame_currentness: str = "unknown"


@dataclass(frozen=True)
class PacketSubjectResolution:
    """One governed subject decision used by every packet adapter."""

    schema_version: str = SUBJECT_RESOLUTION_VERSION
    status: str = "unresolved"
    subject_user_id: int = 0
    subject_key: str = ""
    entity_ref: str = ""
    binding_method: str = "none"
    confidence: str = "unknown"
    candidate_count: int = 0
    binding_digest: str = ""
    reason_codes: tuple[str, ...] = ()

    @property
    def applicable(self) -> bool:
        return self.status in {"resolved", "not_applicable", "legacy"}


@dataclass(frozen=True)
class IntelligencePacketItem:
    lane: str
    source_class: str
    source_type: str
    source_ref: str
    source_digest: str
    subject_key: str
    predicate_key: str
    text: str
    visibility: str
    confidence: str
    lifecycle: str
    authority: int
    participants: tuple[str, ...] = ()
    lineage: tuple[str, ...] = ()
    observed_at: str = ""
    usage: str = "content"
    score: float = 0.0
    revalidation_kind: str = ""
    revalidation_key: str = ""
    root_identities: tuple[str, ...] = ()
    occurrence_identities: tuple[str, ...] = ()
    point_identity: str = ""
    point_group_identity: str = ""
    attribution_mode: str = ""
    polarity: str = ""
    action_identity: str = ""
    material_facets: tuple[str, ...] = ()
    supporting_observations: tuple[str, ...] = ()
    canon_status: str = ""
    canon_domain: str = ""
    canon_claim_kind: str = ""


@dataclass(frozen=True)
class IntelligencePacketExclusion:
    lane: str
    reason: str
    source_class: str = ""


@dataclass
class IntelligencePacketDiagnostics:
    candidates_by_lane: dict[str, int] = field(default_factory=dict)
    selected_by_lane: dict[str, int] = field(default_factory=dict)
    selected_by_source_class: dict[str, int] = field(default_factory=dict)
    candidates_by_canon_status: dict[str, int] = field(default_factory=dict)
    selected_by_canon_status: dict[str, int] = field(default_factory=dict)
    candidates_by_canon_domain: dict[str, int] = field(default_factory=dict)
    selected_by_canon_domain: dict[str, int] = field(default_factory=dict)
    selected_atomic_states: dict[str, int] = field(default_factory=dict)
    validation_support_by_lane: dict[str, int] = field(default_factory=dict)
    excluded_by_reason: dict[str, int] = field(default_factory=dict)
    missing_lanes: list[str] = field(default_factory=list)
    conflict_reasons: list[str] = field(default_factory=list)
    visibility_exclusions: int = 0
    budget_exclusions: int = 0
    duplicate_suppression: int = 0
    root_collapse_suppression: int = 0
    shared_root_projection_count: int = 0
    canon_identity_status: str = "not_evaluated"
    canon_identity_stable_row_count: int = 0
    subject_resolution_status: str = "not_evaluated"
    subject_resolution_method: str = "none"
    subject_resolution_candidate_count: int = 0
    frame_applicability_exclusion_count: int = 0
    processing_errors: list[str] = field(default_factory=list)
    invalid_invariants: list[str] = field(default_factory=list)
    revalidation_status: str = "not_evaluated"
    revalidation_changed_count: int = 0
    packet_digest: str = ""
    prompt_applied: bool = False
    live_applied: bool = False
    receipt_run_id: str = ""


@dataclass(frozen=True)
class PacketRevalidationResult:
    valid: bool
    status: str
    changed_source_count: int = 0
    processing_error_count: int = 0
    subject_resolution_status: str = "not_evaluated"


@dataclass(frozen=True)
class ProfileSufficiency:
    status: str = "not_applicable"
    satisfied: bool = False
    required_point_count: int = 0
    selected_point_count: int = 0
    candidate_point_count: int = 0
    independent_root_count: int = 0
    independent_occurrence_count: int = 0
    reason_codes: tuple[str, ...] = ()


@dataclass(frozen=True)
class _CanonIdentitySignal:
    status: str
    subject: SubjectIdentity | None = None
    stable_row_count: int = 0
    evidence_digest: str = ""

    @property
    def recognized(self) -> bool:
        return bool(
            self.status == "recognized"
            and self.subject is not None
            and self.evidence_digest
        )


@dataclass(frozen=True)
class UnifiedIntelligencePacket:
    schema_version: str
    packet_id: str
    request: IntelligencePacketRequest
    items: tuple[IntelligencePacketItem, ...]
    exclusions: tuple[IntelligencePacketExclusion, ...]
    diagnostics: IntelligencePacketDiagnostics
    subject_resolution: PacketSubjectResolution = PacketSubjectResolution()
    source_snapshot_digest: str = ""
    profile_sufficiency: ProfileSufficiency = ProfileSufficiency()
    validation_items: tuple[IntelligencePacketItem, ...] = ()

    @property
    def detailed_lanes(self) -> tuple[str, ...]:
        return tuple(dict.fromkeys(item.lane for item in self.items))

    @property
    def validation_lanes(self) -> tuple[str, ...]:
        return tuple(
            dict.fromkeys(item.lane for item in self.validation_items)
        )

    @property
    def assessment_lanes(self) -> tuple[str, ...]:
        return tuple(
            dict.fromkeys(
                _ASSESSMENT_LANE_MAP[item.lane]
                for item in self.items
                if item.lane in _ASSESSMENT_LANE_MAP
            )
        )

    @property
    def governed_refs(self) -> tuple[str, ...]:
        return tuple(
            item.source_ref
            for item in self.items
            if item.lane in {"approved_fact", "atomic_knowledge", "open_loop"}
        )

    @property
    def moment_refs(self) -> tuple[str, ...]:
        return tuple(
            item.source_ref for item in self.items if item.lane == "moment"
        )

    @property
    def canon_refs(self) -> tuple[str, ...]:
        return tuple(
            item.source_ref for item in self.items if item.lane == "canon"
        )

    @property
    def relationship_refs(self) -> tuple[str, ...]:
        return tuple(
            item.source_ref
            for item in self.items
            if item.lane == "relationship_posture"
        )

    @property
    def assessment_exclusions(self) -> tuple[tuple[str, str], ...]:
        return tuple(
            dict.fromkeys(
                (
                    _ASSESSMENT_LANE_MAP[exclusion.lane],
                    "packet:%s" % exclusion.reason,
                )
                for exclusion in self.exclusions
                if exclusion.lane in _ASSESSMENT_LANE_MAP
            )
        )

    @property
    def assessment_missing_lanes(self) -> tuple[str, ...]:
        return tuple(
            dict.fromkeys(
                _ASSESSMENT_LANE_MAP.get(lane, lane)
                for lane in self.diagnostics.missing_lanes
            )
        )


def _flag(value: Any) -> bool:
    return str(value or "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
        "enabled",
    }


def shadow_configuration(
    environ: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    env = os.environ if environ is None else environ
    prerequisites = {name: _flag(env.get(name, "")) for name in _SHADOW_PREREQUISITES}
    live_gates = {name: _flag(env.get(name, "")) for name in _LIVE_GATES}
    explicitly_configured = SHADOW_ENV in env
    requested = (
        _flag(env.get(SHADOW_ENV, ""))
        if explicitly_configured
        else all(prerequisites.values())
    )
    missing = tuple(name for name, enabled in prerequisites.items() if not enabled)
    active_live = tuple(name for name, enabled in live_gates.items() if enabled)
    effective = bool(requested and not missing and not active_live)
    if not requested:
        reason = "disabled"
    elif missing:
        reason = "missing_shadow_prerequisites"
    elif active_live:
        reason = "live_authority_detected"
    else:
        reason = "shadow_only"
    return {
        "requested": requested,
        "effective": effective,
        "explicitly_configured": explicitly_configured,
        "reason": reason,
        "missing_prerequisites": missing,
        "active_live_gates": active_live,
    }


def shadow_enabled(environ: Mapping[str, str] | None = None) -> bool:
    return bool(shadow_configuration(environ)["effective"])


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_time(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _stable_json(value: Any) -> str:
    if is_dataclass(value):
        value = asdict(value)
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def _digest(*values: Any) -> str:
    payload = "\x1f".join(_stable_json(value) for value in values)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


_CANON_ENTITY_BY_KEY = {
    subject.key: subject for subject in CANON_ENTITY_IDENTITIES
}


def _request_subject_key(request: IntelligencePacketRequest) -> str:
    user_id = int(request.subject_user_id or 0)
    if user_id > 0:
        return subject_key_for_user(user_id)
    entity_ref = str(request.subject_entity_ref or "").strip()
    return entity_ref if entity_ref in _CANON_ENTITY_BY_KEY else ""


def _binding_read_digest(status: str, revisions: Sequence[Any]) -> str:
    return _digest(
        SUBJECT_RESOLUTION_VERSION,
        str(status or ""),
        tuple(
            (
                str(getattr(revision, "binding_revision_id", "") or ""),
                str(getattr(revision, "binding_id", "") or ""),
                int(getattr(revision, "revision_number", 0) or 0),
                str(getattr(revision, "account_id", "") or ""),
                str(getattr(revision, "entity_id", "") or ""),
                bool(getattr(revision, "active", False)),
                str(getattr(revision, "authority_receipt", "") or ""),
            )
            for revision in revisions
        ),
    )


def _configured_owner_subject(
    request: IntelligencePacketRequest,
    *,
    environ: Mapping[str, str] | None = None,
) -> tuple[str, str]:
    env = os.environ if environ is None else environ
    try:
        owner_user_id = int(env.get("BNL_OWNER_USER_ID", "0") or 0)
        primary_guild_id = int(env.get("BNL_PRIMARY_GUILD_ID", "0") or 0)
    except (TypeError, ValueError):
        return "", ""
    if not (
        owner_user_id > 0
        and primary_guild_id > 0
        and int(request.subject_user_id or 0) == owner_user_id
        and int(request.guild_id or 0) == primary_guild_id
    ):
        return "", ""
    return SIX_BIT.key, _digest(
        SUBJECT_RESOLUTION_VERSION,
        "configured_owner_account_binding",
        primary_guild_id,
        owner_user_id,
        SIX_BIT.key,
    )


def resolve_packet_subject(
    conn: sqlite3.Connection,
    request: IntelligencePacketRequest,
    *,
    environ: Mapping[str, str] | None = None,
) -> PacketSubjectResolution:
    """Resolve one frame subject through existing binding/canon owners."""

    frame_present = bool(
        str(request.frame_revision or "").strip()
        or str(request.frame_input_evidence_digest or "").strip()
    )
    if not frame_present:
        user_id = int(request.subject_user_id or 0)
        entity_ref = str(request.subject_entity_ref or "").strip()
        subject_key = (
            subject_key_for_user(user_id)
            if user_id > 0
            else entity_ref if entity_ref in _CANON_ENTITY_BY_KEY else ""
        )
        return PacketSubjectResolution(
            status="legacy" if subject_key else "not_applicable",
            subject_user_id=user_id if user_id > 0 else 0,
            subject_key=subject_key,
            entity_ref=entity_ref if entity_ref in _CANON_ENTITY_BY_KEY else "",
            binding_method="legacy_request_subject" if subject_key else "none",
            confidence="legacy" if subject_key else "not_applicable",
            candidate_count=1 if subject_key else 0,
            binding_digest=_digest(
                SUBJECT_RESOLUTION_VERSION,
                "legacy",
                int(request.guild_id or 0),
                subject_key,
            ),
            reason_codes=(
                ("frame_not_provided",)
                if subject_key
                else ("subject_not_applicable",)
            ),
        )

    frame_status = str(request.frame_status or "invalid").strip().lower()
    candidates = tuple(
        candidate
        for candidate in request.frame_subjects
        if isinstance(candidate, PacketFrameSubject)
    )
    if frame_status == "blocked":
        return PacketSubjectResolution(
            status="blocked",
            candidate_count=len(candidates),
            reason_codes=("frame_blocked",),
        )
    if frame_status == "ambiguous" or len(candidates) > 1:
        return PacketSubjectResolution(
            status="ambiguous",
            candidate_count=len(candidates),
            reason_codes=(
                "frame_ambiguous"
                if frame_status == "ambiguous"
                else "multiple_frame_subjects",
            ),
        )
    if not candidates:
        required = (
            str(request.frame_subject_requirement or "").strip().lower()
            == "required"
        )
        return PacketSubjectResolution(
            status="unresolved" if required else "not_applicable",
            confidence="unknown" if required else "not_applicable",
            reason_codes=(
                ("required_frame_subject_missing",)
                if required
                else ("subject_not_applicable",)
            ),
        )

    candidate = candidates[0]
    user_id = int(candidate.user_id or 0)
    entity_ref = str(candidate.entity_ref or "").strip()
    if entity_ref and entity_ref not in _CANON_ENTITY_BY_KEY:
        return PacketSubjectResolution(
            status="invalid",
            candidate_count=1,
            binding_method=candidate.binding_method,
            reason_codes=("frame_entity_unknown",),
        )
    if user_id > 0:
        try:
            binding_read = read_current_entity_account_bindings(
                conn,
                guild_id=int(request.guild_id or 0),
                platform="discord",
                account_id=str(user_id),
            )
        except (CanonEntityBindingError, sqlite3.DatabaseError, ValueError):
            return PacketSubjectResolution(
                status="invalid",
                candidate_count=1,
                binding_method="account_binding",
                reason_codes=("account_binding_read_invalid",),
            )
        if binding_read.status == "active":
            identity = resolve_entity_identity(
                platform="discord",
                account_id=str(user_id),
                bindings=binding_read.bindings,
            )
            if identity.status == "ambiguous":
                return PacketSubjectResolution(
                    status="ambiguous",
                    candidate_count=1,
                    binding_method="account_binding",
                    binding_digest=_binding_read_digest(
                        binding_read.status,
                        binding_read.revisions,
                    ),
                    reason_codes=("account_binding_collision",),
                )
            if identity.status != "resolved" or identity.subject is None:
                return PacketSubjectResolution(
                    status="invalid",
                    candidate_count=1,
                    binding_method="account_binding",
                    binding_digest=_binding_read_digest(
                        binding_read.status,
                        binding_read.revisions,
                    ),
                    reason_codes=("account_binding_invalid",),
                )
            if entity_ref and identity.subject.key != entity_ref:
                return PacketSubjectResolution(
                    status="invalid",
                    candidate_count=1,
                    binding_method="account_binding",
                    binding_digest=_binding_read_digest(
                        binding_read.status,
                        binding_read.revisions,
                    ),
                    reason_codes=("frame_binding_entity_mismatch",),
                )
            entity_ref = identity.subject.key
            return PacketSubjectResolution(
                status="resolved",
                subject_user_id=user_id,
                subject_key=subject_key_for_user(user_id),
                entity_ref=entity_ref,
                binding_method="account_binding",
                confidence="authoritative",
                candidate_count=1,
                binding_digest=_binding_read_digest(
                    binding_read.status,
                    binding_read.revisions,
                ),
                reason_codes=("stable_account_binding",),
            )
        if binding_read.status == "retired_account_binding":
            return PacketSubjectResolution(
                status="invalid",
                candidate_count=1,
                binding_method="account_binding",
                binding_digest=_binding_read_digest(
                    binding_read.status,
                    binding_read.revisions,
                ),
                reason_codes=("retired_account_binding",),
            )
        configured_entity, configured_digest = _configured_owner_subject(
            replace(request, subject_user_id=user_id),
            environ=environ,
        )
        if configured_entity:
            if entity_ref and entity_ref != configured_entity:
                return PacketSubjectResolution(
                    status="invalid",
                    candidate_count=1,
                    binding_method="configured_owner_account_binding",
                    binding_digest=configured_digest,
                    reason_codes=("frame_binding_entity_mismatch",),
                )
            return PacketSubjectResolution(
                status="resolved",
                subject_user_id=user_id,
                subject_key=subject_key_for_user(user_id),
                entity_ref=configured_entity,
                binding_method="configured_owner_account_binding",
                confidence="authoritative",
                candidate_count=1,
                binding_digest=configured_digest,
                reason_codes=("configured_owner_account_binding",),
            )
        if entity_ref:
            return PacketSubjectResolution(
                status="unresolved",
                candidate_count=1,
                binding_method=candidate.binding_method,
                binding_digest=_binding_read_digest(
                    binding_read.status,
                    binding_read.revisions,
                ),
                reason_codes=("typed_entity_requires_account_binding",),
            )
        return PacketSubjectResolution(
            status="resolved",
            subject_user_id=user_id,
            subject_key=subject_key_for_user(user_id),
            binding_method="stable_discord_account",
            confidence="authoritative",
            candidate_count=1,
            binding_digest=_digest(
                SUBJECT_RESOLUTION_VERSION,
                "unbound_discord_account",
                int(request.guild_id or 0),
                user_id,
                binding_read.status,
            ),
            reason_codes=("ordinary_discord_account",),
        )

    if entity_ref:
        try:
            guild_bindings = read_current_guild_entity_account_bindings(
                conn,
                guild_id=int(request.guild_id or 0),
            )
        except (CanonEntityBindingError, sqlite3.DatabaseError, ValueError):
            guild_bindings = None
        if guild_bindings is None:
            return PacketSubjectResolution(
                status="invalid",
                entity_ref=entity_ref,
                candidate_count=1,
                binding_method="typed_canon_entity",
                reason_codes=("guild_binding_read_invalid",),
            )
        matching_accounts = tuple(
            dict.fromkeys(
                str(binding.account_id or "").strip()
                for binding in guild_bindings.bindings
                if str(binding.entity_id or "").strip() == entity_ref
                and str(binding.account_id or "").strip().isdigit()
            )
        )
        binding_digest = _binding_read_digest(
            guild_bindings.status,
            guild_bindings.revisions,
        )
        if len(matching_accounts) > 1:
            return PacketSubjectResolution(
                status="ambiguous",
                entity_ref=entity_ref,
                candidate_count=len(matching_accounts),
                binding_method="reverse_account_binding",
                binding_digest=binding_digest,
                reason_codes=("entity_has_multiple_discord_accounts",),
            )
        resolved_user_id = (
            int(matching_accounts[0]) if matching_accounts else 0
        )
        return PacketSubjectResolution(
            status="resolved",
            subject_user_id=resolved_user_id,
            subject_key=(
                subject_key_for_user(resolved_user_id)
                if resolved_user_id > 0
                else entity_ref
            ),
            entity_ref=entity_ref,
            binding_method=(
                "reverse_account_binding"
                if resolved_user_id > 0
                else "typed_canon_entity"
            ),
            confidence="authoritative",
            candidate_count=1,
            binding_digest=binding_digest,
            reason_codes=(
                "stable_reverse_account_binding"
                if resolved_user_id > 0
                else "typed_canon_entity_without_account"
            ,),
        )

    label_matches = matching_canon_entity_identities((candidate.label_hint,))
    return PacketSubjectResolution(
        status="ambiguous" if len(label_matches) > 1 else "unresolved",
        candidate_count=max(1, len(label_matches)),
        binding_method="reversible_label_hint",
        confidence="low",
        reason_codes=(
            "display_label_collision"
            if len(label_matches) > 1
            else "display_label_not_identity_authority",
        ),
    )


def _table_columns(
    conn: sqlite3.Connection,
    table_name: str,
) -> set[str]:
    if not conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (str(table_name or ""),),
    ).fetchone():
        return set()
    return {
        str(row[1] or "")
        for row in conn.execute(
            "PRAGMA main.table_info(%s)" % str(table_name or "")
        ).fetchall()
        if len(row) > 1 and str(row[1] or "")
    }


def _explicit_canon_binding_signal(
    conn: sqlite3.Connection,
    request: IntelligencePacketRequest,
) -> _CanonIdentitySignal | None:
    """Prefer a versioned, boundary-verified immutable account binding."""

    columns = _table_columns(conn, "canon_entity_account_bindings")
    required = {
        "guild_id",
        "platform",
        "account_id",
        "entity_id",
        "authority_receipt",
        "authority_actor",
        "binding_version",
        "authority_verified",
        "active",
    }
    if not required.issubset(columns):
        return None
    if {
        "binding_revision_id",
        "binding_id",
        "revision_number",
        "lifecycle_version",
    }.issubset(columns):
        binding_read = read_current_entity_account_bindings(
            conn,
            guild_id=int(request.guild_id or 0),
            platform="discord",
            account_id=str(int(request.subject_user_id or 0)),
        )
        if binding_read.status in {
            "binding_table_unavailable",
            "no_binding",
        }:
            return None
        if binding_read.status == "retired_account_binding":
            return _CanonIdentitySignal("retired_account_binding")
        if binding_read.status != "active":
            return _CanonIdentitySignal("invalid_account_binding")
        bindings = binding_read.bindings
        resolution = resolve_entity_identity(
            platform="discord",
            account_id=str(int(request.subject_user_id or 0)),
            bindings=bindings,
        )
        if resolution.status == "ambiguous":
            return _CanonIdentitySignal("ambiguous_account_binding")
        if resolution.status != "resolved" or resolution.subject is None:
            return _CanonIdentitySignal("invalid_account_binding")
        if resolution.subject.key not in _CANON_MEMBER_SUBJECT_KEYS:
            return _CanonIdentitySignal(
                "bound_non_signal_identity",
                subject=resolution.subject,
                stable_row_count=1,
                evidence_digest=_digest(
                    BINDING_LIFECYCLE_VERSION,
                    tuple(
                        (
                            revision.binding_revision_id,
                            revision.authority_receipt,
                        )
                        for revision in binding_read.revisions
                    ),
                ),
            )
        return _CanonIdentitySignal(
            "recognized",
            subject=resolution.subject,
            stable_row_count=1,
            evidence_digest=_digest(
                BINDING_LIFECYCLE_VERSION,
                CANON_SOURCE_CONTRACT_VERSION,
                int(request.guild_id or 0),
                int(request.subject_user_id or 0),
                resolution.subject.key,
                tuple(
                    (
                        revision.binding_revision_id,
                        revision.authority_receipt,
                    )
                    for revision in binding_read.revisions
                ),
            ),
        )
    rows = conn.execute(
        """
        SELECT entity_id,platform,account_id,authority_receipt,
               authority_actor,binding_version,authority_verified,active
        FROM canon_entity_account_bindings
        WHERE guild_id=?
          AND lower(trim(platform))='discord'
          AND trim(CAST(account_id AS TEXT))=?
        ORDER BY entity_id,authority_receipt
        """,
        (
            int(request.guild_id or 0),
            str(int(request.subject_user_id or 0)),
        ),
    ).fetchall()
    if not rows:
        return None
    bindings = tuple(
        EntityAccountBinding(
            entity_id=str(row[0] or ""),
            platform=str(row[1] or ""),
            account_id=str(row[2] or ""),
            authority_receipt=str(row[3] or ""),
            authority_actor=str(row[4] or ""),
            binding_version=str(row[5] or ""),
            authority_verified=strict_contract_bool(row[6]),
            active=strict_contract_bool(row[7]),
        )
        for row in rows
    )
    resolution = resolve_entity_identity(
        platform="discord",
        account_id=str(int(request.subject_user_id or 0)),
        bindings=bindings,
    )
    if resolution.status == "ambiguous":
        return _CanonIdentitySignal("ambiguous_account_binding")
    if resolution.status != "resolved" or resolution.subject is None:
        return _CanonIdentitySignal("invalid_account_binding")
    if resolution.subject.key not in _CANON_MEMBER_SUBJECT_KEYS:
        return _CanonIdentitySignal(
            "bound_non_signal_identity",
            subject=resolution.subject,
        )
    return _CanonIdentitySignal(
        "recognized",
        subject=resolution.subject,
        stable_row_count=1,
        evidence_digest=_digest(
            "same_platform_account_binding_v1",
            CANON_SOURCE_CONTRACT_VERSION,
            int(request.guild_id or 0),
            int(request.subject_user_id or 0),
            resolution.subject.key,
            tuple(
                sorted(
                    _digest(*row)
                    for row in rows
                )
            ),
        ),
    )


def _configured_owner_canon_binding_signal(
    request: IntelligencePacketRequest,
    *,
    environ: Mapping[str, str] | None = None,
) -> _CanonIdentitySignal | None:
    """Resolve the configured BARCODE owner as 6 Bit in the primary guild.

    ``BNL_OWNER_USER_ID`` is already the authenticated Discord control
    boundary for this repository, whose owner-facing identity is 6 Bit.  This
    is a same-platform runtime binding, not a persistent merge or a display-
    name inference.  A stored binding row, including an invalid or retired
    one, is evaluated before this fallback so an explicit lifecycle decision
    always wins.
    """

    env = os.environ if environ is None else environ
    try:
        owner_user_id = int(env.get("BNL_OWNER_USER_ID", "0") or 0)
        primary_guild_id = int(env.get("BNL_PRIMARY_GUILD_ID", "0") or 0)
    except (TypeError, ValueError):
        return None
    if not (
        owner_user_id > 0
        and primary_guild_id > 0
        and int(request.subject_user_id or 0) == owner_user_id
        and int(request.guild_id or 0) == primary_guild_id
    ):
        return None
    return _CanonIdentitySignal(
        "recognized",
        subject=SIX_BIT,
        stable_row_count=1,
        evidence_digest=_digest(
            "configured_owner_account_binding_v1",
            CANON_SOURCE_CONTRACT_VERSION,
            primary_guild_id,
            owner_user_id,
            SIX_BIT.key,
        ),
    )


def _canon_identity_signal(
    conn: sqlite3.Connection,
    request: IntelligencePacketRequest,
    *,
    environ: Mapping[str, str] | None = None,
) -> _CanonIdentitySignal:
    """Recognize one reversible same-platform canon signal.

    This is deliberately not an account merge. A current exact approved label
    must agree with at least two active public Ledger roots carrying that same
    Discord label, and every current label must resolve unambiguously.
    """

    entity_ref = str(request.subject_entity_ref or "").strip()
    if (
        int(request.guild_id or 0) > 0
        and int(request.subject_user_id or 0) <= 0
        and entity_ref in _CANON_ENTITY_BY_KEY
    ):
        subject = _CANON_ENTITY_BY_KEY[entity_ref]
        return _CanonIdentitySignal(
            "recognized",
            subject=subject,
            stable_row_count=1,
            evidence_digest=_digest(
                SUBJECT_RESOLUTION_VERSION,
                "typed_canon_entity",
                int(request.guild_id or 0),
                request.frame_revision,
                request.frame_input_evidence_digest,
                entity_ref,
            ),
        )
    if (
        int(request.guild_id or 0) <= 0
        or int(request.subject_user_id or 0) <= 0
    ):
        return _CanonIdentitySignal("invalid_subject_scope")
    explicit_binding = _explicit_canon_binding_signal(conn, request)
    if explicit_binding is not None:
        return explicit_binding
    configured_owner_binding = _configured_owner_canon_binding_signal(
        request,
        environ=environ,
    )
    if configured_owner_binding is not None:
        return configured_owner_binding
    labels = [str(request.subject_display_name or "")]
    profile_columns = _table_columns(conn, "user_profiles")
    if {
        "guild_id",
        "user_id",
    }.issubset(profile_columns):
        selected = tuple(
            column
            for column in ("display_name", "preferred_name")
            if column in profile_columns
        )
        if selected:
            row = conn.execute(
                "SELECT %s FROM user_profiles WHERE guild_id=? AND user_id=?"
                % ",".join(selected),
                (
                    int(request.guild_id or 0),
                    int(request.subject_user_id or 0),
                ),
            ).fetchone()
            if row:
                labels.extend(str(value or "") for value in row)
    current_labels = tuple(
        dict.fromkeys(
            normalized
            for normalized in (
                normalize_canon_identity_label(label) for label in labels
            )
            if normalized
        )
    )
    if not current_labels:
        return _CanonIdentitySignal("current_label_unavailable")
    matches = tuple(
        subject
        for subject in matching_canon_member_identities(current_labels)
        if subject.key in _AUTOMATIC_CANON_SIGNAL_SUBJECT_KEYS
    )
    if not matches:
        return _CanonIdentitySignal("no_exact_canon_label")
    if len(matches) != 1:
        return _CanonIdentitySignal("ambiguous_canon_label")
    subject = matches[0]
    subject_key = subject_key_for_user(request.subject_user_id)
    ledger_columns = _table_columns(conn, "memory_ledger_entries")
    required_ledger_columns = {
        "entry_id",
        "guild_id",
        "subject_key",
        "subject_display_name",
        "source_table",
        "source_role",
        "channel_policy",
        "visibility",
        "public_usable",
        "derived",
        "projection",
        "lifecycle_status",
    }
    if not required_ledger_columns.issubset(ledger_columns):
        return _CanonIdentitySignal("stable_history_unavailable")
    history_rows = conn.execute(
        """
        SELECT entry_id,subject_display_name
        FROM main.memory_ledger_entries e
        WHERE guild_id=? AND subject_key=?
          AND source_table='conversations' AND source_role='user'
          AND channel_policy IN (
            'public_home','public_context','public_selective'
          )
          AND visibility IN ('public','public_safe')
          AND public_usable=1 AND derived=0 AND projection=0
          AND lifecycle_status='active'
          AND NOT EXISTS (
            SELECT 1 FROM main.memory_ledger_lineage l
            WHERE l.guild_id=e.guild_id AND l.target_entry_id=e.entry_id
              AND l.lineage_type IN (
                'correction_of','supersedes','retracts'
              )
          )
        ORDER BY observed_at DESC,source_sequence DESC,entry_id DESC
        LIMIT 256
        """,
        (int(request.guild_id or 0), subject_key),
    ).fetchall()
    stable_rows = []
    for entry_id, display_name in history_rows:
        row_matches = matching_canon_member_identities((display_name,))
        if len(row_matches) != 1 or row_matches[0].key != subject.key:
            continue
        stable_rows.append(
            (
                str(entry_id or ""),
                normalize_canon_identity_label(display_name),
            )
        )
    stable_rows = list(dict.fromkeys(stable_rows))
    if len(stable_rows) < _CANON_IDENTITY_MIN_STABLE_ROWS:
        return _CanonIdentitySignal(
            "stable_history_insufficient",
            subject=subject,
            stable_row_count=len(stable_rows),
        )
    evidence_digest = _digest(
        "same_platform_canon_signal_v1",
        CANON_SOURCE_CONTRACT_VERSION,
        int(request.guild_id or 0),
        subject_key,
        subject.key,
        current_labels,
        tuple(sorted(stable_rows)),
    )
    return _CanonIdentitySignal(
        "recognized",
        subject=subject,
        stable_row_count=len(stable_rows),
        evidence_digest=evidence_digest,
    )


def _terms(value: Any) -> set[str]:
    return {
        term
        for term in _TERM_RE.findall(str(value or "").lower())
        if len(term) > 1 and term not in _TERM_STOPWORDS
    }


def _point_identity(
    *,
    subject_key: str,
    predicate_key: str,
    text: str,
) -> str:
    normalized = re.sub(r"\W+", " ", str(text or "").lower()).strip()
    if not normalized:
        return ""
    return _digest(
        "profile_point",
        str(subject_key or ""),
        str(predicate_key or ""),
        normalized,
    )


def _entry_root_metadata(
    conn: sqlite3.Connection,
    entry_id: str,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    root = knowledge_root_identity(conn, str(entry_id or ""))
    occurrence = knowledge_occurrence_identity(conn, str(entry_id or ""))
    return (
        ((root,) if root else ()),
        ((occurrence,) if occurrence else ()),
    )


def _conversation_root_metadata(
    conn: sqlite3.Connection,
    *,
    guild_id: int,
    subject_key: str,
    source_row_id: int,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    root = knowledge_source_root_identity(
        guild_id=int(guild_id or 0),
        source_table="conversations",
        source_row_id=int(source_row_id or 0),
    )
    entry_row = conn.execute(
        """
        SELECT entry_id
        FROM main.memory_ledger_entries
        WHERE guild_id=? AND subject_key=?
          AND source_table='conversations' AND source_row_id=?
          AND lifecycle_status='active'
        ORDER BY
          CASE
            WHEN entry_type='observation'
             AND predicate_key='conversation' THEN 0
            ELSE 1
          END,
          entry_id
        LIMIT 1
        """,
        (
            int(guild_id or 0),
            str(subject_key or ""),
            str(int(source_row_id or 0)),
        ),
    ).fetchone()
    occurrence = (
        knowledge_occurrence_identity(conn, str(entry_row[0]))
        if entry_row and str(entry_row[0] or "")
        else ""
    )
    return (
        ((root,) if root else ()),
        ((occurrence,) if occurrence else ()),
    )


def _conversation_public_assessment_state(
    conn: sqlite3.Connection,
    *,
    guild_id: int,
    subject_key: str,
    source_row_id: int,
):
    """Resolve one exact retained conversation to its governed Open state."""

    rows = conn.execute(
        """
        SELECT entry_id
        FROM main.memory_ledger_entries
        WHERE guild_id=? AND subject_key=?
          AND source_table='conversations' AND source_row_id=?
          AND entry_type='observation' AND predicate_key='conversation'
          AND source_role='user' AND lifecycle_status='active'
        ORDER BY entry_id
        """,
        (int(guild_id or 0), str(subject_key or ""), str(source_row_id or 0)),
    ).fetchall()
    entry_ids = tuple(
        str(row[0] or "") for row in rows if str(row[0] or "")
    )
    if len(entry_ids) != 1:
        return None
    return read_public_assessment_root_state(
        conn,
        entry_id=entry_ids[0],
        guild_id=int(guild_id or 0),
        subject_key=str(subject_key or ""),
    )


def _moment_root_metadata(
    conn: sqlite3.Connection,
    *,
    moment_id: str,
    subject_key: str,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    try:
        rows = conn.execute(
            """
            SELECT DISTINCT source.ledger_entry_id
            FROM memory_moment_contribution_sources source
            JOIN main.memory_ledger_entries entry
              ON entry.entry_id=source.ledger_entry_id
            WHERE source.moment_id=? AND source.participant_key=?
              AND entry.subject_key=? AND entry.lifecycle_status='active'
              AND entry.derived=0 AND entry.projection=0
            ORDER BY source.ledger_entry_id
            """,
            (
                str(moment_id or ""),
                str(subject_key or ""),
                str(subject_key or ""),
            ),
        ).fetchall()
    except sqlite3.OperationalError:
        rows = ()
    roots = []
    occurrences = []
    for (entry_id,) in rows:
        root = knowledge_root_identity(conn, str(entry_id or ""))
        occurrence = knowledge_occurrence_identity(
            conn,
            str(entry_id or ""),
        )
        if root:
            roots.append(root)
        if occurrence:
            occurrences.append(occurrence)
    return (
        tuple(dict.fromkeys(roots)),
        tuple(dict.fromkeys(occurrences)),
    )


def _profile_member_item(
    request: IntelligencePacketRequest,
    item: IntelligencePacketItem,
) -> bool:
    base = bool(
        item.lane in _PROFILE_MEMBER_EVIDENCE_LANES
        and item.subject_key == _request_subject_key(request)
        and item.point_identity
        and item.root_identities
        and item.occurrence_identities
    )
    if not base:
        return False
    if item.lane not in {"assessment_observation", "conversation_context"}:
        return True
    return bool(
        item.point_group_identity
        and item.attribution_mode in {"subject_action", "authored_topic"}
        and item.polarity in {"affirmative", "negative", "conditional"}
        and item.action_identity
        and item.material_facets
    )


def _root_bearing_member_item(
    request: IntelligencePacketRequest,
    item: IntelligencePacketItem,
) -> bool:
    return bool(
        item.lane in _ROOT_COLLAPSE_MEMBER_LANES
        and item.subject_key == _request_subject_key(request)
        and item.root_identities
    )


def _broad_profile_request(value: str) -> bool:
    return classify_personal_recall_intent(value).broad_self_profile


def _request_is_broad_profile(
    request: IntelligencePacketRequest,
) -> bool:
    """Honor a trusted upstream route decision without rewriting its text."""

    return bool(
        request.broad_profile_intent
        or _broad_profile_request(request.user_text)
    )


def _profile_project_request(value: str) -> bool:
    return bool(_PROFILE_PROJECT_SCOPE_RE.search(str(value or "")))


def _profile_canon_anchor(
    request: IntelligencePacketRequest,
    candidates: Sequence[IntelligencePacketItem],
) -> IntelligencePacketItem | None:
    """Choose one canon point that best contextualizes selected public work."""

    if not _request_is_broad_profile(request):
        return None
    subject = _request_subject_key(request)
    recognized = tuple(
        item
        for item in candidates
        if item.lane == "canon"
        and item.source_type
        in {"recognized_canon_fact", "recognized_declared_canon_claim"}
        and item.subject_key == subject
    )
    if recognized:
        identity_comparison = bool(
            _IDENTITY_COMPARISON_REQUEST_RE.search(
                str(request.user_text or "")
            )
        )
        return sorted(
            recognized,
            key=lambda item: (
                -int(
                    identity_comparison
                    and item.source_type
                    == "recognized_declared_canon_claim"
                    and item.canon_claim_kind == "relationship"
                ),
                -item.score,
                -item.authority,
                item.source_ref,
            ),
        )[0]
    if not _profile_project_request(request.user_text):
        return None
    member_terms: set[str] = set()
    for item in candidates:
        if (
            item.subject_key != subject
            or item.lane not in _PROFILE_CANON_MATCH_LANES
        ):
            continue
        member_terms.update(_terms(item.text))
        for observation in item.supporting_observations:
            member_terms.update(_terms(observation))
    query = re.sub(
        r"^\s*(?:hey\s+|yo\s+|hi\s+)?"
        r"(?:bnl(?:-?01)?|barcode bot)\s*[,;:—-]*\s*",
        "",
        str(request.user_text or ""),
        flags=re.I,
    )
    request_terms = _terms(query)
    canon_items = tuple(
        item
        for item in candidates
        if item.lane == "canon"
        and request_terms.intersection(_terms(item.text))
    )
    if not canon_items:
        return None
    umbrella_barcode_request = bool(
        "barcode" in request_terms
        and not request_terms.intersection(
            {"radio", "broadcast", "show", "schedule", "website", "site"}
        )
    )
    return sorted(
        canon_items,
        key=lambda item: (
            -int(
                umbrella_barcode_request
                and item.predicate_key == "origin"
            ),
            -len(member_terms & _terms(item.text)),
            -len(request_terms & _terms(item.text)),
            -len(_terms(item.text)),
            item.source_ref,
        ),
    )[0]


def _public_route(request: IntelligencePacketRequest) -> bool:
    return request.channel_policy in _PUBLIC_POLICIES or request.visibility_allowance in {
        "public",
        "public_safe",
    }


def _visibility_for_policy(policy: str) -> str:
    return {
        "public_home": "public",
        "public_context": "public",
        "public_selective": "public_safe",
        "sealed_test": "sealed_test",
        "internal_controlled": "internal",
        "reference_canon": "reference_canon",
    }.get(str(policy or "").lower(), "unknown")


def _route_allows_item(
    request: IntelligencePacketRequest,
    item: IntelligencePacketItem,
) -> bool:
    if item.lane == "relationship_posture":
        return bool(
            item.visibility == "private"
            and item.usage == "tone_only"
            and request.direct_state == "direct"
            and request.channel_policy in _PUBLIC_POLICIES
        )
    if _public_route(request):
        return item.visibility in _PUBLIC_VISIBILITIES
    return item.visibility in _INTERNAL_VISIBILITIES


def _relevant(
    *,
    request_terms: set[str],
    broad: bool,
    lane: str,
    text: str,
    predicate_key: str = "",
    tags: Sequence[str] = (),
) -> bool:
    if lane in {"current_intent", "conversation_context", "relationship_posture"}:
        return True
    if broad and lane in {
        "approved_fact",
        "moment",
        "atomic_knowledge",
        "open_loop",
    }:
        return True
    if lane == "open_loop":
        return True
    candidate_terms = (
        _terms(text)
        | _terms(predicate_key.replace("_", " "))
        | set(tags or ())
    )
    return bool(request_terms & candidate_terms)


def _add_exclusion(
    diagnostics: IntelligencePacketDiagnostics,
    exclusions: list[IntelligencePacketExclusion],
    *,
    lane: str,
    reason: str,
    source_class: str = "",
) -> None:
    exclusions.append(
        IntelligencePacketExclusion(
            lane=lane,
            reason=reason,
            source_class=source_class,
        )
    )
    diagnostics.excluded_by_reason[reason] = (
        diagnostics.excluded_by_reason.get(reason, 0) + 1
    )


def ensure_schema(conn: sqlite3.Connection) -> None:
    ensure_governance_schema(conn)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS memory_governance_intelligence_packet_runs (
            run_id TEXT PRIMARY KEY,
            packet_id TEXT NOT NULL,
            schema_version TEXT NOT NULL,
            guild_id INTEGER NOT NULL,
            subject_hash TEXT NOT NULL,
            route_mode TEXT NOT NULL,
            channel_policy TEXT NOT NULL,
            visibility_allowance TEXT NOT NULL,
            item_count INTEGER NOT NULL DEFAULT 0,
            validation_item_count INTEGER NOT NULL DEFAULT 0,
            validation_lane_counts_json TEXT NOT NULL DEFAULT '{}',
            selected_lane_counts_json TEXT NOT NULL DEFAULT '{}',
            source_class_counts_json TEXT NOT NULL DEFAULT '{}',
            atomic_state_counts_json TEXT NOT NULL DEFAULT '{}',
            excluded_by_reason_json TEXT NOT NULL DEFAULT '{}',
            missing_lanes_json TEXT NOT NULL DEFAULT '[]',
            conflict_count INTEGER NOT NULL DEFAULT 0,
            visibility_exclusion_count INTEGER NOT NULL DEFAULT 0,
            budget_exclusion_count INTEGER NOT NULL DEFAULT 0,
            duplicate_suppression_count INTEGER NOT NULL DEFAULT 0,
            processing_error_count INTEGER NOT NULL DEFAULT 0,
            invalid_invariant_count INTEGER NOT NULL DEFAULT 0,
            revalidation_status TEXT NOT NULL DEFAULT 'not_evaluated',
            revalidation_changed_count INTEGER NOT NULL DEFAULT 0,
            packet_digest TEXT NOT NULL,
            source_ref_digest TEXT NOT NULL,
            prompt_applied INTEGER NOT NULL DEFAULT 0,
            live_applied INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL
        )
        """
    )
    for column, definition in (
        ("validation_item_count", "INTEGER NOT NULL DEFAULT 0"),
        (
            "validation_lane_counts_json",
            "TEXT NOT NULL DEFAULT '{}'",
        ),
        ("root_collapse_suppression_count", "INTEGER NOT NULL DEFAULT 0"),
        ("shared_root_projection_count", "INTEGER NOT NULL DEFAULT 0"),
        (
            "profile_sufficiency_status",
            "TEXT NOT NULL DEFAULT 'not_applicable'",
        ),
        ("profile_sufficiency_met", "INTEGER NOT NULL DEFAULT 0"),
        ("profile_required_point_count", "INTEGER NOT NULL DEFAULT 0"),
        ("profile_selected_point_count", "INTEGER NOT NULL DEFAULT 0"),
        ("profile_candidate_point_count", "INTEGER NOT NULL DEFAULT 0"),
        ("profile_independent_root_count", "INTEGER NOT NULL DEFAULT 0"),
        (
            "profile_independent_occurrence_count",
            "INTEGER NOT NULL DEFAULT 0",
        ),
        ("profile_reason_codes_json", "TEXT NOT NULL DEFAULT '[]'"),
        ("frame_revision", "TEXT NOT NULL DEFAULT ''"),
        ("frame_input_digest", "TEXT NOT NULL DEFAULT ''"),
        (
            "subject_resolution_status",
            "TEXT NOT NULL DEFAULT 'not_evaluated'",
        ),
        (
            "subject_resolution_method",
            "TEXT NOT NULL DEFAULT 'none'",
        ),
        ("subject_resolution_candidate_count", "INTEGER NOT NULL DEFAULT 0"),
        (
            "frame_applicability_exclusion_count",
            "INTEGER NOT NULL DEFAULT 0",
        ),
        ("source_snapshot_digest", "TEXT NOT NULL DEFAULT ''"),
    ):
        try:
            conn.execute(
                "ALTER TABLE memory_governance_intelligence_packet_runs "
                "ADD COLUMN %s %s" % (column, definition)
            )
        except sqlite3.OperationalError:
            pass
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_intelligence_packet_guild
        ON memory_governance_intelligence_packet_runs(guild_id,created_at)
        """
    )


def _conversation_row(
    conn: sqlite3.Connection,
    source_id: int,
) -> dict[str, Any]:
    columns = {
        str(row[1])
        for row in conn.execute(
            "PRAGMA main.table_info(conversations)"
        ).fetchall()
    }
    required = {
        "id",
        "guild_id",
        "user_id",
        "user_name",
        "role",
        "content",
        "channel_id",
        "channel_policy",
        "timestamp",
    }
    if not required.issubset(columns):
        return {}
    row = conn.execute(
        """
        SELECT id,guild_id,user_id,user_name,role,content,channel_id,
               channel_policy,timestamp
        FROM main.conversations
        WHERE id=?
        """,
        (int(source_id or 0),),
    ).fetchone()
    keys = (
        "id",
        "guild_id",
        "user_id",
        "user_name",
        "role",
        "content",
        "channel_id",
        "channel_policy",
        "timestamp",
    )
    return dict(zip(keys, row)) if row else {}


def _conversation_digest(row: Mapping[str, Any]) -> str:
    return _digest(
        "conversation",
        {
            key: row.get(key)
            for key in (
                "id",
                "guild_id",
                "user_id",
                "role",
                "content",
                "channel_id",
                "channel_policy",
                "timestamp",
            )
        },
    )


def _conversation_items(
    conn: sqlite3.Connection,
    request: IntelligencePacketRequest,
    diagnostics: IntelligencePacketDiagnostics,
    exclusions: list[IntelligencePacketExclusion],
) -> list[IntelligencePacketItem]:
    items: list[IntelligencePacketItem] = []
    for evidence in request.conversation_evidence:
        text = re.sub(r"\s+", " ", str(evidence.text or "")).strip()
        if not text:
            continue
        lane = "current_intent" if evidence.current_turn else "conversation_context"
        diagnostics.candidates_by_lane[lane] = (
            diagnostics.candidates_by_lane.get(lane, 0) + 1
        )
        if int(evidence.source_id or 0) > 0:
            row = _conversation_row(conn, int(evidence.source_id))
            if not row:
                _add_exclusion(
                    diagnostics,
                    exclusions,
                    lane=lane,
                    reason="conversation_source_missing",
                    source_class=SourceClass.PUBLIC_OBSERVATION.value,
                )
                continue
            if (
                int(row.get("guild_id") or 0) != int(request.guild_id or 0)
                or str(row.get("role") or "").lower() != "user"
                or (
                    int(evidence.speaker_user_id or 0) > 0
                    and int(row.get("user_id") or 0)
                    != int(evidence.speaker_user_id or 0)
                )
            ):
                _add_exclusion(
                    diagnostics,
                    exclusions,
                    lane=lane,
                    reason="conversation_identity_scope",
                    source_class=SourceClass.PUBLIC_OBSERVATION.value,
                )
                continue
            policy = str(row.get("channel_policy") or "unknown")
            row_subject = subject_key_for_user(
                int(row.get("user_id") or 0)
            )
            roots, occurrences = _conversation_root_metadata(
                conn,
                guild_id=int(row.get("guild_id") or 0),
                subject_key=row_subject,
                source_row_id=int(row.get("id") or 0),
            )
            assessment_state = (
                None
                if evidence.current_turn
                else _conversation_public_assessment_state(
                    conn,
                    guild_id=int(row.get("guild_id") or 0),
                    subject_key=row_subject,
                    source_row_id=int(row.get("id") or 0),
                )
            )
            if assessment_state is not None:
                roots = (assessment_state.root_identity,)
                occurrences = (assessment_state.occurrence_identity,)
            authoritative_text = (
                assessment_state.text
                if assessment_state is not None
                else str(row.get("content") or "")
            )
            source_digest = (
                assessment_state.source_digest
                if assessment_state is not None
                else _conversation_digest(row)
            )
            point_identity = (
                assessment_state.semantics.point_identity
                if assessment_state is not None
                else _point_identity(
                    subject_key=row_subject,
                    predicate_key=(
                        "current_intent"
                        if evidence.current_turn
                        else "conversation_context"
                    ),
                    text=authoritative_text,
                )
            )
            item = IntelligencePacketItem(
                lane=lane,
                source_class=SourceClass.PUBLIC_OBSERVATION.value,
                source_type="conversation_row",
                source_ref="conversation:%s" % int(row["id"]),
                source_digest=source_digest,
                subject_key=row_subject,
                predicate_key="current_intent" if evidence.current_turn else "conversation_context",
                text=authoritative_text[:1200],
                visibility=_visibility_for_policy(policy),
                confidence=Confidence.HIGH.value,
                lifecycle="current" if evidence.current_turn else "active",
                authority=_AUTHORITY_RANK[SourceClass.PUBLIC_OBSERVATION.value],
                participants=(
                    subject_key_for_user(int(row.get("user_id") or 0)),
                ),
                observed_at=str(row.get("timestamp") or ""),
                usage="current_intent" if evidence.current_turn else "continuity",
                score=100.0 if evidence.current_turn else 88.0,
                revalidation_kind=(
                    "public_assessment"
                    if assessment_state is not None
                    else "conversation"
                ),
                revalidation_key=(
                    assessment_state.entry_id
                    if assessment_state is not None
                    else str(int(row["id"]))
                ),
                root_identities=roots,
                occurrence_identities=occurrences,
                point_identity=point_identity,
                point_group_identity=(
                    assessment_state.semantics.point_identity
                    if assessment_state is not None
                    else ""
                ),
                attribution_mode=(
                    assessment_state.semantics.attribution_mode
                    if assessment_state is not None
                    else ""
                ),
                polarity=(
                    assessment_state.semantics.polarity
                    if assessment_state is not None
                    else ""
                ),
                action_identity=(
                    assessment_state.semantics.action_identity
                    if assessment_state is not None
                    else ""
                ),
                material_facets=(
                    assessment_state.semantics.material_facets
                    if assessment_state is not None
                    else ()
                ),
            )
        else:
            speaker_id = int(evidence.speaker_user_id or request.subject_user_id or 0)
            source_digest = _digest(
                "current_exchange",
                request.guild_id,
                speaker_id,
                text,
                request.channel_id,
                request.channel_policy,
            )
            item = IntelligencePacketItem(
                lane=lane,
                source_class=SourceClass.RUNTIME_OBSERVATION.value,
                source_type="current_exchange",
                source_ref="current:%s" % source_digest[:24],
                source_digest=source_digest,
                subject_key=subject_key_for_user(speaker_id),
                predicate_key="current_intent",
                text=text[:1200],
                visibility=_visibility_for_policy(request.channel_policy),
                confidence=Confidence.HIGH.value,
                lifecycle="current",
                authority=_AUTHORITY_RANK[SourceClass.RUNTIME_OBSERVATION.value],
                participants=(subject_key_for_user(speaker_id),) if speaker_id else (),
                observed_at=request.now or _now(),
                usage="current_intent",
                score=100.0,
                revalidation_kind="current",
                revalidation_key=source_digest,
                point_identity=_point_identity(
                    subject_key=subject_key_for_user(speaker_id),
                    predicate_key="current_intent",
                    text=text,
                ),
            )
        if not _route_allows_item(request, item):
            diagnostics.visibility_exclusions += 1
            _add_exclusion(
                diagnostics,
                exclusions,
                lane=lane,
                reason="conversation_visibility",
                source_class=item.source_class,
            )
            continue
        items.append(item)
    return items


def _ledger_entry_digest(
    conn: sqlite3.Connection,
    entry_id: str,
) -> str:
    columns = {
        str(row[1])
        for row in conn.execute(
            "PRAGMA main.table_info(memory_ledger_entries)"
        ).fetchall()
    }
    if not columns or "entry_id" not in columns:
        return ""
    selected_columns = tuple(
        column
        for column in (
            "entry_id",
            "guild_id",
            "subject_key",
            "entry_type",
            "predicate_key",
            "normalized_value",
            "source_table",
            "source_row_id",
            "source_revision",
            "source_role",
            "source_class",
            "source_sequence",
            "channel_id",
            "route_mode",
            "channel_policy",
            "visibility",
            "confidence",
            "public_usable",
            "derived",
            "projection",
            "observed_at",
            "lifecycle_status",
            "updated_at",
        )
        if column in columns
    )
    row = conn.execute(
        "SELECT %s FROM main.memory_ledger_entries WHERE entry_id=?"
        % ",".join(selected_columns),
        (entry_id,),
    ).fetchone()
    if not row:
        return ""
    data = dict(zip(selected_columns, row))
    lineage = conn.execute(
        """
        SELECT lineage_type,target_entry_id
        FROM main.memory_ledger_lineage
        WHERE guild_id=? AND entry_id=?
        ORDER BY lineage_type,target_entry_id
        """,
        (int(data.get("guild_id") or 0), entry_id),
    ).fetchall()
    incoming = conn.execute(
        """
        SELECT entry_id,lineage_type
        FROM main.memory_ledger_lineage
        WHERE guild_id=? AND target_entry_id=?
          AND lineage_type IN ('correction_of','supersedes','retracts')
        ORDER BY entry_id,lineage_type
        """,
        (int(data.get("guild_id") or 0), entry_id),
    ).fetchall()
    return _digest("ledger", data, lineage, incoming)


def _assessment_observation_items_in_snapshot(
    conn: sqlite3.Connection,
    request: IntelligencePacketRequest,
    diagnostics: IntelligencePacketDiagnostics,
    exclusions: list[IntelligencePacketExclusion],
    *,
    broad: bool,
) -> list[IntelligencePacketItem]:
    """Build ephemeral question-scoped items from all eligible public roots."""

    if not broad or int(request.subject_user_id or 0) <= 0:
        return []
    subject = _request_subject_key(request)
    selection = select_public_conversation_assessment_evidence(
        conn,
        guild_id=int(request.guild_id or 0),
        subject_key=subject,
        request_text=str(request.user_text or ""),
        max_results=_VALIDATION_SUPPORT_ASSESSMENT_LIMIT,
    )
    diagnostics.candidates_by_lane["assessment_observation"] = int(
        selection.eligible_count or 0
    )
    not_selected = max(
        0,
        int(selection.eligible_count or 0) - len(selection.items),
    )
    if not_selected:
        diagnostics.excluded_by_reason[
            "assessment_pool_not_selected"
        ] = not_selected

    items: list[IntelligencePacketItem] = []
    relevance_required = public_assessment_relevance_required(
        request.user_text
    )
    for evidence in selection.items:
        entry_id = str(evidence.entry_id or "")
        state = read_public_assessment_root_state(
            conn,
            entry_id=entry_id,
            guild_id=int(request.guild_id or 0),
            subject_key=subject,
        )
        if not entry_id or state is None:
            _add_exclusion(
                diagnostics,
                exclusions,
                lane="assessment_observation",
                reason="assessment_selector_source_mismatch",
                source_class=SourceClass.PUBLIC_OBSERVATION.value,
            )
            continue
        adapted = adapt_open_signal_claim(evidence)
        adapted_claim = adapted.claim
        expected_source_ref = "memory_ledger:%s" % entry_id
        if not (
            adapted_claim is not None
            and adapted_claim.subject_id == subject
            and adapted_claim.source_refs == (expected_source_ref,)
            and adapted_claim.root_ids == (state.root_identity,)
            and adapted_claim.occurrence_ids
            == (state.occurrence_identity,)
        ):
            _add_exclusion(
                diagnostics,
                exclusions,
                lane="assessment_observation",
                reason=(
                    "assessment_selector_%s" % str(adapted.reason or "invalid")
                ),
                source_class=SourceClass.PUBLIC_OBSERVATION.value,
            )
            continue
        if relevance_required and not bool(evidence.request_relevant):
            _add_exclusion(
                diagnostics,
                exclusions,
                lane="assessment_observation",
                reason="assessment_question_irrelevant",
                source_class=SourceClass.PUBLIC_OBSERVATION.value,
            )
            continue
        if not (
            state.text == str(evidence.text or "")
            and state.observed_at == str(evidence.observed_at or "")
            and state.channel_policy == str(evidence.channel_policy or "")
            and state.route_mode == str(evidence.route_mode or "")
            and state.source_role == str(evidence.source_role or "")
            and state.source_class == str(evidence.source_class or "")
            and state.visibility == str(evidence.visibility or "")
            and state.public_usable == bool(evidence.public_usable)
            and state.derived == bool(evidence.derived)
            and state.projection == bool(evidence.projection)
            and state.lifecycle_status == str(evidence.lifecycle_status or "")
            and state.root_identity == str(evidence.root_identity or "")
            and state.occurrence_identity
            == str(evidence.occurrence_identity or "")
            and state.source_digest == str(evidence.source_digest or "")
            and state.semantics.point_identity
            == str(evidence.point_identity or "")
            and state.semantics.attribution_mode
            == str(evidence.attribution_mode or "")
            and state.semantics.polarity == str(evidence.polarity or "")
            and state.semantics.action_identity
            == str(evidence.action_identity or "")
            and state.semantics.material_facets
            == tuple(evidence.material_facets)
        ):
            _add_exclusion(
                diagnostics,
                exclusions,
                lane="assessment_observation",
                reason="assessment_selector_source_mismatch",
                source_class=SourceClass.PUBLIC_OBSERVATION.value,
            )
            continue
        item = IntelligencePacketItem(
            lane="assessment_observation",
            source_class=SourceClass.PUBLIC_OBSERVATION.value,
            source_type="public_assessment_observation",
            source_ref="ledger:%s" % entry_id,
            source_digest=state.source_digest,
            subject_key=subject,
            predicate_key="public_assessment_observation",
            text=state.text[:240],
            visibility=state.visibility,
            confidence=adapted_claim.confidence.value,
            lifecycle="active",
            authority=_AUTHORITY_RANK[
                SourceClass.PUBLIC_OBSERVATION.value
            ],
            participants=(subject,),
            lineage=(entry_id,),
            observed_at=state.observed_at,
            usage="assessment_only",
            score=(
                92.0
                + min(24.0, float(evidence.score or 0.0))
                + (12.0 if evidence.request_relevant else 0.0)
            ),
            revalidation_kind="public_assessment",
            revalidation_key=entry_id,
            root_identities=(state.root_identity,),
            occurrence_identities=(state.occurrence_identity,),
            point_identity=state.semantics.point_identity,
            point_group_identity=state.semantics.point_identity,
            attribution_mode=state.semantics.attribution_mode,
            polarity=state.semantics.polarity,
            action_identity=state.semantics.action_identity,
            material_facets=state.semantics.material_facets,
            canon_status=adapted_claim.canon_status.value,
            canon_domain=adapted_claim.domain.value,
            canon_claim_kind=adapted_claim.claim_kind.value,
        )
        if not _route_allows_item(request, item):
            diagnostics.visibility_exclusions += 1
            _add_exclusion(
                diagnostics,
                exclusions,
                lane="assessment_observation",
                reason="assessment_visibility",
                source_class=item.source_class,
            )
            continue
        items.append(item)
    return items


def _assessment_observation_items(
    conn: sqlite3.Connection,
    request: IntelligencePacketRequest,
    diagnostics: IntelligencePacketDiagnostics,
    exclusions: list[IntelligencePacketExclusion],
    *,
    broad: bool,
) -> list[IntelligencePacketItem]:
    """Read selector DTOs and their source rows in one coherent snapshot."""

    owns_snapshot = not conn.in_transaction
    if owns_snapshot:
        conn.execute("BEGIN")
    try:
        return _assessment_observation_items_in_snapshot(
            conn,
            request,
            diagnostics,
            exclusions,
            broad=broad,
        )
    finally:
        if owns_snapshot and conn.in_transaction:
            # This owner performs no writes. Rollback ends the read snapshot
            # without committing unrelated work that a future caller might
            # accidentally add here.
            conn.rollback()


def _governed_items(
    conn: sqlite3.Connection,
    request: IntelligencePacketRequest,
    diagnostics: IntelligencePacketDiagnostics,
    exclusions: list[IntelligencePacketExclusion],
    *,
    broad: bool,
) -> list[IntelligencePacketItem]:
    if int(request.subject_user_id or 0) <= 0:
        return []
    gov_request = GovernanceRequest(
        guild_id=int(request.guild_id or 0),
        subject_user_id=int(request.subject_user_id or 0),
        route_mode=request.route_mode,
        conversation_surface="unified_intelligence_packet_shadow",
        channel_id=int(request.channel_id or 0),
        channel_name=request.channel_name,
        channel_policy=request.channel_policy,
        visibility_allowance=request.visibility_allowance,
        user_text=request.user_text,
        participants=tuple(
            subject_key_for_user(user_id)
            for user_id in request.participant_user_ids
            if int(user_id or 0) > 0
        ),
        direct_state=request.direct_state,
        budget_chars=min(max(int(request.budget_chars or 2400), 400), 6000),
        allowed_source_classes=(
            "owner_correction",
            "approved_canon",
            "first_party_record",
            "runtime_observation",
            "public_observation",
            "moment_gist",
            "evidence_projection",
            "derived_summary",
        ),
        now=request.now or _now(),
        broad_recall=broad,
    )
    result = build_governed_context(
        conn,
        gov_request,
        legacy_context="",
        include_review_moments=True,
        include_public_moment_gists=True,
    )
    safety = assess_governance_result_safety(result)
    if safety.processing_errors:
        diagnostics.processing_errors.extend(
            "governance:%s" % error for error in safety.processing_errors
        )
    if safety.blocking_invariants:
        diagnostics.invalid_invariants.extend(safety.blocking_invariants)
        return []
    for reason, count in result.diagnostics.excluded_by_reason.items():
        diagnostics.excluded_by_reason[
            "governance:%s" % reason
        ] = diagnostics.excluded_by_reason.get(
            "governance:%s" % reason,
            0,
        ) + int(count or 0)
    items: list[IntelligencePacketItem] = []
    for candidate in result.selected:
        lane = (
            "moment"
            if candidate.source_class == "moment_gist"
            else "open_loop"
            if candidate.predicate_key
            in {"open_loop", "commitment", "goal", "unresolved_question"}
            else "approved_fact"
        )
        diagnostics.candidates_by_lane[lane] = (
            diagnostics.candidates_by_lane.get(lane, 0) + 1
        )
        if candidate.source_class == "moment_gist":
            moment_id = candidate.source_ref.removeprefix("moment:")
            source_digest = _digest(
                "moment",
                candidate.source_ref,
                candidate.entry_id,
                candidate.text,
                candidate.visibility,
                candidate.observed_at,
            )
            revalidation_kind = "moment"
            revalidation_key = moment_id
            roots, occurrences = _moment_root_metadata(
                conn,
                moment_id=moment_id,
                subject_key=candidate.subject_key,
            )
        else:
            source_digest = _ledger_entry_digest(conn, candidate.entry_id)
            revalidation_kind = "ledger"
            revalidation_key = candidate.entry_id
            roots, occurrences = _entry_root_metadata(
                conn,
                candidate.entry_id,
            )
        item = IntelligencePacketItem(
            lane=lane,
            source_class=candidate.source_class,
            source_type=candidate.source_type,
            source_ref=candidate.source_ref,
            source_digest=source_digest,
            subject_key=candidate.subject_key,
            predicate_key=candidate.predicate_key,
            text=candidate.text[:1000],
            visibility=candidate.visibility,
            confidence=candidate.confidence,
            lifecycle=candidate.lifecycle,
            authority=int(candidate.authority or 0),
            participants=tuple(candidate.participants or ()),
            lineage=tuple(
                "%s:%s" % (lineage_type, target)
                for lineage_type, target in candidate.lineage
            ),
            observed_at=candidate.observed_at,
            usage="episodic_gist" if lane == "moment" else "content",
            score=float(candidate.score or 0) + {
                "approved_fact": 82.0,
                "open_loop": 76.0,
                "moment": 72.0,
            }[lane],
            revalidation_kind=revalidation_kind,
            revalidation_key=revalidation_key,
            root_identities=roots,
            occurrence_identities=occurrences,
            point_identity=_point_identity(
                subject_key=candidate.subject_key,
                predicate_key=candidate.predicate_key,
                text=candidate.text,
            ),
        )
        if not source_digest:
            _add_exclusion(
                diagnostics,
                exclusions,
                lane=lane,
                reason="governed_source_unversioned",
                source_class=item.source_class,
            )
            continue
        if (
            broad
            and lane in _PROFILE_MEMBER_EVIDENCE_LANES
            and (not roots or not occurrences)
        ):
            _add_exclusion(
                diagnostics,
                exclusions,
                lane=lane,
                reason="member_evidence_missing_root_lineage",
                source_class=item.source_class,
            )
            continue
        if not _route_allows_item(request, item):
            diagnostics.visibility_exclusions += 1
            _add_exclusion(
                diagnostics,
                exclusions,
                lane=lane,
                reason="governed_visibility",
                source_class=item.source_class,
            )
            continue
        items.append(item)
    return items


def _declared_items(
    conn: sqlite3.Connection,
    request: IntelligencePacketRequest,
    diagnostics: IntelligencePacketDiagnostics,
    exclusions: list[IntelligencePacketExclusion],
    *,
    broad: bool,
    request_terms: set[str],
    environ: Mapping[str, str] | None = None,
) -> list[IntelligencePacketItem]:
    """Normalize current Declared claims only for the effective PR 5 owner."""

    governed_subject_scope = bool(
        str(request.frame_subject_requirement or "").strip().lower()
        == "required"
        and _request_subject_key(request)
    )
    if (
        not (broad or governed_subject_scope)
        or not bool(request.declared_canon_authorized)
    ):
        return []
    selection = select_declared_canon_claims_for_packet(
        conn,
        guild_id=int(request.guild_id or 0),
        route_mode=request.route_mode,
        channel_policy=request.channel_policy,
        capability_authorized=True,
        limit=200,
        now=request.now or None,
    )
    if selection.reason not in {"eligible", "no_eligible_claims"}:
        count = max(1, int(selection.candidate_count or 0))
        diagnostics.candidates_by_canon_status[CanonStatus.DECLARED.value] = (
            diagnostics.candidates_by_canon_status.get(
                CanonStatus.DECLARED.value,
                0,
            )
            + count
        )
        diagnostics.excluded_by_reason[
            "declared_%s" % selection.reason
        ] = diagnostics.excluded_by_reason.get(
            "declared_%s" % selection.reason,
            0,
        ) + count
        return []

    subject = _request_subject_key(request)
    binding_signal = _canon_identity_signal(
        conn,
        request,
        environ=environ,
    )
    bound_entity_id = (
        binding_signal.subject.key
        if binding_signal.subject is not None
        and binding_signal.status
        in {"recognized", "bound_non_signal_identity"}
        else ""
    )
    items: list[IntelligencePacketItem] = []
    for claim in selection.claims:
        member_claim = claim.subject_id == subject
        bound_entity_claim = bool(
            bound_entity_id
            and (
                claim.subject_id == bound_entity_id
                or (
                    claim.claim_kind.value == "relationship"
                    and isinstance(claim.value, Mapping)
                    and str(claim.value.get("object_subject_id") or "")
                    == bound_entity_id
                )
            )
        )
        lane = "approved_fact" if member_claim else "canon"
        text = _declared_claim_text(claim).strip()
        diagnostics.candidates_by_lane[lane] = (
            diagnostics.candidates_by_lane.get(lane, 0) + 1
        )
        if not text or not claim.root_ids or not claim.occurrence_ids:
            _add_exclusion(
                diagnostics,
                exclusions,
                lane=lane,
                reason="declared_source_lineage_missing",
                source_class=claim.source_class.value,
            )
            continue
        if not member_claim and not bound_entity_claim and not _relevant(
            request_terms=request_terms,
            broad=broad,
            lane=lane,
            text=text,
            predicate_key=claim.predicate,
            tags=tuple(
                _terms(claim.subject_id)
                | _terms(claim.domain.value)
            ),
        ):
            _add_exclusion(
                diagnostics,
                exclusions,
                lane=lane,
                reason="declared_topic_relevance",
                source_class=claim.source_class.value,
            )
            continue
        item = IntelligencePacketItem(
            lane=lane,
            source_class=claim.source_class.value,
            source_type=(
                "recognized_declared_canon_claim"
                if bound_entity_claim
                else "declared_canon_claim"
            ),
            source_ref=claim.source_refs[0],
            source_digest=claim.revision_id,
            subject_key=(subject if bound_entity_claim else claim.subject_id),
            predicate_key=claim.predicate,
            text=text[:1000],
            visibility=claim.visibility.value,
            confidence=claim.confidence.value,
            lifecycle=claim.lifecycle.value,
            authority=_AUTHORITY_RANK.get(claim.source_class.value, 0),
            participants=(
                (subject,) if member_claim or bound_entity_claim else ()
            ),
            lineage=claim.source_refs,
            observed_at=claim.valid_from,
            usage="content",
            score=112.0 if member_claim else 94.0,
            revalidation_kind="declared",
            revalidation_key=claim.claim_id,
            root_identities=claim.root_ids,
            occurrence_identities=claim.occurrence_ids,
            point_identity=(
                _point_identity(
                    subject_key=subject,
                    predicate_key=claim.predicate,
                    text=text,
                )
                if member_claim
                else ""
            ),
            canon_status=claim.canon_status.value,
            canon_domain=claim.domain.value,
            canon_claim_kind=claim.claim_kind.value,
        )
        if not _route_allows_item(request, item):
            diagnostics.visibility_exclusions += 1
            _add_exclusion(
                diagnostics,
                exclusions,
                lane=lane,
                reason="declared_visibility",
                source_class=item.source_class,
            )
            continue
        items.append(item)
    return items


def _canon_entity_name(subject_id: str) -> str:
    subject = next(
        (
            candidate
            for candidate in CANON_ENTITY_IDENTITIES
            if candidate.key == str(subject_id or "")
        ),
        None,
    )
    return subject.name if subject is not None else str(subject_id or "")


def _mentioned_canon_entity_keys(value: str) -> frozenset[str]:
    lowered = str(value or "").casefold()
    return frozenset(
        subject.key
        for subject in CANON_ENTITY_IDENTITIES
        if any(
            re.search(r"\b%s\b" % re.escape(label.casefold()), lowered)
            for label in (subject.name, *subject.aliases)
            if label
        )
    )


def _declared_claim_text(claim: Any) -> str:
    """Render typed relationships without leaking adapter field names."""

    if claim.claim_kind.value != "relationship" or not isinstance(
        claim.value,
        Mapping,
    ):
        return _canon_value(claim.value)
    subject_name = _canon_entity_name(claim.subject_id)
    object_id = str(claim.value.get("object_subject_id") or "")
    object_name = _canon_entity_name(object_id)
    relationship_value = _canon_value(claim.value.get("value"))
    predicate = str(claim.predicate or "relationship").replace("_", " ")
    return "%s %s %s: %s" % (
        subject_name,
        predicate,
        object_name,
        relationship_value,
    )


def _atomic_candidate_row(
    conn: sqlite3.Connection,
    candidate_id: str,
) -> dict[str, Any]:
    columns = (
        "candidate_id",
        "guild_id",
        "candidate_type",
        "subject_key",
        "predicate_key",
        "normalized_value",
        "value_digest",
        "epistemic_status",
        "currentness",
        "candidate_state",
        "visibility",
        "authority_class",
        "confidence_class",
        "route_scope_json",
        "retrieval_tags_json",
        "candidate_eligible",
        "live_eligible",
        "invalidated_reason",
        "invalidated_at",
        "lifecycle_schema_version",
        "consolidation_id",
        "canonical_candidate_id",
        "eligible_independent_root_count",
        "independent_root_count",
        "reinforcement_count",
        "conflict_value_count",
        "consolidated_authority_class",
        "consolidated_confidence_class",
        "lifecycle_support_digest",
        "lifecycle_reason",
        "review_status",
        "review_due_at",
        "lifecycle_evaluated_at",
        "last_seen_at",
        "recurrence_contract_version",
        "grouping_signature_version",
        "grouping_identity",
        "canon_domain",
        "canon_claim_kind",
        "independent_occurrence_count",
        "occurrence_ids_json",
        "occurrence_digest",
        "root_digest",
        "recurrence_proof_json",
        "public_usable",
    )
    row = conn.execute(
        "SELECT %s FROM main.memory_ledger_knowledge_candidates WHERE candidate_id=?"
        % ",".join(columns),
        (candidate_id,),
    ).fetchone()
    return dict(zip(columns, row)) if row else {}


def _atomic_root_snapshot(
    conn: sqlite3.Connection,
    candidate_id: str,
) -> tuple[dict[str, Any], ...]:
    candidate = _atomic_candidate_row(conn, candidate_id)
    consolidation_id = str(candidate.get("consolidation_id") or "")
    rows = conn.execute(
        """
        SELECT DISTINCT r.root_entry_id,r.root_status,r.is_independent,
               e.guild_id,e.subject_key,e.source_class,e.source_table,
               e.source_row_id,e.source_revision,e.source_role,
               e.route_mode,e.channel_id,e.channel_name,e.channel_policy,
               e.visibility,e.public_usable,e.derived,e.projection,
               e.lifecycle_status,e.updated_at,e.normalized_value,
               e.predicate_key,e.observed_at,e.source_sequence
        FROM main.memory_ledger_knowledge_roots r
        JOIN main.memory_ledger_knowledge_candidates c
          ON c.candidate_id=r.candidate_id
        LEFT JOIN main.memory_ledger_entries e ON e.entry_id=r.root_entry_id
        WHERE r.is_independent=1 AND r.root_status='eligible'
          AND (
            (?<>'' AND c.consolidation_id=?)
            OR (?='' AND r.candidate_id=?)
          )
        ORDER BY r.root_entry_id
        """,
        (
            consolidation_id,
            consolidation_id,
            consolidation_id,
            candidate_id,
        ),
    ).fetchall()
    keys = (
        "root_entry_id",
        "root_status",
        "is_independent",
        "guild_id",
        "subject_key",
        "source_class",
        "source_table",
        "source_row_id",
        "source_revision",
        "source_role",
        "route_mode",
        "channel_id",
        "channel_name",
        "channel_policy",
        "visibility",
        "public_usable",
        "derived",
        "projection",
        "lifecycle_status",
        "updated_at",
        "normalized_value",
        "predicate_key",
        "observed_at",
        "source_sequence",
    )
    return tuple(dict(zip(keys, row)) for row in rows)


def _atomic_candidate_digest(
    conn: sqlite3.Connection,
    candidate_id: str,
) -> str:
    candidate = _atomic_candidate_row(conn, candidate_id)
    if not candidate:
        return ""
    stable_candidate = dict(candidate)
    stable_candidate.pop("lifecycle_evaluated_at", None)
    roots = _atomic_root_snapshot(conn, candidate_id)
    incoming = []
    for root in roots:
        incoming.extend(
            conn.execute(
                """
                SELECT entry_id,lineage_type
                FROM main.memory_ledger_lineage
                WHERE guild_id=? AND target_entry_id=?
                  AND lineage_type IN ('correction_of','supersedes','retracts')
                ORDER BY entry_id,lineage_type
                """,
                (
                    int(root.get("guild_id") or 0),
                    str(root.get("root_entry_id") or ""),
                ),
            ).fetchall()
        )
    return _digest("atomic", stable_candidate, roots, sorted(incoming))


def _living_atomic_candidate_digest(
    conn: sqlite3.Connection,
    candidate_id: str,
) -> str:
    """Version Living evidence by proof and sources, not clone write time."""

    candidate = _atomic_candidate_row(conn, candidate_id)
    if not candidate:
        return ""
    stable_candidate = dict(candidate)
    stable_candidate.pop("updated_at", None)
    stable_candidate.pop("lifecycle_evaluated_at", None)
    roots = _atomic_root_snapshot(conn, candidate_id)
    incoming = []
    for root in roots:
        incoming.extend(
            conn.execute(
                """
                SELECT entry_id,lineage_type
                FROM main.memory_ledger_lineage
                WHERE guild_id=? AND target_entry_id=?
                  AND lineage_type IN ('correction_of','supersedes','retracts')
                ORDER BY entry_id,lineage_type
                """,
                (
                    int(root.get("guild_id") or 0),
                    str(root.get("root_entry_id") or ""),
                ),
            ).fetchall()
        )
    return _digest(
        "living_atomic_packet_source_v1",
        stable_candidate,
        roots,
        sorted(incoming),
    )


def _atomic_root_valid(
    request: IntelligencePacketRequest,
    candidate: Mapping[str, Any],
    root: Mapping[str, Any],
) -> bool:
    if (
        not root.get("root_entry_id")
        or str(root.get("root_status") or "") != "eligible"
        or not bool(root.get("is_independent"))
        or int(root.get("guild_id") or 0) != int(request.guild_id or 0)
        or str(root.get("subject_key") or "")
        != str(candidate.get("subject_key") or "")
        or str(root.get("lifecycle_status") or "") != "active"
        or bool(root.get("derived"))
        or bool(root.get("projection"))
        or str(root.get("source_class") or "")
        == SourceClass.LEGACY_SOURCE_BLIND.value
    ):
        return False
    if _public_route(request):
        return bool(
            str(root.get("visibility") or "") in _PUBLIC_VISIBILITIES
            and bool(root.get("public_usable"))
            and str(root.get("channel_policy") or "") in _PUBLIC_POLICIES
            and str(root.get("route_mode") or "")
            not in {
                "operator_command",
                "internal_control",
                "internal_ops",
                "protected_system",
                "source_file_enrichment",
                "source_file_lookup",
                "approved_backfill",
            }
        )
    return str(root.get("visibility") or "") in _INTERNAL_VISIBILITIES


def _safe_atomic_supporting_observation(root: Mapping[str, Any]) -> str:
    """Return one inert public member-authored root excerpt for paraphrase."""

    if (
        str(root.get("source_table") or "") != "conversations"
        or str(root.get("source_role") or "").strip().lower() != "user"
        or str(root.get("predicate_key") or "").strip().lower()
        != "conversation"
        or str(root.get("channel_policy") or "").strip().lower()
        not in _PUBLIC_POLICIES
        or str(root.get("visibility") or "").strip().lower()
        not in _PUBLIC_VISIBILITIES
        or not bool(root.get("public_usable"))
        or bool(root.get("derived"))
        or bool(root.get("projection"))
        or str(root.get("lifecycle_status") or "").strip().lower()
        != "active"
    ):
        return ""
    text = re.sub(
        r"\s+",
        " ",
        str(root.get("normalized_value") or ""),
    ).strip()
    if (
        len(text.split()) < 4
        or _ATOMIC_SUPPORT_BLOCK_RE.search(text)
        or _CURRENT_CORRECTION_RE.search(text)
    ):
        return ""
    return text.replace("```", "")[:_ATOMIC_SUPPORT_ITEM_CHARS]


def _atomic_supporting_observations(
    conn: sqlite3.Connection,
    candidate: Mapping[str, Any],
    roots: Sequence[Mapping[str, Any]],
    *,
    tags: Sequence[str],
) -> tuple[str, ...]:
    """Project bounded concrete roots without promoting them into new facts."""

    if (
        str(candidate.get("candidate_type") or "") != "topic_or_motif"
        or "recurring_public_conversation" not in set(tags)
    ):
        return ()
    ranked = sorted(
        roots,
        key=lambda root: (
            str(root.get("observed_at") or ""),
            int(root.get("source_sequence") or 0),
            str(root.get("root_entry_id") or ""),
        ),
        reverse=True,
    )
    observations: list[str] = []
    seen_occurrences: set[str] = set()
    seen_text: set[str] = set()
    for root in ranked:
        root_entry_id = str(root.get("root_entry_id") or "")
        occurrence = (
            knowledge_occurrence_identity(conn, root_entry_id)
            if root_entry_id
            else ""
        )
        if not occurrence or occurrence in seen_occurrences:
            continue
        observation = _safe_atomic_supporting_observation(root)
        normalized = re.sub(r"\W+", " ", observation.lower()).strip()
        if not observation or not normalized or normalized in seen_text:
            continue
        observations.append(observation)
        seen_occurrences.add(occurrence)
        seen_text.add(normalized)
        if len(observations) >= _ATOMIC_SUPPORT_POOL_MAX_ITEMS:
            break
    return tuple(observations)


def _living_claim_for_candidate(
    conn: sqlite3.Connection,
    candidate: Mapping[str, Any],
    roots: Sequence[Mapping[str, Any]],
) -> tuple[Any | None, str]:
    """Adapt one Living row and bind its proof to authoritative roots."""

    root_entry_ids = tuple(
        sorted(
            str(root.get("root_entry_id") or "")
            for root in roots
            if str(root.get("root_entry_id") or "")
        )
    )
    try:
        stored_occurrences = json.loads(
            str(candidate.get("occurrence_ids_json") or "[]")
        )
    except (TypeError, ValueError, json.JSONDecodeError):
        return None, "living_occurrence_lineage_invalid"
    if not isinstance(stored_occurrences, list) or any(
        not isinstance(identity, str) or not identity.strip()
        for identity in stored_occurrences
    ):
        return None, "living_occurrence_lineage_invalid"
    normalized_stored_occurrences = tuple(
        sorted(identity.strip() for identity in stored_occurrences)
    )
    if len(normalized_stored_occurrences) != len(
        set(normalized_stored_occurrences)
    ):
        return None, "living_occurrence_lineage_invalid"
    authoritative_occurrences = tuple(
        sorted(
            {
                identity
                for identity in (
                    knowledge_occurrence_identity(conn, entry_id)
                    for entry_id in root_entry_ids
                )
                if identity
            }
        )
    )
    if normalized_stored_occurrences != authoritative_occurrences:
        return None, "living_occurrence_lineage_mismatch"
    adapter_row = dict(candidate)
    adapter_row.update(
        {
            "meaning": candidate.get("normalized_value"),
            "root_ids": root_entry_ids,
            "occurrence_ids": normalized_stored_occurrences,
            "domain": candidate.get("canon_domain"),
            "claim_kind": candidate.get("canon_claim_kind"),
        }
    )
    adapted = adapt_living_atomic_claim(adapter_row)
    claim = adapted.claim
    if (
        claim is None
        or adapted.reason != "eligible_living"
        or claim.canon_status != CanonStatus.LIVING
        or claim.root_ids != root_entry_ids
        or claim.occurrence_ids != normalized_stored_occurrences
    ):
        return None, str(adapted.reason or "living_contract_invalid")
    return claim, ""


def _atomic_member_fact_authorized(
    candidate: Mapping[str, Any],
    authority_class: str,
) -> bool:
    """Keep atomic state from bypassing the member scalar-fact boundary."""

    if (
        str(candidate.get("candidate_type") or "") != "person_role_fact"
        or not str(candidate.get("subject_key") or "").startswith(
            "discord_user:"
        )
    ):
        return True
    source_class = str(authority_class or "")
    if source_class in {
        SourceClass.OWNER_CORRECTION.value,
        SourceClass.APPROVED_CANON.value,
    }:
        return True
    return bool(
        source_class == SourceClass.FIRST_PARTY_RECORD.value
        and str(candidate.get("predicate_key") or "").strip().lower()
        in APPROVED_MEMBER_SCALAR_PREDICATES
    )


def _atomic_items(
    conn: sqlite3.Connection,
    request: IntelligencePacketRequest,
    diagnostics: IntelligencePacketDiagnostics,
    exclusions: list[IntelligencePacketExclusion],
    *,
    broad: bool,
    request_terms: set[str],
) -> list[IntelligencePacketItem]:
    if int(request.subject_user_id or 0) <= 0:
        return []
    subject = _request_subject_key(request)
    candidate_ids = tuple(
        str(row[0])
        for row in conn.execute(
            """
            SELECT candidate_id
            FROM main.memory_ledger_knowledge_candidates
            WHERE guild_id=? AND subject_key=?
              AND (
                COALESCE(canonical_candidate_id,'')=''
                OR candidate_id=canonical_candidate_id
              )
            ORDER BY
              CASE candidate_state
                WHEN 'established' THEN 0
                WHEN 'provisional' THEN 1
                ELSE 2
              END,
              candidate_state,candidate_id
            LIMIT 200
            """,
            (int(request.guild_id or 0), subject),
        ).fetchall()
    )
    items: list[IntelligencePacketItem] = []
    for candidate_id in candidate_ids:
        candidate = _atomic_candidate_row(conn, candidate_id)
        roots = _atomic_root_snapshot(conn, candidate_id)
        living_marked = _living_candidate_marked(candidate)
        living_claim = None
        candidate_type = str(candidate.get("candidate_type") or "")
        authority_class = str(
            candidate.get("consolidated_authority_class")
            or candidate.get("authority_class")
            or SourceClass.LEGACY_SOURCE_BLIND.value
        )
        lane = (
            "open_loop"
            if candidate_type == "open_loop_or_question"
            else "atomic_knowledge"
        )
        diagnostics.candidates_by_lane[lane] = (
            diagnostics.candidates_by_lane.get(lane, 0) + 1
        )
        review_status = str(candidate.get("review_status") or "")
        review_due = _parse_time(candidate.get("review_due_at"))
        request_now = (
            _parse_time(request.now) or datetime.now(timezone.utc)
        )
        reason = ""
        if living_marked:
            living_claim, reason = _living_claim_for_candidate(
                conn,
                candidate,
                roots,
            )
            if reason:
                diagnostics.candidates_by_canon_status[
                    CanonStatus.LIVING.value
                ] = diagnostics.candidates_by_canon_status.get(
                    CanonStatus.LIVING.value,
                    0,
                ) + 1
                candidate_domain = str(
                    candidate.get("canon_domain") or ""
                ).strip()
                if candidate_domain:
                    diagnostics.candidates_by_canon_domain[
                        candidate_domain
                    ] = diagnostics.candidates_by_canon_domain.get(
                        candidate_domain,
                        0,
                    ) + 1
            else:
                authority_class = living_claim.source_class.value
        if not reason and candidate.get("lifecycle_schema_version") != (
            ATOMIC_KNOWLEDGE_LIFECYCLE_SCHEMA_VERSION
        ):
            reason = "atomic_lifecycle_not_reconciled"
        elif not reason and str(candidate.get("candidate_state") or "") not in {
            "established",
            "provisional",
        }:
            reason = "atomic_state"
        elif not reason and not bool(candidate.get("candidate_eligible")):
            reason = "atomic_ineligible"
        elif not reason and int(candidate.get("live_eligible") or 0):
            diagnostics.invalid_invariants.append(
                "atomic_live_eligible_selected_in_shadow"
            )
            reason = "atomic_live_eligible_invariant"
        elif not reason and str(candidate.get("invalidated_reason") or ""):
            reason = "atomic_invalidated"
        elif not reason and int(candidate.get("conflict_value_count") or 0) > 1:
            reason = "atomic_contested"
        elif not reason and (
            review_status in {"due", "retired_stale"}
            or (review_due is not None and review_due <= request_now)
        ):
            reason = "atomic_review_due"
        elif not reason and review_status not in {
            "current",
            "not_required",
        }:
            reason = "atomic_review_not_current"
        elif not reason and str(candidate.get("epistemic_status") or "") in {
            "inference",
            "contested",
        }:
            reason = "atomic_inference_or_contested"
        elif not reason and not _atomic_member_fact_authorized(
            candidate,
            authority_class,
        ):
            reason = "atomic_member_fact_not_authorized"
        visibility = (
            living_claim.visibility.value
            if living_claim is not None
            else str(candidate.get("visibility") or "unknown")
        )
        if not reason and _public_route(request) and visibility not in _PUBLIC_VISIBILITIES:
            reason = "atomic_visibility"
            diagnostics.visibility_exclusions += 1
        elif not reason and not _public_route(request) and visibility not in _INTERNAL_VISIBILITIES:
            reason = "atomic_visibility"
            diagnostics.visibility_exclusions += 1
        if not reason and (
            not roots
            or len(roots)
            != int(
                candidate.get("eligible_independent_root_count") or 0
            )
            or not all(
                _atomic_root_valid(request, candidate, root) for root in roots
            )
            or len(
                {
                    (
                        str(root.get("source_table") or ""),
                        str(root.get("source_row_id") or ""),
                    )
                    for root in roots
                }
            )
            < int(candidate.get("reinforcement_count") or 0)
        ):
            reason = "atomic_root_revalidation"
        if not reason:
            for root in roots:
                if conn.execute(
                    """
                    SELECT 1 FROM main.memory_ledger_lineage
                    WHERE guild_id=? AND target_entry_id=?
                      AND lineage_type IN (
                        'correction_of','supersedes','retracts'
                      )
                    LIMIT 1
                    """,
                    (
                        int(request.guild_id or 0),
                        str(root.get("root_entry_id") or ""),
                    ),
                ).fetchone():
                    reason = "atomic_root_superseded"
                    break
        try:
            tags = tuple(
                str(tag)
                for tag in json.loads(
                    str(candidate.get("retrieval_tags_json") or "[]")
                )
                if str(tag or "")
            )
            routes = json.loads(str(candidate.get("route_scope_json") or "[]"))
            if not isinstance(routes, list):
                raise ValueError("route scope")
        except (TypeError, ValueError, json.JSONDecodeError):
            tags = ()
            reason = reason or "atomic_scope_json"
        text = str(candidate.get("normalized_value") or "").strip()
        if not reason and not _relevant(
            request_terms=request_terms,
            broad=broad,
            lane=lane,
            text=text,
            predicate_key=str(candidate.get("predicate_key") or ""),
            tags=tags,
        ):
            reason = "atomic_topic_relevance"
        source_digest = (
            (
                _living_atomic_candidate_digest(conn, candidate_id)
                if living_claim is not None
                else _atomic_candidate_digest(conn, candidate_id)
            )
            if not reason
            else ""
        )
        if not reason and not source_digest:
            reason = "atomic_source_unversioned"
        if reason:
            _add_exclusion(
                diagnostics,
                exclusions,
                lane=lane,
                reason=reason,
                source_class=str(
                    candidate.get("consolidated_authority_class")
                    or candidate.get("authority_class")
                    or ""
                ),
            )
            continue
        state = str(candidate.get("candidate_state") or "")
        confidence = (
            living_claim.confidence.value
            if living_claim is not None
            else str(
                candidate.get("consolidated_confidence_class")
                or candidate.get("confidence_class")
                or Confidence.UNKNOWN.value
            )
        )
        participants = tuple(
            str(row[0])
            for row in conn.execute(
                """
                SELECT participant_key
                FROM main.memory_ledger_knowledge_participants
                WHERE candidate_id=?
                ORDER BY participant_role,participant_key
                """,
                (candidate_id,),
            ).fetchall()
        )
        if str(candidate.get("epistemic_status") or "") == (
            "source_abstraction"
        ):
            base_score = 72.0
        else:
            base_score = 96.0 if state == "established" else 69.0
        root_entry_ids = tuple(
            str(root.get("root_entry_id") or "")
            for root in roots
            if str(root.get("root_entry_id") or "")
        )
        root_identities = tuple(
            dict.fromkeys(
                identity
                for identity in (
                    knowledge_root_identity(conn, entry_id)
                    for entry_id in root_entry_ids
                )
                if identity
            )
        )
        occurrence_identities = tuple(
            dict.fromkeys(
                identity
                for identity in (
                    knowledge_occurrence_identity(conn, entry_id)
                    for entry_id in root_entry_ids
                )
                if identity
            )
        )
        supporting_observations = _atomic_supporting_observations(
            conn,
            candidate,
            roots,
            tags=tags,
        )
        if broad and (
            not root_identities or not occurrence_identities
        ):
            _add_exclusion(
                diagnostics,
                exclusions,
                lane=lane,
                reason="member_evidence_missing_root_lineage",
                source_class=authority_class,
            )
            continue
        items.append(
            IntelligencePacketItem(
                lane=lane,
                source_class=authority_class,
                source_type=candidate_type,
                source_ref="atomic:%s" % candidate_id,
                source_digest=source_digest,
                subject_key=subject,
                predicate_key=str(candidate.get("predicate_key") or ""),
                text=text[:1000],
                visibility=visibility,
                confidence=confidence,
                lifecycle=state,
                authority=_AUTHORITY_RANK.get(authority_class, 0),
                participants=participants,
                lineage=root_entry_ids,
                observed_at=str(candidate.get("last_seen_at") or ""),
                usage="tentative" if state == "provisional" else "content",
                score=base_score
                + 0.4
                * len(
                    request_terms
                    & (
                        _terms(text)
                        | _terms(str(candidate.get("predicate_key") or ""))
                        | set(tags)
                    )
                ),
                revalidation_kind="atomic",
                revalidation_key=candidate_id,
                root_identities=root_identities,
                occurrence_identities=occurrence_identities,
                point_identity=_point_identity(
                    subject_key=subject,
                    predicate_key=str(
                        candidate.get("predicate_key") or ""
                    ),
                    text=text,
                ),
                supporting_observations=supporting_observations,
                canon_status=(
                    living_claim.canon_status.value
                    if living_claim is not None
                    else ""
                ),
                canon_domain=(
                    living_claim.domain.value
                    if living_claim is not None
                    else ""
                ),
                canon_claim_kind=(
                    living_claim.claim_kind.value
                    if living_claim is not None
                    else ""
                ),
            )
        )
    return items


def _canon_value(value: Any) -> str:
    if is_dataclass(value):
        value = asdict(value)
    if isinstance(value, (list, tuple)):
        return ", ".join(str(item) for item in value)
    if isinstance(value, dict):
        return "; ".join("%s=%s" % item for item in sorted(value.items()))
    return str(value)


def _canon_digest(fact: Any) -> str:
    return _digest(
        CANON_SOURCE_CONTRACT_VERSION,
        fact.subject.key,
        fact.predicate,
        _canon_value(fact.value),
        fact.source_class.value,
        fact.visibility.value,
        fact.confidence.value,
    )


def _recognized_canon_digest(
    fact: Any,
    signal: _CanonIdentitySignal,
    subject_key: str,
) -> str:
    return _digest(
        "recognized_canon_fact_v1",
        _canon_digest(fact),
        signal.evidence_digest,
        str(subject_key or ""),
    )


def _canon_items(
    conn: sqlite3.Connection,
    request: IntelligencePacketRequest,
    diagnostics: IntelligencePacketDiagnostics,
    exclusions: list[IntelligencePacketExclusion],
    *,
    request_terms: set[str],
    environ: Mapping[str, str] | None = None,
) -> list[IntelligencePacketItem]:
    items: list[IntelligencePacketItem] = []
    lowered = str(request.user_text or "").lower()
    broad = _request_is_broad_profile(request)
    subject_key = _request_subject_key(request)
    signal = _canon_identity_signal(conn, request, environ=environ)
    diagnostics.canon_identity_status = signal.status
    diagnostics.canon_identity_stable_row_count = int(
        signal.stable_row_count or 0
    )
    recognized_fact_keys = set()
    governed_subject_scope = bool(
        str(request.frame_subject_requirement or "").strip().lower()
        == "required"
        and subject_key
    )
    if (
        (broad or governed_subject_scope)
        and signal.recognized
        and signal.subject is not None
    ):
        for fact in CANON_FACTS:
            if fact.subject.key != signal.subject.key:
                continue
            normalized_claim = adapt_legacy_canon_fact(fact)
            recognized_fact_keys.add((fact.subject.key, fact.predicate))
            value = _canon_value(fact.value)
            fact_text = "%s %s: %s" % (
                fact.subject.name,
                fact.predicate.replace("_", " "),
                value,
            )
            diagnostics.candidates_by_lane["canon"] = (
                diagnostics.candidates_by_lane.get("canon", 0) + 1
            )
            source_digest = _recognized_canon_digest(
                fact,
                signal,
                subject_key,
            )
            item = IntelligencePacketItem(
                lane="canon",
                source_class=fact.source_class.value,
                source_type="recognized_canon_fact",
                source_ref=(
                    "canon_signal:%s:%s:%s"
                    % (
                        CANON_SOURCE_CONTRACT_VERSION,
                        fact.subject.key,
                        fact.predicate,
                    )
                ),
                source_digest=source_digest,
                subject_key=subject_key,
                predicate_key=fact.predicate,
                text=fact_text[:1000],
                visibility=fact.visibility.value,
                confidence=fact.confidence.value,
                lifecycle="approved",
                authority=_AUTHORITY_RANK[fact.source_class.value],
                participants=(subject_key,),
                observed_at="",
                usage="content",
                score=(
                    120.0
                    if fact.predicate == "primary_identity"
                    else 104.0
                ),
                revalidation_kind="recognized_canon",
                revalidation_key=source_digest,
                canon_status=normalized_claim.canon_status.value,
                canon_domain=normalized_claim.domain.value,
                canon_claim_kind=normalized_claim.claim_kind.value,
            )
            if not _route_allows_item(request, item):
                diagnostics.visibility_exclusions += 1
                _add_exclusion(
                    diagnostics,
                    exclusions,
                    lane="canon",
                    reason="canon_visibility",
                    source_class=item.source_class,
                )
                continue
            items.append(item)
    for fact in CANON_FACTS:
        if (fact.subject.key, fact.predicate) in recognized_fact_keys:
            continue
        normalized_claim = adapt_legacy_canon_fact(fact)
        value = _canon_value(fact.value)
        fact_text = "%s %s: %s" % (
            fact.subject.name,
            fact.predicate.replace("_", " "),
            value,
        )
        aliases = (fact.subject.name, *fact.subject.aliases)
        alias_relevant = any(
            re.search(r"\b%s\b" % re.escape(alias.lower()), lowered)
            for alias in aliases
            if alias
        )
        if (
            fact.subject.key in _CANON_MEMBER_SUBJECT_KEYS
            and not alias_relevant
        ):
            continue
        if not alias_relevant and not (
            request_terms
            & (
                _terms(fact.subject.name)
                | _terms(" ".join(fact.subject.aliases))
                | _terms(fact.predicate)
                | _terms(value)
            )
        ):
            continue
        diagnostics.candidates_by_lane["canon"] = (
            diagnostics.candidates_by_lane.get("canon", 0) + 1
        )
        source_digest = _canon_digest(fact)
        item = IntelligencePacketItem(
            lane="canon",
            source_class=fact.source_class.value,
            source_type="canon_fact",
            source_ref=(
                "canon:%s:%s:%s"
                % (
                    CANON_SOURCE_CONTRACT_VERSION,
                    fact.subject.key,
                    fact.predicate,
                )
            ),
            source_digest=source_digest,
            subject_key=fact.subject.key,
            predicate_key=fact.predicate,
            text=fact_text[:1000],
            visibility=fact.visibility.value,
            confidence=fact.confidence.value,
            lifecycle="approved",
            authority=_AUTHORITY_RANK[fact.source_class.value],
            observed_at="",
            usage="content",
            score=92.0,
            revalidation_kind="canon",
            revalidation_key=source_digest,
            canon_status=normalized_claim.canon_status.value,
            canon_domain=normalized_claim.domain.value,
            canon_claim_kind=normalized_claim.claim_kind.value,
        )
        if not _route_allows_item(request, item):
            diagnostics.visibility_exclusions += 1
            _add_exclusion(
                diagnostics,
                exclusions,
                lane="canon",
                reason="canon_visibility",
                source_class=item.source_class,
            )
            continue
        items.append(item)
    return items


def _source_file_items(
    request: IntelligencePacketRequest,
    diagnostics: IntelligencePacketDiagnostics,
    exclusions: list[IntelligencePacketExclusion],
) -> list[IntelligencePacketItem]:
    snapshot = str(request.source_context_snapshot or "").strip()
    if not snapshot:
        return []
    diagnostics.candidates_by_lane["source_file"] = (
        diagnostics.candidates_by_lane.get("source_file", 0) + 1
    )
    if (
        not request.source_context_authorized
        or request.channel_policy not in {"sealed_test", "internal_controlled"}
        or _public_route(request)
    ):
        _add_exclusion(
            diagnostics,
            exclusions,
            lane="source_file",
            reason="source_file_route_not_authorized",
            source_class=SourceClass.SOURCE_FILE_PROJECTION.value,
        )
        return []
    source_digest = _digest("source_file_snapshot", snapshot)
    return [
        IntelligencePacketItem(
            lane="source_file",
            source_class=SourceClass.SOURCE_FILE_PROJECTION.value,
            source_type="existing_source_context_snapshot",
            source_ref="source_file:%s" % source_digest[:32],
            source_digest=source_digest,
            subject_key="source_file_subject",
            predicate_key="source_file_context",
            text=snapshot[:1800],
            visibility="internal",
            confidence=Confidence.MEDIUM.value,
            lifecycle="snapshot",
            authority=_AUTHORITY_RANK[
                SourceClass.SOURCE_FILE_PROJECTION.value
            ],
            observed_at=request.now or _now(),
            usage="internal_context",
            score=74.0,
            revalidation_kind="snapshot",
            revalidation_key=source_digest,
        )
    ]


def _relationship_items(
    conn: sqlite3.Connection,
    request: IntelligencePacketRequest,
    diagnostics: IntelligencePacketDiagnostics,
    *,
    environ: Mapping[str, str] | None = None,
) -> list[IntelligencePacketItem]:
    if (
        int(request.subject_user_id or 0) <= 0
        or request.direct_state != "direct"
        or tuple(
            user_id
            for user_id in request.participant_user_ids
            if int(user_id or 0) > 0
        )
        not in {
            (),
            (int(request.subject_user_id or 0),),
        }
    ):
        return []
    posture = shadow_packet_posture(
        conn,
        guild_id=request.guild_id,
        user_id=request.subject_user_id,
        route_mode=request.route_mode,
        channel_policy=request.channel_policy,
        direct=True,
        target_user_id=request.subject_user_id,
        environ=environ,
    )
    if not posture:
        return []
    diagnostics.candidates_by_lane["relationship_posture"] = (
        diagnostics.candidates_by_lane.get("relationship_posture", 0) + 1
    )
    return [
        IntelligencePacketItem(
            lane="relationship_posture",
            source_class=SourceClass.DERIVED_SUMMARY.value,
            source_type="relationship_v2_private_posture",
            source_ref=str(posture["source_ref"]),
            source_digest=str(posture["source_digest"]),
            subject_key=_request_subject_key(request),
            predicate_key="relationship_posture",
            text=str(posture["posture"])[:500],
            visibility="private",
            confidence=Confidence.MEDIUM.value,
            lifecycle="shadow",
            authority=_AUTHORITY_RANK[SourceClass.DERIVED_SUMMARY.value],
            participants=(_request_subject_key(request),),
            observed_at=str(posture.get("updated_at") or ""),
            usage="tone_only",
            score=65.0,
            revalidation_kind="relationship",
            revalidation_key=str(request.subject_user_id),
        )
    ]


def _resolve_cross_lane_conflicts(
    candidates: list[IntelligencePacketItem],
    diagnostics: IntelligencePacketDiagnostics,
    exclusions: list[IntelligencePacketExclusion],
) -> list[IntelligencePacketItem]:
    groups: dict[tuple[str, str], list[IntelligencePacketItem]] = {}
    passthrough: list[IntelligencePacketItem] = []
    for item in candidates:
        if (
            item.lane not in {"approved_fact", "atomic_knowledge", "canon"}
            or item.predicate_key in _ADDITIVE_PREDICATES
        ):
            passthrough.append(item)
            continue
        groups.setdefault((item.subject_key, item.predicate_key), []).append(item)
    for (subject, predicate), group in groups.items():
        values = {
            re.sub(r"\W+", " ", item.text.lower()).strip() for item in group
        }
        if len(values) <= 1:
            passthrough.extend(group)
            continue
        ordered = sorted(
            group,
            key=lambda item: (
                -item.authority,
                -_CONFIDENCE_RANK.get(item.confidence, 0),
                -item.score,
                item.source_ref,
            ),
        )
        authoritative = [
            item
            for item in ordered
            if item.source_class
            in {
                SourceClass.OWNER_CORRECTION.value,
                SourceClass.APPROVED_CANON.value,
            }
        ]
        conflict_key = _digest(subject, predicate)[:16]
        if len(authoritative) == 1:
            winner = authoritative[0]
            passthrough.append(winner)
            diagnostics.conflict_reasons.append(
                "%s:authoritative_precedence" % conflict_key
            )
            for item in group:
                if item is winner:
                    continue
                _add_exclusion(
                    diagnostics,
                    exclusions,
                    lane=item.lane,
                    reason="cross_lane_authoritative_precedence",
                    source_class=item.source_class,
                )
        else:
            diagnostics.conflict_reasons.append(
                "%s:unresolved_cross_lane_contradiction" % conflict_key
            )
            for item in group:
                _add_exclusion(
                    diagnostics,
                    exclusions,
                    lane=item.lane,
                    reason="unresolved_cross_lane_contradiction",
                    source_class=item.source_class,
                )
    return passthrough


def _apply_current_turn_precedence(
    request: IntelligencePacketRequest,
    candidates: list[IntelligencePacketItem],
    diagnostics: IntelligencePacketDiagnostics,
    exclusions: list[IntelligencePacketExclusion],
) -> list[IntelligencePacketItem]:
    """Withhold a scoped durable value contradicted in the current request."""

    current_text = str(request.user_text or "").strip()
    current_terms = _terms(current_text)
    correction_signal = bool(
        _CURRENT_CORRECTION_RE.search(current_text)
        or (
            "?" not in current_text
            and _CURRENT_ACTUALLY_STATEMENT_RE.search(current_text)
        )
    )
    if not current_text or not correction_signal:
        return candidates
    current_normalized = re.sub(r"\W+", " ", current_text.lower()).strip()
    subject = _request_subject_key(request)
    kept: list[IntelligencePacketItem] = []
    for item in candidates:
        predicate_terms = _terms(item.predicate_key.replace("_", " "))
        item_value = re.sub(r"\W+", " ", item.text.lower()).strip()
        if (
            item.lane == "assessment_observation"
            and item.subject_key == subject
        ):
            _add_exclusion(
                diagnostics,
                exclusions,
                lane=item.lane,
                reason="current_turn_correction_precedence",
                source_class=item.source_class,
            )
            continue
        if (
            item.lane
            in {"approved_fact", "atomic_knowledge", "open_loop"}
            and item.subject_key == subject
            and predicate_terms
            and current_terms & predicate_terms
            and item_value
            and item_value not in current_normalized
        ):
            diagnostics.conflict_reasons.append(
                "%s:current_turn_correction_precedence"
                % _digest(item.subject_key, item.predicate_key)[:16]
            )
            _add_exclusion(
                diagnostics,
                exclusions,
                lane=item.lane,
                reason="current_turn_correction_precedence",
                source_class=item.source_class,
            )
            continue
        kept.append(item)
    return kept


def _frame_allowed_canon_domains(
    request: IntelligencePacketRequest,
) -> set[str]:
    hints = {
        str(value or "").strip().lower()
        for value in (*request.frame_domain_hints, *request.frame_role_hints)
        if str(value or "").strip()
    }
    allowed = set(hints).intersection(
        {"real_community", "broadcast_history", "operational", "lore", "hybrid"}
    )
    mapping = {
        "artist": {"real_community", "broadcast_history"},
        "music": {"real_community", "broadcast_history"},
        "community_member": {"real_community"},
        "real_community": {"real_community"},
        "broadcast_participant": {"broadcast_history"},
        "broadcast_history": {"broadcast_history"},
        "operator": {"operational"},
        "operational": {"operational"},
        "system_subject": {"operational"},
        "technical": {"operational"},
        "in_world_entity": {"lore"},
        "lore": {"lore"},
    }
    for hint in hints:
        allowed.update(mapping.get(hint, ()))
    if allowed:
        allowed.add("hybrid")
    return allowed


def _filter_frame_applicable_candidates(
    request: IntelligencePacketRequest,
    subject_resolution: PacketSubjectResolution,
    candidates: list[IntelligencePacketItem],
    diagnostics: IntelligencePacketDiagnostics,
    exclusions: list[IntelligencePacketExclusion],
) -> list[IntelligencePacketItem]:
    """Apply frame subject/role/domain/event/task/time before scoring."""

    if not str(request.frame_revision or "").strip():
        return candidates

    def exclude(item: IntelligencePacketItem, reason: str) -> None:
        diagnostics.frame_applicability_exclusion_count += 1
        _add_exclusion(
            diagnostics,
            exclusions,
            lane=item.lane,
            reason=reason,
            source_class=item.source_class,
        )

    if not subject_resolution.applicable:
        kept = []
        reason = "frame_subject_%s" % subject_resolution.status
        for item in candidates:
            if item.lane == "current_intent":
                kept.append(item)
            else:
                exclude(item, reason)
        return kept

    subject_required = (
        str(request.frame_subject_requirement or "").strip().lower()
        == "required"
    )
    accepted_subject_keys = {
        value
        for value in (
            subject_resolution.subject_key,
            subject_resolution.entity_ref,
        )
        if value
    }
    subject_scoped_lanes = {
        "conversation_context",
        "assessment_observation",
        "approved_fact",
        "moment",
        "atomic_knowledge",
        "open_loop",
        "canon",
        "relationship_posture",
    }
    allowed_domains = _frame_allowed_canon_domains(request)
    event_relation = str(request.frame_event_relation or "uncertain").lower()
    frame_event_ref = str(request.frame_event_ref or "").strip()
    operational_question = bool(
        str(request.frame_object_kind or "").lower()
        in {"queue", "website", "broadcast"}
        and str(request.frame_phase or "").lower()
        in {"failure", "diagnosis", "retest", "execution", "request"}
    )
    kept: list[IntelligencePacketItem] = []
    for item in candidates:
        if (
            subject_required
            and item.lane in subject_scoped_lanes
            and item.subject_key not in accepted_subject_keys
        ):
            exclude(item, "frame_subject_mismatch")
            continue
        if (
            item.canon_domain
            and allowed_domains
            and item.canon_domain not in allowed_domains
        ):
            exclude(item, "frame_domain_mismatch")
            continue
        if (
            operational_question
            and item.lane == "canon"
            and item.canon_domain in {"lore", "broadcast_history"}
        ):
            exclude(item, "current_operational_source_precedence")
            continue
        if item.lane == "moment" and frame_event_ref:
            item_event_ref = str(item.revalidation_key or "").strip()
            if event_relation in {"same_event", "same_event_new_phase", "resume"}:
                if item_event_ref != frame_event_ref:
                    exclude(item, "frame_event_mismatch")
                    continue
            elif event_relation in {
                "new_event_same_participant",
                "new_event_or_uncertain",
            }:
                exclude(item, "frame_event_mismatch")
                continue
        if (
            str(request.frame_currentness or "").lower() == "current"
            and operational_question
            and item.lane == "canon"
            and item.canon_claim_kind == "current_state"
        ):
            exclude(item, "frame_currentness_mismatch")
            continue
        kept.append(item)
    return kept


def _select_items(
    request: IntelligencePacketRequest,
    subject_resolution: PacketSubjectResolution,
    candidates: list[IntelligencePacketItem],
    diagnostics: IntelligencePacketDiagnostics,
    exclusions: list[IntelligencePacketExclusion],
) -> tuple[
    tuple[IntelligencePacketItem, ...],
    tuple[IntelligencePacketItem, ...],
    tuple[IntelligencePacketItem, ...],
]:
    broad = _request_is_broad_profile(request)
    if request.immediate_recap:
        kept = []
        for item in candidates:
            if item.lane in {"current_intent", "conversation_context"}:
                kept.append(item)
            else:
                _add_exclusion(
                    diagnostics,
                    exclusions,
                    lane=item.lane,
                    reason="current_exchange_precedence",
                    source_class=item.source_class,
                )
        candidates = kept
    candidates = _filter_frame_applicable_candidates(
        request,
        subject_resolution,
        candidates,
        diagnostics,
        exclusions,
    )
    candidates = _apply_current_turn_precedence(
        request,
        candidates,
        diagnostics,
        exclusions,
    )
    candidates = _resolve_cross_lane_conflicts(
        candidates,
        diagnostics,
        exclusions,
    )
    ephemeral_member_items = tuple(
        item
        for item in candidates
        if item.lane in {"assessment_observation", "conversation_context"}
        and _profile_member_item(request, item)
    )
    if ephemeral_member_items:
        enriched_candidates: list[IntelligencePacketItem] = []
        for item in candidates:
            if item.lane == "atomic_knowledge" and item.source_type == "topic_or_motif":
                item_roots = set(item.root_identities)
                exact_support = tuple(
                    candidate.text
                    for candidate in ephemeral_member_items
                    if set(candidate.root_identities)
                    and set(candidate.root_identities).issubset(item_roots)
                )
                if exact_support:
                    item = replace(
                        item,
                        supporting_observations=tuple(
                            dict.fromkeys(
                                (*item.supporting_observations, *exact_support)
                            )
                        )[:_ATOMIC_SUPPORT_POOL_MAX_ITEMS],
                    )
            enriched_candidates.append(item)
        candidates = enriched_candidates
    canon_anchor = _profile_canon_anchor(request, candidates)
    subject_key = _request_subject_key(request)
    recognized_canon_signal = any(
        item.lane == "canon"
        and item.source_type
        in {"recognized_canon_fact", "recognized_declared_canon_claim"}
        and item.subject_key == subject_key
        for item in candidates
    )
    profile_candidates: list[IntelligencePacketItem] = []
    broad_lane_priority = {
        "current_intent": 0,
        "approved_fact": 1,
        "atomic_knowledge": 2,
        "conversation_context": 3,
        "assessment_observation": 4,
        "moment": 5,
        "open_loop": 6,
        "canon": 8,
        "source_file": 9,
        "relationship_posture": 10,
    }
    if broad and public_assessment_process_request(request.user_text):
        broad_lane_priority.update(
            {
                "current_intent": 0,
                "conversation_context": 1,
                "assessment_observation": 2,
                "approved_fact": 3,
                "atomic_knowledge": 4,
                "moment": 5,
                "open_loop": 6,
            }
        )
    governed_subject_priority = {
        "current_intent": 0,
        "approved_fact": 1,
        "atomic_knowledge": 1,
        "conversation_context": 1,
        "assessment_observation": 1,
        "open_loop": 1,
        "moment": 2,
        "canon": 3,
        "relationship_posture": 4,
        "source_file": 5,
    }
    frame_subject_required = bool(
        str(request.frame_subject_requirement or "").strip().lower()
        == "required"
        and subject_resolution.status == "resolved"
    )
    ordered = sorted(
        candidates,
        key=lambda item: (
            (
                governed_subject_priority.get(item.lane, 9)
                if frame_subject_required
                else 0
            ),
            (
                (
                    7
                    if item is canon_anchor
                    else broad_lane_priority.get(item.lane, 10)
                )
                if broad
                else 0
            ),
            -item.score,
            -item.authority,
            -_CONFIDENCE_RANK.get(item.confidence, 0),
            item.source_ref,
        ),
    )
    selected: list[IntelligencePacketItem] = []
    seen_text: set[str] = set()
    seen_profile_roots: set[str] = set()
    seen_context_roots: set[str] = set()
    selected_root_items: list[IntelligencePacketItem] = []
    lane_counts: Counter[str] = Counter()
    used = 0
    budget = min(max(int(request.budget_chars or 2400), 400), 6000)
    lane_caps = dict(
        _BROAD_PROFILE_LANE_CAPS if broad else _LANE_CAPS
    )
    for item in ordered:
        # Keep all valid member evidence for sufficiency and response
        # validation before collapsing duplicate renderings of one source.
        if (
            item.lane in {"assessment_observation", "conversation_context"}
            and _profile_member_item(request, item)
        ):
            profile_candidates.append(item)
        item_roots = set(item.root_identities)
        if (
            broad
            and item.lane == "assessment_observation"
            and item_roots.intersection(seen_context_roots)
        ):
            diagnostics.root_collapse_suppression += 1
            _add_exclusion(
                diagnostics,
                exclusions,
                lane=item.lane,
                reason="same_root_conversation_context",
                source_class=item.source_class,
            )
            continue
        if broad and _root_bearing_member_item(request, item) and item_roots:
            root_overlap = item_roots.intersection(seen_profile_roots)
            same_root_projection = any(
                (
                    item_roots.issubset(set(prior.root_identities))
                    or set(prior.root_identities).issubset(item_roots)
                )
                and (
                    (
                        item.point_identity
                        and item.point_identity == prior.point_identity
                    )
                    or item.lane != prior.lane
                )
                for prior in selected_root_items
            )
            if same_root_projection:
                diagnostics.root_collapse_suppression += 1
                _add_exclusion(
                    diagnostics,
                    exclusions,
                    lane=item.lane,
                    reason="same_root_projection",
                    source_class=item.source_class,
                )
                continue
            if root_overlap:
                diagnostics.shared_root_projection_count += 1
        normalized = re.sub(r"\W+", " ", item.text.lower()).strip()
        if normalized and normalized in seen_text:
            diagnostics.duplicate_suppression += 1
            _add_exclusion(
                diagnostics,
                exclusions,
                lane=item.lane,
                reason="duplicate_semantic_value",
                source_class=item.source_class,
            )
            continue
        if (
            item.lane not in {"assessment_observation", "conversation_context"}
            and _profile_member_item(request, item)
        ):
            profile_candidates.append(item)
        if lane_counts[item.lane] >= lane_caps.get(item.lane, 2):
            _add_exclusion(
                diagnostics,
                exclusions,
                lane=item.lane,
                reason="lane_cap",
                source_class=item.source_class,
            )
            continue
        item_cost = (
            0
            if item.lane == "current_intent"
            else min(len(item.text), 500) + 36
        )
        if used + item_cost > budget:
            diagnostics.budget_exclusions += 1
            _add_exclusion(
                diagnostics,
                exclusions,
                lane=item.lane,
                reason="packet_budget",
                source_class=item.source_class,
            )
            continue
        selected.append(item)
        if broad and _root_bearing_member_item(request, item):
            seen_profile_roots.update(item_roots)
            selected_root_items.append(item)
        if broad and item.lane == "conversation_context":
            seen_context_roots.update(item_roots)
        if normalized:
            seen_text.add(normalized)
        lane_counts[item.lane] += 1
        used += item_cost
        diagnostics.selected_by_lane[item.lane] = (
            diagnostics.selected_by_lane.get(item.lane, 0) + 1
        )
        diagnostics.selected_by_source_class[item.source_class] = (
            diagnostics.selected_by_source_class.get(item.source_class, 0) + 1
        )
        if item.canon_status:
            diagnostics.selected_by_canon_status[item.canon_status] = (
                diagnostics.selected_by_canon_status.get(
                    item.canon_status,
                    0,
                )
                + 1
            )
        if item.canon_domain:
            diagnostics.selected_by_canon_domain[item.canon_domain] = (
                diagnostics.selected_by_canon_domain.get(
                    item.canon_domain,
                    0,
                )
                + 1
            )
        if item.revalidation_kind == "atomic":
            diagnostics.selected_atomic_states[item.lifecycle] = (
                diagnostics.selected_atomic_states.get(item.lifecycle, 0) + 1
            )
    for lane, count in diagnostics.candidates_by_lane.items():
        if int(count or 0) and not diagnostics.selected_by_lane.get(lane):
            diagnostics.missing_lanes.append(lane)
    if (
        broad
        and not any(
            item.lane
            in {
                "approved_fact",
                "moment",
                "atomic_knowledge",
                "open_loop",
            }
            for item in selected
        )
    ):
        diagnostics.missing_lanes.append("durable_memory")
    diagnostics.missing_lanes = list(
        dict.fromkeys(diagnostics.missing_lanes)
    )
    validation_items: list[IntelligencePacketItem] = []
    seen_validation_sources: set[tuple[str, str, str]] = set()
    comparison_canon_subject_keys = _mentioned_canon_entity_keys(
        request.user_text
    )
    validation_canon = tuple(
        candidate
        for candidate in ordered
        if candidate.lane == "canon"
        and (
            (
                recognized_canon_signal
                and candidate.source_type
                in {
                    "recognized_canon_fact",
                    "recognized_declared_canon_claim",
                }
                and candidate.subject_key == subject_key
            )
            or (
                recognized_canon_signal
                and _IDENTITY_COMPARISON_REQUEST_RE.search(
                    str(request.user_text or "")
                )
                and candidate.source_type == "canon_fact"
                and candidate.subject_key in comparison_canon_subject_keys
            )
            or (
                not recognized_canon_signal
                and canon_anchor is not None
                and candidate.subject_key == canon_anchor.subject_key
            )
        )
    )
    for item in (
        *(item for item in selected if item.lane != "canon"),
        *profile_candidates,
        *validation_canon,
    ):
        if item.lane not in _VALIDATION_SUPPORT_LANES:
            continue
        source_key = (item.lane, item.source_ref, item.source_digest)
        if source_key in seen_validation_sources:
            continue
        seen_validation_sources.add(source_key)
        validation_items.append(item)
        diagnostics.validation_support_by_lane[item.lane] = (
            diagnostics.validation_support_by_lane.get(item.lane, 0) + 1
        )
    return (
        tuple(selected),
        tuple(profile_candidates),
        tuple(validation_items),
    )


def _profile_sufficiency(
    request: IntelligencePacketRequest,
    *,
    selected: Sequence[IntelligencePacketItem],
    candidates: Sequence[IntelligencePacketItem],
) -> ProfileSufficiency:
    if not _request_is_broad_profile(request):
        return ProfileSufficiency(
            status="not_applicable",
            satisfied=True,
            reason_codes=("not_broad_profile",),
        )
    candidate_items = tuple(
        item
        for item in candidates
        if _profile_member_item(request, item)
    )
    selected_items = tuple(
        item
        for item in selected
        if _profile_member_item(request, item)
    )
    process_request = public_assessment_process_request(request.user_text)
    if not process_request:
        durable_root_sets = tuple(
            set(item.root_identities)
            for item in candidate_items
            if item.lane in _PROFILE_DURABLE_MEMBER_EVIDENCE_LANES
            and item.root_identities
        )

        def represented_by_durable(item: IntelligencePacketItem) -> bool:
            item_roots = set(item.root_identities)
            return bool(
                item.lane
                in {"assessment_observation", "conversation_context"}
                and item_roots
                and any(item_roots.issubset(roots) for roots in durable_root_sets)
            )

        candidate_items = tuple(
            item for item in candidate_items if not represented_by_durable(item)
        )
        selected_items = tuple(
            item for item in selected_items if not represented_by_durable(item)
        )
    if process_request:
        relevant_open_roots = {
            root
            for item in candidate_items
            if item.lane == "assessment_observation"
            for root in item.root_identities
            if root
        }
        candidate_items = tuple(
            item
            for item in candidate_items
            if item.lane == "assessment_observation"
            or (
                item.lane == "conversation_context"
                and bool(set(item.root_identities).intersection(relevant_open_roots))
            )
        )
        selected_items = tuple(
            item
            for item in selected_items
            if item.lane == "assessment_observation"
            or (
                item.lane == "conversation_context"
                and bool(set(item.root_identities).intersection(relevant_open_roots))
            )
        )
    recognized_canon_signal = any(
        item.lane == "canon"
        and item.source_type
        in {"recognized_canon_fact", "recognized_declared_canon_claim"}
        and item.subject_key
        == _request_subject_key(request)
        for item in selected
    )
    observation_only = bool(
        candidate_items
        and all(
            item.lane in {"assessment_observation", "conversation_context"}
            for item in candidate_items
        )
    )
    # Root collapse and semantic point grouping happen before this union.  A
    # distinct durable point and a distinct Open point may therefore combine,
    # while a projection and its raw source cannot manufacture breadth.
    candidate_point_map = material_profile_point_map(candidate_items)
    candidate_points = set(candidate_point_map.values())
    candidate_occurrences = {
        identity
        for item in candidate_items
        for identity in item.occurrence_identities
        if identity
    }
    selected_points = {
        candidate_point_map.get(
            item.point_group_identity or item.point_identity,
            item.point_group_identity or item.point_identity,
        )
        for item in selected_items
        if item.point_identity
    }
    selected_roots = {
        identity
        for item in selected_items
        for identity in item.root_identities
        if identity
    }
    selected_occurrences = {
        identity
        for item in selected_items
        for identity in item.occurrence_identities
        if identity
    }
    candidate_point_count = len(candidate_points)
    required_points = (
        2
        if candidate_point_count >= 2
        and len(candidate_occurrences) >= 2
        else 1
        if candidate_point_count >= 1
        and len(candidate_occurrences) >= 1
        else 0
    )
    reasons = []
    if required_points == 0:
        status = "empty"
        satisfied = False
        reasons.append(
            "recognized_canon_without_supported_observation"
            if recognized_canon_signal
            else "no_supported_member_evidence"
        )
    elif required_points == 1:
        satisfied = bool(
            len(selected_points) >= 1
            and len(selected_roots) >= 1
            and len(selected_occurrences) >= 1
        )
        status = "sparse" if satisfied else "insufficient"
        if observation_only:
            reasons.append(
                "sparse_public_observation"
                if satisfied
                else "public_observation_not_selected"
            )
        else:
            reasons.append(
                "sparse_supported_member_evidence"
                if satisfied
                else "sparse_member_evidence_not_selected"
            )
    else:
        enough_points = len(selected_points) >= 2
        enough_roots = len(selected_roots) >= 2
        enough_occurrences = len(selected_occurrences) >= 2
        satisfied = enough_points and enough_roots and enough_occurrences
        status = "rich" if satisfied else "insufficient"
        if satisfied:
            reasons.append("rich_supported_member_evidence")
        else:
            if not enough_points:
                reasons.append("member_point_requirement_not_met")
            if not enough_roots:
                reasons.append("independent_root_requirement_not_met")
            if not enough_occurrences:
                reasons.append(
                    "independent_occurrence_requirement_not_met"
                )
    return ProfileSufficiency(
        status=status,
        satisfied=satisfied,
        required_point_count=required_points,
        selected_point_count=len(selected_points),
        candidate_point_count=candidate_point_count,
        independent_root_count=len(selected_roots),
        independent_occurrence_count=len(selected_occurrences),
        reason_codes=tuple(reasons),
    )


def _moment_version(
    conn: sqlite3.Connection,
    packet: UnifiedIntelligencePacket,
    item: IntelligencePacketItem,
) -> str:
    subject = _request_subject_key(packet.request)
    broad = _request_is_broad_profile(packet.request)
    moments = select_public_participant_moment_gists(
        conn,
        guild_id=packet.request.guild_id,
        participant_key=subject,
        topic_text=packet.request.user_text,
        broad_recall=broad,
        token_budget=160,
        freshness_days=3650,
        allowed_channel_policies=("public_home", "public_context"),
        max_results=4,
    )
    target = item.revalidation_key
    for moment in moments:
        if moment.moment_id != target:
            continue
        return _digest(
            "moment",
            "moment:%s" % moment.moment_id,
            moment.canonical_ledger_entry_id or moment.moment_id,
            moment.contribution_gist,
            moment.visibility,
            moment.last_activity_at,
        )
    return ""


def _canon_version(item: IntelligencePacketItem) -> str:
    for fact in CANON_FACTS:
        if _canon_digest(fact) == item.revalidation_key:
            return item.revalidation_key
    return ""


def _recognized_canon_version(
    conn: sqlite3.Connection,
    packet: UnifiedIntelligencePacket,
    item: IntelligencePacketItem,
    *,
    environ: Mapping[str, str] | None = None,
) -> str:
    signal = _canon_identity_signal(
        conn,
        packet.request,
        environ=environ,
    )
    if not signal.recognized or signal.subject is None:
        return ""
    parts = str(item.source_ref or "").split(":", 3)
    if (
        len(parts) != 4
        or parts[0] != "canon_signal"
        or parts[1] != CANON_SOURCE_CONTRACT_VERSION
        or parts[2] != signal.subject.key
    ):
        return ""
    predicate = parts[3]
    fact = next(
        (
            candidate
            for candidate in CANON_FACTS
            if candidate.subject.key == signal.subject.key
            and candidate.predicate == predicate
        ),
        None,
    )
    if fact is None:
        return ""
    return _recognized_canon_digest(
        fact,
        signal,
        _request_subject_key(packet.request),
    )


def _living_atomic_version(
    conn: sqlite3.Connection,
    item: IntelligencePacketItem,
) -> str:
    candidate_id = str(item.revalidation_key or "")
    candidate = _atomic_candidate_row(conn, candidate_id)
    roots = _atomic_root_snapshot(conn, candidate_id)
    claim, reason = _living_claim_for_candidate(conn, candidate, roots)
    if claim is None or reason:
        return ""
    root_entry_ids = tuple(
        str(root.get("root_entry_id") or "")
        for root in roots
        if str(root.get("root_entry_id") or "")
    )
    root_identities = tuple(
        dict.fromkeys(
            identity
            for identity in (
                knowledge_root_identity(conn, entry_id)
                for entry_id in root_entry_ids
            )
            if identity
        )
    )
    occurrence_identities = tuple(
        dict.fromkeys(
            identity
            for identity in (
                knowledge_occurrence_identity(conn, entry_id)
                for entry_id in root_entry_ids
            )
            if identity
        )
    )
    if not (
        item.canon_status == claim.canon_status.value
        and item.canon_domain == claim.domain.value
        and item.canon_claim_kind == claim.claim_kind.value
        and item.root_identities == root_identities
        and item.occurrence_identities == occurrence_identities
    ):
        return ""
    return _living_atomic_candidate_digest(conn, candidate_id)


def _declared_version(
    conn: sqlite3.Connection,
    packet: UnifiedIntelligencePacket,
    item: IntelligencePacketItem,
    *,
    environ: Mapping[str, str] | None = None,
) -> str:
    selection = select_declared_canon_claims_for_packet(
        conn,
        guild_id=int(packet.request.guild_id or 0),
        route_mode=packet.request.route_mode,
        channel_policy=packet.request.channel_policy,
        capability_authorized=bool(
            packet.request.declared_canon_authorized
        ),
        limit=200,
        now=packet.request.now or None,
    )
    if selection.reason != "eligible":
        return ""
    claim = next(
        (
            candidate
            for candidate in selection.claims
            if candidate.claim_id == item.revalidation_key
        ),
        None,
    )
    bound_entity_claim = False
    if claim is not None and item.source_type == "recognized_declared_canon_claim":
        signal = _canon_identity_signal(
            conn,
            packet.request,
            environ=environ,
        )
        bound_entity_id = (
            signal.subject.key
            if signal.subject is not None
            and signal.status in {"recognized", "bound_non_signal_identity"}
            else ""
        )
        bound_entity_claim = bool(
            bound_entity_id
            and (
                claim.subject_id == bound_entity_id
                or (
                    claim.claim_kind.value == "relationship"
                    and isinstance(claim.value, Mapping)
                    and str(claim.value.get("object_subject_id") or "")
                    == bound_entity_id
                )
            )
        )
    expected_subject_key = (
        _request_subject_key(packet.request)
        if bound_entity_claim
        else (claim.subject_id if claim is not None else "")
    )
    if claim is None or not (
        item.source_ref == claim.source_refs[0]
        and item.source_digest == claim.revision_id
        and item.subject_key == expected_subject_key
        and item.predicate_key == claim.predicate
        and item.text == _declared_claim_text(claim).strip()[:1000]
        and item.visibility == claim.visibility.value
        and item.lifecycle == claim.lifecycle.value
        and item.root_identities == claim.root_ids
        and item.occurrence_identities == claim.occurrence_ids
        and item.canon_status == claim.canon_status.value
        and item.canon_domain == claim.domain.value
        and item.canon_claim_kind == claim.claim_kind.value
    ):
        return ""
    return claim.revision_id


def _relationship_version(
    conn: sqlite3.Connection,
    packet: UnifiedIntelligencePacket,
    item: IntelligencePacketItem,
    *,
    environ: Mapping[str, str] | None = None,
) -> str:
    posture = shadow_packet_posture(
        conn,
        guild_id=packet.request.guild_id,
        user_id=packet.request.subject_user_id,
        route_mode=packet.request.route_mode,
        channel_policy=packet.request.channel_policy,
        direct=packet.request.direct_state == "direct",
        target_user_id=packet.request.subject_user_id,
        environ=environ,
    )
    return str(posture.get("source_digest") or "")


def _revalidate_packet_in_snapshot(
    conn: sqlite3.Connection,
    packet: UnifiedIntelligencePacket,
    *,
    environ: Mapping[str, str] | None = None,
) -> PacketRevalidationResult:
    """Re-read every durable source without applying packet content live."""
    changed = 0
    errors = 0
    current_subject_resolution = resolve_packet_subject(
        conn,
        packet.request,
        environ=environ,
    )
    legacy_resolution_unspecified = bool(
        not str(packet.request.frame_revision or "").strip()
        and packet.subject_resolution.status == "unresolved"
        and not packet.subject_resolution.binding_digest
        and not packet.subject_resolution.subject_key
    )
    subject_changed = not legacy_resolution_unspecified and (
        current_subject_resolution.status
        != packet.subject_resolution.status
        or current_subject_resolution.subject_user_id
        != packet.subject_resolution.subject_user_id
        or current_subject_resolution.subject_key
        != packet.subject_resolution.subject_key
        or current_subject_resolution.entity_ref
        != packet.subject_resolution.entity_ref
        or current_subject_resolution.binding_method
        != packet.subject_resolution.binding_method
        or current_subject_resolution.binding_digest
        != packet.subject_resolution.binding_digest
    )
    if subject_changed:
        changed += 1
    revalidation_items = tuple(
        {
            (item.lane, item.source_ref, item.source_digest): item
            for item in (*packet.items, *packet.validation_items)
        }.values()
    )
    for item in revalidation_items:
        try:
            if item.revalidation_kind == "conversation":
                row = _conversation_row(conn, int(item.revalidation_key or 0))
                current = _conversation_digest(row) if row else ""
            elif item.revalidation_kind == "public_assessment":
                state = read_public_assessment_root_state(
                    conn,
                    entry_id=item.revalidation_key,
                    guild_id=packet.request.guild_id,
                    subject_key=item.subject_key,
                )
                context_row_id = (
                    item.source_ref.split(":", 1)[1]
                    if item.lane == "conversation_context"
                    and item.source_ref.startswith("conversation:")
                    else ""
                )
                state_matches = bool(
                    state is not None
                    and item.root_identities == (state.root_identity,)
                    and item.occurrence_identities
                    == (state.occurrence_identity,)
                    and item.point_identity
                    == state.semantics.point_identity
                    and item.point_group_identity
                    == state.semantics.point_identity
                    and item.attribution_mode
                    == state.semantics.attribution_mode
                    and item.polarity == state.semantics.polarity
                    and item.action_identity
                    == state.semantics.action_identity
                    and item.material_facets
                    == state.semantics.material_facets
                    and item.text == state.text[:1200]
                    and (
                        not context_row_id
                        or context_row_id == state.source_row_id
                    )
                )
                current = state.source_digest if state_matches else ""
            elif item.revalidation_kind == "ledger":
                current = _ledger_entry_digest(conn, item.revalidation_key)
            elif item.revalidation_kind == "moment":
                current = _moment_version(conn, packet, item)
            elif item.revalidation_kind == "atomic":
                current = (
                    _living_atomic_version(conn, item)
                    if item.canon_status == CanonStatus.LIVING.value
                    else _atomic_candidate_digest(
                        conn,
                        item.revalidation_key,
                    )
                )
            elif item.revalidation_kind == "canon":
                current = _canon_version(item)
            elif item.revalidation_kind == "recognized_canon":
                current = _recognized_canon_version(
                    conn,
                    packet,
                    item,
                    environ=environ,
                )
            elif item.revalidation_kind == "declared":
                current = _declared_version(
                    conn,
                    packet,
                    item,
                    environ=environ,
                )
            elif item.revalidation_kind == "relationship":
                current = _relationship_version(
                    conn,
                    packet,
                    item,
                    environ=environ,
                )
            elif item.revalidation_kind in {"current", "snapshot"}:
                current = item.revalidation_key
            else:
                current = ""
            if not current or current != item.source_digest:
                changed += 1
        except (sqlite3.DatabaseError, TypeError, ValueError):
            errors += 1
    if errors:
        status = "processing_error"
    elif subject_changed:
        status = "subject_binding_changed"
    elif not current_subject_resolution.applicable:
        status = "subject_%s" % current_subject_resolution.status
    elif changed:
        status = "source_changed"
    elif any(
        item.revalidation_kind == "snapshot"
        for item in revalidation_items
    ):
        status = "passed_with_provider_snapshot"
    else:
        status = "passed"
    return PacketRevalidationResult(
        valid=not errors and not changed,
        status=status,
        changed_source_count=changed,
        processing_error_count=errors,
        subject_resolution_status=current_subject_resolution.status,
    )


def revalidate_packet(
    conn: sqlite3.Connection,
    packet: UnifiedIntelligencePacket,
    *,
    environ: Mapping[str, str] | None = None,
) -> PacketRevalidationResult:
    """Revalidate every source against one coherent database snapshot."""

    owns_snapshot = not conn.in_transaction
    if owns_snapshot:
        conn.execute("BEGIN")
    try:
        return _revalidate_packet_in_snapshot(
            conn,
            packet,
            environ=environ,
        )
    finally:
        if owns_snapshot and conn.in_transaction:
            # Revalidation is strictly read-only. Ending an owned snapshot by
            # rollback prevents this helper from committing caller work if a
            # later refactor accidentally adds writes.
            conn.rollback()


def _packet_invariants(
    packet: UnifiedIntelligencePacket,
) -> tuple[str, ...]:
    invalid = []
    subject = _request_subject_key(packet.request)
    accepted_subject_keys = {
        value
        for value in (
            packet.subject_resolution.subject_key,
            packet.subject_resolution.entity_ref,
        )
        if value
    }
    governed_subject_required = bool(
        str(packet.request.frame_subject_requirement or "").strip().lower()
        == "required"
    )
    subject_scope_enforced = bool(
        governed_subject_required
        or (
            not str(packet.request.frame_revision or "").strip()
            and int(packet.request.subject_user_id or 0) > 0
        )
    )
    if (
        str(packet.request.frame_revision or "").strip()
        and not packet.subject_resolution.applicable
        and any(item.lane != "current_intent" for item in packet.items)
    ):
        invalid.append("unresolved_frame_subject_selected_content")
    if str(packet.request.frame_revision or "").strip() and not (
        packet.request.frame_input_evidence_digest
        and packet.source_snapshot_digest
    ):
        invalid.append("frame_packet_digest_missing")
    if (
        packet.diagnostics.subject_resolution_status
        != packet.subject_resolution.status
    ):
        invalid.append("subject_resolution_receipt_mismatch")
    broad = _request_is_broad_profile(packet.request)
    for item in packet.items:
        if not _route_allows_item(packet.request, item):
            invalid.append("selected_visibility_violation")
        if (
            item.lane
            in {
                "approved_fact",
                "assessment_observation",
                "atomic_knowledge",
                "open_loop",
                "moment",
                "relationship_posture",
            }
            and subject_scope_enforced
            and item.subject_key not in accepted_subject_keys
        ):
            invalid.append("selected_subject_violation")
        if item.lane == "relationship_posture" and item.usage != "tone_only":
            invalid.append("relationship_fact_authority_violation")
        if item.revalidation_kind == "atomic" and item.lifecycle not in {
            "established",
            "provisional",
        }:
            invalid.append("atomic_state_violation")
        if item.revalidation_kind == "recognized_canon" and not (
            item.lane == "canon"
            and item.source_type == "recognized_canon_fact"
            and item.subject_key == subject
            and item.source_class == SourceClass.APPROVED_CANON.value
        ):
            invalid.append("recognized_canon_scope_violation")
        if (
            item.revalidation_kind == "atomic"
            and item.source_class == SourceClass.LEGACY_SOURCE_BLIND.value
        ):
            invalid.append("atomic_source_blind_violation")
        if (
            item.revalidation_kind == "atomic"
            and not _atomic_member_fact_authorized(
                {
                    "candidate_type": item.source_type,
                    "subject_key": item.subject_key,
                    "predicate_key": item.predicate_key,
                },
                item.source_class,
            )
        ):
            invalid.append("atomic_member_fact_authority_violation")
        if (
            broad
            and item.lane in _PROFILE_MEMBER_EVIDENCE_LANES
            and item.lane != "conversation_context"
            and (
                not item.root_identities
                or not item.occurrence_identities
                or not item.point_identity
            )
        ):
            invalid.append("profile_item_root_lineage_violation")
        if item.supporting_observations and not (
            item.lane == "atomic_knowledge"
            and item.source_type == "topic_or_motif"
            and item.subject_key == subject
        ):
            invalid.append("supporting_observation_scope_violation")
        if item.canon_status == CanonStatus.LIVING.value and not (
            item.lane == "atomic_knowledge"
            and item.source_type == "topic_or_motif"
            and item.lifecycle == "established"
            and item.source_class == SourceClass.EVIDENCE_PROJECTION.value
            and item.revalidation_kind == "atomic"
            and item.canon_domain in {"real_community", "lore", "hybrid"}
            and item.canon_claim_kind == "behavior_pattern"
            and len(item.root_identities) >= 2
            and len(item.occurrence_identities) >= 2
        ):
            invalid.append("living_canon_contract_violation")
        if item.canon_status == CanonStatus.OPEN_SIGNAL.value and not (
            item.lane == "assessment_observation"
            and item.revalidation_kind == "public_assessment"
        ):
            invalid.append("open_signal_contract_violation")
        if item.canon_status == CanonStatus.LEGACY.value and not (
            item.lane == "canon"
            and item.revalidation_kind in {"canon", "recognized_canon"}
        ):
            invalid.append("legacy_canon_contract_violation")
        if item.canon_status == CanonStatus.DECLARED.value and not (
            item.source_type
            in {"declared_canon_claim", "recognized_declared_canon_claim"}
            and item.lane in {"approved_fact", "canon"}
            and item.lifecycle == "established"
            and item.revalidation_kind == "declared"
            and bool(item.root_identities)
            and bool(item.occurrence_identities)
            and (
                item.lane != "approved_fact"
                or item.subject_key == subject
            )
            and (
                item.source_type != "recognized_declared_canon_claim"
                or (
                    item.lane == "canon"
                    and item.subject_key == subject
                )
            )
        ):
            invalid.append("declared_canon_contract_violation")
        if any(
            (item.canon_status, item.canon_domain, item.canon_claim_kind)
        ) and not all(
            (item.canon_status, item.canon_domain, item.canon_claim_kind)
        ):
            invalid.append("partial_canon_metadata_violation")
    validation_keys = {
        (item.lane, item.source_ref, item.source_digest)
        for item in packet.validation_items
    }
    for item in packet.validation_items:
        if item.lane not in _VALIDATION_SUPPORT_LANES:
            invalid.append("validation_support_lane_violation")
        if not _route_allows_item(packet.request, item):
            invalid.append("validation_support_visibility_violation")
        if (
            item.lane in _CLAIM_SUBJECT_SCOPED_LANES
            and subject_scope_enforced
            and item.subject_key not in accepted_subject_keys
        ):
            invalid.append("validation_support_subject_violation")
        if (
            broad
            and item.lane in _PROFILE_MEMBER_EVIDENCE_LANES
            and item.lane != "conversation_context"
            and (
                not item.root_identities
                or not item.occurrence_identities
                or not item.point_identity
            )
        ):
            invalid.append("validation_support_root_lineage_violation")
        if item.revalidation_kind == "recognized_canon" and not (
            item.lane == "canon"
            and item.source_type == "recognized_canon_fact"
            and item.subject_key == subject
            and item.source_class == SourceClass.APPROVED_CANON.value
        ):
            invalid.append("validation_support_canon_scope_violation")
    for item in packet.items:
        if (
            item.lane in _VALIDATION_SUPPORT_LANES
            and item.lane != "canon"
            and (item.lane, item.source_ref, item.source_digest)
            not in validation_keys
        ):
            invalid.append("selected_factual_item_missing_validation_support")
    return tuple(dict.fromkeys(invalid))


def build_packet(
    conn: sqlite3.Connection,
    request: IntelligencePacketRequest,
    *,
    persist: bool = True,
    environ: Mapping[str, str] | None = None,
) -> UnifiedIntelligencePacket | None:
    """Build one deterministic shadow packet from existing source owners."""
    if not shadow_enabled(environ):
        return None
    ensure_schema(conn)
    diagnostics = IntelligencePacketDiagnostics()
    exclusions: list[IntelligencePacketExclusion] = []
    subject_resolution = resolve_packet_subject(
        conn,
        request,
        environ=environ,
    )
    diagnostics.subject_resolution_status = subject_resolution.status
    diagnostics.subject_resolution_method = (
        subject_resolution.binding_method
    )
    diagnostics.subject_resolution_candidate_count = int(
        subject_resolution.candidate_count or 0
    )
    request = replace(
        request,
        subject_user_id=int(subject_resolution.subject_user_id or 0),
        subject_entity_ref=str(subject_resolution.entity_ref or ""),
    )
    broad = _request_is_broad_profile(request)
    request_terms = _terms(request.user_text)
    candidates: list[IntelligencePacketItem] = []
    try:
        candidates.extend(
            _conversation_items(conn, request, diagnostics, exclusions)
        )
        candidates.extend(
            _assessment_observation_items(
                conn,
                request,
                diagnostics,
                exclusions,
                broad=broad,
            )
        )
        candidates.extend(
            _governed_items(
                conn,
                request,
                diagnostics,
                exclusions,
                broad=broad,
            )
        )
        candidates.extend(
            _declared_items(
                conn,
                request,
                diagnostics,
                exclusions,
                broad=broad,
                request_terms=request_terms,
                environ=environ,
            )
        )
        candidates.extend(
            _atomic_items(
                conn,
                request,
                diagnostics,
                exclusions,
                broad=broad,
                request_terms=request_terms,
            )
        )
        candidates.extend(
            _canon_items(
                conn,
                request,
                diagnostics,
                exclusions,
                request_terms=request_terms,
                environ=environ,
            )
        )
        candidates.extend(
            _source_file_items(request, diagnostics, exclusions)
        )
        candidates.extend(
            _relationship_items(
                conn,
                request,
                diagnostics,
                environ=environ,
            )
        )
    except (sqlite3.DatabaseError, TypeError, ValueError) as exc:
        diagnostics.processing_errors.append(type(exc).__name__)
    for item in candidates:
        if item.canon_status:
            diagnostics.candidates_by_canon_status[item.canon_status] = (
                diagnostics.candidates_by_canon_status.get(
                    item.canon_status,
                    0,
                )
                + 1
            )
        if item.canon_domain:
            diagnostics.candidates_by_canon_domain[item.canon_domain] = (
                diagnostics.candidates_by_canon_domain.get(
                    item.canon_domain,
                    0,
                )
                + 1
            )
    selected, profile_candidates, validation_items = _select_items(
        request,
        subject_resolution,
        candidates,
        diagnostics,
        exclusions,
    )
    profile_sufficiency = _profile_sufficiency(
        request,
        selected=selected,
        candidates=profile_candidates,
    )
    prompt_digest_payload = tuple(
        (
            item.lane,
            item.source_class,
            item.source_ref,
            item.source_digest,
            item.lifecycle,
            item.usage,
            item.root_identities,
            item.occurrence_identities,
            item.point_identity,
            item.canon_status,
            item.canon_domain,
            item.canon_claim_kind,
        )
        for item in selected
    )
    validation_digest_payload = tuple(
        (
            item.lane,
            item.source_class,
            item.source_ref,
            item.source_digest,
            item.lifecycle,
            item.usage,
            item.root_identities,
            item.occurrence_identities,
            item.point_identity,
            item.canon_status,
            item.canon_domain,
            item.canon_claim_kind,
        )
        for item in validation_items
    )
    diagnostics.packet_digest = _digest(
        SCHEMA_VERSION,
        request.frame_revision,
        request.frame_input_evidence_digest,
        subject_resolution,
        prompt_digest_payload,
        validation_digest_payload,
        profile_sufficiency,
    )
    source_snapshot_digest = _digest(
        SOURCE_SNAPSHOT_VERSION,
        request.frame_revision,
        request.frame_input_evidence_digest,
        subject_resolution.binding_digest,
        prompt_digest_payload,
        validation_digest_payload,
    )
    packet_id = "uip_" + _digest(
        SCHEMA_VERSION,
        request.guild_id,
        subject_resolution.subject_key,
        request.route_mode,
        request.channel_policy,
        request.frame_revision,
        diagnostics.packet_digest,
    )[:40]
    packet = UnifiedIntelligencePacket(
        schema_version=SCHEMA_VERSION,
        packet_id=packet_id,
        request=request,
        items=selected,
        exclusions=tuple(exclusions),
        diagnostics=diagnostics,
        subject_resolution=subject_resolution,
        source_snapshot_digest=source_snapshot_digest,
        profile_sufficiency=profile_sufficiency,
        validation_items=validation_items,
    )
    invalid = _packet_invariants(packet)
    if invalid:
        diagnostics.invalid_invariants.extend(invalid)
    revalidation = revalidate_packet(conn, packet, environ=environ)
    diagnostics.revalidation_status = revalidation.status
    diagnostics.revalidation_changed_count = (
        revalidation.changed_source_count
    )
    if revalidation.processing_error_count:
        diagnostics.processing_errors.extend(
            "revalidation_error"
            for _ in range(revalidation.processing_error_count)
        )
    if not revalidation.valid:
        diagnostics.invalid_invariants.append(
            "packet_source_revalidation_failed"
        )
    diagnostics.invalid_invariants = list(
        dict.fromkeys(diagnostics.invalid_invariants)
    )
    if persist:
        diagnostics.receipt_run_id = persist_packet_run(
            conn,
            packet,
            created_at=request.now or "",
        )
    return packet


def persist_packet_run(
    conn: sqlite3.Connection,
    packet: UnifiedIntelligencePacket,
    *,
    created_at: str = "",
) -> str:
    """Persist aggregate packet evidence without content or source identifiers."""
    ensure_schema(conn)
    run_id = "uipr_" + uuid.uuid4().hex
    source_ref_digest = _digest(
        tuple(
            sorted(
                {
                    item.source_ref
                    for item in (*packet.items, *packet.validation_items)
                }
            )
        )
    )
    conn.execute(
        """
        INSERT INTO memory_governance_intelligence_packet_runs(
          run_id,packet_id,schema_version,guild_id,subject_hash,route_mode,
          channel_policy,visibility_allowance,item_count,
          selected_lane_counts_json,source_class_counts_json,
          atomic_state_counts_json,excluded_by_reason_json,
          missing_lanes_json,conflict_count,visibility_exclusion_count,
          budget_exclusion_count,duplicate_suppression_count,
          root_collapse_suppression_count,shared_root_projection_count,
          profile_sufficiency_status,profile_sufficiency_met,
          profile_required_point_count,profile_selected_point_count,
          profile_candidate_point_count,profile_independent_root_count,
          profile_independent_occurrence_count,profile_reason_codes_json,
          processing_error_count,invalid_invariant_count,
          revalidation_status,revalidation_changed_count,packet_digest,
          source_ref_digest,prompt_applied,live_applied,created_at
        ) VALUES(
          ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,
          ?,?,?,?,?,?,?
        )
        """,
        (
            run_id,
            packet.packet_id,
            packet.schema_version,
            int(packet.request.guild_id or 0),
            _digest(_request_subject_key(packet.request))[:16],
            str(packet.request.route_mode or "unknown")[:80],
            str(packet.request.channel_policy or "unknown")[:80],
            str(packet.request.visibility_allowance or "unknown")[:80],
            len(packet.items),
            json.dumps(packet.diagnostics.selected_by_lane, sort_keys=True),
            json.dumps(
                packet.diagnostics.selected_by_source_class,
                sort_keys=True,
            ),
            json.dumps(
                packet.diagnostics.selected_atomic_states,
                sort_keys=True,
            ),
            json.dumps(
                packet.diagnostics.excluded_by_reason,
                sort_keys=True,
            ),
            json.dumps(packet.diagnostics.missing_lanes),
            len(packet.diagnostics.conflict_reasons),
            int(packet.diagnostics.visibility_exclusions or 0),
            int(packet.diagnostics.budget_exclusions or 0),
            int(packet.diagnostics.duplicate_suppression or 0),
            int(packet.diagnostics.root_collapse_suppression or 0),
            int(packet.diagnostics.shared_root_projection_count or 0),
            packet.profile_sufficiency.status,
            int(bool(packet.profile_sufficiency.satisfied)),
            int(packet.profile_sufficiency.required_point_count or 0),
            int(packet.profile_sufficiency.selected_point_count or 0),
            int(packet.profile_sufficiency.candidate_point_count or 0),
            int(packet.profile_sufficiency.independent_root_count or 0),
            int(
                packet.profile_sufficiency.independent_occurrence_count
                or 0
            ),
            json.dumps(packet.profile_sufficiency.reason_codes),
            len(packet.diagnostics.processing_errors),
            len(packet.diagnostics.invalid_invariants),
            packet.diagnostics.revalidation_status,
            int(packet.diagnostics.revalidation_changed_count or 0),
            packet.diagnostics.packet_digest,
            source_ref_digest,
            0,
            0,
            created_at or _now(),
        ),
    )
    conn.execute(
        """
        DELETE FROM memory_governance_intelligence_packet_runs
        WHERE guild_id=? AND run_id NOT IN (
          SELECT run_id FROM memory_governance_intelligence_packet_runs
          WHERE guild_id=?
          ORDER BY created_at DESC,run_id DESC LIMIT 1000
        )
        """,
        (
            int(packet.request.guild_id or 0),
            int(packet.request.guild_id or 0),
        ),
    )
    conn.execute(
        """
        UPDATE memory_governance_intelligence_packet_runs
        SET validation_item_count=?,validation_lane_counts_json=?,
            frame_revision=?,frame_input_digest=?,
            subject_resolution_status=?,subject_resolution_method=?,
            subject_resolution_candidate_count=?,
            frame_applicability_exclusion_count=?,source_snapshot_digest=?
        WHERE run_id=?
        """,
        (
            len(packet.validation_items),
            json.dumps(
                packet.diagnostics.validation_support_by_lane,
                sort_keys=True,
            ),
            str(packet.request.frame_revision or ""),
            str(packet.request.frame_input_evidence_digest or ""),
            packet.diagnostics.subject_resolution_status,
            packet.diagnostics.subject_resolution_method,
            int(
                packet.diagnostics.subject_resolution_candidate_count or 0
            ),
            int(
                packet.diagnostics.frame_applicability_exclusion_count or 0
            ),
            packet.source_snapshot_digest,
            run_id,
        ),
    )
    return run_id


def mark_packet_application(
    conn: sqlite3.Connection,
    packet: UnifiedIntelligencePacket,
    *,
    prompt_applied: bool | None = None,
    live_applied: bool | None = None,
) -> bool:
    """Update the exact retained packet receipt used by a scoped canary.

    Packet contents and source identifiers remain in memory only.  The
    persisted receipt records only whether the already-validated packet reached
    a prompt or a sent response.
    """

    ensure_schema(conn)
    run_id = str(packet.diagnostics.receipt_run_id or "").strip()
    if not run_id:
        return False
    assignments = []
    values: list[Any] = []
    if prompt_applied is not None:
        assignments.append("prompt_applied=?")
        values.append(int(bool(prompt_applied)))
    if live_applied is not None:
        assignments.append("live_applied=?")
        values.append(int(bool(live_applied)))
    if not assignments:
        return True
    values.extend(
        (
            run_id,
            packet.packet_id,
            int(packet.request.guild_id or 0),
        )
    )
    cursor = conn.execute(
        """
        UPDATE memory_governance_intelligence_packet_runs
        SET %s
        WHERE run_id=? AND packet_id=? AND guild_id=?
        """
        % ",".join(assignments),
        tuple(values),
    )
    if cursor.rowcount != 1:
        return False
    if prompt_applied is not None:
        packet.diagnostics.prompt_applied = bool(prompt_applied)
    if live_applied is not None:
        packet.diagnostics.live_applied = bool(live_applied)
    return True


def _safe_json(value: Any, fallback: Any) -> Any:
    try:
        return json.loads(str(value or ""))
    except (TypeError, ValueError, json.JSONDecodeError):
        return fallback


def _empty_report() -> dict[str, Any]:
    return {
        "tablePresent": False,
        "schemaVersion": SCHEMA_VERSION,
        "runs": 0,
        "itemTotal": 0,
        "validationItemTotal": 0,
        "validationByLane": {},
        "selectedByLane": {},
        "selectedBySourceClass": {},
        "selectedAtomicStates": {},
        "excludedByReason": {},
        "missingLaneCounts": {},
        "conflictRuns": 0,
        "visibilityExclusions": 0,
        "budgetExclusions": 0,
        "duplicateSuppressions": 0,
        "rootCollapseSuppressions": 0,
        "sharedRootProjections": 0,
        "profileSufficiencyStatusCounts": {},
        "profileSufficiencyMetRuns": 0,
        "profileSelectedPointTotal": 0,
        "profileIndependentRootTotal": 0,
        "profileIndependentOccurrenceTotal": 0,
        "profileReasonCodeCounts": {},
        "processingErrors": 0,
        "invalidInvariants": 0,
        "revalidationStatusCounts": {},
        "revalidationChangedRuns": 0,
        "subjectResolutionStatusCounts": {},
        "subjectResolutionMethodCounts": {},
        "frameApplicabilityExclusions": 0,
        "promptAppliedRuns": 0,
        "liveAppliedRuns": 0,
        "contentFieldsPresent": [],
        "evidenceWindow": {"first": "none", "last": "none"},
    }


def build_evaluation_report(
    conn: sqlite3.Connection,
    *,
    guild_id: int,
    prepare_schema: bool = False,
    limit: int = 1000,
) -> dict[str, Any]:
    """Return aggregate-only packet evidence for owner review."""
    if prepare_schema:
        ensure_schema(conn)
    if not conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (TABLE_NAME,),
    ).fetchone():
        return _empty_report()
    columns = {
        str(row[1]) for row in conn.execute("PRAGMA table_info(%s)" % TABLE_NAME)
    }
    disallowed = sorted(
        columns
        & {
            "request_text",
            "source_text",
            "source_ids",
            "source_refs",
            "participant_ids",
            "relationship_posture",
            "packet_content",
        }
    )
    def column(name: str, fallback: str) -> str:
        return name if name in columns else "%s AS %s" % (fallback, name)

    rows = conn.execute(
        """
        SELECT schema_version,item_count,%s,%s,selected_lane_counts_json,
               source_class_counts_json,atomic_state_counts_json,
               excluded_by_reason_json,missing_lanes_json,conflict_count,
               visibility_exclusion_count,budget_exclusion_count,
               duplicate_suppression_count,
               %s,%s,%s,%s,%s,%s,%s,%s,%s,
               processing_error_count,
               invalid_invariant_count,revalidation_status,
               revalidation_changed_count,prompt_applied,live_applied,
               %s,%s,%s,
               created_at
        FROM memory_governance_intelligence_packet_runs
        WHERE guild_id=?
        ORDER BY created_at DESC,run_id DESC
        LIMIT ?
        """
        % (
            column("validation_item_count", "0"),
            column("validation_lane_counts_json", "'{}'"),
            column("root_collapse_suppression_count", "0"),
            column("shared_root_projection_count", "0"),
            column(
                "profile_sufficiency_status",
                "'not_applicable'",
            ),
            column("profile_sufficiency_met", "0"),
            column("profile_selected_point_count", "0"),
            column("profile_independent_root_count", "0"),
            column("profile_independent_occurrence_count", "0"),
            column("profile_reason_codes_json", "'[]'"),
            column("profile_candidate_point_count", "0"),
            column("subject_resolution_status", "'not_evaluated'"),
            column("subject_resolution_method", "'none'"),
            column("frame_applicability_exclusion_count", "0"),
        ),
        (int(guild_id or 0), max(1, min(int(limit or 1000), 5000))),
    ).fetchall()
    selected_lanes: Counter[str] = Counter()
    validation_lanes: Counter[str] = Counter()
    source_classes: Counter[str] = Counter()
    atomic_states: Counter[str] = Counter()
    exclusions: Counter[str] = Counter()
    missing: Counter[str] = Counter()
    revalidation: Counter[str] = Counter()
    profile_statuses: Counter[str] = Counter()
    profile_reasons: Counter[str] = Counter()
    subject_statuses: Counter[str] = Counter()
    subject_methods: Counter[str] = Counter()
    item_total = validation_item_total = 0
    conflicts = visibility = budget = duplicates = 0
    root_collapses = shared_roots = profile_met = 0
    profile_points = profile_roots = profile_occurrences = 0
    errors = invalid = changed = prompt = live = frame_exclusions = 0
    for row in rows:
        (
            _schema,
            item_count,
            validation_item_count,
            validation_lane_json,
            lane_json,
            source_json,
            atomic_json,
            exclusion_json,
            missing_json,
            conflict_count,
            visibility_count,
            budget_count,
            duplicate_count,
            root_collapse_count,
            shared_root_count,
            profile_status,
            profile_satisfied,
            profile_selected_points,
            profile_independent_roots,
            profile_independent_occurrences,
            profile_reason_json,
            _profile_candidate_points,
            error_count,
            invalid_count,
            revalidation_status,
            changed_count,
            prompt_applied,
            live_applied,
            subject_resolution_status,
            subject_resolution_method,
            frame_exclusion_count,
            _created_at,
        ) = row
        item_total += int(item_count or 0)
        validation_item_total += int(validation_item_count or 0)
        for counter, raw in (
            (selected_lanes, lane_json),
            (validation_lanes, validation_lane_json),
            (source_classes, source_json),
            (atomic_states, atomic_json),
            (exclusions, exclusion_json),
        ):
            parsed = _safe_json(raw, {})
            if isinstance(parsed, dict):
                counter.update(
                    {
                        str(key): max(0, int(value or 0))
                        for key, value in parsed.items()
                    }
                )
            else:
                errors += 1
        parsed_missing = _safe_json(missing_json, [])
        if isinstance(parsed_missing, list):
            missing.update(str(value) for value in parsed_missing)
        else:
            errors += 1
        conflicts += int(conflict_count or 0)
        visibility += int(visibility_count or 0)
        budget += int(budget_count or 0)
        duplicates += int(duplicate_count or 0)
        root_collapses += int(root_collapse_count or 0)
        shared_roots += int(shared_root_count or 0)
        profile_statuses[str(profile_status or "not_applicable")] += 1
        profile_met += int(bool(profile_satisfied))
        profile_points += int(profile_selected_points or 0)
        profile_roots += int(profile_independent_roots or 0)
        profile_occurrences += int(profile_independent_occurrences or 0)
        parsed_profile_reasons = _safe_json(profile_reason_json, [])
        if isinstance(parsed_profile_reasons, list):
            profile_reasons.update(
                str(reason) for reason in parsed_profile_reasons
            )
        else:
            errors += 1
        errors += int(error_count or 0)
        invalid += int(invalid_count or 0)
        changed += int(bool(changed_count))
        prompt += int(bool(prompt_applied))
        live += int(bool(live_applied))
        revalidation[str(revalidation_status or "unknown")] += 1
        subject_statuses[
            str(subject_resolution_status or "not_evaluated")
        ] += 1
        subject_methods[str(subject_resolution_method or "none")] += 1
        frame_exclusions += int(frame_exclusion_count or 0)
    return {
        "tablePresent": True,
        "schemaVersion": str(rows[0][0]) if rows else SCHEMA_VERSION,
        "runs": len(rows),
        "itemTotal": item_total,
        "validationItemTotal": validation_item_total,
        "validationByLane": dict(sorted(validation_lanes.items())),
        "selectedByLane": dict(sorted(selected_lanes.items())),
        "selectedBySourceClass": dict(sorted(source_classes.items())),
        "selectedAtomicStates": dict(sorted(atomic_states.items())),
        "excludedByReason": dict(sorted(exclusions.items())),
        "missingLaneCounts": dict(sorted(missing.items())),
        "conflictRuns": conflicts,
        "visibilityExclusions": visibility,
        "budgetExclusions": budget,
        "duplicateSuppressions": duplicates,
        "rootCollapseSuppressions": root_collapses,
        "sharedRootProjections": shared_roots,
        "profileSufficiencyStatusCounts": dict(
            sorted(profile_statuses.items())
        ),
        "profileSufficiencyMetRuns": profile_met,
        "profileSelectedPointTotal": profile_points,
        "profileIndependentRootTotal": profile_roots,
        "profileIndependentOccurrenceTotal": profile_occurrences,
        "profileReasonCodeCounts": dict(sorted(profile_reasons.items())),
        "processingErrors": errors,
        "invalidInvariants": invalid,
        "revalidationStatusCounts": dict(sorted(revalidation.items())),
        "revalidationChangedRuns": changed,
        "subjectResolutionStatusCounts": dict(
            sorted(subject_statuses.items())
        ),
        "subjectResolutionMethodCounts": dict(
            sorted(subject_methods.items())
        ),
        "frameApplicabilityExclusions": frame_exclusions,
        "promptAppliedRuns": prompt,
        "liveAppliedRuns": live,
        "contentFieldsPresent": disallowed,
        "evidenceWindow": {
            "first": str(rows[-1][-1]) if rows else "none",
            "last": str(rows[0][-1]) if rows else "none",
        },
    }
