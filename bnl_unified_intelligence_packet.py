"""Shadow-only governed retrieval and unified intelligence packet assembly.

This module owns no facts.  It coordinates references selected by the existing
Conversation Context, Memory Governance, Ledger, Moment, Relationship, canon,
and Source File owners into one bounded comparison packet.  The packet is never
rendered into a live prompt in this stage. Prompt items remain bounded
separately from the route-safe factual support retained for validation.
"""
from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass, field, is_dataclass
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
    CANON_FACTS,
    CANON_MEMBER_IDENTITIES,
    CANON_SOURCE_CONTRACT_VERSION,
    Confidence,
    EntityAccountBinding,
    SourceClass,
    SubjectIdentity,
    matching_canon_member_identities,
    normalize_canon_identity_label,
    resolve_entity_identity,
    strict_contract_bool,
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
    select_public_conversation_assessment_evidence,
    subject_key_for_user,
)
from bnl_moment_engine import select_public_participant_moment_gists
from bnl_relationship_engine import shadow_packet_posture


SCHEMA_VERSION = "unified_intelligence_packet_v3"
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
    | {"assessment_observation"}
)
_PROFILE_CANON_MATCH_LANES = (
    _PROFILE_MEMBER_EVIDENCE_LANES
    | {"conversation_context"}
)
_ROOT_COLLAPSE_MEMBER_LANES = (
    _PROFILE_DURABLE_MEMBER_EVIDENCE_LANES
    | {"conversation_context"}
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
    supporting_observations: tuple[str, ...] = ()


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
            "PRAGMA table_info(%s)" % str(table_name or "")
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
    if resolution.subject.key not in _AUTOMATIC_CANON_SIGNAL_SUBJECT_KEYS:
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


def _canon_identity_signal(
    conn: sqlite3.Connection,
    request: IntelligencePacketRequest,
) -> _CanonIdentitySignal:
    """Recognize one reversible same-platform canon signal.

    This is deliberately not an account merge. A current exact approved label
    must agree with at least two active public Ledger roots carrying that same
    Discord label, and every current label must resolve unambiguously.
    """

    if (
        int(request.guild_id or 0) <= 0
        or int(request.subject_user_id or 0) <= 0
    ):
        return _CanonIdentitySignal("invalid_subject_scope")
    explicit_binding = _explicit_canon_binding_signal(conn, request)
    if explicit_binding is not None:
        return explicit_binding
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
        FROM memory_ledger_entries
        WHERE guild_id=? AND subject_key=?
          AND source_table='conversations' AND source_role='user'
          AND channel_policy IN (
            'public_home','public_context','public_selective'
          )
          AND visibility IN ('public','public_safe')
          AND public_usable=1 AND derived=0 AND projection=0
          AND lifecycle_status='active'
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
        FROM memory_ledger_entries
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
            JOIN memory_ledger_entries entry
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
    return bool(
        item.lane in _PROFILE_MEMBER_EVIDENCE_LANES
        and item.subject_key == subject_key_for_user(request.subject_user_id)
        and item.point_identity
        and item.root_identities
        and item.occurrence_identities
    )


def _root_bearing_member_item(
    request: IntelligencePacketRequest,
    item: IntelligencePacketItem,
) -> bool:
    return bool(
        item.lane in _ROOT_COLLAPSE_MEMBER_LANES
        and item.subject_key == subject_key_for_user(request.subject_user_id)
        and item.root_identities
    )


def _broad_profile_request(value: str) -> bool:
    return classify_personal_recall_intent(value).broad_self_profile


def _profile_project_request(value: str) -> bool:
    return bool(_PROFILE_PROJECT_SCOPE_RE.search(str(value or "")))


def _profile_canon_anchor(
    request: IntelligencePacketRequest,
    candidates: Sequence[IntelligencePacketItem],
) -> IntelligencePacketItem | None:
    """Choose one canon point that best contextualizes selected public work."""

    if not _broad_profile_request(request.user_text):
        return None
    subject = subject_key_for_user(request.subject_user_id)
    recognized = tuple(
        item
        for item in candidates
        if item.lane == "canon"
        and item.source_type == "recognized_canon_fact"
        and item.subject_key == subject
    )
    if recognized:
        return sorted(
            recognized,
            key=lambda item: (-item.score, item.source_ref),
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
    return sorted(
        canon_items,
        key=lambda item: (
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
        for row in conn.execute("PRAGMA table_info(conversations)").fetchall()
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
        FROM conversations
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
            item = IntelligencePacketItem(
                lane=lane,
                source_class=SourceClass.PUBLIC_OBSERVATION.value,
                source_type="conversation_row",
                source_ref="conversation:%s" % int(row["id"]),
                source_digest=_conversation_digest(row),
                subject_key=row_subject,
                predicate_key="current_intent" if evidence.current_turn else "conversation_context",
                text=str(row.get("content") or "")[:1200],
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
                revalidation_kind="conversation",
                revalidation_key=str(int(row["id"])),
                root_identities=roots,
                occurrence_identities=occurrences,
                point_identity=_point_identity(
                    subject_key=row_subject,
                    predicate_key=(
                        "current_intent"
                        if evidence.current_turn
                        else "conversation_context"
                    ),
                    text=str(row.get("content") or ""),
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
        for row in conn.execute("PRAGMA table_info(memory_ledger_entries)").fetchall()
    }
    if not columns or "entry_id" not in columns:
        return ""
    selected_columns = tuple(
        column
        for column in (
            "entry_id",
            "guild_id",
            "subject_key",
            "predicate_key",
            "normalized_value",
            "source_class",
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
        "SELECT %s FROM memory_ledger_entries WHERE entry_id=?"
        % ",".join(selected_columns),
        (entry_id,),
    ).fetchone()
    if not row:
        return ""
    data = dict(zip(selected_columns, row))
    lineage = conn.execute(
        """
        SELECT lineage_type,target_entry_id
        FROM memory_ledger_lineage
        WHERE guild_id=? AND entry_id=?
        ORDER BY lineage_type,target_entry_id
        """,
        (int(data.get("guild_id") or 0), entry_id),
    ).fetchall()
    incoming = conn.execute(
        """
        SELECT entry_id,lineage_type
        FROM memory_ledger_lineage
        WHERE guild_id=? AND target_entry_id=?
          AND lineage_type IN ('correction_of','supersedes','retracts')
        ORDER BY entry_id,lineage_type
        """,
        (int(data.get("guild_id") or 0), entry_id),
    ).fetchall()
    return _digest("ledger", data, lineage, incoming)


def _assessment_observation_items(
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
    subject = subject_key_for_user(request.subject_user_id)
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
    for evidence in selection.items:
        entry_id = str(evidence.entry_id or "")
        source_digest = _ledger_entry_digest(conn, entry_id)
        root = knowledge_root_identity(conn, entry_id)
        occurrence = str(evidence.occurrence_identity or "")
        if not source_digest or not root or not occurrence:
            _add_exclusion(
                diagnostics,
                exclusions,
                lane="assessment_observation",
                reason="assessment_source_unversioned",
                source_class=SourceClass.PUBLIC_OBSERVATION.value,
            )
            continue
        item = IntelligencePacketItem(
            lane="assessment_observation",
            source_class=SourceClass.PUBLIC_OBSERVATION.value,
            source_type="public_assessment_observation",
            source_ref="ledger:%s" % entry_id,
            source_digest=source_digest,
            subject_key=subject,
            predicate_key="public_assessment_observation",
            text=str(evidence.text or "")[:240],
            visibility=str(evidence.visibility or "unknown"),
            confidence=Confidence.HIGH.value,
            lifecycle="active",
            authority=_AUTHORITY_RANK[
                SourceClass.PUBLIC_OBSERVATION.value
            ],
            participants=(subject,),
            lineage=(entry_id,),
            observed_at=str(evidence.observed_at or ""),
            usage="assessment_only",
            score=(
                92.0
                + min(24.0, float(evidence.score or 0.0))
                + (12.0 if evidence.request_relevant else 0.0)
            ),
            revalidation_kind="ledger",
            revalidation_key=entry_id,
            root_identities=(root,),
            occurrence_identities=(occurrence,),
            point_identity=_point_identity(
                subject_key=subject,
                predicate_key="public_assessment_observation",
                text=str(evidence.text or ""),
            ),
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
        "lifecycle_schema_version",
        "consolidation_id",
        "canonical_candidate_id",
        "eligible_independent_root_count",
        "reinforcement_count",
        "conflict_value_count",
        "consolidated_authority_class",
        "consolidated_confidence_class",
        "lifecycle_support_digest",
        "review_status",
        "review_due_at",
        "last_seen_at",
    )
    row = conn.execute(
        "SELECT %s FROM memory_ledger_knowledge_candidates WHERE candidate_id=?"
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
        FROM memory_ledger_knowledge_roots r
        JOIN memory_ledger_knowledge_candidates c
          ON c.candidate_id=r.candidate_id
        LEFT JOIN memory_ledger_entries e ON e.entry_id=r.root_entry_id
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
    roots = _atomic_root_snapshot(conn, candidate_id)
    incoming = []
    for root in roots:
        incoming.extend(
            conn.execute(
                """
                SELECT entry_id,lineage_type
                FROM memory_ledger_lineage
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
    return _digest("atomic", candidate, roots, sorted(incoming))


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
    subject = subject_key_for_user(request.subject_user_id)
    candidate_ids = tuple(
        str(row[0])
        for row in conn.execute(
            """
            SELECT candidate_id
            FROM memory_ledger_knowledge_candidates
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
        if candidate.get("lifecycle_schema_version") != (
            ATOMIC_KNOWLEDGE_LIFECYCLE_SCHEMA_VERSION
        ):
            reason = "atomic_lifecycle_not_reconciled"
        elif str(candidate.get("candidate_state") or "") not in {
            "established",
            "provisional",
        }:
            reason = "atomic_state"
        elif not bool(candidate.get("candidate_eligible")):
            reason = "atomic_ineligible"
        elif int(candidate.get("live_eligible") or 0):
            diagnostics.invalid_invariants.append(
                "atomic_live_eligible_selected_in_shadow"
            )
            reason = "atomic_live_eligible_invariant"
        elif str(candidate.get("invalidated_reason") or ""):
            reason = "atomic_invalidated"
        elif int(candidate.get("conflict_value_count") or 0) > 1:
            reason = "atomic_contested"
        elif review_status in {"due", "retired_stale"} or (
            review_due is not None and review_due <= request_now
        ):
            reason = "atomic_review_due"
        elif review_status not in {
            "current",
            "not_required",
        }:
            reason = "atomic_review_not_current"
        elif str(candidate.get("epistemic_status") or "") in {
            "inference",
            "contested",
        }:
            reason = "atomic_inference_or_contested"
        elif not _atomic_member_fact_authorized(
            candidate,
            authority_class,
        ):
            reason = "atomic_member_fact_not_authorized"
        visibility = str(candidate.get("visibility") or "unknown")
        if not reason and _public_route(request) and visibility not in _PUBLIC_VISIBILITIES:
            reason = "atomic_visibility"
            diagnostics.visibility_exclusions += 1
        elif not reason and not _public_route(request) and visibility not in _INTERNAL_VISIBILITIES:
            reason = "atomic_visibility"
            diagnostics.visibility_exclusions += 1
        roots = _atomic_root_snapshot(conn, candidate_id)
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
                    SELECT 1 FROM memory_ledger_lineage
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
            _atomic_candidate_digest(conn, candidate_id) if not reason else ""
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
        confidence = str(
            candidate.get("consolidated_confidence_class")
            or candidate.get("confidence_class")
            or Confidence.UNKNOWN.value
        )
        participants = tuple(
            str(row[0])
            for row in conn.execute(
                """
                SELECT participant_key
                FROM memory_ledger_knowledge_participants
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
) -> list[IntelligencePacketItem]:
    items: list[IntelligencePacketItem] = []
    lowered = str(request.user_text or "").lower()
    broad = _broad_profile_request(request.user_text)
    subject_key = subject_key_for_user(request.subject_user_id)
    signal = _canon_identity_signal(conn, request)
    diagnostics.canon_identity_status = signal.status
    diagnostics.canon_identity_stable_row_count = int(
        signal.stable_row_count or 0
    )
    recognized_fact_keys = set()
    if broad and signal.recognized and signal.subject is not None:
        for fact in CANON_FACTS:
            if fact.subject.key != signal.subject.key:
                continue
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
            subject_key=subject_key_for_user(request.subject_user_id),
            predicate_key="relationship_posture",
            text=str(posture["posture"])[:500],
            visibility="private",
            confidence=Confidence.MEDIUM.value,
            lifecycle="shadow",
            authority=_AUTHORITY_RANK[SourceClass.DERIVED_SUMMARY.value],
            participants=(subject_key_for_user(request.subject_user_id),),
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
    subject = subject_key_for_user(request.subject_user_id)
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


def _select_items(
    request: IntelligencePacketRequest,
    candidates: list[IntelligencePacketItem],
    diagnostics: IntelligencePacketDiagnostics,
    exclusions: list[IntelligencePacketExclusion],
) -> tuple[
    tuple[IntelligencePacketItem, ...],
    tuple[IntelligencePacketItem, ...],
    tuple[IntelligencePacketItem, ...],
]:
    broad = _broad_profile_request(request.user_text)
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
    canon_anchor = _profile_canon_anchor(request, candidates)
    subject_key = subject_key_for_user(request.subject_user_id)
    recognized_canon_signal = any(
        item.lane == "canon"
        and item.source_type == "recognized_canon_fact"
        and item.subject_key == subject_key
        for item in candidates
    )
    durable_member_candidate = any(
        item.lane in _PROFILE_DURABLE_MEMBER_EVIDENCE_LANES
        and item.subject_key == subject_key
        and item.point_identity
        and item.root_identities
        and item.occurrence_identities
        for item in candidates
    )
    profile_candidates: list[IntelligencePacketItem] = []
    broad_lane_priority = {
        "current_intent": 0,
        "approved_fact": 1,
        "atomic_knowledge": 3,
        "conversation_context": 4,
        "assessment_observation": 5,
        "moment": 6,
        "open_loop": 7,
        "canon": 8,
        "source_file": 9,
        "relationship_posture": 10,
    }
    ordered = sorted(
        candidates,
        key=lambda item: (
            (
                (
                    2
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
    selected_root_items: list[IntelligencePacketItem] = []
    lane_counts: Counter[str] = Counter()
    used = 0
    budget = min(max(int(request.budget_chars or 2400), 400), 6000)
    lane_caps = dict(
        _BROAD_PROFILE_LANE_CAPS if broad else _LANE_CAPS
    )
    if broad and recognized_canon_signal and not durable_member_candidate:
        # One stable canon-name signal may unlock one cautious historical
        # example. It must never turn several isolated examples into a rich
        # or durable personality profile.
        lane_caps["assessment_observation"] = 1
    for item in ordered:
        item_roots = set(item.root_identities)
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
        if _profile_member_item(request, item):
            profile_candidates.append(item)
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
    validation_canon = tuple(
        candidate
        for candidate in ordered
        if candidate.lane == "canon"
        and (
            (
                recognized_canon_signal
                and candidate.source_type == "recognized_canon_fact"
                and candidate.subject_key == subject_key
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
    if not _broad_profile_request(request.user_text):
        return ProfileSufficiency(
            status="not_applicable",
            satisfied=True,
            reason_codes=("not_broad_profile",),
        )
    durable_candidate_items = tuple(
        item
        for item in candidates
        if _profile_member_item(request, item)
        and item.lane in _PROFILE_DURABLE_MEMBER_EVIDENCE_LANES
    )
    durable_selected_items = tuple(
        item
        for item in selected
        if _profile_member_item(request, item)
        and item.lane in _PROFILE_DURABLE_MEMBER_EVIDENCE_LANES
    )
    recognized_canon_signal = any(
        item.lane == "canon"
        and item.source_type == "recognized_canon_fact"
        and item.subject_key
        == subject_key_for_user(request.subject_user_id)
        for item in selected
    )
    observational_candidate_items = tuple(
        item
        for item in candidates
        if _profile_member_item(request, item)
        and item.lane == "assessment_observation"
    )
    observational_selected_items = tuple(
        item
        for item in selected
        if _profile_member_item(request, item)
        and item.lane == "assessment_observation"
    )
    observation_only = bool(
        not durable_candidate_items
        and recognized_canon_signal
        and observational_candidate_items
    )
    if durable_candidate_items:
        candidate_items = durable_candidate_items
        selected_items = durable_selected_items
    elif observation_only:
        candidate_items = observational_candidate_items
        selected_items = observational_selected_items
    else:
        candidate_items = ()
        selected_items = ()
    candidate_points = {
        item.point_identity for item in candidate_items if item.point_identity
    }
    candidate_occurrences = {
        identity
        for item in candidate_items
        for identity in item.occurrence_identities
        if identity
    }
    selected_points = {
        item.point_identity for item in selected_items if item.point_identity
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
        1
        if observation_only
        else 2
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
                "recognized_canon_sparse_public_observation"
                if satisfied
                else "recognized_canon_observation_not_selected"
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
    subject = subject_key_for_user(packet.request.subject_user_id)
    broad = _broad_profile_request(packet.request.user_text)
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
) -> str:
    signal = _canon_identity_signal(conn, packet.request)
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
        subject_key_for_user(packet.request.subject_user_id),
    )


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


def revalidate_packet(
    conn: sqlite3.Connection,
    packet: UnifiedIntelligencePacket,
    *,
    environ: Mapping[str, str] | None = None,
) -> PacketRevalidationResult:
    """Re-read every durable source without applying packet content live."""
    changed = 0
    errors = 0
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
            elif item.revalidation_kind == "ledger":
                current = _ledger_entry_digest(conn, item.revalidation_key)
            elif item.revalidation_kind == "moment":
                current = _moment_version(conn, packet, item)
            elif item.revalidation_kind == "atomic":
                current = _atomic_candidate_digest(conn, item.revalidation_key)
            elif item.revalidation_kind == "canon":
                current = _canon_version(item)
            elif item.revalidation_kind == "recognized_canon":
                current = _recognized_canon_version(
                    conn,
                    packet,
                    item,
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
    )


def _packet_invariants(
    packet: UnifiedIntelligencePacket,
) -> tuple[str, ...]:
    invalid = []
    subject = subject_key_for_user(packet.request.subject_user_id)
    broad = _broad_profile_request(packet.request.user_text)
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
            and int(packet.request.subject_user_id or 0) > 0
            and item.subject_key != subject
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
            and int(packet.request.subject_user_id or 0) > 0
            and item.subject_key != subject
        ):
            invalid.append("validation_support_subject_violation")
        if (
            broad
            and item.lane in _PROFILE_MEMBER_EVIDENCE_LANES
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
    broad = _broad_profile_request(request.user_text)
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
    selected, profile_candidates, validation_items = _select_items(
        request,
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
        )
        for item in validation_items
    )
    diagnostics.packet_digest = _digest(
        SCHEMA_VERSION,
        prompt_digest_payload,
        validation_digest_payload,
        profile_sufficiency,
    )
    packet_id = "uip_" + _digest(
        SCHEMA_VERSION,
        request.guild_id,
        request.subject_user_id,
        request.route_mode,
        request.channel_policy,
        diagnostics.packet_digest,
    )[:40]
    packet = UnifiedIntelligencePacket(
        schema_version=SCHEMA_VERSION,
        packet_id=packet_id,
        request=request,
        items=selected,
        exclusions=tuple(exclusions),
        diagnostics=diagnostics,
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
            _digest(subject_key_for_user(packet.request.subject_user_id))[:16],
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
        SET validation_item_count=?,validation_lane_counts_json=?
        WHERE run_id=?
        """,
        (
            len(packet.validation_items),
            json.dumps(
                packet.diagnostics.validation_support_by_lane,
                sort_keys=True,
            ),
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
    item_total = validation_item_total = 0
    conflicts = visibility = budget = duplicates = 0
    root_collapses = shared_roots = profile_met = 0
    profile_points = profile_roots = profile_occurrences = 0
    errors = invalid = changed = prompt = live = 0
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
        "promptAppliedRuns": prompt,
        "liveAppliedRuns": live,
        "contentFieldsPresent": disallowed,
        "evidenceWindow": {
            "first": str(rows[-1][-1]) if rows else "none",
            "last": str(rows[0][-1]) if rows else "none",
        },
    }
