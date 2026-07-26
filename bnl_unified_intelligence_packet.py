"""Shadow-only governed retrieval and unified intelligence packet assembly.

This module owns no facts.  It coordinates references selected by the existing
Conversation Context, Memory Governance, Ledger, Moment, Relationship, canon,
and Source File owners into one bounded comparison packet.  The packet is never
rendered into a live prompt in this stage.
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
    CANON_FACTS,
    CANON_SOURCE_CONTRACT_VERSION,
    Confidence,
    SourceClass,
)
from bnl_memory_governance import (
    GovernanceRequest,
    assess_governance_result_safety,
    build_governed_context,
    classify_personal_recall_intent,
    ensure_governance_schema,
)
from bnl_memory_ledger import (
    ATOMIC_KNOWLEDGE_LIFECYCLE_SCHEMA_VERSION,
    subject_key_for_user,
)
from bnl_moment_engine import select_public_participant_moment_gists
from bnl_relationship_engine import shadow_packet_posture


SCHEMA_VERSION = "unified_intelligence_packet_v1"
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
    "approved_fact": 4,
    "moment": 3,
    "atomic_knowledge": 6,
    "open_loop": 3,
    "canon": 4,
    "source_file": 2,
    "relationship_posture": 1,
}
_ASSESSMENT_LANE_MAP = {
    "current_intent": "current_exchange",
    "conversation_context": "conversation_context",
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
    excluded_by_reason: dict[str, int] = field(default_factory=dict)
    missing_lanes: list[str] = field(default_factory=list)
    conflict_reasons: list[str] = field(default_factory=list)
    visibility_exclusions: int = 0
    budget_exclusions: int = 0
    duplicate_suppression: int = 0
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
class UnifiedIntelligencePacket:
    schema_version: str
    packet_id: str
    request: IntelligencePacketRequest
    items: tuple[IntelligencePacketItem, ...]
    exclusions: tuple[IntelligencePacketExclusion, ...]
    diagnostics: IntelligencePacketDiagnostics

    @property
    def detailed_lanes(self) -> tuple[str, ...]:
        return tuple(dict.fromkeys(item.lane for item in self.items))

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


def _terms(value: Any) -> set[str]:
    return {
        term
        for term in _TERM_RE.findall(str(value or "").lower())
        if len(term) > 1 and term not in _TERM_STOPWORDS
    }


def _broad_profile_request(value: str) -> bool:
    return classify_personal_recall_intent(value).broad_self_profile


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
            item = IntelligencePacketItem(
                lane=lane,
                source_class=SourceClass.PUBLIC_OBSERVATION.value,
                source_type="conversation_row",
                source_ref="conversation:%s" % int(row["id"]),
                source_digest=_conversation_digest(row),
                subject_key=subject_key_for_user(
                    int(row.get("user_id") or 0)
                ),
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
            source_digest = _digest(
                "moment",
                candidate.source_ref,
                candidate.entry_id,
                candidate.text,
                candidate.visibility,
                candidate.observed_at,
            )
            revalidation_kind = "moment"
            revalidation_key = candidate.source_ref.removeprefix("moment:")
        else:
            source_digest = _ledger_entry_digest(conn, candidate.entry_id)
            revalidation_kind = "ledger"
            revalidation_key = candidate.entry_id
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
               e.lifecycle_status,e.updated_at
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
        authority_class = str(
            candidate.get("consolidated_authority_class")
            or candidate.get("authority_class")
            or SourceClass.LEGACY_SOURCE_BLIND.value
        )
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
                lineage=tuple(
                    str(root.get("root_entry_id") or "") for root in roots
                ),
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


def _canon_items(
    request: IntelligencePacketRequest,
    diagnostics: IntelligencePacketDiagnostics,
    exclusions: list[IntelligencePacketExclusion],
    *,
    request_terms: set[str],
) -> list[IntelligencePacketItem]:
    items: list[IntelligencePacketItem] = []
    lowered = str(request.user_text or "").lower()
    for fact in CANON_FACTS:
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
) -> tuple[IntelligencePacketItem, ...]:
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
    ordered = sorted(
        candidates,
        key=lambda item: (
            -item.score,
            -item.authority,
            -_CONFIDENCE_RANK.get(item.confidence, 0),
            item.source_ref,
        ),
    )
    selected: list[IntelligencePacketItem] = []
    seen_text: set[str] = set()
    lane_counts: Counter[str] = Counter()
    used = 0
    budget = min(max(int(request.budget_chars or 2400), 400), 6000)
    for item in ordered:
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
        if lane_counts[item.lane] >= _LANE_CAPS.get(item.lane, 2):
            _add_exclusion(
                diagnostics,
                exclusions,
                lane=item.lane,
                reason="lane_cap",
                source_class=item.source_class,
            )
            continue
        item_cost = min(len(item.text), 500) + 36
        if item.lane != "current_intent" and used + item_cost > budget:
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
        _broad_profile_request(request.user_text)
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
    return tuple(selected)


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
    for item in packet.items:
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
    elif any(item.revalidation_kind == "snapshot" for item in packet.items):
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
    for item in packet.items:
        if not _route_allows_item(packet.request, item):
            invalid.append("selected_visibility_violation")
        if (
            item.lane
            in {
                "approved_fact",
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
    selected = _select_items(
        request,
        candidates,
        diagnostics,
        exclusions,
    )
    digest_payload = tuple(
        (
            item.lane,
            item.source_class,
            item.source_ref,
            item.source_digest,
            item.lifecycle,
            item.usage,
        )
        for item in selected
    )
    diagnostics.packet_digest = _digest(SCHEMA_VERSION, digest_payload)
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
        tuple(sorted(item.source_ref for item in packet.items))
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
          processing_error_count,invalid_invariant_count,
          revalidation_status,revalidation_changed_count,packet_digest,
          source_ref_digest,prompt_applied,live_applied,created_at
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
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
        "selectedByLane": {},
        "selectedBySourceClass": {},
        "selectedAtomicStates": {},
        "excludedByReason": {},
        "missingLaneCounts": {},
        "conflictRuns": 0,
        "visibilityExclusions": 0,
        "budgetExclusions": 0,
        "duplicateSuppressions": 0,
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
    rows = conn.execute(
        """
        SELECT schema_version,item_count,selected_lane_counts_json,
               source_class_counts_json,atomic_state_counts_json,
               excluded_by_reason_json,missing_lanes_json,conflict_count,
               visibility_exclusion_count,budget_exclusion_count,
               duplicate_suppression_count,processing_error_count,
               invalid_invariant_count,revalidation_status,
               revalidation_changed_count,prompt_applied,live_applied,
               created_at
        FROM memory_governance_intelligence_packet_runs
        WHERE guild_id=?
        ORDER BY created_at DESC,run_id DESC
        LIMIT ?
        """,
        (int(guild_id or 0), max(1, min(int(limit or 1000), 5000))),
    ).fetchall()
    selected_lanes: Counter[str] = Counter()
    source_classes: Counter[str] = Counter()
    atomic_states: Counter[str] = Counter()
    exclusions: Counter[str] = Counter()
    missing: Counter[str] = Counter()
    revalidation: Counter[str] = Counter()
    item_total = conflicts = visibility = budget = duplicates = 0
    errors = invalid = changed = prompt = live = 0
    for row in rows:
        (
            _schema,
            item_count,
            lane_json,
            source_json,
            atomic_json,
            exclusion_json,
            missing_json,
            conflict_count,
            visibility_count,
            budget_count,
            duplicate_count,
            error_count,
            invalid_count,
            revalidation_status,
            changed_count,
            prompt_applied,
            live_applied,
            _created_at,
        ) = row
        item_total += int(item_count or 0)
        for counter, raw in (
            (selected_lanes, lane_json),
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
        "selectedByLane": dict(sorted(selected_lanes.items())),
        "selectedBySourceClass": dict(sorted(source_classes.items())),
        "selectedAtomicStates": dict(sorted(atomic_states.items())),
        "excludedByReason": dict(sorted(exclusions.items())),
        "missingLaneCounts": dict(sorted(missing.items())),
        "conflictRuns": conflicts,
        "visibilityExclusions": visibility,
        "budgetExclusions": budget,
        "duplicateSuppressions": duplicates,
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
