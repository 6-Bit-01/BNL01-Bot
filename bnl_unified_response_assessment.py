"""Shadow-only unified response assessment for BNL's existing planner.

This module does not retrieve conversation history, memory, Moments,
relationships, canon, or Source Files.  The existing owners select those
inputs first; the conversation planner passes only their typed references and
aggregate metadata here.  The resulting assessment is never rendered into a
production prompt in this stage.
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
import uuid
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Mapping, Optional, Sequence, Tuple


ASSESSMENT_VERSION = "unified_response_assessment_v1"
SHADOW_ENV = "BNL_UNIFIED_RESPONSE_ASSESSMENT_SHADOW_ENABLED"
TABLE_NAME = "unified_response_assessment_shadow_runs"

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
_LOWER_PRECEDENCE_LANES = (
    "show_state",
    "website_read_model",
    "source_context",
    "broadcast_memory",
    "active_episode",
    "prior_moment",
    "governed_memory",
    "relationship",
    "canon",
)
_KNOWN_LANES = frozenset(
    (
        "current_exchange",
        "conversation_context",
        "show_state",
        "website_read_model",
        "source_context",
        "broadcast_memory",
        "active_episode",
        "prior_moment",
        "governed_memory",
        "legacy_memory",
        "relationship",
        "canon",
    )
)
_VISIBLE_CONTROL_MARKER_RE = re.compile(
    r"(?im)^\s*(?:"
    r"\[\s*(?:pause|wait|typing|thinking)\s*(?::[^\]\n]{0,40})?\s*\]"
    r"|<\s*(?:pause|wait|typing|thinking)(?:\s+[^>\n]{0,40})?\s*>"
    r")\s*"
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
    environ: Optional[Mapping[str, str]] = None,
) -> Dict[str, Any]:
    """Return the derived shadow state without changing environment values."""

    env = os.environ if environ is None else environ
    prerequisites = {
        name: _flag(env.get(name, ""))
        for name in _SHADOW_PREREQUISITES
    }
    live_gates = {
        name: _flag(env.get(name, ""))
        for name in _LIVE_GATES
    }
    explicitly_configured = SHADOW_ENV in env
    requested = (
        _flag(env.get(SHADOW_ENV, ""))
        if explicitly_configured
        else all(prerequisites.values())
    )
    missing = tuple(
        name for name, enabled in prerequisites.items() if not enabled
    )
    active_live = tuple(
        name for name, enabled in live_gates.items() if enabled
    )
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


def shadow_enabled(
    environ: Optional[Mapping[str, str]] = None,
) -> bool:
    return bool(shadow_configuration(environ)["effective"])


def _unique_positive_ints(values: Sequence[Any]) -> Tuple[int, ...]:
    result = set()
    for value in values or ():
        try:
            parsed = int(value or 0)
        except (TypeError, ValueError):
            continue
        if parsed > 0:
            result.add(parsed)
    return tuple(sorted(result))


def _unique_strings(values: Sequence[Any]) -> Tuple[str, ...]:
    return tuple(
        dict.fromkeys(
            str(value or "").strip()
            for value in (values or ())
            if str(value or "").strip()
        )
    )


def _known_lanes(values: Sequence[Any]) -> Tuple[str, ...]:
    return _unique_strings(
        value
        for value in (values or ())
        if str(value or "").strip() in _KNOWN_LANES
    )


def _basis_kind(value: Any) -> str:
    name = type(value).__name__
    return {
        "ConversationPromptSourceBasis": "conversation",
        "MemoryPromptSourceBasis": "memory",
        "BatchMomentPromptSourceBasis": "batch_moment",
    }.get(name, "unknown")


def source_basis_kinds(values: Sequence[Any]) -> Tuple[str, ...]:
    return _unique_strings(_basis_kind(value) for value in (values or ()))


def _response_act(
    *,
    immediate_recap: bool,
    continuity_required: bool,
    exact_quote_requested: bool,
    route_mode: str,
    show_state_present: bool,
) -> str:
    if immediate_recap:
        return "recap_current_exchange"
    if exact_quote_requested:
        return "verify_exact_wording"
    if continuity_required:
        return "continue_active_thread"
    if route_mode == "direct_payload_task":
        return "complete_request_payload"
    if show_state_present or route_mode == "show_status":
        return "answer_show_status"
    return "answer_current_turn"


@dataclass(frozen=True)
class UnifiedResponseAssessment:
    """One response-time view over references chosen by existing owners."""

    schema_version: str
    guild_id: int
    route_mode: str
    channel_policy: str
    conversation_surface: str
    current_speaker_user_ids: Tuple[int, ...]
    target_user_ids: Tuple[int, ...]
    participant_user_ids: Tuple[int, ...]
    speaker_labels: Tuple[str, ...]
    current_exchange_source_ids: Tuple[int, ...]
    active_episode_id: str
    prior_moment_ids: Tuple[str, ...]
    governed_entry_ids: Tuple[str, ...]
    relationship_candidate_keys: Tuple[str, ...]
    canon_refs: Tuple[str, ...]
    selected_lanes: Tuple[str, ...]
    excluded_lanes: Tuple[Tuple[str, str], ...]
    conflict_reasons: Tuple[str, ...]
    supported_inferences: Tuple[str, ...]
    response_act: str
    exact_quote_authority_present: bool
    source_basis_kinds: Tuple[str, ...]
    prompt_budget: int
    prompt_lanes: Tuple[str, ...]
    comparison_status: str
    prompt_extra_lanes: Tuple[str, ...]
    prompt_missing_lanes: Tuple[str, ...]
    diagnostic_reasons: Tuple[str, ...]
    moment_candidate_count: int
    governed_candidate_count: int
    governance_exclusion_count: int


def build_unified_response_assessment(
    *,
    guild_id: int,
    route_mode: str,
    channel_policy: str,
    conversation_surface: str,
    current_speaker_user_ids: Sequence[int],
    target_user_ids: Sequence[int] = (),
    participant_user_ids: Sequence[int] = (),
    speaker_labels: Sequence[str] = (),
    current_exchange_source_ids: Sequence[int] = (),
    active_episode_id: str = "",
    prior_moment_ids: Sequence[str] = (),
    governed_entry_ids: Sequence[str] = (),
    relationship_candidate_keys: Sequence[str] = (),
    canon_refs: Sequence[str] = (),
    prompt_lanes: Sequence[str] = (),
    continuity_required: bool = False,
    immediate_recap: bool = False,
    exact_quote_requested: bool = False,
    exact_quote_authority_present: bool = False,
    source_bases: Sequence[Any] = (),
    prompt_budget: int = 0,
    moment_candidate_count: int = 0,
    governed_candidate_count: int = 0,
    governance_exclusion_count: int = 0,
    governance_contradiction_count: int = 0,
    legacy_memory_present: bool = False,
    legacy_relationship_present: bool = False,
    relationship_v2_candidate_present: bool = False,
    canon_relevant: bool = False,
    show_state_present: bool = False,
    website_read_model_present: bool = False,
    source_context_present: bool = False,
    broadcast_memory_present: bool = False,
) -> UnifiedResponseAssessment:
    """Assemble one deterministic assessment without rendering it live."""

    current_speakers = _unique_positive_ints(current_speaker_user_ids)
    targets = _unique_positive_ints(target_user_ids)
    participants = _unique_positive_ints(
        tuple(participant_user_ids) + current_speakers + targets
    )
    labels = _unique_strings(speaker_labels)
    exchange_ids = _unique_positive_ints(current_exchange_source_ids)
    moment_ids = _unique_strings(prior_moment_ids)
    governed_ids = _unique_strings(governed_entry_ids)
    relationship_keys = _unique_strings(relationship_candidate_keys)
    canonical_refs = _unique_strings(canon_refs)
    actual_prompt_lanes = _known_lanes(prompt_lanes)

    selected = ["current_exchange"]
    excluded = []
    conflicts = []
    inferences = []
    diagnostics = []

    if immediate_recap:
        if "conversation_context" in actual_prompt_lanes or exchange_ids:
            selected.append("conversation_context")
        inferences.append("shared_current_exchange")
        diagnostics.append("current_exchange_primary")
    elif continuity_required:
        if "conversation_context" in actual_prompt_lanes or exchange_ids:
            selected.append("conversation_context")
        inferences.append("active_thread_continuity")
    elif "conversation_context" in actual_prompt_lanes or exchange_ids:
        selected.append("conversation_context")

    authoritative_current_lanes = (
        ("show_state", show_state_present),
        ("website_read_model", website_read_model_present),
        ("source_context", source_context_present),
        ("broadcast_memory", broadcast_memory_present),
    )
    for lane, present in authoritative_current_lanes:
        if not present:
            continue
        if immediate_recap:
            excluded.append((lane, "current_exchange_precedence"))
        else:
            selected.append(lane)

    episode_id = str(active_episode_id or "").strip()
    if episode_id:
        if immediate_recap:
            excluded.append(("active_episode", "current_exchange_precedence"))
        else:
            selected.append("active_episode")
    else:
        diagnostics.append("active_episode_unavailable_in_moment_v1")

    if moment_ids:
        if immediate_recap:
            excluded.append(("prior_moment", "current_exchange_precedence"))
        else:
            selected.append("prior_moment")
    elif int(moment_candidate_count or 0) > 0:
        excluded.append(("prior_moment", "not_selected_by_current_governance"))
        diagnostics.append("moment_candidates_observed_without_selected_ids")

    governed_count = max(int(governed_candidate_count or 0), len(governed_ids))
    if governed_count:
        if immediate_recap:
            excluded.append(("governed_memory", "current_exchange_precedence"))
        else:
            selected.append("governed_memory")
    elif legacy_memory_present:
        excluded.append(("legacy_memory", "no_governed_candidate"))

    if relationship_v2_candidate_present or relationship_keys:
        if immediate_recap:
            excluded.append(("relationship", "current_exchange_precedence"))
        else:
            selected.append("relationship")
    elif legacy_relationship_present:
        excluded.append(("relationship", "legacy_only_live_authority_off"))

    if canon_relevant and canonical_refs:
        if immediate_recap:
            excluded.append(("canon", "current_exchange_precedence"))
        else:
            selected.append("canon")

    if immediate_recap and any(
        lane in actual_prompt_lanes for lane in _LOWER_PRECEDENCE_LANES
    ):
        conflicts.append("current_exchange_precedence")
    if int(governance_contradiction_count or 0) > 0:
        conflicts.append("governance_contradiction_resolved")

    selected_lanes = _unique_strings(selected)
    excluded_lanes = tuple(
        dict.fromkeys(
            (str(lane or "").strip(), str(reason or "").strip())
            for lane, reason in excluded
            if str(lane or "").strip() and str(reason or "").strip()
        )
    )
    selected_set = set(selected_lanes)
    prompt_set = set(actual_prompt_lanes)
    prompt_extra = tuple(sorted(prompt_set - selected_set))
    prompt_missing = tuple(sorted(selected_set - prompt_set))
    if not prompt_extra and not prompt_missing:
        comparison = "match"
    elif prompt_extra and not prompt_missing:
        comparison = "prompt_overincluded"
    elif prompt_missing and not prompt_extra:
        comparison = "prompt_underincluded"
    else:
        comparison = "different"

    act = _response_act(
        immediate_recap=bool(immediate_recap),
        continuity_required=bool(continuity_required),
        exact_quote_requested=bool(exact_quote_requested),
        route_mode=str(route_mode or "unknown"),
        show_state_present=bool(show_state_present),
    )
    diagnostics.append("response_act:%s" % act)
    diagnostics.append("prompt_comparison:%s" % comparison)

    return UnifiedResponseAssessment(
        schema_version=ASSESSMENT_VERSION,
        guild_id=int(guild_id or 0),
        route_mode=str(route_mode or "unknown")[:80],
        channel_policy=str(channel_policy or "unknown")[:80],
        conversation_surface=str(conversation_surface or "unknown")[:80],
        current_speaker_user_ids=current_speakers,
        target_user_ids=targets,
        participant_user_ids=participants,
        speaker_labels=labels,
        current_exchange_source_ids=exchange_ids,
        active_episode_id=episode_id,
        prior_moment_ids=moment_ids,
        governed_entry_ids=governed_ids,
        relationship_candidate_keys=relationship_keys,
        canon_refs=canonical_refs,
        selected_lanes=selected_lanes,
        excluded_lanes=excluded_lanes,
        conflict_reasons=_unique_strings(conflicts),
        supported_inferences=_unique_strings(inferences),
        response_act=act,
        exact_quote_authority_present=bool(exact_quote_authority_present),
        source_basis_kinds=source_basis_kinds(source_bases),
        prompt_budget=max(0, int(prompt_budget or 0)),
        prompt_lanes=actual_prompt_lanes,
        comparison_status=comparison,
        prompt_extra_lanes=prompt_extra,
        prompt_missing_lanes=prompt_missing,
        diagnostic_reasons=_unique_strings(diagnostics),
        moment_candidate_count=max(0, int(moment_candidate_count or 0)),
        governed_candidate_count=governed_count,
        governance_exclusion_count=max(
            0,
            int(governance_exclusion_count or 0),
        ),
    )


def ensure_schema(conn: sqlite3.Connection) -> None:
    """Create the additive, content-free shadow receipt table."""

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS unified_response_assessment_shadow_runs (
            run_id TEXT PRIMARY KEY,
            schema_version TEXT NOT NULL,
            guild_id INTEGER NOT NULL,
            route_mode TEXT NOT NULL,
            channel_policy TEXT NOT NULL,
            conversation_surface TEXT NOT NULL,
            current_speaker_count INTEGER NOT NULL DEFAULT 0,
            target_count INTEGER NOT NULL DEFAULT 0,
            participant_count INTEGER NOT NULL DEFAULT 0,
            current_exchange_source_count INTEGER NOT NULL DEFAULT 0,
            active_episode_present INTEGER NOT NULL DEFAULT 0,
            prior_moment_count INTEGER NOT NULL DEFAULT 0,
            moment_candidate_count INTEGER NOT NULL DEFAULT 0,
            governed_entry_count INTEGER NOT NULL DEFAULT 0,
            governed_candidate_count INTEGER NOT NULL DEFAULT 0,
            governance_exclusion_count INTEGER NOT NULL DEFAULT 0,
            relationship_candidate_count INTEGER NOT NULL DEFAULT 0,
            canon_ref_count INTEGER NOT NULL DEFAULT 0,
            response_act TEXT NOT NULL,
            selected_lanes_json TEXT NOT NULL DEFAULT '[]',
            excluded_lanes_json TEXT NOT NULL DEFAULT '{}',
            conflict_reasons_json TEXT NOT NULL DEFAULT '[]',
            supported_inference_count INTEGER NOT NULL DEFAULT 0,
            exact_quote_authority_present INTEGER NOT NULL DEFAULT 0,
            source_basis_kinds_json TEXT NOT NULL DEFAULT '[]',
            source_basis_count INTEGER NOT NULL DEFAULT 0,
            prompt_budget INTEGER NOT NULL DEFAULT 0,
            prompt_lanes_json TEXT NOT NULL DEFAULT '[]',
            comparison_status TEXT NOT NULL,
            prompt_extra_lanes_json TEXT NOT NULL DEFAULT '[]',
            prompt_missing_lanes_json TEXT NOT NULL DEFAULT '[]',
            source_basis_changed_before_send INTEGER NOT NULL DEFAULT 0,
            guard_triggered INTEGER NOT NULL DEFAULT 0,
            guard_repaired INTEGER NOT NULL DEFAULT 0,
            response_sent INTEGER NOT NULL DEFAULT 0,
            response_length INTEGER NOT NULL DEFAULT 0,
            speaker_label_coverage_count INTEGER NOT NULL DEFAULT 0,
            visible_control_marker INTEGER NOT NULL DEFAULT 0,
            response_alignment TEXT NOT NULL,
            processing_errors_json TEXT NOT NULL DEFAULT '[]',
            behavior_changed INTEGER NOT NULL DEFAULT 0,
            new_authority_applied INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_unified_assessment_shadow_guild
        ON unified_response_assessment_shadow_runs(guild_id, created_at)
        """
    )


def _excluded_counts(
    excluded_lanes: Sequence[Tuple[str, str]],
) -> Dict[str, int]:
    counts = Counter()
    for lane, reason in excluded_lanes:
        counts["%s:%s" % (lane, reason)] += 1
    return dict(sorted(counts.items()))


def _guard_signal(guard_diagnostics: Mapping[str, Any]) -> Tuple[bool, bool]:
    source = guard_diagnostics or {}
    trigger_keys = (
        "scripted_mode_leak_guard_triggered",
        "register_mismatch_guard_triggered",
        "generic_non_answer_triggered",
        "source_grounding_guard_triggered",
        "contextual_followthrough_guard_triggered",
        "community_visual_guard_triggered",
        "exact_quote_guard_triggered",
        "prompt_source_basis_changed",
    )
    repair_keys = (
        "regenerated_for_mode_leak",
        "regenerated_for_register_mismatch",
        "generic_non_answer_regenerated",
        "source_grounding_regenerated",
        "contextual_followthrough_regenerated",
        "community_visual_regenerated",
        "exact_quote_regenerated",
        "prompt_source_basis_regenerated",
    )
    return (
        any(bool(source.get(key)) for key in trigger_keys),
        any(bool(source.get(key)) for key in repair_keys),
    )


def persist_shadow_run(
    conn: sqlite3.Connection,
    assessment: UnifiedResponseAssessment,
    *,
    response: str,
    guard_diagnostics: Optional[Mapping[str, Any]] = None,
    response_sent: bool = True,
    processing_errors: Sequence[str] = (),
    created_at: str = "",
) -> str:
    """Persist one content-free comparison receipt."""

    ensure_schema(conn)
    guard = guard_diagnostics or {}
    guard_triggered, guard_repaired = _guard_signal(guard)
    response_text = str(response or "")
    lowered_response = response_text.lower()
    labels = tuple(
        label
        for label in assessment.speaker_labels
        if len(label.strip()) >= 2
    )
    speaker_coverage = sum(
        1 for label in labels if label.lower() in lowered_response
    )
    visible_control_marker = bool(
        _VISIBLE_CONTROL_MARKER_RE.search(response_text)
    )
    source_changed = bool(guard.get("prompt_source_basis_changed"))
    if not response_sent:
        alignment = "not_sent"
    elif source_changed and not guard_repaired:
        alignment = "source_changed_unrepaired"
    elif bool(guard.get("suppressed")):
        alignment = "suppressed"
    elif visible_control_marker:
        alignment = "visible_control_marker"
    elif assessment.response_act == "recap_current_exchange" and labels:
        alignment = (
            "recap_full_speaker_label_coverage"
            if speaker_coverage == len(labels)
            else "recap_partial_speaker_label_coverage"
        )
    elif guard_repaired:
        alignment = "guard_repaired"
    else:
        alignment = "guard_clear"

    run_id = "ura_" + uuid.uuid4().hex
    timestamp = created_at or datetime.now(timezone.utc).isoformat()
    conn.execute(
        """
        INSERT INTO unified_response_assessment_shadow_runs (
            run_id, schema_version, guild_id, route_mode, channel_policy,
            conversation_surface, current_speaker_count, target_count,
            participant_count, current_exchange_source_count,
            active_episode_present, prior_moment_count, moment_candidate_count,
            governed_entry_count, governed_candidate_count,
            governance_exclusion_count, relationship_candidate_count,
            canon_ref_count, response_act, selected_lanes_json,
            excluded_lanes_json, conflict_reasons_json,
            supported_inference_count, exact_quote_authority_present,
            source_basis_kinds_json, source_basis_count, prompt_budget,
            prompt_lanes_json, comparison_status, prompt_extra_lanes_json,
            prompt_missing_lanes_json, source_basis_changed_before_send,
            guard_triggered, guard_repaired, response_sent, response_length,
            speaker_label_coverage_count, visible_control_marker,
            response_alignment, processing_errors_json, behavior_changed,
            new_authority_applied, created_at
        ) VALUES (
            ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,
            ?,?,?,?,?,?,?,?,?
        )
        """,
        (
            run_id,
            assessment.schema_version,
            assessment.guild_id,
            assessment.route_mode,
            assessment.channel_policy,
            assessment.conversation_surface,
            len(assessment.current_speaker_user_ids),
            len(assessment.target_user_ids),
            len(assessment.participant_user_ids),
            len(assessment.current_exchange_source_ids),
            int(bool(assessment.active_episode_id)),
            len(assessment.prior_moment_ids),
            assessment.moment_candidate_count,
            len(assessment.governed_entry_ids),
            assessment.governed_candidate_count,
            assessment.governance_exclusion_count,
            len(assessment.relationship_candidate_keys),
            len(assessment.canon_refs),
            assessment.response_act,
            json.dumps(assessment.selected_lanes),
            json.dumps(_excluded_counts(assessment.excluded_lanes), sort_keys=True),
            json.dumps(assessment.conflict_reasons),
            len(assessment.supported_inferences),
            int(assessment.exact_quote_authority_present),
            json.dumps(assessment.source_basis_kinds),
            len(assessment.source_basis_kinds),
            assessment.prompt_budget,
            json.dumps(assessment.prompt_lanes),
            assessment.comparison_status,
            json.dumps(assessment.prompt_extra_lanes),
            json.dumps(assessment.prompt_missing_lanes),
            int(source_changed),
            int(guard_triggered),
            int(guard_repaired),
            int(bool(response_sent)),
            len(response_text),
            speaker_coverage,
            int(visible_control_marker),
            alignment,
            json.dumps(
                tuple(
                    "processing_error"
                    for _error in (processing_errors or ())
                )
            ),
            0,
            0,
            timestamp,
        ),
    )
    return run_id


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    return bool(
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        ).fetchone()
    )


def _safe_json(value: Any, fallback: Any) -> Any:
    try:
        return json.loads(str(value or ""))
    except (TypeError, ValueError):
        return fallback


def _empty_evaluation_report() -> Dict[str, Any]:
    return {
        "tablePresent": False,
        "runs": 0,
        "response_sent_runs": 0,
        "current_exchange_primary_runs": 0,
        "comparison_status_counts": {},
        "selected_lane_counts": {},
        "excluded_lane_reason_counts": {},
        "response_act_counts": {},
        "response_alignment_counts": {},
        "prompt_overincluded_runs": 0,
        "prompt_underincluded_runs": 0,
        "prompt_different_runs": 0,
        "source_basis_changed_runs": 0,
        "guard_triggered_runs": 0,
        "guard_repaired_runs": 0,
        "visible_control_marker_runs": 0,
        "processing_errors": 0,
        "behavior_changed_runs": 0,
        "new_authority_applied_runs": 0,
        "content_fields_present": [],
        "evidenceWindow": {"first": "none", "last": "none"},
    }


def build_evaluation_report(
    conn: sqlite3.Connection,
    *,
    guild_id: int,
    prepare_schema: bool = False,
    limit: int = 500,
) -> Dict[str, Any]:
    """Aggregate retained receipts without exposing people, text, or source IDs."""

    if prepare_schema:
        ensure_schema(conn)
    if not _table_exists(conn, TABLE_NAME):
        return _empty_evaluation_report()
    columns = {
        str(row[1])
        for row in conn.execute("PRAGMA table_info(%s)" % TABLE_NAME)
    }
    disallowed_content_columns = sorted(
        columns
        & {
            "raw_text",
            "request_text",
            "response_text",
            "speaker_labels",
            "participant_ids",
            "source_ids",
            "source_text",
        }
    )
    rows = conn.execute(
        """
        SELECT response_sent, selected_lanes_json, excluded_lanes_json,
               response_act, comparison_status,
               source_basis_changed_before_send, guard_triggered,
               guard_repaired, visible_control_marker, response_alignment,
               processing_errors_json, behavior_changed,
               new_authority_applied, created_at
        FROM unified_response_assessment_shadow_runs
        WHERE guild_id=?
        ORDER BY created_at DESC, run_id DESC
        LIMIT ?
        """,
        (int(guild_id or 0), max(1, min(int(limit or 500), 5000))),
    ).fetchall()

    selected_counts = Counter()
    excluded_counts = Counter()
    act_counts = Counter()
    comparison_counts = Counter()
    alignment_counts = Counter()
    response_sent_runs = current_primary = source_changed = 0
    guard_triggered_runs = guard_repaired_runs = marker_runs = 0
    processing_errors = behavior_changed = new_authority = 0
    for row in rows:
        (
            response_sent,
            selected_json,
            excluded_json,
            response_act,
            comparison,
            source_basis_changed,
            guard_triggered,
            guard_repaired,
            visible_control_marker,
            response_alignment,
            errors_json,
            changed_behavior,
            applied_authority,
            _created_at,
        ) = row
        selected = _safe_json(selected_json, [])
        if not isinstance(selected, list):
            selected = []
            processing_errors += 1
        excluded = _safe_json(excluded_json, {})
        if not isinstance(excluded, dict):
            excluded = {}
            processing_errors += 1
        errors = _safe_json(errors_json, ["invalid_json"])
        if not isinstance(errors, (list, tuple, dict)):
            errors = ["invalid_shape"] if errors else []
        selected_counts.update(str(item) for item in selected)
        excluded_counts.update(
            {
                str(key): max(0, int(value or 0))
                for key, value in excluded.items()
            }
        )
        act_counts[str(response_act or "unknown")] += 1
        comparison_counts[str(comparison or "unknown")] += 1
        alignment_counts[str(response_alignment or "unknown")] += 1
        response_sent_runs += int(bool(response_sent))
        current_primary += int(bool(selected and selected[0] == "current_exchange"))
        source_changed += int(bool(source_basis_changed))
        guard_triggered_runs += int(bool(guard_triggered))
        guard_repaired_runs += int(bool(guard_repaired))
        marker_runs += int(bool(visible_control_marker))
        processing_errors += len(errors)
        behavior_changed += int(bool(changed_behavior))
        new_authority += int(bool(applied_authority))

    return {
        "tablePresent": True,
        "runs": len(rows),
        "response_sent_runs": response_sent_runs,
        "current_exchange_primary_runs": current_primary,
        "comparison_status_counts": dict(sorted(comparison_counts.items())),
        "selected_lane_counts": dict(sorted(selected_counts.items())),
        "excluded_lane_reason_counts": dict(sorted(excluded_counts.items())),
        "response_act_counts": dict(sorted(act_counts.items())),
        "response_alignment_counts": dict(sorted(alignment_counts.items())),
        "prompt_overincluded_runs": comparison_counts.get(
            "prompt_overincluded",
            0,
        ),
        "prompt_underincluded_runs": comparison_counts.get(
            "prompt_underincluded",
            0,
        ),
        "prompt_different_runs": comparison_counts.get("different", 0),
        "source_basis_changed_runs": source_changed,
        "guard_triggered_runs": guard_triggered_runs,
        "guard_repaired_runs": guard_repaired_runs,
        "visible_control_marker_runs": marker_runs,
        "processing_errors": processing_errors,
        "behavior_changed_runs": behavior_changed,
        "new_authority_applied_runs": new_authority,
        "content_fields_present": disallowed_content_columns,
        "evidenceWindow": {
            "first": str(rows[-1][13]) if rows else "none",
            "last": str(rows[0][13]) if rows else "none",
        },
    }
