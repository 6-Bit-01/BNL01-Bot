"""Privacy-safe, read-only acceptance evidence for BNL's v2 shadow stack.

This module reports the state of systems that already exist. It never enables an
environment gate, chooses a live cutover, mutates evaluation rows, or emits a
Discord message.
"""

from __future__ import annotations

import json
import os
import sqlite3
from collections import Counter
from typing import Any, Dict, List, Mapping, Optional, Sequence

from bnl_memory_ledger import build_memory_ledger_evaluation
from bnl_moment_engine import build_moment_evaluation_report
from bnl_relationship_engine import build_evaluation_report as build_relationship_evaluation_report
from bnl_unified_response_assessment import (
    build_evaluation_report as build_unified_assessment_evaluation_report,
    shadow_configuration as unified_assessment_shadow_configuration,
)


SHADOW_ACCEPTANCE_VERSION = "v2_shadow_acceptance_v2"
SHADOW_EVALUATION_ORDER = (
    "memory_ledger",
    "moment_engine",
    "memory_governance",
    "relationship_v2",
)

# Older Governance writers used this selected-invariant label when a candidate
# was safely rejected before selection. Keep the retained evidence visible, but
# do not let that known historical classification create a false hard blocker.
LEGACY_RECLASSIFIED_SAFE_EXCLUSIONS = {
    "invalid_route_channel_policy_selected": "invalid_route_channel_policy",
}


def _flag_enabled(environ: Mapping[str, str], name: str, default: str = "") -> bool:
    value = str(environ.get(name, default) or default)
    return value.strip().lower() in {"1", "true", "yes", "on", "enabled"}


def build_gate_snapshot(environ: Optional[Mapping[str, str]] = None) -> Dict[str, Any]:
    """Return requested and effective gate state without changing the environment."""

    env = environ if environ is not None else os.environ
    ledger = _flag_enabled(env, "BNL_MEMORY_LEDGER_SHADOW_ENABLED")
    moment = _flag_enabled(env, "BNL_MOMENT_ENGINE_SHADOW_ENABLED")
    governance_shadow = _flag_enabled(env, "BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED")
    governance_live_requested = _flag_enabled(env, "BNL_MEMORY_GOVERNANCE_LIVE_ENABLED")
    relationship_shadow = _flag_enabled(env, "BNL_RELATIONSHIP_V2_SHADOW_ENABLED")
    relationship_live = _flag_enabled(env, "BNL_RELATIONSHIP_V2_LIVE_ENABLED")
    engagement_live = _flag_enabled(env, "BNL_ACTIVE_ENGAGEMENT_V2_LIVE_ENABLED")
    context_value = str(env.get("BNL_CONVERSATION_CONTEXT_V2_ENABLED", "true") or "true")
    context_enabled = context_value.strip().lower() not in {"false", "0", "off"}
    unified_assessment = unified_assessment_shadow_configuration(env)
    return {
        "conversation_context_v2": context_enabled,
        "memory_ledger_shadow_requested": ledger,
        "moment_engine_shadow_requested": moment,
        "moment_engine_shadow_effective": ledger and moment,
        "memory_governance_shadow_requested": governance_shadow,
        "memory_governance_live_requested": governance_live_requested,
        "memory_governance_live_effective": governance_shadow and governance_live_requested,
        "relationship_v2_shadow_requested": relationship_shadow,
        "relationship_v2_live_requested": relationship_live,
        "active_engagement_v2_live_requested": engagement_live,
        "unified_response_assessment_shadow_requested": bool(
            unified_assessment["requested"]
        ),
        "unified_response_assessment_shadow_effective": bool(
            unified_assessment["effective"]
        ),
        "unified_response_assessment_shadow_reason": str(
            unified_assessment["reason"]
        ),
        "all_live_gates_clear": not (
            governance_live_requested or relationship_live or engagement_live
        ),
    }


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    return bool(
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        ).fetchone()
    )


def _table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    if not _table_exists(conn, table_name):
        return set()
    return {str(row[1]) for row in conn.execute("PRAGMA table_info(%s)" % table_name)}


def _evidence_window(
    conn: sqlite3.Connection,
    *,
    table_name: str,
    timestamp_column: str,
    guild_id: int,
) -> Dict[str, str]:
    if not _table_exists(conn, table_name):
        return {"first": "none", "last": "none"}
    row = conn.execute(
        "SELECT MIN(%s), MAX(%s) FROM %s WHERE guild_id=?"
        % (timestamp_column, timestamp_column, table_name),
        (guild_id,),
    ).fetchone()
    return {
        "first": str((row or (None, None))[0] or "none"),
        "last": str((row or (None, None))[1] or "none"),
    }


def _report_error(report: Dict[str, Any], exc: Exception) -> Dict[str, Any]:
    report["reportError"] = type(exc).__name__
    return report


def _empty_ledger_report() -> Dict[str, Any]:
    return {
        "tablePresent": False,
        "schemaVersion": "memory_ledger_v1",
        "actualLedgerRows": 0,
        "eligibleLegacyWrites": 0,
        "insertedLedgerEntries": 0,
        "exactSourceDeduplications": 0,
        "shadowWriteErrors": 0,
        "skippedWrites": {},
        "missingUnmappedProvenance": 0,
        "entriesWithMultipleActiveValues": 0,
        "danglingLineageTargets": 0,
        "legacyToLedgerParityMismatches": 0,
    }


def _read_ledger_report(conn: sqlite3.Connection, guild_id: int) -> Dict[str, Any]:
    required = {
        "memory_ledger_entries",
        "memory_ledger_lineage",
        "memory_ledger_shadow_receipts",
    }
    if not all(_table_exists(conn, table) for table in required):
        return _empty_ledger_report()
    try:
        report = build_memory_ledger_evaluation(
            conn,
            guild_id=guild_id,
            prepare_schema=False,
        )
        report["tablePresent"] = True
        report["actualLedgerRows"] = int(
            conn.execute(
                "SELECT COUNT(*) FROM memory_ledger_entries WHERE guild_id=?",
                (guild_id,),
            ).fetchone()[0]
            or 0
        )
        report["evidenceWindow"] = _evidence_window(
            conn,
            table_name="memory_ledger_shadow_receipts",
            timestamp_column="attempted_at",
            guild_id=guild_id,
        )
        return report
    except (sqlite3.DatabaseError, ValueError, TypeError) as exc:
        return _report_error({**_empty_ledger_report(), "tablePresent": True}, exc)


def _empty_moment_report() -> Dict[str, Any]:
    return {
        "tablePresent": False,
        "eligible_entries_observed": 0,
        "open_windows": 0,
        "finalized_moments": 0,
        "processing_errors": 0,
        "rejected_windows_by_reason": {},
        "duplicate_memberships": 0,
        "bnl_only_violations": 0,
        "cross_guild_violations": 0,
        "cross_channel_violations": 0,
        "incompatible_visibility_violations": 0,
        "dangling_lineage_targets": 0,
        "affected_moments_awaiting_correction_review": 0,
    }


def _read_moment_report(conn: sqlite3.Connection, guild_id: int) -> Dict[str, Any]:
    required = {
        "memory_moment_windows",
        "memory_moment_diagnostics",
        "memory_moment_members",
        "memory_ledger_entries",
        "memory_ledger_lineage",
    }
    if not all(_table_exists(conn, table) for table in required):
        return _empty_moment_report()
    try:
        report = build_moment_evaluation_report(
            conn,
            guild_id=guild_id,
            prepare_schema=False,
        )
        report["tablePresent"] = True
        report["evidenceWindow"] = _evidence_window(
            conn,
            table_name="memory_moment_diagnostics",
            timestamp_column="created_at",
            guild_id=guild_id,
        )
        return report
    except (sqlite3.DatabaseError, ValueError, TypeError) as exc:
        return _report_error({**_empty_moment_report(), "tablePresent": True}, exc)


def _json_value(value: Any, fallback: Any) -> Any:
    if value is None or value == "":
        return fallback
    try:
        return json.loads(str(value))
    except (TypeError, ValueError):
        return fallback


def _count_mapping(value: Any) -> Dict[str, int]:
    parsed = _json_value(value, {})
    if not isinstance(parsed, dict):
        return {"invalid_json": 1}
    result: Dict[str, int] = {}
    for key, count in parsed.items():
        result[str(key)] = _aggregate_count(count)
    return result


def _error_count(value: Any) -> int:
    parsed = _json_value(value, ["invalid_json"])
    if isinstance(parsed, (list, tuple, dict)):
        return len(parsed)
    return 1 if parsed else 0


def _invariant_counts(value: Any) -> Dict[str, int]:
    if isinstance(value, dict):
        return {str(k): _aggregate_count(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return dict(Counter(str(item) for item in value))
    return {}


def _partition_legacy_preselection_labels(
    invariants: Mapping[str, int],
    exclusions: Mapping[str, int],
) -> tuple[Dict[str, int], Dict[str, int]]:
    """Reclassify corroborated legacy labels exactly once at report time."""

    active = dict(invariants)
    reclassified: Dict[str, int] = {}
    for marker, exclusion_reason in LEGACY_RECLASSIFIED_SAFE_EXCLUSIONS.items():
        matched = min(
            int(active.get(marker, 0) or 0),
            int(exclusions.get(exclusion_reason, 0) or 0),
        )
        if not matched:
            continue
        reclassified[marker] = matched
        remaining = int(active[marker]) - matched
        if remaining:
            active[marker] = remaining
        else:
            active.pop(marker, None)
    return active, reclassified


def _aggregate_count(value: Any) -> int:
    """Parse a persisted aggregate count or fail the diagnostic closed."""

    count = int(value or 0)
    if count < 0:
        raise ValueError("negative aggregate count")
    return count


def _empty_governance_report() -> Dict[str, Any]:
    return {
        "tablePresent": False,
        "aggregateDiagnosticsPresent": False,
        "aggregateDiagnosticRuns": 0,
        "runs": 0,
        "selected_total": 0,
        "zero_selection_runs": 0,
        "excluded_total": 0,
        "excluded_by_reason": {},
        "processing_errors": 0,
        "selected_by_source": {},
        "invalid_invariants": {},
        "invalid_invariant_count": 0,
        "invalid_invariant_runs": 0,
        "legacy_reclassified_exclusions": {},
        "legacy_reclassified_exclusion_count": 0,
        "legacy_reclassified_exclusion_runs": 0,
        "fallback_reasons": {},
        "visibility_exclusions": 0,
        "token_budget_exclusions": 0,
        "duplicate_suppression": 0,
        "contradiction_resolution_count": 0,
        "moment_candidate_count": 0,
        "moment_needs_review_excluded": 0,
        "last_run_at": "none",
    }


def build_persisted_governance_report(
    conn: sqlite3.Connection,
    *,
    guild_id: int,
) -> Dict[str, Any]:
    """Aggregate retained Governance diagnostics without subject or memory content."""

    table = "memory_governance_shadow_runs"
    if not _table_exists(conn, table):
        return _empty_governance_report()
    has_diagnostics = "diagnostics_json" in _table_columns(conn, table)
    diagnostic_select = ", diagnostics_json" if has_diagnostics else ""
    try:
        rows = conn.execute(
            """
            SELECT route_mode, channel_policy, created_at, rendered_hash,
                   rendered_size, legacy_hash, legacy_size, selected_count,
                   excluded_json, errors_json%s
            FROM memory_governance_shadow_runs
            WHERE guild_id=?
            ORDER BY created_at DESC, run_id DESC
            LIMIT 500
            """ % diagnostic_select,
            (guild_id,),
        ).fetchall()
    except sqlite3.DatabaseError as exc:
        return _report_error({**_empty_governance_report(), "tablePresent": True}, exc)

    exclusions: Counter[str] = Counter()
    selected_sources: Counter[str] = Counter()
    invalid_invariants: Counter[str] = Counter()
    legacy_reclassified_exclusions: Counter[str] = Counter()
    fallback_reasons: Counter[str] = Counter()
    selected_total = processing_errors = same_hash = 0
    invalid_runs = legacy_reclassified_runs = aggregate_runs = visibility = budget = duplicates = contradictions = 0
    moment_candidates = moment_review = 0

    zero_selection_runs = 0
    try:
        for row in rows:
            (
                _route,
                _policy,
                _created_at,
                rendered_hash,
                _rendered_size,
                legacy_hash,
                _legacy_size,
                selected,
                excluded,
                errors,
                *diagnostics,
            ) = row
            selected_count = _aggregate_count(selected)
            selected_total += selected_count
            zero_selection_runs += int(selected_count == 0)
            row_exclusions = _count_mapping(excluded)
            exclusions.update(row_exclusions)
            processing_errors += _error_count(errors)
            same_hash += int(bool(rendered_hash and legacy_hash and rendered_hash == legacy_hash))
            diag = _json_value(diagnostics[0], {}) if diagnostics else {}
            if not isinstance(diag, dict):
                diag = {}
            aggregate_runs += int(bool(diag))
            selected_sources.update(_invariant_counts(diag.get("selected_by_source")))
            row_invariants = _invariant_counts(
                diag.get("invalid_invariant_counts", diag.get("invalid_invariants"))
            )
            row_actual_invariants, row_reclassified = (
                _partition_legacy_preselection_labels(
                    row_invariants,
                    row_exclusions,
                )
            )
            legacy_reclassified_exclusions.update(row_reclassified)
            legacy_reclassified_runs += int(bool(row_reclassified))
            invalid_invariants.update(row_actual_invariants)
            invalid_runs += int(bool(row_actual_invariants))
            fallback = str(diag.get("fallback_reason") or "none")
            fallback_reasons[fallback] += 1
            visibility += _aggregate_count(diag.get("visibility_exclusions"))
            budget += _aggregate_count(diag.get("token_budget_exclusions"))
            duplicates += _aggregate_count(diag.get("duplicate_suppression"))
            contradictions += _aggregate_count(
                diag.get("contradiction_resolution_count")
            )
            moment_candidates += _aggregate_count(diag.get("moment_candidate_count"))
            moment_review += _aggregate_count(
                diag.get("moment_needs_review_excluded")
            )
    except (TypeError, ValueError, OverflowError) as exc:
        return _report_error(
            {
                **_empty_governance_report(),
                "tablePresent": True,
                "aggregateDiagnosticsPresent": has_diagnostics,
            },
            exc,
        )

    run_count = len(rows)
    return {
        "tablePresent": True,
        "aggregateDiagnosticsPresent": has_diagnostics,
        "aggregateDiagnosticRuns": aggregate_runs,
        "runs": run_count,
        "selected_total": selected_total,
        "zero_selection_runs": zero_selection_runs,
        "excluded_total": sum(exclusions.values()),
        "excluded_by_reason": dict(sorted(exclusions.items())),
        "processing_errors": processing_errors,
        "same_as_legacy_hash_runs": same_hash,
        "different_from_legacy_hash_runs": run_count - same_hash,
        "selected_by_source": dict(sorted(selected_sources.items())),
        "invalid_invariants": dict(sorted(invalid_invariants.items())),
        "invalid_invariant_count": sum(invalid_invariants.values()),
        "invalid_invariant_runs": invalid_runs,
        "legacy_reclassified_exclusions": dict(
            sorted(legacy_reclassified_exclusions.items())
        ),
        "legacy_reclassified_exclusion_count": sum(
            legacy_reclassified_exclusions.values()
        ),
        "legacy_reclassified_exclusion_runs": legacy_reclassified_runs,
        "fallback_reasons": dict(sorted(fallback_reasons.items())),
        "visibility_exclusions": visibility,
        "token_budget_exclusions": budget,
        "duplicate_suppression": duplicates,
        "contradiction_resolution_count": contradictions,
        "moment_candidate_count": moment_candidates,
        "moment_needs_review_excluded": moment_review,
        "last_run_at": str(rows[0][2]) if rows else "none",
        "evidenceWindow": {
            "first": str(rows[-1][2]) if rows else "none",
            "last": str(rows[0][2]) if rows else "none",
        },
    }


def _empty_relationship_report() -> Dict[str, Any]:
    return {
        "tablePresent": False,
        "eligible_user_authored_events": 0,
        "rejected_or_unclassified_user_evidence": 0,
        "engagement_shadow_runs": 0,
        "engagement_would_select": 0,
        "engagement_live_emission_allowed": 0,
        "policy_eligible_shadow_candidates": 0,
        "actual_live_emissions": 0,
        "withheld_reasons": {},
        "cross_guild_member_violations": 0,
        "ledger_link_integrity_violations": 0,
        "moment_link_integrity_violations": 0,
        "visibility_violations": 0,
        "processing_errors": 0,
        "legacy_v2_comparison": "not_collected",
    }


def _read_relationship_report(conn: sqlite3.Connection, guild_id: int) -> Dict[str, Any]:
    required = {
        "relationship_events_v2",
        "relationship_observation_diagnostics_v2",
        "relationship_event_moment_links_v2",
        "relationship_engagement_shadow_runs",
    }
    if not all(_table_exists(conn, table) for table in required):
        return _empty_relationship_report()
    try:
        report = build_relationship_evaluation_report(
            conn,
            guild_id=guild_id,
            prepare_schema=False,
        )
        report["engagement_shadow_runs"] = int(
            report.get("engagement_shadow_runs", report.get("shadow_runs", 0)) or 0
        )
        report["engagement_would_select"] = int(
            report.get("engagement_would_select", report.get("would_select", 0)) or 0
        )
        report["engagement_live_emission_allowed"] = int(
            report.get(
                "engagement_live_emission_allowed",
                report.get("live_emission_allowed", 0),
            )
            or 0
        )
        report["evidenceWindow"] = _evidence_window(
            conn,
            table_name="relationship_engagement_shadow_runs",
            timestamp_column="created_at",
            guild_id=guild_id,
        )
        report["tablePresent"] = True
        return report
    except (sqlite3.DatabaseError, ValueError, TypeError) as exc:
        return _report_error({**_empty_relationship_report(), "tablePresent": True}, exc)


def _empty_unified_assessment_report() -> Dict[str, Any]:
    return {
        "tablePresent": False,
        "runs": 0,
        "response_sent_runs": 0,
        "current_exchange_primary_runs": 0,
        "comparison_status_counts": {},
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


def _read_unified_assessment_report(
    conn: sqlite3.Connection,
    guild_id: int,
) -> Dict[str, Any]:
    try:
        return build_unified_assessment_evaluation_report(
            conn,
            guild_id=guild_id,
            prepare_schema=False,
        )
    except (sqlite3.DatabaseError, ValueError, TypeError) as exc:
        return _report_error(
            _empty_unified_assessment_report(),
            exc,
        )


def _safe_context_report(diagnostics: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    source = diagnostics or {}
    numeric = (
        "same_room_paired_turn_count",
        "cross_channel_paired_turn_count",
        "unpaired_row_count",
        "current_message_duplicates_removed",
        "visibility_policy_exclusions",
        "final_char_count",
    )
    result: Dict[str, Any] = {
        "scope": "process_last_run",
        "contract_version": str(source.get("contract_version") or "unknown")[:64],
        "route_mode": str(source.get("route_mode") or "unknown")[:64],
        "channel_policy": str(source.get("channel_policy") or "unknown")[:64],
        "selection_fallback_reason": str(
            source.get("selection_fallback_reason") or "not_used"
        )[:96],
        "selection_reason_count": len(list(source.get("selection_reasons") or ())[:8]),
    }
    for key in numeric:
        result[key] = int(source.get(key) or 0)
    return result


def _positive(report: Mapping[str, Any], keys: Sequence[str]) -> bool:
    return any(int(report.get(key, 0) or 0) > 0 for key in keys)


def _stage_status(
    *,
    requested: bool,
    prerequisites: Sequence[str],
    evidence: bool,
    blockers: Sequence[str],
) -> str:
    if not requested:
        return "disabled"
    if blockers:
        return "failed"
    if prerequisites:
        return "blocked_by_prerequisite"
    if not evidence:
        return "collecting"
    return "candidate_pass_owner_review_required"


def build_v2_shadow_acceptance_snapshot(
    conn: sqlite3.Connection,
    *,
    guild_id: int,
    environ: Optional[Mapping[str, str]] = None,
    conversation_context_diagnostics: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """Build a dependency-aware snapshot from aggregate evidence only."""

    gates = build_gate_snapshot(environ)
    ledger = _read_ledger_report(conn, guild_id)
    moments = _read_moment_report(conn, guild_id)
    governance = build_persisted_governance_report(conn, guild_id=guild_id)
    relationship = _read_relationship_report(conn, guild_id)
    unified_assessment = _read_unified_assessment_report(conn, guild_id)
    context = _safe_context_report(conversation_context_diagnostics)

    live_gates = [
        name
        for name, enabled in (
            ("BNL_MEMORY_GOVERNANCE_LIVE_ENABLED", gates["memory_governance_live_requested"]),
            ("BNL_RELATIONSHIP_V2_LIVE_ENABLED", gates["relationship_v2_live_requested"]),
            ("BNL_ACTIVE_ENGAGEMENT_V2_LIVE_ENABLED", gates["active_engagement_v2_live_requested"]),
        )
        if enabled
    ]

    ledger_blockers = []
    if int(ledger.get("shadowWriteErrors", 0) or 0):
        ledger_blockers.append("shadow_write_errors")
    if int(ledger.get("danglingLineageTargets", 0) or 0):
        ledger_blockers.append("dangling_lineage_targets")
    if ledger.get("reportError"):
        ledger_blockers.append("report_error:%s" % ledger["reportError"])
    ledger_evidence = (
        int(ledger.get("eligibleLegacyWrites", 0) or 0) > 0
        and int(ledger.get("insertedLedgerEntries", 0) or 0) > 0
        and int(ledger.get("exactSourceDeduplications", 0) or 0) > 0
    )
    ledger_status = _stage_status(
        requested=gates["memory_ledger_shadow_requested"],
        prerequisites=(),
        evidence=ledger_evidence,
        blockers=ledger_blockers,
    )

    moment_blockers = [
        key
        for key in (
            "processing_errors",
            "duplicate_memberships",
            "bnl_only_violations",
            "cross_guild_violations",
            "cross_channel_violations",
            "incompatible_visibility_violations",
            "dangling_lineage_targets",
        )
        if int(moments.get(key, 0) or 0)
    ]
    if moments.get("reportError"):
        moment_blockers.append("report_error:%s" % moments["reportError"])
    moment_evidence = (
        int(moments.get("eligible_entries_observed", 0) or 0) > 0
        and int(moments.get("finalized_moments", 0) or 0) > 0
    )
    moment_prerequisites = [] if ledger_status.startswith("candidate_pass") else ["memory_ledger"]
    moment_status = _stage_status(
        requested=gates["moment_engine_shadow_requested"],
        prerequisites=moment_prerequisites,
        evidence=moment_evidence,
        blockers=moment_blockers,
    )

    governance_blockers = []
    if int(governance.get("processing_errors", 0) or 0):
        governance_blockers.append("processing_errors")
    if int(governance.get("invalid_invariant_count", 0) or 0):
        governance_blockers.append("invalid_invariants")
    if governance.get("reportError"):
        governance_blockers.append("report_error:%s" % governance["reportError"])
    governance_evidence = (
        int(governance.get("runs", 0) or 0) > 0
        and int(governance.get("aggregateDiagnosticRuns", 0) or 0) > 0
    )
    governance_prerequisites = [
        name
        for name, status in (
            ("memory_ledger", ledger_status),
            ("moment_engine", moment_status),
        )
        if not status.startswith("candidate_pass")
    ]
    governance_status = _stage_status(
        requested=gates["memory_governance_shadow_requested"],
        prerequisites=governance_prerequisites,
        evidence=governance_evidence,
        blockers=governance_blockers,
    )

    relationship_blockers = []
    for key in (
        "processing_errors",
        "visibility_violations",
        "cross_guild_member_violations",
        "ledger_link_integrity_violations",
        "moment_link_integrity_violations",
        "engagement_live_emission_allowed",
        "actual_live_emissions",
    ):
        try:
            value = int(relationship.get(key, 0) or 0)
        except (TypeError, ValueError):
            value = 1
        if value:
            relationship_blockers.append(key)
    if relationship.get("reportError"):
        relationship_blockers.append("report_error:%s" % relationship["reportError"])
    relationship_evidence = (
        _positive(
            relationship,
            ("eligible_user_authored_events", "rejected_or_unclassified_user_evidence"),
        )
        and int(relationship.get("engagement_shadow_runs", 0) or 0) > 0
    )
    relationship_prerequisites = [
        name
        for name, status in (
            ("memory_ledger", ledger_status),
            ("moment_engine", moment_status),
            ("memory_governance", governance_status),
        )
        if not status.startswith("candidate_pass")
    ]
    relationship_status = _stage_status(
        requested=gates["relationship_v2_shadow_requested"],
        prerequisites=relationship_prerequisites,
        evidence=relationship_evidence,
        blockers=relationship_blockers,
    )

    context_cross_channel = int(context.get("cross_channel_paired_turn_count", 0) or 0)
    if not gates["conversation_context_v2"]:
        context_status = "rollback_disabled"
    elif context_cross_channel:
        context_status = "failed_cross_channel_pair_detected"
    elif int(context.get("same_room_paired_turn_count", 0) or 0) > 0:
        context_status = "candidate_pass_process_last_run_only"
    else:
        context_status = "collecting_process_last_run_only"

    stages = {
        "memory_ledger": {
            "status": ledger_status,
            "evidenceObserved": ledger_evidence,
            "blockers": ledger_blockers,
            "prerequisites": [],
        },
        "moment_engine": {
            "status": moment_status,
            "evidenceObserved": moment_evidence,
            "blockers": moment_blockers,
            "prerequisites": moment_prerequisites,
        },
        "memory_governance": {
            "status": governance_status,
            "evidenceObserved": governance_evidence,
            "blockers": governance_blockers,
            "prerequisites": governance_prerequisites,
        },
        "relationship_v2": {
            "status": relationship_status,
            "evidenceObserved": relationship_evidence,
            "blockers": relationship_blockers,
            "prerequisites": relationship_prerequisites,
        },
    }

    unified_assessment_blockers = []
    for key in (
        "processing_errors",
        "behavior_changed_runs",
        "new_authority_applied_runs",
    ):
        if int(unified_assessment.get(key, 0) or 0):
            unified_assessment_blockers.append(key)
    if unified_assessment.get("content_fields_present"):
        unified_assessment_blockers.append("content_fields_present")
    if unified_assessment.get("reportError"):
        unified_assessment_blockers.append(
            "report_error:%s" % unified_assessment["reportError"]
        )

    blockers = ["live_gate_enabled:%s" % name for name in live_gates]
    if context_cross_channel:
        blockers.append("conversation_context_cross_channel_pair")
    for stage_name, stage in stages.items():
        blockers.extend("%s:%s" % (stage_name, reason) for reason in stage["blockers"])
    blockers.extend(
        "unified_response_assessment:%s" % reason
        for reason in unified_assessment_blockers
    )

    warnings = []
    if int(ledger.get("legacyToLedgerParityMismatches", 0) or 0):
        warnings.append("ledger_parity_metric_informational_derived_rows_included")
    if int(ledger.get("missingUnmappedProvenance", 0) or 0):
        warnings.append("ledger_unmapped_provenance_review")
    if int(ledger.get("entriesWithMultipleActiveValues", 0) or 0):
        warnings.append("ledger_multiple_active_values_review")
    if int(moments.get("affected_moments_awaiting_correction_review", 0) or 0):
        warnings.append("moment_correction_review_pending")
    if relationship.get("legacy_v2_comparison") == "not_collected":
        warnings.append("relationship_legacy_v2_comparison_not_collected")
    if int(governance.get("aggregateDiagnosticRuns", 0) or 0) < int(
        governance.get("runs", 0) or 0
    ):
        warnings.append("governance_older_rows_lack_aggregate_diagnostics")
    if int(governance.get("legacy_reclassified_exclusion_count", 0) or 0):
        warnings.append("governance_legacy_safe_exclusions_reclassified")
    if (
        gates.get("unified_response_assessment_shadow_effective")
        and int(unified_assessment.get("runs", 0) or 0) == 0
    ):
        warnings.append("unified_assessment_no_response_evidence")
    if any(
        int(unified_assessment.get(key, 0) or 0)
        for key in (
            "prompt_overincluded_runs",
            "prompt_underincluded_runs",
            "prompt_different_runs",
        )
    ):
        warnings.append("unified_assessment_prompt_comparison_review")
    if int(unified_assessment.get("visible_control_marker_runs", 0) or 0):
        warnings.append("unified_assessment_visible_control_marker_review")

    stage_statuses = [stage["status"] for stage in stages.values()]
    unified_assessment_ready = bool(
        not gates.get("unified_response_assessment_shadow_requested")
        or (
            int(unified_assessment.get("runs", 0) or 0) > 0
            and not unified_assessment_blockers
        )
    )
    if live_gates:
        status = "blocked_live_authority_detected"
    elif blockers:
        status = "blocked_shadow_invariant_failure"
    elif (
        all(value.startswith("candidate_pass") for value in stage_statuses)
        and unified_assessment_ready
    ):
        status = "ready_for_owner_review_not_live_cutover"
    else:
        status = "collecting_shadow_evidence"

    return {
        "version": SHADOW_ACCEPTANCE_VERSION,
        "status": status,
        "evaluationOrder": list(SHADOW_EVALUATION_ORDER),
        "conversationContextPreflight": {
            "status": context_status,
            "scope": "process_last_run",
            "blocksAutomaticCutover": bool(context_cross_channel),
        },
        "gates": gates,
        "stages": stages,
        "blockers": sorted(set(blockers)),
        "warnings": sorted(set(warnings)),
        "reports": {
            "conversationContextV2": context,
            "memoryLedger": ledger,
            "momentEngine": moments,
            "memoryGovernance": governance,
            "relationshipV2": relationship,
            "unifiedResponseAssessment": unified_assessment,
        },
        "unifiedResponseAssessmentShadow": {
            "requested": bool(
                gates.get(
                    "unified_response_assessment_shadow_requested"
                )
            ),
            "effective": bool(
                gates.get(
                    "unified_response_assessment_shadow_effective"
                )
            ),
            "reason": str(
                gates.get(
                    "unified_response_assessment_shadow_reason",
                    "disabled",
                )
            ),
            "evidenceObserved": int(
                unified_assessment.get("runs", 0) or 0
            )
            > 0,
            "blockers": unified_assessment_blockers,
        },
        "rollback": {
            "legacyProductionTruthPreserved": gates["all_live_gates_clear"],
            "databaseDeletionRequired": False,
            "restartRequiredAfterEnvironmentChange": True,
            "disableOrder": [
                "BNL_UNIFIED_RESPONSE_ASSESSMENT_SHADOW_ENABLED",
                "BNL_ACTIVE_ENGAGEMENT_V2_LIVE_ENABLED",
                "BNL_RELATIONSHIP_V2_LIVE_ENABLED",
                "BNL_MEMORY_GOVERNANCE_LIVE_ENABLED",
                "BNL_RELATIONSHIP_V2_SHADOW_ENABLED",
                "BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED",
                "BNL_MOMENT_ENGINE_SHADOW_ENABLED",
                "BNL_MEMORY_LEDGER_SHADOW_ENABLED",
            ],
            "retainShadowTablesForEvidence": True,
        },
        "ownerReviewRequired": True,
        "automaticCutoverAllowed": False,
        "behaviorChangesApplied": False,
    }


def _on(value: Any) -> str:
    return "on" if value else "off"


def render_v2_shadow_acceptance_lines(snapshot: Mapping[str, Any]) -> List[str]:
    """Render aggregate-only owner diagnostics; never include member content or IDs."""

    gates = snapshot.get("gates") or {}
    stages = snapshot.get("stages") or {}
    reports = snapshot.get("reports") or {}
    context = reports.get("conversationContextV2") or {}
    ledger = reports.get("memoryLedger") or {}
    moments = reports.get("momentEngine") or {}
    governance = reports.get("memoryGovernance") or {}
    relationship = reports.get("relationshipV2") or {}
    unified_assessment = reports.get("unifiedResponseAssessment") or {}
    unified_state = snapshot.get("unifiedResponseAssessmentShadow") or {}
    blockers = snapshot.get("blockers") or []
    warnings = snapshot.get("warnings") or []

    return [
        "**V2 Shadow Acceptance / Rollback Evidence**",
        "- purpose: `read-only evidence; no gates or behavior changed`",
        "- overall_status: `%s`" % snapshot.get("status", "unknown"),
        "- evaluation_order: `%s`" % " -> ".join(snapshot.get("evaluationOrder") or SHADOW_EVALUATION_ORDER),
        "- all_live_gates_clear: `%s`" % ("yes" if gates.get("all_live_gates_clear") else "NO - STOP"),
        "- context_v2_preflight: `%s` scope=`process_last_run` same_room_pairs=`%s` cross_channel_pairs=`%s` fallback=`%s`" % (
            (snapshot.get("conversationContextPreflight") or {}).get("status", "unknown"),
            context.get("same_room_paired_turn_count", 0),
            context.get("cross_channel_paired_turn_count", 0),
            context.get("selection_fallback_reason", "not_used"),
        ),
        "- ledger: status=`%s` shadow=`%s` receipts=`%s` inserted=`%s` deduplicated=`%s` skipped=`%s` errors=`%s` actual_rows=`%s` dangling_lineage=`%s` window_last=`%s`" % (
            (stages.get("memory_ledger") or {}).get("status", "unknown"),
            _on(gates.get("memory_ledger_shadow_requested")),
            ledger.get("eligibleLegacyWrites", 0),
            ledger.get("insertedLedgerEntries", 0),
            ledger.get("exactSourceDeduplications", 0),
            json.dumps(ledger.get("skippedWrites", {}), sort_keys=True),
            ledger.get("shadowWriteErrors", 0),
            ledger.get("actualLedgerRows", 0),
            ledger.get("danglingLineageTargets", 0),
            (ledger.get("evidenceWindow") or {}).get("last", "none"),
        ),
        "- ledger_parity_metric: `%s` (`informational`; intentional derived rows are included)" % ledger.get("legacyToLedgerParityMismatches", 0),
        "- moments: status=`%s` requested=`%s` effective=`%s` eligible=`%s` open=`%s` finalized=`%s` rejected=`%s` errors=`%s` safety_violations=`%s` window_last=`%s`" % (
            (stages.get("moment_engine") or {}).get("status", "unknown"),
            _on(gates.get("moment_engine_shadow_requested")),
            _on(gates.get("moment_engine_shadow_effective")),
            moments.get("eligible_entries_observed", 0),
            moments.get("open_windows", 0),
            moments.get("finalized_moments", 0),
            json.dumps(moments.get("rejected_windows_by_reason", {}), sort_keys=True),
            moments.get("processing_errors", 0),
            sum(int(moments.get(key, 0) or 0) for key in ("duplicate_memberships", "bnl_only_violations", "cross_guild_violations", "cross_channel_violations", "incompatible_visibility_violations", "dangling_lineage_targets")),
            (moments.get("evidenceWindow") or {}).get("last", "none"),
        ),
        "- governance: status=`%s` shadow=`%s` live_requested=`%s` live_effective=`%s` runs=`%s` aggregate_runs=`%s` selected=`%s` empty_runs=`%s` errors=`%s` invalid_invariants=`%s` window_last=`%s`" % (
            (stages.get("memory_governance") or {}).get("status", "unknown"),
            _on(gates.get("memory_governance_shadow_requested")),
            _on(gates.get("memory_governance_live_requested")),
            _on(gates.get("memory_governance_live_effective")),
            governance.get("runs", 0),
            governance.get("aggregateDiagnosticRuns", 0),
            governance.get("selected_total", 0),
            governance.get("zero_selection_runs", 0),
            governance.get("processing_errors", 0),
            governance.get("invalid_invariant_count", 0),
            (governance.get("evidenceWindow") or {}).get("last", "none"),
        ),
        "- governance_invalid_invariants: `%s`" % json.dumps(governance.get("invalid_invariants", {}), sort_keys=True),
        "- governance_legacy_reclassified_exclusions: `%s`" % json.dumps(governance.get("legacy_reclassified_exclusions", {}), sort_keys=True),
        "- governance_exclusions: `%s`" % json.dumps(governance.get("excluded_by_reason", {}), sort_keys=True),
        "- relationship: status=`%s` shadow=`%s` relationship_live=`%s` engagement_live=`%s` events=`%s` rejected_observations=`%s` plans=`%s` would_select=`%s` live_allowed=`%s` actual_emissions=`%s` errors=`%s` window_last=`%s`" % (
            (stages.get("relationship_v2") or {}).get("status", "unknown"),
            _on(gates.get("relationship_v2_shadow_requested")),
            _on(gates.get("relationship_v2_live_requested")),
            _on(gates.get("active_engagement_v2_live_requested")),
            relationship.get("eligible_user_authored_events", 0),
            relationship.get("rejected_or_unclassified_user_evidence", 0),
            relationship.get("engagement_shadow_runs", 0),
            relationship.get("engagement_would_select", 0),
            relationship.get("engagement_live_emission_allowed", 0),
            relationship.get("actual_live_emissions", 0),
            relationship.get("processing_errors", 0),
            (relationship.get("evidenceWindow") or {}).get("last", "none"),
        ),
        "- relationship_withheld_reasons: `%s`" % json.dumps(relationship.get("withheld_reasons", {}), sort_keys=True),
        "- relationship_link_integrity: ledger=`%s` moments=`%s`" % (
            relationship.get("ledger_link_integrity_violations", 0),
            relationship.get("moment_link_integrity_violations", 0),
        ),
        "- relationship_legacy_v2_comparison: `%s`" % relationship.get("legacy_v2_comparison", "not_collected"),
        "- unified_assessment: requested=`%s` effective=`%s` reason=`%s` runs=`%s` current_primary=`%s` comparison=`%s` alignments=`%s` source_changes=`%s` guards=`%s/%s` visible_control_markers=`%s` errors=`%s` behavior_changes=`%s` new_authority=`%s` window_last=`%s`" % (
            _on(unified_state.get("requested")),
            _on(unified_state.get("effective")),
            unified_state.get("reason", "disabled"),
            unified_assessment.get("runs", 0),
            unified_assessment.get("current_exchange_primary_runs", 0),
            json.dumps(
                unified_assessment.get("comparison_status_counts", {}),
                sort_keys=True,
            ),
            json.dumps(
                unified_assessment.get("response_alignment_counts", {}),
                sort_keys=True,
            ),
            unified_assessment.get("source_basis_changed_runs", 0),
            unified_assessment.get("guard_triggered_runs", 0),
            unified_assessment.get("guard_repaired_runs", 0),
            unified_assessment.get("visible_control_marker_runs", 0),
            unified_assessment.get("processing_errors", 0),
            unified_assessment.get("behavior_changed_runs", 0),
            unified_assessment.get("new_authority_applied_runs", 0),
            (unified_assessment.get("evidenceWindow") or {}).get(
                "last",
                "none",
            ),
        ),
        "- blockers: `%s`" % (", ".join(blockers) if blockers else "none"),
        "- warnings: `%s`" % (", ".join(warnings) if warnings else "none"),
        "- rollback: `disable in reverse dependency order; restart; preserve shadow evidence; delete nothing`",
        "- owner_review_required: `yes`",
        "- automatic_cutover: `forbidden`",
    ]
