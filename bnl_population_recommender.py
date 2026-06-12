"""BNL shared-memory population recommendation helpers.

The population recommender scans approved local/shared BNL evidence records and
builds admin-only routing recommendations for the existing site recommendation
pipeline. It does not publish pages, edit public dossier text, create a new
public dossier system, merge identities, or discard raw evidence references.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import sqlite3
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Any, Callable

from bnl_admin_summaries import build_admin_summary, build_dossier_update_summary
from bnl_dossier_recommendations import build_dossier_recommendation_payload, normalize_trusted_site_callback_base_url, send_dossier_recommendation

GENERATED_BY = "BNL"
POPULATION_RECOMMENDATION_VERSION = "population_recommendation_v1"

RECOMMENDED_LANES = {
    "candidate_intake",
    "active_source_file",
    "existing_dossier_update",
    "public_dossier_update_signal",
    "identity_review",
    "diagnostic_artifact",
    "resolved_duplicate",
    "needs_population_review",
}
RECOMMENDED_ACTIONS = {
    "create_source_file_candidate",
    "attach_to_existing_source_file",
    "create_dossier_update_workspace",
    "attach_to_existing_dossier_update",
    "suggest_identity_link",
    "keep_separate",
    "archive_diagnostic",
    "mark_duplicate_no_new_info",
    "admin_review_required",
}
POPULATION_RECOMMENDATION_SCHEMA_FIELDS = (
    "recommendationId",
    "generatedAt",
    "generatedBy",
    "subjectName",
    "normalizedSubjectKey",
    "candidateType",
    "recommendedLane",
    "recommendedAction",
    "confidence",
    "confidenceReason",
    "sourceLanes",
    "sourceEvidenceRefs",
    "memoryWindow",
    "matchedExistingCandidateId",
    "matchedExistingCandidateName",
    "matchedPublicDossierId",
    "matchedPublicDossierName",
    "possibleTargets",
    "duplicateRisk",
    "identityRisk",
    "publicSafetyLevel",
    "adminSummary",
    "missingInfo",
    "recommendedNextStep",
    "doNotPublishReason",
    "rawEvidenceRefs",
    "inputHash",
    "stale",
)
CONFIDENCE_ORDER = {"blocked": -1, "low": 0, "medium": 1, "high": 2}
APPROVED_SOURCE_LANES = {
    "conversation_evidence",
    "conversations",
    "broadcast_memory",
    "rd_context",
    "user_profile_observation",
    "local_profile_observed",
    "relationship_journal",
    "source_file_notes",
    "source_file_archive",
    "source_file_case_report",
    "dossier_recommendation",
    "source_file_candidate",
    "confirmed_alias",
    "public_dossier_match",
    "site_refresh_request",
    "memory_tiers",
    "community_presence",
    "entity_evidence_events",
}
PRIVATE_LANES = {"admin_private", "private_admin", "sealed_test", "diagnostic_test", "test_memory"}
ADMIN_ONLY_POLICIES = {"admin_private", "private", "sealed_test", "internal_only", "admin_only", "protected_system"}
_SAFE_KEY_RE = re.compile(r"[^a-z0-9]+")
_SAFE_LANE_RE = re.compile(r"[^a-z0-9_-]+")
_RAW_BLOB_RE = re.compile(r"\b(?:relationship_journal|local_profile_observed|rawRefJson|sourceRowId|source_row_id|memory_tiers|entity_evidence_events|user_profiles|broadcast_memory|rd_context)\b", re.I)
_UNKNOWN_CHAIN_RE = re.compile(r"\bunknown\s*(?:->|→)\s*unknown\b", re.I)
_ALIAS_RE = re.compile(r"\b(?:alias(?:es)?|aka|known as|identity link)\s*[:=]\s*[^.;,|]+", re.I)
_DIAGNOSTIC_RE = re.compile(r"\b(?:diagnostic|test junk|fixture|synthetic|sandbox|heartbeat|ping test|dummy subject|lorem ipsum)\b", re.I)
_STRONG_DIAGNOSTIC_RE = re.compile(r"\b(?:diagnostic[_ -]?(?:test|artifact|fixture)|test[_ -]?(?:artifact|fixture)|fixture[_ -]?(?:subject|row|marker)|synthetic[_ -]?(?:fixture|artifact)|dummy subject|lorem ipsum)\b", re.I)
_RAW_ID_ONLY_RE = re.compile(r"^(?:\[?redacted[-_ ]?id\]?|unknown|none|null|n/?a|\d+|rowid:?\d+|id:?\d+|https?://\S+|<@!?\d+>|<#\d+>)$", re.I)
_SOURCE_QUOTAS = {
    "conversations": 6,
    "entity_evidence_events": 6,
    "user_memory_facts": 4,
    "relationship_journal": 3,
    "relationship_state": 3,
    "broadcast_memory": 2,
    "community_presence": 3,
    "memory_tiers": 2,
}
_SOURCE_FILE_TABLES = {"source_file_notes", "source_file_archive", "source_file_archives", "dossier_recommendations", "source_file_refresh_requests"}
_SOURCE_FILE_COMBINED_QUOTA = 4
POPULATION_CONTEXT_PATH = "/api/bnl/population-context"
POPULATION_CONTEXT_TIMEOUT_SECONDS = 8


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def normalize_subject_key(value: Any) -> str:
    normalized = _SAFE_KEY_RE.sub("-", str(value or "").strip().lower()).strip("-")
    return normalized or "unknown-subject"


def _safe_text(value: Any, limit: int = 240) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list, tuple, set)):
        try:
            text = json.dumps(value, sort_keys=True, default=str)
        except Exception:
            text = str(value)
    else:
        text = str(value)
    text = re.sub(r"\s+", " ", text).strip()
    text = re.sub(r"<@!?\d+>|<@&\d+>|<#\d+>", "[redacted-mention]", text)
    text = re.sub(r"\b\d{15,22}\b", "[redacted-id]", text)
    text = re.sub(r"https?://\S+", "[redacted-url]", text, flags=re.I)
    text = _UNKNOWN_CHAIN_RE.sub("", text)
    text = _RAW_BLOB_RE.sub("internal evidence", text)
    text = _ALIAS_RE.sub("alias/identity signal kept internal", text)
    return text[:limit].strip(" -|;,.")


def _is_insufficient_subject_identity(value: Any) -> bool:
    """Return True for raw IDs/redacted placeholders that must not become subjects."""

    text = _safe_text(value, 160)
    if not text:
        return True
    low = text.strip().lower()
    if _RAW_ID_ONLY_RE.match(low):
        return True
    if low in {"[redacted-id]", "[redacted-mention]", "[redacted-url]", "redacted id", "redacted-id"}:
        return True
    if re.fullmatch(r"[a-f0-9]{16,64}", low):
        return True
    return False


def _safe_subject_name(value: Any) -> str:
    text = _safe_text(value, 140)
    return "" if _is_insufficient_subject_identity(text) else text


def _json_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        parsed = json.loads(str(value))
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def _first_mapping_value(mapping: dict[str, Any], keys: list[str]) -> Any:
    for key in keys:
        value = mapping.get(key)
        if value not in (None, "", [], {}):
            return value
    return None

def _list(value: Any, *, limit: int = 12, item_limit: int = 180) -> list[str]:
    if value is None:
        items: list[Any] = []
    elif isinstance(value, dict):
        items = [f"{k}: {v}" for k, v in value.items() if v not in (None, "", [], {})]
    elif isinstance(value, (list, tuple, set)):
        items = list(value)
    else:
        items = [value]
    out: list[str] = []
    seen: set[str] = set()
    for item in items:
        text = _safe_text(item, item_limit)
        if not text or text.lower() in seen:
            continue
        seen.add(text.lower())
        out.append(text)
        if len(out) >= limit:
            break
    return out


def _raw_ref_list(value: Any, *, limit: int = 24, item_limit: int = 180) -> list[str]:
    items = value if isinstance(value, (list, tuple, set)) else [value]
    out: list[str] = []
    seen: set[str] = set()
    for item in items:
        if isinstance(item, (list, tuple, set)):
            nested = _raw_ref_list(list(item), limit=limit - len(out), item_limit=item_limit)
            for text in nested:
                if text.lower() not in seen:
                    seen.add(text.lower()); out.append(text)
            continue
        text = str(item or "").strip()
        text = re.sub(r"\s+", " ", text)
        text = re.sub(r"<@!?\d+>|<@&\d+>|<#\d+>", "[redacted-mention]", text)
        text = re.sub(r"\b\d{15,22}\b", "[redacted-id]", text)
        text = re.sub(r"https?://\S+", "[redacted-url]", text, flags=re.I)
        text = text[:item_limit].strip(" -|;,")
        if text and text.lower() not in seen:
            seen.add(text.lower()); out.append(text)
        if len(out) >= limit:
            break
    return out


def _lane(value: Any) -> str:
    lane = _SAFE_LANE_RE.sub("_", str(value or "").strip().lower()).strip("_")
    return lane[:64] or "unknown"


def _confidence_allowed(value: Any) -> str:
    confidence = str(value or "medium").strip().lower()
    return confidence if confidence in CONFIDENCE_ORDER else "medium"


def _input_hash(material: Any) -> str:
    encoded = json.dumps(material, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _stable_id(subject_key: str, input_hash: str) -> str:
    return f"bnl-pop-{subject_key}-{input_hash[:12]}"


def _extract_name(record: dict[str, Any]) -> str:
    return _safe_subject_name(record.get("subjectName") or record.get("subject") or record.get("name") or record.get("displayName"))


def _record_subject_key(record: dict[str, Any]) -> str:
    subject = _extract_name(record)
    if not subject:
        return "insufficient-subject-identity"
    return normalize_subject_key(record.get("normalizedSubjectKey") or record.get("subjectKey") or subject)


def _record_lane(record: dict[str, Any]) -> str:
    return _lane(record.get("sourceLane") or record.get("source") or record.get("sourceType") or record.get("lane") or "unknown")


def _record_tier(record: dict[str, Any]) -> str:
    tier = str(record.get("memoryTier") or record.get("tier") or record.get("memoryWindow") or "short_term").strip().lower()
    aliases = {"live": "live", "immediate": "live", "short": "short_term", "mid": "mid_term", "long": "long_term", "archive": "dormant_archive", "dormant": "dormant_archive"}
    return aliases.get(tier, tier if tier in {"live", "short_term", "mid_term", "long_term", "dormant_archive", "admin_private"} else "short_term")


def _is_private_record(record: dict[str, Any]) -> bool:
    lane = _record_lane(record)
    visibility = str(record.get("visibility") or record.get("channelPolicy") or record.get("sourcePolicy") or "").strip().lower()
    return bool(record.get("private") or record.get("adminOnly") or lane in PRIVATE_LANES or visibility in ADMIN_ONLY_POLICIES)


def _is_diagnostic(record: dict[str, Any]) -> bool:
    if record.get("diagnostic") or record.get("testArtifact"):
        return True
    lane = _record_lane(record)
    policy = str(record.get("channelPolicy") or record.get("channel_policy") or record.get("visibility") or "").strip().lower()
    evidence_kind = str(record.get("evidenceKind") or record.get("evidence_kind") or "").strip().lower()
    text = " ".join(str(record.get(k) or "") for k in ("summary", "content", "reason", "evidenceSummary", "sourceLane"))
    if lane in {"diagnostic_test", "test_memory"}:
        return True
    if evidence_kind in {"diagnostic", "diagnostic_test", "test", "test_fixture", "fixture"}:
        return True
    if policy == "sealed_test" and _STRONG_DIAGNOSTIC_RE.search(text):
        return True
    if _STRONG_DIAGNOSTIC_RE.search(text):
        return True
    return False


def _public_dossier_id(record: dict[str, Any]) -> str:
    return _safe_text(record.get("publicDossierId") or record.get("matchedPublicDossierId") or record.get("targetDossierId"), 120)


def _candidate_id(record: dict[str, Any]) -> str:
    return _safe_text(record.get("candidateId") or record.get("sourceFileCandidateId") or record.get("matchedExistingCandidateId") or record.get("targetCandidateId"), 120)


def _build_indexes(items: list[dict[str, Any]], *, id_keys: tuple[str, ...], name_keys: tuple[str, ...]) -> tuple[dict[str, dict[str, Any]], dict[str, list[dict[str, Any]]]]:
    by_id: dict[str, dict[str, Any]] = {}
    by_key: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in items or []:
        if not isinstance(item, dict):
            continue
        for key in id_keys:
            value = _safe_text(item.get(key), 140)
            if value:
                by_id[value] = item
        name = ""
        for key in name_keys:
            name = _safe_text(item.get(key), 140)
            if name:
                break
        subject_key = normalize_subject_key(item.get("subjectKey") or item.get("normalizedSubjectKey") or item.get("slug") or name)
        by_key[subject_key].append(item)
    return by_id, by_key


def _similar_targets(subject_name: str, candidates: list[dict[str, Any]], dossiers: list[dict[str, Any]]) -> list[dict[str, Any]]:
    key = normalize_subject_key(subject_name)
    tokens = {t for t in key.split("-") if len(t) >= 4}
    targets: list[dict[str, Any]] = []
    for kind, rows in (("source_file", candidates or []), ("public_dossier", dossiers or [])):
        for row in rows:
            name = _safe_text(row.get("name") or row.get("subjectName") or row.get("title") or row.get("slug"), 120)
            row_key = normalize_subject_key(row.get("subjectKey") or row.get("normalizedSubjectKey") or row.get("slug") or name)
            if row_key == key:
                continue
            row_tokens = {t for t in row_key.split("-") if len(t) >= 4}
            if tokens and row_tokens and tokens.intersection(row_tokens):
                targets.append({"targetType": kind, "id": _safe_text(row.get("id") or row.get("candidateId") or row.get("publicDossierId"), 120), "name": name, "matchReason": "similar_name"})
            if len(targets) >= 5:
                return targets
    return targets


def _alias_target(subject_key: str, aliases: dict[str, Any]) -> dict[str, Any] | None:
    raw = aliases.get(subject_key) if isinstance(aliases, dict) else None
    if not raw:
        return None
    if isinstance(raw, dict):
        target = dict(raw)
    else:
        target = {"candidateId": raw}
    target["confirmedAlias"] = True
    return target


def build_population_recommendation(
    records: list[dict[str, Any]],
    *,
    existing_candidates: list[dict[str, Any]] | None = None,
    public_dossiers: list[dict[str, Any]] | None = None,
    dossier_update_workspaces: list[dict[str, Any]] | None = None,
    confirmed_aliases: dict[str, Any] | None = None,
    generated_at: str | None = None,
    stale: bool = False,
) -> dict[str, Any] | None:
    """Build one admin-only populationRecommendation from grouped memory records."""

    usable = [dict(r) for r in (records or []) if isinstance(r, dict)]
    if not usable:
        return None
    generated = generated_at or utc_now()
    subject_name = _extract_name(usable[0])
    if _is_insufficient_subject_identity(subject_name):
        return None
    subject_key = _record_subject_key(usable[0])
    lanes = sorted({_record_lane(r) for r in usable}) or ["unknown"]
    tiers = [_record_tier(r) for r in usable]
    tier_counts = Counter(tiers)
    memory_window = ", ".join(f"{tier}:{count}" for tier, count in sorted(tier_counts.items()))
    source_refs = _list([r.get("sourceEvidenceRef") or r.get("evidenceRef") or r.get("ref") or f"{_record_lane(r)}:{_record_tier(r)}" for r in usable], limit=24)
    raw_refs = _raw_ref_list([r.get("rawEvidenceRef") or r.get("rawRef") or r.get("rawEvidenceRefs") or r.get("sourceEvidenceRef") for r in usable], limit=24)
    summaries = _list([r.get("summary") or r.get("content") or r.get("reason") or r.get("evidenceSummary") for r in usable], limit=8)
    private_only = any(_is_private_record(r) for r in usable)
    diagnostics = any(_is_diagnostic(r) for r in usable)
    public_update_signal = any(bool(r.get("publicDossierUpdateSignal") or r.get("dossierUpdateSignal") or str(r.get("candidateType") or "").lower() == "public_dossier_update") for r in usable)
    duplicate_no_new = any(bool(r.get("duplicate") or r.get("noNewInfo")) for r in usable)

    candidates = existing_candidates or []
    dossiers = public_dossiers or []
    workspaces = dossier_update_workspaces or []
    candidate_by_id, candidate_by_key = _build_indexes(candidates, id_keys=("id", "candidateId", "sourceFileCandidateId"), name_keys=("name", "subjectName", "title"))
    dossier_by_id, dossier_by_key = _build_indexes(dossiers, id_keys=("id", "publicDossierId", "dossierId"), name_keys=("name", "subjectName", "title"))
    workspace_by_dossier_id, workspace_by_key = _build_indexes(workspaces, id_keys=("publicDossierId", "targetDossierId", "dossierId", "id"), name_keys=("name", "subjectName", "title"))

    matched_candidate = None
    matched_dossier = None
    matched_workspace = None
    possible_targets: list[dict[str, Any]] = []
    for record in usable:
        cid = _candidate_id(record)
        if cid and cid in candidate_by_id:
            matched_candidate = candidate_by_id[cid]
        did = _public_dossier_id(record)
        if did and did in dossier_by_id:
            matched_dossier = dossier_by_id[did]
        if did and did in workspace_by_dossier_id:
            matched_workspace = workspace_by_dossier_id[did]
    if not matched_candidate and candidate_by_key.get(subject_key):
        if len(candidate_by_key[subject_key]) == 1:
            matched_candidate = candidate_by_key[subject_key][0]
        else:
            possible_targets.extend({"targetType": "source_file", "id": _safe_text(t.get("id") or t.get("candidateId"), 120), "name": _safe_text(t.get("name") or t.get("subjectName"), 120), "matchReason": "conflicting_exact_subject_key"} for t in candidate_by_key[subject_key])
    if not matched_dossier and dossier_by_key.get(subject_key):
        if len(dossier_by_key[subject_key]) == 1:
            matched_dossier = dossier_by_key[subject_key][0]
        else:
            possible_targets.extend({"targetType": "public_dossier", "id": _safe_text(t.get("id") or t.get("publicDossierId"), 120), "name": _safe_text(t.get("name") or t.get("subjectName") or t.get("title"), 120), "matchReason": "conflicting_exact_subject_key"} for t in dossier_by_key[subject_key])
    if not matched_workspace and workspace_by_key.get(subject_key):
        matched_workspace = workspace_by_key[subject_key][0]
    if matched_workspace and not matched_dossier:
        workspace_dossier_id = _safe_text(matched_workspace.get("publicDossierId") or matched_workspace.get("targetDossierId") or matched_workspace.get("dossierId"), 120)
        matched_dossier = dossier_by_id.get(workspace_dossier_id) if workspace_dossier_id else None
        if not matched_dossier:
            matched_dossier = {"id": workspace_dossier_id, "name": _safe_text(matched_workspace.get("name") or matched_workspace.get("subjectName") or subject_name, 120)} if workspace_dossier_id else {"name": _safe_text(matched_workspace.get("name") or matched_workspace.get("subjectName") or subject_name, 120)}
    if matched_workspace and not matched_candidate:
        workspace_candidate_id = _safe_text(matched_workspace.get("candidateId") or matched_workspace.get("sourceFileCandidateId") or matched_workspace.get("targetCandidateId"), 120)
        if workspace_candidate_id:
            matched_candidate = candidate_by_id.get(workspace_candidate_id) or {"id": workspace_candidate_id, "name": _safe_text(matched_workspace.get("name") or matched_workspace.get("subjectName") or subject_name, 120)}

    alias = _alias_target(subject_key, confirmed_aliases or {})
    if alias and not matched_candidate:
        target_id = _safe_text(alias.get("candidateId") or alias.get("sourceFileCandidateId") or alias.get("id"), 120)
        matched_candidate = candidate_by_id.get(target_id) or {"id": target_id, "name": _safe_text(alias.get("canonicalName") or alias.get("subjectName") or subject_name, 120)}

    if not possible_targets:
        possible_targets = _similar_targets(subject_name, candidates, dossiers)

    conflict = len([t for t in possible_targets if t.get("matchReason", "").startswith("conflicting")]) > 1 or len(dossier_by_key.get(subject_key, [])) > 1
    similar_only = bool(possible_targets) and not matched_candidate and not matched_dossier
    all_live = all(t == "live" for t in tiers)
    repeated_mid = tier_counts.get("mid_term", 0) >= 2 or (len(usable) >= 2 and (tier_counts.get("mid_term") or tier_counts.get("short_term")))

    recommended_lane = "needs_population_review"
    recommended_action = "admin_review_required"
    confidence = "low"
    confidence_reason = "Weak or incomplete shared-memory evidence needs admin review."
    duplicate_risk = "low"
    identity_risk = "low"
    missing_info = ["Confirm destination and public-safe evidence before site action."]
    next_step = "Admin should review evidence refs and choose an existing workflow destination."
    public_safety_level = "admin_review_only"
    do_not_publish_reason = "Admin-only recommendation. Do not publish public pages, dossier text, raw evidence, identity assumptions, or internal aliases."

    if diagnostics:
        recommended_lane = "diagnostic_artifact"
        recommended_action = "archive_diagnostic"
        confidence = "high"
        confidence_reason = "Diagnostic/test artifact markers were detected."
        duplicate_risk = "none"
        next_step = "Archive as diagnostic artifact; do not route to public population."
    elif conflict:
        confidence = "blocked"
        confidence_reason = "Conflicting public dossier or Source File targets require admin resolution."
        recommended_lane = "needs_population_review"
        recommended_action = "admin_review_required"
        duplicate_risk = "high"
        identity_risk = "high"
        next_step = "Resolve conflicting targets in the existing consolidation/review workflow before attaching evidence."
    elif duplicate_no_new:
        recommended_lane = "resolved_duplicate"
        recommended_action = "mark_duplicate_no_new_info"
        confidence = "medium"
        confidence_reason = "Evidence was marked as duplicate or no-new-info."
        duplicate_risk = "high"
        next_step = "Mark as duplicate/no-new-info unless an admin identifies new evidence."
    elif matched_workspace:
        recommended_lane = "existing_dossier_update"
        recommended_action = "attach_to_existing_dossier_update"
        confidence = "high"
        confidence_reason = "Exact Dossier Update workspace ID/name/subject key matched an existing workspace."
        duplicate_risk = "low"
        next_step = "Attach evidence to the existing Dossier Update workspace; keep public copy unchanged until reviewed."
    elif matched_candidate:
        recommended_lane = "active_source_file"
        recommended_action = "attach_to_existing_source_file"
        confidence = "high"
        confidence_reason = "Exact Source File candidate ID/subject key or confirmed identity link matched an existing Source File."
        duplicate_risk = "low"
        identity_risk = "medium" if alias else "low"
        next_step = "Attach evidence to the existing Source File; do not create a duplicate candidate."
    elif matched_dossier:
        recommended_lane = "public_dossier_update_signal"
        recommended_action = "create_dossier_update_workspace"
        confidence = "high"
        confidence_reason = "Exact public dossier ID/name/subject key matched an existing public dossier."
        duplicate_risk = "low"
        next_step = "Route as a Dossier Update workspace signal; keep public copy unchanged until reviewed."
    elif public_update_signal:
        recommended_lane = "public_dossier_update_signal"
        recommended_action = "create_dossier_update_workspace"
        confidence = "medium" if not private_only else "blocked"
        confidence_reason = "Shared memory indicates a public dossier update signal but no visible workspace was matched."
        next_step = "Create a Dossier Update workspace through the existing site workflow if admin approves."
    elif similar_only:
        recommended_lane = "needs_population_review"
        recommended_action = "admin_review_required"
        confidence = "low"
        confidence_reason = "Only similar-name or weak alias signals matched; auto-merge is unsafe."
        duplicate_risk = "medium"
        identity_risk = "high"
        next_step = "Review possible targets manually; keep subjects separate until confirmed."
    elif repeated_mid and not all_live:
        recommended_lane = "candidate_intake"
        recommended_action = "create_source_file_candidate"
        confidence = "medium" if not private_only else "blocked"
        confidence_reason = "Repeated short/mid-term memory signals with consistent subject naming support candidate intake."
        duplicate_risk = "medium"
        next_step = "Create a Source File candidate through the existing intake/recommendation workflow after admin review."
    elif all_live:
        recommended_lane = "needs_population_review"
        recommended_action = "admin_review_required"
        confidence = "low"
        confidence_reason = "Live/immediate memory alone is a weak signal and should not create a Source File."
        next_step = "Wait for repeated short/mid-term evidence or attach only if a strong existing target appears."

    if private_only:
        public_safety_level = "private_or_admin_only"
        do_not_publish_reason = "Evidence includes private/admin/test memory; use only for internal review labels and do not publish as public fact."
    if private_only and recommended_action in {"create_source_file_candidate", "create_dossier_update_workspace"}:
        confidence = "blocked"
        confidence_reason = "Private/admin-only evidence cannot drive public-facing population without review."
        recommended_action = "admin_review_required"
        recommended_lane = "needs_population_review"

    matched_candidate_id = _safe_text((matched_candidate or {}).get("id") or (matched_candidate or {}).get("candidateId") or (matched_candidate or {}).get("sourceFileCandidateId"), 120)
    matched_candidate_name = _safe_text((matched_candidate or {}).get("name") or (matched_candidate or {}).get("subjectName") or (matched_candidate or {}).get("title"), 140)
    matched_dossier_id = _safe_text((matched_dossier or {}).get("id") or (matched_dossier or {}).get("publicDossierId") or (matched_dossier or {}).get("dossierId"), 120)
    matched_dossier_name = _safe_text((matched_dossier or {}).get("name") or (matched_dossier or {}).get("subjectName") or (matched_dossier or {}).get("title"), 140)
    public_subject_name = matched_candidate_name if alias and matched_candidate_name else subject_name

    summary_record = {
        "subjectName": public_subject_name,
        "subjectKey": subject_key,
        "confidence": "low" if confidence == "blocked" else confidence,
        "sourceLanes": lanes,
        "knownContext": summaries,
        "usefulEvidence": source_refs,
        "reason": confidence_reason,
        "recommendedAction": next_step,
        "missingInfo": missing_info,
        "publicSafetyNotes": [do_not_publish_reason],
        "doNotSay": ["Do not publish public pages or edit public dossier text from this recommendation.", "Do not expose internal aliases publicly or present identity links as public facts."],
        "rawEvidenceRefs": raw_refs,
    }
    admin_summary = build_dossier_update_summary(summary_record, generated_at=generated, stale=stale) if recommended_lane in {"existing_dossier_update", "public_dossier_update_signal"} else build_admin_summary(summary_record, generated_at=generated, stale=stale)

    material = {
        "subjectKey": subject_key,
        "lanes": lanes,
        "sourceRefs": source_refs,
        "rawRefs": raw_refs,
        "recommendedLane": recommended_lane,
        "recommendedAction": recommended_action,
        "matchedCandidate": matched_candidate_id,
        "matchedDossier": matched_dossier_id,
    }
    input_hash = _input_hash(material)
    recommendation = {
        "recommendationId": _stable_id(subject_key, input_hash),
        "generatedAt": generated,
        "generatedBy": GENERATED_BY,
        "subjectName": public_subject_name,
        "normalizedSubjectKey": subject_key,
        "candidateType": _safe_text(usable[0].get("candidateType") or "shared_memory_subject", 80),
        "recommendedLane": recommended_lane,
        "recommendedAction": recommended_action,
        "confidence": confidence,
        "confidenceReason": confidence_reason,
        "sourceLanes": lanes,
        "sourceEvidenceRefs": source_refs,
        "memoryWindow": memory_window,
        "matchedExistingCandidateId": matched_candidate_id,
        "matchedExistingCandidateName": matched_candidate_name,
        "matchedPublicDossierId": matched_dossier_id,
        "matchedPublicDossierName": matched_dossier_name,
        "possibleTargets": possible_targets,
        "duplicateRisk": duplicate_risk,
        "identityRisk": identity_risk,
        "publicSafetyLevel": public_safety_level,
        "adminSummary": admin_summary,
        "missingInfo": missing_info,
        "recommendedNextStep": next_step,
        "doNotPublishReason": do_not_publish_reason,
        "rawEvidenceRefs": raw_refs,
        "inputHash": input_hash,
        "stale": bool(stale),
    }
    # Guarantee the public/admin summary prose did not leak raw storage labels or unknown chains.
    summary_text = json.dumps(recommendation.get("adminSummary") or {}, sort_keys=True, default=str)
    if _RAW_BLOB_RE.search(summary_text) or _UNKNOWN_CHAIN_RE.search(summary_text):
        recommendation["adminSummary"] = build_admin_summary({**summary_record, "knownContext": ["Internal evidence exists and requires review."], "usefulEvidence": source_refs}, generated_at=generated, stale=stale)
    return recommendation


def build_population_recommendation_payload(recommendation: dict[str, Any]) -> dict[str, Any]:
    """Wrap a populationRecommendation for the existing dossier ingest pathway."""

    rec = dict(recommendation or {})
    return build_dossier_recommendation_payload(
        {
            "type": "population_recommendation",
            "subjectName": rec.get("subjectName"),
            "subjectKey": rec.get("normalizedSubjectKey"),
            "targetCandidateId": rec.get("matchedExistingCandidateId"),
            "targetDossierId": rec.get("matchedPublicDossierId"),
            "reason": rec.get("confidenceReason"),
            "evidenceSummary": rec.get("recommendedNextStep"),
            "confidence": "low" if rec.get("confidence") == "blocked" else rec.get("confidence"),
            "sourceLanes": rec.get("sourceLanes"),
            "suggestedAction": rec.get("recommendedAction"),
            "recommendedAction": rec.get("recommendedAction"),
            "missingInfo": rec.get("missingInfo"),
            "publicSafetyNotes": rec.get("doNotPublishReason"),
            "doNotSay": ["Do not publish pages or edit public dossier text from this recommendation.", "Do not expose internal aliases publicly."],
            "createdBy": "bnl_population_recommender",
            "ingestSource": "bnl_population_recommender",
            "rawProvenance": {"populationRecommendationId": rec.get("recommendationId"), "rawEvidenceRefs": rec.get("rawEvidenceRefs")},
            "adminSummary": rec.get("adminSummary"),
            "populationRecommendation": rec,
        }
    )


def _passes_min_confidence(confidence: str, minimum: str) -> bool:
    if confidence == "blocked":
        return True
    return CONFIDENCE_ORDER.get(confidence, 0) >= CONFIDENCE_ORDER.get(minimum, 0)


def group_memory_records(records: list[dict[str, Any]], *, source_lanes: list[str] | None = None, subject_filter: str | None = None, max_items: int | None = None) -> list[list[dict[str, Any]]]:
    allowed = {_lane(l) for l in source_lanes or []}
    filter_key = normalize_subject_key(subject_filter) if subject_filter else ""
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    scanned = 0
    for record in records or []:
        if not isinstance(record, dict):
            continue
        lane = _record_lane(record)
        if allowed and lane not in allowed:
            continue
        key = _record_subject_key(record)
        if filter_key and key != filter_key and filter_key not in key:
            continue
        grouped[key].append(dict(record))
        scanned += 1
        if max_items and scanned >= max_items:
            break
    return list(grouped.values())


def _safe_error_message(exc: Exception, limit: int = 220) -> str:
    """Return a short operator-safe error reason for Discord/log summaries."""

    message = _safe_text(str(exc) or exc.__class__.__name__, limit)
    message = re.sub(r"(?i)(token|secret|password|authorization|api[_-]?key)\s*[=:]\s*\S+", r"\1=[redacted]", message)
    return message or exc.__class__.__name__


def _quote_ident(identifier: str) -> str:
    return '"' + str(identifier).replace('"', '""') + '"'


def _row_get(row: sqlite3.Row, column: str, default: Any = None) -> Any:
    try:
        return row[column]
    except Exception:
        return default


def _first_existing(cols: set[str], candidates: list[str]) -> str | None:
    for candidate in candidates:
        if candidate in cols:
            return candidate
    return None


def _memory_tier_from_timestamp(value: Any) -> str:
    if not value:
        return "short_term"
    text = str(value)
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        age_days = (datetime.now(timezone.utc) - dt.astimezone(timezone.utc)).days
        return "live" if age_days <= 7 else "short_term"
    except Exception:
        return "short_term"


def _build_user_name_map(conn: sqlite3.Connection, tables: set[str], guild_id: int | None) -> dict[str, str]:
    if "user_profiles" not in tables:
        return {}
    cols = {row[1] for row in conn.execute("PRAGMA table_info(user_profiles)").fetchall()}
    user_col = _first_existing(cols, ["user_id", "id"])
    name_col = _first_existing(cols, ["preferred_name", "display_name", "user_name", "name"])
    if not user_col or not name_col:
        return {}
    where = ""
    params: list[Any] = []
    if "guild_id" in cols and guild_id is not None:
        where = "WHERE guild_id=?"
        params.append(guild_id)
    query = f"SELECT {_quote_ident(user_col)} AS user_id,{_quote_ident(name_col)} AS name FROM user_profiles {where}"
    names: dict[str, str] = {}
    try:
        for row in conn.execute(query, params).fetchall():
            if _row_get(row, "user_id") is not None and _row_get(row, "name"):
                names[str(_row_get(row, "user_id"))] = _safe_text(_row_get(row, "name"), 120)
    except Exception as exc:
        logging.warning("population_scan_table_skipped table=user_profiles reason=%s", _safe_error_message(exc))
    return names


def _as_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, dict):
        for key in ("items", "dossiers", "publicDossiers", "candidates", "sourceFiles", "workspaces", "aliases"):
            if isinstance(value.get(key), list):
                return value.get(key) or []
    return []


def _read_model_sections(read_model: dict[str, Any]) -> dict[str, Any]:
    sections = read_model.get("sections") if isinstance(read_model, dict) else {}
    return sections if isinstance(sections, dict) else {}


def _compact_site_item(item: Any, *, kind: str) -> dict[str, Any] | None:
    if not isinstance(item, dict):
        return None
    name = _safe_subject_name(_first_mapping_value(item, ["name", "subjectName", "title", "displayName", "candidateName"]))
    slug = _safe_text(item.get("slug"), 120)
    subject_key = _safe_text(item.get("subjectKey") or item.get("normalizedSubjectKey") or (normalize_subject_key(name) if name else slug), 140)
    compact = {
        "id": _safe_text(_first_mapping_value(item, ["id", "publicDossierId", "dossierId", "candidateId", "sourceFileCandidateId"]), 140),
        "name": name,
        "subjectName": name,
        "subjectKey": subject_key,
        "slug": slug,
        "kind": kind,
    }
    return {k: v for k, v in compact.items() if v not in (None, "", [], {})}


def extract_population_site_context(read_model: dict[str, Any] | None) -> dict[str, Any]:
    """Extract compact public-safe site context from the existing read model shape."""

    if not isinstance(read_model, dict) or not read_model:
        return {"site_context_loaded": False, "public_dossiers": [], "existing_candidates": [], "dossier_update_workspaces": [], "confirmed_aliases": {}}
    sections = _read_model_sections(read_model)
    public_dossiers: list[dict[str, Any]] = []
    existing_candidates: list[dict[str, Any]] = []
    workspaces: list[dict[str, Any]] = []

    dossier_sources = [read_model.get("publicDossiers"), read_model.get("dossiers"), sections.get("dossiers"), sections.get("publicDossiers"), sections.get("artists")]
    for source in dossier_sources:
        for item in _as_list(source):
            compact = _compact_site_item(item, kind="public_dossier")
            if compact and (compact.get("name") or compact.get("slug")):
                public_dossiers.append(compact)

    source_context = sections.get("sourceContext") if isinstance(sections.get("sourceContext"), dict) else read_model.get("sourceContext")
    candidate_sources = [read_model.get("existingCandidates"), read_model.get("candidates"), read_model.get("sourceFiles"), sections.get("candidates"), sections.get("sourceFiles")]
    if isinstance(source_context, dict):
        candidate_sources.extend([source_context.get("existingCandidates"), source_context.get("candidates"), source_context.get("sourceFiles")])
    for source in candidate_sources:
        for item in _as_list(source):
            compact = _compact_site_item(item, kind="source_file_candidate")
            if compact and (compact.get("id") or compact.get("name")):
                existing_candidates.append(compact)

    workspace_sources = [read_model.get("dossierUpdateWorkspaces"), sections.get("dossierUpdateWorkspaces")]
    if isinstance(source_context, dict):
        workspace_sources.append(source_context.get("dossierUpdateWorkspaces"))
    for source in workspace_sources:
        for item in _as_list(source):
            compact = _compact_site_item(item, kind="dossier_update_workspace")
            if compact:
                public_id = _safe_text(_first_mapping_value(item, ["publicDossierId", "targetDossierId", "dossierId"]), 140)
                if public_id:
                    compact["publicDossierId"] = public_id
                workspaces.append(compact)

    # Only consume alias maps explicitly exposed by the public-safe read model. Internal alias labels are not copied.
    confirmed_aliases: dict[str, Any] = {}
    aliases = read_model.get("confirmedAliases") or sections.get("confirmedAliases")
    if isinstance(aliases, dict):
        for alias_key, target in aliases.items():
            safe_key = normalize_subject_key(alias_key)
            if isinstance(target, dict):
                confirmed_aliases[safe_key] = {k: _safe_text(v, 140) for k, v in target.items() if k in {"candidateId", "sourceFileCandidateId", "id", "canonicalName", "subjectName"}}
            else:
                confirmed_aliases[safe_key] = _safe_text(target, 140)

    def dedupe(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        seen: set[tuple[str, str, str]] = set()
        out: list[dict[str, Any]] = []
        for item in items:
            key = (str(item.get("kind") or ""), str(item.get("id") or ""), str(item.get("subjectKey") or item.get("name") or item.get("slug") or ""))
            if key in seen:
                continue
            seen.add(key); out.append(item)
        return out

    return {
        "site_context_loaded": True,
        "public_dossiers": dedupe(public_dossiers),
        "existing_candidates": dedupe(existing_candidates),
        "dossier_update_workspaces": dedupe(workspaces),
        "confirmed_aliases": confirmed_aliases,
    }


def load_population_site_context(read_model_loader: Callable[..., dict[str, Any]] | None = None) -> dict[str, Any]:
    if not read_model_loader:
        logging.info("population_site_context_unavailable reason=no_read_model_loader")
        return extract_population_site_context(None)
    try:
        read_model = read_model_loader()
    except TypeError:
        read_model = read_model_loader(False)
    except Exception as exc:
        logging.warning("population_site_context_unavailable reason=%s", _safe_error_message(exc))
        return extract_population_site_context(None)
    if not read_model:
        logging.info("population_site_context_unavailable reason=empty_read_model")
        return extract_population_site_context(None)
    context = extract_population_site_context(read_model)
    logging.info(
        "population_site_context_loaded public_dossiers=%s candidates=%s source_files=%s",
        len(context.get("public_dossiers") or []),
        len(context.get("existing_candidates") or []),
        len(context.get("existing_candidates") or []),
    )
    return context


def _dedupe_context_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[str, str, str]] = set()
    out: list[dict[str, Any]] = []
    for item in items or []:
        if not isinstance(item, dict):
            continue
        key = (str(item.get("kind") or ""), str(item.get("id") or item.get("candidateId") or item.get("publicDossierId") or ""), str(item.get("subjectKey") or item.get("name") or item.get("slug") or ""))
        if key in seen:
            continue
        seen.add(key)
        out.append(item)
    return out


def _population_context_url(environ: dict[str, str] | None = None) -> str:
    env = environ if environ is not None else os.environ
    configured = (env.get("BNL_POPULATION_CONTEXT_URL") or "").strip()
    if configured:
        return configured
    base = normalize_trusted_site_callback_base_url(env.get("BNL_WEBSITE_BASE_URL") or env.get("BNL_SITE_BASE_URL"), environ=env)
    if base:
        return urllib.parse.urljoin(base.rstrip("/") + "/", POPULATION_CONTEXT_PATH.lstrip("/"))
    read_model_url = (env.get("BNL_READ_MODEL_URL") or "").strip()
    if read_model_url:
        parsed = urllib.parse.urlparse(read_model_url)
        if parsed.scheme in {"https", "http"} and parsed.netloc:
            return urllib.parse.urlunparse((parsed.scheme, parsed.netloc, POPULATION_CONTEXT_PATH, "", "", ""))
    return ""


def _population_context_token(environ: dict[str, str] | None = None) -> str:
    env = environ if environ is not None else os.environ
    for key in ("BNL_API_KEY", "BNL_TOKEN", "BNL_SOURCE_FILE_READ_TOKEN", "BNL_DOSSIER_INGEST_TOKEN", "BNL_SOURCE_FILE_ARCHIVE_TOKEN"):
        token = (env.get(key) or "").strip()
        if token:
            return token
    return ""


def is_population_context_url_configured(environ: dict[str, str] | None = None) -> bool:
    return bool(_population_context_url(environ))


def is_population_context_auth_configured(environ: dict[str, str] | None = None) -> bool:
    return bool(_population_context_token(environ))


def extract_population_context(context: dict[str, Any] | None) -> dict[str, Any]:
    """Extract admin-only routing context from authenticated population-context v1."""

    if not isinstance(context, dict) or context.get("ok") is not True or context.get("version") != "population_context_v1":
        return {
            "population_context_loaded": False,
            "public_dossiers": [],
            "existing_candidates": [],
            "dossier_update_workspaces": [],
            "confirmed_aliases": {},
            "resolved_records": {},
            "source_files_count": 0,
            "candidates_count": 0,
            "identity_links_count": 0,
            "resolved_records_count": 0,
        }

    public_dossiers: list[dict[str, Any]] = []
    existing_candidates: list[dict[str, Any]] = []
    workspaces: list[dict[str, Any]] = []
    confirmed_aliases: dict[str, Any] = {}
    resolved_records: dict[str, dict[str, Any]] = {}

    for item in _as_list(context.get("publicDossiers")):
        compact = _compact_site_item(item, kind="public_dossier")
        if compact:
            public_dossiers.append(compact)

    source_files = _as_list(context.get("sourceFiles"))
    for item in source_files:
        compact = _compact_site_item(item, kind="source_file")
        if compact:
            existing_candidates.append(compact)

    candidates = _as_list(context.get("candidates") or context.get("candidateIntakeRecords"))
    for item in candidates:
        compact = _compact_site_item(item, kind="candidate_intake")
        if compact:
            existing_candidates.append(compact)

    for item in _as_list(context.get("dossierUpdateWorkspaces")):
        compact = _compact_site_item(item, kind="dossier_update_workspace")
        if compact:
            public_id = _safe_text(_first_mapping_value(item, ["publicDossierId", "targetDossierId", "dossierId"]), 140)
            candidate_id = _safe_text(_first_mapping_value(item, ["candidateId", "sourceFileCandidateId", "targetCandidateId"]), 140)
            if public_id:
                compact["publicDossierId"] = public_id
            if candidate_id:
                compact["candidateId"] = candidate_id
            workspaces.append(compact)

    for link in _as_list(context.get("identityLinks")):
        if not isinstance(link, dict):
            continue
        status = str(link.get("status") or link.get("state") or "confirmed").strip().lower()
        if status not in {"confirmed", "active", "resolved", "approved"}:
            continue
        aliases = []
        for key in ("alias", "aliasName", "sourceName", "fromName", "subjectName"):
            if link.get(key):
                aliases.append(link.get(key))
        aliases.extend(_as_list(link.get("aliases")))
        target = {
            "candidateId": _safe_text(_first_mapping_value(link, ["candidateId", "sourceFileCandidateId", "targetCandidateId", "sourceFileId", "targetSourceFileId", "id"]), 140),
            "sourceFileCandidateId": _safe_text(_first_mapping_value(link, ["sourceFileCandidateId", "sourceFileId", "targetSourceFileId"]), 140),
            "canonicalName": _safe_text(_first_mapping_value(link, ["canonicalName", "targetName", "subjectName", "displayName"]), 140),
            "subjectName": _safe_text(_first_mapping_value(link, ["canonicalName", "targetName", "displayName"]), 140),
        }
        target = {k: v for k, v in target.items() if v}
        for alias in aliases:
            alias_key = normalize_subject_key(alias)
            if alias_key and alias_key != "unknown-subject":
                confirmed_aliases[alias_key] = dict(target)

    for item in _as_list(context.get("resolvedRecords")):
        if not isinstance(item, dict):
            continue
        subject_key = normalize_subject_key(_first_mapping_value(item, ["subjectKey", "normalizedSubjectKey", "name", "subjectName", "alias", "sourceName"]))
        if subject_key and subject_key != "unknown-subject":
            resolved_records[subject_key] = {
                "id": _safe_text(_first_mapping_value(item, ["id", "recordId", "sourceId"]), 140),
                "targetCandidateId": _safe_text(_first_mapping_value(item, ["targetCandidateId", "candidateId", "sourceFileCandidateId"]), 140),
                "targetDossierId": _safe_text(_first_mapping_value(item, ["targetDossierId", "publicDossierId", "dossierId"]), 140),
                "status": _safe_text(item.get("status") or "resolved", 80),
            }

    return {
        "population_context_loaded": True,
        "public_dossiers": _dedupe_context_items(public_dossiers),
        "existing_candidates": _dedupe_context_items(existing_candidates),
        "dossier_update_workspaces": _dedupe_context_items(workspaces),
        "confirmed_aliases": confirmed_aliases,
        "resolved_records": resolved_records,
        "source_files_count": len(source_files),
        "candidates_count": len(candidates),
        "identity_links_count": len(_as_list(context.get("identityLinks"))),
        "resolved_records_count": len(_as_list(context.get("resolvedRecords"))),
    }


def fetch_bnl_population_context(environ: dict[str, str] | None = None) -> dict[str, Any]:
    """Fetch authenticated site population-context as transient routing input only."""

    env = environ if environ is not None else os.environ
    url = _population_context_url(env)
    token = _population_context_token(env)
    if not url or not token:
        logging.info("population_context_fetch_skipped reason=missing_config url_configured=%s auth_configured=%s", bool(url), bool(token))
        return {}
    logging.info("population_context_fetch_started url_configured=%s auth_configured=%s", bool(url), bool(token))
    request = urllib.request.Request(
        url,
        method="GET",
        headers={
            "Accept": "application/json",
            "Authorization": f"Bearer {token}",
            "x-api-key": token,
            "X-BNL-API-Key": token,
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=POPULATION_CONTEXT_TIMEOUT_SECONDS) as response:
            status = getattr(response, "status", None) or response.getcode()
            raw = response.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        logging.warning("population_context_fetch_failed status=%s", getattr(exc, "code", "unknown"))
        return {}
    except Exception as exc:
        logging.warning("population_context_fetch_failed error=%s", _safe_error_message(exc))
        return {}
    if not (200 <= int(status or 0) < 300):
        logging.warning("population_context_fetch_failed status=%s", status)
        return {}
    try:
        data = json.loads(raw) if raw else {}
    except Exception as exc:
        logging.warning("population_context_invalid_shape error=%s", _safe_error_message(exc))
        return {}
    extracted = extract_population_context(data if isinstance(data, dict) else {})
    if not extracted.get("population_context_loaded"):
        logging.warning("population_context_invalid_shape")
        return {}
    logging.info(
        "population_context_fetch_success public_dossiers=%s source_files=%s candidates=%s dossier_update_workspaces=%s identity_links=%s resolved_records=%s",
        len(extracted.get("public_dossiers") or []),
        extracted.get("source_files_count", 0),
        extracted.get("candidates_count", 0),
        len(extracted.get("dossier_update_workspaces") or []),
        extracted.get("identity_links_count", 0),
        extracted.get("resolved_records_count", 0),
    )
    return data


def collect_shared_memory_population_records(
    db_path: str,
    guild_id: int | None,
    *,
    max_items: int = 200,
    source_lanes: list[str] | None = None,
    subject_filter: str | None = None,
    allow_sealed_test: bool = False,
    diagnostics: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Collect approved existing BNL memory/evidence rows for population scan.

    This uses already-existing local evidence tables when present. It creates no
    new memory database and stores nothing. Tables/columns are optional by
    design; unexpected table schemas are skipped with table-level warnings.
    """

    lane_filter = {_lane(l) for l in source_lanes or []}
    subject_filter_key = normalize_subject_key(subject_filter) if subject_filter else ""
    records: list[dict[str, Any]] = []
    table_counts: Counter[str] = Counter()
    attempted_counts: Counter[str] = Counter()
    quota_skips: Counter[str] = Counter()
    policy_skips: Counter[str] = Counter()
    schema_skips: Counter[str] = Counter()
    missing_subject_skips: Counter[str] = Counter()
    filter_decisions: Counter[str] = Counter()
    safe_policies = {"public_home", "public_context", "broadcast_memory", "rd_context"}
    if allow_sealed_test or "sealed_test" in lane_filter:
        safe_policies.add("sealed_test")
    excluded_policies = {"protected_system", "private_admin", "internal_controlled", "unknown"}

    diag = diagnostics if diagnostics is not None else None
    if diag is not None:
        diag.setdefault("db_path_exists", False)
        diag.setdefault("tables", {})
        diag.setdefault("filter_decisions", {})

    def diag_table(table: str, *, exists: bool, cols: set[str] | None = None, rows: int | None = None, collected: int | None = None, skipped_reason: str | None = None) -> None:
        logging.info("population_scan_table_checked table=%s exists=%s", table, 1 if exists else 0)
        if diag is not None:
            info = diag.setdefault("tables", {}).setdefault(table, {})
            info["exists"] = bool(exists)
            if cols is not None:
                info["columns"] = sorted(cols)
            if rows is not None:
                info["row_count"] = rows
            if collected is not None:
                info["records_collected"] = collected
            if skipped_reason:
                info["skipped_reason"] = skipped_reason
                info["skipped_due_to_schema"] = info.get("skipped_due_to_schema", 0) + 1

    def skip_table(table: str, reason: str) -> None:
        logging.warning("population_scan_table_skipped table=%s reason=%s", table, reason)
        schema_skips[table] += 1
        if diag is not None:
            info = diag.setdefault("tables", {}).setdefault(table, {})
            info["skipped_reason"] = reason
            info["skipped_due_to_schema"] = info.get("skipped_due_to_schema", 0) + 1

    def quota_for(table: str) -> int:
        if table in _SOURCE_FILE_TABLES:
            return _SOURCE_FILE_COMBINED_QUOTA
        return _SOURCE_QUOTAS.get(table, max_items)

    def source_file_collected() -> int:
        return sum(table_counts[t] for t in _SOURCE_FILE_TABLES)

    def add(table: str, record: dict[str, Any]) -> None:
        attempted_counts[table] += 1
        if len(records) >= max_items:
            filter_decisions["max_items_reached"] += 1
            quota_skips[table] += 1
            return
        if table in _SOURCE_FILE_TABLES:
            if source_file_collected() >= quota_for(table):
                filter_decisions[f"quota_filtered:{table}"] += 1
                quota_skips[table] += 1
                return
        elif table_counts[table] >= quota_for(table):
            filter_decisions[f"quota_filtered:{table}"] += 1
            quota_skips[table] += 1
            return
        lane = _record_lane(record)
        if lane_filter and lane not in lane_filter:
            filter_decisions[f"lane_filtered:{lane}"] += 1
            policy_skips[table] += 1
            return
        if _is_insufficient_subject_identity(record.get("subjectName") or record.get("subject") or record.get("name") or record.get("displayName")):
            filter_decisions[f"missing_subject_identity:{table}"] += 1
            missing_subject_skips[table] += 1
            return
        if subject_filter_key and subject_filter_key not in _record_subject_key(record):
            filter_decisions["subject_filtered"] += 1
            return
        records.append(record)
        table_counts[table] += 1
        filter_decisions["included"] += 1

    def table_cols(conn: sqlite3.Connection, table: str, tables: set[str]) -> set[str] | None:
        exists = table in tables
        if not exists:
            diag_table(table, exists=False, collected=0)
            skip_table(table, "missing")
            return None
        cols = {row[1] for row in conn.execute(f"PRAGMA table_info({_quote_ident(table)})").fetchall()}
        try:
            row_count = conn.execute(f"SELECT COUNT(*) FROM {_quote_ident(table)}").fetchone()[0]
        except Exception:
            row_count = None
        diag_table(table, exists=True, cols=cols, rows=row_count)
        return cols

    def select_rows(conn: sqlite3.Connection, table: str, cols: set[str], order_candidates: list[str] | None = None) -> list[sqlite3.Row]:
        where_parts: list[str] = []
        params: list[Any] = []
        if "guild_id" in cols and guild_id is not None:
            where_parts.append("guild_id=?")
            params.append(guild_id)
        where = "WHERE " + " AND ".join(where_parts) if where_parts else ""
        order_col = _first_existing(cols, order_candidates or ["updated_at", "timestamp", "created_at", "id"])
        order = f"ORDER BY {_quote_ident(order_col)} DESC" if order_col else "ORDER BY rowid DESC"
        limit = max(max_items, quota_for(table) * 3)
        return conn.execute(f"SELECT rowid,* FROM {_quote_ident(table)} {where} {order} LIMIT ?", (*params, limit)).fetchall()

    if diag is not None:
        diag["db_path_exists"] = bool(db_path and os.path.exists(db_path))
        diag["source_lanes"] = sorted(lane_filter)
        diag["subject_filter"] = bool(subject_filter)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        user_names = _build_user_name_map(conn, tables, guild_id)

        def user_name(user_id: Any, fallback: Any = None) -> str:
            if fallback:
                return _safe_text(fallback, 120)
            return user_names.get(str(user_id), _safe_text(user_id, 80) or "unknown")

        # conversations
        table = "conversations"
        cols = table_cols(conn, table, tables)
        if cols is not None:
            text_col = _first_existing(cols, ["content", "message", "text"])
            if not text_col:
                skip_table(table, "missing content column")
            else:
                for row in select_rows(conn, table, cols, ["timestamp", "created_at", "id"]):
                    policy = str(_row_get(row, "channel_policy", "unknown") or "unknown")
                    if policy in excluded_policies or policy not in safe_policies:
                        filter_decisions[f"policy_filtered:{policy}"] += 1
                        policy_skips[table] += 1
                        continue
                    role = str(_row_get(row, "role", "") or "").lower()
                    subject = user_name(_row_get(row, "user_id"), _row_get(row, "user_name") if role == "user" else None)
                    add(table, {"subjectName": subject, "memoryTier": _memory_tier_from_timestamp(_row_get(row, "timestamp")), "sourceLane": policy if policy in {"broadcast_memory", "rd_context"} else "conversations", "summary": _row_get(row, text_col), "content": _row_get(row, text_col), "sourceEvidenceRef": f"conversations:{_row_get(row, 'id', _row_get(row, 'rowid'))}", "rawEvidenceRef": f"conversations:{_row_get(row, 'id', _row_get(row, 'rowid'))}", "visibility": policy})
            logging.info("population_scan_table_collected table=%s count=%s", table, table_counts[table])

        # user_memory_facts
        table = "user_memory_facts"
        cols = table_cols(conn, table, tables)
        if cols is not None:
            if not {"user_id", "fact_key", "fact_value"}.issubset(cols):
                skip_table(table, "missing required fact columns")
            else:
                for row in select_rows(conn, table, cols, ["updated_at", "id"]):
                    key = _safe_text(_row_get(row, "fact_key"), 80)
                    val = _safe_text(_row_get(row, "fact_value"), 220)
                    add(table, {"subjectName": user_name(_row_get(row, "user_id")), "memoryTier": "long_term" if _row_get(row, "is_core") else "mid_term", "sourceLane": table, "summary": f"{key}: {val}".strip(": "), "sourceEvidenceRef": f"user_memory_facts:{_row_get(row, 'id', _row_get(row, 'rowid'))}", "rawEvidenceRef": f"user_memory_facts:{_row_get(row, 'id', _row_get(row, 'rowid'))}", "visibility": "review_only"})
            logging.info("population_scan_table_collected table=%s count=%s", table, table_counts[table])

        # relationship_journal
        table = "relationship_journal"
        cols = table_cols(conn, table, tables)
        if cols is not None:
            if not {"user_id", "summary"}.issubset(cols):
                skip_table(table, "missing required journal columns")
            else:
                for row in select_rows(conn, table, cols, ["timestamp", "updated_at", "id"]):
                    safe_summary = _safe_text(_row_get(row, "summary"), 180)
                    add(table, {"subjectName": user_name(_row_get(row, "user_id")), "memoryTier": "mid_term", "sourceLane": table, "summary": safe_summary, "sourceEvidenceRef": f"relationship_journal:{_row_get(row, 'id', _row_get(row, 'rowid'))}", "rawEvidenceRef": f"relationship_journal:{_row_get(row, 'id', _row_get(row, 'rowid'))}", "visibility": "internal_only"})
            logging.info("population_scan_table_collected table=%s count=%s", table, table_counts[table])

        # relationship_state
        table = "relationship_state"
        cols = table_cols(conn, table, tables)
        if cols is not None:
            if "user_id" not in cols:
                skip_table(table, "missing user_id column")
            else:
                for row in select_rows(conn, table, cols, ["updated_at", "user_id"]):
                    parts = ["stable relationship/activity signal"]
                    for c in ["interaction_count", "affinity_score", "trust_stage", "social_stance", "last_topic"]:
                        if c in cols and _row_get(row, c) not in (None, ""):
                            parts.append(f"{c}={_safe_text(_row_get(row, c), 60)}")
                    uid = _row_get(row, "user_id")
                    add(table, {"subjectName": user_name(uid), "memoryTier": "mid_term", "sourceLane": table, "summary": "; ".join(parts), "sourceEvidenceRef": f"relationship_state:{uid}", "rawEvidenceRef": f"relationship_state:{uid}", "visibility": "internal_only"})
            logging.info("population_scan_table_collected table=%s count=%s", table, table_counts[table])

        # broadcast_memory
        table = "broadcast_memory"
        cols = table_cols(conn, table, tables)
        if cols is not None:
            text_col = _first_existing(cols, ["cleaned_summary", "summary", "content", "text"])
            if not text_col:
                skip_table(table, "missing text column")
            else:
                for row in select_rows(conn, table, cols, ["updated_at", "created_at", "id"]):
                    status = str(_row_get(row, "status", "") or "").lower()
                    public_safe = _row_get(row, "public_safe", None)
                    visibility = _safe_text(_row_get(row, "usage_scope", None), 80) or "review_only"
                    if status in {"blocked", "rejected", "private"} or public_safe in (0, "0", False):
                        filter_decisions["broadcast_memory_safety_filtered"] += 1
                        policy_skips[table] += 1
                        continue
                    add(table, {"subjectName": _row_get(row, text_col), "memoryTier": "dormant_archive", "sourceLane": table, "summary": _row_get(row, text_col), "sourceEvidenceRef": f"broadcast_memory:{_row_get(row, 'id', _row_get(row, 'rowid'))}", "rawEvidenceRef": f"broadcast_memory:{_row_get(row, 'id', _row_get(row, 'rowid'))}", "visibility": visibility})
            logging.info("population_scan_table_collected table=%s count=%s", table, table_counts[table])

        # memory_tiers
        table = "memory_tiers"
        cols = table_cols(conn, table, tables)
        if cols is not None:
            text_col = _first_existing(cols, ["summary", "content", "text"])
            tier_col = _first_existing(cols, ["tier", "memory_tier"])
            if not text_col:
                skip_table(table, "missing summary column")
            else:
                subject_col = _first_existing(cols, ["subject_name", "subject", "display_name", "user_id"])
                for row in select_rows(conn, table, cols, ["updated_at", "created_at", "id"]):
                    add(table, {"subjectName": _row_get(row, subject_col) if subject_col else _row_get(row, text_col), "memoryTier": _row_get(row, tier_col, "mid_term") if tier_col else "mid_term", "sourceLane": table, "summary": _row_get(row, text_col), "sourceEvidenceRef": f"memory_tiers:{_row_get(row, 'id', _row_get(row, 'rowid'))}", "rawEvidenceRef": f"memory_tiers:{_row_get(row, 'id', _row_get(row, 'rowid'))}", "visibility": _row_get(row, "source_channel_policy", None) if "source_channel_policy" in cols else "review_only"})
            logging.info("population_scan_table_collected table=%s count=%s", table, table_counts[table])

        # community_presence
        table = "community_presence"
        cols = table_cols(conn, table, tables)
        if cols is not None:
            subject_col = _first_existing(cols, ["display_name", "subject_name", "subject_key", "user_id"])
            if not subject_col:
                skip_table(table, "missing subject column")
            else:
                for row in select_rows(conn, table, cols, ["last_seen_at", "updated_at", "rowid"]):
                    summary = _row_get(row, "last_topic", None) if "last_topic" in cols else None
                    summary = summary or _row_get(row, "connection_notes", None) if "connection_notes" in cols else summary
                    add(table, {"subjectName": _row_get(row, subject_col), "memoryTier": "short_term", "sourceLane": table, "summary": summary or "community presence signal", "sourceEvidenceRef": f"community_presence:{_row_get(row, 'id', _row_get(row, 'rowid'))}", "rawEvidenceRef": f"community_presence:{_row_get(row, 'id', _row_get(row, 'rowid'))}", "visibility": "review_only"})
            logging.info("population_scan_table_collected table=%s count=%s", table, table_counts[table])

        # entity_evidence_events: schema-tolerant evidence lane with safe subject resolution.
        table = "entity_evidence_events"
        cols = table_cols(conn, table, tables)
        if cols is not None:
            id_col = _first_existing(cols, ["id"])
            author_col = _first_existing(cols, ["author", "author_name", "display_name"])
            matched_user_col = _first_existing(cols, ["matched_user_id", "user_id"])
            kind_col = _first_existing(cols, ["evidence_kind", "kind", "event_type"])
            relevance_col = _first_existing(cols, ["dossier_relevance", "relevance"])
            confidence_col = _first_existing(cols, ["confidence", "confidence_score"])
            public_safe_col = _first_existing(cols, ["public_safe_candidate", "public_safe"])
            raw_ref_col = _first_existing(cols, ["raw_ref_json", "rawRefJson", "raw_ref"])
            channel_policy_col = _first_existing(cols, ["channel_policy", "source_channel_policy", "policy"])
            channel_name_col = _first_existing(cols, ["channel_name", "source_channel_name"])
            summary_cols = [c for c in [kind_col, relevance_col, _first_existing(cols, ["community_signal"]), _first_existing(cols, ["music_signal"]), _first_existing(cols, ["relation_type"])] if c]
            for row in select_rows(conn, table, cols, ["observed_at", "created_at", "id"]):
                raw_ref = _json_mapping(_row_get(row, raw_ref_col)) if raw_ref_col else {}
                subject = ""
                if matched_user_col and _row_get(row, matched_user_col) is not None:
                    subject = user_names.get(str(_row_get(row, matched_user_col)), "")
                if not subject and author_col:
                    subject = _safe_subject_name(_row_get(row, author_col))
                if not subject and raw_ref:
                    subject = _safe_subject_name(_first_mapping_value(raw_ref, ["displayLabel", "display_name", "safeDisplayLabel", "safe_label", "subjectName", "name", "authorName"]))
                if not subject:
                    attempted_counts[table] += 1
                    missing_subject_skips[table] += 1
                    filter_decisions[f"missing_subject_identity:{table}"] += 1
                    continue
                row_id = _row_get(row, id_col) if id_col else _row_get(row, "rowid")
                policy = _safe_text(_row_get(row, channel_policy_col), 80) if channel_policy_col else "review_only"
                evidence_kind = _safe_text(_row_get(row, kind_col), 80) if kind_col else "evidence"
                confidence_value = _safe_text(_row_get(row, confidence_col), 40) if confidence_col else "medium"
                relevance = _safe_text(_row_get(row, relevance_col), 80) if relevance_col else ""
                parts = [f"evidence_kind={evidence_kind}" if evidence_kind else "entity evidence signal"]
                for c in summary_cols:
                    value = _safe_text(_row_get(row, c), 80)
                    if value and value not in parts:
                        parts.append(f"{c}={value}")
                if channel_name_col and _row_get(row, channel_name_col):
                    parts.append(f"channel={_safe_text(_row_get(row, channel_name_col), 80)}")
                public_safe = _row_get(row, public_safe_col) if public_safe_col else None
                memory_tier = "long_term" if str(confidence_value).lower() == "high" or str(relevance).lower() in {"high", "strong"} else "mid_term"
                safety_level = "public_candidate" if public_safe in (1, "1", True, "true", "yes") and policy not in ADMIN_ONLY_POLICIES else "admin_review_only"
                add(table, {
                    "subjectName": subject,
                    "memoryTier": memory_tier,
                    "sourceLane": table,
                    "summary": "; ".join(parts),
                    "sourceEvidenceRef": f"entity_evidence_events:{row_id}",
                    "rawEvidenceRef": f"entity_evidence_events:{row_id}",
                    "visibility": policy or "review_only",
                    "channelPolicy": policy,
                    "evidenceKind": evidence_kind,
                    "confidence": confidence_value,
                    "publicSafetyLevel": safety_level,
                })
            logging.info("population_scan_table_collected table=%s count=%s", table, table_counts[table])

        optional_tables = ["source_file_notes", "source_file_archive", "source_file_archives", "dossier_recommendations", "source_file_refresh_requests"]
        for table in optional_tables:
            cols = table_cols(conn, table, tables)
            if cols is None:
                continue
            text_col = _first_existing(cols, ["summary", "note", "content", "evidence_summary", "reason", "status", "last_error"])
            subject_col = _first_existing(cols, ["subject_name", "subject", "display_name", "subject_key", "target_name", "candidate_name"])
            if not text_col:
                skip_table(table, "missing text/summary column")
                logging.info("population_scan_table_collected table=%s count=%s", table, table_counts[table])
                continue
            for row in select_rows(conn, table, cols, ["updated_at", "created_at", "timestamp", "id"]):
                subject = _row_get(row, subject_col) if subject_col else table.replace("_", " ")
                add(table, {"subjectName": subject, "memoryTier": "mid_term", "sourceLane": table, "summary": _safe_text(_row_get(row, text_col), 220), "sourceEvidenceRef": f"{table}:{_row_get(row, 'id', _row_get(row, 'rowid'))}", "rawEvidenceRef": f"{table}:{_row_get(row, 'id', _row_get(row, 'rowid'))}", "visibility": "review_only"})
            logging.info("population_scan_table_collected table=%s count=%s", table, table_counts[table])
    finally:
        conn.close()

    if diag is not None:
        all_diag_tables = set(diag.setdefault("tables", {}).keys()) | set(attempted_counts) | set(table_counts) | set(quota_skips) | set(policy_skips) | set(schema_skips) | set(missing_subject_skips)
        for table in all_diag_tables:
            info = diag.setdefault("tables", {}).setdefault(table, {})
            info["attempted_collection"] = attempted_counts[table]
            info["records_collected"] = table_counts[table]
            info["skipped_due_to_quota"] = quota_skips[table]
            info["skipped_due_to_policy"] = policy_skips[table]
            info["skipped_due_to_schema"] = max(info.get("skipped_due_to_schema", 0), schema_skips[table])
            info["skipped_due_to_missing_subject_name"] = missing_subject_skips[table]
        diag["filter_decisions"] = dict(filter_decisions)
        diag["final_collected_count"] = len(records[:max_items])
    return records[:max_items]



_NOT_POPULATION_SUBJECT_RE = re.compile(
    r"\b(?:everybody kept saying|normal broadcast schedule|broadcast schedule|show is back on|resume normal broadcast|running joke|episode arc)\b",
    re.I,
)
_SHOW_STATE_RE = re.compile(r"\b(?:show is back on|normal broadcast schedule restored|resume normal broadcast schedule|broadcast schedule)\b", re.I)


def classify_non_population_subject(records: list[dict[str, Any]]) -> str:
    """Classify broadcast/show-status notes that must not enter dossier population."""

    text = " ".join(str(r.get(k) or "") for r in records or [] for k in ("subjectName", "summary", "content", "reason", "evidenceSummary"))
    if not text.strip():
        return ""
    if _SHOW_STATE_RE.search(text):
        return "show_state_note"
    if _NOT_POPULATION_SUBJECT_RE.search(text):
        return "broadcast_memory_note"
    return ""


def run_population_scan(
    db_path: str | None = None,
    *,
    guild_id: int | None = None,
    dry_run: bool = True,
    max_items: int = 25,
    source_lanes: list[str] | None = None,
    min_confidence: str = "low",
    subject_filter: str | None = None,
    memory_records: list[dict[str, Any]] | None = None,
    existing_candidates: list[dict[str, Any]] | None = None,
    public_dossiers: list[dict[str, Any]] | None = None,
    dossier_update_workspaces: list[dict[str, Any]] | None = None,
    confirmed_aliases: dict[str, Any] | None = None,
    read_model_loader: Callable[..., dict[str, Any]] | None = None,
    population_context_fetcher: Callable[..., dict[str, Any]] | None = None,
    sender: Callable[..., dict[str, Any]] | None = None,
    environ: dict[str, str] | None = None,
    diagnostics: bool = False,
    allow_sealed_test: bool = False,
) -> dict[str, Any]:
    """Run an owner/admin population scan with safe dry-run defaults."""

    counts = {"scanned": 0, "generated": 0, "sent_to_site": 0, "dry_run": 0, "skipped": 0, "blocked": 0, "errors": 0}
    logging.info("population_scan_started dry_run=%s max_items=%s min_confidence=%s", dry_run, max_items, min_confidence)
    diagnostic_info: dict[str, Any] = {} if diagnostics else {}
    env_for_context = environ if environ is not None else os.environ
    diagnostic_info.update({
        "population_context_url_configured": is_population_context_url_configured(env_for_context),
        "population_context_auth_configured": is_population_context_auth_configured(env_for_context),
    })
    fetcher = population_context_fetcher or fetch_bnl_population_context
    raw_population_context: dict[str, Any] = {}
    try:
        raw_population_context = fetcher(env_for_context)
    except TypeError:
        raw_population_context = fetcher()
    except Exception as exc:
        logging.warning("population_context_fetch_failed error=%s", _safe_error_message(exc))
        raw_population_context = {}
    population_context = extract_population_context(raw_population_context)
    if raw_population_context and not population_context.get("population_context_loaded"):
        logging.warning("population_context_invalid_shape")
    site_context = extract_population_site_context(None)
    fallback_used = "memory_only"
    resolved_records: dict[str, dict[str, Any]] = {}
    if population_context.get("population_context_loaded"):
        public_dossiers = list(public_dossiers or []) + list(population_context.get("public_dossiers") or [])
        existing_candidates = list(existing_candidates or []) + list(population_context.get("existing_candidates") or [])
        dossier_update_workspaces = list(dossier_update_workspaces or []) + list(population_context.get("dossier_update_workspaces") or [])
        merged_aliases = dict(population_context.get("confirmed_aliases") or {})
        merged_aliases.update(confirmed_aliases or {})
        confirmed_aliases = merged_aliases
        resolved_records = dict(population_context.get("resolved_records") or {})
        fallback_used = "population_context"
        logging.info("population_context_applied public_dossiers=%s source_files=%s candidates=%s dossier_update_workspaces=%s identity_links=%s resolved_records=%s", len(population_context.get("public_dossiers") or []), population_context.get("source_files_count", 0), population_context.get("candidates_count", 0), len(population_context.get("dossier_update_workspaces") or []), population_context.get("identity_links_count", 0), population_context.get("resolved_records_count", 0))
    else:
        site_context = load_population_site_context(read_model_loader) if read_model_loader else extract_population_site_context(None)
        if site_context.get("site_context_loaded"):
            public_dossiers = list(public_dossiers or []) + list(site_context.get("public_dossiers") or [])
            existing_candidates = list(existing_candidates or []) + list(site_context.get("existing_candidates") or [])
            dossier_update_workspaces = list(dossier_update_workspaces or []) + list(site_context.get("dossier_update_workspaces") or [])
            merged_aliases = dict(site_context.get("confirmed_aliases") or {})
            merged_aliases.update(confirmed_aliases or {})
            confirmed_aliases = merged_aliases
            fallback_used = "public_read_model"
        else:
            logging.info("population_site_context_unavailable")
    diagnostic_info.update({
        "population_context_loaded": bool(population_context.get("population_context_loaded")),
        "site_context_loaded": bool(site_context.get("site_context_loaded")),
        "public_dossiers": len(public_dossiers or []),
        "existing_candidates": len(existing_candidates or []),
        "source_files": int(population_context.get("source_files_count") or len(existing_candidates or [])),
        "candidates": int(population_context.get("candidates_count") or 0),
        "dossier_update_workspaces": len(dossier_update_workspaces or []),
        "identity_links": int(population_context.get("identity_links_count") or len(confirmed_aliases or {})),
        "resolved_records": int(population_context.get("resolved_records_count") or len(resolved_records or {})),
        "fallback_used": fallback_used,
        "recommendations_using_population_context": 0,
        "skipped_not_population_subject": 0,
        "skipped_already_represented": 0,
    })
    try:
        records = list(memory_records) if memory_records is not None else collect_shared_memory_population_records(
            db_path or "",
            guild_id,
            max_items=max_items,
            source_lanes=source_lanes,
            subject_filter=subject_filter,
            allow_sealed_test=allow_sealed_test,
            diagnostics=diagnostic_info if diagnostics else None,
        )
    except Exception as exc:
        safe_reason = _safe_error_message(exc)
        logging.exception(
            "population_scan_collect_failed exception_type=%s exception_message=%s db_path=%s guild_id=%s source_lanes=%s subject_filter=%s",
            exc.__class__.__name__,
            safe_reason,
            _safe_text(db_path or "", 220),
            guild_id,
            sorted({_lane(l) for l in source_lanes or []}),
            bool(subject_filter),
        )
        logging.warning("population_scan_completed scanned=0 generated=0 sent_to_site=0 dry_run=0 skipped=0 blocked=0 errors=1")
        return {"ok": False, "dryRun": bool(dry_run), "recommendations": [], "payloads": [], "counts": {**counts, "errors": 1}, "error": "population_scan_collect_failed", "reason": safe_reason, "diagnostics": diagnostic_info if diagnostics else None}

    if diagnostics:
        diagnostic_info.setdefault("final_collected_count", len(records))

    grouped = group_memory_records(records, source_lanes=source_lanes, subject_filter=subject_filter, max_items=max_items)
    counts["scanned"] = sum(len(g) for g in grouped)
    if not records and not grouped:
        warning = "no eligible memory records found"
        logging.info("population_scan_completed scanned=0 generated=0 sent_to_site=0 dry_run=0 skipped=0 blocked=0 errors=0 warning=%s", warning)
        result = {"ok": True, "dryRun": bool(dry_run), "recommendations": [], "payloads": [], "counts": counts, "warning": warning}
        if diagnostics:
            result["diagnostics"] = diagnostic_info
        return result
    recommendations: list[dict[str, Any]] = []
    payloads: list[dict[str, Any]] = []
    send_func = sender or send_dossier_recommendation
    for group in grouped:
        try:
            non_subject_reason = classify_non_population_subject(group)
            if non_subject_reason:
                counts["skipped"] += 1
                diagnostic_info["skipped_not_population_subject"] = diagnostic_info.get("skipped_not_population_subject", 0) + 1
                logging.info("population_recommendation_skipped reason=not_population_subject classification=%s", non_subject_reason)
                continue
            group_subject_key = _record_subject_key(group[0]) if group else ""
            if group_subject_key in resolved_records:
                counts["skipped"] += 1
                diagnostic_info["skipped_already_represented"] = diagnostic_info.get("skipped_already_represented", 0) + 1
                logging.info("population_recommendation_skipped subject_key=%s reason=already_represented", group_subject_key)
                continue
            rec = build_population_recommendation(group, existing_candidates=existing_candidates, public_dossiers=public_dossiers, dossier_update_workspaces=dossier_update_workspaces, confirmed_aliases=confirmed_aliases)
            if not rec:
                counts["skipped"] += 1
                logging.info("population_recommendation_skipped reason=no_recommendation")
                continue
            if population_context.get("population_context_loaded") and rec.get("recommendedAction") in {"attach_to_existing_source_file", "attach_to_existing_dossier_update", "create_dossier_update_workspace"} and (rec.get("matchedExistingCandidateId") or rec.get("matchedPublicDossierId")):
                diagnostic_info["recommendations_using_population_context"] = diagnostic_info.get("recommendations_using_population_context", 0) + 1
                logging.info("population_context_match_found subject_key=%s lane=%s action=%s", rec.get("normalizedSubjectKey"), rec.get("recommendedLane"), rec.get("recommendedAction"))
            if not _passes_min_confidence(str(rec.get("confidence") or "low"), _confidence_allowed(min_confidence)):
                counts["skipped"] += 1
                logging.info("population_recommendation_skipped subject_key=%s reason=min_confidence", rec.get("normalizedSubjectKey"))
                continue
            recommendations.append(rec)
            counts["generated"] += 1
            logging.info("population_recommendation_generated subject_key=%s lane=%s action=%s confidence=%s", rec.get("normalizedSubjectKey"), rec.get("recommendedLane"), rec.get("recommendedAction"), rec.get("confidence"))
            if rec.get("confidence") == "blocked":
                counts["blocked"] += 1
                logging.warning("population_recommendation_blocked subject_key=%s reason=%s", rec.get("normalizedSubjectKey"), rec.get("confidenceReason"))
            payload = build_population_recommendation_payload(rec)
            payloads.append(payload)
            if dry_run or rec.get("confidence") == "blocked":
                counts["dry_run"] += 1
                logging.info("population_recommendation_dry_run subject_key=%s lane=%s action=%s", rec.get("normalizedSubjectKey"), rec.get("recommendedLane"), rec.get("recommendedAction"))
                continue
            result = send_func(payload, environ=environ) if environ is not None else send_func(payload)
            if result.get("ok"):
                counts["sent_to_site"] += 1
                logging.info("population_recommendation_sent_to_site subject_key=%s recommendation_id=%s", rec.get("normalizedSubjectKey"), result.get("recommendationId") or "none")
            else:
                counts["errors"] += 1
                logging.warning("population_recommendation_skipped subject_key=%s reason=send_failed", rec.get("normalizedSubjectKey"))
        except Exception:
            counts["errors"] += 1
            logging.warning("population_recommendation_skipped reason=unexpected_error")
    logging.info(
        "population_scan_completed scanned=%s generated=%s sent_to_site=%s dry_run=%s skipped=%s blocked=%s errors=%s",
        counts["scanned"], counts["generated"], counts["sent_to_site"], counts["dry_run"], counts["skipped"], counts["blocked"], counts["errors"],
    )
    result = {"ok": counts["errors"] == 0, "dryRun": bool(dry_run), "recommendations": recommendations, "payloads": payloads, "counts": counts}
    if diagnostics:
        diagnostic_info["final_collected_count"] = len(records)
        result["diagnostics"] = diagnostic_info
    return result


def parse_population_scan_command(content: str) -> tuple[bool, dict[str, Any] | None, str]:
    """Parse `!bnl population scan [--send] [max=25] [lanes=a,b] [min=medium] [subject=name]`."""

    text = (content or "").strip()
    if not re.match(r"^!bnl\s+(?:population|source_population)\s+scan(?:\s+now)?\b", text, flags=re.I):
        return False, None, "not_population_scan_command"
    options: dict[str, Any] = {"dry_run": True, "max_items": 25, "source_lanes": None, "min_confidence": "low", "subject_filter": None, "diagnostics": False, "allow_sealed_test": False}
    tail = re.sub(r"^!bnl\s+(?:population|source_population)\s+scan(?:\s+now)?\b", "", text, flags=re.I).strip()
    if not tail:
        return True, options, ""
    for part in re.split(r"\s+", tail):
        if not part:
            continue
        low = part.lower()
        if low in {"diagnostics", "debug", "--diagnostics", "--debug"}:
            options["diagnostics"] = True
        elif low in {"allow_sealed_test=true", "sealed_test=true", "--allow-sealed-test"}:
            options["allow_sealed_test"] = True
        elif low in {"--send", "send", "dry_run=false", "dry=false"}:
            options["dry_run"] = False
        elif low in {"--dry-run", "dry_run=true", "dry=true"}:
            options["dry_run"] = True
        elif low.startswith("max=") or low.startswith("max_items="):
            try:
                options["max_items"] = max(1, min(200, int(part.split("=", 1)[1])))
            except Exception:
                return True, None, "max_items must be a number."
        elif low.startswith("lanes=") or low.startswith("source_lanes="):
            options["source_lanes"] = [_lane(x) for x in part.split("=", 1)[1].split(",") if x.strip()]
        elif low.startswith("min=") or low.startswith("min_confidence="):
            conf = part.split("=", 1)[1].lower()
            if conf not in {"low", "medium", "high", "blocked"}:
                return True, None, "min_confidence must be low, medium, high, or blocked."
            options["min_confidence"] = conf
        elif low.startswith("subject="):
            options["subject_filter"] = part.split("=", 1)[1].strip()
    return True, options, ""


def _format_population_scan_diagnostics(diagnostics: dict[str, Any]) -> list[str]:
    lines = ["Diagnostics (metadata only):"]
    lines.append(f"- db_path exists: {bool((diagnostics or {}).get('db_path_exists'))}")
    lines.append(f"- population_context_loaded: {bool((diagnostics or {}).get('population_context_loaded'))} url_configured={bool((diagnostics or {}).get('population_context_url_configured'))} auth_configured={bool((diagnostics or {}).get('population_context_auth_configured'))} fallback_used={(diagnostics or {}).get('fallback_used', 'memory_only')}")
    lines.append(f"- context_counts: public_dossiers={(diagnostics or {}).get('public_dossiers', 0)} source_files={(diagnostics or {}).get('source_files', 0)} candidates={(diagnostics or {}).get('candidates', (diagnostics or {}).get('existing_candidates', 0))} dossier_update_workspaces={(diagnostics or {}).get('dossier_update_workspaces', 0)} identity_links={(diagnostics or {}).get('identity_links', 0)} resolved_records={(diagnostics or {}).get('resolved_records', 0)}")
    lines.append(f"- recommendation_context_usage: recommendations_using_population_context={(diagnostics or {}).get('recommendations_using_population_context', 0)} skipped_not_population_subject={(diagnostics or {}).get('skipped_not_population_subject', 0)} skipped_already_represented={(diagnostics or {}).get('skipped_already_represented', 0)}")
    lines.append(f"- site_context_loaded: {bool((diagnostics or {}).get('site_context_loaded'))} public_dossiers={(diagnostics or {}).get('public_dossiers', 0)} candidates={(diagnostics or {}).get('existing_candidates', 0)} source_files={(diagnostics or {}).get('source_files', 0)}")
    tables = (diagnostics or {}).get("tables") or {}
    for table in sorted(tables):
        info = tables.get(table) or {}
        cols = ",".join(info.get("columns") or []) or "none"
        lines.append(f"- {table}: exists={int(bool(info.get('exists')))} rows={info.get('row_count', 0)} attempted={info.get('attempted_collection', 0)} collected={info.get('records_collected', 0)} quota_skipped={info.get('skipped_due_to_quota', 0)} policy_skipped={info.get('skipped_due_to_policy', 0)} schema_skipped={info.get('skipped_due_to_schema', 0)} missing_subject={info.get('skipped_due_to_missing_subject_name', 0)} columns={cols}")
        if info.get("skipped_reason"):
            lines.append(f"  skipped={_safe_text(info.get('skipped_reason'), 120)}")
    decisions = (diagnostics or {}).get("filter_decisions") or {}
    if decisions:
        lines.append("- filter decisions: " + ", ".join(f"{k}={v}" for k, v in sorted(decisions.items())))
    lines.append(f"- final collected count: {(diagnostics or {}).get('final_collected_count', 0)}")
    return lines


def format_population_scan_response(summary: dict[str, Any]) -> str:
    counts = summary.get("counts") or {}
    count_line = "Counts: " + ", ".join(f"{key}={counts.get(key, 0)}" for key in ("scanned", "generated", "sent_to_site", "dry_run", "skipped", "blocked", "errors"))
    if not summary.get("ok", True) or counts.get("errors", 0) > 0:
        lines = [
            "Population scan failed during memory collection.",
            f"Reason: {_safe_text(summary.get('reason') or summary.get('error') or 'unknown error', 220)}",
            "No site records were created and no public pages were edited.",
            count_line,
        ]
    else:
        lines = [f"Population scan {'dry-run' if summary.get('dryRun', True) else 'send'} complete."]
        if summary.get("warning") == "no eligible memory records found" or counts.get("scanned", 0) == 0:
            lines.append("No eligible memory records found for the selected filters.")
        lines.append(count_line)
        diag = summary.get("diagnostics") or {}
        if diag:
            lines.append(f"Context: population_context_loaded={bool(diag.get('population_context_loaded'))}, fallback_used={diag.get('fallback_used', 'memory_only')}, public_dossiers={diag.get('public_dossiers', 0)}")
            decisions = diag.get("filter_decisions") or {}
            missing = sum(v for k, v in decisions.items() if str(k).startswith("missing_subject_identity"))
            policy = sum(v for k, v in decisions.items() if "policy_filtered" in str(k) or "safety_filtered" in str(k))
            if missing or policy:
                lines.append(f"Skipped: missing_subject_identity={missing}, source_policy={policy}")
        for rec in (summary.get("recommendations") or [])[:8]:
            lines.append(f"- {rec.get('subjectName')} → {rec.get('recommendedLane')} / {rec.get('recommendedAction')} ({rec.get('confidence')})")
        if summary.get("dryRun", True):
            lines.append("Dry-run only: no public pages were published and no public dossier text was edited.")
    if summary.get("diagnostics"):
        lines.extend(_format_population_scan_diagnostics(summary.get("diagnostics") or {}))
    return "\n".join(lines)[:1900]
