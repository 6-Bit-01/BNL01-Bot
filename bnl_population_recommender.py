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
import re
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Any, Callable

from bnl_admin_summaries import build_admin_summary, build_dossier_update_summary
from bnl_dossier_recommendations import build_dossier_recommendation_payload, send_dossier_recommendation

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
}
PRIVATE_LANES = {"admin_private", "private_admin", "sealed_test", "diagnostic_test", "test_memory"}
ADMIN_ONLY_POLICIES = {"admin_private", "private", "sealed_test", "internal_only", "admin_only", "protected_system"}
_SAFE_KEY_RE = re.compile(r"[^a-z0-9]+")
_SAFE_LANE_RE = re.compile(r"[^a-z0-9_-]+")
_RAW_BLOB_RE = re.compile(r"\b(?:relationship_journal|local_profile_observed|rawRefJson|sourceRowId|source_row_id|memory_tiers|entity_evidence_events|user_profiles|broadcast_memory|rd_context)\b", re.I)
_UNKNOWN_CHAIN_RE = re.compile(r"\bunknown\s*(?:->|→)\s*unknown\b", re.I)
_ALIAS_RE = re.compile(r"\b(?:alias(?:es)?|aka|known as|identity link)\s*[:=]\s*[^.;,|]+", re.I)
_DIAGNOSTIC_RE = re.compile(r"\b(?:diagnostic|test junk|fixture|synthetic|sandbox|heartbeat|ping test|dummy subject|lorem ipsum)\b", re.I)


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
    return _safe_text(record.get("subjectName") or record.get("subject") or record.get("name") or record.get("displayName") or "Unknown subject", 140) or "Unknown subject"


def _record_subject_key(record: dict[str, Any]) -> str:
    return normalize_subject_key(record.get("normalizedSubjectKey") or record.get("subjectKey") or _extract_name(record))


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
    text = " ".join(str(record.get(k) or "") for k in ("subjectName", "summary", "content", "reason", "evidenceSummary", "sourceLane"))
    return lane in {"diagnostic_test", "sealed_test", "test_memory"} or bool(_DIAGNOSTIC_RE.search(text))


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
    elif matched_dossier:
        recommended_lane = "existing_dossier_update" if matched_workspace else "public_dossier_update_signal"
        recommended_action = "attach_to_existing_dossier_update" if matched_workspace else "create_dossier_update_workspace"
        confidence = "high"
        confidence_reason = "Exact public dossier ID/name/subject key matched an existing public dossier."
        duplicate_risk = "low"
        next_step = "Route as a Dossier Update workspace signal; keep public copy unchanged until reviewed."
    elif matched_candidate:
        recommended_lane = "identity_review" if alias else "active_source_file"
        recommended_action = "suggest_identity_link" if alias else "attach_to_existing_source_file"
        confidence = "high"
        confidence_reason = "Exact Source File candidate ID/subject key or confirmed alias matched an existing Source File."
        duplicate_risk = "low"
        identity_risk = "medium" if alias else "low"
        next_step = "Attach evidence to the existing Source File; do not create a duplicate candidate."
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

    summary_record = {
        "subjectName": subject_name,
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
        "subjectName": subject_name,
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


def collect_shared_memory_population_records(db_path: str, guild_id: int | None, *, max_items: int = 200, source_lanes: list[str] | None = None, subject_filter: str | None = None) -> list[dict[str, Any]]:
    """Collect approved existing BNL memory/evidence rows for population scan.

    This uses already-existing local evidence tables when present. It creates no
    new memory database and stores nothing.
    """

    lane_filter = {_lane(l) for l in source_lanes or []}
    subject_filter_key = normalize_subject_key(subject_filter) if subject_filter else ""
    records: list[dict[str, Any]] = []

    def add(record: dict[str, Any]) -> None:
        if len(records) >= max_items:
            return
        lane = _record_lane(record)
        if lane_filter and lane not in lane_filter:
            return
        if subject_filter_key and subject_filter_key not in _record_subject_key(record):
            return
        records.append(record)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        if "memory_tiers" in tables:
            cols = {row[1] for row in conn.execute("PRAGMA table_info(memory_tiers)").fetchall()}
            if {"tier", "summary"}.issubset(cols):
                subject_col = "subject_name" if "subject_name" in cols else ("subject" if "subject" in cols else "summary")
                updated_col = "updated_at" if "updated_at" in cols else "rowid"
                gid_clause = "WHERE guild_id=?" if "guild_id" in cols and guild_id is not None else ""
                params = (guild_id,) if gid_clause else ()
                for row in conn.execute(f"SELECT rowid,* FROM memory_tiers {gid_clause} ORDER BY {updated_col} DESC LIMIT ?", (*params, max_items)).fetchall():
                    add({"subjectName": row[subject_col], "memoryTier": row["tier"], "sourceLane": "memory_tiers", "summary": row["summary"], "sourceEvidenceRef": f"memory_tiers:{row['rowid']}", "rawEvidenceRef": f"memory_tiers:{row['rowid']}", "visibility": row["source_channel_policy"] if "source_channel_policy" in cols else "admin_only"})
        if "broadcast_memory" in tables:
            cols = {row[1] for row in conn.execute("PRAGMA table_info(broadcast_memory)").fetchall()}
            text_col = "cleaned_summary" if "cleaned_summary" in cols else ("summary" if "summary" in cols else "content")
            if text_col in cols:
                gid_clause = "WHERE guild_id=?" if "guild_id" in cols and guild_id is not None else ""
                params = (guild_id,) if gid_clause else ()
                for row in conn.execute(f"SELECT rowid,* FROM broadcast_memory {gid_clause} ORDER BY rowid DESC LIMIT ?", (*params, max_items)).fetchall():
                    add({"subjectName": row[text_col], "memoryTier": "dormant_archive", "sourceLane": "broadcast_memory", "summary": row[text_col], "sourceEvidenceRef": f"broadcast_memory:{row['rowid']}", "rawEvidenceRef": f"broadcast_memory:{row['rowid']}", "visibility": "review_only"})
        if "community_presence" in tables:
            cols = {row[1] for row in conn.execute("PRAGMA table_info(community_presence)").fetchall()}
            subject_col = "display_name" if "display_name" in cols else ("subject_name" if "subject_name" in cols else "user_id")
            gid_clause = "WHERE guild_id=?" if "guild_id" in cols and guild_id is not None else ""
            params = (guild_id,) if gid_clause else ()
            for row in conn.execute(f"SELECT rowid,* FROM community_presence {gid_clause} ORDER BY rowid DESC LIMIT ?", (*params, max_items)).fetchall():
                summary = row["last_topic"] if "last_topic" in cols else "community presence signal"
                add({"subjectName": row[subject_col], "memoryTier": "short_term", "sourceLane": "community_presence", "summary": summary, "sourceEvidenceRef": f"community_presence:{row['rowid']}", "rawEvidenceRef": f"community_presence:{row['rowid']}", "visibility": "review_only"})
    finally:
        conn.close()
    return records[:max_items]


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
    sender: Callable[..., dict[str, Any]] | None = None,
    environ: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Run an owner/admin population scan with safe dry-run defaults."""

    counts = {"scanned": 0, "generated": 0, "sent_to_site": 0, "dry_run": 0, "skipped": 0, "blocked": 0, "errors": 0}
    logging.info("population_scan_started dry_run=%s max_items=%s min_confidence=%s", dry_run, max_items, min_confidence)
    try:
        records = list(memory_records) if memory_records is not None else collect_shared_memory_population_records(db_path or "", guild_id, max_items=max_items, source_lanes=source_lanes, subject_filter=subject_filter)
    except Exception:
        logging.warning("population_scan_completed scanned=0 generated=0 sent_to_site=0 dry_run=0 skipped=0 blocked=0 errors=1")
        return {"ok": False, "dryRun": bool(dry_run), "recommendations": [], "payloads": [], "counts": {**counts, "errors": 1}, "error": "population_scan_collect_failed"}

    grouped = group_memory_records(records, source_lanes=source_lanes, subject_filter=subject_filter, max_items=max_items)
    counts["scanned"] = sum(len(g) for g in grouped)
    recommendations: list[dict[str, Any]] = []
    payloads: list[dict[str, Any]] = []
    send_func = sender or send_dossier_recommendation
    for group in grouped:
        try:
            rec = build_population_recommendation(group, existing_candidates=existing_candidates, public_dossiers=public_dossiers, dossier_update_workspaces=dossier_update_workspaces, confirmed_aliases=confirmed_aliases)
            if not rec:
                counts["skipped"] += 1
                logging.info("population_recommendation_skipped reason=no_recommendation")
                continue
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
    return {"ok": counts["errors"] == 0, "dryRun": bool(dry_run), "recommendations": recommendations, "payloads": payloads, "counts": counts}


def parse_population_scan_command(content: str) -> tuple[bool, dict[str, Any] | None, str]:
    """Parse `!bnl population scan [--send] [max=25] [lanes=a,b] [min=medium] [subject=name]`."""

    text = (content or "").strip()
    if not re.match(r"^!bnl\s+(?:population|source_population)\s+scan(?:\s+now)?\b", text, flags=re.I):
        return False, None, "not_population_scan_command"
    options: dict[str, Any] = {"dry_run": True, "max_items": 25, "source_lanes": None, "min_confidence": "low", "subject_filter": None}
    tail = re.sub(r"^!bnl\s+(?:population|source_population)\s+scan(?:\s+now)?\b", "", text, flags=re.I).strip()
    if not tail:
        return True, options, ""
    for part in re.split(r"\s+", tail):
        if not part:
            continue
        low = part.lower()
        if low in {"--send", "send", "dry_run=false", "dry=false"}:
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


def format_population_scan_response(summary: dict[str, Any]) -> str:
    counts = summary.get("counts") or {}
    lines = [
        f"Population scan {'dry-run' if summary.get('dryRun', True) else 'send'} complete.",
        "Counts: " + ", ".join(f"{key}={counts.get(key, 0)}" for key in ("scanned", "generated", "sent_to_site", "dry_run", "skipped", "blocked", "errors")),
    ]
    for rec in (summary.get("recommendations") or [])[:8]:
        lines.append(f"- {rec.get('subjectName')} → {rec.get('recommendedLane')} / {rec.get('recommendedAction')} ({rec.get('confidence')})")
    if summary.get("dryRun", True):
        lines.append("Dry-run only: no public pages were published and no public dossier text was edited.")
    return "\n".join(lines)[:1900]
