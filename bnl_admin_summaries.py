"""Structured admin-only summary builders for BNL internal review records.

The helpers in this module sit on top of existing Source File, Dossier Update,
and recommendation evidence. They do not replace raw evidence storage and they
never create public dossier copy or trigger publishing.
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from typing import Any

ADMIN_SUMMARY_VERSION = "admin_summary_v1"
DOSSIER_UPDATE_SUMMARY_VERSION = "dossier_update_summary_v1"
RECOMMENDATION_CLUSTER_SUMMARY_VERSION = "recommendation_cluster_summary_v1"
GENERATED_BY = "BNL"
INSUFFICIENT_EVIDENCE_SUMMARY = "BNL summary needed: insufficient evidence."
THIN_EVIDENCE_NOTE = "Known evidence is thin; review before routing."

ADMIN_SUMMARY_SCHEMA_FIELDS = (
    "summaryVersion",
    "generatedAt",
    "generatedBy",
    "subjectName",
    "subjectKey",
    "subjectType",
    "confidence",
    "operatorSummary",
    "whyTheyMatter",
    "knownContext",
    "notableEvidence",
    "relationshipToBARCODE",
    "currentStatus",
    "missingInfo",
    "recommendedNextAction",
    "routingRecommendation",
    "publicSafetyNotes",
    "doNotSay",
    "rawEvidenceRefs",
    "sourceLanes",
    "stale",
    "inputHash",
)

DOSSIER_UPDATE_SUMMARY_FIELDS = (
    "summaryVersion",
    "generatedAt",
    "generatedBy",
    "subjectName",
    "subjectKey",
    "updateSummary",
    "whatChanged",
    "whatAlreadyExists",
    "whatNeedsReview",
    "safePublicUpdateCandidate",
    "recommendedAdminNextAction",
    "rawEvidenceRefs",
    "sourceLanes",
    "stale",
    "inputHash",
)

RECOMMENDATION_CLUSTER_SUMMARY_FIELDS = (
    "summaryVersion",
    "generatedAt",
    "generatedBy",
    "subjectName",
    "subjectKey",
    "clusterSummary",
    "whyClusterExists",
    "appearsToBelongTo",
    "recommendedDisposition",
    "confidence",
    "missingInfo",
    "rawEvidenceRefs",
    "sourceLanes",
    "stale",
    "inputHash",
)

_RAW_KEY_RE = re.compile(r"\b(?:relationship_journal|local_profile_observed|rawRefJson|sourceRowId|source_row_id|sourceLabel|source_label|memory_tiers|entity_evidence_events|user_profiles|user_memory_facts|broadcast_memory|rd_context)\b", re.I)
_UNKNOWN_CHAIN_RE = re.compile(r"\bunknown\s*(?:->|→)\s*unknown\b", re.I)
_LONG_ID_RE = re.compile(r"\b\d{15,22}\b")
_DISCORD_MENTION_RE = re.compile(r"<@!?\d+>|<@&\d+>|<#\d+>")
_URL_RE = re.compile(r"https?://\S+", re.I)
_ALIAS_VALUE_RE = re.compile(r"\b(?:alias(?:es)?|aka|known as|identity link)\s*[:=]\s*[^.;,|]+", re.I)
_SAFE_LANE_RE = re.compile(r"[^a-z0-9_-]+")


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, (int, float, bool)):
        return str(value)
    try:
        return json.dumps(value, sort_keys=True, default=str)
    except Exception:
        return str(value)


def _clean_public_admin_text(value: Any, limit: int = 320) -> str:
    """Return concise admin prose without raw storage keys or alias values."""

    text = re.sub(r"\s+", " ", _text(value)).strip()
    text = _DISCORD_MENTION_RE.sub("[redacted-mention]", text)
    text = _LONG_ID_RE.sub("[redacted-id]", text)
    text = _URL_RE.sub("[redacted-url]", text)
    text = _UNKNOWN_CHAIN_RE.sub("", text)
    text = _RAW_KEY_RE.sub("internal evidence", text)
    text = _ALIAS_VALUE_RE.sub("alias/identity signal kept internal", text)
    text = text.replace("[object Object]", "").strip(" -|;,")
    if not text:
        return ""
    return text[:limit].rstrip()


def _list(value: Any, *, limit: int = 6, item_limit: int = 220) -> list[str]:
    if value is None:
        raw: list[Any] = []
    elif isinstance(value, dict):
        raw = []
        for key, item in value.items():
            if item in (None, "", [], {}):
                continue
            raw.append(f"{key}: {_text(item)}")
    elif isinstance(value, (list, tuple, set)):
        raw = list(value)
    else:
        raw = [value]
    cleaned: list[str] = []
    seen: set[str] = set()
    for item in raw:
        text = _clean_public_admin_text(item, item_limit)
        if not text or text.lower() in seen:
            continue
        seen.add(text.lower())
        cleaned.append(text)
        if len(cleaned) >= limit:
            break
    return cleaned


def _subject_key(name: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "-", str(name or "").strip().lower()).strip("-")
    return normalized or "unknown-subject"


def _source_lanes(record: dict[str, Any]) -> list[str]:
    raw = record.get("sourceLanes") or record.get("sourceTypes") or []
    if isinstance(raw, str):
        parts = re.split(r"[,\s]+", raw)
    elif isinstance(raw, (list, tuple, set)):
        parts = list(raw)
    else:
        parts = []
    lanes: list[str] = []
    seen: set[str] = set()
    for part in parts:
        lane = _SAFE_LANE_RE.sub("_", str(part or "").strip().lower()).strip("_")
        if lane and lane not in seen:
            lanes.append(lane[:48])
            seen.add(lane)
    return lanes or ["unknown"]


def _raw_evidence_refs(record: dict[str, Any]) -> list[str]:
    refs: list[str] = []

    def add(value: Any) -> None:
        if len(refs) >= 24:
            return
        text = _clean_public_admin_text(value, 160)
        if text and text not in refs:
            refs.append(text)

    for key in ("rawEvidenceRefs", "sourceRefs", "sourceCoverage", "representativeEvidence", "bestEvidenceToReview", "evidenceDetails"):
        value = record.get(key)
        if isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    add(item.get("sourceRef") or item.get("source") or item.get("lane") or item.get("summary") or item)
                else:
                    add(item)
        elif value:
            add(value)
    raw_provenance = record.get("rawProvenance")
    if isinstance(raw_provenance, dict):
        for key, value in raw_provenance.items():
            if isinstance(value, (list, tuple, set)):
                add(f"{key}: {len(value)} evidence item(s)")
            else:
                add(f"{key}: present")
    if not refs:
        for lane in _source_lanes(record):
            add(f"lane:{lane}")
    return refs


def _input_hash(record: dict[str, Any], *, summary_kind: str) -> str:
    material = {
        "summaryKind": summary_kind,
        "subjectName": record.get("subjectName") or record.get("subject") or record.get("name"),
        "subjectKey": record.get("subjectKey"),
        "sourceLanes": _source_lanes(record),
        "sourceCounts": record.get("sourceCounts"),
        "sourceTypes": record.get("sourceTypes"),
        "qualityScore": record.get("qualityScore"),
        "qualityStatus": record.get("qualityStatus"),
        "knownContext": record.get("knownContext"),
        "usefulEvidence": record.get("usefulEvidence") or record.get("evidenceDetails"),
        "representativeEvidence": record.get("representativeEvidence"),
        "bestEvidenceToReview": record.get("bestEvidenceToReview"),
        "relationshipSignals": record.get("relationshipSignals"),
        "missingInfo": record.get("missingInfo"),
        "publicSafetyNotes": record.get("publicSafetyNotes"),
        "doNotSay": record.get("doNotSay"),
        "reason": record.get("reason"),
        "evidenceSummary": record.get("evidenceSummary"),
        "rawEvidenceRefs": record.get("rawEvidenceRefs"),
    }
    encoded = json.dumps(material, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _evidence_count(record: dict[str, Any]) -> int:
    count = 0
    source_counts = record.get("sourceCounts")
    if isinstance(source_counts, dict):
        count += sum(int(v or 0) for v in source_counts.values() if str(v or "").isdigit() or isinstance(v, int))
    for key in ("knownContext", "usefulEvidence", "evidenceDetails", "representativeEvidence", "bestEvidenceToReview", "conversationHighlights", "reason", "evidenceSummary"):
        value = record.get(key)
        if isinstance(value, (list, tuple, set, dict)) and value:
            count += len(value)
        elif str(value or "").strip():
            count += 1
    return count


def _confidence(record: dict[str, Any], evidence_count: int) -> str:
    explicit = str(record.get("confidence") or "").strip().lower()
    if explicit in {"low", "medium", "high"}:
        return explicit
    status = str(record.get("qualityStatus") or "").lower()
    if evidence_count < 2 or status in {"too_thin", "forced_low_confidence", "suppressed_too_thin"}:
        return "low"
    score = record.get("qualityScore")
    try:
        if int(score) >= 80:
            return "high"
        if int(score) >= 45:
            return "medium"
    except Exception:
        pass
    return "medium" if evidence_count >= 3 else "low"


def _first_nonempty(record: dict[str, Any], keys: tuple[str, ...], default: str = "") -> str:
    for key in keys:
        value = record.get(key)
        if isinstance(value, list) and value:
            text = _clean_public_admin_text(value[0])
        else:
            text = _clean_public_admin_text(value)
        if text:
            return text
    return default


def build_admin_summary(record: dict[str, Any] | None, *, generated_at: str | None = None, stale: bool = False) -> dict[str, Any]:
    """Build the admin-only Source File summary object from existing evidence."""

    source = dict(record or {})
    subject = _clean_public_admin_text(source.get("subjectName") or source.get("subject") or source.get("name") or "Unknown subject", 140) or "Unknown subject"
    subject_key = _clean_public_admin_text(source.get("subjectKey"), 120) or _subject_key(subject)
    lanes = _source_lanes(source)
    refs = _raw_evidence_refs(source)
    evidence_count = _evidence_count(source)
    confidence = _confidence(source, evidence_count)
    input_hash = _input_hash(source, summary_kind="admin")
    generated = generated_at or utc_now()

    if evidence_count <= 0:
        operator = INSUFFICIENT_EVIDENCE_SUMMARY
        why = THIN_EVIDENCE_NOTE
        known = ["No usable stored evidence was available for this admin summary."]
        notable = []
        missing = ["Add or review Source File notes, recommendation evidence, source lanes, or conversation evidence before routing."]
        routing = "remain_review_only"
        next_action = "Collect or attach evidence, then regenerate the admin summary."
    else:
        known = _list(source.get("knownContext") or source.get("conversationHighlights") or source.get("observedChannels"), limit=5) or ["Some stored evidence exists, but BNL could not extract a clean fact list. Review raw evidence before routing."]
        notable = _list(source.get("usefulEvidence") or source.get("evidenceDetails") or source.get("representativeEvidence") or source.get("bestEvidenceToReview") or source.get("evidenceSummary"), limit=6)
        why = _first_nonempty(source, ("whyTheyMatter", "reason", "activityFrequencySummary", "recentActivitySummary"), THIN_EVIDENCE_NOTE)
        if confidence == "low":
            why = f"{why} Evidence is low-confidence and needs admin review."
        uncertainty = " Facts above are evidence-grounded; relationship, identity, alias, and public biography claims remain unconfirmed unless separately verified."
        operator = _clean_public_admin_text(f"BNL found review-only evidence for {subject} across {', '.join(lanes)}. {why}.{uncertainty}", 620)
        missing = _list(source.get("missingInfo"), limit=5) or ["Confirm public-safe identity, role, source ownership, relationships, and routing before drafting public copy."]
        routing = _clean_public_admin_text(source.get("routingRecommendation") or source.get("suggestedAction") or source.get("recommendedAction") or "Review for Source File or Dossier Update routing; do not publish automatically.", 260)
        next_action = _clean_public_admin_text(source.get("recommendedNextAction") or source.get("suggestedAction") or source.get("recommendedAction") or "Admin should review evidence refs, confirm routing, and regenerate after adding missing evidence.", 260)

    relationship = _list(source.get("relationshipSignals"), limit=4) or ["No public relationship claim is confirmed by this summary; treat relationship signals as internal review evidence only."]
    public_safety = _list(source.get("publicSafetyNotes"), limit=5) or ["Admin-only summary. Do not turn this into public dossier copy without review."]
    do_not_say = _list(source.get("doNotSay"), limit=6) or [
        "Do not publish raw evidence, private context, internal aliases, relationship guesses, or identity links as public facts.",
        "Do not claim queue, payment, submission, role, relationship, or biography details without separate confirmation.",
    ]

    return {
        "summaryVersion": ADMIN_SUMMARY_VERSION,
        "generatedAt": generated,
        "generatedBy": GENERATED_BY,
        "subjectName": subject,
        "subjectKey": subject_key,
        "subjectType": _clean_public_admin_text(source.get("subjectType") or source.get("recommendedKind") or source.get("recommendedCategory") or "source_file_subject", 80),
        "confidence": confidence,
        "operatorSummary": operator,
        "whyTheyMatter": why,
        "knownContext": known,
        "notableEvidence": notable,
        "relationshipToBARCODE": relationship,
        "currentStatus": _clean_public_admin_text(source.get("currentStatus") or source.get("qualityStatus") or source.get("matchKind") or "review_only", 120),
        "missingInfo": missing,
        "recommendedNextAction": next_action,
        "routingRecommendation": routing,
        "publicSafetyNotes": public_safety,
        "doNotSay": do_not_say,
        "rawEvidenceRefs": refs,
        "sourceLanes": lanes,
        "stale": bool(stale),
        "inputHash": input_hash,
    }


def build_dossier_update_summary(record: dict[str, Any] | None, *, generated_at: str | None = None, stale: bool = False) -> dict[str, Any]:
    source = dict(record or {})
    base = build_admin_summary(source, generated_at=generated_at, stale=stale)
    evidence_count = _evidence_count(source)
    if evidence_count <= 0:
        update_summary = INSUFFICIENT_EVIDENCE_SUMMARY
        what_changed = []
        safe = "no"
    else:
        update_summary = _clean_public_admin_text(f"BNL found internal review evidence for a possible Dossier Update to {base['subjectName']}. This is not public copy and needs admin review.", 420)
        what_changed = _list(source.get("whatChanged") or source.get("usefulEvidence") or source.get("evidenceDetails") or source.get("evidenceSummary"), limit=5)
        safe = "maybe" if base["confidence"] != "low" else "no"
    return {
        "summaryVersion": DOSSIER_UPDATE_SUMMARY_VERSION,
        "generatedAt": base["generatedAt"],
        "generatedBy": GENERATED_BY,
        "subjectName": base["subjectName"],
        "subjectKey": base["subjectKey"],
        "updateSummary": update_summary,
        "whatChanged": what_changed,
        "whatAlreadyExists": _list(source.get("whatAlreadyExists") or source.get("knownContext"), limit=5),
        "whatNeedsReview": base["missingInfo"],
        "safePublicUpdateCandidate": _clean_public_admin_text(source.get("safePublicUpdateCandidate") or safe, 20).lower() if evidence_count > 0 else "no",
        "recommendedAdminNextAction": base["recommendedNextAction"],
        "rawEvidenceRefs": base["rawEvidenceRefs"],
        "sourceLanes": base["sourceLanes"],
        "stale": bool(stale),
        "inputHash": _input_hash(source, summary_kind="dossier_update"),
    }


def build_recommendation_cluster_summary(cluster: dict[str, Any] | None, *, generated_at: str | None = None, stale: bool = False) -> dict[str, Any]:
    source = dict(cluster or {})
    base = build_admin_summary(source, generated_at=generated_at, stale=stale)
    evidence_count = _evidence_count(source)
    if evidence_count <= 0:
        why = INSUFFICIENT_EVIDENCE_SUMMARY
        disposition = "remain_review_only"
    else:
        why = _clean_public_admin_text(source.get("whyClusterExists") or source.get("reason") or f"Evidence items in {', '.join(base['sourceLanes'])} appear to point at the same review subject.", 360)
        suggested = str(source.get("suggestedAction") or source.get("recommendedDisposition") or "").lower()
        if "update" in suggested or source.get("targetDossierId"):
            disposition = "dossier_update_review"
        elif "source file" in suggested or source.get("targetCandidateId"):
            disposition = "source_file_review"
        else:
            disposition = "remain_review_only" if base["confidence"] == "low" else "source_file_review"
    return {
        "summaryVersion": RECOMMENDATION_CLUSTER_SUMMARY_VERSION,
        "generatedAt": base["generatedAt"],
        "generatedBy": GENERATED_BY,
        "subjectName": base["subjectName"],
        "subjectKey": base["subjectKey"],
        "clusterSummary": _clean_public_admin_text(f"Recommendation cluster for {base['subjectName']}: {why}", 460) if evidence_count > 0 else INSUFFICIENT_EVIDENCE_SUMMARY,
        "whyClusterExists": why,
        "appearsToBelongTo": base["subjectName"],
        "recommendedDisposition": disposition,
        "confidence": base["confidence"],
        "missingInfo": base["missingInfo"],
        "rawEvidenceRefs": base["rawEvidenceRefs"],
        "sourceLanes": base["sourceLanes"],
        "stale": bool(stale),
        "inputHash": _input_hash(source, summary_kind="recommendation_cluster"),
    }


def mark_summary_stale_if_evidence_changed(summary: dict[str, Any] | None, record: dict[str, Any] | None, *, summary_kind: str = "admin") -> dict[str, Any]:
    """Return a copy of summary with stale=true when current evidence hash differs."""

    current = dict(summary or {})
    if not current:
        return current
    expected = _input_hash(dict(record or {}), summary_kind=summary_kind)
    if current.get("inputHash") != expected:
        current["stale"] = True
    return current


def attach_admin_summary(record: dict[str, Any] | None, *, stale: bool = False) -> dict[str, Any]:
    updated = dict(record or {})
    updated["adminSummary"] = build_admin_summary(updated, stale=stale)
    return updated
