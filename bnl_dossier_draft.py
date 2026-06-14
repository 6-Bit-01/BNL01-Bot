"""BNL Proposed Dossier draft generator v1.

Pure helpers for the internal site-to-BNL draft endpoint. These functions do not
write Discord, memory, Source Files, candidates, or website state.
"""
from __future__ import annotations

import hmac
import os
import re
from typing import Any

DRAFT_ENDPOINT_PATH = "/internal/dossiers/draft"
DRAFT_TOKEN_HEADER = "X-BNL-DOSSIER-DRAFT-TOKEN"
DRAFT_TOKEN_ENV = "BNL_DOSSIER_DRAFT_GENERATOR_TOKEN"
REQUEST_TYPE = "bnl_proposed_dossier_draft"
VERSION = "1.0"

_PAYMENT_RE = re.compile(r"\b(stripe|checkout|payment|paid|purchase|purchased|customer|priority\s*signal|priority)\b", re.I)
_INTERNAL_RE = re.compile(r"\b(memory_tiers|rd_context|broadcast_memory|relationship_state|redis|database|table|json|diagnostic|evidence\s*id|recommendation\s*id|source\s*row|raw\s*source\s*lane|sourceFileSummary)\b", re.I)
_ALIAS_RE = re.compile(r"\b(internal\s+alias|alias\s+count|confirmed\s+alias|private\s+alias)\b", re.I)
_FINAL_RE = re.compile(r"\b(published|official|approved|live|complete|final)\b", re.I)


def _as_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if value is None:
        return []
    return [value]


def _text(value: Any, limit: int = 500) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return ""
    compact = re.sub(r"\s+", " ", str(value)).strip()
    return compact[:limit].rstrip()


def _strings(value: Any, *, limit_each: int = 260, max_items: int = 12) -> list[str]:
    out: list[str] = []
    for item in _as_list(value):
        if isinstance(item, dict):
            candidate = item.get("text") or item.get("summary") or item.get("note") or item.get("value") or item.get("label")
        else:
            candidate = item
        s = _text(candidate, limit_each)
        if s and s not in out:
            out.append(s)
        if len(out) >= max_items:
            break
    return out


def is_dossier_draft_token_valid(provided_token: str | None, *, environ: dict[str, str] | None = None) -> bool:
    env = environ if environ is not None else os.environ
    expected = (env.get(DRAFT_TOKEN_ENV) or "").strip()
    provided = (provided_token or "").strip()
    return bool(expected and provided and hmac.compare_digest(provided, expected))


def validate_dossier_draft_packet(packet: Any) -> list[str]:
    errors: list[str] = []
    if not isinstance(packet, dict):
        return ["invalid_payload"]
    if packet.get("requestType") != REQUEST_TYPE:
        errors.append("invalid_requestType")
    if str(packet.get("version") or "") != VERSION:
        errors.append("invalid_version")
    candidate = packet.get("candidate")
    if not isinstance(candidate, dict):
        errors.append("missing_candidate")
    else:
        if not _text(candidate.get("sourceFileId"), 160):
            errors.append("missing_candidate.sourceFileId")
        if not _text(candidate.get("subjectName"), 160):
            errors.append("missing_candidate.subjectName")
    if not isinstance(packet.get("stylePacket"), dict):
        errors.append("missing_stylePacket")
    if not isinstance(packet.get("fieldRequirements"), dict):
        errors.append("missing_fieldRequirements")
    return errors


def _candidate_public_values(candidate: dict[str, Any], safe_classification: dict[str, Any]) -> dict[str, str]:
    def pick(*keys: str, default: str = "") -> str:
        for key in keys:
            value = _text(candidate.get(key) or safe_classification.get(key), 120)
            if value:
                return value
        return default

    return {
        "name": pick("subjectName", "name", default="Unnamed Source File subject"),
        "category": pick("category", default="Source File candidate"),
        "kind": pick("kind", default="proposed_dossier"),
        "ecosystemLane": pick("ecosystemLane", "lane", default="community"),
        "identityAuthority": pick("identityAuthority", default="unconfirmed"),
        "status": pick("status", default="proposed"),
        "clearance": pick("clearance", default="owner_review_required"),
        "origin": pick("origin", default="BNL Source File draft request"),
    }


def _reject_unsafe_public(texts: list[str]) -> tuple[list[str], list[str]]:
    safe: list[str] = []
    rejected: list[str] = []
    for s in texts:
        reasons = []
        if _PAYMENT_RE.search(s):
            reasons.append("payment/Priority material")
        if _INTERNAL_RE.search(s):
            reasons.append("internal provenance or diagnostics")
        if _ALIAS_RE.search(s):
            reasons.append("internal alias material")
        if _FINAL_RE.search(s):
            reasons.append("final/published status wording")
        if reasons:
            rejected.append(f"Rejected {', '.join(reasons)} from public draft wording.")
        else:
            safe.append(s)
    return safe, rejected


def _infer_role(facts: list[str], notes: list[str], category: str, kind: str) -> str:
    haystack = " ".join(facts + notes + [category, kind]).lower()
    if any(term in haystack for term in ("artist", "music", "musician", "producer", "track", "album", "song", "dj")):
        return "Artist / music community subject"
    if any(term in haystack for term in ("community", "server", "participant", "member")):
        return "Community subject"
    return "Source File subject pending owner classification"


def _first_link(packet: dict[str, Any]) -> dict[str, Any] | None:
    for item in _as_list(packet.get("publicSafeLinks") or packet.get("links")):
        if isinstance(item, dict):
            url = _text(item.get("url"), 500)
            if url and not (_PAYMENT_RE.search(url) or _INTERNAL_RE.search(url)):
                return {"label": _text(item.get("label") or item.get("title") or "Primary link", 120), "url": url}
    return None


def generate_dossier_draft(packet: dict[str, Any]) -> dict[str, Any]:
    candidate = packet.get("candidate") if isinstance(packet.get("candidate"), dict) else {}
    safe_classification = packet.get("safeClassification") if isinstance(packet.get("safeClassification"), dict) else {}
    public_facts, rejected_facts = _reject_unsafe_public(_strings(packet.get("publicSafeFacts"), max_items=16))
    public_notes, rejected_notes = _reject_unsafe_public(_strings(packet.get("publicSafeNotes"), max_items=12))
    vals = _candidate_public_values(candidate, safe_classification)
    role = _infer_role(public_facts, public_notes, vals["category"], vals["kind"])
    thin = len(public_facts) + len(public_notes) < 2

    summary = " ".join(public_facts[:2]) if public_facts else f"{vals['name']} is a proposed dossier subject awaiting owner review and stronger public-safe source detail."
    notes = " ".join(public_notes[:3]) if public_notes else "Public-safe notes are limited; admin review should confirm wording before use."
    tags = [t.lower().replace(" ", "-")[:40] for t in _strings(candidate.get("tags") or safe_classification.get("tags"), limit_each=60, max_items=8)]
    proposed = [t.lower().replace(" ", "-")[:40] for t in _strings(packet.get("proposedTags") or packet.get("stylePacket", {}).get("suggestedTags"), limit_each=60, max_items=8)]

    missing = _strings(packet.get("missingInfo"), max_items=8)
    if thin:
        missing.insert(0, "Add more public-safe facts before this draft is treated as rich dossier copy.")
    if vals["identityAuthority"].lower() in {"", "unconfirmed", "unknown", "bnl_inferred"}:
        missing.append("Confirm what identity authority, if any, may be stated publicly.")

    owner_warnings = _strings(packet.get("ownerReviewRules"), max_items=8) + _strings(packet.get("reviewOnlyWarnings"), max_items=8)
    owner_warnings.append("Owner Review must approve identity, role, category, links, and public wording before publication.")
    public_warnings = _strings(packet.get("sourceBoundaryRules"), max_items=8)
    public_warnings.append("Public fields use only publicSafeFacts, publicSafeNotes, safeClassification, stylePacket, and fieldRequirements.")

    rejected = rejected_facts + rejected_notes
    for unsafe_key in ("doNotSayNotes", "reviewOnlyWarnings", "sourceBoundaryNotes"):
        if _strings(packet.get(unsafe_key), max_items=4):
            rejected.append(f"Did not use {unsafe_key} as public-facing dossier copy.")
    if packet.get("internalAliasCount") or packet.get("internalAliases"):
        rejected.append("Did not expose internal aliases or alias counts.")
    if not rejected:
        rejected.append("No unsafe supplied material was needed for public-facing fields.")

    draft = {
        "name": vals["name"], "category": vals["category"], "kind": vals["kind"], "ecosystemLane": vals["ecosystemLane"],
        "identityAuthority": vals["identityAuthority"], "status": vals["status"], "clearance": vals["clearance"], "origin": vals["origin"],
        "role": role, "summary": summary[:700], "notes": notes[:900], "tags": tags, "proposedTags": proposed,
        "primaryLink": _first_link(packet), "links": [], "files": [],
        "missingInfoQuestions": missing[:10], "ownerReviewWarnings": owner_warnings[:12], "publicSafetyWarnings": public_warnings[:12],
        "unsupportedClaimsRejected": rejected[:12],
        "sourceUsageSummary": "BNL drafted from the packet's public-safe facts, public-safe notes, safe classification, style guidance, field requirements, and boundary rules only; review-only and do-not-say material was used only to form warnings or rejections.",
    }
    return {"draft": draft}
