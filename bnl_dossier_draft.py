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

VALID_STATUSES = {"ACTIVE", "INACTIVE", "ARCHIVED", "PENDING", "UNKNOWN"}
VALID_CLEARANCES = {"PUBLIC", "INTERNAL", "RESTRICTED"}
VALID_ORIGINS = {"KNOWN", "UNKNOWN", "UNVERIFIED", "WITHHELD"}
VALID_IDENTITY_AUTHORITIES = {
    "barcode_controlled",
    "community_owned",
    "external_system",
    "sponsor_controlled",
    "mixed_or_unclear",
}

_PAYMENT_RE = re.compile(r"\b(stripe|checkout|payment|paid|purchase|purchased|customer|priority\s*signal|priority)\b", re.I)
_INTERNAL_RE = re.compile(r"\b(memory_tiers|rd_context|broadcast_memory|relationship_state|redis|database|table|json|diagnostic|diagnostics|evidence\s*id|recommendation\s*id|source\s*row|raw\s*source\s*lane|sourceFileSummary|source\s*lane)\b", re.I)
_ALIAS_RE = re.compile(r"\b(internal\s+alias|alias\s+count|confirmed\s+alias|private\s+alias)\b", re.I)
_FINAL_RE = re.compile(r"\b(approved|published|live|final|complete|official)\b", re.I)
_ROLE_LIMIT = 80
_SUMMARY_LIMIT = 700
_NOTES_LIMIT = 900


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
            candidate = item.get("text") or item.get("summary") or item.get("note") or item.get("body") or item.get("value") or item.get("label")
        else:
            candidate = item
        s = _text(candidate, limit_each)
        if s and s not in out:
            out.append(s)
        if len(out) >= max_items:
            break
    return out


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


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
    field_requirements = packet.get("fieldRequirements")
    if not isinstance(field_requirements, list) or not any(isinstance(item, str) and item.strip() for item in field_requirements):
        errors.append("missing_fieldRequirements")
    safe_classification = packet.get("safeClassification")
    if safe_classification is not None and not isinstance(safe_classification, dict):
        errors.append("invalid_safeClassification")
    return errors


def _unsafe_reasons(text: str) -> list[str]:
    reasons: list[str] = []
    if _PAYMENT_RE.search(text):
        reasons.append("payment/Priority material")
    if _INTERNAL_RE.search(text):
        reasons.append("internal provenance or diagnostics")
    if _ALIAS_RE.search(text):
        reasons.append("internal alias material")
    if _FINAL_RE.search(text):
        reasons.append("final/published status wording")
    return reasons


def _reject_unsafe_public(texts: list[str]) -> tuple[list[str], list[str]]:
    safe: list[str] = []
    rejected: list[str] = []
    for s in texts:
        reasons = _unsafe_reasons(s)
        if reasons:
            rejected.append(f"Rejected {', '.join(reasons)} from public draft wording.")
        else:
            safe.append(s)
    return safe, rejected


def _strip_unsafe_sentence(sentence: str) -> str:
    return "" if _unsafe_reasons(sentence) else sentence


def _sentences(texts: list[str], *, max_count: int = 4) -> list[str]:
    out: list[str] = []
    for text in texts:
        for part in re.split(r"(?<=[.!?])\s+", text):
            s = _strip_unsafe_sentence(part.strip())
            if s and s not in out:
                out.append(s)
            if len(out) >= max_count:
                return out
    return out


def _candidate_name(candidate: dict[str, Any]) -> str:
    return _text(candidate.get("subjectName") or candidate.get("name"), 120) or "Unnamed Source File subject"


def _classification_value(safe_classification: dict[str, Any], key: str, default: str) -> str:
    return _text(safe_classification.get(key), 120) or default


def _identity_authority(packet: dict[str, Any], safe_classification: dict[str, Any]) -> str:
    raw = _text(safe_classification.get("identityAuthority") or _dict(packet.get("identityAliasStatus")).get("identityAuthority"), 80)
    normalized = raw.strip().lower().replace(" ", "_").replace("-", "_")
    aliases = {
        "barcode": "barcode_controlled",
        "barcode_controlled": "barcode_controlled",
        "community": "community_owned",
        "community_owned": "community_owned",
        "external": "external_system",
        "external_system": "external_system",
        "sponsor": "sponsor_controlled",
        "sponsor_controlled": "sponsor_controlled",
    }
    return aliases.get(normalized, "mixed_or_unclear")


def _clearance(packet: dict[str, Any]) -> str:
    boundary = " ".join(_strings(packet.get("sourceBoundaryRules"), max_items=20)).lower()
    if "restricted" in boundary or "withheld" in boundary or "private" in boundary:
        return "RESTRICTED"
    if "internal" in boundary and "public" not in boundary:
        return "INTERNAL"
    return "PUBLIC"


def _origin(packet: dict[str, Any]) -> str:
    alias_status = _dict(packet.get("identityAliasStatus"))
    raw = _text(alias_status.get("status"), 80).lower()
    labels = " ".join(_strings(alias_status.get("publicSafeIdentityLabels"), max_items=10)).lower()
    if alias_status.get("needsConfirmation") is True:
        return "UNVERIFIED"
    if any(term in raw or term in labels for term in ("known", "confirmed", "verified")) and not _FINAL_RE.search(raw + " " + labels):
        return "KNOWN"
    if "withheld" in raw or "withheld" in labels:
        return "WITHHELD"
    if "unknown" in raw:
        return "UNKNOWN"
    return "UNVERIFIED"


def _category_kind_lane(safe_classification: dict[str, Any]) -> tuple[str, str, str]:
    return (
        _classification_value(safe_classification, "category", "Unknown"),
        _classification_value(safe_classification, "kind", "Unknown"),
        _classification_value(safe_classification, "ecosystemLane", "Unknown"),
    )


def _role_from_context(facts: list[str], notes: list[str], category: str, kind: str, lane: str) -> str:
    haystack = " ".join(facts + notes + [category, kind, lane]).lower()
    if any(term in haystack for term in ("moderator", "mod", "personnel", "staff")):
        return "Community moderator"
    if "sponsor" in haystack:
        return "Sponsor record"
    if any(term in haystack for term in ("interface", "system", "tool", "tech")):
        return "Systems interface"
    if any(term in haystack for term in ("producer", "production", "broadcast", "radio")):
        return "Production collaborator"
    if any(term in haystack for term in ("artist", "music", "musician", "track", "album", "song", "dj")):
        return "Music artist"
    if "collaborator" in haystack:
        return "Community collaborator"
    if any(term in haystack for term in ("community", "server", "participant", "member")):
        return "Community member"
    return "Review pending"


def _length_guide(style_packet: dict[str, Any]) -> dict[str, Any]:
    guide = _dict(style_packet.get("authoringGuideSummary"))
    return {
        "length": _text(guide.get("lengthGuide"), 500),
        "tone": _text(guide.get("toneGuide"), 500),
        "rules": _strings(guide.get("draftingRules"), max_items=8),
        "has_examples": bool(style_packet.get("representativePublicDossierExamples") or style_packet.get("categorySpecificExamples")),
    }


def _word_count(text: str) -> int:
    return len(re.findall(r"\b\w+\b", text))


def _summary(name: str, role: str, facts: list[str], notes: list[str], style_packet: dict[str, Any]) -> str:
    guide = _length_guide(style_packet)
    source_sentences = _sentences(facts, max_count=3)
    if source_sentences:
        summary = " ".join(source_sentences)
        if _word_count(summary) < 25 and notes:
            extra = _sentences(notes, max_count=1)
            if extra:
                summary = f"{summary} {extra[0]}"
    else:
        summary = f"{name} is a proposed dossier subject awaiting owner review and stronger public-safe source detail."
    if _word_count(summary) > 85:
        words = summary.split()
        summary = " ".join(words[:80]).rstrip(" ,;:") + "."
    if guide["length"] and _word_count(summary) < 25 and source_sentences:
        summary = f"{summary} This draft keeps the public copy compact while owner review confirms the remaining dossier fields."
    return summary[:_SUMMARY_LIMIT].strip()


def _notes(public_notes: list[str], facts: list[str]) -> str:
    note_sentences = _sentences(public_notes, max_count=2)
    if note_sentences:
        notes = " ".join(note_sentences[:2])
    elif len(facts) < 2:
        notes = "Public-safe notes are limited; admin review should confirm wording before use."
    else:
        notes = "Owner Review should verify the public framing before this draft moves forward."
    return notes[:_NOTES_LIMIT].strip()


def _canonical_registry(style_packet: dict[str, Any]) -> set[str]:
    guidance = style_packet.get("tagRegistryGuidance")
    tags: set[str] = set()

    def add_tag(value: Any) -> None:
        s = _text(value, 80).lower().strip().replace(" ", "-")
        if s and not _unsafe_reasons(s):
            tags.add(s)

    def walk(value: Any) -> None:
        if isinstance(value, str):
            add_tag(value)
        elif isinstance(value, list):
            for item in value:
                walk(item)
        elif isinstance(value, dict):
            for key in ("canonicalTags", "existingTags", "allowedTags", "registry", "tags", "publicTags"):
                if key in value:
                    walk(value[key])
            if "tag" in value:
                add_tag(value["tag"])
            if "name" in value and len(value) <= 3:
                add_tag(value["name"])
    walk(guidance)
    return tags


def _tag_candidates(category: str, kind: str, lane: str, facts: list[str], notes: list[str], packet: dict[str, Any]) -> list[str]:
    haystack = " ".join([category, kind, lane] + facts + notes).lower()
    candidates: list[str] = []
    mapping = [
        (("artist", "music", "musician"), "artist"),
        (("collaborator",), "collaborator"),
        (("community", "member"), "community"),
        (("community", "member"), "member"),
        (("personnel", "moderator", "mod"), "mod"),
        (("sponsor",), "sponsor"),
        (("interface", "system"), "systems"),
        (("interface", "system", "tech"), "tech"),
        (("production", "broadcast"), "broadcast"),
        (("radio",), "radio"),
        (("producer",), "producer"),
    ]
    for needles, tag in mapping:
        if any(needle in haystack for needle in needles) and tag not in candidates:
            candidates.append(tag)
    for tag in _strings(packet.get("proposedTags"), limit_each=60, max_items=10):
        normalized = tag.lower().replace(" ", "-")
        if not _unsafe_reasons(normalized) and normalized not in candidates:
            candidates.append(normalized)
    return candidates


def _split_tags(packet: dict[str, Any], style_packet: dict[str, Any], category: str, kind: str, lane: str, facts: list[str], notes: list[str]) -> tuple[list[str], list[str]]:
    registry = _canonical_registry(style_packet)
    candidates = _tag_candidates(category, kind, lane, facts, notes, packet)
    confirmed: list[str] = []
    proposed: list[str] = []
    for tag in candidates:
        if _unsafe_reasons(tag):
            continue
        if tag in registry:
            if tag not in confirmed:
                confirmed.append(tag)
        elif tag not in proposed:
            proposed.append(tag)
    return confirmed[:8], proposed[:8]


def _first_link(packet: dict[str, Any]) -> dict[str, Any] | None:
    for item in _as_list(packet.get("publicSafeLinks") or packet.get("links")):
        if isinstance(item, dict):
            url = _text(item.get("url"), 500)
            label = _text(item.get("label") or item.get("title") or "Primary link", 120)
            if url and not (_unsafe_reasons(url) or _unsafe_reasons(label)):
                return {"label": label, "url": url}
    return None


def _source_usage_summary(packet: dict[str, Any]) -> str:
    usage = _dict(packet.get("sourceUsageSummary"))
    note_count = len(_as_list(usage.get("sourceFileNoteIds")))
    recommendation_count = len(_as_list(usage.get("recommendationIds")))
    lane_count = len(_as_list(usage.get("sourceLanes")))
    parts = ["BNL drafted from public-safe facts, public-safe notes, safe classification, style guidance, field requirements, and boundary rules only."]
    if note_count or recommendation_count:
        parts.append(f"The packet referenced {note_count} source note(s) and {recommendation_count} recommendation reference(s) for admin traceability; raw IDs and lanes were not copied into public fields.")
    if lane_count:
        parts.append("Source lane details were treated as boundary context, not public copy.")
    return " ".join(parts)


def generate_dossier_draft(packet: dict[str, Any]) -> dict[str, Any]:
    candidate = _dict(packet.get("candidate"))
    safe_classification = _dict(packet.get("safeClassification"))
    style_packet = _dict(packet.get("stylePacket"))
    name = _candidate_name(candidate)
    category, kind, lane = _category_kind_lane(safe_classification)
    public_facts, rejected_facts = _reject_unsafe_public(_strings(packet.get("publicSafeFacts"), max_items=16))
    public_notes, rejected_notes = _reject_unsafe_public(_strings(packet.get("publicSafeNotes"), max_items=12))
    role = _role_from_context(public_facts, public_notes, category, kind, lane)[:_ROLE_LIMIT]
    thin = len(public_facts) + len(public_notes) < 2

    missing = _strings(packet.get("missingInfo"), max_items=8)
    if thin:
        missing.insert(0, "Add more public-safe facts before this draft is treated as rich dossier copy.")
    if _dict(packet.get("identityAliasStatus")).get("needsConfirmation") is not False:
        missing.append("Confirm what identity authority, if any, may be stated publicly.")

    owner_warnings = _strings(packet.get("ownerReviewRules"), max_items=8) + _strings(packet.get("reviewOnlyWarnings"), max_items=8)
    owner_warnings.append("Owner Review must approve identity, role, category, links, and public wording before publication.")
    public_warnings = _strings(packet.get("sourceBoundaryRules"), max_items=8)
    public_warnings.append("Public fields use only publicSafeFacts, publicSafeNotes, safeClassification, stylePacket, and fieldRequirements.")

    rejected = rejected_facts + rejected_notes
    for unsafe_key in ("doNotSayNotes", "reviewOnlyWarnings", "forbiddenPublicCopyPatterns"):
        if _strings(packet.get(unsafe_key), max_items=4):
            rejected.append(f"Did not use {unsafe_key} as public-facing dossier copy.")
    alias_status = _dict(packet.get("identityAliasStatus"))
    if alias_status.get("internalAliasCount") or packet.get("internalAliasCount") or packet.get("internalAliases"):
        rejected.append("Did not expose internal aliases or alias counts.")
    if style_packet.get("representativePublicDossierExamples") or style_packet.get("categorySpecificExamples"):
        rejected.append("Used existing public dossier examples only for structure, tone, field shape, length, and tag style; did not copy their facts.")
    if not rejected:
        rejected.append("No unsafe supplied material was needed for public-facing fields.")

    tags, proposed_tags = _split_tags(packet, style_packet, category, kind, lane, public_facts, public_notes)

    draft = {
        "name": name,
        "category": category,
        "kind": kind,
        "ecosystemLane": lane,
        "identityAuthority": _identity_authority(packet, safe_classification),
        "status": "PENDING",
        "clearance": _clearance(packet),
        "origin": _origin(packet),
        "role": role,
        "summary": _summary(name, role, public_facts, public_notes, style_packet),
        "notes": _notes(public_notes, public_facts),
        "tags": tags,
        "proposedTags": proposed_tags,
        "primaryLink": _first_link(packet),
        "links": [],
        "files": [],
        "missingInfoQuestions": missing[:10],
        "ownerReviewWarnings": owner_warnings[:12],
        "publicSafetyWarnings": public_warnings[:12],
        "unsupportedClaimsRejected": rejected[:14],
        "sourceUsageSummary": _source_usage_summary(packet),
    }
    return {"draft": draft}
