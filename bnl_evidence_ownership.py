"""Shared evidence ownership/source routing helpers for BNL memory."""

from __future__ import annotations

import json
import re
from collections import Counter
from typing import Any

OWNED_SCOPES = {
    "owned",
    "direct_address",
    "confirmed_alias",
    "explicit_relationship",
    "queue_authoritative",
    "site_authoritative",
    "broadcast_memory_authoritative",
}
REVIEW_ONLY_SCOPES = {
    "co_mention",
    "shared_topic",
    "generated_or_taxonomy",
    "source_blind_legacy",
    "unknown",
}
OWNERSHIP_SCOPES = (*OWNED_SCOPES, *REVIEW_ONLY_SCOPES)

# This ownership classifier is the shared gate for BNL memory.
# Source File intelligence, Discord memory retrieval, website/dossier context,
# queue/submission context, and future canonical entity profiles should all pass
# through ownership/source/visibility classification before BNL treats evidence
# as subject knowledge.
def classify_evidence_ownership(raw_item: Any, subject_name: str, subject_key: str, confirmed_aliases: list[str] | None = None) -> dict[str, str]:
    confirmed_aliases = confirmed_aliases or []
    text = _safe_summary(raw_item)
    haystack = _jsonish(raw_item).lower()
    source_type = _source_type(raw_item)
    visibility = _visibility(raw_item)
    authority = _authority(raw_item)
    subject_tokens = _subject_tokens(subject_name, subject_key, confirmed_aliases)
    subject_hit = _any_token(haystack, subject_tokens)
    author_tokens = _field_values(raw_item, ("author", "authorName", "author_name", "username", "userName", "displayName", "memberName", "sender", "speaker", "createdBy", "owner", "ownerName"))
    subject_field_tokens = _field_values(raw_item, ("subject", "subjectName", "subject_name", "subjectKey", "subject_key", "entity", "entityName", "normalizedName"))
    relationship_tokens = _field_values(raw_item, ("from", "to", "source", "target", "subjectA", "subjectB", "endpointA", "endpointB", "relatedName", "relatedSubject"))

    def result(ownership: str, confidence: str, reason: str) -> dict[str, str]:
        return {
            "ownership": ownership,
            "confidence": confidence,
            "reason": reason,
            "safeSummary": text,
            "sourceType": source_type,
            "visibility": visibility,
            "authority": authority,
        }

    if _has_source_blind_marker(raw_item, haystack):
        return result("source_blind_legacy", "weak", "source-blind or legacy memory trace lacks item-level ownership metadata")
    if _has_generated_marker(raw_item, haystack):
        return result("generated_or_taxonomy", "weak", "generated report/taxonomy/system wording is not source evidence")
    if _has_queue_marker(raw_item, haystack) and _visibility_safe(haystack, visibility):
        return result("queue_authoritative", "strong" if subject_hit else "moderate", "queue/submission authority marker is present and visibility-safe")
    if _has_site_marker(raw_item, haystack) and _visibility_safe(haystack, visibility):
        return result("site_authoritative", "strong" if subject_hit else "moderate", "site/source-file authority marker is present and visibility-safe")
    if _has_broadcast_marker(raw_item, haystack) and _visibility_safe(haystack, visibility):
        return result("broadcast_memory_authoritative", "strong" if subject_hit else "moderate", "broadcast memory authority marker is present and visibility-safe")
    if _field_matches(author_tokens, subject_tokens):
        return result("owned", "strong", "author/user metadata matches the subject or confirmed alias")
    if _field_matches(subject_field_tokens, subject_tokens):
        return result("owned", "strong", "subject metadata matches the requested subject")
    if _field_matches(author_tokens, [_norm(a) for a in confirmed_aliases if a]):
        return result("confirmed_alias", "strong", "author/user metadata matches a confirmed subject alias")
    if relationship_tokens and _field_matches(relationship_tokens, subject_tokens):
        return result("explicit_relationship", "moderate", "relationship endpoint metadata includes the subject")
    if _direct_address_to_subject(raw_item, haystack, subject_tokens):
        return result("direct_address", "moderate", "item is addressed directly to the subject")
    if _text_starts_as_subject(text, subject_tokens):
        return result("owned", "moderate", "text is written as subject-authored/action evidence")
    if subject_hit and _shared_topic_marker(raw_item, haystack):
        return result("shared_topic", "weak", "subject appears in topic/community context without ownership metadata")
    if subject_hit or _co_mention_marker(raw_item, haystack):
        return result("co_mention", "weak", "subject or another named entity appears nearby without ownership metadata")
    if _shared_topic_marker(raw_item, haystack):
        return result("shared_topic", "weak", "topic/community context lacks subject ownership metadata")
    return result("unknown", "weak", "no reliable ownership/source scope metadata was available")


def subject_owned_text_fragments(memory: dict[str, Any], subject_name: str, subject_key: str, confirmed_aliases: list[str] | None = None) -> dict[str, Any]:
    fragments: dict[str, list[dict[str, Any]]] = {scope: [] for scope in OWNERSHIP_SCOPES}
    for raw in _iter_memory_evidence(memory):
        classified = classify_evidence_ownership(raw, subject_name, subject_key, confirmed_aliases)
        scope = classified["ownership"]
        if classified.get("safeSummary"):
            fragments.setdefault(scope, []).append({"text": classified["safeSummary"], "classification": classified})
    counts = Counter({scope: len(fragments.get(scope, [])) for scope in OWNERSHIP_SCOPES})
    owned_items = [item for scope in OWNED_SCOPES for item in fragments.get(scope, [])]
    review_items = [item for scope in REVIEW_ONLY_SCOPES for item in fragments.get(scope, [])]
    summary = {
        "ownedEvidenceCount": counts["owned"],
        "directAddressEvidenceCount": counts["direct_address"],
        "confirmedAliasEvidenceCount": counts["confirmed_alias"],
        "explicitRelationshipEvidenceCount": counts["explicit_relationship"],
        "siteAuthoritativeEvidenceCount": counts["site_authoritative"],
        "queueAuthoritativeEvidenceCount": counts["queue_authoritative"],
        "broadcastMemoryAuthoritativeEvidenceCount": counts["broadcast_memory_authoritative"],
        "coMentionEvidenceCount": counts["co_mention"],
        "sharedTopicEvidenceCount": counts["shared_topic"],
        "generatedOrNoiseEvidenceCount": counts["generated_or_taxonomy"],
        "sourceBlindLegacyEvidenceCount": counts["source_blind_legacy"],
        "unknownEvidenceCount": counts["unknown"],
        "ownedEvidenceExamples": _texts(owned_items, 5),
        "coMentionExamples": _texts(fragments.get("co_mention", []), 5),
        "sourceBlindWarnings": [f"Source-blind legacy memory was quarantined: {t}" for t in _texts(fragments.get("source_blind_legacy", []), 4)],
        "routingWarnings": [],
    }
    if counts["co_mention"] or counts["shared_topic"]:
        summary["routingWarnings"].append("Co-mentioned/shared-topic evidence is review context only and must not become subject facts.")
    if counts["generated_or_taxonomy"]:
        summary["routingWarnings"].append("Generated/taxonomy/report labels were ignored for subject-owned claims.")
    if counts["unknown"]:
        summary["routingWarnings"].append("Unknown ownership evidence needs review before public-safe subject claims.")
    if counts["source_blind_legacy"]:
        summary["routingWarnings"].append("Source-blind legacy memory is quarantined from owned subject facts.")
    return {"owned": owned_items, "reviewOnly": review_items, "byOwnership": fragments, "summary": summary}


def _iter_memory_evidence(memory: dict[str, Any]):
    context = memory.get("sourceIntelligenceContext") if isinstance(memory.get("sourceIntelligenceContext"), dict) else {}
    evidence_fields = (
        "representativeEvidence", "evidenceDetails", "bestEvidenceToReview", "usefulEvidence", "conversationHighlights",
        "bnlInteractionSignals", "musicSignals", "communitySignals", "relationshipSignals", "knownContext",
    )
    generated_fields = (
        "subjectRead", "bnlTake", "confidenceRead", "queueSubmissionRead", "queueSubmissionNote", "sourceFileGaps",
        "recommendedAdminActions", "doNotSayPubliclyYet", "sourceCoverage", "topTopicDetails", "topicBreakdown", "topicThemes",
        "conversationThemes", "publicUseCandidates", "reviewOnlyEvidence", "missingInfo", "sourceAuthority", "sourceCounts",
        "sourceTypes", "archivePayloadMetadata",
    )
    for field in evidence_fields:
        yield from _wrap_items(context.get(field), field)
    raw_meta = context.get("archivePayloadMetadata") if isinstance(context.get("archivePayloadMetadata"), dict) else {}
    yield from _wrap_items(raw_meta.get("representativeFragments"), "representativeFragments")
    yield from _wrap_items(memory.get("representativeMoments"), "representativeMoments")
    for field in ("communityContext", "musicCreativeContext", "relationshipContext", "publicSafeCandidateFacts"):
        yield from _wrap_items(memory.get(field), field)
    for field in generated_fields:
        yield from _wrap_items(context.get(field), field, generated=True)
    for path in (("memory_tiers",), ("source_blind_memory_trace",), ("memoryTiers",), ("sourceBlindMemoryTrace",)):
        cur: Any = memory
        for key in path:
            cur = cur.get(key) if isinstance(cur, dict) else None
        if cur:
            yield from _wrap_items(cur, "/".join(path), source_blind=True)


def _wrap_items(value: Any, lane: str, *, generated: bool = False, source_blind: bool = False):
    if value in (None, "", [], {}):
        return
    if isinstance(value, list):
        for item in value:
            yield from _wrap_items(item, lane, generated=generated, source_blind=source_blind)
    elif isinstance(value, dict):
        item = dict(value)
        item.setdefault("sourceLane", lane)
        if generated:
            item.setdefault("generatedReportField", True)
        if source_blind:
            item.setdefault("sourceBlindLegacy", True)
        yield item
    else:
        yield {"summary": str(value), "sourceLane": lane, **({"generatedReportField": True} if generated else {}), **({"sourceBlindLegacy": True} if source_blind else {})}


def _safe_summary(raw: Any) -> str:
    if isinstance(raw, dict):
        for key in ("summary", "safeSummary", "detail", "evidenceSummary", "claim", "text", "message", "note", "explanation", "snippet", "topic", "theme", "label"):
            if raw.get(key):
                return _clean(str(raw.get(key)), 360)
        return _clean(json.dumps({k: v for k, v in raw.items() if k not in {"id", "rowId", "sourceRowId", "userId", "channelId", "rawRefJson"}}, default=str), 360)
    return _clean(str(raw or ""), 360)


def _clean(text: str, limit: int) -> str:
    return re.sub(r"\s+", " ", text).strip()[:limit]


def _jsonish(raw: Any) -> str:
    try:
        return json.dumps(raw, default=str)
    except Exception:
        return str(raw)


def _source_type(raw: Any) -> str:
    if isinstance(raw, dict):
        for key in ("sourceType", "sourceLane", "source", "lane", "table", "sourceTable", "origin", "type"):
            if raw.get(key):
                return _clean(str(raw.get(key)), 90)
    return "unknown"


def _visibility(raw: Any) -> str:
    if isinstance(raw, dict):
        for key in ("visibility", "channelPolicy", "policy", "publicSafe", "privacy"):
            if raw.get(key) is not None:
                return _clean(str(raw.get(key)), 80)
    return "unknown"


def _authority(raw: Any) -> str:
    if isinstance(raw, dict):
        for key in ("authority", "sourceAuthority", "authorityType", "sourceAuthorityType"):
            if raw.get(key):
                return _clean(str(raw.get(key)), 90)
    return "unverified"


def _norm(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(value or "").lower()).strip()


def _subject_tokens(subject_name: str, subject_key: str, aliases: list[str]) -> list[str]:
    vals = [subject_name, subject_key, str(subject_key or "").replace("-", " "), *aliases]
    return [v for v in {_norm(v) for v in vals if _norm(v)}]


def _any_token(haystack: str, tokens: list[str]) -> bool:
    normalized = _norm(haystack)
    return any(re.search(rf"(?<![a-z0-9]){re.escape(token)}(?![a-z0-9])", normalized) for token in tokens if token)


def _field_values(raw: Any, keys: tuple[str, ...]) -> list[str]:
    out: list[str] = []
    if isinstance(raw, dict):
        for key in keys:
            value = raw.get(key)
            if isinstance(value, dict):
                out.extend(str(v) for v in value.values() if v)
            elif isinstance(value, (list, tuple, set)):
                out.extend(str(v) for v in value if v)
            elif value:
                out.append(str(value))
    return [_norm(v) for v in out if _norm(v)]


def _field_matches(values: list[str], tokens: list[str]) -> bool:
    return any(v == token or _any_token(v, [token]) for v in values for token in tokens if v and token)


def _visibility_safe(haystack: str, visibility: str) -> bool:
    lowered = f"{haystack} {visibility}".lower()
    return "public" in lowered or "visibility safe" in lowered or "approved" in lowered or "review" not in lowered


def _has_source_blind_marker(raw: Any, haystack: str) -> bool:
    return "source_blind" in haystack or "sourceblind" in haystack or "memory_tiers" in haystack or (isinstance(raw, dict) and raw.get("sourceBlindLegacy"))


def _has_generated_marker(raw: Any, haystack: str) -> bool:
    lane = str(raw.get("sourceLane") if isinstance(raw, dict) else "").lower()
    return bool(isinstance(raw, dict) and raw.get("generatedReportField")) or any(t in haystack or t in lane for t in ("generated", "taxonomy", "report label", "classification label", "topicbreakdown", "toptopicdetails", "sourcecoverage"))


def _has_queue_marker(raw: Any, haystack: str) -> bool:
    return any(t in haystack for t in ("queue_authoritative", "queue authoritative", "queue submission", "submission_id", "submitted track"))


def _has_site_marker(raw: Any, haystack: str) -> bool:
    return any(t in haystack for t in ("site_authoritative", "site authoritative", "source file", "identity check", "public profile", "site_visibility"))


def _has_broadcast_marker(raw: Any, haystack: str) -> bool:
    return any(t in haystack for t in ("broadcast_memory_authoritative", "broadcast authoritative", "broadcast memory", "played on", "radio episode"))


def _direct_address_to_subject(raw: Any, haystack: str, subject_tokens: list[str]) -> bool:
    targets = _field_values(raw, ("target", "targetName", "recipient", "addressedTo", "mentions"))
    return _field_matches(targets, subject_tokens) or any(f"@{token}" in haystack for token in subject_tokens)


def _text_starts_as_subject(text: str, subject_tokens: list[str]) -> bool:
    normalized = _norm(text[:120])
    return any(normalized.startswith(token + " ") or normalized.startswith(token + "s ") for token in subject_tokens)


def _shared_topic_marker(raw: Any, haystack: str) -> bool:
    lane = str(raw.get("sourceLane") if isinstance(raw, dict) else "").lower()
    return any(t in haystack or t in lane for t in ("topic", "theme", "community", "knowncontext", "relationshipsignals", "musicsignals"))


def _co_mention_marker(raw: Any, haystack: str) -> bool:
    lane = str(raw.get("sourceLane") if isinstance(raw, dict) else "").lower()
    return "mention" in haystack or "near" in haystack or "appears in" in haystack or "relationshipsignals" in lane


def _texts(items: list[dict[str, Any]], limit: int) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for item in items:
        text = _clean(str(item.get("text") or item.get("classification", {}).get("safeSummary") or ""), 240)
        key = text.lower()
        if text and key not in seen:
            out.append(text)
            seen.add(key)
        if len(out) >= limit:
            break
    return out
