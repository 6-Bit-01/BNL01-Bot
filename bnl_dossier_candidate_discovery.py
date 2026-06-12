"""Dynamic candidate discovery for review-only BNL dossier recommendations.

The discovery layer is intentionally narrow: it only reads caller-supplied,
approved source-safe lane material and returns ordinary dossier recommendation
payloads for the existing website ingest pipeline. It does not scan Discord,
create source files, create drafts, publish, merge identities, or confirm aliases.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from collections import Counter
from typing import Any, Callable

from bnl_admin_summaries import build_recommendation_cluster_summary
from bnl_dossier_recommendations import VALID_DOSSIER_CATEGORIES, build_dossier_recommendation_payload
from bnl_community_scouting import COMMUNITY_PRESENCE_LANE, community_category_taxonomy
from bnl_dossier_source_packets import (
    APPROVED_SOURCE_LANES,
    DEFAULT_MISSING_INFO,
    DEFAULT_PUBLIC_SAFETY_NOTES,
    DEFAULT_SUGGESTED_ACTION,
    MAX_EVIDENCE_SUMMARY_LENGTH,
    MAX_SNIPPET_LENGTH,
    normalize_source_lanes,
    subject_key,
)

DISCOVERY_INGEST_SOURCE = "bnl_dynamic_candidate_discovery"
DEFAULT_DISCOVERY_LANES = ["rd_context", "broadcast_memory"]
COMMUNITY_DISCOVERY_LANE = COMMUNITY_PRESENCE_LANE
MAX_DISCOVERY_LIMIT = 25
DEFAULT_DISCOVERY_LIMIT = 10
MIN_CANDIDATE_SCORE = 3
TITLE_ONLY_MIN_SCORE = 5
MEDIUM_CONTEXT_MIN_SCORE = 2
MAX_REASON_LENGTH = 300
_ALLOWED_DISCOVERY_LANES = (set(DEFAULT_DISCOVERY_LANES) | {COMMUNITY_DISCOVERY_LANE}) & set(APPROVED_SOURCE_LANES)
_DISCORD_MENTION_PATTERN = re.compile(r"<[@#!&]?[0-9]{5,}>")
_LONG_ID_PATTERN = re.compile(r"\b\d{15,22}\b")
_SAFE_LANE_PATTERN = re.compile(r"[^a-z0-9_-]+")

_STOP_PHRASES = {
    "BNL",
    "BNL 01",
    "BARCODE",
    "BARCODE Network",
    "BARCODE Radio",
    "Discord",
    "R&D",
    "Research Development",
    "Source File",
    "Source Files",
    "Dossier",
    "Dossiers",
    "Broadcast Memory",
    "Recommendation Inbox",
    "Operator",
    "Admin",
    "Owner",
    "Network",
    "The Network",
    "Do Not",
    "Public Safe",
}

_WEAK_SUBJECT_TERMS = {
    "next",
    "add",
    "origin",
    "bit",
    "signal",
    "source",
    "file",
    "candidate",
    "review",
    "lane",
    "memory",
    "public",
    "private",
    "context",
    "system",
    "entity",
    "recommendation",
    "inbox",
    "subject",
    "name",
    "admin",
    "owner",
    "operator",
    "mod",
    "user",
    "channel",
    "message",
    "everyone",
    "somebody",
    "someone",
    "anyone",
    "today",
    "tonight",
    "yesterday",
    "tomorrow",
    "chat",
    "thread",
    "post",
    "comment",
    "reply",
    "song",
    "track",
    "live",
    "show",
    "server",
    "discord",
}

_KNOWN_ONE_WORD_SUBJECTS = {"cliff", "sheila", "shadowspit"}
_SUBJECT_WORD = r"[A-Z0-9][A-Za-z0-9'_-]*"
_SUBJECT_PHRASE = rf"{_SUBJECT_WORD}(?:\s+{_SUBJECT_WORD}){{0,4}}"
_EXPLICIT_SUBJECT_BOUNDARY = r"(?=$|[.?!,;:]|\s+(?:because|and|with|from|in|as|that|so|when|while|but|for review)\b)"

_CATEGORY_TERMS = (
    ("identity_alias_review", re.compile(r"\b(alias|alternate name|aka|same as|duplicate|identity|connect(?:s|ed)? to)\b", re.I)),
    ("system_concept_candidate", re.compile(r"\b(system|concept|protocol|workflow|lane|interface|program)\b", re.I)),
    ("new_source_file_candidate", re.compile(r"\b(source file|dossier|candidate|review|track|recurring|sponsor|artist|collaborator|lore|character|entity)\b", re.I)),
)

@dataclass
class DiscoverySourceItem:
    lane: str
    text: str
    label: str = ""
    source_ref: str = ""
    weight: int = 1
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class CandidateAccumulator:
    subject_name: str
    category: str = "new_source_file_candidate"
    lanes: set[str] = field(default_factory=set)
    evidence: list[dict[str, str]] = field(default_factory=list)
    mentions: int = 0
    explicit_hits: int = 0
    title_hits: int = 0
    contextual_hits: int = 0
    score: int = 0
    community_signal_count: int = 0
    direct_interaction_count: int = 0
    operator_mention_count: int = 0
    days_active: int = 0
    connection_notes: list[str] = field(default_factory=list)


def parse_dossier_discovery_command(content: str) -> tuple[bool, dict[str, Any] | None, str]:
    """Parse `!bnl dossier discover candidates [| lanes=...] [| limit=...] [| dry_run=true]`."""

    text = (content or "").strip()
    match = re.match(r"^!bnl\s+dossier\s+discover\s+candidates(?:\s*(?:\|\s*)?(.*))?$", text, flags=re.I | re.S)
    alias_match = None if match else re.match(r"^!bnl\s+community\s+scout\s+candidates(?:\s*(?:\|\s*)?(.*))?$", text, flags=re.I | re.S)
    if not match and not alias_match:
        return False, None, "not_a_dossier_discovery_command"
    if alias_match:
        match = alias_match

    options_text = (match.group(1) or "").strip()
    options: dict[str, Any] = {
        "lanes": [COMMUNITY_DISCOVERY_LANE] if alias_match else list(DEFAULT_DISCOVERY_LANES),
        "limit": DEFAULT_DISCOVERY_LIMIT,
        "dry_run": False,
    }
    if options_text:
        for part in [piece.strip() for piece in options_text.split("|") if piece.strip()]:
            key_match = re.match(r"^([a-zA-Z_]+)\s*=\s*(.+)$", part)
            if not key_match:
                return True, None, "Use: `!bnl dossier discover candidates [| lanes=rd_context,broadcast_memory,community_presence] [| limit=10] [| dry_run=true]`"
            key = key_match.group(1).strip().lower()
            value = key_match.group(2).strip()
            if key in {"lane", "lanes", "sourcelanes"}:
                lanes = normalize_discovery_lanes(value)
                if not lanes:
                    return True, None, "No approved discovery lanes were requested. Approved lanes: rd_context,broadcast_memory,community_presence."
                options["lanes"] = lanes
            elif key == "limit":
                try:
                    options["limit"] = max(1, min(int(value), MAX_DISCOVERY_LIMIT))
                except ValueError:
                    return True, None, "Discovery limit must be a number."
            elif key in {"dry", "dryrun", "dry_run"}:
                options["dry_run"] = value.lower() in {"1", "true", "yes", "y", "on"}
            else:
                return True, None, f"Unsupported discovery option: {key}."
    return True, options, ""


def normalize_discovery_lanes(lanes: Any) -> list[str]:
    """Normalize to the approved v1 candidate-discovery lane subset only."""

    normalized = normalize_source_lanes(lanes or DEFAULT_DISCOVERY_LANES)
    return [lane for lane in normalized if lane in _ALLOWED_DISCOVERY_LANES]


def _safe_snippet(text: str, max_length: int = MAX_SNIPPET_LENGTH) -> str:
    cleaned = _DISCORD_MENTION_PATTERN.sub("[redacted-mention]", str(text or ""))
    cleaned = _LONG_ID_PATTERN.sub("[redacted-id]", cleaned)
    cleaned = re.sub(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", "[redacted-email]", cleaned, flags=re.I)
    cleaned = re.sub(r"https?://\S+", "[redacted-url]", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    if len(cleaned) <= max_length:
        return cleaned
    return cleaned[: max(0, max_length - 1)].rstrip() + "…"


def _row_text(row: Any) -> str:
    if isinstance(row, dict):
        return str(row.get("cleaned_summary") or row.get("summary") or row.get("text") or row.get("response_summary") or row.get("user_request") or "")
    if isinstance(row, (list, tuple)):
        if len(row) > 1 and row[1]:
            return str(row[1])
        return " ".join(str(part or "") for part in row)
    return str(row or "")


def _rd_entry_to_text(entry: Any) -> str:
    if isinstance(entry, dict):
        parts = [
            entry.get("main_subject"),
            entry.get("user_request"),
            entry.get("response_summary"),
            entry.get("suggested_lane"),
            entry.get("detected_intent"),
        ]
        return " | ".join(str(part) for part in parts if str(part or "").strip())
    return str(entry or "")


def collect_discovery_source_items(
    guild_id: int | None,
    lanes: Any = None,
    *,
    broadcast_memory_reader: Callable[..., Any] | None = None,
    rd_context_reader: Callable[..., Any] | None = None,
    rd_context: list[Any] | None = None,
    broadcast_limit: int = 500,
    community_presence_reader: Callable[..., Any] | None = None,
) -> list[DiscoverySourceItem]:
    """Collect approved source-safe lane text from explicit readers/context only."""

    requested = normalize_discovery_lanes(lanes or DEFAULT_DISCOVERY_LANES)
    items: list[DiscoverySourceItem] = []

    if "rd_context" in requested:
        rd_rows: list[Any] = list(rd_context or [])
        if callable(rd_context_reader):
            try:
                rd_rows.extend(rd_context_reader() or [])
            except Exception:
                rd_rows.extend([])
        for idx, entry in enumerate(rd_rows[-25:], start=1):
            text = _safe_snippet(_rd_entry_to_text(entry), 500)
            if text:
                items.append(DiscoverySourceItem("rd_context", text, "R&D context", f"rd_context:{idx}", weight=1))

    if COMMUNITY_DISCOVERY_LANE in requested and guild_id and callable(community_presence_reader):
        try:
            rows = community_presence_reader(guild_id, limit=max(1, min(int(broadcast_limit or 1), 500)))
        except Exception:
            rows = []
        for idx, row in enumerate(rows or [], start=1):
            if not isinstance(row, dict):
                continue
            subject = _clean_subject(str(row.get("subjectName") or row.get("displayName") or ""))
            reason = _subject_rejection_reason(subject)
            if reason:
                continue
            mentions = int(row.get("mentionCount") or 0)
            direct = int(row.get("directInteractionCount") or 0)
            operator = int(row.get("operatorMentionCount") or 0)
            days = int(row.get("daysActive") or 0)
            notes = [str(note) for note in (row.get("connectionNotes") or []) if str(note or "").strip()][:3]
            category = str(row.get("category") or "community_regular_candidate")
            context = "recurring approved-channel participant"
            if category == "introduced_subject_candidate" and notes:
                context = notes[0]
            elif operator:
                context = "operator/mod-mentioned community subject"
            elif direct:
                context = "direct BNL interaction in approved community context"
            text = _safe_snippet(
                f"Community presence candidate for {subject}: {context}. "
                f"Signals: mentions {mentions}, direct interactions {direct}, operator mentions {operator}, active windows {days}. "
                "Review only; no identity, alias, relationship, draft, or public dossier is confirmed.",
                500,
            )
            items.append(DiscoverySourceItem(
                COMMUNITY_DISCOVERY_LANE, text, "Community presence", f"community_presence:{idx}",
                weight=2 + min(3, direct + operator),
                metadata={**row, "subjectName": subject, "category": category},
            ))

    if "broadcast_memory" in requested and guild_id and callable(broadcast_memory_reader):
        try:
            rows = broadcast_memory_reader(guild_id, public_only=False, limit=max(1, min(int(broadcast_limit or 1), 500)))
        except Exception:
            rows = []
        for idx, row in enumerate(rows or [], start=1):
            text = _safe_snippet(_row_text(row), 500)
            if text:
                items.append(DiscoverySourceItem("broadcast_memory", text, "Broadcast memory", f"broadcast_memory:{idx}", weight=2))

    return items


def _explicit_subjects(text: str) -> list[str]:
    subjects: list[str] = []
    patterns = (
        rf"\b(?:source file|dossier|alias review|identity review)\s+(?:for|on|about)\s+({_SUBJECT_PHRASE}){_EXPLICIT_SUBJECT_BOUNDARY}",
        rf"\b({_SUBJECT_PHRASE})\s+(?:needs|deserves|should have|should get|may need)\s+(?:a\s+)?(?:source file|dossier|alias review|identity review)\b",
        rf"\b({_SUBJECT_PHRASE})\s+is\s+(?:an?\s+)?(?:recurring|repeated|priority)\s+(?:entity|character|candidate|subject)\b",
    )
    for pattern in patterns:
        for match in re.finditer(pattern, text or "", flags=re.I):
            subjects.append(match.group(1))
    return subjects


def _title_subjects(text: str) -> list[str]:
    candidates = re.findall(r"\b[A-Z][A-Za-z0-9'_-]*(?:\s+[A-Z0-9][A-Za-z0-9'_-]*){0,3}\b", text or "")
    # Also catch compact/camel names like ShadowsPit when they start with a capital.
    candidates.extend(re.findall(r"\b[A-Z][A-Za-z]+[A-Z][A-Za-z0-9]*\b", text or ""))
    return candidates


def _clean_subject(candidate: str) -> str:
    cleaned = re.sub(r"\s+", " ", (candidate or "").strip(" .,:;!?()[]{}\"'"))
    cleaned = re.sub(r"^(?:the|a|an)\s+", "", cleaned, flags=re.I).strip()
    return cleaned[:80]


def _subject_rejection_reason(candidate: str) -> str:
    subject = _clean_subject(candidate)
    normalized = subject.lower()
    if not subject or len(subject) < 3:
        return "invalid_subject_shape"
    if subject in _STOP_PHRASES or normalized in {item.lower() for item in _STOP_PHRASES}:
        return "weak_generic_subject"
    if normalized in _WEAK_SUBJECT_TERMS:
        return "weak_generic_subject"
    words = re.findall(r"[A-Za-z0-9]+", normalized)
    if words and all(word in _WEAK_SUBJECT_TERMS for word in words):
        return "weak_generic_subject"
    if words and words[0] in _WEAK_SUBJECT_TERMS and words[0] not in {"signal"}:
        return "weak_generic_subject"
    if normalized in {"barcode", "barcode radio", "barcode network"}:
        return "weak_generic_subject"
    if re.fullmatch(r"\d+", subject):
        return "invalid_subject_shape"
    if subject.lower().startswith(("do not", "public safe", "source lane", "in ", "on ", "at ", "for ", "with ")):
        return "invalid_subject_shape"
    if not re.search(r"[A-Za-z]", subject):
        return "invalid_subject_shape"
    return ""


def _valid_subject(candidate: str) -> bool:
    return not _subject_rejection_reason(candidate)


def _has_contextual_evidence(acc: CandidateAccumulator) -> bool:
    return acc.contextual_hits > 0 or any(
        re.search(r"\b(recurring|repeated|priority|source file|dossier|candidate|alias review|identity review|entity|character|lore|track|review)\b", item.get("summary") or "", flags=re.I)
        for item in acc.evidence
    )


def _has_strong_evidence(acc: CandidateAccumulator) -> bool:
    if acc.explicit_hits > 0:
        return True
    normalized = acc.subject_name.lower()
    if len(acc.subject_name.split()) == 1 and normalized not in _KNOWN_ONE_WORD_SUBJECTS and not acc.community_signal_count:
        return False
    if acc.mentions < 2 or acc.score < TITLE_ONLY_MIN_SCORE:
        return False
    return _has_contextual_evidence(acc)


def _has_medium_evidence(acc: CandidateAccumulator) -> bool:
    if not acc.evidence or not acc.lanes:
        return False
    normalized = acc.subject_name.lower()
    if len(acc.subject_name.split()) == 1 and normalized not in _KNOWN_ONE_WORD_SUBJECTS and not acc.community_signal_count:
        return False
    if acc.score < MEDIUM_CONTEXT_MIN_SCORE:
        return False
    return acc.mentions >= 2 or _has_contextual_evidence(acc) or acc.community_signal_count >= 1 or acc.direct_interaction_count or acc.operator_mention_count


def _withheld_reason(acc: CandidateAccumulator) -> str:
    if not _valid_subject(acc.subject_name):
        return _subject_rejection_reason(acc.subject_name) or "invalid_subject_shape"
    if not acc.lanes:
        return "unsupported_lane"
    if not acc.evidence:
        return "insufficient_source_context"
    if acc.title_hits and not acc.explicit_hits and not _has_contextual_evidence(acc):
        return "title_only_low_evidence"
    if acc.mentions < 2 and not _has_contextual_evidence(acc):
        return "insufficient_source_context"
    return "below_minimum_score"


def _payload_taxonomy(acc: CandidateAccumulator) -> dict[str, Any]:
    if acc.category in {"community_regular_candidate", "artist_or_collaborator_candidate", "mod_support_candidate", "introduced_subject_candidate", "possible_connection_review", "possible_alias_review"}:
        return community_category_taxonomy(acc.category)
    if acc.category == "identity_alias_review":
        return {"type": "identity_link"}
    if acc.category == "system_concept_candidate":
        return {
            "type": "new_subject",
            "recommendedCategory": "Interface",
            "recommendedKind": "system",
            "recommendedEcosystemLane": "infrastructure",
            "recommendedIdentityAuthority": "mixed_or_unclear",
        }
    return {
        "type": "new_subject",
        "recommendedCategory": "Entity",
        "recommendedIdentityAuthority": "mixed_or_unclear",
    }


def _category_for_text(text: str) -> str:
    for category, pattern in _CATEGORY_TERMS:
        if pattern.search(text or ""):
            return category
    return "new_source_file_candidate"


def _evidence_summary(evidence: list[dict[str, str]]) -> str:
    pieces: list[str] = []
    for item in evidence[:5]:
        lane = item.get("lane") or "source"
        summary = _safe_snippet(item.get("summary") or "", 160)
        if summary:
            pieces.append(f"{lane}: {summary}")
    joined = " | ".join(pieces)
    if len(joined) > MAX_EVIDENCE_SUMMARY_LENGTH:
        return joined[: MAX_EVIDENCE_SUMMARY_LENGTH - 1].rstrip() + "…"
    return joined


def discover_candidate_recommendations(
    guild_id: int | None,
    lanes: Any = None,
    *,
    limit: int = DEFAULT_DISCOVERY_LIMIT,
    broadcast_memory_reader: Callable[..., Any] | None = None,
    rd_context_reader: Callable[..., Any] | None = None,
    rd_context: list[Any] | None = None,
    community_presence_reader: Callable[..., Any] | None = None,
) -> dict[str, Any]:
    """Discover review candidates and return normal recommendation payloads plus safe counts."""

    requested_lanes = normalize_discovery_lanes(lanes or DEFAULT_DISCOVERY_LANES)
    max_candidates = max(1, min(int(limit or DEFAULT_DISCOVERY_LIMIT), MAX_DISCOVERY_LIMIT))
    source_items = collect_discovery_source_items(
        guild_id,
        requested_lanes,
        broadcast_memory_reader=broadcast_memory_reader,
        rd_context_reader=rd_context_reader,
        rd_context=rd_context,
        community_presence_reader=community_presence_reader,
    )
    accumulators: dict[str, CandidateAccumulator] = {}
    withheld_reasons: Counter[str] = Counter()

    if not requested_lanes:
        withheld_reasons["unsupported_lane"] += 1

    for item in source_items:
        text = item.text
        candidate_sources: list[tuple[str, bool]] = []
        if item.lane == COMMUNITY_DISCOVERY_LANE and item.metadata.get("subjectName"):
            candidate_sources.append((_clean_subject(str(item.metadata.get("subjectName") or "")), True))
        for subject in ([] if item.lane == COMMUNITY_DISCOVERY_LANE else _explicit_subjects(text)):
            reason = _subject_rejection_reason(subject)
            if reason:
                withheld_reasons[reason] += 1
                continue
            candidate_sources.append((_clean_subject(subject), True))
        for subject in ([] if item.lane == COMMUNITY_DISCOVERY_LANE else _title_subjects(text)):
            reason = _subject_rejection_reason(subject)
            if reason:
                withheld_reasons[reason] += 1
                continue
            candidate_sources.append((_clean_subject(subject), False))
        seen_in_item: set[str] = set()
        for subject, is_explicit in candidate_sources:
            key = subject_key(subject)
            if key in seen_in_item:
                continue
            seen_in_item.add(key)
            acc = accumulators.get(key)
            if not acc:
                acc = CandidateAccumulator(subject_name=subject)
                accumulators[key] = acc
            acc.mentions += 1
            acc.lanes.add(item.lane)
            if is_explicit:
                acc.explicit_hits += 1
            else:
                acc.title_hits += 1
            category = str(item.metadata.get("category") or _category_for_text(text))
            if acc.category == "new_source_file_candidate" or category != "new_source_file_candidate":
                acc.category = category
            if len(acc.evidence) < 5:
                acc.evidence.append({"lane": item.lane, "label": item.label, "summary": _safe_snippet(text), "sourceRef": item.source_ref})
            acc.score += item.weight + (2 if is_explicit else 0)
            if item.lane == COMMUNITY_DISCOVERY_LANE:
                acc.community_signal_count += int(item.metadata.get("signalCount") or item.metadata.get("mentionCount") or 0)
                acc.direct_interaction_count += int(item.metadata.get("directInteractionCount") or 0)
                acc.operator_mention_count += int(item.metadata.get("operatorMentionCount") or 0)
                acc.days_active = max(acc.days_active, int(item.metadata.get("daysActive") or 0))
                for note in item.metadata.get("connectionNotes") or []:
                    note_text = _safe_snippet(str(note), 120)
                    if note_text and note_text not in acc.connection_notes:
                        acc.connection_notes.append(note_text)
                acc.contextual_hits += 1
                acc.score += min(4, acc.community_signal_count + acc.direct_interaction_count + acc.operator_mention_count + max(0, acc.days_active - 1))
            if re.search(r"\b(recurring|repeated|important|priority|source file|dossier|candidate|alias review|identity review|entity|character|lore|track|review)\b", text, flags=re.I):
                acc.contextual_hits += 1
                acc.score += 1

    candidates: list[dict[str, Any]] = []
    strong_count = 0
    medium_count = 0
    for acc in accumulators.values():
        is_strong = acc.score >= MIN_CANDIDATE_SCORE and _has_strong_evidence(acc)
        is_medium = not is_strong and _has_medium_evidence(acc)
        if not (is_strong or is_medium):
            withheld_reasons[_withheld_reason(acc)] += 1
            continue
        if is_strong:
            strong_count += 1
        else:
            medium_count += 1
        lane_list = [lane for lane in requested_lanes if lane in acc.lanes] or sorted(acc.lanes)
        confidence = "high" if is_strong and acc.score >= 7 and len(acc.lanes) > 1 else ("medium" if acc.score >= 4 else "low")
        if COMMUNITY_DISCOVERY_LANE in acc.lanes:
            if acc.category == "introduced_subject_candidate" and acc.connection_notes:
                reason_text = (
                    f"BNL community scouting found {acc.subject_name} as a subject {acc.connection_notes[0]} in approved community context. "
                    "This is a possible connection, not confirmed identity. Review before creating relationships, aliases, drafts, or public dossiers."
                )
            else:
                reason_text = (
                    f"BNL community scouting found {acc.subject_name} as a recurring approved-channel participant with repeated community presence. "
                    "Review only; owner/admin must decide whether this becomes a Source File, alias proposal, duplicate, or nothing. No public identity or private account claim is made."
                )
        else:
            reason_text = (
                f"BNL dynamic candidate discovery found {acc.subject_name} in approved source-safe lanes "
                f"({', '.join(lane_list)}) as {acc.category.replace('_', ' ')}. Review only; owner/admin must decide whether this becomes a source file, alias proposal, duplicate, or nothing."
            )
        reason = _safe_snippet(reason_text, MAX_REASON_LENGTH)
        evidence_summary = _evidence_summary(acc.evidence)
        evidence_hash = hashlib.sha256(f"{reason}\n{evidence_summary}".encode("utf-8")).hexdigest()[:8]
        taxonomy = _payload_taxonomy(acc)
        cluster_summary = build_recommendation_cluster_summary({
            **taxonomy,
            "subjectName": acc.subject_name,
            "subjectKey": subject_key(acc.subject_name),
            "reason": reason,
            "evidenceSummary": evidence_summary,
            "sourceLanes": lane_list,
            "suggestedAction": DEFAULT_SUGGESTED_ACTION,
            "confidence": confidence,
            "rawEvidenceRefs": [item.get("sourceRef") or item.get("source") or item.get("summary") for item in acc.evidence],
        })
        payload = build_dossier_recommendation_payload(
            {
                **taxonomy,
                "subjectName": acc.subject_name,
                "subjectKey": subject_key(acc.subject_name),
                "reason": reason,
                "evidenceSummary": evidence_summary,
                "sourceLanes": lane_list,
                "suggestedAction": DEFAULT_SUGGESTED_ACTION,
                "missingInfo": list(DEFAULT_MISSING_INFO),
                "publicSafetyNotes": list(DEFAULT_PUBLIC_SAFETY_NOTES) + [
                    f"Internal discovery classification: {acc.category}.",
                    "BNL dynamic discovery is review-only and does not confirm identity, aliases, or publication readiness.",
                ] + ([
                    "Medium-confidence BNL discovery. Review before converting, merging, aliasing, drafting, or publishing."
                ] if is_medium else []),
                "confidence": confidence,
                "createdBy": "bnl",
                "ingestSource": DISCOVERY_INGEST_SOURCE,
                "recommendationClusterSummary": cluster_summary,
                "ingestKey": f"bnl:dossier:{DISCOVERY_INGEST_SOURCE}:{acc.category}:{subject_key(acc.subject_name)}:{'-'.join(lane_list)}:{evidence_hash}",
            }
        )
        if payload.get("recommendedCategory") and payload.get("recommendedCategory") not in VALID_DOSSIER_CATEGORIES:
            payload.pop("recommendedCategory", None)
        candidates.append({"subjectName": acc.subject_name, "category": acc.category, "score": acc.score, "confidenceTier": "strong" if is_strong else "medium", "payload": payload})

    candidates.sort(key=lambda row: (-int(row.get("score") or 0), str(row.get("subjectName") or "").lower()))
    deduped: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    duplicate_count = 0
    for candidate in candidates:
        key = candidate["payload"]["ingestKey"]
        if key in seen_keys:
            duplicate_count += 1
            withheld_reasons["duplicate_internal_candidate"] += 1
            continue
        seen_keys.add(key)
        deduped.append(candidate)
        if len(deduped) >= max_candidates:
            break

    limited_count = max(0, len(candidates) - len(deduped) - duplicate_count)
    if limited_count:
        withheld_reasons["below_minimum_score"] += limited_count
    reason_counts = dict(sorted(withheld_reasons.items()))
    top_withheld_reason = "none"
    if withheld_reasons:
        top_withheld_reason = sorted(withheld_reasons.items(), key=lambda item: (-item[1], item[0]))[0][0]

    return {
        "lanes": requested_lanes,
        "sourceItemCount": len(source_items),
        "candidateCount": len(deduped),
        "sendableCandidateCount": len(deduped),
        "withheldCount": sum(withheld_reasons.values()),
        "withheldReasonCounts": reason_counts,
        "topWithheldReason": top_withheld_reason,
        "mediumConfidenceCount": sum(1 for candidate in deduped if candidate.get("confidenceTier") == "medium"),
        "strongConfidenceCount": sum(1 for candidate in deduped if candidate.get("confidenceTier") == "strong"),
        "duplicateCount": duplicate_count,
        "candidates": deduped,
        "payloads": [candidate["payload"] for candidate in deduped],
    }
