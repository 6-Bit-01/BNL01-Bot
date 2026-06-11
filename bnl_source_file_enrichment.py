"""Review-only Source File enrichment helpers for BNL.

This module builds structured case-file notes from approved local BNL stores and
optionally posts them through the existing review-only dossier recommendation
endpoint. It never publishes, promotes, merges, or overwrites Source Files.
"""

from __future__ import annotations

import hashlib
import inspect
import json
import logging
import re
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Any, Callable

from bnl_dossier_recommendations import build_dossier_recommendation_payload, is_source_file_archive_token_configured, send_dossier_recommendation, send_source_file_archive_enrichment
from bnl_entity_activity_summary import build_entity_activity_summary, refresh_entity_evidence_for_subject
from bnl_entity_evidence import _website_safe_payload_text
from bnl_entity_intelligence import build_entity_intelligence_profile, resolve_entity_context_for_surface, safe_text as _entity_safe_text
from bnl_evidence_ownership import subject_owned_text_fragments
from bnl_source_file_lookup import lookup_source_file
from bnl_source_refresh_context import refresh_generation_context

ENRICHMENT_INGEST_SOURCE = "bnl_source_file_enrichment"
FALLBACK_INGEST_SOURCE = "bnl_source_knowledge_bridge"
SOURCE_BLIND_WARNING = "source-blind memory is review-only; do not treat as confirmed fact"
QUEUE_NOT_CONNECTED_NOTE = "Queue/submission identity is not connected yet."
SITE_EVIDENCE_SUMMARY_LIMIT = 2000
SITE_EVIDENCE_SUMMARY_TARGET = 1800
SITE_RAW_PROVENANCE_JSON_LIMIT = 20000
SITE_RAW_PROVENANCE_JSON_TARGET = 18000
SITE_LIST_ITEM_LIMIT = 500
SITE_LIST_MAX_ITEMS = 25
COMPACT_RECOMMENDATION_LIST_MAX_ITEMS = 8
COMPACT_RECOMMENDATION_LIST_ITEM_LIMIT = 220
COMPACT_RECOMMENDATION_TEXT_FIELD_LIMITS = {
    "reason": 700,
    "evidenceSummary": SITE_EVIDENCE_SUMMARY_TARGET,
    "publicSafetyNotes": 500,
    "doNotSay": 500,
    "missingInfo": 500,
    "suggestedAction": 500,
    "queueSubmissionNote": 500,
    "recentActivitySummary": 500,
    "authoredVsMentionedSummary": 500,
}
COMPACT_RECOMMENDATION_JSON_FIELD_LIMITS = {
    "entityIntelligenceProfile": 3000,
    "rawProvenance": 3000,
}
COMPACT_RECOMMENDATION_ARCHIVE_NOTICE = "Full Source File archive available internally."
SITE_RAW_FRAGMENT_MAX_ITEMS = 4
SITE_RAW_FRAGMENT_TEXT_LIMIT = 500
SITE_SOURCE_COVERAGE_ALLOWED_KEYS = {"source", "count", "counts", "status"}
SITE_SOURCE_COVERAGE_COUNTS_MAX_KEYS = 25
SITE_READOUT_ALLOWED_OBJECT_KEYS = {
    "summary",
    "label",
    "detail",
    "topic",
    "channel",
    "channels",
    "context",
    "status",
    "kind",
    "type",
    "activityType",
    "relationship",
    "visibility",
    "window",
    "recency",
    "frequency",
    "count",
    "counts",
    "postedCount",
    "mentionedCount",
    "publicCount",
    "recentCount",
    "firstSeen",
    "lastSeen",
}
SITE_CONTRACT_READOUT_FIELDS = (
    "representativeEvidence",
    "activityFrequencySummary",
    "topChannels",
    "topTopicDetails",
    "recentActivitySummary",
    "authoredVsMentionedSummary",
    "conversationHighlights",
    "topicBreakdown",
    "evidenceDetails",
    "bestEvidenceToReview",
    "musicSignals",
    "communitySignals",
    "bnlInteractionSignals",
    "sourceCoverage",
)
SITE_BOUND_LIST_FIELDS = (
    "knownContext",
    "usefulEvidence",
    "relationshipSignals",
    "observedChannels",
    "conversationHighlights",
    "topicBreakdown",
    "bestEvidenceToReview",
    "bnlInteractionSignals",
    "musicSignals",
    "communitySignals",
    "evidenceDetails",
    "representativeEvidence",
    "topChannels",
    "topTopicDetails",
    "publicSafePossibilities",
    "publicUseCandidates",
    "privateOnlyNotes",
    "reviewOnlyEvidence",
    "notPublicYet",
    "sourceAuthority",
    "missingInfo",
    "publicSafetyNotes",
    "doNotSay",
)
_PRIVATE_CHANNEL_NAME_RE = re.compile(r"(?i)(?:#?(?:research-and-development|private[-_ ]?internal|private[-_ ]?chat|staff[-_ ]?only|mod[-_ ]?chat|direct[-_ ]?message)s?|\bdm\b)")

_RAW_REPRESENTATIVE_EVIDENCE_RE = re.compile(
    r"(?:\b(?:source(?:_file)?_?lookup|entity_evidence_events|conversations|user_profiles|memory_tiers|broadcast_memory|rd_context|rawRefJson|sourceTable|sourceRowId|sourceLabel)\b|\b(?:row|id)[:=]\s*[A-Za-z0-9_-]+|\b[a-z_]+/[A-Za-z0-9_-]+)",
    re.I,
)
_UNSAFE_REPRESENTATIVE_EVIDENCE_TOKENS = ("authored_public_conversation", "profile_match", "[object Object]")
_SOURCE_TABLE_NAME_RE = re.compile(r"\b(?:entity_evidence_events|conversations|user_profiles|user_memory_facts|user_habits|relationship_state|relationship_journal|memory_tiers|broadcast_memory|community_presence|rd_context|source_file_lookup)\b", re.I)
_RAW_SOURCE_LABEL_RE = re.compile(r"\b(?:sourceLabel|source_label|rawRefJson|sourceRowId|source_row_id|channel_id|user_id)\b|\b[a-z_]+/[A-Za-z0-9_-]+", re.I)
_PATH_LOOKING_RE = re.compile(r"(?:^|\s)(?:[/~]|[A-Za-z]:\\|[\w.-]+[/\\][\w.-]+)")

ENTITY_SUMMARY_EVIDENCE_FIELDS = {
    "observedChannels": 6,
    "conversationHighlights": 6,
    "topicBreakdown": 8,
    "bestEvidenceToReview": 6,
    "bnlInteractionSignals": 5,
    "musicSignals": 5,
    "communitySignals": 5,
    "sourceCoverage": 10,
    "evidenceDetails": 8,
    "representativeEvidence": 6,
    "topChannels": 5,
    "topTopicDetails": 8,
    "activityFrequencySummary": 1,
    "recentActivitySummary": 1,
    "authoredVsMentionedSummary": 1,
    "publicUseCandidates": 6,
    "reviewOnlyEvidence": 6,
}
SECTION_ORDER = [
    "Subject Overview",
    "Why This Subject Matters",
    "Known Roles / Category",
    "History With BARCODE / BNL / Discord / BARCODE Radio",
    "Observed Patterns",
    "Known Facts",
    "Claimed or Inferred Notes",
    "Possible Aliases / Connections",
    "Public-Safe Notes",
    "Internal-Only / Review-Only Notes",
    "Missing Info",
    "Do Not Say",
    "Suggested Next Action",
]
_APPROVED_TABLES = {
    "user_profiles",
    "user_memory_facts",
    "user_habits",
    "relationship_state",
    "relationship_journal",
    "conversations",
    "memory_tiers",
    "broadcast_memory",
    "community_presence",
    "rd_context",
    "source_file_lookup",
}
_DISCORD_MENTION_RE = re.compile(r"<@!?\d+>|<@&\d+>|<#\d+>")
_LONG_ID_RE = re.compile(r"\b\d{15,22}\b")
_URL_RE = re.compile(r"https?://\S+", re.I)
_EMAIL_RE = re.compile(r"\b[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}\b")
_WEAK_ALIAS_RE = re.compile(r"\b(?:alias|known as|aka|connection|connected to|via|through|introduced by)\b", re.I)


ROLE_LABELS = {
    "artist": "Artist",
    "submitter": "Submitter",
    "active_community_member": "Active community member",
    "viewer_listener": "Viewer/listener",
    "moderator": "Moderator",
    "barcode_team": "BARCODE team",
    "barcode_entity": "BARCODE entity",
    "collaborator": "Collaborator",
    "supporter": "Supporter",
    "sponsor": "Sponsor",
    "system": "System",
    "unknown": "Unknown / needs review",
}
ENGAGEMENT_LABELS = {
    "welcome_back": "Welcome-back context",
    "community_followup": "Community follow-up",
    "artist_followup": "Artist follow-up",
    "dossier_review": "Possible future dossier review",
    "existing_dossier_update": "Existing dossier update",
    "relationship_mapping": "Relationship mapping",
    "show_history_context": "Show-history context",
    "moderator_context": "Moderator context",
    "do_not_engage_publicly_yet": "Do not engage publicly yet",
    "archive_if_no_substance": "Archive if no substance",
}
DOSSIER_LABELS = {
    "not_ready": "Not ready",
    "source_file_only": "Source File only",
    "possible_future_dossier": "Possible future dossier",
    "existing_dossier_update": "Existing dossier update",
    "draftable_after_review": "Draftable after owner/admin review",
}
RELATIONSHIP_LABELS = {
    "none": "No relationship read yet",
    "possible_connections": "Possible connections need owner review",
    "known_context": "Known context for review",
    "needs_owner_review": "Needs owner review",
}
TOPIC_LABELS = {
    "music submissions": "Music submissions",
    "production": "Production",
    "BARCODE Radio": "BARCODE Radio",
    "community chat": "Community chat",
    "AI/art": "AI/art",
    "memes/off-topic": "Memes/off-topic",
    "moderation/admin": "Moderation/admin",
    "sponsor/commercial": "Sponsor/commercial",
    "lore/entity discussion": "Lore/entity discussion",
    "unknown/no clear theme": "Unknown/no clear theme",
}

_TOPIC_KEYWORDS = {
    "music submissions": ("submit", "submission", "song", "track", "beat", "demo", "queue"),
    "production": ("mix", "master", "produce", "producer", "fl studio", "ableton", "sample", "vocal"),
    "BARCODE Radio": ("barcode radio", "show", "episode", "broadcast", "live"),
    "AI/art": ("ai", "art", "image", "visual", "generate", "model"),
    "memes/off-topic": ("meme", "lol", "joke", "off topic", "shitpost"),
    "moderation/admin": ("mod", "moderator", "timeout", "ban", "rules", "admin"),
    "sponsor/commercial": ("sponsor", "ad", "commercial", "brand", "promo"),
    "lore/entity discussion": ("lore", "entity", "canon", "character", "protocol", "glitch"),
}

_ROLE_KEYWORDS = {
    "artist": ("artist", "musician", "rapper", "singer", "producer", "dj", "performer"),
    "submitter": ("submitter", "submitted", "submission", "queue submission", "sent a track", "uploaded a track"),
    "moderator": ("moderator", " mod ", "staff role", "admin role"),
    "barcode_team": ("barcode team", "core team", "staff", "crew", "owner"),
    "barcode_entity": ("barcode entity", "lore entity", "character", "fictional entity", "protocol", "glitch virus"),
    "collaborator": ("collaborator", "collaboration", "featured", "worked with", "partner"),
    "supporter": ("supporter", "show support", "community support"),
    "sponsor": ("sponsor", "commercial partner", "brand partner"),
    "system": ("system", "bot", "automation", "service account"),
}


def _contains_any(text: str, needles: tuple[str, ...]) -> bool:
    hay = f" {re.sub(r'[^a-z0-9/+-]+', ' ', (text or '').lower())} "
    return any(needle in hay or needle.strip() in hay for needle in needles)


def _classification_plain_list(values: list[str], labels: dict[str, str]) -> str:
    return ", ".join(labels.get(v, v.replace("_", " ")) for v in values) or "None"


def _extract_topic_themes(texts: list[str], *, approved_activity_count: int = 0) -> list[str]:
    scores: Counter[str] = Counter()
    for text in texts:
        cleaned = str(text or "").lower()
        for theme, keywords in _TOPIC_KEYWORDS.items():
            if any(keyword in cleaned for keyword in keywords):
                scores[theme] += 1
    themes = [theme for theme, count in scores.most_common() if count >= 1]
    if not themes:
        return ["unknown/no clear theme"] if approved_activity_count else []
    # Keep v1 cautious: require repetition for chatter-only themes unless only one clear lane exists.
    repeated = [theme for theme in themes if scores[theme] >= 2]
    return (repeated or themes)[:3]


def classify_entity_activity(subject_identity: dict[str, Any] | None, evidence_bundle: dict[str, Any] | None) -> dict[str, Any]:
    """Convert approved local evidence into cautious internal role/activity intelligence.

    The result is internal review context only. It does not confirm aliases,
    publish dossier content, merge identities, or treat Discord chatter as proof
    of artist/submission/mod/team/sponsor status.
    """

    identity = subject_identity or {}
    bundle = evidence_bundle or {}
    sections = bundle.get("sections") if isinstance(bundle.get("sections"), dict) else {}
    source_counts = Counter(bundle.get("sourceCounts") or {})
    diagnostics = bundle.get("diagnostics") if isinstance(bundle.get("diagnostics"), dict) else {}
    source_file = bundle.get("sourceFile") if isinstance(bundle.get("sourceFile"), dict) else {}
    match_kind = str(bundle.get("matchKind") or identity.get("workflowLane") or "none")
    channel = bundle.get("channelActivity") if isinstance(bundle.get("channelActivity"), dict) else {}
    community = bundle.get("communityActivity") if isinstance(bundle.get("communityActivity"), dict) else {}

    source_text_parts: list[str] = []
    for key in ("candidateType", "sourceType", "type", "kind", "recommendedCategory", "knownFacts", "facts", "evidenceSummary", "reason", "summary", "publicSafetyNotes", "safetyNotes"):
        if source_file.get(key) not in (None, "", []):
            source_text_parts.append(str(source_file.get(key)))
    for name in ("Known Facts", "Public-Safe Notes", "History With BARCODE / BNL / Discord / BARCODE Radio", "Why This Subject Matters"):
        source_text_parts.extend(str(item) for item in sections.get(name, [])[:5])
    source_text = " ".join(source_text_parts)

    roles: list[str] = []
    role_sources: list[str] = []
    for role in ("sponsor", "barcode_team", "barcode_entity", "moderator", "submitter", "artist", "collaborator", "system", "supporter"):
        if _contains_any(source_text, _ROLE_KEYWORDS[role]):
            roles.append(role)
            role_sources.append(role)
    # Artist can be supported by explicit source/submission/collaboration evidence, but never by chatter alone.
    if "artist" not in roles and any(r in roles for r in ("submitter", "collaborator")) and _contains_any(source_text, _ROLE_KEYWORDS["artist"]):
        roles.append("artist")

    approved_count = int(channel.get("approvedMessageCount") or source_counts.get("conversations_by_author") or 0)
    community_count = int(community.get("signalCount") or 0)
    profile_count = int(diagnostics.get("matchedUserProfileCount") or identity.get("matchedUserProfileCount") or 0)
    relationship_count = source_counts.get("relationship_state", 0) + source_counts.get("relationship_journal", 0)
    broadcast_count = source_counts.get("broadcast_memory", 0)
    public_safe_count = len(bundle.get("publicSafeCandidates") or []) + len(sections.get("Public-Safe Notes") or [])

    if approved_count or community_count:
        if "active_community_member" not in roles:
            roles.append("active_community_member")
    elif broadcast_count:
        roles.append("viewer_listener")

    if not roles:
        roles = ["unknown"]
    primary_order = ["sponsor", "barcode_team", "moderator", "barcode_entity", "artist", "submitter", "collaborator", "active_community_member", "viewer_listener", "supporter", "system", "unknown"]
    primary_role = next((role for role in primary_order if role in roles), "unknown")
    secondary_roles = [role for role in roles if role != primary_role]

    non_lookup_refs = sum(v for k, v in source_counts.items() if k not in {"source_file_lookup"})
    day_count = int(channel.get("approvedDayCount") or 0)
    channel_count = int(channel.get("approvedChannelCount") or 0)
    useful_role_evidence = bool(role_sources)
    if non_lookup_refs <= 0 and profile_count <= 0:
        activity_level = "none"
    elif approved_count >= 3 or day_count >= 2 or channel_count >= 2 or community_count >= 3:
        activity_level = "strong" if useful_role_evidence and (approved_count or community_count) else "recurring"
    elif approved_count > 0:
        activity_level = "recent"
    elif non_lookup_refs <= 2 or profile_count:
        activity_level = "thin"
    else:
        activity_level = "recent"

    activity_signals: list[str] = []
    if profile_count:
        activity_signals.append("Local profile match exists; identity and public role still need review.")
    if approved_count:
        signal = "Recurring approved-channel activity found." if activity_level in {"recurring", "strong"} else "Recent approved-channel activity found."
        activity_signals.append(signal)
    if community_count:
        activity_signals.append("Community presence rows support internal follow-up context.")
    if relationship_count:
        activity_signals.append("Relationship/history rows exist for internal context only.")
    if broadcast_count:
        activity_signals.append("BARCODE Radio memory references exist for show-history context.")
    if not activity_signals:
        activity_signals.append("No substantive activity signal found yet.")

    topic_texts = list(channel.get("themeTexts") or []) + list(community.get("themeTexts") or [])
    topic_themes = _extract_topic_themes(topic_texts, approved_activity_count=approved_count + community_count)
    if not topic_themes:
        topic_themes = ["unknown/no clear theme"]

    engagement_use: list[str] = []
    if approved_count or community_count:
        engagement_use.extend(["welcome_back", "community_followup"])
    if primary_role in {"artist", "submitter", "collaborator"}:
        engagement_use.append("artist_followup")
    if match_kind == "existing_dossier_update" or diagnostics.get("existingDossierUpdateLane"):
        engagement_use.append("existing_dossier_update")
    if relationship_count or sections.get("Possible Aliases / Connections"):
        engagement_use.append("relationship_mapping")
    if broadcast_count:
        engagement_use.append("show_history_context")
    if primary_role == "moderator":
        engagement_use.append("moderator_context")
    if primary_role == "unknown" and activity_level in {"none", "thin"}:
        engagement_use.extend(["do_not_engage_publicly_yet", "archive_if_no_substance"])
    if "dossier_review" not in engagement_use and (public_safe_count or useful_role_evidence or match_kind == "existing_dossier_update"):
        engagement_use.append("dossier_review")
    engagement_use = list(dict.fromkeys(engagement_use))

    mostly_metadata = not useful_role_evidence and public_safe_count <= 1 and primary_role in {"unknown", "active_community_member", "viewer_listener"}
    if match_kind == "existing_dossier_update" or diagnostics.get("existingDossierUpdateLane"):
        dossier_use = "existing_dossier_update"
    elif activity_level in {"none", "thin"} and mostly_metadata:
        dossier_use = "not_ready" if primary_role == "unknown" else "source_file_only"
    elif mostly_metadata:
        dossier_use = "source_file_only"
    elif public_safe_count >= 2 and useful_role_evidence:
        dossier_use = "draftable_after_review"
    elif public_safe_count or useful_role_evidence:
        dossier_use = "possible_future_dossier"
    else:
        dossier_use = "source_file_only"

    possible_connection_count = len(sections.get("Possible Aliases / Connections") or [])
    if possible_connection_count:
        relationship_use = "needs_owner_review" if possible_connection_count >= 2 else "possible_connections"
    elif relationship_count:
        relationship_use = "known_context"
    else:
        relationship_use = "none"

    if activity_level in {"strong"} and useful_role_evidence:
        source_confidence = "medium"
    elif activity_level in {"recurring", "recent"} or useful_role_evidence or match_kind == "existing_dossier_update":
        source_confidence = "low-medium"
    else:
        source_confidence = "low"

    missing_info = []
    if primary_role in {"unknown", "active_community_member", "viewer_listener"}:
        missing_info.append("Confirm whether this person is an artist, submitter, collaborator, moderator, BARCODE team/entity, or community-only subject.")
    if dossier_use in {"not_ready", "source_file_only", "possible_future_dossier"}:
        missing_info.append("Confirm public-safe role/context before any dossier language is drafted or updated.")
    if relationship_use != "none":
        missing_info.append("Review possible connections through the existing owner/admin identity workflow before treating them as relationships or aliases.")

    review_warnings = [
        "Internal classification only; not public truth and not an identity merge.",
        "Discord activity alone must not be treated as artist, submitter, moderator, sponsor, team, or collaborator proof.",
    ]
    if topic_themes == ["unknown/no clear theme"] and (approved_count or community_count):
        review_warnings.append("Approved-channel activity exists, but no clear repeated topic/theme has been extracted yet.")
    if primary_role == "supporter":
        review_warnings.append("Supporter status must not be inferred from payment/customer data unless a future safe source explicitly provides it.")

    public_safety_notes = [
        "Do not expose raw transcripts, raw IDs, private/internal notes, or unreviewed aliases in operator-facing output.",
        "Do not publish, auto-create dossiers, auto-confirm aliases, or mutate website content from this classification.",
    ]

    return {
        "primaryRole": primary_role,
        "secondaryRoles": secondary_roles,
        "activityLevel": activity_level,
        "activitySignals": activity_signals[:6],
        "topicThemes": topic_themes,
        "engagementUse": engagement_use,
        "dossierUse": dossier_use,
        "relationshipUse": relationship_use,
        "sourceConfidence": source_confidence,
        "missingInfo": missing_info[:6],
        "reviewWarnings": review_warnings,
        "publicSafetyNotes": public_safety_notes,
        "roleRead": _format_role_read(primary_role, secondary_roles),
        "activityRead": _format_activity_read(activity_level, approved_count, community_count),
        "topicRead": _format_topic_read(topic_themes, approved_count + community_count),
        "engagementRead": _classification_plain_list(engagement_use, ENGAGEMENT_LABELS),
        "dossierRead": _format_dossier_read(dossier_use, primary_role),
        "relationshipRead": RELATIONSHIP_LABELS.get(relationship_use, relationship_use),
    }


def _format_role_read(primary_role: str, secondary_roles: list[str]) -> str:
    if primary_role == "active_community_member":
        extra = "; artist status unconfirmed"
    elif primary_role == "unknown":
        extra = "; needs owner/admin review"
    else:
        extra = ""
    secondary = f"; secondary: {_classification_plain_list(secondary_roles, ROLE_LABELS)}" if secondary_roles else ""
    return f"{ROLE_LABELS.get(primary_role, primary_role)}{extra}{secondary}."


def _format_activity_read(activity_level: str, approved_count: int, community_count: int) -> str:
    if activity_level == "none":
        return "No substantive approved activity found yet."
    if activity_level == "thin":
        return "Only thin review signals are present."
    if activity_level == "recent":
        return "Recent approved-channel activity found."
    if activity_level == "recurring":
        return "Recurring approved-channel activity found."
    return "Strong activity found, with repeated activity plus useful source/role evidence."


def _format_topic_read(topic_themes: list[str], approved_activity_count: int) -> str:
    if topic_themes == ["unknown/no clear theme"] and approved_activity_count:
        return "Approved-channel activity exists, but no clear repeated topic/theme has been extracted yet."
    return _classification_plain_list(topic_themes, TOPIC_LABELS) + "."


def _format_dossier_read(dossier_use: str, primary_role: str) -> str:
    if dossier_use == "source_file_only":
        return "Source File only until owner/admin confirms public-safe role."
    if dossier_use == "not_ready":
        return "Not ready for dossier use; keep as internal review context."
    if dossier_use == "possible_future_dossier":
        return "Possible future dossier review after public-safe context is confirmed."
    if dossier_use == "existing_dossier_update":
        return "Existing dossier update lane for owner/admin review only."
    return "Draftable only after owner/admin review approves public-safe substance."


_ENRICHMENT_QUERY_KEYS = {
    "alias": "alias",
    "candidateid": "candidateId",
    "candidate_id": "candidateId",
    "normalizedname": "normalizedName",
    "normalized_name": "normalizedName",
    "subject": "subject",
}


def parse_source_enrichment_command(content: str) -> tuple[bool, dict[str, Any] | None, str]:
    """Parse operator Source File enrichment commands."""

    text = (content or "").strip()
    match = re.match(
        r"^!bnl\s+(?:(?:source\s+enrich)|(?:enrich\s+source)|(?:source\s+file\s+enrich))\s+(.+)$",
        text,
        flags=re.I | re.S,
    )
    if not match:
        return False, None, "not_a_source_enrichment_command"
    body = match.group(1).strip()
    if not body:
        return True, None, "Use: `!bnl source enrich <subject> [| dry_run=true]`"
    parts = [part.strip() for part in body.split("|")]
    raw_target = re.sub(r"\s+", " ", parts[0]).strip()
    if not raw_target:
        return True, None, "Subject is required."

    lookup_key = "subject"
    lookup_value = raw_target
    key_match = re.match(r"^([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.+)$", raw_target, flags=re.S)
    if key_match:
        requested_key = key_match.group(1).strip().lower()
        canonical_key = _ENRICHMENT_QUERY_KEYS.get(requested_key)
        if not canonical_key:
            return True, None, "Enrichment target must be subject, alias, candidateId, or normalizedName."
        lookup_key = canonical_key
        lookup_value = re.sub(r"\s+", " ", key_match.group(2).strip())
        if not lookup_value:
            return True, None, f"Lookup value for `{canonical_key}` is required."

    options: dict[str, Any] = {
        "subject": lookup_value,
        "lookupKey": lookup_key,
        "lookupValue": lookup_value,
        lookup_key: lookup_value,
        "dry_run": False,
        "force": False,
        "diagnostics": False,
    }
    for extra in parts[1:]:
        if not extra:
            continue
        opt = re.match(r"^([A-Za-z_]+)\s*=\s*(.+)$", extra, flags=re.S)
        if not opt:
            return True, None, "Supported options: dry_run=true|false, force=true|false, diagnostics=true|false."
        key = opt.group(1).strip().lower()
        value = opt.group(2).strip()
        if key in {"dry_run", "dryrun"}:
            options["dry_run"] = value.lower() in {"true", "1", "yes", "on"}
        elif key == "force":
            options["force"] = value.lower() in {"true", "1", "yes", "on"}
        elif key in {"diagnostics", "verbose", "debug"}:
            options["diagnostics"] = value.lower() in {"true", "1", "yes", "on"}
        else:
            return True, None, "Supported options: dry_run=true|false, force=true|false, diagnostics=true|false."
    return True, options, ""


def source_enrichment_ingest_source(environ: dict[str, str] | None = None) -> str:
    """Return the safest ingestSource value for the current site contract."""

    env = environ or {}
    if str(env.get("BNL_SOURCE_ENRICHMENT_INGEST_SUPPORTED") or "").strip().lower() in {"1", "true", "yes", "on"}:
        return ENRICHMENT_INGEST_SOURCE
    return FALLBACK_INGEST_SOURCE


def _subject_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", (value or "").strip().lower()).strip("-") or "unknown-subject"


def _safe_text(value: Any, limit: int = 220) -> str:
    text = str(value or "")
    text = _DISCORD_MENTION_RE.sub("[redacted-mention]", text)
    text = _LONG_ID_RE.sub("[redacted-id]", text)
    text = _EMAIL_RE.sub("[redacted-email]", text)
    text = _URL_RE.sub("[redacted-url]", text)
    text = re.sub(r"\s+", " ", text).strip()
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 1)].rstrip() + "…"


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    if table not in _APPROVED_TABLES:
        return False
    return conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone() is not None


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    if not _table_exists(conn, table):
        return set()
    return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def _like_terms(subject: str) -> list[str]:
    words = [w for w in re.findall(r"[A-Za-z0-9][A-Za-z0-9_-]*", subject or "") if len(w) >= 3]
    terms = [subject.strip()] + words
    out: list[str] = []
    for term in terms:
        lowered = term.lower()
        if lowered not in {x.lower() for x in out}:
            out.append(term[:80])
    return out[:5]


def _matches_subject(text: str, terms: list[str]) -> bool:
    lower = str(text or "").lower()
    return any(term.lower() in lower for term in terms if term)


def _normalize_identity_label(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").strip().lower())


def _identity_label_variants(value: Any) -> set[str]:
    raw = str(value or "").strip()
    if not raw:
        return set()
    variants = {raw, raw.lower(), _subject_key(raw), _normalize_identity_label(raw)}
    for word in re.findall(r"[A-Za-z0-9][A-Za-z0-9_-]*", raw):
        if len(word) >= 3:
            variants.update({word, word.lower(), _normalize_identity_label(word)})
    return {v for v in variants if v}


def _iter_lookup_values(value: Any) -> list[Any]:
    if value in (None, ""):
        return []
    if isinstance(value, dict):
        out: list[Any] = []
        for key in ("name", "label", "alias", "displayName", "display_name", "preferredName", "preferred_name", "subject", "normalizedName", "value", "id", "candidateId", "sourceFileId", "targetDossierId", "dossierId", "publicDossierId"):
            if value.get(key) not in (None, "", []):
                out.append(value.get(key))
        return out
    if isinstance(value, (list, tuple, set)):
        out = []
        for item in value:
            out.extend(_iter_lookup_values(item))
        return out
    text = str(value).strip()
    if not text:
        return []
    # JSON-encoded source-file fields often carry alias arrays or identity-link maps.
    if text[:1] in "[{":
        try:
            parsed = json.loads(text)
        except Exception:
            parsed = None
        if parsed is not None:
            return _iter_lookup_values(parsed)
    return [text]


def _safe_lookup_metadata(obj: dict[str, Any], keys: tuple[str, ...]) -> dict[str, str]:
    meta: dict[str, str] = {}
    for key in keys:
        value = obj.get(key)
        if value not in (None, "", []):
            meta[key] = _safe_text(value, 120)
    return meta


def _lookup_existing_dossier_metadata(lookup_result: dict[str, Any] | None, source_file: dict[str, Any]) -> dict[str, str]:
    data = lookup_result.get("data") if isinstance((lookup_result or {}).get("data"), dict) else {}
    candidates: list[dict[str, Any]] = []
    for obj in (data.get("existingDossierMatch"), data.get("existing_dossier_match"), data.get("publicDossierMatch"), data.get("targetDossier")):
        if isinstance(obj, dict):
            candidates.append(obj)
    if source_file:
        candidates.append(source_file)
    merged: dict[str, str] = {}
    for obj in candidates:
        merged.update(_safe_lookup_metadata(obj, ("existingDossierMatch", "targetDossierId", "dossierId", "publicDossierId", "publicSlug", "slug", "title", "name", "matchKind", "status", "type")))
    return merged


def resolve_enrichment_subject_identity(subject: str, lookup_result: dict[str, Any] | None, db_path: str, guild_id: int | None) -> dict[str, Any]:
    """Resolve a Source File subject to local review-only identity context.

    The returned object is designed for diagnostics and bounded local evidence
    collection. Discord/user identifiers are kept in private keys and are never
    rendered directly in Discord responses or site payload prose.
    """

    source_file = _source_file_obj(lookup_result or {}) if lookup_result else {}
    match_kind, _match_note, _sf = classify_source_match(lookup_result or {}) if lookup_result else ("none", "", {})
    source_name = _first(source_file, ("name", "sourceFileName", "subject", "displayName", "title", "normalizedName")) or subject
    labels: set[str] = set()
    display_labels: list[str] = []

    def add_label(value: Any) -> None:
        for item in _iter_lookup_values(value):
            text = _safe_text(item, 90)
            if not text:
                continue
            for variant in _identity_label_variants(text):
                labels.add(variant)
            if text not in display_labels and not _LONG_ID_RE.fullmatch(text):
                display_labels.append(text)

    add_label(subject)
    add_label(source_name)
    for key in ("aliases", "alias", "matchedAlias", "confirmedAliases", "proposedAliases", "possibleAliases", "identityLinks", "identity_links", "possibleConnections", "normalizedName", "displayName", "preferredName"):
        if lookup_result and isinstance(lookup_result.get("data"), dict):
            add_label(lookup_result["data"].get(key))
        add_label(source_file.get(key))

    matched_user_ids: set[int] = set()
    matched_profile_count = 0
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        if _table_exists(conn, "user_profiles"):
            cols = _columns(conn, "user_profiles")
            select_cols = ["user_id", "display_name"]
            if "preferred_name" in cols:
                select_cols.append("preferred_name")
            rows = conn.execute(f"SELECT {', '.join(select_cols)} FROM user_profiles WHERE guild_id=?", (guild_id,)).fetchall()
            for row in rows:
                haystack = set()
                for col in row.keys():
                    if col == "user_id":
                        continue
                    for variant in _identity_label_variants(row[col]):
                        haystack.add(variant)
                if labels & haystack:
                    try:
                        matched_user_ids.add(int(row["user_id"]))
                    except (TypeError, ValueError):
                        pass
                    matched_profile_count += 1
                    add_label(row["display_name"])
                    if "preferred_name" in row.keys():
                        add_label(row["preferred_name"])
        if _table_exists(conn, "community_presence"):
            cols = _columns(conn, "community_presence")
            select_cols = [c for c in ("display_name", "subject_key", "source_lanes", "connection_notes", "user_id", "discord_user_id", "member_id") if c in cols]
            if select_cols:
                rows = conn.execute(f"SELECT {', '.join(select_cols)} FROM community_presence WHERE guild_id=?", (guild_id,)).fetchall()
                for row in rows:
                    haystack = set()
                    for col in ("display_name", "subject_key", "connection_notes"):
                        if col in row.keys():
                            for variant in _identity_label_variants(row[col]):
                                haystack.add(variant)
                    if labels & haystack:
                        add_label(row["display_name"] if "display_name" in row.keys() else "")
                        for id_col in ("user_id", "discord_user_id", "member_id"):
                            if id_col in row.keys() and row[id_col] not in (None, ""):
                                try:
                                    matched_user_ids.add(int(row[id_col]))
                                except (TypeError, ValueError):
                                    pass
    finally:
        conn.close()

    existing_dossier = _lookup_existing_dossier_metadata(lookup_result, source_file)
    candidate_id = _first(source_file, ("candidateId", "candidate_id", "id", "sourceFileId", "source_file_id"))
    identity = {
        "subjectName": _safe_text(subject, 90),
        "normalizedSubjectName": _subject_key(subject),
        "sourceFileName": _safe_text(source_name, 90),
        "workflowLane": match_kind,
        "sourceFileRecordId": _safe_text(candidate_id, 90) if candidate_id else "",
        "existingDossierMatch": existing_dossier,
        "aliasLabels": display_labels[:12],
        "matchedUserProfileCount": matched_profile_count,
        "matchedUserIdCount": len(matched_user_ids),
        "matchedIdentityLabels": display_labels[:6],
        "publicDossierMatchFound": bool(existing_dossier),
        "existingDossierUpdateLane": match_kind == "existing_dossier_update",
        "_matchedUserIds": matched_user_ids,
        "_matchLabels": labels,
    }
    return identity


def _source_status(checked: dict[str, str], source_type: str, status: str) -> None:
    checked[source_type] = status


def _policy_is_dm_or_private(policy: Any, channel_name: Any = "") -> bool:
    text = f"{policy or ''} {channel_name or ''}".strip().lower()
    return any(token in text for token in ("dm", "direct_message", "private_dm", "private"))


def _policy_bucket(policy: Any, channel_name: Any = "") -> str:
    text = f"{policy or ''} {channel_name or ''}".lower()
    if any(token in text for token in ("operator", "internal", "staff", "mod")):
        return "internal"
    return "public"


def _summarize_channel_activity(subject: str, rows: list[sqlite3.Row]) -> str:
    public_count = 0
    internal_count = 0
    channels: list[str] = []
    direct = mention = general = 0
    for row in rows:
        bucket = _policy_bucket(row["channel_policy"] if "channel_policy" in row.keys() else "", row["channel_name"] if "channel_name" in row.keys() else "")
        if bucket == "internal":
            internal_count += 1
        else:
            public_count += 1
        label = _safe_text((row["channel_name"] if "channel_name" in row.keys() else "") or (row["channel_policy"] if "channel_policy" in row.keys() else "approved channel"), 40)
        if label and label not in channels:
            channels.append(label)
        content = str(row["content"] if "content" in row.keys() else "")
        if _matches_subject(content, _like_terms(subject)):
            mention += 1
        else:
            general += 1
        if "role" in row.keys() and str(row["role"] or "").lower() == "user":
            direct += 1
    parts = []
    if public_count:
        parts.append("public-channel activity")
    if internal_count:
        parts.append("internal/operator-channel activity")
    channel_text = f" Recent approved channels observed: {', '.join(channels[:4])}." if channels else ""
    recurring = "recurring " if len(rows) >= 3 else ""
    interaction = "direct participation" if direct else ("mention context" if mention else "general participation")
    if direct and channels:
        lead = f"{_safe_text(subject, 90)} has {recurring}activity in #{channels[0]}"
    else:
        lead = f"{_safe_text(subject, 90)} has {recurring}prior activity in approved Discord-side channels"
    return f"{lead}. review-only internal context; internal source context, not public dossier copy. no specific public-safe role has been confirmed yet. No meaningful repeated theme has been extracted yet.{channel_text}"


def _entity_summary_has_evidence(summary: dict[str, Any] | None) -> bool:
    return bool(((summary or {}).get("rawProvenance") or {}).get("rawFragments"))


def _append_entity_summary_sections(sections: dict[str, list[str]], summary: dict[str, Any]) -> None:
    for item in (summary.get("knownContext") or [])[:2]:
        if "No approved local" not in str(item):
            _append_section(sections, "Subject Overview", str(item), limit=5)
    for item in (summary.get("activitySignals") or [])[:2] + (summary.get("channelActivity") or [])[:2] + (summary.get("recurringTopics") or [])[:2]:
        _append_section(sections, "Observed Patterns", str(item), limit=8)
    for item in (summary.get("publicSafePossibilities") or [])[:3]:
        if str(item) != "No public-safe facts confirmed yet.":
            safe_item = re.sub(r"owner review", "human review", str(item), flags=re.I)
            _append_section(sections, "Public-Safe Notes", safe_item, limit=6)
    review_items = list(summary.get("relationshipSignals") or []) + list(summary.get("privateOnlyNotes") or []) + list(summary.get("notPublicYet") or [])
    for item in review_items[:5]:
        _append_section(sections, "Internal-Only / Review-Only Notes", str(item), limit=10)
    for item in (summary.get("missingInfo") or [])[:4]:
        _append_section(sections, "Missing Info", str(item), limit=8)
    if summary.get("recommendedAction"):
        _append_section(
            sections,
            "Suggested Next Action",
            "Keep this as internal Source File review material until an owner confirms public-safe identity, role, and wording; queue/submission identity is not connected yet.",
            limit=4,
        )


def _append_section(sections: dict[str, list[str]], name: str, value: str, *, limit: int = 5) -> None:
    clean = _safe_text(value, 240)
    if not clean:
        return
    bucket = sections.setdefault(name, [])
    if clean not in bucket and len(bucket) < limit:
        bucket.append(clean)


def _source_file_obj(lookup_result: dict[str, Any]) -> dict[str, Any]:
    data = lookup_result.get("data") if isinstance(lookup_result.get("data"), dict) else {}
    for key in ("sourceFile", "source_file", "file", "candidate", "result", "dossier"):
        if isinstance(data.get(key), dict):
            return data[key]
    return data if isinstance(data, dict) else {}


def _first(obj: dict[str, Any], keys: tuple[str, ...]) -> Any:
    for key in keys:
        if obj.get(key) not in (None, "", []):
            return obj.get(key)
    return None


def _possible_match_items(lookup_result: dict[str, Any]) -> list[dict[str, str]]:
    data = lookup_result.get("data") if isinstance(lookup_result.get("data"), dict) else {}
    raw = data.get("possibleMatches") or data.get("possible_matches") or data.get("matches") or []
    if isinstance(raw, dict):
        raw = [raw]
    if not isinstance(raw, (list, tuple)):
        return []
    items: list[dict[str, str]] = []
    for item in raw[:10]:
        if isinstance(item, dict):
            name = _safe_text(_first(item, ("name", "sourceFileName", "source_file_name", "subject", "displayName", "title", "normalizedName", "alias")), 90)
            kind = _safe_text(_first(item, ("matchKind", "match_kind", "kind", "reason", "status")) or "possible_match", 80)
            candidate_id = _safe_text(_first(item, ("candidateId", "candidate_id", "id", "sourceFileId", "source_file_id")), 90)
        else:
            name = _safe_text(item, 90)
            kind = "possible_match"
            candidate_id = ""
        if name:
            items.append({"name": name, "matchKind": kind, "candidateId": candidate_id})
    return items


def _is_confirmed_alias_lookup(lookup_result: dict[str, Any]) -> bool:
    data = lookup_result.get("data") if isinstance(lookup_result.get("data"), dict) else {}
    match_text = " ".join(str(x or "") for x in (
        lookup_result.get("matchKind"), data.get("matchKind"), data.get("match_kind"), data.get("aliasStatus"), data.get("alias_status"),
    )).lower()
    return "alias" in match_text and "confirm" in match_text


def _simple_expansion_note(subject: str, matches: list[dict[str, str]]) -> str:
    compact_subject = re.sub(r"[^a-z0-9]", "", (subject or "").lower())
    if not compact_subject:
        return ""
    for item in matches:
        compact_name = re.sub(r"[^a-z0-9]", "", item.get("name", "").lower())
        if compact_name and compact_name != compact_subject and (compact_name.startswith(compact_subject) or compact_name.endswith(compact_subject)):
            return f"This looks like a simple prefix/suffix expansion of “{_safe_text(subject, 60)}”, but it still needs candidateId or exact-name targeting before notes are sent."
    return ""


def _possible_match_review_result(subject: str, lookup_result: dict[str, Any], *, dry_run: bool, resolution_mode: str, match_note: str = "") -> dict[str, Any]:
    matches = _possible_match_items(lookup_result)
    return {
        "ok": True,
        "dryRun": dry_run,
        "subject": _safe_text(subject, 90),
        "status": "possible_match_review",
        "matchKind": "possible_match",
        "matchNote": match_note or "Possible match found, but enrichment needs an exact active Source File target.",
        "possibleMatches": matches,
        "possibleMatchCount": len(matches),
        "possibleMatchNames": [m["name"] for m in matches[:5]],
        "resolutionMode": resolution_mode,
        "sections": {},
        "sourceCounts": {},
        "warningCounts": {},
        "sourceTypes": [],
        "warnings": [],
        "sent": False,
        "sendResult": {},
        "runTime": datetime.now(timezone.utc).isoformat(),
    }


def classify_source_match(lookup_result: dict[str, Any]) -> tuple[str, str, dict[str, Any]]:
    """Classify lookup result as active Source File, intake, existing dossier update, or none."""

    if not lookup_result.get("ok"):
        return "error", _safe_text(lookup_result.get("error") or "lookup failed", 140), {}
    if not lookup_result.get("found"):
        return "none", "No active Source File, Candidate Intake item, or existing dossier target was confirmed.", {}
    obj = _source_file_obj(lookup_result)
    data = lookup_result.get("data") if isinstance(lookup_result.get("data"), dict) else {}
    raw = " ".join(str(x or "") for x in (
        lookup_result.get("matchKind"), data.get("matchKind"), obj.get("status"), obj.get("state"), obj.get("reviewStatus"),
        obj.get("kind"), obj.get("type"), obj.get("candidateType"), obj.get("sourceType"), obj.get("dossierStatus"),
    )).lower()
    if "candidate" in raw or "intake" in raw or "needs_review" in raw or "review" in raw:
        return "candidate_intake", "Candidate Intake enrichment only — not active case-file fact.", obj
    if "dossier" in raw or "public" in raw or obj.get("targetDossierId") or obj.get("dossierId"):
        return "existing_dossier_update", "Existing Dossier Update material for admin review.", obj
    return "active_source_file", "Active Source File enrichment target.", obj



def case_file_lane_label(match_kind: str) -> str:
    return {
        "active_source_file": "Active Source File",
        "candidate_intake": "Candidate Intake",
        "existing_dossier_update": "Existing Dossier Update",
    }.get(match_kind, "Source File review")


def _case_file_overview(subject: str, match_kind: str, source_file: dict[str, Any]) -> str:
    name = _safe_text(_first(source_file, ("name", "sourceFileName", "subject", "displayName", "title", "normalizedName")) or subject, 90)
    if match_kind == "candidate_intake":
        return f"{name} is being reviewed as a Candidate Intake subject. BNL can prepare internal notes, but the public role and whether this should become a public dossier still need owner/admin confirmation."
    if match_kind == "existing_dossier_update":
        return f"{name} appears to map to an existing public dossier update lane. Treat this as internal update material only until an owner approves public-facing wording."
    return f"{name} is already known to the BARCODE Network workflow as a tracked Source File subject. The current record needs human review before any public-facing update is made."


def _normalize_case_category(value: Any, *, inferred: bool = False) -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        label = "unknown"
    elif any(x in raw for x in ("artist", "musician", "producer", "dj")):
        label = "artist"
    elif any(x in raw for x in ("collaborator", "partner")):
        label = "collaborator"
    elif any(x in raw for x in ("community", "participant", "discord", "member")):
        label = "community member"
    elif any(x in raw for x in ("team", "staff", "barcode")):
        label = "BARCODE team"
    elif "sponsor" in raw:
        label = "sponsor"
    elif any(x in raw for x in ("system", "interface", "bot")):
        label = "system"
    elif "character" in raw:
        label = "character"
    elif any(x in raw for x in ("dossier", "public")):
        label = "public dossier subject"
    else:
        label = _safe_text(value, 80)
    suffix = " (inferred; needs owner review)" if inferred and label != "unknown" else ""
    return f"Category: {label}{suffix}."


def _case_memory_fact(key: Any, value: Any, confidence: float, is_core: bool) -> str:
    fact = _safe_text(value, 170)
    label = _safe_text(key, 48)
    if confidence >= 0.75 or is_core:
        return f"Supported local fact ({label}): {fact}."
    return f"Unconfirmed local note ({label}): {fact}. Confidence is limited, so keep this out of Known Facts until reviewed."


def _case_habit_pattern(row: sqlite3.Row) -> str:
    total = int(row["total_messages"] or 0)
    questions = int(row["question_messages"] or 0)
    humor = int(row["humor_messages"] or 0)
    late = int(row["late_night_messages"] or 0)
    topic = _safe_text(row["last_topic"] or "", 90)
    parts = []
    if total:
        parts.append("recurring Discord-side activity" if total >= 5 else "limited Discord-side activity")
    if questions:
        parts.append("asks questions")
    if humor:
        parts.append("uses humor in community exchanges")
    if late:
        parts.append("has some late-night activity")
    summary = ", ".join(parts) or "activity is present but not yet patterned"
    tail = f" Last known topic: {topic}." if topic else ""
    return f"BNL sees {summary}; this is a behavior pattern for review, not a public bio detail.{tail}"


def _case_relationship_history(row: sqlite3.Row) -> str:
    count = int(row["interaction_count"] or 0)
    stance = _safe_text(row["social_stance"] or "", 60)
    topic = _safe_text(row["last_topic"] or "", 90)
    if count >= 3:
        base = "BNL has prior interaction history with this subject"
    else:
        base = "BNL has only limited direct interaction history with this subject"
    qualifiers = []
    if stance:
        qualifiers.append(f"current internal stance reads as {stance}")
    if topic:
        qualifiers.append(f"last recorded topic was {topic}")
    detail = "; ".join(qualifiers)
    return f"{base}, but the exact public-facing significance is not fully established from the available records" + (f" ({detail})." if detail else ".")


def _case_community_presence(row: sqlite3.Row) -> str:
    name = _safe_text(row["display_name"] or "This subject", 80)
    mention = int(row["mention_count"] or 0)
    direct = int(row["direct_interaction_count"] or 0)
    operator = int(row["operator_mention_count"] or 0)
    total = mention + direct + operator
    if total >= 3:
        return f"{name} appears in community-side activity enough to justify keeping a working record, but the role and public-safe summary still need to be confirmed."
    return f"{name} has light community-side signals. Keep this as review context unless additional source notes establish a clearer role."


def _case_missing_info(match_kind: str) -> list[str]:
    items = [
        "Confirm the public role/category before using this outside internal review.",
        "Confirm the relationship to BARCODE, Discord, BARCODE Radio, or the public dossier lane.",
        "Prepare a source-safe summary and owner-approved wording before any public update.",
    ]
    if match_kind == "existing_dossier_update":
        items.append("Decide whether the existing public dossier should actually be updated.")
    elif match_kind == "candidate_intake":
        items.append("Promote only if this is not already covered by an existing dossier.")
    items.append("Confirm possible aliases/connections before attaching them to identity fields.")
    return items


def _case_next_action(subject: str, match_kind: str, source_file: dict[str, Any]) -> str:
    name = _safe_text(subject, 90)
    if match_kind == "existing_dossier_update":
        dossier = _safe_text(_first(source_file, ("targetDossierId", "dossierId", "publicDossierId")) or "the existing dossier", 80)
        return f"Attach to Existing Dossier Update only after owner review confirms {name} belongs with {dossier} and approves public-safe wording."
    if match_kind == "candidate_intake":
        return f"Review {name} as Candidate Intake; promote only if it is not already covered by an existing dossier and the public role can be confirmed."
    return f"Ask the owner/admin to approve a public-safe summary for {name}; archive the enrichment if no useful supported notes remain."


def _has_public_safe_material(sections: dict[str, list[str]]) -> bool:
    return any(item and "no public-safe" not in item.lower() for item in sections.get("Public-Safe Notes") or [])


def evaluate_enrichment_quality(packet: dict[str, Any]) -> dict[str, Any]:
    sections = packet.get("sections") or {}
    counts = Counter(packet.get("sourceCounts") or {})
    non_lookup_sources = sum(v for k, v in counts.items() if k != "source_file_lookup")
    useful_sections = sum(1 for key in ("Known Facts", "History With BARCODE / BNL / Discord / BARCODE Radio", "Observed Patterns", "Public-Safe Notes", "Claimed or Inferred Notes") if sections.get(key))
    score = 0
    if non_lookup_sources:
        score += min(3, non_lookup_sources)
    score += min(3, useful_sections)
    if sections.get("Known Facts"):
        score += 2
    if _has_public_safe_material(sections):
        score += 1
    if sections.get("Internal-Only / Review-Only Notes") and not non_lookup_sources:
        score -= 1
    score = max(0, score)
    status = "sendable" if score >= 3 else "too_thin"
    reason = "" if status == "sendable" else "Enrichment is too thin to send. Add more source notes or confirm the subject’s role first."
    bullets = build_preview_bullets(packet)
    return {"score": score, "status": status, "suppressedReason": reason, "previewBullets": bullets, "previewBulletsCount": len(bullets)}


def build_preview_bullets(packet: dict[str, Any]) -> list[str]:
    sections = packet.get("sections") or {}
    preferred = ("Subject Overview", "Why This Subject Matters", "Known Facts", "History With BARCODE / BNL / Discord / BARCODE Radio", "Observed Patterns", "Suggested Next Action")
    bullets: list[str] = []
    for name in preferred:
        for item in sections.get(name) or []:
            text = _safe_text(item, 180)
            if text and text not in bullets:
                bullets.append(text)
            if len(bullets) >= 3:
                return bullets
    return bullets

def _add_source_file_context(sections: dict[str, list[str]], source_file: dict[str, Any], subject: str, match_kind: str = "active_source_file") -> None:
    if not source_file:
        _append_section(sections, "Subject Overview", _case_file_overview(subject, match_kind, {}))
        return
    _append_section(sections, "Subject Overview", _case_file_overview(subject, match_kind, source_file))
    category = _first(source_file, ("candidateType", "sourceType", "type", "kind", "recommendedCategory"))
    _append_section(sections, "Known Roles / Category", _normalize_case_category(category, inferred=True))
    existing_fact = _first(source_file, ("knownFacts", "facts", "evidenceSummary", "reason", "summary"))
    if existing_fact:
        _append_section(sections, "Known Facts", f"Existing Source File summary says: {_safe_text(existing_fact, 180)}")
    public_notes = _first(source_file, ("publicSafetyNotes", "safetyNotes", "publicUseNotes"))
    if public_notes:
        _append_section(sections, "Public-Safe Notes", f"Existing public-safe note: {_safe_text(public_notes, 180)}")
    missing = _first(source_file, ("missingInfo", "missing"))
    if missing:
        _append_section(sections, "Missing Info", f"Existing record still needs: {_safe_text(missing, 180)}")
    do_not_say = _first(source_file, ("doNotSay", "do_not_say"))
    if do_not_say:
        _append_section(sections, "Do Not Say", f"Existing restriction: {_safe_text(do_not_say, 180)}")
    action = _first(source_file, ("nextRecommendedAction", "suggestedAction", "nextAction"))
    if action:
        _append_section(sections, "Suggested Next Action", f"Existing next action: {_safe_text(action, 180)}")
    aliases = _first(source_file, ("aliases", "identityLinks", "identity_links", "possibleConnections"))
    if aliases:
        _append_section(sections, "Possible Aliases / Connections", f"Possible connection only; needs owner review before identity fields are changed: {_safe_text(aliases, 180)}")


def _owner_admin_review_label(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    return (
        value.replace("pending owner review", "pending owner/admin review")
        .replace("after owner review", "after owner/admin review")
        .replace("before owner review", "before owner/admin review")
    )


def _is_review_only_summary_item(value: Any, review_only_evidence: list[str]) -> bool:
    text = str(value or "")
    lowered = text.lower()
    if "review-only channel" in lowered:
        return True
    if "review-only conversation context" in lowered or "private and internal channel details" in lowered:
        return True
    if "internal/unknown/sealed" in lowered or "internal and unknown and sealed" in lowered:
        return True
    return any(item and item in text for item in review_only_evidence)


def _bounded_entity_summary_fields(summary: dict[str, Any], *, include: bool) -> dict[str, Any]:
    fields: dict[str, Any] = {}
    review_only_evidence = [str(item or "") for item in (summary.get("reviewOnlyEvidence") or [])]
    public_normal_fields = {"observedChannels", "conversationHighlights", "evidenceDetails", "bestEvidenceToReview"}
    for key, limit in ENTITY_SUMMARY_EVIDENCE_FIELDS.items():
        value = summary.get(key) if include else []
        if isinstance(value, list):
            items = list(value)
            if key in public_normal_fields:
                items = [item for item in items if not _is_review_only_summary_item(item, review_only_evidence)]
            items = items[:limit]
            if key == "publicUseCandidates":
                items = [_owner_admin_review_label(item) for item in items]
            fields[key] = items
        elif isinstance(value, dict):
            fields[key] = dict(value)
        elif key in {"recentActivitySummary", "authoredVsMentionedSummary"}:
            fields[key] = str(value or "")
        else:
            fields[key] = value if value else ([] if key != "sourceCoverage" else [])
    return fields


_BLOCKED_SOURCE_PAYLOAD_LABELS = {
    "entire civilizations communicate purely", "not", "the tradeoff", "precision matters", "the value",
    "trap", "hey b", "friday", "pm pacific time", "user", "bit", "bit s", "bit's",
    "sustained analytical input", "not incidental", "are still",
}
_BLOCKED_SOURCE_PAYLOAD_RE = re.compile(r"(?i)(?:\bentire civilizations communicate purely\b|\bsustained analytical input\b|\bnot incidental\b|\bare still\b|\bthe tradeoff\b|\bprecision matters\b|\bthe value\b|\bhey b\b|\bpm pacific time\b|(?:^|[:\s])(?:not|trap|friday|user|bit|bit['’]?s?)(?:$|[.!,;:\s]))")


def _source_payload_item_has_blocked_label(value: Any) -> bool:
    text = json.dumps(value, sort_keys=True, default=str) if isinstance(value, (dict, list)) else str(value or "")
    lowered = re.sub(r"[^a-z0-9']+", " ", text.lower()).strip()
    if lowered in _BLOCKED_SOURCE_PAYLOAD_LABELS:
        return True
    return bool(_BLOCKED_SOURCE_PAYLOAD_RE.search(text))


def _validate_source_payload_fields(container: dict[str, Any]) -> list[dict[str, Any]]:
    rejected: list[dict[str, Any]] = []
    fields = ("topicBreakdown", "bestEvidenceToReview", "relationshipSignals", "publicUseCandidates", "reviewOnlyEvidence", "conversationHighlights", "evidenceDetails", "knownContext", "usefulEvidence")
    for key in fields:
        value = container.get(key)
        if not isinstance(value, list):
            continue
        kept = []
        for item in value:
            if _source_payload_item_has_blocked_label(item):
                rejected.append({"label": _safe_text(item, 120), "kind": key, "accepted": False, "reason": "blocked_source_payload_label", "sourceScope": "payload_validation", "sourceTable": "source_file_enrichment", "evidenceCount": 1})
            else:
                kept.append(item)
        container[key] = kept
    diagnostics = container.setdefault("diagnostics", {}) if isinstance(container.get("diagnostics"), dict) or "diagnostics" not in container else {}
    if rejected and isinstance(diagnostics, dict):
        diagnostics.setdefault("rejectedLabelTraces", []).extend(rejected[:50])
    return rejected


def _copy_evidence_fields(target: dict[str, Any], source: dict[str, Any]) -> None:
    for key in (
        "knownContext",
        "usefulEvidence",
        "relationshipSignals",
        "publicSafePossibilities",
        "privateOnlyNotes",
        "notPublicYet",
        "sourceAuthority",
        "entityIntelligenceProfile",
        *ENTITY_SUMMARY_EVIDENCE_FIELDS.keys(),
    ):
        if key in source:
            value = source.get(key)
            if isinstance(value, list):
                items = list(value)
                if key == "publicUseCandidates":
                    items = [_owner_admin_review_label(item) for item in items]
                target[key] = items
            elif isinstance(value, dict):
                target[key] = dict(value)
            else:
                target[key] = value
    target["queueSubmissionStatus"] = source.get("queueSubmissionStatus") or "not_connected"
    target["queueSubmissionNote"] = source.get("queueSubmissionNote") or QUEUE_NOT_CONNECTED_NOTE
    target["rawProvenance"] = dict(source.get("rawProvenance") or {})
    _validate_source_payload_fields(target)


def collect_source_enrichment_evidence(db_path: str, guild_id: int | None, subject: str, *, rd_context: list[dict[str, Any]] | None = None, lookup_result: dict[str, Any] | None = None) -> dict[str, Any]:
    """Collect bounded evidence from approved local stores only."""

    identity = resolve_enrichment_subject_identity(subject, lookup_result, db_path, guild_id)
    terms = sorted(identity.get("_matchLabels") or _like_terms(subject), key=len, reverse=True)[:20]
    matched_user_ids = set(identity.get("_matchedUserIds") or set())
    sections: dict[str, list[str]] = {name: [] for name in SECTION_ORDER}
    source_counts: Counter[str] = Counter()
    warning_counts: Counter[str] = Counter()
    source_types: set[str] = set()
    source_statuses: dict[str, str] = {}
    skipped_sources: dict[str, str] = {}
    public_safe_candidates: list[str] = []
    review_only_candidates: list[str] = []
    channel_evidence_found = False
    channel_activity_rows = 0
    channel_activity_labels: set[str] = set()
    channel_activity_days: set[str] = set()
    channel_theme_texts: list[str] = []
    community_signal_count = 0
    community_theme_texts: list[str] = []

    if lookup_result:
        match_kind, match_note, sf_obj = classify_source_match(lookup_result)
        source_counts["source_file_lookup"] += 1
        source_types.add("source_file_lookup")
        _source_status(source_statuses, "source_file_lookup", "yes")
        _add_source_file_context(sections, sf_obj, subject, match_kind)
        _append_section(sections, "Internal-Only / Review-Only Notes", f"Targeting note: {match_note}")
        if identity.get("existingDossierUpdateLane") or identity.get("publicDossierMatchFound"):
            _source_status(source_statuses, "public_dossier_match", "yes")
            _append_section(sections, "History With BARCODE / BNL / Discord / BARCODE Radio", f"Existing dossier awareness: {_safe_text(subject, 90)} is connected to an existing public dossier/update lane. Treat enrichment as proposed update material only; public dossier content was not changed.")
            _append_section(sections, "Internal-Only / Review-Only Notes", "Public dossier match metadata was used only for targeting awareness; BNL did not scrape or mutate public dossier content.")
        else:
            _source_status(source_statuses, "public_dossier_match", "no_match")
    else:
        match_kind = "none"
        sf_obj = {}
        skipped_sources["source_file_lookup"] = "no_lookup_result"
        _source_status(source_statuses, "source_file_lookup", "skipped")
        _source_status(source_statuses, "public_dossier_match", "no_match")

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        if _table_exists(conn, "user_profiles"):
            if identity.get("matchedUserIdCount"):
                source_counts["user_profiles"] += int(identity.get("matchedUserProfileCount") or identity.get("matchedUserIdCount") or 1)
                source_types.add("user_profiles")
                _source_status(source_statuses, "user_profiles", "yes")
                labels = ", ".join(identity.get("matchedIdentityLabels") or [subject])
                _append_section(sections, "Subject Overview", f"{_safe_text(labels, 120)} is already present in local BNL profile records. Treat this as a workflow identity link for review, not a public identity claim.")
                _append_section(sections, "Known Roles / Category", _normalize_case_category("community member", inferred=True))
            else:
                _source_status(source_statuses, "user_profiles", "no_match")
        else:
            skipped_sources["user_profiles"] = "table_missing"
            _source_status(source_statuses, "user_profiles", "skipped")

        if _table_exists(conn, "user_memory_facts"):
            rows = conn.execute("SELECT user_id, fact_key, fact_value, confidence, is_core FROM user_memory_facts WHERE guild_id=? ORDER BY updated_at DESC LIMIT 500", (guild_id,)).fetchall()
            matched = 0
            for row in rows:
                text = f"{row['fact_key']}: {row['fact_value']}"
                if int(row["user_id"] or 0) in matched_user_ids or _matches_subject(text, terms):
                    matched += 1
                    source_counts["user_memory_facts"] += 1
                    source_types.add("user_memory_facts")
                    conf = float(row["confidence"] or 0)
                    target = "Known Facts" if conf >= 0.75 or int(row["is_core"] or 0) else "Claimed or Inferred Notes"
                    _append_section(sections, target, _case_memory_fact(row["fact_key"], row["fact_value"], conf, bool(row["is_core"])))
            _source_status(source_statuses, "user_memory_facts", "yes" if matched else "no_match")
        else:
            skipped_sources["user_memory_facts"] = "table_missing"
            _source_status(source_statuses, "user_memory_facts", "skipped")

        if _table_exists(conn, "user_habits"):
            rows = conn.execute("SELECT user_id, total_messages, question_messages, humor_messages, late_night_messages, last_topic FROM user_habits WHERE guild_id=?", (guild_id,)).fetchall()
            matched = 0
            for row in rows:
                if int(row["user_id"] or 0) in matched_user_ids or _matches_subject(row["last_topic"], terms):
                    matched += 1
                    source_counts["user_habits"] += 1
                    source_types.add("user_habits")
                    _append_section(sections, "Observed Patterns", _case_habit_pattern(row))
            _source_status(source_statuses, "user_habits", "yes" if matched else "no_match")
        else:
            skipped_sources["user_habits"] = "table_missing"
            _source_status(source_statuses, "user_habits", "skipped")

        if _table_exists(conn, "relationship_state"):
            rows = conn.execute("SELECT user_id, interaction_count, trust_stage, social_stance, last_topic FROM relationship_state WHERE guild_id=?", (guild_id,)).fetchall()
            matched = 0
            for row in rows:
                if int(row["user_id"] or 0) in matched_user_ids or _matches_subject(row["last_topic"], terms):
                    matched += 1
                    source_counts["relationship_state"] += 1
                    source_types.add("relationship_state")
                    _append_section(sections, "History With BARCODE / BNL / Discord / BARCODE Radio", _case_relationship_history(row))
            _source_status(source_statuses, "relationship_state", "yes" if matched else "no_match")
        else:
            skipped_sources["relationship_state"] = "table_missing"
            _source_status(source_statuses, "relationship_state", "skipped")

        if _table_exists(conn, "relationship_journal"):
            rows = conn.execute("SELECT user_id, entry_type, summary FROM relationship_journal WHERE guild_id=? ORDER BY timestamp DESC LIMIT 300", (guild_id,)).fetchall()
            matched = 0
            for row in rows:
                if int(row["user_id"] or 0) in matched_user_ids or _matches_subject(row["summary"], terms):
                    matched += 1
                    source_counts["relationship_journal"] += 1
                    source_types.add("relationship_journal")
                    _append_section(sections, "History With BARCODE / BNL / Discord / BARCODE Radio", f"Recent BNL relationship note ({_safe_text(row['entry_type'], 40)}): {_safe_text(row['summary'], 180)}")
            _source_status(source_statuses, "relationship_journal", "yes" if matched else "no_match")
        else:
            skipped_sources["relationship_journal"] = "table_missing"
            _source_status(source_statuses, "relationship_journal", "skipped")

        if _table_exists(conn, "conversations"):
            cols = _columns(conn, "conversations")
            select_cols = [c for c in ("user_id", "author_id", "discord_user_id", "member_id", "user_name", "author_name", "channel_name", "channel_policy", "role", "content", "timestamp") if c in cols]
            if select_cols:
                order_col = "timestamp" if "timestamp" in cols else "id" if "id" in cols else "rowid"
                rows = conn.execute(f"SELECT {', '.join(select_cols)} FROM conversations WHERE guild_id=? ORDER BY {order_col} DESC LIMIT 800", (guild_id,)).fetchall()
                matched_rows: list[sqlite3.Row] = []
                mention_rows = 0
                for row in rows:
                    role = str(row["role"] if "role" in row.keys() else "").lower()
                    if role == "model":
                        continue
                    if _policy_is_dm_or_private(row["channel_policy"] if "channel_policy" in row.keys() else "", row["channel_name"] if "channel_name" in row.keys() else ""):
                        continue
                    author_match = False
                    for id_col in ("user_id", "author_id", "discord_user_id", "member_id"):
                        if id_col in row.keys() and row[id_col] not in (None, ""):
                            try:
                                if int(row[id_col]) in matched_user_ids:
                                    author_match = True
                            except (TypeError, ValueError):
                                pass
                    for name_col in ("user_name", "author_name"):
                        if name_col in row.keys() and any(v in terms for v in _identity_label_variants(row[name_col])):
                            author_match = True
                    content_match = _matches_subject(row["content"] if "content" in row.keys() else "", terms)
                    if author_match:
                        matched_rows.append(row)
                    elif content_match:
                        mention_rows += 1
                        source_counts["conversations"] += 1
                        source_types.add("conversations")
                        label = _safe_text((row["channel_policy"] if "channel_policy" in row.keys() else "") or (row["channel_name"] if "channel_name" in row.keys() else "approved channel"), 60)
                        content = row["content"] if "content" in row.keys() else ""
                        note = f"Mentioned in {label} context; review the source before treating this as a fact: {_safe_text(content, 150)}"
                        if _WEAK_ALIAS_RE.search(content or ""):
                            _append_section(sections, "Possible Aliases / Connections", f"Possible connection only; needs owner review and must not be treated as a confirmed alias: {_safe_text(content, 150)}")
                        else:
                            _append_section(sections, "Claimed or Inferred Notes", note)
                        review_only_candidates.append(note)
                if matched_rows:
                    channel_evidence_found = True
                    channel_activity_rows += len(matched_rows)
                    for matched_row in matched_rows:
                        label = _safe_text((matched_row["channel_name"] if "channel_name" in matched_row.keys() else "") or (matched_row["channel_policy"] if "channel_policy" in matched_row.keys() else "approved channel"), 60)
                        if label:
                            channel_activity_labels.add(label)
                        ts = str(matched_row["timestamp"] if "timestamp" in matched_row.keys() else "")
                        if ts:
                            channel_activity_days.add(ts[:10])
                        if "content" in matched_row.keys():
                            channel_theme_texts.append(str(matched_row["content"] or ""))
                    source_counts["conversations_by_author"] += len(matched_rows)
                    source_types.add("conversations_by_author")
                    _append_section(sections, "Observed Patterns", _summarize_channel_activity(subject, matched_rows), limit=6)
                    review_only_candidates.append(_summarize_channel_activity(subject, matched_rows))
                _source_status(source_statuses, "conversations_by_author", "yes" if matched_rows else ("no_identity_match" if not matched_user_ids else "no_match"))
                if mention_rows:
                    _source_status(source_statuses, "conversations", "yes")
                elif "conversations" not in source_statuses:
                    _source_status(source_statuses, "conversations", "no_match")
            else:
                skipped_sources["conversations"] = "no_supported_columns"
                _source_status(source_statuses, "conversations", "skipped")
                _source_status(source_statuses, "conversations_by_author", "skipped")
        else:
            skipped_sources["conversations"] = "table_missing"
            _source_status(source_statuses, "conversations", "skipped")
            _source_status(source_statuses, "conversations_by_author", "skipped")

        if _table_exists(conn, "memory_tiers"):
            cols = _columns(conn, "memory_tiers")
            trust_col = "source_trust" if "source_trust" in cols else "'legacy_unknown'"
            policy_col = "source_channel_policy" if "source_channel_policy" in cols else "'legacy_unknown'"
            rows = conn.execute(f"SELECT tier, summary, salience, {trust_col} AS source_trust, {policy_col} AS source_channel_policy FROM memory_tiers WHERE guild_id=? ORDER BY salience DESC, updated_at DESC LIMIT 500", (guild_id,)).fetchall()
            matched = 0
            for row in rows:
                if _matches_subject(row["summary"], terms):
                    matched += 1
                    source_counts["memory_tiers"] += 1
                    source_types.add("memory_tiers")
                    warning_counts["source_blind_memory"] += 1
                    _append_section(sections, "Internal-Only / Review-Only Notes", f"Review-only legacy memory mentions this subject, but the source is not strong enough for Known Facts: {_safe_text(row['summary'], 160)}")
                    review_only_candidates.append(_safe_text(row["summary"], 160))
            _source_status(source_statuses, "memory_tiers", "yes" if matched else "no_match")
        else:
            skipped_sources["memory_tiers"] = "table_missing"
            _source_status(source_statuses, "memory_tiers", "skipped")

        if _table_exists(conn, "broadcast_memory"):
            rows = conn.execute("SELECT episode_date, cleaned_summary, entry_type, public_safe, usage_scope FROM broadcast_memory WHERE guild_id=? AND status='active' ORDER BY created_at DESC LIMIT 300", (guild_id,)).fetchall()
            matched = 0
            for row in rows:
                text = row["cleaned_summary"]
                if _matches_subject(text, terms):
                    matched += 1
                    source_counts["broadcast_memory"] += 1
                    source_types.add("broadcast_memory")
                    note = f"BARCODE Radio memory from {row['episode_date']} ({row['entry_type']}) mentions this subject: {_safe_text(text, 180)}"
                    _append_section(sections, "History With BARCODE / BNL / Discord / BARCODE Radio", note)
                    if int(row["public_safe"] or 0):
                        _append_section(sections, "Public-Safe Notes", text)
                        public_safe_candidates.append(_safe_text(text, 160))
                    else:
                        _append_section(sections, "Internal-Only / Review-Only Notes", note)
            _source_status(source_statuses, "broadcast_memory", "yes" if matched else "no_match")
        else:
            skipped_sources["broadcast_memory"] = "table_missing"
            _source_status(source_statuses, "broadcast_memory", "skipped")

        if _table_exists(conn, "community_presence"):
            cols = _columns(conn, "community_presence")
            select_cols = [c for c in ("display_name", "subject_key", "source_lanes", "approved_channel_labels", "mention_count", "direct_interaction_count", "operator_mention_count", "connection_notes", "evidence_snippets", "category", "user_id", "discord_user_id", "member_id") if c in cols]
            rows = conn.execute(f"SELECT {', '.join(select_cols)} FROM community_presence WHERE guild_id=?", (guild_id,)).fetchall() if select_cols else []
            matched = 0
            for row in rows:
                row_labels: set[str] = set()
                for col in ("display_name", "subject_key", "connection_notes", "source_lanes"):
                    if col in row.keys():
                        row_labels.update(_identity_label_variants(row[col]))
                identity_match = bool(set(terms) & row_labels)
                for id_col in ("user_id", "discord_user_id", "member_id"):
                    if id_col in row.keys() and row[id_col] not in (None, ""):
                        try:
                            identity_match = identity_match or int(row[id_col]) in matched_user_ids
                        except (TypeError, ValueError):
                            pass
                if identity_match:
                    matched += 1
                    source_counts["community_presence"] += 1
                    source_types.add("community_presence")
                    _append_section(sections, "Observed Patterns", _case_community_presence(row))
                    _append_section(sections, "Known Roles / Category", _normalize_case_category(row["category"] if "category" in row.keys() else "community member", inferred=True))
                    mention = int(row["mention_count"] if "mention_count" in row.keys() and row["mention_count"] is not None else 0)
                    direct = int(row["direct_interaction_count"] if "direct_interaction_count" in row.keys() and row["direct_interaction_count"] is not None else 0)
                    operator = int(row["operator_mention_count"] if "operator_mention_count" in row.keys() and row["operator_mention_count"] is not None else 0)
                    community_signal_count += mention + direct + operator
                    for text_col in ("connection_notes", "evidence_snippets", "category"):
                        if text_col in row.keys() and row[text_col]:
                            community_theme_texts.append(str(row[text_col] or ""))
                    if "connection_notes" in row.keys() and row["connection_notes"]:
                        _append_section(sections, "Possible Aliases / Connections", f"Possible community connection only; needs owner review before it is treated as an identity link: {_safe_text(row['connection_notes'], 180)}")
            _source_status(source_statuses, "community_presence", "yes" if matched else "no_match")
        else:
            skipped_sources["community_presence"] = "table_missing"
            _source_status(source_statuses, "community_presence", "skipped")
    finally:
        conn.close()

    for item in rd_context or []:
        text = " ".join(str(item.get(k) or "") for k in ("summary", "content", "text", "reason", "title")) if isinstance(item, dict) else str(item)
        if _matches_subject(text, terms):
            source_counts["rd_context"] += 1
            source_types.add("rd_context")
            _source_status(source_statuses, "rd_context", "yes")
            _append_section(sections, "Why This Subject Matters", f"R&D context gives BNL a reason to review this subject: {_safe_text(text, 160)}")
            review_only_candidates.append(_safe_text(text, 160))
    if "rd_context" not in source_statuses:
        _source_status(source_statuses, "rd_context", "no_match" if rd_context else "not_provided")

    refresh_result: dict[str, Any] = {}
    try:
        refresh_result = refresh_entity_evidence_for_subject(db_path, subject, guild_id=guild_id, limit=50)
    except Exception as exc:  # pragma: no cover - defensive: enrichment should still read existing evidence.
        refresh_result = {"ok": False, "error": _safe_text(str(exc), 180)}

    entity_summary = build_entity_activity_summary(
        db_path,
        subject,
        guild_id,
        [
            "entity_evidence_events",
            "user_profiles",
            "user_memory_facts",
            "user_habits",
            "relationship_state",
            "relationship_journal",
            "memory_tiers",
            "conversations",
            "broadcast_memory",
            "community_presence",
            "rd_context",
        ],
        "admin_internal",
        50,
        rd_context,
    )
    raw_provenance = entity_summary.get("rawProvenance") if _entity_summary_has_evidence(entity_summary) else {}
    if raw_provenance:
        _append_entity_summary_sections(sections, entity_summary)
        source_counts["entity_activity_summary"] += len(raw_provenance.get("rawFragments") or [])
        source_types.add("entity_activity_summary")
        _source_status(source_statuses, "entity_activity_summary", "yes")
    else:
        _source_status(source_statuses, "entity_activity_summary", "no_match")

    if not sections["Why This Subject Matters"] and (sum(source_counts.values()) > source_counts.get("source_file_lookup", 0)):
        _append_section(sections, "Why This Subject Matters", "BNL has enough local review signals to keep this subject in the working record; admins should decide what, if anything, belongs in a public-safe Source File summary.")
    if not sections["Subject Overview"]:
        _append_section(sections, "Subject Overview", f"{subject} is currently thin. BNL can identify this as a tracked workflow subject, but the public-facing role and useful update angle still need owner/admin confirmation.")
    if not sections["Public-Safe Notes"]:
        _append_section(sections, "Public-Safe Notes", "No public-safe summary is ready yet; use this only as internal review material until wording is approved.")
    if not sections["Missing Info"]:
        for item in _case_missing_info(match_kind):
            _append_section(sections, "Missing Info", item)
    if not sections["Do Not Say"]:
        _append_section(sections, "Do Not Say", "Do not present review-only memory, possible aliases, private identity, raw private transcripts, or source-blind notes as confirmed public fact.")
    if not sections["Suggested Next Action"]:
        _append_section(sections, "Suggested Next Action", _case_next_action(subject, match_kind, sf_obj if lookup_result else {}))

    safe_identity = {k: v for k, v in identity.items() if not k.startswith("_")}
    diagnostics = {
        "sourceTypesChecked": source_statuses,
        "sourceTypesSkipped": skipped_sources,
        "matchedUserProfileCount": int(identity.get("matchedUserProfileCount") or 0),
        "matchedUserIdCount": int(identity.get("matchedUserIdCount") or 0),
        "matchedIdentityLabels": list(identity.get("matchedIdentityLabels") or [])[:6],
        "channelEvidenceFound": channel_evidence_found,
        "publicDossierMatchFound": bool(identity.get("publicDossierMatchFound")),
        "existingDossierUpdateLane": bool(identity.get("existingDossierUpdateLane")),
        "entityEvidenceRefresh": refresh_result if isinstance(refresh_result, dict) else {},
    }
    classification_bundle = {
        "sections": {name: values for name, values in sections.items() if values},
        "sourceCounts": dict(source_counts),
        "diagnostics": diagnostics,
        "sourceFile": sf_obj if lookup_result else {},
        "matchKind": match_kind,
        "publicSafeCandidates": public_safe_candidates[:5],
        "channelActivity": {
            "approvedMessageCount": channel_activity_rows,
            "approvedChannelCount": len(channel_activity_labels),
            "approvedDayCount": len(channel_activity_days),
            "themeTexts": channel_theme_texts[:25],
        },
        "communityActivity": {
            "signalCount": community_signal_count,
            "themeTexts": community_theme_texts[:25],
        },
    }
    classification = classify_entity_activity(safe_identity, classification_bundle)
    diagnostics.update({
        "classificationPrimaryRole": classification["primaryRole"],
        "classificationActivityLevel": classification["activityLevel"],
        "classificationDossierUse": classification["dossierUse"],
        "classificationConfidence": classification["sourceConfidence"],
        "classificationMissingInfoCount": len(classification.get("missingInfo") or []),
    })
    return {
        "sections": {name: values for name, values in sections.items() if values},
        "sourceCounts": dict(source_counts),
        "warningCounts": dict(warning_counts),
        "sourceTypes": sorted(source_types),
        "warnings": ([SOURCE_BLIND_WARNING] if warning_counts.get("source_blind_memory") else []),
        "publicSafeCandidates": public_safe_candidates[:5],
        "reviewOnlyCandidates": review_only_candidates[:5],
        "subjectIdentity": safe_identity,
        "diagnostics": diagnostics,
        "classification": classification,
        "knownContext": list(entity_summary.get("knownContext") or [])[:6] if raw_provenance else [],
        "usefulEvidence": list(entity_summary.get("usefulEvidence") or [])[:6] if raw_provenance else [],
        "relationshipSignals": list(entity_summary.get("relationshipSignals") or [])[:4] if raw_provenance else [],
        "publicSafePossibilities": list(entity_summary.get("publicSafePossibilities") or [])[:4] if raw_provenance else [],
        "privateOnlyNotes": list(entity_summary.get("privateOnlyNotes") or [])[:6] if raw_provenance else [],
        "notPublicYet": list(entity_summary.get("notPublicYet") or [])[:6] if raw_provenance else [],
        "sourceAuthority": list(entity_summary.get("sourceAuthority") or [])[:5] if raw_provenance else [],
        "subjectIntelligenceDiagnostics": entity_summary.get("subjectIntelligenceDiagnostics") or {},
        "linkSignalDiagnostics": entity_summary.get("linkSignalDiagnostics") or {},
        **_bounded_entity_summary_fields(entity_summary, include=bool(raw_provenance)),
        "queueSubmissionStatus": str(entity_summary.get("queueSubmissionStatus") or "not_connected") if raw_provenance else "not_connected",
        "queueSubmissionNote": str(entity_summary.get("queueSubmissionNote") or QUEUE_NOT_CONNECTED_NOTE) if raw_provenance else QUEUE_NOT_CONNECTED_NOTE,
        "rawProvenance": {
            "sourceLabels": list(raw_provenance.get("sourceLabels") or []),
            "sourceCounts": dict(raw_provenance.get("sourceCounts") or {}),
            "rawFragments": list(raw_provenance.get("rawFragments") or [])[:8],
            "channelPolicyCounts": dict(raw_provenance.get("channelPolicies") or {}),
            "sourceAuthority": list(entity_summary.get("sourceAuthority") or [])[:5],
        } if raw_provenance else {},
    }

def _section_text(sections: dict[str, list[str]], name: str, *, limit: int = 4) -> str:
    items = sections.get(name) or []
    return "\n".join(f"- {_safe_text(item, 240)}" for item in items[:limit]) or "- currently unknown."


def _site_safe_text(value: Any, limit: int = SITE_LIST_ITEM_LIMIT) -> str:
    text = _safe_text(value, limit)
    text = _website_safe_payload_text(text)
    text = text.replace("authored_public_conversation", "authored public conversation classification")
    text = text.replace("mentioned_public_conversation", "mentioned public conversation classification")
    text = text.replace("profile_match", "local profile match classification")
    text = re.sub(r"\brow\(s\)", "item(s)", text, flags=re.I)
    text = re.sub(r"\brows\b", "items", text, flags=re.I)
    text = _PRIVATE_CHANNEL_NAME_RE.sub("[private-channel]", text)
    return text.replace("[object Object]", "[unsupported-object]")


def _compact_representative_evidence_item_for_site(value: Any) -> str:
    text = _site_safe_text(value, SITE_LIST_ITEM_LIMIT)
    channel = "approved public-side channel"
    body = text
    if ":" in text:
        maybe_channel, body = text.split(":", 1)
        if maybe_channel.strip().startswith("#"):
            channel = maybe_channel.strip()
    body = body.strip().rstrip(".")
    lowered = body.lower()
    if "classified under" in lowered and "review before" in lowered and "/" not in body:
        return f"{channel}: {body}." if channel not in text[: len(channel) + 1] else text
    match = re.search(r"(?:authored|mentioned)?\s*([A-Za-z0-9 -]+(?: and |, or )[A-Za-z0-9 ,or-]+)(?: classification)?(?: discussion)?", body, flags=re.I)
    if match:
        topic = _website_safe_payload_text(match.group(1)).removesuffix(" classification")
        if topic.lower().startswith("bnl source-file and dossier"):
            return f"{channel}: BNL classified this approved public-context item under {topic} context; review before treating it as a subject claim."
        return f"{channel}: approved public-context item classified under {topic}; review before using as a subject claim."
    if "/" in text or "authored" in lowered or "mentioned" in lowered or _RAW_REPRESENTATIVE_EVIDENCE_RE.search(text):
        return f"{channel}: approved public-context item has a BNL classification; review before using as a subject claim."
    return text


def _compact_value_for_site(value: Any, *, text_limit: int = SITE_LIST_ITEM_LIMIT, depth: int = 0) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        return _site_safe_text(value, text_limit)
    if isinstance(value, (int, float, bool)):
        return value
    if isinstance(value, dict):
        if depth >= 3:
            return _site_safe_text(json.dumps(value, sort_keys=True, default=str), text_limit)
        compact: dict[str, Any] = {}
        for key, raw in value.items():
            safe_key = _site_safe_text(key, 80)
            nested_limit = min(text_limit, SITE_RAW_FRAGMENT_TEXT_LIMIT) if key in {"content", "message", "raw", "transcript", "snippet", "rawRefJson", "channel_name", "channelName"} else text_limit
            compact[safe_key] = _compact_value_for_site(raw, text_limit=nested_limit, depth=depth + 1)
        return compact
    if isinstance(value, (list, tuple, set)):
        out = []
        for item in list(value)[:SITE_LIST_MAX_ITEMS]:
            compact_item = _compact_value_for_site(item, text_limit=text_limit, depth=depth + 1)
            if compact_item not in (None, "", [], {}):
                out.append(compact_item)
        return out
    return _site_safe_text(value, text_limit)


def _compact_list_for_site(value: Any, *, max_items: int = SITE_LIST_MAX_ITEMS, item_limit: int = SITE_LIST_ITEM_LIMIT) -> list[Any]:
    raw_items = value if isinstance(value, (list, tuple, set)) else ([value] if value not in (None, "") else [])
    compact: list[Any] = []
    seen: set[str] = set()
    for item in list(raw_items)[:max_items]:
        compact_item = _compact_value_for_site(item, text_limit=item_limit)
        if compact_item in (None, "", [], {}):
            continue
        fingerprint = json.dumps(compact_item, sort_keys=True, default=str) if isinstance(compact_item, (dict, list)) else str(compact_item)
        if fingerprint in seen:
            continue
        seen.add(fingerprint)
        compact.append(compact_item)
    return compact


def _safe_count(value: Any) -> int | float | None:
    if _is_finite_number(value):
        return value
    try:
        if value in (None, ""):
            return None
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if parsed != parsed or parsed in (float("inf"), float("-inf")):
        return None
    return int(parsed) if parsed.is_integer() else parsed


def _first_present(mapping: dict[str, Any], keys: tuple[str, ...]) -> Any:
    for key in keys:
        if key in mapping and mapping.get(key) not in (None, ""):
            return mapping.get(key)
    return None


def _normalize_channel_label_for_site(value: Any) -> str:
    text = _site_safe_text(value, 160).strip()
    if not text or _PRIVATE_CHANNEL_NAME_RE.search(text) or _LONG_ID_RE.search(text) or text.startswith("<#"):
        return "approved public-side channel"
    if text.startswith("#"):
        return text.split()[0]
    return text


def _normalize_source_name_for_site(value: Any) -> str:
    text = _site_safe_text(value, 160).strip()
    mapping = {
        "entity_evidence_events": "entity evidence events",
        "conversations": "conversation evidence",
        "user_profiles": "profile evidence",
        "user_memory_facts": "memory-fact evidence",
        "user_habits": "habit evidence",
        "relationship_state": "relationship-state evidence",
        "relationship_journal": "relationship-journal evidence",
        "memory_tiers": "memory-tier evidence",
        "broadcast_memory": "broadcast memory evidence",
        "community_presence": "community presence evidence",
        "rd_context": "review context evidence",
        "source_file_lookup": "source file lookup evidence",
    }
    if text in mapping:
        return mapping[text]
    for raw_name, safe_name in mapping.items():
        text = re.sub(rf"\b{re.escape(raw_name)}\b", safe_name, text, flags=re.I)
    return text.replace("_", " ")


def _normalize_topic_label_for_site(value: Any) -> str:
    text = _website_safe_payload_text(_site_safe_text(value, 180))
    text = re.sub(r"\s+", " ", text).strip()
    return text or "local context classification"


def _normalize_allowed_readout_object_for_site(item: dict[str, Any], *, field_name: str) -> dict[str, Any]:
    compact: dict[str, Any] = {}
    for key in SITE_READOUT_ALLOWED_OBJECT_KEYS:
        if key not in item:
            continue
        raw = item.get(key)
        if key in {"count", "postedCount", "mentionedCount", "publicCount", "recentCount"}:
            count = _safe_count(raw)
            if count is not None:
                compact[key] = count
        elif key == "counts" and isinstance(raw, dict):
            counts: dict[str, int | float] = {}
            for count_key, count_value in list(raw.items())[:SITE_SOURCE_COVERAGE_COUNTS_MAX_KEYS]:
                count = _safe_count(count_value)
                if count is None:
                    continue
                safe_key = _site_safe_text(count_key, 80).replace("/", " and ").replace("_", " ")
                if safe_key:
                    counts[safe_key] = count
            if counts:
                compact[key] = counts
        elif key == "channel":
            compact[key] = _normalize_channel_label_for_site(raw)
        elif key == "channels":
            channels = [_normalize_channel_label_for_site(value) for value in (raw if isinstance(raw, list) else [raw])]
            compact[key] = [channel for channel in channels if channel]
        elif key == "topic":
            compact[key] = _normalize_topic_label_for_site(raw)
        else:
            compact[key] = _site_safe_text(raw, SITE_LIST_ITEM_LIMIT)
    if field_name == "topChannels" and "channel" not in compact:
        channel = _first_present(item, ("channelName", "channel_name", "name", "label"))
        compact["channel"] = _normalize_channel_label_for_site(channel)
    if field_name == "topTopicDetails" and "topic" not in compact:
        topic = _first_present(item, ("topicLabel", "topic_name", "label", "summary"))
        compact["topic"] = _normalize_topic_label_for_site(topic)
    if field_name == "topChannels" and "count" not in compact:
        count = _safe_count(_first_present(item, ("count", "publicCount", "approvedPublicCount", "approvedPublicRows")))
        if count is not None:
            compact["count"] = count
    if field_name == "topTopicDetails" and "count" not in compact:
        count = _safe_count(_first_present(item, ("count", "publicCount", "approvedPublicCount", "approvedPublicRows")))
        if count is not None:
            compact["count"] = count
    return {key: value for key, value in compact.items() if value not in (None, "", [], {})}


def _normalize_activity_frequency_summary_for_site(value: Any) -> Any:
    if not isinstance(value, dict):
        return _site_safe_text(value, SITE_LIST_ITEM_LIMIT) if value not in (None, "") else ""
    posted = _safe_count(_first_present(value, ("postedCount", "approvedPublicAuthoredRows", "approvedPublicPostedRows", "publicPostedCount")))
    mentioned = _safe_count(_first_present(value, ("mentionedCount", "approvedPublicMentionedRows", "publicMentionedCount")))
    review_only = _safe_count(_first_present(value, ("reviewOnlyEvidenceCount", "reviewOnlyCount")))
    recent = _first_present(value, ("lastSeen", "mostRecentObservedAt", "recentAt", "recency"))
    summary_parts: list[str] = []
    if posted is not None:
        summary_parts.append(f"{posted:g} approved public posted items")
    if mentioned is not None:
        summary_parts.append(f"{mentioned:g} approved public mentions")
    if review_only is not None:
        summary_parts.append(f"{review_only:g} review-only evidence items")
    entry: dict[str, Any] = {"summary": "Activity frequency: " + (", ".join(summary_parts) if summary_parts else "approved public activity was summarized") + "."}
    if posted is not None:
        entry["postedCount"] = posted
    if mentioned is not None:
        entry["mentionedCount"] = mentioned
    if recent:
        entry["recentCount"] = 1
        entry["recency"] = f"Most recent observed evidence: {_site_safe_text(recent, 120)}."
    if review_only is not None:
        entry["context"] = f"Review-only evidence count: {review_only:g}."
    return entry


def _normalize_recent_activity_summary_for_site(value: Any) -> str:
    if isinstance(value, dict):
        recent = _first_present(value, ("lastSeen", "mostRecentObservedAt", "recentAt", "recency"))
        if recent:
            return f"Recent activity: most recent observed evidence timestamp is {_site_safe_text(recent, 120)}."
        return _site_safe_text(json.dumps(_normalize_allowed_readout_object_for_site(value, field_name="recentActivitySummary"), sort_keys=True, default=str), SITE_LIST_ITEM_LIMIT)
    return _site_safe_text(value, SITE_LIST_ITEM_LIMIT)


def _normalize_authored_vs_mentioned_summary_for_site(value: Any) -> Any:
    if isinstance(value, dict):
        posted = _safe_count(_first_present(value, ("postedCount", "approvedPublicAuthoredRows", "approvedPublicPostedRows")))
        mentioned = _safe_count(_first_present(value, ("mentionedCount", "approvedPublicMentionedRows")))
        entry: dict[str, Any] = {"summary": "Posted and mentioned balance."}
        if posted is not None:
            entry["postedCount"] = posted
        if mentioned is not None:
            entry["mentionedCount"] = mentioned
        return entry
    text = _site_safe_text(value, SITE_LIST_ITEM_LIMIT)
    text = re.sub(r"\bauthored\b", "posted", text, flags=re.I)
    return text


def _normalize_readout_field_for_site(field_name: str, value: Any) -> Any:
    if field_name == "activityFrequencySummary":
        return _normalize_activity_frequency_summary_for_site(value)
    if field_name == "recentActivitySummary":
        return _normalize_recent_activity_summary_for_site(value)
    if field_name == "authoredVsMentionedSummary":
        return _normalize_authored_vs_mentioned_summary_for_site(value)
    if field_name == "sourceCoverage":
        return _compact_source_coverage_for_site(value)
    if field_name == "representativeEvidence":
        compact_items: list[str] = []
        seen_rep: set[str] = set()
        for item in _compact_list_for_site(value, max_items=SITE_LIST_MAX_ITEMS, item_limit=SITE_LIST_ITEM_LIMIT):
            rep_item = _compact_representative_evidence_item_for_site(item)
            if rep_item and rep_item not in seen_rep:
                compact_items.append(rep_item)
                seen_rep.add(rep_item)
        return compact_items
    raw_items = value if isinstance(value, (list, tuple, set)) else ([value] if value not in (None, "") else [])
    compact_items: list[Any] = []
    seen: set[str] = set()
    for item in list(raw_items)[:SITE_LIST_MAX_ITEMS]:
        compact_item: Any
        if isinstance(item, dict):
            compact_item = _normalize_allowed_readout_object_for_site(item, field_name=field_name)
            if not compact_item:
                compact_item = _site_safe_text(json.dumps(item, sort_keys=True, default=str), SITE_LIST_ITEM_LIMIT)
        else:
            compact_item = _site_safe_text(item, SITE_LIST_ITEM_LIMIT)
        if compact_item in (None, "", [], {}):
            continue
        fingerprint = json.dumps(compact_item, sort_keys=True, default=str) if isinstance(compact_item, (dict, list)) else str(compact_item)
        if fingerprint in seen:
            continue
        seen.add(fingerprint)
        compact_items.append(compact_item)
    return compact_items


def _is_finite_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and value == value and value not in (float("inf"), float("-inf"))


def _compact_source_coverage_counts_for_site(value: Any) -> dict[str, int | float]:
    if not isinstance(value, dict):
        return {}
    compact: dict[str, int | float] = {}
    for key, raw_count in list(value.items())[:SITE_SOURCE_COVERAGE_COUNTS_MAX_KEYS]:
        if not _is_finite_number(raw_count):
            continue
        safe_key = _site_safe_text(key, 120).replace("_", " ")
        if not safe_key:
            continue
        compact[safe_key] = raw_count
    return compact


def _compact_source_coverage_for_site(value: Any) -> list[Any]:
    raw_items = value if isinstance(value, (list, tuple, set)) else ([value] if value not in (None, "") else [])
    compact: list[Any] = []
    seen: set[str] = set()
    for item in list(raw_items)[:SITE_LIST_MAX_ITEMS]:
        compact_item: Any = None
        if isinstance(item, str):
            compact_item = _normalize_source_name_for_site(item)
        elif isinstance(item, dict):
            entry: dict[str, Any] = {}
            if item.get("source") not in (None, ""):
                entry["source"] = _normalize_source_name_for_site(item.get("source"))
            if _is_finite_number(item.get("count")):
                entry["count"] = item.get("count")
            counts = _compact_source_coverage_counts_for_site(item.get("counts"))
            if counts:
                entry["counts"] = counts
            if item.get("status") not in (None, ""):
                entry["status"] = _site_safe_text(item.get("status"), 160)
            compact_item = entry
        else:
            compact_item = _site_safe_text(item, SITE_LIST_ITEM_LIMIT)
        if compact_item in (None, "", {}, []):
            continue
        fingerprint = json.dumps(compact_item, sort_keys=True, default=str) if isinstance(compact_item, (dict, list)) else str(compact_item)
        if fingerprint in seen:
            continue
        seen.add(fingerprint)
        compact.append(compact_item)
    return compact


def _compact_raw_provenance_for_site(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        return {}
    compact: dict[str, Any] = {}
    source_labels = _compact_list_for_site(raw.get("sourceLabels"), max_items=25, item_limit=160)
    if source_labels:
        compact["sourceLabels"] = source_labels
    source_counts = dict(raw.get("sourceCounts") or {})
    if source_counts:
        compact["sourceCounts"] = source_counts
    channel_counts = dict(raw.get("channelPolicyCounts") or raw.get("channelPolicies") or {})
    if channel_counts:
        compact["channelPolicyCounts"] = channel_counts
    source_authority = _compact_list_for_site(raw.get("sourceAuthority"), max_items=10, item_limit=240)
    if source_authority:
        compact["sourceAuthority"] = source_authority
    fragments = []
    for fragment in list(raw.get("rawFragments") or [])[:SITE_RAW_FRAGMENT_MAX_ITEMS]:
        if isinstance(fragment, dict):
            kept = {}
            for key in ("table", "sourceTable", "sourceLabel", "sourceRowId", "visibility", "authority", "confidence", "relationToSubject", "topic", "evidenceKind", "safeSummary", "snippet", "rawRefJson", "channelPolicy"):
                if key in fragment and fragment.get(key) not in (None, ""):
                    limit = SITE_RAW_FRAGMENT_TEXT_LIMIT if key in {"safeSummary", "snippet", "rawRefJson"} else 180
                    kept[key] = _compact_value_for_site(fragment.get(key), text_limit=limit)
            if kept:
                fragments.append(kept)
        else:
            fragments.append(_site_safe_text(fragment, SITE_RAW_FRAGMENT_TEXT_LIMIT))
    if fragments:
        compact["rawFragments"] = fragments
    # Last-resort trim to stay below the website's rawProvenance JSON limit.
    while len(json.dumps(compact, sort_keys=True, default=str)) > SITE_RAW_PROVENANCE_JSON_TARGET and compact.get("rawFragments"):
        compact["rawFragments"] = compact["rawFragments"][:-1]
    if len(json.dumps(compact, sort_keys=True, default=str)) > SITE_RAW_PROVENANCE_JSON_TARGET:
        compact.pop("sourceAuthority", None)
    return compact


def truncate_compact_text_field(value: Any, max_length: int, *, field_name: str = "compact_field") -> str:
    """Deterministically cap compact recommendation text while preserving useful boundaries."""

    text = _site_safe_text(value, max(max_length * 4, max_length))
    original_length = len(text)
    if original_length <= max_length:
        logging.info("compact_recommendation_field_length field=%s original=%s capped=%s truncated=false", field_name, original_length, original_length)
        return text
    suffix = "… [truncated; full Source File archive available internally]"
    budget = max(0, max_length - len(suffix))
    candidate = text[:budget].rstrip()
    boundary = max(candidate.rfind("\n- "), candidate.rfind(". "), candidate.rfind("; "), candidate.rfind("\n"))
    if boundary >= max(80, int(budget * 0.55)):
        candidate = candidate[: boundary + (1 if candidate[boundary:boundary + 1] == "." else 0)].rstrip()
    capped = f"{candidate}{suffix}" if candidate else suffix[-max_length:]
    logging.info("compact_recommendation_field_length field=%s original=%s capped=%s truncated=true", field_name, original_length, len(capped))
    return capped


def _compact_json_value_for_recommendation(value: Any, *, field_name: str, max_json_length: int) -> Any:
    compact = _compact_value_for_site(value, text_limit=COMPACT_RECOMMENDATION_LIST_ITEM_LIMIT)
    original_length = len(json.dumps(value, sort_keys=True, default=str)) if value not in (None, "") else 0
    capped_length = len(json.dumps(compact, sort_keys=True, default=str))
    if capped_length <= max_json_length:
        logging.info("compact_recommendation_field_length field=%s original=%s capped=%s truncated=%s", field_name, original_length, capped_length, str(original_length != capped_length).lower())
        return compact
    if isinstance(compact, dict):
        reduced: dict[str, Any] = {}
        for key in ("subjectKey", "qualityScore", "qualityStatus", "diagnostics", "rowScopeCounts", "sourceCounts", "warningCounts", "summary"):
            if key in compact:
                reduced[key] = compact[key]
        reduced.setdefault("summary", COMPACT_RECOMMENDATION_ARCHIVE_NOTICE)
        compact = reduced
    elif isinstance(compact, list):
        compact = compact[:2]
    capped_length = len(json.dumps(compact, sort_keys=True, default=str))
    if capped_length > max_json_length:
        compact = {"summary": COMPACT_RECOMMENDATION_ARCHIVE_NOTICE}
        capped_length = len(json.dumps(compact, sort_keys=True, default=str))
    logging.info("compact_recommendation_field_length field=%s original=%s capped=%s truncated=true", field_name, original_length, capped_length)
    return compact


def build_compact_source_file_recommendation_summary(packet: dict[str, Any], payload: dict[str, Any] | None = None, *, archive_id: str | None = None) -> str:
    """Build a short pointer-style card for compact dossier recommendations."""

    source = payload or {}
    subject = str(source.get("subjectName") or packet.get("subject") or "").strip()
    subject_key = _subject_key(subject or source.get("subjectKey") or packet.get("subjectKey") or "unknown-subject")
    recommendation_type = str(source.get("type") or "source_file_review").strip() or "source_file_review"
    confidence = str(source.get("confidence") or "unknown").strip() or "unknown"
    quality_score = packet.get("qualityScore", source.get("qualityScore"))
    quality_status = str(packet.get("qualityStatus") or source.get("qualityStatus") or "unknown").strip() or "unknown"
    match_kind = str(packet.get("matchKind") or "none").strip() or "none"
    source_counts = packet.get("sourceCounts") or source.get("sourceCounts") or {}
    if isinstance(source_counts, dict) and source_counts:
        count_text = ", ".join(f"{_site_safe_text(k, 40)}={int(v or 0) if isinstance(v, (int, float)) else _site_safe_text(v, 20)}" for k, v in list(source_counts.items())[:4])
    else:
        count_text = "no compact counts"
    archive_ref = archive_id or packet.get("archiveId") or source.get("archiveId") or "pending/not returned"
    lines = [
        "Source File compact review pointer.",
        f"- Subject key: {subject_key}",
        f"- Recommendation type: {recommendation_type}",
        f"- Confidence: {confidence}; priority/quality: {quality_status} ({quality_score if quality_score is not None else 'n/a'}).",
        f"- Why review: BNL refreshed Source File intelligence for match target {match_kind}; admin review may update dossier context without publishing or identity-link changes.",
        f"- Compact source counts: {count_text}.",
        f"- Archive id: {_site_safe_text(archive_ref, 160)}.",
        f"- {COMPACT_RECOMMENDATION_ARCHIVE_NOTICE}",
    ]
    return truncate_compact_text_field("\n".join(lines), COMPACT_RECOMMENDATION_TEXT_FIELD_LIMITS["evidenceSummary"], field_name="evidenceSummary")


def sanitize_compact_recommendation_payload(payload: dict[str, Any], *, packet: dict[str, Any] | None = None, archive_id: str | None = None) -> dict[str, Any]:
    """Apply centralized Source File compact recommendation caps before site send."""

    compact = dict(payload or {})
    compact.pop("subjectMemoryPacketV1", None)
    compact.pop("sourceFileCaseReportV1", None)
    packet = packet or {}
    existing_summary = str(compact.get("evidenceSummary") or "")
    pointer_summary = build_compact_source_file_recommendation_summary(packet, compact, archive_id=archive_id)
    if len(existing_summary) <= COMPACT_RECOMMENDATION_TEXT_FIELD_LIMITS["evidenceSummary"] and not archive_id:
        compact["evidenceSummary"] = existing_summary
    else:
        compact["evidenceSummary"] = pointer_summary
    match_kind = str(packet.get("matchKind") or "none")
    compact["reason"] = truncate_compact_text_field(
        f"Review-only Source File compact pointer for subject_key={_subject_key(compact.get('subjectName') or packet.get('subject') or '')}; match target={match_kind}. {COMPACT_RECOMMENDATION_ARCHIVE_NOTICE} No publishing, promotion, merge, or alias confirmation is requested.",
        COMPACT_RECOMMENDATION_TEXT_FIELD_LIMITS["reason"],
        field_name="reason",
    )
    for key, limit in COMPACT_RECOMMENDATION_TEXT_FIELD_LIMITS.items():
        if key in {"reason", "evidenceSummary"}:
            continue
        if key in compact:
            compact[key] = truncate_compact_text_field(compact.get(key), limit, field_name=key)
    for key in SITE_BOUND_LIST_FIELDS:
        if key in compact:
            if key in SITE_CONTRACT_READOUT_FIELDS:
                compact[key] = _normalize_readout_field_for_site(key, compact.get(key))
                if isinstance(compact[key], list):
                    compact[key] = _compact_list_for_site(compact[key], max_items=COMPACT_RECOMMENDATION_LIST_MAX_ITEMS, item_limit=COMPACT_RECOMMENDATION_LIST_ITEM_LIMIT)
                elif isinstance(compact[key], str):
                    compact[key] = truncate_compact_text_field(compact[key], COMPACT_RECOMMENDATION_LIST_ITEM_LIMIT, field_name=key)
            else:
                compact[key] = _compact_list_for_site(compact.get(key), max_items=COMPACT_RECOMMENDATION_LIST_MAX_ITEMS, item_limit=COMPACT_RECOMMENDATION_LIST_ITEM_LIMIT)
    if "entityIntelligenceProfile" in compact:
        profile = compact.get("entityIntelligenceProfile") if isinstance(compact.get("entityIntelligenceProfile"), dict) else {}
        diagnostics = profile.get("diagnostics") if isinstance(profile, dict) else {}
        profile_pointer = {"summary": COMPACT_RECOMMENDATION_ARCHIVE_NOTICE, "subjectKey": profile.get("subjectKey") or _subject_key(compact.get("subjectName") or packet.get("subject") or ""), "diagnostics": diagnostics}
        for keep_key in ("roles", "relationships", "themes", "behaviorPosture", "eventContest", "dossierUsefulness"):
            if keep_key in profile:
                profile_pointer[keep_key] = _compact_list_for_site(profile.get(keep_key), max_items=4, item_limit=120)
        compact["entityIntelligenceProfile"] = _compact_json_value_for_recommendation(
            profile_pointer,
            field_name="entityIntelligenceProfile",
            max_json_length=COMPACT_RECOMMENDATION_JSON_FIELD_LIMITS["entityIntelligenceProfile"],
        )
    if "rawProvenance" in compact:
        compact["rawProvenance"] = _compact_json_value_for_recommendation(compact.get("rawProvenance"), field_name="rawProvenance", max_json_length=COMPACT_RECOMMENDATION_JSON_FIELD_LIMITS["rawProvenance"])
    compact["compactRecommendationNotice"] = COMPACT_RECOMMENDATION_ARCHIVE_NOTICE
    return compact


def build_compact_enrichment_evidence_summary(packet: dict[str, Any]) -> str:
    classification = packet.get("classification") or {}
    sections = packet.get("sections") or {}
    lines: list[str] = ["Source File enrichment review summary."]
    if classification:
        lines.append("## Internal Classification")
        lines.append(
            f"- Role read: {_site_safe_text(classification.get('roleRead') or ROLE_LABELS.get(classification.get('primaryRole'), 'Unknown / needs review'), 120)}"
        )
        lines.append(
            f"- Activity read: {_site_safe_text(classification.get('activityRead') or classification.get('activityLevel') or 'unknown', 120)}; "
            f"dossier={_site_safe_text(classification.get('dossierRead') or DOSSIER_LABELS.get(classification.get('dossierUse'), 'Unknown'), 120)}."
        )
    section_useful: list[Any] = []
    useful_sources = (
        ("Subject Overview", 0),
        ("History With BARCODE / BNL / Discord / BARCODE Radio", 1),
        ("Observed Patterns", 0),
        ("Why This Subject Matters", 0),
        ("Known Facts", 0),
    )
    for section_name, item_index in useful_sources:
        section_items = sections.get(section_name) or []
        if len(section_items) > item_index:
            section_useful.append(section_items[item_index])
    useful = _compact_list_for_site(section_useful or packet.get("usefulEvidence") or packet.get("knownContext") or packet.get("evidenceDetails"), max_items=3, item_limit=220)
    if useful:
        lines.append("Useful evidence:")
        lines.extend(f"- {item if isinstance(item, str) else _site_safe_text(json.dumps(item, sort_keys=True, default=str), 220)}" for item in useful[:3])
    best = _compact_list_for_site(packet.get("bestEvidenceToReview"), max_items=3, item_limit=240)
    if best:
        lines.append("Best evidence to review:")
        lines.extend(f"- {item if isinstance(item, str) else _site_safe_text(json.dumps(item, sort_keys=True, default=str), 240)}" for item in best[:3])
    missing = _compact_list_for_site(packet.get("missingInfo") or sections.get("Missing Info"), max_items=3, item_limit=180)
    action = _section_text(sections, "Suggested Next Action", limit=1)
    lines.append("Missing info / next action:")
    if missing:
        lines.extend(f"- {item if isinstance(item, str) else _site_safe_text(json.dumps(item, sort_keys=True, default=str), 180)}" for item in missing[:3])
    lines.append(_site_safe_text(action, 240))
    summary = "\n".join(lines).strip()
    if len(summary) > SITE_EVIDENCE_SUMMARY_TARGET:
        summary = summary[: SITE_EVIDENCE_SUMMARY_TARGET - 1].rstrip() + "…"
    return summary


def compact_enrichment_payload_for_site(payload: dict[str, Any]) -> dict[str, Any]:
    compact = dict(payload or {})
    compact.pop("subjectMemoryPacketV1", None)
    compact.pop("sourceFileCaseReportV1", None)
    compact["evidenceSummary"] = _site_safe_text(compact.get("evidenceSummary"), SITE_EVIDENCE_SUMMARY_TARGET)
    for key in SITE_BOUND_LIST_FIELDS:
        if key in compact:
            if key in SITE_CONTRACT_READOUT_FIELDS:
                compact[key] = _normalize_readout_field_for_site(key, compact.get(key))
            else:
                compact[key] = _compact_list_for_site(compact.get(key), max_items=SITE_LIST_MAX_ITEMS, item_limit=SITE_LIST_ITEM_LIMIT)
    for key in ("activityFrequencySummary", "recentActivitySummary", "authoredVsMentionedSummary", "sourceCoverage"):
        if key in compact:
            compact[key] = _normalize_readout_field_for_site(key, compact.get(key))
    compact["rawProvenance"] = _compact_raw_provenance_for_site(compact.get("rawProvenance"))
    return compact


def _validate_source_coverage_for_site(source_coverage: Any) -> list[str]:
    issues: list[str] = []
    if not isinstance(source_coverage, list):
        return ["sourceCoverage must be a list"]
    if len(source_coverage) > SITE_LIST_MAX_ITEMS:
        issues.append("sourceCoverage has too many items")
    for item in source_coverage:
        if isinstance(item, str):
            if _readout_text_validation_issue("sourceCoverage", item):
                issues.append("sourceCoverage contains unsafe text")
                break
            continue
        if not isinstance(item, dict):
            issues.append("sourceCoverage contains unsupported item type")
            break
        unsupported = set(item) - SITE_SOURCE_COVERAGE_ALLOWED_KEYS
        if unsupported:
            issues.append("sourceCoverage contains unsupported object keys")
            break
        text = json.dumps(item, sort_keys=True, default=str)
        if _readout_text_validation_issue("sourceCoverage", text):
            issues.append("sourceCoverage contains unsafe text")
            break
        for text_key in ("source", "status"):
            if text_key in item and (not isinstance(item.get(text_key), str) or len(item.get(text_key) or "") > 1000):
                issues.append(f"sourceCoverage {text_key} must be a bounded string")
                break
        if issues and issues[-1].startswith("sourceCoverage "):
            break
        if "count" in item and not _is_finite_number(item.get("count")):
            issues.append("sourceCoverage count must be finite")
            break
        if "counts" in item:
            counts = item.get("counts")
            if not isinstance(counts, dict):
                issues.append("sourceCoverage counts must be an object")
                break
            if len(counts) > SITE_SOURCE_COVERAGE_COUNTS_MAX_KEYS:
                issues.append("sourceCoverage counts has too many keys")
                break
            for count_key, count_value in counts.items():
                count_key_text = str(count_key or "")
                if _readout_text_validation_issue("sourceCoverage counts", count_key_text):
                    issues.append("sourceCoverage counts contains unsafe keys")
                    break
                if not _is_finite_number(count_value):
                    issues.append("sourceCoverage counts values must be finite")
                    break
            if issues and issues[-1].startswith("sourceCoverage counts"):
                break
    return issues



def _readout_text_validation_issue(field_name: str, text: str) -> str | None:
    validation_text = (
        (text or "")
        .replace("Tool/platform", "Tool platform")
        .replace("tool/platform", "tool platform")
        .replace("Queue/submission", "Queue submission")
        .replace("queue/submission", "queue submission")
    )
    if len(text) > 1000:
        return f"{field_name} contains an oversized item"
    if "[object Object]" in text:
        return f"{field_name} contains [object Object]"
    if "/" in validation_text:
        return f"{field_name} contains slash taxonomy or raw path text"
    if _LONG_ID_RE.search(text):
        return f"{field_name} contains raw IDs"
    if _PRIVATE_CHANNEL_NAME_RE.search(text):
        return f"{field_name} contains private channel names"
    if _RAW_SOURCE_LABEL_RE.search(validation_text):
        return f"{field_name} contains raw source labels"
    if _SOURCE_TABLE_NAME_RE.search(text):
        return f"{field_name} contains source table names"
    if _PATH_LOOKING_RE.search(validation_text):
        return f"{field_name} contains raw path-looking strings"
    return None


def _validate_readout_value_for_site(field_name: str, value: Any) -> list[str]:
    issues: list[str] = []
    if field_name in {"activityFrequencySummary", "recentActivitySummary", "authoredVsMentionedSummary"}:
        values = [value]
    else:
        if not isinstance(value, list):
            return [f"{field_name} must be a list"]
        if len(value) > SITE_LIST_MAX_ITEMS:
            issues.append(f"{field_name} has too many items")
        values = list(value)
    for item in values:
        if isinstance(item, dict):
            unsupported = set(item) - SITE_READOUT_ALLOWED_OBJECT_KEYS
            if unsupported:
                issues.append(f"{field_name} contains unsupported object keys: {', '.join(sorted(unsupported))}")
                break
            for count_key in ("count", "postedCount", "mentionedCount", "publicCount", "recentCount"):
                if count_key in item and not _is_finite_number(item.get(count_key)):
                    issues.append(f"{field_name} {count_key} must be finite")
                    break
            if issues and issues[-1].startswith(f"{field_name} "):
                break
            if "counts" in item:
                counts = item.get("counts")
                if not isinstance(counts, dict):
                    issues.append(f"{field_name} counts must be an object")
                    break
                if any(not _is_finite_number(v) for v in counts.values()):
                    issues.append(f"{field_name} counts values must be finite")
                    break
        text = json.dumps(item, sort_keys=True, default=str) if isinstance(item, (dict, list)) else str(item)
        issue = _readout_text_validation_issue(field_name, text)
        if issue:
            issues.append(issue)
            break
    return issues


def _validate_representative_evidence_for_site(value: Any) -> list[str]:
    issues: list[str] = []
    if not isinstance(value, list):
        return ["representativeEvidence must be a list"]
    for item in value:
        text = json.dumps(item, sort_keys=True, default=str) if isinstance(item, (dict, list)) else str(item)
        lowered = text.lower()
        if "/" in text:
            issues.append("representativeEvidence contains slash taxonomy")
            break
        if any(token.lower() in lowered for token in _UNSAFE_REPRESENTATIVE_EVIDENCE_TOKENS):
            issues.append("representativeEvidence contains unsupported raw metadata")
            break
        if _LONG_ID_RE.search(text) or _RAW_REPRESENTATIVE_EVIDENCE_RE.search(text):
            issues.append("representativeEvidence contains unsupported raw metadata")
            break
    return issues

def validate_enrichment_payload_for_site(payload: dict[str, Any]) -> list[str]:
    issues: list[str] = []
    if len(str(payload.get("evidenceSummary") or "")) > SITE_EVIDENCE_SUMMARY_LIMIT:
        issues.append("evidenceSummary exceeds site limit")
    raw_len = len(json.dumps(payload.get("rawProvenance") or {}, sort_keys=True, default=str))
    if raw_len > SITE_RAW_PROVENANCE_JSON_LIMIT:
        issues.append("rawProvenance exceeds site JSON limit")
    if "sourceCoverage" in payload:
        issues.extend(_validate_source_coverage_for_site(payload.get("sourceCoverage")))
    for key in SITE_CONTRACT_READOUT_FIELDS:
        if key in payload and key != "sourceCoverage":
            issues.extend(_validate_readout_value_for_site(key, payload.get(key)))
    if "representativeEvidence" in payload:
        issues.extend(_validate_representative_evidence_for_site(payload.get("representativeEvidence")))
    for key in SITE_BOUND_LIST_FIELDS:
        if key not in payload or key in SITE_CONTRACT_READOUT_FIELDS:
            continue
        value = payload.get(key)
        if not isinstance(value, list):
            issues.append(f"{key} must be a list")
            continue
        if len(value) > SITE_LIST_MAX_ITEMS:
            issues.append(f"{key} has too many items")
        for item in value:
            text = json.dumps(item, sort_keys=True, default=str) if isinstance(item, (dict, list)) else str(item)
            if len(text) > 1000:
                issues.append(f"{key} contains an oversized item")
                break
            issue = _readout_text_validation_issue(key, text)
            if issue:
                issues.append(issue)
                break
    return issues




def _entity_context_labels(items: list[dict[str, Any]], *, label_key: str = "label", limit: int = 8) -> list[str]:
    labels: list[str] = []
    for item in items or []:
        label = _safe_text(item.get(label_key) or item.get("label") or item.get("objectLabel") or "", 180)
        value = _safe_text(item.get("value") or "", 180)
        if label and value and value != label and label_key != "objectLabel":
            text = f"{label}: {value}"
        else:
            text = label
        if text and text not in labels:
            labels.append(text)
        if len(labels) >= limit:
            break
    return labels


def _merge_entity_intelligence_into_evidence(evidence: dict[str, Any], profile: dict[str, Any], resolver: dict[str, Any]) -> None:
    """Map the Entity Intelligence Resolver's source_file surface into existing website-safe fields."""

    roles = _entity_context_labels(resolver.get("allowedRoles") or [], limit=8)
    relationships = []
    for rel in resolver.get("allowedRelationships") or []:
        obj = _safe_text(rel.get("objectLabel") or rel.get("label"), 120)
        relation = _safe_text(rel.get("relationType") or rel.get("value"), 80)
        if obj and relation:
            relationships.append(f"{obj}: {relation}")
    themes = _entity_context_labels(resolver.get("allowedThemes") or [], limit=8)
    activity = _entity_context_labels(resolver.get("allowedActivity") or [], limit=12)
    action_items = _entity_context_labels(resolver.get("allowedActionItems") or [], limit=8)
    warnings = _entity_context_labels(resolver.get("allowedWarnings") or [], limit=6)
    missing = [_safe_text(item, 180) for item in (resolver.get("missingInfo") or []) if _safe_text(item, 180)]

    def add_list(key: str, values: list[str]) -> None:
        bucket = evidence.setdefault(key, [])
        for value in values:
            clean = _safe_text(value, 220)
            if clean and clean not in bucket:
                bucket.append(clean)

    add_list("knownContext", [f"Detected role facet: {r}" for r in roles])
    add_list("relationshipSignals", [f"Detected relationship facet: {r}" for r in relationships])
    add_list("conversationHighlights", [f"Conversation theme: {t}" for t in themes])
    add_list("topicBreakdown", themes)
    music = [a for a in activity if re.search(r"music|suno|udio|youtube|soundcloud|spotify|bandcamp|track|song|demo|wip|collab", a, re.I)]
    bnl = [a for a in activity if re.search(r"BNL|source-file|boundary|challenge", a, re.I)]
    community = [a for a in activity if re.search(r"community|mod|contest|event|dossier|source file", a, re.I)]
    add_list("musicSignals", music)
    add_list("bnlInteractionSignals", bnl)
    add_list("communitySignals", community)
    add_list("bestEvidenceToReview", action_items + warnings)
    add_list("reviewOnlyEvidence", action_items + warnings)
    add_list("missingInfo", missing)
    add_list("usefulEvidence", roles + relationships + themes + activity[:8])
    add_list("evidenceDetails", [f"Entity intelligence surface {resolver.get('surface')}: {item}" for item in (roles + relationships + themes + activity[:8])])
    sections = evidence.setdefault("sections", {})
    if missing:
        section_missing = sections.setdefault("Missing Info", [])
        for item in reversed(missing[:8]):
            if item in section_missing:
                section_missing.remove(item)
            section_missing.insert(0, item)
    if action_items:
        evidence["recommendedAction"] = action_items[0]
        suggested = sections.setdefault("Suggested Next Action", [])
        if action_items[0] not in suggested:
            suggested.insert(0, action_items[0])
    elif missing:
        evidence["recommendedAction"] = f"Confirm missing info: {missing[0]}"
    evidence["queueSubmissionStatus"] = resolver.get("queueSubmissionStatus") or "not_connected"
    evidence["queueSubmissionNote"] = QUEUE_NOT_CONNECTED_NOTE
    coverage = evidence.setdefault("sourceCoverage", [])
    for item in profile.get("sourceCoverage") or []:
        safe_item = {"source": _safe_text(item.get("source"), 80), "count": int(item.get("count") or 0), "status": _safe_text(item.get("status") or "used", 40)}
        if safe_item not in coverage:
            coverage.append(safe_item)
    evidence["entityIntelligenceProfile"] = {
        "subjectKey": _safe_text(profile.get("subjectKey"), 120),
        "subjectName": _safe_text(profile.get("subjectName"), 120),
        "roles": roles[:8],
        "relationships": relationships[:8],
        "creativeMusic": music[:8],
        "bnlInteraction": bnl[:8],
        "behaviorPosture": [a for a in activity if re.search(r"challenging|lore|anomaly|supportive|frequent|occasional", a, re.I)][:8],
        "conversationThemes": themes[:8],
        "eventContest": [a for a in activity if re.search(r"contest|event|rules", a, re.I)][:8],
        "dossierUsefulness": [a for a in activity if re.search(r"dossier|source file|approval|candidate", a, re.I)][:8],
        "missingInfo": missing[:8],
        "officialPublicDossierConnected": bool(resolver.get("officialPublicDossierConnected")),
        "queueSubmissionStatus": resolver.get("queueSubmissionStatus") or "not_connected",
    }
    diagnostics = evidence.setdefault("diagnostics", {})
    diagnostics["entityIntelligence"] = {
        "ledgerStatus": (profile.get("diagnostics") or {}).get("ledgerStatus") or "unknown",
        "profileStatus": (profile.get("diagnostics") or {}).get("profileStatus") or "unknown",
        "resolverSurface": resolver.get("surface") or "source_file",
        "detectedRoleFacets": roles,
        "detectedRelationshipFacets": relationships,
        "creativeMusicFacets": music,
        "behaviorPostureFacets": evidence["entityIntelligenceProfile"]["behaviorPosture"],
        "conversationThemes": themes,
        "eventContestActivity": evidence["entityIntelligenceProfile"]["eventContest"],
        "dossierUsefulness": evidence["entityIntelligenceProfile"]["dossierUsefulness"],
        "modActionItems": action_items,
        "missingInfo": missing,
        "sourceRowsBySource": (profile.get("diagnostics") or {}).get("rowsBySource") or {},
        "rowScopeCounts": (profile.get("diagnostics") or {}).get("rowScopeCounts") or {},
        "extractableRows": int((profile.get("diagnostics") or {}).get("extractableRows") or 0),
        "globalMixedRows": int((profile.get("diagnostics") or {}).get("globalMixedRows") or 0),
        "sourceBlindRows": int((profile.get("diagnostics") or {}).get("sourceBlindRows") or 0),
        "queueSubmissionStatus": (profile.get("diagnostics") or {}).get("queueSubmissionStatus") or "not_connected",
        "publicSafeRows": int((profile.get("diagnostics") or {}).get("publicSafeRows") or 0),
        "reviewOnlyRows": int((profile.get("diagnostics") or {}).get("reviewOnlyRows") or 0),
        "officialPublicDossierConnected": bool((profile.get("diagnostics") or {}).get("officialPublicDossierConnected")),
        "officialPublicDossierNote": (profile.get("diagnostics") or {}).get("officialPublicDossierNote") or "official_public_dossier source not connected yet",
    }
    evidence["subjectIntelligenceDiagnostics"] = dict(evidence.get("subjectIntelligenceDiagnostics") or {})
    evidence["subjectIntelligenceDiagnostics"]["entityIntelligence"] = diagnostics["entityIntelligence"]

def build_enrichment_markdown(packet: dict[str, Any]) -> str:
    sections = packet.get("sections") or {}
    lines: list[str] = []
    classification = packet.get("classification") or {}
    if classification:
        missing = classification.get("missingInfo") or []
        classification_lines = [
            f"- Role read: {_safe_text(classification.get('roleRead') or ROLE_LABELS.get(classification.get('primaryRole'), 'Unknown / needs review'), 240)}",
            f"- Activity read: {_safe_text(classification.get('activityRead') or classification.get('activityLevel') or 'unknown', 240)}",
            f"- Topic read: {_safe_text(classification.get('topicRead') or _classification_plain_list(classification.get('topicThemes') or [], TOPIC_LABELS), 240)}",
            f"- Engagement use: {_safe_text(classification.get('engagementRead') or _classification_plain_list(classification.get('engagementUse') or [], ENGAGEMENT_LABELS), 240)}",
            f"- Dossier use: {_safe_text(classification.get('dossierRead') or DOSSIER_LABELS.get(classification.get('dossierUse'), 'Unknown'), 240)}",
            f"- Relationship use: {_safe_text(classification.get('relationshipRead') or RELATIONSHIP_LABELS.get(classification.get('relationshipUse'), 'No relationship read yet'), 240)}",
            f"- Missing info: {_safe_text('; '.join(missing) if missing else 'No additional classification gaps recorded.', 260)}",
        ]
        lines.append("## Internal Classification\n" + "\n".join(classification_lines))
    for name in SECTION_ORDER:
        if name in sections:
            lines.append(f"## {name}\n{_section_text(sections, name)}")
    return "\n\n".join(lines)


def deterministic_enrichment_ingest_key(subject: str, match_kind: str, sections: dict[str, list[str]]) -> str:
    material = json.dumps({"subject": _subject_key(subject), "matchKind": match_kind, "sections": sections}, sort_keys=True)
    digest = hashlib.sha256(material.encode("utf-8")).hexdigest()[:12]
    return f"bnl:source-enrichment:{_subject_key(subject)}:{match_kind}:{digest}"



_ARCHIVE_REDACT_KEY_RE = re.compile(r"(?i)(token|secret|authorization|api[_-]?key|password|customer[_-]?id|payment[_-]?id|stripe|raw[_-]?discord[_-]?id|discord[_-]?user[_-]?id|user[_-]?id|member[_-]?id|channel[_-]?id|private.*transcript|raw.*transcript)")
_ARCHIVE_ALLOWED_KEYS_TO_KEEP_AS_REDACTED = {"channelPolicy", "visibility", "authority", "sourceScope", "sourceTable", "sourceLabel", "sourceRowId"}


def _archive_safe_scalar(value: Any, limit: int = 5000) -> Any:
    if value is None or isinstance(value, (bool, int, float)):
        return value
    text = str(value)
    text = _DISCORD_MENTION_RE.sub("[redacted-mention]", text)
    text = _LONG_ID_RE.sub("[redacted-id]", text)
    text = _EMAIL_RE.sub("[redacted-email]", text)
    text = re.sub(r"(?i)(Bearer\s+)[A-Za-z0-9._~+/-]+", r"\1[redacted]", text)
    text = re.sub(r"(?i)(token|secret|authorization|api[_-]?key|password)\s*[:=]\s*[^\s,;}]+", r"\1=[redacted]", text)
    text = re.sub(r"\s+", " ", text).strip()
    if len(text) > limit:
        text = text[: max(0, limit - 1)].rstrip() + "…"
    return text


def _sanitize_archive_value(value: Any, *, key: str = "", depth: int = 0) -> Any:
    if depth > 12:
        return "[redacted-depth]"
    if key and _ARCHIVE_REDACT_KEY_RE.search(key) and key not in _ARCHIVE_ALLOWED_KEYS_TO_KEEP_AS_REDACTED:
        return "[redacted]"
    if isinstance(value, dict):
        cleaned: dict[str, Any] = {}
        for raw_key, raw_value in value.items():
            safe_key = _archive_safe_scalar(raw_key, 120)
            if not safe_key:
                continue
            cleaned[str(safe_key)] = _sanitize_archive_value(raw_value, key=str(safe_key), depth=depth + 1)
        return cleaned
    if isinstance(value, (list, tuple, set)):
        return [_sanitize_archive_value(item, key=key, depth=depth + 1) for item in list(value)]
    return _archive_safe_scalar(value)


BRIEF_V2_LIST_MAX_ITEMS = 6
BRIEF_V2_TEXT_ITEM_LIMIT = 300
BRIEF_V2_VERSION = "2"


def _brief_text(value: Any, limit: int = BRIEF_V2_TEXT_ITEM_LIMIT) -> str:
    """Human-readable brief text sanitizer for admin readouts."""

    text = _site_safe_text(value, limit)
    text = re.sub(r"\b(?:source[-_ ]?blind|global[-_ ]?mixed|rejected)\b", "review-only", text, flags=re.I)
    text = re.sub(r"\b(?:sourceTable|sourceRowId|rawRefJson|sourceLabel|channel_id|user_id|discord_user_id)\b\s*[:=]?\s*[^,; ]*", "", text, flags=re.I)
    text = re.sub(r"\b(?:entity_evidence_events|conversations|user_profiles|memory_tiers|broadcast_memory|source_file_lookup)\b", "local evidence", text, flags=re.I)
    text = text.replace("{", "").replace("}", "")
    text = re.sub(r"\s+", " ", text).strip(" -:;,.")
    if len(text) > limit:
        text = text[: max(0, limit - 1)].rstrip() + "…"
    return text


def _brief_fingerprint(value: Any) -> str:
    text = json.dumps(value, sort_keys=True, default=str) if isinstance(value, (dict, list)) else str(value or "")
    text = re.sub(r"https?://\S+", "[url]", text.lower())
    text = re.sub(r"[^a-z0-9]+", " ", text).strip()
    return text[:240]


def _brief_dedupe_texts(values: Any, *, max_items: int = BRIEF_V2_LIST_MAX_ITEMS, limit: int = BRIEF_V2_TEXT_ITEM_LIMIT) -> list[str]:
    raw_items = values if isinstance(values, (list, tuple, set)) else ([values] if values not in (None, "") else [])
    items: list[str] = []
    seen: set[str] = set()
    for raw in raw_items:
        if isinstance(raw, dict):
            raw = raw.get("summary") or raw.get("claim") or raw.get("detail") or raw.get("safeSummary") or raw.get("evidenceSummary") or raw.get("label") or raw.get("topic") or raw.get("source") or ""
        text = _brief_text(raw, limit)
        if not text or text.lower() in {"currently unknown", "none", "unknown", "n/a"}:
            continue
        fp = _brief_fingerprint(text)
        if not fp or fp in seen:
            continue
        if any(fp in existing or existing in fp for existing in seen if len(fp) > 36 and len(existing) > 36):
            continue
        seen.add(fp)
        items.append(text)
        if len(items) >= max_items:
            break
    return items


def _brief_evidence_item(raw: Any) -> dict[str, Any] | None:
    if isinstance(raw, dict):
        claim = raw.get("claim") or raw.get("summary") or raw.get("safeSummary") or raw.get("detail") or raw.get("label") or raw.get("topic") or raw.get("evidenceKind")
        evidence_summary = raw.get("evidenceSummary") or raw.get("safeSummary") or raw.get("snippet") or raw.get("detail") or raw.get("summary") or claim
        item: dict[str, Any] = {
            "claim": _brief_text(claim or evidence_summary, 220),
            "evidenceSummary": _brief_text(evidence_summary or claim, 260),
        }
        source_type = raw.get("sourceType") or raw.get("sourceLane") or raw.get("source") or raw.get("sourceLabel") or raw.get("sourceTable") or raw.get("table")
        if source_type:
            item["sourceType"] = _brief_text(source_type, 80)
        confidence = raw.get("confidence") or raw.get("strength") or raw.get("authority") or raw.get("visibility")
        if confidence:
            item["confidence"] = _brief_text(confidence, 80)
        public_safe = raw.get("publicSafe")
        if public_safe is not None:
            item["publicSafe"] = str(public_safe).strip().lower() not in {"0", "false", "no", "off", "internal", "private"}
        elif str(raw.get("visibility") or raw.get("authority") or "").lower() in {"public", "public_safe", "public-safe"}:
            item["publicSafe"] = True
        return item if item["claim"] or item["evidenceSummary"] else None
    text = _brief_text(raw, 260)
    if not text:
        return None
    return {"claim": text, "evidenceSummary": text}


def _brief_best_evidence(packet: dict[str, Any]) -> list[dict[str, Any]]:
    sources: list[Any] = []
    for key in ("bestEvidenceToReview", "representativeEvidence", "evidenceDetails", "conversationHighlights", "usefulEvidence"):
        value = packet.get(key)
        if isinstance(value, list):
            sources.extend(value)
    raw = packet.get("rawProvenance") if isinstance(packet.get("rawProvenance"), dict) else {}
    for fragment in list(raw.get("rawFragments") or []):
        if isinstance(fragment, dict) and (fragment.get("safeSummary") or fragment.get("summary")):
            sources.append(fragment)
    items: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw_item in sources:
        item = _brief_evidence_item(raw_item)
        if not item:
            continue
        fp = _brief_fingerprint(item.get("claim") or item.get("evidenceSummary"))
        if not fp or fp in seen:
            continue
        if any(fp in existing or existing in fp for existing in seen if len(fp) > 36 and len(existing) > 36):
            continue
        seen.add(fp)
        items.append(item)
        if len(items) >= BRIEF_V2_LIST_MAX_ITEMS:
            break
    return items


def _brief_identity_context(packet: dict[str, Any], subject_name: str, subject_key: str) -> dict[str, Any]:
    identity = packet.get("subjectIdentity") if isinstance(packet.get("subjectIdentity"), dict) else {}
    alias_labels = _brief_dedupe_texts(identity.get("aliasLabels") or identity.get("matchedIdentityLabels"), max_items=6, limit=100)
    notes: list[str] = []
    if identity:
        if identity.get("existingDossierMatch") or identity.get("publicDossierMatchFound"):
            notes.append("This subject may connect to an existing dossier/update lane; review before using it publicly.")
        if identity.get("matchedUserProfileCount") or identity.get("matchedUserIdCount"):
            notes.append("Local profile-matched identity signals exist, but this brief does not confirm identity or aliases.")
        if alias_labels:
            notes.append("Possible aliases or labels are present for admin review.")
    if not notes:
        notes.append("Identity links are reviewed on the admin Source File page.")
    return {
        "subjectName": subject_name,
        "subjectKey": subject_key,
        "workflowLane": _brief_text(identity.get("workflowLane") or packet.get("matchKind") or "source file review", 80),
        "aliasLabels": alias_labels,
        "confirmed": False,
        "notes": notes[:BRIEF_V2_LIST_MAX_ITEMS],
    }


def _brief_quality_warnings(packet: dict[str, Any]) -> list[str]:
    warnings: list[str] = []
    quality = str(packet.get("qualityStatus") or "").lower()
    score = packet.get("qualityScore")
    if quality in {"too_thin", "forced_low_confidence", "low", "suppressed_too_thin"} or (_is_finite_number(score) and float(score) < 50):
        warnings.append("Evidence is thin.")
    if packet.get("warningCounts") or packet.get("warnings"):
        warnings.extend(_brief_dedupe_texts(packet.get("warnings"), max_items=3, limit=160))
    identity = packet.get("subjectIdentity") if isinstance(packet.get("subjectIdentity"), dict) else {}
    if identity.get("aliasLabels") or identity.get("matchedIdentityLabels"):
        warnings.append("Possible alias or identity uncertainty needs admin review.")
    warnings.append("Full archive exists, but compact recommendation only contains a pointer.")
    return _brief_dedupe_texts(warnings, max_items=BRIEF_V2_LIST_MAX_ITEMS, limit=180)


def build_source_file_brief_v2(packet: dict[str, Any], *, archive_id: str | None = None) -> dict[str, Any]:
    """Build a compact, human-readable admin brief for the full Source File archive."""

    source_file = packet.get("sourceFile") if isinstance(packet.get("sourceFile"), dict) else {}
    sections = packet.get("sections") if isinstance(packet.get("sections"), dict) else {}
    subject_name = _brief_text(packet.get("subject") or _first(source_file, ("name", "sourceFileName", "subject", "displayName", "title", "normalizedName")) or "Unknown subject", 140)
    subject_key = _subject_key(subject_name or _first(source_file, ("normalizedName", "subjectKey", "slug")))
    match_kind = str(packet.get("matchKind") or "source file review").replace("_", " ")
    evidence_items = _brief_best_evidence(packet)
    public_notes = _brief_dedupe_texts(packet.get("publicSafePossibilities") or sections.get("Public-Safe Notes"), max_items=BRIEF_V2_LIST_MAX_ITEMS, limit=220)
    internal_notes = _brief_dedupe_texts((packet.get("privateOnlyNotes") or []) + (packet.get("notPublicYet") or []) + sections.get("Internal-Only / Review-Only Notes", []) + sections.get("Do Not Say", []), max_items=BRIEF_V2_LIST_MAX_ITEMS, limit=220)
    dossier_questions = _brief_dedupe_texts(packet.get("missingInfo") or sections.get("Missing Info"), max_items=BRIEF_V2_LIST_MAX_ITEMS, limit=220)
    if not dossier_questions:
        dossier_questions = [
            "Is there enough public-safe evidence to draft a public dossier?",
            "Does this subject connect to an existing dossier under another name?",
        ]
    what = _brief_dedupe_texts(
        (packet.get("knownContext") or [])
        + sections.get("Subject Overview", [])
        + sections.get("History With BARCODE / BNL / Discord / BARCODE Radio", [])
        + sections.get("Known Facts", [])
        + sections.get("Observed Patterns", []),
        max_items=BRIEF_V2_LIST_MAX_ITEMS,
        limit=240,
    )
    if not what and evidence_items:
        what = [item["claim"] for item in evidence_items[:3] if item.get("claim")]
    source_counts = packet.get("sourceCounts") if isinstance(packet.get("sourceCounts"), dict) else {}
    if source_counts and len(what) < BRIEF_V2_LIST_MAX_ITEMS:
        what.append("BNL has enough local evidence to preserve this Source File for review.")
    why = _brief_dedupe_texts(
        sections.get("Why This Subject Matters", [])
        + [
            "Possible dossier seed or update material." if match_kind != "none" else "Possible dossier seed if public-safe evidence improves.",
            "Identity context may need admin review." if (packet.get("subjectIdentity") or {}).get("aliasLabels") else "Evidence preservation for future Source File review.",
            "Review can decide what, if anything, is safe for dossier drafting.",
        ],
        max_items=BRIEF_V2_LIST_MAX_ITEMS,
        limit=220,
    )
    if public_notes:
        action = "Review public-safe notes and decide whether a short Proposed Dossier is justified."
    elif packet.get("qualityStatus") in {"too_thin", "forced_low_confidence"}:
        action = "Keep as Source File only; not ready for Proposed Dossier."
    else:
        action = "Review Identity Check before drafting."
    one_line = _brief_text(f"{subject_name} is a Source File subject in the {match_kind} lane with {len(evidence_items)} concise evidence item{'s' if len(evidence_items) != 1 else ''} ready for admin review.", 220)
    admin_summary = _brief_text(
        f"BNL has a review-only Source File for {subject_name}. The packet preserves the underlying evidence, while this brief pulls out the useful admin read: what appears supported, what may be safe to draft from, and what still needs human review. Treat this as internal guidance, not a public claim or identity decision.",
        900,
    )
    archive_refs = {
        "archiveId": _brief_text(archive_id or packet.get("archiveId") or "pending/not returned", 120),
        "ingestKey": _brief_text(packet.get("ingestKey") or "", 180),
        "compactPointerNote": COMPACT_RECOMMENDATION_ARCHIVE_NOTICE,
    }
    return {
        "version": BRIEF_V2_VERSION,
        "subjectName": subject_name,
        "subjectKey": subject_key,
        "oneLineSummary": one_line,
        "adminSummary": admin_summary,
        "whatBnlKnows": what[:BRIEF_V2_LIST_MAX_ITEMS],
        "whyThisMatters": why[:BRIEF_V2_LIST_MAX_ITEMS],
        "evidenceToReview": evidence_items[:BRIEF_V2_LIST_MAX_ITEMS],
        "identityContext": _brief_identity_context(packet, subject_name, subject_key),
        "publicSafeDraftingNotes": public_notes[:BRIEF_V2_LIST_MAX_ITEMS] or ["No public-safe drafting notes are confirmed yet."],
        "internalOnlyNotes": internal_notes[:BRIEF_V2_LIST_MAX_ITEMS] or ["Keep this Source File internal until owner/admin review decides what is usable."],
        "dossierQuestions": dossier_questions[:BRIEF_V2_LIST_MAX_ITEMS],
        "recommendedNextAction": _brief_text(action, 220),
        "qualityWarnings": _brief_quality_warnings(packet),
        "archiveReferences": archive_refs,
    }


SUBJECT_MEMORY_PACKET_VERSION = "1"
SOURCE_FILE_CASE_REPORT_VERSION = "1"
SUBJECT_MEMORY_MAX_MOMENTS = 8
_CASE_REPORT_FORBIDDEN_LABEL_RE = re.compile(
    r"\b(?:global_mixed|source_blind|source\s+rows?|internal\s+classification|automated\s+topic\s+label|approved\s+approved|relationship\s+facet)\b",
    re.I,
)
_CASE_REPORT_CATEGORY_ONLY_RE = re.compile(
    r"\b(?:music\s+discussion\s+exists|creative\s+language\s+observed|relationship\s+signals\s+found|community\s+activity\s+detected)\b",
    re.I,
)


def _case_text(value: Any, limit: int = 320) -> str:
    """Safe internal-report text: concise, redacted, and stripped of debug labels."""

    text = _archive_safe_scalar(value, limit)
    text = str(text or "")
    text = _CASE_REPORT_FORBIDDEN_LABEL_RE.sub("review-only evidence", text)
    text = _CASE_REPORT_CATEGORY_ONLY_RE.sub("the current evidence packet exposes a category but not the underlying detail", text)
    text = re.sub(r"\b(?:sourceTable|sourceRowId|rawRefJson|sourceLabel|channel_id|user_id|discord_user_id)\b\s*[:=]?\s*[^,; ]*", "", text, flags=re.I)
    text = re.sub(r"\b(?:entity_evidence_events|conversations|user_profiles|user_memory_facts|user_habits|relationship_state|relationship_journal|memory_tiers|broadcast_memory|source_file_lookup)\b", "archive evidence", text, flags=re.I)
    text = text.replace("{", "").replace("}", "")
    text = re.sub(r"\s+", " ", text).strip(" -:;,")
    if len(text) > limit:
        text = text[: max(0, limit - 1)].rstrip() + "…"
    return text


def _case_list(values: Any, *, limit: int = 6, item_limit: int = 260) -> list[str]:
    raw_items = values if isinstance(values, (list, tuple, set)) else ([values] if values not in (None, "") else [])
    out: list[str] = []
    seen: set[str] = set()
    for raw in raw_items:
        if isinstance(raw, dict):
            raw = raw.get("summary") or raw.get("safeSummary") or raw.get("detail") or raw.get("evidenceSummary") or raw.get("claim") or raw.get("label") or raw.get("topic") or ""
        text = _case_text(raw, item_limit)
        key = re.sub(r"[^a-z0-9]+", " ", text.lower()).strip()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(text)
        if len(out) >= limit:
            break
    return out


def _packet_generated_at(packet: dict[str, Any]) -> str:
    return _case_text(packet.get("runTime") or datetime.now(timezone.utc).isoformat(), 80)


def _extract_channel_name(raw: Any) -> str:
    if isinstance(raw, dict):
        for key in ("channelName", "channel", "channel_name", "context", "sourceContext"):
            value = raw.get(key)
            if value:
                text = _case_text(value, 80)
                if text and not _PRIVATE_CHANNEL_NAME_RE.search(text):
                    return text if text.startswith("#") or " " in text else (f"#{text}" if re.match(r"^[A-Za-z0-9_-]+$", text) else text)
    text = _case_text(raw, 180)
    m = re.search(r"#[A-Za-z0-9][A-Za-z0-9_-]{1,80}", text)
    if m and not _PRIVATE_CHANNEL_NAME_RE.search(m.group(0)):
        return m.group(0)
    return ""


def _extract_occurred_at(raw: Any) -> str:
    if isinstance(raw, dict):
        for key in ("occurredAt", "createdAt", "created_at", "timestamp", "episodeDate", "date", "lastSeen", "firstSeen"):
            if raw.get(key):
                return _case_text(raw.get(key), 80)
    text = _case_text(raw, 260)
    m = re.search(r"\b20\d{2}[-/]\d{1,2}[-/]\d{1,2}(?:[ T]\d{1,2}:\d{2}(?::\d{2})?)?", text)
    return m.group(0) if m else ""


def _extract_visibility(raw: Any, fallback: str = "internal_review_only") -> str:
    if isinstance(raw, dict):
        for key in ("visibility", "channelPolicy", "policy", "authority"):
            value = str(raw.get(key) or "").lower().strip()
            if value:
                if "public" in value and "private" not in value:
                    return "public_reviewable"
                if "private" in value or "review" in value or "internal" in value:
                    return "internal_review_only"
                return _case_text(value, 60)
    return fallback


def _extract_source_type(raw: Any, fallback: str = "archive evidence") -> str:
    if isinstance(raw, dict):
        for key in ("sourceType", "sourceLane", "source", "lane", "table", "sourceTable", "origin", "label"):
            value = raw.get(key)
            if value:
                text = _case_text(value, 90)
                if text:
                    lowered = text.lower()
                    if "conversation" in lowered or "discord" in lowered:
                        return "Discord conversation"
                    if "broadcast" in lowered:
                        return "BARCODE Radio memory"
                    if "profile" in lowered or "identity" in lowered:
                        return "identity/profile signal"
                    if "relationship" in lowered:
                        return "relationship/context evidence"
                    if "source" in lowered and "file" in lowered:
                        return "Source File archive"
                    return text
    return fallback


def _moment_from_raw(raw: Any, subject: str, default_why: str = "It gives admin review at least one concrete source/context detail instead of a category label.") -> dict[str, Any] | None:
    summary = ""
    if isinstance(raw, dict):
        summary = _case_text(raw.get("summary") or raw.get("safeSummary") or raw.get("evidenceSummary") or raw.get("claim") or raw.get("detail") or raw.get("snippet") or raw.get("label") or raw.get("topic"), 260)
    else:
        summary = _case_text(raw, 260)
    if not summary:
        return None
    if _CASE_REPORT_FORBIDDEN_LABEL_RE.search(summary):
        return None
    channel = _extract_channel_name(raw)
    source_type = _extract_source_type(raw)
    visibility = _extract_visibility(raw)
    occurred = _extract_occurred_at(raw)
    dossier_use = "Review as supporting internal Source File context; do not convert into a public claim without admin confirmation."
    public_safe = visibility == "public_reviewable" and not _PRIVATE_CHANNEL_NAME_RE.search(channel or summary)
    return {
        "summary": summary,
        "sourceType": source_type,
        **({"channelName": channel} if channel else {}),
        "visibility": visibility,
        **({"occurredAt": occurred} if occurred else {}),
        "whyItMatters": _case_text(default_why, 220),
        "dossierUse": dossier_use,
        "publicSafe": bool(public_safe),
    }


def _collect_representative_moments(packet: dict[str, Any], subject: str) -> list[dict[str, Any]]:
    candidates: list[Any] = []
    for field in ("representativeEvidence", "bestEvidenceToReview", "evidenceDetails", "conversationHighlights", "bnlInteractionSignals", "musicSignals", "communitySignals"):
        value = packet.get(field)
        if isinstance(value, list):
            candidates.extend(value)
    raw = packet.get("rawProvenance") if isinstance(packet.get("rawProvenance"), dict) else {}
    candidates.extend(list(raw.get("rawFragments") or [])[:12])
    moments: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw_item in candidates:
        moment = _moment_from_raw(raw_item, subject)
        if not moment:
            continue
        fp = re.sub(r"[^a-z0-9]+", " ", moment["summary"].lower()).strip()[:220]
        if not fp or fp in seen:
            continue
        seen.add(fp)
        moments.append(moment)
        if len(moments) >= SUBJECT_MEMORY_MAX_MOMENTS:
            break
    return moments


def _infer_subject_memory_coverage(packet: dict[str, Any]) -> dict[str, Any]:
    source_counts = dict(packet.get("sourceCounts") or {})
    source_types = [str(item) for item in (packet.get("sourceTypes") or [])]
    raw = packet.get("rawProvenance") if isinstance(packet.get("rawProvenance"), dict) else {}
    source_labels = [str(item) for item in (raw.get("sourceLabels") or [])]
    haystack = " ".join([*source_types, *source_counts.keys(), *source_labels]).lower()

    def lane(name: str, used: bool, detail: str) -> dict[str, Any]:
        return {"lane": name, "status": "used" if used else "unavailable", "detail": _case_text(detail, 260)}

    queue_connected = str(packet.get("queueSubmissionStatus") or "not_connected") not in {"", "not_connected", "unknown"}
    identity_used = bool(packet.get("subjectIdentity") or packet.get("linkSignalDiagnostics") or packet.get("entityIntelligenceProfile"))
    coverage = [
        lane("recent Discord conversation memory", "conversation" in haystack or bool(packet.get("conversationHighlights")), "Conversation highlights or raw archive fragments were available." if "conversation" in haystack or packet.get("conversationHighlights") else "Recent Discord conversation memory is not separately exposed to this report."),
        lane("historical Discord conversation memory", "broadcast" in haystack or bool(packet.get("bestEvidenceToReview")), "Historical/BARCODE memory or best evidence items were available for review." if "broadcast" in haystack or packet.get("bestEvidenceToReview") else "Historical Discord memory is not separately exposed to this report."),
        lane("entity intelligence / Entity Ledger", bool(packet.get("entityIntelligenceProfile") or (packet.get("diagnostics") or {}).get("entityIntelligence")), "Entity Intelligence diagnostics/profile were included." if packet.get("entityIntelligenceProfile") or (packet.get("diagnostics") or {}).get("entityIntelligence") else "Entity Ledger details were not available to this report."),
        lane("Source File archive", True, "The full Source File archive envelope and sourcePackage are preserved for admin review."),
        lane("Source File notes", bool(packet.get("sections")), "Source File enrichment sections/notes were included." if packet.get("sections") else "No Source File note sections were exposed."),
        lane("identity links / aliases", identity_used, "Identity/link signals are review-only inputs; identity is not auto-confirmed." if identity_used else "Identity review is handled through the site Identity Check when links are available."),
        lane("website/source file state", bool(packet.get("sourceFile") or packet.get("matchKind")), f"Website lookup match target: {packet.get('matchKind') or 'unknown'}."),
        lane("queue/submission data", queue_connected, "Queue/submission data is connected to this report." if queue_connected else "Queue/submission memory is not connected to this report. Do not claim submissions, song counts, play history, payment, or Priority status."),
        lane("public/private channel policy", bool(packet.get("warningCounts") or packet.get("sourceAuthority")), "Visibility/channel-policy warnings were carried into review context." if packet.get("warningCounts") or packet.get("sourceAuthority") else "Public/private channel policy is not fully exposed per item."),
        lane("short-term subject context", False, "Short/mid/long-term subject memory lanes are not exposed separately to this report yet; this report uses available conversation, entity, and Source File archive data."),
        lane("mid-term subject patterns", False, "Short/mid/long-term subject memory lanes are not exposed separately to this report yet; this report uses available conversation, entity, and Source File archive data."),
        lane("long-term subject profile", False, "Short/mid/long-term subject memory lanes are not exposed separately to this report yet; this report uses available conversation, entity, and Source File archive data."),
    ]
    return {"lanes": coverage, "summary": "Short/mid/long-term subject memory lanes are not exposed separately to this report yet; this report uses available conversation, entity, and Source File archive data."}


def _has_concrete_case_detail(text: str) -> bool:
    lowered = str(text or "").lower()
    if any(token in lowered for token in ("#", "http://", "https://", "suno", "spotify", "soundcloud", "youtube", "demo", "wip", "beat", "track", "song", "artist", "profile", "link", "queue", "submission", "barcode", "bnl")):
        return True
    if re.search(r"\b20\d{2}[-/]\d{1,2}[-/]\d{1,2}\b", lowered):
        return True
    return False


def _generic_label_gap(packet: dict[str, Any], field: str, readable: str) -> list[str]:
    values = packet.get(field)
    if values:
        detailed = _case_list(values, limit=4)
        if detailed and any(_has_concrete_case_detail(item) for item in detailed):
            return detailed
        return [f"The archive flags {readable}, but the current evidence packet does not expose the specific songs, collaborators, messages, channels, platforms, or links behind that label."]
    return [f"No specific {readable} detail is confirmed in the current report packet."]


def _relationship_review_items(packet: dict[str, Any], subject: str) -> list[str]:
    values = _case_list(packet.get("relationshipSignals") or packet.get("reviewOnlyEvidence"), limit=5)
    if not values:
        return ["No specific relationship/context connection is confirmed in the current report packet; keep relationship interpretation review-only."]
    out = []
    for text in values:
        if "review-only" not in text.lower() and "not confirm" not in text.lower() and "uncertain" not in text.lower():
            text = f"{text}. Treat the relationship/context meaning as review-only and unconfirmed."
        out.append(text)
    return out


_ANCHOR_NOISE_WORDS = {
    "a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "from", "has", "have", "how", "if", "in", "is", "it", "its", "of", "on", "or", "our", "the", "this", "to", "was", "we", "with", "you", "your",
    "entity", "local", "source", "source file", "source files", "conversation", "relationship", "internal", "existing", "determine", "most", "review", "reviewed", "public", "private",
    "context", "evidence", "classification", "subject", "report", "dossier", "queue", "submission", "identity", "bnl-facing", "known facts", "public-safe", "case summary",
    "dossier use", "review blockers", "raw", "file", "generated", "summary", "label", "labels", "status", "packet", "archive", "coverage", "candidate",
    "facing", "barcode-facing",
}
_MEANINGFUL_ANCHOR_TYPES = {
    "orion": "person",
    "bnl": "system",
    "bnl-01": "system",
    "network": "lore",
    "barcode": "project",
    "edge": "lore",
    "bit": "person",
    "bits": "person",
    "bit's": "person",
    "lardcode": "project",
    "suno": "platform",
    "discord": "platform",
    "spotify": "platform",
    "soundcloud": "platform",
    "youtube": "platform",
}
_ANCHOR_EVIDENCE_FIELDS = (
    "representativeEvidence", "evidenceDetails", "bestEvidenceToReview", "usefulEvidence", "conversationHighlights",
    "bnlInteractionSignals", "musicSignals", "communitySignals", "relationshipSignals",
)
_ANCHOR_GENERATED_FIELDS = (
    "subjectRead", "bnlTake", "confidenceRead", "queueSubmissionRead", "queueSubmissionNote", "sourceFileGaps",
    "recommendedAdminActions", "doNotSayPubliclyYet", "sourceCoverage", "topTopicDetails", "topicBreakdown",
    "topicThemes", "conversationThemes", "knownContext", "publicUseCandidates", "reviewOnlyEvidence",
    "missingInfo", "sourceAuthority", "sourceCounts", "sourceTypes", "archivePayloadMetadata",
)

_ANCHOR_NOISE_KEYS = {re.sub(r"[^a-z0-9]+", " ", term.lower()).strip() for term in _ANCHOR_NOISE_WORDS}
_ANCHOR_COMMON_STOP_NOISE = {
    "a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "from", "has", "have", "how", "if", "in", "is", "it", "its", "of", "on", "or", "our", "the", "this", "to", "was", "we", "with", "you", "your",
}


def _is_anchor_noise_key(key: str) -> bool:
    return key in _ANCHOR_NOISE_WORDS or key in _ANCHOR_NOISE_KEYS


def _case_flatten_text_values(value: Any, *, max_items: int = 80, item_limit: int = 260) -> list[str]:
    out: list[str] = []

    def walk(node: Any) -> None:
        if len(out) >= max_items:
            return
        if isinstance(node, dict):
            preferred = (
                "summary", "safeSummary", "detail", "evidenceSummary", "claim", "label", "topic", "theme",
                "name", "title", "note", "explanation", "snippet", "channel", "channelName", "latestObservedAt",
                "recentActivitySummary", "authoredVsMentionedSummary",
            )
            for key in preferred:
                if key in node:
                    walk(node.get(key))
            for key, raw in node.items():
                if key not in preferred and key not in {"id", "rowId", "sourceRowId", "rawRefJson", "userId", "channelId"}:
                    walk(raw)
        elif isinstance(node, (list, tuple, set)):
            for item in node:
                walk(item)
        elif node not in (None, ""):
            text = _case_text(node, item_limit)
            if text:
                out.append(text)

    walk(value)
    deduped: list[str] = []
    seen: set[str] = set()
    for item in out:
        key = re.sub(r"[^a-z0-9]+", " ", item.lower()).strip()[:180]
        if key and key not in seen:
            seen.add(key)
            deduped.append(item)
        if len(deduped) >= max_items:
            break
    return deduped


def _build_subject_intelligence_context(packet: dict[str, Any]) -> dict[str, Any]:
    fields = (
        "conversationThemes", "topicThemes", "topTopicDetails", "topicBreakdown", "knownContext", "usefulEvidence",
        "bestEvidenceToReview", "bnlInteractionSignals", "musicSignals", "communitySignals", "relationshipSignals",
        "evidenceDetails", "representativeEvidence", "sourceCoverage", "topChannels", "activityFrequencySummary",
        "recentActivitySummary", "authoredVsMentionedSummary", "publicUseCandidates", "reviewOnlyEvidence", "missingInfo",
        "sourceAuthority", "conversationHighlights",
    )
    context = {field: _sanitize_archive_value(packet.get(field)) for field in fields if packet.get(field) not in (None, "", [], {})}
    context["queueSubmissionStatus"] = _case_text(packet.get("queueSubmissionStatus") or "not_connected", 80)
    context["queueSubmissionNote"] = _case_text(packet.get("queueSubmissionNote") or QUEUE_NOT_CONNECTED_NOTE, 260)
    source_counts = packet.get("sourceCounts") if isinstance(packet.get("sourceCounts"), dict) else {}
    if source_counts:
        context["sourceCounts"] = _sanitize_archive_value(source_counts)
    raw = packet.get("rawProvenance") if isinstance(packet.get("rawProvenance"), dict) else {}
    if raw:
        meta: dict[str, Any] = {}
        for key in ("sourceLabels", "sourceTypes", "fragmentCount", "latestObservedAt"):
            if raw.get(key) not in (None, "", [], {}):
                meta[key] = _sanitize_archive_value(raw.get(key))
        if raw.get("rawFragments"):
            meta["representativeFragments"] = _sanitize_archive_value(list(raw.get("rawFragments") or [])[:6])
        if meta:
            context["archivePayloadMetadata"] = meta
    return context


def _brief_context(memory: dict[str, Any]) -> dict[str, Any]:
    return memory.get("sourceIntelligenceContext") if isinstance(memory.get("sourceIntelligenceContext"), dict) else {}


def _as_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)) and value >= 0:
        return int(value)
    if isinstance(value, str) and value.strip().isdigit():
        return int(value.strip())
    return None



_MAIN_READ_ELIGIBLE_SCOPES = {
    "owned",
    "direct_address",
    "confirmed_alias",
    "explicit_relationship",
    "queue_authoritative",
    "site_authoritative",
    "broadcast_memory_authoritative",
}
_REVIEW_ONLY_SHAPE_SCOPES = {"co_mention", "shared_topic", "generated_or_taxonomy", "source_blind_legacy", "unknown"}


def _ownership_item_scope(item: Any) -> str:
    if not isinstance(item, dict):
        return "unknown"
    classification = item.get("classification") if isinstance(item.get("classification"), dict) else {}
    return str(classification.get("ownership") or item.get("ownership") or "unknown")


def _main_read_eligible_texts(ownership: dict[str, Any]) -> list[str]:
    """Return only subject-owned/explicitly authoritative fragments allowed to shape main reads."""
    if not isinstance(ownership, dict):
        return []
    items: list[Any] = []
    by_ownership = ownership.get("byOwnership") if isinstance(ownership.get("byOwnership"), dict) else {}
    for scope in _MAIN_READ_ELIGIBLE_SCOPES:
        items.extend(by_ownership.get(scope) or [])
    items.extend(ownership.get("owned") or [])
    texts: list[str] = []
    seen: set[str] = set()
    for item in items:
        if _ownership_item_scope(item) not in _MAIN_READ_ELIGIBLE_SCOPES:
            continue
        text = _case_text(item.get("text") if isinstance(item, dict) else item, 300)
        key = text.lower()
        if text and key not in seen:
            seen.add(key)
            texts.append(text)
    return texts


def _review_only_shape_texts(ownership: dict[str, Any]) -> list[str]:
    """Return nearby/review-only/generated fragments for diagnostics, never main read shape."""
    if not isinstance(ownership, dict):
        return []
    items: list[Any] = []
    by_ownership = ownership.get("byOwnership") if isinstance(ownership.get("byOwnership"), dict) else {}
    for scope in _REVIEW_ONLY_SHAPE_SCOPES:
        items.extend(by_ownership.get(scope) or [])
    items.extend(ownership.get("reviewOnly") or [])
    texts: list[str] = []
    seen: set[str] = set()
    for item in items:
        if _ownership_item_scope(item) not in _REVIEW_ONLY_SHAPE_SCOPES:
            continue
        text = _case_text(item.get("text") if isinstance(item, dict) else item, 300)
        key = text.lower()
        if text and key not in seen:
            seen.add(key)
            texts.append(text)
    return texts

def _latest_observed_from_texts(texts: list[str]) -> str:
    latest = ""
    for text in texts:
        for match in re.findall(r"\b20\d{2}-\d{2}-\d{2}(?:[T ][0-9:.+-]+Z?)?", text):
            if match > latest:
                latest = match
    return _case_text(latest, 80)


def _subject_activity_profile(memory: dict[str, Any]) -> dict[str, Any]:
    context = _brief_context(memory)
    freq = context.get("activityFrequencySummary") if isinstance(context.get("activityFrequencySummary"), dict) else {}
    counts = context.get("sourceCounts") if isinstance(context.get("sourceCounts"), dict) else {}
    all_texts = _case_flatten_text_values(context, max_items=80) + _case_flatten_text_values(memory.get("representativeMoments"), max_items=20)
    total_scanned = sum(v for v in (_as_int(v) for v in counts.values()) if v is not None) or None
    authored = _as_int(freq.get("approvedPublicAuthoredRows") or freq.get("totalApprovedPublicAuthoredItems"))
    mentioned = _as_int(freq.get("approvedPublicMentionedRows") or freq.get("totalApprovedPublicMentions"))
    review = _as_int(freq.get("reviewOnlyEvidenceCount"))
    latest = _case_text(freq.get("latestObservedAt") or context.get("recentActivitySummary") or _latest_observed_from_texts(all_texts), 140)
    observed_total = sum(v for v in (authored, mentioned, review) if v is not None) or total_scanned or len(memory.get("representativeMoments") or [])
    if observed_total >= 10:
        level = "high / recurring"
    elif observed_total >= 4:
        level = "moderate / repeated"
    elif observed_total > 0:
        level = "low / limited"
    else:
        level = "unknown / insufficient extracted evidence"
    profile: dict[str, Any] = {"activityLevel": level}
    if authored is not None:
        profile["totalApprovedPublicAuthoredItems"] = authored
    if mentioned is not None:
        profile["totalApprovedPublicMentions"] = mentioned
    if review is not None:
        profile["reviewOnlyEvidenceCount"] = review
    if total_scanned is not None:
        profile["totalEvidenceScanned"] = total_scanned
    if latest:
        profile["latestObservedAt"] = latest
    return profile


def _safe_channel_name(value: Any, note_text: str = "") -> str:
    text = _case_text(value, 80)
    lowered = f"{text} {note_text}".lower()
    if not text:
        return "unknown channel"
    if any(token in lowered for token in ("private", "internal_controlled", "review-only", "internal review", "dm")) and not text.startswith("#"):
        return "review-only channel"
    if re.search(r"\b(?:channel_id|discord|user_id|rowid|sourcerowid)\b", lowered):
        return "review-only channel"
    if not text.startswith("#") and re.search(r"(?:research|internal|admin|mod|ops|control)", lowered):
        return "review-only channel"
    return text


def _channel_breakdown(memory: dict[str, Any]) -> list[dict[str, Any]]:
    context = _brief_context(memory)
    raw_channels = context.get("topChannels") if isinstance(context.get("topChannels"), list) else []
    rows: list[dict[str, Any]] = []
    for item in raw_channels[:8]:
        if isinstance(item, dict):
            count = _as_int(item.get("count") or item.get("total"))
            name = _safe_channel_name(item.get("channelName") or item.get("channel") or item.get("name"), json.dumps(item, default=str))
            note = _case_text(item.get("note") or item.get("summary") or (f"{count} observed item(s)" if count is not None else "Observed in extracted source coverage."), 180)
        else:
            count = None
            name = _safe_channel_name(item)
            note = "Observed in extracted source coverage."
        role = "unknown"
        if count is not None:
            role = "primary" if count >= 3 else ("secondary" if count == 2 else "minor")
        rows.append({"channelName": name, **({"count": count} if count is not None else {}), "role": role, "note": note})
    if not rows:
        for moment in (memory.get("representativeMoments") or [])[:5]:
            if not isinstance(moment, dict):
                continue
            name = _safe_channel_name(moment.get("channelName"), json.dumps(moment, default=str))
            if name and name != "unknown channel":
                rows.append({"channelName": name, "role": "unknown", "note": _case_text(moment.get("summary") or "Representative evidence appears here.", 180)})
    return rows or [{"channelName": "unknown channel", "role": "unknown", "note": "The current packet does not expose a safe channel breakdown."}]


def _clean_topic_label(value: Any) -> str:
    text = _case_text(value, 120)
    lowered = text.lower()
    if "bnl" in lowered and re.search(r"source[- ]file|dossier|threshold|boundary|operation", lowered):
        return "BNL-facing behavior"
    if re.search(r"source[- ]file|dossier", lowered):
        return "Source File / dossier discussion"
    if re.search(r"suno|music|song|track|link-sharing|link sharing", lowered):
        return "Music or Suno link sharing"
    if re.search(r"interface|lore|orion|sync|convergence|node|liaison", lowered):
        return "Interface / lore language"
    if re.search(r"barcode.*(?:radio|show)|(?:radio|show).*barcode", lowered):
        return "BARCODE Radio / show talk"
    if re.search(r"community|collaboration|collaborator", lowered):
        return "Community / collaboration context"
    text = re.sub(r"\bclassification\b", "", text, flags=re.I)
    text = re.sub(r"\s+", " ", text).strip(" -:/")
    return text[:1].upper() + text[1:] if text else "Unlabeled recurring topic"


def _topic_explanation(topic: str, detail: str, signals: list[str]) -> str:
    lowered = f"{topic} {detail} {' '.join(signals)}".lower()
    if "music" in lowered or "suno" in lowered or "spotify" in lowered or "soundcloud" in lowered or "youtube" in lowered or "link" in lowered:
        return _case_text(detail or "Music/link sharing appears in the evidence, but the packet does not prove the links are the subject's own music. Treat this as a prompt to confirm artist role, link ownership, queue status, and public music links.", 320)
    if "interface" in lowered or "lore" in lowered or "orion" in lowered or "sync" in lowered or "convergence" in lowered or "node" in lowered:
        return _case_text(detail or "Interface/lore language suggests the subject may be participating as a BARCODE/BNL-facing or character-adjacent presence, not merely ordinary chat; meaning still needs admin confirmation.", 320)
    if "bnl" in lowered or "source file" in lowered or "source-file" in lowered or "dossier" in lowered or "threshold" in lowered:
        return _case_text(detail or "BNL-facing Source File behavior is prominent for this subject; the useful read is about how they interact with BNL review boundaries, not a public identity claim.", 320)
    if "collab" in lowered or "wip" in lowered or "finished" in lowered:
        return _case_text(detail or "Collaboration/WIP context makes this subject useful for network mapping, but the packet still needs confirmation of owned work versus helping, sharing, or appearing around projects.", 320)
    if "radio" in lowered or "show" in lowered:
        return _case_text(detail or "Show/radio context appears as a participation signal; confirm whether this is hosting, guesting, playlist/play-history, or ordinary discussion before public wording.", 320)
    if "community" in lowered or "chat" in lowered or "social" in lowered or "support" in lowered:
        return _case_text(detail or "The subject reads primarily through community presence and conversation/support patterns; do not upgrade that into artist, submitter, or lore identity without stronger evidence.", 320)
    return _case_text(detail or f"Extracted evidence points at {topic.lower()}, but the packet has limited subject-specific detail behind that label.", 320)

def _topic_buckets(memory: dict[str, Any], ownership: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    context = _brief_context(memory)
    candidates: list[Any] = []
    for field in ("topTopicDetails", "topicBreakdown", "topicThemes", "conversationThemes"):
        value = context.get(field)
        if isinstance(value, list):
            candidates.extend(value)
    buckets: list[dict[str, Any]] = []
    seen: set[str] = set()
    if ownership:
        evidence_texts = _main_read_eligible_texts(ownership)
        review_only_texts = _review_only_shape_texts(ownership)
    else:
        evidence_texts = _case_flatten_text_values([context.get("knownContext"), context.get("usefulEvidence"), context.get("bestEvidenceToReview"), context.get("evidenceDetails"), memory.get("representativeMoments")], max_items=40)
        review_only_texts = []
    for item in candidates:
        if isinstance(item, dict):
            topic = _clean_topic_label(item.get("topic") or item.get("label") or item.get("theme") or item.get("name"))
            count = _as_int(item.get("count") or item.get("evidenceCount") or item.get("mentions"))
            detail = _case_text(item.get("summary") or item.get("explanation") or item.get("detail") or item.get("note") or "", 260)
        else:
            topic = _clean_topic_label(item)
            count = None
            detail = ""
        key = topic.lower()
        if not topic or key in seen:
            continue
        seen.add(key)
        topic_tokens = [part for part in re.split(r"[^a-z0-9]+", key) if len(part) >= 3]
        signals = [text for text in evidence_texts if any(token in text.lower() for token in topic_tokens[:3])][:3]
        review_signals = [text for text in review_only_texts if any(token in text.lower() for token in topic_tokens[:3])][:2]
        main_read_eligible = bool(signals)
        if count is None:
            count = len(signals) or (len(review_signals) if review_signals else None)
        strength = "weak"
        if not main_read_eligible and ownership:
            strength = "weak"
        elif signals and count is not None and count >= 5:
            strength = "strong"
        elif signals and count is not None and count >= 2:
            strength = "moderate"
        elif re.search(r"\bnoise|generic|unclear\b", topic, re.I):
            strength = "noise"
        ownership_basis = "owned" if main_read_eligible else ("nearby_context" if review_signals else ("generated_or_taxonomy" if ownership else "unknown"))
        explanation = _topic_explanation(topic, detail, signals) if main_read_eligible else _case_text(f"Review-only / nearby context only; do not treat this topic label as subject-owned shape evidence. {_topic_explanation(topic, detail, review_signals)}", 360)
        buckets.append({"topic": topic, "strength": strength, "mainReadEligible": main_read_eligible, "ownershipBasis": ownership_basis, **({"evidenceCount": count} if count is not None else {}), "explanation": explanation, **({"exampleSignals": signals[:3] or review_signals[:2]} if (signals or review_signals) else {})})
        if len(buckets) >= 10:
            break
    if not buckets:
        for text in _case_list(memory.get("repeatedTopics"), limit=4):
            buckets.append({"topic": _clean_topic_label(text), "strength": "weak", "mainReadEligible": False, "ownershipBasis": "unknown", "explanation": text})
    return buckets or [{"topic": "Insufficient topic extraction", "strength": "weak", "mainReadEligible": False, "ownershipBasis": "unknown", "explanation": "The current packet does not expose enough topic detail to summarize recurring subject themes."}]


def _anchor_type(name: str) -> str:
    return _MEANINGFUL_ANCHOR_TYPES.get(name.lower(), "unknown")


def _anchor_noise_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", value.lower()).strip()


def _record_anchor_noise(noise: dict[str, dict[str, Any]], term: str, reason: str, *, count: int = 1) -> None:
    key = _anchor_noise_key(term)
    if not key or not _is_anchor_noise_key(key):
        return
    item = noise.setdefault(key, {"term": key, "reason": reason, "count": 0})
    item["count"] = int(item.get("count") or 0) + max(1, count)
    if reason not in str(item.get("reason") or ""):
        item["reason"] = reason if not item.get("reason") else f"{item['reason']}; {reason}"


def _anchor_text_fragments(memory: dict[str, Any]) -> tuple[list[str], list[str]]:
    context = _brief_context(memory)
    evidence_values: list[Any] = [context.get(field) for field in _ANCHOR_EVIDENCE_FIELDS]
    raw_meta = context.get("archivePayloadMetadata") if isinstance(context.get("archivePayloadMetadata"), dict) else {}
    evidence_values.append(raw_meta.get("representativeFragments"))
    evidence_values.append(memory.get("representativeMoments"))
    evidence_texts = _case_flatten_text_values(evidence_values, max_items=120)
    generated_texts = _case_flatten_text_values([context.get(field) for field in _ANCHOR_GENERATED_FIELDS], max_items=120)
    return evidence_texts, generated_texts


def _extract_ignored_anchor_noise(evidence_texts: list[str], generated_texts: list[str]) -> list[dict[str, Any]]:
    noise: dict[str, dict[str, Any]] = {}
    all_texts = [(text, "taxonomy/system/report wording is not a named anchor") for text in generated_texts]
    all_texts.extend((text, "taxonomy word was filtered from evidence-bearing text") for text in evidence_texts)
    diagnostic_terms = [term for term in _ANCHOR_NOISE_WORDS if _anchor_noise_key(term) not in _ANCHOR_COMMON_STOP_NOISE]
    sorted_noise = sorted(diagnostic_terms, key=len, reverse=True)
    for text, reason in all_texts:
        lowered = text.lower()
        for term in sorted_noise:
            if len(term) <= 2:
                continue
            pattern = re.escape(term).replace("\\ ", r"\s+")
            found = re.findall(rf"(?<![-A-Za-z0-9]){pattern}(?![-A-Za-z0-9])", lowered, flags=re.I)
            if found:
                _record_anchor_noise(noise, term, reason, count=len(found))
    return sorted(noise.values(), key=lambda item: (-int(item.get("count") or 0), item.get("term", "")))[:40]


def _domain_anchor_type(name: str) -> str:
    if re.match(r"https?://", name, flags=re.I):
        return "url"
    if re.search(r"\.[a-z]{2,}(?:/|$)", name, flags=re.I):
        return "domain"
    return _anchor_type(name)


def _named_anchor_extraction(memory: dict[str, Any], ownership: dict[str, Any] | None = None) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    if ownership:
        evidence_texts = _case_flatten_text_values([item.get("text") for item in ownership.get("owned", [])], max_items=120)
        review_texts = _case_flatten_text_values([item.get("text") for item in ownership.get("reviewOnly", [])], max_items=120)
        _raw_evidence_texts, generated_texts = _anchor_text_fragments(memory)
    else:
        evidence_texts, generated_texts = _anchor_text_fragments(memory)
        review_texts = []
    ignored_noise = _extract_ignored_anchor_noise(evidence_texts, generated_texts)
    counts: dict[str, int] = {}
    fragments: dict[str, set[int]] = {}
    canonical: dict[str, str] = {}
    qualifiers: dict[str, set[str]] = {}

    def add_candidate(name: str, fragment_index: int, qualifier: str, count: int = 1) -> None:
        clean = _case_text(name, 120).strip(".,;:()[]{}<>\"'")
        key = _anchor_noise_key(clean)
        if not key or _is_anchor_noise_key(key):
            return
        canonical.setdefault(key, clean)
        counts[key] = counts.get(key, 0) + max(1, count)
        fragments.setdefault(key, set()).add(fragment_index)
        qualifiers.setdefault(key, set()).add(qualifier)

    for idx, text in enumerate(evidence_texts):
        for match in re.findall(r"https?://[^\s)\]}>]+", text, flags=re.I):
            add_candidate(match.rstrip(".,;:"), idx, "url/domain/tool")
        for match in re.findall(r"(?<!@)\b(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,}(?:/[A-Za-z0-9._~:/?#\[\]@!$&'()*+,;=%-]*)?", text, flags=re.I):
            add_candidate(match.rstrip(".,;:"), idx, "url/domain/tool")
        for known, anchor_type in _MEANINGFUL_ANCHOR_TYPES.items():
            pattern = re.escape(known).replace("\\ ", r"\s+")
            found = re.findall(rf"(?<![-A-Za-z0-9]){pattern}(?![-A-Za-z0-9])", text, flags=re.I)
            if found:
                display = "BNL-01" if known == "bnl-01" else ("BNL" if known == "bnl" else ("BARCODE" if known == "barcode" else ("EDGE" if known == "edge" else ("Bit's" if known == "bit's" else known[:1].upper() + known[1:]))))
                add_candidate(display, idx, f"known {anchor_type} term in evidence", len(found))
        for match in re.findall(r"(?<![-A-Za-z0-9])[A-Z][A-Za-z0-9'’-]{2,}(?:-[A-Za-z0-9'’-]+)*\b", text):
            add_candidate(match, idx, "proper noun in evidence")

    anchors: list[dict[str, Any]] = []
    subject_key = _anchor_noise_key(str(memory.get("subjectName") or ""))
    subject_parts = {part for part in subject_key.split() if len(part) >= 3}
    for key, count in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0])):
        if _is_anchor_noise_key(key) or key == subject_key or key == f"{subject_key} s" or key in subject_parts:
            continue
        qs = qualifiers.get(key, set())
        distinct = len(fragments.get(key, set()))
        qualifies = (
            any(q.startswith("proper noun") for q in qs)
            or any(q.startswith("known") for q in qs)
            or "url/domain/tool" in qs
            or distinct > 1
            or ("relationship" in key and distinct > 0)
        )
        if not qualifies:
            continue
        name = canonical.get(key, key)
        strength = "strong" if count >= 4 or distinct >= 3 else ("moderate" if count >= 2 or distinct >= 2 else "weak")
        anchor_type = _domain_anchor_type(name)
        note = f"Appears in {count} evidence-bearing source fragment{'s' if count != 1 else ''}; admin should decide whether it has subject-specific public meaning."
        if key == "orion":
            note = "Recurring Orion-linked context; relationship/meaning not confirmed."
        elif key == "barcode":
            note = "Network/project context appears; not enough by itself to define role."
        elif key in {"bnl", "bnl-01", "bnl 01"}:
            note = "BNL-facing interaction pattern; suggests the subject interacts with BNL as a system/persona."
        elif key in {"suno", "spotify", "soundcloud", "youtube"} or anchor_type in {"url", "domain"}:
            note = "Music-platform/link signal; ownership and submission status not confirmed."
        elif key in {"network", "edge", "lardcode"}:
            note = "BARCODE/lore-adjacent context appears; do not treat role or canon meaning as confirmed."
        anchors.append({"name": name, "type": anchor_type, "strength": strength, "note": note})
        if len(anchors) >= 12:
            break
    nearby = _nearby_context_signals(review_texts, set(counts.keys()), subject_key)
    return anchors, ignored_noise, nearby


def _nearby_context_signals(review_texts: list[str], owned_anchor_keys: set[str], subject_key: str) -> list[dict[str, Any]]:
    signals: dict[str, dict[str, Any]] = {}

    def add(name: str, reason: str) -> None:
        clean = _case_text(name, 120).strip(".,;:()[]{}<>\"'")
        key = _anchor_noise_key(clean)
        if not key or key in owned_anchor_keys or key == subject_key or _is_anchor_noise_key(key):
            return
        item = signals.setdefault(key, {
            "name": clean,
            "reason": reason,
            "confidence": "weak",
            "warning": "Nearby/co-mentioned/shared-topic context only; do not treat as a subject-owned anchor or fact without owned/explicit evidence.",
        })
        if item["confidence"] == "weak" and key in _MEANINGFUL_ANCHOR_TYPES:
            item["confidence"] = "moderate"

    for text in review_texts:
        for known in _MEANINGFUL_ANCHOR_TYPES:
            pattern = re.escape(known).replace("\\ ", r"\s+")
            if re.search(rf"(?<![-A-Za-z0-9]){pattern}(?![-A-Za-z0-9])", text, flags=re.I):
                display = "BNL-01" if known == "bnl-01" else ("BNL" if known == "bnl" else ("BARCODE" if known == "barcode" else ("EDGE" if known == "edge" else known[:1].upper() + known[1:])))
                add(display, "appears only in co-mentioned/shared-topic/review-only evidence")
        for match in re.findall(r"(?<![-A-Za-z0-9])[A-Z][A-Za-z0-9'’-]{2,}(?:-[A-Za-z0-9'’-]+)*\b", text):
            add(match, "appears only in co-mentioned/shared-topic/review-only evidence")
        if len(signals) >= 12:
            break
    return list(signals.values())[:12]


def _named_anchors(memory: dict[str, Any]) -> list[dict[str, Any]]:
    anchors, _ignored_noise, _nearby = _named_anchor_extraction(memory)
    return anchors


def _signals_from(memory: dict[str, Any], fields: tuple[str, ...], fallback: str, *, limit: int = 6) -> list[str]:
    context = _brief_context(memory)
    items: list[str] = []
    for field in fields:
        items.extend(_case_flatten_text_values(context.get(field), max_items=limit, item_limit=260))
    if not items and fields == ("relationshipSignals",):
        items.extend(_case_list(memory.get("relationshipContext"), limit=limit))
    deduped = _case_list(items, limit=limit, item_limit=260)
    return deduped or [fallback]


def _brief_signal_text(memory: dict[str, Any], topics: list[dict[str, Any]], anchors: list[dict[str, Any]], channels: list[dict[str, Any]] | None = None, ownership: dict[str, Any] | None = None) -> str:
    context = _brief_context(memory)
    if ownership:
        # Main-read shape is allowed to see only explicitly subject-owned or subject-attached authoritative text.
        # Topic labels, topic explanations, nearby context, review-only text, diagnostics, generated report
        # wording, and generic channel names are intentionally excluded here.
        values = [_main_read_eligible_texts(ownership)]
    else:
        shape_fields = (
            "knownContext", "usefulEvidence", "bestEvidenceToReview", "bnlInteractionSignals", "musicSignals",
            "communitySignals", "relationshipSignals", "evidenceDetails", "representativeEvidence",
        )
        values = [memory.get("representativeMoments")]
        values.extend(context.get(field) for field in shape_fields)
    fragments = []
    for text in _case_flatten_text_values(values, max_items=160, item_limit=300):
        lowered = text.lower()
        if any(generic in lowered for generic in ("no specific", "current report packet", "current evidence packet does not expose", "not connected to this report")):
            continue
        fragments.append(text)
    return json.dumps(fragments, default=str).lower()


def _queue_connected(memory: dict[str, Any]) -> bool:
    return str(_brief_context(memory).get("queueSubmissionStatus") or "not_connected").lower() not in {"", "not_connected", "unknown", "none"}


def _subject_archetype_read(subject: str, memory: dict[str, Any], profile: dict[str, Any], topics: list[dict[str, Any]], anchors: list[dict[str, Any]], channels: list[dict[str, Any]], ownership: dict[str, Any] | None = None) -> dict[str, Any]:
    body = _brief_signal_text(memory, topics, anchors, channels, ownership)
    total = _as_int(profile.get("totalEvidenceScanned")) or _as_int(profile.get("totalApprovedPublicAuthoredItems")) or len(memory.get("representativeMoments") or [])
    queue_connected = _queue_connected(memory)
    has_bnl = any(term in body for term in ("bnl-01", "bnl", "threshold", "boundary", "operational"))
    has_lore = any(term in body for term in ("orion", "interface", "lore", "sync", "convergence", "edge", "node", "liaison", "archive", "ai-proxy"))
    has_network_skeptic = bool(re.search(r"\b(network|barcode network)\b.*\b(why|motive|intent|trust|skeptic|question|what does|what are they)\b|\b(question|skeptic|doubt|trust)\w*\b.*\b(network|barcode network)\b", body, flags=re.I))
    has_weird_concrete = any(term in body for term in ("vibrating bed", "vibrating beds", "weird physical", "weird concrete", "odd concrete"))
    has_music = any(term in body for term in ("suno", "spotify", "soundcloud", "youtube", "music", "song", "track", "demo", "beat", "link"))
    has_collab = any(term in body for term in ("collab", "wip", "helper", "network helper"))
    has_show = any(term in body for term in ("radio", "show", "broadcast"))
    has_sponsor = any(term in body for term in ("sponsor", "ad entity", "advertiser", "promo"))
    has_operator = any(term in body for term in ("operator", "moderator", "mod/support", "admin support", "support subject"))
    has_chat = any(term in body for term in ("general-chat", "chat", "social", "community", "support chatter"))
    anchor_names = {str(a.get("name") or "").lower() for a in anchors}
    signals: list[str] = []
    if has_bnl:
        signals.append("BNL/source-file interaction appears repeatedly")
    if has_lore or "orion" in anchor_names:
        signals.append("Orion/interface/lore language appears in the subject-owned evidence")
    if has_weird_concrete:
        signals.append("weird concrete subjects appear in subject-owned evidence")
    if has_network_skeptic:
        signals.append("Network skepticism/questioning appears in subject-owned evidence")
    if has_music:
        signals.append("music platforms, music links, tracks, or song language appear")
    if has_collab:
        signals.append("collaboration, WIP, finished-track, or helper context appears")
    if has_show:
        signals.append("show/radio/broadcast context appears")
    if has_chat:
        signals.append("community conversation or social/support chatter appears")
    if queue_connected:
        signals.append("connected queue/submission status is present")

    if total <= 1 and len(signals) <= 1:
        archetype = "weak/unknown subject"
    elif has_sponsor and not (has_bnl or has_music or has_chat):
        archetype = "sponsor/ad entity"
    elif has_operator and not has_music:
        archetype = "operator/mod/support subject"
    elif queue_connected and has_music:
        archetype = "artist / music submitter"
    elif has_bnl and has_lore:
        archetype = "BNL-facing lore/interface subject"
    elif has_bnl:
        archetype = "BNL-facing interaction subject"
    elif has_weird_concrete or has_network_skeptic:
        archetype = "weird-topic community participant" if has_weird_concrete else "Network skeptic"
    elif has_music and has_collab:
        archetype = "collaborator / network helper"
    elif has_music:
        archetype = "music-link sharer"
    elif has_show:
        archetype = "show/radio participant"
    elif has_lore:
        archetype = "lore/interface subject"
    elif has_chat or (ownership and len(_main_read_eligible_texts(ownership)) >= 2) or total >= 4:
        archetype = "active community participant"
    else:
        archetype = "weak/unknown subject"

    if archetype != "weak/unknown subject" and sum(bool(x) for x in (has_bnl or has_lore, has_music, has_collab, has_show, has_sponsor, has_operator, has_chat, has_weird_concrete or has_network_skeptic)) >= 3:
        if not (queue_connected and has_music):
            archetype = "mixed subject / needs admin classification" if not (has_bnl and has_lore) else "BNL-facing lore/interface subject"
    confidence = "weak" if archetype == "weak/unknown subject" or total <= 1 else ("strong" if total >= 8 and len(signals) >= 2 else "moderate")
    missing: list[str] = []
    if has_music and not queue_connected:
        missing.append("confirmed owned music links or connected queue/submission history")
    if has_bnl or has_lore:
        missing.append("confirmed public-safe meaning for BNL/Orion/interface/lore anchors")
    if not queue_connected:
        missing.append("artist, submitter, played-track, payment, or Priority status")
    missing.append("public identity, alias certainty, and relationship meaning")
    if archetype == "weak/unknown subject":
        missing.insert(0, "specific recurring activity, channels, links, relationships, or public-safe role")
    review_body = " ".join(_review_only_shape_texts(ownership or {})).lower()
    excluded_shape = []
    if any(term in review_body for term in ("orion", "interface", "lore")) and not has_lore:
        excluded_shape.append("nearby Orion/interface/lore context is quarantined for review")
    rationale = _case_text(f"BNL classifies {subject} this way because the strongest main-read-eligible pattern is {', '.join(signals[:4]) if signals else 'too thin or generic to type safely'}. This is a shape read for internal review, not a public identity confirmation.", 520)
    return {"archetype": archetype, "confidence": confidence, "rationale": rationale, "distinguishingSignals": _case_list(signals or ["Evidence is too thin/generic to separate this subject from a generic Source File."], limit=6), "notEnoughEvidenceFor": _case_list(missing, limit=6), "excludedShapeSignals": excluded_shape}


def _distinguishing_read(archetype: dict[str, Any]) -> str:
    signals = " ".join(archetype.get("distinguishingSignals") or []).lower()
    name = str(archetype.get("archetype") or "")
    if "weak/unknown" in name:
        return "Not distinctive yet; current evidence is too thin or too generic."
    if "orion" in signals or "interface" in signals:
        return "Distinctive because main-read-eligible subject-owned evidence is Orion/interface-linked rather than generic community chatter."
    if "bnl" in signals:
        return "Distinctive because main-read-eligible subject-owned evidence is BNL-facing rather than generic community chatter."
    if any("orion/interface" in str(item).lower() for item in archetype.get("excludedShapeSignals") or []):
        return "Not distinctive enough from owned evidence; nearby Orion/interface context is quarantined for review."
    if "weird concrete" in signals or "network skepticism" in signals:
        return "Distinctive because subject-owned evidence includes weird concrete topics and/or Network skepticism rather than nearby lore context."
    if "music" in signals or "platform" in signals or "track" in signals:
        return "Distinctive because the strongest evidence is music/link sharing, not conversation volume or confirmed queue history."
    if "collaboration" in signals or "wip" in signals or "helper" in signals:
        return "Distinctive because the subject appears mainly through collaboration, WIP, finished-track, or network-helper contexts."
    if "radio" in signals or "show" in signals:
        return "Distinctive because show/radio participation is the clearest pattern."
    return "Distinctive mainly by recurring community participation; current evidence does not support a narrower artist, lore, or submitter read."


def _build_subject_read(subject: str, profile: dict[str, Any], topics: list[dict[str, Any]], channels: list[dict[str, Any]], archetype: dict[str, Any], behavioral_signature: dict[str, Any] | None = None) -> str:
    eligible_topics = [bucket.get("topic", "") for bucket in topics if bucket.get("mainReadEligible") and bucket.get("topic")]
    topic_names = ", ".join(eligible_topics[:4]) or "not enough main-read-eligible topic detail"
    missing = "; ".join((archetype.get("notEnoughEvidenceFor") or [])[:2])
    signature = behavioral_signature.get("signatureRead") if isinstance(behavioral_signature, dict) else ""
    signature_note = f" Subject-owned behavior note: {signature}" if signature else ""
    return _case_text(f"BNL reads {subject} as {archetype.get('archetype')} ({archetype.get('confidence')} confidence) based on {profile.get('activityLevel', 'unknown activity')} evidence. What makes this file different is: {_distinguishing_read(archetype)} The main-read-eligible evidence pattern runs through {topic_names}; nearby/review-only topic labels and generic channel names are not allowed to create the type.{signature_note} BNL is not allowed to assume {missing or 'public identity, role, relationship meaning, or queue history'} yet.", 1100)


def _build_bnl_take(subject: str, memory: dict[str, Any], topics: list[dict[str, Any]], anchors: list[dict[str, Any]], archetype: dict[str, Any], behavioral_signature: dict[str, Any] | None = None, nearby_context: list[dict[str, Any]] | None = None) -> str:
    name = str(archetype.get("archetype") or "weak/unknown subject")
    missing = "; ".join((archetype.get("notEnoughEvidenceFor") or [])[:3])
    signals = ", ".join((archetype.get("distinguishingSignals") or [])[:3])
    sig = behavioral_signature or {}
    sig_read = str(sig.get("signatureRead") or "")
    bnl_stance = (sig.get("stanceTowardBNL") or {}).get("stance") if isinstance(sig.get("stanceTowardBNL"), dict) else ""
    network_stance = (sig.get("stanceTowardNetwork") or {}).get("stance") if isinstance(sig.get("stanceTowardNetwork"), dict) else ""
    unusual_subjects = [item.get("subject") for item in (sig.get("unusualRecurringSubjects") or []) if isinstance(item, dict)]
    nearby_names = [item.get("name") for item in (nearby_context or []) if isinstance(item, dict)]
    has_nearby_orion = any(str(name).lower() in {"orion", "interface", "lore"} for name in nearby_names)
    if "orion" in sig_read.lower() and ("archive" in sig_read.lower() or "ai" in sig_read.lower()):
        text = f"BNL reads {subject} as an Orion/archive-facing subject when the owned evidence frames Orion as an AI, speaker, archive, or connected intelligence. This is review-only: relationship meaning and canon status still need explicit confirmation."
    elif bnl_stance in {"antagonistic", "playful_challenger"}:
        text = f"BNL reads {subject} as a BNL-facing antagonist or playful challenger because subject-owned evidence shows repeated challenge, pushback, teasing, or provocation toward BNL. That distinguishes the file from generic community chatter, but tone and public wording need review."
    elif unusual_subjects or network_stance in {"questioning", "skeptical"}:
        text = f"BNL reads {subject} as a weird-topic community participant whose owned evidence includes unusual recurring subjects{(': ' + ', '.join(unusual_subjects[:3])) if unusual_subjects else ''} and Network skepticism/questioning when supported by the cited fragments. Nearby context stays quarantined unless owned evidence supports it."
    elif "BNL-facing lore/interface" in name:
        text = f"BNL reads {subject} as a recurring BARCODE-side participant whose activity is unusually BNL-facing. The strongest pattern is not ordinary artist submission behavior; it is interface/lore-style interaction around {signals}. That makes {subject} more useful as a community/lore/interface subject than as a confirmed artist profile until queue history, public music ownership, and anchor meaning are confirmed."
    elif "music-link" in name:
        text = f"BNL reads {subject} as a music-link sharer: the useful signal is the presence of music platforms, tracks, songs, or link-sharing language. The packet should not turn those links into owned music, queue submissions, played tracks, or an artist profile unless connected queue data or public ownership evidence is added."
    elif "artist / music submitter" in name:
        text = f"BNL reads {subject} as an artist/music submitter candidate because music evidence and connected queue/submission status appear together. Admin should still verify which links are owned, which tracks were submitted or played, and what can be said publicly."
    elif "collaborator" in name:
        text = f"BNL reads {subject} as a collaboration/network-helper subject. The shape is less about standalone identity and more about WIP, finished-track, helper, or project-adjacent contexts; confirm owned work and public role before drafting."
    elif "active community" in name:
        text = f"BNL reads {subject} primarily as an active community participant. The evidence supports recurring conversation or support presence, not a confirmed artist, submitter, lore identity, or public relationship claim."
    elif "weak/unknown" in name:
        text = f"BNL does not have enough distinctive evidence to type {subject} beyond a weak Source File candidate. Add recurring activity, channel pattern, music/link details, relationship context, or queue/public-source confirmation before making a sharper read."
    else:
        text = f"BNL reads {subject} as {name}. The useful pattern is {signals or 'the available subject evidence'}, but the packet still cannot support {missing or 'public identity, role, relationship meaning, or queue history'} without admin confirmation."
    if has_nearby_orion and "orion/archive-facing" not in text.lower():
        text = f"{text} Nearby context references Orion, but BNL does not have subject-owned evidence to treat Orion as part of this subject’s profile."
    return _case_text(text, 980)


def _behavioral_signature_v1(subject: str, ownership: dict[str, Any]) -> dict[str, Any]:
    owned_texts = _case_list([item.get("text") for item in ownership.get("owned", [])], limit=80, item_limit=260)
    body = "\n".join(owned_texts).lower()

    def matches(pattern: str) -> list[str]:
        return [text for text in owned_texts if re.search(pattern, text, flags=re.I)][:4]

    recurring: list[dict[str, Any]] = []
    unusual: list[dict[str, Any]] = []

    def add_behavior(label: str, evidence: list[str], note: str) -> None:
        if not evidence:
            return
        recurring.append({
            "behavior": label,
            "strength": "strong" if len(evidence) >= 3 else ("moderate" if len(evidence) >= 2 else "weak"),
            "evidenceCount": len(evidence),
            "exampleSignals": evidence[:3],
            "note": note,
        })

    add_behavior("BNL challenge / pushback", matches(r"\b(antagoniz|challenge|push(?:es|ed)? back|teas(?:e|es|ed)|provok|argu(?:e|es|ed)|calls? out|tests? bnl)\b"), "Subject-owned evidence shows repeated BNL-facing challenge/pushback; keep tone review-only.")
    add_behavior("Orion/archive/AI framing", matches(r"\b(orion|archive|my ai|his ai|her ai|speaks? (?:for|through)|ai[- ]proxy|interface)\b"), "Subject-owned evidence uses Orion/archive/AI or interface language; relationship meaning remains review-only unless explicitly confirmed.")
    add_behavior("Network motive questioning", matches(r"\b(network|barcode network|barcode)\b.*\b(why|motive|intent|trust|skeptic|question|what does|what are they)\b|\b(question|skeptic|doubt|trust)\w*\b.*\b(network|barcode network)\b"), "Subject-owned evidence questions Network/BARCODE motives or intent.")

    concrete_patterns = [
        ("vibrating beds", r"\bvibrating beds?\b"),
        ("unusual physical/object/environment references", r"\b(vibrating|bed|mattress|object|physical|weird concrete|environment|room|device)\b"),
    ]
    for label, pattern in concrete_patterns:
        ev = matches(pattern)
        if ev:
            unusual.append({"subject": label, "evidenceCount": len(ev), "exampleSignals": ev[:3], "note": "Unusual concrete recurring subject appears in subject-owned evidence."})

    bnl_ev = matches(r"\b(bnl|bnl-01)\b")
    challenger_ev = matches(r"\b(antagoniz|challenge|push(?:es|ed)? back|teas(?:e|es|ed)|provok|argu(?:e|es|ed)|calls? out)\b")
    supportive_ev = matches(r"\b(support|thanks|help(?:s|ed)?|appreciat|boost)\b.*\b(bnl|bnl-01)\b")
    if challenger_ev:
        bnl_stance = "playful_challenger" if matches(r"\b(teas(?:e|es|ed)|playful|joke|provok)\b") else "antagonistic"
        bnl_conf = "strong" if len(challenger_ev) >= 3 else "moderate"
        bnl_note = "Subject-owned evidence shows direct challenge, antagonism, teasing, or pushback toward BNL."
        bnl_evidence = challenger_ev
    elif supportive_ev:
        bnl_stance, bnl_conf, bnl_note, bnl_evidence = "supportive", "moderate", "Subject-owned evidence has supportive BNL-facing language.", supportive_ev
    elif bnl_ev:
        bnl_stance, bnl_conf, bnl_note, bnl_evidence = "BNL-facing", "moderate", "Subject-owned evidence addresses or references BNL.", bnl_ev
    else:
        bnl_stance, bnl_conf, bnl_note, bnl_evidence = "unknown", "weak", "No subject-owned BNL stance is clear.", []

    network_question_ev = matches(r"\b(network|barcode network|barcode)\b.*\b(why|motive|intent|trust|skeptic|question|what does|what are they)\b|\b(question|skeptic|doubt|trust)\w*\b.*\b(network|barcode network)\b")
    network_lore_ev = matches(r"\b(network|barcode|edge|lardcode)\b")
    if network_question_ev:
        stance = "skeptical" if matches(r"\b(skeptic|doubt|trust)\w*\b") else "questioning"
        network = {"stance": stance, "confidence": "strong" if len(network_question_ev) >= 3 else "moderate", "evidence": network_question_ev, "note": "Subject-owned evidence questions Network/BARCODE motives or intent."}
    elif network_lore_ev:
        network = {"stance": "lore-engaged", "confidence": "moderate", "evidence": network_lore_ev[:3], "note": "Subject-owned evidence engages Network/BARCODE lore without a clear trust/skepticism stance."}
    else:
        network = {"stance": "unknown", "confidence": "weak", "evidence": [], "note": "No subject-owned Network stance is clear."}

    if not recurring and unusual:
        add_behavior("unusual concrete topic recurrence", [sig for item in unusual for sig in item.get("exampleSignals", [])], "Subject-owned evidence includes unusual concrete recurring topics.")
    if not recurring:
        add_behavior("general subject-owned participation", owned_texts[:2], "Subject-owned evidence exists but does not yet form a sharper behavioral pattern.")

    if "orion" in body and ("archive" in body or "ai" in body):
        read = f"BNL reads {subject}'s owned evidence as Orion/archive/AI-facing; relationship meaning remains review-only unless confirmed."
    elif challenger_ev:
        read = f"BNL reads {subject}'s owned evidence as BNL-facing challenge or playful antagonism rather than generic community chatter."
    elif unusual or network_question_ev:
        read = f"BNL reads {subject}'s owned evidence as unusual concrete-topic participation with Network skepticism/questioning when supported by the cited fragments."
    elif owned_texts:
        read = f"BNL has subject-owned evidence for {subject}, but the behavioral signature is still limited and review-only."
    else:
        read = f"BNL does not have enough subject-owned evidence to form a behavioral signature for {subject}."
    return {"signatureRead": _case_text(read, 420), "recurringBehaviors": recurring[:6], "stanceTowardBNL": {"stance": bnl_stance, "confidence": bnl_conf, "evidence": bnl_evidence[:4], "note": bnl_note}, "stanceTowardNetwork": network, "unusualRecurringSubjects": unusual[:6]}


def _source_file_gaps(memory: dict[str, Any], anchors: list[dict[str, Any]]) -> list[str]:
    gaps = _case_list(memory.get("openQuestions"), limit=6, item_limit=220)
    base = [
        "Decide the public display name to use, if any, and whether it is safe to use publicly.",
        "Clarify whether the subject is an artist, community member, lore/interface figure, collaborator, supporter, or something else.",
        "Explain what the Orion connection means before using it in public language." if any(a.get("name", "").lower() == "orion" for a in anchors) else "Check whether named anchors have any confirmed public meaning.",
        "Verify whether music links such as Suno are the subject's own work or links the subject shared.",
        "Connect actual queue submissions or played tracks before claiming submission/play history.",
        "Add public links/socials and mark which pieces are safe to say publicly.",
    ]
    return _case_list([*gaps, *base], limit=10, item_limit=240)



def _brief_ownership_routing_diagnostics(ownership: dict[str, Any], nearby_context: list[dict[str, Any]]) -> dict[str, Any]:
    summary = ownership.get("summary", {}) if isinstance(ownership, dict) else {}
    diagnostics = summary.get("ownershipRoutingDiagnostics") if isinstance(summary, dict) else None
    if not isinstance(diagnostics, dict):
        diagnostics = {"overpromotedCandidatesBlocked": [], "strictAuthorityWarnings": []}
    blocked = list(diagnostics.get("overpromotedCandidatesBlocked") or [])
    seen = {(str(item.get("term") or "").lower(), str(item.get("sourceLane") or "")) for item in blocked if isinstance(item, dict)}
    for item in nearby_context or []:
        if not isinstance(item, dict) or not item.get("name"):
            continue
        key = (str(item.get("name")).lower(), "nearbyContextSignals")
        if key in seen:
            continue
        blocked.append({
            "term": str(item.get("name")),
            "sourceLane": "nearbyContextSignals",
            "attemptedScope": "namedAnchors/subject-owned intelligence",
            "routedAs": "co_mention",
            "reason": str(item.get("warning") or item.get("reason") or "nearby context only; not subject-owned evidence"),
        })
        seen.add(key)
        if len(blocked) >= 20:
            break
    return {
        "overpromotedCandidatesBlocked": blocked[:20],
        "strictAuthorityWarnings": _case_list(diagnostics.get("strictAuthorityWarnings") or [], limit=12, item_limit=240),
    }


def _main_read_routing_diagnostics(ownership: dict[str, Any], topics: list[dict[str, Any]], nearby_context: list[dict[str, Any]]) -> dict[str, Any]:
    excluded: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()

    def add(signal: str, reason: str, routed_to: str) -> None:
        clean = _case_text(signal, 180)
        if not clean:
            return
        key = (clean.lower(), routed_to)
        if key in seen:
            return
        seen.add(key)
        excluded.append({"signal": clean, "reason": _case_text(reason, 220), "routedTo": routed_to})

    for text in _review_only_shape_texts(ownership):
        lowered = text.lower()
        if any(term in lowered for term in ("orion", "interface", "lore")):
            add("Orion/interface/lore", "Review-only or nearby context lacks main-read-eligible subject ownership/explicit relationship evidence.", "nearbyContextSignals")
        elif len(excluded) < 8:
            add(text, "Review-only evidence is available for diagnostics only and cannot shape subjectRead, bnlTake, or archetype.", "ownershipRoutingDiagnostics")
        if len(excluded) >= 16:
            break
    for bucket in topics:
        if not isinstance(bucket, dict) or bucket.get("mainReadEligible"):
            continue
        topic = str(bucket.get("topic") or "")
        if topic:
            add(topic, "Topic bucket has no main-read-eligible owned signal; it remains review-only.", "reviewOnlyTopic")
    for item in nearby_context or []:
        if isinstance(item, dict) and item.get("name"):
            add(str(item.get("name")), str(item.get("warning") or "Nearby context only; not subject-owned shape evidence."), "nearbyContextSignals")
    return {"excludedShapeSignals": excluded[:24]}

def build_subject_intelligence_brief_v1(memory: dict[str, Any]) -> dict[str, Any]:
    subject = _case_text(memory.get("subjectName") or "this subject", 140)
    subject_key = _case_text(memory.get("subjectKey") or _subject_key(subject), 140)
    confirmed_aliases = _case_list(memory.get("confirmedAliases") or memory.get("aliases"), limit=12, item_limit=120)
    ownership = subject_owned_text_fragments(memory, subject, subject_key, confirmed_aliases)
    profile = _subject_activity_profile(memory)
    channels = _channel_breakdown(memory)
    topics = _topic_buckets(memory, ownership)
    anchors, ignored_noise, nearby_context = _named_anchor_extraction(memory, ownership)
    behavioral_signature = _behavioral_signature_v1(subject, ownership)
    archetype = _subject_archetype_read(subject, memory, profile, topics, anchors, channels, ownership)
    distinguishing = _distinguishing_read(archetype)
    music = _signals_from(memory, ("musicSignals", "bestEvidenceToReview", "representativeEvidence"), "No specific music/link signal is confirmed in the current packet; do not infer submissions or ownership.")
    relationship = _signals_from(memory, ("relationshipSignals",), "No relationship signal is confirmed; keep any relationship interpretation review-only.")
    queue_connected = str(_brief_context(memory).get("queueSubmissionStatus") or "not_connected") not in {"", "not_connected", "unknown"}
    queue_read = _case_text(_brief_context(memory).get("queueSubmissionNote") or _join_case_items(memory.get("queueSubmissionContext"), QUEUE_NOT_CONNECTED_NOTE), 360)
    if not queue_connected and "not connected" not in queue_read.lower():
        queue_read = f"Queue/submission data is not connected. {queue_read}"
    confidence = _case_text(f"Archetype confidence is {archetype.get('confidence')} for the internal shape read. Safety remains strict: identity, public role, relationship meaning, queue/submission claims, payment/Priority status, and public-safe anchor meaning stay unconfirmed until admin review connects source links. This brief is admin-facing and review-only.", 520)
    gaps = _source_file_gaps(memory, anchors)
    return _sanitize_archive_value({
        "subjectRead": _build_subject_read(subject, profile, topics, channels, archetype, behavioral_signature),
        "bnlTake": _build_bnl_take(subject, memory, topics, anchors, archetype, behavioral_signature, nearby_context),
        "subjectArchetypeRead": archetype,
        "distinguishingRead": distinguishing,
        "activityProfile": profile,
        "channelBreakdown": channels,
        "topicBuckets": topics,
        "namedAnchors": anchors,
        "nearbyContextSignals": nearby_context,
        "evidenceOwnershipSummary": ownership.get("summary", {}),
        "ownershipRoutingDiagnostics": _brief_ownership_routing_diagnostics(ownership, nearby_context),
        "mainReadRoutingDiagnostics": _main_read_routing_diagnostics(ownership, topics, nearby_context),
        "behavioralSignatureV1": behavioral_signature,
        "ignoredExtractionNoise": ignored_noise,
        "interactionPatterns": _signals_from(memory, ("bnlInteractionSignals", "communitySignals", "conversationThemes"), "No specific interaction pattern is exposed beyond the representative evidence."),
        "musicAndLinkSignals": music,
        "relationshipSignals": [item if any(term in item.lower() for term in ("review-only", "unconfirmed", "not confirm", "uncertain")) else f"{item}. Treat as review-only until confirmed." for item in relationship],
        "queueSubmissionRead": queue_read,
        "confidenceRead": confidence,
        "sourceFileGaps": gaps,
        "recommendedAdminActions": _case_list([
            "Review the top topic buckets and representative evidence before drafting any public dossier language.",
            "Use Identity Check/public links to confirm display name, role, aliases, and socials without auto-confirming identity.",
            "If music links appear, separate owned tracks, shared links, queue submissions, and played tracks.",
            "Promote only public-safe facts; keep relationship/lore/interface interpretation internal until confirmed.",
        ], limit=8, item_limit=240),
        "doNotSayPubliclyYet": _case_list([
            "Do not claim queue submissions, played tracks, payment, or Priority status without connected queue data.",
            "Do not claim public identity, public role, alias certainty, or relationship meaning from review-only evidence.",
            "Do not publish raw channel context, raw IDs, internal table/debug labels, or private-source detail.",
            "Do not present Orion/Network/EDGE/BARCODE/BNL-01 anchors as confirmed public facts until admin review marks them safe.",
        ], limit=8, item_limit=240),
    })


def build_subject_memory_packet_v1(packet: dict[str, Any]) -> dict[str, Any]:
    """Build evidence-grounded working memory for the BNL-authored case report."""

    source_file = packet.get("sourceFile") if isinstance(packet.get("sourceFile"), dict) else {}
    subject = _case_text(packet.get("subject") or _first(source_file, ("name", "sourceFileName", "subject", "displayName", "title", "normalizedName")), 140)
    subject_key = _subject_key(subject or _first(source_file, ("normalizedName", "subjectKey", "slug")))
    generated_at = _packet_generated_at(packet)
    moments = _collect_representative_moments(packet, subject)
    music_items = _generic_label_gap(packet, "musicSignals", "music/creative context")
    if music_items and all("No specific" in item for item in music_items):
        music_items.append("When music/creative details are unavailable, BNL must not claim a submitted song, artist profile, platform ownership, collaborator, or public link.")
    queue_connected = str(packet.get("queueSubmissionStatus") or "not_connected") not in {"", "not_connected", "unknown"}
    queue_context = [
        _case_text(packet.get("queueSubmissionNote") or ("Queue/submission data is connected to this report." if queue_connected else "Queue/submission memory is not connected to this report. Do not claim submissions, song counts, play history, payment, or Priority status."), 320)
    ]
    if queue_connected:
        queue_context.extend(_case_list(packet.get("queueSubmissionFacts") or packet.get("queueSubmissions"), limit=4))
    identity_signals = _case_list(packet.get("subjectIdentity") or packet.get("linkSignalDiagnostics") or packet.get("publicUseCandidates"), limit=5)
    if not identity_signals:
        identity_signals = ["Identity review is handled through the site Identity Check when links are available. No identity is auto-confirmed by this report."]
    elif not any("not auto" in item.lower() or "review" in item.lower() for item in identity_signals):
        identity_signals.append("Identity signals are review inputs only; this report does not auto-confirm identity or merge aliases.")

    packet_v1 = {
        "version": SUBJECT_MEMORY_PACKET_VERSION,
        "generatedAt": generated_at,
        "subjectName": subject,
        "subjectKey": subject_key,
        "memoryCoverage": _infer_subject_memory_coverage(packet),
        "identitySignals": identity_signals,
        "discordActivity": _case_list(packet.get("conversationHighlights") or packet.get("bestEvidenceToReview"), limit=6) or ["Discord activity is only represented by archive evidence; no raw transcript is exposed to this report."],
        "websiteContext": _case_list([f"Website/Source File match target is {packet.get('matchKind') or 'unknown'}.", *(_case_list(source_file, limit=3) if source_file else [])], limit=5),
        "queueSubmissionContext": queue_context,
        "musicCreativeContext": music_items,
        "relationshipContext": _relationship_review_items(packet, subject),
        "communityContext": _generic_label_gap(packet, "communitySignals", "community context"),
        "repeatedTopics": _generic_label_gap(packet, "topicBreakdown", "repeated topics"),
        "sourceIntelligenceContext": _build_subject_intelligence_context(packet),
        "representativeMoments": moments,
        "publicSafeCandidateFacts": _case_list(packet.get("publicSafePossibilities") or packet.get("publicUseCandidates"), limit=6) or ["No public-safe fact is confirmed by this packet; owner review is required before public use."],
        "reviewOnlyFacts": _case_list(packet.get("reviewOnlyEvidence") or packet.get("privateOnlyNotes") or packet.get("notPublicYet"), limit=8),
        "openQuestions": _case_list(packet.get("missingInfo") or _section_text(packet.get("sections") or {}, "Missing Info", limit=8), limit=8) or ["Which identity/display name, public role, public links, and queue/submission facts are safe to use?"],
        "internalWarnings": _case_list(packet.get("warnings") or packet.get("privateOnlyNotes") or packet.get("notPublicYet"), limit=8) or ["Do not publish, merge identities, auto-confirm aliases, or infer public facts from review-only evidence."],
        "evidenceGaps": [],
    }
    if not moments:
        packet_v1["evidenceGaps"].append("The current memory path did not expose representative event details; the report must state evidence limitations instead of inventing specifics.")
    if not packet.get("musicSignals"):
        packet_v1["evidenceGaps"].append("The current memory path exposes no specific music/creative details to the report generator; do not pretend songs, platforms, collaborators, or links are confirmed.")
    if packet.get("musicSignals") and not any(any(term in item.lower() for term in ("#", "http", "suno", "demo", "wip", "beat", "track", "song", "platform", "link")) for item in music_items):
        packet_v1["evidenceGaps"].append("The current memory path exposes only category labels for music discussion; raw representative music details are not available to the report generator yet.")
    if not queue_connected:
        packet_v1["evidenceGaps"].append("Queue/submission memory is not connected to this report. Do not claim submissions, song counts, play history, payment, or Priority status.")
    return _sanitize_archive_value(packet_v1)


def _join_case_items(items: Any, fallback: str, limit: int = 3) -> str:
    values = _case_list(items, limit=limit, item_limit=260)
    if not values:
        return fallback
    return " ".join(f"{item}." if item and item[-1] not in ".!?" else item for item in values)


def build_source_file_case_report_v1(subject_memory_packet: dict[str, Any]) -> dict[str, Any]:
    """Build BNL's internal analyst report from subjectMemoryPacketV1 only."""

    memory = subject_memory_packet or {}
    subject = _case_text(memory.get("subjectName") or "this subject", 140)
    subject_key = _case_text(memory.get("subjectKey") or _subject_key(subject), 140)
    moments = memory.get("representativeMoments") if isinstance(memory.get("representativeMoments"), list) else []
    has_moments = bool(moments)
    evidence_summary_bits = []
    for moment in moments[:3]:
        if isinstance(moment, dict):
            channel = f" in {moment.get('channelName')}" if moment.get("channelName") else ""
            when = f" around {moment.get('occurredAt')}" if moment.get("occurredAt") else ""
            evidence_summary_bits.append(_case_text(f"{moment.get('summary')} ({moment.get('sourceType')}{channel}{when}; {moment.get('visibility')})", 320))
    if not evidence_summary_bits:
        evidence_summary_bits.append("The current archive does not expose representative event details to the case report; use the packet's evidence gaps before making claims.")
    report = {
        "version": SOURCE_FILE_CASE_REPORT_VERSION,
        "generatedAt": _case_text(memory.get("generatedAt") or datetime.now(timezone.utc).isoformat(), 80),
        "subjectName": subject,
        "subjectKey": subject_key,
        "reportStatus": "internal_review_only_evidence_grounded" if has_moments else "internal_review_only_evidence_limited",
        "caseSummary": _case_text(f"{subject} is being reviewed as an internal Source File subject. The strongest usable read comes from the subject memory packet: {evidence_summary_bits[0]} This report does not publish, merge identities, or convert review-only evidence into public fact.", 650),
        "dossierUse": _case_text("Use this as an internal case-file readout for admin review. Draft public dossier language only after identity, public-safe role, public links, relationship meaning, and queue/submission status are reviewed.", 420),
        "publicSafeClaims": _case_list(memory.get("publicSafeCandidateFacts"), limit=6) or ["No public-safe claim is confirmed yet; owner/admin review is required before public use."],
        "evidenceSummary": evidence_summary_bits,
        "communityContext": _join_case_items(memory.get("communityContext"), "Community context is not specifically detailed in the current packet; do not claim more than the available evidence supports."),
        "creativeMusicContext": _join_case_items(memory.get("musicCreativeContext"), "No specific collaborator, track title, platform ownership, submitted queue track, or public link is confirmed in the current report packet."),
        "relationshipContext": _join_case_items(memory.get("relationshipContext"), "No specific relationship/context connection is confirmed; keep relationship interpretation review-only."),
        "queueSubmissionContext": _join_case_items(memory.get("queueSubmissionContext"), "Queue/submission memory is not connected to this report. Do not claim submissions, song counts, play history, payment, or Priority status."),
        "identityContext": _case_text(_join_case_items(memory.get("identitySignals"), "Identity review is handled through the site Identity Check when links are available. This report does not auto-confirm identity.") + " This report does not auto-confirm identity.", 520),
        "reviewBlockers": _case_list(memory.get("openQuestions"), limit=8),
        "internalOnlyNotes": _case_list(memory.get("internalWarnings") or memory.get("reviewOnlyFacts"), limit=8) or ["Keep review-only evidence internal; do not publish or merge identities from this report."],
        "recommendedNextSteps": [
            "Review the representative moments and evidence gaps before drafting a public dossier.",
            "Confirm public-safe display name, role, public links, identity links, and queue/submission status through the existing admin workflows.",
            "Keep this as Source File material unless an admin decides the evidence is sufficient for public-safe drafting.",
        ],
        "confidenceNotes": _case_list(memory.get("evidenceGaps"), limit=8) or ["Confidence is limited to the visible subject memory packet; no raw private transcripts are included."],
        "memoryCoverage": memory.get("memoryCoverage") or {},
        "subjectIntelligenceBriefV1": build_subject_intelligence_brief_v1(memory),
    }
    return _sanitize_archive_value(report)

def _source_package_from_packet(packet: dict[str, Any]) -> dict[str, Any]:
    """Build the full review-only archive package without compact-card trimming."""

    excluded = {"payload", "sendResult", "archiveResult", "sourcePackage", "subjectMemoryPacketV1", "sourceFileCaseReportV1"}
    package = {key: value for key, value in (packet or {}).items() if key not in excluded}
    package["archiveNotice"] = "Internal/review-only Source File archive package. No public publishing, draft creation, tag creation, queue/payment action, identity merge, or alias confirmation is requested."
    return _sanitize_archive_value(package)


def build_source_file_archive_payload(packet: dict[str, Any], *, environ: dict[str, str] | None = None) -> dict[str, Any]:
    """Build the full Source File archive envelope for the site archive endpoint."""

    source_file = packet.get("sourceFile") if isinstance(packet.get("sourceFile"), dict) else {}
    compact_summary = build_compact_enrichment_evidence_summary(packet)
    subject = _safe_text(packet.get("subject") or _first(source_file, ("name", "sourceFileName", "subject", "displayName", "title", "normalizedName")), 140)
    subject_key = _subject_key(subject or _first(source_file, ("normalizedName", "subjectKey", "slug")))
    candidate_id = _first(source_file, ("candidateId", "targetCandidateId", "candidate_id", "id"))
    sections = packet.get("sections") or {}
    receipt = {
        "qualityScore": packet.get("qualityScore"),
        "qualityStatus": packet.get("qualityStatus"),
        "sourceCounts": dict(packet.get("sourceCounts") or {}),
        "sourceTypes": list(packet.get("sourceTypes") or []),
        "warningCounts": dict(packet.get("warningCounts") or {}),
        "warnings": list(packet.get("warnings") or []),
        "matchKind": packet.get("matchKind"),
        "runTime": packet.get("runTime"),
    }
    subject_memory_packet = build_subject_memory_packet_v1(packet)
    case_report = build_source_file_case_report_v1(subject_memory_packet)
    envelope = {
        "candidateId": _sanitize_archive_value(candidate_id) if candidate_id else None,
        "subjectName": subject,
        "subjectKey": subject_key,
        "ingestKey": packet.get("ingestKey") or deterministic_enrichment_ingest_key(subject, str(packet.get("matchKind") or "none"), sections),
        "compactSummary": compact_summary,
        "publicSafePossibilities": _sanitize_archive_value(list(packet.get("publicSafePossibilities") or [])),
        "missingInfo": _sanitize_archive_value(_section_text(sections, "Missing Info", limit=10) or packet.get("missingInfo") or ""),
        "publicSafetyNotes": _sanitize_archive_value(_section_text(sections, "Public-Safe Notes", limit=10) or packet.get("publicSafetyNotes") or ""),
        "doNotSay": _sanitize_archive_value(_section_text(sections, "Do Not Say", limit=10) or packet.get("doNotSay") or ""),
        "evidenceReceiptSummary": _sanitize_archive_value(receipt),
        "sourceFileBriefV2": _sanitize_archive_value(build_source_file_brief_v2(packet, archive_id=packet.get("archiveId") or "")),
        "subjectMemoryPacketV1": subject_memory_packet,
        "sourceFileCaseReportV1": case_report,
        "sourcePackage": _source_package_from_packet(packet),
    }
    return envelope

def build_enrichment_recommendation_payload(packet: dict[str, Any], *, environ: dict[str, str] | None = None) -> dict[str, Any]:
    sections = packet.get("sections") or {}
    subject = str(packet.get("subject") or "").strip()
    match_kind = str(packet.get("matchKind") or "none")
    source_file = packet.get("sourceFile") if isinstance(packet.get("sourceFile"), dict) else {}
    target_candidate_id = _first(source_file, ("candidateId", "targetCandidateId", "id")) if match_kind in {"candidate_intake", "active_source_file"} else None
    target_dossier_id = _first(source_file, ("targetDossierId", "dossierId", "publicDossierId")) if match_kind == "existing_dossier_update" else None
    _validate_source_payload_fields(packet)
    evidence = build_compact_enrichment_evidence_summary(packet)
    payload = {
        "type": "modify_existing_dossier" if match_kind in {"active_source_file", "existing_dossier_update", "candidate_intake"} else "new_subject",
        "targetCandidateId": target_candidate_id,
        "targetDossierId": target_dossier_id,
        "subjectName": subject,
        "reason": f"Review-only Source File enrichment generated by BNL. Match target: {match_kind}. No publishing, promotion, merge, or alias confirmation is requested.",
        "evidenceSummary": evidence,
        "publicSafetyNotes": _section_text(sections, "Public-Safe Notes", limit=5),
        "doNotSay": _section_text(sections, "Do Not Say", limit=5),
        "missingInfo": _section_text(sections, "Missing Info", limit=5),
        "suggestedAction": _section_text(sections, "Suggested Next Action", limit=3),
        "sourceLanes": ["source_file_enrichment"] + list(packet.get("sourceTypes") or []),
        "sourceTypes": list(packet.get("sourceTypes") or []),
        "sourceCounts": dict(packet.get("sourceCounts") or {}),
        "warningCounts": dict(packet.get("warningCounts") or {}),
        "qualityScore": packet.get("qualityScore"),
        "qualityStatus": packet.get("qualityStatus"),
        "forced": bool(packet.get("forced")),
        "safetyWarnings": list(packet.get("warnings") or []),
        "sourceCoverage": list(packet.get("sourceCoverage") or []),
        "identityDiagnostics": dict(packet.get("diagnostics") or {}),
        "internalClassification": dict(packet.get("classification") or {}),
        "knownContext": list(packet.get("knownContext") or []),
        "usefulEvidence": list(packet.get("usefulEvidence") or packet.get("evidenceDetails") or []),
        "relationshipSignals": list(packet.get("relationshipSignals") or []),
        "publicSafePossibilities": list(packet.get("publicSafePossibilities") or []),
        "privateOnlyNotes": list(packet.get("privateOnlyNotes") or []),
        "notPublicYet": list(packet.get("notPublicYet") or []),
        "sourceAuthority": list(packet.get("sourceAuthority") or []),
        "observedChannels": list(packet.get("observedChannels") or []),
        "conversationHighlights": list(packet.get("conversationHighlights") or []),
        "topicBreakdown": list(packet.get("topicBreakdown") or []),
        "bestEvidenceToReview": list(packet.get("bestEvidenceToReview") or []),
        "bnlInteractionSignals": list(packet.get("bnlInteractionSignals") or []),
        "musicSignals": list(packet.get("musicSignals") or []),
        "communitySignals": list(packet.get("communitySignals") or []),
        "evidenceDetails": list(packet.get("evidenceDetails") or []),
        "representativeEvidence": list(packet.get("representativeEvidence") or []),
        "topChannels": list(packet.get("topChannels") or []),
        "topTopicDetails": list(packet.get("topTopicDetails") or []),
        "activityFrequencySummary": dict(packet.get("activityFrequencySummary") or {}),
        "recentActivitySummary": packet.get("recentActivitySummary") or "",
        "authoredVsMentionedSummary": packet.get("authoredVsMentionedSummary") or "",
        "publicUseCandidates": list(packet.get("publicUseCandidates") or []),
        "reviewOnlyEvidence": list(packet.get("reviewOnlyEvidence") or []),
        "queueSubmissionStatus": packet.get("queueSubmissionStatus") or "not_connected",
        "queueSubmissionNote": packet.get("queueSubmissionNote") or QUEUE_NOT_CONNECTED_NOTE,
        "entityIntelligenceProfile": dict(packet.get("entityIntelligenceProfile") or {}),
        "linkSignalDiagnostics": dict(packet.get("linkSignalDiagnostics") or {}),
        "rawProvenance": dict(packet.get("rawProvenance") or {}),
        "visibilityLabels": ["internal_review_only", "admin_review_required"],
        "confidence": "low" if packet.get("qualityStatus") in {"too_thin", "forced_low_confidence"} else ("medium" if match_kind == "active_source_file" else "low"),
        "createdBy": "bnl",
        "ingestSource": source_enrichment_ingest_source(environ),
        "ingestKey": deterministic_enrichment_ingest_key(subject, match_kind, sections),
    }
    _validate_source_payload_fields(payload)
    compact_payload = compact_enrichment_payload_for_site(build_dossier_recommendation_payload(payload))
    compact_payload = sanitize_compact_recommendation_payload(compact_payload, packet=packet)
    _validate_source_payload_fields(compact_payload)
    compact_payload["siteValidationIssues"] = validate_enrichment_payload_for_site(compact_payload)
    if not compact_payload["siteValidationIssues"]:
        compact_payload.pop("siteValidationIssues", None)
    return compact_payload



def _call_site_sender(sender: Callable[..., dict[str, Any]], payload: dict[str, Any], *, environ: dict[str, str] | None = None, callback_base_url: str | None = None) -> dict[str, Any]:
    """Call a site sender with routing kwargs when its signature supports them."""

    kwargs: dict[str, Any] = {}
    try:
        sig = inspect.signature(sender)
        params = sig.parameters
        accepts_kwargs = any(param.kind == inspect.Parameter.VAR_KEYWORD for param in params.values())
        if accepts_kwargs or "environ" in params:
            kwargs["environ"] = environ
        if callback_base_url and (accepts_kwargs or "callback_base_url" in params):
            kwargs["callback_base_url"] = callback_base_url
    except (TypeError, ValueError):
        kwargs = {}
    return sender(payload, **kwargs) if kwargs else sender(payload)


def run_source_file_enrichment(
    db_path: str,
    guild_id: int | None,
    subject: str,
    *,
    dry_run: bool = False,
    rd_context: list[dict[str, Any]] | None = None,
    lookup_func: Callable[[dict[str, str]], dict[str, Any]] = lookup_source_file,
    sender: Callable[[dict[str, Any]], dict[str, Any]] = send_dossier_recommendation,
    archive_sender: Callable[[dict[str, Any]], dict[str, Any]] = send_source_file_archive_enrichment,
    environ: dict[str, str] | None = None,
    lookup_key: str = "subject",
    lookup_value: str | None = None,
    force: bool = False,
    diagnostics: bool = False,
    callback_base_url: str | None = None,
) -> dict[str, Any]:
    canonical_key = lookup_key if lookup_key in {"subject", "alias", "candidateId", "normalizedName"} else "subject"
    target_value = (lookup_value if lookup_value is not None else subject) or ""
    query = {"lookupKey": canonical_key, "lookupValue": target_value, canonical_key: target_value}
    lookup_result = lookup_func(query)
    match_kind, match_note, source_file = classify_source_match(lookup_result)
    resolution_mode = {"subject": "exact", "candidateId": "candidateId", "alias": "alias", "normalizedName": "exact"}.get(canonical_key, "exact")

    if match_kind == "none":
        possible_matches = _possible_match_items(lookup_result)
        if possible_matches:
            note = "Possible match found, but enrichment needs an exact active Source File target."
            if canonical_key == "alias":
                note = "Alias lookup only returned unconfirmed possible matches; use candidateId after review."
            expansion_note = _simple_expansion_note(target_value, possible_matches)
            if expansion_note:
                note = f"{note} {expansion_note}"
            return _possible_match_review_result(target_value, lookup_result, dry_run=dry_run, resolution_mode="possible_match_review", match_note=note)
        return {
            "ok": True,
            "dryRun": dry_run,
            "subject": _safe_text(target_value, 90),
            "status": "no_target",
            "matchKind": match_kind,
            "matchNote": match_note,
            "possibleMatches": [],
            "possibleMatchCount": 0,
            "possibleMatchNames": [],
            "resolutionMode": "no_target",
            "sections": {},
            "sourceCounts": {},
            "warningCounts": {},
            "sourceTypes": [],
            "warnings": [],
            "sent": False,
            "sendResult": {},
            "runTime": datetime.now(timezone.utc).isoformat(),
        }
    if match_kind == "error":
        return {
            "ok": False,
            "dryRun": dry_run,
            "subject": _safe_text(target_value, 90),
            "status": "lookup_failed",
            "matchKind": match_kind,
            "matchNote": match_note,
            "possibleMatches": [],
            "possibleMatchCount": 0,
            "possibleMatchNames": [],
            "resolutionMode": "no_target",
            "sections": {},
            "sourceCounts": {},
            "warningCounts": {},
            "sourceTypes": [],
            "warnings": [],
            "sent": False,
            "sendResult": {},
            "runTime": datetime.now(timezone.utc).isoformat(),
        }

    if canonical_key == "alias" and not _is_confirmed_alias_lookup(lookup_result):
        possible_result = dict(lookup_result)
        data = possible_result.get("data") if isinstance(possible_result.get("data"), dict) else {}
        source_name = _first(_source_file_obj(possible_result), ("name", "sourceFileName", "subject", "displayName", "title", "normalizedName"))
        data = dict(data)
        data["possibleMatches"] = data.get("possibleMatches") or [{"name": source_name or target_value, "matchKind": lookup_result.get("matchKind") or "unconfirmed_alias", "candidateId": _first(_source_file_obj(possible_result), ("candidateId", "candidate_id", "id"))}]
        possible_result["data"] = data
        return _possible_match_review_result(target_value, possible_result, dry_run=dry_run, resolution_mode="possible_match_review", match_note="Alias lookup was not a confirmed alias match; use candidateId after review.")

    effective_subject = _first(source_file, ("name", "sourceFileName", "subject", "displayName", "title", "normalizedName")) or target_value
    with refresh_generation_context(str(effective_subject)):
        evidence = collect_source_enrichment_evidence(db_path, guild_id, str(effective_subject), rd_context=rd_context, lookup_result=lookup_result)
        entity_profile = build_entity_intelligence_profile(db_path, guild_id, str(effective_subject), refresh=True)
    entity_resolver = resolve_entity_context_for_surface(entity_profile, "source_file", max_items=16)
    entity_diag_for_log = entity_profile.get("diagnostics") or {}
    scope_for_log = entity_diag_for_log.get("rowScopeCounts") or {}
    logging.info(
        "source_enrichment entity_intelligence_rows_collected subject_key=%s owned=%s keyed=%s authored=%s co_mention=%s global_mixed=%s source_blind=%s rejected=%s",
        entity_profile.get("subjectKey") or "unknown",
        int(scope_for_log.get("subject_owned") or 0), int(scope_for_log.get("subject_keyed") or 0), int(scope_for_log.get("subject_authored") or 0),
        int(scope_for_log.get("subject_co_mention") or 0), int(scope_for_log.get("global_mixed_memory") or 0), int(scope_for_log.get("source_blind_global") or 0), int(scope_for_log.get("rejected") or 0),
    )
    _merge_entity_intelligence_into_evidence(evidence, entity_profile, entity_resolver)
    packet = {
        "ok": True,
        "dryRun": dry_run,
        "subject": _safe_text(effective_subject, 90),
        "status": "dry_run" if dry_run else "ready_to_send",
        "matchKind": match_kind,
        "matchNote": match_note,
        "sourceFile": source_file,
        "possibleMatches": [],
        "possibleMatchCount": 0,
        "possibleMatchNames": [],
        "resolutionMode": resolution_mode,
        "sections": evidence["sections"],
        "sourceCounts": evidence["sourceCounts"],
        "warningCounts": evidence["warningCounts"],
        "sourceTypes": evidence["sourceTypes"],
        "warnings": evidence["warnings"],
        "subjectIdentity": evidence.get("subjectIdentity") or {},
        "diagnostics": evidence.get("diagnostics") or {},
        "classification": evidence.get("classification") or {},
        "sourceTypesChecked": (evidence.get("diagnostics") or {}).get("sourceTypesChecked", {}),
        "sourceTypesSkipped": (evidence.get("diagnostics") or {}).get("sourceTypesSkipped", {}),
        "runTime": datetime.now(timezone.utc).isoformat(),
        "forced": bool(force),
        "diagnosticsRequested": bool(diagnostics),
    }
    _copy_evidence_fields(packet, evidence)
    packet["subjectIntelligenceDiagnostics"] = evidence.get("subjectIntelligenceDiagnostics") or {}
    packet["linkSignalDiagnostics"] = evidence.get("linkSignalDiagnostics") or {}
    packet["diagnosticsRequested"] = bool(diagnostics)
    quality = evaluate_enrichment_quality(packet)
    packet["qualityScore"] = quality["score"]
    packet["qualityStatus"] = "forced_low_confidence" if force and quality["status"] != "sendable" else quality["status"]
    packet["suppressedReason"] = quality["suppressedReason"]
    packet["previewBullets"] = quality["previewBullets"]
    packet["previewBulletsCount"] = quality["previewBulletsCount"]
    payload = build_enrichment_recommendation_payload(packet, environ=environ)
    packet["payload"] = payload
    packet["ingestKey"] = payload.get("ingestKey")
    packet["ingestSource"] = payload.get("ingestSource")
    if dry_run:
        packet["sent"] = False
        packet["sendResult"] = {}
        return packet
    if quality["status"] != "sendable" and not force:
        packet["sent"] = False
        packet["sendResult"] = {"ok": False, "error": quality["suppressedReason"]}
        packet["status"] = "suppressed_too_thin"
        return packet
    archive_payload = build_source_file_archive_payload(packet, environ=environ)
    packet["archivePayload"] = archive_payload
    packet["subjectMemoryPacketGenerated"] = bool((archive_payload or {}).get("subjectMemoryPacketV1"))
    packet["caseReportGenerated"] = bool((archive_payload or {}).get("sourceFileCaseReportV1"))
    archive_required = is_source_file_archive_token_configured(environ)
    if archive_required:
        archive_result = _call_site_sender(archive_sender, archive_payload, environ=environ, callback_base_url=callback_base_url)
    else:
        archive_result = {"ok": True, "archiveId": None, "error": "", "status": "not_configured_skipped"}
        logging.info("source_file_archive_send_skipped reason=archive_token_not_configured subject_key=%s", _subject_key(subject))
    packet["archiveResult"] = archive_result
    packet["archiveSent"] = bool(archive_required and (archive_result or {}).get("ok"))
    packet["archiveStatus"] = (archive_result or {}).get("status")
    packet["archiveId"] = (archive_result or {}).get("archiveId") or ""
    packet["archiveError"] = _safe_text((archive_result or {}).get("error") or "", 240)

    payload = sanitize_compact_recommendation_payload(payload, packet=packet, archive_id=packet.get("archiveId") or "")
    packet["payload"] = payload
    send_result = _call_site_sender(sender, payload, environ=environ, callback_base_url=callback_base_url)
    packet["sendResult"] = send_result
    packet["recommendationSent"] = bool((send_result or {}).get("ok"))
    packet["recommendationId"] = (send_result or {}).get("recommendationId") or (send_result or {}).get("id") or ""
    packet["sent"] = bool(packet["recommendationSent"] and ((not archive_required) or packet["archiveSent"]))
    if packet["sent"]:
        packet["status"] = "sent"
    elif (packet["archiveSent"] or not archive_required) and not packet["recommendationSent"]:
        packet["status"] = "partial_success" if packet["archiveSent"] else "recommendation_send_failed"
        packet["partialSuccess"] = bool(packet["archiveSent"])
        packet["partialSuccessReason"] = "compact_recommendation_rejected"
        logging.warning("source_enrichment_compact_recommendation_failed archive_ok=%s compact_recommendation_ok=false reason=compact_recommendation_rejected subject_key=%s", str(bool(packet["archiveSent"])).lower(), _subject_key(subject))
    elif packet["recommendationSent"] and not packet["archiveSent"]:
        packet["status"] = "archive_send_failed"
        logging.warning("source_enrichment_archive_failed recommendation_ok=true subject_key=%s", _subject_key(subject))
    else:
        packet["status"] = "send_failed"
    return packet




def _format_source_coverage(result: dict[str, Any]) -> str:
    checked = result.get("sourceTypesChecked") or (result.get("diagnostics") or {}).get("sourceTypesChecked") or {}
    if not checked:
        used = set(result.get("sourceTypes") or [])
        checked = {name: ("yes" if name in used else "no_match") for name in ("source_file_lookup", "user_profiles", "conversations_by_author", "community_presence", "public_dossier_match", "broadcast_memory")}
    preferred = ["source_file_lookup", "user_profiles", "conversations_by_author", "community_presence", "public_dossier_match", "broadcast_memory", "user_habits", "relationship_state", "relationship_journal", "user_memory_facts", "memory_tiers", "rd_context"]
    parts = []
    for key in preferred:
        if key in checked:
            parts.append(f"{key} {checked[key]}")
    for key in sorted(k for k in checked if k not in preferred):
        parts.append(f"{key} {checked[key]}")
    return ", ".join(parts) or "none checked"


def format_source_enrichment_response(result: dict[str, Any]) -> str:
    subject = _safe_text(result.get("subject") or "subject", 90)
    match_kind = _safe_text(result.get("matchKind") or "none", 80)
    if result.get("status") == "possible_match_review":
        matches = result.get("possibleMatches") or []
        lines = [
            f"Source enrichment: No confirmed target found for “{subject}.”",
            "Possible match found, but enrichment needs an exact active Source File target.",
        ]
        if len(matches) == 1:
            item = matches[0]
            candidate = _safe_text(item.get("candidateId"), 90)
            lines.append(f"Possible match found: {_safe_text(item.get('name'), 90)}")
            lines.append(f"Match kind: {_safe_text(item.get('matchKind') or 'possible_match', 80)}")
            target_hint = "Use " + (f"candidateId={candidate} or " if candidate else "") + "exact name to target it."
            lines.append(target_hint)
        elif matches:
            lines.append("Possible matches (narrow with exact name or candidateId):")
            for item in matches[:5]:
                candidate = _safe_text(item.get("candidateId"), 90)
                suffix = f" — candidateId={candidate}" if candidate else ""
                lines.append(f"* {_safe_text(item.get('name'), 90)} — {_safe_text(item.get('matchKind') or 'possible_match', 80)}{suffix}")
        note = _safe_text(result.get("matchNote"), 220)
        if note and note not in lines:
            lines.append(note)
        lines.append("No notes were sent.")
        if len(matches) != 1:
            lines.append("Use exact name or candidateId=<id> to target one active Source File.")
        return "\n".join(lines)[:1900]
    if result.get("status") == "no_target":
        return (f"Source enrichment: No confirmed target found for “{subject}.”\n"
                "Next: create/promote a Candidate Intake item first or run candidate bridge/discovery. No notes were sent.")
    if result.get("status") == "lookup_failed":
        return f"Source enrichment lookup failed for “{subject}”: {_safe_text(result.get('matchNote'), 140)}"
    sections = result.get("sections") or {}
    source_types = ", ".join(result.get("sourceTypes") or []) or "source_file_lookup"
    warnings = result.get("warnings") or []
    warning_text = str(len(warnings))
    attach_label = {
        "active_source_file": "active Source File",
        "candidate_intake": "Candidate Intake (intake-only)",
        "existing_dossier_update": "Existing Dossier Update",
    }.get(match_kind, match_kind)
    if result.get("dryRun"):
        preview_lines = [
            f"Dry run Source File enrichment for “{subject}.”",
            f"Target lane: {attach_label}.",
            f"Would send as BNL Source File Enrichment: {'yes' if result.get('qualityStatus') != 'too_thin' else 'no — too thin'}.",
        ]
        preview_lines.extend(_safe_text(line, 180) for line in (result.get("refreshDiagnostics") or [])[:5])
        preview_lines.extend([
            f"Source types used: {source_types}.",
            f"Source coverage: {_safe_text(_format_source_coverage(result), 120)}.",
            f"Matched local profiles/users: {int((result.get('diagnostics') or {}).get('matchedUserProfileCount') or 0)}/{int((result.get('diagnostics') or {}).get('matchedUserIdCount') or 0)}.",
            f"Matched identity labels: {_safe_text(', '.join((result.get('diagnostics') or {}).get('matchedIdentityLabels') or []) or 'none', 160)}.",
            f"Channel evidence found: {'yes' if (result.get('diagnostics') or {}).get('channelEvidenceFound') else 'no'}; public dossier match found: {'yes' if (result.get('diagnostics') or {}).get('publicDossierMatchFound') else 'no'}; existing dossier update lane: {'yes' if (result.get('diagnostics') or {}).get('existingDossierUpdateLane') else 'no'}.",
            f"public_dossier_match {'yes' if (result.get('diagnostics') or {}).get('publicDossierMatchFound') else 'no'}.",
            f"Warning count: {warning_text}.",
        ])
        early_entity_diag = ((result.get("diagnostics") or {}).get("entityIntelligence") or {})
        if early_entity_diag:
            scope_counts = early_entity_diag.get("rowScopeCounts") or {}
            preview_lines.extend([
                f"Entity intelligence ledger/profile: {_safe_text(early_entity_diag.get('ledgerStatus') or 'unknown', 40)} / {_safe_text(early_entity_diag.get('profileStatus') or 'unknown', 40)}; Resolver surface: {_safe_text(early_entity_diag.get('resolverSurface') or 'source_file', 40)}.",
                f"Entity ledger roles: {_safe_text(', '.join(early_entity_diag.get('detectedRoleFacets') or []) or 'none', 140)}.",
                f"Entity ledger relationships: {_safe_text(', '.join(early_entity_diag.get('detectedRelationshipFacets') or []) or 'none', 140)}.",
                f"Entity ledger creative/music activity: {_safe_text(', '.join(early_entity_diag.get('creativeMusicFacets') or []) or 'none', 120)}.",
                f"Entity ledger BNL interaction/themes: {_safe_text(', '.join((early_entity_diag.get('bnlInteractionFacets') or []) + (early_entity_diag.get('conversationThemes') or [])) or 'none', 170)}.",
                f"Entity ledger action items/missing info: {_safe_text('; '.join((early_entity_diag.get('modActionItems') or []) + (early_entity_diag.get('missingInfo') or [])) or 'none', 100)}.",
                f"Entity ledger scope/rejection counts: owned={int(scope_counts.get('subject_owned') or 0)} keyed={int(scope_counts.get('subject_keyed') or 0)} authored={int(scope_counts.get('subject_authored') or 0)} rejected={int(scope_counts.get('rejected') or 0)}.",
            ])
        subject_intel = (result.get("diagnostics") or {}).get("subjectIntelligence") or result.get("subjectIntelligenceDiagnostics") or {}
        if subject_intel:
            rows_by_source = subject_intel.get("rowsBySource") or subject_intel.get("sourceCounts") or {}
            accepted_subjects = subject_intel.get("acceptedRecurringSubjects") or []
            rejected_candidates = [_safe_text(item, 80) for item in (subject_intel.get("rejectedGarbageCandidates") or [])]
            rejected_count = int(subject_intel.get("rejectedRecurringSubjectCount") or len(rejected_candidates) or 0)
            advisory_count = int(subject_intel.get("acceptedRecurringSubjectCount") or len(accepted_subjects) or len(subject_intel.get("topRecurringSubjects") or []) or 0)
            preview_lines.extend([
                f"Subject intelligence rows scanned: {int(subject_intel.get('rowsScanned') or 0)}.",
                f"Subject intelligence rows by source: {_safe_text(', '.join(f'{k} {v}' for k, v in rows_by_source.items()) or 'none', 220)}.",
                f"Legacy recurring-subject diagnostics: {rejected_count} rejected / {advisory_count} advisory only; hidden in normal dry-run. Use diagnostics=true for candidate details.",
                f"Public-safe vs review-only intelligence rows: {int(subject_intel.get('publicSafeRows') or subject_intel.get('publicRows') or 0)} public-safe / {int(subject_intel.get('reviewOnlyRows') or 0)} review-only.",
            ])
            if result.get("diagnosticsRequested"):
                preview_lines.append("Legacy recurring-subject diagnostics only. These are not Source File claims.")
                accepted_subject_parts = []
                for item in accepted_subjects:
                    if not isinstance(item, dict):
                        continue
                    label = _safe_text(item.get("label") or "", 80)
                    reason = _safe_text(item.get("reason") or "", 80)
                    if label:
                        accepted_subject_parts.append(f"{label}: {reason}" if reason else label)
                accepted_subject_text = ", ".join(accepted_subject_parts) or "none"
                rejected_candidate_text = ", ".join(item for item in rejected_candidates if item) or "none"
                conversation_clusters = [_safe_text(item, 100) for item in (subject_intel.get("topConversationClusters") or [])]
                activity_patterns = [_safe_text(item, 120) for item in (subject_intel.get("topActivityPatterns") or [])]
                preview_lines.extend([
                    f"Legacy top recurring subjects (diagnostic only): {_safe_text(', '.join(subject_intel.get('topRecurringSubjects') or []) or 'none', 180)}.",
                    f"Accepted recurring subjects with reason (diagnostic only): {_safe_text(accepted_subject_text, 220)}.",
                    f"Rejected legacy candidates: {_safe_text(rejected_candidate_text, 120)}.",
                    f"Top recurring themes (diagnostic only): {_safe_text(', '.join(subject_intel.get('topRecurringThemes') or []) or 'none', 140)}.",
                    f"Top conversation clusters (diagnostic only): {_safe_text(', '.join(item for item in conversation_clusters if item) or 'none', 150)}.",
                    f"Top activity patterns (diagnostic only): {_safe_text(' | '.join(item for item in activity_patterns if item) or 'none', 150)}.",
                    f"Top domains/tools (diagnostic only): {_safe_text(', '.join(subject_intel.get('topDomains') or []) or 'none', 180)}.",
                ])

        bullets = result.get("previewBullets") or build_preview_bullets(result)
        if bullets:
            preview_lines.append("Useful preview bullets: available after entity-ledger and classification readout.")
        classification = result.get("classification") or {}
        if classification:
            missing = classification.get("missingInfo") or []
            preview_lines.extend([
                f"Role read: {_safe_text(classification.get('roleRead') or ROLE_LABELS.get(classification.get('primaryRole'), 'Unknown / needs review'), 180)}",
                f"Activity read: {_safe_text(classification.get('activityRead') or classification.get('activityLevel') or 'unknown', 160)}",
                f"Topic read: {_safe_text(classification.get('topicRead') or _classification_plain_list(classification.get('topicThemes') or [], TOPIC_LABELS), 170)}",
                f"Engagement use: {_safe_text(classification.get('engagementRead') or _classification_plain_list(classification.get('engagementUse') or [], ENGAGEMENT_LABELS), 170)}",
                f"Dossier use: {_safe_text(classification.get('dossierRead') or DOSSIER_LABELS.get(classification.get('dossierUse'), 'Unknown'), 170)}",
                f"Relationship use: {_safe_text(classification.get('relationshipRead') or RELATIONSHIP_LABELS.get(classification.get('relationshipUse'), 'No relationship read yet'), 140)}",
                f"Missing info: {_safe_text('; '.join(missing) if missing else 'No additional classification gaps recorded.', 180)}",
            ])
        bullets = result.get("previewBullets") or build_preview_bullets(result)
        if bullets:
            preview_lines.append("Useful preview bullets:")
            preview_lines.extend(f"* {_safe_text(bullet, 100)}" for bullet in bullets[:1])
        if result.get("qualityStatus") == "too_thin":
            preview_lines.append(result.get("suppressedReason") or "Enrichment is too thin to send. Add more source notes or confirm the subject’s role first.")
        entity_diag = ((result.get("diagnostics") or {}).get("entityIntelligence") or {})
        if entity_diag:
            rows_by_source = entity_diag.get("sourceRowsBySource") or {}
            scope_counts = entity_diag.get("rowScopeCounts") or {}
            preview_lines.extend([
                f"Entity ledger subject-owned facets: roles={_safe_text(', '.join(entity_diag.get('detectedRoleFacets') or []) or 'none', 105)}; relationships={_safe_text(', '.join(entity_diag.get('detectedRelationshipFacets') or []) or 'none', 105)}; music={_safe_text(', '.join(entity_diag.get('creativeMusicFacets') or []) or 'none', 95)}.",
                f"Entity themes/activity: themes={_safe_text(', '.join(entity_diag.get('conversationThemes') or []) or 'none', 120)}; actions={_safe_text('; '.join(entity_diag.get('modActionItems') or []) or 'none', 110)}.",
                f"Scoped row split: owned={int(scope_counts.get('subject_owned') or 0)} keyed={int(scope_counts.get('subject_keyed') or 0)} authored={int(scope_counts.get('subject_authored') or 0)} profile={int(scope_counts.get('subject_profile_matched') or 0)} direct={int(scope_counts.get('subject_direct_evidence') or 0)} co-mention={int(scope_counts.get('subject_co_mention') or 0)}.",
                f"Excluded review-only rows: global_mixed={int(entity_diag.get('globalMixedRows') or 0)} source_blind={int(entity_diag.get('sourceBlindRows') or 0)} rejected={int(scope_counts.get('rejected') or 0)}; extractable={int(entity_diag.get('extractableRows') or 0)}.",
                f"Source split: {_safe_text(', '.join(f'{k} {v}' for k, v in rows_by_source.items()) or 'none', 105)}; official public dossier source connected: {'yes' if entity_diag.get('officialPublicDossierConnected') else 'no'}; queue submission connected: {'yes' if entity_diag.get('queueSubmissionStatus') != 'not_connected' else 'no'}.",
                "Legacy recurring-subject diagnostics are subordinate; scoped entity ledger facets are shown first.",
            ])
        preview_lines.append("No raw private transcripts were included; nothing was sent to the website.")
        return "\n".join(preview_lines)[:1900]
    if result.get("sent"):
        forced = " Forced low-confidence send." if result.get("qualityStatus") == "forced_low_confidence" else ""
        diag = " ".join(_safe_text(line, 140) for line in (result.get("refreshDiagnostics") or [])[:2])
        suffix = f" {diag}" if diag else ""
        return f"Source enrichment sent for admin review: {subject}. Match: {match_kind}.{forced} Ingest key: {_safe_text(result.get('ingestKey'), 120)}.{suffix}"[:1900]
    err = (result.get("sendResult") or {}).get("error") or "site ingest did not accept the review-only payload"
    return f"Source enrichment was built but not sent for “{subject}”: {_safe_text(err, 150)}"
