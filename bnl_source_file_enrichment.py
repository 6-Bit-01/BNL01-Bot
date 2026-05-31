"""Review-only Source File enrichment helpers for BNL.

This module builds structured case-file notes from approved local BNL stores and
optionally posts them through the existing review-only dossier recommendation
endpoint. It never publishes, promotes, merges, or overwrites Source Files.
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Any, Callable

from bnl_dossier_recommendations import build_dossier_recommendation_payload, send_dossier_recommendation
from bnl_source_file_lookup import lookup_source_file

ENRICHMENT_INGEST_SOURCE = "bnl_source_file_enrichment"
FALLBACK_INGEST_SOURCE = "bnl_source_knowledge_bridge"
SOURCE_BLIND_WARNING = "source-blind memory is review-only; do not treat as confirmed fact"
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
    }
    for extra in parts[1:]:
        if not extra:
            continue
        opt = re.match(r"^([A-Za-z_]+)\s*=\s*(.+)$", extra, flags=re.S)
        if not opt:
            return True, None, "Supported options: dry_run=true|false, force=true|false."
        key = opt.group(1).strip().lower()
        value = opt.group(2).strip()
        if key in {"dry_run", "dryrun"}:
            options["dry_run"] = value.lower() in {"true", "1", "yes", "on"}
        elif key == "force":
            options["force"] = value.lower() in {"true", "1", "yes", "on"}
        else:
            return True, None, "Supported options: dry_run=true|false, force=true|false."
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
    }

def _section_text(sections: dict[str, list[str]], name: str, *, limit: int = 4) -> str:
    items = sections.get(name) or []
    return "\n".join(f"- {_safe_text(item, 240)}" for item in items[:limit]) or "- currently unknown."


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


def build_enrichment_recommendation_payload(packet: dict[str, Any], *, environ: dict[str, str] | None = None) -> dict[str, Any]:
    sections = packet.get("sections") or {}
    subject = str(packet.get("subject") or "").strip()
    match_kind = str(packet.get("matchKind") or "none")
    source_file = packet.get("sourceFile") if isinstance(packet.get("sourceFile"), dict) else {}
    target_candidate_id = _first(source_file, ("candidateId", "targetCandidateId", "id")) if match_kind in {"candidate_intake", "active_source_file"} else None
    target_dossier_id = _first(source_file, ("targetDossierId", "dossierId", "publicDossierId")) if match_kind == "existing_dossier_update" else None
    evidence = build_enrichment_markdown(packet)
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
        "sourceCoverage": dict(packet.get("sourceTypesChecked") or {}),
        "identityDiagnostics": dict(packet.get("diagnostics") or {}),
        "internalClassification": dict(packet.get("classification") or {}),
        "visibilityLabels": ["internal_review_only", "admin_review_required"],
        "confidence": "low" if packet.get("qualityStatus") in {"too_thin", "forced_low_confidence"} else ("medium" if match_kind == "active_source_file" else "low"),
        "createdBy": "bnl",
        "ingestSource": source_enrichment_ingest_source(environ),
        "ingestKey": deterministic_enrichment_ingest_key(subject, match_kind, sections),
    }
    return build_dossier_recommendation_payload(payload)


def run_source_file_enrichment(
    db_path: str,
    guild_id: int | None,
    subject: str,
    *,
    dry_run: bool = False,
    rd_context: list[dict[str, Any]] | None = None,
    lookup_func: Callable[[dict[str, str]], dict[str, Any]] = lookup_source_file,
    sender: Callable[[dict[str, Any]], dict[str, Any]] = send_dossier_recommendation,
    environ: dict[str, str] | None = None,
    lookup_key: str = "subject",
    lookup_value: str | None = None,
    force: bool = False,
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
    evidence = collect_source_enrichment_evidence(db_path, guild_id, str(effective_subject), rd_context=rd_context, lookup_result=lookup_result)
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
    }
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
    send_result = sender(payload)
    packet["sendResult"] = send_result
    packet["sent"] = bool((send_result or {}).get("ok"))
    packet["status"] = "sent" if packet["sent"] else "send_failed"
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
            f"Source types used: {source_types}.",
            f"Source coverage: {_format_source_coverage(result)}.",
            f"Matched local profiles/users: {int((result.get('diagnostics') or {}).get('matchedUserProfileCount') or 0)}/{int((result.get('diagnostics') or {}).get('matchedUserIdCount') or 0)}.",
            f"Matched identity labels: {_safe_text(', '.join((result.get('diagnostics') or {}).get('matchedIdentityLabels') or []) or 'none', 160)}.",
            f"Channel evidence found: {'yes' if (result.get('diagnostics') or {}).get('channelEvidenceFound') else 'no'}; public dossier match found: {'yes' if (result.get('diagnostics') or {}).get('publicDossierMatchFound') else 'no'}; existing dossier update lane: {'yes' if (result.get('diagnostics') or {}).get('existingDossierUpdateLane') else 'no'}.",
            f"Warning count: {warning_text}.",
        ]
        classification = result.get("classification") or {}
        if classification:
            missing = classification.get("missingInfo") or []
            preview_lines.extend([
                f"Role read: {_safe_text(classification.get('roleRead') or ROLE_LABELS.get(classification.get('primaryRole'), 'Unknown / needs review'), 220)}",
                f"Activity read: {_safe_text(classification.get('activityRead') or classification.get('activityLevel') or 'unknown', 220)}",
                f"Topic read: {_safe_text(classification.get('topicRead') or _classification_plain_list(classification.get('topicThemes') or [], TOPIC_LABELS), 220)}",
                f"Engagement use: {_safe_text(classification.get('engagementRead') or _classification_plain_list(classification.get('engagementUse') or [], ENGAGEMENT_LABELS), 220)}",
                f"Dossier use: {_safe_text(classification.get('dossierRead') or DOSSIER_LABELS.get(classification.get('dossierUse'), 'Unknown'), 220)}",
                f"Relationship use: {_safe_text(classification.get('relationshipRead') or RELATIONSHIP_LABELS.get(classification.get('relationshipUse'), 'No relationship read yet'), 220)}",
                f"Missing info: {_safe_text('; '.join(missing) if missing else 'No additional classification gaps recorded.', 260)}",
            ])
        bullets = result.get("previewBullets") or build_preview_bullets(result)
        if bullets:
            preview_lines.append("Useful preview bullets:")
            preview_lines.extend(f"* {_safe_text(bullet, 180)}" for bullet in bullets[:3])
        if result.get("qualityStatus") == "too_thin":
            preview_lines.append(result.get("suppressedReason") or "Enrichment is too thin to send. Add more source notes or confirm the subject’s role first.")
        preview_lines.append("No raw private transcripts were included; nothing was sent to the website.")
        return "\n".join(preview_lines)[:1900]
    if result.get("sent"):
        forced = " Forced low-confidence send." if result.get("qualityStatus") == "forced_low_confidence" else ""
        return f"Source enrichment sent for admin review: {subject}. Match: {match_kind}.{forced} Ingest key: {_safe_text(result.get('ingestKey'), 120)}."
    err = (result.get("sendResult") or {}).get("error") or "site ingest did not accept the review-only payload"
    return f"Source enrichment was built but not sent for “{subject}”: {_safe_text(err, 150)}"
