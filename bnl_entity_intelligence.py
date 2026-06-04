"""Generic source-aware entity intelligence facets for BNL.

This module converts subject-linked evidence rows into bounded, review-safe
facets that can be reused by Source File enrichment without hardcoding any
specific person, crew, dispute, or scenario.
"""

from __future__ import annotations

import re
from collections import Counter, defaultdict
from typing import Any

from bnl_dossier_source_packets import normalize_subject_name, subject_key

MAX_FACET_ITEMS = 8
MAX_EVIDENCE_ITEMS = 4
MAX_TEXT = 220

_DISCORD_MENTION_RE = re.compile(r"<[@#!&]?[0-9]{5,}>")
_LONG_ID_RE = re.compile(r"\b\d{15,22}\b")
_URL_RE = re.compile(r"https?://\S+", re.I)
_WORD_RE = re.compile(r"[a-z0-9]+", re.I)
_CHANNEL_RE = re.compile(r"(?:^|\s)#([a-z0-9][a-z0-9_-]{1,40})", re.I)

_ROLE_PATTERNS: tuple[tuple[str, str, re.Pattern[str]], ...] = (
    ("moderator_or_staff", "Possible moderator/staff role", re.compile(r"\b(mod(?:erator)?|admin|staff|owner|manage[rs]?|moderates?|moderating|server team)\b", re.I)),
    ("artist_or_music_maker", "Possible artist/music-maker role", re.compile(r"\b(artist|musician|producer|singer|rapper|dj|makes? music|made (?:(?:a|an|the) )?(?:new )?(?:song|track|beat)|my (?:song|track|album|ep)|wrote (?:a )?(?:song|track))\b", re.I)),
    ("community_member", "Community participant", re.compile(r"\b(member|community|server|chat|barcode|hangs?|participates?|joins?|regular)\b", re.I)),
    ("contest_runner_or_participant", "Contest/giveaway participant or organizer", re.compile(r"\b(contest|competition|challenge|giveaway|winner|entry|entries|vote|prize|submission round)\b", re.I)),
    ("collaborator", "Collaboration-oriented participant", re.compile(r"\b(collab(?:oration)?|collaborat(?:e|ion|or)|feature|feat\.?|team up|work with|project with|duet)\b", re.I)),
)

_BEHAVIOR_PATTERNS: tuple[tuple[str, str, re.Pattern[str]], ...] = (
    ("bnl_facing_questions", "Repeated BNL/source-file questions or requests", re.compile(r"\b(bnl|bot|source files?|dossiers?|review|explain|help|question|ask(?:ed|s)?)\b", re.I)),
    ("music_or_link_sharing", "Music/link sharing behavior", re.compile(r"\b(link|url|youtube|soundcloud|spotify|bandcamp|suno|udio|track|song|playlist|listen|submitted?|submission|queue|finished tracks?|wips?)\b", re.I)),
    ("community_presence", "Community conversation presence", re.compile(r"\b(community|server|chat|channel|barcode|radio|public|conversation|thread)\b", re.I)),
    ("lore_or_strange_conversation", "Lore-heavy or unusual conversation", re.compile(r"\b(lore|canon|myth|ritual|weird|strange|haunt(?:ed)?|void|entity|prophecy|cipher|simulation|dream)\b", re.I)),
    ("antagonistic_or_escalation", "Possible antagonistic/escalation pattern", re.compile(r"\b(argue|fight|beef|drama|attack|insult|troll|hostile|antagoniz(?:e|es|ed|ing)|threat|ban|call(?:ed)? out)\b", re.I)),
    ("moderation_activity", "Moderation/staff activity", re.compile(r"\b(mod(?:erator)?|admin|staff|ban(?:ned)?|mute(?:d)?|warn(?:ed)?|delete(?:d)?|rules?|enforce|moderates?)\b", re.I)),
)

_CONNECTION_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("works with", re.compile(r"\b(?:with|collab(?:orates?|oration)? with|featuring|feat\.?|project with)\s+([A-Z][A-Za-z0-9_-]{2,}(?:\s+[A-Z][A-Za-z0-9_-]{2,}){0,2})")),
    ("through", re.compile(r"\bthrough\s+([A-Z][A-Za-z0-9_-]{2,}(?:\s+[A-Z][A-Za-z0-9_-]{2,}){0,2})")),
    ("mentions", re.compile(r"\b(?:mentions?|talks? about|references?)\s+([A-Z][A-Za-z0-9_-]{2,}(?:\s+[A-Z][A-Za-z0-9_-]{2,}){0,2})")),
)

_CREATED_ENTITY_RE = re.compile(r"\b(?:created|built|runs?|hosts?|started|founded|released|dropped|posted|shared)\s+(?:a|an|the|my|their)?\s*([A-Z][A-Za-z0-9_-]+(?:\s+[A-Z][A-Za-z0-9_-]+){0,4})")
_NAMED_CREATED_RE = re.compile(r"\b(?:for|about)\s+(?:the\s+)?([A-Z][A-Za-z0-9_-]+(?:\s+[A-Z][A-Za-z0-9_-]+){0,3})\s+(?:collab|project|track|song|contest|show|radio)\b")
_STOP_CONNECTIONS = {"BNL", "Bot", "Source", "File", "Dossier", "Discord", "Community", "Public", "Review", "Owner", "Admin"}


def _clean_text(value: Any, limit: int = MAX_TEXT) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    text = _DISCORD_MENTION_RE.sub("[discord-mention]", text)
    text = _LONG_ID_RE.sub("[id]", text)
    text = _URL_RE.sub("[link]", text)
    return text[:limit].rstrip()


def _row_text(row: dict[str, Any]) -> str:
    return _clean_text(row.get("text") or row.get("safe_summary") or row.get("summary") or "", 2000)


def _safe_label(label: str, subject: str) -> str:
    clean = re.sub(r"\s+", " ", str(label or "")).strip(" -:.,;!?()[]{}\"'“”‘’")
    if not clean or len(clean) < 3 or len(clean) > 80:
        return ""
    if clean.lower() == normalize_subject_name(subject).lower():
        return ""
    if clean in _STOP_CONNECTIONS:
        return ""
    if "_" in clean or re.fullmatch(r"[A-Z0-9_-]{6,}", clean):
        return ""
    words = [w.lower() for w in _WORD_RE.findall(clean)]
    if not words or all(w in {"the", "and", "for", "with", "from", "this", "that", "source", "file", "public", "review"} for w in words):
        return ""
    return clean


def _visibility(row: dict[str, Any]) -> str:
    return "public_safe_candidate" if row.get("publicSafe") else "review_only"


def _source(row: dict[str, Any]) -> str:
    return re.sub(r"_", " ", str(row.get("source") or "unknown")).strip()[:80]


def _evidence(row: dict[str, Any], text: str) -> dict[str, str]:
    return {"source": _source(row), "visibility": _visibility(row), "relation": str(row.get("relation") or "mentioned")[:40], "summary": "Source-linked signal observed; review the cited source lane before reuse."}


def _append_facet(facets: dict[str, list[dict[str, Any]]], key: str, label: str, row: dict[str, Any], text: str) -> None:
    items = facets[key]
    for item in items:
        if item["label"] == label:
            item["count"] += 1
            item["visibilityCounts"][_visibility(row)] += 1
            if len(item["evidence"]) < MAX_EVIDENCE_ITEMS:
                item["evidence"].append(_evidence(row, text))
            return
    items.append({"label": label, "count": 1, "visibilityCounts": Counter({_visibility(row): 1}), "evidence": [_evidence(row, text)]})


def _finalize_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    ranked = sorted(items, key=lambda item: (item.get("count", 0), item.get("visibilityCounts", {}).get("public_safe_candidate", 0)), reverse=True)
    out: list[dict[str, Any]] = []
    for item in ranked[:MAX_FACET_ITEMS]:
        out.append({
            "label": item["label"],
            "count": int(item.get("count") or 0),
            "visibilityCounts": dict(item.get("visibilityCounts") or {}),
            "evidence": list(item.get("evidence") or [])[:MAX_EVIDENCE_ITEMS],
        })
    return out


def _confirmation_items(profile: dict[str, Any]) -> list[str]:
    items = [
        "Confirm the subject's public role/title before using role language.",
        "Confirm queue/submission identity separately; this intelligence does not prove queue entries, play status, or billing.",
    ]
    if profile.get("musicLinkActivity"):
        items.append("Review public music/link evidence and confirm which links are owned by the subject before reuse.")
    if profile.get("connections"):
        items.append("Confirm collaborations/connections with the named people or entities before stating them publicly.")
    if profile.get("moderationSignals"):
        items.append("Confirm any mod/staff status with admins before using it as a public fact.")
    if profile.get("antagonisticPatterns"):
        items.append("Keep antagonistic/escalation reads review-only unless an admin approves a neutral public framing.")
    if profile.get("contestActivity"):
        items.append("Confirm contest role, dates, and outcome before claiming organizer/entrant/winner status.")
    return items[:8]


def _readouts(profile: dict[str, Any], public: bool) -> list[str]:
    key = "public_safe_candidate" if public else "review_only"
    lines: list[str] = []
    labels = (
        ("roles", "Role signal"),
        ("behaviorPatterns", "Behavior pattern"),
        ("musicLinkActivity", "Music/link signal"),
        ("connections", "Connection signal"),
        ("moderationSignals", "Moderation signal"),
        ("contestActivity", "Contest signal"),
        ("antagonisticPatterns", "Escalation signal"),
        ("loreSignals", "Lore/strange-conversation signal"),
    )
    for facet_key, prefix in labels:
        for item in profile.get(facet_key) or []:
            if (item.get("visibilityCounts") or {}).get(key):
                suffix = "owner review required" if public else "review-only; do not publish without admin approval"
                lines.append(f"{prefix}: {item.get('label')} ({int(item.get('count') or 0)} source-linked signal(s); {suffix}).")
                break
    return lines[:8]


def build_entity_intelligence_profile(rows: list[dict[str, Any]], subject: str, *, max_items: int = MAX_FACET_ITEMS) -> dict[str, Any]:
    """Build generic dossier-useful facets from subject-linked evidence rows.

    Rows are expected to contain ``source``, ``text``, ``publicSafe``, and
    ``relation`` keys, matching ``collect_subject_intelligence_rows`` output.
    The result is source-aware, bounded, and separates public-safe candidates
    from review-only signals. It does not confirm public facts.
    """

    subject = normalize_subject_name(subject)
    facets: dict[str, list[dict[str, Any]]] = defaultdict(list)
    source_counts: Counter = Counter()
    visibility_counts: Counter = Counter()
    relation_counts: Counter = Counter()
    channel_counts: Counter = Counter()

    for row in rows or []:
        text = _row_text(row)
        if not text:
            continue
        source_counts[_source(row)] += 1
        visibility_counts[_visibility(row)] += 1
        relation_counts[str(row.get("relation") or "mentioned")[:40]] += 1
        for channel in _CHANNEL_RE.findall(text):
            channel_counts[f"#{channel.lower()}"] += 1

        for _code, label, pattern in _ROLE_PATTERNS:
            if pattern.search(text):
                _append_facet(facets, "roles", label, row, text)
        for code, label, pattern in _BEHAVIOR_PATTERNS:
            if pattern.search(text):
                target = {
                    "music_or_link_sharing": "musicLinkActivity",
                    "antagonistic_or_escalation": "antagonisticPatterns",
                    "moderation_activity": "moderationSignals",
                    "lore_or_strange_conversation": "loreSignals",
                }.get(code, "behaviorPatterns")
                _append_facet(facets, target, label, row, text)
        if re.search(r"\b(contest|competition|challenge|giveaway|winner|entry|entries|prize)\b", text, re.I):
            _append_facet(facets, "contestActivity", "Contest/giveaway activity", row, text)
        if re.search(r"\b(collab|collaboration|feature|feat\.?|work with|project with|duet)\b", text, re.I):
            _append_facet(facets, "collaborations", "Collaboration offer or relationship", row, text)

        for relation, pattern in _CONNECTION_PATTERNS:
            for match in pattern.finditer(text):
                label = _safe_label(match.group(1), subject)
                if label:
                    _append_facet(facets, "connections", f"{relation}: {label}", row, text)
        for created_pattern in (_CREATED_ENTITY_RE, _NAMED_CREATED_RE):
            for match in created_pattern.finditer(text):
                label = _safe_label(match.group(1), subject)
                if label and not re.search(r"\b(help|question|review|context|conversation|link)\b", label, re.I):
                    _append_facet(facets, "createdEntities", label, row, text)

    profile: dict[str, Any] = {
        "subjectName": subject,
        "subjectKey": subject_key(subject),
        "roles": _finalize_items(facets.get("roles") or [])[:max_items],
        "behaviorPatterns": _finalize_items(facets.get("behaviorPatterns") or [])[:max_items],
        "channelParticipation": [{"channel": channel, "count": count} for channel, count in channel_counts.most_common(max_items)],
        "connections": _finalize_items(facets.get("connections") or [])[:max_items],
        "collaborations": _finalize_items(facets.get("collaborations") or [])[:max_items],
        "createdEntities": _finalize_items(facets.get("createdEntities") or [])[:max_items],
        "antagonisticPatterns": _finalize_items(facets.get("antagonisticPatterns") or [])[:max_items],
        "contestActivity": _finalize_items(facets.get("contestActivity") or [])[:max_items],
        "moderationSignals": _finalize_items(facets.get("moderationSignals") or [])[:max_items],
        "musicLinkActivity": _finalize_items(facets.get("musicLinkActivity") or [])[:max_items],
        "loreSignals": _finalize_items(facets.get("loreSignals") or [])[:max_items],
        "diagnostics": {
            "rowsScanned": sum(source_counts.values()),
            "sourceCounts": dict(source_counts),
            "visibilityCounts": dict(visibility_counts),
            "relationCounts": dict(relation_counts),
        },
    }
    profile["publicSafeReadouts"] = _readouts(profile, public=True)
    profile["reviewOnlyReadouts"] = _readouts(profile, public=False)
    profile["recommendedConfirmations"] = _confirmation_items(profile)
    return profile
