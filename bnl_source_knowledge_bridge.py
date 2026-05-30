"""Operator-triggered bridge from BNL local knowledge stores to Source File recommendations.

The bridge is intentionally review-only. It reads approved local SQLite stores,
labels source quality/visibility, builds normal dossier recommendation payloads,
and never publishes, drafts, merges identities, fetches Discord history, or uses
payment/account/customer/queue identity data.
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable

from bnl_dossier_recommendations import build_dossier_recommendation_payload, send_dossier_recommendation
from bnl_dossier_source_packets import DEFAULT_MISSING_INFO, DEFAULT_SUGGESTED_ACTION, subject_key

KNOWLEDGE_BRIDGE_INGEST_SOURCE = "bnl_source_knowledge_bridge"
DEFAULT_KNOWLEDGE_BRIDGE_LIMIT = 20
MAX_KNOWLEDGE_BRIDGE_LIMIT = 50
SOURCE_BLIND_WARNING = "Source-blind BNL memory trace. Review source/visibility before use."
_PRIVATE_WARNING = "Internal/private or unknown visibility context. Owner review required before public use."
_PUBLIC_REVIEW_WARNING = "Public-safe candidate only after owner review; do not overclaim identity, aliases, or facts."
MAX_EVIDENCE_ITEMS_PER_SUBJECT = 6
MAX_SNIPPET_LENGTH = 180
MAX_REASON_LENGTH = 420
MAX_EVIDENCE_SUMMARY_LENGTH = 900

_PUBLIC_POLICIES = {"public_home", "public_context", "public_selective", "broadcast_memory"}
_INTERNAL_POLICIES = {"internal_controlled", "sealed_test", "legacy_unknown", "unknown", ""}
_SKIP_POLICIES = {"dm", "direct_message", "private_dm"}
_NAME_STOPWORDS = {
    "BNL", "BNL 01", "BARCODE", "BARCODE Network", "Discord", "Source File", "Source Files",
    "Dossier", "Dossiers", "Owner Review", "Public Safe", "R&D", "Research Development",
    "Unknown", "None", "Admin", "Operator", "Moderator", "Mod", "User", "Customer", "Account",
}
_WEAK_TERMS = {
    "the", "and", "for", "with", "from", "this", "that", "source", "file", "dossier", "review",
    "candidate", "memory", "conversation", "summary", "profile", "relationship", "community", "presence",
    "internal", "public", "private", "unknown", "channel", "message", "discord", "operator", "admin",
    "owner", "mod", "moderator", "someone", "somebody", "everyone", "anyone", "today", "tonight",
}
_SUBJECT_WORD = r"[A-Z0-9][A-Za-z0-9'_-]*"
_SUBJECT_PHRASE = rf"{_SUBJECT_WORD}(?:\s+{_SUBJECT_WORD}){{0,4}}"
_DISCORD_MENTION_PATTERN = re.compile(r"<[@#!&]?[0-9]{5,}>")
_LONG_ID_PATTERN = re.compile(r"\b\d{15,22}\b")
_EMAIL_PATTERN = re.compile(r"\b[^\s@]+@[^\s@]+\.[^\s@]+\b")
_URL_PATTERN = re.compile(r"https?://\S+", re.I)


@dataclass
class KnowledgeEvidence:
    subject_name: str
    source_type: str
    source_quality: str
    visibility_labels: list[str]
    warnings: list[str]
    snippet: str
    confidence_points: int = 1
    candidate_type: str = "new_subject"
    connected_subject: str = ""


@dataclass
class KnowledgeAccumulator:
    subject_name: str
    evidence: list[KnowledgeEvidence] = field(default_factory=list)
    subject_names: Counter = field(default_factory=Counter)

    def add(self, item: KnowledgeEvidence) -> None:
        self.subject_names[item.subject_name] += 1
        if len(self.evidence) < MAX_EVIDENCE_ITEMS_PER_SUBJECT:
            self.evidence.append(item)


def parse_source_knowledge_bridge_command(content: str) -> tuple[bool, dict[str, Any] | None, str]:
    """Parse `!bnl source bridge knowledge [| limit=20] [| dry_run=true]`."""

    text = (content or "").strip()
    match = re.match(
        r"^!bnl\s+(?:(?:source\s+bridge\s+(?:knowledge|candidates))|(?:dossier\s+bridge\s+knowledge))(?:\s*(?:\|\s*)?(.*))?$",
        text,
        flags=re.I | re.S,
    )
    if not match:
        return False, None, "not_a_source_knowledge_bridge_command"
    options: dict[str, Any] = {"limit": DEFAULT_KNOWLEDGE_BRIDGE_LIMIT, "dry_run": True}
    options_text = (match.group(1) or "").strip()
    if options_text:
        for part in [piece.strip() for piece in options_text.split("|") if piece.strip()]:
            key_match = re.match(r"^([a-zA-Z_]+)\s*=\s*(.+)$", part)
            if not key_match:
                return True, None, "Use: `!bnl source bridge knowledge [| limit=20] [| dry_run=true]`"
            key = key_match.group(1).strip().lower()
            value = key_match.group(2).strip()
            if key == "limit":
                try:
                    options["limit"] = max(1, min(MAX_KNOWLEDGE_BRIDGE_LIMIT, int(value)))
                except ValueError:
                    return True, None, "limit must be a number."
            elif key in {"dry_run", "dryrun"}:
                options["dry_run"] = value.lower() not in {"false", "0", "no", "off"}
            else:
                return True, None, "Supported options: limit, dry_run."
    return True, options, ""


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    cur = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,))
    return cur.fetchone() is not None


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    if not _table_exists(conn, table):
        return set()
    return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def _safe_snippet(text: str, max_length: int = MAX_SNIPPET_LENGTH) -> str:
    cleaned = _DISCORD_MENTION_PATTERN.sub("[redacted-mention]", str(text or ""))
    cleaned = _LONG_ID_PATTERN.sub("[redacted-id]", cleaned)
    cleaned = _EMAIL_PATTERN.sub("[redacted-email]", cleaned)
    cleaned = _URL_PATTERN.sub("[redacted-url]", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    if len(cleaned) <= max_length:
        return cleaned
    return cleaned[: max(0, max_length - 1)].rstrip() + "…"


def _clean_subject_name(value: str) -> str:
    cleaned = re.sub(r"\s+", " ", str(value or "").strip(" .,:;!?()[]{}\"'"))
    cleaned = re.sub(r"^(?:the|a|an)\s+", "", cleaned, flags=re.I).strip()
    return cleaned[:80]


def _is_weak_subject(value: str) -> bool:
    name = _clean_subject_name(value)
    if not name or len(name) < 3 or name in _NAME_STOPWORDS:
        return True
    if re.fullmatch(r"\d+", name) or _LONG_ID_PATTERN.fullmatch(name):
        return True
    words = re.findall(r"[A-Za-z0-9]+", name.lower())
    if not words or all(word in _WEAK_TERMS for word in words):
        return True
    if len(words) > 5:
        return True
    return False


def _extract_subjects(text: str) -> list[str]:
    subjects: list[str] = []
    patterns = (
        rf"\b(?:source file|dossier|profile|memory|fact|habit|relationship|journal|context|subject|candidate|regular|artist|collaborator|mod|moderator|supporter|sponsor|entity|concept|lore)\s+(?:for|on|about|named)?\s*({_SUBJECT_PHRASE})\b",
        rf"\b({_SUBJECT_PHRASE})\s+(?:is|was|seems|appears|became|becomes|remains)\s+(?:a\s+)?(?:regular|known|recurring|artist|collaborator|mod|moderator|supporter|sponsor|entity|concept|candidate|subject)\b",
        rf"\b(?:known as|called|named|aka)\s+({_SUBJECT_PHRASE})\b",
    )
    for pattern in patterns:
        for match in re.finditer(pattern, text or "", flags=re.I):
            candidate = _clean_subject_name(match.group(1))
            if not _is_weak_subject(candidate):
                subjects.append(candidate)
    # Also capture explicitly capitalized short names in dense local summaries.
    for match in re.finditer(rf"\b({_SUBJECT_PHRASE})\b", text or ""):
        candidate = _clean_subject_name(match.group(1))
        if candidate and not _is_weak_subject(candidate) and any(ch.isupper() for ch in candidate):
            words = candidate.split()
            if len(words) <= 3 and candidate not in subjects:
                subjects.append(candidate)
    deduped: list[str] = []
    seen: set[str] = set()
    for subject in subjects:
        key = subject_key(subject)
        if key not in seen:
            seen.add(key)
            deduped.append(subject)
    return deduped[:8]


def _alias_or_connection(text: str, subject: str) -> tuple[str, str]:
    lowered = text or ""
    if re.search(r"\b(alias|aka|also known as|same as|same-as|identity link|confirmed alias)\b", lowered, re.I):
        return "identity_link", ""
    patterns = (
        rf"\b{re.escape(subject)}\s+(?:via|through|introduced\s+by|connected\s+to)\s+({_SUBJECT_PHRASE})\b",
        rf"\b({_SUBJECT_PHRASE})\s+(?:via|through|introduced\s+by|connected\s+to)\s+{re.escape(subject)}\b",
    )
    for pattern in patterns:
        match = re.search(pattern, lowered, flags=re.I)
        if match:
            connected = _clean_subject_name(match.group(1))
            if connected and not _is_weak_subject(connected) and subject_key(connected) != subject_key(subject):
                return "possible_connection_review", connected
    return "new_subject", ""


def _labels_for_policy(policy: str) -> tuple[str, list[str], list[str]]:
    policy_key = (policy or "unknown").strip().lower()
    if policy_key in _PUBLIC_POLICIES:
        return "public_discord_observed", ["public_safe_candidate", "owner_review_required", "public_use_not_allowed_until_review"], [_PUBLIC_REVIEW_WARNING]
    return "internal_context_review_only", ["internal_only", "private_review_required", "public_use_not_allowed_until_review", "owner_review_required"], [_PRIVATE_WARNING]


def _add(accs: dict[str, KnowledgeAccumulator], item: KnowledgeEvidence) -> None:
    key = subject_key(item.subject_name)
    if key not in accs:
        accs[key] = KnowledgeAccumulator(item.subject_name)
    accs[key].add(item)


def _read_user_profiles(conn: sqlite3.Connection, guild_id: int | None, accs: dict[str, KnowledgeAccumulator]) -> None:
    if not _table_exists(conn, "user_profiles"):
        return
    where = "WHERE guild_id=?" if guild_id else ""
    params: tuple[Any, ...] = (guild_id,) if guild_id else ()
    for row in conn.execute(f"SELECT display_name, preferred_name FROM user_profiles {where} LIMIT 500", params):
        for name in (row[1], row[0]):
            subject = _clean_subject_name(name)
            if _is_weak_subject(subject):
                continue
            _add(accs, KnowledgeEvidence(subject, "user_profiles", "local_profile_observed", ["internal_only", "private_review_required", "owner_review_required", "public_use_not_allowed_until_review"], [_PRIVATE_WARNING], f"Local profile observed for {subject}.", 2))
            break


def _read_user_memory_facts(conn: sqlite3.Connection, guild_id: int | None, accs: dict[str, KnowledgeAccumulator]) -> None:
    if not _table_exists(conn, "user_memory_facts"):
        return
    where = "WHERE guild_id=?" if guild_id else ""
    params: tuple[Any, ...] = (guild_id,) if guild_id else ()
    for row in conn.execute(f"SELECT fact_key, fact_value, confidence FROM user_memory_facts {where} ORDER BY updated_at DESC LIMIT 800", params):
        text = f"{row[0]}: {row[1]}"
        for subject in _extract_subjects(text):
            ctype, connected = _alias_or_connection(text, subject)
            _add(accs, KnowledgeEvidence(subject, "user_memory_facts", "local_memory_fact", ["internal_only", "private_review_required", "owner_review_required", "public_use_not_allowed_until_review"], [_PRIVATE_WARNING], _safe_snippet(text), max(1, int(float(row[2] or 0.5) * 3)), ctype, connected))


def _read_user_habits(conn: sqlite3.Connection, guild_id: int | None, accs: dict[str, KnowledgeAccumulator]) -> None:
    if not _table_exists(conn, "user_habits"):
        return
    where = "WHERE guild_id=?" if guild_id else ""
    params: tuple[Any, ...] = (guild_id,) if guild_id else ()
    for row in conn.execute(f"SELECT last_topic, total_messages FROM user_habits {where} ORDER BY updated_at DESC LIMIT 500", params):
        text = f"Habit trace topic: {row[0] or ''}; messages observed: {int(row[1] or 0)}"
        for subject in _extract_subjects(str(row[0] or "")):
            _add(accs, KnowledgeEvidence(subject, "user_habits", "local_behavior_trace", ["internal_only", "private_review_required", "owner_review_required", "public_use_not_allowed_until_review"], [_PRIVATE_WARNING], _safe_snippet(text), 1))


def _read_relationships(conn: sqlite3.Connection, guild_id: int | None, accs: dict[str, KnowledgeAccumulator]) -> None:
    if _table_exists(conn, "relationship_state"):
        where = "WHERE guild_id=?" if guild_id else ""
        params: tuple[Any, ...] = (guild_id,) if guild_id else ()
        for row in conn.execute(f"SELECT last_topic, trust_stage, social_stance, interaction_count FROM relationship_state {where} ORDER BY updated_at DESC LIMIT 500", params):
            text = f"Relationship trace topic: {row[0] or ''}; stage {row[1] or 'unknown'}; stance {row[2] or 'unknown'}; interactions {int(row[3] or 0)}"
            for subject in _extract_subjects(str(row[0] or "")):
                _add(accs, KnowledgeEvidence(subject, "relationship_state", "local_relationship_trace", ["internal_only", "private_review_required", "owner_review_required", "public_use_not_allowed_until_review"], [_PRIVATE_WARNING], _safe_snippet(text), 1))
    if _table_exists(conn, "relationship_journal"):
        where = "WHERE guild_id=?" if guild_id else ""
        params = (guild_id,) if guild_id else ()
        for row in conn.execute(f"SELECT entry_type, summary FROM relationship_journal {where} ORDER BY timestamp DESC LIMIT 800", params):
            text = f"{row[0]}: {row[1]}"
            for subject in _extract_subjects(text):
                ctype, connected = _alias_or_connection(text, subject)
                _add(accs, KnowledgeEvidence(subject, "relationship_journal", "local_relationship_trace", ["internal_only", "private_review_required", "owner_review_required", "public_use_not_allowed_until_review"], [_PRIVATE_WARNING], _safe_snippet(text), 1, ctype, connected))


def _read_conversations(conn: sqlite3.Connection, guild_id: int | None, accs: dict[str, KnowledgeAccumulator]) -> None:
    if not _table_exists(conn, "conversations"):
        return
    columns = _columns(conn, "conversations")
    policy_expr = "COALESCE(channel_policy, 'unknown')" if "channel_policy" in columns else "'unknown'"
    channel_expr = "COALESCE(channel_name, '')" if "channel_name" in columns else "''"
    where = "WHERE guild_id=?" if guild_id else ""
    params = (guild_id,) if guild_id else ()
    query = f"SELECT role, content, {policy_expr} AS policy, {channel_expr} AS channel_name FROM conversations {where} ORDER BY id DESC LIMIT 1000"
    for row in conn.execute(query, params):
        role, content, policy, channel_name = row
        if str(policy or "").lower() in _SKIP_POLICIES or str(channel_name or "").lower() in {"dm", "dms", "direct-message", "direct_messages"}:
            continue
        quality, labels, warnings = _labels_for_policy(str(policy or "unknown"))
        for subject in _extract_subjects(str(content or "")):
            ctype, connected = _alias_or_connection(str(content or ""), subject)
            _add(accs, KnowledgeEvidence(subject, "conversations", quality, labels, warnings, _safe_snippet(f"{policy or 'unknown'} conversation {role or 'row'} mentions {subject}."), 2 if quality == "public_discord_observed" else 1, ctype, connected))


def _read_memory_tiers(conn: sqlite3.Connection, guild_id: int | None, accs: dict[str, KnowledgeAccumulator]) -> None:
    if not _table_exists(conn, "memory_tiers"):
        return
    where = "WHERE guild_id=?" if guild_id else ""
    params = (guild_id,) if guild_id else ()
    for row in conn.execute(f"SELECT tier, summary, salience FROM memory_tiers {where} ORDER BY updated_at DESC LIMIT 800", params):
        text = f"{row[0]} memory: {row[1]}"
        for subject in _extract_subjects(str(row[1] or "")):
            ctype, connected = _alias_or_connection(str(row[1] or ""), subject)
            _add(accs, KnowledgeEvidence(subject, "memory_tiers", "source_blind_memory_trace", ["source_blind_review_required", "internal_only", "owner_review_required", "public_use_not_allowed_until_review"], [SOURCE_BLIND_WARNING], _safe_snippet(text), max(1, int(float(row[2] or 0.5) * 2)), ctype, connected))


def _read_broadcast_memory(conn: sqlite3.Connection, guild_id: int | None, accs: dict[str, KnowledgeAccumulator]) -> None:
    if not _table_exists(conn, "broadcast_memory"):
        return
    where = "WHERE status='active'" + (" AND guild_id=?" if guild_id else "")
    params = (guild_id,) if guild_id else ()
    for row in conn.execute(f"SELECT cleaned_summary, entry_type, public_safe, usage_scope FROM broadcast_memory {where} ORDER BY id DESC LIMIT 500", params):
        text = f"{row[1]}: {row[0]}"
        labels = ["owner_review_required", "public_use_not_allowed_until_review"]
        if int(row[2] or 0):
            labels.insert(0, "public_safe_candidate")
        else:
            labels.insert(0, "internal_only")
        for subject in _extract_subjects(text):
            ctype, connected = _alias_or_connection(text, subject)
            _add(accs, KnowledgeEvidence(subject, "broadcast_memory", "official_show_memory", labels, [_PUBLIC_REVIEW_WARNING], _safe_snippet(text), 3, ctype, connected))


def _read_community_presence(conn: sqlite3.Connection, guild_id: int | None, accs: dict[str, KnowledgeAccumulator]) -> None:
    if not _table_exists(conn, "community_presence"):
        return
    where = "WHERE guild_id=?" if guild_id else ""
    params = (guild_id,) if guild_id else ()
    for row in conn.execute(f"SELECT display_name, mention_count, direct_interaction_count, operator_mention_count, connection_notes, evidence_snippets FROM community_presence {where} ORDER BY (mention_count + direct_interaction_count + operator_mention_count) DESC LIMIT 500", params):
        subject = _clean_subject_name(row[0])
        if _is_weak_subject(subject):
            continue
        notes = " ".join(_json_list(row[4])[:2] + _json_list(row[5])[:2])
        ctype, connected = _alias_or_connection(notes, subject)
        signals = int(row[1] or 0) + int(row[2] or 0) + int(row[3] or 0)
        _add(accs, KnowledgeEvidence(subject, "community_presence", "public_or_community_observed", ["public_safe_candidate", "owner_review_required", "public_use_not_allowed_until_review"], [_PUBLIC_REVIEW_WARNING], _safe_snippet(f"Community presence observed with {signals} approved-channel signal(s). {notes}"), 2 + min(3, signals), ctype, connected))


def _json_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(v) for v in value]
    if not value:
        return []
    try:
        parsed = json.loads(str(value))
    except Exception:
        return []
    return [str(v) for v in parsed] if isinstance(parsed, list) else []


def collect_knowledge_bridge_candidates(db_path: str, guild_id: int | None = None, *, limit: int = DEFAULT_KNOWLEDGE_BRIDGE_LIMIT, rd_context: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    """Read approved local stores and return review-only recommendation payloads."""

    max_candidates = max(1, min(MAX_KNOWLEDGE_BRIDGE_LIMIT, int(limit or DEFAULT_KNOWLEDGE_BRIDGE_LIMIT)))
    accs: dict[str, KnowledgeAccumulator] = {}
    conn = sqlite3.connect(db_path)
    try:
        _read_user_profiles(conn, guild_id, accs)
        _read_user_memory_facts(conn, guild_id, accs)
        _read_user_habits(conn, guild_id, accs)
        _read_relationships(conn, guild_id, accs)
        _read_conversations(conn, guild_id, accs)
        _read_memory_tiers(conn, guild_id, accs)
        _read_broadcast_memory(conn, guild_id, accs)
        _read_community_presence(conn, guild_id, accs)
    finally:
        conn.close()

    for item in rd_context or []:
        text = " ".join(str(item.get(key) or "") for key in ("user_request", "main_subject", "response_summary", "suggested_lane")) if isinstance(item, dict) else str(item)
        for subject in _extract_subjects(text):
            ctype, connected = _alias_or_connection(text, subject)
            _add(accs, KnowledgeEvidence(subject, "rd_context", "internal_operator_context", ["internal_only", "owner_review_required", "public_use_not_allowed_until_review"], [_PRIVATE_WARNING], _safe_snippet(text), 2, ctype, connected))

    candidates: list[dict[str, Any]] = []
    duplicate_count = 0
    withheld_count = 0
    seen_ingest: set[str] = set()
    for key, acc in sorted(accs.items(), key=lambda item: (-sum(e.confidence_points for e in item[1].evidence), item[0])):
        if not acc.evidence:
            withheld_count += 1
            continue
        display_name = acc.subject_names.most_common(1)[0][0] if acc.subject_names else acc.subject_name
        source_types = sorted({e.source_type for e in acc.evidence})
        source_qualities = sorted({e.source_quality for e in acc.evidence})
        visibility = sorted({label for e in acc.evidence for label in e.visibility_labels})
        warnings = sorted({warning for e in acc.evidence for warning in e.warnings})
        score = sum(e.confidence_points for e in acc.evidence)
        confidence = "high" if score >= 7 and len(source_types) > 1 else ("medium" if score >= 3 else "low")
        candidate_type = _candidate_type(acc.evidence)
        evidence_summary = _safe_snippet(" | ".join(f"{e.source_type}/{e.source_quality}: {e.snippet}" for e in acc.evidence), MAX_EVIDENCE_SUMMARY_LENGTH)
        reason = _safe_snippet(
            f"Knowledge bridge found {display_name} in existing BNL local knowledge stores ({', '.join(source_types)}). "
            f"Source qualities: {', '.join(source_qualities)}. Visibility labels: {', '.join(visibility)}. Review-only Source File material; do not publish, draft, merge identities, or use publicly until owner review.",
            MAX_REASON_LENGTH,
        )
        if candidate_type == "possible_connection_review":
            connection = next((e.connected_subject for e in acc.evidence if e.connected_subject), "another subject")
            reason = _safe_snippet(f"Knowledge bridge found {display_name} connected to {connection}. This is a possible connection review, not confirmed identity or alias. " + reason, MAX_REASON_LENGTH)
        elif candidate_type == "identity_link":
            reason = _safe_snippet(f"Knowledge bridge found explicit alias/same-as language for {display_name}. Review identity link before merging or public use. " + reason, MAX_REASON_LENGTH)
        evidence_hash = hashlib.sha256(f"{key}\n{candidate_type}\n{','.join(source_types)}".encode("utf-8")).hexdigest()[:10]
        payload = build_dossier_recommendation_payload(
            {
                "type": candidate_type,
                "subjectName": display_name,
                "subjectKey": key,
                "reason": reason,
                "evidenceSummary": evidence_summary,
                "confidence": confidence,
                "sourceLanes": source_types,
                "sourceTypes": source_types,
                "sourceQuality": source_qualities,
                "visibilityLabels": visibility,
                "safetyWarnings": warnings,
                "suggestedAction": DEFAULT_SUGGESTED_ACTION,
                "missingInfo": list(DEFAULT_MISSING_INFO),
                "publicSafetyNotes": warnings + ["Internal BNL Source File recommendation only; proposed/public dossiers require separate owner review."],
                "doNotSay": ["Do not expose raw private/internal notes, raw Discord IDs, or full transcripts publicly.", "Do not claim identity certainty or confirmed aliases unless owner review confirms it."],
                "createdBy": "bnl",
                "ingestSource": KNOWLEDGE_BRIDGE_INGEST_SOURCE,
                "ingestKey": f"bnl:dossier:{KNOWLEDGE_BRIDGE_INGEST_SOURCE}:{candidate_type}:{key}:{evidence_hash}",
            }
        )
        if payload["ingestKey"] in seen_ingest:
            duplicate_count += 1
            continue
        seen_ingest.add(payload["ingestKey"])
        candidates.append({"subjectName": display_name, "subjectKey": key, "type": candidate_type, "sourceTypes": source_types, "warnings": warnings, "payload": payload, "score": score})
        if len(candidates) >= max_candidates:
            break

    limited = max(0, len(accs) - len(candidates) - duplicate_count - withheld_count)
    withheld_count += limited
    source_counts = Counter(source for c in candidates for source in c["sourceTypes"])
    warning_counts = Counter(warning for c in candidates for warning in c["warnings"])
    return {
        "totalSubjectsFound": len(accs),
        "sendableRecommendationCount": len(candidates),
        "duplicateCount": duplicate_count,
        "withheldCount": withheld_count,
        "sourceCounts": dict(sorted(source_counts.items())),
        "warningCounts": dict(sorted(warning_counts.items())),
        "candidates": candidates,
        "payloads": [c["payload"] for c in candidates],
    }


def _candidate_type(evidence: list[KnowledgeEvidence]) -> str:
    if any(e.candidate_type == "identity_link" for e in evidence):
        return "identity_link"
    if any(e.candidate_type == "possible_connection_review" for e in evidence):
        return "possible_connection_review"
    return "new_subject"


def run_source_knowledge_bridge(
    db_path: str,
    guild_id: int | None = None,
    *,
    limit: int = DEFAULT_KNOWLEDGE_BRIDGE_LIMIT,
    dry_run: bool = True,
    rd_context: list[dict[str, Any]] | None = None,
    sender: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    discovery = collect_knowledge_bridge_candidates(db_path, guild_id, limit=limit, rd_context=rd_context)
    sent = 0
    failed = 0
    send_duplicates = int(discovery.get("duplicateCount") or 0)
    first_error = "none"
    if not dry_run:
        send = sender or send_dossier_recommendation
        for payload in discovery.get("payloads") or []:
            result = send(payload)
            if result.get("ok") and result.get("duplicate"):
                send_duplicates += 1
            elif result.get("ok"):
                sent += 1
            else:
                failed += 1
                if first_error == "none":
                    first_error = f"failed:{result.get('status') or 'no_status'}"
    return {
        **discovery,
        "dryRun": bool(dry_run),
        "sentCount": sent,
        "failedCount": failed,
        "duplicateCount": send_duplicates,
        "errorStatus": first_error,
        "runTime": datetime.now(timezone.utc).isoformat(),
    }


def format_source_knowledge_bridge_response(result: dict[str, Any]) -> str:
    """Return a compact safe Discord summary without raw evidence/transcripts/IDs."""

    names = [str(c.get("subjectName") or "subject")[:40] for c in (result.get("candidates") or [])[:5]]
    sources = sorted((result.get("sourceCounts") or {}).items(), key=lambda item: (-int(item[1]), item[0]))[:3]
    warnings = sorted((result.get("warningCounts") or {}).items(), key=lambda item: (-int(item[1]), item[0]))[:2]
    source_text = ", ".join(source for source, _count in sources) or "none"
    warning_text = ", ".join(f"{count} {warning}" for warning, count in warnings) or "none"
    candidate_text = ", ".join(names) or "none"
    if result.get("dryRun"):
        return (
            f"Knowledge bridge dry run: {int(result.get('sendableRecommendationCount') or 0)} sendable, "
            f"{int(result.get('duplicateCount') or 0)} duplicates, {int(result.get('withheldCount') or 0)} withheld. "
            f"Candidates: {candidate_text}. Sources: {source_text}. Warnings: {warning_text}."
        )
    return (
        f"Knowledge bridge complete: {int(result.get('sentCount') or 0)} recommendations sent, "
        f"{int(result.get('duplicateCount') or 0)} duplicates skipped, {int(result.get('withheldCount') or 0)} withheld, "
        f"{int(result.get('failedCount') or 0)} failed."
    )
