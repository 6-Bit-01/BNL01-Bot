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
from bnl_entity_activity_summary import build_entity_activity_summary
from bnl_dossier_source_packets import DEFAULT_MISSING_INFO, DEFAULT_SUGGESTED_ACTION, merge_entity_activity_summary_into_packet, subject_key

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
    "a", "an", "the", "and", "or", "but", "if", "so", "to", "of", "in", "on", "at", "by",
    "what", "when", "where", "why", "who", "how", "there", "here", "then", "than", "this",
    "that", "these", "those", "such", "also", "just", "really", "very", "much", "more", "most",
    "less", "least", "alot", "about", "into", "onto", "from", "with", "without", "because",
    "before", "after", "again", "still", "even", "maybe", "probably", "actually", "basically",
    "something", "anything", "nothing", "everything", "thing", "stuff", "people", "person", "anyone",
    "someone", "somebody", "everyone", "nobody", "message", "messages", "conversation", "reply",
    "replies", "said", "says", "say", "ask", "asked", "tell", "told", "think", "thought", "know",
    "knew", "make", "made", "want", "need", "use", "used", "using", "channel", "server", "discord",
    "bot", "command", "source", "file", "candidate", "candidates", "dossier", "dossiers", "memory",
    "bridge", "public", "private", "internal", "review", "admin", "operator", "owner", "mod", "moderator",
    "summary", "profile", "relationship", "community", "presence", "unknown", "today", "tonight", "user",
    "customer", "account",
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


def _subject_words(value: str) -> list[str]:
    return re.findall(r"[A-Za-z0-9]+", str(value or "").lower())


def _is_trusted_name_source(source_type: str) -> bool:
    return source_type in {"user_profiles", "community_presence", "trusted_name_field", "structured_identity"}


def _has_actual_proper_noun(value: str) -> bool:
    return any(re.match(r"^[A-Z0-9]", part) for part in re.findall(r"[A-Za-z0-9][A-Za-z0-9'_-]*", str(value or "")))


def bridge_subject_rejection_reason(
    subject: str,
    evidence_context: str | dict[str, Any] | None = None,
    source_type: str = "free_text",
    *,
    known_names: set[str] | None = None,
    trusted_name_field: bool = False,
    explicit_subject_language: bool = False,
) -> str:
    """Return an empty string when a subject is safe enough for bridge review."""

    name = _clean_subject_name(subject)
    known_keys = {subject_key(name) for name in (known_names or set()) if name}
    trusted = trusted_name_field or _is_trusted_name_source(source_type)
    if not name or len(name) < 3:
        return "too_short"
    if name in _NAME_STOPWORDS:
        return "weak_generic_term"
    if re.fullmatch(r"\d+", name) or _LONG_ID_PATTERN.fullmatch(name):
        return "numeric_or_id"
    if not re.search(r"[A-Za-z0-9]", name):
        return "punctuation_only"
    words = _subject_words(name)
    if not words:
        return "punctuation_only"
    compact = " ".join(words)
    if compact in _WEAK_TERMS or compact == "a lot":
        return "weak_generic_term"
    if words[0] in _WEAK_TERMS or words[-1] in _WEAK_TERMS:
        return "weak_edge_term"
    weak_count = sum(1 for word in words if word in _WEAK_TERMS)
    if weak_count and weak_count >= max(1, len(words) / 2):
        return "mostly_weak_terms"
    if len(words) > 4 and not trusted:
        return "phrase_too_long"
    if len(words) == 1 and name.islower() and not trusted and subject_key(name) not in known_keys:
        return "lowercase_free_text_fragment"
    if not trusted and subject_key(name) not in known_keys and not explicit_subject_language and not _has_actual_proper_noun(name):
        return "lowercase_free_text_fragment"
    return ""


def is_valid_bridge_subject(subject: str, evidence_context: str | dict[str, Any] | None = None, source_type: str = "free_text") -> bool:
    """Strict public helper for Source Knowledge Bridge subject validation."""

    return not bridge_subject_rejection_reason(subject, evidence_context, source_type)


def _is_weak_subject(value: str) -> bool:
    return bool(bridge_subject_rejection_reason(value, source_type="trusted_name_field", trusted_name_field=True))


@dataclass
class BridgeDiagnostics:
    withheld_reasons: Counter = field(default_factory=Counter)
    withheld_examples: dict[str, list[str]] = field(default_factory=dict)
    rejected_source_counts: Counter = field(default_factory=Counter)

    def reject(self, subject: str, source_type: str, reason: str) -> None:
        if not reason:
            return
        self.withheld_reasons[reason] += 1
        self.rejected_source_counts[source_type or "unknown"] += 1
        examples = self.withheld_examples.setdefault(reason, [])
        cleaned = _clean_subject_name(subject)
        if cleaned and len(examples) < 5 and cleaned not in examples:
            examples.append(cleaned)


def _candidate_reason(candidate: str, source_type: str, known_names: set[str] | None, explicit: bool, trusted: bool) -> str:
    return bridge_subject_rejection_reason(
        candidate,
        source_type=source_type,
        known_names=known_names,
        trusted_name_field=trusted,
        explicit_subject_language=explicit,
    )


def _add_candidate(
    subjects: list[str],
    seen: set[str],
    candidate: str,
    *,
    source_type: str,
    known_names: set[str] | None,
    diagnostics: BridgeDiagnostics | None,
    explicit: bool = False,
    trusted: bool = False,
) -> None:
    cleaned = _clean_subject_name(candidate)
    reason = _candidate_reason(cleaned, source_type, known_names, explicit, trusted)
    if reason:
        if diagnostics:
            diagnostics.reject(cleaned or candidate, source_type, reason)
        return
    key = subject_key(cleaned)
    if key not in seen:
        seen.add(key)
        subjects.append(cleaned)


def _extract_known_name_mentions(text: str, known_names: set[str] | None) -> list[str]:
    found: list[str] = []
    for known in sorted(known_names or set(), key=lambda item: (-len(item), item.lower())):
        cleaned = _clean_subject_name(known)
        if not cleaned or bridge_subject_rejection_reason(cleaned, source_type="trusted_name_field", trusted_name_field=True):
            continue
        if re.search(rf"(?<![A-Za-z0-9]){re.escape(cleaned)}(?![A-Za-z0-9])", text or "", flags=re.I):
            found.append(cleaned)
    return found


def _extract_subjects(
    text: str,
    *,
    source_type: str = "free_text",
    known_names: set[str] | None = None,
    diagnostics: BridgeDiagnostics | None = None,
    trusted_name_field: bool = False,
) -> list[str]:
    subjects: list[str] = []
    seen: set[str] = set()
    source = source_type or "free_text"
    if trusted_name_field:
        _add_candidate(subjects, seen, text, source_type=source, known_names=known_names, diagnostics=diagnostics, trusted=True)
        return subjects[:8]

    # Known display/preferred/community names are allowed to surface from broad text as corroborating evidence.
    for known in _extract_known_name_mentions(text or "", known_names):
        _add_candidate(subjects, seen, known, source_type=source, known_names=known_names, diagnostics=diagnostics, explicit=True)

    label_patterns = (
        rf"\b(?i:source file|dossier|profile|memory|fact|habit|relationship|journal|context|subject|candidate|regular|artist|collaborator|supporter|sponsor|entity|concept|lore)\s+(?i:for|on|about|named)?\s*({_SUBJECT_PHRASE})\b",
        rf"\b(?i:known as|called|named|aka|artist|subject:)\s+({_SUBJECT_PHRASE})\b",
        rf"\b(?i:source file for|source file on|dossier for|candidate named)\s+({_SUBJECT_PHRASE})\b",
    )
    statement_patterns = (
        rf"\b({_SUBJECT_PHRASE})\s+(?i:is|was|seems|appears|became|becomes|remains)\s+(?i:a\s+|an\s+)?(?i:private\s+|internal\s+|public\s+|community\s+|known\s+|recurring\s+)*?(?i:regular|known|recurring|artist|collaborator|mod|moderator|supporter|sponsor|entity|concept|candidate|subject|participant)\b",
        rf"\b({_SUBJECT_PHRASE})\s+(?i:aka|also known as|same as|same-as|alias for)\s+({_SUBJECT_PHRASE})\b",
        rf"\b({_SUBJECT_PHRASE})\s+(?i:via|through|introduced\s+by|connected\s+to)\s+({_SUBJECT_PHRASE})\b",
        rf"\b(?i:memory says|operator noted|manual note says)\s+({_SUBJECT_PHRASE})\s+(?i:is|was|seems|appears)?\s*(?i:a\s+|an\s+)?(?i:private\s+|internal\s+|public\s+|community\s+|known\s+|recurring\s+)*?(?i:regular|known|recurring|artist|collaborator|entity|concept|candidate|subject)\b",
    )
    for pattern in label_patterns + statement_patterns:
        for match in re.finditer(pattern, text or ""):
            _add_candidate(subjects, seen, match.group(1), source_type=source, known_names=known_names, diagnostics=diagnostics, explicit=True)
            if len(match.groups()) > 1:
                _add_candidate(subjects, seen, match.group(2), source_type=source, known_names=known_names, diagnostics=diagnostics, explicit=True)

    # Intentionally no case-insensitive all-text subject sweep: lowercase filler must not become candidates.
    return subjects[:8]


def _alias_or_connection(text: str, subject: str) -> tuple[str, str]:
    lowered = text or ""
    if re.search(r"\b(alias|aka|also known as|same as|same-as|identity link|confirmed alias)\b", lowered, re.I):
        return "identity_link", ""
    patterns = (
        rf"\b{re.escape(subject)}\s+(?:via|through|introduced\s+by|connected\s+to)\s+({_SUBJECT_PHRASE})\b",
        rf"\b({_SUBJECT_PHRASE})\s+(?:via|through|introduced\s+by|connected\s+to)\s+{re.escape(subject)}\b",
    )
    for pattern in patterns:
        match = re.search(pattern, lowered)
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


def _read_user_profiles(conn: sqlite3.Connection, guild_id: int | None, accs: dict[str, KnowledgeAccumulator], known_names: set[str], diagnostics: BridgeDiagnostics) -> None:
    if not _table_exists(conn, "user_profiles"):
        return
    where = "WHERE guild_id=?" if guild_id else ""
    params: tuple[Any, ...] = (guild_id,) if guild_id else ()
    for row in conn.execute(f"SELECT display_name, preferred_name FROM user_profiles {where} LIMIT 500", params):
        for name in (row[1], row[0]):
            subject = _clean_subject_name(name)
            reason = bridge_subject_rejection_reason(subject, source_type="user_profiles", trusted_name_field=True)
            if reason:
                diagnostics.reject(subject or str(name or ""), "user_profiles", reason)
                continue
            known_names.add(subject)
            _add(accs, KnowledgeEvidence(subject, "user_profiles", "local_profile_observed", ["internal_only", "private_review_required", "owner_review_required", "public_use_not_allowed_until_review"], [_PRIVATE_WARNING], f"Local profile observed for {subject}.", 2))
            break


def _read_user_memory_facts(conn: sqlite3.Connection, guild_id: int | None, accs: dict[str, KnowledgeAccumulator], known_names: set[str], diagnostics: BridgeDiagnostics) -> None:
    if not _table_exists(conn, "user_memory_facts"):
        return
    where = "WHERE guild_id=?" if guild_id else ""
    params: tuple[Any, ...] = (guild_id,) if guild_id else ()
    for row in conn.execute(f"SELECT fact_key, fact_value, confidence FROM user_memory_facts {where} ORDER BY updated_at DESC LIMIT 800", params):
        text = f"{row[0]}: {row[1]}"
        for subject in _extract_subjects(text, source_type="user_memory_facts", known_names=known_names, diagnostics=diagnostics):
            ctype, connected = _alias_or_connection(text, subject)
            _add(accs, KnowledgeEvidence(subject, "user_memory_facts", "local_memory_fact", ["internal_only", "private_review_required", "owner_review_required", "public_use_not_allowed_until_review"], [_PRIVATE_WARNING], _safe_snippet(text), max(1, int(float(row[2] or 0.5) * 3)), ctype, connected))


def _read_user_habits(conn: sqlite3.Connection, guild_id: int | None, accs: dict[str, KnowledgeAccumulator], known_names: set[str], diagnostics: BridgeDiagnostics) -> None:
    if not _table_exists(conn, "user_habits"):
        return
    where = "WHERE guild_id=?" if guild_id else ""
    params: tuple[Any, ...] = (guild_id,) if guild_id else ()
    for row in conn.execute(f"SELECT last_topic, total_messages FROM user_habits {where} ORDER BY updated_at DESC LIMIT 500", params):
        text = f"Habit trace topic: {row[0] or ''}; messages observed: {int(row[1] or 0)}"
        for subject in _extract_subjects(str(row[0] or ""), source_type="user_habits", known_names=known_names, diagnostics=diagnostics):
            _add(accs, KnowledgeEvidence(subject, "user_habits", "local_behavior_trace", ["internal_only", "private_review_required", "owner_review_required", "public_use_not_allowed_until_review"], [_PRIVATE_WARNING], _safe_snippet(text), 1))


def _read_relationships(conn: sqlite3.Connection, guild_id: int | None, accs: dict[str, KnowledgeAccumulator], known_names: set[str], diagnostics: BridgeDiagnostics) -> None:
    if _table_exists(conn, "relationship_state"):
        where = "WHERE guild_id=?" if guild_id else ""
        params: tuple[Any, ...] = (guild_id,) if guild_id else ()
        for row in conn.execute(f"SELECT last_topic, trust_stage, social_stance, interaction_count FROM relationship_state {where} ORDER BY updated_at DESC LIMIT 500", params):
            text = f"Relationship trace topic: {row[0] or ''}; stage {row[1] or 'unknown'}; stance {row[2] or 'unknown'}; interactions {int(row[3] or 0)}"
            for subject in _extract_subjects(str(row[0] or ""), source_type="relationship_state", known_names=known_names, diagnostics=diagnostics):
                _add(accs, KnowledgeEvidence(subject, "relationship_state", "local_relationship_trace", ["internal_only", "private_review_required", "owner_review_required", "public_use_not_allowed_until_review"], [_PRIVATE_WARNING], _safe_snippet(text), 1))
    if _table_exists(conn, "relationship_journal"):
        where = "WHERE guild_id=?" if guild_id else ""
        params = (guild_id,) if guild_id else ()
        for row in conn.execute(f"SELECT entry_type, summary FROM relationship_journal {where} ORDER BY timestamp DESC LIMIT 800", params):
            text = f"{row[0]}: {row[1]}"
            for subject in _extract_subjects(text, source_type="relationship_journal", known_names=known_names, diagnostics=diagnostics):
                ctype, connected = _alias_or_connection(text, subject)
                _add(accs, KnowledgeEvidence(subject, "relationship_journal", "local_relationship_trace", ["internal_only", "private_review_required", "owner_review_required", "public_use_not_allowed_until_review"], [_PRIVATE_WARNING], _safe_snippet(text), 1, ctype, connected))


def _read_conversations(conn: sqlite3.Connection, guild_id: int | None, accs: dict[str, KnowledgeAccumulator], known_names: set[str], diagnostics: BridgeDiagnostics) -> None:
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
        for subject in _extract_subjects(str(content or ""), source_type="conversations", known_names=known_names, diagnostics=diagnostics):
            ctype, connected = _alias_or_connection(str(content or ""), subject)
            _add(accs, KnowledgeEvidence(subject, "conversations", quality, labels, warnings, _safe_snippet(f"{policy or 'unknown'} conversation {role or 'row'} mentions {subject}."), 2 if quality == "public_discord_observed" else 1, ctype, connected))


def _read_memory_tiers(conn: sqlite3.Connection, guild_id: int | None, accs: dict[str, KnowledgeAccumulator], known_names: set[str], diagnostics: BridgeDiagnostics) -> None:
    if not _table_exists(conn, "memory_tiers"):
        return
    where = "WHERE guild_id=?" if guild_id else ""
    params = (guild_id,) if guild_id else ()
    for row in conn.execute(f"SELECT tier, summary, salience FROM memory_tiers {where} ORDER BY updated_at DESC LIMIT 800", params):
        text = f"{row[0]} memory: {row[1]}"
        for subject in _extract_subjects(str(row[1] or ""), source_type="memory_tiers", known_names=known_names, diagnostics=diagnostics):
            ctype, connected = _alias_or_connection(str(row[1] or ""), subject)
            _add(accs, KnowledgeEvidence(subject, "memory_tiers", "source_blind_memory_trace", ["source_blind_review_required", "internal_only", "owner_review_required", "public_use_not_allowed_until_review"], [SOURCE_BLIND_WARNING], _safe_snippet(text), max(1, int(float(row[2] or 0.5) * 2)), ctype, connected))


def _read_broadcast_memory(conn: sqlite3.Connection, guild_id: int | None, accs: dict[str, KnowledgeAccumulator], known_names: set[str], diagnostics: BridgeDiagnostics) -> None:
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
        for subject in _extract_subjects(text, source_type="broadcast_memory", known_names=known_names, diagnostics=diagnostics):
            ctype, connected = _alias_or_connection(text, subject)
            _add(accs, KnowledgeEvidence(subject, "broadcast_memory", "official_show_memory", labels, [_PUBLIC_REVIEW_WARNING], _safe_snippet(text), 3, ctype, connected))


def _read_community_presence(conn: sqlite3.Connection, guild_id: int | None, accs: dict[str, KnowledgeAccumulator], known_names: set[str], diagnostics: BridgeDiagnostics) -> None:
    if not _table_exists(conn, "community_presence"):
        return
    where = "WHERE guild_id=?" if guild_id else ""
    params = (guild_id,) if guild_id else ()
    for row in conn.execute(f"SELECT display_name, mention_count, direct_interaction_count, operator_mention_count, connection_notes, evidence_snippets FROM community_presence {where} ORDER BY (mention_count + direct_interaction_count + operator_mention_count) DESC LIMIT 500", params):
        subject = _clean_subject_name(row[0])
        reason = bridge_subject_rejection_reason(subject, source_type="community_presence", trusted_name_field=True)
        if reason:
            diagnostics.reject(subject or str(row[0] or ""), "community_presence", reason)
            continue
        known_names.add(subject)
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



def _source_meaning(item: KnowledgeEvidence, display_name: str) -> tuple[str, str, str]:
    """Return meaning-first source title, observation, and privacy boundary."""

    name = _safe_snippet(display_name or item.subject_name, 80)
    if item.source_type == "user_profiles":
        return (
            "Internal local profile",
            f"BNL found an internal local profile match for {name}.",
            "internal_only_owner_review_required",
        )
    if item.source_type in {"relationship_state", "relationship_journal"}:
        return (
            "Prior relationship/context notes",
            f"BNL found prior relationship/context notes connected to {name}.",
            "private_review_required_do_not_use_publicly",
        )
    if item.source_type == "conversations":
        if "public_safe_candidate" in item.visibility_labels:
            return (
                "Approved public-side conversation context",
                f"BNL found approved public-side conversation context connected to {name}.",
                "public_safe_candidate_owner_review_required",
            )
        return (
            "Internal conversation context",
            f"BNL found conversation context connected to {name}, but its visibility is not public-safe without review.",
            "internal_only_owner_review_required",
        )
    if item.source_type == "community_presence":
        return (
            "Community presence signal",
            f"BNL found approved community presence signals connected to {name}.",
            "public_safe_candidate_owner_review_required",
        )
    if item.source_type == "broadcast_memory":
        return (
            "BARCODE Radio memory",
            f"BNL found BARCODE Radio/show-history context connected to {name}.",
            "owner_review_required_before_public_use",
        )
    if item.source_type == "memory_tiers":
        return (
            "Source-blind memory trace",
            f"BNL found older source-blind memory context connected to {name}.",
            "source_blind_private_review_required",
        )
    if item.source_type == "user_memory_facts":
        return (
            "Internal memory fact",
            f"BNL found an internal memory fact connected to {name}.",
            "internal_only_owner_review_required",
        )
    if item.source_type == "user_habits":
        return (
            "Internal habit/topic signal",
            f"BNL found internal topic/habit context connected to {name}.",
            "internal_only_owner_review_required",
        )
    if item.source_type == "rd_context":
        return (
            "Operator R&D context",
            f"BNL found operator-provided R&D context connected to {name}.",
            "internal_only_owner_review_required",
        )
    return ("Internal review context", f"BNL found review-only context connected to {name}.", "owner_review_required")


def _unique_preserve(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        clean = _safe_snippet(value, 240)
        if not clean or clean in seen:
            continue
        seen.add(clean)
        out.append(clean)
    return out


def _build_meaning_first_source_packet(
    display_name: str,
    evidence: list[KnowledgeEvidence],
    source_types: list[str],
    source_qualities: list[str],
    visibility: list[str],
    warnings: list[str],
    confidence: str,
    candidate_type: str,
) -> dict[str, Any]:
    """Build structured review context while keeping raw labels in rawProvenance."""

    source_counts = Counter(item.source_type for item in evidence)
    known_context: list[str] = []
    useful_evidence: list[str] = []
    relationship_signals: list[str] = []
    public_safe: list[str] = []
    private_notes: list[str] = []
    not_public_yet: list[str] = []
    source_authority: list[dict[str, str]] = []

    for item in evidence:
        title, observation, boundary = _source_meaning(item, display_name)
        known_context.append(observation)
        useful_evidence.append(observation)
        source_authority.append({"source": title, "boundary": boundary, "confidence": confidence})
        if item.source_type in {"relationship_state", "relationship_journal"}:
            relationship_signals.append(observation + " Treat this as private relationship context, not a public fact.")
            private_notes.append(observation + " Owner review must decide if any wording can be reused.")
        if "public_safe_candidate" in item.visibility_labels:
            public_safe.append(observation + " It may inform public wording only after owner review.")
        else:
            private_notes.append(observation + " Keep this internal until reviewed.")
        if "owner_review_required" in item.visibility_labels or "public_use_not_allowed_until_review" in item.visibility_labels:
            not_public_yet.append(observation + " Not public until owner review confirms safe use.")
        if "source_blind_review_required" in item.visibility_labels:
            private_notes.append("Source-blind memory requires source/visibility review before use.")

    if candidate_type == "possible_connection_review":
        connection = next((e.connected_subject for e in evidence if e.connected_subject), "another subject")
        relationship_signals.insert(0, f"Possible connection to {_safe_snippet(connection, 80)} needs review; this is not an alias or identity match.")
        not_public_yet.insert(0, "Do not present possible connections as confirmed identity, alias, or public fact.")
    elif candidate_type == "identity_link":
        private_notes.insert(0, "Alias/same-as language was found, but BNL must not merge aliases or identities without owner review.")
        not_public_yet.insert(0, "Do not publish identity-link claims until owner review confirms them.")

    if not public_safe:
        public_safe.append("No public-safe fact is confirmed by this packet; owner review is required before any public use.")

    return {
        "knownContext": _unique_preserve(known_context)[:6],
        "usefulEvidence": _unique_preserve(useful_evidence)[:6],
        "relationshipSignals": _unique_preserve(relationship_signals)[:4],
        "publicSafePossibilities": _unique_preserve(public_safe)[:4],
        "privateOnlyNotes": _unique_preserve(private_notes)[:6],
        "missingInfo": list(DEFAULT_MISSING_INFO),
        "notPublicYet": _unique_preserve(not_public_yet)[:6],
        "recommendedAction": "Keep as internal Source File review material until owner review decides what, if anything, can be public-safe.",
        "confidence": confidence,
        "sourceAuthority": source_authority[:8],
        "rawProvenance": {
            "sourceLabels": [f"{item.source_type}/{item.source_quality}" for item in evidence],
            "sourceLaneMapping": {source: "unknown" for source in source_types},
            "rawFragments": [
                {
                    "sourceLabel": f"{item.source_type}/{item.source_quality}",
                    "visibilityLabels": list(item.visibility_labels),
                    "warnings": list(item.warnings),
                    "snippet": item.snippet,
                }
                for item in evidence
            ],
            "sourceCounts": dict(sorted(source_counts.items())),
            "sourceTypes": list(source_types),
            "sourceQualities": list(source_qualities),
            "visibilityLabels": list(visibility),
        },
    }


def _meaning_first_reason(display_name: str, packet: dict[str, Any], candidate_type: str) -> str:
    contexts = list(packet.get("knownContext") or [])
    relationship = list(packet.get("relationshipSignals") or [])
    name = _safe_snippet(display_name, 80)
    if candidate_type == "possible_connection_review":
        prefix = f"BNL found review-only context for {name}, including a possible relationship/connection signal; this is not confirmed identity or alias."
    elif candidate_type == "identity_link":
        prefix = f"BNL found review-only identity/alias language for {name}."
    else:
        prefix = f"BNL found existing internal context for {name}."
    details = []
    for item in contexts + relationship:
        if "local profile match" in item or "relationship/context notes" in item or "public-side conversation" in item or "community presence" in item or "BARCODE Radio" in item:
            details.append(item)
        if len(details) >= 2:
            break
    middle = " " + " ".join(details) if details else ""
    return _safe_snippet(prefix + middle + " Keep this internal until owner review confirms what can be used publicly.", MAX_REASON_LENGTH)


def _case_bridge_evidence_summary(display_name: str, evidence: list[KnowledgeEvidence], source_types: list[str], candidate_type: str) -> str:
    name = _safe_snippet(display_name, 80)
    if not evidence:
        return f"{name} needs owner review before BNL can write a useful Source File note."

    observations = []
    for item in evidence:
        _title, observation, _boundary = _source_meaning(item, display_name)
        observations.append(observation)

    if candidate_type == "possible_connection_review":
        connection = next((e.connected_subject for e in evidence if e.connected_subject), "another subject")
        lead = f"BNL found review-only context for {name}, including a possible connection to {_safe_snippet(connection, 80)}; this is not a confirmed alias or identity match."
    elif candidate_type == "identity_link":
        lead = f"BNL found review-only alias/same-as language for {name}; identity changes still need owner review before any merge or public use."
    else:
        lead = f"BNL found existing internal context for {name}."

    detail = " ".join(_unique_preserve(observations)[:3])
    return _safe_snippet(f"{lead} {detail} Keep this as internal Source File review material until owner-approved public wording exists.", MAX_EVIDENCE_SUMMARY_LENGTH)


def collect_knowledge_bridge_candidates(db_path: str, guild_id: int | None = None, *, limit: int = DEFAULT_KNOWLEDGE_BRIDGE_LIMIT, rd_context: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    """Read approved local stores and return review-only recommendation payloads."""

    max_candidates = max(1, min(MAX_KNOWLEDGE_BRIDGE_LIMIT, int(limit or DEFAULT_KNOWLEDGE_BRIDGE_LIMIT)))
    accs: dict[str, KnowledgeAccumulator] = {}
    known_names: set[str] = set()
    diagnostics = BridgeDiagnostics()
    conn = sqlite3.connect(db_path)
    try:
        # Build trusted name registry first, before broad free-text stores.
        _read_user_profiles(conn, guild_id, accs, known_names, diagnostics)
        _read_community_presence(conn, guild_id, accs, known_names, diagnostics)
        _read_user_memory_facts(conn, guild_id, accs, known_names, diagnostics)
        _read_user_habits(conn, guild_id, accs, known_names, diagnostics)
        _read_relationships(conn, guild_id, accs, known_names, diagnostics)
        _read_conversations(conn, guild_id, accs, known_names, diagnostics)
        _read_memory_tiers(conn, guild_id, accs, known_names, diagnostics)
        _read_broadcast_memory(conn, guild_id, accs, known_names, diagnostics)
    finally:
        conn.close()

    for item in rd_context or []:
        text = " ".join(str(item.get(key) or "") for key in ("user_request", "main_subject", "response_summary", "suggested_lane")) if isinstance(item, dict) else str(item)
        for subject in _extract_subjects(text, source_type="rd_context", known_names=known_names, diagnostics=diagnostics):
            ctype, connected = _alias_or_connection(text, subject)
            _add(accs, KnowledgeEvidence(subject, "rd_context", "internal_operator_context", ["internal_only", "owner_review_required", "public_use_not_allowed_until_review"], [_PRIVATE_WARNING], _safe_snippet(text), 2, ctype, connected))

    candidates: list[dict[str, Any]] = []
    duplicate_count = 0
    withheld_count = sum(diagnostics.withheld_reasons.values())
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
        source_packet = _build_meaning_first_source_packet(
            display_name,
            acc.evidence,
            source_types,
            source_qualities,
            visibility,
            warnings,
            confidence,
            candidate_type,
        )
        entity_summary = build_entity_activity_summary(
            db_path,
            display_name,
            guild_id,
            [
                "user_profiles",
                "user_memory_facts",
                "user_habits",
                "relationship_state",
                "relationship_journal",
                "memory_tiers",
                "conversations",
                "broadcast_memory",
                "rd_context",
            ],
            "admin_internal",
            50,
            rd_context,
        )
        merge_entity_activity_summary_into_packet(source_packet, entity_summary)
        confidence = str(source_packet.get("confidence") or confidence)
        evidence_summary = _case_bridge_evidence_summary(display_name, acc.evidence, source_types, candidate_type)
        reason = _meaning_first_reason(display_name, source_packet, candidate_type)
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
                "suggestedAction": source_packet["recommendedAction"],
                "knownContext": source_packet["knownContext"],
                "usefulEvidence": source_packet["usefulEvidence"],
                "relationshipSignals": source_packet["relationshipSignals"],
                "publicSafePossibilities": source_packet["publicSafePossibilities"],
                "privateOnlyNotes": source_packet["privateOnlyNotes"],
                "missingInfo": source_packet["missingInfo"],
                "notPublicYet": source_packet["notPublicYet"],
                "recommendedAction": source_packet["recommendedAction"],
                "sourceAuthority": source_packet["sourceAuthority"],
                "rawProvenance": source_packet["rawProvenance"],
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
    top_withheld_reasons = dict(diagnostics.withheld_reasons.most_common(8))
    junk_terms_blocked = sum(
        count
        for reason, count in diagnostics.withheld_reasons.items()
        if reason in {"weak_generic_term", "weak_edge_term", "mostly_weak_terms", "lowercase_free_text_fragment"}
    )
    return {
        "totalSubjectsFound": len(accs),
        "sendableRecommendationCount": len(candidates),
        "duplicateCount": duplicate_count,
        "withheldCount": withheld_count,
        "sourceCounts": dict(sorted(source_counts.items())),
        "warningCounts": dict(sorted(warning_counts.items())),
        "topWithheldReasons": top_withheld_reasons,
        "junkTermsBlockedCount": junk_terms_blocked,
        "rejectedSourceCounts": dict(sorted(diagnostics.rejected_source_counts.items())),
        "withheldExamples": {reason: values for reason, values in sorted(diagnostics.withheld_examples.items())},
        "diagnosticSummary": {
            "knownNameCount": len(known_names),
            "knownNamesSample": sorted(known_names, key=str.lower)[:10],
            "sourceTablesWithCandidates": sorted(source_counts),
            "rejectedSourceCounts": dict(sorted(diagnostics.rejected_source_counts.items())),
        },
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
    withheld_reasons = sorted((result.get("topWithheldReasons") or {}).items(), key=lambda item: (-int(item[1]), item[0]))[:3]
    source_text = ", ".join(source for source, _count in sources) or "none"
    warning_text = ", ".join(f"{count} {warning}" for warning, count in warnings) or "none"
    withheld_text = ", ".join(f"{reason}={count}" for reason, count in withheld_reasons) or "none"
    candidate_text = ", ".join(names) or "none"
    if result.get("dryRun"):
        return (
            f"Knowledge bridge dry run: {int(result.get('sendableRecommendationCount') or 0)} sendable, "
            f"{int(result.get('duplicateCount') or 0)} duplicates, {int(result.get('withheldCount') or 0)} withheld. "
            f"Sendable: {candidate_text}. Top withheld reasons: {withheld_text}. "
            f"Junk terms blocked: {int(result.get('junkTermsBlockedCount') or 0)}. Sources: {source_text}. Warnings: {warning_text}."
        )
    return (
        f"Knowledge bridge complete: {int(result.get('sentCount') or 0)} recommendations sent, "
        f"{int(result.get('duplicateCount') or 0)} duplicates skipped, {int(result.get('withheldCount') or 0)} withheld, "
        f"{int(result.get('failedCount') or 0)} failed."
    )
