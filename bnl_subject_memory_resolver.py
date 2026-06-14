"""Subject memory evidence resolver for BNL dossier/source-file drafting.

The resolver is read-only and conservative: it scans known memory/evidence tables
when they exist, classifies candidate subject memories by public-readiness, and
only returns sanitized text in public-safe sections.
"""
from __future__ import annotations

import logging
import re
import sqlite3
from typing import Any

LOG = logging.getLogger(__name__)

_PAYMENT_RE = re.compile(r"\b(stripe|checkout|payment|paid|purchase|purchased|customer|priority\s*signal|priority)\b", re.I)
_INTERNAL_RE = re.compile(r"\b(admin[-\s]*only|internal|private\s+alias|raw\s*id|discord\s*id|user\s*id|rowid|debug|diagnostic|database|table|source\s*row|memory_tiers|redis|dm|direct\s+message)\b", re.I)
_SOURCE_BLIND_RE = re.compile(r"\b(source[-_\s]*blind|unknown[-_\s]*policy|no\s+provenance|unverified\s+source)\b", re.I)
_REVIEW_RE = re.compile(r"\b(review[-\s]*only|needs?\s+confirmation|unconfirmed|uncertain|inferred|ambiguous|maybe|appears\s+to|queue|submission|relationship)\b", re.I)
_PUBLIC_RE = re.compile(r"\b(public[-_\s]*safe|dossier[-_\s]*safe|public\s+context|public\s+home|public\s+discord|owner[-_\s]*confirmed|official\s+public|broadcast\s+memory)\b", re.I)
_LINK_RE = re.compile(r"https?://\S+", re.I)
_ROLE_OR_TAXONOMY_LABELS = {
    "artist", "member", "community", "collaborator", "mod", "moderator", "personnel", "staff", "sponsor", "system", "interface", "production", "radio", "producer", "ai", "human", "hybrid", "unknown", "public", "internal", "restricted", "active", "pending", "person", "music", "tech", "systems", "broadcast", "participant", "dossier", "source", "source file",
}
_PUBLIC_POLICIES = {"public_home", "public_context", "public_selective", "broadcast_memory", "public"}
_PUBLIC_VISIBILITIES = {"public", "public_safe", "dossier_safe", "public_candidate", "public_use"}
_PUBLIC_AUTHORITIES = {"public", "public_safe", "dossier_safe", "broadcast_memory", "public_conversation", "owner_confirmed", "official_public_dossier", "public_discord_observed", "queue_submission_confirmed"}


def _text(value: Any, limit: int = 320) -> str:
    if value is None or isinstance(value, (dict, list)):
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()[:limit].rstrip()


def _subject_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", (value or "").lower()).strip("_")


def _is_probable_subject_alias(label: str) -> bool:
    clean = _text(label, 100)
    low = clean.lower().strip()
    if not clean or low in _ROLE_OR_TAXONOMY_LABELS or len(clean) < 3 or len(clean) > 80:
        return False
    words = re.findall(r"[A-Za-z0-9]+", clean)
    return bool(words) and not (len(words) == 1 and clean.islower() and not any(ch.isdigit() for ch in clean))


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    try:
        return bool(conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone())
    except sqlite3.Error:
        return False


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}
    except sqlite3.Error:
        return set()


def _matches(text: str, terms: list[str]) -> bool:
    low = (text or "").lower()
    keys = {_subject_key(text)}
    return any(term and (term.lower() in low or _subject_key(term) in keys or _subject_key(term) in _subject_key(text)) for term in terms)


def _sanitize_public(text: str, subject: str, aliases: list[str]) -> str:
    clean = _text(text, 260)
    for alias in sorted([a for a in aliases if a.lower() != subject.lower()], key=len, reverse=True):
        clean = re.sub(re.escape(alias), subject, clean, flags=re.I)
    if _PAYMENT_RE.search(clean) or _INTERNAL_RE.search(clean):
        return ""
    return clean


def _blank_result(subject: str, aliases: list[str]) -> dict[str, Any]:
    return {
        "subjectName": subject,
        "matchedAliasesUsedPrivately": aliases[:8],
        "publicSafeFacts": [], "publicSafeNotes": [], "reviewOnlyEvidence": [], "privateOrInternalEvidence": [],
        "publicRoleSignals": [], "publicCreativeMusicSignals": [], "publicCommunitySignals": [], "publicLinkSignals": [],
        "queueOrSubmissionSignals": [], "relationshipOrContextSignals": [], "sourceSafetyWarnings": [], "missingInfoQuestions": [],
        "confidence": "low",
        "evidenceCounts": {"publicSafe": 0, "reviewOnly": 0, "privateOrInternal": 0, "sourceBlind": 0, "totalScanned": 0},
        "diagnostic": {"tablesScanned": [], "tablesContributed": {}, "classificationReasons": {}},
    }


def _public_ready(data: dict[str, Any], text: str) -> bool:
    blob = " ".join(str(data.get(k) or "") for k in data)
    return bool(data.get("public_safe") or data.get("public_safe_candidate") or _PUBLIC_RE.search(blob) or str(data.get("channel_policy") or "").lower() in _PUBLIC_POLICIES or str(data.get("visibility") or "").lower() in _PUBLIC_VISIBILITIES or str(data.get("authority") or "").lower() in _PUBLIC_AUTHORITIES) and not (_REVIEW_RE.search(blob) or _SOURCE_BLIND_RE.search(blob) or _PAYMENT_RE.search(text) or _INTERNAL_RE.search(text))


def _classify(data: dict[str, Any], text: str) -> tuple[str, str]:
    blob = " ".join(str(data.get(k) or "") for k in data)
    if not text:
        return "unusable", "empty_text"
    if _PAYMENT_RE.search(blob) or _INTERNAL_RE.search(blob):
        return "private_internal", "private_or_internal_terms"
    if _SOURCE_BLIND_RE.search(blob):
        return "source_blind", "missing_public_provenance"
    if _REVIEW_RE.search(blob) or bool(data.get("review_only")):
        return "review_only", "needs_review_or_confirmation"
    if _public_ready(data, text):
        return "public_safe", "explicit_public_safe_status"
    return "source_blind", "no_public_safe_status"


def _add_public_sections(result: dict[str, Any], text: str) -> None:
    low = text.lower()
    for key in ("publicSafeFacts", "publicSafeNotes"):
        if text not in result[key] and len(result[key]) < 10:
            result[key].append(text)
    if any(w in low for w in ("artist", "member", "moderator", "producer", "dj", "role", "known for")):
        result["publicRoleSignals"].append(text)
    if any(w in low for w in ("music", "track", "song", "artist", "producer", "dj", "radio", "album")):
        result["publicCreativeMusicSignals"].append(text)
    if any(w in low for w in ("barcode", "bnl", "community", "source file", "dossier")):
        result["publicCommunitySignals"].append(text)
    if _LINK_RE.search(text):
        result["publicLinkSignals"].append(text)
    if any(w in low for w in ("relationship", "context", "barcode", "bnl", "community")):
        result["relationshipOrContextSignals"].append(text)


def _scan_table(conn: sqlite3.Connection, table: str, subject: str, aliases: list[str]) -> list[dict[str, Any]]:
    cols = _table_columns(conn, table)
    if not cols:
        return []
    text_cols = [c for c in ("safe_summary", "cleaned_summary", "summary", "fact_value", "fact_label", "note", "body", "message", "content", "text", "topic", "description", "public_summary") if c in cols]
    terms = [subject] + aliases
    key_terms = [_subject_key(t) for t in terms]
    clauses, params = [], []
    for col in ("subject_name", "entity_name", "name", "display_name"):
        if col in cols:
            clauses.extend([f"LOWER({col})=?" for _ in terms]); params.extend(t.lower() for t in terms)
    for col in ("subject_key", "entity_key", "normalized_subject"):
        if col in cols:
            clauses.extend([f"LOWER({col})=?" for _ in key_terms]); params.extend(key_terms)
    for col in text_cols:
        for term in terms:
            clauses.append(f"{col} LIKE ?"); params.append(f"%{term}%")
    where = " OR ".join(f"({c})" for c in clauses) or "1=1"
    try:
        rows = conn.execute(f"SELECT * FROM {table} WHERE {where} ORDER BY rowid DESC LIMIT 300", params).fetchall()
    except sqlite3.Error:
        return []
    out = []
    for row in rows:
        data = dict(row)
        text = next((_text(data.get(c)) for c in text_cols if _text(data.get(c))), "")
        hay = " ".join(_text(data.get(c), 120) for c in cols)
        if text and _matches(hay, terms):
            data["_resolver_text"] = text
            out.append(data)
    return out


def resolve_subject_memory(subject_name: str, db_path: str, aliases: list[str] | None = None) -> dict[str, Any]:
    subject = _text(subject_name, 120) or "Unnamed subject"
    clean_aliases = []
    for alias in aliases or []:
        if _is_probable_subject_alias(alias) and alias.lower() != subject.lower() and alias not in clean_aliases:
            clean_aliases.append(alias)
    result = _blank_result(subject, clean_aliases)
    if not db_path:
        result["sourceSafetyWarnings"].append("No local BNL memory database was available for subject memory resolution.")
        return result
    tables = ["entity_evidence_events", "entity_intelligence_facts", "broadcast_memory", "source_file_enrichments", "bnl_source_file_enrichment", "dossier_candidates", "bnl_dossier_recommendations", "dossier_recommendations", "population_recommendations", "conversation_memory", "message_memory", "messages", "memory_tiers", "user_memory_facts", "relationship_journal", "member_activity_events"]
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True); conn.row_factory = sqlite3.Row
    except sqlite3.Error:
        result["sourceSafetyWarnings"].append("Local BNL memory database could not be opened read-only.")
        return result
    try:
        for table in tables:
            if not _table_exists(conn, table):
                continue
            result["diagnostic"]["tablesScanned"].append(table)
            rows = _scan_table(conn, table, subject, clean_aliases)
            if rows:
                result["diagnostic"]["tablesContributed"][table] = len(rows)
            for data in rows:
                raw = data.get("_resolver_text") or ""
                klass, reason = _classify(data, raw)
                result["evidenceCounts"]["totalScanned"] += 1
                result["diagnostic"]["classificationReasons"][reason] = result["diagnostic"]["classificationReasons"].get(reason, 0) + 1
                if klass == "public_safe":
                    safe = _sanitize_public(raw, subject, clean_aliases)
                    if safe:
                        _add_public_sections(result, safe); result["evidenceCounts"]["publicSafe"] += 1
                elif klass == "review_only":
                    result["reviewOnlyEvidence"].append({"table": table, "summary": "Subject memory needs owner/admin confirmation before public use.", "reason": reason})
                    result["evidenceCounts"]["reviewOnly"] += 1
                    if re.search(r"queue|submission", str(raw), re.I): result["queueOrSubmissionSignals"].append("Queue or submission context needs admin confirmation before public use.")
                elif klass == "private_internal":
                    result["privateOrInternalEvidence"].append({"table": table, "summary": "Private/internal memory was excluded from public copy.", "reason": reason})
                    result["evidenceCounts"]["privateOrInternal"] += 1
                elif klass == "source_blind":
                    result["sourceSafetyWarnings"].append("Some subject memory lacked public-safe provenance and was not used as public copy.")
                    result["evidenceCounts"]["sourceBlind"] += 1
        ps = result["evidenceCounts"]["publicSafe"]
        result["confidence"] = "high" if ps >= 5 else ("medium" if ps >= 2 else "low")
        if ps == 0:
            result["missingInfoQuestions"].append("Confirm at least one public-safe subject fact before drafting rich public dossier copy.")
        if result["evidenceCounts"]["reviewOnly"] or result["evidenceCounts"]["sourceBlind"]:
            result["missingInfoQuestions"].append("Review BNL subject memory and promote confirmed public-safe facts with provenance.")
    finally:
        conn.close()
    LOG.debug("subject_memory_resolver subject=%s tables=%s publicSafe=%s reviewOnly=%s rejected=%s confidence=%s", subject, result["diagnostic"]["tablesScanned"], result["evidenceCounts"]["publicSafe"], result["evidenceCounts"]["reviewOnly"], result["evidenceCounts"]["privateOrInternal"] + result["evidenceCounts"]["sourceBlind"], result["confidence"])
    return result


def build_subject_memory_diagnostic(subject_name: str, db_path: str) -> dict[str, Any]:
    resolved = resolve_subject_memory(subject_name, db_path)
    counts = resolved["evidenceCounts"]
    return {
        "subjectName": resolved["subjectName"],
        "candidateMemoriesFound": counts["totalScanned"],
        "tablesScanned": resolved["diagnostic"]["tablesScanned"],
        "tablesContributed": resolved["diagnostic"]["tablesContributed"],
        "classificationCounts": counts,
        "classificationReasons": resolved["diagnostic"]["classificationReasons"],
        "safePublicSummary": resolved["publicSafeFacts"][:5],
        "adminConfirmationNeeded": resolved["missingInfoQuestions"][:5],
        "confidence": resolved["confidence"],
    }
