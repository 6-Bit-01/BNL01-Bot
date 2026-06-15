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
_EMAIL_RE = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.I)
_PHONE_RE = re.compile(r"(?<!\w)(?:\+?1[\s.-]?)?(?:\(?\d{3}\)?[\s.-]?)\d{3}[\s.-]?\d{4}(?!\w)")
_RAW_ID_RE = re.compile(r"\b\d{15,22}\b|\b(?:discord|user|channel|message|guild|member)[-_\s]*(?:id)?\s*[:=#]?\s*\d+\b", re.I)
_SECRET_RE = re.compile(r"\b(?:api[_-]?key|token|secret|authorization|bearer|password)\b\s*[:=]?\s*[A-Za-z0-9_./+\-=]{8,}|\b[A-Za-z0-9_-]{24,}\.[A-Za-z0-9_-]{6,}\.[A-Za-z0-9_-]{20,}\b", re.I)
_CONTACT_RE = re.compile(r"\b(?:email|e-mail|phone|telephone|contact me|contact info|address|sms|text me)\b", re.I)
_PRIVATE_ALIAS_RE = re.compile(r"\b(?:private alias|alias privately|known privately as|legal name|real name)\b", re.I)
_DB_ROW_RE = re.compile(r"\b(?:rowid|source row|database row|raw json|raw payload|table row|memory_tiers|entity_evidence_events)\b", re.I)
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
    if (_PAYMENT_RE.search(clean) or _INTERNAL_RE.search(clean) or _EMAIL_RE.search(clean) or _PHONE_RE.search(clean) or _RAW_ID_RE.search(clean) or _SECRET_RE.search(clean) or _CONTACT_RE.search(clean) or _PRIVATE_ALIAS_RE.search(clean) or _DB_ROW_RE.search(clean)):
        return ""
    return clean



def _redact_review_evidence_summary(text: str, subject: str) -> str:
    """Return a privacy-safe review/source-blind evidence summary.

    Unlike _sanitize_public, this is for non-public metadata: it may preserve
    coarse claim category (Orion/context, queue/submission, music/link) but must
    not preserve raw contact details, private aliases, IDs, URLs, tokens, payment
    details, or database-row wording.
    """
    original = _text(text, 360)
    low = original.lower()
    if not original:
        return f"Review-needed evidence exists for {subject}, but the raw detail was withheld for privacy."
    payment = bool(_PAYMENT_RE.search(original))
    unsafe = bool(
        payment
        or _EMAIL_RE.search(original)
        or _PHONE_RE.search(original)
        or _RAW_ID_RE.search(original)
        or _SECRET_RE.search(original)
        or _CONTACT_RE.search(original)
        or _INTERNAL_RE.search(original)
        or _PRIVATE_ALIAS_RE.search(original)
        or _DB_ROW_RE.search(original)
        or _LINK_RE.search(original)
    )
    if unsafe:
        if payment:
            return "Private/payment-related evidence was counted but withheld."
        if "source-blind" in low or "source blind" in low or "unknown-policy" in low or "no provenance" in low:
            if "orion" in low:
                return f"Source-blind Orion/context evidence exists for {subject}, but raw details were withheld."
            return "Source-blind context exists, but raw details were withheld."
        if "orion" in low:
            return f"{subject} has recurring Orion-related context. Raw supporting detail withheld for privacy/source safety."
        if "queue" in low or "submission" in low:
            return f"{subject} has possible queue/submission context. Raw supporting detail withheld for privacy/source safety."
        if "suno" in low or "music" in low or "link" in low or "url" in low:
            return f"{subject} has possible music/link context. Raw supporting detail withheld for privacy/source safety."
        if "relationship" in low or "relay" in low or "context" in low:
            return f"{subject} has possible relationship/context evidence. Raw supporting detail withheld for privacy/source safety."
        return "Review-needed evidence exists, but the raw detail was withheld for privacy."
    clean = original
    clean = _EMAIL_RE.sub("[redacted email]", clean)
    clean = _PHONE_RE.sub("[redacted phone]", clean)
    clean = _RAW_ID_RE.sub("[redacted id]", clean)
    clean = _SECRET_RE.sub("[redacted secret]", clean)
    clean = _LINK_RE.sub("[redacted link]", clean)
    clean = re.sub(r"\s+", " ", clean).strip()
    if _EMAIL_RE.search(clean) or _PHONE_RE.search(clean) or _RAW_ID_RE.search(clean) or _SECRET_RE.search(clean) or _LINK_RE.search(clean):
        return "Review-needed evidence exists, but the raw detail was withheld for privacy."
    return clean[:240].rstrip()


def _source_blind_warning_from_summary(summary: str, subject: str) -> str:
    """Return only category-level safe wording for source-blind warnings."""
    low = _text(summary, 360).lower()
    if "orion" in low or "relay" in low:
        return f"Source-blind Orion/context evidence exists for {subject}, but raw details were withheld."
    if "queue" in low or "submission" in low:
        return f"Source-blind queue/submission context exists for {subject}, but raw details were withheld."
    if "suno" in low or "music" in low or "song" in low or "track" in low or "link" in low or "url" in low or _LINK_RE.search(summary or ""):
        return f"Source-blind music/link context exists for {subject}, but raw details were withheld."
    if "relationship" in low or "context" in low:
        return f"Source-blind Orion/context evidence exists for {subject}, but raw details were withheld."
    if "barcode" in low or "bnl" in low or "community" in low:
        return f"Source-blind community context exists for {subject}, but raw details were withheld."
    return "Some subject memory lacked public-safe provenance and was not used as public copy."

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
    if _PAYMENT_RE.search(blob):
        return "private_internal", "private_or_internal_terms"
    if _SOURCE_BLIND_RE.search(blob):
        return "source_blind", "missing_public_provenance"
    if _INTERNAL_RE.search(blob):
        return "private_internal", "private_or_internal_terms"
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
                    safe_review = _redact_review_evidence_summary(raw, subject) or "Subject memory needs owner/admin confirmation before public use."
                    result["reviewOnlyEvidence"].append({"table": table, "summary": safe_review, "reason": reason})
                    result["evidenceCounts"]["reviewOnly"] += 1
                    if re.search(r"queue|submission", str(raw), re.I): result["queueOrSubmissionSignals"].append(safe_review if safe_review else "Queue or submission context needs admin confirmation before public use.")
                    if re.search(r"relationship|context|orion|relay|references|through", str(raw), re.I) and safe_review:
                        result["relationshipOrContextSignals"].append(safe_review)
                elif klass == "private_internal":
                    result["privateOrInternalEvidence"].append({"table": table, "summary": "Private/internal memory was excluded from public copy.", "reason": reason})
                    result["evidenceCounts"]["privateOrInternal"] += 1
                elif klass == "source_blind":
                    warning = _source_blind_warning_from_summary(raw, subject)
                    if re.search(r"relationship|context|orion|relay|references|through", str(raw), re.I):
                        result["relationshipOrContextSignals"].append(warning)
                    result["sourceSafetyWarnings"].append(warning)
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



def _dedupe_by_pattern(items: list[str], subject: str, limit: int = 9) -> list[str]:
    """Collapse repetitive subject evidence into a small public-safe ingredient set."""
    out: list[str] = []
    seen: set[str] = set()
    for item in items:
        clean = _sanitize_public(item, subject, [])
        if not clean:
            continue
        key = re.sub(re.escape(subject), "subject", clean.lower(), flags=re.I)
        key = re.sub(r"\b\d+\b", "#", key)
        key = re.sub(r"\b(public[-_\s]*safe|public|barcode|bnl|community|subject|memory|context|recurring|regularly|appears|mentioned)\b", "", key)
        words = sorted(set(re.findall(r"[a-z]{4,}", key)))[:8]
        pattern = " ".join(words) or clean.lower()[:80]
        if pattern in seen:
            continue
        seen.add(pattern)
        out.append(clean)
        if len(out) >= limit:
            break
    return out



def _claim_type_from_text(text: str) -> str:
    low = (text or "").lower()
    if any(w in low for w in ("do not", "never state", "must not")):
        return "do_not_say"
    if any(w in low for w in ("queue", "submission")):
        return "queue_submission"
    if any(w in low for w in ("suno", "music link", "social link", "http", "link")):
        return "music_link" if any(w in low for w in ("suno", "music", "song", "track")) else "public_link"
    if any(w in low for w in ("orion", "relationship", "relay", "references", "through", "context")):
        return "relationship"
    if any(w in low for w in ("artist", "participant", "member", "role", "collaborator", "moderator", "producer", "dj")):
        return "role"
    if any(w in low for w in ("barcode", "bnl", "community")):
        return "community_context"
    return "missing_confirmation"


def _claim_prefix(claim_type: str) -> str:
    return {
        "role": "Possible role claim",
        "relationship": "Possible relationship/context claim",
        "music_link": "Possible music/link claim",
        "public_link": "Possible public-link claim",
        "queue_submission": "Possible queue/submission claim",
        "community_context": "Possible community/context claim",
        "source_blind_context": "Source-blind context claim",
    }.get(claim_type, "Possible review claim")


def _claim_actionability(claim_text: str, claim_type: str, lane: str, public_safe: bool) -> str:
    low = (claim_text or "").lower()
    if "subject-owned/keyed local evidence exists" in low or "local/keyed evidence" in low:
        return "non_actionable_artifact"
    if "lacked public-safe provenance" in low or "source-blind" in low or lane == "source_blind":
        return "source_blind_warning"
    if claim_text.strip().lower() in {"contest organizer", "organizer", "participant", "artist", "member"} or "contest organizer" in low:
        return "weak_label"
    if "?" in claim_text or "confirm" in low or "can bnl" in low:
        return "missing_confirmation"
    if public_safe:
        return "actionable_claim"
    if claim_type in {"relationship", "music_link", "public_link", "queue_submission", "role", "community_context"}:
        return "actionable_claim"
    return "evidence_signal"


def _suggested_public_wording(subject: str, claim_text: str, claim_type: str, public_safe: bool) -> str:
    if not public_safe:
        return ""
    text = _sanitize_public(claim_text, subject, [])
    low = text.lower()
    if not text:
        return ""
    if claim_type == "identity":
        return subject
    if claim_type in {"role", "community_context"}:
        if "radio" in low:
            return f"{subject} is a BARCODE Radio viewer/community participant."
        if re.search(r"\b(member|community|barcode|bnl|participant)\b", low):
            return f"{subject} is a BARCODE Network community member."
    if claim_type in {"music_link", "public_link"} and _LINK_RE.search(text):
        return f"{subject} has an approved public music link: {_LINK_RE.search(text).group(0)}."
    if claim_type == "relationship" and "orion" in low:
        return f"{subject} has recurring Orion-related BARCODE lore/context."
    if claim_type == "queue_submission":
        return f"{subject} has submitted music to BARCODE Radio."
    return text if not re.search(r"\b(may|possible|unconfirmed|needs confirmation|source-blind|internal)\b", low) else ""


def _review_guidance(subject: str, claim_text: str, claim_type: str, lane: str, public_safe: bool) -> dict[str, str]:
    actionability = _claim_actionability(claim_text, claim_type, lane, public_safe)
    low = (claim_text or "").lower()
    guidance: dict[str, str] = {"actionability": actionability}
    public_wording = _suggested_public_wording(subject, claim_text, claim_type, public_safe)
    if public_wording:
        guidance.update({
            "suggestedPublicWording": public_wording,
            "recommendedAction": "approve_public",
            "recommendedActionReason": "BNL found public-safe or owner/admin-confirmed support for exact public wording.",
        })
        return guidance
    if actionability == "non_actionable_artifact":
        guidance.update({
            "recommendedAction": "reject",
            "suggestedRejectionReason": "This is a technical evidence artifact, not a reviewable Source File fact.",
            "suggestedInternalNote": f"BNL found local/keyed evidence for {subject}, but it is not specific enough to become a public or internal claim without more context.",
            "cannotSuggestPublicReason": "No concrete public-safe claim was identified.",
            "recommendedActionReason": "The item is audit metadata rather than a subject fact.",
        })
        return guidance
    if actionability == "weak_label":
        label = _text(claim_text, 80)
        if "contest organizer" in low:
            label = "contest organizer"
        guidance.update({
            "recommendedAction": "keep_internal",
            "suggestedInternalNote": f"BNL found a weak pattern label: \"{label}\". Keep internal only if useful; reject it if inaccurate.",
            "suggestedMissingInfoQuestion": f"Is the label \"{label}\" accurate and useful for {subject}, or should it be rejected?",
            "suggestedRejectionReason": "This label is too thin to approve without supporting public-safe context.",
            "cannotSuggestPublicReason": "The label is not specific enough to become public copy.",
            "recommendedActionReason": "Weak labels need supporting context before approval.",
        })
        return guidance
    if actionability == "source_blind_warning":
        guidance.update({
            "recommendedAction": "needs_public_source",
            "suggestedInternalNote": f"BNL found source-blind context for {subject}. Keep this internal until a public source or owner-approved wording is provided.",
            "suggestedMissingInfoQuestion": "What public-safe source or owner-approved wording supports this source-blind context?",
            "cannotSuggestPublicReason": "Source-blind memory cannot become public copy by itself.",
            "recommendedActionReason": "Source-blind context needs separate public-safe provenance or owner/admin confirmation.",
        })
        return guidance
    if claim_type in {"music_link", "public_link"}:
        note = f"BNL found repeated music/link context for {subject}, but public ownership/use is not confirmed. Keep this internal until owner/admin approves links or wording."
        question = f"Which {subject} links are owned or approved for public reference?"
    elif claim_type == "relationship" and "orion" in low:
        note = f"BNL found Orion-related context for {subject}. Keep this internal unless owner/admin confirms it may be referenced publicly."
        question = f"Can BNL mention Orion in public {subject} context, or should Orion stay internal?"
    elif claim_type == "queue_submission":
        note = f"BNL found possible queue/submission context for {subject}. Keep this internal unless admin confirms it may be referenced publicly."
        question = f"Can {subject}'s queue/submission history be referenced publicly, or should it stay internal?"
    elif claim_type in {"role", "community_context"}:
        note = f"BNL found possible role/community context for {subject}. Keep this internal until public wording is confirmed."
        question = f"What public role/title may BNL use for {subject}, if any?"
    else:
        note = f"BNL found review-only context for {subject}. Keep this internal until a public source or owner-approved wording is provided."
        question = "What public-safe source or owner-approved wording supports this claim?"
    guidance.update({
        "recommendedAction": "needs_more_info",
        "recommendedActionReason": "BNL cannot safely suggest public wording until the missing confirmation is resolved.",
        "suggestedInternalNote": note,
        "suggestedMissingInfoQuestion": question,
        "cannotSuggestPublicReason": "No confirmed public-safe wording or provenance was identified.",
    })
    return guidance


def _safe_evidence_example(text: str, subject: str) -> str:
    clean = _redact_review_evidence_summary(text, subject)
    clean = re.sub(r"\b(cus|sub|pi|ch|tok)_[A-Za-z0-9_\-]+\b", "[redacted token]", clean, flags=re.I)
    clean = re.sub(r"\b\d{6,}\b", "[redacted id]", clean)
    return clean or f"Redacted protected memory about {subject}."


def _make_reviewable_claim(subject: str, claim_text: str, claim_type: str, lane: str, why: str, public_safe: bool, confidence: str, evidence: str = "", blocked: list[str] | None = None) -> dict[str, Any]:
    item = {
        "claimText": _text(claim_text, 300),
        "claimType": claim_type,
        "reviewLane": lane,
        "suggestedDecision": "confirm_public" if public_safe else ("keep_boundary" if lane in {"source_blind", "private_internal_withheld"} else "needs_more_info"),
        "why": _text(why, 220),
        "publicSafe": bool(public_safe),
        "confidence": confidence,
        "safeEvidenceSummary": _text(evidence or why, 240),
        "blockedBy": blocked or ([] if public_safe else ["missing owner/admin confirmation", "missing public-safe provenance"]),
    }
    item.update(_review_guidance(subject, item["claimText"], claim_type, lane, public_safe))
    return item


def _withheld_audit(subject: str, resolved_memory: dict[str, Any], private: int, blind: int, review: int) -> dict[str, Any]:
    private_examples = [_safe_evidence_example(x.get("summary", ""), subject) for x in (resolved_memory.get("privateOrInternalEvidence") or [])[:2] if isinstance(x, dict)]
    blind_examples = [_safe_evidence_example(x, subject) for x in (resolved_memory.get("sourceSafetyWarnings") or [])[:2] if isinstance(x, str)]
    review_examples = [_safe_evidence_example(x.get("summary", ""), subject) for x in (resolved_memory.get("reviewOnlyEvidence") or [])[:2] if isinstance(x, dict)]
    cats = {
        "private_internal": {"count": private, "reason": "Private/internal, payment/customer, raw-ID, DM, database-row, or operational content is never public draft evidence.", "safeExamples": private_examples},
        "payment_customer_priority": {"count": private, "reason": "Protected payment/customer/Priority signals are counted only as withheld safety evidence.", "safeExamples": private_examples[:1]},
        "raw_ids_or_database_rows": {"count": private, "reason": "Raw IDs, tokens, and database-row style content are redacted and withheld.", "safeExamples": []},
        "source_blind": {"count": blind, "reason": "Source-blind memory can guide internal review but cannot prove public claims.", "safeExamples": blind_examples},
        "unsafe_public_copy": {"count": review, "reason": "Review-only claims may be meaningful but need confirmation before public use.", "safeExamples": review_examples},
        "duplicate_or_repetitive": {"count": 0, "reason": "Repetitive public-safe items are deduped before draft ingredients.", "safeExamples": []},
        "low_value_context": {"count": 0, "reason": "Thin context is kept out of public copy until it supports a specific claim.", "safeExamples": []},
    }
    return {"totalWithheld": private + blind + review, "categories": cats}

def build_subject_analyst_read(subject_name: str, resolved_memory: dict[str, Any], packet: dict[str, Any] | None = None) -> dict[str, Any]:
    """Synthesize resolver output into internal/read-review/public-draft buckets.

    The analyst read may reason from non-private review/source-blind counts, but
    public draft ingredients only come from sanitized public-safe resolver text.
    """
    subject = _text(subject_name, 120) or _text(resolved_memory.get("subjectName"), 120) or "Unnamed subject"
    counts = resolved_memory.get("evidenceCounts") if isinstance(resolved_memory.get("evidenceCounts"), dict) else {}
    public_items: list[str] = []
    for key in ("publicSafeFacts", "publicSafeNotes", "publicCommunitySignals", "publicCreativeMusicSignals", "publicRoleSignals", "publicLinkSignals"):
        for item in resolved_memory.get(key) or []:
            clean = _sanitize_public(str(item), subject, resolved_memory.get("matchedAliasesUsedPrivately") or [])
            if clean and clean not in public_items:
                public_items.append(clean)
    draft_ingredients = _dedupe_by_pattern(public_items, subject, limit=9)
    hay = " ".join(draft_ingredients).lower()
    has_music = bool(re.search(r"\b(music|artist|musician|track|song|album|producer|dj)\b", hay))
    has_community = bool(re.search(r"\b(barcode|bnl|community|dossier|source file|public interaction|public context|radio)\b", hay))
    if has_music:
        likely_type = "artist"
    elif has_community:
        likely_type = "community_participant"
    else:
        likely_type = "unknown"
    ps = int(counts.get("publicSafe") or 0)
    review = int(counts.get("reviewOnly") or 0)
    blind = int(counts.get("sourceBlind") or 0)
    private = int(counts.get("privateOrInternal") or 0)
    scanned = int(counts.get("totalScanned") or 0)
    confidence = "high" if len(draft_ingredients) >= 5 else ("medium" if len(draft_ingredients) >= 2 or ps >= 2 else "low")
    posture = "confident" if confidence == "high" and not (review or blind) else ("conservative" if draft_ingredients else "placeholder_only")
    if private and not draft_ingredients:
        posture = "do_not_draft_publicly_yet"
    strongest = []
    if has_community:
        strongest.append(f"Repeated public BARCODE/BNL/community context for {subject}.")
    if has_music:
        strongest.append(f"Clean public-safe music or artist wording is present for {subject}.")
    if review:
        strongest.append("Review-needed evidence suggests additional role, link, queue, or relationship context that needs confirmation.")
    if blind:
        strongest.append("Source-blind memory contributes internal pattern context but lacks public provenance.")
    if not strongest and scanned:
        strongest.append("Subject memory exists, but public-safe patterns are thin.")
    if likely_type == "artist":
        internal = f"{subject} appears to be an artist-facing BARCODE subject with public-safe music context."
    elif likely_type == "community_participant":
        internal = f"{subject} appears to be a recurring BARCODE-facing community subject with repeated public interaction/context around BNL or BARCODE."
    elif scanned:
        internal = f"{subject} has subject-memory matches, but BNL lacks enough clean public-safe evidence to make a specific public role claim."
    else:
        internal = f"{subject} has little or no usable subject-memory evidence for a public dossier read."
    if review or blind:
        internal += " Inferred role, link, queue, relationship, or source-blind claims should stay in owner/admin review rather than public copy."
    public_claims = draft_ingredients[:6]
    reviewable_claims: list[dict[str, Any]] = []
    review_claims: list[str] = []
    for txt in public_claims[:6]:
        ctype = _claim_type_from_text(txt)
        if ctype == "missing_confirmation":
            ctype = "community_context" if re.search(r"\b(barcode|bnl|community|radio)\b", txt, re.I) else "role"
        reviewable_claims.append(_make_reviewable_claim(subject, txt, ctype, "public_safe", "Public-safe subject memory supports this Source File fact.", True, confidence, _safe_evidence_example(txt, subject), []))
    review_sources: list[str] = []
    for ev in resolved_memory.get("reviewOnlyEvidence") or []:
        if isinstance(ev, dict):
            txt = _redact_review_evidence_summary(str(ev.get("summary") or ""), subject)
            if txt and txt not in review_sources:
                review_sources.append(txt)
    for item in (resolved_memory.get("queueOrSubmissionSignals") or []) + (resolved_memory.get("relationshipOrContextSignals") or []):
        txt = _redact_review_evidence_summary(str(item), subject)
        if txt and txt not in review_sources and txt not in draft_ingredients:
            review_sources.append(txt)
    if review and not review_sources:
        review_sources.append(f"{subject} may have role, link, queue/submission, music, or relationship context, but BNL needs confirmation before public use.")
    for txt in review_sources[:10]:
        ctype = _claim_type_from_text(txt)
        if ctype == "community_context" and not ps:
            ctype = "role"
        if ctype == "relationship" and "orion" in txt.lower():
            line = f"Possible relationship/context claim: {subject} references Orion as an AI or message relay context. Confirm whether this may be used publicly or should remain internal community context."
        elif ctype == "queue_submission":
            line = f"Possible queue/submission claim: {subject} may have queue/submission history, but public use needs confirmation. Evidence context: {txt}"
        elif ctype in {"music_link", "public_link"}:
            line = f"Possible music/link claim: {subject} may have public music or link context, but ownership/use needs confirmation. Evidence context: {txt}"
        elif ctype == "role":
            role_hint = "recurring BARCODE community participant" if re.search(r"barcode|community|participant|recurring", txt, re.I) else "a role or title BNL should not state publicly yet"
            line = f"Possible role claim: {subject} may be {role_hint}. Evidence context: {txt}"
        else:
            line = f"{_claim_prefix(ctype)}: {txt} Confirm the exact public-safe wording before use."
        if line not in review_claims:
            review_claims.append(line)
            reviewable_claims.append(_make_reviewable_claim(subject, line, ctype, "needs_confirmation", f"Review-only subject memory mentions {ctype.replace('_', ' ')} context.", False, "low", _safe_evidence_example(txt, subject)))
    blind_insights = []
    if blind:
        for warn in resolved_memory.get("sourceSafetyWarnings") or []:
            clean = _redact_review_evidence_summary(str(warn), subject)
            if clean and clean not in blind_insights:
                blind_insights.append(clean if clean.startswith("Source-blind") else f"Source-blind/internal context: {clean}")
    if blind and not blind_insights:
        blind_insights = ["Source-blind memory indicates possible additional subject context, but it is not public-copy provenance."]
    for txt in blind_insights[:5]:
        ctype = "source_blind_context"
        if "orion" in txt.lower() or "relay" in txt.lower():
            ctype = "relationship"
            line = f"Possible relationship/context claim: {subject} references Orion or relay context in source-blind memory. Keep internal unless admin confirms public-safe provenance."
            if line not in review_claims:
                review_claims.append(line)
        reviewable_claims.append(_make_reviewable_claim(subject, txt, ctype, "source_blind", "Context lacks public-safe provenance and must not be public copy.", False, "low", _safe_evidence_example(txt, subject), ["source-blind provenance", "missing public source"]))
    exclusions = []
    if private:
        exclusions.append(f"Excluded {private} protected/private memory item(s) from public and provenance text; see withheldEvidenceAudit category counts for safe details.")
    do_not = ["Do not state artist/music role, owned links, formal BARCODE role, queue/submission history, or relationships unless confirmed public-safe evidence supports that exact claim."]
    if private:
        do_not.append("Do not expose private, payment, customer, DM, raw ID, token, database-row, private alias, or internal operational details.")
    missing = [
        {"question": f"Confirm preferred public display name: What exact name may BNL use for {subject}?", "confirmationType": "needs_owner_confirmation", "suggestedReviewer": "owner_or_admin", "why": "Identity wording should be owner-approved before public promotion.", "canAutoResolve": False, "relatedClaimType": "identity"},
        {"question": f"Confirm public role/title: Is {subject} a community participant, artist, collaborator, or something else?", "confirmationType": "needs_admin_confirmation", "suggestedReviewer": "admin", "why": "Role wording is only public when backed by clean public evidence or admin-confirmed Source File fact.", "canAutoResolve": bool(has_community or has_music), "relatedClaimType": "role"},
        {"question": f"Confirm public links: Which {subject}/Suno/music/social links are owned or approved for public reference?", "confirmationType": "needs_owner_confirmation", "suggestedReviewer": "owner_or_admin", "why": "Link ownership/use must be explicit before public dossier use.", "canAutoResolve": False, "relatedClaimType": "music_link"},
    ]
    if any("orion" in x.lower() or "relay" in x.lower() for x in review_claims + blind_insights):
        missing.append({"question": f"Confirm Orion context: Can BNL mention Orion as {subject}'s AI/message relay context, or keep it internal?", "confirmationType": "needs_admin_confirmation", "suggestedReviewer": "admin", "why": "Named relationship/context should not become public copy without permission and provenance.", "canAutoResolve": False, "relatedClaimType": "relationship"})
    if review or re.search(r"queue|submission", " ".join(str(x) for x in resolved_memory.get("queueOrSubmissionSignals") or []), re.I):
        missing.append({"question": f"Confirm queue/submission history: Can {subject}'s queue or submission history be referenced publicly?", "confirmationType": "needs_admin_confirmation", "suggestedReviewer": "admin", "why": "Queue/submission evidence is review-only unless explicitly confirmed public-safe.", "canAutoResolve": False, "relatedClaimType": "queue_submission"})
    missing_lines = [m["question"] + f" ({m['confirmationType'].replace('_', ' ')}.)" for m in missing]
    actions = [
        "Review each sourceFileReviewClaims line as an individual approve/reject/edit item; do not approve a whole bucket.",
        "Promote only confirmed public-safe facts into the Source File before expanding public copy.",
        "Attach owner-approved provenance for any role, link, music, queue, Orion/context, or relationship claim.",
    ]
    withheld_audit = _withheld_audit(subject, resolved_memory, private, blind, review) if (private or blind or review) else {}
    source_file_ingredients = []
    for item in [internal, *strongest[:6], *public_claims, *review_claims, *blind_insights, *actions, *missing_lines]:
        clean = _text(item, 320)
        if clean and clean not in source_file_ingredients:
            source_file_ingredients.append(clean)
    prov = [f"BNL subject memory resolver reviewed {scanned} subject-memory matches and reduced them to {len(draft_ingredients)} public-safe draft ingredients."]
    if review:
        prov.append(f"BNL held {len(review_claims) or review} specific review-needed role/link/music/queue/relationship claim(s) out of public copy pending confirmation.")
    if ps > len(draft_ingredients):
        prov.append("BNL treated repetitive public context as internal pattern evidence, not public dossier prose.")
    if private or blind:
        prov.append(f"BNL withheld {private + blind} private/internal or source-unproven item(s) from public draft copy; audit categories contain only redacted summaries.")
    return {
        "subjectName": subject,
        "internalRead": internal,
        "likelySubjectType": likely_type,
        "confidence": confidence,
        "publicDraftPosture": posture,
        "strongestSignals": strongest[:6],
        "publicSafeClaims": public_claims,
        "publicReadyClaims": public_claims,
        "reviewNeededClaims": review_claims,
        "reviewableClaims": reviewable_claims[:14],
        "sourceBlindInsights": blind_insights,
        "privateOrInternalExclusions": exclusions,
        "withheldEvidenceAudit": withheld_audit,
        "doNotSayPublicly": do_not,
        "missingInfoQuestions": missing_lines,
        "missingConfirmations": missing,
        "recommendedAdminActions": actions,
        "draftIngredients": draft_ingredients,
        "sourceFileIngredients": source_file_ingredients[:18],
        "provenanceSummary": prov,
    }


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
