"""BNL Proposed Dossier draft generator v1.

Pure helpers for the internal site-to-BNL draft endpoint. These functions do not
write Discord, memory, Source Files, candidates, or website state.
"""
from __future__ import annotations

import hmac
import logging
import os
import re
import sqlite3
from typing import Any

from bnl_subject_memory_resolver import build_subject_analyst_read, resolve_subject_memory

DRAFT_ENDPOINT_PATH = "/internal/dossiers/draft"
DRAFT_TOKEN_HEADER = "X-BNL-DOSSIER-DRAFT-TOKEN"
DRAFT_TOKEN_ENV = "BNL_DOSSIER_DRAFT_GENERATOR_TOKEN"
REQUEST_TYPE = "bnl_proposed_dossier_draft"
VERSION = "1.0"

WEAK_EVIDENCE_FALLBACK_SUMMARY = "{name} is a BARCODE Network dossier subject with public-facing details still being confirmed."
WEAK_EVIDENCE_FALLBACK_NOTES = "Public-facing context is limited; keep wording concise and confirmation-based."

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
_PUBLIC_NOTE_META_RE = re.compile(
    r"\b("
    r"owner\s+review|admin(?:-only|\s+only)?|review-only|review\s+only|"
    r"must\s+not\s+be\s+copied\s+into\s+public\s+text|not\s+public|"
    r"source[-_\s]*blind|missing[-_\s]*info|missing\s+info|"
    r"needs?\s+review\s+before\s+claiming|"
    r"preferred\s+display\s+name|public\s+link|role\s+confirmation|owner\s+approval|"
    r"dossier\s+blueprint\s+readiness|boundaries|what\s+not\s+to\s+claim|"
    r"internal\s+diagnostic|diagnostic\s+wording"
    r")\b",
    re.I,
)
_SOURCE_USAGE_FORBIDDEN_RE = re.compile(r"\b(private|source[-_\s]*blind|review-only|review\s+only|internal|admin-only|admin\s+only|payment|priority|stripe|checkout|customer)\b", re.I)
_NON_PUBLIC_COPY_RE = re.compile(
    r"\b("
    r"recurring\s+named\s+topic|may\s+inform\s+public[-\s]*safe\s+wording|topic\s+bucket|named\s+topic|"
    r"generic\s+evidence\s+counts?|source[-\s]*blind|review[-\s]*only|admin[-\s]*only|"
    r"clean\s+public\s+summary\s+needed|clean\s+role\s+needed|needs?\s+review\s+before\s+claiming|"
    r"admin[-\s]*only\s+evidence\s+exists|placeholder[-\s]*like"
    r")\b",
    re.I,
)
_ROLE_LIMIT = 80
_SUMMARY_LIMIT = 700
_NOTES_LIMIT = 900
_PUBLIC_EVIDENCE_LIMIT = 8
_PUBLIC_POLICIES = {"public_home", "public_context", "public_selective", "broadcast_memory", "public"}
_PUBLIC_VISIBILITIES = {"public", "public_safe", "dossier_safe", "public_candidate", "public_use"}
_PUBLIC_AUTHORITIES = {"public", "public_safe", "dossier_safe", "broadcast_memory", "public_conversation", "owner_confirmed", "official_public_dossier", "public_discord_observed", "queue_submission_confirmed"}
_ROLE_OR_TAXONOMY_LABELS = {
    "artist", "member", "community", "collaborator", "mod", "moderator", "personnel", "staff",
    "sponsor", "system", "interface", "production", "radio", "producer", "ai", "human", "hybrid",
    "unknown", "public", "internal", "restricted", "active", "pending", "person", "music",
    "tech", "systems", "broadcast", "participant", "dossier", "source", "source file",
}
VALID_DOSSIER_CATEGORIES = {"Entity", "Personnel", "Artist", "Collaborator", "Community", "Sponsor", "Interface", "Production"}
VALID_DOSSIER_KINDS = {
    "program", "interface", "platform", "system", "entity", "artist", "sponsor_character",
    "story_arc", "technical_component", "archive_record", "core_entity", "network_operator",
    "network_staff", "moderator", "collaborator", "community_member", "radio_regular",
    "radio_entity", "unknown",
}
VALID_DOSSIER_ECOSYSTEM_LANES = {
    "core_team", "network_operator", "network_staff", "community_mod", "radio_support",
    "technical_operator", "artist", "collaborator", "community_member", "radio_regular",
    "sponsor", "radio_entity", "infrastructure", "production", "external_platform", "unknown",
}
_VALID_CLASSIFICATION_VALUES = {
    "category": VALID_DOSSIER_CATEGORIES,
    "kind": VALID_DOSSIER_KINDS,
    "ecosystemLane": VALID_DOSSIER_ECOSYSTEM_LANES,
}
_CLASSIFICATION_DEFAULTS = {"category": "Entity", "kind": "entity", "ecosystemLane": "unknown"}
_PUBLIC_FIELD_FORBIDDEN_RE = re.compile(
    r"\b("
    r"internal|source\s+file|debug|source/debug|internal/source|review\w*|review-only|source-blind|"
    r"admin-only|owner\s+review|listed\s+for\s+review|no\s+public-safe\s+material|page\s+plan|draft\s+generator|"
    r"resolver|diagnostics?|validation|unsupported\s+claims?|rejected\s+material"
    r")\b",
    re.I,
)

_SITE_PUBLIC_COPY_JUNK_RE = re.compile(
    r"(?:"
    r"starter\s+(?:evidence\s+)?note(?:\s+only)?\s*:|public\s+safety\s*:|missing\s+info\s*:|"
    r"broadcast_memory|do\s+not\s+expose\s+private\s+discord\s+identity|"
    r"verify\s+public-safe\s+wording|internal\s+discovery\s+classification|"
    r"bnl\s+discovery\s+is\s+review-only|review\s+before\s+converting|"
    r"medium-confidence\s+bnl\s+discovery|source[-\s]*file|dossier\s+seed|"
    r"add\s+a\s+dossier\s+entry|user_profiles/|conversations/|relationship_journal|"
    r"memory_tiers|local_profile_observed|public_discord_observed|local_relationship_trace|"
    r"source\s+lane\s+mapping|source\s+lanes?|source\s+types?|ingest\s*key|ingestKey|"
    r"candidateId|targetId|recommendationId|workflow\s+record|unknown\s*->\s*unknown|"
    r"dossier_(?:candidate|recommendation)_[a-z0-9_\-]+|source_file_note_[a-z0-9_\-]+|"
    r"(?:candidate|source|recommendation|target)[-_\s]*id\b"
    r")",
    re.I,
)
_BACKEND_COPY_TERMS_RE = re.compile(
    r"\b(?:source|candidate|recommendation|target|workflow|ingest|memory|tier|lane|"
    r"backend|internal|review|debug|diagnostic|metadata|packet|resolver|classification|"
    r"dossier|source_file|source-file|public_safe|broadcast_memory)\b",
    re.I,
)
_SOURCE_LABEL_RE = re.compile(r"(?:^|[\n;|])\s*(?:source|source\s+file|source\s+lane|evidence)\s*[:#-]", re.I)
_GENERIC_SOURCE_COUNT_SUMMARY_RE = re.compile(
    r"(?:"
    r"mentions\s+without\s+link\s+claims|without\s+link\s+claims|"
    r"(?:music\s+)?discussion\s+only|feedback\s+requests?|"
    r"song\s*/\s*track\s*/\s*demo\s*/\s*wip|"
    r"source[-\s]*(?:count|counts|stat|stats|statistics|summary)|"
    r"count[-\s]*(?:summary|summaries)|topic\s+buckets?"
    r")",
    re.I,
)
_COUNT_SUMMARY_CHAIN_RE = re.compile(
    r"(?:\b[A-Za-z][A-Za-z0-9'’/ -]{1,48}:\s*\d+\b[;,.]?\s*){3,}",
    re.I,
)


def _generic_source_count_summary_reason(value: Any) -> str | None:
    clean = _text(value, 2000)
    if not clean:
        return None
    if _GENERIC_SOURCE_COUNT_SUMMARY_RE.search(clean) or _COUNT_SUMMARY_CHAIN_RE.search(clean):
        return "generic_source_count_summary"
    return None


def _has_repeated_public_lines(text: str) -> bool:
    lines = [re.sub(r"\s+", " ", line).strip().lower() for line in str(text).splitlines()]
    lines = [line for line in lines if len(line) >= 12]
    return len(lines) != len(set(lines))


def contains_dossier_public_copy_junk(value: Any) -> bool:
    """Mirror the website's public-copy junk guard for fields returned publicly."""
    clean = _text(value, 2000)
    if not clean:
        return False
    if _PUBLIC_FIELD_FORBIDDEN_RE.search(clean) or _SITE_PUBLIC_COPY_JUNK_RE.search(clean):
        return True
    if _generic_source_count_summary_reason(clean):
        return True
    if len(_SOURCE_LABEL_RE.findall(clean)) >= 2:
        return True
    if _has_repeated_public_lines(str(value)):
        return True
    words = re.findall(r"\b[\w'-]+\b", clean)
    if len(words) >= 6:
        backend_hits = len(_BACKEND_COPY_TERMS_RE.findall(clean))
        if backend_hits >= 4 and backend_hits / max(len(words), 1) >= 0.28:
            return True
    return False


def _sanitize_public_field(value: str, fallback: str) -> tuple[str, bool, str | None]:
    clean = _text(value, 1000)
    safe_fallback = _text(fallback, 1000)
    if contains_dossier_public_copy_junk(safe_fallback):
        safe_fallback = WEAK_EVIDENCE_FALLBACK_NOTES
    if not clean:
        return safe_fallback, False, None
    generic_reason = _generic_source_count_summary_reason(clean)
    if contains_dossier_public_copy_junk(clean):
        return safe_fallback, True, generic_reason or "site_rejected_source_process_copy"
    return clean, False, None


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
    if _NON_PUBLIC_COPY_RE.search(text):
        reasons.append("topic-only or review-only material")
    if _generic_source_count_summary_reason(text):
        reasons.append("generic source/count summary")
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



def _is_public_dossier_note_text(text: str) -> bool:
    clean = _text(text, 500)
    if not clean:
        return False
    if _unsafe_reasons(clean) or _PUBLIC_NOTE_META_RE.search(clean):
        return False
    return True


def _public_note_rejection_reason(text: str) -> str | None:
    clean = _text(text, 500)
    if not clean:
        return None
    reasons = _unsafe_reasons(clean)
    if _PUBLIC_NOTE_META_RE.search(clean):
        reasons.append("review/admin/missing-info note text")
    if reasons:
        return f"Moved {', '.join(dict.fromkeys(reasons))} out of public notes metadata."
    return None



def _source_usage_line(count: int, singular: str, plural: str) -> str:
    return f"Used {count} {singular if count == 1 else plural}."

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


def _identity_label_kind(label: str) -> str:
    clean = _text(label, 100)
    low = clean.lower().strip()
    if not clean:
        return "empty"
    if low in _ROLE_OR_TAXONOMY_LABELS:
        return "role_label"
    if len(clean) < 3 or len(clean) > 80:
        return "role_label"
    words = re.findall(r"[A-Za-z0-9]+", clean)
    if not words:
        return "role_label"
    if len(words) == 1 and clean.islower() and not any(ch.isdigit() for ch in clean):
        return "role_label"
    return "subject_alias"


def _is_probable_subject_alias(label: str) -> bool:
    return _identity_label_kind(label) == "subject_alias"


def _subject_terms(packet: dict[str, Any]) -> tuple[str, list[str]]:
    candidate = _dict(packet.get("candidate"))
    name = _candidate_name(candidate)
    labels = _strings(_dict(packet.get("identityAliasStatus")).get("publicSafeIdentityLabels"), limit_each=100, max_items=8)
    labels += _strings(packet.get("publicSafeIdentityLabels"), limit_each=100, max_items=8)
    current = _dict(packet.get("currentDraft"))
    labels += _strings(current.get("name"), limit_each=100, max_items=1)
    aliases: list[str] = []
    for label in labels:
        if label and label.lower() != name.lower() and label not in aliases and not _unsafe_reasons(label) and _is_probable_subject_alias(label):
            aliases.append(label)
    return name, aliases[:8]


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}
    except sqlite3.Error:
        return set()


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    try:
        row = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
        return bool(row)
    except sqlite3.Error:
        return False


def _matches_subject(text: str, terms: list[str]) -> bool:
    low = (text or "").lower()
    return any(term and term.lower() in low for term in terms)


def _subject_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", (value or "").lower()).strip("_")


def _redact_private_match_terms(text: str, private_terms: list[str], public_subject_name: str) -> str:
    clean = _text(text, 500)
    replacement = _text(public_subject_name, 120) or "the subject"
    for term in sorted({t for t in private_terms if t}, key=len, reverse=True):
        if term.lower() == replacement.lower():
            continue
        clean = re.sub(re.escape(term), replacement, clean, flags=re.I)
    return clean


def _sql_subject_filter(cols: set[str], text_cols: list[str], name: str, aliases: list[str]) -> tuple[str, list[Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    key_terms = [_subject_key(name)] + [_subject_key(alias) for alias in aliases if alias]
    exact_terms = [name] + aliases
    if "subject_key" in cols:
        placeholders = ",".join("?" for _ in key_terms)
        clauses.append(f"LOWER(subject_key) IN ({placeholders})")
        params.extend(key_terms)
    if "subject_name" in cols:
        placeholders = ",".join("?" for _ in exact_terms)
        clauses.append(f"LOWER(subject_name) IN ({placeholders})")
        params.extend(term.lower() for term in exact_terms)
    for col in text_cols:
        if col in cols:
            for term in exact_terms:
                clauses.append(f"{col} LIKE ?")
                params.append(f"%{term}%")
    return (" OR ".join(f"({clause})" for clause in clauses), params)


def _fetch_subject_rows(
    conn: sqlite3.Connection,
    table: str,
    *,
    cols: set[str],
    text_cols: list[str],
    name: str,
    aliases: list[str],
    order_expr: str = "rowid DESC",
    limit: int = 250,
) -> list[sqlite3.Row]:
    where, params = _sql_subject_filter(cols, text_cols, name, aliases)
    if where:
        return conn.execute(f"SELECT * FROM {table} WHERE {where} ORDER BY {order_expr} LIMIT ?", [*params, limit]).fetchall()
    return conn.execute(f"SELECT * FROM {table} ORDER BY {order_expr} LIMIT ?", (limit,)).fetchall()


def _add_bundle_item(bundle: dict[str, Any], key: str, value: str, *, max_items: int = _PUBLIC_EVIDENCE_LIMIT) -> None:
    clean = _text(value, 260)
    if not clean or _unsafe_reasons(clean):
        return
    items = bundle.setdefault(key, [])
    if clean not in items and len(items) < max_items:
        items.append(clean)


def _classify_public_evidence(bundle: dict[str, Any], text: str) -> None:
    _add_bundle_item(bundle, "publicFacts", text)
    low = text.lower()
    if any(x in low for x in ("artist", "music", "track", "song", "radio", "show", "producer", "dj", "beat")):
        _add_bundle_item(bundle, "publicCreativeMusicContext", text)
    if any(x in low for x in ("barcode", "bnl", "dossier", "source file", "community")):
        _add_bundle_item(bundle, "publicCommunityContext", text)
    if any(x in low for x in ("bnl", "barcode", "community", "interact", "asked", "helps", "collaborat")):
        _add_bundle_item(bundle, "publicRelationshipToBarcode", text)
    if any(x in low for x in ("repeat", "recurring", "often", "regular", "again", "pattern")):
        _add_bundle_item(bundle, "publicInteractionPatterns", text)
    if any(x in low for x in ("role", "represents", "known for", "moderator", "artist", "collaborator", "member")):
        _add_bundle_item(bundle, "publicRoleSignals", text)
    _add_bundle_item(bundle, "notablePublicSignals", text, max_items=5)


def _style_guidance_used(style_packet: dict[str, Any]) -> list[str]:
    fields = (
        "representativePublicDossierExamples",
        "categorySpecificExamples",
        "goodRoleLineExamples",
        "goodSummaryExamples",
        "goodNotesExamples",
        "authoringGuideSummary",
        "taxonomyGuide",
        "tagRegistryGuidance",
    )
    used = [field for field in fields if style_packet.get(field)]
    if not used:
        return []
    return [f"Used site public dossier style guidance for structure, tone, length, taxonomy, and tag style only ({', '.join(used[:8])})."]


def _read_model_public_dossier_items(read_model: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not isinstance(read_model, dict):
        return []
    sections = read_model.get("sections") if isinstance(read_model.get("sections"), dict) else {}
    dossiers_section = sections.get("dossiers") if sections.get("dossiers") is not None else read_model.get("dossiers")
    candidates: list[Any] = []
    if isinstance(dossiers_section, dict):
        for key in ("items", "public", "dossiers", "publicDossiers"):
            if isinstance(dossiers_section.get(key), list):
                candidates.extend(dossiers_section[key])
    elif isinstance(dossiers_section, list):
        candidates.extend(dossiers_section)
    for key in ("publicDossiers", "dossiers"):
        if isinstance(read_model.get(key), list):
            candidates.extend(read_model[key])
    out: list[dict[str, Any]] = []
    for item in candidates:
        if not isinstance(item, dict):
            continue
        visibility = str(item.get("visibility") or item.get("clearance") or item.get("status") or item.get("publicationStatus") or "public").lower()
        if any(term in visibility for term in ("draft", "private", "owner", "internal", "restricted")):
            continue
        if item not in out:
            out.append(item)
    return out


def _dossier_public_texts(dossier: dict[str, Any]) -> list[str]:
    bnl_context = _dict(dossier.get("bnlContext"))
    texts = _strings(
        [
            dossier.get("role"),
            dossier.get("summary") or dossier.get("description") or dossier.get("publicSummary"),
            dossier.get("notes") or bnl_context.get("notes"),
        ],
        max_items=5,
    )
    texts += _strings(dossier.get("publicFacts"), max_items=6)
    return [text for text in texts if not _unsafe_reasons(text)]



def _public_dossier_style_context(packet: dict[str, Any], read_model: dict[str, Any] | None) -> dict[str, Any]:
    """Summarize current public dossier corpus for house-style matching.

    Defensive input contract: callers may pass bounded style/context in the packet
    (`stylePacket.publicDossierStyleContext`, `currentPublicDossierContext`) or in
    the public read model. Only already-public items are consumed; draft/private/
    owner-only/internal items are ignored.
    """
    style_packet = _dict(packet.get("stylePacket"))
    inline: list[Any] = []
    for key in ("publicDossierStyleContext", "currentPublicDossierContext", "publicDossierCorpus", "representativePublicDossierExamples", "categorySpecificExamples"):
        value = style_packet.get(key) if key in style_packet else packet.get(key)
        if isinstance(value, list):
            inline.extend(value)
        elif isinstance(value, dict):
            inline.append(value)
            for inner in ("items", "examples", "dossiers", "patterns"):
                if isinstance(value.get(inner), list):
                    inline.extend(value.get(inner) or [])
    items = [x for x in inline if isinstance(x, dict)] + _read_model_public_dossier_items(read_model)
    clean: list[dict[str, Any]] = []
    for item in items:
        visibility = str(item.get("visibility") or item.get("clearance") or item.get("status") or item.get("publicationStatus") or "public").lower()
        if any(term in visibility for term in ("draft", "private", "owner", "internal", "restricted")):
            continue
        if item not in clean:
            clean.append(item)
        if len(clean) >= 10:
            break
    role_hay = " ".join(_strings(packet.get("publicSafeFacts"), max_items=8) + _strings(packet.get("publicSafeNotes"), max_items=4)).lower()
    if any(t in role_hay for t in ("artist", "music", "track", "song", "producer", "dj")):
        nearest = "artist/music public dossier pattern"
    elif any(t in role_hay for t in ("collaborator", "broadcast", "radio", "production")):
        nearest = "collaborator/production public dossier pattern"
    else:
        nearest = "community/member public dossier pattern"
    notes: list[str] = []
    for item in clean:
        for key in ("styleNotes", "toneNotes", "structureNotes", "pattern", "type", "role"):
            notes.extend(_strings(item.get(key), max_items=2))
    return {
        "available": bool(clean),
        "count": len(clean),
        "nearestPattern": nearest,
        "styleNotes": notes[:6] or _style_guidance_used(style_packet),
        "warning": "No current public dossier corpus/style context was supplied; site should pass bounded public dossier context for stronger house-style matching." if not clean else "",
    }

def _matching_public_dossiers(packet: dict[str, Any], read_model: dict[str, Any] | None) -> list[dict[str, Any]]:
    name, aliases = _subject_terms(packet)
    candidate = _dict(packet.get("candidate"))
    source_file_id = _text(candidate.get("sourceFileId"), 160)
    terms = {name.lower(), _subject_key(name)}
    terms.update(alias.lower() for alias in aliases)
    terms.update(_subject_key(alias) for alias in aliases)
    if source_file_id:
        terms.add(source_file_id.lower())
    matches: list[dict[str, Any]] = []
    packet_contexts = []
    for key in ("officialPublicDossierContext", "currentPublicDossierContext", "matchingPublicDossier", "publicDossierContext"):
        value = packet.get(key)
        if isinstance(value, dict):
            packet_contexts.append(value)
        elif isinstance(value, list):
            packet_contexts.extend(item for item in value if isinstance(item, dict))
    for dossier in packet_contexts + _read_model_public_dossier_items(read_model):
        fields = [
            dossier.get("name"),
            dossier.get("title"),
            dossier.get("slug"),
            dossier.get("id"),
            dossier.get("publicId"),
            dossier.get("sourceFileId"),
            dossier.get("subjectName"),
        ]
        keys = {str(field).strip().lower() for field in fields if field}
        keys.update(_subject_key(str(field)) for field in fields if field)
        if terms & keys:
            matches.append(dossier)
    return matches[:3]


def build_public_dossier_draft_evidence(packet: dict[str, Any], db_path: str | None, public_read_model: dict[str, Any] | None = None) -> dict[str, Any]:
    """Build a temporary public-safe evidence bundle for draft authoring.

    This is read-only and intentionally narrow: the Source File packet provides
    subject/boundary/classification, while only explicitly public/dossier-safe
    local lanes can add draft evidence.
    """
    name, aliases = _subject_terms(packet)
    terms = [name] + aliases
    style_context = _public_dossier_style_context(packet, public_read_model)
    bundle: dict[str, Any] = {
        "subjectName": name,
        "matchedAliasesUsedPrivately": [],
        "publicFacts": [],
        "publicRoleSignals": [],
        "publicInteractionPatterns": [],
        "publicCommunityContext": [],
        "publicCreativeMusicContext": [],
        "publicRelationshipToBarcode": [],
        "recurringPublicTopics": [],
        "notablePublicSignals": [],
        "officialPublicDossierContext": [],
        "publicDossierStyleGuidanceUsed": _style_guidance_used(_dict(packet.get("stylePacket"))),
        "publicDossierContextWarnings": [],
        "publicDossierStyleContext": style_context,
        "sourceSummariesUsed": [],
        "excludedSourceWarnings": [],
        "missingInfoQuestions": [],
        "confidence": "low",
        "thinReasons": [],
    }
    for fact in _strings(packet.get("publicSafeFacts"), max_items=8):
        _classify_public_evidence(bundle, fact)
    for note in _strings(packet.get("publicSafeNotes"), max_items=6):
        if _is_public_dossier_note_text(note):
            _classify_public_evidence(bundle, note)
        else:
            reason = _public_note_rejection_reason(note)
            if reason:
                bundle["excludedSourceWarnings"].append(reason)
    matching_dossiers = _matching_public_dossiers(packet, public_read_model)
    if matching_dossiers:
        used = 0
        for dossier in matching_dossiers:
            dossier_haystack = " ".join(
                _text(dossier.get(key), 180)
                for key in ("name", "title", "slug", "id", "publicId", "sourceFileId", "subjectName", "role", "summary", "description", "publicSummary")
            )
            matched_aliases = [alias for alias in aliases if _matches_subject(dossier_haystack, [alias])]
            for text in _dossier_public_texts(dossier):
                text = _redact_private_match_terms(text, matched_aliases, name)
                _add_bundle_item(bundle, "officialPublicDossierContext", text, max_items=8)
                _classify_public_evidence(bundle, text)
                used += 1
            for alias in matched_aliases:
                if alias not in bundle["matchedAliasesUsedPrivately"]:
                    bundle["matchedAliasesUsedPrivately"].append(alias)
        if used:
            bundle["sourceSummariesUsed"].append(f"Used matching current public dossier context as official public dossier authority for {name}.")
    else:
        bundle["publicDossierContextWarnings"].append("No matching current public dossier/read-model facts were available as a direct official source; style examples were used only for structure and tone.")
    if style_context.get("available"):
        bundle["sourceSummariesUsed"].append(f"Compared draft against {style_context.get('count')} current public dossier style/context item(s); closest house pattern: {style_context.get('nearestPattern')}.")
    elif style_context.get("warning"):
        bundle["publicDossierContextWarnings"].append(style_context.get("warning"))
    if not db_path:
        bundle["excludedSourceWarnings"].append("Local BNL evidence database was not available; used the packet boundary only.")
    else:
        try:
            conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        except sqlite3.Error:
            bundle["excludedSourceWarnings"].append("Local BNL evidence database could not be opened read-only; used the packet boundary only.")
            conn = None
        if conn is not None:
            conn.row_factory = sqlite3.Row
            try:
                if _table_exists(conn, "entity_evidence_events"):
                    ecols = _table_columns(conn, "entity_evidence_events")
                    if {"updated_at", "created_at"} <= ecols:
                        order_expr = "COALESCE(updated_at, created_at, '') DESC"
                    elif "updated_at" in ecols:
                        order_expr = "updated_at DESC"
                    elif "created_at" in ecols:
                        order_expr = "created_at DESC"
                    else:
                        order_expr = "rowid DESC"
                    rows = _fetch_subject_rows(
                        conn,
                        "entity_evidence_events",
                        cols=ecols,
                        text_cols=["safe_summary", "topic"],
                        name=name,
                        aliases=aliases,
                        order_expr=order_expr,
                        limit=250,
                    )
                    used = 0
                    excluded = 0
                    for row in rows:
                        data = dict(row)
                        hay = " ".join(str(data.get(c) or "") for c in ("subject_name", "safe_summary", "topic", "relation_to_subject"))
                        matched = [t for t in terms[1:] if _matches_subject(hay, [t])]
                        if not _matches_subject(hay, terms):
                            continue
                        policy = str(data.get("channel_policy") or "").lower()
                        visibility = str(data.get("visibility") or "").lower()
                        authority = str(data.get("authority") or "").lower()
                        safe_flag = bool(data.get("public_safe_candidate"))
                        review_only = bool(data.get("review_only"))
                        if review_only or not (safe_flag or policy in _PUBLIC_POLICIES or visibility in _PUBLIC_VISIBILITIES or authority in _PUBLIC_AUTHORITIES):
                            excluded += 1
                            continue
                        text = _redact_private_match_terms(_text(data.get("safe_summary"), 260), matched, name)
                        for alias in matched:
                            if alias not in bundle["matchedAliasesUsedPrivately"]:
                                bundle["matchedAliasesUsedPrivately"].append(alias)
                        if text and not _unsafe_reasons(text):
                            _classify_public_evidence(bundle, text)
                            topic = _text(data.get("topic"), 120)
                            if topic and not _unsafe_reasons(topic):
                                _add_bundle_item(bundle, "recurringPublicTopics", topic, max_items=6)
                            used += 1
                    if used:
                        bundle["sourceSummariesUsed"].append(_source_usage_line(used, "public-safe structured entity evidence summary", "public-safe structured entity evidence summaries"))
                    if excluded:
                        bundle["excludedSourceWarnings"].append("Excluded review-only or non-public structured entity evidence.")
                if _table_exists(conn, "entity_intelligence_facts"):
                    cols = _table_columns(conn, "entity_intelligence_facts")
                    order_col = "last_seen_at" if "last_seen_at" in cols else ("updated_at" if "updated_at" in cols else "rowid")
                    rows = _fetch_subject_rows(
                        conn,
                        "entity_intelligence_facts",
                        cols=cols,
                        text_cols=["fact_value", "fact_label"],
                        name=name,
                        aliases=aliases,
                        order_expr=f"{order_col} DESC",
                        limit=250,
                    )
                    used = 0
                    excluded = 0
                    wanted_keys = {_subject_key(name), *(_subject_key(alias) for alias in aliases)}
                    wanted_names = {name.lower(), *(alias.lower() for alias in aliases)}
                    for row in rows:
                        data = dict(row)
                        row_subject_key = _subject_key(str(data.get("subject_key") or data.get("subject_name") or ""))
                        subject_name = str(data.get("subject_name") or "")
                        if row_subject_key not in wanted_keys and subject_name.lower() not in wanted_names:
                            continue
                        status = str(data.get("status") or "active").lower()
                        visibility = str(data.get("visibility") or "").lower()
                        authority = str(data.get("authority") or "").lower()
                        public_safe = bool(data.get("public_safe"))
                        review_only = bool(data.get("review_only"))
                        if status != "active" or not public_safe or review_only or visibility not in _PUBLIC_VISIBILITIES or authority not in _PUBLIC_AUTHORITIES:
                            excluded += 1
                            continue
                        hay = " ".join(str(data.get(c) or "") for c in ("subject_name", "fact_value", "fact_label"))
                        matched = [t for t in terms[1:] if _matches_subject(hay, [t])]
                        text = _redact_private_match_terms(_text(data.get("fact_value") or data.get("fact_label"), 260), matched, name)
                        for alias in matched:
                            if alias not in bundle["matchedAliasesUsedPrivately"]:
                                bundle["matchedAliasesUsedPrivately"].append(alias)
                        if text and not _unsafe_reasons(text):
                            _classify_public_evidence(bundle, text)
                            used += 1
                    if used:
                        bundle["sourceSummariesUsed"].append(_source_usage_line(used, "public-safe entity intelligence fact", "public-safe entity intelligence facts"))
                    if excluded:
                        bundle["excludedSourceWarnings"].append("Excluded private, review-only, inactive, or non-public entity intelligence facts.")
                if _table_exists(conn, "broadcast_memory"):
                    bcols = _table_columns(conn, "broadcast_memory")
                    rows = _fetch_subject_rows(
                        conn,
                        "broadcast_memory",
                        cols=bcols,
                        text_cols=["cleaned_summary", "summary", "raw_note"],
                        name=name,
                        aliases=aliases,
                        order_expr="rowid DESC",
                        limit=250,
                    )
                    used = 0
                    excluded = 0
                    for row in rows:
                        data = dict(row)
                        raw_text = _text(data.get("cleaned_summary") or data.get("summary") or data.get("raw_note"), 260)
                        matched = [t for t in terms[1:] if _matches_subject(raw_text, [t])]
                        text = _redact_private_match_terms(raw_text, matched, name)
                        if not _matches_subject(text, terms):
                            continue
                        for alias in matched:
                            if alias not in bundle["matchedAliasesUsedPrivately"]:
                                bundle["matchedAliasesUsedPrivately"].append(alias)
                        status = str(data.get("status") or "active").lower()
                        scope = str(data.get("usage_scope") or data.get("visibility") or "").lower()
                        has_scope = "usage_scope" in bcols or "visibility" in bcols
                        if not bool(data.get("public_safe")) or status != "active" or (has_scope and scope not in {"public", "public_safe", "broadcast_memory", "dossier_safe"}):
                            excluded += 1
                            continue
                        if text and not _unsafe_reasons(text):
                            _classify_public_evidence(bundle, text)
                            used += 1
                    if used:
                        bundle["sourceSummariesUsed"].append(_source_usage_line(used, "active public-safe broadcast memory summary", "active public-safe broadcast memory summaries"))
                    if excluded:
                        bundle["excludedSourceWarnings"].append("Excluded inactive, internal, or non-public broadcast memory.")
                for unsafe in ("memory_tiers", "user_memory_facts", "relationship_journal", "member_activity_events"):
                    if _table_exists(conn, unsafe):
                        bundle["excludedSourceWarnings"].append(f"Excluded {unsafe} because it is private, source-blind, or review-only for this draft.")
            finally:
                conn.close()
    total = len(bundle["publicFacts"])
    if total >= 5:
        bundle["confidence"] = "high"
    elif total >= 2:
        bundle["confidence"] = "medium"
    else:
        bundle["thinReasons"].append("Fewer than two public-safe evidence items were available after filtering.")
        bundle["missingInfoQuestions"].append("Add approved public-safe subject evidence before treating this as a rich dossier.")
    return bundle


def _classification_value(safe_classification: dict[str, Any], key: str, default: str | None = None) -> str:
    fallback = default or _CLASSIFICATION_DEFAULTS[key]
    value = _text(safe_classification.get(key), 120) or fallback
    allowed = _VALID_CLASSIFICATION_VALUES.get(key)
    if allowed and value not in allowed:
        return fallback
    return value


def _conservative_classification(packet: dict[str, Any], owner_warnings: list[str] | None = None) -> dict[str, str]:
    safe = _dict(packet.get("safeClassification"))
    source = _dict(packet.get("sourceFileClassificationV1"))
    out = {
        key: _classification_value(safe, key)
        for key in ("category", "kind", "ecosystemLane")
    }
    source_out = {
        key: _classification_value(source, key)
        for key in ("category", "kind", "ecosystemLane")
    }
    for key in ("category", "kind", "ecosystemLane"):
        raw_safe = _text(safe.get(key), 120)
        raw_source = _text(source.get(key), 120)
        fallback = _CLASSIFICATION_DEFAULTS[key]
        if owner_warnings is not None and raw_safe and raw_safe != out[key]:
            owner_warnings.append(f"Invalid safeClassification {key} value was treated as {fallback} for site taxonomy compatibility.")
        if owner_warnings is not None and raw_source and raw_source != source_out[key]:
            owner_warnings.append(f"Invalid sourceFileClassificationV1 {key} value was treated as {fallback} for site taxonomy compatibility.")
        if raw_source and source_out[key] == fallback and raw_source not in _VALID_CLASSIFICATION_VALUES[key]:
            if raw_safe and raw_source != raw_safe and owner_warnings is not None:
                owner_warnings.append(f"Source File classification conflicts with safeClassification for {key}; used the conservative site-compatible fallback.")
            out[key] = fallback
        elif raw_source and out[key] != fallback and source_out[key] != out[key]:
            out[key] = fallback
            if owner_warnings is not None:
                owner_warnings.append(f"Source File classification conflicts with safeClassification for {key}; used the conservative site-compatible fallback.")
    return out


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
        _classification_value(safe_classification, "category"),
        _classification_value(safe_classification, "kind"),
        _classification_value(safe_classification, "ecosystemLane"),
    )


def _role_from_context(facts: list[str], notes: list[str], category: str, kind: str, lane: str) -> str:
    haystack = " ".join(facts + notes + [category, kind, lane]).lower()
    evidence_haystack = " ".join(facts + notes).lower()
    classification_haystack = " ".join([category, kind, lane]).lower()
    if any(term in haystack for term in ("moderator", "mod", "personnel", "staff")):
        return "Community moderator"
    if "sponsor" in haystack:
        return "Sponsor record"
    if any(term in haystack for term in ("interface", "system", "tool", "tech")):
        return "Systems interface"
    if any(term in haystack for term in ("artist", "music", "musician", "track", "album", "song", "dj")):
        if any(term in evidence_haystack for term in ("artist", "music", "musician", "track", "album", "song", "dj")) or any(term in classification_haystack for term in ("artist", "music")):
            return "Music artist"
    if any(term in haystack for term in ("producer", "production", "broadcast", "radio")):
        if any(term in evidence_haystack for term in ("producer", "production", "broadcast", "radio")) or any(term in classification_haystack for term in ("producer", "production", "broadcast", "radio")):
            return "Production collaborator"
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
        summary = WEAK_EVIDENCE_FALLBACK_SUMMARY.format(name=name)
    if _word_count(summary) > 85:
        words = summary.split()
        summary = " ".join(words[:80]).rstrip(" ,;:") + "."
    if guide["length"] and _word_count(summary) < 25 and source_sentences:
        summary = f"{summary} Public-facing context stays concise and confirmation-based."
    return summary[:_SUMMARY_LIMIT].strip()


def _notes(public_notes: list[str], facts: list[str]) -> str:
    clean_notes = [note for note in public_notes if _is_public_dossier_note_text(note)]
    note_sentences = [s for s in _sentences(clean_notes, max_count=2) if _is_public_dossier_note_text(s)]
    if note_sentences:
        notes = " ".join(note_sentences[:2])
    elif len(facts) < 2:
        notes = WEAK_EVIDENCE_FALLBACK_NOTES
    else:
        notes = WEAK_EVIDENCE_FALLBACK_NOTES
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
        (("collaborat",), "collaborator"),
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
    source_classification = _dict(packet.get("sourceFileClassificationV1"))
    blocked_classification_tags = {
        tag.lower().replace(" ", "-")
        for tag in _strings(source_classification.get("internalTags"), limit_each=80, max_items=20)
        + _strings(source_classification.get("rejectedTagCandidates"), limit_each=80, max_items=20)
        + _strings(source_classification.get("doNotPubliclyTagAs"), limit_each=80, max_items=20)
    }
    blocked_classification_tags.update(
        _text(item.get("tag"), 80).lower().replace(" ", "-")
        for item in _as_list(source_classification.get("blockedPublicTags"))
        if isinstance(item, dict)
    )
    supported_tag_text = haystack
    for tag in _strings(packet.get("proposedTags"), limit_each=60, max_items=10):
        normalized = tag.lower().replace(" ", "-")
        if normalized in blocked_classification_tags:
            continue
        if normalized in {"artist", "community", "member", "mod", "moderator"} and normalized not in supported_tag_text:
            continue
        if not _unsafe_reasons(normalized) and normalized not in candidates:
            candidates.append(normalized)
    for tag in _strings(source_classification.get("publicSafeTagCandidates"), limit_each=60, max_items=10):
        normalized = tag.lower().replace(" ", "-")
        if normalized in blocked_classification_tags:
            continue
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
    parts = ["Used supplied public-facing facts, notes, site-compatible classification, and site style guidance as draft boundary context."]
    return " ".join(part for part in parts if not _SOURCE_USAGE_FORBIDDEN_RE.search(part))


def _clean_public_field(value: str, fallback: str) -> str:
    return _sanitize_public_field(value, fallback)[0]


def generate_dossier_draft(packet: dict[str, Any], db_path: str | None = None, public_read_model: dict[str, Any] | None = None) -> dict[str, Any]:
    candidate = _dict(packet.get("candidate"))
    safe_classification = _dict(packet.get("safeClassification"))
    style_packet = _dict(packet.get("stylePacket"))
    name = _candidate_name(candidate)
    classification_warnings: list[str] = []
    conservative_classification = _conservative_classification(packet, classification_warnings)
    category, kind, lane = _category_kind_lane(conservative_classification)
    public_facts, rejected_facts = _reject_unsafe_public(_strings(packet.get("publicSafeFacts"), max_items=16))
    raw_public_notes = _strings(packet.get("publicSafeNotes"), max_items=12)
    public_notes, rejected_notes = _reject_unsafe_public(raw_public_notes)
    clean_public_notes: list[str] = []
    for note in public_notes:
        reason = _public_note_rejection_reason(note)
        if reason:
            rejected_notes.append(reason)
        elif note not in clean_public_notes:
            clean_public_notes.append(note)
    public_notes = clean_public_notes
    evidence: dict[str, Any] | None = None
    resolver: dict[str, Any] | None = None
    analyst: dict[str, Any] | None = None
    try:
        evidence = build_public_dossier_draft_evidence(packet, db_path, public_read_model=public_read_model)
        evidence_texts: list[str] = []
        for key in (
            "publicFacts",
            "publicRoleSignals",
            "publicInteractionPatterns",
            "publicCommunityContext",
            "publicCreativeMusicContext",
            "publicRelationshipToBarcode",
            "notablePublicSignals",
            "officialPublicDossierContext",
        ):
            evidence_texts.extend(_strings(evidence.get(key), max_items=8))
        evidence_safe, evidence_rejected = _reject_unsafe_public(evidence_texts)
        for item in evidence_safe:
            if item not in public_facts:
                public_facts.append(item)
        rejected_facts.extend(evidence_rejected)
    except Exception:
        evidence = None
    supplied_analyst = _dict(packet.get("subjectAnalystReadV1") or packet.get("subjectAnalystRead") or packet.get("analystRead"))
    if supplied_analyst and not analyst:
        analyst = supplied_analyst
    if db_path:
        try:
            _, resolver_aliases = _subject_terms(packet)
            resolver = resolve_subject_memory(name, db_path, aliases=resolver_aliases)
            analyst = build_subject_analyst_read(name, resolver, packet)
            resolver_texts = _strings(analyst.get("draftIngredients"), max_items=9) + _strings(analyst.get("publicSafeClaims"), max_items=6)
            resolver_safe, resolver_rejected = _reject_unsafe_public(resolver_texts)
            for item in resolver_safe:
                if item not in public_facts:
                    public_facts.append(item)
            rejected_facts.extend(resolver_rejected)
        except Exception:
            resolver = None
    review_actionability = _dict(packet.get("reviewActionabilityV1") or (analyst or {}).get("reviewActionabilityV1"))
    subject_dossier_state = _dict(packet.get("subjectDossierStateV1") or (analyst or {}).get("subjectDossierStateV1"))
    source_file_surface = _dict(packet.get("sourceFileSurfaceV1") or (analyst or {}).get("sourceFileSurfaceV1"))
    source_file_page_plan = _dict(packet.get("sourceFilePagePlanV1") or (analyst or {}).get("sourceFilePagePlanV1"))
    page_plan_draft_plan = _dict(source_file_page_plan.get("draftOrUpdatePlan")) if source_file_page_plan else {}
    page_plan_public_material_added = False
    if source_file_page_plan:
        public_facts = []
        public_notes = []
        page_plan_public_candidates = _strings(page_plan_draft_plan.get("useTheseMaterials"), max_items=8)
        for material in source_file_page_plan.get("publicSafeMaterial") or []:
            if not isinstance(material, dict):
                continue
            confidence = str(material.get("confidence") or "").lower()
            text = _text(material.get("material"), 260)
            if text and confidence != "none" and not re.search(r"\b(no public-safe|not ready|not enough public-safe)\b", text, re.I):
                page_plan_public_candidates.append(text)
        for item in page_plan_public_candidates:
            safe_items, unsafe_items = _reject_unsafe_public([item])
            for safe in safe_items:
                if safe not in public_facts:
                    public_facts.append(safe)
                    page_plan_public_material_added = True
            rejected_facts.extend(unsafe_items)
        for item in _strings(page_plan_draft_plan.get("omitTheseMaterials"), max_items=10):
            rejected_facts.append(f"Source File page plan says to omit from public draft: {item}")
        internal_hold = _dict(source_file_page_plan.get("internalOmitHold"))
        for bucket in ("keepInternal", "omitFromPublicDraft"):
            for hold in internal_hold.get(bucket) or []:
                if isinstance(hold, dict):
                    title = _text(hold.get("material") or hold.get("whyInternal") or hold.get("whyOmitted"), 220)
                    if title:
                        rejected_facts.append(f"Source File page plan withheld {bucket}: {title}")
    if review_actionability:
        rejected_review_texts: list[str] = []
        for item in review_actionability.get("items") or []:
            if not isinstance(item, dict):
                continue
            action = str(item.get("actionability") or "")
            public_use = str(item.get("publicUse") or "")
            text = _text(item.get("claimText") or item.get("question") or item.get("recommendedAction"), 260)
            if action in {"keep_internal", "omit_from_public", "diagnostic_only", "already_resolved"} or public_use in {"not_public", "internal_only", "omit"}:
                if text:
                    rejected_facts.append(text)
                continue
            if action in {"required_to_finish", "true_blocker", "needs_owner_wording", "needs_admin_confirmation"}:
                if text:
                    rejected_review_texts.append(text)
        if rejected_review_texts:
            rejected_facts.extend(rejected_review_texts[:6])
    if subject_dossier_state:
        for item in _strings(subject_dossier_state.get("publicSafeToUseNow"), max_items=5):
            safe_items, unsafe_items = _reject_unsafe_public([item])
            for safe in safe_items:
                if not source_file_page_plan and safe not in public_facts:
                    public_facts.append(safe)
            rejected_facts.extend(unsafe_items)
        for item in _strings(subject_dossier_state.get("keepInternal"), max_items=4) + _strings(subject_dossier_state.get("omitForNow"), max_items=4):
            if item:
                rejected_facts.append(item)
    role = _role_from_context(public_facts, public_notes, category, kind, lane)[:_ROLE_LIMIT]
    if analyst:
        analyst_public = " ".join(_strings(analyst.get("draftIngredients"), max_items=9) + _strings(analyst.get("publicSafeClaims"), max_items=6)).lower()
        if not source_file_page_plan and re.search(r"\b(music|artist|musician|track|song|album|producer|dj)\b", analyst_public):
            role = "Music artist"
        elif analyst.get("likelySubjectType") == "community_participant" and analyst.get("publicDraftPosture") in {"confident", "conservative"}:
            role = "Community participant"
        elif analyst.get("confidence") == "low" and role == "Review pending":
            role = "Dossier subject"
    if source_file_page_plan and not page_plan_public_material_added:
        category, kind, lane = _CLASSIFICATION_DEFAULTS["category"], _CLASSIFICATION_DEFAULTS["kind"], _CLASSIFICATION_DEFAULTS["ecosystemLane"]
        public_facts = []
        public_notes = []
        role = "Dossier subject"
    elif role == "Review pending":
        role = "Dossier subject"
    thin = len(public_facts) + len(public_notes) < 2 and not page_plan_public_material_added

    missing = _strings(packet.get("missingInfo"), max_items=8)
    if evidence:
        for item in _strings(evidence.get("missingInfoQuestions"), max_items=4):
            if item not in missing:
                missing.append(item)
    if resolver:
        for item in _strings(resolver.get("missingInfoQuestions"), max_items=4):
            if item not in missing:
                missing.append(item)
    if analyst:
        for item in _strings(analyst.get("missingInfoQuestions"), max_items=5):
            if item not in missing:
                missing.append(item)
        if analyst.get("readyForDraft") is True:
            blocked_sections = {str(x).lower() for x in _strings(analyst.get("dossierBlockedBy"), max_items=5)}
            if blocked_sections:
                missing = [
                    item for item in missing
                    if any(section in item.lower() for section in blocked_sections)
                ][:3]
            else:
                missing = [
                    item for item in missing
                    if not re.search(r"\b(orion|lore|source[-_ ]blind|internal)\b", item, re.I)
                ][:5]
    if thin:
        missing.insert(0, "Add more public-safe facts before this draft is treated as rich dossier copy.")
    if _dict(packet.get("identityAliasStatus")).get("needsConfirmation") is not False:
        missing.append("Confirm preferred public display name and what identity authority, if any, may be stated publicly.")

    owner_warnings = _strings(packet.get("ownerReviewRules"), max_items=8) + _strings(packet.get("reviewOnlyWarnings"), max_items=8)
    owner_warnings.extend(classification_warnings)
    owner_warnings.append("Owner Review must approve identity, role, category, links, and public wording before publication.")
    if source_file_page_plan:
        for item in _strings(page_plan_draft_plan.get("ownerReviewWarnings"), max_items=8):
            owner_warnings.append(f"Source File page plan warning: {item}")
        if page_plan_draft_plan.get("canDraft") is False:
            missing.append("Source File page plan says drafting is not recommended yet.")
    if subject_dossier_state:
        owner_warnings.append(f"Subject dossier state: {subject_dossier_state.get('state')} / {subject_dossier_state.get('recommendedNextAction')}.")
    if source_file_surface:
        primary = _dict(source_file_surface.get("primaryAction"))
        owner_warnings.append(f"Source File surface primary action: {primary.get('label') or primary.get('actionKey')}.")
    if source_file_page_plan:
        worth = _dict(source_file_page_plan.get("dossierWorthItDecision"))
        owner_warnings.append(f"Source File page plan decision: {worth.get('decision') or 'not supplied'} / readiness {worth.get('draftReadiness') or 'unknown'}.")
    public_warnings = _strings(packet.get("sourceBoundaryRules"), max_items=8)
    public_warnings.append("Public fields use the Source File packet as the subject boundary plus approved public-safe BNL evidence.")
    if evidence:
        for item in _strings(evidence.get("excludedSourceWarnings"), max_items=5):
            owner_warnings.append(item)
        for item in _strings(evidence.get("publicDossierContextWarnings"), max_items=3):
            owner_warnings.append(item)
        for item in _strings(evidence.get("thinReasons"), max_items=3):
            owner_warnings.append(item)
    if resolver:
        counts = _dict(resolver.get("evidenceCounts"))
        if counts:
            owner_warnings.append(
                f"BNL subject memory resolver scanned {int(counts.get('totalScanned') or 0)} candidate memories: "
                f"{int(counts.get('publicSafe') or 0)} public-safe, {int(counts.get('reviewOnly') or 0)} review-needed, "
                f"{int(counts.get('privateOrInternal') or 0) + int(counts.get('sourceBlind') or 0)} withheld."
            )
        for item in _strings(resolver.get("sourceSafetyWarnings"), max_items=3):
            owner_warnings.append(item)
    if analyst:
        completion = _dict(analyst.get("dossierCompletionReadV1"))
        if completion:
            owner_warnings.append(f"Dossier-completion read: {completion.get('completionStatus')} / {completion.get('likelyDossierAngle')} using {completion.get('nearestPublicDossierPattern')}.")
        if _text(analyst.get("internalRead"), 500):
            owner_warnings.append(f"BNL internal subject read: {_text(analyst.get('internalRead'), 460)}")
        for item in _strings(analyst.get("recommendedAdminActions"), max_items=3):
            owner_warnings.append(f"BNL analyst recommended action: {item}")
        for item in _strings(analyst.get("doNotSayPublicly"), max_items=3):
            public_warnings.append(item)

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
    if evidence and evidence.get("matchedAliasesUsedPrivately"):
        rejected.append("Used approved identity labels only as private matching hints; did not expose alias text in public fields.")
    if resolver and resolver.get("matchedAliasesUsedPrivately"):
        rejected.append("Used probable subject aliases only as private matching hints for BNL memory resolution; did not expose alias text in public fields.")
    if resolver and _dict(resolver.get("evidenceCounts")).get("reviewOnly"):
        rejected.append("BNL memory needing review was used only for owner-review metadata, not public dossier copy.")
    if resolver and _dict(resolver.get("evidenceCounts")).get("sourceBlind"):
        rejected.append("BNL memory without public-safe provenance was counted but not used as public dossier copy.")
    if analyst:
        for item in _strings(analyst.get("reviewNeededClaims"), max_items=4):
            rejected.append(item)
        for item in _strings(analyst.get("sourceBlindInsights"), max_items=2):
            rejected.append(item)
    style_context = _dict(evidence.get("publicDossierStyleContext")) if evidence else {}
    if evidence and evidence.get("publicDossierStyleGuidanceUsed"):
        rejected.append("Used public dossier examples for style and field shape only; unrelated example facts were not copied.")
    if style_context.get("available"):
        rejected.append(f"Matched current public dossier house style via {style_context.get('nearestPattern')}; did not copy existing dossier text in bulk.")

    tags, proposed_tags = _split_tags(packet, style_packet, category, kind, lane, public_facts, public_notes)

    summary_raw = _summary(name, role, public_facts, public_notes, style_packet)
    public_summary, summary_sanitized, summary_sanitize_reason = _sanitize_public_field(
        summary_raw,
        WEAK_EVIDENCE_FALLBACK_SUMMARY.format(name=name),
    )
    notes_raw = _notes(public_notes, public_facts)
    public_notes_text, notes_sanitized, notes_sanitize_reason = _sanitize_public_field(
        notes_raw,
        WEAK_EVIDENCE_FALLBACK_NOTES,
    )
    public_role, role_sanitized, role_sanitize_reason = _sanitize_public_field(role, "Dossier subject")
    public_role = public_role[:_ROLE_LIMIT]
    source_usage_raw = " ".join(
        part
        for part in [
            _source_usage_summary(packet),
            *(
                item
                for item in (_strings(evidence.get("sourceSummariesUsed"), max_items=4) if evidence else [])
                if not (_SOURCE_USAGE_FORBIDDEN_RE.search(item) or _PUBLIC_FIELD_FORBIDDEN_RE.search(item))
            ),
            *(
                item
                for item in (_strings(analyst.get("provenanceSummary"), max_items=3) if analyst else [])
                if not (_SOURCE_USAGE_FORBIDDEN_RE.search(item) or _PUBLIC_FIELD_FORBIDDEN_RE.search(item))
            ),
        ]
        if part
    )
    public_source_usage, source_usage_sanitized, source_usage_sanitize_reason = _sanitize_public_field(
        source_usage_raw,
        "Draft used supplied public-facing facts, site-compatible classification, and style guidance.",
    )
    original_tags, original_proposed_tags = list(tags), list(proposed_tags)
    tags = [tag for tag in tags if not contains_dossier_public_copy_junk(tag)]
    proposed_tags = [tag for tag in proposed_tags if not contains_dossier_public_copy_junk(tag)]
    tags_sanitized = len(tags) != len(original_tags) or len(proposed_tags) != len(original_proposed_tags)
    if summary_sanitized or role_sanitized or notes_sanitized or source_usage_sanitized or tags_sanitized:
        warning = "BNL sanitized public dossier field copy because it contained site-rejected source/process copy."
        owner_warnings.append(warning)
        rejected.append(warning)
    if tags_sanitized:
        rejected.append("Removed public tag candidates containing site-rejected source/process copy.")
    sanitize_reasons = [
        reason
        for reason in (summary_sanitize_reason, role_sanitize_reason, notes_sanitize_reason, source_usage_sanitize_reason)
        if reason
    ]
    sanitize_reason = "generic_source_count_summary" if "generic_source_count_summary" in sanitize_reasons else (sanitize_reasons[0] if sanitize_reasons else "none")
    logging.info(
        "dossier_draft_public_copy_guard subject=%s category=%s kind=%s ecosystemLane=%s summary_sanitized=%s role_sanitized=%s notes_sanitized=%s sourceUsageSummary_sanitized=%s reason=%s",
        re.sub(r"[^A-Za-z0-9 ._-]", "", name)[:80] or "Unknown",
        category,
        kind,
        lane,
        summary_sanitized,
        role_sanitized,
        notes_sanitized,
        source_usage_sanitized,
        sanitize_reason,
    )

    draft = {
        "name": name,
        "category": category,
        "kind": kind,
        "ecosystemLane": lane,
        "identityAuthority": _identity_authority(packet, safe_classification),
        "status": "PENDING",
        "clearance": _clearance(packet),
        "origin": _origin(packet),
        "role": public_role,
        "summary": public_summary,
        "notes": public_notes_text,
        "tags": tags,
        "proposedTags": proposed_tags,
        "primaryLink": _first_link(packet),
        "links": [],
        "files": [],
        "missingInfoQuestions": missing[:10],
        "ownerReviewWarnings": owner_warnings[:12],
        "publicSafetyWarnings": public_warnings[:12],
        "unsupportedClaimsRejected": rejected[:14],
        "sourceUsageSummary": public_source_usage,
    }
    readiness = {}
    if analyst:
        readiness = {
            "dossierReadinessQuestions": analyst.get("dossierReadinessQuestions") or [],
            "dossierReadinessSummary": _text(analyst.get("dossierReadinessSummary"), 500),
            "dossierBlockedBy": _strings(analyst.get("dossierBlockedBy"), max_items=7),
            "readyForDraft": bool(analyst.get("readyForDraft")),
            "draftReadinessReason": _text(analyst.get("draftReadinessReason"), 500),
        }
    return {"draft": draft, "draftReadiness": readiness}
