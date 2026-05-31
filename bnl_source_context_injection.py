"""Conservative Source File context injection helpers for operator-only answers.

This module builds compact prompt context from the existing protected Source File
read client. It does not mutate Source Files, dossiers, recommendations, memory,
or website state.
"""

from __future__ import annotations

import os
import re
from typing import Any, Callable

MAX_SOURCE_CONTEXT_SUBJECTS = 2
_SOURCE_CONTEXT_DEFAULT_ENABLED = "true"

_CONTEXTUAL_VERBS_RE = re.compile(
    r"\b(?:what\s+do\s+we\s+know|what(?:'|’)?s\s+missing|summari[sz]e|review|next\s+with|do\s+next|connected\s+to|source\s+file|case\s+file|dossier)\b",
    re.IGNORECASE,
)
_STOP_TRAILING_RE = re.compile(
    r"\s+(?:source\s+file|case\s+file|dossier|for\s+internal\s+review|internal\s+review|please|pls|now|today)\b.*$",
    re.IGNORECASE,
)
_BAD_SUBJECTS = {
    "a", "an", "and", "any", "anything", "case", "connected", "dossier", "file", "info",
    "internal", "it", "me", "missing", "next", "review", "source", "that", "the", "them",
    "there", "this", "us", "we", "what", "who", "you",
}

_last_status = "idle"
_last_subjects: list[str] = []
_last_found_count = 0
_last_match_kinds: list[str] = []
_last_error_status = "none"


def is_source_context_injection_enabled(environ: dict[str, str] | None = None) -> bool:
    env = environ if environ is not None else os.environ
    return (env.get("BNL_SOURCE_CONTEXT_INJECTION_ENABLED", _SOURCE_CONTEXT_DEFAULT_ENABLED) or "").strip().lower() not in {"false", "0", "off", "no"}


def _compact(value: Any, limit: int = 180) -> str:
    if value is None:
        return ""
    text = re.sub(r"\s+", " ", str(value)).strip().replace("`", "'")
    if not text:
        return ""
    return text if len(text) <= limit else text[: max(1, limit - 1)].rstrip() + "…"


def _clean_subject(raw: str) -> str:
    text = re.sub(r"<@!?\d+>", " ", raw or "")
    text = re.sub(r"\s+", " ", text).strip(" \t\r\n'\"“”‘’.,!?;:()[]{}")
    text = _STOP_TRAILING_RE.sub("", text).strip(" \t\r\n'\"“”‘’.,!?;:()[]{}")
    text = re.sub(r"^(?:the|a|an|summari[sz]e)\s+", "", text, flags=re.IGNORECASE).strip()
    text = re.sub(r"(?:'|’)?s$", "", text).strip()
    # Stop before request continuations that are not part of names.
    text = re.split(r"\b(?:if|and\s+what|but|because|with\s+the)\b", text, maxsplit=1, flags=re.IGNORECASE)[0].strip(" ,")
    return _compact(text, 80)


def _subject_like(subject: str) -> bool:
    text = (subject or "").strip()
    if not (2 <= len(text) <= 80):
        return False
    if text.lower() in _BAD_SUBJECTS:
        return False
    if re.search(r"https?://|@everyone|@here|\n", text, re.IGNORECASE):
        return False
    tokens = re.findall(r"[A-Za-z0-9][A-Za-z0-9'_-]*", text)
    if not tokens or len(tokens) > 5:
        return False
    if all(tok.lower() in _BAD_SUBJECTS for tok in tokens):
        return False
    # Conservative v1: quoted names may be arbitrary; unquoted names should look like entity/title wording.
    titleish = any(tok[:1].isupper() or re.search(r"\d", tok) for tok in tokens)
    multiword = len(tokens) >= 2 and not all(tok.islower() for tok in tokens)
    return bool(titleish or multiword)


def _add_subject(subjects: list[str], raw: str) -> None:
    subject = _clean_subject(raw)
    if not _subject_like(subject):
        return
    key = subject.casefold()
    if key not in {s.casefold() for s in subjects}:
        subjects.append(subject)


def parse_source_context_subjects(message_text: str) -> list[str]:
    """Extract up to a small number of likely Source File subjects from an operator prompt.

    This intentionally recognizes direct case-file/source-file wording only; it is
    not broad named-entity extraction and should reject casual conversation.
    """

    text = re.sub(r"<@!?\d+>", " ", message_text or "")
    text = re.sub(r"\s+", " ", text).strip()
    if not text or text.startswith("!bnl "):
        return []
    if not _CONTEXTUAL_VERBS_RE.search(text):
        return []

    subjects: list[str] = []

    for quoted in re.findall(r"[\"“‘']([^\"”’']{2,80})[\"”’']", text):
        _add_subject(subjects, quoted)

    patterns = [
        r"\bwhat\s+do\s+we\s+know\s+about\s+(.+?)(?:[?.!]|$)",
        r"\bwhat(?:'|’)?s\s+missing\s+from\s+(.+?)(?:[?.!]|$)",
        r"\bsummari[sz]e\s+(.+?\s+(?:source\s+file|case\s+file|dossier))(?:[?.!]|$)",
        r"\bwhat\s+should\s+we\s+do\s+next\s+with\s+(.+?)(?:[?.!]|$)",
        r"\b(?:about|for)\s+(.+?)\s+(?:source\s+file|case\s+file|dossier)\b",
        r"\b([A-Z][A-Za-z0-9'_-]*(?:\s+[A-Z][A-Za-z0-9'_-]*){0,4})\s+(?:source\s+file|case\s+file|dossier)\b",
    ]
    for pattern in patterns:
        for match in re.finditer(pattern, text, flags=re.IGNORECASE):
            _add_subject(subjects, match.group(1))

    for match in re.finditer(
        r"\bis\s+(.+?)\s+connected\s+to\s+(.+?)(?:[?.!]|$)", text, flags=re.IGNORECASE
    ):
        _add_subject(subjects, match.group(1))
        _add_subject(subjects, match.group(2))

    return subjects


def should_inject_source_context(
    *,
    channel_policy: str,
    channel_name: str = "",
    privileged: bool = False,
    direct_interaction: bool = False,
    route: str = "direct",
    environ: dict[str, str] | None = None,
) -> bool:
    """Return True only for privileged direct/operator routes in approved internal contexts."""

    if not is_source_context_injection_enabled(environ):
        return False
    if not privileged or not direct_interaction:
        return False
    policy = (channel_policy or "unknown").strip().lower()
    name = re.sub(r"[^a-z0-9-]+", "-", (channel_name or "").strip().lower()).strip("-")
    if policy in {"public_home", "public_context", "public_selective", "protected_system", "broadcast_memory", "reference_canon", "ai_image_tool", "unknown"}:
        return False
    if name in {"welcome", "episode-tracker", "bnl-broadcast-memory"}:
        return False
    if route in {"website_relay", "ambient", "passive", "queue", "payment", "dm", "broadcast_memory_intake"}:
        return False
    if policy == "sealed_test" or name == "bnl-testing":
        return True
    if policy == "internal_controlled" and name == "research-and-development":
        return True
    return False


def _lookup_path(data: dict[str, Any], paths: list[tuple[str, ...]]) -> Any:
    for path in paths:
        cur: Any = data
        for key in path:
            if not isinstance(cur, dict) or key not in cur:
                cur = None
                break
            cur = cur.get(key)
        if cur not in (None, "", [], {}):
            return cur
    return None


def _source_file_object(data: dict[str, Any]) -> dict[str, Any]:
    for key in ("sourceFile", "source_file", "file", "candidate", "result"):
        value = data.get(key)
        if isinstance(value, dict):
            return value
    return data if isinstance(data, dict) else {}


def _list_text(value: Any, *, limit: int = 4, item_limit: int = 130) -> str:
    if value is None:
        return ""
    if isinstance(value, dict):
        iterable = value.values()
    elif isinstance(value, (list, tuple, set)):
        iterable = value
    else:
        iterable = [value]
    parts: list[str] = []
    for item in iterable:
        if isinstance(item, dict):
            item = item.get("summary") or item.get("text") or item.get("note") or item.get("name") or item.get("label") or item.get("reason") or item.get("value") or item.get("status")
        safe = _compact(item, item_limit)
        if safe:
            parts.append(safe)
        if len(parts) >= limit:
            break
    return "; ".join(parts)


def _append_section(lines: list[str], label: str, value: Any, fallback: str = "") -> None:
    text = _list_text(value) if isinstance(value, (list, tuple, set, dict)) else _compact(value, 220)
    if not text:
        text = fallback
    if text:
        lines.append(f"{label}: {text}")


def _alias_review(source_file: dict[str, Any], data: dict[str, Any], match_kind: str, requested: str) -> str:
    aliases = _lookup_path(source_file, [("aliases",), ("identityLinks",), ("identity_links",)])
    matched = _lookup_path(data, [("matchedAlias",), ("alias",), ("match", "alias")]) or requested
    if "alias" in (match_kind or "").lower():
        if "confirm" in (match_kind or "").lower():
            return f"Requested subject matched confirmed alias '{_compact(matched, 70)}'. Treat as internal routing/context only; do not present as a public identity claim unless the Source File explicitly says public-safe."
        return f"Requested subject appears to be an alias-style match ('{_compact(matched, 70)}'); keep identity/connection language review-only."
    if isinstance(aliases, dict):
        proposed = len(aliases.get("proposed") or aliases.get("proposedAliases") or [])
        confirmed = len(aliases.get("confirmed") or aliases.get("confirmedAliases") or [])
        return f"Aliases on file: {confirmed} confirmed, {proposed} proposed; proposed aliases remain review-only."
    if aliases:
        return _list_text(aliases, limit=3, item_limit=70)
    return "No confirmed alias instruction surfaced in the compact read result."


def build_source_context_block(result: dict[str, Any], original_subject: str = "") -> str:
    """Build a bounded internal prompt block from one Source File lookup result."""

    requested = _compact(original_subject or (result.get("query") or {}).get("lookupValue") or "that subject", 90)
    if not result.get("ok"):
        return (
            "SOURCE FILE / INTERNAL CASE FILE CONTEXT:\n"
            f"Subject requested: {requested}\n"
            f"Lookup status: unavailable ({_compact(result.get('kind') or result.get('error') or 'error', 120)})\n"
            "Boundary: Do not hallucinate a case file. Say the Source File read is unavailable and keep any answer uncertain."
        )

    data = result.get("data") if isinstance(result.get("data"), dict) else {}
    match_kind = _compact(result.get("matchKind") or data.get("matchKind") or data.get("match_kind") or "none", 70) or "none"
    if not result.get("found"):
        possible = data.get("possibleMatches") or data.get("possible_matches") or data.get("matches")
        possible_text = _list_text(possible, limit=3, item_limit=80)
        lines = [
            "SOURCE FILE / INTERNAL CASE FILE CONTEXT:",
            f"Subject requested: {requested}",
            "Match kind: none",
            "Case file status: no Source File found by the protected read endpoint.",
        ]
        if possible_text:
            lines.append(f"Possible matches: {possible_text} (review-only; not confirmed)")
        lines.append("Boundary: Do not hallucinate a case file. Say BNL does not see a Source File yet and suggest creating/reviewing one if relevant.")
        return "\n".join(lines)

    source_file = _source_file_object(data)
    subject = _lookup_path(source_file, [("name",), ("sourceFileName",), ("source_file_name",), ("subject",), ("displayName",), ("title",), ("normalizedName",)]) or requested
    lines = [
        "SOURCE FILE / INTERNAL CASE FILE CONTEXT:",
        f"Subject: {_compact(subject, 90)}",
        f"Requested subject: {requested}",
        f"Match kind: {match_kind}",
        "Case file status: internal working case file, not public dossier",
    ]
    _append_section(lines, "Known facts", _lookup_path(source_file, [("knownFacts",), ("facts",), ("evidenceSummary",), ("summary",), ("reason",)]), "not surfaced in compact read result")
    _append_section(lines, "Warnings", _lookup_path(source_file, [("warnings",), ("safetyWarnings",), ("sourceWarnings",), ("duplicateWarnings",), ("duplicateWarning",)]), "none surfaced")
    _append_section(lines, "Public-safety notes", _lookup_path(source_file, [("publicSafetyNotes",), ("safetyNotes",), ("publicUseNotes",), ("safety",)]), "internal-only until reviewed for public use")
    _append_section(lines, "Missing info", _lookup_path(source_file, [("missingInfo",), ("missing",), ("openQuestions",)]), "not surfaced in compact read result")
    _append_section(lines, "Identity/alias review", _alias_review(source_file, data, match_kind, requested))
    _append_section(lines, "Review-only/internal notes", _lookup_path(source_file, [("reviewNotes",), ("internalNotes",), ("ownerReview",), ("ownerReviewState",), ("draft",), ("activeDraft",)]), "do not frame review-only notes as public-safe facts")
    _append_section(lines, "Recommended next action", _lookup_path(source_file, [("nextRecommendedAction",), ("suggestedAction",), ("nextAction",)]), "review source notes before drafting or public use")
    lines.append("Boundary: Use this as internal case-file context. Do not present review-only, source-blind, or internal-only material as confirmed public fact. Do not call this a public dossier unless public dossier status is explicitly present.")
    return "\n".join(lines[:16])


def inject_source_context_into_prompt(base_prompt: str, context_block: str) -> str:
    if not context_block:
        return base_prompt or ""
    return f"{base_prompt or ''}\n\n{context_block}\n"


def record_source_context_attempt(subjects: list[str] | None, results: list[dict[str, Any]] | None = None, *, status: str = "idle", error_status: str = "none") -> None:
    global _last_status, _last_subjects, _last_found_count, _last_match_kinds, _last_error_status
    _last_status = _compact(status, 80) or "idle"
    _last_subjects = [_compact(s, 80) for s in (subjects or []) if _compact(s, 80)][:MAX_SOURCE_CONTEXT_SUBJECTS + 1]
    found = 0
    kinds: list[str] = []
    for result in results or []:
        if result.get("found"):
            found += 1
        kind = _compact(result.get("matchKind") or ((result.get("data") or {}) if isinstance(result.get("data"), dict) else {}).get("matchKind") or "none", 60)
        kinds.append(kind or "none")
    _last_found_count = found
    _last_match_kinds = kinds[:MAX_SOURCE_CONTEXT_SUBJECTS]
    _last_error_status = _compact(error_status, 80) or "none"


def build_source_context_diagnostics(environ: dict[str, str] | None = None) -> dict[str, Any]:
    return {
        "available": True,
        "enabled": is_source_context_injection_enabled(environ),
        "last_status": _last_status,
        "last_subjects": list(_last_subjects),
        "last_found_count": _last_found_count,
        "last_match_kinds": list(_last_match_kinds),
        "last_error_status": _last_error_status,
    }


def build_source_context_for_subjects(subjects: list[str], lookup_func: Callable[[dict[str, str]], dict[str, Any]]) -> tuple[str, list[dict[str, Any]]]:
    """Lookup up to two subjects and return a combined compact context block."""

    clean_subjects = []
    for subject in subjects or []:
        cleaned = _clean_subject(subject)
        if cleaned and cleaned.casefold() not in {s.casefold() for s in clean_subjects}:
            clean_subjects.append(cleaned)
    if len(clean_subjects) > MAX_SOURCE_CONTEXT_SUBJECTS:
        record_source_context_attempt(clean_subjects, [], status="too_many_subjects")
        return (
            "SOURCE FILE / INTERNAL CASE FILE CONTEXT:\n"
            f"Subject candidates: {', '.join(clean_subjects[:4])}\n"
            "Lookup status: not run; more than two likely subjects were detected.\n"
            "Boundary: Ask the operator to narrow the request to one or two Source Files before using case-file context.",
            [],
        )

    blocks: list[str] = []
    results: list[dict[str, Any]] = []
    for subject in clean_subjects:
        result = lookup_func({"lookupKey": "subject", "lookupValue": subject, "subject": subject})
        results.append(result)
        blocks.append(build_source_context_block(result, subject))
    status = "loaded" if blocks else "no_subjects"
    error_status = "none"
    for result in results:
        if not result.get("ok"):
            error_status = _compact(result.get("kind") or result.get("error") or "error", 80)
            status = "lookup_error"
            break
    record_source_context_attempt(clean_subjects, results, status=status, error_status=error_status)
    return "\n\n".join(blocks), results
