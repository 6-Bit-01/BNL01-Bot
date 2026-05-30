"""Safe read-only BNL Source File lookup client and Discord formatter.

This module only performs token-protected GET lookups against the website's
read-only source-file endpoint. It intentionally contains no mutation helpers
for source files, aliases, drafts, recommendations, publishing, or content.
"""

from __future__ import annotations

import json
import logging
import os
import re
import socket
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

DEFAULT_SOURCE_FILE_READ_URL = "https://www.barcode-network.com/api/bnl/source-files"
SOURCE_FILE_READ_TIMEOUT_SECONDS = 8
_SOURCE_LOOKUP_PATTERN = re.compile(
    r"^!bnl\s+(?:(?:source\s+(?:lookup|find))|(?:dossier\s+lookup))\s+(.+)$",
    re.IGNORECASE | re.DOTALL,
)
_ALLOWED_QUERY_KEYS = {
    "alias": "alias",
    "candidateid": "candidateId",
    "candidate_id": "candidateId",
    "normalizedname": "normalizedName",
    "normalized_name": "normalizedName",
    "subject": "subject",
}
_SENSITIVE_KEY_RE = re.compile(
    r"(token|secret|authorization|password|credential|customer|payment|discordid|discord_id|account|upload|contact|transcript|member_activity_events|queue)",
    re.IGNORECASE,
)

_last_lookup_status = "idle"
_last_lookup_match_kind = "none"
_last_lookup_subject = "none"
_last_lookup_found = False
_last_lookup_error_status = "none"


def get_source_file_read_url(environ: dict[str, str] | None = None) -> str:
    env = environ if environ is not None else os.environ
    configured = (env.get("BNL_SOURCE_FILE_READ_URL") or "").strip()
    return configured or DEFAULT_SOURCE_FILE_READ_URL


def is_source_file_read_token_configured(environ: dict[str, str] | None = None) -> bool:
    env = environ if environ is not None else os.environ
    return bool((env.get("BNL_SOURCE_FILE_READ_TOKEN") or "").strip())


def parse_source_file_lookup_command(content: str) -> tuple[bool, dict[str, str] | None, str]:
    """Parse explicit operator source-file lookup commands.

    Supported forms:
      !bnl source lookup Signal Witch
      !bnl source find alias=ShadowsPit
      !bnl dossier lookup normalizedName=signal-witch
    """

    text = (content or "").strip()
    match = _SOURCE_LOOKUP_PATTERN.match(text)
    if not match:
        return False, None, "not_a_source_file_lookup_command"

    raw_query = re.sub(r"\s+", " ", match.group(1).strip())
    if not raw_query:
        return True, None, "Use: `!bnl source lookup <subject>`"

    key_match = re.match(r"^([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.+)$", raw_query, flags=re.DOTALL)
    if key_match:
        requested_key = key_match.group(1).strip().lower()
        value = key_match.group(2).strip()
        canonical_key = _ALLOWED_QUERY_KEYS.get(requested_key)
        if not canonical_key:
            return True, None, "Lookup key must be one of: subject, alias, candidateId, normalizedName."
        if not value:
            return True, None, f"Lookup value for `{canonical_key}` is required."
        return True, {"lookupKey": canonical_key, "lookupValue": value, canonical_key: value}, ""

    return True, {"lookupKey": "subject", "lookupValue": raw_query, "subject": raw_query}, ""


def _safe_failure(error: str, status: int | None = None, *, kind: str = "error") -> dict[str, Any]:
    return {"ok": False, "found": False, "error": error, "status": status, "kind": kind}


def lookup_source_file(query: dict[str, str], *, environ: dict[str, str] | None = None, opener=None) -> dict[str, Any]:
    """GET a source-file lookup result from the website without exposing secrets."""

    global _last_lookup_status, _last_lookup_match_kind, _last_lookup_subject, _last_lookup_found, _last_lookup_error_status
    env = environ if environ is not None else os.environ
    token = (env.get("BNL_SOURCE_FILE_READ_TOKEN") or "").strip()
    lookup_key = str((query or {}).get("lookupKey") or "subject")
    lookup_value = str((query or {}).get("lookupValue") or (query or {}).get(lookup_key) or "").strip()
    _last_lookup_subject = _safe_scalar(lookup_value, 90) or "none"
    _last_lookup_found = False
    _last_lookup_match_kind = "none"
    _last_lookup_error_status = "none"

    if not token:
        _last_lookup_status = "missing_token"
        logging.warning("source_file_lookup_failed reason=missing_read_token")
        return _safe_failure("Source-file read token is not configured.", kind="missing_token")
    if lookup_key not in {"subject", "alias", "candidateId", "normalizedName"} or not lookup_value:
        _last_lookup_status = "invalid_query"
        return _safe_failure("Source-file lookup query is invalid.", kind="invalid_query")

    base_url = get_source_file_read_url(env)
    separator = "&" if urllib.parse.urlparse(base_url).query else "?"
    url = f"{base_url}{separator}{urllib.parse.urlencode({lookup_key: lookup_value})}"
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "x-bnl-source-file-read-token": token,
        },
        method="GET",
    )
    urlopen = opener.open if opener is not None else urllib.request.urlopen

    try:
        with urlopen(request, timeout=SOURCE_FILE_READ_TIMEOUT_SECONDS) as response:
            status = getattr(response, "status", None) or response.getcode()
            raw = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        status = exc.code
        _last_lookup_status = f"http_error:{status}"
        _last_lookup_error_status = f"HTTP {status}"
        logging.warning("source_file_lookup_failed status=%s", status)
        if status == 401:
            return _safe_failure("Source-file endpoint rejected the read token.", status=status, kind="unauthorized")
        if status == 404:
            return {"ok": True, "found": False, "status": status, "query": query}
        if 500 <= int(status or 0) <= 599:
            return _safe_failure("Source-file endpoint is temporarily unavailable.", status=status, kind="server_error")
        return _safe_failure("Source-file endpoint rejected the lookup.", status=status, kind="http_error")
    except (urllib.error.URLError, TimeoutError, socket.timeout):
        _last_lookup_status = "network_error"
        _last_lookup_error_status = "network_or_timeout"
        logging.warning("source_file_lookup_failed reason=network_or_timeout")
        return _safe_failure("Source-file endpoint was unreachable or timed out.", kind="network_error")
    except Exception:
        _last_lookup_status = "unexpected_error"
        _last_lookup_error_status = "unexpected_error"
        logging.warning("source_file_lookup_failed reason=unexpected_error")
        return _safe_failure("Source-file lookup failed unexpectedly.", kind="unexpected_error")

    try:
        parsed = json.loads(raw) if raw.strip() else {}
    except json.JSONDecodeError:
        _last_lookup_status = f"invalid_json:{status or 'no_status'}"
        _last_lookup_error_status = "invalid_json"
        logging.warning("source_file_lookup_failed status=%s reason=invalid_json", status)
        return _safe_failure("Source-file endpoint returned invalid JSON.", status=status, kind="invalid_json")

    found = bool(parsed.get("found"))
    match_obj = parsed.get("match") if isinstance(parsed.get("match"), dict) else {}
    match_kind = _safe_scalar(parsed.get("matchKind") or parsed.get("match_kind") or match_obj.get("kind") or "", 60)
    if not match_kind:
        match_kind = "confirmed" if found else "none"
    _last_lookup_status = f"ok:{status or 200}"
    _last_lookup_match_kind = match_kind
    _last_lookup_found = found
    _last_lookup_error_status = "none"
    return {"ok": True, "found": found, "status": status, "query": query, "data": parsed, "matchKind": match_kind}


def build_source_file_lookup_diagnostics(environ: dict[str, str] | None = None) -> dict[str, Any]:
    env = environ if environ is not None else os.environ
    return {
        "read_url_configured": bool((env.get("BNL_SOURCE_FILE_READ_URL") or "").strip()),
        "read_url_using_default": not bool((env.get("BNL_SOURCE_FILE_READ_URL") or "").strip()),
        "read_token_configured": is_source_file_read_token_configured(env),
        "last_lookup_status": _last_lookup_status,
        "last_lookup_match_kind": _last_lookup_match_kind,
        "last_lookup_subject": _last_lookup_subject,
        "last_lookup_found": _last_lookup_found,
        "last_lookup_error_status": _last_lookup_error_status,
    }


def _safe_scalar(value: Any, limit: int = 160) -> str:
    if value is None:
        return ""
    text = re.sub(r"\s+", " ", str(value)).strip().replace("`", "'")
    if not text:
        return ""
    if len(text) <= limit:
        return text
    return text[: max(1, limit - 1)].rstrip() + "…"


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


def _safe_list(value: Any, *, limit: int = 3, item_limit: int = 90) -> list[str]:
    if value is None:
        return []
    if isinstance(value, dict):
        iterable = value.values()
    elif isinstance(value, (list, tuple, set)):
        iterable = value
    else:
        iterable = [value]
    out: list[str] = []
    for item in iterable:
        if isinstance(item, dict):
            if any(_SENSITIVE_KEY_RE.search(str(k)) for k in item.keys()):
                item = {k: v for k, v in item.items() if not _SENSITIVE_KEY_RE.search(str(k))}
            text = item.get("summary") or item.get("name") or item.get("label") or item.get("reason") or item.get("value") or item.get("status")
        else:
            text = item
        safe = _safe_scalar(text, item_limit)
        if safe:
            out.append(safe)
        if len(out) >= limit:
            break
    return out


def _append_line(lines: list[str], label: str, value: Any, *, limit: int = 180) -> None:
    if isinstance(value, (list, tuple, set, dict)):
        parts = _safe_list(value, limit=3, item_limit=max(40, limit // 2))
        text = "; ".join(parts)
    else:
        text = _safe_scalar(value, limit)
    if text:
        lines.append(f"{label}: {text}")


def _alias_summary(source_file: dict[str, Any], data: dict[str, Any]) -> str:
    aliases = _lookup_path(source_file, [("aliases",), ("identityLinks",), ("identity_links",)])
    if isinstance(aliases, dict):
        confirmed = len(aliases.get("confirmed") or aliases.get("confirmedAliases") or [])
        proposed = len(aliases.get("proposed") or aliases.get("proposedAliases") or [])
        parts = []
        if proposed or "proposed" in aliases or "proposedAliases" in aliases:
            parts.append(f"{proposed} proposed")
        if confirmed or "confirmed" in aliases or "confirmedAliases" in aliases:
            parts.append(f"{confirmed} confirmed")
        if parts:
            return ", ".join(parts)
    if isinstance(aliases, list):
        confirmed = 0
        proposed = 0
        names = []
        for alias in aliases:
            if isinstance(alias, dict):
                status = str(alias.get("status") or alias.get("state") or "").lower()
                if "confirm" in status:
                    confirmed += 1
                elif "propos" in status:
                    proposed += 1
                names.append(alias.get("alias") or alias.get("name") or alias.get("value"))
            else:
                names.append(alias)
        summary = []
        if proposed:
            summary.append(f"{proposed} proposed")
        if confirmed:
            summary.append(f"{confirmed} confirmed")
        if summary:
            return ", ".join(summary)
        return ", ".join(_safe_list(names, limit=3, item_limit=40))
    matched_alias = _lookup_path(data, [("matchedAlias",), ("alias",)])
    return _safe_scalar(matched_alias, 80)


def _recommendation_summary(source_file: dict[str, Any]) -> str:
    recs = _lookup_path(source_file, [("recommendations",), ("attachedRecommendations",), ("attached_recommendations",)])
    if isinstance(recs, list):
        bits = _safe_list(recs, limit=2, item_limit=70)
        return f"{len(recs)} attached" + (f" — {'; '.join(bits)}" if bits else "")
    count = _lookup_path(source_file, [("recommendationCount",), ("recommendationsCount",), ("attachedRecommendationCount",)])
    if count not in (None, ""):
        summary = _lookup_path(source_file, [("recommendationSummary",), ("attachedRecommendationSummary",)])
        return _safe_scalar(f"{count} attached" + (f" — {summary}" if summary else ""), 140)
    return ""


def format_source_file_lookup_response(result: dict[str, Any], original_query: str = "") -> str:
    """Build a concise internal-safe Discord response from a lookup result."""

    if not result.get("ok"):
        status = result.get("status")
        if result.get("kind") == "unauthorized" or status == 401:
            return "Source-file lookup failed: endpoint authorization was rejected. Check the server-side read token."
        return f"Source-file lookup failed: {_safe_scalar(result.get('error') or 'lookup unavailable', 180)}"

    data = result.get("data") if isinstance(result.get("data"), dict) else {}
    query_value = original_query or (result.get("query") or {}).get("lookupValue") or "that subject"
    possible = data.get("possibleMatches") or data.get("possible_matches") or data.get("matches")
    if not result.get("found"):
        possible_items = _possible_match_lines(possible)
        if possible_items:
            lines = ["No confirmed source-file match. Possible matches need owner/admin review:"]
            lines.extend(f"* {item}" for item in possible_items)
            lines.append("Next: confirm/reject alias from the Source File page.")
            return _cap_discord("\n".join(lines))
        return _cap_discord(
            f"No BNL Source File found for “{_safe_scalar(query_value, 90)}.”\n"
            "Next: use dynamic discovery or create a source-file candidate if this subject matters."
        )

    source_file = _source_file_object(data)
    match_kind = _safe_scalar(result.get("matchKind") or data.get("matchKind") or data.get("match_kind") or "confirmed", 70)
    name = _lookup_path(source_file, [("name",), ("sourceFileName",), ("source_file_name",), ("subject",), ("displayName",), ("title",), ("normalizedName",)])
    lines: list[str] = [f"Source File: {_safe_scalar(name or query_value, 90)}"]
    _append_line(lines, "Match", match_kind, limit=90)
    if "alias" in match_kind.lower() and "confirm" in match_kind.lower():
        alias_value = _lookup_path(data, [("matchedAlias",), ("alias",), ("match", "alias")]) or query_value
        lines[-1] = f"Match: confirmed alias “{_safe_scalar(alias_value, 70)}”"
        lines.append("Identity note: confirmed aliases route workflow context only; do not expose private identity unless public-safe.")
    _append_line(lines, "Status", _lookup_path(source_file, [("status",), ("reviewStatus",), ("state",)]), limit=80)
    _append_line(lines, "Type", _lookup_path(source_file, [("candidateType",), ("sourceType",), ("type",), ("kind",)]), limit=80)
    _append_line(lines, "Confidence", _lookup_path(source_file, [("confidence",), ("confidenceLevel",)]), limit=50)
    _append_line(lines, "Source", _lookup_path(source_file, [("source",), ("sourceLane",), ("sourceLanes",), ("lanes",), ("source_lanes",)]), limit=130)
    _append_line(lines, "Evidence", _lookup_path(source_file, [("evidenceSummary",), ("reason",), ("summary",), ("notesSummary",), ("sourceNotesSummary",)]), limit=220)
    _append_line(lines, "Known facts", _lookup_path(source_file, [("knownFacts",), ("facts",)]), limit=180)
    _append_line(lines, "Missing info", _lookup_path(source_file, [("missingInfo",), ("missing",)]), limit=170)
    safety = _lookup_path(source_file, [("publicSafetyNotes",), ("safetyNotes",), ("publicUseNotes",), ("safety",)])
    _append_line(lines, "Safety", safety or "Internal source-file context — not a public dossier yet.", limit=180)
    _append_line(lines, "Do-not-say", _lookup_path(source_file, [("doNotSay",), ("do_not_say",)]), limit=140)
    alias_text = _alias_summary(source_file, data)
    _append_line(lines, "Aliases", alias_text, limit=130)
    _append_line(lines, "Recommendations", _recommendation_summary(source_file), limit=150)
    _append_line(lines, "Draft/review", _lookup_path(source_file, [("activeDraft",), ("draft",), ("ownerReview",), ("ownerReviewState",)]), limit=150)
    _append_line(lines, "Duplicate warning", _lookup_path(source_file, [("duplicateWarnings",), ("duplicates",), ("duplicateWarning",)]), limit=150)
    _append_line(lines, "Next", _lookup_path(source_file, [("nextRecommendedAction",), ("suggestedAction",), ("nextAction",)]) or "review source notes before drafting or public use.", limit=160)
    return _cap_discord("\n".join(lines))


def _possible_match_lines(value: Any) -> list[str]:
    if not isinstance(value, (list, tuple)):
        return _safe_list(value, limit=5, item_limit=90)
    lines: list[str] = []
    for item in value[:5]:
        if isinstance(item, dict):
            name = _safe_scalar(item.get("name") or item.get("sourceFileName") or item.get("subject") or item.get("normalizedName") or item.get("alias"), 70)
            kind = _safe_scalar(item.get("matchKind") or item.get("match_kind") or item.get("reason") or item.get("status"), 60)
            if name and kind:
                lines.append(f"{name} — {kind}")
            elif name:
                lines.append(name)
        else:
            text = _safe_scalar(item, 90)
            if text:
                lines.append(text)
    return lines


def _cap_discord(text: str, limit: int = 1850) -> str:
    safe = text.strip()
    if len(safe) <= limit:
        return safe
    return safe[: limit - 1].rstrip() + "…"
