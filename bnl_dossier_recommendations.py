"""Safe sender for BNL dossier recommendation ingest.

This module is intentionally isolated from Discord runtime state. It builds a
review-only recommendation payload and sends it to the BARCODE Network site
endpoint when explicitly called by an operator-controlled bot path.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import socket
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

DEFAULT_DOSSIER_INGEST_URL = "https://www.barcode-network.com/api/bnl/dossier-recommendations"
DEFAULT_SOURCE_FILE_ARCHIVE_URL = "https://www.barcode-network.com/api/bnl/source-file-enrichments"
DOSSIER_INGEST_PATH = "/api/bnl/dossier-recommendations"
SOURCE_FILE_ARCHIVE_PATH = "/api/bnl/source-file-enrichments"
DOSSIER_INGEST_TIMEOUT_SECONDS = 10
SOURCE_FILE_ARCHIVE_TIMEOUT_SECONDS = 15
SUPPORTED_PAYLOAD_FIELDS = {
    "type",
    "subjectName",
    "subjectKey",
    "targetCandidateId",
    "targetDossierId",
    "reason",
    "evidenceSummary",
    "confidence",
    "sourceLanes",
    "sourceTypes",
    "sourceQuality",
    "sourceCounts",
    "warningCounts",
    "qualityScore",
    "qualityStatus",
    "forced",
    "visibilityLabels",
    "safetyWarnings",
    "suggestedAction",
    "missingInfo",
    "publicSafetyNotes",
    "doNotSay",
    "recommendedTags",
    "recommendedCategory",
    "recommendedKind",
    "recommendedEcosystemLane",
    "recommendedIdentityAuthority",
    "createdBy",
    "ingestKey",
    "ingestSource",
    "knownContext",
    "usefulEvidence",
    "relationshipSignals",
    "observedChannels",
    "channelActivity",
    "conversationHighlights",
    "representativeEvidence",
    "activityFrequencySummary",
    "topChannels",
    "topTopicDetails",
    "recentActivitySummary",
    "authoredVsMentionedSummary",
    "topicBreakdown",
    "recurringTopics",
    "bnlInteractionSignals",
    "musicSignals",
    "communitySignals",
    "sourceCoverage",
    "evidenceDetails",
    "bestEvidenceToReview",
    "publicSafePossibilities",
    "publicUseCandidates",
    "privateOnlyNotes",
    "reviewOnlyEvidence",
    "notPublicYet",
    "recommendedAction",
    "sourceAuthority",
    "rawProvenance",
    "queueSubmissionStatus",
    "queueSubmissionNote",
    "entityIntelligenceProfile",
    "adminSummary",
    "updateSummary",
    "recommendationClusterSummary",
}
_SAFE_LANE_PATTERN = re.compile(r"[^a-z0-9_-]+")
VALID_DOSSIER_CATEGORIES = {"Entity", "Personnel", "Sponsor", "Interface", "Production"}



def _safe_url_host(value: str) -> str:
    try:
        parsed = urllib.parse.urlparse(str(value or "").strip())
        return (parsed.hostname or "").lower()[:180]
    except Exception:
        return ""


def _configured_callback_hosts(environ: dict[str, str] | None = None) -> set[str]:
    env = environ if environ is not None else os.environ
    raw = env.get("BNL_TRUSTED_SITE_CALLBACK_HOSTS") or env.get("BNL_SITE_CALLBACK_ALLOWED_HOSTS") or ""
    return {part.strip().lower() for part in re.split(r"[,\s]+", raw) if part.strip()}


def is_trusted_site_callback_base_url(value: str | None, *, environ: dict[str, str] | None = None) -> bool:
    """Return whether a site callback base URL is safe for BNL archive posts.

    The callback target must be HTTPS and belong to the production BARCODE site,
    a configured allowlist host, or a BARCODE Network Vercel preview host. This
    intentionally rejects localhost and arbitrary external hosts by default.
    """

    text = str(value or "").strip()
    if not text:
        return False
    try:
        parsed = urllib.parse.urlparse(text)
    except Exception:
        return False
    host = (parsed.hostname or "").lower()
    if parsed.scheme != "https" or not host:
        return False
    if host == "www.barcode-network.com":
        return True
    for allowed in _configured_callback_hosts(environ):
        if allowed.startswith("*.") and host.endswith(allowed[1:]):
            return True
        if host == allowed:
            return True
    # Vercel preview deployments for this project are allowed, while generic
    # arbitrary Vercel/external hosts remain rejected.
    if host.endswith(".vercel.app") and "barcode-network" in host:
        return True
    return False


def normalize_trusted_site_callback_base_url(value: str | None, *, environ: dict[str, str] | None = None) -> str:
    """Return a normalized trusted origin/base URL, or an empty string."""

    if not is_trusted_site_callback_base_url(value, environ=environ):
        return ""
    parsed = urllib.parse.urlparse(str(value or "").strip())
    return urllib.parse.urlunparse((parsed.scheme, parsed.netloc, "", "", "", "")).rstrip("/")


def site_endpoint_url_from_callback_base(callback_base_url: str | None, path: str, *, environ: dict[str, str] | None = None) -> str:
    """Build a site endpoint URL from a trusted callback base URL."""

    base = normalize_trusted_site_callback_base_url(callback_base_url, environ=environ)
    if not base:
        return ""
    return urllib.parse.urljoin(base.rstrip("/") + "/", path.lstrip("/"))


def select_trusted_site_callback_base_url(payload: dict[str, Any] | None, *, environ: dict[str, str] | None = None) -> tuple[str, str]:
    """Pick a trusted requesting-site callback base from a refresh payload.

    Returns (base_url, source), where source is requesting_site_origin when a
    trusted payload value is honored, otherwise env_default.
    """

    request = payload if isinstance(payload, dict) else {}
    for key in ("sourceFileArchiveCallbackBaseUrl", "siteCallbackBaseUrl", "requestingSiteOrigin"):
        base = normalize_trusted_site_callback_base_url(request.get(key), environ=environ)
        if base:
            return base, "requesting_site_origin"
    return "", "env_default"


def get_dossier_ingest_url(environ: dict[str, str] | None = None, *, callback_base_url: str | None = None) -> str:
    """Return the selected ingest URL, honoring a trusted callback base first."""

    env = environ if environ is not None else os.environ
    callback_url = site_endpoint_url_from_callback_base(callback_base_url, DOSSIER_INGEST_PATH, environ=env)
    if callback_url:
        return callback_url
    configured = (env.get("BNL_DOSSIER_INGEST_URL") or "").strip()
    if configured:
        return configured
    base = normalize_trusted_site_callback_base_url(env.get("BNL_WEBSITE_BASE_URL") or env.get("BNL_SITE_BASE_URL"), environ=env)
    if base:
        return urllib.parse.urljoin(base.rstrip("/") + "/", DOSSIER_INGEST_PATH.lstrip("/"))
    return DEFAULT_DOSSIER_INGEST_URL


def is_dossier_ingest_token_configured(environ: dict[str, str] | None = None) -> bool:
    """Return whether the ingest token exists without exposing its value."""

    env = environ if environ is not None else os.environ
    return bool((env.get("BNL_DOSSIER_INGEST_TOKEN") or "").strip())


def get_source_file_archive_url(environ: dict[str, str] | None = None, *, callback_base_url: str | None = None) -> str:
    """Return the selected Source File archive URL, honoring a trusted callback base first."""

    env = environ if environ is not None else os.environ
    callback_url = site_endpoint_url_from_callback_base(callback_base_url, SOURCE_FILE_ARCHIVE_PATH, environ=env)
    if callback_url:
        return callback_url
    configured = (env.get("BNL_SOURCE_FILE_ARCHIVE_URL") or "").strip()
    if configured:
        return configured
    base = normalize_trusted_site_callback_base_url(env.get("BNL_WEBSITE_BASE_URL") or env.get("BNL_SITE_BASE_URL"), environ=env)
    if base:
        return urllib.parse.urljoin(base.rstrip("/") + "/", SOURCE_FILE_ARCHIVE_PATH.lstrip("/"))
    return DEFAULT_SOURCE_FILE_ARCHIVE_URL


def get_source_file_archive_token(environ: dict[str, str] | None = None) -> str:
    """Return archive token, falling back to dossier ingest token when needed."""

    env = environ if environ is not None else os.environ
    archive_token = (env.get("BNL_SOURCE_FILE_ARCHIVE_TOKEN") or "").strip()
    if archive_token:
        return archive_token
    return (env.get("BNL_DOSSIER_INGEST_TOKEN") or "").strip()


def is_source_file_archive_token_configured(environ: dict[str, str] | None = None) -> bool:
    """Return whether the Source File archive sender has a usable token."""

    return bool(get_source_file_archive_token(environ))


def _normalize_subject_key(subject_name: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "-", (subject_name or "").strip().lower())
    return normalized.strip("-") or "unknown-subject"


def _normalize_source_lanes(source_lanes: Any) -> list[str]:
    if isinstance(source_lanes, str):
        raw_lanes = re.split(r"[,\s]+", source_lanes)
    elif isinstance(source_lanes, (list, tuple, set)):
        raw_lanes = list(source_lanes)
    else:
        raw_lanes = []

    lanes: list[str] = []
    seen = set()
    for lane in raw_lanes:
        normalized = _SAFE_LANE_PATTERN.sub("_", str(lane or "").strip().lower()).strip("_")
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        lanes.append(normalized[:48])
    return lanes or ["unknown"]


def generate_dossier_ingest_key(payload: dict[str, Any]) -> str:
    """Create a deterministic non-sensitive ingest key for deduplication."""

    recommendation_type = re.sub(r"[^a-z0-9_-]+", "_", str(payload.get("type") or "new_subject").strip().lower()).strip("_") or "new_subject"
    subject_key = _normalize_subject_key(str(payload.get("subjectName") or payload.get("subjectKey") or ""))
    source_lanes = _normalize_source_lanes(payload.get("sourceLanes"))
    lane_part = "-".join(source_lanes)
    reason_material = "\n".join(
        str(payload.get(key) or "")
        for key in ("reason", "evidenceSummary")
        if str(payload.get(key) or "").strip()
    )
    reason_hash = hashlib.sha256(reason_material.strip().encode("utf-8")).hexdigest()[:8] if reason_material.strip() else "no-reason"
    return f"bnl:dossier:{recommendation_type}:{subject_key}:{lane_part}:{reason_hash}"


def build_dossier_recommendation_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Return a sanitized recommendation payload with safe defaults."""

    cleaned = {key: value for key, value in (payload or {}).items() if key in SUPPORTED_PAYLOAD_FIELDS and value is not None}
    cleaned["type"] = str(cleaned.get("type") or "new_subject").strip() or "new_subject"
    cleaned["createdBy"] = str(cleaned.get("createdBy") or "bnl").strip() or "bnl"
    cleaned["confidence"] = str(cleaned.get("confidence") or "medium").strip() or "medium"
    cleaned["sourceLanes"] = _normalize_source_lanes(cleaned.get("sourceLanes"))
    if cleaned.get("recommendedCategory") not in VALID_DOSSIER_CATEGORIES:
        cleaned.pop("recommendedCategory", None)

    if cleaned.get("subjectName") is not None:
        cleaned["subjectName"] = str(cleaned.get("subjectName") or "").strip()
    if not cleaned.get("subjectKey") and cleaned.get("subjectName"):
        cleaned["subjectKey"] = _normalize_subject_key(str(cleaned["subjectName"]))
    if not cleaned.get("ingestKey"):
        cleaned["ingestKey"] = generate_dossier_ingest_key(cleaned)
    return cleaned


def build_manual_dossier_recommendation(subject: str, reason: str, source_lanes: Any = None) -> dict[str, Any]:
    """Build the v1 operator/manual recommendation payload."""

    return build_dossier_recommendation_payload(
        {
            "type": "new_subject",
            "subjectName": (subject or "").strip(),
            "reason": (reason or "").strip(),
            "evidenceSummary": (reason or "").strip(),
            "sourceLanes": source_lanes or ["rd_context"],
            "suggestedAction": "review_for_future_dossier",
            "createdBy": "bnl",
            "confidence": "medium",
        }
    )


def parse_manual_dossier_recommendation_command(content: str) -> tuple[bool, dict[str, Any] | None, str]:
    """Parse `!bnl dossier recommend <subject> | <reason> [| lanes=a,b]`."""

    text = (content or "").strip()
    match = re.match(r"^!bnl\s+dossier\s+recommend\s+(.+)$", text, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        return False, None, "not_a_dossier_recommendation_command"

    body = match.group(1).strip()
    parts = [part.strip() for part in body.split("|")]
    if len(parts) < 2:
        return True, None, "Use: `!bnl dossier recommend Subject | Reason`"

    subject, reason = parts[0], parts[1]
    if not subject:
        return True, None, "Subject is required."
    if not reason:
        return True, None, "Reason is required."

    lanes = ["rd_context"]
    for extra in parts[2:]:
        lane_match = re.match(r"^(?:lanes?|sourceLanes)\s*=\s*(.+)$", extra, flags=re.IGNORECASE)
        if lane_match:
            lanes = _normalize_source_lanes(lane_match.group(1))
    payload = build_manual_dossier_recommendation(subject, reason, lanes)
    return True, payload, ""


def _extract_recommendation_id(response_json: dict[str, Any]) -> Any:
    for key in ("recommendationId", "recommendation_id", "id"):
        if response_json.get(key) is not None:
            return response_json.get(key)
    recommendation = response_json.get("recommendation")
    if isinstance(recommendation, dict):
        return recommendation.get("id") or recommendation.get("recommendationId")
    return None



def _safe_response_error_text(raw: Any, limit: int = 280) -> str:
    """Return compact endpoint error text without secrets or payload material."""

    text = ""
    try:
        if isinstance(raw, (bytes, bytearray)):
            text = raw.decode("utf-8", errors="replace")
        else:
            text = str(raw or "")
        parsed = json.loads(text) if text.strip().startswith(("{", "[")) else None
        if isinstance(parsed, dict):
            for key in ("error", "message", "detail", "reason"):
                if parsed.get(key):
                    text = str(parsed.get(key))
                    break
    except Exception:
        text = str(raw or "")
    text = re.sub(r"Bearer\s+[A-Za-z0-9._~+/-]+", "Bearer [redacted]", text, flags=re.I)
    text = re.sub(r"(?i)(token|secret|authorization)\s*[:=]\s*[^\s,;}]+", r"\1=[redacted]", text)
    text = re.sub(r"\b\d{15,22}\b", "[redacted-id]", text)
    text = re.sub(r"\s+", " ", text).strip()
    if len(text) > limit:
        text = text[: max(0, limit - 1)].rstrip() + "…"
    return text

def _safe_failure(error: str, status: int | None = None, duplicate: bool = False) -> dict[str, Any]:
    return {
        "ok": False,
        "duplicate": duplicate,
        "recommendationId": None,
        "error": error,
        "status": status,
    }


def send_dossier_recommendation(payload: dict[str, Any], *, environ: dict[str, str] | None = None, opener=None, callback_base_url: str | None = None) -> dict[str, Any]:
    """POST a dossier recommendation to the configured site ingest endpoint."""

    env = environ if environ is not None else os.environ
    token = (env.get("BNL_DOSSIER_INGEST_TOKEN") or "").strip()
    if not token:
        logging.warning("dossier_recommendation_send_failed reason=missing_ingest_token")
        return _safe_failure("Dossier ingest token is not configured.")

    url = get_dossier_ingest_url(env, callback_base_url=callback_base_url)
    safe_payload = build_dossier_recommendation_payload(payload or {})
    logging.info(
        "dossier_recommendation_send_attempted "
        f"endpoint_configured={bool(url)} subject_key={safe_payload.get('subjectKey', 'unknown')}"
    )

    body = json.dumps(safe_payload).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=body,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        },
        method="POST",
    )
    urlopen = opener.open if opener is not None else urllib.request.urlopen

    try:
        with urlopen(request, timeout=DOSSIER_INGEST_TIMEOUT_SECONDS) as response:
            status = getattr(response, "status", None) or response.getcode()
            raw = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        status = exc.code
        body_text = ""
        try:
            body_text = _safe_response_error_text(exc.read())
        except Exception:
            body_text = _safe_response_error_text(getattr(exc, "reason", ""))
        error = "Dossier recommendation endpoint rejected the request."
        if body_text:
            error = f"{error} {body_text}"
        logging.warning(f"dossier_recommendation_send_failed status={status} endpoint_error={body_text or 'unavailable'}")
        return _safe_failure(error, status=status)
    except (urllib.error.URLError, TimeoutError, socket.timeout):
        logging.warning("dossier_recommendation_send_failed reason=network_or_timeout")
        return _safe_failure("Dossier recommendation endpoint was unreachable or timed out.")
    except Exception:
        logging.warning("dossier_recommendation_send_failed reason=unexpected_error")
        return _safe_failure("Dossier recommendation send failed unexpectedly.")

    try:
        parsed = json.loads(raw) if raw.strip() else {}
    except json.JSONDecodeError:
        logging.warning(f"dossier_recommendation_send_failed status={status} reason=invalid_json")
        return _safe_failure("Dossier recommendation endpoint returned invalid JSON.", status=status)

    ok = bool(parsed.get("ok")) and 200 <= int(status or 0) < 300
    duplicate = bool(parsed.get("duplicate"))
    recommendation_id = _extract_recommendation_id(parsed)
    if ok:
        if duplicate:
            logging.info(f"dossier_recommendation_duplicate status={status} recommendation_id={recommendation_id or 'none'}")
        else:
            logging.info(f"dossier_recommendation_send_succeeded status={status} recommendation_id={recommendation_id or 'none'}")
        return {
            "ok": True,
            "duplicate": duplicate,
            "recommendationId": recommendation_id,
            "error": "",
            "status": status,
        }

    logging.warning(f"dossier_recommendation_send_failed status={status}")
    return _safe_failure("Dossier recommendation endpoint returned a failure.", status=status, duplicate=duplicate)


def _extract_archive_id(parsed: dict[str, Any]) -> str | None:
    for key in ("archiveId", "archive_id", "sourceFileEnrichmentId", "source_file_enrichment_id", "id"):
        value = parsed.get(key)
        if value:
            return str(value)[:160]
    data = parsed.get("data")
    if isinstance(data, dict):
        return _extract_archive_id(data)
    archive = parsed.get("archive") or parsed.get("enrichment")
    if isinstance(archive, dict):
        return _extract_archive_id(archive)
    return None


def _safe_archive_failure(error: str, status: int | None = None) -> dict[str, Any]:
    return {"ok": False, "archiveId": None, "error": error, "status": status}


def send_source_file_archive_enrichment(payload: dict[str, Any], *, environ: dict[str, str] | None = None, opener=None, callback_base_url: str | None = None) -> dict[str, Any]:
    """POST a full Source File enrichment envelope to the review-only archive endpoint.

    This sender is intentionally separate from send_dossier_recommendation() so
    large case-file packages never have to fit through compact recommendation
    card fields. Logs include only routing/status metadata, never tokens or the
    archive payload body.
    """

    env = environ if environ is not None else os.environ
    token = get_source_file_archive_token(env)
    if not token:
        logging.warning("source_file_archive_send_failed reason=missing_archive_token")
        return _safe_archive_failure("Source File archive token is not configured.")

    url = get_source_file_archive_url(env, callback_base_url=callback_base_url)
    safe_payload = dict(payload or {})
    subject_key = _normalize_subject_key(str(safe_payload.get("subjectKey") or safe_payload.get("subjectName") or ""))
    logging.info("source_file_archive_send_attempted endpoint_configured=%s subject_key=%s", bool(url), subject_key)

    body = json.dumps(safe_payload).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"},
        method="POST",
    )
    urlopen = opener.open if opener is not None else urllib.request.urlopen
    try:
        with urlopen(request, timeout=SOURCE_FILE_ARCHIVE_TIMEOUT_SECONDS) as response:
            status = getattr(response, "status", None) or response.getcode()
            raw = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        status = exc.code
        try:
            body_text = _safe_response_error_text(exc.read())
        except Exception:
            body_text = _safe_response_error_text(getattr(exc, "reason", ""))
        error = "Source File archive endpoint rejected the request."
        if body_text:
            error = f"{error} {body_text}"
        logging.warning("source_file_archive_send_failed status=%s endpoint_error=%s", status, body_text or "unavailable")
        return _safe_archive_failure(error, status=status)
    except (urllib.error.URLError, TimeoutError, socket.timeout):
        logging.warning("source_file_archive_send_failed reason=network_or_timeout")
        return _safe_archive_failure("Source File archive endpoint was unreachable or timed out.")
    except Exception:
        logging.warning("source_file_archive_send_failed reason=unexpected_error")
        return _safe_archive_failure("Source File archive send failed unexpectedly.")

    try:
        parsed = json.loads(raw) if raw.strip() else {}
    except json.JSONDecodeError:
        logging.warning("source_file_archive_send_failed status=%s reason=invalid_json", status)
        return _safe_archive_failure("Source File archive endpoint returned invalid JSON.", status=status)

    ok = bool(parsed.get("ok", True)) and 200 <= int(status or 0) < 300
    archive_id = _extract_archive_id(parsed) if isinstance(parsed, dict) else None
    if ok:
        logging.info("source_file_archive_send_succeeded status=%s archive_id=%s", status, archive_id or "none")
        return {"ok": True, "archiveId": archive_id, "error": "", "status": status}
    logging.warning("source_file_archive_send_failed status=%s", status)
    return _safe_archive_failure("Source File archive endpoint returned a failure.", status=status)
