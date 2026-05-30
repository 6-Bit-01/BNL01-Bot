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
import urllib.request
from typing import Any

DEFAULT_DOSSIER_INGEST_URL = "https://www.barcode-network.com/api/bnl/dossier-recommendations"
DOSSIER_INGEST_TIMEOUT_SECONDS = 10
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
}
_SAFE_LANE_PATTERN = re.compile(r"[^a-z0-9_-]+")


def get_dossier_ingest_url(environ: dict[str, str] | None = None) -> str:
    """Return configured ingest URL or the production endpoint fallback."""

    env = environ if environ is not None else os.environ
    configured = (env.get("BNL_DOSSIER_INGEST_URL") or "").strip()
    return configured or DEFAULT_DOSSIER_INGEST_URL


def is_dossier_ingest_token_configured(environ: dict[str, str] | None = None) -> bool:
    """Return whether the ingest token exists without exposing its value."""

    env = environ if environ is not None else os.environ
    return bool((env.get("BNL_DOSSIER_INGEST_TOKEN") or "").strip())


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


def _safe_failure(error: str, status: int | None = None, duplicate: bool = False) -> dict[str, Any]:
    return {
        "ok": False,
        "duplicate": duplicate,
        "recommendationId": None,
        "error": error,
        "status": status,
    }


def send_dossier_recommendation(payload: dict[str, Any], *, environ: dict[str, str] | None = None, opener=None) -> dict[str, Any]:
    """POST a dossier recommendation to the configured site ingest endpoint."""

    env = environ if environ is not None else os.environ
    token = (env.get("BNL_DOSSIER_INGEST_TOKEN") or "").strip()
    if not token:
        logging.warning("dossier_recommendation_send_failed reason=missing_ingest_token")
        return _safe_failure("Dossier ingest token is not configured.")

    url = get_dossier_ingest_url(env)
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
        logging.warning(f"dossier_recommendation_send_failed status={status}")
        return _safe_failure("Dossier recommendation endpoint rejected the request.", status=status)
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
