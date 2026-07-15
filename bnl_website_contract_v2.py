from __future__ import annotations

import json
import re
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Dict, Optional

STATUSES = {"ONLINE", "OFFLINE"}
MODES = {"STANDBY", "OBSERVATION", "ACTIVE_LIAISON", "SIGNAL_DEGRADATION", "RESTRICTED"}
PRESENCE_SOURCES = {"heartbeat", "startup", "admin", "reset", "unknown"}
MAX_MESSAGE_LENGTH = 600
MAX_DIRECTIVE_LENGTH = 800
WEBSITE_SOURCE_CLASSES = {
    "fresh_public_event",
    "recent_public_continuity",
    "scoped_broadcast_memory",
    "public_safe_memory",
    "approved_canon",
    "grounded_reflection",
}
SOURCE_CLASS_MAP = {
    "fresh_discord": "fresh_public_event",
    "conversation_continuity": "recent_public_continuity",
    "broadcast_memory": "scoped_broadcast_memory",
    "public_safe_memory": "public_safe_memory",
    "canon": "approved_canon",
    "reflection": "grounded_reflection",
}
TRIGGER_MAP = {"scheduled": "scheduled", "forcePull": "force_pull", "manual": "manual"}
RELAY_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{2,159}$")


class ContractV2Error(ValueError):
    pass


@dataclass
class DeliveryResult:
    ok: bool
    status: str
    reason: str = ""
    relay_id: str = ""
    published_at: str = ""
    idempotent: bool = False
    http_status: int = 0


def effective_contract_version(raw: Optional[str]) -> str:
    version = (raw or "2").strip() or "2"
    if version not in {"1", "2"}:
        raise ContractV2Error("invalid_contract_version")
    return version


def _validated_text(value: str, max_length: int, reason: str) -> str:
    if not isinstance(value, str):
        raise ContractV2Error(reason)
    clean = value.strip()
    if not clean or len(clean) > max_length:
        raise ContractV2Error(reason)
    return clean


def validate_status(status: str) -> str:
    if status not in STATUSES:
        raise ContractV2Error("invalid_status")
    return status


def validate_mode(mode: str) -> str:
    if mode not in MODES:
        raise ContractV2Error("invalid_mode")
    return mode


def validate_relay_id(relay_id: str) -> str:
    rid = _validated_text(relay_id, 160, "invalid_relay_id")
    if not RELAY_ID_RE.match(rid):
        raise ContractV2Error("invalid_relay_id")
    return rid


def map_source_class(source_class: str) -> str:
    if source_class in WEBSITE_SOURCE_CLASSES:
        return source_class
    try:
        return SOURCE_CLASS_MAP[source_class]
    except KeyError:
        raise ContractV2Error("unknown_source_class")


def map_trigger(trigger: str) -> str:
    try:
        return TRIGGER_MAP[trigger]
    except KeyError:
        raise ContractV2Error("unknown_trigger")


def build_presence_envelope(status: str = "ONLINE", mode: str = "OBSERVATION", source: str = "heartbeat") -> Dict[str, Any]:
    source = (source or "unknown").strip() or "unknown"
    if source not in PRESENCE_SOURCES:
        raise ContractV2Error("unknown_presence_source")
    return {"contractVersion": 2, "kind": "presence", "presence": {"status": validate_status(status), "mode": validate_mode(mode), "source": source}}


def build_relay_envelope(relay_id: str, message: str, current_directive: str, source_class: str, trigger: str) -> Dict[str, Any]:
    website_source_class = map_source_class(source_class)
    website_trigger = map_trigger(trigger)
    return {
        "contractVersion": 2,
        "kind": "relay",
        "relay": {
            "relayId": validate_relay_id(relay_id),
            "message": _validated_text(message, MAX_MESSAGE_LENGTH, "invalid_message"),
            "currentDirective": _validated_text(current_directive, MAX_DIRECTIVE_LENGTH, "invalid_directive"),
            "sourceClass": website_source_class,
            "trigger": website_trigger,
        },
    }


def canonical_payload_bytes(envelope: Dict[str, Any]) -> bytes:
    return json.dumps(envelope, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _valid_published_at(value: Any) -> bool:
    if not isinstance(value, str) or not value.strip():
        return False
    normalized = value.replace("Z", "+00:00")
    try:
        datetime.fromisoformat(normalized)
        return True
    except ValueError:
        return False


def validate_presence_response(data: Any, submitted: Dict[str, Any]) -> DeliveryResult:
    if not isinstance(data, dict):
        return DeliveryResult(False, "delivery_failed", "malformed_json")
    if data.get("ok") is not True:
        return DeliveryResult(False, "delivery_failed", "contract_rejected")
    if data.get("persisted") is not True:
        return DeliveryResult(False, "delivery_failed", "not_persisted")
    presence = data.get("presence")
    expected = submitted.get("presence") if isinstance(submitted, dict) else None
    if not isinstance(presence, dict) or not isinstance(expected, dict):
        return DeliveryResult(False, "delivery_failed", "malformed_json")
    if presence.get("contractVersion") != 2:
        return DeliveryResult(False, "delivery_failed", "response_mismatch")
    for key in ("status", "mode", "source"):
        if presence.get(key) != expected.get(key):
            return DeliveryResult(False, "delivery_failed", "response_mismatch")
    if not _valid_published_at(presence.get("receivedAt")):
        return DeliveryResult(False, "delivery_failed", "response_mismatch")
    return DeliveryResult(True, "published")


def validate_relay_response(data: Any, submitted: Dict[str, Any]) -> DeliveryResult:
    if not isinstance(data, dict):
        return DeliveryResult(False, "delivery_failed", "malformed_json")
    if data.get("ok") is not True:
        return DeliveryResult(False, "delivery_failed", "contract_rejected")
    if data.get("persisted") is not True:
        return DeliveryResult(False, "delivery_failed", "not_persisted")
    if "idempotent" in data and not isinstance(data.get("idempotent"), bool):
        return DeliveryResult(False, "delivery_failed", "response_mismatch")
    relay = data.get("relay")
    expected = submitted.get("relay") if isinstance(submitted, dict) else None
    if not isinstance(relay, dict) or not isinstance(expected, dict):
        return DeliveryResult(False, "delivery_failed", "malformed_json")
    if relay.get("contractVersion") != 2:
        return DeliveryResult(False, "delivery_failed", "response_mismatch")
    for key in ("relayId", "message", "currentDirective", "sourceClass", "trigger"):
        if relay.get(key) != expected.get(key):
            return DeliveryResult(False, "delivery_failed", "response_mismatch")
    published_at = relay.get("publishedAt")
    if not _valid_published_at(published_at):
        return DeliveryResult(False, "delivery_failed", "response_mismatch")
    return DeliveryResult(True, "published", "", relay_id=relay["relayId"], published_at=published_at, idempotent=bool(data.get("idempotent")))


def deliver_json(url: str, api_key: str, envelope: Dict[str, Any], *, opener: Callable[..., Any] = urllib.request.urlopen, retries: int = 1, timeout: int = 10) -> DeliveryResult:
    payload = canonical_payload_bytes(envelope)
    last_reason = "retryable_delivery_failure"
    for attempt in range(max(0, retries) + 1):
        req = urllib.request.Request(url, data=payload, method="POST", headers={"Content-Type": "application/json", "x-api-key": api_key})
        try:
            with opener(req, timeout=timeout) as response:
                code = getattr(response, "status", None) or response.getcode()
                body = response.read().decode("utf-8")
            if 200 <= code < 300:
                try:
                    data = json.loads(body or "{}")
                    return validate_relay_response(data, envelope) if envelope.get("kind") == "relay" else validate_presence_response(data, envelope)
                except json.JSONDecodeError:
                    return DeliveryResult(False, "delivery_failed", "malformed_json", http_status=code)
            if code == 400:
                return DeliveryResult(False, "delivery_failed", "contract_rejected", http_status=code)
            if code in {401, 403}:
                return DeliveryResult(False, "delivery_failed", "authentication_failed", http_status=code)
            if code == 409:
                return DeliveryResult(False, "delivery_failed", "relay_id_conflict", http_status=code)
            if code == 429 or code >= 500:
                if attempt >= retries:
                    return DeliveryResult(False, "delivery_failed", "retryable_delivery_failure", http_status=code)
                continue
            return DeliveryResult(False, "delivery_failed", "http_rejected", http_status=code)
        except urllib.error.HTTPError as e:
            if e.code == 400:
                return DeliveryResult(False, "delivery_failed", "contract_rejected", http_status=e.code)
            if e.code in {401, 403}:
                return DeliveryResult(False, "delivery_failed", "authentication_failed", http_status=e.code)
            if e.code == 409:
                return DeliveryResult(False, "delivery_failed", "relay_id_conflict", http_status=e.code)
            if e.code == 429 or e.code >= 500:
                if attempt >= retries:
                    return DeliveryResult(False, "delivery_failed", "retryable_delivery_failure", http_status=e.code)
                continue
            return DeliveryResult(False, "delivery_failed", "http_rejected", http_status=e.code)
        except (urllib.error.URLError, TimeoutError, OSError, ValueError):
            if attempt >= retries:
                return DeliveryResult(False, "delivery_failed", last_reason)
    return DeliveryResult(False, "delivery_failed", last_reason)


def parse_status_relay(data: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(data, dict) or data.get("contractVersion") != 2:
        return None
    relay = data.get("relay")
    if not isinstance(relay, dict):
        return None
    required = {"contractVersion", "relayId", "message", "currentDirective", "sourceClass", "trigger", "publishedAt"}
    if set(relay.keys()) != required:
        return None
    try:
        if relay.get("contractVersion") != 2:
            return None
        validate_relay_id(relay["relayId"])
        _validated_text(relay.get("message"), MAX_MESSAGE_LENGTH, "invalid_message")
        _validated_text(relay.get("currentDirective"), MAX_DIRECTIVE_LENGTH, "invalid_directive")
        if relay["sourceClass"] not in WEBSITE_SOURCE_CLASSES or relay["trigger"] not in set(TRIGGER_MAP.values()):
            return None
        if not _valid_published_at(relay["publishedAt"]):
            return None
    except ContractV2Error:
        return None
    return relay
