from __future__ import annotations

import json
import re
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Dict, Optional, Tuple

PRESENCE_SOURCES = {"heartbeat", "startup", "admin", "reset", "unknown"}
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
RELAY_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{7,127}$")


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


def validate_relay_id(relay_id: str) -> str:
    rid = (relay_id or "").strip()
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
    return {"contractVersion": 2, "kind": "presence", "presence": {"status": status, "mode": mode, "source": source}}


def build_relay_envelope(relay_id: str, message: str, current_directive: str, source_class: str, trigger: str) -> Dict[str, Any]:
    website_source_class = map_source_class(source_class)
    website_trigger = map_trigger(trigger)
    return {
        "contractVersion": 2,
        "kind": "relay",
        "relay": {
            "relayId": validate_relay_id(relay_id),
            "message": message or "",
            "currentDirective": current_directive or "",
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


def validate_relay_response(data: Any, submitted: Dict[str, Any]) -> DeliveryResult:
    if not isinstance(data, dict):
        return DeliveryResult(False, "delivery_failed", "malformed_json")
    if data.get("ok") is not True:
        return DeliveryResult(False, "delivery_failed", "contract_rejected")
    relay = data.get("relay")
    expected = submitted.get("relay") if isinstance(submitted, dict) else None
    if not isinstance(relay, dict) or not isinstance(expected, dict):
        return DeliveryResult(False, "delivery_failed", "malformed_json")
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
                    return validate_relay_response(json.loads(body or "{}"), envelope) if envelope.get("kind") == "relay" else DeliveryResult(True, "published", http_status=code)
                except json.JSONDecodeError:
                    return DeliveryResult(False, "delivery_failed", "malformed_json", http_status=code)
            if code == 400:
                return DeliveryResult(False, "delivery_failed", "contract_rejected", http_status=code)
            if code == 409:
                return DeliveryResult(False, "delivery_failed", "relay_id_conflict", http_status=code)
            if code < 500 or attempt >= retries:
                return DeliveryResult(False, "delivery_failed", "retryable_delivery_failure", http_status=code)
        except urllib.error.HTTPError as e:
            if e.code == 400:
                return DeliveryResult(False, "delivery_failed", "contract_rejected", http_status=e.code)
            if e.code == 409:
                return DeliveryResult(False, "delivery_failed", "relay_id_conflict", http_status=e.code)
            if e.code < 500 or attempt >= retries:
                return DeliveryResult(False, "delivery_failed", "retryable_delivery_failure", http_status=e.code)
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
    required = {"relayId", "message", "currentDirective", "sourceClass", "trigger", "publishedAt"}
    if set(relay.keys()) != required:
        return None
    try:
        validate_relay_id(relay["relayId"])
        if relay["sourceClass"] not in WEBSITE_SOURCE_CLASSES or relay["trigger"] not in set(TRIGGER_MAP.values()):
            return None
        if not _valid_published_at(relay["publishedAt"]):
            return None
    except ContractV2Error:
        return None
    return relay
