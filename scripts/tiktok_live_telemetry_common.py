"""Shared bounded helpers for the isolated TikTok LIVE telemetry transport."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import signal
import sys
from collections import OrderedDict
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional, Sequence

SCHEMA_VERSION = 1
MAX_ROOM_ID_CHARS = 80
MAX_UNIQUE_ID_CHARS = 80
MAX_DISPLAY_NAME_CHARS = 120
MAX_COMMENT_CHARS = 1000
MAX_QUESTION_CHARS = 1000
MAX_GIFT_NAME_CHARS = 160
MAX_ERROR_CODE_CHARS = 80
DEFAULT_DEDUPE_CAPACITY = 20_000

_CONTROL_CHAR_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
_WHITESPACE_RE = re.compile(r"\s+")
_SAFE_CODE_RE = re.compile(r"[^A-Za-z0-9_.:-]+")
_USERNAME_RE = re.compile(r"^[A-Za-z0-9._]+$")


class EventIdDeduper:
    """Bound replay suppression without retaining comment or event content."""

    def __init__(self, capacity: int = DEFAULT_DEDUPE_CAPACITY) -> None:
        if capacity <= 0:
            raise ValueError("dedupe capacity must be positive")
        self.capacity = int(capacity)
        self._event_ids: "OrderedDict[str, None]" = OrderedDict()
        self.duplicates = 0

    def accept(self, event_id: str) -> bool:
        if not event_id:
            return True
        if event_id in self._event_ids:
            self._event_ids.move_to_end(event_id)
            self.duplicates += 1
            return False
        self._event_ids[event_id] = None
        while len(self._event_ids) > self.capacity:
            self._event_ids.popitem(last=False)
        return True


def _bounded_text(value: Any, max_chars: int) -> str:
    if value is None:
        return ""
    if isinstance(value, (str, int, float, bool)):
        text = str(value)
    else:
        return ""
    text = _CONTROL_CHAR_RE.sub(" ", text)
    text = _WHITESPACE_RE.sub(" ", text).strip()
    return text[:max_chars].rstrip()


def _bounded_code(value: Any, fallback: str) -> str:
    text = _bounded_text(value, MAX_ERROR_CODE_CHARS)
    text = _SAFE_CODE_RE.sub("_", text).strip("_")
    return text or fallback


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _mapping(value: Any) -> Mapping:
    return value if isinstance(value, Mapping) else {}


def _first_nonempty(*values: Any, max_chars: int = MAX_DISPLAY_NAME_CHARS) -> str:
    for value in values:
        text = _bounded_text(value, max_chars)
        if text:
            return text
    return ""


def _coerce_int(value: Any, default: int = 0) -> int:
    if isinstance(value, bool):
        return default
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return default


def _nonnegative_int(value: Any, default: int = 0, maximum: int = 10**12) -> int:
    return min(maximum, max(0, _coerce_int(value, default)))


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return isinstance(value, str) and value.strip().lower() in {"1", "true", "yes"}


def normalize_username(value: str) -> str:
    username = _bounded_text(value, MAX_UNIQUE_ID_CHARS).lstrip("@").strip()
    if not username:
        raise argparse.ArgumentTypeError("TikTok username is required")
    if not _USERNAME_RE.fullmatch(username):
        raise argparse.ArgumentTypeError(
            "TikTok username may contain letters, numbers, periods, and underscores"
        )
    return username


def _event_room_id(event: Any, data: Mapping) -> str:
    common = _mapping(data.get("common"))
    return _bounded_text(
        _first_nonempty(
            getattr(event, "room_id", ""),
            data.get("roomId"),
            data.get("room_id"),
            common.get("roomId"),
            common.get("room_id"),
            max_chars=MAX_ROOM_ID_CHARS,
        ),
        MAX_ROOM_ID_CHARS,
    )


def _event_message_id(data: Mapping) -> str:
    common = _mapping(data.get("common"))
    return _first_nonempty(
        common.get("msgId"),
        common.get("msg_id"),
        data.get("msgId"),
        data.get("msg_id"),
        common.get("logId"),
        common.get("log_id"),
        max_chars=120,
    )


def _event_source_time(data: Mapping) -> str:
    common = _mapping(data.get("common"))
    return _first_nonempty(
        common.get("createTime"),
        common.get("create_time"),
        common.get("clientSendTime"),
        common.get("client_send_time"),
        data.get("createTime"),
        data.get("create_time"),
        max_chars=80,
    )


def _source_at(data: Mapping) -> str:
    raw = _event_source_time(data)
    if not raw:
        return ""
    try:
        numeric = float(raw)
    except (TypeError, ValueError, OverflowError):
        return ""
    if numeric > 10**15:
        numeric /= 1_000_000.0
    elif numeric > 10**11:
        numeric /= 1_000.0
    if not (946684800 <= numeric <= 4102444800):
        return ""
    try:
        return datetime.fromtimestamp(numeric, tz=timezone.utc).isoformat().replace(
            "+00:00", "Z"
        )
    except (OSError, OverflowError, ValueError):
        return ""


def _moderator_flag(user: Mapping) -> bool:
    identity = _mapping(user.get("identity"))
    user_attr = _mapping(user.get("userAttr"))
    if not user_attr:
        user_attr = _mapping(user.get("user_attr"))
    return any(
        bool(value)
        for value in (
            identity.get("isModeratorOfAnchor"),
            identity.get("is_moderator_of_anchor"),
            user_attr.get("isAdmin"),
            user_attr.get("is_admin"),
            user_attr.get("isSuperAdmin"),
            user_attr.get("is_super_admin"),
        )
    )


def _user_fields(user: Mapping) -> Dict[str, Any]:
    unique_id = _bounded_text(
        _first_nonempty(
            user.get("uniqueId"),
            user.get("unique_id"),
            max_chars=MAX_UNIQUE_ID_CHARS,
        ),
        MAX_UNIQUE_ID_CHARS,
    ).lstrip("@")
    display_name = _bounded_text(
        _first_nonempty(
            user.get("nickname"),
            unique_id,
            "TikTok viewer",
            max_chars=MAX_DISPLAY_NAME_CHARS,
        ),
        MAX_DISPLAY_NAME_CHARS,
    )
    return {
        "unique_id": unique_id,
        "display_name": display_name,
        "moderator_flag": _moderator_flag(user),
    }


def _fallback_event_id(
    event_type: str,
    room_id: str,
    parts: Sequence[Any],
    source_marker: str,
) -> str:
    material = "\x1f".join(
        [event_type, room_id, *(_bounded_text(part, 1000) for part in parts), source_marker]
    ).encode("utf-8", errors="replace")
    return "local:{}".format(hashlib.sha256(material).hexdigest()[:32])


def _observation_base(
    event_type: str,
    event: Any,
    data: Mapping,
    fallback_parts: Sequence[Any],
) -> Dict[str, Any]:
    room_id = _event_room_id(event, data)
    observed_at = _utc_now()
    source_at = _source_at(data)
    message_id = _bounded_text(_event_message_id(data), 120)
    event_id = (
        "tiktok:{}:{}".format(room_id or "room", message_id)
        if message_id
        else _fallback_event_id(
            event_type,
            room_id,
            fallback_parts,
            _event_source_time(data) or source_at or observed_at,
        )
    )
    payload: Dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "event_type": event_type,
        "event_id": event_id,
        "room_id": room_id,
        "observed_at": observed_at,
    }
    if source_at:
        payload["source_at"] = source_at
    return payload


def build_lifecycle_payload(event_type: str, event: Any) -> Dict[str, Any]:
    data = _mapping(getattr(event, "data", {}))
    room_id = _event_room_id(event, data)
    payload: Dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "event_type": event_type,
        "event_id": "{}:{}:{}".format(
            event_type, room_id or "room", datetime.now(timezone.utc).timestamp()
        ),
        "room_id": room_id,
        "observed_at": _utc_now(),
    }
    source_at = _source_at(data)
    if source_at:
        payload["source_at"] = source_at
    if event_type == "reconnecting":
        payload.update(
            {
                "attempt": max(0, _coerce_int(data.get("attempt"))),
                "max_retries": max(0, _coerce_int(data.get("max_retries"))),
                "delay_seconds": max(0, _coerce_int(data.get("delay"))),
            }
        )
    return payload


def build_transport_error_payload(error: Any) -> Dict[str, Any]:
    error_code = _bounded_code(
        error.__class__.__name__ if isinstance(error, BaseException) else error,
        "transport_error",
    )
    observed_at = _utc_now()
    return {
        "schema_version": SCHEMA_VERSION,
        "event_type": "transport_error",
        "event_id": "transport_error:{}".format(observed_at),
        "room_id": "",
        "observed_at": observed_at,
        "error_code": error_code,
    }


def emit_payload(payload: Mapping) -> None:
    sys.stdout.write(json.dumps(dict(payload), separators=(",", ":"), sort_keys=True))
    sys.stdout.write("\n")
    sys.stdout.flush()


def emit_diagnostic(code: str) -> None:
    sys.stderr.write(_bounded_code(code, "transport_diagnostic") + "\n")
    sys.stderr.flush()


