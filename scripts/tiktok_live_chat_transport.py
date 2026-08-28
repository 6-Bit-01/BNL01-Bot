#!/usr/bin/env python3
"""Emit a read-only TikTok LIVE comment stream as versioned NDJSON.

This script is intentionally isolated from the main BNL environment. It imports
``piratetok_live`` only after argument validation, listens only for comments and
connection lifecycle events, writes protocol objects to stdout, and writes only
bounded diagnostic codes to stderr. It cannot post, moderate, gift, follow, or
change BARCODE show state.
"""

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
from typing import Any, Dict, Optional

SCHEMA_VERSION = 1
MAX_ROOM_ID_CHARS = 80
MAX_UNIQUE_ID_CHARS = 80
MAX_DISPLAY_NAME_CHARS = 120
MAX_COMMENT_CHARS = 1000
MAX_ERROR_CODE_CHARS = 80
DEFAULT_DEDUPE_CAPACITY = 20_000

_CONTROL_CHAR_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
_WHITESPACE_RE = re.compile(r"\s+")
_SAFE_CODE_RE = re.compile(r"[^A-Za-z0-9_.:-]+")
_USERNAME_RE = re.compile(r"^[A-Za-z0-9._]+$")


class EventIdDeduper:
    """Bound replay suppression without retaining comment text."""

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


def _first_nonempty(*values: Any) -> str:
    for value in values:
        text = _bounded_text(value, MAX_DISPLAY_NAME_CHARS)
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
    )


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


def _fallback_event_id(
    room_id: str,
    unique_id: str,
    comment_text: str,
    source_marker: str,
) -> str:
    material = "\x1f".join(
        [room_id, unique_id, comment_text, source_marker]
    ).encode("utf-8", errors="replace")
    return "local:{}".format(hashlib.sha256(material).hexdigest()[:32])


def build_comment_payload(event: Any) -> Optional[Dict[str, Any]]:
    data = _mapping(getattr(event, "data", {}))
    user = _mapping(data.get("user"))
    comment_text = _bounded_text(data.get("content"), MAX_COMMENT_CHARS)
    if not comment_text:
        return None

    unique_id = _bounded_text(
        _first_nonempty(user.get("uniqueId"), user.get("unique_id")),
        MAX_UNIQUE_ID_CHARS,
    ).lstrip("@")
    display_name = _bounded_text(
        _first_nonempty(user.get("nickname"), unique_id, "TikTok viewer"),
        MAX_DISPLAY_NAME_CHARS,
    )
    room_id = _event_room_id(event, data)
    observed_at = _utc_now()
    message_id = _bounded_text(_event_message_id(data), 120)
    event_id = (
        "tiktok:{}:{}".format(room_id or "room", message_id)
        if message_id
        else _fallback_event_id(
            room_id,
            unique_id,
            comment_text,
            _event_source_time(data) or observed_at,
        )
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "event_type": "comment",
        "event_id": event_id,
        "room_id": room_id,
        "observed_at": observed_at,
        "unique_id": unique_id,
        "display_name": display_name,
        "comment_text": comment_text,
        "moderator_flag": _moderator_flag(user),
    }


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


def run_transport(args: argparse.Namespace) -> int:
    try:
        from piratetok_live import EventType, TikTokLiveClient
    except ImportError:
        emit_payload(build_transport_error_payload("missing_dependency"))
        emit_diagnostic("missing_dependency")
        return 2

    client = TikTokLiveClient(args.username)
    if args.cdn == "us":
        client.cdn_us()
    elif args.cdn == "eu":
        client.cdn_eu()
    client.max_retries(args.max_retries)
    client.stale_timeout(args.stale_timeout)

    comment_deduper = EventIdDeduper(args.dedupe_capacity)
    stream_ended = False
    disconnected_emitted = False

    @client.on(EventType.connected)
    def on_connected(event: Any) -> None:
        if not stream_ended:
            emit_payload(build_lifecycle_payload("connected", event))

    @client.on(EventType.chat)
    def on_chat(event: Any) -> None:
        if stream_ended:
            return
        payload = build_comment_payload(event)
        if payload is not None and comment_deduper.accept(payload["event_id"]):
            emit_payload(payload)

    @client.on(EventType.reconnecting)
    def on_reconnecting(event: Any) -> None:
        if not stream_ended:
            emit_payload(build_lifecycle_payload("reconnecting", event))

    @client.on(EventType.live_ended)
    def on_live_ended(event: Any) -> None:
        nonlocal stream_ended
        if stream_ended:
            return
        stream_ended = True
        emit_payload(build_lifecycle_payload("live_ended", event))
        client.disconnect()

    @client.on(EventType.disconnected)
    def on_disconnected(event: Any) -> None:
        nonlocal disconnected_emitted
        if disconnected_emitted:
            return
        disconnected_emitted = True
        emit_payload(build_lifecycle_payload("disconnected", event))

    @client.on("error")
    def on_error(event: Any) -> None:
        if not stream_ended:
            emit_payload(build_transport_error_payload(getattr(event, "data", event)))

    def request_stop(_signum: int, _frame: Any) -> None:
        emit_diagnostic("stop_requested")
        client.disconnect()

    for signal_name in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(signal_name, request_stop)
        except (AttributeError, OSError, ValueError):
            pass

    try:
        client.run()
    except KeyboardInterrupt:
        client.disconnect()
        return 130
    except Exception as exc:
        emit_payload(build_transport_error_payload(exc))
        emit_diagnostic(exc.__class__.__name__)
        return 1
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Read TikTok LIVE comments and emit BARCODE shadow NDJSON"
    )
    parser.add_argument("--username", required=True, type=normalize_username)
    parser.add_argument(
        "--cdn",
        choices=("default", "us", "eu"),
        default="us",
        help="TikTok Webcast CDN preference",
    )
    parser.add_argument("--max-retries", type=int, default=5)
    parser.add_argument("--stale-timeout", type=float, default=60.0)
    parser.add_argument(
        "--dedupe-capacity",
        type=int,
        default=DEFAULT_DEDUPE_CAPACITY,
        help="Maximum recent TikTok message IDs retained for replay suppression",
    )
    return parser


def main(argv: Optional[list] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.max_retries < 0 or args.max_retries > 100:
        parser.error("--max-retries must be between 0 and 100")
    if args.stale_timeout < 10 or args.stale_timeout > 600:
        parser.error("--stale-timeout must be between 10 and 600 seconds")
    if args.dedupe_capacity < 100 or args.dedupe_capacity > 100_000:
        parser.error("--dedupe-capacity must be between 100 and 100000")
    return run_transport(args)


if __name__ == "__main__":
    raise SystemExit(main())
