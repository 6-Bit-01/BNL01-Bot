"""Ephemeral, read-only TikTok LIVE chat boundary for BNL.

The direct Webcast client lives in an isolated Python 3.11+ process. This
standard-library-only module stays compatible with the bot's Python 3.9/3.12
runtime, validates the transport's NDJSON, and keeps comments in bounded RAM.
It does not start the collector, call Gemini, write a database, or post output.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import time
from collections import OrderedDict, deque
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Deque, Dict, List, Optional, Union

SCHEMA_VERSION = 1
SOURCE = "tiktok_live_webcast"
VISIBILITY = "public_observation"
AUTHORITY = "viewer_statement"
LIFECYCLE = "current_show_only"
MEMORY_DEFAULT = "do_not_store"
IDENTITY_DEFAULT = "tiktok_only_unlinked"

COMMENT = "comment"
LIFECYCLE_EVENTS = frozenset(
    {"connected", "reconnecting", "disconnected", "live_ended", "transport_error"}
)
ALLOWED_EVENTS = frozenset({COMMENT, *LIFECYCLE_EVENTS})

MAX_LINE_BYTES = 32 * 1024
MAX_COMMENT_CHARS = 1000
MAX_TEXT_CHARS = 160
MAX_CLOCK_SKEW_SECONDS = 5 * 60
_CONTROL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
_SPACE_RE = re.compile(r"\s+")
_CODE_RE = re.compile(r"[^A-Za-z0-9_.:-]+")
_HANDLE_RE = re.compile(r"^[A-Za-z0-9._]+$")


class ProtocolError(ValueError):
    """Bounded parse failure that never includes the raw transport line."""

    def __init__(self, code: str) -> None:
        self.code = _code(code, "protocol_error")
        super().__init__(self.code)


@dataclass(frozen=True)
class LiveEvent:
    event_type: str
    event_id: str
    room_id: str
    observed_at: float
    unique_id: str = ""
    display_name: str = ""
    comment_text: str = ""
    moderator_flag: bool = False
    error_code: str = ""

    @property
    def is_comment(self) -> bool:
        return self.event_type == COMMENT

    def context_record(self) -> Dict[str, Any]:
        if not self.is_comment:
            raise ValueError("only comments can become context")
        return {
            "event_id": self.event_id,
            "room_id": self.room_id,
            "observed_at": self.observed_at,
            "unique_id": self.unique_id,
            "display_name": self.display_name,
            "comment_text": self.comment_text,
            "moderator_flag": self.moderator_flag,
            "source": SOURCE,
            "visibility": VISIBILITY,
            "authority": AUTHORITY,
            "lifecycle": LIFECYCLE,
            "memory_default": MEMORY_DEFAULT,
            "identity_default": IDENTITY_DEFAULT,
        }


class LiveChatBuffer:
    """Current-show ring buffer with replay deduplication."""

    def __init__(
        self,
        max_events: int = 1000,
        max_age_seconds: float = 15 * 60,
        time_fn: Callable[[], float] = time.time,
    ) -> None:
        if max_events <= 0 or max_age_seconds <= 0:
            raise ValueError("buffer limits must be positive")
        self.max_events = int(max_events)
        self.max_age_seconds = float(max_age_seconds)
        self.time_fn = time_fn
        self.events: Deque[LiveEvent] = deque()
        self.seen: "OrderedDict[str, float]" = OrderedDict()
        self.duplicates = 0
        self.expired = 0
        self.overflow = 0

    def clear(self) -> None:
        self.events.clear()
        self.seen.clear()

    def prune(self, now: Optional[float] = None) -> None:
        current = self.time_fn() if now is None else float(now)
        cutoff = current - self.max_age_seconds
        while self.events and self.events[0].observed_at < cutoff:
            self.events.popleft()
            self.expired += 1
        while self.seen and next(iter(self.seen.values())) < cutoff:
            self.seen.popitem(last=False)

    def add(self, event: LiveEvent, now: Optional[float] = None) -> bool:
        if not event.is_comment:
            return False
        current = self.time_fn() if now is None else float(now)
        self.prune(current)
        if event.event_id in self.seen:
            self.duplicates += 1
            return False
        self.seen[event.event_id] = current
        self.events.append(event)
        while len(self.events) > self.max_events:
            self.events.popleft()
            self.overflow += 1
        return True

    def snapshot(
        self,
        window_seconds: float = 5 * 60,
        limit: int = 100,
        now: Optional[float] = None,
    ) -> List[LiveEvent]:
        current = self.time_fn() if now is None else float(now)
        self.prune(current)
        if window_seconds <= 0 or limit <= 0:
            return []
        cutoff = current - float(window_seconds)
        return [event for event in self.events if event.observed_at >= cutoff][
            -int(limit) :
        ]


class LiveChatAdapter:
    """Protocol parser plus health counters; no persistence or generation."""

    def __init__(
        self,
        buffer: Optional[LiveChatBuffer] = None,
        time_fn: Callable[[], float] = time.time,
        clear_on_live_end: bool = True,
    ) -> None:
        self.time_fn = time_fn
        self.buffer = buffer if buffer is not None else LiveChatBuffer(time_fn=time_fn)
        self.clear_on_live_end = clear_on_live_end
        self.health: Dict[str, Any] = {
            "state": "stopped",
            "room_id": "",
            "last_event_at": None,
            "last_comment_at": None,
            "last_error_code": "",
            "received_lines": 0,
            "invalid_lines": 0,
            "comments_received": 0,
            "reconnect_count": 0,
            "transport_error_count": 0,
        }

    def ingest_line(self, line: Union[str, bytes]) -> Optional[LiveEvent]:
        self.health["received_lines"] += 1
        try:
            event = parse_line(line, now=self.time_fn())
        except ProtocolError as exc:
            self.health["invalid_lines"] += 1
            self.health["last_error_code"] = exc.code
            return None
        self.ingest_event(event)
        return event

    def ingest_event(self, event: LiveEvent) -> bool:
        now = self.time_fn()
        old_room = self.health["room_id"]
        if event.room_id and old_room and event.room_id != old_room:
            self.buffer.clear()
        if event.room_id:
            self.health["room_id"] = event.room_id
        self.health["last_event_at"] = now

        if event.event_type == "connected":
            self.health["state"] = "connected"
            self.health["last_error_code"] = ""
        elif event.event_type == "reconnecting":
            self.health["state"] = "reconnecting"
            self.health["reconnect_count"] += 1
        elif event.event_type == "disconnected":
            if self.health["state"] != "ended":
                self.health["state"] = "disconnected"
        elif event.event_type == "live_ended":
            self.health["state"] = "ended"
            if self.clear_on_live_end:
                self.buffer.clear()
        elif event.event_type == "transport_error":
            self.health["state"] = "error"
            self.health["transport_error_count"] += 1
            self.health["last_error_code"] = event.error_code or "transport_error"
        elif event.is_comment:
            self.health["comments_received"] += 1
            self.health["last_comment_at"] = now
            return self.buffer.add(event, now=now)
        return False

    def context_snapshot(self, window_seconds: float = 300, limit: int = 100):
        return [
            event.context_record()
            for event in self.buffer.snapshot(window_seconds, limit, self.time_fn())
        ]

    def health_snapshot(self) -> Dict[str, Any]:
        self.buffer.prune(self.time_fn())
        return {
            **self.health,
            "comments_buffered": len(self.buffer.events),
            "duplicate_count": self.buffer.duplicates,
            "expired_count": self.buffer.expired,
            "overflow_count": self.buffer.overflow,
            "source": SOURCE,
            "lifecycle": LIFECYCLE,
            "memory_default": MEMORY_DEFAULT,
        }


def parse_line(line: Union[str, bytes], now: Optional[float] = None) -> LiveEvent:
    current = time.time() if now is None else float(now)
    if isinstance(line, bytes):
        if len(line) > MAX_LINE_BYTES:
            raise ProtocolError("line_too_long")
        try:
            text = line.decode("utf-8")
        except UnicodeDecodeError:
            raise ProtocolError("invalid_utf8")
    elif isinstance(line, str):
        if len(line.encode("utf-8")) > MAX_LINE_BYTES:
            raise ProtocolError("line_too_long")
        text = line
    else:
        raise ProtocolError("invalid_line_type")

    try:
        payload = json.loads(text)
    except (json.JSONDecodeError, TypeError, ValueError):
        raise ProtocolError("invalid_json")
    if not isinstance(payload, Mapping):
        raise ProtocolError("payload_not_object")
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise ProtocolError("unsupported_schema_version")

    event_type = _code(payload.get("event_type"), "")
    if event_type not in ALLOWED_EVENTS:
        raise ProtocolError("unsupported_event_type")
    observed_at = _timestamp(payload.get("observed_at"), current)
    room_id = _text(payload.get("room_id"), 80)
    event_id = _text(payload.get("event_id"), MAX_TEXT_CHARS)

    if event_type == COMMENT:
        unique_id = _text(payload.get("unique_id"), 80).lstrip("@")
        if unique_id and not _HANDLE_RE.fullmatch(unique_id):
            raise ProtocolError("invalid_unique_id")
        display_name = _text(payload.get("display_name"), 120)
        comment_text = _text(payload.get("comment_text"), MAX_COMMENT_CHARS)
        if not comment_text:
            raise ProtocolError("empty_comment")
        event_id = event_id or _fallback_id(
            event_type, room_id, unique_id, comment_text, observed_at
        )
        return LiveEvent(
            event_type=event_type,
            event_id=event_id,
            room_id=room_id,
            observed_at=observed_at,
            unique_id=unique_id,
            display_name=display_name or unique_id or "TikTok viewer",
            comment_text=comment_text,
            moderator_flag=_bool(payload.get("moderator_flag")),
        )

    event_id = event_id or _fallback_id(event_type, room_id, "", "", observed_at)
    return LiveEvent(
        event_type=event_type,
        event_id=event_id,
        room_id=room_id,
        observed_at=observed_at,
        error_code=(
            _code(payload.get("error_code"), "transport_error")
            if event_type == "transport_error"
            else ""
        ),
    )


def _text(value: Any, limit: int) -> str:
    if not isinstance(value, (str, int, float, bool)):
        return ""
    text = _SPACE_RE.sub(" ", _CONTROL_RE.sub(" ", str(value))).strip()
    return text[:limit].rstrip()


def _code(value: Any, fallback: str) -> str:
    return _CODE_RE.sub("_", _text(value, 80)).strip("_") or fallback


def _bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return isinstance(value, str) and value.lower().strip() in {"1", "true", "yes"}


def _timestamp(value: Any, now: float) -> float:
    parsed: Optional[float] = None
    try:
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            parsed = float(value)
        elif isinstance(value, str):
            text = value.strip().replace("Z", "+00:00")
            try:
                parsed = float(text)
            except ValueError:
                dt = datetime.fromisoformat(text)
                parsed = (dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)).timestamp()
    except (TypeError, ValueError, OverflowError):
        parsed = None
    if parsed is None or not math.isfinite(parsed):
        return now
    return parsed if abs(parsed - now) <= MAX_CLOCK_SKEW_SECONDS else now


def _fallback_id(
    event_type: str,
    room_id: str,
    unique_id: str,
    comment_text: str,
    observed_at: float,
) -> str:
    raw = "\x1f".join(
        [event_type, room_id, unique_id, comment_text, "{:.3f}".format(observed_at)]
    ).encode("utf-8", errors="replace")
    return "local:" + hashlib.sha256(raw).hexdigest()[:32]
