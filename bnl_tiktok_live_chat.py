"""Read-only TikTok LIVE public telemetry boundary for BNL.

The direct Webcast client lives in an isolated Python 3.11+ process. This
standard-library-only module stays compatible with the bot's Python 3.9/3.12
runtime, validates the transport's NDJSON, and keeps current-show observations
in bounded RAM. A separate handoff may archive public comments/questions through
BNL's normal memory owners. This module does not start the collector, call
Gemini, write a database, or post output.
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
COMMENT_AUTHORITY = "viewer_statement"
INTERACTION_AUTHORITY = "public_interaction_event"
METRIC_AUTHORITY = "platform_room_metric"
AUTHORITY = COMMENT_AUTHORITY  # compatibility alias for comment context
LIFECYCLE = "current_show_only"
MEMORY_DEFAULT = "source_aware"
PUBLIC_TEXT_MEMORY = "durable_public_conversation"
METRIC_MEMORY = "current_show_only"
IDENTITY_DEFAULT = "handle_display_correlated_v1"

COMMENT = "comment"
LIKE = "like"
VIEWER_SNAPSHOT = "viewer_snapshot"
SHARE = "share"
FOLLOW = "follow"
GIFT = "gift"
QUESTION = "question"
JOIN = "join"

OBSERVATION_EVENTS = frozenset(
    {COMMENT, LIKE, VIEWER_SNAPSHOT, SHARE, FOLLOW, GIFT, QUESTION, JOIN}
)
RETAINED_EVENTS = frozenset(
    {COMMENT, LIKE, VIEWER_SNAPSHOT, SHARE, FOLLOW, GIFT, QUESTION}
)
LIFECYCLE_EVENTS = frozenset(
    {"connected", "reconnecting", "disconnected", "live_ended", "transport_error"}
)
ALLOWED_EVENTS = frozenset({*OBSERVATION_EVENTS, *LIFECYCLE_EVENTS})

MAX_LINE_BYTES = 32 * 1024
MAX_COMMENT_CHARS = 1000
MAX_QUESTION_CHARS = 1000
MAX_TEXT_CHARS = 160
MAX_CLOCK_SKEW_SECONDS = 5 * 60
MAX_SOURCE_CLOCK_SKEW_SECONDS = 24 * 60 * 60
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
    source_at: float = 0.0
    unique_id: str = ""
    display_name: str = ""
    comment_text: str = ""
    moderator_flag: bool = False
    like_count: int = 0
    like_total: int = 0
    viewer_count: int = 0
    share_type: int = 0
    gift_id: int = 0
    gift_name: str = ""
    gift_count: int = 0
    diamond_count: int = 0
    diamond_total: int = 0
    combo: bool = False
    streak_over: bool = False
    question_id: str = ""
    question_text: str = ""
    answer_status: int = 0
    join_count: int = 0
    error_code: str = ""

    @property
    def is_comment(self) -> bool:
        return self.event_type == COMMENT

    @property
    def is_observation(self) -> bool:
        return self.event_type in OBSERVATION_EVENTS

    @property
    def event_time(self) -> float:
        return self.source_at or self.observed_at

    @property
    def authority(self) -> str:
        if self.event_type in {COMMENT, QUESTION}:
            return COMMENT_AUTHORITY
        if self.event_type == VIEWER_SNAPSHOT:
            return METRIC_AUTHORITY
        return INTERACTION_AUTHORITY

    def context_record(self) -> Dict[str, Any]:
        if not self.is_comment:
            raise ValueError("only comments can become conversation context")
        return {
            "event_id": self.event_id,
            "room_id": self.room_id,
            "observed_at": self.observed_at,
            "source_at": self.source_at or None,
            "unique_id": self.unique_id,
            "display_name": self.display_name,
            "comment_text": self.comment_text,
            "moderator_flag": self.moderator_flag,
            "source": SOURCE,
            "visibility": VISIBILITY,
            "authority": COMMENT_AUTHORITY,
            "lifecycle": LIFECYCLE,
            "memory_default": PUBLIC_TEXT_MEMORY,
            "identity_default": IDENTITY_DEFAULT,
        }

    def telemetry_record(self) -> Dict[str, Any]:
        if not self.is_observation:
            raise ValueError("only public observations can become telemetry")
        record: Dict[str, Any] = {
            "event_type": self.event_type,
            "event_id": self.event_id,
            "room_id": self.room_id,
            "observed_at": self.observed_at,
            "source_at": self.source_at or None,
            "source": SOURCE,
            "visibility": VISIBILITY,
            "authority": self.authority,
            "lifecycle": LIFECYCLE,
            "memory_default": (
                PUBLIC_TEXT_MEMORY
                if self.event_type in {COMMENT, QUESTION}
                else METRIC_MEMORY
            ),
            "identity_default": IDENTITY_DEFAULT,
        }
        if self.unique_id:
            record.update(
                {
                    "unique_id": self.unique_id,
                    "display_name": self.display_name,
                    "moderator_flag": self.moderator_flag,
                }
            )
        if self.event_type == COMMENT:
            record["comment_text"] = self.comment_text
        elif self.event_type == LIKE:
            record.update({"like_count": self.like_count, "like_total": self.like_total})
        elif self.event_type == VIEWER_SNAPSHOT:
            record["viewer_count"] = self.viewer_count
        elif self.event_type == SHARE:
            record["share_type"] = self.share_type
        elif self.event_type == GIFT:
            record.update(
                {
                    "gift_id": self.gift_id,
                    "gift_name": self.gift_name,
                    "gift_count": self.gift_count,
                    "diamond_count": self.diamond_count,
                    "diamond_total": self.diamond_total,
                    "combo": self.combo,
                    "streak_over": self.streak_over,
                }
            )
        elif self.event_type == QUESTION:
            record.update(
                {
                    "question_id": self.question_id,
                    "question_text": self.question_text,
                    "answer_status": self.answer_status,
                }
            )
        elif self.event_type == JOIN:
            record["join_count"] = self.join_count
        return record


class LiveChatBuffer:
    """Current-show ring buffer with replay deduplication for public telemetry."""

    def __init__(
        self,
        max_events: int = 1000,
        max_age_seconds: float = 15 * 60,
        time_fn: Callable[[], float] = time.time,
        max_seen_events: Optional[int] = None,
    ) -> None:
        resolved_max_seen = (
            int(max_events) * 4 if max_seen_events is None else int(max_seen_events)
        )
        if max_events <= 0 or max_age_seconds <= 0 or resolved_max_seen <= 0:
            raise ValueError("buffer limits must be positive")
        self.max_events = int(max_events)
        self.max_age_seconds = float(max_age_seconds)
        self.max_seen_events = resolved_max_seen
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
        if not event.is_observation:
            return False
        current = self.time_fn() if now is None else float(now)
        self.prune(current)
        if event.event_id in self.seen:
            self.seen.move_to_end(event.event_id)
            self.duplicates += 1
            return False
        self.seen[event.event_id] = current
        while len(self.seen) > self.max_seen_events:
            self.seen.popitem(last=False)
        if event.event_type in RETAINED_EVENTS:
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
        event_types: Optional[frozenset] = None,
    ) -> List[LiveEvent]:
        current = self.time_fn() if now is None else float(now)
        self.prune(current)
        if window_seconds <= 0 or limit <= 0:
            return []
        cutoff = current - float(window_seconds)
        return [
            event
            for event in self.events
            if event.observed_at >= cutoff
            and (event_types is None or event.event_type in event_types)
        ][-int(limit) :]


class LiveChatAdapter:
    """Protocol parser plus bounded health/telemetry counters; no persistence."""

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
            "last_signal_at": None,
            "last_error_code": "",
            "received_lines": 0,
            "invalid_lines": 0,
            "events_received": 0,
            "events_accepted": 0,
            "comments_received": 0,
            "comments_accepted": 0,
            "like_events": 0,
            "taps_observed": 0,
            "latest_like_total": 0,
            "viewer_snapshots": 0,
            "viewer_count": 0,
            "peak_viewers": 0,
            "shares": 0,
            "follows": 0,
            "gift_events": 0,
            "gift_units": 0,
            "diamond_total": 0,
            "questions": 0,
            "joins": 0,
            "reconnect_count": 0,
            "transport_error_count": 0,
        }

    def ingest_line(self, line: Union[str, bytes]) -> Optional[LiveEvent]:
        """Parse one transport line and return only accepted events.

        Invalid lines and replayed public observations return ``None``.
        Lifecycle events are returned after their health state is applied.
        """

        self.health["received_lines"] += 1
        try:
            event = parse_line(line, now=self.time_fn())
        except ProtocolError as exc:
            self.health["invalid_lines"] += 1
            self.health["last_error_code"] = exc.code
            return None
        accepted = self.ingest_event(event)
        if event.is_observation and not accepted:
            return None
        return event

    def ingest_event(self, event: LiveEvent) -> bool:
        now = self.time_fn()
        old_room = self.health["room_id"]
        if event.room_id and old_room and event.room_id != old_room:
            self.buffer.clear()
            self.health["viewer_count"] = 0
            self.health["latest_like_total"] = 0
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
        elif event.is_observation:
            self.health["events_received"] += 1
            if event.is_comment:
                self.health["comments_received"] += 1
            accepted = self.buffer.add(event, now=now)
            if not accepted:
                return False
            self.health["events_accepted"] += 1
            self.health["last_signal_at"] = now
            if event.event_type == COMMENT:
                self.health["comments_accepted"] += 1
                self.health["last_comment_at"] = now
            elif event.event_type == LIKE:
                self.health["like_events"] += 1
                self.health["taps_observed"] += event.like_count
                if event.like_total:
                    self.health["latest_like_total"] = event.like_total
            elif event.event_type == VIEWER_SNAPSHOT:
                self.health["viewer_snapshots"] += 1
                self.health["viewer_count"] = event.viewer_count
                self.health["peak_viewers"] = max(
                    self.health["peak_viewers"], event.viewer_count
                )
            elif event.event_type == SHARE:
                self.health["shares"] += 1
            elif event.event_type == FOLLOW:
                self.health["follows"] += 1
            elif event.event_type == GIFT:
                self.health["gift_events"] += 1
                self.health["gift_units"] += event.gift_count
                self.health["diamond_total"] += event.diamond_total
            elif event.event_type == QUESTION:
                self.health["questions"] += 1
            elif event.event_type == JOIN:
                self.health["joins"] += max(1, event.join_count)
            return True
        return False

    def context_snapshot(self, window_seconds: float = 300, limit: int = 100):
        return [
            event.context_record()
            for event in self.buffer.snapshot(
                window_seconds,
                limit,
                self.time_fn(),
                event_types=frozenset({COMMENT}),
            )
        ]

    def telemetry_snapshot(self, window_seconds: float = 300, limit: int = 500):
        return [
            event.telemetry_record()
            for event in self.buffer.snapshot(
                window_seconds,
                limit,
                self.time_fn(),
                event_types=RETAINED_EVENTS,
            )
        ]

    def health_snapshot(self) -> Dict[str, Any]:
        self.buffer.prune(self.time_fn())
        return {
            **self.health,
            "events_buffered": len(self.buffer.events),
            "comments_buffered": sum(
                1 for event in self.buffer.events if event.event_type == COMMENT
            ),
            "seen_event_ids": len(self.buffer.seen),
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
    source_at = _optional_timestamp(
        payload.get("source_at"), observed_at, MAX_SOURCE_CLOCK_SKEW_SECONDS
    )
    room_id = _text(payload.get("room_id"), 80)
    event_id = _text(payload.get("event_id"), MAX_TEXT_CHARS)

    if event_type in OBSERVATION_EVENTS:
        unique_id, display_name, moderator_flag = _identity_fields(payload)
        kwargs: Dict[str, Any] = {
            "event_type": event_type,
            "event_id": event_id,
            "room_id": room_id,
            "observed_at": observed_at,
            "source_at": source_at,
            "unique_id": unique_id,
            "display_name": display_name,
            "moderator_flag": moderator_flag,
        }

        if event_type == COMMENT:
            comment_text = _text(payload.get("comment_text"), MAX_COMMENT_CHARS)
            if not comment_text:
                raise ProtocolError("empty_comment")
            kwargs["comment_text"] = comment_text
        elif event_type == LIKE:
            like_count = _nonnegative_int(payload.get("like_count"), 10**9)
            like_total = _nonnegative_int(payload.get("like_total"), 10**12)
            if like_count <= 0 and like_total <= 0:
                raise ProtocolError("empty_like_event")
            kwargs.update({"like_count": like_count, "like_total": like_total})
        elif event_type == VIEWER_SNAPSHOT:
            kwargs["viewer_count"] = _nonnegative_int(
                payload.get("viewer_count"), 10**9
            )
        elif event_type == SHARE:
            kwargs["share_type"] = _nonnegative_int(payload.get("share_type"), 1000)
        elif event_type == GIFT:
            gift_count = max(1, _nonnegative_int(payload.get("gift_count"), 10**9))
            kwargs.update(
                {
                    "gift_id": _nonnegative_int(payload.get("gift_id"), 10**12),
                    "gift_name": _text(payload.get("gift_name"), 160) or "Gift",
                    "gift_count": gift_count,
                    "diamond_count": _nonnegative_int(
                        payload.get("diamond_count"), 10**9
                    ),
                    "diamond_total": _nonnegative_int(
                        payload.get("diamond_total"), 10**12
                    ),
                    "combo": _bool(payload.get("combo")),
                    "streak_over": _bool(payload.get("streak_over")),
                }
            )
        elif event_type == QUESTION:
            question_text = _text(payload.get("question_text"), MAX_QUESTION_CHARS)
            if not question_text:
                raise ProtocolError("empty_question")
            kwargs.update(
                {
                    "question_id": _text(payload.get("question_id"), 120),
                    "question_text": question_text,
                    "answer_status": _nonnegative_int(
                        payload.get("answer_status"), 1000
                    ),
                }
            )
        elif event_type == JOIN:
            kwargs["join_count"] = max(
                1, _nonnegative_int(payload.get("join_count"), 10**6)
            )

        if not event_id:
            kwargs["event_id"] = _fallback_observation_id(
                event_type, room_id, payload, source_at or observed_at
            )
        return LiveEvent(**kwargs)

    event_id = event_id or _fallback_id(event_type, room_id, "", "", observed_at)
    return LiveEvent(
        event_type=event_type,
        event_id=event_id,
        room_id=room_id,
        observed_at=observed_at,
        source_at=source_at,
        error_code=(
            _code(payload.get("error_code"), "transport_error")
            if event_type == "transport_error"
            else ""
        ),
    )


def _identity_fields(payload: Mapping) -> tuple:
    unique_id = _text(payload.get("unique_id"), 80).lstrip("@")
    if unique_id and not _HANDLE_RE.fullmatch(unique_id):
        raise ProtocolError("invalid_unique_id")
    display_name = _text(payload.get("display_name"), 120)
    if unique_id or display_name:
        display_name = display_name or unique_id or "TikTok viewer"
    return unique_id, display_name, _bool(payload.get("moderator_flag"))


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


def _nonnegative_int(value: Any, maximum: int) -> int:
    if isinstance(value, bool):
        return 0
    try:
        numeric = int(value)
    except (TypeError, ValueError, OverflowError):
        return 0
    return min(maximum, max(0, numeric))


def _timestamp(value: Any, now: float) -> float:
    parsed = _parse_timestamp(value)
    if parsed is None or not math.isfinite(parsed):
        return now
    return parsed if abs(parsed - now) <= MAX_CLOCK_SKEW_SECONDS else now


def _optional_timestamp(value: Any, reference: float, max_skew: float) -> float:
    parsed = _parse_timestamp(value)
    if parsed is None or not math.isfinite(parsed):
        return 0.0
    return parsed if abs(parsed - reference) <= max_skew else 0.0


def _parse_timestamp(value: Any) -> Optional[float]:
    try:
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            numeric = float(value)
            if numeric > 10**15:
                numeric /= 1_000_000.0
            elif numeric > 10**11:
                numeric /= 1_000.0
            return numeric
        if isinstance(value, str):
            text = value.strip().replace("Z", "+00:00")
            try:
                numeric = float(text)
                if numeric > 10**15:
                    numeric /= 1_000_000.0
                elif numeric > 10**11:
                    numeric /= 1_000.0
                return numeric
            except ValueError:
                dt = datetime.fromisoformat(text)
                return (dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)).timestamp()
    except (TypeError, ValueError, OverflowError, OSError):
        return None
    return None


def _fallback_observation_id(
    event_type: str,
    room_id: str,
    payload: Mapping,
    event_time: float,
) -> str:
    safe: Dict[str, Any] = {
        "event_type": event_type,
        "room_id": room_id,
        "unique_id": _text(payload.get("unique_id"), 80),
        "event_time": "{:.3f}".format(event_time),
    }
    for key in (
        "comment_text",
        "like_count",
        "like_total",
        "viewer_count",
        "share_type",
        "gift_id",
        "gift_count",
        "diamond_total",
        "question_id",
        "question_text",
        "join_count",
    ):
        if key in payload:
            safe[key] = payload.get(key)
    raw = json.dumps(safe, sort_keys=True, separators=(",", ":")).encode(
        "utf-8", errors="replace"
    )
    return "local:" + hashlib.sha256(raw).hexdigest()[:32]


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
