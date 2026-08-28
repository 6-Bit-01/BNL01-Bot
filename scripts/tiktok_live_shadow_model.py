"""Schedule, formatting, and immutable models for TikTok LIVE shadow monitoring."""

from __future__ import annotations

import argparse
import asyncio
import os
import re
import signal
import sys
from dataclasses import dataclass
from datetime import datetime, time as clock_time, timedelta
from pathlib import Path
from typing import Optional, Sequence, Set
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from bnl_tiktok_live_chat import (  # noqa: E402
    FOLLOW,
    GIFT,
    JOIN,
    LIKE,
    QUESTION,
    SHARE,
    VIEWER_SNAPSHOT,
    LiveChatAdapter,
    LiveChatBuffer,
    LiveEvent,
)

DEFAULT_TIMEZONE = "America/Los_Angeles"
DEFAULT_WEEKDAY = 4  # Monday=0, Friday=4
DEFAULT_WINDOW_START = clock_time(18, 50)
DEFAULT_WINDOW_END = clock_time(2, 0)
DEFAULT_RETRY_SECONDS = 20.0
DEFAULT_BUFFER_EVENTS = 5000
DEFAULT_DEDUPE_SECONDS = 8 * 60 * 60

_CLOCK_RE = re.compile(r"^(?:[01]\d|2[0-3]):[0-5]\d$")
_USERNAME_RE = re.compile(r"^[A-Za-z0-9._]+$")
_SAFE_CODE_RE = re.compile(r"[^A-Za-z0-9_.:-]+")
_WEEKDAYS = {
    "monday": 0,
    "mon": 0,
    "tuesday": 1,
    "tue": 1,
    "wednesday": 2,
    "wed": 2,
    "thursday": 3,
    "thu": 3,
    "friday": 4,
    "fri": 4,
    "saturday": 5,
    "sat": 5,
    "sunday": 6,
    "sun": 6,
}


@dataclass(frozen=True)
class ActiveWindow:
    start: datetime
    end: datetime


@dataclass
class CycleState:
    saw_connected: bool = False
    saw_live_end: bool = False
    events_emitted: int = 0
    comments_emitted: int = 0
    duplicate_replays_suppressed: int = 0
    invalid_lines: int = 0
    last_viewer_count: Optional[int] = None


@dataclass(frozen=True)
class CycleResult:
    return_code: int
    state: CycleState
    stop_reason: str


def parse_clock(value: str) -> clock_time:
    text = str(value).strip()
    if not _CLOCK_RE.fullmatch(text):
        raise argparse.ArgumentTypeError("time must use 24-hour HH:MM format")
    hour, minute = (int(part) for part in text.split(":", 1))
    return clock_time(hour, minute)


def parse_weekday(value: str) -> int:
    text = str(value).strip().lower()
    if text.isdigit():
        numeric = int(text)
        if 0 <= numeric <= 6:
            return numeric
    if text in _WEEKDAYS:
        return _WEEKDAYS[text]
    raise argparse.ArgumentTypeError("weekday must be monday-sunday or 0-6")


def normalize_username(value: str) -> str:
    username = str(value).strip().lstrip("@")
    if not username or not _USERNAME_RE.fullmatch(username):
        raise argparse.ArgumentTypeError(
            "TikTok username may contain letters, numbers, periods, and underscores"
        )
    return username


def resolve_active_window(
    now: datetime,
    weekday: int,
    start_time: clock_time,
    end_time: clock_time,
) -> Optional[ActiveWindow]:
    """Return the current scheduled window, including a cross-midnight tail."""

    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    if start_time == end_time:
        raise ValueError("window start and end must differ")
    if not 0 <= weekday <= 6:
        raise ValueError("weekday must be between 0 and 6")

    local_date = now.date()
    local_clock = now.timetz().replace(tzinfo=None)

    if start_time < end_time:
        if now.weekday() != weekday or not (start_time <= local_clock < end_time):
            return None
        start = datetime.combine(local_date, start_time, tzinfo=now.tzinfo)
        end = datetime.combine(local_date, end_time, tzinfo=now.tzinfo)
        return ActiveWindow(start=start, end=end)

    next_weekday = (weekday + 1) % 7
    if now.weekday() == weekday and local_clock >= start_time:
        start = datetime.combine(local_date, start_time, tzinfo=now.tzinfo)
        end = datetime.combine(
            local_date + timedelta(days=1), end_time, tzinfo=now.tzinfo
        )
        return ActiveWindow(start=start, end=end)
    if now.weekday() == next_weekday and local_clock < end_time:
        start = datetime.combine(
            local_date - timedelta(days=1), start_time, tzinfo=now.tzinfo
        )
        end = datetime.combine(local_date, end_time, tzinfo=now.tzinfo)
        return ActiveWindow(start=start, end=end)
    return None


def _handle(event: LiveEvent) -> str:
    return event.unique_id or event.display_name or "viewer"


def format_event(event: LiveEvent, timezone: ZoneInfo) -> str:
    stamp = datetime.fromtimestamp(event.event_time, tz=timezone).strftime("%H:%M:%S")
    if event.is_comment:
        moderator = " [MOD]" if event.moderator_flag else ""
        return "{} @{}{}: {}".format(
            stamp, _handle(event), moderator, event.comment_text
        )
    if event.event_type == LIKE:
        total = " (total {:,})".format(event.like_total) if event.like_total else ""
        return "{} [TAPS] +{:,}{}".format(stamp, event.like_count, total)
    if event.event_type == VIEWER_SNAPSHOT:
        return "{} [VIEWERS] {:,}".format(stamp, event.viewer_count)
    if event.event_type == SHARE:
        return "{} [SHARE] @{} shared the LIVE".format(stamp, _handle(event))
    if event.event_type == FOLLOW:
        return "{} [FOLLOW] @{} followed".format(stamp, _handle(event))
    if event.event_type == GIFT:
        diamonds = (
            " · {:,} diamonds".format(event.diamond_total)
            if event.diamond_total
            else ""
        )
        return "{} [GIFT] @{} sent {} x{:,}{}".format(
            stamp,
            _handle(event),
            event.gift_name or "Gift",
            max(1, event.gift_count),
            diamonds,
        )
    if event.event_type == QUESTION:
        return "{} [QUESTION] @{}: {}".format(
            stamp, _handle(event), event.question_text
        )
    if event.event_type == JOIN:
        return "{} [JOIN] @{}".format(stamp, _handle(event))

    room_id = event.room_id or "-"
    suffix = ""
    if event.event_type == "transport_error" and event.error_code:
        suffix = " error={}".format(event.error_code)
    return "[{}] {} room={}{}".format(stamp, event.event_type, room_id, suffix)


def _safe_code(value: object, fallback: str = "transport_diagnostic") -> str:
    text = str(value).strip()[:120]
    return _SAFE_CODE_RE.sub("_", text).strip("_") or fallback


def build_transport_command(args: argparse.Namespace) -> list[str]:
    return [
        str(args.python),
        "-u",
        str(args.transport_script),
        "--username",
        args.username,
        "--cdn",
        args.cdn,
        "--max-retries",
        str(args.max_retries),
        "--stale-timeout",
        str(args.stale_timeout),
        "--dedupe-capacity",
        str(args.transport_dedupe_capacity),
    ]


