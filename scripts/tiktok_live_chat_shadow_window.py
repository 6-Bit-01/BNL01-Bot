#!/usr/bin/env python3
"""Supervise the read-only TikTok LIVE collector for one weekly show window.

The supervisor keeps replay suppression across child-process reconnects, stops
an ended Webcast connection immediately, and starts a fresh connection until
the configured show window closes. It writes only sanitized shadow output to
its terminal. It does not call Gemini, Discord, the website, a database, or any
show-control owner.
"""

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

from bnl_tiktok_live_chat import LiveChatAdapter, LiveChatBuffer, LiveEvent

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
    comments_emitted: int = 0
    duplicate_replays_suppressed: int = 0
    invalid_lines: int = 0


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
        end = datetime.combine(local_date + timedelta(days=1), end_time, tzinfo=now.tzinfo)
        return ActiveWindow(start=start, end=end)
    if now.weekday() == next_weekday and local_clock < end_time:
        start = datetime.combine(local_date - timedelta(days=1), start_time, tzinfo=now.tzinfo)
        end = datetime.combine(local_date, end_time, tzinfo=now.tzinfo)
        return ActiveWindow(start=start, end=end)
    return None


def format_event(event: LiveEvent, timezone: ZoneInfo) -> str:
    stamp = datetime.fromtimestamp(event.observed_at, tz=timezone).strftime("%H:%M:%S")
    if event.is_comment:
        handle = event.unique_id or event.display_name or "viewer"
        moderator = " [MOD]" if event.moderator_flag else ""
        return "{} @{}{}: {}".format(stamp, handle, moderator, event.comment_text)

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


async def _drain_stderr(reader: asyncio.StreamReader) -> None:
    async for raw in reader:
        try:
            text = raw.decode("utf-8", errors="replace")
        except AttributeError:
            text = str(raw)
        code = _safe_code(text)
        if code:
            print("[transport] {}".format(code), flush=True)


async def _consume_stdout(
    reader: asyncio.StreamReader,
    process: asyncio.subprocess.Process,
    adapter: LiveChatAdapter,
    timezone: ZoneInfo,
    ended_rooms: Set[str],
    state: CycleState,
) -> None:
    async for raw in reader:
        duplicates_before = adapter.buffer.duplicates
        invalid_before = int(adapter.health["invalid_lines"])
        event = adapter.ingest_line(raw)
        state.duplicate_replays_suppressed += (
            adapter.buffer.duplicates - duplicates_before
        )
        state.invalid_lines += int(adapter.health["invalid_lines"]) - invalid_before
        if event is None:
            continue

        if event.event_type == "connected":
            state.saw_connected = True
            if not event.room_id or event.room_id not in ended_rooms:
                print(format_event(event, timezone), flush=True)
            continue

        if event.is_comment:
            state.comments_emitted += 1
            print(format_event(event, timezone), flush=True)
            continue

        if event.event_type == "live_ended":
            state.saw_live_end = True
            first_end_for_room = not event.room_id or event.room_id not in ended_rooms
            if event.room_id:
                ended_rooms.add(event.room_id)
            if first_end_for_room:
                print(format_event(event, timezone), flush=True)
            if process.returncode is None:
                try:
                    process.terminate()
                except ProcessLookupError:
                    pass
            return

        if event.event_type == "reconnecting" and event.room_id in ended_rooms:
            continue
        print(format_event(event, timezone), flush=True)


async def _terminate_process(process: asyncio.subprocess.Process) -> None:
    if process.returncode is not None:
        return
    try:
        process.terminate()
    except ProcessLookupError:
        return
    try:
        await asyncio.wait_for(process.wait(), timeout=5.0)
    except asyncio.TimeoutError:
        try:
            process.kill()
        except ProcessLookupError:
            return
        await process.wait()


async def run_transport_cycle(
    args: argparse.Namespace,
    adapter: LiveChatAdapter,
    timezone: ZoneInfo,
    ended_rooms: Set[str],
    stop_event: asyncio.Event,
    deadline: datetime,
) -> CycleResult:
    command = build_transport_command(args)
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    process = await asyncio.create_subprocess_exec(
        *command,
        cwd=str(REPO_ROOT),
        env=env,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    assert process.stdout is not None
    assert process.stderr is not None

    state = CycleState()
    stdout_task = asyncio.create_task(
        _consume_stdout(
            process.stdout,
            process,
            adapter,
            timezone,
            ended_rooms,
            state,
        )
    )
    stderr_task = asyncio.create_task(_drain_stderr(process.stderr))
    process_task = asyncio.create_task(process.wait())
    stop_task = asyncio.create_task(stop_event.wait())

    remaining = max(0.0, (deadline - datetime.now(timezone)).total_seconds())
    done, _pending = await asyncio.wait(
        {process_task, stop_task},
        timeout=remaining,
        return_when=asyncio.FIRST_COMPLETED,
    )

    stop_reason = "process_exit"
    if not done:
        stop_reason = "window_closed"
        await _terminate_process(process)
    elif stop_task in done and stop_event.is_set():
        stop_reason = "stop_requested"
        await _terminate_process(process)

    return_code = await process.wait()
    await asyncio.gather(stdout_task, stderr_task, return_exceptions=True)
    for task in (process_task, stop_task):
        if not task.done():
            task.cancel()
    return CycleResult(return_code=return_code, state=state, stop_reason=stop_reason)


async def _sleep_until_retry(
    seconds: float,
    stop_event: asyncio.Event,
    deadline: datetime,
    timezone: ZoneInfo,
) -> None:
    remaining = max(0.0, (deadline - datetime.now(timezone)).total_seconds())
    delay = min(float(seconds), remaining)
    if delay <= 0:
        return
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=delay)
    except asyncio.TimeoutError:
        pass


def _install_signal_handlers(stop_event: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    for signal_name in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(signal_name, stop_event.set)
        except (NotImplementedError, RuntimeError, ValueError):
            pass


async def run_window(args: argparse.Namespace) -> int:
    try:
        timezone = ZoneInfo(args.timezone)
    except ZoneInfoNotFoundError:
        print("[shadow] invalid timezone", file=sys.stderr, flush=True)
        return 2

    now = datetime.now(timezone)
    if args.run_for_seconds is not None:
        window = ActiveWindow(start=now, end=now + timedelta(seconds=args.run_for_seconds))
    else:
        window = resolve_active_window(
            now,
            args.weekday,
            args.window_start,
            args.window_end,
        )
        if window is None:
            print(
                "[shadow] Outside the configured show window; nothing started.",
                flush=True,
            )
            return 0

    stop_event = asyncio.Event()
    _install_signal_handlers(stop_event)
    adapter = LiveChatAdapter(
        buffer=LiveChatBuffer(
            max_events=args.buffer_events,
            max_age_seconds=args.dedupe_seconds,
            max_seen_events=args.transport_dedupe_capacity,
        ),
        clear_on_live_end=False,
    )
    ended_rooms: Set[str] = set()
    cycle_count = 0

    print("[shadow] BARCODE TikTok LIVE chat monitor", flush=True)
    print("[shadow] Target: @{}".format(args.username), flush=True)
    print(
        "[shadow] Window: {} to {} {}".format(
            window.start.strftime("%a %H:%M"),
            window.end.strftime("%a %H:%M"),
            args.timezone,
        ),
        flush=True,
    )
    print("[shadow] Waiting for the account to become LIVE...", flush=True)
    print("[shadow] Replay suppression: active", flush=True)
    print(flush=True)

    while not stop_event.is_set() and datetime.now(timezone) < window.end:
        cycle_count += 1
        result = await run_transport_cycle(
            args,
            adapter,
            timezone,
            ended_rooms,
            stop_event,
            window.end,
        )
        if result.stop_reason in {"window_closed", "stop_requested"}:
            break

        if result.state.duplicate_replays_suppressed:
            print(
                "[shadow] Suppressed {} replayed comment(s).".format(
                    result.state.duplicate_replays_suppressed
                ),
                flush=True,
            )
        if result.state.saw_live_end:
            print(
                "[shadow] LIVE ended. Continuing to watch for a restart until {}.".format(
                    window.end.strftime("%H:%M %Z")
                ),
                flush=True,
            )
        elif result.return_code != 0:
            print(
                "[shadow] Account is offline or the connection failed (exit {}).".format(
                    result.return_code
                ),
                flush=True,
            )
        else:
            print("[shadow] Transport closed; continuing to watch.", flush=True)

        if datetime.now(timezone) >= window.end or stop_event.is_set():
            break
        print(
            "[shadow] Retrying in {} seconds...".format(int(args.retry_seconds)),
            flush=True,
        )
        print(flush=True)
        await _sleep_until_retry(
            args.retry_seconds,
            stop_event,
            window.end,
            timezone,
        )

    health = adapter.health_snapshot()
    print(flush=True)
    print(
        "[shadow] Window closed. comments={} duplicates_suppressed={} reconnects={} cycles={}".format(
            health["comments_received"] - health["duplicate_count"],
            health["duplicate_count"],
            health["reconnect_count"],
            cycle_count,
        ),
        flush=True,
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Supervise the BARCODE TikTok LIVE shadow reader for one show window"
    )
    parser.add_argument("--username", required=True, type=normalize_username)
    parser.add_argument(
        "--timezone",
        default=DEFAULT_TIMEZONE,
        help="IANA timezone used for schedule and display",
    )
    parser.add_argument(
        "--weekday",
        type=parse_weekday,
        default=DEFAULT_WEEKDAY,
        help="show start weekday; default friday",
    )
    parser.add_argument(
        "--window-start",
        type=parse_clock,
        default=DEFAULT_WINDOW_START,
    )
    parser.add_argument(
        "--window-end",
        type=parse_clock,
        default=DEFAULT_WINDOW_END,
    )
    parser.add_argument("--retry-seconds", type=float, default=DEFAULT_RETRY_SECONDS)
    parser.add_argument("--buffer-events", type=int, default=DEFAULT_BUFFER_EVENTS)
    parser.add_argument("--dedupe-seconds", type=float, default=DEFAULT_DEDUPE_SECONDS)
    parser.add_argument(
        "--transport-dedupe-capacity",
        type=int,
        default=20_000,
    )
    parser.add_argument(
        "--transport-script",
        type=Path,
        default=REPO_ROOT / "scripts" / "tiktok_live_chat_transport.py",
    )
    parser.add_argument("--python", type=Path, default=Path(sys.executable))
    parser.add_argument(
        "--cdn",
        choices=("default", "us", "eu"),
        default="us",
    )
    parser.add_argument("--max-retries", type=int, default=5)
    parser.add_argument("--stale-timeout", type=float, default=60.0)
    parser.add_argument(
        "--run-for-seconds",
        type=float,
        default=None,
        help="manual shadow proof override; bypasses the weekly schedule",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.retry_seconds < 1 or args.retry_seconds > 300:
        parser.error("--retry-seconds must be between 1 and 300")
    if args.buffer_events < 100 or args.buffer_events > 100_000:
        parser.error("--buffer-events must be between 100 and 100000")
    if args.dedupe_seconds < 60 or args.dedupe_seconds > 24 * 60 * 60:
        parser.error("--dedupe-seconds must be between 60 and 86400")
    if args.transport_dedupe_capacity < 100 or args.transport_dedupe_capacity > 100_000:
        parser.error("--transport-dedupe-capacity must be between 100 and 100000")
    if args.max_retries < 0 or args.max_retries > 100:
        parser.error("--max-retries must be between 0 and 100")
    if args.stale_timeout < 10 or args.stale_timeout > 600:
        parser.error("--stale-timeout must be between 10 and 600")
    if args.run_for_seconds is not None and not (1 <= args.run_for_seconds <= 24 * 60 * 60):
        parser.error("--run-for-seconds must be between 1 and 86400")
    if not args.transport_script.is_file():
        parser.error("transport script does not exist")
    if not args.python.is_file():
        parser.error("python executable does not exist")
    return asyncio.run(run_window(args))


if __name__ == "__main__":
    raise SystemExit(main())
