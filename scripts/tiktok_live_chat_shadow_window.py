#!/usr/bin/env python3
"""Supervise the read-only TikTok LIVE telemetry collector for one show window.

The supervisor keeps replay suppression across child-process reconnects, stops
an ended Webcast connection immediately, and starts a fresh connection until
the configured show window closes. It writes only sanitized shadow output to
its terminal. It does not call Gemini, Discord, the website, a database, or any
show-control owner.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Sequence, Set
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from bnl_tiktok_live_chat import LiveChatAdapter, LiveChatBuffer  # noqa: E402
from bnl_tiktok_live_context import LiveContextSnapshotWriter  # noqa: E402
from bnl_tiktok_live_memory import (  # noqa: E402
    TikTokPublicConversationSpoolWriter,
)
from scripts.tiktok_live_shadow_model import (
    DEFAULT_BUFFER_EVENTS,
    DEFAULT_DEDUPE_SECONDS,
    DEFAULT_RETRY_SECONDS,
    DEFAULT_TIMEZONE,
    DEFAULT_WEEKDAY,
    DEFAULT_WINDOW_END,
    DEFAULT_WINDOW_START,
    REPO_ROOT,
    ActiveWindow,
    CycleResult,
    CycleState,
    build_transport_command,
    format_event,
    normalize_username,
    parse_clock,
    parse_weekday,
    resolve_active_window,
)
from scripts.tiktok_live_shadow_runtime import (
    _install_signal_handlers,
    _sleep_until_retry,
    format_summary,
    run_transport_cycle,
)

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
    context_writer = (
        LiveContextSnapshotWriter(str(args.context_path))
        if args.context_path is not None
        else None
    )
    archive_writer = (
        TikTokPublicConversationSpoolWriter(str(args.archive_spool_path))
        if args.archive_spool_path is not None
        else None
    )
    if context_writer is not None:
        context_writer.publish(adapter, force=True)
    ended_rooms: Set[str] = set()
    cycle_count = 0

    print("[shadow] BARCODE TikTok LIVE telemetry monitor", flush=True)
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
    print(
        "[shadow] Signals: comments, taps, viewers, shares, follows, gifts, questions, joins",
        flush=True,
    )
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
            context_writer,
            archive_writer,
        )
        if result.stop_reason in {"window_closed", "stop_requested"}:
            break

        if result.state.duplicate_replays_suppressed:
            print(
                "[shadow] Suppressed {} replayed event(s).".format(
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
    if context_writer is not None:
        context_writer.publish(adapter, force=True)
    print(flush=True)
    print(format_summary(health, cycle_count), flush=True)
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
    parser.add_argument(
        "--context-path",
        type=Path,
        default=None,
        help="volatile JSON path exported for the separately gated BNL reader",
    )
    parser.add_argument(
        "--archive-spool-path",
        type=Path,
        default=None,
        help="volatile append-only handoff for durable public chat ingestion",
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
    if args.run_for_seconds is not None and not (
        1 <= args.run_for_seconds <= 24 * 60 * 60
    ):
        parser.error("--run-for-seconds must be between 1 and 86400")
    if not args.transport_script.is_file():
        parser.error("transport script does not exist")
    if not args.python.is_file():
        parser.error("python executable does not exist")
    return asyncio.run(run_window(args))


if __name__ == "__main__":
    raise SystemExit(main())
