#!/usr/bin/env python3
"""Emit read-only TikTok LIVE public telemetry as versioned NDJSON.

This script is intentionally isolated from the main BNL environment. It imports
``piratetok_live`` only after argument validation, listens only for public LIVE
observations and connection lifecycle events, writes protocol objects to
stdout, and writes only bounded diagnostic codes to stderr. It cannot post,
moderate, gift, follow, or change BARCODE show state.
"""

from __future__ import annotations

import argparse
import signal
import sys
from pathlib import Path
from typing import Any, Callable, Dict, Optional

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.tiktok_live_telemetry_common import (  # noqa: E402
    DEFAULT_DEDUPE_CAPACITY,
    EventIdDeduper,
    build_lifecycle_payload,
    build_transport_error_payload,
    emit_diagnostic,
    emit_payload,
    normalize_username,
)
from scripts.tiktok_live_telemetry_payloads import (  # noqa: E402
    build_comment_payload,
    build_gift_payload,
    build_join_payload,
    build_like_payload,
    build_question_payload,
    build_social_payload,
    build_viewer_snapshot_payload,
)

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

    event_deduper = EventIdDeduper(args.dedupe_capacity)
    stream_ended = False
    disconnected_emitted = False

    def emit_observation(builder: Callable[[Any], Optional[Dict[str, Any]]], event: Any) -> None:
        if stream_ended:
            return
        payload = builder(event)
        if payload is not None and event_deduper.accept(payload["event_id"]):
            emit_payload(payload)

    def register_optional(name: str, callback: Callable[[Any], None]) -> None:
        event_value = getattr(EventType, name, None)
        if event_value is not None:
            client.on(event_value)(callback)

    @client.on(EventType.connected)
    def on_connected(event: Any) -> None:
        if not stream_ended:
            emit_payload(build_lifecycle_payload("connected", event))

    @client.on(EventType.chat)
    def on_chat(event: Any) -> None:
        emit_observation(build_comment_payload, event)

    def on_like(event: Any) -> None:
        emit_observation(build_like_payload, event)

    def on_viewer_snapshot(event: Any) -> None:
        emit_observation(build_viewer_snapshot_payload, event)

    def on_share(event: Any) -> None:
        emit_observation(lambda value: build_social_payload("share", value), event)

    def on_follow(event: Any) -> None:
        emit_observation(lambda value: build_social_payload("follow", value), event)

    def on_gift(event: Any) -> None:
        emit_observation(build_gift_payload, event)

    def on_question(event: Any) -> None:
        emit_observation(build_question_payload, event)

    def on_join(event: Any) -> None:
        emit_observation(build_join_payload, event)

    register_optional("like", on_like)
    register_optional("room_user_seq", on_viewer_snapshot)
    register_optional("share", on_share)
    register_optional("follow", on_follow)
    register_optional("gift", on_gift)
    register_optional("question_new", on_question)
    register_optional("join", on_join)

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
        description="Read TikTok LIVE public telemetry and emit BARCODE shadow NDJSON"
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
        help="Maximum recent TikTok event IDs retained for replay suppression",
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
