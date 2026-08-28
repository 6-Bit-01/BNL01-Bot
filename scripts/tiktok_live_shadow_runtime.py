"""Async process supervision for the isolated TikTok LIVE shadow collector."""

from __future__ import annotations

import argparse
import asyncio
import os
import signal
from datetime import datetime
from typing import Set
from zoneinfo import ZoneInfo

from bnl_tiktok_live_chat import JOIN, VIEWER_SNAPSHOT, LiveChatAdapter
from scripts.tiktok_live_shadow_model import (
    REPO_ROOT,
    CycleResult,
    CycleState,
    _safe_code,
    build_transport_command,
    format_event,
)

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

        if event.is_observation:
            state.events_emitted += 1
            if event.is_comment:
                state.comments_emitted += 1
            if event.event_type == JOIN:
                # Joins are counted in the final summary but not printed one-by-one.
                continue
            if event.event_type == VIEWER_SNAPSHOT:
                if state.last_viewer_count == event.viewer_count:
                    continue
                state.last_viewer_count = event.viewer_count
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


def format_summary(health: dict, cycle_count: int) -> str:
    return (
        "[shadow] Window closed. comments={comments:,} taps={taps:,} "
        "latest_tap_total={latest:,} peak_viewers={peak:,} shares={shares:,} "
        "follows={follows:,} gifts={gifts:,}/{gift_units:,} diamonds={diamonds:,} "
        "questions={questions:,} joins={joins:,} duplicates_suppressed={duplicates:,} "
        "reconnects={reconnects:,} cycles={cycles:,}"
    ).format(
        comments=health["comments_accepted"],
        taps=health["taps_observed"],
        latest=health["latest_like_total"],
        peak=health["peak_viewers"],
        shares=health["shares"],
        follows=health["follows"],
        gifts=health["gift_events"],
        gift_units=health["gift_units"],
        diamonds=health["diamond_total"],
        questions=health["questions"],
        joins=health["joins"],
        duplicates=health["duplicate_count"],
        reconnects=health["reconnect_count"],
        cycles=cycle_count,
    )


