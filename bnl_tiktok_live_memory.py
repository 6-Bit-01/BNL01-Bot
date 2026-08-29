"""Durable handoff contract for public TikTok LIVE conversation events.

The isolated collector cannot write BNL's database.  It appends accepted public
comments and TikTok Q&A questions to a mode-0600 spool in the systemd runtime
directory.  The main bot tails that spool and writes the events through BNL's
existing Journal source archive and Memory Ledger owners.

This module is deliberately standard-library only so the collector's isolated
Python 3.11 environment can use it without importing the Discord bot.
"""

from __future__ import annotations

import json
import math
import os
import re
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Optional, Tuple


ARCHIVE_SCHEMA_VERSION = 1
SOURCE = "tiktok_live_webcast"
ARCHIVE_POLICY = "durable_public_conversation"
MEMORY_PLACEMENT = "above_community_canon"
IDENTITY_POLICY = "handle_display_correlated_v1"

DEFAULT_ARCHIVE_SPOOL_PATH = (
    "/run/bnl-tiktok-chat-shadow/public-conversation.ndjson"
)
DEFAULT_MAX_READ_BYTES = 1024 * 1024
DEFAULT_MAX_RECORDS = 1000
MAX_EVENT_LINE_BYTES = 8 * 1024
MAX_SPOOL_BYTES = 64 * 1024 * 1024

_PUBLIC_TEXT_TYPES = frozenset({"comment", "question"})
_CONTROL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
_SPACE_RE = re.compile(r"\s+")
_HANDLE_RE = re.compile(r"^[A-Za-z0-9._]+$")
_EVENT_ID_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,240}$")
_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")
_TRAILING_DIGITS_RE = re.compile(r"\d{1,4}$")
_LEET_TRANSLATION = str.maketrans({"0": "o", "3": "e", "4": "a", "5": "s", "7": "t"})


@dataclass(frozen=True)
class SpoolReadResult:
    records: Tuple[Dict[str, Any], ...] = ()
    next_offset: int = 0
    reason: str = "ok"
    reset: bool = False


@dataclass(frozen=True)
class TikTokIdentityResolution:
    subject_ref: str
    binding_basis: str
    bound_discord_user_id: int = 0
    trusted_platform_identity: bool = False
    trusted_room_moderator: bool = False


def _bounded_text(value: Any, limit: int) -> str:
    if not isinstance(value, (str, int, float, bool)):
        return ""
    return _SPACE_RE.sub(
        " ", _CONTROL_RE.sub(" ", str(value))
    ).strip()[:limit].rstrip()


def _finite_timestamp(value: Any) -> Optional[float]:
    try:
        numeric = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return numeric if math.isfinite(numeric) and numeric > 0 else None


def normalize_tiktok_handle(value: Any) -> str:
    """Return one exact, case-folded TikTok username or an empty string."""

    handle = _bounded_text(value, 80).lstrip("@").lower()
    return handle if handle and _HANDLE_RE.fullmatch(handle) else ""


def _identity_token(value: Any) -> str:
    raw = _NON_ALNUM_RE.sub("", _bounded_text(value, 120).lower())
    return raw.translate(_LEET_TRANSLATION)


def _edit_distance_at_most_one(left: str, right: str) -> bool:
    if left == right:
        return True
    if not left or not right or abs(len(left) - len(right)) > 1:
        return False
    if len(left) == len(right):
        return sum(a != b for a, b in zip(left, right)) <= 1
    shorter, longer = (left, right) if len(left) < len(right) else (right, left)
    index_short = 0
    index_long = 0
    differences = 0
    while index_short < len(shorter) and index_long < len(longer):
        if shorter[index_short] == longer[index_long]:
            index_short += 1
            index_long += 1
            continue
        differences += 1
        index_long += 1
        if differences > 1:
            return False
    return True


def _name_is_close(left: Any, right: Any) -> bool:
    first = _identity_token(left)
    second = _identity_token(right)
    if len(first) < 3 or len(second) < 3:
        return first == second and bool(first)
    return _edit_distance_at_most_one(first, second)


def _handle_supports_name(handle: str, name: Any) -> bool:
    normalized_handle = normalize_tiktok_handle(handle)
    name_token = _identity_token(name)
    if not normalized_handle or len(name_token) < 3:
        return False
    handle_token = _identity_token(normalized_handle)
    if _edit_distance_at_most_one(handle_token, name_token):
        return True
    without_suffix = _TRAILING_DIGITS_RE.sub("", normalized_handle)
    return bool(without_suffix) and _edit_distance_at_most_one(
        _identity_token(without_suffix),
        name_token,
    )


def public_conversation_record(value: Any) -> Optional[Dict[str, Any]]:
    """Validate the durable subset of one accepted TikTok observation."""

    if not isinstance(value, Mapping):
        return None
    event_type = _bounded_text(value.get("event_type"), 24).lower()
    if event_type not in _PUBLIC_TEXT_TYPES:
        return None
    event_id = _bounded_text(value.get("event_id"), 240)
    if not _EVENT_ID_RE.fullmatch(event_id):
        return None
    observed_at = _finite_timestamp(value.get("observed_at"))
    source_at = _finite_timestamp(value.get("source_at"))
    if observed_at is None:
        return None
    handle = normalize_tiktok_handle(value.get("unique_id"))
    display_name = _bounded_text(value.get("display_name"), 120)
    text_key = "comment_text" if event_type == "comment" else "question_text"
    text = _bounded_text(value.get(text_key), 1000)
    if not text:
        return None
    return {
        "archive_schema_version": ARCHIVE_SCHEMA_VERSION,
        "source": SOURCE,
        "archive_policy": ARCHIVE_POLICY,
        "memory_placement": MEMORY_PLACEMENT,
        "identity_policy": IDENTITY_POLICY,
        "event_type": event_type,
        "event_id": event_id,
        "room_id": _bounded_text(value.get("room_id"), 160),
        "observed_at": observed_at,
        "source_at": source_at,
        "unique_id": handle,
        "display_name": display_name,
        text_key: text,
        "moderator_flag": value.get("moderator_flag") is True,
    }


def resolve_tiktok_identity(
    record: Mapping[str, Any],
    *,
    known_discord_identities: Optional[Mapping[int, Iterable[str]]] = None,
    owner_user_id: int = 0,
    owner_handles: Iterable[str] = ("six.bit", "pr0x60"),
    owner_names: Iterable[str] = ("6 Bit", "PR0X", "Prox"),
) -> TikTokIdentityResolution:
    """Resolve a platform identity from correlated handle and display signals.

    A direct configured owner handle is sufficient.  Every inferred Discord
    binding requires both a close display-name signal and a compatible handle;
    one resemblance alone never merges identities.
    """

    handle = normalize_tiktok_handle(record.get("unique_id"))
    display_name = _bounded_text(record.get("display_name"), 120)
    moderator = record.get("moderator_flag") is True
    normalized_owner_handles = {
        normalize_tiktok_handle(value) for value in owner_handles
    }
    normalized_owner_handles.discard("")
    owner_name_match = any(
        _name_is_close(display_name, owner_name)
        and _handle_supports_name(handle, owner_name)
        for owner_name in owner_names
    )
    if (
        handle
        and int(owner_user_id or 0) > 0
        and (handle in normalized_owner_handles or owner_name_match)
    ):
        return TikTokIdentityResolution(
            subject_ref="discord_user:%s" % int(owner_user_id),
            binding_basis=(
                "owner_declared_exact_tiktok_handle"
                if handle in normalized_owner_handles
                else "owner_declared_handle_display_correlation"
            ),
            bound_discord_user_id=int(owner_user_id),
            trusted_platform_identity=True,
            trusted_room_moderator=moderator,
        )
    matches = set()
    for user_id, labels in (known_discord_identities or {}).items():
        resolved_user_id = int(user_id or 0)
        if resolved_user_id <= 0:
            continue
        if any(
            _name_is_close(display_name, label)
            and _handle_supports_name(handle, label)
            for label in labels
            if str(label or "").strip()
        ):
            matches.add(resolved_user_id)
    if handle and len(matches) == 1:
        user_id = next(iter(matches))
        return TikTokIdentityResolution(
            subject_ref="discord_user:%s" % user_id,
            binding_basis="handle_display_correlation",
            bound_discord_user_id=user_id,
            trusted_platform_identity=True,
            trusted_room_moderator=moderator,
        )
    if handle:
        return TikTokIdentityResolution(
            subject_ref="tiktok_user:%s" % handle,
            binding_basis=(
                "ambiguous_handle_display_match_unlinked"
                if len(matches) > 1
                else "tiktok_handle_identity"
            ),
            trusted_platform_identity=True,
            trusted_room_moderator=moderator,
        )
    event_id = _bounded_text(record.get("event_id"), 240)
    return TikTokIdentityResolution(
        subject_ref="tiktok_event:%s" % event_id,
        binding_basis="missing_handle_event_only",
        trusted_platform_identity=False,
        trusted_room_moderator=moderator,
    )


class TikTokPublicConversationSpoolWriter:
    """Append every accepted public text event to a bounded volatile spool."""

    def __init__(self, path: str) -> None:
        if not str(path or "").strip():
            raise ValueError("archive spool path is required")
        self.path = Path(path)

    def append(self, value: Any) -> bool:
        record = public_conversation_record(value)
        if record is None:
            return False
        encoded = (
            json.dumps(
                record,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            )
            + "\n"
        ).encode("utf-8")
        if len(encoded) > MAX_EVENT_LINE_BYTES:
            raise ValueError("archive spool event exceeds line limit")
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        flags = os.O_WRONLY | os.O_CREAT | os.O_APPEND
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        descriptor = os.open(str(self.path), flags, 0o600)
        try:
            metadata = os.fstat(descriptor)
            if not stat.S_ISREG(metadata.st_mode):
                raise ValueError("archive spool is not a regular file")
            if metadata.st_size + len(encoded) > MAX_SPOOL_BYTES:
                raise ValueError("archive spool exceeds bounded show limit")
            os.fchmod(descriptor, 0o600)
            written = os.write(descriptor, encoded)
            if written != len(encoded):
                raise OSError("archive spool short write")
        finally:
            os.close(descriptor)
        return True


def read_public_conversation_spool(
    path: str,
    *,
    offset: int = 0,
    max_bytes: int = DEFAULT_MAX_READ_BYTES,
    max_records: int = DEFAULT_MAX_RECORDS,
) -> SpoolReadResult:
    """Read complete validated lines without consuming or mutating the spool."""

    requested_offset = max(0, int(offset or 0))
    bounded_bytes = max(1, min(int(max_bytes or 1), DEFAULT_MAX_READ_BYTES))
    bounded_records = max(1, min(int(max_records or 1), DEFAULT_MAX_RECORDS))
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
    except FileNotFoundError:
        return SpoolReadResult(next_offset=0, reason="spool_missing")
    except OSError:
        return SpoolReadResult(next_offset=requested_offset, reason="spool_unreadable")
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            return SpoolReadResult(
                next_offset=requested_offset,
                reason="spool_not_regular",
            )
        reset = metadata.st_size < requested_offset
        start = 0 if reset else requested_offset
        os.lseek(descriptor, start, os.SEEK_SET)
        raw = os.read(descriptor, bounded_bytes)
    except OSError:
        return SpoolReadResult(next_offset=requested_offset, reason="spool_unreadable")
    finally:
        os.close(descriptor)

    if not raw:
        return SpoolReadResult(next_offset=start, reason="ok", reset=reset)
    last_newline = raw.rfind(b"\n")
    if last_newline < 0:
        return SpoolReadResult(
            next_offset=start,
            reason="partial_line_waiting",
            reset=reset,
        )
    complete = raw[: last_newline + 1]
    next_offset = start
    records = []
    for raw_line in complete.splitlines(keepends=True):
        if len(records) >= bounded_records:
            break
        next_offset += len(raw_line)
        if len(raw_line) > MAX_EVENT_LINE_BYTES:
            continue
        try:
            value = json.loads(raw_line.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError):
            continue
        record = public_conversation_record(value)
        if record is not None:
            records.append(record)
    return SpoolReadResult(
        records=tuple(records),
        next_offset=next_offset,
        reason="ok",
        reset=reset,
    )
