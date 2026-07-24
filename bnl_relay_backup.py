from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import os
import re
import sqlite3
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Mapping, Optional, Sequence


SNAPSHOT_FORMAT = "bnl01.accepted-website-relay-snapshot"
SNAPSHOT_FORMAT_VERSION = 1
SCHEDULE_ENABLED_ENV = "BNL_RELAY_BACKUP_SCHEDULE_ENABLED"
DEFAULT_RCLONE_BIN = "rclone"
DEFAULT_TRANSPORT_TIMEOUT_SECONDS = 300
SQLITE_SIDECAR_SUFFIXES = ("-wal", "-shm", "-journal")
MAX_COMPRESSED_BYTES = 64 * 1024 * 1024
MAX_DECOMPRESSED_BYTES = 256 * 1024 * 1024

HISTORY_COLUMNS = (
    "relay_id",
    "guild_id",
    "public_message",
    "public_directive",
    "mode",
    "relay_lane",
    "event_type",
    "highest_source_conversation_id",
    "normalized_message",
    "semantic_family",
    "published_timestamp",
)
STATE_COLUMNS = (
    "guild_id",
    "last_published_conversation_cursor",
    "last_publication_timestamp",
)
SNAPSHOT_KEYS = {
    "format",
    "format_version",
    "snapshot_month",
    "source_max_published_at",
    "history_columns",
    "state_columns",
    "history_count",
    "state_count",
    "history",
    "state",
    "content_sha256",
}
ALLOWED_RESTORE_TABLES = {"website_relay_history", "website_relay_state"}
MONTH_PATTERN = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")
CHECKSUM_PATTERN = re.compile(r"^([0-9a-f]{64})  ([^/\r\n]+)\n?$")


class RelayBackupError(RuntimeError):
    pass


class RelayBackupValidationError(RelayBackupError):
    pass


class RelayBackupConflictError(RelayBackupError):
    pass


class RelayBackupTransportError(RelayBackupError):
    pass


@dataclass(frozen=True)
class ArchiveArtifact:
    archive_path: Path
    checksum_path: Path
    snapshot_month: str
    content_sha256: str
    archive_sha256: str
    history_count: int
    state_count: int


@dataclass(frozen=True)
class RelaySnapshot:
    payload: dict
    archive_path: Path
    checksum_path: Path

    @property
    def snapshot_month(self) -> str:
        return str(self.payload["snapshot_month"])

    @property
    def content_sha256(self) -> str:
        return str(self.payload["content_sha256"])

    @property
    def history_count(self) -> int:
        return int(self.payload["history_count"])

    @property
    def state_count(self) -> int:
        return int(self.payload["state_count"])


@dataclass(frozen=True)
class RestoreReport:
    target_db_path: Path
    history_inserted: int
    history_existing: int
    state_inserted: int
    state_existing: int
    history_count: int
    state_count: int


@dataclass(frozen=True)
class TransportReport:
    operation: str
    transferred: tuple[str, ...]


@dataclass(frozen=True)
class RoundTripReport:
    downloaded_archive_sha256: str
    downloaded_content_sha256: str
    downloaded_history_count: int
    downloaded_state_count: int
    first_restore: RestoreReport
    second_restore: RestoreReport
    production_db_untouched: bool


def _validate_snapshot_month(snapshot_month: str) -> str:
    value = (snapshot_month or "").strip()
    if not MONTH_PATTERN.fullmatch(value):
        raise RelayBackupValidationError("snapshot_month_must_be_yyyy_mm")
    return value


def _canonical_json_bytes(payload: Mapping) -> bytes:
    return json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _logical_content_sha256(payload: Mapping) -> str:
    digest_payload = dict(payload)
    digest_payload.pop("content_sha256", None)
    return hashlib.sha256(_canonical_json_bytes(digest_payload)).hexdigest()


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _gzip_bytes(payload: bytes) -> bytes:
    output = io.BytesIO()
    with gzip.GzipFile(
        filename="",
        mode="wb",
        fileobj=output,
        compresslevel=9,
        mtime=0,
    ) as compressed:
        compressed.write(payload)
    return output.getvalue()


def _read_gzip_bounded(path: Path) -> bytes:
    if not path.is_file():
        raise RelayBackupValidationError("archive_missing")
    if path.stat().st_size > MAX_COMPRESSED_BYTES:
        raise RelayBackupValidationError("archive_too_large")
    try:
        with gzip.open(path, "rb") as compressed:
            payload = compressed.read(MAX_DECOMPRESSED_BYTES + 1)
    except (OSError, EOFError) as exc:
        raise RelayBackupValidationError("archive_gzip_invalid") from exc
    if len(payload) > MAX_DECOMPRESSED_BYTES:
        raise RelayBackupValidationError("archive_decompressed_too_large")
    return payload


def _atomic_write(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_bytes() == data:
            os.chmod(path, 0o600)
            return
        raise RelayBackupConflictError(f"artifact_path_conflict:{path.name}")
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=str(path.parent),
    )
    temporary_path = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(str(temporary_path), str(path))
        os.chmod(path, 0o600)
    except Exception:
        try:
            os.close(descriptor)
        except OSError:
            pass
        try:
            temporary_path.unlink()
        except OSError:
            pass
        raise


def _read_source_snapshot(db_path: Path) -> tuple[list[dict], list[dict]]:
    source = db_path.expanduser().resolve()
    if not source.is_file():
        raise RelayBackupValidationError("source_database_missing")
    uri = source.as_uri() + "?mode=ro"
    try:
        with sqlite3.connect(uri, uri=True) as connection:
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA query_only=ON")
            connection.execute("BEGIN")
            tables = {
                str(row[0])
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
            if not ALLOWED_RESTORE_TABLES.issubset(tables):
                raise RelayBackupValidationError("relay_tables_missing")
            history_columns = tuple(
                str(row[1])
                for row in connection.execute(
                    "PRAGMA table_info(website_relay_history)"
                ).fetchall()
            )
            state_columns = tuple(
                str(row[1])
                for row in connection.execute(
                    "PRAGMA table_info(website_relay_state)"
                ).fetchall()
            )
            if history_columns != HISTORY_COLUMNS:
                raise RelayBackupValidationError("relay_history_schema_mismatch")
            if state_columns != STATE_COLUMNS:
                raise RelayBackupValidationError("relay_state_schema_mismatch")
            history_rows = [
                dict(row)
                for row in connection.execute(
                    f"""
                    SELECT {", ".join(HISTORY_COLUMNS)}
                    FROM website_relay_history
                    ORDER BY guild_id, published_timestamp, relay_id
                    """
                ).fetchall()
            ]
            state_rows = [
                dict(row)
                for row in connection.execute(
                    f"""
                    SELECT {", ".join(STATE_COLUMNS)}
                    FROM website_relay_state
                    ORDER BY guild_id
                    """
                ).fetchall()
            ]
            connection.commit()
    except sqlite3.Error as exc:
        raise RelayBackupValidationError("source_database_unreadable") from exc
    return history_rows, state_rows


def _build_snapshot_payload(
    history_rows: Sequence[Mapping],
    state_rows: Sequence[Mapping],
    snapshot_month: str,
) -> dict:
    history = [dict(row) for row in history_rows]
    state = [dict(row) for row in state_rows]
    payload = {
        "format": SNAPSHOT_FORMAT,
        "format_version": SNAPSHOT_FORMAT_VERSION,
        "snapshot_month": _validate_snapshot_month(snapshot_month),
        "source_max_published_at": max(
            (str(row["published_timestamp"]) for row in history),
            default="",
        ),
        "history_columns": list(HISTORY_COLUMNS),
        "state_columns": list(STATE_COLUMNS),
        "history_count": len(history),
        "state_count": len(state),
        "history": history,
        "state": state,
    }
    payload["content_sha256"] = _logical_content_sha256(payload)
    return payload


def export_monthly_snapshot(
    db_path: os.PathLike,
    output_dir: os.PathLike,
    snapshot_month: str,
) -> ArchiveArtifact:
    month = _validate_snapshot_month(snapshot_month)
    history_rows, state_rows = _read_source_snapshot(Path(db_path))
    payload = _build_snapshot_payload(history_rows, state_rows, month)
    json_bytes = _canonical_json_bytes(payload)
    compressed_bytes = _gzip_bytes(json_bytes)
    archive_sha256 = _sha256_bytes(compressed_bytes)
    content_sha256 = str(payload["content_sha256"])
    archive_name = (
        f"bnl01-relay-history-{month}-{content_sha256[:12]}.json.gz"
    )
    archive_path = Path(output_dir).expanduser().resolve() / archive_name
    checksum_path = archive_path.with_name(archive_path.name + ".sha256")
    checksum_bytes = f"{archive_sha256}  {archive_path.name}\n".encode("ascii")
    _atomic_write(archive_path, compressed_bytes)
    try:
        _atomic_write(checksum_path, checksum_bytes)
    except Exception:
        if not checksum_path.exists():
            try:
                archive_path.unlink()
            except OSError:
                pass
        raise
    return ArchiveArtifact(
        archive_path=archive_path,
        checksum_path=checksum_path,
        snapshot_month=month,
        content_sha256=content_sha256,
        archive_sha256=archive_sha256,
        history_count=len(history_rows),
        state_count=len(state_rows),
    )


def _validate_history_row(row: object) -> dict:
    if not isinstance(row, dict) or set(row) != set(HISTORY_COLUMNS):
        raise RelayBackupValidationError("archive_history_row_schema_invalid")
    if not isinstance(row["relay_id"], str) or not row["relay_id"]:
        raise RelayBackupValidationError("archive_relay_id_invalid")
    if not isinstance(row["guild_id"], int):
        raise RelayBackupValidationError("archive_history_guild_id_invalid")
    if not isinstance(row["highest_source_conversation_id"], int):
        raise RelayBackupValidationError("archive_history_cursor_invalid")
    for key in (
        "public_message",
        "public_directive",
        "mode",
        "relay_lane",
        "event_type",
        "normalized_message",
        "semantic_family",
        "published_timestamp",
    ):
        if not isinstance(row[key], str):
            raise RelayBackupValidationError(f"archive_history_{key}_invalid")
    return {column: row[column] for column in HISTORY_COLUMNS}


def _validate_state_row(row: object) -> dict:
    if not isinstance(row, dict) or set(row) != set(STATE_COLUMNS):
        raise RelayBackupValidationError("archive_state_row_schema_invalid")
    if not isinstance(row["guild_id"], int):
        raise RelayBackupValidationError("archive_state_guild_id_invalid")
    if not isinstance(row["last_published_conversation_cursor"], int):
        raise RelayBackupValidationError("archive_state_cursor_invalid")
    timestamp = row["last_publication_timestamp"]
    if timestamp is not None and not isinstance(timestamp, str):
        raise RelayBackupValidationError("archive_state_timestamp_invalid")
    return {column: row[column] for column in STATE_COLUMNS}


def _validate_snapshot_payload(payload: object) -> dict:
    if not isinstance(payload, dict) or set(payload) != SNAPSHOT_KEYS:
        raise RelayBackupValidationError("archive_top_level_schema_invalid")
    if payload.get("format") != SNAPSHOT_FORMAT:
        raise RelayBackupValidationError("archive_format_invalid")
    if payload.get("format_version") != SNAPSHOT_FORMAT_VERSION:
        raise RelayBackupValidationError("archive_format_version_invalid")
    _validate_snapshot_month(str(payload.get("snapshot_month") or ""))
    if payload.get("history_columns") != list(HISTORY_COLUMNS):
        raise RelayBackupValidationError("archive_history_columns_invalid")
    if payload.get("state_columns") != list(STATE_COLUMNS):
        raise RelayBackupValidationError("archive_state_columns_invalid")
    history_raw = payload.get("history")
    state_raw = payload.get("state")
    if not isinstance(history_raw, list) or not isinstance(state_raw, list):
        raise RelayBackupValidationError("archive_rows_invalid")
    history = [_validate_history_row(row) for row in history_raw]
    state = [_validate_state_row(row) for row in state_raw]
    if payload.get("history_count") != len(history):
        raise RelayBackupValidationError("archive_history_count_mismatch")
    if payload.get("state_count") != len(state):
        raise RelayBackupValidationError("archive_state_count_mismatch")
    relay_ids = [row["relay_id"] for row in history]
    if len(relay_ids) != len(set(relay_ids)):
        raise RelayBackupValidationError("archive_duplicate_relay_id")
    guild_ids = [row["guild_id"] for row in state]
    if len(guild_ids) != len(set(guild_ids)):
        raise RelayBackupValidationError("archive_duplicate_state_guild")
    expected_max = max(
        (str(row["published_timestamp"]) for row in history),
        default="",
    )
    if payload.get("source_max_published_at") != expected_max:
        raise RelayBackupValidationError("archive_max_timestamp_mismatch")
    expected_content_sha256 = _logical_content_sha256(payload)
    if payload.get("content_sha256") != expected_content_sha256:
        raise RelayBackupValidationError("archive_content_checksum_mismatch")
    normalized = dict(payload)
    normalized["history"] = history
    normalized["state"] = state
    return normalized


def verify_archive(
    archive_path: os.PathLike,
    checksum_path: os.PathLike,
) -> RelaySnapshot:
    archive = Path(archive_path).expanduser().resolve()
    checksum = Path(checksum_path).expanduser().resolve()
    if not checksum.is_file():
        raise RelayBackupValidationError("checksum_missing")
    try:
        checksum_text = checksum.read_text(encoding="ascii")
    except (OSError, UnicodeError) as exc:
        raise RelayBackupValidationError("checksum_unreadable") from exc
    match = CHECKSUM_PATTERN.fullmatch(checksum_text)
    if not match or match.group(2) != archive.name:
        raise RelayBackupValidationError("checksum_format_invalid")
    expected_archive_sha256 = match.group(1)
    actual_archive_sha256 = _sha256_file(archive)
    if actual_archive_sha256 != expected_archive_sha256:
        raise RelayBackupValidationError("archive_checksum_mismatch")
    json_bytes = _read_gzip_bounded(archive)
    try:
        decoded = json.loads(json_bytes.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise RelayBackupValidationError("archive_json_invalid") from exc
    payload = _validate_snapshot_payload(decoded)
    return RelaySnapshot(
        payload=payload,
        archive_path=archive,
        checksum_path=checksum,
    )


def _paths_refer_to_same_file(left: Path, right: Path) -> bool:
    if left.resolve(strict=False) == right.resolve(strict=False):
        return True
    if left.exists() and right.exists():
        try:
            return os.path.samefile(str(left), str(right))
        except OSError:
            return False
    return False


def _validated_production_db_path(path: os.PathLike) -> Path:
    production = Path(path).expanduser()
    try:
        resolved = production.resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise RelayBackupValidationError(
            "production_database_missing_or_unresolvable"
        ) from exc
    if not resolved.is_file():
        raise RelayBackupValidationError("production_database_not_a_file")
    return production


def _production_family_paths(production: Path) -> tuple[Path, ...]:
    bases = (production, production.resolve(strict=True))
    candidates = []
    seen = set()
    for base in bases:
        for candidate in (
            base,
            *(Path(f"{base}{suffix}") for suffix in SQLITE_SIDECAR_SUFFIXES),
        ):
            identity = str(candidate.resolve(strict=False))
            if identity in seen:
                continue
            seen.add(identity)
            candidates.append(candidate)
    return tuple(candidates)


def _restore_target_kind(
    target: Path,
    production: Path,
) -> Optional[str]:
    if _paths_refer_to_same_file(target, production):
        return "production_database"
    for related_path in _production_family_paths(production):
        if related_path == production:
            continue
        if _paths_refer_to_same_file(target, related_path):
            return "production_database_sidecar"
    return None


def _create_restore_schema(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS website_relay_state (
            guild_id INTEGER PRIMARY KEY,
            last_published_conversation_cursor INTEGER NOT NULL DEFAULT 0,
            last_publication_timestamp TEXT
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS website_relay_history (
            relay_id TEXT PRIMARY KEY,
            guild_id INTEGER NOT NULL,
            public_message TEXT NOT NULL,
            public_directive TEXT NOT NULL,
            mode TEXT NOT NULL,
            relay_lane TEXT NOT NULL,
            event_type TEXT NOT NULL,
            highest_source_conversation_id INTEGER NOT NULL,
            normalized_message TEXT NOT NULL,
            semantic_family TEXT NOT NULL,
            published_timestamp TEXT NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_website_relay_history_guild_time
        ON website_relay_history(guild_id, published_timestamp DESC)
        """
    )


def _validate_restore_target_schema(connection: sqlite3.Connection) -> None:
    tables = {
        str(row[0])
        for row in connection.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type='table' AND name NOT LIKE 'sqlite_%'
            """
        ).fetchall()
    }
    if not tables.issubset(ALLOWED_RESTORE_TABLES):
        raise RelayBackupValidationError("restore_target_contains_unapproved_tables")
    _create_restore_schema(connection)
    history_columns = tuple(
        str(row[1])
        for row in connection.execute(
            "PRAGMA table_info(website_relay_history)"
        ).fetchall()
    )
    state_columns = tuple(
        str(row[1])
        for row in connection.execute(
            "PRAGMA table_info(website_relay_state)"
        ).fetchall()
    )
    if history_columns != HISTORY_COLUMNS or state_columns != STATE_COLUMNS:
        raise RelayBackupValidationError("restore_target_schema_mismatch")


def _row_dict(row: sqlite3.Row, columns: Sequence[str]) -> dict:
    return {column: row[column] for column in columns}


def _target_rows(connection: sqlite3.Connection) -> tuple[list[dict], list[dict]]:
    connection.row_factory = sqlite3.Row
    history = [
        _row_dict(row, HISTORY_COLUMNS)
        for row in connection.execute(
            f"""
            SELECT {", ".join(HISTORY_COLUMNS)}
            FROM website_relay_history
            ORDER BY guild_id, published_timestamp, relay_id
            """
        ).fetchall()
    ]
    state = [
        _row_dict(row, STATE_COLUMNS)
        for row in connection.execute(
            f"""
            SELECT {", ".join(STATE_COLUMNS)}
            FROM website_relay_state
            ORDER BY guild_id
            """
        ).fetchall()
    ]
    return history, state


def restore_to_isolated_db(
    snapshot: RelaySnapshot,
    target_db_path: os.PathLike,
    production_db_path: os.PathLike,
) -> RestoreReport:
    target = Path(target_db_path).expanduser()
    production = _validated_production_db_path(production_db_path)
    if target.is_symlink():
        raise RelayBackupValidationError("restore_target_symlink_rejected")
    target_kind = _restore_target_kind(target, production)
    if target_kind is not None:
        raise RelayBackupValidationError(f"restore_target_is_{target_kind}")
    target.parent.mkdir(parents=True, exist_ok=True)
    created_target = False
    if not target.exists():
        descriptor = os.open(
            str(target),
            os.O_CREAT | os.O_EXCL | os.O_WRONLY,
            0o600,
        )
        os.close(descriptor)
        created_target = True
    elif target.is_dir():
        raise RelayBackupValidationError("restore_target_is_directory")
    try:
        with sqlite3.connect(str(target)) as connection:
            connection.row_factory = sqlite3.Row
            _validate_restore_target_schema(connection)
            connection.commit()
            connection.execute("BEGIN IMMEDIATE")
            history_inserted = 0
            history_existing = 0
            state_inserted = 0
            state_existing = 0
            for archived in snapshot.payload["history"]:
                existing = connection.execute(
                    f"""
                    SELECT {", ".join(HISTORY_COLUMNS)}
                    FROM website_relay_history
                    WHERE relay_id=?
                    """,
                    (archived["relay_id"],),
                ).fetchone()
                if existing is not None:
                    if _row_dict(existing, HISTORY_COLUMNS) != archived:
                        raise RelayBackupConflictError(
                            f"restore_relay_id_conflict:{archived['relay_id']}"
                        )
                    history_existing += 1
                    continue
                connection.execute(
                    f"""
                    INSERT INTO website_relay_history({", ".join(HISTORY_COLUMNS)})
                    VALUES({", ".join("?" for _column in HISTORY_COLUMNS)})
                    """,
                    tuple(archived[column] for column in HISTORY_COLUMNS),
                )
                history_inserted += 1
            for archived in snapshot.payload["state"]:
                existing = connection.execute(
                    f"""
                    SELECT {", ".join(STATE_COLUMNS)}
                    FROM website_relay_state
                    WHERE guild_id=?
                    """,
                    (archived["guild_id"],),
                ).fetchone()
                if existing is not None:
                    if _row_dict(existing, STATE_COLUMNS) != archived:
                        raise RelayBackupConflictError(
                            f"restore_state_conflict:{archived['guild_id']}"
                        )
                    state_existing += 1
                    continue
                connection.execute(
                    f"""
                    INSERT INTO website_relay_state({", ".join(STATE_COLUMNS)})
                    VALUES({", ".join("?" for _column in STATE_COLUMNS)})
                    """,
                    tuple(archived[column] for column in STATE_COLUMNS),
                )
                state_inserted += 1
            restored_history, restored_state = _target_rows(connection)
            if restored_history != snapshot.payload["history"]:
                raise RelayBackupConflictError("restore_history_not_isolated_exact")
            if restored_state != snapshot.payload["state"]:
                raise RelayBackupConflictError("restore_state_not_isolated_exact")
            integrity = connection.execute("PRAGMA integrity_check").fetchone()
            if not integrity or str(integrity[0]).lower() != "ok":
                raise RelayBackupValidationError("restore_integrity_check_failed")
            connection.commit()
    except Exception:
        if created_target:
            try:
                target.unlink()
            except OSError:
                pass
        raise
    os.chmod(target, 0o600)
    return RestoreReport(
        target_db_path=target.resolve(),
        history_inserted=history_inserted,
        history_existing=history_existing,
        state_inserted=state_inserted,
        state_existing=state_existing,
        history_count=snapshot.history_count,
        state_count=snapshot.state_count,
    )


def _remote_path(remote_root: str, file_name: str) -> str:
    root = (remote_root or "").strip().rstrip("/")
    if not root or ":" not in root:
        raise RelayBackupValidationError("rclone_remote_root_invalid")
    if Path(file_name).name != file_name:
        raise RelayBackupValidationError("rclone_file_name_invalid")
    return f"{root}/{file_name}"


def _run_rclone(
    arguments: Sequence[str],
    *,
    runner: Callable = subprocess.run,
    rclone_bin: str = DEFAULT_RCLONE_BIN,
    timeout_seconds: int = DEFAULT_TRANSPORT_TIMEOUT_SECONDS,
) -> None:
    command = [rclone_bin, *arguments]
    try:
        completed = runner(
            command,
            shell=False,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )
    except FileNotFoundError as exc:
        raise RelayBackupTransportError("rclone_not_installed") from exc
    except subprocess.TimeoutExpired as exc:
        raise RelayBackupTransportError("rclone_timeout") from exc
    except OSError as exc:
        raise RelayBackupTransportError("rclone_start_failed") from exc
    return_code = int(getattr(completed, "returncode", 1))
    if return_code != 0:
        raise RelayBackupTransportError(
            f"rclone_failed:{arguments[0]}:exit_{return_code}"
        )


def upload_archive(
    artifact: ArchiveArtifact,
    remote_root: str,
    *,
    runner: Callable = subprocess.run,
    rclone_bin: str = DEFAULT_RCLONE_BIN,
    timeout_seconds: int = DEFAULT_TRANSPORT_TIMEOUT_SECONDS,
) -> TransportReport:
    for local_path in (artifact.archive_path, artifact.checksum_path):
        if not local_path.is_file():
            raise RelayBackupValidationError(
                f"upload_artifact_missing:{local_path.name}"
            )
        _run_rclone(
            (
                "copyto",
                str(local_path),
                _remote_path(remote_root, local_path.name),
            ),
            runner=runner,
            rclone_bin=rclone_bin,
            timeout_seconds=timeout_seconds,
        )
    return TransportReport(
        operation="upload",
        transferred=(artifact.archive_path.name, artifact.checksum_path.name),
    )


def download_archive(
    remote_root: str,
    archive_name: str,
    checksum_name: str,
    destination_dir: os.PathLike,
    *,
    runner: Callable = subprocess.run,
    rclone_bin: str = DEFAULT_RCLONE_BIN,
    timeout_seconds: int = DEFAULT_TRANSPORT_TIMEOUT_SECONDS,
) -> ArchiveArtifact:
    if Path(archive_name).name != archive_name:
        raise RelayBackupValidationError("download_archive_name_invalid")
    if Path(checksum_name).name != checksum_name:
        raise RelayBackupValidationError("download_checksum_name_invalid")
    destination = Path(destination_dir).expanduser().resolve()
    destination.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix=".relay-download-",
        dir=str(destination),
    ) as temporary_dir:
        temporary = Path(temporary_dir)
        temporary_archive = temporary / archive_name
        temporary_checksum = temporary / checksum_name
        for remote_name, local_path in (
            (archive_name, temporary_archive),
            (checksum_name, temporary_checksum),
        ):
            _run_rclone(
                (
                    "copyto",
                    _remote_path(remote_root, remote_name),
                    str(local_path),
                ),
                runner=runner,
                rclone_bin=rclone_bin,
                timeout_seconds=timeout_seconds,
            )
        snapshot = verify_archive(temporary_archive, temporary_checksum)
        final_archive = destination / archive_name
        final_checksum = destination / checksum_name
        if final_archive.exists() or final_checksum.exists():
            raise RelayBackupConflictError("download_destination_exists")
        os.replace(str(temporary_archive), str(final_archive))
        os.replace(str(temporary_checksum), str(final_checksum))
        os.chmod(final_archive, 0o600)
        os.chmod(final_checksum, 0o600)
    return ArchiveArtifact(
        archive_path=final_archive,
        checksum_path=final_checksum,
        snapshot_month=snapshot.snapshot_month,
        content_sha256=snapshot.content_sha256,
        archive_sha256=_sha256_file(final_archive),
        history_count=snapshot.history_count,
        state_count=snapshot.state_count,
    )


def _file_fingerprint(path: Path) -> Optional[tuple[int, str]]:
    if not path.is_file():
        return None
    return path.stat().st_size, _sha256_file(path)


def _database_family_fingerprint(
    production: Path,
) -> tuple[tuple[str, Optional[tuple[int, str]]], ...]:
    return tuple(
        (
            str(path.resolve(strict=False)),
            _file_fingerprint(path),
        )
        for path in _production_family_paths(production)
    )


def prove_remote_round_trip(
    artifact: ArchiveArtifact,
    remote_root: str,
    production_db_path: os.PathLike,
    *,
    runner: Callable = subprocess.run,
    rclone_bin: str = DEFAULT_RCLONE_BIN,
    timeout_seconds: int = DEFAULT_TRANSPORT_TIMEOUT_SECONDS,
) -> RoundTripReport:
    production = _validated_production_db_path(production_db_path)
    production_before = _database_family_fingerprint(production)
    with tempfile.TemporaryDirectory(prefix="bnl-relay-round-trip-") as work_dir:
        downloaded = download_archive(
            remote_root,
            artifact.archive_path.name,
            artifact.checksum_path.name,
            Path(work_dir) / "downloaded",
            runner=runner,
            rclone_bin=rclone_bin,
            timeout_seconds=timeout_seconds,
        )
        snapshot = verify_archive(
            downloaded.archive_path,
            downloaded.checksum_path,
        )
        target = Path(work_dir) / "isolated-relay-restore.db"
        first = restore_to_isolated_db(snapshot, target, production)
        second = restore_to_isolated_db(snapshot, target, production)
        if second.history_inserted != 0 or second.state_inserted != 0:
            raise RelayBackupConflictError("round_trip_restore_not_idempotent")
    production_after = _database_family_fingerprint(production)
    untouched = production_before == production_after
    if not untouched:
        raise RelayBackupConflictError("production_database_changed")
    return RoundTripReport(
        downloaded_archive_sha256=downloaded.archive_sha256,
        downloaded_content_sha256=downloaded.content_sha256,
        downloaded_history_count=downloaded.history_count,
        downloaded_state_count=downloaded.state_count,
        first_restore=first,
        second_restore=second,
        production_db_untouched=True,
    )


def scheduled_backup_enabled(
    environ: Optional[Mapping[str, str]] = None,
) -> bool:
    source = os.environ if environ is None else environ
    return str(source.get(SCHEDULE_ENABLED_ENV, "")).strip().lower() == "true"


def _artifact_from_verified_snapshot(snapshot: RelaySnapshot) -> ArchiveArtifact:
    return ArchiveArtifact(
        archive_path=snapshot.archive_path,
        checksum_path=snapshot.checksum_path,
        snapshot_month=snapshot.snapshot_month,
        content_sha256=snapshot.content_sha256,
        archive_sha256=_sha256_file(snapshot.archive_path),
        history_count=snapshot.history_count,
        state_count=snapshot.state_count,
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Export and prove Relay-only accepted-history backups.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    export_parser = subparsers.add_parser("export")
    export_parser.add_argument("--db", required=True)
    export_parser.add_argument("--month", required=True)
    export_parser.add_argument("--output-dir", required=True)

    verify_parser = subparsers.add_parser("verify")
    verify_parser.add_argument("--archive", required=True)
    verify_parser.add_argument("--checksum", required=True)

    upload_parser = subparsers.add_parser("upload")
    upload_parser.add_argument("--archive", required=True)
    upload_parser.add_argument("--checksum", required=True)
    upload_parser.add_argument("--remote", required=True)
    upload_parser.add_argument("--rclone-bin", default=DEFAULT_RCLONE_BIN)

    restore_parser = subparsers.add_parser("restore")
    restore_parser.add_argument("--archive", required=True)
    restore_parser.add_argument("--checksum", required=True)
    restore_parser.add_argument("--target-db", required=True)
    restore_parser.add_argument("--production-db", required=True)

    round_trip_parser = subparsers.add_parser("round-trip")
    round_trip_parser.add_argument("--archive", required=True)
    round_trip_parser.add_argument("--checksum", required=True)
    round_trip_parser.add_argument("--remote", required=True)
    round_trip_parser.add_argument("--production-db", required=True)
    round_trip_parser.add_argument("--rclone-bin", default=DEFAULT_RCLONE_BIN)

    scheduled_parser = subparsers.add_parser("scheduled-run")
    scheduled_parser.add_argument("--db", required=True)
    scheduled_parser.add_argument("--output-dir", required=True)
    scheduled_parser.add_argument("--remote", required=True)
    scheduled_parser.add_argument("--rclone-bin", default=DEFAULT_RCLONE_BIN)
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "scheduled-run":
            if not scheduled_backup_enabled():
                raise RelayBackupValidationError(
                    "scheduled_backup_disabled_fail_closed"
                )
            month = datetime.now(timezone.utc).strftime("%Y-%m")
            artifact = export_monthly_snapshot(
                args.db,
                args.output_dir,
                month,
            )
            upload_archive(
                artifact,
                args.remote,
                rclone_bin=args.rclone_bin,
            )
            print(
                json.dumps(
                    {
                        "status": "uploaded",
                        "archive": artifact.archive_path.name,
                        "history_count": artifact.history_count,
                        "state_count": artifact.state_count,
                    },
                    sort_keys=True,
                )
            )
            return 0
        if args.command == "export":
            artifact = export_monthly_snapshot(
                args.db,
                args.output_dir,
                args.month,
            )
            print(
                json.dumps(
                    {
                        "status": "exported",
                        "archive": str(artifact.archive_path),
                        "checksum": str(artifact.checksum_path),
                        "history_count": artifact.history_count,
                        "state_count": artifact.state_count,
                    },
                    sort_keys=True,
                )
            )
            return 0
        snapshot = verify_archive(args.archive, args.checksum)
        if args.command == "verify":
            print(
                json.dumps(
                    {
                        "status": "verified",
                        "snapshot_month": snapshot.snapshot_month,
                        "history_count": snapshot.history_count,
                        "state_count": snapshot.state_count,
                    },
                    sort_keys=True,
                )
            )
            return 0
        artifact = _artifact_from_verified_snapshot(snapshot)
        if args.command == "upload":
            upload_archive(
                artifact,
                args.remote,
                rclone_bin=args.rclone_bin,
            )
            print(
                json.dumps(
                    {
                        "status": "uploaded",
                        "archive": artifact.archive_path.name,
                    },
                    sort_keys=True,
                )
            )
            return 0
        if args.command == "restore":
            report = restore_to_isolated_db(
                snapshot,
                args.target_db,
                args.production_db,
            )
            print(
                json.dumps(
                    {
                        "status": "restored",
                        "target_db": str(report.target_db_path),
                        "history_inserted": report.history_inserted,
                        "history_existing": report.history_existing,
                        "state_inserted": report.state_inserted,
                        "state_existing": report.state_existing,
                    },
                    sort_keys=True,
                )
            )
            return 0
        report = prove_remote_round_trip(
            artifact,
            args.remote,
            args.production_db,
            rclone_bin=args.rclone_bin,
        )
        print(
            json.dumps(
                {
                    "status": "round_trip_verified",
                    "history_count": report.first_restore.history_count,
                    "state_count": report.first_restore.state_count,
                    "second_restore_inserted": (
                        report.second_restore.history_inserted
                        + report.second_restore.state_inserted
                    ),
                    "production_db_untouched": (
                        report.production_db_untouched
                    ),
                },
                sort_keys=True,
            )
        )
        return 0
    except RelayBackupError as exc:
        print(f"relay_backup_error:{exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
