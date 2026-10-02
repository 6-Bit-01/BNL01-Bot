"""Capacity-checked SQLite copies and verified archives for operator workflows.

This is a caller-owned utility, not a scheduler or retention policy. Production
is opened read-only; only explicitly selected private copies may be archived.
"""

import argparse
from contextlib import closing, contextmanager
from datetime import datetime, timezone
import gzip
import hashlib
import json
import math
import os
from pathlib import Path
import shutil
import sqlite3
import stat
import subprocess
import sys
import tempfile
import time


DEFAULT_RESERVE_BYTES = 5 * 1024**3
PRODUCTION_DB = Path("/home/ubuntu/bnl01/bnl01_conversations.db")
BLOCK_BYTES = 1024**2
REHEARSAL_LOCK_PATH = Path(
    "/tmp/bnl-sqlite-rehearsal.lock" if os.name != "nt"
    else str(Path(tempfile.gettempdir()) / "bnl-sqlite-rehearsal.lock")
)


class SnapshotSafetyError(RuntimeError):
    """The requested copy cannot safely proceed."""


def _path(value):
    path = Path(value).absolute()
    for part in (path,) + tuple(path.parents):
        if part.is_symlink():
            raise SnapshotSafetyError("symlink paths are not allowed")
    return path.resolve()


def _regular(path):
    info = path.stat()
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
        raise SnapshotSafetyError("expected a regular file with exactly one link")
    return info


def _identity(info):
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns)


def _overhead(size):
    return max(64 * 1024**2, math.ceil(size / 10))


def _space(directory, required):
    if shutil.disk_usage(directory).free < required:
        raise SnapshotSafetyError("insufficient free space for copy and service reserve")


def _reserve(value):
    if not isinstance(value, int) or value < 0:
        raise ValueError("reserve_bytes must be a nonnegative integer")
    return value


def _sync_directory(directory):
    if os.name != "nt":
        fd = os.open(str(directory), os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


def _exclusive(path):
    return os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_EXCL
                   | getattr(os, "O_NOFOLLOW", 0), 0o600)


def _publish(partial, final):
    # A hard link publishes without overwriting a concurrently created target.
    os.link(str(partial), str(final))
    partial.unlink()
    _sync_directory(final.parent)


def _receipt(path, data):
    partial = Path(str(path) + ".partial")
    fd = _exclusive(partial)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(data, stream, sort_keys=True, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        _publish(partial, path)
    finally:
        if partial.exists():
            partial.unlink()


def _digest(path, compressed=False):
    digest = hashlib.sha256()
    size = 0
    opener = gzip.open if compressed else open
    with opener(path, "rb") as stream:
        for block in iter(lambda: stream.read(BLOCK_BYTES), b""):
            digest.update(block)
            size += len(block)
    return digest.hexdigest(), size


def _private_file(path, owner_uid):
    info = _regular(path)
    if info.st_uid != owner_uid or info.st_mode & 0o077:
        raise SnapshotSafetyError("archive input must be owned by the expected user and private")
    return info


def _assert_no_sidecars(path):
    for suffix in ("-wal", "-shm", "-journal"):
        if Path(str(path) + suffix).exists():
            raise SnapshotSafetyError("snapshot has SQLite sidecars; close/checkpoint it first")


def _assert_unprotected(path, protected_paths):
    for protected in (PRODUCTION_DB,) + tuple(protected_paths):
        protected = _path(protected)
        for member in (protected,) + tuple(Path(str(protected) + suffix)
                                            for suffix in ("-wal", "-shm", "-journal")):
            if path == member or (member.exists() and path.exists()
                                  and os.path.samefile(path, member)):
                raise SnapshotSafetyError("cannot archive a protected live/rollback database or sidecar")


def _assert_inactive(path, *, owner_uid=None, scan_all_users=False):
    """Reject same-user open handles; private copies must remain owner-only.

    Linux's service account owns the rehearsals. Checking that account's /proc
    handles avoids assuming that a PID in an old receipt is still authoritative.
    On other platforms archiving fails closed; backup creation remains usable.
    """
    if not Path("/proc/self/fd").is_dir() or not hasattr(os, "geteuid"):
        raise SnapshotSafetyError("archive open-file checks require Linux /proc")
    owner_uid = os.geteuid() if owner_uid is None else owner_uid
    info = _private_file(path, owner_uid)
    for process in Path("/proc").iterdir():
        if not process.name.isdigit():
            continue
        try:
            if not scan_all_users and process.stat().st_uid != owner_uid:
                continue
            handles = list((process / "fd").iterdir())
        except FileNotFoundError:
            continue
        except PermissionError as exc:
            raise SnapshotSafetyError("cannot establish that snapshot is inactive") from exc
        for handle in handles:
            try:
                target = handle.stat()
            except FileNotFoundError:
                continue
            except PermissionError as exc:
                raise SnapshotSafetyError("cannot inspect snapshot users") from exc
            if (target.st_dev, target.st_ino) == (info.st_dev, info.st_ino):
                raise SnapshotSafetyError("snapshot is still open by a process")
    _assert_no_sidecars(path)


def inspect_snapshot(path, *, owner_uid, protected_paths=()):
    """Read-only privileged inspection; never copy, compress, unlink or chmod.

    The ordinary caller supplies its own UID. Even root must verify that the
    selected file belongs to that user, is private, and is not protected.
    """
    if not hasattr(os, "geteuid") or os.geteuid() != 0:
        raise SnapshotSafetyError("privileged inspection requires an explicit root invocation")
    if type(owner_uid) is not int or owner_uid < 0:
        raise SnapshotSafetyError("expected owner UID is required")
    path = _path(path)
    _assert_unprotected(path, protected_paths)
    before = _identity(_private_file(path, owner_uid))
    _assert_inactive(path, owner_uid=owner_uid, scan_all_users=True)
    if _identity(_private_file(path, owner_uid)) != before:
        raise SnapshotSafetyError("snapshot changed during inspection")
    return {"kind": "inactive_snapshot", "path": str(path), "owner_uid": owner_uid,
            "identity": list(before)}


def _verify_inactive(path, *, protected_paths=(), privileged_inspection=False):
    if not privileged_inspection:
        _assert_inactive(path)
        return
    if not hasattr(os, "geteuid"):
        raise SnapshotSafetyError("privileged inspection requires Linux")
    owner_uid = os.geteuid()
    before = _identity(_private_file(path, owner_uid))
    _assert_unprotected(path, protected_paths)
    _assert_no_sidecars(path)
    command = ["sudo", "-n", sys.executable, str(Path(__file__).resolve()),
               "inspect", "--path", str(path), "--owner-uid", str(owner_uid)]
    for protected in (PRODUCTION_DB,) + tuple(protected_paths):
        command.extend(["--protect", str(_path(protected))])
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=False, timeout=30)
        if result.returncode != 0:
            raise SnapshotSafetyError("privileged snapshot inspection failed; raw copy retained")
        receipt = json.loads(result.stdout)
    except (OSError, subprocess.TimeoutExpired, ValueError) as exc:
        raise SnapshotSafetyError("privileged snapshot inspection unavailable; raw copy retained") from exc
    expected = {"kind": "inactive_snapshot", "path": str(path), "owner_uid": owner_uid,
                "identity": list(before)}
    if receipt != expected or _identity(_private_file(_path(path), owner_uid)) != before:
        raise SnapshotSafetyError("privileged inspection did not match this snapshot")
    _assert_unprotected(path, protected_paths)
    _assert_no_sidecars(path)


@contextmanager
def _rehearsal_lock():
    path = _path(REHEARSAL_LOCK_PATH)
    fd = os.open(str(path), os.O_RDWR | os.O_CREAT
                 | getattr(os, "O_NOFOLLOW", 0), 0o600)
    locked = False
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode) or os.fstat(fd).st_nlink != 1:
            raise SnapshotSafetyError("unsafe rehearsal lock")
        try:
            if os.name == "nt":
                import msvcrt
                if os.fstat(fd).st_size == 0:
                    os.write(fd, b"0")
                os.lseek(fd, 0, os.SEEK_SET)
                msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            locked = True
        except OSError as exc:
            raise SnapshotSafetyError("another managed rehearsal is active") from exc
        yield
    finally:
        if locked:
            if os.name == "nt":
                import msvcrt
                os.lseek(fd, 0, os.SEEK_SET)
                msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


def create_snapshot(source, destination, *, reserve_bytes=DEFAULT_RESERVE_BYTES,
                    archive_headroom=False, timeout_seconds=900):
    """Create one verified raw backup; never overwrite or prune an older one."""
    reserve_bytes = _reserve(reserve_bytes)
    source, destination = _path(source), _path(destination)
    _regular(source)
    if not destination.parent.is_dir():
        raise SnapshotSafetyError("destination directory must already exist")
    for suffix in ("", "-wal", "-shm", "-journal", ".partial",
                   ".partial-wal", ".partial-shm", ".partial-journal",
                   ".snapshot.json", ".snapshot.json.partial",
                   ".gz", ".gz.partial", ".archive.json", ".archive.json.partial"):
        if Path(str(destination) + suffix).exists():
            raise SnapshotSafetyError("destination or prior snapshot artifact already exists")
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")
    partial = Path(str(destination) + ".partial")
    created = False
    started = time.monotonic()
    with closing(sqlite3.connect(source.as_uri() + "?mode=ro", uri=True, timeout=5)) as live:
        page_size = live.execute("PRAGMA page_size").fetchone()[0]
        allocation = max(source.stat().st_size,
                         live.execute("PRAGMA page_count").fetchone()[0] * page_size)
        overhead = _overhead(allocation)
        extra = allocation + overhead if archive_headroom else 0
        _space(destination.parent, allocation + reserve_bytes + overhead + extra)
        try:
            fd = _exclusive(partial)
            os.close(fd)
            created = True

            def progress(status, remaining, total):
                if time.monotonic() - started > timeout_seconds:
                    raise SnapshotSafetyError("snapshot exceeded its time limit")
                _space(destination.parent,
                       remaining * page_size + reserve_bytes + overhead + extra)

            with closing(sqlite3.connect(str(partial), timeout=5)) as copy:
                live.backup(copy, pages=256, progress=progress, sleep=0.05)
                if copy.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                    raise SnapshotSafetyError("snapshot failed quick_check")
            digest, size = _digest(partial)
            _space(destination.parent, reserve_bytes + overhead + extra)
            with open(partial, "r+b") as stream:
                os.fsync(stream.fileno())
            _publish(partial, destination)
            _receipt(Path(str(destination) + ".snapshot.json"), {
                "kind": "sqlite_backup", "source": str(source),
                "path": str(destination), "sha256": digest, "bytes": size,
                "quick_check": "ok", "created_at": datetime.now(timezone.utc).isoformat(),
            })
        finally:
            if created:
                for owned in (partial,) + tuple(Path(str(partial) + suffix)
                                                for suffix in ("-wal", "-shm", "-journal")):
                    if owned.exists():
                        _regular(_path(owned))
                        owned.unlink()
    return destination


def archive_snapshot(path, *, protected_paths=(), reserve_bytes=DEFAULT_RESERVE_BYTES,
                     privileged_inspection=False):
    """Archive an explicit inactive copy; verified existing archives are resumable.

    Pass the current production DB and latest retained deployment rollback in
    protected_paths. The standard production DB is always protected. Raw input
    is removed only after the archive and matching receipt are durable.
    """
    reserve_bytes = _reserve(reserve_bytes)
    path = _path(path)
    _assert_unprotected(path, protected_paths)
    _verify_inactive(path, protected_paths=protected_paths, privileged_inspection=privileged_inspection)
    before = _identity(_regular(path))
    archive = _path(str(path) + ".gz")
    partial = _path(str(archive) + ".partial")
    receipt = _path(str(path) + ".archive.json")
    digest, size = _digest(path)
    if archive.exists():
        _regular(archive)
        if _digest(archive, compressed=True) != (digest, size):
            raise SnapshotSafetyError("existing archive does not match raw snapshot")
    else:
        if receipt.exists():
            raise SnapshotSafetyError("archive receipt exists without an archive")
        _space(path.parent, size + _overhead(size) + reserve_bytes)
        fd = _exclusive(partial)
        try:
            with os.fdopen(fd, "wb") as output:
                with gzip.GzipFile(filename="", mode="wb", fileobj=output, compresslevel=1) as zipped:
                    with open(path, "rb") as source:
                        for block in iter(lambda: source.read(BLOCK_BYTES), b""):
                            _space(path.parent, reserve_bytes + _overhead(size))
                            zipped.write(block)
                output.flush()
                os.fsync(output.fileno())
            if _digest(partial, compressed=True) != (digest, size):
                raise SnapshotSafetyError("archive verification failed")
            _publish(partial, archive)
        finally:
            if partial.exists():
                partial.unlink()
    data = {"kind": "verified_gzip", "path": str(path), "archive": str(archive),
            "sha256": digest, "bytes": size, "archive_bytes": archive.stat().st_size}
    if receipt.exists():
        _regular(receipt)
        if json.loads(receipt.read_text(encoding="utf-8")) != data:
            raise SnapshotSafetyError("archive receipt does not match snapshot")
    else:
        _receipt(receipt, data)
    _verify_inactive(path, protected_paths=protected_paths, privileged_inspection=privileged_inspection)
    if _identity(_regular(path)) != before:
        raise SnapshotSafetyError("snapshot changed during archive; raw copy retained")
    path.unlink()
    _sync_directory(path.parent)
    return archive


@contextmanager
def rehearsal_snapshot(source, destination, *, reserve_bytes=DEFAULT_RESERVE_BYTES,
                       privileged_inspection=False):
    """Hold one host rehearsal slot and archive its copy even if the run fails.

    The caller must close all snapshot connections before exiting this context.
    Archiving failure retains the raw copy and raises; it never deletes evidence.
    """
    with _rehearsal_lock():
        path = create_snapshot(source, destination, reserve_bytes=reserve_bytes,
                               archive_headroom=True)
        try:
            yield path
        finally:
            archive_snapshot(path, protected_paths=(source,), reserve_bytes=reserve_bytes,
                             privileged_inspection=privileged_inspection)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    backup = commands.add_parser("backup")
    backup.add_argument("--source", required=True)
    backup.add_argument("--destination", required=True)
    archive = commands.add_parser("archive")
    archive.add_argument("--path", required=True)
    archive.add_argument("--protect", action="append", required=True,
                         help="explicit live or retained rollback path; repeat as needed")
    archive.add_argument("--privileged-inspection", action="store_true",
                         help="explicitly use sudo -n only for read-only open-file inspection")
    inspect = commands.add_parser("inspect", help="read-only privileged open-file inspection")
    inspect.add_argument("--path", required=True)
    inspect.add_argument("--owner-uid", type=int, required=True)
    inspect.add_argument("--protect", action="append", required=True)
    args = parser.parse_args()
    if args.command == "backup":
        result = create_snapshot(args.source, args.destination)
    elif args.command == "archive":
        result = archive_snapshot(args.path, protected_paths=args.protect,
                                  privileged_inspection=args.privileged_inspection)
    else:
        result = json.dumps(inspect_snapshot(args.path, owner_uid=args.owner_uid,
                                             protected_paths=args.protect), sort_keys=True)
    print(result)


if __name__ == "__main__":
    main()
