"""Read optional appearance pixels through the existing Declared Canon owner.

This adapter stores no authority, memory, or metadata. Private content-addressed
files are transport assets; only a current owner declaration permits their use.
"""
from __future__ import annotations

from contextlib import closing
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import struct

from bnl_canon_source_contract import _inventory_current_declared_claim
from bnl_declared_canon import validate_declared_canon_read_boundary


MAX_REFERENCE_BYTES = 8 * 1024 * 1024
MAX_TOTAL_REFERENCE_BYTES = 16 * 1024 * 1024
REFERENCE_COUNT = 4
_SUBJECT = re.compile(r"[a-z0-9][a-z0-9_.:-]{0,95}\Z")
_LABEL = re.compile(r"[A-Za-z0-9][A-Za-z0-9 .'-]{0,71}\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


class VisualReferenceError(ValueError):
    """A bounded, content-free refusal to supply reference pixels."""


def _safe_identity(subject_id, label) -> bool:
    return bool(isinstance(subject_id, str) and _SUBJECT.fullmatch(subject_id)
                and isinstance(label, str) and _LABEL.fullmatch(label)
                and (subject_id != "6_bit" or label == "6 Bit"))


def _reference_value(value, subject_id):
    if (not isinstance(value, dict)
            or set(value) != {"version", "subjectId", "label", "use", "assets"}
            or type(value["version"]) is not int or value["version"] != 1
            or value["subjectId"] != subject_id
            or not _safe_identity(subject_id, value["label"])
            or value["use"] != "appearance_only_when_depicted"
            or not isinstance(value["assets"], list)
            or len(value["assets"]) != REFERENCE_COUNT):
        return None
    assets = []
    hashes = set()
    total = 0
    for asset in value["assets"]:
        if (not isinstance(asset, dict) or set(asset) != {"sha256", "mimeType", "bytes"}
                or not isinstance(asset["sha256"], str) or not _SHA256.fullmatch(asset["sha256"])
                or asset["sha256"] in hashes or asset["mimeType"] != "image/png"
                or type(asset["bytes"]) is not int or not 45 <= asset["bytes"] <= MAX_REFERENCE_BYTES):
            return None
        total += asset["bytes"]
        if total > MAX_TOTAL_REFERENCE_BYTES:
            return None
        hashes.add(asset["sha256"])
        assets.append(dict(asset))
    return assets


def _no_link_components(path: Path):
    """Check before resolving, including Windows junction/reparse attributes."""
    for component in (*reversed(path.parents), path):
        info = component.lstat()
        if stat.S_ISLNK(info.st_mode) or getattr(info, "st_file_attributes", 0) & 0x400:
            raise VisualReferenceError("visual_reference_link_forbidden")
    return info


def read_visual_reference_snapshot(db_file, guild_id: int, *, subject_id="6_bit") -> dict | None:
    """Return private descriptors, never pixels; no schema creation or writes.

    The authenticated declaration's latest revision is the sole authority.
    Unrelated declarations are not a second source of appearance evidence.
    """
    if (type(guild_id) is not int or guild_id <= 0
            or not isinstance(subject_id, str) or not _SUBJECT.fullmatch(subject_id)):
        return None
    try:
        db_path = Path(db_file).absolute()
        if not stat.S_ISREG(_no_link_components(db_path).st_mode):
            return None
        with closing(sqlite3.connect(db_path.as_uri() + "?mode=ro", uri=True, timeout=0.5)) as conn:
            conn.execute("BEGIN")
            revisions = validate_declared_canon_read_boundary(conn, guild_id=guild_id)
            candidates = [revision for revision in revisions
                          if revision.source_system == "general_declaration"
                          and revision.subject_id == subject_id
                          and revision.predicate == "appearance_references"
                          and revision.lifecycle_status == "established"]
            # A second active declaration cannot silently override the first.
            if len(candidates) != 1:
                return None
            revision = candidates[0]
            claim, _ = _inventory_current_declared_claim(conn, revision, now=datetime.now(timezone.utc))
            if (claim is None or revision.subject_type not in {"person", "character", "entity"}
                    or revision.visibility not in {"public_safe", "reference_canon"}
                    or tuple(json.loads(revision.eligible_routes_json)) != ("reference_canon",)):
                return None
            value = json.loads(revision.value_json)
            assets = _reference_value(value, subject_id)
            if assets is None:
                return None
            return {"contractVersion": 1, "guildId": guild_id, "subjectId": subject_id,
                    "label": value["label"], "declarationId": revision.declaration_id,
                    "revisionId": revision.revision_id, "sourceFingerprint": revision.source_fingerprint,
                    "assets": assets}
    except (OSError, sqlite3.Error, ValueError, TypeError, KeyError):
        return None


def visual_reference_availability(snapshot) -> tuple[dict, ...]:
    """Only public labels enter concept context; no paths, hashes, or pixels."""
    if (not isinstance(snapshot, dict)
            or not _safe_identity(snapshot.get("subjectId"), snapshot.get("label"))):
        return ()
    return ({"subjectId": snapshot["subjectId"], "label": snapshot["label"]},)


def _read_reference_pixels(db_file, snapshot) -> tuple[dict, ...]:
    root = Path(db_file).absolute().parent / "reference_assets"
    result = []
    try:
        for asset in snapshot["assets"]:
            # No path or filename is accepted from declaration content.
            path = root / (asset["sha256"] + ".png")
            before = _no_link_components(path)
            if not stat.S_ISREG(before.st_mode) or before.st_size != asset["bytes"]:
                raise VisualReferenceError("visual_reference_size_changed")
            flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
            with os.fdopen(os.open(path, flags), "rb") as handle:
                opened = os.fstat(handle.fileno())
                if (not stat.S_ISREG(opened.st_mode)
                        or (opened.st_dev, opened.st_ino, opened.st_size)
                        != (before.st_dev, before.st_ino, before.st_size)):
                    raise VisualReferenceError("visual_reference_file_changed")
                data = handle.read(asset["bytes"] + 1)
            after = _no_link_components(path)
            if ((after.st_dev, after.st_ino, after.st_size) != (before.st_dev, before.st_ino, before.st_size)
                    or len(data) != asset["bytes"] or hashlib.sha256(data).hexdigest() != asset["sha256"]):
                raise VisualReferenceError("visual_reference_hash_changed")
            if (data[:8] != b"\x89PNG\r\n\x1a\n" or data[8:16] != b"\x00\x00\x00\rIHDR"
                    or data[-12:] != b"\x00\x00\x00\x00IEND\xaeB`\x82"):
                raise VisualReferenceError("visual_reference_png_invalid")
            width, height = struct.unpack(">II", data[16:24])
            if not (0 < width <= 4096 and 0 < height <= 4096):
                raise VisualReferenceError("visual_reference_dimensions_invalid")
            result.append({"mimeType": "image/png", "data": data})
    except (OSError, ValueError, TypeError, KeyError, struct.error) as exc:
        if isinstance(exc, VisualReferenceError):
            raise
        raise VisualReferenceError("visual_reference_pixels_unavailable") from None
    return tuple(result)


def load_visual_reference_inputs(db_file, guild_id: int, depicted_subjects, *,
                                 snapshot=None, subject_id="6_bit") -> tuple[dict | None, tuple[dict, ...]]:
    """Supply originals only for an explicitly selected exact subject identity."""
    selected_id = snapshot.get("subjectId") if isinstance(snapshot, dict) else subject_id
    if (not isinstance(depicted_subjects, (list, tuple))
            or selected_id not in depicted_subjects):
        return None, ()
    current = read_visual_reference_snapshot(db_file, guild_id, subject_id=selected_id)
    if current is None or (snapshot is not None and current != snapshot):
        raise VisualReferenceError("visual_reference_source_changed_or_unavailable")
    images = _read_reference_pixels(db_file, current)
    if read_visual_reference_snapshot(db_file, guild_id, subject_id=selected_id) != current:
        raise VisualReferenceError("visual_reference_source_changed")
    return current, images


def visual_reference_snapshot_current(db_file, guild_id: int, snapshot) -> bool:
    """Revalidate authority and original pixels before provider use or delivery."""
    if not isinstance(snapshot, dict) or not isinstance(snapshot.get("subjectId"), str):
        return False
    try:
        current, _ = load_visual_reference_inputs(db_file, guild_id, (snapshot["subjectId"],), snapshot=snapshot)
        return current == snapshot
    except (VisualReferenceError, TypeError, ValueError):
        return False
