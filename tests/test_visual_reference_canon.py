"""Neutral synthetic pixels exercise the existing canon lifecycle and blob boundary."""
import copy
from contextlib import closing
import hashlib
import importlib.util
import os
from pathlib import Path
import sqlite3
import struct
import tempfile
import unittest
from unittest import mock
import zlib

import bnl_declared_canon as declared


def png(red):
    def chunk(kind, data):
        return struct.pack(">I", len(data)) + kind + data + struct.pack(">I", zlib.crc32(kind + data))
    return (b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", struct.pack(">IIBBBBB", 1, 1, 8, 6, 0, 0, 0))
            + chunk(b"IDAT", zlib.compress(bytes((0, red, 0, 0, 255)))) + chunk(b"IEND", b""))


class VisualReferenceCanonTests(unittest.TestCase):
    def setUp(self):
        self.assertIsNotNone(importlib.util.find_spec("bnl_visual_references"), "visual reference adapter is missing")
        import bnl_visual_references
        self.visual = bnl_visual_references
        self.env = mock.patch.dict(os.environ, {
            "BNL_OWNER_USER_ID": "61", "BNL_PRIMARY_GUILD_ID": "7",
            "BNL_DECLARED_CANON_AUTHORITY_SECRET": "visual-reference-test-signing-secret-0001",
        })
        self.env.start()
        self.addCleanup(self.env.stop)
        # Honor the explicitly selected test volume without tempfile's fallback.
        scratch = os.environ.get("TEMP") or os.environ.get("TMPDIR")
        self.temp = tempfile.TemporaryDirectory(prefix="visual-reference-test-", dir=scratch)
        self.addCleanup(self.temp.cleanup)
        self.db = Path(self.temp.name) / "test.sqlite"
        self.conn = sqlite3.connect(self.db)
        self.addCleanup(self.conn.close)
        declared.ensure_declared_canon_schema(self.conn)
        self.root = self.db.parent / "reference_assets"
        self.root.mkdir()
        self.pixels = tuple(png(i) for i in (10, 20, 30, 40))
        self.assets = []
        for data in self.pixels:
            digest = hashlib.sha256(data).hexdigest()
            (self.root / (digest + ".png")).write_bytes(data)
            self.assets.append({"sha256": digest, "mimeType": "image/png", "bytes": len(data)})
        self.payload = {"version": 1, "subjectId": "test_character", "label": "Test Character",
                        "use": "appearance_only_when_depicted", "assets": self.assets}
        self.fields = {"guild_id": 7, "subject_type": "character", "subject_id": "test_character",
                       "predicate": "appearance_references", "value": self.payload,
                       "raw_declaration": "Use Test Character references only when depicting Test Character.",
                       "cleaned_summary": "Test Character has appearance references for optional depiction.",
                       "domain": "lore", "claim_kind": "identity", "visibility": "reference_canon",
                       "eligible_routes": ("reference_canon",)}

    def add(self, nonce="visual-add-0001", **overrides):
        return declared.add_declared_canon(self.conn, actor_user_id=61, authority_nonce=nonce,
                                          **{**self.fields, **overrides}).primary

    def read(self, guild_id=7):
        return self.visual.read_visual_reference_snapshot(self.db, guild_id, subject_id="test_character")

    def load(self, snapshot=None, subjects=("test_character",)):
        return self.visual.load_visual_reference_inputs(self.db, 7, subjects,
                                                        snapshot=snapshot, subject_id="test_character")

    def test_selected_reference_returns_original_pixels_with_safe_availability(self):
        self.add()
        snapshot = self.read()
        self.assertEqual(self.visual.visual_reference_availability(snapshot),
                         ({"subjectId": "test_character", "label": "Test Character"},))
        selected, images = self.load(snapshot)
        self.assertEqual(selected, snapshot)
        self.assertEqual(tuple(image["data"] for image in images), self.pixels)
        self.assertTrue(all(image["mimeType"] == "image/png" for image in images))
        self.assertTrue(self.visual.visual_reference_snapshot_current(self.db, 7, snapshot))
        self.assertNotIn("path", str(snapshot).lower())

    def test_unselected_subject_does_not_read_database_or_pixels(self):
        self.add()
        snapshot = self.read()
        with mock.patch.object(sqlite3, "connect", side_effect=AssertionError("unexpected database I/O")), \
             mock.patch.object(os, "open", side_effect=AssertionError("unexpected pixel I/O")):
            self.assertEqual(self.load(snapshot, ("unrelated_character",)), (None, ()))
            self.assertEqual(self.load(None, ()), (None, ()))
            self.assertEqual(self.load(snapshot, ("Do not depict test_character",)), (None, ()))

    def test_cross_guild_and_missing_database_are_unavailable_without_creation(self):
        self.add()
        self.assertIsNone(self.read(8))
        missing = self.db.parent / "missing.sqlite"
        self.assertIsNone(self.visual.read_visual_reference_snapshot(missing, 7))
        self.assertFalse(missing.exists())

    def test_expired_and_future_declarations_are_unavailable(self):
        for times in ({"valid_until": "2001-01-01T00:00:00Z"}, {"valid_from": "2999-01-01T00:00:00Z"}):
            with self.subTest(times=times):
                revision = self.add(nonce="time-" + str(len(times)) + next(iter(times)), **times)
                self.assertIsNone(self.read())
                declared.retire_declared_canon(self.conn, actor_user_id=61, authority_nonce="retire-" + revision.declaration_id,
                    guild_id=7, declaration_id=revision.declaration_id, expected_revision_id=revision.revision_id)

    def test_correction_and_retirement_invalidate_saved_snapshot(self):
        first = self.add()
        snapshot = self.read()
        changed = copy.deepcopy(self.payload)
        changed["label"] = "Test Character Updated"
        second = declared.correct_declared_canon(self.conn, actor_user_id=61, authority_nonce="visual-correct-0001",
            declaration_id=first.declaration_id, expected_revision_id=first.revision_id,
            **{**self.fields, "value": changed}).primary
        self.assertFalse(self.visual.visual_reference_snapshot_current(self.db, 7, snapshot))
        with self.assertRaises(self.visual.VisualReferenceError):
            self.load(snapshot)
        current = self.read()
        self.assertEqual(current["label"], "Test Character Updated")
        declared.retire_declared_canon(self.conn, actor_user_id=61, authority_nonce="visual-retire-0001", guild_id=7,
            declaration_id=second.declaration_id, expected_revision_id=second.revision_id)
        self.assertIsNone(self.read())
        self.assertFalse(self.visual.visual_reference_snapshot_current(self.db, 7, current))

    def test_multiple_current_declarations_fail_closed(self):
        self.add()
        self.add(nonce="visual-add-0002")
        self.assertIsNone(self.read())

    def test_wrong_authority_secret_fails_closed(self):
        self.add()
        with mock.patch.dict(os.environ, {"BNL_DECLARED_CANON_AUTHORITY_SECRET": "different-invalid-test-signing-key-0002"}):
            self.assertIsNone(self.read())

    def test_private_visibility_and_extra_public_route_fail_closed(self):
        for number, overrides in enumerate((
                {"visibility": "private"}, {"eligible_routes": ("reference_canon", "public_home")},
                {"eligible_routes": ()})):
            with self.subTest(overrides=overrides):
                created = self.add(nonce="scope-add-%04d" % number, **overrides)
                self.assertIsNone(self.read())
                declared.retire_declared_canon(self.conn, actor_user_id=61, authority_nonce="scope-retire-%04d" % number,
                    guild_id=7, declaration_id=created.declaration_id, expected_revision_id=created.revision_id)

    def test_malformed_descriptors_and_paths_fail_closed(self):
        changes = ({"subjectId": "unrelated_character"}, {"assets": self.assets[:3]},
                   {"assets": self.assets + self.assets[:1]}, {"use": "always_include"},
                   {"path": "/not-an-allowed-root"}, {"label": "Test\nPrivate instruction"},
                   {"assets": [{**self.assets[0], "sha256": "../escape"}] + self.assets[1:]},
                   {"assets": [{**self.assets[0], "bytes": True}] + self.assets[1:]},
                   {"assets": [self.assets[0]] * 4},
                   {"assets": [{**self.assets[0], "bytes": 8 * 1024 * 1024 + 1}] + self.assets[1:]},
                   {"assets": [{**asset, "bytes": 5 * 1024 * 1024} for asset in self.assets]})
        for number, change in enumerate(changes):
            with self.subTest(change=change):
                created = self.add(nonce="malformed-add-%04d" % number, value={**self.payload, **change})
                self.assertIsNone(self.read())
                declared.retire_declared_canon(self.conn, actor_user_id=61, authority_nonce="malformed-retire-%04d" % number,
                    guild_id=7, declaration_id=created.declaration_id, expected_revision_id=created.revision_id)

    def test_changed_blob_invalidates_original_snapshot(self):
        self.add()
        snapshot = self.read()
        blob = self.root / (self.assets[0]["sha256"] + ".png")
        blob.write_bytes(png(99))
        self.assertFalse(self.visual.visual_reference_snapshot_current(self.db, 7, snapshot))
        with self.assertRaises(self.visual.VisualReferenceError):
            self.load(snapshot)

    def test_missing_blob_and_changed_snapshot_cannot_be_used(self):
        self.add()
        snapshot = self.read()
        forged = copy.deepcopy(snapshot)
        forged["assets"][0]["sha256"] = "0" * 64
        with self.assertRaises(self.visual.VisualReferenceError):
            self.load(forged)
        blob = self.root / (self.assets[0]["sha256"] + ".png")
        blob.rename(blob.with_suffix(".unavailable"))
        self.assertFalse(self.visual.visual_reference_snapshot_current(self.db, 7, snapshot))
        with self.assertRaises(self.visual.VisualReferenceError):
            self.load(snapshot)

    def test_no_schema_is_added_to_uninitialized_database(self):
        empty = self.db.parent / "empty.sqlite"
        with closing(sqlite3.connect(empty)) as conn, conn:
            conn.execute("CREATE TABLE unrelated (value TEXT)")
        self.assertIsNone(self.visual.read_visual_reference_snapshot(empty, 7))
        with closing(sqlite3.connect(empty)) as conn:
            self.assertEqual(conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall(),
                             [("unrelated",)])

    def test_selected_subject_without_authorized_reference_is_refused(self):
        with self.assertRaises(self.visual.VisualReferenceError):
            self.load()

    def test_signed_non_png_blob_cannot_enter_provider_request(self):
        data = b"not a PNG" * 20
        digest = hashlib.sha256(data).hexdigest()
        (self.root / (digest + ".png")).write_bytes(data)
        assets = [{"sha256": digest, "mimeType": "image/png", "bytes": len(data)}] + self.assets[1:]
        self.add(value={**self.payload, "assets": assets})
        with self.assertRaises(self.visual.VisualReferenceError):
            self.load(self.read())

    def test_reparse_metadata_blocks_blob_read(self):
        self.add()
        snapshot = self.read()
        original = Path.lstat
        def stat_with_reparse(path):
            result = original(path)
            if path == self.root:
                return type("ReparseStat", (), {"st_mode": result.st_mode, "st_file_attributes": 0x400})()
            return result
        # Windows junction creation needs platform privileges; emulate only the OS flag.
        with mock.patch.object(Path, "lstat", stat_with_reparse):
            with self.assertRaises(self.visual.VisualReferenceError):
                self.load(snapshot)


if __name__ == "__main__":
    unittest.main()
