import gzip
from contextlib import closing, contextmanager
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

from scripts import sqlite_snapshot as snapshots


class SnapshotTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.source = self.root / "live.sqlite"
        self.target = self.root / "private.sqlite"
        with closing(sqlite3.connect(self.source)) as connection:
            connection.execute("CREATE TABLE evidence(value TEXT)")
            connection.execute("INSERT INTO evidence VALUES ('Test Member')")
            connection.commit()
        self.lock_directory = mock.patch.object(snapshots, "REHEARSAL_LOCK_PATH", self.root / "rehearsal.lock")
        self.lock_directory.start()
        self.addCleanup(self.lock_directory.stop)

    def copy(self, **kwargs):
        return snapshots.create_snapshot(self.source, self.target, reserve_bytes=0, **kwargs)

    def archive(self, **kwargs):
        # Platform-neutral algorithm tests; real Linux open-handle checks below.
        with mock.patch.object(snapshots, "_assert_inactive"):
            return snapshots.archive_snapshot(self.target, protected_paths=(self.source,),
                                              reserve_bytes=0, **kwargs)

    @contextmanager
    def own_process_scan(self):
        """Isolate /proc integration fixtures from unrelated host processes."""
        original = Path.iterdir
        def entries(path):
            if path == Path("/proc"):
                return iter([path / str(os.getpid())])
            return original(path)
        with mock.patch.object(Path, "iterdir", entries):
            yield

    def test_copy_is_verified_and_source_unchanged(self):
        before = snapshots._digest(self.source)
        self.copy()
        with closing(sqlite3.connect(self.target)) as copied:
            self.assertEqual(copied.execute("SELECT value FROM evidence").fetchall(), [("Test Member",)])
        self.assertEqual(snapshots._digest(self.source), before)
        receipt = json.loads(Path(str(self.target) + ".snapshot.json").read_text())
        self.assertEqual(receipt["sha256"], snapshots._digest(self.target)[0])
        self.assertEqual(receipt["quick_check"], "ok")
        self.assertFalse(Path(str(self.target) + ".partial").exists())

    def test_backup_reads_committed_wal_rows(self):
        connection = sqlite3.connect(self.source)
        self.addCleanup(connection.close)
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("INSERT INTO evidence VALUES ('A second committed source')")
        connection.commit()
        self.copy()
        with closing(sqlite3.connect(self.target)) as result:
            self.assertEqual(result.execute("SELECT count(*) FROM evidence").fetchone()[0], 2)

    def test_low_disk_stops_before_partial_creation(self):
        with mock.patch.object(snapshots.shutil, "disk_usage", return_value=mock.Mock(free=1)):
            with self.assertRaisesRegex(snapshots.SnapshotSafetyError, "insufficient"):
                self.copy()
        self.assertFalse(self.target.exists())
        self.assertFalse(Path(str(self.target) + ".partial").exists())

    def test_progress_low_disk_cleans_incomplete_copy(self):
        with mock.patch.object(snapshots, "_space", side_effect=[None, snapshots.SnapshotSafetyError("disk fell")]):
            with self.assertRaisesRegex(snapshots.SnapshotSafetyError, "disk fell"):
                self.copy()
        self.assertFalse(self.target.exists())
        self.assertFalse(Path(str(self.target) + ".partial").exists())
        self.assertEqual(list(self.root.glob("private.sqlite*")), [])

    def test_timeout_cleans_incomplete_copy(self):
        with mock.patch.object(snapshots.time, "monotonic", side_effect=[0, 100]):
            with self.assertRaisesRegex(snapshots.SnapshotSafetyError, "time limit"):
                self.copy(timeout_seconds=1)
        self.assertEqual(list(self.root.glob("private.sqlite*")), [])

    def test_rehearsal_reserves_archive_headroom(self):
        with mock.patch.object(snapshots, "_space") as space:
            self.copy(archive_headroom=True)
        size = self.source.stat().st_size
        self.assertEqual(space.call_args_list[0].args[1], 2 * (size + snapshots._overhead(size)))

    def test_default_service_reserve_is_five_gib(self):
        self.assertEqual(snapshots.DEFAULT_RESERVE_BYTES, 5 * 1024**3)
        with mock.patch.object(snapshots, "_space") as space:
            snapshots.create_snapshot(self.source, self.target)
        self.assertGreater(space.call_args_list[0].args[1], 5 * 1024**3)

    def test_existing_artifacts_are_never_reused_or_overwritten(self):
        for suffix in ("", "-journal", "-wal", "-shm", ".partial", ".partial-wal",
                       ".partial-shm", ".partial-journal", ".gz", ".gz.partial", ".snapshot.json",
                       ".snapshot.json.partial", ".archive.json", ".archive.json.partial"):
            with self.subTest(suffix=suffix):
                existing = Path(str(self.target) + suffix)
                existing.write_bytes(b"existing evidence")
                with self.assertRaisesRegex(snapshots.SnapshotSafetyError, "already exists"):
                    self.copy()
                self.assertEqual(existing.read_bytes(), b"existing evidence")
                existing.unlink()

    def test_backup_receipt_failure_keeps_verified_complete_copy(self):
        with mock.patch.object(snapshots, "_receipt", side_effect=OSError("receipt failed")):
            with self.assertRaisesRegex(OSError, "receipt failed"):
                self.copy()
        self.assertTrue(self.target.exists())
        self.assertFalse(Path(str(self.target) + ".partial").exists())
        with closing(sqlite3.connect(self.target)) as result:
            self.assertEqual(result.execute("PRAGMA quick_check").fetchone()[0], "ok")
        with self.assertRaisesRegex(snapshots.SnapshotSafetyError, "already exists"):
            self.copy()

    def test_source_and_destination_symlinks_rejected(self):
        alias = self.root / "alias.sqlite"
        try:
            alias.symlink_to(self.source)
        except OSError:
            self.skipTest("symlink creation unavailable")
        with self.assertRaisesRegex(snapshots.SnapshotSafetyError, "symlink"):
            snapshots.create_snapshot(alias, self.target, reserve_bytes=0)
        with self.assertRaisesRegex(snapshots.SnapshotSafetyError, "symlink"):
            snapshots.create_snapshot(self.source, alias, reserve_bytes=0)

    def test_parent_symlink_rejected(self):
        alias = self.root / "alias-directory"
        try:
            alias.symlink_to(self.root, target_is_directory=True)
        except OSError:
            self.skipTest("symlink creation unavailable")
        with self.assertRaisesRegex(snapshots.SnapshotSafetyError, "symlink"):
            snapshots.create_snapshot(self.source, alias / "copy.sqlite", reserve_bytes=0)

    def test_host_lock_rejects_second_context_and_releases(self):
        with snapshots._rehearsal_lock():
            with self.assertRaisesRegex(snapshots.SnapshotSafetyError, "another managed rehearsal"):
                with snapshots._rehearsal_lock():
                    self.fail("second lock entered")
        with snapshots._rehearsal_lock():
            pass

    def test_archive_roundtrip_and_receipt_precede_raw_removal(self):
        self.copy()
        digest = snapshots._digest(self.target)
        original_unlink = Path.unlink

        def guarded_unlink(path, *args, **kwargs):
            if path == self.target:
                self.assertTrue(Path(str(path) + ".gz").exists())
                receipt = json.loads(Path(str(path) + ".archive.json").read_text())
                self.assertEqual(receipt["sha256"], digest[0])
                self.assertEqual(snapshots._digest(Path(str(path) + ".gz"), True), digest)
            return original_unlink(path, *args, **kwargs)

        with mock.patch.object(Path, "unlink", guarded_unlink):
            archive = self.archive()
        self.assertFalse(self.target.exists())
        self.assertEqual(snapshots._digest(archive, True), digest)

    def test_archive_low_disk_retains_raw(self):
        self.copy()
        with mock.patch.object(snapshots.shutil, "disk_usage", return_value=mock.Mock(free=0)):
            with self.assertRaises(snapshots.SnapshotSafetyError):
                self.archive()
        self.assertTrue(self.target.exists())
        self.assertFalse(Path(str(self.target) + ".gz.partial").exists())

    def test_archive_failure_cleans_partial_and_retains_raw(self):
        self.copy()
        with mock.patch.object(gzip.GzipFile, "write", side_effect=OSError("storage failure")):
            with self.assertRaisesRegex(OSError, "storage failure"):
                self.archive()
        self.assertTrue(self.target.exists())
        self.assertFalse(Path(str(self.target) + ".gz.partial").exists())

    def test_failed_decompression_verification_retains_raw(self):
        self.copy()
        real_digest = snapshots._digest

        def corrupt_digest(path, compressed=False):
            return ("wrong", 0) if compressed else real_digest(path)

        with mock.patch.object(snapshots, "_digest", side_effect=corrupt_digest):
            with self.assertRaisesRegex(snapshots.SnapshotSafetyError, "verification failed"):
                self.archive()
        self.assertTrue(self.target.exists())
        self.assertFalse(Path(str(self.target) + ".gz").exists())

    def test_receipt_failure_preserves_raw_and_archive_can_resume(self):
        self.copy()
        with mock.patch.object(snapshots, "_receipt", side_effect=OSError("receipt failure")):
            with self.assertRaisesRegex(OSError, "receipt failure"):
                self.archive()
        self.assertTrue(self.target.exists())
        self.assertTrue(Path(str(self.target) + ".gz").exists())
        self.archive()
        self.assertFalse(self.target.exists())

    def test_interrupted_removal_can_resume_with_verified_receipt(self):
        self.copy()
        real_unlink = Path.unlink

        def interrupted(path, *args, **kwargs):
            if path == self.target:
                raise OSError("interrupted cleanup")
            return real_unlink(path, *args, **kwargs)

        with mock.patch.object(Path, "unlink", interrupted):
            with self.assertRaisesRegex(OSError, "interrupted cleanup"):
                self.archive()
        self.assertTrue(self.target.exists())
        self.archive()
        self.assertFalse(self.target.exists())

    def test_conflicting_archive_or_receipt_never_deletes_raw(self):
        self.copy()
        archive = Path(str(self.target) + ".gz")
        with gzip.open(archive, "wb") as stream:
            stream.write(b"different source")
        with self.assertRaisesRegex(snapshots.SnapshotSafetyError, "does not match"):
            self.archive()
        self.assertTrue(self.target.exists())

    def test_protected_live_and_latest_rollback_are_preserved(self):
        self.copy()
        with self.assertRaisesRegex(snapshots.SnapshotSafetyError, "protected"):
            snapshots.archive_snapshot(self.target, protected_paths=(self.target,), reserve_bytes=0)
        with mock.patch.object(snapshots, "PRODUCTION_DB", self.target):
            with self.assertRaisesRegex(snapshots.SnapshotSafetyError, "protected"):
                self.archive()
        self.assertTrue(self.target.exists())

    def test_hardlinked_archive_input_rejected(self):
        self.copy()
        alias = self.root / "another-copy.sqlite"
        os.link(self.target, alias)
        with self.assertRaisesRegex(snapshots.SnapshotSafetyError, "one link"):
            self.archive()
        self.assertTrue(alias.exists())
        self.assertTrue(self.target.exists())

    def test_protected_database_sidecars_and_aliases_are_never_archived(self):
        for suffix in ("-wal", "-shm", "-journal"):
            with self.subTest(suffix=suffix):
                sidecar = Path(str(self.source) + suffix)
                sidecar.write_bytes(b"committed recovery data")
                alias = self.root / ("sidecar-alias" + suffix)
                os.link(sidecar, alias)
                for candidate in (sidecar, alias):
                    with self.assertRaisesRegex(snapshots.SnapshotSafetyError, "protected"):
                        snapshots.archive_snapshot(candidate, protected_paths=(self.source,), reserve_bytes=0)
                self.assertEqual(sidecar.read_bytes(), b"committed recovery data")
                alias.unlink()
                sidecar.unlink()

    def test_change_during_archive_preserves_raw(self):
        self.copy()
        real_receipt = snapshots._receipt

        def mutate(path, data):
            real_receipt(path, data)
            with open(self.target, "ab") as stream:
                stream.write(b"changed")

        with mock.patch.object(snapshots, "_receipt", side_effect=mutate):
            with self.assertRaisesRegex(snapshots.SnapshotSafetyError, "changed"):
                self.archive()
        self.assertTrue(self.target.exists())

    def test_managed_run_archives_on_success_or_body_error(self):
        with mock.patch.object(snapshots, "_assert_inactive"):
            with self.assertRaisesRegex(ValueError, "rehearsal failed"):
                with snapshots.rehearsal_snapshot(self.source, self.target, reserve_bytes=0) as path:
                    self.assertEqual(path, self.target)
                    raise ValueError("rehearsal failed")
        self.assertFalse(self.target.exists())
        self.assertTrue(Path(str(self.target) + ".gz").exists())

    @unittest.skipUnless(Path("/proc/self/fd").is_dir(), "Linux managed archive")
    def test_parent_context_archives_after_worker_process_closes_connections(self):
        with self.own_process_scan(), snapshots.rehearsal_snapshot(self.source, self.target, reserve_bytes=0) as path:
            subprocess.run([
                sys.executable, "-c",
                "import sqlite3,sys; c=sqlite3.connect(sys.argv[1]); "
                "c.execute('INSERT INTO evidence VALUES (?)', ('Private test result',)); "
                "c.commit(); c.close()", str(path),
            ], check=True, timeout=10)
            final_digest = snapshots._digest(path)
        self.assertFalse(self.target.exists())
        self.assertEqual(snapshots._digest(Path(str(self.target) + ".gz"), True), final_digest)

    @unittest.skipUnless(Path("/proc/self/fd").is_dir(), "Linux archive guard")
    def test_real_open_file_and_sqlite_sidecar_guards(self):
        self.copy()
        os.chmod(self.target, 0o600)
        with self.own_process_scan(), open(self.target, "rb"):
            with self.assertRaisesRegex(snapshots.SnapshotSafetyError, "still open"):
                snapshots._assert_inactive(self.target)
        sidecar = Path(str(self.target) + "-wal")
        sidecar.touch()
        with self.own_process_scan(), self.assertRaisesRegex(snapshots.SnapshotSafetyError, "sidecars"):
            snapshots._assert_inactive(self.target)
        sidecar.unlink()
        with self.own_process_scan():
            snapshots._assert_inactive(self.target)

    @unittest.skipUnless(Path("/proc/self/fd").is_dir(), "Linux inspection")
    def test_unreadable_same_owner_process_stays_closed_without_explicit_opt_in(self):
        self.copy()
        process = mock.Mock(name="unreadable-process")
        process.name = "123456"
        process.stat.return_value = mock.Mock(st_uid=os.geteuid())
        handles = mock.Mock()
        handles.iterdir.side_effect = PermissionError("process is nondumpable")
        process = mock.MagicMock(wraps=process)
        process.name = "123456"
        process.stat.return_value = mock.Mock(st_uid=os.geteuid())
        process.__truediv__.return_value = handles
        original = Path.iterdir
        def entries(path):
            return iter([process]) if path == Path("/proc") else original(path)
        with mock.patch.object(Path, "iterdir", entries), mock.patch.object(snapshots.subprocess, "run") as elevated:
            with self.assertRaisesRegex(snapshots.SnapshotSafetyError, "cannot establish"):
                snapshots.archive_snapshot(self.target, protected_paths=(self.source,), reserve_bytes=0)
        elevated.assert_not_called()
        self.assertTrue(self.target.exists())
        self.assertFalse(Path(str(self.target) + ".gz").exists())

    def inspection_receipt(self):
        return {"kind": "inactive_snapshot", "path": str(self.target.resolve()),
                "owner_uid": os.geteuid(), "identity": list(snapshots._identity(self.target.stat()))}

    @unittest.skipUnless(Path("/proc/self/fd").is_dir(), "Linux inspection")
    def test_opt_in_invokes_only_read_only_inspector_and_binds_caller_and_inode(self):
        self.copy()
        expected = self.inspection_receipt()
        with mock.patch.object(snapshots.subprocess, "run", return_value=mock.Mock(
                returncode=0, stdout=json.dumps(expected))) as inspector:
            with mock.patch.object(snapshots, "_assert_inactive", side_effect=AssertionError("caller must not inspect root-only proc")):
                result = snapshots.archive_snapshot(self.target, protected_paths=(self.source,),
                                                     reserve_bytes=0, privileged_inspection=True)
        self.assertEqual(inspector.call_count, 2)
        command = inspector.call_args.args[0]
        self.assertEqual(command[:5], ["sudo", "-n", sys.executable,
                         str(Path(snapshots.__file__).resolve()), "inspect"])
        self.assertEqual(command[5:9], ["--path", str(self.target), "--owner-uid", str(os.geteuid())])
        self.assertIn(str(self.source), command)
        self.assertIn(str(snapshots.PRODUCTION_DB), command)
        self.assertEqual(inspector.call_args.kwargs,
                         {"capture_output": True, "text": True, "check": False, "timeout": 30})
        self.assertFalse(self.target.exists())
        self.assertTrue(result.exists())
        self.assertEqual(result.stat().st_uid, os.geteuid())

    @unittest.skipUnless(Path("/proc/self/fd").is_dir(), "Linux inspection")
    def test_failed_unavailable_or_wrong_inspection_never_archives_raw_copy(self):
        self.copy()
        receipt = self.inspection_receipt()
        responses = [mock.Mock(returncode=1, stdout=""), mock.Mock(returncode=0, stdout="not JSON")]
        for key, value in (("owner_uid", receipt["owner_uid"] + 1), ("identity", [0, 0, 0, 0]),
                           ("path", str(self.source)), ("kind", "other")):
            responses.append(mock.Mock(returncode=0, stdout=json.dumps({**receipt, key: value})))
        for response in responses:
            with self.subTest(response=response), mock.patch.object(snapshots.subprocess, "run", return_value=response):
                with self.assertRaises(snapshots.SnapshotSafetyError):
                    snapshots.archive_snapshot(self.target, reserve_bytes=0, privileged_inspection=True)
                self.assertTrue(self.target.exists())
                self.assertFalse(Path(str(self.target) + ".gz").exists())
        for error in (FileNotFoundError("sudo unavailable"), subprocess.TimeoutExpired("inspect", 30)):
            with self.subTest(error=error), mock.patch.object(snapshots.subprocess, "run", side_effect=error):
                with self.assertRaisesRegex(snapshots.SnapshotSafetyError, "unavailable"):
                    snapshots.archive_snapshot(self.target, reserve_bytes=0, privileged_inspection=True)
                self.assertTrue(self.target.exists())

    @unittest.skipUnless(Path("/proc/self/fd").is_dir(), "Linux inspection")
    def test_snapshot_changed_during_privileged_inspection_is_retained(self):
        self.copy()
        receipt = self.inspection_receipt()
        def inspect(*args, **kwargs):
            with open(self.target, "ab") as stream:
                stream.write(b"concurrent change")
            return mock.Mock(returncode=0, stdout=json.dumps(receipt))
        with mock.patch.object(snapshots.subprocess, "run", side_effect=inspect):
            with self.assertRaisesRegex(snapshots.SnapshotSafetyError, "did not match"):
                snapshots.archive_snapshot(self.target, reserve_bytes=0, privileged_inspection=True)
        self.assertTrue(self.target.exists())
        self.assertFalse(Path(str(self.target) + ".gz").exists())

    @unittest.skipUnless(Path("/proc/self/fd").is_dir(), "Linux inspection")
    def test_inspector_is_read_only_preserves_expected_owner_and_scans_all_users(self):
        self.copy()
        before = snapshots._digest(self.target)
        expected_uid = self.target.stat().st_uid
        with mock.patch.object(snapshots.os, "geteuid", return_value=0), \
             mock.patch.object(snapshots, "_assert_inactive") as check:
            receipt = snapshots.inspect_snapshot(self.target, owner_uid=expected_uid,
                                                   protected_paths=(self.source,))
        check.assert_called_once_with(self.target, owner_uid=expected_uid, scan_all_users=True)
        self.assertEqual(receipt["owner_uid"], expected_uid)
        self.assertEqual(receipt["identity"], list(snapshots._identity(self.target.stat())))
        self.assertEqual(snapshots._digest(self.target), before)
        self.assertFalse(Path(str(self.target) + ".gz").exists())
        with mock.patch.object(snapshots.os, "geteuid", return_value=0):
            with self.assertRaisesRegex(snapshots.SnapshotSafetyError, "expected user"):
                snapshots.inspect_snapshot(self.target, owner_uid=expected_uid + 1)

    def test_inspector_refuses_nonroot_and_protected_input_before_scanning(self):
        self.copy()
        with mock.patch.object(snapshots.os, "geteuid", return_value=1000, create=True):
            with self.assertRaisesRegex(snapshots.SnapshotSafetyError, "explicit root"):
                snapshots.inspect_snapshot(self.target, owner_uid=1000)
        with mock.patch.object(snapshots.os, "geteuid", return_value=0, create=True), \
             mock.patch.object(snapshots, "_assert_inactive") as scan:
            with self.assertRaisesRegex(snapshots.SnapshotSafetyError, "protected"):
                snapshots.inspect_snapshot(self.target, owner_uid=0, protected_paths=(self.target,))
        scan.assert_not_called()

    @unittest.skipUnless(Path("/proc/self/fd").is_dir()
                         and os.environ.get("BNL_SNAPSHOT_PRIVILEGED_INSPECTION_TEST") == "1",
                         "explicit tiny-fixture privileged inspection rehearsal required")
    def test_explicit_privileged_inspector_roundtrip_on_tiny_fixture(self):
        # Only this opt-in integration test executes sudo, against this test's
        # temporary fixture. Normal CI and unit tests never elevate.
        with snapshots.rehearsal_snapshot(self.source, self.target, reserve_bytes=0,
                                          privileged_inspection=True) as path:
            subprocess.run([sys.executable, "-c",
                            "import sqlite3,sys; c=sqlite3.connect(sys.argv[1]); "
                            "c.execute('SELECT value FROM evidence').fetchall(); c.close()",
                            str(path)], check=True, timeout=10)
            digest = snapshots._digest(path)
        self.assertFalse(self.target.exists())
        archive = Path(str(self.target) + ".gz")
        self.assertEqual(snapshots._digest(archive, True), digest)
        self.assertEqual(archive.stat().st_uid, os.geteuid())


if __name__ == "__main__":
    unittest.main()
