import gzip
import hashlib
import json
import os
import shutil
import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import bnl_relay_backup as backup
import bnl_website_relay_state as state


class RelayBackupTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.source_db = self.root / "bnl01_conversations.db"
        state.ensure_schema(str(self.source_db))
        for index in range(30):
            message = (
                "Accepted public Relay %02d preserves a distinct archive subject."
                % index
            )
            directive = (
                "Follow accepted public subject %02d for a confirmed next detail."
                % index
            )
            if index == 7:
                message = (
                    "Accepted public Relay 07 keeps Unicode exact: 6 Bit ⚡\n"
                    "A second public line remains intact."
                )
                directive = "Preserve the exact Unicode and newline payload."
            state.record_publication(
                str(self.source_db),
                42,
                message=message,
                directive=directive,
                mode="OBSERVATION",
                relay_lane=(
                    "current_signal" if index % 2 == 0 else "question_formation"
                ),
                event_type=(
                    "fresh_public_discord_activity"
                    if index % 3
                    else "canon"
                ),
                source_cursor=index,
                published_timestamp=f"2026-07-15T00:00:{index:02d}Z",
                relay_id=f"relay-{index:02d}",
            )
        self._seed_excluded_private_data()

    def tearDown(self):
        self.temporary.cleanup()

    def _seed_excluded_private_data(self):
        state.begin_attempt(
            str(self.source_db),
            "attempt-secret",
            42,
            "scheduled",
            source_class="provider-trace-secret",
        )
        state.complete_attempt(
            str(self.source_db),
            "attempt-secret",
            source_class="provider-trace-secret",
            outcome="rejected",
            reason="PRIVATE-ATTEMPT-SENTINEL",
        )
        state.save_pending_v2_publication(
            str(self.source_db),
            42,
            relay_id="pending-secret",
            message="PRIVATE-PENDING-SENTINEL",
            current_directive="PRIVATE-PENDING-DIRECTIVE",
            source_class="canon",
            trigger="scheduled",
            source_cursor=99,
            source_conversation_fingerprint="PRIVATE-FINGERPRINT",
            canonical_json='{"secret":"PRIVATE-CANONICAL-SENTINEL"}',
        )
        with sqlite3.connect(self.source_db) as connection:
            connection.execute(
                "CREATE TABLE conversations(content TEXT NOT NULL)"
            )
            connection.execute(
                "INSERT INTO conversations VALUES('PRIVATE-CONVERSATION-SENTINEL')"
            )
            connection.execute(
                "CREATE TABLE website_presence_heartbeats(payload TEXT NOT NULL)"
            )
            connection.execute(
                "INSERT INTO website_presence_heartbeats VALUES('PRIVATE-HEARTBEAT-SENTINEL')"
            )
            connection.execute(
                "CREATE TABLE provider_traces(trace TEXT NOT NULL)"
            )
            connection.execute(
                "INSERT INTO provider_traces VALUES('PRIVATE-PROVIDER-TRACE-SENTINEL')"
            )

    def _export(self, directory_name="exports"):
        return backup.export_monthly_snapshot(
            self.source_db,
            self.root / directory_name,
            "2026-07",
        )

    def _source_fingerprint(self):
        data = self.source_db.read_bytes()
        return len(data), hashlib.sha256(data).hexdigest()

    def test_full_archive_is_deterministic_and_strictly_allowlisted(self):
        recent = state.recent_history(str(self.source_db), 42, 50)
        self.assertEqual(len(recent), 25)
        with sqlite3.connect(self.source_db) as connection:
            count = connection.execute(
                "SELECT COUNT(*) FROM website_relay_history"
            ).fetchone()[0]
            oldest = connection.execute(
                """
                SELECT relay_id, published_timestamp
                FROM website_relay_history
                ORDER BY published_timestamp, relay_id
                LIMIT 1
                """
            ).fetchone()
        self.assertEqual(count, 30)
        self.assertEqual(oldest, ("relay-00", "2026-07-15T00:00:00Z"))

        first = self._export("first")
        second = self._export("second")
        self.assertEqual(
            first.archive_path.read_bytes(),
            second.archive_path.read_bytes(),
        )
        self.assertEqual(
            first.checksum_path.read_bytes(),
            second.checksum_path.read_bytes(),
        )
        self.assertEqual(first.history_count, 30)
        self.assertEqual(first.state_count, 1)

        snapshot = backup.verify_archive(
            first.archive_path,
            first.checksum_path,
        )
        self.assertEqual(snapshot.history_count, 30)
        self.assertEqual(
            [row["relay_id"] for row in snapshot.payload["history"]],
            [f"relay-{index:02d}" for index in range(30)],
        )
        unicode_row = next(
            row
            for row in snapshot.payload["history"]
            if row["relay_id"] == "relay-07"
        )
        self.assertIn("6 Bit ⚡\nA second public line", unicode_row["public_message"])
        self.assertEqual(
            unicode_row["published_timestamp"],
            "2026-07-15T00:00:07Z",
        )
        self.assertEqual(
            snapshot.payload["state"],
            [
                {
                    "guild_id": 42,
                    "last_published_conversation_cursor": 29,
                    "last_publication_timestamp": "2026-07-15T00:00:29Z",
                }
            ],
        )

        decompressed = gzip.decompress(first.archive_path.read_bytes())
        for sentinel in (
            b"PRIVATE-ATTEMPT-SENTINEL",
            b"PRIVATE-PENDING-SENTINEL",
            b"PRIVATE-CANONICAL-SENTINEL",
            b"PRIVATE-CONVERSATION-SENTINEL",
            b"PRIVATE-HEARTBEAT-SENTINEL",
            b"PRIVATE-PROVIDER-TRACE-SENTINEL",
        ):
            self.assertNotIn(sentinel, decompressed)
        payload = json.loads(decompressed.decode("utf-8"))
        self.assertEqual(set(payload), backup.SNAPSHOT_KEYS)
        self.assertEqual(payload["history_columns"], list(backup.HISTORY_COLUMNS))
        self.assertEqual(payload["state_columns"], list(backup.STATE_COLUMNS))

    def test_restore_is_exact_idempotent_and_never_mutates_production(self):
        artifact = self._export()
        snapshot = backup.verify_archive(
            artifact.archive_path,
            artifact.checksum_path,
        )
        target = self.root / "isolated.db"
        before = self._source_fingerprint()
        first = backup.restore_to_isolated_db(
            snapshot,
            target,
            self.source_db,
        )
        second = backup.restore_to_isolated_db(
            snapshot,
            target,
            self.source_db,
        )
        after = self._source_fingerprint()

        self.assertEqual(before, after)
        self.assertEqual(first.history_inserted, 30)
        self.assertEqual(first.state_inserted, 1)
        self.assertEqual(second.history_inserted, 0)
        self.assertEqual(second.history_existing, 30)
        self.assertEqual(second.state_inserted, 0)
        self.assertEqual(second.state_existing, 1)
        with sqlite3.connect(target) as connection:
            tables = {
                row[0]
                for row in connection.execute(
                    """
                    SELECT name FROM sqlite_master
                    WHERE type='table' AND name NOT LIKE 'sqlite_%'
                    """
                ).fetchall()
            }
            restored = connection.execute(
                """
                SELECT public_message, published_timestamp
                FROM website_relay_history
                WHERE relay_id='relay-07'
                """
            ).fetchone()
            cursor = connection.execute(
                """
                SELECT last_published_conversation_cursor,
                       last_publication_timestamp
                FROM website_relay_state
                WHERE guild_id=42
                """
            ).fetchone()
            integrity = connection.execute("PRAGMA integrity_check").fetchone()
        self.assertEqual(tables, backup.ALLOWED_RESTORE_TABLES)
        self.assertIn("6 Bit ⚡\nA second public line", restored[0])
        self.assertEqual(restored[1], "2026-07-15T00:00:07Z")
        self.assertEqual(cursor, (29, "2026-07-15T00:00:29Z"))
        self.assertEqual(integrity, ("ok",))

    def test_corruption_is_rejected_before_restore_target_creation(self):
        artifact = self._export()
        original = bytearray(artifact.archive_path.read_bytes())
        original[-1] ^= 0x01
        artifact.archive_path.write_bytes(bytes(original))
        target = self.root / "must-not-exist.db"
        result = backup.main(
            [
                "restore",
                "--archive",
                str(artifact.archive_path),
                "--checksum",
                str(artifact.checksum_path),
                "--target-db",
                str(target),
                "--production-db",
                str(self.source_db),
            ]
        )
        self.assertEqual(result, 1)
        self.assertFalse(target.exists())

    def test_restore_conflict_rolls_back_earlier_inserts(self):
        artifact = self._export()
        snapshot = backup.verify_archive(
            artifact.archive_path,
            artifact.checksum_path,
        )
        target = self.root / "conflict.db"
        with sqlite3.connect(target) as connection:
            backup._create_restore_schema(connection)
            conflict = dict(snapshot.payload["history"][1])
            conflict["public_message"] = "conflicting text"
            connection.execute(
                f"""
                INSERT INTO website_relay_history({", ".join(backup.HISTORY_COLUMNS)})
                VALUES({", ".join("?" for _ in backup.HISTORY_COLUMNS)})
                """,
                tuple(conflict[column] for column in backup.HISTORY_COLUMNS),
            )
        with self.assertRaises(backup.RelayBackupConflictError):
            backup.restore_to_isolated_db(
                snapshot,
                target,
                self.source_db,
            )
        with sqlite3.connect(target) as connection:
            rows = connection.execute(
                "SELECT relay_id, public_message FROM website_relay_history"
            ).fetchall()
        self.assertEqual(rows, [("relay-01", "conflicting text")])

    def test_production_alias_symlink_and_hardlink_targets_are_rejected(self):
        artifact = self._export()
        snapshot = backup.verify_archive(
            artifact.archive_path,
            artifact.checksum_path,
        )
        before = self._source_fingerprint()
        with self.assertRaises(backup.RelayBackupValidationError):
            backup.restore_to_isolated_db(
                snapshot,
                self.source_db,
                self.source_db,
            )

        symlink_target = self.root / "production-symlink.db"
        symlink_target.symlink_to(self.source_db)
        with self.assertRaises(backup.RelayBackupValidationError):
            backup.restore_to_isolated_db(
                snapshot,
                symlink_target,
                self.source_db,
            )

        hardlink_target = self.root / "production-hardlink.db"
        os.link(self.source_db, hardlink_target)
        with self.assertRaises(backup.RelayBackupValidationError):
            backup.restore_to_isolated_db(
                snapshot,
                hardlink_target,
                self.source_db,
            )
        self.assertEqual(before, self._source_fingerprint())

    def test_missing_production_reference_and_sidecars_are_rejected(self):
        artifact = self._export()
        snapshot = backup.verify_archive(
            artifact.archive_path,
            artifact.checksum_path,
        )
        missing_production = self.root / "mistyped-production.db"
        target = self.root / "must-not-be-created.db"
        with self.assertRaisesRegex(
            backup.RelayBackupValidationError,
            "production_database_missing_or_unresolvable",
        ):
            backup.restore_to_isolated_db(
                snapshot,
                target,
                missing_production,
            )
        self.assertFalse(target.exists())

        before = self._source_fingerprint()
        for suffix in backup.SQLITE_SIDECAR_SUFFIXES:
            sidecar_target = Path(f"{self.source_db}{suffix}")
            with self.subTest(suffix=suffix):
                with self.assertRaisesRegex(
                    backup.RelayBackupValidationError,
                    "restore_target_is_production_database_sidecar",
                ):
                    backup.restore_to_isolated_db(
                        snapshot,
                        sidecar_target,
                        self.source_db,
                    )
                self.assertFalse(sidecar_target.exists())
        self.assertEqual(before, self._source_fingerprint())

    def test_rclone_upload_download_and_isolated_round_trip(self):
        artifact = self._export()
        remote_root = "gdrive:BNL-01 Backups/Relay Archive"
        remote_dir = self.root / "fake-google-drive"
        remote_dir.mkdir()
        calls = []

        def fake_runner(command, **kwargs):
            calls.append((list(command), dict(kwargs)))
            self.assertFalse(kwargs["shell"])
            self.assertEqual(command[1], "copyto")
            source = command[2]
            destination = command[3]
            if source.startswith(remote_root + "/"):
                remote_name = source[len(remote_root) + 1 :]
                shutil.copyfile(remote_dir / remote_name, destination)
            else:
                remote_name = destination[len(remote_root) + 1 :]
                shutil.copyfile(source, remote_dir / remote_name)
            return SimpleNamespace(returncode=0, stdout="", stderr="")

        upload = backup.upload_archive(
            artifact,
            remote_root,
            runner=fake_runner,
        )
        self.assertEqual(upload.operation, "upload")
        self.assertTrue((remote_dir / artifact.archive_path.name).is_file())
        self.assertTrue((remote_dir / artifact.checksum_path.name).is_file())

        before = self._source_fingerprint()
        proof = backup.prove_remote_round_trip(
            artifact,
            remote_root,
            self.source_db,
            runner=fake_runner,
        )
        self.assertEqual(before, self._source_fingerprint())
        self.assertTrue(proof.production_db_untouched)
        self.assertEqual(proof.first_restore.history_inserted, 30)
        self.assertEqual(proof.second_restore.history_inserted, 0)
        self.assertEqual(proof.second_restore.state_inserted, 0)
        self.assertEqual(len(calls), 4)
        for command, kwargs in calls:
            self.assertEqual(command[0], "rclone")
            self.assertFalse(kwargs["shell"])
            self.assertTrue(
                any("BNL-01 Backups/Relay Archive" in arg for arg in command)
            )

    def test_rclone_failure_is_observable_and_non_destructive(self):
        artifact = self._export()
        before = self._source_fingerprint()

        def failed_runner(command, **kwargs):
            return SimpleNamespace(returncode=9, stdout="", stderr="provider trace")

        with self.assertRaises(backup.RelayBackupTransportError):
            backup.upload_archive(
                artifact,
                "gdrive:BNL-01 Backups/Relay Archive",
                runner=failed_runner,
            )
        self.assertTrue(artifact.archive_path.is_file())
        self.assertTrue(artifact.checksum_path.is_file())
        self.assertEqual(before, self._source_fingerprint())

    def test_scheduled_run_defaults_off_before_database_or_rclone_work(self):
        missing_db = self.root / "missing.db"
        with mock.patch.dict(
            os.environ,
            {backup.SCHEDULE_ENABLED_ENV: ""},
            clear=False,
        ), mock.patch.object(
            backup,
            "export_monthly_snapshot",
            side_effect=AssertionError("database opened"),
        ) as export_mock, mock.patch.object(
            backup,
            "upload_archive",
            side_effect=AssertionError("rclone invoked"),
        ) as upload_mock:
            result = backup.main(
                [
                    "scheduled-run",
                    "--db",
                    str(missing_db),
                    "--output-dir",
                    str(self.root / "scheduled"),
                    "--remote",
                    "gdrive:BNL-01 Backups/Relay Archive",
                ]
            )
        self.assertEqual(result, 1)
        export_mock.assert_not_called()
        upload_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
