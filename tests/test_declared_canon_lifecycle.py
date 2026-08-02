from dataclasses import asdict
import inspect
import os
import sqlite3
import tempfile
import unittest
from unittest import mock

import bnl_declared_canon as declared


def add_kwargs(**overrides):
    values = {
        "guild_id": 7,
        "subject_type": "person",
        "subject_id": "test_member",
        "predicate": "community_role",
        "value": {"role": "signal keeper", "current": True},
        "raw_declaration": "6 Bit declares Test Member the signal keeper.",
        "cleaned_summary": "Test Member is the signal keeper.",
        "domain": "real_community",
        "claim_kind": "role",
        "visibility": "internal",
        "eligible_routes": ("declared_canon_review",),
        "valid_from": "2026-08-01T00:00:00+00:00",
    }
    values.update(overrides)
    return values


class DeclaredCanonLifecycleTests(unittest.TestCase):
    def setUp(self):
        self.env = mock.patch.dict(
            os.environ,
            {
                "BNL_OWNER_USER_ID": "61",
                "BNL_PRIMARY_GUILD_ID": "7",
            },
        )
        self.env.start()
        self.conn = sqlite3.connect(":memory:")
        declared.ensure_declared_canon_schema(self.conn)

    def tearDown(self):
        self.conn.close()
        self.env.stop()

    def rows(self, query, params=()):
        return self.conn.execute(query, params).fetchall()

    def add(self, nonce="add-case-0001", actor=61, **overrides):
        return declared.add_declared_canon(
            self.conn,
            actor_user_id=actor,
            authority_nonce=nonce,
            **add_kwargs(**overrides),
        )

    def test_schema_is_idempotent_one_table_and_append_only(self):
        declared.ensure_declared_canon_schema(self.conn)
        tables = {
            row[0]
            for row in self.rows(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        self.assertEqual(tables, {declared.DECLARED_CANON_TABLE})
        created = self.add().primary
        with self.assertRaisesRegex(
            sqlite3.IntegrityError, "declared_canon_append_only_update"
        ):
            self.conn.execute(
                "UPDATE declared_canon_revisions SET lifecycle_status='retired' "
                "WHERE revision_id=?",
                (created.revision_id,),
            )
        self.conn.rollback()
        with self.assertRaisesRegex(
            sqlite3.IntegrityError, "declared_canon_append_only_delete"
        ):
            self.conn.execute(
                "DELETE FROM declared_canon_revisions WHERE revision_id=?",
                (created.revision_id,),
            )
        self.conn.rollback()

        replacement = asdict(created)
        replacement["cleaned_summary"] = "REPLACED HISTORY"
        with self.assertRaisesRegex(
            sqlite3.IntegrityError, "declared_canon_append_only_conflict"
        ):
            self.conn.execute(
                "INSERT OR REPLACE INTO declared_canon_revisions (%s) VALUES (%s)"
                % (
                    ",".join(declared._REVISION_COLUMNS),
                    ",".join("?" for _ in declared._REVISION_COLUMNS),
                ),
                tuple(replacement[column] for column in declared._REVISION_COLUMNS),
            )
        self.conn.rollback()
        stored = self.rows(
            "SELECT cleaned_summary FROM declared_canon_revisions WHERE revision_id=?",
            (created.revision_id,),
        )
        self.assertEqual(stored, [(created.cleaned_summary,)])

    def test_public_apis_accept_no_caller_built_owner_or_receipt(self):
        signature = inspect.signature(declared.add_declared_canon)
        self.assertNotIn("authority", signature.parameters)
        self.assertNotIn("authority_receipt", signature.parameters)
        self.assertNotIn("configured_owner_user_id", signature.parameters)
        self.assertFalse(hasattr(declared, "OwnerAuthority"))
        self.assertFalse(hasattr(declared, "build_owner_authority"))

    def test_every_boundary_reads_trusted_owner_and_primary_guild(self):
        with mock.patch.dict(os.environ, {"BNL_OWNER_USER_ID": ""}):
            with self.assertRaisesRegex(
                declared.DeclaredCanonError, "owner_user_id_not_configured"
            ):
                self.add()
        with self.assertRaisesRegex(
            declared.DeclaredCanonError, "configured_owner_required"
        ):
            self.add(actor=62)
        with mock.patch.dict(os.environ, {"BNL_PRIMARY_GUILD_ID": ""}):
            with self.assertRaisesRegex(
                declared.DeclaredCanonError, "primary_guild_id_not_configured"
            ):
                self.add()
        with self.assertRaisesRegex(
            declared.DeclaredCanonError, "configured_primary_guild_required"
        ):
            self.add(guild_id=8)
        self.assertEqual(self.rows("SELECT COUNT(*) FROM declared_canon_revisions")[0][0], 0)

        no_schema = sqlite3.connect(":memory:")
        try:
            with self.assertRaisesRegex(
                declared.DeclaredCanonError, "configured_owner_required"
            ):
                declared.validate_current_declared_canon_revision(
                    no_schema,
                    actor_user_id=62,
                    authority_nonce="no-schema-auth1",
                    guild_id=7,
                    declaration_id="dcl_" + "0" * 32,
                    expected_revision_id="drev_" + "0" * 40,
                    expected_source_fingerprint="src_" + "0" * 40,
                )
        finally:
            no_schema.close()

    def test_add_separates_content_and_stores_bound_versioned_receipt(self):
        revision = self.add().primary
        self.assertEqual(revision.revision_number, 1)
        self.assertEqual(revision.lifecycle_status, "established")
        self.assertEqual(revision.source_system, "general_declaration")
        self.assertEqual(revision.authority_actor, "discord_user:61")
        self.assertTrue(revision.authority_verified)
        self.assertNotEqual(revision.raw_declaration, revision.cleaned_summary)
        self.assertTrue(revision.source_fingerprint.startswith("src_"))
        self.assertRegex(
            revision.authority_receipt,
            r"^owner_command:declared_canon_lifecycle_v1:"
            r"declared_canon_internal_receipt_v1:add:[0-9a-f]{64}$",
        )
        self.assertRegex(
            revision.authority_request_fingerprint,
            r"^req_[0-9a-f]{48}$",
        )

    def test_well_formed_forged_receipt_and_recomputed_revision_id_fail(self):
        created = self.add("forge-source01").primary
        forged = asdict(created)
        forged["declaration_id"] = "dcl_" + "2" * 32
        forged["source_row_id"] = forged["declaration_id"]
        forged["operation_id"] = "op_" + "3" * 32
        forged["authority_request_fingerprint"] = "req_" + "4" * 48
        forged["authority_verified"] = 1
        forged["authority_receipt"] = (
            "owner_command:declared_canon_lifecycle_v1:"
            "declared_canon_internal_receipt_v1:add:" + "5" * 64
        )
        forged["revision_id"] = "drev_" + declared._digest(
            *(forged[column] for column in declared._REVISION_COLUMNS
              if column != "revision_id")
        )[:40]
        self.conn.execute(
            "INSERT INTO declared_canon_revisions (%s) VALUES (%s)"
            % (
                ",".join(declared._REVISION_COLUMNS),
                ",".join("?" for _ in declared._REVISION_COLUMNS),
            ),
            tuple(forged[column] for column in declared._REVISION_COLUMNS),
        )
        self.conn.commit()
        with self.assertRaisesRegex(
            declared.DeclaredCanonError, "stored_authority_invalid"
        ):
            declared.validate_current_declared_canon_revision(
                self.conn,
                actor_user_id=61,
                authority_nonce="forge-validate1",
                guild_id=7,
                declaration_id=forged["declaration_id"],
                expected_revision_id=forged["revision_id"],
                expected_source_fingerprint=forged["source_fingerprint"],
                now="2026-08-01T12:00:00+00:00",
            )

    def test_mutation_cannot_launder_forged_latest_revision(self):
        created = self.add("launder-source1").primary
        forged = asdict(created)
        forged["revision_number"] = 2
        forged["previous_revision_id"] = created.revision_id
        forged["operation_id"] = "op_" + "6" * 32
        forged["authority_request_fingerprint"] = "req_" + "7" * 48
        forged["authority_verified"] = 1
        forged["raw_declaration"] = "Forged latest declaration."
        forged["cleaned_summary"] = "Forged latest summary."
        forged["source_fingerprint"] = declared._general_fingerprint(forged)
        forged["authority_receipt"] = (
            "owner_command:declared_canon_lifecycle_v1:"
            "declared_canon_internal_receipt_v1:add:" + "8" * 64
        )
        forged["revision_id"] = "drev_" + declared._digest(
            *(forged[column] for column in declared._REVISION_COLUMNS
              if column != "revision_id")
        )[:40]
        self.conn.execute(
            "INSERT INTO declared_canon_revisions (%s) VALUES (%s)"
            % (
                ",".join(declared._REVISION_COLUMNS),
                ",".join("?" for _ in declared._REVISION_COLUMNS),
            ),
            tuple(forged[column] for column in declared._REVISION_COLUMNS),
        )
        self.conn.commit()
        before = self.conn.total_changes
        with self.assertRaisesRegex(
            declared.DeclaredCanonError, "stored_authority_invalid"
        ):
            declared.change_declared_canon_status(
                self.conn,
                actor_user_id=61,
                authority_nonce="launder-status1",
                guild_id=7,
                declaration_id=created.declaration_id,
                expected_revision_id=forged["revision_id"],
                lifecycle_status="contested",
                now="2026-08-01T12:00:00+00:00",
            )
        self.assertEqual(self.conn.total_changes, before)
        self.assertEqual(
            self.rows(
                "SELECT COUNT(*) FROM declared_canon_revisions WHERE declaration_id=?",
                (created.declaration_id,),
            )[0][0],
            2,
        )

    def test_receipts_require_no_secret_configuration(self):
        created = self.add("no-secret-source1").primary
        changed = declared.change_declared_canon_status(
            self.conn,
            actor_user_id=61,
            authority_nonce="no-secret-status1",
            guild_id=7,
            declaration_id=created.declaration_id,
            expected_revision_id=created.revision_id,
            lifecycle_status="contested",
        ).primary
        self.assertEqual(changed.lifecycle_status, "contested")
        self.assertTrue(declared._stored_authority_valid(created))
        self.assertTrue(declared._stored_authority_valid(changed))

    def test_exact_retry_is_zero_write_and_changed_binding_rejects(self):
        first = self.add("idempotent-0001")
        before = self.conn.total_changes
        retry = self.add("idempotent-0001")
        self.assertEqual(retry.primary.revision_id, first.primary.revision_id)
        self.assertEqual(retry.operation_id, first.operation_id)
        self.assertEqual(self.conn.total_changes, before)
        distinct_message = self.add("idempotent-0002")
        self.assertNotEqual(
            distinct_message.primary.declaration_id,
            first.primary.declaration_id,
        )
        self.assertNotEqual(distinct_message.operation_id, first.operation_id)
        self.assertNotEqual(
            distinct_message.primary.authority_receipt,
            first.primary.authority_receipt,
        )
        with self.assertRaisesRegex(
            declared.DeclaredCanonError, "authority_nonce_replay_mismatch"
        ):
            self.add(
                "idempotent-0001",
                cleaned_summary="A changed payload cannot reuse the interaction.",
            )

    def test_same_nonce_cannot_cross_operations(self):
        created = self.add("one-interaction1").primary
        with self.assertRaisesRegex(
            declared.DeclaredCanonError, "authority_nonce_replay_mismatch"
        ):
            declared.retire_declared_canon(
                self.conn,
                actor_user_id=61,
                authority_nonce="one-interaction1",
                guild_id=7,
                declaration_id=created.declaration_id,
                expected_revision_id=created.revision_id,
            )

    def test_content_free_preview_is_owner_only_opaque_and_zero_write(self):
        created = self.add().primary
        before = self.conn.total_changes
        preview = declared.preview_declared_canon(
            self.conn,
            actor_user_id=61,
            authority_nonce="preview-0001",
            guild_id=7,
        )
        self.assertEqual(preview.mutation_count, 0)
        self.assertEqual(self.conn.total_changes, before)
        self.assertEqual(preview.total_rows, 1)
        self.assertEqual(preview.items[0].declaration_id, created.declaration_id)
        self.assertTrue(preview.items[0].current_revision)
        self.assertRegex(preview.authority_actor_ref, r"^actor_[0-9a-f]{20}$")
        self.assertRegex(preview.authority_receipt_id, r"^receipt_[0-9a-f]{24}$")
        item_fields = asdict(preview.items[0])
        self.assertNotIn("source_row_id", item_fields)
        self.assertNotIn("subject_id", item_fields)
        self.assertNotIn("predicate", item_fields)
        self.assertNotIn(created.raw_declaration, repr(preview))
        self.assertNotIn(created.cleaned_summary, repr(preview))
        with self.assertRaisesRegex(
            declared.DeclaredCanonError, "configured_owner_required"
        ):
            declared.preview_declared_canon(
                self.conn,
                actor_user_id=62,
                authority_nonce="preview-wrong1",
                guild_id=7,
            )

    def test_correction_requires_exact_revision_and_appends_lineage(self):
        original = self.add().primary
        with self.assertRaisesRegex(
            declared.DeclaredCanonError, "expected_revision_mismatch"
        ):
            declared.correct_declared_canon(
                self.conn,
                actor_user_id=61,
                authority_nonce="correct-stale1",
                declaration_id=original.declaration_id,
                expected_revision_id="drev_" + "0" * 40,
                reason="stale",
                **add_kwargs(),
            )
        corrected = declared.correct_declared_canon(
            self.conn,
            actor_user_id=61,
            authority_nonce="correct-0001",
            declaration_id=original.declaration_id,
            expected_revision_id=original.revision_id,
            reason="Role wording corrected.",
            **add_kwargs(
                value={"role": "signal archivist", "current": True},
                raw_declaration="6 Bit corrects the role to signal archivist.",
                cleaned_summary="Test Member is the signal archivist.",
            ),
        ).primary
        self.assertEqual(corrected.declaration_id, original.declaration_id)
        self.assertEqual(corrected.revision_number, 2)
        self.assertEqual(corrected.previous_revision_id, original.revision_id)
        self.assertEqual(corrected.correction_of_revision_id, original.revision_id)
        self.assertNotEqual(corrected.source_fingerprint, original.source_fingerprint)

    def test_status_retire_terminal_and_idempotent_retry(self):
        original = self.add().primary
        contested = declared.change_declared_canon_status(
            self.conn,
            actor_user_id=61,
            authority_nonce="status-00001",
            guild_id=7,
            declaration_id=original.declaration_id,
            expected_revision_id=original.revision_id,
            lifecycle_status="contested",
        ).primary
        retired_result = declared.retire_declared_canon(
            self.conn,
            actor_user_id=61,
            authority_nonce="retire-00001",
            guild_id=7,
            declaration_id=original.declaration_id,
            expected_revision_id=contested.revision_id,
        )
        before = self.conn.total_changes
        retry = declared.retire_declared_canon(
            self.conn,
            actor_user_id=61,
            authority_nonce="retire-00001",
            guild_id=7,
            declaration_id=original.declaration_id,
            expected_revision_id=contested.revision_id,
        )
        self.assertEqual(retry.primary.revision_id, retired_result.primary.revision_id)
        self.assertEqual(self.conn.total_changes, before)
        with self.assertRaisesRegex(declared.DeclaredCanonError, "declaration_terminal"):
            declared.change_declared_canon_status(
                self.conn,
                actor_user_id=61,
                authority_nonce="status-00002",
                guild_id=7,
                declaration_id=original.declaration_id,
                expected_revision_id=retired_result.primary.revision_id,
                lifecycle_status="established",
            )

    def test_supersession_is_atomic_bidirectional_and_idempotent(self):
        old = self.add("add-old-0001", subject_id="old_role").primary
        replacement = self.add(
            "add-new-0001",
            subject_id="new_role",
            raw_declaration="6 Bit declares the replacement role.",
        ).primary
        kwargs = {
            "actor_user_id": 61,
            "authority_nonce": "supersede-001",
            "guild_id": 7,
            "declaration_id": old.declaration_id,
            "expected_revision_id": old.revision_id,
            "replacement_declaration_id": replacement.declaration_id,
            "expected_replacement_revision_id": replacement.revision_id,
            "reason": "Replacement is authoritative.",
        }
        result = declared.supersede_declared_canon(self.conn, **kwargs)
        old_terminal, new_current = result.revisions
        self.assertEqual(old_terminal.lifecycle_status, "superseded")
        self.assertEqual(old_terminal.superseded_by_declaration_id, replacement.declaration_id)
        self.assertEqual(new_current.supersedes_declaration_id, old.declaration_id)
        before = self.conn.total_changes
        retry = declared.supersede_declared_canon(self.conn, **kwargs)
        self.assertEqual(
            tuple(item.revision_id for item in retry.revisions),
            tuple(item.revision_id for item in result.revisions),
        )
        self.assertEqual(self.conn.total_changes, before)

    def test_replacement_status_and_correction_preserve_supersession_lineage(self):
        old = self.add("lineage-old-01", subject_id="old_signal_role").primary
        replacement = self.add(
            "lineage-new-01",
            subject_id="new_signal_role",
            raw_declaration="6 Bit declares the new signal role.",
        ).primary
        _, linked_replacement = declared.supersede_declared_canon(
            self.conn,
            actor_user_id=61,
            authority_nonce="lineage-link-01",
            guild_id=7,
            declaration_id=old.declaration_id,
            expected_revision_id=old.revision_id,
            replacement_declaration_id=replacement.declaration_id,
            expected_replacement_revision_id=replacement.revision_id,
        ).revisions
        contested = declared.change_declared_canon_status(
            self.conn,
            actor_user_id=61,
            authority_nonce="lineage-status1",
            guild_id=7,
            declaration_id=replacement.declaration_id,
            expected_revision_id=linked_replacement.revision_id,
            lifecycle_status="contested",
        ).primary
        self.assertEqual(contested.supersedes_declaration_id, old.declaration_id)

        corrected = declared.correct_declared_canon(
            self.conn,
            actor_user_id=61,
            authority_nonce="lineage-correct",
            declaration_id=replacement.declaration_id,
            expected_revision_id=contested.revision_id,
            reason="Correct wording without erasing lifecycle or lineage.",
            **add_kwargs(
                subject_id="new_signal_role",
                raw_declaration="6 Bit corrects the new signal role wording.",
                cleaned_summary="The new signal role has corrected wording.",
            ),
        ).primary
        self.assertEqual(corrected.lifecycle_status, "contested")
        self.assertEqual(corrected.supersedes_declaration_id, old.declaration_id)
        self.assertEqual(corrected.superseded_by_declaration_id, "")

    def test_supersession_rolls_back_when_replacement_is_missing(self):
        old = self.add("add-old-0002", subject_id="old_role_2").primary
        before = self.conn.total_changes
        with self.assertRaisesRegex(
            declared.DeclaredCanonError, "general_declaration_not_found"
        ):
            declared.supersede_declared_canon(
                self.conn,
                actor_user_id=61,
                authority_nonce="supersede-bad1",
                guild_id=7,
                declaration_id=old.declaration_id,
                expected_revision_id=old.revision_id,
                replacement_declaration_id="dcl_missing",
                expected_replacement_revision_id="drev_" + "0" * 40,
            )
        self.assertEqual(self.conn.total_changes, before)

    def test_supersession_rolls_back_first_insert_if_second_insert_aborts(self):
        old = self.add("atomic-old-001", subject_id="atomic_old").primary
        replacement = self.add(
            "atomic-new-001", subject_id="atomic_new"
        ).primary
        self.conn.execute(
            """
            CREATE TRIGGER fail_old_supersede
            BEFORE INSERT ON declared_canon_revisions
            WHEN NEW.operation='supersede' AND NEW.declaration_id='%s'
            BEGIN
                SELECT RAISE(ABORT, 'forced_second_insert_failure');
            END
            """ % old.declaration_id
        )
        self.conn.commit()
        with self.assertRaisesRegex(
            sqlite3.IntegrityError, "forced_second_insert_failure"
        ):
            declared.supersede_declared_canon(
                self.conn,
                actor_user_id=61,
                authority_nonce="atomic-super001",
                guild_id=7,
                declaration_id=old.declaration_id,
                expected_revision_id=old.revision_id,
                replacement_declaration_id=replacement.declaration_id,
                expected_replacement_revision_id=replacement.revision_id,
            )
        rows = self.rows(
            "SELECT declaration_id,revision_number,lifecycle_status "
            "FROM declared_canon_revisions ORDER BY rowid"
        )
        self.assertEqual(
            rows,
            [
                (old.declaration_id, 1, "established"),
                (replacement.declaration_id, 1, "established"),
            ],
        )

    def test_relationship_claim_requires_typed_counterpart(self):
        with self.assertRaisesRegex(
            declared.DeclaredCanonError,
            "relationship_object_subject_type_required",
        ):
            self.add(claim_kind="relationship", predicate="works_with")
        relation = self.add(
            "relationship-01",
            claim_kind="relationship",
            predicate="works_with",
            object_subject_type="organization",
            object_subject_id="barcode_network",
        ).primary
        self.assertEqual(relation.object_subject_type, "organization")
        self.assertEqual(relation.object_subject_id, "barcode_network")
        with self.assertRaisesRegex(
            declared.DeclaredCanonError, "relationship_object_not_allowed"
        ):
            self.add(
                "bad-endpoint-01",
                object_subject_type="organization",
                object_subject_id="barcode_network",
            )

    def test_validation_and_closed_route_allowlist_fail_closed(self):
        invalid_cases = (
            {"subject_id": "Display Name"},
            {"subject_type": "unknown"},
            {"predicate": "Role Label"},
            {"domain": "everything"},
            {"claim_kind": "biography"},
            {"visibility": "internal", "eligible_routes": ("public_selective",)},
            {"eligible_routes": ("surprise_public_route",)},
            {
                "valid_from": "2026-08-02T00:00:00+00:00",
                "valid_until": "2026-08-01T00:00:00+00:00",
            },
        )
        for index, overrides in enumerate(invalid_cases):
            with self.assertRaises(declared.DeclaredCanonError):
                self.add("invalid-%04d" % index, **overrides)
        self.assertEqual(self.rows("SELECT COUNT(*) FROM declared_canon_revisions")[0][0], 0)

    def test_read_validator_is_zero_write_and_rejects_stale_or_cross_guild(self):
        revision = self.add().primary
        before = self.conn.total_changes
        validated = declared.validate_current_declared_canon_revision(
            self.conn,
            actor_user_id=61,
            authority_nonce="validator-0001",
            guild_id=7,
            declaration_id=revision.declaration_id,
            expected_revision_id=revision.revision_id,
            expected_source_fingerprint=revision.source_fingerprint,
            now="2026-08-01T01:00:00+00:00",
        )
        self.assertEqual(validated.revision_id, revision.revision_id)
        self.assertEqual(self.conn.total_changes, before)
        with self.assertRaisesRegex(
            declared.DeclaredCanonError, "expected_source_fingerprint_mismatch"
        ):
            declared.validate_current_declared_canon_revision(
                self.conn,
                actor_user_id=61,
                authority_nonce="validator-0002",
                guild_id=7,
                declaration_id=revision.declaration_id,
                expected_revision_id=revision.revision_id,
                expected_source_fingerprint="src_" + "0" * 40,
            )
        with self.assertRaisesRegex(
            declared.DeclaredCanonError, "configured_primary_guild_required"
        ):
            declared.validate_current_declared_canon_revision(
                self.conn,
                actor_user_id=61,
                authority_nonce="validator-0003",
                guild_id=8,
                declaration_id=revision.declaration_id,
                expected_revision_id=revision.revision_id,
                expected_source_fingerprint=revision.source_fingerprint,
            )

    def test_read_validator_honors_full_validity_window_and_lifecycle(self):
        future = self.add(
            "future-claim01",
            valid_from="2026-09-01T00:00:00+00:00",
        ).primary
        with self.assertRaisesRegex(
            declared.DeclaredCanonError, "declared_revision_not_current"
        ):
            declared.validate_current_declared_canon_revision(
                self.conn,
                actor_user_id=61,
                authority_nonce="future-check01",
                guild_id=7,
                declaration_id=future.declaration_id,
                expected_revision_id=future.revision_id,
                expected_source_fingerprint=future.source_fingerprint,
                now="2026-08-15T00:00:00+00:00",
            )
        contested = declared.change_declared_canon_status(
            self.conn,
            actor_user_id=61,
            authority_nonce="future-status1",
            guild_id=7,
            declaration_id=future.declaration_id,
            expected_revision_id=future.revision_id,
            lifecycle_status="contested",
        ).primary
        with self.assertRaisesRegex(
            declared.DeclaredCanonError, "declared_revision_not_established"
        ):
            declared.validate_current_declared_canon_revision(
                self.conn,
                actor_user_id=61,
                authority_nonce="contested-check",
                guild_id=7,
                declaration_id=contested.declaration_id,
                expected_revision_id=contested.revision_id,
                expected_source_fingerprint=contested.source_fingerprint,
                now="2026-09-02T00:00:00+00:00",
            )

    def test_latest_validator_returns_terminal_revision_without_calling_it_current(self):
        original = self.add("terminal-source1").primary
        retired = declared.retire_declared_canon(
            self.conn,
            actor_user_id=61,
            authority_nonce="terminal-retire",
            guild_id=7,
            declaration_id=original.declaration_id,
            expected_revision_id=original.revision_id,
        ).primary
        before = self.conn.total_changes
        latest = declared.validate_latest_declared_canon_revision(
            self.conn,
            actor_user_id=61,
            authority_nonce="terminal-read01",
            guild_id=7,
            declaration_id=retired.declaration_id,
            expected_revision_id=retired.revision_id,
            expected_source_fingerprint=retired.source_fingerprint,
            expected_lifecycle_status="retired",
        )
        self.assertEqual(latest.lifecycle_status, "retired")
        self.assertEqual(self.conn.total_changes, before)
        with self.assertRaisesRegex(
            declared.DeclaredCanonError, "declared_revision_not_established"
        ):
            declared.validate_current_declared_canon_revision(
                self.conn,
                actor_user_id=61,
                authority_nonce="terminal-current",
                guild_id=7,
                declaration_id=retired.declaration_id,
                expected_revision_id=retired.revision_id,
                expected_source_fingerprint=retired.source_fingerprint,
            )
        with self.assertRaisesRegex(
            declared.DeclaredCanonError,
            "latest_validator_requires_noncurrent_lifecycle",
        ):
            declared.validate_latest_declared_canon_revision(
                self.conn,
                actor_user_id=61,
                authority_nonce="terminal-wrong1",
                guild_id=7,
                declaration_id=retired.declaration_id,
                expected_revision_id=retired.revision_id,
                expected_source_fingerprint=retired.source_fingerprint,
                expected_lifecycle_status="established",
            )

    def test_current_validator_owns_one_wal_snapshot_across_latest_append(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            database_path = os.path.join(temp_dir, "declared-current-snapshot.sqlite3")
            writer = sqlite3.connect(database_path, timeout=5)
            reader = sqlite3.connect(database_path, timeout=5)
            try:
                writer.execute("PRAGMA journal_mode=WAL")
                declared.ensure_declared_canon_schema(writer)
                original = declared.add_declared_canon(
                    writer,
                    actor_user_id=61,
                    authority_nonce="wal-current-add1",
                    **add_kwargs(),
                ).primary
                real_latest = declared._latest_revision
                interleaved = {"revision": None}

                def latest_then_append(conn, **kwargs):
                    revision = real_latest(conn, **kwargs)
                    if conn is reader and interleaved["revision"] is None:
                        interleaved["revision"] = declared.change_declared_canon_status(
                            writer,
                            actor_user_id=61,
                            authority_nonce="wal-current-status1",
                            guild_id=7,
                            declaration_id=original.declaration_id,
                            expected_revision_id=original.revision_id,
                            lifecycle_status="contested",
                        ).primary
                    return revision

                before = reader.total_changes
                with mock.patch.object(
                    declared, "_latest_revision", side_effect=latest_then_append
                ):
                    validated = declared.validate_current_declared_canon_revision(
                        reader,
                        actor_user_id=61,
                        authority_nonce="wal-current-read1",
                        guild_id=7,
                        declaration_id=original.declaration_id,
                        expected_revision_id=original.revision_id,
                        expected_source_fingerprint=original.source_fingerprint,
                        now="2026-08-01T01:00:00+00:00",
                    )
                self.assertEqual(validated.revision_id, original.revision_id)
                self.assertIsNotNone(interleaved["revision"])
                self.assertEqual(reader.total_changes, before)
                self.assertFalse(reader.in_transaction)
                with self.assertRaisesRegex(
                    declared.DeclaredCanonError, "expected_revision_mismatch"
                ):
                    declared.validate_current_declared_canon_revision(
                        reader,
                        actor_user_id=61,
                        authority_nonce="wal-current-read2",
                        guild_id=7,
                        declaration_id=original.declaration_id,
                        expected_revision_id=original.revision_id,
                        expected_source_fingerprint=original.source_fingerprint,
                    )
            finally:
                reader.close()
                writer.close()

    def test_latest_validator_reuses_caller_wal_snapshot_and_leaves_it_open(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            database_path = os.path.join(temp_dir, "declared-latest-snapshot.sqlite3")
            writer = sqlite3.connect(database_path, timeout=5)
            reader = sqlite3.connect(database_path, timeout=5)
            try:
                writer.execute("PRAGMA journal_mode=WAL")
                declared.ensure_declared_canon_schema(writer)
                original = declared.add_declared_canon(
                    writer,
                    actor_user_id=61,
                    authority_nonce="wal-latest-add01",
                    **add_kwargs(),
                ).primary
                contested = declared.change_declared_canon_status(
                    writer,
                    actor_user_id=61,
                    authority_nonce="wal-latest-status1",
                    guild_id=7,
                    declaration_id=original.declaration_id,
                    expected_revision_id=original.revision_id,
                    lifecycle_status="contested",
                ).primary

                reader.execute("BEGIN")
                reader.execute(
                    "SELECT revision_id FROM declared_canon_revisions "
                    "WHERE revision_id=?",
                    (contested.revision_id,),
                ).fetchone()
                resolved = declared.change_declared_canon_status(
                    writer,
                    actor_user_id=61,
                    authority_nonce="wal-latest-status2",
                    guild_id=7,
                    declaration_id=contested.declaration_id,
                    expected_revision_id=contested.revision_id,
                    lifecycle_status="resolved",
                ).primary
                before = reader.total_changes
                validated = declared.validate_latest_declared_canon_revision(
                    reader,
                    actor_user_id=61,
                    authority_nonce="wal-latest-read01",
                    guild_id=7,
                    declaration_id=contested.declaration_id,
                    expected_revision_id=contested.revision_id,
                    expected_source_fingerprint=contested.source_fingerprint,
                    expected_lifecycle_status="contested",
                )
                self.assertEqual(validated.revision_id, contested.revision_id)
                self.assertEqual(reader.total_changes, before)
                self.assertTrue(reader.in_transaction)
                reader.commit()
                latest = declared.validate_latest_declared_canon_revision(
                    reader,
                    actor_user_id=61,
                    authority_nonce="wal-latest-read02",
                    guild_id=7,
                    declaration_id=resolved.declaration_id,
                    expected_revision_id=resolved.revision_id,
                    expected_source_fingerprint=resolved.source_fingerprint,
                    expected_lifecycle_status="resolved",
                )
                self.assertEqual(latest.revision_id, resolved.revision_id)
                self.assertFalse(reader.in_transaction)
            finally:
                if reader.in_transaction:
                    reader.rollback()
                reader.close()
                writer.close()

    def test_mutation_refuses_caller_owned_transaction(self):
        self.conn.execute("BEGIN")
        with self.assertRaisesRegex(
            declared.DeclaredCanonError, "transaction_already_active"
        ):
            self.add()
        self.conn.rollback()
        self.assertEqual(self.rows("SELECT COUNT(*) FROM declared_canon_revisions")[0][0], 0)


if __name__ == "__main__":
    unittest.main()
