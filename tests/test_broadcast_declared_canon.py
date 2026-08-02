from dataclasses import asdict
from datetime import datetime, timezone
import inspect
import os
import re
import sqlite3
import unittest
from unittest import mock

import bnl_declared_canon as declared


class BroadcastDeclaredCanonTests(unittest.TestCase):
    def setUp(self):
        self.env = mock.patch.dict(
            os.environ,
            {
                "BNL_OWNER_USER_ID": "61",
                "BNL_PRIMARY_GUILD_ID": "7",
                "BNL_DECLARED_CANON_AUTHORITY_SECRET": (
                    "declared-canon-test-signing-secret-0001"
                ),
            },
        )
        self.env.start()
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.conn.execute(
            """
            CREATE TABLE broadcast_memory (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id INTEGER NOT NULL,
                episode_date TEXT NOT NULL,
                submitted_by_user_id INTEGER,
                submitted_by_name TEXT,
                raw_note TEXT,
                cleaned_summary TEXT NOT NULL,
                entry_type TEXT NOT NULL,
                importance TEXT DEFAULT 'medium',
                public_safe INTEGER DEFAULT 0,
                affects_next_show INTEGER DEFAULT 0,
                usage_scope TEXT DEFAULT 'internal',
                target_show_date TEXT,
                valid_until TEXT,
                override_span_count INTEGER DEFAULT 1,
                needs_clarification INTEGER DEFAULT 0,
                status TEXT DEFAULT 'active',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                corrected_by_user_id INTEGER,
                corrected_by_name TEXT,
                correction_reason TEXT,
                supersedes_id INTEGER,
                superseded_by_id INTEGER
            )
            """
        )
        self.conn.commit()
        declared.ensure_declared_canon_schema(self.conn)

    def tearDown(self):
        self.conn.close()
        self.env.stop()

    def insert_broadcast(
        self,
        entry_type="notable_moment",
        *,
        guild_id=7,
        submitter=61,
        raw="Owner raw broadcast declaration.",
        cleaned="Public-safe broadcast summary.",
        public_safe=1,
        usage_scope="ambient,direct",
        valid_until="",
        status="active",
        created_at="2026-08-01T01:00:00+00:00",
        updated_at="2026-08-01T01:00:00+00:00",
        supersedes_id=None,
        superseded_by_id=None,
    ):
        cursor = self.conn.execute(
            """
            INSERT INTO broadcast_memory(
                guild_id,episode_date,submitted_by_user_id,submitted_by_name,
                raw_note,cleaned_summary,entry_type,importance,public_safe,
                affects_next_show,usage_scope,target_show_date,valid_until,
                override_span_count,needs_clarification,status,created_at,
                updated_at,corrected_by_user_id,corrected_by_name,
                correction_reason,supersedes_id,superseded_by_id
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                guild_id,
                "2026-08-01",
                submitter,
                "6 Bit" if submitter == 61 else "Test Member",
                raw,
                cleaned,
                entry_type,
                "medium",
                public_safe,
                0,
                usage_scope,
                None,
                valid_until,
                1,
                0,
                status,
                created_at,
                updated_at,
                None,
                None,
                None,
                supersedes_id,
                superseded_by_id,
            ),
        )
        self.conn.commit()
        return int(cursor.lastrowid)

    def source(self, row_id):
        row = self.conn.execute(
            "SELECT * FROM broadcast_memory WHERE id=?", (row_id,)
        ).fetchone()
        return dict(row)

    def classify(
        self,
        row_id,
        nonce="classify-0001",
        *,
        expected_fingerprint=None,
        expected_revision_id="",
        actor=61,
        **overrides,
    ):
        values = {
            "guild_id": 7,
            "broadcast_row_id": row_id,
            "expected_source_fingerprint": (
                expected_fingerprint
                if expected_fingerprint is not None
                else declared.broadcast_source_fingerprint(self.source(row_id))
            ),
            "expected_revision_id": expected_revision_id,
            "subject_type": "broadcast",
            "subject_id": "barcode_radio",
            "visibility": "internal",
            "eligible_routes": ("broadcast_memory",),
            "now": "2026-08-01T02:00:00+00:00",
        }
        values.update(overrides)
        return declared.classify_broadcast_memory(
            self.conn,
            actor_user_id=actor,
            authority_nonce=nonce,
            **values,
        )

    def test_six_exact_current_types_receive_only_versioned_defaults(self):
        for index, (entry_type, expected) in enumerate(
            declared.BROADCAST_TYPE_DEFAULTS.items(), start=1
        ):
            row_id = self.insert_broadcast(entry_type)
            revision = self.classify(
                row_id, nonce="classify-%04d" % index
            ).primary
            self.assertEqual((revision.domain, revision.claim_kind), expected)
            self.assertEqual(revision.predicate, entry_type)
            self.assertEqual(revision.lifecycle_status, "established")
            self.assertEqual(revision.classification_mode, "owner_explicit_default_mapping")

    def test_owner_can_override_default_mapping_without_changing_source_type(self):
        row_id = self.insert_broadcast("notable_moment")
        revision = self.classify(
            row_id,
            domain="lore",
            claim_kind="other",
        ).primary
        self.assertEqual((revision.domain, revision.claim_kind), ("lore", "other"))
        self.assertEqual(revision.predicate, "notable_moment")
        self.assertEqual(revision.classification_mode, "owner_explicit_mapping_override")
        self.assertEqual(self.source(row_id)["entry_type"], "notable_moment")

    def test_broadcast_sidecar_copies_no_source_content(self):
        raw = "Private owner wording remains only in Broadcast Memory."
        cleaned = "Clean public wording remains only in Broadcast Memory."
        row_id = self.insert_broadcast(raw=raw, cleaned=cleaned)
        revision = self.classify(row_id).primary
        stored = self.conn.execute(
            "SELECT raw_declaration,cleaned_summary,value_json "
            "FROM declared_canon_revisions WHERE revision_id=?",
            (revision.revision_id,),
        ).fetchone()
        self.assertEqual(tuple(stored), ("", "", ""))
        table_dump = repr(
            self.conn.execute(
                "SELECT raw_declaration,cleaned_summary,value_json "
                "FROM declared_canon_revisions"
            ).fetchall()
        )
        self.assertNotIn(raw, table_dump)
        self.assertNotIn(cleaned, table_dump)

    def test_legacy_exact_type_is_zero_write_until_explicit_owner_mapping(self):
        legacy_types = (
            "continuity_backreference",
            "show_note",
            "recap",
            "arbitrary_low_level_value",
            "Notable_Moment",
        )
        for index, entry_type in enumerate(legacy_types, start=1):
            row_id = self.insert_broadcast(entry_type)
            before = self.conn.total_changes
            with self.assertRaisesRegex(
                declared.DeclaredCanonError, "legacy_type_review_only"
            ):
                self.classify(row_id, nonce="legacy-%04d" % index)
            self.assertEqual(self.conn.total_changes, before)

        row_id = self.insert_broadcast("continuity_backreference")
        revision = self.classify(
            row_id,
            nonce="legacy-explicit1",
            domain="broadcast_history",
            claim_kind="event",
        ).primary
        self.assertEqual(revision.predicate, "continuity_backreference")
        self.assertEqual(revision.classification_mode, "owner_explicit_legacy_mapping")
        self.assertEqual(self.source(row_id)["entry_type"], "continuity_backreference")
        preview = declared.preview_historical_broadcast_memory(
            self.conn,
            actor_user_id=61,
            authority_nonce="legacy-preview1",
            guild_id=7,
            now="2026-08-01T04:00:00+00:00",
        )
        explicit_item = preview.items[-1]
        self.assertEqual(explicit_item.entry_type, "legacy_or_unrecognized")
        self.assertEqual(explicit_item.disposition, "declared_classification_current")
        self.assertIn("legacy_type_explicitly_classified", explicit_item.reason_codes)

    def test_typed_subject_is_explicit_and_submitter_is_provenance_only(self):
        row_id = self.insert_broadcast(submitter=62)
        revision = self.classify(
            row_id,
            subject_type="person",
            subject_id="test_member",
        ).primary
        self.assertEqual(revision.subject_id, "test_member")
        self.assertNotEqual(revision.subject_id, "discord_user:62")

    def test_relationship_mapping_requires_typed_counterpart_and_predicate(self):
        row_id = self.insert_broadcast()
        with self.assertRaisesRegex(
            declared.DeclaredCanonError, "relationship_predicate_required"
        ):
            self.classify(
                row_id,
                claim_kind="relationship",
                object_subject_type="organization",
                object_subject_id="barcode_network",
            )
        relation = self.classify(
            row_id,
            nonce="relation-map01",
            claim_kind="relationship",
            predicate="broadcasts_for",
            object_subject_type="organization",
            object_subject_id="barcode_network",
        ).primary
        self.assertEqual(relation.object_subject_type, "organization")
        self.assertEqual(relation.object_subject_id, "barcode_network")

    def test_public_classification_requires_safe_current_source_and_scope(self):
        good = self.insert_broadcast(public_safe=1, usage_scope="ambient,direct")
        revision = self.classify(
            good,
            visibility="public_safe",
            eligible_routes=("public_home", "public_selective"),
        ).primary
        self.assertEqual(revision.visibility, "public_safe")

        false_source = self.insert_broadcast(public_safe=0)
        with self.assertRaisesRegex(
            declared.DeclaredCanonError, "broadcast_source_not_public_safe"
        ):
            self.classify(
                false_source,
                nonce="public-false1",
                visibility="public_safe",
                eligible_routes=("public_home",),
            )
        string_true = self.insert_broadcast(public_safe="true")
        with self.assertRaisesRegex(
            declared.DeclaredCanonError, "broadcast_source_not_public_safe"
        ):
            self.classify(
                string_true,
                nonce="public-string1",
                visibility="public_safe",
                eligible_routes=("public_home",),
            )
        internal_scope = self.insert_broadcast(public_safe=1, usage_scope="internal,direct")
        with self.assertRaisesRegex(
            declared.DeclaredCanonError, "broadcast_internal_scope_veto"
        ):
            self.classify(
                internal_scope,
                nonce="public-intern1",
                visibility="public_safe",
                eligible_routes=("public_home",),
            )
        no_relay_scope = self.insert_broadcast(public_safe=1, usage_scope="ambient,direct")
        with self.assertRaisesRegex(
            declared.DeclaredCanonError, "broadcast_route_scope_widening"
        ):
            self.classify(
                no_relay_scope,
                nonce="route-widen-01",
                visibility="public_safe",
                eligible_routes=("relay",),
            )
        resolved = self.insert_broadcast(status="resolved")
        with self.assertRaisesRegex(
            declared.DeclaredCanonError, "broadcast_source_not_current_for_public"
        ):
            self.classify(
                resolved,
                nonce="public-resolved",
                visibility="public_safe",
                eligible_routes=("public_home",),
            )

    def test_moderation_context_is_internal_even_if_source_says_safe(self):
        row_id = self.insert_broadcast("moderation_context", public_safe=1)
        with self.assertRaisesRegex(
            declared.DeclaredCanonError, "moderation_context_internal_only"
        ):
            self.classify(
                row_id,
                visibility="public_safe",
                eligible_routes=("public_home",),
            )
        internal = self.classify(
            row_id, nonce="moderation-002", visibility="internal"
        ).primary
        self.assertEqual(internal.visibility, "internal")

    def test_fingerprint_binds_every_known_and_future_source_column(self):
        row_id = self.insert_broadcast()
        original_row = self.source(row_id)
        original = declared.broadcast_source_fingerprint(original_row)
        mutations = {
            "cleaned_summary": "changed",
            "updated_at": "2026-08-01T03:00:00+00:00",
            "status": "resolved",
            "public_safe": 0,
            "usage_scope": "internal",
            "valid_until": "2026-07-01T00:00:00+00:00",
            "submitted_by_user_id": 62,
            "corrected_by_user_id": 62,
            "correction_reason": "changed provenance",
            "supersedes_id": 99,
            "superseded_by_id": 100,
        }
        for column, value in mutations.items():
            changed = dict(original_row)
            changed[column] = value
            self.assertNotEqual(
                declared.broadcast_source_fingerprint(changed),
                original,
                column,
            )
        unrelated_future_column = dict(original_row)
        unrelated_future_column["future_display_hint"] = None
        self.assertNotEqual(
            declared.broadcast_source_fingerprint(unrelated_future_column),
            original,
        )
        changed_future_column = dict(unrelated_future_column)
        changed_future_column["future_display_hint"] = "now populated"
        self.assertNotEqual(
            declared.broadcast_source_fingerprint(changed_future_column),
            declared.broadcast_source_fingerprint(unrelated_future_column),
        )
        self.assertEqual(
            declared.broadcast_source_fingerprint(
                dict(reversed(tuple(unrelated_future_column.items())))
            ),
            declared.broadcast_source_fingerprint(unrelated_future_column),
        )

    def test_source_schema_addition_stales_existing_approval_even_when_null(self):
        row_id = self.insert_broadcast()
        revision = self.classify(row_id, nonce="schema-stale-add1").primary
        self.conn.execute(
            "ALTER TABLE broadcast_memory ADD COLUMN future_display_hint TEXT"
        )
        self.conn.commit()
        preview = declared.preview_historical_broadcast_memory(
            self.conn,
            actor_user_id=61,
            authority_nonce="schema-stale-preview1",
            guild_id=7,
            now="2026-08-01T04:00:00+00:00",
        )
        self.assertEqual(preview.items[0].source_fingerprint_state, "stale_or_unversioned")
        self.assertIn("classification_source_stale", preview.items[0].reason_codes)
        with self.assertRaisesRegex(
            declared.DeclaredCanonError, "expected_source_fingerprint_mismatch"
        ):
            declared.validate_current_declared_canon_revision(
                self.conn,
                actor_user_id=61,
                authority_nonce="schema-stale-read01",
                guild_id=7,
                declaration_id=revision.declaration_id,
                expected_revision_id=revision.revision_id,
                expected_source_fingerprint=revision.source_fingerprint,
                now="2026-08-01T04:00:00+00:00",
            )

    def test_stale_owner_snapshot_rejects_before_write(self):
        row_id = self.insert_broadcast()
        approved = declared.broadcast_source_fingerprint(self.source(row_id))
        self.conn.execute(
            "UPDATE broadcast_memory SET public_safe=0,usage_scope='internal',updated_at=? WHERE id=?",
            ("2026-08-01T03:00:00+00:00", row_id),
        )
        self.conn.commit()
        before = self.conn.total_changes
        with self.assertRaisesRegex(
            declared.DeclaredCanonError, "expected_source_fingerprint_mismatch"
        ):
            self.classify(
                row_id,
                expected_fingerprint=approved,
                nonce="stale-source01",
            )
        self.assertEqual(self.conn.total_changes, before)

    def test_classification_exact_retry_and_nonce_replay_are_fail_closed(self):
        row_one = self.insert_broadcast()
        first = self.classify(row_one, nonce="class-replay01")
        before = self.conn.total_changes
        retry = self.classify(row_one, nonce="class-replay01")
        self.assertEqual(retry.primary.revision_id, first.primary.revision_id)
        self.assertEqual(self.conn.total_changes, before)
        row_two = self.insert_broadcast()
        with self.assertRaisesRegex(
            declared.DeclaredCanonError, "authority_nonce_replay_mismatch"
        ):
            self.classify(row_two, nonce="class-replay01")

    def test_reclassification_cannot_launder_forged_prior_sidecar(self):
        row_id = self.insert_broadcast()
        created = self.classify(row_id, nonce="forge-sidecar01").primary
        forged = asdict(created)
        forged["revision_number"] = 2
        forged["previous_revision_id"] = created.revision_id
        forged["correction_of_revision_id"] = created.revision_id
        forged["operation_id"] = "op_" + "6" * 32
        forged["authority_request_fingerprint"] = "req_" + "7" * 48
        forged["authority_verified"] = 1
        forged["authority_receipt"] = (
            "owner_command:declared_canon_lifecycle_v1:"
            "declared_canon_internal_receipt_v1:classify_broadcast:"
            + "8" * 64
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
            self.classify(
                row_id,
                nonce="forge-sidecar02",
                expected_revision_id=forged["revision_id"],
            )
        self.assertEqual(self.conn.total_changes, before)

    def test_reclassification_requires_expected_revision_and_appends(self):
        row_id = self.insert_broadcast()
        original = self.classify(row_id).primary
        self.conn.execute(
            "UPDATE broadcast_memory SET cleaned_summary=?,updated_at=? WHERE id=?",
            ("Corrected source summary.", "2026-08-01T03:00:00+00:00", row_id),
        )
        self.conn.commit()
        current_fingerprint = declared.broadcast_source_fingerprint(self.source(row_id))
        with self.assertRaisesRegex(
            declared.DeclaredCanonError, "expected_revision_mismatch"
        ):
            self.classify(
                row_id,
                nonce="reclass-stale1",
                expected_fingerprint=current_fingerprint,
            )
        corrected = self.classify(
            row_id,
            nonce="reclass-good01",
            expected_fingerprint=current_fingerprint,
            expected_revision_id=original.revision_id,
        ).primary
        self.assertEqual(corrected.revision_number, 2)
        self.assertEqual(corrected.previous_revision_id, original.revision_id)
        self.assertNotEqual(corrected.source_fingerprint, original.source_fingerprint)

    def test_classification_owner_and_primary_guild_boundaries(self):
        row_id = self.insert_broadcast()
        with self.assertRaisesRegex(
            declared.DeclaredCanonError, "configured_owner_required"
        ):
            self.classify(row_id, actor=62)
        with self.assertRaisesRegex(
            declared.DeclaredCanonError, "configured_primary_guild_required"
        ):
            self.classify(row_id, guild_id=8)

    def test_historical_preview_is_zero_write_content_free_and_fixed_era_grouped(self):
        pre_id = self.insert_broadcast(
            created_at="2026-07-22T12:00:00+00:00",
            updated_at="2026-07-22T12:00:00+00:00",
            raw="Pre-boundary raw content.",
            cleaned="Pre-boundary clean content.",
        )
        post_id = self.insert_broadcast(
            "continuity_backreference",
            created_at="2026-07-24T12:00:00+00:00",
            updated_at="2026-07-24T12:00:00+00:00",
            raw="Post-boundary raw content.",
            cleaned="Post-boundary clean content.",
            submitter=62,
            supersedes_id=pre_id,
        )
        before = self.conn.total_changes
        preview = declared.preview_historical_broadcast_memory(
            self.conn,
            actor_user_id=61,
            authority_nonce="preview-bcast1",
            guild_id=7,
            now="2026-08-01T04:00:00+00:00",
        )
        self.assertEqual(preview.mutation_count, 0)
        self.assertEqual(self.conn.total_changes, before)
        self.assertEqual(
            preview.owner_era_cutoff,
            declared.BROADCAST_DECLARED_CANON_OWNER_ERA_CUTOFF,
        )
        self.assertEqual(
            tuple(item.source_era for item in preview.items),
            ("pre_declared_canon_owner_era", "post_declared_canon_owner_era"),
        )
        self.assertEqual(
            tuple(item.disposition for item in preview.items),
            ("needs_owner_review", "needs_owner_review"),
        )
        self.assertRegex(preview.authority_actor_ref, r"^actor_[0-9a-f]{20}$")
        self.assertRegex(preview.authority_receipt_id, r"^receipt_[0-9a-f]{24}$")
        for item in preview.items:
            fields = asdict(item)
            self.assertNotIn("source_row_id", fields)
            self.assertNotIn("source_token", fields)
            self.assertRegex(item.preview_item, r"^item_[0-9a-f]{16}$")
        self.assertNotIn("Pre-boundary raw content.", repr(preview))
        self.assertNotIn("Post-boundary clean content.", repr(preview))
        self.assertNotIn("submitted_by_user_id", repr(preview))
        self.assertNotIn("owner_era_cutoff", inspect.signature(
            declared.preview_historical_broadcast_memory
        ).parameters)
        second = declared.preview_historical_broadcast_memory(
            self.conn,
            actor_user_id=61,
            authority_nonce="preview-bcast2",
            guild_id=7,
            now="2026-08-01T04:00:00+00:00",
        )
        self.assertNotEqual(preview.items[0].preview_item, second.items[0].preview_item)
        paged = declared.preview_historical_broadcast_memory(
            self.conn,
            actor_user_id=61,
            authority_nonce="preview-page01",
            guild_id=7,
            limit=1,
            now="2026-08-01T04:00:00+00:00",
        )
        self.assertEqual(paged.total_rows, 2)
        self.assertTrue(paged.truncated)
        self.assertEqual(paged.counts_scope, "returned_page")
        self.assertEqual(sum(paged.type_counts.values()), 1)
        self.assertEqual(sum(paged.era_counts.values()), 1)
        self.assertEqual(sum(paged.disposition_counts.values()), 1)
        self.assertEqual(pre_id + 1, post_id)

    def test_historical_preview_reason_codes_malformed_provenance(self):
        self.insert_broadcast(
            submitter="not-a-user-id",
            created_at="not-a-timestamp",
            updated_at="2026-08-01T01:00:00+00:00",
        )
        preview = declared.preview_historical_broadcast_memory(
            self.conn,
            actor_user_id=61,
            authority_nonce="malformed-prev1",
            guild_id=7,
            now="2026-08-01T04:00:00+00:00",
        )
        self.assertEqual(len(preview.items), 1)
        item = preview.items[0]
        self.assertEqual(item.source_era, "unknown_era")
        self.assertEqual(item.submitter_state, "unverified")
        self.assertIn("submitter_identity_malformed", item.reason_codes)
        self.assertIn("owner_authorship_unverified", item.reason_codes)

    def test_historical_preview_buckets_hostile_legacy_metadata(self):
        secret_type = "person_email_alice@example.com"
        secret_scope = "internal,secret_account_12345"
        secret_status = "active_private_secret"
        self.insert_broadcast(
            secret_type,
            usage_scope=secret_scope,
            status=secret_status,
            created_at="2026-07-22T12:00:00+00:00",
            updated_at="2026-07-22T12:00:00+00:00",
        )
        preview = declared.preview_historical_broadcast_memory(
            self.conn,
            actor_user_id=61,
            authority_nonce="hostile-preview1",
            guild_id=7,
            now="2026-08-01T04:00:00+00:00",
        )
        rendered = repr(preview)
        self.assertNotIn(secret_type, rendered)
        self.assertNotIn("secret_account_12345", rendered)
        self.assertNotIn(secret_status, rendered)
        self.assertEqual(preview.items[0].entry_type, "legacy_or_unrecognized")
        self.assertEqual(preview.items[0].status, "unrecognized")
        self.assertEqual(
            preview.items[0].usage_scope, "internal,unknown_scope_present"
        )
        self.assertEqual(
            preview.type_counts, {"legacy_or_unrecognized": 1}
        )

    def test_preview_recomputes_fingerprint_and_never_calls_stale_public_current(self):
        row_id = self.insert_broadcast(public_safe=1, usage_scope="ambient,direct")
        revision = self.classify(
            row_id,
            visibility="public_safe",
            eligible_routes=("public_home",),
        ).primary
        current = declared.preview_historical_broadcast_memory(
            self.conn,
            actor_user_id=61,
            authority_nonce="preview-current",
            guild_id=7,
            now="2026-08-01T04:00:00+00:00",
        )
        self.assertEqual(current.items[0].source_fingerprint_state, "current")
        self.assertEqual(current.items[0].disposition, "declared_classification_current")

        self.conn.execute(
            "UPDATE broadcast_memory SET status='resolved',public_safe=0,"
            "usage_scope='internal',valid_until=?,updated_at=? WHERE id=?",
            (
                "2026-07-01T00:00:00+00:00",
                "2026-08-01T05:00:00+00:00",
                row_id,
            ),
        )
        self.conn.commit()
        stale = declared.preview_historical_broadcast_memory(
            self.conn,
            actor_user_id=61,
            authority_nonce="preview-stale01",
            guild_id=7,
            now="2026-08-01T06:00:00+00:00",
        )
        item = stale.items[0]
        self.assertEqual(item.source_fingerprint_state, "stale_or_unversioned")
        self.assertEqual(item.disposition, "stale_classification_review")
        self.assertIn("classification_source_stale", item.reason_codes)
        self.assertTrue(item.classification_state.startswith("not_current:"))
        with self.assertRaisesRegex(
            declared.DeclaredCanonError, "expected_source_fingerprint_mismatch"
        ):
            declared.validate_current_declared_canon_revision(
                self.conn,
                actor_user_id=61,
                authority_nonce="validate-stale1",
                guild_id=7,
                declaration_id=revision.declaration_id,
                expected_revision_id=revision.revision_id,
                expected_source_fingerprint=revision.source_fingerprint,
                now="2026-08-01T06:00:00+00:00",
            )

    def test_read_validator_accepts_only_current_source_intersection_zero_write(self):
        row_id = self.insert_broadcast(public_safe=1, usage_scope="ambient,direct")
        revision = self.classify(
            row_id,
            visibility="public_safe",
            eligible_routes=("public_home",),
        ).primary
        before = self.conn.total_changes
        result = declared.validate_current_declared_canon_revision(
            self.conn,
            actor_user_id=61,
            authority_nonce="validate-bcast1",
            guild_id=7,
            declaration_id=revision.declaration_id,
            expected_revision_id=revision.revision_id,
            expected_source_fingerprint=revision.source_fingerprint,
            now="2026-08-01T04:00:00+00:00",
        )
        self.assertEqual(result.revision_id, revision.revision_id)
        self.assertEqual(self.conn.total_changes, before)

    def test_implicit_runtime_now_preserves_microseconds_for_immediate_validation(self):
        source_now = datetime.now(timezone.utc).isoformat()
        row_id = self.insert_broadcast(
            created_at=source_now,
            updated_at=source_now,
        )
        revision = self.classify(
            row_id,
            nonce="microsecond-add1",
            now="",
        ).primary
        validated = declared.validate_current_declared_canon_revision(
            self.conn,
            actor_user_id=61,
            authority_nonce="microsecond-read",
            guild_id=7,
            declaration_id=revision.declaration_id,
            expected_revision_id=revision.revision_id,
            expected_source_fingerprint=revision.source_fingerprint,
        )
        self.assertEqual(validated.revision_id, revision.revision_id)

    def test_latest_validator_exposes_resolved_broadcast_for_retraction_only(self):
        row_id = self.insert_broadcast(status="resolved", public_safe=0, usage_scope="internal")
        revision = self.classify(row_id).primary
        self.assertEqual(revision.lifecycle_status, "resolved")
        before = self.conn.total_changes
        latest = declared.validate_latest_declared_canon_revision(
            self.conn,
            actor_user_id=61,
            authority_nonce="resolved-read01",
            guild_id=7,
            declaration_id=revision.declaration_id,
            expected_revision_id=revision.revision_id,
            expected_source_fingerprint=revision.source_fingerprint,
            expected_lifecycle_status="resolved",
            now="2026-08-01T04:00:00+00:00",
        )
        self.assertEqual(latest.lifecycle_status, "resolved")
        self.assertEqual(self.conn.total_changes, before)
        with self.assertRaisesRegex(
            declared.DeclaredCanonError, "declared_revision_not_established"
        ):
            declared.validate_current_declared_canon_revision(
                self.conn,
                actor_user_id=61,
                authority_nonce="resolved-current",
                guild_id=7,
                declaration_id=revision.declaration_id,
                expected_revision_id=revision.revision_id,
                expected_source_fingerprint=revision.source_fingerprint,
                now="2026-08-01T04:00:00+00:00",
            )

    def test_preview_and_validator_reject_forged_stored_authority(self):
        row_id = self.insert_broadcast()
        revision = self.classify(row_id).primary
        self.conn.execute("DROP TRIGGER trg_declared_canon_revisions_no_update")
        self.conn.execute(
            "UPDATE declared_canon_revisions SET authority_receipt='owner_confirmed' "
            "WHERE revision_id=?",
            (revision.revision_id,),
        )
        self.conn.execute(
            declared._DECLARED_CANON_TRIGGER_DDL[
                "trg_declared_canon_revisions_no_update"
            ]
        )
        self.conn.commit()
        declared._require_schema(self.conn)
        with self.assertRaisesRegex(
            declared.DeclaredCanonError, "stored_authority_invalid"
        ):
            declared.preview_historical_broadcast_memory(
                self.conn,
                actor_user_id=61,
                authority_nonce="forged-preview1",
                guild_id=7,
                now="2026-08-01T04:00:00+00:00",
            )
        with self.assertRaisesRegex(
            declared.DeclaredCanonError, "stored_authority_invalid"
        ):
            declared.validate_current_declared_canon_revision(
                self.conn,
                actor_user_id=61,
                authority_nonce="forged-validate1",
                guild_id=7,
                declaration_id=revision.declaration_id,
                expected_revision_id=revision.revision_id,
                expected_source_fingerprint=revision.source_fingerprint,
                now="2026-08-01T04:00:00+00:00",
            )

    def test_temp_broadcast_table_cannot_shadow_terminal_main_source(self):
        row_id = self.insert_broadcast(public_safe=1, usage_scope="ambient,direct")
        revision = self.classify(
            row_id,
            nonce="temp-broadcast1",
            visibility="public_safe",
            eligible_routes=("public_home",),
        ).primary
        self.conn.execute(
            "CREATE TEMP TABLE broadcast_memory AS "
            "SELECT * FROM main.broadcast_memory WHERE id=?",
            (row_id,),
        )
        self.conn.execute(
            "UPDATE main.broadcast_memory SET status='resolved',public_safe=0,"
            "usage_scope='internal',updated_at=? WHERE id=?",
            ("2026-08-01T05:00:00+00:00", row_id),
        )
        self.conn.commit()
        with self.assertRaisesRegex(
            declared.DeclaredCanonError, "expected_source_fingerprint_mismatch"
        ):
            declared.validate_current_declared_canon_revision(
                self.conn,
                actor_user_id=61,
                authority_nonce="temp-broadcast2",
                guild_id=7,
                declaration_id=revision.declaration_id,
                expected_revision_id=revision.revision_id,
                expected_source_fingerprint=revision.source_fingerprint,
                now="2026-08-01T06:00:00+00:00",
            )
        preview = declared.preview_historical_broadcast_memory(
            self.conn,
            actor_user_id=61,
            authority_nonce="temp-broadcast3",
            guild_id=7,
            now="2026-08-01T06:00:00+00:00",
        )
        self.assertEqual(preview.items[0].source_fingerprint_state, "stale_or_unversioned")

    def test_no_projection_or_derived_classification_api_exists(self):
        signature = inspect.signature(declared.classify_broadcast_memory)
        self.assertNotIn("derived_from_source_ref", signature.parameters)
        self.assertFalse(any("project" in name for name in declared.__all__))

    def test_missing_or_unversioned_broadcast_schema_fails_closed(self):
        other = sqlite3.connect(":memory:")
        try:
            declared.ensure_declared_canon_schema(other)
            before = other.total_changes
            with self.assertRaisesRegex(
                declared.DeclaredCanonError, "broadcast_source_not_found"
            ):
                declared.classify_broadcast_memory(
                    other,
                    actor_user_id=61,
                    authority_nonce="missing-table1",
                    guild_id=7,
                    broadcast_row_id=1,
                    expected_source_fingerprint="bsrc_" + "0" * 40,
                    subject_type="broadcast",
                    subject_id="barcode_radio",
                )
            self.assertEqual(other.total_changes, before)
        finally:
            other.close()


if __name__ == "__main__":
    unittest.main()
