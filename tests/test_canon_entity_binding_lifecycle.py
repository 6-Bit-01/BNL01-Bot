import os
import sqlite3
import unittest
from unittest import mock

import bnl_canon_entity_binding as binding
import bnl_canon_source_contract as canon_contract
from bnl_canon_source_contract import CALL_EM_BINI, CACHE_BACK


class CanonEntityBindingLifecycleTests(unittest.TestCase):
    def setUp(self):
        self.env = mock.patch.dict(
            os.environ,
            {
                "BNL_OWNER_USER_ID": "61",
                "BNL_PRIMARY_GUILD_ID": "7",
                "BNL_DECLARED_CANON_AUTHORITY_SECRET": (
                    "canon-entity-binding-test-secret-0001"
                ),
            },
            clear=False,
        )
        self.env.start()
        self.conn = sqlite3.connect(":memory:")
        binding.ensure_canon_entity_binding_schema(self.conn)

    def tearDown(self):
        self.conn.close()
        self.env.stop()

    def bind(
        self,
        nonce="binding-add-0001",
        *,
        account_id="4242",
        entity_id=CALL_EM_BINI.key,
        actor=61,
        guild=7,
    ):
        return binding.bind_discord_account(
            self.conn,
            actor_user_id=actor,
            authority_nonce=nonce,
            guild_id=guild,
            account_id=account_id,
            entity_id=entity_id,
            reason="Owner-approved same-platform identity binding.",
        )

    def test_schema_is_exact_idempotent_and_append_only(self):
        binding.ensure_canon_entity_binding_schema(self.conn)
        created = self.bind().revision
        with self.assertRaisesRegex(
            sqlite3.IntegrityError,
            "canon_entity_binding_append_only_update",
        ):
            self.conn.execute(
                "UPDATE canon_entity_account_bindings SET active=0 "
                "WHERE binding_revision_id=?",
                (created.binding_revision_id,),
            )
        self.conn.rollback()
        with self.assertRaisesRegex(
            sqlite3.IntegrityError,
            "canon_entity_binding_append_only_delete",
        ):
            self.conn.execute(
                "DELETE FROM canon_entity_account_bindings "
                "WHERE binding_revision_id=?",
                (created.binding_revision_id,),
            )
        self.conn.rollback()

    def test_bind_read_and_retire_without_identity_merge(self):
        created = self.bind().revision
        current = binding.read_current_entity_account_bindings(
            self.conn,
            guild_id=7,
            platform="discord",
            account_id="4242",
        )
        self.assertEqual(current.status, "active")
        self.assertEqual(len(current.bindings), 1)
        self.assertEqual(current.bindings[0].entity_id, CALL_EM_BINI.key)
        self.assertNotEqual(current.bindings[0].entity_id, CACHE_BACK.key)

        retired = binding.retire_discord_account_binding(
            self.conn,
            actor_user_id=61,
            authority_nonce="binding-retire-0001",
            guild_id=7,
            binding_id=created.binding_id,
            expected_revision_id=created.binding_revision_id,
            reason="Account identity changed; retire the old binding.",
        ).revision
        self.assertFalse(retired.active)
        self.assertEqual(retired.previous_revision_id, created.binding_revision_id)
        after = binding.read_current_entity_account_bindings(
            self.conn,
            guild_id=7,
            platform="discord",
            account_id="4242",
        )
        self.assertEqual(after.status, "retired_account_binding")
        self.assertEqual(after.bindings, ())

    def test_collision_rejects_until_old_binding_is_retired(self):
        self.bind()
        with self.assertRaisesRegex(
            binding.CanonEntityBindingError,
            "account_binding_collision",
        ):
            self.bind(
                "binding-add-0002",
                entity_id=CACHE_BACK.key,
            )

    def test_exact_retry_is_zero_write_and_changed_replay_rejects(self):
        first = self.bind()
        changes = self.conn.total_changes
        retried = self.bind()
        self.assertEqual(
            retried.revision.binding_revision_id,
            first.revision.binding_revision_id,
        )
        self.assertEqual(self.conn.total_changes, changes)
        with self.assertRaisesRegex(
            binding.CanonEntityBindingError,
            "binding_operation_replay_conflict",
        ):
            self.bind(
                account_id="4343",
            )

    def test_owner_guild_and_secret_are_rechecked(self):
        with self.assertRaisesRegex(
            binding.CanonEntityBindingError,
            "configured_owner_required",
        ):
            self.bind("binding-wrong-owner", actor=62)
        with self.assertRaisesRegex(
            binding.CanonEntityBindingError,
            "configured_primary_guild_required",
        ):
            self.bind("binding-wrong-guild", guild=8)
        with mock.patch.dict(
            os.environ,
            {"BNL_DECLARED_CANON_AUTHORITY_SECRET": ""},
            clear=False,
        ):
            with self.assertRaisesRegex(
                binding.CanonEntityBindingError,
                "declared_canon_authority_secret_not_configured",
            ):
                self.bind("binding-no-secret")

    def test_preview_never_returns_account_ids(self):
        self.bind(account_id="424242424242")
        preview = binding.preview_canon_entity_bindings(
            self.conn,
            actor_user_id=61,
            authority_nonce="binding-preview-0001",
            guild_id=7,
        )
        self.assertEqual(preview.active_count, 1)
        self.assertEqual(preview.retired_count, 0)
        self.assertEqual(preview.mutation_count, 0)
        self.assertNotIn("424242424242", repr(preview))

    def test_content_free_inventory_uses_latest_revision_only(self):
        created = self.bind().revision
        active = canon_contract.build_claim_contract_inventory(
            self.conn,
            guild_id=7,
        )
        self.assertEqual(
            active["reasonCounts"].get("eligible_account_binding"),
            1,
        )
        binding.retire_discord_account_binding(
            self.conn,
            actor_user_id=61,
            authority_nonce="binding-retire-inventory",
            guild_id=7,
            binding_id=created.binding_id,
            expected_revision_id=created.binding_revision_id,
            reason="Retire before inventory validation.",
        )
        retired = canon_contract.build_claim_contract_inventory(
            self.conn,
            guild_id=7,
        )
        self.assertEqual(
            retired["reasonCounts"].get("eligible_account_binding"),
            0,
        )
        self.assertEqual(
            retired["reasonCounts"].get(
                "historical_or_inactive_account_binding_revision"
            ),
            2,
        )
        self.assertEqual(retired["identityBindingCollisionCount"], 0)
        self.assertEqual(retired["sourceReconciliationStatus"], "complete")


if __name__ == "__main__":
    unittest.main()
