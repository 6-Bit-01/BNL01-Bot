import hashlib
import json
import os
import sqlite3
import tempfile
import unittest
from dataclasses import replace
from unittest import mock

import bnl_canon_source_contract as canon
import bnl_canon_entity_binding as entity_binding
import bnl_declared_canon as declared_canon
import bnl_memory_ledger as ledger
import bnl_moment_engine as moments
import bnl_unified_intelligence_packet as packet


def _ledger_digest(*parts):
    return hashlib.sha256(
        "\x1f".join(str(part or "") for part in parts).encode("utf-8")
    ).hexdigest()


def _living_adapter_row():
    subject_key = "discord_user:7"
    predicate_key = "unseen_topic_pattern"
    domain = "real_community"
    root_ids = ("root-1", "root-2")
    occurrence_ids = ("exchange-1", "exchange-2")
    grouping_identity = _ledger_digest(
        canon.LIVING_CANON_GROUPING_SIGNATURE_VERSION,
        subject_key,
        predicate_key,
        domain,
    )
    root_digest = _ledger_digest(*root_ids)
    occurrence_digest = _ledger_digest(*occurrence_ids)
    meaning = "They repeatedly compare unusual antenna materials."
    proof = {
        "recurrence_contract_version": canon.LIVING_CANON_RECURRENCE_VERSION,
        "grouping_signature_version": (
            canon.LIVING_CANON_GROUPING_SIGNATURE_VERSION
        ),
        "grouping_identity": grouping_identity,
        "candidate_state": "established",
        "candidate_eligible": True,
        "source_eligible": True,
        "roots_valid": True,
        "occurrence_bounded": True,
        "correction_fence_clear": True,
        "contradiction_clear": True,
        "independent_root_count": 2,
        "independent_occurrence_count": 2,
        "root_digest": root_digest,
        "occurrence_digest": occurrence_digest,
        "bounds": {
            "eligible_ledger_scan_max": 1200,
            "motif_candidates_max": 6,
            "retained_roots_max": 12,
            "occurrence_lookback_max": 64,
            "idle_boundary_seconds": 1800,
        },
    }
    return {
        "candidate_id": "candidate-2",
        "candidate_type": "topic_or_motif",
        "candidate_state": "established",
        "subject_key": subject_key,
        "predicate_key": predicate_key,
        "meaning": meaning,
        "value_digest": _ledger_digest(" ".join(meaning.lower().split())),
        "root_ids": root_ids,
        "occurrence_ids": occurrence_ids,
        "recurrence_contract_version": canon.LIVING_CANON_RECURRENCE_VERSION,
        "grouping_signature_version": (
            canon.LIVING_CANON_GROUPING_SIGNATURE_VERSION
        ),
        "grouping_identity": grouping_identity,
        "domain": domain,
        "claim_kind": "behavior_pattern",
        "candidate_eligible": True,
        "live_eligible": False,
        "invalidated_reason": "",
        "invalidated_at": "",
        "lifecycle_schema_version": (
            canon.ATOMIC_KNOWLEDGE_LIFECYCLE_SCHEMA_VERSION
        ),
        "conflict_value_count": 1,
        "review_status": "not_required",
        "review_due_at": "",
        "eligible_independent_root_count": 2,
        "reinforcement_count": 2,
        "lifecycle_reason": "independent_recurrence_established",
        "lifecycle_evaluated_at": "2026-08-02T00:00:00+00:00",
        "independent_root_count": 2,
        "independent_occurrence_count": 2,
        "root_digest": root_digest,
        "occurrence_digest": occurrence_digest,
        "recurrence_proof_json": json.dumps(proof, sort_keys=True),
        "visibility": "public_safe",
        "public_usable": True,
    }


class HybridCanonClaimContractTests(unittest.TestCase):
    def setUp(self):
        self._authority_secret = os.environ.get(
            declared_canon.DECLARED_CANON_AUTHORITY_SECRET_ENV
        )
        os.environ[
            declared_canon.DECLARED_CANON_AUTHORITY_SECRET_ENV
        ] = "hybrid-contract-test-authority-secret-0001"

    def tearDown(self):
        if self._authority_secret is None:
            os.environ.pop(
                declared_canon.DECLARED_CANON_AUTHORITY_SECRET_ENV,
                None,
            )
        else:
            os.environ[
                declared_canon.DECLARED_CANON_AUTHORITY_SECRET_ENV
            ] = self._authority_secret

    def create_pr2_broadcast_table(self, conn):
        conn.execute(
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
        conn.commit()
        declared_canon.ensure_declared_canon_schema(conn)

    def insert_pr2_broadcast(
        self,
        conn,
        *,
        cleaned="A source-owned Broadcast summary.",
        raw="Private operator wording.",
        entry_type="notable_moment",
        status="active",
        public_safe=1,
        usage_scope="ambient,direct",
        created_at="2026-08-01T00:00:00+00:00",
        valid_until="",
    ):
        cursor = conn.execute(
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
                7,
                "2026-08-01",
                61,
                "6 Bit",
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
                created_at,
                None,
                None,
                None,
                None,
                None,
            ),
        )
        conn.commit()
        return int(cursor.lastrowid)

    def classify_pr2_broadcast(
        self,
        conn,
        row_id,
        *,
        nonce="classify-contract-0001",
        expected_revision_id="",
        visibility="public_safe",
        eligible_routes=("public_home",),
    ):
        columns = tuple(
            row[1] for row in conn.execute("PRAGMA table_info(broadcast_memory)")
        )
        values = conn.execute(
            "SELECT %s FROM broadcast_memory WHERE id=?" % ",".join(columns),
            (row_id,),
        ).fetchone()
        fingerprint = declared_canon.broadcast_source_fingerprint(
            dict(zip(columns, values))
        )
        return declared_canon.classify_broadcast_memory(
            conn,
            actor_user_id=61,
            authority_nonce=nonce,
            guild_id=7,
            broadcast_row_id=row_id,
            expected_source_fingerprint=fingerprint,
            expected_revision_id=expected_revision_id,
            subject_type="broadcast",
            subject_id="barcode_radio",
            visibility=visibility,
            eligible_routes=eligible_routes,
            now="2026-08-01T01:00:00+00:00",
        ).primary

    def test_contract_separates_status_domain_and_lifecycle(self):
        fact = next(
            item
            for item in canon.CANON_FACTS
            if item.subject == canon.MAC_MODEM
            and item.predicate == "behavior"
        )
        claim = canon.adapt_legacy_canon_fact(fact)

        self.assertEqual(
            canon.HYBRID_CANON_CLAIM_CONTRACT_VERSION,
            "hybrid_canon_claim_v1",
        )
        self.assertEqual(claim.canon_status, canon.CanonStatus.LEGACY)
        self.assertEqual(claim.domain, canon.CanonDomain.HYBRID)
        self.assertEqual(
            claim.claim_kind,
            canon.ClaimKind.BEHAVIOR_PATTERN,
        )
        self.assertEqual(
            claim.lifecycle,
            canon.ClaimLifecycle.ESTABLISHED,
        )
        self.assertEqual(claim.source_system, "legacy_canon_registry")
        self.assertTrue(claim.claim_id.startswith("clm_"))
        self.assertTrue(claim.revision_id.startswith("rev_"))
        self.assertEqual(claim.root_ids, claim.source_refs)

    def test_legacy_claim_ids_are_stable_and_revisions_are_content_bound(self):
        original = canon.CanonFact(
            canon.CLIFF,
            "primary_identity",
            "A quiet signal witness.",
        )
        replacement = canon.CanonFact(
            canon.CLIFF,
            "primary_identity",
            "A quiet signal witness with a new revision.",
        )

        first = canon.adapt_legacy_canon_fact(original)
        replay = canon.adapt_legacy_canon_fact(original)
        changed = canon.adapt_legacy_canon_fact(replacement)

        self.assertEqual(first.claim_id, replay.claim_id)
        self.assertEqual(first.revision_id, replay.revision_id)
        self.assertEqual(first.claim_id, changed.claim_id)
        self.assertNotEqual(first.revision_id, changed.revision_id)

        with mock.patch.object(
            canon,
            "HYBRID_CANON_CLAIM_CONTRACT_VERSION",
            "hybrid_canon_claim_v2_test",
        ):
            upgraded = canon.adapt_legacy_canon_fact(original)
        self.assertEqual(first.claim_id, upgraded.claim_id)
        self.assertNotEqual(first.revision_id, upgraded.revision_id)

    def test_revision_id_binds_every_normalized_contract_dimension(self):
        first = canon.adapt_legacy_canon_fact(canon.CANON_FACTS[0])
        changed = replace(
            first,
            revision_id="",
            visibility=canon.Visibility.INTERNAL,
            eligible_routes=(),
        )
        changed = canon._with_final_revision(
            changed,
            source_revision=first.source_revision,
        )

        self.assertEqual(first.claim_id, changed.claim_id)
        self.assertNotEqual(first.revision_id, changed.revision_id)

        internal_fact = canon.CanonFact(
            canon.CLIFF,
            "private_working_note",
            "Internal fixture.",
            visibility=canon.Visibility.INTERNAL,
        )
        internal = canon.adapt_legacy_canon_fact(internal_fact)
        self.assertEqual(internal.visibility, canon.Visibility.INTERNAL)
        self.assertNotIn("public_home", internal.eligible_routes)

    def test_source_owned_claim_id_survives_subject_or_predicate_correction(self):
        base = {
            "id": 81,
            "entry_type": "notable_moment",
            "cleaned_summary": "An owner-reviewed moment.",
            "status": "active",
            "subject_id": "barcode_radio",
        }
        original = canon.adapt_broadcast_memory_claim(base).claim
        corrected = canon.adapt_broadcast_memory_claim(
            {
                **base,
                "entry_type": "technical_issue",
                "subject_id": "mac_modem",
            }
        ).claim
        self.assertEqual(original.claim_id, corrected.claim_id)
        self.assertNotEqual(original.revision_id, corrected.revision_id)

    def test_same_label_cannot_inherit_authority_across_source_systems(self):
        self.assertEqual(
            canon.map_system_source_label(
                "legacy_canon_registry",
                "approved_canon",
            ),
            canon.SourceClass.APPROVED_CANON,
        )
        self.assertEqual(
            canon.map_system_source_label(
                "website_relay",
                "approved_canon",
            ),
            canon.SourceClass.EVIDENCE_PROJECTION,
        )
        self.assertEqual(
            canon.map_system_source_label(
                "website_dossier",
                "owner_confirmed",
            ),
            canon.SourceClass.DOSSIER_PROJECTION,
        )

    def test_callem_bini_and_cache_back_are_distinct(self):
        self.assertNotIn("Call'em Bini", canon.CACHE_BACK.aliases)
        self.assertEqual(
            canon.matching_canon_entity_identities(("Call'em Bini",)),
            (canon.CALLEM_BINI,),
        )
        self.assertEqual(
            canon.matching_canon_member_identities(("Call'em Bini",)),
            (),
        )
        self.assertEqual(
            canon.matching_canon_member_identities(("Cache Back",)),
            (canon.CACHE_BACK,),
        )

    def test_account_binding_beats_display_label_and_collisions_fail_closed(self):
        binding = canon.EntityAccountBinding(
            entity_id=canon.MAC_MODEM.key,
            platform="Discord",
            account_id="4242",
            authority_receipt="binding_receipt:1",
            authority_actor="discord_user:1",
            authority_verified=True,
        )
        resolved = canon.resolve_entity_identity(
            platform="discord",
            account_id="4242",
            labels=("Cache Back",),
            bindings=(binding,),
        )
        self.assertEqual(resolved.status, "resolved")
        self.assertEqual(resolved.method, "account_binding")
        self.assertEqual(resolved.subject, canon.MAC_MODEM)

        collision = canon.resolve_entity_identity(
            platform="discord",
            account_id="4242",
            bindings=(
                binding,
                canon.EntityAccountBinding(
                    entity_id=canon.CACHE_BACK.key,
                    platform="discord",
                    account_id="4242",
                    authority_receipt="binding_receipt:2",
                    authority_actor="discord_user:1",
                    authority_verified=True,
                ),
            ),
        )
        self.assertEqual(collision.status, "ambiguous")
        self.assertEqual(collision.reason, "account_binding_collision")

        inactive = canon.resolve_entity_identity(
            platform="discord",
            account_id="4242",
            labels=("Cache Back",),
            bindings=(replace(binding, active="false"),),
        )
        self.assertEqual(inactive.status, "hint_only")
        self.assertEqual(inactive.subject, canon.CACHE_BACK)

        descriptive_only = canon.resolve_entity_identity(
            platform="discord",
            account_id="4242",
            labels=("Cache Back",),
            bindings=(
                replace(
                    binding,
                    authority_receipt="owner_confirmed",
                ),
            ),
        )
        self.assertEqual(descriptive_only.status, "hint_only")
        self.assertEqual(descriptive_only.subject, canon.CACHE_BACK)

        name_shaped_actor = canon.resolve_entity_identity(
            platform="discord",
            account_id="4242",
            labels=("Cache Back",),
            bindings=(
                replace(
                    binding,
                    authority_actor="discord_user:Jane Doe",
                ),
            ),
        )
        self.assertEqual(name_shaped_actor.status, "hint_only")
        self.assertEqual(name_shaped_actor.subject, canon.CACHE_BACK)

        name_shaped_owner_ref = canon.resolve_entity_identity(
            platform="discord",
            account_id="4242",
            labels=("Cache Back",),
            bindings=(
                replace(
                    binding,
                    authority_actor="owner_ref:first.last",
                ),
            ),
        )
        self.assertEqual(name_shaped_owner_ref.status, "hint_only")
        self.assertEqual(name_shaped_owner_ref.subject, canon.CACHE_BACK)

        empty_scope = canon.resolve_entity_identity(
            platform="",
            account_id="",
            bindings=(
                replace(binding, platform="", account_id=""),
            ),
        )
        self.assertEqual(empty_scope.status, "unresolved")

        name_shaped_account = canon.resolve_entity_identity(
            platform="discord",
            account_id="Jane Doe",
            labels=("Cache Back",),
            bindings=(
                replace(binding, account_id="Jane Doe"),
            ),
        )
        self.assertEqual(name_shaped_account.status, "hint_only")
        self.assertEqual(name_shaped_account.subject, canon.CACHE_BACK)

    def test_label_only_resolution_is_reversible_hint_not_account_binding(self):
        resolution = canon.resolve_entity_identity(
            platform="discord",
            account_id="9999",
            labels=("Sheila",),
        )
        self.assertEqual(resolution.status, "hint_only")
        self.assertEqual(resolution.subject, canon.SHEILA)
        self.assertEqual(resolution.method, "exact_label_hint")

        bound = canon.resolve_entity_identity(
            platform="discord",
            account_id="9001",
            labels=("Someone Else",),
            bindings=(
                canon.EntityAccountBinding(
                    entity_id=canon.SHEILA.key,
                    platform="DISCORD",
                    account_id="9001",
                    authority_receipt="binding_receipt:sheila",
                    authority_actor="discord_user:1",
                    authority_verified=True,
                ),
            ),
        )
        self.assertEqual(bound.status, "resolved")
        self.assertEqual(bound.subject, canon.SHEILA)
        self.assertEqual(bound.method, "account_binding")

        cliff = canon.resolve_entity_identity(
            platform="discord",
            account_id="9002",
            labels=(),
            bindings=(
                canon.EntityAccountBinding(
                    entity_id=canon.CLIFF.key,
                    platform="discord",
                    account_id="9002",
                    authority_receipt="binding_receipt:cliff",
                    authority_actor="discord_user:1",
                    authority_verified=True,
                ),
            ),
        )
        self.assertEqual(cliff.status, "resolved")
        self.assertEqual(cliff.subject, canon.CLIFF)

    def test_packet_prefers_a_fully_approved_account_binding(self):
        conn = sqlite3.connect(":memory:")
        try:
            conn.execute(
                """
                CREATE TABLE canon_entity_account_bindings(
                  guild_id INTEGER NOT NULL,
                  platform TEXT NOT NULL,
                  account_id TEXT NOT NULL,
                  entity_id TEXT NOT NULL,
                  authority_receipt TEXT NOT NULL,
                  authority_actor TEXT NOT NULL,
                  binding_version TEXT NOT NULL,
                  authority_verified INTEGER NOT NULL,
                  active INTEGER NOT NULL
                )
                """
            )
            conn.execute(
                "INSERT INTO canon_entity_account_bindings VALUES(?,?,?,?,?,?,?,?,?)",
                (
                    1,
                    "Discord",
                    "42",
                    "mac_modem",
                    "binding_receipt:42",
                    "discord_user:1",
                    "canon_entity_account_binding_v1",
                    1,
                    1,
                ),
            )
            request = packet.IntelligencePacketRequest(
                guild_id=1,
                channel_id=10,
                channel_policy="public_home",
                route_mode="normal_chat",
                conversation_surface="public_home",
                subject_user_id=42,
                subject_display_name="Cache Back",
                user_text="BNL, what am I all about?",
            )
            signal = packet._canon_identity_signal(conn, request)
            self.assertTrue(signal.recognized)
            self.assertEqual(signal.subject, canon.MAC_MODEM)
            self.assertEqual(signal.stable_row_count, 1)
            conn.execute(
                "UPDATE canon_entity_account_bindings SET active=?",
                ("false",),
            )
            invalid = packet._canon_identity_signal(conn, request)
            self.assertFalse(invalid.recognized)
            self.assertEqual(invalid.status, "invalid_account_binding")
        finally:
            conn.close()

    def test_configured_owner_binding_recognizes_6_bit_without_label_inference(self):
        conn = sqlite3.connect(":memory:")
        try:
            request = packet.IntelligencePacketRequest(
                guild_id=7,
                channel_id=10,
                channel_policy="public_home",
                route_mode="normal_chat",
                conversation_surface="public_home",
                subject_user_id=61,
                subject_display_name="Cache Back",
                user_text="BNL, what am I all about?",
            )
            signal = packet._canon_identity_signal(
                conn,
                request,
                environ={
                    "BNL_OWNER_USER_ID": "61",
                    "BNL_PRIMARY_GUILD_ID": "7",
                },
            )
            self.assertTrue(signal.recognized)
            self.assertEqual(signal.subject, canon.SIX_BIT)
            self.assertEqual(signal.stable_row_count, 1)

            wrong_guild = packet._canon_identity_signal(
                conn,
                request,
                environ={
                    "BNL_OWNER_USER_ID": "61",
                    "BNL_PRIMARY_GUILD_ID": "8",
                },
            )
            self.assertFalse(wrong_guild.recognized)
            self.assertNotEqual(wrong_guild.subject, canon.SIX_BIT)
        finally:
            conn.close()

    def test_lifecycle_binding_resolves_callem_bini_without_cache_back_merge(self):
        conn = sqlite3.connect(":memory:")
        env = {
            "BNL_OWNER_USER_ID": "61",
            "BNL_PRIMARY_GUILD_ID": "7",
            "BNL_DECLARED_CANON_AUTHORITY_SECRET": (
                "hybrid-binding-integration-secret-0001"
            ),
        }
        try:
            with mock.patch.dict(os.environ, env, clear=False):
                entity_binding.ensure_canon_entity_binding_schema(conn)
                entity_binding.bind_discord_account(
                    conn,
                    actor_user_id=61,
                    authority_nonce="hybrid-binding-0001",
                    guild_id=7,
                    account_id="4242",
                    entity_id=canon.CALL_EM_BINI.key,
                    reason="Approve Call'em Bini's Discord account binding.",
                )
                request = packet.IntelligencePacketRequest(
                    guild_id=7,
                    channel_id=10,
                    channel_policy="public_home",
                    route_mode="normal_chat",
                    conversation_surface="public_home",
                    subject_user_id=4242,
                    subject_display_name="Cache Back",
                    user_text="BNL, what am I all about?",
                )
                signal = packet._canon_identity_signal(
                    conn,
                    request,
                    environ=env,
                )
            self.assertEqual(signal.status, "bound_non_signal_identity")
            self.assertEqual(signal.subject, canon.CALL_EM_BINI)
            self.assertNotEqual(signal.subject, canon.CACHE_BACK)
            self.assertTrue(signal.evidence_digest)
        finally:
            conn.close()

    def test_typed_relationship_applies_through_binding_without_becoming_activity(self):
        conn = sqlite3.connect(":memory:")
        env = {
            "BNL_OWNER_USER_ID": "61",
            "BNL_PRIMARY_GUILD_ID": "7",
            "BNL_DECLARED_CANON_AUTHORITY_SECRET": (
                "hybrid-binding-relationship-secret-0001"
            ),
        }
        try:
            with mock.patch.dict(os.environ, env, clear=False):
                declared_canon.ensure_declared_canon_schema(conn)
                entity_binding.ensure_canon_entity_binding_schema(conn)
                bound = entity_binding.bind_discord_account(
                    conn,
                    actor_user_id=61,
                    authority_nonce="relationship-binding-0001",
                    guild_id=7,
                    account_id="4242",
                    entity_id=canon.CALL_EM_BINI.key,
                    reason="Approve the same-platform entity binding.",
                ).revision
                declared_canon.add_declared_canon(
                    conn,
                    actor_user_id=61,
                    authority_nonce="relationship-declare-0001",
                    guild_id=7,
                    subject_type="entity",
                    subject_id=canon.CACHE_BACK.key,
                    object_subject_type="entity",
                    object_subject_id=canon.CALL_EM_BINI.key,
                    predicate="test_typed_relationship",
                    value={"description": "Approved test relationship."},
                    raw_declaration="Approve the test relationship.",
                    cleaned_summary="Approved test relationship.",
                    domain="lore",
                    claim_kind="relationship",
                    visibility="public_safe",
                    eligible_routes=("public_home",),
                    valid_from="2026-08-01T00:00:00+00:00",
                )
                request = packet.IntelligencePacketRequest(
                    guild_id=7,
                    channel_id=10,
                    channel_policy="public_home",
                    route_mode="normal_chat",
                    conversation_surface="public_home",
                    subject_user_id=4242,
                    subject_display_name="Call'em Bini",
                    user_text=(
                        "Who am I in BARCODE, and what is my relationship "
                        "to Cache Back?"
                    ),
                    declared_canon_authorized=True,
                    broad_profile_intent=True,
                    now="2026-08-08T12:00:00+00:00",
                )
                diagnostics = packet.IntelligencePacketDiagnostics()
                exclusions = []
                items = packet._declared_items(
                    conn,
                    request,
                    diagnostics,
                    exclusions,
                    broad=True,
                    request_terms={"cache", "back", "relationship"},
                    environ=env,
                )
                self.assertEqual(len(items), 1)
                item = items[0]
                self.assertEqual(item.lane, "canon")
                self.assertEqual(
                    item.source_type,
                    "recognized_declared_canon_claim",
                )
                self.assertEqual(item.subject_key, "discord_user:4242")
                self.assertFalse(item.point_identity)
                self.assertIn("Call'em Bini", item.text)
                self.assertIn("Cache Back", item.text)
                self.assertNotIn("object_subject_id", item.text)

                built = packet.UnifiedIntelligencePacket(
                    schema_version=packet.SCHEMA_VERSION,
                    packet_id="test-packet",
                    request=request,
                    items=(item,),
                    exclusions=(),
                    diagnostics=diagnostics,
                    validation_items=(item,),
                )
                self.assertEqual(
                    packet._declared_version(
                        conn,
                        built,
                        item,
                        environ=env,
                    ),
                    item.source_digest,
                )
                entity_binding.retire_discord_account_binding(
                    conn,
                    actor_user_id=61,
                    authority_nonce="relationship-binding-retire-0001",
                    guild_id=7,
                    binding_id=bound.binding_id,
                    expected_revision_id=bound.binding_revision_id,
                    reason="Retire the test binding.",
                )
                self.assertEqual(
                    packet._declared_version(
                        conn,
                        built,
                        item,
                        environ=env,
                    ),
                    "",
                )
        finally:
            conn.close()

    def test_broadcast_adapter_preserves_legacy_types_as_review_only(self):
        legacy = canon.adapt_broadcast_memory_claim(
            {
                "id": 9,
                "entry_type": "continuity_backreference",
                "raw_note": "A prior episode still matters.",
                "status": "active",
                "public_safe": 1,
            },
            owner_authorized=True,
            authority_actor="discord_user:1",
            authority_receipt="owner_command:9",
        )
        self.assertIsNotNone(legacy.claim)
        self.assertEqual(legacy.reason, "legacy_type_review_only")
        self.assertEqual(
            legacy.claim.lifecycle,
            canon.ClaimLifecycle.REVIEW_ONLY,
        )
        self.assertEqual(
            legacy.claim.predicate,
            "continuity_backreference",
        )
        self.assertEqual(legacy.claim.visibility, canon.Visibility.INTERNAL)
        self.assertEqual(legacy.claim.eligible_routes, ("broadcast_memory",))

    def test_raw_broadcast_adapter_ignores_legacy_caller_authority(self):
        row = {
            "id": 4,
            "entry_type": "notable_moment",
            "raw_note": "Private operator wording.",
            "cleaned_summary": "The signal held through the final track.",
            "status": "active",
            "public_safe": 1,
            "usage_scope": "ambient,direct",
            "subject_id": "barcode_radio",
            "updated_at": "2026-08-01T00:00:00+00:00",
        }
        review = canon.adapt_broadcast_memory_claim(row)
        declared = canon.adapt_broadcast_memory_claim(
            row,
            owner_authorized=True,
            authority_actor="discord_user:1",
            authority_receipt="owner_command:4",
        )
        self.assertEqual(
            review.claim.lifecycle,
            canon.ClaimLifecycle.REVIEW_ONLY,
        )
        self.assertEqual(
            review.claim.canon_status,
            canon.CanonStatus.OPEN_SIGNAL,
        )
        self.assertEqual(review.claim.visibility, canon.Visibility.INTERNAL)
        self.assertNotIn("public_home", review.claim.eligible_routes)
        self.assertEqual(declared.claim.lifecycle, canon.ClaimLifecycle.REVIEW_ONLY)
        self.assertFalse(declared.live_eligible)
        self.assertEqual(declared.reason, "broadcast_source_review_only")
        self.assertEqual(
            declared.claim.canon_status,
            canon.CanonStatus.OPEN_SIGNAL,
        )
        self.assertEqual(
            declared.claim.domain,
            canon.CanonDomain.BROADCAST_HISTORY,
        )
        self.assertEqual(declared.claim.visibility, canon.Visibility.INTERNAL)
        self.assertEqual(declared.claim.authority_actor, "")
        self.assertEqual(declared.claim.authority_receipt, "")
        self.assertFalse(declared.live_eligible)

    def test_broadcast_adapter_fails_closed_on_scope_and_boolean_strings(self):
        cases = (
            ({"public_safe": "false", "usage_scope": "direct"},),
            ({"public_safe": "1", "usage_scope": "internal"},),
        )
        for (overrides,) in cases:
            with self.subTest(overrides=overrides):
                result = canon.adapt_broadcast_memory_claim(
                    {
                        "id": 12,
                        "entry_type": "notable_moment",
                        "raw_note": "Private operator wording.",
                        "cleaned_summary": "Safe summary.",
                        "status": "active",
                        **overrides,
                    }
                )
                self.assertEqual(result.claim.visibility, canon.Visibility.INTERNAL)
                self.assertEqual(
                    result.claim.eligible_routes,
                    ("broadcast_memory",),
                )

        raw_only = canon.adapt_broadcast_memory_claim(
            {
                "id": 13,
                "entry_type": "notable_moment",
                "raw_note": "Private operator wording.",
                "status": "active",
                "public_safe": "true",
                "usage_scope": "direct",
            }
        )
        self.assertEqual(raw_only.claim.visibility, canon.Visibility.INTERNAL)
        self.assertNotIn("public_home", raw_only.claim.eligible_routes)

        forged = canon.adapt_broadcast_memory_claim(
            {
                "id": 14,
                "entry_type": "notable_moment",
                "cleaned_summary": "A safe summary.",
                "status": "active",
            },
            owner_authorized=True,
            authority_actor="Personal Human Name",
            authority_receipt="owner_confirmed",
        )
        self.assertEqual(forged.reason, "broadcast_source_review_only")
        self.assertEqual(forged.claim.authority_actor, "")
        self.assertEqual(forged.claim.authority_receipt, "")
        self.assertEqual(forged.claim.confidence, canon.Confidence.LOW)

        mixed_internal = canon.adapt_broadcast_memory_claim(
            {
                "id": 15,
                "entry_type": "moderation_context",
                "cleaned_summary": "A moderation fixture.",
                "status": "active",
                "public_safe": True,
                "usage_scope": "internal,direct",
            }
        )
        self.assertEqual(
            mixed_internal.claim.visibility,
            canon.Visibility.INTERNAL,
        )
        self.assertNotIn("public_home", mixed_internal.claim.eligible_routes)

    def test_exact_current_sidecar_join_is_declared_shadow_and_rereads_source(self):
        with mock.patch.dict(
            os.environ,
            {"BNL_OWNER_USER_ID": "61", "BNL_PRIMARY_GUILD_ID": "7"},
        ):
            conn = sqlite3.connect(":memory:")
            try:
                self.create_pr2_broadcast_table(conn)
                row_id = self.insert_pr2_broadcast(conn)
                revision = self.classify_pr2_broadcast(conn, row_id)
                before = conn.total_changes
                result = canon.adapt_declared_canon_revision(
                    conn,
                    actor_user_id=61,
                    authority_nonce="adapt-declared-0001",
                    guild_id=7,
                    declaration_id=revision.declaration_id,
                    expected_revision_id=revision.revision_id,
                    expected_source_fingerprint=revision.source_fingerprint,
                    now="2026-08-01T02:00:00+00:00",
                )
                self.assertEqual(conn.total_changes, before)
                self.assertEqual(result.reason, "declared_shadow_current")
                self.assertFalse(result.live_eligible)
                self.assertEqual(result.claim.canon_status, canon.CanonStatus.DECLARED)
                self.assertEqual(result.claim.lifecycle, canon.ClaimLifecycle.ESTABLISHED)
                self.assertEqual(result.claim.value, "A source-owned Broadcast summary.")
                self.assertNotIn("Private operator wording", result.claim.value)
                self.assertEqual(
                    result.claim.root_ids,
                    ("broadcast_memory:%s" % row_id,),
                )
                self.assertEqual(result.claim.occurrence_ids, result.claim.root_ids)
                self.assertIn(
                    "declared_canon_revision:%s" % revision.revision_id,
                    result.claim.source_refs,
                )
                self.assertNotIn(
                    "declared_canon_revision:%s" % revision.revision_id,
                    result.claim.root_ids,
                )
                self.assertEqual(result.claim.eligible_routes, ("public_home",))
                self.assertEqual(result.claim.authority_receipt, revision.authority_receipt)
                raw = canon.adapt_broadcast_memory_claim(
                    {
                        "id": row_id,
                        "entry_type": "notable_moment",
                        "cleaned_summary": "A source-owned Broadcast summary.",
                    }
                ).claim
                duplicate_view = canon.canon_claim_inventory_diagnostics(
                    (raw, result.claim)
                )
                self.assertEqual(
                    duplicate_view["duplicateCurrentSourceClaimCount"],
                    1,
                )
                conn.execute("BEGIN")
                nested = canon.adapt_declared_canon_revision(
                    conn,
                    actor_user_id=61,
                    authority_nonce="adapt-caller-snapshot",
                    guild_id=7,
                    declaration_id=revision.declaration_id,
                    expected_revision_id=revision.revision_id,
                    expected_source_fingerprint=revision.source_fingerprint,
                    now="2026-08-01T02:00:00+00:00",
                )
                self.assertEqual(nested.reason, "declared_shadow_current")
                self.assertTrue(conn.in_transaction)
                conn.rollback()
                latest = self.classify_pr2_broadcast(
                    conn,
                    row_id,
                    nonce="classify-contract-latest",
                    expected_revision_id=revision.revision_id,
                )
                old = canon.adapt_declared_canon_revision(
                    conn,
                    actor_user_id=61,
                    authority_nonce="adapt-declared-old1",
                    guild_id=7,
                    declaration_id=revision.declaration_id,
                    expected_revision_id=revision.revision_id,
                    expected_source_fingerprint=revision.source_fingerprint,
                    now="2026-08-01T02:00:00+00:00",
                )
                self.assertIsNone(old.claim)
                self.assertEqual(old.reason, "declared_expected_revision_mismatch")
                self.assertNotEqual(latest.revision_id, revision.revision_id)
            finally:
                conn.close()

    def test_pr5_declared_packet_read_requires_effective_capability(self):
        with mock.patch.dict(
            os.environ,
            {"BNL_OWNER_USER_ID": "61", "BNL_PRIMARY_GUILD_ID": "7"},
        ):
            conn = sqlite3.connect(":memory:")
            try:
                self.create_pr2_broadcast_table(conn)
                row_id = self.insert_pr2_broadcast(conn)
                revision = self.classify_pr2_broadcast(
                    conn,
                    row_id,
                    eligible_routes=(
                        "sealed_test",
                        "public_home",
                        "public_context",
                    ),
                )
                disabled = canon.select_declared_canon_claims_for_packet(
                    conn,
                    guild_id=7,
                    route_mode="normal_chat",
                    channel_policy="public_home",
                    capability_authorized=False,
                    now="2026-08-01T02:00:00+00:00",
                )
                self.assertEqual(disabled.reason, "capability_disabled")
                self.assertEqual(disabled.claims, ())

                before = conn.total_changes
                for channel_policy in (
                    "sealed_test",
                    "public_home",
                    "public_context",
                ):
                    with self.subTest(channel_policy=channel_policy):
                        selected = canon.select_declared_canon_claims_for_packet(
                            conn,
                            guild_id=7,
                            route_mode="normal_chat",
                            channel_policy=channel_policy,
                            capability_authorized=True,
                            now="2026-08-01T02:00:00+00:00",
                        )
                        self.assertEqual(conn.total_changes, before)
                        self.assertEqual(selected.reason, "eligible")
                        self.assertEqual(len(selected.claims), 1)
                        self.assertEqual(
                            selected.claims[0].source_revision,
                            revision.revision_id,
                        )
                        self.assertEqual(
                            selected.claims[0].value,
                            "A source-owned Broadcast summary.",
                        )
            finally:
                conn.close()

    def test_declared_join_rejects_stale_source_and_duplicate_authority(self):
        with mock.patch.dict(
            os.environ,
            {"BNL_OWNER_USER_ID": "61", "BNL_PRIMARY_GUILD_ID": "7"},
        ):
            conn = sqlite3.connect(":memory:")
            try:
                self.create_pr2_broadcast_table(conn)
                row_id = self.insert_pr2_broadcast(conn)
                revision = self.classify_pr2_broadcast(conn, row_id)
                conn.execute(
                    """
                    INSERT INTO declared_canon_revisions(%s)
                    SELECT %s FROM declared_canon_revisions
                    WHERE revision_id=?
                    """ % (
                        ",".join(declared_canon._REVISION_COLUMNS),
                        ",".join(
                            "?" if column in {"revision_id", "declaration_id"}
                            else column
                            for column in declared_canon._REVISION_COLUMNS
                        ),
                    ),
                    (
                        "dcr_" + "a" * 40,
                        "dcl_" + "b" * 32,
                        revision.revision_id,
                    ),
                )
                conn.commit()
                duplicate = canon.adapt_declared_canon_revision(
                    conn,
                    actor_user_id=61,
                    authority_nonce="adapt-duplicate-0001",
                    guild_id=7,
                    declaration_id=revision.declaration_id,
                    expected_revision_id=revision.revision_id,
                    expected_source_fingerprint=revision.source_fingerprint,
                    now="2026-08-01T02:00:00+00:00",
                )
                self.assertIsNone(duplicate.claim)
                self.assertEqual(
                    duplicate.reason,
                    "declared_broadcast_duplicate_authority",
                )
            finally:
                conn.close()

            conn = sqlite3.connect(":memory:")
            try:
                self.create_pr2_broadcast_table(conn)
                row_id = self.insert_pr2_broadcast(conn)
                revision = self.classify_pr2_broadcast(conn, row_id)
                conn.execute(
                    """
                    UPDATE broadcast_memory
                    SET cleaned_summary=?,public_safe=0,usage_scope='internal',
                        updated_at='2026-08-01T03:00:00+00:00'
                    WHERE id=?
                    """,
                    ("Changed after classification.", row_id),
                )
                conn.commit()
                stale = canon.adapt_declared_canon_revision(
                    conn,
                    actor_user_id=61,
                    authority_nonce="adapt-stale-0001",
                    guild_id=7,
                    declaration_id=revision.declaration_id,
                    expected_revision_id=revision.revision_id,
                    expected_source_fingerprint=revision.source_fingerprint,
                    now="2026-08-01T04:00:00+00:00",
                )
                self.assertIsNone(stale.claim)
                self.assertEqual(
                    stale.reason,
                    "declared_expected_source_fingerprint_mismatch",
                )
            finally:
                conn.close()

            conn = sqlite3.connect(":memory:")
            try:
                self.create_pr2_broadcast_table(conn)
                row_id = self.insert_pr2_broadcast(
                    conn,
                    public_safe=0,
                    usage_scope="internal,unreviewed_scope",
                )
                revision = self.classify_pr2_broadcast(
                    conn,
                    row_id,
                    nonce="classify-scope-0001",
                    visibility="internal",
                    eligible_routes=("broadcast_memory",),
                )
                invalid_scope = canon.adapt_declared_canon_revision(
                    conn,
                    actor_user_id=61,
                    authority_nonce="adapt-scope-0001",
                    guild_id=7,
                    declaration_id=revision.declaration_id,
                    expected_revision_id=revision.revision_id,
                    expected_source_fingerprint=revision.source_fingerprint,
                    now="2026-08-01T02:00:00+00:00",
                )
                self.assertIsNone(invalid_scope.claim)
                self.assertEqual(
                    invalid_scope.reason,
                    "declared_broadcast_scope_unrecognized",
                )
            finally:
                conn.close()

    def test_general_declared_adapter_uses_latest_owner_revision_only(self):
        with mock.patch.dict(
            os.environ,
            {"BNL_OWNER_USER_ID": "61", "BNL_PRIMARY_GUILD_ID": "7"},
        ):
            conn = sqlite3.connect(":memory:")
            try:
                declared_canon.ensure_declared_canon_schema(conn)
                revision = declared_canon.add_declared_canon(
                    conn,
                    actor_user_id=61,
                    authority_nonce="general-declared-0001",
                    guild_id=7,
                    subject_type="project",
                    subject_id="barcode_radio",
                    predicate="official_role",
                    value={"role": "weekly broadcast"},
                    raw_declaration="BARCODE Radio is the weekly broadcast.",
                    cleaned_summary="The weekly BARCODE broadcast.",
                    domain="broadcast_history",
                    claim_kind="role",
                    visibility="internal",
                    eligible_routes=("declared_canon_review",),
                    now="2026-08-01T01:00:00+00:00",
                ).primary
                before = conn.total_changes
                result = canon.adapt_declared_canon_revision(
                    conn,
                    actor_user_id=61,
                    authority_nonce="adapt-general-0001",
                    guild_id=7,
                    declaration_id=revision.declaration_id,
                    expected_revision_id=revision.revision_id,
                    expected_source_fingerprint=revision.source_fingerprint,
                    now="2026-08-01T02:00:00+00:00",
                )
                self.assertEqual(conn.total_changes, before)
                self.assertEqual(result.reason, "declared_shadow_current")
                self.assertEqual(result.claim.value, {"role": "weekly broadcast"})
                self.assertEqual(result.claim.canon_status, canon.CanonStatus.DECLARED)
                self.assertEqual(result.claim.root_ids, ("declared_canon:%s" % revision.declaration_id,))
                self.assertFalse(result.live_eligible)
            finally:
                conn.close()

    def test_declared_adapter_uses_one_snapshot_across_concurrent_source_change(self):
        with mock.patch.dict(
            os.environ,
            {"BNL_OWNER_USER_ID": "61", "BNL_PRIMARY_GUILD_ID": "7"},
        ), tempfile.TemporaryDirectory() as temp_dir:
            database_path = os.path.join(temp_dir, "declared_snapshot.sqlite3")
            reader = sqlite3.connect(database_path, timeout=2)
            writer = None
            try:
                reader.execute("PRAGMA journal_mode=WAL")
                self.create_pr2_broadcast_table(reader)
                row_id = self.insert_pr2_broadcast(
                    reader,
                    cleaned="Snapshot-old Broadcast value.",
                )
                revision = self.classify_pr2_broadcast(reader, row_id)
                writer = sqlite3.connect(database_path, timeout=2)
                original_validator = (
                    declared_canon.validate_current_declared_canon_revision
                )
                interleaved = {"committed": False}

                def validate_then_change(*args, **kwargs):
                    validated = original_validator(*args, **kwargs)
                    writer.execute(
                        """
                        UPDATE broadcast_memory
                        SET cleaned_summary='Snapshot-new Broadcast value.',
                            public_safe=0,usage_scope='internal',
                            updated_at='2026-08-01T03:00:00+00:00'
                        WHERE id=?
                        """,
                        (row_id,),
                    )
                    writer.commit()
                    interleaved["committed"] = True
                    return validated

                before = reader.total_changes
                with mock.patch.object(
                    declared_canon,
                    "validate_current_declared_canon_revision",
                    side_effect=validate_then_change,
                ):
                    result = canon.adapt_declared_canon_revision(
                        reader,
                        actor_user_id=61,
                        authority_nonce="adapt-snapshot-0001",
                        guild_id=7,
                        declaration_id=revision.declaration_id,
                        expected_revision_id=revision.revision_id,
                        expected_source_fingerprint=revision.source_fingerprint,
                        now="2026-08-01T02:00:00+00:00",
                    )
                self.assertTrue(interleaved["committed"])
                self.assertEqual(reader.total_changes, before)
                self.assertFalse(reader.in_transaction)
                self.assertEqual(result.reason, "declared_shadow_current")
                self.assertEqual(result.claim.value, "Snapshot-old Broadcast value.")
                self.assertNotEqual(
                    result.claim.value,
                    writer.execute(
                        "SELECT cleaned_summary FROM broadcast_memory WHERE id=?",
                        (row_id,),
                    ).fetchone()[0],
                )
                self.assertFalse(result.live_eligible)
            finally:
                if writer is not None:
                    writer.close()
                reader.close()

    def test_inventory_uses_one_snapshot_across_concurrent_source_change(self):
        with mock.patch.dict(
            os.environ,
            {"BNL_OWNER_USER_ID": "61", "BNL_PRIMARY_GUILD_ID": "7"},
        ), tempfile.TemporaryDirectory() as temp_dir:
            database_path = os.path.join(temp_dir, "inventory_snapshot.sqlite3")
            reader = sqlite3.connect(database_path, timeout=2)
            writer = None
            try:
                reader.execute("PRAGMA journal_mode=WAL")
                self.create_pr2_broadcast_table(reader)
                row_id = self.insert_pr2_broadcast(
                    reader,
                    cleaned="Inventory-old source value.",
                )
                self.classify_pr2_broadcast(reader, row_id)
                writer = sqlite3.connect(database_path, timeout=2)
                original_inventory_join = canon._inventory_declared_canon_claims
                interleaved = {"committed": False}

                def change_then_join(*args, **kwargs):
                    writer.execute(
                        """
                        UPDATE broadcast_memory
                        SET cleaned_summary='Inventory-new source value.',
                            public_safe=0,usage_scope='internal',
                            updated_at='2026-08-01T03:00:00+00:00'
                        WHERE id=?
                        """,
                        (row_id,),
                    )
                    writer.commit()
                    interleaved["committed"] = True
                    return original_inventory_join(*args, **kwargs)

                before = reader.total_changes
                with mock.patch.object(
                    canon,
                    "_inventory_declared_canon_claims",
                    side_effect=change_then_join,
                ):
                    inventory = canon.build_claim_contract_inventory(
                        reader,
                        guild_id=7,
                        now="2026-08-01T02:00:00+00:00",
                    )
                self.assertTrue(interleaved["committed"])
                self.assertEqual(reader.total_changes, before)
                self.assertFalse(reader.in_transaction)
                self.assertEqual(inventory["mutationCount"], 0)
                self.assertEqual(inventory["broadcastDeclaredCurrentCount"], 1)
                self.assertEqual(inventory["broadcastOpenReviewCount"], 0)
                self.assertEqual(inventory["broadcastStaleSidecarCount"], 0)
                self.assertNotIn("Inventory-old source value", repr(inventory))
                self.assertNotIn("Inventory-new source value", repr(inventory))
            finally:
                if writer is not None:
                    writer.close()
                reader.close()

    def test_declared_subject_type_distinguishes_same_id_across_entity_kinds(self):
        with mock.patch.dict(
            os.environ,
            {"BNL_OWNER_USER_ID": "61", "BNL_PRIMARY_GUILD_ID": "7"},
        ):
            conn = sqlite3.connect(":memory:")
            try:
                declared_canon.ensure_declared_canon_schema(conn)
                claims = []
                for subject_type, nonce in (
                    ("person", "typed-person-0001"),
                    ("project", "typed-project-0001"),
                ):
                    revision = declared_canon.add_declared_canon(
                        conn,
                        actor_user_id=61,
                        authority_nonce=nonce,
                        guild_id=7,
                        subject_type=subject_type,
                        subject_id="shared_signal_id",
                        predicate="official_role",
                        value={"role": "fixture"},
                        raw_declaration="Typed subject fixture.",
                        cleaned_summary="Typed subject fixture.",
                        domain="real_community",
                        claim_kind="role",
                        visibility="internal",
                        eligible_routes=("declared_canon_review",),
                        now="2026-08-01T01:00:00+00:00",
                    ).primary
                    adapted = canon.adapt_declared_canon_revision(
                        conn,
                        actor_user_id=61,
                        authority_nonce="adapt-%s" % nonce,
                        guild_id=7,
                        declaration_id=revision.declaration_id,
                        expected_revision_id=revision.revision_id,
                        expected_source_fingerprint=revision.source_fingerprint,
                        now="2026-08-01T02:00:00+00:00",
                    )
                    self.assertFalse(adapted.live_eligible)
                    claims.append(adapted.claim)
                self.assertEqual(
                    tuple(claim.subject_type for claim in claims),
                    ("person", "project"),
                )
                self.assertEqual(claims[0].subject_id, claims[1].subject_id)
                self.assertNotEqual(claims[0].claim_id, claims[1].claim_id)
                retyped = canon._with_final_revision(
                    replace(
                        claims[0],
                        revision_id="",
                        subject_type="project",
                    ),
                    source_revision=claims[0].source_revision,
                )
                self.assertNotEqual(retyped.revision_id, claims[0].revision_id)
                diagnostics = canon.canon_claim_inventory_diagnostics(claims)
                self.assertEqual(diagnostics["declaredTypedSubjectCount"], 2)
                self.assertEqual(diagnostics["declaredUntypedSubjectCount"], 0)
                self.assertEqual(diagnostics["declaredSubjectIdMultiTypeCount"], 1)
            finally:
                conn.close()

    def test_declared_relationship_preserves_typed_object_endpoint(self):
        with mock.patch.dict(
            os.environ,
            {"BNL_OWNER_USER_ID": "61", "BNL_PRIMARY_GUILD_ID": "7"},
        ):
            conn = sqlite3.connect(":memory:")
            try:
                declared_canon.ensure_declared_canon_schema(conn)
                revision = declared_canon.add_declared_canon(
                    conn,
                    actor_user_id=61,
                    authority_nonce="typed-relation-0001",
                    guild_id=7,
                    subject_type="person",
                    subject_id="six_bit",
                    object_subject_type="project",
                    object_subject_id="barcode_radio",
                    predicate="broadcasts_for",
                    value={"role": "host"},
                    raw_declaration="6 Bit hosts BARCODE Radio.",
                    cleaned_summary="6 Bit hosts BARCODE Radio.",
                    domain="real_community",
                    claim_kind="relationship",
                    visibility="internal",
                    eligible_routes=("declared_canon_review",),
                    now="2026-08-01T01:00:00+00:00",
                ).primary
                result = canon.adapt_declared_canon_revision(
                    conn,
                    actor_user_id=61,
                    authority_nonce="adapt-relation-0001",
                    guild_id=7,
                    declaration_id=revision.declaration_id,
                    expected_revision_id=revision.revision_id,
                    expected_source_fingerprint=revision.source_fingerprint,
                    now="2026-08-01T02:00:00+00:00",
                )
                self.assertEqual(result.claim.subject_type, "person")
                self.assertEqual(result.claim.subject_id, "six_bit")
                self.assertEqual(result.claim.object_subject_type, "project")
                self.assertEqual(result.claim.object_subject_id, "barcode_radio")
                self.assertEqual(
                    result.claim.value["object_subject_id"],
                    "barcode_radio",
                )
                self.assertFalse(result.live_eligible)
                diagnostics = canon.canon_claim_inventory_diagnostics(
                    (result.claim,)
                )
                self.assertEqual(
                    diagnostics["declaredRelationshipEndpointMissingCount"],
                    0,
                )
                malformed = replace(
                    result.claim,
                    object_subject_type="",
                    object_subject_id="",
                )
                malformed_diagnostics = canon.canon_claim_inventory_diagnostics(
                    (malformed,)
                )
                self.assertEqual(
                    malformed_diagnostics[
                        "declaredRelationshipEndpointMissingCount"
                    ],
                    1,
                )
            finally:
                conn.close()

    def test_living_adapter_rejects_heterogeneous_or_unestablished_rows(self):
        for candidate_type in (
            "person_role_fact",
            "milestone",
            "open_loop_or_question",
            "inference_or_contested_claim",
        ):
            with self.subTest(candidate_type=candidate_type):
                heterogeneous = canon.adapt_living_atomic_claim(
                    {
                        "candidate_id": "%s-candidate" % candidate_type,
                        "candidate_type": candidate_type,
                        "candidate_state": "established",
                        "subject_key": "discord_user:7",
                        "predicate_key": "heterogeneous",
                        "meaning": "A non-topic claim.",
                        "root_ids": ("root-1", "root-2"),
                    }
                )
                self.assertEqual(
                    heterogeneous.reason,
                    "living_claim_kind_ineligible",
                )
        provisional = canon.adapt_living_atomic_claim(
            {
                "candidate_id": "provisional-candidate",
                "candidate_type": "topic_or_motif",
                "candidate_state": "provisional",
                "subject_key": "discord_user:7",
                "predicate_key": "topic",
                "meaning": "A motif.",
                "root_ids": ("root-1", "root-2"),
            }
        )
        self.assertEqual(
            provisional.reason,
            "living_lifecycle_ineligible",
        )
        contested = canon.adapt_living_atomic_claim(
            {
                "candidate_id": "contested-candidate",
                "candidate_type": "topic_or_motif",
                "candidate_state": "contested",
                "subject_key": "discord_user:7",
                "predicate_key": "topic",
                "meaning": "A contested motif.",
                "root_ids": ("root-1", "root-2"),
            }
        )
        self.assertEqual(contested.reason, "living_lifecycle_ineligible")

    def test_established_atomic_or_finalized_moment_needs_recurrence_proof(self):
        established = canon.adapt_living_atomic_claim(
            {
                "candidate_id": "candidate-1",
                "candidate_type": "topic_or_motif",
                "candidate_state": "established",
                "subject_key": "discord_user:7",
                "predicate_key": "topic",
                "meaning": "A motif.",
                "root_ids": ("root-1", "root-2"),
            }
        )
        moment = canon.adapt_living_moment_claim(
            {
                "moment_id": "moment-1",
                "lifecycle_status": "finalized",
                "topic_key": "unseen-topic",
                "summary": "Several rows in one finalized Moment.",
                "root_ids": ("root-1", "root-2", "root-3"),
                "occurrence_ids": ("moment-1",),
            }
        )
        self.assertEqual(established.reason, "living_recurrence_unverified")
        self.assertIsNotNone(established.claim)
        self.assertEqual(
            established.claim.canon_status,
            canon.CanonStatus.OPEN_SIGNAL,
        )
        self.assertEqual(
            established.claim.lifecycle,
            canon.ClaimLifecycle.REVIEW_ONLY,
        )
        self.assertEqual(moment.reason, "living_recurrence_unverified")
        self.assertIsNotNone(moment.claim)
        self.assertEqual(moment.claim.claim_kind, canon.ClaimKind.EVENT)
        self.assertEqual(moment.claim.lifecycle, canon.ClaimLifecycle.REVIEW_ONLY)

    def test_living_adapter_accepts_only_explicit_cross_occurrence_proof(self):
        row = _living_adapter_row()
        adapted = canon.adapt_living_atomic_claim(row)
        self.assertEqual(adapted.reason, "eligible_living")
        self.assertEqual(adapted.claim.canon_status, canon.CanonStatus.LIVING)
        self.assertEqual(
            adapted.claim.recurrence_contract_version,
            canon.LIVING_CANON_RECURRENCE_VERSION,
        )
        self.assertEqual(
            adapted.claim.grouping_signature_version,
            canon.LIVING_CANON_GROUPING_SIGNATURE_VERSION,
        )
        self.assertEqual(
            adapted.claim.grouping_identity,
            row["grouping_identity"],
        )
        self.assertFalse(adapted.live_eligible)

        for drift in (
            {"invalidated_reason": "root_deleted"},
            {"invalidated_at": "2026-08-01T00:00:00+00:00"},
            {"conflict_value_count": 0},
            {"conflict_value_count": 2},
            {"conflict_value_count": "1"},
            {"conflict_value_count": True},
            {"lifecycle_schema_version": "bogus"},
            {"review_status": "contested"},
            {"review_status": "current", "review_due_at": "2099-01-01"},
            {"review_due_at": "2020-01-01T00:00:00+00:00"},
            {"review_due_at": "malformed"},
            {"eligible_independent_root_count": 0},
            {"reinforcement_count": 0},
            {"lifecycle_reason": "unresolved_contradiction"},
            {"lifecycle_evaluated_at": "malformed"},
            {"live_eligible": True},
        ):
            hostile = canon.adapt_living_atomic_claim({**row, **drift})
            self.assertEqual(hostile.reason, "living_lifecycle_unverified")
            self.assertEqual(
                hostile.claim.lifecycle,
                canon.ClaimLifecycle.REVIEW_ONLY,
            )
        missing_review_due = dict(row)
        missing_review_due.pop("review_due_at")
        self.assertEqual(
            canon.adapt_living_atomic_claim(missing_review_due).reason,
            "living_lifecycle_unverified",
        )
        for missing_field in (
            "eligible_independent_root_count",
            "reinforcement_count",
            "lifecycle_reason",
            "lifecycle_evaluated_at",
        ):
            missing = dict(row)
            missing.pop(missing_field)
            self.assertEqual(
                canon.adapt_living_atomic_claim(missing).reason,
                "living_lifecycle_unverified",
                missing_field,
            )
        self.assertEqual(
            canon.adapt_living_atomic_claim(
                {**row, "meaning": "Arbitrary replacement text."}
            ).reason,
            "living_recurrence_proof_mismatch",
        )
        self.assertEqual(
            canon.adapt_living_atomic_claim(
                {**row, "claim_kind": "tradition_or_joke"}
            ).reason,
            "living_domain_ineligible",
        )

        stored_proof = json.loads(row["recurrence_proof_json"])
        stored_proof["independent_occurrence_count"] = 3
        tampered = canon.adapt_living_atomic_claim(
            {
                **row,
                "recurrence_proof_json": json.dumps(stored_proof),
            }
        )
        self.assertEqual(tampered.reason, "living_recurrence_proof_mismatch")
        self.assertEqual(
            tampered.claim.lifecycle,
            canon.ClaimLifecycle.REVIEW_ONLY,
        )

        missing_identity = canon.adapt_living_atomic_claim(
            {
                "candidate_type": "topic_or_motif",
                "candidate_state": "established",
                "subject_key": "discord_user:7",
                "predicate_key": "unseen_topic_pattern",
                "meaning": "A repeated pattern.",
                "root_ids": ("root-1", "root-2"),
                "occurrence_ids": ("exchange-1", "exchange-2"),
                "recurrence_contract_version": "living_canon_recurrence_v1",
                "domain": "real_community",
                "claim_kind": "behavior_pattern",
                "candidate_eligible": True,
                "source_eligible": True,
                "roots_valid": True,
                "occurrence_bounded": True,
                "correction_fence_clear": True,
                "contradiction_clear": True,
                "independent_root_count": 2,
                "independent_occurrence_count": 2,
            }
        )
        self.assertIsNone(missing_identity.claim)
        self.assertEqual(
            missing_identity.reason,
            "living_source_identity_missing",
        )

    def test_living_adapter_rejects_forged_or_stale_recurrence_proof(self):
        row = _living_adapter_row()
        proof = json.loads(row["recurrence_proof_json"])

        self_asserted = dict(row)
        self_asserted.pop("recurrence_proof_json")
        self_asserted.update(proof)

        string_boolean_proof = dict(proof)
        string_boolean_proof["source_eligible"] = "true"

        wrong_grouping = "f" * 64
        wrong_grouping_proof = dict(proof)
        wrong_grouping_proof["grouping_identity"] = wrong_grouping

        wrong_root_digest = "e" * 64
        wrong_root_proof = dict(proof)
        wrong_root_proof["root_digest"] = wrong_root_digest

        cases = (
            (
                "missing stored proof",
                self_asserted,
                "living_recurrence_unverified",
            ),
            (
                "string proof boolean",
                {
                    **row,
                    "recurrence_proof_json": json.dumps(
                        string_boolean_proof,
                        sort_keys=True,
                    ),
                },
                "living_recurrence_unverified",
            ),
            (
                "wrong deterministic grouping",
                {
                    **row,
                    "grouping_identity": wrong_grouping,
                    "recurrence_proof_json": json.dumps(
                        wrong_grouping_proof,
                        sort_keys=True,
                    ),
                },
                "living_grouping_unverified",
            ),
            (
                "wrong deterministic root digest",
                {
                    **row,
                    "root_digest": wrong_root_digest,
                    "recurrence_proof_json": json.dumps(
                        wrong_root_proof,
                        sort_keys=True,
                    ),
                },
                "living_recurrence_proof_mismatch",
            ),
            (
                "same-count mutated roots",
                {
                    **row,
                    "root_ids": ("root-3", "root-4"),
                },
                "living_recurrence_proof_mismatch",
            ),
            (
                "same-count mutated occurrences",
                {
                    **row,
                    "occurrence_ids": ("exchange-3", "exchange-4"),
                },
                "living_recurrence_proof_mismatch",
            ),
            (
                "non-string occurrence identity",
                {
                    **row,
                    "occurrence_ids": ("exchange-1", 2),
                },
                "living_recurrence_proof_mismatch",
            ),
        )
        for label, candidate, expected_reason in cases:
            with self.subTest(label=label):
                result = canon.adapt_living_atomic_claim(candidate)
                self.assertEqual(result.reason, expected_reason)
                self.assertIsNotNone(result.claim)
                self.assertEqual(
                    result.claim.lifecycle,
                    canon.ClaimLifecycle.REVIEW_ONLY,
                )

    def test_atomic_inventory_requires_stored_grouping_and_recurrence_proof(self):
        conn = sqlite3.connect(":memory:")
        try:
            conn.executescript(
                """
                CREATE TABLE memory_ledger_knowledge_candidates (
                  candidate_id TEXT PRIMARY KEY,
                  guild_id INTEGER NOT NULL,
                  candidate_type TEXT NOT NULL,
                  candidate_state TEXT NOT NULL,
                  subject_key TEXT NOT NULL,
                  predicate_key TEXT NOT NULL,
                  normalized_value TEXT NOT NULL,
                  value_digest TEXT NOT NULL,
                  visibility TEXT NOT NULL,
                  public_usable INTEGER NOT NULL DEFAULT 0,
                  candidate_eligible INTEGER NOT NULL DEFAULT 0,
                  live_eligible INTEGER NOT NULL DEFAULT 0,
                  invalidated_reason TEXT DEFAULT '',
                  invalidated_at TEXT DEFAULT '',
                  lifecycle_schema_version TEXT DEFAULT '',
                  conflict_value_count INTEGER NOT NULL DEFAULT 0,
                  review_status TEXT DEFAULT '',
                  review_due_at TEXT DEFAULT '',
                  eligible_independent_root_count INTEGER NOT NULL DEFAULT 0,
                  reinforcement_count INTEGER NOT NULL DEFAULT 0,
                  lifecycle_reason TEXT DEFAULT '',
                  lifecycle_evaluated_at TEXT DEFAULT '',
                  independent_root_count INTEGER NOT NULL DEFAULT 0,
                  recurrence_contract_version TEXT DEFAULT '',
                  grouping_signature_version TEXT DEFAULT '',
                  grouping_identity TEXT DEFAULT '',
                  canon_domain TEXT DEFAULT '',
                  canon_claim_kind TEXT DEFAULT '',
                  independent_occurrence_count INTEGER NOT NULL DEFAULT 0,
                  occurrence_ids_json TEXT NOT NULL DEFAULT '[]',
                  recurrence_proof_json TEXT NOT NULL DEFAULT '{}',
                  root_digest TEXT DEFAULT '',
                  occurrence_digest TEXT DEFAULT '',
                  updated_at TEXT DEFAULT '',
                  last_seen_at TEXT DEFAULT ''
                );
                CREATE TABLE memory_ledger_knowledge_roots (
                  candidate_id TEXT NOT NULL,
                  guild_id INTEGER NOT NULL,
                  root_entry_id TEXT NOT NULL,
                  is_independent INTEGER NOT NULL DEFAULT 0,
                  PRIMARY KEY(candidate_id, root_entry_id)
                );
                """
            )
            bounds = {
                "eligible_ledger_scan_max": 1200,
                "motif_candidates_max": 6,
                "retained_roots_max": 12,
                "occurrence_lookback_max": 64,
                "idle_boundary_seconds": 1800,
            }
            subject_key = "discord_user:7"
            predicate_key = "conversation_pattern_unseen_topic"
            domain = "real_community"
            valid_roots = ("root:1", "root:2")
            valid_occurrences = ("occurrence:1", "occurrence:2")
            grouping_identity = _ledger_digest(
                canon.LIVING_CANON_GROUPING_SIGNATURE_VERSION,
                subject_key,
                predicate_key,
                domain,
            )
            root_digest = _ledger_digest(*valid_roots)
            occurrence_digest = _ledger_digest(*valid_occurrences)
            proof = {
                "recurrence_contract_version": (
                    canon.LIVING_CANON_RECURRENCE_VERSION
                ),
                "grouping_signature_version": (
                    canon.LIVING_CANON_GROUPING_SIGNATURE_VERSION
                ),
                "grouping_identity": grouping_identity,
                "candidate_state": "established",
                "candidate_eligible": True,
                "source_eligible": True,
                "roots_valid": True,
                "occurrence_bounded": True,
                "correction_fence_clear": True,
                "contradiction_clear": True,
                "independent_root_count": 2,
                "independent_occurrence_count": 2,
                "root_digest": root_digest,
                "occurrence_digest": occurrence_digest,
                "bounds": bounds,
            }
            base_values = (
                1,
                "topic_or_motif",
                "established",
                subject_key,
                predicate_key,
                "private-source-text-must-not-appear",
                _ledger_digest("private-source-text-must-not-appear"),
                "public_safe",
                1,
                1,
                2,
            )
            conn.execute(
                """
                INSERT INTO memory_ledger_knowledge_candidates(
                  candidate_id,guild_id,candidate_type,candidate_state,
                  subject_key,predicate_key,normalized_value,value_digest,visibility,
                  public_usable,candidate_eligible,independent_root_count,
                  recurrence_contract_version,grouping_signature_version,
                  grouping_identity,canon_domain,canon_claim_kind,
                  independent_occurrence_count,occurrence_ids_json,
                  recurrence_proof_json,root_digest,occurrence_digest,
                  live_eligible,lifecycle_schema_version,invalidated_reason,
                  invalidated_at,conflict_value_count,review_status,
                  review_due_at,
                  eligible_independent_root_count,reinforcement_count,
                  lifecycle_reason,lifecycle_evaluated_at,
                  updated_at,last_seen_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    "candidate-valid",
                    *base_values,
                    canon.LIVING_CANON_RECURRENCE_VERSION,
                    canon.LIVING_CANON_GROUPING_SIGNATURE_VERSION,
                    grouping_identity,
                    domain,
                    "behavior_pattern",
                    2,
                    json.dumps(valid_occurrences),
                    json.dumps(proof, sort_keys=True),
                    root_digest,
                    occurrence_digest,
                    0,
                    canon.ATOMIC_KNOWLEDGE_LIFECYCLE_SCHEMA_VERSION,
                    "",
                    "",
                    1,
                    "not_required",
                    "",
                    2,
                    2,
                    "independent_recurrence_established",
                    "2026-08-02T00:00:00+00:00",
                    "2026-08-02T00:00:00+00:00",
                    "2026-08-02T00:00:00+00:00",
                ),
            )
            conn.execute(
                """
                INSERT INTO memory_ledger_knowledge_candidates(
                  candidate_id,guild_id,candidate_type,candidate_state,
                  subject_key,predicate_key,normalized_value,value_digest,visibility,
                  public_usable,candidate_eligible,independent_root_count,
                  root_digest,updated_at,last_seen_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    "candidate-old",
                    *base_values,
                    "c" * 64,
                    "2026-08-02T00:00:00+00:00",
                    "2026-08-02T00:00:00+00:00",
                ),
            )
            for candidate_id, root_ids in (
                ("candidate-valid", ("root:1", "root:2")),
                ("candidate-old", ("root:3", "root:4")),
            ):
                conn.executemany(
                    """
                    INSERT INTO memory_ledger_knowledge_roots(
                      candidate_id,guild_id,root_entry_id,is_independent
                    ) VALUES(?,1,?,1)
                    """,
                    ((candidate_id, root_id) for root_id in root_ids),
                )
            conn.commit()

            inventory = canon.build_claim_contract_inventory(conn, guild_id=1)

            self.assertEqual(inventory["mutationCount"], 0)
            self.assertEqual(inventory["sourceRows"]["atomic_knowledge"], 2)
            self.assertEqual(inventory["statuses"]["living"], 1)
            self.assertEqual(inventory["livingRecurrenceVerifiedCount"], 1)
            self.assertEqual(inventory["livingGroupingVerifiedCount"], 1)
            self.assertEqual(inventory["reasonCounts"]["eligible_living"], 1)
            self.assertEqual(
                inventory["reasonCounts"]["living_recurrence_unverified"],
                1,
            )
            self.assertNotIn("private-source-text-must-not-appear", repr(inventory))
            self.assertNotIn("root:1", repr(inventory))
            self.assertNotIn("occurrence:1", repr(inventory))
        finally:
            conn.close()

    def test_pre_pr4_atomic_schema_stays_review_only_and_zero_write(self):
        conn = sqlite3.connect(":memory:")
        try:
            conn.executescript(
                """
                CREATE TABLE memory_ledger_knowledge_candidates (
                  candidate_id TEXT PRIMARY KEY,
                  guild_id INTEGER NOT NULL,
                  candidate_type TEXT NOT NULL,
                  candidate_state TEXT NOT NULL,
                  subject_key TEXT NOT NULL,
                  predicate_key TEXT NOT NULL,
                  normalized_value TEXT NOT NULL,
                  visibility TEXT NOT NULL,
                  candidate_eligible INTEGER NOT NULL DEFAULT 0,
                  independent_root_count INTEGER NOT NULL DEFAULT 0,
                  root_digest TEXT NOT NULL,
                  last_seen_at TEXT DEFAULT '',
                  updated_at TEXT NOT NULL
                );
                CREATE TABLE memory_ledger_knowledge_roots (
                  candidate_id TEXT NOT NULL,
                  guild_id INTEGER NOT NULL,
                  root_entry_id TEXT NOT NULL,
                  is_independent INTEGER NOT NULL DEFAULT 0,
                  PRIMARY KEY(candidate_id, root_entry_id)
                );
                """
            )
            conn.execute(
                """
                INSERT INTO memory_ledger_knowledge_candidates(
                  candidate_id,guild_id,candidate_type,candidate_state,
                  subject_key,predicate_key,normalized_value,visibility,
                  candidate_eligible,independent_root_count,root_digest,
                  last_seen_at,updated_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    "pre-pr4-candidate",
                    1,
                    "topic_or_motif",
                    "established",
                    "discord_user:7",
                    "conversation_motif_old",
                    "pre-pr4-private-text",
                    "public_safe",
                    1,
                    2,
                    _ledger_digest("pre-pr4-root-1", "pre-pr4-root-2"),
                    "2026-08-01T00:00:00+00:00",
                    "2026-08-01T00:00:00+00:00",
                ),
            )
            conn.executemany(
                """
                INSERT INTO memory_ledger_knowledge_roots(
                  candidate_id,guild_id,root_entry_id,is_independent
                ) VALUES('pre-pr4-candidate',1,?,1)
                """,
                (("pre-pr4-root-1",), ("pre-pr4-root-2",)),
            )
            conn.commit()
            before_changes = conn.total_changes

            inventory = canon.build_claim_contract_inventory(conn, guild_id=1)

            self.assertEqual(conn.total_changes, before_changes)
            self.assertEqual(inventory["mutationCount"], 0)
            self.assertEqual(inventory["sourceRows"]["atomic_knowledge"], 1)
            self.assertEqual(inventory["statuses"]["living"], 0)
            self.assertEqual(
                inventory["reasonCounts"]["living_recurrence_unverified"],
                1,
            )
            self.assertNotIn("pre-pr4-private-text", repr(inventory))
            self.assertNotIn("pre-pr4-root-1", repr(inventory))
        finally:
            conn.close()

    def test_open_signal_is_ephemeral_and_source_linked(self):
        adapted = canon.adapt_open_signal_claim(
            {
                "subject_key": "discord_user:7",
                "entry_id": "entry-7",
                "text": "I keep returning to unusual signal routing.",
                "occurrence_identity": "occurrence-7",
                "root_identity": "root-7",
                "source_digest": "digest-7",
                "point_identity": "point-7",
                "attribution_mode": "subject_action",
                "polarity": "affirmative",
                "action_identity": "return",
                "observed_at": "2026-08-01T00:00:00+00:00",
                "assessment_contract_version": "public_assessment_evidence_v3",
                "source_system": "memory_ledger_public_assessment",
                "source_role": "user",
                "source_class": "public_observation",
                "lifecycle_status": "active",
                "visibility": "public_safe",
                "channel_policy": "public_home",
                "route_mode": "normal_chat",
                "public_usable": True,
                "subject_authored": True,
                "request_relevant": True,
                "selector_eligible": True,
                "derived": False,
                "projection": False,
            }
        )
        self.assertEqual(adapted.reason, "eligible_open_signal")
        self.assertEqual(
            adapted.claim.canon_status,
            canon.CanonStatus.OPEN_SIGNAL,
        )
        self.assertEqual(
            adapted.claim.lifecycle,
            canon.ClaimLifecycle.CANDIDATE,
        )
        self.assertEqual(adapted.claim.projection_state, "ephemeral")
        self.assertEqual(
            adapted.claim.root_ids,
            ("root-7",),
        )
        self.assertEqual(adapted.claim.occurrence_ids, ("occurrence-7",))

    def test_open_signal_rejects_private_deleted_or_model_derived_rows(self):
        base = {
            "subject_key": "discord_user:7",
            "entry_id": "entry-7",
            "text": "A bounded observation.",
            "occurrence_identity": "occurrence-7",
            "root_identity": "root-7",
            "source_digest": "digest-7",
            "point_identity": "point-7",
            "attribution_mode": "subject_action",
            "polarity": "affirmative",
            "action_identity": "discuss",
            "assessment_contract_version": "public_assessment_evidence_v3",
            "source_system": "memory_ledger_public_assessment",
            "source_role": "user",
            "source_class": "public_observation",
            "lifecycle_status": "active",
            "visibility": "public_safe",
            "channel_policy": "public_home",
            "route_mode": "normal_chat",
            "public_usable": True,
            "subject_authored": True,
            "request_relevant": True,
            "selector_eligible": True,
            "derived": False,
            "projection": False,
        }
        for overrides in (
            {"visibility": "private"},
            {"lifecycle_status": "deleted"},
            {"source_role": "model", "derived": True},
            {"public_usable": "false"},
            {"subject_authored": False},
        ):
            with self.subTest(overrides=overrides):
                result = canon.adapt_open_signal_claim({**base, **overrides})
                self.assertIsNone(result.claim)
                self.assertEqual(result.reason, "open_signal_source_ineligible")

    def test_real_public_assessment_output_normalizes_directly(self):
        conn = sqlite3.connect(":memory:")
        try:
            ledger.ensure_memory_ledger_schema(conn)
            conn.execute(
                """
                CREATE TABLE conversations (
                  id INTEGER PRIMARY KEY,guild_id INTEGER,user_id INTEGER,
                  user_name TEXT,role TEXT,content TEXT,channel_id INTEGER,
                  channel_policy TEXT,route_mode TEXT NOT NULL,timestamp TEXT
                )
                """
            )
            conn.execute(
                """
                INSERT INTO conversations VALUES(701,1,7,'Crow','user',?,10,
                                                  'public_home','normal_chat',?)
                """,
                (
                    "I keep testing unusual antenna materials for the signal.",
                    "2026-08-01T00:00:00+00:00",
                ),
            )
            inserted = ledger.shadow_conversation_row(
                conn,
                row_id=701,
                user_id=7,
                user_name="Crow",
                guild_id=1,
                role="user",
                content="I keep testing unusual antenna materials for the signal.",
                channel_name="barcode-bot",
                channel_policy="public_home",
                channel_id=10,
                route_mode="normal_chat",
                observed_at="2026-08-01T00:00:00+00:00",
            )
            self.assertEqual(inserted.outcome, "inserted")
            selected = ledger.select_public_conversation_assessment_evidence(
                conn,
                guild_id=1,
                subject_key="discord_user:7",
                request_text="What am I all about?",
            )
            self.assertEqual(len(selected.items), 1)
            adapted = canon.adapt_open_signal_claim(selected.items[0])
            self.assertEqual(adapted.reason, "eligible_open_signal")
            self.assertEqual(
                adapted.claim.subject_id,
                "discord_user:7",
            )
            self.assertEqual(
                adapted.claim.canon_status,
                canon.CanonStatus.OPEN_SIGNAL,
            )
        finally:
            conn.close()

    def test_website_lore_origin_stays_review_only_and_nonidentifying(self):
        result = canon.adapt_website_lore_relationship_candidate(
            canon.LEGACY_WEBSITE_LORE_RELATIONSHIP_CANDIDATES[0]
        )
        self.assertEqual(result.reason, "website_lore_review_only")
        self.assertEqual(result.claim.lifecycle, canon.ClaimLifecycle.REVIEW_ONLY)
        self.assertEqual(result.claim.domain, canon.CanonDomain.LORE)
        self.assertEqual(result.claim.claim_kind, canon.ClaimKind.RELATIONSHIP)
        self.assertEqual(result.claim.visibility, canon.Visibility.INTERNAL)
        self.assertFalse(result.live_eligible)
        self.assertEqual(result.claim.subject_id, canon.CACHE_BACK.key)
        self.assertEqual(
            result.claim.value["object_id"],
            canon.CALL_EM_BINI.key,
        )
        self.assertEqual(
            canon.matching_canon_member_identities(("Call'em Bini",)),
            (),
        )

    def test_validity_is_orthogonal_to_status_visibility_and_lifecycle(self):
        row = {
            "id": 40,
            "entry_type": "show_state_override",
            "cleaned_summary": "A temporary show state.",
            "status": "active",
            "public_safe": False,
            "usage_scope": "internal",
            "valid_from": "2026-08-01T00:00:00+00:00",
            "valid_until": "2026-08-02T00:00:00+00:00",
        }
        claim = canon.adapt_broadcast_memory_claim(
            row,
            owner_authorized=True,
            authority_actor="discord_user:1",
            authority_receipt="owner_command:40",
        ).claim
        self.assertEqual(claim.canon_status, canon.CanonStatus.OPEN_SIGNAL)
        self.assertEqual(claim.visibility, canon.Visibility.INTERNAL)
        self.assertEqual(claim.lifecycle, canon.ClaimLifecycle.REVIEW_ONLY)
        self.assertTrue(
            canon.claim_within_validity_window(
                claim,
                at="2026-08-01T12:00:00+00:00",
            )
        )
        self.assertFalse(
            canon.claim_within_validity_window(
                claim,
                at="2026-08-03T00:00:00+00:00",
            )
        )
        self.assertEqual(claim.canon_status, canon.CanonStatus.OPEN_SIGNAL)
        self.assertEqual(claim.lifecycle, canon.ClaimLifecycle.REVIEW_ONLY)

    def test_inventory_reports_contract_and_content_free_collisions(self):
        claims = tuple(
            canon.adapt_legacy_canon_fact(fact)
            for fact in canon.CANON_FACTS
        )
        diagnostics = canon.canon_claim_inventory_diagnostics(claims)
        self.assertEqual(
            diagnostics["claimContractVersion"],
            canon.HYBRID_CANON_CLAIM_CONTRACT_VERSION,
        )
        self.assertEqual(diagnostics["claimCount"], len(claims))
        self.assertEqual(diagnostics["claimIdCollisionCount"], 0)
        self.assertEqual(diagnostics["revisionIdCollisionCount"], 0)
        self.assertEqual(diagnostics["revisionDigestMismatchCount"], 0)
        self.assertEqual(diagnostics["duplicateCurrentSourceClaimCount"], 0)
        self.assertEqual(diagnostics["duplicateRootWithinClaimCount"], 0)
        self.assertEqual(diagnostics["duplicateDeclaredRootAuthorityCount"], 0)
        self.assertEqual(diagnostics["identityBindingCollisionCount"], 0)
        self.assertEqual(diagnostics["nonOpaqueAuthorityActorCount"], 0)

        repeated = canon.canon_claim_inventory_diagnostics(
            (claims[0], claims[0])
        )
        self.assertEqual(repeated["claimIdCollisionCount"], 0)
        self.assertEqual(repeated["revisionIdCollisionCount"], 0)

        hostile_claim = replace(
            claims[0],
            authority_actor="owner_ref:first.last",
        )
        hostile = canon.canon_claim_inventory_diagnostics(
            (hostile_claim,),
            bindings=(
                canon.EntityAccountBinding(
                    entity_id=canon.MAC_MODEM.key,
                    platform="discord",
                    account_id="7",
                    authority_receipt="binding_receipt:hostile",
                    authority_actor="owner_ref:first.last",
                    authority_verified=True,
                ),
            ),
        )
        self.assertEqual(hostile["nonOpaqueAuthorityActorCount"], 1)
        self.assertEqual(hostile["nonOpaqueBindingActorCount"], 1)

    def test_database_inventory_reconciles_and_performs_zero_writes(self):
        conn = sqlite3.connect(":memory:")
        try:
            conn.execute(
                """
                CREATE TABLE broadcast_memory(
                  id INTEGER PRIMARY KEY,
                  guild_id INTEGER,
                  entry_type TEXT,
                  raw_note TEXT,
                  status TEXT,
                  public_safe INTEGER,
                  created_at TEXT
                )
                """
            )
            conn.executemany(
                "INSERT INTO broadcast_memory VALUES(?,?,?,?,?,?,?)",
                (
                    (
                        1,
                        1,
                        "notable_moment",
                        "Private fixture text must not enter diagnostics.",
                        "active",
                        1,
                        "2026-08-01T00:00:00+00:00",
                    ),
                    (
                        2,
                        1,
                        "recap",
                        "Another private fixture value.",
                        "active",
                        0,
                        "2026-08-01T01:00:00+00:00",
                    ),
                ),
            )
            conn.commit()
            before = conn.total_changes
            inventory = canon.build_claim_contract_inventory(
                conn,
                guild_id=1,
            )
            self.assertEqual(conn.total_changes, before)
            self.assertEqual(inventory["mutationCount"], 0)
            self.assertEqual(inventory["sourceRows"]["broadcast_memory"], 2)
            self.assertEqual(inventory["adaptedRows"]["broadcast_memory"], 2)
            self.assertTrue(inventory["sourceAdaptedReconciled"])
            self.assertEqual(inventory["sourceReconciliationStatus"], "complete")
            self.assertEqual(inventory["truncatedSources"], ())
            self.assertEqual(inventory["callemCacheIdentityCollisionCount"], 0)
            rendered = repr(inventory)
            self.assertNotIn("Private fixture", rendered)
            self.assertNotIn("Another private", rendered)
            conn.execute("BEGIN")
            nested = canon.build_claim_contract_inventory(conn, guild_id=1)
            self.assertEqual(nested["mutationCount"], 0)
            self.assertTrue(conn.in_transaction)
            conn.rollback()
        finally:
            conn.close()

    def test_database_inventory_ignores_temp_source_shadow(self):
        conn = sqlite3.connect(":memory:")
        try:
            source_schema = """
                CREATE {scope} TABLE broadcast_memory(
                  id INTEGER PRIMARY KEY,
                  guild_id INTEGER,
                  entry_type TEXT,
                  raw_note TEXT,
                  status TEXT,
                  public_safe INTEGER,
                  created_at TEXT
                )
            """
            conn.execute(source_schema.format(scope=""))
            conn.execute(
                "INSERT INTO main.broadcast_memory VALUES(?,?,?,?,?,?,?)",
                (
                    1,
                    1,
                    "notable_moment",
                    "Main source",
                    "active",
                    1,
                    "2026-08-01T00:00:00+00:00",
                ),
            )
            conn.execute(source_schema.format(scope="TEMP"))
            conn.executemany(
                "INSERT INTO temp.broadcast_memory VALUES(?,?,?,?,?,?,?)",
                (
                    (9, 1, "recap", "Temp one", "active", 1, "2025-01-01"),
                    (10, 1, "recap", "Temp two", "active", 1, "2025-01-02"),
                ),
            )
            conn.commit()

            inventory = canon.build_claim_contract_inventory(conn, guild_id=1)

            self.assertEqual(inventory["sourceRows"]["broadcast_memory"], 1)
            self.assertEqual(inventory["adaptedRows"]["broadcast_memory"], 1)
            self.assertNotIn("Temp one", repr(inventory))
            self.assertNotIn("Temp two", repr(inventory))
        finally:
            conn.close()

    def test_inventory_collapses_broadcast_sidecar_and_counts_old_revisions(self):
        with mock.patch.dict(
            os.environ,
            {"BNL_OWNER_USER_ID": "61", "BNL_PRIMARY_GUILD_ID": "7"},
        ):
            conn = sqlite3.connect(":memory:")
            try:
                self.create_pr2_broadcast_table(conn)
                declared_row = self.insert_pr2_broadcast(
                    conn,
                    cleaned="Declared fixture content must stay out of diagnostics.",
                )
                self.insert_pr2_broadcast(
                    conn,
                    cleaned="Open fixture content must stay out of diagnostics.",
                )
                first = self.classify_pr2_broadcast(conn, declared_row)
                second = self.classify_pr2_broadcast(
                    conn,
                    declared_row,
                    nonce="classify-contract-0002",
                    expected_revision_id=first.revision_id,
                )
                self.assertNotEqual(first.revision_id, second.revision_id)
                before = conn.total_changes
                inventory = canon.build_claim_contract_inventory(
                    conn,
                    guild_id=7,
                    now="2026-08-01T02:00:00+00:00",
                )
                self.assertEqual(conn.total_changes, before)
                self.assertEqual(inventory["mutationCount"], 0)
                self.assertEqual(inventory["adaptedRows"]["broadcast_memory"], 2)
                self.assertEqual(inventory["broadcastDeclaredCurrentCount"], 1)
                self.assertEqual(inventory["broadcastOpenReviewCount"], 1)
                self.assertEqual(inventory["broadcastStaleSidecarCount"], 0)
                self.assertEqual(inventory["duplicateCurrentSourceClaimCount"], 0)
                self.assertEqual(inventory["duplicateRootWithinClaimCount"], 0)
                self.assertEqual(inventory["duplicateDeclaredRootAuthorityCount"], 0)
                self.assertEqual(inventory["declaredHistoricalRevisionCount"], 1)
                self.assertEqual(inventory["statuses"]["declared"], 1)
                self.assertGreaterEqual(inventory["statuses"]["open_signal"], 1)
                rendered = repr(inventory)
                self.assertNotIn("Declared fixture content", rendered)
                self.assertNotIn("Open fixture content", rendered)
                self.assertNotIn(first.revision_id, rendered)
                self.assertNotIn("broadcast_memory:%s" % declared_row, rendered)
            finally:
                conn.close()

    def test_inventory_falls_back_to_one_open_claim_for_stale_or_duplicate_sidecar(self):
        with mock.patch.dict(
            os.environ,
            {"BNL_OWNER_USER_ID": "61", "BNL_PRIMARY_GUILD_ID": "7"},
        ):
            conn = sqlite3.connect(":memory:")
            try:
                self.create_pr2_broadcast_table(conn)
                row_id = self.insert_pr2_broadcast(conn)
                revision = self.classify_pr2_broadcast(conn, row_id)
                conn.execute(
                    """
                    UPDATE broadcast_memory
                    SET status='resolved',public_safe=0,usage_scope='internal',
                        updated_at='2026-08-01T03:00:00+00:00'
                    WHERE id=?
                    """,
                    (row_id,),
                )
                conn.commit()
                before = conn.total_changes
                inventory = canon.build_claim_contract_inventory(
                    conn,
                    guild_id=7,
                    now="2026-08-01T04:00:00+00:00",
                )
                self.assertEqual(conn.total_changes, before)
                self.assertEqual(inventory["broadcastDeclaredCurrentCount"], 0)
                self.assertEqual(inventory["broadcastOpenReviewCount"], 1)
                self.assertEqual(inventory["broadcastStaleSidecarCount"], 1)
                self.assertEqual(inventory["statuses"]["declared"], 0)
                self.assertGreaterEqual(inventory["statuses"]["open_signal"], 1)
                self.assertEqual(inventory["adaptedRows"]["broadcast_memory"], 1)
                self.assertNotIn(revision.revision_id, repr(inventory))
            finally:
                conn.close()

    def test_current_atomic_source_row_normalizes_as_review_only(self):
        conn = sqlite3.connect(":memory:")
        try:
            ledger.ensure_memory_ledger_schema(conn)
            roots = []
            for row_id, observed_at, text in (
                (
                    801,
                    "2026-07-24T20:00:00+00:00",
                    "I keep fixing the bot code and memory system.",
                ),
                (
                    802,
                    "2026-07-25T20:00:00+00:00",
                    "The website code needs another careful test.",
                ),
            ):
                roots.append(
                    ledger.insert_ledger_entry(
                        conn,
                        ledger.LedgerEntry(
                            guild_id=1,
                            source_table="conversations",
                            source_row_id=row_id,
                            source_revision=str(row_id),
                            source_role="user",
                            entry_type="observation",
                            subject_key="discord_user:7",
                            subject_display_name="Crow",
                            predicate_key="conversation",
                            value=text,
                            source_class=canon.SourceClass.PUBLIC_OBSERVATION,
                            route_mode="normal_chat",
                            channel_id=10,
                            channel_name="barcode-bot",
                            channel_policy="public_home",
                            visibility=canon.Visibility.PUBLIC,
                            confidence=canon.Confidence.MEDIUM,
                            public_usable=True,
                            observed_at=observed_at,
                            source_sequence=row_id,
                            participants=(
                                ledger.LedgerParticipant(
                                    "discord_user:7",
                                    "Crow",
                                    "author",
                                    0,
                                ),
                            ),
                        ),
                    ).entry_id
                )
            formed = ledger.form_atomic_candidates_from_recurring_conversation(
                conn,
                trigger_entry_id=roots[-1],
                environ={
                    ledger.MEMORY_LEDGER_SHADOW_ENV: "true",
                    ledger.CONVERSATION_MOTIF_FORMATION_ENV: "true",
                },
            )
            self.assertEqual(len(formed), 1)
            inventory = canon.build_claim_contract_inventory(conn, guild_id=1)
            self.assertEqual(inventory["sourceRows"]["atomic_knowledge"], 1)
            self.assertEqual(inventory["adaptedRows"]["atomic_knowledge"], 1)
            self.assertEqual(inventory["rejectedRows"].get("atomic_knowledge", 0), 0)
            self.assertGreaterEqual(
                inventory["reasonCounts"].get(
                    "living_recurrence_unverified",
                    0,
                ),
                1,
            )
        finally:
            conn.close()

    def test_current_v1_living_candidate_normalizes_as_verified_living(self):
        conn = sqlite3.connect(":memory:")
        try:
            ledger.ensure_memory_ledger_schema(conn)
            conn.execute(
                """
                CREATE TABLE conversations(
                  id INTEGER PRIMARY KEY,guild_id INTEGER,user_id INTEGER,
                  user_name TEXT,role TEXT,content TEXT,channel_id INTEGER,
                  channel_name TEXT,channel_policy TEXT,route_mode TEXT,
                  timestamp TEXT
                )
                """
            )
            roots = []
            for row_id, observed_at, text in (
                (
                    811,
                    "2026-07-24T20:00:00+00:00",
                    "I tune ceramic antennas with copper meshes during field experiments.",
                ),
                (
                    812,
                    "2026-07-25T20:00:00+00:00",
                    "We tune the ceramic antenna with a copper mesh during the field experiment.",
                ),
            ):
                conn.execute(
                    "INSERT INTO conversations VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        row_id, 1, 7, "Test Member", "user", text, 10,
                        "barcode-bot", "public_home", "normal_chat",
                        observed_at,
                    ),
                )
                roots.append(
                    ledger.shadow_conversation_row(
                        conn,
                        row_id=row_id,
                        user_id=7,
                        user_name="Test Member",
                        guild_id=1,
                        role="user",
                        content=text,
                        channel_name="barcode-bot",
                        channel_policy="public_home",
                        channel_id=10,
                        route_mode="normal_chat",
                        observed_at=observed_at,
                        source_sequence=row_id,
                        environ={ledger.MEMORY_LEDGER_SHADOW_ENV: "true"},
                    ).entry_id
                )
            formed = ledger.form_atomic_candidates_from_recurring_conversation(
                conn,
                trigger_entry_id=roots[-1],
                environ={
                    ledger.MEMORY_LEDGER_SHADOW_ENV: "true",
                    ledger.LIVING_CANON_V1_FORMATION_ENV: "true",
                },
            )
            self.assertEqual(len(formed), 1)
            inventory = canon.build_claim_contract_inventory(conn, guild_id=1)
            self.assertEqual(inventory["statuses"]["living"], 1)
            self.assertEqual(inventory["reasonCounts"]["eligible_living"], 1)
            self.assertEqual(inventory["livingRecurrenceVerifiedCount"], 1)
            self.assertEqual(inventory["livingGroupingVerifiedCount"], 1)
        finally:
            conn.close()

    def test_current_finalized_moment_normalizes_as_review_only(self):
        conn = sqlite3.connect(":memory:")
        try:
            ledger.ensure_memory_ledger_schema(conn)
            moments.ensure_moment_schema(conn)
            with mock.patch.dict(
                "os.environ",
                {
                    ledger.MEMORY_LEDGER_SHADOW_ENV: "true",
                    moments.MOMENT_ENGINE_SHADOW_ENV: "true",
                },
            ):
                for row_id, user_id, name, text in (
                    (901, 7, "Crow", "The synth patch should open the chorus."),
                    (902, 8, "Moth", "The drums can answer that synth chorus."),
                    (903, 7, "Crow", "The modular patch and drums resolve together."),
                ):
                    written = ledger.shadow_conversation_row(
                        conn,
                        row_id=row_id,
                        user_id=user_id,
                        user_name=name,
                        guild_id=1,
                        role="user",
                        content=text,
                        channel_name="barcode-bot",
                        channel_policy="public_home",
                        channel_id=10,
                        route_mode="normal_chat",
                        observed_at=(
                            "2026-08-01T00:%02d:00+00:00" % (row_id - 901)
                        ),
                    )
                    moments.observe_ledger_entry(conn, written.entry_id)
                moments.sweep_expired_windows(
                    conn,
                    now="2026-08-01T00:10:00+00:00",
                )
            finalized = conn.execute(
                """
                SELECT COUNT(*) FROM memory_moment_windows
                WHERE lifecycle_status='finalized'
                """
            ).fetchone()[0]
            self.assertGreaterEqual(finalized, 1)
            inventory = canon.build_claim_contract_inventory(conn, guild_id=1)
            self.assertGreaterEqual(inventory["sourceRows"]["memory_moment"], 1)
            self.assertGreaterEqual(inventory["adaptedRows"]["memory_moment"], 1)
        finally:
            conn.close()

    def test_inventory_discloses_truncation_instead_of_overclaiming(self):
        conn = sqlite3.connect(":memory:")
        try:
            conn.execute(
                """
                CREATE TABLE broadcast_memory(
                  id INTEGER PRIMARY KEY,
                  guild_id INTEGER,
                  entry_type TEXT,
                  cleaned_summary TEXT,
                  status TEXT
                )
                """
            )
            conn.executemany(
                "INSERT INTO broadcast_memory VALUES(?,?,?,?,?)",
                (
                    (1, 1, "notable_moment", "One.", "active"),
                    (2, 1, "notable_moment", "Two.", "active"),
                ),
            )
            inventory = canon.build_claim_contract_inventory(
                conn,
                guild_id=1,
                max_rows_per_source=1,
            )
            self.assertTrue(inventory["boundedRowsReconciled"])
            self.assertFalse(inventory["sourceAdaptedReconciled"])
            self.assertEqual(
                inventory["sourceReconciliationStatus"],
                "partial_truncated",
            )
            self.assertIn("broadcast_memory", inventory["truncatedSources"])
        finally:
            conn.close()

    def test_inventory_fails_closed_on_source_schema_drift(self):
        conn = sqlite3.connect(":memory:")
        try:
            conn.execute("CREATE TABLE broadcast_memory(foo TEXT)")
            conn.execute("INSERT INTO broadcast_memory VALUES(?)", ("row",))
            inventory = canon.build_claim_contract_inventory(
                conn,
                guild_id=1,
            )
            self.assertEqual(inventory["sourceRows"]["broadcast_memory"], 1)
            self.assertEqual(
                inventory["inspectedRows"]["broadcast_memory"],
                0,
            )
            self.assertFalse(inventory["sourceAdaptedReconciled"])
            self.assertEqual(
                inventory["sourceReconciliationStatus"],
                "partial_truncated",
            )
            self.assertIn("broadcast_memory", inventory["truncatedSources"])
        finally:
            conn.close()

    def test_inventory_lineage_lookup_is_candidate_and_guild_scoped(self):
        conn = sqlite3.connect(":memory:")
        try:
            conn.execute(
                """
                CREATE TABLE memory_ledger_knowledge_roots(
                  candidate_id TEXT,
                  guild_id INTEGER,
                  root_entry_id TEXT,
                  is_independent INTEGER
                )
                """
            )
            conn.executemany(
                "INSERT INTO memory_ledger_knowledge_roots VALUES(?,?,?,?)",
                (
                    ("candidate-1", 1, "guild-1-root", 1),
                    ("candidate-1", 2, "guild-2-root", 1),
                    ("candidate-2", 1, "other-candidate-root", 1),
                ),
            )
            roots = canon._inventory_scoped_lineage_map(
                conn,
                table_name="memory_ledger_knowledge_roots",
                owner_column="candidate_id",
                root_column="root_entry_id",
                owner_ids=("candidate-1",),
                guild_id=1,
                independent_only=True,
            )
            self.assertEqual(roots, {"candidate-1": ["guild-1-root"]})
        finally:
            conn.close()

    def test_inventory_lineage_lookup_requires_requested_scope_columns(self):
        for omitted_column in ("guild_id", "is_independent"):
            conn = sqlite3.connect(":memory:")
            try:
                definitions = {
                    "candidate_id": "TEXT",
                    "guild_id": "INTEGER",
                    "root_entry_id": "TEXT",
                    "is_independent": "INTEGER",
                }
                selected = tuple(
                    column
                    for column in definitions
                    if column != omitted_column
                )
                conn.execute(
                    "CREATE TABLE memory_ledger_knowledge_roots(%s)"
                    % ",".join(
                        "%s %s" % (column, definitions[column])
                        for column in selected
                    )
                )
                values = {
                    "candidate_id": "candidate-1",
                    "guild_id": 1,
                    "root_entry_id": "root-x",
                    "is_independent": 1,
                }
                conn.execute(
                    "INSERT INTO memory_ledger_knowledge_roots VALUES(%s)"
                    % ",".join("?" for _column in selected),
                    tuple(values[column] for column in selected),
                )

                roots = canon._inventory_scoped_lineage_map(
                    conn,
                    table_name="memory_ledger_knowledge_roots",
                    owner_column="candidate_id",
                    root_column="root_entry_id",
                    owner_ids=("candidate-1",),
                    guild_id=1,
                    independent_only=True,
                )

                self.assertEqual(roots, {}, omitted_column)
            finally:
                conn.close()

    def test_callem_bini_cannot_unlock_cache_back_in_broad_packet(self):
        conn = sqlite3.connect(":memory:")
        try:
            ledger.ensure_memory_ledger_schema(conn)
            conn.execute(
                """
                CREATE TABLE conversations (
                  id INTEGER PRIMARY KEY,guild_id INTEGER,user_id INTEGER,
                  user_name TEXT,role TEXT,content TEXT,channel_id INTEGER,
                  channel_policy TEXT,route_mode TEXT NOT NULL,timestamp TEXT
                )
                """
            )
            for row_id, observed_at, text in (
                (
                    1,
                    "2026-08-01T00:00:00+00:00",
                    "I keep returning to distorted tape transitions.",
                ),
                (
                    2,
                    "2026-08-01T01:00:00+00:00",
                    "I tested another transition for the live set.",
                ),
            ):
                conn.execute(
                    """
                    INSERT INTO conversations VALUES(?,1,42,?, 'user',?,10,
                                                      'public_home','normal_chat',?)
                    """,
                    (row_id, "Call'em Bini", text, observed_at),
                )
                result = ledger.shadow_conversation_row(
                    conn,
                    row_id=row_id,
                    user_id=42,
                    user_name="Call'em Bini",
                    guild_id=1,
                    role="user",
                    content=text,
                    channel_name="barcode-bot",
                    channel_policy="public_home",
                    channel_id=10,
                    route_mode="normal_chat",
                    observed_at=observed_at,
                )
                self.assertEqual(result.outcome, "inserted")
            request = packet.IntelligencePacketRequest(
                guild_id=1,
                channel_id=10,
                channel_policy="public_home",
                route_mode="normal_chat",
                conversation_surface="mention_or_reply",
                subject_user_id=42,
                subject_display_name="Call'em Bini",
                user_text="BNL, what am I all about?",
                now="2026-08-01T02:00:00+00:00",
            )
            built = packet.build_packet(
                conn,
                request,
                persist=False,
                environ={
                    "BNL_UNIFIED_INTELLIGENCE_PACKET_SHADOW_ENABLED": "true",
                    "BNL_MEMORY_LEDGER_SHADOW_ENABLED": "true",
                    "BNL_MOMENT_ENGINE_SHADOW_ENABLED": "true",
                    "BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED": "true",
                    "BNL_RELATIONSHIP_V2_SHADOW_ENABLED": "true",
                    "BNL_MEMORY_GOVERNANCE_LIVE_ENABLED": "false",
                    "BNL_RELATIONSHIP_V2_LIVE_ENABLED": "false",
                    "BNL_ACTIVE_ENGAGEMENT_V2_LIVE_ENABLED": "false",
                },
            )
            self.assertIsNotNone(built)
            self.assertEqual(
                built.diagnostics.canon_identity_status,
                "no_exact_canon_label",
            )
            self.assertFalse(
                any(
                    item.source_type == "recognized_canon_fact"
                    for item in (*built.items, *built.validation_items)
                )
            )
            self.assertFalse(
                any("Cache Back" in item.text for item in built.items)
            )
            self.assertEqual(built.profile_sufficiency.status, "rich")
            self.assertTrue(built.profile_sufficiency.satisfied)
            self.assertEqual(
                len(
                    tuple(
                        item
                        for item in built.items
                        if item.lane == "assessment_observation"
                    )
                ),
                2,
            )
        finally:
            conn.close()

    def test_shadow_canon_reference_is_projection_not_independent_authority(self):
        conn = sqlite3.connect(":memory:")
        try:
            ledger.ensure_memory_ledger_schema(conn)
            result = ledger.shadow_canon_reference(
                conn,
                canon_id="claim-1",
                revision_id="revision-1",
                guild_id=1,
                subject_key=ledger.BNL_SUBJECT_KEY,
                subject_display_name="BNL-01",
                predicate_key="roles",
                value="BNL is the continuity layer.",
            )
            row = conn.execute(
                """
                SELECT source_table,source_revision,source_role,derived,
                       projection
                FROM memory_ledger_entries WHERE entry_id=?
                """,
                (result.entry_id,),
            ).fetchone()
            self.assertEqual(
                row,
                (
                    "canon_claim_projection",
                    "revision-1",
                    "canon_projection",
                    1,
                    1,
                ),
            )
            attempted = ledger.form_atomic_candidate_from_ledger_entry(
                conn,
                result.entry_id,
            )
            self.assertEqual(attempted.outcome, "rejected")
            self.assertEqual(
                attempted.reason_code,
                "derivative_misclassified_as_independent",
            )
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
