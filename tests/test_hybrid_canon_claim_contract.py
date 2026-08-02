import sqlite3
import unittest
from dataclasses import replace
from unittest import mock

import bnl_canon_source_contract as canon
import bnl_memory_ledger as ledger
import bnl_moment_engine as moments
import bnl_unified_intelligence_packet as packet


class HybridCanonClaimContractTests(unittest.TestCase):
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
                    authority_actor="owner_ref:binding_owner",
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
                    authority_actor="owner_ref:binding_owner",
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

    def test_broadcast_adapter_stays_review_only_in_pr1(self):
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
        self.assertEqual(
            declared.claim.lifecycle,
            canon.ClaimLifecycle.REVIEW_ONLY,
        )
        self.assertFalse(declared.live_eligible)
        self.assertEqual(declared.reason, "owner_verified_review_only")
        self.assertEqual(
            declared.claim.canon_status,
            canon.CanonStatus.DECLARED,
        )
        self.assertEqual(
            declared.claim.domain,
            canon.CanonDomain.BROADCAST_HISTORY,
        )
        self.assertEqual(declared.claim.visibility, canon.Visibility.PUBLIC_SAFE)
        self.assertNotIn("Private operator wording", declared.claim.value)

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
        self.assertEqual(forged.reason, "owner_authority_review_only")
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

    def test_living_adapter_rejects_heterogeneous_or_unestablished_rows(self):
        role = canon.adapt_living_atomic_claim(
            {
                "candidate_id": "role-candidate",
                "candidate_type": "person_role_fact",
                "candidate_state": "established",
                "subject_key": "discord_user:7",
                "predicate_key": "role",
                "meaning": "A role claim.",
                "root_ids": ("root-1", "root-2"),
            }
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
        self.assertEqual(role.reason, "living_claim_kind_ineligible")
        self.assertEqual(
            provisional.reason,
            "living_lifecycle_ineligible",
        )

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
        adapted = canon.adapt_living_atomic_claim(
            {
                "candidate_id": "candidate-2",
                "candidate_type": "topic_or_motif",
                "candidate_state": "established",
                "subject_key": "discord_user:7",
                "predicate_key": "unseen_topic_pattern",
                "meaning": "They repeatedly compare unusual antenna materials.",
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
                "visibility": "public_safe",
                "public_usable": True,
            }
        )
        self.assertEqual(adapted.reason, "eligible_living")
        self.assertEqual(adapted.claim.canon_status, canon.CanonStatus.LIVING)
        self.assertEqual(
            adapted.claim.recurrence_contract_version,
            canon.LIVING_CANON_RECURRENCE_VERSION,
        )
        self.assertFalse(adapted.live_eligible)

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

    def test_open_signal_is_ephemeral_and_source_linked(self):
        adapted = canon.adapt_open_signal_claim(
            {
                "subject_key": "discord_user:7",
                "entry_id": "entry-7",
                "text": "They keep returning to unusual signal routing.",
                "occurrence_identity": "occurrence-7",
                "observed_at": "2026-08-01T00:00:00+00:00",
                "assessment_contract_version": "public_assessment_evidence_v1",
                "source_system": "memory_ledger_public_assessment",
                "source_role": "user",
                "source_class": "public_observation",
                "lifecycle_status": "active",
                "visibility": "public_safe",
                "channel_policy": "public_home",
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
            ("memory_ledger:entry-7",),
        )
        self.assertEqual(adapted.claim.occurrence_ids, ("occurrence-7",))

    def test_open_signal_rejects_private_deleted_or_model_derived_rows(self):
        base = {
            "subject_key": "discord_user:7",
            "entry_id": "entry-7",
            "text": "A bounded observation.",
            "occurrence_identity": "occurrence-7",
            "assessment_contract_version": "public_assessment_evidence_v1",
            "source_system": "memory_ledger_public_assessment",
            "source_role": "user",
            "source_class": "public_observation",
            "lifecycle_status": "active",
            "visibility": "public_safe",
            "channel_policy": "public_home",
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
        self.assertEqual(claim.canon_status, canon.CanonStatus.DECLARED)
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
        self.assertEqual(claim.canon_status, canon.CanonStatus.DECLARED)
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
        self.assertEqual(diagnostics["identityBindingCollisionCount"], 0)
        self.assertEqual(diagnostics["nonOpaqueAuthorityActorCount"], 0)

        repeated = canon.canon_claim_inventory_diagnostics(
            (claims[0], claims[0])
        )
        self.assertEqual(repeated["claimIdCollisionCount"], 0)
        self.assertEqual(repeated["revisionIdCollisionCount"], 0)

        hostile_claim = replace(
            claims[0],
            authority_actor="discord_user:Jane Doe",
        )
        hostile = canon.canon_claim_inventory_diagnostics(
            (hostile_claim,),
            bindings=(
                canon.EntityAccountBinding(
                    entity_id=canon.MAC_MODEM.key,
                    platform="discord",
                    account_id="7",
                    authority_receipt="binding_receipt:hostile",
                    authority_actor="discord_user:Jane Doe",
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

    def test_callem_bini_cannot_unlock_cache_back_in_broad_packet(self):
        conn = sqlite3.connect(":memory:")
        try:
            ledger.ensure_memory_ledger_schema(conn)
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
            self.assertEqual(built.profile_sufficiency.status, "empty")
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
