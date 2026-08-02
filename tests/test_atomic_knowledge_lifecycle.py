import os
import sqlite3
import tempfile
import unittest

from bnl_canon_source_contract import Confidence, SourceClass, Visibility
import bnl_memory_ledger as ledger


class AtomicKnowledgeLifecycleTests(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        ledger.ensure_memory_ledger_schema(self.conn)

    def tearDown(self):
        self.conn.close()

    def add_root(
        self,
        row_id,
        *,
        source_revision=None,
        subject_key="discord_user:7",
        entry_type="preference",
        predicate_key="favorite_color",
        value="green",
        source_class=SourceClass.FIRST_PARTY_RECORD,
        source_role="member_self_report",
        visibility=Visibility.PUBLIC,
        confidence=Confidence.HIGH,
        public_usable=None,
        route_mode="normal_chat",
        channel_policy="public_home",
        participants=None,
        lineage=(),
        lifecycle_status="active",
        source_table="conversations",
        observed_at=None,
        guild_id=1,
    ):
        if public_usable is None:
            public_usable = visibility in {
                Visibility.PUBLIC,
                Visibility.PUBLIC_SAFE,
                Visibility.REFERENCE_CANON,
            }
        if participants is None:
            participants = (
                ledger.LedgerParticipant(
                    subject_key,
                    "Crow",
                    "author",
                    0,
                ),
            ) if subject_key.startswith("discord_user:") else ()
        entry = ledger.LedgerEntry(
            guild_id=guild_id,
            source_table=source_table,
            source_row_id=row_id,
            source_revision=(
                str(row_id)
                if source_revision is None
                else str(source_revision)
            ),
            source_role=source_role,
            entry_type=entry_type,
            subject_key=subject_key,
            subject_display_name=(
                "Crow"
                if subject_key.startswith("discord_user:")
                else "BARCODE Network"
            ),
            predicate_key=predicate_key,
            value=value,
            source_class=source_class,
            route_mode=route_mode,
            channel_id=10,
            channel_name="barcode-bot",
            channel_policy=channel_policy,
            visibility=visibility,
            confidence=confidence,
            public_usable=public_usable,
            observed_at=(
                observed_at
                or f"2026-07-25T00:00:{int(row_id) % 60:02d}+00:00"
            ),
            lifecycle_status=lifecycle_status,
            participants=tuple(participants),
            lineage=tuple(lineage),
        )
        result = ledger.insert_ledger_entry(self.conn, entry)
        self.assertIn(result.outcome, {"inserted", "deduplicated"})
        return result.entry_id

    def form(
        self,
        roots,
        *,
        meaning="green",
        predicate_key="favorite_color",
        candidate_type="person_role_fact",
        epistemic_status="stated",
        currentness="current",
        subject_key="discord_user:7",
        participant_keys=("discord_user:7",),
        derivative_entry_ids=(),
        contradiction_key="",
    ):
        return ledger.form_atomic_knowledge_candidate(
            self.conn,
            ledger.AtomicKnowledgeProposal(
                candidate_type=candidate_type,
                subject_key=subject_key,
                predicate_key=predicate_key,
                meaning=meaning,
                root_entry_ids=tuple(roots),
                derivative_entry_ids=tuple(derivative_entry_ids),
                participant_keys=tuple(participant_keys),
                epistemic_status=epistemic_status,
                currentness=currentness,
                contradiction_key=contradiction_key,
            ),
        )

    def lifecycle_row(self, candidate_id):
        return self.conn.execute(
            """
            SELECT
              candidate_state,candidate_eligible,live_eligible,
              lifecycle_schema_version,consolidation_id,
              canonical_candidate_id,supporting_candidate_count,
              eligible_independent_root_count,reinforcement_count,
              duplicate_support_count,conflict_value_count,
              lifecycle_reason,review_status,review_due_at,
              invalidated_reason
            FROM memory_ledger_knowledge_candidates
            WHERE candidate_id=?
            """,
            (candidate_id,),
        ).fetchone()

    def test_single_source_is_provisional_and_independent_reinforcement_establishes(self):
        first_root = self.add_root(1)
        first = ledger.form_atomic_candidate_from_ledger_entry(
            self.conn,
            first_root,
        )
        first_row = self.lifecycle_row(first.candidate_id)
        self.assertEqual(first_row[0], "provisional")
        self.assertEqual(first_row[1:3], (1, 0))
        self.assertEqual(first_row[8], 1)

        second_root = self.add_root(2)
        second = ledger.form_atomic_candidate_from_ledger_entry(
            self.conn,
            second_root,
        )
        first_row = self.lifecycle_row(first.candidate_id)
        second_row = self.lifecycle_row(second.candidate_id)

        self.assertEqual(first_row[0], "established")
        self.assertEqual(second_row[0], "established")
        self.assertEqual(first_row[4], second_row[4])
        self.assertEqual(first_row[5], second_row[5])
        self.assertIn(
            first_row[5],
            {first.candidate_id, second.candidate_id},
        )
        self.assertEqual(first_row[6:10], (2, 2, 2, 0))
        self.assertEqual(
            first_row[11],
            "independent_reinforcement_established",
        )

    def test_exact_source_copies_and_derivatives_do_not_reinforce(self):
        first_root = self.add_root(
            3,
            source_revision="same-source",
            predicate_key="root_a",
            entry_type="preference",
        )
        duplicate_root = self.add_root(
            3,
            source_revision="edited-same-source",
            predicate_key="root_b",
            entry_type="claim",
        )
        derivative = self.add_root(
            4,
            subject_key="moment:duplicate-source",
            entry_type="shared_moment",
            predicate_key="shared_moment",
            value="BNL derivative of the same source.",
            source_class=SourceClass.DERIVED_SUMMARY,
            source_role="derived_assessment",
            confidence=Confidence.APPROVED,
            participants=(
                ledger.LedgerParticipant(
                    "discord_user:7",
                    "Crow",
                    "participant",
                    0,
                ),
            ),
            lineage=(("derived_from", first_root),),
            source_table="memory_moment_windows",
        )
        first = self.form(
            (first_root,),
            derivative_entry_ids=(derivative,),
        )
        second = self.form((duplicate_root,))
        row = self.lifecycle_row(second.candidate_id)

        self.assertEqual(row[0], "provisional")
        self.assertEqual(row[6:10], (2, 2, 1, 1))
        lifecycle_roots = self.conn.execute(
            """
            SELECT root_entry_id,counts_as_reinforcement
            FROM memory_ledger_knowledge_lifecycle_roots
            WHERE candidate_id=?
            ORDER BY root_entry_id
            """,
            (row[5],),
        ).fetchall()
        self.assertNotIn(derivative, {root[0] for root in lifecycle_roots})
        event_counts = self.conn.execute(
            """
            SELECT event_id,SUM(counts_as_reinforcement)
            FROM memory_ledger_knowledge_lifecycle_roots
            WHERE candidate_id=?
            GROUP BY event_id
            """,
            (row[5],),
        ).fetchall()
        self.assertTrue(event_counts)
        self.assertTrue(
            all(int(count or 0) <= 1 for _event_id, count in event_counts)
        )
        self.assertTrue(first)

    def test_authority_can_establish_but_confidence_cannot_create_authority(self):
        approved_root = self.add_root(
            90,
            subject_key=ledger.BNL_SUBJECT_KEY,
            entry_type="canon_reference",
            predicate_key="roles",
            value="BNL is the Network continuity layer.",
            source_class=SourceClass.APPROVED_CANON,
            source_role="approved_canon_source",
            visibility=Visibility.REFERENCE_CANON,
            confidence=Confidence.APPROVED,
            route_mode="approved_canon",
            channel_policy="reference_canon",
            participants=(),
            source_table="approved_canon_registry",
            observed_at="2026-07-25T00:00:05+00:00",
        )
        approved = ledger.form_atomic_candidate_from_ledger_entry(
            self.conn,
            approved_root,
        )
        self.assertEqual(
            self.lifecycle_row(approved.candidate_id)[0:2],
            ("established", 1),
        )
        self.assertEqual(
            self.lifecycle_row(approved.candidate_id)[11],
            "authoritative_source_established",
        )

        observation_root = self.add_root(
            6,
            source_class=SourceClass.PUBLIC_OBSERVATION,
            source_role="user",
            confidence=Confidence.APPROVED,
        )
        observation = ledger.form_atomic_candidate_from_ledger_entry(
            self.conn,
            observation_root,
        )
        self.assertEqual(
            self.lifecycle_row(observation.candidate_id)[0],
            "provisional",
        )
        authority = self.conn.execute(
            """
            SELECT authority_class
            FROM memory_ledger_knowledge_candidates
            WHERE candidate_id=?
            """,
            (observation.candidate_id,),
        ).fetchone()[0]
        self.assertEqual(
            authority,
            SourceClass.PUBLIC_OBSERVATION.value,
        )
        stronger_root = self.add_root(
            62,
            source_class=SourceClass.FIRST_PARTY_RECORD,
            source_role="member_self_report",
            confidence=Confidence.HIGH,
        )
        stronger = ledger.form_atomic_candidate_from_ledger_entry(
            self.conn,
            stronger_root,
        )
        consolidated = self.conn.execute(
            """
            SELECT consolidated_authority_class,
                   consolidated_confidence_class
            FROM memory_ledger_knowledge_candidates
            WHERE candidate_id=?
            """,
            (stronger.candidate_id,),
        ).fetchone()
        self.assertEqual(
            consolidated,
            (
                SourceClass.PUBLIC_OBSERVATION.value,
                Confidence.HIGH.value,
            ),
        )

    def test_motif_rendering_variants_reinforce_one_atomic_scope(self):
        first_root = self.add_root(
            60,
            entry_type="observation",
            predicate_key="conversation",
            value="I keep returning to modular patch design.",
            source_class=SourceClass.PUBLIC_OBSERVATION,
            source_role="user",
        )
        second_root = self.add_root(
            61,
            entry_type="observation",
            predicate_key="conversation",
            value="Modular routing came up again.",
            source_class=SourceClass.PUBLIC_OBSERVATION,
            source_role="user",
        )
        first = self.form(
            (first_root,),
            meaning="Modular patch design appeared as a topic.",
            predicate_key="moment_observation_modular",
            candidate_type="topic_or_motif",
            epistemic_status="source_abstraction",
            currentness="historical",
            contradiction_key="discord_user:7:observation:modular",
        )
        second = self.form(
            (second_root,),
            meaning="The participant returned to modular routing.",
            predicate_key="moment_observation_modular",
            candidate_type="topic_or_motif",
            epistemic_status="source_abstraction",
            currentness="historical",
            contradiction_key="discord_user:7:observation:modular",
        )

        first_row = self.lifecycle_row(first.candidate_id)
        second_row = self.lifecycle_row(second.candidate_id)
        self.assertEqual(first_row[0], "established")
        self.assertEqual(second_row[0], "established")
        self.assertEqual(first_row[4], second_row[4])
        self.assertEqual(first_row[8], 2)
        self.assertEqual(first_row[10], 1)

    def test_unresolved_conflicts_remain_contested_without_implicit_winner(self):
        green_root = self.add_root(7, value="green")
        green = ledger.form_atomic_candidate_from_ledger_entry(
            self.conn,
            green_root,
        )
        blue_root = self.add_root(
            8,
            value="blue",
            source_class=SourceClass.OWNER_CORRECTION,
            source_role="owner",
            confidence=Confidence.APPROVED,
        )
        blue = ledger.form_atomic_candidate_from_ledger_entry(
            self.conn,
            blue_root,
        )

        self.assertEqual(self.lifecycle_row(green.candidate_id)[0], "contested")
        self.assertEqual(self.lifecycle_row(blue.candidate_id)[0], "contested")
        self.assertEqual(self.lifecycle_row(green.candidate_id)[10], 2)
        report = ledger.build_memory_ledger_evaluation(self.conn, guild_id=1)
        self.assertEqual(report["knowledgeLifecycleConflictScopes"], 1)

    def test_explicit_correction_supersedes_old_and_establishes_owner_record(self):
        old_root = self.add_root(
            9,
            predicate_key="favorite_movie",
            value="Hackers",
        )
        old = ledger.form_atomic_candidate_from_ledger_entry(
            self.conn,
            old_root,
        )
        correction_root = self.add_root(
            10,
            predicate_key="favorite_movie",
            value="The Matrix",
            source_class=SourceClass.FIRST_PARTY_RECORD,
            source_role="member_control",
            confidence=Confidence.HIGH,
            lineage=(
                ("correction_of", old_root),
                ("supersedes", old_root),
            ),
        )
        correction = ledger.form_atomic_candidate_from_ledger_entry(
            self.conn,
            correction_root,
        )

        self.assertEqual(self.lifecycle_row(old.candidate_id)[0], "superseded")
        corrected_row = self.lifecycle_row(correction.candidate_id)
        self.assertEqual(corrected_row[0], "established")
        self.assertEqual(
            corrected_row[11],
            "explicit_correction_established",
        )
        self.assertEqual(corrected_row[10], 1)

    def test_candidate_provisional_retired_and_invalidated_states_are_distinct(self):
        inference_root = self.add_root(
            11,
            entry_type="observation",
            predicate_key="possible_motif",
            value="A possible motif appeared.",
            source_class=SourceClass.PUBLIC_OBSERVATION,
            source_role="user",
        )
        inference = self.form(
            (inference_root,),
            meaning="Inference: this may be a motif.",
            predicate_key="possible_motif",
            candidate_type="inference_or_contested_claim",
            epistemic_status="inference",
            currentness="uncertain",
        )
        self.assertEqual(self.lifecycle_row(inference.candidate_id)[0], "candidate")

        open_root = self.add_root(
            12,
            entry_type="open_loop",
            predicate_key="unfinished_mix",
            value="Finish the mix.",
            observed_at="2025-01-01T00:00:00+00:00",
        )
        open_candidate = ledger.form_atomic_candidate_from_ledger_entry(
            self.conn,
            open_root,
        )
        ledger.reconcile_atomic_knowledge_lifecycle(
            self.conn,
            candidate_ids=(open_candidate.candidate_id,),
            now="2026-07-25T00:00:00+00:00",
        )
        stale_row = self.lifecycle_row(open_candidate.candidate_id)
        self.assertEqual(stale_row[0], "retired")
        self.assertEqual(stale_row[12], "retired_stale")

        resolved_root = self.add_root(
            13,
            entry_type="open_loop",
            predicate_key="resolved_loop",
            value="Resolve this loop.",
        )
        resolved_candidate = ledger.form_atomic_candidate_from_ledger_entry(
            self.conn,
            resolved_root,
        )
        self.conn.execute(
            """
            UPDATE memory_ledger_entries
            SET lifecycle_status='resolved'
            WHERE entry_id=?
            """,
            (resolved_root,),
        )
        self.assertEqual(
            self.lifecycle_row(resolved_candidate.candidate_id)[0],
            "retired",
        )

        private_root = self.add_root(
            14,
            predicate_key="favorite_food",
            value="pizza",
        )
        private_candidate = ledger.form_atomic_candidate_from_ledger_entry(
            self.conn,
            private_root,
        )
        self.conn.execute(
            """
            UPDATE memory_ledger_entries
            SET visibility='private',public_usable=0
            WHERE entry_id=?
            """,
            (private_root,),
        )
        private_row = self.lifecycle_row(private_candidate.candidate_id)
        self.assertEqual(private_row[0], "invalidated")
        self.assertEqual(private_row[1:3], (0, 0))

    def test_review_due_does_not_silently_retire_current_or_historical_fact(self):
        current_root = self.add_root(
            15,
            predicate_key="preferred_name",
            value="Crow",
            observed_at="2025-01-01T00:00:00+00:00",
        )
        current = ledger.form_atomic_candidate_from_ledger_entry(
            self.conn,
            current_root,
        )
        ledger.reconcile_atomic_knowledge_lifecycle(
            self.conn,
            candidate_ids=(current.candidate_id,),
            now="2026-07-25T00:00:00+00:00",
        )
        current_row = self.lifecycle_row(current.candidate_id)
        self.assertEqual(current_row[0], "provisional")
        self.assertEqual(current_row[12], "due")

        event_root = self.add_root(
            16,
            subject_key="barcode_network",
            entry_type="event",
            predicate_key="checkpoint",
            value="The checkpoint happened.",
            source_role="operator",
            participants=(),
            source_table="project_evidence",
            observed_at="2024-01-01T00:00:00+00:00",
        )
        event = ledger.form_atomic_candidate_from_ledger_entry(
            self.conn,
            event_root,
        )
        ledger.reconcile_atomic_knowledge_lifecycle(
            self.conn,
            candidate_ids=(event.candidate_id,),
            now="2026-07-25T00:00:00+00:00",
        )
        event_row = self.lifecycle_row(event.candidate_id)
        self.assertEqual(event_row[0], "provisional")
        self.assertEqual(event_row[12], "not_required")

    def test_visibility_and_participant_scopes_never_consolidate(self):
        public_root = self.add_root(17)
        public = ledger.form_atomic_candidate_from_ledger_entry(
            self.conn,
            public_root,
        )
        internal_root = self.add_root(
            18,
            visibility=Visibility.INTERNAL,
            public_usable=False,
            channel_policy="internal_controlled",
        )
        internal = ledger.form_atomic_candidate_from_ledger_entry(
            self.conn,
            internal_root,
        )

        self.assertNotEqual(
            self.lifecycle_row(public.candidate_id)[4],
            self.lifecycle_row(internal.candidate_id)[4],
        )
        self.assertEqual(self.lifecycle_row(public.candidate_id)[8], 1)
        self.assertEqual(self.lifecycle_row(internal.candidate_id)[8], 1)
        self.assertEqual(self.lifecycle_row(public.candidate_id)[0], "provisional")
        self.assertEqual(self.lifecycle_row(internal.candidate_id)[0], "provisional")

    def test_promotion_receipts_preserve_exact_roots_and_replay_is_idempotent(self):
        first_root = self.add_root(19)
        first = ledger.form_atomic_candidate_from_ledger_entry(
            self.conn,
            first_root,
        )
        second_root = self.add_root(20)
        second = ledger.form_atomic_candidate_from_ledger_entry(
            self.conn,
            second_root,
        )
        canonical_id = self.lifecycle_row(first.candidate_id)[5]
        event = self.conn.execute(
            """
            SELECT event_id
            FROM memory_ledger_knowledge_lifecycle_events
            WHERE candidate_id=? AND next_state='established'
            ORDER BY occurred_at DESC,event_id DESC
            LIMIT 1
            """,
            (canonical_id,),
        ).fetchone()
        self.assertIsNotNone(event)
        roots = self.conn.execute(
            """
            SELECT root_entry_id,counts_as_reinforcement
            FROM memory_ledger_knowledge_lifecycle_roots
            WHERE event_id=?
            ORDER BY root_entry_id
            """,
            (event[0],),
        ).fetchall()
        self.assertEqual(
            {root[0] for root in roots},
            {first_root, second_root},
        )
        self.assertEqual(sum(int(root[1] or 0) for root in roots), 2)
        before = self.conn.execute(
            "SELECT COUNT(*) FROM memory_ledger_knowledge_lifecycle_events"
        ).fetchone()[0]
        result = ledger.reconcile_atomic_knowledge_lifecycle(
            self.conn,
            candidate_ids=(first.candidate_id, second.candidate_id),
            now="2026-07-25T01:00:00+00:00",
        )
        after = self.conn.execute(
            "SELECT COUNT(*) FROM memory_ledger_knowledge_lifecycle_events"
        ).fetchone()[0]
        self.assertEqual(result["state_changes"], 0)
        self.assertEqual(before, after)

    def test_lifecycle_backfill_is_bounded_resumable_and_restart_safe(self):
        fd, path = tempfile.mkstemp(
            prefix="bnl-lifecycle-",
            suffix=".sqlite3",
        )
        os.close(fd)
        try:
            conn = sqlite3.connect(path)
            ledger.ensure_memory_ledger_schema(conn)
            for row_id, predicate in (
                (21, "favorite_color"),
                (22, "favorite_movie"),
            ):
                entry = ledger.LedgerEntry(
                    guild_id=1,
                    source_table="conversations",
                    source_row_id=row_id,
                    source_revision=str(row_id),
                    source_role="member_self_report",
                    entry_type="preference",
                    subject_key="discord_user:7",
                    subject_display_name="Crow",
                    predicate_key=predicate,
                    value=f"value-{row_id}",
                    source_class=SourceClass.FIRST_PARTY_RECORD,
                    route_mode="normal_chat",
                    channel_policy="public_home",
                    visibility=Visibility.PUBLIC,
                    confidence=Confidence.HIGH,
                    public_usable=True,
                    observed_at=f"2026-07-25T00:00:{row_id}+00:00",
                    participants=(
                        ledger.LedgerParticipant(
                            "discord_user:7",
                            "Crow",
                            "author",
                            0,
                        ),
                    ),
                )
                root = ledger.insert_ledger_entry(conn, entry).entry_id
                ledger.form_atomic_candidate_from_ledger_entry(conn, root)
            conn.execute(
                """
                UPDATE memory_ledger_knowledge_candidates
                SET candidate_state='candidate',candidate_eligible=1,
                    lifecycle_schema_version='',consolidation_id='',
                    canonical_candidate_id='',
                    supporting_candidate_count=0,
                    eligible_independent_root_count=0,
                    reinforcement_count=0,duplicate_support_count=0,
                    conflict_value_count=0,lifecycle_reason='',
                    review_status='not_evaluated',review_due_at='',
                    lifecycle_evaluated_at=''
                """
            )
            conn.execute(
                "DELETE FROM memory_ledger_knowledge_lifecycle_roots"
            )
            conn.execute(
                "DELETE FROM memory_ledger_knowledge_lifecycle_events"
            )
            conn.execute(
                """
                DELETE FROM memory_ledger_knowledge_backfill
                WHERE migration_key=?
                """,
                (ledger.ATOMIC_KNOWLEDGE_LIFECYCLE_BACKFILL,),
            )
            first = ledger.backfill_atomic_knowledge_lifecycle(
                conn,
                batch_size=1,
                now="2026-07-25T00:00:00+00:00",
            )
            conn.commit()
            self.assertFalse(first["completed"])
            conn.close()

            conn = sqlite3.connect(path)
            second = ledger.backfill_atomic_knowledge_lifecycle(
                conn,
                batch_size=1,
                now="2026-07-25T00:00:00+00:00",
            )
            third = ledger.backfill_atomic_knowledge_lifecycle(
                conn,
                batch_size=1,
                now="2026-07-25T00:00:00+00:00",
            )
            fourth = ledger.backfill_atomic_knowledge_lifecycle(
                conn,
                batch_size=1,
                now="2026-07-25T00:00:00+00:00",
            )
            conn.commit()
            self.assertFalse(second["completed"])
            self.assertTrue(third["completed"])
            self.assertTrue(fourth["completed"])
            self.assertEqual(third, fourth)
            rows = conn.execute(
                """
                SELECT candidate_state,lifecycle_schema_version,
                       live_eligible
                FROM memory_ledger_knowledge_candidates
                """
            ).fetchall()
            self.assertEqual(len(rows), 2)
            self.assertTrue(
                all(
                    state == "provisional"
                    and version
                    == ledger.ATOMIC_KNOWLEDGE_LIFECYCLE_SCHEMA_VERSION
                    and not live_eligible
                    for state, version, live_eligible in rows
                )
            )
            conn.close()
        finally:
            if os.path.exists(path):
                os.unlink(path)

    def test_periodic_sweep_applies_due_review_without_a_new_write(self):
        root = self.add_root(
            26,
            entry_type="open_loop",
            predicate_key="stale_without_write",
            value="This open loop needs review.",
        )
        candidate = ledger.form_atomic_candidate_from_ledger_entry(
            self.conn,
            root,
        )
        self.conn.execute(
            """
            UPDATE memory_ledger_knowledge_candidates
            SET first_seen_at='2025-01-01T00:00:00+00:00',
                last_seen_at='2025-01-01T00:00:00+00:00'
            WHERE candidate_id=?
            """,
            (candidate.candidate_id,),
        )

        swept = ledger.sweep_atomic_knowledge_lifecycle(
            self.conn,
            batch_size=1,
            now="2026-07-25T00:00:00+00:00",
            min_interval_seconds=0,
        )
        skipped = ledger.sweep_atomic_knowledge_lifecycle(
            self.conn,
            batch_size=1,
            now="2026-07-25T00:01:00+00:00",
            min_interval_seconds=900,
        )

        self.assertTrue(swept["ran"])
        self.assertEqual(
            self.lifecycle_row(candidate.candidate_id)[0],
            "retired",
        )
        self.assertFalse(skipped["ran"])
        report = ledger.build_memory_ledger_evaluation(
            self.conn,
            guild_id=1,
        )
        self.assertEqual(
            report["knowledgeLifecycleSweep"]["counts"]["runs"],
            1,
        )

    def test_diagnostics_are_aggregate_complete_and_shadow_only(self):
        secret = "lifecycle-secret-should-not-leak-991"
        first_root = self.add_root(23, value=secret)
        first = self.form((first_root,), meaning=secret)
        second_root = self.add_root(24, value=secret)
        second = self.form((second_root,), meaning=secret)
        self.assertTrue(first)
        self.assertTrue(second)
        ledger.backfill_atomic_knowledge_lifecycle(
            self.conn,
            now="2026-07-25T00:00:00+00:00",
        )

        report = ledger.build_memory_ledger_evaluation(
            self.conn,
            guild_id=1,
        )

        self.assertEqual(
            report["knowledgeLifecycleSchemaVersion"],
            ledger.ATOMIC_KNOWLEDGE_LIFECYCLE_SCHEMA_VERSION,
        )
        self.assertEqual(report["knowledgeLifecycleConsolidationGroups"], 1)
        self.assertEqual(report["knowledgeLifecycleCanonicalCandidates"], 1)
        self.assertEqual(
            report["knowledgeLifecycleReinforcementDistribution"]["2"],
            1,
        )
        self.assertEqual(
            report["knowledgeLifecycleConsolidatedAuthority"][
                SourceClass.FIRST_PARTY_RECORD.value
            ],
            1,
        )
        self.assertEqual(
            report["knowledgeLifecycleMissingPromotionProvenance"],
            0,
        )
        self.assertEqual(report["knowledgeLifecycleDirtyCandidates"], 0)
        self.assertEqual(report["knowledgeCandidateLiveEligibleCount"], 0)
        self.assertNotIn(secret, repr(report))

    def test_complete_delete_removes_lifecycle_audit_boundary(self):
        root = self.add_root(25)
        candidate = ledger.form_atomic_candidate_from_ledger_entry(
            self.conn,
            root,
        )
        self.assertTrue(candidate)
        counts = ledger.purge_atomic_knowledge_for_subject(
            self.conn,
            guild_id=1,
            subject_key="discord_user:7",
        )
        self.assertGreaterEqual(
            counts["memory_ledger_knowledge_lifecycle_events"],
            1,
        )
        for table in (
            "memory_ledger_knowledge_candidates",
            "memory_ledger_knowledge_roots",
            "memory_ledger_knowledge_participants",
            "memory_ledger_knowledge_lifecycle_events",
            "memory_ledger_knowledge_lifecycle_roots",
        ):
            self.assertEqual(
                self.conn.execute(
                    f"SELECT COUNT(*) FROM {table}"
                ).fetchone()[0],
                0,
            )


if __name__ == "__main__":
    unittest.main()
