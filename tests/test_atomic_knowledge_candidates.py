import os
import sqlite3
import tempfile
import unittest

from bnl_canon_source_contract import Confidence, SourceClass, Visibility
from bnl_memory_governance import (
    GovernanceRequest,
    build_governed_context,
    complete_delete_member_data,
    correct_member_memory,
    forget_member_memory,
    view_member_memory,
)
import bnl_memory_ledger as ledger
import bnl_moment_engine as moments


class AtomicKnowledgeCandidateTests(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        ledger.ensure_memory_ledger_schema(self.conn)

    def tearDown(self):
        self.conn.close()

    def add_root(
        self,
        row_id,
        *,
        guild_id=1,
        subject_key="discord_user:7",
        entry_type="preference",
        predicate_key="favorite_color",
        value="green",
        source_class=SourceClass.FIRST_PARTY_RECORD,
        source_role="member_self_report",
        route_mode="normal_chat",
        channel_policy="public_home",
        visibility=Visibility.PUBLIC,
        confidence=Confidence.HIGH,
        public_usable=None,
        derived=False,
        projection=False,
        participants=None,
        lineage=(),
        lifecycle_status="active",
        source_table="conversations",
    ):
        if public_usable is None:
            public_usable = visibility in {
                Visibility.PUBLIC,
                Visibility.PUBLIC_SAFE,
                Visibility.REFERENCE_CANON,
            }
        if participants is None:
            participants = (
                (
                    ledger.LedgerParticipant(
                        subject_key,
                        "Crow",
                        "author",
                        0,
                    ),
                )
                if subject_key.startswith("discord_user:")
                else ()
            )
        entry = ledger.LedgerEntry(
            guild_id=guild_id,
            source_table=source_table,
            source_row_id=row_id,
            source_revision=str(row_id),
            source_role=source_role,
            entry_type=entry_type,
            subject_key=subject_key,
            subject_display_name="Crow"
            if subject_key.startswith("discord_user:")
            else "BARCODE Network",
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
            derived=derived,
            projection=projection,
            observed_at=f"2026-07-25T00:00:{int(row_id) % 60:02d}+00:00",
            source_sequence=int(row_id),
            lifecycle_status=lifecycle_status,
            participants=tuple(participants),
            lineage=tuple(lineage),
        )
        result = ledger.insert_ledger_entry(self.conn, entry)
        self.assertIn(result.outcome, {"inserted", "deduplicated"})
        return result.entry_id

    def candidate_row(self, candidate_id):
        return self.conn.execute(
            """
            SELECT candidate_type,subject_key,normalized_value,
                   epistemic_status,currentness,candidate_state,
                   visibility,authority_class,confidence_class,
                   independent_root_count,derivative_root_count,
                   candidate_eligible,live_eligible,promotion_status,
                   invalidated_reason,supersedes_candidate_id
            FROM memory_ledger_knowledge_candidates
            WHERE candidate_id=?
            """,
            (candidate_id,),
        ).fetchone()

    def test_schema_and_exact_replay_are_deterministic_and_idempotent(self):
        ledger.ensure_memory_ledger_schema(self.conn)
        root = self.add_root(1)

        created = ledger.form_atomic_candidate_from_ledger_entry(
            self.conn,
            root,
        )
        replayed = ledger.form_atomic_candidate_from_ledger_entry(
            self.conn,
            root,
        )

        self.assertEqual(created.outcome, "created")
        self.assertEqual(replayed.outcome, "matched_existing")
        self.assertEqual(created.candidate_id, replayed.candidate_id)
        self.assertEqual(
            self.conn.execute(
                "SELECT COUNT(*) FROM memory_ledger_knowledge_candidates"
            ).fetchone()[0],
            1,
        )
        row = self.candidate_row(created.candidate_id)
        self.assertEqual(row[0], "person_role_fact")
        self.assertEqual(row[11:14], (1, 0, "unpromoted"))

    def test_supported_atomic_types_remain_unpromoted_and_shadow_only(self):
        person_root = self.add_root(2)
        event_root = self.add_root(
            3,
            subject_key="barcode_network",
            entry_type="event",
            predicate_key="shared_brain_checkpoint",
            value="The shared brain checkpoint was production-proven.",
            source_role="operator",
            participants=(),
            source_table="project_evidence",
        )
        open_root = self.add_root(
            4,
            subject_key="barcode_network",
            entry_type="open_loop",
            predicate_key="durable_knowledge_layer",
            value="Source-linked candidate runtime behavior remains to assess.",
            source_role="operator",
            participants=(),
            source_table="project_evidence",
        )
        inference_root = self.add_root(
            5,
            value="Crow returned to modular synthesis discussions.",
            entry_type="claim",
            predicate_key="modular_synthesis_return",
            source_class=SourceClass.PUBLIC_OBSERVATION,
            source_role="user",
        )
        bnl_role_root = self.add_root(
            90,
            subject_key=ledger.BNL_SUBJECT_KEY,
            entry_type="canon_reference",
            predicate_key="roles",
            value="BNL-01 is the Network's memory and continuity layer.",
            source_class=SourceClass.APPROVED_CANON,
            source_role="approved_canon_source",
            route_mode="approved_canon",
            channel_policy="reference_canon",
            visibility=Visibility.REFERENCE_CANON,
            confidence=Confidence.APPROVED,
            participants=(),
            source_table="approved_canon_registry",
        )

        results = [
            ledger.form_atomic_candidate_from_ledger_entry(
                self.conn,
                person_root,
            ),
            ledger.form_atomic_candidate_from_ledger_entry(
                self.conn,
                event_root,
            ),
            ledger.form_atomic_candidate_from_ledger_entry(
                self.conn,
                open_root,
            ),
            ledger.form_atomic_knowledge_candidate(
                self.conn,
                ledger.AtomicKnowledgeProposal(
                    candidate_type="inference_or_contested_claim",
                    subject_key="discord_user:7",
                    predicate_key="modular_synthesis_motif",
                    meaning=(
                        "Inference: modular synthesis may be a recurring "
                        "topic for this participant."
                    ),
                    root_entry_ids=(inference_root,),
                    participant_keys=("discord_user:7",),
                    epistemic_status="inference",
                    uncertainty_note=(
                        "A single source supports only a tentative inference."
                    ),
                    currentness="uncertain",
                ),
            ),
            ledger.form_atomic_candidate_from_ledger_entry(
                self.conn,
                bnl_role_root,
            ),
        ]

        self.assertEqual(
            {result.candidate_type for result in results},
            {
                "person_role_fact",
                "project_event_or_milestone",
                "open_loop_or_question",
                "inference_or_contested_claim",
            },
        )
        self.assertTrue(all(result.outcome == "created" for result in results))
        bnl_candidate = self.candidate_row(results[-1].candidate_id)
        self.assertEqual(bnl_candidate[0], "person_role_fact")
        self.assertEqual(bnl_candidate[1], ledger.BNL_SUBJECT_KEY)
        self.assertEqual(
            bnl_candidate[7],
            SourceClass.APPROVED_CANON.value,
        )
        self.assertEqual(
            self.conn.execute(
                """
                SELECT COUNT(*) FROM memory_ledger_knowledge_candidates
                WHERE live_eligible<>0 OR promotion_status<>'unpromoted'
                """
            ).fetchone()[0],
            0,
        )

    def test_derivative_lineage_is_retained_without_self_corroboration(self):
        human_root = self.add_root(
            6,
            entry_type="observation",
            predicate_key="conversation",
            value="I keep returning to modular synth patch design.",
            source_class=SourceClass.PUBLIC_OBSERVATION,
            source_role="user",
        )
        derivative = self.add_root(
            7,
            subject_key="moment:one",
            entry_type="shared_moment",
            predicate_key="shared_moment",
            value="BNL summary: a modular synthesis motif.",
            source_class=SourceClass.DERIVED_SUMMARY,
            source_role="derived_assessment",
            confidence=Confidence.APPROVED,
            derived=True,
            projection=True,
            participants=(
                ledger.LedgerParticipant(
                    "discord_user:7",
                    "Crow",
                    "participant",
                    0,
                ),
            ),
            lineage=(("derived_from", human_root),),
            source_table="memory_moment_windows",
        )
        proposal = ledger.AtomicKnowledgeProposal(
            candidate_type="topic_or_motif",
            subject_key="discord_user:7",
            predicate_key="modular_synthesis_motif",
            meaning="Modular synthesis has appeared as a conversation motif.",
            root_entry_ids=(human_root,),
            derivative_entry_ids=(derivative,),
            participant_keys=("discord_user:7",),
            epistemic_status="source_abstraction",
            uncertainty_note="This is a bounded abstraction, not a quote.",
            currentness="historical",
        )

        result = ledger.form_atomic_knowledge_candidate(self.conn, proposal)

        self.assertEqual(result.outcome, "created")
        row = self.candidate_row(result.candidate_id)
        self.assertEqual(row[7], SourceClass.PUBLIC_OBSERVATION.value)
        self.assertEqual(row[8], Confidence.HIGH.value)
        self.assertEqual(row[9:11], (1, 1))
        roots = self.conn.execute(
            """
            SELECT root_entry_id,root_kind,is_independent,lineage_path_json
            FROM memory_ledger_knowledge_roots
            WHERE candidate_id=?
            ORDER BY is_independent DESC,root_entry_id
            """,
            (result.candidate_id,),
        ).fetchall()
        self.assertEqual(roots[0][0:3], (human_root, "human_source", 1))
        self.assertEqual(roots[1][0:3], (derivative, "bnl_derivative", 0))
        self.assertIn(human_root, roots[1][3])

        derivative_only = ledger.form_atomic_knowledge_candidate(
            self.conn,
            ledger.AtomicKnowledgeProposal(
                candidate_type="topic_or_motif",
                subject_key="discord_user:7",
                predicate_key="derivative_only",
                meaning="A derivative cannot establish this.",
                root_entry_ids=(),
                derivative_entry_ids=(derivative,),
                participant_keys=("discord_user:7",),
                epistemic_status="source_abstraction",
                currentness="uncertain",
            ),
        )
        misclassified = ledger.form_atomic_knowledge_candidate(
            self.conn,
            ledger.AtomicKnowledgeProposal(
                candidate_type="topic_or_motif",
                subject_key="discord_user:7",
                predicate_key="misclassified_derivative",
                meaning="A derivative cannot be an independent root.",
                root_entry_ids=(derivative,),
                participant_keys=("discord_user:7",),
                epistemic_status="source_abstraction",
                currentness="uncertain",
            ),
        )
        self.assertEqual(
            derivative_only.reason_code,
            "derivative_only_no_independent_root",
        )
        self.assertEqual(
            misclassified.reason_code,
            "derivative_misclassified_as_independent",
        )

    def test_repetition_and_confidence_never_create_authority(self):
        low_root = self.add_root(
            8,
            value="Crow likes modular synthesis.",
            source_class=SourceClass.PUBLIC_OBSERVATION,
            confidence=Confidence.LOW,
            visibility=Visibility.INTERNAL,
            public_usable=False,
            channel_policy="internal_controlled",
            source_role="user",
        )
        approved_root = self.add_root(
            9,
            value="Crow likes modular synthesis.",
            source_class=SourceClass.PUBLIC_OBSERVATION,
            confidence=Confidence.APPROVED,
            visibility=Visibility.INTERNAL,
            public_usable=False,
            channel_policy="internal_controlled",
            source_role="user",
        )
        results = [
            ledger.form_atomic_candidate_from_ledger_entry(
                self.conn,
                low_root,
            ),
            ledger.form_atomic_candidate_from_ledger_entry(
                self.conn,
                approved_root,
            ),
        ]

        rows = [
            self.candidate_row(result.candidate_id)
            for result in results
        ]
        self.assertEqual(
            [row[7] for row in rows],
            [
                SourceClass.PUBLIC_OBSERVATION.value,
                SourceClass.PUBLIC_OBSERVATION.value,
            ],
        )
        self.assertEqual(
            [row[8] for row in rows],
            [Confidence.LOW.value, Confidence.APPROVED.value],
        )
        self.assertTrue(all(row[6] == Visibility.INTERNAL.value for row in rows))
        self.assertTrue(all(row[9:14] == (1, 0, 1, 0, "unpromoted") for row in rows))

    def test_identity_provenance_visibility_and_authority_fail_closed(self):
        subject_one = self.add_root(10)
        subject_two = self.add_root(
            11,
            subject_key="discord_user:8",
            participants=(
                ledger.LedgerParticipant(
                    "discord_user:8",
                    "Other",
                    "author",
                    0,
                ),
            ),
        )
        other_guild = self.add_root(12, guild_id=2)
        unknown_visibility = self.add_root(
            13,
            visibility=Visibility.UNKNOWN,
            public_usable=False,
            channel_policy="unknown",
        )
        source_blind = self.add_root(
            14,
            source_class=SourceClass.LEGACY_SOURCE_BLIND,
            visibility=Visibility.PRIVATE,
            public_usable=False,
            channel_policy="internal_controlled",
        )

        cross_subject = ledger.form_atomic_knowledge_candidate(
            self.conn,
            ledger.AtomicKnowledgeProposal(
                "topic_or_motif",
                "discord_user:7",
                "mixed_subjects",
                "Subjects must not be combined.",
                (subject_one, subject_two),
                participant_keys=("discord_user:7",),
                epistemic_status="source_abstraction",
                currentness="uncertain",
            ),
        )
        cross_guild = ledger.form_atomic_knowledge_candidate(
            self.conn,
            ledger.AtomicKnowledgeProposal(
                "topic_or_motif",
                "discord_user:7",
                "mixed_guilds",
                "Guild evidence must not be combined.",
                (subject_one, other_guild),
                participant_keys=("discord_user:7",),
                epistemic_status="source_abstraction",
                currentness="uncertain",
            ),
        )
        visibility = ledger.form_atomic_candidate_from_ledger_entry(
            self.conn,
            unknown_visibility,
        )
        authority = ledger.form_atomic_candidate_from_ledger_entry(
            self.conn,
            source_blind,
        )

        self.assertEqual(
            cross_subject.reason_code,
            "subject_root_isolation_failure",
        )
        self.assertEqual(
            cross_guild.reason_code,
            "cross_guild_or_ambiguous_provenance",
        )
        self.assertEqual(
            visibility.reason_code,
            "ambiguous_route_or_policy",
        )
        self.assertEqual(authority.reason_code, "source_blind_provenance")

    def test_participant_scope_mismatch_fails_closed(self):
        one = self.add_root(15)
        two = self.add_root(
            16,
            participants=(
                ledger.LedgerParticipant(
                    "discord_user:7",
                    "Crow",
                    "author",
                    0,
                ),
                ledger.LedgerParticipant(
                    "discord_user:8",
                    "Other",
                    "participant",
                    1,
                ),
            ),
        )
        result = ledger.form_atomic_knowledge_candidate(
            self.conn,
            ledger.AtomicKnowledgeProposal(
                "topic_or_motif",
                "discord_user:7",
                "scope_mismatch",
                "Participant scopes cannot be merged ambiguously.",
                (one, two),
                participant_keys=("discord_user:7",),
                epistemic_status="source_abstraction",
                currentness="uncertain",
            ),
        )
        self.assertEqual(result.reason_code, "participant_scope_mismatch")

    def test_contradictions_and_explicit_supersession_are_diagnosable(self):
        old_root = self.add_root(
            17,
            predicate_key="favorite_color",
            value="green",
        )
        old = ledger.form_atomic_candidate_from_ledger_entry(
            self.conn,
            old_root,
        )
        conflict_root = self.add_root(
            18,
            predicate_key="favorite_color",
            value="blue",
        )
        conflict = ledger.form_atomic_candidate_from_ledger_entry(
            self.conn,
            conflict_root,
        )
        self.assertEqual(conflict.outcome, "contested")
        self.assertEqual(self.candidate_row(old.candidate_id)[5], "contested")
        self.assertEqual(self.candidate_row(conflict.candidate_id)[5], "contested")

        corrected_old_root = self.add_root(
            19,
            predicate_key="favorite_movie",
            value="Hackers",
        )
        corrected_old = ledger.form_atomic_candidate_from_ledger_entry(
            self.conn,
            corrected_old_root,
        )
        replacement_root = self.add_root(
            20,
            predicate_key="favorite_movie",
            value="The Matrix",
            lineage=(
                ("correction_of", corrected_old_root),
                ("supersedes", corrected_old_root),
            ),
        )
        replacement = ledger.form_atomic_candidate_from_ledger_entry(
            self.conn,
            replacement_root,
        )

        self.assertEqual(replacement.outcome, "created")
        replacement_row = self.candidate_row(replacement.candidate_id)
        self.assertEqual(replacement_row[5], "established")
        self.assertEqual(
            replacement_row[15],
            corrected_old.candidate_id,
        )
        self.assertEqual(
            self.candidate_row(corrected_old.candidate_id)[5],
            "superseded",
        )

    def test_same_roots_with_different_meaning_becomes_contested(self):
        root = self.add_root(
            21,
            entry_type="observation",
            predicate_key="conversation",
            value="We discussed an unresolved signal.",
            source_class=SourceClass.PUBLIC_OBSERVATION,
            source_role="user",
        )
        base = dict(
            candidate_type="inference_or_contested_claim",
            subject_key="discord_user:7",
            predicate_key="unresolved_signal",
            root_entry_ids=(root,),
            participant_keys=("discord_user:7",),
            epistemic_status="inference",
            currentness="uncertain",
        )
        first = ledger.form_atomic_knowledge_candidate(
            self.conn,
            ledger.AtomicKnowledgeProposal(
                meaning="Inference: the signal may be recurring.",
                **base,
            ),
        )
        second = ledger.form_atomic_knowledge_candidate(
            self.conn,
            ledger.AtomicKnowledgeProposal(
                meaning="Inference: the signal may be isolated.",
                **base,
            ),
        )
        self.assertEqual(first.outcome, "created")
        self.assertEqual(second.outcome, "contested")
        self.assertEqual(self.candidate_row(first.candidate_id)[5], "contested")
        self.assertEqual(
            self.candidate_row(first.candidate_id)[14],
            "same_roots_meaning_mismatch",
        )

    def test_privacy_source_invalidation_and_deletion_propagate(self):
        privacy_root = self.add_root(22)
        privacy_candidate = ledger.form_atomic_candidate_from_ledger_entry(
            self.conn,
            privacy_root,
        )
        self.conn.execute(
            """
            UPDATE memory_ledger_entries
            SET visibility='private',public_usable=0
            WHERE entry_id=?
            """,
            (privacy_root,),
        )
        privacy_row = self.candidate_row(privacy_candidate.candidate_id)
        self.assertEqual(privacy_row[2], "")
        self.assertEqual(privacy_row[5], "invalidated")
        self.assertEqual(
            privacy_row[14],
            "root_privacy_or_provenance_changed",
        )

        invalid_root = self.add_root(23, predicate_key="favorite_movie")
        invalid_candidate = ledger.form_atomic_candidate_from_ledger_entry(
            self.conn,
            invalid_root,
        )
        self.conn.execute(
            """
            UPDATE memory_ledger_entries
            SET lifecycle_status='quarantined'
            WHERE entry_id=?
            """,
            (invalid_root,),
        )
        self.assertEqual(
            self.candidate_row(invalid_candidate.candidate_id)[5],
            "invalidated",
        )

        deleted_root = self.add_root(24, predicate_key="preferred_name")
        deleted_candidate = ledger.form_atomic_candidate_from_ledger_entry(
            self.conn,
            deleted_root,
        )
        self.conn.execute(
            "DELETE FROM memory_ledger_entries WHERE entry_id=?",
            (deleted_root,),
        )
        deleted_row = self.candidate_row(deleted_candidate.candidate_id)
        self.assertEqual(deleted_row[2], "")
        self.assertEqual(deleted_row[5], "invalidated")
        self.assertEqual(deleted_row[14], "root_deleted")
        self.assertEqual(
            self.conn.execute(
                """
                SELECT root_status
                FROM memory_ledger_knowledge_roots
                WHERE candidate_id=? AND root_entry_id=?
                """,
                (deleted_candidate.candidate_id, deleted_root),
            ).fetchone()[0],
            "deleted",
        )

    def test_member_correction_and_forget_propagate_without_reintroduction(self):
        original = ledger.shadow_first_party_user_fact(
            self.conn,
            row_id=25,
            user_id=7,
            user_name="Crow",
            guild_id=1,
            fact_key="favorite_color",
            fact_value="green",
            channel_name="barcode-bot",
            channel_policy="public_home",
            channel_id=10,
            message_id=1025,
            route_mode="normal_chat",
            observed_at="2026-07-25T00:00:25+00:00",
        )
        original_candidate = ledger.form_atomic_candidate_from_ledger_entry(
            self.conn,
            original.entry_id,
        )
        safe_ref = view_member_memory(
            self.conn,
            guild_id=1,
            user_id=7,
        )[0]["ref"]

        corrected = correct_member_memory(
            self.conn,
            guild_id=1,
            user_id=7,
            safe_ref=safe_ref,
            corrected_text="violet",
        )
        self.assertTrue(corrected["ok"])
        corrected_candidate_id = self.conn.execute(
            """
            SELECT candidate_id
            FROM memory_ledger_knowledge_candidates
            WHERE subject_key='discord_user:7'
              AND normalized_value='violet'
            """
        ).fetchone()[0]
        self.assertEqual(
            self.candidate_row(original_candidate.candidate_id)[5],
            "superseded",
        )
        self.assertEqual(
            self.candidate_row(corrected_candidate_id)[5],
            "established",
        )
        self.assertEqual(
            self.candidate_row(corrected_candidate_id)[15],
            original_candidate.candidate_id,
        )

        forgotten = forget_member_memory(
            self.conn,
            guild_id=1,
            user_id=7,
            safe_ref=safe_ref,
        )
        self.assertTrue(forgotten["ok"])
        rows = self.conn.execute(
            """
            SELECT normalized_value,candidate_state,candidate_eligible,
                   live_eligible
            FROM memory_ledger_knowledge_candidates
            WHERE subject_key='discord_user:7'
            """
        ).fetchall()
        self.assertTrue(rows)
        self.assertTrue(
            all(
                value == ""
                and state in {"invalidated", "superseded"}
                and not candidate_eligible
                and not live_eligible
                for value, state, candidate_eligible, live_eligible in rows
            )
        )
        replay = ledger.form_atomic_candidate_from_ledger_entry(
            self.conn,
            original.entry_id,
        )
        self.assertEqual(
            replay.reason_code,
            "ineligible_root_lifecycle",
        )

    def test_complete_delete_purges_candidate_boundary(self):
        root = self.add_root(26)
        created = ledger.form_atomic_candidate_from_ledger_entry(
            self.conn,
            root,
        )
        self.assertTrue(created)

        result = complete_delete_member_data(
            self.conn,
            guild_id=1,
            user_id=7,
            confirmation="DELETE MY BNL DATA 1",
        )

        self.assertTrue(result["ok"])
        for table in (
            "memory_ledger_knowledge_candidates",
            "memory_ledger_knowledge_roots",
            "memory_ledger_knowledge_participants",
        ):
            self.assertEqual(
                self.conn.execute(
                    f"SELECT COUNT(*) FROM {table}"
                ).fetchone()[0],
                0,
            )
        self.assertEqual(
            self.conn.execute(
                """
                SELECT COUNT(*)
                FROM memory_ledger_knowledge_receipts
                WHERE candidate_id=?
                """,
                (created.candidate_id,),
            ).fetchone()[0],
            0,
        )

    def test_backfill_is_bounded_resumable_and_restart_safe(self):
        fd, path = tempfile.mkstemp(prefix="bnl-atomic-", suffix=".sqlite3")
        os.close(fd)
        try:
            conn = sqlite3.connect(path)
            ledger.ensure_memory_ledger_schema(conn)
            for row_id, predicate in ((27, "favorite_color"), (28, "favorite_movie")):
                entry = ledger.LedgerEntry(
                    guild_id=1,
                    source_table="conversations",
                    source_row_id=row_id,
                    source_revision=str(row_id),
                    source_role="member_self_report",
                    entry_type="preference",
                    subject_key="discord_user:7",
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
                ledger.insert_ledger_entry(conn, entry)
            first = ledger.backfill_atomic_knowledge_candidates(
                conn,
                batch_size=1,
            )
            conn.commit()
            self.assertFalse(first["completed"])
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM memory_ledger_knowledge_candidates"
                ).fetchone()[0],
                1,
            )
            conn.close()

            conn = sqlite3.connect(path)
            second = ledger.backfill_atomic_knowledge_candidates(
                conn,
                batch_size=1,
            )
            third = ledger.backfill_atomic_knowledge_candidates(
                conn,
                batch_size=1,
            )
            fourth = ledger.backfill_atomic_knowledge_candidates(
                conn,
                batch_size=1,
            )
            conn.commit()
            self.assertFalse(second["completed"])
            self.assertTrue(third["completed"])
            self.assertTrue(fourth["completed"])
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM memory_ledger_knowledge_candidates"
                ).fetchone()[0],
                2,
            )
            self.assertEqual(third["counts"]["created"], 2)
            self.assertEqual(third, fourth)
            conn.close()
        finally:
            if os.path.exists(path):
                os.unlink(path)

    def test_moment_candidate_uses_participant_gist_and_actual_roots(self):
        moments.ensure_moment_schema(self.conn)
        source = self.add_root(
            29,
            entry_type="observation",
            predicate_key="conversation",
            value="I keep returning to modular synth patch design.",
            source_class=SourceClass.PUBLIC_OBSERVATION,
            source_role="user",
        )
        canonical = self.add_root(
            30,
            subject_key="moment:moment-one",
            entry_type="shared_moment",
            predicate_key="shared_moment",
            value="A source-bounded Moment abstraction.",
            source_class=SourceClass.DERIVED_SUMMARY,
            source_role="derived_assessment",
            confidence=Confidence.LOW,
            derived=True,
            projection=True,
            participants=(
                ledger.LedgerParticipant(
                    "discord_user:7",
                    "Crow",
                    "participant",
                    0,
                ),
            ),
            lineage=(("derived_from", source),),
            source_table="memory_moment_windows",
        )
        self.conn.execute(
            """
            INSERT INTO memory_moment_windows(
              moment_id,guild_id,channel_id,channel_name,channel_policy,
              route_mode,topic_key,topic_family,window_started_at,
              last_activity_at,lifecycle_status,visibility,public_usable,
              canonical_ledger_entry_id,created_at,updated_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                "moment-one",
                1,
                10,
                "barcode-bot",
                "public_home",
                "normal_chat",
                "modular_synth",
                "music_production",
                "2026-07-25T00:00:29+00:00",
                "2026-07-25T00:00:30+00:00",
                "finalized",
                "public",
                1,
                canonical,
                "now",
                "now",
            ),
        )
        self.conn.execute(
            """
            INSERT INTO memory_moment_contributions(
              moment_id,participant_key,contribution_gist,frame_type,
              source_digest,source_count,gist_version,lifecycle_status,
              public_usable,created_at,updated_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                "moment-one",
                "discord_user:7",
                "The participant returned to modular synth patch design.",
                "observation",
                "digest",
                1,
                "test",
                "review_only",
                1,
                "now",
                "now",
            ),
        )
        self.conn.execute(
            """
            INSERT INTO memory_moment_contribution_sources(
              moment_id,participant_key,ledger_entry_id,gist_version,
              created_at
            ) VALUES(?,?,?,?,?)
            """,
            ("moment-one", "discord_user:7", source, "test", "now"),
        )

        results = ledger.form_atomic_candidates_from_moment(
            self.conn,
            "moment-one",
        )

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].outcome, "created")
        row = self.candidate_row(results[0].candidate_id)
        self.assertEqual(row[0], "topic_or_motif")
        self.assertEqual(row[3], "source_abstraction")
        self.assertEqual(row[9:11], (1, 1))

    def test_rehearsal_and_show_operational_sources_are_excluded(self):
        excluded_values = (
            "Queue rehearsal with a synthetic artist passed.",
            "A test payment was recorded for the wheel spin.",
            "The queue simulation marked a track up next.",
        )
        results = []
        for offset, value in enumerate(excluded_values, start=31):
            root = self.add_root(
                offset,
                subject_key="barcode_network",
                entry_type="event",
                predicate_key=f"operational_event_{offset}",
                value=value,
                source_role="operator",
                participants=(),
                source_table="project_evidence",
            )
            results.append(
                ledger.form_atomic_candidate_from_ledger_entry(
                    self.conn,
                    root,
                )
            )
        ordinary = self.add_root(
            34,
            subject_key="barcode_network",
            entry_type="event",
            predicate_key="shared_brain_checkpoint",
            value="The governed recall canary was production-proven.",
            source_role="operator",
            participants=(),
            source_table="project_evidence",
        )
        ordinary_result = ledger.form_atomic_candidate_from_ledger_entry(
            self.conn,
            ordinary,
        )

        self.assertTrue(
            all(
                result.reason_code
                == "operational_or_rehearsal_source_excluded"
                for result in results
            )
        )
        self.assertEqual(ordinary_result.outcome, "created")

    def test_diagnostics_are_aggregate_and_expose_invariants_not_content(self):
        secret = "candidate-only-diagnostic-secret-417"
        root = self.add_root(35, value="green")
        created = ledger.form_atomic_knowledge_candidate(
            self.conn,
            ledger.AtomicKnowledgeProposal(
                candidate_type="person_role_fact",
                subject_key="discord_user:7",
                predicate_key="favorite_color",
                meaning=secret,
                root_entry_ids=(root,),
                participant_keys=("discord_user:7",),
                epistemic_status="stated",
                currentness="current",
            ),
        )
        self.assertTrue(created)
        ledger.form_atomic_knowledge_candidate(
            self.conn,
            ledger.AtomicKnowledgeProposal(
                candidate_type="topic_or_motif",
                subject_key="unknown",
                predicate_key="ambiguous",
                meaning="This must be rejected.",
                root_entry_ids=(root,),
                epistemic_status="source_abstraction",
                currentness="uncertain",
            ),
        )

        report = ledger.build_memory_ledger_evaluation(
            self.conn,
            guild_id=1,
        )

        self.assertEqual(
            report["knowledgeCandidateSchemaVersion"],
            ledger.ATOMIC_KNOWLEDGE_SCHEMA_VERSION,
        )
        self.assertEqual(
            report["knowledgeCandidateTotalsByType"]["person_role_fact"],
            1,
        )
        self.assertEqual(
            report["knowledgeCandidateTotalsByEpistemicStatus"]["stated"],
            1,
        )
        self.assertEqual(
            report["knowledgeCandidateTotalsByCurrentness"]["current"],
            1,
        )
        self.assertEqual(
            report["knowledgeCandidateTotalsByConfidence"]["high"],
            1,
        )
        self.assertEqual(report["knowledgeCandidateLiveEligibleCount"], 0)
        self.assertEqual(report["knowledgeCandidateOrphanedRoots"], 0)
        self.assertEqual(
            report["knowledgeCandidateParticipantIsolationViolations"],
            0,
        )
        self.assertGreaterEqual(
            report["knowledgeCandidateAmbiguousRejections"],
            1,
        )
        self.assertNotIn(secret, repr(report))

    def test_candidate_table_is_not_a_live_governance_source(self):
        root = self.add_root(
            36,
            predicate_key="favorite_movie",
            value="favorite movie is RootFilm",
        )
        candidate_only_text = "candidate-only phrase 8675309"
        result = ledger.form_atomic_knowledge_candidate(
            self.conn,
            ledger.AtomicKnowledgeProposal(
                candidate_type="person_role_fact",
                subject_key="discord_user:7",
                predicate_key="favorite_movie",
                meaning=candidate_only_text,
                root_entry_ids=(root,),
                participant_keys=("discord_user:7",),
                epistemic_status="stated",
                currentness="current",
            ),
        )
        self.assertTrue(result)

        governed = build_governed_context(
            self.conn,
            GovernanceRequest(
                guild_id=1,
                subject_user_id=7,
                route_mode="normal_chat",
                conversation_surface="test",
                channel_policy="public_home",
                visibility_allowance="public_safe",
                user_text="what is my favorite movie?",
                budget_chars=500,
                allowed_source_classes=(
                    "first_party_record",
                    "owner_correction",
                    "public_observation",
                ),
                now="2026-07-25T01:00:00+00:00",
            ),
        )

        self.assertIn("RootFilm", governed.rendered_context)
        self.assertNotIn(candidate_only_text, governed.rendered_context)


if __name__ == "__main__":
    unittest.main()
