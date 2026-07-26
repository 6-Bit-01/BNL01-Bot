import sqlite3
import unittest
from datetime import datetime, timedelta, timezone

from bnl_canon_source_contract import Confidence, SourceClass, Visibility
import bnl_memory_ledger as ledger


class ProductionShapedMemoryFormationTests(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        ledger.ensure_memory_ledger_schema(self.conn)
        self.env = {
            ledger.MEMORY_LEDGER_SHADOW_ENV: "true",
            ledger.CONVERSATION_MOTIF_FORMATION_ENV: "true",
        }

    def tearDown(self):
        self.conn.close()

    def add_conversation(
        self,
        row_id,
        value,
        observed_at,
        *,
        subject_key="discord_user:7",
        channel_policy="public_home",
        visibility=Visibility.PUBLIC,
        public_usable=True,
        lifecycle_status="active",
    ):
        result = ledger.insert_ledger_entry(
            self.conn,
            ledger.LedgerEntry(
                guild_id=1,
                source_table="conversations",
                source_row_id=row_id,
                source_revision=str(row_id),
                source_role="user",
                entry_type="observation",
                subject_key=subject_key,
                subject_display_name="Crow",
                predicate_key="conversation",
                value=value,
                source_class=SourceClass.PUBLIC_OBSERVATION,
                route_mode="normal_chat",
                channel_id=10,
                channel_name="barcode-bot",
                channel_policy=channel_policy,
                visibility=visibility,
                confidence=Confidence.MEDIUM,
                public_usable=public_usable,
                observed_at=observed_at,
                source_sequence=int(row_id),
                lifecycle_status=lifecycle_status,
                participants=(
                    ledger.LedgerParticipant(
                        subject_key,
                        "Crow",
                        "author",
                        0,
                    ),
                ),
            ),
        )
        self.assertIn(result.outcome, {"inserted", "deduplicated"})
        return result.entry_id

    def candidate_rows(self):
        return self.conn.execute(
            """
            SELECT candidate_id,normalized_value,candidate_state,
                   reinforcement_count,eligible_independent_root_count,
                   retrieval_tags_json
            FROM memory_ledger_knowledge_candidates
            ORDER BY candidate_id
            """
        ).fetchall()

    def add_shadow_conversation(
        self,
        row_id,
        value,
        observed_at,
        *,
        subject_key="discord_user:7",
        environ=None,
    ):
        user_id = int(subject_key.rsplit(":", 1)[-1])
        result = ledger.shadow_conversation_row(
            self.conn,
            row_id=int(row_id),
            user_id=user_id,
            user_name="Crow",
            guild_id=1,
            role="user",
            content=str(value),
            channel_name="barcode-bot",
            channel_policy="public_home",
            channel_id=10,
            message_id=1000 + int(row_id),
            route_mode="normal_chat",
            observed_at=str(observed_at),
            environ=self.env if environ is None else environ,
        )
        self.assertIn(result.outcome, {"inserted", "deduplicated"})
        return result.entry_id

    def test_one_exchange_cannot_create_recurrence_across_bucket_boundary(self):
        self.add_conversation(
            1,
            "I keep fixing the bot code and memory system.",
            "2026-07-25T00:29:00+00:00",
        )
        trigger = self.add_conversation(
            2,
            "The bot code still needs careful testing.",
            "2026-07-25T00:31:00+00:00",
        )

        results = ledger.form_atomic_candidates_from_recurring_conversation(
            self.conn,
            trigger_entry_id=trigger,
            environ=self.env,
        )

        self.assertEqual(results, [])
        self.assertEqual(self.candidate_rows(), [])

    def test_one_exchange_cannot_create_recurrence_across_midnight(self):
        first = self.add_conversation(
            60,
            "I keep fixing the bot code and memory system.",
            "2026-07-24T23:59:00+00:00",
        )
        second = self.add_conversation(
            61,
            "The website code needs another careful deployment pass.",
            "2026-07-25T00:01:00+00:00",
        )

        self.assertEqual(
            ledger.knowledge_occurrence_identity(self.conn, first),
            ledger.knowledge_occurrence_identity(self.conn, second),
        )
        results = ledger.form_atomic_candidates_from_recurring_conversation(
            self.conn,
            trigger_entry_id=second,
            environ=self.env,
        )

        self.assertEqual(results, [])
        self.assertEqual(self.candidate_rows(), [])

    def test_over_sixty_four_continuous_rows_are_not_two_occurrences(self):
        started = datetime(2026, 7, 24, 20, 0, tzinfo=timezone.utc)
        roots = []
        for index in range(70):
            roots.append(
                self.add_conversation(
                    600 + index,
                    (
                        "I keep fixing the bot code and memory system."
                        if index % 2 == 0
                        else "The website deployment code needs testing."
                    ),
                    (started + timedelta(minutes=index)).isoformat(),
                )
            )

        self.assertEqual(
            ledger.knowledge_occurrence_identity(self.conn, roots[-1]),
            "",
        )
        results = ledger.form_atomic_candidates_from_recurring_conversation(
            self.conn,
            trigger_entry_id=roots[-1],
            environ=self.env,
        )

        self.assertEqual(results, [])
        self.assertEqual(self.candidate_rows(), [])

    def test_separate_occurrences_form_one_source_linked_established_motif(self):
        first = self.add_conversation(
            3,
            "I keep fixing the bot code and memory system.",
            "2026-07-24T20:00:00+00:00",
        )
        second = self.add_conversation(
            4,
            "The website code needs another careful test.",
            "2026-07-25T20:00:00+00:00",
        )

        created = ledger.form_atomic_candidates_from_recurring_conversation(
            self.conn,
            trigger_entry_id=second,
            environ=self.env,
        )
        replayed = ledger.form_atomic_candidates_from_recurring_conversation(
            self.conn,
            trigger_entry_id=second,
            environ=self.env,
        )

        self.assertEqual(len(created), 1)
        self.assertEqual(created[0].outcome, "created")
        self.assertEqual(len(replayed), 1)
        self.assertEqual(replayed[0].outcome, "matched_existing")
        rows = self.candidate_rows()
        self.assertEqual(len(rows), 1)
        self.assertIn("software and technical systems", rows[0][1])
        self.assertEqual(rows[0][2:5], ("established", 2, 2))
        self.assertIn("recurring_public_conversation", rows[0][5])
        roots = {
            row[0]
            for row in self.conn.execute(
                """
                SELECT root_entry_id
                FROM memory_ledger_knowledge_roots
                WHERE candidate_id=? AND is_independent=1
                """,
                (rows[0][0],),
            )
        }
        self.assertEqual(roots, {first, second})

    def test_all_bounded_human_roots_survive_one_occurrence_collapse(self):
        first = self.add_conversation(
            30,
            "I keep fixing the bot code and memory system.",
            "2026-07-24T20:00:00+00:00",
        )
        second = self.add_conversation(
            31,
            "The website code needs another careful test.",
            "2026-07-24T20:05:00+00:00",
        )
        third = self.add_conversation(
            32,
            "I am still troubleshooting the bot system.",
            "2026-07-25T20:00:00+00:00",
        )

        created = ledger.form_atomic_candidates_from_recurring_conversation(
            self.conn,
            trigger_entry_id=third,
            environ=self.env,
        )

        self.assertEqual(len(created), 1)
        candidate_id = self.candidate_rows()[0][0]
        roots = {
            row[0]
            for row in self.conn.execute(
                """
                SELECT root_entry_id
                FROM memory_ledger_knowledge_roots
                WHERE candidate_id=? AND is_independent=1
                """,
                (candidate_id,),
            )
        }
        self.assertEqual(roots, {first, second, third})
        lifecycle = self.conn.execute(
            """
            SELECT eligible_independent_root_count,reinforcement_count,
                   duplicate_support_count,candidate_state
            FROM memory_ledger_knowledge_candidates
            WHERE candidate_id=?
            """,
            (candidate_id,),
        ).fetchone()
        self.assertEqual(lifecycle, (3, 2, 1, "established"))

    def test_privacy_loss_immediately_invalidates_recurring_motif(self):
        first = self.add_conversation(
            33,
            "I keep fixing the bot code and memory system.",
            "2026-07-24T20:00:00+00:00",
        )
        second = self.add_conversation(
            34,
            "The website code needs another careful test.",
            "2026-07-24T20:05:00+00:00",
        )
        third = self.add_conversation(
            35,
            "I am still troubleshooting the bot system.",
            "2026-07-25T20:00:00+00:00",
        )
        created = ledger.form_atomic_candidates_from_recurring_conversation(
            self.conn,
            trigger_entry_id=third,
            environ=self.env,
        )
        candidate_id = created[0].candidate_id

        self.conn.execute(
            """
            UPDATE memory_ledger_entries
            SET visibility='private',public_usable=0
            WHERE entry_id=?
            """,
            (first,),
        )
        invalidated = self.conn.execute(
            """
            SELECT candidate_state,candidate_eligible,live_eligible,
                   invalidated_reason
            FROM memory_ledger_knowledge_candidates
            WHERE candidate_id=?
            """,
            (candidate_id,),
        ).fetchone()
        self.assertEqual(
            invalidated,
            (
                "invalidated",
                0,
                0,
                "root_privacy_or_provenance_changed",
            ),
        )

    def test_correction_row_never_counts_as_positive_motif_evidence(self):
        self.add_conversation(
            70,
            "I keep fixing the bot code and memory system.",
            "2026-07-24T20:00:00+00:00",
        )
        correction = self.add_conversation(
            71,
            "Actually, correction: I do not work on bot code anymore.",
            "2026-07-25T20:00:00+00:00",
        )

        results = ledger.form_atomic_candidates_from_recurring_conversation(
            self.conn,
            trigger_entry_id=correction,
            environ=self.env,
        )

        self.assertEqual(results, [])
        self.assertEqual(self.candidate_rows(), [])

    def test_instead_of_comparison_remains_positive_motif_evidence(self):
        first = self.add_shadow_conversation(
            72,
            "I keep making comedy songs instead of generic tracks.",
            "2026-07-24T20:00:00+00:00",
        )
        second = self.add_shadow_conversation(
            73,
            "My music tracks use comedy vocals and a new beat.",
            "2026-07-25T20:00:00+00:00",
        )

        formed = ledger.form_atomic_candidates_from_recurring_conversation(
            self.conn,
            trigger_entry_id=second,
            environ=self.env,
        )

        self.assertEqual(len(formed), 1)
        self.assertEqual(formed[0].outcome, "created")
        rows = self.candidate_rows()
        self.assertEqual(rows[0][2:5], ("established", 2, 2))
        roots = {
            str(row[0])
            for row in self.conn.execute(
                """
                SELECT root_entry_id
                FROM memory_ledger_knowledge_roots
                WHERE candidate_id=? AND is_independent=1
                """,
                (formed[0].candidate_id,),
            )
        }
        self.assertEqual(roots, {first, second})
        self.assertEqual(
            self.conn.execute(
                """
                SELECT COUNT(*)
                FROM memory_ledger_conversation_motif_fences
                WHERE guild_id=1 AND subject_key='discord_user:7'
                """
            ).fetchone()[0],
            0,
        )

    def test_not_that_hedge_remains_positive_motif_evidence(self):
        first = self.add_shadow_conversation(
            720,
            "It's not that I dislike pizza; I still cook it every week.",
            "2026-07-24T20:00:00+00:00",
        )
        second = self.add_shadow_conversation(
            721,
            "The pizza recipe needs another cooking and baking pass.",
            "2026-07-25T20:00:00+00:00",
        )

        formed = ledger.form_atomic_candidates_from_recurring_conversation(
            self.conn,
            trigger_entry_id=second,
            environ=self.env,
        )

        self.assertEqual(len(formed), 1)
        self.assertEqual(formed[0].outcome, "created")
        rows = self.candidate_rows()
        self.assertEqual(rows[0][2:5], ("established", 2, 2))
        roots = {
            str(row[0])
            for row in self.conn.execute(
                """
                SELECT root_entry_id
                FROM memory_ledger_knowledge_roots
                WHERE candidate_id=? AND is_independent=1
                """,
                (formed[0].candidate_id,),
            )
        }
        self.assertEqual(roots, {first, second})
        self.assertEqual(
            self.conn.execute(
                """
                SELECT COUNT(*)
                FROM memory_ledger_conversation_motif_fences
                WHERE guild_id=1 AND subject_key='discord_user:7'
                """
            ).fetchone()[0],
            0,
        )

    def test_unresolved_correction_withholds_named_motif_family(self):
        first = self.add_shadow_conversation(
            74,
            "I keep fixing the bot code and memory system.",
            "2026-07-22T20:00:00+00:00",
        )
        second = self.add_shadow_conversation(
            75,
            "The deployment architecture needs a careful code review.",
            "2026-07-23T20:00:00+00:00",
        )
        created = ledger.form_atomic_candidates_from_recurring_conversation(
            self.conn,
            trigger_entry_id=second,
            environ=self.env,
        )
        self.assertEqual(len(created), 1)

        correction = self.add_shadow_conversation(
            76,
            "Actually, correction: the website release was wrong.",
            "2026-07-24T20:00:00+00:00",
        )

        self.assertEqual(
            self.conn.execute(
                """
                SELECT COUNT(*) FROM memory_ledger_lineage
                WHERE entry_id=? AND lineage_type IN (
                  'correction_of','supersedes'
                )
                """,
                (correction,),
            ).fetchone()[0],
            0,
        )
        self.assertEqual(
            self.conn.execute(
                """
                SELECT candidate_state,candidate_eligible,invalidated_reason
                FROM memory_ledger_knowledge_candidates
                WHERE candidate_id=?
                """,
                (created[0].candidate_id,),
            ).fetchone(),
            (
                "contested",
                0,
                "conversation_motif_correction_unresolved",
            ),
        )
        self.assertEqual(
            self.conn.execute(
                """
                SELECT fence_state,reason_code
                FROM memory_ledger_conversation_motif_fences
                WHERE guild_id=1 AND subject_key='discord_user:7'
                  AND predicate_key='conversation_motif_code_and_systems'
                """
            ).fetchone(),
            ("active", "conversation_motif_correction_unresolved"),
        )
        self.assertEqual(
            ledger.form_atomic_candidates_from_recurring_conversation(
                self.conn,
                trigger_entry_id=correction,
                environ=self.env,
            ),
            [],
        )
        roots = {
            str(row[0])
            for row in self.conn.execute(
                """
                SELECT root_entry_id
                FROM memory_ledger_knowledge_roots
                WHERE candidate_id=?
                """,
                (created[0].candidate_id,),
            )
        }
        self.assertEqual(roots, {first, second})
        self.assertNotIn(correction, roots)

    def test_unique_raw_correction_fences_until_two_new_occurrences(self):
        first = self.add_shadow_conversation(
            80,
            "I keep fixing the bot code and memory system.",
            "2026-07-22T20:00:00+00:00",
        )
        second = self.add_shadow_conversation(
            81,
            "The website code needs another careful deployment pass.",
            "2026-07-23T20:00:00+00:00",
        )
        created = ledger.form_atomic_candidates_from_recurring_conversation(
            self.conn,
            trigger_entry_id=second,
            environ=self.env,
        )
        self.assertEqual(len(created), 1)
        old_candidate_id = created[0].candidate_id

        correction = self.add_shadow_conversation(
            82,
            (
                "Actually, correction: I do not work on website code or "
                "deployment anymore."
            ),
            "2026-07-24T20:00:00+00:00",
        )
        lineages = self.conn.execute(
            """
            SELECT lineage_type,target_entry_id
            FROM memory_ledger_lineage
            WHERE entry_id=?
            ORDER BY lineage_type,target_entry_id
            """,
            (correction,),
        ).fetchall()
        self.assertEqual(
            lineages,
            [("correction_of", second), ("supersedes", second)],
        )
        self.assertEqual(
            self.conn.execute(
                """
                SELECT candidate_state,candidate_eligible
                FROM memory_ledger_knowledge_candidates
                WHERE candidate_id=?
                """,
                (old_candidate_id,),
            ).fetchone(),
            ("superseded", 0),
        )
        self.assertEqual(
            ledger.form_atomic_candidates_from_recurring_conversation(
                self.conn,
                trigger_entry_id=correction,
                environ=self.env,
            ),
            [],
        )

        post_one = self.add_shadow_conversation(
            83,
            "I am discussing bot code architecture again.",
            "2026-07-25T20:00:00+00:00",
        )
        self.assertEqual(
            ledger.form_atomic_candidates_from_recurring_conversation(
                self.conn,
                trigger_entry_id=post_one,
                environ=self.env,
            ),
            [],
        )
        self.assertEqual(
            self.conn.execute(
                """
                SELECT COUNT(*)
                FROM memory_ledger_conversation_motif_fences
                WHERE guild_id=1 AND subject_key='discord_user:7'
                  AND predicate_key='conversation_motif_code_and_systems'
                """
            ).fetchone()[0],
            1,
        )

        post_two = self.add_shadow_conversation(
            84,
            "The website deployment code needs a fresh architecture pass.",
            "2026-07-26T20:00:00+00:00",
        )
        reestablished = (
            ledger.form_atomic_candidates_from_recurring_conversation(
                self.conn,
                trigger_entry_id=post_two,
                environ=self.env,
            )
        )
        self.assertEqual(len(reestablished), 1)
        rows = self.candidate_rows()
        self.assertEqual(len(rows), 2)
        live_rows = [
            row
            for row in rows
            if row[2] not in ledger.KNOWLEDGE_TERMINAL_CANDIDATE_STATES
        ]
        self.assertEqual(len(live_rows), 1)
        self.assertEqual(live_rows[0][2:5], ("established", 2, 2))
        self.assertEqual(
            next(row for row in rows if row[0] == old_candidate_id)[2],
            "superseded",
        )
        self.assertEqual(
            self.conn.execute(
                """
                SELECT fence_state
                FROM memory_ledger_conversation_motif_fences
                WHERE guild_id=1 AND subject_key='discord_user:7'
                  AND predicate_key='conversation_motif_code_and_systems'
                """,
            ).fetchone()[0],
            "satisfied",
        )
        roots = {
            str(row[0])
            for row in self.conn.execute(
                """
                SELECT root_entry_id
                FROM memory_ledger_knowledge_roots
                WHERE candidate_id=? AND is_independent=1
                """,
                (live_rows[0][0],),
            )
        }
        self.assertEqual(roots, {post_one, post_two})
        self.assertNotIn(first, roots)
        self.assertNotIn(second, roots)
        self.assertNotIn(correction, roots)

    def test_ambiguous_raw_correction_never_guesses_and_withholds(self):
        first = self.add_shadow_conversation(
            90,
            "I keep fixing the bot code and memory details.",
            "2026-07-20T20:00:00+00:00",
        )
        second = self.add_shadow_conversation(
            91,
            "I keep testing the bot code and deployment details.",
            "2026-07-21T20:00:00+00:00",
        )
        created = ledger.form_atomic_candidates_from_recurring_conversation(
            self.conn,
            trigger_entry_id=second,
            environ=self.env,
        )
        self.assertEqual(len(created), 1)

        correction = self.add_shadow_conversation(
            92,
            "Actually, correction: the bot code details were wrong.",
            "2026-07-22T20:00:00+00:00",
        )
        self.assertEqual(
            self.conn.execute(
                """
                SELECT COUNT(*) FROM memory_ledger_lineage
                WHERE entry_id=? AND lineage_type IN (
                  'correction_of','supersedes'
                )
                """,
                (correction,),
            ).fetchone()[0],
            0,
        )
        self.assertEqual(
            self.conn.execute(
                """
                SELECT candidate_state,candidate_eligible,invalidated_reason
                FROM memory_ledger_knowledge_candidates
                WHERE candidate_id=?
                """,
                (created[0].candidate_id,),
            ).fetchone(),
            (
                "contested",
                0,
                "conversation_motif_correction_ambiguous",
            ),
        )

        post_one = self.add_shadow_conversation(
            93,
            "I am discussing bot code architecture again today.",
            "2026-07-23T20:00:00+00:00",
        )
        self.assertEqual(
            ledger.form_atomic_candidates_from_recurring_conversation(
                self.conn,
                trigger_entry_id=post_one,
                environ=self.env,
            ),
            [],
        )
        post_two = self.add_shadow_conversation(
            94,
            "The website deployment code has a new architecture plan.",
            "2026-07-24T20:00:00+00:00",
        )
        reestablished = (
            ledger.form_atomic_candidates_from_recurring_conversation(
                self.conn,
                trigger_entry_id=post_two,
                environ=self.env,
            )
        )
        self.assertEqual(len(reestablished), 1)
        self.assertEqual(
            self.candidate_rows()[0][2:5],
            ("established", 2, 2),
        )
        roots = {
            str(row[0])
            for row in self.conn.execute(
                """
                SELECT root_entry_id
                FROM memory_ledger_knowledge_roots
                WHERE is_independent=1
                """
            )
        }
        self.assertEqual(roots, {post_one, post_two})
        self.assertNotIn(first, roots)
        self.assertNotIn(second, roots)
        self.assertNotIn(correction, roots)

    def test_flag_off_raw_correction_is_discovered_before_later_formation(self):
        disabled_env = {
            ledger.MEMORY_LEDGER_SHADOW_ENV: "true",
            ledger.CONVERSATION_MOTIF_FORMATION_ENV: "false",
        }
        first = self.add_shadow_conversation(
            100,
            "I keep fixing the bot code and memory system.",
            "2026-07-20T20:00:00+00:00",
            environ=disabled_env,
        )
        second = self.add_shadow_conversation(
            101,
            "The website deployment code needs an architecture pass.",
            "2026-07-21T20:00:00+00:00",
            environ=disabled_env,
        )
        correction = self.add_shadow_conversation(
            102,
            (
                "Actually, correction: I do not work on website deployment "
                "architecture anymore."
            ),
            "2026-07-22T20:00:00+00:00",
            environ=disabled_env,
        )
        self.assertEqual(
            self.conn.execute(
                """
                SELECT COUNT(*) FROM memory_ledger_lineage
                WHERE entry_id=? AND lineage_type IN (
                  'correction_of','supersedes'
                )
                """,
                (correction,),
            ).fetchone()[0],
            0,
        )
        self.assertEqual(
            self.conn.execute(
                """
                SELECT COUNT(*)
                FROM memory_ledger_conversation_motif_fences
                """
            ).fetchone()[0],
            0,
        )

        self.assertEqual(
            ledger.form_atomic_candidates_from_recurring_conversation(
                self.conn,
                guild_id=1,
                subject_key="discord_user:7",
                environ=self.env,
            ),
            [],
        )
        self.assertEqual(self.candidate_rows(), [])
        self.assertEqual(
            self.conn.execute(
                """
                SELECT fence_state
                FROM memory_ledger_conversation_motif_fences
                WHERE guild_id=1 AND subject_key='discord_user:7'
                  AND predicate_key='conversation_motif_code_and_systems'
                """
            ).fetchone()[0],
            "active",
        )

        post_one = self.add_shadow_conversation(
            103,
            "I am discussing bot code architecture again today.",
            "2026-07-23T20:00:00+00:00",
        )
        self.assertEqual(
            ledger.form_atomic_candidates_from_recurring_conversation(
                self.conn,
                trigger_entry_id=post_one,
                environ=self.env,
            ),
            [],
        )
        post_two = self.add_shadow_conversation(
            104,
            "The website deployment code has a new architecture plan.",
            "2026-07-24T20:00:00+00:00",
        )
        formed = ledger.form_atomic_candidates_from_recurring_conversation(
            self.conn,
            trigger_entry_id=post_two,
            environ=self.env,
        )

        self.assertEqual(len(formed), 1)
        rows = self.candidate_rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][2:5], ("established", 2, 2))
        retained_roots = {
            str(row[0])
            for row in self.conn.execute(
                """
                SELECT root_entry_id
                FROM memory_ledger_knowledge_roots
                WHERE candidate_id=? AND is_independent=1
                """,
                (rows[0][0],),
            )
        }
        self.assertEqual(retained_roots, {post_one, post_two})
        self.assertTrue({first, second, correction}.isdisjoint(retained_roots))
        self.assertEqual(
            self.conn.execute(
                """
                SELECT fence_state
                FROM memory_ledger_conversation_motif_fences
                WHERE guild_id=1 AND subject_key='discord_user:7'
                  AND predicate_key='conversation_motif_code_and_systems'
                """
            ).fetchone()[0],
            "satisfied",
        )

    def test_one_moment_counts_as_one_occurrence(self):
        first = self.add_conversation(
            5,
            "I keep working on visual art and character design.",
            "2026-07-25T10:00:00+00:00",
        )
        second = self.add_conversation(
            6,
            "The character artwork needs a different visual style.",
            "2026-07-25T11:00:00+00:00",
        )
        moment = ledger.insert_ledger_entry(
            self.conn,
            ledger.LedgerEntry(
                guild_id=1,
                source_table="memory_moment_windows",
                source_row_id="one-moment",
                source_revision="1",
                source_role="derived_assessment",
                entry_type="shared_moment",
                subject_key="moment:one-moment",
                predicate_key="shared_moment",
                value="Derived moment gist.",
                source_class=SourceClass.DERIVED_SUMMARY,
                route_mode="normal_chat",
                channel_id=10,
                channel_name="barcode-bot",
                channel_policy="public_home",
                visibility=Visibility.PUBLIC,
                confidence=Confidence.LOW,
                public_usable=True,
                derived=True,
                projection=True,
                observed_at="2026-07-25T11:00:00+00:00",
                lifecycle_status="review_only",
                participants=(
                    ledger.LedgerParticipant(
                        "discord_user:7",
                        "Crow",
                        "participant",
                        0,
                    ),
                ),
                lineage=(
                    ("derived_from", first),
                    ("derived_from", second),
                ),
            ),
        )
        for root in (first, second):
            self.conn.execute(
                """
                INSERT OR IGNORE INTO memory_ledger_lineage(
                  entry_id,guild_id,lineage_type,target_entry_id,created_at
                ) VALUES(?,?,?,?,?)
                """,
                (root, 1, "part_of_moment", moment.entry_id, "now"),
            )

        results = ledger.form_atomic_candidates_from_recurring_conversation(
            self.conn,
            trigger_entry_id=second,
            environ=self.env,
        )

        self.assertEqual(results, [])
        self.assertEqual(self.candidate_rows(), [])

    def test_recall_sensitive_and_nonpublic_rows_never_form_topics(self):
        rows = (
            (
                7,
                "What do you remember about me and my code projects?",
                "2026-07-22T10:00:00+00:00",
                "public_home",
                Visibility.PUBLIC,
                True,
            ),
            (
                8,
                "My therapy and medication affect the art I make.",
                "2026-07-23T10:00:00+00:00",
                "public_home",
                Visibility.PUBLIC,
                True,
            ),
            (
                9,
                "I keep fixing private bot code.",
                "2026-07-24T10:00:00+00:00",
                "sealed_test",
                Visibility.SEALED_TEST,
                False,
            ),
        )
        trigger = ""
        for row in rows:
            trigger = self.add_conversation(
                row[0],
                row[1],
                row[2],
                channel_policy=row[3],
                visibility=row[4],
                public_usable=row[5],
            )

        results = ledger.form_atomic_candidates_from_recurring_conversation(
            self.conn,
            guild_id=1,
            subject_key="discord_user:7",
            environ=self.env,
        )

        self.assertEqual(results, [])
        self.assertEqual(self.candidate_rows(), [])

    def test_repeated_generic_markers_do_not_become_false_motifs(self):
        rows = (
            (
                50,
                "The test results need another careful review.",
                "2026-07-20T10:00:00+00:00",
            ),
            (
                51,
                "That test result still needs checking.",
                "2026-07-21T10:00:00+00:00",
            ),
            (
                52,
                "The character design style changed again.",
                "2026-07-22T10:00:00+00:00",
            ),
            (
                53,
                "That character style needs a different design.",
                "2026-07-23T10:00:00+00:00",
            ),
            (
                54,
                "This project needs another planning pass.",
                "2026-07-24T10:00:00+00:00",
            ),
            (
                55,
                "The project plan still needs review.",
                "2026-07-25T10:00:00+00:00",
            ),
        )
        for row_id, value, observed_at in rows:
            self.add_conversation(row_id, value, observed_at)

        results = ledger.form_atomic_candidates_from_recurring_conversation(
            self.conn,
            guild_id=1,
            subject_key="discord_user:7",
            environ=self.env,
        )

        self.assertEqual(results, [])
        self.assertEqual(self.candidate_rows(), [])

    def test_incremental_motif_refresh_stays_one_candidate_and_twelve_roots(self):
        roots = []
        last_trigger = ""
        stable_candidate_id = ""
        initial_lifecycle_event_ids = set()
        for index in range(20):
            last_trigger = self.add_conversation(
                200 + index,
                (
                    "I keep fixing the bot code and memory system."
                    if index % 2 == 0
                    else (
                        "The website deployment code needs another "
                        "architecture pass."
                    )
                ),
                "2026-06-%02dT20:00:00+00:00" % (index + 1),
            )
            roots.append(last_trigger)
            formed = ledger.form_atomic_candidates_from_recurring_conversation(
                self.conn,
                trigger_entry_id=last_trigger,
                environ=self.env,
            )
            if formed:
                if not stable_candidate_id:
                    stable_candidate_id = formed[0].candidate_id
                self.assertEqual(formed[0].candidate_id, stable_candidate_id)
            if index == 1:
                initial_lifecycle_event_ids = {
                    str(row[0])
                    for row in self.conn.execute(
                        """
                        SELECT event_id
                        FROM memory_ledger_knowledge_lifecycle_events
                        """
                    )
                }

        for _replay in range(3):
            replay = (
                ledger.form_atomic_candidates_from_recurring_conversation(
                    self.conn,
                    trigger_entry_id=last_trigger,
                    environ=self.env,
                )
            )
            self.assertEqual(len(replay), 1)
            self.assertEqual(replay[0].outcome, "matched_existing")

        rows = self.candidate_rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][2:5], ("established", 12, 12))
        candidate_id = rows[0][0]
        retained_roots = {
            str(row[0])
            for row in self.conn.execute(
                """
                SELECT root_entry_id
                FROM memory_ledger_knowledge_roots
                WHERE candidate_id=? AND is_independent=1
                """,
                (candidate_id,),
            )
        }
        self.assertEqual(retained_roots, set(roots[-12:]))
        final_lifecycle_event_ids = {
            str(row[0])
            for row in self.conn.execute(
                """
                SELECT event_id
                FROM memory_ledger_knowledge_lifecycle_events
                """
            )
        }
        self.assertTrue(initial_lifecycle_event_ids)
        self.assertTrue(
            initial_lifecycle_event_ids.issubset(final_lifecycle_event_ids)
        )
        self.assertGreater(
            len(final_lifecycle_event_ids),
            len(initial_lifecycle_event_ids),
        )
        self.assertGreater(
            self.conn.execute(
                """
                SELECT COUNT(*)
                FROM memory_ledger_knowledge_receipts
                WHERE candidate_id=? AND event_type='refreshed'
                """
                ,
                (candidate_id,),
            ).fetchone()[0],
            0,
        )
        self.assertEqual(
            self.conn.execute(
                """
                SELECT COUNT(DISTINCT candidate_id)
                FROM memory_ledger_knowledge_receipts
                WHERE COALESCE(candidate_id,'')<>''
                  AND candidate_type='topic_or_motif'
                """
            ).fetchone()[0],
            1,
        )

    def test_feature_switch_and_subject_isolation_fail_closed(self):
        self.add_conversation(
            10,
            "I keep building radio music tracks.",
            "2026-07-24T10:00:00+00:00",
            subject_key="discord_user:7",
        )
        other = self.add_conversation(
            11,
            "I keep building radio music tracks.",
            "2026-07-25T10:00:00+00:00",
            subject_key="discord_user:8",
        )

        disabled = ledger.form_atomic_candidates_from_recurring_conversation(
            self.conn,
            trigger_entry_id=other,
            environ={
                ledger.MEMORY_LEDGER_SHADOW_ENV: "true",
                ledger.CONVERSATION_MOTIF_FORMATION_ENV: "false",
            },
        )
        enabled = ledger.form_atomic_candidates_from_recurring_conversation(
            self.conn,
            trigger_entry_id=other,
            environ=self.env,
        )

        self.assertEqual(disabled, [])
        self.assertEqual(enabled, [])
        self.assertEqual(self.candidate_rows(), [])

    def test_formation_requires_its_own_explicit_shadow_switch(self):
        self.add_conversation(
            40,
            "I keep building radio music tracks.",
            "2026-07-24T10:00:00+00:00",
        )
        trigger = self.add_conversation(
            41,
            "The music mix needs another production pass.",
            "2026-07-25T10:00:00+00:00",
        )

        results = ledger.form_atomic_candidates_from_recurring_conversation(
            self.conn,
            trigger_entry_id=trigger,
            environ={ledger.MEMORY_LEDGER_SHADOW_ENV: "true"},
        )

        self.assertEqual(results, [])
        self.assertEqual(self.candidate_rows(), [])


if __name__ == "__main__":
    unittest.main()
