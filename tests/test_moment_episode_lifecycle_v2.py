import os
import sqlite3
import unittest
from datetime import datetime, timedelta, timezone

import bnl_memory_ledger as ledger
import bnl_moment_engine as moments


class MomentEpisodeLifecycleV2Tests(unittest.TestCase):
    def setUp(self):
        self.original_gate_values = {
            key: os.environ.get(key)
            for key in (
                "BNL_MEMORY_LEDGER_SHADOW_ENABLED",
                "BNL_MOMENT_ENGINE_SHADOW_ENABLED",
            )
        }
        os.environ["BNL_MEMORY_LEDGER_SHADOW_ENABLED"] = "1"
        os.environ["BNL_MOMENT_ENGINE_SHADOW_ENABLED"] = "1"
        self.conn = sqlite3.connect(":memory:")
        ledger.ensure_memory_ledger_schema(self.conn)
        moments.ensure_moment_schema(self.conn)

    def tearDown(self):
        self.conn.close()
        for key, value in self.original_gate_values.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    @staticmethod
    def timestamp(*, hours=0, minutes=0):
        return (
            datetime(2026, 1, 1, tzinfo=timezone.utc)
            + timedelta(hours=hours, minutes=minutes)
        ).isoformat()

    def add(
        self,
        row_id,
        user_id,
        text,
        *,
        hours=0,
        minutes=0,
        channel_id=10,
        policy="public_home",
        role="user",
        name="",
    ):
        result = ledger.shadow_conversation_row(
            self.conn,
            row_id=row_id,
            user_id=user_id,
            user_name=name or "Member %s" % user_id,
            guild_id=1,
            role=role,
            content=text,
            channel_policy=policy,
            channel_id=channel_id,
            channel_name="channel-%s" % channel_id,
            route_mode="normal_chat",
            observed_at=self.timestamp(hours=hours, minutes=minutes),
        )
        moments.observe_ledger_entry(self.conn, result.entry_id)
        return result

    def finalize_shared_moment(
        self,
        row_id,
        messages,
        *,
        hours=0,
        minutes=0,
        channel_id=10,
        users=(1, 2, 1),
        policy="public_home",
    ):
        sources = []
        for offset, text in enumerate(messages):
            sources.append(
                self.add(
                    row_id + offset,
                    users[offset],
                    text,
                    hours=hours,
                    minutes=minutes,
                    channel_id=channel_id,
                    policy=policy,
                )
            )
        moments.sweep_expired_windows(
            self.conn,
            now=self.timestamp(hours=hours, minutes=minutes + 3),
        )
        moment_id = self.conn.execute(
            """
            SELECT moment_id FROM memory_moment_windows
            WHERE channel_id=? AND lifecycle_status='finalized'
            ORDER BY window_started_at DESC,moment_id LIMIT 1
            """,
            (channel_id,),
        ).fetchone()[0]
        return moment_id, tuple(sources)

    def test_sealed_canary_renders_only_revalidated_aggregate_episode(self):
        _moment_id, sources = self.finalize_shared_moment(
            95,
            (
                "Let's build the synth routing for the chorus",
                "The synth drum patch needs a bass answer",
                "Which synth layer should we test next?",
            ),
            users=(1, 2, 3),
            policy="sealed_test",
        )
        rendered = moments.render_active_episode_canary_context(
            self.conn,
            guild_id=1,
            channel_id=10,
            channel_policy="sealed_test",
            route_mode="normal_chat",
            topic_text="How should we continue the synth routing?",
            participant_keys=("discord_user:1",),
            now=self.timestamp(minutes=4),
        )
        self.assertIn("Active same-channel episode signal", rendered)
        self.assertIn("Shared human participants: 3", rendered)
        self.assertIn("Unresolved open loops:", rendered)
        for forbidden in (
            "Member 1",
            "Member 2",
            "Member 3",
            "chorus",
            "bass answer",
            "mep_",
            "mm_",
        ):
            self.assertNotIn(forbidden, rendered)

        self.assertEqual(
            moments.render_active_episode_canary_context(
                self.conn,
                guild_id=1,
                channel_id=10,
                channel_policy="public_home",
                route_mode="normal_chat",
                topic_text="synth routing",
                now=self.timestamp(minutes=4),
            ),
            "",
        )

        self.conn.execute(
            """
            UPDATE memory_ledger_entries
            SET lifecycle_status='retracted'
            WHERE entry_id=?
            """,
            (sources[0].entry_id,),
        )
        self.assertEqual(
            moments.render_active_episode_canary_context(
                self.conn,
                guild_id=1,
                channel_id=10,
                channel_policy="sealed_test",
                route_mode="normal_chat",
                topic_text="How should we continue the synth routing?",
                participant_keys=("discord_user:1",),
                now=self.timestamp(minutes=4),
            ),
            "",
        )

    def test_coherent_moments_extend_one_shared_episode_with_any_participant_count(self):
        first_moment, _ = self.finalize_shared_moment(
            100,
            (
                "Let's build the synth routing and test the chorus",
                "Member Two will handle the synth drum patch",
                "Which synth bass layer should remain open?",
            ),
            users=(1, 2, 3),
        )
        second_moment, _ = self.finalize_shared_moment(
            110,
            (
                "I agree the synth routing should keep the chorus",
                "We decided to choose the warmer synth bass patch",
                "The synth drum patch still needs a final test",
            ),
            minutes=10,
            users=(4, 5, 1),
        )

        episode = self.conn.execute(
            """
            SELECT episode_id,lifecycle_status,moment_count,participant_count,
                   semantic_types_json,action_count,reaction_count,
                   decision_count,assignment_count,open_loop_count
            FROM memory_moment_episodes
            """
        ).fetchone()
        self.assertIsNotNone(episode)
        self.assertEqual(episode[1], "active")
        self.assertEqual(episode[2], 2)
        self.assertEqual(episode[3], 5)
        self.assertGreaterEqual(episode[5], 1)
        self.assertGreaterEqual(episode[6], 1)
        self.assertGreaterEqual(episode[7], 1)
        self.assertGreaterEqual(episode[8], 1)
        self.assertGreaterEqual(episode[9], 1)
        self.assertIn('"action"', episode[4])
        self.assertIn('"open_loop"', episode[4])
        self.assertEqual(
            self.conn.execute(
                """
                SELECT COUNT(*) FROM memory_moment_episode_moments
                WHERE episode_id=?
                """,
                (episode[0],),
            ).fetchone()[0],
            2,
        )
        self.assertEqual(
            {
                row[0]
                for row in self.conn.execute(
                    """
                    SELECT moment_id FROM memory_moment_episode_moments
                    WHERE episode_id=?
                    """,
                    (episode[0],),
                )
            },
            {first_moment, second_moment},
        )
        self.assertEqual(
            self.conn.execute(
                "SELECT COUNT(*) FROM memory_moment_episodes"
            ).fetchone()[0],
            1,
        )

    def test_topic_change_splits_and_preserves_interruption_lineage(self):
        self.finalize_shared_moment(
            200,
            (
                "Let's build the synth routing for the chorus",
                "The synth drum patch needs a bass answer",
                "Which synth layer should we test next?",
            ),
        )
        old_episode_id = self.conn.execute(
            "SELECT episode_id FROM memory_moment_episodes"
        ).fetchone()[0]

        new_moment_id, new_sources = self.finalize_shared_moment(
            210,
            (
                "Pizza dough needs a hotter oven stone",
                "Pizza sauce works with the dough structure",
                "The pizza oven should keep the crust crisp",
            ),
            minutes=10,
        )
        episodes = self.conn.execute(
            """
            SELECT episode_id,lifecycle_status,finalization_reason
            FROM memory_moment_episodes ORDER BY opened_at,episode_id
            """
        ).fetchall()
        self.assertEqual(len(episodes), 2)
        old = next(row for row in episodes if row[0] == old_episode_id)
        new = next(row for row in episodes if row[0] != old_episode_id)
        self.assertEqual(old[1:], ("finalized", "topic_interruption"))
        self.assertEqual(new[1], "active")
        self.assertEqual(
            self.conn.execute(
                """
                SELECT relation_type,evidence_moment_id,evidence_entry_id
                FROM memory_moment_episode_lineage
                WHERE from_episode_id=? AND to_episode_id=?
                """,
                (new[0], old[0]),
            ).fetchone(),
            ("interrupted_from", new_moment_id, new_sources[0].entry_id),
        )

    def test_unique_explicit_resume_reopens_source_backed_episode(self):
        first_moment, _ = self.finalize_shared_moment(
            300,
            (
                "The synth routing should keep the chorus wide",
                "The synth drum patch can answer the bass",
                "Which synth layer should we revisit?",
            ),
        )
        episode_id = self.conn.execute(
            "SELECT episode_id FROM memory_moment_episodes"
        ).fetchone()[0]
        moments.sweep_expired_episodes(
            self.conn,
            now=self.timestamp(hours=25),
        )
        self.assertEqual(
            self.conn.execute(
                """
                SELECT lifecycle_status FROM memory_moment_episodes
                WHERE episode_id=?
                """,
                (episode_id,),
            ).fetchone()[0],
            "finalized",
        )

        resumed_moment, _ = self.finalize_shared_moment(
            310,
            (
                "Let's return to the synth routing and chorus",
                "The synth drum patch still fits the bass",
                "We should continue the synth layer test",
            ),
            hours=48,
        )
        episode = self.conn.execute(
            """
            SELECT lifecycle_status,reopen_count,moment_count
            FROM memory_moment_episodes WHERE episode_id=?
            """,
            (episode_id,),
        ).fetchone()
        self.assertEqual(episode, ("active", 1, 2))
        self.assertEqual(
            self.conn.execute(
                "SELECT COUNT(*) FROM memory_moment_episodes"
            ).fetchone()[0],
            1,
        )
        self.assertEqual(
            self.conn.execute(
                """
                SELECT link_role FROM memory_moment_episode_moments
                WHERE episode_id=? AND moment_id=?
                """,
                (episode_id, resumed_moment),
            ).fetchone()[0],
            "reopened",
        )
        self.assertEqual(
            {
                row[0]
                for row in self.conn.execute(
                    """
                    SELECT moment_id FROM memory_moment_episode_moments
                    WHERE episode_id=?
                    """,
                    (episode_id,),
                )
            },
            {first_moment, resumed_moment},
        )

    def test_ambiguous_resume_opens_new_episode_without_guessing(self):
        self.finalize_shared_moment(
            400,
            (
                "The synth routing should keep the chorus wide",
                "The synth drum patch can answer the bass",
                "The synth layer still needs a decision",
            ),
        )
        moments.sweep_expired_episodes(
            self.conn,
            now=self.timestamp(hours=25),
        )
        self.finalize_shared_moment(
            410,
            (
                "The synth routing has a different chorus shape",
                "The synth drum patch follows that bass",
                "The synth layer needs another test",
            ),
            hours=48,
        )
        moments.sweep_expired_episodes(
            self.conn,
            now=self.timestamp(hours=73),
        )
        self.finalize_shared_moment(
            420,
            (
                "Let's return to the synth routing and chorus",
                "The synth drum patch still follows the bass",
                "We should continue the synth layer test",
            ),
            hours=96,
        )

        self.assertEqual(
            self.conn.execute(
                "SELECT COUNT(*) FROM memory_moment_episodes"
            ).fetchone()[0],
            3,
        )
        self.assertEqual(
            self.conn.execute(
                """
                SELECT COALESCE(SUM(reopen_count),0)
                FROM memory_moment_episodes
                """
            ).fetchone()[0],
            0,
        )
        self.assertEqual(
            self.conn.execute(
                """
                SELECT COUNT(*) FROM memory_moment_diagnostics
                WHERE event_type='episode_reopen_skipped'
                  AND reason_code='ambiguous_reopen_candidates'
                """
            ).fetchone()[0],
            1,
        )

    def test_explicit_unique_related_source_links_distinct_episodes(self):
        self.finalize_shared_moment(
            450,
            (
                "The synth routing should keep the chorus wide",
                "The synth drum patch can answer the bass",
                "The synth layer still needs a decision",
            ),
        )
        first_episode_id = self.conn.execute(
            "SELECT episode_id FROM memory_moment_episodes"
        ).fetchone()[0]
        moments.sweep_expired_episodes(
            self.conn,
            now=self.timestamp(hours=25),
        )
        related_moment_id, sources = self.finalize_shared_moment(
            460,
            (
                "Let's connect this pizza discussion to the earlier plan",
                "Pizza dough needs a hotter oven stone",
                "Pizza sauce works with the dough structure",
            ),
            hours=48,
        )
        second_episode_id = self.conn.execute(
            """
            SELECT episode_id FROM memory_moment_episodes
            WHERE episode_id<>?
            """,
            (first_episode_id,),
        ).fetchone()[0]

        self.assertEqual(
            self.conn.execute(
                """
                SELECT relation_type,evidence_moment_id,evidence_entry_id
                FROM memory_moment_episode_lineage
                WHERE from_episode_id=? AND to_episode_id=?
                """,
                (second_episode_id, first_episode_id),
            ).fetchone(),
            ("related_to", related_moment_id, sources[0].entry_id),
        )

    def test_explicit_outcome_finalizes_episode(self):
        self.finalize_shared_moment(
            500,
            (
                "The synth deployment tests passed and this is done",
                "The synth routing fix is complete",
                "The chorus patch is resolved",
            ),
        )
        self.assertEqual(
            self.conn.execute(
                """
                SELECT lifecycle_status,finalization_reason,outcome_count,
                       open_loop_count
                FROM memory_moment_episodes
                """
            ).fetchone(),
            ("finalized", "explicit_outcome", 3, 0),
        )

    def test_negated_outcomes_do_not_finalize_episode(self):
        self.finalize_shared_moment(
            550,
            (
                "The synth fix is not deployed",
                "The synth patch is not finished",
                "The synth chorus test has not passed",
            ),
        )
        self.assertEqual(
            self.conn.execute(
                """
                SELECT lifecycle_status,outcome_count
                FROM memory_moment_episodes
                """
            ).fetchone(),
            ("active", 0),
        )

    def test_correction_and_source_deletion_invalidate_episode(self):
        _moment_id, sources = self.finalize_shared_moment(
            600,
            (
                "The synth routing should keep the chorus wide",
                "The synth drum patch can answer the bass",
                "Which synth layer should we test?",
            ),
        )
        episode_id = self.conn.execute(
            "SELECT episode_id FROM memory_moment_episodes"
        ).fetchone()[0]
        correction = ledger.insert_ledger_entry(
            self.conn,
            ledger.LedgerEntry(
                guild_id=1,
                source_table="member_memory_controls",
                source_row_id="correction-1",
                source_revision="1",
                source_role="member_control",
                entry_type="boundary",
                subject_key="discord_user:1",
                subject_display_name="Member 1",
                predicate_key="source_correction",
                value="The earlier source was corrected by its author.",
                source_class=ledger.SourceClass.FIRST_PARTY_RECORD,
                route_mode="normal_chat",
                channel_id=10,
                channel_name="channel-10",
                channel_policy="public_home",
                visibility=ledger.Visibility.PUBLIC,
                confidence=ledger.Confidence.HIGH,
                observed_at=self.timestamp(minutes=5),
                source_sequence=999,
                lineage=(
                    ("correction_of", sources[0].entry_id),
                    ("supersedes", sources[0].entry_id),
                ),
            ),
        )
        moments.observe_ledger_entry(self.conn, correction.entry_id)
        self.assertEqual(
            self.conn.execute(
                """
                SELECT lifecycle_status FROM memory_moment_episodes
                WHERE episode_id=?
                """,
                (episode_id,),
            ).fetchone()[0],
            "needs_review",
        )
        self.assertIsNone(
            moments.active_episode_for_assessment(
                self.conn,
                guild_id=1,
                channel_id=10,
                channel_policy="public_home",
                route_mode="normal_chat",
                topic_text="synth routing chorus",
                participant_keys=("discord_user:1",),
            )
        )

        self.conn.close()
        self.conn = sqlite3.connect(":memory:")
        ledger.ensure_memory_ledger_schema(self.conn)
        moments.ensure_moment_schema(self.conn)
        _moment_id, sources = self.finalize_shared_moment(
            610,
            (
                "Pizza dough needs a hotter oven stone",
                "Pizza sauce works with the dough structure",
                "Which pizza crust should we test?",
            ),
        )
        episode_id = self.conn.execute(
            "SELECT episode_id FROM memory_moment_episodes"
        ).fetchone()[0]
        self.conn.execute(
            "DELETE FROM memory_ledger_entries WHERE entry_id=?",
            (sources[0].entry_id,),
        )
        self.assertEqual(
            self.conn.execute(
                """
                SELECT lifecycle_status FROM memory_moment_episodes
                WHERE episode_id=?
                """,
                (episode_id,),
            ).fetchone()[0],
            "needs_review",
        )
        report = moments.build_moment_evaluation_report(
            self.conn,
            guild_id=1,
        )
        self.assertEqual(report["episode_orphaned_source_links"], 0)

    def test_active_episode_reference_is_opaque_and_scope_bounded(self):
        moment_id, _ = self.finalize_shared_moment(
            700,
            (
                "The synth routing should keep the chorus wide",
                "The synth drum patch can answer the bass",
                "Which synth layer should we test?",
            ),
            users=(1, 2, 3),
        )
        reference = moments.active_episode_for_assessment(
            self.conn,
            guild_id=1,
            channel_id=10,
            channel_policy="public_home",
            route_mode="normal_chat",
            topic_text="synth routing chorus",
            participant_keys=("discord_user:2",),
            now=self.timestamp(minutes=4),
        )
        self.assertIsNotNone(reference)
        self.assertTrue(reference.episode_id.startswith("mep_"))
        self.assertEqual(reference.source_moment_ids, (moment_id,))
        self.assertEqual(reference.participant_count, 3)
        self.assertNotIn("synth", repr(reference).lower())
        for changed in (
            {"channel_id": 99},
            {"topic_text": "pizza dough oven"},
            {"participant_keys": ("discord_user:999",)},
        ):
            kwargs = {
                "guild_id": 1,
                "channel_id": 10,
                "channel_policy": "public_home",
                "route_mode": "normal_chat",
                "topic_text": "synth routing chorus",
                "participant_keys": ("discord_user:2",),
                "now": self.timestamp(minutes=4),
            }
            kwargs.update(changed)
            self.assertIsNone(
                moments.active_episode_for_assessment(self.conn, **kwargs)
            )
        self.assertIsNone(
            moments.active_episode_for_assessment(
                self.conn,
                guild_id=1,
                channel_id=10,
                channel_policy="public_home",
                route_mode="normal_chat",
                topic_text="synth routing chorus",
                participant_keys=("discord_user:2",),
                now=self.timestamp(hours=25),
            )
        )

    def test_backfill_runs_even_when_shadow_gate_is_currently_disabled(self):
        moment_id, _ = self.finalize_shared_moment(
            750,
            (
                "The synth routing should keep the chorus wide",
                "The synth drum patch needs a bass answer",
                "Which synth layer should we test?",
            ),
        )
        self.conn.execute("DELETE FROM memory_moment_episode_events")
        self.conn.execute("DELETE FROM memory_moment_episode_participants")
        self.conn.execute("DELETE FROM memory_moment_episode_moments")
        self.conn.execute("DELETE FROM memory_moment_episode_lineage")
        self.conn.execute("DELETE FROM memory_moment_episodes")
        os.environ.pop("BNL_MOMENT_ENGINE_SHADOW_ENABLED", None)

        report = moments.backfill_episodic_lifecycle(self.conn)

        self.assertEqual(report["errors"], 0)
        self.assertEqual(report["observed"], 1)
        self.assertEqual(
            self.conn.execute(
                """
                SELECT moment_id FROM memory_moment_episode_moments
                """
            ).fetchall(),
            [(moment_id,)],
        )

    def test_report_exposes_content_free_lifecycle_and_zero_invariants(self):
        self.finalize_shared_moment(
            800,
            (
                "Let's build the synth routing for the chorus",
                "The synth drum patch needs a bass answer",
                "Which synth layer should we test next?",
            ),
        )
        report = moments.build_moment_evaluation_report(
            self.conn,
            guild_id=1,
        )
        self.assertTrue(report["episode_schema_present"])
        self.assertEqual(report["active_episodes"], 1)
        self.assertEqual(report["episode_moment_links"], 1)
        self.assertGreaterEqual(report["episode_source_links"], 1)
        self.assertIn("open_loop", report["episode_events_by_type"])
        for key in (
            "episode_processing_errors",
            "episode_duplicate_moment_links",
            "episode_active_scope_duplicates",
            "episode_cross_scope_violations",
            "episode_orphaned_moment_links",
            "episode_orphaned_source_links",
            "episode_participant_link_violations",
        ):
            self.assertEqual(report[key], 0, key)
        episode_tables = (
            "memory_moment_episodes",
            "memory_moment_episode_moments",
            "memory_moment_episode_participants",
            "memory_moment_episode_events",
            "memory_moment_episode_lineage",
        )
        forbidden_columns = {
            "content",
            "normalized_value",
            "summary",
            "gist",
            "quote",
            "transcript",
        }
        for table in episode_tables:
            columns = {
                str(row[1])
                for row in self.conn.execute(
                    "PRAGMA table_info(%s)" % table
                ).fetchall()
            }
            self.assertFalse(columns.intersection(forbidden_columns), table)
            persisted = repr(
                self.conn.execute("SELECT * FROM %s" % table).fetchall()
            ).lower()
            for raw_phrase in ("synth routing", "chorus", "bass answer"):
                self.assertNotIn(raw_phrase, persisted, table)

    def test_backfill_is_idempotent(self):
        self.finalize_shared_moment(
            900,
            (
                "The synth routing should keep the chorus wide",
                "The synth drum patch can answer the bass",
                "Which synth layer should we test?",
            ),
        )
        before = (
            self.conn.execute(
                "SELECT COUNT(*) FROM memory_moment_episodes"
            ).fetchone()[0],
            self.conn.execute(
                "SELECT COUNT(*) FROM memory_moment_episode_moments"
            ).fetchone()[0],
        )
        first = moments.backfill_episodic_lifecycle(self.conn)
        second = moments.backfill_episodic_lifecycle(self.conn)
        after = (
            self.conn.execute(
                "SELECT COUNT(*) FROM memory_moment_episodes"
            ).fetchone()[0],
            self.conn.execute(
                "SELECT COUNT(*) FROM memory_moment_episode_moments"
            ).fetchone()[0],
        )
        self.assertEqual(before, after)
        self.assertEqual(first["deduplicated"], 1)
        self.assertEqual(second["deduplicated"], 1)


if __name__ == "__main__":
    unittest.main()
