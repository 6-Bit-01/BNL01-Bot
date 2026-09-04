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
        moment_id, sources = self.finalize_shared_moment(
            95,
            (
                "Let's build the synth routing for the chorus",
                "The synth drum patch needs a bass answer",
                "Which synth layer should we test next?",
            ),
            users=(1, 2, 3),
            policy="sealed_test",
        )
        reference_out = {}
        rendered = moments.render_active_episode_canary_context(
            self.conn,
            guild_id=1,
            channel_id=10,
            channel_policy="sealed_test",
            route_mode="normal_chat",
            topic_text="How should we continue the synth routing?",
            participant_keys=("discord_user:1",),
            now=self.timestamp(minutes=4),
            reference_out=reference_out,
        )
        self.assertIn("Active same-channel episode signal", rendered)
        self.assertIn("Shared human participants: 3", rendered)
        self.assertIn("Unresolved open loops:", rendered)
        self.assertEqual(
            reference_out["reference"].source_moment_ids,
            (moment_id,),
        )
        question_reference_out = {}
        question_rendered = moments.render_active_episode_canary_context(
            self.conn,
            guild_id=1,
            channel_id=10,
            channel_policy="sealed_test",
            route_mode="normal_chat",
            topic_text="Is this a separate task, or should we continue?",
            participant_keys=("discord_user:1",),
            now=self.timestamp(minutes=4),
            expected_episode_id=(
                reference_out["reference"].episode_id
            ),
            reference_out=question_reference_out,
        )
        self.assertIn(
            "Active same-channel episode signal",
            question_rendered,
        )
        self.assertEqual(
            question_reference_out["reference"].source_moment_ids,
            (moment_id,),
        )
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

    def test_sealed_canary_expected_episode_rejects_scope_ambiguity(self):
        self.finalize_shared_moment(
            105,
            (
                "Let's build the synth routing for the chorus",
                "The synth drum patch needs a bass answer",
                "Which synth layer should we test next?",
            ),
            users=(1, 2, 3),
            policy="sealed_test",
        )
        old_episode_id = self.conn.execute(
            "SELECT episode_id FROM memory_moment_episodes"
        ).fetchone()[0]
        self.finalize_shared_moment(
            115,
            (
                "Pizza dough needs a hotter oven stone",
                "Pizza sauce works with the dough structure",
                "The pizza oven should keep the crust crisp",
            ),
            minutes=10,
            policy="sealed_test",
        )
        self.conn.execute(
            """
            UPDATE memory_moment_episodes
            SET lifecycle_status='active',finalized_at=NULL,
                finalization_reason=''
            WHERE episode_id=?
            """,
            (old_episode_id,),
        )
        self.assertEqual(
            self.conn.execute(
                """
                SELECT COUNT(*) FROM memory_moment_episodes
                WHERE guild_id=1 AND channel_id=10
                  AND channel_policy='sealed_test'
                  AND route_mode='normal_chat'
                  AND lifecycle_status='active'
                """
            ).fetchone()[0],
            2,
        )
        reference_out = {"reference": object()}
        self.assertEqual(
            moments.render_active_episode_canary_context(
                self.conn,
                guild_id=1,
                channel_id=10,
                channel_policy="sealed_test",
                route_mode="normal_chat",
                topic_text="Is this a separate task, or should we continue?",
                participant_keys=("discord_user:1",),
                now=self.timestamp(minutes=14),
                expected_episode_id=old_episode_id,
                reference_out=reference_out,
            ),
            "",
        )
        self.assertEqual(reference_out, {})

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

    def test_situation_reader_selects_next_phase_and_closed_open_loop(self):
        first_moment, _ = self.finalize_shared_moment(
            120,
            (
                "Let's build the synth routing and test the chorus",
                "The synth drum patch can answer the bass",
                "Which synth layer remains open?",
            ),
            users=(1, 2, 3),
        )
        second_moment, _ = self.finalize_shared_moment(
            130,
            (
                "The synth bass routing now uses the warmer patch",
                "Let's test the synth chorus routing again",
                "The synth routing test passed and this is complete",
            ),
            minutes=10,
            users=(1, 2, 3),
        )

        rows = moments.select_situation_aware_episode_gists(
            self.conn,
            guild_id=1,
            topic_text="What happened next with the synth chorus?",
            frame_event_ref=first_moment,
            frame_event_relation="same_event_new_phase",
            frame_phase="diagnosis",
            broad_recall=True,
            allowed_channel_policies=("public_home",),
            max_results=4,
        )
        self.assertEqual(tuple(row.moment_id for row in rows), (second_moment,))
        self.assertEqual(rows[0].sequence_index, 1)
        self.assertIn("retest", rows[0].semantic_types)
        self.assertIn("outcome", rows[0].semantic_types)
        self.assertEqual(rows[0].episode_lifecycle, "finalized")
        self.assertEqual(rows[0].event_relation, "same_event_new_phase")

        open_rows = moments.select_situation_aware_episode_gists(
            self.conn,
            guild_id=1,
            topic_text="What remains open with the synth chorus?",
            frame_event_ref=second_moment,
            frame_event_relation="same_event",
            frame_phase="diagnosis",
            broad_recall=True,
            allowed_channel_policies=("public_home",),
            max_results=4,
        )
        self.assertEqual(open_rows, ())
        new_event_rows = moments.select_situation_aware_episode_gists(
            self.conn,
            guild_id=1,
            topic_text="This is a different synth failure.",
            frame_event_ref=second_moment,
            frame_event_relation="new_event_same_participant",
            frame_phase="failure",
            broad_recall=True,
            allowed_channel_policies=("public_home",),
            max_results=4,
        )
        self.assertEqual(new_event_rows, ())

    def test_correction_retest_reader_uses_only_fresh_episode(self):
        first_moment, _ = self.finalize_shared_moment(
            140,
            (
                "Let's build the synth routing and test the chorus",
                "The synth drum patch can answer the bass",
                "Which synth layer remains open?",
            ),
            users=(1, 2, 3),
        )
        corrected_moment, _ = self.finalize_shared_moment(
            150,
            (
                "Actually the synth bass routing uses the warmer patch",
                "Let's test the synth chorus routing again",
                "The synth routing test passed and this is done",
            ),
            minutes=10,
            users=(1, 2, 3),
        )
        self.assertEqual(
            self.conn.execute(
                """
                SELECT lifecycle_status FROM memory_moment_windows
                WHERE moment_id=?
                """,
                (first_moment,),
            ).fetchone()[0],
            "needs_review",
        )

        for query, phase, semantic_type in (
            ("What changed with the synth routing?", "correction", "correction"),
            ("What was retested with the synth routing?", "retest", "retest"),
            ("What was completed with the synth routing?", "completion", "outcome"),
        ):
            with self.subTest(query=query):
                rows = moments.select_situation_aware_episode_gists(
                    self.conn,
                    guild_id=1,
                    topic_text=query,
                    frame_event_ref=corrected_moment,
                    frame_event_relation="same_event_new_phase",
                    frame_phase=phase,
                    broad_recall=True,
                    allowed_channel_policies=("public_home",),
                    max_results=4,
                )
                self.assertEqual(
                    tuple(row.moment_id for row in rows),
                    (corrected_moment,),
                )
                self.assertIn(semantic_type, rows[0].semantic_types)

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

    def test_explicit_separate_task_overrides_broad_topic_overlap(self):
        glass_messages = (
            (
                "Sealed acceptance fixture: Project Glass Harbor is in "
                "rehearsal. The amber signal failed, and the open question "
                "is whether to test the relay or the decoder first. Which "
                "should we test first?",
                "Test the relay first; decoder diagnostics remain open.",
            ),
            (
                "For Project Glass Harbor, commit the plan: test the relay "
                "first, then investigate the amber signal. The decoder "
                "remains an open follow-up. Recap the current phase and "
                "open loop.",
                "The relay test is current and decoder diagnostics remain open.",
            ),
        )
        row_id = 260
        for human_text, model_text in glass_messages:
            self.add(
                row_id,
                1,
                human_text,
                policy="sealed_test",
            )
            self.add(
                row_id + 1,
                0,
                model_text,
                policy="sealed_test",
                role="model",
                name="BNL-01",
            )
            row_id += 2
        moments.sweep_expired_windows(
            self.conn,
            now=self.timestamp(minutes=3),
        )
        glass_episode_id = self.conn.execute(
            "SELECT episode_id FROM memory_moment_episodes"
        ).fetchone()[0]

        copper_prompt = (
            "Sealed acceptance fixture: this is a separate task. Project "
            "Copper Kite has a stable blue indicator, and the antenna "
            "calibration must be completed before its notes are archived. "
            "What is the current task, and what remains open?"
        )
        self.assertIsNone(
            moments.active_episode_for_assessment(
                self.conn,
                guild_id=1,
                channel_id=10,
                channel_policy="sealed_test",
                route_mode="normal_chat",
                topic_text=copper_prompt,
                participant_keys=("discord_user:1",),
                now=self.timestamp(minutes=10),
            )
        )
        self.assertEqual(
            moments.render_active_episode_canary_context(
                self.conn,
                guild_id=1,
                channel_id=10,
                channel_policy="sealed_test",
                route_mode="normal_chat",
                topic_text=copper_prompt,
                participant_keys=("discord_user:1",),
                now=self.timestamp(minutes=10),
            ),
            "",
        )

        copper_messages = (
            (
                copper_prompt,
                "Antenna calibration is current; archiving remains open.",
            ),
            (
                "This remains a separate task: Project Copper Kite. Commit "
                "the plan: perform antenna calibration first, leave notes "
                "unarchived, and keep archiving as the open follow-up.",
                "Calibration is current and note archiving remains open.",
            ),
        )
        for human_text, model_text in copper_messages:
            self.add(
                row_id,
                1,
                human_text,
                minutes=10,
                policy="sealed_test",
            )
            self.add(
                row_id + 1,
                0,
                model_text,
                minutes=10,
                policy="sealed_test",
                role="model",
                name="BNL-01",
            )
            row_id += 2
        moments.sweep_expired_windows(
            self.conn,
            now=self.timestamp(minutes=13),
        )

        episodes = self.conn.execute(
            """
            SELECT episode_id,lifecycle_status,finalization_reason
            FROM memory_moment_episodes ORDER BY opened_at,episode_id
            """
        ).fetchall()
        self.assertEqual(len(episodes), 2)
        old = next(row for row in episodes if row[0] == glass_episode_id)
        new = next(row for row in episodes if row[0] != glass_episode_id)
        self.assertEqual(old[1:], ("finalized", "topic_interruption"))
        self.assertEqual(new[1], "active")
        self.assertEqual(
            self.conn.execute(
                """
                SELECT link_role FROM memory_moment_episode_moments
                WHERE episode_id=?
                """,
                (new[0],),
            ).fetchone()[0],
            "opened",
        )
        self.assertEqual(
            self.conn.execute(
                """
                SELECT to_episode_id,relation_type
                FROM memory_moment_episode_lineage
                WHERE from_episode_id=?
                """,
                (new[0],),
            ).fetchone(),
            (glass_episode_id, "interrupted_from"),
        )

    def test_negated_separate_task_preserves_active_episode(self):
        first_moment, _ = self.finalize_shared_moment(
            280,
            (
                "Let's calibrate the Copper Kite antenna",
                "The Copper Kite indicator is still flickering",
                "Archiving the Copper Kite notes remains open",
            ),
        )
        episode_id = self.conn.execute(
            "SELECT episode_id FROM memory_moment_episodes"
        ).fetchone()[0]

        second_moment, _ = self.finalize_shared_moment(
            290,
            (
                "This is not a separate task; continue the same Copper "
                "Kite incident",
                "The Copper Kite antenna calibration is still active",
                "The Copper Kite notes remain unarchived",
            ),
            minutes=10,
        )

        episode = self.conn.execute(
            """
            SELECT episode_id,lifecycle_status,moment_count
            FROM memory_moment_episodes
            """
        ).fetchone()
        self.assertEqual(episode, (episode_id, "active", 2))
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
            {first_moment, second_moment},
        )

    def test_active_episode_boundary_uses_only_current_turn_text(self):
        moment_id, _ = self.finalize_shared_moment(
            295,
            (
                "Let's build the synth routing and test the chorus",
                "The synth drum patch needs a bass answer",
                "Which synth layer should we test next?",
            ),
            policy="sealed_test",
        )
        historical_and_current = (
            "This is a separate task: synth routing.\n"
            "Correction: the synth routing should use the warmer patch."
        )
        reference = moments.active_episode_for_assessment(
            self.conn,
            guild_id=1,
            channel_id=10,
            channel_policy="sealed_test",
            route_mode="normal_chat",
            topic_text=historical_and_current,
            current_turn_text=(
                "Correction: the synth routing should use the warmer patch."
            ),
            participant_keys=("discord_user:1",),
            now=self.timestamp(minutes=4),
        )
        self.assertIsNotNone(reference)
        self.assertEqual(reference.source_moment_ids, (moment_id,))

        uncertain_reference = moments.active_episode_for_assessment(
            self.conn,
            guild_id=1,
            channel_id=10,
            channel_policy="sealed_test",
            route_mode="normal_chat",
            topic_text=(
                "Is this a separate task, or should we continue?"
            ),
            current_turn_text=(
                "Is this a separate task, or should we continue?"
            ),
            participant_keys=("discord_user:1",),
            now=self.timestamp(minutes=4),
        )
        self.assertIsNotNone(uncertain_reference)
        self.assertEqual(
            uncertain_reference.source_moment_ids,
            (moment_id,),
        )

        for current_turn_text in (
            "Maybe this is a separate task; I am not sure yet.",
            "Isn't this a separate task?",
            "Does this count as a separate task?",
            "Is the decoder work a separate task?",
            "This is a separate task, right?",
        ):
            with self.subTest(current_turn_text=current_turn_text):
                self.assertIsNotNone(
                    moments.active_episode_for_assessment(
                        self.conn,
                        guild_id=1,
                        channel_id=10,
                        channel_policy="sealed_test",
                        route_mode="normal_chat",
                        topic_text=(
                            "The synth routing and decoder remain active.\n"
                            + current_turn_text
                        ),
                        current_turn_text=current_turn_text,
                        participant_keys=("discord_user:1",),
                        now=self.timestamp(minutes=4),
                    )
                )

        for current_turn_text in (
            "Don't start a new task; continue this incident.",
            "Do not treat this as a separate task.",
            "We should not start a new task; continue this incident.",
            "Let's not start a new task.",
            "Never start a new task; continue this incident.",
        ):
            with self.subTest(current_turn_text=current_turn_text):
                self.assertIsNotNone(
                    moments.active_episode_for_assessment(
                        self.conn,
                        guild_id=1,
                        channel_id=10,
                        channel_policy="sealed_test",
                        route_mode="normal_chat",
                        topic_text=current_turn_text,
                        current_turn_text=current_turn_text,
                        participant_keys=("discord_user:1",),
                        now=self.timestamp(minutes=4),
                    )
                )

        self.assertIsNone(
            moments.active_episode_for_assessment(
                self.conn,
                guild_id=1,
                channel_id=10,
                channel_policy="sealed_test",
                route_mode="normal_chat",
                topic_text="synth routing chorus",
                current_turn_text="Can you start another task?",
                participant_keys=("discord_user:1",),
                now=self.timestamp(minutes=4),
            )
        )

        for current_turn_text in (
            "Okay, can you start another task?",
            "Before we continue, can you start a new task?",
            "This is a separate task: what should we do next?",
            "Can we treat this as a new task?",
            "Could you consider this a separate task?",
        ):
            with self.subTest(current_turn_text=current_turn_text):
                self.assertIsNone(
                    moments.active_episode_for_assessment(
                        self.conn,
                        guild_id=1,
                        channel_id=10,
                        channel_policy="sealed_test",
                        route_mode="normal_chat",
                        topic_text=current_turn_text,
                        current_turn_text=current_turn_text,
                        participant_keys=("discord_user:1",),
                        now=self.timestamp(minutes=4),
                    )
                )

        unrelated_uncertain = (
            "Maybe the pizza launch is a separate task"
        )
        self.assertIsNone(
            moments.active_episode_for_assessment(
                self.conn,
                guild_id=1,
                channel_id=10,
                channel_policy="sealed_test",
                route_mode="normal_chat",
                topic_text=unrelated_uncertain,
                current_turn_text=unrelated_uncertain,
                participant_keys=("discord_user:1",),
                now=self.timestamp(minutes=4),
            )
        )

        self.assertIsNone(
            moments.active_episode_for_assessment(
                self.conn,
                guild_id=1,
                channel_id=10,
                channel_policy="sealed_test",
                route_mode="normal_chat",
                topic_text=(
                    "Even if it looks similar, this is a separate incident."
                ),
                current_turn_text=(
                    "Even if it looks similar, this is a separate incident."
                ),
                participant_keys=("discord_user:1",),
                now=self.timestamp(minutes=4),
            )
        )

        self.assertIsNone(
            moments.active_episode_for_assessment(
                self.conn,
                guild_id=1,
                channel_id=10,
                channel_policy="sealed_test",
                route_mode="normal_chat",
                topic_text=historical_and_current,
                current_turn_text=(
                    "This is a separate task: Project Silver Compass."
                ),
                participant_keys=("discord_user:1",),
                now=self.timestamp(minutes=4),
            )
        )

    def test_interrogative_boundary_does_not_split_episode(self):
        first_moment, _ = self.finalize_shared_moment(
            297,
            (
                "The Copper Kite antenna calibration is still active",
                "The Copper Kite indicator remains stable",
                "Archiving the Copper Kite notes remains open",
            ),
        )
        episode_id = self.conn.execute(
            "SELECT episode_id FROM memory_moment_episodes"
        ).fetchone()[0]

        second_moment, _ = self.finalize_shared_moment(
            307,
            (
                "The Copper Kite antenna calibration is still active. "
                "Does this count as a separate task?",
                "The Copper Kite indicator remains stable",
                "The Copper Kite notes remain unarchived",
            ),
            minutes=10,
        )

        episode = self.conn.execute(
            """
            SELECT episode_id,lifecycle_status,moment_count
            FROM memory_moment_episodes
            """
        ).fetchone()
        self.assertEqual(episode, (episode_id, "active", 2))
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
            {first_moment, second_moment},
        )
        self.assertEqual(
            self.conn.execute(
                "SELECT COUNT(*) FROM memory_moment_episode_lineage"
            ).fetchone()[0],
            0,
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

    def test_delayed_and_explicit_cross_channel_resume_are_source_bounded(self):
        moment_id, _ = self.finalize_shared_moment(
            430,
            (
                "The synth routing should keep the chorus wide",
                "The synth drum patch can answer the bass",
                "Which synth layer should we revisit?",
            ),
        )
        before = self.conn.total_changes
        delayed = moments.recent_moment_situation_for_assessment(
            self.conn,
            guild_id=1,
            channel_id=10,
            channel_policy="public_home",
            route_mode="normal_chat",
            topic_text="Delayed reply: let's continue the synth routing.",
            participant_keys=("discord_user:1",),
            now=self.timestamp(hours=48),
        )
        self.assertIsNotNone(delayed)
        self.assertEqual(delayed.moment_id, moment_id)
        self.assertEqual(delayed.selection_reason, "explicit_delayed_resume")
        self.assertFalse(delayed.cross_channel_continuation)

        cross_channel = moments.recent_moment_situation_for_assessment(
            self.conn,
            guild_id=1,
            channel_id=99,
            channel_policy="public_home",
            route_mode="normal_chat",
            topic_text="Let's continue the synth routing from <#10>.",
            participant_keys=("discord_user:1",),
            now=self.timestamp(hours=48),
        )
        self.assertIsNotNone(cross_channel)
        self.assertEqual(cross_channel.moment_id, moment_id)
        self.assertEqual(cross_channel.source_channel_id, 10)
        self.assertTrue(cross_channel.cross_channel_continuation)
        self.assertEqual(
            cross_channel.selection_reason,
            "explicit_cross_channel_resume",
        )
        self.assertEqual(self.conn.total_changes, before)

    def test_situation_reader_rejects_ambiguous_delayed_resume(self):
        self.finalize_shared_moment(
            470,
            (
                "The synth routing should keep the chorus wide",
                "The synth drum patch can answer the bass",
                "Which synth layer should we revisit?",
            ),
        )
        moments.sweep_expired_episodes(
            self.conn,
            now=self.timestamp(hours=25),
        )
        self.finalize_shared_moment(
            480,
            (
                "The synth routing has a warmer chorus shape",
                "The synth drum patch follows the bass",
                "The synth layer needs another test",
            ),
            hours=48,
        )
        moments.sweep_expired_episodes(
            self.conn,
            now=self.timestamp(hours=73),
        )

        self.assertIsNone(
            moments.recent_moment_situation_for_assessment(
                self.conn,
                guild_id=1,
                channel_id=10,
                channel_policy="public_home",
                route_mode="normal_chat",
                topic_text="Coming back to this: continue the synth routing.",
                participant_keys=("discord_user:1",),
                now=self.timestamp(hours=96),
            )
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
