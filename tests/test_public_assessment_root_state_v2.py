import sqlite3
import unittest
from itertools import product

import bnl_memory_ledger as ledger


class PublicAssessmentRootStateV2Tests(unittest.TestCase):
    GUILD_ID = 1
    USER_ID = 7
    SUBJECT_KEY = "discord_user:7"
    USER_NAME = "Crow"
    CHANNEL_ID = 10
    CHANNEL_NAME = "barcode-bot"
    POLICY = "public_home"

    def setUp(self):
        self.conn = self._new_connection()

    def tearDown(self):
        self.conn.close()

    @staticmethod
    def _new_connection():
        conn = sqlite3.connect(":memory:")
        ledger.ensure_memory_ledger_schema(conn)
        conn.execute(
            """
            CREATE TABLE conversations (
                id INTEGER PRIMARY KEY,
                guild_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                user_name TEXT NOT NULL,
                role TEXT NOT NULL,
                content TEXT NOT NULL,
                channel_id INTEGER NOT NULL,
                channel_name TEXT NOT NULL,
                channel_policy TEXT NOT NULL,
                message_id INTEGER NOT NULL,
                route_mode TEXT NOT NULL,
                public_usable,
                visibility TEXT NOT NULL,
                timestamp TEXT NOT NULL
            )
            """
        )
        conn.commit()
        return conn

    def _add_source(
        self,
        row_id,
        text="I compare audio mixes before the final release.",
        observed_at="2026-08-01T12:00:00+00:00",
        *,
        conn=None,
        guild_id=GUILD_ID,
        user_id=USER_ID,
        user_name=USER_NAME,
        role="user",
        channel_id=CHANNEL_ID,
        channel_name=CHANNEL_NAME,
        channel_policy=POLICY,
        route_mode="normal_chat",
        public_usable=1,
        visibility="public",
        source_sequence=None,
    ):
        conn = conn or self.conn
        message_id = 10_000 + int(row_id)
        conn.execute(
            """
            INSERT INTO conversations(
                id,guild_id,user_id,user_name,role,content,channel_id,
                channel_name,channel_policy,message_id,route_mode,
                public_usable,visibility,timestamp
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                int(row_id),
                int(guild_id),
                int(user_id),
                user_name,
                role,
                text,
                int(channel_id),
                channel_name,
                channel_policy,
                message_id,
                route_mode,
                public_usable,
                visibility,
                observed_at,
            ),
        )
        result = ledger.shadow_conversation_row(
            conn,
            row_id=int(row_id),
            user_id=int(user_id),
            user_name=user_name,
            guild_id=int(guild_id),
            role=role,
            content=text,
            channel_id=int(channel_id),
            channel_name=channel_name,
            channel_policy=channel_policy,
            message_id=message_id,
            route_mode=route_mode,
            observed_at=observed_at,
            source_sequence=int(source_sequence or row_id),
        )
        self.assertEqual(result.outcome, "inserted")
        conn.commit()
        return result.entry_id

    def _state(self, entry_id, *, conn=None):
        return ledger.read_public_assessment_root_state(
            conn or self.conn,
            entry_id=entry_id,
            guild_id=self.GUILD_ID,
            subject_key=self.SUBJECT_KEY,
        )

    def _selection(self, *, conn=None):
        return ledger.select_public_conversation_assessment_evidence(
            conn or self.conn,
            guild_id=self.GUILD_ID,
            subject_key=self.SUBJECT_KEY,
            request_text="What am I all about in the BARCODE project?",
        )

    def _assert_ineligible(self, entry_id, *, conn=None):
        conn = conn or self.conn
        self.assertIsNone(self._state(entry_id, conn=conn))
        self.assertNotIn(
            entry_id,
            {item.entry_id for item in self._selection(conn=conn).items},
        )

    def test_valid_source_is_content_bound_and_selected(self):
        entry_id = self._add_source(1)

        state = self._state(entry_id)
        self.assertIsNotNone(state)
        self.assertEqual(state.entry_id, entry_id)
        self.assertEqual(state.subject_key, self.SUBJECT_KEY)
        self.assertEqual(state.source_row_id, "1")
        self.assertEqual(state.route_mode, "normal_chat")
        self.assertTrue(state.root_identity)
        self.assertTrue(state.occurrence_identity)
        self.assertTrue(state.source_digest)
        self.assertEqual(state.semantics.attribution_mode, "subject_action")
        self.assertEqual(state.semantics.action_identity, "evaluate")

        selection = self._selection()
        self.assertEqual(selection.eligible_count, 1)
        self.assertEqual(len(selection.items), 1)
        item = selection.items[0]
        self.assertEqual(item.entry_id, entry_id)
        self.assertEqual(item.source_digest, state.source_digest)
        self.assertEqual(item.root_identity, state.root_identity)
        self.assertEqual(item.occurrence_identity, state.occurrence_identity)
        self.assertEqual(item.point_identity, state.semantics.point_identity)

    def test_raw_source_edit_delete_and_provenance_drift_fail_closed(self):
        mutations = (
            (
                "content",
                "UPDATE conversations SET content=? WHERE id=?",
                ("I publish every unreviewed mix immediately now.",),
            ),
            (
                "user_id",
                "UPDATE conversations SET user_id=? WHERE id=?",
                (8,),
            ),
            (
                "user_name",
                "UPDATE conversations SET user_name=? WHERE id=?",
                ("Other Member",),
            ),
            (
                "role",
                "UPDATE conversations SET role=? WHERE id=?",
                ("assistant",),
            ),
            (
                "policy",
                "UPDATE conversations SET channel_policy=? WHERE id=?",
                ("internal_controlled",),
            ),
            (
                "channel_id",
                "UPDATE conversations SET channel_id=? WHERE id=?",
                (999,),
            ),
            (
                "channel_name",
                "UPDATE conversations SET channel_name=? WHERE id=?",
                ("operations",),
            ),
            (
                "timestamp",
                "UPDATE conversations SET timestamp=? WHERE id=?",
                ("2026-08-02T12:00:00+00:00",),
            ),
        )
        for index, (label, sql, values) in enumerate(mutations, start=10):
            with self.subTest(drift=label):
                entry_id = self._add_source(
                    index,
                    observed_at=f"2026-08-01T{index:02d}:00:00+00:00",
                )
                self.assertIsNotNone(self._state(entry_id))
                self.conn.execute(sql, (*values, index))
                self.conn.commit()
                self._assert_ineligible(entry_id)

        entry_id = self._add_source(
            30,
            observed_at="2026-08-02T06:00:00+00:00",
        )
        self.assertIsNotNone(self._state(entry_id))
        self.conn.execute("DELETE FROM conversations WHERE id=30")
        self.conn.commit()
        self._assert_ineligible(entry_id)

    def test_author_binding_rejects_conflict_and_wrong_guild(self):
        conflicting = self._add_source(40)
        self.conn.execute(
            """
            INSERT INTO memory_ledger_participants(
                entry_id,guild_id,participant_key,display_name,
                participant_role,order_index,created_at
            ) VALUES(?,?,?,?,?,?,?)
            """,
            (
                conflicting,
                self.GUILD_ID,
                "discord_user:8",
                "Other Member",
                "author",
                1,
                "2026-08-01T12:01:00+00:00",
            ),
        )
        self.conn.commit()
        self._assert_ineligible(conflicting)

        wrong_guild = self._add_source(
            41,
            observed_at="2026-08-01T13:00:00+00:00",
        )
        self.conn.execute(
            """
            UPDATE memory_ledger_participants
            SET guild_id=2
            WHERE entry_id=? AND participant_role='author'
            """,
            (wrong_guild,),
        )
        self.conn.commit()
        self._assert_ineligible(wrong_guild)

    def test_privileged_and_unresolved_routes_are_not_selected(self):
        for index, route_mode in enumerate(
            ("operator_command", "direct_payload", "unknown"),
            start=50,
        ):
            with self.subTest(route_mode=route_mode):
                entry_id = self._add_source(
                    index,
                    observed_at=f"2026-08-01T{index - 48:02d}:00:00+00:00",
                    route_mode=route_mode,
                )
                self._assert_ineligible(entry_id)

    def test_ambiguous_and_unresolved_fences_withhold_pre_and_equal_rows(self):
        reasons = (
            "conversation_motif_correction_ambiguous",
            "conversation_motif_correction_unresolved",
        )
        states = ("active", "satisfied")
        positions = (
            ("pre", "2026-08-01T11:59:59+00:00", False),
            ("equal", "2026-08-01T12:00:00+00:00", False),
            ("post", "2026-08-01T12:00:01+00:00", True),
        )
        for case_number, (reason, fence_state, position) in enumerate(
            product(reasons, states, positions),
            start=1,
        ):
            label, observed_at, eligible = position
            with self.subTest(
                reason=reason,
                fence_state=fence_state,
                position=label,
            ):
                conn = self._new_connection()
                try:
                    entry_id = self._add_source(
                        case_number,
                        observed_at=observed_at,
                        conn=conn,
                    )
                    conn.execute(
                        """
                        INSERT INTO memory_ledger_conversation_motif_fences(
                            guild_id,subject_key,predicate_key,
                            correction_entry_id,correction_observed_at,
                            reason_code,fence_state,satisfied_at,
                            created_at,updated_at
                        ) VALUES(?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            self.GUILD_ID,
                            self.SUBJECT_KEY,
                            "conversation_motif_process",
                            f"correction-{case_number}",
                            "2026-08-01T12:00:00+00:00",
                            reason,
                            fence_state,
                            (
                                "2026-08-01T12:00:00+00:00"
                                if fence_state == "satisfied"
                                else ""
                            ),
                            "2026-08-01T12:00:00+00:00",
                            "2026-08-01T12:00:00+00:00",
                        ),
                    )
                    conn.commit()
                    state = self._state(entry_id, conn=conn)
                    selected_ids = {
                        item.entry_id for item in self._selection(conn=conn).items
                    }
                    if eligible:
                        self.assertIsNotNone(state)
                        self.assertIn(entry_id, selected_ids)
                    else:
                        self.assertIsNone(state)
                        self.assertNotIn(entry_id, selected_ids)
                finally:
                    conn.close()

    def test_incoming_correction_supersession_and_retraction_invalidate(self):
        for index, lineage_type in enumerate(
            ("correction_of", "supersedes", "retracts"),
            start=70,
        ):
            with self.subTest(lineage_type=lineage_type):
                conn = self._new_connection()
                try:
                    entry_id = self._add_source(index, conn=conn)
                    self.assertIsNotNone(self._state(entry_id, conn=conn))
                    conn.execute(
                        """
                        INSERT INTO memory_ledger_lineage(
                            entry_id,guild_id,lineage_type,target_entry_id,
                            created_at
                        ) VALUES(?,?,?,?,?)
                        """,
                        (
                            f"incoming-{lineage_type}",
                            self.GUILD_ID,
                            lineage_type,
                            entry_id,
                            "2026-08-01T12:01:00+00:00",
                        ),
                    )
                    conn.commit()
                    self._assert_ineligible(entry_id, conn=conn)
                finally:
                    conn.close()

    def test_occurrence_bridge_changes_bound_state_and_selector_receipt(self):
        target = self._add_source(
            2,
            observed_at="2026-08-01T12:20:00+00:00",
        )
        before = self._state(target)
        self.assertIsNotNone(before)

        self._add_source(
            1,
            text="I review the final audio release before publishing it.",
            observed_at="2026-08-01T12:00:00+00:00",
        )
        after = self._state(target)
        self.assertIsNotNone(after)
        self.assertEqual(after.root_identity, before.root_identity)
        self.assertNotEqual(after.occurrence_identity, before.occurrence_identity)
        self.assertNotEqual(after.source_digest, before.source_digest)

        selected = {item.entry_id: item for item in self._selection().items}
        self.assertIn(target, selected)
        self.assertEqual(
            selected[target].occurrence_identity,
            after.occurrence_identity,
        )
        self.assertEqual(selected[target].source_digest, after.source_digest)

    def test_third_party_reports_are_excluded_but_authored_action_is_retained(self):
        excluded = (
            "Mac Modem tests the broadcast signal every night.",
            "I think Mac Modem tests the broadcast signal every night.",
            "I heard that Mac Modem reviews the radio show before release.",
        )
        for index, text in enumerate(excluded, start=90):
            with self.subTest(text=text):
                entry_id = self._add_source(
                    index,
                    text=text,
                    observed_at=f"2026-08-01T{index - 84:02d}:00:00+00:00",
                )
                self._assert_ineligible(entry_id)

        authored = self._add_source(
            93,
            text="I asked Mac Modem to test the broadcast signal.",
            observed_at="2026-08-01T12:00:00+00:00",
        )
        state = self._state(authored)
        self.assertIsNotNone(state)
        self.assertEqual(state.semantics.attribution_mode, "subject_action")
        self.assertEqual(state.semantics.action_identity, "ask")
        self.assertEqual(state.semantics.polarity, "affirmative")

    def test_negation_is_scoped_to_the_subject_action_clause(self):
        cases = (
            (
                100,
                "I never compare audio mixes before the final release.",
                "negative",
                "evaluate",
            ),
            (
                101,
                "I compare audio mixes, not visual drafts, before release.",
                "affirmative",
                "evaluate",
            ),
            (
                102,
                "I asked Mac Modem not to test the broadcast signal.",
                "affirmative",
                "ask",
            ),
        )
        for row_id, text, polarity, action in cases:
            with self.subTest(text=text):
                entry_id = self._add_source(
                    row_id,
                    text=text,
                    observed_at=f"2026-08-02T{row_id - 99:02d}:00:00+00:00",
                )
                state = self._state(entry_id)
                self.assertIsNotNone(state)
                self.assertEqual(state.semantics.polarity, polarity)
                self.assertEqual(state.semantics.action_identity, action)

    def test_synonymous_authored_points_share_one_semantic_identity(self):
        first_id = self._add_source(
            110,
            text="I compare audio mixes before the final release.",
            observed_at="2026-08-01T12:00:00+00:00",
        )
        second_id = self._add_source(
            111,
            text="I weigh sound before publishing the final track.",
            observed_at="2026-08-01T13:00:00+00:00",
        )
        first = self._state(first_id)
        second = self._state(second_id)
        self.assertIsNotNone(first)
        self.assertIsNotNone(second)
        self.assertEqual(first.semantics.action_identity, "evaluate")
        self.assertEqual(second.semantics.action_identity, "evaluate")
        self.assertEqual(
            first.semantics.material_facets,
            (
                "action:evaluate",
                "topic:audio",
                "topic:release",
                "relation:before",
                "temporal:present",
            ),
        )
        self.assertEqual(
            second.semantics.material_facets,
            first.semantics.material_facets,
        )
        self.assertEqual(
            second.semantics.point_identity,
            first.semantics.point_identity,
        )
        selected = self._selection().items
        self.assertEqual({item.entry_id for item in selected}, {first_id, second_id})
        self.assertEqual(len({item.point_identity for item in selected}), 1)

    def test_temp_schema_shadow_cannot_hide_authoritative_main_source(self):
        entry_id = self._add_source(120)
        before = self._state(entry_id)
        self.assertIsNotNone(before)

        self.conn.execute(
            """
            CREATE TEMP TABLE memory_ledger_entries AS
            SELECT * FROM main.memory_ledger_entries WHERE 0
            """
        )
        self.conn.execute(
            """
            CREATE TEMP TABLE conversations AS
            SELECT * FROM main.conversations WHERE 0
            """
        )

        after = self._state(entry_id)
        self.assertIsNotNone(after)
        self.assertEqual(after.source_digest, before.source_digest)
        selected = self._selection()
        self.assertIn(entry_id, {item.entry_id for item in selected.items})

    def test_malformed_and_false_explicit_boolean_states_fail_closed(self):
        cases = (
            ("ledger_public_false", "memory_ledger_entries", "public_usable", "false"),
            ("ledger_public_malformed", "memory_ledger_entries", "public_usable", "definitely"),
            ("ledger_derived_true", "memory_ledger_entries", "derived", "true"),
            ("ledger_projection_true", "memory_ledger_entries", "projection", "true"),
            ("raw_public_false", "conversations", "public_usable", "false"),
            ("raw_public_malformed", "conversations", "public_usable", "definitely"),
        )
        for index, (label, table, column, value) in enumerate(cases, start=130):
            with self.subTest(case=label):
                entry_id = self._add_source(
                    index,
                    observed_at=f"2026-08-02T{index - 129:02d}:00:00+00:00",
                )
                self.assertIsNotNone(self._state(entry_id))
                key_column = "entry_id" if table == "memory_ledger_entries" else "id"
                key = entry_id if table == "memory_ledger_entries" else index
                self.conn.execute(
                    f"UPDATE {table} SET {column}=? WHERE {key_column}=?",
                    (value, key),
                )
                self.conn.commit()
                self._assert_ineligible(entry_id)


if __name__ == "__main__":
    unittest.main()
