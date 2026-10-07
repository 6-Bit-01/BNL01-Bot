"""Relationship link refresh preserves lineage without scanning unrelated Moments."""
import sqlite3
import unittest
from unittest import mock

import bnl_memory_ledger as ledger
import bnl_moment_engine as moments
import bnl_relationship_engine as relationships


NOW = "2026-01-01T00:00:00+00:00"


class RelationshipMomentLinkRefreshTests(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.addCleanup(self.conn.close)
        ledger.ensure_memory_ledger_schema(self.conn)
        moments.ensure_moment_schema(self.conn)
        relationships.ensure_relationship_v2_schema(self.conn)
        clock = mock.patch.object(relationships, "_now", return_value=NOW)
        clock.start()
        self.addCleanup(clock.stop)

    def add_moment(self, suffix, *, guild=1, user=7, visibility="public_safe"):
        moment_id = "moment_" + str(suffix)
        self.conn.execute(
            """INSERT INTO memory_moment_windows (
                moment_id,guild_id,channel_id,channel_policy,route_mode,topic_key,
                window_started_at,last_activity_at,lifecycle_status,visibility,created_at,updated_at
            ) VALUES (?,?,10,'public_context','normal_chat','neutral',?,?,'finalized',?,?,?)""",
            (moment_id, guild, NOW, NOW, visibility, NOW, NOW),
        )
        self.conn.execute(
            """INSERT INTO memory_moment_participants (
                moment_id,participant_key,participant_role,created_at,updated_at
            ) VALUES (?,?,'author',?,?)""",
            (moment_id, "discord_user:%s" % user, NOW, NOW),
        )
        return moment_id

    def add_chain(self, suffix="one", *, guild=1, user=7):
        root = "root_" + str(suffix)
        event = "event_" + str(suffix)
        moment = self.add_moment(suffix, guild=guild, user=user)
        self.conn.execute(
            """INSERT INTO memory_ledger_entries (
                entry_id,schema_version,guild_id,subject_key,entry_type,predicate_key,
                normalized_value,source_class,source_table,source_row_id,source_role,
                visibility,confidence,public_usable,derived,projection,salience,
                observed_at,lifecycle_status,created_at,updated_at
            ) VALUES (?,'memory_ledger_v1',?,?,'observation','conversation',
                'Neutral fixture observation','first_party_record','conversations',?,'user',
                'public_safe','high',1,0,0,0.5,?,'active',?,?)""",
            (root, guild, "discord_user:%s" % user, str(suffix), NOW, NOW, NOW),
        )
        self.conn.execute(
            """INSERT INTO relationship_events_v2 (
                event_id,schema_version,guild_id,subject_user_id,subject_key,actor_role,
                event_type,direction,source_table,source_row_id,channel_policy,
                observed_at,lifecycle,created_at,updated_at
            ) VALUES (?,'relationship_v2.1',?,?,?,'user','appreciation','user_to_bnl',
                'conversations',?,'public_context',?,'active',?,?)""",
            (event, guild, user, "discord_user:%s" % user, str(suffix), NOW, NOW, NOW),
        )
        self.conn.execute(
            """INSERT INTO memory_moment_members (
                moment_id,ledger_entry_id,membership_role,created_at
            ) VALUES (?,?,'author',?)""", (moment, root, NOW),
        )
        return event, root, moment

    def links(self):
        return self.conn.execute(
            "SELECT * FROM relationship_event_moment_links_v2 ORDER BY event_id,moment_id"
        ).fetchall()

    def test_refresh_rechecks_lifecycle_role_visibility_subject_and_guild(self):
        event, root, moment = self.add_chain()
        self.assertEqual(relationships.refresh_moment_links(self.conn, guild_id=1), 1)
        expected = [(event, moment, 1, 7, "active", NOW, NOW)]
        self.assertEqual(self.links(), expected)
        cases = (
            ("relationship_events_v2", "event_id", event, "lifecycle", "corrected", "active"),
            ("relationship_events_v2", "event_id", event, "lifecycle", "forgotten", "active"),
            ("relationship_events_v2", "event_id", event, "lifecycle", "review_only", "active"),
            ("relationship_events_v2", "event_id", event, "actor_role", "model", "user"),
            ("memory_ledger_entries", "entry_id", root, "lifecycle_status", "corrected", "active"),
            ("memory_ledger_entries", "entry_id", root, "lifecycle_status", "deleted", "active"),
            ("memory_ledger_entries", "entry_id", root, "source_role", "model", "user"),
            ("memory_ledger_entries", "entry_id", root, "guild_id", 2, 1),
            ("memory_moment_windows", "moment_id", moment, "lifecycle_status", "needs_review", "finalized"),
            ("memory_moment_windows", "moment_id", moment, "visibility", "private", "public_safe"),
            ("memory_moment_windows", "moment_id", moment, "guild_id", 2, 1),
            ("memory_moment_participants", "moment_id", moment, "participant_key", "discord_user:8", "discord_user:7"),
        )
        for table, key, identity, column, changed, original in cases:
            with self.subTest(table=table, column=column, changed=changed):
                statement = "UPDATE %s SET %s=? WHERE %s=?" % (table, column, key)
                self.conn.execute(statement, (changed, identity))
                self.assertEqual(relationships.refresh_moment_links(self.conn, guild_id=1), 0)
                self.assertEqual(self.links()[0][4], "retracted")
                self.conn.execute(statement, (original, identity))
                self.assertEqual(relationships.refresh_moment_links(self.conn, guild_id=1), 1)
                self.assertEqual(self.links(), expected)

    def test_source_revision_replacement_and_removal_change_links(self):
        event, root, moment = self.add_chain()
        self.assertEqual(relationships.refresh_moment_links(self.conn, guild_id=1), 1)
        self.conn.execute("UPDATE memory_ledger_entries SET source_row_id='replacement' WHERE entry_id=?", (root,))
        self.assertEqual(relationships.refresh_moment_links(self.conn, guild_id=1), 0)
        self.conn.execute("UPDATE relationship_events_v2 SET source_row_id='replacement' WHERE event_id=?", (event,))
        self.assertEqual(relationships.refresh_moment_links(self.conn, guild_id=1), 1)
        self.conn.execute("DELETE FROM memory_moment_members WHERE moment_id=?", (moment,))
        self.assertEqual(relationships.refresh_moment_links(self.conn, guild_id=1), 0)
        self.assertEqual(self.links()[0][4], "retracted")

    def test_other_guild_unchanged_and_global_refresh_still_refreshes_both(self):
        first, _root, _moment = self.add_chain("one")
        second, _root, second_moment = self.add_chain("two", guild=2, user=8)
        self.assertEqual(relationships.refresh_moment_links(self.conn), 2)
        self.conn.execute("UPDATE memory_moment_windows SET lifecycle_status='needs_review' WHERE moment_id=?", (second_moment,))
        self.assertEqual(relationships.refresh_moment_links(self.conn, guild_id=1), 1)
        states = {row[0]: row[4] for row in self.links()}
        self.assertEqual(states, {first: "active", second: "active"})
        self.assertEqual(relationships.refresh_moment_links(self.conn), 1)
        self.assertEqual({row[0]: row[4] for row in self.links()}, {first: "active", second: "retracted"})

    def test_existing_review_only_and_private_source_contract_is_preserved(self):
        event, root, moment = self.add_chain()
        self.conn.execute("UPDATE memory_ledger_entries SET lifecycle_status='review_only' WHERE entry_id=?", (root,))
        self.assertEqual(relationships.refresh_moment_links(self.conn, guild_id=1), 1)
        self.conn.execute("UPDATE relationship_events_v2 SET channel_policy='internal_controlled' WHERE event_id=?", (event,))
        self.conn.execute("UPDATE memory_moment_windows SET visibility='private' WHERE moment_id=?", (moment,))
        self.assertEqual(relationships.refresh_moment_links(self.conn, guild_id=1), 1)
        # Sealed observations remain review-only and cannot become active links.
        self.conn.execute("UPDATE relationship_events_v2 SET channel_policy='sealed_test',lifecycle='review_only' WHERE event_id=?", (event,))
        self.assertEqual(relationships.refresh_moment_links(self.conn, guild_id=1), 0)

    def test_duplicate_participation_keeps_return_count_and_noop_write_effects(self):
        _event, _root, moment = self.add_chain()
        self.conn.execute(
            """INSERT INTO memory_moment_participants (
                moment_id,participant_key,participant_role,created_at,updated_at
            ) VALUES (?,'discord_user:7','observer',?,?)""", (moment, NOW, NOW),
        )
        self.assertEqual(relationships.refresh_moment_links(self.conn, guild_id=1), 2)
        expected = self.links()
        self.assertEqual(len(expected), 1)
        before = self.conn.total_changes
        self.assertEqual(relationships.refresh_moment_links(self.conn, guild_id=1), 2)
        self.assertEqual(self.conn.total_changes - before, 3)
        self.assertEqual(self.links(), expected)

    def test_missing_optional_membership_index_does_not_prevent_refresh(self):
        self.add_chain()
        self.conn.execute("DROP INDEX idx_mmm_entry")
        self.assertEqual(relationships.refresh_moment_links(self.conn, guild_id=1), 1)
        self.assertEqual(self.links()[0][4], "active")

    def test_unavailable_source_table_still_retracts_and_returns_zero(self):
        self.add_chain()
        self.assertEqual(relationships.refresh_moment_links(self.conn, guild_id=1), 1)
        self.conn.execute("DROP TABLE memory_moment_windows")
        self.assertEqual(relationships.refresh_moment_links(self.conn, guild_id=1), 0)
        self.assertEqual(self.links()[0][4], "retracted")

    def test_work_does_not_grow_with_unrelated_moments_for_same_participant(self):
        for index in range(32):
            self.add_chain(str(index))

        def measured_refresh():
            steps = [0]
            def progress():
                steps[0] += 100
                return 0
            self.conn.set_progress_handler(progress, 100)
            try:
                count = relationships.refresh_moment_links(self.conn, guild_id=1)
            finally:
                self.conn.set_progress_handler(None, 0)
            self.assertEqual(count, 32)
            return steps[0]

        small_steps = measured_refresh()
        expected = self.links()
        for index in range(2000):
            self.add_moment("unrelated_%s" % index)
        large_steps = measured_refresh()
        self.assertEqual(self.links(), expected)
        # Allow planner/schema overhead; unrelated participation must not make
        # every event walk that history before checking its exact source root.
        self.assertLess(large_steps, max(50000, small_steps * 4))


if __name__ == "__main__":
    unittest.main()
