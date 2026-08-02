import os
import sqlite3
import unittest
from unittest import mock

import bnl_memory_ledger as ledger


class MemoryLedgerV1Tests(unittest.TestCase):
    def setUp(self):
        self.env = mock.patch.dict(
            os.environ,
            {"BNL_OWNER_USER_ID": "61", "BNL_PRIMARY_GUILD_ID": "1"},
        )
        self.env.start()
        self.conn = sqlite3.connect(":memory:")
        ledger.ensure_memory_ledger_schema(self.conn)

    def tearDown(self):
        self.conn.close()
        self.env.stop()

    def count_entries(self):
        return self.conn.execute("SELECT COUNT(*) FROM memory_ledger_entries").fetchone()[0]

    def add_approved_fact(
        self,
        row_id,
        fact_key,
        fact_value,
        *,
        observed_at="2026-01-01T00:00:00+00:00",
    ):
        return ledger.shadow_first_party_user_fact(
            self.conn,
            row_id=row_id,
            user_id=7,
            user_name="Crow",
            guild_id=1,
            fact_key=fact_key,
            fact_value=fact_value,
            channel_name="barcode-bot",
            channel_policy="public_home",
            channel_id=10,
            message_id=1000 + row_id,
            route_mode="normal_chat",
            observed_at=observed_at,
        )

    def test_schema_idempotent_and_stable_id(self):
        ledger.ensure_memory_ledger_schema(self.conn)
        one = ledger.stable_entry_id(guild_id=1, source_table="conversations", source_row_id=500, entry_type="preference", subject_key="discord_user:7", predicate_key="favorite_color")
        two = ledger.stable_entry_id(guild_id=1, source_table="conversations", source_row_id=500, entry_type="preference", subject_key="discord_user:7", predicate_key="favorite_color")
        self.assertEqual(one, two)

    def test_exact_source_dedup_but_separate_rows_survive(self):
        kwargs = dict(user_id=7, user_name="Crow", guild_id=1, role="user", content="remember this number: 8", channel_policy="sealed_test")
        first = ledger.shadow_conversation_row(self.conn, row_id=500, **kwargs)
        duplicate = ledger.shadow_conversation_row(self.conn, row_id=500, **kwargs)
        second = ledger.shadow_conversation_row(self.conn, row_id=501, **kwargs)
        self.conn.commit()
        self.assertEqual(first.entry_id, duplicate.entry_id)
        self.assertEqual(duplicate.outcome, "deduplicated")
        self.assertNotEqual(first.entry_id, second.entry_id)
        self.assertEqual(self.count_entries(), 2)

    def test_remember_directives_remain_raw_conversation_without_durable_lineage(self):
        ledger.shadow_conversation_row(self.conn, row_id=1, user_id=7, user_name="Crow", guild_id=1, role="user", content="remember this number: 731946", channel_policy="sealed_test", observed_at="2026-01-01T00:00:01+00:00")
        ledger.shadow_conversation_row(self.conn, row_id=2, user_id=7, user_name="Crow", guild_id=1, role="user", content="remember this number: 284517", channel_policy="sealed_test", observed_at="2026-01-01T00:00:02+00:00")
        rows = self.conn.execute("SELECT predicate_key, normalized_value, lifecycle_status FROM memory_ledger_entries ORDER BY source_sequence").fetchall()
        self.assertEqual(
            rows,
            [
                ("conversation", "remember this number: 731946", "active"),
                ("conversation", "remember this number: 284517", "active"),
            ],
        )
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM memory_ledger_lineage WHERE lineage_type IN ('supersedes','correction_of')").fetchone()[0], 0)

    def test_model_row_is_derived_and_cannot_establish_user_fact(self):
        ledger.shadow_conversation_row(self.conn, row_id=3, user_id=7, user_name="BNL-01", guild_id=1, role="model", content="I remember 284517.", channel_policy="sealed_test")
        row = self.conn.execute("SELECT entry_type, source_role, derived, projection, predicate_key, public_usable FROM memory_ledger_entries").fetchone()
        self.assertEqual(row, ("derived_summary", "model", 1, 1, "model_output", 0))

    def test_participants_are_queryable_and_ordered(self):
        entry = ledger.LedgerEntry(guild_id=1, source_table="manual", source_row_id=1, source_role="operator", entry_type="shared_moment", subject_key="moment:1", predicate_key="manual_shared_moment", participants=(ledger.LedgerParticipant("discord_user:2", "B", "participant", 1), ledger.LedgerParticipant("discord_user:1", "A", "participant", 0)))
        ledger.insert_ledger_entry(self.conn, entry)
        rows = self.conn.execute("SELECT participant_key FROM memory_ledger_participants WHERE entry_id=? ORDER BY order_index", (entry.entry_id,)).fetchall()
        self.assertEqual([r[0] for r in rows], ["discord_user:1", "discord_user:2"])

    def test_broadcast_uses_cleaned_summary_not_raw_note(self):
        ledger.shadow_broadcast_memory_row(self.conn, row_id=9, guild_id=1, cleaned_summary="Safe show summary", entry_type="show_note", public_safe=True, status="active", usage_scope="ambient,direct")
        row = self.conn.execute("SELECT normalized_value, public_usable, visibility FROM memory_ledger_entries").fetchone()
        self.assertEqual(row, ("Safe show summary", 1, "public_safe"))

    def test_queue_discussion_is_conversation_not_website_state(self):
        ledger.shadow_conversation_row(self.conn, row_id=10, user_id=7, user_name="Crow", guild_id=1, role="user", content="We joked that the queue is haunted.", channel_policy="public_home")
        row = self.conn.execute("SELECT source_table, predicate_key FROM memory_ledger_entries").fetchone()
        self.assertEqual(row, ("conversations", "conversation"))


    def test_number_directives_are_preserved_verbatim_as_conversation_rows(self):
        r1 = ledger.shadow_conversation_row(self.conn, row_id=20, user_id=7, user_name="Crow", guild_id=1, role="user", content="Remember this number: 8", channel_policy="sealed_test")
        r2 = ledger.shadow_conversation_row(self.conn, row_id=21, user_id=7, user_name="Crow", guild_id=1, role="user", content="remember this number: 123456789012", channel_policy="sealed_test")
        self.assertEqual((r1.outcome, r2.outcome), ("inserted", "inserted"))
        rows = self.conn.execute("SELECT predicate_key, normalized_value FROM memory_ledger_entries ORDER BY source_sequence").fetchall()
        self.assertEqual(
            rows,
            [
                ("conversation", "Remember this number: 8"),
                ("conversation", "remember this number: 123456789012"),
            ],
        )

    def test_all_remember_language_stays_conversation_only(self):
        samples = [
            "remember 8",
            "do you remember 9?",
            "I remember 10 people",
            "I do not remember 11",
            "what's up? / remember 12",
            "remember 13 / what's up?",
            "hey BNL, please remember the number 14",
            "remember 15 for me",
            ", remember 16",
            "Hey, BNL, remember 17",
            "bnlremember 18",
            "bnlplease remember 19",
        ]
        for row_id, content in enumerate(samples, start=22):
            ledger.shadow_conversation_row(
                self.conn,
                row_id=row_id,
                user_id=7,
                user_name="Crow",
                guild_id=1,
                role="user",
                content=content,
                channel_policy="sealed_test",
            )
        rows = self.conn.execute(
            "SELECT predicate_key, normalized_value FROM memory_ledger_entries ORDER BY source_sequence"
        ).fetchall()
        self.assertEqual(rows, [("conversation", content) for content in samples])
        self.assertEqual(
            self.conn.execute(
                "SELECT COUNT(*) FROM memory_ledger_entries WHERE predicate_key='remembered_number'"
            ).fetchone()[0],
            0,
        )

    def test_only_four_approved_source_linked_fields_are_projected(self):
        expected = {
            "preferred_name": "Crow",
            "pronouns": "they/them",
            "favorite_color": "green",
            "favorite_movie": "Hackers",
        }
        for row_id, (key, value) in enumerate(expected.items(), start=30):
            result = self.add_approved_fact(row_id, key, value)
            self.assertEqual(result.outcome, "inserted")
        rejected = self.add_approved_fact(40, "remembered_number", "8")
        self.assertEqual(rejected.outcome, "skipped")
        self.assertEqual(rejected.reason_code, "self_authored_fact_not_allowlisted")
        rows = self.conn.execute(
            """
            SELECT predicate_key, normalized_value, source_role, source_class,
                   channel_policy, source_message_id, public_usable
            FROM memory_ledger_entries
            ORDER BY source_sequence
            """
        ).fetchall()
        self.assertEqual(
            rows,
            [
                (key, value, "member_self_report", "first_party_record", "public_home", 1000 + row_id, 1)
                for row_id, (key, value) in enumerate(expected.items(), start=30)
            ],
        )

    def test_repetition_adds_no_authority_but_changed_approved_value_supersedes(self):
        original = self.add_approved_fact(
            40,
            "favorite_color",
            "green",
            observed_at="2026-01-01T00:00:01+00:00",
        )
        repeated = self.add_approved_fact(
            41,
            "favorite_color",
            "green",
            observed_at="2026-01-01T00:00:02+00:00",
        )
        changed = self.add_approved_fact(
            42,
            "favorite_color",
            "violet",
            observed_at="2026-01-01T00:00:03+00:00",
        )
        self.assertEqual(repeated.outcome, "skipped")
        self.assertEqual(repeated.reason_code, "repeated_self_authored_value")
        self.assertEqual(changed.outcome, "inserted")
        rows = self.conn.execute(
            """
            SELECT entry_id, normalized_value, lifecycle_status
            FROM memory_ledger_entries
            WHERE predicate_key='favorite_color'
            ORDER BY source_sequence
            """
        ).fetchall()
        self.assertEqual(
            rows,
            [
                (original.entry_id, "green", "superseded"),
                (changed.entry_id, "violet", "active"),
            ],
        )
        edges = set(self.conn.execute("SELECT lineage_type, target_entry_id FROM memory_ledger_lineage WHERE entry_id=?", (changed.entry_id,)).fetchall())
        self.assertEqual(
            edges,
            {
                ("correction_of", original.entry_id),
                ("supersedes", original.entry_id),
            },
        )
        report = ledger.build_memory_ledger_evaluation(self.conn, guild_id=1)
        self.assertEqual(report["entriesWithMultipleActiveValues"], 0)
        self.assertEqual(report["explicitCorrectionCounts"], 1)

    def test_all_lineage_targets_resolve_to_entries(self):
        self.add_approved_fact(50, "favorite_movie", "Hackers")
        self.add_approved_fact(51, "favorite_movie", "The Matrix")
        missing = self.conn.execute("""
            SELECT COUNT(*) FROM memory_ledger_lineage l
            LEFT JOIN memory_ledger_entries e ON e.guild_id=l.guild_id AND e.entry_id=l.target_entry_id
            WHERE e.entry_id IS NULL
        """).fetchone()[0]
        self.assertEqual(missing, 0)

    def test_broadcast_usage_scopes_public_usability(self):
        cases = [("ambient,direct", 1, "public_safe"), ("show_status", 1, "public_safe"), ("relay", 1, "public_safe"), ("internal", 0, "internal"), ("ambient", 0, "internal")]
        for idx, (scope, expected_public, expected_visibility) in enumerate(cases, start=60):
            ledger.shadow_broadcast_memory_row(self.conn, row_id=idx, guild_id=1, cleaned_summary=f"Summary {idx}", entry_type="show_note", public_safe=(idx != 64), status="active", usage_scope=scope)
            row = self.conn.execute("SELECT public_usable, visibility FROM memory_ledger_entries WHERE source_row_id=?", (str(idx),)).fetchone()
            self.assertEqual(row, (expected_public, expected_visibility))

    def test_broadcast_status_event_retracts_the_active_source_projection(self):
        self.conn.execute(
            """
            CREATE TABLE broadcast_memory(
                id INTEGER PRIMARY KEY,
                guild_id INTEGER NOT NULL,
                status TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                corrected_by_user_id INTEGER,
                corrected_by_name TEXT,
                superseded_by_id INTEGER
            )
            """
        )
        self.conn.execute(
            """
            INSERT INTO broadcast_memory(
                id,guild_id,status,updated_at,corrected_by_user_id,
                corrected_by_name,superseded_by_id
            ) VALUES(70,1,'active','2026-08-01T00:00:00+00:00',NULL,NULL,NULL)
            """
        )
        original = ledger.shadow_broadcast_memory_row(
            self.conn,
            row_id=70,
            guild_id=1,
            cleaned_summary="A current Broadcast fact.",
            entry_type="notable_moment",
            public_safe=True,
            status="active",
            usage_scope="ambient,direct",
            updated_at="2026-08-01T00:00:00+00:00",
        )
        self.conn.execute(
            """
            UPDATE broadcast_memory
            SET status='resolved',updated_at=?,corrected_by_user_id=?,
                corrected_by_name=?
            WHERE guild_id=1 AND id=70
            """,
            ("2026-08-01T01:00:00+00:00", 61, "configured_owner"),
        )
        status = ledger.shadow_broadcast_status_event(
            self.conn,
            row_id=70,
            guild_id=1,
            status="resolved",
            updated_at="2026-08-01T01:00:00+00:00",
            actor_id=61,
            actor_name="configured_owner",
        )
        self.assertEqual(status.outcome, "inserted")
        self.assertEqual(
            self.conn.execute(
                """
                SELECT lineage_type,target_entry_id
                FROM memory_ledger_lineage WHERE entry_id=?
                """,
                (status.entry_id,),
            ).fetchall(),
            [("retracts", original.entry_id)],
        )
        self.assertEqual(
            self.conn.execute(
                """
                SELECT COUNT(*) FROM memory_ledger_entries
                WHERE source_table='broadcast_memory'
                  AND source_role='broadcast_memory'
                  AND lifecycle_status='active'
                  AND entry_id NOT IN (
                    SELECT target_entry_id FROM memory_ledger_lineage
                    WHERE lineage_type IN ('supersedes','retracts')
                  )
                """
            ).fetchone()[0],
            0,
        )

    def test_broadcast_status_event_retracts_every_duplicate_effective_root(self):
        self.conn.execute(
            """
            CREATE TABLE broadcast_memory(
                id INTEGER PRIMARY KEY,
                guild_id INTEGER NOT NULL,
                status TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                corrected_by_user_id INTEGER,
                corrected_by_name TEXT,
                superseded_by_id INTEGER
            )
            """
        )
        self.conn.execute(
            "INSERT INTO broadcast_memory VALUES(72,1,'active',?,NULL,NULL,NULL)",
            ("2026-08-01T00:00:00+00:00",),
        )
        roots = tuple(
            ledger.shadow_broadcast_memory_row(
                self.conn,
                row_id=72,
                guild_id=1,
                cleaned_summary="Duplicate effective Broadcast root.",
                entry_type="notable_moment",
                public_safe=True,
                status="active",
                usage_scope="ambient,direct",
                updated_at=updated_at,
            ).entry_id
            for updated_at in (
                "2026-08-01T00:00:00+00:00",
                "2026-08-01T00:01:00+00:00",
            )
        )
        self.assertEqual(len(set(roots)), 2)
        self.conn.execute(
            """
            UPDATE broadcast_memory
            SET status='resolved',updated_at=?,corrected_by_user_id=?,
                corrected_by_name=? WHERE guild_id=1 AND id=72
            """,
            ("2026-08-01T01:00:00+00:00", 61, "configured_owner"),
        )
        status = ledger.shadow_broadcast_status_event(
            self.conn,
            row_id=72,
            guild_id=1,
            status="resolved",
            updated_at="2026-08-01T01:00:00+00:00",
            actor_id=61,
            actor_name="configured_owner",
        )
        self.assertEqual(status.outcome, "inserted")
        self.assertEqual(
            {
                row[0]
                for row in self.conn.execute(
                    """
                    SELECT target_entry_id FROM memory_ledger_lineage
                    WHERE entry_id=? AND lineage_type='retracts'
                    """,
                    (status.entry_id,),
                ).fetchall()
            },
            set(roots),
        )
        self.assertEqual(
            ledger._effective_broadcast_primary_entries(
                self.conn, guild_id=1, source_row_id=72
            ),
            (),
        )

    def test_broadcast_status_event_fails_closed_on_actor_or_stale_snapshot(self):
        self.conn.execute(
            """
            CREATE TABLE broadcast_memory(
                id INTEGER PRIMARY KEY,
                guild_id INTEGER NOT NULL,
                status TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                corrected_by_user_id INTEGER,
                corrected_by_name TEXT,
                superseded_by_id INTEGER
            )
            """
        )
        self.conn.execute(
            """
            INSERT INTO broadcast_memory VALUES(
                71,1,'resolved','2026-08-01T01:00:00+00:00',
                61,'configured_owner',NULL
            )
            """
        )
        original = ledger.shadow_broadcast_memory_row(
            self.conn,
            row_id=71,
            guild_id=1,
            cleaned_summary="A source that must not be retracted by stale input.",
            entry_type="notable_moment",
            public_safe=True,
            status="active",
            usage_scope="ambient,direct",
            updated_at="2026-08-01T00:00:00+00:00",
        )
        before_entries = self.count_entries()

        nonowner = ledger.shadow_broadcast_status_event(
            self.conn,
            row_id=71,
            guild_id=1,
            status="resolved",
            updated_at="2026-08-01T01:00:00+00:00",
            actor_id=62,
            actor_name="forged",
        )
        stale = ledger.shadow_broadcast_status_event(
            self.conn,
            row_id=71,
            guild_id=1,
            status="resolved",
            updated_at="2026-08-01T00:59:59+00:00",
            actor_id=61,
            actor_name="configured_owner",
        )

        self.assertEqual(nonowner.outcome, "skipped")
        self.assertEqual(
            nonowner.reason_code,
            "broadcast_status_configured_owner_required",
        )
        self.assertEqual(stale.outcome, "skipped")
        self.assertEqual(
            stale.reason_code,
            "broadcast_status_source_snapshot_mismatch",
        )
        self.assertEqual(self.count_entries(), before_entries)
        self.assertEqual(
            self.conn.execute(
                "SELECT COUNT(*) FROM memory_ledger_lineage"
            ).fetchone()[0],
            0,
        )
        self.assertEqual(
            self.conn.execute(
                "SELECT lifecycle_status FROM memory_ledger_entries WHERE entry_id=?",
                (original.entry_id,),
            ).fetchone()[0],
            "active",
        )

    def test_evaluation_detects_missing_success_receipt_entry_and_dangling_lineage(self):
        ledger.record_shadow_receipt(self.conn, guild_id=1, writer="test", source_table="conversations", source_row_id=999, outcome="inserted", reason_code="ok", entry_id="missing")
        self.conn.execute("INSERT INTO memory_ledger_lineage(entry_id, guild_id, lineage_type, target_entry_id, created_at) VALUES ('source', 1, 'supersedes', 'missing-target', 'now')")
        report = ledger.build_memory_ledger_evaluation(self.conn, guild_id=1)
        self.assertGreaterEqual(report["legacyToLedgerParityMismatches"], 2)
        self.assertEqual(report["danglingLineageTargets"], 1)

    def test_shadow_gate_default_disabled(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertFalse(ledger.shadow_enabled())


if __name__ == "__main__":
    unittest.main()
