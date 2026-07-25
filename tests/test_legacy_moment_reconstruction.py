import os
import sqlite3
import unittest
from datetime import datetime, timedelta, timezone

import bnl_memory_ledger as ledger
import bnl_moment_engine as moments


class LegacyMomentReconstructionTests(unittest.TestCase):
    def setUp(self):
        os.environ["BNL_MEMORY_LEDGER_SHADOW_ENABLED"] = "1"
        os.environ["BNL_MOMENT_ENGINE_SHADOW_ENABLED"] = "1"
        self.conn = sqlite3.connect(":memory:")
        ledger.ensure_memory_ledger_schema(self.conn)
        moments.ensure_moment_schema(self.conn)

    def tearDown(self):
        self.conn.close()

    def add_source(
        self,
        row_id,
        role,
        text,
        *,
        minute,
        channel_id=10,
    ):
        observed_at = (
            datetime(2026, 1, 10, 12, 0, tzinfo=timezone.utc)
            + timedelta(minutes=minute)
        ).isoformat()
        return ledger.shadow_conversation_row(
            self.conn,
            row_id=row_id,
            user_id=6,
            user_name="Test Member" if role == "user" else "BNL-01",
            guild_id=1,
            role=role,
            content=text,
            channel_policy="public_home",
            channel_id=channel_id,
            channel_name="barcode-bot",
            route_mode="normal_chat",
            observed_at=observed_at,
        )

    def add_legacy_window(self, moment_id, sources, *, channel_id=10):
        started = "2026-01-10T12:00:00+00:00"
        ended = "2026-01-10T12:08:00+00:00"
        canonical = ledger.insert_ledger_entry(
            self.conn,
            ledger.LedgerEntry(
                guild_id=1,
                source_table="memory_moment_windows",
                source_row_id=moment_id,
                source_revision="legacy",
                source_role="derived_assessment",
                entry_type="shared_moment",
                subject_key=f"moment:{moment_id}",
                predicate_key="shared_moment",
                value="",
                source_class=ledger.SourceClass.DERIVED_SUMMARY,
                route_mode="normal_chat",
                channel_id=channel_id,
                channel_name="barcode-bot",
                channel_policy="public_home",
                visibility=ledger.Visibility.PUBLIC,
                confidence=ledger.Confidence.LOW,
                public_usable=False,
                derived=True,
                projection=True,
                lifecycle_status="quarantined",
            ),
        )
        self.conn.execute(
            """
            INSERT INTO memory_moment_windows(
              moment_id,guild_id,channel_id,channel_name,channel_policy,
              route_mode,topic_key,topic_family,topic_signature,
              window_started_at,last_activity_at,finalized_at,
              qualification_type,qualification_reason,lifecycle_status,
              visibility,public_usable,salience,human_entry_count,
              model_entry_count,participant_count,summary,created_at,
              updated_at,canonical_ledger_entry_id
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                moment_id,
                1,
                channel_id,
                "barcode-bot",
                "public_home",
                "normal_chat",
                "topic_legacy",
                "",
                "[]",
                started,
                ended,
                ended,
                "conversational",
                "legacy_extractive_projection",
                "retracted",
                "public",
                0,
                0.5,
                sum(source_role == "user" for source_role, _source in sources),
                sum(source_role != "user" for source_role, _source in sources),
                1,
                "",
                ended,
                ended,
                canonical.entry_id,
            ),
        )
        for order, (source_role, source) in enumerate(sources):
            self.conn.execute(
                """
                INSERT INTO memory_moment_members(
                  moment_id,ledger_entry_id,source_sequence,observed_at,
                  membership_role,created_at
                ) VALUES(?,?,?,?,?,?)
                """,
                (
                    moment_id,
                    source.entry_id,
                    order,
                    (
                        datetime(2026, 1, 10, 12, 0, tzinfo=timezone.utc)
                        + timedelta(minutes=order)
                    ).isoformat(),
                    (
                        "human_author"
                        if source_role == "user"
                        else "bnl_participant"
                    ),
                    ended,
                ),
            )
        return canonical.entry_id

    def question_thread_exchange(self):
        messages = (
            (
                "user",
                "If this construct dreams, does that mean it is alive?",
            ),
            (
                "model",
                "Dreaming can reflect synthesized states. "
                "[SIGNAL_DECAY::0x4E]",
            ),
            ("user", "Does being alive depend on dreaming?"),
            ("model", "That is a question about existence."),
            (
                "user",
                "If the construct was programmed, can its identity "
                "still be authentic?",
            ),
            ("model", "Programmed origins do not settle authentic identity."),
            (
                "user",
                "Could anxiety connect those questions about programmed "
                "identity?",
            ),
            ("model", "Anxiety can create questions about architecture."),
        )
        return tuple(
            (
                role,
                self.add_source(
                    1001 + offset,
                    role,
                    text,
                    minute=offset,
                ),
            )
            for offset, (role, text) in enumerate(messages)
        )

    def test_rebuilds_question_thread_without_reviving_legacy_record(self):
        legacy_id = "mom_legacy_question_thread"
        sources = self.question_thread_exchange()
        legacy_canonical_id = self.add_legacy_window(legacy_id, sources)

        report = moments.reconstruct_legacy_moments(self.conn)

        self.assertEqual(report["reconstructed"], 1)
        self.assertEqual(report["errors"], 0)
        reconstructed_id = moments.stable_reconstructed_moment_id(legacy_id)
        self.assertEqual(
            self.conn.execute(
                """
                SELECT lifecycle_status,qualification_reason,summary,
                       public_usable
                FROM memory_moment_windows WHERE moment_id=?
                """,
                (legacy_id,),
            ).fetchone(),
            ("retracted", "legacy_extractive_projection", "", 0),
        )
        self.assertEqual(
            self.conn.execute(
                """
                SELECT normalized_value,lifecycle_status,public_usable
                FROM memory_ledger_entries WHERE entry_id=?
                """,
                (legacy_canonical_id,),
            ).fetchone(),
            ("", "quarantined", 0),
        )
        rebuilt = self.conn.execute(
            """
            SELECT lifecycle_status,qualification_type,public_usable,
                   canonical_ledger_entry_id
            FROM memory_moment_windows WHERE moment_id=?
            """,
            (reconstructed_id,),
        ).fetchone()
        self.assertEqual(rebuilt[:3], ("finalized", "conversational", 1))
        self.assertTrue(rebuilt[3].startswith("mle_"))
        frame_type, gist = self.conn.execute(
            """
            SELECT frame_type,contribution_gist
            FROM memory_moment_contributions
            WHERE moment_id=? AND participant_key='discord_user:6'
            """,
            (reconstructed_id,),
        ).fetchone()
        self.assertEqual(frame_type, "question_thread")
        self.assertEqual(
            gist,
            "The participant explored connected questions involving "
            "dreaming, being alive, programmed identity, and anxiety.",
        )
        rendered = moments.render_shadow_moment_context(
            self.conn,
            guild_id=1,
            channel_id=99,
            participant_key="discord_user:6",
            visibility="public_safe",
            topic_text=(
                "BNL, without quoting me, remind me of the main point "
                "from our earlier public discussion about dreaming. "
                "What was the gist of what I was trying to decide?"
            ),
            token_budget=160,
            freshness_days=3650,
            allow_cross_channel=True,
            allowed_channel_policies=("public_home", "public_context"),
        )
        self.assertIn(gist, rendered)
        self.assertNotIn("SIGNAL_DECAY", rendered)
        self.assertNotIn("authentic identity", rendered)
        audit = self.conn.execute(
            """
            SELECT reconstructed_moment_id,outcome,reason_code
            FROM memory_moment_reconstructions
            WHERE legacy_moment_id=?
            """,
            (legacy_id,),
        ).fetchone()
        self.assertEqual(
            audit,
            (
                reconstructed_id,
                "reconstructed",
                "eligible_sources_rebuilt",
            ),
        )

        second = moments.reconstruct_legacy_moments(self.conn)
        self.assertEqual(second["reconstructed"], 0)
        self.assertEqual(second["deduplicated"], 1)
        self.assertEqual(
            self.conn.execute(
                """
                SELECT COUNT(*) FROM memory_moment_windows
                WHERE moment_id IN (?,?)
                """,
                (legacy_id, reconstructed_id),
            ).fetchone()[0],
            2,
        )

    def test_sensitive_sources_stay_retracted(self):
        sensitive_sources = (
            (
                "user",
                self.add_source(
                    5001,
                    "user",
                    "My password is Opal-4829",
                    minute=0,
                ),
            ),
            (
                "model",
                self.add_source(
                    5002,
                    "model",
                    "I cannot store that.",
                    minute=1,
                ),
            ),
            (
                "user",
                self.add_source(
                    5003,
                    "user",
                    "Should that secret become a memory?",
                    minute=2,
                ),
            ),
            (
                "user",
                self.add_source(
                    5004,
                    "user",
                    "Why would that be remembered?",
                    minute=3,
                ),
            ),
        )
        legacy_id = "mom_legacy_sensitive"
        self.add_legacy_window(legacy_id, sensitive_sources)

        report = moments.reconstruct_legacy_moments(self.conn)

        self.assertEqual(report["reconstructed"], 0)
        self.assertEqual(report["skipped"], 1)
        self.assertIsNone(
            self.conn.execute(
                """
                SELECT 1 FROM memory_moment_windows WHERE moment_id=?
                """,
                (moments.stable_reconstructed_moment_id(legacy_id),),
            ).fetchone()
        )
        self.assertEqual(
            self.conn.execute(
                """
                SELECT outcome,reason_code
                FROM memory_moment_reconstructions
                WHERE legacy_moment_id=?
                """,
                (legacy_id,),
            ).fetchone(),
            ("skipped", "sensitive_source_excluded"),
        )

    def test_scope_mismatched_sources_stay_retracted(self):
        legacy_id = "mom_legacy_wrong_scope"
        self.add_legacy_window(
            legacy_id,
            self.question_thread_exchange(),
            channel_id=11,
        )

        report = moments.reconstruct_legacy_moments(self.conn)

        self.assertEqual(report["reconstructed"], 0)
        self.assertEqual(report["skipped"], 1)
        self.assertIsNone(
            self.conn.execute(
                """
                SELECT 1 FROM memory_moment_windows WHERE moment_id=?
                """,
                (moments.stable_reconstructed_moment_id(legacy_id),),
            ).fetchone()
        )
        self.assertEqual(
            self.conn.execute(
                """
                SELECT outcome,reason_code
                FROM memory_moment_reconstructions
                WHERE legacy_moment_id=?
                """,
                (legacy_id,),
            ).fetchone(),
            ("skipped", "source_scope_mismatch"),
        )

    def test_schema_migration_runs_once_for_existing_quarantine(self):
        legacy_id = "mom_legacy_migration"
        self.add_legacy_window(legacy_id, self.question_thread_exchange())
        self.conn.execute(
            "DELETE FROM memory_moment_migrations WHERE migration_key=?",
            (moments.LEGACY_MOMENT_RECONSTRUCTION_MIGRATION,),
        )

        moments.ensure_moment_schema(self.conn)
        first_count = self.conn.execute(
            """
            SELECT COUNT(*) FROM memory_moment_windows
            WHERE moment_id=?
            """,
            (moments.stable_reconstructed_moment_id(legacy_id),),
        ).fetchone()[0]
        moments.ensure_moment_schema(self.conn)
        second_count = self.conn.execute(
            """
            SELECT COUNT(*) FROM memory_moment_windows
            WHERE moment_id=?
            """,
            (moments.stable_reconstructed_moment_id(legacy_id),),
        ).fetchone()[0]

        self.assertEqual((first_count, second_count), (1, 1))
        self.assertEqual(
            self.conn.execute(
                """
                SELECT COUNT(*) FROM memory_moment_migrations
                WHERE migration_key=?
                """,
                (moments.LEGACY_MOMENT_RECONSTRUCTION_MIGRATION,),
            ).fetchone()[0],
            1,
        )


if __name__ == "__main__":
    unittest.main()
