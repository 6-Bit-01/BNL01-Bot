import os
import sqlite3
import tempfile
import unittest
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl01_bot
from bnl_memory_governance import (
    GovernanceRequest,
    build_governed_context,
    correct_member_memory,
    ensure_governance_schema,
    forget_member_memory,
    view_member_memory,
)
from bnl_memory_ledger import subject_key_for_user


class MemberMemoryIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.old_db = bnl01_bot.DB_FILE
        self.tmp = tempfile.NamedTemporaryFile(delete=False)
        self.tmp.close()
        bnl01_bot.DB_FILE = self.tmp.name
        self.env = mock.patch.dict(os.environ, {}, clear=False)
        self.env.start()
        os.environ["BNL_MEMORY_LEDGER_SHADOW_ENABLED"] = "1"
        os.environ["BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED"] = "1"
        os.environ.pop("BNL_MEMORY_GOVERNANCE_LIVE_ENABLED", None)
        bnl01_bot.init_db()
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            ensure_governance_schema(conn)

    def tearDown(self):
        self.env.stop()
        bnl01_bot.DB_FILE = self.old_db
        try:
            os.unlink(self.tmp.name)
        except OSError:
            pass

    def save_direct(self, text, *, user_id=42, message_id=1):
        return bnl01_bot.save_user_message(
            user_id,
            "Crow",
            1,
            text,
            channel_name="barcode-bot",
            channel_policy="public_home",
            channel_id=99,
            message_id=message_id,
            directed_to_bnl=True,
        )

    def approved_ref(self, expected_summary, *, user_id=42):
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            rows = view_member_memory(
                conn,
                guild_id=1,
                user_id=user_id,
                limit=100,
            )
        return next(
            item["ref"]
            for item in rows
            if item["kind"] == "member-authored fact"
            and item["summary"].lower() == expected_summary.lower()
        )

    def test_direct_approved_fact_has_persisted_provenance_and_is_not_core(self):
        self.save_direct("My favorite movie is Hackers.", message_id=901)
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            row = conn.execute(
                """
                SELECT fact_key, fact_value, is_core, source_conversation_row_id,
                       source_message_id, source_channel_policy, source_route_mode,
                       source_kind, source_directed, source_ledger_entry_id,
                       lifecycle_status
                FROM user_memory_facts
                WHERE guild_id=1 AND user_id=42
                """
            ).fetchone()
        self.assertEqual(row[0:3], ("favorite_movie", "Hackers", 0))
        self.assertGreater(row[3], 0)
        self.assertEqual(row[4], 901)
        self.assertEqual(row[5], "public_home")
        self.assertEqual(row[6], bnl01_bot.ROUTE_MODE_NORMAL_CHAT)
        self.assertEqual(row[7:9], ("member_self_report", 1))
        self.assertTrue(row[9].startswith("mle_"))
        self.assertEqual(row[10], "active")

    def test_source_row_id_alone_does_not_prove_direct_self_report(self):
        bnl01_bot.upsert_user_fact(
            42,
            1,
            "favorite_color",
            "green",
            0.9,
            source_conversation_row_id=777,
            source_kind="member_self_report",
            source_channel_policy="public_home",
        )
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            directed = conn.execute(
                """
                SELECT source_directed
                FROM user_memory_facts
                WHERE guild_id=1 AND user_id=42 AND fact_key='favorite_color'
                """
            ).fetchone()[0]
            first_party = conn.execute(
                """
                SELECT COUNT(*)
                FROM memory_ledger_entries
                WHERE guild_id=1
                  AND subject_key=?
                  AND predicate_key='favorite_color'
                  AND source_class='first_party_record'
                """,
                (subject_key_for_user(42),),
            ).fetchone()[0]
        self.assertEqual(directed, 0)
        self.assertEqual(first_party, 0)
        self.assertEqual(
            bnl01_bot.get_approved_member_fact_evidence(42, 1),
            (),
        )

    def test_direct_flag_without_stable_source_does_not_approve_fact(self):
        bnl01_bot.upsert_user_fact(
            42,
            1,
            "favorite_color",
            "green",
            0.9,
            source_kind="member_self_report",
            source_directed=True,
            source_channel_policy="public_home",
        )
        bnl01_bot.upsert_user_fact(
            43,
            1,
            "preferred_name",
            "Nova",
            0.99,
            source_kind="member_control",
            source_directed=True,
            source_channel_policy="member_control",
        )
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            sources = conn.execute(
                """
                SELECT user_id, source_directed, source_conversation_row_id,
                       source_control_ref
                FROM user_memory_facts
                WHERE guild_id=1 AND user_id IN (42,43)
                ORDER BY user_id
                """
            ).fetchall()
        self.assertEqual(sources, [(42, 1, 0, ""), (43, 1, 0, "")])
        self.assertEqual(
            bnl01_bot.get_approved_member_fact_evidence(42, 1),
            (),
        )
        self.assertEqual(
            bnl01_bot.get_approved_member_fact_evidence(43, 1),
            (),
        )

    def test_member_name_control_and_direct_correction_share_one_lifecycle(self):
        bnl01_bot.upsert_user_fact(
            42,
            1,
            "preferred_name",
            "Nova",
            0.99,
            source_channel_policy="member_control",
            source_route_mode="member_control",
            source_kind="member_control",
            source_directed=True,
            source_control_ref="discord_interaction:1234",
        )
        self.assertEqual(bnl01_bot.get_user_profile(42, 1)[1], "Nova")
        evidence = bnl01_bot.get_approved_member_fact_evidence(42, 1)
        self.assertEqual(
            [(item.key, item.value, item.source_kind) for item in evidence],
            [("preferred_name", "Nova", "member_control")],
        )
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            row = conn.execute(
                """
                SELECT source_role, source_class, lifecycle_status,
                       normalized_value
                FROM memory_ledger_entries
                WHERE guild_id=1 AND subject_key=?
                  AND predicate_key='preferred_name'
                  AND source_role='member_control'
                """,
                (subject_key_for_user(42),),
            ).fetchone()
        self.assertEqual(
            row,
            ("member_control", "first_party_record", "active", "Nova"),
        )

        self.save_direct("Please call me Iris.", message_id=5678)
        self.assertEqual(bnl01_bot.get_user_profile(42, 1)[1], "Iris")
        evidence = bnl01_bot.get_approved_member_fact_evidence(42, 1)
        self.assertEqual(
            [
                (
                    item.key,
                    item.value,
                    item.source_kind,
                    item.conversation_row_id,
                )
                for item in evidence
            ],
            [("preferred_name", "Iris", "member_self_report", 1)],
        )
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            facts = conn.execute(
                """
                SELECT fact_value, source_kind, source_directed,
                       source_conversation_row_id, source_control_ref,
                       lifecycle_status, source_ledger_entry_id
                FROM user_memory_facts
                WHERE guild_id=1 AND user_id=42
                  AND fact_key='preferred_name'
                """
            ).fetchall()
            ledger_rows = conn.execute(
                """
                SELECT source_role, normalized_value, lifecycle_status,
                       entry_id
                FROM memory_ledger_entries
                WHERE guild_id=1 AND subject_key=?
                  AND predicate_key='preferred_name'
                  AND source_class='first_party_record'
                """,
                (subject_key_for_user(42),),
            ).fetchall()
            lineage_rows = conn.execute(
                """
                SELECT entry_id, lineage_type, target_entry_id
                FROM memory_ledger_lineage
                WHERE guild_id=1
                """
            ).fetchall()
        self.assertEqual(len(facts), 1)
        self.assertEqual(
            facts[0][0:6],
            ("Iris", "member_self_report", 1, 1, "", "active"),
        )
        self.assertTrue(facts[0][6].startswith("mle_"))
        ledger_by_value = {
            value: (role, lifecycle, entry_id)
            for role, value, lifecycle, entry_id in ledger_rows
        }
        self.assertEqual(
            ledger_by_value["Nova"][0:2],
            ("member_control", "superseded"),
        )
        self.assertEqual(
            ledger_by_value["Iris"][0:2],
            ("member_self_report", "active"),
        )
        self.assertEqual(
            {
                (entry_id, lineage_type, target_entry_id)
                for entry_id, lineage_type, target_entry_id in lineage_rows
                if entry_id == ledger_by_value["Iris"][2]
                and target_entry_id == ledger_by_value["Nova"][2]
            },
            {
                (
                    ledger_by_value["Iris"][2],
                    "correction_of",
                    ledger_by_value["Nova"][2],
                ),
                (
                    ledger_by_value["Iris"][2],
                    "supersedes",
                    ledger_by_value["Nova"][2],
                ),
            },
        )

    def test_governed_correction_preserves_member_control_evidence_lane(self):
        bnl01_bot.upsert_user_fact(
            42,
            1,
            "preferred_name",
            "Nova",
            0.99,
            source_channel_policy="member_control",
            source_route_mode="member_control",
            source_kind="member_control",
            source_directed=True,
            source_control_ref="discord_interaction:member-name",
        )
        nova_ref = self.approved_ref("Nova")
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            result = correct_member_memory(
                conn,
                guild_id=1,
                user_id=42,
                safe_ref=nova_ref,
                corrected_text="Iris",
            )
        self.assertTrue(result["ok"])
        evidence = bnl01_bot.get_approved_member_fact_evidence(42, 1)
        self.assertEqual(
            [(item.key, item.value, item.source_kind) for item in evidence],
            [("preferred_name", "Iris", "member_control")],
        )
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            row = conn.execute(
                """
                SELECT fact_value,source_kind,source_channel_policy,
                       source_conversation_row_id,source_control_ref,
                       lifecycle_status
                FROM user_memory_facts
                WHERE guild_id=1 AND user_id=42
                  AND fact_key='preferred_name'
                """
            ).fetchone()
        self.assertEqual(row[0:4], ("Iris", "member_control", "member_control", 0))
        self.assertTrue(row[4].startswith("mgr_"))
        self.assertEqual(row[5], "active")
        self.assertEqual(bnl01_bot.get_user_profile(42, 1)[1], "Iris")

    def test_fact_clause_parser_keeps_values_separate_from_followup_actions(self):
        cases = {
            "Call me Nova and wave": [("preferred_name", "Nova")],
            "My favorite color is blue and tell me a joke": [
                ("favorite_color", "blue")
            ],
            (
                "My favorite color is blue and my favorite movie is "
                "Apocalypse Now"
            ): [
                ("favorite_color", "blue"),
                ("favorite_movie", "Apocalypse Now"),
            ],
            "Use she/her pronouns for me please": [
                ("pronouns", "she/her")
            ],
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                self.assertEqual(
                    [(key, value) for key, value, _ in bnl01_bot.extract_user_facts(text)],
                    expected,
                )

    def test_non_direct_and_ambiguous_personal_language_do_not_write_facts(self):
        samples = (
            ("My favorite color is blue.", False),
            ('Crow said, "my favorite movie is Hackers."', True),
            ("In this scene, my favorite color is blue.", True),
            ("My favorite color is no longer blue.", True),
            ("What if my favorite movie is Hackers?", True),
            ("Remember my backup contact is Leo.", True),
        )
        for index, (text, directed) in enumerate(samples, start=1):
            bnl01_bot.save_user_message(
                42,
                "Crow",
                1,
                text,
                channel_name="barcode-bot",
                channel_policy="public_home",
                channel_id=99,
                message_id=index,
                directed_to_bnl=directed,
            )
        bnl01_bot.save_user_message(
            42,
            "Crow",
            1,
            "My favorite color is violet.",
            channel_name="music",
            channel_policy="public_selective",
            channel_id=100,
            message_id=100,
            directed_to_bnl=True,
        )
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            count = conn.execute(
                "SELECT COUNT(*) FROM user_memory_facts WHERE guild_id=1 AND user_id=42"
            ).fetchone()[0]
        self.assertEqual(count, 0)

    def test_source_linked_fact_survives_transcript_pruning_while_legacy_row_stays_hidden(self):
        self.save_direct("My favorite movie is Hackers.", message_id=77)
        bnl01_bot.upsert_user_fact(43, 1, "favorite_color", "orange", 0.99)
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            conn.execute("DELETE FROM conversations WHERE user_id=42 AND guild_id=1")
            conn.commit()

        evidence = bnl01_bot.get_approved_member_fact_evidence(42, 1)
        self.assertEqual([(item.key, item.value) for item in evidence], [("favorite_movie", "Hackers")])
        self.assertEqual(bnl01_bot.get_approved_member_fact_evidence(43, 1), ())
        self.assertIn(
            "Hackers",
            bnl01_bot.try_memory_recall_response(
                42,
                1,
                "What is my favorite movie?",
            ),
        )
        self.assertNotIn(
            "orange",
            bnl01_bot.try_memory_recall_response(
                43,
                1,
                "What do you know about me?",
            ),
        )

    def test_personal_recall_uses_only_three_recent_public_observations(self):
        messages = (
            "The broadcast mix and radio show need another music pass.",
            "The VPS deploy and server restart need an infrastructure check.",
            "This bug error traceback needs a careful debugging fix.",
            "The BARCODE canon and lore need a consistent answer.",
            "That joke and meme landed as good community banter.",
        )
        for index, text in enumerate(messages, start=1):
            self.save_direct(text, message_id=1000 + index)

        basis = bnl01_bot.build_personal_recall_basis(
            42,
            1,
            current_text="What do you know about me?",
        )
        self.assertEqual(basis.recent_public_message_count, 3)
        self.assertEqual(
            set(basis.recent_public_topics),
            {
                "fixes and troubleshooting",
                "BARCODE lore and canon",
                "jokes and community banter",
            },
        )
        self.assertNotIn(
            "infrastructure and deployment work",
            basis.recent_public_topics,
        )
        self.assertNotIn(
            "music and BARCODE Radio",
            basis.recent_public_topics,
        )
        rendered = bnl01_bot.render_personal_recall_basis(basis)
        self.assertIn("recent observations, not permanent facts", rendered)

    def test_correction_forget_and_later_self_report_share_one_live_owner(self):
        self.save_direct("My favorite color is blue.", message_id=1)
        blue_ref = self.approved_ref("blue")
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            corrected = correct_member_memory(
                conn,
                guild_id=1,
                user_id=42,
                safe_ref=blue_ref,
                corrected_text="My favorite color is green.",
            )
        self.assertTrue(corrected["ok"])
        self.assertIn(
            "green",
            bnl01_bot.try_memory_recall_response(
                42,
                1,
                "What is my favorite color?",
            ).lower(),
        )

        self.save_direct("My favorite color is red.", message_id=2)
        recalled = bnl01_bot.try_memory_recall_response(
            42,
            1,
            "What is my favorite color?",
        ).lower()
        self.assertIn("red", recalled)
        self.assertNotIn("green", recalled)

        red_ref = self.approved_ref("red")
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            forgotten = forget_member_memory(
                conn,
                guild_id=1,
                user_id=42,
                safe_ref=red_ref,
            )
        self.assertTrue(forgotten["ok"])
        after_forget = bnl01_bot.try_memory_recall_response(
            42,
            1,
            "What is my favorite color?",
        )
        self.assertEqual(
            after_forget,
            "I don't have your favorite color reliably recorded yet.",
        )

    def test_forgetting_preferred_name_clears_addressing_projection(self):
        self.save_direct("Please call me Nova.", message_id=11)
        self.assertEqual(bnl01_bot.get_user_profile(42, 1)[1], "Nova")
        ref = self.approved_ref("Nova")
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            result = forget_member_memory(
                conn,
                guild_id=1,
                user_id=42,
                safe_ref=ref,
            )
        self.assertTrue(result["ok"])
        self.assertIsNone(bnl01_bot.get_user_profile(42, 1)[1])
        self.assertEqual(bnl01_bot.get_approved_member_fact_evidence(42, 1), ())

    def test_prompt_keeps_conversation_traces_distinct_from_approved_facts(self):
        self.save_direct("My favorite movie is Hackers.", message_id=31)
        bnl01_bot._add_memory_tier_entry(
            42,
            1,
            "medium",
            "I love green motorcycles and this is my permanent preference.",
            0.9,
            source_role="user",
            source_channel_policy="public_home",
            source_channel_name="barcode-bot",
            source_origin="conversations",
            source_trust="source_safe_public",
        )
        bnl01_bot._add_memory_tier_entry(
            42,
            1,
            "medium",
            "The antenna calibration failed after the third retry.",
            0.8,
            source_role="user",
            source_channel_policy="public_home",
            source_channel_name="barcode-bot",
            source_origin="conversations",
            source_trust="source_safe_public",
        )
        context = bnl01_bot.build_user_memory_context(
            42,
            1,
            channel_policy="public_home",
            user_text="Continue troubleshooting the antenna calibration.",
            current_direct=True,
        )
        self.assertIn("Approved direct self-reports", context)
        self.assertIn("Favorite movie: Hackers", context)
        self.assertIn("Derived memory summaries", context)
        self.assertIn("antenna calibration failed", context)
        self.assertNotIn("green motorcycles", context)
        self.assertIn(
            "neither exact prior messages nor verified personal facts",
            context,
        )

        with (
            mock.patch.object(
                bnl01_bot,
                "get_user_profile",
                return_value=("Crow", ""),
            ),
            mock.patch.object(
                bnl01_bot,
                "should_allow_greeting",
                return_value=False,
            ),
            mock.patch.object(
                bnl01_bot,
                "choose_response_style",
                return_value=("steady_reply", "Respond naturally."),
            ),
            mock.patch.object(
                bnl01_bot,
                "build_broadcast_memory_context",
                return_value="",
            ),
        ):
            prompt, _allow_greeting, _style = bnl01_bot.build_user_aware_prompt(
                42,
                1,
                "Crow",
                "Continue troubleshooting the antenna calibration.",
                channel_name="barcode-bot",
                channel_policy="public_home",
                route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                is_direct_interaction=True,
            )
        self.assertIn("antenna calibration failed", prompt)
        # A lower-authority derived tier can inform relevance, but it is not a
        # recent-message transcript and must not activate the typed
        # follow-through guard on its own.
        self.assertNotIn("[CONVERSATION_CONTINUITY_REQUIRED]", prompt)

    def test_governance_rejects_arbitrary_first_party_preferences(self):
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            ensure_governance_schema(conn)
            conn.execute(
                """
                INSERT INTO memory_ledger_entries (
                    entry_id, schema_version, guild_id, subject_key,
                    subject_display_name, entry_type, predicate_key,
                    normalized_value, source_class, source_table, source_row_id,
                    source_revision, source_role, visibility, confidence,
                    public_usable, derived, projection, salience, observed_at,
                    lifecycle_status, created_at, updated_at, route_mode,
                    channel_policy
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    "arbitrary-preference",
                    "memory_ledger_v1",
                    1,
                    subject_key_for_user(42),
                    "Crow",
                    "preference",
                    "favorite_food",
                    "pizza",
                    "first_party_record",
                    "test",
                    "1",
                    "1",
                    "member_self_report",
                    "public_safe",
                    "high",
                    1,
                    0,
                    0,
                    0.9,
                    "2026-07-01T00:00:00+00:00",
                    "active",
                    "2026-07-01T00:00:00+00:00",
                    "2026-07-01T00:00:00+00:00",
                    bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                    "public_home",
                ),
            )
            result = build_governed_context(
                conn,
                GovernanceRequest(
                    guild_id=1,
                    subject_user_id=42,
                    route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                    conversation_surface="test",
                    channel_policy="public_home",
                    visibility_allowance="public_safe",
                    user_text="What do you remember about me?",
                    budget_chars=500,
                    allowed_source_classes=("first_party_record",),
                    now="2026-07-20T00:00:00+00:00",
                ),
            )
        self.assertEqual(result.rendered_context, "")
        self.assertEqual(
            result.diagnostics.excluded_by_reason.get("member_fact_not_allowlisted"),
            1,
        )

    def test_ambient_ignores_historical_core_and_mixed_legacy_tiers(self):
        bnl01_bot.update_user_habits(
            42,
            1,
            "We repaired the antenna controller after three retries.",
        )
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            conn.execute(
                """
                INSERT INTO user_memory_facts (
                    user_id, guild_id, fact_key, fact_value, confidence,
                    is_core, updated_at
                ) VALUES (42,1,'user_note','PRIVATE LEGACY CORE',1.0,1,'2026-07-01')
                """
            )
            conn.commit()
        bnl01_bot._add_memory_tier_entry(
            42,
            1,
            "long",
            "PRIVATE MIXED LEGACY TRACE",
            1.0,
            source_role="legacy_unknown",
            source_channel_policy="legacy_unknown",
            source_origin="legacy",
            source_trust="mixed_or_legacy_consolidated",
        )
        bnl01_bot._add_memory_tier_entry(
            42,
            1,
            "short",
            "Public antenna repair thread.",
            0.8,
            source_role="user",
            source_channel_policy="public_home",
            source_origin="conversations",
            source_trust="source_safe_public",
        )
        snapshot = bnl01_bot.get_guild_curiosity_snapshot(1, limit_users=3)
        self.assertEqual(len(snapshot), 1)
        self.assertEqual(snapshot[0]["core_fact"], "none")
        self.assertEqual(snapshot[0]["long_trace"], "none")
        self.assertEqual(snapshot[0]["short_trace"], "Public antenna repair thread.")
        self.assertNotIn("PRIVATE", str(snapshot))


if __name__ == "__main__":
    unittest.main()
