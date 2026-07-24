import os
import sqlite3
import tempfile
import unittest
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl01_bot
from bnl_memory_governance import ensure_governance_schema
from bnl_memory_ledger import subject_key_for_user


class ExplicitRecallGovernanceTests(unittest.TestCase):
    def setUp(self):
        self.old_db = bnl01_bot.DB_FILE
        self.tmp = tempfile.NamedTemporaryFile(delete=False)
        self.tmp.close()
        bnl01_bot.DB_FILE = self.tmp.name
        self.env = mock.patch.dict(os.environ, {}, clear=False)
        self.env.start()
        os.environ.pop("BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED", None)
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

    def rows(self, sql, params=()):
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            return conn.execute(sql, params).fetchall()

    def insert_ledger(self, value="favorite color is green", *, public=1, vis="public_safe", life="active", user=42, guild=1, eid="e1", pred="favorite_color"):
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            ensure_governance_schema(conn)
            subj = subject_key_for_user(user)
            conn.execute(
                "INSERT INTO memory_ledger_entries (entry_id,schema_version,guild_id,subject_key,subject_display_name,entry_type,predicate_key,normalized_value,source_class,source_table,source_row_id,source_revision,source_role,visibility,confidence,public_usable,derived,projection,salience,observed_at,lifecycle_status,created_at,updated_at,route_mode,channel_policy) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (eid, "memory_ledger_v1", guild, subj, "", "preference", pred, value, "first_party_record", "test", eid, "", "test", vis, "high", public, 0, 0, 0.9, "2026-07-01T00:00:00+00:00", life, "2026-07-01T00:00:00+00:00", "2026-07-01T00:00:00+00:00", bnl01_bot.ROUTE_MODE_NORMAL_CHAT, "public_home"),
            )
            conn.commit()

    def govern(self, legacy="Archive recall:\n- favorite color: old"):
        return bnl01_bot.apply_explicit_recall_governance(
            42, 1, "BNL, what do you remember about me?", legacy,
            bnl01_bot.ROUTE_MODE_NORMAL_CHAT, "public_home", channel_id=99, channel_name="general",
        )

    def test_public_direct_shadow_live_off_one_row_and_legacy_byte_exact(self):
        os.environ["BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED"] = "1"
        legacy = "Archive recall:\n- favorite color: old"
        out = self.govern(legacy)
        self.assertEqual(out, legacy)
        self.assertEqual(self.rows("SELECT COUNT(*) FROM memory_governance_shadow_runs")[0][0], 1)
        self.assertEqual(self.rows("SELECT errors_json FROM memory_governance_shadow_runs")[0][0], "[]")

    def test_public_batched_shadow_live_off_one_row(self):
        os.environ["BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED"] = "1"
        out = bnl01_bot.apply_explicit_recall_governance(42, 1, "what do you remember about me?", "legacy", bnl01_bot.ROUTE_MODE_NORMAL_CHAT, "public_home")
        self.assertEqual(out, "legacy")
        self.assertEqual(self.rows("SELECT COUNT(*) FROM memory_governance_shadow_runs")[0][0], 1)

    def test_unknown_sealed_and_protected_are_excluded(self):
        os.environ["BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED"] = "1"
        for policy in ("unknown", "sealed_test", "protected_system", "internal_controlled"):
            self.assertEqual(bnl01_bot.apply_explicit_recall_governance(42, 1, "what do you remember about me?", "legacy", bnl01_bot.ROUTE_MODE_NORMAL_CHAT, policy), "legacy")
        self.assertEqual(self.rows("SELECT COUNT(*) FROM memory_governance_shadow_runs")[0][0], 0)

    def test_live_on_safe_selection_user_facing_and_safe_empty_hides_legacy(self):
        os.environ["BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED"] = "1"
        os.environ["BNL_MEMORY_GOVERNANCE_LIVE_ENABLED"] = "1"
        self.insert_ledger()
        out = self.govern("legacy says old private thing")
        self.assertIn("Here is what I can safely recall:", out)
        self.assertIn("favorite color is green", out)
        self.assertNotIn("legacy", out)
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            conn.execute("DELETE FROM memory_ledger_entries")
            conn.commit()
        empty = self.govern("legacy ineligible secret")
        self.assertEqual(empty, "I do not have eligible durable memories available for this recall.")
        self.assertNotIn("secret", empty)

    def test_unsetting_live_gate_restores_legacy_byte_exact_and_keeps_shadow_evidence(self):
        os.environ["BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED"] = "1"
        os.environ["BNL_MEMORY_GOVERNANCE_LIVE_ENABLED"] = "1"
        self.insert_ledger()
        legacy = "Archive recall:\n- favorite color: old"

        live = self.govern(legacy)
        self.assertNotEqual(live, legacy)
        self.assertIn("favorite color is green", live)
        self.assertEqual(
            self.rows("SELECT COUNT(*) FROM memory_governance_shadow_runs")[0][0],
            1,
        )

        os.environ.pop("BNL_MEMORY_GOVERNANCE_LIVE_ENABLED", None)
        rolled_back = self.govern(legacy)
        self.assertEqual(rolled_back, legacy)
        self.assertEqual(
            self.rows("SELECT COUNT(*) FROM memory_governance_shadow_runs")[0][0],
            2,
        )
        self.assertEqual(
            self.rows("SELECT DISTINCT errors_json FROM memory_governance_shadow_runs"),
            [("[]",)],
        )

    def test_public_owner_mod_live_recall_remains_public_safe_only(self):
        os.environ["BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED"] = "1"
        os.environ["BNL_MEMORY_GOVERNANCE_LIVE_ENABLED"] = "1"
        self.insert_ledger("public-safe favorite color is green", eid="pub")
        self.insert_ledger("operator-only secret color is ultraviolet", eid="priv", vis="private", public=0)
        out = bnl01_bot.apply_explicit_recall_governance(
            42, 1, "BNL, what do you remember about me?", "legacy",
            bnl01_bot.ROUTE_MODE_NORMAL_CHAT, "public_home", is_owner_or_mod=True,
        )
        self.assertIn("public-safe favorite color is green", out)
        self.assertNotIn("operator-only secret", out)

    def test_error_result_falls_back_legacy_and_no_duplicate_rows(self):
        os.environ["BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED"] = "1"
        os.environ["BNL_MEMORY_GOVERNANCE_LIVE_ENABLED"] = "1"
        with self.assertLogs(level="WARNING") as logs:
            with mock.patch("bnl01_bot.build_governed_context", side_effect=RuntimeError("boom")):
                self.assertEqual(self.govern("legacy"), "legacy")
        joined = "\n".join(logs.output)
        self.assertIn("explicit_recall_governance_exception", joined)
        self.assertIn("route_mode=normal_chat", joined)
        self.assertIn("channel_policy=public_home", joined)
        self.assertIn("exception_type=RuntimeError", joined)
        self.assertNotIn("boom", joined)
        self.assertNotIn("what do you remember", joined)
        self.assertEqual(self.rows("SELECT COUNT(*) FROM memory_governance_shadow_runs")[0][0], 0)
        os.environ.pop("BNL_MEMORY_GOVERNANCE_LIVE_ENABLED", None)
        self.govern("legacy")
        self.assertEqual(self.rows("SELECT COUNT(*) FROM memory_governance_shadow_runs")[0][0], 1)

    def test_every_try_memory_recall_call_site_has_nearby_governance(self):
        from pathlib import Path
        text = Path("bnl01_bot.py").read_text()
        offsets = [i for i in range(len(text)) if text.startswith("try_memory_recall_response", i) and text[max(0, i-4):i].strip().endswith("=")]
        self.assertGreaterEqual(len(offsets), 4)
        for i in offsets:
            window = text[i:i + 1200]
            self.assertIn("apply_explicit_recall_governance", window)
            self.assertIn("format_explicit_recall_for_chat", window)
            self.assertIn("validate_deterministic_normal_chat_response", window)

    def test_specific_recall_requires_source_linked_direct_fact_and_survives_pruning(self):
        os.environ["BNL_MEMORY_LEDGER_SHADOW_ENABLED"] = "1"
        bnl01_bot.upsert_user_fact(
            42,
            1,
            "favorite_movie",
            "Legacy Guess",
            0.7,
        )
        source_blind = bnl01_bot.try_memory_recall_response(
            42,
            1,
            "what is my favorite movie?",
        )
        self.assertEqual(
            source_blind,
            "I don't have your favorite movie reliably recorded yet.",
        )

        bnl01_bot.save_user_message(
            42,
            "Crow",
            1,
            "my favorite movie is Hackers",
            channel_name="barcode-bot",
            channel_policy="public_home",
            channel_id=10,
            message_id=5001,
            directed_to_bnl=True,
        )
        row = self.rows(
            """
            SELECT fact_value, source_conversation_row_id, source_message_id,
                   source_channel_policy, source_kind, source_directed,
                   source_ledger_entry_id, lifecycle_status
            FROM user_memory_facts
            WHERE user_id=42 AND guild_id=1 AND fact_key='favorite_movie'
            """
        )[0]
        self.assertEqual(
            row[:6],
            ("Hackers", 1, 5001, "public_home", "member_self_report", 1),
        )
        self.assertTrue(row[6].startswith("mle_"))
        self.assertEqual(row[7], "active")
        self.assertIn(
            "**Hackers**",
            bnl01_bot.try_memory_recall_response(
                42,
                1,
                "what is my favorite movie?",
            ),
        )

        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            conn.execute("DELETE FROM conversations")
            conn.commit()
        self.assertIn(
            "**Hackers**",
            bnl01_bot.try_memory_recall_response(
                42,
                1,
                "what is my favorite movie?",
            ),
        )


class ApprovedMemberFactExtractionTests(unittest.TestCase):
    def facts(self, text):
        return bnl01_bot.extract_user_facts(text)

    def test_remember_requests_and_arbitrary_notes_never_become_durable_facts(self):
        unsupported = [
            "remember this number: 731946",
            "Remember this number - 8",
            "please remember the number 284517",
            "remember this: 731946",
            "remember that the number is 8",
            "remember that my door code changed",
            "remember my backup contact is Leo",
            "please remember I prefer short answers",
            "my favorite song is Blue Monday",
            "my current project is a visual album",
            "do you remember the number 8?",
            "can you remember this number: 731946?",
            "what number did I tell you to remember, 8?",
            "why don’t you remember number 8?",
            "do you remember X?",
            "what do you remember about me?",
            "what do you remember?",
            "what number did I tell you to remember?",
            "what numbers did I ask you to remember?",
            "do you remember the number?",
            "can you remember what I said?",
            "what number did I ask you to remember?",
            "why don’t you remember X?",
        ]
        for text in unsupported:
            self.assertEqual(self.facts(text), [], text)
        self.assertEqual(self.facts("hello BNL"), [])

    def test_only_four_clear_direct_self_reports_are_extracted(self):
        cases = {
            "call me Crow": [("preferred_name", "Crow", 0.90)],
            "my pronouns are they/them": [("pronouns", "they/them", 0.90)],
            "my favorite color is green": [("favorite_color", "green", 0.88)],
            "my favourite colour is blue": [("favorite_color", "blue", 0.88)],
            "my favorite movie is Hackers": [("favorite_movie", "Hackers", 0.90)],
        }
        for text, expected in cases.items():
            self.assertEqual(self.facts(text), expected, text)
        extracted_keys = {
            key
            for text in cases
            for key, _value, _confidence in self.facts(text)
        }
        self.assertEqual(
            extracted_keys,
            {"preferred_name", "pronouns", "favorite_color", "favorite_movie"},
        )

    def test_questions_roleplay_quotes_past_and_third_party_claims_are_rejected(self):
        rejected = [
            "is my favorite color green?",
            'pretend I said "my favorite color is green"',
            '"my favorite movie is Hackers"',
            "my favorite color used to be green",
            "my favorite movie is no longer Hackers",
            "someone said my favorite color is green",
            "they wrote my pronouns are they/them",
        ]
        for text in rejected:
            self.assertEqual(self.facts(text), [], text)


if __name__ == "__main__":
    unittest.main()
