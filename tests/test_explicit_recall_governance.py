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


class RememberDirectiveExtractionTests(unittest.TestCase):
    def facts(self, text):
        return bnl01_bot.extract_user_facts(text)

    def test_negative_interrogatives_create_no_remember_derived_facts_and_greetings_unchanged(self):
        negatives = [
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
        remember_derived_keys = {"remembered_number", "user_note"}
        for text in negatives:
            facts = self.facts(text)
            self.assertFalse(any(k in remember_derived_keys for k, _v, _c in facts), text)
        self.assertEqual(self.facts("hello BNL"), [])

    def test_exact_remember_number_production_phrases_create_only_remembered_number(self):
        cases = {
            "remember this number: 731946": "731946",
            "Remember this number - 8": "8",
            "please remember the number 284517": "284517",
            "remember this: 731946": "731946",
            "remember that the number is 8": "8",
        }
        for text, expected in cases.items():
            facts = self.facts(text)
            self.assertEqual([f for f in facts if f[0] == "remembered_number"], [("remembered_number", expected, 0.9)], text)
            self.assertFalse(any(k == "user_note" for k, _v, _c in facts), text)

    def test_positive_directives_create_notes_without_structured_duplicates(self):
        cases = {
            "remember that my door code changed": "my door code changed",
            "remember my backup contact is Leo": "my backup contact is Leo",
        }
        for text, expected in cases.items():
            self.assertIn(("user_note", expected, 0.64), self.facts(text))
        preference_facts = self.facts("please remember I prefer short answers")
        self.assertIn(("preferences", "short answers", 0.72), preference_facts)
        self.assertFalse(any(k == "user_note" and "short answers" in v for k, v, _c in preference_facts))
        facts = self.facts("remember that my favorite movie is Hackers")
        self.assertIn(("favorite_movie", "Hackers", 0.92), facts)
        self.assertFalse(any(k == "user_note" and v == "my favorite movie is Hackers" for k, v, _c in facts))


if __name__ == "__main__":
    unittest.main()
