import os
import sqlite3
import tempfile
import unittest

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl01_bot


class AdaptiveMemoryLifecycleTests(unittest.TestCase):
    def setUp(self):
        self.old_db = bnl01_bot.DB_FILE
        self.tmp = tempfile.NamedTemporaryFile(delete=False)
        self.tmp.close()
        bnl01_bot.DB_FILE = self.tmp.name
        bnl01_bot.init_db()

    def tearDown(self):
        bnl01_bot.DB_FILE = self.old_db
        try:
            os.unlink(self.tmp.name)
        except OSError:
            pass

    def _rows(self, tier=None):
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            if tier:
                return conn.execute("SELECT tier, summary, source_trust, source_channel_policy, topic_key, mentions FROM memory_tiers WHERE tier=? ORDER BY id", (tier,)).fetchall()
            return conn.execute("SELECT tier, summary, source_trust, source_channel_policy, topic_key, mentions FROM memory_tiers ORDER BY id").fetchall()

    def test_configurable_defaults_raise_old_caps(self):
        cfg = bnl01_bot._memory_lifecycle_config()
        self.assertGreater(cfg["short_base"], 28)
        self.assertGreater(cfg["medium_base"], 16)
        self.assertGreater(cfg["long_base"], 10)
        self.assertGreater(cfg["short_summary_chars"], 220)
        self.assertGreater(cfg["entry_chars"], 340)
        self.assertLess(cfg["prompt_public"], cfg["prompt_internal"])
        self.assertLess(cfg["prompt_internal"], cfg["prompt_operator"])

    def test_adaptive_limits_expand_for_familiar_operator_high_salience(self):
        bnl01_bot.update_relationship_state(42, 1, "remember this project plan matters for next PR", delta_affinity=0.2)
        for _ in range(40):
            bnl01_bot.update_user_habits(42, 1, "Source file project plan follow-up needs memory continuity.")
        limits = bnl01_bot.calculate_adaptive_memory_limits(
            42, 1, channel_policy="public_context", user_text="remember this project plan for next PR", is_owner_or_mod=True
        )
        self.assertGreater(limits["short"], bnl01_bot.SHORT_MEMORY_LIMIT_BASE)
        self.assertGreater(limits["medium"], bnl01_bot.MEDIUM_MEMORY_LIMIT_BASE)
        self.assertEqual(limits["prompt_budget"], bnl01_bot.MEMORY_PROMPT_BUDGET_OPERATOR)
        self.assertIn("operator", limits["reasons"])

    def test_low_value_chatter_is_skipped(self):
        bnl01_bot.maybe_add_memory_trace(42, 1, "ok", "public_home", "user", channel_name="general")
        bnl01_bot.maybe_add_memory_trace(42, 1, "diagnostics no response needed", "public_home", "user", channel_name="general")
        self.assertEqual(self._rows(), [])

    def test_short_overflow_clusters_to_midterm_by_topic(self):
        old_short = bnl01_bot.SHORT_MEMORY_LIMIT_BASE
        bnl01_bot.SHORT_MEMORY_LIMIT_BASE = 3
        try:
            for idx in range(7):
                bnl01_bot.add_short_memory_trace(
                    42, 1,
                    f"Source file workflow thread item {idx}: dashboard diagnostics should stay hidden unless actionable.",
                    source_role="user", source_channel_policy="public_home", source_trust="source_safe_public",
                )
            mediums = self._rows("medium")
            self.assertTrue(any("Source File thread" in row[1] for row in mediums))
            self.assertTrue(all(row[3] == "public_home" or row[3] == "consolidated" for row in mediums))
        finally:
            bnl01_bot.SHORT_MEMORY_LIMIT_BASE = old_short

    def test_midterm_repeated_high_salience_promotes_to_long(self):
        old_med = bnl01_bot.MEDIUM_MEMORY_LIMIT_BASE
        bnl01_bot.MEDIUM_MEMORY_LIMIT_BASE = 1
        try:
            with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
                cur = conn.cursor()
                for idx in range(4):
                    bnl01_bot._insert_memory_tier(
                        cur, 42, 1, "medium",
                        f"Queue system thread: Next In Line resolver decision slot repeated {idx}; Priority interrupts but does not consume pointer.",
                        0.82, mentions=2, source_channel_policy="public_home", source_trust="source_safe_public",
                        topic_key="queue", lifecycle_note="test_seed",
                    )
            result = bnl01_bot._consolidate_memory_tiers(42, 1)
            longs = self._rows("long")
            self.assertGreaterEqual(result["medium_to_long"], 1)
            self.assertTrue(any("Durable Queue memory" in row[1] for row in longs))
        finally:
            bnl01_bot.MEDIUM_MEMORY_LIMIT_BASE = old_med

    def test_visibility_separation_and_sealed_test_boundaries(self):
        bnl01_bot.add_short_memory_trace(42, 1, "Public source file memory should be available in public context.", source_channel_policy="public_home", source_trust="source_safe_public")
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            cur = conn.cursor()
            bnl01_bot._insert_memory_tier(cur, 42, 1, "long", "PRIVATE_ADMIN_ALPHA should never leak to public outputs.", 0.95, source_channel_policy="internal_controlled", source_trust="legacy_unknown", topic_key="project_plan")
            bnl01_bot._insert_memory_tier(cur, 42, 1, "medium", "Sealed test junk should not become durable memory.", 0.95, source_channel_policy="sealed_test", source_trust="legacy_unknown", topic_key="memory")
        context = bnl01_bot.build_user_memory_context(42, 1, channel_policy="public_home", user_text="source file memory")
        self.assertIn("Public source file memory", context)
        self.assertNotIn("PRIVATE_ADMIN_ALPHA", context)
        bnl01_bot._consolidate_memory_tiers(42, 1, limits={"short": 99, "medium": 0, "long": 10})
        self.assertFalse(any("Sealed test junk" in row[1] for row in self._rows("long")))

    def test_diagnostics_show_counts_without_raw_private_content(self):
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            cur = conn.cursor()
            bnl01_bot._insert_memory_tier(cur, 42, 1, "long", "PRIVATE_ADMIN_ALPHA raw secret", 0.9, source_channel_policy="internal_controlled", source_trust="legacy_unknown", topic_key="project_plan")
        diag = bnl01_bot.build_memory_diagnostic_snapshot(42, 1, channel_policy="public_home")
        text = str(diag)
        self.assertIn("configured_limits", text)
        self.assertIn("tier_counts", text)
        self.assertNotIn("PRIVATE_ADMIN_ALPHA", text)


if __name__ == "__main__":
    unittest.main()
