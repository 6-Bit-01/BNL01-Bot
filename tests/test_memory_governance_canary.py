import json
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock


os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl01_bot
from bnl_memory_governance import ensure_governance_schema
from bnl_memory_ledger import subject_key_for_user


class MemoryGovernanceCanaryAuthorizationTests(unittest.TestCase):
    def allowed_env(self):
        return {
            "BNL_MEMORY_GOVERNANCE_CANARY_ENABLED": "true",
            "BNL_MEMORY_GOVERNANCE_CANARY_GUILD_IDS": "101",
            "BNL_MEMORY_GOVERNANCE_CANARY_USER_IDS": "202",
            "BNL_MEMORY_LEDGER_SHADOW_ENABLED": "true",
            "BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED": "true",
        }

    def request(self, **overrides):
        values = {
            "guild_id": 101,
            "user_id": 202,
            "route_mode": bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
            "channel_policy": "public_home",
        }
        values.update(overrides)
        return values

    def test_default_off_and_global_live_is_not_a_canary_prerequisite(self):
        with mock.patch.object(
            bnl01_bot,
            "BNL_MEMORY_GOVERNANCE_CANARY_ENABLED",
            False,
        ):
            self.assertFalse(
                bnl01_bot.memory_governance_canary_enabled(
                    **self.request(),
                    environ={},
                )
            )

        environ = self.allowed_env()
        self.assertNotIn("BNL_MEMORY_GOVERNANCE_LIVE_ENABLED", environ)
        self.assertTrue(
            bnl01_bot.memory_governance_canary_enabled(
                **self.request(),
                environ=environ,
            )
        )

    def test_one_guild_user_scope_and_shadow_prerequisites_fail_closed(self):
        base = self.allowed_env()
        cases = {
            "missing_enabled": "BNL_MEMORY_GOVERNANCE_CANARY_ENABLED",
            "missing_guilds": "BNL_MEMORY_GOVERNANCE_CANARY_GUILD_IDS",
            "missing_users": "BNL_MEMORY_GOVERNANCE_CANARY_USER_IDS",
            "missing_ledger_shadow": "BNL_MEMORY_LEDGER_SHADOW_ENABLED",
            "missing_governance_shadow": (
                "BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED"
            ),
        }
        for label, missing in cases.items():
            with self.subTest(label=label):
                environ = dict(base)
                environ.pop(missing)
                self.assertFalse(
                    bnl01_bot.memory_governance_canary_enabled(
                        **self.request(),
                        environ=environ,
                    )
                )

        multiple_guilds = dict(base)
        multiple_guilds[
            "BNL_MEMORY_GOVERNANCE_CANARY_GUILD_IDS"
        ] = "101,102"
        self.assertFalse(
            bnl01_bot.memory_governance_canary_enabled(
                **self.request(),
                environ=multiple_guilds,
            )
        )

        multiple_users = dict(base)
        multiple_users[
            "BNL_MEMORY_GOVERNANCE_CANARY_USER_IDS"
        ] = "202,203"
        self.assertTrue(
            bnl01_bot.memory_governance_canary_enabled(
                **self.request(),
                environ=multiple_users,
            )
        )

    def test_subject_route_and_policy_scope_fail_closed(self):
        environ = self.allowed_env()
        cases = {
            "wrong_guild": {"guild_id": 999},
            "wrong_user": {"user_id": 999},
            "wrong_route": {
                "route_mode": bnl01_bot.ROUTE_MODE_SIMPLE_GREETING,
            },
            "selective_public": {"channel_policy": "public_selective"},
            "sealed": {"channel_policy": "sealed_test"},
            "internal": {"channel_policy": "internal_controlled"},
        }
        for label, overrides in cases.items():
            with self.subTest(label=label):
                self.assertFalse(
                    bnl01_bot.memory_governance_canary_enabled(
                        **self.request(**overrides),
                        environ=environ,
                    )
                )

        for route_mode in (
            bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
            bnl01_bot.ROUTE_MODE_DIRECT_PAYLOAD,
        ):
            for channel_policy in ("public_home", "public_context"):
                with self.subTest(
                    route_mode=route_mode,
                    channel_policy=channel_policy,
                ):
                    self.assertTrue(
                        bnl01_bot.memory_governance_canary_enabled(
                            **self.request(
                                route_mode=route_mode,
                                channel_policy=channel_policy,
                            ),
                            environ=environ,
                        )
                    )

    def test_configuration_reports_counts_without_allowlisted_ids(self):
        configuration = (
            bnl01_bot.memory_governance_canary_configuration(
                self.allowed_env()
            )
        )
        self.assertEqual(
            configuration,
            {
                "configured_enabled": True,
                "guild_allowlist_count": 1,
                "user_allowlist_count": 1,
                "fully_scoped": True,
            },
        )
        self.assertNotIn("101", str(configuration))
        self.assertNotIn("202", str(configuration))


class MemoryGovernanceCanaryIntegrationTests(unittest.TestCase):
    CANARY_KEYS = (
        "BNL_MEMORY_GOVERNANCE_CANARY_ENABLED",
        "BNL_MEMORY_GOVERNANCE_CANARY_GUILD_IDS",
        "BNL_MEMORY_GOVERNANCE_CANARY_USER_IDS",
        "BNL_MEMORY_LEDGER_SHADOW_ENABLED",
        "BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED",
        "BNL_MEMORY_GOVERNANCE_LIVE_ENABLED",
    )

    def setUp(self):
        self.old_db = bnl01_bot.DB_FILE
        self.tmp = tempfile.NamedTemporaryFile(delete=False)
        self.tmp.close()
        bnl01_bot.DB_FILE = self.tmp.name
        self.env = mock.patch.dict(os.environ, {}, clear=False)
        self.env.start()
        for key in self.CANARY_KEYS:
            os.environ.pop(key, None)
        bnl01_bot.init_db()
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            ensure_governance_schema(conn)
        self.old_canary_diagnostics = dict(
            bnl01_bot.LAST_MEMORY_GOVERNANCE_CANARY_DIAGNOSTICS
        )
        bnl01_bot.LAST_MEMORY_GOVERNANCE_CANARY_DIAGNOSTICS.clear()

    def tearDown(self):
        bnl01_bot.LAST_MEMORY_GOVERNANCE_CANARY_DIAGNOSTICS.clear()
        bnl01_bot.LAST_MEMORY_GOVERNANCE_CANARY_DIAGNOSTICS.update(
            self.old_canary_diagnostics
        )
        self.env.stop()
        bnl01_bot.DB_FILE = self.old_db
        try:
            os.unlink(self.tmp.name)
        except OSError:
            pass

    def enable_canary(self, *, guilds="1", users="42"):
        os.environ.update(
            {
                "BNL_MEMORY_GOVERNANCE_CANARY_ENABLED": "true",
                "BNL_MEMORY_GOVERNANCE_CANARY_GUILD_IDS": guilds,
                "BNL_MEMORY_GOVERNANCE_CANARY_USER_IDS": users,
                "BNL_MEMORY_LEDGER_SHADOW_ENABLED": "true",
                "BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED": "true",
            }
        )

    def insert_ledger(
        self,
        value,
        *,
        entry_id,
        predicate,
        lifecycle="active",
        visibility="public_safe",
        public_usable=1,
    ):
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            ensure_governance_schema(conn)
            timestamp = "2026-07-20T00:00:00+00:00"
            conn.execute(
                """
                INSERT INTO memory_ledger_entries (
                    entry_id, schema_version, guild_id, subject_key,
                    subject_display_name, entry_type, predicate_key,
                    normalized_value, source_class, source_table,
                    source_row_id, source_revision, source_role, visibility,
                    confidence, public_usable, derived, projection, salience,
                    observed_at, lifecycle_status, created_at, updated_at,
                    route_mode, channel_policy
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    entry_id,
                    "memory_ledger_v1",
                    1,
                    subject_key_for_user(42),
                    "",
                    "preference",
                    predicate,
                    value,
                    "first_party_record",
                    "test",
                    entry_id,
                    "",
                    "member",
                    visibility,
                    "high",
                    public_usable,
                    0,
                    0,
                    0.9,
                    timestamp,
                    lifecycle,
                    timestamp,
                    timestamp,
                    bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                    "public_home",
                ),
            )
            conn.commit()

    def govern(
        self,
        legacy="Archive recall:\n- favorite color: legacy blue",
        *,
        user_id=42,
        guild_id=1,
        policy="public_home",
    ):
        return bnl01_bot.apply_explicit_recall_governance(
            user_id,
            guild_id,
            "BNL, what do you remember about me?",
            legacy,
            bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
            policy,
            channel_id=99,
            channel_name="barcode-bot",
        )

    def latest_persisted_diagnostics(self):
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            row = conn.execute(
                """
                SELECT diagnostics_json
                FROM memory_governance_shadow_runs
                ORDER BY created_at DESC, run_id DESC
                LIMIT 1
                """
            ).fetchone()
        return json.loads(row[0]) if row else {}

    def test_scoped_canary_serves_governed_recall_with_global_live_off(self):
        self.enable_canary()
        self.insert_ledger(
            "favorite color is green",
            entry_id="current-color",
            predicate="favorite_color",
        )

        response = self.govern()

        self.assertFalse(bnl01_bot.memory_governance_live_enabled())
        self.assertIn("Here is what I can safely recall:", response)
        self.assertIn("favorite color is green", response)
        self.assertNotIn("legacy blue", response)
        diagnostics = bnl01_bot.memory_governance_canary_last_diagnostics(
            42,
            1,
        )
        self.assertEqual(
            diagnostics["response_mode"],
            "governed",
        )
        self.assertTrue(diagnostics["enabled_for_route"])
        persisted = self.latest_persisted_diagnostics()["route_policy"]
        self.assertTrue(
            persisted["memory_governance_canary_enabled_for_route"]
        )
        self.assertFalse(persisted["global_live_enabled"])
        self.assertEqual(persisted["response_mode"], "governed")

    def test_correction_visibility_forget_and_delete_remain_fail_closed(self):
        self.enable_canary()
        self.insert_ledger(
            "old favorite color is blue",
            entry_id="old-color",
            predicate="favorite_color",
            lifecycle="superseded",
        )
        self.insert_ledger(
            "favorite color is green",
            entry_id="current-color",
            predicate="favorite_color",
        )
        self.insert_ledger(
            "private operator color is ultraviolet",
            entry_id="private-color",
            predicate="favorite_movie",
            visibility="private",
            public_usable=0,
        )

        corrected = self.govern()
        self.assertIn("favorite color is green", corrected)
        self.assertNotIn("old favorite color", corrected)
        self.assertNotIn("ultraviolet", corrected)

        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            conn.execute(
                """
                UPDATE memory_ledger_entries
                SET lifecycle_status='forgotten'
                WHERE entry_id='current-color'
                """
            )
            conn.commit()
        forgotten = self.govern("legacy must not return after forget")
        self.assertEqual(
            forgotten,
            "I do not have eligible durable memories available for this recall.",
        )
        self.assertNotIn("legacy", forgotten)
        diagnostics = bnl01_bot.memory_governance_canary_last_diagnostics(
            42,
            1,
        )
        self.assertEqual(
            diagnostics["response_mode"],
            "safe_empty",
        )

        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            conn.execute(
                "DELETE FROM memory_ledger_entries "
                "WHERE entry_id='current-color'"
            )
            conn.commit()
        deleted = self.govern("legacy must not return after delete")
        self.assertEqual(
            deleted,
            "I do not have eligible durable memories available for this recall.",
        )

    def test_kill_switch_restores_byte_exact_legacy_and_keeps_comparison(self):
        self.enable_canary()
        self.insert_ledger(
            "favorite color is green",
            entry_id="current-color",
            predicate="favorite_color",
        )
        legacy = "Archive recall:\n- favorite color: legacy blue"

        self.assertNotEqual(self.govern(legacy), legacy)
        os.environ.pop("BNL_MEMORY_GOVERNANCE_CANARY_ENABLED")
        rolled_back = self.govern(legacy)

        self.assertEqual(rolled_back, legacy)
        diagnostics = bnl01_bot.memory_governance_canary_last_diagnostics(
            42,
            1,
        )
        self.assertFalse(diagnostics["enabled_for_route"])
        self.assertEqual(
            diagnostics["response_mode"],
            "legacy_shadow_comparison",
        )
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM memory_governance_shadow_runs"
                ).fetchone()[0],
                2,
            )

    def test_wrong_subject_and_unsafe_result_fall_back_to_legacy(self):
        self.enable_canary()
        self.insert_ledger(
            "favorite color is green",
            entry_id="current-color",
            predicate="favorite_color",
        )
        legacy = "byte-exact legacy"

        self.assertEqual(self.govern(legacy, user_id=43), legacy)
        wrong_subject_diagnostics = (
            bnl01_bot.memory_governance_canary_last_diagnostics(43, 1)
        )
        self.assertFalse(
            wrong_subject_diagnostics["enabled_for_route"]
        )

        with mock.patch(
            "bnl01_bot.build_governed_context",
            side_effect=RuntimeError("synthetic private detail"),
        ):
            with self.assertLogs(level="WARNING") as logs:
                self.assertEqual(self.govern(legacy), legacy)
        diagnostics = bnl01_bot.memory_governance_canary_last_diagnostics(
            42,
            1,
        )
        self.assertEqual(
            diagnostics["response_mode"],
            "legacy_exception_fallback",
        )
        self.assertEqual(
            diagnostics["fallback_reason"],
            "RuntimeError",
        )
        self.assertNotIn(
            "synthetic private detail",
            str(diagnostics),
        )
        self.assertNotIn("synthetic private detail", "\n".join(logs.output))

    def test_sealed_and_selective_routes_never_receive_canary_output(self):
        self.enable_canary()
        self.insert_ledger(
            "favorite color is green",
            entry_id="current-color",
            predicate="favorite_color",
        )
        legacy = "sealed legacy response"

        self.assertEqual(self.govern(legacy, policy="sealed_test"), legacy)
        diagnostics = bnl01_bot.memory_governance_canary_last_diagnostics(
            42,
            1,
        )
        self.assertEqual(
            diagnostics["response_mode"],
            "legacy_route_excluded",
        )
        self.assertEqual(self.govern(legacy, policy="public_selective"), legacy)
        diagnostics = bnl01_bot.memory_governance_canary_last_diagnostics(
            42,
            1,
        )
        self.assertEqual(
            diagnostics["response_mode"],
            "legacy_shadow_comparison",
        )

    def test_diagnostic_snapshot_exposes_safe_scope_and_last_decision(self):
        self.enable_canary()
        self.insert_ledger(
            "favorite color is green",
            entry_id="current-color",
            predicate="favorite_color",
        )
        self.govern()

        snapshot = bnl01_bot.build_memory_diagnostic_snapshot(
            42,
            1,
            channel_policy="public_home",
        )
        runtime = snapshot["runtime_gates"]
        self.assertEqual(
            runtime["memory_governance_canary"],
            {
                "configured_enabled": True,
                "guild_allowlist_count": 1,
                "user_allowlist_count": 1,
                "fully_scoped": True,
            },
        )
        self.assertEqual(
            runtime["memory_governance_canary_last"]["response_mode"],
            "governed",
        )
        self.assertFalse(runtime["memory_governance_live"])
        self.assertNotIn("discord_user:42", str(runtime))

    def test_media_followup_precedence_and_every_recall_wrapper_are_preserved(self):
        text = Path("bnl01_bot.py").read_text()
        offsets = [
            index
            for index in range(len(text))
            if text.startswith("try_memory_recall_response", index)
            and text[max(0, index - 4) : index].strip().endswith("=")
        ]
        self.assertGreaterEqual(len(offsets), 4)
        for index in offsets:
            before = text[max(0, index - 500) : index]
            after = text[index : index + 1200]
            self.assertIn("resolve_recent_media_followup", before)
            self.assertIn("apply_explicit_recall_governance", after)
            self.assertIn("format_explicit_recall_for_chat", after)


if __name__ == "__main__":
    unittest.main()
