import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock


os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl01_bot
from bnl_memory_governance import (
    GovernanceDiagnostics,
    GovernanceResult,
    ensure_governance_schema,
)
from bnl_memory_ledger import subject_key_for_user


class SourceSafeMultiSourceRecallTests(unittest.IsolatedAsyncioTestCase):
    ENV_KEYS = (
        "BNL_MEMORY_GOVERNANCE_CANARY_ENABLED",
        "BNL_MEMORY_GOVERNANCE_CANARY_GUILD_IDS",
        "BNL_MEMORY_GOVERNANCE_CANARY_USER_IDS",
        "BNL_MEMORY_LEDGER_SHADOW_ENABLED",
        "BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED",
        "BNL_MEMORY_GOVERNANCE_LIVE_ENABLED",
        "BNL_MOMENT_ENGINE_SHADOW_ENABLED",
        "BNL_RELATIONSHIP_V2_LIVE_ENABLED",
        "BNL_RELATIONSHIP_V2_SHADOW_ENABLED",
        "BNL_UNIFIED_RESPONSE_ASSESSMENT_SHADOW_ENABLED",
    )

    async def asyncSetUp(self):
        self.old_db = bnl01_bot.DB_FILE
        self.tmp = tempfile.NamedTemporaryFile(delete=False)
        self.tmp.close()
        bnl01_bot.DB_FILE = self.tmp.name
        self.env = mock.patch.dict(os.environ, {}, clear=False)
        self.env.start()
        for key in self.ENV_KEYS:
            os.environ.pop(key, None)
        bnl01_bot.init_db()
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            ensure_governance_schema(conn)
        self.old_canary_diagnostics = dict(
            bnl01_bot.LAST_MEMORY_GOVERNANCE_CANARY_DIAGNOSTICS
        )
        bnl01_bot.LAST_MEMORY_GOVERNANCE_CANARY_DIAGNOSTICS.clear()

    async def asyncTearDown(self):
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

    def enable_canary(self):
        os.environ.update(
            {
                "BNL_MEMORY_GOVERNANCE_CANARY_ENABLED": "true",
                "BNL_MEMORY_GOVERNANCE_CANARY_GUILD_IDS": "1",
                "BNL_MEMORY_GOVERNANCE_CANARY_USER_IDS": "42",
                "BNL_MEMORY_LEDGER_SHADOW_ENABLED": "true",
                "BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED": "true",
                "BNL_MOMENT_ENGINE_SHADOW_ENABLED": "true",
                "BNL_UNIFIED_RESPONSE_ASSESSMENT_SHADOW_ENABLED": "true",
                "BNL_RELATIONSHIP_V2_SHADOW_ENABLED": "true",
            }
        )

    def insert_ledger(
        self,
        value,
        *,
        entry_id,
        predicate,
        entry_type,
        source_class,
        lifecycle="active",
        visibility="public_safe",
        public_usable=1,
        user_id=42,
        source_policy="public_home",
    ):
        timestamp = "2026-07-25T18:00:00+00:00"
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            ensure_governance_schema(conn)
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
                    subject_key_for_user(user_id),
                    "",
                    entry_type,
                    predicate,
                    value,
                    source_class,
                    "test_source",
                    entry_id,
                    "revision:" + entry_id,
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
                    source_policy,
                ),
            )
            conn.commit()

    def build_prompt(self, room_context=""):
        metadata = {}
        with (
            mock.patch.object(
                bnl01_bot,
                "get_user_profile",
                return_value=("Test Member", ""),
            ),
            mock.patch.object(
                bnl01_bot,
                "should_allow_greeting",
                return_value=False,
            ),
            mock.patch.object(
                bnl01_bot,
                "choose_response_style",
                return_value=("balanced", ""),
            ),
            mock.patch.object(
                bnl01_bot,
                "build_broadcast_memory_context",
                return_value="",
            ),
        ):
            prompt, _allow_greeting, _style = (
                bnl01_bot.build_user_aware_prompt(
                    42,
                    1,
                    "Test Member",
                    "BNL, what do you remember about me?",
                    room_context=room_context,
                    channel_name="barcode-bot",
                    channel_policy="public_home",
                    route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                    is_direct_interaction=True,
                    channel_id=99,
                    prompt_metadata=metadata,
                )
            )
        return prompt, metadata

    def test_scope_is_exact_broad_recall_and_kill_switch_restores_old_path(self):
        self.enable_canary()
        base = {
            "guild_id": 1,
            "user_id": 42,
            "route_mode": bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
            "channel_policy": "public_home",
            "current_direct": True,
        }
        for text in (
            "what do you remember about me?",
            "BNL, what do you know about me?",
            "please tell me everything you remember about me",
        ):
            with self.subTest(text=text):
                self.assertTrue(
                    bnl01_bot.source_safe_recall_synthesis_enabled(
                        **base,
                        user_text=text,
                    )
                )
        for text in (
            "what is my favorite movie?",
            "what habits have you noticed?",
            "what do you remember about Crow?",
            "what do you remember about me? wait",
        ):
            with self.subTest(text=text):
                self.assertFalse(
                    bnl01_bot.source_safe_recall_synthesis_enabled(
                        **base,
                        user_text=text,
                    )
                )
        os.environ.pop("BNL_MEMORY_GOVERNANCE_CANARY_ENABLED")
        self.assertFalse(
            bnl01_bot.source_safe_recall_synthesis_enabled(
                **base,
                user_text="what do you remember about me?",
            )
        )

    def test_prompt_assembles_governed_and_conversation_sources_for_synthesis(self):
        self.enable_canary()
        self.insert_ledger(
            "favorite color is green",
            entry_id="direct-color",
            predicate="favorite_color",
            entry_type="preference",
            source_class="first_party_record",
        )
        self.insert_ledger(
            "finishing the source-safe shared-brain connection",
            entry_id="runtime-goal",
            predicate="goal",
            entry_type="goal",
            source_class="runtime_observation",
        )
        bnl01_bot.save_user_message(
            42,
            "Test Member",
            1,
            "I want the memory system to connect ideas across our work.",
            channel_name="barcode-bot",
            channel_policy="public_home",
            channel_id=99,
            message_id=1001,
            directed_to_bnl=True,
        )
        bnl01_bot.save_model_message(
            42,
            1,
            "You want connected recall without flattening its sources.",
            channel_name="barcode-bot",
            channel_policy="public_home",
            channel_id=99,
        )
        room_context = bnl01_bot.build_conversation_context_v2_for_prompt(
            guild_id=1,
            current_user_id=42,
            channel_id=99,
            channel_name="barcode-bot",
            channel_policy="public_home",
            route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
            conversation_surface="mention_or_reply",
            current_texts=("BNL, what do you remember about me?",),
            current_participants={42},
            is_direct_target=True,
        )

        prompt, metadata = self.build_prompt(room_context)

        self.assertIn(
            "Source-safe personal recall synthesis contract:",
            prompt,
        )
        self.assertIn("favorite color is green", prompt)
        self.assertIn(
            "finishing the source-safe shared-brain connection",
            prompt,
        )
        self.assertIn(
            "I want the memory system to connect ideas across our work.",
            prompt,
        )
        self.assertNotIn("Relationship state:", prompt)
        self.assertNotIn("Observed habits:", prompt)
        self.assertNotIn("Derived memory summaries", prompt)
        self.assertFalse(
            bnl01_bot.memory_governance_live_enabled()
        )
        self.assertTrue(metadata["source_safe_recall_synthesis"])
        basis_types = {
            type(basis)
            for basis in metadata["prompt_source_bases"]
        }
        self.assertIn(bnl01_bot.MemoryPromptSourceBasis, basis_types)
        self.assertIn(
            bnl01_bot.ConversationPromptSourceBasis,
            basis_types,
        )
        diagnostics = (
            bnl01_bot.memory_governance_canary_last_diagnostics(42, 1)
        )
        self.assertEqual(
            diagnostics["response_mode"],
            "source_safe_synthesis",
        )
        self.assertTrue(diagnostics["synthesis_packet_assembled"])
        self.assertGreaterEqual(diagnostics["packet_lane_count"], 3)
        self.assertGreaterEqual(diagnostics["packet_source_count"], 4)
        self.assertGreaterEqual(
            diagnostics["packet_conversation_source_count"],
            1,
        )
        self.assertEqual(diagnostics["atomic_candidates_live_used"], 0)
        assessment = metadata["unified_response_assessment_shadow"]
        self.assertIn("governed_memory", assessment.prompt_lanes)
        self.assertIn("conversation_context", assessment.prompt_lanes)
        self.assertNotIn("relationship", assessment.prompt_lanes)

    def test_same_text_replacement_changes_typed_governed_source_basis(self):
        self.enable_canary()
        self.insert_ledger(
            "favorite color is green",
            entry_id="source-a",
            predicate="favorite_color",
            entry_type="preference",
            source_class="first_party_record",
        )
        _prompt, metadata = self.build_prompt()
        basis = next(
            basis
            for basis in metadata["prompt_source_bases"]
            if isinstance(basis, bnl01_bot.MemoryPromptSourceBasis)
        )
        original_rendered_digest = basis.expected_digest
        original_source_digest = basis.governed_basis_digest
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            conn.execute(
                """
                UPDATE memory_ledger_entries
                SET lifecycle_status='superseded'
                WHERE entry_id='source-a'
                """
            )
            conn.commit()
        self.insert_ledger(
            "favorite color is green",
            entry_id="source-b",
            predicate="favorite_color",
            entry_type="preference",
            source_class="first_party_record",
        )

        fresh, changed = bnl01_bot.refresh_prompt_source_basis(basis)

        self.assertTrue(changed)
        self.assertEqual(
            fresh.expected_digest,
            original_rendered_digest,
        )
        self.assertNotEqual(
            fresh.governed_basis_digest,
            original_source_digest,
        )

    def test_canary_disable_after_prompt_assembly_fails_closed(self):
        self.enable_canary()
        self.insert_ledger(
            "favorite color is green",
            entry_id="source-a",
            predicate="favorite_color",
            entry_type="preference",
            source_class="first_party_record",
        )
        prompt, metadata = self.build_prompt()
        basis = next(
            basis
            for basis in metadata["prompt_source_bases"]
            if isinstance(basis, bnl01_bot.MemoryPromptSourceBasis)
        )
        self.assertTrue(basis.source_safe_recall_synthesis)

        os.environ.pop("BNL_MEMORY_GOVERNANCE_CANARY_ENABLED")
        (
            refreshed_prompt,
            refreshed_bases,
            changed_kinds,
            replacement_failed,
        ) = bnl01_bot.refresh_prompt_source_bases(prompt, (basis,))

        self.assertEqual(refreshed_prompt, prompt)
        self.assertEqual(changed_kinds, ("memory",))
        self.assertTrue(replacement_failed)
        self.assertFalse(
            refreshed_bases[0].source_safe_recall_synthesis
        )

    def test_unsafe_governance_removes_durable_lane_without_legacy_fallback(self):
        self.enable_canary()
        unsafe = GovernanceResult(
            rendered_context="legacy relationship material must not render",
            selected=(),
            exclusions=(),
            diagnostics=GovernanceDiagnostics(
                invalid_invariants=("cross_subject_selected",)
            ),
        )
        with (
            mock.patch.object(
                bnl01_bot,
                "build_governed_context",
                return_value=unsafe,
            ),
            mock.patch.object(
                bnl01_bot,
                "get_relationship_state",
                return_value=(99, 1.0, "close", "trusted", "private", ""),
            ),
            mock.patch.object(
                bnl01_bot,
                "get_user_habits",
                return_value=(99, 1, 1, 1, 10.0, "private", ""),
            ),
        ):
            context = bnl01_bot.build_user_memory_context(
                42,
                1,
                route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                channel_policy="public_home",
                user_text="what do you remember about me?",
                current_direct=True,
                channel_id=99,
            )

        self.assertEqual(
            context,
            "No currently eligible source-bearing durable memory context.",
        )
        self.assertNotIn("legacy relationship", context)
        diagnostics = (
            bnl01_bot.memory_governance_canary_last_diagnostics(42, 1)
        )
        self.assertEqual(
            diagnostics["response_mode"],
            "source_safe_synthesis_blocked",
        )
        self.assertEqual(diagnostics["invalid_invariant_count"], 1)
        self.assertEqual(diagnostics["atomic_candidates_live_used"], 0)

    async def test_internal_recall_controls_regenerate_once_then_fail_closed(self):
        self.enable_canary()
        provider = mock.AsyncMock(
            return_value=(
                "I remember your green preference and the shared memory "
                "system work."
            )
        )
        with mock.patch.object(
            bnl01_bot,
            "get_gemini_response_with_optional_typing",
            new=provider,
        ):
            response, diagnostics = (
                await bnl01_bot.apply_guarded_response_regeneration(
                    "The source-safe recall packet has two source lanes.",
                    prompt="eligible prompt",
                    user_id=42,
                    guild_id=1,
                    route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                    channel_policy="public_home",
                    current_user_text=(
                        "BNL, what do you remember about me?"
                    ),
                    channel=SimpleNamespace(id=99),
                )
            )
        self.assertIn("green preference", response)
        self.assertTrue(
            diagnostics[
                "source_safe_recall_output_leak_guard_triggered"
            ]
        )
        self.assertTrue(
            diagnostics[
                "source_safe_recall_output_leak_regenerated"
            ]
        )
        provider.assert_awaited_once()

        leaking_provider = mock.AsyncMock(
            return_value="The recall source lane still says green."
        )
        with mock.patch.object(
            bnl01_bot,
            "get_gemini_response_with_optional_typing",
            new=leaking_provider,
        ):
            response, diagnostics = (
                await bnl01_bot.apply_guarded_response_regeneration(
                    "The source-safe recall packet has two source lanes.",
                    prompt="eligible prompt",
                    user_id=42,
                    guild_id=1,
                    route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                    channel_policy="public_home",
                    current_user_text=(
                        "BNL, what do you remember about me?"
                    ),
                    channel=SimpleNamespace(id=99),
                )
            )
        self.assertEqual(response, "")
        self.assertTrue(diagnostics["suppressed"])
        self.assertEqual(
            diagnostics["suppression_reason"],
            "source_safe_recall_output_leak_after_retry",
        )

    def test_all_four_recall_routes_keep_fallback_and_add_synthesis_branch(self):
        source = Path("bnl01_bot.py").read_text()
        offsets = [
            index
            for index in range(len(source))
            if source.startswith("try_memory_recall_response", index)
            and source[max(0, index - 4) : index].strip().endswith("=")
        ]
        self.assertEqual(len(offsets), 4)
        for index in offsets:
            window = source[index : index + 2200]
            self.assertIn(
                "source_safe_recall_synthesis_enabled",
                window,
            )
            self.assertIn("memory_recall = \"\"", window)
            self.assertIn("apply_explicit_recall_governance", window)
            self.assertIn("format_explicit_recall_for_chat", window)

    def test_atomic_candidate_table_is_not_a_recall_read_dependency(self):
        governance_source = Path("bnl_memory_governance.py").read_text()
        selector = governance_source[
            governance_source.index("def build_governed_context") :
            governance_source.index("def persist_shadow_diagnostics")
        ]
        self.assertNotIn(
            "memory_ledger_atomic_knowledge_candidates",
            selector,
        )
        self.assertNotIn("atomic_candidate", selector)


if __name__ == "__main__":
    unittest.main()
