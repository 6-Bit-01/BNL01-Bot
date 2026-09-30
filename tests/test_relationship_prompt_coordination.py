"""Relationship tone must compose with the existing factual memory reader."""
import asyncio
import json
import os
import sqlite3
import tempfile
import unittest
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-token")

import bnl01_bot as bot
import bnl_relationship_engine as relationships
from bnl_memory_governance import ensure_governance_schema


class RelationshipPromptCoordinationTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.db = str(Path(self.directory.name) / "relationship.db")
        patch = mock.patch.object(bot, "DB_FILE", self.db)
        patch.start()
        self.addCleanup(patch.stop)
        env = mock.patch.dict(os.environ, {
            key: "false" for key in os.environ
            if key.startswith("BNL_") and key.endswith("_ENABLED")
        })
        env.start()
        self.addCleanup(env.stop)
        os.environ.update({
            "BNL_RELATIONSHIP_V2_LIVE_ENABLED": "true",
            "BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED": "false",
            "BNL_MEMORY_GOVERNANCE_LIVE_ENABLED": "false",
            "BNL_MEMORY_GOVERNANCE_CANARY_ENABLED": "false",
        })
        bot.init_db()
        bot.update_relationship_state(42, 1, "That was annoying.")
        self.observed = datetime.now(timezone.utc).isoformat()
        with closing(sqlite3.connect(self.db)) as conn, conn:
            ensure_governance_schema(conn)
            relationships.observe_message(
                conn, guild_id=1, user_id=42, role="user",
                content="Thanks for fixing that. We are good.",
                source_row_id=10, channel_policy="public_home",
                route_mode="normal_chat", directed=True,
                observed_at=self.observed,
            )
            conn.execute("""
                INSERT INTO memory_ledger_entries (
                    entry_id,schema_version,guild_id,subject_key,entry_type,
                    predicate_key,normalized_value,source_class,source_table,
                    source_row_id,source_role,visibility,confidence,public_usable,
                    derived,projection,salience,observed_at,lifecycle_status,
                    created_at,updated_at,channel_policy,route_mode
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                "test-goal", "memory_ledger_v1", 1, "discord_user:42", "goal",
                "goal", "preparing a live percussion set", "runtime_observation",
                "test_source", "test-goal", "member", "public_safe", "high", 1,
                0, 0, 0.9, self.observed, "active", self.observed, self.observed,
                "public_home", "normal_chat",
            ))

    def enable_governed(self, mode):
        os.environ["BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED"] = "true"
        if mode == "global":
            os.environ["BNL_MEMORY_GOVERNANCE_LIVE_ENABLED"] = "true"
        else:
            os.environ.update({
                "BNL_MEMORY_GOVERNANCE_CANARY_ENABLED": "true",
                "BNL_MEMORY_GOVERNANCE_CANARY_GUILD_IDS": "1",
                "BNL_MEMORY_GOVERNANCE_CANARY_USER_IDS": "42",
                "BNL_MEMORY_LEDGER_SHADOW_ENABLED": "true",
                "BNL_MOMENT_ENGINE_SHADOW_ENABLED": "true",
            })

    def read(self, **overrides):
        metadata = {}
        kwargs = dict(
            route_mode="normal_chat", channel_policy="public_home",
            user_text="What do you remember about me?",
            current_direct=True, governance_allowed=True,
            source_metadata=metadata, record_operational_diagnostics=False,
        )
        kwargs.update(overrides)
        return bot.build_user_memory_context(42, 1, **kwargs), metadata

    def assert_tone_once(self, text, metadata):
        self.assertEqual(text.count("Private relationship calibration"), 1)
        self.assertNotIn("stance=rival", text)
        self.assertNotIn("affinity=", text)
        self.assertEqual(
            sum(unit.kind == "relationship_v2" for unit in metadata["memory_context_units"]),
            1,
        )
        self.assertFalse(any(unit.kind == "relationship" for unit in metadata["memory_context_units"]))
        self.assertTrue(metadata["relationship_v2_candidate_present"])

    def test_v2_and_legacy_cannot_give_competing_tone(self):
        text, metadata = self.read()
        self.assert_tone_once(text, metadata)
        self.assertFalse(metadata["legacy_relationship_present"])

    def test_governed_fact_selection_preserves_independent_tone_and_source_ids(self):
        for mode in ("global", "canary"):
            with self.subTest(mode=mode), mock.patch.dict(os.environ):
                self.enable_governed(mode)
                text, metadata = self.read()
                self.assertIn("preparing a live percussion set", text)
                self.assert_tone_once(text, metadata)
                self.assertIn("test-goal", metadata["governed_entry_ids"])
                self.assertEqual(metadata["governed_candidate_count"], 1)
                bounded = bot._bounded_member_memory_context(
                    metadata, speaker_label="speaker 1 - Test Member",
                    budget_chars=1800,
                )
                self.assertIn("preparing a live percussion set", bounded)
                self.assert_tone_once(bounded, metadata)

    def test_empty_governed_fact_set_does_not_remove_eligible_tone(self):
        self.enable_governed("canary")
        with closing(sqlite3.connect(self.db)) as conn, conn:
            conn.execute("UPDATE memory_ledger_entries SET lifecycle_status='forgotten' WHERE entry_id='test-goal'")
        text, metadata = self.read()
        self.assert_tone_once(text, metadata)
        self.assertEqual(metadata["governed_entry_ids"], ())

    def test_gate_off_preserves_facts_without_using_shadow_tone(self):
        self.enable_governed("canary")
        os.environ["BNL_RELATIONSHIP_V2_LIVE_ENABLED"] = "false"
        os.environ["BNL_RELATIONSHIP_V2_SHADOW_ENABLED"] = "true"
        text, metadata = self.read()
        self.assertIn("preparing a live percussion set", text)
        self.assertNotIn("Private relationship calibration", text)
        self.assertFalse(metadata["relationship_v2_candidate_present"])

    def test_unapproved_surfaces_cannot_add_v2_tone(self):
        for overrides in (
            {"current_direct": False}, {"governance_allowed": False},
            {"channel_policy": "sealed_test"}, {"route_mode": "relay"},
        ):
            with self.subTest(overrides=overrides):
                text, metadata = self.read(**overrides)
                self.assertNotIn("Private relationship calibration", text)
                self.assertFalse(metadata["relationship_v2_candidate_present"])

    def test_unsafe_governance_does_not_restore_tone_or_stale_rivalry(self):
        for mode in ("global", "canary"):
            with self.subTest(mode=mode), mock.patch.dict(os.environ):
                self.enable_governed(mode)
                with mock.patch.object(bot, "assess_governance_result_safety", return_value=SimpleNamespace(unsafe=True)):
                    text, metadata = self.read()
                self.assertNotIn("Private relationship calibration", text)
                self.assertNotIn("stance=rival", text)
                self.assertFalse(metadata["relationship_v2_candidate_present"])

    def test_context_environment_controls_both_relationship_gate_checks(self):
        os.environ["BNL_RELATIONSHIP_V2_LIVE_ENABLED"] = "false"
        env = dict(os.environ, BNL_RELATIONSHIP_V2_LIVE_ENABLED="true")
        text, metadata = self.read(environ=env)
        self.assert_tone_once(text, metadata)

    def test_correction_removes_only_the_corrected_factual_source(self):
        self.enable_governed("canary")
        before, _ = self.read()
        self.assertIn("preparing a live percussion set", before)
        with closing(sqlite3.connect(self.db)) as conn, conn:
            conn.execute("UPDATE memory_ledger_entries SET lifecycle_status='corrected' WHERE entry_id='test-goal'")
        after, metadata = self.read()
        self.assertNotIn("preparing a live percussion set", after)
        self.assertEqual(metadata["governed_entry_ids"], ())
        self.assert_tone_once(after, metadata)

    def test_governance_error_withholds_tone_without_reviving_legacy_rivalry(self):
        for mode in ("global", "canary"):
            with self.subTest(mode=mode), mock.patch.dict(os.environ):
                self.enable_governed(mode)
                with mock.patch.object(bot, "build_governed_context", side_effect=sqlite3.OperationalError("test failure")):
                    text, metadata = self.read()
                self.assertNotIn("Private relationship calibration", text)
                self.assertNotIn("stance=rival", text)
                self.assertFalse(metadata["relationship_v2_candidate_present"])

    def test_complete_prompt_keeps_current_repair_facts_and_one_tone_owner(self):
        self.enable_governed("canary")
        current = "What do you remember about me?"
        repair = "We have settled that misunderstanding."
        for single_packet in (False, True):
            with self.subTest(single_packet=single_packet):
                prompt, *_ = bot.build_user_aware_prompt(
                    42, 1, "Test Member", current,
                    channel_name="general-chat", channel_policy="public_home",
                    route_mode="normal_chat", is_direct_interaction=True,
                    room_context="Test Member: " + repair,
                    _ordinary_chat_single_packet_enabled_override=single_packet,
                )
                self.assertEqual(prompt.count("Private relationship calibration"), 1)
                self.assertIn(current, prompt)
                self.assertIn(repair, prompt)
                self.assertIn("preparing a live percussion set", prompt)
                self.assertNotIn("stance=rival", prompt)
                self.assertIn("Current context outranks old impressions", prompt)

    def tone_basis(self):
        text, metadata = self.read()
        return bot.build_memory_prompt_source_basis(
            text, user_id=42, guild_id=1, route_mode="normal_chat",
            channel_policy="public_home", user_text="What do you remember about me?",
            is_owner_or_mod=False, current_direct=True, governance_allowed=True,
            channel_id=0, moment_attribution_target_user_id=0,
            governed_basis_digest=metadata["governed_basis_digest"],
            source_safe_recall_synthesis=metadata["source_safe_recall_synthesis"],
        )

    def test_tone_only_context_revalidates_member_boundary_before_send(self):
        basis = self.tone_basis()
        self.assertIsNotNone(basis)
        unchanged, changed = bot.refresh_prompt_source_basis(basis)
        self.assertFalse(changed)
        with closing(sqlite3.connect(self.db)) as conn, conn:
            relationships.set_member_setting(conn, guild_id=1, user_id=42, proactive_enabled=False)
        fresh, changed = bot.refresh_prompt_source_basis(unchanged)
        self.assertTrue(changed)
        self.assertIn("No proactive recognition or follow-up.", fresh.rendered_context)

    def test_tone_only_context_revalidates_kill_switch_before_send(self):
        basis = self.tone_basis()
        self.assertIsNotNone(basis)
        os.environ["BNL_RELATIONSHIP_V2_LIVE_ENABLED"] = "false"
        fresh, changed = bot.refresh_prompt_source_basis(basis)
        self.assertTrue(changed)
        self.assertNotIn("Private relationship calibration", fresh.rendered_context)

    def test_bounded_tone_preserves_boundaries_and_current_context_guidance(self):
        with closing(sqlite3.connect(self.db)) as conn, conn:
            conn.execute("UPDATE relationship_state_v2 SET rivalry_state='mutual_rivalry',friction=0.1")
            relationships.set_member_setting(conn, guild_id=1, user_id=42, proactive_enabled=False)
            tone = relationships.governed_summary(
                conn, guild_id=1, user_id=42, route_mode="normal_chat",
                channel_policy="public_home",
            )
        self.assertLessEqual(len(tone), 500)
        self.assertIn("Current context outranks old impressions.", tone)
        self.assertIn("No proactive recognition or follow-up.", tone)
        self.assertIn("Keep factual recall and helpfulness independent of rapport.", tone)
        self.assertNotIn("Rivalry only", tone)

    def sealed_env(self):
        return {
            "BNL_RELATIONSHIP_V2_SEALED_CANARY_ENABLED": "true",
            "BNL_RELATIONSHIP_V2_SEALED_CANARY_GUILD_IDS": "1",
            "BNL_RELATIONSHIP_V2_SEALED_CANARY_CHANNEL_IDS": "99",
            "BNL_RELATIONSHIP_V2_SEALED_CANARY_USER_IDS": "42",
            "BNL_RELATIONSHIP_V2_SHADOW_ENABLED": "true",
            "BNL_MEMORY_LEDGER_SHADOW_ENABLED": "true",
            "BNL_MOMENT_ENGINE_SHADOW_ENABLED": "true",
            "BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED": "true",
            "BNL_RELATIONSHIP_V2_LIVE_ENABLED": "false",
            "BNL_MEMORY_GOVERNANCE_LIVE_ENABLED": "false",
            "BNL_ACTIVE_ENGAGEMENT_V2_LIVE_ENABLED": "false",
        }

    def test_sealed_canary_requires_exact_scope_and_shadow_prerequisites(self):
        request = dict(guild_id=1, user_id=42, channel_id=99, route_mode="normal_chat", channel_policy="sealed_test", direct=True)
        env = self.sealed_env()
        self.assertTrue(relationships.sealed_canary_enabled(**request, environ=env))
        self.assertFalse(relationships.sealed_canary_enabled(**request, environ={}))
        for key, value in env.items():
            with self.subTest(key=key):
                changed = dict(env)
                if value == "false":
                    changed[key] = "true"
                else:
                    changed.pop(key)
                self.assertFalse(relationships.sealed_canary_enabled(**request, environ=changed))
        for changes in (
            {"guild_id": 2}, {"user_id": 43}, {"channel_id": 100},
            {"route_mode": "relay"}, {"channel_policy": "public_home"}, {"direct": False},
        ):
            with self.subTest(changes=changes):
                self.assertFalse(relationships.sealed_canary_enabled(**dict(request, **changes), environ=env))
        for key in ("GUILD_IDS", "CHANNEL_IDS", "USER_IDS"):
            with self.subTest(key=key):
                malformed = dict(env, **{"BNL_RELATIONSHIP_V2_SEALED_CANARY_" + key: "42,invalid"})
                self.assertFalse(relationships.sealed_canary_enabled(**request, environ=malformed))
        for key in ("GUILD_IDS", "CHANNEL_IDS"):
            with self.subTest(ambiguous=key):
                ambiguous = dict(env, **{"BNL_RELATIONSHIP_V2_SEALED_CANARY_" + key: "1,99"})
                self.assertFalse(relationships.sealed_canary_enabled(**request, environ=ambiguous))

    def test_sealed_canary_learns_privately_without_changing_public_posture(self):
        os.environ.update(self.sealed_env())
        tables = ("relationship_state", "relationship_state_v2", "relationship_events_v2", "relationship_member_preferences_v2")
        def snapshot():
            with closing(sqlite3.connect(self.db)) as conn:
                return [conn.execute("SELECT * FROM " + table +
                    (" WHERE channel_policy<>'sealed_test'" if table == 'relationship_events_v2' else "")).fetchall()
                    for table in tables]
        before = snapshot()
        bot.save_user_message(
            42, "Test Member", 1, "Thanks for fixing that. We are good.",
            channel_policy="sealed_test", channel_name="bnl-testing", channel_id=99,
            route_mode="normal_chat", directed_to_bnl=True,
        )
        bot.save_model_message(
            42, 1, "We can move forward with the arrangement.",
            channel_policy="sealed_test", channel_name="bnl-testing", channel_id=99,
            route_mode="normal_chat",
        )
        with mock.patch.dict(bot.LAST_MEMORY_PROMPT_DIAGNOSTICS):
            text, metadata = self.read(channel_policy="sealed_test", channel_id=99, governance_allowed=False, record_operational_diagnostics=True)
            self.assert_tone_once(text, metadata)
            self.assertEqual(bot.LAST_MEMORY_PROMPT_DIAGNOSTICS[(42, 1)]["relationship_v2"]["authority"], "sealed_canary")
        self.assertEqual(snapshot(), before)
        with closing(sqlite3.connect(self.db)) as conn:
            private_events = conn.execute("SELECT channel_id,lifecycle FROM relationship_events_v2 "
                                          "WHERE channel_policy='sealed_test'").fetchall()
        self.assertTrue(private_events)
        self.assertTrue(all(row == (99, 'review_only') for row in private_events))

    def test_sealed_canary_reaches_normal_prompt_without_global_activation(self):
        os.environ.update(self.sealed_env())
        current = "We have settled that misunderstanding. Help me work through this drum arrangement."
        prompt, *_ = bot.build_user_aware_prompt(
            42, 1, "Test Member", current, channel_id=99,
            channel_name="bnl-testing", channel_policy="sealed_test",
            route_mode="normal_chat", is_direct_interaction=True,
        )
        self.assertIn(current, prompt)
        self.assertEqual(prompt.count("Private relationship calibration"), 1)
        self.assertNotIn("stance=rival", prompt)
        self.assertFalse(relationships.live_enabled())
        self.assertFalse(relationships.active_engagement_live_enabled())

    def test_sealed_canary_has_same_member_isolation_and_send_time_kill_switch(self):
        os.environ.update(self.sealed_env())
        with closing(sqlite3.connect(self.db)) as conn:
            self.assertEqual(relationships.governed_summary(
                conn, guild_id=1, user_id=42, target_user_id=43, channel_id=99,
                route_mode="normal_chat", channel_policy="sealed_test",
            ), "")
        text, metadata = self.read(channel_policy="sealed_test", channel_id=99, governance_allowed=False)
        basis = bot.build_memory_prompt_source_basis(
            text, user_id=42, guild_id=1, route_mode="normal_chat", channel_policy="sealed_test",
            user_text="What do you remember about me?", is_owner_or_mod=False,
            current_direct=True, governance_allowed=False, channel_id=99,
            moment_attribution_target_user_id=0,
            governed_basis_digest=metadata["governed_basis_digest"],
        )
        self.assertIsNotNone(basis)
        self.assertFalse(bot.refresh_prompt_source_basis(basis)[1])
        os.environ[relationships.SEALED_CANARY_ENV] = "false"
        fresh, changed = bot.refresh_prompt_source_basis(basis)
        self.assertTrue(changed)
        self.assertNotIn("Private relationship calibration", fresh.rendered_context)

    def enable_packet_and_sealed_tone(self):
        os.environ.update(self.sealed_env())
        os.environ.update({
            "BNL_UNIFIED_RESPONSE_ASSESSMENT_SHADOW_ENABLED": "true",
            "BNL_UNIFIED_INTELLIGENCE_PACKET_SHADOW_ENABLED": "true",
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_ENABLED": "true",
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_PUBLIC_ENABLED": "false",
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_GUILD_IDS": "1",
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_USER_IDS": "42",
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_CHANNEL_IDS": "99",
        })
        self.assertTrue(bot.ordinary_chat_configuration()["effective"])

    def private_message(self, text, *, user_id=42, channel_id=99):
        bot.save_user_message(
            user_id, "Test Member", 1, text,
            channel_name="bnl-testing", channel_policy="sealed_test",
            channel_id=channel_id, route_mode="normal_chat", directed_to_bnl=True,
        )
        with closing(sqlite3.connect(self.db)) as conn:
            return conn.execute(
                "SELECT id FROM conversations WHERE guild_id=1 AND user_id=? "
                "AND channel_id=? AND role='user' ORDER BY id DESC LIMIT 1",
                (user_id, channel_id),
            ).fetchone()[0]

    def packet_prompt(self, *, user_id=42, channel_id=99):
        current = "Help me work through this drum arrangement."
        context_out = {}
        room = bot.build_conversation_context_v2_for_prompt(
            guild_id=1, current_user_id=user_id, channel_id=channel_id,
            channel_name="bnl-testing", channel_policy="sealed_test",
            current_texts=[current], current_participants={user_id},
            is_direct_target=True, result_out=context_out,
        )
        orchestration = bot.build_live_conversation_orchestration_decision(
            engagement_decision="answer", engagement_reason="direct_request",
            channel_policy="sealed_test", addressings=(),
            context_result=context_out["result"], moment_situation=None,
            guild_id=1, channel_id=channel_id, current_text=current,
            conversation_surface=bot.conversation_surface_for_channel_policy("sealed_test"),
            current_speaker_user_ids=(user_id,), current_speaker_labels=("Test Member",),
        )
        metadata = {}
        prompt, *_ = bot.build_user_aware_prompt(
            user_id, 1, "Test Member", current, channel_id=channel_id,
            channel_name="bnl-testing", channel_policy="sealed_test",
            route_mode="normal_chat", is_direct_interaction=True,
            room_context=room, conversation_context_result=context_out["result"],
            conversation_orchestration=orchestration, prompt_metadata=metadata,
        )
        self.assertTrue(metadata["ordinary_chat_single_packet_scope"].eligible)
        self.assertTrue(metadata["ordinary_chat_single_packet_applied"])
        self.assertEqual(metadata["ordinary_chat_single_packet_preflight_reason"], "")
        self.assertIsNotNone(metadata["ordinary_chat_single_packet_basis"])
        return current, prompt, metadata

    def test_scoped_packet_and_relationship_tone_share_one_real_generation_path(self):
        self.enable_packet_and_sealed_tone()
        self.private_message("Don't ping me. The drum arrangement uses brushed snares.")
        current, prompt, metadata = self.packet_prompt()
        basis = metadata["ordinary_chat_single_packet_basis"]
        self.assertTrue(basis.ordinary_chat_single_packet)
        self.assertEqual(prompt.count("Private relationship calibration"), 1)
        self.assertIn("No proactive recognition or follow-up.", prompt)
        self.assertIn("brushed snares", prompt)
        self.assertNotIn("stance=rival", prompt)
        tone_items = [item for item in basis.packet.items if item.lane == "relationship_posture"]
        # This task concerns an arrangement, not a member profile. Tone remains
        # owned by the independently revalidated memory basis even when the
        # factual packet has no relationship comparison item.
        self.assertTrue(all(
            item.subject_key == "discord_user:42" and item.usage == "tone_only"
            and item.visibility == "private" for item in tone_items
        ))
        self.assertNotIn("relationship_posture", dict(basis.rendered_lane_counts))
        self.assertFalse(any(item.source_ref in basis.rendered_evidence_refs for item in tone_items))
        for private_state in ("rapport=", "trust=", "familiarity=", "affinity=", "rivalry_state="):
            self.assertNotIn(private_state, prompt)
        provider = mock.AsyncMock(return_value=bot.TrackedGenerationResponse(
            text="Try leaving space around those brushed snares.", provider_call_count=1,
        ))
        with mock.patch.object(bot, "get_tracked_gemini_response_with_optional_typing", provider):
            execution = asyncio.run(bot.maybe_generate_ordinary_chat_single_packet(
                channel=SimpleNamespace(id=99), prompt=prompt, basis=basis,
                scope_applied=metadata["ordinary_chat_single_packet_applied"],
                preflight_reason=metadata["ordinary_chat_single_packet_preflight_reason"],
                situation_frame=metadata["situation_frame_shadow"],
                situation_frame_current_text=current, route_mode="normal_chat",
                channel_policy="sealed_test",
                conversation_surface=bot.conversation_surface_for_channel_policy("sealed_test"),
                user_id=42, guild_id=1, user_display_name="Test Member",
                source_context_available=True,
                prompt_source_bases=metadata["prompt_source_bases"],
            ))
        self.assertIsNotNone(execution)
        provider.assert_awaited_once()
        self.assertEqual(execution.provider_call_count, 1)
        self.assertEqual(execution.corrective_call_count, 0)
        self.assertTrue(execution.decision.run.prompt_applied)
        self.assertEqual(execution.prompt.count("Private relationship calibration"), 1)
        self.assertIn("No proactive recognition or follow-up.", execution.prompt)
        self.assertIn(basis, execution.prompt_source_bases)
        self.assertTrue(any(isinstance(item, bot.MemoryPromptSourceBasis)
                            for item in execution.prompt_source_bases))
        self.assertFalse(relationships.live_enabled())
        self.assertFalse(relationships.active_engagement_live_enabled())

    def test_packet_scope_does_not_grant_another_member_or_room_private_tone(self):
        self.enable_packet_and_sealed_tone()
        self.private_message("Don't ping me. The private arrangement uses brushed snares.")
        for user_id, channel_id in ((43, 99), (42, 100)):
            with self.subTest(user_id=user_id, channel_id=channel_id), mock.patch.dict(os.environ, {
                "BNL_ORDINARY_CHAT_SINGLE_PACKET_USER_IDS": str(user_id),
                "BNL_ORDINARY_CHAT_SINGLE_PACKET_CHANNEL_IDS": str(channel_id),
            }):
                _, prompt, metadata = self.packet_prompt(user_id=user_id, channel_id=channel_id)
                self.assertNotIn("Private relationship calibration", prompt)
                self.assertNotIn("No proactive recognition or follow-up.", prompt)
                if channel_id != 99:
                    self.assertNotIn("brushed snares", prompt)
                self.assertTrue(metadata["ordinary_chat_single_packet_applied"])

    def test_real_packet_prompt_uses_ready_meaning_instead_of_lexical_praise(self):
        self.enable_packet_and_sealed_tone()
        os.environ.update({
            relationships.MEANING_SHADOW_ENV: "true",
            "BNL_RELATIONSHIP_V2_MEANING_GUILD_IDS": "1",
        })
        with closing(sqlite3.connect(self.db)) as conn, conn:
            # Start with no prior repair so the semantic replacement must
            # cause the visible tone change; mere V2 presence cannot pass.
            conn.execute("DELETE FROM relationship_events_v2")
            conn.execute("DELETE FROM relationship_state_v2")
        sarcastic = "Thanks for ignoring that again."
        source_id = self.private_message(sarcastic)
        _, lexical_prompt, _ = self.packet_prompt()
        self.assertIn("neutral-warm, new/low-history", lexical_prompt)
        self.assertNotIn("Allow repair; do not replay old friction.", lexical_prompt)
        with closing(sqlite3.connect(self.db)) as conn, conn:
            request = relationships.claim_relationship_meaning(conn)
            self.assertIsNotNone(request)
            self.assertEqual(request.target_text, sarcastic)
            self.assertTrue(relationships.finish_relationship_meaning(
                conn, request, text=json.dumps({"signals": [{"type": "friction", "quote": sarcastic}]}),
            ))
            stored = conn.execute("SELECT * FROM relationship_state_v2").fetchall()
        _, semantic_prompt, metadata = self.packet_prompt()
        self.assertIn("neutral, new/low-history", semantic_prompt)
        self.assertNotIn("neutral-warm, new/low-history", semantic_prompt)
        self.assertIn("Allow repair; do not replay old friction.", semantic_prompt)
        final_prompt = bot.build_packet_owned_prompt(
            semantic_prompt, metadata["ordinary_chat_single_packet_basis"],
        )
        self.assertTrue(final_prompt.ready)
        self.assertEqual(final_prompt.prompt.count("Private relationship calibration"), 1)
        self.assertIn("Allow repair; do not replay old friction.", final_prompt.prompt)
        memory = next(item for item in metadata["prompt_source_bases"]
                      if isinstance(item, bot.MemoryPromptSourceBasis))
        self.assertFalse(bot.refresh_prompt_source_basis(memory)[1])
        with mock.patch.dict(os.environ, {relationships.MEANING_SHADOW_ENV: "false"}):
            fresh, changed = bot.refresh_prompt_source_basis(memory)
            self.assertTrue(changed)
            self.assertNotIn("Allow repair; do not replay old friction.", fresh.rendered_context)
        with closing(sqlite3.connect(self.db)) as conn, conn:
            conn.execute(
                "UPDATE memory_ledger_entries SET lifecycle_status='corrected' "
                "WHERE guild_id=1 AND source_table='conversations' AND source_row_id=?",
                (str(source_id),),
            )
        fresh, changed = bot.refresh_prompt_source_basis(memory)
        self.assertTrue(changed)
        self.assertNotIn("Allow repair; do not replay old friction.", fresh.rendered_context)
        with closing(sqlite3.connect(self.db)) as conn:
            self.assertEqual(conn.execute("SELECT * FROM relationship_state_v2").fetchall(), stored)

    def test_combined_packet_rechecks_corrected_and_private_relationship_roots(self):
        self.enable_packet_and_sealed_tone()
        for mutation in ("correction", "privacy"):
            with self.subTest(mutation=mutation):
                source_id = self.private_message("Don't ping me.")
                _, _, metadata = self.packet_prompt()
                packet = metadata["ordinary_chat_single_packet_basis"]
                memory = next(item for item in metadata["prompt_source_bases"]
                              if isinstance(item, bot.MemoryPromptSourceBasis))
                self.assertIn("No proactive recognition or follow-up.", memory.rendered_context)
                self.assertFalse(bot.refresh_prompt_source_basis(packet)[1])
                with closing(sqlite3.connect(self.db)) as conn, conn:
                    field, value = ("lifecycle_status", "corrected") if mutation == "correction" else ("channel_policy", "internal_controlled")
                    conn.execute(
                        "UPDATE memory_ledger_entries SET " + field + "=? "
                        "WHERE guild_id=1 AND source_table='conversations' AND source_row_id=?",
                        (value, str(source_id)),
                    )
                fresh, changed = bot.refresh_prompt_source_basis(memory)
                self.assertTrue(changed)
                self.assertIn("Private relationship calibration", fresh.rendered_context)
                self.assertNotIn("No proactive recognition or follow-up.", fresh.rendered_context)
                # The independent tone basis detects this change; do not make
                # an unrelated factual packet claim ownership of the tone.
                self.assertTrue(bot.prompt_source_basis_failure(metadata["prompt_source_bases"]))

    def test_combined_packet_keeps_independent_relationship_and_packet_kill_switches(self):
        self.enable_packet_and_sealed_tone()
        _, _, metadata = self.packet_prompt()
        packet = metadata["ordinary_chat_single_packet_basis"]
        memory = next(item for item in metadata["prompt_source_bases"]
                      if isinstance(item, bot.MemoryPromptSourceBasis))
        self.assertFalse(bot.refresh_prompt_source_basis(memory)[1])
        self.assertFalse(bot.refresh_prompt_source_basis(packet)[1])
        with mock.patch.dict(os.environ, {relationships.SEALED_CANARY_ENV: "false"}):
            fresh, changed = bot.refresh_prompt_source_basis(memory)
            self.assertTrue(changed)
            self.assertNotIn("Private relationship calibration", fresh.rendered_context)
            self.assertFalse(bot.refresh_prompt_source_basis(packet)[1])
        with mock.patch.dict(os.environ, {"BNL_ORDINARY_CHAT_SINGLE_PACKET_ENABLED": "false"}):
            self.assertTrue(bot.refresh_prompt_source_basis(packet)[1])
            fresh, changed = bot.refresh_prompt_source_basis(memory)
            self.assertFalse(changed)
            self.assertIn("Private relationship calibration", fresh.rendered_context)


if __name__ == "__main__":
    unittest.main()
