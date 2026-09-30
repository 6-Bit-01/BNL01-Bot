import asyncio
from contextlib import ExitStack
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl01_bot as bot
import bnl_journal as journal
from bnl_shared_brain_synthesis import ordinary_chat_task_support_plan


class PublicationPromptLifecycleTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db = str(Path(self.tmp.name) / "bot.db")
        self.patch_db = mock.patch.object(bot, "DB_FILE", self.db)
        self.patch_db.start()
        self.addCleanup(self.patch_db.stop)
        journal.ensure_schema(self.db)
        now = datetime.now(timezone.utc)
        self.snapshot = journal.JournalControlSnapshot(
            snapshot_version=1,
            revision=(now - timedelta(minutes=2)).isoformat(),
            digest="a" * 64,
            observed_at=now.isoformat(),
            fresh_until=(now + timedelta(seconds=120)).isoformat(),
            fresh_for_seconds=120,
            public_excluded_entry_ids=(), memory_excluded_entry_ids=(),
        )
        self.add_journal("journal_lifecycle", "Ceramic Receivers", "Ceramic receivers returned.",
                         "Receivers caught the signal.", "2026-08-02T01:00:00Z")

    def add_journal(self, entry_id, title, excerpt, body, published_at, *, lifecycle="published"):
        entry = {
            "entryId": entry_id, "revision": 1,
            "title": title, "excerpt": excerpt,
            "sections": [{"heading": "Carrier Notes", "body": body}],
            "authoredAt": "2026-08-02T00:00:00Z",
            "sourceWindowStart": "2026-08-01T00:00:00Z",
            "sourceWindowEnd": "2026-08-02T00:00:00Z",
        }
        def canonical(value):
            return json.dumps(value, sort_keys=True, separators=(",", ":"))
        sections = canonical(entry["sections"])
        entry["contentHash"] = hashlib.sha256(
            "|".join((entry["title"], entry["excerpt"], sections)).encode()
        ).hexdigest()
        payload = canonical({"contractVersion": 1, "kind": "journal_entry", "entry": entry})
        with sqlite3.connect(self.db) as conn:
            conn.execute(
                """INSERT INTO bnl_journal_entries(
                  entry_id,revision,guild_id,lifecycle_state,title,excerpt,
                  sections_json,public_payload_json,canonical_payload_bytes,
                  content_hash,source_window_start,source_window_end,authored_at,
                  published_at,created_at,updated_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (entry["entryId"], 1, 1, lifecycle, entry["title"], entry["excerpt"],
                 sections, payload, payload.encode(), entry["contentHash"],
                 entry["sourceWindowStart"], entry["sourceWindowEnd"], entry["authoredAt"],
                 published_at, entry["authoredAt"], entry["authoredAt"]),
            )

    def basis(self):
        return bot._build_publication_prompt_source_basis(
            guild_id=1, user_text="Tell me about ceramic receivers.",
            source_kind="journal", journal_control_snapshot=self.snapshot,
            journal_control_snapshot_provided=True,
        )

    def enable_actual_publication_pipeline(self):
        stack = ExitStack()
        self.addCleanup(stack.close)
        stack.enter_context(mock.patch.dict(os.environ, {
            **{key: "false" for key in os.environ
               if key.startswith("BNL_") and key.endswith("_ENABLED")},
            "BNL_CONVERSATION_CONTEXT_V2_ENABLED": "true",
            "BNL_MEMORY_LEDGER_SHADOW_ENABLED": "true",
            "BNL_MOMENT_ENGINE_SHADOW_ENABLED": "true",
            "BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED": "true",
            "BNL_RELATIONSHIP_V2_SHADOW_ENABLED": "true",
            "BNL_UNIFIED_INTELLIGENCE_PACKET_SHADOW_ENABLED": "true",
            "BNL_UNIFIED_RESPONSE_ASSESSMENT_SHADOW_ENABLED": "true",
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_ENABLED": "true",
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_PUBLIC_ENABLED": "true",
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_GUILD_IDS": "1",
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_USER_IDS": "7",
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_CHANNEL_IDS": "10",
        }))
        bot.init_db()
        bot.upsert_user_profile(7, 1, "Test Speaker")
        self.addCleanup(bot.purge_member_memory_caches, 7, 1)
        self.pipeline_now = datetime.fromisoformat(self.snapshot.observed_at)
        stack.enter_context(mock.patch.object(
            bot, "_journal_publication_control_snapshot_sync", return_value=(self.snapshot, "valid"),
        ))
        stack.enter_context(mock.patch.object(bot, "get_temporal_context",
            return_value=bot.get_temporal_context(self.pipeline_now)))
        stack.enter_context(mock.patch.object(bot, "should_allow_greeting", return_value=False))
        stack.enter_context(mock.patch.object(bot, "choose_response_style",
            return_value=("balanced", "Respond naturally.")))

    def actual_publication_prompt(self, text, policy, *, unrelated_hint=False,
                                  history=None, history_age_minutes=0):
        # Neutral earlier exchanges exercise the real history selector. They
        # are not the latest Journal and must not become its source authority.
        history = history or (
            ("user", "Recall the correction about the missing shoes."),
            ("model", "The correction concerned footwear."),
            ("user", "What practical question did the earlier discussion leave open?"),
            ("model", "Earlier we discussed intake feedback."),
        )
        with sqlite3.connect(self.db) as conn:
            conn.execute("DELETE FROM conversations")
            for index, (role, content) in enumerate(history, 1):
                conn.execute("""INSERT INTO conversations(
                    guild_id,user_id,user_name,role,content,channel_id,channel_name,
                    channel_policy,route_mode,timestamp
                ) VALUES(?,?,?,?,?,?,?,?,?,?)""", (
                    1, 7, "Test Speaker", role, content, 10, "test-room", policy,
                    "normal_chat", (self.pipeline_now - timedelta(minutes=history_age_minutes+5-index)).isoformat(),
                ))
        context_out = {}
        surface = bot.conversation_surface_for_channel_policy(policy)
        room = bot.build_conversation_context_v2_for_prompt(
            guild_id=1, current_user_id=7, channel_id=10, channel_name="test-room",
            channel_policy=policy, route_mode="normal_chat", conversation_surface=surface,
            current_texts=(text,), current_participants={7}, is_direct_target=True,
            now=self.pipeline_now, result_out=context_out,
        )
        context = context_out["result"]
        orchestration = bot.build_live_conversation_orchestration_decision(
            engagement_decision="answer", engagement_reason="direct_request",
            channel_policy=policy, addressings=(), context_result=context,
            moment_situation=None, guild_id=1, channel_id=10, route_mode="normal_chat",
            conversation_surface=surface, current_text=text,
            current_speaker_user_ids=(7,), current_speaker_labels=("Test Speaker",),
            # A reversible label is a hint, never a fabricated person binding.
            subject_label_hints=("Test Visitor",) if unrelated_hint else (),
            influence_mode="live",
        )
        metadata = {}
        prompt, *_ = bot.build_user_aware_prompt(
            7, 1, "Test Speaker", text, channel_id=10, channel_name="test-room",
            channel_policy=policy, route_mode="normal_chat", is_direct_interaction=True,
            room_context=room, conversation_context_result=context,
            conversation_orchestration=orchestration, prompt_metadata=metadata,
        )
        self.assertTrue(metadata["ordinary_chat_single_packet_applied"])
        basis = metadata["ordinary_chat_single_packet_basis"]
        self.assertIsNotNone(basis)
        final = bot.build_packet_owned_prompt(prompt, basis)
        return context, orchestration.situation_frame, basis, final

    def test_live_journal_requests_survive_real_context_frame_packet_pipeline(self):
        self.enable_actual_publication_pipeline()
        requests = (
            "BNL, we can put that rough exchange behind us. The limit on teasing still stands. "
            "Which part of your latest published Journal deserves another conversation, and why?",
            "BNL, I mean your most recently published Journal. Pick one actual topic from it "
            "and tell me why you think it matters to the community.",
            "I mean your newest published Journal. Choose a subject from it and explain why it matters.",
        )
        for policy in ("public_home", "sealed_test"):
            for index, text in enumerate(requests):
                variants = ((False, 0), (True, 0), (False, 130)) if index == 0 else ((False, 0), (True, 0))
                prior = (("user", requests[0]), ("model", "Which Journal edition do you mean?")) if index else None
                for hint, history_age in variants:
                    with self.subTest(policy=policy, request=text, hint=hint, history_age=history_age):
                        context, frame, basis, final = self.actual_publication_prompt(
                            text, policy, unrelated_hint=hint, history=prior,
                            history_age_minutes=history_age,
                        )
                        diagnostics = basis.packet.diagnostics
                        self.assertEqual(diagnostics.journal_query_status, "eligible")
                        self.assertEqual(diagnostics.journal_control_status, "valid")
                        self.assertGreater(diagnostics.journal_candidate_count, 0)
                        path = {
                            "frame": frame.status,
                            "tasks": [(task.task_kind, task.object_kind, task.authority_scope) for task in frame.tasks],
                            "source_status": diagnostics.revalidation_status,
                            "selected_lanes": [item.lane for item in basis.packet.items],
                            "journal_in_final_prompt": "Receivers caught the signal" in final.prompt,
                        }
                        self.assertEqual(context.referent_status, "not_requested", path)
                        if history_age:
                            self.assertEqual(context.referent_candidate_count, 0)
                            self.assertFalse(context.referent_selected_row_ids)
                        self.assertNotIn("referent_unresolved", frame.ambiguity_reasons)
                        journal_tasks = [task for task in frame.tasks
                                         if task.object_kind == "journal" and task.task_kind == "retrieve_publication"]
                        self.assertTrue(journal_tasks)
                        self.assertFalse(any(task.authority_scope == "external_public" for task in frame.tasks))
                        self.assertEqual(basis.packet.diagnostics.revalidation_status, "passed")
                        publications = [item for item in basis.packet.items if item.lane == "journal_publication"]
                        self.assertEqual([item.source_ref for item in publications], ["journal:journal_lifecycle:1"])
                        self.assertIn("Receivers caught the signal", publications[0].text)
                        self.assertNotIn("intake feedback", publications[0].text)
                        self.assertTrue(final.ready, final.reason)
                        self.assertIn("Ceramic Receivers", final.prompt)
                        self.assertIn(text, final.prompt)
                        plans = {plan.task_id: plan for plan in ordinary_chat_task_support_plan(basis)}
                        for task in journal_tasks:
                            self.assertEqual(plans[task.task_id].support_kind, "packet")
                            self.assertTrue(plans[task.task_id].evidence_ids)
                        self.assertFalse(bot.refresh_prompt_source_basis(basis)[1])

    def test_most_recently_selects_the_newest_edition_through_the_real_prompt_pipeline(self):
        self.enable_actual_publication_pipeline()
        self.add_journal(
            "journal_older_dense", "An actual community topic",
            "An actual topic that matters to the community.",
            "Earlier intake feedback was a community topic worth discussing.",
            "2026-07-25T01:00:00Z",
        )
        self.add_journal(
            "journal_unpublished", "Unpublished future edition", "This is only a draft.",
            "Unpublished copy must not become the newest publication.", None,
            lifecycle="draft",
        )
        text = ("BNL, I mean your most recently published Journal. Pick one actual topic from it "
                "and tell me why you think it matters to the community.")
        history = (("user", "Which part of your latest Journal deserves another conversation?"),
                   ("model", "Which Journal edition do you mean?"))
        for policy in ("public_home", "sealed_test"):
            for hint in (False, True):
                with self.subTest(policy=policy, unrelated_hint=hint):
                    _context, _frame, basis, final = self.actual_publication_prompt(
                        text, policy, unrelated_hint=hint, history=history,
                    )
                    publications = [item for item in basis.packet.items if item.lane == "journal_publication"]
                    self.assertEqual([item.source_ref for item in publications], ["journal:journal_lifecycle:1"])
                    self.assertTrue(final.ready, final.reason)
                    self.assertIn("Receivers caught the signal", final.prompt)
                    self.assertNotIn("Earlier intake feedback was a community topic", final.prompt)
                    self.assertNotIn("Unpublished copy must not", final.prompt)
                    self.assertFalse(bot.refresh_prompt_source_basis(basis)[1])

        self.add_journal(
            "journal_just_published", "A later published edition", "A newer edition is now public.",
            "A newer release supersedes the selection before delivery.", "2026-08-03T01:00:00Z",
        )
        self.assertTrue(bot.refresh_prompt_source_basis(basis)[1])

    def test_actual_publication_pipeline_keeps_person_and_ambiguous_reference_fences(self):
        self.enable_actual_publication_pipeline()
        for policy in ("public_home", "sealed_test"):
            for text in (
                "What did that Journal entry say?",
                "What does your latest published Journal say about Test Visitor?",
                "What is in your latest Journal? What does your latest Journal say about Test Visitor?",
            ):
                with self.subTest(policy=policy, text=text):
                    _context, frame, basis, final = self.actual_publication_prompt(
                        text, policy, unrelated_hint=True,
                    )
                    self.assertFalse(any(item.lane == "journal_publication" for item in basis.packet.items))
                    self.assertNotIn("Receivers caught the signal", final.prompt)
                    self.assertTrue(frame.ambiguity_reasons or
                                    any(task.subject_requirement == "required" for task in frame.tasks))
        text = "I mean your latest published Journal. Where is Seattle?"
        _context, frame, _basis, _final = self.actual_publication_prompt(text, "sealed_test")
        self.assertTrue(any(task.authority_scope == "external_public" for task in frame.tasks))

    def test_actual_publication_pipeline_rechecks_source_withdrawal(self):
        self.enable_actual_publication_pipeline()
        text = "BNL, I mean your most recently published Journal. Pick one actual topic from it."
        _context, _frame, basis, final = self.actual_publication_prompt(text, "sealed_test", unrelated_hint=True)
        self.assertEqual(basis.packet.diagnostics.journal_query_status, "eligible")
        self.assertEqual(basis.packet.diagnostics.journal_control_status, "valid")
        self.assertGreater(basis.packet.diagnostics.journal_candidate_count, 0)
        self.assertTrue(final.ready)
        self.assertIn("Receivers caught the signal", final.prompt)
        self.assertFalse(bot.refresh_prompt_source_basis(basis)[1])
        hidden = replace(self.snapshot, public_excluded_entry_ids=("journal_lifecycle",), digest="b" * 64)
        with mock.patch.object(bot, "_journal_publication_control_snapshot_sync", return_value=(hidden, "valid")):
            self.assertTrue(bot.refresh_prompt_source_basis(basis)[1])
        with sqlite3.connect(self.db) as conn:
            conn.execute("UPDATE bnl_journal_entries SET lifecycle_state='retired'")
        self.assertTrue(bot.refresh_prompt_source_basis(basis)[1])

    async def test_wrapper_builds_once_off_loop_and_publishes_metadata(self):
        loop_thread = threading.get_ident()
        metadata = {"original": True}
        def build(*args, **kwargs):
            self.assertNotEqual(loop_thread, threading.get_ident())
            self.assertIsNot(kwargs["prompt_metadata"], metadata)
            kwargs["prompt_metadata"]["built"] = True
            return "prompt", False, "steady"
        with mock.patch.object(bot, "build_user_aware_prompt", side_effect=build) as builder:
            result = await bot.build_user_aware_prompt_async(
                7, 1, "Test Member", "Hello", prompt_metadata=metadata,
            )
        builder.assert_called_once()
        self.assertEqual(("prompt", False, "steady"), result)
        self.assertEqual({"original": True, "built": True}, metadata)

    async def test_cancelled_builder_cannot_mutate_caller_metadata(self):
        started, release, finished = threading.Event(), threading.Event(), threading.Event()
        metadata = {"original": True}
        def build(*args, **kwargs):
            started.set()
            try:
                release.wait(2)
                kwargs["prompt_metadata"]["abandoned"] = True
                return "prompt", False, "steady"
            finally:
                finished.set()
        with mock.patch.object(bot, "build_user_aware_prompt", side_effect=build):
            task = asyncio.create_task(bot.build_user_aware_prompt_async(
                7, 1, "Test Member", "Hello", prompt_metadata=metadata,
            ))
            try:
                self.assertTrue(await asyncio.to_thread(started.wait, 2))
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
            finally:
                release.set()
                self.assertTrue(await asyncio.to_thread(finished.wait, 2))
        self.assertEqual({"original": True}, metadata)

    def prompt_builder_patches(self, packet_basis, snapshot=None, control_status="valid"):
        stack = ExitStack()
        self.addCleanup(stack.close)
        results = {
            "get_user_profile": ("Test Member", ""), "should_allow_greeting": False,
            "choose_response_style": ("steady", "Respond naturally."),
            "build_user_memory_context": "", "build_broadcast_memory_context": "",
            "build_queue_artist_memory_context": "", "build_tiktok_show_evidence_context_for_turn": "",
            "build_community_visual_basis": SimpleNamespace(status="not_requested"),
            "render_community_visual_basis_for_prompt": "",
            "source_safe_recall_synthesis_enabled": False,
            "ordinary_chat_route_scope_decision": SimpleNamespace(eligible=True),
            "build_ordinary_chat_basis": packet_basis,
        }
        for name, result in results.items():
            stack.enter_context(mock.patch.object(bot, name, return_value=result))
        def assessment(**kwargs):
            kwargs["intelligence_packet_out"]["packet"] = SimpleNamespace(request=SimpleNamespace(
                journal_control_snapshot=snapshot, journal_control_status=control_status,
            ))
            return None
        stack.enter_context(mock.patch.object(bot, "build_unified_response_assessment_shadow", side_effect=assessment))

    async def test_actual_packet_basis_skips_normal_publication_selection(self):
        self.prompt_builder_patches(object(), self.snapshot)
        with mock.patch.object(bot, "build_publication_prompt_source_bases", side_effect=AssertionError("redundant publication read")):
            result = await bot.build_user_aware_prompt_async(
                7, 1, "Test Member", "What did the latest Journal say?",
                channel_policy="public_home", is_direct_interaction=True,
            )
        self.assertNotIn("Published Journal context", result[0])

    async def test_normal_fallback_reuses_shadow_control_observation(self):
        self.prompt_builder_patches(None, self.snapshot)
        with mock.patch.object(bot, "_journal_publication_control_snapshot_sync", side_effect=AssertionError("duplicate control fetch")):
            result = await bot.build_user_aware_prompt_async(
                7, 1, "Test Member", "What did the latest Journal say?",
                channel_policy="public_home", is_direct_interaction=True,
            )
        self.assertIn("Published Journal context", result[0])
        self.assertIn("Ceramic Receivers", result[0])

    async def test_failed_shadow_controls_do_not_trigger_selection_retry(self):
        self.prompt_builder_patches(None, None, "control_snapshot_unavailable")
        with mock.patch.object(bot, "_journal_publication_control_snapshot_sync", side_effect=AssertionError("duplicate control fetch")):
            result = await bot.build_user_aware_prompt_async(
                7, 1, "Test Member", "What did the latest Journal say?",
                channel_policy="public_home", is_direct_interaction=True,
            )
        self.assertNotIn("Published Journal context", result[0])

    async def test_fresh_control_revocation_removes_only_journal_context(self):
        basis = self.basis()
        prompt = "Current queue: closed.\n" + basis.rendered_context
        for fresh in (
            replace(self.snapshot, public_excluded_entry_ids=("journal_lifecycle",)),
            replace(self.snapshot, memory_excluded_entry_ids=("journal_lifecycle",)),
            replace(self.snapshot, digest="b" * 64),
            None,
        ):
            with self.subTest(fresh=fresh), mock.patch.object(
                bot, "_journal_publication_control_snapshot_sync", return_value=(fresh, "")
            ) as controls:
                revised, sources, changed, failed = bot.refresh_prompt_source_bases(prompt, (basis,))
                controls.assert_called_once()
                self.assertEqual("Current queue: closed.\n", revised)
                self.assertEqual(("publication",), changed)
                self.assertFalse(failed)
                self.assertFalse(sources[0].publications)

    async def test_fresh_same_authority_remains_eligible(self):
        basis = self.basis()
        later = replace(
            self.snapshot,
            observed_at=(datetime.fromisoformat(self.snapshot.observed_at) + timedelta(seconds=1)).isoformat(),
            fresh_until=(datetime.fromisoformat(self.snapshot.fresh_until) + timedelta(seconds=1)).isoformat(),
        )
        with mock.patch.object(bot, "_journal_publication_control_snapshot_sync", return_value=(later, "")) as controls:
            fresh, changed = bot.refresh_prompt_source_basis(basis)
        controls.assert_called_once()
        self.assertFalse(changed)
        self.assertEqual(basis.expected_digest, fresh.expected_digest)
        self.assertEqual(later, fresh.journal_control_snapshot)

    async def test_source_change_during_control_await_is_caught_by_sync_fence(self):
        basis = self.basis()
        loop_thread = threading.get_ident()
        def observe():
            self.assertNotEqual(loop_thread, threading.get_ident())
            with sqlite3.connect(self.db) as conn:
                conn.execute("UPDATE bnl_journal_entries SET lifecycle_state='retired'")
            return self.snapshot, ""
        with mock.patch.object(bot, "_journal_publication_control_snapshot_sync", side_effect=observe) as controls:
            snapshot, provided = await bot.journal_control_snapshot_for_source_fence((basis,))
            reason = bot.prompt_source_basis_failure(
                (basis,), journal_control_snapshot=snapshot,
                journal_control_snapshot_provided=provided,
            )
        controls.assert_called_once()
        self.assertEqual("publication_source_changed", reason)

    async def test_failed_fence_observation_is_not_retried_or_replaced(self):
        basis = self.basis()
        with mock.patch.object(bot, "_journal_publication_control_snapshot_sync", return_value=(None, "unavailable")) as controls:
            snapshot, provided = await bot.journal_control_snapshot_for_source_fence((basis,))
            reason = bot.prompt_source_basis_failure(
                (basis,), journal_control_snapshot=snapshot,
                journal_control_snapshot_provided=provided,
            )
        controls.assert_called_once()
        self.assertEqual("publication_source_changed", reason)
