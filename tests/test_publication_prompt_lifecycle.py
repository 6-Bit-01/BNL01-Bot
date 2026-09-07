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
        entry = {
            "entryId": "journal_lifecycle", "revision": 1,
            "title": "Ceramic Receivers", "excerpt": "Ceramic receivers returned.",
            "sections": [{"heading": "Carrier Notes", "body": "Receivers caught the signal."}],
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
                (entry["entryId"], 1, 1, "published", entry["title"], entry["excerpt"],
                 sections, payload, payload.encode(), entry["contentHash"],
                 entry["sourceWindowStart"], entry["sourceWindowEnd"], entry["authoredAt"],
                 "2026-08-02T01:00:00Z", entry["authoredAt"], entry["authoredAt"]),
            )

    def basis(self):
        return bot._build_publication_prompt_source_basis(
            guild_id=1, user_text="Tell me about ceramic receivers.",
            source_kind="journal", journal_control_snapshot=self.snapshot,
            journal_control_snapshot_provided=True,
        )

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
