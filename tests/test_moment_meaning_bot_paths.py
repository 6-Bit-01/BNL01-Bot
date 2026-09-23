"""The existing Moment sweep uses one accounted, cancellable background attempt."""

import asyncio
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest import mock

os.environ.setdefault('GEMINI_API_KEY', 'test-gemini-key')
os.environ.setdefault('DISCORD_BOT_TOKEN', 'test-discord-token')
import bnl01_bot as bot
from bnl_gemini_routing import policy_for_route
import test_moment_meaning as fixtures


class MomentMeaningBotTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.fixture = fixtures.MomentMeaningTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.mid, self.roots = self.fixture.captured_moment()
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = str(Path(self.temp.name) / 'test.db')
        dest = sqlite3.connect(self.path)
        self.fixture.conn.backup(dest)
        dest.close()
        for patch in (
            mock.patch.object(bot, 'DB_FILE', self.path),
            mock.patch.object(bot, '_moment_meaning_public_guilds', return_value=(1,)),
            mock.patch.object(bot, '_moment_meaning_task', None),
        ):
            patch.start()
            self.addCleanup(patch.stop)

    def status(self):
        with sqlite3.connect(self.path) as conn:
            return conn.execute('SELECT meaning_status FROM memory_moment_windows WHERE moment_id=?',
                                (self.mid,)).fetchone()[0]

    def claim_and_age(self, *, age=601):
        with sqlite3.connect(self.path) as conn:
            request = bot.claim_pending_moment_meaning(conn, guild_ids=(1,))
            self.assertIsNotNone(request)
            conn.execute('UPDATE memory_moment_windows SET meaning_attempted_at=? WHERE moment_id=?',
                         ((datetime.now(timezone.utc) - timedelta(seconds=age)).isoformat(), self.mid))
            summary = conn.execute('SELECT summary FROM memory_moment_windows WHERE moment_id=?', (self.mid,)).fetchone()[0]
        return request, summary

    async def test_restart_sweep_expires_committed_claim_without_replaying_provider(self):
        request, original_summary = self.claim_and_age()
        # Reopen the persisted database through the real periodic sweep.
        with mock.patch.object(bot, '_generate_gemini_content_result_async') as generate:
            await bot.moment_engine_sweep_task.coro()
            if bot._moment_meaning_task:
                await bot._moment_meaning_task
        generate.assert_not_called()
        self.assertEqual(self.status(), 'interrupted')
        with sqlite3.connect(self.path) as conn:
            self.assertEqual(conn.execute('SELECT summary FROM memory_moment_windows WHERE moment_id=?', (self.mid,)).fetchone()[0], original_summary)
            self.assertFalse(bot.apply_moment_meaning(conn, request, json.dumps(fixtures.REPORTER_MEANING)))
            self.assertIsNone(bot.claim_pending_moment_meaning(conn, guild_ids=(1,)))

    async def test_recent_inflight_claim_is_not_expired_or_duplicated_by_sweep(self):
        self.claim_and_age(age=30)
        with mock.patch.object(bot, '_generate_gemini_content_result_async') as generate:
            await bot.moment_engine_sweep_task.coro()
            if bot._moment_meaning_task:
                await bot._moment_meaning_task
        self.assertEqual(self.status(), 'generating')
        generate.assert_not_called()

    async def test_recovery_respects_existing_shadow_gates(self):
        self.claim_and_age()
        for gate in ('BNL_MEMORY_LEDGER_SHADOW_ENABLED', 'BNL_MOMENT_ENGINE_SHADOW_ENABLED'):
            with mock.patch.dict(os.environ, {gate: 'false'}), sqlite3.connect(self.path) as conn:
                self.assertEqual(bot.expire_stale_moment_meaning_attempts(conn), 0)
        self.assertEqual(self.status(), 'generating')

    async def test_invalid_attempt_timestamp_is_closed_without_provider(self):
        self.claim_and_age()
        with sqlite3.connect(self.path) as conn:
            conn.execute("UPDATE memory_moment_windows SET meaning_attempted_at='invalid' WHERE moment_id=?", (self.mid,))
            self.assertEqual(bot.expire_stale_moment_meaning_attempts(conn), 1)
            self.assertEqual(bot.expire_stale_moment_meaning_attempts(conn), 0)
        self.assertEqual(self.status(), 'interrupted')

    async def test_one_metered_call_runs_after_commit_and_saves_a_derived_revision(self):
        async def provider(prompt, route, *, attempt_counter):
            self.assertEqual(route, 'moment_meaning_background')
            attempt_counter.mark_started()
            self.assertIn('"speaker": "BNL"', prompt)
            # This write would fail if the source snapshot held a write lock.
            with sqlite3.connect(self.path, timeout=0.01) as conn:
                conn.execute('BEGIN IMMEDIATE')
                self.assertEqual(conn.execute('SELECT meaning_status FROM memory_moment_windows WHERE moment_id=?',
                                              (self.mid,)).fetchone()[0], 'generating')
            return bot.GenerationResult(True, json.dumps(fixtures.REPORTER_MEANING),
                                        elapsed_seconds=1.25, estimated_cost_nanos=1000, cost_priced=True)
        with mock.patch.object(bot, '_generate_gemini_content_result_async', side_effect=provider) as generate:
            await bot._process_one_moment_meaning()
            await bot._process_one_moment_meaning()
        generate.assert_awaited_once()
        self.assertEqual(self.status(), 'ready')

    async def test_provider_failure_keeps_original_gist_without_retry(self):
        with mock.patch.object(bot, '_generate_gemini_content_result_async',
                               return_value=bot.GenerationResult(False)) as generate:
            await bot._process_one_moment_meaning()
            await bot._process_one_moment_meaning()
        generate.assert_awaited_once()
        self.assertEqual(self.status(), 'provider_unavailable')

    async def test_scope_disabled_during_provider_wait_discards_result(self):
        async def provider(*args, **kwargs):
            bot._moment_meaning_public_guilds.return_value = ()
            return bot.GenerationResult(True, json.dumps(fixtures.REPORTER_MEANING))
        with mock.patch.object(bot, '_generate_gemini_content_result_async', side_effect=provider):
            await bot._process_one_moment_meaning()
        self.assertEqual(self.status(), 'scope_disabled')

    async def test_source_changed_during_provider_wait_discards_result(self):
        async def provider(*args, **kwargs):
            with sqlite3.connect(self.path) as conn:
                conn.execute("UPDATE memory_ledger_entries SET normalized_value='A corrected source.' WHERE entry_id=?",
                             (self.roots[0],))
            return bot.GenerationResult(True, json.dumps(fixtures.REPORTER_MEANING))
        with mock.patch.object(bot, '_generate_gemini_content_result_async', side_effect=provider):
            await bot._process_one_moment_meaning()
        self.assertEqual(self.status(), 'source_changed')

    async def test_cancelled_background_call_never_applies_a_late_result(self):
        entered = asyncio.Event()
        async def provider(*args, **kwargs):
            entered.set()
            await asyncio.Event().wait()
        with mock.patch.object(bot, '_generate_gemini_content_result_async', side_effect=provider):
            worker = asyncio.create_task(bot._process_one_moment_meaning())
            await asyncio.wait_for(entered.wait(), timeout=2)
            worker.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await worker
        self.assertEqual(self.status(), 'interrupted')

    async def test_sweep_starts_only_one_background_worker_at_a_time(self):
        done = asyncio.Event()
        with mock.patch.object(bot, '_process_one_moment_meaning', side_effect=done.wait) as process:
            bot._start_moment_meaning_work()
            first = bot._moment_meaning_task
            bot._start_moment_meaning_work()
            self.assertIs(first, bot._moment_meaning_task)
            done.set()
            await first
        process.assert_awaited_once()

    def test_generation_uses_json_and_bounded_nonprotected_background_policy(self):
        with mock.patch.dict(os.environ, {'BNL_GEMINI_PROVIDER_RETRIES': '2'}):
            policy = policy_for_route('moment_meaning_background')
        self.assertEqual(policy.lane, 'background')
        self.assertEqual(policy.max_output_tokens, 2048)
        self.assertEqual(policy.provider_retries, 0)
        self.assertFalse(policy.allow_fallback)
        self.assertFalse(policy.journal_protected)
        self.assertFalse(policy.relay_protected)
        config = bot._generation_config_for_model(bot.GEMINI_MODEL, 'moment_meaning_background')
        self.assertEqual(config.response_mime_type, 'application/json')
        self.assertEqual(config.max_output_tokens, 2048)


if __name__ == '__main__':
    unittest.main()
