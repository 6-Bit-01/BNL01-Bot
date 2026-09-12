"""The existing Moment sweep uses one accounted, cancellable background attempt."""

import asyncio
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
