"""Existing maintenance and provider accounting own semantic comparison work."""
import asyncio
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest import mock

os.environ.setdefault('GEMINI_API_KEY', 'test-key')
os.environ.setdefault('DISCORD_BOT_TOKEN', 'test-token')
import bnl01_bot as bot
from bnl_gemini_routing import policy_for_route
import bnl_relationship_engine as rel
from tests import test_relationship_meaning as fixtures


class RelationshipMeaningBotTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.fixture = fixtures.RelationshipMeaningTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.fixture.observe('Please stop teasing me.')
        self.fixture.conn.commit()
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = str(Path(self.directory.name) / 'comparison.db')
        with sqlite3.connect(self.path) as dest:
            self.fixture.conn.backup(dest)
        dest.close()
        for patch in (mock.patch.object(bot, 'DB_FILE', self.path),
                      mock.patch.object(bot, '_relationship_meaning_task', None)):
            patch.start()
            self.addCleanup(patch.stop)

    def status(self):
        with sqlite3.connect(self.path) as conn:
            return conn.execute('SELECT status FROM relationship_meaning_v2').fetchone()[0]

    async def test_metered_background_call_commits_before_provider_and_records_comparison_only(self):
        async def provider(prompt, route, *, attempt_counter):
            self.assertEqual(route, 'relationship_meaning_background')
            self.assertIn('Please stop teasing me.', prompt)
            attempt_counter.mark_started()
            with sqlite3.connect(self.path, timeout=0.01) as conn:
                conn.execute('BEGIN IMMEDIATE')
                self.assertEqual(conn.execute('SELECT status FROM relationship_meaning_v2').fetchone()[0], 'generating')
            return bot.GenerationResult(True, self.fixture.result('boundary', 'Please stop teasing me.'))
        with mock.patch.object(bot, '_generate_gemini_content_result_async', side_effect=provider) as generate:
            await bot._process_one_relationship_meaning()
            await bot._process_one_relationship_meaning()
        generate.assert_awaited_once()
        self.assertEqual(self.status(), 'ready')
        with sqlite3.connect(self.path) as conn:
            self.assertEqual(conn.execute('SELECT COUNT(*) FROM relationship_events_v2').fetchone()[0], 0)
            self.assertEqual(conn.execute('SELECT COUNT(*) FROM relationship_member_preferences_v2').fetchone()[0], 0)
        policy = policy_for_route('relationship_meaning_background')
        self.assertEqual(policy.lane, 'background')
        self.assertEqual(policy.provider_retries, 0)
        self.assertFalse(policy.allow_fallback)
        self.assertTrue(policy.memory_protected)

    async def test_zero_call_budget_denial_defers_durably_then_revalidates_sources(self):
        denied = bot.GenerationResult(False, error_category=bot.GENERATION_ERROR_LOCAL_MODEL_BUDGET)
        with mock.patch.object(bot, '_generate_gemini_content_result_async', return_value=denied) as generate:
            await bot._process_one_relationship_meaning()
            await bot._process_one_relationship_meaning()
            generate.assert_awaited_once()
        self.assertEqual(self.status(), 'budget_deferred')
        with sqlite3.connect(self.path) as conn:
            conn.execute("UPDATE relationship_meaning_v2 SET retry_after='2000-01-01'")
        success = bot.GenerationResult(True, self.fixture.result('boundary', 'Please stop teasing me.'))
        with mock.patch.object(bot, '_generate_gemini_content_result_async', return_value=success) as generate:
            await bot._process_one_relationship_meaning()
            await bot._process_one_relationship_meaning()
            generate.assert_awaited_once()
        self.assertEqual(self.status(), 'ready')

    async def test_provider_attempt_never_replayed_even_with_local_budget_category(self):
        async def provider(*args, attempt_counter):
            attempt_counter.mark_started()
            return bot.GenerationResult(False, error_category=bot.GENERATION_ERROR_LOCAL_MODEL_BUDGET)
        with mock.patch.object(bot, '_generate_gemini_content_result_async', side_effect=provider) as generate:
            await bot._process_one_relationship_meaning()
            await bot._process_one_relationship_meaning()
            generate.assert_awaited_once()
        self.assertEqual(self.status(), 'provider_unavailable')

    async def test_cancelled_attempt_closed_without_retry(self):
        with mock.patch.object(bot, '_generate_gemini_content_result_async', side_effect=asyncio.CancelledError):
            with self.assertRaises(asyncio.CancelledError):
                await bot._process_one_relationship_meaning()
        self.assertEqual(self.status(), 'interrupted')
        with mock.patch.object(bot, '_generate_gemini_content_result_async') as generate:
            await bot._process_one_relationship_meaning()
            generate.assert_not_called()

    async def test_revoked_scope_during_call_cannot_save_result(self):
        async def provider(*args, **kwargs):
            os.environ[rel.MEANING_SHADOW_ENV] = '0'
            return bot.GenerationResult(True, self.fixture.result('boundary', 'Please stop teasing me.'))
        with mock.patch.object(bot, '_generate_gemini_content_result_async', side_effect=provider):
            await bot._process_one_relationship_meaning()
        self.assertEqual(self.status(), 'scope_disabled')

    async def test_disabled_scope_performs_no_provider_or_database_work(self):
        with mock.patch.dict(os.environ, {rel.MEANING_SHADOW_ENV: '0'}), \
                mock.patch.object(bot, 'DB_FILE', str(Path(self.directory.name) / 'must-not-exist.db')), \
                mock.patch.object(bot, '_generate_gemini_content_result_async') as generate:
            await bot._process_one_relationship_meaning()
            bot._start_relationship_meaning_work()
            self.assertIsNone(bot._relationship_meaning_task)
            generate.assert_not_called()
            self.assertFalse(Path(bot.DB_FILE).exists())

    async def test_background_starter_starts_one_worker_without_waiting_for_provider(self):
        started, release = asyncio.Event(), asyncio.Event()
        async def provider(*args, **kwargs):
            started.set()
            await release.wait()
            return bot.GenerationResult(True, '{"signals":[]}')
        with mock.patch.object(bot, '_generate_gemini_content_result_async', side_effect=provider) as generate:
            bot._start_relationship_meaning_work()
            first_task = bot._relationship_meaning_task
            await asyncio.wait_for(started.wait(), timeout=5)
            bot._start_relationship_meaning_work()
            self.assertIs(bot._relationship_meaning_task, first_task)
            release.set()
            await first_task
            generate.assert_awaited_once()

    async def test_real_capture_and_existing_maintenance_reach_comparison_without_moment_gate(self):
        path = str(Path(self.directory.name) / 'capture.db')
        with mock.patch.object(bot, 'DB_FILE', path), \
                mock.patch.dict(os.environ, {'BNL_MOMENT_ENGINE_SHADOW_ENABLED': '0'}):
            bot.init_db()
            bot.save_user_message(2, 'Test Member', 1, 'Please stop teasing me.',
                channel_id=10, channel_policy='public_home', route_mode='normal_chat', directed_to_bnl=True)
            with sqlite3.connect(path) as conn:
                self.assertEqual(conn.execute('SELECT COUNT(*) FROM relationship_meaning_v2').fetchone()[0], 1)
            with mock.patch.object(bot, '_generate_gemini_content_result_async',
                    return_value=bot.GenerationResult(True, self.fixture.result('boundary', 'Please stop teasing me.'))) as generate:
                await bot.moment_engine_sweep_task.coro()
                self.assertIsNotNone(bot._relationship_meaning_task)
                await bot._relationship_meaning_task
                generate.assert_awaited_once()


if __name__ == '__main__':
    unittest.main()
