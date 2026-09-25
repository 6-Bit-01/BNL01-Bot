"""Exercise existing Ambient assembly and the real scheduler/send boundary."""

from contextlib import closing
from datetime import timedelta
import os
import sqlite3
import threading
import unittest
from unittest import mock

import test_ambient_show_context as fixtures
import bnl_memory_ledger as ledger

bot = fixtures.bot
ANSWER = 'The room carries an unexpected rhythm through the evening.'


class AmbientSourceLifecycleTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.fixture = fixtures.AmbientShowContextTests()
        self.fixture.setUp()
        # This fixture has no IsolatedAsyncioTestCase runner of its own.
        self.addCleanup(self.fixture.stack.close)
        self.provider = self.fixture.provider
        with closing(sqlite3.connect(bot.DB_FILE)) as conn:
            self.source_id = conn.execute('SELECT id FROM conversations').fetchone()[0]

    def execute(self, sql, args=()):
        with closing(sqlite3.connect(bot.DB_FILE)) as conn, conn:
            return conn.execute(sql, args).lastrowid

    def tier(self, summary='The room traded ideas about asymmetric percussion.', *, linked=True):
        with closing(sqlite3.connect(bot.DB_FILE)) as conn, conn:
            conn.execute("INSERT OR IGNORE INTO user_habits(user_id,guild_id,total_messages,updated_at) VALUES(7,42,10,'2026-09-11')")
            return bot._insert_memory_tier(
                conn.cursor(), 7, 42, 'short', summary, 0.6,
                source_role='user', source_channel_policy='public_home', source_trust='source_safe_public',
                source_conversation_row_ids=(self.source_id,) if linked else (),
            )

    def broadcast(self, summary='An approved public broadcast observation.', **overrides):
        data = dict(guild_id=42, episode_date='2026-09-04', cleaned_summary=summary,
                    entry_type='show_note', public_safe=1, usage_scope='ambient',
                    status='active', created_at='2026-09-11', updated_at='2026-09-11',
                    raw_note='PRIVATE OPERATOR RAW NOTE')
        data.update(overrides)
        return self.execute(f"INSERT INTO broadcast_memory({','.join(data)}) VALUES({','.join('?' for _ in data)})", tuple(data.values()))

    def retire_source(self, *, lineage=False):
        with closing(sqlite3.connect(bot.DB_FILE)) as conn, conn, mock.patch.dict(os.environ, {'BNL_MEMORY_LEDGER_SHADOW_ENABLED': 'true'}):
            result = ledger.shadow_conversation_row(
                conn, row_id=self.source_id, guild_id=42, user_id=7, user_name='Test Member',
                role='user', content='A new rhythm is forming in the room.',
                channel_id=100, channel_name='barcode-bot', channel_policy='public_home',
                route_mode='normal_chat', observed_at='2026-09-11T18:45:00-07:00',
            )
            self.assertTrue(result.entry_id)
            if lineage:
                conn.execute("INSERT INTO memory_ledger_lineage(entry_id,guild_id,target_entry_id,lineage_type,created_at) VALUES(?,42,?,'correction_of','2026-09-11')",
                             ('test-correction', result.entry_id))
            else:
                conn.execute("UPDATE memory_ledger_entries SET lifecycle_status='forgotten' WHERE entry_id=?", (result.entry_id,))

    async def generate(self, mutation=None, **kwargs):
        async def provider(*_args, **_kwargs):
            if mutation:
                mutation()
            return ANSWER
        self.provider.side_effect = provider
        return await bot.generate_dynamic_ambient(42, 100, **kwargs)

    async def test_prompt_uses_linked_memory_and_approved_current_broadcast_only(self):
        self.tier()
        self.tier('UNLINKED LEGACY CLAIM', linked=False)
        self.broadcast()
        for summary, values in (
            ('EXPIRED CLAIM', {'valid_until': '2026-09-10'}),
            ('UNREVIEWED CLAIM', {'needs_clarification': 1}),
            ('PRIVATE CLAIM', {'public_safe': 0}),
            ('WRONG SCOPE CLAIM', {'usage_scope': 'not_ambient'}),
            ('REPLACED CLAIM', {'superseded_by_id': 900}),
            ('BAD EXPIRY CLAIM', {'valid_until': 'not-a-date'}),
        ):
            self.broadcast(summary, **values)
        self.assertEqual(await self.generate(), ANSWER)
        prompt = self.provider.call_args.args[0]
        self.assertIn('asymmetric percussion', prompt)
        self.assertIn('An approved public broadcast observation.', prompt)
        for forbidden in ('LEGACY CLAIM', 'EXPIRED CLAIM', 'UNREVIEWED CLAIM', 'PRIVATE CLAIM', 'WRONG SCOPE CLAIM', 'REPLACED CLAIM', 'BAD EXPIRY CLAIM', 'PRIVATE OPERATOR RAW NOTE'):
            self.assertNotIn(forbidden, prompt)

    async def test_forgotten_conversation_and_its_memory_never_enter_prompt(self):
        self.tier()
        self.retire_source()
        self.assertEqual(await self.generate(), ANSWER)
        prompt = self.provider.call_args.args[0]
        self.assertNotIn('A new rhythm is forming', prompt)
        self.assertNotIn('asymmetric percussion', prompt)

    async def test_original_changed_during_generation_discards_once_without_retry(self):
        result = await self.generate(lambda: self.execute("UPDATE conversations SET content='Corrected original.' WHERE id=?", (self.source_id,)))
        self.assertEqual(result, '')
        self.provider.assert_awaited_once()

    async def test_correction_control_during_generation_discards_draft(self):
        self.assertEqual(await self.generate(lambda: self.retire_source(lineage=True)), '')
        self.provider.assert_awaited_once()

    async def test_changed_source_blocks_existing_shape_retry(self):
        async def provider(*_args, **_kwargs):
            self.execute("UPDATE conversations SET content='Corrected original.' WHERE id=?", (self.source_id,))
            return 'An unfinished line and'
        self.provider.side_effect = provider
        self.assertEqual(await bot.generate_dynamic_ambient(42, 100), '')
        self.provider.assert_awaited_once()

    async def test_existing_similarity_retry_cannot_bypass_source_check(self):
        calls = []
        async def provider(*_args, **_kwargs):
            calls.append(1)
            if len(calls) == 2:
                self.execute("UPDATE conversations SET content='Corrected original.' WHERE id=?", (self.source_id,))
            return ANSWER
        self.provider.side_effect = provider
        with mock.patch.object(bot, '_too_similar', side_effect=[True, False]), mock.patch.object(bot, 'AMBIENT_RETRY_ON_SIMILAR', 1):
            self.assertEqual(await bot.generate_dynamic_ambient(42, 100), '')
        self.assertEqual(self.provider.await_count, 2)

    async def test_public_permission_withdrawn_during_generation_discards_draft(self):
        self.execute('ALTER TABLE conversations ADD COLUMN public_usable INTEGER DEFAULT 1')
        self.execute("ALTER TABLE conversations ADD COLUMN visibility TEXT DEFAULT 'public'")
        self.assertEqual(await self.generate(lambda: self.execute('UPDATE conversations SET public_usable=0 WHERE id=?', (self.source_id,))), '')

    async def test_linked_memory_changed_during_generation_discards_draft(self):
        tier_id = self.tier()
        self.assertEqual(await self.generate(lambda: self.execute("UPDATE memory_tiers SET summary='A corrected pattern.' WHERE id=?", (tier_id,))), '')

    async def test_memory_lineage_removed_during_generation_discards_draft(self):
        tier_id = self.tier()
        self.assertEqual(await self.generate(lambda: self.execute('DELETE FROM memory_tier_conversation_sources WHERE tier_row_id=?', (tier_id,))), '')

    async def test_tier_control_withdrawal_blocks_summary_even_with_unchanged_original(self):
        tier_id = self.tier()
        def withdraw():
            with closing(sqlite3.connect(bot.DB_FILE)) as conn, conn, mock.patch.dict(os.environ, {'BNL_MEMORY_LEDGER_SHADOW_ENABLED': 'true'}):
                result = ledger.shadow_memory_tier_row(
                    conn, row_id=tier_id, user_id=7, guild_id=42, tier='short',
                    summary='The room traded ideas about asymmetric percussion.', channel_policy='public_home',
                )
                self.assertTrue(result.entry_id)
                conn.execute("UPDATE memory_ledger_entries SET lifecycle_status='forgotten' WHERE entry_id=?", (result.entry_id,))
        self.assertEqual(await self.generate(withdraw), '')
        self.provider.assert_awaited_once()
        self.assertEqual(await self.generate(), ANSWER)
        prompt = self.provider.call_args.args[0]
        self.assertNotIn('asymmetric percussion', prompt)
        self.assertIn('A new rhythm is forming', prompt)

    async def test_unavailable_database_discards_draft_without_generation_retry(self):
        self.assertEqual(await self.generate(lambda: os.rename(bot.DB_FILE, bot.DB_FILE + '.unavailable')), '')
        self.provider.assert_awaited_once()

    async def test_broadcast_expiry_during_generation_discards_draft(self):
        self.broadcast(valid_until=(self.fixture.now + timedelta(seconds=5)).isoformat())
        def advance():
            self.fixture.now += timedelta(seconds=6)
        self.assertEqual(await self.generate(advance), '')

    async def test_new_unrelated_message_does_not_invalidate_selected_originals(self):
        self.assertEqual(await self.generate(lambda: self.execute(
            "INSERT INTO conversations(user_id,user_name,guild_id,role,channel_policy,content) VALUES(8,'Test Other',42,'user','public_home','Another topic.')")), ANSWER)

    async def test_live_observation_expired_during_generation_discards_draft(self):
        self.fixture.write_snapshot()
        def advance():
            self.fixture.now += timedelta(seconds=21)
        self.assertEqual(await self.generate(advance), '')

    async def test_fresh_heartbeat_with_same_state_preserves_draft(self):
        self.fixture.write_snapshot()
        def heartbeat():
            self.fixture.now += timedelta(seconds=3)
            self.fixture.write_snapshot()
        self.assertEqual(await self.generate(heartbeat), ANSWER)

    async def test_website_state_change_during_generation_discards_draft(self):
        def change():
            self.fixture.http.return_value = fixtures.Response(self.fixture.read_model(status='closed', phase='post_show'))
        self.assertEqual(await self.generate(change), '')

    async def test_original_reader_is_off_event_loop(self):
        caller = threading.get_ident()
        threads = []
        original = bot.get_recent_guild_user_messages
        def read(*args, **kwargs):
            threads.append(threading.get_ident())
            return original(*args, **kwargs)
        with mock.patch.object(bot, 'get_recent_guild_user_messages', side_effect=read):
            self.assertEqual(await self.generate(), ANSWER)
        self.assertTrue(threads)
        self.assertTrue(all(thread != caller for thread in threads))

    async def run_scheduler(self, *, withdraw_after_generation=False, withdraw_channel=False, send_error=False):
        self.execute("INSERT INTO guild_configs(guild_id,active_channel_id,next_ambient_message_at) VALUES(42,100,?)",
                     ((self.fixture.now - timedelta(minutes=1)).isoformat(),))
        channel = mock.Mock(id=100)
        channel.send = mock.AsyncMock()
        if send_error:
            channel.send.side_effect = TimeoutError('test uncertain transport')
        policy = ['public_home']
        original = bot.generate_dynamic_ambient
        async def generate(*args, **kwargs):
            result = await original(*args, **kwargs)
            if withdraw_after_generation:
                self.execute("UPDATE conversations SET channel_policy='sealed_test' WHERE id=?", (self.source_id,))
            if withdraw_channel:
                policy[0] = 'protected'
            return result
        with (
            mock.patch.object(bot.client, 'get_channel', return_value=channel),
            mock.patch.object(bot, 'resolve_channel_policy', side_effect=lambda _channel: policy[0]),
            mock.patch.object(bot, 'process_due_occasion_for_guild', new=mock.AsyncMock(return_value={'status': 'idle'})),
            mock.patch.object(bot, 'get_last_ambient_posted_at', return_value=None),
            mock.patch.object(bot, 'ambient_capacity_decision', return_value={'allowed': True, 'capacityUsed': 0, 'cap': 3}),
            mock.patch.object(bot, 'has_ambient_signal', return_value=True),
            mock.patch.object(bot, 'prepare_dormant_echo_canary', new=mock.AsyncMock(return_value={'status': 'idle'})),
            mock.patch.object(bot, 'generate_dynamic_ambient', side_effect=generate),
            mock.patch.object(bot, '_reschedule_ambient_soon') as reschedule,
            mock.patch.object(bot, 'schedule_after_ambient_post') as schedule,
            mock.patch.object(bot, 'log_ambient') as logged,
        ):
            await bot.ambient_message_task.coro()
        return channel.send, reschedule, schedule, logged

    async def test_scheduler_rechecks_after_generation_before_actual_send(self):
        send, reschedule, schedule, logged = await self.run_scheduler(withdraw_after_generation=True)
        self.provider.assert_awaited_once()
        send.assert_not_awaited()
        reschedule.assert_called_once()
        schedule.assert_not_called()
        logged.assert_not_called()

    async def test_scheduler_sends_validated_result_once_and_records_only_after_send(self):
        send, reschedule, schedule, logged = await self.run_scheduler()
        send.assert_awaited_once()
        self.assertEqual(send.call_args.args[0], ANSWER)
        self.provider.assert_awaited_once()
        logged.assert_called_once()
        schedule.assert_called_once()
        reschedule.assert_not_called()

    async def test_destination_policy_is_rechecked_before_send(self):
        send, reschedule, schedule, logged = await self.run_scheduler(withdraw_channel=True)
        send.assert_not_awaited()
        reschedule.assert_called_once()
        schedule.assert_not_called()
        logged.assert_not_called()

    async def test_uncertain_send_does_not_trigger_another_call_or_record_success(self):
        send, _reschedule, schedule, logged = await self.run_scheduler(send_error=True)
        self.provider.assert_awaited_once()
        send.assert_awaited_once()
        schedule.assert_not_called()
        logged.assert_not_called()


    async def test_recent_source_aging_out_during_generation_discards_draft(self):
        self.execute('UPDATE conversations SET timestamp=?', ((self.fixture.now - timedelta(hours=24) + timedelta(seconds=1)).isoformat(),))
        def advance():
            self.fixture.now += timedelta(seconds=2)
        self.assertEqual(await self.generate(advance), '')
        self.provider.assert_awaited_once()

    async def test_older_linked_memory_remains_eligible_as_historical_context(self):
        self.execute('UPDATE conversations SET timestamp=?', ((self.fixture.now - timedelta(days=3)).isoformat(),))
        self.tier()
        self.assertEqual(await self.generate(), ANSWER)
        prompt = self.provider.call_args.args[0]
        self.assertIn('asymmetric percussion', prompt)
        self.assertNotIn('A new rhythm is forming', prompt)


if __name__ == '__main__':
    unittest.main()

