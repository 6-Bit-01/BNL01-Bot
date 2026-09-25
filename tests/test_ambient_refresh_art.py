from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from datetime import timedelta
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import unittest
from unittest import mock

import test_ambient_show_context as fixtures
import bnl_ambient_art as art

bot = fixtures.bot
REAL_GET = bot.get_gemini_response
TEXT = 'I keep returning to the shape of a rhythm that leaves space for an answer.'
CONCEPT = {'action': 'create', 'title': 'Space Between', 'meaning': 'An imagined room shaped by rhythm.',
           'imagePrompt': 'An original abstract room made of distant percussion.', 'inspirationRefs': []}


class AmbientRefreshTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.fixture = fixtures.AmbientShowContextTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.stack.close)
        self.provider = self.fixture.provider
        self.fixture.stack.enter_context(mock.patch.dict(os.environ, {'BNL_OWN_ART_ENABLED': 'false'}))

    def execute(self, sql, args=()):
        with sqlite3.connect(bot.DB_FILE) as conn:
            return conn.execute(sql, args).fetchall()

    async def test_current_public_selective_included_old_future_private_excluded(self):
        self.execute('DELETE FROM conversations')
        for text, when, policy in (
            ('Fresh public rhythm', self.fixture.now, 'public_selective'),
            ('OLD CLAIM', self.fixture.now - timedelta(days=2), 'public_home'),
            ('FUTURE CLAIM', self.fixture.now + timedelta(hours=2), 'public_home'),
            ('PRIVATE CLAIM', self.fixture.now, 'sealed_test'),
        ):
            self.execute("INSERT INTO conversations(user_id,user_name,guild_id,channel_id,role,channel_policy,content,timestamp) VALUES(8,'Test Member',42,100,'user',?,?,?)", (policy, text, when.isoformat()))
        self.provider.return_value = json.dumps({'action': 'post', 'text': TEXT, 'art': None})
        self.assertEqual(await bot.generate_dynamic_ambient(42, 100), TEXT)
        prompt = self.provider.call_args.args[0]
        self.assertIn('Fresh public rhythm', prompt)
        for excluded in ('OLD CLAIM', 'FUTURE CLAIM', 'PRIVATE CLAIM'):
            self.assertNotIn(excluded, prompt)
        self.assertIn(self.fixture.now.isoformat(), prompt)

    async def test_quiet_reflection_needs_no_room_activity_or_magic_words(self):
        self.execute('DELETE FROM conversations')
        self.provider.return_value = json.dumps({'action': 'post', 'text': TEXT, 'art': None})
        self.assertEqual(await bot.generate_dynamic_ambient(42, 100), TEXT)
        self.provider.assert_awaited_once()
        prompt = self.provider.call_args.args[0]
        self.assertNotIn('mode for this cycle', prompt)
        self.assertIn('Historical memories stay historical', prompt)

    async def test_silence_is_a_decision_and_costs_one_call(self):
        basis = {}
        self.provider.return_value = '{"action":"skip"}'
        self.assertEqual(await bot.generate_dynamic_ambient(42, 100, source_basis_out=basis), '')
        self.assertTrue(basis['declined'])
        self.provider.assert_awaited_once()

    async def test_total_repair_limit_is_two_even_for_incomplete_then_duplicate(self):
        self.provider.return_value = 'An unfinished thought and'
        self.assertEqual(await bot.generate_dynamic_ambient(42, 100), '')
        self.assertEqual(self.provider.await_count, 2)

    async def test_owner_label_normalized_at_source_read(self):
        self.execute("UPDATE conversations SET user_name='PRIVATE ACCOUNT LABEL'")
        with mock.patch.object(bot, 'BNL_OWNER_USER_ID', 7):
            self.assertEqual(await bot.generate_dynamic_ambient(42, 100), self.provider.return_value)
        prompt = self.provider.call_args.args[0]
        self.assertNotIn('PRIVATE ACCOUNT LABEL', prompt)
        self.assertIn('6 Bit', prompt)

    async def test_art_failure_preserves_standalone_text_and_consumes_daily_claim(self):
        with mock.patch.dict(os.environ, {'BNL_OWN_ART_ENABLED': 'true'}), mock.patch.object(art, 'journal_context', return_value=None):
            self.provider.return_value = json.dumps({'action': 'post', 'text': TEXT, 'art': CONCEPT})
            basis = {}
            self.assertEqual(await bot.generate_dynamic_ambient(42, 100, source_basis_out=basis), TEXT)
            with mock.patch.object(art, 'generate_private_image', side_effect=TimeoutError) as provider:
                self.assertIsNone(await art.prepare(bot, 42, basis))
                self.assertIsNone(await art.prepare(bot, 42, basis))
            provider.assert_called_once()
            self.assertFalse(art.available(bot, 42))
        self.assertEqual(self.execute('SELECT status FROM bnl_own_art_delivery')[0][0], 'generation_failed_or_withdrawn')

    async def test_source_withdrawal_during_image_generation_blocks_draft(self):
        with mock.patch.dict(os.environ, {'BNL_OWN_ART_ENABLED': 'true'}), mock.patch.object(art, 'journal_context', return_value=None):
            self.provider.return_value = json.dumps({'action': 'post', 'text': TEXT, 'art': CONCEPT})
            basis = {}
            await bot.generate_dynamic_ambient(42, 100, source_basis_out=basis)
            def image(*_):
                self.execute("UPDATE conversations SET channel_policy='sealed_test'")
                return b'png', {'sha256': hashlib.sha256(b'png').hexdigest()}
            with mock.patch.object(art, 'generate_private_image', side_effect=image):
                self.assertIsNone(await art.prepare(bot, 42, basis))
            self.assertFalse((Path(bot.DB_FILE).parent / 'bnl-own-art').exists())

    def test_daily_claim_is_atomic_restart_safe_and_pacific_not_utc(self):
        with mock.patch.dict(os.environ, {'BNL_OWN_ART_ENABLED': 'true'}):
            art.available(bot, 42)  # Prepare schema before concurrent workers.
            with ThreadPoolExecutor(max_workers=4) as pool:
                results = list(pool.map(lambda _: art.claim(bot, 42), range(8)))
            self.assertEqual([r for r in results if r], ['bnl-art-2026-09-11'])
            art.record(bot, results[0] or 'bnl-art-2026-09-11', 'discord_unconfirmed')
            self.assertIsNone(art.claim(bot, 42))
            self.assertIsNone(art.claim(bot, 99))
            self.fixture.now += timedelta(days=1)
            self.assertEqual(art.claim(bot, 42), 'bnl-art-2026-09-12')

    def test_default_off_and_invalid_optional_art_never_discard_text(self):
        self.assertFalse(art.available(bot, 42))
        self.assertEqual(self.execute("SELECT name FROM sqlite_master WHERE name='bnl_own_art_delivery'"), [])
        raw = json.dumps({'action': 'post', 'text': TEXT, 'art': {**CONCEPT, 'inspirationRefs': ['private:99']}})
        self.assertEqual(art.parse_response(raw), (TEXT, None, False))

    async def _run_art_scheduler_case(self, withdraw=False):
        self.execute("INSERT INTO guild_configs(guild_id,active_channel_id,next_ambient_message_at) VALUES(42,100,?)", ((self.fixture.now - timedelta(minutes=1)).isoformat(),))
        channel = mock.Mock(id=100, name='public-room')
        channel.send = mock.AsyncMock(return_value=mock.Mock(id=123456))
        self.provider.return_value = json.dumps({'action': 'post', 'text': TEXT, 'art': CONCEPT})
        png = b'private-test-image'
        with ExitStack() as stack:
            stack.enter_context(mock.patch.dict(os.environ, {'BNL_OWN_ART_ENABLED': 'true'}))
            stack.enter_context(mock.patch.object(art, 'journal_context', return_value=None))
            stack.enter_context(mock.patch.object(art, 'generate_private_image', return_value=(png, {'sha256': hashlib.sha256(png).hexdigest(), 'mimeType': 'image/jpeg'})))
            stack.enter_context(mock.patch.object(bot.client, 'get_channel', return_value=channel))
            stack.enter_context(mock.patch.object(bot, 'resolve_channel_policy', return_value='public_home'))
            stack.enter_context(mock.patch.object(bot, 'is_community_image_channel', return_value=False))
            stack.enter_context(mock.patch.object(bot, 'process_due_occasion_for_guild', new=mock.AsyncMock(return_value={'status':'idle'})))
            stack.enter_context(mock.patch.object(bot, 'prepare_dormant_echo_canary', new=mock.AsyncMock(return_value={'status':'idle'})))
            stack.enter_context(mock.patch.object(bot, 'get_last_ambient_posted_at', return_value=None))
            stack.enter_context(mock.patch.object(bot, 'ambient_capacity_decision', return_value={'allowed': True, 'capacityUsed': 0, 'cap': 1}))
            website = stack.enter_context(mock.patch.object(art, 'publish_website'))
            record = art.record
            def save(*args, **kwargs):
                if withdraw and args[2] == 'discord_delivery_reserved':
                    self.execute("UPDATE conversations SET channel_policy='sealed_test'")
                return record(*args, **kwargs)
            stack.enter_context(mock.patch.object(art, 'record', side_effect=save))
            await bot.ambient_message_task.coro()
            if withdraw:
                channel.send.assert_not_awaited()
                website.assert_not_called()
                self.assertEqual(self.execute('SELECT status FROM bnl_own_art_delivery')[0][0], 'withdrawn_before_delivery')
                return
            channel.send.assert_awaited_once()
            self.assertEqual(channel.send.call_args.args[0], TEXT)
            self.assertIsInstance(channel.send.call_args.kwargs['file'], bot.discord.File)
            self.assertTrue(channel.send.call_args.kwargs['file'].filename.endswith('.jpg'))
            self.assertEqual(website.call_count, 1)
            self.assertFalse(art.available(bot, 42))
        row = self.execute('SELECT status,discord_message_id,website_status FROM bnl_own_art_delivery')[0]
        self.assertEqual(row, ('discord_confirmed', '123456', ''))
        self.assertEqual(self.execute("SELECT COUNT(*) FROM ambient_log WHERE source_type='ambient'")[0][0], 1)

    def test_website_timeout_is_unconfirmed_and_does_not_resend(self):
        with mock.patch.dict(os.environ, {'BNL_OWN_ART_ENABLED': 'true'}):
            art_id = art.claim(bot, 42)
            with mock.patch.object(bot, '_journal_website_base_url', return_value='https://example.test'), mock.patch.object(art.urllib.request, 'build_opener') as opener:
                opener.return_value.open.side_effect = TimeoutError()
                art.publish_website(bot, {'image': b'png', 'metadata': {'artId': art_id}})
                opener.return_value.open.assert_called_once()
            self.assertEqual(self.execute('SELECT website_status FROM bnl_own_art_delivery')[0][0], 'unconfirmed')

    def test_website_upload_uses_actual_image_type_and_generic_payload(self):
        data = b'provider-jpeg-fixture'
        digest = hashlib.sha256(data).hexdigest()
        with mock.patch.dict(os.environ, {'BNL_OWN_ART_ENABLED': 'true'}):
            art_id = art.claim(bot, 42)
            response = mock.MagicMock()
            response.__enter__.return_value.read.return_value = json.dumps({'ok': True, 'artId': art_id, 'sha256': digest}).encode()
            with mock.patch.object(bot, '_journal_website_base_url', return_value='https://example.test'), mock.patch.object(art.urllib.request, 'build_opener') as opener:
                opener.return_value.open.return_value = response
                art.publish_website(bot, {'image': data, 'metadata': {'artId': art_id, 'sha256': digest, 'mimeType': 'image/jpeg'}})
                packet = json.loads(opener.return_value.open.call_args.args[0].data)
            self.assertEqual(packet['contractVersion'], 2)
            self.assertIn('imageBase64', packet)
            self.assertNotIn('pngBase64', packet)
            self.assertEqual(packet['art']['mimeType'], 'image/jpeg')
            self.assertEqual(self.execute('SELECT website_status FROM bnl_own_art_delivery')[0][0], 'confirmed')

    async def test_structured_ambient_uses_one_provider_call_and_preserves_json(self):
        from types import SimpleNamespace
        raw = json.dumps({'action': 'post', 'text': TEXT, 'art': CONCEPT})
        with mock.patch.object(bot, 'check_quota_availability', return_value=True), mock.patch.object(bot, '_generate_gemini_content_result_async', new=mock.AsyncMock(return_value=SimpleNamespace(success=True, text=raw))) as provider, mock.patch.object(bot, '_generate_gemini_content_with_fallback_async', new=mock.AsyncMock()) as rewrite:
            result = await REAL_GET('A self-directed ambient thought.', 0, 42, route='ambient_generation', ambient_envelope=True, raise_on_generation_failure=True)
        self.assertEqual(result, raw)
        provider.assert_awaited_once()
        rewrite.assert_not_awaited()

    async def test_scheduler_attaches_one_image_and_distinguishes_delivery(self):
        await self._run_art_scheduler_case()

    async def test_withdrawal_during_delivery_reservation_is_checked_before_send(self):
        await self._run_art_scheduler_case(withdraw=True)
