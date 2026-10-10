import asyncio
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import ast
import logging
from types import ModuleType, SimpleNamespace
from urllib import request, parse, error, response
from io import BytesIO
from email.message import Message
from bnl_gemini_routing import policy_for_route
from bnl_creative_protocol import SUNO_LYRIC_PROTOCOL
import bnl_song_workspace as songs
from bnl_song_workspace import SongContext, SongFailure, SongGeneration, ROUTE, execute_command, read_context, prepare_delivery

# Exercise the real runtime functions; this route has no Linux file-lock work.
# Importing the entire runtime on Windows requires unrelated POSIX dependencies.
source = Path(__file__).parents[1] / 'bnl01_bot.py'
tree = ast.parse(source.read_text(encoding='utf-8-sig'))
names = {'_song_control_request_sync', '_run_song_control_cycle', '_run_ballad_control_cycle', 'website_presence_heartbeat_task'}
nodes = [node for node in tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in names]
for node in nodes:
    node.decorator_list = []
bot = ModuleType('song_runtime_functions')
bot.__dict__.update(dict(asyncio=asyncio, json=json, logging=logging, urllib=SimpleNamespace(request=request, parse=parse),
    SONG_ROUTE=ROUTE, SongFailure=SongFailure, SongGeneration=SongGeneration,
    execute_song_command=execute_command, prepare_song_delivery=prepare_delivery, read_song_context=read_context,
    BNL01_PACKET_OWNED_SYSTEM_PROMPT='Shared BNL mind\n' + SUNO_LYRIC_PROTOCOL,
    SUNO_LYRIC_PROTOCOL=SUNO_LYRIC_PROTOCOL, SONG_BACKGROUND='BARCODE is music-first.',
    BNL_PRIMARY_GUILD_ID=77, DB_FILE='', BNL_API_KEY='key',
    BNL_WEBSITE_CONTRACT_VERSION='1', _ballad_cycle_task=None,
    GENERATION_ERROR_LOCAL_MODEL_BUDGET='local_model_budget_exhausted',
    _journal_website_base_url=lambda: 'https://www.barcode-network.com', check_quota_availability=lambda _: True,
    _generate_gemini_content_result_async=lambda *_: None))
if hasattr(songs, 'SongNoRedirect'):
    bot.SongNoRedirect = songs.SongNoRedirect
if any(node.name == '_run_song_control_cycle' for node in nodes):
    bot._song_cycle_task = None
exec(compile(ast.fix_missing_locations(ast.Module(body=nodes, type_ignores=[])), str(source), 'exec'), bot.__dict__)
bot.website_presence_heartbeat_task = SimpleNamespace(coro=bot.website_presence_heartbeat_task)
bot.GenerationResult = lambda success, text='', **kwargs: SimpleNamespace(success=success, text=text,
    error_category=kwargs.get('error_category', ''), finish_reason=kwargs.get('finish_reason', 'STOP'))


class SongControlCycleTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.command = dict(id='command-1', leaseId='lease-1', kind='generate', options={},
                            base=dict(title='', lyrics='', style=''),
                            limits=dict(maxLyricsWords=2000, targetSeconds=300))
        self.control = dict(contractVersion=1, commands=[self.command])
        self.output = dict(title='The Lantern', lyrics='[Chorus]\nCarry it home.', style='1983 chamber soul with dub bass.')
        self.patchers = [mock.patch.object(bot, 'DB_FILE', str(Path(self.tmp.name) / 'creative.db')),
                         mock.patch.object(bot, 'BNL_PRIMARY_GUILD_ID', 77)]
        for patcher in self.patchers:
            patcher.start()
            self.addCleanup(patcher.stop)

    async def test_delivery_failure_replays_exact_receipt_without_another_physical_call(self):
        self.assertTrue(hasattr(bot, '_run_song_control_cycle'), 'private song bridge cycle is missing')
        provider = mock.AsyncMock(return_value=bot.GenerationResult(True, json.dumps(self.output), finish_reason='STOP'))
        with mock.patch.object(bot, 'check_quota_availability', return_value=True), \
             mock.patch.object(bot, '_generate_gemini_content_result_async', provider), \
             mock.patch.object(bot, 'read_song_context', return_value=SongContext()), \
             mock.patch.object(bot, '_song_control_request_sync', side_effect=[self.control, OSError('private detail'), self.control, {'ok': True}]) as transport:
            await bot._run_song_control_cycle()
            await bot._run_song_control_cycle()
        self.assertEqual(provider.await_count, 1)
        self.assertEqual(provider.call_args.args[1], ROUTE)
        self.assertEqual(transport.call_args_list[1].args[1], transport.call_args_list[3].args[1])
        self.assertEqual(transport.call_args_list[3].args[1]['result'], self.output)
        self.assertIn('Shared BNL mind', provider.call_args.args[0])
        self.assertEqual(provider.call_args.args[0].count(SUNO_LYRIC_PROTOCOL), 1)
        self.assertIn('BARCODE is music-first', provider.call_args.args[0])

    async def test_budget_denial_prevents_provider_and_only_delivers_allowlisted_failure(self):
        self.assertTrue(hasattr(bot, '_run_song_control_cycle'), 'private song bridge cycle is missing')
        provider = mock.AsyncMock(side_effect=AssertionError('must not call provider'))
        with mock.patch.object(bot, 'check_quota_availability', return_value=False), \
             mock.patch.object(bot, '_generate_gemini_content_result_async', provider), \
             mock.patch.object(bot, 'read_song_context', return_value=SongContext()), \
             mock.patch.object(bot, '_song_control_request_sync', side_effect=[self.control, {'ok': True}]) as transport:
            await bot._run_song_control_cycle()
        self.assertEqual(provider.await_count, 0)
        self.assertEqual(transport.call_args_list[1].args[1], dict(commandId='command-1', leaseId='lease-1', outcome='failed', errorCode='BUDGET_UNAVAILABLE'))

    async def test_ballad_wait_does_not_starve_song_control_or_add_a_scheduler(self):
        self.assertTrue(hasattr(bot, '_song_cycle_task'), 'private song heartbeat integration is missing')
        gate = asyncio.Event()
        completed = asyncio.Event()
        async def ballad():
            await gate.wait()
        async def song():
            completed.set()
        with mock.patch.object(bot, '_ballad_cycle_task', None), mock.patch.object(bot, '_song_cycle_task', None), \
             mock.patch.object(bot, '_run_ballad_control_cycle', ballad), mock.patch.object(bot, '_run_song_control_cycle', song), \
             mock.patch.object(bot, 'BNL_WEBSITE_CONTRACT_VERSION', '1'):
            await bot.website_presence_heartbeat_task.coro()
            try:
                await asyncio.wait_for(completed.wait(), timeout=1)
                self.assertFalse(bot._ballad_cycle_task.done())
            finally:
                gate.set()
                await asyncio.gather(bot._ballad_cycle_task, bot._song_cycle_task)

    def test_authentication_key_cannot_follow_a_redirect_to_another_origin(self):
        seen = []
        class RedirectingWebsite(request.HTTPSHandler):
            def https_open(self, req):
                seen.append(req)
                headers = Message()
                if len(seen) == 1:
                    headers['Location'] = 'https://untrusted.invalid/private'
                    reply = response.addinfourl(BytesIO(b''), headers, req.full_url, code=302)
                    reply.msg = 'Found'
                    return reply
                reply = response.addinfourl(BytesIO(b'{}'), headers, req.full_url, code=200)
                reply.msg = 'OK'
                return reply
        real_build_opener = request.build_opener
        def opener(*handlers):
            return real_build_opener(*handlers, RedirectingWebsite())
        with mock.patch.object(request, '_opener', None), mock.patch.object(request, 'build_opener', side_effect=opener):
            with self.assertRaises(error.HTTPError):
                bot._song_control_request_sync()
        self.assertEqual(len(seen), 1)
        self.assertEqual(seen[0].get_header('X-api-key'), 'key')

    def test_bridge_body_limit_counts_bytes_and_bounds_response_read(self):
        seen = []
        class OversizedWebsite(request.HTTPSHandler):
            def https_open(self, req):
                seen.append((req.full_url, req.timeout))
                reply = response.addinfourl(BytesIO(b'x' * 262145), Message(), req.full_url, code=200)
                reply.msg = 'OK'
                return reply
        real_build_opener = request.build_opener
        with mock.patch.object(request, 'build_opener', side_effect=lambda *handlers: real_build_opener(*handlers, OversizedWebsite())):
            with self.assertRaises(ValueError):
                bot._song_control_request_sync()
            with self.assertRaises(ValueError):
                bot._song_control_request_sync('POST', {'result': 'large' * 60000})
        self.assertEqual(seen, [('https://www.barcode-network.com/api/bnl/songs', 10)])

    def test_song_transport_refuses_unapproved_origin_before_sending_the_key(self):
        with mock.patch.object(bot, '_journal_website_base_url', return_value='https://untrusted.invalid'), mock.patch.object(request, 'urlopen', return_value=BytesIO(b'{}')):
            self.assertIsNone(bot._song_control_request_sync())

    def test_song_uses_existing_manual_priority_and_single_bounded_provider_attempt(self):
        policy = policy_for_route(ROUTE)
        self.assertEqual(policy.lane, 'conversation')
        self.assertEqual(policy.provider_retries, 0)
        self.assertFalse(policy.allow_fallback)
        self.assertGreaterEqual(policy.max_output_tokens, 16000)
        self.assertLessEqual(policy.max_output_tokens, 16384)
        self.assertFalse(policy.journal_protected)
        self.assertFalse(policy.showday_protected)


if __name__ == '__main__':
    unittest.main()
