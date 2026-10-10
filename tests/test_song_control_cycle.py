import asyncio
import sys
import __future__
import re
import time
from collections import Counter
from dataclasses import dataclass, replace
import hashlib
import sqlite3
from contextlib import closing
import json
import os
import tempfile
import unittest
from pathlib import Path
from datetime import datetime, timezone, timedelta
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
from bnl_memory_ledger import subject_key_for_user
from bnl_canon_source_contract import CANON_ENTITY_IDENTITIES
from bnl_conversation_context_v2 import STOPWORDS as CONVERSATION_CONTEXT_STOPWORDS, sanitize_history_text
from bnl_tiktok_live_context import (PUBLIC_MEMBER_RECALL_REQUEST_WORDS, requested_show_date,
    requested_history_window, has_explicit_show_date, strip_explicit_show_dates)
from bnl_unified_response_assessment import (SituationFrameV1, situation_subject_label_spans,
    build_situation_frame_v1, build_conversation_evidence_item)
import pytz
from bnl_song_workspace import SongContext, SongFailure, SongGeneration, ROUTE, execute_command, read_context, prepare_delivery

# Reuse the pure recall/ranking code without importing unrelated journal locks.
governance_source = Path(__file__).parents[1] / 'bnl_memory_governance.py'
governance_tree = ast.parse(governance_source.read_text(encoding='utf-8-sig'))
governance_names = {'PersonalRecallIntent', 'normalize_personal_recall_intent',
    'classify_personal_recall_intent', 'memory_relevance_terms', 'PERSONAL_RECALL_ROUTE_FAMILY',
    '_PERSONAL_RECALL_HOLD_RE', '_PROFILE_SCOPE_SUFFIX', '_BROAD_SELF_PROFILE_PATTERNS', '_AMBIGUOUS_RECALL_PATTERNS'}
governance_nodes = [node for node in governance_tree.body
    if isinstance(node, (ast.FunctionDef, ast.ClassDef)) and node.name in governance_names
    or isinstance(node, ast.Assign) and any(isinstance(target, ast.Name) and target.id in governance_names for target in node.targets)]
governance = {'re': re, 'dataclass': dataclass}
exec(compile(ast.fix_missing_locations(ast.Module(body=governance_nodes, type_ignores=[])), str(governance_source), 'exec',
    flags=__future__.annotations.compiler_flag), governance)
classify_personal_recall_intent = governance['classify_personal_recall_intent']
memory_relevance_terms = governance['memory_relevance_terms']

# Exercise the real runtime functions; this route has no Linux file-lock work.
# Importing the entire runtime on Windows requires unrelated POSIX dependencies.
source = Path(__file__).parents[1] / 'bnl01_bot.py'
tree = ast.parse(source.read_text(encoding='utf-8-sig'))
names = {'_song_named_public_subjects', '_song_named_public_chat_is_current', '_read_song_public_context', '_song_public_context_is_current', '_song_control_request_sync', '_run_song_control_cycle', '_run_ballad_control_cycle', 'website_presence_heartbeat_task'}
nodes = [node for node in tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in names]
for node in nodes:
    node.decorator_list = []
bot = ModuleType('song_runtime_functions')
bot.__dict__.update(dict(asyncio=asyncio, json=json, logging=logging, urllib=SimpleNamespace(request=request, parse=parse),
    Counter=Counter, re=re, sqlite3=sqlite3, subject_key_for_user=subject_key_for_user, SONG_ROUTE=ROUTE, SongFailure=SongFailure, SongGeneration=SongGeneration, SongContext=SongContext,
    datetime=datetime, timezone=timezone, song_context_is_current=songs.context_is_current,
    song_retrieval_query=songs.retrieval_query, client=SimpleNamespace(get_guild=lambda _: None),
    _named_public_member_subjects=lambda *a,**k: ((),()),
    _prompt_source_digest=lambda value: hashlib.sha256(value.encode()).hexdigest(),
    build_situation_frame_v1=build_situation_frame_v1,
    build_named_public_conversation_context=lambda **k: ('', None),
    build_conversation_evidence_item=build_conversation_evidence_item,
    get_recent_guild_user_messages=lambda *a,**k: [], revalidate_ambient_local_sources=lambda *a,**k: True,
    should_exclude_from_prompt_history=lambda *a: False, sanitize_history_text=lambda text,**k: text,
    _safe_prompt_display_label=lambda text,fallback: text or fallback,
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


# Keep source owners real in these regressions without importing unrelated bot
# startup and POSIX locks. Native SQLite executes every original/control query.
owner_names = {'_ambient_source_rows', '_ambient_source_hash', '_remember_ambient_sources',
    '_merge_ambient_source_hashes', 'revalidate_ambient_local_sources',
    'get_recent_guild_user_messages', '_public_conversation_recall_controls', '_prompt_source_digest'}
owner_nodes = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in owner_names]
public_sources = ModuleType('song_public_source_owners')
public_sources.__dict__.update(dict(sqlite3=sqlite3, closing=closing, json=json, hashlib=hashlib,
    datetime=datetime, timezone=timezone, timedelta=timedelta, subject_key_for_user=subject_key_for_user,
    _AMBIENT_SOURCE_FIELDS={'conversations': 'id,user_id,user_name,content,role,channel_id,channel_policy,timestamp'},
    BNL_OWNER_USER_ID=0, DB_FILE='', AMBIENT_CONTEXT_MESSAGES=24,
    PUBLIC_CHAT_POLICIES={'public_home', 'public_context', 'public_selective'},
    _pacific_now=lambda: datetime.now(timezone.utc)))
exec(compile(ast.fix_missing_locations(ast.Module(body=owner_nodes, type_ignores=[])), str(source), 'exec',
    flags=__future__.annotations.compiler_flag), public_sources.__dict__)


# The same named resolver, original reader and source refresher as the runtime.
# Unrelated basis types only satisfy isinstance dispatch; no reader is stubbed.
named_names = {'_named_public_member_subjects', '_safe_prompt_display_label', '_named_public_recall_scope',
    'build_named_public_conversation_context', '_memory_query_relevance', '_conversation_prompt_row_snapshot',
    '_conversation_prompt_selected_digest', '_conversation_prompt_basis_digest', '_conversations_columns',
    '_open_member_memory_read_connection', 'refresh_prompt_source_basis'}
named_nodes = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in named_names
    or isinstance(node, ast.ClassDef) and node.name == 'ConversationPromptSourceBasis']
named_sources = ModuleType('song_named_public_source_owners')
named_sources.__dict__.update(public_sources.__dict__)
named_sources.__dict__['__name__'] = 'song_named_public_source_owners'
sys.modules[named_sources.__name__] = named_sources
named_sources.__dict__.update(dict(dataclass=dataclass, replace=replace, re=re, Counter=Counter,
    Path=Path, time=time, logging=logging, SituationFrameV1=SituationFrameV1,
    build_situation_frame_v1=build_situation_frame_v1, build_conversation_evidence_item=build_conversation_evidence_item,
    situation_subject_label_spans=situation_subject_label_spans, CANON_ENTITY_IDENTITIES=CANON_ENTITY_IDENTITIES,
    classify_personal_recall_intent=classify_personal_recall_intent, memory_relevance_terms=memory_relevance_terms,
    CONVERSATION_CONTEXT_STOPWORDS=CONVERSATION_CONTEXT_STOPWORDS,
    PUBLIC_MEMBER_RECALL_REQUEST_WORDS=PUBLIC_MEMBER_RECALL_REQUEST_WORDS,
    strip_explicit_show_dates=strip_explicit_show_dates, requested_show_date=requested_show_date,
    requested_history_window=requested_history_window, has_explicit_show_date=has_explicit_show_date,
    ROUTE_MODE_NORMAL_CHAT='normal_chat', PACIFIC_TZ=pytz.timezone('America/Los_Angeles'),
    MEMORY_PROMPT_BUDGET_PUBLIC=900, CONVERSATION_ROWS_PER_USER_MAX=1200,
    should_exclude_from_prompt_history=bot.should_exclude_from_prompt_history,
    sanitize_history_text=sanitize_history_text,
    _PROMPT_CONTROL_LABEL_RE=re.compile(r'\b(?:ignore|disregard|override|reveal|system|developer|assistant|prompt|instructions?)\b', re.I)))
for unused in ('FinalizedShowPromptSourceBasis', 'PublicationPromptSourceBasis', 'SharedBrainSynthesisBasis',
               'UnifiedMomentCanaryPromptSourceBasis', 'MemoryPromptSourceBasis', 'BatchMomentPromptSourceBasis'):
    named_sources.__dict__[unused] = type(unused, (), {})
exec(compile(ast.fix_missing_locations(ast.Module(body=named_nodes, type_ignores=[])), str(source), 'exec',
    flags=__future__.annotations.compiler_flag), named_sources.__dict__)
bot.ConversationPromptSourceBasis = named_sources.ConversationPromptSourceBasis


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

    async def test_recent_public_conversation_reaches_song_and_withdrawal_blocks_delivery(self):
        provider = mock.AsyncMock(return_value=bot.GenerationResult(True, json.dumps(self.output), finish_reason='STOP'))
        visible = [True]
        def recent(guild_id, limit, *, source_basis, dated, require_current_originals):
            self.assertTrue(require_current_originals)
            source_basis['rows'] = {'conversations': {12: 'public-root-version'}}
            source_basis['recent_conversation_ids'] = (12,)
            return [('6 Bit', 'Paper lanterns kept the community smiling through the outage.')]
        def current(guild_id, basis, *, require_current_originals):
            self.assertTrue(require_current_originals)
            self.assertEqual(set(basis['rows']['conversations']), {12})
            return visible[0]
        with mock.patch.object(bot, 'read_song_context', return_value=SongContext('Public Moment: lanterns.')), \
             mock.patch.object(bot, 'get_recent_guild_user_messages', recent, create=True), \
             mock.patch.object(bot, 'revalidate_ambient_local_sources', current, create=True), \
             mock.patch.object(bot, '_generate_gemini_content_result_async', provider), \
             mock.patch.object(bot, '_song_control_request_sync', side_effect=[self.control, {'ok': True}, self.control, {'ok': True}]) as transport:
            await bot._run_song_control_cycle()
            self.assertEqual(provider.await_count, 1)
            self.assertIn('6 Bit', provider.call_args.args[0])
            self.assertTrue('Paper lanterns kept the community smiling' in provider.call_args.args[0])
            self.assertNotIn('public-root-version', provider.call_args.args[0])
            visible[0] = False
            await bot._run_song_control_cycle()
        self.assertEqual(provider.await_count, 1)
        self.assertEqual(transport.call_args_list[3].args[1].get('errorCode'), 'CONTEXT_UNAVAILABLE')
        self.assertNotIn('result', transport.call_args_list[3].args[1])

    def test_recent_chat_basis_rejects_other_guild_private_tables_and_unknown_fields(self):
        good = {'sourceKind': 'recent_public_chat', 'basis': {'guild_id': 77, 'ambient_source_window_end': '2026-10-10T00:00:00+00:00', 'rows': {'conversations': {'12': 'digest'}}, 'recent_conversation_ids': [12]}}
        with mock.patch.object(bot, 'song_context_is_current', return_value=True, create=True), \
             mock.patch.object(bot, 'revalidate_ambient_local_sources', return_value=True, create=True):
            self.assertTrue(bot._song_public_context_is_current(SongContext('', (good,))))
            for bad in [dict(good['basis'], guild_id=78), dict(good['basis'], rows={'memory_tiers': {'12': 'digest'}}), dict(good['basis'], tier_sources={})]:
                self.assertFalse(bot._song_public_context_is_current(SongContext('', ({'sourceKind': 'recent_public_chat', 'basis': bad},))))

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


class SongPublicOriginalOwnerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db = str(Path(self.tmp.name) / 'public-song-fixture.db')
        self.text = 'The paper lantern went sideways, and Pat laughed with the room.'
        with closing(sqlite3.connect(self.db)) as conn, conn:
            conn.executescript("""
                CREATE TABLE conversations(id INTEGER PRIMARY KEY,user_id INTEGER,user_name TEXT,
                    content TEXT,role TEXT,channel_id INTEGER,channel_policy TEXT,timestamp TEXT,guild_id INTEGER);
                CREATE TABLE memory_ledger_entries(entry_id TEXT,guild_id INTEGER,source_table TEXT,
                    source_row_id TEXT,subject_key TEXT,lifecycle_status TEXT,public_usable INTEGER,entry_type TEXT);
                CREATE TABLE memory_ledger_lineage(entry_id TEXT,guild_id INTEGER,target_entry_id TEXT,lineage_type TEXT);
            """)
            conn.execute('INSERT INTO conversations VALUES (12,43,?, ?,?,?,?, ?,77)',
                ('Pat', self.text, 'user', 9001, 'public_home', datetime.now(timezone.utc).isoformat()))
            conn.execute("INSERT INTO memory_ledger_entries VALUES ('root-12',77,'conversations','12','discord_user:43','active',1,'observation')")
        self.command = dict(id='source-song-1', leaseId='lease-1', kind='generate', options={},
            base=dict(title='', lyrics='', style=''), limits=dict(maxLyricsWords=2000, targetSeconds=300))
        self.control = dict(contractVersion=1, commands=[self.command])
        self.output = dict(title='Lantern', lyrics='[Chorus]\nCarry the paper light.', style='Chamber soul.')
        for patcher in (mock.patch.object(bot, 'DB_FILE', self.db),
                        mock.patch.object(public_sources, 'DB_FILE', self.db),
                        mock.patch.object(bot, 'read_song_context', return_value=SongContext()),
                        mock.patch.object(bot, 'get_recent_guild_user_messages', public_sources.get_recent_guild_user_messages),
                        mock.patch.object(bot, 'revalidate_ambient_local_sources', public_sources.revalidate_ambient_local_sources)):
            patcher.start()
            self.addCleanup(patcher.stop)

    def change(self, sql, values=()):
        with closing(sqlite3.connect(self.db)) as conn, conn:
            conn.execute(sql, values)

    def read(self):
        return bot._read_song_public_context({})

    def test_public_flag_and_nonactive_lifecycle_withdraw_original_from_song(self):
        for sql, values in (
            ('UPDATE memory_ledger_entries SET public_usable=?', (0,)),
            ('UPDATE memory_ledger_entries SET lifecycle_status=?', ('quarantined',)),
            ('UPDATE memory_ledger_entries SET lifecycle_status=?', ('unknown_lifecycle',)),
            ('UPDATE memory_ledger_entries SET lifecycle_status=?', ('superseded',)),
        ):
            with self.subTest(values=values):
                self.change("UPDATE memory_ledger_entries SET public_usable=1,lifecycle_status='active'")
                context = self.read()
                self.assertIn(self.text, context.text)
                self.assertTrue(bot._song_public_context_is_current(context))
                self.change(sql, values)
                self.assertFalse(bot._song_public_context_is_current(context))
                self.assertNotIn(self.text, self.read().text)

    def test_incoming_correction_withdraws_original_and_json_receipt_basis(self):
        context = self.read()
        serialized = SongContext('', tuple(json.loads(json.dumps(context.basis))))
        self.assertTrue(bot._song_public_context_is_current(serialized))
        self.change("INSERT INTO memory_ledger_lineage VALUES ('correction-13',77,'root-12','correction_of')")
        self.assertFalse(bot._song_public_context_is_current(serialized))
        self.assertNotIn(self.text, self.read().text)

    def test_original_deletion_or_changed_public_scope_invalidates_saved_basis(self):
        for sql, values in (
            ('UPDATE conversations SET channel_policy=?', ('sealed_test',)),
            ('UPDATE conversations SET role=?', ('model',)),
            ('UPDATE conversations SET content=?', ('A corrected public original.',)),
            ('DELETE FROM conversations WHERE id=?', (12,)),
        ):
            with self.subTest(values=values):
                context = self.read()
                self.assertTrue(bot._song_public_context_is_current(context))
                self.change(sql, values)
                self.assertFalse(bot._song_public_context_is_current(context))
                if 'DELETE' not in sql:
                    self.change("UPDATE conversations SET channel_policy='public_home',role='user',content=?", (self.text,))

    def test_existing_recent_reader_and_revalidation_defaults_stay_unchanged(self):
        basis = {'guild_id': 77, 'ambient_source_window_end': datetime.now(timezone.utc).isoformat()}
        self.assertIn(('Pat', self.text), public_sources.get_recent_guild_user_messages(77, source_basis=basis))
        self.change("UPDATE memory_ledger_entries SET public_usable=0,lifecycle_status='quarantined'")
        self.assertIn(('Pat', self.text), public_sources.get_recent_guild_user_messages(77))
        self.assertTrue(public_sources.revalidate_ambient_local_sources(77, basis))
        self.assertNotIn(self.text, self.read().text)

    def test_other_guild_control_does_not_withdraw_this_original(self):
        context = self.read()
        self.change("INSERT INTO memory_ledger_entries VALUES ('other-guild',78,'conversations','12','discord_user:43','quarantined',0,'observation')")
        self.assertTrue(bot._song_public_context_is_current(context))
        self.assertIn(self.text, self.read().text)

    def test_wrong_subject_control_keeps_legacy_isolation_and_strict_source_withdrawal(self):
        context = self.read()
        self.change("UPDATE memory_ledger_entries SET subject_key='discord_user:44',public_usable=0")
        # Existing named/Ambient recall still uses its author-scoped control.
        self.assertIn(('Pat', self.text), public_sources.get_recent_guild_user_messages(77))
        # Strict show source governance conservatively withdraws the referenced
        # original instead of promoting a malformed control into public evidence.
        self.assertFalse(bot._song_public_context_is_current(context))
        self.assertNotIn(self.text, self.read().text)

    def test_strict_source_control_query_is_readonly_and_incomplete_schema_fails_closed(self):
        statements = []
        real_connect = sqlite3.connect
        def readonly_connect(database, **kwargs):
            self.assertTrue(str(database).endswith('?mode=ro'))
            self.assertTrue(kwargs.get('uri'))
            conn = real_connect(database, **kwargs)
            conn.set_trace_callback(statements.append)
            return conn
        with mock.patch.object(public_sources, 'sqlite3', SimpleNamespace(connect=readonly_connect,
                Error=sqlite3.Error, DatabaseError=sqlite3.DatabaseError)):
            context = self.read()
            self.assertTrue(bot._song_public_context_is_current(context))
        self.assertFalse(any(statement.lstrip().upper().startswith(('INSERT', 'UPDATE', 'DELETE', 'CREATE')) for statement in statements))
        self.change('DROP TABLE memory_ledger_lineage')
        self.assertFalse(bot._song_public_context_is_current(context))

    async def test_privacy_withdrawal_after_model_blocks_result_delivery(self):
        async def generate(*_):
            self.change('UPDATE memory_ledger_entries SET public_usable=0')
            return bot.GenerationResult(True, json.dumps(self.output))
        provider = mock.AsyncMock(side_effect=generate)
        with mock.patch.object(bot, '_generate_gemini_content_result_async', provider), \
             mock.patch.object(bot, '_song_control_request_sync', side_effect=[self.control, {'ok': True}]) as transport:
            await bot._run_song_control_cycle()
        self.assertEqual(provider.await_count, 1)
        self.assertTrue(self.text in provider.call_args.args[0], 'selected original did not reach the writer')
        self.assertEqual(transport.call_args_list[1].args[1].get('errorCode'), 'CONTEXT_UNAVAILABLE')
        self.assertNotIn('result', transport.call_args_list[1].args[1])

    async def test_privacy_withdrawal_blocks_exact_receipt_replay_without_regeneration(self):
        provider = mock.AsyncMock(return_value=bot.GenerationResult(True, json.dumps(self.output)))
        with mock.patch.object(bot, '_generate_gemini_content_result_async', provider), \
             mock.patch.object(bot, '_song_control_request_sync', side_effect=[self.control, OSError('failed delivery'), self.control, {'ok': True}]) as transport:
            await bot._run_song_control_cycle()
            self.assertEqual(provider.await_count, 1)
            self.assertIn('result', transport.call_args_list[1].args[1])
            self.change("UPDATE memory_ledger_entries SET lifecycle_status='quarantined'")
            await bot._run_song_control_cycle()
        self.assertEqual(provider.await_count, 1)
        self.assertEqual(transport.call_args_list[3].args[1].get('errorCode'), 'CONTEXT_UNAVAILABLE')
        self.assertNotIn('result', transport.call_args_list[3].args[1])


class SongNamedPublicOriginalTests(unittest.IsolatedAsyncioTestCase):
    change = SongPublicOriginalOwnerTests.change
    read = SongPublicOriginalOwnerTests.read

    def setUp(self):
        SongPublicOriginalOwnerTests.setUp(self)
        self.text = 'I found coordinates to Supply Closet B. Get the homies out before Panda Bit becomes patient zero.'
        with closing(sqlite3.connect(self.db)) as conn, conn:
            conn.execute('ALTER TABLE conversations ADD COLUMN channel_name TEXT')
            conn.execute('ALTER TABLE conversations ADD COLUMN message_id INTEGER')
            conn.execute("UPDATE conversations SET user_name='Chris',content=?,timestamp=?,channel_name='public-stage',message_id=9012",
                (self.text, (datetime.now(timezone.utc) - timedelta(days=3)).isoformat()))
        self.member = SimpleNamespace(id=43, display_name='Chris', global_name='Chris', name='Chris', bot=False)
        self.guild = SimpleNamespace(id=77, members=[self.member])
        self.command['options'] = {'idea': 'Song about Chris', 'musicalDirection': 'rock', 'mood': 'Angry'}
        for patcher in (mock.patch.object(named_sources, 'DB_FILE', self.db),
                       mock.patch.object(bot, 'client', SimpleNamespace(get_guild=lambda _: self.guild)),
                       mock.patch.object(bot, '_named_public_member_subjects', named_sources._named_public_member_subjects),
                       mock.patch.object(bot, 'build_named_public_conversation_context', named_sources.build_named_public_conversation_context),
                       mock.patch.object(bot, 'refresh_prompt_source_basis', named_sources.refresh_prompt_source_basis, create=True)):
            patcher.start()
            self.addCleanup(patcher.stop)

    async def test_older_named_member_original_reaches_writer_without_moments_or_recent_chat(self):
        provider = mock.AsyncMock(return_value=bot.GenerationResult(True, json.dumps(self.output)))
        with mock.patch.object(bot, '_generate_gemini_content_result_async', provider), \
             mock.patch.object(bot, '_song_control_request_sync', side_effect=[self.control, {'ok': True}]) as transport, \
             mock.patch.object(bot, 'read_song_context', return_value=SongContext()) as module_reader:
            await bot._run_song_control_cycle()
        self.assertEqual(provider.await_count, 1)
        self.assertTrue(self.text in provider.call_args.args[0], 'selected original did not reach the writer')
        self.assertNotIn('discord_user:43', provider.call_args.args[0])
        self.assertEqual(module_reader.call_args.kwargs['public_subjects'], (('discord_user:43', 'Chris'),))
        self.assertIn('result', transport.call_args_list[1].args[1])

    async def test_song_named_history_keeps_whole_quotes_and_callbacks_beyond_chat_budget(self):
        originals = (
            'I called this the midnight train: send the unfinished track and let the room hear where it is going. '
            'I want to hear that chorus again, with the guitar up front and the little bell still at the end.',
            'The midnight train is back. My exact advice was, "Keep the odd little melody; it gives the song its own face." '
            + ('I listened for the way the rhythm changed after the quiet opening, how the bass answered the guitar, '
              'and whether the last chorus made the earlier rough idea feel complete. ' * 7).rstrip(),
            'That little bell is our midnight train callback again. Send the finished version when it is ready; '
            'I will listen to the whole thing and share the link with the room.',
        )
        self.assertGreater(len(originals[1]), 900)
        with closing(sqlite3.connect(self.db)) as conn, conn:
            stamp = datetime.fromisoformat(conn.execute('SELECT timestamp FROM conversations WHERE id=12').fetchone()[0])
            conn.execute('UPDATE conversations SET content=? WHERE id=12', (originals[0],))
            for row_id, original in ((13, originals[1]), (14, originals[2])):
                conn.execute("INSERT INTO conversations VALUES (?,43,'Chris',?,'user',9001,'public_home',?,77,'public-stage',?)",
                    (row_id, original, (stamp + timedelta(seconds=(row_id - 12) * 30)).isoformat(), 9000 + row_id))
                conn.execute("INSERT INTO memory_ledger_entries VALUES (?,77,'conversations',?,'discord_user:43','active',1,'observation')",
                    ('root-' + str(row_id), str(row_id)))
            for row_id, marker, policy, guild, role in (
                (15, 'PRIVATE DRAFT MARKER', 'sealed_test', 77, 'user'),
                (16, 'OTHER GUILD MARKER', 'public_home', 78, 'user'),
                (17, 'WITHDRAWN ORIGINAL MARKER', 'public_home', 77, 'user'),
                (18, 'PRIOR MODEL MARKER', 'public_home', 77, 'model'),
            ):
                conn.execute("INSERT INTO conversations VALUES (?,43,'Chris',?,?,9001,?,?,?,'public-stage',?)",
                    (row_id, marker, role, policy, (stamp + timedelta(seconds=row_id)).isoformat(), guild, 9000 + row_id))
                conn.execute("INSERT INTO memory_ledger_entries VALUES (?,?,'conversations',?,'discord_user:43','active',?,'observation')",
                    ('root-' + str(row_id), guild, str(row_id), 0 if row_id == 17 else 1))
        frame = build_situation_frame_v1(route_allowed=True, route_mode='normal_chat',
            conversation_surface='discord', channel_policy='public_home', current_text='Song about Chris',
            subject_user_ids=(43,), subject_label_hints=('Chris',))
        chat, _basis = named_sources.build_named_public_conversation_context(
            situation_frame=frame, guild_id=77, route_mode='normal_chat', channel_policy='public_home',
            user_text='Song about Chris', require_current_originals=True)
        self.assertLessEqual(len(chat), 900)
        self.assertNotIn(originals[1], chat)
        provider = mock.AsyncMock(return_value=bot.GenerationResult(True, json.dumps(self.output)))
        with mock.patch.object(bot, '_generate_gemini_content_result_async', provider), \
             mock.patch.object(bot, '_song_control_request_sync', side_effect=[self.control, {'ok': True}]) as transport:
            await bot._run_song_control_cycle()
        self.assertEqual(provider.await_count, 1)
        prompt = provider.call_args.args[0]
        for original in originals:
            self.assertTrue(original in prompt, 'complete named public original did not reach song writer')
        self.assertLess(prompt.index(originals[0]), prompt.index(originals[1]))
        self.assertLess(prompt.index(originals[1]), prompt.index(originals[2]))
        self.assertIn('Chris in #public-stage', prompt)
        for marker in ('PRIVATE DRAFT MARKER', 'OTHER GUILD MARKER', 'WITHDRAWN ORIGINAL MARKER', 'PRIOR MODEL MARKER'):
            self.assertNotIn(marker, prompt)
        self.assertIn('result', transport.call_args_list[1].args[1])
        context = bot._read_song_public_context(self.command['options'])
        named = next(ref for ref in context.basis if ref['sourceKind'] == 'named_public_chat')
        self.assertEqual(named['sourceBasis']['sourceRowIds'], (12, 13, 14))
        self.assertLessEqual(len(named['sourceBasis']['sourceRowIds']), 64)
        serialized = SongContext('', tuple(json.loads(json.dumps(context.basis))))
        self.assertTrue(bot._song_public_context_is_current(serialized))
        self.change('UPDATE memory_ledger_entries SET public_usable=0 WHERE entry_id=?', ('root-13',))
        self.assertFalse(bot._song_public_context_is_current(serialized))
        self.assertNotIn(originals[1], bot._read_song_public_context(self.command['options']).text)

    def test_expanded_song_history_keeps_existing_64_original_limit_and_json_revalidation(self):
        with closing(sqlite3.connect(self.db)) as conn, conn:
            stamp = datetime.fromisoformat(conn.execute('SELECT timestamp FROM conversations WHERE id=12').fetchone()[0])
            for row_id in range(13, 93):
                conn.execute("INSERT INTO conversations VALUES (?,43,'Chris',?,'user',9001,'public_home',?,77,'public-stage',?)",
                    (row_id, 'The little bell returns, take ' + str(row_id) + '.',
                     (stamp + timedelta(seconds=row_id)).isoformat(), 9000 + row_id))
                conn.execute("INSERT INTO memory_ledger_entries VALUES (?,77,'conversations',?,'discord_user:43','active',1,'observation')",
                    ('root-' + str(row_id), str(row_id)))
        context = bot._read_song_public_context(self.command['options'])
        named = next(ref for ref in context.basis if ref['sourceKind'] == 'named_public_chat')
        self.assertEqual(len(named['sourceBasis']['sourceRowIds']), 64)
        self.assertEqual(len(named['sourceBasis']['sourceUsers']), 64)
        self.assertTrue(bot._song_public_context_is_current(SongContext('', tuple(json.loads(json.dumps(context.basis))))))

    def test_named_reader_explicit_limits_are_bounded_and_ordinary_budget_is_unchanged(self):
        frame = build_situation_frame_v1(route_allowed=True, route_mode='normal_chat',
            conversation_surface='discord', channel_policy='public_home', current_text='Song about Chris',
            subject_user_ids=(43,), subject_label_hints=('Chris',))
        arguments = dict(situation_frame=frame, guild_id=77, route_mode='normal_chat',
            channel_policy='public_home', user_text='Song about Chris', require_current_originals=True)
        default, _ = named_sources.build_named_public_conversation_context(**arguments)
        explicit, _ = named_sources.build_named_public_conversation_context(**arguments,
            character_budget=900, source_row_limit=64)
        self.assertEqual(default, explicit)
        self.assertLessEqual(len(default), 900)
        for key, value in (('character_budget', -1), ('character_budget', 8001),
                           ('character_budget', True), ('source_row_limit', 0),
                           ('source_row_limit', 65), ('source_row_limit', '64')):
            with self.subTest(key=key, value=value), self.assertRaises(ValueError):
                named_sources.build_named_public_conversation_context(**arguments, **{key: value})

    async def test_song_request_medium_does_not_displace_newer_public_callback_from_bounded_history(self):
        callback = 'I left a tiny brass bell by the station; that is the midnight train joke from yesterday.'
        generic = 'I heard another song in the room today, take '
        with closing(sqlite3.connect(self.db)) as conn, conn:
            stamp = datetime.fromisoformat(conn.execute('SELECT timestamp FROM conversations WHERE id=12').fetchone()[0])
            # Output-medium matches can exhaust the reader's existing 1,200
            # SQL candidate bound before its final content/recency ranking.
            for row_id in range(13, 1214):
                original = callback if row_id == 1213 else generic + str(row_id) + '.'
                conn.execute("INSERT INTO conversations VALUES (?,43,'Chris',?,'user',9001,'public_home',?,77,'public-stage',?)",
                    (row_id, original, (stamp + timedelta(seconds=7200 if row_id == 1213 else row_id)).isoformat(), 9000 + row_id))
                conn.execute("INSERT INTO memory_ledger_entries VALUES (?,77,'conversations',?,'discord_user:43','active',1,'observation')",
                    ('root-' + str(row_id), str(row_id)))
        provider = mock.AsyncMock(return_value=bot.GenerationResult(True, json.dumps(self.output)))
        with mock.patch.object(bot, '_generate_gemini_content_result_async', provider), \
             mock.patch.object(bot, '_song_control_request_sync', side_effect=[self.control, {'ok': True}]) as transport:
            await bot._run_song_control_cycle()
        self.assertEqual(provider.await_count, 1)
        self.assertTrue(callback in provider.call_args.args[0], 'output-medium ranking crowded out newer public callback')
        self.assertTrue('"idea": "Song about Chris"' in provider.call_args.args[0], 'writer direction was rewritten')
        self.assertIn('result', transport.call_args_list[1].args[1])
        with mock.patch.object(bot, 'build_named_public_conversation_context', wraps=named_sources.build_named_public_conversation_context) as reader:
            for idea, expected, broad in (
                ('Song about Chris', 'Chris', True),
                ('write a song about Chris', 'Chris', True),
                ("Chris's song", "Chris's song", False),
                ("Song about Chris's song", "Chris's song", False),
            ):
                with self.subTest(idea=idea):
                    context = await asyncio.to_thread(bot._read_song_public_context, {'idea': idea})
                    self.assertEqual(reader.call_args.kwargs['user_text'], expected)
                    self.assertEqual(callback in context.text, broad)
                    self.assertTrue(generic in context.text, 'actual music topic vanished from source selection')

    async def test_renamed_current_member_keeps_eligible_original_author_label_for_writer(self):
        self.member.display_name = self.member.global_name = self.member.name = 'Current Member'
        provider = mock.AsyncMock(return_value=bot.GenerationResult(True, json.dumps(self.output)))
        with mock.patch.object(bot, '_generate_gemini_content_result_async', provider), \
             mock.patch.object(bot, '_song_control_request_sync', side_effect=[self.control, {'ok': True}]) as transport, \
             mock.patch.object(bot, 'read_song_context', return_value=SongContext()) as module_reader:
            await bot._run_song_control_cycle()
        self.assertEqual(provider.await_count, 1)
        self.assertTrue(self.text in provider.call_args.args[0], 'eligible older renamed-member original did not reach writer')
        self.assertEqual(module_reader.call_args.kwargs['public_subjects'], (('discord_user:43', 'Chris'),))
        self.assertIn('result', transport.call_args_list[1].args[1])

    def test_historical_author_lookup_is_song_only_and_merges_current_name_collisions(self):
        self.member.display_name = self.member.global_name = self.member.name = 'Current Member'
        self.assertEqual(named_sources._named_public_member_subjects(self.guild, 'Song about Chris'), ((), ()))
        self.assertEqual(bot._song_named_public_subjects('Song about Chris')[0], ((43, 'Chris'),))
        self.guild.members.append(SimpleNamespace(id=44, display_name='Chris', global_name='', name='', bot=False))
        self.assertEqual(bot._song_named_public_subjects('Song about Chris')[0], ())

    def test_eligible_historical_labels_keep_collisions_until_private_source_is_withdrawn(self):
        self.member.display_name = self.member.global_name = self.member.name = 'Current Member'
        self.guild.members.append(SimpleNamespace(id=44, display_name='Other Current Member', global_name='', name='', bot=False))
        with closing(sqlite3.connect(self.db)) as conn, conn:
            stamp = conn.execute('SELECT timestamp FROM conversations WHERE id=12').fetchone()[0]
            conn.execute("INSERT INTO conversations VALUES (13,44,'Chris','A different public author.', 'user',9001,'public_home',?,77,'public-stage',9013)", (stamp,))
            conn.execute("INSERT INTO memory_ledger_entries VALUES ('root-13',77,'conversations','13','discord_user:44','active',1,'observation')")
        self.assertEqual(bot._song_named_public_subjects('Song about Chris')[0], ())
        self.change('UPDATE memory_ledger_entries SET public_usable=0 WHERE entry_id=?', ('root-13',))
        self.assertEqual(bot._song_named_public_subjects('Song about Chris')[0], ((43, 'Chris'),))

    def test_historical_label_requires_current_guild_member_and_public_original(self):
        self.member.display_name = self.member.global_name = self.member.name = 'Current Member'
        self.assertEqual(bot._song_named_public_subjects('Song about Chris')[0], ((43, 'Chris'),))
        self.guild.members.clear()
        self.assertEqual(bot._song_named_public_subjects('Song about Chris')[0], ())
        self.guild.members.append(self.member)
        self.member.bot = True
        self.assertEqual(bot._song_named_public_subjects('Song about Chris')[0], ())
        self.member.bot = False
        self.guild.id = 78
        self.assertEqual(bot._song_named_public_subjects('Song about Chris')[0], ())
        self.guild.id = 77
        for sql in (
            "UPDATE conversations SET channel_policy='sealed_test'",
            "UPDATE conversations SET role='model'",
            "UPDATE conversations SET guild_id=78",
        ):
            with self.subTest(sql=sql):
                self.change(sql)
                self.assertEqual(bot._song_named_public_subjects('Song about Chris')[0], ())
                self.change("UPDATE conversations SET channel_policy='public_home',role='user',guild_id=77")

    def test_historical_author_labels_still_require_complete_labels(self):
        self.member.display_name = self.member.global_name = self.member.name = 'Current Member'
        self.change("UPDATE conversations SET user_name='Chris Crew'")
        self.assertEqual(bot._song_named_public_subjects('Song about Chris')[0], ())
        self.assertEqual(bot._song_named_public_subjects('Song about Chris Crew')[0], ((43, 'Chris Crew'),))

    async def test_historical_source_read_unavailable_blocks_provider(self):
        provider = mock.AsyncMock(return_value=bot.GenerationResult(True, json.dumps(self.output)))
        with mock.patch.object(named_sources, '_open_member_memory_read_connection', side_effect=sqlite3.DatabaseError('unavailable')), \
             mock.patch.object(bot, '_generate_gemini_content_result_async', provider), \
             mock.patch.object(bot, '_song_control_request_sync', side_effect=[self.control, {'ok': True}]) as transport:
            await bot._run_song_control_cycle()
        self.assertEqual(provider.await_count, 0)
        self.assertEqual(transport.call_args_list[1].args[1].get('errorCode'), 'CONTEXT_UNAVAILABLE')

    async def test_renamed_member_original_withdrawal_blocks_exact_receipt_replay(self):
        self.member.display_name = self.member.global_name = self.member.name = 'Current Member'
        provider = mock.AsyncMock(return_value=bot.GenerationResult(True, json.dumps(self.output)))
        with mock.patch.object(bot, '_generate_gemini_content_result_async', provider), \
             mock.patch.object(bot, '_song_control_request_sync', side_effect=[self.control, OSError('failed delivery'), self.control, {'ok': True}]) as transport:
            await bot._run_song_control_cycle()
            self.assertEqual(provider.await_count, 1)
            self.assertTrue(self.text in provider.call_args.args[0], 'renamed-member original missing before withdrawal')
            self.assertIn('result', transport.call_args_list[1].args[1])
            self.change('UPDATE memory_ledger_entries SET public_usable=0')
            await bot._run_song_control_cycle()
        self.assertEqual(provider.await_count, 1)
        self.assertEqual(transport.call_args_list[3].args[1].get('errorCode'), 'CONTEXT_UNAVAILABLE')
        self.assertNotIn('result', transport.call_args_list[3].args[1])

    def test_duplicate_name_wrong_guild_and_private_original_never_seed_named_history(self):
        self.assertIn(self.text, bot._read_song_public_context(self.command['options']).text)
        duplicate = SimpleNamespace(id=44, display_name='Chris', global_name='', name='', bot=False)
        self.guild.members.append(duplicate)
        self.assertNotIn(self.text, bot._read_song_public_context(self.command['options']).text)
        self.guild.members.pop()
        self.guild.id = 78
        self.assertNotIn(self.text, bot._read_song_public_context(self.command['options']).text)
        self.guild.id = 77
        self.change("UPDATE conversations SET channel_policy='sealed_test'")
        self.assertNotIn(self.text, bot._read_song_public_context(self.command['options']).text)

    def test_named_basis_roundtrip_binds_current_name_and_original_controls(self):
        context = bot._read_song_public_context(self.command['options'])
        serialized = SongContext('', tuple(json.loads(json.dumps(context.basis))))
        self.assertTrue(bot._song_public_context_is_current(serialized))
        self.member.display_name = self.member.global_name = self.member.name = 'Renamed Member'
        self.assertTrue(bot._song_public_context_is_current(serialized))
        self.guild.members.clear()
        self.assertFalse(bot._song_public_context_is_current(serialized))
        self.guild.members.append(self.member)
        self.member.display_name = self.member.global_name = self.member.name = 'Chris'
        self.change('UPDATE memory_ledger_entries SET public_usable=0')
        self.assertFalse(bot._song_public_context_is_current(serialized))
        self.assertNotIn(self.text, bot._read_song_public_context(self.command['options']).text)

    def test_unchanged_named_history_with_timestamp_id_reordering_survives_json_replay(self):
        older = 'Earlier, the lantern made the room laugh.'
        with closing(sqlite3.connect(self.db)) as conn, conn:
            stamp = conn.execute('SELECT timestamp FROM conversations WHERE id=12').fetchone()[0]
            prior = (datetime.fromisoformat(stamp) - timedelta(seconds=1)).isoformat()
            conn.execute("INSERT INTO conversations VALUES (13,43,'Chris',?,'user',9001,'public_home',?,77,'public-stage',9013)", (older, prior))
            conn.execute("INSERT INTO memory_ledger_entries VALUES ('root-13',77,'conversations','13','discord_user:43','active',1,'observation')")
        context = bot._read_song_public_context(self.command['options'])
        named = next(ref for ref in context.basis if ref['sourceKind'] == 'named_public_chat')
        self.assertEqual(named['sourceBasis']['sourceRowIds'], (13, 12))
        self.assertLess(context.text.index(older), context.text.index(self.text))
        serialized = SongContext('', tuple(json.loads(json.dumps(context.basis))))
        self.assertTrue(bot._song_public_context_is_current(serialized), 'unchanged original revisions must survive source ID/time reordering')

    def test_named_basis_unknown_fields_and_wrong_subject_are_rejected(self):
        context = bot._read_song_public_context(self.command['options'])
        refs = json.loads(json.dumps(context.basis))
        self.assertTrue(any(ref['sourceKind'] == 'named_public_chat' for ref in refs))
        named = next(ref for ref in refs if ref['sourceKind'] == 'named_public_chat')
        for change in (
            lambda value: value.update(unexpected='private'),
            lambda value: value.update(guildId=78),
            lambda value: value['sourceBasis'].update(sourceUsers=[[12, 44]]),
            lambda value: value['sourceBasis'].update(sourceRowIds=[12, 12]),
        ):
            modified = json.loads(json.dumps(refs))
            change(next(ref for ref in modified if ref['sourceKind'] == 'named_public_chat'))
            self.assertFalse(bot._song_public_context_is_current(SongContext('', tuple(modified))))

    def test_strict_named_reader_filters_private_neighbors_and_keeps_default(self):
        with closing(sqlite3.connect(self.db)) as conn, conn:
            stamp = conn.execute('SELECT timestamp FROM conversations WHERE id=12').fetchone()[0]
            neighbor_stamp = (datetime.fromisoformat(stamp) + timedelta(seconds=1)).isoformat()
            conn.execute("INSERT INTO conversations VALUES (13,43,'Chris','WITHDRAWN NEIGHBOR', 'user',9001,'public_home',?,77,'public-stage',9013)", (neighbor_stamp,))
            conn.execute("INSERT INTO memory_ledger_entries VALUES ('root-13',77,'conversations','13','discord_user:43','active',0,'observation')")
        frame = build_situation_frame_v1(route_allowed=True, route_mode='normal_chat',
            conversation_surface='discord', channel_policy='public_home', current_text='Song about Chris',
            subject_user_ids=(43,), subject_label_hints=('Chris',))
        arguments = dict(situation_frame=frame, guild_id=77, route_mode='normal_chat',
            channel_policy='public_home', user_text='Song about Chris')
        legacy, _ = named_sources.build_named_public_conversation_context(**arguments)
        strict, basis = named_sources.build_named_public_conversation_context(**arguments, require_current_originals=True)
        self.assertIn('WITHDRAWN NEIGHBOR', legacy)
        self.assertNotIn('WITHDRAWN NEIGHBOR', strict)
        self.assertEqual(basis.source_row_ids, (12,))

    async def test_unavailable_named_owner_stops_provider_and_reports_only_status(self):
        self.change('DROP TABLE memory_ledger_lineage')
        provider = mock.AsyncMock(side_effect=AssertionError('unavailable source cannot reach provider'))
        with mock.patch.object(bot, '_generate_gemini_content_result_async', provider), \
             mock.patch.object(bot, '_song_control_request_sync', side_effect=[self.control, {'ok': True}]) as transport, \
             self.assertLogs(level='INFO') as logs:
            await bot._run_song_control_cycle()
        self.assertEqual(provider.await_count, 0)
        self.assertEqual(transport.call_args_list[1].args[1].get('errorCode'), 'CONTEXT_UNAVAILABLE')
        diagnostics = '\n'.join(line for line in logs.output if 'song_public_context_' in line)
        self.assertIn('named_status=unavailable', diagnostics)
        self.assertNotIn('Chris', diagnostics)
        self.assertNotIn(self.text, diagnostics)

    def test_named_context_diagnostics_are_counts_and_status_without_source_identity(self):
        with self.assertLogs(level='INFO') as logs:
            context = bot._read_song_public_context(self.command['options'])
        self.assertIn(self.text, context.text)
        diagnostics = '\n'.join(line for line in logs.output if 'song_public_context_assembled' in line)
        self.assertIn('named_subjects=1 named_original_rows=1 named_status=available', diagnostics)
        self.assertIn('named_public_chat', diagnostics)
        self.assertNotIn('Chris', diagnostics)
        self.assertNotIn(self.text, diagnostics)
        self.assertNotIn('sourceRowIds', diagnostics)

    async def test_named_correction_after_model_blocks_delivery(self):
        async def generate(*_):
            self.change("INSERT INTO memory_ledger_lineage VALUES ('correction-13',77,'root-12','correction_of')")
            return bot.GenerationResult(True, json.dumps(self.output))
        provider = mock.AsyncMock(side_effect=generate)
        with mock.patch.object(bot, '_generate_gemini_content_result_async', provider), \
             mock.patch.object(bot, '_song_control_request_sync', side_effect=[self.control, {'ok': True}]) as transport:
            await bot._run_song_control_cycle()
        self.assertEqual(provider.await_count, 1)
        self.assertTrue(self.text in provider.call_args.args[0], 'selected original did not reach the writer')
        self.assertEqual(transport.call_args_list[1].args[1].get('errorCode'), 'CONTEXT_UNAVAILABLE')
        self.assertNotIn('result', transport.call_args_list[1].args[1])

    async def test_named_binding_change_blocks_receipt_replay_without_regeneration(self):
        provider = mock.AsyncMock(return_value=bot.GenerationResult(True, json.dumps(self.output)))
        with mock.patch.object(bot, '_generate_gemini_content_result_async', provider), \
             mock.patch.object(bot, '_song_control_request_sync', side_effect=[self.control, OSError('failed delivery'), self.control, {'ok': True}]) as transport:
            await bot._run_song_control_cycle()
            self.assertIn('result', transport.call_args_list[1].args[1])
            self.guild.members.append(SimpleNamespace(id=44, display_name='Chris', global_name='', name='', bot=False))
            await bot._run_song_control_cycle()
        self.assertEqual(provider.await_count, 1)
        self.assertEqual(transport.call_args_list[3].args[1].get('errorCode'), 'CONTEXT_UNAVAILABLE')
        self.assertNotIn('result', transport.call_args_list[3].args[1])

    async def test_new_blank_generate_does_not_inherit_previous_named_track(self):
        self.command['options'] = {}
        self.command['base'] = dict(title='Chris in the Supply Closet', lyrics='[Chorus]\nChris carries the lantern.', style='Chris-inspired rock.')
        provider = mock.AsyncMock(return_value=bot.GenerationResult(True, json.dumps(self.output)))
        with mock.patch.object(bot, '_generate_gemini_content_result_async', provider), \
             mock.patch.object(bot, '_song_control_request_sync', side_effect=[self.control, {'ok': True}]) as transport, \
             mock.patch.object(bot, 'read_song_context', return_value=SongContext()) as module_reader:
            await bot._run_song_control_cycle()
        self.assertEqual(provider.await_count, 1)
        self.assertIsNone(module_reader.call_args.kwargs['base'])
        self.assertNotIn(self.text, provider.call_args.args[0])
        self.assertNotIn('Chris in the Supply Closet', provider.call_args.args[0])
        self.assertIn('result', transport.call_args_list[1].args[1])


if __name__ == '__main__':
    unittest.main()
