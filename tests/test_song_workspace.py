import importlib.util
import json
import hashlib
import bnl_broadcast_ballads as ballads
import sqlite3
import tempfile
import unittest
from pathlib import Path
from contextlib import closing
from unittest import mock
import bnl_memory_ledger as ledger
import bnl_moment_engine as moments

song_spec = importlib.util.find_spec('bnl_song_workspace')
if song_spec:
    import bnl_song_workspace as songs
else:
    songs = None


class CatalogResponse:
    status = 200

    def __init__(self, catalog):
        self.body = json.dumps({"ballads": catalog}).encode()

    def read(self, limit=-1):
        return self.body[:limit] if limit >= 0 else self.body

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

def seed_ballad(db, published_at):
    ballads.initialize(db)
    version = dict(id="released-1", showId="show-1", ordinal=1, title="The Chairs Stayed Warm",
                   style="Chamber soul with dub bass", palette={"genres": "chamber soul"},
                   lyrics="LYRICS_ARE_NOT_TESTIMONY", rawOutput="PRIVATE_RAW_OUTPUT",
                   options={"feedback": "PRIVATE_PRODUCER_FEEDBACK"}, author="BNL-01")
    with closing(sqlite3.connect(db)) as conn, conn:
        for ordinal, title in ((1, version["title"]), (2, "UNPUBLISHED_NEWER_DRAFT")):
            saved = {**version, "ordinal": ordinal, "id": f"released-{ordinal}", "title": title}
            saved["contentHash"] = hashlib.sha256(json.dumps(saved, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
            conn.execute("INSERT INTO bnl_ballad_versions VALUES (?,?,?,?,?)",
                         (1, "show-1", saved["id"], ordinal, json.dumps(saved)))
    return [{"show": {"sessionId": "show-1", "title": "Friday Radio", "showDate": "2026-08-28"},
             "version": {key: version[key] for key in ("id", "title", "lyrics", "style", "palette", "author")},
             "linerNotes": {"about": "A song about the last light", "inspiration": "A creative interpretation",
                            "mentions": "", "inspiredBy": "An earlier broadcast"},
             "publishedAt": published_at, "audioId": "audio-1", "duration": 180,
             "presentation": {"credits": "BNL-01"}, "artistLinks": []}]

class SongWorkspaceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.assertIsNotNone(songs, 'private song command worker is missing')
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db = str(Path(self.tmp.name) / 'creative.db')
        self.command = dict(id='song-1', leaseId='lease-1', kind='generate', options={},
                            base=dict(title='', lyrics='', style=''),
                            limits=dict(maxLyricsWords=2000, targetSeconds=300))
        self.output = dict(title='The Paper Moon', lyrics='[Verse]\nFold the daylight into paper.\n[Chorus]\nBring it home.',
                           style='1981 chamber pop and dub, brushed percussion, close warm vocal.')
        self.calls = []

    async def generate(self, prompt):
        self.calls.append(prompt)
        return songs.SongGeneration(json.dumps(self.output), 'STOP')

    async def execute(self, command=None, **kwargs):
        return await songs.execute_command(self.db, 77, command or self.command,
            generate=kwargs.pop('generate', self.generate),
            context_reader=kwargs.pop('context_reader', lambda _: songs.SongContext()),
            **kwargs)

    async def test_blank_optional_fields_generate_a_complete_song_without_episode_requirement(self):
        result = await self.execute()
        self.assertEqual(result, dict(commandId='song-1', leaseId='lease-1', outcome='applied', result=self.output))
        self.assertEqual(len(self.calls), 1)
        self.assertIn('All options are optional', self.calls[0])
        self.assertNotIn('AUTHORIZED SHOW EVIDENCE', self.calls[0])
        self.assertIn('Do not add real people or cast lists', self.calls[0])

    def test_private_song_keeps_the_complete_ballad_songwriting_requirements(self):
        prompt = songs.build_prompt(self.command, songs.SongContext())
        for requirement in ('1,400 characters', '2–4 contrasting genres/styles', '1970–2010',
                            '250–400 characters', '500 characters', 'multisyllabic rhyme families'):
            self.assertTrue(requirement in prompt, requirement)
        self.assertIn('form', prompt)
        self.assertIn('No show or episode is required', prompt)
        self.assertNotIn('Style maximum 6000 characters', prompt)

    async def test_new_style_uses_existing_compact_copy_boundary_and_preserves_lyrics(self):
        from bnl_creative_protocol import SUNO_STYLE_MAX_CHARS
        self.output['style'] = '1978 chamber pop and dub; close dry voice. ' + 'Warm acoustic arrangement ' * 35
        result = await self.execute()
        self.assertEqual(result['outcome'], 'applied')
        self.assertLessEqual(len(result['result']['style']), SUNO_STYLE_MAX_CHARS)
        self.assertEqual(result['result']['lyrics'], self.output['lyrics'])

    def test_blank_song_can_draw_on_broad_existing_public_memory(self):
        with mock.patch('bnl_moment_engine.select_public_situation_moment_gists', return_value=()) as selector:
            with closing(sqlite3.connect(':memory:')) as conn:
                songs.read_context_on_connection(conn, 77, {})
        self.assertTrue(selector.call_args.kwargs['broad_recall'])
        self.assertFalse(selector.call_args.kwargs['require_topic_overlap'])
        self.assertGreaterEqual(selector.call_args.kwargs['max_results'], 6)

    def test_released_catalog_and_finalized_show_context_reach_song_without_private_drafts(self):
        from types import SimpleNamespace
        import bnl_broadcast_ballads as ballads
        catalog = seed_ballad(self.db, '2026-08-28T12:00:00Z')
        with mock.patch('urllib.request.urlopen', return_value=CatalogResponse(catalog)):
            snapshot = ballads.read_publication_catalog('https://www.barcode-network.com')
        show = SimpleNamespace(kind='dialogue', text='Public listeners joked about a crooked paper lantern.',
                               source_ref='show:one:dialogue', source_digest='show-v1')
        with mock.patch('bnl_moment_engine.select_public_situation_moment_gists', return_value=()), \
             mock.patch('bnl_tiktok_show_ledger.select_tiktok_show_episode_context_items', return_value=(show,)) as reader:
            with closing(sqlite3.connect(self.db)) as conn:
                context = songs.read_context_on_connection(conn, 1, {}, publication_snapshot=snapshot,
                                                         now='2026-10-10T12:00:00Z')
        self.assertIn('crooked paper lantern', context.text)
        self.assertIn('The Chairs Stayed Warm', context.text)
        prompt = songs.build_prompt(self.command, context)
        self.assertIn('PRIOR CREATIVE CATALOG', prompt)
        for private in ('PRIVATE_PRODUCER_FEEDBACK', 'PRIVATE_RAW_OUTPUT', 'UNPUBLISHED_NEWER_DRAFT',
                        'LYRICS_ARE_NOT_TESTIMONY'):
            self.assertNotIn(private, prompt)
        self.assertTrue(reader.call_args.kwargs['require_current_originals'])
        self.assertEqual(reader.call_args.kwargs['subject_user_id'], 0)
        self.assertFalse(reader.call_args.kwargs['allow_subject_continuity'])

    async def test_released_song_withdrawal_invalidates_saved_inspiration(self):
        import bnl_broadcast_ballads as ballads
        catalog = seed_ballad(self.db, '2026-08-28T12:00:00Z')
        with mock.patch('urllib.request.urlopen', return_value=CatalogResponse(catalog)):
            snapshot = ballads.read_publication_catalog('https://www.barcode-network.com')
        with closing(sqlite3.connect(self.db)) as conn:
            context = songs.read_context_on_connection(conn, 1, {}, publication_snapshot=snapshot,
                                                     now='2026-10-10T12:00:00Z')
        self.assertIn('The Chairs Stayed Warm', context.text)
        async def generate(prompt):
            return songs.SongGeneration(json.dumps(self.output))
        with mock.patch('bnl_broadcast_ballads.read_publication_catalog', return_value=snapshot):
            result = await songs.execute_command(self.db, 1, self.command, generate=generate,
                                                context_reader=lambda _: context)
        self.assertEqual(result['outcome'], 'applied')
        with mock.patch('bnl_broadcast_ballads.read_publication_catalog', return_value={'available':True,'songs':[]}):
            replay = await songs.execute_command(self.db, 1, self.command, generate=generate,
                                                context_reader=lambda _: context)
        self.assertEqual(replay['errorCode'], 'CONTEXT_UNAVAILABLE')
        self.assertNotIn('result', replay)

    def test_show_revision_revalidation_uses_the_same_frozen_public_read_scope(self):
        basis = ({'sourceKind':'show_episode','sourceId':'show:one:dialogue','sourceVersion':'v1',
                  'query':'last 3 shows community','observedAt':'2026-10-10T12:00:00Z','maxShows':3},)
        context = songs.SongContext('Public scene', basis)
        with closing(sqlite3.connect(self.db)):
            pass
        with mock.patch('bnl_tiktok_show_ledger.tiktok_show_episode_context_item_versions',
                        return_value={'show:one:dialogue':'v1'}) as versions:
            self.assertTrue(songs.context_is_current(self.db, 77, context))
        self.assertTrue(versions.call_args.kwargs['require_current_originals'])
        self.assertEqual(versions.call_args.kwargs['max_shows'], 3)
        self.assertEqual(versions.call_args.kwargs['now'], '2026-10-10T12:00:00Z')
        with mock.patch('bnl_tiktok_show_ledger.tiktok_show_episode_context_item_versions', return_value={}):
            self.assertFalse(songs.context_is_current(self.db, 77, context))

    async def test_replay_and_a_reclaimed_lease_deliver_saved_result_without_another_generation(self):
        first = await self.execute()
        self.assertEqual(await self.execute(), first)
        new = await self.execute({**self.command, 'leaseId': 'lease-2'})
        self.assertEqual(new, {**first, 'leaseId': 'lease-2'})
        self.assertEqual(len(self.calls), 1)
        with closing(sqlite3.connect(self.db)) as conn, conn:
            self.assertEqual(conn.execute('SELECT COUNT(*) FROM bnl_song_commands').fetchone()[0], 1)
            self.assertFalse(conn.execute("SELECT 1 FROM sqlite_master WHERE name='memory_ledger_entries'").fetchone())

    async def test_changed_command_identity_cannot_replay_private_result(self):
        await self.execute()
        changed = await self.execute({**self.command, 'options': {'idea': 'a different subject'}})
        self.assertEqual(changed['errorCode'], 'INVALID_COMMAND')
        self.assertNotIn('result', changed)
        other_guild = await songs.execute_command(self.db, 88, self.command, generate=self.generate,
                                                 context_reader=lambda _: songs.SongContext())
        self.assertEqual(other_guild['outcome'], 'applied')
        self.assertEqual(len(self.calls), 2)

    async def test_style_regeneration_preserves_title_and_lyrics_byte_exact(self):
        base = dict(title='  Untouched title\n', lyrics='[Verse]\n Exact words.  \n', style='old style')
        self.output = dict(title='provider tries a new title', lyrics='provider tries new lyrics', style='New arrangement')
        result = await self.execute({**self.command, 'kind': 'style', 'base': base})
        self.assertEqual(result['result'], {**base, 'style': 'New arrangement'})
        self.assertIn('existing lyric structure', self.calls[0])

    async def test_lyrics_regeneration_preserves_title_and_style_byte_exact(self):
        base = dict(title='  Untouched title\n', lyrics='old lyrics', style=' close dry voice. \n')
        result = await self.execute({**self.command, 'kind': 'lyrics', 'base': base})
        self.assertEqual(result['result'], {**base, 'lyrics': self.output['lyrics']})

    async def test_2000_whitespace_words_are_allowed_and_2001_fail_without_truncation(self):
        self.output['lyrics'] = 'word\t' * 1999 + 'last'
        accepted = await self.execute()
        self.assertEqual(accepted['result']['lyrics'], self.output['lyrics'])
        self.output['lyrics'] += '\nextra'
        failed = await self.execute({**self.command, 'id': 'song-2'})
        self.assertEqual(failed['errorCode'], 'LYRICS_TOO_LONG')
        self.assertNotIn('result', failed)

    async def test_provider_failure_and_malformed_result_are_safe_durable_failures(self):
        async def outage(_):
            self.calls.append('attempt')
            raise RuntimeError('sensitive provider diagnostic')
        failure = await self.execute(generate=outage)
        self.assertEqual(failure['errorCode'], 'PROVIDER_UNAVAILABLE')
        self.assertEqual(await self.execute(generate=outage), failure)
        self.assertEqual(len(self.calls), 1)
        self.assertNotIn('sensitive', json.dumps(failure))
        async def malformed(_):
            return songs.SongGeneration('{"lyrics":"unfinished', 'MAX_TOKENS')
        result = await self.execute({**self.command, 'id': 'song-2'}, generate=malformed)
        self.assertEqual(result['errorCode'], 'INVALID_RESULT')
        self.assertNotIn('result', result)

    async def test_budget_denial_is_saved_without_private_exception_text_or_retry(self):
        async def denied(_):
            self.calls.append('attempt')
            raise songs.SongFailure('BUDGET_UNAVAILABLE')
        failure = await self.execute(generate=denied)
        self.assertEqual(failure['errorCode'], 'BUDGET_UNAVAILABLE')
        self.assertEqual(await self.execute(generate=denied), failure)
        self.assertEqual(len(self.calls), 1)

    async def test_utf16_limits_and_control_characters_match_the_site_contract(self):
        self.output['title'] = '\U0001f3b5' * 80
        self.assertEqual((await self.execute())['outcome'], 'applied')
        self.output['title'] += '\U0001f3b5'
        result = await self.execute({**self.command, 'id': 'unicode-title'})
        self.assertEqual(result.get('errorCode'), 'RESULT_TOO_LONG')
        self.output['title'] = 'Valid title'
        self.output['lyrics'] = 'word\ufeff' * 2000 + 'extra'
        result = await self.execute({**self.command, 'id': 'unicode-space'})
        self.assertEqual(result['errorCode'], 'LYRICS_TOO_LONG')
        self.output['lyrics'] = 'Private\x00control'
        result = await self.execute({**self.command, 'id': 'unsafe-control'})
        self.assertEqual(result['errorCode'], 'INVALID_RESULT')
        self.assertNotIn('result', result)
        self.output['lyrics'] = '\ud800'
        result = await self.execute({**self.command, 'id': 'unsafe-surrogate'})
        self.assertEqual(result['errorCode'], 'INVALID_RESULT')

    async def test_revocation_during_receipt_save_is_checked_again_before_delivery(self):
        state = {'current': True}
        context = songs.SongContext('Public inspiration', ({'id': 'm1', 'version': 'v1'},))
        save = songs._save
        def save_and_revoke(*args):
            receipt = save(*args)
            state['current'] = False
            return receipt
        with mock.patch.object(songs, '_save', side_effect=save_and_revoke):
            receipt = await self.execute(context_reader=lambda _: context, context_current=lambda _: state['current'])
            self.assertEqual(receipt['outcome'], 'applied')
            self.assertTrue(hasattr(songs, 'prepare_delivery'), 'final source revalidation is missing')
            final = await songs.prepare_delivery(self.db, 77, self.command, receipt, context_current=lambda _: state['current'])
        self.assertEqual(final['errorCode'], 'CONTEXT_UNAVAILABLE')
        self.assertNotIn('result', final)
        self.assertEqual(len(self.calls), 1)

    async def test_output_field_caps_fail_honestly(self):
        for field, limit in (('title', 160), ('style', 6000), ('lyrics', 40000)):
            self.output[field] = 'x' * (limit + 1)
            result = await self.execute({**self.command, 'id': field})
            self.assertEqual(result.get('errorCode'), 'RESULT_TOO_LONG')
            self.output[field] = 'valid'

    async def test_stale_original_context_invalidates_fresh_and_saved_private_result(self):
        context = songs.SongContext('Public inspiration', ({'id': 'm1', 'version': 'v1'},))
        state = {'current': True}
        result = await self.execute(context_reader=lambda _: context,
                                    context_current=lambda _: state['current'])
        self.assertEqual(result['outcome'], 'applied')
        state['current'] = False
        failure = await self.execute(context_reader=lambda _: context,
                                     context_current=lambda _: state['current'])
        self.assertEqual(failure['errorCode'], 'CONTEXT_UNAVAILABLE')
        self.assertNotIn('result', failure)
        self.assertEqual(len(self.calls), 1)

    async def test_running_claim_cannot_make_a_second_physical_call_after_interruption(self):
        async def cancelled(_):
            import asyncio
            raise asyncio.CancelledError()
        import asyncio
        with self.assertRaises(asyncio.CancelledError):
            await self.execute(generate=cancelled)
        with closing(sqlite3.connect(self.db)) as conn, conn:
            conn.execute("UPDATE bnl_song_commands SET created_at='2000-01-01T00:00:00+00:00'")
        result = await self.execute()
        self.assertEqual(result['errorCode'], 'GENERATION_INTERRUPTED')
        self.assertEqual(len(self.calls), 0)

    async def test_concurrent_command_claim_cannot_make_a_second_physical_call(self):
        import asyncio
        started, release = asyncio.Event(), asyncio.Event()
        async def waiting(prompt):
            self.calls.append(prompt)
            started.set()
            await release.wait()
            return songs.SongGeneration(json.dumps(self.output))
        task = asyncio.create_task(self.execute(generate=waiting))
        await started.wait()
        try:
            self.assertIsNone(await self.execute(generate=waiting))
        finally:
            release.set()
            await task
        self.assertEqual(len(self.calls), 1)

    async def test_context_database_outage_after_model_does_not_return_private_result(self):
        checks = iter((True, RuntimeError('sensitive context diagnostic')))
        def current(_):
            value = next(checks)
            if isinstance(value, Exception):
                raise value
            return value
        result = await self.execute(context_current=current)
        self.assertEqual(result['errorCode'], 'CONTEXT_UNAVAILABLE')
        self.assertNotIn('result', result)
        self.assertNotIn('sensitive', json.dumps(result))

    async def test_real_public_moment_privacy_change_invalidates_saved_result(self):
        import os
        from datetime import datetime, timedelta, timezone
        import bnl_memory_ledger as ledger
        import bnl_moment_engine as moments
        with mock.patch.dict(os.environ, {'BNL_MEMORY_LEDGER_SHADOW_ENABLED': 'true', 'BNL_MOMENT_ENGINE_SHADOW_ENABLED': 'true',
                                          'BNL_MEMORY_GOVERNANCE_LIVE_ENABLED': 'false'}):
            with closing(sqlite3.connect(self.db)) as conn, conn:
                moments.ensure_moment_schema(conn)
                start = datetime.now(timezone.utc) - timedelta(minutes=20)
                roots = []
                turns = ('I made a paper lantern for the BARCODE music community table.',
                         'The paper lantern lights our BARCODE music community table.',
                         'The BARCODE music community paper lantern will hang above the table.')
                for index, text in enumerate(turns):
                    result = ledger.shadow_conversation_row(conn, row_id=index + 1, guild_id=77, user_id=index + 1,
                        user_name='Test Member', role='user', content=text, channel_id=10, channel_name='public-room',
                        channel_policy='public_home', route_mode='normal_chat', observed_at=(start + timedelta(seconds=index * 10)).isoformat())
                    roots.append(result.entry_id)
                    moments.observe_ledger_entry(conn, result.entry_id)
                moments.sweep_expired_windows(conn, now=(start + timedelta(minutes=10)).isoformat())
                request = moments.claim_pending_moment_meaning(conn, guild_ids=(77,))
                self.assertIsNotNone(request)
                meaning = dict(summary='Community members discussed a handmade lantern near their shared listening station.',
                               contributions={f'participant_{index + 1}': 'The participant described handmade lighting at a communal listening place.' for index in range(3)})
                if request.admission_required:
                    meaning['retain'] = True
                self.assertTrue(moments.apply_moment_meaning(conn, request, json.dumps(meaning)))
            context = songs.read_context(self.db, 77, {})
            self.assertIn('handmade lantern', context.text)
            self.assertNotIn(turns[0], context.text)
            result = await self.execute(context_reader=lambda _: songs.read_context(self.db, 77, {}))
            self.assertEqual(result['outcome'], 'applied')
            with closing(sqlite3.connect(self.db)) as conn, conn:
                conn.execute('UPDATE memory_ledger_entries SET public_usable=0 WHERE entry_id=?', (roots[0],))
            failure = await self.execute()
            self.assertEqual(failure['errorCode'], 'CONTEXT_UNAVAILABLE')
            self.assertNotIn('result', failure)
            self.assertEqual(len(self.calls), 1)
            self.assertEqual(os.environ['BNL_MEMORY_GOVERNANCE_LIVE_ENABLED'], 'false')

    async def test_locked_creative_store_does_not_block_the_existing_event_loop(self):
        import asyncio
        import threading
        import time
        from bnl_broadcast_ballads import initialize
        initialize(self.db)
        conn = sqlite3.connect(self.db, check_same_thread=False)
        conn.execute('BEGIN EXCLUSIVE')
        timer = threading.Timer(2.0, conn.rollback)
        timer.start()
        task = asyncio.create_task(self.execute())
        started = time.monotonic()
        try:
            await asyncio.sleep(0.03)
            self.assertLess(time.monotonic() - started, 1.2)
        finally:
            timer.cancel()
            await asyncio.to_thread(timer.join)
            conn.rollback()
            conn.close()
            await task

    def test_blank_context_query_is_bounded_public_music_context_without_episode_reader(self):
        with mock.patch('bnl_moment_engine.select_public_situation_moment_gists', return_value=()) as selector:
            with closing(sqlite3.connect(':memory:')) as conn:
                context = songs.read_context_on_connection(conn, 77, {})
        self.assertEqual(context.text, '')
        self.assertEqual(selector.call_args.kwargs['topic_text'], '')
        self.assertEqual(selector.call_args.kwargs['allowed_channel_policies'], ('public_home', 'public_context'))
        self.assertFalse(selector.call_args.kwargs['prepare_schema'])

    def test_context_has_no_participant_identity_or_private_raw_source_packet(self):
        from types import SimpleNamespace
        basis = dict(summary='The community made a paper lantern.', sourceVersion='v1',
                     contributions=[dict(displayName='Test Member', summary='Private contribution')],
                     originalSourceRefs=[dict(sourceRowId=6, subjectRef='discord_user:7')])
        with mock.patch('bnl_moment_engine.select_public_situation_moment_gists', return_value=(SimpleNamespace(moment_id='m1'),)), \
             mock.patch('bnl_moment_engine.public_moment_source_basis', return_value=basis):
            with closing(sqlite3.connect(':memory:')) as conn:
                context = songs.read_context_on_connection(conn, 77, {})
        self.assertIn('paper lantern', context.text)
        self.assertNotIn('Private contribution', context.text)
        self.assertNotIn('discord_user', context.text)
        self.assertNotIn('Test Member', context.text)


if __name__ == '__main__':
    unittest.main()
