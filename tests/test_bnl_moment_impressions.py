"""Impression ownership/scope tests; controlled payloads are not model-quality proof."""
import json
import os
import sqlite3
import unittest
from datetime import datetime, timedelta, timezone
from unittest import mock

import bnl_memory_ledger as ledger
import bnl_moment_engine as moments


TURNS = (
    ('user', 'I tried a wavering synthesizer under the acoustic drums in my new track.'),
    ('model', 'That contrast might make the room feel unusually elastic.'),
    ('user', 'The synthesizer kept drifting, so I left its crooked pitch in the final mix.'),
    ('model', 'A little instability can give the rhythm a surprising edge.'),
    ('user', 'I am pleased that an accidental texture became the part I wanted to keep.'),
    ('model', 'An unplanned detail found a useful place in the arrangement.'),
)
MEANING = {
    'summary': 'A member described keeping an accidental synthesizer texture in a track with acoustic percussion.',
    'contributions': {'participant_1': 'The participant experimented with contrasting sounds and chose to retain an unexpected tuning detail.'},
}
IMPRESSION = {
    'impression': 'I enjoy the stubborn little wobble surviving the cleanup. Perfectly aligned signals can get dull.',
    'reason': 'The member chose an unexpected sound instead of erasing it; that creative decision caught my attention.',
    'sourceRefs': ['turn_3', 'turn_5'],
}


class MomentImpressionTests(unittest.TestCase):
    def setUp(self):
        self.flags = {
            'BNL_MEMORY_LEDGER_SHADOW_ENABLED': 'true',
            'BNL_MOMENT_ENGINE_SHADOW_ENABLED': 'true',
            'BNL_IMPRESSIONS_FORMATION_ENABLED': 'true',
            'BNL_IMPRESSIONS_USE_ENABLED': 'true',
            'BNL_IMPRESSIONS_GUILD_IDS': '1',
        }
        patch = mock.patch.dict(os.environ, self.flags)
        patch.start()
        self.addCleanup(patch.stop)
        self.conn = sqlite3.connect(':memory:')
        self.addCleanup(self.conn.close)
        moments.ensure_moment_schema(self.conn)

    def moment(self, *, channel=10, policy='public_home', turns=TURNS):
        started = datetime(2026, 9, 12, 7, channel % 60, tzinfo=timezone.utc)
        roots, mid = [], ''
        for index, (role, text) in enumerate(turns):
            result = ledger.shadow_conversation_row(
                self.conn, row_id=channel * 100 + index, guild_id=1, user_id=7,
                user_name='Test Musician', role=role, content=text, channel_id=channel,
                channel_name='test-room', channel_policy=policy, route_mode='normal_chat',
                observed_at=(started + timedelta(seconds=10 * index)).isoformat(),
            )
            self.assertEqual(result.outcome, 'inserted')
            roots.append(result.entry_id)
            if not mid:
                mid = moments.observe_ledger_entry(self.conn, result.entry_id).moment_id
            else:
                source = moments._fetch_entry(self.conn, result.entry_id)
                moments._insert_membership(self.conn, mid, source,
                    moments._meaningful(text, role, source.predicate_key),
                    moments._topic_family(text, source.predicate_key),
                    moments._topic_signature(text, source.predicate_key))
        moments.finalize_moment(self.conn, mid)
        self.conn.commit()
        return mid, roots

    def ready(self, *, channel=10, policy='public_home', impression=IMPRESSION, turns=TURNS, meaning=MEANING):
        mid, roots = self.moment(channel=channel, policy=policy, turns=turns)
        request = moments.claim_pending_moment_meaning(self.conn, guild_ids=(1,))
        self.assertIsNotNone(request)
        self.assertTrue(moments.apply_moment_meaning(
            self.conn, request, json.dumps(dict(meaning, impression=impression))))
        return mid, roots, request

    def read(self, mid, **kwargs):
        return moments.read_moment_impression(self.conn, guild_id=1, moment_id=mid, **kwargs)

    def test_default_off_retains_old_request_and_factual_result(self):
        mid, _ = self.moment()
        with mock.patch.dict(os.environ, {'BNL_IMPRESSIONS_FORMATION_ENABLED': 'false'}):
            request = moments.claim_pending_moment_meaning(self.conn, guild_ids=(1,))
        self.assertFalse(request.impression_requested)
        self.assertNotIn('optional impression', request.prompt)
        self.assertTrue(moments.apply_moment_meaning(self.conn, request, json.dumps(MEANING)))
        self.assertIsNone(self.read(mid))
        self.assertIsNotNone(moments.public_moment_source_basis(self.conn, guild_id=1, moment_id=mid))

    def test_one_existing_call_forms_separate_root_bound_opinion(self):
        mid, roots, request = self.ready()
        self.assertTrue(request.impression_requested)
        self.assertIn('tentative, revisable', request.prompt)
        item = self.read(mid)
        self.assertEqual(item['impression'], IMPRESSION['impression'])
        self.assertEqual(item['sourceRefs'], [roots[2], roots[4]])
        self.assertEqual([row['ledgerEntryId'] for row in item['originalSourceRefs']], roots)
        self.assertEqual(len(item['evidence']), len(TURNS))
        self.assertEqual(item['evidence'][0]['participantAlias'], 'participant_1')
        self.assertEqual(item['evidence'][1]['participantAlias'], 'BNL')
        factual = moments.public_moment_source_basis(self.conn, guild_id=1, moment_id=mid)
        self.assertNotIn(IMPRESSION['impression'], json.dumps(factual))
        self.assertEqual(item, self.read(mid))
        self.assertIsNone(moments.claim_pending_moment_meaning(self.conn, guild_ids=(1,)))
        self.assertFalse(moments.apply_moment_meaning(self.conn, request, json.dumps(dict(MEANING, impression=IMPRESSION))))

    def test_guild_scope_and_independent_gates(self):
        for env in ({}, {'BNL_IMPRESSIONS_USE_ENABLED': 'true'},
                    dict(self.flags, BNL_IMPRESSIONS_GUILD_IDS='2')):
            self.assertFalse(moments.impressions_enabled(1, environ=env))
        self.assertTrue(moments.impressions_enabled(1, environ=dict(self.flags, BNL_IMPRESSIONS_GUILD_IDS='2, 1')))
        mid, _, _ = self.ready()
        self.assertIsNone(self.read(mid, environ=dict(self.flags, BNL_IMPRESSIONS_USE_ENABLED='false')))
        self.assertIsNotNone(self.read(mid, environ=dict(self.flags, BNL_IMPRESSIONS_FORMATION_ENABLED='false')))
        self.assertIsNone(moments.read_moment_impression(self.conn, guild_id=2, moment_id=mid,
                                                       environ=dict(self.flags, BNL_IMPRESSIONS_GUILD_IDS='1,2')))

    def test_gate_switch_during_call_drops_optional_opinion_not_meaning(self):
        mid, _ = self.moment()
        request = moments.claim_pending_moment_meaning(self.conn, guild_ids=(1,))
        with mock.patch.dict(os.environ, {'BNL_IMPRESSIONS_FORMATION_ENABLED': 'false'}):
            self.assertTrue(moments.apply_moment_meaning(self.conn, request, json.dumps(dict(MEANING, impression=IMPRESSION))))
        self.assertIsNone(self.read(mid))
        self.assertIsNotNone(moments.public_moment_source_basis(self.conn, guild_id=1, moment_id=mid))

    def test_formation_requires_allowlist_and_original_human_anchor(self):
        mid, _ = self.moment()
        with mock.patch.dict(os.environ, {'BNL_IMPRESSIONS_GUILD_IDS': '2'}):
            request = moments.claim_pending_moment_meaning(self.conn, guild_ids=(1,))
        self.assertFalse(request.impression_requested)
        self.assertTrue(moments.apply_moment_meaning(self.conn, request, json.dumps(MEANING)))
        self.assertIsNone(self.read(mid))
        other, roots = self.moment(channel=11)
        request = moments.claim_pending_moment_meaning(self.conn, guild_ids=(1,))
        self.conn.execute('UPDATE memory_ledger_entries SET derived=1 WHERE entry_id=?', (roots[2],))
        self.assertTrue(moments.apply_moment_meaning(self.conn, request, json.dumps(dict(MEANING, impression=IMPRESSION))))
        self.assertEqual(self.conn.execute('SELECT impression_payload FROM memory_moment_windows WHERE moment_id=?', (other,)).fetchone()[0], '')

    def test_declined_or_malformed_impression_does_not_discard_factual_memory(self):
        for index, payload in enumerate((None, {}, 'opinion',
                dict(IMPRESSION, sourceRefs=['turn_2']),
                dict(IMPRESSION, sourceRefs=['turn_99']),
                dict(IMPRESSION, sourceRefs=['turn_3', 'turn_3']),
                dict(IMPRESSION, sourceRefs=[[]]),
                dict(IMPRESSION, impression='My password is example.'),
                dict(IMPRESSION, impression=TURNS[0][1]))):
            with self.subTest(payload=payload):
                mid, _, _ = self.ready(channel=20 + index, impression=payload)
                self.assertIsNone(self.read(mid))
                self.assertIsNotNone(moments.public_moment_source_basis(self.conn, guild_id=1, moment_id=mid))

    def test_every_original_and_projection_remains_live_at_read_time(self):
        mid, roots, _ = self.ready()
        for sql, args in (
            ("UPDATE memory_ledger_entries SET normalized_value='A changed reply.' WHERE entry_id=?", (roots[1],)),
            ("UPDATE memory_ledger_entries SET lifecycle_status='forgotten' WHERE entry_id=?", (roots[0],)),
            ("UPDATE memory_ledger_entries SET visibility='private' WHERE entry_id=?", (roots[4],)),
            ("UPDATE memory_ledger_entries SET public_usable=0 WHERE entry_id=?", (roots[0],)),
            ("UPDATE memory_ledger_entries SET derived=1 WHERE entry_id=?", (roots[0],)),
            ("UPDATE memory_ledger_entries SET subject_key='discord_user:8' WHERE entry_id=?", (roots[2],)),
            ("DELETE FROM memory_ledger_entries WHERE entry_id=?", (roots[3],)),
            ("UPDATE memory_moment_contributions SET contribution_gist='Unsupported substitution.' WHERE moment_id=?", (mid,)),
            ("UPDATE memory_moment_windows SET impression_projection_digest='modified' WHERE moment_id=?", (mid,)),
            ("INSERT INTO memory_ledger_lineage VALUES(?,?,?,?,?)", (roots[4], 1, 'correction_of', roots[0], '2026-09-13T00:00:00Z')),
        ):
            with self.subTest(sql=sql):
                self.conn.execute('SAVEPOINT changed')
                self.conn.execute(sql, args)
                self.assertIsNone(self.read(mid))
                self.assertEqual(moments.select_moment_impressions(self.conn, guild_id=1), [])
                self.conn.execute('ROLLBACK TO changed')
                self.conn.execute('RELEASE changed')
        self.assertIsNotNone(self.read(mid))

    def test_impression_changes_do_not_change_factual_moment_version(self):
        mid, _, _ = self.ready()
        factual = moments.public_moment_source_basis(self.conn, guild_id=1, moment_id=mid)
        self.conn.execute("UPDATE memory_moment_windows SET impression_payload='{}' WHERE moment_id=?", (mid,))
        self.assertIsNone(self.read(mid))
        self.assertEqual(factual, moments.public_moment_source_basis(self.conn, guild_id=1, moment_id=mid))

    def test_source_change_before_apply_cannot_create_impression(self):
        mid, roots = self.moment()
        request = moments.claim_pending_moment_meaning(self.conn, guild_ids=(1,))
        self.conn.execute("UPDATE memory_ledger_entries SET normalized_value='Changed.' WHERE entry_id=?", (roots[0],))
        self.assertFalse(moments.apply_moment_meaning(self.conn, request, json.dumps(dict(MEANING, impression=IMPRESSION))))
        self.assertIsNone(self.read(mid))

    def test_public_and_exact_private_scope_are_isolated(self):
        public, _, _ = self.ready()
        private, _, _ = self.ready(channel=20, policy='sealed_test')
        other_private, _, _ = self.ready(channel=21, policy='sealed_test')
        self.assertIsNone(self.read(private))
        self.assertIsNone(self.read(private, channel_policy='sealed_test', channel_id=21))
        self.assertIsNone(self.read(private, channel_policy='sealed_test'))
        self.assertIsNone(self.read(public, channel_policy='private', channel_id=20))
        self.assertIsNotNone(self.read(public, channel_policy='sealed_test', channel_id=20))
        chosen = moments.select_moment_impressions(self.conn, guild_id=1, channel_policy='sealed_test', channel_id=20)
        self.assertEqual({item['momentId'] for item in chosen}, {public, private})
        self.assertNotIn(other_private, {item['momentId'] for item in chosen})

    def test_selector_respects_sources_subject_time_and_limit(self):
        mid, _, _ = self.ready()
        self.assertEqual(len(moments.select_moment_impressions(self.conn, guild_id=1, topic_text='synthesizer', subject_key='discord_user:7')), 1)
        for kwargs in ({'subject_key': 'discord_user:8'},
                       {'topic_text': 'What caught your attention on 2026-09-13?'},
                       {'observed_before': '2026-09-12T07:00:00Z'}, {'max_results': 0}):
            self.assertEqual(moments.select_moment_impressions(self.conn, guild_id=1, **kwargs), [])
        self.assertEqual(moments.select_moment_impressions(self.conn, guild_id=1, observed_before='2026-09-13T00:00:00Z')[0]['momentId'], mid)

    def test_general_or_paraphrased_question_does_not_require_keyword_overlap(self):
        mid, _, _ = self.ready()
        for text in ('What stayed with you?', 'Anything worth reflecting on?', 'What surprised you recently?'):
            with self.subTest(text=text):
                self.assertEqual(moments.select_moment_impressions(self.conn, guild_id=1, topic_text=text)[0]['momentId'], mid)

    def test_actual_topic_overlap_ranks_a_relevant_older_experience_first(self):
        older, _, _ = self.ready()
        alternate = tuple((role, text.replace('synthesizer', 'marimba')) for role, text in TURNS)
        newer, _, _ = self.ready(channel=11, turns=alternate,
                                meaning=json.loads(json.dumps(MEANING).replace('synthesizer', 'marimba')))
        self.assertEqual(moments.select_moment_impressions(self.conn, guild_id=1, topic_text='synthesizer', max_results=1)[0]['momentId'], older)
        self.assertEqual(moments.select_moment_impressions(self.conn, guild_id=1, max_results=1)[0]['momentId'], newer)

    def test_read_only_and_old_schema_do_not_initialize_or_backfill(self):
        old = sqlite3.connect(':memory:')
        self.addCleanup(old.close)
        self.assertIsNone(moments.read_moment_impression(old, guild_id=1, moment_id='missing'))
        self.assertEqual(old.execute('SELECT name FROM sqlite_master').fetchall(), [])
        old.execute('CREATE TABLE memory_moment_windows(moment_id TEXT)')
        self.assertEqual(moments.select_moment_impressions(old, guild_id=1), [])
        self.assertEqual([r[1] for r in old.execute('PRAGMA table_info(memory_moment_windows)')], ['moment_id'])
        mid, _, _ = self.ready()
        changes = self.conn.total_changes
        self.assertIsNotNone(self.read(mid))
        self.assertTrue(moments.select_moment_impressions(self.conn, guild_id=1))
        self.assertEqual(self.conn.total_changes, changes)


if __name__ == '__main__':
    unittest.main()
