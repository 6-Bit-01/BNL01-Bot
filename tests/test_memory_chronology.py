"""Memory dates survive original-source retrieval, derivation and delivery checks."""
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from itertools import product
import os
import sqlite3
import unittest
from unittest import mock

import bnl_moment_engine as moments
import bnl_memory_ledger as ledger
from bnl_shared_brain_synthesis import render_packet_context
from bnl_unified_intelligence_packet import build_packet, revalidate_packet
import test_moment_meaning as meaning
import test_retained_resume_context as retained
import test_public_network_knowledge as network


class RetainedMemoryChronologyTests(unittest.TestCase):
    def setUp(self):
        self.fixture = retained.RetainedResumeContextTests()
        self.fixture.now = datetime(2026, 9, 23, 6, 55, tzinfo=timezone.utc)
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.mid, self.roots = self.fixture.seed(
            200, datetime(2026, 9, 22, 2, tzinfo=timezone.utc),
        )
        self.fixture.conn.commit()

    def test_evening_exchange_uses_pacific_date_for_named_iso_and_relative_recall(self):
        for date in ('September 21, 2026', '2026-09-21', 'yesterday'):
            with self.subTest(date=date):
                text, result = self.fixture.context(current_texts=(retained.QUERY + ' From ' + date + '.',))
                self.assertIn('Playback is still unconfirmed', text)
                self.assertEqual(result.retained_moment_ids, (self.mid,))
                self.assertEqual(set(result.selected_row_ids), {200, 201, 202})
        text, result = self.fixture.context(current_texts=(retained.QUERY + ' From September 22, 2026.',))
        self.assertEqual(text, '')
        self.assertEqual(result.retained_moment_ids, ())

    def test_relative_request_keeps_its_date_during_midnight_delivery_revalidation(self):
        text, result = self.fixture.context(current_texts=(retained.QUERY + ' From yesterday.',))
        self.assertEqual(result.retained_moment_ids, (self.mid,))
        basis = self.fixture.bot.build_conversation_prompt_source_basis(
            text, guild_id=1, current_user_id=7, channel_id=10,
            channel_name='bnl-testing', channel_policy='sealed_test', context_result=result,
        )
        # Restart/reopen and a provider wait crossing Pacific midnight must not
        # reinterpret the same request. Source controls still use current rows.
        self.fixture.conn.close()
        self.fixture.conn = sqlite3.connect(self.fixture.db_file)
        self.addCleanup(self.fixture.conn.close)
        after_midnight = '2026-09-23T07:01:00+00:00'
        with mock.patch.object(moments, '_now', return_value=after_midnight):
            self.assertFalse(self.fixture.bot.refresh_prompt_source_basis(basis)[1])
            self.fixture.conn.execute("UPDATE memory_ledger_entries SET lifecycle_status='retracted' WHERE entry_id=?", (self.roots[0],))
            self.fixture.conn.commit()
            self.assertTrue(self.fixture.bot.refresh_prompt_source_basis(basis)[1])

    def test_resumed_transcript_carries_the_original_exchange_date(self):
        text, result = self.fixture.context(current_texts=(retained.QUERY + ' From 2026-09-21.',))
        self.assertEqual(result.retained_moment_ids, (self.mid,))
        self.assertIn('Conversation dates (Pacific): 2026-09-21', text)
        self.assertNotIn('Conversation dates (Pacific): 2026-09-23', text)

    def test_date_boundaries_use_the_source_interval_and_dst_calendar(self):
        cases = (
            # Spring and fall changes still use calendar days, not 24 hours.
            ('yesterday', '2026-03-08T07:30:00Z', '2026-03-09T06:30:00Z', True),
            ('yesterday', '2026-11-01T06:30:00Z', '2026-11-02T07:30:00Z', True),
            ('December 31, 2025', '2026-01-01T07:59:59Z', '2026-01-02T10:00:00Z', True),
            ('2026-09-22 UTC', '2026-09-22T02:00:00Z', '2026-09-23T06:55:00Z', True),
            ('September 21', '2026-09-22 02:00:00', '2026-09-23T06:55:00Z', True),
            ('February 30, 2026', '2026-03-01T01:00:00Z', '2026-03-02T10:00:00Z', False),
            ('yesterday', 'invalid', '2026-09-23T06:55:00Z', False),
        )
        for query, observed, now, expected in cases:
            with self.subTest(query=query, observed=observed):
                self.assertEqual(moments._resume_date_matches(query, observed, now), expected)
        self.assertTrue(moments._resume_date_matches(
            'September 21', '2026-09-22T07:00:10Z', '2026-09-23T10:00:00Z',
            started_at='2026-09-22T06:59:50Z'))

    def test_new_turn_reresolves_yesterday_and_invalidates_missing_originals(self):
        # The next request gets a fresh calendar; the previous request stays pinned.
        next_day = datetime(2026, 9, 23, 7, 1, tzinfo=timezone.utc)
        text, result = self.fixture.context(now=next_day, current_texts=(retained.QUERY + ' From yesterday.',))
        self.assertEqual(text, '')
        self.assertEqual(result.retained_moment_ids, ())
        self.fixture.conn.execute('DELETE FROM conversations WHERE id=200')
        self.fixture.conn.commit()
        self.assertEqual(self.fixture.context(current_texts=(retained.QUERY + ' From September 21.',))[0], '')


class DerivedMemoryChronologyTests(unittest.TestCase):
    def setUp(self):
        self.fixture = meaning.MomentMeaningTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.first, self.roots = self.fixture.captured_moment(
            started_at=datetime(2026, 9, 22, 2, tzinfo=timezone.utc),
        )
        self.fixture.enrich()
        self.second, _ = self.fixture.captured_moment(
            channel=20, started_at=datetime(2026, 9, 23, 2, tzinfo=timezone.utc),
        )
        self.fixture.enrich()

    def test_dated_recall_filters_before_duplicate_gists_and_preserves_participant_scope(self):
        for reader, extra in (
            (moments.select_public_situation_moment_gists, {}),
            (moments.select_public_participant_moment_gists, {'participant_key': 'discord_user:1'}),
        ):
            with self.subTest(reader=reader.__name__):
                rows = reader(self.fixture.conn, guild_id=1,
                    topic_text='Recall the Test Reporters journalists discussion from September 21, 2026.',
                    broad_recall=True, allowed_channel_policies=('public_home',), **extra)
                self.assertEqual(tuple(row.moment_id for row in rows), (self.first,))

    def test_packet_keeps_event_date_separate_from_later_meaning_save(self):
        request = replace(self.fixture.packet().request,
            user_text='Return to the Test Reporters journalists discussion from September 21, 2026.',
            frame_event_relation='resume', now='2026-09-23T06:55:00+00:00')
        packet = build_packet(self.fixture.conn, request, environ=self.fixture.flags)
        rows = [item for item in packet.items if item.lane == 'episode']
        self.assertEqual(tuple(row.event_ref for row in rows), (self.first,))
        rendered, *_ = render_packet_context(packet)
        self.assertIn('conversation last activity 2026-09-21T19:00:50-07:00', rendered)
        self.assertIn('not the time of a later memory revision', rendered)
        with mock.patch.object(moments, '_now', return_value='2026-09-23T07:01:00+00:00'):
            self.assertTrue(revalidate_packet(self.fixture.conn, packet, environ=self.fixture.flags).valid)
            self.fixture.conn.execute("UPDATE memory_ledger_entries SET normalized_value='A corrected source.' WHERE entry_id=?", (self.roots[0],))
            self.assertFalse(revalidate_packet(self.fixture.conn, packet, environ=self.fixture.flags).valid)

    def test_legacy_moment_reader_obeys_the_same_date_and_privacy_boundary(self):
        text = moments.render_shadow_moment_context(self.fixture.conn,
            guild_id=1, channel_id=10, topic_text='Test Reporters journalists September 21, 2026',
            token_budget=300, freshness_days=3650, visibility='public_safe',
            participant_key='discord_user:1', allow_cross_channel=True,
            allowed_channel_policies=('public_home',))
        self.assertIn('2026-09-21T19:00:50-07:00', text)
        self.assertNotIn('2026-09-22T19:00:50-07:00', text)
        self.fixture.conn.execute("UPDATE memory_ledger_entries SET visibility='private' WHERE entry_id=?", (self.roots[0],))
        self.assertEqual(moments.render_shadow_moment_context(self.fixture.conn,
            guild_id=1, channel_id=10, topic_text='Test Reporters journalists September 21, 2026',
            token_budget=300, freshness_days=3650, visibility='public_safe',
            participant_key='discord_user:1', allow_cross_channel=True,
            allowed_channel_policies=('public_home',)), '')


class MemoryChronologyDeliveryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.runtime = network.PublicNetworkKnowledgeTests()
        await self.runtime.asyncSetUp()
        self.addAsyncCleanup(self.runtime.asyncTearDown)
        self.bot = network.bnl01_bot
        self.runtime.stack.enter_context(mock.patch.object(self.bot, 'fetch_bnl_read_model', return_value={}))

    def flags(self, enabled, channel_id):
        return mock.patch.dict(os.environ, {
            'BNL_MEMORY_LEDGER_SHADOW_ENABLED': 'true',
            'BNL_MOMENT_ENGINE_SHADOW_ENABLED': 'true',
            'BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED': 'true',
            'BNL_RELATIONSHIP_V2_SHADOW_ENABLED': 'true',
            'BNL_UNIFIED_RESPONSE_ASSESSMENT_SHADOW_ENABLED': 'true',
            'BNL_UNIFIED_INTELLIGENCE_PACKET_SHADOW_ENABLED': 'true',
            'BNL_ORDINARY_CHAT_SINGLE_PACKET_ENABLED': str(enabled).lower(),
            'BNL_ORDINARY_CHAT_SINGLE_PACKET_PUBLIC_ENABLED': str(enabled).lower(),
            'BNL_ORDINARY_CHAT_SINGLE_PACKET_GUILD_IDS': str(self.runtime.guild_id),
            'BNL_ORDINARY_CHAT_SINGLE_PACKET_USER_IDS': str(self.runtime.user_id),
            'BNL_ORDINARY_CHAT_SINGLE_PACKET_CHANNEL_IDS': str(channel_id),
        })

    def seed(self, policy, channel_id):
        with sqlite3.connect(self.bot.DB_FILE) as conn:
            for index, text in enumerate(retained.SOURCES):
                uid = self.runtime.user_id + index % 2
                hour = 3 if policy == 'sealed_test' else 2
                timestamp = datetime(2026, 9, 22, hour, 0, index * 10, tzinfo=timezone.utc).isoformat()
                row_id = conn.execute(
                    'INSERT INTO conversations (guild_id,user_id,user_name,role,content,channel_id,channel_name,'
                    "channel_policy,route_mode,timestamp) VALUES (?,?,?,'user',?,?,?,?,'normal_chat',?)",
                    (self.runtime.guild_id, uid, 'Test Member', text, channel_id, 'test-room', policy, timestamp),
                ).lastrowid
                entry = ledger.shadow_conversation_row(conn, row_id=row_id,
                    guild_id=self.runtime.guild_id, user_id=uid, user_name='Test Member', role='user',
                    content=text, channel_id=channel_id, channel_name='test-room', channel_policy=policy,
                    route_mode='normal_chat', observed_at=timestamp)
                moments.observe_ledger_entry(conn, entry.entry_id)
            moments.sweep_expired_windows(conn, now=f'2026-09-22T{hour:02}:03:00+00:00')

    async def test_direct_and_batch_prompts_keep_dated_sources_with_packet_on_and_off(self):
        query = retained.QUERY + ' From September 21, 2026.'
        answer = 'The saved-record change remained unconfirmed; acknowledging the error was separate.'
        async def provider(*args, **kwargs):
            if kwargs.get('attempt_counter') is not None:
                kwargs['attempt_counter'].mark_started()
            return answer
        for policy, enabled in product(('public_home', 'sealed_test'), (False, True)):
            with self.subTest(policy=policy, packet=enabled), self.flags(enabled, 8810):
                self.seed(policy, 8810)
                prompt, metadata = await self.runtime._direct_prompt_async(policy, query, privileged=False)
                self.assertIn('Playback is still unconfirmed', prompt)
                self.assertIn('Conversation dates (Pacific): 2026-09-21', prompt)
                self.assertEqual(metadata['ordinary_chat_single_packet_applied'], enabled)
                channel_id = 8811 + len(self.runtime.channel_ids)
                self.seed(policy, channel_id)
                with self.flags(enabled, channel_id):
                    channel, generation, _guard = await self.runtime._batch(policy, query, provider, privileged=False)
                generation.assert_awaited_once()
                self.assertIn('Playback is still unconfirmed', generation.await_args.args[0])
                self.assertIn('2026-09-21', generation.await_args.args[0])
                self.assertEqual(channel.sent, [answer])


if __name__ == '__main__':
    unittest.main()
