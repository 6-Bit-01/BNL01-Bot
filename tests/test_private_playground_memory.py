"""Same existing memory owners, with channel-scoped private additions."""
import json
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock

os.environ.setdefault('GEMINI_API_KEY', 'test-gemini-key')
os.environ.setdefault('DISCORD_BOT_TOKEN', 'test-discord-token')

import bnl01_bot as bot
import bnl_memory_governance as governance
import bnl_memory_ledger as ledger
import bnl_moment_engine as moments
import bnl_relationship_engine as relationships
import bnl_unified_intelligence_packet as intelligence
import test_relationship_meaning as relationship_fixture
import test_moment_meaning as moment_fixture


class PrivatePlaygroundMemoryTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = str(Path(self.directory.name) / 'memory.db')
        for patch in (mock.patch.object(bot, 'DB_FILE', self.path), mock.patch.dict(os.environ, {
            'BNL_MEMORY_LEDGER_SHADOW_ENABLED': 'true',
            'BNL_MOMENT_ENGINE_SHADOW_ENABLED': 'true',
            'BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED': 'true',
            'BNL_RELATIONSHIP_V2_SHADOW_ENABLED': 'true',
            'BNL_RELATIONSHIP_V2_MEANING_SHADOW_ENABLED': 'true',
            'BNL_RELATIONSHIP_V2_MEANING_GUILD_IDS': '1',
            'BNL_RELATIONSHIP_V2_LIVE_ENABLED': 'false',
            'BNL_UNIFIED_INTELLIGENCE_PACKET_SHADOW_ENABLED': 'true',
        })):
            patch.start()
            self.addCleanup(patch.stop)
        bot.init_db()

    def save(self, text, policy='sealed_test', channel=10, user=2):
        return bot.save_user_message(user, 'Test Member', 1, text, channel_name='test-room',
                                    channel_policy=policy, channel_id=channel, directed_to_bnl=True)

    def read(self, policy='sealed_test', channel=10, user=2, text='What do you remember about me?'):
        with sqlite3.connect(self.path) as conn:
            request = governance.GovernanceRequest(1, user, 'normal_chat', 'discord_prompt_assembly',
                channel_id=channel, channel_policy=policy, user_text=text, broad_recall=True)
            return governance.build_governed_context(conn, request,
                private_fact_extractor=bot.extract_user_facts, include_public_moment_gists=True)

    def test_private_learning_survives_consolidation_reopen_and_public_pruning(self):
        self.save('Remember the amber synth arrangement for my upcoming music release.')
        self.save('The amber synth arrangement uses a slow distorted bass passage.')
        bot._consolidate_memory_tiers(2, 1, {'short': 0, 'medium': 0, 'long': 10})
        bot.prune_conversation_history(2, 1, 0)
        with sqlite3.connect(self.path) as conn:
            self.assertTrue(conn.execute("SELECT 1 FROM memory_tiers WHERE tier='long' AND source_channel_policy='sealed_test'").fetchone())
        self.assertIn('amber synth', self.read().rendered_context)
        for policy, channel, user in [('public_home', 20, 2), ('public_context', 10, 2),
                                     ('sealed_test', 11, 2), ('sealed_test', 10, 3)]:
            with self.subTest(policy=policy, channel=channel, user=user):
                self.assertNotIn('amber synth', self.read(policy, channel, user).rendered_context)

    def test_private_consolidation_cannot_merge_or_evict_public_or_other_room(self):
        self.save('Remember the public amber arrangement for the album.', 'public_home', 20)
        self.save('Remember the private silver arrangement for the album.', channel=11)
        with sqlite3.connect(self.path) as conn:
            before = conn.execute('SELECT id,summary FROM memory_tiers ORDER BY id').fetchall()
        self.save('Remember the secret copper arrangement for the album.', channel=10)
        bot._consolidate_memory_tiers(2, 1, {'short': 0, 'medium': 0, 'long': 1}, private_channel_id=10)
        with sqlite3.connect(self.path) as conn:
            after = conn.execute('SELECT id,summary FROM memory_tiers WHERE id<=? ORDER BY id', (before[-1][0],)).fetchall()
        self.assertEqual(before, after)
        self.assertNotIn('silver', self.read().rendered_context)

    def test_private_preference_correction_keeps_public_fact_and_live_state_unchanged(self):
        self.save('My favorite color is blue.', 'public_home', 20)
        with sqlite3.connect(self.path) as conn:
            before = {t: conn.execute('SELECT * FROM ' + t).fetchall()
                      for t in ('user_memory_facts', 'relationship_state', 'relationship_state_v2', 'user_habits')}
        self.save('Actually, my favorite color is green now.')
        self.save('Please stop teasing me about that color.')
        private = self.read(text='What is my favorite color?')
        self.assertIn('favorite color: green', private.rendered_context)
        self.assertFalse(governance.assess_governance_result_safety(private).unsafe)
        self.assertIn('blue', self.read('public_home', 20, text='What is my favorite color?').rendered_context)
        with sqlite3.connect(self.path) as conn:
            for table, rows in before.items():
                self.assertEqual(rows, conn.execute('SELECT * FROM ' + table).fetchall(), table)

    def test_private_source_correction_and_deletion_withdraw_derived_memory(self):
        self.save('Remember the violet bass arrangement for the next release.')
        self.assertIn('violet bass', self.read().rendered_context)
        with sqlite3.connect(self.path) as conn:
            conn.execute("UPDATE conversations SET content='This source has been corrected.' WHERE channel_id=10")
        self.assertNotIn('violet bass', self.read().rendered_context)
        self.save('Remember the turquoise drums arrangement for the next release.')
        self.assertIn('turquoise drums', self.read().rendered_context)
        with sqlite3.connect(self.path) as conn:
            conn.execute('DELETE FROM conversations WHERE channel_id=10')
        self.assertNotIn('turquoise drums', self.read().rendered_context)

    def test_private_room_reads_existing_public_fact(self):
        self.save('My favorite movie is Moon.', 'public_home', 20)
        self.assertIn('Moon', self.read().rendered_context)

    def test_private_tiers_reach_only_their_room_even_for_privileged_internal_reads(self):
        self.save('Remember the violet bass arrangement for my upcoming album.')
        for governed in (False, True):
            with mock.patch.dict(os.environ, {'BNL_MEMORY_GOVERNANCE_LIVE_ENABLED': str(governed).lower()}):
                for policy, channel in [('sealed_test', 10), ('sealed_test', 11),
                                        ('public_home', 20), ('internal_controlled', 20)]:
                    with self.subTest(governed=governed, policy=policy, channel=channel):
                        context = bot.build_user_memory_context(2, 1, channel_policy=policy,
                            channel_id=channel, user_text='What do you remember about the violet bass arrangement?',
                            is_owner_or_mod=True, current_direct=True, record_operational_diagnostics=False)
                        self.assertEqual('violet bass' in context, policy == 'sealed_test' and channel == 10)

    def test_shared_packet_uses_private_sources_and_revalidates_their_roots(self):
        self.save('Remember my amber synth arrangement for the upcoming music release.')
        self.save('My favorite color is green.')
        request = intelligence.IntelligencePacketRequest(guild_id=1, subject_user_id=2,
            conversation_surface='discord_prompt_assembly',
            channel_id=10, channel_policy='sealed_test', visibility_allowance='sealed_test',
            route_mode='normal_chat', user_text='What do you remember about me?',
            direct_state='direct', participant_user_ids=(2,))
        with sqlite3.connect(self.path) as conn:
            packet = intelligence.build_packet(conn, request, persist=False)
            self.assertIsNotNone(packet)
            self.assertFalse(packet.diagnostics.invalid_invariants)
            self.assertFalse(packet.diagnostics.processing_errors)
            self.assertTrue(any(item.revalidation_kind == 'sealed_memory' for item in packet.items))
            self.assertTrue(any('favorite color: green' in item.text for item in packet.items))
            self.assertTrue(intelligence.revalidate_packet(conn, packet).valid)
            conn.execute('DELETE FROM conversations WHERE channel_id=10')
            self.assertFalse(intelligence.revalidate_packet(conn, packet).valid)

    def test_real_prompt_and_send_fence_withdraw_deleted_private_sources(self):
        self.save('Remember the violet bass arrangement for my upcoming album.')
        text = 'What do you remember about the violet bass arrangement?'
        for governed in (False, True):
            with self.subTest(governed=governed), mock.patch.dict(os.environ, {
                'BNL_MEMORY_GOVERNANCE_LIVE_ENABLED': str(governed).lower(),
            }):
                context = bot.build_user_memory_context(2, 1, channel_policy='sealed_test',
                    channel_id=10, user_text=text, current_direct=True, record_operational_diagnostics=False)
                self.assertIn('violet bass', context)
                basis = bot.build_memory_prompt_source_basis(context, user_id=2, guild_id=1,
                    route_mode='normal_chat', channel_policy='sealed_test', user_text=text,
                    is_owner_or_mod=False, current_direct=True, governance_allowed=True,
                    channel_id=10, moment_attribution_target_user_id=0)
                self.assertIsNotNone(basis)
                with sqlite3.connect(self.path) as conn:
                    conn.execute('UPDATE conversations SET content=? WHERE channel_id=10', ('Source removed.',))
                fresh, changed = bot.refresh_prompt_source_basis(basis)
                self.assertTrue(changed)
                self.assertNotIn('violet bass', fresh.rendered_context)
                self.save('Remember the violet bass arrangement for my upcoming album.')

    def test_private_tone_and_habits_use_public_reducers_without_writing_public_state(self):
        self.save('I am arranging a new music release.', 'public_home', 20)
        self.save('I am arranging a new music release.', 'public_home', 20, user=3)
        public_relation = bot.get_relationship_state(2, 1)
        public_habits = bot.get_user_habits(2, 1)
        self.save('Thank you for helping with my music. What did you think?')
        self.save('Thank you for helping with my music. What did you think?', 'public_home', 20, user=3)
        # Compare actual public capture/read with private capture/read under
        # the same starting history, excluding only their capture timestamps.
        self.assertEqual(bot.get_relationship_state(2, 1, private_channel_id=10)[:-1],
                         bot.get_relationship_state(3, 1)[:-1])
        self.assertEqual(bot.get_user_habits(2, 1, private_channel_id=10)[:-1],
                         bot.get_user_habits(3, 1)[:-1])
        self.assertEqual(bot.get_relationship_state(2, 1), public_relation)
        self.assertEqual(bot.get_user_habits(2, 1), public_habits)
        self.assertEqual(bot.get_relationship_state(2, 1, private_channel_id=11), public_relation)
        with sqlite3.connect(self.path) as conn:
            conn.execute('DELETE FROM conversations WHERE channel_id=10')
        self.assertEqual(bot.get_relationship_state(2, 1, private_channel_id=10), public_relation)
        self.assertEqual(bot.get_user_habits(2, 1, private_channel_id=10), public_habits)

    def test_private_activity_cannot_evict_public_history_or_queue_source_refresh(self):
        self.save('Hello', 'public_home', 20)
        with mock.patch.object(bot, 'mark_subject_dirty_for_evidence') as refresh:
            self.save('Hello privately')
            self.save('Goodbye privately')
            refresh.assert_not_called()
        bot.prune_conversation_history(2, 1, 1)
        with sqlite3.connect(self.path) as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM conversations WHERE channel_id=20").fetchone()[0], 1)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM conversations WHERE channel_id=10").fetchone()[0], 1)


class PrivateRelationshipTests(unittest.TestCase):
    def setUp(self):
        self.fixture = relationship_fixture.RelationshipMeaningTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)

    def test_private_interpretation_has_separate_report_and_no_public_preferences(self):
        f = self.fixture
        f.observe('Please stop teasing me.', policy='sealed_test')
        request = relationships.claim_relationship_meaning(f.conn)
        self.assertIsNotNone(request)
        self.assertTrue(relationships.finish_relationship_meaning(f.conn, request,
                        text=f.result('boundary', 'Please stop teasing me.')))
        self.assertEqual(relationships.relationship_meaning_report(f.conn)['current_results'], 0)
        self.assertEqual(relationships.relationship_meaning_report(f.conn, private_channel_id=10)['current_results'], 1)
        self.assertEqual(f.conn.execute('SELECT COUNT(*) FROM relationship_state_v2').fetchone()[0], 0)
        self.assertEqual(f.conn.execute('SELECT COUNT(*) FROM relationship_member_preferences_v2').fetchone()[0], 0)

    def test_private_relationship_rebuild_does_not_change_public_state(self):
        f = self.fixture
        f.observe('Thank you for helping me.')
        before = f.conn.execute('SELECT * FROM relationship_state_v2').fetchall()
        f.observe('Do not joke about my music.', policy='sealed_test')
        private = relationships.rebuild_state(f.conn, guild_id=1, subject_user_id=2, private_channel_id=10)
        public = relationships.rebuild_state(f.conn, guild_id=1, subject_user_id=2)
        self.assertGreater(private['evidence_counts'].get('boundary', 0), 0)
        self.assertEqual(public['evidence_counts'].get('boundary', 0), 0)
        self.assertEqual(len(before), 1)


class PrivateMomentTests(unittest.TestCase):
    def test_private_meaning_uses_same_interpreter_and_remains_private(self):
        f = moment_fixture.MomentMeaningTests()
        f.setUp()
        self.addCleanup(f.doCleanups)
        moment_id, _roots = f.captured_moment(policy='sealed_test')
        request = moments.claim_pending_moment_meaning(f.conn, guild_ids=(1,))
        self.assertIsNotNone(request)
        self.assertTrue(moments.apply_moment_meaning(f.conn, request, json.dumps(moment_fixture.REPORTER_MEANING)))
        row = f.conn.execute('SELECT public_usable,meaning_status FROM memory_moment_windows WHERE moment_id=?', (moment_id,)).fetchone()
        self.assertEqual(row, (0, 'ready'))
        private = moments.render_shadow_moment_context(f.conn, guild_id=1, channel_id=10,
            participant_key='discord_user:1', visibility='sealed_test', topic_text='reporters', freshness_days=3650)
        self.assertTrue(private)
        request = governance.GovernanceRequest(1, 1, 'normal_chat', 'discord_prompt_assembly',
            channel_id=10, channel_policy='sealed_test', user_text='Tell me about Test Reporters', broad_recall=True)
        selected = governance.build_governed_context(f.conn, request, include_public_moment_gists=True)
        self.assertTrue(any(c.source_type == 'sealed_moment' for c in selected.selected))
        self.assertFalse(governance.assess_governance_result_safety(selected).unsafe)
        for scope in (0, 11):
            self.assertFalse(moments.select_public_participant_moment_gists(f.conn, guild_id=1,
                participant_key='discord_user:1', broad_recall=True, allowed_channel_policies=('public_home',),
                private_channel_id=scope))
        public = moments.render_shadow_moment_context(f.conn, guild_id=1, channel_id=10,
            participant_key='discord_user:1', visibility='public_safe', topic_text='reporters', freshness_days=3650)
        self.assertFalse(public)
