"""Private semantic tone composes with existing evidence and member controls."""
import json
import os
import sqlite3
import unittest
from datetime import datetime, timedelta, timezone
from unittest import mock

import bnl_memory_ledger as ledger
import bnl_relationship_engine as rel


class RelationshipMeaningCanaryTests(unittest.TestCase):
    def setUp(self):
        self.env = {
            rel.SHADOW_ENV: '1', rel.MEANING_SHADOW_ENV: '1', rel.SEALED_CANARY_ENV: '1',
            'BNL_MEMORY_LEDGER_SHADOW_ENABLED': '1',
            'BNL_MOMENT_ENGINE_SHADOW_ENABLED': '1',
            'BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED': '1',
            'BNL_RELATIONSHIP_V2_MEANING_GUILD_IDS': '1',
            'BNL_RELATIONSHIP_V2_SEALED_CANARY_GUILD_IDS': '1',
            'BNL_RELATIONSHIP_V2_SEALED_CANARY_CHANNEL_IDS': '99',
            'BNL_RELATIONSHIP_V2_SEALED_CANARY_USER_IDS': '2',
            rel.LIVE_ENV: '0', rel.ACTIVE_ENGAGEMENT_LIVE_ENV: '0',
            'BNL_MEMORY_GOVERNANCE_LIVE_ENABLED': '0',
        }
        patch = mock.patch.dict(os.environ, self.env)
        patch.start()
        self.addCleanup(patch.stop)
        self.new_database()

    def new_database(self):
        self.conn = sqlite3.connect(':memory:')
        self.addCleanup(self.conn.close)
        ledger.ensure_memory_ledger_schema(self.conn)
        self.conn.execute('''CREATE TABLE conversations (
            id INTEGER PRIMARY KEY,user_id INTEGER,guild_id INTEGER,channel_id INTEGER,
            channel_policy TEXT,route_mode TEXT,role TEXT,content TEXT,timestamp TEXT)''')
        self.conn.execute('''CREATE TABLE conversation_response_participants (
            conversation_row_id INTEGER,guild_id INTEGER,user_id INTEGER)''')
        rel.ensure_relationship_v2_schema(self.conn)
        self.index = 0

    def source(self, text, *, role='user', uid=2, guild=1, policy='sealed_test',
               channel=99, route='normal_chat'):
        self.index += 1
        rid = self.index
        stamp = (datetime(2026, 9, 28, tzinfo=timezone.utc) + timedelta(seconds=rid)).isoformat()
        self.conn.execute('INSERT INTO conversations VALUES (?,?,?,?,?,?,?,?,?)',
            (rid, uid, guild, channel, policy, route, role, text, stamp))
        root = ledger.shadow_conversation_row(self.conn, row_id=rid, user_id=uid,
            user_name='Test Member', guild_id=guild, role=role, content=text,
            channel_id=channel, channel_policy=policy, route_mode=route, observed_at=stamp).entry_id
        return rid, root

    def observe(self, text, **kwargs):
        rid, root = self.source(text, **kwargs)
        rel.observe_message(self.conn, guild_id=kwargs.get('guild', 1), user_id=kwargs.get('uid', 2),
            role=kwargs.get('role', 'user'), content=text, source_row_id=rid,
            channel_policy=kwargs.get('policy', 'sealed_test'), channel_id=kwargs.get('channel', 99),
            route_mode=kwargs.get('route', 'normal_chat'), directed=True,
            observed_at='2026-09-28T00:00:00+00:00')
        return rid, root

    def complete(self, text, types, **kwargs):
        rid, root = self.observe(text, **kwargs)
        request = rel.claim_relationship_meaning(self.conn)
        self.assertIsNotNone(request)
        self.assertEqual(request.target_text, text)
        self.assertTrue(rel.finish_relationship_meaning(self.conn, request,
            text=json.dumps({'signals': [{'type': kind, 'quote': text} for kind in types]})))
        return rid, root

    def state(self, **kwargs):
        args = dict(guild_id=1, subject_user_id=2, private_channel_id=99,
                    evaluated_at='2026-09-28T00:01:00+00:00', meaning_canary=True)
        args.update(kwargs)
        return rel.rebuild_state(self.conn, **args)

    def tone(self, **kwargs):
        args = dict(guild_id=1, user_id=2, channel_id=99, channel_policy='sealed_test',
                    route_mode='normal_chat', direct=True)
        args.update(kwargs)
        return rel.governed_summary(self.conn, **args)

    def stored(self):
        tables = [row[0] for row in self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]
        return {name: list(self.conn.execute('SELECT * FROM ' + name + ' ORDER BY rowid'))
                for name in tables}

    def test_public_and_exact_private_paraphrases_layer_without_persisting_scores(self):
        self.complete('That solved it; I can finally move forward.', ['support_received_accepted'],
                      policy='public_home', channel=10)
        self.source('BNL is appreciated and trusted.', role='model')
        self.complete('We can put that behind us.', ['repair_accepted'])
        before = self.stored()
        state = self.state()
        self.assertEqual(state['evidence_counts'], {'support_received_accepted': 1, 'repair_accepted': 1})
        self.assertGreater(state['repair'], .11)
        self.assertGreater(state['support'], .08)
        self.assertIn('Allow repair', self.tone())
        self.assertEqual(before, self.stored())
        self.assertEqual(self.state(meaning_canary=False)['evidence_counts'], {})
        public = self.state(private_channel_id=0)
        self.assertEqual(public['evidence_counts'], {})
        self.assertNotIn('meaning_basis_digest', public)

    def test_semantic_replacement_and_ready_empty_remove_lexical_false_positives(self):
        self.complete('Thanks for the wormhole chaos, I guess.', ['playful_exchange'],
                      policy='public_home', channel=10)
        self.complete('The reviewer wrote thanks, but I am only quoting them.', [])
        lexical = self.state(meaning_canary=False)
        self.assertEqual(lexical['evidence_counts'], {'appreciation': 2})
        before = self.stored()
        self.assertEqual(self.state()['evidence_counts'], {'playful_exchange': 1})
        self.assertEqual(before, self.stored())
        self.assertEqual(self.state(private_channel_id=0)['evidence_counts'], {'appreciation': 1})

    def test_one_target_signal_once_despite_multiple_events_and_receipt_versions(self):
        rid, _ = self.complete('Thanks, that solved it.', ['appreciation', 'support_received_accepted'])
        rel.record_event(self.conn, rel.RelationshipEventV2(1, 2, 'user', 'follow_up', 'user_to_bnl',
            'conversations', rid, route_mode='normal_chat', channel_id=99, channel_policy='sealed_test',
            lifecycle='review_only', observed_at='2026-09-28T00:00:03+00:00'))
        columns = [row[1] for row in self.conn.execute('PRAGMA table_info(relationship_meaning_v2)')]
        values = ['?' if name in {'receipt_id', 'version', 'semantic_types_json'} else name for name in columns]
        self.conn.execute('INSERT INTO relationship_meaning_v2 (' + ','.join(columns) + ') SELECT '
            + ','.join(values) + ' FROM relationship_meaning_v2',
            ('old-receipt', 'unselected-older-version', '["friction"]'))
        self.assertEqual(self.state()['evidence_counts'], {'appreciation': 1, 'support_received_accepted': 1})

    def test_unavailable_interpretation_uses_only_current_lexical_fallback(self):
        for status in ('missing', 'pending', 'budget_deferred', 'provider_unavailable', 'interrupted'):
            with self.subTest(status=status):
                self.new_database()
                self.observe('Thanks for helping.')
                if status == 'missing':
                    self.conn.execute('DELETE FROM relationship_meaning_v2')
                else:
                    self.conn.execute('UPDATE relationship_meaning_v2 SET status=?', (status,))
                self.assertEqual(self.state()['evidence_counts'], {'appreciation': 1})
                self.conn.execute("UPDATE conversations SET content='A changed observation.'")
                self.assertEqual(self.state()['evidence_counts'], {})

    def test_invalid_receipt_does_not_restore_old_lexical_label(self):
        invalid = ('["appreciation", "appreciation"]', '["explicit_engagement_opt_in"]',
                   '["model_playful_rivalry_acceptance"]', 'null', '{"type":"appreciation"}')
        for types in invalid:
            with self.subTest(types=types):
                self.new_database()
                self.complete('Thanks, I guess.', ['playful_exchange'])
                self.conn.execute('UPDATE relationship_meaning_v2 SET semantic_types_json=?', (types,))
                self.assertEqual(self.state()['evidence_counts'], {})

    def test_target_and_context_changes_remove_meaning_without_lexical_resurrection(self):
        for target in (False, True):
            for mutation in ('delete', 'edit', 'privacy', 'subject', 'policy', 'route'):
                with self.subTest(target=target, mutation=mutation):
                    self.new_database()
                    prior, prior_root = self.source('I misunderstood the request.', role='model')
                    rid, root = self.complete('Thanks, I guess.', ['playful_exchange'])
                    self.assertEqual(self.state()['evidence_counts'], {'playful_exchange': 1})
                    changed, changed_root = (rid, root) if target else (prior, prior_root)
                    if mutation == 'delete':
                        self.conn.execute('DELETE FROM conversations WHERE id=?', (changed,))
                    elif mutation == 'privacy':
                        self.conn.execute("UPDATE memory_ledger_entries SET visibility='private' WHERE entry_id=?", (changed_root,))
                    else:
                        column, value = {'edit': ('content', 'Changed source'), 'subject': ('user_id', 3),
                            'policy': ('channel_policy', 'admin'), 'route': ('route_mode', 'ambient')}[mutation]
                        self.conn.execute('UPDATE conversations SET ' + column + '=? WHERE id=?', (value, changed))
                    self.assertEqual(self.state()['evidence_counts'], {})

    def test_full_digest_revalidation_rejects_stale_receipt_even_without_triggers(self):
        prior, _ = self.source('Earlier context.', role='model')
        self.complete('Thanks, I guess.', ['playful_exchange'])
        self.conn.execute('DROP TRIGGER rel_meaning_conversation_update_v1')
        self.conn.execute("UPDATE conversations SET content='A different earlier context.' WHERE id=?", (prior,))
        self.assertEqual(self.conn.execute('SELECT status FROM relationship_meaning_v2').fetchone()[0], 'ready')
        self.assertEqual(self.state()['evidence_counts'], {})

    def test_bare_correction_retires_context_receipt_permanently_and_is_guild_scoped(self):
        _, root = self.source('Earlier context.', role='model')
        self.complete('Thanks, I guess.', ['playful_exchange'])
        self.conn.execute("INSERT INTO memory_ledger_lineage VALUES ('foreign-correction',3,'correction_of',?,'2026-09-28')", (root,))
        self.assertEqual(self.state()['evidence_counts'], {'playful_exchange': 1})
        self.conn.execute("INSERT INTO memory_ledger_lineage VALUES ('correction',1,'correction_of',?,'2026-09-28')", (root,))
        self.assertEqual(self.state()['evidence_counts'], {})
        self.conn.execute("DELETE FROM memory_ledger_lineage WHERE entry_id='correction'")
        self.assertEqual(self.conn.execute('SELECT status FROM relationship_meaning_v2').fetchone()[0], 'source_invalidated')
        self.assertEqual(self.state()['evidence_counts'], {})

    def test_upgrade_retires_preexisting_bare_correction_with_v1_trigger_present(self):
        _, root = self.complete('Thanks, I guess.', ['playful_exchange'])
        self.conn.execute('DROP TRIGGER rel_meaning_lineage_correction_v2')
        self.conn.execute("INSERT INTO memory_ledger_lineage VALUES ('old-correction',1,'correction_of',?,'2026-09-28')", (root,))
        self.assertEqual(self.conn.execute('SELECT status FROM relationship_meaning_v2').fetchone()[0], 'ready')
        rel.ensure_relationship_v2_schema(self.conn)
        self.conn.execute("DELETE FROM memory_ledger_lineage WHERE entry_id='old-correction'")
        self.assertEqual(self.conn.execute('SELECT status FROM relationship_meaning_v2').fetchone()[0], 'source_invalidated')
        self.assertEqual(self.state()['evidence_counts'], {})

    def test_other_member_guild_private_room_and_forged_scope_cannot_contribute(self):
        self.complete('That solved it.', ['support_received_accepted'], uid=3)
        self.complete('That solved it.', ['support_received_accepted'], channel=100)
        with mock.patch.dict(os.environ, {'BNL_RELATIONSHIP_V2_MEANING_GUILD_IDS': '1,3'}):
            self.complete('That solved it.', ['support_received_accepted'], guild=3)
        self.assertEqual(self.state()['evidence_counts'], {})
        self.conn.execute('UPDATE relationship_meaning_v2 SET scope_channel_id=0 WHERE scope_channel_id=100')
        self.assertEqual(self.state()['evidence_counts'], {})

    def test_explicit_controls_stay_authoritative_and_semantics_never_grant_consent(self):
        self.complete("Don't ping me.", [])
        self.complete('Enable playful rivalry.', ['playful_exchange'])
        self.complete('An absurd little robot duel.', ['playful_exchange'])
        before = self.stored()
        state = self.state()
        self.assertTrue(state['engagement_opt_out'])
        self.assertEqual(state['rivalry_state'], 'neutral')
        self.assertEqual(state['evidence_counts']['explicit_engagement_opt_out'], 1)
        self.assertEqual(state['evidence_counts']['explicit_relationship_mode_preference'], 1)
        self.assertEqual(self.conn.execute('SELECT COUNT(*) FROM relationship_member_preferences_v2').fetchone()[0], 0)
        self.assertIn('No proactive recognition', self.tone())
        self.assertEqual(before, self.stored())

    def test_boundaries_and_reconciliation_are_separate_from_permission(self):
        self.complete('Enough with the jokes at my expense.', ['boundary'])
        self.complete('We can put that behind us.', ['repair_accepted'])
        state = self.state()
        self.assertEqual(state['evidence_counts'], {'boundary': 1, 'repair_accepted': 1})
        self.assertEqual(state['rivalry_state'], 'neutral')
        self.assertEqual(state['boundary_alignment'], -.15)
        self.assertGreater(state['repair'], .11)
        self.assertIn('Allow repair', self.tone())
        self.assertNotIn('Rivalry only', self.tone())

    def test_single_boundary_changes_tone_and_needs_explicit_reopening(self):
        before = self.tone()
        self.complete('Enough with the jokes at my expense.', ['boundary'])
        self.assertNotEqual(before, self.tone())
        self.assertIn("Respect the member's stated limits", self.tone())
        self.assertTrue(self.state()['active_boundary'])
        self.complete('We can put that behind us.', ['repair_accepted'])
        self.complete('I see you kept your promise to stop.', ['boundary_respected'])
        self.assertTrue(self.state()['active_boundary'])
        self.assertIn("Respect the member's stated limits", self.tone())
        self.complete('You can joke again.', ['boundary_respected'])
        self.assertFalse(self.state()['active_boundary'])
        self.assertNotIn("Respect the member's stated limits", self.tone())
        self.assertNotEqual(self.state()['rivalry_state'], 'mutual_rivalry')

    def test_new_boundary_overrides_older_reopenings_and_blocks_rivalry(self):
        self.complete('Enable playful rivalry.', [])
        rel.record_model_playful_rivalry_acceptance(self.conn, guild_id=1, user_id=2,
            source_row_id=900, observed_at='2026-09-28T00:00:00+00:00')
        self.complete('An absurd little robot duel.', ['playful_exchange'])
        self.complete('The toaster wins the duel this time.', ['playful_exchange'])
        self.complete('You can joke again.', ['boundary_respected'])
        self.complete('It is ok to tease again.', ['boundary_respected'])
        self.assertEqual(self.state()['rivalry_state'], 'mutual_rivalry')
        self.complete('Enough with the jokes at my expense.', ['boundary'])
        self.assertTrue(self.state()['active_boundary'])
        self.assertEqual(self.state()['rivalry_state'], 'neutral')
        self.assertIn("Respect the member's stated limits", self.tone())
        self.assertNotIn('Rivalry only', self.tone())

    def test_ready_empty_or_boundary_overrides_misleading_lexical_reopening(self):
        for types in ([], ['boundary']):
            with self.subTest(types=types):
                self.new_database()
                self.complete('Enough with the jokes at my expense.', ['boundary'])
                self.complete('You can joke was a quotation, not permission.', types)
                self.assertTrue(self.state()['active_boundary'])
                self.assertIn("Respect the member's stated limits", self.tone())

    def test_explicit_scoped_authority_does_not_require_global_governance(self):
        self.complete('Thanks for ignoring my request.', ['friction'])
        self.assertIn('Allow repair', self.tone(governance_allowed=False))
        self.assertNotIn('neutral-warm', self.tone(governance_allowed=False))

    def test_retired_lexical_boundary_and_reopening_cannot_change_private_limits(self):
        self.observe('Do not tease the drummer.')
        self.conn.execute("UPDATE relationship_events_v2 SET lifecycle='forgotten'")
        self.assertEqual(self.state()['evidence_counts'], {})
        self.assertFalse(self.state()['active_boundary'])
        self.new_database()
        self.complete('Enough with the jokes at my expense.', ['boundary'])
        rid, _ = self.observe('You can joke again.')
        self.conn.execute("UPDATE relationship_events_v2 SET lifecycle='forgotten' WHERE source_row_id=?", (str(rid),))
        self.assertEqual(self.state()['evidence_counts'], {'boundary': 1})
        self.assertTrue(self.state()['active_boundary'])

    def test_scope_gates_and_rollback_restore_existing_path(self):
        self.complete('Thanks, I guess.', ['playful_exchange'])
        lexical = self.state(meaning_canary=False)
        self.assertEqual(self.state()['evidence_counts'], {'playful_exchange': 1})
        changes = {rel.SEALED_CANARY_ENV: '0', rel.MEANING_SHADOW_ENV: '0',
            'BNL_RELATIONSHIP_V2_MEANING_GUILD_IDS': '3',
            'BNL_RELATIONSHIP_V2_SEALED_CANARY_USER_IDS': '3',
            'BNL_RELATIONSHIP_V2_SEALED_CANARY_CHANNEL_IDS': '100',
            'BNL_MEMORY_LEDGER_SHADOW_ENABLED': '0', 'BNL_MOMENT_ENGINE_SHADOW_ENABLED': '0',
            'BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED': '0', rel.LIVE_ENV: '1',
            'BNL_ACTIVE_ENGAGEMENT_V2_LIVE_ENABLED': '1', 'BNL_MEMORY_GOVERNANCE_LIVE_ENABLED': '1'}
        before = self.stored()
        for key, value in changes.items():
            with self.subTest(key=key), mock.patch.dict(os.environ, {key: value}):
                self.assertEqual(self.state(), lexical)
        for overrides in ({'direct': False}, {'target_user_id': 3}, {'route_mode': 'ambient'},
                          {'channel_policy': 'public_home'}, {'channel_id': 100}):
            with self.subTest(overrides=overrides):
                self.assertEqual(self.tone(**overrides), '')
        self.assertEqual(before, self.stored())

    def test_prompt_tone_and_shadow_basis_change_together_at_send_time(self):
        rid, root = self.complete('That solved it; I can finally move forward.', ['support_received_accepted'])
        args = dict(guild_id=1, user_id=2, channel_id=99, channel_policy='sealed_test',
                    route_mode='normal_chat', direct=True)
        first = rel.shadow_packet_posture(self.conn, **args)
        self.assertIn('neutral-warm', self.tone())
        self.conn.execute("UPDATE memory_ledger_entries SET visibility='private' WHERE entry_id=?", (root,))
        second = rel.shadow_packet_posture(self.conn, **args)
        self.assertNotEqual(first['source_digest'], second['source_digest'])
        self.assertNotIn('neutral-warm', self.tone())
        self.assertEqual(self.state()['evidence_counts'], {})


if __name__ == '__main__':
    unittest.main()
