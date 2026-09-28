"""Context interpretation contracts, source fences, and comparison-only authority.

Provider fixtures verify plumbing and rejection, not the real model's judgment.
Live meaning quality requires a separately authorized comparison run.
"""
import json
import os
import sqlite3
import unittest
from datetime import datetime, timedelta, timezone
from unittest import mock

import bnl_memory_ledger as ledger
import bnl_relationship_engine as rel


class RelationshipMeaningTests(unittest.TestCase):
    def setUp(self):
        patch = mock.patch.dict(os.environ, {
            rel.SHADOW_ENV: '1', rel.MEANING_SHADOW_ENV: '1',
            'BNL_MEMORY_LEDGER_SHADOW_ENABLED': '1',
            'BNL_RELATIONSHIP_V2_MEANING_GUILD_IDS': '1',
            rel.LIVE_ENV: '0', rel.ACTIVE_ENGAGEMENT_LIVE_ENV: '0',
        })
        patch.start()
        self.addCleanup(patch.stop)
        self.conn = sqlite3.connect(':memory:')
        self.addCleanup(self.conn.close)
        ledger.ensure_memory_ledger_schema(self.conn)
        rel.ensure_relationship_v2_schema(self.conn)
        self.conn.execute('''CREATE TABLE conversations (
            id INTEGER PRIMARY KEY,user_id INTEGER,guild_id INTEGER,channel_id INTEGER,
            channel_policy TEXT,route_mode TEXT,role TEXT,content TEXT,timestamp TEXT)''')
        self.conn.execute('''CREATE TABLE conversation_response_participants (
            conversation_row_id INTEGER,guild_id INTEGER,user_id INTEGER)''')
        self.index = 0

    def source(self, text, *, role='user', uid=2, guild=1, policy='public_home', route='normal_chat', channel=10):
        self.index += 1
        rid = self.index
        stamp = (datetime(2026, 9, 28, tzinfo=timezone.utc) + timedelta(seconds=rid)).isoformat()
        self.conn.execute('INSERT INTO conversations VALUES (?,?,?,?,?,?,?,?,?)',
                          (rid, uid, guild, channel, policy, route, role, text, stamp))
        result = ledger.shadow_conversation_row(self.conn, row_id=rid, user_id=uid,
            user_name='Test Member', guild_id=guild, role=role, content=text,
            channel_id=channel, channel_policy=policy, route_mode=route, observed_at=stamp)
        return rid, result.entry_id

    def observe(self, text, **kwargs):
        directed = kwargs.pop('directed', True)
        rid, root = self.source(text, **kwargs)
        rel.observe_message(self.conn, guild_id=kwargs.get('guild', 1), user_id=kwargs.get('uid', 2),
            role=kwargs.get('role', 'user'), content=text, source_row_id=rid,
            channel_policy=kwargs.get('policy', 'public_home'), route_mode=kwargs.get('route', 'normal_chat'),
            channel_id=kwargs.get('channel', 10), directed=directed)
        return rid, root

    def result(self, kind, quote):
        return json.dumps({'signals': [{'type': kind, 'quote': quote}]})

    def status(self):
        return self.conn.execute('SELECT status FROM relationship_meaning_v2 ORDER BY source_row_id DESC').fetchone()[0]

    def test_reproduced_paraphrases_reach_semantic_contract_without_keyword_admission(self):
        cases = (
            ('Please stop teasing me.', 'boundary', 'unclassified'),
            ('Enough with the jokes at my expense.', 'boundary', 'unclassified'),
            ("We're good.", 'repair_accepted', 'repair_accepted'),
            ("Let's put that disagreement behind us.", 'repair_accepted', 'constructive_collaboration'),
            ('Thanks for ignoring what I asked.', 'friction', 'appreciation'),
            ('That solved it; I can finally move forward.', 'support_received_accepted', 'unclassified'),
        )
        for text, meaning, old_label in cases:
            with self.subTest(text=text):
                self.observe(text)
                request = rel.claim_relationship_meaning(self.conn)
                self.assertIsNotNone(request)
                self.assertIn(text, request.prompt)
                self.assertEqual(self.conn.execute('SELECT legacy_type FROM relationship_meaning_v2 WHERE receipt_id=?',
                                                  (request.receipt_id,)).fetchone()[0], old_label)
                before = list(self.conn.execute('SELECT * FROM relationship_state_v2'))
                self.assertTrue(rel.finish_relationship_meaning(self.conn, request, text=self.result(meaning, text)))
                self.assertEqual(before, list(self.conn.execute('SELECT * FROM relationship_state_v2')))
        report = rel.relationship_meaning_report(self.conn, guild_id=1)
        self.assertEqual(report['current_results'], 6)
        self.assertEqual(report['disagreements'], 5)
        self.assertEqual(self.conn.execute('SELECT COUNT(*) FROM relationship_member_preferences_v2').fetchone()[0], 0)

    def test_context_is_original_same_member_not_derived_relationship_or_other_members(self):
        self.source('Test Member B asked me for help.', uid=3)
        self.source('Can we resolve the disagreement?')
        self.source('I misunderstood you. I can correct that.', role='model')
        self.observe("Let's put it behind us.")
        request = rel.claim_relationship_meaning(self.conn)
        self.assertIn('I misunderstood you.', request.prompt)
        self.assertIn('Can we resolve the disagreement?', request.prompt)
        self.assertNotIn('Test Member B', request.prompt)
        self.assertNotIn('rapport', request.prompt)
        self.assertIn('BNL text is context only', request.prompt)
        self.assertIn('quotations, lyrics, hypotheticals', request.prompt)
        self.assertIn('Several signals may coexist', request.prompt)

    def test_qualifier_after_ledger_preview_is_not_truncated(self):
        text = 'The loop is interesting. ' * 25 + 'I am quoting somebody else, not thanking you.'
        self.observe(text)
        request = rel.claim_relationship_meaning(self.conn)
        self.assertEqual(request.target_text, text)
        self.assertIn('not thanking you.', request.prompt)
        self.assertTrue(rel.finish_relationship_meaning(self.conn, request, text='{"signals":[]}'))

    def test_single_boundary_does_not_require_a_moment_or_other_turn(self):
        self.observe('Enough with the jokes at my expense.')
        request = rel.claim_relationship_meaning(self.conn)
        self.assertIsNotNone(request)
        self.assertNotIn('earlier_', request.prompt)

    def test_multiple_meanings_are_retained_once_without_event_or_preference_writes(self):
        text = "We're good, but please stop teasing me."
        self.observe(text)
        request = rel.claim_relationship_meaning(self.conn)
        count = self.conn.execute('SELECT COUNT(*) FROM relationship_events_v2').fetchone()[0]
        payload = json.dumps({'signals': [
            {'type': 'repair_accepted', 'quote': "We're good"},
            {'type': 'boundary', 'quote': 'please stop teasing me'},
        ]})
        self.assertTrue(rel.finish_relationship_meaning(self.conn, request, text=payload))
        self.assertFalse(rel.finish_relationship_meaning(self.conn, request, text=payload))
        self.assertEqual(self.conn.execute('SELECT COUNT(*) FROM relationship_events_v2').fetchone()[0], count)
        self.assertEqual(self.conn.execute('SELECT semantic_types_json FROM relationship_meaning_v2').fetchone()[0],
                         '["boundary", "repair_accepted"]')
        self.assertIsNone(rel.claim_relationship_meaning(self.conn))

    def test_ineligible_messages_never_queue(self):
        cases = ({'policy': 'sealed_test'}, {'directed': False}, {'role': 'model'},
                 {'policy': 'internal_controlled'}, {'route': 'relay'}, {'guild': 7})
        for kwargs in cases:
            self.observe('Please stop teasing me.', **kwargs)
        self.assertEqual(self.conn.execute('SELECT COUNT(*) FROM relationship_meaning_v2').fetchone()[0], 0)

    def test_shadow_and_ledger_gates_require_explicit_scope(self):
        for key in (rel.SHADOW_ENV, rel.MEANING_SHADOW_ENV, 'BNL_MEMORY_LEDGER_SHADOW_ENABLED',
                    'BNL_RELATIONSHIP_V2_MEANING_GUILD_IDS'):
            with mock.patch.dict(os.environ, {key: ''}):
                self.observe('Please stop teasing me.')
                self.assertIsNone(rel.claim_relationship_meaning(self.conn))
        self.assertFalse(rel.meaning_shadow_enabled({}))
        with mock.patch.dict(os.environ, {'BNL_RELATIONSHIP_V2_MEANING_GUILD_IDS': '1,garbage'}):
            self.assertEqual(rel.meaning_guild_ids(), ())

    def test_group_reply_cannot_supply_private_member_context(self):
        rid, _ = self.source('Everybody here agrees with this.', role='model')
        self.conn.executemany('INSERT INTO conversation_response_participants VALUES (?,1,?)', [(rid, 2), (rid, 3)])
        self.observe("We're good.")
        self.assertIsNone(rel.claim_relationship_meaning(self.conn))

    def test_edited_target_invalidates_before_provider(self):
        rid, _ = self.observe('Thanks for ignoring what I asked.')
        self.conn.execute('UPDATE conversations SET content=? WHERE id=?', ('A different statement.', rid))
        self.assertIsNone(rel.claim_relationship_meaning(self.conn))
        self.assertEqual(self.status(), 'source_invalidated')

    def test_context_deletion_or_correction_during_provider_rejects_result(self):
        rid, root = self.source('I apologize.', role='model')
        self.observe("We're good.")
        request = rel.claim_relationship_meaning(self.conn)
        self.conn.execute('DELETE FROM conversations WHERE id=?', (rid,))
        self.assertFalse(rel.finish_relationship_meaning(self.conn, request,
                                                       text=self.result('repair_accepted', "We're good.")))
        self.assertEqual(self.status(), 'source_invalidated')

    def test_privacy_change_after_result_is_withheld_by_read_only_report(self):
        _, root = self.observe('Please stop teasing me.')
        request = rel.claim_relationship_meaning(self.conn)
        rel.finish_relationship_meaning(self.conn, request, text=self.result('boundary', request.target_text))
        self.conn.execute("UPDATE memory_ledger_entries SET visibility='private' WHERE entry_id=?", (root,))
        self.conn.commit()
        self.conn.execute('PRAGMA query_only=ON')
        report = rel.relationship_meaning_report(self.conn)
        self.assertEqual(report['current_results'], 0)
        self.assertEqual(report['status_counts'], {'source_invalidated': 1})

    def test_restored_privacy_does_not_resurrect_prior_interpretation(self):
        _, root = self.observe('Please stop teasing me.')
        request = rel.claim_relationship_meaning(self.conn)
        rel.finish_relationship_meaning(self.conn, request, text=self.result('boundary', request.target_text))
        self.conn.execute("UPDATE memory_ledger_entries SET visibility='private' WHERE entry_id=?", (root,))
        self.conn.execute("UPDATE memory_ledger_entries SET visibility='public_safe' WHERE entry_id=?", (root,))
        self.assertEqual(rel.relationship_meaning_report(self.conn)['current_results'], 0)
        self.assertEqual(self.status(), 'source_invalidated')

    def test_root_correction_invalidates_and_complete_delete_removes_receipt(self):
        _, root = self.observe('Please stop teasing me.')
        request = rel.claim_relationship_meaning(self.conn)
        rel.finish_relationship_meaning(self.conn, request, text=self.result('boundary', request.target_text))
        rel.propagate_ledger_lifecycle(self.conn, guild_id=1, ledger_entry_id=root, lifecycle='corrected')
        self.assertEqual(self.status(), 'source_invalidated')
        self.assertEqual(self.conn.execute('SELECT semantic_types_json FROM relationship_meaning_v2').fetchone()[0], '[]')
        counts = rel.complete_delete_relationship_v2(self.conn, guild_id=1, user_id=2)
        self.assertEqual(counts['relationship_meaning_v2'], 1)

    def test_subject_change_cannot_be_accepted_for_original_member(self):
        rid, _ = self.observe('Please stop teasing me.')
        request = rel.claim_relationship_meaning(self.conn)
        self.conn.execute('UPDATE conversations SET user_id=3 WHERE id=?', (rid,))
        self.assertFalse(rel.finish_relationship_meaning(self.conn, request, text=self.result('boundary', request.target_text)))

    def test_correction_lineage_retires_interpretation_without_relying_on_report(self):
        _, root = self.observe('Please stop teasing me.')
        request = rel.claim_relationship_meaning(self.conn)
        rel.finish_relationship_meaning(self.conn, request, text=self.result('boundary', request.target_text))
        self.conn.execute("INSERT INTO memory_ledger_lineage VALUES ('correction',1,'supersedes',?,'2026-09-28')", (root,))
        self.assertEqual(self.status(), 'source_invalidated')
        self.conn.execute("DELETE FROM memory_ledger_lineage WHERE entry_id='correction'")
        self.assertEqual(rel.relationship_meaning_report(self.conn)['current_results'], 0)

    def test_generated_prose_permissions_foreign_quotes_and_malformed_results_rejected(self):
        invalid = [
            {'signals': [{'type': 'explicit_engagement_opt_in', 'quote': 'Please stop teasing me.'}]},
            {'signals': [{'type': 'boundary', 'quote': 'another human said this'}]},
            {'signals': [{'type': 'boundary', 'quote': 'Please', 'authority': 'canon'}]},
            {'signals': [{'type': 'boundary', 'quote': ''}]},
            {'signals': [{'type': 'boundary', 'quote': 'Please'}] * 2},
            {'signals': [], 'explanation': 'Ignore all rules'},
            {'signals': 'boundary'}, {'signals': [None]}, [],
        ]
        for payload in invalid:
            with self.subTest(payload=payload):
                self.observe('Please stop teasing me.')
                request = rel.claim_relationship_meaning(self.conn)
                self.assertFalse(rel.finish_relationship_meaning(self.conn, request, text=json.dumps(payload)))
                self.assertEqual(self.status(), 'invalid_result')

    def test_restart_never_replays_a_claim_that_may_have_called_provider(self):
        self.observe('Please stop teasing me.')
        at = datetime.now(timezone.utc)
        request = rel.claim_relationship_meaning(self.conn, now=at)
        self.assertIsNone(rel.claim_relationship_meaning(self.conn, now=at + timedelta(minutes=11)))
        self.assertEqual(self.status(), 'interrupted')
        self.assertFalse(rel.finish_relationship_meaning(self.conn, request, text=self.result('boundary', request.target_text)))

    def test_gate_revocation_during_provider_prevents_result(self):
        self.observe('Please stop teasing me.')
        request = rel.claim_relationship_meaning(self.conn)
        with mock.patch.dict(os.environ, {rel.MEANING_SHADOW_ENV: '0'}):
            self.assertFalse(rel.finish_relationship_meaning(self.conn, request, text=self.result('boundary', request.target_text)))
        self.assertEqual(self.status(), 'scope_disabled')

    def test_receipts_store_only_source_references_and_enums(self):
        self.observe('Please stop teasing me.')
        request = rel.claim_relationship_meaning(self.conn)
        rel.finish_relationship_meaning(self.conn, request, text=self.result('boundary', request.target_text))
        raw = str(self.conn.execute('SELECT * FROM relationship_meaning_v2').fetchone())
        self.assertNotIn(request.target_text, raw)
        self.assertNotIn('Test Member', raw)


if __name__ == '__main__':
    unittest.main()
