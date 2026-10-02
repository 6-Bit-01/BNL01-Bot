"""One retained reaction reaches chat without acquiring factual authority."""
import os
import unittest
from dataclasses import replace
from unittest import mock

import test_moment_meaning as fixtures
import bnl_unified_intelligence_packet as packets


class SharedImpressionPacketTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.MomentMeaningTests(methodName='runTest')
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.flags = {
            **self.fixture.flags,
            'BNL_IMPRESSIONS_FORMATION_ENABLED': 'true',
            'BNL_IMPRESSIONS_USE_ENABLED': 'true',
            'BNL_IMPRESSIONS_GUILD_IDS': '1',
        }
        self.fixture.flags.update(self.flags)
        patch = mock.patch.dict(os.environ, self.flags)
        patch.start()
        self.addCleanup(patch.stop)
        self.conn = self.fixture.conn
        self.mid, self.roots = self.fixture.captured_moment()
        self.fixture.enrich({
            **fixtures.REPORTER_MEANING,
            'impression': {
                'impression': 'I enjoyed the affectionate doubt inside that journalism joke.',
                'reason': 'Playful skepticism gave the conversation room to breathe.',
                'sourceRefs': ['turn_1'],
            },
        })

    def packet(self, **changes):
        request = replace(self.fixture.packet().request, **changes)
        return packets.build_packet(self.conn, request, environ=self.flags)

    def test_retained_reaction_has_separate_revalidated_moment_basis(self):
        packet = self.packet()
        self.assertFalse(packet.diagnostics.invalid_invariants)
        reaction = next(item for item in packet.items if item.lane == 'bnl_impression')
        basis = next(item for item in packet.items if item.source_type == 'impression_moment_context')
        self.assertEqual(reaction.event_ref, basis.event_ref)
        self.assertNotEqual(reaction.source_digest, basis.source_digest)
        self.assertEqual(reaction.subject_key, 'bnl_01')
        self.assertEqual(reaction.authority, 0)
        self.assertEqual(reaction.root_identities, ())
        self.assertNotIn(reaction, packet.validation_items)
        self.assertNotIn(reaction.source_ref, packet.governed_refs)
        self.assertNotIn(reaction.text, basis.text)
        self.assertIn('bnl_impression', packet.assessment_lanes)
        self.assertTrue(packets.revalidate_packet(self.conn, packet, environ=self.flags).valid)

    def test_original_change_invalidates_both_frozen_projections(self):
        packet = self.packet()
        self.conn.execute("UPDATE memory_ledger_entries SET normalized_value=? WHERE entry_id=?",
                          ('The source has been corrected to withdraw that claim.', self.roots[0]))
        result = packets.revalidate_packet(self.conn, packet, environ=self.flags)
        self.assertFalse(result.valid)
        self.assertGreaterEqual(result.changed_source_count, 2)
        self.assertFalse(any(item.lane == 'bnl_impression' for item in self.packet().items))

    def test_gate_withdrawal_blocks_saved_packet_and_new_retrieval(self):
        packet = self.packet()
        disabled = {**self.flags, 'BNL_IMPRESSIONS_USE_ENABLED': 'false'}
        self.assertFalse(packets.revalidate_packet(self.conn, packet, environ=disabled).valid)
        rebuilt = packets.build_packet(self.conn, packet.request, environ=disabled)
        self.assertFalse(any(item.revalidation_kind == 'impression' for item in rebuilt.items))

    def test_impression_never_survives_without_its_grounding_companion(self):
        for size in (120, 300, 800):
            packet = self.packet(budget_chars=size)
            for item in packet.items:
                if item.lane == 'bnl_impression':
                    self.assertTrue(any(other.source_type == 'impression_moment_context'
                                        and other.event_ref == item.event_ref for other in packet.items))
            self.assertFalse(packet.diagnostics.invalid_invariants)

    def test_reaction_cannot_claim_fact_or_recurrence_authority(self):
        packet = self.packet()
        impression = next(item for item in packet.items if item.lane == 'bnl_impression')
        corrupted = replace(packet, items=tuple(
            replace(item, authority=5, root_identities=('invented-credit',))
            if item == impression else item for item in packet.items))
        self.assertIn('impression_fact_authority_violation', packets._packet_invariants(corrupted))

    def test_immediate_recap_does_not_add_historical_perspective(self):
        self.assertFalse(any(item.lane == 'bnl_impression'
                             for item in self.packet(immediate_recap=True).items))

    def test_broad_profile_keeps_reaction_without_inventing_a_member_fact(self):
        packet = self.packet(frame_revision='', frame_subject_requirement='legacy',
                             subject_user_id=1, broad_profile_intent=True,
                             user_text='Tell me about Test Member 1 and what you think of them.')
        self.assertFalse(packet.diagnostics.invalid_invariants)
        self.assertTrue(any(item.lane == 'bnl_impression' for item in packet.items))
        for item in packet.items:
            if item.source_type in {'bnl_retained_impression', 'impression_moment_context'}:
                self.assertFalse(packets._profile_member_item(packet.request, item))

    def test_person_context_uses_that_persons_contribution_not_group_summary(self):
        basis = {'summary': 'Two participants discussed different projects.', 'contributions': [
            {'subjectRef': 'discord_user:1', 'displayName': 'Test Member One',
             'summary': 'The participant described acoustic percussion.'},
            {'subjectRef': 'discord_user:2', 'displayName': 'Test Member Two',
             'summary': 'The participant described a distorted synthesizer.'},
        ]}
        value = packets._impression_text(basis, 'moment', 'discord_user:1')
        self.assertIn('acoustic percussion', value)
        self.assertNotIn('synthesizer', value)
        self.assertNotIn('Two participants', value)
        self.assertEqual(packets._impression_text(basis, 'moment', 'discord_user:9'), '')


if __name__ == '__main__':
    unittest.main()
