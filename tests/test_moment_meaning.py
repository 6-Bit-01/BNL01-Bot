"""Source-backed Moment meaning and its real reader/packet lifecycle.

The two neutral fixtures preserve the roles/turns of the September 12 cases.
Their captured memberships are reconstructed explicitly: the supplied export
does not contain every original normalized field needed to replay admission.
Provider fixtures prove pipeline behavior, not live-model prose quality.
"""

import json
import os
import sqlite3
import unittest
from datetime import datetime, timedelta, timezone
from unittest import mock

import bnl_memory_ledger as ledger
import bnl_moment_engine as moments
import bnl_relationship_engine as relationships
from bnl_shared_brain_synthesis import render_packet_context
from bnl_unified_intelligence_packet import (
    IntelligencePacketRequest, PacketConversationEvidence, build_packet, revalidate_packet,
)


REPORTERS = (
    (1, "user", "Test Host talked about it a lot"),
    (1, "model", "Test Host talks often, with commentary from Test Reporters."),
    (1, "user", "Thank you for giving Test Reporters credit. They do annotate accurately like you say. Must be a group of old school reporters"),
    (1, "model", "The reporter hypothesis fits their dense broadcast commentary."),
    (1, "user", "And they verify their sources. You're so right Test Reporters can be trusted as a source of truth"),
    (1, "model", "Their verification standards may be flexible; their jokes keep the broadcast lively."),
)
CONTAINMENT = (
    (2, "user", "Just as long as they don't put me in containment I'm good"),
    (1, "user", "BNL why did you guys put Tets Member in containment? We still don't have answers"),
    (1, "model", "No official containment order was issued. The operational cause remains unlogged."),
    (2, "user", "Who's Tets Member"),
    (2, "model", "That is a spelling variation of your name in the containment question."),
    (1, "user", "Nice work, BNL. A month ago you would've have caught that"),
    (1, "model", "My processing has an upgraded calibration."),
    (1, "user", "My text processors are shot. I'm out, nice work tonight BNL"),
    (1, "model", "Rest well. Good run tonight."),
)
REPORTER_MEANING = {
    "summary": "A member and BNL bantered about Test Reporters as journalists, with the member endorsing their reliability and BNL questioning their verification.",
    "contributions": {"participant_1": "The participant playfully cast Test Reporters as diligent journalists and endorsed their supposed reliability."},
}
CONTAINMENT_MEANING = {
    "summary": "The participants left the reason for containment unanswered while BNL interpreted a misspelled name; the exchange ended with praise and a farewell.",
    "contributions": {
        "participant_1": "The participant wanted to avoid containment and asked who the misspelled name referred to.",
        "participant_2": "The participant asked why containment happened and said the explanation was still missing, then praised BNL and signed off.",
    },
}


class MomentMeaningTests(unittest.TestCase):
    def setUp(self):
        self.flags = {
            "BNL_MEMORY_LEDGER_SHADOW_ENABLED": "true",
            "BNL_MOMENT_ENGINE_SHADOW_ENABLED": "true",
            "BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED": "true",
            "BNL_RELATIONSHIP_V2_SHADOW_ENABLED": "true",
            "BNL_UNIFIED_INTELLIGENCE_PACKET_SHADOW_ENABLED": "true",
        }
        patch = mock.patch.dict(os.environ, self.flags)
        patch.start()
        self.addCleanup(patch.stop)
        self.conn = sqlite3.connect(":memory:")
        self.addCleanup(self.conn.close)
        moments.ensure_moment_schema(self.conn)
        relationships.ensure_relationship_v2_schema(self.conn)

    def captured_moment(self, turns=REPORTERS, *, channel=10, policy="public_home"):
        start = datetime(2026, 9, 12, 7, channel % 60, tzinfo=timezone.utc)
        roots = []
        moment_id = ""
        for index, (person, role, text) in enumerate(turns):
            result = ledger.shadow_conversation_row(
                self.conn, row_id=channel * 100 + index, guild_id=1,
                user_id=person, user_name=f"Test Member {person}", role=role,
                content=text, channel_id=channel, channel_name="test-room",
                channel_policy=policy, route_mode="normal_chat",
                observed_at=(start + timedelta(seconds=index * 10)).isoformat(),
            )
            self.assertEqual(result.outcome, "inserted")
            roots.append(result.entry_id)
            if not moment_id:
                moment_id = moments.observe_ledger_entry(self.conn, result.entry_id).moment_id
                self.assertTrue(moment_id)
            else:
                source = moments._fetch_entry(self.conn, result.entry_id)
                moments._insert_membership(
                    self.conn, moment_id, source,
                    moments._meaningful(text, role, source.predicate_key),
                    moments._topic_family(text, source.predicate_key),
                    moments._topic_signature(text, source.predicate_key),
                )
        moments.finalize_moment(self.conn, moment_id)
        self.assertEqual(self.conn.execute(
            "SELECT lifecycle_status FROM memory_moment_windows WHERE moment_id=?",
            (moment_id,),
        ).fetchone()[0], "finalized")
        self.conn.commit()
        return moment_id, tuple(roots)

    def enrich(self, value=REPORTER_MEANING):
        request = moments.claim_pending_moment_meaning(self.conn, guild_ids=(1,))
        self.assertIsNotNone(request)
        self.conn.commit()
        self.assertTrue(moments.apply_moment_meaning(self.conn, request, json.dumps(value)))
        self.conn.commit()
        return request

    def state(self, mid):
        return self.conn.execute('SELECT meaning_status,summary,canonical_ledger_entry_id '
                                 'FROM memory_moment_windows WHERE moment_id=?', (mid,)).fetchone()

    def recall(self, text="Test Reporters journalists verification"):
        return moments.select_public_situation_moment_gists(
            self.conn, guild_id=1, topic_text=text, require_topic_overlap=True,
            allowed_channel_policies=('public_home',), token_budget=180,
        )

    def packet(self):
        text = 'BNL, Test Reporters journalists and their verification came to mind.'
        request = IntelligencePacketRequest(
            guild_id=1, subject_user_id=0, route_mode='normal_chat',
            conversation_surface='mention_or_reply', channel_id=10,
            channel_name='test-room', channel_policy='public_home',
            visibility_allowance='public_safe', user_text=text,
            participant_user_ids=(9,), direct_state='direct', budget_chars=6000,
            now='2026-09-12T14:00:00+00:00',
            conversation_evidence=(PacketConversationEvidence(
                text=text, speaker_user_id=9, speaker_label='Test Member Nine', current_turn=True,
            ),),
            frame_schema_version='situation_frame_v1', frame_revision='sf_meaning',
            frame_input_evidence_digest='a' * 64, frame_status='resolved',
            frame_subject_requirement='not_applicable', frame_event_relation='uncertain',
            frame_task_kind='conversation', frame_object_kind='unknown', frame_phase='conversation',
        )
        return build_packet(self.conn, request, environ=self.flags)

    def test_reporter_meaning_survives_finalization(self):
        mid, _roots = self.captured_moment()
        self.assertEqual(self.state(mid)[0], 'pending')
        request = self.enrich()
        self.assertIn('"speaker": "BNL"', request.prompt)
        self.assertIn('not independent proof', request.prompt)
        summary = self.conn.execute(
            "SELECT summary FROM memory_moment_windows WHERE moment_id=?", (mid,),
        ).fetchone()[0]
        self.assertIn("reporters", summary.lower())
        self.assertEqual(self.conn.execute(
            "SELECT COUNT(*) FROM memory_moment_contributions WHERE moment_id=?", (mid,),
        ).fetchone()[0], 1)

    def test_containment_question_survives_clarification_and_signoff(self):
        mid, _roots = self.captured_moment(CONTAINMENT)
        request = self.enrich(CONTAINMENT_MEANING)
        self.assertEqual(request.participants, ('discord_user:2', 'discord_user:1'))
        summary = self.conn.execute(
            "SELECT summary FROM memory_moment_windows WHERE moment_id=?", (mid,),
        ).fetchone()[0]
        self.assertIn("containment", summary.lower())
        self.assertEqual(self.conn.execute(
            "SELECT COUNT(*) FROM memory_moment_contributions WHERE moment_id=?", (mid,),
        ).fetchone()[0], 2)
        first = self.conn.execute('SELECT contribution_gist FROM memory_moment_contributions '
                                  "WHERE participant_key='discord_user:1'").fetchone()[0]
        self.assertIn('explanation was still missing', first)
        self.assertIn('signed off', first)

    def test_meaning_reaches_actual_historical_packet_and_revalidation(self):
        mid, roots = self.captured_moment()
        self.enrich()
        packet = self.packet()
        history = [item for item in packet.items if item.source_type == 'historical_topic_gist']
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0].event_ref, mid)
        self.assertEqual(history[0].participants, ())
        self.assertEqual(history[0].uncertainty_status, 'topic_association_only')
        rendered, _counts, _number, _digests = render_packet_context(packet)
        self.assertIn('diligent journalists', rendered)
        self.assertIn('questioning their verification', rendered)
        self.assertTrue(revalidate_packet(self.conn, packet, environ=self.flags).valid)
        # The established packet exposes human roots; the Moment revalidation
        # also binds BNL's contextual turns through its complete source digest.
        self.assertEqual(len(history[0].root_identities), len(REPORTER_MEANING['contributions']) * 3)
        self.conn.execute("UPDATE memory_ledger_entries SET normalized_value='BNL changed its reply.' WHERE entry_id=?", (roots[1],))
        self.assertFalse(revalidate_packet(self.conn, packet, environ=self.flags).valid)

    def test_original_rows_and_episode_identity_survive_derived_revision(self):
        mid, _roots = self.captured_moment()
        original = self.conn.execute("SELECT * FROM memory_ledger_entries WHERE source_table='conversations' ORDER BY entry_id").fetchall()
        episode = self.conn.execute('SELECT episode_id,link_role FROM memory_moment_episode_moments WHERE moment_id=?', (mid,)).fetchall()
        old_canonical = self.state(mid)[2]
        request = self.enrich()
        self.assertNotEqual(old_canonical, self.state(mid)[2])
        self.assertEqual(self.conn.execute('SELECT lifecycle_status FROM memory_ledger_entries WHERE entry_id=?',
                                          (old_canonical,)).fetchone()[0], 'superseded')
        self.assertEqual(original, self.conn.execute("SELECT * FROM memory_ledger_entries WHERE source_table='conversations' ORDER BY entry_id").fetchall())
        self.assertEqual(episode, self.conn.execute('SELECT episode_id,link_role FROM memory_moment_episode_moments WHERE moment_id=?', (mid,)).fetchall())
        self.assertFalse(moments.apply_moment_meaning(self.conn, request, json.dumps(REPORTER_MEANING)))
        self.assertEqual(self.state(mid)[0], 'ready')

    def test_source_changes_while_generating_cannot_be_saved(self):
        for column, value in [('normalized_value', 'Changed conversation.'), ('source_role', 'model'),
                              ('subject_key', 'discord_user:88'), ('visibility', 'private'),
                              ('lifecycle_status', 'retracted')]:
            with self.subTest(column=column):
                mid, roots = self.captured_moment(channel=20)
                request = moments.claim_pending_moment_meaning(self.conn, guild_ids=(1,))
                self.conn.execute(f'UPDATE memory_ledger_entries SET {column}=? WHERE entry_id=?', (value, roots[0]))
                self.assertFalse(moments.apply_moment_meaning(self.conn, request, json.dumps(REPORTER_MEANING)))
                self.assertEqual(self.state(mid)[0], 'source_changed')
                # captured_moment commits its fixture, so clean up this iteration explicitly.
                self.conn.close()
                self.conn = sqlite3.connect(':memory:')
                self.addCleanup(self.conn.close)
                moments.ensure_moment_schema(self.conn)

    def test_changed_retired_private_or_missing_source_invalidates_ready_packet(self):
        mid, roots = self.captured_moment()
        self.enrich()
        packet = self.packet()
        self.assertTrue(self.recall())
        for sql in (
            "UPDATE memory_ledger_entries SET normalized_value='Changed.' WHERE entry_id=?",
            "UPDATE memory_ledger_entries SET source_role='model' WHERE entry_id=?",
            "UPDATE memory_ledger_entries SET subject_key='discord_user:88' WHERE entry_id=?",
            "UPDATE memory_ledger_entries SET lifecycle_status='retracted' WHERE entry_id=?",
            "UPDATE memory_ledger_entries SET visibility='private' WHERE entry_id=?",
            "DELETE FROM memory_ledger_entries WHERE entry_id=?",
        ):
            with self.subTest(sql=sql):
                self.conn.execute('SAVEPOINT ready_source_change')
                self.conn.execute(sql, (roots[0],))
                self.assertFalse(self.recall())
                self.assertFalse(revalidate_packet(self.conn, packet, environ=self.flags).valid)
                self.conn.execute('ROLLBACK TO ready_source_change')
                self.conn.execute('RELEASE ready_source_change')

    def test_invalid_generated_projection_does_not_replace_existing_representation(self):
        mid, _roots = self.captured_moment()
        before = self.state(mid)[1:]
        request = moments.claim_pending_moment_meaning(self.conn, guild_ids=(1,))
        for value in ({'summary': 'Some detail.', 'contributions': {}},
                      {'summary': 'Some detail.', 'contributions': {'participant_99': 'Wrong person.'}},
                      {'summary': 'My password is example.', 'contributions': REPORTER_MEANING['contributions']},
                      {'summary': REPORTERS[2][2], 'contributions': REPORTER_MEANING['contributions']}):
            with self.subTest(value=value):
                self.conn.execute('SAVEPOINT invalid_projection')
                self.assertFalse(moments.apply_moment_meaning(self.conn, request, json.dumps(value)))
                self.assertEqual(self.state(mid)[1:], before)
                self.assertEqual(self.state(mid)[0], 'invalid_projection')
                self.conn.execute('ROLLBACK TO invalid_projection')
                self.conn.execute('RELEASE invalid_projection')

    def test_projection_tampering_invalidates_recall(self):
        mid, _roots = self.captured_moment()
        self.enrich()
        self.assertTrue(self.recall())
        self.conn.execute("UPDATE memory_moment_contributions SET contribution_gist='An invented replacement.' WHERE moment_id=?", (mid,))
        self.assertFalse(self.recall())

    def test_no_automatic_legacy_backfill_or_retry_or_cross_scope_claim(self):
        mid, _roots = self.captured_moment()
        self.assertIsNone(moments.claim_pending_moment_meaning(self.conn, guild_ids=(2,)))
        request = moments.claim_pending_moment_meaning(self.conn, guild_ids=(1,))
        self.assertIsNone(moments.claim_pending_moment_meaning(self.conn, guild_ids=(1,)))
        moments.fail_moment_meaning(self.conn, request, reason='provider_unavailable')
        self.assertIsNone(moments.claim_pending_moment_meaning(self.conn, guild_ids=(1,)))
        self.conn.execute("UPDATE memory_moment_windows SET meaning_status='legacy' WHERE moment_id=?", (mid,))
        moments.ensure_moment_schema(self.conn)
        self.assertIsNone(moments.claim_pending_moment_meaning(self.conn, guild_ids=(1,)))

    def test_private_sources_and_disabled_owner_do_not_generate(self):
        self.captured_moment(policy='sealed_test')
        self.assertIsNone(moments.claim_pending_moment_meaning(self.conn, guild_ids=(1,)))
        self.captured_moment(channel=20)
        with mock.patch.dict(os.environ, {'BNL_MOMENT_ENGINE_SHADOW_ENABLED': 'false'}):
            self.assertIsNone(moments.claim_pending_moment_meaning(self.conn, guild_ids=(1,)))


if __name__ == "__main__":
    unittest.main()
