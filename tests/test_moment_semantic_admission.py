"""Natural capture reaches the existing meaning worker without forced membership."""
import json
import os
import sqlite3
import unittest
from datetime import datetime, timedelta, timezone
from unittest import mock

import bnl_memory_ledger as ledger
import bnl_moment_engine as moments
from test_moment_meaning import REPORTERS, REPORTER_MEANING


class MomentSemanticAdmissionTests(unittest.TestCase):
    ANNOUNCEMENT = ((1, "user", "After six months recording in my spare room, I finished the debut EP and booked my first local performance."),)
    ACCEPTED = {
        "retain": True,
        "summary": "A member announced completing a home-recorded debut release and arranging a first live appearance.",
        "contributions": {"participant_1": "The participant shared a recording milestone and plans to perform locally."},
    }

    def setUp(self):
        env = mock.patch.dict(os.environ, {
            "BNL_MEMORY_LEDGER_SHADOW_ENABLED": "true",
            "BNL_MOMENT_ENGINE_SHADOW_ENABLED": "true",
        })
        env.start()
        self.addCleanup(env.stop)
        self.conn = sqlite3.connect(":memory:")
        self.addCleanup(self.conn.close)
        moments.ensure_moment_schema(self.conn)

    def observe(self, turns, *, policy="public_home", offsets=None):
        start = datetime(2026, 9, 12, 7, tzinfo=timezone.utc)
        roots, windows = [], []
        for index, (person, role, text) in enumerate(turns):
            result = ledger.shadow_conversation_row(
                self.conn, row_id=index + 1, guild_id=1, user_id=person,
                user_name=f"Test Member {person}", role=role, content=text,
                channel_id=10, channel_name="test-room", channel_policy=policy,
                route_mode="normal_chat", observed_at=(start + timedelta(
                    seconds=offsets[index] if offsets is not None else index * 10)).isoformat(),
            )
            self.assertEqual(result.outcome, "inserted")
            observation = moments.observe_ledger_entry(self.conn, result.entry_id)
            self.assertEqual(observation.outcome, "observed")
            roots.append(result.entry_id)
            windows.append(observation.moment_id)
        moments.sweep_expired_windows(self.conn, now=(start + timedelta(minutes=10)).isoformat())
        self.conn.commit()
        return tuple(roots), tuple(dict.fromkeys(windows))

    def test_natural_exchange_reaches_meaning_without_lexical_fragmentation(self):
        roots, windows = self.observe(REPORTERS)
        self.assertEqual(len(windows), 1)
        def read():
            return moments.select_public_situation_moment_gists(
                self.conn, guild_id=1, topic_text="Test Reporters journalists verification",
                require_topic_overlap=True, allowed_channel_policies=("public_home",), token_budget=180,
            )
        self.assertFalse(read())
        request = moments.claim_pending_moment_meaning(self.conn, guild_ids=(1,))
        self.assertIsNotNone(request)
        self.assertEqual(tuple(source.entry_id for source in request.sources), roots)
        self.assertTrue(request.admission_required)
        self.assertTrue(moments.apply_moment_meaning(
            self.conn, request, json.dumps(dict(REPORTER_MEANING, retain=True))))
        status, summary = self.conn.execute(
            "SELECT meaning_status,summary FROM memory_moment_windows WHERE moment_id=?", windows,
        ).fetchone()
        self.assertEqual(status, "ready")
        self.assertIn("playfully", self.conn.execute(
            "SELECT contribution_gist FROM memory_moment_contributions WHERE moment_id=?", windows,
        ).fetchone()[0])
        self.assertIn("bantered", summary)
        self.assertTrue(read())
        self.conn.execute("UPDATE memory_ledger_entries SET lifecycle_status='forgotten' WHERE entry_id=?", (roots[0],))
        self.assertFalse(read())

    def test_specific_single_contribution_gets_semantic_assessment_before_rejection(self):
        _roots, windows = self.observe(self.ANNOUNCEMENT)
        self.assertEqual(self.conn.execute(
            "SELECT lifecycle_status,canonical_ledger_entry_id FROM memory_moment_windows WHERE moment_id=?", windows,
        ).fetchone(), ("awaiting_meaning", ""))
        request = moments.claim_pending_moment_meaning(self.conn, guild_ids=(1,))
        self.assertIsNotNone(request)
        self.assertTrue(request.admission_required)
        self.assertTrue(moments.apply_moment_meaning(self.conn, request, json.dumps(self.ACCEPTED)))
        self.assertEqual(self.conn.execute(
            "SELECT lifecycle_status,qualification_type,meaning_status FROM memory_moment_windows WHERE moment_id=?", windows,
        ).fetchone(), ("finalized", "noteworthy_contribution", "ready"))

    def test_semantic_decline_leaves_no_durable_projection_or_episode(self):
        self.observe(((1, "user", "I am going to grab a drink and sit somewhere comfortable for a while."),))
        request = moments.claim_pending_moment_meaning(self.conn, guild_ids=(1,))
        self.assertIsNotNone(request)
        self.assertFalse(moments.apply_moment_meaning(self.conn, request, json.dumps({
            "retain": False, "summary": "", "contributions": {},
        })))
        self.assertEqual(self.conn.execute(
            "SELECT lifecycle_status,meaning_status,canonical_ledger_entry_id FROM memory_moment_windows",
        ).fetchone(), ("rejected", "not_retained", ""))
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM memory_moment_episodes").fetchone()[0], 0)
        self.assertEqual(self.conn.execute(
            "SELECT COUNT(*) FROM memory_ledger_entries WHERE entry_type='shared_moment'",
        ).fetchone()[0], 0)
        self.assertIsNone(moments.claim_pending_moment_meaning(self.conn, guild_ids=(1,)))

    def test_changed_source_cannot_be_admitted_from_stale_model_response(self):
        roots, _windows = self.observe(self.ANNOUNCEMENT)
        request = moments.claim_pending_moment_meaning(self.conn, guild_ids=(1,))
        self.conn.execute("UPDATE memory_ledger_entries SET normalized_value=? WHERE entry_id=?",
                          ("The performance was cancelled and the release is unfinished.", roots[0]))
        self.assertFalse(moments.apply_moment_meaning(self.conn, request, json.dumps(self.ACCEPTED)))
        self.assertEqual(self.conn.execute("SELECT canonical_ledger_entry_id FROM memory_moment_windows").fetchone()[0], "")
        self.assertIsNone(moments.claim_pending_moment_meaning(self.conn, guild_ids=(1,)))

    def test_correction_marks_pending_admission_for_review(self):
        roots, _windows = self.observe(self.ANNOUNCEMENT)
        request = moments.claim_pending_moment_meaning(self.conn, guild_ids=(1,))
        moments.handle_source_correction(self.conn, roots[0], guild_id=1)
        self.assertEqual(self.conn.execute("SELECT lifecycle_status FROM memory_moment_windows").fetchone()[0], "needs_review")
        self.assertFalse(moments.apply_moment_meaning(self.conn, request, json.dumps(self.ACCEPTED)))
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM memory_moment_episodes").fetchone()[0], 0)

    def test_privacy_change_blocks_pending_admission(self):
        roots, _windows = self.observe(self.ANNOUNCEMENT)
        request = moments.claim_pending_moment_meaning(self.conn, guild_ids=(1,))
        self.conn.execute("UPDATE memory_ledger_entries SET public_usable=0 WHERE entry_id=?", (roots[0],))
        self.assertFalse(moments.apply_moment_meaning(self.conn, request, json.dumps(self.ACCEPTED)))
        self.assertEqual(self.conn.execute("SELECT canonical_ledger_entry_id FROM memory_moment_windows").fetchone()[0], "")

    def test_disabled_gate_blocks_pending_admission(self):
        self.observe(self.ANNOUNCEMENT)
        request = moments.claim_pending_moment_meaning(self.conn, guild_ids=(1,))
        with mock.patch.dict(os.environ, {"BNL_MOMENT_ENGINE_SHADOW_ENABLED": "false"}):
            self.assertFalse(moments.apply_moment_meaning(self.conn, request, json.dumps(self.ACCEPTED)))
        self.assertEqual(self.conn.execute("SELECT meaning_status FROM memory_moment_windows").fetchone()[0], "disabled")

    def test_sealed_sources_receive_private_semantic_admission_only(self):
        self.observe(self.ANNOUNCEMENT, policy="sealed_test")
        request = moments.claim_pending_moment_meaning(self.conn, guild_ids=(1,))
        self.assertIsNotNone(request)
        self.assertTrue(request.admission_required)
        self.assertTrue(moments.apply_moment_meaning(self.conn, request, json.dumps(self.ACCEPTED)))
        self.assertEqual(self.conn.execute(
            "SELECT lifecycle_status,public_usable FROM memory_moment_windows",
        ).fetchone(), ("finalized", 0))
        self.assertFalse(moments.select_public_participant_moment_gists(
            self.conn, guild_id=1, participant_key="discord_user:1", broad_recall=True,
        ))

    def test_explicit_topic_change_keeps_separate_windows(self):
        _roots, windows = self.observe((
            (1, "user", "The synth needs a warmer attack for the chorus."),
            (1, "model", "A slower envelope would soften the opening transient."),
            (1, "user", "Separate topic: hiking conditions look rainy on the trail."),
        ))
        self.assertEqual(len(windows), 2)

    def test_other_speaker_does_not_inherit_unrelated_bnl_exchange(self):
        _roots, windows = self.observe((
            (1, "user", "The synth needs a warmer attack for the chorus."),
            (1, "model", "A slower envelope would soften the opening transient."),
            (2, "user", "Hiking conditions look rainy on the trail."),
        ))
        self.assertEqual(len(windows), 2)

    def test_older_backfilled_turn_does_not_become_a_new_followup(self):
        _roots, windows = self.observe((
            (1, "user", "The synth needs a warmer attack for the chorus."),
            (1, "model", "A slower envelope would soften the opening transient."),
            (1, "user", "Hiking conditions look rainy on the trail."),
        ), offsets=(10, 20, 5))
        self.assertEqual(len(windows), 2)

    def test_zero_call_budget_deferral_preserves_pending_admission(self):
        self.observe(self.ANNOUNCEMENT)
        now = datetime(2026, 9, 12, 7, 10, tzinfo=timezone.utc)
        request = moments.claim_pending_moment_meaning(self.conn, guild_ids=(1,), now=now)
        self.assertTrue(moments.defer_moment_meaning(self.conn, request, reason="monthly_hard_limit", now=now))
        self.assertIsNone(moments.claim_pending_moment_meaning(self.conn, guild_ids=(1,), now=now))
        resumed = moments.claim_pending_moment_meaning(self.conn, guild_ids=(1,), now=now + timedelta(minutes=30))
        self.assertTrue(resumed.admission_required)
        self.assertEqual(resumed.source_digest, request.source_digest)
        self.assertTrue(moments.apply_moment_meaning(self.conn, resumed, json.dumps(self.ACCEPTED)))

    def test_invalid_retention_shape_does_not_publish_or_retry(self):
        self.observe(self.ANNOUNCEMENT)
        request = moments.claim_pending_moment_meaning(self.conn, guild_ids=(1,))
        self.assertFalse(moments.apply_moment_meaning(self.conn, request, json.dumps(dict(self.ACCEPTED, retain="yes"))))
        self.assertEqual(self.conn.execute("SELECT meaning_status FROM memory_moment_windows").fetchone()[0], "invalid_projection")
        self.assertEqual(self.conn.execute("SELECT canonical_ledger_entry_id FROM memory_moment_windows").fetchone()[0], "")
        self.assertIsNone(moments.claim_pending_moment_meaning(self.conn, guild_ids=(1,)))

    def test_over_budget_uncertain_window_cannot_publish_a_template(self):
        with mock.patch.object(moments, "MOMENT_MEANING_MAX_SOURCES", 5):
            # The final model reply crosses the source-count bound
            # only after the earlier nonlexical follow-up has been retained.
            _roots, windows = self.observe(REPORTERS)
        self.assertEqual(len(windows), 1)
        self.assertEqual(self.conn.execute(
            "SELECT lifecycle_status,canonical_ledger_entry_id FROM memory_moment_windows",
        ).fetchone(), ("rejected", ""))
        self.assertIsNone(moments.claim_pending_moment_meaning(self.conn, guild_ids=(1,)))
