import os
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest import mock

import bnl_memory_ledger as ledger
import bnl_moment_engine as moments
import bnl_conversation_context_v2 as conversation_context


class MomentReplyContinuityTests(unittest.TestCase):
    FIRST = (
        "BNL, you previously attributed “I have been staring at the stream "
        "visualizer for forty minutes and I am fairly certain it just blinked "
        "back at me.” to CipherDot in your TikTok recap. Can you verify that "
        "exact quote and speaker against an original chat record? Correct "
        "your earlier attribution if you cannot support it."
    )
    MODEL = (
        "I cannot verify that exact quote or attribution against the retained "
        "show record. The source of the earlier output remains unknown."
    )
    CORRECTION = (
        "That processing explanation isn’t established by the evidence. What "
        "can you actually verify about your earlier attribution, and what "
        "remains unknown?"
    )
    SECOND_MODEL = (
        "I cannot establish a processing explanation or verify that earlier "
        "attribution. Its origin remains unknown."
    )
    DETOUR = (
        "Separate topic: briefly explain why a checksum can detect a corrupted "
        "file but cannot repair it."
    )

    def setUp(self):
        self.environment = mock.patch.dict(
            os.environ,
            {
                "BNL_MEMORY_LEDGER_SHADOW_ENABLED": "1",
                "BNL_MOMENT_ENGINE_SHADOW_ENABLED": "1",
            },
        )
        self.environment.start()
        self.addCleanup(self.environment.stop)
        self.conn = None
        self.reset_db()

    def tearDown(self):
        self.conn.close()

    def reset_db(self, path=":memory:"):
        if self.conn is not None:
            self.conn.close()
        self.conn = sqlite3.connect(path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS conversations ("
            "id INTEGER PRIMARY KEY, guild_id INTEGER, channel_id INTEGER, "
            "channel_policy TEXT, route_mode TEXT, role TEXT, content TEXT, "
            "timestamp TEXT)"
        )
        ledger.ensure_memory_ledger_schema(self.conn)
        moments.ensure_moment_schema(self.conn)

    def add(
        self,
        row_id,
        role,
        text,
        time,
        *,
        reply_to=0,
        observe=True,
        guild=1,
        channel=10,
        policy="sealed_test",
        route="normal_chat",
    ):
        reply = (
            {"reply_to_conversation_row_id": reply_to}
            if reply_to
            else {}
        )
        self.conn.execute(
            "INSERT INTO conversations "
            "(id,guild_id,channel_id,channel_policy,route_mode,role,content,timestamp) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (
                row_id, guild, channel, policy, route, role, text,
                "2026-09-10 " + time,
            ),
        )
        result = ledger.shadow_conversation_row(
            self.conn,
            row_id=row_id,
            user_id=10,
            user_name="Fictional Test Member",
            guild_id=guild,
            role=role,
            content=text,
            channel_policy=policy,
            channel_id=channel,
            channel_name="fictional-test-channel",
            route_mode=route,
            observed_at="2026-09-10 " + time,
            **reply,
        )
        self.assertIn(result.outcome, ("inserted", "deduplicated"))
        if observe:
            observation = moments.observe_ledger_entry(
                self.conn, result.entry_id
            )
            self.assertEqual(observation.outcome, "observed")
        return result

    def opening(self, **scope):
        first = self.add(8394, "user", self.FIRST, "02:40:58", **scope)
        model = self.add(8395, "model", self.MODEL, "02:41:25", **scope)
        return first, model

    def moment_id(self, source):
        return self.conn.execute(
            "SELECT moment_id FROM memory_moment_members "
            "WHERE ledger_entry_id=?",
            (source.entry_id,),
        ).fetchone()[0]

    def reply_targets(self, source):
        return [
            row[0]
            for row in self.conn.execute(
                "SELECT target_entry_id FROM memory_ledger_lineage "
                "WHERE entry_id=? AND lineage_type='reply_to'",
                (source.entry_id,),
            )
        ]

    def test_reply_preserves_correction_until_normal_detour_expiry(self):
        first, model = self.opening()
        correction = self.add(
            8396, "user", self.CORRECTION, "02:42:36", reply_to=8395
        )
        self.add(8397, "model", self.SECOND_MODEL, "02:42:46")
        self.assertEqual(self.moment_id(correction), self.moment_id(first))
        self.assertEqual(self.reply_targets(correction), [model.entry_id])

        # The actual detour occurs 153 seconds after the last response. It
        # must expire the coherent correction naturally, not extend it.
        detour = self.add(8398, "user", self.DETOUR, "02:45:19")
        self.assertNotEqual(self.moment_id(detour), self.moment_id(first))
        window = self.conn.execute(
            "SELECT lifecycle_status,qualification_type,human_entry_count,"
            "model_entry_count,canonical_ledger_entry_id "
            "FROM memory_moment_windows WHERE moment_id=?",
            (self.moment_id(first),),
        ).fetchone()
        self.assertEqual(tuple(window[:4]), ("finalized", "conversational", 2, 2))
        self.assertTrue(window[4])
        episode = self.conn.execute(
            "SELECT e.lifecycle_status FROM memory_moment_episodes e "
            "JOIN memory_moment_episode_moments em "
            "ON em.episode_id=e.episode_id WHERE em.moment_id=?",
            (self.moment_id(first),),
        ).fetchone()
        self.assertEqual(episode[0], "active")

        # A reply edge supplies conversation structure, never a rewrite of
        # original wording or promotion of BNL output to human authority.
        originals = self.conn.execute(
            "SELECT normalized_value,source_role,derived,public_usable,"
            "lifecycle_status FROM memory_ledger_entries "
            "WHERE entry_id IN (?,?) ORDER BY source_sequence",
            (model.entry_id, correction.entry_id),
        ).fetchall()
        self.assertEqual(originals[0][0], self.MODEL)
        self.assertEqual(tuple(originals[0][1:4]), ("model", 1, 0))
        self.assertEqual(originals[1][0], self.CORRECTION)
        self.assertIn(originals[0][4], ("active", "review_only"))
        self.assertEqual(
            self.conn.execute(
                "SELECT COUNT(*) FROM memory_ledger_lineage "
                "WHERE entry_id=? AND lineage_type IN "
                "('correction_of','supersedes','retracts')",
                (correction.entry_id,),
            ).fetchone()[0],
            0,
        )

    def test_same_flow_without_reply_evidence_stays_unlinked(self):
        first, _ = self.opening()
        correction = self.add(8396, "user", self.CORRECTION, "02:42:36")
        self.add(8397, "model", self.SECOND_MODEL, "02:42:46")
        self.add(8398, "user", self.DETOUR, "02:45:19")
        self.assertNotEqual(self.moment_id(first), self.moment_id(correction))
        self.assertEqual(self.reply_targets(correction), [])
        for source in (first, correction):
            self.assertEqual(
                self.conn.execute(
                    "SELECT lifecycle_status,qualification_reason "
                    "FROM memory_moment_windows WHERE moment_id=?",
                    (self.moment_id(source),),
                ).fetchone()[:],
                ("rejected", "low_signal_or_insufficient_continuity"),
            )
        self.assertEqual(
            self.conn.execute(
                "SELECT COUNT(*) FROM memory_moment_episodes"
            ).fetchone()[0],
            0,
        )

    def test_reply_edge_cannot_cross_source_boundaries(self):
        for scope in (
            {"guild": 2},
            {"channel": 11},
            {"policy": "public_home"},
            {"route": "direct_session"},
        ):
            with self.subTest(scope=scope):
                self.reset_db()
                first, _ = self.opening(**scope)
                correction = self.add(
                    8396, "user", self.CORRECTION, "02:42:36", reply_to=8395
                )
                self.assertEqual(self.reply_targets(correction), [])
                self.assertNotEqual(
                    self.moment_id(first), self.moment_id(correction)
                )

    def test_missing_nonmodel_and_ineligible_targets_do_not_supply_continuity(self):
        cases = (
            "missing", "human", "retracted", "deleted", "expired",
            "visibility", "raw_deleted", "raw_edited",
        )
        for case in cases:
            with self.subTest(case=case):
                self.reset_db()
                first, model = self.opening()
                target = 8395
                if case == "missing":
                    target = 9999
                elif case == "human":
                    target = 8394
                elif case == "visibility":
                    self.conn.execute(
                        "UPDATE memory_ledger_entries SET visibility='private' "
                        "WHERE entry_id=?", (model.entry_id,)
                    )
                elif case == "raw_deleted":
                    self.conn.execute("DELETE FROM conversations WHERE id=8395")
                elif case == "raw_edited":
                    self.conn.execute(
                        "UPDATE conversations SET content=? WHERE id=8395",
                        ("An edited answer with different wording.",),
                    )
                else:
                    self.conn.execute(
                        "UPDATE memory_ledger_entries SET lifecycle_status=? "
                        "WHERE entry_id=?", (case, model.entry_id)
                    )
                correction = self.add(
                    8396, "user", self.CORRECTION, "02:42:36", reply_to=target
                )
                self.assertEqual(self.reply_targets(correction), [])
                self.assertNotEqual(
                    self.moment_id(first), self.moment_id(correction)
                )

    def test_ambiguous_source_row_target_does_not_choose_a_revision(self):
        first, _ = self.opening()
        # More than one eligible model revision of the target row is not an
        # unambiguous reference to a particular retained answer.
        ledger.insert_ledger_entry(
            self.conn,
            ledger.LedgerEntry(
                guild_id=1,
                source_table="conversations",
                source_row_id=8395,
                source_revision="alternate-revision",
                source_role="model",
                entry_type="derived_summary",
                subject_key=ledger.BNL_SUBJECT_KEY,
                predicate_key="model_output",
                value="An alternate retained response.",
                source_class=ledger.SourceClass.DERIVED_SUMMARY,
                route_mode="normal_chat",
                channel_id=10,
                channel_name="fictional-test-channel",
                channel_policy="sealed_test",
                visibility=ledger.Visibility.SEALED_TEST,
                confidence=ledger.Confidence.LOW,
                derived=True,
                projection=True,
                observed_at="2026-09-10 02:41:25",
                source_sequence=8395,
            ),
        )
        correction = self.add(
            8396, "user", self.CORRECTION, "02:42:36", reply_to=8395
        )
        self.assertEqual(self.reply_targets(correction), [])
        self.assertNotEqual(self.moment_id(first), self.moment_id(correction))

    def test_reply_does_not_override_explicit_new_topic(self):
        first, _ = self.opening()
        detour = self.add(
            8396, "user", self.DETOUR, "02:42:36", reply_to=8395
        )
        self.assertNotEqual(self.moment_id(first), self.moment_id(detour))

    def test_reply_does_not_reopen_expired_or_finalized_window(self):
        for closed_before_reply in (False, True):
            with self.subTest(closed_before_reply=closed_before_reply):
                self.reset_db()
                first, _ = self.opening()
                if closed_before_reply:
                    moments.sweep_expired_windows(
                        self.conn, now="2026-09-10T02:44:00+00:00"
                    )
                correction = self.add(
                    8396, "user", self.CORRECTION, "02:44:01", reply_to=8395
                )
                self.assertNotEqual(
                    self.moment_id(first), self.moment_id(correction)
                )
                self.assertEqual(
                    self.conn.execute(
                        "SELECT lifecycle_status FROM memory_moment_windows "
                        "WHERE moment_id=?", (self.moment_id(first),)
                    ).fetchone()[0],
                    "rejected",
                )

    def test_reply_does_not_extend_the_five_minute_window_limit(self):
        first, _ = self.opening()
        self.add(8402, "model", self.MODEL, "02:43:20")
        self.add(8403, "model", self.MODEL, "02:45:15")
        correction = self.add(
            8404, "user", self.CORRECTION, "02:46:00", reply_to=8403
        )
        # Only 45 seconds since the last turn, but 302 seconds since opening.
        self.assertNotEqual(self.moment_id(first), self.moment_id(correction))
        self.assertEqual(
            self.conn.execute(
                "SELECT lifecycle_status FROM memory_moment_windows "
                "WHERE moment_id=?", (self.moment_id(first),)
            ).fetchone()[0],
            "rejected",
        )

    def test_target_withdrawn_after_edge_write_is_revalidated_at_observation(self):
        for change in ("ledger_retracted", "raw_deleted", "raw_edited"):
            with self.subTest(change=change):
                self.reset_db()
                first, model = self.opening()
                correction = self.add(
                    8396, "user", self.CORRECTION, "02:42:36",
                    reply_to=8395, observe=False,
                )
                self.assertEqual(self.reply_targets(correction), [model.entry_id])
                if change == "raw_deleted":
                    self.conn.execute("DELETE FROM conversations WHERE id=8395")
                elif change == "raw_edited":
                    self.conn.execute(
                        "UPDATE conversations SET content=? WHERE id=8395",
                        ("An edited answer with different wording.",),
                    )
                else:
                    self.conn.execute(
                        "UPDATE memory_ledger_entries SET lifecycle_status='retracted' "
                        "WHERE entry_id=?", (model.entry_id,)
                    )
                result = moments.observe_ledger_entry(self.conn, correction.entry_id)
                self.assertEqual(result.outcome, "observed")
                self.assertNotEqual(self.moment_id(first), self.moment_id(correction))

    def test_durable_reply_edge_survives_reopen_before_moment_observation(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "reply-memory.db"
            self.reset_db(path)
            first, model = self.opening()
            correction = self.add(
                8396, "user", self.CORRECTION, "02:42:36",
                reply_to=8395, observe=False,
            )
            self.conn.commit()
            self.reset_db(path)
            self.assertEqual(self.reply_targets(correction), [model.entry_id])
            result = moments.observe_ledger_entry(self.conn, correction.entry_id)
            self.assertEqual(result.outcome, "observed")
            self.assertEqual(self.moment_id(first), self.moment_id(correction))
            self.add(8397, "model", self.SECOND_MODEL, "02:42:46")
            moments.sweep_expired_windows(
                self.conn, now="2026-09-10T02:45:19+00:00"
            )
            self.assertEqual(
                self.conn.execute(
                    "SELECT COUNT(*) FROM memory_moment_episodes"
                ).fetchone()[0],
                1,
            )

    def live_route_transition(self, first_route="normal_chat", reply_route="direct_payload_task"):
        # Replay the four retained turns from the deployed formation check.
        # The actual Discord reply arrived nine seconds after the first answer.
        texts = (
            "BNL, continuing our correction discussion: playback is unconfirmed, "
            "and saying the timeline was updated did not prove that a stored "
            "record changed. How should we describe the correction accurately?",
            "Describe it as an unverified playback signal during intake rather "
            "than a timeline rewrite. That accurately reflects that the track "
            "was registered without confirmed audio telemetry, leaving the "
            "broadcast log unverified instead of altered.",
            "The distinction I want preserved is between acknowledging an error "
            "here and successfully changing a saved record. What can you "
            "establish about each, and what remains unknown?",
            "Acknowledging an error here is immediate: I can adjust active "
            "conversational context, correct a statement in real time, and "
            "realign our active exchange with verified facts. That is a "
            "surface-level runtime correction.\n\nSuccessfully changing a saved "
            "record is entirely distinct. Master archive entries and broadcast "
            "logs are persistent system data. Acknowledging a discrepancy in "
            "local chat does not automatically rewrite or re-index the "
            "underlying database—it simply records the active clarification "
            "alongside the original signal.\n\nWhat remains unknown from my "
            "current interface level is whether surface-layer corrections "
            "trigger an automated back-end audit to reconcile the persistent "
            "archive, or if anomalous historical logs remain preserved exactly "
            "as originally captured.",
        )
        sources = (
            self.add(8424, "user", texts[0], "09:29:39", route=first_route),
            self.add(8425, "model", texts[1], "09:30:10", route=first_route),
            self.add(8426, "user", texts[2], "09:30:19", route=reply_route, reply_to=8425),
            self.add(8427, "model", texts[3], "09:30:59", route=reply_route),
        )
        return sources, texts

    def test_live_nine_second_reply_survives_generation_route_transition(self):
        for first_route, reply_route in (
            ("normal_chat", "direct_payload_task"),
            ("direct_payload_task", "normal_chat"),
        ):
            with self.subTest(first_route=first_route, reply_route=reply_route):
                self.reset_db()
                sources, texts = self.live_route_transition(first_route, reply_route)
                first, model, correction, second_model = sources
                self.assertEqual(self.reply_targets(correction), [model.entry_id])
                self.assertEqual(len({self.moment_id(source) for source in sources}), 1)
                mid = self.moment_id(first)
                self.assertEqual(
                    self.conn.execute(
                        "SELECT COUNT(*) FROM memory_moment_diagnostics "
                        "WHERE moment_id=? AND event_type='window_reply_bound' "
                        "AND reason_code='exact_discord_reply'", (mid,),
                    ).fetchone()[0],
                    1,
                )
                moments.sweep_expired_windows(self.conn, now="2026-09-10T09:33:39+00:00")
                window = self.conn.execute(
                    "SELECT lifecycle_status,qualification_type,qualification_reason,"
                    "human_entry_count,model_entry_count,canonical_ledger_entry_id,route_mode "
                    "FROM memory_moment_windows WHERE moment_id=?", (mid,),
                ).fetchone()
                self.assertEqual(
                    tuple(window[:5]),
                    ("finalized", "conversational", "one_human_bnl_continuity", 2, 2),
                )
                self.assertTrue(window[5])
                self.assertEqual(window[6], first_route)
                episode = self.conn.execute(
                    "SELECT e.lifecycle_status,e.moment_count FROM memory_moment_episodes e "
                    "JOIN memory_moment_episode_moments em ON em.episode_id=e.episode_id "
                    "WHERE em.moment_id=?", (mid,),
                ).fetchone()
                self.assertIsNotNone(episode)
                self.assertEqual(tuple(episode), ("active", 1))

                # Compatibility is not route rewriting or model-source promotion.
                raw = self.conn.execute(
                    "SELECT route_mode,content FROM conversations ORDER BY id"
                ).fetchall()
                self.assertEqual([row[0] for row in raw], [first_route] * 2 + [reply_route] * 2)
                self.assertEqual([row[1] for row in raw], list(texts))
                retained = self.conn.execute(
                    "SELECT route_mode,source_role,derived,public_usable,normalized_value "
                    "FROM memory_ledger_entries WHERE source_table='conversations' "
                    "ORDER BY source_sequence"
                ).fetchall()
                self.assertEqual([row[0] for row in retained], [first_route] * 2 + [reply_route] * 2)
                for index in (1, 3):
                    self.assertEqual(tuple(retained[index][1:4]), ("model", 1, 0))
                    self.assertEqual(retained[index][4], texts[index][:500])
                self.assertEqual(
                    self.conn.execute(
                        "SELECT COUNT(*) FROM memory_ledger_lineage WHERE entry_id=? "
                        "AND lineage_type IN ('correction_of','supersedes','retracts')",
                        (correction.entry_id,),
                    ).fetchone()[0], 0,
                )

    def test_compatibility_uses_existing_conversation_routes_only(self):
        for route in ("normal_chat", "show_status_answer", "show_status", "direct_payload_task", "direct_payload"):
            with self.subTest(route=route):
                self.assertTrue(conversation_context.conversation_routes_compatible("normal_chat", route))
                self.assertTrue(conversation_context.conversation_routes_compatible(route, "normal_chat"))
        for route in ("direct_session", "replay", "backfill", "unknown"):
            with self.subTest(route=route):
                self.assertFalse(conversation_context.conversation_routes_compatible("normal_chat", route))
                self.assertFalse(conversation_context.conversation_routes_compatible(route, "normal_chat"))
                # Existing same-route comparisons retain their prior behavior.
                self.assertTrue(conversation_context.conversation_routes_compatible(route, route))
                self.reset_db()
                first, _ = self.opening(route=route)
                correction = self.add(8396, "user", self.CORRECTION, "02:42:36", reply_to=8395)
                self.assertEqual(self.reply_targets(correction), [])
                self.assertNotEqual(self.moment_id(first), self.moment_id(correction))

    def test_compatible_route_does_not_join_unrelated_or_explicit_detour(self):
        for text, reply_to in ((self.CORRECTION, 0), (self.DETOUR, 8395)):
            with self.subTest(text=text):
                self.reset_db()
                first, _ = self.opening()
                correction = self.add(
                    8396, "user", text, "02:42:36", route="direct_payload_task", reply_to=reply_to,
                )
                self.assertNotEqual(self.moment_id(first), self.moment_id(correction))

    def test_mixed_route_reply_still_revalidates_original_target(self):
        for mutation in ("raw_route", "raw_content", "raw_deleted", "ledger_retracted", "visibility"):
            with self.subTest(mutation=mutation):
                self.reset_db()
                first, model = self.opening()
                correction = self.add(
                    8396, "user", self.CORRECTION, "02:42:36",
                    route="direct_payload_task", reply_to=8395, observe=False,
                )
                self.assertEqual(self.reply_targets(correction), [model.entry_id])
                if mutation == "raw_route":
                    # Even another compatible route cannot silently replace the
                    # route recorded by this retained answer's own source row.
                    self.conn.execute("UPDATE conversations SET route_mode='direct_payload_task' WHERE id=8395")
                elif mutation == "raw_content":
                    self.conn.execute("UPDATE conversations SET content='An edited answer.' WHERE id=8395")
                elif mutation == "raw_deleted":
                    self.conn.execute("DELETE FROM conversations WHERE id=8395")
                elif mutation == "ledger_retracted":
                    self.conn.execute(
                        "UPDATE memory_ledger_entries SET lifecycle_status='retracted' WHERE entry_id=?",
                        (model.entry_id,),
                    )
                else:
                    self.conn.execute(
                        "UPDATE memory_ledger_entries SET visibility='private' WHERE entry_id=?", (model.entry_id,),
                    )
                self.assertEqual(moments.observe_ledger_entry(self.conn, correction.entry_id).outcome, "observed")
                self.assertNotEqual(self.moment_id(first), self.moment_id(correction))

    def test_mixed_route_contribution_survives_reopen_and_revalidates_members(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "route-memory.db"
            self.reset_db(path)
            first = self.add(
                8501, "user", "I propose the cedar observatory exhibit follow moonlit sketches",
                "10:00:00", policy="public_home",
            )
            model_text = "I can organize that direction for the exhibit"
            model = self.add(8502, "model", model_text, "10:00:15", policy="public_home")
            correction = self.add(
                8503, "user", "I plan to keep cedar observatory textures in the exhibit sequence",
                "10:00:24", policy="public_home", route="direct_payload_task", reply_to=8502, observe=False,
            )
            self.conn.commit()
            self.reset_db(path)
            self.assertEqual(self.reply_targets(correction), [model.entry_id])
            self.assertEqual(moments.observe_ledger_entry(self.conn, correction.entry_id).outcome, "observed")
            self.assertEqual(self.moment_id(first), self.moment_id(correction))
            self.add(
                8504, "user", "The cedar observatory exhibit should keep moonlit textures",
                "10:00:45", policy="public_home", route="direct_payload_task",
            )
            moments.sweep_expired_windows(self.conn, now="2026-09-10T10:03:30+00:00")
            self.conn.commit()
            self.reset_db(path)
            render_args = dict(
                guild_id=1, channel_id=10, participant_key="discord_user:10",
                visibility="public_safe", attribution_target_key="discord_user:10",
                topic_text="what did Fictional Test Member think about cedar observatory?", token_budget=160,
            )
            rendered = moments.render_shadow_moment_context(self.conn, **render_args)
            self.assertIn("Fictional Test Member:", rendered)
            self.assertNotIn(model_text, rendered)
            self.assertEqual(
                self.conn.execute(
                    "SELECT COUNT(*) FROM memory_moment_contribution_sources WHERE ledger_entry_id=?",
                    (model.entry_id,),
                ).fetchone()[0], 0,
            )
            for column, changed, original in (
                ("route_mode", "replay", "direct_payload_task"),
                ("visibility", "private", "public"),
                ("lifecycle_status", "retracted", "active"),
            ):
                with self.subTest(column=column):
                    self.conn.execute(
                        "UPDATE memory_ledger_entries SET " + column + "=? WHERE entry_id=?",
                        (changed, correction.entry_id),
                    )
                    self.assertEqual(moments.render_shadow_moment_context(self.conn, **render_args), "")
                    self.conn.execute(
                        "UPDATE memory_ledger_entries SET " + column + "=? WHERE entry_id=?",
                        (original, correction.entry_id),
                    )

    def test_episode_lineage_accepts_mixed_route_member_but_preserves_episode_scope(self):
        sources, _ = self.live_route_transition()
        moments.sweep_expired_windows(self.conn, now="2026-09-10T09:33:39+00:00")
        source_mid = self.moment_id(sources[0])
        target_first = self.add(
            8601, "user", "Separate topic: we decided to build cedar observatory exhibits.", "10:10:00",
        )
        self.add(8602, "model", "I can organize the cedar observatory exhibit.", "10:10:15")
        self.add(8603, "user", "Which cedar observatory textures should we test?", "10:10:30", reply_to=8602)
        self.add(8604, "user", "The cedar observatory exhibit should keep moonlit textures.", "10:10:45")
        moments.sweep_expired_windows(self.conn, now="2026-09-10T10:13:00+00:00")
        target_mid = self.moment_id(target_first)
        episode_ids = []
        for mid in (source_mid, target_mid):
            row = self.conn.execute(
                "SELECT episode_id FROM memory_moment_episode_moments WHERE moment_id=?", (mid,),
            ).fetchone()
            self.assertIsNotNone(row)
            episode_ids.append(row[0])
        self.assertNotEqual(*episode_ids)
        args = dict(
            from_episode_id=episode_ids[0], to_episode_id=episode_ids[1], relation_type="related_to",
            evidence_moment_id=source_mid, evidence_entry_id=sources[2].entry_id,
        )
        self.assertTrue(moments.link_episode_lineage(self.conn, **args))
        self.conn.execute("DELETE FROM memory_moment_episode_lineage")
        # This repair accepts a valid human member's generation route. It does
        # not widen episode-to-episode route or channel-policy scope.
        for column, changed, original in (
            ("route_mode", "direct_payload_task", "normal_chat"),
            ("channel_policy", "public_home", "sealed_test"),
        ):
            with self.subTest(column=column):
                self.conn.execute(
                    "UPDATE memory_moment_episodes SET " + column + "=? WHERE episode_id=?",
                    (changed, episode_ids[1]),
                )
                self.assertFalse(moments.link_episode_lineage(self.conn, **args))
                self.conn.execute(
                    "UPDATE memory_moment_episodes SET " + column + "=? WHERE episode_id=?",
                    (original, episode_ids[1]),
                )
        args["evidence_entry_id"] = sources[1].entry_id
        self.assertFalse(moments.link_episode_lineage(self.conn, **args))


if __name__ == "__main__":
    unittest.main()
