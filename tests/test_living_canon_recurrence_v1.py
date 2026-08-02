import hashlib
import json
import sqlite3
import unittest
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from unittest import mock

import bnl_memory_ledger as ledger
import bnl_moment_engine as moments


class LivingCanonRecurrenceV1Tests(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        ledger.ensure_memory_ledger_schema(self.conn)
        moments.ensure_moment_schema(self.conn)
        self.conn.execute(
            """
            CREATE TABLE conversations (
              id INTEGER PRIMARY KEY,guild_id INTEGER,user_id INTEGER,
              user_name TEXT,role TEXT,content TEXT,channel_id INTEGER,
              channel_name TEXT,channel_policy TEXT,route_mode TEXT,
              timestamp TEXT
            )
            """
        )
        self.v1_env = {
            ledger.MEMORY_LEDGER_SHADOW_ENV: "true",
            ledger.LIVING_CANON_V1_FORMATION_ENV: "true",
        }

    def tearDown(self):
        self.conn.close()

    def add_conversation(
        self,
        row_id,
        text,
        observed_at,
        *,
        route_mode="normal_chat",
        user_id=7,
        user_name="Test Member",
        role="user",
    ):
        self.conn.execute(
            "INSERT INTO conversations VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (
                row_id,
                1,
                user_id,
                user_name,
                role,
                text,
                10,
                "barcode-bot",
                "public_home",
                route_mode,
                observed_at,
            ),
        )
        result = ledger.shadow_conversation_row(
            self.conn,
            row_id=row_id,
            user_id=user_id,
            user_name=user_name,
            guild_id=1,
            role=role,
            content=text,
            channel_name="barcode-bot",
            channel_policy="public_home",
            channel_id=10,
            route_mode=route_mode,
            observed_at=observed_at,
            source_sequence=row_id,
            environ={ledger.MEMORY_LEDGER_SHADOW_ENV: "true"},
        )
        self.assertIn(result.outcome, {"inserted", "deduplicated"})
        return result.entry_id

    def ensure_journal_receipt_schema(self):
        self.conn.execute(
            """
            CREATE TABLE bnl_journal_source_events (
              event_seq INTEGER PRIMARY KEY AUTOINCREMENT,
              guild_id INTEGER NOT NULL,source_kind TEXT NOT NULL,
              source_key TEXT NOT NULL,occurred_at_ms INTEGER NOT NULL,
              ingested_at_ms INTEGER NOT NULL,channel_id INTEGER,
              channel_policy TEXT NOT NULL,subject_ref TEXT NOT NULL,
              private_display_name TEXT NOT NULL,raw_text TEXT NOT NULL,
              sanitized_summary TEXT NOT NULL,content_hash TEXT NOT NULL,
              public_usable INTEGER NOT NULL,metadata_json TEXT NOT NULL,
              UNIQUE(guild_id,source_kind,source_key)
            )
            """
        )
        self.conn.executescript(
            """
            CREATE TRIGGER trg_bnl_journal_sources_no_duplicate_insert
            BEFORE INSERT ON bnl_journal_source_events
            WHEN EXISTS (
              SELECT 1 FROM bnl_journal_source_events
              WHERE event_seq=NEW.event_seq OR (
                guild_id=NEW.guild_id AND source_kind=NEW.source_kind
                AND source_key=NEW.source_key
              )
            ) BEGIN
              SELECT RAISE(ABORT,'bnl_journal_source_events_duplicate_identity');
            END;
            CREATE TRIGGER trg_bnl_journal_sources_no_update
            BEFORE UPDATE ON bnl_journal_source_events BEGIN
              SELECT RAISE(ABORT,'bnl_journal_source_events_immutable');
            END;
            CREATE TRIGGER trg_bnl_journal_sources_no_delete
            BEFORE DELETE ON bnl_journal_source_events BEGIN
              SELECT RAISE(ABORT,'bnl_journal_source_events_immutable');
            END;
            """
        )

    def add_continuity_receipt(self, row_id, text, observed_at):
        observed = datetime.fromisoformat(observed_at)
        metadata = {
            "conversationRowId": row_id,
            "routeMode": "conversation_continuity",
            "source": "discord_backfill",
        }
        self.conn.execute(
            """
            INSERT INTO bnl_journal_source_events(
              guild_id,source_kind,source_key,occurred_at_ms,ingested_at_ms,
              channel_id,channel_policy,subject_ref,private_display_name,
              raw_text,sanitized_summary,content_hash,public_usable,
              metadata_json
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                1, "discord_message", "legacy_row:%s" % row_id,
                int(observed.timestamp() * 1000),
                int(observed.timestamp() * 1000) + 1, 10, "public_home",
                "discord_user:7", "Test Member", text, "inert summary",
                hashlib.sha256(text.encode("utf-8")).hexdigest(), 1,
                json.dumps(metadata, sort_keys=True, separators=(",", ":")),
            ),
        )

    def add_moment(self, moment_id, roots, observed_at, *, edge_roots=None):
        source_rows = []
        for root in roots:
            row = self.conn.execute(
                """
                SELECT entry_id,source_role,subject_key,subject_display_name,
                       source_sequence,observed_at
                FROM memory_ledger_entries WHERE entry_id=?
                """,
                (root,),
            ).fetchone()
            self.assertIsNotNone(row)
            source_rows.append(row)
        participant_records = {}
        for row in source_rows:
            role_name = "human_author" if row[1] == "user" else "bnl_participant"
            participant_key = row[2] if row[1] == "user" else ledger.BNL_SUBJECT_KEY
            participant_name = row[3] if row[1] == "user" else "BNL-01"
            participant_records.setdefault(
                (participant_key, role_name),
                {
                    "name": participant_name,
                    "first": row[5],
                    "last": row[5],
                    "authored": 0,
                    "order": len(participant_records),
                },
            )
            record = participant_records[(participant_key, role_name)]
            record["first"] = min(record["first"], row[5])
            record["last"] = max(record["last"], row[5])
            record["authored"] += int(row[1] == "user")
        canonical = ledger.insert_ledger_entry(
            self.conn,
            ledger.LedgerEntry(
                guild_id=1,
                source_table="memory_moment_windows",
                source_row_id=moment_id,
                source_revision="1",
                source_role="derived_assessment",
                entry_type="shared_moment",
                subject_key="moment:%s" % moment_id,
                predicate_key="shared_moment",
                value="Content-free recurrence fixture.",
                source_class=ledger.SourceClass.DERIVED_SUMMARY,
                route_mode="normal_chat",
                channel_id=10,
                channel_name="barcode-bot",
                channel_policy="public_home",
                visibility=ledger.Visibility.PUBLIC,
                confidence=ledger.Confidence.LOW,
                public_usable=True,
                derived=True,
                projection=True,
                observed_at=observed_at,
                lifecycle_status="review_only",
                participants=tuple(
                    ledger.LedgerParticipant(
                        participant_key,
                        record["name"],
                        role_name,
                        record["order"],
                    )
                    for (participant_key, role_name), record
                    in participant_records.items()
                ),
                lineage=tuple(("derived_from", root) for root in roots),
            ),
        )
        self.conn.execute(
            """
            INSERT INTO memory_moment_windows(
              moment_id,guild_id,channel_id,channel_name,channel_policy,
              route_mode,topic_key,window_started_at,last_activity_at,
              lifecycle_status,visibility,public_usable,salience,
              human_entry_count,model_entry_count,participant_count,summary,
              created_at,updated_at,canonical_ledger_entry_id
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                moment_id, 1, 10, "barcode-bot", "public_home", "normal_chat",
                "topic-recurrence", observed_at, observed_at, "finalized",
                "public", 1, 0.5,
                sum(int(row[1] == "user") for row in source_rows),
                sum(int(row[1] != "user") for row in source_rows),
                len(participant_records), "", observed_at,
                observed_at, canonical.entry_id,
            ),
        )
        exact_edge_roots = set(roots if edge_roots is None else edge_roots)
        for root, source in zip(roots, source_rows):
            membership_role = (
                "human_author" if source[1] == "user" else "bnl_participant"
            )
            self.conn.execute(
                "INSERT INTO memory_moment_members VALUES(?,?,?,?,?,?)",
                (
                    moment_id,
                    root,
                    source[4],
                    source[5],
                    membership_role,
                    observed_at,
                ),
            )
            if root in exact_edge_roots:
                self.conn.execute(
                    "INSERT INTO memory_ledger_lineage VALUES(?,?,?,?,?)",
                    (root, 1, "part_of_moment", canonical.entry_id, observed_at),
                )
        for (participant_key, role_name), record in participant_records.items():
            self.conn.execute(
                """
                INSERT INTO memory_moment_participants(
                  moment_id,participant_key,safe_display_name,participant_role,
                  first_seen_at,last_seen_at,authored_entry_count,
                  participation_order,created_at,updated_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    moment_id,
                    participant_key,
                    record["name"],
                    role_name,
                    record["first"],
                    record["last"],
                    record["authored"],
                    record["order"],
                    observed_at,
                    observed_at,
                ),
            )

    def form(self, trigger):
        return ledger.form_atomic_candidates_from_recurring_conversation(
            self.conn,
            trigger_entry_id=trigger,
            environ=self.v1_env,
        )

    def candidate(self):
        return self.conn.execute(
            """
            SELECT candidate_id,candidate_state,reinforcement_count,
                   eligible_independent_root_count,
                   independent_occurrence_count,predicate_key,
                   recurrence_contract_version,grouping_signature_version,
                   public_usable,recurrence_proof_json
            FROM memory_ledger_knowledge_candidates
            WHERE recurrence_contract_version=?
            ORDER BY created_at,candidate_id LIMIT 1
            """,
            (ledger.LIVING_CANON_RECURRENCE_VERSION,),
        ).fetchone()

    def test_v1_is_default_off_and_legacy_singleton_stays_unformed(self):
        root = self.add_conversation(
            1,
            "I keep tuning ceramic antennas during field experiments.",
            "2026-07-20T10:00:00+00:00",
        )
        legacy_env = {
            ledger.MEMORY_LEDGER_SHADOW_ENV: "true",
            ledger.CONVERSATION_MOTIF_FORMATION_ENV: "true",
        }
        self.assertEqual(
            ledger.form_atomic_candidates_from_recurring_conversation(
                self.conn,
                trigger_entry_id=root,
                environ=legacy_env,
            ),
            [],
        )
        self.assertEqual(self.candidate(), None)

        self.conn.execute(
            "INSERT INTO conversations VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (
                99, 1, 9, "Retained Member", "user",
                "I arrange a memory palace beside the violet archive doorway.",
                10, "barcode-bot", "public_home", "normal_chat",
                "2026-07-21T10:00:00+00:00",
            ),
        )
        both_env = {
            ledger.MEMORY_LEDGER_SHADOW_ENV: "true",
            ledger.CONVERSATION_MOTIF_FORMATION_ENV: "true",
            ledger.LIVING_CANON_V1_FORMATION_ENV: "true",
        }
        self.assertEqual(
            ledger.form_atomic_candidates_from_recurring_conversation(
                self.conn,
                guild_id=1,
                subject_key="discord_user:9",
                environ=both_env,
            ),
            [],
        )
        self.assertEqual(
            self.conn.execute(
                "SELECT COUNT(*) FROM memory_ledger_entries "
                "WHERE subject_key='discord_user:9'"
            ).fetchone()[0],
            0,
        )

    def test_one_occurrence_is_provisional_and_two_establish(self):
        first = self.add_conversation(
            2,
            "I tune ceramic antennas with copper meshes during field experiments.",
            "2026-07-20T10:00:00+00:00",
        )
        created = self.form(first)
        self.assertEqual(len(created), 1)
        row = self.candidate()
        self.assertEqual(row[1:5], ("provisional", 1, 1, 1))
        self.assertEqual(row[6], ledger.LIVING_CANON_RECURRENCE_VERSION)
        self.assertEqual(row[7], ledger.LIVING_CANON_GROUPING_SIGNATURE_VERSION)
        self.assertEqual(row[8], 0)

        second = self.add_conversation(
            3,
            "We tune the ceramic antenna with a copper mesh during the field experiment.",
            "2026-07-21T10:00:00+00:00",
        )
        replay = self.form(second)
        self.assertEqual(len(replay), 1)
        row = self.candidate()
        self.assertEqual(row[1:5], ("established", 2, 2, 2))
        proof = json.loads(row[9])
        self.assertTrue(proof["candidate_eligible"])
        self.assertEqual(self.form(second)[0].candidate_id, row[0])

    def test_curated_family_and_neutral_groups_share_v1_recurrence_standard(self):
        family_one = self.add_conversation(
            5000,
            "I tune the synth patch and drum mix for the radio track.",
            "2026-07-20T10:00:00+00:00",
        )
        self.form(family_one)
        family_predicate = "conversation_motif_music_production"
        family_row = self.conn.execute(
            """
            SELECT candidate_state,recurrence_contract_version,
                   grouping_signature_version,eligible_independent_root_count,
                   independent_occurrence_count,canon_domain,canon_claim_kind
            FROM memory_ledger_knowledge_candidates WHERE predicate_key=?
            """,
            (family_predicate,),
        ).fetchone()
        self.assertEqual(family_row[0], "provisional")

        family_two = self.add_conversation(
            5001,
            "The synth patch and drum mix shape the radio track.",
            "2026-07-21T10:00:00+00:00",
        )
        self.form(family_two)
        family_row = self.conn.execute(
            """
            SELECT candidate_state,recurrence_contract_version,
                   grouping_signature_version,eligible_independent_root_count,
                   independent_occurrence_count,canon_domain,canon_claim_kind
            FROM memory_ledger_knowledge_candidates WHERE predicate_key=?
              AND candidate_state!='superseded'
            ORDER BY created_at DESC LIMIT 1
            """,
            (family_predicate,),
        ).fetchone()
        self.assertEqual(family_row[0], "established")

        neutral_one = self.add_conversation(
            5002,
            "I tune ceramic antennas with copper meshes during field experiments.",
            "2026-07-22T10:00:00+00:00",
        )
        self.form(neutral_one)
        neutral_predicate = ledger._conversation_motif_neutral_predicate(
            ledger._conversation_motif_exact_signature(
                "I tune ceramic antennas with copper meshes during field experiments."
            ),
            subject_key="discord_user:7",
        )
        neutral_two = self.add_conversation(
            5003,
            "We tune the ceramic antenna with a copper mesh during the field experiment.",
            "2026-07-23T10:00:00+00:00",
        )
        self.form(neutral_two)
        neutral_row = self.conn.execute(
            """
            SELECT candidate_state,recurrence_contract_version,
                   grouping_signature_version,eligible_independent_root_count,
                   independent_occurrence_count,canon_domain,canon_claim_kind
            FROM memory_ledger_knowledge_candidates WHERE predicate_key=?
              AND candidate_state!='superseded'
            ORDER BY created_at DESC LIMIT 1
            """,
            (neutral_predicate,),
        ).fetchone()
        self.assertEqual(neutral_row[0], "established")
        self.assertEqual(family_row[1:], neutral_row[1:])
        self.assertEqual(
            family_row[1:],
            (
                ledger.LIVING_CANON_RECURRENCE_VERSION,
                ledger.LIVING_CANON_GROUPING_SIGNATURE_VERSION,
                2,
                2,
                "real_community",
                "behavior_pattern",
            ),
        )

    def test_same_exchange_collapses_to_one_occurrence(self):
        self.add_conversation(
            4,
            "I tune ceramic antennas with copper meshes during field experiments.",
            "2026-07-20T10:00:00+00:00",
        )
        second = self.add_conversation(
            5,
            "We tune the ceramic antenna with a copper mesh during the field experiment.",
            "2026-07-20T10:05:00+00:00",
        )
        self.form(second)
        row = self.candidate()
        self.assertEqual(row[1], "provisional")
        self.assertEqual(row[4], 1)

    def test_occurrence_boundaries_cross_midnight_and_exact_thirty_minutes(self):
        cross_midnight = (
            self.add_conversation(
                5100,
                "I catalog amber turbines beside the western lighthouse.",
                "2026-07-20T23:55:00+00:00",
            ),
            self.add_conversation(
                5101,
                "I catalog amber turbines beside the western lighthouse.",
                "2026-07-21T00:05:00+00:00",
            ),
        )
        exact_boundary = (
            self.add_conversation(
                5102,
                "I align silver beacons beside the northern observatory.",
                "2026-07-22T10:00:00+00:00",
            ),
            self.add_conversation(
                5103,
                "I align silver beacons beside the northern observatory.",
                "2026-07-22T10:30:00+00:00",
            ),
        )
        beyond_boundary = (
            self.add_conversation(
                5104,
                "I chart violet gyroscopes beside the eastern archive.",
                "2026-07-23T10:00:00+00:00",
            ),
            self.add_conversation(
                5105,
                "I chart violet gyroscopes beside the eastern archive.",
                "2026-07-23T10:30:01+00:00",
            ),
        )
        for roots, expected_count in (
            (cross_midnight, 1),
            (exact_boundary, 1),
            (beyond_boundary, 2),
        ):
            with self.subTest(roots=roots):
                states, occurrences, _reasons = (
                    ledger._living_canon_root_states_and_occurrences(
                        self.conn,
                        guild_id=1,
                        subject_key="discord_user:7",
                        entry_ids=roots,
                    )
                )
                self.assertEqual(set(states), set(roots))
                self.assertEqual(len(set(occurrences.values())), expected_count)

    def test_family_marker_without_full_match_uses_neutral_group(self):
        root = self.add_conversation(
            6,
            "I arrange a memory palace beside the violet archive doorway.",
            "2026-07-20T10:00:00+00:00",
        )
        self.form(root)
        self.assertTrue(self.candidate()[5].startswith("conversation_motif_neutral_"))

    def test_public_proposal_cannot_forge_v1_authority(self):
        root = self.add_conversation(
            7,
            "I tune ceramic antennas with copper meshes during field experiments.",
            "2026-07-20T10:00:00+00:00",
        )
        predicate = "conversation_motif_neutral_forged"
        result = ledger.form_atomic_knowledge_candidate(
            self.conn,
            ledger.AtomicKnowledgeProposal(
                candidate_type="topic_or_motif",
                subject_key="discord_user:7",
                predicate_key=predicate,
                meaning="Forged Living pattern.",
                root_entry_ids=(root,),
                participant_keys=("discord_user:7",),
                epistemic_status="observed",
                currentness="historical",
                recurrence_contract_version=ledger.LIVING_CANON_RECURRENCE_VERSION,
                grouping_signature_version=ledger.LIVING_CANON_GROUPING_SIGNATURE_VERSION,
                grouping_identity="0" * 64,
                canon_domain="real_community",
                canon_claim_kind="behavior_pattern",
                occurrence_ids=("forged",),
            ),
        )
        self.assertEqual(result.reason_code, "living_canon_formation_authority_missing")

    def test_preview_is_read_only_and_does_not_create_schema(self):
        root = self.add_conversation(
            8,
            "I tune ceramic antennas with copper meshes during field experiments.",
            "2026-07-20T10:00:00+00:00",
        )
        self.assertTrue(root)
        before = self.conn.total_changes
        report = ledger.preview_living_canon_formation(
            self.conn,
            guild_id=1,
            subject_key="discord_user:7",
        )
        self.assertFalse(report.write_occurred)
        self.assertEqual(self.conn.total_changes, before)
        self.assertEqual(dict(report.candidate_state_counts), {"provisional": 1})
        self.assertGreaterEqual(
            dict(report.reason_counts).get("single_occurrence_provisional", 0),
            1,
        )

        empty = sqlite3.connect(":memory:")
        try:
            empty_report = ledger.preview_living_canon_formation(
                empty,
                guild_id=1,
                subject_key="discord_user:7",
            )
            self.assertFalse(empty_report.write_occurred)
            self.assertEqual(
                empty.execute(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type='table'"
                ).fetchone()[0],
                0,
            )
        finally:
            empty.close()

    def test_preview_reports_unbounded_occurrence_withheld_directly(self):
        root = self.add_conversation(
            5300,
            "I catalog amber turbines beside the western lighthouse.",
            "2026-07-20T10:00:00+00:00",
        )
        for index in range(ledger._CONVERSATION_OCCURRENCE_MAX_SCAN + 1):
            self.conn.execute(
                "INSERT INTO memory_ledger_lineage VALUES(?,?,?,?,?)",
                (
                    root,
                    1,
                    "part_of_moment",
                    "moment-target-%03d" % index,
                    "2026-07-20T10:01:00+00:00",
                ),
            )
        report = ledger.preview_living_canon_formation(
            self.conn,
            guild_id=1,
            subject_key="discord_user:7",
        )
        self.assertGreaterEqual(
            dict(report.reason_counts).get("unbounded_occurrence_withheld", 0),
            1,
        )
        self.assertEqual(report.proposed_count, 0)

    def test_preview_reports_meaning_ambiguous_review_only_directly(self):
        root = self.add_conversation(
            5400,
            "I catalog amber turbines beside the western lighthouse.",
            "2026-07-20T10:00:00+00:00",
        )
        states, occurrences, _reasons = (
            ledger._living_canon_root_states_and_occurrences(
                self.conn,
                guild_id=1,
                subject_key="discord_user:7",
                entry_ids=(root,),
            )
        )
        rebound = replace(
            states[root],
            text="I align silver beacons beside the northern observatory.",
        )
        with mock.patch.object(
            ledger,
            "_living_canon_root_states_and_occurrences",
            return_value=({root: rebound}, occurrences, ()),
        ):
            report = ledger.preview_living_canon_formation(
                self.conn,
                guild_id=1,
                subject_key="discord_user:7",
            )
        self.assertGreaterEqual(
            dict(report.reason_counts).get("meaning_ambiguous_review_only", 0),
            1,
        )
        self.assertGreaterEqual(report.ambiguous_count, 1)
        self.assertEqual(report.proposed_count, 0)

    def test_raw_occurrence_and_candidate_stay_stable_after_moments(self):
        roots = [
            self.add_conversation(
                20 + index,
                "I tune ceramic antennas with copper meshes during field experiments.",
                "2026-07-20T10:%02d:00+00:00" % (index * 5),
            )
            for index in range(3)
        ]
        states, occurrences, _ = ledger._living_canon_root_states_and_occurrences(
            self.conn,
            guild_id=1,
            subject_key="discord_user:7",
            entry_ids=tuple(roots),
        )
        self.assertEqual(len(states), 3)
        raw_occurrence = occurrences[roots[0]]
        self.assertEqual(set(occurrences.values()), {raw_occurrence})
        candidate_id = self.form(roots[-1])[0].candidate_id

        self.add_moment("moment-a", (roots[0],), "2026-07-20T10:20:00+00:00")
        self.add_moment(
            "moment-b", tuple(roots[1:]), "2026-07-20T10:21:00+00:00"
        )
        states, occurrences, _ = ledger._living_canon_root_states_and_occurrences(
            self.conn,
            guild_id=1,
            subject_key="discord_user:7",
            entry_ids=tuple(roots),
        )
        self.assertEqual(len(states), 3)
        self.assertEqual(set(occurrences.values()), {raw_occurrence})
        self.assertEqual(self.form(roots[-1])[0].candidate_id, candidate_id)

        hostile_one = self.add_conversation(
            40,
            "I chart cobalt weather vanes beside the eastern observatory.",
            "2026-07-22T10:00:00+00:00",
        )
        hostile_two = self.add_conversation(
            41,
            "I chart cobalt weather vanes beside the eastern observatory.",
            "2026-07-23T10:00:00+00:00",
        )
        self.add_moment(
            "forged-member",
            (hostile_one, hostile_two),
            "2026-07-23T10:10:00+00:00",
            edge_roots=(hostile_one,),
        )
        states, occurrences, reasons = (
            ledger._living_canon_root_states_and_occurrences(
                self.conn,
                guild_id=1,
                subject_key="discord_user:7",
                entry_ids=(hostile_one, hostile_two),
            )
        )
        self.assertEqual(len(states), 2)
        self.assertEqual(occurrences, {})
        self.assertIn("moment_lifecycle_or_membership_ineligible", reasons)
        hostile_report = ledger.preview_living_canon_formation(
            self.conn, guild_id=1, subject_key="discord_user:7"
        )
        self.assertGreaterEqual(hostile_report.rejected_count, 1)
        self.assertGreaterEqual(
            dict(hostile_report.reason_counts).get(
                "moment_lifecycle_or_membership_ineligible", 0
            ),
            1,
        )

    def test_neutral_correction_fence_requires_two_fresh_occurrences(self):
        old = self.add_conversation(
            30,
            "I tune ceramic antennas with copper meshes during field experiments.",
            "2026-07-20T10:00:00+00:00",
        )
        self.form(old)
        predicate = self.candidate()[5]
        self.conn.execute(
            """
            INSERT INTO memory_ledger_conversation_motif_fences(
              guild_id,subject_key,predicate_key,correction_entry_id,
              correction_observed_at,reason_code,fence_state,satisfied_at,
              created_at,updated_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?)
            """,
            (
                1, "discord_user:7", predicate, "correction-fixture",
                "2026-07-21T10:00:00+00:00",
                "conversation_motif_correction_ambiguous", "active", "",
                "2026-07-21T10:00:00+00:00",
                "2026-07-21T10:00:00+00:00",
            ),
        )
        fresh_one = self.add_conversation(
            31,
            "I tune ceramic antennas with copper meshes during field experiments.",
            "2026-07-22T10:00:00+00:00",
        )
        self.form(fresh_one)
        self.assertNotEqual(self.candidate()[1], "established")
        self.assertEqual(
            self.conn.execute(
                "SELECT fence_state FROM memory_ledger_conversation_motif_fences "
                "WHERE guild_id=1 AND subject_key='discord_user:7' AND predicate_key=?",
                (predicate,),
            ).fetchone()[0],
            "active",
        )
        fresh_two = self.add_conversation(
            32,
            "We tune the ceramic antenna with a copper mesh during the field experiment.",
            "2026-07-23T10:00:00+00:00",
        )
        self.form(fresh_two)
        self.assertEqual(self.candidate()[1], "established")
        self.assertEqual(
            self.conn.execute(
                "SELECT fence_state FROM memory_ledger_conversation_motif_fences "
                "WHERE guild_id=1 AND subject_key='discord_user:7' AND predicate_key=?",
                (predicate,),
            ).fetchone()[0],
            "satisfied",
        )

    def test_overlapping_valid_moments_collapse_one_connected_component(self):
        roots = [
            self.add_conversation(
                100 + index,
                "I chart cobalt weather vanes beside the eastern observatory.",
                "2026-07-%02dT10:00:00+00:00" % (20 + index),
            )
            for index in range(3)
        ]
        self.add_moment("overlap-a", tuple(roots[:2]), "2026-07-22T11:00:00+00:00")
        self.add_moment("overlap-b", tuple(roots[1:]), "2026-07-22T12:00:00+00:00")

        states, occurrences, reasons = (
            ledger._living_canon_root_states_and_occurrences(
                self.conn,
                guild_id=1,
                subject_key="discord_user:7",
                entry_ids=tuple(roots),
            )
        )
        self.assertEqual(len(states), 3)
        self.assertEqual(len(set(occurrences.values())), 1)
        self.assertIn("overlapping_occurrence_representation_collapsed", reasons)

        formed = self.form(roots[-1])
        self.assertEqual(len(formed), 1)
        self.assertEqual(self.candidate()[1], "provisional")
        self.assertEqual(self.candidate()[4], 1)
        report = ledger.preview_living_canon_formation(
            self.conn, guild_id=1, subject_key="discord_user:7"
        )
        report_reasons = dict(report.reason_counts)
        self.assertGreaterEqual(
            report_reasons.get("overlapping_occurrence_representation_collapsed", 0),
            1,
        )
        self.assertGreaterEqual(report_reasons.get("same_occurrence_collapsed", 0), 1)

    def test_producer_shaped_shared_moment_selects_only_resolved_human_roots(self):
        target_one = self.add_conversation(
            130,
            "I tune ceramic antennas with copper meshes during field experiments.",
            "2026-07-20T10:00:00+00:00",
        )
        other_human = self.add_conversation(
            131,
            "I compare ceramic antenna meshes during the same field experiment.",
            "2026-07-20T10:01:00+00:00",
            user_id=8,
            user_name="Other Member",
        )
        bnl_turn = self.add_conversation(
            132,
            "The antenna comparison can stay scoped to the public experiment.",
            "2026-07-20T10:02:00+00:00",
            user_id=0,
            user_name="BNL-01",
            role="model",
        )
        target_two = self.add_conversation(
            133,
            "We tune the ceramic antenna with a copper mesh during the field experiment.",
            "2026-07-20T10:03:00+00:00",
        )
        self.add_moment(
            "producer-shaped-shared",
            (target_one, other_human, bnl_turn, target_two),
            "2026-07-20T10:04:00+00:00",
        )
        target_id = self.conn.execute(
            "SELECT canonical_ledger_entry_id FROM memory_moment_windows "
            "WHERE moment_id='producer-shaped-shared'"
        ).fetchone()[0]

        self.assertEqual(
            ledger._living_canon_validated_moment_members(
                self.conn,
                guild_id=1,
                subject_key="discord_user:7",
                target_id=target_id,
            ),
            tuple(sorted((target_one, target_two))),
        )
        self.assertEqual(
            ledger._living_canon_validated_moment_members(
                self.conn,
                guild_id=1,
                subject_key="discord_user:8",
                target_id=target_id,
            ),
            (other_human,),
        )
        states, occurrences, reasons = (
            ledger._living_canon_root_states_and_occurrences(
                self.conn,
                guild_id=1,
                subject_key="discord_user:7",
                entry_ids=(target_one, target_two),
            )
        )
        self.assertEqual(set(states), {target_one, target_two})
        self.assertEqual(len(set(occurrences.values())), 1)
        self.assertIn("same_root_projection_collapsed", reasons)

    def test_current_moment_producer_is_recurrence_compatible(self):
        with mock.patch.dict(
            moments.os.environ,
            {
                ledger.MEMORY_LEDGER_SHADOW_ENV: "1",
                moments.MOMENT_ENGINE_SHADOW_ENV: "1",
            },
        ):
            human_roots = (
                self.add_conversation(
                    5500,
                    "The synth patch needs a warmer bass in the radio mix.",
                    "2026-07-20T10:00:00+00:00",
                ),
                self.add_conversation(
                    5502,
                    "The synth patch should keep the bass warm in the radio mix.",
                    "2026-07-20T10:02:00+00:00",
                ),
                self.add_conversation(
                    5503,
                    "Let us test the synth patch against the radio chorus.",
                    "2026-07-20T10:03:00+00:00",
                ),
            )
            bnl_root = self.add_conversation(
                5501,
                "I can compare the synth bass against the radio chorus.",
                "2026-07-20T10:01:00+00:00",
                user_id=0,
                user_name="BNL-01",
                role="model",
            )
            for root in (human_roots[0], bnl_root, *human_roots[1:]):
                self.assertIn(
                    moments.observe_ledger_entry(self.conn, root).outcome,
                    {"observed", "deduplicated"},
                )
            moments.sweep_expired_windows(
                self.conn,
                now="2026-07-20T10:06:00+00:00",
            )
        target_id = self.conn.execute(
            """
            SELECT canonical_ledger_entry_id FROM memory_moment_windows
            WHERE lifecycle_status='finalized'
            ORDER BY window_started_at DESC LIMIT 1
            """
        ).fetchone()[0]
        self.assertEqual(
            ledger._living_canon_validated_moment_members(
                self.conn,
                guild_id=1,
                subject_key="discord_user:7",
                target_id=target_id,
            ),
            tuple(sorted(human_roots)),
        )

    def test_group_first_preview_has_bounded_select_count(self):
        started = datetime(2023, 1, 1, tzinfo=timezone.utc)
        for index in range(1200):
            self.add_conversation(
                2000 + index,
                "I arrange archive token %04d beside the violet doorway." % index,
                (started + timedelta(hours=index)).isoformat(),
            )
        statements = []
        self.conn.set_trace_callback(
            lambda sql: statements.append(sql)
            if sql.lstrip().upper().startswith("SELECT")
            else None
        )
        try:
            report = ledger.preview_living_canon_formation(
                self.conn,
                guild_id=1,
                subject_key="discord_user:7",
                max_scan=1200,
            )
        finally:
            self.conn.set_trace_callback(None)
        self.assertLessEqual(report.proposed_count, 6)
        self.assertLessEqual(len(statements), 256)

    def test_continuity_requires_exact_immutable_journal_receipt(self):
        text = "I tune ceramic antennas with copper meshes during field experiments."
        first_time = "2026-07-20T10:00:00+00:00"
        first = self.add_conversation(
            4000, text, first_time, route_mode="conversation_continuity"
        )
        self.assertEqual(self.form(first), [])

        self.ensure_journal_receipt_schema()
        self.add_continuity_receipt(4000, text, first_time)
        second_time = "2026-07-21T10:00:00+00:00"
        second = self.add_conversation(
            4001, text, second_time, route_mode="conversation_continuity"
        )
        self.add_continuity_receipt(4001, text, second_time)
        formed = self.form(second)
        self.assertEqual(len(formed), 1)
        self.assertEqual(self.candidate()[1], "established")

    def test_temp_shadow_cannot_bypass_main_correction_fence(self):
        first = self.add_conversation(
            4100,
            "I tune ceramic antennas with copper meshes during field experiments.",
            "2026-07-20T10:00:00+00:00",
        )
        second = self.add_conversation(
            4101,
            "We tune the ceramic antenna with a copper mesh during the field experiment.",
            "2026-07-21T10:00:00+00:00",
        )
        self.form(second)
        predicate = self.candidate()[5]
        self.conn.execute(
            """
            INSERT INTO main.memory_ledger_conversation_motif_fences
            VALUES(?,?,?,?,?,?,?,?,?,?)
            """,
            (
                1, "discord_user:7", predicate, "main-correction",
                "2026-07-22T10:00:00+00:00",
                "conversation_motif_correction_ambiguous", "active", "",
                "2026-07-22T10:00:00+00:00", "2026-07-22T10:00:00+00:00",
            ),
        )
        ledger._withhold_conversation_motif_candidates(
            self.conn,
            guild_id=1,
            subject_key="discord_user:7",
            predicate_keys=(predicate,),
            correction_entry_id="main-correction",
            reason_code="conversation_motif_correction_ambiguous",
        )
        self.conn.execute(
            """
            CREATE TEMP TABLE memory_ledger_conversation_motif_fences(
              guild_id,subject_key,predicate_key,correction_entry_id,
              correction_observed_at,reason_code,fence_state,satisfied_at,
              created_at,updated_at
            )
            """
        )
        self.assertEqual(self.form(second), [])
        state = self.conn.execute(
            """
            SELECT candidate_state,public_usable,candidate_eligible
            FROM main.memory_ledger_knowledge_candidates
            WHERE predicate_key=?
            """,
            (predicate,),
        ).fetchone()
        self.assertEqual(state, ("contested", 0, 0))

    def test_newer_active_wildcard_overrides_old_satisfied_exact_fence(self):
        old = self.add_conversation(
            4200,
            "I tune ceramic antennas with copper meshes during field experiments.",
            "2026-07-20T10:00:00+00:00",
        )
        self.form(old)
        predicate = self.candidate()[5]
        self.conn.execute(
            "INSERT INTO memory_ledger_conversation_motif_fences VALUES(?,?,?,?,?,?,?,?,?,?)",
            (
                1, "discord_user:7", predicate, "old-correction",
                "2026-07-19T10:00:00+00:00", "conversation_motif_correction_ambiguous",
                "satisfied", "2026-07-19T11:00:00+00:00",
                "2026-07-19T10:00:00+00:00", "2026-07-19T11:00:00+00:00",
            ),
        )
        self.conn.execute(
            "INSERT INTO memory_ledger_conversation_motif_fences VALUES(?,?,?,?,?,?,?,?,?,?)",
            (
                1, "discord_user:7", ledger._CONVERSATION_MOTIF_NEUTRAL_FENCE_WILDCARD,
                "new-wildcard", "2026-07-21T10:00:00+00:00",
                "conversation_motif_correction_ambiguous", "active", "",
                "2026-07-21T10:00:00+00:00", "2026-07-21T10:00:00+00:00",
            ),
        )
        ledger._withhold_conversation_motif_candidates(
            self.conn,
            guild_id=1,
            subject_key="discord_user:7",
            predicate_keys=(ledger._CONVERSATION_MOTIF_NEUTRAL_FENCE_WILDCARD,),
            correction_entry_id="new-wildcard",
            reason_code="conversation_motif_correction_ambiguous",
        )
        fresh = self.add_conversation(
            4201,
            "I tune ceramic antennas with copper meshes during field experiments.",
            "2026-07-22T10:00:00+00:00",
        )
        self.form(fresh)
        self.assertNotEqual(self.candidate()[1], "established")
        report = ledger.preview_living_canon_formation(
            self.conn, guild_id=1, subject_key="discord_user:7"
        )
        self.assertGreaterEqual(
            dict(report.reason_counts).get(
                "fresh_recurrence_required_after_correction", 0
            ),
            1,
        )
        self.assertGreaterEqual(
            dict(report.reason_counts).get("correction_fence_active", 0),
            1,
        )

    def test_preview_plans_unsynchronized_correction_without_source_writes(self):
        self.add_conversation(
            5200,
            "I tune ceramic antennas with copper meshes during field experiments.",
            "2026-07-20T10:00:00+00:00",
        )
        self.add_conversation(
            5201,
            "We tune the ceramic antenna with a copper mesh during the field experiment.",
            "2026-07-21T10:00:00+00:00",
        )
        correction = self.add_conversation(
            5202,
            "Actually, that is wrong; disregard that pattern.",
            "2026-07-22T10:00:00+00:00",
        )
        before_changes = self.conn.total_changes
        before_fences = self.conn.execute(
            "SELECT COUNT(*) FROM memory_ledger_conversation_motif_fences"
        ).fetchone()[0]

        report = ledger.preview_living_canon_formation(
            self.conn,
            guild_id=1,
            subject_key="discord_user:7",
        )

        self.assertEqual(report.proposed_count, 0)
        self.assertFalse(report.write_occurred)
        self.assertEqual(report.source_write_count, 0)
        self.assertEqual(self.conn.total_changes, before_changes)
        self.assertEqual(
            self.conn.execute(
                "SELECT COUNT(*) FROM memory_ledger_conversation_motif_fences"
            ).fetchone()[0],
            before_fences,
        )
        reasons = dict(report.reason_counts)
        self.assertGreaterEqual(reasons.get("correction_fence_active", 0), 1)
        self.assertGreaterEqual(
            reasons.get("fresh_recurrence_required_after_correction", 0),
            1,
        )
        self.assertEqual(self.form(correction), [])
        self.assertGreater(
            self.conn.execute(
                "SELECT COUNT(*) FROM memory_ledger_conversation_motif_fences"
            ).fetchone()[0],
            before_fences,
        )

    def test_predicate_correction_does_not_fence_unrelated_neutral_topic(self):
        antenna = self.add_conversation(
            4300,
            "I tune ceramic antennas with copper meshes during field experiments.",
            "2026-07-20T10:00:00+00:00",
        )
        weather = self.add_conversation(
            4301,
            "I chart cobalt gyroscope bearings beside the eastern observatory.",
            "2026-07-20T11:00:00+00:00",
        )
        self.form(antenna)
        self.form(weather)
        antenna_predicate = ledger._conversation_motif_neutral_predicate(
            ledger._conversation_motif_exact_signature(
                "I tune ceramic antennas with copper meshes during field experiments."
            ),
            subject_key="discord_user:7",
        )
        weather_predicate = ledger._conversation_motif_neutral_predicate(
            ledger._conversation_motif_exact_signature(
                "I chart cobalt gyroscope bearings beside the eastern observatory."
            ),
            subject_key="discord_user:7",
        )
        self.conn.execute(
            "INSERT INTO memory_ledger_conversation_motif_fences VALUES(?,?,?,?,?,?,?,?,?,?)",
            (
                1, "discord_user:7", antenna_predicate, "antenna-correction",
                "2026-07-21T10:00:00+00:00",
                "conversation_motif_correction_ambiguous", "active", "",
                "2026-07-21T10:00:00+00:00", "2026-07-21T10:00:00+00:00",
            ),
        )
        weather_fresh = self.add_conversation(
            4302,
            "I chart cobalt gyroscope bearings beside the eastern observatory.",
            "2026-07-22T11:00:00+00:00",
        )
        self.form(weather_fresh)
        weather_row = self.conn.execute(
            """
            SELECT candidate_state,eligible_independent_root_count
            FROM memory_ledger_knowledge_candidates
            WHERE predicate_key=? AND recurrence_contract_version=?
            ORDER BY created_at LIMIT 1
            """,
            (weather_predicate, ledger.LIVING_CANON_RECURRENCE_VERSION),
        ).fetchone()
        self.assertEqual(weather_row, ("established", 2))

    def test_active_singleton_provisionals_are_bounded_across_restart(self):
        started = datetime(2026, 7, 1, tzinfo=timezone.utc)
        for index in range(10):
            root = self.add_conversation(
                4400 + index,
                "I arrange unique archive token %02d beside violet doorway." % index,
                (started + timedelta(hours=index)).isoformat(),
            )
            self.form(root)
        self.assertLessEqual(
            self.conn.execute(
                """
                SELECT COUNT(*) FROM memory_ledger_knowledge_candidates
                WHERE recurrence_contract_version=? AND candidate_state='provisional'
                """,
                (ledger.LIVING_CANON_RECURRENCE_VERSION,),
            ).fetchone()[0],
            6,
        )
        self.conn.commit()
        restarted = sqlite3.connect(":memory:")
        self.conn.backup(restarted)
        self.conn.close()
        self.conn = restarted
        for index in range(10, 20):
            root = self.add_conversation(
                4400 + index,
                "I arrange unique archive token %02d beside violet doorway." % index,
                (started + timedelta(hours=index)).isoformat(),
            )
            self.form(root)
        self.assertLessEqual(
            self.conn.execute(
                """
                SELECT COUNT(*) FROM memory_ledger_knowledge_candidates
                WHERE recurrence_contract_version=? AND candidate_state='provisional'
                """,
                (ledger.LIVING_CANON_RECURRENCE_VERSION,),
            ).fetchone()[0],
            6,
        )

    def test_immediate_source_invalidation_clears_public_proof(self):
        first = self.add_conversation(
            4500,
            "I tune ceramic antennas with copper meshes during field experiments.",
            "2026-07-20T10:00:00+00:00",
        )
        second = self.add_conversation(
            4501,
            "We tune the ceramic antenna with a copper mesh during the field experiment.",
            "2026-07-21T10:00:00+00:00",
        )
        self.form(second)
        self.conn.commit()
        baseline = sqlite3.connect(":memory:")
        self.conn.backup(baseline)
        mutations = (
            ("delete", lambda conn: conn.execute(
                "DELETE FROM memory_ledger_entries WHERE entry_id=?", (first,)
            )),
            ("privacy", lambda conn: conn.execute(
                "UPDATE memory_ledger_entries SET public_usable=0 WHERE entry_id=?",
                (first,),
            )),
            ("correction", lambda conn: conn.execute(
                "INSERT INTO memory_ledger_lineage VALUES(?,?,?,?,?)",
                ("correction-fixture", 1, "correction_of", first, "now"),
            )),
            ("supersession", lambda conn: conn.execute(
                "INSERT INTO memory_ledger_lineage VALUES(?,?,?,?,?)",
                ("supersession-fixture", 1, "supersedes", first, "now"),
            )),
            ("retraction", lambda conn: conn.execute(
                "INSERT INTO memory_ledger_lineage VALUES(?,?,?,?,?)",
                ("retraction-fixture", 1, "retracts", first, "now"),
            )),
        )
        for name, mutation in mutations:
            with self.subTest(name=name):
                clone = sqlite3.connect(":memory:")
                baseline.backup(clone)
                mutation(clone)
                public_usable, proof_json = clone.execute(
                    """
                    SELECT public_usable,recurrence_proof_json
                    FROM memory_ledger_knowledge_candidates
                    WHERE recurrence_contract_version=?
                    """,
                    (ledger.LIVING_CANON_RECURRENCE_VERSION,),
                ).fetchone()
                self.assertEqual(public_usable, 0)
                proof = json.loads(proof_json)
                self.assertFalse(proof["candidate_eligible"])
                self.assertFalse(proof["roots_valid"])
                clone.close()
        baseline.close()

    def test_source_invalidation_preserves_legacy_public_and_proof_fields(self):
        root = self.add_conversation(
            4550,
            "I maintain the public archive index for weekly field reports.",
            "2026-07-20T10:00:00+00:00",
        )
        result = ledger.form_atomic_knowledge_candidate(
            self.conn,
            ledger.AtomicKnowledgeProposal(
                candidate_type="topic_or_motif",
                subject_key="discord_user:7",
                predicate_key="archive_index_role",
                meaning="Maintains the public archive index.",
                root_entry_ids=(root,),
                participant_keys=("discord_user:7",),
                epistemic_status="stated",
                currentness="current",
                retrieval_tags=("recurring_public_conversation",),
            ),
        )
        self.assertEqual(result.outcome, "created")
        fence_source = self.add_conversation(
            4551,
            "Correction source for the archive-index fixture.",
            "2026-07-21T10:00:00+00:00",
        )
        self.conn.execute(
            """
            INSERT INTO memory_ledger_conversation_motif_fences(
              guild_id,subject_key,predicate_key,correction_entry_id,
              correction_observed_at,reason_code,fence_state,satisfied_at,
              created_at,updated_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?)
            """,
            (
                1, "discord_user:7", "archive_index_role", fence_source,
                "2026-07-21T10:00:00+00:00",
                "conversation_motif_correction_ambiguous", "active", "",
                "2026-07-21T10:00:00+00:00",
                "2026-07-21T10:00:00+00:00",
            ),
        )
        legacy_proof = '{"legacy_contract":"preserve_exactly"}'
        self.conn.execute(
            """
            UPDATE memory_ledger_knowledge_candidates
            SET public_usable=1,recurrence_proof_json=?
            WHERE candidate_id=?
            """,
            (legacy_proof, result.candidate_id),
        )
        self.conn.commit()
        baseline = sqlite3.connect(":memory:")
        self.conn.backup(baseline)
        mutations = (
            ("root_delete", lambda conn: conn.execute(
                "DELETE FROM memory_ledger_entries WHERE entry_id=?", (root,)
            )),
            ("privacy_change", lambda conn: conn.execute(
                "UPDATE memory_ledger_entries SET public_usable=0 WHERE entry_id=?",
                (root,),
            )),
            ("participant_delete", lambda conn: conn.execute(
                "DELETE FROM memory_ledger_participants "
                "WHERE entry_id=? AND participant_key='discord_user:7'",
                (root,),
            )),
            ("lineage_change", lambda conn: conn.execute(
                "INSERT INTO memory_ledger_lineage VALUES(?,?,?,?,?)",
                ("legacy-correction", 1, "correction_of", root, "now"),
            )),
            ("fence_source_delete", lambda conn: conn.execute(
                "DELETE FROM memory_ledger_entries WHERE entry_id=?",
                (fence_source,),
            )),
            ("correction_withhold", lambda conn: (
                ledger._withhold_conversation_motif_candidates(
                    conn,
                    guild_id=1,
                    subject_key="discord_user:7",
                    predicate_keys=("archive_index_role",),
                    correction_entry_id=fence_source,
                    reason_code="conversation_motif_correction_ambiguous",
                )
            )),
        )
        for name, mutation in mutations:
            with self.subTest(name=name):
                clone = sqlite3.connect(":memory:")
                baseline.backup(clone)
                mutation(clone)
                row = clone.execute(
                    """
                    SELECT candidate_state,recurrence_contract_version,
                           public_usable,recurrence_proof_json
                    FROM memory_ledger_knowledge_candidates
                    WHERE candidate_id=?
                    """,
                    (result.candidate_id,),
                ).fetchone()
                self.assertIn(row[0], {"invalidated", "contested"})
                self.assertEqual(row[1], "")
                self.assertEqual(row[2:], (1, legacy_proof))
                clone.close()
        baseline.close()

    def test_candidate_identity_is_incremental_replay_invariant_and_regenerates(self):
        self.conn.commit()
        empty = sqlite3.connect(":memory:")
        self.conn.backup(empty)
        first = self.add_conversation(
            4600,
            "I tune ceramic antennas with copper meshes during field experiments.",
            "2026-07-20T10:00:00+00:00",
        )
        self.form(first)
        second = self.add_conversation(
            4601,
            "We tune the ceramic antenna with a copper mesh during the field experiment.",
            "2026-07-21T10:00:00+00:00",
        )
        self.form(second)
        columns = (
            "candidate_id,candidate_state,reinforcement_count,"
            "eligible_independent_root_count,independent_occurrence_count,"
            "predicate_key,recurrence_contract_version,"
            "grouping_signature_version,grouping_identity,root_digest,"
            "occurrence_digest,public_usable,recurrence_proof_json"
        )
        incremental = self.conn.execute(
            "SELECT %s FROM memory_ledger_knowledge_candidates "
            "WHERE recurrence_contract_version=?" % columns,
            (ledger.LIVING_CANON_RECURRENCE_VERSION,),
        ).fetchone()

        incremental_conn = self.conn
        replay = sqlite3.connect(":memory:")
        empty.backup(replay)
        self.conn = replay
        replay_first = self.add_conversation(
            4600,
            "I tune ceramic antennas with copper meshes during field experiments.",
            "2026-07-20T10:00:00+00:00",
        )
        replay_second = self.add_conversation(
            4601,
            "We tune the ceramic antenna with a copper mesh during the field experiment.",
            "2026-07-21T10:00:00+00:00",
        )
        self.form(replay_second)
        clean_replay = replay.execute(
            "SELECT %s FROM memory_ledger_knowledge_candidates "
            "WHERE recurrence_contract_version=?" % columns,
            (ledger.LIVING_CANON_RECURRENCE_VERSION,),
        ).fetchone()
        self.assertEqual(clean_replay, incremental)
        replay.close()
        empty.close()
        self.conn = incremental_conn

        self.conn.execute(
            "DELETE FROM memory_ledger_entries WHERE entry_id=?", (first,)
        )
        fresh_one = self.add_conversation(
            4602,
            "I tune ceramic antennas with copper meshes during field experiments.",
            "2026-07-22T10:00:00+00:00",
        )
        fresh_two = self.add_conversation(
            4603,
            "We tune the ceramic antenna with a copper mesh during the field experiment.",
            "2026-07-23T10:00:00+00:00",
        )
        self.form(fresh_two)
        generations = self.conn.execute(
            """
            SELECT candidate_id,candidate_state
            FROM memory_ledger_knowledge_candidates
            WHERE recurrence_contract_version=? ORDER BY created_at,candidate_id
            """,
            (ledger.LIVING_CANON_RECURRENCE_VERSION,),
        ).fetchall()
        self.assertEqual(len(generations), 2)
        self.assertNotEqual(generations[0][0], generations[1][0])
        self.assertEqual({row[1] for row in generations}, {"invalidated", "established"})

    def test_preview_reports_content_free_rejection_and_contested_reasons(self):
        first = self.add_conversation(
            4700,
            "I tune ceramic antennas with copper meshes during field experiments.",
            "2026-07-20T10:00:00+00:00",
        )
        second = self.add_conversation(
            4701,
            "We tune the ceramic antenna with a copper mesh during the field experiment.",
            "2026-07-21T10:00:00+00:00",
        )
        self.form(second)
        self.conn.execute(
            """
            UPDATE memory_ledger_knowledge_candidates
            SET candidate_state='contested',candidate_eligible=0,
                public_usable=0,invalidated_reason='unresolved_contradiction'
            WHERE recurrence_contract_version=?
            """,
            (ledger.LIVING_CANON_RECURRENCE_VERSION,),
        )
        ledger.insert_ledger_entry(
            self.conn,
            ledger.LedgerEntry(
                guild_id=1,source_table="conversations",source_row_id=4702,
                source_revision="4702",source_role="user",entry_type="observation",
                subject_key="discord_user:7",subject_display_name="Test Member",
                predicate_key="conversation",value="Private excluded fixture text.",
                source_class=ledger.SourceClass.PUBLIC_OBSERVATION,
                route_mode="normal_chat",channel_id=10,channel_name="private",
                channel_policy="private",visibility=ledger.Visibility.PRIVATE,
                confidence=ledger.Confidence.MEDIUM,public_usable=False,
                observed_at="2026-07-22T10:00:00+00:00",source_sequence=4702,
                participants=(ledger.LedgerParticipant(
                    "discord_user:7","Test Member","author",0
                ),),
            ),
        )
        ledger.insert_ledger_entry(
            self.conn,
            ledger.LedgerEntry(
                guild_id=1,source_table="conversations",source_row_id=4703,
                source_revision="4703",source_role="derived_assessment",
                entry_type="observation",subject_key="discord_user:7",
                subject_display_name="Test Member",predicate_key="conversation",
                value="Derived excluded fixture text.",
                source_class=ledger.SourceClass.DERIVED_SUMMARY,
                route_mode="normal_chat",channel_id=10,channel_name="barcode-bot",
                channel_policy="public_home",visibility=ledger.Visibility.PUBLIC,
                confidence=ledger.Confidence.LOW,public_usable=False,
                derived=True,projection=True,
                observed_at="2026-07-23T10:00:00+00:00",source_sequence=4703,
            ),
        )
        report = ledger.preview_living_canon_formation(
            self.conn, guild_id=1, subject_key="discord_user:7"
        )
        reasons = dict(report.reason_counts)
        self.assertGreaterEqual(reasons.get("visibility_ineligible", 0), 1)
        self.assertGreaterEqual(reasons.get("derived_source_not_independent", 0), 1)
        self.assertGreaterEqual(reasons.get("contradiction_contested", 0), 1)

    def test_preview_and_untriggered_formation_share_group_ranking(self):
        self.add_conversation(
            4800,
            "I tune ceramic antennas with copper meshes during field experiments.",
            "2026-07-20T10:00:00+00:00",
        )
        self.add_conversation(
            4801,
            "I chart cobalt gyroscope bearings beside the eastern observatory.",
            "2026-07-20T11:00:00+00:00",
        )
        preview = ledger.preview_living_canon_formation(
            self.conn, guild_id=1, subject_key="discord_user:7"
        )
        formed = ledger.form_atomic_candidates_from_recurring_conversation(
            self.conn,
            guild_id=1,
            subject_key="discord_user:7",
            environ=self.v1_env,
        )
        self.assertEqual(len(formed), preview.proposed_count)
        states = dict(preview.candidate_state_counts)
        self.assertEqual(states, {"provisional": len(formed)})


if __name__ == "__main__":
    unittest.main()
