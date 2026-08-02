import sqlite3
import unittest
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from bnl_canon_source_contract import Confidence, SourceClass, Visibility
import bnl_memory_ledger as ledger
import bnl_moment_engine as moments
import bnl_unified_intelligence_packet as packet_module


@dataclass(frozen=True)
class _Observation:
    entry_id: str
    row_id: int
    guild_id: int
    user_id: int
    user_name: str
    message_id: int
    route_mode: str
    observed_at: str

    @property
    def subject_key(self) -> str:
        return ledger.subject_key_for_user(self.user_id)


class _RootStateFixture:
    def __init__(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.conn.execute(
            """
            CREATE TABLE conversations (
              id INTEGER PRIMARY KEY,
              guild_id INTEGER NOT NULL,
              user_id INTEGER NOT NULL,
              user_name TEXT NOT NULL,
              role TEXT NOT NULL,
              content TEXT NOT NULL,
              channel_id INTEGER NOT NULL,
              channel_name TEXT NOT NULL,
              channel_policy TEXT NOT NULL,
              message_id INTEGER,
              route_mode TEXT NOT NULL,
              public_usable INTEGER NOT NULL,
              visibility TEXT NOT NULL,
              timestamp TEXT NOT NULL
            )
            """
        )
        ledger.ensure_memory_ledger_schema(self.conn)
        moments.ensure_moment_schema(self.conn)

    def close(self) -> None:
        self.conn.close()

    def add_observation(
        self,
        *,
        row_id: int = 1,
        guild_id: int = 1,
        user_id: int = 7,
        user_name: str = "Test Member",
        route_mode: str = "normal_chat",
        content: str = "I compare audio mixes before choosing a final release plan.",
        observed_at: str | None = None,
    ) -> _Observation:
        timestamp = observed_at or (
            datetime(2026, 7, 1, tzinfo=timezone.utc)
            + timedelta(hours=row_id)
        ).isoformat()
        message_id = 10_000 + row_id
        self.conn.execute(
            """
            INSERT INTO conversations(
              id,guild_id,user_id,user_name,role,content,channel_id,
              channel_name,channel_policy,message_id,route_mode,
              public_usable,visibility,timestamp
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                row_id,
                guild_id,
                user_id,
                user_name,
                "user",
                content,
                10,
                "barcode-bot",
                "public_home",
                message_id,
                route_mode,
                1,
                Visibility.PUBLIC.value,
                timestamp,
            ),
        )
        result = ledger.shadow_conversation_row(
            self.conn,
            row_id=row_id,
            user_id=user_id,
            user_name=user_name,
            guild_id=guild_id,
            role="user",
            content=content,
            channel_name="barcode-bot",
            channel_policy="public_home",
            channel_id=10,
            message_id=message_id,
            route_mode=route_mode,
            observed_at=timestamp,
            source_sequence=row_id,
        )
        if result.outcome != "inserted":
            raise AssertionError(result)
        return _Observation(
            entry_id=result.entry_id,
            row_id=row_id,
            guild_id=guild_id,
            user_id=user_id,
            user_name=user_name,
            message_id=message_id,
            route_mode=route_mode,
            observed_at=timestamp,
        )

    def state(self, observation: _Observation, *, entry_id: str | None = None):
        return ledger.read_public_assessment_root_state(
            self.conn,
            entry_id=entry_id or observation.entry_id,
            guild_id=observation.guild_id,
            subject_key=observation.subject_key,
        )

    def add_lineage(
        self,
        *,
        entry_id: str,
        guild_id: int,
        lineage_type: str,
        target_entry_id: str,
    ) -> None:
        self.conn.execute(
            """
            INSERT INTO memory_ledger_lineage(
              entry_id,guild_id,lineage_type,target_entry_id,created_at
            ) VALUES(?,?,?,?,?)
            """,
            (
                entry_id,
                guild_id,
                lineage_type,
                target_entry_id,
                "2026-07-02T00:00:00+00:00",
            ),
        )

    def add_moment(
        self,
        root: _Observation,
        *,
        moment_id: str,
        guild_id: int | None = None,
        participant_key: str | None = None,
        reverse_link: bool = True,
    ) -> str:
        target_guild = root.guild_id if guild_id is None else guild_id
        participant = participant_key or root.subject_key
        result = ledger.insert_ledger_entry(
            self.conn,
            ledger.LedgerEntry(
                guild_id=target_guild,
                source_table="memory_moment_windows",
                source_row_id=moment_id,
                source_revision="1",
                source_role="derived_assessment",
                entry_type="shared_moment",
                subject_key=f"moment:{moment_id}",
                predicate_key="shared_moment",
                value='{"summary":"A bounded public Moment."}',
                source_class=SourceClass.DERIVED_SUMMARY,
                route_mode="normal_chat",
                channel_id=10,
                channel_name="barcode-bot",
                channel_policy="public_home",
                visibility=Visibility.PUBLIC,
                confidence=Confidence.LOW,
                public_usable=True,
                derived=True,
                projection=True,
                observed_at=root.observed_at,
                source_sequence=0,
                lifecycle_status=ledger.REVIEW_ONLY_LIFECYCLE,
                participants=(
                    ledger.LedgerParticipant(
                        participant,
                        "Test Member" if participant == root.subject_key else "Other Member",
                        "participant",
                        0,
                    ),
                ),
                lineage=(
                    (("derived_from", root.entry_id),)
                    if reverse_link
                    else ()
                ),
            ),
        )
        if result.outcome != "inserted":
            raise AssertionError(result)
        self.conn.execute(
            """
            INSERT INTO memory_moment_windows(
              moment_id,guild_id,channel_id,channel_name,channel_policy,
              route_mode,topic_key,window_started_at,last_activity_at,
              finalized_at,qualification_type,qualification_reason,
              lifecycle_status,visibility,public_usable,salience,
              human_entry_count,model_entry_count,participant_count,summary,
              created_at,updated_at,topic_family,topic_signature,
              canonical_ledger_entry_id
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                moment_id,
                target_guild,
                10,
                "barcode-bot",
                "public_home",
                "normal_chat",
                "test-topic",
                root.observed_at,
                root.observed_at,
                root.observed_at,
                "conversational",
                "coherent_test_fixture",
                "finalized",
                Visibility.PUBLIC.value,
                1,
                0.5,
                1,
                0,
                1,
                "A bounded public Moment.",
                root.observed_at,
                root.observed_at,
                "",
                "[]",
                result.entry_id,
            ),
        )
        self.conn.execute(
            """
            INSERT INTO memory_moment_members(
              moment_id,ledger_entry_id,source_sequence,observed_at,
              membership_role,created_at
            ) VALUES(?,?,?,?,?,?)
            """,
            (
                moment_id,
                root.entry_id,
                root.row_id,
                root.observed_at,
                "human_author",
                root.observed_at,
            ),
        )
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
                participant,
                "Test Member" if participant == root.subject_key else "Other Member",
                "human_author",
                root.observed_at,
                root.observed_at,
                1,
                0,
                root.observed_at,
                root.observed_at,
            ),
        )
        return result.entry_id


class PublicAssessmentRootStateTests(unittest.TestCase):
    def new_fixture(self) -> tuple[_RootStateFixture, _Observation]:
        fixture = _RootStateFixture()
        root = fixture.add_observation()
        self.addCleanup(fixture.close)
        self.assertIsNotNone(fixture.state(root))
        return fixture, root

    def test_exact_stable_id_schema_revision_and_sequence_are_required(self):
        with self.subTest("stable_id"):
            fixture, root = self.new_fixture()
            forged_id = "mle_forged_public_assessment_root"
            fixture.conn.execute(
                "UPDATE memory_ledger_entries SET entry_id=? WHERE entry_id=?",
                (forged_id, root.entry_id),
            )
            fixture.conn.execute(
                "UPDATE memory_ledger_participants SET entry_id=? WHERE entry_id=?",
                (forged_id, root.entry_id),
            )
            self.assertIsNone(fixture.state(root, entry_id=forged_id))

        with self.subTest("schema_version"):
            fixture, root = self.new_fixture()
            fixture.conn.execute(
                "UPDATE memory_ledger_entries SET schema_version='forged' "
                "WHERE entry_id=?",
                (root.entry_id,),
            )
            self.assertIsNone(fixture.state(root))

        with self.subTest("source_revision"):
            fixture, root = self.new_fixture()
            fixture.conn.execute(
                "UPDATE memory_ledger_entries SET source_revision='forged' "
                "WHERE entry_id=?",
                (root.entry_id,),
            )
            self.assertIsNone(fixture.state(root))

        with self.subTest("message_sequence_is_allowed"):
            fixture, root = self.new_fixture()
            fixture.conn.execute(
                "UPDATE memory_ledger_entries SET source_sequence=? "
                "WHERE entry_id=?",
                (root.message_id, root.entry_id),
            )
            self.assertIsNotNone(fixture.state(root))

        with self.subTest("forged_sequence"):
            fixture, root = self.new_fixture()
            fixture.conn.execute(
                "UPDATE memory_ledger_entries SET source_sequence=? "
                "WHERE entry_id=?",
                (root.message_id + 77, root.entry_id),
            )
            self.assertIsNone(fixture.state(root))

    def test_author_participant_scope_is_exact(self):
        with self.subTest("wrong_author"):
            fixture, root = self.new_fixture()
            fixture.conn.execute(
                "UPDATE memory_ledger_participants SET participant_key=? "
                "WHERE entry_id=? AND participant_role='author'",
                (ledger.subject_key_for_user(8), root.entry_id),
            )
            self.assertIsNone(fixture.state(root))

        with self.subTest("missing_author"):
            fixture, root = self.new_fixture()
            fixture.conn.execute(
                "DELETE FROM memory_ledger_participants WHERE entry_id=?",
                (root.entry_id,),
            )
            self.assertIsNone(fixture.state(root))

        with self.subTest("second_author"):
            fixture, root = self.new_fixture()
            fixture.conn.execute(
                """
                INSERT INTO memory_ledger_participants(
                  entry_id,guild_id,participant_key,display_name,
                  participant_role,order_index,created_at
                ) VALUES(?,?,?,?,?,?,?)
                """,
                (
                    root.entry_id,
                    root.guild_id,
                    ledger.subject_key_for_user(8),
                    "Other Member",
                    "author",
                    1,
                    "2026-07-02T00:00:00+00:00",
                ),
            )
            self.assertIsNone(fixture.state(root))

        with self.subTest("extra_non_author"):
            fixture, root = self.new_fixture()
            fixture.conn.execute(
                """
                INSERT INTO memory_ledger_participants(
                  entry_id,guild_id,participant_key,display_name,
                  participant_role,order_index,created_at
                ) VALUES(?,?,?,?,?,?,?)
                """,
                (
                    root.entry_id,
                    root.guild_id,
                    ledger.subject_key_for_user(8),
                    "Other Member",
                    "observer",
                    1,
                    "2026-07-02T00:00:00+00:00",
                ),
            )
            self.assertIsNone(fixture.state(root))

    def test_visibility_must_match_policy_and_authoritative_raw_state(self):
        with self.subTest("ledger_visibility_drift"):
            fixture, root = self.new_fixture()
            fixture.conn.execute(
                "UPDATE memory_ledger_entries SET visibility='public_safe' "
                "WHERE entry_id=?",
                (root.entry_id,),
            )
            self.assertIsNone(fixture.state(root))

        with self.subTest("ledger_private"):
            fixture, root = self.new_fixture()
            fixture.conn.execute(
                "UPDATE memory_ledger_entries "
                "SET visibility='private',public_usable=0 WHERE entry_id=?",
                (root.entry_id,),
            )
            self.assertIsNone(fixture.state(root))

        with self.subTest("raw_visibility_drift"):
            fixture, root = self.new_fixture()
            fixture.conn.execute(
                "UPDATE conversations SET visibility='public_safe' WHERE id=?",
                (root.row_id,),
            )
            self.assertIsNone(fixture.state(root))

        with self.subTest("raw_public_usable_removed"):
            fixture, root = self.new_fixture()
            fixture.conn.execute(
                "UPDATE conversations SET public_usable=0 WHERE id=?",
                (root.row_id,),
            )
            self.assertIsNone(fixture.state(root))

    def test_every_authoritative_raw_field_is_bound(self):
        mutations = {
            "id": "UPDATE conversations SET id=999 WHERE id=?",
            "guild_id": "UPDATE conversations SET guild_id=2 WHERE id=?",
            "user_id": "UPDATE conversations SET user_id=8 WHERE id=?",
            "user_name": "UPDATE conversations SET user_name='Changed' WHERE id=?",
            "role": "UPDATE conversations SET role='model' WHERE id=?",
            "content": "UPDATE conversations SET content='Changed source text.' WHERE id=?",
            "channel_id": "UPDATE conversations SET channel_id=99 WHERE id=?",
            "channel_name": "UPDATE conversations SET channel_name='changed' WHERE id=?",
            "channel_policy": (
                "UPDATE conversations SET channel_policy='internal_controlled' WHERE id=?"
            ),
            "message_id": "UPDATE conversations SET message_id=99999 WHERE id=?",
            "route_mode": "UPDATE conversations SET route_mode='room' WHERE id=?",
            "timestamp": (
                "UPDATE conversations SET timestamp='2027-01-01T00:00:00+00:00' "
                "WHERE id=?"
            ),
        }
        for field, sql in mutations.items():
            with self.subTest(field=field):
                fixture, root = self.new_fixture()
                fixture.conn.execute(sql, (root.row_id,))
                self.assertIsNone(fixture.state(root))

    def test_authoritative_raw_deletion_fails_closed(self):
        fixture, root = self.new_fixture()
        fixture.conn.execute(
            "DELETE FROM conversations WHERE id=?",
            (root.row_id,),
        )

        self.assertIsNone(fixture.state(root))
        self.assertEqual(
            fixture.conn.execute(
                "SELECT COUNT(*) FROM memory_ledger_entries WHERE entry_id=?",
                (root.entry_id,),
            ).fetchone()[0],
            1,
        )

    def test_hostile_duplicate_lineage_is_rejected(self):
        with self.subTest("self"):
            fixture, root = self.new_fixture()
            fixture.add_lineage(
                entry_id=root.entry_id,
                guild_id=root.guild_id,
                lineage_type="duplicate_of",
                target_entry_id=root.entry_id,
            )
            self.assertIsNone(fixture.state(root))

        with self.subTest("cycle"):
            fixture, root = self.new_fixture()
            other = fixture.add_observation(row_id=2)
            fixture.add_lineage(
                entry_id=root.entry_id,
                guild_id=1,
                lineage_type="duplicate_of",
                target_entry_id=other.entry_id,
            )
            fixture.add_lineage(
                entry_id=other.entry_id,
                guild_id=1,
                lineage_type="duplicate_of",
                target_entry_id=root.entry_id,
            )
            self.assertIsNone(fixture.state(root))

        with self.subTest("multiple_targets"):
            fixture, root = self.new_fixture()
            first = fixture.add_observation(row_id=2)
            second = fixture.add_observation(row_id=3)
            for target in (first, second):
                fixture.add_lineage(
                    entry_id=root.entry_id,
                    guild_id=1,
                    lineage_type="duplicate_of",
                    target_entry_id=target.entry_id,
                )
            self.assertIsNone(fixture.state(root))

        with self.subTest("dangling"):
            fixture, root = self.new_fixture()
            fixture.add_lineage(
                entry_id=root.entry_id,
                guild_id=1,
                lineage_type="duplicate_of",
                target_entry_id="missing-root",
            )
            self.assertIsNone(fixture.state(root))

        with self.subTest("cross_guild_target"):
            fixture, root = self.new_fixture()
            other = fixture.add_observation(row_id=2, guild_id=2)
            fixture.add_lineage(
                entry_id=root.entry_id,
                guild_id=1,
                lineage_type="duplicate_of",
                target_entry_id=other.entry_id,
            )
            self.assertIsNone(fixture.state(root))

        with self.subTest("cross_guild_edge"):
            fixture, root = self.new_fixture()
            other = fixture.add_observation(row_id=2)
            fixture.add_lineage(
                entry_id=root.entry_id,
                guild_id=2,
                lineage_type="duplicate_of",
                target_entry_id=other.entry_id,
            )
            self.assertIsNone(fixture.state(root))

        with self.subTest("cross_subject"):
            fixture, root = self.new_fixture()
            other = fixture.add_observation(row_id=2, user_id=8)
            fixture.add_lineage(
                entry_id=root.entry_id,
                guild_id=1,
                lineage_type="duplicate_of",
                target_entry_id=other.entry_id,
            )
            self.assertIsNone(fixture.state(root))

    def test_one_fully_linked_same_guild_moment_is_valid(self):
        fixture, root = self.new_fixture()
        moment_id = fixture.add_moment(root, moment_id="valid-moment")
        fixture.add_lineage(
            entry_id=root.entry_id,
            guild_id=1,
            lineage_type="part_of_moment",
            target_entry_id=moment_id,
        )

        state = fixture.state(root)

        self.assertIsNotNone(state)
        self.assertTrue(state.occurrence_identity)

    def test_hostile_part_of_moment_lineage_is_rejected(self):
        with self.subTest("self"):
            fixture, root = self.new_fixture()
            fixture.add_lineage(
                entry_id=root.entry_id,
                guild_id=1,
                lineage_type="part_of_moment",
                target_entry_id=root.entry_id,
            )
            self.assertIsNone(fixture.state(root))

        with self.subTest("dangling"):
            fixture, root = self.new_fixture()
            fixture.add_lineage(
                entry_id=root.entry_id,
                guild_id=1,
                lineage_type="part_of_moment",
                target_entry_id="missing-moment",
            )
            self.assertIsNone(fixture.state(root))

        with self.subTest("multiple_targets"):
            fixture, root = self.new_fixture()
            first = fixture.add_moment(root, moment_id="first-moment")
            second = fixture.add_moment(root, moment_id="second-moment")
            for target in (first, second):
                fixture.add_lineage(
                    entry_id=root.entry_id,
                    guild_id=1,
                    lineage_type="part_of_moment",
                    target_entry_id=target,
                )
            self.assertIsNone(fixture.state(root))

        with self.subTest("cross_guild_target"):
            fixture, root = self.new_fixture()
            target = fixture.add_moment(
                root,
                moment_id="foreign-guild-moment",
                guild_id=2,
            )
            fixture.add_lineage(
                entry_id=root.entry_id,
                guild_id=1,
                lineage_type="part_of_moment",
                target_entry_id=target,
            )
            self.assertIsNone(fixture.state(root))

        with self.subTest("cross_guild_edge"):
            fixture, root = self.new_fixture()
            target = fixture.add_moment(root, moment_id="wrong-edge-guild")
            fixture.add_lineage(
                entry_id=root.entry_id,
                guild_id=2,
                lineage_type="part_of_moment",
                target_entry_id=target,
            )
            self.assertIsNone(fixture.state(root))

        with self.subTest("foreign_subject_participation"):
            fixture, root = self.new_fixture()
            target = fixture.add_moment(
                root,
                moment_id="foreign-subject-moment",
                participant_key=ledger.subject_key_for_user(8),
            )
            fixture.add_lineage(
                entry_id=root.entry_id,
                guild_id=1,
                lineage_type="part_of_moment",
                target_entry_id=target,
            )
            self.assertIsNone(fixture.state(root))

        with self.subTest("missing_reverse_derivation"):
            fixture, root = self.new_fixture()
            target = fixture.add_moment(
                root,
                moment_id="unbacked-moment",
                reverse_link=False,
            )
            fixture.add_lineage(
                entry_id=root.entry_id,
                guild_id=1,
                lineage_type="part_of_moment",
                target_entry_id=target,
            )
            self.assertIsNone(fixture.state(root))

        with self.subTest("cycle"):
            fixture, root = self.new_fixture()
            target = fixture.add_moment(root, moment_id="cyclic-moment")
            fixture.add_lineage(
                entry_id=root.entry_id,
                guild_id=1,
                lineage_type="part_of_moment",
                target_entry_id=target,
            )
            fixture.add_lineage(
                entry_id=target,
                guild_id=1,
                lineage_type="part_of_moment",
                target_entry_id=root.entry_id,
            )
            self.assertIsNone(fixture.state(root))

        for field, value in (
            ("source_role", "user"),
            ("derived", 0),
            ("projection", 0),
        ):
            with self.subTest(target_field=field):
                fixture, root = self.new_fixture()
                target = fixture.add_moment(root, moment_id=f"forged-{field}")
                fixture.conn.execute(
                    f"UPDATE memory_ledger_entries SET {field}=? WHERE entry_id=?",
                    (value, target),
                )
                fixture.add_lineage(
                    entry_id=root.entry_id,
                    guild_id=1,
                    lineage_type="part_of_moment",
                    target_entry_id=target,
                )
                self.assertIsNone(fixture.state(root))

    def test_lineage_mutation_after_packet_construction_breaks_revalidation(self):
        fixture, root = self.new_fixture()
        state = fixture.state(root)
        self.assertIsNotNone(state)
        request = packet_module.IntelligencePacketRequest(
            guild_id=1,
            subject_user_id=root.user_id,
            route_mode="normal_chat",
            conversation_surface="mention_or_reply",
            subject_display_name=root.user_name,
            channel_id=10,
            channel_name="barcode-bot",
            channel_policy="public_home",
            visibility_allowance="public_safe",
            user_text="What am I all about?",
            participant_user_ids=(root.user_id,),
            direct_state="direct",
            now="2026-07-03T00:00:00+00:00",
        )
        item = packet_module.IntelligencePacketItem(
            lane="assessment_observation",
            source_class=SourceClass.PUBLIC_OBSERVATION.value,
            source_type="public_assessment_observation",
            source_ref=f"ledger:{root.entry_id}",
            source_digest=state.source_digest,
            subject_key=root.subject_key,
            predicate_key="public_assessment_observation",
            text=state.text[:240],
            visibility=state.visibility,
            confidence=Confidence.LOW.value,
            lifecycle="active",
            authority=3,
            participants=(root.subject_key,),
            lineage=(root.entry_id,),
            observed_at=state.observed_at,
            usage="assessment_only",
            revalidation_kind="public_assessment",
            revalidation_key=root.entry_id,
            root_identities=(state.root_identity,),
            occurrence_identities=(state.occurrence_identity,),
            point_identity=state.semantics.point_identity,
            point_group_identity=state.semantics.point_identity,
            attribution_mode=state.semantics.attribution_mode,
            polarity=state.semantics.polarity,
            action_identity=state.semantics.action_identity,
            material_facets=state.semantics.material_facets,
        )
        packet = packet_module.UnifiedIntelligencePacket(
            schema_version=packet_module.SCHEMA_VERSION,
            packet_id="root-state-revalidation-test",
            request=request,
            items=(item,),
            validation_items=(item,),
            exclusions=(),
            diagnostics=packet_module.IntelligencePacketDiagnostics(),
        )
        before = packet_module.revalidate_packet(fixture.conn, packet)
        self.assertTrue(before.valid)

        fixture.add_lineage(
            entry_id=root.entry_id,
            guild_id=1,
            lineage_type="duplicate_of",
            target_entry_id="hostile-late-root",
        )
        after = packet_module.revalidate_packet(fixture.conn, packet)

        self.assertFalse(after.valid)
        self.assertEqual(after.status, "source_changed")

    def test_only_approved_source_routes_are_admitted(self):
        for route in ("normal_chat", "conversation_continuity"):
            with self.subTest(route=route):
                fixture = _RootStateFixture()
                self.addCleanup(fixture.close)
                root = fixture.add_observation(route_mode=route)
                self.assertIsNotNone(fixture.state(root))

        for route in (
            "room",
            "approved_channel_history",
            "operator_command",
            "unknown",
        ):
            with self.subTest(route=route):
                fixture = _RootStateFixture()
                self.addCleanup(fixture.close)
                root = fixture.add_observation(route_mode=route)
                self.assertIsNone(fixture.state(root))

    def test_temp_schema_lookalikes_cannot_override_main_authority(self):
        fixture, root = self.new_fixture()
        before = fixture.state(root)
        self.assertIsNotNone(before)
        for table in (
            "conversations",
            "memory_ledger_entries",
            "memory_ledger_participants",
            "memory_ledger_lineage",
        ):
            fixture.conn.execute(
                f"CREATE TEMP TABLE {table} AS SELECT * FROM main.{table}"
            )
        fixture.conn.execute(
            "UPDATE temp.conversations "
            "SET user_id=8,role='model',visibility='private'"
        )
        fixture.conn.execute(
            "UPDATE temp.memory_ledger_entries "
            "SET subject_key='discord_user:8',public_usable=0"
        )
        fixture.conn.execute(
            "DELETE FROM temp.memory_ledger_participants"
        )
        fixture.conn.execute(
            """
            INSERT INTO temp.memory_ledger_lineage(
              entry_id,guild_id,lineage_type,target_entry_id,created_at
            ) VALUES(?,?,?,?,?)
            """,
            (
                root.entry_id,
                2,
                "duplicate_of",
                "temp-hostile-root",
                "2026-07-02T00:00:00+00:00",
            ),
        )

        after = fixture.state(root)

        self.assertIsNotNone(after)
        self.assertEqual(after.source_digest, before.source_digest)


if __name__ == "__main__":
    unittest.main()
