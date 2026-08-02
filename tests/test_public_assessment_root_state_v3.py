import hashlib
import json
import sqlite3
import unittest
from dataclasses import dataclass
from datetime import datetime

import bnl_memory_ledger as ledger


@dataclass(frozen=True)
class _Source:
    entry_id: str
    row_id: int
    guild_id: int
    user_id: int
    user_name: str
    content: str
    observed_at: str
    message_id: int
    channel_id: int
    channel_name: str
    channel_policy: str
    ledger_route: str
    raw_route: str

    @property
    def subject_key(self):
        return ledger.subject_key_for_user(self.user_id)


class _RootStateV3Fixture:
    def __init__(self, *, route_column=True, raw_controls=True):
        self.conn = sqlite3.connect(":memory:")
        ledger.ensure_memory_ledger_schema(self.conn)
        optional_columns = []
        if route_column:
            optional_columns.append("route_mode TEXT NOT NULL")
        if raw_controls:
            optional_columns.extend(
                (
                    "public_usable INTEGER NOT NULL",
                    "visibility TEXT NOT NULL",
                )
            )
        optional_sql = (
            ",\n              " + ",\n              ".join(optional_columns)
            if optional_columns
            else ""
        )
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
              timestamp TEXT NOT NULL%s
            )
            """ % optional_sql
        )
        self.route_column = bool(route_column)
        self.raw_controls = bool(raw_controls)

    def close(self):
        self.conn.close()

    def add_source(
        self,
        row_id,
        content,
        observed_at,
        *,
        guild_id=1,
        user_id=7,
        user_name="Crow",
        channel_id=10,
        channel_name="barcode-bot",
        channel_policy="public_home",
        raw_route="normal_chat",
        ledger_route=None,
        shadow=True,
    ):
        row_id = int(row_id)
        guild_id = int(guild_id)
        user_id = int(user_id)
        channel_id = int(channel_id)
        message_id = (guild_id * 100_000) + 10_000 + row_id
        columns = [
            "id",
            "guild_id",
            "user_id",
            "user_name",
            "role",
            "content",
            "channel_id",
            "channel_name",
            "channel_policy",
            "message_id",
            "timestamp",
        ]
        values = [
            row_id,
            guild_id,
            user_id,
            str(user_name),
            "user",
            str(content),
            channel_id,
            str(channel_name),
            str(channel_policy),
            message_id,
            str(observed_at),
        ]
        if self.route_column:
            columns.append("route_mode")
            values.append(str(raw_route))
        if self.raw_controls:
            columns.extend(("public_usable", "visibility"))
            values.extend((1, "public"))
        self.conn.execute(
            "INSERT INTO conversations(%s) VALUES(%s)"
            % (",".join(columns), ",".join("?" for _ in columns)),
            tuple(values),
        )
        resolved_ledger_route = str(ledger_route or raw_route)
        entry_id = ""
        if shadow:
            result = ledger.shadow_conversation_row(
                self.conn,
                row_id=row_id,
                user_id=user_id,
                user_name=str(user_name),
                guild_id=guild_id,
                role="user",
                content=str(content),
                channel_name=str(channel_name),
                channel_policy=str(channel_policy),
                channel_id=channel_id,
                message_id=message_id,
                route_mode=resolved_ledger_route,
                observed_at=str(observed_at),
                source_sequence=row_id,
            )
            if result.outcome != "inserted":
                raise AssertionError(result)
            entry_id = result.entry_id
        self.conn.commit()
        return _Source(
            entry_id=entry_id,
            row_id=row_id,
            guild_id=guild_id,
            user_id=user_id,
            user_name=str(user_name),
            content=str(content),
            observed_at=str(observed_at),
            message_id=message_id,
            channel_id=channel_id,
            channel_name=str(channel_name),
            channel_policy=str(channel_policy),
            ledger_route=resolved_ledger_route,
            raw_route=str(raw_route),
        )

    def add_ledger_only(
        self,
        row_id,
        content,
        observed_at,
        *,
        route_mode="normal_chat",
    ):
        row_id = int(row_id)
        result = ledger.shadow_conversation_row(
            self.conn,
            row_id=row_id,
            user_id=7,
            user_name="Crow",
            guild_id=1,
            role="user",
            content=str(content),
            channel_name="barcode-bot",
            channel_policy="public_home",
            channel_id=10,
            message_id=110_000 + row_id,
            route_mode=str(route_mode),
            observed_at=str(observed_at),
            source_sequence=row_id,
        )
        if result.outcome != "inserted":
            raise AssertionError(result)
        self.conn.commit()
        return result.entry_id

    def add_lineage(self, source_entry_id, target_entry_id, *, guild_id=1):
        self.conn.execute(
            """
            INSERT INTO memory_ledger_lineage(
              entry_id,guild_id,lineage_type,target_entry_id,created_at
            ) VALUES(?,?,?,?,?)
            """,
            (
                str(source_entry_id),
                int(guild_id),
                "correction_of",
                str(target_entry_id),
                "2026-08-01T13:00:01+00:00",
            ),
        )
        self.conn.commit()

    def state(self, source):
        return ledger.read_public_assessment_root_state(
            self.conn,
            entry_id=source.entry_id,
            guild_id=source.guild_id,
            subject_key=source.subject_key,
        )

    def selected_ids(self, source):
        selection = ledger.select_public_conversation_assessment_evidence(
            self.conn,
            guild_id=source.guild_id,
            subject_key=source.subject_key,
            request_text="What am I all about?",
        )
        return {item.entry_id for item in selection.items}

    def ensure_journal_receipt_schema(self):
        self.conn.execute(
            """
            CREATE TABLE bnl_journal_source_events (
              event_seq INTEGER PRIMARY KEY AUTOINCREMENT,
              guild_id INTEGER NOT NULL,
              source_kind TEXT NOT NULL,
              source_key TEXT NOT NULL,
              occurred_at_ms INTEGER NOT NULL,
              ingested_at_ms INTEGER NOT NULL,
              channel_id INTEGER,
              channel_policy TEXT NOT NULL,
              subject_ref TEXT NOT NULL,
              private_display_name TEXT NOT NULL,
              raw_text TEXT NOT NULL,
              sanitized_summary TEXT NOT NULL,
              content_hash TEXT NOT NULL,
              public_usable INTEGER NOT NULL,
              metadata_json TEXT NOT NULL,
              UNIQUE(guild_id,source_kind,source_key)
            )
            """
        )
        self.conn.execute(
            """
            CREATE TRIGGER trg_bnl_journal_sources_no_duplicate_insert
            BEFORE INSERT ON bnl_journal_source_events
            WHEN EXISTS (
              SELECT 1 FROM bnl_journal_source_events
              WHERE event_seq=NEW.event_seq OR (
                guild_id=NEW.guild_id
                AND source_kind=NEW.source_kind
                AND source_key=NEW.source_key
              )
            )
            BEGIN
              SELECT RAISE(ABORT,'bnl_journal_source_events_duplicate_identity');
            END
            """
        )
        self.conn.execute(
            """
            CREATE TRIGGER trg_bnl_journal_sources_no_update
            BEFORE UPDATE ON bnl_journal_source_events
            BEGIN
              SELECT RAISE(ABORT,'bnl_journal_source_events_immutable');
            END
            """
        )
        self.conn.execute(
            """
            CREATE TRIGGER trg_bnl_journal_sources_no_delete
            BEFORE DELETE ON bnl_journal_source_events
            BEGIN
              SELECT RAISE(ABORT,'bnl_journal_source_events_immutable');
            END
            """
        )
        self.conn.commit()

    def add_journal_receipt(
        self,
        source,
        *,
        route_mode="normal_chat",
        metadata_overrides=None,
        raw_text=None,
        content_hash=None,
        channel_policy=None,
        subject_ref=None,
        private_display_name=None,
    ):
        receipt_text = source.content if raw_text is None else str(raw_text)
        metadata = {
            "conversationRowId": source.row_id,
            "messageId": source.message_id,
        }
        if route_mode is not None:
            metadata["routeMode"] = str(route_mode)
        metadata.update(dict(metadata_overrides or {}))
        observed = datetime.fromisoformat(
            source.observed_at[:-1] + "+00:00"
            if source.observed_at.endswith("Z")
            else source.observed_at
        )
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
                source.guild_id,
                "discord_message",
                str(source.message_id),
                int(observed.timestamp() * 1000),
                int(observed.timestamp() * 1000) + 1,
                source.channel_id,
                source.channel_policy
                if channel_policy is None
                else str(channel_policy),
                source.subject_key if subject_ref is None else str(subject_ref),
                source.user_name
                if private_display_name is None
                else str(private_display_name),
                receipt_text,
                "inert summary",
                (
                    hashlib.sha256(receipt_text.encode("utf-8")).hexdigest()
                    if content_hash is None
                    else str(content_hash)
                ),
                1,
                json.dumps(metadata, sort_keys=True, separators=(",", ":")),
            ),
        )
        self.conn.commit()


class PublicAssessmentRootStateV3Tests(unittest.TestCase):
    ROOT_TEXT = "I test audio every morning before release."
    ROOT_TIME = "2026-08-01T12:00:00+00:00"
    LATER_TIME = "2026-08-01T13:00:00+00:00"

    def fixture(self, **kwargs):
        fixture = _RootStateV3Fixture(**kwargs)
        self.addCleanup(fixture.close)
        return fixture

    def root(self, fixture):
        source = fixture.add_source(
            1,
            self.ROOT_TEXT,
            self.ROOT_TIME,
        )
        self.assertIsNotNone(fixture.state(source))
        return source

    def assert_withheld(self, fixture, source):
        self.assertIsNone(fixture.state(source))
        self.assertNotIn(source.entry_id, fixture.selected_ids(source))

    def test_raw_correction_wins_after_later_ledger_text_tamper(self):
        fixture = self.fixture()
        root = self.root(fixture)
        correction = fixture.add_source(
            2,
            "Actually, I review visual drafts instead.",
            self.LATER_TIME,
        )
        fixture.conn.execute(
            "UPDATE memory_ledger_entries SET normalized_value=? "
            "WHERE entry_id=?",
            (
                "I discuss harmless visual releases every afternoon.",
                correction.entry_id,
            ),
        )
        fixture.conn.commit()

        self.assert_withheld(fixture, root)

    def test_raw_polarity_wins_after_later_ledger_text_tamper(self):
        fixture = self.fixture()
        root = self.root(fixture)
        contradiction = fixture.add_source(
            2,
            "I do not test audio every morning before release.",
            self.LATER_TIME,
        )
        fixture.conn.execute(
            "UPDATE memory_ledger_entries SET normalized_value=? "
            "WHERE entry_id=?",
            (
                "I review visual posters every afternoon.",
                contradiction.entry_id,
            ),
        )
        fixture.conn.commit()

        self.assert_withheld(fixture, root)

    def test_raw_only_relevant_later_row_fails_closed(self):
        fixture = self.fixture()
        root = self.root(fixture)
        fixture.add_source(
            2,
            "I do not test audio every morning before release.",
            self.LATER_TIME,
            shadow=False,
        )

        self.assert_withheld(fixture, root)

    def test_ledger_only_relevant_later_row_fails_closed(self):
        fixture = self.fixture()
        root = self.root(fixture)
        fixture.add_ledger_only(
            2,
            "I do not test audio every morning before release.",
            self.LATER_TIME,
        )

        self.assert_withheld(fixture, root)

    def test_malformed_timestamp_opposite_claims_fail_closed(self):
        cases = ("raw_and_ledger", "raw_only", "ledger_only")
        contradiction = (
            "I do not test audio every morning before release."
        )
        for case in cases:
            with self.subTest(case=case):
                fixture = self.fixture()
                root = self.root(fixture)
                if case == "raw_and_ledger":
                    fixture.add_source(
                        2,
                        contradiction,
                        "not-a-time",
                    )
                elif case == "raw_only":
                    fixture.add_source(
                        2,
                        contradiction,
                        "not-a-time",
                        shadow=False,
                    )
                else:
                    fixture.add_ledger_only(
                        2,
                        contradiction,
                        "not-a-time",
                    )

                self.assert_withheld(fixture, root)

    def test_invalid_correction_targets_fail_closed(self):
        cases = ("dangling", "cross_subject", "cross_guild", "later")
        for case_number, case in enumerate(cases, start=1):
            with self.subTest(case=case):
                fixture = self.fixture()
                root = self.root(fixture)
                correction = fixture.add_source(
                    2,
                    "Actually, I review visual drafts instead.",
                    self.LATER_TIME,
                )
                if case == "dangling":
                    target_id = "mle_missing_correction_target"
                elif case == "cross_subject":
                    target_id = fixture.add_source(
                        3,
                        "I review visual posters every week.",
                        "2026-08-01T12:30:00+00:00",
                        user_id=8,
                        user_name="Other Member",
                    ).entry_id
                elif case == "cross_guild":
                    target_id = fixture.add_source(
                        3,
                        "I review visual posters every week.",
                        "2026-08-01T12:30:00+00:00",
                        guild_id=2,
                    ).entry_id
                else:
                    target_id = fixture.add_source(
                        3,
                        "I review visual posters every week.",
                        "2026-08-01T14:00:00+00:00",
                    ).entry_id
                fixture.add_lineage(correction.entry_id, target_id)

                self.assert_withheld(fixture, root)

    def _resolved_unrelated_correction(self, fixture):
        target = fixture.add_source(
            1,
            "I review visual drafts every morning.",
            "2026-08-01T11:00:00+00:00",
        )
        root = fixture.add_source(
            2,
            self.ROOT_TEXT,
            self.ROOT_TIME,
        )
        correction = fixture.add_source(
            3,
            "Actually, I build radio posters instead.",
            self.LATER_TIME,
        )
        fixture.add_lineage(correction.entry_id, target.entry_id)
        return target, root, correction

    def test_exact_resolved_unrelated_correction_preserves_other_root(self):
        fixture = self.fixture()
        target, root, _correction = self._resolved_unrelated_correction(
            fixture
        )

        self.assertIsNone(fixture.state(target))
        self.assertIsNotNone(fixture.state(root))
        self.assertIn(root.entry_id, fixture.selected_ids(root))

    def test_correction_twin_authority_drift_fails_closed(self):
        mutations = (
            ("schema_version", "forged"),
            ("source_class", "private_assertion"),
            ("route_mode", "operator_command"),
            ("public_usable", 0),
            ("derived", 1),
            ("projection", 1),
            ("lifecycle_status", "retired"),
        )
        for column, value in mutations:
            with self.subTest(column=column):
                fixture = self.fixture()
                _target, root, correction = (
                    self._resolved_unrelated_correction(fixture)
                )
                self.assertIsNotNone(fixture.state(root))
                fixture.conn.execute(
                    "UPDATE memory_ledger_entries SET %s=? WHERE entry_id=?"
                    % column,
                    (value, correction.entry_id),
                )
                fixture.conn.commit()

                self.assert_withheld(fixture, root)

    def test_correction_target_authority_drift_fails_closed(self):
        mutations = (
            ("schema_version", "forged"),
            ("entry_type", "forged"),
            ("predicate_key", "forged"),
            ("source_class", "private_assertion"),
            ("source_revision", "999"),
            ("public_usable", 0),
            ("derived", 1),
            ("projection", 1),
            ("lifecycle_status", "retired"),
        )
        for column, value in mutations:
            with self.subTest(column=column):
                fixture = self.fixture()
                target, root, _correction = (
                    self._resolved_unrelated_correction(fixture)
                )
                self.assertIsNotNone(fixture.state(root))
                fixture.conn.execute(
                    "UPDATE memory_ledger_entries SET %s=? WHERE entry_id=?"
                    % column,
                    (value, target.entry_id),
                )
                fixture.conn.commit()

                self.assert_withheld(fixture, root)

    def test_common_invalidators_withhold_older_root(self):
        invalidators = (
            "I no longer test audio every morning before release.",
            "I stopped testing audio every morning before release.",
            "I quit testing audio every morning before release.",
            "I was wrong about testing audio every morning before release.",
            "I do not test audio anymore.",
            "I used to test audio, but I do not anymore.",
            "I changed my mind; I review visual drafts instead.",
            "I take that back; I review visual drafts instead.",
            "I switched from testing audio to reviewing visual drafts.",
            "I have moved on from testing audio every morning.",
            "No, I review visual drafts instead.",
            "Scratch that.",
        )
        for invalidator in invalidators:
            with self.subTest(invalidator=invalidator):
                fixture = self.fixture()
                root = self.root(fixture)
                later = fixture.add_source(
                    2,
                    invalidator,
                    self.LATER_TIME,
                )

                self.assertRegex(
                    invalidator,
                    ledger._CONVERSATION_CORRECTION_RE,
                )
                self.assert_withheld(fixture, root)
                self.assertIsNone(fixture.state(later))

    def test_correction_false_positive_controls_remain_eligible(self):
        controls = (
            "I stopped by the studio to test audio equipment.",
            "No worries, I review visual drafts instead when asked.",
            "I scratch audio onto that disc for texture.",
        )
        for control in controls:
            with self.subTest(control=control):
                fixture = self.fixture()
                root = self.root(fixture)
                fixture.add_source(2, control, self.LATER_TIME)

                self.assertIsNone(
                    ledger._CONVERSATION_CORRECTION_RE.search(control)
                )
                self.assertIsNotNone(fixture.state(root))
                self.assertIn(root.entry_id, fixture.selected_ids(root))

    def test_production_raw_route_missing_fails_without_receipt(self):
        fixture = self.fixture(route_column=False, raw_controls=False)
        source = fixture.add_source(
            1,
            self.ROOT_TEXT,
            self.ROOT_TIME,
            ledger_route="normal_chat",
        )

        self.assert_withheld(fixture, source)

    def test_exact_immutable_journal_route_receipt_qualifies(self):
        fixture = self.fixture(route_column=False, raw_controls=False)
        source = fixture.add_source(
            1,
            self.ROOT_TEXT,
            self.ROOT_TIME,
            ledger_route="normal_chat",
        )
        fixture.ensure_journal_receipt_schema()
        fixture.add_journal_receipt(source, route_mode="normal_chat")

        state = fixture.state(source)
        self.assertIsNotNone(state)
        self.assertIn(source.entry_id, fixture.selected_ids(source))
        with self.assertRaises(sqlite3.IntegrityError):
            fixture.conn.execute(
                "UPDATE bnl_journal_source_events SET metadata_json='{}'"
            )
        fixture.conn.rollback()
        self.assertIsNotNone(fixture.state(source))

    def test_same_named_noop_journal_triggers_fail_closed(self):
        fixture = self.fixture(route_column=False, raw_controls=False)
        source = fixture.add_source(
            1,
            self.ROOT_TEXT,
            self.ROOT_TIME,
            ledger_route="normal_chat",
        )
        fixture.ensure_journal_receipt_schema()
        for name in (
            "trg_bnl_journal_sources_no_duplicate_insert",
            "trg_bnl_journal_sources_no_update",
            "trg_bnl_journal_sources_no_delete",
        ):
            fixture.conn.execute("DROP TRIGGER %s" % name)
        fixture.conn.executescript(
            """
            CREATE TRIGGER trg_bnl_journal_sources_no_duplicate_insert
            BEFORE INSERT ON bnl_journal_source_events
            BEGIN SELECT 1; END;
            CREATE TRIGGER trg_bnl_journal_sources_no_update
            BEFORE UPDATE ON bnl_journal_source_events
            BEGIN SELECT 1; END;
            CREATE TRIGGER trg_bnl_journal_sources_no_delete
            BEFORE DELETE ON bnl_journal_source_events
            BEGIN SELECT 1; END;
            """
        )
        fixture.add_journal_receipt(source, route_mode="normal_chat")

        self.assert_withheld(fixture, source)

    def test_journal_receipt_mismatches_fail_closed(self):
        cases = (
            ("row", {"conversationRowId": 999}, None),
            ("message", {"messageId": 999}, None),
            ("content", {}, "Different retained content."),
        )
        for label, metadata, raw_text in cases:
            with self.subTest(mismatch=label):
                fixture = self.fixture(
                    route_column=False,
                    raw_controls=False,
                )
                source = fixture.add_source(
                    1,
                    self.ROOT_TEXT,
                    self.ROOT_TIME,
                    ledger_route="normal_chat",
                )
                fixture.ensure_journal_receipt_schema()
                fixture.add_journal_receipt(
                    source,
                    route_mode="normal_chat",
                    metadata_overrides=metadata,
                    raw_text=raw_text,
                )

                self.assert_withheld(fixture, source)

    def test_backfill_receipt_cannot_promote_normal_chat(self):
        fixture = self.fixture(route_column=False, raw_controls=False)
        source = fixture.add_source(
            1,
            self.ROOT_TEXT,
            self.ROOT_TIME,
            ledger_route="normal_chat",
        )
        fixture.ensure_journal_receipt_schema()
        fixture.add_journal_receipt(
            source,
            route_mode=None,
            metadata_overrides={"source": "discord_backfill"},
        )

        self.assert_withheld(fixture, source)

    def test_exact_backfill_receipt_qualifies_as_continuity(self):
        fixture = self.fixture(route_column=False, raw_controls=False)
        source = fixture.add_source(
            1,
            self.ROOT_TEXT,
            self.ROOT_TIME,
            ledger_route="conversation_continuity",
        )
        fixture.ensure_journal_receipt_schema()
        fixture.add_journal_receipt(
            source,
            route_mode=None,
            metadata_overrides={"source": "discord_backfill"},
        )

        state = fixture.state(source)
        self.assertIsNotNone(state)
        self.assertEqual(state.route_mode, "conversation_continuity")
        selected = ledger.select_public_conversation_assessment_evidence(
            fixture.conn,
            guild_id=1,
            subject_key="discord_user:7",
            request_text="What am I all about?",
        )
        self.assertEqual(
            [item.entry_id for item in selected.items],
            [state.entry_id],
        )

    def test_backfill_source_and_normal_route_conflict_fails_closed(self):
        fixture = self.fixture(route_column=False, raw_controls=False)
        source = fixture.add_source(
            1,
            self.ROOT_TEXT,
            self.ROOT_TIME,
            ledger_route="normal_chat",
        )
        fixture.ensure_journal_receipt_schema()
        fixture.add_journal_receipt(
            source,
            route_mode="normal_chat",
            metadata_overrides={"source": "discord_backfill"},
        )

        self.assert_withheld(fixture, source)

    def test_fractional_raw_identifiers_fail_closed(self):
        mutations = (
            ("guild_id", 1.9),
            ("user_id", 7.9),
            ("channel_id", 10.9),
            ("message_id", 110001.9),
        )
        for column, value in mutations:
            with self.subTest(column=column):
                fixture = self.fixture()
                source = self.root(fixture)
                fixture.conn.execute(
                    "UPDATE conversations SET %s=? WHERE id=?" % column,
                    (value, source.row_id),
                )
                fixture.conn.commit()

                self.assert_withheld(fixture, source)

    def test_fractional_journal_metadata_identifiers_fail_closed(self):
        cases = (
            {"conversationRowId": 1.9},
            {"messageId": 110001.9},
        )
        for metadata in cases:
            with self.subTest(metadata=metadata):
                fixture = self.fixture(
                    route_column=False,
                    raw_controls=False,
                )
                source = fixture.add_source(
                    1,
                    self.ROOT_TEXT,
                    self.ROOT_TIME,
                    ledger_route="normal_chat",
                )
                fixture.ensure_journal_receipt_schema()
                fixture.add_journal_receipt(
                    source,
                    route_mode="normal_chat",
                    metadata_overrides=metadata,
                )

                self.assert_withheld(fixture, source)

    def test_raw_route_conflict_cannot_be_overridden_by_receipt_or_ledger(self):
        fixture = self.fixture()
        source = fixture.add_source(
            1,
            self.ROOT_TEXT,
            self.ROOT_TIME,
            raw_route="operator_command",
            ledger_route="normal_chat",
        )
        fixture.ensure_journal_receipt_schema()
        fixture.add_journal_receipt(source, route_mode="normal_chat")

        self.assert_withheld(fixture, source)


if __name__ == "__main__":
    unittest.main()
