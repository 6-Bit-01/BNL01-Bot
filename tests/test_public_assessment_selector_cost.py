import sqlite3
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import bnl_memory_ledger as ledger


class PublicAssessmentSelectorCostTests(unittest.TestCase):
    GUILD_ID = 1
    USER_ID = 7
    SUBJECT_KEY = "discord_user:7"
    USER_NAME = "Crow"

    @staticmethod
    def _alpha_label(value):
        label = []
        remaining = int(value)
        while remaining:
            remaining, digit = divmod(remaining - 1, 26)
            label.append(chr(ord("a") + digit))
        return "".join(reversed(label))

    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        ledger.ensure_memory_ledger_schema(self.conn)
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
                message_id INTEGER NOT NULL,
                route_mode TEXT NOT NULL,
                public_usable INTEGER NOT NULL,
                visibility TEXT NOT NULL,
                timestamp TEXT NOT NULL
            )
            """
        )
        self.conn.commit()

    def tearDown(self):
        self.conn.close()

    def _add_source(
        self,
        row_id,
        *,
        observed_at,
        source_sequence_domain="row",
        text=None,
        route_mode="normal_chat",
    ):
        message_id = 1_000_000 + int(row_id)
        source_sequence = (
            message_id if source_sequence_domain == "message" else int(row_id)
        )
        source_text = text or (
            "I compare audio mixes before the final release for archive %s."
            % self._alpha_label(row_id)
        )
        self.conn.execute(
            """
            INSERT INTO conversations(
                id,guild_id,user_id,user_name,role,content,channel_id,
                channel_name,channel_policy,message_id,route_mode,
                public_usable,visibility,timestamp
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                int(row_id),
                self.GUILD_ID,
                self.USER_ID,
                self.USER_NAME,
                "user",
                source_text,
                10,
                "barcode-bot",
                "public_home",
                message_id,
                str(route_mode),
                1,
                "public",
                observed_at,
            ),
        )
        result = ledger.shadow_conversation_row(
            self.conn,
            row_id=int(row_id),
            user_id=self.USER_ID,
            user_name=self.USER_NAME,
            guild_id=self.GUILD_ID,
            role="user",
            content=source_text,
            channel_id=10,
            channel_name="barcode-bot",
            channel_policy="public_home",
            message_id=message_id,
            route_mode=str(route_mode),
            observed_at=observed_at,
            source_sequence=source_sequence,
        )
        self.assertEqual(result.outcome, "inserted")
        return result.entry_id

    def _select(self, *, max_results=4):
        return ledger.select_public_conversation_assessment_evidence(
            self.conn,
            guild_id=self.GUILD_ID,
            subject_key=self.SUBJECT_KEY,
            request_text="What am I all about in the BARCODE project?",
            max_results=max_results,
        )

    def test_selector_cost_is_bounded_for_221_retained_rows(self):
        base = datetime(2026, 7, 1, tzinfo=timezone.utc)
        for row_id in range(1, 222):
            self._add_source(
                row_id,
                observed_at=(base + timedelta(hours=2 * row_id)).isoformat(),
                source_sequence_domain=(
                    "message" if row_id % 2 else "row"
                ),
            )
        self.conn.commit()

        sql_statements = []
        validated_states = {}
        real_validator = ledger.read_public_assessment_root_state

        def validating_wrapper(conn, *, entry_id, guild_id, subject_key):
            state = real_validator(
                conn,
                entry_id=entry_id,
                guild_id=guild_id,
                subject_key=subject_key,
            )
            validated_states[entry_id] = state
            return state

        self.conn.set_trace_callback(sql_statements.append)
        try:
            with patch.object(
                ledger,
                "read_public_assessment_root_state",
                side_effect=validating_wrapper,
            ) as validator:
                selection = self._select()
        finally:
            self.conn.set_trace_callback(None)

        self.assertEqual(selection.scanned_count, 221)
        self.assertEqual(validator.call_count, 8)
        self.assertLessEqual(validator.call_count, 12)
        self.assertEqual(selection.eligible_count, 8)
        self.assertEqual(len(selection.items), 4)
        # The bound includes schema probes and occurrence/correction checks in
        # every central validation, without depending on wall-clock timing.
        self.assertLessEqual(len(sql_statements), 900)

        for item in selection.items:
            state = validated_states.get(item.entry_id)
            self.assertIsNotNone(state)
            self.assertEqual(item.source_digest, state.source_digest)
            self.assertEqual(item.root_identity, state.root_identity)
            self.assertEqual(
                item.occurrence_identity,
                state.occurrence_identity,
            )
            self.assertEqual(
                item.point_identity,
                state.semantics.point_identity,
            )

    def test_selector_cost_is_bounded_with_many_later_corrections(self):
        base = datetime(2026, 1, 1, tzinfo=timezone.utc)
        actions = (
            "test",
            "review",
            "compare",
            "build",
            "adjust",
            "choose",
            "fix",
            "discuss",
        )
        for row_id, action in enumerate(actions, start=1):
            self._add_source(
                row_id,
                observed_at=(base + timedelta(hours=row_id)).isoformat(),
                text=(
                    "I %s audio release drafts before archive alpha%s."
                    % (action, row_id)
                ),
            )
        for row_id in range(9, 209):
            self._add_source(
                row_id,
                observed_at=(base + timedelta(hours=row_id)).isoformat(),
                text=(
                    "Actually, I meant visual draft archive correction %s."
                    % row_id
                ),
            )
        self.conn.commit()

        sql_statements = []
        self.conn.set_trace_callback(sql_statements.append)
        try:
            with patch.object(
                ledger,
                "read_public_assessment_root_state",
                wraps=ledger.read_public_assessment_root_state,
            ) as validator:
                selection = self._select()
        finally:
            self.conn.set_trace_callback(None)

        self.assertEqual(selection.scanned_count, 208)
        self.assertEqual(validator.call_count, 8)
        self.assertEqual(selection.eligible_count, 0)
        self.assertEqual(selection.items, ())
        self.assertLessEqual(len(sql_statements), 1200)

    def test_newer_continuity_rows_share_the_bounded_validation_budget(self):
        base = datetime(2026, 1, 1, tzinfo=timezone.utc)
        normal_entry_id = self._add_source(
            1,
            observed_at=base.isoformat(),
            text="I compare audio mixes before the final release.",
        )
        for row_id in range(2, 11):
            self._add_source(
                row_id,
                observed_at=(base + timedelta(hours=row_id)).isoformat(),
                route_mode="conversation_continuity",
            )
        self.conn.commit()

        with patch.object(
            ledger,
            "read_public_assessment_root_state",
            wraps=ledger.read_public_assessment_root_state,
        ) as validator:
            selection = self._select(max_results=1)

        self.assertEqual(selection.scanned_count, 10)
        self.assertEqual(validator.call_count, 8)
        self.assertEqual(selection.eligible_count, 8)
        self.assertEqual(len(selection.items), 1)
        self.assertNotEqual(selection.items[0].entry_id, normal_entry_id)
        self.assertEqual(
            selection.items[0].route_mode,
            "conversation_continuity",
        )

    def test_large_continuity_history_stays_bounded(self):
        base = datetime(2026, 1, 1, tzinfo=timezone.utc)
        normal_entry_id = self._add_source(
            1,
            observed_at=base.isoformat(),
            text="I compare audio mixes before the final release.",
        )
        for row_id in range(2, 1203):
            self._add_source(
                row_id,
                observed_at=(base + timedelta(hours=row_id)).isoformat(),
                route_mode="conversation_continuity",
            )
        self.conn.commit()

        with patch.object(
            ledger,
            "read_public_assessment_root_state",
            wraps=ledger.read_public_assessment_root_state,
        ) as validator:
            selection = self._select(max_results=1)

        self.assertEqual(
            selection.scanned_count,
            ledger._CONVERSATION_MOTIF_MAX_SCAN,
        )
        self.assertEqual(validator.call_count, 8)
        self.assertEqual(selection.eligible_count, 8)
        self.assertEqual(len(selection.items), 1)
        self.assertNotEqual(selection.items[0].entry_id, normal_entry_id)
        self.assertEqual(
            selection.items[0].route_mode,
            "conversation_continuity",
        )

    def test_mixed_source_sequence_domains_order_by_observed_at(self):
        rows = (
            (
                1,
                "2026-08-01T08:00:00+00:00",
                "message",
                "I compare audio mixes before release for archive alpha.",
            ),
            (
                2,
                "2026-08-01T10:00:00+00:00",
                "message",
                "I compare audio mixes before release for archive beta.",
            ),
            (
                3,
                "2026-08-01T12:00:00+00:00",
                "row",
                "I compare audio mixes before release for archive gamma.",
            ),
        )
        entry_ids = {}
        for row_id, observed_at, domain, text in rows:
            entry_ids[row_id] = self._add_source(
                row_id,
                observed_at=observed_at,
                source_sequence_domain=domain,
                text=text,
            )
        self.conn.commit()

        selection = self._select(max_results=3)

        self.assertEqual(len(selection.items), 3)
        self.assertEqual(
            [item.entry_id for item in selection.items],
            [entry_ids[3], entry_ids[2], entry_ids[1]],
        )
        self.assertEqual(
            [item.observed_at for item in selection.items],
            sorted(
                (item.observed_at for item in selection.items),
                reverse=True,
            ),
        )
        newest_sequence = self.conn.execute(
            "SELECT source_sequence FROM memory_ledger_entries WHERE entry_id=?",
            (entry_ids[3],),
        ).fetchone()[0]
        older_sequence = self.conn.execute(
            "SELECT source_sequence FROM memory_ledger_entries WHERE entry_id=?",
            (entry_ids[2],),
        ).fetchone()[0]
        self.assertLess(newest_sequence, older_sequence)


if __name__ == "__main__":
    unittest.main()
