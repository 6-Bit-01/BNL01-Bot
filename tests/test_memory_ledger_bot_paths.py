import asyncio
import os
import sqlite3
import tempfile
import unittest
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl01_bot
import bnl_memory_ledger as ledger
import bnl_moment_engine as moments
import bnl_unified_intelligence_packet as packet


class _Author:
    id = 42
    display_name = "Crow"

class _Message:
    def __init__(self, content="note"):
        self.content = content
        self.author = _Author()
        self.replies = []
    async def reply(self, text, **kwargs):
        self.replies.append(text)

class _FailMessage(_Message):
    async def reply(self, text, **kwargs):
        raise RuntimeError("send failed")

class MemoryLedgerBotPathTests(unittest.TestCase):
    def setUp(self):
        self.old_db = bnl01_bot.DB_FILE
        self.old_owner_user_id = bnl01_bot.BNL_OWNER_USER_ID
        self.old_primary_guild_id = bnl01_bot.BNL_PRIMARY_GUILD_ID
        self.tmp = tempfile.NamedTemporaryFile(delete=False)
        self.tmp.close()
        bnl01_bot.DB_FILE = self.tmp.name
        bnl01_bot.BNL_OWNER_USER_ID = 42
        bnl01_bot.BNL_PRIMARY_GUILD_ID = 1
        self.env = mock.patch.dict(
            os.environ,
            {
                "BNL_OWNER_USER_ID": "42",
                "BNL_PRIMARY_GUILD_ID": "1",
            },
            clear=False,
        )
        self.env.start()
        os.environ.pop("BNL_MEMORY_LEDGER_SHADOW_ENABLED", None)
        bnl01_bot.init_db()

    def tearDown(self):
        self.env.stop()
        bnl01_bot.DB_FILE = self.old_db
        bnl01_bot.BNL_OWNER_USER_ID = self.old_owner_user_id
        bnl01_bot.BNL_PRIMARY_GUILD_ID = self.old_primary_guild_id
        try:
            os.unlink(self.tmp.name)
        except OSError:
            pass

    def rows(self, sql, params=()):
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            return conn.execute(sql, params).fetchall()

    def enable(self):
        os.environ["BNL_MEMORY_LEDGER_SHADOW_ENABLED"] = "1"

    def replace_conversations_with_legacy_schema(self):
        """Install the last production shape that had no raw route column."""

        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            conn.execute("DROP TABLE conversations")
            conn.execute(
                """
                CREATE TABLE conversations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    user_name TEXT NOT NULL,
                    guild_id INTEGER NOT NULL,
                    channel_name TEXT,
                    channel_policy TEXT,
                    channel_id INTEGER,
                    message_id INTEGER,
                    role TEXT NOT NULL,
                    content TEXT NOT NULL,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            conn.commit()

    def open_assessment_for_source_row(self, *, user_id, source_row_id):
        subject_key = ledger.subject_key_for_user(user_id)
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            row = conn.execute(
                """
                SELECT entry_id
                FROM memory_ledger_entries
                WHERE guild_id=1 AND subject_key=?
                  AND source_table='conversations' AND source_row_id=?
                  AND source_role='user'
                ORDER BY entry_id
                """,
                (subject_key, str(source_row_id)),
            ).fetchone()
            state = (
                ledger.read_public_assessment_root_state(
                    conn,
                    entry_id=str(row[0]),
                    guild_id=1,
                    subject_key=subject_key,
                )
                if row
                else None
            )
            selection = ledger.select_public_conversation_assessment_evidence(
                conn,
                guild_id=1,
                subject_key=subject_key,
                request_text="What am I all about?",
            )
        return state, selection

    def add_declared_broadcast_projection(
        self,
        *,
        memory_id,
        root_entry_id,
        suffix,
    ):
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            result = ledger.insert_ledger_entry(
                conn,
                ledger.LedgerEntry(
                    guild_id=1,
                    source_table="declared_canon_projection",
                    source_row_id="bot-declared-%s" % suffix,
                    source_revision="bot-declared-revision-%s" % suffix,
                    source_event_key="revision:bot-%s" % suffix,
                    source_role="declared_canon_projection",
                    entry_type="canon_reference",
                    subject_key="broadcast:barcode_radio",
                    predicate_key="broadcast_memory",
                    value="Bot-path Declared Broadcast projection.",
                    source_class=ledger.SourceClass.EVIDENCE_PROJECTION,
                    visibility=ledger.Visibility.INTERNAL,
                    confidence=ledger.Confidence.LOW,
                    public_usable=False,
                    derived=True,
                    projection=True,
                    observed_at="2026-08-01T00:00:00+00:00",
                    lifecycle_status=ledger.REVIEW_ONLY_LIFECYCLE,
                    lineage=(("derived_from", root_entry_id),),
                ),
            )
            self.assertEqual(result.outcome, "inserted")
            return result

    def test_group_model_reply_is_one_room_source_with_all_participants(self):
        self.enable()

        decision = bnl01_bot.save_model_message(
            42,
            1,
            "One answer to the room.",
            channel_name="barcode-bot",
            channel_policy="public_home",
            channel_id=10,
            conversation_target_user_ids=(42, 43),
        )

        self.assertTrue(decision.save_conversation)
        self.assertEqual(
            self.rows(
                """
                SELECT user_id, role, content
                FROM conversations
                WHERE guild_id=1 AND role='model'
                """
            ),
            [(0, "model", "One answer to the room.")],
        )
        self.assertEqual(
            self.rows(
                """
                SELECT user_id
                FROM conversation_response_participants
                WHERE guild_id=1
                ORDER BY user_id
                """
            ),
            [(42,), (43,)],
        )
        ledger_rows = self.rows(
            """
            SELECT entry_id
            FROM memory_ledger_entries
            WHERE guild_id=1 AND source_table='conversations'
              AND source_role='model'
            """
        )
        self.assertEqual(len(ledger_rows), 1)
        self.assertEqual(
            self.rows(
                """
                SELECT participant_key, participant_role
                FROM memory_ledger_participants
                WHERE entry_id=?
                ORDER BY order_index
                """,
                (ledger_rows[0][0],),
            ),
            [
                ("bnl_01", "author"),
                ("discord_user:42", "conversation_target"),
                ("discord_user:43", "conversation_target"),
            ],
        )
        self.assertEqual(
            self.rows(
                "SELECT user_id FROM relationship_state WHERE guild_id=1"
            ),
            [],
        )
        self.assertEqual(
            self.rows(
                "SELECT user_id FROM memory_tiers WHERE guild_id=1"
            ),
            [],
        )
        self.assertEqual(bnl01_bot.get_conversation_history(42, 1), [])
        self.assertEqual(bnl01_bot.get_conversation_history(43, 1), [])

    def test_complete_delete_removes_group_response_and_all_associations_without_ledger(self):
        bnl01_bot.save_model_message(
            42,
            1,
            "Shared answer involving Crow.",
            channel_name="barcode-bot",
            channel_policy="public_home",
            channel_id=10,
            conversation_target_user_ids=(42, 43),
        )
        bnl01_bot.save_model_message(
            43,
            1,
            "Separate shared answer.",
            channel_name="barcode-bot",
            channel_policy="public_home",
            channel_id=10,
            conversation_target_user_ids=(43, 44),
        )
        target_row_id = self.rows(
            "SELECT id FROM conversations WHERE content='Shared answer involving Crow.'"
        )[0][0]
        other_row_id = self.rows(
            "SELECT id FROM conversations WHERE content='Separate shared answer.'"
        )[0][0]

        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            result = bnl01_bot.complete_delete_member_data(
                conn,
                guild_id=1,
                user_id=42,
                confirmation="DELETE MY BNL DATA 1",
            )

        self.assertTrue(result["ok"])
        self.assertEqual(
            result["row_counts"]["conversation_group_model_responses"],
            1,
        )
        self.assertEqual(
            result["row_counts"]["conversation_response_participants"],
            2,
        )
        self.assertEqual(
            self.rows(
                "SELECT id FROM conversations WHERE id IN (?,?) ORDER BY id",
                (target_row_id, other_row_id),
            ),
            [(other_row_id,)],
        )
        self.assertEqual(
            self.rows(
                """
                SELECT conversation_row_id, user_id
                FROM conversation_response_participants
                ORDER BY conversation_row_id, user_id
                """
            ),
            [(other_row_id, 43), (other_row_id, 44)],
        )

    def test_complete_delete_group_response_cleanup_is_atomic_with_ledger(self):
        self.enable()
        bnl01_bot.save_model_message(
            42,
            1,
            "Shared answer involving Crow.",
            channel_name="barcode-bot",
            channel_policy="public_home",
            channel_id=10,
            conversation_target_user_ids=(42, 43),
        )
        row_id = self.rows(
            "SELECT id FROM conversations WHERE content='Shared answer involving Crow.'"
        )[0][0]
        ledger_entry_id = self.rows(
            """
            SELECT entry_id
            FROM memory_ledger_entries
            WHERE guild_id=1 AND source_table='conversations'
              AND source_row_id=? AND source_role='model'
            """,
            (str(row_id),),
        )[0][0]

        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            with self.assertRaises(RuntimeError):
                bnl01_bot.complete_delete_member_data(
                    conn,
                    guild_id=1,
                    user_id=42,
                    confirmation="DELETE MY BNL DATA 1",
                    inject_failure=True,
                )

        self.assertEqual(
            self.rows("SELECT COUNT(*) FROM conversations WHERE id=?", (row_id,))[0][0],
            1,
        )
        self.assertEqual(
            self.rows(
                """
                SELECT COUNT(*)
                FROM conversation_response_participants
                WHERE conversation_row_id=?
                """,
                (row_id,),
            )[0][0],
            2,
        )
        self.assertEqual(
            self.rows(
                "SELECT COUNT(*) FROM memory_ledger_entries WHERE entry_id=?",
                (ledger_entry_id,),
            )[0][0],
            1,
        )

        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            result = bnl01_bot.complete_delete_member_data(
                conn,
                guild_id=1,
                user_id=42,
                confirmation="DELETE MY BNL DATA 1",
            )

        self.assertTrue(result["ok"])
        self.assertEqual(
            self.rows("SELECT COUNT(*) FROM conversations WHERE id=?", (row_id,))[0][0],
            0,
        )
        self.assertEqual(
            self.rows(
                """
                SELECT COUNT(*)
                FROM conversation_response_participants
                WHERE conversation_row_id=?
                """,
                (row_id,),
            )[0][0],
            0,
        )
        self.assertEqual(
            self.rows(
                "SELECT COUNT(*) FROM memory_ledger_entries WHERE entry_id=?",
                (ledger_entry_id,),
            )[0][0],
            0,
        )

    def test_clearhistory_and_prune_remove_group_response_associations(self):
        bnl01_bot.save_model_message(
            42,
            1,
            "Shared answer involving Crow.",
            channel_name="barcode-bot",
            channel_policy="public_home",
            channel_id=10,
            conversation_target_user_ids=(42, 43),
        )
        bnl01_bot.save_model_message(
            43,
            1,
            "Older unrelated shared answer.",
            channel_name="barcode-bot",
            channel_policy="public_home",
            channel_id=10,
            conversation_target_user_ids=(43, 44),
        )
        target_row_id = self.rows(
            "SELECT id FROM conversations WHERE content='Shared answer involving Crow.'"
        )[0][0]
        older_other_row_id = self.rows(
            "SELECT id FROM conversations WHERE content='Older unrelated shared answer.'"
        )[0][0]

        self.assertEqual(bnl01_bot.clear_user_history(42, 1), 1)
        self.assertEqual(
            self.rows("SELECT COUNT(*) FROM conversations WHERE id=?", (target_row_id,))[0][0],
            0,
        )
        self.assertEqual(
            self.rows(
                """
                SELECT COUNT(*)
                FROM conversation_response_participants
                WHERE conversation_row_id=?
                """,
                (target_row_id,),
            )[0][0],
            0,
        )
        self.assertEqual(
            self.rows(
                """
                SELECT user_id
                FROM conversation_response_participants
                WHERE conversation_row_id=?
                ORDER BY user_id
                """,
                (older_other_row_id,),
            ),
            [(43,), (44,)],
        )

        bnl01_bot.save_model_message(
            43,
            1,
            "Newer unrelated shared answer.",
            channel_name="barcode-bot",
            channel_policy="public_home",
            channel_id=10,
            conversation_target_user_ids=(43, 44),
        )
        newer_other_row_id = self.rows(
            "SELECT id FROM conversations WHERE content='Newer unrelated shared answer.'"
        )[0][0]
        bnl01_bot.prune_conversation_history(0, 1, max_rows=1)
        self.assertEqual(
            self.rows(
                "SELECT id FROM conversations WHERE user_id=0 ORDER BY id"
            ),
            [(newer_other_row_id,)],
        )
        self.assertEqual(
            self.rows(
                """
                SELECT conversation_row_id, user_id
                FROM conversation_response_participants
                ORDER BY conversation_row_id, user_id
                """
            ),
            [(newer_other_row_id, 43), (newer_other_row_id, 44)],
        )

    def test_single_speaker_model_reply_keeps_personal_storage_contract(self):
        self.enable()

        bnl01_bot.save_model_message(
            42,
            1,
            "One answer to Crow.",
            channel_name="barcode-bot",
            channel_policy="public_home",
            channel_id=10,
            conversation_target_user_ids=(42,),
        )

        self.assertEqual(
            self.rows(
                """
                SELECT user_id, role, content
                FROM conversations
                WHERE guild_id=1 AND role='model'
                """
            ),
            [(42, "model", "One answer to Crow.")],
        )
        self.assertEqual(
            bnl01_bot.get_conversation_history(42, 1),
            [{"role": "model", "parts": ["One answer to Crow."]}],
        )

    def _conversation_id(self, *, guild_id, user_id, content):
        rows = self.rows(
            """
            SELECT id
            FROM conversations
            WHERE guild_id=? AND user_id=? AND content=?
            ORDER BY id DESC
            LIMIT 1
            """,
            (guild_id, user_id, content),
        )
        self.assertEqual(len(rows), 1)
        return int(rows[0][0])

    def _attach_test_moment(self, *, guild_id, source_row_id):
        moment_id = f"mom_lifecycle_{guild_id}_{source_row_id}"
        timestamp = "2026-07-23T12:00:00+00:00"
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            moments.ensure_moment_schema(conn)
            raw_rows = conn.execute(
                """
                SELECT entry_id
                FROM memory_ledger_entries
                WHERE guild_id=? AND source_table='conversations'
                  AND source_row_id=?
                  AND predicate_key='conversation'
                  AND entry_type='observation'
                """,
                (guild_id, str(source_row_id)),
            ).fetchall()
            self.assertEqual(len(raw_rows), 1)
            raw_entry_id = str(raw_rows[0][0])
            canonical = ledger.insert_ledger_entry(
                conn,
                ledger.LedgerEntry(
                    guild_id=guild_id,
                    source_table="memory_moment_windows",
                    source_row_id=moment_id,
                    source_role="derived_assessment",
                    entry_type="shared_moment",
                    subject_key=f"moment:{moment_id}",
                    predicate_key="shared_moment",
                    value='{"summary":"source-bound test moment"}',
                    source_class=ledger.SourceClass.DERIVED_SUMMARY,
                    visibility=ledger.Visibility.PRIVATE,
                    confidence=ledger.Confidence.HIGH,
                    derived=True,
                    projection=True,
                    observed_at=timestamp,
                    lineage=(("derived_from", raw_entry_id),),
                ),
            )
            self.assertTrue(canonical.entry_id)
            conn.execute(
                """
                INSERT INTO memory_moment_windows (
                    moment_id, guild_id, channel_id, channel_name,
                    channel_policy, route_mode, topic_key,
                    window_started_at, last_activity_at,
                    qualification_reason, lifecycle_status, visibility,
                    public_usable, salience, human_entry_count,
                    model_entry_count, participant_count, summary,
                    created_at, updated_at, canonical_ledger_entry_id
                )
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    moment_id,
                    guild_id,
                    10,
                    "barcode-bot",
                    "public_home",
                    bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                    "topic_lifecycle_test",
                    timestamp,
                    timestamp,
                    "qualified_test_fixture",
                    "finalized",
                    "private",
                    1,
                    0.8,
                    1,
                    0,
                    1,
                    "source-bound test moment",
                    timestamp,
                    timestamp,
                    canonical.entry_id,
                ),
            )
            conn.execute(
                """
                INSERT INTO memory_moment_members (
                    moment_id, ledger_entry_id, source_sequence,
                    observed_at, membership_role, created_at
                )
                VALUES (?,?,?,?,?,?)
                """,
                (
                    moment_id,
                    raw_entry_id,
                    source_row_id,
                    timestamp,
                    "human",
                    timestamp,
                ),
            )
            conn.execute(
                """
                INSERT INTO memory_moment_participants (
                    moment_id, participant_key, safe_display_name,
                    participant_role, first_seen_at, last_seen_at,
                    authored_entry_count, participation_order,
                    created_at, updated_at
                )
                VALUES (?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    moment_id,
                    "discord_user:42",
                    "Crow",
                    "author",
                    timestamp,
                    timestamp,
                    1,
                    0,
                    timestamp,
                    timestamp,
                ),
            )
            conn.commit()
        return moment_id, raw_entry_id, canonical.entry_id

    def test_disabled_gate_save_user_message_creates_no_ledger(self):
        bnl01_bot.save_user_message(42, "Crow", 1, "remember this number: 8", channel_policy="sealed_test")
        self.assertEqual(self.rows("SELECT COUNT(*) FROM conversations")[0][0], 1)
        self.assertEqual(self.rows("SELECT COUNT(*) FROM memory_ledger_entries")[0][0], 0)

    def test_enabled_gate_save_user_message_inserts_and_dedup_receipts(self):
        self.enable()
        bnl01_bot.save_user_message(42, "Crow", 1, "remember this number: 8", channel_policy="sealed_test")
        self.assertEqual(
            self.rows(
                "SELECT predicate_key, normalized_value FROM memory_ledger_entries"
            )[0],
            ("conversation", "remember this number: 8"),
        )
        self.assertEqual(
            self.rows(
                "SELECT COUNT(*) FROM memory_ledger_entries WHERE predicate_key='remembered_number'"
            )[0][0],
            0,
        )
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            result = ledger.shadow_conversation_row(conn, row_id=1, user_id=42, user_name="Crow", guild_id=1, role="user", content="remember this number: 8", channel_policy="sealed_test")
            ledger.record_shadow_receipt(conn, guild_id=1, writer="test", source_table=result.source_table, source_row_id=result.source_row_id, source_revision=result.source_revision, outcome=result.outcome, reason_code=result.reason_code, entry_id=result.entry_id)
            conn.commit()
        outcomes = dict(self.rows("SELECT outcome, COUNT(*) FROM memory_ledger_shadow_receipts GROUP BY outcome"))
        self.assertEqual(outcomes.get("inserted"), 1)
        self.assertEqual(outcomes.get("deduplicated"), 1)

    def test_shadow_failure_keeps_legacy_conversation_and_records_error(self):
        self.enable()
        with mock.patch("bnl01_bot.shadow_conversation_row", side_effect=RuntimeError("boom")):
            bnl01_bot.save_user_message(42, "Crow", 1, "hello", channel_policy="public_home")
        self.assertEqual(self.rows("SELECT COUNT(*) FROM conversations")[0][0], 1)
        self.assertEqual(self.rows("SELECT outcome, reason_code FROM memory_ledger_shadow_receipts")[0], ("error", "shadow_exception"))

    def test_legacy_schema_migration_defaults_route_unknown_and_stays_out_of_open(self):
        self.replace_conversations_with_legacy_schema()
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            conn.execute(
                """
                INSERT INTO conversations(
                    id,user_id,user_name,guild_id,channel_name,
                    channel_policy,channel_id,message_id,role,content,timestamp
                ) VALUES(1,42,'Crow',1,'barcode-bot','public_home',10,101,
                         'user',?,?)
                """,
                (
                    "I compare audio mixes before the final release.",
                    "2026-08-01T12:00:00+00:00",
                ),
            )
            conn.commit()

        bnl01_bot.init_db()

        self.assertIn(
            "route_mode",
            {
                str(row[1])
                for row in self.rows("PRAGMA table_info(conversations)")
            },
        )
        self.assertEqual(
            self.rows("SELECT route_mode FROM conversations WHERE id=1"),
            [("unknown",)],
        )
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            result = ledger.shadow_conversation_row(
                conn,
                row_id=1,
                user_id=42,
                user_name="Crow",
                guild_id=1,
                role="user",
                content="I compare audio mixes before the final release.",
                channel_name="barcode-bot",
                channel_policy="public_home",
                channel_id=10,
                message_id=101,
                route_mode="normal_chat",
                observed_at="2026-08-01T12:00:00+00:00",
                source_sequence=101,
            )
            conn.commit()
        self.assertEqual(result.outcome, "inserted")

        state, selection = self.open_assessment_for_source_row(
            user_id=42,
            source_row_id=1,
        )
        self.assertIsNone(state)
        self.assertEqual(selection.items, ())

    def test_real_save_persists_matching_normal_route_and_qualifies_open(self):
        self.enable()

        decision = bnl01_bot.save_user_message(
            42,
            "Crow",
            1,
            "I compare audio mixes before the final release.",
            channel_name="barcode-bot",
            channel_policy="public_home",
            channel_id=10,
            message_id=101,
            route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
        )

        self.assertTrue(decision.save_conversation)
        row_id, raw_route = self.rows(
            "SELECT id,route_mode FROM conversations WHERE role='user'"
        )[0]
        self.assertEqual(raw_route, bnl01_bot.ROUTE_MODE_NORMAL_CHAT)
        self.assertEqual(
            self.rows(
                "SELECT route_mode FROM memory_ledger_entries "
                "WHERE source_table='conversations' AND source_row_id=?",
                (str(row_id),),
            ),
            [(bnl01_bot.ROUTE_MODE_NORMAL_CHAT,)],
        )

        state, selection = self.open_assessment_for_source_row(
            user_id=42,
            source_row_id=row_id,
        )
        self.assertIsNotNone(state)
        self.assertEqual(state.route_mode, bnl01_bot.ROUTE_MODE_NORMAL_CHAT)
        self.assertEqual(
            [item.entry_id for item in selection.items],
            [state.entry_id],
        )

    def test_legacy_schema_exact_immutable_journal_receipt_qualifies_open(self):
        self.replace_conversations_with_legacy_schema()
        self.enable()

        bnl01_bot.save_user_message(
            42,
            "Crow",
            1,
            "I compare audio mixes before the final release.",
            channel_name="barcode-bot",
            channel_policy="public_home",
            channel_id=10,
            message_id=101,
            route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
        )

        self.assertNotIn(
            "route_mode",
            {
                str(row[1])
                for row in self.rows("PRAGMA table_info(conversations)")
            },
        )
        state, selection = self.open_assessment_for_source_row(
            user_id=42,
            source_row_id=1,
        )
        self.assertIsNotNone(state)
        self.assertEqual(state.route_mode, bnl01_bot.ROUTE_MODE_NORMAL_CHAT)
        self.assertEqual(
            [item.entry_id for item in selection.items],
            [state.entry_id],
        )
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            with self.assertRaises(sqlite3.IntegrityError):
                conn.execute(
                    "UPDATE bnl_journal_source_events SET metadata_json='{}'"
                )

    def test_legacy_schema_missing_journal_receipt_stays_out_of_open(self):
        self.replace_conversations_with_legacy_schema()
        self.enable()

        with mock.patch.object(
            bnl01_bot,
            "record_journal_source_event",
            side_effect=RuntimeError("journal unavailable"),
        ):
            bnl01_bot.save_user_message(
                42,
                "Crow",
                1,
                "I compare audio mixes before the final release.",
                channel_name="barcode-bot",
                channel_policy="public_home",
                channel_id=10,
                message_id=101,
                route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
            )

        state, selection = self.open_assessment_for_source_row(
            user_id=42,
            source_row_id=1,
        )
        self.assertIsNone(state)
        self.assertEqual(selection.items, ())

    def test_legacy_schema_mismatched_journal_receipt_stays_out_of_open(self):
        self.replace_conversations_with_legacy_schema()
        self.enable()
        real_record = bnl01_bot.record_journal_source_event

        def record_with_wrong_row(db_path, **kwargs):
            mismatched = dict(kwargs)
            metadata = dict(mismatched.get("metadata") or {})
            metadata["conversationRowId"] = int(
                metadata.get("conversationRowId") or 0
            ) + 1000
            mismatched["metadata"] = metadata
            return real_record(db_path, **mismatched)

        with mock.patch.object(
            bnl01_bot,
            "record_journal_source_event",
            side_effect=record_with_wrong_row,
        ):
            bnl01_bot.save_user_message(
                42,
                "Crow",
                1,
                "I compare audio mixes before the final release.",
                channel_name="barcode-bot",
                channel_policy="public_home",
                channel_id=10,
                message_id=101,
                route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
            )

        self.assertEqual(
            self.rows("SELECT COUNT(*) FROM bnl_journal_source_events"),
            [(1,)],
        )
        state, selection = self.open_assessment_for_source_row(
            user_id=42,
            source_row_id=1,
        )
        self.assertIsNone(state)
        self.assertEqual(selection.items, ())

    def test_retained_repair_preserves_explicit_normal_but_not_unknown_legacy(self):
        self.enable()
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            conn.executemany(
                """
                INSERT INTO conversations(
                    id,user_id,user_name,guild_id,channel_name,
                    channel_policy,channel_id,message_id,route_mode,
                    role,content,timestamp
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    (
                        1,
                        42,
                        "Crow",
                        1,
                        "barcode-bot",
                        "public_home",
                        10,
                        101,
                        "normal_chat",
                        "user",
                        "I compare audio mixes before the final release.",
                        "2026-08-01T12:00:00+00:00",
                    ),
                    (
                        2,
                        43,
                        "Legacy Member",
                        1,
                        "barcode-bot",
                        "public_home",
                        10,
                        102,
                        "unknown",
                        "user",
                        "I organize visual drafts before the archive release.",
                        "2026-08-01T13:00:00+00:00",
                    ),
                ),
            )
            result = ledger.backfill_retained_conversation_ledger_entries(
                conn,
                environ={
                    ledger.MEMORY_LEDGER_SHADOW_ENV: "true",
                    ledger.CONVERSATION_MOTIF_FORMATION_ENV: "false",
                },
            )
            conn.commit()

        self.assertTrue(result["completed"])
        self.assertEqual(
            self.rows(
                "SELECT source_row_id,route_mode FROM memory_ledger_entries "
                "WHERE source_table='conversations' ORDER BY source_row_id"
            ),
            [
                ("1", "normal_chat"),
                ("2", "conversation_continuity"),
            ],
        )
        normal_state, normal_selection = self.open_assessment_for_source_row(
            user_id=42,
            source_row_id=1,
        )
        legacy_state, legacy_selection = self.open_assessment_for_source_row(
            user_id=43,
            source_row_id=2,
        )
        self.assertIsNotNone(normal_state)
        self.assertEqual(
            [item.entry_id for item in normal_selection.items],
            [normal_state.entry_id],
        )
        self.assertIsNone(legacy_state)
        self.assertEqual(legacy_selection.items, ())

    def test_backfill_continuity_enters_open_signal_through_bound_authority(self):
        self.enable()

        inserted = bnl01_bot.insert_backfilled_conversation_row(
            guild_id=1,
            channel_id=10,
            channel_name="barcode-bot",
            channel_policy="public_home",
            user_id=42,
            user_name="Crow",
            content="I compare audio mixes before the final release.",
            timestamp="2026-08-01T12:00:00+00:00",
            message_id=101,
        )

        self.assertTrue(inserted)
        self.assertEqual(
            self.rows("SELECT route_mode FROM conversations"),
            [(bnl01_bot.ROUTE_MODE_CONVERSATION_CONTINUITY,)],
        )
        self.assertEqual(
            self.rows(
                "SELECT route_mode FROM memory_ledger_entries "
                "WHERE source_table='conversations'"
            ),
            [(bnl01_bot.ROUTE_MODE_CONVERSATION_CONTINUITY,)],
        )
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            entry_id = self.rows(
                "SELECT entry_id FROM memory_ledger_entries "
                "WHERE source_table='conversations'"
            )[0][0]
            root_state = ledger.read_public_assessment_root_state(
                conn,
                entry_id=entry_id,
                guild_id=1,
                subject_key="discord_user:42",
            )
            selection = ledger.select_public_conversation_assessment_evidence(
                conn,
                guild_id=1,
                subject_key="discord_user:42",
                request_text="What am I all about?",
            )
            built = packet.build_packet(
                conn,
                packet.IntelligencePacketRequest(
                    guild_id=1,
                    subject_user_id=42,
                    subject_display_name="Crow",
                    route_mode="normal_chat",
                    conversation_surface="mention_or_reply",
                    channel_id=10,
                    channel_name="barcode-bot",
                    channel_policy="public_home",
                    visibility_allowance="public_safe",
                    user_text="What am I all about?",
                    participant_user_ids=(42,),
                    direct_state="direct",
                    now="2026-08-01T12:01:00+00:00",
                ),
                persist=False,
                environ={
                    "BNL_UNIFIED_INTELLIGENCE_PACKET_SHADOW_ENABLED": "true",
                    "BNL_MEMORY_LEDGER_SHADOW_ENABLED": "true",
                    "BNL_MOMENT_ENGINE_SHADOW_ENABLED": "true",
                    "BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED": "true",
                    "BNL_RELATIONSHIP_V2_SHADOW_ENABLED": "true",
                    "BNL_MEMORY_GOVERNANCE_LIVE_ENABLED": "false",
                    "BNL_RELATIONSHIP_V2_LIVE_ENABLED": "false",
                    "BNL_ACTIVE_ENGAGEMENT_V2_LIVE_ENABLED": "false",
                },
            )
            revalidation = packet.revalidate_packet(conn, built)
        self.assertIsNotNone(root_state)
        self.assertEqual(root_state.route_mode, "conversation_continuity")
        self.assertEqual(len(selection.items), 1)
        self.assertEqual(
            selection.items[0].route_mode,
            "conversation_continuity",
        )
        self.assertEqual(built.profile_sufficiency.status, "sparse")
        self.assertEqual(built.request.route_mode, "normal_chat")
        self.assertTrue(revalidation.valid)
        self.assertTrue(
            any(
                item.lane == "assessment_observation"
                and item.text
                == "I compare audio mixes before the final release."
                for item in built.items
            )
        )

    def test_model_send_after_delivery_subject_and_participants(self):
        self.enable()
        asyncio.run(bnl01_bot.send_reply_then_save_model(_Message(), "I remember 8.", user_id=42, guild_id=1, channel_policy="sealed_test"))
        entry = self.rows("SELECT subject_key, source_role, derived, projection, public_usable FROM memory_ledger_entries")[0]
        self.assertEqual(entry, ("bnl_01", "model", 1, 1, 0))
        parts = self.rows("SELECT participant_key, participant_role FROM memory_ledger_participants ORDER BY order_index")
        self.assertEqual(parts[0], ("bnl_01", "author"))
        self.assertEqual(parts[1], ("discord_user:42", "conversation_target"))

    def test_failed_send_creates_no_model_conversation_or_ledger(self):
        self.enable()
        with self.assertRaises(RuntimeError):
            asyncio.run(bnl01_bot.send_reply_then_save_model(_FailMessage(), "No row", user_id=42, guild_id=1, channel_policy="sealed_test"))
        self.assertEqual(self.rows("SELECT COUNT(*) FROM conversations WHERE role='model'")[0][0], 0)
        self.assertEqual(self.rows("SELECT COUNT(*) FROM memory_ledger_entries")[0][0], 0)

    def test_direct_public_fact_is_source_linked_repetition_is_noop_and_change_supersedes(self):
        self.enable()
        bnl01_bot.save_user_message(
            42,
            "Crow",
            1,
            "my favorite color is green",
            channel_name="barcode-bot",
            channel_policy="public_home",
            channel_id=10,
            message_id=101,
            directed_to_bnl=True,
        )
        bnl01_bot.save_user_message(
            42,
            "Crow",
            1,
            "my favorite color is violet",
            channel_name="barcode-bot",
            channel_policy="public_home",
            channel_id=10,
            message_id=102,
            directed_to_bnl=True,
        )
        bnl01_bot.save_user_message(
            42,
            "Crow",
            1,
            "my favorite color is violet",
            channel_name="barcode-bot",
            channel_policy="public_home",
            channel_id=10,
            message_id=103,
            directed_to_bnl=True,
        )

        live_fact = self.rows(
            """
            SELECT fact_key, fact_value, is_core, source_conversation_row_id,
                   source_message_id, source_channel_policy, source_channel_id,
                   source_route_mode, source_kind, source_directed,
                   source_ledger_entry_id, lifecycle_status
            FROM user_memory_facts
            WHERE user_id=42 AND guild_id=1 AND fact_key='favorite_color'
            """
        )
        self.assertEqual(len(live_fact), 1)
        self.assertEqual(live_fact[0][:10], (
            "favorite_color",
            "violet",
            0,
            2,
            102,
            "public_home",
            10,
            bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
            "member_self_report",
            1,
        ))
        self.assertTrue(live_fact[0][10].startswith("mle_"))
        self.assertEqual(live_fact[0][11], "active")

        entries = self.rows(
            """
            SELECT entry_id, normalized_value, lifecycle_status, source_row_id,
                   source_message_id, public_usable
            FROM memory_ledger_entries
            WHERE source_role='member_self_report'
              AND predicate_key='favorite_color'
            ORDER BY source_sequence
            """
        )
        self.assertEqual(len(entries), 2)
        original_id, replacement_id = entries[0][0], entries[1][0]
        self.assertEqual(entries[0][1:], ("green", "superseded", "1", 101, 1))
        self.assertEqual(entries[1][1:], ("violet", "active", "2", 102, 1))
        self.assertEqual(
            set(self.rows(
                "SELECT lineage_type, target_entry_id FROM memory_ledger_lineage WHERE entry_id=?",
                (replacement_id,),
            )),
            {
                ("correction_of", original_id),
                ("supersedes", original_id),
            },
        )
        self.assertEqual(live_fact[0][10], replacement_id)

    def test_discord_queue_and_payment_discussion_is_not_content_excluded(self):
        self.enable()
        samples = [
            "is the queue open?",
            "payment is pending",
            "what's your availability?",
            "the current session was fun",
            "we were talking about the queue",
            "the DJ queue is full tonight",
        ]
        for idx, text in enumerate(samples, start=1):
            bnl01_bot.save_user_message(42, "Crow", 1, text, channel_policy="public_home", message_id=idx)
        rows = self.rows("SELECT normalized_value FROM memory_ledger_entries WHERE source_table='conversations' ORDER BY source_row_id")
        self.assertEqual([r[0] for r in rows], samples)
        self.assertEqual(self.rows("SELECT COUNT(*) FROM memory_ledger_shadow_receipts WHERE reason_code='queue_operational_state_excluded'")[0][0], 0)

    def test_partial_shadow_failure_rolls_back_and_records_safe_error(self):
        self.enable()
        def partial_then_fail(conn):
            ledger.shadow_conversation_row(conn, row_id=1, user_id=42, user_name="Crow", guild_id=1, role="user", content="remember this number: 8", channel_policy="sealed_test")
            raise RuntimeError("boom with PRIVATE CONTENT")
        bnl01_bot.save_user_message(42, "Crow", 1, "legacy survives", channel_policy="sealed_test")
        bnl01_bot._shadow_memory_ledger_write("partial_failure_test", partial_then_fail, guild_id=1, source_table="conversations", source_row_id=999, source_revision="999")
        self.assertEqual(self.rows("SELECT COUNT(*) FROM conversations")[0][0], 1)
        self.assertEqual(self.rows("SELECT COUNT(*) FROM memory_ledger_entries WHERE normalized_value='remember this number: 8'")[0][0], 0)
        self.assertEqual(self.rows("SELECT outcome, reason_code FROM memory_ledger_shadow_receipts WHERE writer='partial_failure_test'")[0], ("error", "shadow_exception"))
        self.assertNotIn("PRIVATE CONTENT", str(self.rows("SELECT * FROM memory_ledger_shadow_receipts")))

    def test_broadcast_insert_cleaned_summary_and_raw_absent_from_receipts(self):
        self.enable()
        msg = _Message("RAW SECRET note")
        new_id = bnl01_bot.add_broadcast_memory_entry(1, msg, {"cleaned_summary": "Clean show summary.", "entry_type": "show_note", "public_safe": True, "usage_scope": "public"})
        self.assertGreater(new_id, 0)
        self.assertEqual(self.rows("SELECT normalized_value FROM memory_ledger_entries WHERE source_table='broadcast_memory'")[0][0], "Clean show summary.")
        dump = str(self.rows("SELECT * FROM memory_ledger_shadow_receipts"))
        self.assertNotIn("RAW SECRET", dump)

    def test_broadcast_low_level_mutations_require_configured_owner(self):
        msg = _Message("RAW owner boundary note")
        with mock.patch.object(bnl01_bot, "BNL_OWNER_USER_ID", 0):
            with self.assertRaisesRegex(
                PermissionError,
                "owner_user_id_not_configured",
            ):
                bnl01_bot.add_broadcast_memory_entry(
                    1,
                    msg,
                    {
                        "cleaned_summary": "No write is allowed.",
                        "entry_type": "notable_moment",
                    },
                )
        with mock.patch.object(bnl01_bot, "BNL_OWNER_USER_ID", 999):
            with self.assertRaisesRegex(
                PermissionError,
                "configured_owner_required",
            ):
                bnl01_bot.add_broadcast_memory_entry(
                    1,
                    msg,
                    {
                        "cleaned_summary": "Still no write is allowed.",
                        "entry_type": "notable_moment",
                    },
                )
        self.assertEqual(self.rows("SELECT COUNT(*) FROM broadcast_memory")[0][0], 0)

    def test_broadcast_low_level_mutations_require_configured_primary_guild(self):
        msg = _Message("RAW primary guild boundary note")
        with mock.patch.object(bnl01_bot, "BNL_PRIMARY_GUILD_ID", 0):
            with self.assertRaisesRegex(
                PermissionError,
                "primary_guild_id_not_configured",
            ):
                bnl01_bot.add_broadcast_memory_entry(
                    1,
                    msg,
                    {
                        "cleaned_summary": "No write without configured scope.",
                        "entry_type": "notable_moment",
                    },
                )
        with self.assertRaisesRegex(
            PermissionError,
            "configured_primary_guild_required",
        ):
            bnl01_bot.add_broadcast_memory_entry(
                2,
                msg,
                {
                    "cleaned_summary": "No cross-guild owner write.",
                    "entry_type": "notable_moment",
                },
            )
        self.assertEqual(self.rows("SELECT COUNT(*) FROM broadcast_memory")[0][0], 0)

    def test_raw_broadcast_insert_helper_rechecks_owner_and_guild(self):
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            with mock.patch.object(bnl01_bot, "BNL_OWNER_USER_ID", 999):
                with self.assertRaisesRegex(
                    PermissionError,
                    "configured_owner_required",
                ):
                    bnl01_bot._insert_broadcast_memory_row(
                        conn,
                        guild_id=1,
                        actor_id=42,
                        actor_name="forged",
                        raw_content="RAW direct helper bypass",
                        processed={
                            "cleaned_summary": "Must not be inserted.",
                            "entry_type": "notable_moment",
                        },
                        now="2026-08-01T00:00:00+00:00",
                    )
        self.assertEqual(self.rows("SELECT COUNT(*) FROM broadcast_memory")[0][0], 0)

    def test_broadcast_status_mutation_rejects_cross_guild_scope(self):
        memory_id = bnl01_bot.add_broadcast_memory_entry(
            1,
            _Message("RAW scoped status note"),
            {
                "cleaned_summary": "Guild-scoped status fixture.",
                "entry_type": "notable_moment",
                "public_safe": True,
                "usage_scope": "ambient,direct",
            },
        )
        with self.assertRaisesRegex(
            PermissionError,
            "configured_primary_guild_required",
        ):
            bnl01_bot._set_broadcast_memory_status(
                2,
                memory_id,
                "resolved",
                42,
                "6 Bit",
                "cross-guild hostile fixture",
            )
        self.assertEqual(
            self.rows(
                "SELECT status FROM broadcast_memory WHERE guild_id=1 AND id=?",
                (memory_id,),
            )[0][0],
            "active",
        )

    def test_broadcast_status_helper_rejects_unowned_transition(self):
        memory_id = bnl01_bot.add_broadcast_memory_entry(
            1,
            _Message("RAW status transition note"),
            {
                "cleaned_summary": "Status transition fixture.",
                "entry_type": "notable_moment",
            },
        )
        with self.assertRaisesRegex(
            ValueError,
            "broadcast_status_transition_not_allowed",
        ):
            bnl01_bot._set_broadcast_memory_status(
                1,
                memory_id,
                "active",
                42,
                "6 Bit",
                "attempted reactivation",
            )
        self.assertEqual(
            self.rows("SELECT status FROM broadcast_memory WHERE id=?", (memory_id,))[0][0],
            "active",
        )

    def test_broadcast_status_cannot_rewrite_superseded_source(self):
        original_id = bnl01_bot.add_broadcast_memory_entry(
            1,
            _Message("RAW original status fixture"),
            {
                "cleaned_summary": "Original status fixture.",
                "entry_type": "notable_moment",
            },
        )
        replacement_id = bnl01_bot._replace_broadcast_memory_entry(
            1,
            original_id,
            _Message("RAW replacement status fixture"),
            {
                "cleaned_summary": "Replacement status fixture.",
                "entry_type": "notable_moment",
            },
        )
        self.assertEqual(
            bnl01_bot._set_broadcast_memory_status(
                1,
                original_id,
                "resolved",
                42,
                "6 Bit",
                "hostile terminal rewrite",
            ),
            0,
        )
        self.assertEqual(
            self.rows(
                "SELECT status,superseded_by_id FROM broadcast_memory WHERE id=?",
                (original_id,),
            )[0],
            ("superseded", replacement_id),
        )

    def test_broadcast_status_resolution_is_single_use_and_retracts_source(self):
        self.enable()
        memory_id = bnl01_bot.add_broadcast_memory_entry(
            1,
            _Message("RAW status resolution note"),
            {
                "cleaned_summary": "Status resolution fixture.",
                "entry_type": "notable_moment",
                "public_safe": True,
                "usage_scope": "ambient,direct",
            },
        )
        self.assertEqual(
            bnl01_bot._set_broadcast_memory_status(
                1, memory_id, "resolved", 42, "6 Bit", "no longer current"
            ),
            1,
        )
        self.assertEqual(
            bnl01_bot._set_broadcast_memory_status(
                1, memory_id, "resolved", 42, "6 Bit", "replayed command"
            ),
            0,
        )
        source_entry = self.rows(
            """
            SELECT entry_id FROM memory_ledger_entries
            WHERE source_table='broadcast_memory'
              AND source_role='broadcast_memory' AND source_row_id=?
            """,
            (str(memory_id),),
        )[0][0]
        status_entry = self.rows(
            """
            SELECT entry_id FROM memory_ledger_entries
            WHERE source_table='broadcast_memory'
              AND source_role='broadcast_memory_status' AND source_row_id=?
            """,
            (str(memory_id),),
        )[0][0]
        self.assertEqual(
            self.rows(
                """
                SELECT lineage_type,target_entry_id
                FROM memory_ledger_lineage WHERE entry_id=?
                """,
                (status_entry,),
            ),
            [("retracts", source_entry)],
        )

    def test_broadcast_status_resolution_retracts_all_duplicate_primary_roots(self):
        self.enable()
        memory_id = bnl01_bot.add_broadcast_memory_entry(
            1,
            _Message("RAW duplicate primary resolution note"),
            {
                "cleaned_summary": "Duplicate primary resolution fixture.",
                "entry_type": "notable_moment",
                "public_safe": True,
                "usage_scope": "ambient,direct",
            },
        )
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            duplicate = ledger.shadow_broadcast_memory_row(
                conn,
                row_id=memory_id,
                guild_id=1,
                cleaned_summary="Duplicate primary resolution fixture.",
                entry_type="notable_moment",
                public_safe=True,
                status="active",
                usage_scope="ambient,direct",
                updated_at="duplicate-effective-revision",
            )
            self.assertEqual(duplicate.outcome, "inserted")
        primary_ids = {
            row[0]
            for row in self.rows(
                """
                SELECT entry_id FROM memory_ledger_entries
                WHERE source_table='broadcast_memory'
                  AND source_role='broadcast_memory' AND source_row_id=?
                """,
                (str(memory_id),),
            )
        }
        self.assertEqual(len(primary_ids), 2)
        self.assertEqual(
            bnl01_bot._set_broadcast_memory_status(
                1, memory_id, "resolved", 42, "6 Bit", "terminal correction"
            ),
            1,
        )
        status_entry = self.rows(
            """
            SELECT entry_id FROM memory_ledger_entries
            WHERE source_table='broadcast_memory'
              AND source_role='broadcast_memory_status' AND source_row_id=?
            """,
            (str(memory_id),),
        )[0][0]
        self.assertEqual(
            {
                row[0]
                for row in self.rows(
                    """
                    SELECT target_entry_id FROM memory_ledger_lineage
                    WHERE entry_id=? AND lineage_type='retracts'
                    """,
                    (status_entry,),
                )
            },
            primary_ids,
        )

    def test_broadcast_status_resolution_retracts_declared_projection_atomically(self):
        self.enable()
        memory_id = bnl01_bot.add_broadcast_memory_entry(
            1,
            _Message("RAW declared projection status note"),
            {
                "cleaned_summary": "Declared projection status fixture.",
                "entry_type": "notable_moment",
                "public_safe": True,
                "usage_scope": "ambient,direct",
            },
        )
        primary_id = self.rows(
            """
            SELECT entry_id FROM memory_ledger_entries
            WHERE source_table='broadcast_memory'
              AND source_role='broadcast_memory' AND source_row_id=?
            """,
            (str(memory_id),),
        )[0][0]
        projection = self.add_declared_broadcast_projection(
            memory_id=memory_id,
            root_entry_id=primary_id,
            suffix="status-success",
        )

        self.assertEqual(
            bnl01_bot._set_broadcast_memory_status(
                1, memory_id, "resolved", 42, "6 Bit", "terminal correction"
            ),
            1,
        )

        status_entry = self.rows(
            """
            SELECT entry_id FROM memory_ledger_entries
            WHERE source_table='broadcast_memory'
              AND source_role='broadcast_memory_status' AND source_row_id=?
            """,
            (str(memory_id),),
        )[0][0]
        projection_status_entry = self.rows(
            """
            SELECT entry_id FROM memory_ledger_entries
            WHERE source_table='broadcast_memory'
              AND source_role='broadcast_memory_projection_status'
              AND source_row_id=?
            """,
            (str(memory_id),),
        )[0][0]
        self.assertEqual(
            self.rows(
                """
                SELECT lineage_type,target_entry_id
                FROM memory_ledger_lineage WHERE entry_id=?
                """,
                (status_entry,),
            ),
            [("retracts", primary_id)],
        )
        self.assertEqual(
            self.rows(
                """
                SELECT lineage_type,target_entry_id
                FROM memory_ledger_lineage WHERE entry_id=?
                """,
                (projection_status_entry,),
            ),
            [("retracts", projection.entry_id)],
        )

    def test_broadcast_projection_retraction_failure_rolls_back_source_status(self):
        self.enable()
        memory_id = bnl01_bot.add_broadcast_memory_entry(
            1,
            _Message("RAW declared rollback status note"),
            {
                "cleaned_summary": "Declared rollback status fixture.",
                "entry_type": "notable_moment",
                "public_safe": True,
                "usage_scope": "ambient,direct",
            },
        )
        primary_id = self.rows(
            """
            SELECT entry_id FROM memory_ledger_entries
            WHERE source_table='broadcast_memory'
              AND source_role='broadcast_memory' AND source_row_id=?
            """,
            (str(memory_id),),
        )[0][0]
        projection = self.add_declared_broadcast_projection(
            memory_id=memory_id,
            root_entry_id=primary_id,
            suffix="status-rollback",
        )
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            conn.execute(
                """
                CREATE TRIGGER reject_bot_projection_retraction
                BEFORE INSERT ON memory_ledger_lineage
                WHEN NEW.lineage_type='retracts'
                 AND NEW.target_entry_id='%s'
                BEGIN
                    SELECT RAISE(ABORT, 'bot projection retraction rejected');
                END
                """ % projection.entry_id
            )

        with self.assertRaisesRegex(
            sqlite3.IntegrityError, "bot projection retraction rejected"
        ):
            bnl01_bot._set_broadcast_memory_status(
                1, memory_id, "resolved", 42, "6 Bit", "must roll back"
            )

        self.assertEqual(
            self.rows(
                "SELECT status FROM broadcast_memory WHERE id=?",
                (memory_id,),
            ),
            [("active",)],
        )
        self.assertEqual(
            self.rows(
                """
                SELECT COUNT(*) FROM memory_ledger_entries
                WHERE source_table='broadcast_memory'
                  AND source_row_id=?
                  AND source_role IN (
                    'broadcast_memory_status',
                    'broadcast_memory_projection_status'
                  )
                """,
                (str(memory_id),),
            ),
            [(0,)],
        )

    def test_broadcast_status_resolution_cannot_rewrite_superseded_source(self):
        self.enable()
        original_id = bnl01_bot.add_broadcast_memory_entry(
            1,
            _Message("RAW original status source"),
            {
                "cleaned_summary": "Original source.",
                "entry_type": "notable_moment",
                "public_safe": True,
                "usage_scope": "ambient,direct",
            },
        )
        replacement_id = bnl01_bot._replace_broadcast_memory_entry(
            1,
            original_id,
            _Message("RAW replacement status source"),
            {
                "cleaned_summary": "Replacement source.",
                "entry_type": "notable_moment",
                "public_safe": True,
                "usage_scope": "ambient,direct",
            },
        )
        before = self.rows(
            """
            SELECT status,superseded_by_id,corrected_by_user_id,correction_reason
            FROM broadcast_memory WHERE id=?
            """,
            (original_id,),
        )[0]
        self.assertEqual(
            bnl01_bot._set_broadcast_memory_status(
                1, original_id, "resolved", 42, "6 Bit", "hostile replay"
            ),
            0,
        )
        self.assertEqual(
            self.rows(
                """
                SELECT status,superseded_by_id,corrected_by_user_id,correction_reason
                FROM broadcast_memory WHERE id=?
                """,
                (original_id,),
            )[0],
            before,
        )
        self.assertEqual(before[0:2], ("superseded", replacement_id))

    def test_clear_show_state_low_level_helper_requires_owner(self):
        with mock.patch.object(bnl01_bot, "BNL_OWNER_USER_ID", 999):
            with self.assertRaisesRegex(
                PermissionError,
                "configured_owner_required",
            ):
                bnl01_bot.clear_active_show_state_overrides(1, 42)

    def test_clear_show_state_records_each_resolved_lineage_event(self):
        self.enable()
        ids = [
            bnl01_bot.add_broadcast_memory_entry(
                1,
                _Message(f"RAW show override {index}"),
                {
                    "cleaned_summary": f"Show override fixture {index}.",
                    "entry_type": "show_state_override",
                    "importance": "high",
                    "public_safe": True,
                    "usage_scope": "show_status,direct",
                },
            )
            for index in (1, 2)
        ]
        self.assertEqual(bnl01_bot.clear_active_show_state_overrides(1, 42), 2)
        self.assertEqual(
            self.rows(
                """
                SELECT id,status,corrected_by_user_id,corrected_by_name,
                       correction_reason
                FROM broadcast_memory WHERE id IN (?,?) ORDER BY id
                """,
                tuple(ids),
            ),
            [
                (
                    ids[0],
                    "resolved",
                    42,
                    "configured_owner",
                    "owner restored normal show state",
                ),
                (
                    ids[1],
                    "resolved",
                    42,
                    "configured_owner",
                    "owner restored normal show state",
                ),
            ],
        )
        self.assertEqual(
            self.rows(
                """
                SELECT source_row_id,lifecycle_status
                FROM memory_ledger_entries
                WHERE source_table='broadcast_memory'
                  AND source_role='broadcast_memory_status'
                ORDER BY CAST(source_row_id AS INTEGER)
                """
            ),
            [(str(ids[0]), "resolved"), (str(ids[1]), "resolved")],
        )

    def test_clear_show_state_rolls_back_before_shadow_on_update_failure(self):
        self.enable()
        memory_id = bnl01_bot.add_broadcast_memory_entry(
            1,
            _Message("RAW show override rollback"),
            {
                "cleaned_summary": "Show override rollback fixture.",
                "entry_type": "show_state_override",
                "importance": "high",
            },
        )
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            conn.execute(
                """
                CREATE TRIGGER reject_test_show_state_resolution
                BEFORE UPDATE OF status ON broadcast_memory
                WHEN NEW.status='resolved'
                BEGIN
                    SELECT RAISE(ABORT, 'forced show-state failure');
                END
                """
            )
        with self.assertRaises(sqlite3.DatabaseError):
            bnl01_bot.clear_active_show_state_overrides(1, 42)
        self.assertEqual(
            self.rows("SELECT status FROM broadcast_memory WHERE id=?", (memory_id,))[0][0],
            "active",
        )
        self.assertEqual(
            self.rows(
                """
                SELECT COUNT(*) FROM memory_ledger_entries
                WHERE source_table='broadcast_memory'
                  AND source_role='broadcast_memory_status'
                  AND source_row_id=?
                """,
                (str(memory_id),),
            )[0][0],
            0,
        )

    def test_broadcast_add_rolls_back_when_required_primary_shadow_fails(self):
        self.enable()
        with mock.patch.object(
            bnl01_bot,
            "shadow_broadcast_memory_row",
            side_effect=RuntimeError("forced primary shadow failure"),
        ):
            with self.assertRaisesRegex(
                RuntimeError,
                "forced primary shadow failure",
            ):
                bnl01_bot.add_broadcast_memory_entry(
                    1,
                    _Message("RAW atomic add rollback"),
                    {
                        "cleaned_summary": "Atomic add rollback fixture.",
                        "entry_type": "notable_moment",
                        "public_safe": True,
                        "usage_scope": "ambient,direct",
                    },
                )
        self.assertEqual(
            self.rows("SELECT COUNT(*) FROM broadcast_memory")[0][0],
            0,
        )
        self.assertEqual(
            self.rows(
                "SELECT COUNT(*) FROM memory_ledger_entries "
                "WHERE source_table='broadcast_memory'"
            )[0][0],
            0,
        )

    def test_broadcast_add_rejects_a_wrong_primary_deduplication(self):
        self.enable()
        hostile_dedup = ledger.LedgerWriteResult(
            entry_id="mle_hostile_existing_snapshot",
            outcome="deduplicated",
            reason_code="exact_source_duplicate",
            source_table="broadcast_memory",
            source_row_id="1",
            source_revision="rev:1:hostile",
            guild_id=1,
        )
        with mock.patch.object(
            bnl01_bot,
            "shadow_broadcast_memory_row",
            return_value=hostile_dedup,
        ):
            with self.assertRaisesRegex(
                RuntimeError,
                "broadcast_shadow_write_required:deduplicated",
            ):
                bnl01_bot.add_broadcast_memory_entry(
                    1,
                    _Message("RAW hostile dedup rollback"),
                    {
                        "cleaned_summary": "Hostile dedup rollback fixture.",
                        "entry_type": "notable_moment",
                        "public_safe": True,
                        "usage_scope": "ambient,direct",
                    },
                )
        self.assertEqual(
            self.rows("SELECT COUNT(*) FROM broadcast_memory")[0][0],
            0,
        )

    def test_broadcast_resolution_rolls_back_when_required_retraction_fails(self):
        self.enable()
        memory_id = bnl01_bot.add_broadcast_memory_entry(
            1,
            _Message("RAW atomic resolve rollback"),
            {
                "cleaned_summary": "Atomic resolve rollback fixture.",
                "entry_type": "notable_moment",
                "public_safe": True,
                "usage_scope": "ambient,direct",
            },
        )
        with mock.patch.object(
            bnl01_bot,
            "shadow_broadcast_status_event",
            side_effect=RuntimeError("forced status shadow failure"),
        ):
            with self.assertRaisesRegex(
                RuntimeError,
                "forced status shadow failure",
            ):
                bnl01_bot._set_broadcast_memory_status(
                    1,
                    memory_id,
                    "resolved",
                    42,
                    "6 Bit",
                    "atomic rollback fixture",
                )
        self.assertEqual(
            self.rows(
                "SELECT status FROM broadcast_memory WHERE id=?",
                (memory_id,),
            )[0][0],
            "active",
        )
        self.assertEqual(
            self.rows(
                """
                SELECT COUNT(*) FROM memory_ledger_entries
                WHERE source_table='broadcast_memory'
                  AND source_role='broadcast_memory_status'
                  AND source_row_id=?
                """,
                (str(memory_id),),
            )[0][0],
            0,
        )

    def test_disabled_shadow_still_retracts_an_existing_primary(self):
        self.enable()
        memory_id = bnl01_bot.add_broadcast_memory_entry(
            1,
            _Message("RAW disabled gate retraction"),
            {
                "cleaned_summary": "Existing public primary fixture.",
                "entry_type": "notable_moment",
                "public_safe": True,
                "usage_scope": "ambient,direct",
            },
        )
        os.environ.pop("BNL_MEMORY_LEDGER_SHADOW_ENABLED", None)
        self.assertEqual(
            bnl01_bot._set_broadcast_memory_status(
                1,
                memory_id,
                "resolved",
                42,
                "6 Bit",
                "source no longer current",
            ),
            1,
        )
        primary_id = self.rows(
            """
            SELECT entry_id FROM memory_ledger_entries
            WHERE source_table='broadcast_memory'
              AND source_role='broadcast_memory' AND source_row_id=?
            """,
            (str(memory_id),),
        )[0][0]
        self.assertEqual(
            self.rows(
                """
                SELECT l.lineage_type,l.target_entry_id
                FROM memory_ledger_lineage l
                JOIN memory_ledger_entries e ON e.entry_id=l.entry_id
                WHERE e.source_table='broadcast_memory'
                  AND e.source_role='broadcast_memory_status'
                  AND e.source_row_id=?
                """,
                (str(memory_id),),
            ),
            [("retracts", primary_id)],
        )

    def test_disabled_shadow_replacement_without_primary_writes_no_ledger_rows(self):
        memory_id = bnl01_bot.add_broadcast_memory_entry(
            1,
            _Message("RAW disabled replacement source"),
            {
                "cleaned_summary": "Disabled replacement source.",
                "entry_type": "notable_moment",
                "public_safe": True,
                "usage_scope": "ambient,direct",
            },
        )
        self.assertEqual(
            self.rows(
                "SELECT COUNT(*) FROM memory_ledger_entries "
                "WHERE source_table='broadcast_memory'"
            )[0][0],
            0,
        )

        replacement_id = bnl01_bot._replace_broadcast_memory_entry(
            1,
            memory_id,
            _Message("RAW disabled replacement target"),
            {
                "cleaned_summary": "Disabled replacement target.",
                "entry_type": "notable_moment",
                "public_safe": True,
                "usage_scope": "ambient,direct",
            },
        )

        self.assertGreater(replacement_id, memory_id)
        self.assertEqual(
            self.rows(
                "SELECT id,status,supersedes_id,superseded_by_id "
                "FROM broadcast_memory ORDER BY id"
            ),
            [
                (memory_id, "superseded", None, replacement_id),
                (replacement_id, "active", memory_id, None),
            ],
        )
        self.assertEqual(
            self.rows(
                "SELECT COUNT(*) FROM memory_ledger_entries "
                "WHERE source_table='broadcast_memory'"
            )[0][0],
            0,
        )

    def test_replacement_retracts_all_duplicate_existing_primaries(self):
        self.enable()
        memory_id = bnl01_bot.add_broadcast_memory_entry(
            1,
            _Message("RAW ambiguous primary fixture"),
            {
                "cleaned_summary": "Ambiguous primary fixture.",
                "entry_type": "notable_moment",
                "public_safe": True,
                "usage_scope": "ambient,direct",
            },
        )
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            second = ledger.shadow_broadcast_memory_row(
                conn,
                row_id=memory_id,
                guild_id=1,
                cleaned_summary="Conflicting stale primary fixture.",
                entry_type="notable_moment",
                public_safe=True,
                status="active",
                usage_scope="ambient,direct",
                submitted_by_user_id=42,
                submitted_by_name="6 Bit",
                created_at="2026-08-01T00:00:00+00:00",
                updated_at="2026-08-01T00:01:00+00:00",
            )
            self.assertEqual(second.outcome, "inserted")
        old_primary_ids = {
            row[0]
            for row in self.rows(
                """
                SELECT entry_id FROM memory_ledger_entries
                WHERE source_table='broadcast_memory'
                  AND source_role='broadcast_memory' AND source_row_id=?
                """,
                (str(memory_id),),
            )
        }
        self.assertEqual(len(old_primary_ids), 2)
        os.environ.pop("BNL_MEMORY_LEDGER_SHADOW_ENABLED", None)
        replacement_id = bnl01_bot._replace_broadcast_memory_entry(
            1,
            memory_id,
            _Message("RAW duplicate-root replacement"),
            {
                "cleaned_summary": "Replacement after duplicate roots.",
                "entry_type": "notable_moment",
                "public_safe": True,
                "usage_scope": "ambient,direct",
            },
        )
        self.assertGreater(replacement_id, memory_id)
        self.assertEqual(
            self.rows(
                "SELECT status,superseded_by_id FROM broadcast_memory WHERE id=?",
                (memory_id,),
            )[0],
            ("superseded", replacement_id),
        )
        status_entry = self.rows(
            """
            SELECT entry_id FROM memory_ledger_entries
            WHERE source_table='broadcast_memory'
              AND source_role='broadcast_memory_status' AND source_row_id=?
            """,
            (str(memory_id),),
        )[0][0]
        self.assertEqual(
            {
                row[0]
                for row in self.rows(
                    """
                    SELECT target_entry_id FROM memory_ledger_lineage
                    WHERE entry_id=? AND lineage_type='retracts'
                    """,
                    (status_entry,),
                )
            },
            old_primary_ids,
        )
        self.assertEqual(
            len(
                self.rows(
                    """
                    SELECT entry_id FROM memory_ledger_entries
                    WHERE source_table='broadcast_memory'
                      AND source_role='broadcast_memory' AND source_row_id=?
                    """,
                    (str(replacement_id),),
                )
            ),
            1,
        )

    def test_broadcast_replacement_has_one_revision_and_real_lineage(self):
        self.enable()
        original_id = bnl01_bot.add_broadcast_memory_entry(
            1,
            _Message("RAW original"),
            {
                "cleaned_summary": "Original safe summary.",
                "entry_type": "show_note",
                "public_safe": True,
                "usage_scope": "ambient,direct",
            },
        )
        replacement_id = bnl01_bot._replace_broadcast_memory_entry(
            1,
            original_id,
            _Message("RAW replacement"),
            {
                "cleaned_summary": "Replacement safe summary.",
                "entry_type": "show_note",
                "public_safe": True,
                "usage_scope": "ambient,direct",
            },
        )
        self.assertEqual(
            self.rows(
                "SELECT status,superseded_by_id FROM broadcast_memory WHERE id=?",
                (original_id,),
            )[0],
            ("superseded", replacement_id),
        )
        primary_rows = self.rows(
            """
            SELECT source_row_id, entry_id, normalized_value
            FROM memory_ledger_entries
            WHERE source_table='broadcast_memory' AND source_role='broadcast_memory'
            ORDER BY CAST(source_row_id AS INTEGER)
            """
        )
        self.assertEqual(len(primary_rows), 2)
        original_entry = primary_rows[0][1]
        replacement_entry = primary_rows[1][1]
        self.assertEqual(primary_rows[0][0], str(original_id))
        self.assertEqual(primary_rows[1][0], str(replacement_id))
        self.assertEqual(primary_rows[1][2], "Replacement safe summary.")
        self.assertEqual(
            self.rows(
                "SELECT COUNT(*) FROM memory_ledger_entries WHERE source_table='broadcast_memory' AND source_role='broadcast_memory' AND source_row_id=?",
                (str(replacement_id),),
            )[0][0],
            1,
        )
        self.assertEqual(
            self.rows(
                "SELECT COUNT(*) FROM memory_ledger_entries WHERE source_table='broadcast_memory' AND source_role='broadcast_memory_status' AND source_row_id=?",
                (str(original_id),),
            )[0][0],
            1,
        )
        self.assertEqual(
            set(self.rows("SELECT lineage_type, target_entry_id FROM memory_ledger_lineage WHERE entry_id=?", (replacement_entry,))),
            {("correction_of", original_entry), ("supersedes", original_entry)},
        )
        self.assertEqual(
            self.rows(
                """
                SELECT COUNT(*)
                FROM memory_ledger_lineage l
                LEFT JOIN memory_ledger_entries e
                  ON e.guild_id=l.guild_id AND e.entry_id=l.target_entry_id
                WHERE e.entry_id IS NULL
                """
            )[0][0],
            0,
        )
        effective_active = self.rows(
            """
            SELECT entry_id
            FROM memory_ledger_entries
            WHERE source_table='broadcast_memory'
              AND source_role='broadcast_memory'
              AND lifecycle_status='active'
              AND entry_id NOT IN (
                SELECT target_entry_id FROM memory_ledger_lineage
                WHERE lineage_type IN ('supersedes', 'retracts')
              )
            """
        )
        self.assertEqual(effective_active, [(replacement_entry,)])

    def test_broadcast_replacement_requires_atomic_helper(self):
        original_id = bnl01_bot.add_broadcast_memory_entry(
            1,
            _Message("RAW original"),
            {
                "cleaned_summary": "Original safe summary.",
                "entry_type": "notable_moment",
                "public_safe": True,
                "usage_scope": "ambient,direct",
            },
        )
        with self.assertRaisesRegex(
            ValueError,
            "broadcast_replacement_requires_atomic_helper",
        ):
            bnl01_bot.add_broadcast_memory_entry(
                1,
                _Message("RAW unsafe split replacement"),
                {
                    "cleaned_summary": "This must not be stored separately.",
                    "entry_type": "notable_moment",
                    "supersedes_id": original_id,
                },
            )
        self.assertEqual(self.rows("SELECT COUNT(*) FROM broadcast_memory")[0][0], 1)

    def test_broadcast_replacement_rolls_back_if_supersession_update_fails(self):
        original_id = bnl01_bot.add_broadcast_memory_entry(
            1,
            _Message("RAW original"),
            {
                "cleaned_summary": "Original safe summary.",
                "entry_type": "notable_moment",
                "public_safe": True,
                "usage_scope": "ambient,direct",
            },
        )
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            conn.execute(
                """
                CREATE TRIGGER reject_test_broadcast_supersession
                BEFORE UPDATE OF status ON broadcast_memory
                WHEN NEW.status='superseded'
                BEGIN
                    SELECT RAISE(ABORT, 'forced supersession failure');
                END
                """
            )
        with self.assertRaises(sqlite3.DatabaseError):
            bnl01_bot._replace_broadcast_memory_entry(
                1,
                original_id,
                _Message("RAW replacement"),
                {
                    "cleaned_summary": "Replacement must roll back.",
                    "entry_type": "notable_moment",
                    "public_safe": True,
                    "usage_scope": "ambient,direct",
                },
            )
        self.assertEqual(
            self.rows(
                "SELECT id,status,superseded_by_id FROM broadcast_memory ORDER BY id"
            ),
            [(original_id, "active", None)],
        )

    def test_prune_purges_exact_raw_source_and_moment_before_transcript_delete(self):
        self.enable()
        bnl01_bot.save_user_message(
            42,
            "Crow",
            1,
            "my favorite color is green",
            channel_name="barcode-bot",
            channel_policy="public_home",
            channel_id=10,
            message_id=101,
            directed_to_bnl=True,
        )
        old_row_id = self._conversation_id(
            guild_id=1,
            user_id=42,
            content="my favorite color is green",
        )
        bnl01_bot.save_user_message(
            42,
            "Crow",
            1,
            "newer target row",
            channel_policy="sealed_test",
        )
        keep_row_id = self._conversation_id(
            guild_id=1,
            user_id=42,
            content="newer target row",
        )
        bnl01_bot.save_user_message(
            43,
            "Other",
            1,
            "same guild other member",
            channel_policy="sealed_test",
        )
        other_member_row_id = self._conversation_id(
            guild_id=1,
            user_id=43,
            content="same guild other member",
        )
        bnl01_bot.save_user_message(
            42,
            "Crow",
            2,
            "same member other guild",
            channel_policy="sealed_test",
        )
        other_guild_row_id = self._conversation_id(
            guild_id=2,
            user_id=42,
            content="same member other guild",
        )
        moment_id, raw_entry_id, canonical_entry_id = self._attach_test_moment(
            guild_id=1,
            source_row_id=old_row_id,
        )
        scalar_entry_id = self.rows(
            """
            SELECT entry_id
            FROM memory_ledger_entries
            WHERE guild_id=1
              AND source_table='conversations'
              AND source_row_id=?
              AND predicate_key='favorite_color'
              AND entry_type='preference'
            """,
            (str(old_row_id),),
        )[0][0]
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            legacy_raw = ledger.insert_ledger_entry(
                conn,
                ledger.LedgerEntry(
                    guild_id=1,
                    source_table="conversations",
                    source_row_id=old_row_id,
                    source_revision=str(old_row_id),
                    source_role="user",
                    entry_type="observation",
                    subject_key=ledger.subject_key_for_user(42),
                    predicate_key="remembered_number",
                    value="1234",
                    source_class=ledger.SourceClass.PUBLIC_OBSERVATION,
                    visibility=ledger.Visibility.PUBLIC_SAFE,
                    confidence=ledger.Confidence.MEDIUM,
                    public_usable=False,
                    observed_at="2026-07-23T00:00:00+00:00",
                ),
            )
            conn.commit()
        self.assertTrue(legacy_raw.entry_id)

        real_purge = bnl01_bot.purge_conversation_ledger_sources
        observed = {}

        def inspect_then_purge(conn, *, guild_id, source_row_ids, reason):
            exact_ids = tuple(sorted(int(row_id) for row_id in source_row_ids))
            placeholders = ",".join("?" for _ in exact_ids)
            observed["guild_id"] = guild_id
            observed["source_row_ids"] = exact_ids
            observed["reason"] = reason
            observed["transcripts_before"] = tuple(
                int(row[0])
                for row in conn.execute(
                    f"""
                    SELECT id FROM conversations
                    WHERE guild_id=? AND id IN ({placeholders})
                    ORDER BY id
                    """,
                    (guild_id, *exact_ids),
                ).fetchall()
            )
            result = real_purge(
                conn,
                guild_id=guild_id,
                source_row_ids=source_row_ids,
                reason=reason,
            )
            observed["raw_after_purge"] = conn.execute(
                "SELECT COUNT(*) FROM memory_ledger_entries WHERE entry_id=?",
                (raw_entry_id,),
            ).fetchone()[0]
            observed["legacy_raw_after_purge"] = conn.execute(
                "SELECT COUNT(*) FROM memory_ledger_entries WHERE entry_id=?",
                (legacy_raw.entry_id,),
            ).fetchone()[0]
            observed["moment_after_purge"] = conn.execute(
                """
                SELECT lifecycle_status, summary, public_usable
                FROM memory_moment_windows
                WHERE moment_id=?
                """,
                (moment_id,),
            ).fetchone()
            return result

        with mock.patch(
            "bnl01_bot.purge_conversation_ledger_sources",
            side_effect=inspect_then_purge,
        ):
            bnl01_bot.prune_conversation_history(42, 1, max_rows=1)

        self.assertEqual(observed["guild_id"], 1)
        self.assertEqual(observed["source_row_ids"], (old_row_id,))
        self.assertEqual(observed["transcripts_before"], (old_row_id,))
        self.assertEqual(observed["reason"], "bounded_conversation_prune")
        self.assertEqual(observed["raw_after_purge"], 0)
        self.assertEqual(observed["legacy_raw_after_purge"], 0)
        self.assertEqual(observed["moment_after_purge"], ("retracted", "", 0))
        self.assertEqual(
            self.rows(
                "SELECT id FROM conversations WHERE guild_id=1 AND user_id=42 ORDER BY id"
            ),
            [(keep_row_id,)],
        )
        self.assertEqual(
            self.rows(
                "SELECT id FROM conversations WHERE id IN (?,?) ORDER BY id",
                (other_member_row_id, other_guild_row_id),
            ),
            [(other_member_row_id,), (other_guild_row_id,)],
        )
        self.assertEqual(
            self.rows(
                """
                SELECT fact_value, source_conversation_row_id,
                       source_ledger_entry_id, lifecycle_status
                FROM user_memory_facts
                WHERE guild_id=1 AND user_id=42 AND fact_key='favorite_color'
                """
            ),
            [("green", old_row_id, scalar_entry_id, "active")],
        )
        self.assertEqual(
            self.rows(
                """
                SELECT normalized_value, lifecycle_status
                FROM memory_ledger_entries
                WHERE entry_id=?
                """,
                (scalar_entry_id,),
            ),
            [("green", "active")],
        )
        self.assertEqual(
            self.rows(
                """
                SELECT normalized_value, lifecycle_status, public_usable
                FROM memory_ledger_entries
                WHERE entry_id=?
                """,
                (canonical_entry_id,),
            ),
            [("", "retracted", 0)],
        )
        self.assertEqual(
            self.rows(
                "SELECT COUNT(*) FROM memory_moment_members WHERE moment_id=?",
                (moment_id,),
            )[0][0],
            0,
        )

    def test_clear_user_and_guild_purge_exact_rows_before_delete_and_preserve_isolation(self):
        self.enable()
        fixtures = (
            (42, "Crow", 1, "target one"),
            (42, "Crow", 1, "target two"),
            (43, "Other", 1, "same guild survivor"),
            (42, "Crow", 2, "cross guild survivor"),
        )
        row_ids = {}
        for user_id, user_name, guild_id, content in fixtures:
            bnl01_bot.save_user_message(
                user_id,
                user_name,
                guild_id,
                content,
                channel_policy="sealed_test",
            )
            row_ids[content] = self._conversation_id(
                guild_id=guild_id,
                user_id=user_id,
                content=content,
            )

        real_purge = bnl01_bot.purge_conversation_ledger_sources
        calls = []

        def inspect_then_purge(conn, *, guild_id, source_row_ids, reason):
            exact_ids = tuple(sorted(int(row_id) for row_id in source_row_ids))
            placeholders = ",".join("?" for _ in exact_ids)
            present = tuple(
                int(row[0])
                for row in conn.execute(
                    f"""
                    SELECT id FROM conversations
                    WHERE guild_id=? AND id IN ({placeholders})
                    ORDER BY id
                    """,
                    (guild_id, *exact_ids),
                ).fetchall()
            )
            calls.append((reason, guild_id, exact_ids, present))
            return real_purge(
                conn,
                guild_id=guild_id,
                source_row_ids=source_row_ids,
                reason=reason,
            )

        with mock.patch(
            "bnl01_bot.purge_conversation_ledger_sources",
            side_effect=inspect_then_purge,
        ):
            self.assertEqual(bnl01_bot.clear_user_history(42, 1), 2)
            self.assertEqual(
                self.rows(
                    "SELECT id FROM conversations WHERE guild_id=1 ORDER BY id"
                ),
                [(row_ids["same guild survivor"],)],
            )
            self.assertEqual(
                self.rows(
                    """
                    SELECT source_row_id
                    FROM memory_ledger_entries
                    WHERE guild_id=1 AND source_table='conversations'
                      AND predicate_key='conversation'
                    ORDER BY CAST(source_row_id AS INTEGER)
                    """
                ),
                [(str(row_ids["same guild survivor"]),)],
            )
            self.assertEqual(bnl01_bot.clear_guild_history(1), 1)

        target_ids = tuple(sorted((row_ids["target one"], row_ids["target two"])))
        self.assertEqual(
            calls[0],
            ("clear_user_history", 1, target_ids, target_ids),
        )
        survivor_id = row_ids["same guild survivor"]
        self.assertEqual(
            calls[1],
            ("clear_guild_history", 1, (survivor_id,), (survivor_id,)),
        )
        cross_guild_id = row_ids["cross guild survivor"]
        self.assertEqual(
            self.rows(
                "SELECT guild_id, user_id, id FROM conversations ORDER BY id"
            ),
            [(2, 42, cross_guild_id)],
        )
        self.assertEqual(
            self.rows(
                """
                SELECT guild_id, source_row_id
                FROM memory_ledger_entries
                WHERE source_table='conversations'
                  AND predicate_key='conversation'
                ORDER BY guild_id, CAST(source_row_id AS INTEGER)
                """
            ),
            [(2, str(cross_guild_id))],
        )

    def test_prune_lifecycle_failure_rolls_back_ledger_moment_and_transcript(self):
        self.enable()
        bnl01_bot.save_user_message(
            42,
            "Crow",
            1,
            "old row must survive failed purge",
            channel_policy="sealed_test",
        )
        old_row_id = self._conversation_id(
            guild_id=1,
            user_id=42,
            content="old row must survive failed purge",
        )
        bnl01_bot.save_user_message(
            42,
            "Crow",
            1,
            "new row also survives",
            channel_policy="sealed_test",
        )
        keep_row_id = self._conversation_id(
            guild_id=1,
            user_id=42,
            content="new row also survives",
        )
        moment_id, raw_entry_id, canonical_entry_id = self._attach_test_moment(
            guild_id=1,
            source_row_id=old_row_id,
        )
        real_purge = bnl01_bot.purge_conversation_ledger_sources
        observed = {}

        def partial_then_fail(conn, *, guild_id, source_row_ids, reason):
            observed["transcript_before"] = conn.execute(
                "SELECT COUNT(*) FROM conversations WHERE guild_id=? AND id=?",
                (guild_id, old_row_id),
            ).fetchone()[0]
            real_purge(
                conn,
                guild_id=guild_id,
                source_row_ids=source_row_ids,
                reason=reason,
            )
            observed["raw_removed_inside_transaction"] = conn.execute(
                "SELECT COUNT(*) FROM memory_ledger_entries WHERE entry_id=?",
                (raw_entry_id,),
            ).fetchone()[0]
            raise RuntimeError("injected lifecycle failure")

        with mock.patch(
            "bnl01_bot.purge_conversation_ledger_sources",
            side_effect=partial_then_fail,
        ):
            bnl01_bot.prune_conversation_history(42, 1, max_rows=1)

        self.assertEqual(observed["transcript_before"], 1)
        self.assertEqual(observed["raw_removed_inside_transaction"], 0)
        self.assertEqual(
            self.rows(
                "SELECT id FROM conversations WHERE guild_id=1 AND user_id=42 ORDER BY id"
            ),
            [(old_row_id,), (keep_row_id,)],
        )
        self.assertEqual(
            self.rows(
                "SELECT normalized_value, lifecycle_status FROM memory_ledger_entries WHERE entry_id=?",
                (raw_entry_id,),
            ),
            [("old row must survive failed purge", "active")],
        )
        self.assertEqual(
            self.rows(
                """
                SELECT lifecycle_status, summary, public_usable
                FROM memory_moment_windows WHERE moment_id=?
                """,
                (moment_id,),
            ),
            [("finalized", "source-bound test moment", 1)],
        )
        self.assertEqual(
            self.rows(
                "SELECT normalized_value, lifecycle_status FROM memory_ledger_entries WHERE entry_id=?",
                (canonical_entry_id,),
            ),
            [('{"summary":"source-bound test moment"}', "active")],
        )
        self.assertEqual(
            self.rows(
                "SELECT ledger_entry_id FROM memory_moment_members WHERE moment_id=?",
                (moment_id,),
            ),
            [(raw_entry_id,)],
        )

    def test_init_db_reconciles_preexisting_raw_orphan_without_deleting_scalar_or_live_rows(self):
        self.enable()
        bnl01_bot.save_user_message(
            42,
            "Crow",
            1,
            "my favorite color is green",
            channel_name="barcode-bot",
            channel_policy="public_home",
            channel_id=10,
            message_id=101,
            directed_to_bnl=True,
        )
        orphan_source_row_id = self._conversation_id(
            guild_id=1,
            user_id=42,
            content="my favorite color is green",
        )
        bnl01_bot.save_user_message(
            43,
            "Other",
            1,
            "live same guild row",
            channel_policy="sealed_test",
        )
        live_same_guild_row_id = self._conversation_id(
            guild_id=1,
            user_id=43,
            content="live same guild row",
        )
        bnl01_bot.save_user_message(
            42,
            "Crow",
            2,
            "live cross guild row",
            channel_policy="sealed_test",
        )
        live_cross_guild_row_id = self._conversation_id(
            guild_id=2,
            user_id=42,
            content="live cross guild row",
        )
        moment_id, orphan_raw_entry_id, canonical_entry_id = (
            self._attach_test_moment(
                guild_id=1,
                source_row_id=orphan_source_row_id,
            )
        )
        scalar_entry_id = self.rows(
            """
            SELECT source_ledger_entry_id
            FROM user_memory_facts
            WHERE guild_id=1 AND user_id=42 AND fact_key='favorite_color'
            """
        )[0][0]
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            legacy_orphan = ledger.insert_ledger_entry(
                conn,
                ledger.LedgerEntry(
                    guild_id=1,
                    source_table="conversations",
                    source_row_id=orphan_source_row_id,
                    source_revision=str(orphan_source_row_id),
                    source_role="user",
                    entry_type="observation",
                    subject_key=ledger.subject_key_for_user(42),
                    predicate_key="remembered_number",
                    value="1234",
                    source_class=ledger.SourceClass.PUBLIC_OBSERVATION,
                    visibility=ledger.Visibility.PUBLIC_SAFE,
                    confidence=ledger.Confidence.MEDIUM,
                    public_usable=False,
                    observed_at="2026-07-23T00:00:00+00:00",
                ),
            )
            conn.execute(
                "DELETE FROM conversations WHERE guild_id=1 AND id=?",
                (orphan_source_row_id,),
            )
            conn.commit()
        self.assertTrue(legacy_orphan.entry_id)
        self.assertEqual(
            self.rows(
                "SELECT COUNT(*) FROM memory_ledger_entries WHERE entry_id=?",
                (orphan_raw_entry_id,),
            )[0][0],
            1,
        )
        self.assertEqual(
            self.rows(
                "SELECT COUNT(*) FROM memory_ledger_entries WHERE entry_id=?",
                (legacy_orphan.entry_id,),
            )[0][0],
            1,
        )

        bnl01_bot.init_db()

        self.assertEqual(
            self.rows(
                "SELECT COUNT(*) FROM memory_ledger_entries WHERE entry_id=?",
                (orphan_raw_entry_id,),
            )[0][0],
            0,
        )
        self.assertEqual(
            self.rows(
                "SELECT COUNT(*) FROM memory_ledger_entries WHERE entry_id=?",
                (legacy_orphan.entry_id,),
            )[0][0],
            0,
        )
        self.assertEqual(
            self.rows(
                """
                SELECT fact_value, source_conversation_row_id,
                       source_ledger_entry_id, lifecycle_status
                FROM user_memory_facts
                WHERE guild_id=1 AND user_id=42 AND fact_key='favorite_color'
                """
            ),
            [("green", orphan_source_row_id, scalar_entry_id, "active")],
        )
        self.assertEqual(
            self.rows(
                "SELECT normalized_value, lifecycle_status FROM memory_ledger_entries WHERE entry_id=?",
                (scalar_entry_id,),
            ),
            [("green", "active")],
        )
        self.assertEqual(
            self.rows(
                """
                SELECT guild_id, source_row_id
                FROM memory_ledger_entries
                WHERE source_table='conversations'
                  AND predicate_key='conversation'
                ORDER BY guild_id, CAST(source_row_id AS INTEGER)
                """
            ),
            [
                (1, str(live_same_guild_row_id)),
                (2, str(live_cross_guild_row_id)),
            ],
        )
        self.assertEqual(
            self.rows(
                """
                SELECT lifecycle_status, summary, public_usable
                FROM memory_moment_windows WHERE moment_id=?
                """,
                (moment_id,),
            ),
            [("retracted", "", 0)],
        )
        self.assertEqual(
            self.rows(
                """
                SELECT normalized_value, lifecycle_status, public_usable
                FROM memory_ledger_entries WHERE entry_id=?
                """,
                (canonical_entry_id,),
            ),
            [("", "retracted", 0)],
        )

        stable_snapshot = (
            self.rows(
                "SELECT entry_id, lifecycle_status FROM memory_ledger_entries ORDER BY entry_id"
            ),
            self.rows(
                """
                SELECT moment_id, lifecycle_status, summary
                FROM memory_moment_windows ORDER BY moment_id
                """
            ),
        )
        bnl01_bot.init_db()
        self.assertEqual(
            (
                self.rows(
                    "SELECT entry_id, lifecycle_status FROM memory_ledger_entries ORDER BY entry_id"
                ),
                self.rows(
                    """
                    SELECT moment_id, lifecycle_status, summary
                    FROM memory_moment_windows ORDER BY moment_id
                    """
                ),
            ),
            stable_snapshot,
        )

    def test_report_is_guild_scoped_and_counts_real_outcomes(self):
        self.enable()
        bnl01_bot.save_user_message(42, "Crow", 1, "remember this number: 8", channel_policy="sealed_test")
        bnl01_bot.save_user_message(43, "Other", 2, "remember this number: 9", channel_policy="sealed_test")
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            r1 = ledger.build_memory_ledger_evaluation(conn, guild_id=1)
            r2 = ledger.build_memory_ledger_evaluation(conn, guild_id=2)
        self.assertEqual(r1["eligibleLegacyWrites"], 1)
        self.assertEqual(r2["eligibleLegacyWrites"], 1)
        self.assertEqual(r1["insertedLedgerEntries"], 1)

if __name__ == "__main__":
    unittest.main()
