import asyncio
import json
import os
import sqlite3
import tempfile
import unittest
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl01_bot
import bnl_declared_canon as declared


class _Author:
    def __init__(self, user_id=61):
        self.id = user_id
        self.display_name = "Owner" if user_id == 61 else "Other"


class _Guild:
    def __init__(self, guild_id=7):
        self.id = guild_id


class _Channel:
    id = 700
    name = "research-and-development"


class _Message:
    def __init__(self, content, *, message_id=1001, user_id=61, guild_id=7):
        self.content = content
        self.id = message_id
        self.author = _Author(user_id)
        self.guild = _Guild(guild_id)
        self.channel = _Channel()
        self.replies = []

    async def reply(self, text, **_kwargs):
        self.replies.append(text)


class DeclaredCanonBotControlTests(unittest.TestCase):
    def setUp(self):
        self.old_db = bnl01_bot.DB_FILE
        self.old_owner = bnl01_bot.BNL_OWNER_USER_ID
        self.old_guild = bnl01_bot.BNL_PRIMARY_GUILD_ID
        self.tmp = tempfile.NamedTemporaryFile(delete=False)
        self.tmp.close()
        bnl01_bot.DB_FILE = self.tmp.name
        bnl01_bot.BNL_OWNER_USER_ID = 61
        bnl01_bot.BNL_PRIMARY_GUILD_ID = 7
        self.runtime = mock.patch.dict(
            os.environ,
            {
                "BNL_OWNER_USER_ID": "61",
                "BNL_PRIMARY_GUILD_ID": "7",
                "BNL_MEMORY_LEDGER_SHADOW_ENABLED": "1",
            },
            clear=False,
        )
        self.runtime.start()
        bnl01_bot.init_db()
        self.rd = mock.patch.object(
            bnl01_bot,
            "is_research_and_development_channel",
            return_value=True,
        )
        self.rd.start()

    def tearDown(self):
        self.rd.stop()
        self.runtime.stop()
        bnl01_bot.DB_FILE = self.old_db
        bnl01_bot.BNL_OWNER_USER_ID = self.old_owner
        bnl01_bot.BNL_PRIMARY_GUILD_ID = self.old_guild
        try:
            os.unlink(self.tmp.name)
        except OSError:
            pass

    def rows(self, sql, params=()):
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            return conn.execute(sql, params).fetchall()

    def invoke(self, command, **message_kwargs):
        message = _Message(command, **message_kwargs)
        handled = asyncio.run(
            bnl01_bot.maybe_handle_declared_canon_command(message, command)
        )
        self.assertTrue(handled)
        self.assertTrue(message.replies)
        return message

    @staticmethod
    def add_payload(summary="Owner-only role summary."):
        return {
            "subject_type": "project",
            "subject_id": "barcode_radio",
            "predicate": "current_role",
            "value": {"summary": summary},
            "raw_declaration": "PRIVATE RAW DECLARATION: " + summary,
            "cleaned_summary": summary,
            "domain": "real_community",
            "claim_kind": "role",
            "visibility": "internal",
            "eligible_routes": ["declared_canon_review"],
        }

    def test_owner_add_is_idempotent_and_emits_only_nonlive_projection(self):
        payload = self.add_payload()
        command = "!bnl canon add " + json.dumps(payload, sort_keys=True)
        first = self.invoke(command, message_id=1101)
        self.assertNotIn("PRIVATE RAW DECLARATION", first.replies[0])
        self.assertEqual(
            self.rows("SELECT COUNT(*) FROM declared_canon_revisions")[0][0],
            1,
        )
        self.assertEqual(
            self.rows(
                """
                SELECT source_class,visibility,public_usable,derived,projection,
                       lifecycle_status
                FROM memory_ledger_entries
                WHERE source_table='declared_canon_projection'
                """
            ),
            [("evidence_projection", "internal", 0, 1, 1, "review_only")],
        )
        retry = self.invoke(command, message_id=1101)
        self.assertIn("operation committed", retry.replies[0].lower())
        self.assertEqual(
            self.rows("SELECT COUNT(*) FROM declared_canon_revisions")[0][0],
            1,
        )
        self.assertEqual(
            self.rows(
                "SELECT COUNT(*) FROM memory_ledger_entries WHERE source_table='declared_canon_projection'"
            )[0][0],
            1,
        )

    def test_owner_add_reports_projection_gate_disabled_exactly(self):
        command = "!bnl canon add " + json.dumps(
            self.add_payload("Disabled shadow summary."), sort_keys=True
        )
        with mock.patch.dict(
            os.environ,
            {"BNL_MEMORY_LEDGER_SHADOW_ENABLED": "0"},
            clear=False,
        ):
            reply = self.invoke(command, message_id=1111)

        self.assertIn("Shadow projections: disabled.", reply.replies[0])
        self.assertNotIn("error:shadow_exception", reply.replies[0])
        self.assertEqual(
            self.rows("SELECT COUNT(*) FROM declared_canon_revisions")[0][0],
            1,
        )
        self.assertEqual(
            self.rows(
                "SELECT COUNT(*) FROM memory_ledger_entries "
                "WHERE source_table='declared_canon_projection'"
            )[0][0],
            0,
        )

    def test_owner_add_reports_enabled_projection_exception_after_commit(self):
        command = "!bnl canon add " + json.dumps(
            self.add_payload("Projection exception summary."), sort_keys=True
        )
        with mock.patch.object(
            bnl01_bot,
            "shadow_declared_canon_projection",
            side_effect=RuntimeError("forced declared projection failure"),
        ):
            reply = self.invoke(command, message_id=1112)

        self.assertIn(
            "Shadow projections: error:shadow_exception.", reply.replies[0]
        )
        self.assertNotIn("Shadow projections: disabled.", reply.replies[0])
        self.assertEqual(
            self.rows("SELECT COUNT(*) FROM declared_canon_revisions")[0][0],
            1,
        )
        self.assertEqual(
            self.rows(
                "SELECT COUNT(*) FROM memory_ledger_entries "
                "WHERE source_table='declared_canon_projection'"
            )[0][0],
            0,
        )
        self.assertEqual(
            self.rows(
                """
                SELECT outcome,reason_code
                FROM memory_ledger_shadow_receipts
                WHERE writer='declared_canon_projection'
                """
            ),
            [("error", "shadow_exception")],
        )

    def test_changed_nonce_reuse_and_stale_revision_fail_without_write(self):
        original_command = "!bnl canon add " + json.dumps(
            self.add_payload("Original summary."), sort_keys=True
        )
        self.invoke(original_command, message_id=1201)
        changed_command = "!bnl canon add " + json.dumps(
            self.add_payload("Changed under reused nonce."), sort_keys=True
        )
        changed = self.invoke(changed_command, message_id=1201)
        self.assertIn("authority_nonce_replay_mismatch", changed.replies[0])
        declaration_id, revision_id = self.rows(
            "SELECT declaration_id,revision_id FROM declared_canon_revisions"
        )[0]
        correction = self.add_payload("Correction with stale expectation.")
        correction.update(
            {
                "declaration_id": declaration_id,
                "expected_revision_id": "drev_00000000000000000000000000000000",
                "reason": "stale fixture",
            }
        )
        stale = self.invoke(
            "!bnl canon correct " + json.dumps(correction, sort_keys=True),
            message_id=1202,
        )
        self.assertIn("expected_revision_mismatch", stale.replies[0])
        self.assertEqual(
            self.rows(
                "SELECT declaration_id,revision_id FROM declared_canon_revisions"
            ),
            [(declaration_id, revision_id)],
        )

    def test_nonowner_and_cross_guild_controls_fail_before_write(self):
        command = "!bnl canon add " + json.dumps(self.add_payload(), sort_keys=True)
        denied = self.invoke(command, message_id=1301, user_id=62)
        self.assertIn("configured_owner_required", denied.replies[0])
        cross_guild = self.invoke(command, message_id=1302, guild_id=8)
        self.assertIn("configured_primary_guild_required", cross_guild.replies[0])
        self.assertEqual(
            self.rows("SELECT COUNT(*) FROM declared_canon_revisions")[0][0],
            0,
        )

    def test_broadcast_classification_keeps_content_in_source_and_projects_root(self):
        source_message = _Message(
            "PRIVATE RAW BROADCAST SOURCE",
            message_id=1401,
        )
        row_id = bnl01_bot.add_broadcast_memory_entry(
            7,
            source_message,
            {
                "episode_date": "2026-08-01",
                "cleaned_summary": "Clean Broadcast source summary.",
                "entry_type": "notable_moment",
                "public_safe": True,
                "usage_scope": "ambient,direct",
            },
        )
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            columns = [row[1] for row in conn.execute("PRAGMA table_info(broadcast_memory)")]
            values = conn.execute(
                "SELECT %s FROM broadcast_memory WHERE id=?" % ",".join(columns),
                (row_id,),
            ).fetchone()
        fingerprint = declared.broadcast_source_fingerprint(dict(zip(columns, values)))
        payload = {
            "broadcast_row_id": row_id,
            "expected_source_fingerprint": fingerprint,
            "subject_type": "broadcast",
            "subject_id": "barcode_radio",
            "visibility": "internal",
            "eligible_routes": ["broadcast_memory"],
        }
        reply = self.invoke(
            "!bnl canon broadcast-classify " + json.dumps(payload, sort_keys=True),
            message_id=1402,
        )
        self.assertNotIn("PRIVATE RAW", reply.replies[0])
        self.assertEqual(
            self.rows(
                """
                SELECT raw_declaration,cleaned_summary,value_json
                FROM declared_canon_revisions
                """
            ),
            [("", "", "")],
        )
        projection = self.rows(
            """
            SELECT entry_id,normalized_value,public_usable,lifecycle_status
            FROM memory_ledger_entries
            WHERE source_table='declared_canon_projection'
            """
        )
        self.assertEqual(
            projection[0][1:],
            ("Clean Broadcast source summary.", 0, "review_only"),
        )
        root_lineage = self.rows(
            "SELECT lineage_type FROM memory_ledger_lineage WHERE entry_id=?",
            (projection[0][0],),
        )
        self.assertEqual(root_lineage, [("derived_from",)])

    def test_previews_are_zero_write_and_do_not_echo_unrecognized_metadata(self):
        bnl01_bot.add_broadcast_memory_entry(
            7,
            _Message("PRIVATE PREVIEW SOURCE", message_id=1501),
            {
                "episode_date": "2026-08-01",
                "cleaned_summary": "Preview source summary.",
                "entry_type": "person_123456789_private_label",
                "public_safe": False,
                "usage_scope": "account_987654321,internal",
            },
        )
        before = self.rows("SELECT COUNT(*) FROM declared_canon_revisions")[0][0]
        preview = self.invoke("!bnl canon broadcast-preview", message_id=1502)
        rendered = preview.replies[0]
        self.assertNotIn("person_123456789_private_label", rendered)
        self.assertNotIn("account_987654321", rendered)
        self.assertNotIn("PRIVATE PREVIEW SOURCE", rendered)
        self.assertIn("legacy_or_unrecognized", rendered)
        self.assertIn("counts_scope=returned_page", rendered)
        self.assertEqual(
            self.rows("SELECT COUNT(*) FROM declared_canon_revisions")[0][0],
            before,
        )

    def test_on_message_preserves_raw_control_before_any_conversational_sink(self):
        raw_declaration = "Owner keeps <@123>  and  exact spacing."
        payload = self.add_payload()
        payload["raw_declaration"] = raw_declaration
        command = "!bnl canon add " + json.dumps(payload)
        message = _Message(command, message_id=1601)
        with (
            mock.patch.object(
                bnl01_bot,
                "_register_direct_conversation_ingress",
            ) as ingress,
            mock.patch.object(bnl01_bot, "upsert_user_profile") as profile,
            mock.patch.object(
                bnl01_bot, "resolve_discord_user_mentions_for_conversation"
            ) as normalize,
            mock.patch.object(
                bnl01_bot,
                "resolve_discord_turn_addressing_async",
                new=mock.AsyncMock(),
            ) as addressing,
            mock.patch.object(
                bnl01_bot,
                "record_recent_room_event_from_message",
            ) as recent_room,
            mock.patch.object(
                bnl01_bot,
                "maybe_handle_provider_status_command",
                new=mock.AsyncMock(),
            ) as provider,
        ):
            asyncio.run(bnl01_bot.on_message(message))

        self.assertEqual(
            self.rows("SELECT raw_declaration FROM declared_canon_revisions"),
            [(raw_declaration,)],
        )
        ingress.assert_not_called()
        profile.assert_not_called()
        normalize.assert_not_called()
        addressing.assert_not_awaited()
        recent_room.assert_not_called()
        provider.assert_not_awaited()

    def test_on_message_denied_wrong_channel_and_malformed_controls_never_sink(self):
        cases = (
            (_Message("!bnl canon preview", message_id=1611, user_id=62), True),
            (_Message("!bnl canon preview", message_id=1612), False),
            (_Message("!bnl canon add {broken-json", message_id=1613), True),
        )
        for message, rd_allowed in cases:
            with self.subTest(message_id=message.id):
                with (
                    mock.patch.object(
                        bnl01_bot,
                        "is_research_and_development_channel",
                        return_value=rd_allowed,
                    ),
                    mock.patch.object(
                        bnl01_bot, "_register_direct_conversation_ingress"
                    ) as ingress,
                    mock.patch.object(
                        bnl01_bot, "upsert_user_profile"
                    ) as profile,
                    mock.patch.object(
                        bnl01_bot,
                        "resolve_discord_user_mentions_for_conversation",
                    ) as normalize,
                    mock.patch.object(
                        bnl01_bot,
                        "resolve_discord_turn_addressing_async",
                        new=mock.AsyncMock(),
                    ) as addressing,
                    mock.patch.object(
                        bnl01_bot, "record_recent_room_event_from_message"
                    ) as recent_room,
                    mock.patch.object(
                        bnl01_bot,
                        "maybe_handle_provider_status_command",
                        new=mock.AsyncMock(),
                    ) as provider,
                ):
                    asyncio.run(bnl01_bot.on_message(message))

                self.assertTrue(message.replies)
                ingress.assert_not_called()
                profile.assert_not_called()
                normalize.assert_not_called()
                addressing.assert_not_awaited()
                recent_room.assert_not_called()
                provider.assert_not_awaited()

        self.assertEqual(
            self.rows("SELECT COUNT(*) FROM declared_canon_revisions")[0][0],
            0,
        )


if __name__ == "__main__":
    unittest.main()
