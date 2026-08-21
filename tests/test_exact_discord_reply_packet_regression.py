import os
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock


os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl01_bot
import bnl_memory_ledger as ledger
import bnl_moment_engine as moments
import bnl_relationship_engine as relationships
from bnl_shared_brain_synthesis import (
    begin_single_packet_run,
    build_ordinary_chat_basis,
    evaluate_single_packet_response,
    finalize_run,
    parse_ordinary_chat_response_contract,
    validate_ordinary_chat_response_contract,
)


class ExactDiscordReplyPacketRegressionTests(unittest.TestCase):
    def setUp(self):
        self.flags = {
            "BNL_CONVERSATION_CONTEXT_V2_ENABLED": "true",
            "BNL_MEMORY_LEDGER_SHADOW_ENABLED": "true",
            "BNL_MOMENT_ENGINE_SHADOW_ENABLED": "true",
            "BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED": "true",
            "BNL_RELATIONSHIP_V2_SHADOW_ENABLED": "true",
            "BNL_UNIFIED_RESPONSE_ASSESSMENT_SHADOW_ENABLED": "true",
            "BNL_UNIFIED_INTELLIGENCE_PACKET_SHADOW_ENABLED": "true",
            "BNL_SHARED_BRAIN_SYNTHESIS_CANARY_ENABLED": "false",
            "BNL_PUBLIC_HOME_BROAD_RECALL_OWNER_ENABLED": "false",
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_ENABLED": "true",
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_GUILD_IDS": "1",
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_USER_IDS": "7",
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_CHANNEL_IDS": "10",
            "BNL_MEMORY_GOVERNANCE_LIVE_ENABLED": "false",
            "BNL_RELATIONSHIP_V2_LIVE_ENABLED": "false",
            "BNL_ACTIVE_ENGAGEMENT_V2_LIVE_ENABLED": "false",
        }
        self.env = mock.patch.dict(os.environ, self.flags, clear=False)
        self.env.start()
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.db_path = str(
            Path(self.temporary_directory.name) / "exact-reply.sqlite3"
        )
        self.db_file = mock.patch.object(
            bnl01_bot,
            "DB_FILE",
            self.db_path,
        )
        self.db_file.start()

        self.now = datetime.now(timezone.utc)
        with sqlite3.connect(self.db_path) as conn:
            ledger.ensure_memory_ledger_schema(conn)
            moments.ensure_moment_schema(conn)
            relationships.ensure_relationship_v2_schema(conn)
            conn.execute(
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
                    route_mode TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    message_id INTEGER
                )
                """
            )
            conn.executemany(
                """
                INSERT INTO conversations(
                    id,guild_id,user_id,user_name,role,content,channel_id,
                    channel_name,channel_policy,route_mode,timestamp,message_id
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    (
                        100,
                        1,
                        7,
                        "Test Member A",
                        "user",
                        "My test code is cobalt",
                        10,
                        "bnl-testing",
                        "sealed_test",
                        "normal_chat",
                        (self.now - timedelta(minutes=2)).isoformat(),
                        700,
                    ),
                    (
                        101,
                        1,
                        8,
                        "Test Member B",
                        "user",
                        "My test code is amber",
                        10,
                        "bnl-testing",
                        "sealed_test",
                        "normal_chat",
                        (self.now - timedelta(minutes=1)).isoformat(),
                        701,
                    ),
                ),
            )

    def tearDown(self):
        self.db_file.stop()
        self.temporary_directory.cleanup()
        self.env.stop()

    @staticmethod
    def _addressing():
        return bnl01_bot.DiscordTurnAddressing(
            speaker="Test Member A",
            explicit_tag_recipients=("@BNL-01",),
            reply_target="Test Member A",
            explicitly_mentions_bnl=True,
            reply_targets_bnl=False,
            directly_targets_bnl=True,
            targets_other_human=False,
            plain_text_names_bnl=False,
            speaker_user_id=7,
            source_message_id=702,
            reply_message_id=700,
            reply_conversation_row_id=100,
        )

    def test_exact_reply_cobalt_is_packet_evidence_and_amber_is_not_selected(
        self,
    ):
        question = "what test code did I give you?"
        result_out = {}
        rendered_context = (
            bnl01_bot.build_conversation_context_v2_for_prompt(
                guild_id=1,
                current_user_id=7,
                channel_id=10,
                channel_name="bnl-testing",
                channel_policy="sealed_test",
                route_mode="normal_chat",
                conversation_surface="mention_or_reply",
                current_texts=(question,),
                current_participants={7},
                is_direct_target=True,
                referenced_message_ids={700},
                referenced_conversation_row_ids={100},
                now=self.now,
                route_allowed_sources={"conversation_continuity"},
                result_out=result_out,
            )
        )
        context_result = result_out["result"]

        self.assertEqual(context_result.referent_status, "resolved")
        self.assertEqual(
            context_result.referent_reason,
            "discord_reply_source",
        )
        self.assertEqual(context_result.referent_selected_row_ids, (100,))
        self.assertEqual(context_result.referent_competing_row_ids, (101,))
        self.assertEqual(context_result.selected_row_ids, (100,))
        self.assertIn("My test code is cobalt", rendered_context)
        self.assertNotIn("My test code is amber", rendered_context)

        conversation_basis = (
            bnl01_bot.build_conversation_prompt_source_basis(
                rendered_context,
                guild_id=1,
                current_user_id=7,
                channel_id=10,
                channel_name="bnl-testing",
                channel_policy="sealed_test",
                context_result=context_result,
            )
        )
        self.assertIsNotNone(conversation_basis)
        self.assertEqual(conversation_basis.source_row_ids, (100,))
        self.assertEqual(
            tuple(item.text for item in conversation_basis.evidence_items),
            ("My test code is cobalt",),
        )
        # Amber is retained only as content-bearing revalidation competition;
        # it is not selected prompt or packet evidence.
        self.assertEqual(
            tuple(
                item.text
                for item in (
                    conversation_basis.referent_competing_evidence_items
                )
            ),
            ("My test code is amber",),
        )

        orchestration = (
            bnl01_bot.build_live_conversation_orchestration_decision(
                engagement_decision="answer",
                engagement_reason="direct_request",
                channel_policy="sealed_test",
                addressings=(self._addressing(),),
                context_result=context_result,
                moment_situation=None,
                guild_id=1,
                channel_id=10,
                route_mode="normal_chat",
                conversation_surface="mention_or_reply",
                current_text=question,
                current_speaker_user_ids=(7,),
                current_speaker_labels=("Test Member A",),
                influence_mode="live",
                packet_revision="turn_exact_reply_cobalt",
            )
        )
        frame = orchestration.situation_frame
        self.assertEqual(frame.status, "resolved")
        self.assertEqual(frame.exact_source_row_ids, (100,))
        self.assertEqual(frame.tasks[0].authority_scope, "packet")
        self.assertEqual(frame.tasks[0].required_response_act, "answer")

        packet_out = {}
        assessment = bnl01_bot.build_unified_response_assessment_shadow(
            guild_id=1,
            route_mode="normal_chat",
            channel_policy="sealed_test",
            conversation_surface="mention_or_reply",
            current_text=question,
            current_speaker_user_ids=(7,),
            current_speaker_labels=("Test Member A",),
            channel_id=10,
            prompt_source_bases=(conversation_basis,),
            prompt_lanes=("current_exchange", "conversation_context"),
            continuity_required=True,
            current_direct=True,
            intelligence_packet_out=packet_out,
            situation_frame=frame,
        )
        packet = packet_out["packet"]

        self.assertIsNotNone(assessment)
        self.assertTrue(packet_out["usable"])
        self.assertTrue(
            packet.diagnostics.revalidation_status.startswith("passed")
        )
        selected_context = tuple(
            item.text
            for item in packet.items
            if item.lane == "conversation_context"
        )
        self.assertEqual(selected_context, ("My test code is cobalt",))
        self.assertNotIn(
            "My test code is amber",
            tuple(item.text for item in packet.items),
        )

        ordinary_basis = build_ordinary_chat_basis(
            guild_id=1,
            user_id=7,
            channel_id=10,
            route_mode="normal_chat",
            channel_policy="sealed_test",
            current_direct=True,
            user_text=question,
            packet=packet,
            assessment=assessment,
            environ=self.flags,
        )
        self.assertIsNotNone(ordinary_basis)
        cobalt_evidence_id = next(
            evidence_id
            for evidence_id, lane, _digest, _subjects in (
                ordinary_basis.rendered_evidence_refs
            )
            if lane == "conversation_context"
        )
        response_contract = parse_ordinary_chat_response_contract(
            '{"tasks":[{"taskId":"T1","text":"You gave me cobalt.",'
            '"supportKind":"packet","evidenceIds":["%s"]}]}'
            % cobalt_evidence_id
        )
        validation = validate_ordinary_chat_response_contract(
            ordinary_basis,
            response_contract,
        )
        self.assertTrue(validation.valid, validation.status)

        with sqlite3.connect(self.db_path) as conn:
            run = begin_single_packet_run(
                conn,
                ordinary_basis,
                prompt_ready=True,
                frame_revalidation_status="valid",
                environ=self.flags,
            )
            decision = evaluate_single_packet_response(
                conn,
                run,
                response=response_contract.response,
                response_contract=response_contract,
                typed_contract_required=True,
                provider_call_count=1,
                corrective_call_count=0,
                environ=self.flags,
            )
            self.assertTrue(decision.candidate_selected)
            self.assertTrue(
                finalize_run(
                    conn,
                    decision,
                    final_response=decision.response,
                    response_sent=True,
                    candidate_live=True,
                    guard_status="single_packet_candidate_sent",
                )
            )

            receipt_tables = (
                "memory_governance_intelligence_packet_runs",
                "memory_governance_shared_brain_synthesis_runs",
            )
            forbidden_content_columns = {
                "request_text",
                "packet_content",
                "source_text",
                "response_text",
                "baseline_response",
                "candidate_response",
                "final_response",
            }
            for table in receipt_tables:
                columns = tuple(
                    str(column[1])
                    for column in conn.execute(
                        "PRAGMA table_info(%s)" % table
                    )
                )
                self.assertFalse(
                    set(columns) & forbidden_content_columns,
                    table,
                )
                receipt_rows = conn.execute(
                    "SELECT * FROM %s" % table
                ).fetchall()
                self.assertTrue(receipt_rows, table)
                serialized_receipts = repr(receipt_rows).casefold()
                self.assertNotIn("cobalt", serialized_receipts)
                self.assertNotIn("amber", serialized_receipts)


if __name__ == "__main__":
    unittest.main()
