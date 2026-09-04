import asyncio
import os
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
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
                    timestamp TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
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

    def test_no_store_operational_reply_is_turn_local_exact_referent(self):
        reply_text = (
            "The Journal described the August 8 queue trial. "
            "The queue is closed, and the August 28 session is archived."
        )
        result_out = {}
        with sqlite3.connect(self.db_path) as conn:
            rows_before = conn.execute(
                "SELECT COUNT(*) FROM conversations"
            ).fetchone()[0]

        rendered_context = bnl01_bot.build_conversation_context_v2_for_prompt(
            guild_id=1,
            current_user_id=7,
            channel_id=10,
            channel_name="bnl-testing",
            channel_policy="sealed_test",
            route_mode="normal_chat",
            conversation_surface="mention_or_reply",
            current_texts=(
                "What part came from the Journal, and what part is the "
                "current status?",
            ),
            current_participants={7},
            is_direct_target=True,
            is_reply_to_bnl=True,
            referenced_message_ids={9001},
            transient_reply_sources=(
                bnl01_bot.TransientDiscordReplySource(
                    message_id=9001,
                    content=reply_text,
                    channel_id=10,
                ),
            ),
            now=self.now,
            route_allowed_sources={"conversation_continuity"},
            result_out=result_out,
        )
        context_result = result_out["result"]

        self.assertEqual(context_result.referent_status, "resolved")
        self.assertEqual(context_result.selected_row_ids, ())
        self.assertEqual(
            context_result.transient_referent_message_ids,
            (9001,),
        )
        self.assertIn(reply_text, rendered_context)

        basis = bnl01_bot.build_conversation_prompt_source_basis(
            rendered_context,
            guild_id=1,
            current_user_id=7,
            channel_id=10,
            channel_name="bnl-testing",
            channel_policy="sealed_test",
            context_result=context_result,
        )
        self.assertIsNotNone(basis)
        self.assertEqual(basis.source_row_ids, ())
        self.assertEqual(
            basis.revalidation_row_ids,
            tuple(
                sorted(
                    set(context_result.referent_selected_row_ids)
                    | set(context_result.referent_competing_row_ids)
                )
            ),
        )
        self.assertNotIn(9001, basis.revalidation_row_ids)
        self.assertEqual(basis.transient_referent_message_ids, (9001,))
        self.assertEqual(
            tuple(item.text for item in basis.referent_source_evidence_items),
            (reply_text,),
        )
        refreshed, changed = bnl01_bot.refresh_prompt_source_basis(basis)
        self.assertFalse(changed)
        self.assertEqual(refreshed.expected_digest, basis.expected_digest)
        self.assertTrue(
            bnl01_bot.turn_local_discord_reply_requires_no_store((basis,))
        )
        self.assertFalse(
            bnl01_bot.model_response_persistence_allowed_with_website_context(
                "What part was the current status?",
                "sealed_test",
                "",
                prompt_source_bases=(basis,),
            )
        )

        delivered = "The first sentence was the Journal summary; the second was the status portion."
        message = SimpleNamespace(
            content="What part was the current status?",
            author=SimpleNamespace(id=7, display_name="Test Member A"),
            guild=SimpleNamespace(id=1),
            channel=SimpleNamespace(id=10, name="bnl-testing"),
            reply=mock.AsyncMock(return_value=SimpleNamespace(id=9002)),
        )
        plan = bnl01_bot.plan_conversation_response(
            message.content,
            "sealed_test",
            route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
            active_channel=True,
            real_direct_target=True,
            batching_enabled=True,
            conversation_surface="mention_or_reply",
        )
        save_model = mock.Mock()
        with (
            mock.patch.object(
                bnl01_bot,
                "_apply_direct_response_pacing",
                new=mock.AsyncMock(),
            ),
            mock.patch.object(
                bnl01_bot,
                "maybe_generate_shared_brain_synthesis_canary",
                new=mock.AsyncMock(return_value=None),
            ),
            mock.patch.object(
                bnl01_bot,
                "apply_guarded_response_regeneration",
                new=mock.AsyncMock(
                    return_value=(delivered, {"suppressed": False})
                ),
            ),
            mock.patch.object(
                bnl01_bot,
                "build_message_media_context",
                return_value={"present": False},
            ),
            mock.patch.object(bnl01_bot, "update_last_route_debug"),
            mock.patch.object(
                bnl01_bot,
                "save_model_message",
                new=save_model,
            ),
            mock.patch.object(
                bnl01_bot,
                "persist_bnl_self_name_decision_after_send_async",
                new=mock.AsyncMock(),
            ),
            mock.patch.object(
                bnl01_bot,
                "record_unified_response_assessment_shadow_after_send",
                new=mock.AsyncMock(),
            ),
        ):
            decision = asyncio.run(
                bnl01_bot.send_planned_conversation_response(
                    message,
                    delivered,
                    plan,
                    prompt=rendered_context,
                    source_context_available=True,
                    prompt_source_bases=(basis,),
                    mark_recent_direct=False,
                )
            )

        message.reply.assert_awaited_once()
        save_model.assert_not_called()
        self.assertFalse(decision.save_conversation)

        with sqlite3.connect(self.db_path) as conn:
            rows_after = conn.execute(
                "SELECT COUNT(*) FROM conversations"
            ).fetchone()[0]
        self.assertEqual(rows_after, rows_before)

    def test_exact_name_echo_delivery_becomes_pronoun_reply_source(self):
        seed = (
            "reply with exactly these two names, and no other words: "
            "cache back, call'em bini"
        )
        exact_reply = bnl01_bot.parse_exact_name_echo_instruction(seed)
        self.assertEqual(exact_reply, "cache back, call'em bini")

        class ReplyMessage:
            def __init__(self):
                self.sent = []

            async def reply(self, text, **_kwargs):
                self.sent.append(text)
                return type("Sent", (), {"id": 710})()

        message = ReplyMessage()
        with (
            mock.patch.object(
                bnl01_bot,
                "calculate_adaptive_memory_limits",
                return_value={"conversation_rows": 80},
            ),
            mock.patch.object(bnl01_bot, "prune_conversation_history"),
            mock.patch.object(bnl01_bot, "update_relationship_state"),
            mock.patch.object(bnl01_bot, "maybe_add_memory_trace"),
        ):
            decision = asyncio.run(
                bnl01_bot.send_reply_then_save_model(
                    message,
                    exact_reply,
                    user_id=7,
                    guild_id=1,
                    channel_name="bnl-testing",
                    channel_policy="sealed_test",
                    channel_id=10,
                    route_mode=bnl01_bot.ROUTE_MODE_DIRECT_PAYLOAD,
                    reply_text=exact_reply,
                )
            )
        self.assertTrue(decision.save_conversation)
        self.assertEqual(message.sent, [exact_reply])

        with sqlite3.connect(self.db_path) as conn:
            saved = conn.execute(
                """
                SELECT id,role,content,message_id,route_mode
                FROM conversations
                WHERE message_id=710
                """
            ).fetchone()
        self.assertIsNotNone(saved)
        self.assertEqual(saved[1:], (
            "model",
            exact_reply,
            710,
            bnl01_bot.ROUTE_MODE_DIRECT_PAYLOAD,
        ))

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
                current_texts=(
                    "How is he connected to Call'em Bini?",
                ),
                current_participants={7},
                is_direct_target=True,
                referenced_message_ids={710},
                referenced_conversation_row_ids={saved[0]},
                now=self.now,
                route_allowed_sources={"conversation_continuity"},
                result_out=result_out,
            )
        )
        context = result_out["result"]
        self.assertEqual(context.referent_status, "resolved")
        self.assertEqual(context.referent_reason, "discord_reply_source")
        self.assertEqual(context.selected_row_ids, (saved[0],))
        self.assertIn(exact_reply, rendered_context)

        addressing = bnl01_bot.DiscordTurnAddressing(
            speaker="Test Member A",
            explicit_tag_recipients=(),
            reply_target="BNL-01",
            explicitly_mentions_bnl=False,
            reply_targets_bnl=True,
            directly_targets_bnl=True,
            targets_other_human=False,
            plain_text_names_bnl=False,
            speaker_user_id=7,
            source_message_id=711,
            reply_message_id=710,
            reply_conversation_row_id=saved[0],
        )

        for question in (
            "How is he connected to Call'em Bini?",
            "How are they related to Call'em Bini?",
        ):
            with self.subTest(question=question):
                subjects, status = (
                    bnl01_bot._exact_reply_canon_subject_references(
                        context,
                        question,
                    )
                )
                self.assertEqual(status, "resolved")
                self.assertEqual(
                    subjects,
                    (("cache_back", "Cache Back"),),
                )
                orchestration = (
                    bnl01_bot.build_live_conversation_orchestration_decision(
                        engagement_decision="answer",
                        engagement_reason="direct_request",
                        channel_policy="sealed_test",
                        addressings=(addressing,),
                        context_result=context,
                        moment_situation=None,
                        guild_id=1,
                        channel_id=10,
                        route_mode="normal_chat",
                        conversation_surface="mention_or_reply",
                        current_text=question,
                        current_speaker_user_ids=(7,),
                        current_speaker_labels=("Test Member A",),
                        influence_mode="live",
                        packet_revision="exact_name_echo_chain",
                    )
                )
                frame = orchestration.situation_frame
                self.assertEqual(frame.status, "resolved")
                subject_keys = tuple(
                    subject.entity_ref for subject in frame.subjects
                )
                self.assertEqual(
                    set(subject_keys),
                    {"cache_back", "call_em_bini"},
                )
                self.assertEqual(
                    {
                        subject_keys[index]
                        for index in frame.tasks[0].subject_indexes
                    },
                    {"cache_back", "call_em_bini"},
                )


if __name__ == "__main__":
    unittest.main()
