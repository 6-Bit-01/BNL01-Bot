import json
import os
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl01_bot
import bnl_moment_engine
from bnl_conversation_context_v2 import (
    CONVERSATION_CONTEXT_VERSION,
    ConversationContextRequest,
    ConversationContextResult,
    assemble_conversation_context_v2,
)
from bnl_memory_ledger import (
    current_bnl_self_name_records,
    ensure_memory_ledger_schema,
    record_bnl_self_name_decision,
    shadow_conversation_row,
)
from bnl_moment_engine import (
    ensure_moment_schema,
    recent_moment_situation_for_assessment,
)
from bnl_unified_response_assessment import (
    ConversationOrchestrationInput,
    coordinate_conversation_turn,
    render_conversation_orchestration_prompt,
)


NOW = datetime(2026, 7, 27, 15, 0, tzinfo=timezone.utc)


def _context_row(
    row_id,
    content,
    *,
    user_id=10,
    user_name="member",
    role="user",
    channel_id=700,
    channel_name="home",
    minutes=1,
    message_id=None,
):
    return {
        "id": row_id,
        "role": role,
        "content": content,
        "user_id": user_id,
        "user_name": user_name,
        "channel_id": channel_id,
        "channel_name": channel_name,
        "channel_policy": "public_home",
        "timestamp": (NOW - timedelta(minutes=minutes)).isoformat(),
        "message_id": message_id,
    }


def _context_request(text, **overrides):
    values = {
        "guild_id": 99,
        "current_user_id": 999,
        "channel_id": 700,
        "channel_name": "home",
        "channel_policy": "public_home",
        "route_mode": "normal_chat",
        "conversation_surface": "free_speak_public_home",
        "current_texts": (text,),
        "current_participants": frozenset({999}),
        "is_direct_target": True,
        "now": NOW,
        "route_allowed_sources": frozenset({"conversation_continuity"}),
    }
    values.update(overrides)
    return ConversationContextRequest(**values)


def _empty_context_result(**overrides):
    values = {
        "rendered_context": "",
        "selected_row_ids": (),
        "same_room_paired_turn_count": 0,
        "unpaired_row_count": 0,
        "cross_channel_paired_turn_count": 0,
        "current_message_duplicates_removed": 0,
        "visibility_policy_exclusions": 0,
        "selection_reasons": (),
        "final_char_count": 0,
        "contract_version": CONVERSATION_CONTEXT_VERSION,
    }
    values.update(overrides)
    return ConversationContextResult(**values)


def _addressing(
    *,
    bnl=False,
    state="none",
    value="",
    requires_decision=False,
    other_human=False,
):
    return bnl01_bot.DiscordTurnAddressing(
        speaker="member",
        explicit_tag_recipients=(),
        reply_target="none",
        explicitly_mentions_bnl=False,
        reply_targets_bnl=False,
        directly_targets_bnl=False,
        targets_other_human=other_human,
        plain_text_names_bnl=bnl,
        bnl_name_state=state,
        bnl_name_value=value,
        bnl_name_requires_decision=requires_decision,
        source_message_id=1234,
    )


class GovernedSelfNameTests(unittest.TestCase):
    def setUp(self):
        bnl01_bot._bnl_self_name_cache.clear()

    def tearDown(self):
        bnl01_bot._bnl_self_name_cache.clear()

    def test_unknown_vocatives_use_one_generic_proposal_path(self):
        guild = SimpleNamespace(id=10, members=[])
        with mock.patch.object(
            bnl01_bot,
            "_load_bnl_self_name_records",
            return_value=(),
        ):
            short = bnl01_bot._resolve_bnl_self_name_address(
                "Hey B, that was a great response.",
                guild=guild,
            )
            unrelated = bnl01_bot._resolve_bnl_self_name_address(
                "Hey Blue, can you look at this?",
                guild=guild,
            )
            explicit = bnl01_bot._resolve_bnl_self_name_address(
                "Can I call you Night Circuit?",
                guild=guild,
            )

        self.assertEqual(short, (True, "proposed", "B", True))
        self.assertEqual(unrelated, (True, "proposed", "Blue", True))
        self.assertEqual(
            explicit,
            (True, "proposed", "Night Circuit", True),
        )

    def test_known_human_and_interrogative_are_not_self_name_candidates(self):
        human = SimpleNamespace(
            bot=False,
            display_name="Chris",
            global_name="",
            name="chris-user",
        )
        guild = SimpleNamespace(id=10, members=[human])
        with mock.patch.object(
            bnl01_bot,
            "_load_bnl_self_name_records",
            return_value=(),
        ):
            known = bnl01_bot._resolve_bnl_self_name_address(
                "Hey Chris, what do you think?",
                guild=guild,
            )
            interrogative = bnl01_bot._resolve_bnl_self_name_address(
                "what do you think about Friday?",
                guild=SimpleNamespace(id=10, members=[]),
            )
            idiomatic = bnl01_bot._resolve_bnl_self_name_address(
                "I want to call you out on this.",
                guild=SimpleNamespace(id=10, members=[]),
            )
            temporal = bnl01_bot._resolve_bnl_self_name_address(
                "Can I call you later?",
                guild=SimpleNamespace(id=10, members=[]),
            )
            discourse = bnl01_bot._resolve_bnl_self_name_address(
                "Actually, I think Friday works.",
                guild=SimpleNamespace(id=10, members=[]),
            )

        self.assertEqual(known, (False, "other_human", "Chris", False))
        self.assertEqual(interrogative, (False, "none", "", False))
        self.assertEqual(idiomatic, (False, "none", "", False))
        self.assertEqual(temporal, (False, "none", "", False))
        self.assertEqual(discourse, (False, "none", "", False))

    def test_response_decision_parser_is_explicit_and_fail_closed(self):
        self.assertEqual(
            bnl01_bot.infer_bnl_self_name_decision(
                "Blue",
                "Yeah, people can call me Blue.",
            ),
            "accepted",
        )
        self.assertEqual(
            bnl01_bot.infer_bnl_self_name_decision(
                "Blue",
                "Don't call me Blue. Stick with BNL.",
            ),
            "denied",
        )
        self.assertEqual(
            bnl01_bot.infer_bnl_self_name_decision(
                "Blue",
                "I'm not sure about Blue yet. We'll see.",
            ),
            "deferred",
        )
        self.assertEqual(
            bnl01_bot.infer_bnl_self_name_decision(
                "Blue",
                "That was a blue-screen kind of day.",
            ),
            "",
        )

    def test_ledger_acceptance_survives_reopen_and_routes_without_literal_patch(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = os.path.join(temp_dir, "bnl.sqlite")
            with sqlite3.connect(db_path) as conn:
                ensure_memory_ledger_schema(conn)
                source = shadow_conversation_row(
                    conn,
                    row_id=1,
                    user_id=44,
                    user_name="member",
                    guild_id=77,
                    role="user",
                    content="Can I call you Blue?",
                    channel_name="home",
                    channel_policy="public_home",
                    channel_id=700,
                    message_id=8001,
                    route_mode="normal_chat",
                    observed_at=NOW.isoformat(),
                )
                self.assertEqual(source.outcome, "inserted")
                model = shadow_conversation_row(
                    conn,
                    row_id=2,
                    user_id=0,
                    user_name="BNL-01",
                    guild_id=77,
                    role="model",
                    content="People can call me Blue.",
                    channel_name="home",
                    channel_policy="public_home",
                    channel_id=700,
                    message_id=None,
                    route_mode="normal_chat",
                    observed_at=(NOW + timedelta(seconds=1)).isoformat(),
                )
                self.assertEqual(model.outcome, "inserted")
                decision = record_bnl_self_name_decision(
                    conn,
                    guild_id=77,
                    name="Blue",
                    decision="accepted",
                    source_conversation_row_id=1,
                    decision_conversation_row_id=2,
                    source_message_id=8001,
                    channel_id=700,
                    channel_name="home",
                    channel_policy="public_home",
                    route_mode="normal_chat",
                    response_digest="a" * 64,
                    observed_at=(NOW + timedelta(seconds=1)).isoformat(),
                )
                self.assertEqual(decision.outcome, "inserted")
                conn.commit()

            with (
                mock.patch.object(bnl01_bot, "DB_FILE", db_path),
                mock.patch.object(
                    bnl01_bot,
                    "memory_ledger_shadow_enabled",
                    return_value=True,
                ),
            ):
                bnl01_bot._bnl_self_name_cache.clear()
                routed = bnl01_bot._resolve_bnl_self_name_address(
                    "Blue, what do you think?",
                    guild=SimpleNamespace(id=77, members=[]),
                )

            self.assertEqual(routed, (True, "accepted", "Blue", False))

    def test_correction_supersedes_acceptance_and_reconsideration_stays_possible(self):
        with sqlite3.connect(":memory:") as conn:
            ensure_memory_ledger_schema(conn)
            for row_id, content in (
                (1, "Can I call you Blue?"),
                (2, "Are you still okay with Blue?"),
            ):
                shadow_conversation_row(
                    conn,
                    row_id=row_id,
                    user_id=44,
                    user_name="member",
                    guild_id=77,
                    role="user",
                    content=content,
                    channel_name="home",
                    channel_policy="public_home",
                    channel_id=700,
                    message_id=8000 + row_id,
                    route_mode="normal_chat",
                    observed_at=(
                        NOW + timedelta(seconds=row_id)
                    ).isoformat(),
                )
            for row_id, content in (
                (101, "People can call me Blue."),
                (102, "Don't call me Blue. Stick with BNL."),
            ):
                shadow_conversation_row(
                    conn,
                    row_id=row_id,
                    user_id=0,
                    user_name="BNL-01",
                    guild_id=77,
                    role="model",
                    content=content,
                    channel_name="home",
                    channel_policy="public_home",
                    channel_id=700,
                    message_id=None,
                    route_mode="normal_chat",
                    observed_at=(
                        NOW + timedelta(seconds=row_id)
                    ).isoformat(),
                )
            first = record_bnl_self_name_decision(
                conn,
                guild_id=77,
                name="Blue",
                decision="accepted",
                source_conversation_row_id=1,
                decision_conversation_row_id=101,
                source_message_id=8001,
                channel_id=700,
                channel_name="home",
                channel_policy="public_home",
                route_mode="normal_chat",
                response_digest="a" * 64,
                observed_at=(NOW + timedelta(seconds=3)).isoformat(),
            )
            second = record_bnl_self_name_decision(
                conn,
                guild_id=77,
                name="Blue",
                decision="denied",
                source_conversation_row_id=2,
                decision_conversation_row_id=102,
                source_message_id=8002,
                channel_id=700,
                channel_name="home",
                channel_policy="public_home",
                route_mode="normal_chat",
                response_digest="b" * 64,
                observed_at=(NOW + timedelta(seconds=4)).isoformat(),
            )
            records = current_bnl_self_name_records(conn, guild_id=77)
            supersedes = conn.execute(
                """
                SELECT COUNT(*)
                FROM memory_ledger_lineage
                WHERE entry_id=? AND lineage_type='supersedes'
                  AND target_entry_id=?
                """,
                (second.entry_id, first.entry_id),
            ).fetchone()[0]

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].decision, "denied")
        self.assertEqual(supersedes, 1)
        with mock.patch.object(
            bnl01_bot,
            "_load_bnl_self_name_records",
            return_value=records,
        ):
            simple = bnl01_bot._resolve_bnl_self_name_address(
                "Blue, answer this.",
                guild=SimpleNamespace(id=77, members=[]),
            )
            reconsider = bnl01_bot._resolve_bnl_self_name_address(
                "Can I call you Blue?",
                guild=SimpleNamespace(id=77, members=[]),
            )
        self.assertEqual(simple, (False, "denied", "Blue", False))
        self.assertEqual(reconsider, (True, "reconsideration", "Blue", True))

    def test_sent_explicit_decision_commits_through_existing_ledger_path(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = os.path.join(temp_dir, "bnl.sqlite")
            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    """
                    CREATE TABLE conversations (
                        id INTEGER PRIMARY KEY,
                        guild_id INTEGER,
                        message_id INTEGER,
                        role TEXT,
                        channel_id INTEGER,
                        content TEXT
                    )
                    """
                )
                conn.execute(
                    """
                    INSERT INTO conversations (
                        id,guild_id,message_id,role,channel_id,content
                    ) VALUES (1,77,8001,'user',700,'Can I call you Blue?')
                    """
                )
                conn.execute(
                    """
                    INSERT INTO conversations (
                        id,guild_id,message_id,role,channel_id,content
                    ) VALUES (
                        2,77,NULL,'model',700,'People can call me Blue.'
                    )
                    """
                )
                ensure_memory_ledger_schema(conn)
                shadow_conversation_row(
                    conn,
                    row_id=1,
                    user_id=44,
                    user_name="member",
                    guild_id=77,
                    role="user",
                    content="Can I call you Blue?",
                    channel_name="home",
                    channel_policy="public_home",
                    channel_id=700,
                    message_id=8001,
                    route_mode="normal_chat",
                    observed_at=NOW.isoformat(),
                )
                shadow_conversation_row(
                    conn,
                    row_id=2,
                    user_id=0,
                    user_name="BNL-01",
                    guild_id=77,
                    role="model",
                    content="People can call me Blue.",
                    channel_name="home",
                    channel_policy="public_home",
                    channel_id=700,
                    message_id=None,
                    route_mode="normal_chat",
                    observed_at=(NOW + timedelta(seconds=1)).isoformat(),
                )
                conn.commit()
            addressing = _addressing(
                bnl=True,
                state="proposed",
                value="Blue",
                requires_decision=True,
            )
            addressing = bnl01_bot.replace(
                addressing,
                source_message_id=8001,
            )
            with (
                mock.patch.object(bnl01_bot, "DB_FILE", db_path),
                mock.patch.object(
                    bnl01_bot,
                    "memory_ledger_shadow_enabled",
                    return_value=True,
                ),
            ):
                result = bnl01_bot.persist_bnl_self_name_decision_after_send(
                    guild_id=77,
                    addressing=addressing,
                    response="People can call me Blue.",
                    channel_id=700,
                    channel_name="home",
                    channel_policy="public_home",
                    route_mode="normal_chat",
                )
            with sqlite3.connect(db_path) as conn:
                records = current_bnl_self_name_records(conn, guild_id=77)
                participants = conn.execute(
                    """
                    SELECT participant_key
                    FROM memory_ledger_participants
                    WHERE entry_id=?
                    ORDER BY participant_key
                    """,
                    (result.entry_id,),
                ).fetchall()

        self.assertIsNotNone(result)
        self.assertEqual(result.outcome, "inserted")
        self.assertEqual(records[0].decision, "accepted")
        self.assertEqual(participants, [("bnl_01",)])

    def test_sealed_only_name_decision_does_not_leak_to_public_routing(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = os.path.join(temp_dir, "bnl.sqlite")
            with sqlite3.connect(db_path) as conn:
                ensure_memory_ledger_schema(conn)
                shadow_conversation_row(
                    conn,
                    row_id=1,
                    user_id=44,
                    user_name="member",
                    guild_id=77,
                    role="user",
                    content="Can I call you Test Lantern?",
                    channel_name="bnl-testing",
                    channel_policy="sealed_test",
                    channel_id=701,
                    message_id=9001,
                    route_mode="normal_chat",
                    observed_at=NOW.isoformat(),
                )
                shadow_conversation_row(
                    conn,
                    row_id=2,
                    user_id=0,
                    user_name="BNL-01",
                    guild_id=77,
                    role="model",
                    content="People can call me Test Lantern.",
                    channel_name="bnl-testing",
                    channel_policy="sealed_test",
                    channel_id=701,
                    message_id=None,
                    route_mode="normal_chat",
                    observed_at=(NOW + timedelta(seconds=1)).isoformat(),
                )
                record_bnl_self_name_decision(
                    conn,
                    guild_id=77,
                    name="Test Lantern",
                    decision="accepted",
                    source_conversation_row_id=1,
                    decision_conversation_row_id=2,
                    source_message_id=9001,
                    channel_id=701,
                    channel_name="bnl-testing",
                    channel_policy="sealed_test",
                    route_mode="normal_chat",
                    response_digest="c" * 64,
                    observed_at=(NOW + timedelta(seconds=1)).isoformat(),
                )
                conn.commit()

            with (
                mock.patch.object(bnl01_bot, "DB_FILE", db_path),
                mock.patch.object(
                    bnl01_bot,
                    "memory_ledger_shadow_enabled",
                    return_value=True,
                ),
            ):
                bnl01_bot._bnl_self_name_cache.clear()
                public = bnl01_bot._resolve_bnl_self_name_address(
                    "Test Lantern, answer this.",
                    guild=SimpleNamespace(id=77, members=[]),
                    channel_policy="public_home",
                )
                sealed = bnl01_bot._resolve_bnl_self_name_address(
                    "Test Lantern, answer this.",
                    guild=SimpleNamespace(id=77, members=[]),
                    channel_policy="sealed_test",
                )

        # Multiword names are deliberately accepted only through explicit
        # proposal grammar or a previously governed record. The public lookup
        # cannot see the sealed record; the sealed lookup can.
        self.assertEqual(public, (False, "none", "", False))
        self.assertEqual(sealed, (True, "accepted", "Test Lantern", False))

    def test_sealed_correction_does_not_supersede_public_name_state(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = os.path.join(temp_dir, "bnl.sqlite")
            with sqlite3.connect(db_path) as conn:
                ensure_memory_ledger_schema(conn)
                rows = (
                    (
                        1,
                        "user",
                        "Can I call you Blue?",
                        "public_home",
                        700,
                        9101,
                    ),
                    (
                        2,
                        "model",
                        "People can call me Blue.",
                        "public_home",
                        700,
                        None,
                    ),
                    (
                        3,
                        "user",
                        "Do you still want to be called Blue here?",
                        "sealed_test",
                        701,
                        9103,
                    ),
                    (
                        4,
                        "model",
                        "Don't call me Blue here. Stick with BNL.",
                        "sealed_test",
                        701,
                        None,
                    ),
                )
                for row_id, role, content, policy, channel_id, message_id in rows:
                    shadow_conversation_row(
                        conn,
                        row_id=row_id,
                        user_id=44 if role == "user" else 0,
                        user_name="member" if role == "user" else "BNL-01",
                        guild_id=77,
                        role=role,
                        content=content,
                        channel_name=(
                            "home"
                            if policy == "public_home"
                            else "bnl-testing"
                        ),
                        channel_policy=policy,
                        channel_id=channel_id,
                        message_id=message_id,
                        route_mode="normal_chat",
                        observed_at=(
                            NOW + timedelta(seconds=row_id)
                        ).isoformat(),
                    )
                record_bnl_self_name_decision(
                    conn,
                    guild_id=77,
                    name="Blue",
                    decision="accepted",
                    source_conversation_row_id=1,
                    decision_conversation_row_id=2,
                    source_message_id=9101,
                    channel_id=700,
                    channel_name="home",
                    channel_policy="public_home",
                    route_mode="normal_chat",
                    response_digest="d" * 64,
                    observed_at=(NOW + timedelta(seconds=2)).isoformat(),
                )
                record_bnl_self_name_decision(
                    conn,
                    guild_id=77,
                    name="Blue",
                    decision="denied",
                    source_conversation_row_id=3,
                    decision_conversation_row_id=4,
                    source_message_id=9103,
                    channel_id=701,
                    channel_name="bnl-testing",
                    channel_policy="sealed_test",
                    route_mode="normal_chat",
                    response_digest="e" * 64,
                    observed_at=(NOW + timedelta(seconds=4)).isoformat(),
                )
                conn.commit()

            with (
                mock.patch.object(bnl01_bot, "DB_FILE", db_path),
                mock.patch.object(
                    bnl01_bot,
                    "memory_ledger_shadow_enabled",
                    return_value=True,
                ),
            ):
                bnl01_bot._bnl_self_name_cache.clear()
                public = bnl01_bot._resolve_bnl_self_name_address(
                    "Blue, answer this.",
                    guild=SimpleNamespace(id=77, members=[]),
                    channel_policy="public_home",
                )
                sealed = bnl01_bot._resolve_bnl_self_name_address(
                    "Blue, answer this.",
                    guild=SimpleNamespace(id=77, members=[]),
                    channel_policy="sealed_test",
                )

        self.assertEqual(public, (True, "accepted", "Blue", False))
        self.assertEqual(sealed, (False, "denied", "Blue", False))

    def test_accepted_name_is_a_direct_surface_obligation(self):
        eligibility = bnl01_bot.decide_reply_eligibility(
            "Blue, help me with this.",
            "public_context",
            conversation_surface=bnl01_bot.CONVERSATION_SURFACE_MENTION_OR_REPLY,
            plain_text_name_seen=True,
            governed_name_obligation=True,
            batching_enabled=True,
        )
        self.assertTrue(eligibility.should_reply)
        self.assertTrue(eligibility.pacing_required)
        self.assertFalse(eligibility.batch_allowed)


class StructuralReferentTests(unittest.TestCase):
    def test_above_passage_resolves_recent_long_form_across_speakers(self):
        passage = (
            "A signal crossed the empty city and found every window awake. "
            "Nobody knew whether it was a warning, a memory, or a machine "
            "learning how to ask for company."
        )
        result = assemble_conversation_context_v2(
            [
                _context_row(
                    1,
                    passage,
                    user_id=11,
                    user_name="Mind Fanatic",
                    minutes=18.833333,
                    message_id=5001,
                )
            ],
            _context_request("BNL, analyze the above passage."),
        )

        self.assertEqual(result.referent_status, "resolved")
        self.assertEqual(result.referent_reason, "nearest_structural_contribution")
        self.assertEqual(result.referent_selected_row_ids, (1,))
        self.assertIn("Mind Fanatic", result.rendered_context)
        self.assertIn("signal crossed", result.rendered_context)

    def test_speaker_attribution_resolves_without_incidental_topic_overlap(self):
        result = assemble_conversation_context_v2(
            [
                _context_row(
                    1,
                    "Copper rain folded sideways over the orchard.",
                    user_id=11,
                    user_name="Mind Fanatic",
                    minutes=19,
                ),
                _context_row(
                    2,
                    "The compressor on my desk is noisy today.",
                    user_id=12,
                    user_name="Other Member",
                    minutes=3,
                ),
            ],
            _context_request("BNL, Mind Fanatic said it. Analyze that."),
        )

        self.assertEqual(result.referent_status, "resolved")
        self.assertEqual(result.referent_reason, "speaker_attribution")
        self.assertEqual(result.referent_selected_row_ids, (1,))
        self.assertIn("Copper rain", result.rendered_context)

    def test_dynamic_speaker_question_grammar_resolves_without_name_rules(self):
        result = assemble_conversation_context_v2(
            [
                _context_row(
                    1,
                    "Copper rain folded sideways over the orchard.",
                    user_id=11,
                    user_name="Mind Fanatic",
                    minutes=9,
                )
            ],
            _context_request("BNL, what did Mind Fanatic write?"),
        )

        self.assertEqual(result.referent_status, "resolved")
        self.assertEqual(result.referent_reason, "speaker_attribution")
        self.assertEqual(result.referent_selected_row_ids, (1,))
        self.assertIn("Copper rain", result.rendered_context)

    def test_completed_current_correction_is_not_reopened_as_ambiguous(self):
        result = assemble_conversation_context_v2(
            [
                _context_row(
                    1,
                    "Should the opener be serious or chaotic?",
                    user_id=11,
                    user_name="Member",
                    minutes=4,
                ),
                _context_row(
                    2,
                    "Serious gives structure; chaotic brings novelty.",
                    user_id=0,
                    user_name="BNL-01",
                    role="model",
                    minutes=3,
                ),
            ],
            _context_request(
                (
                    "BNL, that's not what I meant. I meant the opener video, "
                    "not me talking. Give me your actual read."
                )
            ),
        )

        self.assertEqual(result.referent_status, "not_requested")
        self.assertIn("Serious gives structure", result.rendered_context)

    def test_referent_noun_does_not_become_incidental_speaker_attribution(self):
        result = assemble_conversation_context_v2(
            [
                _context_row(
                    1,
                    (
                        "A long signal crossed the harbor after midnight and "
                        "left every receiver repeating the same unfinished "
                        "question to an empty control room."
                    ),
                    user_id=11,
                    user_name="Writer",
                    minutes=5,
                ),
                _context_row(
                    2,
                    "I saw it.",
                    user_id=12,
                    user_name="Passage",
                    minutes=1,
                ),
            ],
            _context_request("BNL, analyze the above passage."),
        )

        self.assertEqual(result.referent_status, "resolved")
        self.assertEqual(result.referent_reason, "nearest_structural_contribution")
        self.assertEqual(result.referent_selected_row_ids, (1,))
        self.assertIn("long signal", result.rendered_context)
        self.assertNotIn("I saw it", result.rendered_context)

    def test_specific_contribution_type_never_falls_back_to_unrelated_row(self):
        result = assemble_conversation_context_v2(
            [
                _context_row(
                    1,
                    "The deployment finished and the room is quiet.",
                    user_name="Operator",
                    minutes=2,
                ),
                _context_row(
                    2,
                    "Everything looks stable.",
                    user_name="BNL-01",
                    role="model",
                    minutes=1,
                ),
            ],
            _context_request("BNL, analyze the above passage."),
        )

        self.assertEqual(result.referent_status, "unresolved")
        self.assertEqual(
            result.referent_reason,
            "no_matching_contribution_type",
        )
        self.assertEqual(result.referent_selected_row_ids, ())

    def test_multiple_same_speaker_contributions_require_clarification(self):
        first = (
            "The first passage follows a brass signal through a sleeping "
            "city until the broadcast reaches an empty station."
        )
        second = (
            "The second passage follows a paper satellite through a storm "
            "until it settles over a silent orchard."
        )
        result = assemble_conversation_context_v2(
            [
                _context_row(
                    1,
                    first,
                    user_name="Mind Fanatic",
                    minutes=7,
                ),
                _context_row(
                    2,
                    second,
                    user_name="Mind Fanatic",
                    minutes=6,
                ),
            ],
            _context_request(
                "BNL, analyze the passage Mind Fanatic shared."
            ),
        )
        decision = bnl01_bot.build_live_conversation_orchestration_decision(
            engagement_decision="answer",
            engagement_reason="request",
            channel_policy="public_home",
            addressings=(
                _addressing(
                    bnl=True,
                    state="accepted",
                    value="Blue",
                ),
            ),
            context_result=result,
            moment_situation=None,
        )
        prompt = render_conversation_orchestration_prompt(decision)

        self.assertEqual(result.referent_status, "ambiguous")
        self.assertEqual(
            result.referent_reason,
            "multiple_speaker_contributions",
        )
        self.assertEqual(result.referent_candidate_count, 2)
        self.assertEqual(result.referent_selected_row_ids, ())
        self.assertEqual(decision.response_act, "clarify")
        self.assertIn("Bounded candidate count: 2", prompt)
        self.assertIn("Mind Fanatic", prompt)

    def test_resolved_long_referent_has_budget_priority_over_general_context(self):
        passage = (
            "A long-form signal moved through the sleeping city, collecting "
            "small fragments of memory from every lit window before reaching "
            "the harbor. "
        ) * 7 + "FINAL_SIGNAL_MARKER"
        rows = []
        for index in range(4):
            user_row_id = (index * 2) + 1
            rows.extend(
                (
                    _context_row(
                        user_row_id,
                        "Can we keep working through the current plan?",
                        user_id=20 + index,
                        user_name=f"Member {index}",
                        minutes=30 - (index * 2),
                    ),
                    _context_row(
                        user_row_id + 1,
                        (
                            "Yes. The prior working exchange remains bounded "
                            "conversation context. "
                        )
                        * 4,
                        user_id=0,
                        user_name="BNL-01",
                        role="model",
                        minutes=29 - (index * 2),
                    ),
                )
            )
        rows.append(
            _context_row(
                9,
                passage,
                user_id=44,
                user_name="Writer",
                minutes=18,
            )
        )

        result = assemble_conversation_context_v2(
            rows,
            _context_request("BNL, analyze the above passage."),
        )

        self.assertEqual(result.referent_status, "resolved")
        self.assertEqual(result.referent_selected_row_ids, (9,))
        self.assertEqual(result.selected_row_ids[0], 9)
        self.assertIn("FINAL_SIGNAL_MARKER", result.rendered_context)
        self.assertLessEqual(result.final_char_count, 2600)

    def test_discord_reply_identity_outranks_heuristics(self):
        result = assemble_conversation_context_v2(
            [
                _context_row(
                    1,
                    "First nearby contribution.",
                    user_name="First",
                    minutes=2,
                    message_id=4001,
                ),
                _context_row(
                    2,
                    "Second nearby contribution.",
                    user_name="Second",
                    minutes=1,
                    message_id=4002,
                ),
            ],
            _context_request(
                "BNL, what do you think about this message?",
                referenced_message_ids=frozenset({4001}),
            ),
        )

        self.assertEqual(result.referent_reason, "discord_reply_source")
        self.assertEqual(result.referent_selected_row_ids, (1,))
        self.assertIn("First nearby", result.rendered_context)
        self.assertNotIn("Second nearby", result.rendered_context)

    def test_ambiguous_reference_is_reported_instead_of_claimed_missing(self):
        result = assemble_conversation_context_v2(
            [
                _context_row(
                    1,
                    "One possible contribution about copper weather.",
                    user_name="First",
                    minutes=3,
                ),
                _context_row(
                    2,
                    "Another possible contribution about paper satellites.",
                    user_name="Second",
                    minutes=2,
                ),
            ],
            _context_request("BNL, analyze that thing."),
        )
        addressed = _addressing(
            bnl=True,
            state="accepted",
            value="Blue",
        )
        decision = bnl01_bot.build_live_conversation_orchestration_decision(
            engagement_decision="answer",
            engagement_reason="request",
            channel_policy="public_home",
            addressings=(addressed,),
            context_result=result,
            moment_situation=None,
        )
        prompt = render_conversation_orchestration_prompt(decision)

        self.assertEqual(result.referent_status, "ambiguous")
        self.assertEqual(decision.response_act, "clarify")
        self.assertIn("Bounded candidate count: 2", prompt)
        self.assertIn("First", prompt)
        self.assertIn("Second", prompt)
        self.assertIn("Do not claim", prompt)

    def test_cross_room_content_never_becomes_a_nearby_referent(self):
        result = assemble_conversation_context_v2(
            [
                _context_row(
                    1,
                    "A long passage that exists only in another channel. " * 3,
                    channel_id=701,
                    channel_name="other-room",
                    minutes=2,
                )
            ],
            _context_request("BNL, analyze the above passage."),
        )

        self.assertEqual(result.referent_status, "unresolved")
        self.assertEqual(result.referent_selected_row_ids, ())
        self.assertNotIn("another channel", result.rendered_context)


class OneMindCoordinatorTests(unittest.TestCase):
    def test_addressed_turn_cannot_be_silenced_by_engagement_classifier(self):
        decision = bnl01_bot.build_live_conversation_orchestration_decision(
            engagement_decision="observe",
            engagement_reason="no_response_needed",
            channel_policy="public_home",
            addressings=(
                _addressing(
                    bnl=True,
                    state="proposed",
                    value="B",
                    requires_decision=True,
                ),
            ),
            context_result=None,
            moment_situation=None,
        )

        self.assertEqual(decision.response_act, "answer")
        self.assertTrue(decision.response_required)
        self.assertEqual(decision.reason, "addressed_response_obligation")

    def test_route_policy_and_third_party_routing_remain_higher_authority(self):
        blocked = coordinate_conversation_turn(
            ConversationOrchestrationInput(
                route_allowed=False,
                engagement_decision="answer",
                engagement_reason="request",
                response_obligation=True,
                address_kind="discord_mention",
            )
        )
        human_only = bnl01_bot.build_live_conversation_orchestration_decision(
            engagement_decision="answer",
            engagement_reason="stale_followup",
            channel_policy="public_home",
            addressings=(_addressing(other_human=True),),
            context_result=None,
            moment_situation=None,
        )

        self.assertEqual(blocked.response_act, "blocked")
        self.assertEqual(human_only.response_act, "observe")
        self.assertEqual(human_only.reason, "third_party_only")

    def test_optional_context_and_moment_absence_do_not_veto_an_address(self):
        decision = bnl01_bot.build_live_conversation_orchestration_decision(
            engagement_decision="observe",
            engagement_reason="optional_system_unavailable",
            channel_policy="public_home",
            addressings=(
                _addressing(
                    bnl=True,
                    state="accepted",
                    value="Blue",
                ),
            ),
            context_result=None,
            moment_situation=None,
        )

        self.assertTrue(decision.should_generate)
        self.assertEqual(decision.response_act, "answer")

    def test_batch_obligation_is_derived_from_typed_addressing(self):
        addressed = SimpleNamespace(
            addressing=_addressing(
                bnl=True,
                state="accepted",
                value="Blue",
            )
        )
        ambient = SimpleNamespace(addressing=_addressing())
        self.assertTrue(bnl01_bot.batch_has_response_obligation([addressed]))
        self.assertFalse(bnl01_bot.batch_has_response_obligation([ambient]))

    def test_moment_reader_supplies_flow_state_but_does_not_force_reply(self):
        with sqlite3.connect(":memory:") as conn:
            ensure_moment_schema(conn)
            current_signature = bnl_moment_engine._topic_signature(
                "analyze the passage signal",
                "conversation",
            )
            timestamp = (NOW - timedelta(minutes=10)).isoformat()
            conn.execute(
                """
                INSERT INTO memory_moment_windows (
                    moment_id,guild_id,channel_id,channel_name,
                    channel_policy,route_mode,topic_key,topic_family,
                    topic_signature,window_started_at,last_activity_at,
                    qualification_type,qualification_reason,lifecycle_status,
                    visibility,public_usable,salience,human_entry_count,
                    model_entry_count,participant_count,summary,created_at,
                    updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    "mom_test",
                    99,
                    700,
                    "home",
                    "public_home",
                    "normal_chat",
                    "topic_test",
                    "topic_other",
                    json.dumps(current_signature),
                    timestamp,
                    timestamp,
                    "rejected",
                    "low_signal_or_insufficient_continuity",
                    "rejected",
                    "public_safe",
                    0,
                    0.1,
                    1,
                    0,
                    1,
                    "",
                    timestamp,
                    timestamp,
                ),
            )
            conn.execute(
                """
                INSERT INTO memory_moment_participants (
                    moment_id,participant_key,safe_display_name,
                    participant_role,first_seen_at,last_seen_at,
                    authored_entry_count,participation_order,created_at,
                    updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    "mom_test",
                    "discord_user:999",
                    "member",
                    "human_author",
                    timestamp,
                    timestamp,
                    1,
                    0,
                    timestamp,
                    timestamp,
                ),
            )
            conn.commit()
            with mock.patch.dict(
                os.environ,
                {
                    "BNL_MEMORY_LEDGER_SHADOW_ENABLED": "1",
                    "BNL_MOMENT_ENGINE_SHADOW_ENABLED": "1",
                },
                clear=False,
            ):
                situation = recent_moment_situation_for_assessment(
                    conn,
                    guild_id=99,
                    channel_id=700,
                    channel_policy="public_home",
                    route_mode="normal_chat",
                    topic_text="analyze the passage signal",
                    participant_keys=("discord_user:999",),
                    now=NOW.isoformat(),
                )

        self.assertIsNotNone(situation)
        self.assertEqual(situation.lifecycle_status, "rejected")
        self.assertTrue(situation.participant_overlap)
        self.assertTrue(situation.topic_coherent)
        decision = bnl01_bot.build_live_conversation_orchestration_decision(
            engagement_decision="observe",
            engagement_reason="no_response_needed",
            channel_policy="public_home",
            addressings=(),
            context_result=None,
            moment_situation=situation,
        )
        self.assertEqual(decision.response_act, "observe")
        self.assertEqual(decision.moment_human_entry_count, 1)
        self.assertEqual(decision.moment_model_entry_count, 0)
        self.assertTrue(decision.moment_participant_overlap)
        self.assertTrue(decision.moment_topic_coherent)

    def test_prompt_keeps_context_content_and_moment_flow_roles_separate(self):
        decision = coordinate_conversation_turn(
            ConversationOrchestrationInput(
                route_allowed=True,
                engagement_decision="answer",
                engagement_reason="request",
                response_obligation=True,
                address_kind="self_name_accepted",
                referent_status="resolved",
                referent_candidate_count=1,
                moment_situation_state="recent_rejected",
                moment_topic_coherent=True,
                moment_participant_overlap=True,
                moment_human_entry_count=3,
                moment_model_entry_count=1,
            )
        )
        prompt = render_conversation_orchestration_prompt(decision)

        self.assertIn("Context v2's selected raw contribution", prompt)
        self.assertIn("Moment state describes activity/flow only", prompt)
        self.assertIn("never supplies a quote", prompt)
        self.assertIn("human contributions=3", prompt)
        self.assertIn("BNL replies=1", prompt)
        self.assertIn("current participant overlap=yes", prompt)
        self.assertIn("topic coherent=yes", prompt)


if __name__ == "__main__":
    unittest.main()
