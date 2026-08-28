import asyncio
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
    assess_reply_referent_grounding,
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


def _seed_conversation_source_rows(conn, *rows):
    """Create the source rows that governed Ledger decisions must retain."""

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS conversations (
            id INTEGER PRIMARY KEY,
            guild_id INTEGER,
            role TEXT,
            channel_id INTEGER,
            channel_policy TEXT,
            content TEXT
        )
        """
    )
    conn.executemany(
        """
        INSERT INTO conversations (
            id,guild_id,role,channel_id,channel_policy,content
        ) VALUES (?,?,?,?,?,?)
        """,
        rows,
    )


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
        bnl_name_influence_mode="live",
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

    def test_punctuation_only_discourse_fails_closed_by_structure(self):
        cases = (
            "Personally, I think Friday works.",
            "Apparently, the queue is moving.",
            "Technically, that is correct.",
            "Unfortunately, it failed.",
            "Honestly, I am not sure.",
            "Meanwhile, the room kept moving.",
            "However, the queue kept moving.",
            "But, I think Friday works.",
            "Maybe, Friday still works.",
            "In general, this seems fine.",
            "In the room, the queue is moving.",
            "HERE, the queue is moving.",
            "Today, I finished the change.",
            "I think Friday works, personally.",
            "The queue is moving, apparently.",
            "The queue is moving, in general.",
            "Actually，I think Friday works.",
        )
        guild = SimpleNamespace(id=10, members=[])
        with mock.patch.object(
            bnl01_bot,
            "_load_bnl_self_name_records",
            return_value=(),
        ):
            baseline = {
                text: bnl01_bot.classify_bnl_self_name_request(text)
                for text in cases
            }
            for text, request in baseline.items():
                with self.subTest(text=text):
                    self.assertEqual(request.action, "none")
                    self.assertEqual(request.evidence_kind, "")
                    self.assertEqual(
                        bnl01_bot._resolve_bnl_self_name_address(
                            text,
                            guild=guild,
                        ),
                        (False, "none", "", False),
                    )
            with mock.patch.object(
                bnl01_bot,
                "_SELF_NAME_STOPWORDS",
                frozenset(),
            ):
                for text, original in baseline.items():
                    with self.subTest(text=text, stopwords="removed"):
                        changed = (
                            bnl01_bot.classify_bnl_self_name_request(text)
                        )
                        self.assertEqual(changed.action, "none")
                        self.assertEqual(
                            changed.classification,
                            original.classification,
                        )

        addressing = _addressing(bnl=False, state="none")
        decision = bnl01_bot.build_live_conversation_orchestration_decision(
            engagement_decision="observe",
            engagement_reason="no_response_needed",
            channel_policy="public_home",
            addressings=(addressing,),
            context_result=None,
            moment_situation=None,
            influence_mode="live",
        )
        self.assertFalse(decision.response_required)
        self.assertEqual(decision.response_act, "observe")

    def test_positive_vocative_grammar_requires_independent_targeting(self):
        unsupported = bnl01_bot.classify_bnl_self_name_request(
            "Blue, can you look at this?"
        )
        supported = bnl01_bot.classify_bnl_self_name_request(
            "Blue, can you look at this?",
            independent_bnl_target=True,
        )
        trailing = bnl01_bot.classify_bnl_self_name_request(
            "What do you think, Blue?",
            independent_bnl_target=True,
        )
        discourse_reply = bnl01_bot.classify_bnl_self_name_request(
            "Personally, I think Friday works.",
            independent_bnl_target=True,
        )

        self.assertEqual(unsupported.action, "none")
        self.assertTrue(unsupported.ambiguous)
        self.assertEqual(
            (supported.action, supported.name, supported.evidence_kind),
            ("propose", "Blue", "target_supported_bare_vocative"),
        )
        self.assertEqual(
            (trailing.action, trailing.name, trailing.evidence_kind),
            ("propose", "Blue", "target_supported_bare_vocative"),
        )
        self.assertEqual(discourse_reply.action, "none")
        self.assertEqual(
            discourse_reply.classification,
            "discourse_modifier",
        )

        human = SimpleNamespace(
            bot=False,
            display_name="Chris",
            global_name="",
            name="chris-user",
        )
        with mock.patch.object(
            bnl01_bot,
            "_load_bnl_self_name_records",
            return_value=(),
        ):
            self.assertEqual(
                bnl01_bot._resolve_bnl_self_name_address(
                    "What do you think, Chris?",
                    guild=SimpleNamespace(id=10, members=[human]),
                ),
                (False, "other_human", "Chris", False),
            )
            self.assertEqual(
                bnl01_bot._resolve_bnl_self_name_address(
                    "Hey Blue, can you look at this?",
                    guild=SimpleNamespace(id=10, members=[]),
                    targets_other_human=True,
                ),
                (False, "ambiguous", "", False),
            )
            self.assertEqual(
                bnl01_bot._resolve_bnl_self_name_address(
                    "Blue, can you look at this?",
                    guild=SimpleNamespace(id=10, members=[]),
                    independent_bnl_target=True,
                    targets_other_human=True,
                ),
                (False, "ambiguous", "", False),
            )

    def test_discord_reply_support_is_carried_into_typed_name_addressing(self):
        author = SimpleNamespace(
            id=44,
            bot=False,
            display_name="Member",
            global_name="",
            name="member",
        )
        guild = SimpleNamespace(id=10, members=[author])
        channel = SimpleNamespace(id=700, name="home")
        message = SimpleNamespace(
            id=8001,
            author=author,
            guild=guild,
            channel=channel,
            content="Blue, can you look at this?",
            raw_mentions=[],
            mentions=[],
            reference=None,
        )
        with (
            mock.patch.object(
                bnl01_bot,
                "resolve_channel_policy",
                return_value="public_home",
            ),
            mock.patch.object(
                bnl01_bot,
                "conversation_orchestration_influence_mode",
                return_value="live",
            ),
            mock.patch.object(
                bnl01_bot,
                "_load_bnl_self_name_records",
                return_value=(),
            ),
            mock.patch.object(
                bnl01_bot,
                "_conversation_row_for_discord_message",
                return_value=(0, "", ""),
            ),
        ):
            unsupported = bnl01_bot.resolve_discord_turn_addressing(
                message,
                direct_to_bnl=False,
                reply_to_bnl=False,
            )
            generic_direct = bnl01_bot.resolve_discord_turn_addressing(
                message,
                direct_to_bnl=True,
                reply_to_bnl=False,
            )
            reply_supported = bnl01_bot.resolve_discord_turn_addressing(
                message,
                direct_to_bnl=True,
                reply_to_bnl=True,
            )

        self.assertFalse(unsupported.plain_text_names_bnl)
        self.assertEqual(unsupported.bnl_name_state, "none")
        self.assertFalse(unsupported.bnl_name_requires_decision)
        self.assertEqual(generic_direct.bnl_name_state, "none")
        self.assertFalse(generic_direct.bnl_name_requires_decision)
        self.assertTrue(reply_supported.plain_text_names_bnl)
        self.assertEqual(reply_supported.bnl_name_state, "proposed")
        self.assertTrue(reply_supported.bnl_name_requires_decision)
        self.assertEqual(
            reply_supported.bnl_name_evidence_kind,
            "target_supported_bare_vocative",
        )
        self.assertEqual(
            reply_supported.bnl_name_validation_version,
            bnl01_bot.BNL_SELF_NAME_VALIDATION_VERSION,
        )

    def test_plain_text_human_vocatives_preserve_typed_ambiguity(self):
        author = SimpleNamespace(
            id=44,
            bot=False,
            display_name="Member",
            global_name="",
            name="member",
        )
        human = SimpleNamespace(
            id=55,
            bot=False,
            display_name="Chris",
            global_name="",
            name="chris-user",
        )
        guild = SimpleNamespace(id=10, members=[author, human])
        channel = SimpleNamespace(id=700, name="home")

        def message(content, message_id):
            return SimpleNamespace(
                id=message_id,
                author=author,
                guild=guild,
                channel=channel,
                content=content,
                raw_mentions=[],
                mentions=[],
                reference=None,
            )

        with (
            mock.patch.object(
                bnl01_bot,
                "resolve_channel_policy",
                return_value="public_home",
            ),
            mock.patch.object(
                bnl01_bot,
                "conversation_orchestration_influence_mode",
                return_value="live",
            ),
            mock.patch.object(
                bnl01_bot,
                "_load_bnl_self_name_records",
                return_value=(),
            ),
            mock.patch.object(
                bnl01_bot,
                "_conversation_row_for_discord_message",
                return_value=(0, "", ""),
            ),
        ):
            human_only = bnl01_bot.resolve_discord_turn_addressing(
                message("Hey Chris, what do you think?", 8003),
                direct_to_bnl=False,
                reply_to_bnl=False,
            )
            mixed = bnl01_bot.resolve_discord_turn_addressing(
                message("Hey Blue, Chris, can you check this?", 8004),
                direct_to_bnl=False,
                reply_to_bnl=False,
            )

        self.assertTrue(human_only.targets_other_human)
        self.assertEqual(human_only.bnl_name_state, "other_human")
        self.assertEqual(
            human_only.bnl_name_classification,
            "another_human_address",
        )
        self.assertFalse(human_only.addresses_bnl)
        self.assertTrue(mixed.targets_other_human)
        self.assertEqual(mixed.bnl_name_state, "ambiguous")
        self.assertEqual(
            mixed.bnl_name_classification,
            "mixed_human_ambiguous",
        )
        self.assertEqual(mixed.bnl_name_action, "none")
        self.assertFalse(mixed.bnl_name_requires_decision)
        self.assertFalse(mixed.addresses_bnl)

    def test_accepted_unicode_multiword_name_routes_in_vocative_positions(self):
        record = bnl01_bot.BnlSelfNameRecord(
            normalized_name="módem azul",
            display_name="Módem Azul",
            decision="accepted",
            entry_id="accepted-name-entry",
            validation_version=(
                bnl01_bot.BNL_SELF_NAME_VALIDATION_VERSION
            ),
            evidence_kind="explicit_proposal",
            routing_eligible=True,
            validation_basis="recorded_current_grammar",
            quarantine_reason="",
        )
        guild = SimpleNamespace(id=10, members=[])
        cases = (
            "Hey MÓDEM AZUL—can you check this?",
            "Módem Azul: can you check this?",
            "Could you, Módem Azul, check this?",
            "What do you think, Módem Azul?",
        )
        with mock.patch.object(
            bnl01_bot,
            "_load_bnl_self_name_records",
            return_value=(record,),
        ):
            for text in cases:
                with self.subTest(text=text):
                    self.assertEqual(
                        bnl01_bot._resolve_bnl_self_name_address(
                            text,
                            guild=guild,
                        ),
                        (True, "accepted", "Módem Azul", False),
                    )

            author = SimpleNamespace(
                id=44,
                bot=False,
                display_name="Member",
                global_name="",
                name="member",
            )
            message = SimpleNamespace(
                id=8002,
                author=author,
                guild=guild,
                channel=SimpleNamespace(id=700, name="home"),
                content="Could you, Módem Azul, check this?",
                raw_mentions=[],
                mentions=[],
                reference=None,
            )
            with (
                mock.patch.object(
                    bnl01_bot,
                    "resolve_channel_policy",
                    return_value="public_home",
                ),
                mock.patch.object(
                    bnl01_bot,
                    "conversation_orchestration_influence_mode",
                    return_value="live",
                ),
                mock.patch.object(
                    bnl01_bot,
                    "_conversation_row_for_discord_message",
                    return_value=(0, "", ""),
                ),
            ):
                addressing = bnl01_bot.resolve_discord_turn_addressing(
                    message,
                    direct_to_bnl=False,
                    reply_to_bnl=False,
                )

        self.assertEqual(addressing.bnl_name_state, "accepted")
        self.assertEqual(
            addressing.bnl_name_classification,
            "accepted_governed_vocative",
        )
        self.assertEqual(addressing.bnl_name_action, "none")
        self.assertFalse(addressing.bnl_name_requires_decision)

    def test_explicit_and_greeting_grammar_is_unicode_and_quote_safe(self):
        explicit_cases = {
            "Can I call you Actually?": "Actually",
            "BNL, can I call you Módem Azul?": "Módem Azul",
            "Can I call you O’Clock-7?": "O’Clock-7",
            "Your nickname should be Test Lantern.": "Test Lantern",
        }
        for text, name in explicit_cases.items():
            with self.subTest(text=text):
                request = bnl01_bot.classify_bnl_self_name_request(text)
                self.assertEqual(
                    (request.action, request.name, request.evidence_kind),
                    ("propose", name, "explicit_proposal"),
                )

        greeting = bnl01_bot.classify_bnl_self_name_request(
            "Hey Módem Azul, can you check this? 👋"
        )
        self.assertEqual(
            (greeting.action, greeting.name, greeting.evidence_kind),
            ("propose", "Módem Azul", "strong_greeting_vocative"),
        )

        queue_question = bnl01_bot.classify_bnl_self_name_request(
            "Hey is the queue open?"
        )
        self.assertEqual(queue_question.action, "none")
        self.assertEqual(queue_question.evidence_kind, "")

        for text in (
            'She asked, "Can I call you Blue?"',
            '"Can I call you Blue?"',
            "`Can I call you Blue?` is the example.",
            "The phrase 'your name is Blue' appears in the draft.",
            "Can I call you Blue/Red?",
            "Can I call you " + ("A" * 60) + "?",
        ):
            with self.subTest(text=text):
                request = bnl01_bot.classify_bnl_self_name_request(text)
                self.assertEqual(request.action, "none")
                self.assertEqual(request.evidence_kind, "")

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
                _seed_conversation_source_rows(
                    conn,
                    (
                        1,
                        77,
                        "user",
                        700,
                        "public_home",
                        "Can I call you Blue?",
                    ),
                    (
                        2,
                        77,
                        "model",
                        700,
                        "public_home",
                        "People can call me Blue.",
                    ),
                )
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
                mock.patch.dict(
                    os.environ,
                    {
                        "BNL_CONVERSATION_ORCHESTRATION_INFLUENCE_ENABLED": "1"
                    },
                    clear=False,
                ),
            ):
                bnl01_bot._bnl_self_name_cache.clear()
                routed = bnl01_bot._resolve_bnl_self_name_address(
                    "Blue, what do you think?",
                    guild=SimpleNamespace(id=77, members=[]),
                )

            self.assertEqual(routed, (True, "accepted", "Blue", False))

    def test_historical_weak_grammar_is_reported_and_quarantined_read_only(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = os.path.join(temp_dir, "bnl.sqlite")
            with sqlite3.connect(db_path) as conn:
                ensure_memory_ledger_schema(conn)
                _seed_conversation_source_rows(
                    conn,
                    (
                        1,
                        77,
                        "user",
                        700,
                        "public_home",
                        "Can I call you Blue?",
                    ),
                    (
                        2,
                        77,
                        "model",
                        700,
                        "public_home",
                        "People can call me Blue.",
                    ),
                    (
                        3,
                        77,
                        "user",
                        700,
                        "public_home",
                        "Personally, I think Friday works.",
                    ),
                    (
                        4,
                        77,
                        "model",
                        700,
                        "public_home",
                        "People can call me Personally.",
                    ),
                )
                rows = (
                    (1, "user", "Can I call you Blue?", 8001),
                    (2, "model", "People can call me Blue.", 9001),
                    (
                        3,
                        "user",
                        "Personally, I think Friday works.",
                        8003,
                    ),
                    (
                        4,
                        "model",
                        "People can call me Personally.",
                        9004,
                    ),
                )
                for row_id, role, content, message_id in rows:
                    shadow_conversation_row(
                        conn,
                        row_id=row_id,
                        user_id=44 if role == "user" else 0,
                        user_name=(
                            "member" if role == "user" else "BNL-01"
                        ),
                        guild_id=77,
                        role=role,
                        content=content,
                        channel_name="home",
                        channel_policy="public_home",
                        channel_id=700,
                        message_id=message_id,
                        route_mode="normal_chat",
                        observed_at=(
                            NOW + timedelta(seconds=row_id)
                        ).isoformat(),
                    )
                for (
                    name,
                    source_row,
                    decision_row,
                    source_message,
                    digest,
                ) in (
                    ("Blue", 1, 2, 8001, "a" * 64),
                    ("Personally", 3, 4, 8003, "b" * 64),
                ):
                    record_bnl_self_name_decision(
                        conn,
                        guild_id=77,
                        name=name,
                        decision="accepted",
                        source_conversation_row_id=source_row,
                        decision_conversation_row_id=decision_row,
                        source_message_id=source_message,
                        channel_id=700,
                        channel_name="home",
                        channel_policy="public_home",
                        route_mode="normal_chat",
                        response_digest=digest,
                        observed_at=(
                            NOW + timedelta(seconds=decision_row)
                        ).isoformat(),
                    )
                conn.commit()
                before_count = conn.execute(
                    "SELECT COUNT(*) FROM memory_ledger_entries"
                ).fetchone()[0]
                report = bnl01_bot.build_bnl_self_name_validation_report(
                    conn,
                    guild_id=77,
                    channel_policies=("public_home",),
                )
                after_count = conn.execute(
                    "SELECT COUNT(*) FROM memory_ledger_entries"
                ).fetchone()[0]

            self.assertEqual(before_count, after_count)
            self.assertEqual(report["activeDecisionCount"], 2)
            self.assertEqual(report["routingEligibleCount"], 1)
            self.assertEqual(report["historicalRevalidatedCount"], 1)
            self.assertEqual(report["quarantinedCount"], 1)
            self.assertEqual(report["mutationCount"], 0)
            self.assertEqual(
                report["quarantineReasons"],
                {"weak_or_ambiguous_historical_grammar": 1},
            )
            rendered_report = json.dumps(report, sort_keys=True)
            self.assertNotIn("Blue", rendered_report)
            self.assertNotIn("Personally", rendered_report)

            with (
                mock.patch.object(bnl01_bot, "DB_FILE", db_path),
                mock.patch.object(
                    bnl01_bot,
                    "memory_ledger_shadow_enabled",
                    return_value=True,
                ),
            ):
                records = bnl01_bot._load_bnl_self_name_records(
                    77,
                    "public_home",
                )
            self.assertEqual(len(records), 1)
            self.assertEqual(records[0].normalized_name, "blue")
            self.assertEqual(
                records[0].validation_basis,
                "historical_positive_revalidation",
            )

            greeting_record = bnl01_bot.BnlSelfNameRecord(
                normalized_name="beacon",
                display_name="Beacon",
                decision="accepted",
                entry_id="legacy-greeting-entry",
            )
            self.assertEqual(
                bnl01_bot._historical_self_name_request_supports_record(
                    "Hey Beacon, can you check this?",
                    greeting_record,
                ),
                (False, "weak_or_ambiguous_historical_grammar"),
            )

    def test_punctuation_only_source_cannot_persist_name_authority(self):
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
                conn.executemany(
                    """
                    INSERT INTO conversations (
                        id,guild_id,message_id,role,channel_id,content
                    ) VALUES (?,?,?,?,?,?)
                    """,
                    (
                        (
                            1,
                            77,
                            8001,
                            "user",
                            700,
                            "Personally, I think Friday works.",
                        ),
                        (
                            2,
                            77,
                            None,
                            "model",
                            700,
                            "People can call me Personally.",
                        ),
                    ),
                )
                ensure_memory_ledger_schema(conn)
                for row_id, role, content, message_id in (
                    (
                        1,
                        "user",
                        "Personally, I think Friday works.",
                        8001,
                    ),
                    (
                        2,
                        "model",
                        "People can call me Personally.",
                        None,
                    ),
                ):
                    shadow_conversation_row(
                        conn,
                        row_id=row_id,
                        user_id=44 if role == "user" else 0,
                        user_name=(
                            "member" if role == "user" else "BNL-01"
                        ),
                        guild_id=77,
                        role=role,
                        content=content,
                        channel_name="home",
                        channel_policy="public_home",
                        channel_id=700,
                        message_id=message_id,
                        route_mode="normal_chat",
                        observed_at=NOW.isoformat(),
                    )
                conn.commit()

            addressing = _addressing(
                bnl=True,
                state="proposed",
                value="Personally",
                requires_decision=True,
            )
            addressing = bnl01_bot.replace(
                addressing,
                source_message_id=8001,
                bnl_name_action="propose",
                bnl_name_classification="legacy_bare_comma",
                bnl_name_evidence_kind="",
            )
            with (
                mock.patch.object(bnl01_bot, "DB_FILE", db_path),
                mock.patch.object(
                    bnl01_bot,
                    "memory_ledger_shadow_enabled",
                    return_value=True,
                ),
                mock.patch.dict(
                    os.environ,
                    {
                        "BNL_CONVERSATION_ORCHESTRATION_INFLUENCE_ENABLED": "1"
                    },
                    clear=False,
                ),
            ):
                result = (
                    bnl01_bot.persist_bnl_self_name_decision_after_send(
                        guild_id=77,
                        addressing=addressing,
                        response="People can call me Personally.",
                        channel_id=700,
                        channel_name="home",
                        channel_policy="public_home",
                        route_mode="normal_chat",
                    )
                )
            self.assertIsNone(result)
            with sqlite3.connect(db_path) as conn:
                self.assertEqual(
                    conn.execute(
                        """
                        SELECT COUNT(*) FROM memory_ledger_entries
                        WHERE source_table='bnl_self_name_decisions'
                        """
                    ).fetchone()[0],
                    0,
                )

    def test_correction_supersedes_acceptance_and_reconsideration_stays_possible(self):
        with sqlite3.connect(":memory:") as conn:
            ensure_memory_ledger_schema(conn)
            _seed_conversation_source_rows(
                conn,
                (
                    1,
                    77,
                    "user",
                    700,
                    "public_home",
                    "Can I call you Blue?",
                ),
                (
                    2,
                    77,
                    "user",
                    700,
                    "public_home",
                    "Are you still okay with Blue?",
                ),
                (
                    101,
                    77,
                    "model",
                    700,
                    "public_home",
                    "People can call me Blue.",
                ),
                (
                    102,
                    77,
                    "model",
                    700,
                    "public_home",
                    "Don't call me Blue. Stick with BNL.",
                ),
            )
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
                mock.patch.dict(
                    os.environ,
                    {
                        "BNL_CONVERSATION_ORCHESTRATION_INFLUENCE_ENABLED": "1"
                    },
                    clear=False,
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
        self.assertTrue(records[0].routing_eligible)
        self.assertEqual(
            records[0].validation_version,
            bnl01_bot.BNL_SELF_NAME_VALIDATION_VERSION,
        )
        self.assertEqual(records[0].evidence_kind, "explicit_proposal")
        self.assertEqual(participants, [("bnl_01",)])

    def test_sealed_only_name_decision_does_not_leak_to_public_routing(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = os.path.join(temp_dir, "bnl.sqlite")
            with sqlite3.connect(db_path) as conn:
                ensure_memory_ledger_schema(conn)
                _seed_conversation_source_rows(
                    conn,
                    (
                        1,
                        77,
                        "user",
                        701,
                        "sealed_test",
                        "Can I call you Test Lantern?",
                    ),
                    (
                        2,
                        77,
                        "model",
                        701,
                        "sealed_test",
                        "People can call me Test Lantern.",
                    ),
                )
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
                _seed_conversation_source_rows(
                    conn,
                    (
                        1,
                        77,
                        "user",
                        700,
                        "public_home",
                        "Can I call you Blue?",
                    ),
                    (
                        2,
                        77,
                        "model",
                        700,
                        "public_home",
                        "People can call me Blue.",
                    ),
                    (
                        3,
                        77,
                        "user",
                        701,
                        "sealed_test",
                        "Do you still want to be called Blue here?",
                    ),
                    (
                        4,
                        77,
                        "model",
                        701,
                        "sealed_test",
                        "Don't call me Blue here. Stick with BNL.",
                    ),
                )
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
            influence_mode="live",
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
            influence_mode="live",
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
            influence_mode="live",
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
            influence_mode="live",
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
            influence_mode="live",
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
            influence_mode="live",
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
                influence_mode="live",
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


class OrchestrationHardeningRegressionTests(unittest.TestCase):
    """Merge blockers found by the independent review of PR #396."""

    def setUp(self):
        bnl01_bot._bnl_self_name_cache.clear()

    def tearDown(self):
        bnl01_bot._bnl_self_name_cache.clear()

    def _seed_governed_self_name(self):
        conn = sqlite3.connect(":memory:")
        conn.execute(
            """
            CREATE TABLE conversations (
                id INTEGER PRIMARY KEY,
                guild_id INTEGER,
                message_id INTEGER,
                role TEXT,
                channel_id INTEGER,
                channel_policy TEXT,
                content TEXT
            )
            """
        )
        conn.executemany(
            """
            INSERT INTO conversations (
                id,guild_id,message_id,role,channel_id,channel_policy,content
            ) VALUES (?,?,?,?,?,?,?)
            """,
            (
                (
                    1,
                    77,
                    8001,
                    "user",
                    700,
                    "public_home",
                    "Can I call you Blue?",
                ),
                (
                    2,
                    77,
                    9001,
                    "model",
                    700,
                    "public_home",
                    "People can call me Blue.",
                ),
            ),
        )
        ensure_memory_ledger_schema(conn)
        for row_id, role, content, message_id in (
            (1, "user", "Can I call you Blue?", 8001),
            (2, "model", "People can call me Blue.", 9001),
        ):
            shadow_conversation_row(
                conn,
                row_id=row_id,
                user_id=44 if role == "user" else 0,
                user_name="member" if role == "user" else "BNL-01",
                guild_id=77,
                role=role,
                content=content,
                channel_name="home",
                channel_policy="public_home",
                channel_id=700,
                message_id=message_id,
                route_mode="normal_chat",
                observed_at=NOW.isoformat(),
            )
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
            response_digest="c" * 64,
            observed_at=(NOW + timedelta(seconds=1)).isoformat(),
        )
        conn.commit()
        return conn, decision

    def test_exact_reply_identity_is_resolved_without_linguistic_pointer(self):
        result = assemble_conversation_context_v2(
            [
                _context_row(
                    1,
                    "The exact source BNL was asked to revisit.",
                    user_name="Writer",
                    minutes=2,
                    message_id=4001,
                )
            ],
            _context_request(
                "BNL, thoughts?",
                referenced_message_ids=frozenset({4001}),
            ),
        )

        self.assertEqual(result.referent_status, "resolved")
        self.assertEqual(result.referent_reason, "discord_reply_source")
        self.assertEqual(result.referent_selected_row_ids, (1,))

    def test_multiple_exact_reply_sources_remain_ambiguous(self):
        result = assemble_conversation_context_v2(
            [
                _context_row(
                    1,
                    "First exact reply source.",
                    user_name="First",
                    minutes=3,
                    message_id=4001,
                ),
                _context_row(
                    2,
                    "Second exact reply source.",
                    user_name="Second",
                    minutes=2,
                    message_id=4002,
                ),
            ],
            _context_request(
                "BNL, thoughts?",
                referenced_message_ids=frozenset({4001, 4002}),
            ),
        )

        self.assertEqual(result.referent_status, "ambiguous")
        self.assertEqual(result.referent_reason, "multiple_discord_reply_sources")
        self.assertEqual(result.referent_candidate_count, 2)
        self.assertEqual(result.referent_selected_row_ids, ())

    def test_exact_reply_source_outranks_general_recency_without_widening_context(self):
        result = assemble_conversation_context_v2(
            [
                _context_row(
                    1,
                    "The exact older source named by the Discord reply.",
                    user_name="Writer",
                    minutes=90,
                    message_id=4001,
                ),
                _context_row(
                    2,
                    "A newer but unrelated room contribution.",
                    user_name="Other",
                    minutes=2,
                    message_id=4002,
                ),
            ],
            _context_request(
                "BNL, thoughts?",
                referenced_message_ids=frozenset({4001}),
            ),
        )

        self.assertEqual(result.referent_status, "resolved")
        self.assertEqual(result.referent_selected_row_ids, (1,))
        self.assertIn("exact older source", result.rendered_context)
        self.assertNotIn("newer but unrelated", result.rendered_context)

    def test_exact_reply_is_the_only_continuity_source_without_scope_expansion(self):
        result = assemble_conversation_context_v2(
            [
                _context_row(
                    1,
                    "Idea A: a radio tower that wakes up at midnight.",
                    user_name="Test Member",
                    minutes=3,
                    message_id=4001,
                ),
                _context_row(
                    2,
                    "Idea B: a vending machine that trades memories.",
                    user_name="Test Member",
                    minutes=2,
                    message_id=4002,
                ),
            ],
            _context_request(
                "BNL, improve this idea in one sentence.",
                referenced_message_ids=frozenset({4001}),
            ),
        )

        self.assertEqual(result.referent_status, "resolved")
        self.assertEqual(result.referent_selected_row_ids, (1,))
        self.assertEqual(result.referent_competing_row_ids, (2,))
        self.assertFalse(result.referent_scope_expanded)
        self.assertEqual(result.thread_focus_mode, "exact_discord_reply")
        self.assertIn("exact Discord reply source", result.rendered_context)
        self.assertIn("radio tower", result.rendered_context)
        self.assertNotIn("vending machine", result.rendered_context)

    def test_exact_reply_can_explicitly_expand_to_another_room_contribution(self):
        result = assemble_conversation_context_v2(
            [
                _context_row(
                    1,
                    "Idea A: a radio tower that wakes up at midnight.",
                    user_name="Test Member",
                    minutes=3,
                    message_id=4001,
                ),
                _context_row(
                    2,
                    "Idea B: a vending machine that trades memories.",
                    user_name="Test Member",
                    minutes=2,
                    message_id=4002,
                ),
            ],
            _context_request(
                "BNL, compare this idea with the newer idea.",
                referenced_message_ids=frozenset({4001}),
            ),
        )

        self.assertEqual(result.referent_selected_row_ids, (1,))
        self.assertTrue(result.referent_scope_expanded)
        self.assertIn("radio tower", result.rendered_context)
        self.assertIn("vending machine", result.rendered_context)

    def test_choice_inside_exact_reply_does_not_widen_to_other_messages(self):
        result = assemble_conversation_context_v2(
            [
                _context_row(
                    1,
                    "Choose a red tower or a blue tower.",
                    user_name="Test Member",
                    minutes=3,
                    message_id=4001,
                ),
                _context_row(
                    2,
                    "A newer room message about a memory vending machine.",
                    user_name="Test Member",
                    minutes=2,
                    message_id=4002,
                ),
            ],
            _context_request(
                "BNL, which one is stronger?",
                referenced_message_ids=frozenset({4001}),
            ),
        )

        self.assertFalse(result.referent_scope_expanded)
        self.assertIn("red tower or a blue tower", result.rendered_context)
        self.assertNotIn("memory vending machine", result.rendered_context)

    def test_reply_grounding_assessment_detects_the_canary_source_switch(self):
        wrong = assess_reply_referent_grounding(
            (
                "The vending machine hums after dark, accepting memories "
                "instead of coins."
            ),
            referent_texts=(
                "Idea A: a radio tower that wakes up at midnight.",
            ),
            competing_texts=(
                "Idea B: a vending machine that trades memories.",
            ),
        )
        correct = assess_reply_referent_grounding(
            (
                "At midnight, the radio tower wakes and broadcasts one "
                "forgotten voice across the sleeping city."
            ),
            referent_texts=(
                "Idea A: a radio tower that wakes up at midnight.",
            ),
            competing_texts=(
                "Idea B: a vending machine that trades memories.",
            ),
        )
        expanded = assess_reply_referent_grounding(
            "The vending machine trades memories.",
            referent_texts=(
                "Idea A: a radio tower that wakes up at midnight.",
            ),
            competing_texts=(
                "Idea B: a vending machine that trades memories.",
            ),
            scope_expanded=True,
        )

        self.assertEqual(
            wrong.status,
            "competing_reply_source_substitution",
        )
        self.assertTrue(wrong.failed)
        self.assertEqual(correct.status, "grounded_exact_reply_source")
        self.assertFalse(correct.failed)
        self.assertEqual(expanded.status, "not_applicable")
        self.assertFalse(expanded.failed)

    def test_exact_reply_source_is_loaded_beyond_general_row_limit(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = os.path.join(temp_dir, "bnl.sqlite")
            with mock.patch.object(bnl01_bot, "DB_FILE", db_path):
                bnl01_bot.init_db()
                with sqlite3.connect(db_path) as conn:
                    conn.execute(
                        """
                        INSERT INTO conversations (
                            user_id,user_name,guild_id,role,content,
                            channel_name,channel_policy,channel_id,
                            timestamp,message_id
                        ) VALUES (?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            44,
                            "Writer",
                            77,
                            "user",
                            "Exact source outside the general row limit.",
                            "home",
                            "public_home",
                            700,
                            (NOW - timedelta(hours=2)).isoformat(),
                            4001,
                        ),
                    )
                    for index in range(3):
                        conn.execute(
                            """
                            INSERT INTO conversations (
                                user_id,user_name,guild_id,role,content,
                                channel_name,channel_policy,channel_id,
                                timestamp,message_id
                            ) VALUES (?,?,?,?,?,?,?,?,?,?)
                            """,
                            (
                                55,
                                "Other",
                                77,
                                "user",
                                f"Newer unrelated row {index}.",
                                "home",
                                "public_home",
                                700,
                                (NOW - timedelta(minutes=index + 1)).isoformat(),
                                5000 + index,
                            ),
                        )
                    conn.commit()

                rows = bnl01_bot.get_conversation_context_v2_rows(
                    77,
                    limit=1,
                    current_user_id=44,
                    channel_id=700,
                    channel_name="home",
                    channel_policy="public_home",
                    referenced_message_ids={4001},
                )

        self.assertIn(4001, {row["message_id"] for row in rows})

    def test_complete_current_payload_outranks_generic_demonstratives(self):
        rows = [
            _context_row(
                1,
                "An unrelated older answer about another deployment.",
                user_name="BNL-01",
                role="model",
                minutes=2,
            ),
            _context_row(
                2,
                "An unrelated older idea about moving a release.",
                user_name="Member",
                minutes=1,
            ),
        ]
        question = assemble_conversation_context_v2(
            rows,
            _context_request(
                "BNL, can you answer this question: is the deployment ready?"
            ),
        )
        idea = assemble_conversation_context_v2(
            rows,
            _context_request(
                "BNL, what do you think about this idea: "
                "We should move the meeting?"
            ),
        )

        self.assertEqual(question.referent_status, "not_requested")
        self.assertEqual(idea.referent_status, "not_requested")

    def test_self_name_request_parser_is_typed_and_bounded(self):
        classify = bnl01_bot.classify_bnl_self_name_request

        false_positive_one = classify("Okay cool, thanks.")
        false_positive_two = classify("Okay perfect, that works.")
        explicit = classify("BNL, can I call you Blue?")
        contextual = classify("Can I call you Blue when we are joking?")
        temporal = classify("Can I call you Blue tomorrow?")
        short_form = classify("Can I call you Blue for short?")
        revoke = classify("Can we stop calling you Blue?")
        correction = classify(
            "Stop calling you Blue and call you Circuit."
        )

        self.assertEqual(false_positive_one.action, "none")
        self.assertEqual(false_positive_two.action, "none")
        self.assertEqual((explicit.action, explicit.name), ("propose", "Blue"))
        self.assertEqual(
            (contextual.action, contextual.name),
            ("propose", "Blue"),
        )
        self.assertEqual((temporal.action, temporal.name), ("propose", "Blue"))
        self.assertEqual(
            (short_form.action, short_form.name),
            ("propose", "Blue"),
        )
        self.assertEqual((revoke.action, revoke.name), ("revoke", "Blue"))
        self.assertEqual(
            (
                correction.action,
                correction.prior_name,
                correction.name,
            ),
            ("correct", "Blue", "Circuit"),
        )

    def test_self_name_response_decision_is_action_aware(self):
        self.assertEqual(
            bnl01_bot.infer_bnl_self_name_decision(
                "Blue",
                "Never call me Blue. Stick with BNL.",
                request_action="propose",
            ),
            "denied",
        )
        self.assertEqual(
            bnl01_bot.infer_bnl_self_name_decision(
                "Blue",
                "I don't think you should call me Blue.",
                request_action="propose",
            ),
            "denied",
        )
        self.assertEqual(
            bnl01_bot.infer_bnl_self_name_decision(
                "Blue",
                "Yeah, stop using Blue for me. Stick with BNL.",
                request_action="revoke",
            ),
            "revoked",
        )

    def test_self_name_correction_rolls_back_as_one_lifecycle_transaction(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = os.path.join(temp_dir, "bnl.sqlite")
            response = (
                "I'll stop using Blue. People may call me Circuit."
            )
            with (
                mock.patch.object(bnl01_bot, "DB_FILE", db_path),
                mock.patch.dict(
                    os.environ,
                    {
                        "BNL_MEMORY_LEDGER_SHADOW_ENABLED": "1",
                        "BNL_CONVERSATION_ORCHESTRATION_INFLUENCE_ENABLED": "1",
                    },
                    clear=False,
                ),
            ):
                bnl01_bot.init_db()
                bnl01_bot.save_user_message(
                    44,
                    "Member",
                    77,
                    "Stop calling you Blue and call you Circuit.",
                    channel_name="home",
                    channel_policy="public_home",
                    channel_id=700,
                    message_id=8001,
                )
                bnl01_bot.save_model_message(
                    44,
                    77,
                    response,
                    channel_name="home",
                    channel_policy="public_home",
                    channel_id=700,
                    discord_message_ids=(9001,),
                )
                addressing = bnl01_bot.DiscordTurnAddressing(
                    speaker="Member",
                    explicit_tag_recipients=(),
                    reply_target="none",
                    explicitly_mentions_bnl=False,
                    reply_targets_bnl=False,
                    directly_targets_bnl=False,
                    targets_other_human=False,
                    plain_text_names_bnl=True,
                    bnl_name_state="correction",
                    bnl_name_value="Circuit",
                    bnl_name_requires_decision=True,
                    bnl_name_action="correct",
                    bnl_name_prior_value="Blue",
                    bnl_name_influence_mode="live",
                    source_message_id=8001,
                )
                original_record = (
                    bnl01_bot.record_bnl_self_name_decision
                )

                def fail_second_decision(conn, **kwargs):
                    if kwargs.get("name") == "Circuit":
                        return bnl01_bot.LedgerWriteResult(
                            outcome="skipped",
                            reason_code="injected_second_write_failure",
                            guild_id=77,
                        )
                    return original_record(conn, **kwargs)

                with mock.patch.object(
                    bnl01_bot,
                    "record_bnl_self_name_decision",
                    side_effect=fail_second_decision,
                ):
                    result = (
                        bnl01_bot.persist_bnl_self_name_decision_after_send(
                            guild_id=77,
                            addressing=addressing,
                            response=response,
                            channel_id=700,
                            channel_name="home",
                            channel_policy="public_home",
                            route_mode="normal_chat",
                        )
                    )

                self.assertIsNone(result)
                with sqlite3.connect(db_path) as conn:
                    derived_count = conn.execute(
                        """
                        SELECT COUNT(*)
                        FROM memory_ledger_entries
                        WHERE source_table='bnl_self_name_decisions'
                        """
                    ).fetchone()[0]
                self.assertEqual(derived_count, 0)

                success = (
                    bnl01_bot.persist_bnl_self_name_decision_after_send(
                        guild_id=77,
                        addressing=addressing,
                        response=response,
                        channel_id=700,
                        channel_name="home",
                        channel_policy="public_home",
                        route_mode="normal_chat",
                    )
                )
                self.assertIn(
                    success.outcome,
                    {"inserted", "deduplicated"},
                )
                with sqlite3.connect(db_path) as conn:
                    states = {
                        record.normalized_name: record.decision
                        for record in current_bnl_self_name_records(
                            conn,
                            guild_id=77,
                        )
                    }
                self.assertEqual(
                    states,
                    {"blue": "revoked", "circuit": "accepted"},
                )

    def test_self_name_authority_fails_closed_when_provenance_is_removed(self):
        with sqlite3.connect(":memory:") as conn:
            conn.execute(
                """
                CREATE TABLE conversations (
                    id INTEGER PRIMARY KEY,
                    guild_id INTEGER,
                    message_id INTEGER,
                    role TEXT,
                    channel_id INTEGER,
                    channel_policy TEXT,
                    content TEXT
                )
                """
            )
            conn.executemany(
                """
                INSERT INTO conversations (
                    id,guild_id,message_id,role,channel_id,channel_policy,content
                ) VALUES (?,?,?,?,?,?,?)
                """,
                (
                    (
                        1,
                        77,
                        8001,
                        "user",
                        700,
                        "public_home",
                        "Can I call you Blue?",
                    ),
                    (
                        2,
                        77,
                        9001,
                        "model",
                        700,
                        "public_home",
                        "People can call me Blue.",
                    ),
                ),
            )
            ensure_memory_ledger_schema(conn)
            for row_id, role, content, message_id in (
                (1, "user", "Can I call you Blue?", 8001),
                (2, "model", "People can call me Blue.", 9001),
            ):
                shadow_conversation_row(
                    conn,
                    row_id=row_id,
                    user_id=44 if role == "user" else 0,
                    user_name="member" if role == "user" else "BNL-01",
                    guild_id=77,
                    role=role,
                    content=content,
                    channel_name="home",
                    channel_policy="public_home",
                    channel_id=700,
                    message_id=message_id,
                    route_mode="normal_chat",
                    observed_at=NOW.isoformat(),
                )
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
            self.assertEqual(
                len(current_bnl_self_name_records(conn, guild_id=77)),
                1,
            )

            conn.execute(
                """
                DELETE FROM memory_ledger_lineage
                WHERE entry_id=? AND lineage_type='derived_from'
                """,
                (decision.entry_id,),
            )
            conn.commit()

            self.assertEqual(
                current_bnl_self_name_records(conn, guild_id=77),
                (),
            )

    def test_self_name_authority_fails_closed_when_source_is_deleted(self):
        with sqlite3.connect(":memory:") as conn:
            conn.execute(
                """
                CREATE TABLE conversations (
                    id INTEGER PRIMARY KEY,
                    guild_id INTEGER,
                    message_id INTEGER,
                    role TEXT,
                    channel_id INTEGER,
                    channel_policy TEXT,
                    content TEXT
                )
                """
            )
            conn.executemany(
                """
                INSERT INTO conversations (
                    id,guild_id,message_id,role,channel_id,channel_policy,content
                ) VALUES (?,?,?,?,?,?,?)
                """,
                (
                    (1, 77, 8001, "user", 700, "public_home", "proposal"),
                    (2, 77, 9001, "model", 700, "public_home", "acceptance"),
                ),
            )
            ensure_memory_ledger_schema(conn)
            for row_id, role, content, message_id in (
                (1, "user", "proposal", 8001),
                (2, "model", "acceptance", 9001),
            ):
                shadow_conversation_row(
                    conn,
                    row_id=row_id,
                    user_id=44 if role == "user" else 0,
                    user_name="member" if role == "user" else "BNL-01",
                    guild_id=77,
                    role=role,
                    content=content,
                    channel_name="home",
                    channel_policy="public_home",
                    channel_id=700,
                    message_id=message_id,
                    route_mode="normal_chat",
                    observed_at=NOW.isoformat(),
                )
            record_bnl_self_name_decision(
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
                response_digest="b" * 64,
                observed_at=(NOW + timedelta(seconds=1)).isoformat(),
            )
            conn.execute("DELETE FROM conversations WHERE id=1")
            conn.commit()

            self.assertEqual(
                current_bnl_self_name_records(conn, guild_id=77),
                (),
            )

    def test_self_name_authority_fails_closed_without_conversation_store(self):
        conn, _decision = self._seed_governed_self_name()
        try:
            self.assertEqual(
                len(current_bnl_self_name_records(conn, guild_id=77)),
                1,
            )
            conn.execute("DROP TABLE conversations")
            conn.commit()
            self.assertEqual(
                current_bnl_self_name_records(conn, guild_id=77),
                (),
            )
        finally:
            conn.close()

    def test_self_name_authority_tracks_root_lifecycle_and_visibility(self):
        conn, _decision = self._seed_governed_self_name()
        try:
            roots = conn.execute(
                """
                SELECT entry_id
                FROM memory_ledger_entries
                WHERE guild_id=77 AND source_table='conversations'
                ORDER BY source_row_id
                """
            ).fetchall()
            self.assertEqual(
                len(current_bnl_self_name_records(conn, guild_id=77)),
                1,
            )

            conn.execute(
                """
                UPDATE memory_ledger_entries
                SET lifecycle_status='retracted'
                WHERE entry_id=?
                """,
                (roots[0][0],),
            )
            conn.commit()
            self.assertEqual(
                current_bnl_self_name_records(conn, guild_id=77),
                (),
            )
        finally:
            conn.close()

        conn, _decision = self._seed_governed_self_name()
        try:
            user_root = conn.execute(
                """
                SELECT entry_id
                FROM memory_ledger_entries
                WHERE guild_id=77 AND source_table='conversations'
                  AND source_role='user'
                """
            ).fetchone()[0]
            conn.execute(
                """
                UPDATE memory_ledger_entries
                SET visibility='sealed_test'
                WHERE entry_id=?
                """,
                (user_root,),
            )
            conn.commit()
            self.assertEqual(
                current_bnl_self_name_records(conn, guild_id=77),
                (),
            )
        finally:
            conn.close()

    def test_self_name_lookup_does_not_reuse_stale_positive_cache(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = os.path.join(temp_dir, "bnl.sqlite")
            conn, _decision = self._seed_governed_self_name()
            try:
                with sqlite3.connect(db_path) as persisted:
                    conn.backup(persisted)
            finally:
                conn.close()
            with (
                mock.patch.object(bnl01_bot, "DB_FILE", db_path),
                mock.patch.object(
                    bnl01_bot,
                    "memory_ledger_shadow_enabled",
                    return_value=True,
                ),
            ):
                first = bnl01_bot._load_bnl_self_name_records(
                    77,
                    "public_home",
                )
                self.assertEqual(len(first), 1)
                bnl01_bot._bnl_self_name_cache[
                    (77, ("public_home",))
                ] = (9999999999.0, first)
                with sqlite3.connect(db_path) as persisted:
                    persisted.execute(
                        """
                        UPDATE memory_ledger_entries
                        SET lifecycle_status='retracted'
                        WHERE source_table='conversations'
                          AND source_role='user'
                        """
                    )
                    persisted.commit()
                self.assertEqual(
                    bnl01_bot._load_bnl_self_name_records(
                        77,
                        "public_home",
                    ),
                    (),
                )

    def test_orchestration_influence_requires_its_own_fail_closed_gate(self):
        env_names = {
            "BNL_CONVERSATION_ORCHESTRATION_INFLUENCE_ENABLED": "0",
            "BNL_CONVERSATION_ORCHESTRATION_SEALED_CANARY_ENABLED": "0",
            "BNL_CONVERSATION_ORCHESTRATION_SEALED_CANARY_GUILD_IDS": "",
            "BNL_CONVERSATION_ORCHESTRATION_SEALED_CANARY_CHANNEL_IDS": "",
            "BNL_MEMORY_LEDGER_SHADOW_ENABLED": "1",
            "BNL_MOMENT_ENGINE_SHADOW_ENABLED": "1",
        }
        with mock.patch.dict(os.environ, env_names, clear=False):
            self.assertEqual(
                bnl01_bot.conversation_orchestration_influence_mode(
                    guild_id=77,
                    channel_id=700,
                    channel_policy="public_home",
                ),
                "off",
            )
            self.assertEqual(
                bnl01_bot.conversation_orchestration_influence_mode(
                    guild_id=77,
                    channel_id=700,
                    channel_policy="sealed_test",
                ),
                "off",
            )

    def test_sealed_canary_requires_exact_guild_and_channel_scope(self):
        with mock.patch.dict(
            os.environ,
            {
                "BNL_CONVERSATION_ORCHESTRATION_INFLUENCE_ENABLED": "0",
                "BNL_CONVERSATION_ORCHESTRATION_SEALED_CANARY_ENABLED": "1",
                "BNL_CONVERSATION_ORCHESTRATION_SEALED_CANARY_GUILD_IDS": "77",
                "BNL_CONVERSATION_ORCHESTRATION_SEALED_CANARY_CHANNEL_IDS": "700",
            },
            clear=False,
        ):
            self.assertEqual(
                bnl01_bot.conversation_orchestration_influence_mode(
                    guild_id=77,
                    channel_id=700,
                    channel_policy="sealed_test",
                ),
                "sealed_canary",
            )
            self.assertEqual(
                bnl01_bot.conversation_orchestration_influence_mode(
                    guild_id=77,
                    channel_id=701,
                    channel_policy="sealed_test",
                ),
                "off",
            )
            self.assertEqual(
                bnl01_bot.conversation_orchestration_influence_mode(
                    guild_id=77,
                    channel_id=700,
                    channel_policy="public_home",
                ),
                "off",
            )

    def test_orchestration_gate_re_evaluates_and_rolls_back_fail_closed(self):
        env = {
            "BNL_CONVERSATION_ORCHESTRATION_INFLUENCE_ENABLED": "1",
            "BNL_CONVERSATION_ORCHESTRATION_SEALED_CANARY_ENABLED": "0",
            "BNL_CONVERSATION_ORCHESTRATION_SEALED_CANARY_GUILD_IDS": "",
            "BNL_CONVERSATION_ORCHESTRATION_SEALED_CANARY_CHANNEL_IDS": "",
        }
        self.assertEqual(
            bnl01_bot.conversation_orchestration_influence_mode(
                guild_id=77,
                channel_id=700,
                channel_policy="public_home",
                environ=env,
            ),
            "live",
        )
        env["BNL_CONVERSATION_ORCHESTRATION_INFLUENCE_ENABLED"] = "0"
        self.assertEqual(
            bnl01_bot.conversation_orchestration_influence_mode(
                guild_id=77,
                channel_id=700,
                channel_policy="public_home",
                environ=env,
            ),
            "off",
        )

        env.update(
            {
                "BNL_CONVERSATION_ORCHESTRATION_SEALED_CANARY_ENABLED": "1",
                "BNL_CONVERSATION_ORCHESTRATION_SEALED_CANARY_GUILD_IDS": "77",
                "BNL_CONVERSATION_ORCHESTRATION_SEALED_CANARY_CHANNEL_IDS": "700",
            }
        )
        self.assertEqual(
            bnl01_bot.conversation_orchestration_influence_mode(
                guild_id=77,
                channel_id=700,
                channel_policy="sealed_test",
                environ=env,
            ),
            "sealed_canary",
        )
        env["BNL_CONVERSATION_ORCHESTRATION_SEALED_CANARY_ENABLED"] = "0"
        self.assertEqual(
            bnl01_bot.conversation_orchestration_influence_mode(
                guild_id=77,
                channel_id=700,
                channel_policy="sealed_test",
                environ=env,
            ),
            "off",
        )

    def test_supporting_owner_states_are_typed_and_relationship_read_is_offloaded(
        self,
    ):
        relation = (12, 0.8, "trusted", "warm", "topic", NOW.isoformat())

        async def run():
            with (
                mock.patch.object(
                    bnl01_bot.os.path,
                    "exists",
                    return_value=True,
                ),
                mock.patch(
                    "bnl01_bot.asyncio.to_thread",
                    new=mock.AsyncMock(return_value=relation),
                ) as to_thread,
                mock.patch.object(
                    bnl01_bot,
                    "_canon_relevant_to_response",
                    return_value=True,
                ),
            ):
                states = await bnl01_bot.conversation_supporting_owner_states(
                    guild_id=77,
                    channel_policy="public_home",
                    current_text="A canon-sensitive request.",
                    participant_user_ids=(44,),
                    addressings=(
                        _addressing(
                            bnl=True,
                            state="accepted",
                            value="Blue",
                        ),
                    ),
                )
                return states, to_thread

        states, to_thread = asyncio.run(run())
        self.assertEqual(
            states["governed_memory_state"],
            "routing_self_name:accepted",
        )
        self.assertEqual(states["relationship_state"], "tone:trusted:warm")
        self.assertEqual(states["canon_state"], "applicable_content_owner")
        self.assertEqual(
            states["source_control_state"],
            "route_and_visibility_applied",
        )
        to_thread.assert_awaited_once_with(
            bnl01_bot._relationship_state_for_turn,
            44,
            77,
        )

    def test_supporting_owner_states_cannot_become_a_second_response_veto(self):
        baseline = coordinate_conversation_turn(
            ConversationOrchestrationInput(
                route_allowed=True,
                engagement_decision="answer",
                engagement_reason="request",
            )
        )
        with_owner_states = coordinate_conversation_turn(
            ConversationOrchestrationInput(
                route_allowed=True,
                engagement_decision="answer",
                engagement_reason="request",
                governed_memory_state="content_memory_not_routing_authority",
                relationship_state="tone:trusted:warm",
                canon_state="applicable_content_owner",
                source_control_state="route_and_visibility_applied",
            )
        )

        self.assertEqual(
            with_owner_states.response_act,
            baseline.response_act,
        )
        self.assertEqual(
            with_owner_states.reason,
            baseline.reason,
        )
        self.assertEqual(
            with_owner_states.relationship_state,
            "tone:trusted:warm",
        )
        self.assertEqual(
            with_owner_states.canon_state,
            "applicable_content_owner",
        )

    def test_optional_direct_context_failure_fails_closed(self):
        result_out = {}

        async def run():
            with mock.patch.object(
                bnl01_bot,
                "build_room_first_direct_context",
                side_effect=sqlite3.OperationalError("locked"),
            ):
                return await (
                    bnl01_bot.build_room_first_direct_context_async(
                        77,
                        700,
                        "home",
                        "public_home",
                        "Member",
                        context_result_out=result_out,
                    )
                )

        self.assertEqual(asyncio.run(run()), "")
        self.assertIsNone(result_out["result"])

    def test_optional_batch_context_failure_cannot_veto_direct_address(self):
        direct_addressing = bnl01_bot.DiscordTurnAddressing(
            **{
                **_addressing().__dict__,
                "explicitly_mentions_bnl": True,
                "directly_targets_bnl": True,
            }
        )
        item = bnl01_bot.BatchConversationTurn(
            "Member",
            "Please answer.",
            44,
            direct_addressing,
        )
        active_packet = {
            "items": (item,),
            "addressed_to_bot": True,
            "has_request_payload": False,
            "payload_items": (),
        }

        async def run():
            with (
                mock.patch.dict(
                    os.environ,
                    {
                        "BNL_CONVERSATION_ORCHESTRATION_INFLUENCE_ENABLED": "1",
                    },
                    clear=False,
                ),
                mock.patch.object(
                    bnl01_bot,
                    "conversation_context_v2_enabled",
                    return_value=True,
                ),
                mock.patch.object(
                    bnl01_bot,
                    "build_active_batch_conversation_context_v2_prompt",
                    side_effect=sqlite3.OperationalError("locked"),
                ),
                mock.patch.object(
                    bnl01_bot,
                    "_recent_moment_situation_for_turn_async",
                    new=mock.AsyncMock(return_value=None),
                ),
                mock.patch.object(
                    bnl01_bot,
                    "conversation_supporting_owner_states",
                    new=mock.AsyncMock(
                        return_value={
                            "governed_memory_state": (
                                "content_memory_not_routing_authority"
                            ),
                            "relationship_state": "owner_unavailable",
                            "canon_state": "not_relevant",
                            "source_control_state": (
                                "route_and_visibility_applied"
                            ),
                        }
                    ),
                ),
            ):
                return await bnl01_bot.build_active_batch_orchestration(
                    guild_id=77,
                    channel_id=700,
                    channel_name="home",
                    channel_policy="public_home",
                    first_uid=44,
                    collapsed_items=(item,),
                    unique_user_ids=(44,),
                    active_packet=active_packet,
                    engagement_decision="observe",
                    engagement_reason="no_response_needed",
                )

        state = asyncio.run(run())
        self.assertIsNone(state["context_result"])
        self.assertEqual(state["recent_room_prompt"], "")
        self.assertEqual(state["decision"].response_act, "answer")
        self.assertTrue(state["decision"].response_required)

    def test_model_discord_message_ids_are_persisted_as_reply_identity(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = os.path.join(temp_dir, "bnl.sqlite")
            with mock.patch.object(bnl01_bot, "DB_FILE", db_path):
                bnl01_bot.init_db()
                bnl01_bot.save_model_message(
                    44,
                    77,
                    "A delivered BNL response.",
                    channel_name="home",
                    channel_policy="public_home",
                    channel_id=700,
                    discord_message_ids=(9001, 9002),
                )
                with sqlite3.connect(db_path) as conn:
                    model_row = conn.execute(
                        """
                        SELECT id,message_id
                        FROM conversations
                        WHERE guild_id=77 AND role='model'
                        """
                    ).fetchone()
                    links = conn.execute(
                        """
                        SELECT message_id
                        FROM conversation_discord_message_links
                        WHERE conversation_row_id=?
                        ORDER BY message_id
                        """,
                        (model_row[0],),
                    ).fetchall()

            self.assertEqual(model_row[1], 9001)
            self.assertEqual(links, [(9001,), (9002,)])

    def test_unresolved_discord_reference_uses_persisted_model_identity(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = os.path.join(temp_dir, "bnl.sqlite")
            with mock.patch.object(bnl01_bot, "DB_FILE", db_path):
                bnl01_bot.init_db()
                bnl01_bot.save_model_message(
                    44,
                    77,
                    "A delivered BNL response.",
                    channel_name="home",
                    channel_policy="public_home",
                    channel_id=700,
                    discord_message_ids=(9001,),
                )
                message = SimpleNamespace(
                    id=9100,
                    content="Thoughts?",
                    author=SimpleNamespace(display_name="Member"),
                    guild=SimpleNamespace(id=77, members=[]),
                    channel=SimpleNamespace(
                        id=700,
                        name="home",
                        category=None,
                        guild=SimpleNamespace(id=77),
                    ),
                    raw_mentions=[],
                    mentions=[],
                    reference=SimpleNamespace(
                        resolved=None,
                        message_id=9001,
                    ),
                )
                with (
                    mock.patch.object(
                        bnl01_bot,
                        "_load_bnl_self_name_records",
                        return_value=(),
                    ),
                    mock.patch.object(
                        bnl01_bot.client._connection,
                        "user",
                        SimpleNamespace(id=999, display_name="BNL-01"),
                    ),
                ):
                    addressing = bnl01_bot.resolve_discord_turn_addressing(
                        message
                    )

            self.assertEqual(addressing.reply_message_id, 9001)
            self.assertTrue(addressing.reply_targets_bnl)
            self.assertTrue(addressing.directly_targets_bnl)
            self.assertGreater(addressing.reply_conversation_row_id, 0)

    def test_unresolved_discord_reference_to_member_stays_third_party_only(
        self,
    ):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = os.path.join(temp_dir, "bnl.sqlite")
            with mock.patch.object(bnl01_bot, "DB_FILE", db_path):
                bnl01_bot.init_db()
                bnl01_bot.save_user_message(
                    55,
                    "Prior Member",
                    77,
                    "A member contribution.",
                    channel_name="home",
                    channel_policy="public_home",
                    channel_id=700,
                    message_id=9003,
                )
                message = SimpleNamespace(
                    id=9100,
                    content="What do you think?",
                    author=SimpleNamespace(display_name="Current Member"),
                    guild=SimpleNamespace(id=77, members=[]),
                    channel=SimpleNamespace(
                        id=700,
                        name="home",
                        category=None,
                        guild=SimpleNamespace(id=77),
                    ),
                    raw_mentions=[],
                    mentions=[],
                    reference=SimpleNamespace(
                        resolved=None,
                        message_id=9003,
                    ),
                )
                with (
                    mock.patch.object(
                        bnl01_bot,
                        "_load_bnl_self_name_records",
                        return_value=(),
                    ),
                    mock.patch.object(
                        bnl01_bot.client._connection,
                        "user",
                        SimpleNamespace(id=999, display_name="BNL-01"),
                    ),
                ):
                    addressing = bnl01_bot.resolve_discord_turn_addressing(
                        message
                    )

            self.assertEqual(addressing.reply_target, "Prior Member")
            self.assertFalse(addressing.reply_targets_bnl)
            self.assertFalse(addressing.directly_targets_bnl)
            self.assertTrue(addressing.targets_other_human)
            self.assertTrue(addressing.third_party_only)

    def test_live_address_resolution_offloads_database_reads(self):
        sentinel = _addressing()

        async def run():
            with (
                mock.patch(
                    "bnl01_bot.asyncio.to_thread",
                    new=mock.AsyncMock(return_value=sentinel),
                ) as to_thread,
            ):
                result = await bnl01_bot.resolve_discord_turn_addressing_async(
                    SimpleNamespace()
                )
                return result, to_thread

        result, to_thread = asyncio.run(run())
        self.assertIs(result, sentinel)
        to_thread.assert_awaited_once()

    def test_disabled_gate_skips_governed_name_ledger_read(self):
        message = SimpleNamespace(
            id=9100,
            content="BNL, thoughts?",
            author=SimpleNamespace(display_name="Member"),
            guild=SimpleNamespace(id=77, members=[]),
            channel=SimpleNamespace(
                id=700,
                name="home",
                category=None,
                guild=SimpleNamespace(id=77),
            ),
            raw_mentions=[],
            mentions=[],
            reference=None,
        )
        with (
            mock.patch.dict(
                os.environ,
                {
                    "BNL_CONVERSATION_ORCHESTRATION_INFLUENCE_ENABLED": "0",
                    "BNL_CONVERSATION_ORCHESTRATION_SEALED_CANARY_ENABLED": "0",
                    "BNL_CONVERSATION_ORCHESTRATION_SEALED_CANARY_GUILD_IDS": "",
                    "BNL_CONVERSATION_ORCHESTRATION_SEALED_CANARY_CHANNEL_IDS": "",
                },
                clear=False,
            ),
            mock.patch.object(
                bnl01_bot,
                "_load_bnl_self_name_records",
            ) as load_records,
            mock.patch.object(
                bnl01_bot.client._connection,
                "user",
                SimpleNamespace(id=999, display_name="BNL-01"),
            ),
        ):
            addressing = bnl01_bot.resolve_discord_turn_addressing(message)

        load_records.assert_not_called()
        self.assertEqual(addressing.bnl_name_influence_mode, "off")
        self.assertEqual(addressing.bnl_name_state, "canonical")
        self.assertTrue(addressing.addresses_bnl)

    def test_interruption_rebuild_has_no_stale_answer_latch(self):
        with open("bnl01_bot.py", encoding="utf-8") as source_file:
            source = source_file.read()

        self.assertNotIn("answer_intent_locked", source)
        self.assertNotIn("preserved_prior_request_intent", source)

    def test_legacy_previous_message_shortcut_cannot_bypass_live_packet(self):
        with open("bnl01_bot.py", encoding="utf-8") as source_file:
            source = source_file.read()

        self.assertIn(
            "_is_previous_message_request(clean_content)\n"
            "        and not orchestration_influences",
            source,
        )

    def test_live_third_party_batch_reaches_final_packet_authority(self):
        with open("bnl01_bot.py", encoding="utf-8") as source_file:
            source = source_file.read()

        self.assertEqual(
            source.count(
                "batch_exclusively_targets_other_people(items)\n"
                "                and not (pending_state or pending_anchor)\n"
                "                and not batch_orchestration_influences"
            ),
            1,
        )
        self.assertIn(
            "batch_exclusively_targets_other_people(items)\n"
            "            and not (pending_state or pending_anchor)\n"
            "            and not batch_orchestration_influences",
            source,
        )

    def test_delivered_direct_snapshot_preserves_newer_payload_revision(self):
        key = (77, 700, 44)
        session = {
            "revision": 2,
            "payload_lines": ["first", "newer"],
            "last_committed_payload_count": 0,
            "generating": True,
            "generation_invalidated": True,
        }
        bnl01_bot._direct_payload_sessions[key] = session
        try:
            state = bnl01_bot.commit_direct_payload_session_delivery(
                key,
                session,
                generation_revision=1,
                payload_count=1,
            )
        finally:
            bnl01_bot._direct_payload_sessions.pop(key, None)

        self.assertEqual(state, "newer_revision_pending")
        self.assertEqual(session["last_committed_revision"], 1)
        self.assertEqual(session["last_committed_payload_count"], 1)
        self.assertEqual(session["revision"], 2)
        self.assertEqual(session["payload_lines"], ["first", "newer"])
        self.assertFalse(session["generating"])
        self.assertFalse(session["generation_invalidated"])

    def test_old_delivery_never_mutates_replacement_session(self):
        key = (77, 700, 44)
        old_session = {
            "revision": 1,
            "payload_lines": ["old"],
            "generating": True,
        }
        replacement = {
            "revision": 0,
            "payload_lines": ["replacement"],
            "generating": False,
        }
        bnl01_bot._direct_payload_sessions[key] = replacement
        try:
            state = bnl01_bot.commit_direct_payload_session_delivery(
                key,
                old_session,
                generation_revision=1,
                payload_count=1,
            )
        finally:
            bnl01_bot._direct_payload_sessions.pop(key, None)

        self.assertEqual(state, "replaced")
        self.assertFalse(old_session["generating"])
        self.assertEqual(
            replacement,
            {
                "revision": 0,
                "payload_lines": ["replacement"],
                "generating": False,
            },
        )

    def test_batch_prompt_uses_the_same_typed_name_lifecycle(self):
        addressing = bnl01_bot.replace(
            _addressing(
                bnl=True,
                state="correction",
                value="Circuit",
                requires_decision=True,
            ),
            bnl_name_action="correct",
            bnl_name_prior_value="Blue",
        )
        turn = bnl01_bot.BatchConversationTurn(
            "Member",
            "Stop calling you Blue and call you Circuit.",
            44,
            addressing,
        )

        prompt = bnl01_bot._format_batched_prompt(
            [turn],
            "steady_reply",
            "Answer naturally.",
        )

        self.assertIn("BNL self-name lifecycle action=correct", prompt)
        self.assertIn('prior self-name="Blue"', prompt)
        self.assertIn("explicitly retire the prior name", prompt)
        self.assertIn("accept the new one only if BNL agrees", prompt)

    def test_turn_evidence_packet_is_immutable_and_revisioned(self):
        packet = ConversationOrchestrationInput(
            route_allowed=True,
            engagement_decision="answer",
            engagement_reason="request",
        )
        first = coordinate_conversation_turn(packet)
        second = coordinate_conversation_turn(packet)
        changed = coordinate_conversation_turn(
            ConversationOrchestrationInput(
                route_allowed=True,
                engagement_decision="observe",
                engagement_reason="newer_packet",
            )
        )

        with self.assertRaises(AttributeError):
            packet.engagement_decision = "observe"
        self.assertEqual(first.packet_revision, second.packet_revision)
        self.assertNotEqual(first.packet_revision, changed.packet_revision)
        self.assertFalse(first.influences_response)

    def test_packet_revision_includes_each_discord_source_identity(self):
        first_addressing = _addressing()
        second_addressing = bnl01_bot.DiscordTurnAddressing(
            **{
                **first_addressing.__dict__,
                "source_message_id": 5678,
            }
        )
        first = bnl01_bot.BatchConversationTurn(
            "Member",
            "Same text",
            44,
            first_addressing,
        )
        second = bnl01_bot.BatchConversationTurn(
            "Member",
            "Same text",
            44,
            second_addressing,
        )

        first_revision = bnl01_bot.conversation_turn_packet_revision(
            guild_id=77,
            channel_id=700,
            route_mode="normal_chat",
            current_items=(first,),
        )
        second_revision = bnl01_bot.conversation_turn_packet_revision(
            guild_id=77,
            channel_id=700,
            route_mode="normal_chat",
            current_items=(second,),
        )
        combined_revision = bnl01_bot.conversation_turn_packet_revision(
            guild_id=77,
            channel_id=700,
            route_mode="normal_chat",
            current_items=(first, second),
        )

        self.assertNotEqual(first_revision, second_revision)
        self.assertNotEqual(first_revision, combined_revision)
        self.assertNotEqual(second_revision, combined_revision)


if __name__ == "__main__":
    unittest.main()
