import asyncio
import json
import os
from pathlib import Path
import sqlite3
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl01_bot
import bnl_memory_ledger as ledger
from bnl_memory_preview import (
    MemoryPreviewRequest,
    evaluate_memory_preview,
    prepare_memory_preview,
)


class _ProductionChannel:
    def __init__(self):
        self.id = 10
        self.name = "barcode-bot"
        self.sent = []

    async def send(self, content, **_kwargs):
        self.sent.append(content)


class _ProductionMessage:
    def __init__(self, *, user_id, user_name, content):
        self.author = SimpleNamespace(
            id=int(user_id),
            display_name=str(user_name),
        )
        self.guild = SimpleNamespace(id=1)
        self.channel = _ProductionChannel()
        self.content = str(content)
        self.attachments = ()
        self.stickers = ()
        self.reference = None
        self.replies = []

    async def reply(self, content, **_kwargs):
        self.replies.append(content)


class ProductionShapedBroadRecallAcceptanceTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = str(
            Path(self.tempdir.name) / "production-shaped.db"
        )
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE conversations (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  user_id INTEGER NOT NULL,
                  user_name TEXT NOT NULL,
                  guild_id INTEGER NOT NULL,
                  channel_name TEXT NOT NULL,
                  channel_policy TEXT NOT NULL,
                  channel_id INTEGER NOT NULL,
                  message_id INTEGER,
                  role TEXT NOT NULL,
                  content TEXT NOT NULL,
                  timestamp TEXT NOT NULL
                )
                """
            )
            ledger.ensure_memory_ledger_schema(conn)
            conn.commit()
        self.db_patch = mock.patch.object(
            bnl01_bot,
            "DB_FILE",
            self.db_path,
        )
        self.db_patch.start()
        self.env_patch = mock.patch.dict(
            os.environ,
            {
                ledger.MEMORY_LEDGER_SHADOW_ENV: "true",
                ledger.CONVERSATION_MOTIF_FORMATION_ENV: "true",
                "BNL_MOMENT_ENGINE_SHADOW_ENABLED": "false",
                "BNL_RELATIONSHIP_V2_SHADOW_ENABLED": "false",
                "BNL_RELATIONSHIP_V2_LIVE_ENABLED": "false",
                "BNL_MEMORY_GOVERNANCE_LIVE_ENABLED": "false",
                "BNL_ACTIVE_ENGAGEMENT_V2_LIVE_ENABLED": "false",
            },
            clear=False,
        )
        self.env_patch.start()
        self.next_message_id = 1000

    def tearDown(self):
        self.env_patch.stop()
        self.db_patch.stop()
        self.tempdir.cleanup()

    def add_raw_message(
        self,
        *,
        user_id,
        user_name,
        content,
        observed_at,
        channel_id=10,
        channel_name="barcode-bot",
        channel_policy="public_home",
    ):
        self.next_message_id += 1
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                INSERT INTO conversations(
                  user_id,user_name,guild_id,channel_name,channel_policy,
                  channel_id,message_id,role,content,timestamp
                ) VALUES(?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    int(user_id),
                    str(user_name),
                    1,
                    str(channel_name),
                    str(channel_policy),
                    int(channel_id),
                    self.next_message_id,
                    "user",
                    str(content),
                    str(observed_at),
                ),
            )
            row_id = int(cursor.lastrowid)
            conn.commit()

        result = bnl01_bot._shadow_memory_ledger_write(
            "production_shaped_acceptance",
            lambda ledger_conn: ledger.shadow_conversation_row(
                ledger_conn,
                row_id=row_id,
                user_id=int(user_id),
                user_name=str(user_name),
                guild_id=1,
                role="user",
                content=str(content),
                channel_name=str(channel_name),
                channel_policy=str(channel_policy),
                channel_id=int(channel_id),
                message_id=self.next_message_id,
                route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                observed_at=str(observed_at),
            ),
            guild_id=1,
            source_table="conversations",
            source_row_id=row_id,
            source_revision=str(row_id),
        )
        self.assertIsNotNone(result)
        self.assertIn(result.outcome, {"inserted", "deduplicated"})
        return row_id

    def add_recurring_topic(
        self,
        *,
        user_id,
        user_name,
        first,
        second,
        day_offset=0,
    ):
        self.add_raw_message(
            user_id=user_id,
            user_name=user_name,
            content=first,
            observed_at=(
                "2026-07-%02dT18:00:00+00:00"
                % (20 + int(day_offset))
            ),
        )
        self.add_raw_message(
            user_id=user_id,
            user_name=user_name,
            content=second,
            observed_at=(
                "2026-07-%02dT18:00:00+00:00"
                % (21 + int(day_offset))
            ),
        )

    def request(self, *, user_id, user_name, wording):
        baseline = (
            "Current user request: %s\n"
            "BNL persona and BARCODE lore remain active.\n"
            "Current channel: #barcode-bot\n"
            "Current channel policy: public_home\n"
            "Durable memory context:\n"
            "No stored member facts are supplied to this comparison.\n"
            "Personal-recall route contract remains active."
            % wording
        )
        return MemoryPreviewRequest(
            source_db_path=self.db_path,
            guild_id=1,
            subject_user_id=int(user_id),
            subject_display_name=str(user_name),
            simulated_channel_id=10,
            wording=str(wording),
            baseline_prompt=baseline,
            factual_placeholder=(
                "No stored member facts are supplied to this comparison."
            ),
            now="2026-07-26T12:00:00+00:00",
        )

    def composition_env(self, *, user_id):
        return {
            ledger.MEMORY_LEDGER_SHADOW_ENV: "true",
            ledger.CONVERSATION_MOTIF_FORMATION_ENV: "true",
            "BNL_MOMENT_ENGINE_SHADOW_ENABLED": "true",
            "BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED": "true",
            "BNL_RELATIONSHIP_V2_SHADOW_ENABLED": "true",
            "BNL_UNIFIED_INTELLIGENCE_PACKET_SHADOW_ENABLED": "true",
            "BNL_UNIFIED_RESPONSE_ASSESSMENT_SHADOW_ENABLED": "true",
            "BNL_MEMORY_GOVERNANCE_CANARY_ENABLED": "true",
            "BNL_MEMORY_GOVERNANCE_CANARY_GUILD_IDS": "1",
            "BNL_MEMORY_GOVERNANCE_CANARY_USER_IDS": str(int(user_id)),
            "BNL_SHARED_BRAIN_SYNTHESIS_CANARY_ENABLED": "true",
            "BNL_SHARED_BRAIN_SYNTHESIS_CANARY_GUILD_IDS": "1",
            "BNL_SHARED_BRAIN_SYNTHESIS_CANARY_USER_IDS": str(int(user_id)),
            "BNL_SHARED_BRAIN_SYNTHESIS_CANARY_CHANNEL_IDS": "10",
            "BNL_MEMORY_GOVERNANCE_LIVE_ENABLED": "false",
            "BNL_RELATIONSHIP_V2_LIVE_ENABLED": "false",
            "BNL_ACTIVE_ENGAGEMENT_V2_LIVE_ENABLED": "false",
        }

    def build_composition_prompt(self, *, user_id, user_name, wording):
        metadata = {}
        prompt, _allow_greeting, _style = (
            bnl01_bot.build_user_aware_prompt(
                int(user_id),
                1,
                str(user_name),
                str(wording),
                channel_name="barcode-bot",
                channel_policy="public_home",
                route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                is_direct_interaction=True,
                prompt_metadata=metadata,
                channel_id=10,
            )
        )
        return prompt, metadata

    def composition_plan(self, wording):
        return bnl01_bot.plan_conversation_response(
            str(wording),
            "public_home",
            route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
            real_direct_target=True,
            batching_enabled=False,
            conversation_surface=(
                bnl01_bot.CONVERSATION_SURFACE_MENTION_OR_REPLY
            ),
        )

    async def send_composition_response(
        self,
        *,
        message,
        wording,
        baseline,
        prompt,
        metadata,
    ):
        return await bnl01_bot.send_planned_conversation_response(
            message,
            baseline,
            self.composition_plan(wording),
            prompt=prompt,
            source_context_available=metadata.get(
                "source_context_available",
                False,
            ),
            allow_model_save=False,
            mark_recent_direct=False,
            conversation_continuity_required=metadata.get(
                "conversation_continuity_required",
                False,
            ),
            community_visual_basis=metadata.get(
                "community_visual_basis"
            ),
            exact_quote_requested=metadata.get(
                "exact_quote_requested",
                False,
            ),
            exact_quote_authority=metadata.get(
                "exact_quote_authority"
            ),
            third_party_attribution_requested=metadata.get(
                "third_party_attribution_requested",
                False,
            ),
            prompt_source_bases=metadata.get(
                "prompt_source_bases",
                (),
            ),
            unified_response_assessment_shadow=metadata.get(
                "unified_response_assessment_shadow"
            ),
            shared_brain_synthesis_canary_basis=metadata.get(
                "shared_brain_synthesis_canary_basis"
            ),
        )

    def assert_grounded_candidate(
        self,
        *,
        user_id,
        user_name,
        wording,
        candidate,
        expected_status,
        expected_points,
    ):
        prepared = prepare_memory_preview(
            self.request(
                user_id=user_id,
                user_name=user_name,
                wording=wording,
            )
        )
        try:
            self.assertTrue(prepared.ready)
            self.assertEqual(
                prepared.diagnostics.route_status,
                "matched",
            )
            self.assertEqual(
                prepared.diagnostics.profile_status,
                expected_status,
            )
            self.assertGreaterEqual(
                prepared.diagnostics.profile_selected_point_count,
                expected_points,
            )
            self.assertGreaterEqual(
                prepared.diagnostics.profile_independent_root_count,
                expected_points,
            )
            self.assertGreaterEqual(
                prepared.diagnostics.profile_independent_occurrence_count,
                expected_points,
            )
            evaluation = evaluate_memory_preview(
                prepared,
                baseline_response=(
                    "I only have a narrow grounded view so far."
                ),
                candidate_response=candidate,
            )
            self.assertTrue(
                evaluation.candidate_selected,
                evaluation.fallback_reason,
            )
            self.assertGreaterEqual(
                evaluation.candidate_member_point_count,
                expected_points,
            )
            self.assertNotIn(
                "operational profile",
                evaluation.response.lower(),
            )
            self.assertNotIn(
                "entity parameters",
                evaluation.response.lower(),
            )
            return prepared
        except Exception:
            prepared.close()
            raise

    def test_four_named_regression_shapes_reach_one_grounded_route(self):
        cases = (
            (
                101,
                "Chris",
                "BNL, what am I all about?",
                (
                    (
                        "I keep developing Digi-Screwed music tracks.",
                        "The Digi-Screwed song mix needs another pass.",
                    ),
                    (
                        "I keep bringing dad jokes to the community.",
                        "Another joke had the whole community laughing.",
                    ),
                ),
                (
                    "Digi-Screwed tracks and the song-mix work keep "
                    "showing up in your music production, while your dad "
                    "jokes keep driving the community banter."
                ),
            ),
            (
                102,
                "SlayGoat",
                "B what do you know about me?",
                (
                    (
                        "I keep making art-pop songs with dynamic vocals.",
                        "The synth music track needs another vocal pass.",
                    ),
                    (
                        "I keep sharing jokes when the community chats.",
                        "Another funny joke had the group laughing.",
                    ),
                ),
                (
                    "Your art-pop songs, dynamic vocals, and synth-track "
                    "passes form a clear music production thread; the "
                    "jokes you share in community chats form another "
                    "community banter thread."
                ),
            ),
            (
                103,
                "WittyFox",
                "hey bud, what do you remember about me?",
                (
                    (
                        "I keep making comedy songs instead of generic tracks.",
                        "The funny music track needs another mix.",
                    ),
                    (
                        "I keep asking other players about Diablo games.",
                        "The game players were comparing battle classes.",
                    ),
                ),
                (
                    "You keep returning to comedy songs and the mix on "
                    "funny music tracks, alongside Diablo conversations "
                    "about players and battle classes."
                ),
            ),
            (
                104,
                "LostMarbles",
                "<@123>, what am I all about?",
                (
                    (
                        "I keep collaborating on music with the community.",
                        "The community music collab needs another track.",
                    ),
                    (
                        "I keep bringing jokes to the group chat.",
                        "Another funny joke had the group laughing.",
                    ),
                ),
                (
                    "You keep collaborating with the community on music "
                    "and the next track, while your jokes and banter "
                    "regularly get the group laughing."
                ),
            ),
        )
        prepared_cases = []
        try:
            for offset, (
                user_id,
                name,
                wording,
                messages,
                response,
            ) in enumerate(cases):
                for topic_index, topic_messages in enumerate(messages):
                    self.add_recurring_topic(
                        user_id=user_id,
                        user_name=name,
                        first=topic_messages[0],
                        second=topic_messages[1],
                        day_offset=(offset * 2) + topic_index,
                    )
                prepared_cases.append(
                    self.assert_grounded_candidate(
                        user_id=user_id,
                        user_name=name,
                        wording=wording,
                        candidate=response,
                        expected_status="rich",
                        expected_points=2,
                    )
                )
        finally:
            for prepared in prepared_cases:
                prepared.close()

    def test_rich_profile_rejects_category_only_candidate(self):
        self.add_recurring_topic(
            user_id=101,
            user_name="Chris",
            first="I keep developing Digi-Screwed music tracks.",
            second="The Digi-Screwed song mix needs another pass.",
        )
        self.add_recurring_topic(
            user_id=101,
            user_name="Chris",
            first="I keep bringing dad jokes to the community.",
            second="Another joke had the whole community laughing.",
            day_offset=2,
        )
        prepared = prepare_memory_preview(
            self.request(
                user_id=101,
                user_name="Chris",
                wording="BNL, what am I all about?",
            )
        )
        try:
            prompt = prepared.packet_owned_prompt.prompt
            self.assertIn("Digi-Screwed", prompt)
            self.assertIn("dad jokes", prompt)
            evaluation = evaluate_memory_preview(
                prepared,
                baseline_response=(
                    "I only have a narrow grounded view so far."
                ),
                candidate_response=(
                    "Music and audio production keep showing up in what "
                    "you work on, and jokes and community banter are "
                    "another recurring public thread."
                ),
            )
            self.assertFalse(evaluation.candidate_selected)
            self.assertEqual(
                evaluation.fallback_reason,
                "candidate_member_details_insufficient",
            )
        finally:
            prepared.close()

    def test_broad_profile_keeps_all_six_bounded_recurrent_points(self):
        user_id = 102
        user_name = "Test Member"
        topics = (
            (
                "I keep producing synth music and mixing audio tracks.",
                "The song vocals and drum mix need another pass.",
            ),
            (
                "I keep designing visual artwork and animation sprites.",
                "The image style and banner design need another pass.",
            ),
            (
                "I keep debugging bot code and testing deployments.",
                "The website system still has another bug to fix.",
            ),
            (
                "I keep organizing community collaboration in Discord.",
                "The server team has another shared project to coordinate.",
            ),
            (
                "I keep writing lore and shaping the worldbuilding.",
                "The story narrative and canon need another writing pass.",
            ),
            (
                "I keep balancing game battles and creature cards.",
                "The player classes and monster levels need another pass.",
            ),
        )
        for index, (first, second) in enumerate(topics):
            self.add_recurring_topic(
                user_id=user_id,
                user_name=user_name,
                first=first,
                second=second,
                day_offset=index,
            )

        prepared = prepare_memory_preview(
            self.request(
                user_id=user_id,
                user_name=user_name,
                wording="BNL-01, what am I all about?",
            )
        )
        try:
            funnel = dict(prepared.diagnostics.source_funnel_counts)
            self.assertEqual(funnel["motif_candidates_returned"], 6)
            atomic_items = tuple(
                item
                for item in prepared.packet.items
                if item.lane == "atomic_knowledge"
            )
            self.assertEqual(len(atomic_items), 6)
            self.assertEqual(
                prepared.packet.profile_sufficiency.candidate_point_count,
                6,
            )
            self.assertEqual(
                prepared.packet.profile_sufficiency.selected_point_count,
                6,
            )
            self.assertEqual(
                prepared.diagnostics.profile_candidate_point_count,
                6,
            )
            self.assertEqual(
                dict(prepared.basis.rendered_lane_counts).get(
                    "atomic_knowledge",
                    0,
                ),
                6,
            )
            self.assertFalse(
                any(
                    exclusion.lane == "atomic_knowledge"
                    and exclusion.reason == "lane_cap"
                    for exclusion in prepared.packet.exclusions
                ),
            )
        finally:
            prepared.close()

    def test_work_and_decision_profile_uses_full_pool_and_rejects_icon_role(
        self,
    ):
        user_id = 112
        user_name = "DecisionMember"
        process_rows = (
            (
                "I want to compare both approaches before choosing one.",
                "2026-07-20T12:00:00+00:00",
            ),
            (
                "Let's test the smaller version and check the result first.",
                "2026-07-22T12:00:00+00:00",
            ),
            (
                "We should revise one piece at a time before deciding.",
                "2026-07-24T12:00:00+00:00",
            ),
        )
        for content, observed_at in process_rows:
            self.add_raw_message(
                user_id=user_id,
                user_name=user_name,
                content=content,
                observed_at=observed_at,
            )
        self.add_recurring_topic(
            user_id=user_id,
            user_name=user_name,
            first="I keep producing synth music and mixing audio tracks.",
            second="The song vocals and drum mix need another pass.",
            day_offset=0,
        )
        self.add_raw_message(
            user_id=user_id,
            user_name=user_name,
            content="The synth track still needs a careful audio mix.",
            observed_at="2026-07-22T20:00:00+00:00",
        )
        self.add_recurring_topic(
            user_id=user_id,
            user_name=user_name,
            first=(
                "I want custom character emotes for Modem and "
                "Floppydisc."
            ),
            second=(
                "The Modem and Floppydisc icons need another visual "
                "design pass."
            ),
            day_offset=3,
        )
        self.add_raw_message(
            user_id=user_id,
            user_name=user_name,
            content=(
                "The custom character emotes need one more artwork "
                "review."
            ),
            observed_at="2026-07-25T20:00:00+00:00",
        )
        wording = (
            "What have you learned about how I work and make decisions?"
        )
        prepared = prepare_memory_preview(
            self.request(
                user_id=user_id,
                user_name=user_name,
                wording=wording,
            )
        )
        try:
            self.assertTrue(prepared.ready)
            self.assertGreaterEqual(
                prepared.diagnostics.assessment_pool_eligible_count,
                7,
            )
            self.assertEqual(
                prepared.diagnostics.assessment_pool_selected_count,
                4,
            )
            self.assertEqual(
                dict(prepared.basis.rendered_lane_counts).get(
                    "assessment_observation",
                    0,
                ),
                4,
            )
            prompt = prepared.packet_owned_prompt.prompt
            self.assertIn("compare both approaches", prompt)
            self.assertIn("test the smaller version", prompt)
            self.assertIn(
                "An inventory of projects, interests, or community "
                "activities does not answer this question.",
                prompt,
            )

            activity_inventory = evaluate_memory_preview(
                prepared,
                baseline_response=(
                    "I only have a narrow grounded view so far."
                ),
                candidate_response=(
                    "Testing the smaller version, checking the result, "
                    "and revising one piece at a time are recurring "
                    "activities in your public history."
                ),
            )
            self.assertFalse(activity_inventory.candidate_selected)
            self.assertEqual(
                activity_inventory.fallback_reason,
                "candidate_request_angle_missed",
            )

            grounded_process = evaluate_memory_preview(
                prepared,
                baseline_response=(
                    "I only have a narrow grounded view so far."
                ),
                candidate_response=(
                    "My read is that you make decisions by comparing "
                    "approaches, testing a smaller version, checking the "
                    "result, and then choosing. My read is that the "
                    "pattern is iterative rather than impulsive."
                ),
            )
            self.assertTrue(
                grounded_process.candidate_selected,
                grounded_process.fallback_reason,
            )

            invented_icon_role = evaluate_memory_preview(
                prepared,
                baseline_response=(
                    "I only have a narrow grounded view so far."
                ),
                candidate_response=(
                    "My read is that you make decisions by comparing "
                    "approaches, testing a smaller version, and checking "
                    "the result before choosing. You organize character "
                    "icons like Modem and Floppydisc."
                ),
            )
            self.assertFalse(invented_icon_role.candidate_selected)
            self.assertEqual(
                invented_icon_role.fallback_reason,
                "candidate_claims_ungrounded",
            )
            self.assertEqual(
                invented_icon_role
                .candidate_unsupported_factual_claim_count,
                1,
            )
        finally:
            prepared.close()

    def test_lostmarbles_never_inherits_wittyfox_evidence(self):
        self.add_recurring_topic(
            user_id=103,
            user_name="WittyFox",
            first="I keep refining character art and visual design.",
            second="The artwork needs a stronger animation style.",
        )
        self.add_recurring_topic(
            user_id=104,
            user_name="LostMarbles",
            first="I keep debugging the bot code and memory system.",
            second="The website code needs another careful test.",
            day_offset=2,
        )
        prepared = prepare_memory_preview(
            self.request(
                user_id=104,
                user_name="LostMarbles",
                wording="BNL, what am I all about?",
            )
        )
        try:
            prompt = prepared.packet_owned_prompt.prompt.lower()
            self.assertIn(
                "software and technical systems",
                prompt,
            )
            self.assertNotIn(
                "art and visual design",
                prompt,
            )
            self.assertNotIn("wittyfox", prompt)
            self.assertEqual(
                {
                    item.subject_key
                    for item in prepared.packet.items
                    if item.lane
                    in {"approved_fact", "atomic_knowledge", "moment"}
                },
                {"discord_user:104"},
            )
        finally:
            prepared.close()

    def test_owner_profile_is_member_first_with_additive_canon(self):
        self.add_recurring_topic(
            user_id=105,
            user_name="6 Bit",
            first="I keep debugging the bot code and memory system.",
            second="The website code needs another careful test.",
        )
        self.add_recurring_topic(
            user_id=105,
            user_name="6 Bit",
            first="I keep writing songs and producing synth tracks.",
            second="The music mix needs another production pass.",
            day_offset=2,
        )
        prepared = self.assert_grounded_candidate(
            user_id=105,
            user_name="6 Bit",
            wording=(
                "BNL, what am I all about in the BARCODE project?"
            ),
            candidate=(
                "You keep debugging the bot memory system and carefully "
                "testing the website, while writing songs and producing "
                "synth tracks. As a founding BARCODE member, that puts "
                "your software and music work inside the Network's "
                "community and live-broadcast signal."
            ),
            expected_status="rich",
            expected_points=2,
        )
        try:
            self.assertTrue(prepared.basis.profile_requires_canon)
            rendered = prepared.basis.rendered_context
            self.assertIn("durable observation", rendered)
            self.assertIn("approved canon", rendered)
            self.assertLess(
                rendered.index("durable observation"),
                rendered.index("approved canon"),
            )
            self.assertGreaterEqual(
                dict(
                    prepared.basis.rendered_lane_counts
                ).get("atomic_knowledge", 0),
                2,
            )
            memory_only = evaluate_memory_preview(
                prepared,
                baseline_response=(
                    "I only have a narrow grounded view so far."
                ),
                candidate_response=(
                    "You keep debugging the bot memory system and testing "
                    "the website, while writing songs and producing synth "
                    "tracks."
                ),
            )
            self.assertFalse(memory_only.candidate_selected)
            self.assertEqual(
                memory_only.fallback_reason,
                "candidate_project_canon_missing",
            )
        finally:
            prepared.close()

    def test_sparse_empty_and_ambiguous_profiles_do_not_invent_breadth(self):
        self.add_recurring_topic(
            user_id=106,
            user_name="SparseMember",
            first="I keep cooking dinner and testing new recipes.",
            second="The pizza recipe needs another baking pass.",
        )
        sparse = self.assert_grounded_candidate(
            user_id=106,
            user_name="SparseMember",
            wording="What am I all about?",
            candidate=(
                "Food and cooking are the one recurring public thread "
                "I can ground so far."
            ),
            expected_status="sparse",
            expected_points=1,
        )
        sparse.close()

        empty = prepare_memory_preview(
            self.request(
                user_id=107,
                user_name="EmptyMember",
                wording="What am I all about?",
            )
        )
        try:
            self.assertFalse(empty.ready)
            self.assertEqual(
                empty.diagnostics.profile_status,
                "empty",
            )
            self.assertFalse(
                empty.diagnostics.profile_satisfied
            )
            self.assertIn(
                "no_supported_member_evidence",
                empty.diagnostics.profile_reason_codes,
            )
        finally:
            empty.close()

        ambiguous = prepare_memory_preview(
            self.request(
                user_id=107,
                user_name="EmptyMember",
                wording="What do you remember?",
            )
        )
        self.assertIsNone(ambiguous.connection)
        self.assertEqual(
            ambiguous.diagnostics.route_status,
            "needs_context",
        )
        self.assertEqual(
            ambiguous.diagnostics.route_reason,
            "recall_target_ambiguous",
        )

    def test_real_prompt_to_send_records_packet_and_synthesis_live_receipts(
        self,
    ):
        user_id = 109
        wording = "BNL, what am I all about?"
        candidate_response = (
            "You keep debugging bot code and the memory system, including "
            "careful website testing. You also keep producing synth music, "
            "writing songs, and working on the tracks' vocal mix."
        )
        baseline_response = "I only have a narrow grounded view so far."
        message = _ProductionMessage(
            user_id=user_id,
            user_name="CompositionMember",
            content=wording,
        )

        with mock.patch.dict(
            os.environ,
            self.composition_env(user_id=user_id),
            clear=False,
        ):
            bnl01_bot.init_db()
            self.add_recurring_topic(
                user_id=user_id,
                user_name="CompositionMember",
                first="I keep debugging the bot code and memory system.",
                second="The website code needs another careful test.",
            )
            self.add_recurring_topic(
                user_id=user_id,
                user_name="CompositionMember",
                first="I keep producing synth music and writing songs.",
                second="The music track needs another vocal mix.",
                day_offset=2,
            )
            prompt, metadata = self.build_composition_prompt(
                user_id=user_id,
                user_name="CompositionMember",
                wording=wording,
            )
            basis = metadata.get(
                "shared_brain_synthesis_canary_basis"
            )
            self.assertIsNotNone(basis)
            self.assertGreaterEqual(
                sum(
                    1
                    for item in basis.packet.items
                    if item.lane == "atomic_knowledge"
                ),
                2,
            )
            provider = mock.AsyncMock(
                return_value=candidate_response
            )
            with mock.patch.object(
                bnl01_bot,
                "get_gemini_response_with_optional_typing",
                new=provider,
            ):
                asyncio.run(
                    self.send_composition_response(
                        message=message,
                        wording=wording,
                        baseline=baseline_response,
                        prompt=prompt,
                        metadata=metadata,
                    )
                )

            self.assertEqual(message.replies, [candidate_response])
            self.assertTrue(
                any(
                    call.kwargs.get("route")
                    == "shared_brain_synthesis_canary"
                    for call in provider.await_args_list
                )
            )
            candidate_prompt = next(
                call.args[1]
                for call in provider.await_args_list
                if call.kwargs.get("route")
                == "shared_brain_synthesis_canary"
            )
            self.assertIn(basis.rendered_context, candidate_prompt)
            for competing_context in basis.competing_factual_contexts:
                self.assertIn(competing_context, prompt)
                self.assertNotIn(competing_context, candidate_prompt)
            for live_gate in (
                "BNL_MEMORY_GOVERNANCE_LIVE_ENABLED",
                "BNL_RELATIONSHIP_V2_LIVE_ENABLED",
                "BNL_ACTIVE_ENGAGEMENT_V2_LIVE_ENABLED",
            ):
                self.assertEqual(
                    os.environ.get(live_gate),
                    "false",
                )

            with sqlite3.connect(self.db_path) as conn:
                synthesis = conn.execute(
                    """
                    SELECT packet_run_id,candidate_generated,
                           candidate_selected,live_applied,response_sent,
                           guard_status,revalidation_status,fallback_reason
                    FROM memory_governance_shared_brain_synthesis_runs
                    ORDER BY rowid DESC
                    LIMIT 1
                    """
                ).fetchone()
                self.assertIsNotNone(synthesis)
                self.assertEqual(synthesis[1:6], (1, 1, 1, 1, "candidate_sent"))
                self.assertTrue(
                    str(synthesis[6] or "").startswith("passed")
                )
                self.assertEqual(synthesis[7], "")
                packet = conn.execute(
                    """
                    SELECT prompt_applied,live_applied
                    FROM memory_governance_intelligence_packet_runs
                    WHERE run_id=?
                    """,
                    (synthesis[0],),
                ).fetchone()
                self.assertEqual(packet, (1, 1))

    def test_source_change_during_candidate_generation_fails_closed(
        self,
    ):
        cases = (
            ("clear_user_history", 110, "DeletedMember"),
            ("privacy_change", 111, "PrivateMember"),
        )
        wording = "BNL, what am I all about?"
        candidate_response = (
            "Software and technical systems are the recurring public "
            "thread in your history."
        )
        baseline_response = (
            "I only have a narrow grounded view so far."
        )
        bnl01_bot.init_db()

        for mutation_kind, user_id, user_name in cases:
            with self.subTest(mutation=mutation_kind):
                with mock.patch.dict(
                    os.environ,
                    self.composition_env(user_id=user_id),
                    clear=False,
                ):
                    self.add_recurring_topic(
                        user_id=user_id,
                        user_name=user_name,
                        first=(
                            "I keep debugging the bot code and memory "
                            "system."
                        ),
                        second=(
                            "The website code needs another careful test."
                        ),
                    )
                    prompt, metadata = self.build_composition_prompt(
                        user_id=user_id,
                        user_name=user_name,
                        wording=wording,
                    )
                    basis = metadata.get(
                        "shared_brain_synthesis_canary_basis"
                    )
                    self.assertIsNotNone(basis)
                    candidate_id = next(
                        item.revalidation_key
                        for item in basis.packet.items
                        if item.lane == "atomic_knowledge"
                    )
                    with sqlite3.connect(self.db_path) as conn:
                        before_run_rowid = int(
                            conn.execute(
                                """
                                SELECT COALESCE(MAX(rowid),0)
                                FROM memory_governance_shared_brain_synthesis_runs
                                """
                            ).fetchone()[0]
                            or 0
                        )

                    mutated = False

                    async def provider(
                        _channel,
                        _prompt,
                        _user_id,
                        _guild_id,
                        **kwargs,
                    ):
                        nonlocal mutated
                        if (
                            kwargs.get("route")
                            == "shared_brain_synthesis_canary"
                            and not mutated
                        ):
                            mutated = True
                            if mutation_kind == "clear_user_history":
                                self.assertGreater(
                                    bnl01_bot.clear_user_history(
                                        user_id,
                                        1,
                                    ),
                                    0,
                                )
                            else:
                                with sqlite3.connect(
                                    self.db_path
                                ) as conn:
                                    changed = conn.execute(
                                        """
                                        UPDATE memory_ledger_entries
                                        SET visibility='private',
                                            public_usable=0
                                        WHERE guild_id=1
                                          AND subject_key=?
                                          AND source_table='conversations'
                                          AND source_role='user'
                                        """,
                                        (
                                            ledger.subject_key_for_user(
                                                user_id
                                            ),
                                        ),
                                    ).rowcount
                                    conn.commit()
                                self.assertGreaterEqual(changed, 2)
                            return candidate_response
                        return (
                            "I don't have enough eligible history left "
                            "to describe a recurring topic."
                        )

                    message = _ProductionMessage(
                        user_id=user_id,
                        user_name=user_name,
                        content=wording,
                    )
                    with mock.patch.object(
                        bnl01_bot,
                        "get_gemini_response_with_optional_typing",
                        new=mock.AsyncMock(side_effect=provider),
                    ):
                        asyncio.run(
                            self.send_composition_response(
                                message=message,
                                wording=wording,
                                baseline=baseline_response,
                                prompt=prompt,
                                metadata=metadata,
                            )
                        )

                    self.assertTrue(mutated)
                    self.assertNotIn(
                        candidate_response,
                        message.replies,
                    )
                    self.assertEqual(
                        bnl01_bot.prompt_source_basis_failure((basis,)),
                        "shared_brain_synthesis_source_changed",
                    )
                    with sqlite3.connect(self.db_path) as conn:
                        synthesis = conn.execute(
                            """
                            SELECT packet_run_id,candidate_generated,
                                   candidate_selected,live_applied,
                                   revalidation_status,fallback_reason
                            FROM memory_governance_shared_brain_synthesis_runs
                            WHERE rowid>?
                            ORDER BY rowid DESC
                            LIMIT 1
                            """,
                            (before_run_rowid,),
                        ).fetchone()
                        self.assertIsNotNone(synthesis)
                        self.assertEqual(synthesis[1:4], (1, 0, 0))
                        self.assertEqual(
                            synthesis[4],
                            "source_changed",
                        )
                        self.assertEqual(
                            synthesis[5],
                            "post_generation_source_changed",
                        )
                        packet = conn.execute(
                            """
                            SELECT prompt_applied,live_applied
                            FROM memory_governance_intelligence_packet_runs
                            WHERE run_id=?
                            """,
                            (synthesis[0],),
                        ).fetchone()
                        self.assertEqual(packet, (1, 0))

                        if mutation_kind == "clear_user_history":
                            self.assertEqual(
                                conn.execute(
                                    """
                                    SELECT candidate_state,
                                           candidate_eligible,
                                           invalidated_reason
                                    FROM memory_ledger_knowledge_candidates
                                    WHERE candidate_id=?
                                    """,
                                    (candidate_id,),
                                ).fetchone(),
                                ("invalidated", 0, "root_deleted"),
                            )
                            self.assertEqual(
                                conn.execute(
                                    """
                                    SELECT COUNT(*) FROM conversations
                                    WHERE guild_id=1 AND user_id=?
                                    """,
                                    (user_id,),
                                ).fetchone()[0],
                                0,
                            )
                            self.assertEqual(
                                {
                                    str(row[0])
                                    for row in conn.execute(
                                        """
                                        SELECT root_status
                                        FROM memory_ledger_knowledge_roots
                                        WHERE candidate_id=?
                                        """,
                                        (candidate_id,),
                                    )
                                },
                                {"deleted"},
                            )
                        else:
                            invalidated = conn.execute(
                                """
                                SELECT candidate_state,candidate_eligible,
                                       invalidated_reason
                                FROM memory_ledger_knowledge_candidates
                                WHERE candidate_id=?
                                """,
                                (candidate_id,),
                            ).fetchone()
                            self.assertEqual(
                                invalidated,
                                (
                                    "invalidated",
                                    0,
                                    "root_privacy_or_provenance_changed",
                                ),
                            )

                    _rebuilt_prompt, rebuilt_metadata = (
                        self.build_composition_prompt(
                            user_id=user_id,
                            user_name=user_name,
                            wording=wording,
                        )
                    )
                    rebuilt_basis = rebuilt_metadata.get(
                        "shared_brain_synthesis_canary_basis"
                    )
                    if rebuilt_basis is not None:
                        self.assertFalse(
                            any(
                                item.lane == "atomic_knowledge"
                                and item.revalidation_key == candidate_id
                                for item in rebuilt_basis.packet.items
                            )
                        )
                    with sqlite3.connect(self.db_path) as conn:
                        rebuilt_packet = conn.execute(
                            """
                            SELECT selected_lane_counts_json,
                                   excluded_by_reason_json
                            FROM memory_governance_intelligence_packet_runs
                            ORDER BY rowid DESC
                            LIMIT 1
                            """
                        ).fetchone()
                    selected_lanes = json.loads(rebuilt_packet[0])
                    exclusions = json.loads(rebuilt_packet[1])
                    self.assertEqual(
                        int(selected_lanes.get("atomic_knowledge", 0)),
                        0,
                    )
                    self.assertGreaterEqual(
                        int(exclusions.get("atomic_state", 0)),
                        1,
                    )

    def test_raw_write_hook_is_idempotent_and_does_not_form_from_one_turn(self):
        first_row = self.add_raw_message(
            user_id=108,
            user_name="ReplayMember",
            content="I keep building community projects with the team.",
            observed_at="2026-07-20T18:00:00+00:00",
        )
        with sqlite3.connect(self.db_path) as conn:
            self.assertEqual(
                conn.execute(
                    """
                    SELECT COUNT(*)
                    FROM memory_ledger_knowledge_candidates
                    WHERE subject_key='discord_user:108'
                    """
                ).fetchone()[0],
                0,
            )
        self.add_raw_message(
            user_id=108,
            user_name="ReplayMember",
            content="The Discord community project needs collaboration.",
            observed_at="2026-07-21T18:00:00+00:00",
        )
        with sqlite3.connect(self.db_path) as conn:
            before = conn.execute(
                """
                SELECT candidate_id,reinforcement_count,
                       eligible_independent_root_count
                FROM memory_ledger_knowledge_candidates
                WHERE subject_key='discord_user:108'
                """
            ).fetchall()
            first_entry = conn.execute(
                """
                SELECT entry_id FROM memory_ledger_entries
                WHERE source_table='conversations' AND source_row_id=?
                """,
                (str(first_row),),
            ).fetchone()[0]
            replay = ledger.form_atomic_candidates_from_recurring_conversation(
                conn,
                trigger_entry_id=first_entry,
                environ={
                    ledger.MEMORY_LEDGER_SHADOW_ENV: "true",
                    ledger.CONVERSATION_MOTIF_FORMATION_ENV: "true",
                },
            )
            conn.commit()
            after = conn.execute(
                """
                SELECT candidate_id,reinforcement_count,
                       eligible_independent_root_count
                FROM memory_ledger_knowledge_candidates
                WHERE subject_key='discord_user:108'
                """
            ).fetchall()
        self.assertEqual(len(before), 1)
        self.assertEqual(before, after)
        self.assertTrue(
            all(
                result.outcome == "matched_existing"
                for result in replay
            )
        )


if __name__ == "__main__":
    unittest.main()
