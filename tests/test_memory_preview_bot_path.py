import hashlib
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest import mock
from types import SimpleNamespace

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

from bnl_canon_source_contract import (
    Confidence,
    SourceClass,
    Visibility,
)
import bnl_memory_ledger as ledger
import bnl01_bot


class MemoryPreviewBotPathTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = str(
            Path(self.tempdir.name) / "production-memory.db"
        )
        with sqlite3.connect(self.db_path) as conn:
            ledger.ensure_memory_ledger_schema(conn)
            self._add_message(
                conn,
                1,
                "I keep fixing the bot code and memory system carefully.",
                "2026-07-24T20:00:00+00:00",
            )
            self._add_message(
                conn,
                2,
                "The website code needs another troubleshooting pass.",
                "2026-07-25T20:00:00+00:00",
            )
            conn.commit()

    def tearDown(self):
        self.tempdir.cleanup()

    def _add_message(self, conn, row_id, text, observed_at):
        ledger.insert_ledger_entry(
            conn,
            ledger.LedgerEntry(
                guild_id=1,
                source_table="conversations",
                source_row_id=row_id,
                source_revision=str(row_id),
                source_role="user",
                entry_type="observation",
                subject_key="discord_user:7",
                subject_display_name="Crow",
                predicate_key="conversation",
                value=text,
                source_class=SourceClass.PUBLIC_OBSERVATION,
                route_mode="normal_chat",
                channel_id=10,
                channel_name="barcode-bot",
                channel_policy="public_home",
                visibility=Visibility.PUBLIC,
                confidence=Confidence.MEDIUM,
                public_usable=True,
                observed_at=observed_at,
                source_sequence=row_id,
                lifecycle_status="active",
                participants=(
                    ledger.LedgerParticipant(
                        "discord_user:7",
                        "Crow",
                        "author",
                        0,
                    ),
                ),
            ),
        )

    def _source_hash(self):
        return hashlib.sha256(
            Path(self.db_path).read_bytes()
        ).hexdigest()

    def test_partial_clone_reads_do_not_hide_live_database_errors(self):
        with sqlite3.connect(":memory:") as clone:
            self.assertIsNone(
                bnl01_bot.get_relationship_state(
                    7,
                    1,
                    connection=clone,
                )
            )
            self.assertEqual(
                bnl01_bot.get_memory_tiers(
                    7,
                    1,
                    connection=clone,
                ),
                [],
            )

        empty_live_path = str(
            Path(self.tempdir.name) / "empty-live.db"
        )
        with mock.patch.object(
            bnl01_bot,
            "DB_FILE",
            empty_live_path,
        ):
            with self.assertRaises(sqlite3.DatabaseError):
                bnl01_bot.get_relationship_state(7, 1)

    async def test_full_preview_uses_packet_prompt_and_leaves_source_unchanged(
        self,
    ):
        source_hash = self._source_hash()
        calls = []

        async def generator(prompt, route):
            calls.append((route, prompt))
            if route.endswith("baseline"):
                return "I only have a narrow grounded view so far."
            return (
                "You keep returning to software and technical "
                "systems."
            )

        guard = mock.AsyncMock(
            side_effect=lambda response, _prompt: (
                response,
                {"suppressed": False, "suppression_reason": ""},
            )
        )
        result = await bnl01_bot.execute_bnl_memory_preview(
            source_db_path=self.db_path,
            guild_id=1,
            subject_user_id=7,
            subject_display_name="Crow",
            simulated_channel_id=10,
            wording="BNL-01, what am I all about?",
            generator=generator,
            guard=guard,
        )

        self.assertTrue(result.candidate_selected)
        self.assertIn("software", result.proposed_response)
        self.assertEqual([route for route, _prompt in calls], [
            "bnl_memory_preview_baseline",
            "bnl_memory_preview_candidate",
        ])
        baseline_prompt = calls[0][1]
        candidate_prompt = calls[1][1]
        self.assertNotIn(
            bnl01_bot.PREVIEW_FACTUAL_PLACEHOLDER,
            baseline_prompt,
        )
        self.assertIn("No durable memory yet.", baseline_prompt)
        self.assertIn(
            "do not infer that the member is new, quiet, inactive",
            baseline_prompt,
        )
        self.assertNotIn(
            bnl01_bot.PREVIEW_FACTUAL_PLACEHOLDER,
            candidate_prompt,
        )
        self.assertIn("Grounded response evidence", candidate_prompt)
        self.assertIn("Mode contract: normal_chat", candidate_prompt)
        self.assertIn("Current channel policy: public_home", candidate_prompt)
        self.assertEqual(guard.await_count, 1)
        self.assertEqual(
            result.established_response,
            "I only have a narrow grounded view so far.",
        )
        self.assertEqual(
            result.packet_candidate_response,
            (
                "You keep returning to software and technical "
                "systems."
            ),
        )
        self.assertEqual(result.repair_response, "")
        self.assertEqual(result.final_selection, "packet_candidate")
        self.assertEqual(source_hash, self._source_hash())
        self.assertIn(
            "source_db_read_only=true",
            "\n".join(result.diagnostics),
        )

    async def test_rich_preview_repairs_one_category_only_draft(self):
        with sqlite3.connect(self.db_path) as conn:
            self._add_message(
                conn,
                3,
                "I keep composing synth songs and producing music tracks.",
                "2026-07-25T21:00:00+00:00",
            )
            self._add_message(
                conn,
                4,
                "The synth song mix needs another production pass.",
                "2026-07-26T20:00:00+00:00",
            )
            conn.commit()
        source_hash = self._source_hash()
        calls = []

        async def generator(prompt, route):
            calls.append((route, prompt))
            if route.endswith("baseline"):
                return "I only have a narrow grounded view so far."
            if route.endswith("candidate_repair"):
                return (
                    "You keep fixing the bot code and memory system, "
                    "including careful website troubleshooting. You also "
                    "compose synth songs and keep working their music mixes."
                )
            return (
                "Software and technical systems recur alongside music "
                "and audio production."
            )

        guard = mock.AsyncMock(
            side_effect=lambda response, _prompt: (
                response,
                {"suppressed": False, "suppression_reason": ""},
            )
        )
        result = await bnl01_bot.execute_bnl_memory_preview(
            source_db_path=self.db_path,
            guild_id=1,
            subject_user_id=7,
            subject_display_name="Crow",
            simulated_channel_id=10,
            wording="BNL-01, what am I all about?",
            generator=generator,
            guard=guard,
        )

        self.assertTrue(result.candidate_selected, result.fallback_reason)
        self.assertIn("bot code", result.proposed_response)
        self.assertIn("synth songs", result.proposed_response)
        self.assertEqual(
            [route for route, _prompt in calls],
            [
                "bnl_memory_preview_baseline",
                "bnl_memory_preview_candidate",
                "bnl_memory_preview_candidate_repair",
            ],
        )
        self.assertIn(
            "Grounded rewrite requirements",
            calls[-1][1],
        )
        self.assertEqual(guard.await_count, 1)
        self.assertEqual(source_hash, self._source_hash())

    async def test_preview_compares_real_established_memory_to_packet(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE memory_tiers (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  user_id INTEGER NOT NULL,
                  guild_id INTEGER NOT NULL,
                  tier TEXT NOT NULL,
                  summary TEXT NOT NULL,
                  salience REAL DEFAULT 0.5,
                  mentions INTEGER DEFAULT 1,
                  updated_at TEXT NOT NULL,
                  source_role TEXT DEFAULT 'user',
                  source_channel_policy TEXT DEFAULT 'public_home',
                  source_channel_name TEXT DEFAULT 'barcode-bot',
                  source_origin TEXT DEFAULT 'conversation',
                  source_trust TEXT DEFAULT 'source_safe_public',
                  topic_key TEXT DEFAULT '',
                  subject_key TEXT DEFAULT '',
                  project_key TEXT DEFAULT '',
                  first_seen TEXT DEFAULT '',
                  last_seen TEXT DEFAULT '',
                  lifecycle_note TEXT DEFAULT ''
                )
                """
            )
            conn.execute(
                """
                INSERT INTO memory_tiers(
                  user_id,guild_id,tier,summary,salience,mentions,
                  updated_at,source_role,source_channel_policy,
                  source_channel_name,source_origin,source_trust
                ) VALUES(7,1,'long',?,0.95,3,?,'user','public_home',
                         'barcode-bot','conversation','source_safe_public')
                """,
                (
                    "I build pocket synthesizers.",
                    "2026-07-25T20:00:00+00:00",
                ),
            )
            conn.commit()
        calls = []

        async def generator(prompt, route):
            calls.append((route, prompt))
            if route.endswith("baseline"):
                return "I remember your pocket-synthesizer work."
            return (
                "You keep returning to software and technical "
                "systems."
            )

        result = await bnl01_bot.execute_bnl_memory_preview(
            source_db_path=self.db_path,
            guild_id=1,
            subject_user_id=7,
            subject_display_name="Crow",
            simulated_channel_id=10,
            wording="BNL-01, what am I all about?",
            generator=generator,
            guard=mock.AsyncMock(
                side_effect=lambda response, _prompt: (
                    response,
                    {"suppressed": False, "suppression_reason": ""},
                )
            ),
        )

        baseline_prompt = calls[0][1]
        packet_prompt = calls[1][1]
        self.assertIn("I build pocket synthesizers.", baseline_prompt)
        self.assertIn("Derived memory summaries", baseline_prompt)
        self.assertNotIn("Derived memory summaries", packet_prompt)
        self.assertEqual(
            result.established_response,
            "I remember your pocket-synthesizer work.",
        )
        self.assertTrue(result.packet_candidate_response)

    async def test_preview_never_calls_conversation_or_model_savers(self):
        async def generator(_prompt, route):
            if route.endswith("baseline"):
                return "I only have a narrow grounded view so far."
            return (
                "You keep returning to software and technical systems."
            )

        async def guard(response, _prompt):
            return response, {
                "suppressed": False,
                "suppression_reason": "",
            }

        with (
            mock.patch.object(bnl01_bot, "save_user_message") as save_user,
            mock.patch.object(
                bnl01_bot,
                "save_model_message",
            ) as save_model,
        ):
            result = await bnl01_bot.execute_bnl_memory_preview(
                source_db_path=self.db_path,
                guild_id=1,
                subject_user_id=7,
                subject_display_name="Crow",
                simulated_channel_id=10,
                wording="What am I all about?",
                generator=generator,
                guard=guard,
            )

        self.assertTrue(result.proposed_response)
        save_user.assert_not_called()
        save_model.assert_not_called()

    async def test_post_guard_candidate_is_rechecked_before_preview(self):
        async def generator(_prompt, route):
            if route.endswith("baseline"):
                return "I only have a narrow grounded view so far."
            return (
                "You keep returning to software and technical "
                "systems."
            )

        guarded = []

        async def guard(response, _prompt):
            guarded.append(response)
            if len(guarded) == 1:
                return (
                    "A completely generic sentence with no grounded detail.",
                    {"suppressed": False, "suppression_reason": ""},
                )
            return response, {
                "suppressed": False,
                "suppression_reason": "",
            }

        result = await bnl01_bot.execute_bnl_memory_preview(
            source_db_path=self.db_path,
            guild_id=1,
            subject_user_id=7,
            subject_display_name="Crow",
            simulated_channel_id=10,
            wording="What am I all about?",
            generator=generator,
            guard=guard,
        )

        self.assertFalse(result.candidate_selected)
        self.assertEqual(
            result.fallback_reason,
            "candidate_evidence_ungrounded",
        )
        self.assertEqual(
            result.proposed_response,
            "I only have a narrow grounded view so far.",
        )
        self.assertEqual(len(guarded), 2)

    async def test_source_change_during_generation_fails_to_baseline(self):
        async def generator(_prompt, route):
            if route.endswith("baseline"):
                return "I only have a narrow grounded view so far."
            with sqlite3.connect(self.db_path) as source:
                self._add_message(
                    source,
                    3,
                    "I am still testing code and fixing system issues.",
                    "2026-07-26T11:00:00+00:00",
                )
                source.commit()
            return (
                "You keep returning to software and technical "
                "systems."
            )

        async def guard(response, _prompt):
            return response, {
                "suppressed": False,
                "suppression_reason": "",
            }

        result = await bnl01_bot.execute_bnl_memory_preview(
            source_db_path=self.db_path,
            guild_id=1,
            subject_user_id=7,
            subject_display_name="Crow",
            simulated_channel_id=10,
            wording="What am I all about?",
            generator=generator,
            guard=guard,
        )

        self.assertFalse(result.candidate_selected)
        self.assertEqual(result.stale_reason, "preview_source_changed")
        self.assertEqual(
            result.proposed_response,
            "I only have a narrow grounded view so far.",
        )

    async def test_default_guard_uses_public_home_policy_without_live_channel(self):
        async def generator(_prompt, route):
            if route.endswith("baseline"):
                return "I only have a narrow grounded view so far."
            return (
                "You keep returning to software and technical "
                "systems."
            )

        simulated_channel = SimpleNamespace(id=10, name="barcode-bot")
        guarded = mock.AsyncMock(
            side_effect=lambda response, **_kwargs: (
                response,
                {"suppressed": False, "suppression_reason": ""},
            )
        )
        with mock.patch.object(
            bnl01_bot,
            "apply_guarded_response_regeneration",
            new=guarded,
        ):
            result = await bnl01_bot.execute_bnl_memory_preview(
                source_db_path=self.db_path,
                guild_id=1,
                subject_user_id=7,
                subject_display_name="Crow",
                simulated_channel_id=10,
                wording="What am I all about?",
                simulated_channel=simulated_channel,
                generator=generator,
            )

        self.assertTrue(result.candidate_selected)
        guarded.assert_awaited_once()
        guard_call = guarded.await_args
        self.assertEqual(guard_call.kwargs["user_id"], 0)
        self.assertEqual(guard_call.kwargs["guild_id"], 1)
        self.assertEqual(
            guard_call.kwargs["route_mode"],
            bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
        )
        self.assertEqual(
            guard_call.kwargs["channel_policy"],
            "public_home",
        )
        self.assertEqual(
            guard_call.kwargs["generation_route"],
            "bnl_memory_preview",
        )
        self.assertIsNone(guard_call.kwargs["channel"])
        self.assertTrue(guard_call.kwargs["regeneration_allowed"])
        self.assertFalse(
            guard_call.kwargs["source_context_available"]
        )
        self.assertEqual(
            guard_call.kwargs["prompt_source_bases"],
            (),
        )

    async def test_default_guard_retry_never_types_in_public_channel(self):
        class PublicChannel:
            id = 10
            name = "barcode-bot"

            def __init__(self):
                self.typing_calls = 0

            def typing(self):
                self.typing_calls += 1
                raise AssertionError(
                    "private preview touched public channel typing"
                )

        async def generator(_prompt, route):
            if route.endswith("baseline"):
                return "I only have a narrow grounded view so far."
            return (
                "Archive records show you keep returning to software "
                "and technical systems."
            )

        public_channel = PublicChannel()
        regenerated = mock.AsyncMock(
            return_value=(
                "You keep returning to software and technical systems."
            )
        )
        with mock.patch.object(
            bnl01_bot,
            "get_gemini_response",
            new=regenerated,
        ):
            result = await bnl01_bot.execute_bnl_memory_preview(
                source_db_path=self.db_path,
                guild_id=1,
                subject_user_id=7,
                subject_display_name="Crow",
                simulated_channel_id=public_channel.id,
                wording="What am I all about?",
                simulated_channel=public_channel,
                generator=generator,
            )

        self.assertTrue(result.candidate_selected)
        self.assertEqual(
            result.proposed_response,
            "You keep returning to software and technical systems.",
        )
        regenerated.assert_awaited_once()
        self.assertEqual(
            regenerated.await_args.kwargs["route"],
            "bnl_memory_preview",
        )
        self.assertEqual(public_channel.typing_calls, 0)

    async def test_slash_command_is_owner_only_ephemeral_and_sealed(self):
        class FakeGuildChannel:
            def __init__(self, channel_id, name):
                self.id = channel_id
                self.name = name

        testing = FakeGuildChannel(20, "bnl-testing")
        public_home = FakeGuildChannel(10, "barcode-bot")
        guild = SimpleNamespace(
            id=1,
            text_channels=[public_home],
        )
        interaction = SimpleNamespace(
            user=SimpleNamespace(id=99),
            guild=guild,
            channel=testing,
            response=SimpleNamespace(
                defer=mock.AsyncMock(),
                send_message=mock.AsyncMock(),
            ),
        )
        subject = SimpleNamespace(id=7, display_name="Crow")
        execution = bnl01_bot.BnlMemoryPreviewExecution(
            proposed_response="Grounded proposed response.",
            diagnostics=("- persistence: `none`",),
            route_status="matched",
            candidate_selected=True,
            fallback_reason="",
            established_response="Established response.",
            packet_candidate_response="Packet response.",
            repair_response="Repair response.",
            final_selection="repair_attempt",
        )
        sent = mock.AsyncMock()

        def policy(channel):
            return (
                "sealed_test"
                if channel is testing
                else "public_home"
            )

        with (
            mock.patch.object(bnl01_bot, "BNL_OWNER_USER_ID", 99),
            mock.patch.object(bnl01_bot, "BNL_TESTING_CHANNEL_ID", 20),
            mock.patch.object(
                bnl01_bot,
                "is_owner_operator",
                return_value=True,
            ),
            mock.patch.object(
                bnl01_bot.discord.abc,
                "GuildChannel",
                FakeGuildChannel,
            ),
            mock.patch.object(
                bnl01_bot,
                "resolve_channel_policy",
                side_effect=policy,
            ),
            mock.patch.object(
                bnl01_bot,
                "execute_bnl_memory_preview",
                new=mock.AsyncMock(return_value=execution),
            ) as execute,
            mock.patch.object(
                bnl01_bot,
                "send_safe_ephemeral_chunks",
                new=sent,
            ),
        ):
            await bnl01_bot.bnl_memory_preview.callback(
                interaction,
                subject,
                "What am I all about?",
            )

        interaction.response.defer.assert_awaited_once_with(
            ephemeral=True
        )
        interaction.response.send_message.assert_not_awaited()
        execute.assert_awaited_once()
        sent.assert_awaited_once()
        rendered = sent.await_args.args[1]
        self.assertIn("BNL Memory Preview", rendered)
        self.assertIn(
            "Established normal-generation baseline",
            rendered,
        )
        self.assertIn("Established response.", rendered)
        self.assertIn("Packet candidate", rendered)
        self.assertIn("Packet response.", rendered)
        self.assertIn("Grounded repair attempt", rendered)
        self.assertIn("Repair response.", rendered)
        self.assertIn("final_selection: `repair_attempt`", rendered)
        self.assertIn("Grounded proposed response.", rendered)
        self.assertIn("Content-free diagnostics", rendered)

    async def test_slash_command_never_runs_outside_bnl_testing(self):
        class FakeGuildChannel:
            def __init__(self):
                self.id = 30
                self.name = "off-topic"

        channel = FakeGuildChannel()
        interaction = SimpleNamespace(
            user=SimpleNamespace(id=99),
            guild=SimpleNamespace(id=1, text_channels=[]),
            channel=channel,
            response=SimpleNamespace(
                defer=mock.AsyncMock(),
                send_message=mock.AsyncMock(),
            ),
        )
        subject = SimpleNamespace(id=7, display_name="Crow")
        with (
            mock.patch.object(bnl01_bot, "BNL_OWNER_USER_ID", 99),
            mock.patch.object(
                bnl01_bot,
                "is_owner_operator",
                return_value=True,
            ),
            mock.patch.object(
                bnl01_bot.discord.abc,
                "GuildChannel",
                FakeGuildChannel,
            ),
            mock.patch.object(
                bnl01_bot,
                "resolve_channel_policy",
                return_value="public_context",
            ),
            mock.patch.object(
                bnl01_bot,
                "execute_bnl_memory_preview",
                new=mock.AsyncMock(),
            ) as execute,
        ):
            await bnl01_bot.bnl_memory_preview.callback(
                interaction,
                subject,
            )

        interaction.response.send_message.assert_awaited_once()
        self.assertTrue(
            interaction.response.send_message.await_args.kwargs[
                "ephemeral"
            ]
        )
        execute.assert_not_awaited()
        interaction.response.defer.assert_not_awaited()

    async def test_slash_command_reports_local_budget_without_running_preview(self):
        class FakeGuildChannel:
            def __init__(self):
                self.id = 20
                self.name = "bnl-testing"

        channel = FakeGuildChannel()
        interaction = SimpleNamespace(
            user=SimpleNamespace(id=99),
            guild=SimpleNamespace(id=1, text_channels=[]),
            channel=channel,
            response=SimpleNamespace(
                defer=mock.AsyncMock(),
                send_message=mock.AsyncMock(),
            ),
        )
        subject = SimpleNamespace(id=7, display_name="Crow")
        with (
            mock.patch.object(bnl01_bot, "BNL_OWNER_USER_ID", 99),
            mock.patch.object(bnl01_bot, "BNL_TESTING_CHANNEL_ID", 20),
            mock.patch.object(
                bnl01_bot,
                "is_owner_operator",
                return_value=True,
            ),
            mock.patch.object(
                bnl01_bot.discord.abc,
                "GuildChannel",
                FakeGuildChannel,
            ),
            mock.patch.object(
                bnl01_bot,
                "resolve_channel_policy",
                return_value="sealed_test",
            ),
            mock.patch.object(
                bnl01_bot,
                "check_quota_availability",
                return_value=False,
            ),
            mock.patch.object(
                bnl01_bot,
                "get_usage_stats",
                return_value=(1_356_677, "2026-07-26"),
            ),
            mock.patch.object(
                bnl01_bot,
                "execute_bnl_memory_preview",
                new=mock.AsyncMock(),
            ) as execute,
        ):
            await bnl01_bot.bnl_memory_preview.callback(
                interaction,
                subject,
            )

        interaction.response.send_message.assert_awaited_once()
        message = interaction.response.send_message.await_args.args[0]
        self.assertIn("local daily model budget", message)
        self.assertIn("1,356,677/1,350,000", message)
        self.assertIn("not the Gemini app allowance", message)
        self.assertIn("No preview generation was attempted", message)
        self.assertTrue(
            interaction.response.send_message.await_args.kwargs[
                "ephemeral"
            ]
        )
        execute.assert_not_awaited()
        interaction.response.defer.assert_not_awaited()

    async def test_slash_command_rejects_non_owner_before_preparing(self):
        interaction = SimpleNamespace(
            user=SimpleNamespace(id=41),
            guild=SimpleNamespace(id=1, text_channels=[]),
            channel=SimpleNamespace(id=20, name="bnl-testing"),
            response=SimpleNamespace(
                defer=mock.AsyncMock(),
                send_message=mock.AsyncMock(),
            ),
        )
        subject = SimpleNamespace(id=7, display_name="Crow")
        with (
            mock.patch.object(bnl01_bot, "BNL_OWNER_USER_ID", 99),
            mock.patch.object(
                bnl01_bot,
                "is_owner_operator",
                return_value=False,
            ),
            mock.patch.object(
                bnl01_bot,
                "execute_bnl_memory_preview",
                new=mock.AsyncMock(),
            ) as execute,
        ):
            await bnl01_bot.bnl_memory_preview.callback(
                interaction,
                subject,
            )

        interaction.response.send_message.assert_awaited_once()
        self.assertTrue(
            interaction.response.send_message.await_args.kwargs[
                "ephemeral"
            ]
        )
        execute.assert_not_awaited()
        interaction.response.defer.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
