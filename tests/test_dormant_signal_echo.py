import json
import os
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest import mock

import pytz

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl01_bot


PACIFIC = pytz.timezone("America/Los_Angeles")


class FakeMember:
    def __init__(self, user_id, display_name, *, bot=False):
        self.id = user_id
        self.display_name = display_name
        self.name = display_name
        self.bot = bot


class FakeGuild:
    def __init__(self, guild_id, members):
        self.id = guild_id
        self._members = {member.id: member for member in members}

    def get_member(self, user_id):
        return self._members.get(int(user_id))


class FakeChannel:
    def __init__(self, channel_id, guild, *, name="barcode-bot"):
        self.id = channel_id
        self.guild = guild
        self.name = name
        self.sent = []

    async def send(self, content, **kwargs):
        self.sent.append((content, kwargs))
        return SimpleNamespace(id=1000 + len(self.sent), content=content)


class DormantSignalEchoTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        handle = tempfile.NamedTemporaryFile(delete=False)
        handle.close()
        self.db_path = handle.name
        self.db_patch = mock.patch.object(bnl01_bot, "DB_FILE", self.db_path)
        self.db_patch.start()
        bnl01_bot.init_db()
        bnl01_bot._ambient_runtime_state.clear()
        self.now = PACIFIC.localize(datetime(2026, 7, 23, 14, 0, 0))
        self.guild = FakeGuild(77, [FakeMember(42, "Emerald")])
        self.channel = FakeChannel(222, self.guild)

    def tearDown(self):
        self.db_patch.stop()
        bnl01_bot._ambient_runtime_state.clear()
        try:
            os.unlink(self.db_path)
        except FileNotFoundError:
            pass

    def seed_familiar_member(
        self,
        *,
        user_id=42,
        display_name="Emerald",
        last_seen_days=30,
        public_messages=10,
        active_days=4,
        interactions=36,
        trust_stage="familiar",
        social_stance="friend",
    ):
        last_seen = self.now.astimezone(timezone.utc) - timedelta(days=last_seen_days)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO user_profiles (
                    user_id, guild_id, display_name, preferred_name, last_seen
                ) VALUES (?, ?, ?, NULL, ?)
                """,
                (
                    user_id,
                    77,
                    display_name,
                    last_seen.isoformat(),
                ),
            )
            conn.execute(
                """
                INSERT OR REPLACE INTO relationship_state (
                    user_id, guild_id, interaction_count, affinity_score,
                    trust_stage, social_stance, last_topic, updated_at
                ) VALUES (?, ?, ?, 2.5, ?, ?, 'broadcast_music', ?)
                """,
                (
                    user_id,
                    77,
                    interactions,
                    trust_stage,
                    social_stance,
                    last_seen.isoformat(),
                ),
            )
            for index in range(public_messages):
                observed = last_seen - timedelta(
                    days=index % max(1, active_days),
                    minutes=index,
                )
                conn.execute(
                    """
                    INSERT INTO conversations (
                        user_id, user_name, guild_id, channel_name,
                        channel_policy, channel_id, role, content, timestamp
                    ) VALUES (?, ?, 77, 'barcode-bot', 'public_home', 222,
                              'user', ?, ?)
                    """,
                    (
                        user_id,
                        display_name,
                        (
                            f"{display_name} shared synth mix note {index} "
                            "and helped the room compare strange radio textures."
                        ),
                        observed.strftime("%Y-%m-%d %H:%M:%S"),
                    ),
                )
            conn.execute(
                """
                INSERT INTO conversations (
                    user_id, user_name, guild_id, channel_name,
                    channel_policy, channel_id, role, content, timestamp
                ) VALUES (?, ?, 77, 'important', 'internal_controlled', 999,
                          'user', 'PRIVATE INTERNAL ROW MUST NOT ENTER ECHO', ?)
                """,
                (
                    user_id,
                    display_name,
                    last_seen.strftime("%Y-%m-%d %H:%M:%S"),
                ),
            )
            conn.commit()

    def seed_current_room_signal(self):
        with sqlite3.connect(self.db_path) as conn:
            for index, (user_id, user_name, content) in enumerate(
                (
                    (
                        50,
                        "Current One",
                        "The room is comparing patient synth mixes and unusual radio textures.",
                    ),
                    (
                        51,
                        "Current Two",
                        "Those detuned transitions could make the next broadcast sequence stranger.",
                    ),
                    (
                        50,
                        "Current One",
                        "A slower arrangement might leave more space for the signal to breathe.",
                    ),
                )
            ):
                observed = self.now.astimezone(timezone.utc) - timedelta(
                    minutes=10 - index
                )
                conn.execute(
                    """
                    INSERT INTO conversations (
                        user_id, user_name, guild_id, channel_name,
                        channel_policy, channel_id, role, content, timestamp
                    ) VALUES (?, ?, 77, 'barcode-bot', 'public_home', 222,
                              'user', ?, ?)
                    """,
                    (
                        user_id,
                        user_name,
                        content,
                        observed.strftime("%Y-%m-%d %H:%M:%S"),
                    ),
                )
            conn.commit()

    def test_candidate_requires_repeated_public_familiarity_and_real_absence(self):
        self.seed_familiar_member()

        candidate, reason = bnl01_bot.select_dormant_echo_candidate(
            77,
            now_pacific=self.now,
            guild=self.guild,
        )

        self.assertEqual(reason, "candidate_selected")
        self.assertEqual(candidate["userId"], 42)
        self.assertEqual(candidate["displayName"], "Emerald")
        self.assertGreaterEqual(candidate["publicMessageCount"], 8)
        self.assertGreaterEqual(candidate["activeDays"], 3)
        self.assertGreaterEqual(candidate["absenceDays"], 14)
        self.assertEqual(candidate["trustStage"], "familiar")
        evidence_text = json.dumps(candidate["evidence"])
        self.assertNotIn("PRIVATE INTERNAL ROW", evidence_text)

        basis = bnl01_bot.dormant_echo_selection_basis(candidate)
        self.assertEqual(basis["version"], "dormant_echo_canary_v1")
        self.assertEqual(basis["subjectUserId"], 42)
        self.assertTrue(basis["evidenceRowIds"])
        self.assertNotIn("fragment", json.dumps(basis).lower())

    def test_recent_or_unfamiliar_member_is_not_selected(self):
        self.seed_familiar_member(last_seen_days=4)
        candidate, reason = bnl01_bot.select_dormant_echo_candidate(
            77,
            now_pacific=self.now,
            guild=self.guild,
        )
        self.assertIsNone(candidate)
        self.assertEqual(reason, "no_eligible_familiar_absent_member")

        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                UPDATE conversations
                SET timestamp = datetime(timestamp, '-30 days')
                WHERE guild_id = 77 AND user_id = 42
                """
            )
            conn.execute(
                """
                UPDATE relationship_state
                SET interaction_count = 5, trust_stage = 'new'
                WHERE guild_id = 77 AND user_id = 42
                """
            )
            conn.commit()
        candidate, reason = bnl01_bot.select_dormant_echo_candidate(
            77,
            now_pacific=self.now,
            guild=self.guild,
        )
        self.assertIsNone(candidate)
        self.assertEqual(reason, "no_eligible_familiar_absent_member")

    async def test_kill_switch_channel_boundary_and_rarity_gate_fail_closed(self):
        with mock.patch.object(bnl01_bot, "BNL_DORMANT_ECHO_ENABLED", False):
            result = await bnl01_bot.prepare_dormant_echo_canary(
                77,
                222,
                self.channel,
                now_pacific=self.now,
            )
        self.assertEqual(result, {"status": "withheld", "reason": "kill_switch_disabled"})

        outside = FakeChannel(333, self.guild, name="general-chat")
        with mock.patch.object(bnl01_bot, "BNL_DORMANT_ECHO_ENABLED", True):
            result = await bnl01_bot.prepare_dormant_echo_canary(
                77,
                333,
                outside,
                now_pacific=self.now,
            )
        self.assertEqual(result["reason"], "outside_barcode_bot_canary")

        with (
            mock.patch.object(bnl01_bot, "BNL_DORMANT_ECHO_ENABLED", True),
            mock.patch.object(
                bnl01_bot,
                "select_dormant_echo_candidate",
                side_effect=AssertionError(
                    "fresh room signal must run before candidate retrieval"
                ),
            ),
        ):
            result = await bnl01_bot.prepare_dormant_echo_canary(
                77,
                222,
                self.channel,
                now_pacific=self.now,
            )
        self.assertEqual(result["reason"], "no_current_room_signal")

        self.seed_current_room_signal()
        with (
            mock.patch.object(bnl01_bot, "BNL_DORMANT_ECHO_ENABLED", True),
            mock.patch.object(bnl01_bot, "DORMANT_ECHO_SELECTION_CHANCE", 0.05),
            mock.patch.object(bnl01_bot.random, "random", return_value=0.75),
            mock.patch.object(
                bnl01_bot,
                "select_dormant_echo_candidate",
                side_effect=AssertionError("rarity gate must run before candidate retrieval"),
            ),
        ):
            result = await bnl01_bot.prepare_dormant_echo_canary(
                77,
                222,
                self.channel,
                now_pacific=self.now,
            )
        self.assertEqual(result["reason"], "rarity_gate")

    def test_validator_blocks_pings_false_presence_summons_and_surveillance(self):
        candidate = {
            "displayName": "Emerald",
            "evidence": [
                {
                    "fragment": "Emerald shared patient synth mixing notes with the room."
                }
            ],
        }
        valid = (
            "This synth thread carries an Emerald-shaped harmonic: patient mix "
            "notes and strange signal detours still fit the room."
        )
        self.assertEqual(
            bnl01_bot.validate_dormant_echo(valid, candidate),
            "",
        )
        self.assertEqual(
            bnl01_bot.validate_dormant_echo(
                "Emerald is back online and listening.",
                candidate,
            ),
            "false_presence_or_summon",
        )
        self.assertEqual(
            bnl01_bot.validate_dormant_echo(
                "Emerald hasn't posted in weeks, and the archive noticed.",
                candidate,
            ),
            "surveillance_framing",
        )
        self.assertEqual(
            bnl01_bot.validate_dormant_echo(
                "@Emerald, where have you been?",
                candidate,
            ),
            "mention_forbidden",
        )

    async def test_generation_retries_rejected_presence_claim_then_returns_safe_echo(self):
        candidate = {
            "displayName": "Emerald",
            "evidence": [
                {
                    "rowId": 1,
                    "fragment": "Emerald compared synth mixes and strange radio textures.",
                    "observedAt": "2026-06-01",
                }
            ],
        }
        safe_echo = (
            "This synth thread carries an Emerald-shaped harmonic: patient mix "
            "notes and strange signal detours still fit the room."
        )
        provider = mock.AsyncMock(
            side_effect=[
                "Emerald is back online and listening.",
                safe_echo,
            ]
        )
        with (
            mock.patch.object(
                bnl01_bot,
                "get_recent_ambient_owned_messages",
                return_value=[],
            ),
            mock.patch.object(bnl01_bot, "get_gemini_response", provider),
        ):
            message, reason = await bnl01_bot.generate_dormant_echo(
                77,
                222,
                candidate,
                recent_context=[
                    (
                        "Current One",
                        "We are comparing patient synth mixes and strange radio textures.",
                    ),
                    (
                        "Current Two",
                        "The detuned transitions could make the next broadcast stranger.",
                    ),
                    (
                        "Current One",
                        "A slower arrangement leaves the signal enough room to breathe.",
                    ),
                ],
            )

        self.assertEqual(message, safe_echo)
        self.assertEqual(reason, "")
        self.assertEqual(provider.await_count, 2)
        retry_prompt = provider.await_args_list[1].args[0]
        self.assertIn("false_presence_or_summon", retry_prompt)
        self.assertIn("return NO_ECHO", retry_prompt)

    async def test_prepare_and_publish_use_shared_ambient_log_without_ping(self):
        self.seed_familiar_member()
        self.seed_current_room_signal()
        safe_echo = (
            "This synth thread carries an Emerald-shaped harmonic: patient mix "
            "notes and strange signal detours still fit the room."
        )
        with (
            mock.patch.object(bnl01_bot, "BNL_DORMANT_ECHO_ENABLED", True),
            mock.patch.object(bnl01_bot, "DORMANT_ECHO_SELECTION_CHANCE", 1.0),
            mock.patch.object(bnl01_bot.random, "random", return_value=0.0),
            mock.patch.object(
                bnl01_bot,
                "generate_dormant_echo",
                new=mock.AsyncMock(return_value=(safe_echo, "")),
            ),
        ):
            prepared = await bnl01_bot.prepare_dormant_echo_canary(
                77,
                222,
                self.channel,
                now_pacific=self.now,
            )
        self.assertEqual(prepared["status"], "ready")

        with mock.patch.object(
            bnl01_bot,
            "schedule_after_ambient_post",
            return_value="2026-07-24T12:00:00-07:00",
        ) as schedule:
            published = await bnl01_bot.publish_prepared_dormant_echo(
                77,
                222,
                self.channel,
                prepared,
                capacity_used_before_send=0,
                now_pacific=self.now,
            )

        self.assertEqual(published["status"], "published")
        self.assertEqual(len(self.channel.sent), 1)
        sent_text, kwargs = self.channel.sent[0]
        self.assertEqual(sent_text, safe_echo)
        self.assertNotIn("@", sent_text)
        allowed_mentions = kwargs["allowed_mentions"]
        self.assertFalse(allowed_mentions.everyone)
        self.assertFalse(allowed_mentions.users)
        self.assertFalse(allowed_mentions.roles)
        schedule.assert_called_once()

        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                """
                SELECT source_type, subject_user_id, selection_basis_json
                FROM ambient_log
                WHERE guild_id = 77
                ORDER BY id DESC
                LIMIT 1
                """
            ).fetchone()
        self.assertEqual(row[0], "dormant_echo")
        self.assertEqual(row[1], 42)
        stored_basis = json.loads(row[2])
        self.assertEqual(stored_basis["subjectUserId"], 42)
        self.assertNotIn("fragment", json.dumps(stored_basis).lower())
        runtime = bnl01_bot._get_ambient_runtime_state(77)
        self.assertEqual(runtime["last_dormant_echo_status"], "published")

        with sqlite3.connect(self.db_path) as conn:
            deleted = bnl01_bot.complete_delete_member_data(
                conn,
                guild_id=77,
                user_id=42,
                confirmation="DELETE MY BNL DATA 77",
            )
        self.assertTrue(deleted["ok"])
        self.assertEqual(deleted["row_counts"]["ambient_log_dormant_echo"], 1)
        with sqlite3.connect(self.db_path) as conn:
            remaining = conn.execute(
                """
                SELECT COUNT(*)
                FROM ambient_log
                WHERE guild_id = 77 AND subject_user_id = 42
                """
            ).fetchone()[0]
        self.assertEqual(remaining, 0)

    async def test_global_cooldown_prevents_repeat_before_candidate_lookup(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO ambient_log (
                    guild_id, channel_id, message, source_type, posted_at,
                    subject_user_id, selection_basis_json
                ) VALUES (77, 222, 'Prior safe echo.', 'dormant_echo', ?, 42, '{}')
                """,
                (self.now.astimezone(timezone.utc).isoformat(),),
            )
            conn.commit()
        with (
            mock.patch.object(bnl01_bot, "BNL_DORMANT_ECHO_ENABLED", True),
            mock.patch.object(bnl01_bot, "DORMANT_ECHO_SELECTION_CHANCE", 1.0),
            mock.patch.object(
                bnl01_bot,
                "select_dormant_echo_candidate",
                side_effect=AssertionError("cooldown must run before candidate retrieval"),
            ),
        ):
            result = await bnl01_bot.prepare_dormant_echo_canary(
                77,
                222,
                self.channel,
                now_pacific=self.now,
            )
        self.assertEqual(result["reason"], "global_cooldown")


if __name__ == "__main__":
    unittest.main()
