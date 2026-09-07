"""Restoration coverage across real public readers and normal Gemini delivery.

The database, publication/queue readers, prompt assembly, normal provider
dispatch, guards and memory writers are real. HTTP, Gemini SDK and Discord
transport are local fixtures. These tests do not certify a live deployment.
"""

import asyncio
import json
import os
import sqlite3
import tempfile
import unittest
from contextlib import ExitStack
from types import SimpleNamespace
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl01_bot as bot
import bnl_journal
import bnl_website_relay_state
import test_publication_read_adapters as publication_fixtures
from test_conversation_batching import FakeAuthor, FakeChannel, FakeGuild, FakeMessage


JOURNAL = "The Copper Kite instrumental brought the room together."
RELAY = "The Copper Kite listening exchange continued in the public room."
PUBLIC_A = "Test Member A built the Copper Kite melody."
PUBLIC_B = "Test Member B supplied the Copper Kite drum pattern."
PRIVATE = "The sealed-only rehearsal phrase is violet button."
ANSWER = (
    "The Journal described Copper Kite bringing people together, and the Relay "
    "continued that listening exchange. Submissions are closed right now."
)


class HttpResponse:
    status = 200

    def __init__(self, payload):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self):
        return json.dumps(self.payload).encode("utf-8")


class RecoveryPublicMirrorTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.stack = ExitStack()
        self.tmp = self.stack.enter_context(tempfile.TemporaryDirectory())
        self.stack.enter_context(mock.patch.object(bot, "DB_FILE", os.path.join(self.tmp, "bnl.db")))
        self.stack.enter_context(mock.patch.dict(os.environ, {
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_ENABLED": "false",
            "BNL_SHARED_BRAIN_SYNTHESIS_CANARY_ENABLED": "false",
            "BNL_PUBLIC_HOME_BROAD_RECALL_OWNER_ENABLED": "false",
            "BNL_MEMORY_GOVERNANCE_LIVE_ENABLED": "false",
            "BNL_RELATIONSHIP_V2_LIVE_ENABLED": "false",
            "BNL_ACTIVE_ENGAGEMENT_V2_LIVE_ENABLED": "false",
            "BNL_UNIFIED_MOMENT_CANARY_ENABLED": "false",
            "BNL_CONVERSATION_CONTEXT_V2_ENABLED": "true",
            "BNL_QUEUE_PRODUCTION_ENABLED": "true",
            "BNL_JOURNAL_CONTROL_URL": "https://site.test/api/bnl/journal/control",
        }, clear=False))
        for name, value in (
            ("BNL_READ_MODEL_ENABLED", True),
            ("BNL_READ_MODEL_URL", "https://site.test/api/bnl/read-model"),
            ("BNL_API_KEY", "fixture-key"),
            ("_bnl_read_model_cache", None),
            ("_bnl_read_model_cached_at", None),
            ("BNL_ACTIVE_BATCHING_ENABLED", True),
            ("POST_GENERATION_CAPTURE_GRACE_SECONDS", 0),
        ):
            self.stack.enter_context(mock.patch.object(bot, name, value))
        # Hold optional aesthetic randomness constant; do not replace the normal
        # provider dispatcher or any response/source checker.
        self.stack.enter_context(mock.patch.object(bot.random, "random", return_value=1.0))
        self.guild_id = 7755
        self.channel_ids = set()
        self.bot_user = SimpleNamespace(id=999, display_name="BNL-01", bot=True)
        self.stack.enter_context(mock.patch.object(
            type(bot.client), "user", new_callable=mock.PropertyMock,
            return_value=self.bot_user,
        ))
        bot.init_db()
        bnl_journal.ensure_schema(bot.DB_FILE)
        bnl_website_relay_state.ensure_schema(bot.DB_FILE)
        for user_id, label, public_memory in (
            (100, "Test Member A", PUBLIC_A),
            (101, "Test Member B", PUBLIC_B),
        ):
            bot.upsert_user_profile(user_id, self.guild_id, label)
            with sqlite3.connect(bot.DB_FILE) as conn:
                for text, policy, trust in (
                    (public_memory, "public_home", "source_safe_public"),
                    (PRIVATE, "sealed_test", "legacy_unknown"),
                ):
                    bot._insert_memory_tier(
                        conn.cursor(), user_id, self.guild_id, "long", text, 0.95,
                        source_role="user", source_channel_policy=policy,
                        source_trust=trust, topic_key="music",
                    )
        with sqlite3.connect(bot.DB_FILE) as conn:
            fixture = SimpleNamespace(conn=conn)
            publication_fixtures.PublicationReadAdapterTests.add_journal(
                fixture, "journal_copper_kite", title="Copper Kite Connections",
                excerpt=JOURNAL, body=JOURNAL,
            )
            publication_fixtures.PublicationReadAdapterTests.add_relay(
                fixture, "relay_copper_kite", message=RELAY,
                directive="Keep the Copper Kite exchange going.",
            )
            conn.execute("UPDATE bnl_journal_entries SET guild_id=?", (self.guild_id,))
            conn.execute("UPDATE website_relay_history SET guild_id=?", (self.guild_id,))
        self.http = self.stack.enter_context(mock.patch.object(
            bot.urllib.request, "urlopen", side_effect=self._http,
        ))
        self.provider = mock.Mock(side_effect=self._provider_answer)
        self.provider_text = ANSWER
        self.stack.enter_context(mock.patch.object(bot, "gemini_client", SimpleNamespace(
            models=SimpleNamespace(generate_content=self.provider),
        )))

    async def asyncTearDown(self):
        for channel_id in self.channel_ids:
            task = bot._channel_tasks.pop(channel_id, None)
            if task is not None and not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            for name in (
                "_channel_buffers", "_channel_first_seen", "_channel_last_message_at",
                "_channel_last_reply_at", "_channel_generating", "_channel_generation_id",
                "_channel_preempted_generation_id", "_channel_message_interrupt_generation_id",
                "_channel_interrupt_handoff", "_channel_payload_wait_extended",
                "_channel_pending_request_intent", "_channel_pending_request_anchor",
                "_channel_generation_typing_pause_used", "_channel_typing_indicator_last_at",
                "_channel_batch_typing_sessions", "_channel_batch_typing_interrupt_revision",
                "_channel_batch_typing_applied_revision",
            ):
                getattr(bot, name).pop(channel_id, None)
            for name in ("_conversation_continuation_state", "_recent_direct_response_window"):
                cache = getattr(bot, name)
                for key in list(cache):
                    if channel_id in key:
                        cache.pop(key, None)
        for user_id in (100, 101):
            bot.purge_member_memory_caches(user_id, self.guild_id)
        self.stack.close()

    def _http(self, request, timeout):
        self.assertEqual(request.get_method(), "GET")
        if request.full_url.endswith("/read-model"):
            return HttpResponse({
                "ok": True, "version": 1, "publicOnly": True,
                "accessScope": "public", "source": "barcode-network-site",
                "capabilities": {"queueProduction": True},
                "sections": {"queue": {
                    "available": True, "accessScope": "public",
                    "session": {"title": "BARCODE Radio", "status": "closed", "queueOpen": False},
                    "status": {"activeCount": 0, "capacity": 44},
                    "queue": [], "nowPlaying": None, "upNext": None,
                }},
            })
        self.assertEqual(request.full_url, "https://site.test/api/bnl/journal/control")
        now = bot.datetime.now(bot.timezone.utc)
        return HttpResponse({
            "persisted": True, "contractVersion": 1, "controlSnapshotVersion": 1,
            "controlRevision": now.isoformat(), "controlDigest": "a" * 64,
            "controlObservedAt": now.isoformat(),
            "controlFreshUntil": (now + bot.timedelta(minutes=2)).isoformat(),
            "controlFreshForSeconds": 120,
            "publicExcludedEntryIds": [], "memoryExcludedEntryIds": [],
        })

    def _provider_answer(self, **_kwargs):
        return SimpleNamespace(
            candidates=[SimpleNamespace(content=SimpleNamespace(parts=[SimpleNamespace(text=self.provider_text)]))],
            usage_metadata=SimpleNamespace(
                total_token_count=1400, prompt_token_count=1300,
                candidates_token_count=100, thoughts_token_count=0,
                cached_content_token_count=0,
            ),
        )

    def _channel(self, policy):
        channel_id = 8880 + len(self.channel_ids)
        self.channel_ids.add(channel_id)
        return FakeChannel(
            channel_id, name="bnl-testing" if policy == "sealed_test" else "barcode-bot",
            guild=FakeGuild(self.guild_id),
        )

    async def _flush_public_question(self, policy, *, multiple_speakers):
        channel = self._channel(policy)
        now = bot.datetime.now(bot.PACIFIC_TZ)
        texts = [(100, "Test Member A", "What did the latest Journal and Relay say about Copper Kite?")]
        if multiple_speakers:
            texts.append((101, "Test Member B", "And is the queue open right now?"))
        else:
            texts[0] = (100, "Test Member A", texts[0][2] + " Is the queue open right now?")
        for user_id, name, text in texts:
            message = FakeMessage(channel, text, author=FakeAuthor(user_id, name))
            addressing = await bot.resolve_discord_turn_addressing_async(message)
            self.assertFalse(addressing.addresses_bnl)
            bot.save_user_message(
                user_id, name, self.guild_id, text, channel.name,
                policy, channel.id, message.id,
            )
            bot._channel_buffers[channel.id].append(bot.BatchConversationTurn(
                name, text, user_id, addressing=addressing,
            ))
        bot._channel_first_seen[channel.id] = now
        bot._channel_last_message_at[channel.id] = now
        bot._channel_last_reply_at[channel.id] = now - bot.timedelta(hours=2)
        calls_before = self.provider.call_count
        with mock.patch.object(bot, "get_guild_config", return_value=channel.id):
            self.assertEqual(bot.resolve_channel_policy(channel), policy)
            await bot._flush_channel_buffer(channel)
        self.assertEqual(self.provider.call_count - calls_before, 1)
        self.assertEqual(channel.sent, [ANSWER])
        prompt = self.provider.call_args.kwargs["contents"]
        self.assertIn(bot.BNL01_SYSTEM_PROMPT, prompt)
        for evidence in (JOURNAL, RELAY, "queueOpen=False"):
            self.assertIn(evidence, prompt)
        self.assertNotIn(PRIVATE, prompt)
        if multiple_speakers:
            for _user_id, name, text in texts:
                self.assertIn(name, prompt)
                self.assertIn(text, prompt)
        else:
            self.assertIn(PUBLIC_A, prompt)
        return channel, prompt

    async def test_untagged_home_and_private_mirror_read_same_public_sources_and_deliver(self):
        for policy in ("public_home", "sealed_test"):
            with self.subTest(policy=policy):
                await self._flush_public_question(policy, multiple_speakers=False)
        urls = [call.args[0].full_url for call in self.http.call_args_list]
        self.assertIn("https://site.test/api/bnl/read-model", urls)
        self.assertIn("https://site.test/api/bnl/journal/control", urls)

    async def test_multi_member_home_and_mirror_keep_sources_attribution_and_private_writes(self):
        for policy in ("public_home", "sealed_test"):
            with self.subTest(policy=policy):
                channel, _prompt = await self._flush_public_question(policy, multiple_speakers=True)
                with sqlite3.connect(bot.DB_FILE) as conn:
                    rows = conn.execute(
                        "SELECT role,channel_policy,content FROM conversations WHERE channel_id=?",
                        (channel.id,),
                    ).fetchall()
                    if policy == "sealed_test":
                        public_journal_events = conn.execute(
                            "SELECT COUNT(*) FROM bnl_journal_source_events WHERE channel_id=?",
                            (channel.id,),
                        ).fetchone()[0]
                        self.assertEqual(public_journal_events, 0)
                        public_tiers = conn.execute(
                            "SELECT summary FROM memory_tiers WHERE guild_id=? "
                            "AND source_channel_policy IN ('public_home','public_context','public_selective')",
                            (self.guild_id,),
                        ).fetchall()
                        self.assertEqual(set(public_tiers), {(PUBLIC_A,), (PUBLIC_B,)})
                self.assertTrue(rows)
                self.assertTrue(all(row[1] == policy for row in rows))
                # Current operational queue answers stay transient. Private
                # mirror input is stored only with its private channel policy.
                self.assertFalse(any(row[0] == "model" and row[2] == ANSWER for row in rows))

    async def test_actual_unstored_discord_reply_reaches_normal_provider_and_stays_private(self):
        channel = self._channel("sealed_test")
        original = FakeMessage(channel, ANSWER, author=self.bot_user)
        followup = FakeMessage(
            channel, "Which part of that came from the Journal, and which part was the current status?",
            author=FakeAuthor(100, "Test Member A"),
        )
        followup.reference = SimpleNamespace(
            resolved=original, message_id=original.id, channel_id=channel.id,
        )
        addressing = await bot.resolve_discord_turn_addressing_async(followup)
        self.assertTrue(addressing.reply_targets_bnl)
        self.assertEqual(addressing.reply_source_text, ANSWER)
        self.assertEqual(addressing.reply_conversation_row_id, 0)
        now = bot.datetime.now(bot.PACIFIC_TZ)
        bot._channel_buffers[channel.id].append(bot.BatchConversationTurn(
            "Test Member A", followup.content, 100, addressing=addressing,
        ))
        bot._channel_first_seen[channel.id] = now
        bot._channel_last_message_at[channel.id] = now
        bot._channel_last_reply_at[channel.id] = now - bot.timedelta(hours=2)
        self.provider_text = (
            "The first sentence summarized the Journal and Relay. "
            "The final sentence was the queue status at the time of that reply."
        )
        with mock.patch.object(bot, "get_guild_config", return_value=channel.id):
            await bot._flush_channel_buffer(channel)
        self.provider.assert_called_once()
        prompt = self.provider.call_args.kwargs["contents"]
        self.assertIn(bot.BNL01_SYSTEM_PROMPT, prompt)
        self.assertIn(ANSWER, prompt)
        self.assertIn(followup.content, prompt)
        self.assertEqual(channel.sent, [self.provider_text])
        with sqlite3.connect(bot.DB_FILE) as conn:
            stored = conn.execute(
                "SELECT content FROM conversations WHERE channel_id=?", (channel.id,),
            ).fetchall()
        self.assertEqual(stored, [])

    async def test_other_public_channel_tags_and_replies_reach_same_normal_sources(self):
        for kind in ("tag", "reply"):
            with self.subTest(kind=kind):
                channel = self._channel("public_home")
                channel.name = "general-chat"
                question = "What did the Journal and Relay say about Copper Kite, and is the queue open right now?"
                message = FakeMessage(
                    channel, ("<@999> " if kind == "tag" else "") + question,
                    author=FakeAuthor(100, "Test Member A"),
                    mentions=[self.bot_user] if kind == "tag" else [],
                )
                if kind == "reply":
                    prior = FakeMessage(channel, "What would you like to know?", author=self.bot_user)
                    message.reference = SimpleNamespace(
                        resolved=prior, message_id=prior.id, channel_id=channel.id,
                    )
                calls_before = self.provider.call_count
                with mock.patch.object(bot, "get_guild_config", return_value=999_999):
                    self.assertEqual(bot.resolve_channel_policy(channel), "public_context")
                    await bot.on_message(message)
                self.assertEqual(self.provider.call_count - calls_before, 1)
                self.assertEqual(message.replies, [ANSWER])
                prompt = self.provider.call_args.kwargs["contents"]
                for evidence in (bot.BNL01_SYSTEM_PROMPT, JOURNAL, RELAY, PUBLIC_A, "queueOpen=False"):
                    self.assertIn(evidence, prompt)
                self.assertNotIn(PRIVATE, prompt)


if __name__ == "__main__":
    unittest.main()
