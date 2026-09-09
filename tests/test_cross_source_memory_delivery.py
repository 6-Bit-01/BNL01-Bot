"""Public person/topic recall reaches normal Gemini from Discord and TikTok.

Readers, SQLite source lineage, conversation frames, packet decisions, and
source refresh are real. Discord cache/transport and the website/provider
transport are fixtures. Supported replies demonstrate delivery, not live model
factuality or production availability of any particular comment.
"""

import json
import os
import sqlite3
import unittest
from itertools import product
from types import SimpleNamespace
from unittest import mock

import test_public_network_knowledge as network_fixture
import test_tiktok_show_evidence_ledger as show_fixture
from bnl_journal_source_store import record_source_event
from bnl_memory_ledger import shadow_conversation_row, shadow_tiktok_live_chat_event


bot = network_fixture.bnl01_bot
REAL_ADAPTERS = {
    name: getattr(bot, name)
    for name in (
        "build_broadcast_memory_context", "build_queue_artist_memory_context",
        "build_tiktok_show_evidence_context_for_turn", "maybe_build_bnl_read_model_context",
        "choose_response_style", "should_allow_greeting",
    )
}
GUILD = 77
SUBJECT = 42
REQUESTER = 100
REQUEST = "What has Test Signal said about amber lanterns?"
DID_REQUEST = "What did Test Signal say about amber lanterns?"
DISCORD_COMMENT = "I keep the amber lanterns beside my mixing desk."
TIKTOK_COMMENT = "The amber lanterns look great beside the stage."
NEWER_COMMENT = "The silver drums sound crisp tonight."
OTHER_COMMENT = "My amber lanterns arrived in a blue box."
PRIVATE_COMMENT = "The private amber lanterns passphrase is violet thimble."
SEALED_COMMENT = "The sealed amber lanterns passphrase is copper button."
ANSWER = (
    'Test Signal said on Discord, "I keep the amber lanterns beside my mixing desk." '
    'In TikTok chat, they said, "The amber lanterns look great beside the stage."'
)


class CrossSourceMemoryDeliveryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.runtime = network_fixture.PublicNetworkKnowledgeTests()
        await self.runtime.asyncSetUp()
        self.addAsyncCleanup(self.runtime.asyncTearDown)
        self.stack = self.runtime.stack
        self.runtime.guild_id = GUILD
        self.runtime.user_id = REQUESTER
        for name, implementation in REAL_ADAPTERS.items():
            self.stack.enter_context(mock.patch.object(bot, name, new=implementation))
        self.stack.enter_context(mock.patch.object(bot, "BNL_PRIMARY_GUILD_ID", GUILD))
        self.stack.enter_context(mock.patch.dict(os.environ, {
            **show_fixture.ENABLED_QUEUE_ENV,
            "BNL_MEMORY_LEDGER_SHADOW_ENABLED": "true",
            "BNL_MOMENT_ENGINE_SHADOW_ENABLED": "true",
            "BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED": "true",
            "BNL_RELATIONSHIP_V2_SHADOW_ENABLED": "true",
            "BNL_UNIFIED_RESPONSE_ASSESSMENT_SHADOW_ENABLED": "true",
            "BNL_UNIFIED_INTELLIGENCE_PACKET_SHADOW_ENABLED": "true",
            "BNL_CONVERSATION_ORCHESTRATION_INFLUENCE_ENABLED": "false",
            "BNL_CONVERSATION_ORCHESTRATION_SEALED_CANARY_ENABLED": "false",
        }))
        members = [
            SimpleNamespace(id=uid, display_name=label, name=label,
                            global_name=None, bot=False, roles=[],
                            guild_permissions=SimpleNamespace(administrator=False, manage_guild=False))
            for uid, label in ((SUBJECT, "Test Signal"), (43, "Test Other"),
                               (REQUESTER, "Test Member"))
        ]
        self.guild = SimpleNamespace(
            id=GUILD, members=members,
            get_member=lambda uid: next((member for member in members if member.id == uid), None),
        )
        self.stack.enter_context(mock.patch.object(bot.client, "get_guild", return_value=self.guild))
        for member in members:
            bot.upsert_user_profile(member.id, GUILD, member.display_name)
        self.addCleanup(bot.purge_member_memory_caches, SUBJECT, GUILD)
        self._seed_discord_sources()
        self._seed_tiktok_shows()
        self.stack.enter_context(mock.patch.object(bot, "fetch_bnl_read_model", return_value=self.read_model))

    def _seed_discord_sources(self):
        with sqlite3.connect(bot.DB_FILE) as conn:
            for row_id, uid, label, policy, text in (
                (7101, SUBJECT, "Test Signal", "public_home", DISCORD_COMMENT),
                (7102, SUBJECT, "Test Signal", "internal_controlled", PRIVATE_COMMENT),
                (7103, SUBJECT, "Test Signal", "sealed_test", SEALED_COMMENT),
                (7104, 43, "Test Other", "public_home", OTHER_COMMENT),
            ):
                timestamp = "2026-08-30T18:00:00+00:00"  # Outside either broadcast.
                channel_name = "public-lounge" if policy == "public_home" else "private-fixture"
                conn.execute(
                    "INSERT INTO conversations (id,user_id,user_name,guild_id,channel_name,"
                    "channel_policy,route_mode,role,content,timestamp,channel_id,message_id) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                    (row_id, uid, label, GUILD, channel_name, policy, "normal_chat", "user",
                     text, timestamp, 9002 + row_id, row_id + 10000),
                )
                result = shadow_conversation_row(
                    conn, row_id=row_id, user_id=uid, user_name=label, guild_id=GUILD,
                    role="user", content=text, channel_name=channel_name,
                    channel_policy=policy, channel_id=9002 + row_id,
                    message_id=row_id + 10000, route_mode="normal_chat", observed_at=timestamp,
                )
                self.assertEqual(result.outcome, "inserted")

    def _seed_tiktok_shows(self):
        august = show_fixture.archived_show()
        september = json.loads(
            json.dumps(august).replace("show-attendance-1", "show-topic-september")
            .replace("2026-08-28", "2026-09-04").replace("2026-08-29", "2026-09-05")
        )
        for key, date, uid, label, handle, text in (
            ("topic-august", "2026-08-29T00:02:00Z", SUBJECT, "Test Signal", "test.signal", TIKTOK_COMMENT),
            ("topic-other", "2026-08-29T00:03:00Z", 43, "Test Other", "test.other", OTHER_COMMENT),
            ("topic-september", "2026-09-05T00:02:00Z", SUBJECT, "Test Signal", "test.signal", NEWER_COMMENT),
        ):
            result = record_source_event(
                bot.DB_FILE, guild_id=GUILD, source_kind="tiktok_live_chat", source_key=key,
                occurred_at_ms=show_fixture.stamp(date), raw_text=text, sanitized_summary=text,
                channel_policy="public_context", subject_ref="discord_user:%s" % uid,
                private_display_name=label, public_usable=True,
                metadata={"eventType": "comment", "handle": handle,
                          "identityBindingBasis": "exact_source_owned_subject_reference"},
            )
            self.assertTrue(result.ok)
            with sqlite3.connect(bot.DB_FILE) as conn:
                result = shadow_tiktok_live_chat_event(
                    conn, guild_id=GUILD, event_id=key, subject_key="discord_user:%s" % uid,
                    subject_display_name=label, content=text, observed_at=date,
                    source_sequence=show_fixture.stamp(date),
                )
                self.assertEqual(result.outcome, "inserted")
        self.read_model = show_fixture.authorized_read_model({
            "currentShow": None, "latestShow": september, "shows": [august],
        })
        synced = show_fixture.sync_tiktok_show_evidence_ledgers(
            bot.DB_FILE, guild_id=GUILD, read_model=self.read_model,
            artist_identity_index={}, environ=show_fixture.ENABLED_QUEUE_ENV,
        )
        self.assertEqual(synced["showsFinalized"], 2)

    def _packet_configuration(self, enabled, channel_id):
        return mock.patch.dict(os.environ, {
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_ENABLED": str(enabled).lower(),
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_GUILD_IDS": str(GUILD),
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_USER_IDS": str(REQUESTER),
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_CHANNEL_IDS": str(channel_id),
        })

    def _assert_sources(self, prompt, bases):
        self.assertTrue(DISCORD_COMMENT in prompt, "Public Discord authored statement missing from provider prompt")
        self.assertTrue(TIKTOK_COMMENT in prompt, "Public TikTok authored statement missing from provider prompt")
        for excluded in (NEWER_COMMENT, OTHER_COMMENT, PRIVATE_COMMENT, SEALED_COMMENT):
            self.assertNotIn(excluded, prompt)
        self.assertIn("Test Signal", prompt)
        self.assertIn("discord", prompt.casefold())
        self.assertIn("tiktok", prompt.casefold())
        self.assertNotIn("Third-party attribution mode: summarize", prompt)
        self.assertNotIn("Do not provide, reconstruct, or claim exact wording.", prompt)
        self.assertTrue(bases)
        source_context = "\n".join(getattr(basis, "rendered_context", "") for basis in bases)
        self.assertTrue(DISCORD_COMMENT in source_context, "Discord statement has no revalidatable source basis")
        self.assertTrue(TIKTOK_COMMENT in source_context, "TikTok statement has no revalidatable source basis")
        discord_bases = tuple(
            basis for basis in bases
            if isinstance(basis, bot.ConversationPromptSourceBasis)
            and 7101 in basis.source_row_ids
        )
        self.assertEqual(len(discord_bases), 1)
        self.assertEqual(discord_bases[0].participant_user_ids, (SUBJECT,))
        self.assertEqual(discord_bases[0].speaker_labels, ("Test Signal",))
        self.assertTrue(any(
            item.speaker_user_id == SUBJECT and item.text == DISCORD_COMMENT
            for item in discord_bases[0].evidence_items
        ))
        show_bases = tuple(
            basis for basis in bases if isinstance(basis, bot.FinalizedShowPromptSourceBasis)
        )
        self.assertTrue(any(
            excerpt.subject_ref == "discord_user:42"
            and excerpt.source_text == TIKTOK_COMMENT
            and excerpt.speaker_label == "Test Signal (@test.signal)"
            for basis in show_bases
            for excerpt in basis.authored_excerpts
        ))
        # The completed public send may legitimately update requester memory.
        # These independently authored source roots must remain valid.
        self.assertEqual(bot.prompt_source_basis_failure((*discord_bases, *show_bases)), "")

    @staticmethod
    async def _provider_answer(*_args, **kwargs):
        # The real packet owner counts physical attempts at the provider
        # boundary; a plain text AsyncMock otherwise falsely reports zero.
        counter = kwargs.get("attempt_counter")
        if counter is not None:
            counter.mark_started()
        return ANSWER

    async def test_real_direct_assembly_combines_named_public_sources_with_packet_on_and_off(self):
        for policy, enabled, request in product(
            ("public_home", "sealed_test"), (False, True), (REQUEST, DID_REQUEST),
        ):
            with self.subTest(policy=policy, packet_enabled=enabled, request=request), self._packet_configuration(enabled, 8810):
                self.assertEqual(bot.ordinary_chat_configuration()["effective"], enabled)
                prompt, metadata = await self.runtime._direct_prompt_async(
                    policy, request=request, privileged=False,
                )
                self._assert_sources(prompt, metadata["prompt_source_bases"])

    async def test_real_batch_combines_sources_and_sends_one_normal_response(self):
        for policy, enabled, request in product(
            ("public_home", "sealed_test"), (False, True), (REQUEST, DID_REQUEST),
        ):
            channel_id = 8811 + len(self.runtime.channel_ids)
            with self.subTest(policy=policy, packet_enabled=enabled, request=request), self._packet_configuration(enabled, channel_id):
                self.assertEqual(bot.ordinary_chat_configuration()["effective"], enabled)
                channel, generation, guard = await self.runtime._batch(
                    policy, request=request, answer=self._provider_answer, privileged=False,
                )
                generation.assert_awaited_once()
                guard.assert_awaited_once()
                self.assertEqual(channel.sent, [ANSWER])
                self._assert_sources(
                    generation.await_args.args[0], guard.await_args.kwargs["prompt_source_bases"],
                )

    async def test_public_discord_source_scope_change_is_detected_before_send(self):
        prompt, metadata = await self.runtime._direct_prompt_async(
            "public_home", request=REQUEST, privileged=False,
        )
        bases = tuple(metadata["prompt_source_bases"])
        self._assert_sources(prompt, bases)
        with sqlite3.connect(bot.DB_FILE) as conn:
            conn.execute("UPDATE conversations SET channel_policy='internal_controlled' WHERE id=7101")
        self.assertEqual(bot.prompt_source_basis_failure(bases), "conversation_source_changed")
        _prompt, _fresh, changed, replacement_failed = bot.refresh_prompt_source_bases(prompt, bases)
        self.assertIn("conversation", changed)
        self.assertTrue(replacement_failed)  # Existing conversation source fence owns stale delivery.
        for basis in bases:
            if isinstance(basis, bot.FinalizedShowPromptSourceBasis):
                refreshed, changed = bot.refresh_prompt_source_basis(basis)
                self.assertFalse(changed)
                self.assertIn(TIKTOK_COMMENT, refreshed.rendered_context)

    async def test_discord_author_change_is_detected_before_send(self):
        prompt, metadata = await self.runtime._direct_prompt_async(
            "sealed_test", request=REQUEST, privileged=False,
        )
        bases = tuple(metadata["prompt_source_bases"])
        self._assert_sources(prompt, bases)
        with sqlite3.connect(bot.DB_FILE) as conn:
            conn.execute("UPDATE conversations SET user_name='Test Renamed' WHERE id=7101")
        self.assertEqual(bot.prompt_source_basis_failure(bases), "conversation_source_changed")


if __name__ == "__main__":
    unittest.main()
