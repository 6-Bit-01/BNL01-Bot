import os
import unittest
from contextlib import ExitStack, contextmanager
from types import SimpleNamespace
from unittest import mock
from unittest.mock import AsyncMock


os.environ.setdefault("GEMINI_API_KEY", "test-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-token")

try:
    import bnl01_bot
except ModuleNotFoundError as exc:  # Minimal test images may omit Discord.
    if exc.name != "discord":
        raise
    bnl01_bot = None


MUTATING_JOURNAL_ACTIONS = (
    "create",
    "regenerate",
    "reject",
    "approve",
    "retry",
    "run-daily",
    "run-weekly",
    "rehydrate",
)


def permissions(**overrides):
    values = {
        "administrator": False,
        "manage_guild": False,
        "manage_messages": False,
        "kick_members": False,
        "ban_members": False,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def member(user_id, *, roles=(), **permission_overrides):
    return SimpleNamespace(
        id=user_id,
        display_name=f"User{user_id}",
        bot=False,
        roles=list(roles),
        guild_permissions=permissions(**permission_overrides),
    )


@contextmanager
def patched_broadcast_memory_route(processed_entries=None):
    with ExitStack() as stack:
        stack.enter_context(
            mock.patch.object(
                bnl01_bot,
                "client",
                SimpleNamespace(user=SimpleNamespace(id=4242)),
            )
        )
        for name, return_value in (
            ("_register_direct_conversation_ingress", {}),
            ("get_guild_config", None),
            ("resolve_channel_policy", "broadcast_memory"),
            ("upsert_user_profile", None),
            ("conversation_surface_for_channel_policy", "command_only"),
            ("conversation_surface_allows_free_speak", False),
            ("allow_passive_memory_for_policy", False),
            ("is_direct_bnl_target", False),
            ("resolve_discord_user_mentions_for_conversation", "Official show note."),
            ("is_human_to_human_tag_only_turn", False),
            (
                "build_message_media_context",
                {"present": False, "included": False, "count": 0},
            ),
            ("append_media_context_to_text", "Official show note."),
            ("record_recent_room_event_from_message", None),
            ("_is_previous_message_request", False),
            ("maybe_record_live_community_presence", None),
            ("_is_recent_direct_followup", False),
            ("_is_recent_conversation_continuation", False),
            ("build_current_turn_addressing_context", {}),
            ("maybe_record_member_activity_event", None),
            ("is_internal_channel_redirect_candidate", False),
        ):
            stack.enter_context(
                mock.patch.object(bnl01_bot, name, return_value=return_value)
            )
        for name in (
            "maybe_handle_provider_status_command",
            "maybe_handle_debug_last_route_command",
            "maybe_handle_channel_audit_command",
            "maybe_handle_backfill_command",
            "maybe_handle_journal_command",
            "maybe_handle_population_scan_command",
            "maybe_handle_source_file_refresh_command",
            "maybe_handle_source_file_enrichment_command",
            "maybe_handle_source_file_lookup_command",
            "maybe_handle_entity_context_resolver_command",
            "maybe_handle_entity_activity_summary_command",
            "maybe_handle_dossier_recommendation_command",
        ):
            stack.enter_context(
                mock.patch.object(
                    bnl01_bot,
                    name,
                    new_callable=AsyncMock,
                    return_value=False,
                )
            )
        process = stack.enter_context(
            mock.patch.object(
                bnl01_bot,
                "process_broadcast_memory_notes",
                new_callable=AsyncMock,
                return_value=processed_entries or [],
            )
        )
        add = stack.enter_context(
            mock.patch.object(
                bnl01_bot,
                "add_broadcast_memory_entry",
                return_value=1,
            )
        )
        yield process, add


@unittest.skipIf(bnl01_bot is None, "discord.py is not installed")
class OwnerOnlyPermissionTests(unittest.TestCase):
    def test_manual_journal_mutations_allow_only_configured_owner(self):
        guild = SimpleNamespace(id=10, owner_id=2)
        configured_mod_role = SimpleNamespace(id=555, name="MOD")
        named_admin_role = SimpleNamespace(id=777, name="BARCODE_ADMIN")
        denied_actors = (
            ("server_owner", SimpleNamespace(id=2), member(2)),
            (
                "administrator",
                SimpleNamespace(id=3),
                member(3, administrator=True),
            ),
            (
                "manage_guild",
                SimpleNamespace(id=4),
                member(4, manage_guild=True),
            ),
            (
                "configured_mod",
                SimpleNamespace(id=5),
                member(5, roles=(configured_mod_role,)),
            ),
            (
                "named_admin_role",
                SimpleNamespace(id=6),
                member(6, roles=(named_admin_role,)),
            ),
            ("ordinary_user", SimpleNamespace(id=7), member(7)),
        )

        with mock.patch.object(bnl01_bot, "BNL_OWNER_USER_ID", 999), \
             mock.patch.object(bnl01_bot, "BNL_MOD_ROLE_ID", 555):
            for action in MUTATING_JOURNAL_ACTIONS:
                self.assertEqual(
                    "",
                    bnl01_bot.journal_command_permission_denial(
                        action,
                        SimpleNamespace(id=999),
                        member(999),
                        guild,
                    ),
                    action,
                )
                for label, user, actor_member in denied_actors:
                    self.assertEqual(
                        "configured_owner_required",
                        bnl01_bot.journal_command_permission_denial(
                            action,
                            user,
                            actor_member,
                            guild,
                        ),
                        f"{action}:{label}",
                    )

    def test_missing_owner_id_fails_closed_for_every_mutation(self):
        guild = SimpleNamespace(id=10, owner_id=2)
        actor = SimpleNamespace(id=2)
        actor_member = member(2, administrator=True)
        with mock.patch.object(bnl01_bot, "BNL_OWNER_USER_ID", 0):
            for action in MUTATING_JOURNAL_ACTIONS:
                self.assertEqual(
                    "owner_user_id_not_configured",
                    bnl01_bot.journal_command_permission_denial(
                        action,
                        actor,
                        actor_member,
                        guild,
                    ),
                    action,
                )

    def test_preview_and_status_keep_existing_operator_access(self):
        guild = SimpleNamespace(id=10, owner_id=2)
        configured_mod_role = SimpleNamespace(id=555, name="MOD")
        operator = SimpleNamespace(id=5)
        operator_member = member(5, roles=(configured_mod_role,))
        ordinary = SimpleNamespace(id=7)

        with mock.patch.object(bnl01_bot, "BNL_OWNER_USER_ID", 999), \
             mock.patch.object(bnl01_bot, "BNL_MOD_ROLE_ID", 555):
            for action in ("preview", "status"):
                self.assertEqual(
                    "",
                    bnl01_bot.journal_command_permission_denial(
                        action,
                        operator,
                        operator_member,
                        guild,
                    ),
                )
                self.assertEqual(
                    "operator_permission_required",
                    bnl01_bot.journal_command_permission_denial(
                        action,
                        ordinary,
                        member(7),
                        guild,
                    ),
                )


@unittest.skipIf(bnl01_bot is None, "discord.py is not installed")
class OwnerOnlySurfaceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        bnl01_bot._broadcast_memory_denial_last_at.clear()

    async def test_text_journal_mutations_deny_broad_operator_before_side_effects(self):
        actor = SimpleNamespace(id=3)
        actor_member = member(3, administrator=True)
        guild = SimpleNamespace(
            id=10,
            owner_id=2,
            get_member=mock.Mock(return_value=actor_member),
        )
        message = SimpleNamespace(
            guild=guild,
            author=actor,
            channel=SimpleNamespace(id=20, name="research-and-development"),
            reply=AsyncMock(),
        )
        commands = (
            "!bnl journal create | hours=72",
            "!bnl journal regenerate entry-1 | hours=72",
            "!bnl journal reject entry-1",
            "!bnl journal approve entry-1 | hash=abc",
            "!bnl journal retry entry-1",
            "!bnl journal run-daily",
            "!bnl journal run-weekly",
            "!bnl journal rehydrate",
        )

        with mock.patch.object(bnl01_bot, "BNL_OWNER_USER_ID", 999), \
             mock.patch.object(
                 bnl01_bot,
                 "can_send_dossier_recommendation",
                 return_value=True,
             ), \
             mock.patch.object(
                 bnl01_bot,
                 "generate_and_store_journal_draft",
             ) as create, \
             mock.patch.object(bnl01_bot, "regenerate_journal_draft") as regenerate, \
             mock.patch.object(bnl01_bot, "reject_journal_draft") as reject, \
             mock.patch.object(bnl01_bot, "approve_journal_draft") as approve, \
             mock.patch.object(
                 bnl01_bot,
                 "release_prepared_journal_entry",
             ) as retry, \
             mock.patch.object(
                 bnl01_bot,
                 "rehydrate_published_journal_entries",
             ) as rehydrate, \
             mock.patch.object(
                 bnl01_bot,
                 "run_journal_automation_once",
                 new_callable=AsyncMock,
             ) as force_run:
            for command in commands:
                handled = await bnl01_bot.maybe_handle_journal_command(
                    message,
                    command,
                )
                self.assertTrue(handled, command)

        self.assertEqual(len(commands), message.reply.await_count)
        for call in message.reply.await_args_list:
            self.assertIn("owner-only", call.args[0])
        for mutation in (
            create,
            regenerate,
            reject,
            approve,
            retry,
            rehydrate,
            force_run,
        ):
            mutation.assert_not_called()

    async def test_official_broadcast_memory_fails_closed_and_denies_non_owner(self):
        guild = SimpleNamespace(id=10)
        channel = SimpleNamespace(id=20)
        non_owner_message = SimpleNamespace(
            guild=guild,
            channel=channel,
            author=SimpleNamespace(id=3),
            reply=AsyncMock(),
        )
        owner_message = SimpleNamespace(
            guild=guild,
            channel=channel,
            author=SimpleNamespace(id=999),
            reply=AsyncMock(),
        )

        with mock.patch.object(bnl01_bot, "BNL_OWNER_USER_ID", 0):
            denied = await bnl01_bot.maybe_reject_unauthorized_official_broadcast_memory(
                non_owner_message,
                "Official show note.",
            )
        self.assertTrue(denied)
        self.assertIn(
            "not configured",
            non_owner_message.reply.await_args.args[0],
        )

        bnl01_bot._broadcast_memory_denial_last_at.clear()
        non_owner_message.reply.reset_mock()
        with mock.patch.object(bnl01_bot, "BNL_OWNER_USER_ID", 999):
            denied = await bnl01_bot.maybe_reject_unauthorized_official_broadcast_memory(
                non_owner_message,
                "Official show note.",
            )
            allowed = await bnl01_bot.maybe_reject_unauthorized_official_broadcast_memory(
                owner_message,
                "Official show note.",
            )

        self.assertTrue(denied)
        self.assertIn(
            "configured owner",
            non_owner_message.reply.await_args.args[0],
        )
        self.assertFalse(allowed)
        owner_message.reply.assert_not_awaited()

    async def test_broadcast_memory_route_denies_admin_and_allows_owner_storage(self):
        admin = member(3, administrator=True)
        owner = member(999)
        guild = SimpleNamespace(
            id=10,
            owner_id=2,
            get_member=mock.Mock(side_effect=lambda user_id: admin if user_id == 3 else owner),
        )
        channel = SimpleNamespace(id=20, name="bnl-broadcast-memory")

        def message_for(actor):
            return SimpleNamespace(
                id=100 + actor.id,
                content="Official show note.",
                author=actor,
                guild=guild,
                channel=channel,
                reference=None,
                attachments=[],
                embeds=[],
                reply=AsyncMock(),
            )

        denied_message = message_for(admin)
        owner_message = message_for(owner)
        processed = [{
            "entry_type": "notable_moment",
            "episode_date": "2026-07-23",
            "public_safe": True,
        }]

        with mock.patch.object(bnl01_bot, "BNL_OWNER_USER_ID", 999), \
             patched_broadcast_memory_route(processed) as (process, add):
            await bnl01_bot.on_message(denied_message)
            process.assert_not_awaited()
            add.assert_not_called()
            self.assertIn("configured owner", denied_message.reply.await_args.args[0])

            await bnl01_bot.on_message(owner_message)

        process.assert_awaited_once_with(owner_message)
        add.assert_called_once_with(10, owner_message, processed[0])
        self.assertIn("Logged 1 entry", owner_message.reply.await_args.args[0])


if __name__ == "__main__":
    unittest.main()
