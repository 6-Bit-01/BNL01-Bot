import os
import unittest
from types import SimpleNamespace
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl01_bot


class FakeCustomEmoji:
    def __init__(self, name, emoji_id, *, available=True, usable=True):
        self.name = name
        self.id = emoji_id
        self.available = available
        self._usable = usable

    def is_usable(self):
        return self._usable

    def __str__(self):
        return f"<:{self.name}:{self.id}>"


class FakeReactionMessage:
    def __init__(self, content, emojis=()):
        self.content = content
        self.channel = SimpleNamespace(id=9020)
        self.guild = SimpleNamespace(emojis=list(emojis))
        self.reactions = []
        self.fail_custom = False

    async def add_reaction(self, emoji):
        if self.fail_custom and not isinstance(emoji, str):
            raise RuntimeError("custom emoji rejected")
        self.reactions.append(emoji)


class ContextualReactionTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        bnl01_bot._last_reaction_by_channel.clear()

    def tearDown(self):
        bnl01_bot._last_reaction_by_channel.clear()

    def test_reaction_probability_is_slightly_reduced(self):
        self.assertEqual(bnl01_bot.REACTION_CHANCE, 0.22)

    def test_contextual_unicode_pool_includes_expanded_broadcast_reactions(self):
        message = FakeReactionMessage("listen to this new track")
        with (
            mock.patch.object(bnl01_bot.random, "random", return_value=1.0),
            mock.patch.object(bnl01_bot.random, "choice", return_value="🎧") as choose,
        ):
            reaction = bnl01_bot.choose_contextual_reaction(message)

        self.assertEqual(reaction, "🎧")
        self.assertIn("🎧", choose.call_args.args[0])

    def test_only_available_usable_allowlisted_custom_emoji_can_be_selected(self):
        allowed = FakeCustomEmoji("DJ_FloppyDisc", 1)
        not_allowlisted = FakeCustomEmoji("party_blob", 2)
        unavailable = FakeCustomEmoji("barcode", 3, available=False)
        unusable = FakeCustomEmoji("sixbit", 4, usable=False)
        message = FakeReactionMessage(
            "the radio mix is live",
            (not_allowlisted, unavailable, unusable, allowed),
        )

        with (
            mock.patch.object(bnl01_bot.random, "random", return_value=0.0),
            mock.patch.object(bnl01_bot.random, "choice", side_effect=lambda options: options[0]),
        ):
            reaction = bnl01_bot.choose_contextual_reaction(message)

        self.assertIs(reaction, allowed)

    async def test_failed_custom_reaction_falls_back_to_unicode(self):
        allowed = FakeCustomEmoji("barcode", 5)
        message = FakeReactionMessage("signal received", (allowed,))
        message.fail_custom = True

        with (
            mock.patch.object(bnl01_bot.random, "random", return_value=0.0),
            mock.patch.object(bnl01_bot.random, "choice", side_effect=lambda options: options[0]),
        ):
            added = await bnl01_bot.add_contextual_reaction(message)

        self.assertTrue(added)
        self.assertEqual(len(message.reactions), 1)
        self.assertIsInstance(message.reactions[0], str)

    async def test_unicode_reaction_failure_is_nonfatal(self):
        message = FakeReactionMessage("signal received")

        async def reject(_emoji):
            raise RuntimeError("reaction rejected")

        message.add_reaction = reject
        with (
            mock.patch.object(bnl01_bot.random, "random", return_value=1.0),
            mock.patch.object(bnl01_bot.random, "choice", side_effect=lambda options: options[0]),
        ):
            added = await bnl01_bot.add_contextual_reaction(message)

        self.assertFalse(added)


class GuardedBatchTypingTests(unittest.IsolatedAsyncioTestCase):
    async def test_batch_guard_regeneration_uses_existing_managed_typing_session(self):
        provider = mock.AsyncMock(return_value="Grounded replacement.")
        optional_typing = mock.AsyncMock(
            side_effect=AssertionError("batch guard must not open a second typing session")
        )

        with (
            mock.patch.object(
                bnl01_bot,
                "contains_fake_lookup_claim",
                side_effect=lambda candidate: candidate == "Bad lookup claim.",
            ),
            mock.patch.object(
                bnl01_bot,
                "_contains_unsupported_source_authority_claim",
                return_value=False,
            ),
            mock.patch.object(
                bnl01_bot,
                "detect_normal_chat_presentation_mode_leak",
                return_value=False,
            ),
            mock.patch.object(
                bnl01_bot,
                "is_generic_non_answer_response",
                return_value=False,
            ),
            mock.patch.object(bnl01_bot, "get_gemini_response", provider),
            mock.patch.object(
                bnl01_bot,
                "get_gemini_response_with_optional_typing",
                optional_typing,
            ),
        ):
            response, diagnostics = await bnl01_bot.apply_guarded_response_regeneration(
                "Bad lookup claim.",
                prompt="Answer the current message.",
                user_id=100,
                guild_id=7700,
                route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                channel_policy="sealed_test",
                current_user_text="Tell me what happened.",
                batch_generation_id=12,
            )

        self.assertEqual(response, "Grounded replacement.")
        self.assertTrue(diagnostics["source_grounding_regenerated"])
        provider.assert_awaited_once()
        optional_typing.assert_not_awaited()
