import asyncio
import json
import os
import unittest
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl_source_context_injection as sci
import bnl01_bot


class SourceContextSubjectParserTests(unittest.TestCase):
    def test_detects_what_do_we_know_about_subject(self):
        self.assertEqual(sci.parse_source_context_subjects("@BNL-01 what do we know about Hellcat?"), ["Hellcat"])

    def test_detects_source_file_phrase(self):
        self.assertEqual(sci.parse_source_context_subjects("Emerald source file"), ["Emerald"])

    def test_detects_connection_pair(self):
        self.assertEqual(sci.parse_source_context_subjects("is Crow connected to Orion?"), ["Crow", "Orion"])

    def test_detects_case_file_review(self):
        self.assertEqual(sci.parse_source_context_subjects("summarize Mac Modem's case file for internal review."), ["Mac Modem"])

    def test_rejects_generic_casual_messages(self):
        for text in ("that was funny", "what are we doing today", "tell me a joke about pizza", "thanks BNL"):
            self.assertEqual(sci.parse_source_context_subjects(text), [])


class SourceContextPolicyTests(unittest.TestCase):
    def test_allows_privileged_direct_rd_and_sealed_test(self):
        self.assertTrue(sci.should_inject_source_context(channel_policy="internal_controlled", channel_name="research-and-development", privileged=True, direct_interaction=True))
        self.assertTrue(sci.should_inject_source_context(channel_policy="sealed_test", channel_name="bnl-testing", privileged=True, direct_interaction=True))

    def test_rejects_public_channel_injection(self):
        for policy in ("public_home", "public_context", "public_selective"):
            self.assertFalse(sci.should_inject_source_context(channel_policy=policy, channel_name="general-chat", privileged=True, direct_interaction=True))

    def test_rejects_unauthorized_user_injection(self):
        self.assertFalse(sci.should_inject_source_context(channel_policy="internal_controlled", channel_name="research-and-development", privileged=False, direct_interaction=True))

    def test_rejects_protected_channels(self):
        self.assertFalse(sci.should_inject_source_context(channel_policy="protected_system", channel_name="welcome", privileged=True, direct_interaction=True))
        self.assertFalse(sci.should_inject_source_context(channel_policy="protected_system", channel_name="episode-tracker", privileged=True, direct_interaction=True))

    def test_rejects_website_relay_and_ambient_paths(self):
        self.assertFalse(sci.should_inject_source_context(channel_policy="internal_controlled", channel_name="research-and-development", privileged=True, direct_interaction=True, route="website_relay"))
        self.assertFalse(sci.should_inject_source_context(channel_policy="sealed_test", channel_name="bnl-testing", privileged=True, direct_interaction=True, route="ambient"))


class SourceContextBlockTests(unittest.TestCase):
    def test_source_file_found_adds_compact_context_block(self):
        result = {
            "ok": True,
            "found": True,
            "matchKind": "exact",
            "data": {"sourceFile": {"name": "Hellcat", "knownFacts": ["recurring approved-lane entity"], "missingInfo": ["confirm public-safe role"], "publicSafetyNotes": "internal-only until reviewed", "nextRecommendedAction": "review source notes"}},
        }
        block = sci.build_source_context_block(result, "Hellcat")
        self.assertIn("SOURCE FILE / INTERNAL CASE FILE CONTEXT", block)
        self.assertIn("Subject: Hellcat", block)
        self.assertIn("Case file status: internal working case file, not public dossier", block)
        self.assertIn("Known facts: recurring approved-lane entity", block)
        self.assertNotIn("{", block)

    def test_no_source_file_found_gives_bounded_no_file_context(self):
        block = sci.build_source_context_block({"ok": True, "found": False, "data": {"found": False}}, "Unknown")
        self.assertIn("no Source File found", block)
        self.assertIn("Do not hallucinate a case file", block)

    def test_confirmed_alias_is_alias_match_not_public_identity_claim(self):
        result = {"ok": True, "found": True, "matchKind": "confirmed alias", "data": {"matchedAlias": "ShadowsPit", "sourceFile": {"name": "Deadite Ash"}}}
        block = sci.build_source_context_block(result, "ShadowsPit")
        self.assertIn("Match kind: confirmed alias", block)
        self.assertIn("internal routing/context only", block)
        self.assertNotIn("public identity proof", block.lower())

    def test_source_blind_warning_remains_warning_not_fact(self):
        result = {"ok": True, "found": True, "matchKind": "exact", "data": {"sourceFile": {"name": "Emerald", "knownFacts": ["approved-lane mention"], "warnings": ["source-blind memory support only"]}}}
        block = sci.build_source_context_block(result, "Emerald")
        self.assertIn("Known facts: approved-lane mention", block)
        self.assertIn("Warnings: source-blind memory support only", block)

    def test_internal_only_note_is_not_framed_as_public_safe(self):
        result = {"ok": True, "found": True, "matchKind": "exact", "data": {"sourceFile": {"name": "Shortbabe", "internalNotes": "operator review only"}}}
        block = sci.build_source_context_block(result, "Shortbabe")
        self.assertIn("Review-only/internal notes: operator review only", block)
        self.assertIn("Do not present review-only", block)

    def test_lookup_limit_asks_to_narrow_beyond_two_subjects(self):
        calls = []
        block, results = sci.build_source_context_for_subjects(["Hellcat", "Emerald", "Crow"], lambda query: calls.append(query) or {})
        self.assertEqual(calls, [])
        self.assertEqual(results, [])
        self.assertIn("more than two", block)


class SourceContextBotIntegrationTests(unittest.TestCase):
    class Perms:
        administrator = False
        manage_guild = False
        manage_messages = False
        kick_members = False
        ban_members = False

    class User:
        def __init__(self, user_id, perms=None):
            self.id = user_id
            self.guild_permissions = perms or SourceContextBotIntegrationTests.Perms()
            self.roles = []
            self.display_name = "Operator"
            self.bot = False

    class Guild:
        id = 123
        owner_id = 999
        name = "Guild"
        def get_member(self, user_id):
            return None

    class Channel:
        id = 456
        name = "research-and-development"
        parent = None
        def __init__(self, guild):
            self.guild = guild

    class Message:
        def __init__(self, user_id=999, channel_name="research-and-development"):
            self.author = SourceContextBotIntegrationTests.User(user_id)
            self.guild = SourceContextBotIntegrationTests.Guild()
            self.channel = SourceContextBotIntegrationTests.Channel(self.guild)
            self.channel.name = channel_name

    def test_bot_helper_loads_context_for_authorized_rd_direct_question(self):
        bnl01_bot.BNL_OWNER_USER_ID = 999
        fake_result = {"ok": True, "found": True, "matchKind": "exact", "data": {"sourceFile": {"name": "Hellcat", "knownFacts": ["fact"]}}}
        with mock.patch.object(bnl01_bot, "lookup_source_file", return_value=fake_result) as lookup_mock:
            block = asyncio.run(bnl01_bot.maybe_build_source_context_for_direct_message(self.Message(), "what do we know about Hellcat?", "internal_controlled"))
        lookup_mock.assert_called_once()
        self.assertIn("Subject: Hellcat", block)

    def test_bot_helper_rejects_public_channel(self):
        bnl01_bot.BNL_OWNER_USER_ID = 999
        with mock.patch.object(bnl01_bot, "lookup_source_file") as lookup_mock:
            block = asyncio.run(bnl01_bot.maybe_build_source_context_for_direct_message(self.Message(channel_name="general-chat"), "what do we know about Hellcat?", "public_context"))
        lookup_mock.assert_not_called()
        self.assertEqual(block, "")

    def test_bot_helper_rejects_unauthorized_user(self):
        bnl01_bot.BNL_OWNER_USER_ID = 999
        with mock.patch.object(bnl01_bot, "lookup_source_file") as lookup_mock:
            block = asyncio.run(bnl01_bot.maybe_build_source_context_for_direct_message(self.Message(user_id=111), "what do we know about Hellcat?", "internal_controlled"))
        lookup_mock.assert_not_called()
        self.assertEqual(block, "")

    def test_prompt_accepts_source_context_without_breaking_existing_args(self):
        with mock.patch.object(bnl01_bot, "get_user_profile", return_value=("Operator", None)), \
             mock.patch.object(bnl01_bot, "should_allow_greeting", return_value=False), \
             mock.patch.object(bnl01_bot, "choose_response_style", return_value=("concise", "style rule")), \
             mock.patch.object(bnl01_bot, "build_user_memory_context", return_value="No durable memory."), \
             mock.patch.object(bnl01_bot, "build_broadcast_memory_context", return_value=""):
            prompt, _allow, _style = bnl01_bot.build_user_aware_prompt(1, 1, "Operator", "what do we know about Hellcat?", channel_name="research-and-development", privileged=True, channel_policy="internal_controlled", source_context_block="SOURCE FILE / INTERNAL CASE FILE CONTEXT:\nSubject: Hellcat")
        self.assertIn("SOURCE FILE / INTERNAL CASE FILE CONTEXT", prompt)
        self.assertIn("Prompt operator authority: approved_operator_context", prompt)

    def test_diagnostics_include_source_context_safe_metadata(self):
        diag = bnl01_bot.build_dossier_recommendation_diagnostics()
        for key in ("source_context_injection_available", "source_context_injection_enabled", "source_context_last_status", "source_context_last_subjects", "source_context_last_found_count", "source_context_last_match_kinds", "source_context_last_error_status"):
            self.assertIn(key, diag)
        self.assertNotIn("secret", json.dumps(diag).lower())


if __name__ == "__main__":
    unittest.main()
