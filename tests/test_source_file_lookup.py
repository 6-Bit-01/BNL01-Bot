import asyncio
import json
import os
import unittest
import urllib.error
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl_source_file_lookup as source_lookup
import bnl01_bot


class FakeResponse:
    def __init__(self, status, body):
        self.status = status
        self._body = body.encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def read(self):
        return self._body

    def getcode(self):
        return self.status


class FakeOpener:
    def __init__(self, response=None, error=None):
        self.response = response
        self.error = error
        self.requests = []

    def open(self, request, timeout=None):
        self.requests.append((request, timeout))
        if self.error:
            raise self.error
        return self.response


class SourceFileLookupParserTests(unittest.TestCase):
    def test_command_parser_handles_subject_lookup(self):
        matched, query, error = source_lookup.parse_source_file_lookup_command("!bnl source lookup Signal Witch")
        self.assertTrue(matched)
        self.assertFalse(error)
        self.assertEqual(query["lookupKey"], "subject")
        self.assertEqual(query["subject"], "Signal Witch")

    def test_command_parser_handles_alias_lookup(self):
        matched, query, error = source_lookup.parse_source_file_lookup_command("!bnl source find alias=ShadowsPit")
        self.assertTrue(matched)
        self.assertFalse(error)
        self.assertEqual(query["lookupKey"], "alias")
        self.assertEqual(query["alias"], "ShadowsPit")

    def test_command_parser_handles_candidate_id_lookup(self):
        matched, query, error = source_lookup.parse_source_file_lookup_command("!bnl source lookup candidateId=abc123")
        self.assertTrue(matched)
        self.assertFalse(error)
        self.assertEqual(query["lookupKey"], "candidateId")
        self.assertEqual(query["candidateId"], "abc123")

    def test_command_parser_handles_normalized_name_lookup_and_alias_command(self):
        matched, query, error = source_lookup.parse_source_file_lookup_command("!bnl dossier lookup normalizedName=signal-witch")
        self.assertTrue(matched)
        self.assertFalse(error)
        self.assertEqual(query["lookupKey"], "normalizedName")
        self.assertEqual(query["normalizedName"], "signal-witch")


class SourceFileLookupHttpTests(unittest.TestCase):
    def test_lookup_sends_token_header_without_exposing_token(self):
        opener = FakeOpener(FakeResponse(200, json.dumps({"found": False})))
        result = source_lookup.lookup_source_file(
            {"lookupKey": "alias", "lookupValue": "ShadowsPit", "alias": "ShadowsPit"},
            environ={
                "BNL_SOURCE_FILE_READ_TOKEN": "secret-token",
                "BNL_SOURCE_FILE_READ_URL": "https://example.test/api/bnl/source-files",
            },
            opener=opener,
        )
        self.assertTrue(result["ok"])
        request, timeout = opener.requests[0]
        self.assertEqual(timeout, source_lookup.SOURCE_FILE_READ_TIMEOUT_SECONDS)
        self.assertEqual(request.get_header("X-bnl-source-file-read-token"), "secret-token")
        self.assertIn("alias=ShadowsPit", request.full_url)
        self.assertNotIn("secret-token", request.full_url)
        self.assertNotIn("secret-token", json.dumps(result))

    def test_401_response_is_handled_safely(self):
        opener = FakeOpener(error=urllib.error.HTTPError("https://example.test", 401, "Unauthorized", {}, None))
        result = source_lookup.lookup_source_file(
            {"lookupKey": "subject", "lookupValue": "Signal Witch", "subject": "Signal Witch"},
            environ={"BNL_SOURCE_FILE_READ_TOKEN": "secret-token"},
            opener=opener,
        )
        self.assertFalse(result["ok"])
        self.assertEqual(result["status"], 401)
        response = source_lookup.format_source_file_lookup_response(result, "Signal Witch")
        self.assertIn("authorization was rejected", response)
        self.assertNotIn("secret-token", json.dumps(result) + response)

    def test_network_failure_is_handled_safely(self):
        opener = FakeOpener(error=urllib.error.URLError("timed out"))
        result = source_lookup.lookup_source_file(
            {"lookupKey": "subject", "lookupValue": "Signal Witch", "subject": "Signal Witch"},
            environ={"BNL_SOURCE_FILE_READ_TOKEN": "secret-token"},
            opener=opener,
        )
        self.assertFalse(result["ok"])
        self.assertIn("unreachable", result["error"])

    def test_invalid_json_is_handled_safely(self):
        opener = FakeOpener(FakeResponse(200, "not-json"))
        result = source_lookup.lookup_source_file(
            {"lookupKey": "subject", "lookupValue": "Signal Witch", "subject": "Signal Witch"},
            environ={"BNL_SOURCE_FILE_READ_TOKEN": "secret-token"},
            opener=opener,
        )
        self.assertFalse(result["ok"])
        self.assertEqual(result["kind"], "invalid_json")


class SourceFileLookupFormatterTests(unittest.TestCase):
    def test_found_exact_match_response_summarizes_safely_and_truncates(self):
        result = {
            "ok": True,
            "found": True,
            "matchKind": "exact subject",
            "data": {
                "found": True,
                "sourceFile": {
                    "name": "Signal Witch",
                    "status": "needs_review",
                    "candidateType": "entity",
                    "confidence": "medium",
                    "sourceLanes": ["bnl_dynamic_candidate_discovery", "broadcast_memory"],
                    "evidenceSummary": "mentioned in broadcast memory and R&D as a recurring entity. " + ("extra " * 80),
                    "knownFacts": ["recurring signal", "not a public dossier yet"],
                    "missingInfo": ["confirm public-safe description"],
                    "publicSafetyNotes": "Internal source-file context — not a public dossier yet.",
                    "doNotSay": ["do not claim public identity proof"],
                    "aliases": {"proposed": ["ShadowsPit"], "confirmed": []},
                    "recommendationCount": 2,
                    "ownerReviewState": "pending",
                    "duplicateWarnings": ["possible duplicate: Signal Hex"],
                    "nextRecommendedAction": "review source notes and decide whether to draft a public dossier",
                    "discordId": "1234567890",
                    "paymentCustomerId": "cus_secret",
                },
            },
        }
        response = source_lookup.format_source_file_lookup_response(result, "Signal Witch")
        self.assertIn("Source File: Signal Witch", response)
        self.assertIn("Match: exact subject", response)
        self.assertIn("Status: needs_review", response)
        self.assertIn("Aliases: 1 proposed, 0 confirmed", response)
        self.assertIn("Recommendations: 2 attached", response)
        self.assertLessEqual(len(response), 1850)
        self.assertNotIn("1234567890", response)
        self.assertNotIn("cus_secret", response)

    def test_confirmed_alias_response_does_not_claim_public_identity_proof(self):
        result = {
            "ok": True,
            "found": True,
            "matchKind": "confirmed alias",
            "data": {"matchedAlias": "ShadowsPit", "sourceFile": {"name": "Deadite Ash", "status": "needs_review"}},
        }
        response = source_lookup.format_source_file_lookup_response(result, "ShadowsPit")
        self.assertIn("Source File: Deadite Ash", response)
        self.assertIn("Match: confirmed alias “ShadowsPit”", response)
        self.assertIn("do not expose private identity unless public-safe", response)
        self.assertNotIn("public identity proof", response.lower())

    def test_possible_matches_response_does_not_claim_certainty(self):
        result = {
            "ok": True,
            "found": False,
            "data": {
                "found": False,
                "possibleMatches": [
                    {"name": "Deadite Ash", "matchKind": "proposed alias match"},
                    {"name": "ShadowsPit", "matchKind": "partial name match"},
                ],
            },
        }
        response = source_lookup.format_source_file_lookup_response(result, "ShadowsPit")
        self.assertIn("No confirmed source-file match", response)
        self.assertIn("Deadite Ash — proposed alias match", response)
        self.assertIn("owner/admin review", response)
        self.assertNotIn("Source File:", response)

    def test_not_found_response_is_clear(self):
        result = {"ok": True, "found": False, "data": {"found": False}}
        response = source_lookup.format_source_file_lookup_response(result, "Unknown Subject")
        self.assertIn("No BNL Source File found for “Unknown Subject.”", response)
        self.assertIn("Next:", response)


class SourceFileLookupBotTests(unittest.TestCase):
    class Perms:
        administrator = False
        manage_guild = False
        manage_messages = False
        kick_members = False
        ban_members = False

    class User:
        def __init__(self, user_id, perms=None, roles=None):
            self.id = user_id
            self.guild_permissions = perms or SourceFileLookupBotTests.Perms()
            self.roles = roles or []

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
        def __init__(self, user_id):
            self.author = SourceFileLookupBotTests.User(user_id)
            self.guild = SourceFileLookupBotTests.Guild()
            self.channel = SourceFileLookupBotTests.Channel(self.guild)
            self.replies = []

        async def reply(self, text):
            self.replies.append(text)

    def test_unauthorized_users_cannot_use_lookup(self):
        bnl01_bot.BNL_OWNER_USER_ID = 999
        message = self.Message(111)
        with mock.patch.object(bnl01_bot, "lookup_source_file") as lookup_mock:
            handled = asyncio.run(bnl01_bot.maybe_handle_source_file_lookup_command(message, "!bnl source lookup Signal Witch"))
        self.assertTrue(handled)
        lookup_mock.assert_not_called()
        self.assertIn("operator-only", message.replies[-1])

    def test_authorized_lookup_replies_and_calls_no_mutation_functions(self):
        bnl01_bot.BNL_OWNER_USER_ID = 999
        message = self.Message(999)
        fake_result = {"ok": True, "found": False, "data": {"found": False}}
        with mock.patch.object(bnl01_bot, "lookup_source_file", return_value=fake_result) as lookup_mock, \
             mock.patch.object(bnl01_bot, "send_dossier_recommendation") as send_mock, \
             mock.patch.object(bnl01_bot, "discover_candidate_recommendations") as discover_mock:
            handled = asyncio.run(bnl01_bot.maybe_handle_source_file_lookup_command(message, "!bnl source lookup Signal Witch"))
        self.assertTrue(handled)
        lookup_mock.assert_called_once()
        send_mock.assert_not_called()
        discover_mock.assert_not_called()
        self.assertIn("No BNL Source File found", message.replies[-1])

    def test_diagnostics_expose_only_safe_metadata(self):
        with mock.patch.dict(os.environ, {"BNL_SOURCE_FILE_READ_TOKEN": "secret-token", "BNL_SOURCE_FILE_READ_URL": "https://example.test/api"}, clear=False):
            diag = bnl01_bot.build_dossier_recommendation_diagnostics()
        self.assertTrue(diag["source_file_read_token_configured"])
        self.assertTrue(diag["source_file_read_url_configured"])
        self.assertIn("source_file_last_lookup_status", diag)
        self.assertIn("source_context_injection_available", diag)
        self.assertIn("source_context_injection_enabled", diag)
        self.assertNotIn("secret-token", json.dumps(diag))

    def test_existing_source_lookup_command_still_works(self):
        bnl01_bot.BNL_OWNER_USER_ID = 999
        message = self.Message(999)
        fake_result = {"ok": True, "found": True, "matchKind": "exact", "data": {"sourceFile": {"name": "Hellcat"}}}
        with mock.patch.object(bnl01_bot, "lookup_source_file", return_value=fake_result) as lookup_mock:
            handled = asyncio.run(bnl01_bot.maybe_handle_source_file_lookup_command(message, "!bnl source lookup Hellcat"))
        self.assertTrue(handled)
        lookup_mock.assert_called_once()
        self.assertIn("Source File: Hellcat", message.replies[-1])


if __name__ == "__main__":
    unittest.main()
