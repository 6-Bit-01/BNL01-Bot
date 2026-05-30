import json
import os
import unittest
import urllib.error

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl_dossier_recommendations as dossier
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


class DossierRecommendationPayloadTests(unittest.TestCase):
    def test_payload_defaults_and_deterministic_ingest_key(self):
        payload = dossier.build_manual_dossier_recommendation(
            "Signal Witch",
            "Mentioned in R&D and should be reviewed for a future dossier.",
            ["rd_context"],
        )
        again = dossier.build_manual_dossier_recommendation(
            "Signal Witch",
            "Mentioned in R&D and should be reviewed for a future dossier.",
            ["rd_context"],
        )

        self.assertEqual(payload["createdBy"], "bnl")
        self.assertEqual(payload["confidence"], "medium")
        self.assertEqual(payload["sourceLanes"], ["rd_context"])
        self.assertEqual(payload["type"], "new_subject")
        self.assertEqual(payload["subjectKey"], "signal-witch")
        self.assertEqual(payload["ingestKey"], again["ingestKey"])
        self.assertTrue(payload["ingestKey"].startswith("bnl:dossier:new_subject:signal-witch:rd_context:"))
        self.assertNotIn("BNL_DOSSIER_INGEST_TOKEN", payload)

    def test_parse_command_subject_reason_and_lanes(self):
        matched, payload, error = dossier.parse_manual_dossier_recommendation_command(
            "!bnl dossier recommend Signal Witch | Review for future dossier. | lanes=rd_context,broadcast_memory"
        )
        self.assertTrue(matched)
        self.assertFalse(error)
        self.assertEqual(payload["subjectName"], "Signal Witch")
        self.assertEqual(payload["reason"], "Review for future dossier.")
        self.assertEqual(payload["sourceLanes"], ["rd_context", "broadcast_memory"])

    def test_parse_command_rejects_empty_reason(self):
        matched, payload, error = dossier.parse_manual_dossier_recommendation_command(
            "!bnl dossier recommend Signal Witch |   "
        )
        self.assertTrue(matched)
        self.assertIsNone(payload)
        self.assertIn("Reason", error)


class DossierRecommendationSenderTests(unittest.TestCase):
    def test_missing_token_fails_safely(self):
        result = dossier.send_dossier_recommendation(
            {"subjectName": "Signal Witch", "reason": "Review", "sourceLanes": ["rd_context"]},
            environ={},
        )
        self.assertFalse(result["ok"])
        self.assertIsNone(result["status"])
        self.assertNotIn("secret", json.dumps(result).lower())

    def test_token_is_used_in_authorization_header(self):
        opener = FakeOpener(FakeResponse(200, '{"ok": true, "duplicate": false, "recommendationId": "rec_1"}'))
        result = dossier.send_dossier_recommendation(
            {"subjectName": "Signal Witch", "reason": "Review", "sourceLanes": ["rd_context"]},
            environ={"BNL_DOSSIER_INGEST_TOKEN": "secret-token", "BNL_DOSSIER_INGEST_URL": "https://example.test/ingest"},
            opener=opener,
        )
        request, timeout = opener.requests[0]
        self.assertTrue(result["ok"])
        self.assertEqual(result["recommendationId"], "rec_1")
        self.assertEqual(timeout, dossier.DOSSIER_INGEST_TIMEOUT_SECONDS)
        self.assertEqual(request.get_header("Authorization"), "Bearer secret-token")
        self.assertNotIn("secret-token", json.dumps(result))

    def test_duplicate_response_returns_duplicate_true(self):
        opener = FakeOpener(FakeResponse(200, '{"ok": true, "duplicate": true, "id": "rec_1"}'))
        result = dossier.send_dossier_recommendation(
            {"subjectName": "Signal Witch", "reason": "Review", "sourceLanes": ["rd_context"]},
            environ={"BNL_DOSSIER_INGEST_TOKEN": "secret-token"},
            opener=opener,
        )
        self.assertTrue(result["ok"])
        self.assertTrue(result["duplicate"])

    def test_401_response_returns_safe_failure(self):
        http_error = urllib.error.HTTPError("https://example.test", 401, "Unauthorized", hdrs=None, fp=None)
        opener = FakeOpener(error=http_error)
        result = dossier.send_dossier_recommendation(
            {"subjectName": "Signal Witch", "reason": "Review", "sourceLanes": ["rd_context"]},
            environ={"BNL_DOSSIER_INGEST_TOKEN": "secret-token"},
            opener=opener,
        )
        self.assertFalse(result["ok"])
        self.assertEqual(result["status"], 401)
        self.assertNotIn("secret-token", json.dumps(result))

    def test_network_error_returns_safe_failure(self):
        opener = FakeOpener(error=urllib.error.URLError("timed out"))
        result = dossier.send_dossier_recommendation(
            {"subjectName": "Signal Witch", "reason": "Review", "sourceLanes": ["rd_context"]},
            environ={"BNL_DOSSIER_INGEST_TOKEN": "secret-token"},
            opener=opener,
        )
        self.assertFalse(result["ok"])
        self.assertIsNone(result["status"])
        self.assertIn("unreachable", result["error"])


class DossierRecommendationPermissionTests(unittest.TestCase):
    class Perms:
        administrator = False
        manage_guild = False
        manage_messages = False
        kick_members = False
        ban_members = False

    class User:
        def __init__(self, user_id):
            self.id = user_id

    class Role:
        def __init__(self, role_id):
            self.id = role_id

    class Member(User):
        def __init__(self, user_id, perms=None, roles=None):
            super().__init__(user_id)
            self.guild_permissions = perms or DossierRecommendationPermissionTests.Perms()
            self.roles = roles or []

    class Guild:
        owner_id = 100

    def test_non_admin_cannot_send_recommendation(self):
        bnl01_bot.BNL_OWNER_USER_ID = 999
        bnl01_bot.BNL_MOD_ROLE_ID = 555
        member = self.Member(101)
        self.assertFalse(bnl01_bot.can_send_dossier_recommendation(member, member, self.Guild()))

    def test_admin_owner_or_operator_can_send_recommendation(self):
        bnl01_bot.BNL_OWNER_USER_ID = 999
        bnl01_bot.BNL_MOD_ROLE_ID = 555

        owner_user = self.Member(999)
        self.assertTrue(bnl01_bot.can_send_dossier_recommendation(owner_user, owner_user, self.Guild()))

        admin_perms = self.Perms()
        admin_perms.administrator = True
        admin = self.Member(101, perms=admin_perms)
        self.assertTrue(bnl01_bot.can_send_dossier_recommendation(admin, admin, self.Guild()))

        mod = self.Member(102, roles=[self.Role(555)])
        self.assertTrue(bnl01_bot.can_send_dossier_recommendation(mod, mod, self.Guild()))


if __name__ == "__main__":
    unittest.main()
