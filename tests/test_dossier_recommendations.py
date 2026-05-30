import json
import os
import unittest
import urllib.error

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl_dossier_recommendations as dossier
import bnl_dossier_source_packets as packets
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

    def test_parse_command_ignores_unsupported_lanes_in_packet_builder(self):
        matched, payload, error = dossier.parse_manual_dossier_recommendation_command(
            "!bnl dossier recommend Signal Witch | Review. | lanes=bogus,broadcast_memory"
        )
        self.assertTrue(matched)
        self.assertFalse(error)
        packet_result = packets.build_dossier_recommendation_packet(
            payload["subjectName"],
            payload["reason"],
            payload["sourceLanes"],
            guild_id=123,
            options={"broadcast_memory_reader": lambda *a, **k: []},
        )
        self.assertEqual(packet_result["payload"]["sourceLanes"], ["rd_context"])

    def test_parse_command_rejects_empty_subject(self):
        matched, payload, error = dossier.parse_manual_dossier_recommendation_command(
            "!bnl dossier recommend   | Reason"
        )
        self.assertTrue(matched)
        self.assertIsNone(payload)
        self.assertIn("Subject", error)

    def test_parse_command_rejects_empty_reason(self):
        matched, payload, error = dossier.parse_manual_dossier_recommendation_command(
            "!bnl dossier recommend Signal Witch |   "
        )
        self.assertTrue(matched)
        self.assertIsNone(payload)
        self.assertIn("Reason", error)


class DossierSourcePacketTests(unittest.TestCase):
    def test_subject_matching_is_conservative(self):
        self.assertEqual(packets.normalize_subject_name("  Signal   Witch "), "Signal Witch")
        self.assertTrue(packets.contains_exact_subject_mention("Signal Witch should be reviewed.", "signal witch"))
        self.assertTrue(packets.contains_compact_subject_mention("Signal-Witch should be reviewed.", "Signal Witch"))
        self.assertFalse(packets.contains_subject_mention("Signal is noisy but unrelated.", "Signal Witch"))
        self.assertFalse(packets.contains_subject_mention("Witch signal is reversed.", "Signal Witch"))

    def test_manual_packet_defaults_and_no_token(self):
        result = packets.build_dossier_recommendation_packet(
            "Signal Witch",
            "Operator says Signal Witch should be reviewed.",
            ["rd_context"],
        )
        packet = result["packet"]
        payload = result["payload"]
        self.assertEqual(packet["sourceLanes"], ["rd_context"])
        self.assertEqual(packet["confidence"], "low")
        self.assertIn("preferred display name", packet["missingInfo"])
        self.assertIn("Verify public-safe wording before publishing.", packet["publicSafetyNotes"])
        self.assertNotIn("BNL_DOSSIER_INGEST_TOKEN", json.dumps(packet))
        self.assertNotIn("Authorization", json.dumps(payload))

    def test_broadcast_memory_collection_matches_and_caps_snippets(self):
        long_summary = "Signal Witch " + ("context " * 80) + "123456789012345678"

        def fake_reader(guild_id, public_only=False, limit=500):
            self.assertEqual(guild_id, 123)
            self.assertFalse(public_only)
            self.assertLessEqual(limit, 500)
            return [
                ("2026-05-29", long_summary, "notable_moment", "medium", 0, 0, "internal", None, None, 1, 0),
                ("2026-05-22", "Unrelated artist note.", "notable_moment", "medium", 0, 0, "internal", None, None, 1, 0),
            ]

        evidence = packets.collect_broadcast_memory_evidence(123, "Signal Witch", reader=fake_reader)
        self.assertEqual(len(evidence), 1)
        self.assertEqual(evidence[0]["lane"], "broadcast_memory")
        self.assertLessEqual(len(evidence[0]["summary"]), packets.MAX_SNIPPET_LENGTH)
        self.assertNotIn("123456789012345678", evidence[0]["summary"])
        self.assertEqual(packets.collect_broadcast_memory_evidence(None, "Signal Witch", reader=fake_reader), [])
        self.assertEqual(packets.collect_broadcast_memory_evidence(123, "Signal Witch", reader=lambda *a, **k: (_ for _ in ()).throw(RuntimeError())), [])

    def test_packet_builder_manual_plus_broadcast_and_stable_ingest_key(self):
        def fake_reader(guild_id, public_only=False, limit=500):
            return [("2026-05-29", "Signal-Witch came up in broadcast memory as a recurring source candidate.")]

        first = packets.build_dossier_recommendation_packet(
            "Signal Witch",
            "Testing source packet builder.",
            ["rd_context", "broadcast_memory", "unsupported_lane"],
            guild_id=123,
            options={"broadcast_memory_reader": fake_reader},
        )
        second = packets.build_dossier_recommendation_packet(
            "Signal Witch",
            "Testing source packet builder.",
            ["rd_context", "broadcast_memory"],
            guild_id=123,
            options={"broadcast_memory_reader": fake_reader},
        )
        payload = first["payload"]
        self.assertEqual(payload["sourceLanes"], ["rd_context", "broadcast_memory"])
        self.assertEqual(payload["confidence"], "medium")
        self.assertLessEqual(len(payload["evidenceSummary"]), packets.MAX_EVIDENCE_SUMMARY_LENGTH)
        self.assertEqual(payload["ingestKey"], second["payload"]["ingestKey"])

    def test_no_automatic_scanning_constructs_in_packet_module(self):
        with open("bnl_dossier_source_packets.py", encoding="utf-8") as handle:
            source = handle.read()
        self.assertNotIn("@tasks.loop", source)
        self.assertNotIn("history(", source)
        self.assertNotIn("create_source_file", source)
        self.assertNotIn("create_draft", source)
        self.assertNotIn("send_dossier_recommendation", source)


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
