import asyncio
import json
import os
import sqlite3
import tempfile
import unittest
import urllib.error
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl_dossier_candidate_discovery as discovery
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


    def test_manual_packet_can_include_entity_summary_context_when_local_memory_exists(self):
        tmp = tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False)
        tmp.close()
        conn = sqlite3.connect(tmp.name)
        try:
            conn.execute("CREATE TABLE user_profiles (user_id INTEGER, guild_id INTEGER, display_name TEXT, preferred_name TEXT, last_seen TEXT, last_greeting_at TEXT)")
            conn.execute("CREATE TABLE relationship_journal (id INTEGER PRIMARY KEY, user_id INTEGER, guild_id INTEGER, entry_type TEXT, summary TEXT, timestamp TEXT)")
            conn.execute("INSERT INTO user_profiles VALUES (1,123,'Signal Witch','Signal Witch',NULL,NULL)")
            conn.execute("INSERT INTO relationship_journal VALUES (NULL,1,123,'note','Signal Witch private relationship/context note for owner review.','now')")
            conn.commit()

            result = packets.build_dossier_recommendation_packet(
                "Signal Witch",
                "Operator says Signal Witch should be reviewed.",
                ["rd_context"],
                guild_id=123,
                options={"db_path": tmp.name},
            )
            payload = result["payload"]

            self.assertTrue(any("Local profile match found for Signal Witch" in item for item in payload["knownContext"]))
            self.assertTrue(any("relationship/context" in item for item in payload["relationshipSignals"]))
            self.assertFalse(any("relationship" in item.lower() for item in payload["publicSafePossibilities"]))
            self.assertIn("queue/submission identity is not connected yet", payload["recommendedAction"])
            normal_text = json.dumps({
                "knownContext": payload.get("knownContext"),
                "relationshipSignals": payload.get("relationshipSignals"),
                "publicSafePossibilities": payload.get("publicSafePossibilities"),
                "privateOnlyNotes": payload.get("privateOnlyNotes"),
                "notPublicYet": payload.get("notPublicYet"),
            })
            self.assertNotIn("user_profiles/local_profile_observed", normal_text)
            self.assertIn("rd_context/R&D/operator note", payload["rawProvenance"]["sourceLabels"])
            self.assertIn("user_profiles/local_profile_observed", payload["rawProvenance"]["sourceLabels"])
            self.assertIn("relationship_journal/local_relationship_trace", payload["rawProvenance"]["sourceLabels"])
        finally:
            conn.close()
            os.unlink(tmp.name)

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



class DossierCandidateDiscoveryTests(unittest.TestCase):
    def test_discovers_candidate_subjects_from_approved_lane_text(self):
        def broadcast_reader(guild_id, public_only=False, limit=500):
            self.assertEqual(guild_id, 123)
            self.assertFalse(public_only)
            return [
                ("2026-05-29", "Signal Witch came up as a recurring lore entity and source file candidate. Signal Witch should have a dossier review."),
                ("2026-05-22", "Signal Witch returned as a priority signal in broadcast memory."),
            ]

        result = discovery.discover_candidate_recommendations(
            123,
            ["rd_context", "broadcast_memory"],
            broadcast_memory_reader=broadcast_reader,
            rd_context=[{"main_subject": "Priority Signal", "user_request": "Priority Signal deserves a source file."}],
        )
        names = [candidate["subjectName"] for candidate in result["candidates"]]
        self.assertIn("Signal Witch", names)
        payload = next(payload for payload in result["payloads"] if payload["subjectName"] == "Signal Witch")
        self.assertEqual(payload["type"], "new_subject")
        self.assertEqual(payload["createdBy"], "bnl")
        self.assertEqual(payload["ingestSource"], discovery.DISCOVERY_INGEST_SOURCE)
        self.assertIn("broadcast_memory", payload["sourceLanes"])
        self.assertIn("review", payload["reason"].lower())



    def test_medium_confidence_non_generic_candidate_is_sent_review_only(self):
        result = discovery.discover_candidate_recommendations(
            123,
            ["broadcast_memory"],
            broadcast_memory_reader=lambda *a, **k: [("2026-05-29", "Anita Cable surfaced as a dossier candidate for source-file review.")],
        )
        payload = next(payload for payload in result["payloads"] if payload["subjectName"] == "Anita Cable")

        self.assertEqual(payload["type"], "new_subject")
        self.assertIn(payload["confidence"], {"low", "medium"})
        self.assertEqual(payload.get("recommendedCategory"), "Entity")
        self.assertIn("Medium-confidence BNL discovery. Review before converting, merging, aliasing, drafting, or publishing.", " ".join(payload["publicSafetyNotes"]))
        self.assertEqual(result["mediumConfidenceCount"], 1)

    def test_bare_title_case_one_off_remains_withheld_with_reason(self):
        result = discovery.discover_candidate_recommendations(
            123,
            ["broadcast_memory"],
            broadcast_memory_reader=lambda *a, **k: [("2026-05-29", "Anita Cable appeared once during a quiet recap.")],
        )

        self.assertEqual(result["payloads"], [])
        self.assertGreaterEqual(result["withheldReasonCounts"].get("title_only_low_evidence", 0), 1)

    def test_repeated_contextual_subject_passes_as_medium_confidence(self):
        result = discovery.discover_candidate_recommendations(
            None,
            ["rd_context"],
            rd_context=[
                {"main_subject": "Signal Witch", "user_request": "Signal Witch noted in dossier context for source-lane review."},
                {"main_subject": "Signal Witch", "user_request": "Signal Witch returned in entity context for later review."},
            ],
        )
        payload = next(payload for payload in result["payloads"] if payload["subjectName"] == "Signal Witch")

        self.assertEqual(payload["confidence"], "medium")
        self.assertEqual(result["mediumConfidenceCount"], 1)

    def test_weak_generic_subject_reasons_are_tracked(self):
        result = discovery.discover_candidate_recommendations(
            123,
            ["broadcast_memory"],
            broadcast_memory_reader=lambda *a, **k: [("2026-05-29", "Source file for next. Add needs a source file. Owner deserves a dossier.")],
        )

        self.assertEqual(result["payloads"], [])
        self.assertGreaterEqual(result["withheldReasonCounts"].get("weak_generic_subject", 0), 3)
        self.assertNotIn("evidence", json.dumps(result["withheldReasonCounts"]).lower())

    def test_dynamic_discovery_payload_uses_valid_website_taxonomy(self):
        rows = [("2026-05-29", "Anita Cable needs a source file. Anita Cable is a recurring entity.")]
        result = discovery.discover_candidate_recommendations(123, ["broadcast_memory"], broadcast_memory_reader=lambda *a, **k: rows)
        payload = next(payload for payload in result["payloads"] if payload["subjectName"] == "Anita Cable")

        self.assertEqual(payload["type"], "new_subject")
        self.assertEqual(payload["ingestSource"], discovery.DISCOVERY_INGEST_SOURCE)
        self.assertEqual(payload.get("recommendedCategory"), "Entity")
        self.assertNotIn(payload.get("recommendedCategory"), {"new_source_file_candidate", "system_concept_candidate", "identity_alias_review"})
        self.assertIn("Internal discovery classification: new_source_file_candidate", " ".join(payload["publicSafetyNotes"]))

    def test_system_concept_candidate_uses_interface_taxonomy(self):
        result = discovery.discover_candidate_recommendations(
            123,
            ["broadcast_memory"],
            broadcast_memory_reader=lambda *a, **k: [("2026-05-29", "Source file for Mac Modem. Mac Modem is a recurring interface system concept.")],
        )
        payload = next(payload for payload in result["payloads"] if payload["subjectName"] == "Mac Modem")

        self.assertEqual(payload["type"], "new_subject")
        self.assertEqual(payload.get("recommendedCategory"), "Interface")
        self.assertEqual(payload.get("recommendedKind"), "system")
        self.assertEqual(payload.get("recommendedEcosystemLane"), "infrastructure")
        self.assertNotIn(payload.get("recommendedCategory"), {"new_source_file_candidate", "system_concept_candidate", "identity_alias_review"})

    def test_identity_alias_review_is_review_only_identity_link(self):
        result = discovery.discover_candidate_recommendations(
            123,
            ["broadcast_memory"],
            broadcast_memory_reader=lambda *a, **k: [("2026-05-29", "Identity review for Anita Cable. Anita Cable may be an alias and needs identity review.")],
        )
        payload = next(payload for payload in result["payloads"] if payload["subjectName"] == "Anita Cable")

        self.assertEqual(payload["type"], "identity_link")
        self.assertNotIn("recommendedCategory", payload)
        self.assertNotIn("recommendedKind", payload)
        self.assertIn("Internal discovery classification: identity_alias_review", " ".join(payload["publicSafetyNotes"]))

    def test_payload_builder_drops_invalid_recommended_category(self):
        payload = dossier.build_dossier_recommendation_payload(
            {
                "type": "new_subject",
                "subjectName": "Signal Witch",
                "reason": "Review.",
                "sourceLanes": ["rd_context"],
                "recommendedCategory": "new_source_file_candidate",
            }
        )

        self.assertNotIn("recommendedCategory", payload)

    def test_weak_generic_subjects_are_withheld(self):
        result = discovery.discover_candidate_recommendations(
            123,
            ["broadcast_memory"],
            broadcast_memory_reader=lambda *a, **k: [
                ("2026-05-29", "Source file for next. Add needs a source file. Origin deserves a dossier. Bit needs a source file."),
                ("2026-05-22", "Next is a recurring entity. Origin needs a source file. Bit deserves a dossier."),
            ],
        )

        self.assertEqual(result["payloads"], [])
        self.assertGreaterEqual(result["withheldCount"], 0)

    def test_explicit_legitimate_subjects_pass_with_strong_evidence(self):
        rows = [
            ("2026-05-29", "Anita Cable needs a source file. Anita Cable is a recurring entity."),
            ("2026-05-22", "Source file for Signal Witch. Signal Witch deserves a dossier."),
        ]
        result = discovery.discover_candidate_recommendations(123, ["broadcast_memory"], broadcast_memory_reader=lambda *a, **k: rows)
        names = {payload["subjectName"] for payload in result["payloads"]}

        self.assertIn("Anita Cable", names)
        self.assertIn("Signal Witch", names)

    def test_discovery_parse_command_has_no_subject_argument(self):
        matched, options, error = discovery.parse_dossier_discovery_command(
            "!bnl dossier discover candidates | lanes=rd_context,broadcast_memory | limit=7 | dry_run=true"
        )
        self.assertTrue(matched)
        self.assertFalse(error)
        self.assertEqual(options["lanes"], ["rd_context", "broadcast_memory"])
        self.assertEqual(options["limit"], 7)
        self.assertTrue(options["dry_run"])

    def test_deterministic_ingest_keys_and_dedupes_repeated_candidates(self):
        rows = [
            ("2026-05-29", "Signal Witch needs a source file. Signal Witch is a recurring candidate."),
            ("2026-05-22", "Signal Witch needs a source file. Signal Witch is a recurring candidate."),
        ]
        first = discovery.discover_candidate_recommendations(123, ["broadcast_memory"], broadcast_memory_reader=lambda *a, **k: rows)
        second = discovery.discover_candidate_recommendations(123, ["broadcast_memory"], broadcast_memory_reader=lambda *a, **k: rows)
        signal_payloads = [payload for payload in first["payloads"] if payload["subjectName"] == "Signal Witch"]
        self.assertEqual(len(signal_payloads), 1)
        self.assertEqual(signal_payloads[0]["ingestKey"], next(payload for payload in second["payloads"] if payload["subjectName"] == "Signal Witch")["ingestKey"])
        self.assertTrue(signal_payloads[0]["ingestKey"].startswith("bnl:dossier:bnl_dynamic_candidate_discovery:"))

    def test_withholds_low_confidence_one_off_noise(self):
        result = discovery.discover_candidate_recommendations(
            123,
            ["broadcast_memory"],
            broadcast_memory_reader=lambda *a, **k: [("2026-05-29", "A one-off mention of Sheila happened once.")],
        )
        self.assertEqual(result["payloads"], [])
        self.assertGreaterEqual(result["withheldCount"], 1)

    def test_ignores_unsupported_lanes_and_does_not_scan_unapproved_sources(self):
        result = discovery.discover_candidate_recommendations(
            123,
            ["member_activity_events", "memory_tiers", "broadcast_memory"],
            broadcast_memory_reader=lambda *a, **k: [("2026-05-29", "ShadowsPit needs a source file. ShadowsPit is recurring.")],
        )
        self.assertEqual(result["lanes"], ["broadcast_memory"])
        self.assertTrue(all(payload["sourceLanes"] == ["broadcast_memory"] for payload in result["payloads"]))

    def test_evidence_snippets_are_capped_and_redacted(self):
        raw = "Signal Witch needs a source file. " + "context " * 90 + "<@123456789012345678> 123456789012345678 https://private.example/path user@example.com"
        result = discovery.discover_candidate_recommendations(
            123,
            ["broadcast_memory"],
            broadcast_memory_reader=lambda *a, **k: [("2026-05-29", raw), ("2026-05-22", "Signal Witch recurring source file candidate.")],
        )
        payload = next(payload for payload in result["payloads"] if payload["subjectName"] == "Signal Witch")
        self.assertLessEqual(len(payload["evidenceSummary"]), packets.MAX_EVIDENCE_SUMMARY_LENGTH)
        self.assertNotIn("123456789012345678", payload["evidenceSummary"])
        self.assertNotIn("private.example", payload["evidenceSummary"])
        self.assertNotIn("user@example.com", payload["evidenceSummary"])

    def test_discovery_module_has_no_separate_workflow_or_scanning_constructs(self):
        with open("bnl_dossier_candidate_discovery.py", encoding="utf-8") as handle:
            source = handle.read()
        forbidden = [
            "history(",
            "member_activity_events",
            "memory_tiers",
            "create_source_file",
            "create_draft",
            "publish_dossier",
            "auto_confirm",
            "merge_source",
            "send_dossier_recommendation",
            "@tasks.loop",
        ]
        for token in forbidden:
            self.assertNotIn(token, source)

    def test_diagnostics_expose_safe_candidate_discovery_metadata(self):
        diag = bnl01_bot.build_dossier_recommendation_diagnostics()
        self.assertTrue(diag["candidate_discovery_available"])
        self.assertFalse(diag["candidate_discovery_enabled"])
        self.assertEqual(diag["candidate_discovery_approved_lanes"], ["rd_context", "broadcast_memory", "community_presence"])
        self.assertNotIn("token", json.dumps(diag.get("candidate_discovery_last_source_lanes", [])).lower())


class DossierDiscoveryRoutingTests(unittest.TestCase):
    class Guild:
        id = 123
        owner_id = 100
        def get_member(self, user_id):
            return None

    class Author:
        def __init__(self, user_id):
            self.id = user_id
            self.guild_permissions = DossierRecommendationPermissionTests.Perms()
            self.roles = []

    class Channel:
        id = 1369051635657084970
        name = "research-and-development"

    class Message:
        def __init__(self, user_id=999):
            self.author = DossierDiscoveryRoutingTests.Author(user_id)
            self.guild = DossierDiscoveryRoutingTests.Guild()
            self.channel = DossierDiscoveryRoutingTests.Channel()
            self.replies = []
        async def reply(self, text):
            self.replies.append(text)

    def test_unauthorized_users_cannot_run_discovery(self):
        bnl01_bot.BNL_OWNER_USER_ID = 999
        message = self.Message(user_id=101)
        handled = asyncio.run(bnl01_bot.maybe_handle_dossier_recommendation_command(message, "!bnl dossier discover candidates"))
        self.assertTrue(handled)
        self.assertIn("operator-only", message.replies[-1])

    def test_dry_run_operator_trigger_does_not_send_recommendations(self):
        bnl01_bot.BNL_OWNER_USER_ID = 999
        message = self.Message(user_id=999)
        fake_discovery = {
            "candidates": [{"subjectName": "Signal Witch", "category": "new_source_file_candidate", "payload": {"subjectName": "Signal Witch"}}],
            "payloads": [{"subjectName": "Signal Witch"}],
            "withheldCount": 0,
            "duplicateCount": 0,
        }
        with mock.patch.object(bnl01_bot, "discover_candidate_recommendations", return_value=fake_discovery) as discover_mock, \
             mock.patch.object(bnl01_bot, "send_dossier_recommendation") as send_mock:
            handled = asyncio.run(bnl01_bot.maybe_handle_dossier_recommendation_command(message, "!bnl dossier discover candidates | dry_run=true"))
        self.assertTrue(handled)
        discover_mock.assert_called_once()
        send_mock.assert_not_called()
        self.assertIn("Dry run: 1 sendable, 0 withheld", message.replies[-1])
        self.assertIn("Sendable: Signal Witch", message.replies[-1])

    def test_command_sends_normal_review_only_recommendations_when_threshold_met(self):
        bnl01_bot.BNL_OWNER_USER_ID = 999
        message = self.Message(user_id=999)
        fake_payload = {"type": "new_subject", "subjectName": "Signal Witch", "reason": "Review only", "sourceLanes": ["broadcast_memory"]}
        fake_discovery = {"candidates": [{"subjectName": "Signal Witch", "payload": fake_payload}], "payloads": [fake_payload], "withheldCount": 2, "duplicateCount": 0}
        with mock.patch.object(bnl01_bot, "discover_candidate_recommendations", return_value=fake_discovery), \
             mock.patch.object(bnl01_bot, "send_dossier_recommendation", return_value={"ok": True, "duplicate": False, "status": 200}) as send_mock:
            handled = asyncio.run(bnl01_bot.maybe_handle_dossier_recommendation_command(message, "!bnl dossier discover candidates | limit=1"))
        self.assertTrue(handled)
        send_mock.assert_called_once_with(fake_payload)
        self.assertIn("1 recommendations sent", message.replies[-1])
        self.assertIn("2 withheld", message.replies[-1])


    def test_dry_run_reports_sendable_and_withheld_counts(self):
        bnl01_bot.BNL_OWNER_USER_ID = 999
        message = self.Message(user_id=999)
        fake_discovery = {
            "candidates": [{"subjectName": "Anita Cable", "payload": {"subjectName": "Anita Cable"}}],
            "payloads": [{"subjectName": "Anita Cable"}],
            "withheldCount": 6,
            "duplicateCount": 0,
            "sendableCandidateCount": 1,
            "withheldReasonCounts": {"title_only_low_evidence": 6},
            "topWithheldReason": "title_only_low_evidence",
            "mediumConfidenceCount": 1,
            "strongConfidenceCount": 0,
        }
        with mock.patch.object(bnl01_bot, "discover_candidate_recommendations", return_value=fake_discovery), \
             mock.patch.object(bnl01_bot, "send_dossier_recommendation"):
            handled = asyncio.run(bnl01_bot.maybe_handle_dossier_recommendation_command(message, "!bnl dossier discover candidates | dry_run=true"))

        self.assertTrue(handled)
        self.assertIn("Dry run: 1 sendable, 6 withheld", message.replies[-1])
        self.assertIn("Sendable: Anita Cable", message.replies[-1])
        self.assertIn("Main withheld reason: title only low evidence", message.replies[-1])

    def test_command_reply_reports_withheld_reason_summary_and_diagnostics(self):
        bnl01_bot.BNL_OWNER_USER_ID = 999
        message = self.Message(user_id=999)
        fake_payload = {"type": "new_subject", "subjectName": "Signal Witch", "reason": "Review only", "sourceLanes": ["broadcast_memory"]}
        fake_discovery = {
            "candidates": [{"subjectName": "Signal Witch", "payload": fake_payload}],
            "payloads": [fake_payload],
            "withheldCount": 6,
            "duplicateCount": 0,
            "sendableCandidateCount": 1,
            "withheldReasonCounts": {"title_only_low_evidence": 4, "weak_generic_subject": 2},
            "topWithheldReason": "title_only_low_evidence",
            "mediumConfidenceCount": 1,
            "strongConfidenceCount": 0,
        }
        with mock.patch.object(bnl01_bot, "discover_candidate_recommendations", return_value=fake_discovery), \
             mock.patch.object(bnl01_bot, "send_dossier_recommendation", return_value={"ok": True, "duplicate": False, "status": 200}):
            handled = asyncio.run(bnl01_bot.maybe_handle_dossier_recommendation_command(message, "!bnl dossier discover candidates | limit=1"))

        self.assertTrue(handled)
        self.assertIn("Withheld: 4 title only low evidence, 2 weak generic subject", message.replies[-1])
        diag = bnl01_bot.build_dossier_recommendation_diagnostics()
        self.assertEqual(diag["candidate_discovery_last_sendable_candidate_count"], 1)
        self.assertEqual(diag["candidate_discovery_last_withheld_reason_counts"], {"title_only_low_evidence": 4, "weak_generic_subject": 2})
        self.assertEqual(diag["candidate_discovery_last_medium_confidence_count"], 1)
        self.assertEqual(diag["candidate_discovery_last_strong_confidence_count"], 0)
        self.assertEqual(diag["candidate_discovery_last_top_withheld_reason"], "title_only_low_evidence")
        self.assertNotIn("Anita Cable", json.dumps(diag["candidate_discovery_last_withheld_reason_counts"]))

    def test_no_send_reply_uses_main_withheld_reason_guidance(self):
        bnl01_bot.BNL_OWNER_USER_ID = 999
        message = self.Message(user_id=999)
        fake_discovery = {
            "candidates": [],
            "payloads": [],
            "withheldCount": 9,
            "duplicateCount": 0,
            "sendableCandidateCount": 0,
            "withheldReasonCounts": {"title_only_low_evidence": 9},
            "topWithheldReason": "title_only_low_evidence",
            "mediumConfidenceCount": 0,
            "strongConfidenceCount": 0,
        }
        with mock.patch.object(bnl01_bot, "discover_candidate_recommendations", return_value=fake_discovery), \
             mock.patch.object(bnl01_bot, "send_dossier_recommendation"):
            handled = asyncio.run(bnl01_bot.maybe_handle_dossier_recommendation_command(message, "!bnl dossier discover candidates"))

        self.assertTrue(handled)
        self.assertIn("Dossier discovery complete: 0 sent, 0 duplicates, 9 withheld, 0 failed", message.replies[-1])
        self.assertIn("Main withheld reason: title only low evidence", message.replies[-1])
        self.assertIn("Try adding clearer source-file or recurring-entity notes", message.replies[-1])

    def test_failed_dynamic_sends_are_counted_in_operator_reply(self):
        bnl01_bot.BNL_OWNER_USER_ID = 999
        message = self.Message(user_id=999)
        payloads = [
            {"type": "new_subject", "subjectName": "Anita Cable", "reason": "Review only", "sourceLanes": ["broadcast_memory"]},
            {"type": "new_subject", "subjectName": "Signal Witch", "reason": "Review only", "sourceLanes": ["broadcast_memory"]},
        ]
        fake_discovery = {
            "candidates": [{"subjectName": payload["subjectName"], "payload": payload} for payload in payloads],
            "payloads": payloads,
            "withheldCount": 4,
            "duplicateCount": 0,
        }
        with mock.patch.object(bnl01_bot, "discover_candidate_recommendations", return_value=fake_discovery), \
             mock.patch.object(bnl01_bot, "send_dossier_recommendation", return_value={"ok": False, "duplicate": False, "status": 400}):
            handled = asyncio.run(bnl01_bot.maybe_handle_dossier_recommendation_command(message, "!bnl dossier discover candidates | limit=2"))

        self.assertTrue(handled)
        self.assertIn("0 sent", message.replies[-1])
        self.assertIn("4 withheld", message.replies[-1])
        self.assertIn("2 failed", message.replies[-1])
        self.assertIn("First failure: HTTP 400", message.replies[-1])
        diag = bnl01_bot.build_dossier_recommendation_diagnostics()
        self.assertEqual(diag["candidate_discovery_last_failed_count"], 2)
        self.assertEqual(diag["candidate_discovery_last_first_failure_status"], "HTTP 400")


class CommunityCandidateScoutingTests(unittest.TestCase):
    def community_rows(self):
        return [
            {
                "subjectName": "Emerald",
                "mentionCount": 3,
                "directInteractionCount": 1,
                "operatorMentionCount": 0,
                "daysActive": 2,
                "connectionNotes": [],
                "category": "community_regular_candidate",
                "signalCount": 5,
            },
            {
                "subjectName": "Orion",
                "mentionCount": 1,
                "directInteractionCount": 0,
                "operatorMentionCount": 1,
                "daysActive": 1,
                "connectionNotes": ["mentioned through Crow"],
                "category": "introduced_subject_candidate",
                "signalCount": 2,
            },
        ]

    def test_community_presence_lane_is_accepted_when_requested(self):
        matched, options, error = discovery.parse_dossier_discovery_command(
            "!bnl dossier discover candidates | lanes=rd_context,broadcast_memory,community_presence | limit=10"
        )
        self.assertTrue(matched)
        self.assertFalse(error)
        self.assertEqual(options["lanes"], ["rd_context", "broadcast_memory", "community_presence"])

    def test_community_scout_alias_wraps_discovery_pipeline(self):
        matched, options, error = discovery.parse_dossier_discovery_command("!bnl community scout candidates | limit=5")
        self.assertTrue(matched)
        self.assertFalse(error)
        self.assertEqual(options["lanes"], ["community_presence"])
        self.assertEqual(options["limit"], 5)

    def test_community_presence_ignored_when_no_reader_available(self):
        result = discovery.discover_candidate_recommendations(123, ["community_presence"])
        self.assertEqual(result["payloads"], [])
        self.assertEqual(result["sourceItemCount"], 0)

    def test_recurring_community_subject_becomes_review_candidate(self):
        result = discovery.discover_candidate_recommendations(
            123,
            ["community_presence"],
            community_presence_reader=lambda *a, **k: self.community_rows(),
        )
        payload = next(payload for payload in result["payloads"] if payload["subjectName"] == "Emerald")
        self.assertEqual(payload["type"], "new_subject")
        self.assertEqual(payload.get("recommendedCategory"), "Personnel")
        self.assertEqual(payload.get("recommendedKind"), "community_member")
        self.assertIn("community scouting found Emerald", payload["reason"])
        self.assertIn("community_presence", payload["sourceLanes"])
        self.assertNotRegex(json.dumps(payload), r"\b\d{15,22}\b")

    def test_operator_mentioned_subject_and_direct_interaction_score(self):
        rows = [
            {"subjectName": "Hellcat", "mentionCount": 1, "directInteractionCount": 0, "operatorMentionCount": 1, "daysActive": 1, "category": "community_regular_candidate", "signalCount": 2},
            {"subjectName": "Crow", "mentionCount": 1, "directInteractionCount": 1, "operatorMentionCount": 0, "daysActive": 1, "category": "community_regular_candidate", "signalCount": 2},
        ]
        result = discovery.discover_candidate_recommendations(123, ["community_presence"], community_presence_reader=lambda *a, **k: rows)
        names = {payload["subjectName"] for payload in result["payloads"]}
        self.assertIn("Hellcat", names)
        self.assertIn("Crow", names)

    def test_orion_via_crow_is_possible_connection_not_confirmed_identity(self):
        result = discovery.discover_candidate_recommendations(
            123,
            ["community_presence"],
            community_presence_reader=lambda *a, **k: self.community_rows(),
        )
        payload = next(payload for payload in result["payloads"] if payload["subjectName"] == "Orion")
        self.assertEqual(payload["type"], "new_subject")
        self.assertEqual(payload.get("recommendedCategory"), "Personnel")
        self.assertIn("mentioned through Crow", payload["reason"])
        self.assertIn("not confirmed identity", payload["reason"])
        self.assertNotIn("relationship", payload.get("recommendedKind", ""))

    def test_community_weak_terms_and_one_off_title_words_withheld(self):
        rows = [
            {"subjectName": "everyone", "mentionCount": 5, "directInteractionCount": 0, "operatorMentionCount": 0, "daysActive": 4, "category": "community_regular_candidate", "signalCount": 5},
            {"subjectName": "today", "mentionCount": 3, "directInteractionCount": 0, "operatorMentionCount": 0, "daysActive": 2, "category": "community_regular_candidate", "signalCount": 4},
            {"subjectName": "Signal", "mentionCount": 1, "directInteractionCount": 0, "operatorMentionCount": 0, "daysActive": 1, "category": "community_regular_candidate", "signalCount": 1},
        ]
        result = discovery.discover_candidate_recommendations(123, ["community_presence"], community_presence_reader=lambda *a, **k: rows)
        self.assertEqual(result["payloads"], [])

    def test_community_payload_does_not_store_raw_transcript_or_invalid_taxonomy(self):
        rows = [{"subjectName": "Emerald", "mentionCount": 3, "directInteractionCount": 1, "operatorMentionCount": 0, "daysActive": 2, "category": "community_regular_candidate", "signalCount": 5, "evidenceSnippets": ["raw message body should not appear 123456789012345678"]}]
        result = discovery.discover_candidate_recommendations(123, ["community_presence"], community_presence_reader=lambda *a, **k: rows)
        payload = result["payloads"][0]
        self.assertIn(payload.get("recommendedCategory"), {"Entity", "Personnel", "Sponsor", "Interface", "Production"})
        self.assertNotIn("raw message body", json.dumps(payload))
        self.assertNotRegex(json.dumps(payload), r"\b\d{15,22}\b")

    def test_alias_language_maps_to_identity_link_only_when_explicit(self):
        alias_rows = [{"subjectName": "Emerald", "mentionCount": 2, "directInteractionCount": 0, "operatorMentionCount": 1, "daysActive": 1, "category": "possible_alias_review", "signalCount": 3}]
        regular_rows = [{"subjectName": "Hellcat", "mentionCount": 2, "directInteractionCount": 1, "operatorMentionCount": 0, "daysActive": 1, "category": "community_regular_candidate", "signalCount": 3}]
        alias_result = discovery.discover_candidate_recommendations(123, ["community_presence"], community_presence_reader=lambda *a, **k: alias_rows)
        regular_result = discovery.discover_candidate_recommendations(123, ["community_presence"], community_presence_reader=lambda *a, **k: regular_rows)
        self.assertEqual(alias_result["payloads"][0]["type"], "identity_link")
        self.assertEqual(regular_result["payloads"][0]["type"], "new_subject")


class CommunityPresenceStoreTests(unittest.TestCase):
    def setUp(self):
        self.db_path = "test_community_presence.db"
        try:
            os.remove(self.db_path)
        except FileNotFoundError:
            pass

    def tearDown(self):
        try:
            os.remove(self.db_path)
        except FileNotFoundError:
            pass

    def test_live_store_minimal_fields_no_raw_ids(self):
        import bnl_community_scouting as scouting
        result = scouting.record_community_presence_event(
            self.db_path,
            123,
            "Emerald",
            "Orion via Crow should be reviewed <@123456789012345678> with https://secret.example/path",
            channel_id=456,
            channel_policy="public_context",
            direct_interaction=True,
            operator_mention=True,
            environ={"BNL_COMMUNITY_SCOUTING_ENABLED": "true"},
        )
        self.assertTrue(result["ok"])
        rows = scouting.get_community_presence_candidates(self.db_path, 123, min_signals=1)
        names = {row["subjectName"] for row in rows}
        self.assertIn("Emerald", names)
        self.assertIn("Orion", names)
        blob = json.dumps(rows)
        self.assertNotIn("123456789012345678", blob)
        self.assertNotIn("https://secret.example", blob)
        self.assertIn("mentioned through Crow", blob)

    def test_disabled_store_is_safely_unavailable(self):
        import bnl_community_scouting as scouting
        result = scouting.record_community_presence_event(
            self.db_path,
            123,
            "Emerald",
            "Emerald is around",
            channel_id=456,
            channel_policy="public_context",
            environ={"BNL_COMMUNITY_SCOUTING_ENABLED": "false"},
        )
        self.assertFalse(result["ok"])
        self.assertEqual(result["status"], "disabled")


if __name__ == "__main__":
    unittest.main()
