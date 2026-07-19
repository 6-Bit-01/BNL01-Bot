import json
import sqlite3
import tempfile
import unittest
from unittest import mock

import bnl_journal as journal


def _long_body(opening: str) -> str:
    texture = (
        "BNL follows the room as producers trade careful sounds and listeners turn small sparks into a shared musical thread. "
    )
    return opening + " " + texture * 16


def _medium_body(opening: str) -> str:
    texture = (
        "BNL follows the room as producers trade careful sounds and listeners turn small sparks into a shared musical thread. "
    )
    return opening + " " + texture * 7


class Response:
    status = 200

    def __init__(self, request, captured):
        captured.append(request.data)
        entry = json.loads(request.data.decode("utf-8"))["entry"]
        self.body = json.dumps({
            "ok": True,
            "persisted": True,
            "entry": {
                "entryId": entry["entryId"],
                "revision": entry["revision"],
                "contentHash": entry["contentHash"],
                "publishedAt": "2026-07-20T12:00:00Z",
            },
        }).encode("utf-8")

    def read(self):
        return self.body

    def getcode(self):
        return self.status

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


class JournalContextLaneTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.NamedTemporaryFile(delete=False)
        self.db = tmp.name
        tmp.close()
        journal.ensure_schema(self.db)
        with sqlite3.connect(self.db) as conn:
            conn.execute(
                "CREATE TABLE conversations("
                "id INTEGER PRIMARY KEY,user_id INTEGER,user_name TEXT,guild_id INTEGER,channel_name TEXT,"
                "channel_policy TEXT,role TEXT,content TEXT,timestamp TEXT,public_usable INTEGER,visibility TEXT)"
            )
            conn.executemany(
                "INSERT INTO conversations VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                [
                    (1, 10, "Alice", 1, "general", "public_home", "user", "public", "2026-07-19T10:00:00Z", 1, "public_safe"),
                    (2, 20, "Bob", 1, "general", "public_home", "user", "public", "2026-07-19T10:01:00Z", 1, "public_safe"),
                    (3, 50, "Eve", 1, "general", "public_home", "user", "public", "2026-06-01T10:01:00Z", 1, "public_safe"),
                    (4, 60, "DJ Waffles 🍕", 1, "general", "public_home", "user", "public", "2026-06-01T10:02:00Z", 1, "public_safe"),
                    (5, 70, "Signal.Ghost!", 1, "general", "public_home", "user", "public", "2026-06-01T10:03:00Z", 1, "public_safe"),
                ],
            )
            conn.execute(
                """CREATE TABLE broadcast_memory(
                    id INTEGER PRIMARY KEY,guild_id INTEGER,episode_date TEXT,cleaned_summary TEXT,
                    entry_type TEXT,importance TEXT,public_safe INTEGER,usage_scope TEXT,valid_until TEXT,
                    needs_clarification INTEGER,status TEXT,superseded_by_id INTEGER,created_at TEXT,raw_note TEXT
                )"""
            )
            rows = [
                (1, 1, "2026-07-17", "DJ Waffles 🍕 recorded a silver synth chorus during the Friday broadcast.", "notable_moment", "medium", 1, "ambient,direct", None, 0, "active", None, "2026-07-17T12:00:00Z", "RAW MOD NOTE MUST NEVER APPEAR"),
                (2, 1, "2026-07-17", "A silver synth chorus remains secret.", "notable_moment", "medium", 0, "internal", None, 0, "active", None, "2026-07-17T12:00:00Z", "private"),
                (3, 1, "2026-07-17", "A silver synth chorus needs clarification.", "notable_moment", "medium", 1, "ambient", None, 1, "active", None, "2026-07-17T12:00:00Z", "unclear"),
                (4, 1, "2026-07-17", "A silver synth chorus was superseded.", "notable_moment", "medium", 1, "ambient", None, 0, "active", 99, "2026-07-17T12:00:00Z", "old"),
                (5, 1, "2026-07-17", "A silver synth chorus expired.", "notable_moment", "medium", 1, "ambient", "2026-07-18", 0, "active", None, "2026-07-17T12:00:00Z", "expired"),
            ]
            conn.executemany("INSERT INTO broadcast_memory VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)

        self.relays = [{
            "refId": "fresh:1",
            "sourceKind": "relay",
            "relayId": "relay-1",
            "summary": "The Friday broadcast carried a silver synth chorus into the public music room.",
            "observedAt": "2026-07-19T09:00:00Z",
        }]
        self.conversations = [
            {
                "refId": "fresh:101", "sourceKind": "conversation", "messageId": 101,
                "subjectRef": "discord_user:10", "displayName": "Alice", "channelPolicy": "public_home",
                "summary": "Alice heard a rumor that DJ Waffles 🍕 may reveal a hidden synth set during the Friday show.",
                "observedAt": "2026-07-19T10:00:00Z",
            },
            {
                "refId": "fresh:102", "sourceKind": "conversation", "messageId": 102,
                "subjectRef": "discord_user:20", "displayName": "Bob", "channelPolicy": "public_context",
                "summary": "Bob says the rumor around Signal.Ghost! and the Friday show is a hidden synth set reveal.",
                "observedAt": "2026-07-19T10:01:00Z",
            },
            {
                "refId": "fresh:103", "sourceKind": "conversation", "messageId": 103,
                "subjectRef": "discord_user:30", "displayName": "Carol", "channelPolicy": "public_home",
                "summary": "Carol heard a private medical rumor about an artist and the Friday show.",
                "observedAt": "2026-07-19T10:02:00Z",
            },
            {
                "refId": "fresh:104", "sourceKind": "conversation", "messageId": 104,
                "subjectRef": "discord_user:40", "displayName": "Dave", "channelPolicy": "public_home",
                "summary": "Dave heard a private medical rumor about an artist and the Friday show.",
                "observedAt": "2026-07-19T10:03:00Z",
            },
        ]

    def packet(self, entry_kind="daily"):
        return journal.build_packet_from_sources(
            self.db,
            1,
            "2026-07-19T07:00:00Z",
            "2026-07-20T07:00:00Z",
            self.relays,
            self.conversations,
            entry_kind=entry_kind,
        )

    def article(self, packet, opening, *, lane_type="", lane_ref="", basis=None, sections=1):
        fresh_ref = (basis or [packet["safeSources"][0]["refId"]])[-1]
        heading = "What BNL Made of It"
        context_uses = []
        if lane_type:
            context_uses.append({
                "laneType": lane_type,
                "laneRefId": lane_ref,
                "sectionHeading": heading,
                "claim": opening,
                "basisRefIds": list(basis or []),
            })
        return {
            "title": "Signals, Stories, and Side-Eyes",
            "excerpt": "The room carried music, a few side currents, and enough pattern for BNL to notice.",
            "sections": [
                {"heading": heading if index == 0 else f"Another Thread {index}", "body": _long_body(opening),}
                for index in range(sections)
            ],
            "sourceRefIds": {
                (heading if index == 0 else f"Another Thread {index}"): [fresh_ref]
                for index in range(sections)
            },
            "metadata": {
                "topicTags": ["music"], "subjectRefs": [], "continuityNotes": [],
                "unresolvedQuestions": [], "confidenceFlags": [], "safetyFlags": [],
                "contextUses": context_uses,
            },
        }

    def test_context_lanes_are_separate_conservative_and_public_safe(self):
        packet = self.packet()
        lanes = packet["generationContextLanes"]
        self.assertEqual(1, len(lanes["establishedBroadcastMemory"]))
        memory = lanes["establishedBroadcastMemory"][0]
        self.assertEqual("established_network_record", memory["epistemicStatus"])
        self.assertNotIn("Alice", memory["summary"])
        self.assertNotIn("DJ Waffles 🍕", memory["summary"])
        self.assertNotIn("RAW MOD NOTE", json.dumps(lanes))

        self.assertEqual(1, len(lanes["communityRumors"]))
        rumor = lanes["communityRumors"][0]
        self.assertEqual("unconfirmed_public_rumor", rumor["epistemicStatus"])
        self.assertEqual(2, rumor["independentParticipantCount"])
        self.assertNotIn("Alice", json.dumps(rumor))
        self.assertNotIn("Bob", json.dumps(rumor))
        self.assertNotIn("Eve", json.dumps(rumor))
        self.assertNotIn("DJ Waffles 🍕", json.dumps(rumor))
        self.assertNotIn("Signal.Ghost!", json.dumps(rumor))
        self.assertNotIn("medical", json.dumps(rumor).lower())
        self.assertNotIn("medical", json.dumps(packet["safeSources"]).lower())

        self.assertEqual("bnl_inference_only", lanes["bnlInference"]["epistemicStatus"])
        dependency_map = lanes["bnlInference"]["requiredContextLaneRefsByFreshSourceRef"]
        rumor_fresh_ref = lanes["communityRumors"][0]["evidence"][0]["sourceRefId"]
        self.assertIn(lanes["communityRumors"][0]["laneRefId"], dependency_map[rumor_fresh_ref])
        self.assertIn(lanes["establishedBroadcastMemory"][0]["laneRefId"], dependency_map[rumor_fresh_ref])
        self.assertEqual(
            {
                lanes["establishedBroadcastMemory"][0]["laneRefId"],
                lanes["communityRumors"][0]["laneRefId"],
            },
            set(lanes["bnlInference"]["requiredParentContextLaneRefs"]),
        )
        private = packet["privateContextLaneProvenance"]
        self.assertEqual(1, private["establishedBroadcastMemory"][0]["rowId"])
        self.assertEqual(
            {"discord_user:10", "discord_user:20"},
            {item["subjectRef"] for item in private["communityRumors"][0]["sourceRefs"]},
        )

    def test_punctuation_and_emoji_identity_literals_are_scrubbed_and_detected(self):
        redacted = journal.sanitize_source_summary(
            "DJ Waffles 🍕 and Signal.Ghost! traded a synth idea.",
            ["DJ Waffles 🍕", "Signal.Ghost!"],
        )
        self.assertNotIn("DJ Waffles 🍕", redacted)
        self.assertNotIn("Signal.Ghost!", redacted)
        self.assertEqual(2, redacted.count("someone"))

        packet = self.packet()
        for display_name in ("DJ Waffles 🍕", "Signal.Ghost!"):
            named_packet = dict(packet)
            named_packet["privateSources"] = [
                *packet["privateSources"],
                {
                    "refId": "private-name-check",
                    "sourceKind": "conversation",
                    "displayName": display_name,
                    "summary": "A listener kept the musical room moving.",
                },
            ]
            article = self.article(
                packet,
                f"{display_name} kept the room moving through a long musical afternoon.",
            )
            self.assertEqual("community_name_leak", journal.validate_article(article, named_packet, []))

    def test_prompt_keeps_epistemic_lanes_separate_and_optional(self):
        prompt = journal.build_generation_prompt(self.packet())
        self.assertIn("moderator-approved", prompt)
        self.assertIn("It is unconfirmed", prompt)
        self.assertIn("BNL suspects, thinks, or wonders", prompt)
        self.assertIn("dedicated third section is welcome", prompt)
        self.assertIn("never pad beyond three sections", prompt)
        self.assertIn("exact complete public sentence", prompt)
        self.assertIn("every fresh basisRefId", prompt)
        self.assertIn("same section's sourceRefIds", prompt)
        self.assertIn("secondary lane its own contextUse for the same section", prompt)
        self.assertIn("requiredContextLaneRefsByFreshSourceRef map may impose", prompt)
        self.assertIn("Every requiredParentContextLaneRefs item is mandatory", prompt)
        self.assertNotIn("RAW MOD NOTE", prompt)
        self.assertNotIn("discord_user:", prompt)
        self.assertNotIn("medical", prompt.lower())

    def test_thin_unrelated_window_gets_no_padded_context_lane(self):
        packet = journal.build_packet_from_sources(
            self.db,
            1,
            "2026-07-19T07:00:00Z",
            "2026-07-20T07:00:00Z",
            [{
                "refId": "fresh:900", "sourceKind": "relay", "relayId": "quiet",
                "summary": "A drummer shared one new beat.", "observedAt": "2026-07-19T11:00:00Z",
            }],
            [],
            entry_kind="daily",
        )
        self.assertEqual({}, packet["generationContextLanes"])
        prompt = journal.build_generation_prompt(packet)
        self.assertIn("No optional context lane qualified", prompt)
        self.assertIn("contextUses as an empty list", prompt)

    def test_rumor_cannot_be_promoted_to_fact_or_used_without_provenance(self):
        packet = self.packet()
        rumor = packet["generationContextLanes"]["communityRumors"][0]
        fresh_ref = rumor["evidence"][0]["sourceRefId"]
        basis = [rumor["laneRefId"], fresh_ref]
        unframed = self.article(
            packet,
            "The hidden synth set will appear during the Friday show.",
            lane_type="community_rumor",
            lane_ref=rumor["laneRefId"],
            basis=basis,
        )
        self.assertEqual("rumor_not_explicitly_framed", journal.validate_article(unframed, packet, []))

        undeclared_fact = self.article(
            packet,
            "The hidden synth set will appear during the Friday show.",
        )
        self.assertEqual("undeclared_context_use", journal.validate_article(undeclared_fact, packet, []))

        hedged_only = self.article(
            packet,
            "A hidden synth set might surface during the Friday show.",
            lane_type="community_rumor",
            lane_ref=rumor["laneRefId"],
            basis=basis,
        )
        self.assertEqual("rumor_not_explicitly_framed", journal.validate_article(hedged_only, packet, []))

        framed = self.article(
            packet,
            "A community rumor suggested that a hidden synth set may surface during the Friday show.",
            lane_type="community_rumor",
            lane_ref=rumor["laneRefId"],
            basis=basis,
        )
        self.assertEqual("", journal.validate_article(framed, packet, []))

        undeclared = self.article(packet, "A rumor surfaced about an unrevealed synth presentation on Friday.")
        self.assertEqual("undeclared_context_use", journal.validate_article(undeclared, packet, []))

        laundering = self.article(
            packet,
            "The hidden synth set will appear during the Friday show. A rumor about an unrelated album circulated.",
            lane_type="community_rumor",
            lane_ref=rumor["laneRefId"],
            basis=basis,
        )
        laundering["metadata"]["contextUses"][0]["claim"] = (
            "The hidden synth set will appear during the Friday show."
        )
        self.assertEqual("rumor_not_explicitly_framed", journal.validate_article(laundering, packet, []))

        unrelated_ref = next(
            source["refId"]
            for source in packet["safeSources"]
            if source["refId"] not in basis
        )
        wrong_section_basis = self.article(
            packet,
            "A community rumor suggested that a hidden synth set may surface during the Friday show.",
            lane_type="community_rumor",
            lane_ref=rumor["laneRefId"],
            basis=basis,
        )
        wrong_section_basis["sourceRefIds"]["What BNL Made of It"] = [unrelated_ref]
        self.assertEqual("invalid_context_use", journal.validate_article(wrong_section_basis, packet, []))

        second_fresh_ref = rumor["evidence"][1]["sourceRefId"]
        partial_section_basis = self.article(
            packet,
            "A community rumor suggested that a hidden synth set may surface during the Friday show.",
            lane_type="community_rumor",
            lane_ref=rumor["laneRefId"],
            basis=[rumor["laneRefId"], fresh_ref, second_fresh_ref],
        )
        partial_section_basis["sourceRefIds"]["What BNL Made of It"] = [fresh_ref]
        self.assertEqual("invalid_context_use", journal.validate_article(partial_section_basis, packet, []))

        scattered_claim = self.article(
            packet,
            "A rumor circulated about something unclear. The hidden idea drew attention. The synth discussion continued. Friday arrived and the show carried on.",
            lane_type="community_rumor",
            lane_ref=rumor["laneRefId"],
            basis=basis,
        )
        scattered_claim["metadata"]["contextUses"][0]["claim"] = (
            "A community rumor suggested that a hidden synth set may surface during the Friday show."
        )
        self.assertEqual("invalid_context_use", journal.validate_article(scattered_claim, packet, []))

    def test_established_memory_cannot_be_used_without_declaration(self):
        packet = self.packet()
        undeclared = self.article(
            packet,
            "A silver synth chorus was recorded during the Friday broadcast.",
        )
        self.assertEqual("undeclared_context_use", journal.validate_article(undeclared, packet, []))

    def test_context_authority_is_bound_to_the_named_section(self):
        packet = self.packet()
        rumor = packet["generationContextLanes"]["communityRumors"][0]
        inference = packet["generationContextLanes"]["bnlInference"]
        fresh_ref = rumor["evidence"][0]["sourceRefId"]

        article = self.article(
            packet,
            "BNL suspects the Friday synth show points toward a larger plan.",
            lane_type="bnl_inference",
            lane_ref=inference["laneRefId"],
            basis=[inference["laneRefId"], rumor["laneRefId"], fresh_ref],
            sections=2,
        )
        second_heading = "Another Thread 1"
        rumor_claim = "A community rumor suggested that a hidden synth set may surface during the Friday show."
        article["sections"][0]["body"] = _medium_body(
            "BNL suspects the Friday synth show points toward a larger plan."
        )
        article["sections"][1]["body"] = _medium_body(rumor_claim)
        article["metadata"]["contextUses"].append({
            "laneType": "community_rumor",
            "laneRefId": rumor["laneRefId"],
            "sectionHeading": second_heading,
            "claim": rumor_claim,
            "basisRefIds": [rumor["laneRefId"], fresh_ref],
        })
        self.assertEqual("invalid_context_use", journal.validate_article(article, packet, []))

        marker_in_wrong_section = self.article(
            packet,
            "A community rumor suggested that a hidden synth set may surface during the Friday show.",
            sections=2,
        )
        marker_in_wrong_section["sections"][0]["body"] = _medium_body(rumor_claim)
        marker_in_wrong_section["sections"][1]["body"] = _medium_body(rumor_claim)
        marker_in_wrong_section["metadata"]["contextUses"] = [{
            "laneType": "community_rumor",
            "laneRefId": rumor["laneRefId"],
            "sectionHeading": second_heading,
            "claim": rumor_claim,
            "basisRefIds": [rumor["laneRefId"], fresh_ref],
        }]
        marker_in_wrong_section["sourceRefIds"][second_heading] = [fresh_ref]
        self.assertEqual("undeclared_context_use", journal.validate_article(marker_in_wrong_section, packet, []))

        title_claim = self.article(packet, "The room kept making music.")
        title_claim["title"] = "The Hidden Synth Set Arrives Friday"
        self.assertEqual("undeclared_context_use", journal.validate_article(title_claim, packet, []))

    def test_strong_unframed_inference_cue_requires_declaration(self):
        packet = self.packet()
        inference_only = dict(packet)
        inference_only["generationContextLanes"] = {
            "bnlInference": packet["generationContextLanes"]["bnlInference"],
        }
        inference_only["privateContextLaneProvenance"] = {}
        article = self.article(
            inference_only,
            "The Friday synth chatter proves a larger plan for the show.",
        )
        self.assertEqual("undeclared_context_use", journal.validate_article(article, inference_only, []))

    def test_bnl_inference_is_explicit_and_sections_remain_bounded(self):
        packet = self.packet()
        inference = packet["generationContextLanes"]["bnlInference"]
        memory = packet["generationContextLanes"]["establishedBroadcastMemory"][0]
        rumor = packet["generationContextLanes"]["communityRumors"][0]
        fresh_ref = rumor["evidence"][0]["sourceRefId"]
        framing_inference = {
            **inference,
            "allowedBasisRefIds": [fresh_ref],
            "requiredParentContextLaneRefs": [],
            "requiredContextLaneRefsByFreshSourceRef": {},
        }
        framing_packet = dict(packet)
        framing_packet["generationContextLanes"] = {"bnlInference": framing_inference}
        framing_packet["privateContextLaneProvenance"] = {}
        basis = [inference["laneRefId"], fresh_ref]
        unframed = self.article(
            framing_packet,
            "The repeated synth chatter proves a larger plan.",
            lane_type="bnl_inference",
            lane_ref=inference["laneRefId"],
            basis=basis,
        )
        self.assertEqual("inference_not_explicitly_framed", journal.validate_article(unframed, framing_packet, []))
        hedged_only = self.article(
            framing_packet,
            "The repeated synth chatter might be part of a larger plan.",
            lane_type="bnl_inference",
            lane_ref=inference["laneRefId"],
            basis=basis,
        )
        self.assertEqual("inference_not_explicitly_framed", journal.validate_article(hedged_only, framing_packet, []))
        framed = self.article(
            framing_packet,
            "BNL suspects the repeated synth chatter may be part of a larger plan.",
            lane_type="bnl_inference",
            lane_ref=inference["laneRefId"],
            basis=basis,
        )
        self.assertEqual("", journal.validate_article(framed, framing_packet, []))

        laundering = self.article(
            packet,
            "BNL suspects the hidden synth talk may point toward a surprise performance during the Friday show.",
            lane_type="bnl_inference",
            lane_ref=inference["laneRefId"],
            basis=[inference["laneRefId"], fresh_ref],
        )
        self.assertEqual("invalid_context_use", journal.validate_article(laundering, packet, []))

        memory_fresh_ref = "fresh:1"
        synonymized_claim = "BNL suspects the Friday synth activity may point toward a surprise performance."
        memory_claim = "An established Network record says a silver synth chorus was recorded during the Friday broadcast."
        parent_laundering = self.article(
            packet,
            synonymized_claim,
            lane_type="bnl_inference",
            lane_ref=inference["laneRefId"],
            basis=[inference["laneRefId"], memory["laneRefId"], memory_fresh_ref],
        )
        parent_laundering["sections"][0]["body"] = _long_body(" ".join([memory_claim, synonymized_claim]))
        parent_laundering["metadata"]["contextUses"].append({
            "laneType": "established_broadcast_memory",
            "laneRefId": memory["laneRefId"],
            "sectionHeading": "What BNL Made of It",
            "claim": memory_claim,
            "basisRefIds": [memory["laneRefId"], memory_fresh_ref],
        })
        self.assertEqual("invalid_context_use", journal.validate_article(parent_laundering, packet, []))

        valid_claim = "BNL suspects the hidden synth talk may point toward a surprise performance during the Friday show."
        rumor_claim = "A community rumor suggested that a hidden synth set may surface during the Friday show."
        grounded = self.article(
            packet,
            valid_claim,
            lane_type="bnl_inference",
            lane_ref=inference["laneRefId"],
            basis=[inference["laneRefId"], memory["laneRefId"], rumor["laneRefId"], fresh_ref],
        )
        grounded["sections"][0]["body"] = _long_body(" ".join([memory_claim, rumor_claim, valid_claim]))
        grounded["metadata"]["contextUses"].extend([
            {
                "laneType": "established_broadcast_memory",
                "laneRefId": memory["laneRefId"],
                "sectionHeading": "What BNL Made of It",
                "claim": memory_claim,
                "basisRefIds": [memory["laneRefId"], fresh_ref],
            },
            {
                "laneType": "community_rumor",
                "laneRefId": rumor["laneRefId"],
                "sectionHeading": "What BNL Made of It",
                "claim": rumor_claim,
                "basisRefIds": [rumor["laneRefId"], fresh_ref],
            },
        ])
        self.assertEqual("", journal.validate_article(grounded, packet, []))
        too_many = self.article(packet, "The room kept making music.", sections=4)
        self.assertEqual("invalid_section_count", journal.validate_article(too_many, packet, []))

    def test_only_used_lane_provenance_is_persisted(self):
        packet = self.packet()
        rumor = packet["generationContextLanes"]["communityRumors"][0]
        fresh_ref = rumor["evidence"][0]["sourceRefId"]
        article = self.article(
            packet,
            "A community rumor suggested that a hidden synth set may surface during the Friday show.",
            lane_type="community_rumor",
            lane_ref=rumor["laneRefId"],
            basis=[rumor["laneRefId"], fresh_ref],
        )
        result = journal.store_validated_draft(self.db, 1, packet, article)
        self.assertTrue(result.ok, result.reason)
        with sqlite3.connect(self.db) as conn:
            metadata = json.loads(conn.execute(
                "SELECT metadata_json FROM bnl_journal_private_metadata WHERE entry_id=?",
                (result.entry_id,),
            ).fetchone()[0])
        self.assertIn("communityRumors", metadata["usedGenerationContextLanes"])
        self.assertNotIn("establishedBroadcastMemory", metadata["usedGenerationContextLanes"])
        self.assertIn("communityRumors", metadata["usedContextLaneProvenance"])
        self.assertEqual("community_rumor", metadata["contextUses"][0]["laneType"])

    def test_new_payload_has_entry_kind_and_old_rehydrate_stays_byte_stable(self):
        for kind in ("daily", "weekly", "manual"):
            packet = self.packet(kind)
            article = self.article(packet, "The room kept making music.")
            payload = journal.build_public_payload("entry", 1, article, packet, "hash", "2026-07-20T07:00:00Z")
            self.assertEqual(kind, payload["entry"]["entryKind"])

        old_payload = {
            "contractVersion": 1,
            "kind": "journal_entry",
            "entry": {
                "entryId": "old-entry", "revision": 1, "title": "Old Entry", "excerpt": "Old excerpt",
                "sections": [{"heading": "Old", "body": "Old body"}],
                "authoredAt": "2026-07-01T00:00:00Z", "sourceWindowStart": "2026-06-30T00:00:00Z",
                "sourceWindowEnd": "2026-07-01T00:00:00Z", "contentHash": "old-hash",
            },
        }
        canonical = journal._json(old_payload).encode("utf-8")
        with sqlite3.connect(self.db) as conn:
            conn.execute(
                "INSERT INTO bnl_journal_entries VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                ("old-entry", 1, 1, "published", "Old Entry", "Old excerpt", "[]", canonical.decode("utf-8"), canonical,
                 "old-hash", "2026-06-30T00:00:00Z", "2026-07-01T00:00:00Z", "2026-07-01T00:00:00Z",
                 "2026-07-01T00:00:00Z", "2026-07-01T00:00:00Z", None, "", 200,
                 "2026-07-01T00:00:00Z", "2026-07-01T00:00:00Z"),
            )
            conn.execute(
                "INSERT INTO bnl_journal_private_metadata VALUES(?,?,?,?,?,?,?,?)",
                ("old-entry", 1, 1, "{}", "old-hash", "published", "2026-07-01T00:00:00Z", "2026-07-01T00:00:00Z"),
            )
        captured = []

        def opener(request, timeout=10):
            return Response(request, captured)

        result = journal.rehydrate_published_entries(self.db, 1, "https://site.example", "key", opener=opener)
        self.assertTrue(result["ok"], result)
        self.assertEqual([canonical], captured)
        self.assertNotIn("entryKind", captured[0].decode("utf-8"))

    def test_regeneration_preserves_authoritative_entry_kind(self):
        for kind in ("daily", "weekly", "manual"):
            packet = self.packet(kind)
            initial_article = self.article(packet, "The room kept making music and listeners kept the thread moving.")
            entry_id = f"regen-{kind}"
            stored = journal.store_validated_draft(
                self.db,
                1,
                packet,
                initial_article,
                entry_id=entry_id,
            )
            self.assertTrue(stored.ok, stored.reason)

            if kind == "manual":
                with sqlite3.connect(self.db) as conn:
                    payload = json.loads(conn.execute(
                        "SELECT public_payload_json FROM bnl_journal_entries WHERE entry_id=? AND revision=1",
                        (entry_id,),
                    ).fetchone()[0])
                    payload["entry"]["entryKind"] = "daily"
                    conn.execute(
                        "UPDATE bnl_journal_entries SET public_payload_json=? WHERE entry_id=? AND revision=1",
                        (json.dumps(payload), entry_id),
                    )

            replacement = self.article(packet, "The room shaped another musical thread while listeners followed along.")
            generated = {
                "title": replacement["title"],
                "excerpt": replacement["excerpt"],
                "sections": [
                    {
                        **section,
                        "sourceRefIds": replacement["sourceRefIds"][section["heading"]],
                    }
                    for section in replacement["sections"]
                ],
                "metadata": replacement["metadata"],
            }
            with mock.patch.object(journal, "build_source_packet", return_value=packet) as build_packet:
                result = journal.regenerate_draft(
                    self.db,
                    1,
                    entry_id,
                    72,
                    lambda *_args: json.dumps(generated),
                )
            self.assertTrue(result.ok, result.reason)
            build_packet.assert_called_once_with(self.db, 1, 72, entry_kind=kind)
            with sqlite3.connect(self.db) as conn:
                payload_json, metadata_json = conn.execute(
                    """SELECT e.public_payload_json,m.metadata_json
                       FROM bnl_journal_entries e
                       JOIN bnl_journal_private_metadata m
                         ON m.entry_id=e.entry_id AND m.revision=e.revision
                       WHERE e.entry_id=? AND e.revision=2""",
                    (entry_id,),
                ).fetchone()
            self.assertEqual(kind, json.loads(payload_json)["entry"]["entryKind"])
            self.assertEqual(kind, json.loads(metadata_json)["entryKind"])


if __name__ == "__main__":
    unittest.main()
