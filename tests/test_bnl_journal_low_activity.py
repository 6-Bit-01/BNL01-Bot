import hashlib
import json
import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

import bnl_journal as journal
import bnl_journal_automation as automation
import bnl_journal_source_store as source_store
from bnl_canon_source_contract import CanonFact, SIX_BIT


START = "2026-07-19T00:00:00Z"
END = "2026-07-20T00:00:00Z"


def reflection_article(packet, *, body=None, refs=None):
    selected_refs = list(
        refs
        or [
            item["refId"]
            for item in packet.get("reflectionBasis", [])
            if item.get("refId")
        ][:2]
    )
    return {
        "title": "The Roles Behind the Signal",
        "excerpt": "A verified BARCODE record gives BNL a grounded thread to revisit.",
        "sections": [
            {
                "heading": "A Clearer Archive",
                "body": body
                or (
                    "The approved BARCODE record identifies GALAKNOISE as the music "
                    "producer while 6 Bit remains the artist, MC, and host. I admit "
                    "that clear division of roles gives the archive a sturdier shape."
                ),
            }
        ],
        "sourceRefIds": {"A Clearer Archive": selected_refs},
        "metadata": {
            "topicTags": ["barcode"],
            "subjectRefs": [],
            "continuityNotes": [],
            "unresolvedQuestions": [],
            "confidenceFlags": ["approved_canon"],
            "safetyFlags": ["public_safe"],
            "contextUses": [],
        },
    }


def generated_reflection_json(packet, *, body=None):
    article = reflection_article(packet, body=body)
    return json.dumps(
        {
            "title": article["title"],
            "excerpt": article["excerpt"],
            "sections": [
                {
                    **section,
                    "sourceRefIds": article["sourceRefIds"][section["heading"]],
                }
                for section in article["sections"]
            ],
            "metadata": article["metadata"],
        }
    )


class JournalLowActivityTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.NamedTemporaryFile(delete=False)
        self.db = tmp.name
        tmp.close()
        journal.ensure_schema(self.db)
        source_store.ensure_schema(self.db)
        automation.ensure_schema(self.db)

    def packet(self):
        packet = journal.build_packet_from_sources(
            self.db,
            1,
            START,
            END,
            [],
            [],
            entry_kind="daily",
            aggregate_counts={
                "eligibleRelays": 0,
                "eligibleConversations": 0,
            },
        )
        packet["sourceArchiveAvailable"] = True
        return packet

    def record(
        self,
        *,
        source_kind,
        source_key,
        when,
        raw_text,
        channel_policy,
        subject_ref="",
        private_display_name="",
        public_usable=True,
        metadata=None,
    ):
        result = source_store.record_source_event(
            self.db,
            guild_id=1,
            source_kind=source_kind,
            source_key=source_key,
            occurred_at_ms=int(
                datetime.fromisoformat(when.replace("Z", "+00:00")).timestamp()
                * 1000
            ),
            raw_text=raw_text,
            sanitized_summary=raw_text,
            channel_policy=channel_policy,
            subject_ref=subject_ref,
            private_display_name=private_display_name,
            public_usable=public_usable,
            metadata=metadata or {},
        )
        self.assertTrue(result.ok, result)
        return result.event_seq

    def test_empty_daily_gets_stable_typed_corrected_canon_basis(self):
        first = self.packet()
        second = self.packet()

        self.assertTrue(first["lowActivityMode"])
        self.assertEqual(
            [item["refId"] for item in first["reflectionBasis"]],
            [item["refId"] for item in second["reflectionBasis"]],
        )
        self.assertTrue(first["reflectionBasis"])
        for item in first["reflectionBasis"]:
            self.assertTrue(item["refId"].startswith("reflection:"))
            self.assertEqual("historical_or_canon", item["scope"])
            self.assertTrue(item["publicSafe"])
            self.assertTrue(item["reuseEligible"])
            self.assertTrue(
                item.get("sourceVersion") or item.get("sourceObservedAt")
            )

        canon = {
            (item.get("canonSubject"), item.get("canonPredicate")): item
            for item in first["reflectionBasis"]
            if item.get("basisKind") == "approved_canon"
        }
        self.assertIn(("galaknoise", "primary_role"), canon)
        self.assertIn("music producer", canon[("galaknoise", "primary_role")]["summary"])
        self.assertIn(("6_bit", "primary_identity"), canon)
        self.assertNotIn("producer", canon[("6_bit", "primary_identity")]["summary"].lower())
        self.assertFalse(
            journal._canon_fact_is_reflection_safe(
                CanonFact(SIX_BIT, "primary_role", "music producer for BARCODE")
            )
        )

        prompt = journal.build_generation_prompt(first)
        self.assertIn("LOW-ACTIVITY EVIDENCE RULE", prompt)
        self.assertIn("same Journal voice", prompt)
        self.assertNotIn("a concrete current-window moment", prompt)
        self.assertEqual("", journal.validate_article(reflection_article(first), first, []))

    def test_active_window_prompt_remains_byte_compatible(self):
        relays = [
            {
                "refId": f"fresh:r{index}",
                "sourceKind": "relay",
                "summary": f"Public relay music detail {index}.",
                "observedAt": f"2026-07-19T0{index}:00:00Z",
                "eventType": "accepted",
            }
            for index in range(1, 4)
        ]
        conversations = [
            {
                "refId": f"fresh:c{index}",
                "sourceKind": "conversation",
                "summary": f"A regular discussed public chorus detail {index}.",
                "observedAt": f"2026-07-19T1{index}:00:00Z",
                "subjectRef": f"discord_user:{index}",
                "participantAlias": f"participant-{index:08x}",
                "displayName": f"Member{index}",
                "channelPolicy": "public_home",
            }
            for index in range(1, 4)
        ]
        packet = journal.build_packet_from_sources(
            self.db,
            1,
            START,
            END,
            relays,
            conversations,
            entry_kind="daily",
        )
        prompt = journal.build_generation_prompt(packet)

        self.assertNotIn("lowActivityMode", packet)
        self.assertNotIn("reflectionBasis", packet)
        self.assertNotIn("LOW-ACTIVITY EVIDENCE RULE", prompt)
        self.assertEqual(
            "ee42f35a6cceb13317a857635263ea710b12b066239affc30fccd40fcad69c85",
            hashlib.sha256(prompt.encode("utf-8")).hexdigest(),
        )

    def test_historical_projection_excludes_private_and_test_lanes_and_rechecks_deletion(self):
        public_seq = self.record(
            source_kind="discord_message",
            source_key="public-42",
            when="2026-07-01T12:00:00Z",
            raw_text="Alice shared a public chorus revision with the room.",
            channel_policy="public_home",
            subject_ref="discord_user:42",
            private_display_name="Alice",
        )
        sealed_seq = self.record(
            source_kind="discord_message",
            source_key="sealed-43",
            when="2026-07-02T12:00:00Z",
            raw_text="A sealed test discussed a private plan.",
            channel_policy="sealed_test",
            subject_ref="discord_user:43",
            private_display_name="Secret",
        )
        ineligible_seq = self.record(
            source_kind="discord_message",
            source_key="ineligible-44",
            when="2026-07-03T12:00:00Z",
            raw_text="A deleted source should not return.",
            channel_policy="public_home",
            subject_ref="discord_user:44",
            private_display_name="Deleted",
            public_usable=False,
        )
        queue_seq = self.record(
            source_kind="discord_message",
            source_key="queue-45",
            when="2026-07-04T12:00:00Z",
            raw_text="A public queue test row.",
            channel_policy="public_home",
            subject_ref="discord_user:45",
            private_display_name="QueueTester",
            metadata={"mode": "queue_test"},
        )
        relay_seq = self.record(
            source_kind="website_relay",
            source_key="relay-1",
            when="2026-07-05T12:00:00Z",
            raw_text="A public BARCODE relay preserved an accepted show thread.",
            channel_policy="public_relay",
            subject_ref="bnl_01",
            private_display_name="BNL-01",
            metadata={"event_type": "accepted"},
        )

        packet = self.packet()
        basis_refs = {item["refId"] for item in packet["reflectionBasis"]}
        self.assertIn(f"reflection:event:{public_seq}", basis_refs)
        self.assertIn(f"reflection:event:{relay_seq}", basis_refs)
        self.assertNotIn(f"reflection:event:{sealed_seq}", basis_refs)
        self.assertNotIn(f"reflection:event:{ineligible_seq}", basis_refs)
        self.assertNotIn(f"reflection:event:{queue_seq}", basis_refs)
        self.assertNotIn("Alice", json.dumps(packet["reflectionBasis"]))
        provenance = packet["privateReflectionBasisProvenance"][
            "historicalSourceEvents"
        ]
        self.assertIn(
            "discord_user:42",
            {item.get("subjectRef") for item in provenance},
        )
        article = reflection_article(
            packet,
            body=(
                "An earlier public record preserved a chorus revision without "
                "exposing the person behind it. I admit the archive benefits "
                "from that kind of careful continuity."
            ),
            refs=[f"reflection:event:{public_seq}"],
        )
        draft = journal.store_validated_draft(self.db, 1, packet, article)
        self.assertTrue(draft.ok, draft.reason)
        with sqlite3.connect(self.db) as conn:
            metadata = json.loads(
                conn.execute(
                    "SELECT metadata_json FROM bnl_journal_private_metadata "
                    "WHERE entry_id=? AND revision=?",
                    (draft.entry_id, draft.revision),
                ).fetchone()[0]
            )
            self.assertEqual(
                f"reflection:event:{public_seq}",
                metadata["usedReflectionBasis"][0]["refId"],
            )
            self.assertEqual(
                "",
                automation._prepared_invalidation_reason(
                    conn,
                    1,
                    metadata,
                    set(),
                ),
            )

        with sqlite3.connect(self.db) as conn:
            self.assertEqual(
                "",
                automation._frozen_packet_invalidation_reason(conn, 1, packet),
            )
        source_store.purge_user_discord_sources(self.db, 1, 42)
        with sqlite3.connect(self.db) as conn:
            self.assertEqual(
                "privacy_source_ineligible",
                automation._frozen_packet_invalidation_reason(conn, 1, packet),
            )
            self.assertEqual(
                "privacy_source_ineligible",
                automation._prepared_invalidation_reason(
                    conn,
                    1,
                    metadata,
                    set(),
                ),
            )

    def test_public_safe_broadcast_memory_can_be_reflection_basis(self):
        with sqlite3.connect(self.db) as conn:
            conn.execute(
                """CREATE TABLE broadcast_memory(
                    id INTEGER PRIMARY KEY,guild_id INTEGER,episode_date TEXT,
                    cleaned_summary TEXT,entry_type TEXT,importance TEXT,
                    public_safe INTEGER,usage_scope TEXT,valid_until TEXT,
                    needs_clarification INTEGER,status TEXT,superseded_by_id INTEGER,
                    created_at TEXT,raw_note TEXT
                )"""
            )
            conn.executemany(
                "INSERT INTO broadcast_memory VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                [
                    (
                        1,
                        1,
                        "2026-07-10",
                        "A public broadcast preserved a careful chorus revision.",
                        "notable_moment",
                        "medium",
                        1,
                        "journal",
                        None,
                        0,
                        "active",
                        None,
                        "2026-07-10T12:00:00Z",
                        "private operator wording",
                    ),
                    (
                        2,
                        1,
                        "2026-07-11",
                        "An internal plan must stay private.",
                        "notable_moment",
                        "medium",
                        1,
                        "internal",
                        None,
                        0,
                        "active",
                        None,
                        "2026-07-11T12:00:00Z",
                        "private",
                    ),
                ],
            )

        packet = self.packet()
        memory = [
            item
            for item in packet["reflectionBasis"]
            if item["basisKind"] == "established_broadcast_memory"
        ]
        self.assertEqual(1, len(memory))
        self.assertIn("careful chorus revision", memory[0]["summary"])
        self.assertNotIn("operator wording", json.dumps(memory))
        self.assertNotIn("internal plan", json.dumps(memory))

    def test_reflection_validates_but_cannot_claim_current_activity_or_fill_fresh_breadth(self):
        packet = self.packet()
        article = reflection_article(packet)
        self.assertEqual("", journal.validate_article(article, packet, []))

        article["sections"][0]["body"] = (
            "Today GALAKNOISE produced a new BARCODE track."
        )
        self.assertEqual(
            "current_activity_without_fresh_source",
            journal.validate_article(article, packet, []),
        )

        article = reflection_article(packet)
        packet["evidenceCoverageContract"] = {
            **packet["evidenceCoverageContract"],
            "minimumDistinctFreshSources": 1,
        }
        self.assertEqual(
            "insufficient_source_breadth",
            journal.validate_article(article, packet, []),
        )

        packet["safeSources"] = [
            {
                "refId": "fresh:999",
                "sourceKind": "relay",
                "summary": "A current public relay announced a BARCODE track.",
                "observedAt": "2026-07-19T12:00:00Z",
            }
        ]
        article = reflection_article(
            packet,
            body="Today a public BARCODE relay announced a track.",
            refs=["fresh:999"],
        )
        self.assertEqual("", journal.validate_article(article, packet, []))

    def test_no_current_or_reflection_basis_keeps_no_new_source_blocking(self):
        with patch.object(
            journal,
            "_build_reflection_basis",
            return_value=([], {}),
        ):
            packet = self.packet()
        article = reflection_article(packet, refs=["reflection:missing"])
        self.assertEqual("no_new_source", journal.validate_article(article, packet, []))

        calls = []
        result = journal.generate_and_store_packet_draft(
            self.db,
            1,
            packet,
            lambda *_args: calls.append(1) or "{}",
        )
        self.assertFalse(result.ok)
        self.assertEqual("insufficient_grounded_material", result.reason)
        self.assertEqual([], calls)

    def test_blocking_low_activity_output_uses_all_four_repairs_and_no_fallback(self):
        packet = self.packet()
        calls = []

        def generator(source_packet, _prompt):
            calls.append(1)
            return generated_reflection_json(
                source_packet,
                body="Today GALAKNOISE produced a new BARCODE track.",
            )

        result = journal.generate_and_store_packet_draft(
            self.db,
            1,
            packet,
            generator,
        )
        self.assertFalse(result.ok)
        self.assertEqual("current_activity_without_fresh_source", result.reason)
        self.assertEqual(journal.JOURNAL_GENERATION_ATTEMPTS, len(calls))
        with sqlite3.connect(self.db) as conn:
            self.assertEqual(
                0,
                conn.execute("SELECT COUNT(*) FROM bnl_journal_entries").fetchone()[0],
            )

    def test_daily_low_activity_prepares_once_instead_of_finishing_quiet(self):
        packet = self.packet()
        calls = []

        def generator(source_packet, _prompt):
            calls.append(1)
            return generated_reflection_json(source_packet)

        with patch.object(
            automation,
            "build_source_packet_between",
            return_value=packet,
        ):
            first = automation._prepare_daily_window(
                self.db,
                1,
                generator,
                START,
                END,
                "2026-07-19",
                force=True,
            )
            second = automation._prepare_daily_window(
                self.db,
                1,
                lambda *_args: self.fail("prepared occurrence regenerated"),
                START,
                END,
                "2026-07-19",
            )

        self.assertEqual("prepared", first.status, first)
        self.assertEqual("prepared", second.status, second)
        self.assertEqual(1, len(calls))
        with sqlite3.connect(self.db) as conn:
            lifecycle, reason = conn.execute(
                "SELECT lifecycle_state,reason FROM bnl_journal_automation_runs"
            ).fetchone()
        self.assertEqual("prepared", lifecycle)
        self.assertEqual("prepared_waiting_release", reason)


if __name__ == "__main__":
    unittest.main()
