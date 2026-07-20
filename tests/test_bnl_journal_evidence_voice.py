import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone

import bnl_journal as journal


def _article(
    packet,
    *,
    refs=None,
    opening="A producer brought a bass sketch into the room.",
    reaction=True,
    reaction_text="I admit the room's patience has become one of my preferred recurring details.",
):
    texture = (
        "The music moved through several careful revisions while regulars compared what changed, what remained, "
        "and which small choices made the shared idea easier to hear. "
    )
    body = opening + " " + texture * 15
    if reaction:
        body += " " + reaction_text
    return {
        "title": "The Room Kept the Thread",
        "excerpt": "A day's worth of public music talk became one connected community story.",
        "sections": [{"heading": "What Kept Returning", "body": body}],
        "sourceRefIds": {
            "What Kept Returning": list(refs or [source["refId"] for source in packet["safeSources"]])
        },
        "metadata": {
            "topicTags": ["music"],
            "subjectRefs": [],
            "continuityNotes": [],
            "unresolvedQuestions": [],
            "confidenceFlags": ["grounded"],
            "safetyFlags": ["anonymous"],
            "contextUses": [],
        },
    }


class JournalEvidenceVoiceTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.NamedTemporaryFile(delete=False)
        self.db = tmp.name
        tmp.close()
        journal.ensure_schema(self.db)
        self.relays = [
            {
                "refId": f"fresh:r{index}",
                "sourceKind": "relay",
                "summary": f"A public mix update carried bass detail number {index}.",
                "observedAt": f"2026-07-19T{hour:02d}:00:00Z",
                "eventType": "canon",
            }
            for index, hour in enumerate((8, 10, 14, 16, 20, 22), 1)
        ]
        self.conversations = [
            {
                "refId": f"fresh:c{index}",
                "sourceKind": "conversation",
                "summary": f"A regular discussed chorus movement and listening detail number {index}.",
                "observedAt": f"2026-07-19T{hour:02d}:30:00Z",
                "participantAlias": f"participant-{index:08x}",
                "subjectRef": f"discord_user:{index}",
                "displayName": f"Member{index}",
                "channelPolicy": "public_home",
            }
            for index, hour in enumerate((9, 11, 15, 17, 21, 23), 1)
        ]
        self.packet = journal.build_packet_from_sources(
            self.db,
            1,
            "2026-07-19T07:00:00Z",
            "2026-07-20T07:00:00Z",
            self.relays,
            self.conversations,
            entry_kind="daily",
        )

    def test_daily_contract_requires_breadth_not_merely_a_large_input(self):
        contract = self.packet["evidenceCoverageContract"]
        self.assertEqual(5, contract["minimumDistinctFreshSources"])
        self.assertEqual(["conversation", "relay"], contract["requiredSourceKinds"])
        self.assertEqual(3, contract["minimumDistinctParticipants"])
        self.assertEqual(3, contract["minimumDistinctWindowSegments"])

        self.assertEqual("", journal.validate_article(_article(self.packet), self.packet, []))
        one_ref = _article(self.packet, refs=[self.packet["safeSources"][0]["refId"]])
        self.assertEqual("insufficient_source_breadth", journal.validate_article(one_ref, self.packet, []))

    def test_prompt_uses_mixed_voice_without_a_fixed_style_template(self):
        prompt = journal.build_generation_prompt(self.packet)
        self.assertIn("JOURNAL EDITORIAL OVERRIDE", prompt)
        self.assertIn("four beats", prompt)
        self.assertIn("Blend them in any order and any combination", prompt)
        self.assertIn("fixed section template", prompt)
        self.assertIn("evidenceCoverageContract", prompt)
        self.assertIn("concrete, recognizable action", prompt)
        self.assertIn("Never invent a time, place, object, action", prompt)
        self.assertIn("Use a direct quote only rarely", prompt)
        self.assertIn("website relay sources as derivative readings", prompt)
        self.assertIn("do not give the underlying activity a second vote", prompt)
        self.assertIn("keep the whole source window in view", prompt)
        self.assertIn("conversationSources—not relaySources", prompt)
        self.assertIn("windowSegmentActivity", prompt)
        self.assertNotIn("Do not include direct quotes", prompt)

    def test_daily_packet_discounts_relay_echoes_and_reports_full_window_density(self):
        start_at = datetime(2026, 7, 19, tzinfo=timezone.utc)

        def stamp(segment, index, count):
            offset = int((index + 1) * 8 * 60 / (count + 1))
            return (start_at + timedelta(hours=segment * 8, minutes=offset)).isoformat().replace("+00:00", "Z")

        relay_counts = (23, 24, 24)
        relays = [
            {
                "refId": f"fresh:relay-{segment}-{index}",
                "sourceKind": "relay",
                "summary": "A scheduled relay paraphrased a batch of public conversations.",
                "observedAt": stamp(segment, index, count),
                "eventType": "fresh_public_discord_activity",
            }
            for segment, count in enumerate(relay_counts)
            for index in range(count)
        ]
        conversation_counts = (37, 9, 92)
        conversations = [
            {
                "refId": f"fresh:conversation-{segment}-{index}",
                "sourceKind": "conversation",
                "summary": f"A regular shared a distinct public moment {segment}-{index}.",
                "observedAt": stamp(segment, index, count),
                "participantAlias": f"participant-{segment}-{index}",
                "subjectRef": f"discord_user:{segment * 1000 + index}",
                "displayName": f"WindowMember{segment:01d}{index:03d}",
                "channelPolicy": "public_home",
            }
            for segment, count in enumerate(conversation_counts)
            for index in range(count)
        ]

        packet = journal.build_packet_from_sources(
            self.db,
            1,
            "2026-07-19T00:00:00Z",
            "2026-07-20T00:00:00Z",
            relays,
            conversations,
            entry_kind="daily",
        )

        prompt_relays = [source for source in packet["privateSources"] if source["sourceKind"] == "relay"]
        prompt_conversations = [source for source in packet["privateSources"] if source["sourceKind"] == "conversation"]
        self.assertEqual(journal.MAX_DAILY_DERIVATIVE_RELAY_PROMPT_SOURCES, len(prompt_relays))
        self.assertEqual(sum(conversation_counts), len(prompt_conversations))
        self.assertEqual(71, packet["aggregateCounts"]["eligibleRelays"])
        self.assertEqual(138, packet["aggregateCounts"]["eligibleConversations"])
        self.assertEqual(3, packet["aggregateCounts"]["promptRelays"])
        self.assertEqual(138, packet["aggregateCounts"]["promptConversations"])
        self.assertEqual(
            [
                {"segment": "early", "conversationSources": 37, "relaySources": 23},
                {"segment": "middle", "conversationSources": 9, "relaySources": 24},
                {"segment": "late", "conversationSources": 92, "relaySources": 24},
            ],
            packet["windowSegmentActivity"],
        )
        relay_segments = {
            journal._coverage_segment(
                source["observedAt"],
                packet["sourceWindowStart"],
                packet["sourceWindowEnd"],
                3,
            )
            for source in prompt_relays
        }
        self.assertEqual({"segment-1", "segment-2", "segment-3"}, relay_segments)

    def test_daily_conversation_sampling_preserves_quiet_segments_without_flattening_busy_one(self):
        start_at = datetime(2026, 7, 19, tzinfo=timezone.utc)
        counts = (20, 3, 220)
        conversations = []
        for segment, count in enumerate(counts):
            for index in range(count):
                offset = int((index + 1) * 8 * 60 / (count + 1))
                conversations.append({
                    "refId": f"fresh:conversation-{segment}-{index}",
                    "sourceKind": "conversation",
                    "summary": f"A regular shared moment {segment}-{index}.",
                    "observedAt": (
                        start_at + timedelta(hours=segment * 8, minutes=offset)
                    ).isoformat().replace("+00:00", "Z"),
                    "participantAlias": f"participant-{segment}-{index}",
                    "subjectRef": f"discord_user:{segment * 1000 + index}",
                    "displayName": f"Member{segment:01d}{index:03d}",
                    "channelPolicy": "public_home",
                })
        relays = [
            {
                "refId": f"fresh:relay-{index}",
                "sourceKind": "relay",
                "summary": "A derivative relay summarized public activity.",
                "observedAt": (
                    start_at + timedelta(minutes=index * 120)
                ).isoformat().replace("+00:00", "Z"),
                "eventType": "fresh_public_discord_activity",
            }
            for index in range(12)
        ]

        sampled = journal._sample_source_kinds(
            relays,
            conversations,
            start="2026-07-19T00:00:00Z",
            end="2026-07-20T00:00:00Z",
            entry_kind="daily",
        )
        sampled_conversations = [source for source in sampled if source["sourceKind"] == "conversation"]
        sampled_by_segment = {
            segment: [
                source
                for source in sampled_conversations
                if source["refId"].startswith(f"fresh:conversation-{segment}-")
            ]
            for segment in range(3)
        }

        self.assertEqual(180, len(sampled))
        self.assertEqual(20, len(sampled_by_segment[0]))
        self.assertEqual(3, len(sampled_by_segment[1]))
        self.assertGreater(len(sampled_by_segment[2]), len(sampled_by_segment[0]))

    def test_daily_keeps_distinct_relay_status_while_discounting_repeated_echoes(self):
        start_at = datetime(2026, 7, 19, tzinfo=timezone.utc)
        relays = [
            {
                "refId": f"fresh:echo-{index}",
                "sourceKind": "relay",
                "summary": "A scheduled relay paraphrased public Discord activity.",
                "observedAt": (
                    start_at + timedelta(minutes=(index + 1) * 45)
                ).isoformat().replace("+00:00", "Z"),
                "eventType": "fresh_public_discord_activity",
            }
            for index in range(30)
        ]
        relays.insert(15, {
            "refId": "fresh:distinct-status",
            "sourceKind": "relay",
            "summary": "A distinct public Network status changed.",
            "observedAt": "2026-07-19T12:00:00Z",
            "eventType": "status_change",
        })
        conversations = [
            {
                "refId": f"fresh:conversation-{index}",
                "sourceKind": "conversation",
                "summary": f"A regular shared public moment {index}.",
                "observedAt": (
                    start_at + timedelta(minutes=(index + 1) * 100)
                ).isoformat().replace("+00:00", "Z"),
            }
            for index in range(12)
        ]

        sampled = journal._sample_source_kinds(
            relays,
            conversations,
            start="2026-07-19T00:00:00Z",
            end="2026-07-20T00:00:00Z",
            entry_kind="daily",
        )

        self.assertIn("fresh:distinct-status", {source["refId"] for source in sampled})
        self.assertEqual(
            3,
            len([
                source
                for source in sampled
                if source.get("eventType") == "fresh_public_discord_activity"
            ]),
        )
        self.assertEqual(12, len([source for source in sampled if source["sourceKind"] == "conversation"]))

    def test_relay_only_daily_window_keeps_existing_source_breadth(self):
        start_at = datetime(2026, 7, 19, tzinfo=timezone.utc)
        relays = [
            {
                "refId": f"fresh:relay-only-{index}",
                "sourceKind": "relay",
                "summary": f"Unique relay status {index}.",
                "observedAt": (
                    start_at + timedelta(minutes=(index + 0.5) * 24 * 60 / 200)
                ).isoformat().replace("+00:00", "Z"),
                "eventType": "fresh_public_discord_activity",
            }
            for index in range(200)
        ]
        sampled = journal._sample_source_kinds(
            relays,
            [],
            start="2026-07-19T00:00:00Z",
            end="2026-07-20T00:00:00Z",
            entry_kind="daily",
        )
        self.assertEqual(journal.MAX_PROMPT_SOURCES, len(sampled))
        self.assertEqual("fresh:relay-only-0", sampled[0]["refId"])
        self.assertEqual("fresh:relay-only-199", sampled[-1]["refId"])
        self.assertEqual(
            {"segment-1", "segment-2", "segment-3"},
            {
                journal._coverage_segment(
                    source["observedAt"],
                    "2026-07-19T00:00:00Z",
                    "2026-07-20T00:00:00Z",
                    3,
                )
                for source in sampled
            },
        )

    def test_report_opening_clinical_language_and_missing_reaction_are_rejected(self):
        report = _article(self.packet, opening="The Network observes a producer carrying a bass sketch through the room.")
        self.assertEqual("flat_report_voice", journal.validate_article(report, self.packet, []))

        clinical = _article(self.packet, opening="A producer brought nascent audio signals and internal schematics into the room.")
        self.assertEqual("overly_clinical_voice", journal.validate_article(clinical, self.packet, []))

        no_reaction = _article(self.packet, reaction=False)
        self.assertEqual("missing_bnl_reaction", journal.validate_article(no_reaction, self.packet, []))

    def test_affective_first_person_reaction_is_not_an_inference_claim(self):
        for reaction in (
            "I smiled when the room gave the idea another careful pass.",
            "I confess the patience on display pleased me.",
            "I am fond of the way this room lets a chorus change slowly.",
            "I felt the room's patience settle in.",
            "I laughed when the smallest change became the evening's main event.",
        ):
            with self.subTest(reaction=reaction):
                article = _article(self.packet, reaction_text=reaction)
                self.assertEqual("", journal.validate_article(article, self.packet, []))

    def test_quoted_first_person_line_does_not_replace_bnl_reaction(self):
        article = _article(
            self.packet,
            reaction=False,
            opening='A producer brought a bass sketch and said “I smiled when the loop finally landed.”',
        )
        self.assertEqual("missing_bnl_reaction", journal.validate_article(article, self.packet, []))

    def test_reaction_repair_names_natural_validator_compatible_stems(self):
        prompt = journal.build_generation_prompt(
            self.packet,
            repair_reason="missing_bnl_reaction",
            previous_output="{}",
        )
        self.assertIn("I laughed", prompt)
        self.assertIn("I felt", prompt)

    def test_all_window_names_are_scrubbed_and_validated_before_sampling(self):
        conversations = [
            {
                "refId": f"fresh:name-{index}",
                "sourceKind": "conversation",
                "summary": "A regular discussed a chorus.",
                "observedAt": f"2026-07-19T{index % 24:02d}:{index % 60:02d}:00Z",
                "participantAlias": f"participant-{index:08x}",
                "subjectRef": f"discord_user:{index}",
                "displayName": f"WindowName{index:03d}",
                "channelPolicy": "public_home",
            }
            for index in range(journal.MAX_PROMPT_SOURCES + 1)
        ]
        sampled = journal._sample_source_kinds([], conversations)
        sampled_names = {source["displayName"] for source in sampled}
        omitted_name = next(source["displayName"] for source in conversations if source["displayName"] not in sampled_names)
        for source in conversations:
            source["summary"] = f"{omitted_name} discussed a changing chorus with the room."

        packet = journal.build_packet_from_sources(
            self.db,
            1,
            "2026-07-19T00:00:00Z",
            "2026-07-20T00:00:00Z",
            [],
            conversations,
            entry_kind="daily",
        )

        self.assertNotIn(omitted_name, json.dumps(packet["safeSources"]))
        self.assertIn(omitted_name, packet["privateWindowDisplayNames"])
        article = _article(
            packet,
            opening=f"A regular and {omitted_name} brought a chorus back into the room.",
        )
        self.assertEqual("community_name_leak", journal.validate_article(article, packet, []))

    def test_topic_tags_are_frequency_ranked(self):
        sources = [
            {
                "refId": f"fresh:{index}",
                "sourceKind": "relay",
                "summary": "zebra rhythm returned" if index < 5 else "alpha rhythm appeared",
                "observedAt": f"2026-07-19T{8 + index:02d}:00:00Z",
            }
            for index in range(6)
        ]
        packet = journal.build_packet_from_sources(
            self.db,
            1,
            "2026-07-19T07:00:00Z",
            "2026-07-20T07:00:00Z",
            sources,
            [],
            entry_kind="daily",
        )
        self.assertEqual("rhythm", packet["candidateTopicTags"][0])
        self.assertLess(packet["candidateTopicTags"].index("zebra"), packet["candidateTopicTags"].index("alpha"))

    def test_memory_ineligible_entries_are_absent_from_all_history_lanes(self):
        first = journal.store_validated_draft(self.db, 1, self.packet, _article(self.packet))
        self.assertTrue(first.ok, first.reason)
        with journal.sqlite3.connect(self.db) as conn:
            conn.execute(
                "UPDATE bnl_journal_entries SET lifecycle_state='published',published_at='2026-07-20T08:00:00Z' WHERE entry_id=?",
                (first.entry_id,),
            )
            conn.execute(
                "UPDATE bnl_journal_private_metadata SET lifecycle_state='published' WHERE entry_id=?",
                (first.entry_id,),
            )

        included = journal.retrieve_history(self.db, 1, self.packet)
        self.assertEqual(first.entry_id, included["previousEntry"]["entry_id"])
        self.assertTrue(included["recurringTopicCounts"])

        excluded = journal.retrieve_history(
            self.db,
            1,
            self.packet,
            excluded_entry_ids={first.entry_id},
        )
        self.assertIsNone(excluded["previousEntry"])
        self.assertEqual([], excluded["relevantOlderEntries"])
        self.assertEqual({}, excluded["recurringTopicCounts"])
        self.assertEqual([], excluded["matchingContinuityNotes"])
        self.assertEqual([], excluded["matchingUnresolvedQuestions"])

    def test_history_prompt_keeps_bounded_public_prose_without_private_questions(self):
        entry = {
            "entry_id": "journal-old",
            "revision": 2,
            "title": "An Older Thread",
            "excerpt": "The room kept returning to a stubborn chorus.",
            "sections_json": json.dumps([
                {"heading": "The Chorus Returned", "body": "A detailed public-safe callback " * 40}
            ]),
            "published_at": "2026-07-10T12:00:00Z",
        }
        bounded = journal._bounded_history_for_prompt({
            "previousEntry": entry,
            "relevantOlderEntries": [entry],
            "matchingUnresolvedQuestions": ["Will the chorus return in another form?"],
        })
        snapshot = bounded["previousEntry"]["sectionSnapshots"][0]
        self.assertEqual("The Chorus Returned", snapshot["heading"])
        self.assertLessEqual(len(snapshot["bodyExcerpt"]), 420)
        self.assertNotIn("matchingUnresolvedQuestions", bounded)


if __name__ == "__main__":
    unittest.main()
