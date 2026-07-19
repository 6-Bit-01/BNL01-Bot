import json
import tempfile
import unittest

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
                "eventType": "fresh_public_activity",
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
        ):
            with self.subTest(reaction=reaction):
                article = _article(self.packet, reaction_text=reaction)
                self.assertEqual("", journal.validate_article(article, self.packet, []))

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
