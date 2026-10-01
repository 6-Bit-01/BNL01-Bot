"""Publication flow tests; mocked revisions do not prove model accuracy."""
import copy
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import Mock, patch

import bnl_journal as journal
import bnl_journal_source_store as source_store


class JournalGroundedRevisionTests(unittest.TestCase):
    def setUp(self):
        self.packet = {
            "entryKind": "daily",
            "sourceWindowStart": "2026-09-29T00:00:00Z",
            "sourceWindowEnd": "2026-09-30T00:00:00Z",
            "editorialVersion": journal.JOURNAL_EDITORIAL_VERSION,
            "safeSources": [
                {"refId": "fresh:1", "sourceKind": "conversation",
                 "summary": "Test Listener, have you finished the new chorus?",
                 "observedAt": "2026-09-29T10:00:00Z", "channelPolicy": "public_home",
                 "participantAlias": "participant-11111111", "publicSpeakerName": "Test Composer"},
                {"refId": "fresh:2", "sourceKind": "conversation",
                 "summary": "I am still working on that chorus.",
                 "observedAt": "2026-09-29T10:01:00Z", "channelPolicy": "public_home",
                 "participantAlias": "participant-22222222", "publicSpeakerName": "Test Listener"},
            ],
            "privatePublicPeople": [
                {"participantAlias": "participant-11111111", "publicName": "Test Composer",
                 "sourceRefIds": ["fresh:1"]},
                {"participantAlias": "participant-22222222", "publicName": "Test Listener",
                 "sourceRefIds": ["fresh:2"]},
            ],
            "history": {},
        }

    def draft(self, body="Test Composer asked Test Listener about the unfinished chorus.",
              *, title="A Chorus Still Taking Shape", refs=None):
        return json.dumps({
            "title": title, "excerpt": "An unfinished chorus leaves something to return to.",
            "sections": [{"heading": "Room for Another Listen", "body": body,
                          "sourceRefIds": ["fresh:1", "fresh:2"] if refs is None else refs}],
            "metadata": {"topicTags": ["chorus"], "contextUses": []},
        }, indent=2)

    def run_sequence(self, outputs, *, prior_titles=None, max_attempts=None, generation_guard=None):
        generator = Mock(side_effect=outputs)
        events = []
        result = journal._generate_article_with_repairs(
            self.packet, generator, prior_titles or [], events.append,
            max_attempts=max_attempts,
            generation_guard=generation_guard,
        )
        return result, generator, events

    def test_valid_first_draft_requires_second_call_with_exact_draft_and_original_packet(self):
        first = self.draft()
        final = self.draft("Test Listener still has a chorus in progress. I have room for another listen.",
                           title="Room for the Next Listen")
        before = copy.deepcopy(self.packet)
        with patch.object(journal, "build_generation_prompt", wraps=journal.build_generation_prompt) as prompts:
            (article, reason, advisory), generator, events = self.run_sequence([first, final])
        self.assertEqual(generator.call_count, 2)
        self.assertTrue(all(call.args[0] is self.packet for call in generator.call_args_list))
        self.assertEqual(self.packet, before)
        self.assertEqual(prompts.call_args_list[1].kwargs["repair_reason"], "source_grounded_revision")
        self.assertEqual(prompts.call_args_list[1].kwargs["previous_output"], first)
        first_packet, _ = json.JSONDecoder().raw_decode(
            generator.call_args_list[0].args[1].split("Generation-safe packet:\n", 1)[1])
        second_packet, _ = json.JSONDecoder().raw_decode(
            generator.call_args_list[1].args[1].split("Generation-safe packet:\n", 1)[1])
        self.assertEqual(first_packet, second_packet)
        supplied_draft = generator.call_args_list[1].args[1].split(
            "Complete previous draft (not evidence):\n", 1)[1]
        self.assertEqual(json.loads(supplied_draft), json.loads(first))
        self.assertEqual(article, journal.parse_generated_json(final))
        self.assertEqual(reason, "")
        self.assertFalse(advisory)
        self.assertEqual([event["outcome"] for event in events if event["phase"] == "finished"],
                         ["source_revision_required", "accepted"])
        self.assertNotIn("Test Listener", json.dumps(events))

    def test_structural_repair_does_not_replace_grounded_revision(self):
        first_acceptable = self.draft()
        final = self.draft(title="The Chorus Can Wait")
        with patch.object(journal, "build_generation_prompt", wraps=journal.build_generation_prompt) as prompts:
            (article, reason, _), generator, _ = self.run_sequence(["not json", first_acceptable, final])
        self.assertEqual(generator.call_count, 3)
        self.assertEqual(prompts.call_args_list[1].kwargs["repair_reason"], "malformed_json")
        self.assertEqual(prompts.call_args_list[2].kwargs["repair_reason"], "source_grounded_revision")
        self.assertEqual(prompts.call_args_list[2].kwargs["previous_output"], first_acceptable)
        self.assertEqual(article["title"], "The Chorus Can Wait")
        self.assertEqual(reason, "")

    def test_unreviewed_advisory_is_not_a_fallback_when_revision_provider_fails(self):
        title = "A Previously Published Title"
        for prior_titles in ([], [title]):
            with self.subTest(advisory=bool(prior_titles)):
                (article, reason, advisory), generator, _ = self.run_sequence(
                    [self.draft(title=title), RuntimeError("provider stopped")], prior_titles=prior_titles)
                self.assertIsNone(article)
                self.assertEqual(reason, "provider_failure")
                self.assertFalse(advisory)
                self.assertEqual(generator.call_count, 2)

    def test_provider_failure_before_a_candidate_stops_without_review(self):
        for failure, expected in (("local_model_budget_exhausted", "local_budget_unavailable"),
                                  ("journal_preparation_timeout", "journal_preparation_timeout")):
            with self.subTest(failure=failure):
                (article, reason, _), generator, _ = self.run_sequence([RuntimeError(failure)])
                self.assertIsNone(article)
                self.assertEqual(reason, expected)
                self.assertEqual(generator.call_count, 1)

    def test_reviewed_advisory_survives_later_polish_failure(self):
        title = "A Previously Published Title"
        first = self.draft("Test Composer asked about the chorus.", title=title)
        reviewed = self.draft("Test Listener is still working on the chorus.", title=title)
        (article, reason, advisory), generator, events = self.run_sequence(
            [first, reviewed, RuntimeError("provider stopped")], prior_titles=[title])
        self.assertEqual(generator.call_count, 3)
        self.assertEqual(article, journal.parse_generated_json(reviewed))
        self.assertEqual(reason, "")
        self.assertTrue(advisory)
        finished = [event for event in events if event["phase"] == "finished"]
        self.assertNotIn("retainedPublishable", finished[0])
        self.assertTrue(finished[1]["retainedPublishable"])

    def test_reviewed_advisory_survives_exhausted_malformed_polish(self):
        title = "A Previously Published Title"
        reviewed = self.draft("Test Listener is still working on the chorus.", title=title)
        (article, reason, advisory), generator, _ = self.run_sequence(
            [self.draft(title=title), reviewed, "not json", "not json"], prior_titles=[title])
        self.assertEqual(generator.call_count, 4)
        self.assertEqual(article, journal.parse_generated_json(reviewed))
        self.assertEqual(reason, "")
        self.assertTrue(advisory)

    def test_guard_before_next_call_discards_reviewed_advisory_without_calling_provider(self):
        title = "A Previously Published Title"
        guard = Mock(side_effect=["", "", "", "", "privacy_source_ineligible"])
        (article, reason, advisory), generator, _ = self.run_sequence(
            [self.draft(title=title)] * 4, prior_titles=[title], generation_guard=guard)
        self.assertEqual(generator.call_count, 2)
        self.assertIsNone(article)
        self.assertEqual(reason, "privacy_source_ineligible")
        self.assertFalse(advisory)

    def test_unavailable_guard_fails_closed_without_exposing_its_error(self):
        guard = Mock(side_effect=RuntimeError("private failure detail"))
        (article, reason, advisory), generator, events = self.run_sequence(
            [self.draft()], generation_guard=guard)
        self.assertIsNone(article)
        self.assertEqual(reason, "generation_guard_unavailable")
        self.assertFalse(advisory)
        generator.assert_not_called()
        self.assertNotIn("private failure detail", json.dumps(events))

    def test_bad_revision_uses_remaining_slots_without_a_fifth_call_or_first_draft_fallback(self):
        for bad_revision, expected in (("not json", "malformed_json"),
                                       (self.draft(refs=["fresh:missing"]), "invalid_section_source_refs")):
            with self.subTest(reason=expected):
                (article, reason, advisory), generator, _ = self.run_sequence(
                    [self.draft(), bad_revision, bad_revision, bad_revision], max_attempts=99)
                self.assertEqual(generator.call_count, 4)
                self.assertIsNone(article)
                self.assertEqual(reason, expected)
                self.assertFalse(advisory)

    def test_malformed_revision_can_be_repaired_in_remaining_slot(self):
        final = self.draft(title="One More Listen Tomorrow")
        (article, reason, advisory), generator, _ = self.run_sequence([self.draft(), "not json", final])
        self.assertEqual(generator.call_count, 3)
        self.assertEqual(article, journal.parse_generated_json(final))
        self.assertEqual(reason, "")
        self.assertFalse(advisory)

    def test_first_acceptable_draft_in_final_slot_is_withheld(self):
        for limit, outputs in ((1, [self.draft()]), (4, ["not json"] * 3 + [self.draft()])):
            with self.subTest(limit=limit):
                (article, reason, advisory), generator, _ = self.run_sequence(outputs, max_attempts=limit)
                self.assertEqual(generator.call_count, limit)
                self.assertIsNone(article)
                self.assertEqual(reason, "source_grounded_revision")
                self.assertFalse(advisory)

    def test_no_source_packet_still_stops_before_generation(self):
        packet = {"safeSources": [], "coverageComplete": True}
        generator = Mock()
        with patch.object(journal, "store_validated_draft") as store:
            result = journal.generate_and_store_packet_draft("unused.db", 1, packet, generator)
        self.assertFalse(result.ok)
        generator.assert_not_called()
        store.assert_not_called()

    def test_attribution_and_event_connection_analogs_only_return_the_supplied_revision(self):
        # Both flawed drafts currently pass structural checks. These fixtures
        # prove they reach source-grounded revision and never escape as the
        # final result; they do not prove Gemini will identify their mistakes.
        cases = (
            ("Test Composer said the new chorus was finished.",
             "Test Composer asked Test Listener about the chorus; Test Listener said it was unfinished."),
            ("Test Listener's update prompted Test Composer's earlier question.",
             "Test Composer asked about the chorus before Test Listener's update arrived."),
        )
        for flawed, corrected in cases:
            with self.subTest(flawed=flawed):
                first, reviewed = self.draft(flawed), self.draft(corrected)
                self.assertEqual(journal.validate_article(journal.parse_generated_json(first), self.packet, []), "")
                (article, reason, _), generator, _ = self.run_sequence([first, reviewed])
                self.assertEqual(generator.call_count, 2)
                self.assertIn(flawed, generator.call_args_list[1].args[1])
                self.assertIn(self.packet["safeSources"][1]["summary"], generator.call_args_list[1].args[1])
                self.assertEqual(article["sections"][0]["body"], corrected)
                self.assertEqual(reason, "")


class JournalArchivedGenerationGuardTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.db = str(Path(self.temp.name) / "journal.db")
        journal.ensure_schema(self.db)
        source_store.ensure_schema(self.db)
        source_store.record_source_event(
            self.db, guild_id=1, source_kind="discord_message", source_key="test-message",
            occurred_at_ms=source_store.timestamp_to_epoch_ms("2026-09-29T10:00:00Z"),
            raw_text="The new chorus is still unfinished.",
            sanitized_summary="The new chorus is still unfinished.",
            subject_ref="discord_user:10", private_display_name="Test Composer",
            channel_id=100, channel_policy="public_home", public_usable=True,
        )
        self.packet = journal.build_source_packet_between(
            self.db, 1, "2026-09-29T00:00:00Z", "2026-09-30T00:00:00Z",
            entry_kind="manual", prepare_schema=False,
        )
        self.assertTrue(self.packet["sourceArchiveAvailable"])

    @staticmethod
    def draft(packet):
        return json.dumps({
            "title": "A Chorus Still Taking Shape", "excerpt": "There is more to hear later.",
            "sections": [{"heading": "Room for Another Listen",
                          "body": "Test Composer is still working on the chorus.",
                          "sourceRefIds": [source["refId"] for source in packet["safeSources"]]}],
            "metadata": {"contextUses": []},
        })

    def test_archive_backed_manual_draft_rechecks_sources_before_revision(self):
        calls = []
        removed = []

        def generator(packet, _prompt):
            calls.append(packet)
            removed.append(source_store.purge_user_discord_sources(self.db, 1, 10))
            return self.draft(packet)

        result = journal.generate_and_store_packet_draft(self.db, 1, self.packet, generator)
        self.assertEqual(removed, [1])
        self.assertFalse(result.ok)
        self.assertEqual(result.reason, "privacy_source_ineligible")
        self.assertEqual(len(calls), 1)
        with sqlite3.connect(self.db) as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM bnl_journal_entries").fetchone()[0], 0)

    def test_archive_backed_regeneration_keeps_old_draft_when_source_is_withdrawn(self):
        original = journal.generate_and_store_packet_draft(
            self.db, 1, self.packet, lambda packet, _prompt: self.draft(packet))
        self.assertTrue(original.ok, original.reason)
        calls = []
        removed = []

        def generator(packet, _prompt):
            calls.append(packet)
            removed.append(source_store.purge_user_discord_sources(self.db, 1, 10))
            return self.draft(packet)

        with patch.object(journal, "build_source_packet", return_value=self.packet):
            result = journal.regenerate_draft(self.db, 1, original.entry_id, 24, generator)
        self.assertEqual(removed, [1])
        self.assertFalse(result.ok)
        self.assertEqual(result.reason, "privacy_source_ineligible")
        self.assertEqual(len(calls), 1)
        with sqlite3.connect(self.db) as conn:
            self.assertEqual(conn.execute(
                "SELECT revision,lifecycle_state FROM bnl_journal_entries WHERE entry_id=?",
                (original.entry_id,)).fetchall(), [(1, "draft")])


if __name__ == "__main__":
    unittest.main()
