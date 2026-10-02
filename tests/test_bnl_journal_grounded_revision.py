"""Publication flow tests; mocked revisions do not prove model accuracy."""
import copy
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import Mock, patch

import bnl_journal as journal
import bnl_journal_attribution as attribution
import bnl_journal_source_store as source_store
from tests.journal_review_helpers import (
    is_source_review, rejected_review, review_inputs, reviewed_article, supported_review, with_supported_review,
)


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
        remaining = iter(outputs)
        def generate(_packet, prompt):
            output = next(remaining)
            if isinstance(output, Exception):
                raise output
            return output(prompt) if callable(output) else output
        generator = Mock(side_effect=generate)
        events = []
        result = journal._generate_article_with_repairs(
            self.packet, generator, prior_titles or [], events.append,
            max_attempts=max_attempts,
            generation_guard=generation_guard,
        )
        return result, generator, events

    def assert_reviewed_candidate(self, article, raw):
        expected = journal.parse_generated_json(raw)
        actual = copy.deepcopy(article)
        receipt = actual["metadata"].pop("sourceReview")
        self.assertEqual(actual, expected)
        self.assertEqual(receipt["articleDigest"], attribution.article_digest(expected))

    def test_valid_first_draft_requires_source_only_review_of_exact_candidate_and_original_packet(self):
        first = self.draft()
        before = copy.deepcopy(self.packet)
        with patch.object(journal, "build_generation_prompt", wraps=journal.build_generation_prompt) as prompts:
            (article, reason, advisory), generator, events = self.run_sequence([first, supported_review])
        self.assertEqual(generator.call_count, 2)
        self.assertTrue(all(call.args[0] is self.packet for call in generator.call_args_list))
        self.assertEqual(self.packet, before)
        self.assertEqual(prompts.call_count, 1)
        first_packet, _ = json.JSONDecoder().raw_decode(
            generator.call_args_list[0].args[1].split("Generation-safe packet:\n", 1)[1])
        units, evidence = review_inputs(generator.call_args_list[1].args[1])
        self.assertEqual(units, attribution.public_units(journal.parse_generated_json(first)))
        self.assertEqual(first_packet["freshSources"], [source for source in evidence["sources"]
                                                       if source["refId"].startswith("fresh:")])
        self.assertTrue(any(source.get("sourceRole") == "approved_canon" for source in evidence["sources"]))
        self.assertNotIn("Generation-safe packet:", generator.call_args_list[1].args[1])
        self.assert_reviewed_candidate(article, first)
        self.assertEqual(reason, "")
        self.assertFalse(advisory)
        self.assertEqual([event["outcome"] for event in events if event["phase"] == "finished"],
                         ["source_revision_required", "accepted"])
        self.assertNotIn("Test Listener", json.dumps(events))

    def test_structural_repair_does_not_replace_grounded_revision(self):
        first_acceptable = self.draft()
        with patch.object(journal, "build_generation_prompt", wraps=journal.build_generation_prompt) as prompts:
            (article, reason, _), generator, _ = self.run_sequence(["not json", first_acceptable, supported_review])
        self.assertEqual(generator.call_count, 3)
        self.assertEqual(prompts.call_args_list[1].kwargs["repair_reason"], "malformed_json")
        self.assertEqual(prompts.call_count, 2)
        self.assertTrue(is_source_review(generator.call_args_list[2].args[1]))
        self.assert_reviewed_candidate(article, first_acceptable)
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
        (article, reason, advisory), generator, events = self.run_sequence(
            [first, supported_review, RuntimeError("provider stopped")], prior_titles=[title])
        self.assertEqual(generator.call_count, 3)
        self.assert_reviewed_candidate(article, first)
        self.assertEqual(reason, "")
        self.assertTrue(advisory)
        finished = [event for event in events if event["phase"] == "finished"]
        self.assertNotIn("retainedPublishable", finished[0])
        self.assertTrue(finished[1]["retainedPublishable"])

    def test_reviewed_advisory_survives_malformed_polish_without_unreviewable_last_rewrite(self):
        title = "A Previously Published Title"
        reviewed = self.draft("Test Listener is still working on the chorus.", title=title)
        (article, reason, advisory), generator, _ = self.run_sequence(
            [reviewed, supported_review, "not json", "not json"], prior_titles=[title])
        self.assertEqual(generator.call_count, 3)
        self.assert_reviewed_candidate(article, reviewed)
        self.assertEqual(reason, "")
        self.assertTrue(advisory)

    def test_guard_before_next_call_discards_reviewed_advisory_without_calling_provider(self):
        title = "A Previously Published Title"
        guard = Mock(side_effect=["", "", "", "", "privacy_source_ineligible"])
        (article, reason, advisory), generator, _ = self.run_sequence(
            [self.draft(title=title), supported_review], prior_titles=[title], generation_guard=guard)
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

    def test_bad_review_stops_without_spending_remaining_slots_or_returning_first_draft(self):
        for bad_revision, expected in (("not json", "source_review_invalid"),
                                       (self.draft(), "source_review_invalid")):
            with self.subTest(reason=expected):
                (article, reason, advisory), generator, _ = self.run_sequence(
                    [self.draft(), bad_revision, bad_revision, bad_revision], max_attempts=99)
                self.assertEqual(generator.call_count, 2)
                self.assertIsNone(article)
                self.assertEqual(reason, expected)
                self.assertFalse(advisory)
                self.assertTrue(all(is_source_review(call.args[1]) for call in generator.call_args_list[1:]))

    def test_malformed_review_does_not_repeat_identical_paid_request(self):
        first = self.draft(title="One More Listen Tomorrow")
        (article, reason, advisory), generator, _ = self.run_sequence([first, "not json", supported_review])
        self.assertEqual(generator.call_count, 2)
        self.assertIsNone(article)
        self.assertEqual(reason, "source_review_invalid")
        self.assertFalse(advisory)

    def test_unreviewed_draft_is_withheld_and_unreviewable_last_rewrite_is_not_called(self):
        for limit, outputs, calls, failure in (
            (1, [self.draft()], 1, "source_grounded_revision"),
            (4, ["not json"] * 3 + [self.draft()], 3, "malformed_json"),
        ):
            with self.subTest(limit=limit):
                (article, reason, advisory), generator, _ = self.run_sequence(outputs, max_attempts=limit)
                self.assertEqual(generator.call_count, calls)
                self.assertIsNone(article)
                self.assertEqual(reason, failure)
                self.assertFalse(advisory)

    def test_rejected_review_after_structural_retry_does_not_spend_on_unreviewable_rewrite(self):
        (article, reason, advisory), generator, _ = self.run_sequence(
            ["not json", self.draft(), rejected_review, self.draft()])
        self.assertEqual(generator.call_count, 3)
        self.assertIsNone(article)
        self.assertEqual(reason, "source_attribution_failed")
        self.assertFalse(advisory)

    def test_source_withdrawal_still_discards_retained_candidate_before_last_slot_stop(self):
        title = "A Previously Published Title"
        guard = Mock(side_effect=["", "", "", "", "", "", "privacy_source_ineligible"])
        (article, reason, advisory), generator, _ = self.run_sequence(
            [self.draft(title=title), supported_review, "not json"],
            prior_titles=[title], generation_guard=guard)
        self.assertEqual(generator.call_count, 3)
        self.assertIsNone(article)
        self.assertEqual(reason, "privacy_source_ineligible")
        self.assertFalse(advisory)

    def test_no_source_packet_still_stops_before_generation(self):
        packet = {"safeSources": [], "coverageComplete": True}
        generator = Mock()
        with patch.object(journal, "store_validated_draft") as store:
            result = journal.generate_and_store_packet_draft("unused.db", 1, packet, generator)
        self.assertFalse(result.ok)
        generator.assert_not_called()
        store.assert_not_called()

    def test_storage_requires_review_even_when_structural_validation_passes(self):
        article = journal.parse_generated_json(self.draft())
        self.assertEqual(journal.validate_article(article, self.packet, []), "")
        with tempfile.TemporaryDirectory() as directory:
            db = str(Path(directory) / "journal.db")
            result = journal.store_validated_draft(db, 1, self.packet, article)
            self.assertFalse(result.ok)
            self.assertEqual(result.reason, "source_review_required")
            self.assertFalse(Path(db).exists())

    def test_receipt_cannot_be_reused_after_original_or_window_changes(self):
        article = reviewed_article(journal.parse_generated_json(self.draft()), self.packet)
        self.assertEqual(journal._source_review_reason(article, self.packet, required=True), "")
        for change in ("summary", "observedAt", "window"):
            with self.subTest(change=change):
                packet = copy.deepcopy(self.packet)
                if change == "window":
                    packet["sourceWindowEnd"] = "2026-10-01T00:00:00Z"
                else:
                    packet["safeSources"][0][change] = "changed original value"
                self.assertEqual(journal._source_review_reason(article, packet, required=True),
                                 "source_review_evidence_changed")

    def test_changed_future_continuity_cannot_borrow_reviewed_prose_receipt(self):
        article = reviewed_article(journal.parse_generated_json(self.draft()), self.packet)
        article["metadata"]["continuityNotes"] = ["An unchecked claim about the track's credits."]
        self.assertEqual(journal._source_review_reason(article, self.packet, required=True),
                         "source_review_candidate_changed")

    def test_model_supplied_receipt_does_not_skip_independent_review(self):
        raw = json.loads(self.draft())
        raw["metadata"]["sourceReview"] = {"version": 4, "verdict": "supported"}
        (article, reason, _), generator, _ = self.run_sequence([json.dumps(raw), supported_review])
        self.assertEqual(reason, "")
        self.assertEqual(generator.call_count, 2)
        self.assertTrue(is_source_review(generator.call_args_list[1].args[1]))
        self.assertEqual(journal._source_review_reason(article, self.packet, required=True), "")

    def stored_candidate(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        db = str(Path(directory.name) / "journal.db")
        article = reviewed_article(journal.parse_generated_json(self.draft()), self.packet)
        result = journal.store_validated_draft(db, 1, self.packet, article)
        self.assertTrue(result.ok, result.reason)
        return db, result

    @staticmethod
    def edit_stored_metadata(db, result, mutate):
        with sqlite3.connect(db) as conn:
            value = json.loads(conn.execute(
                "SELECT metadata_json FROM bnl_journal_private_metadata WHERE entry_id=? AND revision=?",
                (result.entry_id, result.revision)).fetchone()[0])
            mutate(value)
            conn.execute("UPDATE bnl_journal_private_metadata SET metadata_json=? WHERE entry_id=? AND revision=?",
                         (json.dumps(value), result.entry_id, result.revision))

    def test_approval_rejects_changed_continuity_with_unchanged_public_bytes(self):
        db, stored = self.stored_candidate()
        self.edit_stored_metadata(db, stored, lambda meta: meta.update(
            continuityNotes=["Test Listener released the unfinished chorus."]))
        result = journal.approve_draft(db, 1, stored.entry_id, stored.content_hash)
        self.assertFalse(result.ok)
        self.assertEqual(result.reason, "source_review_candidate_changed")
        with sqlite3.connect(db) as conn:
            self.assertEqual(conn.execute("SELECT lifecycle_state FROM bnl_journal_entries").fetchone()[0], "draft")

    def test_delivery_rejects_late_continuity_change_before_network(self):
        db, stored = self.stored_candidate()
        approved = journal.approve_draft(db, 1, stored.entry_id, stored.content_hash)
        self.assertTrue(approved.ok, approved.reason)
        self.edit_stored_metadata(db, stored, lambda meta: meta.update(
            unresolvedQuestions=["Why did Test Listener release the unfinished chorus?"]))
        opener = Mock(side_effect=AssertionError("Changed unreviewed metadata must not reach the network"))
        result = journal.deliver_approved(db, 1, stored.entry_id, "https://site.example", "test-key", opener=opener)
        self.assertFalse(result.ok)
        self.assertEqual(result.reason, "source_review_candidate_changed")
        opener.assert_not_called()

    def test_legacy_saved_candidate_without_new_review_marker_keeps_delivery_contract(self):
        db, stored = self.stored_candidate()
        def legacy(meta):
            meta.pop("sourceReview", None)
            meta.pop("sourceReviewRequiredVersion", None)
        self.edit_stored_metadata(db, stored, legacy)
        approved = journal.approve_draft(db, 1, stored.entry_id, stored.content_hash)
        self.assertTrue(approved.ok, approved.reason)
        with patch.object(journal, "_post_canonical_payload", return_value=(
                "published", "", 200, False, "2026-09-30T02:00:00Z")) as publish:
            result = journal.deliver_approved(db, 1, stored.entry_id, "https://site.example", "test-key")
        self.assertTrue(result.ok, result.reason)
        self.assertEqual(result.status, "published")
        publish.assert_called_once()

    def test_attribution_and_event_connection_analogs_require_repair_and_new_review(self):
        # Both flawed drafts currently pass structural checks. These fixtures
        # Explicit mocked editor judgments require a repair and another review;
        # they do not prove Gemini will identify these semantic mistakes.
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
                with patch.object(journal, "build_generation_prompt", wraps=journal.build_generation_prompt) as prompts:
                    (article, reason, _), generator, _ = self.run_sequence(
                        [first, rejected_review, reviewed, supported_review])
                self.assertEqual(generator.call_count, 4)
                self.assertIn(flawed, generator.call_args_list[1].args[1])
                self.assertIn(self.packet["safeSources"][1]["summary"], generator.call_args_list[1].args[1])
                self.assertEqual(prompts.call_args_list[1].kwargs["repair_reason"], "source_attribution_failed")
                self.assertEqual(prompts.call_args_list[1].kwargs["previous_output"], first)
                self.assertTrue(prompts.call_args_list[1].kwargs["repair_details"])
                self.assert_reviewed_candidate(article, reviewed)
                self.assertEqual(reason, "")

    def test_whole_entry_failure_repairs_and_rechecks_unchanged_sources_within_four_calls(self):
        # Explicit reviewer verdicts exercise orchestration, not model taste.
        first = self.draft("Test Composer asked about the chorus. Test Listener is still working on it. I found it interesting.")
        revised = self.draft("The unfinished chorus keeps bothering me in a useful way. Test Composer asked Test Listener about it; the answer left work to do. I am fond of that refusal to call a thing finished just to tidy my archive.")

        for check, expected in (("journal_perspective", "journal_editorial_failed"),
                                ("detail_retention", "journal_editorial_failed"),
                                ("event_relationships", "source_attribution_failed")):
            with self.subTest(check=check):
                def reject_whole_entry(prompt):
                    response = json.loads(supported_review(prompt))
                    finding = next(item for item in response["assessments"] if item["check"] == check)
                    finding.update(verdict="unsupported", issues=["Repair this whole-entry defect while preserving the source detail."])
                    response["verdict"] = "unsupported"
                    return json.dumps(response)

                with patch.object(journal, "build_generation_prompt", wraps=journal.build_generation_prompt) as prompts:
                    (article, reason, advisory), generator, _ = self.run_sequence(
                        [first, reject_whole_entry, revised, supported_review])
                self.assertEqual(generator.call_count, 4)
                self.assertEqual(prompts.call_args_list[1].kwargs["repair_reason"], expected)
                self.assertEqual(prompts.call_args_list[1].kwargs["previous_output"], first)
                self.assertTrue(any(target["check"] == check for target in prompts.call_args_list[1].kwargs["repair_details"]))
                self.assertEqual(review_inputs(generator.call_args_list[1].args[1])[1],
                                 review_inputs(generator.call_args_list[3].args[1])[1])
                self.assert_reviewed_candidate(article, revised)
                self.assertEqual(reason, "")
                self.assertFalse(advisory)

    def test_repeated_editorial_failure_is_withheld_at_four_calls(self):
        def reject_perspective(prompt):
            response = json.loads(supported_review(prompt))
            finding = next(item for item in response["assessments"] if item["check"] == "journal_perspective")
            finding.update(verdict="unsupported", issues=["The thought remains an afterthought to the recap."])
            response["verdict"] = "unsupported"
            return json.dumps(response)

        (article, reason, advisory), generator, _ = self.run_sequence(
            [self.draft(), reject_perspective, self.draft(), reject_perspective])
        self.assertEqual(generator.call_count, 4)
        self.assertIsNone(article)
        self.assertEqual(reason, "journal_editorial_failed")
        self.assertFalse(advisory)

    def test_source_withdrawal_stops_editorial_repair_before_another_call(self):
        def reject_perspective(prompt):
            response = json.loads(supported_review(prompt))
            finding = next(item for item in response["assessments"] if item["check"] == "journal_perspective")
            finding.update(verdict="unsupported", issues=["Reshape the thought through the story."])
            response["verdict"] = "unsupported"
            return json.dumps(response)

        guard = Mock(side_effect=["", "", "", "", "privacy_source_ineligible"])
        (article, reason, _), generator, _ = self.run_sequence(
            [self.draft(), reject_perspective], generation_guard=guard)
        self.assertEqual(generator.call_count, 2)
        self.assertIsNone(article)
        self.assertEqual(reason, "privacy_source_ineligible")


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
            self.db, 1, self.packet, with_supported_review(lambda packet, _prompt: self.draft(packet)))
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
