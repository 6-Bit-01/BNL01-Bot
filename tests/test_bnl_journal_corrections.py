"""Historical correction lifecycle; mocked prose does not prove model accuracy."""
import gc
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import Mock, patch

import bnl_journal as journal
import bnl_journal_source_store as store
from tests.journal_review_helpers import is_source_review, review_inputs, with_supported_review


START = "2026-09-28T00:00:00Z"
END = "2026-09-29T00:00:00Z"
PUBLISHED = "2026-09-29T02:00:00Z"
CORRECTED = "2026-10-01T04:00:00Z"


class Response:
    status = 200

    def __init__(self, entry):
        self.entry = entry

    def read(self):
        return json.dumps({"ok": True, "persisted": True, "entry": self.entry}).encode()

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


class JournalCorrectionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.cleanup)
        self.db = str(Path(self.temp.name) / "journal.db")
        journal.ensure_schema(self.db)
        store.ensure_schema(self.db)
        with patch.object(store, "_now_ms", return_value=0):
            self.record("101", "The unfinished chorus still needs another listen.")
            self.record("102", "A future concert must never enter the earlier correction.",
                        observed="2026-09-30T12:00:00Z")
        self.packet = journal.build_source_packet_between(
            self.db, 1, START, END, entry_kind="manual", prepare_schema=False)
        self.assertTrue(self.packet["sourceArchiveAvailable"])
        self.assertTrue(self.packet["coverageComplete"])
        self.entry_id = "journal_" + "a" * 16
        self.base = self.publish_fixture(self.entry_id, 1, "The First Chorus",
                                         "An earlier article assigned the unfinished chorus incorrectly.")

    def cleanup(self):
        gc.collect()
        self.temp.cleanup()

    def record(self, key, text, observed="2026-09-28T12:00:00Z"):
        return store.record_source_event(
            self.db, guild_id=1, source_kind="discord_message", source_key=key,
            occurred_at_ms=store.timestamp_to_epoch_ms(observed), raw_text=text,
            sanitized_summary=text, channel_id=100, channel_policy="public_home",
            subject_ref="discord_user:10", private_display_name="Test Composer",
            public_usable=True, metadata={"messageId": key})

    @staticmethod
    def article(packet, title="A Chorus Corrected", body="Test Composer still has an unfinished chorus.", tag="chorus"):
        return json.dumps({
            "title": title, "excerpt": "There is more to hear later.",
            "sections": [{"heading": "Another Listen", "body": body,
                          "sourceRefIds": [source["refId"] for source in packet["safeSources"]]}],
            "metadata": {"topicTags": [tag], "continuityNotes": [tag + " continuity"],
                         "unresolvedQuestions": [tag + " question"], "contextUses": []},
        })

    def publish_fixture(self, entry_id, revision, title, body, *, packet=None, tag="chorus", published=PUBLISHED):
        packet = packet or self.packet
        article = journal.parse_generated_json(self.article(packet, title, body, tag))
        row, meta, result = journal._draft_records(1, packet, article, entry_id=entry_id, revision=revision)
        with sqlite3.connect(self.db) as conn:
            journal._insert_draft_rows(conn, row, meta)
            conn.execute("UPDATE bnl_journal_entries SET lifecycle_state='published',published_at=? WHERE entry_id=? AND revision=?",
                         (published, entry_id, revision))
            conn.execute("UPDATE bnl_journal_private_metadata SET lifecycle_state='published' WHERE entry_id=? AND revision=?",
                         (entry_id, revision))
        return result

    def kwargs(self, **overrides):
        return {"previous_revision": 1, "previous_content_hash": self.base.content_hash,
                "note": "Corrected the speaker attribution.",
                "control_authority_identity": (1, "revision", "digest", (), ()), **overrides}

    def generate(self, *, preview=False, generator=None, **kwargs):
        generator = generator or Mock(side_effect=with_supported_review(lambda packet, _prompt: self.article(packet)))
        fn = journal.generate_published_correction_preview if preview else journal.generate_published_correction_draft
        return fn(self.db, 1, self.entry_id, generator, **self.kwargs(**kwargs)), generator

    def stored(self, revision=2):
        with sqlite3.connect(self.db) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute("SELECT e.*,m.metadata_json FROM bnl_journal_entries e JOIN bnl_journal_private_metadata m "
                               "ON m.guild_id=e.guild_id AND m.entry_id=e.entry_id AND m.revision=e.revision "
                               "WHERE e.entry_id=? AND e.revision=?", (self.entry_id, revision)).fetchone()
            return dict(row) if row else None

    @staticmethod
    def controls(context):
        return "" if context.get("controlAuthorityIdentity") == [1, "revision", "digest", [], []] else "correction_controls_changed"

    def approve(self, result, **kwargs):
        return journal.approve_draft(self.db, 1, self.entry_id, result.content_hash, 2,
                                     correction_guard=self.controls, **kwargs)

    def test_preview_reconstructs_exact_historical_window_without_old_prose_or_writes(self):
        before = Path(self.db).read_bytes()
        with patch.object(journal, "build_generation_prompt", wraps=journal.build_generation_prompt) as prompts:
            result, generator = self.generate(preview=True)
        self.assertTrue(result["ok"], result)
        self.assertEqual(generator.call_count, 2)
        self.assertEqual(Path(self.db).read_bytes(), before)
        self.assertEqual((result["packet"]["sourceWindowStart"], result["packet"]["sourceWindowEnd"]), (START, END))
        self.assertNotIn("future concert", json.dumps(result["packet"]))
        self.assertNotIn(self.entry_id, json.dumps(result["packet"]["history"]))
        self.assertEqual(prompts.call_args_list[0].kwargs["repair_reason"], "published_correction")
        self.assertEqual("", prompts.call_args_list[0].kwargs["previous_output"])
        writing_prompt = generator.call_args_list[0].args[1]
        self.assertNotIn("assigned the unfinished chorus incorrectly", writing_prompt)
        self.assertNotIn("The First Chorus", writing_prompt)
        self.assertNotIn("Complete previous draft", writing_prompt)
        self.assertNotIn("Make a targeted correction", writing_prompt)
        self.assertIn("Reconstruct this historical Journal", writing_prompt)
        projected, _ = json.JSONDecoder().raw_decode(writing_prompt.split("Generation-safe packet:\n", 1)[1])
        self.assertEqual(projected["freshSources"], result["packet"]["safeSources"])
        self.assertEqual((projected["sourceWindowStart"], projected["sourceWindowEnd"]), (START, END))
        self.assertEqual(prompts.call_count, 1)
        self.assertTrue(is_source_review(generator.call_args_list[1].args[1]))
        units, evidence = review_inputs(generator.call_args_list[1].args[1])
        self.assertIn(result["article"]["title"], [unit["text"] for unit in units])
        self.assertEqual(evidence["sourceWindowEnd"], END)
        self.assertEqual(result["originalPublishedAt"], PUBLISHED)

    def test_historical_reconstruction_cannot_reintroduce_accidental_old_output(self):
        defective = self.article(self.packet, title="Obsolete Invented Encore",
                                 body="Test Composer performed an invented encore on Mars.")
        prompt = journal.build_generation_prompt(
            self.packet, repair_reason="published_correction", previous_output=defective,
        )
        self.assertNotIn("Obsolete Invented Encore", prompt)
        self.assertNotIn("invented encore on Mars", prompt)
        self.assertIn("The unfinished chorus still needs another listen.", prompt)
        self.assertIn("later clarifications", prompt)
        self.assertIn("personal reactions", prompt)

    def test_ordinary_repair_still_receives_the_new_candidate_and_located_failure(self):
        candidate = self.article(self.packet, title="A New Candidate Needs Review")
        prompt = journal.build_generation_prompt(
            self.packet, repair_reason="source_attribution_failed", previous_output=candidate,
            repair_details=[{"field": "sections[0].body", "check": "speaker_mismatch"}],
        )
        self.assertIn("Complete previous draft", prompt)
        self.assertIn("A New Candidate Needs Review", prompt)
        self.assertIn("speaker_mismatch", prompt)
        self.assertIn("Make a targeted correction", prompt)

    def test_new_revision_is_draft_with_lineage_and_unchanged_original(self):
        original = self.stored(1)
        result, generator = self.generate()
        self.assertTrue(result.ok, result.reason)
        self.assertEqual((result.entry_id, result.revision, result.status), (self.entry_id, 2, "draft"))
        self.assertEqual(generator.call_count, 2)
        self.assertEqual(self.stored(1), original)
        row = self.stored()
        entry = json.loads(row["public_payload_json"])["entry"]
        self.assertEqual(entry["correction"], {"previousRevision": 1, "previousContentHash": self.base.content_hash,
                                               "note": "Corrected the speaker attribution."})
        self.assertEqual((entry["sourceWindowStart"], entry["sourceWindowEnd"]), (START, END))
        self.assertNotIn("publishedAt", entry)
        self.assertNotIn("correctedAt", entry)
        self.assertIsNone(row["published_at"])
        private = json.loads(row["metadata_json"])["publishedCorrection"]
        self.assertEqual(private["originalPublishedAt"], PUBLISHED)
        self.assertEqual(private["sourcePacket"]["safeSources"], self.packet["safeSources"])
        self.assertNotIn("sourcePacket", row["public_payload_json"])
        regen = Mock()
        blocked = journal.regenerate_draft(self.db, 1, self.entry_id, 24, regen)
        self.assertEqual(blocked.reason, "correction_requires_original_window")
        regen.assert_not_called()

    def test_stale_base_and_public_note_leaks_never_call_model(self):
        for kwargs in ({"previous_content_hash": "b" * 64}, {"note": "room:123 is the cause"},
                       {"note": "participant-1234abcd said it"}, {"note": "See https://example.test"}):
            with self.subTest(kwargs=kwargs):
                result, generator = self.generate(preview=True, **kwargs)
                self.assertFalse(result["ok"])
                generator.assert_not_called()

    def test_rejected_candidate_can_be_replaced_but_pending_candidate_cannot(self):
        original = self.stored(1)
        first, _ = self.generate()
        self.assertTrue(first.ok, first.reason)
        blocked, generator = self.generate()
        self.assertEqual(blocked.reason, "correction_revision_in_progress")
        generator.assert_not_called()
        rejected = journal.reject_draft(self.db, 1, self.entry_id, "Needs another pass", 2)
        self.assertTrue(rejected.ok, rejected.reason)
        replacement, generator = self.generate(generator=Mock(side_effect=with_supported_review(lambda packet, _prompt:
                                               self.article(packet, title="The Chorus, Reconsidered"))))
        self.assertTrue(replacement.ok, replacement.reason)
        self.assertEqual((replacement.revision, generator.call_count), (2, 2))
        self.assertNotEqual(replacement.content_hash, first.content_hash)
        context = json.loads(self.stored()["metadata_json"])["publishedCorrection"]
        self.assertEqual(context["replacedRejectedCorrection"]["contentHash"], first.content_hash)
        self.assertEqual(self.stored(1), original)
        self.assertTrue(self.approve(replacement).ok)
        blocked, generator = self.generate()
        self.assertEqual(blocked.reason, "correction_revision_in_progress")
        generator.assert_not_called()

    def test_withdrawal_during_first_call_stops_before_revision_and_stores_nothing(self):
        def generate(packet, _prompt):
            self.assertEqual(store.purge_user_discord_sources(self.db, 1, 10), 2)
            return self.article(packet)
        result, generator = self.generate(generator=Mock(side_effect=generate))
        self.assertFalse(result.ok)
        self.assertEqual(result.reason, "privacy_source_ineligible")
        self.assertEqual(generator.call_count, 1)
        self.assertIsNone(self.stored())

    def test_changed_predecessor_during_call_stops_before_review(self):
        def generate(packet, _prompt):
            with sqlite3.connect(self.db) as conn:
                conn.execute("UPDATE bnl_journal_entries SET content_hash=? WHERE entry_id=? AND revision=1",
                             ("b" * 64, self.entry_id))
            return self.article(packet)
        result, generator = self.generate(generator=Mock(side_effect=generate))
        self.assertFalse(result.ok)
        self.assertEqual(result.reason, "correction_base_changed")
        self.assertEqual(generator.call_count, 1)

    def test_unavailable_controls_between_calls_withhold_and_do_not_fallback(self):
        changed = []
        def generate(packet, _prompt):
            changed.append(True)
            return self.article(packet)
        result, generator = self.generate(generator=Mock(side_effect=generate),
                                          generation_guard=lambda: "journal_controls_changed" if changed else "")
        self.assertFalse(result.ok)
        self.assertEqual(result.reason, "journal_controls_changed")
        self.assertEqual(generator.call_count, 1)
        self.assertIsNone(self.stored())

    def test_approval_requires_current_controls_and_rechecks_delayed_source_withdrawal(self):
        result, _ = self.generate()
        self.assertTrue(result.ok, result.reason)
        blocked = journal.approve_draft(self.db, 1, self.entry_id, result.content_hash, 2)
        self.assertEqual(blocked.reason, "correction_controls_unavailable")
        self.assertEqual(store.purge_user_discord_sources(self.db, 1, 10), 2)
        blocked = self.approve(result)
        self.assertEqual(blocked.reason, "privacy_source_ineligible")
        self.assertEqual(self.stored()["lifecycle_state"], "draft")

    def test_delivery_rechecks_original_sources_after_approval(self):
        result, _ = self.generate()
        self.assertTrue(result.ok, result.reason)
        self.assertTrue(self.approve(result).ok)
        self.assertEqual(store.purge_user_discord_sources(self.db, 1, 10), 2)
        opener = Mock()
        delivered = journal.deliver_approved(self.db, 1, self.entry_id, "https://example.test", "test", opener,
                                             revision=2, correction_guard=self.controls)
        self.assertEqual(delivered.reason, "privacy_source_ineligible")
        opener.assert_not_called()

    def test_delivery_preserves_original_publication_and_records_server_correction_time(self):
        result, _ = self.generate()
        self.assertTrue(result.ok, result.reason)
        self.assertTrue(self.approve(result).ok)
        sent = []
        def opener(request, **_kwargs):
            entry = json.loads(request.data)["entry"]
            sent.append(entry)
            return Response({**entry, "publishedAt": PUBLISHED, "correctedAt": CORRECTED})
        delivered = journal.deliver_approved(self.db, 1, self.entry_id, "https://example.test", "test", opener,
                                             revision=2, correction_guard=self.controls)
        self.assertTrue(delivered.ok, delivered.reason)
        row = self.stored()
        self.assertEqual(row["published_at"], PUBLISHED)
        self.assertEqual(json.loads(row["metadata_json"])["correctionReceipt"],
                         {"publishedAt": PUBLISHED, "correctedAt": CORRECTED})
        self.assertEqual(len(sent), 1)
        self.assertNotIn("sourcePacket", json.dumps(sent))
        snapshot = journal.JournalControlSnapshot(1, "revision", "digest", CORRECTED, "2026-10-01T05:00:00Z", 3600)
        publication = journal._journal_publication_from_row(row, snapshot=snapshot, query_mode="latest")
        self.assertIsNotNone(publication)
        self.assertEqual(publication.revision, 2)
        altered = dict(row)
        payload = json.loads(row["public_payload_json"])
        payload["entry"]["correction"]["note"] = "An altered correction note."
        altered.update(public_payload_json=journal._json(payload), canonical_payload_bytes=journal._json(payload).encode())
        self.assertIsNone(journal._journal_publication_from_row(altered, snapshot=snapshot, query_mode="latest"))

    def test_wrong_publication_time_is_not_successful_delivery(self):
        result, _ = self.generate()
        self.assertTrue(result.ok, result.reason)
        self.assertTrue(self.approve(result).ok)
        def opener(request, **_kwargs):
            return Response({**json.loads(request.data)["entry"], "publishedAt": CORRECTED, "correctedAt": CORRECTED})
        delivered = journal.deliver_approved(self.db, 1, self.entry_id, "https://example.test", "test", opener,
                                             revision=2, correction_guard=self.controls)
        self.assertFalse(delivered.ok)
        self.assertEqual(delivered.reason, "correction_receipt_mismatch")
        self.assertIsNone(self.stored()["published_at"])

    def test_history_uses_current_published_revision_once_including_recurrence_metadata(self):
        self.publish_fixture(self.entry_id, 2, "The Corrected Chorus", "A revised account.", tag="revised")
        history = journal.retrieve_history(self.db, 1, self.packet, prepare_schema=False)
        self.assertEqual(history["previousEntry"]["revision"], 2)
        self.assertEqual(history["recurringTopicCounts"], {"revised": 1})
        self.assertNotIn("chorus continuity", history["matchingContinuityNotes"])
        self.assertFalse(history["relevantOlderEntries"])

    def test_historical_history_excludes_target_future_and_old_revision_metadata(self):
        old_packet = {**self.packet, "sourceWindowStart": "2026-09-26T00:00:00Z", "sourceWindowEnd": "2026-09-27T00:00:00Z"}
        old_id = "journal_" + "b" * 16
        self.publish_fixture(old_id, 1, "An Earlier Chorus", "Old assertion.", packet=old_packet,
                             published="2026-09-27T02:00:00Z", tag="obsolete")
        self.publish_fixture(old_id, 2, "An Earlier Chorus Corrected", "Corrected assertion.", packet=old_packet,
                             published="2026-09-27T02:00:00Z", tag="current")
        self.publish_fixture("journal_" + "c" * 16, 1, "A Future Journal", "Future discussion.",
                             published="2026-09-30T02:00:00Z", tag="future")
        result, _ = self.generate(preview=True)
        self.assertTrue(result["ok"], result)
        history = result["packet"]["history"]
        self.assertEqual((history["previousEntry"]["entry_id"], history["previousEntry"]["revision"]), (old_id, 2))
        self.assertEqual(history["recurringTopicCounts"], {"current": 1})
        self.assertNotIn("future", json.dumps(history).lower())
        self.assertNotIn("obsolete", json.dumps(history).lower())

    def test_shared_website_hash_fixture_and_ordinary_hash_are_distinct(self):
        sections = [{"heading": "A Second Listen", "body": "Test Listener described an unfinished chorus."}]
        correction = {"previousRevision": 1, "previousContentHash": "a" * 64,
                      "note": "Corrected the speaker attribution."}
        self.assertEqual(journal._hash("A Chorus Corrected", "A corrected attribution.", journal._json(sections), journal._json(correction)),
                         "511bdfa845aa7403bc7cb156f08ccef9907b296080ef779c2c121766a554b789")
        row = self.stored(1)
        self.assertEqual(row["content_hash"], journal._hash(row["title"], row["excerpt"], row["sections_json"]))


if __name__ == "__main__":
    unittest.main()
