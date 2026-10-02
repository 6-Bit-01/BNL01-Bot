"""Exact saved-candidate review binding; mocked receipts are protocol fixtures."""
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import bnl_journal as journal
from tests.journal_review_helpers import reviewed_article
from tests import test_bnl_journal_grounded_revision as fixtures


class JournalReviewLifecycleTests(unittest.TestCase):
    setUp = fixtures.JournalGroundedRevisionTests.setUp
    draft = fixtures.JournalGroundedRevisionTests.draft

    def stored(self, db):
        article = reviewed_article(journal.parse_generated_json(self.draft()), self.packet)
        return journal.store_validated_draft(db, 1, self.packet, article)

    def edit_metadata(self, db, edit):
        with sqlite3.connect(db) as conn:
            raw = conn.execute("SELECT metadata_json FROM bnl_journal_private_metadata").fetchone()[0]
            meta = json.loads(raw)
            edit(meta)
            conn.execute("UPDATE bnl_journal_private_metadata SET metadata_json=?", (json.dumps(meta),))

    def test_changed_continuity_blocks_approval_without_mutating_draft(self):
        with tempfile.TemporaryDirectory() as directory:
            db = str(Path(directory) / "test.db")
            result = self.stored(db)
            self.assertTrue(result.ok, result.reason)
            self.edit_metadata(db, lambda meta: meta.update(continuityNotes=["An unchecked release occurred."]))
            approved = journal.approve_draft(db, 1, result.entry_id, result.content_hash)
            self.assertFalse(approved.ok)
            self.assertEqual(approved.reason, "source_review_candidate_changed")
            with sqlite3.connect(db) as conn:
                self.assertEqual(conn.execute("SELECT lifecycle_state FROM bnl_journal_entries").fetchone()[0], "draft")

    def test_changed_continuity_blocks_delivery_before_network(self):
        with tempfile.TemporaryDirectory() as directory:
            db = str(Path(directory) / "test.db")
            result = self.stored(db)
            approved = journal.approve_draft(db, 1, result.entry_id, result.content_hash)
            self.assertTrue(approved.ok, approved.reason)
            self.edit_metadata(db, lambda meta: meta.update(unresolvedQuestions=["Who owns the uncredited release?"]))
            with patch.object(journal, "_post_canonical_payload", new=Mock()) as post:
                delivered = journal.deliver_approved(db, 1, result.entry_id, "https://example.invalid", "test-token")
            self.assertFalse(delivered.ok)
            self.assertEqual(delivered.reason, "source_review_candidate_changed")
            post.assert_not_called()

    def test_legacy_owed_payload_keeps_existing_approval_path(self):
        with tempfile.TemporaryDirectory() as directory:
            db = str(Path(directory) / "test.db")
            result = self.stored(db)
            def legacy(meta):
                meta.pop("sourceReview")
                meta.pop("sourceReviewRequiredVersion")
            self.edit_metadata(db, legacy)
            approved = journal.approve_draft(db, 1, result.entry_id, result.content_hash)
            self.assertTrue(approved.ok, approved.reason)

    def test_new_saved_candidate_cannot_drop_its_review_receipt(self):
        with tempfile.TemporaryDirectory() as directory:
            db = str(Path(directory) / "test.db")
            result = self.stored(db)
            self.edit_metadata(db, lambda meta: meta.pop("sourceReview"))
            approved = journal.approve_draft(db, 1, result.entry_id, result.content_hash)
            self.assertFalse(approved.ok)
            self.assertEqual(approved.reason, "source_review_required")
