"""A selected show view must not describe omitted records as absent evidence."""

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

import test_tiktok_show_evidence_ledger as fixture
from bnl_journal_source_store import record_source_event
from bnl_tiktok_show_ledger import build_tiktok_show_evidence_context


class ShowRetrievalScopeTests(unittest.TestCase):
    def test_retained_people_and_comments_can_be_absent_from_selected_view(self):
        with tempfile.TemporaryDirectory() as directory:
            db_file = str(Path(directory) / "scope.db")
            fixture.TikTokShowEvidenceLedgerTests().seed_source_and_memory(db_file)
            authored = {}
            for index in range(24):
                name = f"Test Viewer {index:02d}"
                text = f"The copper lantern flickers beside station marker {index:02d}."
                authored[name] = text
                result = record_source_event(
                    db_file, guild_id=77, source_kind="tiktok_live_chat",
                    source_key=f"scope-comment-{index}",
                    occurred_at_ms=fixture.stamp("2026-08-29T00:03:00Z") + index * 1000,
                    raw_text=text, sanitized_summary=text,
                    channel_policy="public_context", public_usable=True,
                    subject_ref=f"tiktok_handle:scope.viewer.{index}",
                    private_display_name=name,
                    metadata={"eventType": "comment", "handle": f"scope.viewer.{index}"},
                )
                self.assertTrue(result.ok)
            fixture.sync_tiktok_show_evidence_ledgers(
                db_file, guild_id=77,
                read_model=fixture.authorized_read_model({
                    "currentShow": None, "latestShow": fixture.archived_show(), "shows": [],
                }),
                artist_identity_index=fixture.artist_index(),
                environ=fixture.ENABLED_QUEUE_ENV,
            )
            with sqlite3.connect(db_file) as conn:
                ledger = json.loads(conn.execute(
                    "SELECT ledger_json FROM tiktok_show_evidence_ledgers "
                    "WHERE guild_id=77 AND show_key='show-attendance-1'"
                ).fetchone()[0])
            selection = {}
            request = "What stood out in TikTok chat during the August 28, 2026 show?"
            rendered = build_tiktok_show_evidence_context(
                db_file, guild_id=77, user_text=request, message_limit=16,
                selection_out=selection,
            )
            omitted = {name: text for name, text in authored.items()
                       if name not in rendered and text not in rendered}
            self.assertTrue(omitted, "The fixture must actually exceed the rendered selection.")
            retained = {message["text"] for message in ledger["messages"]}
            retained_labels = {p["speakerLabel"] for p in ledger["participants"]}
            for name, text in omitted.items():
                self.assertIn(text, retained)
                self.assertTrue(any(name in label for label in retained_labels))
            self.assertLess(rendered.index("Retrieval scope:"), rendered.index("Show episode:"))
            self.assertLess(rendered.index("Verification scope:"), rendered.index("Show episode:"))
            self.assertIn("Selected participant records (partial list):", rendered)
            self.assertIn("does not report an exhaustive author or exact-quote absence search", rendered)
            self.assertNotIn("complete eligible TikTok chat ledger", rendered)
            self.assertIn(f'{ledger["coverage"]["eligibleMessageCount"]} TikTok messages;', rendered)
            self.assertIn(f'{ledger["coverage"]["participantCount"]} TikTok participants;', rendered)
            for excerpt in selection["authored_excerpts"]:
                self.assertIn(excerpt[5], rendered)
                if excerpt[6] == "tiktok":
                    self.assertIn(excerpt[5], retained)
            refreshed = {}
            rerendered = build_tiktok_show_evidence_context(
                db_file, guild_id=77, user_text=request, message_limit=16,
                pinned_show_keys=("show-attendance-1",), selection_out=refreshed,
            )
            self.assertEqual(rerendered, rendered)
            self.assertEqual(refreshed["authored_excerpts"], selection["authored_excerpts"])
