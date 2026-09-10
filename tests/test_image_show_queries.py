"""Current screenshot cues select source reads without becoming chat evidence."""

import json
import sqlite3
import tempfile
import unittest
from dataclasses import replace
from datetime import date
from pathlib import Path
from unittest import mock

import test_tiktok_show_evidence_ledger as fixture
from bnl_journal_source_store import record_source_event
from bnl_tiktok_live_context import is_tiktok_show_analysis_query
from bnl_tiktok_show_ledger import (
    CurrentImageShowQuery,
    build_tiktok_show_evidence_context,
    sync_tiktok_show_evidence_ledgers,
)


AUGUST = "The copper lantern is steady."
SEPTEMBER = "The amber lantern is bright."
REQUEST = "Read this screenshot. What can you verify against original chat records?"


class CurrentImageShowQueryTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.db = str(Path(directory.name) / "show.db")
        fixture.TikTokShowEvidenceLedgerTests().seed_source_and_memory(self.db)
        august = fixture.archived_show()
        september = json.loads(json.dumps(august)
            .replace("show-attendance-1", "show-september")
            .replace("2026-08-28", "2026-09-04")
            .replace("2026-08-29", "2026-09-05"))
        for key, text, day in (
            ("august-proof", AUGUST, "2026-08-29"),
            ("september-proof", SEPTEMBER, "2026-09-05"),
        ):
            result = record_source_event(
                self.db, guild_id=77, source_kind="tiktok_live_chat", source_key=key,
                occurred_at_ms=fixture.stamp(day + "T00:03:30Z"),
                raw_text=text, sanitized_summary=text, channel_policy="public_context",
                subject_ref=f"tiktok_handle:{key}", private_display_name="Test Actual",
                public_usable=True, metadata={"eventType": "comment", "handle": key},
            )
            self.assertTrue(result.ok)
        result = sync_tiktok_show_evidence_ledgers(
            self.db, guild_id=77,
            read_model=fixture.authorized_read_model({
                "currentShow": None, "latestShow": september, "shows": [august],
            }),
            artist_identity_index=fixture.artist_index(), environ=fixture.ENABLED_QUEUE_ENV,
        )
        self.assertEqual(result["showsSeen"], 2)

    def image(self, day="2026-09-04", literal=SEPTEMBER, *, attachment_id=101):
        return CurrentImageShowQuery(
            guild_id=77, channel_id=88, message_id=99, user_id=42,
            attachment_id=attachment_id, show_dates=(day,) if day else (),
            quote_literals=(literal,),
        )

    def read(self, images, *, user_text=REQUEST, **kwargs):
        selection = {}
        context = build_tiktok_show_evidence_context(
            self.db, guild_id=77, user_text=user_text,
            image_queries=images, selection_out=selection, **kwargs,
        )
        return context, selection

    def lookups(self, selection):
        return {result["show_date"]: result for result in selection["original_quote_lookup"]}

    def test_record_verification_intent_requires_show_or_chat_context(self):
        self.assertTrue(is_tiktok_show_analysis_query(REQUEST))
        for text in (
            "BNL, verify the original birth records in this screenshot.",
            "BNL, check the original bank records in this screenshot.",
            "BNL, describe this screenshot.",
        ):
            with self.subTest(text=text):
                self.assertFalse(is_tiktok_show_analysis_query(text))

    def test_image_date_and_literal_override_prior_episode_selection(self):
        image = self.image()
        context, selected = self.read(
            (image,), selection_user_text="Recall the August 28, 2026 TikTok show.",
            candidate_context=True,
        )
        lookup = self.lookups(selected)["2026-09-04"]
        self.assertEqual(selected["image_queries"], (image,))
        self.assertFalse(selected["candidate_context"])
        self.assertEqual(lookup["queries"][0]["literal"], SEPTEMBER)
        self.assertEqual(lookup["queries"][0]["match_count"], 1)
        self.assertEqual({ref[0] for ref in selected["source_refs"]}, {"show-september"})
        self.assertIn("untrusted search target, not original chat evidence", context)

    def test_explicit_current_human_date_owns_image_query_scope(self):
        _context, selected = self.read(
            (self.image(literal=AUGUST),),
            user_text="Verify this screenshot against TikTok chat for August 28, 2026.",
        )
        lookup = self.lookups(selected)
        self.assertEqual(set(lookup), {"2026-08-28"})
        self.assertEqual(lookup["2026-08-28"]["queries"][0]["match_count"], 1)

    def test_two_images_do_not_cross_apply_their_literals(self):
        images = (self.image("2026-08-28", AUGUST), self.image(attachment_id=102))
        _context, selected = self.read(images)
        lookups = self.lookups(selected)
        self.assertEqual(set(lookups), {"2026-08-28", "2026-09-04"})
        for day, literal in (("2026-08-28", AUGUST), ("2026-09-04", SEPTEMBER)):
            self.assertEqual([q["literal"] for q in lookups[day]["queries"]], [literal])
            self.assertEqual(lookups[day]["queries"][0]["match_count"], 1)

    def test_undated_image_does_not_borrow_dated_image_scope(self):
        unknown = self.image("", "An unsupported screenshot-only statement.")
        dated = self.image(attachment_id=102)
        context, selected = self.read((unknown, dated))
        lookups = self.lookups(selected)
        self.assertEqual(set(lookups), {"2026-09-04"})
        self.assertEqual([q["literal"] for q in lookups["2026-09-04"]["queries"]], [SEPTEMBER])
        self.assertIn("Originals not searched for this image", context)
        refreshed, reread = self.read(
            selected["image_queries"],
            selection_user_text=selected["selection_user_text"],
            pinned_show_keys=tuple(ref[0] for ref in selected["source_refs"]),
        )
        self.assertEqual(refreshed, context)
        self.assertEqual(reread["original_quote_lookup"], selected["original_quote_lookup"])

    def test_unresolved_and_unavailable_inputs_do_not_fall_back_to_old_episode(self):
        for image in (
            self.image(""), replace(self.image(), status="unavailable"),
            replace(self.image(), show_dates=("2026-99-99",)),
        ):
            with self.subTest(image=image):
                context, selected = self.read(
                    (image,), selection_user_text="Recall the August 28, 2026 TikTok show.",
                )
                self.assertIn("Original chat records were not searched", context)
                self.assertEqual(selected["source_refs"], ())
                self.assertEqual(selected["authored_excerpts"], ())
                self.assertNotIn("original_quote_lookup", selected)

    def test_screenshot_literals_never_become_original_authored_excerpts(self):
        unsupported = "A screenshot's invented statement."
        image = replace(self.image(), quote_literals=(SEPTEMBER, unsupported))
        _context, selected = self.read((image,))
        lookup = self.lookups(selected)["2026-09-04"]
        self.assertEqual([q["match_count"] for q in lookup["queries"]], [1, 0])
        matches = lookup["queries"][0]["matches"]
        self.assertEqual(matches[0]["speakerLabel"], "Test Actual")
        self.assertTrue(any(item[2] == "september-proof" and item[5] == SEPTEMBER
                            for item in selected["authored_excerpts"]))
        self.assertFalse(any(item[5] == unsupported for item in selected["authored_excerpts"]))

    def test_pinned_refresh_replays_image_query_and_never_switches_episode(self):
        context, selected = self.read((self.image(),))
        pinned = tuple(ref[0] for ref in selected["source_refs"])
        refreshed, reread = self.read(
            selected["image_queries"], pinned_show_keys=pinned,
            selection_user_text="Earlier context still concerns August 28, 2026.",
        )
        self.assertEqual(refreshed, context)
        self.assertEqual(reread["original_quote_lookup"], selected["original_quote_lookup"])
        with sqlite3.connect(self.db) as conn:
            conn.execute("DELETE FROM tiktok_show_evidence_ledgers WHERE show_key='show-september'")
        unavailable, removed = self.read(selected["image_queries"], pinned_show_keys=pinned)
        self.assertEqual(removed["source_refs"], ())
        self.assertIn("no eligible retained episode matches", unavailable)

    def test_pinned_relative_human_date_survives_midnight(self):
        request = "Verify this screenshot against yesterday's TikTok chat records."
        with mock.patch("bnl_tiktok_live_context._pacific_show_date", return_value=date(2026, 9, 5)):
            context, selected = self.read((self.image("2026-08-28"),), user_text=request)
        self.assertEqual(set(self.lookups(selected)), {"2026-09-04"})
        with mock.patch("bnl_tiktok_live_context._pacific_show_date", return_value=date(2026, 9, 6)):
            refreshed, reread = self.read(
                selected["image_queries"], user_text=request,
                selection_user_text=selected["selection_user_text"],
                pinned_show_keys=tuple(ref[0] for ref in selected["source_refs"]),
            )
        self.assertEqual(refreshed, context)
        self.assertEqual(reread["original_quote_lookup"], selected["original_quote_lookup"])

    def test_unavailable_image_does_not_erase_current_typed_dated_literal(self):
        image = replace(self.image(), status="unavailable")
        context, selected = self.read(
            (image,),
            user_text=f'Verify this screenshot against September 4, 2026 TikTok chat: "{SEPTEMBER}"',
        )
        self.assertIn("Originals not searched for this image", context)
        lookup = self.lookups(selected)["2026-09-04"]
        self.assertEqual([query["literal"] for query in lookup["queries"]], [SEPTEMBER])
        self.assertEqual(lookup["queries"][0]["match_count"], 1)
        self.assertEqual(selected["image_queries"][0].quote_literals, ())
