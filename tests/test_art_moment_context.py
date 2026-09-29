"""Existing public Moments keep their detail and withdrawal fences in art."""
from contextlib import ExitStack
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
import sqlite3
from types import SimpleNamespace
import unittest
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-token")

import bnl01_bot as bot
import bnl_ambient_art as ambient
import bnl_own_art as art
from tests import test_bnl_journal_shared_inputs as journal_inputs

TEXT = "An old argument about reporters still leaves an interesting rhythm behind."
CONCEPT = {"action": "create", "title": "Paper Echoes", "meaning": "An imagined scene.",
           "imagePrompt": "An imagined room of paper waves.", "inspirationRefs": []}


class ArtMomentContextTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.fixture = journal_inputs.JournalSharedInputsTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.stack.enter_context(mock.patch.object(bot, "DB_FILE", self.fixture.db))
        self.stack.enter_context(mock.patch.object(bot, "_pacific_now",
            return_value=datetime.fromisoformat(journal_inputs.END.replace("Z", "+00:00"))))

    def change(self, sql, args=()):
        with sqlite3.connect(self.fixture.db) as conn:
            conn.execute(sql, args)

    def withdraw(self):
        self.change("UPDATE memory_ledger_entries SET lifecycle_status='forgotten' WHERE entry_id=?",
                    (self.fixture.roots[0],))

    def selected(self, topic="reporters journalists", guild_id=1):
        basis = {"guild_id": guild_id}
        records = ambient.moment_context(bot, guild_id, topic, source_basis=basis)
        return records, basis

    def test_private_brief_keeps_only_public_contribution_projection(self):
        packet = self.fixture.packet()
        moment = next(i for i in packet["reflectionBasis"] if i["basisKind"] == "public_moment")
        moment["contributions"][0]["privateRaw"] = "PRIVATE_DETAIL_DO_NOT_RENDER"
        prompt, refs = art.build_own_art_brief(packet)
        self.assertIn(moment["refId"], refs)
        self.assertIn("playfully", prompt)
        self.assertIn("Test Member 1", prompt)
        self.assertIn('"kind": "public_moment"', prompt)
        self.assertIn(moment["sourceObservedAt"], prompt)
        self.assertNotIn("PRIVATE_DETAIL_DO_NOT_RENDER", prompt)
        self.assertNotIn("discord_user:", prompt)
        self.assertNotIn("originalSourceRefs", prompt)

    def test_ambient_selects_existing_historical_detail_and_tracks_exact_version(self):
        records, basis = self.selected()
        self.assertEqual(len(records), 1)
        self.assertIn("playfully", records[0]["summary"])
        self.assertIn("Test Member 1", records[0]["summary"])
        self.assertIn("2026-08-27", records[0]["observedAt"])
        self.assertEqual(set(basis["moments"]), {self.fixture.mid})
        self.assertTrue(bot.revalidate_ambient_local_sources(1, basis))
        with mock.patch.dict(os.environ, {"BNL_OWNER_USER_ID": "1"}):
            owner_records, _ = self.selected()
        self.assertIn("recorded as 6 Bit", owner_records[0]["summary"])
        self.assertNotIn("Test Member 1", owner_records[0]["summary"])

    def test_unrelated_other_guild_private_future_and_forgotten_moments_are_excluded(self):
        self.assertEqual(self.selected("sourdough cinnamon custard")[0], [])
        self.assertEqual(self.selected(guild_id=99)[0], [])
        for column, changed, original in (
            ("channel_policy", "sealed_test", "public_home"),
            ("last_activity_at", "2026-09-01T12:00:00Z", "2026-08-27T12:00:50+00:00"),
        ):
            with self.subTest(column=column):
                self.change(f"UPDATE memory_moment_windows SET {column}=?", (changed,))
                self.assertEqual(self.selected()[0], [])
                self.change(f"UPDATE memory_moment_windows SET {column}=?", (original,))
        self.withdraw()
        self.assertEqual(self.selected()[0], [])

    async def test_original_or_attribution_changes_fail_every_existing_ambient_fence(self):
        _, basis = self.selected()
        self.change("UPDATE memory_moment_participants SET safe_display_name='Test Renamed Member'")
        self.assertFalse(bot.revalidate_ambient_local_sources(1, basis))
        with mock.patch.object(bot, "build_ambient_current_show_context", return_value="unknown"):
            basis["show"] = "unknown"
            for stage in ("after_generation", "after_image", "before_send", "before_website_art"):
                self.assertFalse(await bot.revalidate_ambient_sources(1, basis, stage=stage))

    async def generate(self, mutation=None):
        async def provider(*_args, **_kwargs):
            if mutation:
                mutation()
            return TEXT
        for name, value in (
            ("get_recent_guild_user_messages", ["The reporters and journalists joke returned."]),
            ("get_recent_ambient", []), ("build_dynamic_curiosity_payload", ([], "")),
            ("build_scoped_broadcast_memory_context", ""),
            ("build_ambient_current_show_context", "Current broadcast state is unknown."),
        ):
            self.stack.enter_context(mock.patch.object(bot, name, return_value=value))
        self.stack.enter_context(mock.patch.object(ambient, "available", return_value=False))
        provider_mock = self.stack.enter_context(mock.patch.object(bot, "get_gemini_response",
            new=mock.AsyncMock(side_effect=provider)))
        basis = {}
        result = await bot.generate_dynamic_ambient(1, 10, source_basis_out=basis)
        return result, basis, provider_mock

    async def test_real_ambient_assembly_includes_moment_without_an_extra_provider_call(self):
        result, basis, provider = await self.generate()
        self.assertEqual(result, TEXT)
        provider.assert_awaited_once()
        self.assertIn("playfully", provider.call_args.args[0])
        self.assertIn(self.fixture.mid, basis["moments"])

    async def test_withdrawal_during_ambient_generation_discards_without_retry(self):
        result, basis, provider = await self.generate(self.withdraw)
        self.assertEqual(result, "")
        self.assertEqual(basis, {})
        provider.assert_awaited_once()

    async def test_withdrawal_during_image_generation_blocks_private_draft(self):
        _, basis = self.selected()
        basis.update(show="unknown", art=CONCEPT)
        def image(*_args, **_kwargs):
            self.withdraw()
            return b"fixture", {"sha256": hashlib.sha256(b"fixture").hexdigest(), "mimeType": "image/png"}
        with mock.patch.object(ambient, "enabled", return_value=True), \
             mock.patch.object(ambient, "claim", return_value="fixture-art"), \
             mock.patch.object(ambient, "record"), \
             mock.patch.object(ambient, "generate_private_image", side_effect=image), \
             mock.patch.object(bot, "build_ambient_current_show_context", return_value="unknown"):
            self.assertIsNone(await ambient.prepare(bot, 1, basis))
        self.assertFalse((Path(self.fixture.db).parent / "bnl-own-art").exists())

    def assert_private_preview_rechecks(self, stage):
        packet = self.fixture.packet()
        def concept(*_args, **_kwargs):
            if stage == "concept":
                self.withdraw()
            return json.dumps(CONCEPT)
        def image(*_args, **_kwargs):
            self.withdraw()
            return b"fixture", {"mimeType": "image/png"}
        fake = SimpleNamespace(DB_FILE=self.fixture.db, BNL_PRIMARY_GUILD_ID=1,
            ProviderAttemptCounter=bot.ProviderAttemptCounter,
            BNL01_PACKET_OWNED_SYSTEM_PROMPT="test system",
            _generate_gemini_content_with_fallback=concept,
            _extract_text_and_tokens=lambda response: (response, 0))
        target = Path(self.fixture.db).parent / ("preview-" + stage)
        with mock.patch.object(art, "build_source_packet", return_value=packet), \
             mock.patch.object(art, "build_source_packet_between", side_effect=lambda *_a, **_k: self.fixture.packet()), \
             mock.patch.object(art, "generate_private_image", side_effect=image) as image_mock:
            with self.assertRaisesRegex(ValueError, "art_moment_sources_changed"):
                art.prepare_private_preview(fake, str(target), generate=True)
        self.assertEqual(image_mock.call_count, int(stage == "image"))
        self.assertFalse((target / "bnl-own-art.png").exists())
        receipt = json.loads((target / "receipt.json").read_text())
        self.assertEqual(receipt["status"], "preview_failed")
        self.assertIn(self.fixture.mid, receipt["momentSourceVersions"])

    def test_private_preview_rechecks_after_concept(self):
        self.assert_private_preview_rechecks("concept")

    def test_private_preview_rechecks_after_image(self):
        self.assert_private_preview_rechecks("image")

    def test_private_preview_requires_original_moment_provenance(self):
        packet = self.fixture.packet()
        _, refs = art.build_own_art_brief(packet)
        packet["privateSharedSourceProvenance"] = []
        with self.assertRaisesRegex(ValueError, "art_moment_sources_missing"):
            art._preview_moment_sources(packet, refs)
