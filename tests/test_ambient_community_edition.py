"""Community editions use the real Ambient owner with isolated source data."""
import asyncio
from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
import json
import os
import sqlite3
from types import SimpleNamespace
import unittest
from unittest import mock

import test_ambient_show_context as fixtures
import bnl_ambient_art as art
import bnl_ambient_edition as edition
import bnl_ambient_edition_sources as sources
from bnl_journal_source_store import backfill_legacy_sources


bot = fixtures.bot
REAL_GET = bot.get_gemini_response
LEGACY_TEXT = "The room carries an unexpected rhythm through the evening."


class CommunityEditionIntegrationTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.fixture = fixtures.AmbientShowContextTests()
        self.fixture.setUp()
        self.stack = self.fixture.stack
        self.addCleanup(self.stack.close)
        self.stack.enter_context(mock.patch.dict(os.environ, {
            "BNL_AMBIENT_COMMUNITY_EDITION_ENABLED": "true",
            "BNL_OWN_ART_ENABLED": "false",
        }))
        self.stack.enter_context(mock.patch.object(bot, "_ambient_post_locks", {}, create=True))
        self.stack.enter_context(mock.patch.object(bot, "_journal_website_base_url", return_value="https://site.test"))
        self.publications = self.stack.enter_context(mock.patch.object(
            bot, "_build_publication_prompt_source_basis", return_value=None))
        self.stack.enter_context(mock.patch.object(
            bot, "_refresh_publication_prompt_source_basis", side_effect=lambda basis: (basis, False)))
        self.execute("UPDATE conversations SET timestamp=?", (self.stamp(minutes=5),))
        self.archive()
        self.provider = self.fixture.provider
        self.provider.side_effect = self.response

    def execute(self, sql, args=()):
        with sqlite3.connect(bot.DB_FILE) as conn:
            return conn.execute(sql, args).fetchall()

    def stamp(self, **delta):
        return (self.fixture.now - timedelta(**delta)).astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

    def archive(self):
        backfill_legacy_sources(bot.DB_FILE, 42)

    def material(self, prompt):
        encoded = prompt.split("Eligible material:\n", 1)[1].split("\nRecent Ambient editions", 1)[0]
        return json.loads(encoded)

    def response(self, prompt, *args, **kwargs):
        items = [item for group in self.material(prompt).values() for item in group]
        selected = next((item for item in items if item["kind"] == "conversation"), None)
        if selected is None:
            return '{"action":"skip"}'
        subject = next(iter(selected.get("subject_refs", ())), None)
        person = "[[person:" + subject + "]]" if subject else "A community member"
        paragraph = (person + " noticed an unexpected rhythm taking shape. That is worth listening to: "
                 "the conversation makes room for an unusual sound without demanding a polished release. "
                 "I like that kind of opening. A small observation can give the next conversation "
                 "somewhere interesting to go, and this one deserves more than a passing glance.")
        paragraphs = [{"text": paragraph, "sourceRefs": [selected["ref"]],
                    "publicationRefs": [], "contextRefs": [],
                    "subjectRefs": [subject] if subject else []}]
        journal = next((item for item in items if item["kind"] == "published_journal"), None)
        if journal:
            paragraphs.append({"text": "A newly published Journal revisits an earlier discussion about ceramic receivers.",
                               "sourceRefs": [], "publicationRefs": [journal["ref"]],
                               "contextRefs": [], "subjectRefs": []})
        return json.dumps({"action": "post", "headline": "A rhythm worth following",
                           "paragraphs": paragraphs, "art": None})

    def add_message(self, text, when, *, policy="public_home", user_id=8):
        self.execute("INSERT INTO conversations(user_id,user_name,guild_id,channel_id,channel_name,channel_policy,role,content,timestamp) "
                     "VALUES(?,'Another Member',42,100,'barcode-bot',?,'user',?,?)",
                     (user_id, policy, text, when))

    def add_publication(self, *, body="Ceramic receivers caught a strange signal."):
        publication = SimpleNamespace(
            entry_id="journal_daily_2026-09-10_fixture", revision=1,
            title="Ceramic Receivers", excerpt="An earlier discussion revisited.",
            sections_json=json.dumps([{"heading": "Music", "body": body}]),
            published_at=self.stamp(minutes=10), created_at=self.stamp(minutes=20),
            source_window_start=self.stamp(days=2), source_window_end=self.stamp(days=1))
        basis = SimpleNamespace(publications=(publication,))
        self.publications.side_effect = lambda **kwargs: basis if kwargs["source_kind"] == "journal" else None
        return basis

    async def test_default_off_uses_the_legacy_owner_without_edition_source_reads(self):
        with mock.patch.dict(os.environ, {"BNL_AMBIENT_COMMUNITY_EDITION_ENABLED": ""}), \
                mock.patch.object(sources, "build_context") as reader:
            self.provider.side_effect = None
            self.provider.return_value = LEGACY_TEXT
            self.assertEqual(await bot.generate_dynamic_ambient(42, 100), LEGACY_TEXT)
        reader.assert_not_called()
        self.assertIn("at most 280 characters", self.provider.call_args.args[0])

    async def test_other_guild_cannot_enable_the_primary_edition(self):
        with mock.patch.object(edition, "generate", new=mock.AsyncMock()) as generate:
            self.provider.side_effect = None
            self.provider.return_value = LEGACY_TEXT
            self.assertEqual(await bot.generate_dynamic_ambient(43, 100), LEGACY_TEXT)
        generate.assert_not_awaited()

    async def test_real_provider_wrapper_preserves_rich_envelope_without_voice_rewrite(self):
        self.add_message("WITHHELD_EDITION_MARKER", self.stamp(minutes=4), policy="sealed_test")
        self.archive()
        async def boundary(contents, route, **kwargs):
            return SimpleNamespace(success=True, text=self.response(contents))
        for art_available in (False, True):
            with self.subTest(art_available=art_available), \
                    mock.patch.object(art, "available", return_value=art_available), \
                    mock.patch.object(bot, "get_gemini_response", new=REAL_GET), \
                    mock.patch.object(bot, "check_quota_availability", return_value=True), \
                    mock.patch.object(bot, "_generate_gemini_content_result_async", new=mock.AsyncMock(side_effect=boundary)) as provider, \
                    mock.patch.object(bot, "_generate_gemini_content_with_fallback_async", new=mock.AsyncMock()) as rewrite:
                basis = {}
                result = await bot.generate_dynamic_ambient(42, 100, source_basis_out=basis)
                self.assertGreater(len(result), 280)
                self.assertEqual(basis["edition"]["headline"], "A rhythm worth following")
                provider.assert_awaited_once()
                contents, route = provider.call_args.args
                self.assertEqual(route, "ambient_generation")
                self.assertTrue(contents.startswith(bot.BNL01_PACKET_OWNED_SYSTEM_PROMPT))
                article_context = contents.split("Eligible material:\n", 1)[0]
                self.assertIn(edition.render_prompt_canon_block(), article_context)
                self.assertIn(edition.render_ecosystem_lore_block(include_restricted=False), article_context)
                self.assertEqual(contents.count(edition.render_prompt_canon_block()), 1)
                self.assertEqual(contents.count(edition.render_ecosystem_lore_block(include_restricted=False)), 1)
                self.assertNotIn("9 Bit", article_context)
                self.assertIn("A new rhythm is forming in the room.", contents)
                for private in ("WITHHELD_EDITION_MARKER", "PRIVATE PAYMENT VALUE", "PRIVATE OPERATOR VALUE",
                                "privateSharedSourceProvenance", "_discord_basis"):
                    self.assertNotIn(private, contents)
                self.assertEqual("art_context" in basis, art_available)
                if art_available:
                    self.assertEqual(basis["art_context"]["basis"]["entryKind"], "daily")
                    self.assertTrue(any(ref.startswith("reflection:canon:")
                                        for ref in basis["art_context"]["basis"]["sources"]))
                rewrite.assert_not_awaited()

    async def test_real_provider_failure_never_becomes_an_edition_or_voice_retry(self):
        for art_available in (False, True):
            with self.subTest(art_available=art_available), \
                    mock.patch.object(art, "available", return_value=art_available), \
                    mock.patch.object(bot, "get_gemini_response", new=REAL_GET), \
                    mock.patch.object(bot, "check_quota_availability", return_value=True), \
                    mock.patch.object(bot, "_generate_gemini_content_result_async", new=mock.AsyncMock(
                        return_value=SimpleNamespace(success=False, error_category="fixture_unavailable"))) as provider, \
                    mock.patch.object(bot, "_generate_gemini_content_with_fallback_async", new=mock.AsyncMock()) as rewrite:
                basis = {}
                self.assertEqual(await bot.generate_dynamic_ambient(42, 100, source_basis_out=basis), "")
                self.assertEqual(basis, {})
                provider.assert_awaited_once()
                rewrite.assert_not_awaited()

    async def test_real_day_source_adapter_excludes_old_future_and_private_independent_of_art(self):
        self.add_message("OLD_DAY_MARKER", self.stamp(days=2))
        self.add_message("FUTURE_MARKER", self.stamp(minutes=-5))
        self.add_message("SEALED_MARKER", self.stamp(minutes=4), policy="sealed_test")
        self.add_message("ELIGIBLE_DAY_MARKER", self.stamp(hours=20))
        self.archive()
        basis = {}
        result = await bot.generate_dynamic_ambient(42, 100, source_basis_out=basis)
        self.assertTrue(result)
        prompt = self.provider.call_args.args[0]
        self.assertIn("ELIGIBLE_DAY_MARKER", prompt)
        for excluded in ("FUTURE_MARKER", "SEALED_MARKER"):
            self.assertNotIn(excluded, prompt)
        for item in basis["edition_context"]["items"]:
            if "OLD_DAY_MARKER" in item["text"]:
                self.assertEqual(item["scope"], "historical_context")
        self.assertFalse(any("OLD_DAY_MARKER" in item["text"]
                             for item in basis["edition_context"]["items"]
                             if item["scope"] == "window_activity"))
        self.assertIn("edition_context", basis)
        self.assertNotIn("art_context", basis)

    async def test_publication_keeps_actual_link_and_separate_reported_window(self):
        publication_basis = self.add_publication()
        basis = {}
        result = await bot.generate_dynamic_ambient(42, 100, source_basis_out=basis)
        self.assertIn("https://site.test/journal/journal_daily_2026-09-10_fixture", result)
        item = next(item for item in basis["edition_context"]["items"] if item["kind"] == "published_journal")
        self.assertEqual(item["occurred_at"], "")
        self.assertEqual(item["reported_window_end"], self.stamp(days=1))
        self.assertIn(publication_basis, basis["edition_context"]["_publication_bases"])
        with mock.patch.object(bot, "_refresh_publication_prompt_source_basis", return_value=(SimpleNamespace(publications=()), True)):
            self.assertFalse(await bot.revalidate_ambient_sources(42, basis, stage="before_send"))

    async def test_real_wrapper_keeps_journal_expression_separate_and_repairs_wrong_role_once(self):
        original_text = ("My new project is an electronic collaboration about an optimistic future. "
                         "I have not shared any audio here.")
        journal_text = "The member's booming acoustic rhythm filled the room while everyone cheered."
        self.add_message(original_text, self.stamp(minutes=3))
        self.archive()
        self.add_publication(body=journal_text)
        captured = []
        accepted = {}

        async def boundary(contents, route, **kwargs):
            material = self.material(contents)
            original = next(item for item in material["original_contributions"] if item.get("text") == original_text)
            journal = next(item for item in material["bnl_expressions"] if item["kind"] == "published_journal")
            captured.append((material, route))
            if len(captured) == 1:
                # A publication ref in the original-fact lane must be rejected
                # before any transport, regardless of its plausible prose.
                value = {"action": "post", "paragraphs": [{
                    "text": journal_text, "sourceRefs": [journal["ref"]],
                    "publicationRefs": [], "contextRefs": [], "subjectRefs": [],
                }], "art": None}
            else:
                subject = original["subject_refs"][0]
                marker = "[[person:" + subject + "]]"
                text = ("That is an optimistic future I can get behind. " + marker +
                        " described an electronic collaboration; the audio itself has not been shared here. "
                        "My Journal took a different imaginative angle, and it is linked below.")
                accepted["text"] = text.replace(marker, original["subject_labels"][subject])
                value = {"action": "post", "paragraphs": [{
                    "text": text, "sourceRefs": [original["ref"]],
                    "publicationRefs": [journal["ref"]], "contextRefs": [], "subjectRefs": [subject],
                }], "art": None}
            return SimpleNamespace(success=True, text=json.dumps(value))

        channel, _guild, _ = self.scheduler(fetch_effect=lambda user_id: SimpleNamespace(
            id=user_id, bot=False, guild=SimpleNamespace(id=42)))
        with mock.patch.object(bot, "get_gemini_response", new=REAL_GET), \
                mock.patch.object(bot, "check_quota_availability", return_value=True), \
                mock.patch.object(bot, "_generate_gemini_content_result_async", new=mock.AsyncMock(side_effect=boundary)) as provider, \
                mock.patch.object(bot, "_generate_gemini_content_with_fallback_async", new=mock.AsyncMock()) as rewrite:
            await bot.ambient_message_task.coro()

        self.assertEqual(provider.await_count, 2)
        self.assertEqual([route for _, route in captured], [
            "ambient_generation", "ambient_generation.conversation_grounding_regeneration"])
        for material, _ in captured:
            self.assertEqual(next(iter(material)), "original_contributions")
            original = next(item for item in material["original_contributions"] if item.get("text") == original_text)
            journal = next(item for item in material["bnl_expressions"] if item["kind"] == "published_journal")
            self.assertEqual(original["evidence_role"], "original_contribution")
            self.assertEqual(journal["evidence_role"], "bnl_expression")
            self.assertNotIn("text", journal)
            self.assertIn(journal_text, journal["expression_text"])
            self.assertFalse(any(item["ref"] == journal["ref"] for item in material["recorded_events"]))
        channel.send.assert_awaited_once()
        embed = channel.send.call_args.kwargs["embed"]
        self.assertNotIn("title", embed.to_dict())
        self.assertEqual(embed.description, accepted["text"] +
                         "\n[Ceramic Receivers](<https://site.test/journal/journal_daily_2026-09-10_fixture>)")
        self.assertNotIn(journal_text, embed.description)
        self.assertEqual(channel.send.call_args.kwargs["allowed_mentions"].to_dict(), {"users": [8], "parse": []})
        self.assertEqual(self.execute("SELECT COUNT(*) FROM ambient_log")[0][0], 1)
        rewrite.assert_not_awaited()

    async def test_withdrawal_during_generation_discards_without_repair(self):
        def withdraw(prompt, *args, **kwargs):
            answer = self.response(prompt)
            self.execute("UPDATE conversations SET channel_policy='sealed_test'")
            return answer
        self.provider.side_effect = withdraw
        self.assertEqual(await bot.generate_dynamic_ambient(42, 100), "")
        self.provider.assert_awaited_once()

    async def test_art_enabled_real_provider_discards_a_withdrawn_original_without_repair(self):
        async def withdraw(contents, route, **kwargs):
            response = self.response(contents)
            self.execute("UPDATE conversations SET channel_policy='sealed_test'")
            return SimpleNamespace(success=True, text=response)
        with mock.patch.object(art, "available", return_value=True), \
                mock.patch.object(bot, "get_gemini_response", new=REAL_GET), \
                mock.patch.object(bot, "check_quota_availability", return_value=True), \
                mock.patch.object(bot, "_generate_gemini_content_result_async", new=mock.AsyncMock(side_effect=withdraw)) as provider, \
                mock.patch.object(bot, "_generate_gemini_content_with_fallback_async", new=mock.AsyncMock()) as rewrite:
            basis = {}
            self.assertEqual(await bot.generate_dynamic_ambient(42, 100, source_basis_out=basis), "")
            self.assertEqual(basis, {})
            provider.assert_awaited_once()
            rewrite.assert_not_awaited()

    async def test_silence_is_one_decision_and_not_an_error(self):
        self.provider.side_effect = None
        self.provider.return_value = '{"action":"skip"}'
        basis = {}
        self.assertEqual(await bot.generate_dynamic_ambient(42, 100, source_basis_out=basis), "")
        self.assertTrue(basis["declined"])
        self.provider.assert_awaited_once()

    def scheduler(self, *, send_effect=None, fetch_effect=None):
        self.execute("INSERT INTO guild_configs(guild_id,active_channel_id,next_ambient_message_at) VALUES(42,100,?)",
                     ((self.fixture.now - timedelta(minutes=1)).isoformat(),))
        channel = mock.Mock(id=100)
        channel.send = mock.AsyncMock(side_effect=send_effect, return_value=SimpleNamespace(id=7654))
        guild = SimpleNamespace(id=42)
        member = SimpleNamespace(id=7, bot=False, guild=guild)
        guild.fetch_member = mock.AsyncMock(side_effect=fetch_effect, return_value=member)
        stack = ExitStack()
        stack.enter_context(mock.patch.object(bot.client, "get_channel", return_value=channel))
        stack.enter_context(mock.patch.object(bot.client, "get_guild", return_value=guild))
        stack.enter_context(mock.patch.object(bot, "resolve_channel_policy", return_value="public_home"))
        stack.enter_context(mock.patch.object(bot, "is_community_image_channel", return_value=False))
        stack.enter_context(mock.patch.object(bot, "process_due_occasion_for_guild", new=mock.AsyncMock(return_value={"status": "idle"})))
        stack.enter_context(mock.patch.object(bot, "is_high_activity_day", return_value=True))
        dormant = stack.enter_context(mock.patch.object(bot, "prepare_dormant_echo_canary", new=mock.AsyncMock(return_value={"status": "idle"})))
        self.addCleanup(stack.close)
        return channel, guild, dormant

    async def test_scheduler_delivers_one_rich_embed_and_exact_verified_mention_allowlist(self):
        def send(content, **kwargs):
            scheduled = self.execute("SELECT next_ambient_message_at FROM guild_configs WHERE guild_id=42")[0][0]
            self.assertGreater(datetime.fromisoformat(scheduled), self.fixture.now)
            return SimpleNamespace(id=7654)
        channel, guild, dormant = self.scheduler(send_effect=send)
        await bot.ambient_message_task.coro()
        channel.send.assert_awaited_once()
        self.assertEqual(channel.send.call_args.args[0], "Featuring: <@7>")
        kwargs = channel.send.call_args.kwargs
        self.assertGreater(len(kwargs["embed"].description), 280)
        self.assertEqual(kwargs["embed"].title, "A rhythm worth following")
        self.assertEqual(kwargs["allowed_mentions"].to_dict(), {"users": [7], "parse": []})
        self.assertNotIn("file", kwargs)
        guild.fetch_member.assert_awaited_once_with(7)
        dormant.assert_not_awaited()
        self.assertEqual(self.execute("SELECT COUNT(*) FROM ambient_log")[0][0], 1)
        await bot.ambient_message_task.coro()
        self.assertEqual(channel.send.await_count, 1)
        self.assertEqual(self.provider.await_count, 1)

    async def test_delivery_preserves_a_personal_interpretation_before_the_factual_update(self):
        opening = "That unfinished rhythm has somewhere to go. I am leaving the door open. "
        def reflect(prompt, *args, **kwargs):
            value = json.loads(self.response(prompt))
            value["paragraphs"][0]["text"] = opening + value["paragraphs"][0]["text"]
            return json.dumps(value)
        self.provider.side_effect = reflect
        channel, _guild, _ = self.scheduler()
        await bot.ambient_message_task.coro()
        channel.send.assert_awaited_once()
        description = channel.send.call_args.kwargs["embed"].description
        self.assertTrue(description.startswith(opening + "Test Member noticed"))
        self.assertEqual(channel.send.call_args.kwargs["allowed_mentions"].to_dict(), {"users": [7], "parse": []})

    async def test_membership_lookup_withdrawal_is_caught_by_final_source_fence(self):
        def withdraw(_user_id):
            self.execute("UPDATE conversations SET channel_policy='sealed_test'")
            return SimpleNamespace(id=7, bot=False, guild=SimpleNamespace(id=42))
        channel, guild, _ = self.scheduler(fetch_effect=withdraw)
        await bot.ambient_message_task.coro()
        guild.fetch_member.assert_awaited_once()
        channel.send.assert_not_awaited()
        self.assertEqual(self.execute("SELECT COUNT(*) FROM ambient_log")[0][0], 0)

    async def test_budget_block_never_reaches_provider_or_discord(self):
        channel, _guild, _ = self.scheduler()
        with mock.patch.object(bot, "get_gemini_response", new=REAL_GET), \
                mock.patch.object(bot, "check_quota_availability", return_value=False), \
                mock.patch.object(bot, "_generate_gemini_content_result_async", new=mock.AsyncMock()) as provider:
            await bot.ambient_message_task.coro()
        provider.assert_not_awaited()
        channel.send.assert_not_awaited()

    async def test_unavailable_source_database_never_generates_or_sends(self):
        channel, _guild, _ = self.scheduler()
        with mock.patch.object(sources, "build_context", side_effect=sqlite3.OperationalError("fixture source unavailable")):
            await bot.ambient_message_task.coro()
        self.provider.assert_not_awaited()
        channel.send.assert_not_awaited()

    async def test_uncertain_transport_is_not_retried_on_the_next_tick(self):
        channel, _guild, _ = self.scheduler(send_effect=TimeoutError("fixture uncertain transport"))
        await bot.ambient_message_task.coro()
        await bot.ambient_message_task.coro()
        channel.send.assert_awaited_once()
        self.provider.assert_awaited_once()
        self.assertEqual(self.execute("SELECT COUNT(*) FROM ambient_log")[0][0], 0)

    async def test_process_cancellation_after_acceptance_keeps_durable_next_day_reservation(self):
        channel, _guild, _ = self.scheduler(send_effect=asyncio.CancelledError())
        with self.assertRaises(asyncio.CancelledError):
            await bot.ambient_message_task.coro()
        channel.send.side_effect = None
        await bot.ambient_message_task.coro()
        channel.send.assert_awaited_once()
        self.provider.assert_awaited_once()
        self.assertEqual(self.execute("SELECT COUNT(*) FROM ambient_log")[0][0], 0)

    async def test_high_activity_cap_does_not_authorize_a_second_edition(self):
        channel, _guild, _ = self.scheduler()
        await bot.ambient_message_task.coro()
        decision = bot.ambient_capacity_decision(42, 100, "ambient", now_pacific=self.fixture.now)
        self.assertEqual(decision["cap"], 2)
        self.assertFalse(decision["allowed"])
        self.assertEqual(decision["reason"], "community_edition_already_posted_today")
        self.execute("UPDATE guild_configs SET next_ambient_message_at=? WHERE guild_id=42",
                     ((self.fixture.now - timedelta(minutes=1)).isoformat(),))
        self.fixture.now += timedelta(hours=5)
        await bot.ambient_message_task.coro()
        channel.send.assert_awaited_once()
        self.provider.assert_awaited_once()

    async def test_old_day_art_never_remains_in_a_prebuilt_edition_attachment(self):
        channel, _guild, _ = self.scheduler()
        old_art = {"metadata": {"artId": "bnl-art-2026-09-10"}}
        with mock.patch.object(art, "prepare", new=mock.AsyncMock(return_value=old_art)), \
                mock.patch.object(art, "record"), mock.patch.object(art, "discord_file") as file:
            await bot.ambient_message_task.coro()
        channel.send.assert_awaited_once()
        file.assert_not_called()
        self.assertNotIn("file", channel.send.call_args.kwargs)
        self.assertIsNone(channel.send.call_args.kwargs["embed"].image.url)

    async def test_midnight_during_member_confirmation_drops_the_previous_day_image(self):
        self.fixture.now = self.fixture.now.replace(hour=23, minute=59, second=58)
        def after_midnight(_user_id):
            self.fixture.now += timedelta(seconds=5)
            return SimpleNamespace(id=7, bot=False, guild=SimpleNamespace(id=42))
        channel, _guild, _ = self.scheduler(fetch_effect=after_midnight)
        image = {"metadata": {"artId": "bnl-art-2026-09-11"}}
        attachment = SimpleNamespace(filename="fixture.png", close=mock.Mock())
        with mock.patch.object(art, "prepare", new=mock.AsyncMock(return_value=image)), \
                mock.patch.object(art, "record"), \
                mock.patch.object(art, "discord_file", return_value=attachment), \
                mock.patch.object(art, "publish_website") as website:
            await bot.ambient_message_task.coro()
        channel.send.assert_awaited_once()
        self.assertNotIn("file", channel.send.call_args.kwargs)
        self.assertIsNone(channel.send.call_args.kwargs["embed"].image.url)
        attachment.close.assert_called_once()
        website.assert_not_called()


if __name__ == "__main__":
    unittest.main()
