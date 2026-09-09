"""Requested show dates survive real source selection and batch delivery.

The two-show fixture retains different authored comments for each date. Only
website transport and Gemini/Discord transport are replaced; date selection,
SQLite evidence readers, batching decisions and source refresh remain real.
Fixed supported replies prove delivery, not live model factuality.
"""

import json
import os
import sqlite3
import unittest
from datetime import date
from pathlib import Path
from time import time
from unittest import mock

import test_public_network_knowledge as network_fixture
import test_tiktok_show_evidence_ledger as show_fixture
from bnl_journal_source_store import record_source_event
from bnl_tiktok_live_context import LiveContextSnapshotWriter
from tests import test_tiktok_live_context_bridge as live_fixture


bot = network_fixture.bnl01_bot
REAL_READ_MODEL_CONTEXT = bot.maybe_build_bnl_read_model_context
REQUEST = (
    "BNL, what stood out in the TikTok chat during the August 28, 2026 show? "
    "Give me a couple of actual comments and who said them."
)
ISO_REQUEST = REQUEST.replace("August 28, 2026", "2026-08-28")
AUGUST_COMMENT = "the green visuals during this song are wild."
SEPTEMBER_COMMENT = "The amber lanterns are bright tonight."
ANSWER = (
    'Alex said, "BNL, the green visuals during this song are wild." '
    'Neon Fox said, "The green visuals made that moment hit."'
)
# Exercise an admitted question here; reader tests retain the review's literal
# unaddressed "Compare ..." wording without changing passive-batch admission.
COMPARE_REQUEST = "How does TikTok chat compare across the August 28, 2026 and September 4, 2026 shows?"
COMPARE_ANSWER = (
    "Alex mentioned green visuals on August 28; "
    "Test September mentioned amber lanterns on September 4."
)


class RequestedShowDateDeliveryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.runtime = network_fixture.PublicNetworkKnowledgeTests()
        await self.runtime.asyncSetUp()
        self.addAsyncCleanup(self.runtime.asyncTearDown)
        self.runtime._seed_finalized_show()
        self.stack = self.runtime.stack
        self.stack.enter_context(mock.patch.dict(os.environ, {
            "BNL_CONVERSATION_ORCHESTRATION_INFLUENCE_ENABLED": "false",
            "BNL_CONVERSATION_ORCHESTRATION_SEALED_CANARY_ENABLED": "false",
        }))
        self.stack.enter_context(mock.patch.object(bot, "BNL_PRIMARY_GUILD_ID", 77))
        september_show = json.loads(
            json.dumps(show_fixture.archived_show())
            .replace("show-attendance-1", "show-attendance-september")
            .replace("2026-08-28", "2026-09-04")
            .replace("2026-08-29", "2026-09-05")
        )
        event = show_fixture.durable_events()[0]
        recorded = record_source_event(
            bot.DB_FILE, guild_id=77, source_kind="tiktok_live_chat",
            source_key="september-authored-comment",
            occurred_at_ms=event["occurred_at_ms"] + 7 * 24 * 60 * 60 * 1000,
            raw_text=SEPTEMBER_COMMENT, sanitized_summary=SEPTEMBER_COMMENT,
            channel_policy="public_context", subject_ref="tiktok_handle:test.september",
            private_display_name="Test September", public_usable=True,
            metadata={"eventType": "comment", "handle": "test.september"},
        )
        self.assertTrue(recorded.ok)
        self.read_model = show_fixture.authorized_read_model({
            "currentShow": None,
            "latestShow": september_show,
            "shows": [show_fixture.archived_show()],
        })
        self.read_model["sections"]["queue"] = {
            "available": True, "accessScope": "public", "revision": 55,
            "session": {
                "title": "Current Broadcast", "showDate": "2026-09-04",
                "purpose": "live_broadcast", "status": "open",
                "queueOpen": True, "broadcastPhase": "live",
            },
            "status": {"activeCount": 2, "capacity": 44},
            "nowPlaying": {
                "id": "current-track", "submittedArtistName": "Test Current Artist",
                "submittedSongTitle": "Present Signal",
            },
            "upNext": {
                "id": "next-track", "submittedArtistName": "Test Next Artist",
                "submittedSongTitle": "Next Signal", "queuePosition": 1,
            },
        }
        synced = show_fixture.sync_tiktok_show_evidence_ledgers(
            bot.DB_FILE, guild_id=77, read_model=self.read_model,
            artist_identity_index=show_fixture.artist_index(),
            environ=show_fixture.ENABLED_QUEUE_ENV,
        )
        self.assertEqual(synced["showsSeen"], 2)
        self.fetch = self.stack.enter_context(mock.patch.object(
            bot, "fetch_bnl_read_model", return_value=self.read_model,
        ))
        # Restore the actual adapter hidden by the generic network fixture.
        self.stack.enter_context(mock.patch.object(
            bot, "maybe_build_bnl_read_model_context", new=REAL_READ_MODEL_CONTEXT,
        ))

    def _read(self, request=REQUEST, policy="sealed_test", website_override=None):
        website = (
            REAL_READ_MODEL_CONTEXT(request, policy)
            if website_override is None else website_override
        )
        selection = {}
        episode = network_fixture.REAL_SHOW_CONTEXT_FOR_TURN(
            guild_id=77, user_text=request, subject_user_id=42,
            website_read_model_context=website, selection_out=selection,
        )
        basis = bot.build_finalized_show_prompt_source_basis(
            episode, guild_id=77, selection=selection,
        )
        return website, episode, basis

    def _assert_august_source(self, episode, basis):
        self.assertIsNotNone(basis)
        self.assertIn("on 2026-08-28;", episode)
        self.assertIn(AUGUST_COMMENT, episode)
        self.assertNotIn("on 2026-09-04;", episode)
        self.assertNotIn(SEPTEMBER_COMMENT, episode)
        self.assertTrue(basis.authored_excerpts)
        self.assertTrue(any(
            AUGUST_COMMENT in excerpt.source_text
            and excerpt.speaker_label == "Alex (@alex.signal)"
            for excerpt in basis.authored_excerpts
        ))
        self.assertFalse(any(
            SEPTEMBER_COMMENT in excerpt.source_text
            for excerpt in basis.authored_excerpts
        ))

    async def test_natural_and_iso_dates_select_same_authored_show_in_both_surfaces(self):
        expected_refs = None
        for policy in ("public_home", "sealed_test"):
            for request in (REQUEST, ISO_REQUEST):
                with self.subTest(policy=policy, request=request):
                    website, episode, basis = self._read(request, policy)
                    self.assertIn("showDate=2026-08-28", website)
                    self.assertIn(AUGUST_COMMENT, website)
                    self.assertNotIn(SEPTEMBER_COMMENT, website)
                    self.assertNotIn("showDate=2026-09-04", website)
                    self.assertNotIn("Present Signal", website)
                    self._assert_august_source(episode, basis)
                    refs = tuple((item.show_key, item.event_id) for item in basis.authored_excerpts)
                    if expected_refs is None:
                        expected_refs = refs
                    self.assertEqual(refs, expected_refs)

    async def test_explicit_request_survives_conflicting_website_show_date(self):
        conflicting_website = (
            "Website public read model context:\naccessScope=public\n"
            "- Session=Current Broadcast, showDate=2026-09-04\n"
            "Durable TikTok show analysis context:\n"
            "- Show=Current Broadcast; showDate=2026-09-04; selectedFrom=latestShow.\n"
        )
        for request in (REQUEST, ISO_REQUEST):
            with self.subTest(request=request):
                _website, episode, basis = self._read(
                    request, website_override=conflicting_website,
                )
                self._assert_august_source(episode, basis)

    async def test_provider_await_refresh_keeps_the_requested_show(self):
        _website, episode, basis = self._read()
        self._assert_august_source(episode, basis)
        refreshed, changed = bot.refresh_prompt_source_basis(basis)
        self.assertFalse(changed)
        self.assertEqual(refreshed.show_keys, basis.show_keys)
        self.assertEqual(refreshed.authored_excerpts, basis.authored_excerpts)
        self._assert_august_source(refreshed.rendered_context, refreshed)
        self.assertEqual(bot.prompt_source_basis_failure((refreshed,)), "")

    async def test_two_dates_reach_archive_ledger_and_source_refresh_together(self):
        for policy in ("public_home", "sealed_test"):
            with self.subTest(policy=policy):
                website, episode, basis = self._read(COMPARE_REQUEST, policy)
                self.assertIsNotNone(basis)
                for text in (AUGUST_COMMENT, SEPTEMBER_COMMENT):
                    self.assertIn(text, website)
                    self.assertIn(text, episode)
                    self.assertTrue(any(text in excerpt.source_text for excerpt in basis.authored_excerpts))
                self.assertNotIn("Present Signal", website)
                self.assertEqual(set(basis.show_keys), {"show-attendance-1", "show-attendance-september"})
                refreshed, changed = bot.refresh_prompt_source_basis(basis)
                self.assertFalse(changed)
                self.assertEqual(refreshed.authored_excerpts, basis.authored_excerpts)
                self.assertEqual(bot.prompt_source_basis_failure((refreshed,)), "")

    async def test_coordinated_show_dates_do_not_require_a_platform_word(self):
        for request in (
            "BNL, compare the August 28, 2026 and September 4, 2026 shows",
            "BNL, compare the shows from August 28, 2026 and September 4, 2026",
            "BNL, compare the shows on August 28, 2026 and on September 4, 2026",
            "BNL, compare the shows from 2026-08-28 and from 2026-09-04",
        ):
            with self.subTest(request=request):
                website, episode, basis = self._read(request)
                for text in (AUGUST_COMMENT, SEPTEMBER_COMMENT):
                    self.assertIn(text, website)
                    self.assertIn(text, episode)
                self.assertEqual(len(basis.show_keys), 2)

    async def test_unrelated_date_cannot_select_or_refresh_another_show(self):
        for request in (
            "Tell me about the August 28, 2026 show; my appointment is September 4, 2026.",
            "My appointment is September 4, 2026; tell me about the August 28, 2026 show.",
        ):
            with self.subTest(request=request):
                website, episode, basis = self._read(request)
                self.assertIn(AUGUST_COMMENT, website)
                self.assertNotIn(SEPTEMBER_COMMENT, website)
                self._assert_august_source(episode, basis)
                refreshed, changed = bot.refresh_prompt_source_basis(basis)
                self.assertFalse(changed)
                self._assert_august_source(refreshed.rendered_context, refreshed)

    async def test_removed_second_show_refresh_retains_only_the_valid_source(self):
        _website, _episode, basis = self._read(COMPARE_REQUEST)
        self.assertEqual(len(basis.show_keys), 2)
        with sqlite3.connect(bot.DB_FILE) as conn:
            conn.execute(
                "DELETE FROM %s WHERE guild_id=? AND show_key=?" % show_fixture.TIKTOK_SHOW_EVIDENCE_TABLE,
                (77, "show-attendance-september"),
            )
        refreshed, changed = bot.refresh_prompt_source_basis(basis)
        self.assertTrue(changed)
        self.assertIn(AUGUST_COMMENT, refreshed.rendered_context)
        self.assertNotIn(SEPTEMBER_COMMENT, refreshed.rendered_context)
        self.assertTrue(all(excerpt.show_key == "show-attendance-1" for excerpt in refreshed.authored_excerpts))

    async def test_missing_compared_date_does_not_load_an_unrequested_show(self):
        request = COMPARE_REQUEST.replace("August 28, 2026", "August 14, 2026")
        website, episode, basis = self._read(request)
        self.assertIn("Requested show date: 2026-08-14", website)
        self.assertIn("no public show timeline", website)
        self.assertNotIn(AUGUST_COMMENT, website)
        self.assertNotIn(AUGUST_COMMENT, episode)
        self.assertIn(SEPTEMBER_COMMENT, website)
        self.assertIn(SEPTEMBER_COMMENT, episode)
        self.assertEqual(basis.show_keys, ("show-attendance-september",))

    async def test_missing_earlier_date_does_not_consume_an_available_show_slot(self):
        for dates in (
            "August 14, 2026, August 28, 2026 and September 4, 2026",
            "August 28, 2026, August 14, 2026 and September 4, 2026",
        ):
            for policy in ("public_home", "sealed_test"):
                with self.subTest(dates=dates, policy=policy):
                    request = "How does TikTok chat compare across the " + dates + " shows?"
                    website, episode, basis = self._read(request, policy)
                    self.assertIn("Requested show date: 2026-08-14", website)
                    self.assertIn("no public show timeline", website)
                    for text in (AUGUST_COMMENT, SEPTEMBER_COMMENT):
                        self.assertIn(text, website)
                        self.assertIn(text, episode)
                    self.assertEqual(set(basis.show_keys), {
                        "show-attendance-1", "show-attendance-september",
                    })

    async def test_available_requested_dates_still_stop_at_the_existing_show_limit(self):
        third_show = json.loads(
            json.dumps(self.read_model["sections"]["archive"]["latestShow"])
            .replace("show-attendance-september", "show-attendance-third")
            .replace("2026-09-04", "2026-09-11")
            .replace("2026-09-05", "2026-09-12")
        )
        self.read_model["sections"]["archive"]["shows"].append(third_show)
        request = (
            "How does TikTok chat compare across the August 14, 2026, "
            "August 28, 2026, September 4, 2026 and September 11, 2026 shows?"
        )
        website = REAL_READ_MODEL_CONTEXT(request, "public_home")
        self.assertIn(AUGUST_COMMENT, website)
        self.assertIn(SEPTEMBER_COMMENT, website)
        self.assertNotIn("showDate=2026-09-11", website)
        self.assertNotIn("Requested show date: 2026-09-11", website)

    async def test_batch_delivers_both_explicit_show_sources_with_one_call(self):
        for policy in ("public_home", "sealed_test"):
            with self.subTest(policy=policy):
                channel, generation, guard = await self.runtime._batch(
                    policy, request=COMPARE_REQUEST, answer=COMPARE_ANSWER, privileged=False,
                )
                generation.assert_awaited_once()
                self.assertEqual(channel.sent, [COMPARE_ANSWER])
                prompt = generation.await_args.args[0]
                self.assertIn(AUGUST_COMMENT, prompt)
                self.assertIn(SEPTEMBER_COMMENT, prompt)
                bases = [item for item in guard.await_args.kwargs["prompt_source_bases"]
                         if isinstance(item, bot.FinalizedShowPromptSourceBasis)]
                self.assertEqual(len(bases), 1)
                self.assertEqual(set(bases[0].show_keys), {"show-attendance-1", "show-attendance-september"})

    async def test_current_date_correction_wins_over_an_earlier_show_request(self):
        for request in (REQUEST, ISO_REQUEST):
            with self.subTest(request=request):
                selection = {}
                episode = bot.build_tiktok_show_evidence_context(
                    bot.DB_FILE, guild_id=77, user_text=request,
                    subject_user_id=0,
                    selection_user_text=(
                        "Earlier request: Give me a recap of the September 4, 2026 show.\n"
                        "Current correction: " + request
                    ),
                    selection_out=selection,
                )
                basis = bot.build_finalized_show_prompt_source_basis(
                    episode, guild_id=77, selection=selection,
                )
                self._assert_august_source(episode, basis)

    async def test_removed_requested_source_does_not_refresh_to_latest_show(self):
        _website, episode, basis = self._read()
        self._assert_august_source(episode, basis)
        with sqlite3.connect(bot.DB_FILE) as conn:
            conn.executemany(
                "DELETE FROM %s WHERE guild_id=? AND show_key=?"
                % show_fixture.TIKTOK_SHOW_EVIDENCE_TABLE,
                [(77, key) for key in basis.show_keys],
            )
        refreshed, changed = bot.refresh_prompt_source_basis(basis)
        self.assertTrue(changed)
        self.assertEqual(refreshed.rendered_context, "")
        self.assertEqual(refreshed.authored_excerpts, ())

    async def test_missing_requested_show_does_not_substitute_latest_evidence(self):
        for date in ("August 14, 2026", "2026-08-14"):
            request = REQUEST.replace("August 28, 2026", date)
            for policy in ("public_home", "sealed_test"):
                with self.subTest(date=date, policy=policy):
                    website, episode, basis = self._read(request, policy)
                    self.assertNotIn("showDate=2026-09-04", website)
                    self.assertNotIn("showDate=2026-08-28", website)
                    self.assertNotIn(AUGUST_COMMENT, website)
                    self.assertNotIn(SEPTEMBER_COMMENT, website)

                    self.assertEqual(episode, "")
                    self.assertIsNone(basis)

    async def test_current_queue_read_still_uses_current_session(self):
        for policy in ("public_home", "sealed_test"):
            with self.subTest(policy=policy):
                website = REAL_READ_MODEL_CONTEXT(
                    "BNL, what is playing now, and is the queue open right now?", policy,
                )
                self.assertIn("showDate=2026-09-04", website)
                self.assertIn("queueOpen=True", website)
                self.assertIn("Present Signal", website)
                self.assertNotIn(AUGUST_COMMENT, website)
                self.assertNotIn(SEPTEMBER_COMMENT, website)

    async def test_unrelated_dated_text_does_not_read_the_same_date_show_archive(self):
        for request in (
            "Show me the schedule for September 4, 2026.",
            "I live in Test City and my appointment is September 4, 2026.",
        ):
            for policy in ("public_home", "sealed_test"):
                with self.subTest(request=request, policy=policy):
                    self.fetch.reset_mock()
                    self.assertEqual(REAL_READ_MODEL_CONTEXT(request, policy), "")
                    self.fetch.assert_not_called()
                    self.assertEqual(bot.resolve_tiktok_show_analysis_request(request), "")

    async def test_explicit_live_show_date_keeps_current_comments_after_midnight(self):
        clock = live_fixture.Clock(time())
        adapter = live_fixture.TikTokLiveContextBridgeTests().make_adapter(clock)
        path = Path(bot.DB_FILE).with_name("live-context.json")
        LiveContextSnapshotWriter(str(path), time_fn=clock).publish(adapter, force=True)
        self.read_model["sections"]["archive"]["currentShow"] = {
            "sessionId": "friday-live", "showDate": "2026-09-04",
            "status": "open", "milestones": [],
        }
        # The archive's ongoing show owns the live scope even when the queue
        # session date has advanced. Persistence must retain that same source.
        self.read_model["sections"]["queue"]["session"]["showDate"] = "2026-09-05"
        with mock.patch("bnl_tiktok_live_context._pacific_show_date", return_value=date(2026, 9, 5)), \
                mock.patch.object(bot, "BNL_TIKTOK_LIVE_CONTEXT_PATH", str(path)), \
                mock.patch.object(bot, "BNL_TIKTOK_LIVE_CONTEXT_ENABLED", True):
            for policy in ("public_home", "sealed_test"):
                with self.subTest(policy=policy):
                    website = REAL_READ_MODEL_CONTEXT(
                        "What's TikTok chat saying in the September 4, 2026 show right now?",
                        policy,
                    )
                    self.assertIn("This track is wild.", website)
                    self.assertIn("showDate=2026-09-04", website)
                    self.assertNotIn(AUGUST_COMMENT, website)
                    self.assertNotIn(SEPTEMBER_COMMENT, website)
                    self.assertFalse(bot.public_tiktok_interaction_memory_allowed(
                        "What's TikTok chat saying in the September 5, 2026 show right now?",
                        policy, website,
                    ))
                    self.assertIn("This track is wild.", REAL_READ_MODEL_CONTEXT(
                        "What's TikTok chat saying\nin the September 4, 2026 show right now?",
                        policy,
                    ))

                    answer = "Test Viewer says the current track is wild."
                    channel, generation, _guard = await self.runtime._batch(
                        policy,
                        request="What's TikTok chat saying in the September 4, 2026 show right now?",
                        answer=answer, privileged=False,
                    )
                    generation.assert_awaited_once()
                    self.assertIn("This track is wild.", generation.await_args.args[0])
                    self.assertEqual(channel.sent, [answer])
                    with sqlite3.connect(bot.DB_FILE) as conn:
                        saved = conn.execute(
                            "SELECT content FROM conversations WHERE role='model' AND channel_id=?",
                            (channel.id,),
                        ).fetchall()
                    state = bot._get_conversation_continuation_state(
                        self.runtime.guild_id, channel.id, self.runtime.user_id,
                    )
                    if policy == "public_home":
                        self.assertEqual(saved, [(answer,)])
                        self.assertIsNotNone(state)
                        self.assertIn("last_bnl_reply_at", state)
                    else:
                        self.assertEqual(saved, [])
                        self.assertFalse(state and state.get("last_bnl_reply_at"))

    async def test_real_batch_delivers_one_supported_answer_from_requested_date(self):
        for policy in ("public_home", "sealed_test"):
            with self.subTest(policy=policy):
                channel, generation, guard = await self.runtime._batch(
                    policy, request=REQUEST, answer=ANSWER, privileged=False,
                )
                generation.assert_awaited_once()
                guard.assert_awaited_once()
                self.assertEqual(channel.sent, [ANSWER])
                prompt = generation.await_args.args[0]
                self.assertIn("showDate=2026-08-28", prompt)
                self.assertIn(AUGUST_COMMENT, prompt)
                self.assertNotIn(SEPTEMBER_COMMENT, prompt)
                self.assertNotIn("showDate=2026-09-04", prompt)
                self.assertNotIn("Present Signal", prompt)
                bases = tuple(
                    item for item in guard.await_args.kwargs["prompt_source_bases"]
                    if isinstance(item, bot.FinalizedShowPromptSourceBasis)
                )
                self.assertEqual(len(bases), 1)
                self._assert_august_source(bases[0].rendered_context, bases[0])


if __name__ == "__main__":
    unittest.main()
