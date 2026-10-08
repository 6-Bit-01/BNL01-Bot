"""Artist credits in retained shows are distinct from submission accounts."""

import ast
import hashlib
import json
import logging
import os
import sqlite3
import tempfile
import time
import unittest
from contextlib import closing
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import bnl_tiktok_live_context as live
import bnl_tiktok_show_ledger as ledger
import bnl_unified_response_assessment as assessment
import bnl_unified_intelligence_packet as packet
import bnl_canon_entity_binding as binding
import bnl_conversation_context_v2 as context_owner

from bnl_tiktok_live_context import is_tiktok_show_analysis_query
from bnl_tiktok_show_ledger import (
    broad_show_history_requested, build_tiktok_show_evidence_context,
    select_tiktok_show_episode_context_items, sync_tiktok_show_evidence_ledgers,
    tiktok_show_episode_context_item_version,
)
from bnl_unified_response_assessment import self_public_activity_requested
from tests import test_tiktok_show_evidence_ledger as source_fixture
from tests.test_tiktok_show_evidence_ledger import archived_show, authorized_read_model, ENABLED_QUEUE_ENV


HISTORY_REQUESTS = (
    "Have I ever submitted any songs?",
    "Do I have any songs in any show?",
    "People have submitted my songs before",
    "BNL what songs of mine have been submitted?",
    "Will you list the songs of mine that have been submitted?",
    "Have any of my songs been submitted before, and can I submit again next Friday?",
)


def turn_reader_namespace(db_file):
    """Execute source-owner definitions only, without importing bot startup."""
    source = Path(__file__).resolve().parents[1] / "bnl01_bot.py"
    tree = ast.parse(source.read_text(encoding="utf-8"))
    names = {
        "_show_artist_identity_request", "_show_artist_labels", "_prior_queue_request_for_context",
        "build_tiktok_show_evidence_context_for_turn", "FinalizedShowPromptSourceBasis",
        "FinalizedShowAuthoredExcerpt", "_finalized_show_basis_digest", "_prompt_source_digest",
        "_finalized_show_authored_excerpts_from_selection", "build_finalized_show_prompt_source_basis",
        "refresh_prompt_source_basis", "_open_member_memory_read_connection",
        "finalized_show_packet_owner_requested", "_current_queue_state_query",
        "resolve_tiktok_show_analysis_request",
    }
    namespace = {**vars(live), **vars(ledger), **vars(assessment), **vars(packet),
        "__name__": __name__, "DB_FILE": db_file, "closing": closing, "Path": Path,
        "os": os, "time": time, "logging": logging, "hashlib": hashlib,
        "dataclass": dataclass, "replace": replace,
        "_consented_tiktok_show_subject_user_id": lambda **kwargs: kwargs["subject_user_id"],
        "_public_member_continuation_query": lambda text, *_args, **_kwargs: text,
    }
    nodes = [node for node in tree.body if isinstance(node, (ast.FunctionDef, ast.ClassDef)) and node.name in names]
    code = "from __future__ import annotations\n" + "\n".join(ast.unparse(node) for node in nodes)
    exec(compile(code, str(source), "exec"), namespace)
    return namespace


class ArtistSubmissionHistoryTests(unittest.TestCase):
    def setUp(self):
        self.scratch = tempfile.TemporaryDirectory()
        self.addCleanup(self.scratch.cleanup)
        self.db_file = str(Path(self.scratch.name) / "artist-history.sqlite")
        with closing(sqlite3.connect(self.db_file)):
            pass
        source_fixture.TikTokShowEvidenceLedgerTests().seed_source_and_memory(self.db_file, include_shadow_memory=False)
        shows = []
        for index, date in enumerate(("2026-08-28", "2026-09-04", "2026-09-11", "2026-09-18", "2026-09-25", "2026-10-02")):
            show = archived_show()
            show["sessionId"] = "neutral-history-" + str(index)
            show["showDate"] = date
            show["trackRoster"] = [dict(trackId=f"row-{row}", projectLabel="Other Artist",
                title=f"Neutral Song {row}", submittedByTikTokHandle="other.submitter", outcome="finished")
                for row in range(50)]
            if index == 2:
                show["trackRoster"][48].update(projectLabel="6 Bit", title="Neutral Signal", submittedByTikTokHandle="test.submitter")
                show["trackRoster"][41].update(projectLabel="Different Artist", title="6Bit the imaginary panda")
            if index == 4:
                show["trackRoster"][37].update(projectLabel="6-Bit featuring Second Artist", title="Neutral Collaboration", submittedByTikTokHandle="second.submitter")
            shows.append(show)
        result = sync_tiktok_show_evidence_ledgers(self.db_file, guild_id=77,
            read_model=authorized_read_model({"currentShow": None, "latestShow": shows[-1], "shows": shows}),
            environ=ENABLED_QUEUE_ENV)
        self.assertEqual(result["showsFinalized"], 6, result)

    def test_historical_self_music_uses_show_scope_and_requester_subject(self):
        for request in HISTORY_REQUESTS:
            with self.subTest(request=request):
                self.assertTrue(is_tiktok_show_analysis_query(request))
                self.assertTrue(broad_show_history_requested(request))
                self.assertTrue(self_public_activity_requested(request))

    def test_credit_search_reads_full_retained_rosters_and_separates_submitter(self):
        for request in HISTORY_REQUESTS:
            with self.subTest(request=request):
                selection = {}
                context = build_tiktok_show_evidence_context(self.db_file, guild_id=77,
                    user_text=request, artist_labels=("6 Bit", "Six Bit"), selection_out=selection)
                self.assertIn("Neutral Signal", context)
                self.assertIn("Neutral Collaboration", context)
                self.assertIn("test.submitter", context)
                self.assertIn("second.submitter", context)
                self.assertNotIn("imaginary panda", context)
                self.assertIn("6 eligible retained finalized shows", context)
                self.assertIn("2 matching credit rows", context)
                self.assertEqual(len(selection["source_refs"]), 6)
                self.assertLess(len(context), 2400)

    def test_current_human_scope_beats_prior_show_date(self):
        context = build_tiktok_show_evidence_context(self.db_file, guild_id=77,
            user_text=HISTORY_REQUESTS[1], selection_user_text="What happened in the show on 2026-10-02?",
            artist_labels=("6 Bit",))
        self.assertIn("Neutral Signal", context)
        dated = build_tiktok_show_evidence_context(self.db_file, guild_id=77,
            user_text="Were my songs submitted on 2026-09-11?", artist_labels=("6 Bit",))
        self.assertIn("Neutral Signal", dated)
        self.assertNotIn("Neutral Collaboration", dated)
        self.assertIn("1 eligible retained finalized shows", dated)

    def test_unresolved_and_empty_credit_have_honest_limits(self):
        unresolved = build_tiktok_show_evidence_context(self.db_file, guild_id=77, user_text=HISTORY_REQUESTS[1])
        self.assertIn("artist credit is unresolved", unresolved)
        self.assertNotIn("Neutral Signal", unresolved)
        empty = build_tiktok_show_evidence_context(self.db_file, guild_id=77,
            user_text=HISTORY_REQUESTS[1], artist_labels=("Absent Artist",))
        self.assertIn("No matching artist credit", empty)
        self.assertIn("not all-time submission history", empty)

    def test_packet_projection_uses_same_roots_and_revalidates_removal(self):
        with closing(sqlite3.connect(self.db_file)) as conn:
            items = select_tiktok_show_episode_context_items(conn, guild_id=77,
                user_text=HISTORY_REQUESTS[1], artist_labels=("6 Bit",))
            self.assertEqual(len(items), 1)
            item = items[0]
            self.assertEqual(len(item.show_keys), 6)
            self.assertIn("Neutral Signal", item.text)
            conn.execute("DELETE FROM tiktok_show_evidence_ledgers WHERE show_key=?", (item.show_keys[0],))
            conn.commit()
            version = tiktok_show_episode_context_item_version(conn, guild_id=77,
                user_text=HISTORY_REQUESTS[1], subject_user_id=0, source_ref=item.source_ref,
                artist_labels=("6 Bit",))
            self.assertNotEqual(version, item.source_digest)

    def test_current_queue_and_nonmusic_requests_keep_existing_owner(self):
        for request in ("Is queue open?", "Is my song in the queue?", "How can I submit my song?", "Have I ever hosted a show?",
                        "Will my songs be played next Friday?", "Can my song be featured in the next show?"):
            with self.subTest(request=request):
                from bnl_unified_response_assessment import music_submission_history_requested
                self.assertFalse(music_submission_history_requested(request))

    def test_exact_credit_punctuation_is_not_mistaken_for_collaborators(self):
        for label in ("Test / Artist", "Test & Artist", "Test with Artist", "Test, Artist", "Test + Artist"):
            self.assertTrue(ledger._artist_credit_matches(label, (label,)))
        self.assertFalse(ledger._artist_credit_matches("Other Test Artist", ("Test Artist",)))

    def frame(self, text, speakers=(42,), *, policy="sealed_test"):
        return assessment.build_situation_frame_v1(
            route_allowed=True, route_mode="normal_chat", conversation_surface=policy,
            channel_policy=policy, current_text=text,
            current_speaker_user_ids=speakers, current_speaker_labels=("Test Member",),
            response_act="answer",
        )

    def test_real_turn_frame_resolves_current_artist_and_does_not_reuse_queue_scope(self):
        ns = turn_reader_namespace(self.db_file)
        env = {**ENABLED_QUEUE_ENV, "BNL_OWNER_USER_ID": "42", "BNL_PRIMARY_GUILD_ID": "77"}
        prior = SimpleNamespace(current_user_id=42, evidence_items=(SimpleNamespace(
            source_id=1, speaker_user_id=42, text="Is queue open?"),))
        context_result = SimpleNamespace(thread_focus_mode="continue_or_answer", referent_status="not_requested", selected_row_ids=(1,))
        with mock.patch.dict(os.environ, env):
            for text in HISTORY_REQUESTS:
                with self.subTest(text=text):
                    frame = self.frame(text)
                    self.assertEqual(tuple(subject.user_id for subject in frame.subjects), (42,))
                    selection = {}
                    context = ns["build_tiktok_show_evidence_context_for_turn"](
                        guild_id=77, subject_user_id=42, user_text=text,
                        situation_frame=frame, selection_out=selection)
                    self.assertIn("Neutral Signal", context)
                    self.assertEqual(len(selection["source_refs"]), 6)
                    self.assertEqual(ns["_prior_queue_request_for_context"](
                        text, conversation_basis=prior, context_result=context_result), "")

    def test_native_basis_reloads_artist_binding_and_source_authorization(self):
        ns = turn_reader_namespace(self.db_file)
        env = {**ENABLED_QUEUE_ENV, "BNL_OWNER_USER_ID": "42", "BNL_PRIMARY_GUILD_ID": "77"}
        text = HISTORY_REQUESTS[1]
        with mock.patch.dict(os.environ, env):
            selection = {}
            context = ns["build_tiktok_show_evidence_context_for_turn"](
                guild_id=77, subject_user_id=42, user_text=text,
                situation_frame=self.frame(text), selection_out=selection)
            basis = ns["build_finalized_show_prompt_source_basis"](context, guild_id=77, selection=selection)
            fresh, changed = ns["refresh_prompt_source_basis"](basis)
            self.assertFalse(changed)
            with mock.patch.dict(os.environ, {"BNL_OWNER_USER_ID": "99"}):
                withdrawn, changed = ns["refresh_prompt_source_basis"](basis)
                self.assertTrue(changed)
                self.assertNotIn("Neutral Signal", withdrawn.rendered_context)
            with closing(sqlite3.connect(self.db_file)) as conn:
                conn.execute("UPDATE tiktok_show_evidence_ledgers SET ledger_json='{}'")
                conn.commit()
            invalid, changed = ns["refresh_prompt_source_basis"](basis)
            self.assertTrue(changed)
            self.assertNotIn("Neutral Signal", invalid.rendered_context)

    def test_prefaced_archive_question_reaches_native_and_packet_artist_history(self):
        text = (
            "Archive gremlin, I need a crate inspection: which of my songs have other people "
            "submitted to past shows? Give me the track, artist credit, TikTok submitter profile, "
            "and show date.")
        ns = turn_reader_namespace(self.db_file)
        env = {**ENABLED_QUEUE_ENV, "BNL_OWNER_USER_ID": "42", "BNL_PRIMARY_GUILD_ID": "77"}
        with mock.patch.dict(os.environ, env):
            selection = {}
            native_text = ns["build_tiktok_show_evidence_context_for_turn"](
                guild_id=77, subject_user_id=42, user_text=text,
                situation_frame=self.frame(text, policy="public_context"), selection_out=selection)
            request = packet.IntelligencePacketRequest(
                guild_id=77, subject_user_id=42, route_mode="normal_chat",
                conversation_surface="public_context", channel_policy="public_context", user_text=text,
                show_episode_selection_text=selection.get("selection_user_text", ""),
                show_episode_artist_request=selection.get("artist_identity_request"))
            with closing(sqlite3.connect(self.db_file)) as conn:
                items = packet._show_episode_items(
                    conn, request, packet.IntelligencePacketDiagnostics(), [], environ=env)
            packet_text = "\n".join(item.text for item in items)

        expected = {
            ("2026-09-11", "6 Bit", "Neutral Signal", "test.submitter"),
            ("2026-09-25", "6-Bit featuring Second Artist", "Neutral Collaboration", "second.submitter"),
        }
        for surface, context in (("native", native_text), ("packet", packet_text)):
            with self.subTest(surface=surface):
                observed = set()
                for line in context.splitlines():
                    if line.startswith("- Show "):
                        show, _, payload = line.partition(": ")
                        record = json.loads(payload)
                        observed.add((show.removeprefix("- Show "), record["projectLabel"],
                                      record["title"], record["submittedByTikTokHandle"]))
                self.assertEqual(observed, expected)
                self.assertNotIn("imaginary panda", context)
                self.assertNotIn("Other Artist", context)

    def test_identity_uses_account_authority_not_display_name_and_retirement_wins(self):
        ns = turn_reader_namespace(self.db_file)
        text = HISTORY_REQUESTS[1]
        env = {**ENABLED_QUEUE_ENV, "BNL_OWNER_USER_ID": "42", "BNL_PRIMARY_GUILD_ID": "77",
               "BNL_DECLARED_CANON_AUTHORITY_SECRET": "neutral-artist-binding-test-secret-0001"}
        with mock.patch.dict(os.environ, env), closing(sqlite3.connect(self.db_file)) as conn:
            request = ns["_show_artist_identity_request"](guild_id=77, user_text=text, situation_frame=self.frame(text))
            self.assertEqual(packet.show_artist_labels_for_request(conn, request), ("6 Bit", "Six Bit"))
            other = ns["_show_artist_identity_request"](guild_id=77, user_text=text, situation_frame=self.frame(text, speakers=(99,)))
            self.assertEqual(packet.show_artist_labels_for_request(conn, other), ())
            multi = ns["_show_artist_identity_request"](guild_id=77, user_text=text, situation_frame=self.frame(text, speakers=(42, 99)))
            self.assertEqual(packet.show_artist_labels_for_request(conn, multi), ())
            created = binding.bind_discord_account(conn, actor_user_id=42, authority_nonce="artist-bind-neutral-0001",
                guild_id=77, account_id="42", entity_id="6_bit", reason="Neutral test binding").revision
            binding.retire_discord_account_binding(conn, actor_user_id=42, authority_nonce="artist-retire-neutral-0001",
                guild_id=77, binding_id=created.binding_id, expected_revision_id=created.binding_revision_id,
                reason="Neutral test withdrawal")
            conn.commit()
            self.assertEqual(packet.show_artist_labels_for_request(conn, request), ())

    def test_unrelated_packet_does_not_add_artist_binding_read(self):
        with closing(sqlite3.connect(self.db_file)) as conn:
            request = packet.IntelligencePacketRequest(guild_id=77, subject_user_id=42,
                route_mode="normal_chat", conversation_surface="sealed_test", user_text="Is queue open?")
            with mock.patch.object(packet, "resolve_packet_subject", side_effect=AssertionError("Unrelated binding read")):
                self.assertEqual(packet.show_artist_labels_for_request(conn, request), ())

    def test_resolved_human_history_referent_keeps_artist_separate_from_submitter(self):
        import test_conversation_batching as bot_fixture
        bot = bot_fixture.bnl01_bot
        ns = vars(bot)
        db_patch = mock.patch.object(bot, "DB_FILE", self.db_file)
        db_patch.start()
        self.addCleanup(db_patch.stop)
        text = "And which of those did Test Submitter submit, and when?"
        with closing(sqlite3.connect(self.db_file)) as conn:
            conn.execute("UPDATE conversations SET content=? WHERE id=101", (HISTORY_REQUESTS[1],))
            conn.commit()
        prior = SimpleNamespace(guild_id=77, current_user_id=42, evidence_items=(SimpleNamespace(
            source_id=101, speaker_user_id=42, text=HISTORY_REQUESTS[1]),))
        context_result = SimpleNamespace(thread_focus_mode="continue_or_answer", referent_status="resolved",
            selected_row_ids=(101,), referent_selected_row_ids=(102,), referent_request_row_ids=(101,),
            requester_user_id=42, referent_reason="selected_human_request", requester_human_turns=((101, HISTORY_REQUESTS[1]),))
        env = {**ENABLED_QUEUE_ENV, "BNL_OWNER_USER_ID": "42", "BNL_PRIMARY_GUILD_ID": "77"}
        with mock.patch.dict(os.environ, env):
            selection = {}
            context = ns["build_tiktok_show_evidence_context_for_turn"](
                guild_id=77, subject_user_id=42, user_text=text, situation_frame=self.frame(text),
                conversation_basis=prior, conversation_context_result=context_result, selection_out=selection)
            self.assertIn("Neutral Signal", context)
            self.assertIn("test.submitter", context)
            self.assertIn("2026-09-11", context)
            self.assertIn("Neutral Collaboration", context)
            for followup, website in (
                ("Which of those songs did Test Submitter submit, and when?", ""),
                (text, bot.WebsiteReadModelContext("", continuation_show_dates=("2026-10-02",))),
            ):
                with self.subTest(followup=followup, website=bool(website)):
                    retained = ns["build_tiktok_show_evidence_context_for_turn"](
                        guild_id=77, subject_user_id=42, user_text=followup, situation_frame=self.frame(followup),
                        conversation_basis=prior, conversation_context_result=context_result,
                        website_read_model_context=website)
                    self.assertIn("Neutral Signal", retained)
                    self.assertIn("Neutral Collaboration", retained)
            artist_request = selection["artist_identity_request"]
            self.assertIsNotNone(artist_request)
            request = packet.IntelligencePacketRequest(guild_id=77, subject_user_id=99,
                route_mode="normal_chat", conversation_surface="sealed_test", user_text=text,
                show_episode_selection_text=selection["selection_user_text"],
                show_episode_artist_request=artist_request)
            with closing(sqlite3.connect(self.db_file)) as conn:
                labels = packet.show_artist_labels_for_request(conn, request)
                self.assertEqual(labels[0], "6 Bit")
                items = ledger.select_tiktok_show_episode_context_items(conn, guild_id=77,
                    user_text=packet._show_episode_query(request), artist_labels=labels)
                self.assertIn("Neutral Signal", items[0].text)
                self.assertIn("Neutral Collaboration", items[0].text)
            for new_text, author, status in (
                ("What songs by Other Artist have been submitted?", 42, "resolved"),
                ("What about 2026-09-25?", 42, "resolved"),
                ("Is queue open?", 42, "resolved"),
                (text, 99, "resolved"), (text, 42, "ambiguous"),
                (text, 0, "resolved"), ("What about Mac Modem?", 42, "resolved"),
            ):
                with self.subTest(text=new_text, author=author, status=status):
                    control = SimpleNamespace(**vars(context_result))
                    control.referent_status = status
                    anchor = SimpleNamespace(**vars(prior))
                    anchor.evidence_items = (SimpleNamespace(source_id=101, speaker_user_id=author, text=HISTORY_REQUESTS[1]),)
                    selected = {}
                    frame = self.frame(new_text)
                    if "Mac Modem" in new_text:
                        frame = replace(frame, subjects=(assessment.SituationSubjectReference(entity_ref="mac_modem"),))
                    ns["build_tiktok_show_evidence_context_for_turn"](
                        guild_id=77, subject_user_id=42, user_text=new_text, situation_frame=frame,
                        conversation_basis=anchor, conversation_context_result=control, selection_out=selected)
                    inherited = selected.get("artist_identity_request")
                    self.assertFalse(inherited and inherited.frame_subjects and
                        any(subject.user_id == 42 for subject in inherited.frame_subjects))
            show_basis = bot.build_finalized_show_prompt_source_basis(context, guild_id=77, selection=selection)
            human_basis = bot.ConversationPromptSourceBasis(
                expected_digest=bot._conversation_prompt_basis_digest(
                    bot._conversation_prompt_selected_digest(guild_id=77, source_row_ids=(101,)), (), ()),
                rendered_context=HISTORY_REQUESTS[1], guild_id=77, current_user_id=42,
                channel_id=9001, channel_name="barcode-bot", channel_policy="public_home",
                source_row_ids=(101,), revalidation_row_ids=(101,),
            )
            self.assertEqual(bot.prompt_source_basis_failure((human_basis, show_basis)), "")
            with closing(sqlite3.connect(self.db_file)) as conn:
                conn.execute("DELETE FROM conversations WHERE id=101")
                conn.commit()
            self.assertNotEqual(bot.prompt_source_basis_failure((human_basis, show_basis)), "")

    def test_raw_context_keeps_artist_history_for_submitter_subset_followup(self):
        import test_conversation_batching as bot_fixture
        bot = bot_fixture.bnl01_bot
        now = datetime.now(timezone.utc)
        first = ("BNL, have any songs of mine appeared in the archived shows? Include tracks sent by somebody else, "
                 "and give the recorded artist, title, submitter, and show date.")
        answer = ("The retained shows include 6 Bit — Neutral Signal, submitted by Test Submitter on September 11, "
                  "and 6-Bit featuring Second Artist — Neutral Collaboration, submitted by Second Submitter on September 25.")
        followup = "And which of those songs did Test Submitter submit, and when?"
        with closing(sqlite3.connect(self.db_file)) as conn:
            conn.execute("DELETE FROM conversations")
            conn.executemany("""INSERT INTO conversations
                (id,user_id,user_name,guild_id,channel_name,channel_policy,route_mode,role,content,timestamp,channel_id,message_id)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""", [
                    (201, 42, "Test Member", 77, "bnl-testing", "sealed_test", "normal_chat", "user", first,
                     (now - timedelta(seconds=44)).isoformat(), 9001, 7201),
                    (202, 42, "BNL-01", 77, "bnl-testing", "sealed_test", "normal_chat", "model", answer,
                     (now - timedelta(seconds=19)).isoformat(), 9001, 7202),
                ])
            if getattr(self, "_unpaired_history", False):
                conn.execute("DELETE FROM conversations WHERE id=202")
            conn.execute("""INSERT INTO conversations
                (id,user_id,user_name,guild_id,channel_name,channel_policy,route_mode,role,content,timestamp,channel_id,message_id)
                VALUES (203,42,'Test Member',77,'bnl-testing','sealed_test','normal_chat','user',?,?,9001,7203)""",
                (followup, now.isoformat()))
            conn.commit()
        env = {**ENABLED_QUEUE_ENV, "BNL_OWNER_USER_ID": "42", "BNL_PRIMARY_GUILD_ID": "77"}
        with mock.patch.object(bot, "DB_FILE", self.db_file), mock.patch.dict(os.environ, env):
            result_out = {}
            rendered = bot.build_conversation_context_v2_for_prompt(
                guild_id=77, current_user_id=42, channel_id=9001, channel_name="bnl-testing",
                channel_policy="sealed_test", route_mode="normal_chat", conversation_surface="sealed_test",
                current_texts=(followup,), current_participants={42}, is_batch=True,
                current_message_ids={7203}, is_direct_target=True, now=now, result_out=result_out)
            context_result = result_out["result"]
            basis = bot.build_conversation_prompt_source_basis(rendered, guild_id=77, current_user_id=42,
                channel_id=9001, channel_name="bnl-testing", channel_policy="sealed_test", context_result=context_result)
            self.assertIsNotNone(basis)
            self.assertIn(201, basis.source_row_ids)
            selection = {}
            context = bot.build_tiktok_show_evidence_context_for_turn(
                guild_id=77, subject_user_id=42, user_text=followup, situation_frame=self.frame(followup),
                conversation_basis=basis, conversation_context_result=context_result, selection_out=selection)
            diagnostic = (context_result.thread_focus_mode, context_result.referent_status,
                          context_result.referent_reason, context_result.selected_row_ids,
                          context_result.referent_request_row_ids, len(context))
            with self.subTest(owner="native", context_selection=diagnostic):
                self.assertIn("Neutral Signal", context)
                self.assertIn("test.submitter", context)
                self.assertLess(len(context), 2400)
            request = packet.IntelligencePacketRequest(guild_id=77, subject_user_id=42,
                route_mode="normal_chat", conversation_surface="sealed_test", user_text=followup,
                show_episode_selection_text=selection.get("selection_user_text", ""),
                show_episode_artist_request=selection.get("artist_identity_request"))
            with closing(sqlite3.connect(self.db_file)) as conn:
                items = packet._show_episode_items(conn, request, packet.IntelligencePacketDiagnostics(), [], environ=env)
            with self.subTest(owner="packet", context_selection=diagnostic):
                self.assertTrue(any("Neutral Signal" in item.text for item in items))
            for current in ("What songs by Mac Modem have been submitted?", "Which of those songs were on 2026-09-25?", "Is queue open?"):
                with self.subTest(current_override=current):
                    scoped_out = {}
                    scoped_rendered = bot.build_conversation_context_v2_for_prompt(
                        guild_id=77, current_user_id=42, channel_id=9001, channel_name="bnl-testing",
                        channel_policy="sealed_test", route_mode="normal_chat", conversation_surface="sealed_test",
                        current_texts=(current,), current_participants={42}, is_batch=True,
                        current_message_ids={7203}, is_direct_target=True, now=now, result_out=scoped_out)
                    scoped_basis = bot.build_conversation_prompt_source_basis(scoped_rendered,
                        guild_id=77, current_user_id=42, channel_id=9001, channel_name="bnl-testing",
                        channel_policy="sealed_test", context_result=scoped_out["result"])
                    scoped_selection = {}
                    bot.build_tiktok_show_evidence_context_for_turn(guild_id=77, subject_user_id=42,
                        user_text=current, situation_frame=self.frame(current), conversation_basis=scoped_basis,
                        conversation_context_result=scoped_out["result"], selection_out=scoped_selection)
                    artist_request = scoped_selection.get("artist_identity_request")
                    self.assertFalse(artist_request and any(subject.user_id == 42 for subject in artist_request.frame_subjects))
            show_basis = bot.build_finalized_show_prompt_source_basis(context, guild_id=77, selection=selection)
            self.assertEqual(bot.prompt_source_basis_failure((basis, show_basis)), "")
            with closing(sqlite3.connect(self.db_file)) as conn:
                conn.execute("DELETE FROM conversations WHERE id=201")
                conn.commit()
            self.assertNotEqual(bot.prompt_source_basis_failure((basis, show_basis)), "")

    def test_raw_unpaired_human_request_and_current_duplicate_keep_history(self):
        self._unpaired_history = True
        self.test_raw_context_keeps_artist_history_for_submitter_subset_followup()

    def test_raw_newer_unpaired_archive_request_beats_older_greeting_pair(self):
        """A no-store answer must not make an older complete pair own the follow-up."""
        import test_conversation_batching as bot_fixture
        bot = bot_fixture.bnl01_bot
        now = datetime.now(timezone.utc)
        policy = getattr(self, "_flow_policy", "sealed_test")
        channel_name = "general-chat" if policy == "public_context" else "bnl-testing"
        first = ("BNL, have any songs of mine appeared in the archived shows? Include tracks sent by somebody else, "
                 "and give the recorded artist, title, submitter, and show date.")
        followup = "Which of those did Test Submitter submit, and when was that show?"
        rows = [
            (301, "user", "Hey BNL, how are you?", 240),
            (302, "model", "Running smoothly, Test Member. What is happening?", 220),
            (303, "user", first, 180),
            # The archive answer was delivered without a stored model row.
            (304, "user", followup, 0),
        ]
        with closing(sqlite3.connect(self.db_file)) as conn:
            conn.execute("DELETE FROM conversations")
            conn.executemany("""INSERT INTO conversations
                (id,user_id,user_name,guild_id,channel_name,channel_policy,route_mode,role,content,timestamp,channel_id,message_id)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""", [
                    (row_id, 42, "Test Member" if role == "user" else "BNL-01", 77,
                     channel_name, policy, "normal_chat", role, content,
                     (now - timedelta(seconds=age)).isoformat(), 9001, 7300 + row_id)
                    for row_id, role, content, age in rows
                ])
            conn.commit()
        env = {**ENABLED_QUEUE_ENV, "BNL_OWNER_USER_ID": "42", "BNL_PRIMARY_GUILD_ID": "77"}
        with mock.patch.object(bot, "DB_FILE", self.db_file), mock.patch.dict(os.environ, env):
            result_out = {}
            rendered = bot.build_conversation_context_v2_for_prompt(
                guild_id=77, current_user_id=42, channel_id=9001, channel_name=channel_name,
                channel_policy=policy, route_mode="normal_chat", conversation_surface=policy,
                current_texts=(followup,), current_participants={42}, is_batch=True,
                current_message_ids={7604}, is_direct_target=True, now=now, result_out=result_out)
            context_result = result_out["result"]
            basis = bot.build_conversation_prompt_source_basis(rendered, guild_id=77, current_user_id=42,
                channel_id=9001, channel_name=channel_name, channel_policy=policy, context_result=context_result)
            self.assertIsNotNone(basis)
            selection = {}
            context = bot.build_tiktok_show_evidence_context_for_turn(
                guild_id=77, subject_user_id=42, user_text=followup, situation_frame=self.frame(followup, policy=policy),
                conversation_basis=basis, conversation_context_result=context_result, selection_out=selection)
            diagnostic = (context_result.thread_focus_mode, context_result.referent_status,
                          context_result.referent_reason, context_result.selected_row_ids,
                          context_result.referent_request_row_ids, selection.get("selection_user_text", ""))
            with self.subTest(owner="native", context_selection=diagnostic):
                self.assertIn("Neutral Signal", context)
                self.assertIn("test.submitter", context)
                self.assertIn("2026-09-11", context)
                self.assertNotIn("2026-10-02", context)
                self.assertNotIn("Neutral Song 0", context)
                self.assertEqual(context_result.referent_selected_row_ids, (303,))
                self.assertIn(303, basis.source_row_ids)
            request = packet.IntelligencePacketRequest(guild_id=77, subject_user_id=42,
                route_mode="normal_chat", conversation_surface=policy, channel_policy=policy, user_text=followup,
                show_episode_selection_text=selection.get("selection_user_text", ""),
                show_episode_artist_request=selection.get("artist_identity_request"))
            with closing(sqlite3.connect(self.db_file)) as conn:
                items = packet._show_episode_items(conn, request, packet.IntelligencePacketDiagnostics(), [], environ=env)
            with self.subTest(owner="packet", context_selection=diagnostic):
                text = "\n".join(item.text for item in items)
                self.assertIn("Neutral Signal", text)
                self.assertIn("test.submitter", text)
                self.assertIn("2026-09-11", text)
                self.assertNotIn("2026-10-02", text)
                self.assertNotIn("Neutral Song 0", text)

    def test_public_context_unpaired_archive_request_beats_older_greeting_pair(self):
        self._flow_policy = "public_context"
        self.test_raw_newer_unpaired_archive_request_beats_older_greeting_pair()

    def test_raw_unstored_answer_chain_preserves_artist_and_submitter_scope(self):
        import test_conversation_batching as bot_fixture
        bot = bot_fixture.bnl01_bot
        now = datetime.now(timezone.utc)
        policy = getattr(self, "_flow_policy", "sealed_test")
        channel_name = "general-chat" if policy == "public_context" else "bnl-testing"
        first = ("BNL, have any songs of mine appeared in the archived shows? Include tracks sent by somebody else, "
                 "and give the recorded artist, title, submitter, and show date.")
        subset = "Which of those did Test Submitter submit, and when was that show?"
        saved_subset_answer = getattr(self, "_saved_subset_answer", False)
        current = ("Which of those was the most recent for the submitter we just discussed, and what was the show date?"
                   if saved_subset_answer else "Which of those recordings appeared first, and on what date?")
        rows = [(401, "user", "Hey BNL, how are you?", 300),
                (402, "model", "Running smoothly, Test Member.", 280),
                (403, "user", first, 240), (404, "user", subset, 120)]
        if saved_subset_answer:
            rows.append((405, "model", getattr(self, "_saved_subset_answer_text",
                "Test Submitter sent Neutral Signal, credited to 6 Bit, for the September 11 show."), 60))
        current_row_id = 406 if saved_subset_answer else 405
        if saved_subset_answer and getattr(self, "_saved_subset_answer_count", 1) > 1:
            rows.extend(((406, "user", current, 45),
                (407, "model", "Different Artist performed Neutral Counterfeit on October 2.", 30)))
            current_row_id = 408
            current = "Which of those entries should I look at first, and what was the recorded date?"
        saved_archive_answer = int(bool(getattr(self, "_saved_archive_answer", False)))
        if saved_archive_answer:
            rows = [(row_id + int(row_id >= 404), role, text, age) for row_id, role, text, age in rows]
            rows.append((404, "model", "Two retained recordings appeared in the archive.", 180))
            current_row_id += 1
        subset_row_id = 404 + saved_archive_answer
        last_subset_answer_saved = getattr(self, "_last_subset_answer_saved", True)
        if not last_subset_answer_saved:
            rows = [item for item in rows if item[0] != 407 + saved_archive_answer]
        independent_topic = getattr(self, "_independent_topic", False)
        if independent_topic:
            rows = [(row_id + (2 if row_id >= 406 else 0), role, text, age) for row_id, role, text, age in rows]
            rows.extend(((406, "user", "Which lamps fit a desk?", 50),
                         (407, "model", "A shaded desk lamp fits.", 48)))
            current_row_id += 2
        current = getattr(self, "_current_chain_question", current)
        rows.append((current_row_id, "user", current, 0))
        with closing(sqlite3.connect(self.db_file)) as conn:
            conn.execute("DELETE FROM conversations")
            conn.executemany("""INSERT INTO conversations
                (id,user_id,user_name,guild_id,channel_name,channel_policy,route_mode,role,content,timestamp,channel_id,message_id)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""", [
                    (row_id, 42, "Test Member" if role == "user" else "BNL-01", 77,
                     channel_name, policy, "normal_chat", role, content,
                     (now - timedelta(seconds=age)).isoformat(), 9001, 7400 + row_id)
                    for row_id, role, content, age in rows])
            conn.commit()
        env = {**ENABLED_QUEUE_ENV, "BNL_OWNER_USER_ID": "42", "BNL_PRIMARY_GUILD_ID": "77"}
        with mock.patch.object(bot, "DB_FILE", self.db_file), mock.patch.dict(os.environ, env):
            def read_turn():
                result_out = {}
                rendered = bot.build_conversation_context_v2_for_prompt(
                    guild_id=77, current_user_id=42, channel_id=9001, channel_name=channel_name,
                    channel_policy=policy, route_mode="normal_chat", conversation_surface=policy,
                    current_texts=(current,), current_participants={42}, is_batch=True,
                    current_message_ids={7400 + current_row_id}, is_direct_target=True, now=now, result_out=result_out)
                result = result_out["result"]
                basis = bot.build_conversation_prompt_source_basis(rendered, guild_id=77, current_user_id=42,
                    channel_id=9001, channel_name=channel_name, channel_policy=policy, context_result=result)
                selection = {}
                frame = self.frame(current, policy=policy)
                external_subject = getattr(self, "_current_external_subject", None)
                if external_subject is not None:
                    frame = replace(frame, subjects=(external_subject,))
                context = bot.build_tiktok_show_evidence_context_for_turn(
                    guild_id=77, subject_user_id=42, user_text=current, situation_frame=frame,
                    conversation_basis=basis, conversation_context_result=result, selection_out=selection)
                return result, basis, selection, context

            result, basis, selection, context = read_turn()
            diagnostic = (result.referent_status, result.referent_reason,
                          result.referent_selected_row_ids, selection.get("selection_user_text", ""))
            if getattr(self, "_unresolved_typed_submitter", False):
                self.assertEqual(result.referent_reason, "human_request_subset_chain", diagnostic)
                self.assertIsNone(selection.get("artist_identity_request"), diagnostic)
                self.assertNotIn("Neutral Signal", context)
                return
            if independent_topic:
                self.assertIsNone(selection.get("artist_identity_request"), diagnostic)
                self.assertNotIn("Neutral Signal", context)
                self.assertNotIn(403, result.referent_selected_row_ids)
                return
            self.assertIn("Neutral Signal", context, diagnostic)
            self.assertIn("test.submitter", context)
            self.assertIn("2026-09-11", context)
            self.assertNotIn("2026-10-02", context)
            self.assertNotIn("Neutral Counterfeit", context)
            if getattr(self, "_current_chain_question", ""):
                self.assertLess(len(context), 2000)
                self.assertIn("human request root", basis.rendered_context)
                self.assertIn("human request constraint", basis.rendered_context)
            self.assertTrue({403, subset_row_id}.issubset(set(result.referent_selected_row_ids)))
            self.assertTrue({403, subset_row_id}.issubset(set(basis.source_row_ids)))
            if saved_subset_answer:
                self.assertIn(405 + saved_archive_answer, result.referent_selected_row_ids)
            if saved_archive_answer:
                self.assertIn(404, result.referent_selected_row_ids)
            if getattr(self, "_saved_subset_answer_count", 1) > 1:
                self.assertIn(406 + saved_archive_answer, result.referent_selected_row_ids)
                if last_subset_answer_saved:
                    self.assertIn(407 + saved_archive_answer, result.referent_selected_row_ids)
            self.assertIn(subset, selection["selection_user_text"])
            self.assertIn(first, selection["selection_user_text"])
            artist_request = selection["artist_identity_request"]
            self.assertEqual(tuple(subject.user_id for subject in artist_request.frame_subjects), (42,))
            request = packet.IntelligencePacketRequest(guild_id=77, subject_user_id=42,
                route_mode="normal_chat", conversation_surface=policy, channel_policy=policy, user_text=current,
                show_episode_selection_text=selection["selection_user_text"],
                show_episode_artist_request=artist_request)
            with closing(sqlite3.connect(self.db_file)) as conn:
                items = packet._show_episode_items(conn, request, packet.IntelligencePacketDiagnostics(), [], environ=env)
            packet_text = "\n".join(item.text for item in items)
            self.assertIn("Neutral Signal", packet_text)
            self.assertIn("test.submitter", packet_text)
            self.assertIn("2026-09-11", packet_text)
            self.assertNotIn("2026-10-02", packet_text)
            self.assertNotIn("Neutral Counterfeit", packet_text)
            show_basis = bot.build_finalized_show_prompt_source_basis(context, guild_id=77, selection=selection)
            self.assertEqual(bot.prompt_source_basis_failure((basis, show_basis)), "")

            if getattr(self, "_current_chain_question", ""):
                saved_current = current
                for explicit_current in (
                    "What songs by Other Artist have been submitted?",
                    "What about 2026-09-25?", "Is the queue open?", "What about Mac Modem?",
                    "Which of those tracks by Another Artist was latest?",
                    "Keep this narrowed to Another Artist's music instead",
                    "Keep this narrowed to songs by Another Artist submitted in past shows.",
                    "Keep this narrowed to the latest one by Another Artist that was submitted.",
                ):
                    with self.subTest(current_override=explicit_current):
                        current = explicit_current
                        self._current_external_subject = assessment.SituationSubjectReference(
                            entity_ref="another_artist", binding_method="existing_typed_entity")
                        _result, _basis, override_selection, _context = read_turn()
                        override_request = override_selection.get("artist_identity_request")
                        self.assertFalse(override_request is not None and any(
                            subject.user_id == 42 for subject in override_request.frame_subjects))
                current = saved_current
                self._current_external_subject = None

            # A newer dependent date replaces the older date for retrieval;
            # both original human turns remain governed context.
            dated_root = "Were my songs submitted on 2026-09-11?"
            dated_subset = "Which of those were in the 2026-09-25 show?"
            with closing(sqlite3.connect(self.db_file)) as conn:
                conn.execute("UPDATE conversations SET content=? WHERE id=403", (dated_root,))
                conn.execute("UPDATE conversations SET content=? WHERE id=?", (dated_subset, subset_row_id))
                conn.commit()
            dated_result, dated_basis, dated_selection, dated_context = read_turn()
            self.assertTrue({403, subset_row_id}.issubset(set(dated_result.referent_selected_row_ids)))
            self.assertIn(dated_root, dated_basis.rendered_context)
            self.assertIn(dated_subset, dated_basis.rendered_context)
            self.assertIn("Neutral Collaboration", dated_context)
            self.assertIn("2026-09-25", dated_context)
            self.assertNotIn("Neutral Signal", dated_context)
            dated_request = packet.IntelligencePacketRequest(guild_id=77, subject_user_id=42,
                route_mode="normal_chat", conversation_surface=policy, channel_policy=policy, user_text=current,
                show_episode_selection_text=dated_selection["selection_user_text"],
                show_episode_artist_request=dated_selection["artist_identity_request"])
            with closing(sqlite3.connect(self.db_file)) as conn:
                dated_items = packet._show_episode_items(
                    conn, dated_request, packet.IntelligencePacketDiagnostics(), [], environ=env)
            dated_text = "\n".join(item.text for item in dated_items)
            self.assertIn("Neutral Collaboration", dated_text)
            self.assertIn("2026-09-25", dated_text)
            self.assertNotIn("Neutral Signal", dated_text)
            with closing(sqlite3.connect(self.db_file)) as conn:
                conn.execute("UPDATE conversations SET content=? WHERE id=403", (first,))
                conn.execute("UPDATE conversations SET content=? WHERE id=?", (subset, subset_row_id))
                conn.commit()

            # A distinct latest request cannot lend the older artist scope.
            with closing(sqlite3.connect(self.db_file)) as conn:
                conn.execute("UPDATE conversations SET content=? WHERE id=?", ("Which lamps fit a desk?", subset_row_id))
                conn.commit()
            _result, _basis, changed_selection, changed_context = read_turn()
            self.assertIsNone(changed_selection.get("artist_identity_request"))
            if not getattr(self, "_current_chain_question", ""):
                self.assertNotIn("Neutral Signal", changed_context)
            self.assertNotEqual(bot.prompt_source_basis_failure((basis, show_basis)), "")

            # Correcting or withdrawing the human root invalidates the saved chain.
            with closing(sqlite3.connect(self.db_file)) as conn:
                conn.execute("UPDATE conversations SET content=? WHERE id=?", (subset, subset_row_id))
                conn.execute("UPDATE conversations SET content=? WHERE id=403", ("What instruments does Test Quartet use?",))
                conn.commit()
            self.assertNotEqual(bot.prompt_source_basis_failure((basis, show_basis)), "")
            _result, _basis, corrected_selection, corrected_context = read_turn()
            self.assertIsNone(corrected_selection.get("artist_identity_request"))
            if not getattr(self, "_current_chain_question", ""):
                self.assertNotIn("Neutral Signal", corrected_context)
            with closing(sqlite3.connect(self.db_file)) as conn:
                conn.execute("DELETE FROM conversations WHERE id=403")
                conn.commit()
            self.assertNotEqual(bot.prompt_source_basis_failure((basis, show_basis)), "")
            _result, _basis, withdrawn_selection, withdrawn_context = read_turn()
            self.assertIsNone(withdrawn_selection.get("artist_identity_request"))
            if not getattr(self, "_current_chain_question", ""):
                self.assertNotIn("Neutral Signal", withdrawn_context)

    def test_public_context_unstored_answer_chain_preserves_artist_and_submitter_scope(self):
        self._flow_policy = "public_context"
        self.test_raw_unstored_answer_chain_preserves_artist_and_submitter_scope()

    def test_retained_subset_answer_preserves_original_human_source_chain(self):
        self._flow_policy = "public_context"
        self._saved_subset_answer = True
        self.test_raw_unstored_answer_chain_preserves_artist_and_submitter_scope()

    def test_retained_answer_invented_artist_and_date_cannot_retarget_sources(self):
        self._saved_subset_answer_text = (
            "Test Submitter sent Neutral Counterfeit, credited to Different Artist, for the October 2 show.")
        self.test_retained_subset_answer_preserves_original_human_source_chain()

    def test_multiple_retained_subset_answers_keep_middle_human_constraint(self):
        self._saved_subset_answer_count = 2
        self.test_retained_answer_invented_artist_and_date_cannot_retarget_sources()

    def test_retained_archive_answer_keeps_original_human_root(self):
        self._saved_archive_answer = True
        self.test_multiple_retained_subset_answers_keep_middle_human_constraint()

    def test_latest_unpaired_subset_keeps_earlier_retained_constraints(self):
        self._last_subset_answer_saved = False
        self.test_multiple_retained_subset_answers_keep_middle_human_constraint()

    def test_completed_independent_topic_blocks_old_artist_chain(self):
        self._independent_topic = True
        self.test_multiple_retained_subset_answers_keep_middle_human_constraint()

    def test_scoped_clarification_keeps_original_artist_source_chain(self):
        self._current_chain_question = (
            "Yes, keep this narrowed to Test Submitter's submissions. "
            "What's the artist credit and date for his latest one?")
        self.test_multiple_retained_subset_answers_keep_middle_human_constraint()

    def test_same_submission_credit_keeps_original_artist_source_chain(self):
        self._current_chain_question = "What artist credit is attached to that same submission?"
        self.test_multiple_retained_subset_answers_keep_middle_human_constraint()

    def test_typed_external_subject_still_requires_role_disambiguation(self):
        self._unresolved_typed_submitter = True
        self._current_external_subject = assessment.SituationSubjectReference(
            user_id=99, label_hint="Test Submitter", binding_method="existing_typed_target")
        self.test_scoped_clarification_keeps_original_artist_source_chain()

    def test_raw_subset_context_preserves_ambiguity_and_current_payload(self):
        from tests import test_conversation_context_v2 as fixture
        row, req = fixture.row, fixture.req
        text = "Which of those songs did Test Submitter submit?"
        human = row(1, "user", "Which of my recordings appeared in past shows?", name="Test Member")
        answer = row(2, "model", "Two retained recordings appeared.", name="BNL-01")
        cases = (
            ("multiple requests", [human, row(2, "user", "Which other artist's recordings appeared?")], text),
            ("other author", [row(1, "user", human["content"], user=2)], text),
            ("other room", [dict(human, channel_id=11)], text),
            ("expired", [dict(human, timestamp=(fixture.NOW - timedelta(minutes=11)).isoformat())], text),
            ("orphan model", [answer], text),
            ("current payload", [human, answer], "Which of these options: Blue Lamp or Green Desk?"),
        )
        for label, rows, current in cases:
            with self.subTest(boundary=label):
                result = context_owner.assemble_conversation_context_v2(rows, req(current_texts=(current,)))
                self.assertNotEqual(result.referent_status, "resolved")
        changed_topic = context_owner.assemble_conversation_context_v2(
            [human, answer, row(3, "user", "New topic: help me move this desk.")],
            req(current_texts=(text,)))
        self.assertNotIn(1, changed_topic.referent_selected_row_ids)
        self.assertNotIn(1, changed_topic.referent_request_row_ids)
        exact = context_owner.assemble_conversation_context_v2(
            [human, answer, row(3, "user", "Different question.")],
            req(current_texts=(text,), referenced_conversation_row_ids=frozenset({1})))
        self.assertEqual(exact.referent_reason, "discord_reply_source")
        self.assertEqual(exact.referent_selected_row_ids, (1,))
