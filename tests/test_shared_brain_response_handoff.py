"""Response handoffs retain task support, normal prose and attempt accounting."""
import json
import os
import sqlite3
import tempfile
from contextlib import ExitStack
from dataclasses import replace
from types import SimpleNamespace
import unittest
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl01_bot as bot
import bnl_shared_brain_synthesis as synthesis
import test_conversation_batching as batch_fixtures
import test_ordinary_chat_single_packet_canary as packet_fixtures


class SharedBrainResponseHandoffTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        debug_state = mock.patch.dict(bot.LAST_ROUTE_DEBUG, {}, clear=True)
        debug_state.start()
        self.addCleanup(debug_state.stop)
        self.fixture = packet_fixtures.OrdinaryChatSinglePacketCanaryTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        self.request = "Explain why a checksum detects corruption but cannot repair the file."
        self.answer = "A checksum can reveal changed data, but it lacks the original information needed to reconstruct the file."
        _frame, self.basis = self.fixture._basis_with_authority_frame(self.request)
        self.prompt = synthesis.build_packet_owned_prompt(
            "Current user request: " + self.request,
            self.basis,
        ).prompt
        self.channel = SimpleNamespace(id=10, name="bnl-testing")
        self.envelope = {
            "tasks": [
                {
                    "taskId": plan.task_id,
                    "text": self.answer,
                    "supportKind": plan.support_kind,
                    "evidenceIds": list(plan.evidence_ids),
                }
                for plan in synthesis.ordinary_chat_task_support_plan(self.basis)
            ]
        }
        contract = synthesis.parse_ordinary_chat_response_contract(json.dumps(self.envelope))
        self.assertTrue(synthesis.validate_ordinary_chat_response_contract(self.basis, contract).valid)

    async def test_invalid_repair_contract_gets_valid_rewrite_with_all_attempts(self):
        wrong_task = json.loads(json.dumps(self.envelope))
        wrong_task["tasks"][0]["taskId"] = "T2"
        wrong_task["tasks"][0]["text"] = "An unrelated task replaced the checksum explanation."
        wrong_evidence = json.loads(json.dumps(self.envelope))
        wrong_evidence["tasks"][0]["evidenceIds"] = ["E2"]
        wrong_evidence["tasks"][0]["text"] = "An unrelated reference replaced the checksum explanation."
        for invalid in (json.dumps(wrong_task), json.dumps(wrong_evidence), '{"tasks":[{"taskId":'):
            with self.subTest(invalid=invalid):
                provider = mock.AsyncMock(side_effect=(
                    bot.TrackedGenerationResponse(text=invalid, provider_call_count=2),
                    bot.TrackedGenerationResponse(text=json.dumps(self.envelope), provider_call_count=1),
                ))
                diagnostics = {
                    "suppressed": True,
                    "suppression_reason": "single_packet_candidate_rejected",
                    "response_review_requires_rewrite": True,
                }
                with mock.patch.object(bot, "get_tracked_gemini_response_with_optional_typing", new=provider):
                    response, _prompt, bases, calls, source_neutral = await bot.resolve_guarded_response_obligation(
                        "", baseline_response="", prompt=self.prompt,
                        current_user_text=self.request, diagnostics=diagnostics,
                        route_mode=bot.ROUTE_MODE_NORMAL_CHAT, channel_policy="public_context",
                        user_id=7, guild_id=1, channel=self.channel,
                        prompt_source_bases=(self.basis,), source_context_available=True,
                    )
                self.assertEqual(self.answer, response, diagnostics)
                self.assertEqual(2, provider.await_count)
                self.assertEqual(3, calls)
                self.assertEqual((self.basis,), bases)
                self.assertFalse(source_neutral)
                self.assertFalse(diagnostics["suppressed"])
                self.assertNotIn('"tasks"', response)

    async def test_guard_repair_unwraps_packet_schema_for_direct_and_batch(self):
        blocker = "I can’t ground that answer cleanly in the current scope. Give me one specific target or question and I’ll take another pass."
        for batch_id in (None, 42):
            with self.subTest(batch_id=batch_id):
                raw = json.dumps(self.envelope)
                tracked = mock.AsyncMock(return_value=bot.TrackedGenerationResponse(text=raw, provider_call_count=2))
                plain = mock.AsyncMock(return_value=raw)
                with (
                    mock.patch.object(bot, "get_tracked_gemini_response_with_optional_typing", new=tracked),
                    mock.patch.object(bot, "get_gemini_response_with_optional_typing", new=plain),
                    mock.patch.object(bot, "get_gemini_response", new=plain),
                    mock.patch.object(bot, "refresh_prompt_source_bases", return_value=(self.prompt, (self.basis,), (), False)),
                    mock.patch.object(bot, "prompt_source_basis_failure", return_value=""),
                ):
                    response, diagnostics = await bot.apply_guarded_response_regeneration(
                        blocker, prompt=self.prompt, user_id=7, guild_id=1,
                        route_mode=bot.ROUTE_MODE_NORMAL_CHAT, channel_policy="public_context",
                        current_user_text=self.request, generation_route=bot.ORDINARY_CHAT_SINGLE_PACKET_ROUTE,
                        channel=self.channel, source_context_available=True,
                        prompt_source_bases=(self.basis,), batch_generation_id=batch_id,
                    )
                self.assertEqual(self.answer, response, diagnostics)
                self.assertFalse(diagnostics["suppressed"])
                self.assertNotIn('"tasks"', response)
                self.assertEqual(2, diagnostics["ordinary_chat_repair_provider_call_count"])

    async def test_natural_repair_keeps_authorized_basis_and_source_neutral_route_has_no_schema(self):
        for reason, neutral in (
            ("single_packet_candidate_rejected", False),
            ("source_basis_changed", True),
        ):
            with self.subTest(reason=reason):
                provider = mock.AsyncMock(return_value=bot.TrackedGenerationResponse(
                    text=self.answer, provider_call_count=1,
                ))
                accounting = {}
                with mock.patch.object(bot, "get_tracked_gemini_response_with_optional_typing", new=provider):
                    response, prompt, bases, calls, source_neutral = await bot.regenerate_ordinary_chat_response_obligation(
                        channel=self.channel, prompt=self.prompt, reason=reason,
                        prompt_source_bases=(self.basis,), user_id=7, guild_id=1,
                        source_context_available=True, current_user_text=self.request,
                        generation_accounting=accounting,
                    )
                self.assertEqual(self.answer, response)
                self.assertEqual(1, calls)
                self.assertEqual(neutral, source_neutral)
                self.assertEqual(() if neutral else (self.basis,), bases)
                self.assertEqual("natural_prose", accounting["final_repair_contract_status"])
                self.assertEqual(1, accounting["corrective_call_count"])
                route = provider.await_args.kwargs["route"]
                if neutral:
                    self.assertEqual(bot.ORDINARY_CHAT_RESPONSE_REPAIR_ROUTE, route)
                    self.assertFalse(provider.await_args.kwargs["source_context_available"])
                    self.assertNotIn("PACKET-OWNED RESPONSE CONTRACT:", prompt)
                    config = bot._generation_config_for_model("gemini-2.5-flash", route)
                    self.assertIsNone(config.response_json_schema)
                    self.assertIsNone(config.response_mime_type)
                    self.assertEqual(
                        bot.policy_for_route(bot.ORDINARY_CHAT_SINGLE_PACKET_ROUTE),
                        bot.policy_for_route(route),
                    )
                else:
                    self.assertEqual(bot.ORDINARY_CHAT_SINGLE_PACKET_ROUTE, route)
                    self.assertTrue(provider.await_args.kwargs["source_context_available"])

    async def test_final_receipt_retains_primary_and_repair_physical_usage(self):
        # Use real source revalidation, primary execution, repair decoding and
        # finalization; only the external provider is replaced.
        request = self.fixture.text
        answer = "Your favorite movie is Arrival."
        envelope = {
            "tasks": [
                {"taskId": plan.task_id, "text": answer,
                 "supportKind": plan.support_kind, "evidenceIds": list(plan.evidence_ids)}
                for plan in synthesis.ordinary_chat_task_support_plan(self.fixture.basis)
            ]
        }
        primary = bot.TrackedGenerationResponse(
            text='{"tasks":', provider_call_count=1,
            total_tokens=11, prompt_tokens=7, candidate_tokens=3,
            thought_tokens=1, cached_tokens=2, estimated_cost_nanos=100,
            cost_priced=True,
        )
        repair = bot.TrackedGenerationResponse(
            text=json.dumps(envelope), provider_call_count=2,
            total_tokens=23, prompt_tokens=13, candidate_tokens=7,
            thought_tokens=3, cached_tokens=4, estimated_cost_nanos=200,
            cost_priced=True,
        )
        provider = mock.AsyncMock(side_effect=(primary, repair))
        self.fixture.conn.commit()
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "receipt.db")
            with sqlite3.connect(path) as conn:
                self.fixture.conn.backup(conn)
            with (
                mock.patch.object(bot, "DB_FILE", path),
                mock.patch.object(bot, "get_tracked_gemini_response_with_optional_typing", new=provider),
            ):
                execution = await bot.maybe_generate_ordinary_chat_single_packet(
                    channel=self.channel, prompt="Current user request: " + request,
                    basis=self.fixture.basis, scope_applied=True, preflight_reason="",
                    situation_frame=self.fixture.frame, situation_frame_current_text=request,
                    route_mode=bot.ROUTE_MODE_NORMAL_CHAT, channel_policy="public_context",
                    conversation_surface="mention_or_reply", user_id=7, guild_id=1,
                    user_display_name="Test Member", source_context_available=True,
                )
                self.assertIsNotNone(execution)
                self.assertFalse(execution.candidate_active)
                self.assertEqual("", execution.response)
                accounting = bot._ordinary_chat_run_accounting(execution.decision)
                self.assertEqual(1, accounting["provider_call_count"])
                diagnostics = {"suppressed": True, "suppression_reason": execution.review_reason,
                               "response_review_requires_rewrite": True}
                response, _prompt, bases, calls, source_neutral = await bot.resolve_guarded_response_obligation(
                    execution.response, baseline_response="", prompt=execution.prompt,
                    current_user_text=request, diagnostics=diagnostics,
                    route_mode=bot.ROUTE_MODE_NORMAL_CHAT, channel_policy="public_context",
                    user_id=7, guild_id=1, channel=self.channel,
                    prompt_source_bases=execution.prompt_source_bases,
                    source_context_available=True, generation_accounting=accounting,
                )
                self.assertEqual(answer, response)
                self.assertEqual(2, calls)
                self.assertFalse(source_neutral)
                self.assertEqual(execution.prompt_source_bases, bases)
                with sqlite3.connect(path) as conn:
                    self.assertTrue(synthesis.finalize_run(
                        conn, execution.decision, final_response=response,
                        response_sent=True, candidate_live=False, guard_status="response_obligation_rewrite_sent",
                    ))
                    row = conn.execute(
                        "SELECT provider_call_count,corrective_call_count,candidate_total_tokens,"
                        "turn_generation_usage_json,response_sent FROM memory_governance_shared_brain_synthesis_runs "
                        "WHERE run_id=?", (execution.decision.run.run_id,),
                    ).fetchone()
        self.assertEqual(2, provider.await_count)
        self.assertEqual((3, 2, 11), row[:3])
        self.assertEqual(1, row[4])
        usage = json.loads(row[3])
        for key, expected in {
            "provider_call_count": 3, "corrective_call_count": 2,
            "generation_count": 2, "repair_generation_count": 1,
            "total_tokens": 34, "prompt_tokens": 20, "output_tokens": 10,
            "thought_tokens": 4, "cached_tokens": 6, "estimated_cost_nanos": 300,
            "final_repair_contract_status": "valid", "cost_priced": True,
        }.items():
            self.assertEqual(expected, usage[key], key)
        self.assertGreaterEqual(usage["generation_latency_ms"], 0)
        self.assertNotIn(request, row[3])
        self.assertNotIn(answer, row[3])

    async def test_public_explanation_recovers_with_full_context_and_delivered_receipt(self):
        # The real typed reviewer currently mistakes generic "your file" for
        # an unsupported personal claim. Exercise generation, the existing
        # response obligation, final source/Frame guards, Discord send and the
        # persisted receipt together. Only the external transports are fake.
        answer = (
            "A checksum detects when your file has changed, but it cannot "
            "reconstruct the original bytes."
        )
        frame, basis = self.fixture._basis_with_authority_frame(self.request)
        basis = synthesis.build_ordinary_chat_basis(
            guild_id=1, user_id=7, channel_id=10, route_mode="normal_chat",
            channel_policy="public_context", current_direct=True,
            user_text=self.request, packet=basis.packet,
            assessment=basis.assessment, environ=self.fixture.flags,
        )
        envelope = {
            "tasks": [
                {"taskId": plan.task_id, "text": answer,
                 "supportKind": plan.support_kind,
                 "evidenceIds": list(plan.evidence_ids)}
                for plan in synthesis.ordinary_chat_task_support_plan(basis)
            ]
        }
        original_prompt = (
            "Current user request: " + self.request
            + "\nConversation context: Test Member is discussing file integrity."
            + "\nBNL voice: explain the useful distinction naturally."
        )

        async def provider_reply(_channel, _prompt, _user_id, _guild_id, **kwargs):
            return bot.TrackedGenerationResponse(
                text=(answer if kwargs["route"] == bot.ORDINARY_CHAT_RESPONSE_REPAIR_ROUTE
                      else json.dumps(envelope)),
                provider_call_count=1, total_tokens=17,
            )

        provider = mock.AsyncMock(side_effect=provider_reply)
        message = SimpleNamespace(
            author=SimpleNamespace(id=7, display_name="Test Member"),
            guild=SimpleNamespace(id=1), channel=self.channel,
            content=self.request, attachments=(), embeds=(),
            reply=mock.AsyncMock(return_value=SimpleNamespace(id=302)),
        )
        plan = bot.plan_conversation_response(
            self.request, "sealed_test", route_mode=bot.ROUTE_MODE_NORMAL_CHAT,
            real_direct_target=False, batching_enabled=False,
            conversation_surface="free_speak_sealed_mirror",
        )
        self.fixture.conn.commit()
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "public-repair.db")
            with sqlite3.connect(path) as conn:
                self.fixture.conn.backup(conn)
            with (
                mock.patch.object(bot, "DB_FILE", path),
                mock.patch.object(bot, "get_tracked_gemini_response_with_optional_typing", new=provider),
            ):
                execution = await bot.maybe_generate_ordinary_chat_single_packet(
                    channel=self.channel, prompt=original_prompt, basis=basis,
                    scope_applied=True, preflight_reason="", situation_frame=frame,
                    situation_frame_current_text=self.request,
                    route_mode=bot.ROUTE_MODE_NORMAL_CHAT, channel_policy="sealed_test",
                    conversation_surface="free_speak_sealed_mirror", user_id=7,
                    guild_id=1, user_display_name="Test Member", source_context_available=True,
                )
                self.assertIsNotNone(execution)
                self.assertEqual("typed_contract_task_text_unsupported", execution.review_reason)
                await bot.send_planned_conversation_response(
                    message, execution.response, plan, prompt=execution.prompt,
                    generation_route=bot.ORDINARY_CHAT_SINGLE_PACKET_ROUTE,
                    source_context_available=True, allow_model_save=False,
                    mark_recent_direct=False, prompt_source_bases=execution.prompt_source_bases,
                    situation_frame=frame, situation_frame_current_text=self.request,
                    ordinary_chat_single_packet_execution=execution,
                )
                with sqlite3.connect(path) as conn:
                    row = conn.execute(
                        "SELECT provider_call_count,corrective_call_count,response_sent,"
                        "turn_generation_usage_json,guard_status "
                        "FROM memory_governance_shared_brain_synthesis_runs WHERE run_id=?",
                        (execution.decision.run.run_id,),
                    ).fetchone()
        message.reply.assert_awaited_once()
        self.assertEqual(answer, message.reply.await_args.args[0])
        self.assertEqual(2, provider.await_count)
        repair_call = provider.await_args_list[1]
        self.assertEqual(bot.ORDINARY_CHAT_RESPONSE_REPAIR_ROUTE, repair_call.kwargs["route"])
        self.assertTrue(repair_call.kwargs["source_context_available"])
        repair_prompt = repair_call.args[1]
        self.assertIn(original_prompt, repair_prompt)
        self.assertIn(basis.rendered_context, repair_prompt)
        self.assertEqual((2, 1, 1), row[:3])
        self.assertEqual("single_packet_repaired_response_sent", row[4])
        self.assertEqual("natural_prose", bot.LAST_ROUTE_DEBUG["ordinary_chat_final_repair_status"])
        self.assertEqual(row[4], bot.LAST_ROUTE_DEBUG["ordinary_chat_final_guard_status"])
        self.assertTrue(bot.LAST_ROUTE_DEBUG["ordinary_chat_response_sent"])
        usage = json.loads(row[3])
        self.assertEqual(34, usage["total_tokens"])
        self.assertEqual("natural_prose", usage["final_repair_contract_status"])

    async def test_public_prose_recovery_still_rejects_invalid_internal_envelopes(self):
        invalid_envelopes = []
        for field, value in (("taskId", "T2"), ("evidenceIds", ["E2"])):
            invalid = json.loads(json.dumps(self.envelope))
            invalid["tasks"][0][field] = value
            invalid_envelopes.append((field, json.dumps(invalid)))
        invalid_envelopes.extend((
            ("extra_leading_key", json.dumps({"status": "ok", **self.envelope})),
            ("reordered_internal_keys", json.dumps({"status": "ok", "tasks": [{
                "text": self.answer, "evidenceIds": ["PUBLIC"],
                "supportKind": "external_public", "taskId": "T1",
            }]})),
            ("truncated_reordered_internal_keys", '{"status":"ok","tasks":[{"text":"An answer","evidenceIds":['),
        ))
        for case, invalid in invalid_envelopes:
            with self.subTest(case=case):
                provider = mock.AsyncMock(side_effect=(
                    bot.TrackedGenerationResponse(text=invalid, provider_call_count=1),
                    bot.TrackedGenerationResponse(text=self.answer, provider_call_count=1),
                ))
                diagnostics = {
                    "suppressed": True,
                    "suppression_reason": "typed_contract_task_text_unsupported",
                    "response_review_requires_rewrite": True,
                }
                with mock.patch.object(bot, "get_tracked_gemini_response_with_optional_typing", new=provider):
                    response, _prompt, bases, calls, neutral = await bot.resolve_guarded_response_obligation(
                        "", baseline_response="", prompt=self.prompt,
                        current_user_text=self.request, diagnostics=diagnostics,
                        route_mode=bot.ROUTE_MODE_NORMAL_CHAT, channel_policy="sealed_test",
                        user_id=7, guild_id=1, channel=self.channel,
                        prompt_source_bases=(self.basis,), source_context_available=True,
                    )
                self.assertEqual(self.answer, response)
                self.assertEqual(2, calls)
                self.assertEqual((self.basis,), bases)
                self.assertFalse(neutral)
                self.assertEqual(
                    [bot.ORDINARY_CHAT_RESPONSE_REPAIR_ROUTE] * 2,
                    [call.kwargs["route"] for call in provider.await_args_list],
                )
        for natural_json in ('{"status":"ok"}', '{"tasks":["first","second"]}'):
            with self.subTest(natural_json=natural_json):
                provider = mock.AsyncMock(return_value=bot.TrackedGenerationResponse(
                    text=natural_json, provider_call_count=1,
                ))
                with mock.patch.object(bot, "get_tracked_gemini_response_with_optional_typing", new=provider):
                    response, _prompt, bases, calls, neutral = await bot.regenerate_ordinary_chat_response_obligation(
                        channel=self.channel, prompt=self.prompt,
                        reason="typed_contract_task_text_unsupported",
                        prompt_source_bases=(self.basis,), user_id=7, guild_id=1,
                        source_context_available=True, current_user_text=self.request,
                    )
                self.assertEqual(natural_json, response)
                self.assertEqual(1, calls)
                self.assertEqual((self.basis,), bases)
                self.assertFalse(neutral)
                self.assertEqual(bot.ORDINARY_CHAT_RESPONSE_REPAIR_ROUTE, provider.await_args.kwargs["route"])

    async def test_passive_batch_public_repair_reaches_send_and_final_receipt(self):
        batch = batch_fixtures.ConversationBatchCoordinatorTests()
        await batch.asyncSetUp()
        self.addAsyncCleanup(batch.asyncTearDown)
        channel = batch._channel(10)
        channel.guild = batch_fixtures.FakeGuild(1)
        batch._prime_flush(channel, self.request)
        bot._channel_buffers[channel.id][-1] = ("Test Member", self.request, 7)
        answer = "A checksum detects when your file has changed, but it cannot reconstruct the original bytes."
        envelope = {"tasks": [{"taskId": "T1", "text": answer,
                                "supportKind": "external_public", "evidenceIds": ["PUBLIC"]}]}

        async def provider_reply(_channel, _prompt, _user_id, _guild_id, **kwargs):
            return bot.TrackedGenerationResponse(
                text=answer if kwargs["route"] == bot.ORDINARY_CHAT_RESPONSE_REPAIR_ROUTE else json.dumps(envelope),
                provider_call_count=1, total_tokens=19,
            )

        provider = mock.AsyncMock(side_effect=provider_reply)
        real_guard = bot.apply_guarded_response_regeneration
        real_authority_claim = bot._contains_unsupported_source_authority_claim
        real_debug = bot.update_last_route_debug
        self.fixture.conn.commit()
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "batch-public-repair.db")
            with sqlite3.connect(path) as conn:
                self.fixture.conn.backup(conn)
            with mock.patch.object(bot, "DB_FILE", path):
                bot.init_db()
            with (
                batch._flush_runtime(channel.id, AssertionError("Legacy generation must not replace the packet route")),
                mock.patch.object(bot, "DB_FILE", path),
                mock.patch.dict(os.environ, {"BNL_TESTING_CHANNEL_ID": "10"}),
                mock.patch.object(bot, "get_tracked_gemini_response_with_optional_typing", new=provider),
                mock.patch.object(bot, "apply_guarded_response_regeneration", new=real_guard),
                mock.patch.object(bot, "_contains_unsupported_source_authority_claim", new=real_authority_claim),
                mock.patch.object(bot, "update_last_route_debug", new=real_debug),
                mock.patch.object(bot, "maybe_build_bnl_read_model_context", return_value=""),
                mock.patch.object(bot, "build_tiktok_show_evidence_context_for_turn", return_value=""),
                mock.patch.object(bot, "render_community_visual_basis_for_prompt", return_value=""),
            ):
                await bot._flush_channel_buffer(channel)
                with sqlite3.connect(path) as conn:
                    row = conn.execute(
                        "SELECT provider_call_count,corrective_call_count,response_sent,"
                        "turn_generation_usage_json,guard_status "
                        "FROM memory_governance_shared_brain_synthesis_runs ORDER BY rowid DESC LIMIT 1"
                    ).fetchone()
        self.assertEqual([answer], channel.sent)
        self.assertEqual(2, provider.await_count)
        self.assertEqual(bot.ORDINARY_CHAT_RESPONSE_REPAIR_ROUTE, provider.await_args.kwargs["route"])
        self.assertEqual((2, 1, 1), row[:3])
        self.assertEqual("batch_single_packet_repaired_response_sent", row[4])
        self.assertEqual("natural_prose", bot.LAST_ROUTE_DEBUG["ordinary_chat_final_repair_status"])
        self.assertEqual(row[4], bot.LAST_ROUTE_DEBUG["ordinary_chat_final_guard_status"])
        self.assertTrue(bot.LAST_ROUTE_DEBUG["ordinary_chat_response_sent"])
        usage = json.loads(row[3])
        self.assertEqual(38, usage["total_tokens"])
        self.assertEqual("natural_prose", usage["final_repair_contract_status"])

    async def test_member_and_mixed_task_repairs_keep_typed_support(self):
        mixed_request = (
            "What do you remember about me? Also explain why a checksum detects corruption."
        )
        frame, mixed_basis = self.fixture._basis_with_authority_frame(
            mixed_request, subject_user_ids=(7,),
        )
        mixed_basis = replace(
            mixed_basis, assessment=replace(mixed_basis.assessment, situation_frame=frame),
        )
        self.assertEqual(
            ["packet", "external_public"],
            [plan.support_kind for plan in synthesis.ordinary_chat_task_support_plan(mixed_basis)],
        )
        for request, basis in ((self.fixture.text, self.fixture.basis), (mixed_request, mixed_basis)):
            with self.subTest(request=request):
                plans = synthesis.ordinary_chat_task_support_plan(basis)
                envelope = {"tasks": [
                    {"taskId": plan.task_id, "supportKind": plan.support_kind,
                     "evidenceIds": list(plan.evidence_ids),
                     "text": "Your favorite movie is Arrival." if plan.support_kind == "packet" else self.answer}
                    for plan in plans
                ]}
                provider = mock.AsyncMock(return_value=bot.TrackedGenerationResponse(
                    text=json.dumps(envelope), provider_call_count=1,
                ))
                prompt = synthesis.build_packet_owned_prompt("Current user request: " + request, basis).prompt
                with mock.patch.object(bot, "get_tracked_gemini_response_with_optional_typing", new=provider):
                    response, repair_prompt, bases, calls, neutral = await bot.regenerate_ordinary_chat_response_obligation(
                        channel=self.channel, prompt=prompt,
                        reason="typed_contract_task_text_unsupported", prompt_source_bases=(basis,),
                        user_id=7, guild_id=1, source_context_available=True,
                        current_user_text=request,
                    )
                self.assertIn("Your favorite movie is Arrival.", response)
                self.assertEqual(bot.ORDINARY_CHAT_SINGLE_PACKET_ROUTE, provider.await_args.kwargs["route"])
                self.assertIn(synthesis.render_ordinary_chat_task_contract(basis), repair_prompt)
                self.assertEqual((basis,), bases)
                self.assertEqual(1, calls)
                self.assertFalse(neutral)

    async def test_schema_free_repair_keeps_requested_code_and_json(self):
        for request, answer in (
            ("Give me a Python print example.", '```python\nprint("Hello, BARCODE")\n```'),
            ("Return a JSON status object.", '{"status": "ok"}'),
            ("Write an intro marked with [Intro].", "[Intro]\nWelcome back to BARCODE."),
        ):
            with self.subTest(request=request):
                provider = mock.AsyncMock(return_value=bot.TrackedGenerationResponse(
                    text=answer, provider_call_count=1,
                ))
                with mock.patch.object(bot, "get_tracked_gemini_response_with_optional_typing", new=provider):
                    response, _prompt, bases, calls, source_neutral = await bot.regenerate_ordinary_chat_response_obligation(
                        channel=self.channel, prompt="Current user request: " + request,
                        reason="source_basis_changed", prompt_source_bases=(self.basis,),
                        user_id=7, guild_id=1, source_context_available=True,
                        current_user_text=request,
                    )
                self.assertEqual(answer, response)
                self.assertEqual((), bases)
                self.assertEqual(1, calls)
                self.assertTrue(source_neutral)

    def test_passive_prompt_preserves_directness_through_real_assessment_and_basis(self):
        self.fixture.conn.commit()
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "handoff.db")
            with sqlite3.connect(path) as conn:
                self.fixture.conn.backup(conn)
            with ExitStack() as stack:
                stack.enter_context(mock.patch.object(bot, "DB_FILE", path))
                stack.enter_context(mock.patch.dict(os.environ, {
                    "BNL_TESTING_CHANNEL_ID": "10",
                    "BNL_ORDINARY_CHAT_SINGLE_PACKET_CHANNEL_IDS": "10",
                }, clear=False))
                for name, value in {
                    "get_user_profile": ("Test Member", ""),
                    "should_allow_greeting": False,
                    "choose_response_style": ("balanced", "Respond naturally."),
                    "build_user_memory_context": "",
                    "build_conversation_prompt_source_basis": None,
                    "build_broadcast_memory_context": "",
                    "build_queue_artist_memory_context": "",
                    "build_tiktok_show_evidence_context_for_turn": "",
                    "build_community_visual_basis": SimpleNamespace(status="not_requested"),
                    "render_community_visual_basis_for_prompt": "",
                }.items():
                    stack.enter_context(mock.patch.object(bot, name, return_value=value))
                assessment_builder = stack.enter_context(mock.patch.object(
                    bot, "build_unified_response_assessment_shadow",
                    wraps=bot.build_unified_response_assessment_shadow,
                ))
                basis_builder = stack.enter_context(mock.patch.object(
                    bot, "build_ordinary_chat_basis", wraps=bot.build_ordinary_chat_basis,
                ))
                metadata = {}
                bot.build_user_aware_prompt(
                    7, 1, "Test Member", self.request,
                    channel_name="bnl-testing", channel_id=10,
                    channel_policy="sealed_test", route_mode=bot.ROUTE_MODE_NORMAL_CHAT,
                    is_direct_interaction=False, prompt_metadata=metadata,
                )
            self.assertFalse(assessment_builder.call_args.kwargs.get("current_direct", True))
            self.assertFalse(basis_builder.call_args.kwargs["current_direct"])
            self.assertEqual("indirect", basis_builder.call_args.kwargs["packet"].request.direct_state)
            self.assertIsNotNone(metadata["ordinary_chat_single_packet_basis"])
            self.assertEqual("", metadata["ordinary_chat_single_packet_preflight_reason"])


if __name__ == "__main__":
    unittest.main()
