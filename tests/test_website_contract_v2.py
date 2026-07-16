import asyncio
import json
import os
os.environ.setdefault("GEMINI_API_KEY", "test-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-token")
import tempfile
import unittest
import urllib.error
from unittest import mock

import bnl01_bot
import bnl_website_contract_v2 as c
import bnl_website_relay_state as state


class Resp:
    def __init__(self, code, body):
        self.status = code
        self.body = body
    def read(self):
        return self.body
    def getcode(self):
        return self.status
    def __enter__(self):
        return self
    def __exit__(self, *args):
        return False


def accepted_body(envelope, *, idempotent=False, override=None):
    relay = dict(envelope["relay"])
    relay["contractVersion"] = 2
    relay["publishedAt"] = "2026-07-15T00:00:00Z"
    if override:
        relay.update(override)
    return json.dumps({"ok": True, "relay": relay, "idempotent": idempotent, "persisted": True}).encode()


class ContractV2Tests(unittest.TestCase):
    def test_presence_envelope_exact_and_extra_fields_absent(self):
        env = c.build_presence_envelope(source="heartbeat")
        self.assertEqual(env, {"contractVersion": 2, "kind": "presence", "presence": {"status": "ONLINE", "mode": "OBSERVATION", "source": "heartbeat"}})
        self.assertFalse({"message", "currentDirective", "relayId", "sourceClass", "trigger", "adminNote"} & set(env["presence"]))

    def test_relay_envelope_exact_and_extra_fields_absent(self):
        env = c.build_relay_envelope("bnl-test-0001", "msg", "dir", "canon", "scheduled")
        self.assertEqual(set(env), {"contractVersion", "kind", "relay"})
        self.assertEqual(set(env["relay"]), {"relayId", "message", "currentDirective", "sourceClass", "trigger"})
        self.assertEqual(env["relay"]["sourceClass"], "approved_canon")

    def test_source_and_trigger_mapping_fail_closed(self):
        self.assertEqual(c.map_source_class("fresh_discord"), "fresh_public_event")
        self.assertEqual(c.map_source_class("conversation_continuity"), "recent_public_continuity")
        self.assertEqual(c.map_source_class("broadcast_memory"), "scoped_broadcast_memory")
        self.assertEqual(c.map_source_class("public_safe_memory"), "public_safe_memory")
        self.assertEqual(c.map_source_class("canon"), "approved_canon")
        self.assertEqual(c.map_source_class("reflection"), "grounded_reflection")
        self.assertEqual(c.map_trigger("scheduled"), "scheduled")
        self.assertEqual(c.map_trigger("forcePull"), "force_pull")
        self.assertEqual(c.map_trigger("manual"), "manual")
        with self.assertRaises(c.ContractV2Error):
            c.map_source_class("mystery")
        with self.assertRaises(c.ContractV2Error):
            c.map_trigger("mystery")

    def test_relay_id_validation(self):
        self.assertEqual(c.validate_relay_id("bnl-abc_123:45"), "bnl-abc_123:45")
        for bad in ("", "ab", "bad space"):
            with self.assertRaises(c.ContractV2Error):
                c.validate_relay_id(bad)

    def test_stable_payload_across_retry_and_idempotent_response(self):
        env = c.build_relay_envelope("bnl-retry-0001", "msg", "dir", "canon", "scheduled")
        seen = []
        def opener(req, timeout=10):
            seen.append(req.data)
            if len(seen) == 1:
                raise urllib.error.URLError("temporary")
            return Resp(200, accepted_body(env, idempotent=True))
        result = c.deliver_json("https://site.test", "key", env, opener=opener, retries=1)
        self.assertTrue(result.ok)
        self.assertTrue(result.idempotent)
        self.assertEqual(result.relay_id, "bnl-retry-0001")
        self.assertEqual(result.published_at, "2026-07-15T00:00:00Z")
        self.assertEqual(seen[0], seen[1])

    def test_delivery_failures(self):
        env = c.build_relay_envelope("bnl-fail-0001", "msg", "dir", "canon", "scheduled")
        cases = [
            (Resp(400, b"{}"), "contract_rejected"),
            (Resp(409, b"{}"), "relay_id_conflict"),
            (Resp(500, b"{}"), "retryable_delivery_failure"),
            (Resp(200, b"not-json"), "malformed_json"),
            (Resp(401, b"{}"), "authentication_failed"),
            (Resp(418, b"{}"), "http_rejected"),
            (Resp(200, json.dumps({"ok": True, "relay": dict(env["relay"], contractVersion=2, publishedAt="2026-07-15T00:00:00Z"), "persisted": False}).encode()), "not_persisted"),
            (Resp(200, accepted_body(env, override={"relayId": "bnl-other-0001"})), "response_mismatch"),
            (Resp(200, accepted_body(env, override={"message": "changed"})), "response_mismatch"),
        ]
        for resp, reason in cases:
            result = c.deliver_json("https://site.test", "k", env, opener=lambda req, timeout=10, r=resp: r, retries=0)
            self.assertFalse(result.ok)
            self.assertEqual(result.reason, reason)
        result = c.deliver_json("https://site.test", "k", env, opener=lambda req, timeout=10: (_ for _ in ()).throw(urllib.error.URLError("down")), retries=0)
        self.assertEqual(result.reason, "retryable_delivery_failure")

    def test_record_publication_with_website_id_and_failed_attempt_does_not_advance(self):
        with tempfile.NamedTemporaryFile() as f:
            state.begin_attempt(f.name, "a1", 42, "scheduled", cursor=7)
            state.prepare_attempt_relay(f.name, "a1", "bnl-prepared-0001")
            state.complete_attempt(f.name, "a1", source_class="canon", outcome="delivery_failed", reason="contract_rejected", cursor=7, prepared_relay_id="bnl-prepared-0001")
            self.assertIsNone(state.get_cursor(f.name, 42))
            rid = state.record_publication(f.name, 42, message="msg", directive="dir", mode="OBSERVATION", relay_lane="current_signal", event_type="canon", source_cursor=7, relay_id="bnl-prepared-0001", published_timestamp="2026-07-15T00:00:00Z")
            self.assertEqual(rid, "bnl-prepared-0001")
            self.assertEqual(state.get_cursor(f.name, 42), 7)

    def test_startup_hydration_get_no_post_and_no_duplicate(self):
        env = c.build_relay_envelope("bnl-hydrate-0001", "msg", "dir", "canon", "scheduled")
        body = json.dumps({"status": "ONLINE", "mode": "OBSERVATION", "message": "msg", "currentDirective": "dir", "source": "relay", "lastSeen": "2026-07-15T00:00:00Z", "persisted": True, "contractVersion": 2, "presence": {"contractVersion": 2, "status": "ONLINE", "mode": "OBSERVATION", "source": "startup", "receivedAt": "2026-07-15T00:00:00Z"}, "relay": dict(env["relay"], contractVersion=2, publishedAt="2026-07-15T00:00:00Z")}).encode()
        posts = []
        def opener(req, timeout=10):
            if req.get_method() == "POST":
                posts.append(req.data)
            return Resp(200, body)
        with tempfile.NamedTemporaryFile() as f, mock.patch.object(bnl01_bot, "DB_FILE", f.name), mock.patch.object(bnl01_bot, "BNL_STATUS_URL", "https://site.test"), mock.patch.object(bnl01_bot, "BNL_API_KEY", "key"), mock.patch("urllib.request.urlopen", side_effect=opener):
            self.assertTrue(bnl01_bot.hydrate_website_relay_v2(42))
            self.assertFalse(bnl01_bot.hydrate_website_relay_v2(42))
            self.assertEqual(posts, [])
            self.assertEqual(len(state.recent_history(f.name, 42)), 1)
            self.assertIsNone(state.get_cursor(f.name, 42))

    def test_presence_response_validation(self):
        env = c.build_presence_envelope(source="startup")
        good = {"ok": True, "presence": {"contractVersion": 2, "status": "ONLINE", "mode": "OBSERVATION", "source": "startup", "receivedAt": "2026-07-15T00:00:00Z"}, "persisted": True}
        self.assertTrue(c.validate_presence_response(good, env).ok)
        for bad in (
            b"not-json",
            json.dumps({"ok": False, "presence": good["presence"], "persisted": True}).encode(),
            json.dumps({"ok": True, "presence": dict(good["presence"], source="heartbeat"), "persisted": True}).encode(),
            json.dumps({"ok": True, "presence": good["presence"], "persisted": False}).encode(),
            json.dumps({"ok": True, "presence": dict(good["presence"], receivedAt="not-a-date"), "persisted": True}).encode(),
        ):
            result = c.deliver_json("https://site.test", "k", env, opener=lambda req, timeout=10, body=bad: Resp(200, body), retries=0)
            self.assertFalse(result.ok)

    def test_relay_persisted_false_does_not_advance_cursor_or_history(self):
        with tempfile.NamedTemporaryFile() as f:
            state.begin_attempt(f.name, "a2", 42, "scheduled", cursor=7)
            env = c.build_relay_envelope("bnl-persist-0001", "Substantive public relay speech.", "A grounded line of inquiry or invitation.", "canon", "scheduled")
            body = json.dumps({"ok": True, "relay": dict(env["relay"], contractVersion=2, publishedAt="2026-07-15T00:00:00Z"), "persisted": False}).encode()
            result = c.deliver_json("https://site.test", "k", env, opener=lambda req, timeout=10: Resp(200, body), retries=0)
            self.assertEqual(result.reason, "not_persisted")
            self.assertIsNone(state.get_cursor(f.name, 42))
            self.assertEqual(state.recent_history(f.name, 42), [])

    def test_network_hydration_resolves_one_primary_guild(self):
        env = c.build_relay_envelope("bnl-hydrate-0002", "msg", "dir", "canon", "scheduled")
        body = json.dumps({"status": "ONLINE", "mode": "OBSERVATION", "message": "msg", "currentDirective": "dir", "source": "relay", "lastSeen": "2026-07-15T00:00:00Z", "persisted": True, "contractVersion": 2, "presence": {"contractVersion": 2, "status": "ONLINE", "mode": "OBSERVATION", "source": "startup", "receivedAt": "2026-07-15T00:00:00Z"}, "relay": dict(env["relay"], contractVersion=2, publishedAt="2026-07-15T00:00:00Z")}).encode()
        gets = []
        def opener(req, timeout=10):
            gets.append(req.get_method())
            return Resp(200, body)
        with tempfile.NamedTemporaryFile() as f, mock.patch.object(bnl01_bot, "DB_FILE", f.name), mock.patch.object(bnl01_bot, "BNL_STATUS_URL", "https://site.test"), mock.patch.object(bnl01_bot, "BNL_API_KEY", "key"), mock.patch.object(bnl01_bot, "BNL_PRIMARY_GUILD_ID", 99), mock.patch("urllib.request.urlopen", side_effect=opener):
            gid = bnl01_bot._resolve_startup_hydration_guild_id()
            self.assertEqual(gid, 99)
            self.assertTrue(bnl01_bot.hydrate_website_relay_v2(gid))
            self.assertFalse(bnl01_bot.hydrate_website_relay_v2(gid))
            self.assertEqual(gets, ["GET", "GET"])
            self.assertEqual(len(state.recent_history(f.name, 99)), 1)
            self.assertEqual(state.recent_history(f.name, 42), [])


    def test_durable_pending_replay_across_transactions_and_restart(self):
        decision_one = bnl01_bot.WebsiteRelayDecision(
            True, eventType="canon", sourceConversationIds=[10, 11], sourceCursor=11,
            message="First accepted relay speech stays durable.",
            directive="Preserve this accepted directive across replay.",
            mode="OBSERVATION", relayLane="network_posture",
            metadata={"source_class": "canon", "aggregate_source_counts": {"canon": 1}},
        )
        sent = []
        def lost_response(req, timeout=10):
            sent.append(req.data)
            raise urllib.error.URLError("lost after accept")
        with tempfile.NamedTemporaryFile() as f, mock.patch.object(bnl01_bot, "DB_FILE", f.name), mock.patch.object(bnl01_bot, "BNL_WEBSITE_CONTRACT_VERSION", "2"), mock.patch.object(bnl01_bot, "BNL_STATUS_URL", "https://site.test"), mock.patch.object(bnl01_bot, "BNL_API_KEY", "key"), mock.patch.object(bnl01_bot, "get_bnl_control_flags", return_value={"websiteRelayEnabled": True, "heartbeatEnabled": True}), mock.patch.object(bnl01_bot, "_generate_website_relay_guarded", return_value=decision_one), mock.patch("urllib.request.urlopen", side_effect=lost_response):
            first = asyncio.run(bnl01_bot._execute_website_relay_transaction(42, source="relay"))
            self.assertFalse(first.publish)
            self.assertEqual(first.skipReason, "website_post_failed")
            self.assertEqual(first.metadata["reason"], "retryable_delivery_failure")
            self.assertEqual(state.get_cursor(f.name, 42), None)
            pending = state.get_pending_v2_publication(f.name, 42)
            self.assertTrue(pending)
            first_relay_id = pending["relay_id"]
            first_bytes = pending["canonical_json"].encode()
            self.assertEqual(sent[0], sent[1])
            self.assertEqual(sent[0], first_bytes)

            # Simulate a restart by clearing process-local transaction locks; pending state remains in SQLite.
            bnl01_bot._website_relay_transaction_locks_by_guild.clear()
            def should_not_generate(*args, **kwargs):
                raise AssertionError("new candidate generated despite pending envelope")
            def idempotent_response(req, timeout=10):
                sent.append(req.data)
                env = json.loads(req.data.decode())
                return Resp(200, accepted_body(env, idempotent=True))
            with mock.patch.object(bnl01_bot, "_generate_website_relay_guarded", side_effect=should_not_generate), mock.patch("urllib.request.urlopen", side_effect=idempotent_response):
                second = asyncio.run(bnl01_bot._execute_website_relay_transaction(42, source="relay"))
            self.assertTrue(second.publish)
            self.assertTrue(second.metadata["pending_replay"])
            self.assertEqual(second.metadata["accepted_relay_id"], first_relay_id)
            self.assertEqual(sent[-1], first_bytes)
            self.assertEqual(state.get_cursor(f.name, 42), 11)
            hist = state.recent_history(f.name, 42)
            self.assertEqual(len(hist), 1)
            self.assertEqual(hist[0]["relay_id"], first_relay_id)
            self.assertEqual(state.get_pending_v2_publication(f.name, 42), {})

    def test_delivery_failures_are_hard_failures_for_manual_request(self):
        async def failed_transaction(guild_id, **kwargs):
            return bnl01_bot.WebsiteRelayDecision(False, skipReason="website_post_failed", metadata={"reason": "authentication_failed", "delivery_failure": True})
        with mock.patch.object(bnl01_bot, "_execute_website_relay_transaction", side_effect=failed_transaction):
            ok, _mode, msg, directive = asyncio.run(bnl01_bot.request_fresh_website_relay(42, force=True))
        self.assertFalse(ok)
        self.assertEqual((msg, directive), ("", ""))

    def test_safe_no_publication_remains_non_failure_for_manual_request(self):
        async def safe_empty(guild_id, **kwargs):
            return bnl01_bot.WebsiteRelayDecision(False, skipReason="no_new_public_signal", metadata={"reason": "no_new_public_signal"})
        with mock.patch.object(bnl01_bot, "_execute_website_relay_transaction", side_effect=safe_empty):
            ok, _mode, msg, directive = asyncio.run(bnl01_bot.request_fresh_website_relay(42, force=True))
        self.assertTrue(ok)
        self.assertEqual((msg, directive), ("", ""))

    def test_force_pull_delivery_failure_sets_failed_with_detail(self):
        async def failed_transaction(guild_id, **kwargs):
            return bnl01_bot.WebsiteRelayDecision(False, skipReason="website_post_failed", mode="OBSERVATION", metadata={"reason": "relay_id_conflict", "delivery_failure": True})
        with mock.patch.object(bnl01_bot, "_execute_website_relay_transaction", side_effect=failed_transaction):
            asyncio.run(bnl01_bot._run_force_pull_relay_update(42, request_id="force-fail"))
        fp = bnl01_bot._get_force_pull_state(42)
        self.assertEqual(fp["last_force_pull_status"], "failed")
        self.assertEqual(fp["last_force_pull_error"], "relay_id_conflict")

    def test_startup_hydration_rejects_persisted_false(self):
        env = c.build_relay_envelope("bnl-hydrate-0003", "msg", "dir", "canon", "scheduled")
        body = json.dumps({"status": "ONLINE", "mode": "OBSERVATION", "message": "msg", "currentDirective": "dir", "source": "relay", "lastSeen": "2026-07-15T00:00:00Z", "persisted": False, "contractVersion": 2, "presence": {"contractVersion": 2, "status": "ONLINE", "mode": "OBSERVATION", "source": "startup", "receivedAt": "2026-07-15T00:00:00Z"}, "relay": dict(env["relay"], contractVersion=2, publishedAt="2026-07-15T00:00:00Z")}).encode()
        with tempfile.NamedTemporaryFile() as f, mock.patch.object(bnl01_bot, "DB_FILE", f.name), mock.patch.object(bnl01_bot, "BNL_STATUS_URL", "https://site.test"), mock.patch.object(bnl01_bot, "BNL_API_KEY", "key"), mock.patch("urllib.request.urlopen", return_value=Resp(200, body)):
            self.assertFalse(bnl01_bot.hydrate_website_relay_v2(42))
            self.assertEqual(state.recent_history(f.name, 42), [])
            self.assertIsNone(state.get_cursor(f.name, 42))


    def test_lost_response_then_startup_hydration_confirms_pending_without_post(self):
        decision = bnl01_bot.WebsiteRelayDecision(
            True, eventType="canon", sourceConversationIds=[20], sourceCursor=20,
            message="Hydration confirms the accepted relay.",
            directive="Use the persisted website acceptance as local confirmation.",
            mode="OBSERVATION", relayLane="network_posture",
            metadata={"source_class": "canon", "aggregate_source_counts": {"canon": 1}},
        )
        sent = []
        def lost_response(req, timeout=10):
            sent.append(req.data)
            raise urllib.error.URLError("lost")
        with tempfile.NamedTemporaryFile() as f, mock.patch.object(bnl01_bot, "DB_FILE", f.name), mock.patch.object(bnl01_bot, "BNL_WEBSITE_CONTRACT_VERSION", "2"), mock.patch.object(bnl01_bot, "BNL_STATUS_URL", "https://site.test"), mock.patch.object(bnl01_bot, "BNL_API_KEY", "key"), mock.patch.object(bnl01_bot, "get_bnl_control_flags", return_value={"websiteRelayEnabled": True, "heartbeatEnabled": True}), mock.patch.object(bnl01_bot, "_generate_website_relay_guarded", return_value=decision), mock.patch("urllib.request.urlopen", side_effect=lost_response):
            first = asyncio.run(bnl01_bot._execute_website_relay_transaction(42, source="relay"))
            self.assertFalse(first.publish)
            pending = state.get_pending_v2_publication(f.name, 42)
            self.assertTrue(pending)

            env = json.loads(pending["canonical_json"])
            body = json.dumps({"status": "ONLINE", "mode": "OBSERVATION", "message": pending["message"], "currentDirective": pending["current_directive"], "source": "relay", "lastSeen": "2026-07-15T00:00:00Z", "persisted": True, "contractVersion": 2, "presence": {"contractVersion": 2, "status": "ONLINE", "mode": "OBSERVATION", "source": "startup", "receivedAt": "2026-07-15T00:00:00Z"}, "relay": dict(env["relay"], contractVersion=2, publishedAt="2026-07-15T00:00:00Z")}).encode()
            methods = []
            def get_only(req, timeout=10):
                methods.append(req.get_method())
                return Resp(200, body)
            with mock.patch("urllib.request.urlopen", side_effect=get_only):
                self.assertTrue(bnl01_bot.hydrate_website_relay_v2(42))
            self.assertEqual(methods, ["GET"])
            self.assertEqual(state.get_cursor(f.name, 42), 20)
            hist = state.recent_history(f.name, 42)
            self.assertEqual(len(hist), 1)
            self.assertEqual(hist[0]["relay_id"], pending["relay_id"])
            self.assertEqual(state.get_pending_v2_publication(f.name, 42), {})

    def test_existing_hydrated_row_reconciles_pending_confirmation_once(self):
        with tempfile.NamedTemporaryFile() as f, mock.patch.object(bnl01_bot, "DB_FILE", f.name), mock.patch.object(bnl01_bot, "BNL_STATUS_URL", "https://site.test"), mock.patch.object(bnl01_bot, "BNL_API_KEY", "key"):
            relay_id = "bnl-reconcile-0001"
            msg = "Already hydrated relay text."
            directive = "Already hydrated directive text."
            state.hydrate_publication(f.name, 42, relay_id=relay_id, message=msg, directive=directive, source_class="approved_canon", trigger="scheduled", published_timestamp="2026-07-15T00:00:00Z")
            env = c.build_relay_envelope(relay_id, msg, directive, "approved_canon", "scheduled")
            state.save_pending_v2_publication(f.name, 42, relay_id=relay_id, message=msg, current_directive=directive, source_class="approved_canon", trigger="scheduled", source_cursor=12, source_conversation_fingerprint="fp", canonical_json=c.canonical_payload_bytes(env).decode(), mode="OBSERVATION", relay_lane="network_posture", event_type="canon")
            body = json.dumps({"persisted": True, "contractVersion": 2, "relay": dict(env["relay"], contractVersion=2, publishedAt="2026-07-15T00:00:01Z")}).encode()
            with mock.patch("urllib.request.urlopen", return_value=Resp(200, body)):
                self.assertTrue(bnl01_bot.hydrate_website_relay_v2(42))
            self.assertEqual(state.get_cursor(f.name, 42), 12)
            hist = state.recent_history(f.name, 42)
            self.assertEqual(len(hist), 1)
            self.assertEqual(hist[0]["relay_id"], relay_id)
            self.assertEqual(state.get_pending_v2_publication(f.name, 42), {})

    def test_same_relay_id_different_content_conflicts_and_retains_pending(self):
        with tempfile.NamedTemporaryFile() as f, mock.patch.object(bnl01_bot, "DB_FILE", f.name), mock.patch.object(bnl01_bot, "BNL_STATUS_URL", "https://site.test"), mock.patch.object(bnl01_bot, "BNL_API_KEY", "key"):
            relay_id = "bnl-conflict-0001"
            state.record_publication(f.name, 42, message="Original relay text.", directive="Original directive text.", mode="OBSERVATION", relay_lane="network_posture", event_type="canon", source_cursor=3, relay_id=relay_id, published_timestamp="2026-07-15T00:00:00Z")
            env = c.build_relay_envelope(relay_id, "Different relay text.", "Different directive text.", "approved_canon", "scheduled")
            state.save_pending_v2_publication(f.name, 42, relay_id=relay_id, message="Different relay text.", current_directive="Different directive text.", source_class="approved_canon", trigger="scheduled", source_cursor=9, source_conversation_fingerprint="fp", canonical_json=c.canonical_payload_bytes(env).decode(), mode="OBSERVATION", relay_lane="network_posture", event_type="canon")
            body = json.dumps({"persisted": True, "contractVersion": 2, "relay": dict(env["relay"], contractVersion=2, publishedAt="2026-07-15T00:00:01Z")}).encode()
            with mock.patch("urllib.request.urlopen", return_value=Resp(200, body)):
                self.assertFalse(bnl01_bot.hydrate_website_relay_v2(42))
            self.assertEqual(state.get_cursor(f.name, 42), 3)
            self.assertTrue(state.get_pending_v2_publication(f.name, 42))
            hist = state.recent_history(f.name, 42)
            self.assertEqual(hist[0]["public_message"], "Original relay text.")

    def test_punctuation_and_capitalization_distinct_payloads_get_distinct_ids(self):
        rid1 = bnl01_bot._new_stable_relay_id(guild_id=42, source_cursor=1, message="Relay text.", directive="Directive text.", source_class="canon", trigger="scheduled", source_conversation_fingerprint="fp")
        rid2 = bnl01_bot._new_stable_relay_id(guild_id=42, source_cursor=1, message="relay text", directive="Directive text!", source_class="canon", trigger="scheduled", source_conversation_fingerprint="fp")
        self.assertNotEqual(rid1, rid2)

    def test_v1_heartbeat_noops_without_http_stock_status(self):
        calls = []
        with mock.patch.object(bnl01_bot, "BNL_WEBSITE_CONTRACT_VERSION", "1"), mock.patch.object(bnl01_bot, "update_website_status_controlled_async", side_effect=lambda **kw: calls.append(kw)):
            asyncio.run(bnl01_bot.website_presence_heartbeat_task.coro())
        self.assertEqual(calls, [])

    def test_v1_startup_presence_noops_without_http_stock_status(self):
        calls = []
        with mock.patch.object(bnl01_bot, "BNL_WEBSITE_CONTRACT_VERSION", "1"), mock.patch.object(bnl01_bot, "publish_website_presence_v2", side_effect=lambda **kw: calls.append(kw)):
            self.assertFalse(asyncio.run(bnl01_bot.publish_startup_presence_v2_if_enabled()))
        self.assertEqual(calls, [])

    def test_version_one_legacy_payload_and_no_v2_fallback(self):
        with mock.patch.object(bnl01_bot, "BNL_WEBSITE_CONTRACT_VERSION", "1"), mock.patch.object(bnl01_bot, "BNL_API_KEY", "key"), mock.patch.object(bnl01_bot, "BNL_STATUS_URL", "https://site.test"):
            captured = {}
            def opener(req, timeout=10):
                captured.update(json.loads(req.data.decode()))
                return Resp(200, b"{}")
            with mock.patch("urllib.request.urlopen", side_effect=opener):
                self.assertTrue(bnl01_bot.update_website_status_controlled("OBSERVATION", "A sufficiently specific public relay message for the legacy v1 payload.", force=True, current_directive="Review a sufficiently specific directive for the legacy v1 payload path and preserve exact flat transport fields during rollback testing.", source="relay"))
            self.assertEqual(captured, {"status": "ONLINE", "mode": "OBSERVATION", "message": "A sufficiently specific public relay message for the legacy v1 payload.", "currentDirective": "Review a sufficiently specific directive for the legacy v1 payload path and preserve exact flat transport fields during rollback testing.", "source": "relay"})
        with mock.patch.object(bnl01_bot, "BNL_WEBSITE_CONTRACT_VERSION", "2"):
            self.assertFalse(bnl01_bot.update_website_status_controlled("OBSERVATION", "msg", source="relay"))

    def test_flags_gate_only_expected_tasks_and_cadence_constant(self):
        self.assertEqual(bnl01_bot.BNL_WEBSITE_RELAY_INTERVAL_MINUTES, 20)
        self.assertEqual(bnl01_bot.BNL_WEBSITE_HEARTBEAT_INTERVAL_MINUTES, 5)
        self.assertTrue(hasattr(bnl01_bot, "website_presence_heartbeat_task"))


if __name__ == "__main__":
    unittest.main()
