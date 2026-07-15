import json
import os
os.environ.setdefault("GEMINI_API_KEY", "test-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-token")
import tempfile
import unittest
import urllib.error
from io import BytesIO
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
    relay["publishedAt"] = "2026-07-15T00:00:00Z"
    if override:
        relay.update(override)
    return json.dumps({"ok": True, "relay": relay, "idempotent": idempotent}).encode()


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
        for bad in ("", "short", "bad space"):
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
        body = json.dumps({"contractVersion": 2, "relay": dict(env["relay"], publishedAt="2026-07-15T00:00:00Z")}).encode()
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
