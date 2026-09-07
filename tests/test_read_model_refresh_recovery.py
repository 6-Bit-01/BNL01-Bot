"""Exercise queue acquisition through HTTP and the existing prompt consumer."""

import copy
import json
import os
import unittest
import urllib.error
from datetime import datetime, timedelta, timezone
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl01_bot


def read_model(*, opened=True, scope="public"):
    return {
        "ok": True,
        "version": 1,
        "publicOnly": scope != "private",
        "accessScope": scope,
        "capabilities": {"queueProduction": True},
        "sections": {
            "queue": {
                "available": True,
                "accessScope": scope,
                "session": {"title": "Friday Broadcast", "queueOpen": opened},
                "status": {"activeCount": 2, "capacity": 44},
            },
        },
    }


class Response:
    status = 200

    def __init__(self, payload):
        self.payload = copy.deepcopy(payload)

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self):
        return json.dumps(self.payload).encode("utf-8")


class ReadModelRefreshRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026, 9, 7, tzinfo=timezone.utc)
        patches = [
            mock.patch.object(bnl01_bot, "BNL_READ_MODEL_ENABLED", True),
            mock.patch.object(bnl01_bot, "BNL_READ_MODEL_URL", "https://example.test/api/bnl/read-model"),
            mock.patch.object(bnl01_bot, "BNL_API_KEY", "fixture-service-key"),
            mock.patch.object(bnl01_bot, "_bnl_read_model_cache", None),
            mock.patch.object(bnl01_bot, "_bnl_read_model_cached_at", None),
            mock.patch.object(bnl01_bot, "_bnl_read_model_cache_scope", None),
            mock.patch.dict(os.environ, {"BNL_QUEUE_PRODUCTION_ENABLED": "true"}),
            mock.patch.object(bnl01_bot, "datetime", wraps=datetime),
            mock.patch.object(bnl01_bot.urllib.request, "urlopen"),
        ]
        started = []
        for patch in patches:
            started.append(patch.start())
            self.addCleanup(patch.stop)
        self.clock, self.http = started[-2:]
        self.clock.now.side_effect = lambda tz: self.now.astimezone(tz)

    def seed(self, payload=None):
        payload = read_model() if payload is None else payload
        self.http.return_value = Response(payload)
        result = bnl01_bot.fetch_bnl_read_model(force=True)
        self.http.reset_mock()
        return result

    def context(self, policy="sealed_test"):
        return bnl01_bot.maybe_build_bnl_read_model_context(
            "What was your latest Journal about, and is the queue open right now?",
            policy,
        )

    def test_foreground_timeout_uses_new_background_success_in_real_consumer(self):
        self.seed()
        self.now += timedelta(seconds=5)
        calls = []

        def overlapping_request(request, timeout):
            calls.append(request)
            self.assertEqual(timeout, 3)
            if len(calls) == 1:
                self.now += timedelta(seconds=1)
                bnl01_bot.fetch_bnl_read_model(force=True)
                self.now += timedelta(seconds=1)
                raise TimeoutError("fixture timeout")
            return Response(read_model(opened=False))

        self.http.side_effect = overlapping_request
        context = self.context()
        self.assertIn("queueOpen=False", context)
        self.assertNotIn("queueOpen=True", context)
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[0].get_header("Cache-control"), "no-cache")
        self.assertEqual(calls[0].get_header("X-api-key"), "fixture-service-key")
        self.assertEqual(bnl01_bot._bnl_read_model_cached_at, self.now - timedelta(seconds=1))

    def test_cached_public_queue_is_readable_in_home_and_private_mirror(self):
        self.seed(read_model(opened=False))
        self.http.side_effect = TimeoutError("fixture timeout")
        for policy in ("public_home", "sealed_test"):
            with self.subTest(policy=policy):
                self.assertIn("queueOpen=False", self.context(policy))

    def test_failed_refresh_does_not_extend_original_ttl(self):
        self.seed()
        cached_at = bnl01_bot._bnl_read_model_cached_at
        self.http.side_effect = TimeoutError("fixture timeout")
        self.now += timedelta(seconds=19)
        self.assertIn("queueOpen=True", self.context())
        self.assertEqual(bnl01_bot._bnl_read_model_cached_at, cached_at)
        self.now += timedelta(seconds=1)
        self.assertEqual(self.context(), "")

    def test_snapshot_expiring_during_failed_request_is_unavailable(self):
        self.seed()
        self.now += timedelta(seconds=19)

        def timeout_after_expiry(*_args, **_kwargs):
            self.now += timedelta(seconds=2)
            raise TimeoutError("fixture timeout")

        self.http.side_effect = timeout_after_expiry
        self.assertEqual(self.context(), "")

    def test_unavailable_queue_is_not_fabricated(self):
        self.http.side_effect = TimeoutError("fixture timeout")
        self.assertEqual(self.context(), "")

    def test_cache_is_bound_to_source_and_credentials(self):
        self.seed()
        self.http.side_effect = TimeoutError("fixture timeout")
        for name, value in (
            ("BNL_READ_MODEL_URL", "https://other.test/api/bnl/read-model"),
            ("BNL_API_KEY", "different-fixture-key"),
            ("BNL_API_KEY", ""),
        ):
            with self.subTest(setting=name, value=bool(value)):
                with mock.patch.object(bnl01_bot, name, value):
                    self.assertEqual(bnl01_bot.fetch_bnl_read_model(), {})
                    self.assertEqual(self.context(), "")

    def test_private_snapshot_recovery_obeys_existing_channel_scope(self):
        self.seed(read_model(scope="private"))
        self.http.side_effect = TimeoutError("fixture timeout")
        self.assertIn("queueOpen=True", self.context("sealed_test"))
        self.assertEqual(self.context("public_home"), "")

    def test_explicit_access_removal_replaces_previous_public_snapshot(self):
        self.seed()
        self.http.return_value = Response(read_model(scope="none"))
        self.assertEqual(self.context(), "")
        self.http.side_effect = TimeoutError("fixture timeout")
        self.assertEqual(self.context(), "")

    def test_denial_invalidates_previously_authorized_snapshot(self):
        self.seed()
        self.http.side_effect = urllib.error.HTTPError("fixture", 403, "forbidden", {}, None)
        self.assertEqual(self.context(), "")
        self.http.side_effect = TimeoutError("fixture timeout")
        self.assertEqual(self.context(), "")

    def test_invalid_payload_cannot_recover_an_old_snapshot(self):
        self.seed()
        self.http.return_value = Response({"ok": False})
        self.assertEqual(self.context(), "")
        self.http.side_effect = TimeoutError("fixture timeout")
        self.assertEqual(self.context(), "")

    def test_response_from_changed_source_configuration_is_not_cached(self):
        def changed_source(*_args, **_kwargs):
            bnl01_bot.BNL_READ_MODEL_URL = "https://other.test/api/bnl/read-model"
            return Response(read_model())

        self.http.side_effect = changed_source
        self.assertEqual(self.context(), "")
        self.assertIsNone(bnl01_bot._bnl_read_model_cache)

    def test_transient_server_failure_reuses_only_fresh_cache(self):
        self.seed(read_model(opened=False))
        self.http.side_effect = urllib.error.HTTPError("fixture", 503, "unavailable", {}, None)
        self.assertIn("queueOpen=False", self.context())
        self.now += timedelta(seconds=20)
        self.assertEqual(self.context(), "")

    def test_normal_cached_read_avoids_duplicate_http_until_refresh_requested(self):
        self.seed()
        self.assertTrue(bnl01_bot.fetch_bnl_read_model())
        self.http.assert_not_called()
        bnl01_bot.fetch_bnl_read_model(force=True)
        self.http.assert_called_once()

    def test_late_older_success_does_not_replace_newer_queue_snapshot(self):
        calls = []

        def overlapping_request(*_args, **_kwargs):
            calls.append(True)
            if len(calls) == 1:
                self.now += timedelta(seconds=1)
                bnl01_bot.fetch_bnl_read_model(force=True)
                return Response(read_model(opened=True))
            return Response(read_model(opened=False))

        self.http.side_effect = overlapping_request
        self.assertIn("queueOpen=False", self.context())
        self.assertFalse(bnl01_bot._bnl_read_model_cache["sections"]["queue"]["session"]["queueOpen"])


if __name__ == "__main__":
    unittest.main()
