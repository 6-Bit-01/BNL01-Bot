import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
import threading
import time
import unittest
import urllib.error
from contextlib import ExitStack
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl01_bot


def snapshot(*, private=False, revision="original"):
    return {
        "ok": True,
        "version": 1,
        "publicOnly": not private,
        "accessScope": "private" if private else "public",
        "sections": {"queue": {"revision": revision}},
    }


class Response:
    def __init__(self, payload, *, status=200):
        self.payload = payload
        self.status = status

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self):
        return json.dumps(self.payload).encode("utf-8")


class ReadModelRefreshRecoveryTests(unittest.TestCase):
    """Exercise the real fetch owner with only its HTTP boundary replaced."""

    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.clock = datetime.datetime(2026, 9, 7, tzinfo=datetime.timezone.utc)
        for name, value in {
            "BNL_READ_MODEL_ENABLED": True,
            "BNL_READ_MODEL_URL": "https://example.test/api/bnl/read-model",
            "BNL_API_KEY": "test-service-key",
            "BNL_READ_MODEL_TTL_SECONDS": 20,
            "_bnl_read_model_cache": None,
            "_bnl_read_model_cached_at": None,
            "_bnl_read_model_cache_scope": None,
            "_bnl_read_model_request_serial": 0,
            "_bnl_read_model_applied_serial": 0,
        }.items():
            self.stack.enter_context(mock.patch.object(bnl01_bot, name, value))
        clock = self.stack.enter_context(mock.patch.object(bnl01_bot, "datetime"))
        clock.now.side_effect = lambda _tz: self.clock
        self.http = self.stack.enter_context(
            mock.patch.object(bnl01_bot.urllib.request, "urlopen")
        )

    def advance(self, seconds):
        self.clock += datetime.timedelta(seconds=seconds)

    def seed(self, payload=None):
        self.http.side_effect = None
        self.http.return_value = Response(payload or snapshot())
        return bnl01_bot.fetch_bnl_read_model(force=True)

    def test_slow_private_rehearsal_read_delivers_verified_track_context(self):
        from tests.test_rehearsal_read_model import REQUEST, rehearsal_model

        payload = rehearsal_model()
        payload["sections"]["queue"]["session"]["status"] = "archived"
        body = json.dumps(payload).encode("utf-8")
        received = []

        class SlowReadModel(BaseHTTPRequestHandler):
            def do_GET(self):
                received.append((self.path, self.headers.get("x-api-key")))
                # The observed private feed took 3.54 seconds to send headers.
                # Exercise actual socket waiting, not a mocked timeout value.
                time.sleep(3.6)
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                try:
                    self.wfile.write(body)
                except (BrokenPipeError, ConnectionResetError):
                    pass

            def log_message(self, *_args):
                pass

        with ThreadingHTTPServer(("127.0.0.1", 0), SlowReadModel) as server:
            serving = threading.Thread(target=server.serve_forever, daemon=True)
            serving.start()
            self.http.side_effect = urllib.request.build_opener(
                urllib.request.ProxyHandler({}),
            ).open
            try:
                with mock.patch.object(
                    bnl01_bot, "BNL_READ_MODEL_URL",
                    f"http://127.0.0.1:{server.server_port}/api/bnl/read-model",
                ), mock.patch.dict(os.environ, {"BNL_QUEUE_PRODUCTION_ENABLED": "true"}):
                    context = bnl01_bot.maybe_build_bnl_read_model_context(
                        REQUEST, "sealed_test",
                    )
                self.assertIn("Test Artist B — B2 Complete", context)
                self.assertIn("Test Artist B — B3 Partial Priority", context)
                self.assertIn("status=archived", context)
                self.assertIn("earlyCutoff=True", context)
                self.assertEqual(received, [("/api/bnl/read-model", "test-service-key")])
                self.http.assert_called_once()
            finally:
                server.shutdown()
                serving.join(timeout=2)

    def test_forced_timeout_retains_same_scope_snapshot_without_extending_ttl(self):
        original = self.seed()
        original_time = bnl01_bot._bnl_read_model_cached_at
        self.advance(10)
        self.http.side_effect = TimeoutError("test timeout")

        self.assertEqual(bnl01_bot.fetch_bnl_read_model(force=True), original)
        request = self.http.call_args.args[0]
        self.assertEqual(request.get_header("Cache-control"), "no-cache")
        self.assertEqual(self.http.call_args.kwargs["timeout"], 15)
        self.assertEqual(bnl01_bot._bnl_read_model_cached_at, original_time)
        self.advance(10)
        self.assertEqual(bnl01_bot.fetch_bnl_read_model(force=True), {})
        self.assertEqual(bnl01_bot._bnl_read_model_cached_at, original_time)

    def test_nine_second_public_history_response_reaches_authorized_reader(self):
        from test_public_show_history import public_history_model
        from test_tiktok_show_evidence_ledger import ENABLED_QUEUE_ENV
        from bnl_canon_source_contract import show_queue_evidence_authorization

        payload = public_history_model()
        payload["schemaRevision"] = "1.11"
        body = json.dumps(payload).encode("utf-8")
        received = []
        advance = self.advance

        class SlowPublicHistory(BaseHTTPRequestHandler):
            def do_GET(self):
                received.append((self.path, self.headers.get("x-api-key")))
                # Production headers arrived at 8.77 and 8.95 seconds; the
                # previous eight-second socket budget rejected both feeds.
                time.sleep(9)
                advance(9)
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                try:
                    self.wfile.write(body)
                except (BrokenPipeError, ConnectionResetError):
                    pass

            def log_message(self, *_args):
                pass

        with ThreadingHTTPServer(("127.0.0.1", 0), SlowPublicHistory) as server:
            serving = threading.Thread(target=server.serve_forever, daemon=True)
            serving.start()
            self.http.side_effect = urllib.request.build_opener(
                urllib.request.ProxyHandler({}),
            ).open
            try:
                with mock.patch.object(
                    bnl01_bot, "BNL_READ_MODEL_URL",
                    f"http://127.0.0.1:{server.server_port}/api/bnl/read-model",
                ):
                    started = self.clock
                    loaded = bnl01_bot.fetch_bnl_read_model(force=True)
                    self.assertTrue(loaded, "A nine-second public feed must reach the reader")
                    authorization = show_queue_evidence_authorization(
                        loaded, environ=ENABLED_QUEUE_ENV,
                    )
                    self.assertTrue(authorization["usable"], authorization)
                    self.assertEqual(authorization["receipt"]["archiveSchemaVersion"],
                                     "queue_bnl_public_history_v1")
                    self.assertEqual(bnl01_bot._bnl_read_model_cached_at, started)
                    self.advance(10)
                    self.assertEqual(bnl01_bot.fetch_bnl_read_model(), loaded)
                    self.http.assert_called_once()
                    self.assertEqual(received, [("/api/bnl/read-model", "test-service-key")])
                    # The wait consumes freshness; it never buys a new TTL.
                    self.advance(1)
                    self.http.side_effect = TimeoutError("test timeout")
                    self.assertEqual(bnl01_bot.fetch_bnl_read_model(force=True), {})
                    self.assertEqual(bnl01_bot._bnl_read_model_cached_at, started)
            finally:
                server.shutdown()
                serving.join(timeout=2)

    def test_expiry_during_failed_refresh_cannot_return_current_queue(self):
        self.seed()
        self.advance(19)

        def timeout_after_wait(*_args, **kwargs):
            self.advance(kwargs["timeout"])
            raise TimeoutError("test timeout")

        self.http.side_effect = timeout_after_wait
        self.assertEqual(bnl01_bot.fetch_bnl_read_model(force=True), {})

    def test_exhausted_read_timeout_does_not_retry_or_invent_context(self):
        from tests.test_rehearsal_read_model import REQUEST

        def timeout_after_wait(*_args, **kwargs):
            self.advance(kwargs["timeout"])
            raise TimeoutError("test timeout")

        self.http.side_effect = timeout_after_wait
        with mock.patch.dict(os.environ, {"BNL_QUEUE_PRODUCTION_ENABLED": "true"}):
            context = bnl01_bot.maybe_build_bnl_read_model_context(REQUEST, "sealed_test")
        self.assertEqual(context, "")
        self.http.assert_called_once()
        self.assertIsNone(bnl01_bot._bnl_read_model_cache)

    def test_successful_forced_refresh_replaces_snapshot(self):
        self.seed()
        self.advance(5)
        updated = snapshot(revision="updated")
        self.http.return_value = Response(updated)
        self.assertEqual(bnl01_bot.fetch_bnl_read_model(force=True), updated)
        self.http.reset_mock()
        self.assertEqual(bnl01_bot.fetch_bnl_read_model(), updated)
        self.http.assert_not_called()

    def test_retryable_http_errors_and_transport_errors_can_use_fresh_cache(self):
        original = self.seed()
        errors = [
            urllib.error.HTTPError("https://example.test", code, "test", {}, None)
            for code in (408, 429, 500, 503, 599)
        ] + [urllib.error.URLError("test disconnect"), ConnectionResetError()]
        for error in errors:
            with self.subTest(error=repr(error)):
                self.http.side_effect = error
                self.assertEqual(bnl01_bot.fetch_bnl_read_model(force=True), original)
        self.http.side_effect = None
        self.http.return_value = Response({}, status=503)
        self.assertEqual(bnl01_bot.fetch_bnl_read_model(force=True), original)

    def test_auth_and_other_definitive_rejections_clear_cached_permission(self):
        for code in (400, 401, 403, 404, 410):
            with self.subTest(code=code):
                self.seed(snapshot(private=True))
                self.http.side_effect = urllib.error.HTTPError(
                    "https://example.test", code, "test", {}, None
                )
                self.assertEqual(bnl01_bot.fetch_bnl_read_model(force=True), {})
                self.http.side_effect = TimeoutError()
                self.assertEqual(bnl01_bot.fetch_bnl_read_model(), {})

    def test_invalid_response_clears_cache_instead_of_masking_contract_change(self):
        invalid = [[], {}, snapshot() | {"ok": False},
                   snapshot() | {"version": 2},
                   snapshot() | {"accessScope": "private"},
                   snapshot() | {"sections": []}]
        for payload in invalid:
            with self.subTest(payload=payload):
                self.seed()
                self.http.return_value = Response(payload)
                self.assertEqual(bnl01_bot.fetch_bnl_read_model(force=True), {})
                self.http.side_effect = TimeoutError()
                self.assertEqual(bnl01_bot.fetch_bnl_read_model(), {})

    def test_url_and_credentials_changes_cannot_reuse_previous_identity_scope(self):
        changes = [
            ("BNL_READ_MODEL_URL", "https://another.test/api/bnl/read-model"),
            ("BNL_API_KEY", "another-test-service-key"),
            ("BNL_API_KEY", ""),
        ]
        for name, value in changes:
            with self.subTest(name=name, authenticated=bool(value)):
                self.seed(snapshot(private=True))
                self.http.side_effect = TimeoutError()
                with mock.patch.object(bnl01_bot, name, value):
                    self.assertEqual(bnl01_bot.fetch_bnl_read_model(), {})
                    self.assertEqual(bnl01_bot.fetch_bnl_read_model(force=True), {})

    def test_private_response_requires_current_key_and_public_downgrade_replaces_it(self):
        self.seed(snapshot(private=True))
        public = snapshot()
        self.http.return_value = Response(public)
        with mock.patch.object(bnl01_bot, "BNL_API_KEY", ""):
            self.assertEqual(bnl01_bot.fetch_bnl_read_model(), public)
            self.http.return_value = Response(snapshot(private=True))
            self.assertEqual(bnl01_bot.fetch_bnl_read_model(force=True), {})
            self.http.side_effect = TimeoutError()
            self.assertEqual(bnl01_bot.fetch_bnl_read_model(), {})

    def test_configuration_change_during_fetch_prevents_installing_old_response(self):
        def changed_source(*_args, **_kwargs):
            bnl01_bot.BNL_API_KEY = "changed-during-request"
            return Response(snapshot(private=True))

        self.http.side_effect = changed_source
        self.assertEqual(bnl01_bot.fetch_bnl_read_model(force=True), {})
        self.assertIsNone(bnl01_bot._bnl_read_model_cache)

    def test_disabled_reader_cannot_return_cached_snapshot(self):
        self.seed()
        self.http.reset_mock()
        with mock.patch.object(bnl01_bot, "BNL_READ_MODEL_ENABLED", False):
            self.assertEqual(bnl01_bot.fetch_bnl_read_model(), {})
        self.http.assert_not_called()

    def test_invalid_or_future_dated_cache_cannot_be_used_on_transport_failure(self):
        original = self.seed()
        self.http.side_effect = TimeoutError()
        self.advance(-1)
        self.assertEqual(bnl01_bot.fetch_bnl_read_model(force=True), {})
        self.advance(1)
        original["publicOnly"] = False
        self.assertEqual(bnl01_bot.fetch_bnl_read_model(force=True), {})

    def test_older_success_cannot_undo_newer_auth_rejection(self):
        self.seed(snapshot(private=True))

        def older_request(*_args, **_kwargs):
            with mock.patch.object(
                bnl01_bot.urllib.request, "urlopen",
                side_effect=urllib.error.HTTPError(
                    "https://example.test", 403, "test", {}, None
                ),
            ):
                self.assertEqual(bnl01_bot.fetch_bnl_read_model(force=True), {})
            return Response(snapshot(private=True))

        self.http.side_effect = older_request
        self.assertEqual(bnl01_bot.fetch_bnl_read_model(force=True), {})
        self.http.side_effect = TimeoutError()
        self.assertEqual(bnl01_bot.fetch_bnl_read_model(), {})

    def test_overlapping_refresh_keeps_newer_snapshot_even_when_clocks_equal(self):
        updated = snapshot(revision="newer")

        def older_request(*_args, **_kwargs):
            with mock.patch.object(
                bnl01_bot.urllib.request, "urlopen", return_value=Response(updated)
            ):
                self.assertEqual(bnl01_bot.fetch_bnl_read_model(force=True), updated)
            return Response(snapshot(revision="older"))

        self.http.side_effect = older_request
        self.assertEqual(bnl01_bot.fetch_bnl_read_model(force=True), updated)

    def test_older_rejection_does_not_clear_newer_success(self):
        updated = snapshot(revision="newer")

        def older_request(*_args, **_kwargs):
            with mock.patch.object(
                bnl01_bot.urllib.request, "urlopen", return_value=Response(updated)
            ):
                bnl01_bot.fetch_bnl_read_model(force=True)
            raise urllib.error.HTTPError("https://example.test", 403, "test", {}, None)

        self.http.side_effect = older_request
        self.assertEqual(bnl01_bot.fetch_bnl_read_model(force=True), {})
        self.assertEqual(bnl01_bot.fetch_bnl_read_model(), updated)


if __name__ == "__main__":
    unittest.main()
