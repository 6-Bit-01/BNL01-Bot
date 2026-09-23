"""Published selection, immutable versions, and saved editorial delivery fences."""
import asyncio
import copy
import hashlib
import http.client
import json
import os
import sqlite3
import threading
import unittest
import urllib.error
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

import bnl_broadcast_ballads as ballads
import bnl_journal as journal
import bnl_journal_automation as automation
import bnl_website_relay_state as relay
from tests import test_bnl_journal_shared_inputs as journal_fixture
from tests import test_relay_shared_inputs as relay_fixture
from tests.test_bnl_journal_prepared_release import article_json, AcceptedResponse


class CatalogResponse:
    status = 200

    def __init__(self, catalog):
        self.body = json.dumps({"ballads": catalog}).encode()

    def read(self, limit=-1):
        return self.body[:limit] if limit >= 0 else self.body

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


def seed_ballad(db, published_at):
    ballads.initialize(db)
    version = dict(id="released-1", showId="show-1", ordinal=1, title="The Chairs Stayed Warm",
                   style="Chamber soul with dub bass", palette={"genres": "chamber soul"},
                   lyrics="LYRICS_ARE_NOT_TESTIMONY", rawOutput="PRIVATE_RAW_OUTPUT",
                   options={"feedback": "PRIVATE_PRODUCER_FEEDBACK"}, author="BNL-01")
    with sqlite3.connect(db) as conn:
        for ordinal, title in ((1, version["title"]), (2, "UNPUBLISHED_NEWER_DRAFT")):
            saved = {**version, "ordinal": ordinal, "id": f"released-{ordinal}", "title": title}
            saved["contentHash"] = hashlib.sha256(json.dumps(saved, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
            conn.execute("INSERT INTO bnl_ballad_versions VALUES (?,?,?,?,?)",
                         (1, "show-1", saved["id"], ordinal, json.dumps(saved)))
    return [{"show": {"sessionId": "show-1", "title": "Friday Radio", "showDate": "2026-08-28"},
             "version": {key: version[key] for key in ("id", "title", "lyrics", "style", "palette", "author")},
             "linerNotes": {"about": "A song about the last light", "inspiration": "A creative interpretation",
                            "mentions": "", "inspiredBy": "An earlier broadcast"},
             "publishedAt": published_at, "audioId": "audio-1", "duration": 180,
             "presentation": {"credits": "BNL-01"}, "artistLinks": []}]


class BalladJournalInputsTests(unittest.TestCase):
    def setUp(self):
        self.fixture = journal_fixture.JournalSharedInputsTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.db = self.fixture.db
        self.catalog = seed_ballad(self.db, "2026-08-28T12:00:00Z")
        env = mock.patch.dict(os.environ, {"BNL_STATUS_URL": "https://site.test/api/bnl/status"})
        env.start()
        self.addCleanup(env.stop)
        patch = mock.patch("urllib.request.urlopen", side_effect=lambda *a, **kw: CatalogResponse(self.catalog))
        self.http = patch.start()
        self.addCleanup(patch.stop)

    def packet(self):
        return self.fixture.packet(conversations=[])

    def test_real_journal_writer_gets_published_version_and_no_private_draft_or_lyrics(self):
        packet = self.packet()
        selected = [s for s in packet.get("reflectionBasis", []) if s.get("basisKind") == "published_ballad"]
        self.assertEqual(len(selected), 1)
        prompt = journal.build_generation_prompt(packet)
        self.assertIn("The Chairs Stayed Warm", prompt)
        self.assertIn("/radio/archive?view=shows&show=show-1#broadcast-ballad", prompt)
        for forbidden in ("LYRICS_ARE_NOT_TESTIMONY", "PRIVATE_RAW_OUTPUT", "PRIVATE_PRODUCER_FEEDBACK", "UNPUBLISHED_NEWER_DRAFT"):
            self.assertNotIn(forbidden, json.dumps(packet))
        self.assertEqual(packet["aggregateCounts"]["eligibleConversations"], 0)
        self.assertTrue(packet["lowActivityMode"])

    def test_reader_is_bounded_public_only_and_has_no_database_writes(self):
        snapshot = ballads.read_publication_catalog()
        request = self.http.call_args.args[0]
        self.assertEqual(request.full_url, "https://site.test/api/ballads/catalog")
        self.assertEqual(request.get_method(), "GET")
        self.assertNotIn("X-api-key", request.headers)
        self.assertEqual(self.http.call_args.kwargs["timeout"], 5)
        with sqlite3.connect(Path(self.db).as_uri() + "?mode=ro", uri=True) as conn:
            conn.execute("PRAGMA query_only=ON")
            selected = ballads.select_editorial_publications(conn, 1, snapshot, observed_before=journal_fixture.END)
            self.assertEqual(len(selected), 1)
            self.assertTrue(ballads.local_publication_basis_is_current(conn, 1, selected[0]["basis"]))
            self.assertEqual(conn.total_changes, 0)
        with sqlite3.connect(":memory:") as conn:
            self.assertEqual(ballads.select_editorial_publications(conn, 1, snapshot, observed_before=journal_fixture.END), [])
            self.assertEqual(conn.execute("SELECT count(*) FROM sqlite_master").fetchone()[0], 0)

    def test_unpublished_unknown_version_wrong_guild_and_corrupt_version_are_ineligible(self):
        saved = copy.deepcopy(self.catalog)
        for catalog in ([], [{**saved[0], "version": {**saved[0]["version"], "id": "unknown"}}]):
            self.catalog = catalog
            self.assertEqual(self.packet()["aggregateCounts"]["publishedBalladContext"], 0)
        self.catalog = saved
        with sqlite3.connect(self.db) as conn:
            snapshot = ballads.read_publication_catalog()
            self.assertFalse(ballads.select_editorial_publications(conn, 2, snapshot, observed_before=journal_fixture.END))
            conn.execute("UPDATE bnl_ballad_versions SET document=json_set(document,'$.lyrics','changed')")
        self.assertEqual(self.packet()["aggregateCounts"]["publishedBalladContext"], 0)

    def test_future_and_old_releases_cannot_enter_current_reflection_window(self):
        for when in ("2026-08-29T01:30:00Z", "2026-09-02T12:00:00Z", "2026-07-01T12:00:00Z"):
            self.catalog[0]["publishedAt"] = when
            self.assertEqual(self.packet()["aggregateCounts"]["publishedBalladContext"], 0)

    def test_failed_partial_and_oversized_catalogs_are_unavailable_not_withdrawn(self):
        for response in (CatalogResponse([{}]), CatalogResponse([self.catalog[0], self.catalog[0]])):
            self.http.side_effect = lambda *a, **k: response
            self.assertFalse(ballads.read_publication_catalog()["available"])
        response = CatalogResponse([])
        response.body = b" " * (ballads.PUBLICATION_READ_LIMIT + 1)
        self.assertFalse(ballads.read_publication_catalog()["available"])
        self.http.side_effect = urllib.error.URLError("offline")
        self.assertFalse(ballads.read_publication_catalog()["available"])
        self.http.side_effect = http.client.IncompleteRead(b"partial")
        self.assertFalse(ballads.read_publication_catalog()["available"])

    def test_withdrawal_during_generation_prevents_even_uncited_draft_storage(self):
        packet = self.fixture.packet()
        def generate(value, prompt):
            self.catalog.clear()
            return article_json(value)
        result = journal.generate_and_store_packet_draft(self.db, 1, packet, generate)
        self.assertEqual(result.reason, "ballad_publication_changed")
        with sqlite3.connect(self.db) as conn:
            self.assertEqual(conn.execute("SELECT count(*) FROM bnl_journal_entries").fetchone()[0], 0)

    def draft(self):
        packet = self.fixture.packet()
        result = journal.store_validated_draft(self.db, 1, packet, journal.parse_generated_json(article_json(packet)))
        self.assertTrue(result.ok, result)
        return result

    def test_metadata_change_between_draft_and_approval_blocks_approval(self):
        draft = self.draft()
        self.catalog[0]["linerNotes"]["about"] = "A corrected creative description"
        result = journal.approve_draft(self.db, 1, draft.entry_id, draft.content_hash)
        self.assertEqual(result.reason, "ballad_publication_changed")

    def test_manual_delivery_rechecks_audio_release_version_and_notes(self):
        draft = self.draft()
        self.assertTrue(journal.approve_draft(self.db, 1, draft.entry_id, draft.content_hash).ok)
        saved = copy.deepcopy(self.catalog)
        no_send = mock.Mock(side_effect=AssertionError("changed release must not post"))
        for field in ("audioId", "publishedAt"):
            self.catalog = copy.deepcopy(saved)
            self.catalog[0][field] = "replacement" if field == "audioId" else "2026-08-28T13:00:00Z"
            result = journal.deliver_approved(self.db, 1, draft.entry_id, "https://site.test", "key", opener=no_send)
            self.assertEqual(result.reason, "ballad_publication_changed")
        self.catalog = copy.deepcopy(saved)
        self.catalog[0]["version"]["id"] = "released-2"
        self.assertEqual(journal.deliver_approved(self.db, 1, draft.entry_id, "https://site.test", "key", opener=no_send).reason,
                         "ballad_publication_changed")
        no_send.assert_not_called()

    def test_frozen_packet_outage_holds_exact_inputs_then_withdrawal_retires_them(self):
        packet = {**self.packet(), "sourceArchiveAvailable": True}
        _, run_id, epoch, _ = automation._claim_preparation(self.db, 1, "daily", journal_fixture.START, journal_fixture.END, force=True)
        frozen, digest, reason = automation._freeze_or_load_packet(self.db, 1, run_id, epoch, lambda: packet)
        self.assertEqual(reason, "")
        self.http.side_effect = urllib.error.URLError("offline")
        never_build = mock.Mock(side_effect=AssertionError("no replacement packet"))
        _, _, reason = automation._freeze_or_load_packet(self.db, 1, run_id, epoch, never_build)
        self.assertEqual(reason, "ballad_publication_unavailable")
        with sqlite3.connect(self.db) as conn:
            self.assertEqual(conn.execute("SELECT frozen_packet_hash FROM bnl_journal_automation_runs").fetchone()[0], digest)
        self.http.side_effect = lambda *a, **k: CatalogResponse([])
        _, _, reason = automation._freeze_or_load_packet(self.db, 1, run_id, epoch, never_build)
        self.assertEqual(reason, "ballad_publication_changed")
        with sqlite3.connect(self.db) as conn:
            self.assertIsNone(conn.execute("SELECT frozen_packet_json FROM bnl_journal_automation_runs").fetchone()[0])

    def prepared(self):
        self.fixture.add_show()
        result = automation.prepare_daily(self.db, 1, lambda packet, prompt: article_json(packet), target_day=date(2026, 8, 27), force=True)
        self.assertEqual(result.status, "prepared", result)
        return result

    def release(self, opener):
        return automation.release_daily(self.db, 1, "https://site.test", "key", target_day=date(2026, 8, 27), force=True, opener=opener)

    def test_scheduled_outage_preserves_exact_payload_then_restart_delivers_once(self):
        prepared = self.prepared()
        with sqlite3.connect(self.db) as conn:
            canonical = bytes(conn.execute("SELECT canonical_payload_bytes FROM bnl_journal_entries").fetchone()[0])
            meta = json.loads(conn.execute("SELECT metadata_json FROM bnl_journal_private_metadata").fetchone()[0])
        self.assertTrue(any(s["sourceKind"] == "published_ballad" for s in meta["sharedInputSourceProvenance"]))
        self.http.side_effect = urllib.error.URLError("offline")
        no_send = mock.Mock(side_effect=AssertionError("no send without authority"))
        result = self.release(no_send)
        self.assertEqual(result.reason, "ballad_publication_unavailable")
        with sqlite3.connect(self.db) as conn:
            self.assertEqual(bytes(conn.execute("SELECT canonical_payload_bytes FROM bnl_journal_entries").fetchone()[0]), canonical)
            self.assertEqual(conn.execute("SELECT journal_entry_id FROM bnl_journal_automation_runs").fetchone()[0], prepared.entry_id)
        self.http.side_effect = lambda *a, **k: CatalogResponse(self.catalog)
        posts = []
        def accept(request, timeout=10):
            posts.append(request.data)
            return AcceptedResponse(request)
        result = self.release(accept)
        self.assertTrue(result.ok, result)
        self.assertTrue(self.release(accept).ok)
        self.assertEqual(posts, [canonical])

    def test_final_publication_check_follows_local_release_fence_without_sql_write_lock(self):
        self.prepared()
        original = automation._prepared_invalidation_reason
        calls = []
        def local_check(*args, **kwargs):
            calls.append(1)
            if len(calls) == 2:  # claim, then final delivery preflight
                self.catalog.clear()
            return original(*args, **kwargs)
        def catalog_read(*args, **kwargs):
            with sqlite3.connect(self.db, timeout=0.01) as conn:
                conn.execute("CREATE TABLE IF NOT EXISTS unrelated_writer(value INTEGER)")
                conn.execute("INSERT INTO unrelated_writer VALUES (1)")
            return CatalogResponse(self.catalog)
        self.http.side_effect = catalog_read
        with mock.patch.object(automation, "_prepared_invalidation_reason", side_effect=local_check):
            result = self.release(mock.Mock(side_effect=AssertionError("withdrawn release must not post")))
        self.assertEqual(result.reason, "ballad_publication_changed")
        with sqlite3.connect(self.db) as conn:
            self.assertEqual(conn.execute("SELECT lifecycle_state,journal_entry_id,frozen_packet_json FROM bnl_journal_automation_runs").fetchone(), ("held", None, None))

    def test_weekly_uses_release_metadata_as_reflection_not_a_fresh_show_or_participant(self):
        self.fixture.add_show()
        start, end, _ = automation._weekly_period_for_monday(date(2026, 8, 24))
        packet, _, _ = automation._weekly_packet(self.db, 1, start, end)
        self.assertEqual(len([s for s in packet["reflectionBasis"] if s["basisKind"] == "published_ballad"]), 1)
        self.assertFalse(any(s["sourceKind"] == "published_ballad" for s in packet["safeSources"]))
        self.assertEqual(packet["aggregateCounts"]["participants"], 0)


class BalladRelayInputsTests(unittest.TestCase):
    def setUp(self):
        self.fixture = relay_fixture.RelaySharedInputsTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.db = self.fixture.db
        with sqlite3.connect(self.db) as conn:
            conn.execute("UPDATE memory_moment_windows SET public_usable=0")
        self.catalog = seed_ballad(self.db, (self.fixture.now - timedelta(days=1)).isoformat())
        patch = mock.patch("urllib.request.urlopen", side_effect=lambda *a, **kw: CatalogResponse(self.catalog))
        self.http = patch.start()
        self.addCleanup(patch.stop)

    def test_real_relay_quiet_rotation_has_published_ballad_metadata(self):
        sources = relay_fixture.bot._select_shared_relay_sources(1, 20, 20, "")
        selected = [s for s in sources if s.source_class == "published_ballad"]
        self.assertEqual(len(selected), 1)
        self.assertIn("The Chairs Stayed Warm", selected[0].context)
        self.assertNotIn("LYRICS_ARE_NOT_TESTIMONY", selected[0].context)
        self.assertEqual(selected[0].metadata["shared_source_provenance"][0]["versionId"], "released-1")

    def transaction(self, *, generate=None, post=None, unavailable=False):
        def transport(request, timeout=10):
            if request.get_method() == "GET":
                if unavailable:
                    raise urllib.error.URLError("offline")
                return CatalogResponse(self.catalog)
            return (post or self.fixture.accept)(request, timeout)
        return self.fixture.transaction(generate or (lambda *a, **k:
            "An earlier Broadcast Ballad set its last-light theme in chamber soul.\nWhich musical detail in that release deserves another listen?"), transport)

    def test_actual_relay_writer_delivers_published_ballad_with_saved_basis(self):
        result = self.transaction()
        self.assertTrue(result.publish, result)
        self.assertEqual(result.eventType, "published_ballad")
        basis = json.loads(relay.recent_history(self.db, 1)[0]["source_basis_json"])
        self.assertEqual(basis[0]["versionId"], "released-1")
        self.assertEqual(relay.get_cursor(self.db, 1), 20)

    def test_withdrawal_during_relay_generation_prevents_post_and_pending_save(self):
        async def generate(*a, **k):
            self.catalog.clear()
            return "An earlier Ballad carried the last-light theme into chamber soul.\nWhich part of that release deserves another listen?"
        result = self.transaction(generate=generate, post=mock.Mock(side_effect=AssertionError("no stale send")))
        self.assertEqual(result.skipReason, "relay_source_changed")
        self.assertEqual(relay.get_pending_v2_publication(self.db, 1), {})

    def test_relay_restart_outage_preserves_exact_bytes_and_never_regenerates(self):
        self.transaction(post=self.fixture.fail_delivery)
        pending = relay.get_pending_v2_publication(self.db, 1)
        self.assertTrue(pending)
        self.fixture.reset_process()
        no_generate = mock.Mock(side_effect=AssertionError("saved payload must not be rewritten"))
        result = self.transaction(generate=no_generate, unavailable=True)
        self.assertEqual(result.skipReason, "relay_source_unavailable")
        self.assertEqual(relay.get_pending_v2_publication(self.db, 1), pending)
        posts = []
        def accept(req, timeout=10):
            posts.append(req.data)
            return self.fixture.accept(req, timeout)
        self.assertTrue(self.transaction(generate=no_generate, post=accept).publish)
        self.assertEqual(posts, [pending["canonical_json"].encode()])
        self.assertEqual(relay.get_pending_v2_publication(self.db, 1), {})

    def test_changed_audio_after_restart_retires_saved_relay_without_cursor_advance(self):
        self.transaction(post=self.fixture.fail_delivery)
        self.fixture.reset_process()
        self.catalog[0]["audioId"] = "replacement-audio"
        result = self.transaction(post=mock.Mock(side_effect=AssertionError("no stale send")))
        self.assertEqual(result.skipReason, "relay_source_changed")
        self.assertEqual(relay.get_pending_v2_publication(self.db, 1), {})
        self.assertEqual(relay.get_cursor(self.db, 1), 20)

    def test_each_existing_http_attempt_rechecks_publication(self):
        posts = []
        def post(request, timeout=10):
            posts.append(request.data)
            self.catalog.clear()
            raise urllib.error.URLError("lost response")
        result = self.transaction(post=post)
        self.assertEqual(result.skipReason, "relay_source_changed")
        self.assertEqual(len(posts), 1)

    def test_publication_io_stays_off_event_loop_and_historical_claim_guard_applies(self):
        event_thread = threading.get_ident()
        reader = ballads.read_publication_catalog
        threads = []
        def read(*a, **k):
            threads.append(threading.get_ident())
            return reader(*a, **k)
        with mock.patch.object(ballads, "read_publication_catalog", side_effect=read):
            result = self.transaction()
        self.assertTrue(result.publish, result)
        self.assertTrue(threads)
        self.assertNotIn(event_thread, threads)
        selected = next(s for s in relay_fixture.bot._select_shared_relay_sources(1, 20, 20, "") if s.source_class == "published_ballad")
        with mock.patch.object(relay_fixture.bot, "_is_relay_show_window_active", return_value=True), \
             mock.patch.object(relay_fixture.bot, "_select_approved_quiet_relay_source", return_value=selected):
            result = self.transaction(generate=lambda *a, **k: "The broadcast is live tonight, with reporters checking the signal.\n" + relay_fixture.COPY.splitlines()[1])
        self.assertFalse(result.publish)
        self.assertEqual(result.skipReason, "historical_source_current_claim")
