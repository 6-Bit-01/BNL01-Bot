"""Public show history remains usable independently of current queue access."""

import copy
import hashlib
import json
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl01_bot as bot
import bnl_canon_source_contract as contract
from bnl_journal_source_store import ensure_schema
from bnl_tiktok_show_ledger import sync_tiktok_show_evidence_ledgers
from test_rehearsal_read_model import REQUEST, rehearsal_model
from test_tiktok_show_evidence_ledger import ENABLED_QUEUE_ENV, archived_show, authorized_read_model


def public_history_model(scope="public"):
    model = authorized_read_model({"latestShow": archived_show(), "shows": [archived_show()]})
    history = model["sections"]["archive"].copy()
    history.update(
        schemaVersion="queue_bnl_public_history_v1",
        source="queue_bnl_public_history_projection",
        publicOnly=True, mutationAllowed=False, currentSessionId=None,
        builtAt="2026-08-29T00:12:00Z",
    )
    history.pop("latestShow")
    history.pop("personalHistory")
    model["sections"]["publicHistory"] = history
    model["accessScope"] = scope
    model["publicOnly"] = scope != "private"
    if scope == "private":
        model["sections"].update(rehearsal_model()["sections"])
    elif scope == "none":
        model["sections"]["archive"] = {"available": False, "reason": "queue_access_none"}
    seal_history(model)
    return model


def seal_history(model):
    history = model["sections"]["publicHistory"]
    payload = {key: history[key] for key in (
        "schemaVersion", "historyCoverageStartedAt", "currentSessionId", "shows",
    )}
    history["sourceDigest"] = hashlib.sha256(json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
    ).encode()).hexdigest()


class PublicShowHistoryTests(unittest.TestCase):
    def test_independent_public_authority_does_not_change_current_queue_authority(self):
        for scope in ("public", "private", "none"):
            with self.subTest(scope=scope):
                model = public_history_model(scope)
                auth = contract.show_queue_evidence_authorization(model, environ=ENABLED_QUEUE_ENV)
                self.assertTrue(auth["usable"], auth)
                self.assertTrue(contract.show_queue_evidence_authorization_receipt_valid(auth["receipt"]))
                self.assertEqual(auth["receipt"]["archiveSchemaVersion"], "queue_bnl_public_history_v1")
                self.assertEqual(contract.queue_usability(model, environ=ENABLED_QUEUE_ENV)["usable"], scope == "public")
                archive = contract.public_show_evidence_archive(model, environ=ENABLED_QUEUE_ENV)
                self.assertEqual(archive["latestShow"]["sessionId"], "show-attendance-1")
                self.assertIsNone(archive["currentShow"])
                stripped = contract.strip_queue_sections(model, environ=ENABLED_QUEUE_ENV)
                self.assertEqual(stripped["sections"]["publicHistory"], model["sections"]["publicHistory"])
                if scope != "public":
                    self.assertNotIn("private-rehearsal", json.dumps(stripped))

    def test_public_current_and_completed_records_are_adapted_without_duplicates(self):
        model = public_history_model("none")
        current = copy.deepcopy(archived_show())
        current.update(sessionId="public-current", status="open", showDate="2026-09-04")
        history = model["sections"]["publicHistory"]
        history["shows"].insert(0, current)
        history["currentSessionId"] = "public-current"
        seal_history(model)
        archive = contract.public_show_evidence_archive(model, environ=ENABLED_QUEUE_ENV)
        self.assertEqual(archive["currentShow"], current)
        self.assertEqual([show["sessionId"] for show in archive["shows"]], ["show-attendance-1"])
        self.assertEqual(archive["latestShow"]["sessionId"], "show-attendance-1")

    def test_withdrawn_or_invalid_new_contract_never_falls_back_to_legacy_archive(self):
        cases = (
            {"available": False, "reason": "disabled"},
            {"visibility": "private"}, {"accessScope": "private"},
            {"publicOnly": False}, {"mutationAllowed": True},
            {"sourceRevision": True}, {"sourceDigest": "b" * 64},
            {"source": "queue_bnl_history_projection"},
            {"currentSessionId": "private-rehearsal"},
            {"shows": [dict(archived_show(), isSimulation=True)]},
        )
        for changes in cases:
            with self.subTest(changes=changes):
                model = public_history_model()
                model["sections"]["publicHistory"].update(changes)
                if "shows" in changes or "currentSessionId" in changes:
                    seal_history(model)
                self.assertFalse(contract.show_queue_evidence_authorization(model, environ=ENABLED_QUEUE_ENV)["usable"])
                self.assertEqual(contract.public_show_evidence_archive(model, environ=ENABLED_QUEUE_ENV), {})
                stripped = contract.strip_queue_sections(model, environ=ENABLED_QUEUE_ENV)
                self.assertFalse(stripped["sections"]["publicHistory"]["available"])
        for value in (None, [], "invalid"):
            model = public_history_model()
            model["sections"]["publicHistory"] = value
            self.assertEqual(contract.public_show_evidence_archive(model, environ=ENABLED_QUEUE_ENV), {})

    def test_both_existing_production_gates_still_apply(self):
        for local, remote in ((False, True), (True, False), (True, None)):
            model = public_history_model("private")
            model["capabilities"]["queueProduction"] = remote
            env = {"BNL_QUEUE_PRODUCTION_ENABLED": str(local).lower()}
            self.assertFalse(contract.show_queue_evidence_authorization(model, environ=env)["usable"])
            stripped = contract.strip_queue_sections(model, environ=env, allow_private=True)
            self.assertNotIn("private-rehearsal", json.dumps(stripped))
            self.assertNotIn("show-attendance-1", json.dumps(stripped))

    def test_legacy_receipts_remain_valid_but_schema_source_pairs_cannot_mix(self):
        model = authorized_read_model({"latestShow": archived_show()})
        receipt = contract.show_queue_evidence_authorization(model, environ=ENABLED_QUEUE_ENV)["receipt"]
        self.assertTrue(contract.show_queue_evidence_authorization_receipt_valid(receipt))
        receipt["archiveSource"] = "queue_bnl_public_history_projection"
        self.assertFalse(contract.show_queue_evidence_authorization_receipt_valid(receipt))
        receipt["archiveSource"] = []
        self.assertFalse(contract.show_queue_evidence_authorization_receipt_valid(receipt))
        model.update(accessScope="private", publicOnly=False)
        self.assertFalse(contract.show_queue_evidence_authorization(model, environ=ENABLED_QUEUE_ENV)["usable"])

    def test_private_current_queue_cannot_block_or_enter_durable_public_show_history(self):
        with tempfile.TemporaryDirectory() as directory:
            db = str(Path(directory) / "memory.db")
            ensure_schema(db)
            result = sync_tiktok_show_evidence_ledgers(
                db, guild_id=77, read_model=public_history_model("private"), environ=ENABLED_QUEUE_ENV,
            )
            self.assertEqual(result["status"], "completed", result)
            with sqlite3.connect(db) as conn:
                rows = conn.execute("SELECT show_key,ledger_json FROM tiktok_show_evidence_ledgers").fetchall()
            self.assertEqual([row[0] for row in rows], ["show-attendance-1"])
            self.assertNotIn("private-rehearsal", str(rows))

    def test_public_prompt_uses_history_even_when_current_queue_is_private_or_none(self):
        with mock.patch.dict(os.environ, ENABLED_QUEUE_ENV):
            for scope in ("private", "none"):
                with self.subTest(scope=scope):
                    context = bot.build_bnl_read_model_context(
                        public_history_model(scope), "Recap the August 28, 2026 show", "public_home",
                    )
                    self.assertIn("2026-08-28", context)
                    self.assertIn("Neon Fox", context)
                    self.assertNotIn("private-rehearsal", context)
                    self.assertNotIn("does not authorize", context)
                    self.assertNotIn("public show-history projection is unavailable", context)

    def test_independent_history_preserves_private_rehearsal_playback_and_channel_limits(self):
        with mock.patch.dict(os.environ, ENABLED_QUEUE_ENV):
            model = public_history_model("private")
            context = bot.build_bnl_read_model_context(model, REQUEST, "sealed_test")
            self.assertIn("sessionId=private-rehearsal", context)
            self.assertIn("Test Artist B — B2 Complete", context)
            self.assertEqual(context.count("actualPlayback=confirmed"), 2)
            public_context = bot.build_bnl_read_model_context(model, REQUEST, "public_home")
            self.assertNotIn("private-rehearsal", public_context)
            self.assertNotIn("B2 Complete", public_context)

    def test_current_private_recap_and_last_public_recap_keep_their_own_records(self):
        model = public_history_model("private")
        model["sections"]["archive"]["currentShow"].update(
            showDate="2026-09-15", status="open", title="Private rehearsal",
        )
        with mock.patch.dict(os.environ, ENABLED_QUEUE_ENV):
            current = bot.build_bnl_read_model_context(
                model, "Recap the current private rehearsal show.", "sealed_test",
            )
            self.assertIn("Show=Private rehearsal; showDate=2026-09-15", current)
            self.assertNotIn("showDate=2026-08-28", current)
            public = bot.build_bnl_read_model_context(
                model, "Recap the current private rehearsal show.", "public_home",
            )
            self.assertNotIn("Show=Private rehearsal", public)
            self.assertNotIn("showDate=2026-08-28", public)
            for query in ("Recap the last show.", "Recap the last public show.", "Recap the August 28, 2026 show."):
                history = bot.build_bnl_read_model_context(model, query, "sealed_test")
                self.assertIn("Show=BARCODE Radio; showDate=2026-08-28", history)
                self.assertNotIn("Show=Private rehearsal", history)
