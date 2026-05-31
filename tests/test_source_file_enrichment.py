import asyncio
import json
import os
import sqlite3
import tempfile
import unittest
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl_source_file_enrichment as enrich
import bnl01_bot


class SourceFileEnrichmentTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False)
        self.tmp.close()
        self.db = self.tmp.name
        self.conn = sqlite3.connect(self.db)
        self._schema()

    def tearDown(self):
        self.conn.close()
        os.unlink(self.db)

    def _schema(self):
        c = self.conn.cursor()
        c.execute("CREATE TABLE user_profiles (user_id INTEGER, guild_id INTEGER, display_name TEXT, preferred_name TEXT, last_seen TEXT, last_greeting_at TEXT)")
        c.execute("CREATE TABLE user_memory_facts (id INTEGER PRIMARY KEY, user_id INTEGER, guild_id INTEGER, fact_key TEXT, fact_value TEXT, confidence REAL, is_core INTEGER, updated_at TEXT)")
        c.execute("CREATE TABLE user_habits (user_id INTEGER, guild_id INTEGER, total_messages INTEGER, question_messages INTEGER, humor_messages INTEGER, late_night_messages INTEGER, avg_length REAL, last_topic TEXT, updated_at TEXT)")
        c.execute("CREATE TABLE relationship_state (user_id INTEGER, guild_id INTEGER, interaction_count INTEGER, affinity_score REAL, trust_stage TEXT, social_stance TEXT, last_topic TEXT, updated_at TEXT)")
        c.execute("CREATE TABLE relationship_journal (id INTEGER PRIMARY KEY, user_id INTEGER, guild_id INTEGER, entry_type TEXT, summary TEXT, timestamp TEXT)")
        c.execute("CREATE TABLE conversations (id INTEGER PRIMARY KEY, user_id INTEGER, user_name TEXT, guild_id INTEGER, channel_name TEXT, channel_policy TEXT, role TEXT, content TEXT, timestamp TEXT)")
        c.execute("CREATE TABLE memory_tiers (id INTEGER PRIMARY KEY, user_id INTEGER, guild_id INTEGER, tier TEXT, summary TEXT, salience REAL, mentions INTEGER, updated_at TEXT, source_trust TEXT, source_channel_policy TEXT)")
        c.execute("CREATE TABLE broadcast_memory (id INTEGER PRIMARY KEY, guild_id INTEGER, episode_date TEXT, cleaned_summary TEXT, entry_type TEXT, public_safe INTEGER, usage_scope TEXT, status TEXT, created_at TEXT)")
        c.execute("CREATE TABLE community_presence (guild_id INTEGER, subject_key TEXT, display_name TEXT, first_seen_at TEXT, last_seen_at TEXT, source_lanes TEXT, approved_channel_labels TEXT, mention_count INTEGER, direct_interaction_count INTEGER, operator_mention_count INTEGER, active_windows TEXT, connection_notes TEXT, evidence_snippets TEXT, category TEXT, last_error_status TEXT)")
        self.conn.commit()

    def _lookup(self, status="active"):
        return lambda query: {
            "ok": True,
            "found": True,
            "matchKind": "exact",
            "data": {"sourceFile": {"id": "sf_1", "name": query["lookupValue"], "status": status, "candidateType": "entity"}},
        }

    def test_parses_source_enrich_dry_run(self):
        matched, options, error = enrich.parse_source_enrichment_command("!bnl source enrich Hellcat | dry_run=true")
        self.assertTrue(matched)
        self.assertFalse(error)
        self.assertEqual(options["subject"], "Hellcat")
        self.assertTrue(options["dry_run"])
        for command in ("!bnl enrich source Emerald", "!bnl source file enrich Mac Modem | dry_run=false"):
            matched, options, error = enrich.parse_source_enrichment_command(command)
            self.assertTrue(matched)
            self.assertFalse(error)

    def test_enriches_active_source_file_with_structured_sections(self):
        c = self.conn.cursor()
        c.execute("INSERT INTO user_profiles VALUES (10,1,'Hellcat','Hellcat',NULL,NULL)")
        c.execute("INSERT INTO user_memory_facts VALUES (NULL,10,1,'role','Hellcat is a recurring artist candidate.',0.9,1,'now')")
        c.execute("INSERT INTO broadcast_memory VALUES (NULL,1,'2026-05-30','Hellcat was referenced in a public-safe BARCODE Radio note.','show_note',1,'ambient,direct','active','now')")
        self.conn.commit()
        calls = []
        result = enrich.run_source_file_enrichment(self.db, 1, "Hellcat", dry_run=False, lookup_func=self._lookup("active"), sender=lambda payload: calls.append(payload) or {"ok": True, "status": 200})
        self.assertTrue(result["sent"])
        self.assertEqual(result["matchKind"], "active_source_file")
        self.assertIn("Subject Overview", result["sections"])
        self.assertIn("Known Facts", result["sections"])
        self.assertIn("Public-Safe Notes", result["sections"])
        self.assertEqual(calls[0]["type"], "modify_existing_dossier")
        self.assertIn("review-only", calls[0]["reason"].lower())

    def test_candidate_intake_and_existing_dossier_are_labeled(self):
        candidate = enrich.run_source_file_enrichment(self.db, 1, "Emerald", dry_run=True, lookup_func=self._lookup("candidate_intake"))
        self.assertEqual(candidate["matchKind"], "candidate_intake")
        self.assertIn("Candidate Intake", enrich.format_source_enrichment_response(candidate))
        dossier_lookup = lambda query: {"ok": True, "found": True, "data": {"sourceFile": {"name": "Crow", "type": "public_dossier", "targetDossierId": "d1"}}}
        dossier = enrich.run_source_file_enrichment(self.db, 1, "Crow", dry_run=True, lookup_func=dossier_lookup)
        self.assertEqual(dossier["matchKind"], "existing_dossier_update")
        self.assertIn("Existing Dossier Update", enrich.format_source_enrichment_response(dossier))

    def test_no_match_returns_bounded_no_target_response(self):
        result = enrich.run_source_file_enrichment(self.db, 1, "Unknown", dry_run=True, lookup_func=lambda query: {"ok": True, "found": False, "data": {}})
        response = enrich.format_source_enrichment_response(result)
        self.assertEqual(result["status"], "no_target")
        self.assertIn("create/promote", response)
        self.assertNotIn("payload", response.lower())

    def test_source_blind_memory_review_only_and_alias_not_confirmed(self):
        c = self.conn.cursor()
        c.execute("INSERT INTO memory_tiers VALUES (NULL,10,1,'long','Hellcat may be connected to Emerald in old source-blind memory.',0.9,1,'now','legacy_unknown','legacy_unknown')")
        c.execute("INSERT INTO conversations VALUES (NULL,10,'User',1,'general','public_home','user','Hellcat aka Emerald is a possible connection review, not confirmed identity. 123456789012345678','now')")
        self.conn.commit()
        result = enrich.run_source_file_enrichment(self.db, 1, "Hellcat", dry_run=True, lookup_func=self._lookup("active"))
        self.assertIn(enrich.SOURCE_BLIND_WARNING, result["warnings"])
        self.assertIn("Internal-Only / Review-Only Notes", result["sections"])
        alias_text = json.dumps(result["sections"].get("Possible Aliases / Connections", []))
        self.assertIn("not confirmed", alias_text.lower())
        response = enrich.format_source_enrichment_response(result)
        self.assertNotIn("123456789012345678", response)
        self.assertNotIn("payload", response.lower())

    def test_dry_run_does_not_send_and_ingest_key_is_deterministic(self):
        calls = []
        first = enrich.run_source_file_enrichment(self.db, 1, "Hellcat", dry_run=True, lookup_func=self._lookup("active"), sender=lambda payload: calls.append(payload) or {"ok": True})
        second = enrich.run_source_file_enrichment(self.db, 1, "Hellcat", dry_run=True, lookup_func=self._lookup("active"), sender=lambda payload: calls.append(payload) or {"ok": True})
        self.assertEqual(calls, [])
        self.assertEqual(first["ingestKey"], second["ingestKey"])
        self.assertEqual(first["ingestSource"], enrich.FALLBACK_INGEST_SOURCE)

    def test_does_not_invent_unsupported_sections(self):
        result = enrich.run_source_file_enrichment(self.db, 1, "Mac Modem", dry_run=True, lookup_func=self._lookup("active"))
        self.assertNotIn("Observed Patterns", result["sections"])
        self.assertIn("Missing Info", result["sections"])


class SourceFileEnrichmentBotTests(unittest.TestCase):
    class Perms:
        administrator = False
        manage_guild = False
        manage_messages = False
        kick_members = False
        ban_members = False

    class User:
        def __init__(self, user_id):
            self.id = user_id
            self.guild_permissions = SourceFileEnrichmentBotTests.Perms()
            self.roles = []

    class Guild:
        id = 1
        owner_id = 999
        def get_member(self, user_id):
            return None

    class Channel:
        id = 10
        parent = None
        def __init__(self, name):
            self.name = name

    class Message:
        def __init__(self, user_id=999, channel_name="research-and-development"):
            self.author = SourceFileEnrichmentBotTests.User(user_id)
            self.guild = SourceFileEnrichmentBotTests.Guild()
            self.channel = SourceFileEnrichmentBotTests.Channel(channel_name)
            self.replies = []
        async def reply(self, text):
            self.replies.append(text)

    def test_rejects_unauthorized_user(self):
        bnl01_bot.BNL_OWNER_USER_ID = 999
        message = self.Message(user_id=111)
        with mock.patch.object(bnl01_bot, "run_source_file_enrichment") as run_mock:
            handled = asyncio.run(bnl01_bot.maybe_handle_source_file_enrichment_command(message, "!bnl source enrich Hellcat | dry_run=true"))
        self.assertTrue(handled)
        run_mock.assert_not_called()
        self.assertIn("operator-only", message.replies[-1])

    def test_rejects_public_channel_use(self):
        bnl01_bot.BNL_OWNER_USER_ID = 999
        message = self.Message(user_id=999, channel_name="general")
        with mock.patch.object(bnl01_bot, "resolve_channel_policy", return_value="public_home"), mock.patch.object(bnl01_bot, "run_source_file_enrichment") as run_mock:
            handled = asyncio.run(bnl01_bot.maybe_handle_source_file_enrichment_command(message, "!bnl source enrich Hellcat | dry_run=true"))
        self.assertTrue(handled)
        run_mock.assert_not_called()
        self.assertIn("restricted", message.replies[-1])

    def test_real_run_reports_send_failure_safely_and_diagnostics_update(self):
        bnl01_bot.BNL_OWNER_USER_ID = 999
        message = self.Message(user_id=999)
        fake = {"ok": True, "dryRun": False, "subject": "Hellcat", "status": "send_failed", "matchKind": "active_source_file", "sections": {"Subject Overview": ["x"]}, "sourceCounts": {"source_file_lookup": 1}, "warningCounts": {}, "sourceTypes": ["source_file_lookup"], "warnings": [], "sent": False, "sendResult": {"error": "site support missing"}}
        with mock.patch.object(bnl01_bot, "run_source_file_enrichment", return_value=fake):
            handled = asyncio.run(bnl01_bot.maybe_handle_source_file_enrichment_command(message, "!bnl source enrich Hellcat"))
        self.assertTrue(handled)
        self.assertIn("not sent", message.replies[-1])
        diag = bnl01_bot.build_dossier_recommendation_diagnostics(1)
        self.assertTrue(diag["source_enrichment_available"])
        self.assertEqual(diag["source_enrichment_last_subject"], "Hellcat")
        self.assertEqual(diag["source_enrichment_last_match_kind"], "active_source_file")


if __name__ == "__main__":
    unittest.main()
