import os
import sqlite3
import tempfile
import unittest

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

from bnl_entity_intelligence import (
    build_entity_context_for_public_conversation,
    build_entity_intelligence_profile,
    ensure_entity_intelligence_schema,
    mark_entity_scouting_stale,
    resolve_authoritative_fact,
    resolve_entity_context_for_surface,
)


class EntityIntelligenceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False)
        self.tmp.close()
        self.db = self.tmp.name
        self.conn = sqlite3.connect(self.db)
        self.conn.row_factory = sqlite3.Row
        c = self.conn.cursor()
        c.execute("CREATE TABLE user_profiles (user_id INTEGER, guild_id INTEGER, display_name TEXT, preferred_name TEXT, last_seen TEXT, last_greeting_at TEXT)")
        c.execute("CREATE TABLE conversations (id INTEGER PRIMARY KEY, user_id INTEGER, user_name TEXT, guild_id INTEGER, channel_name TEXT, channel_policy TEXT, role TEXT, content TEXT, timestamp TEXT)")
        c.execute("CREATE TABLE memory_tiers (id INTEGER PRIMARY KEY, user_id INTEGER, guild_id INTEGER, tier TEXT, summary TEXT, salience REAL, mentions INTEGER, updated_at TEXT, source_trust TEXT, source_channel_policy TEXT)")
        c.execute("CREATE TABLE broadcast_memory (id INTEGER PRIMARY KEY, guild_id INTEGER, episode_date TEXT, cleaned_summary TEXT, entry_type TEXT, public_safe INTEGER, usage_scope TEXT, status TEXT, created_at TEXT)")
        c.execute("CREATE TABLE community_presence (guild_id INTEGER, subject_key TEXT, display_name TEXT, first_seen_at TEXT, last_seen_at TEXT, source_lanes TEXT, approved_channel_labels TEXT, mention_count INTEGER, direct_interaction_count INTEGER, operator_mention_count INTEGER, active_windows TEXT, connection_notes TEXT, evidence_snippets TEXT, category TEXT, last_error_status TEXT)")
        self.conn.commit()

    def tearDown(self):
        self.conn.close()
        os.unlink(self.db)

    def add_profile(self, user_id, name):
        self.conn.execute("INSERT INTO user_profiles VALUES (?,1,?,?,NULL,NULL)", (user_id, name, name))

    def add_msg(self, user_id, name, channel, policy, content):
        self.conn.execute("INSERT INTO conversations VALUES (NULL,?,?,?,?,?,?,?,'now')", (user_id, name, 1, channel, policy, "user", content))

    def profile_for(self, name):
        self.conn.commit()
        return build_entity_intelligence_profile(self.db, 1, name, refresh=True)

    def labels(self, items):
        return "\n".join(str(i.get("label") or i.get("objectLabel") or i.get("relationType") or "") + " " + str(i.get("value") or "") for i in items)

    def test_schema_and_stale_queue_are_idempotent(self):
        ensure_entity_intelligence_schema(self.conn)
        ensure_entity_intelligence_schema(self.conn)
        mark_entity_scouting_stale(self.conn, 1, "Test Subject", reason="new eligible public evidence", source="entity_evidence_events")
        row = self.conn.execute("SELECT subject_key, status FROM entity_scouting_queue").fetchone()
        self.assertEqual(row["subject_key"], "test-subject")
        self.assertEqual(row["status"], "stale")

    def test_crow_like_fixture_relationship_music_bnl_and_actions(self):
        self.add_profile(10, "Crow")
        self.add_msg(10, "Crow", "general", "public_home", "Orion through Crow is how this persona speaks with BNL about source-file boundaries.")
        self.add_msg(10, "Crow", "general", "public_home", "Orion here, BNL. Crow created Orion as my AI project persona.")
        self.add_msg(10, "Crow", "finished-tracks", "public_music", "Crow shared a Suno track link https://suno.com/song/abc and asked BNL about dossiers.")
        p = self.profile_for("Crow")
        rels = {(e["objectLabel"], e["relationType"]) for e in p["relationships"]}
        self.assertIn(("Orion", "speaks_through"), rels)
        self.assertIn(("Orion", "created"), rels)
        self.assertIn("shares Suno/Udio/YouTube/SoundCloud/Spotify/Bandcamp links", self.labels(p["creativeMusic"]))
        self.assertIn("frequent BNL-facing conversation", self.labels(p["bnlInteraction"]))
        self.assertIn("AI/persona/project interaction", self.labels(p["conversationThemes"]))
        self.assertIn("Confirm whether Orion relationship/context can be public", self.labels(p["actionItems"]))
        self.assertEqual((p["diagnostics"] or {})["queueSubmissionStatus"], "not_connected")

    def test_lost_marbles_like_mod_music_collab_fixture(self):
        self.add_profile(11, "Lost Marbles")
        self.add_msg(11, "Lost Marbles", "collaboration-hub", "public_music", "As a mod and staff helper, Lost Marbles can collab with artists and share SoundCloud music demos.")
        p = self.profile_for("Lost Marbles")
        self.assertIn("moderator", self.labels(p["roles"]))
        self.assertIn("shares Suno/Udio/YouTube/SoundCloud/Spotify/Bandcamp links", self.labels(p["creativeMusic"]))
        self.assertIn("offers collaboration", self.labels(p["creativeMusic"]))
        self.assertIn("Confirm public-safe mod/staff role wording", self.labels(p["actionItems"]))
        self.assertIn("Confirm public artist links", self.labels(p["actionItems"]))
        self.assertEqual(p["diagnostics"]["queueSubmissionStatus"], "not_connected")

    def test_shortybabe_like_challenging_fixture_stays_out_of_public_copy(self):
        self.add_profile(12, "Shortybabe")
        self.add_msg(12, "Shortybabe", "general", "public_home", "BNL you are wrong, prove it. I challenge your source file behavior and I am baiting the bot.")
        self.add_msg(12, "Shortybabe", "general", "public_home", "Still arguing with BNL and teasing the bot boundaries.")
        p = self.profile_for("Shortybabe")
        self.assertIn("antagonistic/challenging", self.labels(p["behaviorPosture"]))
        self.assertIn("frequent BNL-facing conversation", self.labels(p["bnlInteraction"]))
        public = resolve_entity_context_for_surface(p, "public_conversation")
        public_text = str(public)
        self.assertNotIn("antagonistic", public_text)
        self.assertNotIn("defamatory", public_text)
        self.assertGreater(public["excludedReasonCounts"].get("not_public_safe", 0), 0)

    def test_antigrain_like_anomaly_fixture(self):
        self.add_profile(13, "Antigrain")
        self.add_msg(13, "Antigrain", "general", "public_home", "BNL, this anomaly theory and weird signal lore keeps glitching around the dossier.")
        p = self.profile_for("Antigrain")
        self.assertIn("strange/anomaly conversations", self.labels(p["conversationThemes"]))
        self.assertIn("frequent BNL-facing conversation", self.labels(p["bnlInteraction"]))
        self.assertIn("Determine whether anomaly/lore context is public-safe lore or internal-only", self.labels(p["actionItems"]))

    def test_hellcat_like_mod_contest_fixture(self):
        self.add_profile(14, "Hellcat")
        self.add_msg(14, "Hellcat", "contest-room", "public_home", "Hellcat mod post: contest rules, deadline, entries, winner, prize, and event instructions for the channel.")
        p = self.profile_for("Hellcat")
        self.assertIn("moderator", self.labels(p["roles"]))
        self.assertIn("contest/event activity", self.labels(p["eventContest"]))
        self.assertIn("Confirm contest name, rules, channel, dates, and public link", self.labels(p["actionItems"]))
        draft = resolve_entity_context_for_surface(p, "dossier_draft")
        self.assertNotIn("rules, channel, dates", str(draft.get("allowedActionItems")))

    def test_generic_non_hardcoded_fixture(self):
        self.add_profile(15, "Nova Finch")
        self.add_msg(15, "Nova Finch", "collaboration-hub", "public_music", "Nova Finch is an artist asking for feedback on a demo and wants to collaborate with Blue Kite.")
        p = self.profile_for("Nova Finch")
        self.assertIn("artist", self.labels(p["roles"]))
        self.assertIn("offers collaboration", self.labels(p["creativeMusic"]))
        self.assertIn("collaboration", self.labels(p["conversationThemes"]))

    def test_surface_filters(self):
        self.add_profile(16, "Surface Test")
        self.add_msg(16, "Surface Test", "general", "public_home", "Surface Test artist shares music and asks for feedback.")
        self.conn.execute("INSERT INTO memory_tiers VALUES (NULL,16,1,'long','Surface Test private internal note with rawRefJson and conversations table mention.',0.9,1,'now','legacy_unknown','legacy_unknown')")
        p = self.profile_for("Surface Test")
        public = resolve_entity_context_for_surface(p, "public_conversation")
        ambient = resolve_entity_context_for_surface(p, "ambient_public")
        relay = resolve_entity_context_for_surface(p, "website_relay")
        rd = resolve_entity_context_for_surface(p, "rd_mod_ops")
        source = resolve_entity_context_for_surface(p, "source_file")
        draft = resolve_entity_context_for_surface(p, "dossier_draft")
        owner = resolve_entity_context_for_surface(p, "owner_review")
        diag = resolve_entity_context_for_surface(p, "internal_diagnostics")
        self.assertNotIn("private internal note", str(public))
        self.assertNotIn("rawRefJson", str(ambient))
        self.assertNotIn("rawRefJson", str(relay))
        self.assertIn("source_blind_memory", str(rd.get("allowedFacts")) + str(rd.get("excludedReasonCounts")))
        self.assertIn("missingInfo", source)
        self.assertNotIn("queue/submission bridge not_connected", str(draft.get("allowedFacts")))
        self.assertNotIn("rawRefJson", str(owner))
        self.assertIn("rowsScanned", diag["diagnostics"])
        self.assertNotIn("private internal note", str(diag))
        self.assertLessEqual(len(build_entity_context_for_public_conversation(p)), 4)

    def test_authority_winner_and_conflict_action(self):
        facts = [
            {"label": "role", "value": "public Discord says artist", "authority": "public_discord_observed", "confidence": .8},
            {"label": "role", "value": "official dossier says producer", "authority": "official_public_dossier", "confidence": .9},
        ]
        winner, conflicts = resolve_authoritative_fact(facts)
        self.assertEqual(winner["authority"], "official_public_dossier")
        self.assertTrue(conflicts)
        self.assertIn("Owner review needed", conflicts[0]["label"])


    def test_lost_marbles_global_memory_does_not_import_crow_or_orion(self):
        self.add_profile(11, "Lost Marbles")
        self.add_profile(10, "Crow")
        self.add_msg(11, "Lost Marbles", "collaboration-hub", "public_music", "Lost Marbles is a mod sharing SoundCloud music demos and offering to collab with artists.")
        self.conn.execute("INSERT INTO memory_tiers VALUES (NULL,NULL,1,'long','Orion through Crow appears near Lost Marbles in a source-blind global memory bundle.',0.9,1,'now',NULL,NULL)")
        p = self.profile_for("Lost Marbles")
        labels = self.labels(p["relationships"] + p["conversationThemes"] + p["creativeMusic"] + p["dossierUsefulness"])
        self.assertIn("offers collaboration", self.labels(p["creativeMusic"]))
        self.assertNotIn("Orion", labels)
        self.assertNotIn("Crow", labels)
        scopes = p["diagnostics"]["rowScopeCounts"]
        self.assertGreaterEqual(scopes.get("global_mixed_memory", 0) + scopes.get("source_blind_global", 0), 1)

    def test_antigrain_mixed_memory_does_not_import_signal_witch_or_orion(self):
        self.add_profile(13, "Antigrain")
        self.add_msg(13, "Antigrain", "general", "public_home", "Antigrain asks BNL about this strange anomaly and weird glitch signal lore.")
        self.conn.execute("INSERT INTO memory_tiers VALUES (NULL,NULL,1,'long','Signal Witch, Orion, Lardcode, and Antigrain were bundled in one source-blind global memory row.',0.9,1,'now',NULL,NULL)")
        p = self.profile_for("Antigrain")
        text = self.labels(p["relationships"] + p["conversationThemes"] + p["dossierUsefulness"])
        self.assertIn("strange/anomaly conversations", text)
        self.assertNotIn("Signal Witch", text)
        self.assertNotIn("Orion", text)
        self.assertGreaterEqual(p["diagnostics"].get("sourceBlindRows", 0), 1)

    def test_source_blind_memory_and_relationship_journal_do_not_create_edges(self):
        self.add_profile(20, "Shortybabe")
        self.add_profile(10, "Crow")
        self.conn.execute("CREATE TABLE relationship_journal (id INTEGER PRIMARY KEY, user_id INTEGER, guild_id INTEGER, entry_type TEXT, summary TEXT, timestamp TEXT)")
        self.conn.execute("INSERT INTO memory_tiers VALUES (NULL,NULL,1,'long','Orion through Crow and Shortybabe asked BNL in mixed source-blind memory.',0.9,1,'now',NULL,NULL)")
        self.conn.execute("INSERT INTO relationship_journal VALUES (NULL,NULL,1,'note','Shortybabe, Crow, and Orion were mentioned together in one relationship journal note.','now')")
        p = self.profile_for("Shortybabe")
        self.assertEqual([], p["relationships"])
        self.assertNotIn("Orion", self.labels(p["conversationThemes"] + p["bnlInteraction"]))
        self.assertGreaterEqual(p["diagnostics"].get("sourceBlindRows", 0), 1)


if __name__ == "__main__":
    unittest.main()
