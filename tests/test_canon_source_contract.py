import os
os.environ.setdefault("GEMINI_API_KEY", "test-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")
import unittest
from datetime import datetime, timedelta, timezone

import bnl_canon_source_contract as c
from bnl01_bot import BNL01_SYSTEM_PROMPT, build_bnl_read_model_context
from bnl_website_contract_v2 import build_relay_envelope


class CanonSourceContractTests(unittest.TestCase):
    def test_identity_and_founders(self):
        self.assertEqual(c.CANON_SOURCE_CONTRACT_VERSION, "canon_source_contract_v1")
        self.assertEqual(c.FOUNDING_MEMBERS, ("6 Bit", "DJ Floppydisc", "Cache Back", "Mac Modem"))

    def test_schedule_distinguishes_intake_show_first_track(self):
        self.assertEqual(c.FRIDAY_PUBLIC_SCHEDULE["intake_begins"], "6:40 PM Pacific")
        self.assertEqual(c.FRIDAY_PUBLIC_SCHEDULE["show_begins"], "7:00 PM Pacific")
        self.assertEqual(c.FRIDAY_PUBLIC_SCHEDULE["first_track_target"], "7:05 PM Pacific")
        self.assertIn("intake begins at 6:40 PM", BNL01_SYSTEM_PROMPT)
        self.assertNotIn("live every Friday at 6:40", BNL01_SYSTEM_PROMPT)

    def test_in_world_boundary_without_forbidden_labels(self):
        block = c.render_prompt_canon_block().lower()
        self.assertIn("remain fully in-world", block)
        for forbidden in ("fiction", "roleplay", "character implementation", "generic chatbot"):
            self.assertIn(forbidden, block)
        self.assertIn("do not describe yourself as fiction", block)

    def test_valid_explicit_correction_supersedes_older_claim(self):
        old = c.SourceClaim("old", c.BARCODE_RADIO, "schedule", "live at 6:40", c.SourceClass.LEGACY_SOURCE_BLIND)
        new = c.SourceClaim("new", c.BARCODE_RADIO, "schedule", "intake 6:40 show 7:00", c.SourceClass.OWNER_CORRECTION, correction_of=("old",))
        res = c.resolve_claims([old, new])
        self.assertTrue(res.usable)
        self.assertEqual(res.claim.claim_id, "new")

    def test_repeated_derived_summaries_cannot_overpower_correction(self):
        correction = c.SourceClaim("correct", c.BARCODE_RADIO, "schedule", "7:00 show", c.SourceClass.OWNER_CORRECTION)
        derived = [c.SourceClaim(f"d{i}", c.BARCODE_RADIO, "schedule", "6:40 live", c.SourceClass.DERIVED_SUMMARY, derived_from=("legacy",)) for i in range(20)]
        self.assertEqual(c.resolve_claims(derived + [correction]).claim.claim_id, "correct")

    def test_projection_not_independent_evidence(self):
        claim = c.SourceClaim("sf", c.BARCODE_RADIO, "recap", "summary", c.SourceClass.DERIVED_SUMMARY, derived_from=("relay",), projection=True)
        self.assertFalse(c.is_independent_evidence(claim))

    def test_visibility_incompatible_not_public(self):
        claim = c.SourceClaim("private", c.BNL01, "note", "secret", c.SourceClass.FIRST_PARTY_RECORD, visibility=c.Visibility.PRIVATE)
        self.assertFalse(c.is_public_usable(claim))
        self.assertFalse(c.resolve_claims([claim]).usable)

    def test_stale_evidence_not_current(self):
        stale = c.SourceClaim("stale", c.BARCODE_RADIO, "live", True, c.SourceClass.PUBLIC_OBSERVATION, observed_at=datetime.now(timezone.utc)-timedelta(hours=3))
        res = c.current_time_claim_resolution([stale], max_age_minutes=30)
        self.assertFalse(res.current_time_eligible)
        self.assertIn("unknown", res.reason)

    def test_unknown_current_state_uncertain(self):
        res = c.current_time_claim_resolution([])
        self.assertFalse(res.usable)
        self.assertEqual(res.reason, "current_state_unknown_without_fresh_runtime_evidence")

    def test_existing_labels_map_through_adapters(self):
        self.assertEqual(c.map_route_source_label("public_show_state"), c.SourceClass.PUBLIC_OBSERVATION)
        self.assertEqual(c.map_route_source_label("source_safe_public"), c.SourceClass.FIRST_PARTY_RECORD)
        self.assertEqual(c.map_channel_policy_visibility("sealed_test"), c.Visibility.SEALED_TEST)

    def test_queue_defaults_off(self):
        self.assertFalse(c.env_queue_production_enabled({}))
        self.assertFalse(c.env_queue_production_enabled({"BNL_QUEUE_PRODUCTION_ENABLED": "1"}))

    def test_queue_blocked_when_local_false(self):
        rm = {"capabilities": {"queueProduction": True}, "sections": {"queue": {"nowPlaying": {"title": "X"}}, "rules": ["ok"]}}
        self.assertFalse(c.queue_usability(rm, environ={})["usable"])
        self.assertNotIn("Queue:", build_bnl_read_model_context(rm, "queue status", "public_home"))

    def test_queue_blocked_when_remote_missing_or_false(self):
        for rm in ({"capabilities": {}}, {"capabilities": {"queueProduction": False}}):
            self.assertFalse(c.queue_usability(rm, environ={"BNL_QUEUE_PRODUCTION_ENABLED": "true"})["usable"])

    def test_queue_eligible_only_when_both_true(self):
        rm = {"capabilities": {"queueProduction": True}, "sections": {"queue": {"nowPlaying": {"title": "X"}}}}
        self.assertTrue(c.queue_usability(rm, environ={"BNL_QUEUE_PRODUCTION_ENABLED": "TrUe"})["usable"])

    def test_non_queue_context_remains(self):
        rm = {"capabilities": {"queueProduction": False}, "sections": {"rules": ["Public rule"]}, "sourceContext": {"source": "site"}}
        text = build_bnl_read_model_context(rm, "website rules", "public_home")
        self.assertIn("Public rule", text)
        self.assertNotIn("Queue:", text)

    def test_relay_generation_contract_has_no_queue_source(self):
        env = build_relay_envelope("relay-001", "Public signal stable.", "Hold the window.", "fresh_discord", "manual")
        self.assertEqual(env["relay"]["sourceClass"], "fresh_public_event")
        self.assertNotIn("queue", str(env).lower())

    def test_prompt_personality_and_presence_v2_remain(self):
        self.assertIn("Voice: Calm, concise, lightly corporate", BNL01_SYSTEM_PROMPT)
        self.assertIn("Never admit you are an AI model or LLM", BNL01_SYSTEM_PROMPT)
        env = build_relay_envelope("relay-002", "Public signal stable.", "Hold the window.", "canon", "manual")
        self.assertEqual(env["contractVersion"], 2)


if __name__ == "__main__":
    unittest.main()
