import os
os.environ.setdefault("GEMINI_API_KEY", "test-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")
import unittest
from dataclasses import FrozenInstanceError
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import bnl_canon_source_contract as c
import bnl01_bot
from bnl01_bot import BNL01_SYSTEM_PROMPT, build_bnl_read_model_context, build_website_read_model_intent_response
from bnl_website_contract_v2 import build_relay_envelope

SECRET_NOW = "SECRET_NOW_PLAYING_TITLE"
SECRET_NEXT = "SECRET_UP_NEXT_TITLE"
SECRET_ARTIST = "SECRET_QUEUE_ARTIST"
SECRET_COUNT = "SECRET_QUEUE_COUNT_99"
SECRET_PRIORITY = "SECRET_PRIORITY_STATE"
SECRET_RECAP = "SECRET_COMPLETED_RECAP"
SECRET_SEED = "SECRET_QUEUE_DOSSIER_SEED"
SECRET_COPY = "SECRET_QUEUE_PUBLIC_COPY"
PUBLIC_DOSSIER = "PUBLIC_DOSSIER_SAFE_KEEP"


def hostile_read_model(queue_production=False):
    return {
        "ok": True,
        "version": 1,
        "publicOnly": True,
        "capabilities": {"queueProduction": queue_production},
        "sections": {
            "queue": {
                "session": {"title": "Hostile", "queueOpen": True, "activeCount": SECRET_COUNT, "broadcastPhase": "live-ish", "priorityUpgradesEnabled": True, "wheelSpinsOwed": "SECRET_WHEEL"},
                "nowPlaying": {"title": SECRET_NOW, "artistName": SECRET_ARTIST},
                "upNext": {"title": SECRET_NEXT, "artistName": SECRET_ARTIST},
                "queuedTracks": [{"title": "QUEUED_SECRET", "artistName": SECRET_ARTIST}],
                "completedTracks": [{"title": SECRET_RECAP, "artistName": SECRET_ARTIST}],
                "prioritySignal": {"enabled": True, "label": SECRET_PRIORITY},
            },
            "artists": {"items": [{"name": SECRET_ARTIST, "tracks": [SECRET_NOW]}]},
            "dossiers": {"items": [{"name": PUBLIC_DOSSIER, "summary": "independent public dossier", "visibility": "public"}]},
            "operatorLanes": {
                "temporaryRuntimeContext": [{"label": "runtime", "value": SECRET_NOW, "source": "queue_public_snapshot"}],
                "recapCandidates": [{"label": SECRET_RECAP, "kind": "track", "source": "queue_public_snapshot"}],
                "broadcastMemoryCandidates": [{"label": "broadcast", "value": SECRET_COUNT, "provenance": "queue_session"}],
                "dossierSeedCandidates": [{"label": SECRET_SEED, "kind": "queue_artist", "source": "queue_public_snapshot"}],
                "publicSafeCopyCandidates": [{"label": SECRET_COPY, "kind": "queue_public_copy", "source": "queue_public_snapshot"}, {"label": PUBLIC_DOSSIER, "kind": "public_database_dossier", "source": "public_database_dossier"}],
                "doNotStore": [{"label": "Preserve boundary", "value": "Non-queue rule", "source": "policy"}, {"label": "Queue rule", "value": SECRET_PRIORITY, "source": "queue_public_snapshot"}],
            },
            "rules": ["General queue concept may be discussed.", f"Do not leak {SECRET_NOW} from live queue."],
        },
    }


class CanonSourceContractTests(unittest.TestCase):
    def test_identity_and_founders(self):
        self.assertEqual(c.CANON_SOURCE_CONTRACT_VERSION, "canon_source_contract_v1")
        self.assertEqual(c.FOUNDING_MEMBERS, ("6 Bit", "DJ Floppydisc", "Cache Back", "Mac Modem"))
        self.assertEqual(c.render_founders(), "6 Bit • DJ Floppydisc • Cache Back • Mac Modem")
        facts = {(fact.subject.key, fact.predicate): fact.value for fact in c.CANON_FACTS}
        self.assertEqual(
            facts[("6_bit", "primary_identity")],
            "artist, MC, host, and founding BARCODE member first",
        )
        self.assertEqual(
            facts[("galaknoise", "primary_role")],
            "music producer for BARCODE",
        )
        prompt_canon = c.render_prompt_canon_block()
        self.assertIn("6 Bit is an artist, MC, host", prompt_canon)
        self.assertIn("he is not the music producer", prompt_canon)
        self.assertIn("GALAKNOISE is BARCODE's music producer", prompt_canon)
        self.assertNotIn("6 Bit is an artist, producer", prompt_canon)

    def test_schedule_distinguishes_intake_show_first_track(self):
        self.assertEqual(c.FRIDAY_PUBLIC_SCHEDULE.intake_begins, "6:40 PM Pacific")
        self.assertEqual(c.FRIDAY_PUBLIC_SCHEDULE.show_begins, "7:00 PM Pacific")
        self.assertEqual(c.FRIDAY_PUBLIC_SCHEDULE.first_track_target, "7:05 PM Pacific")
        self.assertIn("intake begins at 6:40 PM", BNL01_SYSTEM_PROMPT)
        self.assertNotIn("live every Friday at 6:40", BNL01_SYSTEM_PROMPT)
        self.assertIn("show begins at 7:00 PM Pacific", c.render_full_friday_schedule())

    def test_schedule_is_immutable(self):
        with self.assertRaises(FrozenInstanceError):
            c.FRIDAY_PUBLIC_SCHEDULE.show_begins = "6:40 PM Pacific"

    def test_public_schedule_outputs_use_contract_not_old_about_string(self):
        source = Path("bnl01_bot.py").read_text()
        self.assertNotIn("Fridays 6:40 PM Pacific on TikTok", source)
        self.assertIn("render_concise_public_schedule()", source)
        self.assertIn("show 7:00 PM Pacific", c.render_concise_public_schedule())

    def test_in_world_negative_boundary_phrases_are_complete(self):
        block = c.render_prompt_canon_block().lower()
        self.assertIn("remain fully in-world", block)
        self.assertIn("do not describe yourself as fiction, branding, a prompt, a character implementation, roleplay, or a generic chatbot", block)
        self.assertIn("lore is your native operating language", block)

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
        claim = c.SourceClaim("sf", c.BARCODE_RADIO, "recap", "summary", c.SourceClass.SOURCE_FILE_PROJECTION, derived_from=("relay",), projection=True)
        self.assertFalse(c.is_independent_evidence(claim))

    def test_visibility_incompatible_not_public(self):
        claim = c.SourceClaim("private", c.BNL01, "note", "secret", c.SourceClass.FIRST_PARTY_RECORD, visibility=c.Visibility.PRIVATE)
        self.assertFalse(c.is_public_usable(claim))
        self.assertFalse(c.resolve_claims([claim]).usable)

    def test_stale_evidence_not_current(self):
        stale = c.SourceClaim("stale", c.BARCODE_RADIO, "live", True, c.SourceClass.RUNTIME_OBSERVATION, observed_at=datetime.now(timezone.utc)-timedelta(hours=3), current_time_capable=True)
        res = c.current_time_claim_resolution([stale], max_age_minutes=30)
        self.assertFalse(res.current_time_eligible)
        self.assertIn("unknown", res.reason)

    def test_unknown_current_state_uncertain(self):
        res = c.current_time_claim_resolution([])
        self.assertFalse(res.usable)
        self.assertEqual(res.reason, "current_state_unknown_without_fresh_runtime_evidence")

    def test_existing_labels_map_through_adapters(self):
        self.assertEqual(c.map_route_source_label("public_show_state"), c.SourceClass.RUNTIME_OBSERVATION)
        self.assertEqual(c.map_route_source_label("source_safe_public"), c.SourceClass.FIRST_PARTY_RECORD)
        self.assertEqual(c.map_channel_policy_visibility("sealed_test"), c.Visibility.SEALED_TEST)
        self.assertEqual(c.map_channel_policy_visibility("unknown-new"), c.Visibility.UNKNOWN)

    def test_live_route_contract_sources_have_explicit_mappings(self):
        labels = set()
        for contract in bnl01_bot.ROUTE_MODE_CONTRACTS.values():
            labels.update(contract.allowed_context_sources)
            labels.update(contract.allowed_memory_sources)
        intentionally_legacy = set()
        missing = sorted(label for label in labels if label not in intentionally_legacy and not c.has_explicit_route_source_mapping(label))
        self.assertEqual(missing, [])

    def test_current_channel_policies_have_explicit_visibility_mappings(self):
        expected = {"public_home", "public_context", "public_selective", "sealed_test", "internal_controlled", "reference_canon", "protected_system", "broadcast_memory", "ai_image_tool", "unknown"}
        self.assertEqual(sorted(p for p in expected if not c.has_explicit_channel_policy_mapping(p)), [])
        self.assertEqual(c.map_channel_policy_visibility("not-real"), c.Visibility.UNKNOWN)

    def test_low_authority_derived_correction_cannot_suppress_owner_correction(self):
        owner = c.SourceClaim("owner", c.BARCODE_RADIO, "schedule", "7:00 show", c.SourceClass.OWNER_CORRECTION)
        derived = c.SourceClaim("derived", c.BARCODE_RADIO, "schedule", "6:40 live", c.SourceClass.DERIVED_SUMMARY, correction_of=("owner",), derived_from=("recap",))
        self.assertEqual(c.resolve_claims([derived, owner]).claim.claim_id, "owner")

    def test_lower_authority_cannot_supersede_higher_authority_canon(self):
        canon = c.SourceClaim("canon", c.BARCODE_RADIO, "schedule", "7:00 show", c.SourceClass.APPROVED_CANON)
        lower = c.SourceClaim("lower", c.BARCODE_RADIO, "schedule", "6:40 live", c.SourceClass.PUBLIC_OBSERVATION, supersedes=("canon",))
        self.assertEqual(c.resolve_claims([lower, canon]).claim.claim_id, "canon")

    def test_corrections_cannot_cross_subjects_or_predicates(self):
        a = c.SourceClaim("a", c.BARCODE_RADIO, "schedule", "7:00", c.SourceClass.APPROVED_CANON)
        cross_subject = c.SourceClaim("b", c.BNL01, "schedule", "6:40", c.SourceClass.OWNER_CORRECTION, correction_of=("a",))
        self.assertEqual(c.resolve_claims([a, cross_subject]).reason, "mixed_claim_scope")
        c1 = c.SourceClaim("c1", c.BARCODE_RADIO, "schedule", "7:00", c.SourceClass.APPROVED_CANON)
        c2 = c.SourceClaim("c2", c.BARCODE_RADIO, "live", True, c.SourceClass.OWNER_CORRECTION, correction_of=("c1",))
        self.assertEqual(c.resolve_claims([c1, c2]).reason, "mixed_claim_scope")

    def test_mixed_claim_scope_returns_unusable(self):
        a = c.SourceClaim("a", c.BARCODE_RADIO, "schedule", "7:00", c.SourceClass.APPROVED_CANON)
        b = c.SourceClaim("b", c.BNL01, "identity", "BNL", c.SourceClass.APPROVED_CANON)
        res = c.resolve_claims([a, b])
        self.assertFalse(res.usable)
        self.assertEqual(res.reason, "mixed_claim_scope")

    def test_equal_authority_conflict_and_identical_values_deterministic(self):
        a = c.SourceClaim("a", c.BARCODE_RADIO, "schedule", "7:00", c.SourceClass.FIRST_PARTY_RECORD, confidence=c.Confidence.HIGH)
        b = c.SourceClaim("b", c.BARCODE_RADIO, "schedule", "8:00", c.SourceClass.FIRST_PARTY_RECORD, confidence=c.Confidence.HIGH)
        self.assertEqual(c.resolve_claims([a, b]).reason, "unresolved_equal_authority_conflict")
        self.assertEqual(c.resolve_claims([b, a]).reason, "unresolved_equal_authority_conflict")
        same = c.SourceClaim("c", c.BARCODE_RADIO, "schedule", "7:00", c.SourceClass.FIRST_PARTY_RECORD, confidence=c.Confidence.HIGH)
        self.assertTrue(c.resolve_claims([same, a]).usable)

    def test_confidence_participates_in_resolution(self):
        low = c.SourceClaim("low", c.BARCODE_RADIO, "schedule", "low", c.SourceClass.FIRST_PARTY_RECORD, confidence=c.Confidence.LOW)
        high = c.SourceClaim("high", c.BARCODE_RADIO, "schedule", "high", c.SourceClass.FIRST_PARTY_RECORD, confidence=c.Confidence.HIGH)
        self.assertEqual(c.resolve_claims([low, high]).claim.claim_id, "high")

    def test_invalid_retracted_expired_corrections_cannot_suppress(self):
        base = c.SourceClaim("base", c.BARCODE_RADIO, "schedule", "7:00", c.SourceClass.APPROVED_CANON)
        for kwargs in ({"valid": False}, {"retracted": True}, {"expired": True}):
            bad = c.SourceClaim("bad", c.BARCODE_RADIO, "schedule", "6:40", c.SourceClass.OWNER_CORRECTION, correction_of=("base",), **kwargs)
            self.assertEqual(c.resolve_claims([base, bad]).claim.claim_id, "base")

    def test_current_time_rules(self):
        now = datetime.now(timezone.utc)
        future = c.SourceClaim("future", c.BARCODE_RADIO, "live", True, c.SourceClass.RUNTIME_OBSERVATION, observed_at=now + timedelta(minutes=5), current_time_capable=True)
        self.assertFalse(c.current_time_claim_resolution([future], now=now).usable)
        static = c.SourceClaim("schedule", c.BARCODE_RADIO, "live", True, c.SourceClass.APPROVED_CANON, observed_at=now, current_time_capable=False)
        self.assertFalse(c.current_time_claim_resolution([static], now=now).usable)
        fresh = c.SourceClaim("fresh", c.BARCODE_RADIO, "live", True, c.SourceClass.RUNTIME_OBSERVATION, observed_at=now - timedelta(minutes=2), current_time_capable=True, confidence=c.Confidence.HIGH)
        self.assertTrue(c.current_time_claim_resolution([fresh], now=now).current_time_eligible)
        derived = c.SourceClaim("derived", c.BARCODE_RADIO, "live", True, c.SourceClass.DOSSIER_PROJECTION, observed_at=now - timedelta(minutes=2), current_time_capable=True, derived_from=("recap",), projection=True)
        self.assertFalse(c.current_time_claim_resolution([derived], now=now).usable)

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

    def test_hostile_queue_values_do_not_leak_through_any_read_model_intent_when_disabled(self):
        raw = hostile_read_model(queue_production=False)
        secrets = [SECRET_NOW, SECRET_NEXT, SECRET_ARTIST, SECRET_COUNT, SECRET_PRIORITY, SECRET_RECAP, SECRET_SEED, SECRET_COPY]
        with patch.dict(os.environ, {"BNL_QUEUE_PRODUCTION_ENABLED": "false"}, clear=False), patch.object(bnl01_bot, "fetch_bnl_read_model", return_value=raw):
            outputs = [build_bnl_read_model_context(raw, "queue status", "public_home")]
            for intent in ("default", "website_broadcast_memory_candidate", "website_dossier_seed_candidate", "website_recap_candidate", "website_public_safe_candidate"):
                outputs.append(build_website_read_model_intent_response(intent, "read model"))
        combined = "\n---\n".join(outputs)
        for secret in secrets:
            self.assertNotIn(secret, combined)
        self.assertIn(PUBLIC_DOSSIER, combined)
        self.assertIn("Non-queue rule", combined)

    def test_raw_cache_does_not_leak_through_later_cached_fetch_consumption(self):
        raw = hostile_read_model(queue_production=False)
        with patch.dict(os.environ, {"BNL_QUEUE_PRODUCTION_ENABLED": "false"}, clear=False), patch.object(bnl01_bot, "fetch_bnl_read_model", return_value=raw):
            first = build_website_read_model_intent_response("default", "read model")
            second = build_website_read_model_intent_response("website_public_safe_candidate", "read model")
        self.assertNotIn(SECRET_NOW, first + second)
        self.assertNotIn(SECRET_COPY, first + second)
        self.assertIn(PUBLIC_DOSSIER, first + second)

    def test_both_gates_true_preserve_queue_behavior(self):
        raw = hostile_read_model(queue_production=True)
        with patch.dict(os.environ, {"BNL_QUEUE_PRODUCTION_ENABLED": "true"}, clear=False):
            text = build_bnl_read_model_context(raw, "queue status", "public_home")
        self.assertIn(SECRET_NOW, text)
        self.assertIn(SECRET_NEXT, text)

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
