import os
import unittest
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl01_bot
from bnl_memory_governance import classify_personal_recall_intent
from bnl_shared_brain_synthesis import (
    PUBLIC_HOME_OWNER_AUTHORITY,
    configuration,
    route_scope_decision,
)


class GovernedBroadRecallRouteExpansionTests(unittest.TestCase):
    def setUp(self):
        self.flags = {
            "BNL_MEMORY_LEDGER_SHADOW_ENABLED": "true",
            "BNL_MOMENT_ENGINE_SHADOW_ENABLED": "true",
            "BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED": "true",
            "BNL_RELATIONSHIP_V2_SHADOW_ENABLED": "true",
            "BNL_UNIFIED_RESPONSE_ASSESSMENT_SHADOW_ENABLED": "true",
            "BNL_UNIFIED_INTELLIGENCE_PACKET_SHADOW_ENABLED": "true",
            "BNL_SHARED_BRAIN_SYNTHESIS_CANARY_ENABLED": "true",
            "BNL_SHARED_BRAIN_SYNTHESIS_CANARY_GUILD_IDS": "1",
            "BNL_SHARED_BRAIN_SYNTHESIS_CANARY_USER_IDS": "7,8,9,10",
            "BNL_SHARED_BRAIN_SYNTHESIS_CANARY_CHANNEL_IDS": "10,11",
            "BNL_MEMORY_GOVERNANCE_LIVE_ENABLED": "false",
            "BNL_RELATIONSHIP_V2_LIVE_ENABLED": "false",
            "BNL_ACTIVE_ENGAGEMENT_V2_LIVE_ENABLED": "false",
        }

    def test_four_production_wordings_share_one_route_family(self):
        transcripts = (
            "hey B? what do you know about me?",
            "Hey bud what do you know about me",
            "Hey BNL what do you know about me?",
            "Hey BNL, what am I all about?",
        )
        for text in transcripts:
            with self.subTest(text=text):
                intent = classify_personal_recall_intent(text)
                self.assertTrue(intent.broad_self_profile)
                self.assertEqual(
                    intent.route_family,
                    "broad_self_profile",
                )
                self.assertTrue(
                    bnl01_bot.is_broad_personal_recall_request(text)
                )

    def test_real_mention_residue_and_full_mention_normalize_identically(self):
        for text in (
            ", what am I all about?",
            "<@1444786398153539605>, what am I all about?",
            "<@!1444786398153539605> what am I all about?",
        ):
            with self.subTest(text=text):
                self.assertTrue(
                    classify_personal_recall_intent(
                        text
                    ).broad_self_profile
                )

    def test_profile_route_accepts_a_single_topic_scope(self):
        for text in (
            "BNL, what am I all about in the BARCODE project?",
            "What do you know about me within this community?",
        ):
            with self.subTest(text=text):
                self.assertTrue(
                    classify_personal_recall_intent(
                        text
                    ).broad_self_profile
                )

    def test_private_acceptance_questions_share_the_profile_route(self):
        for text in (
            "BNL-01, what am I all about?",
            (
                "What have you learned about how I work and make "
                "decisions?"
            ),
            (
                "What parts of BARCODE seem to matter most to me, and "
                "why?"
            ),
        ):
            with self.subTest(text=text):
                self.assertTrue(
                    classify_personal_recall_intent(
                        text
                    ).broad_self_profile
                )

    def test_mixed_negated_and_third_party_requests_do_not_enter_route(self):
        for text in (
            "What do you know about me and tell me a joke?",
            "Do not tell me what you know about me yet.",
            "What do you know about WittyFox?",
        ):
            with self.subTest(text=text):
                self.assertFalse(
                    classify_personal_recall_intent(
                        text
                    ).broad_self_profile
                )

    def test_ambiguous_recall_uses_understand_or_clarify_contract(self):
        intent = classify_personal_recall_intent(
            "Hey BNL, what do you remember?"
        )
        self.assertEqual(intent.status, "needs_context")
        contract = bnl01_bot.personal_recall_interpretation_contract(
            "Hey BNL, what do you remember?"
        )
        self.assertIn("visible current conversation", contract)
        self.assertIn("one brief natural", contract)

    def test_public_route_parity_is_not_tied_to_canary_allowlist(self):
        for text in (
            "hey B? what do you know about me?",
            "Hey bud what do you know about me",
            "Hey BNL what do you know about me?",
            "Hey BNL, what am I all about?",
        ):
            with self.subTest(text=text):
                self.assertTrue(
                    bnl01_bot
                    .broad_personal_recall_enters_normal_generation(
                        route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                        channel_policy="public_home",
                        user_text=text,
                        current_direct=True,
                    )
                )
        self.assertFalse(
            bnl01_bot.broad_personal_recall_enters_normal_generation(
                route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                channel_policy="sealed_test",
                user_text="What do you know about me?",
                current_direct=True,
            )
        )

    def test_explicit_multi_member_multi_channel_scope_stays_bounded(self):
        configured = configuration(self.flags)
        self.assertTrue(configured["effective"])
        self.assertEqual(configured["user_allowlist_count"], 4)
        self.assertEqual(configured["channel_allowlist_count"], 2)
        self.assertEqual(configured["max_scoped_users"], 8)
        self.assertEqual(configured["max_scoped_channels"], 4)

        for policy, channel_id in (
            ("public_home", 10),
            ("public_context", 11),
        ):
            with self.subTest(policy=policy):
                decision = route_scope_decision(
                    guild_id=1,
                    user_id=7,
                    channel_id=channel_id,
                    route_mode="normal_chat",
                    channel_policy=policy,
                    current_direct=True,
                    user_text="Hey BNL, what do you know about me?",
                    environ=self.flags,
                )
                self.assertTrue(decision.eligible)
                self.assertEqual(decision.reason, "eligible")

        oversized = {
            **self.flags,
            "BNL_SHARED_BRAIN_SYNTHESIS_CANARY_USER_IDS": ",".join(
                str(value) for value in range(1, 10)
            ),
        }
        self.assertFalse(configuration(oversized)["effective"])
        self.assertEqual(
            configuration(oversized)["reason"],
            "scope_limit_exceeded",
        )

    def test_packet_canary_still_rejects_non_allowlisted_identity(self):
        decision = route_scope_decision(
            guild_id=1,
            user_id=99,
            channel_id=10,
            route_mode="normal_chat",
            channel_policy="public_home",
            current_direct=True,
            user_text="What do you know about me?",
            environ=self.flags,
        )
        self.assertFalse(decision.eligible)
        self.assertEqual(decision.reason, "user_not_allowlisted")

    def test_public_home_owner_is_a_separate_default_off_route_gate(self):
        owner_flags = {
            **self.flags,
            "BNL_SHARED_BRAIN_SYNTHESIS_CANARY_ENABLED": "false",
            "BNL_PUBLIC_HOME_BROAD_RECALL_OWNER_ENABLED": "true",
            "BNL_PUBLIC_HOME_BROAD_RECALL_OWNER_GUILD_IDS": "1",
            "BNL_PUBLIC_HOME_BROAD_RECALL_OWNER_CHANNEL_IDS": "10",
        }
        configured = configuration(owner_flags)
        self.assertTrue(configured["effective"])
        self.assertFalse(configured["canary_effective"])
        self.assertTrue(configured["public_home_owner_effective"])
        self.assertEqual(
            configured["authority_mode"],
            PUBLIC_HOME_OWNER_AUTHORITY,
        )
        self.assertFalse(configured["user_scope_required"])
        self.assertEqual(configured["user_allowlist_count"], 0)
        self.assertEqual(configured["channel_policies"], ("public_home",))
        self.assertEqual(
            configured["kill_switch_env"],
            "BNL_PUBLIC_HOME_BROAD_RECALL_OWNER_ENABLED",
        )

        for user_id in (7, 99):
            with self.subTest(user_id=user_id):
                decision = route_scope_decision(
                    guild_id=1,
                    user_id=user_id,
                    channel_id=10,
                    route_mode="normal_chat",
                    channel_policy="public_home",
                    current_direct=True,
                    user_text="What do you know about me?",
                    environ=owner_flags,
                )
                self.assertTrue(decision.eligible)
                self.assertEqual(
                    decision.authority_mode,
                    PUBLIC_HOME_OWNER_AUTHORITY,
                )

        for override in (
            {"guild_id": 2},
            {"channel_id": 11},
            {"channel_policy": "public_context"},
            {"route_mode": "direct_payload"},
            {"current_direct": False},
            {"user_text": "Tell me a joke."},
        ):
            common = {
                "guild_id": 1,
                "user_id": 99,
                "channel_id": 10,
                "route_mode": "normal_chat",
                "channel_policy": "public_home",
                "current_direct": True,
                "user_text": "What do you know about me?",
                "environ": owner_flags,
            }
            with self.subTest(override=override):
                self.assertFalse(
                    route_scope_decision(
                        **{**common, **override}
                    ).eligible
                )

    def test_route_authorities_conflict_and_kill_switch_fail_closed(self):
        owner_scope = {
            **self.flags,
            "BNL_PUBLIC_HOME_BROAD_RECALL_OWNER_ENABLED": "true",
            "BNL_PUBLIC_HOME_BROAD_RECALL_OWNER_GUILD_IDS": "1",
            "BNL_PUBLIC_HOME_BROAD_RECALL_OWNER_CHANNEL_IDS": "10",
        }
        conflict = configuration(owner_scope)
        self.assertFalse(conflict["effective"])
        self.assertEqual(conflict["reason"], "authority_conflict")

        disabled = configuration(
            {
                **owner_scope,
                "BNL_SHARED_BRAIN_SYNTHESIS_CANARY_ENABLED": "false",
                "BNL_PUBLIC_HOME_BROAD_RECALL_OWNER_ENABLED": "false",
            }
        )
        self.assertFalse(disabled["effective"])
        self.assertEqual(disabled["reason"], "disabled")

if __name__ == "__main__":
    unittest.main()
