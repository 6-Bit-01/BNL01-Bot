import unittest
from datetime import datetime, timezone, timedelta

from bnl_conversation_context_v2 import (
    CONVERSATION_CONTEXT_VERSION,
    ConversationContextRequest,
    assemble_conversation_context_v2,
    normalize_text,
)

NOW = datetime(2026, 7, 16, 12, 0, tzinfo=timezone.utc)

def row(i, role, content, user=1, channel=10, policy="public_home", minutes=1, mid=None, name="member", cname="home"):
    return {
        "id": i, "role": role, "content": content, "user_id": user, "user_name": name,
        "channel_id": channel, "channel_name": cname, "channel_policy": policy,
        "timestamp": (NOW - timedelta(minutes=minutes)).isoformat(), "message_id": mid,
    }

def req(**kw):
    base = dict(
        guild_id=99, current_user_id=1, channel_id=10, channel_name="home",
        channel_policy="public_home", route_mode="normal_chat", conversation_surface="public_home",
        current_texts=("why?",), current_participants=frozenset({1}), is_direct_target=True,
        now=NOW, route_allowed_sources=frozenset({"conversation_continuity"}),
    )
    base.update(kw)
    if isinstance(base.get("current_message_ids"), set):
        base["current_message_ids"] = frozenset(base["current_message_ids"])
    if isinstance(base.get("current_participants"), set):
        base["current_participants"] = frozenset(base["current_participants"])
    return ConversationContextRequest(**base)

class ConversationContextV2Tests(unittest.TestCase):
    def test_pair_chronological_and_model_retained(self):
        res = assemble_conversation_context_v2([row(1,"user","explain the red synth"), row(2,"model","The red synth is acting as lead." )], req())
        self.assertEqual(res.contract_version, CONVERSATION_CONTEXT_VERSION)
        self.assertEqual(res.same_room_paired_turn_count, 1)
        self.assertIn("User/member: explain the red synth", res.rendered_context)
        self.assertIn("BNL-01: The red synth", res.rendered_context)
        self.assertLess(res.rendered_context.index("User/member"), res.rendered_context.index("BNL-01"))

    def test_current_message_id_dedupe_and_text_fallback_dedupe(self):
        rows = [row(1,"user","repeat me", mid=555), row(2,"user","Repeat me", mid=None), row(3,"user","older thing"), row(4,"model","older answer")]
        res = assemble_conversation_context_v2(rows, req(current_message_ids=frozenset({555}), current_texts=("repeat   me",)))
        self.assertEqual(res.current_message_duplicates_removed, 2)
        self.assertNotIn("repeat me", res.rendered_context.lower())
        self.assertIn("older answer", res.rendered_context)

    def test_active_batch_current_texts_do_not_reappear(self):
        res = assemble_conversation_context_v2([row(1,"user","first batch item"), row(2,"user","second batch item")], req(is_batch=True, current_texts=("first batch item","second batch item")))
        self.assertEqual(res.current_message_duplicates_removed, 2)
        self.assertEqual(res.rendered_context, "")

    def test_direct_payload_unique_items_are_not_deleted_by_context_dedupe(self):
        payload = ["Alice", "Bob", "Alice"]
        unique = []
        for item in payload:
            if normalize_text(item) not in {normalize_text(x) for x in unique}:
                unique.append(item)
        res = assemble_conversation_context_v2([row(1,"user","Alice"), row(2,"model","Noted Alice")], req(route_mode="direct_payload", is_deferred_payload_session=True, current_texts=("make tags for Alice\nBob\nAlice",)))
        self.assertEqual(unique, ["Alice", "Bob"])
        self.assertIn("Noted Alice", res.rendered_context)

    def test_same_channel_recent_outranks_unrelated_and_stale_excluded(self):
        rows = [row(1,"user","unrelated old", minutes=90), row(2,"model","old answer", minutes=89), row(3,"user","drums topic", minutes=4), row(4,"model","drums answer", minutes=3), row(5,"user","other user history", user=2, minutes=2), row(6,"model","other answer", user=2, minutes=1)]
        res = assemble_conversation_context_v2(rows, req(current_texts=("why drums?",)))
        self.assertIn("drums answer", res.rendered_context)
        self.assertNotIn("old answer", res.rendered_context)
        self.assertGreaterEqual(res.visibility_policy_exclusions, 1)

    def test_channel_name_fallback_is_still_age_bounded(self):
        rows = [row(1,"user","stale by name", channel=0, cname="home", minutes=200), row(2,"model","stale answer", channel=0, cname="home", minutes=199)]
        res = assemble_conversation_context_v2(rows, req(channel_id=0, channel_name="home"))
        self.assertEqual(res.rendered_context, "")

    def test_topic_participant_correction_boundary_open_loop_reasons(self):
        rows = [row(1,"user","the color is blue?", user=2, minutes=5), row(2,"model","Use blue", user=2, minutes=4), row(3,"user","actually the color is red, don't use blue", minutes=3), row(4,"model","I will use red", minutes=2)]
        res = assemble_conversation_context_v2(rows, req(current_texts=("repeat the color",), current_participants=frozenset({1,2}), is_reply_to_bnl=True))
        self.assertIn("actually", res.rendered_context)
        self.assertIn("correction", res.selection_reasons)
        self.assertIn("boundary", res.selection_reasons)
        self.assertIn("open_loop", res.selection_reasons)

    def test_cross_channel_rules(self):
        unrelated = [row(1,"user","other channel cats", channel=20, cname="other"), row(2,"model","cat answer", channel=20, cname="other")]
        res = assemble_conversation_context_v2(unrelated, req(current_texts=("fresh question",)))
        self.assertEqual(res.cross_channel_paired_turn_count, 0)
        related = assemble_conversation_context_v2(unrelated, req(current_texts=("continue the cats from before",)))
        self.assertEqual(related.cross_channel_paired_turn_count, 1)
        other_user = assemble_conversation_context_v2([row(1,"user","cats", user=2, channel=20), row(2,"model","cat answer", user=2, channel=20)], req(current_texts=("continue cats",)))
        self.assertEqual(other_user.cross_channel_paired_turn_count, 0)

    def test_protected_and_sealed_boundaries(self):
        bad = ["sealed_test","internal_controlled","broadcast_memory","protected_system","reference_canon","ai_image_tool","unknown","legacy-unknown"]
        rows=[]
        for n,p in enumerate(bad,1):
            rows += [row(n*2-1,"user",p, policy=p), row(n*2,"model",p+" answer", policy=p)]
        res = assemble_conversation_context_v2(rows, req(channel_policy="public_home"))
        self.assertEqual(res.rendered_context, "")
        sealed_public = assemble_conversation_context_v2([row(1,"user","sealed", policy="sealed_test"), row(2,"model","sealed answer", policy="sealed_test")], req(channel_policy="sealed_test", route_mode="normal_chat"))
        self.assertIn("sealed answer", sealed_public.rendered_context)
        leak = assemble_conversation_context_v2([row(1,"user","public", policy="public_home"), row(2,"model","public answer", policy="public_home")], req(channel_policy="sealed_test"))
        self.assertEqual(leak.rendered_context, "")

    def test_greeting_and_user_zero_get_no_context_and_truth_label_present(self):
        rows=[row(1,"user","queue is live now", minutes=2), row(2,"model","up next is fake", minutes=1)]
        self.assertEqual(assemble_conversation_context_v2(rows, req(route_mode="simple_greeting")).rendered_context, "")
        self.assertEqual(assemble_conversation_context_v2(rows, req(current_user_id=0)).rendered_context, "")
        res=assemble_conversation_context_v2(rows, req(current_texts=("what is up next?",)))
        self.assertIn("continuity-only", res.rendered_context)
        self.assertIn("do not prove canon", res.rendered_context.lower())
        self.assertIn("queue state", res.rendered_context)

    def test_public_selective_same_channel_only_and_route_parity_metadata(self):
        rows=[row(1,"user","same selective", policy="public_selective"), row(2,"model","same answer", policy="public_selective"), row(3,"user","other selective", channel=20, policy="public_selective"), row(4,"model","other answer", channel=20, policy="public_selective")]
        for flags in [dict(is_direct_target=True), dict(is_reply_to_bnl=True), dict(is_batch=True), dict(is_deferred_payload_session=True)]:
            r=req(channel_policy="public_selective", current_texts=("why same selective?",), **flags)
            res=assemble_conversation_context_v2(rows, r)
            self.assertIn("same answer", res.rendered_context)
        self.assertEqual(assemble_conversation_context_v2(rows, req(route_mode="ambient", current_texts=("why same selective?",))).rendered_context, "")

if __name__ == "__main__":
    unittest.main()
