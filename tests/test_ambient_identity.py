import unittest

from bnl_ambient_identity import (
    plan_ambient_mentions,
    render_ambient_mentions,
    sanitize_ambient_mentions,
)


def source(ref="conversation:1", subject="discord_user:123", label="Test Member", **updates):
    return {"ref": ref, "guild_id": 42, "kind": "conversation",
            "conversation_surface": "discord", "scope": "window_activity",
            "subject_refs": [subject], "subject_labels": {subject: label}, **updates}


def plan(items, sources=("conversation:1",), subjects=("discord_user:123",), guild_id=42):
    return plan_ambient_mentions(items, sources, guild_id=guild_id, featured_subject_refs=subjects)


class AmbientIdentityTests(unittest.TestCase):
    def test_exact_featured_public_author_can_be_notified_once(self):
        planned = plan([source(), source(ref="conversation:2")],
                       sources=("conversation:1", "conversation:2"))
        self.assertEqual(planned["discord_user:123"].source_refs, ("conversation:1", "conversation:2"))
        rendered = render_ambient_mentions(planned, current_member_ids=(123, 123))
        self.assertEqual(rendered.user_ids, (123,))
        self.assertEqual(rendered.content, "Featuring: <@123>")

    def test_nonfeatured_participant_and_uncited_source_do_not_ping(self):
        participants = source(subject_refs=["discord_user:123", "discord_user:456"],
                              subject_labels={"discord_user:123": "Test Member", "discord_user:456": "Test Artist"})
        planned = plan([participants])
        self.assertEqual(set(planned), {"discord_user:123"})
        self.assertFalse(plan([source()], sources=("conversation:99",)))
        self.assertFalse(plan([source()], subjects=()))

    def test_invented_or_inferred_identity_never_authorizes_a_ping(self):
        for item in (
            source(conversation_surface="tiktok_live_chat"),
            source(kind="published_journal"), source(kind="published_ballad"),
            source(kind="source_file", identity_status="confirmed"),
            source(subject="tiktok_user:test_member"),
            source(subject="discord_user:123garbage"),
            source(subject="discord_user:0"), source(subject="discord_user:-123"),
            source(subject="discord_user:0123"),
        ):
            with self.subTest(item=item):
                self.assertFalse(plan([item], subjects=item["subject_refs"]))
        self.assertFalse(plan([source()], subjects=("discord_user:456",)))

    def test_private_crossguild_or_unlabeled_source_never_pings(self):
        for item in (
            source(guild_id=43), source(guild_id=None), source(guild_id=True),
            source(scope="sealed_test"), source(scope="private"), source(scope=None),
            source(subject_labels={}), source(label=""), source(label="<@456>"),
            source(subject_refs="discord_user:123"),
        ):
            with self.subTest(item=item):
                self.assertFalse(plan([item]))
        self.assertFalse(plan([source()], guild_id=0))
        self.assertFalse(plan([source()], guild_id=True))

    def test_source_withdrawal_removes_the_mention_plan(self):
        self.assertTrue(plan([source()]))
        # Caller reprojects current sources at send time; no saved identity
        # cache can retain an account after its only eligible root disappears.
        self.assertFalse(plan([]))

    def test_departed_unverified_members_get_no_notification(self):
        planned = plan([source()])
        self.assertEqual(render_ambient_mentions(planned, current_member_ids=(456,)).content, "")
        self.assertEqual(render_ambient_mentions(planned, current_member_ids=()).user_ids, ())

    def test_model_cannot_emit_raw_user_role_or_broad_pings(self):
        planned = plan([source()])
        text = sanitize_ambient_mentions("<@123> and <@!456> <@&789> @everyone @here", planned)
        self.assertEqual(text, "Test Member and a community member a community role @\u200beveryone @\u200bhere")
        self.assertNotIn("<@", text)

    def test_public_display_label_cannot_inject_another_ping(self):
        planned = plan([source(label="Test Member <@456> <@&789> @everyone\n@here")])
        label = planned["discord_user:123"].label
        self.assertNotIn("<@", label)
        self.assertNotIn("@everyone", label)
        self.assertNotIn("@here", label)
        self.assertEqual(render_ambient_mentions(planned, current_member_ids=(123, 456)).user_ids, (123,))


if __name__ == "__main__":
    unittest.main()
