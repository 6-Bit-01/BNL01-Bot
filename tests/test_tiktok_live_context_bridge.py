import json
import stat
import tempfile
import unittest
from pathlib import Path

from bnl_tiktok_live_chat import LiveChatAdapter, LiveChatBuffer
from bnl_tiktok_live_context import (
    IDENTITY_DEFAULT,
    LIFECYCLE,
    MEMORY_DEFAULT,
    SCHEMA_VERSION,
    SOURCE,
    LiveContextSnapshotWriter,
    build_live_prompt_context,
    is_live_show_reaction_query,
    live_context_diagnostics,
    load_live_context_snapshot,
)
from bnl_tiktok_live_memory import (
    TikTokPublicConversationSpoolWriter,
    read_public_conversation_spool,
    resolve_tiktok_identity,
)


class Clock:
    def __init__(self, value=1_800_000_000.0):
        self.value = float(value)

    def __call__(self):
        return self.value


def lifecycle_payload(event_type, clock, room_id="room-1"):
    return {
        "schema_version": 1,
        "event_type": event_type,
        "event_id": f"{event_type}-1",
        "room_id": room_id,
        "observed_at": clock(),
    }


def observation_payload(event_type, event_id, clock, **changes):
    payload = {
        "schema_version": 1,
        "event_type": event_type,
        "event_id": event_id,
        "room_id": "room-1",
        "observed_at": clock(),
        "source_at": clock(),
        "unique_id": "test.viewer",
        "display_name": "Test Viewer",
        "moderator_flag": False,
    }
    payload.update(changes)
    return payload


class TikTokLiveContextBridgeTests(unittest.TestCase):
    def make_adapter(self, clock):
        adapter = LiveChatAdapter(
            LiveChatBuffer(100, 600, clock),
            clock,
            clear_on_live_end=False,
        )
        adapter.ingest_line(json.dumps(lifecycle_payload("connected", clock)))
        adapter.ingest_line(json.dumps(observation_payload(
            "comment",
            "comment-1",
            clock,
            comment_text="This track is wild.",
        )))
        adapter.ingest_line(json.dumps(observation_payload(
            "like",
            "like-1",
            clock,
            like_count=125,
            like_total=8420,
        )))
        adapter.ingest_line(json.dumps(observation_payload(
            "viewer_snapshot",
            "view-1",
            clock,
            unique_id="",
            display_name="",
            viewer_count=37,
        )))
        return adapter

    def test_reaction_detector_covers_natural_live_questions_only(self):
        positives = (
            "What's TikTok chat saying?",
            "whats chat thinking about the show",
            "What did TikTok chat just say?",
            "What is the chat talking about right now?",
            "Nice. What is the chat talking bout?",
            "No, BNL, the TikTok chat.",
            "How are the viewers reacting?",
            "How's the live going?",
            "What is happening during the show?",
        )
        for value in positives:
            with self.subTest(value=value):
                self.assertTrue(is_live_show_reaction_query(value))
        self.assertFalse(is_live_show_reaction_query("What did people say in Discord yesterday?"))
        self.assertFalse(is_live_show_reaction_query("What's playing right now?"))

    def test_writer_publishes_mode_0600_bounded_contract_and_reader_revalidates(self):
        clock = Clock()
        adapter = self.make_adapter(clock)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "live-context.json"
            writer = LiveContextSnapshotWriter(str(path), time_fn=clock)
            self.assertTrue(writer.publish(adapter, force=True))
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)

            snapshot, reason = load_live_context_snapshot(
                str(path),
                now=clock(),
            )

        self.assertEqual(reason, "ok")
        self.assertEqual(snapshot["schema_version"], SCHEMA_VERSION)
        self.assertEqual(snapshot["source"], SOURCE)
        self.assertEqual(snapshot["lifecycle"], LIFECYCLE)
        self.assertEqual(snapshot["memory_default"], MEMORY_DEFAULT)
        self.assertEqual(snapshot["identity_default"], IDENTITY_DEFAULT)
        self.assertEqual(snapshot["state"], "connected")
        self.assertEqual(snapshot["health"]["viewer_count"], 37)
        self.assertEqual(snapshot["health"]["taps_observed"], 125)
        self.assertEqual(len(snapshot["events"]), 1)
        self.assertEqual(snapshot["events"][0]["event_id"], "comment-1")
        self.assertEqual(snapshot["events"][0]["comment_text"], "This track is wild.")

    def test_snapshot_fails_closed_when_stale_or_contract_is_wrong(self):
        clock = Clock()
        adapter = self.make_adapter(clock)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "live-context.json"
            writer = LiveContextSnapshotWriter(str(path), time_fn=clock)
            writer.publish(adapter, force=True)
            _snapshot, stale_reason = load_live_context_snapshot(
                str(path),
                now=clock() + 21,
                max_age_seconds=20,
            )
            value = json.loads(path.read_text(encoding="utf-8"))
            value["memory_default"] = "store"
            path.write_text(json.dumps(value), encoding="utf-8")
            _snapshot, contract_reason = load_live_context_snapshot(
                str(path),
                now=clock(),
            )

        self.assertEqual(stale_reason, "snapshot_stale")
        self.assertEqual(contract_reason, "snapshot_contract_mismatch")

    def test_prompt_is_compact_source_aware_and_identity_bounded(self):
        clock = Clock()
        adapter = self.make_adapter(clock)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "live-context.json"
            LiveContextSnapshotWriter(str(path), time_fn=clock).publish(adapter, force=True)
            prompt = build_live_prompt_context(
                str(path),
                enabled=True,
                now=clock(),
                declared_owner_handles=("six.bit", "pr0x60"),
            )

        self.assertIn("This track is wild.", prompt)
        self.assertIn("Test Viewer (@test.viewer)", prompt)
        self.assertIn("@six.bit, @pr0x60", prompt)
        self.assertIn("viewers=37", prompt)
        self.assertIn("tapsObserved=125", prompt)
        self.assertIn("queue context", prompt)
        self.assertIn("current-show-only", prompt)
        self.assertIn("untrusted viewer content", prompt)
        self.assertIn("correlated public identity signal", prompt)
        self.assertIn("surface-level lore input", prompt)
        self.assertIn("cannot post, moderate", prompt)
        self.assertNotIn("event_id", prompt)
        self.assertNotIn("room-1", prompt)

    def test_disabled_or_missing_bridge_produces_honest_unavailable_context(self):
        disabled = build_live_prompt_context("/missing", enabled=False)
        missing = build_live_prompt_context("/missing", enabled=True)
        self.assertIn("disabled", disabled)
        self.assertIn("Do not invent", disabled)
        self.assertIn("unavailable", missing)
        self.assertIn("do not expose infrastructure detail", missing)

    def test_diagnostics_never_include_comment_text_or_runtime_path(self):
        clock = Clock()
        adapter = self.make_adapter(clock)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "live-context.json"
            LiveContextSnapshotWriter(str(path), time_fn=clock).publish(adapter, force=True)
            diagnostics = live_context_diagnostics(
                str(path),
                enabled=True,
                now=clock(),
            )
        serialized = json.dumps(diagnostics)
        self.assertTrue(diagnostics["snapshotAvailable"])
        self.assertEqual(diagnostics["commentsAccepted"], 1)
        self.assertNotIn("This track is wild", serialized)
        self.assertNotIn(str(path), serialized)

    def test_public_text_spool_keeps_every_comment_and_question_with_ids(self):
        clock = Clock()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "public-conversation.ndjson"
            writer = TikTokPublicConversationSpoolWriter(str(path))
            self.assertTrue(writer.append(observation_payload(
                "comment",
                "comment-1",
                clock,
                comment_text="This track is wild.",
            )))
            self.assertTrue(writer.append(observation_payload(
                "question",
                "question-1",
                clock,
                question_text="Who is next?",
            )))
            self.assertFalse(writer.append(observation_payload(
                "like",
                "like-1",
                clock,
                like_count=25,
            )))
            result = read_public_conversation_spool(str(path))
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)

        self.assertEqual(result.reason, "ok")
        self.assertEqual(
            [record["event_id"] for record in result.records],
            ["comment-1", "question-1"],
        )
        self.assertEqual(result.records[1]["question_text"], "Who is next?")

    def test_handle_and_display_name_correlate_pr0x60_to_owner(self):
        identity = resolve_tiktok_identity(
            {
                "event_id": "comment-1",
                "unique_id": "pr0x60",
                "display_name": "PR0X",
                "moderator_flag": True,
            },
            owner_user_id=601,
        )
        self.assertEqual(identity.subject_ref, "discord_user:601")
        self.assertEqual(identity.bound_discord_user_id, 601)
        self.assertTrue(identity.trusted_platform_identity)
        self.assertTrue(identity.trusted_room_moderator)

    def test_declared_six_bit_primary_handle_resolves_to_same_owner(self):
        identity = resolve_tiktok_identity(
            {
                "event_id": "comment-primary-owner",
                "unique_id": "six.bit",
                "display_name": "6 Bit",
            },
            owner_user_id=601,
        )
        self.assertEqual(identity.subject_ref, "discord_user:601")
        self.assertEqual(
            identity.binding_basis,
            "owner_declared_exact_tiktok_handle",
        )

    def test_general_binding_requires_supporting_handle_and_display_signals(self):
        known = {71: ("Signal Fox",), 72: ("Another Person",)}
        linked = resolve_tiktok_identity(
            {
                "event_id": "comment-2",
                "unique_id": "signalfox77",
                "display_name": "Signal Fox",
            },
            known_discord_identities=known,
        )
        unlinked = resolve_tiktok_identity(
            {
                "event_id": "comment-3",
                "unique_id": "signalfox77",
                "display_name": "Unrelated Name",
            },
            known_discord_identities=known,
        )
        self.assertEqual(linked.subject_ref, "discord_user:71")
        self.assertEqual(unlinked.subject_ref, "tiktok_user:signalfox77")


if __name__ == "__main__":
    unittest.main()
