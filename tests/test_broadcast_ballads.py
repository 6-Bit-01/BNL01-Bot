import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock

from bnl_broadcast_ballads import (
    execute_command, versions, creative_history, route_for_command, parse_draft,
    BalladGeneration, PROMPT_VERSION, ROUTE, MANUAL_ROUTE,
)
from bnl_gemini_routing import policy_for_route


class BalladTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db = str(Path(self.tmp.name) / "test.db")
        self.command = dict(id="draft-1", showId="show-1", showDate="2026-09-11", kind="generate", baseVersion=None, options={})
        self.generate = AsyncMock(return_value=json.dumps(dict(title="The Chairs Stayed Warm", lyrics="[Verse]\nThe chairs stayed warm after the room went quiet.\n[Chorus]\nLeave a light for the last one home.", style="1977 chamber soul with dub bass, brushed drums and a close dry vocal.", palette=dict(topics="the last listener", hook="Leave a light", genres="chamber soul, dub"))))

    async def run_command(self, command=None, **kw):
        return await execute_command(self.db, 77, command or self.command,
            evidence_reader=kw.get("reader", lambda _: ("Authorized public show record", "a" * 64)),
            generate=self.generate)

    async def test_duplicate_transport_replays_exact_version_without_generation(self):
        first = await self.run_command()
        second = await self.run_command()
        self.assertEqual(first, second)
        self.generate.assert_awaited_once()
        self.assertEqual(len(versions(self.db, 77, "show-1")), 1)

    async def test_liner_notes_share_the_song_call_and_survive_edit_restore_and_replay(self):
        notes = dict(about="A warm last-listener anthem.", inspiration="I kept the last light on for Test Member.",
                     mentions="Test Member — the last listener.", inspiredBy="The final goodbye in public show chat.")
        output = json.loads(self.generate.return_value)
        output["linerNotes"] = notes
        self.generate.return_value = json.dumps(output)
        original = (await self.run_command())["version"]
        self.assertEqual(original["linerNotes"], notes)
        self.assertEqual(original["promptVersion"], PROMPT_VERSION)
        edit = await self.run_command({**self.command, "id": "edit-2", "kind": "edit", "baseVersion": "draft-1", "content": dict(title="Same night", lyrics="Leave a light on", style="1977 soul")})
        self.assertEqual(edit["version"]["linerNotes"], notes)
        restored = await self.run_command({**self.command, "id": "restore-3", "kind": "restore", "baseVersion": "edit-2", "restoreVersion": "draft-1"})
        self.assertEqual(restored["version"]["linerNotes"], notes)
        self.assertEqual((await self.run_command())["version"], original)
        self.generate.assert_awaited_once()

    def test_optional_liner_notes_never_reject_a_usable_song(self):
        for notes in (None, "malformed", {"about": ["not text"], "mentions": "Test Member", "privateNotes": "not public"}):
            draft = parse_draft(json.dumps(dict(lyrics="A good hook", style="1977 soul", linerNotes=notes)), "2026-09-11")
            self.assertEqual(draft["lyrics"], "A good hook")
            self.assertEqual(set(draft["linerNotes"]), {"about", "inspiration", "mentions", "inspiredBy"})
            self.assertEqual(draft["linerNotes"]["about"], "")
            self.assertNotIn("not public", json.dumps(draft["linerNotes"]))

    async def test_restoring_a_legacy_version_without_notes_keeps_the_song(self):
        await self.run_command()
        with sqlite3.connect(self.db) as conn:
            legacy = json.loads(conn.execute("SELECT document FROM bnl_ballad_versions").fetchone()[0])
            legacy.pop("linerNotes", None)
            conn.execute("UPDATE bnl_ballad_versions SET document=?", (json.dumps(legacy),))
        restored = await self.run_command({**self.command, "id": "restore-2", "kind": "restore", "baseVersion": "draft-1", "restoreVersion": "draft-1"})
        self.assertEqual(restored["outcome"], "complete")
        self.assertEqual(restored["version"]["lyrics"], legacy["lyrics"])
        self.assertFalse(any(restored["version"]["linerNotes"].values()))

    async def test_provider_failure_preserves_original_and_does_not_retry(self):
        first = await self.run_command()
        self.generate.side_effect = RuntimeError("provider detail must not leak")
        cmd = {**self.command, "id": "polish-2", "kind": "polish", "baseVersion": "draft-1"}
        failure = await self.run_command(cmd)
        replay = await self.run_command(cmd)
        self.assertEqual(failure, replay)
        self.assertEqual(failure["error"], "RuntimeError")
        self.assertEqual(len(versions(self.db, 77, "show-1")), 1)
        self.assertEqual(versions(self.db, 77, "show-1")[0], first["version"])
        self.assertEqual(self.generate.await_count, 2)

    async def test_unstructured_and_short_output_is_preserved_without_quality_gate(self):
        self.generate.return_value = "A weird little hook. A weird little hook."
        receipt = await self.run_command()
        self.assertEqual(receipt["outcome"], "complete")
        self.assertEqual(receipt["version"]["lyrics"], self.generate.return_value)
        self.assertEqual(receipt["version"]["rawOutput"], self.generate.return_value)
        self.generate.assert_awaited_once()

    async def test_interrupted_json_recovers_readable_fields_and_replays_without_retry(self):
        raw = '{"title":"Last Light","lyrics":"[Verse]\\nA warm light waits.\\n[Chorus]\\nLeave a'
        self.generate.return_value = BalladGeneration(raw, "MAX_TOKENS")
        with self.assertLogs(level="INFO") as logs:
            receipt = await self.run_command()
        draft = receipt["version"]
        self.assertEqual(draft["title"], "Last Light")
        self.assertEqual(draft["lyrics"], "[Verse]\nA warm light waits.\n[Chorus]\nLeave a")
        self.assertEqual(draft["rawOutput"], raw)
        self.assertEqual(draft["generationStatus"], "incomplete")
        self.assertEqual(draft["finishReason"], "MAX_TOKENS")
        self.assertIn("Incomplete response", draft["note"])
        self.assertFalse(draft["style"])
        self.assertFalse(any(draft["linerNotes"].values()))
        self.assertIn("status=incomplete finish_reason=MAX_TOKENS", " ".join(logs.output))
        self.assertNotIn("A warm light", " ".join(logs.output))
        self.assertEqual(await self.run_command(), receipt)
        self.generate.assert_awaited_once()
        restored = await self.run_command({**self.command, "id": "restore-2", "kind": "restore",
                                          "baseVersion": "draft-1", "restoreVersion": "draft-1"})
        self.assertEqual(restored["version"]["generationStatus"], "incomplete")
        self.assertEqual(restored["version"]["note"], draft["note"])

    def test_interruption_at_each_escaped_lyric_character_preserves_only_received_text(self):
        lyrics = '[Verse]\nA "style": "echo" sings café 🎵 along the C:\\road.\n[Outro]\nHome.'
        prefix = '{"title":"Last Light","style":"Chamber soul","lyrics":'
        encoded = json.dumps(lyrics, ensure_ascii=True)
        for length in range(2, len(encoded)):
            raw = prefix + encoded[:length]
            with self.subTest(cut=length):
                draft = parse_draft(raw, "2026-09-11")
                self.assertTrue(lyrics.startswith(draft["lyrics"]))
                self.assertEqual(draft["title"], "Last Light")
                self.assertEqual(draft["style"], "Chamber soul")
                self.assertEqual(draft["generationStatus"], "incomplete")
                draft["lyrics"].encode("utf-8")  # no broken surrogate escapes
        draft = parse_draft(prefix + encoded + '}', "2026-09-11", "STOP")
        self.assertEqual(draft["lyrics"], lyrics)
        self.assertEqual(draft["generationStatus"], "complete")

    def test_cutoff_in_nested_notes_and_fenced_output_keeps_available_fields(self):
        raw = '```json\n{"title":"Light","lyrics":"Leave a light.","style":"Soul",' \
              '"linerNotes":{"about":"A late goodbye.","inspiration":"I kept the'
        draft = parse_draft(raw, "2026-09-11")
        self.assertEqual(draft["lyrics"], "Leave a light.")
        self.assertEqual(draft["linerNotes"]["about"], "A late goodbye.")
        self.assertEqual(draft["linerNotes"]["inspiration"], "I kept the")
        self.assertEqual(draft["linerNotes"]["mentions"], "")
        self.assertEqual(draft["generationStatus"], "incomplete")

    def test_valid_json_still_warns_when_provider_reports_output_limit(self):
        draft = parse_draft(self.generate.return_value, "2026-09-11", "MAX_TOKENS")
        self.assertEqual(draft["generationStatus"], "incomplete")
        self.assertIn("Polish saved draft", draft["note"])

    async def test_unusable_json_is_a_visible_format_failure_without_overwriting_or_retry(self):
        original = (await self.run_command())["version"]
        command = {**self.command, "id": "broken-2", "baseVersion": original["id"]}
        self.generate.return_value = '{"title":"Light","style":"Soul'
        result = await self.run_command(command)
        self.assertEqual(result["error"], "incomplete_response_without_lyrics_try_manually")
        self.assertEqual(await self.run_command(command), result)
        self.assertEqual(versions(self.db, 77, "show-1"), [original])
        self.assertEqual(self.generate.await_count, 2)

    async def test_polish_recovers_legacy_json_fallback_without_mutating_saved_version(self):
        original = (await self.run_command())["version"]
        raw = '{"title":"Last Light","lyrics":"[Verse]\\nA warm light waits.\\n[Chorus]\\nLeave a'
        # Emulate the old parser's saved fallback, without rewriting on deploy.
        original.update(title="Broadcast Ballad 2026-09-11", lyrics=raw, rawOutput=raw, style="",
                        linerNotes={}, note="Original output preserved.", promptVersion="broadcast-ballad-4")
        original.pop("generationStatus", None)
        original.pop("finishReason", None)
        with sqlite3.connect(self.db) as conn:
            conn.execute("UPDATE bnl_ballad_versions SET document=?", (json.dumps(original),))
        command = {**self.command, "id": "polish-2", "kind": "polish", "baseVersion": original["id"]}
        result = await self.run_command(command)
        prompt = self.generate.await_args.args[0]
        revision = json.loads(prompt.split("EXISTING DRAFT (only revise if requested):\n", 1)[1]
                              .split("\nWRITING REMINDER:", 1)[0])
        self.assertEqual(revision["title"], "Last Light")
        self.assertEqual(revision["lyrics"], "[Verse]\nA warm light waits.\n[Chorus]\nLeave a")
        self.assertIn("finish the interrupted ending", prompt)
        self.assertIn("Do not start a new composition", prompt)
        self.assertEqual(versions(self.db, 77, "show-1")[0], original)
        self.assertEqual(await self.run_command(command), result)
        self.assertEqual(self.generate.await_count, 2)

    async def test_polish_does_not_replace_producer_edits_with_legacy_raw_response(self):
        original = (await self.run_command())["version"]
        original["rawOutput"] = '{"title":"Different","lyrics":"Wrong prior text'
        with sqlite3.connect(self.db) as conn:
            conn.execute("UPDATE bnl_ballad_versions SET document=?", (json.dumps(original),))
        await self.run_command({**self.command, "id": "polish-2", "kind": "polish", "baseVersion": original["id"]})
        prompt = self.generate.await_args.args[0]
        self.assertNotIn("Wrong prior text", prompt)
        self.assertIn(json.dumps(original["lyrics"])[1:-1], prompt)

    async def test_edit_restore_and_conflict_preserve_history(self):
        original = (await self.run_command())["version"]
        edit = await self.run_command({**self.command, "id": "edit-2", "kind": "edit", "baseVersion": "draft-1", "content": dict(title="Edited title", lyrics="An edited chorus", style="2020 soul")})
        conflict = await self.run_command({**self.command, "id": "stale-edit", "kind": "edit", "baseVersion": "draft-1", "content": dict(title="Stale", lyrics="Do not overwrite", style="")})
        restored = await self.run_command({**self.command, "id": "restore-3", "kind": "restore", "baseVersion": "edit-2", "restoreVersion": "draft-1"})
        self.assertEqual(conflict["outcome"], "failed")
        self.assertEqual(restored["version"]["lyrics"], original["lyrics"])
        self.assertEqual(edit["version"]["lyrics"], "An edited chorus")
        self.assertEqual(len(versions(self.db, 77, "show-1")), 3)
        self.generate.assert_awaited_once()

    async def test_missing_authorized_show_never_calls_provider(self):
        receipt = await self.run_command(reader=lambda _: ("", ""))
        self.assertEqual(receipt["outcome"], "failed")
        self.generate.assert_not_awaited()

    async def test_prompt_uses_catalog_as_art_and_honors_direction_without_scores(self):
        await self.run_command({**self.command, "options": {"feedback": "Keep that dry humor; try a gentler hook."}})
        command = {**self.command, "id": "next-show", "showId": "show-2", "options": {"genres": "cumbia and art rock", "era": "2020"}}
        await self.run_command(command)
        prompt = self.generate.await_args.args[0]
        self.assertIn("The Chairs Stayed Warm", prompt)
        self.assertIn("CREATIVE WORK, not factual evidence", prompt)
        self.assertIn("cumbia and art rock", prompt)
        self.assertIn("2020", prompt)
        self.assertIn("No novelty threshold", prompt)
        self.assertTrue(creative_history(self.db, 77))
        history = creative_history(self.db, 77, selected_versions={"show-1": "draft-1"})
        selected = next(v for v in history if v["selectedForShow"])
        self.assertEqual(selected["producerFeedback"], "Keep that dry humor; try a gentler hook.")

    async def test_new_draft_keeps_history_without_replaying_the_previous_verse(self):
        original = (await self.run_command())["version"]
        command = {**self.command, "id": "fresh-2", "baseVersion": original["id"],
                   "options": {"feedback": "A fresh composition with richer verse rhymes."}}
        await self.run_command(command)
        prompt = self.generate.await_args.args[0]
        history_text, remainder = prompt.split("PRIOR CREATIVE CATALOG:\n", 1)[1].split(
            "\nEXISTING DRAFT (only revise if requested):\n", 1)
        history = json.loads(history_text)
        self.assertEqual(history[0]["showId"], original["showId"])
        self.assertEqual(history[0]["title"], original["title"])
        self.assertEqual(history[0]["style"], original["style"])
        self.assertEqual(history[0]["palette"]["hook"], original["palette"]["hook"])
        self.assertIn("the last one home.", history[0]["lineEndingsUsed"])
        self.assertNotIn("lyrics", history[0])
        self.assertNotIn("The chairs stayed warm after the room went quiet.", prompt)
        self.assertIsNone(json.loads(remainder.split("\nWRITING REMINDER:", 1)[0]))
        self.assertIn("A fresh composition with richer verse rhymes.", prompt)
        self.assertEqual(versions(self.db, 77, "show-1")[0], original)
        self.assertEqual(self.generate.await_count, 2)

    async def test_polish_receives_one_complete_source_lyric_and_preserves_it(self):
        original = (await self.run_command())["version"]
        await self.run_command({**self.command, "id": "polish-2", "kind": "polish",
                                "baseVersion": original["id"], "options": {"feedback": "Keep the hook."}})
        prompt = self.generate.await_args.args[0]
        serialized_lyrics = json.dumps(original["lyrics"], ensure_ascii=False)
        self.assertEqual(prompt.count(serialized_lyrics), 1)
        revision = json.loads(prompt.split("EXISTING DRAFT (only revise if requested):\n", 1)[1]
                              .split("\nWRITING REMINDER:", 1)[0])
        self.assertEqual(revision["lyrics"], original["lyrics"])
        self.assertEqual(revision["style"], original["style"])
        self.assertNotIn("rawOutput", revision)
        self.assertIn("Keep the hook.", prompt)
        self.assertEqual(versions(self.db, 77, "show-1")[0], original)
        self.assertEqual(self.generate.await_count, 2)

    async def test_compact_catalog_uses_the_selected_recording_version(self):
        original = (await self.run_command())["version"]
        await self.run_command({**self.command, "id": "edit-2", "kind": "edit",
                                "baseVersion": original["id"], "content": dict(
                                    title="Unselected experiment", lyrics="Different words entirely.", style="1999 ska")})
        await self.run_command({**self.command, "id": "other-show", "showId": "show-2",
                                "catalogVersions": {"show-1": original["id"]}})
        prompt = self.generate.await_args.args[0]
        history = json.loads(prompt.split("PRIOR CREATIVE CATALOG:\n", 1)[1].split(
            "\nEXISTING DRAFT (only revise if requested):\n", 1)[0])
        self.assertTrue(history[0]["selectedForShow"])
        self.assertEqual(history[0]["title"], original["title"])
        self.assertIn("the last one home.", history[0]["lineEndingsUsed"])
        self.assertNotIn("Unselected experiment", prompt)
        self.assertNotIn("Different words entirely.", prompt)
        self.assertEqual(len(versions(self.db, 77, "show-1")), 2)

    def test_manual_and_automatic_commands_use_their_budget_priority(self):
        self.assertEqual(route_for_command(self.command), MANUAL_ROUTE)
        self.assertEqual(route_for_command({**self.command, "kind": "polish"}), MANUAL_ROUTE)
        self.assertEqual(route_for_command({**self.command, "id": "auto-show-1"}), ROUTE)
        self.assertEqual(route_for_command({**self.command, "id": "auto-other-show"}), MANUAL_ROUTE)
        self.assertEqual(policy_for_route(MANUAL_ROUTE).lane, "conversation")
        self.assertTrue(policy_for_route(ROUTE).showday_protected)
        for route in (ROUTE, MANUAL_ROUTE):
            policy = policy_for_route(route)
            self.assertEqual(policy.provider_retries, 0)
            self.assertFalse(policy.allow_fallback)


if __name__ == "__main__":
    unittest.main()
