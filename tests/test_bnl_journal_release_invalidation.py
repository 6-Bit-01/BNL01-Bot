import json
import os
import sqlite3
import tempfile
import unittest
from datetime import date, datetime, timedelta
from unittest.mock import patch

os.environ.setdefault("GEMINI_API_KEY", "test-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-token")

import bnl_journal as journal
import bnl_journal_automation as automation
import bnl_journal_source_store as source_store
from tests.test_bnl_journal_prepared_release import AcceptedResponse, article_json


TARGET_DAY = date(2026, 7, 20)
WEEK_START = date(2026, 7, 13)


class JournalReleaseInvalidationTests(unittest.TestCase):
    def setUp(self):
        self.archive_clock = patch.object(source_store, "_now_ms", return_value=0)
        self.archive_clock.start()
        tmp = tempfile.NamedTemporaryFile(delete=False)
        self.db = tmp.name
        tmp.close()
        journal.ensure_schema(self.db)
        automation.ensure_schema(self.db)
        with sqlite3.connect(self.db) as conn:
            conn.execute(
                "CREATE TABLE website_relay_history("
                "relay_id TEXT PRIMARY KEY,guild_id INTEGER,public_message TEXT,"
                "public_directive TEXT,mode TEXT,relay_lane TEXT,event_type TEXT,"
                "highest_source_conversation_id INTEGER,normalized_message TEXT,"
                "semantic_family TEXT,published_timestamp TEXT)"
            )
            conn.execute(
                "CREATE TABLE conversations("
                "id INTEGER PRIMARY KEY,user_id INTEGER,user_name TEXT,guild_id INTEGER,"
                "channel_name TEXT,channel_policy TEXT,role TEXT,content TEXT,timestamp TEXT,"
                "public_usable INTEGER,visibility TEXT)"
            )

    def tearDown(self):
        self.archive_clock.stop()
        try:
            os.unlink(self.db)
        except FileNotFoundError:
            pass

    def add_day(self, day: date) -> None:
        start, _, _ = automation._daily_period_for_day(day)
        start_utc = datetime.fromisoformat(start.replace("Z", "+00:00"))
        label = day.isoformat()
        with sqlite3.connect(self.db) as conn:
            for index in range(6):
                stamp = (start_utc + timedelta(hours=1, minutes=index)).isoformat().replace(
                    "+00:00", "Z"
                )
                conn.execute(
                    "INSERT INTO website_relay_history VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        f"relay-{label}-{index}",
                        1,
                        f"A public producer shared track activity number {index}.",
                        "Keep listening to the community music room.",
                        "OBSERVATION",
                        "current_signal",
                        "fresh_public_discord_activity",
                        index,
                        "track activity",
                        "public_discord_activity",
                        stamp,
                    ),
                )
                conn.execute(
                    "INSERT INTO conversations VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        int(label.replace("-", "")) * 10 + index,
                        100 + index,
                        f"Member{index}",
                        1,
                        "general",
                        "public_home",
                        "user",
                        (
                            f"Member{index} discusses a new mix, bass movement, and "
                            f"community listening plan number {index}."
                        ),
                        stamp,
                        1,
                        "public_safe",
                    ),
                )

    @staticmethod
    def accepted_opener(request, timeout=10):
        return AcceptedResponse(request)

    def add_broadcast_memory(self, *, valid_until=None) -> None:
        with sqlite3.connect(self.db) as conn:
            conn.execute(
                "CREATE TABLE broadcast_memory("
                "id INTEGER PRIMARY KEY,guild_id INTEGER,episode_date TEXT,cleaned_summary TEXT,"
                "entry_type TEXT,importance TEXT,public_safe INTEGER,usage_scope TEXT,valid_until TEXT,"
                "needs_clarification INTEGER,status TEXT,superseded_by_id INTEGER,created_at TEXT,raw_note TEXT)"
            )
            conn.execute(
                "INSERT INTO broadcast_memory VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    1,
                    1,
                    "2026-07-18",
                    "A public producer documented track activity number four.",
                    "notable_moment",
                    "medium",
                    1,
                    "journal",
                    valid_until,
                    0,
                    "active",
                    None,
                    "2026-07-18T12:00:00Z",
                    "private operator note",
                ),
            )

    @staticmethod
    def memory_generator(packet, _prompt):
        memory = packet["generationContextLanes"]["establishedBroadcastMemory"][0]
        fresh_ref = memory["matchedFreshSourceRefIds"][0]
        claim = (
            "An established Network record says a public producer documented track activity "
            "number four."
        )
        article = json.loads(article_json(packet))
        heading = article["sections"][0]["heading"]
        article["sections"][0]["body"] = claim + " " + article["sections"][0]["body"]
        article["metadata"]["contextUses"] = [
            {
                "laneType": "established_broadcast_memory",
                "laneRefId": memory["laneRefId"],
                "sectionHeading": heading,
                "claim": claim,
                "basisRefIds": [memory["laneRefId"], fresh_ref],
            }
        ]
        return json.dumps(article)

    def prepare_daily(self, generator=None):
        return automation.prepare_daily(
            self.db,
            1,
            generator or (lambda packet, _prompt: article_json(packet)),
            target_day=TARGET_DAY,
            force=True,
        )

    def release_daily(self, opener, **kwargs):
        return automation.release_daily(
            self.db,
            1,
            "https://site.example",
            "key",
            target_day=TARGET_DAY,
            force=True,
            opener=opener,
            **kwargs,
        )

    def run_row(self, cadence: str) -> dict:
        with sqlite3.connect(self.db) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM bnl_journal_automation_runs WHERE cadence=? "
                "ORDER BY created_at DESC LIMIT 1",
                (cadence,),
            ).fetchone()
        return dict(row) if row else {}

    def automation_state(self) -> dict:
        with sqlite3.connect(self.db) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM bnl_journal_automation_state WHERE guild_id=1"
            ).fetchone()
        return dict(row) if row else {}

    def daily_observation_link(self):
        with sqlite3.connect(self.db) as conn:
            return conn.execute(
                "SELECT lifecycle_state,journal_entry_id,journal_revision "
                "FROM bnl_journal_observations "
                "WHERE guild_id=1 AND observation_date=?",
                (TARGET_DAY.isoformat(),),
            ).fetchone()

    def test_nonprivacy_payload_integrity_failure_retains_packet_and_marks_state_held(self):
        self.add_day(TARGET_DAY)
        prepared = self.prepare_daily()
        self.assertEqual("prepared", prepared.status, prepared)
        before = self.run_row("daily")
        self.assertTrue(before["frozen_packet_json"])

        with sqlite3.connect(self.db) as conn:
            canonical = bytes(
                conn.execute(
                    "SELECT canonical_payload_bytes FROM bnl_journal_entries "
                    "WHERE guild_id=1 AND entry_id=? AND revision=?",
                    (prepared.entry_id, prepared.revision),
                ).fetchone()[0]
            )
            conn.execute(
                "UPDATE bnl_journal_entries SET canonical_payload_bytes=? "
                "WHERE guild_id=1 AND entry_id=? AND revision=?",
                (canonical + b" ", prepared.entry_id, prepared.revision),
            )

        posts = []

        def forbidden_opener(request, timeout=10):
            posts.append(request.data)
            raise AssertionError("integrity-invalid bytes reached the website")

        result = self.release_daily(forbidden_opener)
        self.assertEqual("held", result.status, result)
        self.assertEqual("prepared_payload_integrity_failed", result.reason)
        self.assertEqual("", result.entry_id)
        self.assertEqual(0, result.revision)
        self.assertEqual([], posts)

        after = self.run_row("daily")
        self.assertEqual("held", after["lifecycle_state"])
        self.assertEqual(before["frozen_packet_json"], after["frozen_packet_json"])
        self.assertEqual(before["frozen_packet_hash"], after["frozen_packet_hash"])
        state = self.automation_state()
        self.assertEqual("held", state["last_status"])
        self.assertEqual("prepared_payload_integrity_failed", state["last_reason"])
        self.assertEqual(("held", None, 0), self.daily_observation_link())

    def test_payload_mutation_between_claim_and_final_fence_makes_zero_posts(self):
        self.add_day(TARGET_DAY)
        prepared = self.prepare_daily()
        self.assertEqual("prepared", prepared.status, prepared)
        before = self.run_row("daily")
        posts = []

        def forbidden_opener(request, timeout=10):
            posts.append(request.data)
            raise AssertionError("bytes changed after delivery claim reached the website")

        real_deliver = automation.deliver_approved

        def mutate_then_enter_delivery_fence(*args, **kwargs):
            with sqlite3.connect(self.db) as conn:
                canonical = bytes(
                    conn.execute(
                        "SELECT canonical_payload_bytes FROM bnl_journal_entries "
                        "WHERE guild_id=1 AND entry_id=? AND revision=?",
                        (prepared.entry_id, prepared.revision),
                    ).fetchone()[0]
                )
                conn.execute(
                    "UPDATE bnl_journal_entries SET canonical_payload_bytes=? "
                    "WHERE guild_id=1 AND entry_id=? AND revision=?",
                    (canonical + b" ", prepared.entry_id, prepared.revision),
                )
            return real_deliver(*args, **kwargs)

        with patch.object(
            automation,
            "deliver_approved",
            side_effect=mutate_then_enter_delivery_fence,
        ):
            result = self.release_daily(forbidden_opener)

        self.assertEqual("held", result.status, result)
        self.assertEqual("prepared_payload_integrity_failed", result.reason)
        self.assertEqual("", result.entry_id)
        self.assertEqual(0, result.revision)
        self.assertEqual([], posts)
        after = self.run_row("daily")
        self.assertEqual("held", after["lifecycle_state"])
        self.assertEqual(before["frozen_packet_json"], after["frozen_packet_json"])
        self.assertEqual(before["frozen_packet_hash"], after["frozen_packet_hash"])
        self.assertIsNone(after["prepared_payload_hash"])
        with sqlite3.connect(self.db) as conn:
            lifecycle = conn.execute(
                "SELECT lifecycle_state FROM bnl_journal_entries "
                "WHERE guild_id=1 AND entry_id=? AND revision=?",
                (prepared.entry_id, prepared.revision),
            ).fetchone()[0]
        self.assertEqual("rejected", lifecycle)
        self.assertEqual(("held", None, 0), self.daily_observation_link())

    def test_weekly_daily_memory_exclusion_blocks_post_and_retires_packet(self):
        for offset in range(7):
            day = WEEK_START + timedelta(days=offset)
            self.add_day(day)
            if offset == 6:
                continue
            published = automation.run_daily(
                self.db,
                1,
                lambda packet, _prompt: article_json(packet),
                "https://site.example",
                "key",
                target_day=day,
                force=True,
                opener=self.accepted_opener,
            )
            self.assertEqual("published", published.status, published)

        prepared = automation.prepare_weekly(
            self.db,
            1,
            lambda packet, _prompt: article_json(packet),
            target_monday=WEEK_START,
            force=True,
        )
        self.assertEqual("prepared", prepared.status, prepared)
        with sqlite3.connect(self.db) as conn:
            daily_entry_ids = {
                str(row[0])
                for row in conn.execute(
                    "SELECT journal_entry_id FROM bnl_journal_observations "
                    "WHERE guild_id=1 AND lifecycle_state='published'"
                ).fetchall()
                if row[0]
            }
            metadata = json.loads(
                conn.execute(
                    "SELECT metadata_json FROM bnl_journal_private_metadata "
                    "WHERE guild_id=1 AND entry_id=? AND revision=?",
                    (prepared.entry_id, prepared.revision),
                ).fetchone()[0]
            )
        self.assertEqual(6, len(daily_entry_ids))
        self.assertTrue(daily_entry_ids <= set(metadata["relatedPriorJournalEntryIds"]))
        excluded_entry_id = sorted(daily_entry_ids)[0]
        automation.store_journal_memory_exclusions(
            self.db,
            1,
            {excluded_entry_id},
        )

        posts = []

        def forbidden_opener(request, timeout=10):
            posts.append(request.data)
            raise AssertionError("memory-excluded Weekly reached the website")

        result = automation.release_weekly(
            self.db,
            1,
            "https://site.example",
            "key",
            target_monday=WEEK_START,
            force=True,
            opener=forbidden_opener,
            # Simulate a release caller that captured the prior empty set. The
            # final fence must use the newer durable control snapshot.
            memory_excluded_entry_ids=set(),
        )
        self.assertEqual("held", result.status, result)
        self.assertEqual("privacy_eligibility_changed", result.reason)
        self.assertEqual("", result.entry_id)
        self.assertEqual(0, result.revision)
        self.assertEqual([], posts)
        run = self.run_row("weekly")
        self.assertEqual("held", run["lifecycle_state"])
        self.assertIsNone(run["frozen_packet_json"])
        self.assertIsNone(run["frozen_packet_hash"])
        self.assertIsNone(run["prepared_payload_hash"])

    def test_used_broadcast_memory_becoming_ineligible_blocks_post(self):
        self.add_broadcast_memory()
        self.add_day(TARGET_DAY)
        prepared = self.prepare_daily(self.memory_generator)
        self.assertEqual("prepared", prepared.status, prepared)
        with sqlite3.connect(self.db) as conn:
            metadata = json.loads(
                conn.execute(
                    "SELECT metadata_json FROM bnl_journal_private_metadata "
                    "WHERE guild_id=1 AND entry_id=? AND revision=?",
                    (prepared.entry_id, prepared.revision),
                ).fetchone()[0]
            )
            used = metadata["usedContextLaneProvenance"]["establishedBroadcastMemory"]
            self.assertEqual([1], [int(item["rowId"]) for item in used])
            conn.execute("UPDATE broadcast_memory SET status='inactive' WHERE guild_id=1 AND id=1")

        posts = []

        def forbidden_opener(request, timeout=10):
            posts.append(request.data)
            raise AssertionError("ineligible established memory reached the website")

        result = self.release_daily(forbidden_opener)
        self.assertEqual("held", result.status, result)
        self.assertEqual("privacy_memory_ineligible", result.reason)
        self.assertEqual([], posts)
        run = self.run_row("daily")
        self.assertEqual("held", run["lifecycle_state"])
        self.assertIsNone(run["frozen_packet_json"])
        self.assertIsNone(run["prepared_payload_hash"])

    def test_used_broadcast_memory_expiring_after_preparation_blocks_post(self):
        # TARGET_DAY ends before this date expires, so the memory may be used
        # during preparation. At the patched release instant it is stale.
        self.add_broadcast_memory(valid_until="2026-07-22")
        self.add_day(TARGET_DAY)
        prepared = self.prepare_daily(self.memory_generator)
        self.assertEqual("prepared", prepared.status, prepared)

        posts = []

        def forbidden_opener(request, timeout=10):
            posts.append(request.data)
            raise AssertionError("expired established memory reached the website")

        with patch.object(automation, "utc_now_iso", return_value="2026-07-23T12:00:00Z"):
            result = self.release_daily(forbidden_opener)
        self.assertEqual("held", result.status, result)
        self.assertEqual("privacy_memory_ineligible", result.reason)
        self.assertEqual([], posts)
        run = self.run_row("daily")
        self.assertEqual("held", run["lifecycle_state"])
        self.assertIsNone(run["frozen_packet_json"])
        self.assertIsNone(run["prepared_payload_hash"])

    def test_deletion_between_claim_and_post_is_revalidated_inside_delivery_fence(self):
        self.add_day(TARGET_DAY)
        prepared = self.prepare_daily()
        self.assertEqual("prepared", prepared.status, prepared)
        posts = []

        def forbidden_opener(request, timeout=10):
            posts.append(request.data)
            raise AssertionError("source deleted before the final fence reached the website")

        real_deliver = automation.deliver_approved

        def delete_then_enter_delivery_fence(*args, **kwargs):
            self.assertEqual(1, source_store.purge_user_discord_sources(self.db, 1, 100))
            return real_deliver(*args, **kwargs)

        with patch.object(
            automation,
            "deliver_approved",
            side_effect=delete_then_enter_delivery_fence,
        ):
            result = self.release_daily(forbidden_opener)

        self.assertEqual("held", result.status, result)
        self.assertEqual("privacy_source_ineligible", result.reason)
        self.assertEqual([], posts)
        run = self.run_row("daily")
        self.assertEqual("held", run["lifecycle_state"])
        self.assertIsNone(run["journal_entry_id"])
        self.assertIsNone(run["frozen_packet_json"])
        self.assertIsNone(run["prepared_payload_hash"])
        with sqlite3.connect(self.db) as conn:
            lifecycle = conn.execute(
                "SELECT lifecycle_state FROM bnl_journal_entries "
                "WHERE guild_id=1 AND entry_id=? AND revision=?",
                (prepared.entry_id, prepared.revision),
            ).fetchone()[0]
        self.assertEqual("rejected", lifecycle)
        self.assertEqual(("held", None, 0), self.daily_observation_link())


if __name__ == "__main__":
    unittest.main()
