"""A selected show view must not describe omitted records as absent evidence."""

import json
import hashlib
import sqlite3
import tempfile
import unittest
import weakref
from contextlib import closing
from datetime import date, timedelta
from pathlib import Path
from unittest import mock

import test_tiktok_show_evidence_ledger as fixture
from bnl_journal_source_store import record_source_event
from bnl_tiktok_show_ledger import build_tiktok_show_evidence_context
import bnl_tiktok_show_ledger as shows
import bnl_canon_source_contract as contracts


class ShowRetrievalScopeTests(unittest.TestCase):
    def test_separation_constraint_does_not_combine_show_participant_history(self):
        from bnl_tiktok_show_ledger import _document_relevance

        people = [
            dict(speakerLabel="Cedar Vale", handle="cedar_vale", subjectRef="tiktok_user:cedar_vale"),
            dict(speakerLabel="Cedar Glass", handle="glass_alias", subjectRef="tiktok_user:glass_alias"),
        ]
        ledger = dict(showDate="2026-09-08", participants=people)
        for request in (
            "What did Cedar Vale say during the show? Keep his history separate from Cedar Glass / glass_alias.",
            "Cedar Vale is not Cedar Glass. Tell me about Cedar Vale's public show comments.",
        ):
            with self.subTest(request=request):
                _score, selected = _document_relevance(
                    ledger, user_text=request, subject_ref="", recency_rank=0,
                )
                self.assertEqual([p["subjectRef"] for p in selected], ["tiktok_user:cedar_vale"])
        _score, selected = _document_relevance(
            ledger, user_text="Compare Cedar Vale and Cedar Glass during the show.",
            subject_ref="", recency_rank=0,
        )
        self.assertEqual({p["subjectRef"] for p in selected}, {p["subjectRef"] for p in people})

    def test_retained_people_and_comments_can_be_absent_from_selected_view(self):
        with tempfile.TemporaryDirectory() as directory:
            db_file = str(Path(directory) / "scope.db")
            fixture.TikTokShowEvidenceLedgerTests().seed_source_and_memory(db_file)
            authored = {}
            for index in range(24):
                name = f"Test Viewer {index:02d}"
                text = f"The copper lantern flickers beside station marker {index:02d}."
                authored[name] = text
                result = record_source_event(
                    db_file, guild_id=77, source_kind="tiktok_live_chat",
                    source_key=f"scope-comment-{index}",
                    occurred_at_ms=fixture.stamp("2026-08-29T00:03:00Z") + index * 1000,
                    raw_text=text, sanitized_summary=text,
                    channel_policy="public_context", public_usable=True,
                    subject_ref=f"tiktok_handle:scope.viewer.{index}",
                    private_display_name=name,
                    metadata={"eventType": "comment", "handle": f"scope.viewer.{index}"},
                )
                self.assertTrue(result.ok)
            fixture.sync_tiktok_show_evidence_ledgers(
                db_file, guild_id=77,
                read_model=fixture.authorized_read_model({
                    "currentShow": None, "latestShow": fixture.archived_show(), "shows": [],
                }),
                artist_identity_index=fixture.artist_index(),
                environ=fixture.ENABLED_QUEUE_ENV,
            )
            with sqlite3.connect(db_file) as conn:
                ledger = json.loads(conn.execute(
                    "SELECT ledger_json FROM tiktok_show_evidence_ledgers "
                    "WHERE guild_id=77 AND show_key='show-attendance-1'"
                ).fetchone()[0])
            selection = {}
            request = "What stood out in TikTok chat during the August 28, 2026 show?"
            rendered = build_tiktok_show_evidence_context(
                db_file, guild_id=77, user_text=request, message_limit=16,
                selection_out=selection,
            )
            omitted = {name: text for name, text in authored.items()
                       if name not in rendered and text not in rendered}
            self.assertTrue(omitted, "The fixture must actually exceed the rendered selection.")
            retained = {message["text"] for message in ledger["messages"]}
            retained_labels = {p["speakerLabel"] for p in ledger["participants"]}
            for name, text in omitted.items():
                self.assertIn(text, retained)
                self.assertTrue(any(name in label for label in retained_labels))
            self.assertLess(rendered.index("Retrieval scope:"), rendered.index("Show episode:"))
            self.assertLess(rendered.index("Verification scope:"), rendered.index("Show episode:"))
            self.assertIn("Selected participant records (partial list):", rendered)
            self.assertIn("does not report an exhaustive author or exact-quote absence search", rendered)
            self.assertNotIn("complete eligible TikTok chat ledger", rendered)
            self.assertIn(f'{ledger["coverage"]["eligibleMessageCount"]} TikTok messages;', rendered)
            self.assertIn(f'{ledger["coverage"]["participantCount"]} TikTok participants;', rendered)
            for excerpt in selection["authored_excerpts"]:
                self.assertIn(excerpt[5], rendered)
                if excerpt[6] == "tiktok":
                    self.assertIn(excerpt[5], retained)
            refreshed = {}
            rerendered = build_tiktok_show_evidence_context(
                db_file, guild_id=77, user_text=request, message_limit=16,
                pinned_show_keys=("show-attendance-1",), selection_out=refreshed,
            )
            self.assertEqual(rerendered, rendered)
            self.assertEqual(refreshed["authored_excerpts"], selection["authored_excerpts"])


class ShowReadHydrationTests(unittest.TestCase):
    def document(self, index=0):
        receipt = dict(
            contractVersion=contracts.SHOW_QUEUE_EVIDENCE_AUTHORIZATION_VERSION,
            readModelSource="barcode-network-site", publicOnly=True,
            localQueueProduction=True, websiteQueueProduction=True,
            accessScope="public",
            archiveSchemaVersion=contracts.SHOW_QUEUE_ARCHIVE_SCHEMA_VERSION,
            archiveSource=contracts.SHOW_QUEUE_ARCHIVE_SOURCE,
            archiveVisibility="public_safe", archiveSourceRevision=1,
            archiveSourceDigest="a" * 64, historyCoverageStartedAt="2026-01-01",
        )
        document = dict(
            schemaVersion=shows.SHOW_EVIDENCE_LEDGER_SCHEMA_VERSION,
            showKey="synthetic-show-%s" % index,
            showDate=(date(2026, 1, 1) + timedelta(days=index)).isoformat(),
            lifecycle="finalized", startedAtMs=index * 100000,
            endedAtMs=index * 100000 + 50000, sourceAuthorization=receipt,
            messages=[dict(
                eventId="synthetic-message-%s" % index,
                subjectRef="tiktok_user:fixture", speakerLabel="Fixture Viewer",
                text="The copper lantern flickers beside the river.",
                occurredAtMs=index * 100000 + 1000,
            )],
            participants=[dict(
                subjectRef="tiktok_user:fixture", speakerLabel="Fixture Viewer",
                handle="fixture.viewer",
            )],
            topics=[], trackMoments=[], trackRoster=[], operationalEvents=[],
            discordInteractions=[], discordParticipants=[], showTopics=[],
            coverage=dict(eligibleMessageCount=1, participantCount=1),
        )
        document["sourceDigest"] = hashlib.sha256(
            shows._canonical_json(document).encode("utf-8")
        ).hexdigest()
        return document

    def store(self, conn, document, *, raw_json=None):
        conn.execute("""INSERT INTO tiktok_show_evidence_ledgers
            (guild_id,show_key,schema_version,show_date,lifecycle_status,
             started_at_ms,ended_at_ms,source_digest,ledger_json,created_at,updated_at)
            VALUES(?,?,?,?,?,?,?,?,?,?,?)""", (
                77, document["showKey"], document["schemaVersion"],
                document["showDate"], "finalized", document["startedAtMs"],
                document["endedAtMs"], document["sourceDigest"],
                json.dumps(document) if raw_json is None else raw_json,
                "2026-01-01", "2026-01-01",
            ))

    def test_casual_request_validates_all_200_documents_without_authored_hydration(self):
        with tempfile.TemporaryDirectory() as directory:
            db = str(Path(directory) / "synthetic.db")
            with closing(sqlite3.connect(db)) as conn, conn:
                shows.ensure_tiktok_show_evidence_schema(conn)
                for index in range(200):
                    self.store(conn, self.document(index))
            with mock.patch.object(shows, "_safe_document", wraps=shows._safe_document) as validate:
                with mock.patch.object(shows, "_authored_show_messages",
                                       side_effect=AssertionError("irrelevant authored hydration")):
                    selected = {}
                    self.assertEqual(shows.build_tiktok_show_evidence_context(
                        db, guild_id=77, user_text="What's up?", selection_out=selected,
                    ), "")
                    self.assertEqual(selected, {})
            self.assertEqual(validate.call_count, 200)

    def test_explicit_subject_date_community_and_topic_scopes_keep_relevance(self):
        document = self.document()
        # Scores and complete participant records from the original hydrated
        # reader. All seven scopes must survive streamed text scoring.
        cases = (
            ("What did Fixture Viewer say?", "", False, (), 140, document["participants"]),
            ("What did I say?", "tiktok_user:fixture", True, (), 140, document["participants"]),
            ("Recap the show.", "", False, (), 50, []),
            ("What time did the broadcast start?", "", False, (), 50, []),
            ("Summarize the community.", "", False, (), 44, []),
            ("The copper lantern", "", False, (), 180, []),
            ("What happened on January 1, 2026?", "", False, ("2026-01-01",), 170, []),
        )
        for query, subject, direct, dates, expected_score, expected_participants in cases:
            with self.subTest(query=query):
                with mock.patch.object(shows, "_authored_show_messages",
                                       side_effect=AssertionError("ranking must not hydrate attributed output")):
                    result = shows._document_relevance(
                        document, user_text=query, subject_ref=subject,
                        recency_rank=0, allow_direct_subject=direct,
                        requested_dates=dates,
                    )
                self.assertEqual(result, (expected_score, expected_participants))
        changed_original = {
            **document,
            "messages": [{**document["messages"][0], "text": "An entirely separate passage."}],
        }
        self.assertEqual(shows._document_relevance(
            changed_original, user_text="The copper lantern", subject_ref="", recency_rank=0,
        ), (0, []), "The current original text must still determine topic relevance")

    def test_topic_recall_preserves_full_text_and_exact_root_digest(self):
        with tempfile.TemporaryDirectory() as directory:
            db = str(Path(directory) / "synthetic.db")
            document = self.document()
            with closing(sqlite3.connect(db)) as conn, conn:
                shows.ensure_tiktok_show_evidence_schema(conn)
                self.store(conn, document)
            selected = {}
            text = shows.build_tiktok_show_evidence_context(
                db, guild_id=77, user_text="The copper lantern", selection_out=selected,
            )
            self.assertIn(document["messages"][0]["text"], text)
            self.assertEqual(selected["source_refs"],
                             ((document["showKey"], document["sourceDigest"]),))
            refreshed = {}
            self.assertEqual(shows.build_tiktok_show_evidence_context(
                db, guild_id=77, user_text="The copper lantern",
                pinned_show_keys=(document["showKey"],), selection_out=refreshed,
            ), text)
            self.assertEqual(refreshed, selected)

    def test_both_readers_release_consumed_raw_rows_and_keep_validation_order(self):
        class RawJSON(str):
            pass

        class Rows:
            def __init__(self, rows):
                self.rows = rows

            def fetchone(self):
                return (1,)

            def fetchall(self):
                return self.rows

        class Connection:
            def __init__(self, rows):
                self.rows = rows
                self.closed = False

            def execute(self, sql, _params=()):
                return Rows(self.rows)

            def close(self):
                self.closed = True

        documents = [self.document(index) for index in range(5)]
        documents[1]["sourceDigest"] = "0" * 64
        documents[4]["sourceAuthorization"]["publicOnly"] = False
        digest_payload = dict(documents[4])
        digest_payload.pop("sourceDigest")
        documents[4]["sourceDigest"] = hashlib.sha256(
            shows._canonical_json(digest_payload).encode("utf-8")
        ).hexdigest()
        for reader in ("conversation", "packet"):
            with self.subTest(reader=reader):
                rows = [
                    (RawJSON("{" if index == 2 else json.dumps(document)),)
                    if reader == "conversation" else
                    (document["showKey"], document["sourceDigest"],
                     document["endedAtMs"], RawJSON("{" if index == 2 else json.dumps(document)))
                    for index, document in enumerate(documents)
                ]
                references = [weakref.ref(row[-1]) for row in rows]
                connection = Connection(rows)
                original_loads = json.loads
                decoded = []

                def observe_loads(raw, *args, **kwargs):
                    position = len(decoded)
                    self.assertTrue(all(reference() is None
                                        for reference in references[:position]),
                                    "Consumed raw JSON remains retained during later hydration")
                    decoded.append(position)
                    return original_loads(raw, *args, **kwargs)

                with mock.patch.object(shows.json, "loads", new=observe_loads):
                    if reader == "packet":
                        loaded = shows._load_finalized_show_ledgers(connection, guild_id=77)
                        self.assertEqual([item["showKey"] for item in loaded],
                                         [documents[0]["showKey"], documents[3]["showKey"]])
                    else:
                        with mock.patch.object(shows.os.path, "exists", return_value=True):
                            with mock.patch.object(shows.sqlite3, "connect", return_value=connection):
                                self.assertEqual(shows.build_tiktok_show_evidence_context(
                                    "synthetic-only.db", guild_id=77, user_text="What's up?",
                                ), "")
                        self.assertTrue(connection.closed)
                self.assertEqual(decoded, [0, 1, 2, 3, 4])
                self.assertTrue(all(reference() is None for reference in references))
