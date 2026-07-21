import io
import os
os.environ.setdefault("GEMINI_API_KEY", "test-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-token")
import json
import sqlite3
import tempfile
import urllib.error
import unittest
from unittest.mock import AsyncMock, Mock, patch

import bnl_journal as j


def article_json(packet, title="A New Coil in the BARCODE Room", leak=""):
    ref = packet["safeSources"][0]["refId"]
    body = " ".join(["BNL watches the public music room fold a fresh rhythm into community mischief and careful BARCODE lore." for _ in range(27)])
    body += leak
    return json.dumps({
        "title": title,
        "excerpt": "BNL connects fresh public music chatter with older harmless echoes while keeping the people unnamed.",
        "sections": [{"heading": "Public Noise, Carefully Labeled", "body": body, "sourceRefIds": [ref]}],
        "metadata": {"topicTags": ["music", "callback"], "subjectRefs": [], "continuityNotes": ["music callback"], "unresolvedQuestions": [], "confidenceFlags": ["grounded"], "safetyFlags": ["anonymous"]},
    })


class Resp:
    status = 200
    def __init__(self, body): self.body = body
    def read(self): return self.body
    def getcode(self): return self.status
    def __enter__(self): return self
    def __exit__(self, *a): return False


class JournalTests(unittest.TestCase):
    def setUp(self):
        self.clock = patch.object(j, "utc_now_iso", return_value="2026-07-18T01:00:00Z")
        self.clock.start()
        self.tmp = tempfile.NamedTemporaryFile(delete=False); self.db = self.tmp.name; self.tmp.close(); j.ensure_schema(self.db)
        with sqlite3.connect(self.db) as c:
            c.execute("CREATE TABLE website_relay_history(relay_id TEXT PRIMARY KEY,guild_id INTEGER,public_message TEXT,public_directive TEXT,mode TEXT,relay_lane TEXT,event_type TEXT,highest_source_conversation_id INTEGER,normalized_message TEXT,semantic_family TEXT,published_timestamp TEXT)")
            c.execute("CREATE TABLE conversations(id INTEGER PRIMARY KEY,user_id INTEGER,user_name TEXT,guild_id INTEGER,channel_name TEXT,channel_policy TEXT,role TEXT,content TEXT,timestamp TEXT,public_usable INTEGER,visibility TEXT)")
            c.execute("INSERT INTO website_relay_history VALUES (?,?,?,?,?,?,?,?,?,?,?)", ('r1', 1, 'A track was discussed.', 'Keep listening.', 'OBS', 'lane', 'accepted', 1, 'a track', 'public', '2026-07-18T00:00:00Z'))
            rows = [
                (1, 7, 'KnownUser', 1, 'general', 'public_home', 'user', 'KnownUser says people discuss bass chaos and a hook', '2026-07-18T00:01:00Z', 1, 'public_safe'),
                (2, 8, 'Private', 1, 'mods', 'internal_controlled', 'user', 'secret internal', '2026-07-18T00:02:00Z', 1, 'public_safe'),
                (3, 9, 'Nope', 1, 'general', 'public_home', 'model', 'bot text', '2026-07-18T00:03:00Z', 1, 'public_safe'),
                (4, 10, 'Nope2', 1, 'general', 'public_home', 'user', 'not usable', '2026-07-18T00:04:00Z', 0, 'public_safe'),
                (5, 11, 'OtherUser', 1, 'general', 'public_home', 'user', 'OtherUser mentions synth sparks and chorus plans', '2026-07-18T00:05:00Z', 1, 'public_safe'),
            ]
            c.executemany("INSERT INTO conversations VALUES (?,?,?,?,?,?,?,?,?,?,?)", rows)
            c.execute("CREATE TABLE relationship_journal(id INTEGER,user_id INTEGER,guild_id INTEGER,entry_type TEXT,summary TEXT,timestamp TEXT)")
            c.execute("INSERT INTO relationship_journal VALUES (1,7,1,'state','private relationship dirt','2026-07-18T00:00:00Z')")

    def tearDown(self):
        self.clock.stop()

    def packet(self):
        return j.build_source_packet(self.db, 1, 24, '2026-07-18T01:00:00Z')

    def test_source_policy_public_usable_user_authored_and_anonymized(self):
        packet = self.packet()
        self.assertEqual(len([s for s in packet['privateSources'] if s['sourceKind'] == 'conversation']), 2)
        prompt = j.build_generation_prompt(packet)
        self.assertIn('"sourceKind": "conversation"', prompt)
        self.assertNotIn('KnownUser', prompt)
        self.assertNotIn('discord_user:7', prompt)
        self.assertNotIn('relationship_journal', json.dumps(packet))
        self.assertNotIn('secret internal', json.dumps(packet))
        self.assertIn('community chronicle', prompt)
        self.assertIn('Never call people entities or organisms', prompt)
        self.assertIn('Do not invent nicknames', prompt)
        self.assertIn('Juicy means lively pattern recognition', prompt)

    def test_no_hard_coded_fallback_and_malformed_repair_no_rows(self):
        calls = []
        def gen(packet, prompt):
            calls.append(prompt); return 'not-json'
        res = j.generate_and_store_draft(self.db, 1, 24, gen)
        self.assertFalse(res.ok); self.assertEqual(len(calls), j.JOURNAL_GENERATION_ATTEMPTS)
        with sqlite3.connect(self.db) as c:
            self.assertEqual(c.execute("SELECT COUNT(*) FROM bnl_journal_entries").fetchone()[0], 0)

    def test_public_safe_wording_overlap_is_not_a_publish_blocker(self):
        calls = []
        copied = ' people discuss bass chaos and a hook'

        def gen(packet, prompt):
            calls.append(prompt)
            return article_json(packet, title='Public Wording Pass', leak=copied)

        packet = self.packet()
        article = j.parse_generated_json(article_json(packet, title='Guard Check', leak=copied))
        self.assertEqual('', j.validate_article(article, packet, []))

        res = j.generate_and_store_draft(self.db, 1, 24, gen)
        self.assertTrue(res.ok, res.reason)
        self.assertEqual(1, len(calls))
        self.assertIn('Use a direct quote only rarely', calls[0])
        with sqlite3.connect(self.db) as c:
            self.assertEqual(
                1,
                c.execute("SELECT COUNT(*) FROM bnl_journal_entries WHERE lifecycle_state='draft'").fetchone()[0],
            )

    def test_clinical_or_sensitive_copy_gets_rejected_and_rewritten(self):
        packet = self.packet()
        clinical = j.parse_generated_json(article_json(packet, title='Clinical Pass', leak=' Records indicate continuous effort across entities.'))
        sensitive = j.parse_generated_json(article_json(packet, title='Sensitive Pass', leak=' Interpersonal conflicts among juveniles continue.'))
        self.assertEqual('overly_clinical_voice', j.validate_article(clinical, packet, []))
        self.assertEqual('sensitive_personal_detail', j.validate_article(sensitive, packet, []))

        calls = []
        def gen(packet, prompt):
            calls.append(prompt)
            if len(calls) == 1:
                return article_json(packet, title='Clinical First Pass', leak=' Records indicate continuous effort across entities.')
            return article_json(packet, title='Lively Rewritten Pass')

        res = j.generate_and_store_draft(self.db, 1, 24, gen)
        self.assertTrue(res.ok, res.reason)
        self.assertEqual(2, len(calls))
        self.assertIn('overly_clinical_voice', calls[1])
        self.assertIn('lively, concrete community chronicle', calls[1])

    def test_editorial_polish_cannot_cancel_a_blocking_clean_entry(self):
        calls = []

        def gen(packet, prompt):
            calls.append(prompt)
            return article_json(
                packet,
                title='Persistent Clinical Voice',
                leak=' Records indicate continuous effort across entities.',
            )

        result = j.generate_and_store_draft(self.db, 1, 24, gen)
        self.assertTrue(result.ok, result.reason)
        self.assertEqual(j.JOURNAL_GENERATION_ATTEMPTS, len(calls))
        with sqlite3.connect(self.db) as conn:
            self.assertEqual(
                [('draft', 'Persistent Clinical Voice')],
                conn.execute(
                    "SELECT lifecycle_state,title FROM bnl_journal_entries"
                ).fetchall(),
            )

    def test_retained_publishable_entry_survives_later_provider_failure(self):
        calls = []

        def gen(packet, prompt):
            calls.append(prompt)
            if len(calls) == 1:
                return article_json(
                    packet,
                    title='Retained Safe Candidate',
                    leak=' Records indicate continuous effort across entities.',
                )
            raise RuntimeError('provider down during polish')

        result = j.generate_and_store_draft(self.db, 1, 24, gen)
        self.assertTrue(result.ok, result.reason)
        self.assertEqual(2, len(calls))

    def test_advisory_style_never_hides_a_blocking_context_failure(self):
        def gen(packet, prompt):
            article = json.loads(article_json(
                packet,
                title='Clinical But Unsupported',
                leak=' Records indicate continuous effort across entities.',
            ))
            article['metadata']['contextUses'] = [{
                'laneType': 'bnl_inference',
                'laneRefId': 'inference:not-supplied',
                'sectionHeading': 'Public Noise, Carefully Labeled',
                'claim': 'I think this proves a larger plan.',
                'basisRefIds': [packet['safeSources'][0]['refId']],
            }]
            return json.dumps(article)

        result = j.generate_and_store_draft(self.db, 1, 24, gen)
        self.assertFalse(result.ok)
        self.assertEqual('invalid_context_use', result.reason)
        with sqlite3.connect(self.db) as conn:
            self.assertEqual(0, conn.execute("SELECT COUNT(*) FROM bnl_journal_entries").fetchone()[0])

    def test_source_specific_generated_json_creates_draft(self):
        def gen(packet, prompt): return article_json(packet)
        res = j.generate_and_store_draft(self.db, 1, 24, gen)
        self.assertTrue(res.ok, res.reason)
        with sqlite3.connect(self.db) as c:
            self.assertEqual(c.execute("SELECT COUNT(*) FROM bnl_journal_entries WHERE lifecycle_state='draft'").fetchone()[0], 1)

    def test_complete_history_exact_subject_ties_recent_and_rejected_excluded(self):
        with sqlite3.connect(self.db) as c:
            for i in range(12):
                state = 'published' if i != 10 else 'rejected'; eid = f'e{i}'
                meta = {"topicTags": ["bass"], "continuityNotes": [f'note{i}'], "subjectRefs": ["discord_user:7"]}
                c.execute("INSERT INTO bnl_journal_entries VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (eid, 1, 1, state, f'Title {i}', 'Excerpt', '[]', '{}', b'{}', f'h{i}', 's', 'e', '2026', None, f'2026-07-{i+1:02d}T00:00:00Z' if state == 'published' else None, None, None, 0, f'2026-07-{i+1:02d}T00:00:00Z', f'2026-07-{i+1:02d}T00:00:00Z'))
                c.execute("INSERT INTO bnl_journal_private_metadata VALUES (?,?,?,?,?,?,?,?)", (eid, 1, 1, json.dumps(meta), f'h{i}', state, '2026', '2026'))
        hist = j.retrieve_history(self.db, 1, {"safeSources": [{"summary": "bass"}], "privateSources": [{"subjectRef": "discord_user:7"}], "candidateTopicTags": ["bass"]})
        self.assertEqual(hist['previousEntry']['entry_id'], 'e11')
        self.assertEqual(hist['relevantOlderEntries'][0]['entry_id'], 'e9')
        self.assertEqual(hist['recurringTopicCounts']['bass'], 11)
        self.assertFalse(any(r['entry_id'] == 'e10' for r in hist['relevantOlderEntries']))

    def test_validation_rejects_names_refs_urls_mentions_and_ids(self):
        packet = self.packet(); good = j.parse_generated_json(article_json(packet))
        self.assertEqual('', j.validate_article(good, packet, []))
        for leak, reason in [(' KnownUser', 'community_name_leak'), (' fresh:101', 'source_ref_leak'), (' https://x.test', 'public_leak_pattern'), (' @KnownUser', 'public_leak_pattern'), (' 1234567890123', 'public_leak_pattern')]:
            bad = j.parse_generated_json(article_json(packet, title='Unique ' + reason, leak=leak))
            self.assertEqual(reason, j.validate_article(bad, packet, []), leak)

    def test_direct_quotes_and_curly_apostrophes_do_not_hold_generation(self):
        quote = '“people discuss bass chaos and a hook”'
        packet = self.packet()
        article = j.parse_generated_json(
            article_json(packet, title='The Room’s Quoted Line', leak=f' {quote}')
        )
        self.assertEqual('', j.validate_article(article, packet, []))

        calls = []

        def gen(source_packet, prompt):
            calls.append(prompt)
            return article_json(source_packet, title='The Room’s Quoted Line', leak=f' {quote}')

        result = j.generate_and_store_draft(self.db, 1, 24, gen)
        self.assertTrue(result.ok, result.reason)
        self.assertEqual(1, len(calls))
        self.assertIn('Use a direct quote only rarely', calls[0])
        self.assertNotIn('Do not include direct quotes', calls[0])

    def test_approved_entry_cannot_be_rejected_and_tables_remain_approved(self):
        res = j.generate_and_store_draft(self.db, 1, 24, lambda p, pr: article_json(p)); self.assertTrue(res.ok)
        self.assertTrue(j.approve_draft(self.db, 1, res.entry_id, res.content_hash).ok)
        rej = j.reject_draft(self.db, 1, res.entry_id, 'no')
        self.assertFalse(rej.ok); self.assertEqual(rej.reason, 'not_draft')
        with sqlite3.connect(self.db) as c:
            self.assertEqual(c.execute("SELECT lifecycle_state FROM bnl_journal_entries WHERE entry_id=?", (res.entry_id,)).fetchone()[0], 'approved_pending_delivery')
            self.assertEqual(c.execute("SELECT lifecycle_state FROM bnl_journal_private_metadata WHERE entry_id=?", (res.entry_id,)).fetchone()[0], 'approved_pending_delivery')

    def test_regeneration_next_revision_preserves_previous_content_and_syncs_tables(self):
        res = j.generate_and_store_draft(self.db, 1, 24, lambda p, pr: article_json(p, 'First Title')); self.assertTrue(res.ok)
        regen = j.regenerate_draft(self.db, 1, res.entry_id, 24, lambda p, pr: article_json(p, 'Second Title'))
        self.assertTrue(regen.ok, regen.reason); self.assertEqual(regen.revision, 2)
        with sqlite3.connect(self.db) as c:
            rows = c.execute("SELECT revision,lifecycle_state,title FROM bnl_journal_entries WHERE entry_id=? ORDER BY revision", (res.entry_id,)).fetchall()
            metas = c.execute("SELECT revision,lifecycle_state FROM bnl_journal_private_metadata WHERE entry_id=? ORDER BY revision", (res.entry_id,)).fetchall()
        self.assertEqual(rows, [(1, 'rejected', 'First Title'), (2, 'draft', 'Second Title')])
        self.assertEqual(metas, [(1, 'rejected'), (2, 'draft')])

    def test_editorial_polish_cannot_cancel_regeneration(self):
        first = j.generate_and_store_draft(
            self.db,
            1,
            24,
            lambda packet, prompt: article_json(packet, 'First Draft'),
        )
        self.assertTrue(first.ok, first.reason)
        calls = []

        def clinical_regeneration(packet, prompt):
            calls.append(prompt)
            return article_json(
                packet,
                'Regenerated Clinical Draft',
                ' Records indicate continuous effort across entities.',
            )

        regenerated = j.regenerate_draft(
            self.db,
            1,
            first.entry_id,
            24,
            clinical_regeneration,
        )

        self.assertTrue(regenerated.ok, regenerated.reason)
        self.assertEqual(2, regenerated.revision)
        self.assertEqual(j.JOURNAL_GENERATION_ATTEMPTS, len(calls))
        with sqlite3.connect(self.db) as conn:
            self.assertEqual(
                [(1, 'rejected'), (2, 'draft')],
                conn.execute(
                    "SELECT revision,lifecycle_state FROM bnl_journal_entries "
                    "WHERE entry_id=? ORDER BY revision",
                    (first.entry_id,),
                ).fetchall(),
            )


    def test_provider_exception_create_no_rows_and_regen_preserves_old(self):
        def down(packet, prompt): raise RuntimeError('provider down')
        res = j.generate_and_store_draft(self.db, 1, 24, down)
        self.assertFalse(res.ok); self.assertEqual(res.reason, 'provider_failure')
        with sqlite3.connect(self.db) as c:
            self.assertEqual(c.execute("SELECT COUNT(*) FROM bnl_journal_entries").fetchone()[0], 0)
        old = j.generate_and_store_draft(self.db, 1, 24, lambda p, pr: article_json(p, 'Old Draft')); self.assertTrue(old.ok)
        regen = j.regenerate_draft(self.db, 1, old.entry_id, 24, down)
        self.assertFalse(regen.ok); self.assertEqual(regen.reason, 'provider_failure')
        with sqlite3.connect(self.db) as c:
            self.assertEqual(c.execute("SELECT revision,lifecycle_state,title FROM bnl_journal_entries WHERE entry_id=?", (old.entry_id,)).fetchall(), [(1, 'draft', 'Old Draft')])

    def test_cited_sources_only_private_metadata(self):
        packet = self.packet()
        relay_article = j.parse_generated_json(article_json(packet, 'Relay Only'))
        relay_article['sourceRefIds'] = {'Public Noise, Carefully Labeled': ['fresh:1']}
        res = j.store_validated_draft(self.db, 1, packet, relay_article); self.assertTrue(res.ok, res.reason)
        with sqlite3.connect(self.db) as c:
            meta = json.loads(c.execute("SELECT metadata_json FROM bnl_journal_private_metadata WHERE entry_id=?", (res.entry_id,)).fetchone()[0])
        self.assertEqual(meta['supportingRelayIds'], ['r1'])
        self.assertEqual(meta['supportingConversationRefs'], [])
        self.assertEqual(meta['subjectRefs'], [])
        conv = next(src for src in packet['privateSources'] if src.get('displayName') == 'KnownUser')
        conv_article = j.parse_generated_json(article_json(packet, 'One Person Only'))
        conv_article['sourceRefIds'] = {'Public Noise, Carefully Labeled': [conv['refId']]}
        res2 = j.store_validated_draft(self.db, 1, packet, conv_article); self.assertTrue(res2.ok, res2.reason)
        with sqlite3.connect(self.db) as c:
            meta2 = json.loads(c.execute("SELECT metadata_json FROM bnl_journal_private_metadata WHERE entry_id=?", (res2.entry_id,)).fetchone()[0])
        self.assertEqual(meta2['supportingRelayIds'], [])
        self.assertEqual([r['subjectRef'] for r in meta2['supportingConversationRefs']], ['discord_user:7'])
        self.assertEqual(meta2['subjectRefs'], ['discord_user:7'])

    def test_regeneration_transaction_rollback_on_insert_failure(self):
        res = j.generate_and_store_draft(self.db, 1, 24, lambda p, pr: article_json(p, 'Atomic Old')); self.assertTrue(res.ok)
        with sqlite3.connect(self.db) as c:
            c.execute("CREATE TRIGGER fail_journal_revision_two BEFORE INSERT ON bnl_journal_entries WHEN NEW.revision=2 BEGIN SELECT RAISE(ABORT, 'forced'); END;")
        with self.assertRaises(sqlite3.IntegrityError):
            j.regenerate_draft(self.db, 1, res.entry_id, 24, lambda p, pr: article_json(p, 'Atomic New'))
        with sqlite3.connect(self.db) as c:
            self.assertEqual(c.execute("SELECT revision,lifecycle_state,title FROM bnl_journal_entries WHERE entry_id=?", (res.entry_id,)).fetchall(), [(1, 'draft', 'Atomic Old')])
            self.assertEqual(c.execute("SELECT revision,lifecycle_state FROM bnl_journal_private_metadata WHERE entry_id=?", (res.entry_id,)).fetchall(), [(1, 'draft')])

    def test_empty_fields_duplicate_headings_and_arbitrary_fresh_leak(self):
        packet = self.packet(); base = j.parse_generated_json(article_json(packet))
        bad = dict(base); bad['title'] = ''; self.assertEqual('empty_required_field', j.validate_article(bad, packet, []))
        bad = j.parse_generated_json(article_json(packet)); bad['sections'][0]['heading'] = ''; self.assertEqual('empty_required_field', j.validate_article(bad, packet, []))
        bad = j.parse_generated_json(article_json(packet)); bad['sections'].append(dict(bad['sections'][0])); self.assertEqual('duplicate_section_heading', j.validate_article(bad, packet, []))
        bad = j.parse_generated_json(article_json(packet, 'Fresh Leak', ' fresh:999')); self.assertEqual('source_ref_leak', j.validate_article(bad, packet, []))

    def test_public_payload_private_metadata_exact_bytes_idempotency_and_404(self):
        res = j.generate_and_store_draft(self.db, 1, 24, lambda p, pr: article_json(p)); self.assertTrue(res.ok)
        with sqlite3.connect(self.db) as c:
            payload = json.loads(c.execute("SELECT public_payload_json FROM bnl_journal_entries").fetchone()[0]); meta = c.execute("SELECT metadata_json FROM bnl_journal_private_metadata").fetchone()[0]
        self.assertNotIn('supportingConversationRefs', json.dumps(payload)); self.assertIn('supportingConversationRefs', meta)
        self.assertFalse(j.approve_draft(self.db, 1, res.entry_id, 'bad').ok)
        self.assertTrue(j.approve_draft(self.db, 1, res.entry_id, res.content_hash).ok)
        captured = []
        def opener(req, timeout=10):
            captured.append(req.data); submitted = json.loads(req.data.decode())['entry']
            return Resp(json.dumps({"ok": True, "persisted": True, "idempotent": True, "entry": {"entryId": submitted['entryId'], "revision": submitted['revision'], "contentHash": submitted['contentHash'], "publishedAt": "2026-07-18T01:00:00Z"}}).encode())
        d = j.deliver_approved(self.db, 1, res.entry_id, 'https://site.example', 'k', opener); self.assertTrue(d.ok and d.idempotent)
        with sqlite3.connect(self.db) as c:
            c.execute("UPDATE bnl_journal_entries SET lifecycle_state='delivery_failed' WHERE entry_id=?", (res.entry_id,))
            c.execute("UPDATE bnl_journal_private_metadata SET lifecycle_state='delivery_failed' WHERE entry_id=?", (res.entry_id,))
        j.deliver_approved(self.db, 1, res.entry_id, 'https://site.example', 'k', opener)
        self.assertEqual(captured[0], captured[1])
        res2 = j.generate_and_store_draft(self.db, 1, 24, lambda p, pr: article_json(p, '404 Title')); j.approve_draft(self.db, 1, res2.entry_id, res2.content_hash)
        def missing(req, timeout=10): raise urllib.error.HTTPError(req.full_url, 404, 'not found', {}, io.BytesIO())
        d2 = j.deliver_approved(self.db, 1, res2.entry_id, 'https://site.example', 'k', missing)
        self.assertFalse(d2.ok); self.assertEqual(d2.reason, 'endpoint_not_found')

    def test_rehydrate_replays_exact_published_payload_without_regeneration(self):
        res = j.generate_and_store_draft(self.db, 1, 24, lambda p, pr: article_json(p, 'Restore Me')); self.assertTrue(res.ok)
        self.assertTrue(j.approve_draft(self.db, 1, res.entry_id, res.content_hash).ok)
        payloads = []
        def opener(req, timeout=10):
            payloads.append(req.data)
            submitted = json.loads(req.data.decode())['entry']
            return Resp(json.dumps({"ok": True, "persisted": True, "idempotent": False, "entry": {"entryId": submitted['entryId'], "revision": submitted['revision'], "contentHash": submitted['contentHash'], "publishedAt": "2026-07-18T01:00:00Z"}}).encode())
        self.assertTrue(j.deliver_approved(self.db, 1, res.entry_id, 'https://site.example', 'k', opener).ok)
        restored = j.rehydrate_published_entries(self.db, 1, 'https://site.example', 'k', opener=opener)
        self.assertTrue(restored['ok'], restored)
        self.assertEqual(1, restored['restored'])
        self.assertEqual(payloads[0], payloads[1])
        with sqlite3.connect(self.db) as c:
            self.assertEqual('published', c.execute("SELECT lifecycle_state FROM bnl_journal_entries WHERE entry_id=?", (res.entry_id,)).fetchone()[0])


class BotJournalCommandTests(unittest.IsolatedAsyncioTestCase):
    async def test_invalid_hours_and_dm_fail_safely(self):
        import bnl01_bot
        self.assertEqual(72, bnl01_bot._parse_journal_hours('bad'))
        msg = Mock(); msg.guild = None; msg.reply = AsyncMock(); msg.author = Mock(id=1)
        handled = await bnl01_bot.maybe_handle_journal_command(msg, '!bnl journal create | hours=bad')
        self.assertTrue(handled); msg.reply.assert_awaited()


    async def test_quota_exhaustion_does_not_call_gemini(self):
        import bnl01_bot
        with patch.object(bnl01_bot, 'check_quota_availability', return_value=False), patch.object(bnl01_bot, '_generate_gemini_content_with_fallback') as gemini:
            with self.assertRaises(RuntimeError):
                bnl01_bot._generate_journal_json_sync({}, 'prompt')
            gemini.assert_not_called()

    async def test_command_exception_boundary_replies_safely(self):
        import bnl01_bot
        old = bnl01_bot.DB_FILE; bnl01_bot.DB_FILE = self._make_db()
        try:
            msg = Mock(); msg.guild = Mock(id=1, get_member=Mock(return_value=Mock())); msg.author = Mock(id=1); msg.channel = Mock(name='research-and-development'); msg.reply = AsyncMock()
            with patch.object(bnl01_bot, 'resolve_channel_policy', return_value='internal_controlled'), patch.object(bnl01_bot, 'can_send_dossier_recommendation', return_value=True), patch.object(bnl01_bot, 'generate_and_store_journal_draft', side_effect=RuntimeError('boom')):
                handled = await bnl01_bot.maybe_handle_journal_command(msg, '!bnl journal create | hours=72')
            self.assertTrue(handled)
            self.assertIn('failed safely', msg.reply.await_args.args[0])
        finally:
            bnl01_bot.DB_FILE = old

    async def test_operator_create_path_with_mocked_gemini_creates_draft(self):
        import bnl01_bot
        old = bnl01_bot.DB_FILE; bnl01_bot.DB_FILE = self._make_db()
        try:
            packet = j.build_source_packet(bnl01_bot.DB_FILE, 1, 24, '2026-07-18T01:00:00Z')
            response = Mock(candidates=[Mock(content=Mock(parts=[Mock(text=article_json(packet))]))], usage_metadata=Mock(total_token_count=None))
            msg = Mock(); msg.guild = Mock(id=1, get_member=Mock(return_value=Mock())); msg.author = Mock(id=1); msg.channel = Mock(name='research-and-development'); msg.reply = AsyncMock()
            with patch.object(j, 'utc_now_iso', return_value='2026-07-18T01:00:00Z'), patch.object(bnl01_bot, 'resolve_channel_policy', return_value='internal_controlled'), patch.object(bnl01_bot, 'can_send_dossier_recommendation', return_value=True), patch.object(bnl01_bot, 'check_quota_availability', return_value=True), patch.object(bnl01_bot, '_generate_gemini_content_with_fallback', return_value=response):
                handled = await bnl01_bot.maybe_handle_journal_command(msg, '!bnl journal create | hours=bad')
            self.assertTrue(handled)
            with sqlite3.connect(bnl01_bot.DB_FILE) as c:
                self.assertEqual(c.execute("SELECT COUNT(*) FROM bnl_journal_entries").fetchone()[0], 1)
        finally:
            bnl01_bot.DB_FILE = old

    def _make_db(self):
        tmp = tempfile.NamedTemporaryFile(delete=False); path = tmp.name; tmp.close(); j.ensure_schema(path)
        with sqlite3.connect(path) as c:
            c.execute("CREATE TABLE website_relay_history(relay_id TEXT PRIMARY KEY,guild_id INTEGER,public_message TEXT,public_directive TEXT,mode TEXT,relay_lane TEXT,event_type TEXT,highest_source_conversation_id INTEGER,normalized_message TEXT,semantic_family TEXT,published_timestamp TEXT)")
            c.execute("CREATE TABLE conversations(id INTEGER PRIMARY KEY,user_id INTEGER,user_name TEXT,guild_id INTEGER,channel_name TEXT,channel_policy TEXT,role TEXT,content TEXT,timestamp TEXT,public_usable INTEGER,visibility TEXT)")
            c.execute("INSERT INTO website_relay_history VALUES (?,?,?,?,?,?,?,?,?,?,?)", ('r1', 1, 'Track chatter is lively.', 'Watch the hook discussion.', 'OBS', 'lane', 'accepted', 1, 'track', 'public', '2026-07-18T00:00:00Z'))
            c.execute("INSERT INTO conversations VALUES (?,?,?,?,?,?,?,?,?,?,?)", (1, 7, 'KnownUser', 1, 'general', 'public_home', 'user', 'bass chaos and hook talk', '2026-07-18T00:01:00Z', 1, 'public_safe'))
        return path


if __name__ == '__main__': unittest.main()
