"""Complete show-owner parity while lowering canonical serialization peak.

The old hash/equality expressions remain independent primitive oracles. All
reader, authorization, admission, renderer and seal bodies are the real owners;
these tests do not load artifact AST, fake SQL, or call a provider.
"""
from __future__ import annotations
from contextlib import closing
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import tracemalloc
import unittest
from unittest import mock
import bnl_tiktok_show_ledger as shows
import test_show_retrieval_scope as scope_fixture

def native_digest(value):
    return hashlib.sha256(shows._canonical_json(value).encode('utf-8')).hexdigest()

def native_equal(left, right):
    return shows._canonical_json(left) == shows._canonical_json(right)

def outcome(function, *args, **kwargs):
    try:
        return 'return', function(*args, **kwargs)
    except Exception as exc:
        return 'error', type(exc).__name__

def prior_outcome(function, *args, **kwargs):
    with mock.patch.object(shows, '_canonical_digest', native_digest), mock.patch.object(shows, '_canonical_equal', native_equal):
        return outcome(function, *args, **kwargs)


class CanonicalPrimitiveTests(unittest.TestCase):
    def test_native_canonical_bytes_digest_and_error_classes(self):
        cyclic = []
        cyclic.append(cyclic)
        cross = {'messages': []}
        cross['messages'].append(cross)
        deep = 0
        for _ in range(990):
            deep = [deep]
        cases = [None, True, False, 0, -1, 10**70, -0.0, 1.25, 1e-40,
                 float('nan'), float('inf'), float('-inf'), 'neutral\x00\n🙂é',
                 {'z': [1, None, True], 'a': {'neutral': '🙂\x00'}},
                 {'repeat': ['x' * 300000] * 3}, [(), (1, 2)], {1: 'a', 2: 'b'},
                 {1: 'mixed', 'a': 'mixed'}, b'unsupported', object(), '\ud800', cyclic, deep,
                 {}, {'empty': []}, {'messages': [{'text': '🙂é\x00' * 50, 'ordinal': n}
                                               for n in range(129)]},
                 {'z': {}, 'x': (), 'a': [None] * 64}, cross,
                 {'a': object(), 'b': cyclic}, {'a': cyclic, 'b': object()},
                 {'a': {'sort': {1: 'a', 'b': 'b'}}, 'messages': [1]},
                 {'messages': [float('nan'), float('inf'), float('-inf'), -0.0] * 65},
                 {'nested': {'messages': ['neutral'] * 130}, 'messages': ['neutral'] * 130}]
        for ordinal, value in enumerate(cases):
            with self.subTest(ordinal=ordinal):
                expected = outcome(native_digest, value)
                self.assertEqual(outcome(shows._canonical_digest, value), expected)
                if expected[0] == 'return':
                    self.assertEqual(''.join(shows._canonical_json_chunks(value)).encode('utf-8'),
                                     shows._canonical_json(value).encode('utf-8'))

    def test_exact_canonical_equality_types_errors_and_chunk_boundaries(self):
        cyclic = []
        cyclic.append(cyclic)
        pairs = [(None, None), (False, 0), (True, 1), (1, 1.0), (0.0, -0.0),
                 (float('nan'), float('nan')), (float('inf'), float('inf')),
                 ([1, None, 'é🙂\x00'], (1, None, 'é🙂\x00')),
                 ({'z': [1, 2], 'a': 'é🙂'}, {'a': 'é🙂', 'z': [1, 2]}),
                 ({'a': [1] * 65}, {'a': tuple([1] * 65)}),
                 ({'a': ['neutral' * 1000] * 129}, {'a': ['neutral' * 1000] * 129}),
                 ({'a': 1, 'z': object()}, {'a': 2}),
                 ({'a': 1, 'z': cyclic}, {'a': 2, 'z': object()}),
                 ({'a': 1, 'z': object()}, {'a': 1, 'b': cyclic}),
                 ({'a': 1, 'z': cyclic}, {'a': 1, 'b': object()}),
                 ({1: 'a', 'a': 'b'}, {'a': 1}),
                 ({'a': 1}, {'a': 1, 'z': object()}),
                 ('\ud800', '\ud800'), ('', 'a'), ({}, {}), ([], [])]
        for depth in (990, 995, 997, 998, 999, 1000):
            value = 0
            for _ in range(depth):
                value = [value]
            pairs.append(({'a': value}, {'a': value}))
        for ordinal, (left, right) in enumerate(pairs):
            with self.subTest(ordinal=ordinal):
                self.assertEqual(outcome(shows._canonical_equal, left, right),
                                 outcome(native_equal, left, right))
        def partitions(value):
            text, width = value
            for start in range(0, len(text), width):
                yield text[start:start + width]
        with mock.patch.object(shows, '_canonical_json_chunks', side_effect=partitions):
            for text in ('', 'a', 'é🙂\x00' * 70, '{"a":[1,2,3]}'):
                self.assertTrue(shows._canonical_equal((text, 1), (text, 7)))
                self.assertFalse(shows._canonical_equal((text + 'x', 3), (text + 'y', 7)))

    def test_unicode_message_digest_avoids_whole_document_allocation(self):
        value = {'schemaVersion': 'neutral', 'messages': [
            {'text': ('🙂' if ordinal == 0 else '') + 'x' * 8192, 'ordinal': ordinal}
            for ordinal in range(512)]}
        def peak(function):
            tracemalloc.start()
            try:
                result = function(value)
                _retained, allocated = tracemalloc.get_traced_memory()
            finally:
                tracemalloc.stop()
            return result, allocated
        expected, old_peak = peak(native_digest)
        actual, new_peak = peak(shows._canonical_digest)
        self.assertEqual(actual, expected)
        self.assertLess(new_peak, old_peak * 0.75)
        original_encoder = shows._canonical_json
        def reject_whole_document(item):
            self.assertIsNot(item, value, 'Whole document serialization defeats the working-set reduction.')
            if type(item) is list:
                self.assertLessEqual(len(item), 64)
            return original_encoder(item)
        with mock.patch.object(shows, '_canonical_json', side_effect=reject_whole_document):
            self.assertEqual(shows._canonical_digest(value), expected)

def reseal(document):
    document.pop('sourceDigest', None)
    document['sourceDigest'] = hashlib.sha256(shows._canonical_json(document).encode('utf-8')).hexdigest()
    return document

class ShowCanonicalOwnerTests(unittest.TestCase):

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.db = str(Path(self.directory.name) / 'neutral.db')
        self.fixture = scope_fixture.ShowReadHydrationTests()
        self.documents = []
        dates = ('2025-01-03', '2026-01-03', '2026-08-28', '2026-09-29', '2026-10-01', '2026-10-01')
        for index, day in enumerate(dates):
            document = self.fixture.document(index)
            document.update(showDate=day, showTitle='Neutral episode %s' % index)
            subject = 'tiktok_user:fixture' if index % 2 else 'tiktok_user:cedar'
            label = 'Fixture Viewer' if index % 2 else 'Cedar Vale'
            document['participants'] = [dict(subjectRef=subject, speakerLabel=label, displayName=label, handle='fixture.viewer' if index % 2 else 'cedar.vale', artistAttributions=[{'artistName': 'Copper Orchard', 'unused': ['ignore']}], authoredEventIds=['unused-large-scope-%s' % n for n in range(80)], trackMoments=[{'unused': 'not a scope input'}])]
            document['messages'][0].update(subjectRef=subject, speakerLabel=label, text='The copper lantern flickers beside the river. Radio clock %s.' % index)
            document['discordParticipants'] = [dict(subjectRef='discord_user:22', speakerLabel='Test Member', displayName='Test Member', handle='test.member')]
            document['discordInteractions'] = [dict(subjectRef='discord_user:22', speakerLabel='Test Member', userMessages=[dict(conversationRowId=100 + index, occurredAtMs=index * 100000, text='The velvet station has a ceramic antenna.', role='user')], bnlReply={'text': 'Historical model wording.'})]
            document['trackMoments'] = [{'trackLabel': 'Copper Orchard Signal', 'unused': [1] * 60}]
            document['trackRoster'] = [dict(trackLabel='Velvet Station', projectLabel='Orchard', title='Ceramic Antenna', submittedByTikTokHandle='cedar.vale')]
            document['showTopics'] = [{'term': 'copper lantern'}]
            document['topics'] = [{'term': 'velvet station'}]
            document['operationalEvents'] = [dict(eventType='track_played', headline='Signal arrived', detail='Ceramic antenna on the north lane', trackLabel='Copper Orchard Signal', lane='north', outcome='played', occurredAtMs=index * 100000, eventId='op-%s' % index)]
            self.documents.append(reseal(document))
        with closing(sqlite3.connect(self.db)) as conn, conn:
            shows.ensure_tiktok_show_evidence_schema(conn)
            for document in self.documents:
                self.fixture.store(conn, document)

    def tearDown(self):
        self.directory.cleanup()

    def compare(self, **kwargs):
        baseline_selection, candidate_selection = ({}, {})
        with mock.patch.object(shows, '_canonical_digest', native_digest), mock.patch.object(shows, '_canonical_equal', native_equal):
            baseline = shows.build_tiktok_show_evidence_context(self.db, guild_id=77, selection_out=baseline_selection, **kwargs)
        counts = {'full_admission_calls': 0, 'admitted_documents': 0}
        safe_document = shows._safe_document

        def observed(value):
            counts['full_admission_calls'] += 1
            admitted = safe_document(value)
            counts['admitted_documents'] += admitted is not None
            return admitted
        with mock.patch.object(shows, '_safe_document', side_effect=observed):
            actual = shows.build_tiktok_show_evidence_context(self.db, guild_id=77, selection_out=candidate_selection, **kwargs)
        self.assertEqual(actual, baseline)
        self.assertEqual(candidate_selection, baseline_selection)
        return (baseline_selection, {'counters': counts})

    def test_request_scope_shape_matrix(self):
        cases = [
            {'user_text': "What's up?"},
            {'user_text': 'What do you want to be when you grow up?'},
            {'user_text': 'The copper lantern'},
            {'user_text': 'velvet ceramic antenna'},
            {'user_text': 'What did Cedar Vale say?'},
            {'user_text': 'What did Copper Orchard write?'},
            {'user_text': 'What did Fixture Viewer say during the show?'},
            {'user_text': 'Compare Cedar Vale and Fixture Viewer.'},
            {'user_text': 'Cedar Vale is not Fixture Viewer. What did Cedar Vale say?'},
            {'user_text': 'What did I say?', 'subject_user_id': 22},
            {'user_text': 'What did I say?', 'subject_user_id': 999},
            {'user_text': 'What was my public activity?', 'subject_user_id': 22},
            {'user_text': 'Recap the show.'},
            {'user_text': 'Summarize the community.'},
            {'user_text': 'Summarize show history across all shows.'},
            {'user_text': 'What happened in the last 3 shows?'},
            {'user_text': 'What happened on October 1, 2026?'},
            {'user_text': 'Compare October 1, 2026 and September 29, 2026 shows.'},
            {'user_text': 'What happened on January 3?'},
            {'user_text': 'What happened during the October 1, 2026 show and the previous show?'},
            {'user_text': 'What did Cedar Vale say?',
             'selection_user_text': 'Recap the January 3, 2025 show.', 'candidate_context': True},
            {'user_text': 'What happened?', 'selection_user_text': 'Recap September 29, 2026 show.',
             'candidate_context': True},
            {'user_text': 'Tell me about synthetic-show-4.'},
            {'user_text': 'copper lantern', 'pinned_show_keys': ('synthetic-show-2',)},
            {'user_text': 'What tracks were played during the show?'},
            {'user_text': 'Which topics stood out during the show?'},
            {'user_text': 'What time did the broadcast start?'},
            {'user_text': 'What happened around the ceramic antenna?'},
            {'user_text': 'How often did TikTok say "copper" during October 1, 2026 show?'},
            {'user_text': 'Did anyone say "copper lantern" on October 1, 2026?'},
            {'user_text': 'What was discussed preparing for October 1, 2026 show?'},
            {'user_text': 'What was said between 00:01 and 00:03 during October 1, 2026 show?'},
            {'user_text': 'What happened during the last 30 days?'},
            {'user_text': 'Recap the shows.', 'show_limit': 8, 'message_limit': 16},
        ]
        for ordinal, kwargs in enumerate(cases):
            with self.subTest(ordinal=ordinal):
                self.compare(**kwargs)
        self.shape_count = len(cases)

    def test_admission_invalid_documents_never_own_scope(self):
        invalid = deepcopy(self.documents[-1])
        invalid.update(showKey='invalid-scope', showDate='2027-01-01')
        invalid['participants'][0]['speakerLabel'] = 'Invalid Alias'
        with closing(sqlite3.connect(self.db)) as conn, conn:
            self.fixture.store(conn, invalid)
            malformed = deepcopy(self.documents[0])
            malformed['showKey'] = 'invalid-json'
            self.fixture.store(conn, malformed, raw_json='{')
            self.assertEqual(conn.execute('SELECT count(*) FROM tiktok_show_evidence_ledgers').fetchone()[0], 8)
        for query in ('What did Invalid Alias say?', 'What happened January 1, 2027?', 'Hi'):
            with self.subTest(query=query):
                _, receipt = self.compare(user_text=query)
                self.assertEqual(receipt['counters']['full_admission_calls'], 7)
                self.assertEqual(receipt['counters']['admitted_documents'], 6)

    def test_participant_order_and_topic_fallback_keep_context_selection(self):
        document = deepcopy(self.documents[3])
        document['showTopics'] = [None]
        document['participants'].append(deepcopy(document['participants'][0]))
        document['participants'].insert(0, None)
        document['discordParticipants'].append(deepcopy(document['discordParticipants'][0]))
        document = reseal(document)
        with closing(sqlite3.connect(self.db)) as conn, conn:
            conn.execute('UPDATE tiktok_show_evidence_ledgers SET ledger_json=?,source_digest=? WHERE show_key=?', (json.dumps(document), document['sourceDigest'], document['showKey']))
        for query in ('What did Fixture Viewer say?', 'Compare Test Member and Fixture Viewer.', 'velvet station'):
            with self.subTest(query=query):
                self.compare(user_text=query)

    def test_all_200_admissions_keep_existing_irrelevant_authored_hydration_skip(self):
        with closing(sqlite3.connect(self.db)) as conn, conn:
            conn.execute('DELETE FROM tiktok_show_evidence_ledgers')
            for index in range(200):
                self.fixture.store(conn, self.fixture.document(index))
        with mock.patch.object(shows, '_authored_show_messages', side_effect=AssertionError('irrelevant authored hydration')):
            selected, receipt = self.compare(user_text="What's up?")
        self.assertEqual(selected, {})
        self.assertEqual(receipt['counters']['full_admission_calls'], 200)
        self.assertEqual(receipt['counters']['admitted_documents'], 200)

    def test_current_image_scope_keeps_independent_dates_literals_and_refresh(self):
        first = shows.CurrentImageShowQuery(guild_id=77, channel_id=88, message_id=99, user_id=22, attachment_id=1, show_dates=('2026-08-28',), quote_literals=('The copper lantern',))
        second = shows.CurrentImageShowQuery(guild_id=77, channel_id=88, message_id=99, user_id=22, attachment_id=2, show_dates=('2026-09-29',), quote_literals=('The velvet station',))
        unknown = shows.CurrentImageShowQuery(guild_id=77, channel_id=88, message_id=99, user_id=22, attachment_id=3, quote_literals=('Unresolved screenshot text',))
        cases = (
            {'user_text': 'Read this screenshot. Verify against original show chat.',
             'image_queries': (first,), 'selection_user_text': 'Recap September 29, 2026.',
             'candidate_context': True},
            {'user_text': 'Verify this screenshot for September 29, 2026 show.',
             'image_queries': (first,)},
            {'user_text': 'Verify these screenshots against original show chat.',
             'image_queries': (first, second)},
            {'user_text': 'Verify these screenshots against original show chat.',
             'image_queries': (unknown, second)},
            {'user_text': 'Verify this screenshot against original show chat.',
             'image_queries': (unknown,)},
        )
        for ordinal, kwargs in enumerate(cases):
            with self.subTest(ordinal=ordinal):
                selected, _receipt = self.compare(**kwargs)
                if selected.get('source_refs'):
                    self.compare(user_text=kwargs['user_text'], image_queries=selected['image_queries'], selection_user_text=selected['selection_user_text'], pinned_show_keys=tuple((ref[0] for ref in selected['source_refs'])))

    def test_new_call_rechecks_body_authorization_edit_and_deletion(self):
        self.compare(user_text='copper lantern')
        changes = ('edit', 'privacy', 'tamper', 'delete')
        for change in changes:
            with self.subTest(change=change):
                document = deepcopy(self.documents[-1])
                if change == 'edit':
                    document['messages'][0]['text'] = 'A changed eligible ceramic antenna.'
                    document = reseal(document)
                elif change == 'privacy':
                    document['sourceAuthorization']['publicOnly'] = False
                    document = reseal(document)
                elif change == 'tamper':
                    document['messages'][0]['text'] = 'Changed without a valid digest.'
                with closing(sqlite3.connect(self.db)) as conn, conn:
                    conn.execute('DELETE FROM tiktok_show_evidence_ledgers WHERE show_key=?', (document['showKey'],))
                    if change != 'delete':
                        self.fixture.store(conn, document)
                self.compare(user_text='copper lantern', pinned_show_keys=(document['showKey'],))

    def test_full_safe_document_admission_field_and_corruption_parity(self):
        valid = deepcopy(self.documents[0])
        values = [None, [], {}, valid]
        for field, changed in (('schemaVersion', 'unsupported'), ('showKey', ''), ('sourceDigest', 'wrong'), ('messages', {}), ('participants', None), ('topics', None), ('trackMoments', None), ('trackRoster', None), ('operationalEvents', {}), ('discordInteractions', None), ('discordParticipants', None), ('showTopics', {})):
            value = deepcopy(valid)
            value[field] = changed
            values.append(value)
        value = deepcopy(valid)
        value['sourceAuthorization']['publicOnly'] = False
        values.append(value)
        value = deepcopy(valid)
        value['messages'][0]['text'] += ' Corrupted after seal.'
        values.append(value)
        extra_values = [0, 0.0, -0.0, False, True, 1, 1.0, float('nan'), float('inf'), float('-inf'), ('tuple', [1]), {'unicode': '🙂é\x00\n' * 10000}, {'messages': [{'ordinal': n, 'text': 'neutral'} for n in range(129)]}]
        for extra in extra_values:
            value = deepcopy(valid)
            value['diagnosticNeutralValue'] = extra
            value.pop('sourceDigest')
            value['sourceDigest'] = hashlib.sha256(shows._canonical_json(value).encode('utf-8')).hexdigest()
            values.append(value)
        for ordinal, value in enumerate(values):
            with self.subTest(ordinal=ordinal):
                baseline = prior_outcome(shows._safe_document, value)
                candidate = outcome(shows._safe_document, value)
                self.assertEqual(candidate, baseline)

    def test_full_seal_same_payload_revision_authority_and_changed_field_parity(self):
        prior = deepcopy(self.documents[0])
        receipt = deepcopy(prior['sourceAuthorization'])
        payload = deepcopy(prior)
        payload.pop('sourceDigest')
        cases = [(payload, receipt, None), (payload, receipt, prior)]
        advanced = deepcopy(receipt)
        advanced.update(archiveSourceRevision=2, archiveSourceDigest='b' * 64)
        cases.append((payload, advanced, prior))
        for field, changed in (('messages', [dict(text='A current corrected neutral record.', eventId='changed')]), ('showDate', '2026-03-01'), ('showTopics', [dict(term='new topic')]), ('diagnosticNeutralValue', 0), ('diagnosticNeutralValue', 0.0), ('diagnosticNeutralValue', False), ('diagnosticNeutralValue', -0.0), ('diagnosticNeutralValue', ['🙂é\x00' * 5000] * 65)):
            changed_payload = deepcopy(payload)
            changed_payload[field] = changed
            cases.append((changed_payload, receipt, prior))
        denied = deepcopy(receipt)
        denied['publicOnly'] = False
        cases.extend([(payload, denied, prior), (None, receipt, prior)])
        corrupted = deepcopy(prior)
        corrupted['messages'][0]['text'] = 'Unsealed change.'
        cases.append((payload, receipt, corrupted))
        for ordinal, (value, auth, previous) in enumerate(cases):
            with self.subTest(ordinal=ordinal):
                self.assertEqual(outcome(shows._seal_authorized_show_ledger, value, auth, prior_ledger=previous), prior_outcome(shows._seal_authorized_show_ledger, value, auth, prior_ledger=previous))

    def test_full_safe_and_seal_errors_keep_native_classes(self):
        cyclic = []
        cyclic.append(cyclic)
        deep = 0
        for _ in range(990):
            deep = [deep]
        extras = [object(), b'unsupported', '\ud800', cyclic, deep, {1: 'one', 'two': 'two'}, {'a': object(), 'z': cyclic}, {'a': cyclic, 'z': object()}]
        for ordinal, extra in enumerate(extras):
            value = deepcopy(self.documents[0])
            value['diagnosticNeutralValue'] = extra
            with self.subTest(ordinal=ordinal):
                self.assertEqual(outcome(shows._safe_document, value), prior_outcome(shows._safe_document, value))
                self.assertEqual(outcome(shows._seal_authorized_show_ledger, value, value['sourceAuthorization']), prior_outcome(shows._seal_authorized_show_ledger, value, value['sourceAuthorization']))

    def test_seal_mismatched_first_field_still_checks_invalid_later_payload(self):
        prior = deepcopy(self.documents[0])
        cyclic = []
        cyclic.append(cyclic)
        for invalid in (object(), cyclic, {1: 'one', 'two': 'two'}):
            value = deepcopy(prior)
            value['aFirstMismatch'] = 'current changed field'
            value['zInvalidTail'] = invalid
            expected = prior_outcome(shows._seal_authorized_show_ledger,
                value, prior['sourceAuthorization'], prior_ledger=prior)
            self.assertEqual(expected[0], 'error')
            self.assertEqual(outcome(shows._seal_authorized_show_ledger,
                value, prior['sourceAuthorization'], prior_ledger=prior), expected)
