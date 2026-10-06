"""Exact-source repair invariants on neutral SQLite, including legacy COMMIT.

No bot/SDK import. This module runs with Python3.9.5 and the standard library.
Linux exercises the real privacy fence; Windows substitutes only that unavailable
POSIX boundary inside each test and does not claim native lock proof.
"""
import ast
from contextlib import closing, contextmanager, ExitStack
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest import mock

import bnl_memory_ledger as ledger


SOURCE = Path(__file__).resolve().parents[1]
ENV = {ledger.MEMORY_LEDGER_SHADOW_ENV: 'true',
       ledger.CONVERSATION_MOTIF_FORMATION_ENV: 'true'}
JOURNAL_DDL = next(node.value for node in ast.walk(ast.parse(
    (SOURCE / 'bnl_journal_source_store.py').read_text(encoding='utf-8')))
    if isinstance(node, ast.Constant) and isinstance(node.value, str)
    and 'CREATE TABLE IF NOT EXISTS bnl_journal_source_events (' in node.value)


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
        separators=(',', ':')).encode('utf-8')).hexdigest()


class RetainedConversationRepairTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory(prefix='bnl-retained-repair-')
        self.addCleanup(directory.cleanup)
        self.directory = Path(directory.name)
        self.db = self.directory / 'neutral.sqlite'
        Path(str(self.db) + '.journal-privacy.lock').touch()
        self.native_connect = sqlite3.connect
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.retained_errors = []
        self.retained_connections = []
        self.fence_depth = 0
        if os.name == 'nt':
            @contextmanager
            def diagnostic_fence(_path, *, blocking=True):
                self.fence_depth += 1
                try:
                    yield True
                finally:
                    self.fence_depth -= 1
            self.stack.enter_context(mock.patch.dict(sys.modules, {
                'bnl_journal_source_store': SimpleNamespace(
                    journal_release_privacy_fence=diagnostic_fence)}))
        with closing(self.native_connect(self.db)) as conn, conn:
            self.assertEqual(conn.execute('PRAGMA journal_mode=DELETE').fetchone()[0], 'delete')
            ledger.ensure_memory_ledger_schema(conn)
            conn.execute('''CREATE TABLE conversations(id INTEGER PRIMARY KEY,
                guild_id INTEGER,user_id INTEGER,role TEXT,content TEXT,channel_id INTEGER,
                channel_policy TEXT,message_id INTEGER,timestamp TEXT,route_mode TEXT)''')
            conn.execute(JOURNAL_DDL)
            for number in (1, 2):
                content = 'Neutral fixture signal meter number %s.' % number
                conn.execute('INSERT INTO conversations VALUES(?,?,?,?,?,?,?,?,?,?)',
                    (number, 77, 22, 'user', content, 10, 'public_selective', 900 + number,
                     '2026-10-04 03:50:00', 'channel_observation'))
                conn.execute('''INSERT INTO bnl_journal_source_events(
                    guild_id,source_kind,source_key,occurred_at_ms,ingested_at_ms,channel_id,
                    channel_policy,subject_ref,raw_text,sanitized_summary,content_hash,public_usable)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?)''',
                    (77, 'discord_message', str(900 + number), 1791085800000, 1791085800000,
                     10, 'public_selective', ledger.subject_key_for_user(22), content, '',
                     hashlib.sha256(content.encode()).hexdigest(), 1))
            self.manifest = {'version': 'projection_gap_private_manifest_v1',
                'rows': [ledger._retained_repair_binding(conn,
                         ledger._retained_repair_original(conn, number)) for number in (1, 2)]}
            self.manifest_hash = digest(self.manifest)
            self.schema_hash = ledger._retained_repair_schema(conn)
        self.source_before = self.source_state()

    def source_state(self):
        with closing(self.native_connect(self.db)) as conn:
            return tuple(conn.execute('SELECT * FROM ' + table + ' ORDER BY 1').fetchall()
                         for table in ('conversations', 'bnl_journal_source_events'))

    def counts(self):
        with closing(self.native_connect(self.db, timeout=.02)) as conn:
            return tuple(conn.execute('SELECT COUNT(*) FROM ' + table).fetchone()[0]
                for table in ('memory_ledger_entries', 'memory_ledger_participants',
                              'memory_ledger_shadow_receipts'))

    def run_repair(self, **kwargs):
        options = dict(expected_manifest_sha256=self.manifest_hash,
                       expected_schema_sha256=self.schema_hash, environ=ENV)
        options.update(kwargs)
        return ledger.run_retained_conversation_repair(str(self.db), self.manifest, **options)

    def apply(self, **kwargs):
        options = dict(dry_run=False, channel_policy_resolver=lambda *_: 'public_selective')
        options.update(kwargs)
        return self.run_repair(**options)

    def edit(self, sql, parameters=()):
        with closing(self.native_connect(self.db)) as conn, conn:
            conn.execute(sql, parameters)

    def test_default_preview_is_read_only_and_schema_preserving(self):
        before = self.db.read_bytes()
        result = self.run_repair()
        self.assertEqual(result['status'], 'stored_eligibility_preview')
        self.assertEqual(result['counts']['missing_roots'], 2)
        self.assertEqual(result['counts']['blocked'], 0)
        self.assertFalse(result['repair_executed'])
        self.assertFalse(result['external_controls_verified'])
        self.assertEqual(result['schema_sha256'], self.schema_hash)
        self.assertEqual(self.db.read_bytes(), before)
        self.assertEqual(self.counts(), (0, 0, 0))

    def test_apply_requires_explicit_policy_owner_manifest_schema_and_shadow(self):
        cases = ({'channel_policy_resolver': None}, {'expected_manifest_sha256': '0' * 64},
                 {'expected_schema_sha256': ''}, {'environ': {}})
        for options in cases:
            with self.subTest(keys=tuple(options)):
                with self.assertRaises(ValueError):
                    self.apply(**options)
                self.assertEqual(self.counts(), (0, 0, 0))
        self.assertEqual(self.source_state(), self.source_before)

    def test_exact_apply_is_atomic_preserves_sources_and_is_idempotent(self):
        with mock.patch.object(ledger, 'ensure_memory_ledger_schema', side_effect=AssertionError('must not initialize schema')):
            result = self.apply()
            repeated = self.apply()
        self.assertEqual((result['inserted'], result['database_writes']), (2, 6))
        self.assertEqual((repeated['inserted'], repeated['database_writes']), (0, 0))
        self.assertEqual(self.counts(), (2, 2, 2))
        self.assertEqual(self.source_state(), self.source_before)
        with closing(self.native_connect(self.db)) as conn:
            self.assertEqual(ledger._retained_repair_schema(conn), self.schema_hash)
            for number in (1, 2):
                self.assertTrue(ledger._retained_repair_root_exact(conn,
                    ledger._retained_repair_entry(ledger._retained_repair_original(conn, number))))
            self.assertEqual(conn.execute('SELECT COUNT(*) FROM memory_ledger_lineage').fetchone()[0], 0)

    def test_full_23_row_bound_commits_69_writes_and_repeats_without_source_mutation(self):
        with closing(self.native_connect(self.db)) as conn, conn:
            for number in range(3, 24):
                content = 'Neutral complete-cohort signal meter number %s.' % number
                conn.execute('INSERT INTO conversations VALUES(?,?,?,?,?,?,?,?,?,?)',
                    (number, 77, 22, 'user', content, 10, 'public_selective', 900 + number,
                     '2026-10-04 03:50:00', 'channel_observation'))
                conn.execute('''INSERT INTO bnl_journal_source_events(
                    guild_id,source_kind,source_key,occurred_at_ms,ingested_at_ms,channel_id,
                    channel_policy,subject_ref,raw_text,sanitized_summary,content_hash,public_usable)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?)''',
                    (77, 'discord_message', str(900 + number), 1791085800000, 1791085800000,
                     10, 'public_selective', ledger.subject_key_for_user(22), content, '',
                     hashlib.sha256(content.encode()).hexdigest(), 1))
            self.manifest['rows'] = [ledger._retained_repair_binding(conn,
                ledger._retained_repair_original(conn, number)) for number in range(1, 24)]
        self.manifest_hash = digest(self.manifest)
        source_before = self.source_state()
        # The real owner enforces its own deadline; no mocked clock or enlarged
        # budget is used for the complete bounded transaction.
        first = self.apply()
        second = self.apply()
        self.assertEqual((first['inserted'], first['database_writes']), (23, 69))
        self.assertEqual((second['inserted'], second['database_writes']), (0, 0))
        self.assertEqual(self.counts(), (23, 23, 23))
        self.assertEqual(self.source_state(), source_before)
        with closing(self.native_connect(self.db)) as conn:
            self.assertEqual(ledger._retained_repair_schema(conn), self.schema_hash)
            for number in range(1, 24):
                self.assertTrue(ledger._retained_repair_root_exact(conn,
                    ledger._retained_repair_entry(ledger._retained_repair_original(conn, number))))

    def test_original_content_identity_policy_and_deletion_drift_block_entire_batch(self):
        variants = [('content', 'Changed neutral text.'), ('user_id', 23), ('guild_id', 78),
                    ('channel_id', 11), ('channel_policy', 'sealed_test'),
                    ('message_id', 1901), ('route_mode', 'normal_chat'), ('role', 'model')]
        for field, value in variants:
            with self.subTest(field=field):
                with closing(self.native_connect(self.db)) as conn:
                    old = conn.execute('SELECT ' + field + ' FROM conversations WHERE id=2').fetchone()[0]
                self.edit('UPDATE conversations SET ' + field + '=? WHERE id=2', (value,))
                with self.assertRaises(ValueError):
                    self.apply()
                self.assertEqual(self.counts(), (0, 0, 0))
                self.edit('UPDATE conversations SET ' + field + '=? WHERE id=2', (old,))
        self.edit('DELETE FROM conversations WHERE id=2')
        with self.assertRaises(ValueError):
            self.apply()
        self.assertEqual(self.counts(), (0, 0, 0))

    def test_archive_body_binding_and_privacy_drift_block_entire_batch(self):
        for field, value in [('raw_text', 'Changed neutral archive.'), ('content_hash', 'changed'),
                ('channel_policy', 'sealed_test'), ('channel_id', 11), ('subject_ref', 'discord_user:23'),
                ('public_usable', 0), ('metadata_json', '{"changed":true}')]:
            with self.subTest(field=field):
                with closing(self.native_connect(self.db)) as conn:
                    old = conn.execute('SELECT ' + field + ' FROM bnl_journal_source_events WHERE source_key=?', ('902',)).fetchone()[0]
                self.edit('UPDATE bnl_journal_source_events SET ' + field + '=? WHERE source_key=?', (value, '902'))
                with self.assertRaises(ValueError):
                    self.apply()
                self.assertEqual(self.counts(), (0, 0, 0))
                self.edit('UPDATE bnl_journal_source_events SET ' + field + '=? WHERE source_key=?', (old, '902'))

    def test_dangling_controls_and_member_control_block_before_projection(self):
        root = self.manifest['rows'][1]['expected_root']
        for actor, target, kind in [('neutral-control', root, 'retracts'),
                ('neutral-control', root, 'correction_of'), ('neutral-control', root, 'supersedes'),
                (root, 'neutral-target', 'derived_from')]:
            self.edit('INSERT INTO memory_ledger_lineage VALUES(?,?,?,?,?)',
                      (actor, 77, kind, target, '2026-10-04 04:00:00'))
            with self.assertRaises(ValueError):
                self.apply()
            self.assertEqual(self.counts(), (0, 0, 0))
            self.edit('DELETE FROM memory_ledger_lineage')
        with closing(self.native_connect(self.db)) as conn, conn:
            ledger.insert_ledger_entry(conn, ledger.LedgerEntry(guild_id=77,
                source_table='member_memory_control', source_row_id='neutral-control',
                source_role='member_control', entry_type='claim', subject_key=ledger.subject_key_for_user(22),
                predicate_key='retraction', value='', visibility=ledger.Visibility.PRIVATE,
                source_class=ledger.SourceClass.FIRST_PARTY_RECORD))
        before = self.counts()
        with self.assertRaises(ValueError):
            self.apply()
        self.assertEqual(self.counts(), before)

    def test_inexact_existing_root_and_schema_drift_require_review(self):
        self.apply()
        self.edit("UPDATE memory_ledger_entries SET normalized_value='Altered projection'")
        before = self.counts()
        with self.assertRaises(ValueError):
            self.apply()
        self.assertEqual(self.counts(), before)
        for table in ('memory_ledger_entries', 'memory_ledger_participants', 'memory_ledger_shadow_receipts'):
            self.edit('DELETE FROM ' + table)
        self.edit('CREATE INDEX neutral_new_index ON conversations(message_id)')
        with self.assertRaises(ValueError):
            self.apply()
        self.assertEqual(self.counts(), (0, 0, 0))

    def test_extra_participant_prevents_false_idempotent_acceptance(self):
        self.apply()
        root = self.manifest['rows'][0]['expected_root']
        self.edit('''INSERT INTO memory_ledger_participants
            (entry_id,guild_id,participant_key,display_name,participant_role,order_index,created_at)
            VALUES(?,?,?,?,?,?,?)''',
            (root, 77, 'discord_user:23', '', 'author', 1, '2026-10-04 04:00:00'))
        before = self.counts()
        with self.assertRaises(ValueError):
            self.apply()
        self.assertEqual(self.counts(), before)

    def test_trigger_side_effects_cannot_change_sources_or_expand_write_count(self):
        statements = (
            "UPDATE conversations SET content='Forbidden trigger mutation' WHERE id=1;",
            """INSERT INTO memory_ledger_shadow_receipts
                (guild_id,writer,source_table,source_row_id,attempted_at,outcome,reason_code)
                VALUES(77,'neutral-trigger','conversations','1','2026-10-04','inserted','unexpected');""",
        )
        for statement in statements:
            with self.subTest(target=statement.split()[0]):
                self.edit('CREATE TRIGGER neutral_side_effect AFTER INSERT ON memory_ledger_entries BEGIN ' + statement + ' END')
                with closing(self.native_connect(self.db)) as conn:
                    schema = ledger._retained_repair_schema(conn)
                with self.assertRaises((sqlite3.DatabaseError, ValueError)):
                    self.apply(expected_schema_sha256=schema)
                self.assertEqual(self.counts(), (0, 0, 0))
                self.assertEqual(self.source_state(), self.source_before)
                self.edit('DROP TRIGGER neutral_side_effect')

    def test_missing_index_manifest_duplicates_and_oversized_cohort_fail_closed(self):
        original = json.loads(json.dumps(self.manifest))
        for count in (0, 2, 24):
            self.manifest = dict(original, rows=[] if count == 0 else [original['rows'][0]] * count)
            with self.assertRaises(ValueError):
                self.run_repair(expected_manifest_sha256=digest(self.manifest))
            self.assertEqual(self.counts(), (0, 0, 0))
        self.manifest = original
        self.edit('DROP INDEX idx_mll_guild')
        with self.assertRaises(ValueError):
            self.run_repair()
        self.assertEqual(self.counts(), (0, 0, 0))

    def test_second_writer_failure_rolls_back_first_root_participant_and_receipt(self):
        original = ledger.shadow_conversation_row
        calls = []
        def fail_second(*args, **kwargs):
            calls.append(kwargs['row_id'])
            if len(calls) == 2:
                raise RuntimeError('neutral second-source failure')
            return original(*args, **kwargs)
        with mock.patch.object(ledger, 'shadow_conversation_row', side_effect=fail_second):
            with self.assertRaises(RuntimeError) as caught:
                self.apply()
        self.retained_errors.append(caught.exception)
        self.assertEqual(calls, [1, 2])
        self.assertEqual(self.counts(), (0, 0, 0))
        self.assertEqual(self.source_state(), self.source_before)

    def test_policy_drift_and_expired_deadline_after_insert_roll_back_batch(self):
        for expire in (False, True):
            with self.subTest(expired=expire):
                clock = [0.0]
                calls = []
                def policy(*_args):
                    calls.append(1)
                    if len(calls) > 2:
                        if expire:
                            clock[0] = 20.0
                        else:
                            return 'sealed_test'
                    return 'public_selective'
                with mock.patch.object(ledger.time, 'monotonic', side_effect=lambda: clock[0]):
                    with self.assertRaises(TimeoutError if expire else ValueError):
                        self.apply(channel_policy_resolver=policy)
                self.assertGreater(len(calls), 2)
                self.assertEqual(self.counts(), (0, 0, 0))
                self.assertEqual(self.source_state(), self.source_before)

    def test_real_failed_commit_closes_retained_connection_and_releases_all_locks(self):
        reader = self.native_connect(self.db, timeout=.02)
        self.addCleanup(reader.close)
        reader.execute('BEGIN')
        reader.execute('SELECT * FROM conversations').fetchall()
        traces = []
        def tracked(*args, **kwargs):
            kwargs['timeout'] = .02
            conn = self.native_connect(*args, **kwargs)
            conn.set_trace_callback(traces.append)
            self.retained_connections.append(conn)
            return conn
        with mock.patch.object(ledger.sqlite3, 'connect', side_effect=tracked):
            try:
                self.apply()
            except sqlite3.OperationalError as error:
                self.retained_errors.append(error)
                self.assertIn('locked', str(error).lower())
            else:
                self.fail('held real reader must prevent writer COMMIT')
        self.assertIn('COMMIT', traces)
        self.assertIn('ROLLBACK', traces)
        reader.close()
        self.assertEqual(self.counts(), (0, 0, 0))
        for conn in self.retained_connections:
            with self.assertRaises(sqlite3.ProgrammingError):
                conn.execute('SELECT 1')
        self.assertEqual(self.source_state(), self.source_before)

    def test_manifest_is_copied_before_policy_callback_mutates_callers_object(self):
        def policy(*_args):
            self.manifest['rows'].clear()
            return 'public_selective'
        result = self.apply(channel_policy_resolver=policy)
        self.assertEqual(result['inserted'], 2)
        self.assertEqual(self.counts(), (2, 2, 2))

    @unittest.skipIf(os.name == 'nt', 'Requires existing native POSIX privacy fence')
    def test_native_cross_process_privacy_fence_defers_before_database_open(self):
        ready = self.directory / 'ready'
        stop = self.directory / 'stop'
        program = '''import fcntl,os,sys,time
fd=os.open(sys.argv[1],os.O_RDWR)
fcntl.flock(fd,fcntl.LOCK_EX)
open(sys.argv[2],'w').close()
deadline=time.monotonic()+4
while not os.path.exists(sys.argv[3]) and time.monotonic()<deadline: time.sleep(.01)
fcntl.flock(fd,fcntl.LOCK_UN)
os.close(fd)
'''
        child = subprocess.Popen([sys.executable, '-I', '-S', '-B', '-c', program,
            str(self.db) + '.journal-privacy.lock', str(ready), str(stop)],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            deadline = time.monotonic() + 2
            while not ready.exists() and time.monotonic() < deadline:
                time.sleep(.01)
            self.assertTrue(ready.exists())
            with mock.patch.object(ledger.sqlite3, 'connect', side_effect=AssertionError('must not open before fence')):
                result = self.run_repair()
            self.assertEqual(result['status'], 'deferred_privacy_fence_busy')
        finally:
            stop.touch()
            try:
                child.wait(timeout=2)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait(timeout=2)
        self.assertEqual(self.counts(), (0, 0, 0))


if __name__ == '__main__':
    unittest.main()
