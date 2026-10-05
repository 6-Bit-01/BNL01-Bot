"""Prepared experiment, not production code. Synthetic fixture only, Linux workers.

Review before execution. Example in an already dependency-equipped Linux checkout:
  python allocator_show_counterfactual.py run --source /repo --out /tmp/bnl-allocator
The script never accepts a database, read model, credentials or live configuration.
"""
from __future__ import annotations

import argparse
import ast
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
import copy
import ctypes
from datetime import datetime, timedelta, timezone
import gc
import hashlib
import json
import logging
import os
from pathlib import Path
import platform
import sqlite3
import subprocess
import sys
import threading
import time
import xml.etree.ElementTree as ET

# UTF-8 source with universal newlines, identical for LF/CRLF checkouts.
EXPECTED_OWNER_SHA = 'e82ba190c2f7c17e240efe17e8bf4b6c1a3a9a3d66bd915b7012d77d495ff422'
WORKERS = 6
ACTIVE_OWNERS = 2
ROUNDS = 2
ROWS = 7680
TEXT_BYTES = 1024
GUILD = 77  # Existing fictional test fixture identity, never a live guild.


def file_digest(path):
    sha = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            sha.update(block)
    return sha.hexdigest()


def digest(value):
    sha = hashlib.sha256()
    size = 0
    for part in json.JSONEncoder(sort_keys=True, ensure_ascii=False, separators=(',', ':')).iterencode(value):
        encoded = part.encode('utf-8')
        sha.update(encoded)
        size += len(encoded)
    return sha.hexdigest(), size


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + '\n', encoding='utf-8')


def bootstrap(source):
    for key in list(os.environ):
        if key.startswith(('BNL_', 'GEMINI_', 'DISCORD_')):
            os.environ.pop(key, None)
    # This applies only to synthetic owner authorization in this new process.
    os.environ['BNL_QUEUE_PRODUCTION_ENABLED'] = 'true'
    logging.disable(logging.CRITICAL)
    def audit(event, _args):
        if event in {'socket.connect', 'socket.bind', 'socket.getaddrinfo', 'socket.sendto'}:
            raise PermissionError('offline_experiment_network_denied')
    sys.addaudithook(audit)
    sys.path[:0] = [str(source), str(source / 'tests')]
    if sys.platform == 'win32':
        # Portable fixture smoke only: Linux allocator children never use this
        # adapter. No concurrent writers or production file locks are tested.
        import types
        adapter = types.ModuleType('fcntl')
        adapter.LOCK_EX, adapter.LOCK_SH, adapter.LOCK_UN, adapter.LOCK_NB = 2, 1, 8, 4
        adapter.flock = lambda *_args: None
        sys.modules['fcntl'] = adapter
    path = source / 'bnl_tiktok_show_ledger.py'
    assert hashlib.sha256(path.read_text(encoding='utf-8').encode()).hexdigest() == EXPECTED_OWNER_SHA, 'owner_hash_mismatch'
    import bnl_tiktok_show_ledger as owner
    import test_tiktok_show_evidence_ledger as fixture
    return owner, fixture


def shifted_show(fixture, ordinal):
    show = copy.deepcopy(fixture.archived_show())
    shift = timedelta(days=7 * ordinal)
    def move(value):
        if isinstance(value, dict):
            return {key: move(child) for key, child in value.items()}
        if isinstance(value, list):
            return [move(child) for child in value]
        if isinstance(value, str) and value.startswith(('2026-08-28', '2026-08-29')):
            return (datetime.fromisoformat(value.replace('Z', '+00:00')) + shift).isoformat()
        return value
    show = move(show)
    show['showDate'] = (datetime(2026, 8, 28) + shift).date().isoformat()
    show['sessionId'] = 'neutral-allocator-show-%d' % ordinal
    show['title'] = 'Neutral Fixture Show %d' % ordinal
    return show


def prepare(args):
    owner, fixture = bootstrap(args.source)
    assert not (args.out / 'fixture.sqlite').exists(), 'refuse_existing_fixture'
    db = args.out / 'fixture.sqlite'
    def fixture_audit(event, values):
        if event == 'sqlite3.connect' and Path(os.fsdecode(values[0])).absolute() != db.absolute():
            raise PermissionError('setup_nonfixture_database_denied')
    sys.addaudithook(fixture_audit)
    # Existing fixture routines use transaction contexts without closing them.
    # Track/close only their setup handles before the measured child starts.
    from unittest import mock
    native_connect = sqlite3.connect
    opened = []
    def setup_connect(*a, **kw):
        conn = native_connect(*a, **kw)
        opened.append(conn)
        return conn
    try:
        with mock.patch.object(sqlite3, 'connect', side_effect=setup_connect):
            fixture.TikTokShowEvidenceLedgerTests().seed_source_and_memory(str(db))
    finally:
        for conn in opened:
            conn.close()
    shows = [shifted_show(fixture, index) for index in range(6)]
    model = fixture.authorized_read_model({'currentShow': None, 'latestShow': shows[-1], 'shows': shows[:-1]})
    from bnl_journal_source_store import _record_on_connection
    for ordinal in range(1, 6):
        for event in fixture.durable_events():
            outcome = fixture.record_source_event(str(db), guild_id=GUILD,
                source_kind='tiktok_live_chat', source_key='neutral-%d-%s' % (ordinal, event['event_id']),
                occurred_at_ms=event['occurred_at_ms'] + ordinal * 7 * 86400 * 1000,
                raw_text=event['raw_text'], sanitized_summary=event['raw_text'],
                channel_policy='public_context', subject_ref=event['subject_ref'],
                private_display_name=event['private_display_name'], public_usable=True,
                metadata=event['metadata'])
            assert outcome.ok
    with closing(native_connect(db)) as conn:
        owner.ensure_tiktok_show_evidence_schema(conn)
        # The real Journal source owner validates and hashes each fictional
        # authored message. Saved show documents, not invented large Python
        # allocations, produce the repeated hydration/serialization workload.
        for index in range(ROWS):
            ordinal = index % 6
            base = datetime(2026, 8, 29, 0, 1, tzinfo=timezone.utc) + timedelta(days=ordinal * 7)
            occurred = base + timedelta(milliseconds=250 * (index // 6))
            prefix = 'The neutral fixture sound check has steady levels. Observation %08d. ' % index
            text = (prefix + ('violet signal neutral rhythm ' * 100))[:TEXT_BYTES]
            assert len(text.encode('ascii')) == TEXT_BYTES
            outcome = _record_on_connection(conn, guild_id=GUILD, source_kind='tiktok_live_chat',
                source_key='neutral-stress-%08d' % index, occurred_at_ms=int(occurred.timestamp()*1000),
                ingested_at_ms=int(occurred.timestamp()*1000), raw_text=text, sanitized_summary=text,
                channel_policy='public_context', subject_ref='tiktok_user:test_member_%d' % (index % 48),
                private_display_name='Test Member', public_usable=True,
                metadata={'eventType': 'comment', 'handle': 'test_member_%d' % (index % 48),
                          'sessionId': shows[ordinal]['sessionId']})
            assert outcome.ok
        conn.commit()
        assert conn.execute('PRAGMA quick_check').fetchone() == ('ok',)
        total_text = conn.execute("SELECT SUM(length(CAST(raw_text AS BLOB))) FROM bnl_journal_source_events WHERE source_key LIKE 'neutral-stress-%'").fetchone()[0]
        assert total_text == ROWS * TEXT_BYTES
    # Warm the actual durable owner so measured reads include stored ledgers,
    # retained-source validation and completeness checks, as repeated sync does.
    warm = []
    for _ in range(2):
        outcome = owner.sync_tiktok_show_evidence_ledgers(str(db), guild_id=GUILD,
            read_model=model, artist_identity_index=fixture.artist_index(),
            environ=fixture.ENABLED_QUEUE_ENV, max_seconds=20)
        assert outcome['status'] == 'completed' and outcome['showsSeen'] == 6
        assert outcome['projectionErrors'] == 0
        warm.append(outcome)
    assert warm[-1]['showsUnchanged'] == 6, 'fixture_not_stable_after_warmup'
    with closing(native_connect(db)) as conn:
        saved = conn.execute('SELECT ledger_json FROM ' + owner.TIKTOK_SHOW_EVIDENCE_TABLE).fetchall()
        saved_sizes = [len(row[0].encode()) for row in saved]
        message_counts = [len(json.loads(row[0])['messages']) for row in saved]
    assert len(saved) == 6 and all(count == ROWS // 6 + 4 for count in message_counts), 'fixture_events_outside_show'
    write_json(args.out / 'fixture-model.json', model)
    write_json(args.out / 'fixture-receipt.json', {
        'synthetic_only': True, 'existing_seed_owner': 'TikTokShowEvidenceLedgerTests.seed_source_and_memory',
        'show_count': 6, 'added_rows': ROWS, 'added_source_text_bytes': total_text,
        'database_sha256': file_digest(db),
        'model_sha256': hashlib.sha256((args.out / 'fixture-model.json').read_bytes()).hexdigest(),
        'owner_sha256': EXPECTED_OWNER_SHA,
        'owner_hash_encoding': 'utf8_universal_newline_source',
        'real_sync_warmup_results': warm,
        'stored_ledger_bytes': saved_sizes, 'stored_ledger_bytes_total': sum(saved_sizes),
        'authored_messages_per_stored_show': message_counts,
        'limitations': ['Synthetic source distribution, not a copy of live text, IDs or configuration.',
            'Synthetic authored TikTok messages scale the real Journal owner; Discord/preparation controls remain the small existing fixture.',
            'Readonly nested assembly performs real validation but excludes writer/projection/canon execution.']})


def assembly_factory(owner):
    tree = ast.parse(Path(owner.__file__).read_text(encoding='utf-8'))
    sync = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'sync_tiktok_show_evidence_ledgers')
    nested = next(n for n in ast.walk(sync) if isinstance(n, ast.FunctionDef) and n.name == 'assemble_show')
    shell = ast.parse('''
def factory(conn, guild_id, archive, shows, read_model, artist_identity_index, authorization_receipt, deadline):
    cached_related_sources = None
    cached_data_version = None
    return assemble_show
''').body[0]
    shell.body.insert(-1, copy.deepcopy(nested))
    namespace = dict(vars(owner))
    exec(compile(ast.fix_missing_locations(ast.Module(body=[shell], type_ignores=[])), owner.__file__, 'exec'), namespace)
    return namespace['factory'], hashlib.sha256(ast.dump(nested, include_attributes=False).encode()).hexdigest()


def guarded_connect(db, deadline):
    conn = sqlite3.connect(db.as_uri() + '?mode=ro', uri=True, timeout=.25)
    allowed = {sqlite3.SQLITE_SELECT, sqlite3.SQLITE_READ, sqlite3.SQLITE_FUNCTION, sqlite3.SQLITE_RECURSIVE}
    allowed_pragmas = {'table_info', 'index_list', 'index_info', 'data_version'}
    def authorize(action, arg1, arg2, _database, _trigger):
        if action == sqlite3.SQLITE_PRAGMA and str(arg1).lower() in allowed_pragmas:
            return sqlite3.SQLITE_OK
        return sqlite3.SQLITE_OK if action in allowed else sqlite3.SQLITE_DENY
    conn.set_authorizer(authorize)
    conn.set_progress_handler(lambda: int(time.monotonic() >= deadline), 1000)
    return conn


def confine_database(db):
    def audit(event, values):
        if event == 'sqlite3.connect' and os.fsdecode(values[0]) != db.as_uri() + '?mode=ro':
            raise PermissionError('nonfixture_sqlite_denied')
    sys.addaudithook(audit)


class OwnerAdmission:
    """Rotate real owner jobs through six threads with at most two active."""
    def __init__(self):
        self.barrier = threading.Barrier(WORKERS, timeout=20)
        self.semaphore = threading.BoundedSemaphore(ACTIVE_OWNERS)
        self.lock = threading.Lock()
        self.worker_ids = set()
        self.active = 0
        self.maximum = 0
        self.round_workers = set()

    def begin_round(self):
        with self.lock:
            assert self.active == 0
            self.maximum = 0
            self.round_workers.clear()

    def run(self, owner):
        with self.lock:
            self.worker_ids.add(threading.get_ident())
            self.round_workers.add(threading.get_ident())
        # All six persistent threads participate before any waits for admission.
        self.barrier.wait()
        with self.semaphore:
            with self.lock:
                self.active += 1
                self.maximum = max(self.maximum, self.active)
                assert self.active <= ACTIVE_OWNERS
            try:
                return owner()
            finally:
                with self.lock:
                    self.active -= 1

    def round_receipt(self):
        with self.lock:
            assert self.active == 0 and self.maximum <= ACTIVE_OWNERS
            assert len(self.round_workers) == WORKERS
            return {'active_owner_max': self.maximum, 'active_owner_limit': ACTIVE_OWNERS,
                    'participating_threads': len(self.round_workers), 'active_owners_at_end': self.active}


def smoke(args):
    """Portable fixture/owner/guard validation; deliberately no memory claims."""
    owner, fixture = bootstrap(args.source)
    db = (args.out / 'fixture.sqlite').resolve(strict=True)
    receipt = json.loads((args.out / 'fixture-receipt.json').read_text())
    before = file_digest(db)
    assert before == receipt['database_sha256']
    model = json.loads((args.out / 'fixture-model.json').read_text())
    authorization = owner.show_queue_evidence_authorization(model, environ=fixture.ENABLED_QUEUE_ENV)
    archive = owner.public_show_evidence_archive(model, environ=fixture.ENABLED_QUEUE_ENV)
    shows = owner.tiktok_show_records(archive)
    assert len(shows) == 6 and authorization['usable']
    factory, ast_hash = assembly_factory(owner)
    confine_database(db)
    deadline = time.monotonic() + 60
    hashes = []
    checks = {}
    admission = OwnerAdmission()
    rounds = []
    with closing(guarded_connect(db, deadline)) as conn:
        for label, statement in (('write_denied', 'UPDATE conversations SET content=content WHERE id=101'),
                                 ('attach_denied', "ATTACH DATABASE ':memory:' AS forbidden")):
            try:
                conn.execute(statement)
            except sqlite3.DatabaseError:
                checks[label] = True
            else:
                raise AssertionError(label)
    def read_one(show):
        with closing(guarded_connect(db, deadline)) as conn:
            assemble = factory(conn, GUILD, archive, shows, model, fixture.artist_index(), authorization['receipt'], deadline)
            result = assemble(show)
            assert result is not None and result[4] is True and result[5] is False
            return digest(result[2])
    def scheduled_read(show):
        return admission.run(lambda: read_one(show))
    with ThreadPoolExecutor(max_workers=WORKERS, thread_name_prefix='neutral-smoke') as pool:
        for _ in range(2):
            admission.begin_round()
            futures = [pool.submit(scheduled_read, show) for show in shows]
            outputs = [future.result(timeout=max(.1, deadline-time.monotonic())) for future in futures]
            rounds.append(admission.round_receipt())
            hashes.append(digest(outputs)[0])
            del futures, outputs
    try:
        sqlite3.connect(':memory:')
    except PermissionError:
        checks['other_database_denied'] = True
    else:
        raise AssertionError('other_database_denied')
    assert hashes[0] == hashes[1] and file_digest(db) == before
    write_json(args.out / 'portable-smoke.json', {'status': 'passed', 'synthetic_only': True,
        'python': platform.python_version(), 'sqlite': sqlite3.sqlite_version,
        'shows_per_pass': len(shows), 'passes': 2, 'unchanged_ledger_and_complete_projection_each': True,
        'full_ledger_hash_parity': True, 'output_sha256': hashes[0], 'database_unchanged': True,
        'guard_checks': checks, 'nested_owner_ast_sha256': ast_hash,
        'round_admission_proofs': rounds, 'persistent_worker_count': len(admission.worker_ids),
        'windows_fixture_fcntl_adapter': sys.platform == 'win32',
        'memory_measurement': 'not_performed_portable_fixture_validation_only'})


def memory_snapshot(out, label, libc):
    result = {'label': label}
    status = Path('/proc/self/status').read_text()
    for line in status.splitlines():
        key, _, value = line.partition(':')
        if key in {'VmRSS', 'VmHWM', 'VmSwap', 'RssAnon', 'RssFile', 'Threads'}:
            result[key] = int(value.strip().split()[0])
    # mallinfo on older glibc uses signed int fields; record ABI and do not
    # silently interpret negative overflow as valid byte counts.
    fields = ('arena', 'ordblks', 'smblks', 'hblks', 'hblkhd', 'usmblks', 'fsmblks', 'uordblks', 'fordblks', 'keepcost')
    newer = hasattr(libc, 'mallinfo2')
    class Info(ctypes.Structure):
        _fields_ = [(name, ctypes.c_size_t if newer else ctypes.c_int) for name in fields]
    fn = getattr(libc, 'mallinfo2' if newer else 'mallinfo')
    fn.argtypes = ()
    fn.restype = Info
    values = fn()
    result['mallinfo_api'] = 'mallinfo2' if newer else 'mallinfo'
    result['mallinfo'] = {name: int(getattr(values, name)) for name in fields}
    result['mallinfo_overflow'] = any(value < 0 for value in result['mallinfo'].values())
    # C stdio output describes allocation statistics, not heap bytes/addresses.
    xml_path = out / ('allocator-%s.xml' % label)
    libc.fopen.argtypes = (ctypes.c_char_p, ctypes.c_char_p)
    libc.fopen.restype = ctypes.c_void_p
    libc.fclose.argtypes = (ctypes.c_void_p,)
    libc.malloc_info.argtypes = (ctypes.c_int, ctypes.c_void_p)
    stream = libc.fopen(os.fsencode(xml_path), b'w')
    assert stream, 'allocator_statistics_open_failed'
    try:
        assert libc.malloc_info(0, stream) == 0, 'allocator_statistics_failed'
    finally:
        libc.fclose(stream)
    root = ET.parse(xml_path).getroot()
    result['malloc_info'] = {
        'heap_count': len(root.findall('heap')),
        'totals': [dict(node.attrib) for node in root.findall('total')],
        'systems': [dict(node.attrib) for node in root.findall('system')],
    }
    return result


def child(args):
    assert sys.platform == 'linux', 'linux_only_experiment'
    import resource
    resource.setrlimit(resource.RLIMIT_AS, (2 * 1024**3, 2 * 1024**3))
    owner, fixture = bootstrap(args.source)
    db = (args.out / 'fixture.sqlite').resolve(strict=True)
    receipt = json.loads((args.out / 'fixture-receipt.json').read_text())
    assert receipt['synthetic_only'] is True and receipt['owner_sha256'] == EXPECTED_OWNER_SHA
    assert file_digest(db) == receipt['database_sha256']
    model_path = args.out / 'fixture-model.json'
    assert hashlib.sha256(model_path.read_bytes()).hexdigest() == receipt['model_sha256']
    model = json.loads(model_path.read_text())
    environment = dict(fixture.ENABLED_QUEUE_ENV)
    authorization = owner.show_queue_evidence_authorization(model, environ=environment)
    assert authorization['usable'] and owner.show_queue_evidence_authorization_receipt_valid(authorization['receipt'])
    archive = owner.public_show_evidence_archive(model, environ=environment)
    shows = owner.tiktok_show_records(archive)
    assert len(shows) == 6
    factory, ast_hash = assembly_factory(owner)
    confine_database(db)
    libc = ctypes.CDLL(None)
    libc.gnu_get_libc_version.restype = ctypes.c_char_p
    snapshots = []
    label = args.variant + '-' + args.measurement
    run_out = args.out / label
    run_out.mkdir(exist_ok=False)
    deadline = time.monotonic() + 170
    admission = OwnerAdmission()
    def perform_owner():
        with closing(guarded_connect(db, deadline)) as conn:
            assemble = factory(conn, GUILD, archive, shows, model, fixture.artist_index(), authorization['receipt'], deadline)
            outputs = []
            elapsed = 0.0
            previous = None
            for show in shows:
                started = time.perf_counter()
                previous = assemble(show)
                elapsed += time.perf_counter() - started
                assert previous is not None, 'fixture_show_not_assembled'
                _version, _changes, ledger, serialized, unchanged, repair, canon = previous
                content_hash, content_bytes = digest(ledger)
                outputs.append({'hash': content_hash, 'bytes': content_bytes,
                    'write_payload_bytes': len(serialized.encode()), 'unchanged': unchanged,
                    'repair': repair, 'canon': canon})
            # Results deliberately contain no owner objects, text or identities.
            return {'output_sha256': digest(outputs)[0], 'shows': len(outputs), 'owner_seconds': elapsed}
    def perform(_slot):
        return admission.run(perform_owner)
    gc.collect()
    if args.measurement == 'python':
        import tracemalloc
        tracemalloc.start()
    def sample(phase):
        value = memory_snapshot(run_out, phase, libc)
        if args.measurement == 'python':
            value['python_current_bytes'], value['python_peak_bytes'] = tracemalloc.get_traced_memory()
        snapshots.append(value)
    sample('before_workers')
    hashes = []
    per_round = []
    with ThreadPoolExecutor(max_workers=WORKERS, thread_name_prefix='neutral-show') as pool:
        for number in range(ROUNDS):
            admission.begin_round()
            started = time.perf_counter()
            cpu_started = time.process_time()
            futures = [pool.submit(perform, slot) for slot in range(WORKERS)]
            results = [future.result(timeout=max(.1, deadline - time.monotonic())) for future in futures]
            per_round.append({'round': number, 'wall_seconds': time.perf_counter() - started,
                'process_cpu_seconds': time.process_time() - cpu_started,
                'owner_seconds': [r['owner_seconds'] for r in results],
                'admission_proof': admission.round_receipt()})
            hashes.extend(r['output_sha256'] for r in results)
            del results, futures
            sample('round_%d_quiescent_workers_alive' % number)
        gc.collect()
        sample('collected_workers_alive')
    sample('workers_shutdown')
    gc.collect()
    sample('collected_after_shutdown')
    if args.measurement == 'python':
        tracemalloc.stop()
    assert len(admission.worker_ids) == WORKERS and len(set(hashes)) == 1, 'worker_or_output_parity_failed'
    assert file_digest(db) == receipt['database_sha256'], 'fixture_changed'
    write_json(run_out / 'result.json', {
        'status': 'completed', 'variant': args.variant, 'measurement': args.measurement,
        'python': platform.python_version(), 'sqlite': sqlite3.sqlite_version,
        'glibc': libc.gnu_get_libc_version().decode(), 'cpu_count': os.cpu_count(),
        'cpu_affinity': sorted(os.sched_getaffinity(0)), 'address_space_limit_bytes': 2 * 1024**3,
        'arena_limit_setting': os.environ.get('MALLOC_ARENA_MAX', 'default_unset'),
        'source_sha256': EXPECTED_OWNER_SHA, 'nested_owner_ast_sha256': ast_hash,
        'source_hash_encoding': 'utf8_universal_newline_source',
        'fixture_sha256': receipt['database_sha256'], 'fixture_unchanged': True,
        'unique_worker_count': len(admission.worker_ids), 'rounds': ROUNDS, 'shows_per_job': 6,
        'simultaneous_owner_limit': ACTIVE_OWNERS,
        'all_job_output_hashes_equal': True, 'output_sha256': hashes[0],
        'snapshots': snapshots, 'round_timings': per_round,
        'limits': ['Synthetic TikTok source and stored ledger distributions approximate the saved show shape; they do not reproduce live data.',
            'Real read/assembly owners execute; no outer sync DDL, writer lease, projection, provider or Discord workload.',
            'Tracing has its own allocation cost; native and traced child measurements must not be combined as one memory baseline.',
            'GC boundaries are explicitly labeled; no allocator trim or live heap injection is performed.',
            'Even a native allocator difference establishes only isolated counterfactual behavior, not live causality.']})


def run(args):
    assert sys.platform == 'linux', 'linux_only_experiment'
    # Own-process limit only; children and their worker threads inherit it.
    os.sched_setaffinity(0, set(sorted(os.sched_getaffinity(0))[:2]))
    assert not args.out.exists(), 'refuse_existing_output_directory'
    args.out.mkdir(parents=True)
    script = str(Path(__file__).resolve())
    base = [sys.executable, script]
    # Do not pass CI/service credentials or arbitrary ambient settings into
    # measurement children. Python/runtime path and temporary directory only.
    allowed_environment = {'PATH', 'HOME', 'LANG', 'LC_ALL', 'PYTHONPATH',
                           'PYTHONHOME', 'SYSTEMROOT', 'TMPDIR', 'TEMP', 'TMP'}
    clean = {key: value for key, value in os.environ.items() if key in allowed_environment}
    clean['PYTHONDONTWRITEBYTECODE'] = '1'
    results = []
    jobs = [('prepare', '', '', 60)] + [('child', variant, mode, 180)
        for mode in ('native', 'python') for variant in ('default', 'arena2')]
    for stage, variant, mode, timeout in jobs:
        env = dict(clean)
        if variant == 'arena2':
            env['MALLOC_ARENA_MAX'] = '2'
        command = base + [stage, '--source', str(args.source), '--out', str(args.out)]
        if stage == 'child':
            command += ['--variant', variant, '--measurement', mode]
        path = args.out / ('%s-%s-%s.log' % (stage, variant, mode))
        started = time.monotonic()
        with path.open('wb') as stream:
            try:
                completed = subprocess.run(command, env=env, stdout=stream, stderr=subprocess.STDOUT, timeout=timeout)
                code, timed_out = completed.returncode, False
            except subprocess.TimeoutExpired:
                code, timed_out = -1, True
        results.append({'stage': stage, 'variant': variant, 'mode': mode, 'exit_code': code,
            'timed_out': timed_out, 'seconds': time.monotonic() - started, 'log': path.name})
        write_json(args.out / 'supervisor.json', {'status': 'failed' if code else 'running', 'stages': results})
        if code and stage == 'prepare':
            raise RuntimeError('bounded_experiment_child_failed')
    paths = [args.out / (variant + '-' + mode) / 'result.json'
        for mode in ('native', 'python') for variant in ('default', 'arena2')]
    observations = [json.loads(path.read_text()) for path in paths if path.exists()]
    complete = len(observations) == 4 and not any(row['exit_code'] for row in results)
    parity = complete and len({row['output_sha256'] for row in observations}) == 1
    native = [row for row in observations if row['measurement'] == 'native']
    native_parity = len(native) == 2 and len({row['output_sha256'] for row in native}) == 1
    write_json(args.out / 'supervisor.json', {
        'status': 'incomplete_inconclusive' if not complete else ('completed' if parity else 'parity_failed'),
        'all_four_output_hashes_equal': parity, 'native_pair_output_hashes_equal': native_parity,
        'completed_children': len(observations), 'stages': results,
        'interpretation': 'Compare current Python bytes separately from native RSS, malloc used/free bytes and arena counts after quiescence. No live recovery conclusion.'})
    assert parity, 'bounded_experiment_incomplete_or_unequal'


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('stage', choices=('run', 'prepare', 'smoke', 'child'))
    parser.add_argument('--source', required=True, type=lambda p: Path(p).resolve(strict=True))
    parser.add_argument('--out', required=True, type=lambda p: Path(p).resolve())
    parser.add_argument('--variant', choices=('default', 'arena2'))
    parser.add_argument('--measurement', choices=('native', 'python'))
    args = parser.parse_args()
    if args.stage == 'child':
        assert args.variant and args.measurement
    {'run': run, 'prepare': prepare, 'smoke': smoke, 'child': child}[args.stage](args)


if __name__ == '__main__':
    main()
