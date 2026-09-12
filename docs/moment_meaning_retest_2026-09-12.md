# Moment meaning: candidate and bounded retest — September 12

This advances F01 / original acceptance row 6, through the existing Moment and
Intelligence Packet owners. The baseline is main
`387a022b714eebd060db6a339bbca9f1c3475b1e` (#541). Review and merge precede
production deployment. Local verification does not establish live acceptance.

## Why this change exists

The two captured public conversations retained their source membership and
episode links, but both summaries were generic templates and all human
contribution projections were empty. The candidate retains the same eligible
sources and episode identity while producing useful derived summaries and
human contribution paraphrases. It preserves questions, uncertainty, jokes,
corrections and human/model attribution in the summarization instructions.
BNL's own assertions remain model output, not independent corroboration.

The regression fixtures preserve the two captured turn structures with neutral
fictional labels. They reconstruct the captured membership, then use real
finalization and packet readers. The export lacks some original normalized
ledger fields, so this is not a complete live admission replay. Controlled
provider responses prove storage, source validation and packet use, not the
semantic quality of an actual Gemini response.

## Changed behavior and operating bounds

- Newly finalized, eligible public Moments receive a pending meaning status.
  The existing minute sweep starts at most one background worker at a time.
  There is no new scheduler or answer correction loop.
- The existing `moment_meaning_background` provider route accounts for an
  additional call per attempted Moment. It uses the background budget, a
  maximum 2,048 output tokens, no provider retry, no model fallback and no
  Journal/Relay protected reserve. Budget denial can result in zero physical
  calls. The input is limited to 32 original source rows, 12,000 source-text
  characters and six meaningful human participants. A summary is at most 360
  characters; each human contribution is at most 240.
- The existing public ordinary scope and its guild allowlist govern claims.
  Moment and ledger shadow prerequisites must also be effective. The same
  scope is checked again before saving. No configuration is enabled by this PR.
- SQLite commits and releases the source snapshot before the provider call.
  Saving rechecks the complete source digest, source roles/subjects,
  visibility, lifecycle and the existing canonical reference. The existing
  privacy and non-extractive representation checks remain in force. These
  checks cannot prove that every generated paraphrase is semantically correct.
- A successful save creates a new derived ledger revision and source-linked
  human contributions, supersedes the old derived projection, and refreshes
  the existing episode projection. Original conversations, Moment membership
  and episode identity remain intact. The new representation stays low
  confidence / `review_only`; the canonical pointer is not canon approval.
- Recall indexes the new summary through existing readers without changing
  episode grouping signatures. Packet revalidation rejects altered supporting
  sources or a changed saved projection before delivery.
- A failed, cancelled, ineligible or over-budget attempt retains its original
  representation and an explicit status. The sweep does not automatically
  retry it. A process exit after claiming can leave `generating`; that is an
  incomplete attempt requiring investigation, never a pass or permission to
  replay it. Existing finalized records migrate as `legacy`, without backfill.

The two historical captures are not rewritten by deployment. Natural grouping,
association/reopening, public aging/retention and Ambient evidence remain open.
The accepted Oreaganomics joke and all unaffected acceptance passes remain.

## Local verification commands

Run from the repository with the pinned requirements installed:

```bash
env GEMINI_API_KEY=test-gemini-key DISCORD_BOT_TOKEN=test-discord-token python -m unittest discover -s tests -p 'test_moment*.py'
env GEMINI_API_KEY=test-gemini-key DISCORD_BOT_TOKEN=test-discord-token make check PYTHON=python
```

The candidate's PR records the actual result, tested commit and test counts.
Provider responses in local tests are controlled; no production service or
database participates in these commands.

## After review and merge: deploy the verified runtime once

Run this on the existing VPS. The hashes below identify the reviewed runtime
files even when the merge commit differs from the candidate commit. A changed
hash stops before restart and requires review of that newer tree. The capture
state is saved on disk and can be used after reconnecting to SSH.

```bash
(
set -euo pipefail
cd /home/ubuntu/bnl01
git pull --ff-only origin main
venv/bin/python - <<'PY'
import hashlib, json, py_compile, subprocess, tempfile
from datetime import datetime, timezone
from pathlib import Path

expected = {
    'bnl01_bot.py': '7e0eb2f1c1519bfd85c78d0cb354cbce4cc3b543c35c0dadfb76e81d723d27b7',
    'bnl_moment_engine.py': 'bfcdd749641cd144c7a64de53dece82bd7abdf46cee3102014c24087ab56adcf',
    'bnl_gemini_routing.py': 'dba87a57c6eb9719ee9b0c59dbab33cdc58cf4283742a981739b5df7f89a85d4',
}
for name, digest in expected.items():
    if hashlib.sha256(Path(name).read_bytes()).hexdigest() != digest:
        raise SystemExit('STOP: reviewed runtime does not match: ' + name)
    py_compile.compile(name, doraise=True)
state = {
    'start_utc': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
    'directory': tempfile.mkdtemp(prefix='bnl-moment-meaning-retest.'),
    'deployed_commit': subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
    'runtime_sha256': expected,
    'guild_id': 1288269405209235551,
}
subprocess.run(['sudo', 'systemctl', 'restart', 'bnl01'], check=True)
subprocess.run(['sudo', 'systemctl', 'is-active', '--quiet', 'bnl01'], check=True)
state['main_pid'] = subprocess.check_output(
    ['systemctl', 'show', 'bnl01', '-p', 'MainPID', '--value'], text=True).strip()
Path('/tmp/bnl-moment-meaning-retest.json').write_text(json.dumps(state, indent=2) + '\n')
print(json.dumps(state, indent=2))
PY
)
```

If deployment fails, retain the error and stop before testing. No public gate,
schedule, database record or acceptance threshold is changed by this block.

## Inspect one genuine public Moment, then test later recall

Use the next naturally occurring qualifying conversation in the already
authorized public scope. Do not manufacture public messages or force a show or
Ambient publication. After normal finalization and a subsequent minute sweep,
run this inspection. It bookmarks up to two newest post-deployment public
Moments and prints their summaries, contributions and attempt status. Run it
once for the intended exchange; it is not a polling loop.

```bash
cd /home/ubuntu/bnl01 && venv/bin/python - <<'PY'
import json, sqlite3
from contextlib import closing
from pathlib import Path

path = Path('/tmp/bnl-moment-meaning-retest.json')
state = json.loads(path.read_text())
with closing(sqlite3.connect('file:bnl01_conversations.db?mode=ro', uri=True, timeout=3)) as db:
    db.row_factory = sqlite3.Row
    db.execute('PRAGMA query_only=ON')
    db.execute('BEGIN')
    moments = [dict(r) for r in db.execute(
        "SELECT moment_id,channel_name,finalized_at,summary,meaning_status,meaning_attempted_at "
        "FROM memory_moment_windows WHERE guild_id=? AND public_usable=1 "
        "AND visibility IN ('public','public_safe') AND meaning_status!='legacy' "
        "AND julianday(finalized_at)>=julianday(?) ORDER BY finalized_at DESC LIMIT 2",
        (state['guild_id'], state['start_utc']))]
    if not moments:
        raise SystemExit('No post-deployment eligible public Moment found; this is pending, not a pass.')
    for moment in moments:
        moment['contributions'] = [dict(r) for r in db.execute(
            'SELECT participant_key,contribution_gist,source_count,gist_version,lifecycle_status '
            'FROM memory_moment_contributions WHERE moment_id=?', (moment['moment_id'],))]
state['moment_ids'] = [m['moment_id'] for m in moments]
path.write_text(json.dumps(state, indent=2) + '\n')
Path(state['directory'], 'meaning_inspection.json').write_text(json.dumps(moments, indent=2) + '\n')
print(json.dumps(moments, indent=2))
PY
```

Match the bookmarked exchange to the actual conversation. If it is the wrong
exchange, preserve the output and resolve its ID before testing; do not infer
meaning from an unrelated record. `ready` plus a supported, distinct summary
and correctly attributed contributions establishes saved meaning for that
case. A generic summary, wrong attribution or unresolved failure status does
not pass.

For a bookmarked conversation in which 6 Bit participated, use **bnl-testing**
for one later recall request. Ask in ordinary language about the actual topic,
what each person contributed and what remained uncertain. Use the actual topic
and date to identify the conversation; do not paste the desired answer. If two
distinct cases were bookmarked, they can share this one small verification
batch. Keep the public event and sealed recall claims distinct.

After every answer, immediately preserve `!bnl debug last route` with the
complete reply and timestamp. The debug must correspond to that answer. If
there is no answer within two minutes, record the timeout and collect the
export below before sending another prompt. The limit is a capture procedure,
not a changed generation deadline.

## One read-only export after the recall batch

Run immediately after the last reply. This captures the bookmarked Moments,
their source membership, contributions, episodes and diagnostics, and the
last 15 minutes of packet/delivery receipts. It bounds every table and reports
truncation. It does not import the bot, run migrations or update any database.

```bash
cd /home/ubuntu/bnl01 && venv/bin/python - <<'PY'
import json, sqlite3, subprocess
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path
from time import monotonic

state = json.loads(Path('/tmp/bnl-moment-meaning-retest.json').read_text())
ids = state.get('moment_ids', [])
if not 1 <= len(ids) <= 2:
    raise SystemExit('Bookmark the intended public Moment first.')
now = datetime.now(timezone.utc)
end = now.strftime('%Y-%m-%d %H:%M:%S')
recent = (now - timedelta(minutes=15)).strftime('%Y-%m-%d %H:%M:%S')
marks = ','.join('?' for _ in ids)
report = {'deployment': state, 'end_utc': end, 'recall_since_utc': recent, 'tables': {}}
folder = Path(state['directory'])
with closing(sqlite3.connect('file:bnl01_conversations.db?mode=ro', uri=True, timeout=3)) as db:
    db.row_factory = sqlite3.Row
    db.execute('PRAGMA query_only=ON')
    deadline = monotonic() + 10
    db.set_progress_handler(lambda: int(monotonic() > deadline), 10000)
    db.execute('BEGIN')
    def collect(key, sql, params, limit):
        try:
            rows = [dict(r) for r in db.execute(sql + f' LIMIT {limit + 1}', params)]
            report['tables'][key] = {'rows': rows[:limit], 'truncated': len(rows) > limit}
        except sqlite3.Error as exc:
            report['tables'][key] = {'error_type': type(exc).__name__}
    for table, limit in (
        ('memory_moment_windows', 2), ('memory_moment_members', 64),
        ('memory_moment_participants', 16), ('memory_moment_contributions', 12),
        ('memory_moment_contribution_sources', 64), ('memory_moment_episode_moments', 16),
        ('memory_moment_diagnostics', 80),
    ):
        collect(table, f'SELECT * FROM {table} WHERE moment_id IN ({marks})', ids, limit)
    collect('original_ledger_sources',
        'SELECT entry.entry_id,entry.source_table,entry.source_row_id,entry.source_revision,'
        'entry.source_role,entry.subject_key,entry.normalized_value,entry.visibility,'
        'entry.lifecycle_status,entry.public_usable FROM memory_ledger_entries AS entry '
        'JOIN memory_moment_members AS member ON member.ledger_entry_id=entry.entry_id '
        f'WHERE member.moment_id IN ({marks}) AND entry.guild_id=?', ids + [state['guild_id']], 64)
    collect('derived_ledger',
        'SELECT entry.* FROM memory_ledger_entries AS entry JOIN memory_moment_windows AS win '
        'ON entry.entry_id=win.canonical_ledger_entry_id '
        f'WHERE win.moment_id IN ({marks}) AND entry.guild_id=?', ids + [state['guild_id']], 2)
    collect('episodes',
        'SELECT DISTINCT episode.* FROM memory_moment_episodes AS episode '
        'JOIN memory_moment_episode_moments AS link ON link.episode_id=episode.episode_id '
        f'WHERE link.moment_id IN ({marks}) AND episode.guild_id=?', ids + [state['guild_id']], 8)
    for table in ('memory_governance_intelligence_packet_runs',
                  'memory_governance_shared_brain_synthesis_runs',
                  'unified_response_assessment_shadow_runs'):
        collect(table, f'SELECT * FROM {table} WHERE guild_id=? '
            'AND julianday(created_at) BETWEEN julianday(?) AND julianday(?) '
            'ORDER BY created_at DESC', (state['guild_id'], recent, end), 40)
pattern = ('moment_meaning_|moment_engine_sweep|conversation_context_v2|'
           'ordinary_chat_single_packet|shared_brain_synthesis|source_revalidation|'
           'gemini_(model_attempt|generation_completed|model_retry)|'
           'model_generation_attempt|response_send_(succeeded|failed)|'
           'unified_response_assessment_shadow_recorded')
try:
    log = subprocess.check_output(['sudo', 'journalctl', '-u', 'bnl01', '--utc', '--no-pager',
        '-o', 'short-iso', '--since', state['start_utc'] + ' UTC', '--until', end + ' UTC',
        '--grep', pattern, '-n', '1501'], text=True, timeout=20)
    lines = log.splitlines()
    report['service_log_truncated'] = len(lines) > 1500
    Path(folder, 'service.log').write_text('\n'.join(lines[-1500:]) + '\n')
except (subprocess.SubprocessError, OSError) as exc:
    report['service_log_error_type'] = type(exc).__name__
report['current_commit'] = subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip()
report['current_pid'] = subprocess.check_output(
    ['systemctl', 'show', 'bnl01', '-p', 'MainPID', '--value'], text=True).strip()
Path(folder, 'receipts.json').write_text(json.dumps(report, indent=2) + '\n')
print(json.dumps(report, indent=2))
print('Return this capture folder with the replies and immediate route snapshots:', folder)
PY
```

Return the capture folder and the complete replies/route snapshots together in
the private recovery conversation. The candidate passes later recall only when
the correct saved meaning reaches the applied answer path, revalidates and is
used accurately in a delivered answer. A saved row, episode ID or an injected
memory flag alone is insufficient. Missing receipts, errors and truncation are
evidence gaps. Record actual physical attempts, latency, priced cost and any
generation failure alongside semantic quality.

The next assistant reviews this bounded capture, preserves earned passes and
updates the maintained source pack. F02–F07 and pending natural observations
retain their existing dispositions; this candidate does not certify them.
