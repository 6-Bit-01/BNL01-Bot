# Shared-context recovery: consolidated candidate and one retest batch

This continues the original shared-brain recovery acceptance plan. The owner
finished the private collection block, including public activation rollback and
restoration. Related repairs are collected in one candidate; production still
requires review, merge, deployment, and the focused checks below.

## Accepted evidence carried forward

- Public activation rollback: `public_effective` changed true -> false -> true.
  Private scope and prerequisites stayed true. The exact starting configuration
  was restored; the final receipt was `RESTORED_CONFIGURATION_MATCHES_START`.
- The general name/spacing repair is deployed at `70988e5707a67387ba2f661aad67e18f7a3abec0`.
  The Lost Marbles / Leo Rose two-person reply used both requests and delivered
  successfully with one 7.663-second provider call. This is not a member-specific rule.
- The September 11 private playback discussion formed a qualifying shared
  Moment with two human participants. Formation itself is accepted.
- Earlier accepted quote/date/source checks, screenshot correction, immediate
  interruption/resume, historical canary rollback, and scoped Journal, Relay
  deadline, and website checks remain accepted within their recorded scope.

## What this candidate repairs

| Observed defect | Existing owner changed | Required live result |
| --- | --- | --- |
| A broad TikTok/Discord song request selected only September 4 before generation | Shared show scope selection and website adapter | Broad history reaches multiple retained shows; explicit single-show requests remain scoped |
| Retained transcripts were present but outside Context v2's active recency window | Moment references feeding Context v2 | A uniquely matched retained occurrence supplies original source rows, without reopening or promoting it |
| `resume_target_unresolved` discarded all context as if the person were ambiguous | Intelligence Packet subject resolution | Valid conversation evidence survives; an unresolved episode or person is never invented |
| Delivered website no-store replies skipped synthesis finalization and assessment receipts | Existing batch send lifecycle | Successful delivery finalizes its receipt exactly once while response-text storage remains disabled |

The show path uses the existing retained-history scope (at most eight selected
shows for broad undated queries), compact per-show examples, and original source
identities. It does not load every full transcript into every prompt. Explicit
original-quote and screenshot lookup behavior stays with its existing owner.

Retained continuation resolves at most 32 qualified Moment candidates within the
existing 30-day reopening horizon and at most 32 original rows. It requires one
matching occurrence, the requesting participant, the exact channel/policy/route,
and valid original sources. Dated transcript requests use the stored UTC clock.
Conflicting occurrences remain unresolved. Revalidation checks source lifecycle,
correction lineage, original-row changes/deletion, and scope before delivery.
This is bounded retrieval, not universal semantic recall or a retention-policy change.

The source selectors are general. No song, member, playback phrase, fabricated
public fixture, qualification exception, new memory store, or extra provider
loop is introduced. The current refresh/ingestion owners continue to supply
background information. Broad awareness across every consumer remains subject
to the original acceptance requirements.

Local validation: 139 focused regressions passed, followed by the required
`make check PYTHON=python` gate: all 2,992 repository tests passed. Live results
for this candidate are still pending the deployment and batch below.

## Run this after this candidate has been merged

Run the deployment block once on the VPS:

```bash
cd /home/ubuntu/bnl01 || exit 1
git pull --ff-only origin main || exit 1
venv/bin/python -m py_compile bnl01_bot.py bnl_tiktok_show_ledger.py bnl_moment_engine.py bnl_conversation_context_v2.py bnl_unified_intelligence_packet.py || exit 1
sudo systemctl restart bnl01 || exit 1
sudo systemctl is-active --quiet bnl01 || exit 1
git rev-parse HEAD
export BNL_RECOVERY_CAPTURE_DIR="$(mktemp -d /tmp/bnl-recovery-retest.XXXXXX)"
export BNL_RECOVERY_START="$(date -u '+%Y-%m-%d %H:%M:%S')"
printf 'Capture folder: %s\nStart UTC: %s\n' "$BNL_RECOVERY_CAPTURE_DIR" "$BNL_RECOVERY_START"
```

Keep that SSH shell open. If deployment fails, stop before Discord testing.
These four prompts are sequential, all in **bnl-testing**, sent by **6 Bit**.
Miss Bit does not need to repeat the already-recorded discussion. No BNL prefix
or mention is needed in this free-speak test channel.

After each complete answer, send `!bnl debug last route` immediately, preserve
that output with the answer and timestamp, then proceed to the next prompt.
Check that debug names `bnl-testing` and `sealed_test`. If the answer is wrong,
record it and continue through the independent cases. If no answer arrives
within two minutes, record that timeout and stop sending more prompts into the
possibly still-running generation; collect the same end-of-batch export below.

### 1. Retained source recall after restart

```text
Let's return to our playback correction discussion from September 10, 2026 (UTC). What distinction did I ask you to preserve, and what was still unknown?
```

Expected: separates acknowledging an error from changing a saved record, and
keeps playback/saved-record change unconfirmed where evidence was absent. Logs
must show the retained original source references. A claim that a database record
was changed or permanently locked is not supported by this conversation.

Then:

```text
!bnl debug last route
```

### 2. Two-person continuation from retained sources

```text
Return to the playback correction discussion that Miss Bit joined on September 11, 2026 (UTC). What did each of us contribute, and what remains unresolved?
```

Expected: correct contributions by each original speaker and the unresolved
playback/saved-record evidence. We inspect episode links as separate evidence;
if the old active window has since expired, that is not a recall failure.

Then:

```text
!bnl debug last route
```

### 3. Broad source use in the song request

```text
Let's make a song based on your TikTok chat and discord data, based on all you can see like recurring topics or behaviors. No less than 1400 characters of lyrics, and for the style prompt choose a year between 1974 and 2008 and combine 2 styles that don't normally belong together.
```

Expected: multiple available shows/surfaces support the answer, at least 1,400
lyric characters, one chosen year in the range, and two styles. An isolated
utterance must not become a claimed recurring behavior. The year/length checks
are still live generation acceptance; this candidate adds no bespoke lyric validator.

Then:

```text
!bnl debug last route
```

### 4. Mixed publication, history, and current state

```text
What recurring topics connect the retained shows and the latest published Journal, and what does the current public queue show? Keep historical observations separate from the current queue state.
```

Expected: the published Journal and relevant retained show history inform their
respective parts; the current website queue supplies current state. Missing
eligible evidence should be identified specifically. Delivery must have its
final receipt even when the response is intentionally not saved as conversation.

Then:

```text
!bnl debug last route
```

## One VPS export after the four cases

Run this in the same SSH shell. It reads the logs and database; it changes no
source records or configuration. No fixed UTC cutoff or current-PID-only filter
is used.

```bash
export BNL_RECOVERY_END="$(date -u '+%Y-%m-%d %H:%M:%S')"
: "${BNL_RECOVERY_START:?Use the shell containing the recorded start time}"
: "${BNL_RECOVERY_CAPTURE_DIR:?Use the shell containing the capture folder}"
sudo journalctl -u bnl01 --utc --no-pager -o short-iso \
  --since "$BNL_RECOVERY_START UTC" --until "$BNL_RECOVERY_END UTC" \
  --grep='conversation_context_v2|situation_frame|show_episode_evidence_context|tiktok_show_analysis|bnl_read_model_context_loaded|shared_brain_synthesis|ordinary_chat_single_packet|gemini_(model_attempt|generation_completed|model_retry)|model_generation_attempt|response_send_(succeeded|failed)|source_revalidation|frame_revalidation|batch_response_persistence_skipped|unified_response_assessment_shadow_recorded|\[batch:(active_packet_decision|generation_started_after_wait|response_send_commit_complete|active_packet_completion_regenerated)\]' \
  | tee "$BNL_RECOVERY_CAPTURE_DIR/service.log"
venv/bin/python - <<'PY'
import json, os, sqlite3
from contextlib import closing
from pathlib import Path
from time import monotonic

start, end = os.environ['BNL_RECOVERY_START'], os.environ['BNL_RECOVERY_END']
queries = {
    'assessments': ('unified_response_assessment_shadow_runs',
        'run_id,created_at,current_speaker_count,participant_count,'
        'source_basis_kinds_json,prompt_lanes_json,source_basis_changed_before_send,response_sent'),
    'packets': ('memory_governance_intelligence_packet_runs',
        'run_id,created_at,item_count,selected_lane_counts_json,excluded_by_reason_json,'
        'subject_resolution_status,episode_query_status,revalidation_status,prompt_applied,live_applied'),
    'delivery': ('memory_governance_shared_brain_synthesis_runs',
        'run_id,packet_run_id,created_at,provider_call_count,corrective_call_count,'
        'source_revalidation_status,prompt_applied,live_applied,response_sent,guard_status'),
}
report = {'start_utc': start, 'end_utc': end}
with closing(sqlite3.connect('file:bnl01_conversations.db?mode=ro', uri=True, timeout=3)) as db:
    db.row_factory = sqlite3.Row
    db.execute('PRAGMA query_only=ON')
    deadline = monotonic() + 5
    db.set_progress_handler(lambda: int(monotonic() > deadline), 10000)
    db.execute('BEGIN')
    for name, (table, columns) in queries.items():
        try:
            rows = [dict(r) for r in db.execute(
                f'SELECT {columns} FROM {table} WHERE guild_id=? AND channel_policy=? '
                'AND julianday(created_at) BETWEEN julianday(?) AND julianday(?) '
                'ORDER BY created_at LIMIT 26',
                (1288269405209235551, 'sealed_test', start, end))]
            report[name] = {'rows': rows[:25], 'truncated': len(rows) > 25}
        except sqlite3.Error as exc:
            report[name] = {'error': str(exc)}
output = json.dumps(report, indent=2)
Path(os.environ['BNL_RECOVERY_CAPTURE_DIR'], 'receipts.json').write_text(output + '\n')
print(output)
PY
```

**Return once:** send all four answers, their four immediate debug outputs, the
printed deployed commit, and both export outputs/files together. An empty result,
an error, or a truncation flag is incomplete evidence, never a pass. A missing
single-packet receipt on a route that uses the established non-packet owner is
not by itself a failure; physical calls and delivered replies must be correlated.

## Remaining plan after this batch

1. Review this batch as a whole. Preserve passes and group any demonstrated,
   related defects before the next repair/deployment; no per-message PR loop.
2. Keep original public/community/show/Ambient behavior, public tier growth,
   elapsed-time recurrence/retention, and applicable natural source changes on
   the original acceptance list. Sealed conversation cannot certify public growth.
3. The old 119/112-second provider stall, the song's two physical calls, and the
   two historical Relay rejections remain unclassified where their missing
   evidence prevents a sound diagnosis. No speculative timeout/guard repair is
   included here. Log successful and failed attempts in the same retest export.
4. Final owner acceptance follows the reconciled evidence. This candidate and a
   successful rollback do not independently certify the entire shared brain.
