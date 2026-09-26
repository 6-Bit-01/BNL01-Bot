# Batch memory reads and reply delivery — 2026-09-26

The VPS evidence confirms PR 596 is deployed at `661cd98`, with service PID
1798488 started at 21:36:03 UTC. This repair addresses the next reply failure
and the disappearing typing indicator. Production recovery still needs the
post-merge acceptance below.

## What the supplied log proves

| UTC | Evidence | Meaning |
| --- | --- | --- |
| 21:36:26.405 | Capture took 16,906 ms, including 16,222 ms Ledger/Moment | Capture remains a separate latency problem. |
| 21:36:34.724 | Effective answer decision in `sealed_test` | The request was accepted for an answer. |
| 21:36:34.905 | Typing started | Source preparation had begun. |
| After 21:36:45.498 | Main-loop stacks in `build_user_memory_context` → governance → `_moment_counts` → `_table_exists` | Synchronous SQLite reads blocked Discord's event loop. |
| 21:36:59.654 | Batch generation started; 33.25 seconds since batch start | This event precedes the actual provider call. |
| 21:37:04.659 | Typing stopped with `generation_exit` | The request exited before any recorded chat provider call or send. |
| 21:37:10.570 | `ambient_generation` succeeded | This was background generation, not the missing chat reply. |

The filter omitted exception headers and messages. Untimestamped queue-sync
lock stacks came from a ten-minute window spanning the restart; they cannot
all be classified as post-deployment failures. The exact live exception is
unconfirmed.

## Reproduced failures and repair

- The batch path read `guild_configs` five times to supply an argument that
  `conversation_surface_for_channel_policy` ignores. One read occurred directly
  between the last generation-start log and provider dispatch. A SQLite error
  there aborts the whole reply. Surface selection now uses the already resolved
  channel policy. Actual ingress routing and authorization are unchanged.
- Single-speaker memory assembly ran on Discord's event loop and could perform
  schema setup and diagnostic writes. It now calls the same memory owner in an
  off-loop, read-only snapshot using the existing bounded member connection.
  Source metadata is published only after the worker completes successfully.
  A busy snapshot is marked unavailable; it is not reported as nonexistent
  memory, and other available sources remain usable by normal generation.
- Read-only selection still reached Moment schema setup. A SQL trace found 85
  schema statements in one governed read. The two existing Moment readers now
  support the same `prepare_schema=False` convention as the situation reader.
  Governance passes its existing initialization policy through. The real
  contention test retrieves identical governed text and metadata with a writer
  reservation held, without DDL or data writes and without private evidence.
- Multi-speaker Moment attribution now also reads off-loop and closes its
  read-only connection. Speaker selection, attribution targets, visibility,
  source validation and runtime gates retain their existing owners.
- Typing previously stopped at `response_ready`, before quote/source/frame
  checks and possible corrective generation. It now remains active through
  those checks and Discord delivery. Existing interrupt, failure and
  cancellation cleanup still stops it. Failed sends do not persist an answer.
- Unexpected batch exceptions now log `batch_response_failed` with stage,
  error class, function and line. Arbitrary exception messages and source text
  are excluded from that diagnostic. Exceptions still propagate.

No source owner, public-memory gate, quote authority, budget guard, no-store
boundary or stale-response check was removed. No additional model call or
background memory store was introduced. In-memory memory diagnostics remain;
prompt reads no longer write governance shadow receipts.

## Verification

The original three regression tests reproduced the settings-read abort,
event-loop blockage and premature typing shutdown before the change. Additional
tests cover read-only evidence parity under a real SQLite writer reservation,
connection closure with a retained lock exception, unavailable-memory reporting,
generation failures, Discord send failures and cancellation. Existing tests
cover hard-interrupt regeneration, source revalidation, attribution, sealed
continuity and public/private memory boundaries.

Provider, website and Discord transports are mocked in simulations. These
checks validate routing, evidence assembly and delivery mechanics; they do not
measure Gemini interpretation quality or production response time.

- Focused owner/batching suite: 457 tests passed in 25.838 seconds.
- The updated pre-send source-mutation race passed separately in 0.757 seconds.
- `make check PYTHON=.venv/bin/python`: 3,470 tests passed in 187.628 seconds.
- Changed Python files parse with Python 3.9 grammar; `git diff --check` is clean.
- Owner identity rules are preserved; added fixtures use fictional members.

The pull request records remote parent/blob/tree verification and CI status.

## Deployment and exact acceptance

After merging, run on the existing VPS:

```bash
cd /home/ubuntu/bnl01 &&
git pull --ff-only origin main &&
sudo systemctl restart bnl01 &&
systemctl is-active bnl01
```

In `bnl-testing`, send these separately, waiting for the first reply:

1. BNL, what stood out about how people interacted at the September 25, 2026 BARCODE Radio show?
2. What made you think that? Quote a few actual comments and identify the speakers.

Then collect this once. Filtering begins at this process's start so earlier
service failures are not mixed with this acceptance run:

```bash
cd /home/ubuntu/bnl01 &&
git rev-parse --short HEAD &&
systemctl show bnl01 -p MainPID -p ExecMainStartTimestamp &&
sudo journalctl -u bnl01 \
  --since "$(systemctl show bnl01 -p ExecMainStartTimestamp --value)" \
  --no-pager -o cat |
  grep -E 'batch_response_failed |batch_member_memory_read_failed |batch_typing_indicator_|response_stage_timing |conversation_orchestration_decision |gemini_generation_|model_generation_attempt |prompt_source_revalidation |response_send_(succeeded|failed) |response_suppressed_no_fallback |batch_response_persistence_skipped |\[batch:'
```

Expected evidence: each turn reaches its chat provider and
`response_send_succeeded`; typing ends with `response_sent` after delivery;
the follow-up uses original attributed show comments; no
`batch_response_failed` occurs. Return the visible replies with this output.
The command does not export source text or credentials. Do not wait twenty
minutes on a missing reply; a minute without delivery is enough to collect it.

The 16.2-second Ledger/Moment capture, wider retrieval and semantic Moment
audit, and paused private-art integration remain open. This patch does not
certify the entire shared brain or promise a particular production latency.
