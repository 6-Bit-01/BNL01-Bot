# Shared-brain C8 consumers and C9 interrupted-attempt recovery

This continues the locked September 23 completion matrix after PR575, merged at
`836945088484cb6ef4e037e3168119e44ddec85e`. All three PR575 CI jobs passed.
The scope is the existing Ambient consumer, connection checks for relationship
tone and approved SourceFiles, and the documented abandoned Moment meaning
claim. Earlier accepted evidence remains credited.

## Demonstrated gaps and changes

Five behavioral tests fail on the merged parent: Ambient accepts unlinked or
ineligible memory, retains an edited original, misses a correction control,
retains an expired live observation, and the real Moment sweep leaves a
persisted abandoned claim generating. The same tests pass on this branch.

Ambient now carries a transient basis for the exact public conversation rows,
selected linked short/medium/long traces, and eligible cleaned broadcast-memory
summaries supplied to its prompt. The existing correction/forget reader also
checks tier projections. Original conversation ownership, public eligibility,
and complete tier lineage remain required. An unrelated newly arriving message
does not change a selected original. Core flags and unlinked legacy summaries
do not become public authority; this change does not infer or backfill lineage.

Reads run off the event loop with bounded SQLite waits. Selection remains at
most 20 room messages, six curiosity participants, 100 candidate tiers per
participant, one trace per tier, four rendered cues and three broadcast summaries
from 25 candidates. Each eligible trace must have 1–32 exact public human source
rows; larger/incomplete traces are omitted, never partially represented as fully
validated. Raw operator notes are never read by this consumer. Broadcast expiry,
clarification, supersession, visibility and exact usage scope are enforced.

After each existing generation attempt, and again at the scheduler's send
boundary, source changes or unavailable required evidence discard the draft.
The destination policy is checked again before sending. This does not add a
provider retry. Existing shape/similarity retries cannot bypass revalidation.
Website production observations and independent TikTok freshness are rechecked;
a fresh same-state heartbeat is compatible, while a state change or expiry is
not. Schedule time still cannot establish a live or ended broadcast.

The existing Moment sweep marks an abandoned `generating` claim `interrupted`
after ten minutes, beyond the provider's four-minute maximum transport timeout.
It processes at most 100 per sweep under the existing shadow gates, keeps the
original Moment and contributions, and rejects a late result. No attempt is
requeued and no provider call is made by recovery. Recent claims remain intact;
malformed timestamps cannot leave an attempt stuck forever. No migration, new
store, scheduler, activation gate, production write or automatic replay is added.

## Consumer connection evidence

| Consumer | Actual boundary exercised | Authority retained |
|---|---|---|
| Ambient | Real prompt assembly, provider-return checks and scheduler `channel.send` | Public original rows and linked summaries; current show observations keep their separate meanings |
| Relationship tone | Existing memory reader through actual ordinary and single-packet prompt assembly, gate off/on | Private tone guidance only; source events and numerical scores do not enter the prompt |
| SourceFiles | Authorized direct lookup through actual operator prompt assembly | Internal working case-file labels and public-safety notes; public, unauthorized and automated routes remain excluded |
| Website | Ambient reader, read-model recovery and source-contract tests | Public scope and freshness; private fields never enter public Ambient context |
| Moment recovery | Committed claim, reopened SQLite database and actual periodic sweep | Original representation retained; terminal outcome with no replay or late overwrite |

Focused checks include 28 new regressions. The required full-suite
result and exact tested/published tree are recorded in the publication receipt.
Provider and delivery transports use fixtures; this establishes application
behavior, not live model prose or VPS deployment. `ambient_source_read` and
`ambient_source_check` report content-free read/validation durations; existing
`gemini_generation_completed` and Moment attempt diagnostics retain provider
timing/accounting. `ambient_delivery` records transport duration and whether the
send returned confirmation. Live acceptance should retain these for the same turn.

Ambient keeps its existing ephemeral scheduling and delivery semantics. A failed
send is not logged as a success or retried inside that cycle. It does not gain a
durable owed-draft queue or an exactly-once guarantee across an ambiguous Discord
acknowledgment or hard exit. Journal and Relay retain their separately tested
saved-payload, owed-work and idempotent recovery owners.

## Exact remaining completion conditions

1. Verify this package's merge/CI and the deployed bot revision, preserving the
   current authorized gate configuration. A merge alone is not a deployment.
2. Run the one combined sealed C2 batch against that deployed revision: recap
   to actual comments; person/topic to exact words; correction/new person/new
   topic; recent-show scope; mixed historical/current operations. Match original
   sources, assembled prompts and delivered answers in the same receipt.
3. Carry C3 natural meaning, individual contributions, continuation/reopening,
   aging/later recall and corrections forward as named observations. Preserve
   prior clock, formation, privacy, restart and rollback passes.
4. Observe the next eligible natural Journal, Relay and Ambient occurrences,
   including released Ballad metadata when selected. Lyrics/generated prose are
   not independent factual witnesses. A natural event that has not occurred is
   pending evidence, not a reason to restart the broad audit.

No ad hoc user test batch, public rehearsal, synthetic memory seed or new feature
work is required before this package is merged and deployment is established.
Website item 3 remains paused.
