# BNL-01 v2 Shadow Acceptance and Rollback

This document is the operator contract for evaluating BNL-01's existing v2
memory and relationship systems in shadow mode. It does not authorize a live
cutover, and the implementation that reports this evidence must not enable or
change an environment variable.

## Scope and non-goals

This acceptance pass is read-only with respect to live behavior. It aggregates
privacy-safe counters already produced by the shadow systems so the owner can
decide whether each layer has collected enough trustworthy evidence to proceed
to the next shadow layer.

This pass explicitly does **not**:

- enable a shadow or live gate;
- replace legacy prompt context or change a Discord response;
- activate proactive engagement or relationship behavior;
- make an automatic cutover decision;
- change the queue, website, Journal, Relay, dossiers, or Source Files;
- publish, edit, or delete member-facing content; or
- delete or rewrite shadow tables during rollback.

The only successful terminal state is `ready_for_owner_review_not_live_cutover`.
That state means the aggregate evidence is ready for a human decision. It does
not mean any live gate may be enabled.

## Deployment and data-safety preflight

The memory tables share the production SQLite database with conversations and
other bot state. Before deploying a revision that changes memory, Moment,
governance, relationship, correction, or deletion code, create and verify a
point-in-time SQLite backup. A successful test run or Git pull is not a
substitute for this backup.

From `/home/ubuntu/bnl01`, before restarting into the new revision:

```bash
mkdir -p backups/pre-memory
backup_stamp="$(date -u +%Y%m%dT%H%M%SZ)"
backup_path="backups/pre-memory/bnl01_conversations-${backup_stamp}.db"
python3 - "${backup_path}" <<'PY'
import sqlite3
import sys

source = sqlite3.connect(
    "file:bnl01_conversations.db?mode=ro",
    uri=True,
    timeout=5,
)
backup = sqlite3.connect(sys.argv[1])
try:
    source.backup(backup)
    result = backup.execute("PRAGMA quick_check").fetchone()[0]
    if result != "ok":
        raise SystemExit("backup quick_check failed: " + str(result))
finally:
    backup.close()
    source.close()
PY
sha256sum "${backup_path}" > "${backup_path}.sha256"
```

Record the backup path, checksum, pre-deploy Git SHA, and intended new Git SHA.
Do not reuse the Relay-only export as the database backup; it intentionally
contains only accepted Relay history. Do not copy the database into the
repository or commit it.

After deployment, confirm:

1. `git rev-parse HEAD` is the intended merge SHA;
2. `sudo systemctl status bnl01 --no-pager -l` is healthy;
3. `/bnl_memory_check` completes without a database/report error;
4. all three global v2 live gates remain off; and
5. existing conversation, Journal, Relay, Ambient, queue, Source File, and
   dossier behavior has not changed unexpectedly.

If startup, schema work, or the diagnostic fails, stop at rollback. Do not
toggle another memory gate in an attempt to repair the deployment.

## Additive schema, migration, and reconciliation contract

Memory schema preparation is additive. It creates missing tables and indexes,
adds missing columns, and records one-time Moment migration keys. It does not
drop the legacy conversation or memory owners, rename production tables, or
declare v2 authoritative.

The candidate also adds `conversation_response_participants`, an authoritative
mapping from one shared BNL room reply to the members that reply addressed.
That mapping is independent of the Ledger shadow gate: context assembly uses it
to keep speakers distinct, and `/clearhistory`, complete deletion, guild clear,
and pruning remove the shared reply and its mappings together. It does not turn
a group reply into a private memory for every participant.

The Moment-specific passes run when the Moment schema is first ensured, not
merely because Git was updated. If Moment shadow is disabled and no Moment
operation runs, deployment alone may leave those migration keys unapplied.
Report that as “not run,” not as a successful zero-row migration.

The current migration work has three conservative data paths:

- obsolete durable `remembered_number` artifacts are quarantined or retracted;
- older extractive or otherwise unsafe Moment projections are emptied,
  made non-public, and quarantined/retracted while their audit lineage is
  retained; and
- participant contribution gists are backfilled only for already-finalized,
  public-usable Moments whose current source rows still pass scope, lifecycle,
  visibility, and gist-safety checks.

Migration keys make these passes idempotent. A partial first pass may safely be
retried; already-safe rows are not promoted merely because the migration ran.
Rows that cannot be proven safe remain absent, quarantined, retracted, or
`needs_review`. Never hand-edit them to `active` to increase acceptance counts.

Startup reconciliation treats the original conversation row as the authority
for raw conversation Ledger evidence. If that source is gone, the orphaned
Ledger copy, participant links, lineage, receipts, contribution sources, and
dependent Moment projection are purged or retracted before they can be used.
Correction, forget, and complete-delete paths likewise invalidate or remove
dependent contribution gists and leave surviving multi-person Moments
`needs_review` rather than silently rewriting them.

These migrations and reconciliations are correctness work, not evidence of
successful live behavior. Compare pre/post diagnostic counts, investigate every
new quarantine or `needs_review` total, and preserve the verified backup until
the owner accepts the observation period.

## Owner diagnostic

Run `/bnl_memory_check` in Discord as the configured owner. Its existing
owner-only, ephemeral response now includes a **V2 Shadow Acceptance / Rollback
Evidence** section. The acceptance reader performs aggregate `SELECT` queries
only: it does not create a schema, write an evidence row, set a gate, or send a
normal Discord response. Each stage includes its retained evidence window so
the owner can compare snapshots before and after a controlled test period.

## Conversation Context v2 is a separate preflight

Conversation Context v2 is already an immediate-turn continuity layer. It is
not part of the durable shadow dependency chain below and must not be presented
as a fifth shadow stage.

Before collecting shadow-stack evidence, run the sealed continuity check in
`#bnl-testing`:

1. Send `remember this number: 8` and wait for BNL-01's reply.
2. Send `what number did i tell you to remember?`.
3. Confirm the answer directly identifies `8` without archive, database,
   dossier, or "records indicate" framing.
4. Immediately inspect the owner-only context diagnostic. It should report the
   `sealed_test` policy, at least one same-room paired turn, no cross-channel
   pair, and no current-message duplication or protected-context leakage.

If this preflight fails, set `BNL_CONVERSATION_CONTEXT_V2_ENABLED=off`, restart
the bot, and confirm the diagnostic reason is `rollback_disabled`. This rollback
is independent of all memory and relationship shadow gates.

## Required shadow order

Evaluate one layer at a time in this exact order:

1. Unified Memory Ledger
2. Moment Engine
3. Memory Governance
4. Relationship v2

Do not skip a layer. Do not start a later layer while an earlier layer is
blocked, missing evidence, or still under owner review.

| Stage | Shadow gate | Required earlier shadow gates |
| --- | --- | --- |
| Ledger | `BNL_MEMORY_LEDGER_SHADOW_ENABLED` | none |
| Moments | `BNL_MOMENT_ENGINE_SHADOW_ENABLED` | Ledger |
| Governance | `BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED` | Ledger and Moments |
| Relationship | `BNL_RELATIONSHIP_V2_SHADOW_ENABLED` | Ledger, Moments, and Governance |

All live gates must remain off throughout the entire acceptance pass:

- `BNL_MEMORY_GOVERNANCE_LIVE_ENABLED`
- `BNL_RELATIONSHIP_V2_LIVE_ENABLED`
- `BNL_ACTIVE_ENGAGEMENT_V2_LIVE_ENABLED`

Enabling any live gate blocks acceptance immediately. The reporting code does
not set these variables and does not provide an automatic activation path.

## Moment gist response canary

`BNL_MOMENT_GIST_CANARY_ENABLED` is a separate, response-changing canary. It is
not part of the shadow acceptance pass above and defaults off. Do not enable it
until the Ledger and Moment shadow stages have acceptable evidence.

The canary fails closed unless all of the following are true:

- `BNL_MEMORY_LEDGER_SHADOW_ENABLED=true`;
- `BNL_MOMENT_ENGINE_SHADOW_ENABLED=true`;
- `BNL_MOMENT_GIST_CANARY_ENABLED=true`;
- the guild is listed in `BNL_MOMENT_GIST_CANARY_GUILD_IDS`;
- the requesting member is listed in `BNL_MOMENT_GIST_CANARY_USER_IDS`;
- the request is a direct normal-chat or direct-payload turn; and
- the route is `public_home` or `public_context`.

The canary adds attributed, meaning-only continuity from eligible public
Moments. It does not enable Memory Governance, Relationship v2, or Active
Engagement v2. Ordinary Moment continuity remains scoped to a participating
member. An explicit “what did this member mean?” request may retrieve that
uniquely resolved member's public contribution gist for an allowlisted
requester in the same guild, even if the requester was not part of the original
Moment.

Moment summaries and participant contribution gists are paraphrase authority
only. They never authorize exact wording or settle a dispute. A consequential
exact-quote request has a separate fail-closed path limited to a typed target's
still-present raw human message from the same public room, with a Discord
message ID and a maximum age of 45 minutes. If that source cannot be resolved
and revalidated, BNL may give a clearly labeled grounded gist but must not claim
or reconstruct exact wording.

Rollback is environment-only: set `BNL_MOMENT_GIST_CANARY_ENABLED=false` and
restart the bot. Leave the accepted Ledger and Moment shadow layers running for
diagnosis unless their own evidence requires rollback.

### Canary entry criteria

Do not enable the canary merely because the code is merged. All of the
following must be true:

1. the exact deployed SHA and verified pre-deploy database backup are recorded;
2. the complete test suite and CI are green for that SHA;
3. Ledger and Moment shadow evidence have representative human-authored public
   sources, finalized Moments, participant attribution, and no hard blocker;
4. correction, forget, source deletion, complete deletion, orphan
   reconciliation, and source-change-before-send tests pass;
5. the three global v2 live gates are off;
6. exactly one intended test guild and one or more consenting test members are
   selected for the allowlists; and
7. the owner has captured a baseline `/bnl_memory_check` snapshot.

Then set the canary flag and both allowlists explicitly, restart once, and
confirm the configuration reports a fully scoped canary. Do not broaden the
allowlists during the same observation period.

### Canary acceptance and exit criteria

Exercise ordinary same-member continuity and an explicit, uniquely targeted
public gist request. The canary is acceptable only when:

- BNL uses a useful paraphrase without reconstructing exact wording;
- the participant and guild are correct;
- a nonparticipant, non-allowlisted member, private/sealed source, ineligible
  route, ambiguous target, or missing source fails closed;
- an edit, correction, forget, deletion, or source change before send removes
  stale authority;
- exact-quote behavior remains on its independent current-room live-source
  path; and
- no response, provider, Discord, or database regression appears in adjacent
  conversation behavior.

End every planned canary observation by setting
`BNL_MOMENT_GIST_CANARY_ENABLED=false`, restarting, and verifying that Moment
gist no longer reaches prompts. Preserve the shadow evidence for review. A
successful canary authorizes only the next owner decision; it does not authorize
Memory Governance, Relationship v2, or Active Engagement v2.

## Aggregate acceptance evidence

The owner-only acceptance snapshot is aggregate and content-free. It may report
gate states, stage states, counts, reason-code totals, and rollback metadata. It
must not expose message text, member names, Discord IDs, memory contents, or
correlatable subject hashes.

The top-level report includes:

- contract version and evaluation order;
- gate snapshot;
- evidence-observed state for each layer;
- first/last timestamps for retained evidence windows;
- blockers and warnings;
- the four layer reports plus the separate Conversation Context v2 diagnostic;
- whether legacy production truth remains preserved;
- whether an environment-only rollback is available;
- `ownerReviewRequired=true`; and
- `automaticCutoverAllowed=false`.

### Ledger evidence

Review at least these aggregate fields:

- eligible legacy write receipts;
- inserted ledger entries;
- exact-source deduplications;
- skipped writes by reason;
- shadow write errors;
- source-lane, entry-type, visibility, and lifecycle counts;
- missing or unmapped provenance;
- multiple-active-value counts;
- dangling lineage targets; and
- legacy-to-ledger parity mismatch count.

`legacyToLedgerParityMismatches` is **informational**, not a standalone pass/fail
gate. The ledger deliberately contains derived, projection, and review-only rows
that do not have a one-to-one legacy-row equivalent. A nonzero total must be
reviewed alongside its component counters; it must not be treated as automatic
proof of data loss or used to trigger an automatic cutover or rollback.

Ledger evidence is sufficient for owner review only after representative writes
and exact-source deduplication have been observed, shadow writes remain
non-fatal, and no hard stop condition below is present.

### Moment evidence

Review at least:

- eligible ledger entries observed;
- open, finalized, rejected, and correction-review windows;
- rejection reasons and qualification types;
- participant and finalization aggregates;
- processing errors;
- duplicate memberships;
- BNL-only finalized moments;
- cross-guild, cross-channel, and incompatible-visibility violations; and
- dangling lineage targets.

Open windows are collecting evidence, not failures. After the inactivity window,
controlled coherent input should produce at least one finalized moment, while
low-signal or isolated input should be rejected rather than promoted.

### Governance evidence

Review persisted, privacy-safe aggregates for:

- run count, selected count, and zero-selection runs;
- exclusions by reason;
- processing errors;
- same-as-legacy and different-from-legacy hash counts;
- selected counts by source class;
- invalid-invariant names and counts;
- fallback-reason counts;
- visibility, token-budget, duplicate, and contradiction counters; and
- Moment candidate and needs-review counters.

Zero selections and exclusions can be correct fail-closed outcomes. They are not
failures by themselves. Governance live must remain off, so the legacy result
continues to be the actual production result while shadow candidates are
measured.

Retained v1 rows may contain `invalid_route_channel_policy_selected`. The v1
writer emitted that label when a mismatched route or restricted channel-policy
candidate was safely excluded *before* selection. The v2 acceptance reader
preserves a historical count under `legacy_reclassified_exclusions` only when
the same retained run contains a matching `invalid_route_channel_policy`
exclusion. It shows a review warning but does not treat that corroborated count
as a selected-invariant failure. An unmatched count, any remainder, and every
other invalid-invariant label continue to fail closed as hard stops. No retained
row is rewritten or deleted.

Shadow rows preserve the raw aggregate marker and exclusion counts. The
acceptance reader performs the compatibility reclassification exactly once at
report time, so a one-marker/one-exclusion row is safely reclassified while a
two-marker/one-exclusion row retains one hard blocker. The conservative
in-process runtime marker and live-governance fallback remain unchanged;
revising that dormant behavior requires a separate reviewed change.

### Relationship evidence

Review at least:

- eligible directed user-authored events;
- rejected or unclassified passive, sealed, or ambiguous evidence;
- model-audit events;
- events by type and lifecycle exclusions;
- moment-linked relationship events;
- visibility, ledger-link, moment-link, and cross-guild violations;
- policy-eligible shadow candidates;
- withheld reasons, including opt-out, privacy, cooldown, and rivalry reasons;
- processing errors; and
- actual live emissions.

The current Relationship v2 shadow writer records the legacy-comparison field
as `not_collected`. The owner diagnostic displays that limitation explicitly as
a warning; it must not imply that a legacy/v2 comparison has occurred. Because
there is no equivalent legacy proactive-relationship planner to compare against,
this field is transparent context for owner review rather than an invented
pass/fail metric.

Model output must not create positive relationship weight. Member opt-out and
boundary evidence must be honored. Shadow candidates may be recorded, but
`actual_live_emissions` must remain exactly zero.

## Status meanings

- `collecting_shadow_evidence`: one or more shadow gates are off, prerequisites
  are incomplete, or representative evidence has not yet been observed.
- `blocked_live_authority_detected`: at least one live gate is on. Stop.
- `blocked_shadow_invariant_failure`: a hard invariant or report failed. Stop.
- `ready_for_owner_review_not_live_cutover`: all four shadow stages have evidence
  and no hard blocker is present. Owner review is still required.

Warnings remain visible for owner review and never authorize cutover. In
particular, parity mismatches, unmapped provenance, multiple active values,
and correction-review work must be interpreted from their component evidence
rather than collapsed into a false automatic decision. Dangling lineage is a
hard stop.

## Exact stop conditions

Stop the current stage and do not enable the next one if any of the following is
observed:

- any live gate is enabled;
- the bot becomes unstable, stops responding, or produces a response change
  attributable to a shadow path;
- any private, sealed, cross-member, cross-guild, cross-channel, or incompatible
  visibility content crosses its boundary;
- a report cannot be built or returns a database/report error;
- the Ledger records a shadow write error or dangling lineage target;
- the Moment Engine records a processing error, duplicate membership, BNL-only
  finalized moment, cross-guild violation, cross-channel violation,
  incompatible-visibility violation, or dangling lineage target;
- Governance records a processing error or changes the production result while
  its live gate is off;
- Relationship v2 records a processing error, visibility/cross-guild linkage
  violation, live-emission authorization, or any actual live emission;
- model output creates positive relationship weight, a member opt-out or
  boundary is ignored, or mutual rivalry appears without the required explicit
  and reciprocal evidence; or
- queue, site, Journal, Relay, dossier, or Source File behavior changes during
  this acceptance pass.

Do not reinterpret a parity mismatch total alone as a hard stop. Investigate the
underlying counters and derived-row explanation first. The owner makes the final
go/no-go determination; the diagnostic never does.

## Evidence required for later activation

The current broad live switches are not the planned first cutover mechanism.
Before any governed-memory behavior becomes authoritative, implement a
route-, guild-, and participant-scoped canary with its own kill switch. Prove,
on the same source set:

- legacy and governed candidate comparison;
- source authority and route/visibility selection;
- current-message and recent-room precedence;
- correction, supersession, forget, deletion, and pre-send revalidation;
- deterministic fallback when governed evidence is empty or invalid;
- token-budget and duplicate suppression; and
- rollback to the unchanged legacy response owner.

Only after that governed recall canary is reviewed may Memory Governance be
considered route by route. Do not turn on the existing global live flag as a
shortcut.

Relationship v2 follows governance, not vice versa. It additionally requires
enough representative directed human events, valid Ledger/Moment links,
explicit prompt-precedence proof, correction and repair-tone examples, opt-out
enforcement, and zero unauthorized live emissions. Active Engagement v2 is
last; it requires separate frequency, relevance, cooldown, no-ping, opt-out,
and rollback evidence.

Durable knowledge calcification is also later work. Atomic knowledge
candidates, independent reinforcement, contradiction/supersession,
consolidation, governed long-range retrieval, and rare reviewed Core promotion
must be designed and proven separately. Moment repetition and BNL-authored
Journal, Relay, Ambient, dossier, or summary prose cannot confirm a fact.

## Rollback

Rollback disables gates in reverse dependency order, restarts the bot, and
preserves the shadow tables for diagnosis:

1. `BNL_MOMENT_GIST_CANARY_ENABLED`
2. `BNL_ACTIVE_ENGAGEMENT_V2_LIVE_ENABLED`
3. `BNL_RELATIONSHIP_V2_LIVE_ENABLED`
4. `BNL_MEMORY_GOVERNANCE_LIVE_ENABLED`
5. `BNL_RELATIONSHIP_V2_SHADOW_ENABLED`
6. `BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED`
7. `BNL_MOMENT_ENGINE_SHADOW_ENABLED`
8. `BNL_MEMORY_LEDGER_SHADOW_ENABLED`

The Moment gist canary is first because it is the only new response-changing
path described in this document. The live gates should already be off; listing
them makes the emergency rollback order explicit. After changing the
environment, restart the bot and re-run the read-only diagnostic.

For a stage-local failure, disable that stage and do not start later stages.
Earlier accepted shadow layers may remain on for continued observation:

- Ledger failure: disable every v2 shadow layer.
- Moment failure: disable Moments; leave Ledger only if its evidence remains
  accepted.
- Governance failure: disable Governance; do not start Relationship; Ledger and
  Moments may remain on if accepted.
- Relationship failure: disable Relationship; earlier accepted shadow layers may
  remain on.

Do not drop tables, delete rows, or rewrite legacy memory as rollback. Shadow
rows are additive diagnostic evidence. Nothing in this contract authorizes a
live cutover. Restoring the pre-deploy database is a last-resort recovery action
for a failed additive migration or corrupted database, not the normal canary
rollback. It requires stopping the bot, preserving the failed database for
diagnosis, restoring the verified backup, and restarting the previously known
good code. Any live cutover requires a separate, explicit owner-approved
operation.
