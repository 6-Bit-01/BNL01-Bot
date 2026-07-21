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

## Rollback

Rollback disables gates in reverse dependency order, restarts the bot, and
preserves the shadow tables for diagnosis:

1. `BNL_ACTIVE_ENGAGEMENT_V2_LIVE_ENABLED`
2. `BNL_RELATIONSHIP_V2_LIVE_ENABLED`
3. `BNL_MEMORY_GOVERNANCE_LIVE_ENABLED`
4. `BNL_RELATIONSHIP_V2_SHADOW_ENABLED`
5. `BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED`
6. `BNL_MOMENT_ENGINE_SHADOW_ENABLED`
7. `BNL_MEMORY_LEDGER_SHADOW_ENABLED`

The live gates should already be off; listing them makes the emergency rollback
order explicit. After changing the environment, restart the bot and re-run the
read-only diagnostic.

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
live cutover; that would require a separate, explicit owner-approved operation.
