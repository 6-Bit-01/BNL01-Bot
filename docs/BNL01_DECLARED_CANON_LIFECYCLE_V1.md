# Declared Canon Lifecycle v1

`declared_canon_lifecycle_v1` adds an owner-controlled, append-only lifecycle
for explicit BARCODE declarations without creating another database, retriever,
or response owner.

## Source ownership

- General declarations use one additive `declared_canon_revisions` table in the
  existing bot database. The owner's raw declaration and each immutable
  revision remain in that table.
- Broadcast declarations keep `broadcast_memory` as the authority-bearing
  content source. A declaration revision stores only typed classification,
  subject, lifecycle, authority, and an exact source fingerprint; it does not
  copy the raw note or cleaned summary.
- Cleaned summaries are projections. They never become independent authority
  or corroborate their raw parent.
- Legacy Canon remains versioned in code. Living Canon and Open Signal retain
  their existing owners.

## Append-only operations

Every add, correction, supersession, retirement, or status change appends an
immutable revision. The stable declaration ID survives correction; revision
IDs and revision numbers do not. Prior revisions are never deleted or silently
rewritten.

All preview and mutation APIs independently require:

- a configured owner ID;
- an exact actor/owner match;
- the configured primary guild and an exact target-guild match;
- an opaque `discord_user:<id>` actor; and
- an internally issued, versioned receipt bound to the exact operation,
  target, expected snapshot, payload, and Discord-message nonce.

No Declared Canon signing key or secret environment variable exists. Under the
trusted application-and-database threat model, the core deterministically
issues the receipt itself after authenticating the configured owner and guild;
no public API accepts a caller-built authority or receipt. The visible receipt
version commits to actor, guild, operation, nonce, normalized request, and the
complete immutable stored revision. Validators recompute both that receipt and
the revision ID before admitting a row. This detects accidental or partial row
tampering but does not claim to defend against an attacker who can both rewrite
the trusted database and execute trusted application code.

The Discord route is a first boundary, not the only boundary. A direct helper
call with missing, mismatched, cross-guild, malformed, or stale authority fails
before writing.

Mutations also use optimistic concurrency. General declarations require the
exact current revision ID. Broadcast classifications require both the exact
current declaration revision (when reclassifying) and a fingerprint of the
approved source snapshot. Operation nonces are idempotent only for an exact
request; reusing one with a changed binding fails closed.

Exported current/latest validators hold one coherent SQLite read snapshot for
their complete revision-and-source validation sequence. They open and close a
read transaction only when the caller has none; an existing caller transaction
is reused and left open. WAL writers may commit concurrently, but a validator
cannot mix a revision from one database state with a Broadcast row from another.
The validators remain zero-write, and a later call observes the newer commit.

## Broadcast classification

Only the six current parser types have default mappings:

- `episode_arc`
- `notable_moment`
- `running_joke`
- `technical_issue`
- `moderation_context`
- `show_state_override`

The mapping supplies a proposed domain and claim kind, not a subject. Every
classification requires an explicit typed subject. Submitter identity is
provenance and never becomes the subject automatically.

Historical or out-of-contract values—including `continuity_backreference`,
`show_note`, `recap`, and arbitrary legacy strings—retain their exact source
type and stay review-only until an explicit owner classification. A timestamp,
public-safe flag, active status, or post-PR-362 creation date is not owner
authority by itself.

The `broadcast_memory_complete_row_v2` fingerprint hashes the complete source
row returned by SQLite, not a field allowlist. Column names are sorted and every
value is encoded with an explicit scalar type before hashing. This covers
content, provenance, scope/visibility inputs, validity, lifecycle, correction
data, supersession links, source timestamps, and every unknown/future column.
Adding any source column therefore stales existing approval—even when its value
is NULL—until the owner explicitly reviews and reclassifies the row.

## Historical preview

The historical preview is bounded, content-free, and zero-write. It reports
counts by owner-era boundary, submitter class, source type, lifecycle, scope,
visibility, validity, derivation, subject-link state, and review disposition.
It never returns raw notes, cleaned summaries, names, account IDs, or proposed
mutation payloads. An owner-only review may use a turn-scoped opaque source
token to identify an exact row for a later, separately approved action; the
token itself carries no authority and is not stable evidence.

`total_rows` describes the full guild-scoped source query. The count mappings
are explicitly labeled `counts_scope=returned_page`; they never pretend a
bounded page is a global aggregate. Malformed historical timestamps or
submitter identifiers remain reviewable as reason-coded, unknown provenance
instead of aborting the preview.

Applying any historical classification or subject link remains a separate
owner-approved data operation. PR 2 creates no backfill and runs no such write.

## Broadcast source/lineage atomicity

New Broadcast primary projections are derived from an exact source row inside
the source write transaction. Resolve, clear, and replacement transitions
re-read the exact owner/guild/status/update/supersession snapshot and retract
every still-effective Ledger primary for that exact source row in the same
transaction. A missing primary is safe because there is no Ledger
representation to invalidate. If an older defect left duplicate primaries,
the status event must retract the complete pre-write set or the source
transition rolls back; it never chooses one as more authoritative. New primary
creation remains strict at exactly one. A disabled shadow flag therefore
cannot strand an already-written active primary, and a skipped, conflicting,
or failed required projection rolls back the Broadcast source transition.

## Non-live boundary

Declared claim adapters remain read models. New Ledger projections are always:

- derived and projected;
- internal;
- review-only; and
- `public_usable=0`.

Their route and channel labels are both `declared_canon_review`, so no existing
approved-canon or public-route selector can mistake the PR 2 shadow for live
canon evidence. Corrections emit
supersession lineage; contested, resolved, retired, and superseded revisions
emit retraction lineage against the exact prior projection.

Existing Broadcast readers continue to use the mature `broadcast_memory`
compatibility lane in PR 2. The new declaration view does not feed the unified
packet, Journal, Relay, dossiers, website projections, or public responses.
Final one-packet selection and duplicate-source collapse belong to the later
convergence PR.

## Owner command surface

The first owner surface is an exact `!bnl canon <action> <JSON>` command in
`#research-and-development`. It supports content-free `preview` and
`broadcast-preview`, plus `add`, `correct`, `status`, `retire`, `supersede`,
and `broadcast-classify`. The parser rejects unknown fields and never guesses
from natural language. Mutation replies contain only operation/declaration/
revision IDs, lifecycle state, and shadow outcome; they never echo the raw
declaration or Broadcast content.

Control-shaped messages are dispatched from the original Discord payload
before mention/whitespace normalization, direct-conversation ingress, room
context, batching, or other conversational sinks. This preserves exact JSON
string values (including mention tokens and intentional spacing) and keeps
authorized, denied, wrong-channel, and malformed control attempts out of later
prompts.

No environment gate, deployment state, website authority, delegated steward
power, historical classification, or live response behavior is changed by
this lifecycle contract. It introduces no credential, environment variable,
or deployment prerequisite.
