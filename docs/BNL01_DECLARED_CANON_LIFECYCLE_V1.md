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

`BNL_DECLARED_CANON_AUTHORITY_SECRET` is a dedicated signing secret and must
contain at least 32 UTF-8 bytes. The core uses HMAC-SHA-256 to authenticate both
the normalized owner request and the complete immutable stored revision. No
public API accepts a caller-built authority, receipt, or signing key. A missing
or short secret fails before a write; changing the secret makes every existing
receipt invalid and blocks reads and mutations until an explicit, reviewed
rotation/migration restores one coherent signing epoch. Rotation is never
silently accepted as new authority.

The visible receipt version commits to actor, primary guild, operation, nonce,
normalized request, and every stored revision field except its self-referential
receipt and revision ID. Validators recompute the HMAC and revision ID before
admitting a row and compare the integrity values in constant time. Stored rows
must still name the currently configured owner and current primary guild; a
historically valid receipt from different runtime authority does not qualify.

The Discord route is a first boundary, not the only boundary. A direct helper
call with missing, mismatched, cross-guild, malformed, or stale authority fails
before writing.

Mutations also use optimistic concurrency. General declarations require the
exact current revision ID. Broadcast classifications require both the exact
current declaration revision (when reclassifying) and a fingerprint of the
approved source snapshot. Operation nonces are idempotent only for an exact
request; reusing one with a changed binding fails closed.

## Schema and chain integrity

All authoritative table lookups, reads, and writes are explicitly qualified to
SQLite's `main` schema. A same-named TEMP table therefore cannot shadow an older
Declared Canon head or an older Broadcast source row. Every write verifies the
schema after `BEGIN IMMEDIATE` has acquired the write boundary; every exported
read and preview verifies it inside one pinned read snapshot.

Schema creation is one-time and exact. If `declared_canon_revisions` already
exists, initialization validates the complete table definition, primary and
unique constraints, named indexes, and the exact append-only triggers. A
missing, extra, or same-name-but-altered trigger, weakened constraint, changed
column, or unexpected index fails with a schema-integrity error. Initialization
does not repair, recreate, or normalize a damaged existing authority schema.

The insert-conflict, no-update, and no-delete triggers cover ordinary SQL and
SQLite conflict forms including `REPLACE`, `INSERT OR REPLACE`,
`UPDATE OR REPLACE`, and UPSERT against either the primary key or composite
unique constraints. Each trusted head is also derived only after validating the
complete ordered chain: revision numbers must be contiguous from one, every
`previous_revision_id` must name the immediately prior authenticated revision,
source identity must remain stable, and every row must pass its contract and
HMAC checks. Internal inventory code may perform this check inside its existing
read transaction without minting a synthetic owner request.

This design cannot, by itself, prove that an attacker with arbitrary write and
DDL access to the same SQLite file did not delete the newest signed rows and
then restore the exact expected schema. Detecting that perfect whole-file
rollback requires a trusted monotonic head or signed checkpoint stored outside
the database. The lifecycle therefore detects damaged schema, row tampering,
and broken/missing middle links, but does not claim external rollback
attestation that PR 2 does not own.

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

If a Declared Canon table is present, the preview first authenticates its exact
schema and every complete declaration chain. Corrupt sidecar authority aborts
the preview rather than being displayed as a merely stale classification.

Applying any historical classification or subject link remains a separate
owner-approved data operation. PR 2 creates no backfill and runs no such write.

## Broadcast source/lineage atomicity

New Broadcast primary projections are derived from an exact source row inside
the source write transaction. Resolve, clear, and replacement transitions
re-read the exact owner/guild/status/update/supersession snapshot and retract
every still-effective Ledger representation for that source row in the same
transaction: all primary roots, every Declared projection derived from those
roots, and an orphaned Declared projection still linked by the authoritative
sidecar. A missing primary is safe only when no other effective representation
exists. If an older defect left duplicates, multiplicity is diagnostic rather
than a reason to choose one or skip invalidation; the complete pre-write set
must be retracted or the source transition rolls back. Exact terminal-event
retries may add missing lineage only after the complete stored event and
participant set match. New primary creation remains strict at exactly one. A
disabled shadow flag therefore cannot strand an already-written
representation, and a skipped, conflicting, or failed required projection
rolls back the Broadcast source transition.

## Non-live boundary

Declared claim adapters remain read models. New Ledger projections always
remain in the non-live review lane:

- derived and projected;
- internal;
- `public_usable=0`.

Established and contested projection rows use the `review_only` lifecycle.
Resolved, retired, and superseded terminal projection rows retain the
`resolved` lifecycle while remaining non-live.

Their route and channel labels are both `declared_canon_review`, so no existing
approved-canon or public-route selector can mistake the PR 2 shadow for live
canon evidence. Corrections supersede every effective older projection;
contested, resolved, retired, and superseded revisions retract every effective
older projection. Already-ineffective historical rows retain their existing
lineage without a redundant edge.

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

`broadcast-preview` is an aggregate, content-free diagnostic rather than an A5
classification handoff. The dependency-free core computes per-row,
receipt-scoped opaque `preview_item` values, but the Discord reply exposes only
grouped counts and those values are intentionally neither stable nor resolvable
back to a source row through the command surface. Any historical A5 write needs
a later, separately owner-approved resolver or review manifest that binds an
opaque selection to the exact row ID and source fingerprint inside one reviewed
snapshot. PR 2 does not expose raw row IDs/fingerprints or create that workflow.

Control-shaped messages are dispatched from the original Discord payload
before mention/whitespace normalization, direct-conversation ingress, room
context, batching, or other conversational sinks. This preserves exact JSON
string values (including mention tokens and intentional spacing) and keeps
authorized, denied, wrong-channel, and malformed control attempts out of later
prompts.

No environment gate, deployment state, website authority, delegated steward
power, historical classification, or live response behavior is changed by
this lifecycle contract. The new authority secret is code-level configuration
only in this PR: it is not generated, deployed, rotated, or written to any
repository file. Provisioning it is a separate deployment operation that must
occur before the owner command surface can be enabled.
