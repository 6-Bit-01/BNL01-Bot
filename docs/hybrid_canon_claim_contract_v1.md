# Hybrid Canon Claim Contract v1

`hybrid_canon_claim_v1` is the read-model contract that lets BNL compare
curated canon, owner declarations, learned community patterns, and immediate
public observations without replacing their existing source owners.

## Two independent dimensions

- Establishment: `legacy`, `declared`, `living`, or `open_signal`.
- Domain: `real_community`, `broadcast_history`, `operational`, `lore`, or
  `hybrid`.

Lifecycle, visibility, validity, confidence, and projection state remain
independent. A Declared row may be internal, an expired operational claim may
remain historical, and an established atomic row is not Living Canon unless it
is a domain-qualified community `topic_or_motif` with at least two eligible
independent roots across two bounded occurrence identities under
`living_canon_recurrence_v1`. A finalized Moment alone is one occurrence, not
proof of recurrence.

## Source ownership

- `CANON_FACTS` remains the Legacy source.
- `broadcast_memory` remains the Broadcast content source. A raw row alone is
  always internal, review-only Open Signal. Only an exact current join to an
  owner-verified `declared_canon_revisions` sidecar can normalize it as a
  Declared shadow; stale or absent sidecars fall back to the raw Open view.
- Ledger, Moments, and atomic lifecycle remain the Living source machinery.
  Current established atomic and finalized Moment rows normalize as internal,
  review-only Open Signal until the recurrence contract is proved; they are not
  silently promoted to Living Canon.
- Public assessment observations remain ephemeral Open Signal.
- The unified intelligence packet remains the only broad-profile factual
  selection owner.

Adapters produce immutable `CanonClaim` revisions. They do not copy, mutate,
promote, or publish source rows. Every revision keeps a stable claim ID, exact
revision ID, source revision, subject, predicate, source system, adapter
version, source refs, root and occurrence IDs, authority receipt, visibility,
validity, lifecycle, and correction lineage. Claim IDs use the permanent
`bnl_canon_claim_identity_v1` namespace, so a contract-version change creates a
new revision without changing the logical claim identity.

### Living Canon recurrence proof

A Living claim must carry explicit stored proof for
`living_canon_recurrence_v1`: an allowed community domain and pattern claim
kind, a versioned deterministic grouping signature, distinct immutable human
root identities, distinct canonical occurrence identities, and matching
content-free proof counts. Blank, malformed, negative, count-mismatched, or
inconsistent self-asserted fields fail closed. The database inventory also
requires the current reconciled lifecycle state and stored lineage; the proof
is a consistency contract, not a cryptographic attestation. Neither
`candidate_state='established'` nor a finalized Moment is a substitute for
this proof.

Curated family labels and the family-neutral fallback use the same recurrence
standard. One occurrence stays provisional; two qualifying roots across two
qualifying occurrences may become establishment-eligible only after lifecycle
and correction checks. Neutral patterns cannot create scalar identity, roles,
relationships, milestones, operational truth, Declared Canon, or Legacy
Canon.

PR 4 keeps every recurrence-marked Living candidate, curated-family or
family-neutral, outside the live packet until PR 5 owns convergence. Its
historical preview runs against a disposable in-memory clone and
returns only versions, states, bounds, aggregate reason counts, and an explicit
zero-source-write receipt. It does not expose source text or identifiers, and
it never performs historical promotion.

Broadcast normalization uses `cleaned_summary` for any public-safe projection.
`raw_note`, an internal usage scope, an inactive lifecycle, or an ambiguous
boolean always fails closed to internal review metadata. Question-scoped Open
Signal normalization likewise requires explicit active, public, subject-
authored, human-source, nonderived assessment proof.

`shadow_canon_reference()` and `shadow_declared_canon_projection()` are derived
Ledger projections. They are never independent canon roots and cannot
corroborate or promote themselves. The Declared projection revalidates the
exact latest authority/source revision and same-guild roots before insertion.

## Identity

Same-platform identity begins with an explicit immutable account binding.
Display names are reversible hints only. Entities may exist without accounts
and later receive a binding without duplication.

A binding is usable only with `canon_entity_account_binding_v1`, an opaque
authority actor, an explicit boundary-verification bit, a versioned mutation
receipt, and an immutable platform-ID shape (Discord IDs are numeric). A label
such as `owner_confirmed`, a display name in an account-ID field, a malformed
boolean, or an unversioned lookalike row is not authority.

The existing live packet prefers a fully versioned, boundary-verified binding
when an approved binding table and row are already present. An invalid or
ambiguous row fails closed rather than falling back to a conflicting display
label. PR 1 creates no binding table or row and performs no data mutation;
creating or approving binding data remains a separately authorized operation.
Without an approved row, the existing label-compatibility path remains, except
for removal of the false alias.

Call'em Bini and Cache Back are distinct entities. Any relationship between
them requires a separately typed, sourced claim; neither name is an alias or
account-binding shortcut for the other. The existing website origin sentence
is retained as an internal, review-only lore relationship candidate and has no
fact or identity authority.

The content-free inventory distinguishes complete source reconciliation from
a bounded partial scan. Any truncated source forces
`sourceAdaptedReconciled=false` and reports the affected source names; no
source text, display label, account ID, or row ID is emitted.

## Mutation boundary

PR 2 adds the owner-only, append-only `declared_canon_lifecycle_v1` mutation
contract described in `BNL01_DECLARED_CANON_LIFECYCLE_V1.md`. General
declarations live in its revision table; Broadcast content remains in
`broadcast_memory`, with only typed classification metadata and a source
fingerprint stored beside it. No historical classifications are written
automatically.

Every stored revision carries a request fingerprint and an internally issued,
keyed, versioned receipt. `BNL_DECLARED_CANON_AUTHORITY_SECRET` must contain at
least 32 bytes, and no public API accepts a caller-supplied authority or
receipt. Read boundaries recompute the HMAC over the exact actor, guild,
operation, nonce/request bindings and complete immutable stored payload, then
recompute the revision ID. Missing, short, or rotated key material fails
closed; rotation requires an explicit reviewed migration rather than silently
re-signing history. A conflicting `INSERT OR REPLACE` is rejected before
SQLite can replace history.

The sidecar and source reads are pinned to SQLite's `main` schema, and every
admission path verifies the exact table/constraint/trigger contract inside the
same mutation transaction or read snapshot. These controls detect schema
damage and ordinary rollback attempts within the active database. They do not
claim protection against a privileged attacker who can restore both an older
signed database state and the exact schema; a later live authority boundary
would require an external signed head/checkpoint for that stronger threat
model.

Broadcast sidecars bind to `broadcast_memory_complete_row_v2`, which hashes the
complete deterministically canonicalized source row including unknown/future
columns. Any schema addition stales prior approval and requires explicit owner
review; no column is silently treated as non-authoritative.

Core current/latest validators own one coherent read snapshot when the caller
has no transaction and reuse a caller-owned transaction otherwise. This keeps
revision selection, stored-receipt validation, and authoritative source-row
revalidation on one database state while remaining zero-write.

Claim-content use in the live packet, website projection, and delegated
authority remain disabled. New Declared Ledger projections are internal,
derived, nonpublic, and confined to the `declared_canon_review` lane until
final convergence. Established and contested rows use the `review_only`
lifecycle; resolved, retired, and superseded terminal rows retain the
`resolved` lifecycle. The packet's binding lookup remains read-only and
effective only for already-approved binding rows.
