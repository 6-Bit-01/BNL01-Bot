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
- `broadcast_memory` remains the Broadcast source. Only rows carrying verified
  owner authority normalize as Declared; historical rows without that proof
  stay internal, review-only Open Signal.
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

Broadcast normalization uses `cleaned_summary` for any public-safe projection.
`raw_note`, an internal usage scope, an inactive lifecycle, or an ambiguous
boolean always fails closed to internal review metadata. Question-scoped Open
Signal normalization likewise requires explicit active, public, subject-
authored, human-source, nonderived assessment proof.

`shadow_canon_reference()` is a derived Ledger projection. It is never an
independent canon root and cannot corroborate or promote itself.

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

This version is shadow/read-only. Declared mutation, historical Broadcast
classification, claim-content use in the live packet, website projection, and
delegated authority are not enabled by this contract. The packet's binding
lookup is read-only and effective only for already-approved binding rows.
