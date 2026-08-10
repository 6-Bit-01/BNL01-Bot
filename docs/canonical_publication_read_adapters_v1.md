# Canonical Publication Read Adapters v1

This contract adds read-only ordinary-conversation adapters for canonical
Journal publications and accepted Relay publications. It does not add a
Journal or Relay store, change publication, or activate a response gate.

## Journal authority

The bot's `bnl_journal_entries` owner supplies immutable public content,
revision, content hash, lifecycle, and publication time. The website's
existing authenticated `GET /api/bnl/journal/control` owner supplies the
independent visibility and reuse decision introduced by site PR #304:

- `publicExcludedEntryIds` controls exact Discord retrieval;
- `memoryExcludedEntryIds` separately controls topic/continuity reuse; and
- control version, revision, digest, observed time, expiry, and `persisted`
  status bind selection and pre-send revalidation.

Missing, malformed, unpersisted, stale, or changed control state fails closed.
Exact identity, exact title, and exact publication-date lookup may use a public
entry even when reuse is disabled. Topic lookup requires both public visibility
and memory eligibility. The latest published revision wins; a later draft does
not replace it, while a later published revision invalidates an earlier packet.

## Relay authority

`website_relay_history` is the permanent accepted-publication owner. The
recent-25 operational view and newest-20 website view are projections only;
exact identity and date lookup can reach older accepted rows. Draft, rejected,
failed, pending, and presence-only state is never accepted speech.

A normal bot-owned history row is accepted provenance. A site-hydrated/manual
row is excluded unless `website_relay_attempts` contains a completed
`published` receipt with an explicit owner-approval trigger whose accepted ID,
prepared ID, and website publication time exactly match the history row.
`sourceClass=approved_canon`, projection presence, or public wording does not
substitute for that receipt.

## Packet and revalidation

The packet lanes are `journal_publication` and `relay_publication`. Their items
use `publication_projection`, public visibility, published lifecycle, and
`publication_only` attribution. They intentionally carry no root identities,
occurrence identities, profile point, canon status/domain/kind, or subject-fact
authority. Rendering labels them as exact published prose with zero independent
fact or recurrence weight.

Every selected Journal item is re-read against the exact canonical revision and
a newly fetched control authority identity. Every selected Relay is re-read
against its exact durable row and acceptance provenance. Content or lifecycle,
visibility/reuse, control revision/digest, Relay row, or receipt mutation makes
the packet invalid. Receipts retain only aggregate status/count fields and
digests; they do not retain publication text or entry IDs.

All shared-brain and live-authority gates remain default-off. Journal/Relay
generation, cadence, scheduling, acceptance, retries, delivery, rendering,
backup, correction policy, and owner controls are unchanged.
