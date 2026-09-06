# Canonical Publication Read Adapters v1

This contract adds read-only ordinary-conversation adapters for canonical
Journal publications and accepted Relay publications. It does not add a
Journal or Relay store, change publication, or activate a response gate.

## Journal authority

The bot's `bnl_journal_entries` owner supplies immutable public content,
revision, content hash, lifecycle, and publication time. The website's
existing authenticated `GET /api/bnl/journal/control` owner supplies the
public visibility state introduced by site PR #304:

- `publicExcludedEntryIds` excludes hidden entries from conversational reads;
- the September 6 owner direction makes public text available for ordinary
  reuse regardless of `memoryExcludedEntryIds`; and
- the existing validated snapshot supplies public visibility and an expiry.

Missing or invalid visibility state omits Journal input while normal conversation
keeps its other context. Exact identity/title/date, latest and topical reads all
use public entries. Public-read digests cover content and public exclusions,
without treating memory-formation controls as public-read permissions.
The latest published revision wins; a later draft does
not replace it, while a later published revision invalidates an earlier packet.

## Normal Gemini connection — September 6

Normal direct and batched conversations now call these existing readers in all
three public channel policies and their sealed testing mirror. `include_context`
adds a bounded relevance search without requiring the user to say Journal or
Relay. It selects at most two publications per kind; unrelated queries add none.
Journal ranking uses decoded published text, not JSON field names.

The normal prompt receives attributed publication history alongside existing
conversation, memory, show and current queue inputs. Gemini interprets the
question and writes the response. This does not enable experimental generation
or add an output format or language validator.

Journal relevance is checked locally before fetching website visibility. Direct
and batch publication reads run off the Discord event loop. The normal path
reuses its fetched snapshot within the site's declared freshness window (at
most 300 seconds); final source checks perform no HTTP requests. Existing source
bases compare selected local revisions and accepted Relay records. If an input
changes or the snapshot expires, ordinary source recovery removes that
publication block and retains unrelated context. A hide made after the initial
fetch is subject to that existing freshness window; this is not an immediate
remote invalidation subscription.

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

The separate experimental packet path re-reads a selected Journal against the
exact canonical revision and newly fetched full control identity. It remains
disabled in the recovered runtime. Every selected Relay is re-read
against its exact durable row and acceptance provenance. Content or lifecycle,
public visibility, control identity, Relay row, or receipt mutation makes
the packet invalid. Receipts retain only aggregate status/count fields and
digests; they do not retain publication text or entry IDs.

All shared-brain and live-authority gates remain default-off. Journal/Relay
generation, cadence, scheduling, acceptance, retries, delivery, rendering,
backup, correction policy, and owner controls are unchanged.
