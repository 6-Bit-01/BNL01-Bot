# BNL Canon/Source Contract v1

Contract version: `canon_source_contract_v1`.

This PR adds a small typed vocabulary for approved in-world canon and source safety. It does not add a database, migrate memory rows, redesign relationships, expand dossiers, change queue mechanics, or redesign relay generation.

## Canonical now

- BARCODE began as the four-member digital hip-hop collective of 6 Bit, DJ Floppydisc, Cache Back, and Mac Modem.
- The music and collective existed before BARCODE Network; the Network grew around that signal.
- 6 Bit is an artist, MC, host, and founding BARCODE member first; he is not the music producer.
- GALAKNOISE is BARCODE's music producer.
- BARCODE Radio is a real weekly live broadcast/community music space on TikTok.
- Friday public schedule is immutable contract data: intake/submissions at 6:40 PM Pacific, show start at 7:00 PM Pacific, and first-track target at 7:05 PM Pacific.
- BNL-01 remains an in-world BARCODE Network Liaison Entity with filtered surfaces and incomplete-record behavior.
- “Reality first. Meaning second. Mythology deeper.” remains website information architecture, not BNL’s speaking order.

## Owner identity privacy

6 Bit is the only human-readable BARCODE identity for the project owner. A
real-world personal or legal name is private, is not BARCODE canon, and must not
appear in code, tests, fixtures, prompts, seed data, logs, diagnostics,
documentation, or public/admin surfaces.

Owner-only access and diagnostics use `BNL_OWNER_USER_ID` or another opaque
stable identifier. Public BNL output may recognize 6 Bit as an artist, host,
and founding BARCODE figure, but it must not reveal or imply private account,
owner, controller, admin, operator, or infrastructure facts.

## Central sanitized read-model boundary

`fetch_bnl_read_model()` sends the existing `BNL_API_KEY` when configured and may retain the raw validated payload in its private cache for capability checks, cache metadata, and privacy-safe diagnostics. It accepts either the original/explicit public response (`publicOnly=true`, scope absent/`public`/`none`) or an authenticated private response (`publicOnly=false`, `accessScope=private`). A private response is rejected when the service key is not configured. Normal consumers use the channel-scoped sanitized consumption view before prompt assembly or intent dispatch.

When queue production is disabled, the contract strips queue/session/payment/availability/now-playing/up-next/active/completed track/count/Priority/Wheel/queue-derived artist fields, including the complete `artistMemory` section. It also filters `operatorLanes` by provenance: queue-public snapshots and queue/session/track/payment/priority/wheel-derived entries are removed from temporary runtime context, recap candidates, broadcast-memory candidates, dossier seed candidates, and public-safe copy candidates. Non-queue public dossier material and non-queue boundary/do-not-store rules remain available.

Queue production remains disabled unless both gates are explicit: local `BNL_QUEUE_PRODUCTION_ENABLED=true` and website `capabilities.queueProduction=true`. Queue use then follows exactly three website scopes:

- `none`: no queue/history data is usable;
- `private`: queue/history data is usable only in `sealed_test` and `internal_controlled`;
- `public`: queue/history data is usable in public consumers.

Missing, contradictory, or malformed scope/`publicOnly` combinations fail closed. Public and show-day consumers cannot receive private queue data, and Broadcast Memory, dossier, Source File, Relay, Journal, or any other persistence/publication path cannot retain it. Membership in the permission-locked `sealed_test` and `internal_controlled` channels is the requester authorization boundary for this context; access may be granted to explicitly admitted rehearsal testers and operators, while ordinary server members remain excluded. The bot does not impose a second owner-only check after Discord access has already been restricted. An approved private channel may answer an admitted participant from private queue context, including a transient test recap, but the prompt carries an explicit instruction not to use that data in public output and remains non-persistent. In `#bnl-testing`, a queue question creates a response obligation without requiring the participant to be an owner, admin, or mod.

For a live operational question, the bot fetches a fresh snapshot, searches the complete ordered queue, and passes only the request-relevant facts into generation. It answers Now Playing, Up Next, position, movement, and confirmed Wheel-result questions directly; a request for the complete lineup is routed to the queue page. Read access never implies queue, Wheel, or playback control.

TikTok LIVE is a source-aware public conversation lane with a current-show view.
When the local TikTok awareness gate is explicitly enabled, the isolated
collector exposes a bounded volatile snapshot to the bot. BNL may load that view
only for an explicit live-show/reaction question and only when the same channel
is authorized for the current website queue scope. The queue is authoritative
for show state; TikTok comments/questions are viewer statements and engagement
counters are platform-room metrics. Missing or stale context fails closed.

Every accepted public TikTok comment/question is also eligible for the separate
memory handoff. The main bot writes it through the append-only Journal source
archive and Unified Memory Ledger as a conversation observation immediately
above Community Canon. It may support ordinary continuity and surface-level
lore, but one utterance cannot create canon, relationship truth, a Source File,
a dossier, or verified external fact. Aggregate viewers, taps, gifts, joins, and
other room metrics remain current-show-only. Declared owner handles `@six.bit`
and `@pr0x60` resolve to the same owner subject. Other cross-platform bindings
require compatible handle and display-name evidence; ambiguous matches remain
TikTok-only. A TikTok moderator flag is trusted only as room-role evidence for
the exact account. None of these paths grant TikTok output or queue controls.

The existing Open Signal/Living Canon owner may validate those raw utterances
directly against their immutable Journal receipts. A finalized show supplies
one bounded occurrence identity for all compatible remarks in that episode;
message volume within a show cannot manufacture recurrence. Independent later
shows and eligible Discord occurrences may support the existing revisable
Living Canon contract. Episode aggregates never become corroborating roots,
and finalization neither enables formation nor bypasses Declared/Legacy/Core
authority.

The only artist/entity operational queue-data persistence exception is the independently versioned `sections.artistMemory` public catalog. Its validator requires both production gates, exact schema/policy strings, a matching digest, approved public live-broadcast provenance, and a bounded record count. Each record carries source-aware artist, song, optional album/project, show, and lifecycle fields. Accepted is provisional and a confirmed play supersedes that revision. Provider and submitted conflicts remain explicit; exactly one semantic provider artist ID may own the primary subject, while channels/uploaders and all submission attribution remain disconnected from Discord identity. The bridge writes through existing entity evidence and memory ledger tables, skips unchanged digests, supersedes older active revisions, and never marks a Source File refresh dirty. Private, rehearsal, simulation, file, payment, account, contact, dossier, relationship, and canon fields remain ineligible.

A separate public show-episode projection may retain the website archive's sanitized milestone sequence and track roster as operational chronology. It joins that chronology to the complete eligible TikTok source window and to public Discord user/model rows only when a stored BNL response explicitly targets the member in the same channel within the bounded show-response window. The website remains authoritative for queue, playback, Wheel, sponsor, signal-hold, order, and outcome state; authored TikTok/Discord text remains attributed observation evidence. Exact source-owned subject references may bind episode surfaces, while names, handles, and queue attribution alone cannot merge identities. The projection is query-retrieved, lineage-linked, superseding, deletion-aware, above Community Canon, and grants no queue mutation, output, relationship, dossier, Source File, or canon authority.

The website's bounded `sections.sourceContext` list remains public site canon, not live queue state. BNL may load those public summaries for an explicit site/read-model question even while live queue context is disabled. Queue/session/track values are still removed before prompt assembly.

## Vocabulary and compatibility coverage

The contract defines source class, authority, visibility, confidence, freshness/currentness, subject identity, correction, contradiction/supersession, invalidity/retraction, public usability, derived/projection status, and current-time claim eligibility. Existing route/source/channel labels map through compatibility adapters; no persisted values are renamed.

Explicit route/source compatibility covers: `room`, `public_safe_memory`, `show_status_public`, `source_safe_public`, `display_name`, `payload`, `source_files`, `classification`, `community_presence`, `approved_public_presence`, `recommendation_packet`, `approved_channel_history`, `ops`, `broadcast_memory`, `public_show_state`, `join_event`, and `episode_tracker`.

Additional compatibility concepts are mapped for relay/dossier/entity vocabulary: fresh public Discord observations, recent public continuity, scoped broadcast memory, public-safe memory, approved canon, grounded reflection, Source File projections, dossier/public-page projections, and entity evidence projections. Presence/Relay Contract v2 payload values are not changed.

Channel visibility mappings cover `public_home`, `public_context`, `public_selective`, `sealed_test`, `internal_controlled`, `reference_canon`, `protected_system`, `broadcast_memory`, `ai_image_tool`, and `unknown`; unknown policies remain non-public/unknown.

## Claim resolution rules

Claim resolution is scoped to one subject and one predicate. Mixed scopes return `mixed_claim_scope`. Valid corrections/supersessions may suppress only same-subject/same-predicate claims when the correcting claim is public-usable, non-retracted, non-expired, equal-or-higher authority, and not merely a derived/projection claim trying to erase independent evidence.

Resolution ranks source authority first, confidence second, and recency third. Equal-authority/equal-confidence/equal-recency conflicting values return `unresolved_equal_authority_conflict` instead of depending on input order. Identical values resolve deterministically.

## Current-time evidence requirements

Static approved canon and schedule facts cannot prove live/open/current/now state. Current-time claims require fresh, public-usable, explicitly current-time-capable runtime observations with valid timestamps inside the freshness window. Missing, stale, materially future, derived, recap, relay, dossier, Source File projection, or source-blind claims do not prove current state.

## Adapted callers

- `BNL01_SYSTEM_PROMPT` consumes the rendered contract canon block instead of maintaining a conflicting inline Radio schedule.
- `/about` consumes contract schedule/founder render helpers instead of hardcoding the old 6:40 PM show-time wording.
- Website read-model prompt context and R&D/operator read-model intent responses consume the sanitized view.
- Safe diagnostics expose the active contract version, adapter state, local queue capability, observed site queue capability, and effective queue usability reason without raw queue values.

## Native queue and show-day alignment

Show-day announcement canon consumes the same gate decision without persisting queue data. The scheduled 6:40 PM Pacific intake message names the native BARCODE Radio queue only when the local bot gate, website capability, and public queue access are all true. Private access is intentionally insufficient. Otherwise it uses provider-neutral public-intake wording; stock and generated announcements may not fall back to Auxchord-specific copy or imply BNL operates submissions.

The 7:00 PM Pacific announcement is deliberately restrained: the schedule proves the broadcast window, not a current live/on-air state. The optional later-show sponsor reminder does not claim that a commercial break is active, due, required, or already called. Current state still requires fresh public runtime evidence and host control.

Show-day alignment and operational queue context do not write memory, relationships, dossiers, Source Files, Relay, recaps, the Journal, or public copy lanes. For queue-derived data, the separate validated public artist catalog above is the sole memory exception and grants no other destination authority. TikTok public conversation archival is an independent source path governed by the limits above. None of these paths enables show-day Discord posts, proactive engagement, queue write power, or either production gate.

## Not migrated

Memory rows, relationship state, dossiers, Source Files, EntitySnapshot-style records, broadcast-memory rows, and relay ledgers are not migrated. A destructive migration is deferred to future unified memory-ledger work so legacy data can remain readable through adapters while the new vocabulary proves stable.

## Queue remains disabled

`BNL_QUEUE_PRODUCTION_ENABLED` defaults off and must equal `true` case-insensitively. Queue context is still unusable unless the website read model also reports `capabilities.queueProduction=true`. This is defense in depth only; site/Vercel queue behavior is unchanged.

## Future removal points

Future PRs may replace compatibility adapters after conversation-context v2 and unified memory-ledger migration land. Until then, the contract is the shared vocabulary underneath existing systems, not a second brain or replacement canon database.
