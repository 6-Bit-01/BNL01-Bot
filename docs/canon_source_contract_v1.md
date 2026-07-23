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

## Central sanitized read-model boundary

`fetch_bnl_read_model()` may retain the raw validated payload in its private cache for capability checks, cache metadata, and privacy-safe diagnostics. Normal consumers use the sanitized consumption view through the bot boundary before prompt assembly or intent dispatch.

When queue production is disabled, the contract strips queue/session/payment/availability/now-playing/up-next/active/completed track/count/Priority/Wheel/queue-derived artist fields. It also filters `operatorLanes` by provenance: queue-public snapshots and queue/session/track/payment/priority/wheel-derived entries are removed from temporary runtime context, recap candidates, broadcast-memory candidates, dossier seed candidates, and public-safe copy candidates. Non-queue public dossier material and non-queue boundary/do-not-store rules remain available.

Queue production remains disabled unless both gates are explicit: local `BNL_QUEUE_PRODUCTION_ENABLED=true` and website `capabilities.queueProduction=true`. Missing or malformed capability data fails closed.

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

Show-day announcement canon consumes the same two-gate decision without persisting queue data. The scheduled 6:40 PM Pacific intake message names the native BARCODE Radio queue only when the local bot gate and website capability are both true. Otherwise it uses provider-neutral public-intake wording; stock and generated announcements may not fall back to Auxchord-specific copy or imply BNL operates submissions.

The 7:00 PM Pacific announcement is deliberately restrained: the schedule proves the broadcast window, not a current live/on-air state. The optional later-show sponsor reminder does not claim that a commercial break is active, due, required, or already called. Current state still requires fresh public runtime evidence and host control.

This alignment does not write queue context to memory, relationships, dossiers, Source Files, Relay, recaps, the Journal, or public copy lanes. It does not enable show-day Discord posts, proactive engagement, queue write power, or either production gate.

## Not migrated

Memory rows, relationship state, dossiers, Source Files, EntitySnapshot-style records, broadcast-memory rows, and relay ledgers are not migrated. A destructive migration is deferred to future unified memory-ledger work so legacy data can remain readable through adapters while the new vocabulary proves stable.

## Queue remains disabled

`BNL_QUEUE_PRODUCTION_ENABLED` defaults off and must equal `true` case-insensitively. Queue context is still unusable unless the website read model also reports `capabilities.queueProduction=true`. This is defense in depth only; site/Vercel queue behavior is unchanged.

## Future removal points

Future PRs may replace compatibility adapters after conversation-context v2 and unified memory-ledger migration land. Until then, the contract is the shared vocabulary underneath existing systems, not a second brain or replacement canon database.
