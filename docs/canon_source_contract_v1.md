# BNL Canon/Source Contract v1

Contract version: `canon_source_contract_v1`.

This PR adds a small typed vocabulary for approved in-world canon and source safety. It does not add a database, migrate memory rows, redesign relationships, expand dossiers, change queue mechanics, or redesign relay generation.

## Canonical now

- BARCODE began as the four-member digital hip-hop collective of 6 Bit, DJ Floppydisc, Cache Back, and Mac Modem.
- The music and collective existed before BARCODE Network; the Network grew around that signal.
- 6 Bit is an artist, producer, host, and founding BARCODE member first.
- BARCODE Radio is a real weekly live broadcast/community music space on TikTok.
- Friday public schedule distinguishes intake at 6:40 PM Pacific, show start at 7:00 PM Pacific, and first-track target at 7:05 PM Pacific.
- BNL-01 remains an in-world BARCODE Network Liaison Entity with filtered surfaces and incomplete-record behavior.
- “Reality first. Meaning second. Mythology deeper.” remains website information architecture, not BNL’s speaking order.

## Vocabulary and compatibility

The contract defines source class, authority, visibility, confidence, freshness/currentness, subject identity, correction, contradiction/supersession, invalidity/retraction, public usability, derived/projection status, and current-time claim eligibility. Existing route/source/channel labels map through compatibility adapters; no existing persisted values are renamed.

Compatibility-only callers still include RouteModeContract, channel-policy routing, source-aware memory policy, broadcast-memory correction/supersession, dossier/Source File safeguards, entity intelligence, website relay source cascade, and Presence/Relay Contract v2.

## Adapted callers

- `BNL01_SYSTEM_PROMPT` consumes the rendered contract canon block instead of maintaining a conflicting inline Radio schedule.
- Website read-model context is centrally stripped of queue-derived fields unless both queue gates are true.
- Safe diagnostics expose the active contract version, adapter state, local queue capability, observed site queue capability, and effective queue usability reason.

## Not migrated

Memory rows, relationship state, dossiers, Source Files, EntitySnapshot-style records, broadcast-memory rows, and relay ledgers are not migrated. A destructive migration is deferred to the future unified memory-ledger work so legacy data can remain readable through adapters while the new vocabulary proves stable.

## Queue remains disabled

`BNL_QUEUE_PRODUCTION_ENABLED` defaults off and must equal `true` case-insensitively. Queue context is still unusable unless the website read model also reports `capabilities.queueProduction=true`. Missing capability data fails closed. This is defense in depth only; site/Vercel queue behavior is unchanged.

## Future removal points

Future PRs may replace compatibility adapters after conversation-context v2 and unified memory-ledger migration land. Until then, the contract is the shared vocabulary underneath existing systems, not a second brain or replacement canon database.
