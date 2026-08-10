# Ordinary-Chat Single-Packet Canary v1

This capability cuts one exact ordinary-chat scope over to a single factual
prompt owner and a single provider attempt. It is disabled by default and is
separate from the broad-profile comparison canary and public-home recall
owner.

## Exact default-off scope

All four values are required:

- `BNL_ORDINARY_CHAT_SINGLE_PACKET_ENABLED=true`
- `BNL_ORDINARY_CHAT_SINGLE_PACKET_GUILD_IDS=<one guild id>`
- `BNL_ORDINARY_CHAT_SINGLE_PACKET_USER_IDS=<one user id>`
- `BNL_ORDINARY_CHAT_SINGLE_PACKET_CHANNEL_IDS=<one channel id>`

The three allowlists must each contain exactly one positive ID. The route is
limited to direct, text-only `normal_chat` turns in `sealed_test`,
`public_home`, or `public_context`. Direct-payload tasks, simple greetings,
show/status answers, media turns, commands, Journal/Relay controls, website
read-model answers, Broadcast-memory answers, and community-visual owners
stay on their established routes.

The capability fails closed when packet or response-assessment shadows are
unavailable, a prerequisite schema version differs, a global Memory
Governance/Relationship/Active Engagement live gate is active, or either
older shared-brain synthesis authority is requested. Its independent rollback
switch is `BNL_ORDINARY_CHAT_SINGLE_PACKET_ENABLED`.

## One factual owner and one call

The immutable Situation Frame and `unified_intelligence_packet_v8` are frozen
before generation. The packet renderer supplies the sole BARCODE, member,
identity, relationship, episode, publication, canon, and stored-history
factual view. The current request and verified exact-reply/referent text remain
task evidence. Persona, style, route behavior, and safety remain expression
owners but cannot create facts.

The cutover prompt omits legacy conversation history, durable memory tiers,
Relationship facts/counters, duplicate Broadcast/show/site/source blocks,
unselected Journal/Relay prose, and legacy canon/lore factual bodies. A
selected source-file item may still appear through the packet's existing
authorized source adapter; the raw source block is never duplicated.

For an eligible turn:

1. Deterministic scope, frame, packet, prompt-owner, and source checks run
   before generation.
2. The route makes at most one provider attempt with zero provider retries and
   no model fallback.
3. Glitch rewrites, cross-universe rewrites, payload-completion corrections,
   grounding repairs, and other corrective provider calls are disabled.
4. The packet and frame are independently revalidated after generation and
   immediately before send.
5. A generated candidate changed or suppressed by a guard is not sent and is
   never repaired by another provider call.

Preflight ambiguity or invalid source state uses zero provider calls and a
bounded clarification/block response. A failed generation or rejected
candidate never falls back to the legacy generated response. Specialized
owners are excluded before cutover rather than treated as a fallback.

## Content-free receipts

`shared_brain_synthesis_v9` receipts retain only hashes, counts, bounded
statuses, and timing. Ordinary-chat rows include the frame revision and input
digest, packet/source snapshot digests, selected lane/status/domain counts,
prompt-applied state, provider and corrective call counts, candidate-selected
state, separate frame/source revalidation statuses, guard/fallback reason,
response-sent state, and live-application state.

No prompt, packet text, source text, response text, participant IDs, or source
references are stored. Aggregate diagnostics expose ordinary-run totals,
provider/corrective call totals, call-count violations, revalidation statuses,
and the independent kill switch.

## Rollback and acceptance order

Rollback requires no database deletion:

1. Set `BNL_ORDINARY_CHAT_SINGLE_PACKET_ENABLED=false` or remove it.
2. Restart the bot.
3. Confirm the ordinary-chat capability is requested/effective `off`.

With the switch off, ordinary-chat prompt bytes and established generation
behavior remain unchanged. Deployment and private live acceptance are separate
operations: merge the complete PR sequence first, run the combined automated
suite, then enable only the exact private scope for the approved acceptance
matrix.
