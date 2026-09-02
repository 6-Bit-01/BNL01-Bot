# Ordinary-Chat Single-Packet Canary and Scoped Expansion

This capability cuts an explicitly bounded ordinary-chat scope over to a
single factual prompt owner and a single provider attempt. It is disabled by
default and is separate from the broad-profile comparison canary and
public-home recall owner. The default remains the original private acceptance
scope; contract v4 adds a second gate for controlled multi-user or
multi-channel expansion. An eligible turn either uses its one packet-owned
prompt or returns its existing deterministic clarification/refusal/block; it
never switches to a second factual prompt.

## Default-off private acceptance scope

All four values are required:

- `BNL_ORDINARY_CHAT_SINGLE_PACKET_ENABLED=true`
- `BNL_ORDINARY_CHAT_SINGLE_PACKET_GUILD_IDS=<one guild id>`
- `BNL_ORDINARY_CHAT_SINGLE_PACKET_USER_IDS=<one user id>`
- `BNL_ORDINARY_CHAT_SINGLE_PACKET_CHANNEL_IDS=<one channel id>`

Without the scoped-expansion gate, the three allowlists must each contain
exactly one positive ID. Additional user or channel IDs fail closed with
`scoped_expansion_not_enabled`; deploying contract v4 without changing the
environment therefore preserves the accepted private routing and prompt-owner
behavior.

The route is limited to direct, text-only `normal_chat` turns in
`sealed_test`, `public_home`, or `public_context`. Direct-payload tasks, simple
greetings, show/status answers, media turns, commands, Journal/Relay controls,
standalone website read-model answers, Broadcast-memory answers, and
community-visual owners stay on their established routes. A mixed request for
published Journal/Relay prose and the current queue may compose the existing
authorized read-only queue projection into that same packet.

The capability fails closed when packet or response-assessment shadows are
unavailable, a prerequisite schema version differs, a global Memory
Governance/Relationship/Active Engagement live gate is active, or either
older shared-brain synthesis authority is requested. Its independent rollback
switch is `BNL_ORDINARY_CHAT_SINGLE_PACKET_ENABLED`.

## Separately gated bounded expansion

Expanding beyond the private acceptance scope additionally requires:

- `BNL_ORDINARY_CHAT_SINGLE_PACKET_SCOPED_EXPANSION_ENABLED=true`

The expanded contract remains limited to:

- exactly one guild;
- one to eight explicitly allowlisted users; and
- one to four explicitly allowlisted channels.

The expansion gate never supplies or infers IDs. An empty allowlist remains
incomplete, more than one guild or an over-limit user/channel list fails with
`scope_limit_exceeded`, and a non-allowlisted request still fails before prompt
construction or provider use. The primary ordinary-chat kill switch, global
live-gate conflicts, specialized-owner exclusions, one-provider-call limit,
and zero-corrective-call limit are unchanged.

Content-free configuration diagnostics expose the private or
`bounded_expansion` scope mode, allowlist counts, hard caps, expansion-gate
state, expansion-effective state, and a scope digest that changes when either
the allowlists or expansion authorization changes. IDs are not exposed.

## One factual owner and one call

The immutable Situation Frame v3 and `unified_intelligence_packet_v12` are
frozen before generation. The packet renderer supplies the sole BARCODE,
member, identity, relationship, episode, publication, canon, and
stored-history factual view. The current request and verified exact-reply or
referent text remain task evidence. Persona, style, route behavior, and safety
remain expression owners but cannot create facts.

A true multi-subject request is still one governed packet and one provider
attempt. Each frozen task names its required subject indexes. The packet runs
the existing single-subject resolution and selection path once per referenced
subject, then merges those immutable component packets into a task-scoped
composite. An extra unscoped candidate, unresolved component, changed binding,
or incomplete task-to-subject map fails closed before generation.

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
5. The provider must return one typed task/result envelope. Every task must
   appear exactly once and in order, with packet evidence identifiers, the
   stable-public `PUBLIC` marker, the current-request `REQUEST` marker, or an
   explicit hold/clarify act as required by the frozen task. Packet answers
   must cover every subject required by that task and cannot borrow another
   task's subject evidence.
6. A candidate is selected once at that typed boundary. It is not
   semantically reclassified by the legacy prose guard; independent packet,
   source, frame, control-leak, exact-quote, and delivery checks can still
   block it without another provider call.

Provider-call receipts increment only at the physical `generate_content`
invocation boundary. Local quota refusal, budget-reservation failure, client
construction failure, and other pre-provider exits remain zero-attempt runs;
a provider invocation that starts and then fails remains one attempt.

Before candidate selection, the response contract is validated against the
frozen task list and the exact rendered packet evidence map. Packet-owned
tasks require applicable selected evidence identifiers. Stable external
public knowledge requires `PUBLIC`; volatile/current external facts must be
held; non-factual current-request responses require `REQUEST`. Missing,
duplicate, reordered, malformed, cross-lane, or unsupported references block
the entire candidate.

Preflight ambiguity or invalid source state uses zero provider calls and a
bounded clarification/block response. Deliberate ambiguity and volatile or
live-current tasks retain that fail-closed behavior. A failed generation or
rejected candidate never falls back to a second generated response.
Specialized owners are excluded before cutover rather than treated as a
fallback.

## Content-free receipts

`shared_brain_synthesis_v12` receipts retain only hashes, counts, bounded
statuses, and timing. Ordinary-chat rows include the frame revision and input
digest, packet/source snapshot digests, selected lane/status/domain counts,
prompt-applied state, provider and corrective call counts, candidate-selected
state, separate frame/source revalidation statuses, guard/fallback reason,
response-sent state, live-application state, typed-contract status, task
coverage counts, support-reference counts, provider latency, token breakdown,
priced-cost estimate, and bounded provider-error category/code.

The v11-to-v12 initializer adds ten content-free telemetry columns in place
and is idempotent. Existing rows are not rewritten: their new numeric zero and
blank string defaults mean "not measured by v11," not an observed zero-cost or
error-free provider result. `candidate_error_category` may describe a local
pre-provider failure, while `candidate_provider_error_count` and
`candidate_provider_error_code` are populated only when a physical provider
attempt occurred.

No prompt, packet text, source text, response text, participant IDs, or source
references are stored. Aggregate diagnostics expose ordinary-run totals,
provider/corrective call totals, call-count violations, typed-contract
violations, typed task/support totals, revalidation statuses, and the
independent kill switch.

## Rollback and acceptance order

Full rollback requires no database deletion:

1. Set `BNL_ORDINARY_CHAT_SINGLE_PACKET_ENABLED=false` or remove it.
2. Restart the bot.
3. Confirm the ordinary-chat capability is requested/effective `off`.

Rolling code back does not remove the additive v12 SQLite columns. They are
content-free, defaulted, and inert to v11 code; removing them would require a
separate destructive table migration and is not part of the operational
rollback.

Expansion-only rollback keeps the accepted private canary available:

1. Restore exactly one approved user and one approved channel in the
   allowlists.
2. Set
   `BNL_ORDINARY_CHAT_SINGLE_PACKET_SCOPED_EXPANSION_ENABLED=false` or remove
   it in the same environment edit.
3. Restart the bot and confirm `scope_mode=private_acceptance`,
   `scoped_expansion_effective=false`, and ordinary-chat effective `on`.

With the switch off, ordinary-chat prompt bytes and established generation
behavior remain unchanged. Deployment and private live acceptance are separate
operations: merge the complete PR sequence first, run the combined automated
suite and 60-case acceptance matrix, then enable only the exact private scope
for the approved provider-shadow and owner acceptance runs.

After private acceptance passes, deploy contract v4 with the expansion gate
absent or false and confirm the existing private scope remains effective. Then
enable expansion for one additional approved user in the already accepted
channel. Validate one ordinary multi-subject turn and one explicit specialized
Broadcast-memory turn before adding another user or any channel. Expand one
dimension at a time; do not enable the global Memory Governance, Relationship,
or Active Engagement live gates during this rollout.
