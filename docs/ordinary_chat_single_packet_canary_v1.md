# Ordinary-Chat Single-Packet Canary and Scoped Expansion

This capability cuts an explicitly bounded ordinary-chat scope over to a
single factual prompt owner and one natural response obligation. It is disabled by
default and is separate from the broad-profile comparison canary and
public-home recall owner. The default remains the original private acceptance
scope; contract v4 adds a second gate for controlled multi-user or
multi-channel expansion. An eligible turn uses its packet-owned prompt when it
is available. If packet preparation is unavailable, the established
context-rich generation path still answers the user. Neither path emits a
deterministic blocker or canned fallback message.

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

Packet authority stays inactive when packet or response-assessment shadows are
unavailable, a prerequisite schema version differs, a global Memory
Governance/Relationship/Active Engagement live gate is active, or either
older shared-brain synthesis authority is requested. Those conditions do not
cancel the ordinary-chat response obligation; they leave the established
context-rich generation path in place. The independent rollback switch is
`BNL_ORDINARY_CHAT_SINGLE_PACKET_ENABLED`.

## Separately gated bounded expansion

Expanding beyond the private acceptance scope additionally requires:

- `BNL_ORDINARY_CHAT_SINGLE_PACKET_SCOPED_EXPANSION_ENABLED=true`

The expanded contract remains limited to:

- exactly one guild;
- one to eight explicitly allowlisted users; and
- one to four explicitly allowlisted channels.

The expansion gate never supplies or infers IDs. An empty allowlist remains
incomplete, more than one guild or an over-limit user/channel list fails with
`scope_limit_exceeded`, and a non-allowlisted request remains outside packet
authority. The primary ordinary-chat kill switch, global live-gate conflicts,
and specialized-owner exclusions are unchanged. Ordinary generation remains
responsible for the reply when packet authority is not active.

Content-free configuration diagnostics expose the private or
`bounded_expansion` scope mode, allowlist counts, hard caps, expansion-gate
state, expansion-effective state, and a scope digest that changes when either
the allowlists or expansion authorization changes. IDs are not exposed.

## One factual owner and one response

The immutable Situation Frame v3 and `unified_intelligence_packet_v12` are
frozen before generation. The packet renderer supplies the sole BARCODE,
member, identity, relationship, episode, publication, canon, and
stored-history factual view. The current request and verified exact-reply or
referent text remain task evidence. Persona, style, route behavior, and safety
remain expression owners but cannot create facts.

A true multi-subject request is still one governed packet and one coherent
response. Each frozen task names its required subject indexes. The packet runs
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
2. The route asks BNL for one natural response that uses the complete
   authorized packet as one understanding of the turn.
3. If review finds unsafe, stale, internally leaked, generic, or incomplete
   prose, the existing generation path rewrites it naturally. This is a repair
   of the same response obligation, not a second factual owner or canned
   fallback.
4. The packet and frame are independently revalidated after generation and
   immediately before send.
5. The provider returns visible natural prose, not a typed JSON envelope.
   Every task must be answered coherently. A task with unavailable current
   evidence states that specific uncertainty while the rest of the request is
   still answered; ambiguity becomes a useful clarification; privacy becomes
   a natural refusal without disclosure.
6. Packet, source, frame, control-leak, exact-quote, and delivery checks may
   require a grounded or source-neutral rewrite. They do not veto the
   authorized response act or substitute a generic message.

Provider-call receipts increment only at the physical `generate_content`
invocation boundary. Local quota refusal, budget-reservation failure, client
construction failure, and other pre-provider exits remain zero-attempt runs;
a provider invocation that starts and then fails remains one attempt.

The response receipt audits coverage against the frozen task list and rendered
packet evidence map without turning that audit into response authority.
Unsupported packet-domain facts, control leakage, incoherence, and changed
evidence require repair. Stable public knowledge may still supply ordinary
common sense; volatile current facts are stated only when current evidence is
available.

Preflight ambiguity is generated as a natural clarification. Invalid or
changed source state produces a natural, specific uncertainty while preserving
the answerable parts of the request. Specialized owners remain outside the
cutover rather than becoming parallel factual prompts. A genuine provider or
Discord transport failure can prevent delivery, but no governance result is
converted into a canned blocker.

## Content-free receipts

`shared_brain_synthesis_v12` receipts retain only hashes, counts, bounded
statuses, and timing. Ordinary-chat rows include the frame revision and input
digest, packet/source snapshot digests, selected lane/status/domain counts,
prompt-applied state, provider and corrective call counts, candidate-selected
state, separate frame/source revalidation statuses, review reason,
response-sent state, live-application state, task-plan status and task
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
