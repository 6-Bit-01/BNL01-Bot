# BNL-01 Conversation Orchestration Contract

## Purpose

BNL-01's conversation, memory, Moment, relationship, and routing systems are
specialized parts of one mind. They must retain distinct evidence ownership
while converging on one authoritative turn decision before BNL is silenced or
given a generation prompt.

This contract forbids incident-specific alias patches, a second context engine,
a second nickname store, and downstream observers attempting to repair evidence
that an upstream stage already discarded.

## Evidence ownership and authority

| System | Owns | Must not own |
| --- | --- | --- |
| Discord addressing | Mentions, replies, human addressees, and governed BNL self-name use or proposals | Memory truth, nearby-content selection, response prose |
| Memory Ledger | Durable, source-linked BNL self-name decisions and their supersession history | Live address inference or prompt generation |
| Conversation Context v2 | Bounded, visibility-safe raw working context and nearby referent resolution | Durable identity policy, Moment qualification, social tone |
| Moment Engine | Content-free activity, participant, topic, situation, and conversational-flow state | Raw passage retrieval, nickname decisions, response vetoes |
| Relationship Engine | Tone and interpersonal framing | Whether BNL responds, raw evidence selection |
| Engagement classifier | Optional participation recommendation for ambient room activity | Vetoing a confirmed address or unresolved direct referent |
| Canon and source controls | Applicability, route, visibility, and source-authority boundaries | Conversation routing, raw-context selection, social tone |
| Conversation coordinator | One final response act per immutable packet revision from typed evidence: answer, clarify, acknowledge, observe, or refuse | New storage, duplicate retrieval, generation prose |
| Generation and delivery | A natural BNL response from the coordinated packet, followed by a successful send | Retroactively changing the coordinator's route or evidence |

Route, safety, visibility, and third-party-only boundaries remain higher
authority than the content of an ordinary response. Once the coordinator has
authorized an answer, those boundaries may reject, repair, or replace unsafe
prose with a source-neutral reply; they do not cancel the response act.
Ambiguity, missing current evidence, changed sources, and privacy limits shape
that natural reply. They never authorize a generic scope/grounding message or
a deterministic response block. Passive ambient non-participation and a
superseded stale draft are not authorized direct-response acts and remain
separate from this rule.
Relationship evidence may shape tone after a response is authorized, but it
cannot silence or force the turn.

## Authoritative turn order

1. Apply route, safety, and channel-visibility boundaries.
2. Resolve typed addressing, including mentions, Discord reply identity,
   human targets, and governed BNL self-name state.
3. Ask Conversation Context v2 for bounded raw context and referent status.
4. Ask the Moment Engine for content-free current situation and flow state.
5. Add content-free governed-memory, Relationship posture, canon-applicability,
   and source-control states. General durable content is not allowed to become
   a routing veto.
6. Freeze one typed packet revision, coordinate exactly one response act, and
   build one immutable Situation Frame v1 from those existing-owner results.
7. If a new contribution interrupts the turn, discard the superseded draft,
   rebuild the entire packet, assign a new revision, and assess that revision
   once. No answer-intent latch survives the rebuild.
8. Generate from the already selected packet. Do not rerun or reinterpret
   context selection inside the generator.
9. Apply response guards to the draft. A failed repair rejects that draft, not
   the authorized response act: recover a grounded cleaned candidate or a
   source-neutral reply, then deliver it. Only a newer packet revision or an
   earlier route/safety block may stop this send.
10. Only after successful delivery, commit any explicit BNL self-name decision
    through the existing Memory Ledger and consume any repair/retransmission
    state.

A confirmed mention, reply to BNL, accepted self-name, or live self-name
proposal creates a response obligation unless a higher route or safety rule
blocks it. Engagement controls optional ambient participation, timing, length,
and tone; it does not veto that obligation.

The existing `UnifiedResponseAssessment` remains a content and source plan
inside an already-authorized generation. It may select or revalidate memory,
Relationship, Moment, canon, and other prompt lanes, and it may produce a
post-send shadow receipt. It is not a second response-act authority and must not
change answer, clarify, observe, or block after the conversation coordinator
has decided the current packet.

## Situation Frame v1 (shadow-only)

The conversation coordinator now owns one immutable, response-scoped
`situation_frame_v1`. It is a typed applicability decision, not a new context
engine, memory store, retriever, identity service, or factual packet.

The frame freezes:

- its schema revision, input-evidence digest, and current-contribution digest;
- current speakers, authoritative addressee kinds, Discord message/reply/source
  anchors, and reversible subject candidates;
- stable target references received from existing owners, while treating
  display labels only as low-confidence hints;
- current Moment reference and whether the scene is the same event, a new
  phase, a resume, a comparison/participant change, a new event, or uncertain;
- task, object, request phase, role/domain hints, temporal scope, currentness,
  objective, and required response act;
- decision, correction, unresolved-question, and open-loop states; and
- route, surface, channel policy, visibility allowance, competing frames, and
  material ambiguity.

Frame construction is deterministic and makes no provider call. A question
about another person never falls back to the current speaker merely because a
stable subject binding is absent. Multiple people, unresolved referents,
same-name candidates, blocked visibility, and competing event/role readings
remain explicit rather than being guessed away.

The frame is passed unchanged as shadow metadata through prompt construction,
the existing packet/assessment path, response guards, and a separate pre-send
`frame_source_revalidation_v1` result. Revalidation references the original
frame and packet digests; it never mutates either object. During this stage the
frame cannot change retrieval, prompt prose, generation, delivery, routing, or
durable memory. It creates no authority and activates no gate.

Only content-free frame receipts may persist: version/revision/digest, status,
typed counts, phase/object/event-relation classifications, and revalidation
status/reason counts. Raw contribution text, display labels, account IDs, and
subject values are excluded from those receipts. Disabling or rolling back the
shadow wiring requires no data deletion or backfill.

## Governed BNL self-names

BNL self-names are learned decisions, not configured aliases.

- A structurally valid vocative or explicit naming proposal may present a
  candidate to BNL. Punctuation alone never establishes both a candidate and
  a BNL target. Leading/trailing discourse words, modifiers, generic commands,
  quoted speech, code, titles, and discussion about a possible name are not
  performed naming acts. This is a positive grammar contract, not a finite
  discourse-word blacklist, and no specific candidate is privileged in
  production code.
- `Hey <candidate>, ...` is a strong greeting vocative when another-human and
  quotation ambiguity are absent. A novel bare `<candidate>, ...` or
  `..., <candidate>` requires independent BNL targeting from a Discord
  mention/reply plus second-person address structure; the comma cannot create
  that target. An already accepted governed name remains valid as a bare
  vocative without proposing it again.
- A known human display name remains a human target and cannot silently become
  a BNL name.
- The typed lifecycle is propose, accept, reject, defer, correct, or revoke.
  The generation prompt asks BNL to decide naturally in his own voice.
- Persistence fails closed: a decision is written only when the sent response
  explicitly names the candidate and unambiguously accepts, rejects, defers,
  corrects, or revokes it.
- Accepted names become valid addresses. Denied names do not route ordinary
  use, but an explicit reconsideration request reaches BNL. Deferred names
  remain undecided.
- A later decision supersedes the earlier same-name decision through Ledger
  lineage. The current state is a projection over existing Ledger entries, not
  a new nickname table.
- A correction or revocation supersedes the prior state explicitly. Deletion,
  forgetting, retraction, privacy loss, visibility changes, and broken
  provenance invalidate the derived routing state as soon as it is read.
- Current routing state is freshly revalidated against both the member proposal
  and BNL's saved delivered response. A stale positive cache is never routing
  authority.
- New decisions record `bnl_self_name_grammar_v2` and their positive evidence
  kind in the existing Ledger value. Historical decisions remain append-only:
  an owner-only, content-free read diagnostic counts current, revalidated, and
  quarantined decisions without emitting names or message text. A historical
  decision lacking the current validation revision may route only when its
  original user root independently revalidates as an explicit naming act or
  correction/revocation. Old greeting-only and weak comma-only history lacks
  enough trusted target context, so it stays visible to the diagnostic but is
  excluded from routing without mutation.
- Decisions are scoped by guild and visibility. A sealed-test-only decision
  cannot become public routing state.
- The existing member preferred-name system remains separate: it records what
  BNL calls a member, not what members call BNL.

## Nearby referents

Conversation Context v2 resolves nearby same-room contributions in this order:

1. Exact Discord reply source identity, using both resolved and unresolved
   Discord references and the persisted IDs of BNL's successfully delivered
   response chunks.
2. Explicit speaker attribution tied to an act or object, such as a member who
   "said," "wrote," or "posted" something.
3. Structural contribution type, such as passage, message, answer, or model
   response.
4. Positional or immediate bounded candidates.

Selection is visibility-safe, same-room, bounded, and source-linked. Exact
reply identity may retrieve an older same-room source outside the ordinary
recency window; language heuristics may not widen that window. Multiple exact
reply sources remain explicit ambiguity rather than silently choosing one. A
speaker name that merely happens to equal a referent noun is not attribution.

If exactly one bounded source is resolved, Context v2 supplies that raw source
with attribution and reserves its prompt budget before general continuity. An
explicit payload or correction completed in the current turn remains
authoritative and is not reinterpreted as a historical pointer. Requested
contribution type cannot degrade into the nearest unrelated message. If
multiple plausible sources remain, the coordinator requires one concise
clarification naming the bounded candidates. If no eligible source exists, BNL
may clarify honestly. BNL must not claim content is missing when it is safely
available.

An exact Discord reply target is the sole continuity source for that turn
unless the current request explicitly names another message, reply,
contribution, thread, or newer/older idea to combine or compare. A generic
choice, transformation, or question about material inside the exact target
does not widen scope. Nearby non-target contributions remain typed,
source-linked revalidation evidence for drift detection, but they are not
rendered into the generation prompt. If a draft positively switches to or
mixes in one of those competing sources, the shared response guard may perform
one regeneration using only the exact target. A second proven switch rejects
that candidate before delivery and recovers the response obligation without
using the competing source; it does not silently drop the turn.

## Moment boundary

The Moment Engine may inform the coordinator that a recent window is open,
finalized, rejected, or under review, along with participant overlap and topic
coherence. This state helps BNL understand activity and flow even when no
episode or gist was produced.

It never supplies the raw passage. A rejected one-message Moment window may
still describe the live situation, but it does not itself force a response.
Conversation Context v2 remains the only owner of raw nearby content.

## Failure behavior

- Optional Context or Moment read failure cannot veto a confirmed address.
- Ambiguous or unresolved direct referents produce clarification, not invented
  context and not a false absence claim.
- A resolved exact reply cannot silently fall back to recency. Prompt
  construction, regeneration, and the final pre-send check preserve the same
  typed reply source; a proven competing-source substitution fails closed.
- A self-name decision is not persisted when generation fails, delivery fails,
  parsing is ambiguous, the source grammar does not reproduce the claimed
  positive evidence, or the source conversation cannot be linked.
- A partial multi-chunk delivery writes no complete model conversation row and
  no false Discord reply identity.
- Deterministic status and recall shortcuts cannot bypass a pending natural
  self-name decision or an authoritative coordinated response act. Explicit
  protected operator and safety routes retain their higher authority.
- Self-name reads and writes, Context reads, Moment reads, Relationship reads,
  and post-send receipts run outside Discord's event loop. A transient database
  failure fails closed for derived routing state rather than stalling the
  handler or reusing stale authority.

## Regression and rollback boundary

Regression coverage must include arbitrary self-name candidates, acceptance,
denial, deferral, correction, revocation, restart persistence, deletion,
forgetting, retraction, provenance loss, visibility boundaries, known-human
collisions, punctuation-only discourse/modifier constructions, strong greeting
vocatives, mention/reply-supported novel bare vocatives, quoted/discussed/code
candidates, Unicode names, mixed-human ambiguity, validation-version history
quarantine, content-free diagnostics, direct and batched response obligations,
resolved and unresolved
exact reply references, multiple replies, replies to BNL, partial sends,
replies to another human while addressing BNL, competing newer same-room
messages, explicit reply-scope expansion, cross-speaker structural references,
current payload precedence, contribution type, ambiguity, cross-room
exclusion, long-source budget priority, Moment state, third-party-only turns,
route blocks, interruption rebuilds, and event-loop offloading. Exact-reply
coverage must exercise the complete handler path through prompt construction,
generation, guard regeneration or candidate rejection, obligated-response
recovery, and final send.

Response influence has one dedicated fail-closed gate:
`BNL_CONVERSATION_ORCHESTRATION_INFLUENCE_ENABLED`. It defaults off and is not
activated by Memory Ledger or Moment shadow flags. The sealed canary gate
requires exactly one allowed guild, one allowed channel, and `sealed_test`
policy. Gate state is evaluated at use time, so disabling it restores shadow-
only behavior without data migration.

Rollback is first the dedicated gate, then—if needed—one code rollback across
the coordinator, address projection, Context v2 referent resolver, Moment and
Relationship situation readers, and bot wiring. It must never require deleting
or rewriting existing conversations, Ledger entries, Moment windows, or
Relationship data.

## Adjacent governed surfaces

Journal and Relay retain their existing ownership and are not conversation
response authorities. Journal remains scheduled, frozen-evidence expression;
Relay remains durable public-signal generation and delivery. Neither may be
called as a fallback for missing conversational context, and conversation
orchestration must not alter their schedules, cursors, persistence, publication
boundaries, or capacity lanes.
