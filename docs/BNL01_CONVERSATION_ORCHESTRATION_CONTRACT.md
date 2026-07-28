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
| Conversation coordinator | One final response act per immutable packet revision from typed evidence: answer, clarify, acknowledge, observe, or block | New storage, duplicate retrieval, generation prose |
| Generation and delivery | A natural BNL response from the coordinated packet, followed by a successful send | Retroactively changing the coordinator's route or evidence |

Route, safety, visibility, and third-party-only boundaries remain higher
authority than an ordinary response obligation. Relationship evidence may
shape tone after a response is authorized, but it cannot silence or force the
turn.

## Authoritative turn order

1. Apply route, safety, and channel-visibility boundaries.
2. Resolve typed addressing, including mentions, Discord reply identity,
   human targets, and governed BNL self-name state.
3. Ask Conversation Context v2 for bounded raw context and referent status.
4. Ask the Moment Engine for content-free current situation and flow state.
5. Add content-free governed-memory, Relationship posture, canon-applicability,
   and source-control states. General durable content is not allowed to become
   a routing veto.
6. Freeze one typed packet revision and coordinate exactly one response act for
   it.
7. If a new contribution interrupts the turn, discard the superseded draft,
   rebuild the entire packet, assign a new revision, and assess that revision
   once. No answer-intent latch survives the rebuild.
8. Generate from the already selected packet. Do not rerun or reinterpret
   context selection inside the generator.
9. Deliver the response.
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

## Governed BNL self-names

BNL self-names are learned decisions, not configured aliases.

- A structurally valid vocative or explicit naming proposal may present a
  candidate to BNL. Leading discourse words and generic commands are not name
  proposals. No specific candidate is privileged in production code.
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
- A self-name decision is not persisted when generation fails, delivery fails,
  parsing is ambiguous, or the source conversation cannot be linked.
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
collisions, direct and batched response obligations, resolved and unresolved
exact reply references, multiple replies, replies to BNL, partial sends,
cross-speaker structural references, current payload precedence, contribution
type, ambiguity, cross-room exclusion, long-source budget priority, Moment
state, third-party-only turns, route blocks, interruption rebuilds, and
event-loop offloading.

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
