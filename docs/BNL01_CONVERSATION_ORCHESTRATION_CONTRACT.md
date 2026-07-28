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
| Conversation coordinator | One final response act from typed evidence: answer, clarify, acknowledge, observe, or block | New storage, duplicate retrieval, generation prose |
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
5. Coordinate the route, response obligation, engagement recommendation,
   referent status, and Moment state into one response act.
6. Generate from the already selected packet. Do not rerun or reinterpret
   context selection inside the generator.
7. Deliver the response.
8. Only after successful delivery, commit any explicit BNL self-name decision
   through the existing Memory Ledger.

A confirmed mention, reply to BNL, accepted self-name, or live self-name
proposal creates a response obligation unless a higher route or safety rule
blocks it. Engagement controls optional ambient participation, timing, length,
and tone; it does not veto that obligation.

## Governed BNL self-names

BNL self-names are learned decisions, not configured aliases.

- A generic vocative or explicit naming proposal may present a candidate to
  BNL. No specific candidate is privileged in production code.
- A known human display name remains a human target and cannot silently become
  a BNL name.
- The generation prompt asks BNL to accept, deny, or defer naturally in his own
  voice.
- Persistence fails closed: a decision is written only when the sent response
  explicitly names the candidate and unambiguously accepts, denies, or defers
  it.
- Accepted names become valid addresses. Denied names do not route ordinary
  use, but an explicit reconsideration request reaches BNL. Deferred names
  remain undecided.
- A later decision supersedes the earlier same-name decision through Ledger
  lineage. The current state is a projection over existing Ledger entries, not
  a new nickname table.
- Decisions are scoped by guild and visibility. A sealed-test-only decision
  cannot become public routing state.
- The existing member preferred-name system remains separate: it records what
  BNL calls a member, not what members call BNL.

## Nearby referents

Conversation Context v2 resolves nearby same-room contributions in this order:

1. Exact Discord reply source identity.
2. Explicit speaker attribution tied to an act or object, such as a member who
   "said," "wrote," or "posted" something.
3. Structural contribution type, such as passage, message, answer, or model
   response.
4. Positional or immediate bounded candidates.

Selection is visibility-safe, same-room, recent, and source-linked. A speaker
name that merely happens to equal a referent noun is not attribution.

If exactly one bounded source is resolved, Context v2 supplies that raw source
with attribution and reserves its prompt budget before general continuity. An
explicit correction completed in the current turn remains authoritative and is
not reopened as historical ambiguity. If multiple plausible sources remain,
the coordinator requires one concise clarification naming the bounded
candidates. If no eligible source exists, BNL may clarify honestly. BNL must
not claim content is missing when it is safely available.

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
- Deterministic status and recall shortcuts cannot bypass a pending natural
  self-name decision.
- Cached self-name projections retain their last known value on a transient
  database read error and are invalidated after a successful decision write.

## Regression and rollback boundary

Regression coverage must include arbitrary self-name candidates, acceptance,
denial, deferral, correction, restart persistence, visibility boundaries,
known-human collisions, direct and batched response obligations, exact reply
references, cross-speaker structural references, ambiguity, cross-room
exclusion, Moment state, third-party-only turns, and route blocks.

Rollback is one code rollback across the coordinator, address projection,
Context v2 referent resolver, Moment situation reader, and their bot wiring.
It must never require deleting or rewriting existing conversations, Ledger
entries, Moment windows, or Relationship data.
