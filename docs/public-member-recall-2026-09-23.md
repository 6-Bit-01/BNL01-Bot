# Public member recall: originals and durable memory

This repair continues C1/C2/C3 of the locked shared-brain plan on deployed
parent `672e7de6b3c3a9dffee19820348cd06d8e4ad8a2`. Earlier show selection,
follow-up and original-quotation acceptance remains credited.

## Evidence and cause

The completed read-only production inventory found 4,366 public human Discord
messages and 1,538 short, 807 medium and 43 long memory entries. The two queried
Discord members have 107 and 466 public originals. Older memory therefore still
exists. All existing medium/long entries lack complete source links; their
presence is not proof of current eligibility, accurate meaning or Core status.
The previously returned August 20 quotation matches its original, and newer
messages exist. Newer timestamps alone do not establish better content.

Real SQLite/assembly fixtures reproduce two integration defects independently
of production identity ambiguity:

1. Broad public member recall treats request wording as a content topic and
   drops available original messages. Topic search also bounds recent rows
   before matching, hiding older relevant originals behind newer unrelated
   conversation.
2. Durable memory is loaded for current speakers but does not compose the named
   subject's memory alongside that subject's public Discord and TikTok sources.

## Existing owners extended

The conversation reader and bounded member-memory reader share the same frozen
subject and human continuation query. Broad recall selects recent public human
originals by event time, independently of archive insertion order. Specific
topic and date filters run before the existing candidate limit. Whole selected
utterances retain speaker, channel and original timestamp; the existing source
basis and correction checks still control delivery.

Named members now use the existing public memory reader under a shared bounded
allowance in direct and batched assembly, with ordinary packet routing on or
off. Matching older summaries outrank unrelated recent material. Equal-relevance
tiers rotate so short-term entries cannot consume the entire allowance first.
Public-safe legacy summaries remain lower-authority hints, never verbatim
messages, verified personal facts or approved Core. Unknown/private legacy
material stays excluded. No lineage is inferred, seeded or rewritten.

Public tier reads honor explicit tier retraction/correction controls. Existing
source-link invalidation handles corrected or removed originals. Each named
memory basis preserves its subject, query and budget for reconstruction before
delivery. Batched source reads run off the Discord event loop.

There are no gate, model, scheduler, database schema or account-link changes.
Discord labels resolve through the existing guild identity owner. A different
artist/TikTok name does not silently become a Discord account. Learning and
correcting cross-platform identity through moderator/member evidence and
cautious inference remains a separate acceptance requirement; this patch does
not install any real account relationship.

## Verification and remaining acceptance

Nine new regressions cover broad and date-only recall and its quote follow-up, older topic
matches past a recent-row bound, named medium/long composition and refresh,
batched delivery across public/sealed and packet on/off, event-time recency,
tier/source retraction, and person switches/unknown artist names. The focused
cross-source, original-conversation, public-network, member-memory and chronology
set passes 90 tests. The event-loop test also covers both named source readers.
`make check PYTHON=.venv/bin/python` passes compilation and all 3,317 tests.
Provider and Discord transports are fixtures; this proves source delivery and
controls, not live model answer quality.

The existing request logs now include `named_public_conversation_context_loaded`
with selected original row IDs and recent/topic mode, and
`named_public_member_memory_context_loaded` with selected source-kind counts.
These diagnostics contain no raw comments or inferred account links.

After merging, deploy the verified revision through the existing owner-run VPS
procedure. In the same sealed test conversation, send one prompt at a time:

1. Ask for a recent public Discord conversation using a known Discord member's
   actual public label.
2. Ask: `What were their exact words?`
3. Explicitly switch to another known Discord member and request a recent
   public Discord example with a quotation.
4. Ask about a genuinely older known topic for that member, with its date if
   available, and distinguish original wording from retained summaries.

Correlate each reply with selected source rows and original records. Do not
teach the bot a cross-platform alias merely to make this test pass. An
unresolved identity needs an honest scoped answer or natural clarification,
not a different person's anecdotes or an unrelated show recap.

Natural retained-memory meaning, aging/restart behavior, learned identity,
fresh operational queue/live truth and scheduled Journal/Relay/Ballad/Ambient
delivery remain explicit open acceptance items. A successful retrieval test
does not certify those systems or finish the shared-brain plan.
