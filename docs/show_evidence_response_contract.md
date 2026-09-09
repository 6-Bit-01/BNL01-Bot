# Show evidence and natural responses

This correction follows the owner decision to give Gemini relevant authorized
data and conversation context, without a second word-matching judge deciding
whether the model used an allowed phrase.

## Source responsibilities

- Existing readers select eligible public show, TikTok, Discord, queue,
  publication, and conversation records within their existing limits.
- Human-authored excerpts retain their original speaker, event, surface, and
  source revision. Summaries and prior BNL replies remain distinct projections;
  they do not become independent audience transcripts.
- Finalized-show source bases are re-read after provider awaits and before
  delivery. Source removal, revision, visibility, consent, and scope changes
  remain real invalidations.
- Requester-specific continuity repeats the same consent lookup used by initial
  selection. Missing requester scope cannot substitute everybody else's public
  activity for a personal-history answer. Ordinary public show questions stay
  available under their existing authorization.
- A public-only basis cannot gain requester-specific scope during refresh.
- Independent valid sources stay available when one source changes. The
  existing response-repair owner reconstructs the prompt and makes a bounded
  corrective generation when necessary.

## Response responsibilities

Gemini receives the selected evidence together with the current request and
relevant conversation. It interprets intent, including follow-ups, and forms
the response. The show path does not classify the response using word overlap,
verb lists, participant-name syntax, punctuation, quote formatting, required
gist labels, or required uncertainty phrases. It does not strip sentences or
retry/suppress a response merely for failing those checks.

This applies to finalized-show authored responses and the older dedicated
TikTok analysis/episode response checks, on direct and batched delivery paths.
It introduces no replacement semantic judge, provider route, canned fallback,
or persistent memory owner.

The separate existing Discord consequential exact-quote authority, publication
controls, privacy boundaries, current queue authority, and stale-source
revalidation are not removed by this change.

Trusted Discord mentions/replies already directed at BNL retain their response
obligation through batching. An existing unexpired answer window for the same
member, guild, and room accepts short answers without a second length/word
test. It does not authorize unrelated speakers, rooms, generic recent activity,
or turns directed only at another human.

## Verification and limits

Tests verify unchanged source/speaker pairs, relevant inherited context,
natural response delivery without show-wording retries, and real SQLite
consent/source changes across generation. Supported mocked responses test
transport and lifecycle behavior; they do not establish that arbitrary model
output is factually correct.

Inherited show-source selection still uses the existing reader's retrieval
cues. For example, a prior explicit recap request is covered, whereas a prior
bare quote/date request is not always recognized as a show-retrieval cue.
Natural delivery and a preserved prior exchange do not alone prove that every
desired source was retrieved. This preexisting selector limitation is not
masked by the continuation tests or claimed fixed here.

Live acceptance still needs to check grounded show recall and quotes, an
ordinary continuation, a factual correction, a mixed-source question, current
image interpretation, and provider-call/latency receipts. Finite generation
retries cannot guarantee a response during provider failures.

The correction does not deploy, restart services, alter production gates,
change provider selection, add dependencies, or migrate the database.
