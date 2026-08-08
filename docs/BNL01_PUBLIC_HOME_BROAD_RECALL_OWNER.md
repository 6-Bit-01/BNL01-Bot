# Public-Home Broad-Recall Owner

This route promotes the accepted shared-brain packet selector for one narrow
conversation family: direct, first-person broad-profile requests in the
configured `public_home` channel.

It does not enable global Memory Governance, Relationship v2, Active
Engagement, or any queue capability. The established response is still
generated first and remains the fallback whenever packet construction,
factual-prompt ownership, candidate evaluation, guard checks, source
revalidation, or delivery preparation fails.

## Default-off gate

The route is effective only when all three values are present:

- `BNL_PUBLIC_HOME_BROAD_RECALL_OWNER_ENABLED=true`
- `BNL_PUBLIC_HOME_BROAD_RECALL_OWNER_GUILD_IDS=<one guild id>`
- `BNL_PUBLIC_HOME_BROAD_RECALL_OWNER_CHANNEL_IDS=<one public-home channel id>`

The scoped synthesis canary and the public-home owner are mutually exclusive.
If both enable switches are true, synthesis fails closed with
`authority_conflict`. The public-home owner also fails closed when a global
Memory Governance, Relationship v2, or Active Engagement live gate is active,
or when the packet and response-assessment shadows are unavailable.

## Authority and safety

- The governed packet is the only stored factual prompt owner for this route.
- Competing legacy factual context is replaced before the candidate call.
- Prompt selection stays bounded, while claim validation uses a separate
  route-safe support set from the same existing source owners. Lane and prompt
  budget exclusions do not make otherwise eligible support "unsupported."
- Current-turn and exact-reply authority remain ahead of stored memory.
- The subject is always the requesting member; cross-member facts remain
  ineligible.
- Relationship stays tone-only and is not rendered as factual evidence.
- BNL-authored derivatives cannot corroborate themselves.
- Every candidate is revalidated before send; any failure returns to the
  established response.
- A candidate may replace the established response only when it retains every
  exact, question-relevant member point, concrete detail, and approved canon
  item that the established response used from that support set. A shorter or
  differently framed candidate is allowed; loss of supported substance is not.
- Unsupported baseline language does not gain authority from this comparison.

## Universal Open Signal and additive canon

Every correctly resolved Discord subject may use bounded, question-scoped
public observations selected by the existing Ledger assessment selector. The
packet rechecks exact authorship, subject, route, visibility, lifecycle,
correction lineage, source text, root, and occurrence under one coherent read
snapshot. One materially supported point is an honest sparse Open Signal;
two materially distinct points with independent roots and occurrences may
support a rich answer. Near-paraphrases do not manufacture breadth, and
process-profile questions admit only process-relevant observations.

Canon never unlocks that personal evidence. On broad profiles it is ordered
after member-specific evidence and remains additive; canon alone leaves
profile sufficiency empty and normally triggers no candidate generation. The
only exception is an explicit identity/relationship question for an actively
bound account with an exact current Declared Canon relationship: BNL may state
that relationship once while making no claim of observed Discord activity.
Direct non-broad canon and lore questions retain their existing route.

## Same-platform canon recognition

Mac Modem, Cache Back, and DJ Floppydisc now have standalone facts in the
structured canon contract. A broad self-profile may connect one of those
identities to the requesting Discord account only when all of these conditions
hold:

- the member's current Discord display name is an exact, unambiguous approved
  canon name or alias;
- at least two active, public-safe conversation Ledger roots for that same
  Discord user carry the same approved name; and
- the signal still matches during send-time packet revalidation.

The result is a reversible response-time recognition signal, not a persistent
account merge or cross-platform identity claim. Near matches, ambiguous names,
one-row names, private rows, and non-public routes do not qualify. 6 Bit uses
the approved explicit same-platform account-binding path rather than automatic
name recognition; that binding adds canon only after generic member evidence.

Recognition adds the approved identity only after the same generic member
evidence rule succeeds. The configured owner/guild account is the explicit
same-platform 6 Bit binding; other approved entity bindings use the append-only
owner-authorized lifecycle. A single observation stays sparse; multiple
materially distinct observations may be rich only with independent roots and
occurrences. No assessment observation becomes a trait, Moment, or atomic-
memory candidate through response-time use.

Identity/relationship comparisons are bounded to one supported sentence and
cannot become the answer's organizing frame. Discord activity remains the
member evidence; canon only identifies the applicable entity and relationship
context.

An empty packet never suppresses a nonempty established response. The fixed
thin-record response is used only when both the packet route and the already
generated established path are empty.

## Retained-conversation catch-up

Startup runs one bounded, resumable pass through public retained user
conversations that predate their Ledger projection. It uses the existing
Ledger writer, excludes internal/sealed and model rows, and is idempotent.
Motif formation is explicitly disabled during this pass, so catch-up alone
cannot create or promote durable memory. Later restarts resume an unfinished
slice from the recorded cursor.

## Living Canon recurrence v1 boundary

PR 4 extends the existing Ledger/Moment/atomic owner with the versioned
`living_canon_recurrence_v1` contract. The nine curated motif families remain
strong labels, while an unmatched public-human meaning may be proposed under
a deterministic family-neutral signature. One eligible occurrence remains
provisional. Establishment requires at least two distinct immutable human
roots across at least two canonical occurrences; message volume, roles,
Relationship state, canon recognition, and confidence scores cannot replace
that proof.

Occurrence counting is exchange-anchored and bounded. Multiple rows in one
continuous exchange, a finalized Moment representing that exchange, and
overlapping Moment representations collapse to one occurrence. A correction,
deletion, privacy change, retraction, or supersession removes the affected
support, and post-correction establishment requires two fresh qualifying
occurrences.

This PR does not activate that formation path, run historical catch-up, or add
Living claims to the live packet. Its historical analyzer is content-free and
zero-write. Any bounded historical apply remains a separately approved
operation, and packet convergence remains PR 5 work.

## Diagnostics and rollback

Content-free synthesis receipts record `authority_mode` as either
`scoped_canary` or `public_home_broad_recall_owner`. Owner diagnostics report
the requested/effective state, authority mode, scope status, kill switch,
route counts, selection/fallback totals, revalidation, prompt ownership, guard
outcomes, validation-support lane counts, baseline/candidate supported
coverage, supported-coverage regressions, and live applications.

Rollback is the independent kill switch:

1. Set `BNL_PUBLIC_HOME_BROAD_RECALL_OWNER_ENABLED=false` or remove it.
2. Restart the bot.
3. Confirm diagnostics show the owner as requested/effective `off`.

With the gate off, no packet candidate is generated and the established path
is preserved byte-for-byte.
