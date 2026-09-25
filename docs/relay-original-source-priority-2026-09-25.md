# Relay original-source priority — September 25, 2026

The owner observed the scheduled 11:10 Pacific Relay describing a published
Journal rather than the community activity behind it. The public v2 receiver
confirms Relay bnl-abdd5ed5bc07cc8ba4c4f5eb7e5f6542, published at
2026-09-25T18:10:18.798Z. This proves publication resumed, not the exact installed
bot revision or that current Discord activity was available at selection time.

Wednesday's PR574 added published Journals to the approved quiet-source
rotation. That was a real difference from pre-Friday main 10df1c7 (PR566),
not part of the unchanged Gemini request path. Fresh, sufficiently strong
Discord still takes priority, but below-threshold/quiet selection could promote
an unseen Journal over available original public conversation, Moment or show
sources merely for category diversity. A deterministic integration regression
reproduces that exact selection on PR588 main: published_journal wins while
conversation_continuity is eligible.

The existing selector now defers Journal-primary candidates when eligible
conversation continuity, a source-revalidated public Moment, finalized show
operations or scoped broadcast memory is available. Rotation among the remaining
approved categories continues. Journal memory is retained: when no original
source is eligible, an authorized dated Journal callback remains possible. This
is not a ban on mentioning Journals and introduces no mandatory output word.
Shared readers, visibility/reuse controls, source revalidation, pending-payload
replay, cursor advancement and hourly cadence remain unchanged. No fresh
activity is inferred from historical publication time.

The Journal's existing source-period ownership is unchanged. Friday's Daily
cutoff is 18:30 Pacific and publication is 19:00; the 19:00 show belongs to a later
eligible source window. This repair does not change budgets, model selection,
submissions, payments, gates, server state or scheduled tasks. README provider
wording is corrected to the already merged PR587 policy: one reserved fallback
call after primary HTTP503, with no per-model retries.

Focused verification passes 86 tests. The source-priority regression fails on
the old code with actual selected class published_journal, then passes with the
repair. The PR receipt records the full make check result and tested tree.
Fixtures make no real model or publication calls. Deployment is an owner-operated
normal pull/restart after merge; a subsequent naturally scheduled Relay is the
runtime observation. No forced public test or paid probe is required.
