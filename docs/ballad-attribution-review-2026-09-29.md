# Ballad source attribution review

The version-7 private audition assigned a speaker's joking suggestion to the
person being addressed, then described that person performing an action.
Current-source checks passed because the original evidence had not changed.
They did not compare the generated claims with that evidence.

Version 8 preserves the approved writer and adds one independent source-fidelity
review before a generated or polished draft can be saved or returned to the
website. It reviews lyrics, title, Style, palette and liner notes against the
same complete eligible episode. It distinguishes speaker, addressee and actor;
preserves questions, negation, hypothetical speech and banter; and checks concrete
numbers and temporal/causal connections. Obvious fictional imagery, metaphor,
supported paraphrase and musical montage remain eligible. This is not a taste,
novelty, rhyme or genre gate, and it does not require exact quotations.

Each human chat row now repeats its public speaker name next to its source
person reference, so the writer need not resolve a distant directory entry to
distinguish an author from an inline mention. This changes only the Ballad
projection; original sources, identity records and memories are unchanged.

The reviewer receives original evidence and the actual draft, without the
songwriter persona, prior-song catalog or producer instructions. A complete,
valid `supported` verdict with no issues is required. Rejected, uncertain,
malformed, truncated, unavailable or budget-blocked reviews cannot deliver
unchecked copy. A failure preserves existing versions and saves a replayable
failure receipt. There is no automatic rewrite, retry or fallback loop.

Both calls use existing Gemini accounting and limits. Review routes retain the
manual/background writer's spending lane and automatic-show protection, with a
4,096-token output bound and no provider retry/fallback. No configured spending
limit or reserve changes. The review adds input cost for the episode and draft
and adds latency; if its allowance is unavailable after writing, the command
fails rather than returning the unchecked candidate.

Sources are revalidated after writing and again after review. A correction,
withdrawal or privacy change prevents saving the draft even when its review
passed. A passing receipt records the source and draft digests in the existing
immutable version document. Human edits and restores do not inherit a stale
review certificate. Existing producer editing/publication authority is unchanged;
legacy drafts and manually edited content are not retroactively certified.

Offline tests exercise source-speaker preservation, reviewer isolation and
provider schemas, both budget lanes, rejection and unavailable-review handling,
receipt replay, saved-version preservation, edits/restores and source changes
during review. Mocked review verdicts prove enforcement, not model judgment.
Private model evaluation must show that the observed speaker/recipient and
joke/action failures are rejected while faithful paraphrase and creative imagery
remain accepted. A model review reduces risk; it is not an infallible factual
proof. Do not declare live semantic acceptance from code checks alone.

This is a bot-only change. No new production store, scheduler, factual authority,
website control contract or memory gate is introduced. Deployment remains a
separate decision after review. Rollback is a code revert and ordinary bot
restart; no data migration is required.
