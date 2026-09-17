# BNL Broadcast Ballads v1

BNL owns the writing. Each finalized public broadcast gets one song slot on the
site, locked when the producer confirms the selected audio. Site archival clears
the slot or atomically replaces it, while retaining the previous song and prompts.
Drafts and revisions remain available as creative history.

## Writing and continuity

The existing creative protocol now asks BNL to choose a hook, angle, a few specific
show moments and a musical movement before drafting. It preserves swagger, jokes,
exaggeration and creative risks. This is first-draft preparation, not a scorecard.
There is no minimum-quality rejection, automatic polish, critic call or rewrite
loop. The producer may request one light polish; it creates a new immutable version.
Even non-JSON/short provider output is saved with a note so usable material survives.

Two real conversation failures are repaired in their existing owners:

- A standalone, explicitly dated show-song request gets the detailed authorized
  queue records even when it was not preceded by a factual lookup.
- Unaddressed revision instructions such as “Make the chorus less repetitive…”
  reach the generation path. Same-show follow-ups refresh the selected human source
  request, rather than treating prior BNL lyrics as evidence. Explicit new dates
  and public/private source boundaries continue to win.

The regression follows standalone song → unprefixed chorus feedback → eight-line
2020/no-Style rewrite through actual Context/direct/batch composition. It also
checks other artists from the session and refusal to reuse BNL-invented facts.

## Ownership and runtime

`bnl_broadcast_ballads.py` adds `bnl_ballad_commands` and `bnl_ballad_versions` to the
existing bot SQLite database, lazily with `CREATE TABLE IF NOT EXISTS`. Versions
preserve lyrics, Style, title, palette (topics, hook, imagery, genres, era, arrangement),
producer options, raw provider output, source digest, prompt version, parent and
content hash. Edits and restores make new versions; nothing overwrites old lyrics.

Before writing, BNL receives recent songs and relevant older catalog entries. The
site's accepted/public selection chooses the appropriate version, rather than
assuming the last experiment is the released song. All revisions remain stored;
only prompt retrieval is bounded. Repetition avoidance is guidance, not a veto.
Catalog text is explicitly creative work, never factual evidence or memory/canon.

`build_broadcast_ballad_evidence` extends the existing finalized, authorization-bound
show ledger reader. It selects the exact show ID. Private rehearsals are unavailable
for durable Ballads, while ordinary authorized rehearsal songwriting remains ephemeral.

The existing website heartbeat starts one independent Ballad control cycle at a
time. The primary configured guild handles the shared site queue. GET/POST use
`/api/bnl/ballads`, the existing website base URL and `BNL_API_KEY`; no new credential
or timer owner is introduced. Source evidence loading runs off the Discord loop.
The provider call uses existing tracked/budgeted generation with route
`broadcast_ballad_background`: one attempt, no provider retry or fallback, a bounded
wait, and no general chat post-processing loop. It preserves existing budget reserves.

The site defaults automation off. A saved producer setting enables future finalized
public shows. Manual Generate/Edit/Restore requests remain separate from Publish.
The site inserts a stable automatic command per eligible show, so poll retries,
source corrections and archival cannot create additional automatic songs.

SQLite claims each command before provider work. Transport retries resend the exact
saved receipt without another generation. A competing worker sees pending rather
than generating again; interrupted claims become a visible failure after ten
minutes and require a new explicit request. A newly finalized show may wait up to
15 minutes for the existing show ledger to catch up, without making any model calls.
Missing evidence after that, provider/budget failures and format notes are surfaced
without deleting prior work. No automatic quality retry follows any of them.

## Contract

The paired site contract is `docs/bnl-broadcast-ballads-v1.md` in
`6-Bit-01/barcode-network-site`. The bot stores canonical writing and command receipts;
the site owns producer settings, Blob audio, the locked show-song slot, archival and
public snapshots. Neither creates a second queue, Journal, source owner or memory.

`GET /api/bnl/ballads`: contractVersion 1, commands and catalogVersions.
Commands: id, showId, showDate, kind, baseVersion, options, optional content or
restoreVersion. `POST`: showId, commandId, outcome and version or safe error.

## Verification and release

Focused tests cover same-show conversation continuation, real ledger selection,
control-cycle delivery loss/replay, one provider attempt, missing authorized evidence,
short/unstructured output retention, failed polish preservation, edit/restore conflicts
and catalog-aware prompting. Run `make check` and verify Python 3.9 syntax alongside
current Python 3.12 CI. Model creative quality still needs a real audition; mocked
transport/provider tests do not claim that Suno or live generation was exercised.

Changed files: `bnl01_bot.py`, `bnl_creative_protocol.py`,
`bnl_tiktok_show_ledger.py`, new `bnl_broadcast_ballads.py`,
`tests/test_rehearsal_song_followthrough.py`, new `tests/test_broadcast_ballads.py`,
and this contract. Existing payments, native queue semantics, Journal, Relay,
relationship/memory gates and other production flags are unchanged.

Deploy the paired reviewed site/bot branches together. Reuse current website/key,
primary guild and public-show evidence configuration. Keep automation off initially;
verify one private workspace run against an existing public show, then save automation
when ready. No VPS restart, live model call, Discord post or public publication was
performed during implementation. Rollback by disabling site automation and reverting
the paired code changes, retaining SQLite tables, site records and audio history.
