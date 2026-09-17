# BNL Broadcast Ballads v1

## Complete response fields and prompt revision 5

The September 17 revision-4 audition returned an unfinished JSON string. The
old fallback saved the whole response as lyrics, including escaped newlines,
and substituted the dated title. Style and track-story fields after the cutoff
were never received. The supplied evidence proves incomplete output; it does
not contain the provider's finish reason, so token exhaustion is not confirmed
for that particular call. The site accepts substantially longer lyrics; this
failure is in response generation/parsing, not its textarea length.

`broadcast-ballad-5` retains revision 4's songwriting guidance. Both Ballad
routes now request `application/json` with the existing Google SDK's
`response_schema`. The required keys are ordered `title`, `style`, `palette`,
`linerNotes`, `lyrics`, keeping short track fields ahead of the long lyric.
Palette and the four track-story keys are also required strings; empty strings
remain allowed when there is no supported information. See Google's
[structured output documentation](https://ai.google.dev/gemini-api/docs/structured-output).
No second model call writes the notes.

Ballads have their own 16,384-token response allowance, replacing the generic
4,096-token allowance. `BNL_GEMINI_BALLAD_MAX_OUTPUT_TOKENS` can override it
within 4,096–32,768. Existing reservations include this larger upper bound;
daily/monthly dollar limits, token ceilings, billing buffer and protected
reserves still apply. Actual spend can increase for longer responses. Manual
and automatic priorities remain distinct, with one provider attempt and no
fallback, automatic completion or retry. Other routes keep their settings.

Interrupted JSON with usable lyrics is decoded into readable fields without
inventing missing words. Complete escaped characters survive; partial escapes
are left out of the displayed text. The full original response stays in that
immutable version's `rawOutput`. A visible note explains the incomplete draft
and the manual completion action. `generationStatus` and `finishReason` record
the technical result; `MAX_TOKENS` marks a draft incomplete even if the JSON
closed. Plain-text songs remain usable. An object with no recoverable lyrics
returns a visible format failure, leaving prior versions intact. Missing or
malformed optional track notes still do not reject usable lyrics.

Deployment does not change old versions. For the affected existing song,
**Polish saved draft** recognizes the old raw-JSON fallback, recovers its title
and readable lyrics in the prompt, and asks to finish the ending and fill Style
and track notes while preserving the existing wording. This uses one explicit
generation and creates a new version; neither the original nor any selected
recording is replaced. Missing words cannot be recovered byte-for-byte because
they were not received. Producer edits are not replaced with older raw output.

No site code change or data migration is needed. After merging and deploying
the bot, select the affected show's latest draft, use Polish saved draft once,
and verify title, actual lyric line breaks, the final section, separate Style,
and Track story & people for the new version. Optional recording metadata
(Suno URL/model/settings or artwork) is still entered by the producer.

The content-free log below identifies the actual new generation, unlike an
installed-file check. Expect `prompt_version=broadcast-ballad-5`,
`status=complete`, `finish_reason=STOP`, nonzero lyric/Style counts and the
applicable track notes. An `incomplete` status remains visible and must not be
treated as a finished song. A blank note can be appropriate where no person is
mentioned; it must not be fabricated just to fill a field.

```bash
sudo journalctl -u bnl01 --since "10 minutes ago" --no-pager \
  --grep='ballad_draft_saved' -n 5
```

Focused regressions cover long complete structured responses through the real
worker with a fake provider, cutoff at escaped lyric characters, nested notes,
provider finish status, old-draft Polish, unchanged prior versions, receipt
replay, route reservations and unrelated output formats. These offline checks
do not establish live model completion or the next song's artistic quality.

## Verse craft and prompt revision 4

`broadcast-ballad-4` responds to a live draft that still relied on simple end
couplets after revision 3. The shared Discord/site guidance now demonstrates
multisyllabic writing in a melodic four-line scene and denser rhythmic phrasing,
with a concrete instruction to compose each verse around related sound families.
The examples are original fictional teaching material, explicitly separate from
show evidence and lyrics to reuse. The guidance preserves natural stress, humor,
genre choice and room for a simpler hook. Google's
[prompting guidance](https://ai.google.dev/gemini-api/docs/prompting-strategies)
supports using complete, varied examples to demonstrate the desired pattern.

The catalog previously supplied up to 2,200 characters of each prior lyric,
including the current show's last attempt, to a fresh Generate request. That is
an observed input issue and a plausible source of imitation, not a proven sole
cause of weak rhymes. Fresh drafts now receive compact references: title, style,
palette (including hook, topics and imagery), producer feedback and up to 24
distinct literal line endings, each up to 80 characters. These references record
what has been used; they are not exemplar verses or a rhyme blacklist. Selection
still favors the confirmed recording's lyric version. Older matching songs are
still retrieved using their full stored lyrics and palette. Full lyrics, all
versions and their site display remain intact.

Only an explicit Polish supplies the full existing lyric to revise, once; its
duplicated raw response is omitted from the model input. Generate asks for a new
composition in the saved producer direction. Both retain one provider attempt,
unchanged model/budget settings, optional track stories and usable-output retention.
There is no rhyme classifier, rejection threshold, critic or retry loop.

Regression tests exercise fresh generation after a prior draft, an explicit
polish, confirmed-version catalog selection and original-version preservation.
They establish the input and storage behavior, not live creative acceptance.
For the next audition, use one new Generate request with the same show/direction;
compare sustained phrase rhymes across both verses rather than accepting one
isolated two-syllable pair. Check the saved generated version's `promptVersion`
is `broadcast-ballad-4`; a checked-out file alone does not identify an older draft.

## Rhyme guidance and prompt revision 3

`broadcast-ballad-3` strengthens the shared songwriting guidance used by both
Discord and the site. It illustrates multisyllabic and word-spanning rhyme,
asks for related sounds across nearby lines and internal echoes, and keeps
natural stresses and meaningful phrasing. Simple rhymes can support a hook or
contrast without becoming the default pattern throughout the verses. The sound
examples illustrate technique; they are not stock lyrics to reuse.

The Ballad brief ends with a short writing reminder after show evidence and
creative history. The actual worker now includes the full shared songwriting
protocol once, removing the duplicate previously supplied by both the system
prompt and standalone Ballad brief. This reduces repeated input without removing
show evidence, creative history or track-story instructions.

These are first-draft prompt changes, following Google's guidance on
[concrete examples](https://ai.google.dev/gemini-api/docs/prompting-strategies)
and [placing the request after long context](https://ai.google.dev/gemini-api/docs/long-context).
There is still one provider attempt per command, with no rhyme scorer, output
rejection, critic call or automatic rewrite. Tests check delivery of the prompt
and successful receipt replay without another generation; they do not establish
the musical quality or rhyme choices of the next live song. Existing songs and
confirmed selections are not regenerated or replaced by this update.

## Track stories and prompt revision 2

`broadcast-ballad-2` keeps the existing single-attempt writer and adds optional
`linerNotes` to the same JSON response: `about`, `inspiration`, `mentions`, and
`inspiredBy`. These are concise public strings, each stored up to 1,500 characters.
BNL's inspiration is a first-person creative note about the supplied broadcast
and musical choices. Mentions identify people actually named in the lyrics;
inspiration can also identify relevant people or moments from the public show.
Neither field grants a performer, collaborator or endorsement credit. Private
producer feedback and instructions remain outside public copy.

The small Ballad-specific prompt adjustment favors natural phrasing, purposeful
hooks and a structure suited to each song while retaining BNL's technical humor,
swagger and unusual choices. Creative history labels confirmed selections and
includes bounded feedback from their original saved producer directions. This is
context for the next draft, not model training or approval of every selected line.
The shared Discord songwriting protocol, provider limits and budget routing are
unchanged. Prompt revision 1 and all original drafts remain in repository/history.

Missing or malformed story fields become empty strings; usable lyrics are saved
without a second call. Older versions restore normally. Producer lyric edits and
restores carry the original generated story; the site provides a separate Save
track story action to correct or expand public notes for that exact version.
Publication/archival freeze the selected recording's story, independently of later
private notes or draft changes. No generation or publication runs on deployment.

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
The provider call uses existing tracked/budgeted generation: one attempt, no
provider retry or fallback, a bounded wait, and no general chat post-processing loop.
Manual Generate/Polish commands use `broadcast_ballad_manual` with the same dollar
priority as direct Discord requests. Automatic commands use
`broadcast_ballad_background` with the existing show-day priority. The authenticated
site creates automatic command IDs as `auto-<showId>` and admin command IDs as UUIDs;
that existing contract selects the route, including commands queued before this fix.
The generic optional-work pace check no longer suppresses requested songs or the
single automatic show draft. No budget amounts increase. The global hard ceiling,
billing buffer, token limits and Journal reserve still apply; automatic drafts also
leave the interactive reserve untouched. A real budget refusal is delivered as
`budget_restricted:<reason>`. The failed receipt is cached without model retries.

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
