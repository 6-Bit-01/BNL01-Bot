# BNL songwriting: session coverage and craft

## Observed failure and status

After the website timeout repair in #553, the private rehearsal lookup succeeded
and the supplied song stayed with `BARCODE Radio [09-15-2026]`. Its Lyrics/Style
output had no decorative corruption. This is useful live evidence, not completion
of the creative acceptance run.

The song covered only B2 Complete and B3 Partial Priority, then asserted that the
show had only one artist and two submissions. Those totals were unsupported.
The route receipt reports `website_read_model_no_store`; it does not enumerate
the rows sent to generation. Source inspection and a reproduction establish the
mechanism: #552 refreshed the website snapshot but reused the earlier human
lookup as a track filter. With five fixture tracks, the song received only B2/B3.
The live provider payload itself was not captured.

The supplied lookup and song also called B3's four-second cutoff playback
`not_evidenced`. Earlier acceptance described a partial play. This discrepancy is
still open. A missing start in a bounded record does not prove silence, and this
change does not invent a start or mark playback acceptance complete.

## Implementation

The existing Context-selected continuation still identifies the same authorized
session and refreshes it. Its queued, completed and removed records now reach
the model with playback distinctions, rather than inheriting a permanent B2/B3
filter. The current request determines response focus. Ordinary first-turn track
lookups stay selective; an explicit new show or revoked access still removes the
old session. Available collections are explicitly not proof of complete history.

The existing shared creative protocol now supplies a compact craft brief for
songs and revisions. It covers meaning-led multisyllabic/phrase/internal rhyme,
natural stress and breathing, hooks and development, specific imagery, coherent
instrument roles and section dynamics. It rejects automatic cyberpunk filler and
unsupported participation totals. The optional genre palette grows from four to
twelve families; the default two-to-four-genre and 1970–2010 bounds remain.

No new routing owner, model call, output filter, storage, permission, configuration
flag, Suno integration or model training is introduced. User overrides and normal
BNL prose retain their existing behavior. Added text is guidance, not proof that
the live model will write excellent lyrics or that Suno will execute every cue.

## Research used

Reviewed 2026-09-17. These are primary sources. The runtime brief is our synthesis;
the sources do not endorse it or prescribe BNL's defaults.

| Source | Useful finding and application |
| --- | --- |
| [Suno: Custom lyrics](https://help.suno.com/en/articles/2415873) | Custom mode accepts supplied lyrics. BNL continues producing text for the owner to paste, without claiming to create audio. |
| [Suno: Music Glossary](https://help.suno.com/en/articles/9010177) | Musical vocabulary can describe tempo, intensity, instruments, structure and production. Apply a few coherent audible directions, not a long list of unrelated adjectives. |
| [Suno: Creative Sliders](https://help.suno.com/en/articles/6141377) | Weirdness and Style Influence are distinct controls. The brief avoids presenting them as commands embedded in lyrics or promising exact audio results. |
| [Pat Pattison: Language and Songwriting](https://www.patpattison.com/language-and-songwriting) | Respect ordinary speech stress when fitting words to musical emphasis. Use comfortable phrasing and breathing instead of bending syntax to complete a rhyme. |
| [Pat Pattison: Journeys with Rhyme Schemes](https://www.patpattison.com/copy-of-pov-verse-development) | Rhyme placement changes momentum and emotional effect. Vary placement and density deliberately; complexity is useful when it serves the idea. |
| [Berklee: Songwriting Tools and Techniques](https://online.berklee.edu/courses/songwriting-tools-and-techniques) | The curriculum connects concrete imagery, point of view, rhyme types, melodic phrasing, harmonic tension and section contrast. Use those dimensions as compositional choices. |

Multisyllabic practice, broader genre choices, anti-cliche direction and the final
quality check are implementation choices responding to the owner's request.
There is no claim of copying a particular writer, mastering their craft, or
persistent learning. Section cues are suggestions, not a documented deterministic
Suno command language. No third-party lyrics are reproduced here.

## Verification and remaining acceptance

The new direct/batch regression fails on the previous code for both the song and
revision, then passes with the repair. The fixture contains other artist credits,
a loaded-only completed item, a queued item and a removed item. It checks their
delivery without falsely converting them into confirmed plays. Existing tests
cover private/public boundaries, session changes, unrelated public recall and
explicit overrides. Provider-boundary tests verify that the same creative brief
reaches the existing paths and unlabeled feedback without another model call.

Validation: 60 focused tests passed; `make check` compiled the code and passed
all 3,098 tests. `git diff --check` passed. These results verify source delivery
and code behavior, not live lyric quality.
After deployment, resume the existing private song/revision acceptance. Assess
session coverage, truthful playback language, clean lyrics and actual creative
quality; retain earlier accepted checks. Full creative acceptance remains open.

Rollback is a normal revert of this change; there is no data migration.
