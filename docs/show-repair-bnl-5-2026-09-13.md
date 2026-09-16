# Show repairs, package 5: creative behavior and broadcast credits

Base: `edbe937fd0733d8affc7f08e64f799f2720bf169` in `6-Bit-01/BNL01-Bot`. This completes the bot credit companion to site PR 423 and the B01–B03 prompt/behavior work. Shared-brain repair remains paused. No live gate, database, conversation, memory, queue, submission, payment or show-history value is changed by this draft.

## Evidence and implementation

The active ordinary-chat route uses `BNL01_PACKET_OWNED_SYSTEM_PROMPT` and returns before the older glitch rewrite. Editing only the legacy rewrite would miss that route. The new shared creative protocol is included in both active and legacy system prompts, and its glitch rules also apply to the existing optional legacy rewrites. The prompt construction adds no provider call, transport, memory reader or factual evidence source.

Glitches use brief corrupted symbols, broken punctuation, redaction gaps and varied fragments; spoken sound effects such as “bzzt” and stage cues are prohibited by the prompt. Glyphs stay out of vocal lyrics and spoken sound-effect instructions, and names/titles/links stay readable. The effects are expression rather than factual evidence of a real fault or hidden record. Optional per-turn suggestions rotate forms and generate fresh glyph/style combinations. Recent-repeat avoidance is bounded and process-local, not durable memory or a lifetime uniqueness guarantee. Suggestions do not force a glitch or song into unrelated replies, and explicit user instructions take precedence.

Default song requests receive `1. Lyrics`: at least 1,400 characters of original lyrics, with clear section tags; then `2. Style`: a year or short range wholly within 1970–2010 and 2–4 contrasting genres. Ordinary brevity must not shorten requested lyrics. Hip hop is not the default; explicitly requested hip hop, other eras, shorter lengths or another format override the defaults. A lightweight randomized suggestion samples contrasting genre families; the model can follow the user's context and approved feedback instead. No output is padded with filler to claim the minimum was met.

This is a paste-ready writing protocol, not Suno automation. Official guidance confirms Custom mode accepts user lyrics, and Suno's current guide describes structured lyrics and Style in its advanced controls. The requested length/era/genre defaults are 6 Bit's creative rules, not Suno product limits. Sources checked September 13, 2026: [Suno: own lyrics](https://help.suno.com/en/articles/2415873), [Suno: making a song](https://suno.com/hub/how-to-make-a-song).

For show songs, authorized context supplies the real people, submitted artist/title credits, actual plays and banter. Prompt guidance uses supplied approved feedback and earlier song context without inventing persistent learning or treating creative lyrics as facts. Existing consent, memory and source-governance owners remain in place; no new learning database or hidden feedback capture is added. Hosting approved show songs and reviewed links to existing artist/song IDs remains the separately planned feature.

The generic `_track_label` previously preferred detected filename/uploader metadata. It now prefers submitted and legacy explicit broadcast labels; provider-only data does not establish a creator. A missing side of a credit is marked unknown. Provider metadata, alias query matching and durable artist-memory identity logic remain intact; only display attribution changes.

## Validation and practical limits

Four new focused tests cover conflicting submitted/provider credits, 100 seeded varied suggestions within the era/genre bounds, active-route prompt construction with one provider call and intact long lyrics through existing Discord chunking, and an explicit short hip-hop override on the normal route. Provider transport is mocked: these prove integration and preservation, not live model compliance or artistic quality. `make check` passed. Because the redirected output lacked a final summary, a counted full-suite run independently confirmed all 3,077 discovered tests ran, with zero failures, errors or skips. Both supported Python CI versions must also pass before the handoff is marked fully verified. Real lyric length, variety and glitch quality need the later owner rehearsal; no unsolicited Discord test messages are sent.

## Normal post-merge deployment

After review and passing Python 3.9/3.12 CI, update the existing VPS bot checkout to the reviewed main commit with its established fast-forward deployment process. Keep the current environment file, existing database path, current data and feature flags. Use the existing virtual environment; dependencies do not change. Restart the existing bot service through its normal service manager and verify the running process uses the reviewed commit. Do not create a replacement empty database, switch memory ownership, restore a backup or flip a shared-brain gate. Coordinate site PR 423 for full cross-surface credit acceptance. No automatic merge, VPS deployment or Discord post is part of this draft.

## Exact focused post-deploy checks and evidence

From the reviewed bot checkout, with its existing Python environment:

```bash
python -m unittest discover -s tests -p 'test_show_creative_protocol.py'
python -m unittest discover -s tests -p 'test_showday_queue_alignment.py'
python -m unittest discover -s tests -p 'test_packet_recovery_style.py'
```

In the later authorized private rehearsal, request an end-of-show song from the supplied real show context. Save the exact reply and source show/session reference. Count only the lyrics section (excluding the Style section); require at least 1,400 characters and clear structure labels. Check the style year/range and 2–4 genres; ask again with an explicit short hip-hop override and verify it is honored. Across several requests, inspect variety and any occasional glitch fragments; prohibit audible glitch words. Verify creator and title credits against the current queue's submitted labels, including a conflicting uploader/filename fixture. Preserve the provider call-count receipt to confirm the active route still uses one call. Do not assume an unplayed submission was broadcast merely because it appears in full show context.

Capture deployed commit, service health, current gate/status output, source/queue session IDs and the reply receipts. Compare that existing database path and the current record counts before and after deployment without printing private content or restoring older rows. For a transcript-side length check, save the authorized reply locally and use this read-only command:

```bash
python - <<'PY'
from pathlib import Path
text = Path('rehearsal-song.txt').read_text()
lyrics = text.split('1. Lyrics', 1)[1].split('2. Style', 1)[0].strip()
print('Lyrics characters:', len(lyrics))
assert len(lyrics) >= 1400
PY
```

## Recovery

Use a compatible source revert or focused forward repair. Keep the current database, memory, queue, submissions, show logs and settings. The only new variation history is an in-process bounded list of creative suggestions; a service restart resets that list, not BNL memory. No source-data restoration or schema migration is required.
