# Journal, Relay, and shared-brain review — September 17, 2026

## Outcome and evidence limits

6 Bit requested public Discord names or confirmed chosen nicknames, clearer
attribution, less repetitive Journal writing, and a review of Journal/Relay
health and their connections to shared memory and newer show systems.

This review traces the merged bot source at `706caa3` and the changes in this
branch. It also inspected the public Journal archive. Production database and
service access are not available in the development workspace. Offline tests
prove the exercised behavior; they do not establish current production health,
successful future publication, or that BNL's next prose will be better.

The current publication-to-chat recall connection exists. The reverse path is
partial: Journal and Relay writers do not automatically inherit every source
available to ordinary shared-brain chat. In particular, a direct writer input
from the finalized show reader, public Moment reader, or published Ballad slot
has not been implemented by this branch. These are concrete integration gaps,
not evidence that those stores are broken.

## Changes in this branch

- Fresh Journal evidence now has a public speaker name linked to the existing
  stable Discord subject. A confirmed chosen nickname wins over the public
  Discord display name. Authority comes from active, directed member facts
  carrying public self-report/correction provenance or an explicit member
  control receipt. A bare profile preference cannot authorize a nickname.
- Owner projection always uses 6 Bit, scoped with `BNL_OWNER_USER_ID`.
- Matching display labels do not merge accounts. Ambiguous labels stay unnamed.
  Known public mentions retain their separate identities. Unsupported names,
  unknown mentions, IDs, and URLs are scrubbed at the existing packet boundary.
- The prompt distinguishes the message author from the person discussed, and
  proposal, authorship, testing, reply, and quotation. Jokes and roleplay stay
  attributed banter. Similar topics cannot establish that two people are one.
- The archived original remains in its existing source store. Only a bounded
  public projection enters the frozen writing packet. Existing anonymous frozen
  packets gain no naming permission. Confirmed nickname changes are rechecked
  before reuse of a frozen packet or release of a prepared entry.
- Writing no longer prefers two balanced sections, forces four repeated beats,
  or requires a first-person reaction. History is continuity evidence, with
  explicit guidance to vary titles, openings, development, and endings. Section
  count and word targets retain their existing bounds; no new writing judge,
  diversity quota, paid test generation, or retry loop is added.
- Relay's fresh and continuity conversation readers now also honor
  `public_usable` and public visibility when those columns exist. Previously,
  channel policy was the only eligibility filter in these two readers. Legacy
  schemas retain their compatibility behavior. Fresh timestamps are compared
  as timestamps, and speaker breadth uses stable user IDs rather than names.
- The new health command reads existing state and receipts without initializing
  schemas, generating content, fetching site controls, or changing the database.

The public archive showed repeated three-topic titles, anonymous actors, and
stock first-person reactions. One entry framed playful infrastructure/oversight
language as an unsanctioned project. Without its original conversation evidence,
that cannot be adjudicated here; the new attribution guidance directly addresses
the risk of promoting a participant's joke into a reported fact.

## Source and connection map

| Existing owner | Journal writer today | Relay writer today | Integration direction |
| --- | --- | --- | --- |
| Public Discord conversations and durable Journal source archive | Current-window evidence, participant aliases, coverage and timing | Fresh public messages and bounded public continuity | Source-linked names and speaker separation implemented here |
| Approved member facts and governance | Confirmed names added here; other member facts are not a general writer feed | Profiles used for redaction, not an unrestricted fact feed | Use only relevant public, source-bearing facts; preserve correction and forget lifecycle |
| Public TikTok chat archive | Eligible `tiktok_live_chat` Journal source events are read as conversations | No direct chat-reader input in the reviewed Relay selector | Preserve surface and consent-bound identity; never match Discord accounts by display name |
| Approved broadcast memory | Scoped, active, public-safe records; date, supersession and provenance checks | Scoped public-safe quiet-source candidate | Keep its authority separate from community interpretation |
| Approved canon/source contract | Historical/reflection basis when appropriate | Approved canon anchors | Reuse established authority and corrections |
| Earlier Journal entries | Prior entry, topic recurrence, continuity notes, relevant older excerpts | No general Journal-to-Relay writer feed | Historical callbacks; never duplicate testimony for a current event |
| Accepted Relay publications | Fresh and historical Journal evidence | Persistent diversity history, cursor and restart hydration | Already connected; retain publication identity and acceptance receipts |
| Moment/episode engine and Ledger | No direct call to the current public Moment selectors | No direct call in the reviewed writer selectors | Reuse `select_public_situation_moment_gists` and participant contribution readers for relevant prior situations |
| Finalized show evidence ledger | No direct finalized-show reader input | Current fresh prompt explicitly limits show statements to supplied Discord evidence | Reuse `select_tiktok_show_episode_context_items`; separate recorded operations, public dialogue, preparation, and derived patterns |
| Broadcast Ballads | No direct public catalog input | No direct public catalog input | Read the site's one selected, published song slot and matching saved version; bot draft history alone cannot prove publication |
| Public Journal/Relay read adapters | Publications feed back into ordinary chat through canonical readers | Same publication recall path | Already selected and revalidated in shared-brain prompt paths; publication prose does not become new independent proof |

Relevant implementation owners: `bnl_journal.py`, `bnl_journal_source_store.py`,
`bnl_journal_automation.py`, `bnl_website_relay_state.py`,
`bnl_shared_brain_synthesis.py`, `bnl_moment_engine.py`,
`bnl_tiktok_show_ledger.py`, `bnl_broadcast_ballads.py`, and the existing bot
publication source-basis builders and final-send revalidators.

## What the expanded connection should look like

Build one compact editorial evidence selection over existing stores, with
source type, stable subject, time, public eligibility, and source version on
each item. Make all eligible source categories available for selection; do not
require every article or Relay to contain every category. Relevance and the
current writing window should decide what BNL sees.

1. **Current activity:** public Discord conversations, eligible live-chat
   observations, accepted relays, and finalized show events inside the window.
   Keep recorded show operations separate from people's comments about them.
2. **Continuity:** retrieve a few relevant public Moments and episodes, approved
   facts, broadcast memory, and earlier published entries. Keep the original
   participants attached to their contributions. A recurring theme is a useful
   association, not permission to add today's person to yesterday's event.
3. **Creative work:** a selected, publicly published Ballad supplies its title,
   artist credit, show association, style, and approved liner notes. Lyrics are
   creative material. A joke in a song is not evidence that an incident occurred.
4. **Writing:** select an angle, then make one ordinary writing request using
   that bounded selection. A Relay takes a small timely slice; a Journal can
   connect a development over several moments. Quality comes from the supplied
   evidence and direction, not a stack of model reviewers.
5. **Remembering:** preserve the existing publication record and its source
   lineage. Later recall can say what BNL wrote, but must return to original
   sources to establish what actually happened. The same episode retold in
   Discord, a Relay, a Journal, and a Ballad counts as one underlying event.

Illustrative story, not an assertion about a real broadcast: Test Artist shares
a demo, Test Listener tests its chorus, and a finalized show record later
confirms its playback. BNL can connect the development and credit both people
for their actual roles. If a Ballad later references it, that adds a creative
callback rather than another witness to the original event.

The concrete next adapter work is the public Moment and finalized-show input.
Reuse their current source eligibility and version readers at packet creation
and publication. Carry their source versions through the existing frozen packet
and release preflight, rather than treating cached prose as permanent authority.
The existing Moment selector currently initializes schema; its publication read
path needs a genuinely read-only option before reuse under a release fence.

For relays, the site already accepts `public_safe_memory` as a source class.
A selected public Moment or finalized-show history view can use that contract,
with explicit historical framing. Persist its source basis with the pending
payload so retry after a restart can recheck it; a hash without its reference
and selector inputs is insufficient. This requires extending the pending
publication metadata, not merely adding more text to the model prompt.

The Ballad adapter must use site publication/selection authority and carry the
selected version into its source digest. Archiving or replacing that one slot
must invalidate a pending statement about the current song. Public Dossiers,
Transmissions and other site material should enter through their approved
public read models, when relevant, with the same source/version distinction.

## Resilience review

| Path | Source finding and exercised protections | Remaining live evidence |
| --- | --- | --- |
| Source capture | Durable source archive, identity deduplication, activation watermark, legacy backfill, privacy purge paths | Recent capture timestamps and accepted Relay/archive receipt counts |
| Journal preparation | Frozen packet, attempt/lease epochs, bounded cycles, preparation/delivery separation, targeted repair | Next preparation outcome and actual usage; a passing test does not prove prose quality |
| Journal delivery | Exact canonical payload, content hash, privacy/control revalidation, retry and idempotent publication | Receiver acceptance and the visible new revision |
| Relay delivery | Pending v2 payload survives failures; cursor advances after acceptance; retry retains publication identity | Pending age, recent outcomes and next scheduled acceptance |
| Restart recovery | Journal prepared revision reuse and Relay diversity/cursor hydration have existing coverage | Running service version and natural post-restart event |
| Shared-brain recall | Canonical Journal and accepted Relay readers; source digests and site visibility/reuse checks before final send | Rendered/sent lane receipts and one natural relevant recall turn |
| Budget | Existing daily/monthly restrictions and retry ceilings remain authoritative | Current counters and hold reasons; no reset or budget bypass in this work |

The full regression suite includes publication read adapters, prompt lifecycle,
shared-brain routing, show consent, Moment lifecycle, Journal release fences,
and Relay recovery. These checks cover the code path, not an unseen production
configuration. A quiet period or unused source lane is not automatically a fault.

## Deployment and focused production evidence

After merge:

```bash
cd /home/ubuntu/bnl01 &&
git pull --ff-only origin main &&
venv/bin/python -c "from bnl_journal import JOURNAL_EDITORIAL_VERSION; assert JOURNAL_EDITORIAL_VERSION == 'journal-public-voices-1'; print(JOURNAL_EDITORIAL_VERSION)" &&
sudo systemctl restart bnl01 &&
systemctl show bnl01 -p ActiveState -p MainPID
```

Read existing production state, without generation or writes:

```bash
cd /home/ubuntu/bnl01 &&
sudo /home/ubuntu/bnl01/venv/bin/python -m scripts.journal_relay_health \
  --db /home/ubuntu/bnl01/bnl01_conversations.db \
  --guild-id 1288269405209235551 \
  --pid "$(systemctl show bnl01 -p MainPID --value)"
```

After the next natural scheduled cycle:

```bash
sudo journalctl -u bnl01 --since "1 hour ago" --no-pager \
  --grep='journal_preparation_finished|journal_automation_cycle|website_relay_delivery_failed_no_cursor_advance|website_relay_no_publish' -n 40
```

Check the next newly built Journal packet for `journal-public-voices-1` in the
report, source-supported names and distinct actions in the published text, and
the normal acceptance outcome. Inspect recurring holds or missing archive
receipts before declaring production healthy. The report prints counts, states,
timestamps, and lane usage; it does not print messages, identities, or secrets.
