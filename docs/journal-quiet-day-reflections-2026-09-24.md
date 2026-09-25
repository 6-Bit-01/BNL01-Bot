# Quiet-day Journal: imagination with dated evidence

## Incident and owner direction

The September 24 Daily covered September 23 at 18:30 through September 24 at
18:30 America/Los_Angeles. Its captured packet contained 14 accepted Relays and
zero conversations. The Relays recalled completed shows, conversation
continuity, Ballads, canon and earlier Journals. The writer treated those new
Relay publications as fresh community activity, including inquiries from an
earlier Journal. A Relay's publication time did not establish when the
underlying activity occurred.

The owner reports the last Discord activity was around 17:00 the previous day,
before the window. This evidence does not establish a Discord collection
failure. The requested behavior is to keep publishing on quiet days, allowing
BNL's thoughts, memories, opinions and clearly imagined scenes, without
inventing real community activity.

## Behavior

The existing Journal packet now classifies retrospective Relay topics as
reflection before sampling fresh sources. Up to eight diverse callbacks can
inspire an entry. They retain their Relay publication time and known original
source dates separately. A prior Journal's source window remains its own;
unknown origin dates remain unknown. A finalized show's selector lookback is
not used as its event date. The Relay source owner also carries actual Moment
and show dates forward for future publications.

Raw eligible Relay counts remain diagnostic evidence. Only current-activity
Relays, eligible conversations and the existing completed-show reader contribute
to current activity. Historical callbacks cannot crowd out current sources or
make a callback-only day meet the active-day threshold. A show whose recorded
completion falls inside the current window remains legitimate current evidence.

Quiet-day writing may combine dated memories, music, canon, recorded queue/show
context and BNL's own thoughts. Metaphors, opinions, playful speculation and
daydreams are welcome. Natural framing such as "I imagine" or "in my head"
distinguishes an invented scene from an account of actual events. BNL need not
add a boilerplate disclaimer or a canned nothing-happened entry. Personal
thoughts do not require the external-factual-inference context lane.

Claims that people spoke, submitted music, paid, arrived, released a project or
changed the queue still require evidence. Recorded shows remain in their actual
period; released songs and their creative interpretation are not additional
witnesses. Low input counts do not establish that nobody was active.

The existing validator checks explicit current-time/activity language against
fresh citations, including titles and excerpts. It allows framed imagination
and personal reflection while retaining the factual inference rules. Framing
is checked per clause: an imagined scene cannot lend authority to an adjacent
factual assertion. Ambiguous compound scenes require renewed explicit framing.
These
checks and the prompt are guardrails, not a semantic proof of every generated
sentence. Live prose remains a separate observation.

New packets and saved metadata carry `journal-dated-reflection-1` and the
`creativeReflectionAllowed` flag. Old unsent frozen packets, and old saved
drafts citing retrospective Relays, are retired through the existing invalidation
and owed-occurrence recovery path. Already published entries are not rewritten.

## Verification

Focused deterministic tests reproduce the 14-callback/zero-conversation pattern,
preserve source dates, allow thoughts and imagined scenes, reject unsupported
current-day activity, retain real current conversation evidence, store the new
lineage, detect affected stale work and prepare the same scheduled occurrence
only once. The full `make check` result and exact tree belong in the PR receipt.
No paid model call or public generation is needed for these tests.

The Journal keeps its existing source archive, privacy rules, scheduler,
four-attempt generation path, publication time, exact-byte delivery and
idempotency. This change adds no collector, background task, schema migration,
dependency or feature gate.

## Post-merge deployment

Run the normal VPS update from the bot checkout:

```bash
cd /home/ubuntu/bnl01 &&
git pull --ff-only origin main &&
sudo systemctl restart bnl01 &&
systemctl show bnl01 -p ActiveState -p SubState -p MainPID &&
git rev-parse HEAD
```

Then inspect the read-only report from that same directory:

```bash
sudo /home/ubuntu/bnl01/venv/bin/python -m scripts.journal_relay_health \
  --db /home/ubuntu/bnl01/bnl01_conversations.db \
  --guild-id 1288269405209235551 \
  --pid "$(systemctl show bnl01 -p MainPID --value)"
```

The revision and active process establish installation. They do not establish
that the new writer has run. Older published metadata remains older metadata.
After the next natural preparation/publication, look for the new reflection
version and the separate `currentActivityRelays`/`retrospectiveRelays` counts.
On a callback-only window, current activity stays zero and creative reflection
is enabled. For any entry drawing on old material, inspect its citations and
original dates, historical tense and clear framing of imagined scenes. Confirm
actual receiver acceptance separately from generation or stored drafts.

If specific prose still misstates an event, retain the entry ID/revision and
that sentence's cited source and original date for a focused correction. Do
not force another public entry or ask the owner to collect notes while live.
Friday's 19:00 show begins after the Daily's 18:30 cutoff and belongs to a later
eligible Journal window.
