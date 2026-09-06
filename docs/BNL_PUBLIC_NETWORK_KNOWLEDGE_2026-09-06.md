# Public Network knowledge in normal Gemini conversation

This implements the first agreed shared-brain connection from restored main
`f6e36f4275d3d947fc76d4b1fd0679ecfaf0d4cc` (PR #509).

BNL's existing public knowledge must reach his working conversation path.
Public TikTok chat, published Journals/Relays, public site content, non-sensitive
queue/show data and public Discord are already authorized. Relevance, timing
and attribution determine their use. Gemini interprets and responds.

## Result of this change

- Public Discord history from all three public policies can reach relevant
  public or sealed conversation through the existing Context reader.
- Sealed testing can read public durable memory. Public and sealed prompts
  select public-safe memory even when the speaker has Discord privileges.
- Existing publication readers supply relevant published Journals and accepted
  Relays directly to normal Gemini. No exact Journal/Relay cue is necessary.
  A public Journal's memory-formation exclusion does not prohibit reading it.
- Mixed publication/current-queue requests keep normal queue context when
  experimental packet generation is off or has no usable replacement basis.
- Existing source revalidation tracks publication revisions and accepted Relay
  rows. Missing Journal input preserves Relay, memory, queue and ordinary reply
  handling. Source input fetches run off Discord's event loop.

The shared-brain source stores, Gemini response owner, speaking eligibility,
batch pacing and public/test write rules remain the existing owners. This change
does not alter site code, queue/payment/playback actions, Journal/Relay schedules,
TikTok collection/history or any runtime switch. It adds no database migration,
model call, response-format contract or English acceptance classifier.

## Verification and limits

`test_public_network_knowledge` exercises real database initialization, memory
readers, Context/Frame, publication readers, final guards, source refresh and
simulated Discord delivery. Provider and transport boundaries are fixtures.
It reproduces the five original memory/queue failures before the repair and
covers actual publication input, unavailable Journal handling, source changes,
fresh snapshot reuse and event-loop responsiveness during a delayed fetch.

Context tests cover all three public source policies into public/sealed targets,
in direct and batch mode, with private/sealed/foreign-guild exclusions. Existing
batch tests now initialize actual memory schema because sealed reads reach it.
Publication readers retain identity/date/latest semantics and actual public
visibility. The existing snapshot freshness window is documented in
`canonical_publication_read_adapters_v1.md`.

Local `make check` passed all 2,613 tests (79.194 seconds), including 13
normal-input integration tests. Independent review issues were corrected.
Published-commit CI status is recorded in the PR. Checks establish code behavior
with fixtures. Fresh production
memory contents, collector health and natural live Gemini answers require
runtime evidence. This first connection does not certify full shared-brain
completion. Further work follows the existing plan: retained memory and
correction/episode recall, remaining consumer connections and operating health.

## After the reviewed PR is merged

Use the bot's virtual environment; system `python3` on the VPS is Python 3.8.
Keep `BNL_ORDINARY_CHAT_SINGLE_PACKET_ENABLED=false` and current runtime settings.

```bash
cd /home/ubuntu/bnl01
git pull --ff-only origin main
git rev-parse HEAD
PYTHONPATH=tests ./venv/bin/python -m unittest test_public_network_knowledge test_publication_read_adapters test_conversation_context_v2
sudo systemctl restart bnl01
sudo systemctl status bnl01 --no-pager -l
```

Run the restart only after the preceding test command succeeds. For one bounded
live check, use an ordinary untagged test-room question connecting a known
published Journal/Relay topic with current public information. Inspect the
answer and existing route debug. Expected: normal Gemini, available context
used, experimental ordinary-packet receipt absent, and no test history written
into public memory. A passing receipt alone does not prove answer quality.

Recovery reference remains PR #509 and the existing operating configuration.
Do not reset the database or erase public TikTok history to undo a code change.
