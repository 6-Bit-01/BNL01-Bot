# Reply database locks and sealed show continuity — 2026-09-26

## Production evidence

The verified after-show TEST packet
`a2c7a164c8519c18e42cfb6d0bab15cb5418dab04aefb535c139ec62c9e8bce3`
captures the operator observation from 19:54 to 20:24 UTC. BNL was active on
`1aa775dc2b8409c0cf5ba22c7db7a6c59c411ceb`. All three database readers reported
SQLite busy/locked errors. The first answer was sent at 19:55:52 UTC; its
capture spent 18,309 ms in Ledger/Moment processing and Gemini took 6.099 s.

The subsequent VPS diagnostic identifies BNL's service PID 1797172 as the
holder of the database locks. Its write lock covers offsets 1073741824–
1073741825 and its read lock covers 1073741826–1073742335. Both requests were
routed to `answer`. The follow-up's unresolved referent produced a shadow
`clarify` decision, but orchestration influence was off and the effective
batch decision remained `answer`. Numerous `database is locked` exceptions
followed. The first answer used `website_read_model_no_store` in `sealed_test`.

This rules out an effective observe/silence decision as the explanation for
this follow-up. The diagnostic does not include the exception stack or
identify the individual connection inside the service. Production attribution
to one function therefore remains an inference, pending deployment evidence.

## Reproductions and repair

`log_response_style` runs after the answer decision and before show/source
preparation. Previously it opened a connection, inserted its row, committed,
then closed. A real concurrent read transaction can make that commit fail.
The exception bypassed `close`, and a retained exception/task could keep the
writer and its transaction alive. A local reproduction confirmed that new
readers remained blocked even after the original reader closed. Explicitly
closing the leaked writer immediately restored reads. `save_model_message`
had the same failed-commit cleanup defect.

Both writers now use a transaction context inside `contextlib.closing`.
Failed commits roll back and every exit closes the connection, including
while an exception remains referenced. Model persistence errors still
propagate; this change does not claim an unsaved answer was stored.

Style history is optional tone variation. Its read/write waits are bounded
to 0.1 seconds each and SQLite failures emit content-free operation/error-type
diagnostics. Style selection continues with the normal weights if history is
unavailable. Other memory/source reads, provider behavior and their timeouts
are unchanged.

The sealed-channel replay exposed a second failure independently: the public
TikTok conversation exception allowed replies in public rooms to persist but
discarded the corresponding reply in `bnl-testing`. The next reasoning/quote
question lost its answer referent and original show evidence. The existing
continuity exception now applies to delivered replies in sealed tests too.
The writer still receives `sealed_test`; existing same-channel Context and
memory policy keep those records sealed and nonpublic. Only delivered prose
is saved, never the injected website snapshot. Private website context,
queue-only snapshots and unsaved Discord referents retain their no-store
rules. The public-memory eligibility helper still rejects sealed turns.

Retaining replies also exposed an existing false ambiguity: a follow-up
asking for quotes from “those shows” treated the human request and BNL's
answer as competing room contributions. Context's existing time-scope
normalization now recognizes event scopes such as shows and broadcasts too.
The original wording remains available to the show owner, so its local
reader retains both comparison dates even when website transport is down.
An explicit correction still narrows to the newly requested show.

No store, worker, provider call, public send, migration, SQLite journal-mode
change, runtime gate or new memory authority is introduced.

## Validation

Before the repair, the five new initial tests produced three SQLite errors
and one missing-show-evidence failure; only ordinary style-history behavior
passed. After repair, the same tests pass. Expanded focused coverage passes
370 tests across SQLite contention, show interpretation, requested dates,
Context/orchestration, website context, show awareness, exact Discord replies,
memory governance and batching.

The final `make check PYTHON=.venv/bin/python` passes all 3,460 tests in
188.408 seconds. Every changed Python file also parses with Python 3.9's
grammar, and `git diff --check` is clean.

Regressions use real SQLite rollback-journal locks and retain connections or
exceptions while checking that fresh readers/writers work. A real batch path
still sends one show answer when style commit is deliberately blocked. The
two-turn sealed replay stores exactly the delivered answers and reopens
original attributed show comments; public and other sealed rooms cannot
retrieve its prose, and its Ledger projections remain nonpublic. Private
read models (including misleading public markers in source text), operational
snapshots and transient Discord evidence remain excluded from storage.

Provider, website and Discord transports are mocked in those replay tests.
They verify routing, source composition, delivery and storage boundaries,
not Gemini's interpretation quality or production response time.

## Deployment and focused acceptance

After this bot PR is merged, run on the existing VPS:

```bash
cd /home/ubuntu/bnl01 &&
git pull --ff-only origin main &&
sudo systemctl restart bnl01 &&
systemctl is-active bnl01
```

The restart releases connections retained by the old process. Do not delete
the database/journal, copy a live database, change journal mode, or disable
memory owners as a recovery shortcut.

In `bnl-testing`, send these separately, waiting for the first answer:

1. BNL, what stood out about how people interacted at the September 25, 2026 BARCODE Radio show?
2. What made you think that? Quote a few actual comments and identify the speakers.

Then capture once using the already installed collector:

```bash
/home/ubuntu/bnl01/venv/bin/python "$HOME/.local/share/barcode-after-show/after_show.py" --test
```

Verify the new service revision/start, readable database/health sections,
post-restart message and generation/send timings, paired follow-up Context,
and original show selection. The collector's public-only transcript boundary
does not export sealed test prose; the operator's visible replies and scoped
timing metadata are still needed to assess those answers. A recurring lock
requires its exception stack, not another assumption that every lock has the
same source.

This repair does not establish a response-time target or complete the wider
shared-brain audit. Slow Ledger/Moment work, semantic Moment admission,
model-directed retrieval and the private-art adapter remain open.

SQLite documents that a busy commit leaves its transaction active:
<https://www.sqlite.org/lang_transaction.html>. Python 3.9's transaction
context does not close its connection:
<https://docs.python.org/3.9/library/sqlite3.html#using-the-connection-as-a-context-manager>.
