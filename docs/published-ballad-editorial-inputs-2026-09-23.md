# C7: Published Ballad inputs for Journal and Relay

Continues the locked shared-brain plan after PR574. PR574 merged at
`04e5398b8f86d979ccd6dd95e97fe27653131a57`; its tested tree matches main and
GitHub run 35898573911 passed transport, Python 3.9 and Python 3.12.

## Source and scope

The website owns the public publication slot. Its existing read-only
`GET /api/ballads/catalog` returns only releases from eligible archived public
shows. The authenticated Ballad control GET is deliberately not a read adapter:
it can enqueue automatic creative work, and its version map also includes
selected or archived recordings that are not presently published.

The bot's existing Ballad owner reads the public catalog through the configured
`BNL_STATUS_URL` origin, with no credentials, a five-second socket timeout,
two-megabyte response ceiling, 200-release ceiling, and no transport retry.
Malformed, duplicate, oversized or failed responses mean unavailable authority,
not an empty/withdrawn catalog. SQLite reads do not initialize or modify stores.

Each eligible release must match the guild/show/version in the existing immutable
Ballad store, including its recomputed content hash and public title/style/palette.
The newest draft does not supersede a published older version. Bounded public
title, style, palette, liner notes, publication date and show link enter editorial
context. Lyrics, raw provider output, private producer feedback and other draft
fields do not. No new catalog, memory store, scheduler, model call or gate exists.

## Consumers and delivery

- Journal receives at most two releases from the 30 days before its evidence
  cutoff as explicitly creative reflection. They never increase fresh-source,
  participant, show-operation or recurrence counts. Existing identity projection
  applies to summaries. Daily and Weekly share this reader. The private frozen
  packet and article metadata retain every supplied version/publication basis,
  including uncited candidates.
- Relay receives at most one matching release in its existing 30-day quiet-source
  rotation. Fresh public conversation retains priority. The source stays distinct
  internally and uses the existing public-safe-memory wire class. Both lines
  retain the historical-current-state guard.
- Ordinary chat accepts the website's current `/radio/archive?view=shows&show=...`
  Ballad link as well as the legacy public link; unrelated hosts/views are rejected.

Journal checks publication authority when freezing/reopening inputs, saving a
generated draft, approving it and immediately before delivery. Relay checks after
generation and before each existing v2 HTTP attempt. Local immutable-version checks
remain in the established SQLite fences. Network reads occur outside SQLite write
transactions and run in the existing background workers.

A changed, withdrawn, replaced or republished slot retires a scheduled prepared
article's packet or a pending Relay, preserving owed work/cursors and the existing
retry budget. A temporary authority outage preserves exact prepared bytes and
uses the established delivery retry owner without a model call. Previously
accepted public prose retains its existing retention policy.

## Verification and remaining acceptance

Two integration tests first reproduced the missing inputs through the real
Journal packet builder and Relay selector. Regression coverage includes exact
published-versus-newer-draft selection, empty/malformed/oversized catalogs,
wrong guild, corrupt immutable version, date windows, changed metadata/audio,
withdrawal during generation and between HTTP attempts, frozen-packet recovery,
uncited candidate checks, exact-byte restart retry, single accepted delivery,
weekly classification, nonblocking worker reads and independent SQLite writes
during the external publication check. All external model/HTTP responses in
these tests are fixtures. Required full-check results accompany the PR receipt.

No production deployment, live generation, public rehearsal, publication or
settings change is part of this package. Normal deployment and eligible natural
Journal/Relay output remain the live acceptance boundary. This does not establish
live writing quality or close C2's sealed continuity batch, C3's natural memory
meaning/aging observations, C4's operational freshness checks, affected C8 consumer
review, or C9's interrupted Moment meaning attempt left in `generating`.

After merge, continue the affected C8/C9 closure work against the finite matrix.
Keep website item 3 paused. Rollback is a bot code revert with existing stored
versions, pending payloads and publication history retained. Pre-C7 code cannot
validate a C7 Ballad basis; hold such unsent work for a compatible revision rather
than deleting provenance or forcing a post.
