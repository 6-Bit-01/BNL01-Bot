# BNL01 Bot

The production Discord runtime for BNL-01, including governed conversation memory, Presence/Relay v2, the BNL Journal, Source File and dossier assistance, relationship/moment systems, and owner-operated internal controls.

## Supported Python

- Python 3.9 is the current deployment compatibility floor.
- Python 3.12 is the modern development target.
- CI runs the complete suite on both versions.

Runtime dependencies are pinned in `requirements.txt` to versions that support both Python targets. Tests use the Python standard-library `unittest` runner and require no separate test framework.

## Local setup

```bash
python3 -m venv venv
source venv/bin/activate
python -m pip install -r requirements.txt
make check PYTHON=python
```

The direct full-suite command is:

```bash
python -m unittest discover -s tests -p 'test_*.py'
```

`make check` first compiles the bot, support modules, and tests, then runs the full suite.

## Runtime configuration

Configure secrets in the process environment; do not commit them. Core variables include:

- `DISCORD_BOT_TOKEN`
- `GEMINI_API_KEY`
- BNL website URLs, API keys, Relay controls, and feature flags used by the deployed runtime

BNL keeps its existing 1,350,000-token daily API safety budget as a secondary
guard, independent of consumer Gemini subscriptions and Google project limits.
Production defaults to `gemini-3.6-flash`; interactive conversation may use the
same-project `gemini-3.5-flash` fallback after bounded transient-error retry.
Each optional-background provider invocation uses one SDK attempt with no
fallback; a provider outage or local budget refusal stops validator-guided
follow-up calls for that job.
Governed memory, Source File/dossier, the accepted ordinary-chat single-packet
route, and Journal do not automatically downgrade. Route-specific output
allowances preserve quality, and `BNL_GEMINI_JOURNAL_PROTECTED_TOKENS` (default
250,000) keeps secondary token capacity for the Journal. The pinned Python
3.9-compatible SDK leaves Gemini 3 reasoning provider-managed; it does not apply
the obsolete global 1,024-token thinking cap.

Dollar-aware budget controls default to:

- `BNL_GEMINI_MONTHLY_TARGET_USD=20.00`
- `BNL_GEMINI_MONTHLY_HARD_LIMIT_USD=24.00`
- `BNL_GEMINI_DAILY_SOFT_LIMIT_USD=0.65`
- `BNL_GEMINI_BUDGET_ENFORCEMENT_ENABLED=true`
- `BNL_GEMINI_BILLING_LAG_BUFFER_USD=0.50`
- `BNL_GEMINI_INTERACTIVE_RESERVE_USD=2.00`
- `BNL_GEMINI_JOURNAL_RESERVE_USD=1.00`
- `BNL_GEMINI_RELAY_PACE_ALLOWANCE_USD=5.50`
- `BNL_GEMINI_BACKGROUND_MAX_OUTPUT_TOKENS=4096`
- `BNL_GEMINI_PROVIDER_TIMEOUT_SECONDS=120` (clamped to 10–240 seconds)
- `BNL_GEMINI_BUDGET_RESERVATION_TTL_MINUTES=30` (minimum 30 minutes)

Pricing is centralized in `bnl_gemini_cost.py`; exact-model overrides can be
supplied with `BNL_GEMINI_PRICING_OVERRIDES_JSON`. Unknown models remain
explicitly unpriced: an active unknown request model fails closed, while
historical unknown usage is conservatively reserved and stops optional work
without automatically taking Journal or interactive conversation offline.
The conservative historical reserve rate can be raised with
`BNL_GEMINI_UNPRICED_GUARDRAIL_USD_PER_MILLION` (default 9.00). `/usage`
reports provider-returned input, visible-output, thinking, cached, and total
tokens; estimated daily/monthly cost; pace/projection; attempts; and route
restrictions. Daily and monthly accounting boundaries use Pacific Time.

The website Relay makes one scheduled decision every
`BNL_WEBSITE_RELAY_INTERVAL_MINUTES` (default and minimum 60), aligned to
`BNL_WEBSITE_RELAY_MINUTE_OFFSET` (default 10, so the normal run is at `:10`
each hour). Fresh eligible public signal and the existing approved quiet-source
cascade use that same scheduled window; `BNL_WEBSITE_QUIET_RELAY_INTERVAL_MINUTES`
defaults to 60 and may be raised independently. Manual and force-pull requests
remain immediate and retain the same quiet-source cascade, validator, history,
and cursor rules. Website presence heartbeat remains independent at five
minutes.

Relay keeps its background one-attempt/no-fallback provider policy, but may use
the small configured pace allowance when generic background work is restricted.
The allowance never bypasses the effective hard limit or the Journal and
interactive dollar reserves. Show-day generation remains background-shaped but
time-sensitive, so the generic monthly/daily pace gate cannot suppress a
claimed show phase while those harder limits still permit it.

Native queue context has two independent production gates plus one website-owned access scope. The local bot variable `BNL_QUEUE_PRODUCTION_ENABLED` defaults off and accepts only `true` (case-insensitive); the website read model must also report `capabilities.queueProduction=true`. The website then declares `accessScope=none`, `private`, or `public`. `none` is always stripped. `private` is accepted only from an authenticated response obtained with the existing `BNL_API_KEY`, and only `sealed_test` and `internal_controlled` channel policies may retain its queue/history fields. Those Discord channels are permission-locked for explicitly admitted rehearsal testers and operators; ordinary server members remain excluded. Discord channel access is the authorization boundary, so the bot deliberately does not add a second owner-only requester check inside those already restricted channels. In `#bnl-testing`, read-only queue questions create a response obligation regardless of the participant's owner/admin/mod status. `public` may support public queue context. Merging queue-aware code does not enable either production gate or change a site's session access choice.

Private and public responses use the website's same explicit sanitized operational DTO. The bot does not receive payment/checkout, contact, raw upload, legal-acceptance, moderation, browser-capability, or admin-only fields, and it never gains queue mutation or playback control. Private data is temporary prompt context only: it may answer an authorized channel member inside an approved private channel, but it cannot drive public/show-day output, public recaps, Broadcast Memory, dossiers, Source Files, Relay, Journal, or any persistence path.

Operational queue questions bypass the short read-model cache and search the complete current order before prompt assembly. The prompt receives only the matching track or requested live fact; Now Playing and Up Next stay concise, while a full-lineup request receives the canonical queue-page link rather than a Discord data dump. BNL may read this truth but cannot move tracks, choose Wheel winners, or operate playback.

The only queue persistence path is the website's exact `queue_artist_memory_v1` section. Once both production gates are true, a one-minute sync validates the section digest and stores its public production artist/song/album facts through the existing entity-evidence and unified memory-ledger owners. Accepted tracks are provisional; a confirmed play supersedes their provisional ledger revision. Submitted/provider conflicts stay explicit. Only a semantic provider artist ID can become the primary artist key; YouTube channels and SoundCloud uploaders remain account provenance, and every TikTok/provider/submitted label stays disconnected from Discord identity. Private/test/simulation data, file metadata, and every other queue section are rejected. The sync checkpoint skips unchanged catalogs, and ordinary conversation recall is exact-name/music-query scoped. This catalog never marks Source Files dirty and cannot automatically create dossiers, relationships, or canon identity.

Activation order is site first, bot second:

1. Keep the bot gate disabled while the website native-queue cutover is verified.
2. Confirm the website capability is true and test all three session choices: no access, authenticated private access, and public access for a live broadcast.
3. Confirm private queue data is visible only to explicitly admitted participants in private test/operator channels and cannot drive public or show-day output.
4. Confirm the site's public `sections.artistMemory` contract contains only intended live-broadcast records and passes accepted/provisional, played/confirmed, identity, conflict, upload, and private-session isolation tests.
5. Only after explicit owner approval, set `BNL_QUEUE_PRODUCTION_ENABLED=true` and restart the bot. This activates both eligible queue context and the public artist-memory sync; it does not make a private session public.
6. Roll back the bot first by unsetting the variable or changing it away from `true`; the website can then be rolled back independently. Sync and recall stop immediately; previously authorized ledger history remains governed by the normal memory correction/deletion lifecycle.

Show-day copy follows the same boundary and accepts only public queue scope. A private test response cannot drive an intake, live, sponsor, recap, or public-current-state message. The 6:40 PM Pacific intake message names the native queue only when both production gates and public access are usable; otherwise it uses provider-neutral public-intake wording. The 7:00 PM message describes the scheduled broadcast window without claiming unverified live state, and the later sponsor message remains optional and host-controlled.

Current-show TikTok awareness is a separate default-off gate. The isolated
collector exports a bounded live snapshot and a mode-`0600` public-conversation
spool under the volatile systemd runtime directory. When
`BNL_TIKTOK_LIVE_CONTEXT_ENABLED=true`, an explicit live-show or TikTok-reaction
question can combine the snapshot with an authorized website queue read: the
queue determines what is happening, while recent public TikTok comments and
engagement describe reaction. Public Discord use requires public queue scope;
private queue scope remains limited to the existing sealed/operator channels.

Public TikTok comments and Q&A questions are source-aware conversation evidence,
not disposable metrics. `BNL_TIKTOK_LIVE_MEMORY_ENABLED` defaults to the context
gate's value and can be overridden independently. When enabled, the main bot
ingests every accepted public text event from the spool into the append-only
Journal source archive and Unified Memory Ledger. This lane sits immediately
above Community Canon and may support normal conversational continuity and
surface-level lore formation. A public Discord reply BNL gives about those live
reactions follows the ordinary conversation persistence path. Aggregate viewers,
taps, gifts, joins, and other room metrics remain current-show-only and do not
become personal memory or canon.

Declared TikTok owner handles `@six.bit` and `@pr0x60` resolve to the same BNL
owner subject; `PR0X`/`Prox` is the side-account display identity. Other known
community bindings require a compatible handle and a close supporting display
name, and ambiguous matches remain TikTok-only. TikTok's moderator flag is
trusted evidence that the exact account is a moderator in that LIVE room, but it
does not grant BNL moderation controls. No single comment creates canon,
relationship truth, a Source File, or a dossier. BNL still cannot post to TikTok,
moderate, or control the queue. Missing, stale, disconnected, or unauthorized
live context fails closed.

Each scheduled show-day phase takes one atomic pre-generation claim with a unique worker token. A fresh claim blocks duplicate workers, while an active worker renews its six-minute lease once a minute through generation. After six minutes without a heartbeat the claim becomes recoverable, so a crash, restart, malformed timestamp, or material clock error cannot suppress the phase for the rest of its ten-minute delivery window. Immediately before the first Discord or website attempt, the worker atomically converts its token-owned claim into a durable `friday_show_updates` fence. That pre-publication fence prevents a restart after an accepted send from reclaiming and duplicating the phase. Fence persistence uses bounded retries and releases only before any external attempt, so a database outage cannot block the single scheduler loop. This deliberately favors at-most-once show-day copy: a process exit after the durable fence but before delivery can skip that one update, but cannot publish it twice.

Holiday and occasion reflections extend the existing Ambient coordinator and
active liaison channel. The maintained calendar targets 10:00 AM Pacific,
stores each occurrence and canonical payload in the bot database before
delivery, and retries provider or Discord failures while reserving and
consuming one shared Ambient slot. Source-backed major, cultural, community,
art, health, broadcast, archive, communication, and technology dates keep
recurring occasion days no more than ten days apart across the Gregorian
calendar cycle.
They retain their real names; BARCODE flavor comes from BNL's reflection voice,
not invented dates or renamed observances. Ordinary Ambient, Dormant Echoes,
occasion reflections, and automatic show-day Discord posts share one daily
quota: at most one post on a normal day and at most two only after the existing
public activity signal crosses the high-activity thresholds. Occasion
occurrences bypass weak-signal requirements but cannot create an extra post.
`BNL_OCCASION_POSTS_ENABLED` defaults on and can be set to `false` to cancel
unpublished occurrences. `BNL_OCCASION_DISABLED_IDS` accepts a comma-separated
list of calendar IDs for per-occurrence cancellation. These controls do not
activate queue access, Journal reuse, or any memory-v2 live gate.

Dormant Signal Echoes run only through the existing Ambient coordinator and
only in `#barcode-bot`. A candidate must have repeated eligible public history,
an existing familiar/trusted legacy relationship state, and no eligible public
activity for at least 14 days. A callback also requires fresh, meaningful
`#barcode-bot` conversation from the prior 24 hours. Selection then passes a 5%
rarity gate plus a 21-day global and 120-day per-member cooldown. Echoes consume
the same shared automatic-post quota as ordinary Ambient, occasions, and
show-day output; they never ping, DM, summon, or claim the absent member is
currently present.
`BNL_DORMANT_ECHO_ENABLED=false` is the canary kill switch.
`BNL_DORMANT_ECHO_SELECTION_CHANCE` may reduce the default `0.05` probability
without changing the shared cap. The selected basis is stored on the existing
Ambient log as counts and source-row references, never as a second proactive
engine or new memory authority. Memory/Governance/Relationship v2 live gates
remain unchanged.

## Relay accepted-history durability

Accepted public Relays are retained indefinitely in the existing
`website_relay_history` SQLite table. The operational recent-25 view remains
bounded, and the website's public recent-20 projection is unchanged.

`bnl_relay_backup.py` exports a full Relay-only snapshot for a named month. The
compressed artifact contains only accepted Relay rows plus the minimal Relay
cursor state; it does not copy the production database, attempts, pending
drafts, conversations, heartbeats, provider traces, Journals, or other private
tables.

```bash
python -m bnl_relay_backup export \
  --db bnl01_conversations.db \
  --month 2026-07 \
  --output-dir backups/relay

python -m bnl_relay_backup verify \
  --archive backups/relay/<archive>.json.gz \
  --checksum backups/relay/<archive>.json.gz.sha256

python -m bnl_relay_backup upload \
  --archive backups/relay/<archive>.json.gz \
  --checksum backups/relay/<archive>.json.gz.sha256 \
  --remote "gdrive:BNL-01 Backups/Relay Archive"

python -m bnl_relay_backup round-trip \
  --archive backups/relay/<archive>.json.gz \
  --checksum backups/relay/<archive>.json.gz.sha256 \
  --remote "gdrive:BNL-01 Backups/Relay Archive" \
  --production-db /home/ubuntu/bnl01/bnl01_conversations.db
```

Transport uses the operator's external `rclone` configuration and never reads
or writes OAuth credentials itself. Round-trip proof downloads the artifact,
verifies its checksum, restores it twice into a temporary isolated database,
and confirms the production database was untouched.

No timer, service, or cron entry is installed by this repository. The
`scheduled-run` command fails closed unless
`BNL_RELAY_BACKUP_SCHEDULE_ENABLED=true`; keep it false until Google Drive
authentication and an end-to-end owner-approved proof are complete.

Importing `bnl01_bot` does not create a Gemini client or open provider transports. The client is created and cached on the first generation request, so tests, diagnostics, and tooling can import the runtime without valid provider networking.

Run the bot only after the deployment environment is configured:

```bash
python bnl01_bot.py
```

## V2 shadow acceptance

The v2 memory and relationship stack is evaluated in the fixed shadow order
Ledger → Moments → Governance → Relationship. The acceptance diagnostic enables
nothing, keeps every live gate off, requires owner review, and never performs an
automatic cutover. Conversation Context v2 is a separate continuity preflight.

See [BNL-01 v2 Shadow Acceptance and Rollback](docs/BNL01_V2_SHADOW_ACCEPTANCE_AND_ROLLBACK.md)
for the aggregate evidence fields, exact stop conditions, and reverse-order
rollback procedure.

## Conversation and memory authority

BNL's conversation and memory paths are connected, but they are not one
undifferentiated store:

| Layer | Authority and current role |
| --- | --- |
| Conversation Context v2 | The live, bounded owner for nearby room continuity and relevant same-member public follow-ups. It defaults on and does not make every message durable. |
| Approved member facts | Only a direct self-report of preferred name, pronouns, favorite color, or favorite movie may update the live personal fact owner automatically. These facts remain source-linked, changeable, and non-Core. |
| Unified Memory Ledger | Additive, source-linked shadow evidence and lineage. It is not live reply authority by itself. |
| Moment Engine | Builds derived, participant-attributed meaning gists from eligible Ledger evidence. Its shadow episodic lifecycle can join coherent Moments into one shared active episode, split on topic interruption, and reopen or link only from source-backed evidence. A Moment or episode is paraphrase support, never a transcript or quotation authority. |
| Memory Governance | Compares eligible durable candidates in shadow. The existing broad live switch is not an approved production cutover path. |
| Relationship v2 / Active Engagement v2 | Derived tone and proactive-behavior layers. Their live gates remain off until earlier memory authority, precedence, correction, deletion, and rollback canaries pass. |

Normal continuity may connect relevant prior messages across ordinary topics,
including stories, jokes, plans, and technical work. It must keep speakers
distinct, preserve public/private and channel-policy boundaries, and avoid
turning conversation traces into biography. Multi-person replies are shared
room output rather than a personal BNL reply copied into every participant's
history.

The episodic Moment lifecycle remains shadow-only by default. It records
content-free action, reaction, decision, assignment, outcome, and open-loop
roles against their Ledger sources; supports any number of participants on one
shared episode; and propagates correction or deletion into review state. The
Unified Response Assessment may record an opaque active-episode reference for
comparison.

`BNL_UNIFIED_MOMENT_CANARY_ENABLED` defaults off. When it and the exact guild
and channel allowlists are configured, the Unified Assessment and a
source-revalidated, content-free active-episode signal may guide a response
only on that `sealed_test` route. The canary cannot run unless all assessment
shadow prerequisites are effective and all global live gates are off. Public
and other non-allowlisted prompts remain unchanged. Setting the flag to
`false` and restarting is the response-path kill switch.

When asked what another person said, BNL should give a cautious gist by default.
Exact wording is a separate, fail-closed evidence mode: it requires a
consequential verification or dispute request, one typed target, one eligible
same-room public human message, and a still-matching live Discord source.
Memory tiers, relationship notes, summaries, Relays, Journals, and Moments never
authorize a quote.

`BNL_MOMENT_GIST_CANARY_ENABLED` defaults off. Even when explicitly set, it
requires both Ledger and Moment shadow gates, non-empty guild and member
allowlists, a direct request, and an eligible public route. Deploying the code
does not enable this canary or any global v2 live gate. See
[the July 24 memory intelligence checkpoint](BARCODE_BNL_MEMORY_INTELLIGENCE_CHECKPOINT_2026-07-24.md)
for the implemented/enabled/observed distinction and the ordered work that
still remains.

## Release baseline

Before merging a runtime change:

1. Install the committed dependency versions.
2. Run `make check` on a supported Python version.
3. Confirm CI passes on Python 3.9 and 3.12.
4. Keep live behavior, memory governance, public/private evidence boundaries, and website contract changes in explicitly scoped PRs.
