# TikTok LIVE public telemetry transport and gated BNL context

This directory documents the optional transport used by
`scripts/tiktok_live_chat_transport.py`. It connects directly to TikTok's
public LIVE Webcast stream through the signerless `piratetok-live-py` client
and writes a small versioned NDJSON event stream to stdout.

## Boundary

This is **not** part of the main BNL Python environment. The production bot
supports Python 3.9 and 3.12; `piratetok-live-py` requires Python 3.11 or newer.
Keep the transport in its own virtual environment.

The transport and weekly supervisor:

- read public comments, taps/likes, viewer snapshots, shares, follows, gifts,
  TikTok Q&A questions, joins, and stream lifecycle events;
- do not use EulerStream, an API key, a signing service, or TikTok account
  cookies;
- cannot post comments, gift, follow, moderate, or mutate the show;
- emit no profile biography, avatar, follower count, private profile fields,
  payment/customer data, or currency conversion;
- treat TikTok gift diamonds only as platform-provided engagement units, not
  BARCODE payment truth and not a cash value;
- do not write BNL's database directly;
- append accepted public comments/questions to a bounded mode-`0600` handoff
  spool so the main bot can archive them through BNL's existing owners;
- do not call Gemini, Discord, the website, Relay, Journal, Moments,
  Relationship, Source Files, dossiers, queue actions, or payment owners.

The supervisor additionally publishes one bounded, atomically replaced JSON
snapshot at `/run/bnl-tiktok-chat-shadow/live-context.json` and appends public
comments/questions to
`/run/bnl-tiktok-chat-shadow/public-conversation.ndjson`. Both handoff files are
mode `0600` and disappear with the systemd runtime directory. The main bot
cannot use the live snapshot unless
`BNL_TIKTOK_LIVE_CONTEXT_ENABLED=true`; even then, BNL loads it only for an
explicit current-show or TikTok-reaction question whose website queue scope is
authorized in that exact Discord channel. The bot independently ingests the text
spool when `BNL_TIKTOK_LIVE_MEMORY_ENABLED=true`; that memory gate defaults to
the context gate's value.

Every accepted event is hard-coded as:

```text
source=tiktok_live_webcast
visibility=public_observation
lifecycle=current_show_only
memory_default=source_aware
public_text_memory=durable_public_conversation
metric_memory=current_show_only
memory_placement=above_community_canon
identity_default=handle_display_correlated_v1
```

Authority varies by event type:

```text
comment/question=viewer_statement
like/share/follow/gift/join=public_interaction_event
viewer_snapshot=platform_room_metric
```

A TikTok handle alone does not merge an ordinary viewer with a Discord identity,
website account, queue submitter, artist profile, Source File subject, or dossier
identity. A known-member binding requires both a compatible handle and a close
supporting display name. The owner-declared `@six.bit` primary account and
`@pr0x60` / `PR0X` side account resolve to the same owner subject. TikTok's
moderator flag is trusted as room-role evidence for that exact account, never as
permission for BNL to moderate.

## Isolated setup

No setup is performed by the repository or by importing BNL. In an isolated
test environment with Python 3.11+:

```bash
python3.11 -m venv .venv-tiktok-live
.venv-tiktok-live/bin/python -m pip install --upgrade pip
.venv-tiktok-live/bin/python -m pip install \
  -r tools/tiktok-live-ingest/requirements.txt
```

Run the raw transport only for a short connection proof:

```bash
.venv-tiktok-live/bin/python -u \
  scripts/tiktok_live_chat_transport.py \
  --username six.bit \
  --cdn us
```

Do not redirect stdout to a durable file.

## Replay, gift-streak, and LIVE-end handling

TikTok can replay recent Webcast messages after a reconnect. The transport
retains only a bounded set of recent event IDs and suppresses duplicates across
all emitted telemetry types. The window supervisor adds a second deduplication
boundary across fresh child processes.

Combo-gift progress frames are not emitted as completed gifts. The transport
waits for TikTok's streak-over signal and emits the final gift count/diamond
total once, preventing intermediate combo frames from inflating analytics.

When TikTok emits `live_ended`, the transport emits it once, blocks later stale
frames, and requests a clean disconnect instead of reconnecting to the ended
room. The weekly supervisor then keeps checking for a new LIVE until the show
window closes.

## Weekly unattended shadow window

The repository includes:

```text
scripts/tiktok_live_chat_shadow_window.py
scripts/run_tiktok_live_chat_shadow_window.sh
scripts/tiktok_live_chat_shadow_service.sh
deploy/systemd/bnl-tiktok-chat-shadow.service
deploy/systemd/bnl-tiktok-chat-shadow.timer
```

The timer starts every Friday at **6:50 PM America/Los_Angeles**. The service
runs through **2:00 AM Saturday**, including daylight-saving changes. During
that window it:

- waits while `@six.bit` is offline;
- connects automatically when the account becomes LIVE;
- restarts after a connection failure;
- keeps watching after a LIVE ends in case the stream restarts;
- prints comments, batched tap events, changed viewer counts, shares, follows,
  completed gifts, and TikTok Q&A questions;
- appends every accepted public comment/question to the bounded bot handoff
  spool before snapshot throttling;
- counts joins without printing every join line;
- prints a bounded end-of-window telemetry summary;
- stops and destroys its terminal scrollback at 2:00 AM;
- restarts the tmux terminal if the supervisor process crashes.

Raw engagement observations remain only in a dedicated tmux terminal and the
bounded volatile runtime snapshot. Public comments/questions also enter the
volatile handoff spool and are then stored by the main bot in BNL's append-only
Journal source archive and Unified Memory Ledger. Routine systemd logs contain
scheduler health, not the transcript or telemetry stream.

After the unit files are installed and enabled, attach with:

```bash
tmux -S /run/bnl-tiktok-chat-shadow/tmux.sock \
  attach -t tiktok-chat-shadow
```

Detach without stopping it with `Ctrl+B`, then `D`.

Scheduler status:

```bash
systemctl status bnl-tiktok-chat-shadow.service --no-pager -l
systemctl list-timers bnl-tiktok-chat-shadow.timer --no-pager
```

The service/timer alone do not authorize BNL consumption. The bot context gate
and website queue access scope must both authorize a live-reaction response.
The separate memory gate authorizes only public text archival and the normal
conversation-memory path described above. Neither gate permits TikTok output,
queue mutation, automatic canon/relationship promotion, Source Files, dossiers,
or recaps.

## NDJSON contract

Each line is one JSON object with `schema_version=1`.

Common fields:

```text
event_type
event_id
room_id
observed_at      # VPS receipt time
source_at        # TikTok source time when available
```

Public observation types:

```text
comment          unique_id, display_name, comment_text, moderator_flag
like             unique_id/display_name when supplied, like_count, like_total
viewer_snapshot  viewer_count
share            unique_id, display_name, share_type
follow           unique_id, display_name
gift             unique_id, display_name, gift_id, gift_name, gift_count,
                  diamond_count, diamond_total, combo, streak_over
question         unique_id, display_name, question_id, question_text,
                  answer_status
join             unique_id, display_name, join_count
```

Lifecycle types:

```text
connected
reconnecting
disconnected
live_ended
transport_error
```

`transport_error` carries only a bounded error class/code. Raw exception text,
URLs, cookies, and request headers are not emitted.

## Current production boundary

The direct connection has been observed receiving real public LIVE comments,
moderator status, and the LIVE-end event. The live snapshot makes recent
comments/questions and bounded engagement counters available to `bnl01_bot.py`
only for relevant live-show questions. The current website queue snapshot stays
authoritative for what the show is doing; TikTok telemetry is reaction evidence
only. BNL answers the requested fact without dumping a transcript or metrics.

Every accepted public comment/question becomes source-linked conversation
evidence when the memory gate is enabled. It can feed normal continuity, the
Journal, and bounded surface lore immediately above Community Canon, but one
utterance cannot establish canon, a relationship, a submitter/artist identity,
or verified external fact. Aggregate room metrics remain current-show-only.
Disabling the context gate returns BNL to queue-only live awareness; disabling
the memory gate stops new text ingestion without deleting previously governed
source history.
