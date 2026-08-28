# TikTok LIVE chat transport (shadow-only)

This directory documents the optional transport used by
`scripts/tiktok_live_chat_transport.py`. It connects directly to TikTok's
public LIVE Webcast stream through the signerless `piratetok-live-py` client
and writes a small versioned NDJSON event stream to stdout.

## Boundary

This is **not** part of the main BNL Python environment. The production bot
supports Python 3.9 and 3.12; `piratetok-live-py` requires Python 3.11 or newer.
Keep the transport in its own virtual environment.

The transport and weekly shadow supervisor:

- read public comments and stream lifecycle events only;
- do not use EulerStream, an API key, a signing service, or TikTok account
  cookies;
- cannot post comments, gift, follow, moderate, or mutate the show;
- emit no profile biography, avatar, follower count, gift, like, join, or
  payment data;
- do not write a database or durable transcript;
- do not call Gemini, Discord, the website, Relay, Journal, Moments,
  Relationship, Source Files, dossiers, queue actions, or payment owners.

The BNL adapter hard-codes every accepted comment as:

```text
source=tiktok_live_webcast
visibility=public_observation
authority=viewer_statement
lifecycle=current_show_only
memory_default=do_not_store
identity_default=tiktok_only_unlinked
```

A TikTok handle is not a Discord identity, website account, queue submitter,
artist profile, Source File subject, or dossier identity.

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

## Replay and LIVE-end handling

TikTok can replay recent Webcast messages after a reconnect. The transport now
retains only a bounded set of recent event IDs and suppresses duplicate comment
IDs. When TikTok emits `live_ended`, the transport emits it once, blocks later
stale-frame comments, and requests a clean disconnect instead of reconnecting
to the ended room.

The window supervisor adds a second deduplication boundary across fresh child
processes. It can therefore stop an ended connection and keep checking for a
new LIVE without printing the same comments again.

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
- stops and destroys its terminal scrollback at 2:00 AM;
- restarts the tmux terminal if the supervisor process crashes.

Raw comments remain only in a dedicated tmux terminal. Routine systemd logs
contain scheduler health, not the comment transcript.

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

The service/timer are a shadow reliability tool only. Enabling them does not
authorize BNL to consume, answer, store, summarize, publish, or act on TikTok
comments.

## NDJSON contract

Each line is one JSON object with `schema_version=1`.

Comment fields:

```text
event_type=comment
event_id
room_id
observed_at
unique_id
display_name
comment_text
moderator_flag
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

## Current stop point

The direct connection has been observed receiving real public LIVE comments,
moderator status, and the LIVE-end event. This code hardens and automates the
shadow collection window. It still does not wire TikTok comments into
`bnl01_bot.py`, Gemini, Discord output, durable memory, or any public surface.
Private BNL awareness remains a separate implementation and activation gate.
