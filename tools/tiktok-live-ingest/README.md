# TikTok LIVE chat transport (shadow-only)

This directory documents the optional transport used by
`scripts/tiktok_live_chat_transport.py`. It connects directly to TikTok's
public LIVE Webcast stream through the signerless `piratetok-live-py` client
and writes a small versioned NDJSON event stream to stdout.

## Boundary

This is **not** part of the main BNL Python environment. The production bot
supports Python 3.9 and 3.12; `piratetok-live-py` requires Python 3.11 or newer.
Keep the transport in its own virtual environment. A later bot integration may
consume stdout through a local process pipe after the shadow proof passes.

The transport:

- reads public comments and stream lifecycle events only;
- does not use EulerStream, an API key, a signing service, or TikTok account
  cookies;
- cannot post comments, gift, follow, moderate, or mutate the show;
- emits no profile biography, avatar, follower count, gift, like, join, or
  payment data;
- sends human diagnostics to stderr and reserves stdout for NDJSON;
- does not write a database or log file.

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

## Isolated setup for a connection proof

No setup is performed by the repository or by importing BNL. In an isolated
test environment with Python 3.11+:

```bash
python3.11 -m venv .venv-tiktok-live
.venv-tiktok-live/bin/python -m pip install --upgrade pip
.venv-tiktok-live/bin/python -m pip install \
  -r tools/tiktok-live-ingest/requirements.txt
```

Run the transport by itself only during an explicitly approved shadow proof:

```bash
.venv-tiktok-live/bin/python -u \
  scripts/tiktok_live_chat_transport.py \
  --username six.bit \
  --cdn us
```

Do not redirect stdout to a durable file. The intended consumer is the local
in-memory adapter, not a transcript archive.

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

This PR provides the replaceable transport, parser, bounded in-memory buffer,
deduplication, and lifecycle health. It does not start the collector or wire
TikTok comments into `bnl01_bot.py`, Gemini, Discord output, Relay, Journal,
Moments, Relationship, Source Files, dossiers, queue actions, or the website.
Those are separate approval and implementation decisions after a real-length
shadow connection proves the upstream transport.
