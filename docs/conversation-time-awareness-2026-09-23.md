# Conversation time awareness — Pacific calendar foundation

Inspection after #569 found that `_format_batched_prompt` computed temporal
context but did not render it. Its instructions referred to a network time
that was absent. `build_user_aware_prompt` also lacked the clock. The existing
helper called Friday 6:40–9:40 PM `live_now` and later Friday `post_show`, even
though calendar time cannot establish either operational state.

Both conversation builders now render one shared block from that existing
clock owner. It supplies the current Pacific date, weekday and time with an
explicit UTC offset; whether today is the regular Friday show day; and the
next regular scheduled start with that episode's intake and first-track target.
The times come from `FRIDAY_PUBLIC_SCHEDULE`, the existing approved canon.
Next start means strictly after the prompt clock: Friday 6:50 PM still points
to tonight's 7 PM start; at or after 7 PM it points to next Friday. This does
not mark tonight's show ended. Each occurrence is localized independently for
daylight-saving changes. A supplied naive test clock means local Pacific time;
the runtime clock is timezone-aware.

Today's maintained occasion comes from `calendar_occasions_on`, the same
calendar already used by the occasion owner. An absent entry is not a claim
that no holiday exists, and an entry is not a claim that a reflection was
published. Existing scheduling changes and fresh authorized operations retain
precedence. No website read is needed to know the date or regular schedule.

The clock is prompt context. It does not enter source selection as a requested
show date, create a source receipt, or authorize memory or publication. It is
retained when the existing ordinary single-packet path assembles its evidence.
Every newly assembled conversation prompt gets a fresh clock; a reused prompt
still identifies its original snapshot time. Existing source revalidation,
provider accounting, response obligations, channel privacy and gates remain
unchanged. Ambient keeps the same Friday/off-cycle topic variety, with neutral
calendar labels instead of inferred live/end labels.

This is a bounded foundation within the approved time-awareness work. Broader
multi-turn corrections and participant/topic continuity, event/recorded/
publication timestamp coverage, live track/event freshness, and the Journal →
Relay → published-Ballad source connections remain follow-up work. The prompt
instructs BNL to preserve historical episode selection and distinguish those
time meanings, but this PR does not rebuild their source adapters or claim
complete memory-timeline awareness. Website item 3 remains paused.

## Validation

`tests/test_conversation_temporal_context.py` covers Pacific/UTC midnight,
Friday intake versus start, exact start and year rollover, both DST changes,
neutral live/end semantics, maintained occasions, and fresh prompt clocks with
an unchanged historical episode. Real direct/batch builders and packet assembly
run in public and sealed contexts with packet generation enabled and disabled.
The batch tests reach one provider call and one send. External reads, Gemini
and Discord transport are replaced locally; this is not live model acceptance.

Run the focused checks with:

```bash
PYTHONPATH=tests .venv/bin/python -m unittest \
  test_conversation_temporal_context test_ambient_show_context \
  test_cost_control_schedulers test_requested_show_date_delivery
make check PYTHON=.venv/bin/python
```

## Normal post-merge deployment

After review/merge and authorization to deploy, use the normal clean `main`
checkout on the bot VPS. Record the deployed merge SHA. No dependency update,
database migration, timer, service, gate or environment change is required.

```bash
cd ~/bnl01
git status --short --branch
git pull --ff-only origin main
git rev-parse HEAD
source venv/bin/activate
python -m compileall -q bnl01_bot.py bnl_*.py
sudo systemctl restart bnl01
sudo systemctl is-active bnl01
sudo journalctl -u bnl01 --since "5 minutes ago" --no-pager -n 80
```

Stop if the checkout is dirty or is not on `main`; preserve unrelated work.
Rollback is a normal revert of this PR followed by the usual bot deployment
and restart. No stored state needs undoing.

## Focused post-deploy evidence

Use the existing sealed `bnl-testing` channel. Do not open a public test session,
change show state, or toggle a gate solely to exercise these checks.

1. Ask directly: “What day, date and time is it in Pacific time? When is the
   next regular BARCODE Radio show, and when do submissions normally open?”
   Record the message IDs and request timestamp; compare with
   `TZ=America/Los_Angeles date '+%A %Y-%m-%d %H:%M:%S %Z %z'` on the host.
2. Ask the same through the ordinary batched conversation path. Expect one
   natural response using the current Pacific day and regular schedule. Capture
   the route, provider-call count and send receipt from existing diagnostics.
3. Ask “Does that mean we're live?” The regular schedule alone must not supply
   a live/end or playback claim. If a real current observation is available,
   compare the answer with that existing show state and its freshness.
4. Ask for the September 4th show, then an undated follow-up asking for actual
   comments and their speakers. Preserve the existing #569 result: selected
   sources stay with that episode, and the current calendar does not substitute
   the upcoming Friday. Record selected show/date receipts and source-linked
   examples. A failed reply is a focused defect, not a reset of prior acceptance.

Do not change the host clock to exercise midnight/DST. Those boundaries are
covered locally. Production model behavior remains unverified until the
focused replies are observed after deployment.
