# Shared-brain public launch — September 10, 2026

## Owner decision and scope

6 Bit authorized the shared-brain public launch and the remaining natural
public acceptance work on September 10. Most remaining Moment checks will use
real community activity and its stored evidence. This changes the rollout
scope and testing method; it does not declare the original acceptance complete.

The missing launch capability was ordinary-chat packet access for community
members without the existing eight-user canary ceiling. The new
`BNL_ORDINARY_CHAT_SINGLE_PACKET_PUBLIC_ENABLED` switch extends that same prompt
owner to eligible `public_home` and `public_context` turns in one configured
guild. The primary ordinary-chat switch still controls the whole capability.
Existing channel policy and reply eligibility remain authoritative. Public
enablement neither converts private channels to public nor requires private
canary user/channel lists.

Moments remain source-linked observations of people, situations, subjects and
continuation. Recall can recognize an earlier experience without creating a
new Moment or adding a new participant to the old event. The existing Moment,
Ledger, Governance, Conversation Context, adaptive memory and Relationship
owners retain their responsibilities. This release does not implement new
short/mid/long tier migration or change how Moments qualify, evolve or link.

## Configuration being launched

| Setting | Launch value / treatment |
| --- | --- |
| `BNL_ORDINARY_CHAT_SINGLE_PACKET_ENABLED` | `true` |
| `BNL_ORDINARY_CHAT_SINGLE_PACKET_PUBLIC_ENABLED` | `true` |
| `BNL_ORDINARY_CHAT_SINGLE_PACKET_GUILD_IDS` | `1288269405209235551` only |
| Ledger, Moment, Governance and Relationship V2 shadow flags | `true` |
| Unified Intelligence Packet and Unified Response Assessment shadow flags | `true` |
| Governance, Relationship V2 and Active Engagement V2 global live flags | `false`; these conflict with the packet/assessment prerequisites |
| Older synthesis canary / public-home broad-recall owner | `false`; preserve one ordinary-chat prompt owner |
| Existing private canary user/channel lists and expansion switch | Preserve their current values |
| Separate Moment-gist, Governance-recall and unified-Moment canaries | Preserve their current values; this launch does not enable them |
| Queue, Journal, Relay, TikTok, site and Ambient controls | Preserve current operation and cadence |

The newer Relationship V2 live tone path is not activated by this launch.
Existing relationship context remains available through its established path.
Episode selection remains sensitive to the actual request and available source
evidence. Specialized routes and simultaneous multi-person batches retain
their established generation paths; public packet activation does not convert
every reply to a packet run. Those paths can still collect eligible source
evidence for Moments.

## Post-merge deployment and activation

Run this once on the VPS after merging the public-scope change. It verifies the
new configuration reader exists before installing the tracked
`deploy/systemd/bnl01-shared-brain-public.conf` as one late systemd drop-in. The
drop-in sorts after `zzzz-bnl-testing-baseline.conf`. A root-only backup records
whether this exact file existed before the rollout; repeating the block does
not replace that original rollback point. No secret environment values are
printed.

```bash
cd /home/ubuntu/bnl01 || exit 1
git pull --ff-only origin main || exit 1
venv/bin/python - <<'PY'
from bnl_shared_brain_synthesis import ordinary_chat_configuration
assert "public_effective" in ordinary_chat_configuration({}), (
    "The merged public-scope code is required before activation."
)
print("public_scope_code_ready")
PY
if [ "$?" -ne 0 ]; then exit 1; fi

sudo bash <<'SH' || exit 1
set -eu
dropin=/etc/systemd/system/bnl01.service.d/zzzzz-shared-brain-public.conf
backup_dir=/root/bnl-shared-brain-public-20260910
install -d -m 700 "$backup_dir"
install -d -m 755 /etc/systemd/system/bnl01.service.d
if [ ! -e "$backup_dir/original.conf" ] && [ ! -e "$backup_dir/previously-absent" ]; then
    if [ -e "$dropin" ]; then
        cp -p "$dropin" "$backup_dir/original.conf"
    else
        touch "$backup_dir/previously-absent"
    fi
fi
install -m 644 deploy/systemd/bnl01-shared-brain-public.conf "$dropin"
systemctl daemon-reload
systemctl restart bnl01
systemctl is-active bnl01
SH
git rev-parse HEAD
```

### Verify the running process, without a provider call

Run this after the service starts. It reads the running process environment
locally, passes it explicitly to the configuration reader, and prints only
selected content-free fields. It does not import the bot, call a provider,
post a Discord message or write memory.

```bash
cd /home/ubuntu/bnl01 || exit 1
sudo /home/ubuntu/bnl01/venv/bin/python - <<'PYVERIFY'
import json, subprocess
from pathlib import Path
from bnl_shared_brain_synthesis import ordinary_chat_configuration

def service_pid():
    return int(subprocess.check_output(
        ["systemctl", "show", "bnl01", "--property=MainPID", "--value"], text=True,
    ).strip())
pid = service_pid()
assert pid > 0, "bnl01 has no running process"
runtime = dict(item.decode().split("=", 1)
    for item in Path(f"/proc/{pid}/environ").read_bytes().split(b"\0") if b"=" in item)
config = ordinary_chat_configuration(runtime)
assert {int(v.strip()) for v in runtime.get(
    "BNL_ORDINARY_CHAT_SINGLE_PACKET_GUILD_IDS", "").split(",") if v.strip()
} == {1288269405209235551}, "Unexpected public guild scope"
assert config["public_effective"], config["reason"]
assert service_pid() == pid, "Service restarted during verification; rerun"
print(json.dumps({key: config[key] for key in (
    "effective", "public_effective", "private_scope_effective",
    "prerequisites_ready", "reason",
)}, sort_keys=True))
PYVERIFY
```

This establishes configuration on the running process. It does not prove a
particular public channel's resolved policy, a delivered provider answer,
Moment formation, episode recall or a publication. Capture those from the real
events below.

## Natural public acceptance

Use a real conversation, show event, continuation or publication. Record its
time, surface/channel and source message or event references. One event can
supply several original acceptance capabilities; assess each separately.

- **Original row 6:** trace original source rows to the people, Moment and
  episode links when formation qualifies. Later continuation or recurrence
  must preserve the situation and participants. A shared topic can recall an
  earlier event without forcing a new Moment or merging people into it. Keep
  any still-required durable restart proof pending until observed.
- **Original row 8:** when an actual correction, retirement, source change or
  delivery problem occurs, inspect its source and derivative lifecycle. Do not
  manufacture destructive public events. Preserve completed evidence and keep
  remaining cases explicitly pending.
- **Original row 9:** correlate packet/prompt receipts, physical provider
  attempts and final delivery for these same real turns. Record actual selected
  and applied evidence, rather than treating a generic route flag as proof of
  stored or retrieved memory. The September 10 provider wait remains diagnosed
  but unresolved; no extra speed rehearsal is required.
- **Original row 10:** retain the valid historical canary off/on/restart
  rollback evidence. Focused configuration tests cover the new public switch
  being turned off; the VPS rollback procedure below is prepared, not yet
  live-verified. Do not relabel the old canary evidence as a public deployment
  rollback that has already happened.
- **Original row 11:** close with owner acceptance or an explicit bounded
  remainder after the actual evidence is reviewed.

Carry forward the accepted screenshot, quote/date/source, immediate
interruption/resume and narrow PR532 reply-to-Moment formation evidence. Public
episode recall, broader continuity, recurrence and the remaining original group
evidence remain separate from those passes. Observe TikTok, Journal, Relay,
site and Ambient consumers during their normal operating windows with existing
controls. A natural window that has not occurred is pending work, not failure.

The operator supplies real-event references and bounded VPS evidence for
review; this runbook does not install background monitoring or schedule probes.

## Roll back this activation only

Restore the pre-launch version of the single drop-in, or remove that file if it
did not previously exist. Other drop-ins, private canary lists, consumer
settings and stored memories remain in place. If another administrator has
subsequently edited this exact drop-in, reconcile that newer change before
restoring the recorded launch backup.

```bash
sudo bash <<'SH'
set -eu
dropin=/etc/systemd/system/bnl01.service.d/zzzzz-shared-brain-public.conf
backup_dir=/root/bnl-shared-brain-public-20260910
if [ -f "$backup_dir/original.conf" ]; then
    cp -p "$backup_dir/original.conf" "$dropin"
elif [ -f "$backup_dir/previously-absent" ]; then
    rm -f "$dropin"
else
    echo "No recorded launch backup; no configuration changed." >&2
    exit 1
fi
systemctl daemon-reload
systemctl restart bnl01
systemctl is-active bnl01
SH
```

After rollback, read the effective runtime configuration again, omitting the
launch guild and public-enabled assertions, and compare it with the pre-launch
settings.
Do not delete memory databases, replay sources or remove additive receipt
columns. A source-code rollback is a separate operation.
