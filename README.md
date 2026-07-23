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

Native queue context has two independent production gates. The local bot variable `BNL_QUEUE_PRODUCTION_ENABLED` defaults off and accepts only `true` (case-insensitive); the website read model must also report `capabilities.queueProduction=true`. Queue/session/track context is stripped unless both gates agree. Merging queue-aware code does not enable either gate.

Activation order is site first, bot second:

1. Keep the bot gate disabled while the website native-queue cutover is verified.
2. Confirm the website capability is true and its public queue state is sanitized and accurate.
3. Only after explicit owner approval, set `BNL_QUEUE_PRODUCTION_ENABLED=true` and restart the bot.
4. Roll back the bot first by unsetting the variable or changing it away from `true`; the website can then be rolled back independently.

Show-day copy follows the same boundary. The 6:40 PM Pacific intake message names the native queue only when both gates are usable; otherwise it uses provider-neutral public-intake wording. The 7:00 PM message describes the scheduled broadcast window without claiming unverified live state, and the later sponsor message remains optional and host-controlled.

Holiday and occasion reflections use the existing Ambient coordinator and active
liaison channel. The maintained calendar targets 10:00 AM Pacific, stores each
occurrence and canonical payload in the bot database before delivery, and
retries provider or Discord failures without consuming ordinary Ambient
capacity. Major/cultural dates take precedence, while owner-established
BARCODE-relevant dates are curated from source-backed radio, sound, music,
archive, communication, and technology observances or milestones. A roughly
two-week gap is a selection guide, not an automatic filler rule; the curated
layer is limited to twelve real dates. Three house
traditions—Open Channel Day, Archive Pulse Day, and Scan Day—are layered onto
real dates and are not claims about historical BARCODE anniversaries.
`BNL_OCCASION_POSTS_ENABLED` defaults on and can be set to `false` to cancel
unpublished occurrences. `BNL_OCCASION_DISABLED_IDS` accepts a comma-separated
list of calendar IDs for per-occurrence cancellation. These controls do not
activate queue access, Journal reuse, or any memory-v2 live gate.

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

## Release baseline

Before merging a runtime change:

1. Install the committed dependency versions.
2. Run `make check` on a supported Python version.
3. Confirm CI passes on Python 3.9 and 3.12.
4. Keep live behavior, memory governance, public/private evidence boundaries, and website contract changes in explicitly scoped PRs.
