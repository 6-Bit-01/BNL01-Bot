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

Importing `bnl01_bot` does not create a Gemini client or open provider transports. The client is created and cached on the first generation request, so tests, diagnostics, and tooling can import the runtime without valid provider networking.

Run the bot only after the deployment environment is configured:

```bash
python bnl01_bot.py
```

## Release baseline

Before merging a runtime change:

1. Install the committed dependency versions.
2. Run `make check` on a supported Python version.
3. Confirm CI passes on Python 3.9 and 3.12.
4. Keep live behavior, memory governance, public/private evidence boundaries, and website contract changes in explicitly scoped PRs.
