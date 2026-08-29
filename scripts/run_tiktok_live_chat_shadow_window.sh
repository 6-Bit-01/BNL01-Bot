#!/usr/bin/env bash
set -euo pipefail

ROOT="${BNL_TIKTOK_ROOT:-/home/ubuntu/bnl01}"
PYTHON="${BNL_TIKTOK_PYTHON:-$ROOT/.venv-tiktok-live/bin/python}"
USERNAME="${BNL_TIKTOK_USERNAME:-six.bit}"
TIMEZONE="${BNL_TIKTOK_TIMEZONE:-America/Los_Angeles}"
WEEKDAY="${BNL_TIKTOK_WEEKDAY:-friday}"
WINDOW_START="${BNL_TIKTOK_WINDOW_START:-18:50}"
WINDOW_END="${BNL_TIKTOK_WINDOW_END:-02:00}"
RETRY_SECONDS="${BNL_TIKTOK_RETRY_SECONDS:-20}"
CDN="${BNL_TIKTOK_CDN:-us}"
MAX_RETRIES="${BNL_TIKTOK_MAX_RETRIES:-5}"
STALE_TIMEOUT="${BNL_TIKTOK_STALE_TIMEOUT:-60}"
CONTEXT_PATH="${BNL_TIKTOK_LIVE_CONTEXT_PATH:-/run/bnl-tiktok-chat-shadow/live-context.json}"

if [[ ! -x "$PYTHON" ]]; then
  echo "[scheduler] Missing isolated TikTok Python: $PYTHON" >&2
  exit 1
fi

cd "$ROOT"

exec "$PYTHON" -u "$ROOT/scripts/tiktok_live_chat_shadow_window.py" \
  --username "$USERNAME" \
  --timezone "$TIMEZONE" \
  --weekday "$WEEKDAY" \
  --window-start "$WINDOW_START" \
  --window-end "$WINDOW_END" \
  --retry-seconds "$RETRY_SECONDS" \
  --cdn "$CDN" \
  --max-retries "$MAX_RETRIES" \
  --stale-timeout "$STALE_TIMEOUT" \
  --context-path "$CONTEXT_PATH"
