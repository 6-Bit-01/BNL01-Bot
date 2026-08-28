#!/usr/bin/env bash
set -euo pipefail

ROOT="${BNL_TIKTOK_ROOT:-/home/ubuntu/bnl01}"
TIMEZONE="${BNL_TIKTOK_TIMEZONE:-America/Los_Angeles}"
WINDOW_WEEKDAY_ISO="${BNL_TIKTOK_WINDOW_WEEKDAY_ISO:-5}"
WINDOW_START="${BNL_TIKTOK_WINDOW_START:-18:50}"
WINDOW_END="${BNL_TIKTOK_WINDOW_END:-02:00}"
SESSION="${BNL_TIKTOK_TMUX_SESSION:-tiktok-chat-shadow}"
SOCKET="${BNL_TIKTOK_TMUX_SOCKET:-/run/bnl-tiktok-chat-shadow/tmux.sock}"
POLL_SECONDS="${BNL_TIKTOK_SESSION_POLL_SECONDS:-5}"
RUNNER="$ROOT/scripts/run_tiktok_live_chat_shadow_window.sh"
TMUX="$(command -v tmux || true)"

if [[ -z "$TMUX" ]]; then
  echo "[scheduler] tmux is not installed" >&2
  exit 1
fi
if [[ ! -x "$RUNNER" ]]; then
  echo "[scheduler] shadow runner is missing: $RUNNER" >&2
  exit 1
fi
if [[ ! "$WINDOW_WEEKDAY_ISO" =~ ^[1-7]$ ]]; then
  echo "[scheduler] BNL_TIKTOK_WINDOW_WEEKDAY_ISO must be 1-7" >&2
  exit 1
fi
if [[ ! "$WINDOW_START" =~ ^([01][0-9]|2[0-3]):[0-5][0-9]$ ]] \
  || [[ ! "$WINDOW_END" =~ ^([01][0-9]|2[0-3]):[0-5][0-9]$ ]]; then
  echo "[scheduler] window times must use HH:MM" >&2
  exit 1
fi
if [[ ! "$POLL_SECONDS" =~ ^[0-9]+$ ]] || (( POLL_SECONDS < 1 || POLL_SECONDS > 60 )); then
  echo "[scheduler] poll seconds must be 1-60" >&2
  exit 1
fi

mkdir -p "$(dirname "$SOCKET")"

as_minutes() {
  local value="$1"
  local hour="${value%%:*}"
  local minute="${value##*:}"
  printf '%d\n' "$((10#$hour * 60 + 10#$minute))"
}

START_MINUTES="$(as_minutes "$WINDOW_START")"
END_MINUTES="$(as_minutes "$WINDOW_END")"

within_window() {
  local day hour minute now_minutes next_day
  day="$(TZ="$TIMEZONE" date +%u)"
  hour="$(TZ="$TIMEZONE" date +%H)"
  minute="$(TZ="$TIMEZONE" date +%M)"
  now_minutes="$((10#$hour * 60 + 10#$minute))"

  if (( START_MINUTES < END_MINUTES )); then
    [[ "$day" == "$WINDOW_WEEKDAY_ISO" ]] \
      && (( now_minutes >= START_MINUTES && now_minutes < END_MINUTES ))
    return
  fi

  next_day="$((WINDOW_WEEKDAY_ISO == 7 ? 1 : WINDOW_WEEKDAY_ISO + 1))"
  { [[ "$day" == "$WINDOW_WEEKDAY_ISO" ]] && (( now_minutes >= START_MINUTES )); } \
    || { [[ "$day" == "$next_day" ]] && (( now_minutes < END_MINUTES )); }
}

session_exists() {
  "$TMUX" -S "$SOCKET" has-session -t "$SESSION" 2>/dev/null
}

start_session() {
  if [[ -S "$SOCKET" ]] && ! "$TMUX" -S "$SOCKET" list-sessions >/dev/null 2>&1; then
    rm -f "$SOCKET"
  fi
  "$TMUX" -f /dev/null -S "$SOCKET" new-session -d -s "$SESSION" "$RUNNER"
  "$TMUX" -S "$SOCKET" set-option -t "$SESSION" history-limit 5000 >/dev/null
  echo "[scheduler] shadow terminal started"
}

cleanup() {
  "$TMUX" -S "$SOCKET" kill-server >/dev/null 2>&1 || true
  rm -f "$SOCKET"
}
shutdown() {
  exit 0
}
trap cleanup EXIT
trap shutdown INT TERM

if ! within_window; then
  echo "[scheduler] outside configured show window; exiting"
  exit 0
fi

while within_window; do
  if ! session_exists; then
    start_session
  fi
  sleep "$POLL_SECONDS"
done

echo "[scheduler] show window closed; stopping shadow terminal"
