#!/usr/bin/env bash
set -euo pipefail

CONFIG_PATH=${CONFIG_PATH:-/config/spotify.json}
STATUS_PATH=${STATUS_PATH:-/config/librespot-status.json}
FIFO_PATH=${FIFO_PATH:-/tmp/snapfifo}
FALLBACK_NAME=${LIBRESPOT_FALLBACK_NAME:-RoomCast}
LIBRESPOT_BIN=${LIBRESPOT_BIN:-/usr/bin/librespot}
LOG_PATH=${LOG_PATH:-/config/librespot.log}

write_status() {
  local state="$1"
  local message="${2:-}"
  local exit_code="${3:-null}"
  local started="${LAST_START:-0}"
  echo "{}" | jq \
    --arg state "$state" \
    --arg msg "$message" \
    --argjson exit "$exit_code" \
    --arg started "$started" \
    --arg hash "$(hash_config)" \
    '.state=$state | .message=$msg | .last_exit_code=$exit | .last_start=($started|tonumber) | .config_hash=$hash' \
    > "$STATUS_PATH.tmp" && mv "$STATUS_PATH.tmp" "$STATUS_PATH"
}

hash_config() {
  if [ -f "$CONFIG_PATH" ]; then
    sha1sum "$CONFIG_PATH" | awk '{print $1}'
  else
    echo ""
  fi
}

load_cfg() {
  if [ ! -s "$CONFIG_PATH" ]; then
    write_status "waiting_for_config" "Waiting for config at $CONFIG_PATH"
    return 1
  fi
  USERNAME=$(jq -r '.username // empty' "$CONFIG_PATH")
  PASSWORD=$(jq -r '.password // empty' "$CONFIG_PATH")
  DEVICE_NAME=$(jq -r '.device_name // empty' "$CONFIG_PATH")
  BITRATE=$(jq -r '.bitrate // 320' "$CONFIG_PATH")
  INITIAL_VOLUME=$(jq -r '.initial_volume // 75' "$CONFIG_PATH")
  NORMALISATION=$(jq -r '.normalisation // true' "$CONFIG_PATH")
  if [ -z "$USERNAME" ] || [ -z "$PASSWORD" ]; then
    write_status "waiting_for_config" "Config missing username/password"
    return 1
  fi
  return 0
}

start_librespot() {
  echo "[librespot] Starting with device '${DEVICE_NAME:-$FALLBACK_NAME}' -> $FIFO_PATH"
  LAST_START=$(date +%s)
  write_status "starting" "Starting librespot as ${DEVICE_NAME:-$FALLBACK_NAME}"
  if [ ! -x "$LIBRESPOT_BIN" ]; then
    write_status "error" "Librespot binary not found at $LIBRESPOT_BIN"
    return 1
  fi
  "$LIBRESPOT_BIN" \
    --name "${DEVICE_NAME:-$FALLBACK_NAME}" \
    --backend pipe \
    --device "$FIFO_PATH" \
    --bitrate "$BITRATE" \
    --initial-volume "$INITIAL_VOLUME" \
    $( [ "$NORMALISATION" = "true" ] && echo "--enable-volume-normalisation" ) \
    > >(tee "$LOG_PATH") 2>&1 &
  LIBRESPOT_PID=$!
  write_status "running" "Running PID $LIBRESPOT_PID"
}

LIBRESPOT_PID=""
LAST_HASH=""
LAST_START=0

while true; do
  load_cfg || { sleep 3; continue; }
  CUR_HASH=$(hash_config)
  start_librespot
  while true; do
    sleep 5
    NEW_HASH=$(hash_config)
    if [ "$NEW_HASH" != "$CUR_HASH" ]; then
      echo "[librespot] Config changed, restarting..."
      write_status "restarting" "Config changed; restarting"
      kill "$LIBRESPOT_PID" 2>/dev/null || true
      wait "$LIBRESPOT_PID" 2>/dev/null || true
      break
    fi
    if ! kill -0 "$LIBRESPOT_PID" 2>/dev/null; then
      wait "$LIBRESPOT_PID" 2>/dev/null || true
      code=$?
      tail_msg=$(tail -n 5 "$LOG_PATH" 2>/dev/null | tr '\n' ' ')
      write_status "exited" "Librespot exited (code $code). ${tail_msg}" "$code"
      break
    fi
  done
  sleep 3
done
