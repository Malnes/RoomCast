#!/usr/bin/env bash
set -euo pipefail

MARKER_PATH=${MARKER_PATH:-/var/lib/roomcast/firstboot.done}
LOG_PREFIX="[roomcast-firstboot]"
ENV_FILE=${ENV_FILE:-/etc/roomcast/firstboot.env}

log() {
  echo "${LOG_PREFIX} $*"
}

if [[ -f "$MARKER_PATH" ]]; then
  log "Marker exists ($MARKER_PATH); nothing to do."
  exit 0
fi

if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
fi

ROOMCAST_REF=${ROOMCAST_REF:-main}
SERVICE_USER=${SERVICE_USER:-pi}
INSTALL_DIR=${INSTALL_DIR:-/opt/roomcast}
SNAP_PORT=${SNAP_PORT:-1704}
MIXER_CONTROL=${MIXER_CONTROL:-Master}
PLAYBACK_DEVICE=${PLAYBACK_DEVICE:-plughw:0,0}
CAMILLA_PORT=${CAMILLA_PORT:-1234}
CAMILLA_RETRY_INTERVAL=${CAMILLA_RETRY_INTERVAL:-5}

INSTALL_URL=${INSTALL_URL:-https://raw.githubusercontent.com/Malnes/RoomCast/${ROOMCAST_REF}/node-agent/install.sh}

log "Starting first-boot provisioning"
log "Installer: $INSTALL_URL"

mkdir -p "$(dirname "$MARKER_PATH")"

# Wait for network/DNS to be usable (best-effort, retries forever until success)
while true; do
  if curl -fsSL "$INSTALL_URL" -o /tmp/roomcast-install.sh; then
    break
  fi
  log "Installer not reachable yet; retrying in 5s"
  sleep 5
done

chmod +x /tmp/roomcast-install.sh

bash /tmp/roomcast-install.sh \
  --user "$SERVICE_USER" \
  --install-dir "$INSTALL_DIR" \
  --snap-port "$SNAP_PORT" \
  --mixer "$MIXER_CONTROL" \
  --playback-device "$PLAYBACK_DEVICE" \
  --camilla-port "$CAMILLA_PORT" \
  --camilla-retry "$CAMILLA_RETRY_INTERVAL"

date -Iseconds >"$MARKER_PATH"
chmod 600 "$MARKER_PATH" || true

log "Provisioning complete; disabling first-boot unit"
if command -v systemctl >/dev/null 2>&1; then
  systemctl disable --now roomcast-firstboot.service >/dev/null 2>&1 || true
fi

exit 0
