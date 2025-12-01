#!/usr/bin/env bash
set -euo pipefail

SERVICE_USER="pi"
INSTALL_DIR="/opt/roomcast"
REPO_URL="https://github.com/Malnes/RoomCast.git"
SNAP_PORT="1704"
MIXER_CONTROL="Master"
PLAYBACK_DEVICE="plughw:0,0"
CAMILLA_VERSION="3.0.1"
CAMILLA_PORT="1234"
CAMILLA_ARCHIVE=""
CAMILLA_REPO="HEnquist/camilladsp"
CAMILLA_RETRY_INTERVAL="5"
CAMILLA_SERVICE_NAME="roomcast-camilla.service"
CAMILLA_TEMPLATE_PATH="/etc/roomcast/camilladsp.template.yml"
CAMILLA_CONFIG_PATH="/etc/roomcast/camilladsp.yml"
UPDATE_HELPER="/usr/local/bin/roomcast-updater"
UPDATE_ENV="/etc/roomcast/update-env"
SUDOERS_SNIPPET="/etc/sudoers.d/roomcast-agent"
STATE_DIR="/var/lib/roomcast"
AGENT_SECRET_PATH="${STATE_DIR}/agent-secret"
AGENT_CONFIG_PATH="${STATE_DIR}/agent-config.json"
SYSTEMCTL_BIN="$(command -v systemctl || echo /bin/systemctl)"

usage() {
  cat <<'EOF'
Usage: install.sh [options]

Options:
  --user <value>          System user that should own and run the agent (default: pi)
  --install-dir <path>    Directory to place the RoomCast repo (default: /opt/roomcast)
  --repo <url>            Git URL for the RoomCast repo (default: official GitHub)
  --snap-port <port>      Snapserver port (default: 1704)
  --mixer <name>          ALSA mixer control to adjust (default: Master)
  --playback-device <d>   ALSA playback device for CamillaDSP output (default: plughw:0,0)
  --camilla-port <port>   CamillaDSP control port (default: 1234)
  --camilla-retry <sec>   Seconds between CamillaDSP retry attempts (default: 5)
  --camilla-template <p>  CamillaDSP template path (default: /etc/roomcast/camilladsp.template.yml)
  --camilla-config <p>    Rendered CamillaDSP config path (default: /etc/roomcast/camilladsp.yml)
  --camilla-service <n>   Systemd service responsible for CamillaDSP (default: roomcast-camilla.service)
  -h, --help              Show this help message

Example:
  curl -fsSL https://raw.githubusercontent.com/Malnes/RoomCast/main/node-agent/install.sh \
    | sudo bash
EOF
}

log() {
  echo "[roomcast-install] $*"
}

write_update_env() {
  install -d /etc/roomcast
  {
    printf 'SERVICE_USER=%q\n' "$SERVICE_USER"
    printf 'INSTALL_DIR=%q\n' "$INSTALL_DIR"
    printf 'REPO_URL=%q\n' "$REPO_URL"
    printf 'SNAP_PORT=%q\n' "$SNAP_PORT"
    printf 'MIXER_CONTROL=%q\n' "$MIXER_CONTROL"
    printf 'PLAYBACK_DEVICE=%q\n' "$PLAYBACK_DEVICE"
    printf 'CAMILLA_PORT=%q\n' "$CAMILLA_PORT"
    printf 'CAMILLA_RETRY_INTERVAL=%q\n' "$CAMILLA_RETRY_INTERVAL"
    printf 'CAMILLA_TEMPLATE_PATH=%q\n' "$CAMILLA_TEMPLATE_PATH"
    printf 'CAMILLA_CONFIG_PATH=%q\n' "$CAMILLA_CONFIG_PATH"
    printf 'CAMILLA_SERVICE_NAME=%q\n' "$CAMILLA_SERVICE_NAME"
  } >"$UPDATE_ENV"
  chmod 600 "$UPDATE_ENV"
}

write_update_helper() {
  cat <<'EOF' >"$UPDATE_HELPER"
#!/usr/bin/env bash
set -euo pipefail

ENV_FILE="/etc/roomcast/update-env"

if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
fi

SERVICE_USER=${SERVICE_USER:-pi}
INSTALL_DIR=${INSTALL_DIR:-/opt/roomcast}
REPO_URL=${REPO_URL:-https://github.com/Malnes/RoomCast.git}
SNAP_PORT=${SNAP_PORT:-1704}
MIXER_CONTROL=${MIXER_CONTROL:-Master}
PLAYBACK_DEVICE=${PLAYBACK_DEVICE:-plughw:0,0}
CAMILLA_PORT=${CAMILLA_PORT:-1234}
CAMILLA_RETRY_INTERVAL=${CAMILLA_RETRY_INTERVAL:-5}
CAMILLA_TEMPLATE_PATH=${CAMILLA_TEMPLATE_PATH:-/etc/roomcast/camilladsp.template.yml}
CAMILLA_CONFIG_PATH=${CAMILLA_CONFIG_PATH:-/etc/roomcast/camilladsp.yml}
CAMILLA_SERVICE_NAME=${CAMILLA_SERVICE_NAME:-roomcast-camilla.service}

TMP_SCRIPT=$(mktemp)
cleanup() { rm -f "$TMP_SCRIPT"; }
trap cleanup EXIT

curl -fsSL https://raw.githubusercontent.com/Malnes/RoomCast/main/node-agent/install.sh -o "$TMP_SCRIPT"

bash "$TMP_SCRIPT" \
  --user "$SERVICE_USER" \
  --install-dir "$INSTALL_DIR" \
  --repo "$REPO_URL" \
  --snap-port "$SNAP_PORT" \
  --mixer "$MIXER_CONTROL" \
  --playback-device "$PLAYBACK_DEVICE" \
  --camilla-port "$CAMILLA_PORT" \
  --camilla-retry "$CAMILLA_RETRY_INTERVAL" \
  --camilla-template "$CAMILLA_TEMPLATE_PATH" \
  --camilla-config "$CAMILLA_CONFIG_PATH" \
  --camilla-service "$CAMILLA_SERVICE_NAME"
EOF
  chmod 750 "$UPDATE_HELPER"
  chown root:root "$UPDATE_HELPER"
}

configure_update_sudoers() {
  echo "${SERVICE_USER} ALL=(root) NOPASSWD: ${UPDATE_HELPER}, ${SYSTEMCTL_BIN} restart ${CAMILLA_SERVICE_NAME}" >"$SUDOERS_SNIPPET"
  chmod 440 "$SUDOERS_SNIPPET"
}

detect_camilla_archive() {
  local arch
  arch=$(uname -m)
  case "$arch" in
    armv7l|armv6l)
      echo "camilladsp-linux-armv7.tar.gz" ;;
    aarch64|arm64)
      echo "camilladsp-linux-aarch64.tar.gz" ;;
    x86_64|amd64)
      echo "camilladsp-linux-amd64.tar.gz" ;;
    *)
      echo "" ;;
  esac
}

require_root() {
  if [[ ${EUID} -ne 0 ]]; then
    echo "This script must be run as root (try: sudo bash install.sh ...)" >&2
    exit 1
  fi
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --user)
        SERVICE_USER="$2"; shift 2 ;;
      --install-dir)
        INSTALL_DIR="$2"; shift 2 ;;
      --repo)
        REPO_URL="$2"; shift 2 ;;
      --snap-port)
        SNAP_PORT="$2"; shift 2 ;;
      --mixer)
        MIXER_CONTROL="$2"; shift 2 ;;
      --playback-device)
        PLAYBACK_DEVICE="$2"; shift 2 ;;
      --camilla-port)
        CAMILLA_PORT="$2"; shift 2 ;;
      --camilla-retry)
        CAMILLA_RETRY_INTERVAL="$2"; shift 2 ;;
      --camilla-template)
        CAMILLA_TEMPLATE_PATH="$2"; shift 2 ;;
      --camilla-config)
        CAMILLA_CONFIG_PATH="$2"; shift 2 ;;
      --camilla-service)
        CAMILLA_SERVICE_NAME="$2"; shift 2 ;;
      -h|--help)
        usage; exit 0 ;;
      *)
        echo "Unknown option: $1" >&2
        usage; exit 1 ;;
    esac
  done

}

ensure_user_exists() {
  if ! id "$SERVICE_USER" &>/dev/null; then
    log "User $SERVICE_USER does not exist. Creating..."
    useradd -m "$SERVICE_USER"
  fi
  usermod -a -G audio "$SERVICE_USER" || true
}

prepare_state_dir() {
  install -d -o "$SERVICE_USER" -g "$SERVICE_USER" "$STATE_DIR"
  if [[ -f "$AGENT_SECRET_PATH" ]]; then
    chown "$SERVICE_USER":"$SERVICE_USER" "$AGENT_SECRET_PATH"
    chmod 600 "$AGENT_SECRET_PATH"
  fi
}

seed_agent_config() {
  rm -f "$AGENT_CONFIG_PATH"
}

install_packages() {
  log "Installing required apt packages"
  apt-get update -y
  apt-get install -y snapclient git python3 python3-venv python3-pip alsa-utils curl
}

sync_repo() {
  install -d -o "$SERVICE_USER" -g "$SERVICE_USER" "$INSTALL_DIR"
  if [[ ! -d "$INSTALL_DIR/.git" ]]; then
    log "Cloning RoomCast repo into $INSTALL_DIR"
    sudo -u "$SERVICE_USER" git clone "$REPO_URL" "$INSTALL_DIR"
  else
    log "Updating existing repo"
    sudo -u "$SERVICE_USER" git -C "$INSTALL_DIR" fetch --all --prune
    sudo -u "$SERVICE_USER" git -C "$INSTALL_DIR" reset --hard origin/main
  fi
}

setup_venv() {
  log "Setting up Python environment"
  python3 -m venv "$INSTALL_DIR/.venv"
  "$INSTALL_DIR/.venv/bin/pip" install --upgrade pip
  "$INSTALL_DIR/.venv/bin/pip" install -r "$INSTALL_DIR/node-agent/requirements.txt"
  chown -R "$SERVICE_USER":"$SERVICE_USER" "$INSTALL_DIR"
}

configure_loopback() {
  log "Configuring ALSA loopback device"
  modprobe snd-aloop || true
  cat <<EOF >/etc/modules-load.d/roomcast-loopback.conf
snd-aloop
EOF
}

install_camilladsp() {
  if command -v camilladsp >/dev/null 2>&1; then
    log "CamillaDSP already installed"
    return
  fi
  log "Installing CamillaDSP v${CAMILLA_VERSION}"
  local archive_name="${CAMILLA_ARCHIVE}"
  if [[ -z "$archive_name" ]]; then
    archive_name=$(detect_camilla_archive)
    if [[ -z "$archive_name" ]]; then
      echo "Unsupported architecture $(uname -m); please set CAMILLA_ARCHIVE manually" >&2
      exit 1
    fi
  fi
  tmpdir=$(mktemp -d)
  archive="${tmpdir}/${archive_name}"
  curl -L -o "$archive" "https://github.com/${CAMILLA_REPO}/releases/download/v${CAMILLA_VERSION}/${archive_name}"
  tar -xzf "$archive" -C "$tmpdir"
  if [[ ! -f ${tmpdir}/camilladsp ]]; then
    echo "Failed to find camilladsp binary in archive" >&2
    exit 1
  fi
  install -m 0755 "${tmpdir}/camilladsp" /usr/local/bin/camilladsp
  rm -rf "$tmpdir"
}

install_camilla_template() {
  log "Staging CamillaDSP template"
  local template_source="${INSTALL_DIR}/node-agent/camilladsp-config.yml"
  if [[ ! -f "$template_source" ]]; then
    echo "Camilla template not found at $template_source" >&2
    exit 1
  fi
  install -d "$(dirname "$CAMILLA_TEMPLATE_PATH")"
  install -m 0640 "$template_source" "$CAMILLA_TEMPLATE_PATH"
  chown "$SERVICE_USER":"$SERVICE_USER" "$CAMILLA_TEMPLATE_PATH" || true
}

write_camilla_config() {
  log "Rendering CamillaDSP config"
  install -d "$(dirname "$CAMILLA_CONFIG_PATH")"
  if [[ ! -f "$CAMILLA_TEMPLATE_PATH" ]]; then
    install_camilla_template
  fi
  sed -e "s#__PLAYBACK_DEVICE__#${PLAYBACK_DEVICE}#g" \
      -e "s#__CAMILLA_PORT__#${CAMILLA_PORT}#g" \
    "$CAMILLA_TEMPLATE_PATH" > "$CAMILLA_CONFIG_PATH"
  chown "$SERVICE_USER":"$SERVICE_USER" "$CAMILLA_CONFIG_PATH" || true
}

write_camilla_unit() {
  cat <<EOF >/etc/systemd/system/roomcast-camilla.service
[Unit]
Description=CamillaDSP (RoomCast)
After=network-online.target
Wants=network-online.target

[Service]
ExecStart=/usr/local/bin/camilladsp ${CAMILLA_CONFIG_PATH}
Restart=always
RestartSec=3
User=${SERVICE_USER}
WorkingDirectory=${INSTALL_DIR}/node-agent
Environment=CAMILLA_LOG_LEVEL=info

[Install]
WantedBy=multi-user.target
EOF
}

write_agent_unit() {
  cat <<EOF >/etc/systemd/system/roomcast-agent.service
[Unit]
Description=RoomCast Node Agent
After=network-online.target
Wants=network-online.target

[Service]
Environment=MIXER_CONTROL=${MIXER_CONTROL}
Environment=PLAYBACK_DEVICE=${PLAYBACK_DEVICE}
Environment=SNAPCLIENT_PORT=${SNAP_PORT}
Environment=CAMILLA_HOST=127.0.0.1
Environment=CAMILLA_PORT=${CAMILLA_PORT}
Environment=CAMILLA_FILTER_PATH=filters.peq_stack_{slot:02d}
Environment=CAMILLA_MAX_BANDS=31
Environment=CAMILLA_RETRY_INTERVAL=${CAMILLA_RETRY_INTERVAL}
Environment=CAMILLA_TEMPLATE_PATH=${CAMILLA_TEMPLATE_PATH}
Environment=CAMILLA_CONFIG_PATH=${CAMILLA_CONFIG_PATH}
Environment=CAMILLA_SERVICE_NAME=${CAMILLA_SERVICE_NAME}
Environment=AGENT_SECRET_PATH=${AGENT_SECRET_PATH}
Environment=AGENT_CONFIG_PATH=${AGENT_CONFIG_PATH}
WorkingDirectory=${INSTALL_DIR}/node-agent
ExecStart=${INSTALL_DIR}/.venv/bin/python ${INSTALL_DIR}/node-agent/agent.py
Restart=always
RestartSec=3
User=${SERVICE_USER}

[Install]
WantedBy=multi-user.target
EOF
}

enable_services() {
  systemctl daemon-reload
  systemctl enable roomcast-camilla.service
  systemctl enable roomcast-agent.service
  systemctl restart roomcast-camilla.service
  systemctl restart roomcast-agent.service
}

main() {
  parse_args "$@"
  require_root
  ensure_user_exists
  prepare_state_dir
  seed_agent_config
  install_packages
  sync_repo
  setup_venv
  configure_loopback
  install_camilladsp
  install_camilla_template
  write_camilla_config
  write_camilla_unit
  write_agent_unit
  write_update_env
  write_update_helper
  configure_update_sudoers
  enable_services
  log "Installation complete. Register this node via the RoomCast dashboard."
}

main "$@"
