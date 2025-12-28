#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  sudo ./prepare-sd-linux.sh --root <mounted-rootfs> [--ref <git-ref>] [--user <user>]

Example (typical on Linux):
  sudo mount /dev/sdX2 /mnt/pi-root
  sudo ./prepare-sd-linux.sh --root /mnt/pi-root --ref main --user pi

This installs a first-boot systemd oneshot into the mounted root filesystem.
On first boot, the Pi will download and run RoomCast's node-agent installer.
EOF
}

ROOT=""
REF="main"
USER_NAME="pi"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --root)
      ROOT="$2"; shift 2 ;;
    --ref)
      REF="$2"; shift 2 ;;
    --user)
      USER_NAME="$2"; shift 2 ;;
    -h|--help)
      usage; exit 0 ;;
    *)
      echo "Unknown arg: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ -z "$ROOT" ]]; then
  echo "--root is required" >&2
  usage
  exit 1
fi

if [[ ! -d "$ROOT" ]]; then
  echo "Root mount not found: $ROOT" >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

install -d "$ROOT/usr/local/sbin" "$ROOT/etc/systemd/system" "$ROOT/etc/systemd/system/multi-user.target.wants" "$ROOT/etc/roomcast" "$ROOT/var/lib/roomcast"

install -m 0755 "$SCRIPT_DIR/roomcast-firstboot.sh" "$ROOT/usr/local/sbin/roomcast-firstboot.sh"
install -m 0644 "$SCRIPT_DIR/roomcast-firstboot.service" "$ROOT/etc/systemd/system/roomcast-firstboot.service"

cat >"$ROOT/etc/roomcast/firstboot.env" <<EOF
ROOMCAST_REF=${REF}
SERVICE_USER=${USER_NAME}
EOF
chmod 0600 "$ROOT/etc/roomcast/firstboot.env" || true

ln -sf ../roomcast-firstboot.service "$ROOT/etc/systemd/system/multi-user.target.wants/roomcast-firstboot.service"

echo "OK: first-boot installer installed into $ROOT"
echo "- Logs: /var/log/roomcast-firstboot.log"
echo "- Marker: /var/lib/roomcast/firstboot.done"
