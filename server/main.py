import json
import asyncio
import ipaddress
import logging
import os
import re
import secrets
import socket
import string
import subprocess
import threading
import time
import uuid
from pathlib import Path
from urllib.parse import urlencode, urlparse, parse_qs
from typing import Dict, Optional, AsyncIterator, List, Callable, Any, Tuple, Literal
from xml.etree import ElementTree
from xml.sax.saxutils import escape as xml_escape

import httpx
import asyncssh
import websockets
from fastapi import Body, Depends, FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Request, Query
from fastapi.responses import FileResponse, JSONResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from itsdangerous import URLSafeSerializer, URLSafeTimedSerializer, BadSignature
from pydantic import BaseModel, Field
from webrtc import WebAudioRelay
from local_agent import (
    ensure_local_agent_running,
    local_agent_url,
    stop_local_agent,
)
import bcrypt

from providers.registry import AVAILABLE_PROVIDERS, get_provider_spec
from providers.storage import ProviderState, infer_providers, load_providers as _load_providers_file, save_providers as _save_providers_file
from providers.docker_runtime import DockerUnavailable, detect_docker_context, ensure_container_absent, ensure_container_running
from providers import spotify as spotify_provider
from providers import radio as radio_provider
from providers import audiobookshelf as audiobookshelf_provider

from api.auth import create_auth_router
from api.channels import create_channels_router
from api.nodes import NodeRegistration, VolumePayload, create_nodes_router
from api.providers import create_providers_router
from api.spotify import create_spotify_router
from api.radio import create_radio_router
from api.snapcast import create_snapcast_router
from api.sonos import create_sonos_router


logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("roomcast")

SNAPSERVER_HOST = os.getenv("SNAPSERVER_HOST", "snapserver")
SNAPSERVER_PORT = int(os.getenv("SNAPSERVER_PORT", "1780"))
SNAPCLIENT_PORT = int(os.getenv("SNAPCLIENT_PORT", "1704"))
CONFIG_PATH = Path(os.getenv("CONFIG_PATH", "/config/spotify.json"))
CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
STATIC_DIR = Path(__file__).parent / "static"
LIBRESPOT_STATUS_PATH = Path(os.getenv("STATUS_PATH", "/config/librespot-status.json"))
DISCOVERY_CIDR = os.getenv("DISCOVERY_CIDR", "").strip()
DISCOVERY_MAX_HOSTS = int(os.getenv("DISCOVERY_MAX_HOSTS", "4096"))
DISCOVERY_CONCURRENCY = int(os.getenv("DISCOVERY_CONCURRENCY", "25"))
AGENT_PORT = int(os.getenv("AGENT_PORT", "9700"))
SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID", "")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET", "")
SPOTIFY_REDIRECT_URI = os.getenv("SPOTIFY_REDIRECT_URI", "http://localhost:8000/api/spotify/callback")
SPOTIFY_TOKEN_PATH = Path(os.getenv("SPOTIFY_TOKEN_PATH", "/config/spotify-token.json"))
SPOTIFY_REFRESH_CHECK_INTERVAL = int(os.getenv("SPOTIFY_REFRESH_CHECK_INTERVAL", "60"))
SPOTIFY_REFRESH_LEEWAY = int(os.getenv("SPOTIFY_REFRESH_LEEWAY", "180"))
SPOTIFY_REFRESH_FAILURE_BACKOFF = int(os.getenv("SPOTIFY_REFRESH_FAILURE_BACKOFF", "120"))
TOKEN_SIGNER = URLSafeSerializer(os.getenv("SPOTIFY_STATE_SECRET", "changeme"))
NODES_PATH = Path(os.getenv("NODES_PATH", "/config/nodes.json"))
WEBRTC_ENABLED = os.getenv("WEBRTC_ENABLED", "1").lower() not in {"0", "false", "no"}
WEBRTC_LATENCY_MS = int(os.getenv("WEBRTC_LATENCY_MS", "150"))
WEBRTC_SAMPLE_RATE = int(os.getenv("WEBRTC_SAMPLE_RATE", "44100"))
SENSITIVE_NODE_FIELDS = {"agent_secret"}
TRANSIENT_NODE_FIELDS = {
    "wifi",
    "connection_state",
    "connection_error",
    "sonos_connecting_since",
    "sonos_last_stream_head_ts",
    "sonos_last_stream_get_ts",
    "sonos_last_stream_byte_ts",
    "sonos_stream_active",
    "sonos_stream_last_client",
}
AGENT_LATEST_VERSION = os.getenv("AGENT_LATEST_VERSION", "0.3.22").strip()
NODE_RESTART_TIMEOUT = int(os.getenv("NODE_RESTART_TIMEOUT", "120"))
NODE_RESTART_INTERVAL = int(os.getenv("NODE_RESTART_INTERVAL", "5"))
NODE_HEALTH_INTERVAL = int(os.getenv("NODE_HEALTH_INTERVAL", "30"))
NODE_REDISCOVERY_INTERVAL = int(os.getenv("NODE_REDISCOVERY_INTERVAL", "90"))
LIBRESPOT_FALLBACK_NAME = os.getenv("LIBRESPOT_FALLBACK_NAME", "RoomCast").strip() or "RoomCast"
SPOTIFY_SEARCH_TYPES = ("album", "track", "artist", "playlist")
NODE_TERMINAL_ENABLED = os.getenv("NODE_TERMINAL_ENABLED", "1").lower() not in {"0", "false", "no"}
NODE_TERMINAL_SSH_USER = os.getenv("NODE_TERMINAL_SSH_USER", "").strip()
NODE_TERMINAL_SSH_PASSWORD = os.getenv("NODE_TERMINAL_SSH_PASSWORD", "").strip()
NODE_TERMINAL_SSH_KEY_PATH = os.getenv("NODE_TERMINAL_SSH_KEY_PATH", "").strip()
NODE_TERMINAL_SSH_PORT = int(os.getenv("NODE_TERMINAL_SSH_PORT", "22"))
NODE_TERMINAL_TOKEN_TTL = int(os.getenv("NODE_TERMINAL_TOKEN_TTL", "60"))
NODE_TERMINAL_MAX_DURATION = int(os.getenv("NODE_TERMINAL_MAX_DURATION", "900"))
NODE_TERMINAL_STRICT_HOST_KEY = os.getenv("NODE_TERMINAL_STRICT_HOST_KEY", "0").lower() in {"1", "true", "yes"}
USERS_PATH = Path(os.getenv("USERS_PATH", "/config/users.json"))


def _normalize_section_name(name: Any) -> str:
    value = (name or "")
    if not isinstance(value, str):
        value = str(value)
    value = value.strip()
    return value


def _public_section(section: dict) -> dict:
    return {
        "id": section.get("id"),
        "name": section.get("name"),
    }
USERS_PATH.parent.mkdir(parents=True, exist_ok=True)
SERVER_DEFAULT_NAME = os.getenv("ROOMCAST_SERVER_NAME", "RoomCast").strip() or "RoomCast"
LOCAL_AGENT_NAME_SUFFIX = os.getenv("LOCAL_AGENT_NAME_SUFFIX", " (local)").strip() or " (local)"
SESSION_SECRET = os.getenv("AUTH_SESSION_SECRET", "roomcast-auth-secret")
SESSION_SIGNER = URLSafeTimedSerializer(SESSION_SECRET, salt="roomcast-session")
SESSION_COOKIE_NAME = os.getenv("AUTH_SESSION_COOKIE", "roomcast_session")
SESSION_COOKIE_SECURE = os.getenv("AUTH_SESSION_COOKIE_SECURE", "0").lower() in {"1", "true", "yes"}
_raw_samesite = os.getenv("AUTH_SESSION_COOKIE_SAMESITE", "lax").lower()
SESSION_COOKIE_SAMESITE = _raw_samesite if _raw_samesite in {"lax", "strict", "none"} else "lax"
SESSION_MAX_AGE = int(os.getenv("AUTH_SESSION_MAX_AGE", str(7 * 24 * 3600)))
CHANNELS_PATH = Path(os.getenv("CHANNELS_PATH", "/config/channels.json"))
CHANNELS_PATH.parent.mkdir(parents=True, exist_ok=True)
SOURCES_PATH = Path(os.getenv("SOURCES_PATH", "/config/sources.json"))
SOURCES_PATH.parent.mkdir(parents=True, exist_ok=True)
PROVIDERS_PATH = Path(os.getenv("PROVIDERS_PATH", "/config/providers.json"))
PROVIDERS_PATH.parent.mkdir(parents=True, exist_ok=True)
PRIMARY_CHANNEL_ID = os.getenv("PRIMARY_CHANNEL_ID", "ch1").strip() or "ch1"
CHANNEL_ID_PREFIX = os.getenv("CHANNEL_ID_PREFIX", "ch").strip() or "ch"
PLAYER_SNAPSHOT_PATH = Path(os.getenv("PLAYER_SNAPSHOT_PATH", "/config/player-snapshots.json"))
PLAYER_SNAPSHOT_PATH.parent.mkdir(parents=True, exist_ok=True)
RADIO_CHANNEL_SLOTS = [
    {
        "suffix": 3,
        "label": "Radio 1",
        "snap_stream": "Radio_CH1",
        "fifo_path": "/tmp/snapfifo-radio1",
        "color": "#f97316",
    },
    {
        "suffix": 4,
        "label": "Radio 2",
        "snap_stream": "Radio_CH2",
        "fifo_path": "/tmp/snapfifo-radio2",
        "color": "#a855f7",
    },
]


def radio_max_slots_supported() -> int:
    return len(RADIO_CHANNEL_SLOTS)


def radio_max_slots_configured() -> int:
    settings = get_provider_settings("radio")
    raw = settings.get("max_slots")
    try:
        desired = int(raw)
    except (TypeError, ValueError):
        desired = radio_max_slots_supported()
    desired = max(1, min(desired, radio_max_slots_supported()))
    return desired


def radio_channel_slots_active() -> list[dict]:
    return RADIO_CHANNEL_SLOTS[: radio_max_slots_configured()]
RADIO_BROWSER_BASE_URL = os.getenv("RADIO_BROWSER_BASE_URL", "https://de1.api.radio-browser.info/json").rstrip("/")
RADIO_BROWSER_TIMEOUT = float(os.getenv("RADIO_BROWSER_TIMEOUT", "8"))
RADIO_BROWSER_CACHE_TTL = int(os.getenv("RADIO_BROWSER_CACHE_TTL", "300"))
RADIO_BROWSER_USER_AGENT = os.getenv("RADIO_BROWSER_USER_AGENT", "RoomCast/Radio").strip() or "RoomCast/Radio"
RADIO_WORKER_TOKEN = os.getenv("RADIO_WORKER_TOKEN", "").strip()
RADIO_WORKER_PATH_PREFIX = "/api/radio/worker"
ABS_WORKER_TOKEN = os.getenv("AUDIOBOOKSHELF_WORKER_TOKEN", "").strip()
ABS_WORKER_PATH_PREFIX = "/api/audiobookshelf/worker"
ABS_HTTP_TIMEOUT = float(os.getenv("AUDIOBOOKSHELF_HTTP_TIMEOUT", "12"))
ABS_HTTP_TIMEOUT = max(3.0, min(ABS_HTTP_TIMEOUT, 60.0))
ABS_STREAM_URL_TTL = int(os.getenv("AUDIOBOOKSHELF_STREAM_URL_TTL", "300"))
ABS_STREAM_URL_TTL = max(30, min(ABS_STREAM_URL_TTL, 3600))

ABS_CHANNEL_SLOTS = [
    {
        "label": "Audiobookshelf 1",
        "snap_stream": "Audiobookshelf_CH1",
        "fifo_path": "/tmp/snapfifo-abs1",
    },
    {
        "label": "Audiobookshelf 2",
        "snap_stream": "Audiobookshelf_CH2",
        "fifo_path": "/tmp/snapfifo-abs2",
    },
]
CHANNEL_IDLE_TIMEOUT = float(os.getenv("CHANNEL_IDLE_TIMEOUT", "600"))
CHANNEL_IDLE_TIMEOUT = max(30.0, CHANNEL_IDLE_TIMEOUT)
CHANNEL_IDLE_POLL_INTERVAL = float(os.getenv("CHANNEL_IDLE_POLL_INTERVAL", "15"))
CHANNEL_IDLE_POLL_INTERVAL = max(1.0, min(CHANNEL_IDLE_POLL_INTERVAL, CHANNEL_IDLE_TIMEOUT))
WEB_NODE_APPROVAL_TIMEOUT = int(os.getenv("WEB_NODE_APPROVAL_TIMEOUT", "75"))
WEB_NODE_APPROVAL_TIMEOUT = max(10, WEB_NODE_APPROVAL_TIMEOUT)


def _is_private_snap_host(value: str) -> bool:
    try:
        parsed = ipaddress.ip_address(value)
    except ValueError:
        return False
    return parsed.is_loopback or parsed.is_unspecified or parsed.is_link_local


def _detect_primary_ipv4_host() -> Optional[str]:
    try:
        output = subprocess.check_output(
            ["ip", "-o", "-4", "addr", "show", "up", "scope", "global"],
            text=True,
        )
    except Exception:
        return None
    for line in output.splitlines():
        parts = line.split()
        if len(parts) < 4:
            continue
        iface_name = parts[1].rstrip(":")
        if iface_name.startswith("docker") or iface_name.startswith("br-") or iface_name.startswith("veth"):
            continue
        cidr = parts[3]
        try:
            iface = ipaddress.ip_interface(cidr)
        except ValueError:
            continue
        if iface.ip.is_loopback or iface.ip.is_link_local:
            continue
        return str(iface.ip)
    return None


def _resolve_snapserver_agent_host() -> str:
    override = os.getenv("SNAPSERVER_AGENT_HOST", "").strip()
    if override:
        return override
    candidate = (SNAPSERVER_HOST or "").strip()
    if candidate and not _is_private_snap_host(candidate):
        return candidate
    detected = _detect_primary_ipv4_host()
    if detected:
        return detected
    log.warning(
        "Falling back to %s for snapclient configuration; consider setting SNAPSERVER_AGENT_HOST",
        candidate or "127.0.0.1",
    )
    return candidate or "127.0.0.1"


SNAPSERVER_AGENT_HOST = _resolve_snapserver_agent_host()


def _resolve_public_controller_host() -> str:
    override = os.getenv("ROOMCAST_PUBLIC_HOST", "").strip()
    if override:
        return override
    detected = _detect_primary_ipv4_host()
    if detected:
        return detected
    return "127.0.0.1"


ROOMCAST_PUBLIC_HOST = _resolve_public_controller_host()
ROOMCAST_PUBLIC_PORT = int(os.getenv("ROOMCAST_PUBLIC_PORT", "8000"))

SONOS_HTTP_USER_AGENT = os.getenv("SONOS_HTTP_USER_AGENT", "RoomCast/Sonos").strip() or "RoomCast/Sonos"
SONOS_DISCOVERY_TIMEOUT = float(os.getenv("SONOS_DISCOVERY_TIMEOUT", "2.0"))
SONOS_CONTROL_TIMEOUT = float(os.getenv("SONOS_CONTROL_TIMEOUT", "8.0"))
SONOS_STREAM_BITRATE_KBPS = int(os.getenv("SONOS_STREAM_BITRATE_KBPS", "192"))

# Sonos link health monitoring + reconnection.
SONOS_CONNECTION_POLL_INTERVAL = float(os.getenv("SONOS_CONNECTION_POLL_INTERVAL", "5.0"))
SONOS_STREAM_STALE_SECONDS = float(os.getenv("SONOS_STREAM_STALE_SECONDS", "90.0"))
SONOS_CONNECT_GRACE_SECONDS = float(os.getenv("SONOS_CONNECT_GRACE_SECONDS", "30.0"))
SONOS_RECONNECT_ATTEMPTS = int(os.getenv("SONOS_RECONNECT_ATTEMPTS", "2"))
SONOS_RECONNECT_WAIT_FOR_STREAM_SECONDS = float(
    os.getenv("SONOS_RECONNECT_WAIT_FOR_STREAM_SECONDS", "20.0")
)

# If SSDP multicast discovery is blocked, fall back to a bounded HTTP probe.
SONOS_SCAN_MAX_HOSTS = int(os.getenv("SONOS_SCAN_MAX_HOSTS", "512"))
SONOS_SCAN_HTTP_TIMEOUT = float(os.getenv("SONOS_SCAN_HTTP_TIMEOUT", "0.8"))
SONOS_SCAN_CONCURRENCY = int(os.getenv("SONOS_SCAN_CONCURRENCY", "64"))

SONOS_DEVICE_TYPE = "urn:schemas-upnp-org:device:ZonePlayer:1"
SONOS_SSDP_ADDR = ("239.255.255.250", 1900)


channels_by_id: Dict[str, dict] = {}
channel_order: list[str] = []
sources_by_id: Dict[str, dict] = {}
providers_by_id: Dict[str, ProviderState] = {}
player_snapshots: Dict[str, dict] = {}
player_snapshot_lock = threading.Lock()


def load_providers_state() -> None:
    """Load installed providers.

    New installs start with none.
    Existing installs infer providers if providers.json is absent.
    """
    global providers_by_id
    if PROVIDERS_PATH.exists():
        providers_by_id = _load_providers_file(PROVIDERS_PATH)
        return
    inferred = infer_providers(CHANNELS_PATH, SOURCES_PATH)
    providers_by_id = inferred
    if inferred:
        _save_providers_file(PROVIDERS_PATH, inferred)


def save_providers_state() -> None:
    _save_providers_file(PROVIDERS_PATH, providers_by_id)


def is_provider_enabled(provider_id: str) -> bool:
    pid = (provider_id or "").strip().lower()
    state = providers_by_id.get(pid)
    return bool(state and state.enabled)


def get_provider_settings(provider_id: str) -> dict:
    pid = (provider_id or "").strip().lower()
    state = providers_by_id.get(pid)
    if not state or not isinstance(state.settings, dict):
        return {}
    return state.settings


def spotify_instance_count() -> int:
    raw = get_provider_settings("spotify").get("instances", 2)
    try:
        value = int(raw)
    except (TypeError, ValueError):
        value = 2
    return 2 if value >= 2 else 1


def _controller_container_id() -> str:
    # In Docker, HOSTNAME is set to the container id (short).
    return (os.getenv("HOSTNAME", "") or "").strip()


def require_spotify_provider() -> None:
    if not is_provider_enabled("spotify"):
        raise HTTPException(status_code=503, detail="Spotify provider is not installed")


def require_radio_provider() -> None:
    if not is_provider_enabled("radio"):
        raise HTTPException(status_code=503, detail="Radio provider is not installed")


def require_audiobookshelf_provider() -> None:
    if not is_provider_enabled("audiobookshelf"):
        raise HTTPException(status_code=503, detail="Audiobookshelf provider is not installed")


def _reconcile_spotify_runtime(instances: int) -> None:
    image = (os.getenv("ROOMCAST_LIBRESPOT_IMAGE") or "").strip() or "ghcr.io/malnes/roomcast-librespot:latest"
    try:
        spotify_provider.reconcile_runtime(
            controller_container_id=_controller_container_id(),
            instances=instances,
            librespot_image=image,
            fallback_name_a=LIBRESPOT_FALLBACK_NAME,
            fallback_name_b=os.getenv("LIBRESPOT_NAME_CH2", "RoomCast CH2"),
        )
    except DockerUnavailable as exc:
        raise HTTPException(status_code=503, detail=str(exc))


def _reconcile_radio_runtime(enabled: bool) -> None:
    image = (os.getenv("ROOMCAST_RADIO_WORKER_IMAGE") or "").strip() or "ghcr.io/malnes/roomcast-radio-worker:latest"
    try:
        radio_provider.reconcile_runtime(
            controller_container_id=_controller_container_id(),
            enabled=enabled,
            radio_worker_image=image,
            controller_base_url=os.getenv("CONTROLLER_BASE_URL", "http://controller:8000"),
            radio_worker_token=RADIO_WORKER_TOKEN,
            assignment_interval=int(os.getenv("RADIO_ASSIGNMENT_INTERVAL", "10")),
        )
    except DockerUnavailable as exc:
        raise HTTPException(status_code=503, detail=str(exc))


def _reconcile_audiobookshelf_runtime(enabled: bool) -> None:
    image = (os.getenv("ROOMCAST_AUDIOBOOKSHELF_WORKER_IMAGE") or "").strip() or "ghcr.io/malnes/roomcast-audiobookshelf-worker:latest"
    try:
        audiobookshelf_provider.reconcile_runtime(
            controller_container_id=_controller_container_id(),
            enabled=enabled,
            worker_image=image,
            controller_base_url=os.getenv("CONTROLLER_BASE_URL", "http://controller:8000"),
            worker_token=ABS_WORKER_TOKEN,
            assignment_interval=int(os.getenv("AUDIOBOOKSHELF_ASSIGNMENT_INTERVAL", "10")),
        )
    except DockerUnavailable as exc:
        raise HTTPException(status_code=503, detail=str(exc))


def _ensure_channel_exists(channel_id: str, *, name: str, order: int, snap_stream: str, fifo_path: str) -> None:
    cid = (channel_id or "").strip().lower()
    if not cid:
        return
    if cid in channels_by_id:
        return
    entry = _normalize_channel_entry({
        "id": cid,
        "name": name,
        "order": order,
        "snap_stream": snap_stream,
        "fifo_path": fifo_path,
        "enabled": True,
        "source": "none",
    }, fallback_order=order)
    channels_by_id[cid] = entry
    channel_order.append(cid)


def _apply_spotify_provider(instances: int) -> None:
    instances = 2 if instances >= 2 else 1
    sources_entries = spotify_provider.desired_source_entries(
        instances=instances,
        config_path_a=str(CONFIG_PATH),
        token_path_a=str(SPOTIFY_TOKEN_PATH),
        status_path_a=str(LIBRESPOT_STATUS_PATH),
        config_path_b="/config/spotify-ch2.json",
        token_path_b="/config/spotify-token-ch2.json",
        status_path_b="/config/librespot-status-ch2.json",
    )
    _write_sources_file(sources_entries)
    load_sources()
    _reconcile_spotify_runtime(instances)


def _disable_spotify_provider() -> None:
    # Remove spotify sources and detach channels.
    sources_by_id.pop("spotify:a", None)
    sources_by_id.pop("spotify:b", None)
    if SOURCES_PATH.exists():
        save_sources()

    for cid, channel in channels_by_id.items():
        if (channel.get("source") or "").strip().lower() == "spotify":
            channel["source"] = "none"
            channel["source_ref"] = None
            channel["snap_stream"] = f"Spotify_CH{channel.get('order', 1)}"
            channel["fifo_path"] = f"/tmp/snapfifo-{channel['id']}"
    save_channels()
    spotify_provider.stop_runtime()


def _apply_radio_provider() -> None:
    # Radio is a per-channel source; do not auto-create channels.
    _reconcile_radio_runtime(True)


def _apply_audiobookshelf_provider() -> None:
    # Audiobookshelf is a per-channel source; do not auto-create channels.
    _reconcile_audiobookshelf_runtime(True)


def _disable_radio_provider() -> None:
    # Detach radio channels.
    for channel in channels_by_id.values():
        if (channel.get("source") or "").strip().lower() == "radio":
            channel["source"] = "none"
            channel["source_ref"] = None
            channel["snap_stream"] = f"Spotify_CH{channel.get('order', 1)}"
            channel["fifo_path"] = f"/tmp/snapfifo-{channel['id']}"
            channel.pop("radio_state", None)
    save_channels()
    _reconcile_radio_runtime(False)


def _disable_audiobookshelf_provider() -> None:
    # Detach Audiobookshelf channels.
    for channel in channels_by_id.values():
        if (channel.get("source") or "").strip().lower() == "audiobookshelf":
            channel["source"] = "none"
            channel["source_ref"] = None
            channel["snap_stream"] = f"Spotify_CH{channel.get('order', 1)}"
            channel["fifo_path"] = f"/tmp/snapfifo-{channel['id']}"
            channel.pop("abs_state", None)
    save_channels()
    _reconcile_audiobookshelf_runtime(False)


def _sanitize_channel_color(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    raw = value.strip()
    if not raw:
        return None
    if not raw.startswith("#"):
        raw = f"#{raw}"
    if len(raw) not in {4, 7}:
        return None
    allowed = set(string.hexdigits)
    for ch in raw[1:]:
        if ch not in allowed:
            return None
    return raw.lower()


def _normalize_channel_id(value: Optional[str], fallback: str) -> str:
    candidate = (value or "").strip().lower()
    cleaned = "".join(ch for ch in candidate if ch.isalnum() or ch in {"-", "_"})
    return cleaned or fallback


def _load_player_snapshots() -> None:
    global player_snapshots
    if not PLAYER_SNAPSHOT_PATH.exists():
        player_snapshots = {}
        return
    try:
        data = json.loads(PLAYER_SNAPSHOT_PATH.read_text())
    except Exception as exc:  # pragma: no cover - filesystem edge
        log.warning("Failed to load player snapshots: %s", exc)
        player_snapshots = {}
        return
    if isinstance(data, dict):
        player_snapshots = data
    else:
        player_snapshots = {}


def _save_player_snapshots() -> None:
    try:
        PLAYER_SNAPSHOT_PATH.write_text(json.dumps(player_snapshots, indent=2, sort_keys=True))
    except Exception as exc:  # pragma: no cover - filesystem edge
        log.warning("Failed to persist player snapshots: %s", exc)


def _snapshot_track_item(item: Optional[dict]) -> Optional[dict]:
    if not isinstance(item, dict):
        return None
    track_uri = (item.get("uri") or "").strip()
    if not track_uri:
        return None
    simplified_artists = []
    for artist in item.get("artists") or []:
        if not isinstance(artist, dict):
            continue
        name = (artist.get("name") or "").strip()
        if not name:
            continue
        simplified_artists.append({"name": name, "uri": artist.get("uri")})
    album = item.get("album") or {}
    simplified_album = {
        "name": album.get("name"),
        "uri": album.get("uri"),
        "images": album.get("images"),
    }
    return {
        "name": item.get("name"),
        "uri": track_uri,
        "duration_ms": item.get("duration_ms"),
        "artists": simplified_artists,
        "album": simplified_album,
    }


def _snapshot_context(context: Optional[dict]) -> Optional[dict]:
    if not isinstance(context, dict):
        return None
    uri = (context.get("uri") or "").strip()
    if not uri:
        return None
    payload = {"uri": uri}
    context_type = context.get("type")
    if context_type:
        payload["type"] = context_type
    return payload


def _set_player_snapshot(channel_id: str, status: dict) -> None:
    if not channel_id:
        return
    item = _snapshot_track_item(status.get("item"))
    if not item:
        return
    context = _snapshot_context(status.get("context"))
    snapshot = {
        "item": item,
        "context": context,
        "captured_at": int(time.time()),
        "shuffle_state": bool(status.get("shuffle_state")),
        "repeat_state": status.get("repeat_state") or "off",
    }
    with player_snapshot_lock:
        existing = player_snapshots.get(channel_id)
        if existing:
            same_track = (existing.get("item") or {}).get("uri") == item.get("uri")
            same_context = (existing.get("context") or {}).get("uri") == (context or {}).get("uri")
            if same_track and same_context:
                existing["captured_at"] = snapshot["captured_at"]
                existing["shuffle_state"] = snapshot["shuffle_state"]
                existing["repeat_state"] = snapshot["repeat_state"]
                return
        player_snapshots[channel_id] = snapshot
        _save_player_snapshots()


def _get_player_snapshot(channel_id: Optional[str]) -> Optional[dict]:
    if not channel_id:
        return None
    with player_snapshot_lock:
        snapshot = player_snapshots.get(channel_id)
        if not snapshot:
            return None
        return snapshot.copy()


def _public_player_snapshot(snapshot: Optional[dict]) -> Optional[dict]:
    if not snapshot:
        return None
    return {
        "item": snapshot.get("item"),
        "context": snapshot.get("context"),
        "captured_at": snapshot.get("captured_at"),
        "shuffle_state": snapshot.get("shuffle_state", False),
        "repeat_state": snapshot.get("repeat_state", "off"),
    }


_load_player_snapshots()


def _normalize_channel_entry(entry: dict, fallback_order: int) -> dict:
    fallback_id = f"{CHANNEL_ID_PREFIX}{fallback_order}"
    channel_id = _normalize_channel_id(entry.get("id"), fallback_id)
    name = (entry.get("name") or f"Channel {fallback_order}").strip() or f"Channel {fallback_order}"
    try:
        order_value = int(entry.get("order", fallback_order))
    except (TypeError, ValueError):
        order_value = fallback_order
    snap_stream = (entry.get("snap_stream") or f"Spotify_CH{fallback_order}").strip() or f"Spotify_CH{fallback_order}"
    fifo_path = entry.get("fifo_path") or f"/tmp/snapfifo-{channel_id}"
    config_path = str(entry.get("config_path") or f"/config/spotify-{channel_id}.json")
    token_path = str(entry.get("token_path") or f"/config/spotify-token-{channel_id}.json")
    status_path = str(entry.get("status_path") or f"/config/librespot-status-{channel_id}.json")
    color = _sanitize_channel_color(entry.get("color"))
    enabled_value = entry.get("enabled")
    if enabled_value is None:
        enabled = True
    elif isinstance(enabled_value, str):
        enabled = enabled_value.strip().lower() not in {"0", "false", "no"}
    else:
        enabled = bool(enabled_value)
    source = (entry.get("source") or "none").strip().lower()
    if source not in {"spotify", "radio", "audiobookshelf", "none"}:
        source = "none"
    source_ref_raw = (entry.get("source_ref") or "").strip().lower()
    source_ref: Optional[str] = None
    if source == "spotify":
        source_ref = _normalize_spotify_source_id(source_ref_raw)
        if not source_ref:
            # Backwards-compatible mapping: ch2 defaults to Spotify B, everything else to A.
            source_ref = "spotify:b" if channel_id == f"{CHANNEL_ID_PREFIX}2" else "spotify:a"
    elif source == "radio":
        source_ref = "radio"
    elif source == "audiobookshelf":
        source_ref = "audiobookshelf"
    radio_state = None
    if source == "radio":
        radio_state = _normalize_radio_state(entry.get("radio_state"))
    abs_state = None
    if source == "audiobookshelf":
        abs_state = _normalize_abs_state(entry.get("abs_state"))
    return {
        "id": channel_id,
        "name": name,
        "order": order_value,
        "snap_stream": snap_stream,
        "fifo_path": fifo_path,
        "config_path": config_path,
        "token_path": token_path,
        "status_path": status_path,
        "color": color,
        "enabled": enabled,
        "source": source,
        "source_ref": source_ref,
        "radio_state": radio_state,
        "abs_state": abs_state,
    }


def _default_radio_state() -> dict:
    return {
        "station_id": None,
        "station_name": None,
        "stream_url": None,
        "station_country": None,
        "station_countrycode": None,
        "station_favicon": None,
        "station_homepage": None,
        "bitrate": None,
        "last_metadata": None,
        "updated_at": None,
        "tags": [],
        "playback_enabled": True,
    }


def _normalize_radio_state(value: Optional[dict]) -> dict:
    state = _default_radio_state()
    if not isinstance(value, dict):
        return state
    for key in state.keys():
        if key == "tags":
            tags_value = value.get("tags")
            if isinstance(tags_value, list):
                state["tags"] = [str(tag).strip() for tag in tags_value if str(tag).strip()]
            elif isinstance(tags_value, str):
                state["tags"] = [segment.strip() for segment in tags_value.split(",") if segment.strip()]
            else:
                state["tags"] = []
            continue
        if key == "playback_enabled":
            raw = value.get("playback_enabled")
            if isinstance(raw, str):
                state[key] = raw.strip().lower() not in {"0", "false", "no"}
            else:
                state[key] = bool(raw) if raw is not None else True
            continue
        state[key] = value.get(key)
    return state


def _default_abs_state() -> dict:
    return {
        "library_item_id": None,
        "episode_id": None,
        "podcast_title": None,
        "episode_title": None,
        "playback_enabled": True,
        "updated_at": None,
        "content_url": None,
        "content_url_ts": None,
    }


def _normalize_abs_state(value: Optional[dict]) -> dict:
    state = _default_abs_state()
    if not isinstance(value, dict):
        return state
    for key in state.keys():
        if key == "playback_enabled":
            raw = value.get("playback_enabled")
            if isinstance(raw, str):
                state[key] = raw.strip().lower() not in {"0", "false", "no"}
            else:
                state[key] = bool(raw) if raw is not None else True
            continue
        state[key] = value.get(key)
    return state


def _default_channel_entries() -> list[dict]:
    # New installs start with exactly one channel and no source selected.
    # Source selection determines whether a channel is usable.
    return [{
        "id": f"{CHANNEL_ID_PREFIX}1",
        "name": "Channel 1",
        "order": 1,
        "snap_stream": "Spotify_CH1",
        "fifo_path": "/tmp/snapfifo-ch1",
        "config_path": str(CONFIG_PATH),
        "token_path": str(SPOTIFY_TOKEN_PATH),
        "status_path": str(LIBRESPOT_STATUS_PATH),
        "color": "#22c55e",
        "enabled": True,
        "source": "none",
        "source_ref": None,
    }]


def _radio_channel_templates() -> list[dict]:
    templates: list[dict] = []
    for idx, slot in enumerate(radio_channel_slots_active(), start=1):
        templates.append({
            "id": f"{CHANNEL_ID_PREFIX}{slot['suffix']}",
            "name": slot["label"],
            "order": slot["suffix"],
            "snap_stream": slot["snap_stream"],
            "fifo_path": slot["fifo_path"],
            "color": slot["color"],
            "enabled": False,
            "source": "radio",
            "radio_state": _default_radio_state(),
        })
    return templates


def _ensure_radio_channels(entries: list[dict]) -> None:
    existing_ids = {entry["id"] for entry in entries}
    radio_entries = [entry for entry in entries if entry.get("source") == "radio"]
    for template in _radio_channel_templates():
        channel_id = template["id"]
        if channel_id in existing_ids:
            continue
        normalized_entry = _normalize_channel_entry(template, fallback_order=template["order"])
        entries.append(normalized_entry)
        existing_ids.add(channel_id)
        radio_entries.append(normalized_entry)
    desired_radio_count = len(radio_channel_slots_active())
    next_suffix = max((slot["suffix"] for slot in RADIO_CHANNEL_SLOTS), default=2)
    next_order = max((entry.get("order", 0) for entry in entries), default=0)
    while len(radio_entries) < desired_radio_count:
        next_suffix += 1
        channel_id = f"{CHANNEL_ID_PREFIX}{next_suffix}"
        if channel_id in existing_ids:
            continue
        next_order += 1
        radio_index = len(radio_entries) + 1
        template = {
            "id": channel_id,
            "name": f"Radio {radio_index}",
            "order": next_order,
            "snap_stream": f"Radio_CH{radio_index}",
            "fifo_path": f"/tmp/snapfifo-radio{radio_index}",
            "color": radio_index == 1 and "#f97316" or "#a855f7",
            "enabled": False,
            "source": "radio",
            "radio_state": _default_radio_state(),
        }
        normalized_entry = _normalize_channel_entry(template, fallback_order=next_order)
        entries.append(normalized_entry)
        radio_entries.append(normalized_entry)
        existing_ids.add(channel_id)


def _hydrate_channels(raw_entries: list[Any], *, ensure_radio: bool) -> list[dict]:
    normalized: list[dict] = []
    seen_ids: set[str] = set()
    entries = raw_entries or []
    for idx, entry in enumerate(entries, start=1):
        if not isinstance(entry, dict):
            continue
        normalized_entry = _normalize_channel_entry(entry, fallback_order=idx)
        if normalized_entry["id"] in seen_ids:
            continue
        normalized.append(normalized_entry)
        seen_ids.add(normalized_entry["id"])
    if not normalized:
        normalized = _default_channel_entries()
    if ensure_radio:
        _ensure_radio_channels(normalized)
    normalized.sort(key=lambda item: item.get("order", 0))
    return normalized


def _ensure_radio_state(channel: dict) -> dict:
    state = channel.get("radio_state")
    if isinstance(state, dict):
        normalized = _normalize_radio_state(state)
    else:
        normalized = _default_radio_state()
    channel["radio_state"] = normalized
    return normalized


def _get_radio_channel_or_404(channel_id: str) -> dict:
    channel = get_channel(channel_id)
    if channel.get("source") != "radio":
        raise HTTPException(status_code=400, detail="Channel is not a radio channel")
    return channel


def _apply_radio_station(channel: dict, payload: Any) -> dict:
    state = _default_radio_state()
    state.update({
        "station_id": getattr(payload, "station_id", None),
        "station_name": getattr(payload, "name", None),
        "stream_url": getattr(payload, "stream_url", None),
        "station_country": getattr(payload, "country", None),
        "station_countrycode": getattr(payload, "countrycode", None),
        "station_favicon": getattr(payload, "favicon", None),
        "station_homepage": getattr(payload, "homepage", None),
        "bitrate": getattr(payload, "bitrate", None),
        "tags": getattr(payload, "tags", None) or [],
        "updated_at": int(time.time()),
        "playback_enabled": True,
    })
    channel["radio_state"] = state
    return state


def _write_channels_file(entries: list[dict]) -> None:
    CHANNELS_PATH.write_text(json.dumps(entries, indent=2))


def _ensure_channel_paths(entry: dict) -> None:
    Path(entry["config_path"]).parent.mkdir(parents=True, exist_ok=True)
    Path(entry["token_path"]).parent.mkdir(parents=True, exist_ok=True)
    Path(entry["status_path"]).parent.mkdir(parents=True, exist_ok=True)


def _write_sources_file(entries: list[dict]) -> None:
    SOURCES_PATH.write_text(json.dumps(entries, indent=2))


def _is_spotify_source_id(value: Optional[str]) -> bool:
    if not isinstance(value, str):
        return False
    return value.strip().lower().startswith("spotify:")


def _normalize_spotify_source_id(value: Optional[str]) -> Optional[str]:
    if not isinstance(value, str):
        return None
    raw = value.strip().lower()
    if not raw:
        return None
    if not raw.startswith("spotify:"):
        return None
    return raw


def _default_source_entries(*, instances: int) -> list[dict]:
    # Source instances are the stable units of authentication (e.g. Spotify login A/B).
    # They can be routed to any logical channel by updating channel.source_ref.
    entries = [
        {
            "id": "spotify:a",
            "kind": "spotify",
            "name": "Spotify A",
            "snap_stream": "Spotify_CH1",
            "config_path": str(CONFIG_PATH),
            "token_path": str(SPOTIFY_TOKEN_PATH),
            "status_path": str(LIBRESPOT_STATUS_PATH),
        },
    ]
    if instances >= 2:
        entries.append({
            "id": "spotify:b",
            "kind": "spotify",
            "name": "Spotify B",
            "snap_stream": "Spotify_CH2",
            "config_path": "/config/spotify-ch2.json",
            "token_path": "/config/spotify-token-ch2.json",
            "status_path": "/config/librespot-status-ch2.json",
        })
    return entries


def load_sources() -> None:
    global sources_by_id
    entries: list[Any] = []
    if SOURCES_PATH.exists():
        try:
            entries = json.loads(SOURCES_PATH.read_text()) or []
        except json.JSONDecodeError:
            log.warning("sources.json is invalid; regenerating defaults")
            entries = []
    if not entries:
        if is_provider_enabled("spotify"):
            entries = _default_source_entries(instances=spotify_instance_count())
            _write_sources_file(entries)
        else:
            sources_by_id = {}
            return
    normalized: Dict[str, dict] = {}
    for raw in entries:
        if not isinstance(raw, dict):
            continue
        kind = (raw.get("kind") or "").strip().lower()
        if kind != "spotify":
            continue
        source_id = _normalize_spotify_source_id(raw.get("id"))
        if not source_id:
            continue
        name = (raw.get("name") or source_id).strip() or source_id
        snap_stream = (raw.get("snap_stream") or "").strip()
        if not snap_stream:
            continue
        config_path = str(raw.get("config_path") or "").strip()
        token_path = str(raw.get("token_path") or "").strip()
        status_path = str(raw.get("status_path") or "").strip()
        if not (config_path and token_path and status_path):
            continue
        Path(config_path).parent.mkdir(parents=True, exist_ok=True)
        Path(token_path).parent.mkdir(parents=True, exist_ok=True)
        Path(status_path).parent.mkdir(parents=True, exist_ok=True)
        normalized[source_id] = {
            "id": source_id,
            "kind": "spotify",
            "name": name,
            "snap_stream": snap_stream,
            "config_path": config_path,
            "token_path": token_path,
            "status_path": status_path,
        }
    # Ensure required defaults exist even if file is partially configured.
    desired_instances = spotify_instance_count() if is_provider_enabled("spotify") else 0
    defaults = {item["id"]: item for item in _default_source_entries(instances=max(1, desired_instances))} if desired_instances else {}
    dirty = False
    if desired_instances:
        for sid, entry in defaults.items():
            if sid not in normalized:
                normalized[sid] = dict(entry)
                dirty = True
        # Trim sources that exceed the configured instance count.
        if desired_instances < 2 and "spotify:b" in normalized:
            normalized.pop("spotify:b", None)
            dirty = True
    sources_by_id = normalized
    if dirty:
        _write_sources_file(list(sources_by_id.values()))


def save_sources() -> None:
    _write_sources_file(list(sources_by_id.values()))


def get_spotify_source(source_id: Optional[str]) -> dict:
    sid = _normalize_spotify_source_id(source_id)
    if not sid or sid not in sources_by_id:
        raise HTTPException(status_code=404, detail="Spotify source not found")
    return sources_by_id[sid]


def resolve_channel_spotify_source_id(channel_id: Optional[str]) -> Optional[str]:
    channel = get_channel(channel_id)
    ref = (channel.get("source_ref") or "").strip().lower()
    sid = _normalize_spotify_source_id(ref)
    if sid:
        return sid if sid in sources_by_id else None
    if (channel.get("source") or "spotify").strip().lower() != "spotify":
        return None
    # Backwards-compatible mapping for existing installs.
    if channel.get("id") == f"{CHANNEL_ID_PREFIX}2":
        return "spotify:b"
    return "spotify:a"


def load_channels() -> None:
    global channels_by_id, channel_order
    entries: list[Any] = []
    if CHANNELS_PATH.exists():
        try:
            entries = json.loads(CHANNELS_PATH.read_text()) or []
        except json.JSONDecodeError:
            log.warning("channels.json is invalid; regenerating defaults")
    # Radio no longer creates dedicated channel entries; it is a per-channel source.
    normalized = _hydrate_channels(entries, ensure_radio=False)
    channels_by_id = {entry["id"]: entry for entry in normalized}
    channel_order = [entry["id"] for entry in normalized]
    for entry in normalized:
        _ensure_channel_paths(entry)
    if not CHANNELS_PATH.exists() or not entries:
        _write_channels_file(normalized)
    _resequence_channel_order()


def save_channels() -> None:
    _resequence_channel_order()
    ordered = [channels_by_id[cid] for cid in channel_order if cid in channels_by_id]
    _write_channels_file(ordered)


def _resequence_channel_order() -> None:
    global channel_order
    ordered = sorted(channels_by_id.values(), key=lambda entry: entry.get("order", 0))
    channel_order = [entry["id"] for entry in ordered]
    for idx, cid in enumerate(channel_order, start=1):
        channels_by_id[cid]["order"] = idx


def _primary_channel_id() -> str:
    if PRIMARY_CHANNEL_ID in channels_by_id:
        return PRIMARY_CHANNEL_ID
    if channel_order:
        return channel_order[0]
    raise HTTPException(status_code=500, detail="No channels configured")


def resolve_channel_id(channel_id: Optional[str]) -> str:
    if channel_id:
        normalized = channel_id.strip().lower()
        if normalized not in channels_by_id:
            raise HTTPException(status_code=404, detail="Channel not found")
        return normalized
    return _primary_channel_id()


def get_channel(channel_id: Optional[str]) -> dict:
    resolved = resolve_channel_id(channel_id)
    return channels_by_id[resolved]


def channels_public() -> list[dict]:
    items = []
    for cid in channel_order:
        entry = channels_by_id.get(cid)
        if not entry:
            continue
        items.append({
            "id": entry["id"],
            "name": entry["name"],
            "order": entry["order"],
            "snap_stream": entry["snap_stream"],
            "fifo_path": entry["fifo_path"],
            "color": entry.get("color"),
            "enabled": entry.get("enabled", True),
            "source": entry.get("source", "none"),
            "source_ref": entry.get("source_ref"),
            "radio_state": entry.get("radio_state"),
            "abs_state": entry.get("abs_state"),
        })
    return items


load_providers_state()
load_channels()
load_sources()


def channel_detail(channel_id: str) -> dict:
    entry = get_channel(channel_id)
    data = {
        "id": entry["id"],
        "name": entry["name"],
        "order": entry["order"],
        "color": entry.get("color"),
        "snap_stream": entry["snap_stream"],
        "fifo_path": entry["fifo_path"],
        "config_path": entry["config_path"],
        "token_path": entry["token_path"],
        "status_path": entry["status_path"],
        "enabled": entry.get("enabled", True),
        "source": entry.get("source", "none"),
        "source_ref": entry.get("source_ref"),
        "radio_state": entry.get("radio_state"),
        "spotify": read_spotify_config(entry["id"]),
        "librespot_status": read_librespot_status(entry["id"]),
    }
    return data


def all_channel_details() -> list[dict]:
    return [channel_detail(cid) for cid in channel_order if cid in channels_by_id]


def update_channel_metadata(channel_id: str, updates: dict) -> dict:
    channel = get_channel(channel_id)
    was_radio_channel = (channel.get("source") or "").strip().lower() == "radio"
    was_abs_channel = (channel.get("source") or "").strip().lower() == "audiobookshelf"
    routing_changed = False
    previous_snap_stream = (channel.get("snap_stream") or "").strip()
    if "name" in updates:
        name = (updates.get("name") or "").strip()
        if not name:
            raise HTTPException(status_code=400, detail="Channel name cannot be empty")
        channel["name"] = name
    if "color" in updates:
        channel["color"] = _sanitize_channel_color(updates.get("color"))
    if "snap_stream" in updates:
        snap_stream = (updates.get("snap_stream") or "").strip()
        if not snap_stream:
            raise HTTPException(status_code=400, detail="Snapstream name cannot be empty")
        channel["snap_stream"] = snap_stream
        routing_changed = routing_changed or (snap_stream != previous_snap_stream)
    if "enabled" in updates:
        channel["enabled"] = bool(updates.get("enabled"))
    if "source_ref" in updates:
        raw_ref = (updates.get("source_ref") or "").strip().lower()
        if not raw_ref:
            channel["source"] = "none"
            channel["source_ref"] = None
            channel["snap_stream"] = f"Spotify_CH{channel.get('order', 1)}"
            channel["fifo_path"] = f"/tmp/snapfifo-{channel['id']}"
            channel.pop("radio_state", None)
            channel.pop("abs_state", None)
            routing_changed = routing_changed or (channel["snap_stream"] != previous_snap_stream)
        else:
            requested_spotify = _normalize_spotify_source_id(raw_ref)
            requested_radio = raw_ref.startswith("radio")
            requested_abs = raw_ref.startswith("audiobookshelf")

        if raw_ref and requested_spotify:
            require_spotify_provider()
            source = get_spotify_source(requested_spotify)
            channel["source"] = "spotify"
            channel["source_ref"] = requested_spotify
            # Route this logical channel to the selected Spotify source stream.
            channel["snap_stream"] = source["snap_stream"]
            if was_radio_channel:
                # Radio FIFO mappings are not used for Spotify channels.
                channel["fifo_path"] = channel.get("fifo_path") or f"/tmp/snapfifo-{channel['id']}"
            routing_changed = routing_changed or (channel["snap_stream"] != previous_snap_stream)
        elif raw_ref and requested_radio:
            require_radio_provider()
            # Limited radio pipelines: allocate a snapserver/fifo slot.
            cid = channel.get("id")

            def _channel_has_assigned_nodes(target_id: str) -> bool:
                for node in nodes.values():
                    if resolve_node_channel_id(node) == target_id:
                        return True
                return False

            def _is_radio_active(entry: dict) -> bool:
                # Only reserve a radio slot if the other channel is actually
                # configured (tuned). This avoids
                # "stale" radio channels permanently blocking all slots.
                state = entry.get("radio_state")
                normalized = _normalize_radio_state(state) if isinstance(state, dict) else _default_radio_state()
                if not normalized.get("stream_url"):
                    return False
                # Even if paused, keep the slot reserved so channels don't
                # end up sharing a fifo/snap stream.
                return True

            used_streams: set[str] = set()
            for other_id, other in channels_by_id.items():
                if other_id == cid:
                    continue
                if (other.get("source") or "").strip().lower() != "radio":
                    continue
                if not _is_radio_active(other):
                    continue
                stream = (other.get("snap_stream") or "").strip()
                if stream:
                    used_streams.add(stream)

            current_stream = (channel.get("snap_stream") or "").strip()
            current_slot = None
            for slot in radio_channel_slots_active():
                if slot.get("snap_stream") == current_stream:
                    current_slot = slot
                    break

            chosen_slot = None
            if current_slot and current_stream and current_stream not in used_streams:
                chosen_slot = current_slot
            else:
                for slot in radio_channel_slots_active():
                    stream = (slot.get("snap_stream") or "").strip()
                    if not stream:
                        continue
                    if stream in used_streams:
                        continue
                    chosen_slot = slot
                    break

            if not chosen_slot:
                raise HTTPException(
                    status_code=409,
                    detail=f"No radio slots available (max {len(radio_channel_slots_active())}). Switch another channel away from Radio first.",
                )

            channel["source"] = "radio"
            channel["source_ref"] = "radio"
            channel["snap_stream"] = chosen_slot["snap_stream"]
            channel["fifo_path"] = chosen_slot["fifo_path"]
            _ensure_radio_state(channel)
            routing_changed = routing_changed or (channel["snap_stream"] != previous_snap_stream)
        elif raw_ref and requested_abs:
            require_audiobookshelf_provider()

            cid = channel.get("id")

            def _channel_has_assigned_nodes(target_id: str) -> bool:
                for node in nodes.values():
                    if resolve_node_channel_id(node) == target_id:
                        return True
                return False

            def _is_abs_active(entry: dict) -> bool:
                if (entry.get("enabled") is None) or bool(entry.get("enabled")):
                    return True
                entry_id = entry.get("id")
                return bool(entry_id and _channel_has_assigned_nodes(entry_id))

            used_streams: set[str] = set()
            for other_id, other in channels_by_id.items():
                if other_id == cid:
                    continue
                if (other.get("source") or "").strip().lower() != "audiobookshelf":
                    continue
                if not _is_abs_active(other):
                    continue
                stream = (other.get("snap_stream") or "").strip()
                if stream:
                    used_streams.add(stream)

            current_stream = (channel.get("snap_stream") or "").strip()
            current_slot = None
            for slot in ABS_CHANNEL_SLOTS:
                if slot.get("snap_stream") == current_stream:
                    current_slot = slot
                    break

            chosen_slot = None
            if current_slot and current_stream and current_stream not in used_streams:
                chosen_slot = current_slot
            else:
                for slot in ABS_CHANNEL_SLOTS:
                    stream = (slot.get("snap_stream") or "").strip()
                    if not stream:
                        continue
                    if stream in used_streams:
                        continue
                    chosen_slot = slot
                    break

            if not chosen_slot:
                raise HTTPException(
                    status_code=409,
                    detail=f"No Audiobookshelf slots available (max {len(ABS_CHANNEL_SLOTS)}). Switch another channel away from Audiobookshelf first.",
                )

            channel["source"] = "audiobookshelf"
            channel["source_ref"] = "audiobookshelf"
            channel["snap_stream"] = chosen_slot["snap_stream"]
            channel["fifo_path"] = chosen_slot["fifo_path"]
            channel["abs_state"] = _normalize_abs_state(channel.get("abs_state"))
            routing_changed = routing_changed or (channel["snap_stream"] != previous_snap_stream)
        elif raw_ref:
            raise HTTPException(status_code=400, detail="Invalid source_ref")
    if "order" in updates:
        try:
            channel["order"] = max(1, int(updates["order"]))
        except (TypeError, ValueError):
            raise HTTPException(status_code=400, detail="Order must be a positive integer")
    save_channels()
    now_radio_channel = (channel.get("source") or "").strip().lower() == "radio"
    now_abs_channel = (channel.get("source") or "").strip().lower() == "audiobookshelf"
    if was_radio_channel or now_radio_channel:
        _mark_radio_assignments_dirty()
    if was_abs_channel or now_abs_channel:
        _mark_abs_assignments_dirty()
    result = dict(channel)
    result["_routing_changed"] = routing_changed
    return result


async def _apply_channel_routing(channel_id: str) -> None:
    """Re-apply routing for all nodes currently assigned to this channel.

    This is the critical piece for hot-swapping: if a channel's snap_stream changes
    (e.g. via source_ref), existing destinations must be updated immediately.
    """
    cid = resolve_channel_id(channel_id)
    channel = channels_by_id.get(cid)
    if not channel:
        return
    stream_id = (channel.get("snap_stream") or "").strip()

    # Update Snapcast-backed nodes and WebRTC nodes.
    for node in list(nodes.values()):
        if resolve_node_channel_id(node) != cid:
            continue
        if node.get("type") == "agent":
            try:
                client_id = await _ensure_snapclient_stream(node, channel)
                node["snapclient_id"] = client_id
            except Exception as exc:
                log.warning("Channel routing: failed to move snapclient node %s: %s", node.get("id"), exc)
        elif node.get("type") == "browser" and webrtc_relay:
            try:
                await webrtc_relay.update_session_channel(node["id"], cid, stream_id or None)
            except Exception as exc:
                log.warning("Channel routing: failed to update WebRTC node %s: %s", node.get("id"), exc)

    # Sonos uses HTTP pull; force coordinator refresh by reconciling.
    try:
        await _reconcile_sonos_groups()
    except Exception as exc:
        log.warning("Channel routing: Sonos reconcile failed: %s", exc)

    save_nodes()


class SnapcastClient:
    def __init__(self, host: str, port: int = 1780) -> None:
        self.url = f"ws://{host}:{port}/jsonrpc"
        self._supports_client_setstream: Optional[bool] = None

    async def _rpc(self, method: str, params: Optional[dict] = None) -> dict:
        payload = {"id": 1, "jsonrpc": "2.0", "method": method}
        if params:
            payload["params"] = params

        async with websockets.connect(self.url) as ws:
            await ws.send(json.dumps(payload))
            raw = await ws.recv()
        resp = json.loads(raw)
        if "error" in resp:
            raise RuntimeError(resp["error"])
        return resp.get("result", {})

    async def status(self) -> dict:
        return await self._rpc("Server.GetStatus")

    async def set_client_volume(self, client_id: str, percent: int) -> dict:
        params = {"id": client_id, "volume": {"percent": percent}}
        return await self._rpc("Client.SetVolume", params)

    async def list_clients(self) -> list:
        status = await self.status()
        clients = []
        for group in status.get("server", {}).get("groups", []):
            group_id = group.get("id")
            stream_id = group.get("stream_id")
            for client in group.get("clients", []):
                enriched = dict(client)
                enriched["_group_id"] = group_id
                enriched["_stream_id"] = stream_id
                clients.append(enriched)
        return clients

    async def set_client_stream(self, client_id: str, stream_id: str) -> dict:
        """Assign a client to a stream.

        Snapserver JSON-RPC differs by version:
        - Some versions support `Client.SetStream`.
        - Some only support `Group.SetStream`.

        We probe once and then fall back to group switching when needed.
        """
        params = {"id": client_id, "stream_id": stream_id}

        if self._supports_client_setstream is not False:
            try:
                result = await self._rpc("Client.SetStream", params)
            except Exception as exc:
                if _is_rpc_method_not_found_error(exc):
                    self._supports_client_setstream = False
                else:
                    raise
            else:
                self._supports_client_setstream = True
                return result

        status = await self.status()
        group_id: Optional[str] = None
        for group in status.get("server", {}).get("groups", []) or []:
            if not isinstance(group, dict):
                continue
            for client in group.get("clients", []) or []:
                if isinstance(client, dict) and client.get("id") == client_id:
                    group_id = group.get("id")
                    break
            if group_id:
                break
        if not group_id:
            raise RuntimeError(f"Snapclient {client_id} is not registered with snapserver yet")
        return await self.set_group_stream(group_id, stream_id)

    async def set_group_stream(self, group_id: str, stream_id: str) -> dict:
        params = {"id": group_id, "stream_id": stream_id}
        return await self._rpc("Group.SetStream", params)

    async def delete_client(self, client_id: str) -> dict:
        params = {"id": client_id}
        return await self._rpc("Server.DeleteClient", params)


snapcast = SnapcastClient(SNAPSERVER_HOST, SNAPSERVER_PORT)
app = FastAPI(title="RoomCast Controller", version="0.1.0")
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


def _safe_int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _public_snap_stream(entry: Optional[dict]) -> Optional[dict]:
    if not isinstance(entry, dict):
        return None
    uri = entry.get("uri")
    if not uri:
        status = entry.get("status") or {}
        uri = status.get("uri") or entry.get("location")
    parsed = urlparse(uri) if uri else None
    params = parse_qs(parsed.query) if parsed else {}
    sampleformat = (params.get("sampleformat") or [None])[0]
    format_info = None
    if sampleformat:
        parts = sampleformat.split(":")
        format_info = {
            "raw": sampleformat,
            "sample_rate": _safe_int(parts[0]) if parts else None,
            "bit_depth": parts[1] if len(parts) > 1 else None,
            "channels": parts[2] if len(parts) > 2 else None,
        }
    codec = (params.get("codec") or [None])[0]
    payload = {
        "id": entry.get("id") or entry.get("name"),
        "name": entry.get("name"),
        "uri": uri,
        "codec": codec,
        "format": format_info,
        "status": entry.get("status"),
        "properties": entry.get("properties"),
    }
    return payload


def _summarize_snapserver_status(status: Optional[dict]) -> tuple[dict[str, dict], dict[str, list[dict]]]:
    if not isinstance(status, dict):
        return {}, {}
    server = status.get("server") or {}
    streams: dict[str, dict] = {}
    for entry in server.get("streams", []) or []:
        if not isinstance(entry, dict):
            continue
        sid = entry.get("id") or entry.get("name")
        if not sid:
            continue
        streams[sid] = _public_snap_stream(entry)
    stream_clients: dict[str, list[dict]] = {}
    for group in server.get("groups", []) or []:
        if not isinstance(group, dict):
            continue
        stream_id = group.get("stream_id")
        if not stream_id:
            continue
        stream_clients.setdefault(stream_id, [])
        for client in group.get("clients", []) or []:
            if not isinstance(client, dict):
                continue
            enriched = dict(client)
            enriched["_group_id"] = group.get("id")
            enriched["_group_name"] = group.get("name")
            stream_clients[stream_id].append(enriched)
    return streams, stream_clients


def _public_snap_client(client: dict, snapclient_nodes: dict[str, dict]) -> dict:
    cfg = client.get("config") or {}
    volume = cfg.get("volume") or {}
    host = client.get("host") or {}
    snap_info = client.get("snapclient") or {}
    client_id = client.get("id")
    linked_node = snapclient_nodes.get(client_id)
    volume_percent = volume.get("percent")
    try:
        volume_percent = _normalize_percent(volume_percent, default=75)
    except Exception:
        volume_percent = None
    payload = {
        "id": client_id,
        "connected": bool(client.get("connected")),
        "latency_ms": cfg.get("latency"),
        "volume_percent": volume_percent,
        "muted": bool(volume.get("muted")) if isinstance(volume.get("muted"), bool) else None,
        "host": {
            "name": host.get("name"),
            "ip": host.get("ip"),
            "os": host.get("os"),
        },
        "version": snap_info.get("version"),
        "protocol": snap_info.get("protocolVersion"),
        "group_id": client.get("_group_id"),
        "group_name": client.get("_group_name"),
        "configured_name": cfg.get("name"),
        "node_id": linked_node.get("id") if linked_node else None,
        "node_name": linked_node.get("name") if linked_node else None,
    }
    return payload


async def _collect_channel_listener_counts() -> tuple[dict[str, int], bool]:
    """Return (per-channel listeners, has_any_data)."""
    counts: dict[str, int] = {cid: 0 for cid in channel_order}
    stream_to_channel: dict[str, str] = {}
    for cid in channel_order:
        channel = channels_by_id.get(cid)
        if not channel:
            continue
        stream_id = (channel.get("snap_stream") or "").strip()
        if stream_id:
            stream_to_channel[stream_id] = cid
    data_sources = 0
    try:
        clients = await snapcast.list_clients()
    except Exception as exc:  # pragma: no cover - network dependency
        log.warning("Channel idle monitor: failed to list snapclients: %s", exc)
        clients = None
    if clients is not None:
        data_sources += 1
        for client in clients:
            stream_id = client.get("_stream_id")
            if not stream_id:
                continue
            cid = stream_to_channel.get(stream_id)
            if not cid or not client.get("connected"):
                continue
            counts[cid] = counts.get(cid, 0) + 1
    if webrtc_relay:
        try:
            webrtc_counts = await webrtc_relay.channel_listener_counts()
        except Exception as exc:  # pragma: no cover - defensive logging
            log.warning("Channel idle monitor: failed to read WebRTC listeners: %s", exc)
        else:
            data_sources += 1
            for cid, value in (webrtc_counts or {}).items():
                if value:
                    counts[cid] = counts.get(cid, 0) + int(value)
    return counts, data_sources > 0


def _is_rpc_method_not_found_error(exc: Exception) -> bool:
    if isinstance(exc, RuntimeError):
        payload = exc.args[0] if exc.args else None
        if isinstance(payload, dict):
            message = str(payload.get("message") or "").lower()
            code = payload.get("code")
            if code == -32601 or "method not found" in message:
                return True
        text = str(payload).lower() if payload is not None else ""
        if "method not found" in text:
            return True
    return "method not found" in str(exc).lower()

PUBLIC_API_PATHS = {
    "/api/auth/status",
    "/api/auth/login",
    "/api/auth/logout",
    "/api/auth/initialize",
    "/api/health",
    "/api/spotify/callback",
}

PUBLIC_API_PREFIXES = (
    "/api/sonos/stream/",
)


@app.middleware("http")
async def session_middleware(request: Request, call_next):
    path = request.url.path
    token = request.cookies.get(SESSION_COOKIE_NAME)
    request.state.user = _resolve_session_user(token)
    is_worker_endpoint = path.startswith(RADIO_WORKER_PATH_PREFIX) or path.startswith(ABS_WORKER_PATH_PREFIX)
    is_public_path = path in PUBLIC_API_PATHS or path.startswith(PUBLIC_API_PREFIXES)
    requires_auth = path.startswith("/api/") and (not is_worker_endpoint) and (not is_public_path)
    if requires_auth:
        initialized = _is_initialized()
        if not initialized and path != "/api/auth/initialize":
            return JSONResponse(status_code=403, content={"detail": "Instance setup required"})
        if initialized and request.state.user is None:
            return JSONResponse(status_code=401, content={"detail": "Authentication required"})
    response = await call_next(request)
    return response


@app.get("/sw.js", include_in_schema=False)
async def service_worker() -> FileResponse:
    """Serve the service worker from the app root for maximum scope."""
    response = FileResponse(STATIC_DIR / "sw.js", media_type="application/javascript")
    response.headers["Cache-Control"] = "no-cache"
    return response

nodes: Dict[str, dict] = {}
sections: list[dict] = []
browser_ws: Dict[str, WebSocket] = {}
node_watchers: set[WebSocket] = set()
webrtc_relay: Optional[WebAudioRelay] = None
DEFAULT_EQ_PRESET = "peq15"
pending_restarts: Dict[str, dict] = {}
agent_refresh_tasks: Dict[str, asyncio.Task] = {}
node_health_task: Optional[asyncio.Task] = None
spotify_refresh_task: Optional[asyncio.Task] = None
node_rediscovery_tasks: Dict[str, asyncio.Task] = {}
terminal_sessions: Dict[str, dict] = {}
auth_state: dict = {"server_name": SERVER_DEFAULT_NAME, "users": []}
users_by_id: Dict[str, dict] = {}
users_by_username: Dict[str, dict] = {}
radio_runtime_status: Dict[str, dict] = {}
radio_assignments_version = 1
radio_assignment_waiters: set[asyncio.Future] = set()
RADIO_ASSIGNMENT_DEFAULT_WAIT = float(os.getenv("RADIO_ASSIGNMENT_WAIT", "10"))
RADIO_ASSIGNMENT_DEFAULT_WAIT = max(1.0, min(RADIO_ASSIGNMENT_DEFAULT_WAIT, 30.0))
RADIO_ASSIGNMENT_MAX_WAIT = 30.0

abs_runtime_status: Dict[str, dict] = {}
abs_assignments_version = 1
abs_assignment_waiters: set[asyncio.Future] = set()
ABS_ASSIGNMENT_DEFAULT_WAIT = float(os.getenv("AUDIOBOOKSHELF_ASSIGNMENT_WAIT", "10"))
ABS_ASSIGNMENT_DEFAULT_WAIT = max(1.0, min(ABS_ASSIGNMENT_DEFAULT_WAIT, 30.0))
ABS_ASSIGNMENT_MAX_WAIT = 30.0
channel_idle_task: Optional[asyncio.Task] = None
channel_idle_state: Dict[str, dict] = {}
pending_web_node_requests: Dict[str, dict] = {}

sonos_connection_task: Optional[asyncio.Task] = None
sonos_reconcile_lock = asyncio.Lock()


def public_sections() -> list[dict]:
    return [_public_section(section) for section in sections]


def _find_section(section_id: Optional[str]) -> Optional[dict]:
    if not section_id:
        return None
    for section in sections:
        if section.get("id") == section_id:
            return section
    return None


def _ensure_default_section() -> Optional[str]:
    """Ensure at least one section exists; return its id or None."""
    global sections
    if sections:
        return sections[0].get("id")
    default_id = str(uuid.uuid4())
    sections = [{"id": default_id, "name": "Nodes", "created_at": int(time.time()), "updated_at": int(time.time())}]
    return default_id


def _mark_radio_assignments_dirty() -> None:
    global radio_assignments_version
    radio_assignments_version += 1
    waiters = list(radio_assignment_waiters)
    radio_assignment_waiters.clear()
    for waiter in waiters:
        if waiter.done():
            continue
        waiter.set_result(True)


def _mark_abs_assignments_dirty() -> None:
    global abs_assignments_version
    abs_assignments_version += 1
    waiters = list(abs_assignment_waiters)
    abs_assignment_waiters.clear()
    for waiter in waiters:
        if waiter.done():
            continue
        waiter.set_result(True)


async def _wait_for_radio_assignments_change(since: Optional[int], timeout: float = RADIO_ASSIGNMENT_DEFAULT_WAIT) -> int:
    current = radio_assignments_version
    if since is None or since != current:
        return current
    loop = asyncio.get_running_loop()
    future: asyncio.Future = loop.create_future()
    radio_assignment_waiters.add(future)
    # Re-check after registering in case a change happened before we awaited.
    current = radio_assignments_version
    if since != current:
        radio_assignment_waiters.discard(future)
        if not future.done():
            future.cancel()
        return current
    try:
        await asyncio.wait_for(future, timeout)
    except asyncio.TimeoutError:
        radio_assignment_waiters.discard(future)
    return radio_assignments_version


async def _wait_for_abs_assignments_change(since: Optional[int], timeout: float = ABS_ASSIGNMENT_DEFAULT_WAIT) -> int:
    current = abs_assignments_version
    if since is None or since != current:
        return current
    loop = asyncio.get_running_loop()
    future: asyncio.Future = loop.create_future()
    abs_assignment_waiters.add(future)
    current = abs_assignments_version
    if since != current:
        abs_assignment_waiters.discard(future)
        if not future.done():
            future.cancel()
        return current
    try:
        await asyncio.wait_for(future, timeout)
    except asyncio.TimeoutError:
        abs_assignment_waiters.discard(future)
    return abs_assignments_version


def default_eq_state() -> dict:
    return {"preset": DEFAULT_EQ_PRESET, "band_count": 15, "bands": []}


def _require_abs_worker_token(request: Request) -> None:
    if not ABS_WORKER_TOKEN:
        return
    header = request.headers.get("x-audiobookshelf-worker-token") or request.headers.get("authorization", "")
    if header.lower().startswith("bearer "):
        header = header[7:]
    candidate = header.strip()
    if not candidate or not secrets.compare_digest(candidate, ABS_WORKER_TOKEN):
        raise HTTPException(status_code=401, detail="Invalid audiobookshelf worker token")


def _radio_runtime_payload(channel_id: str) -> dict:
    status = radio_runtime_status.get(channel_id)
    if not status:
        return {"state": "idle", "message": None, "updated_at": None, "started_at": None}
    return status


def _abs_runtime_payload(channel_id: str) -> dict:
    status = abs_runtime_status.get(channel_id)
    if not status:
        return {"state": "idle", "message": None, "updated_at": None, "started_at": None}
    return status


def _select_initial_channel_id(preferred: Optional[str] = None, *, fallback: bool = True) -> Optional[str]:
    candidate = (preferred or "").strip().lower()
    if candidate in channels_by_id:
        return candidate
    if fallback:
        return _primary_channel_id()
    return None


def resolve_node_channel_id(node: dict, *, assign_default: bool = False) -> Optional[str]:
    channel_id = (node.get("channel_id") or "").strip().lower()
    if channel_id in channels_by_id:
        return channel_id
    if assign_default:
        fallback = _primary_channel_id()
        node["channel_id"] = fallback
        return fallback
    return None


def public_node(node: dict) -> dict:
    data = {k: v for k, v in node.items() if k not in SENSITIVE_NODE_FIELDS}
    data["paired"] = bool(node.get("agent_secret"))
    if node.get("type") in {"browser", "sonos"}:
        data["configured"] = True
    else:
        data["configured"] = bool(node.get("audio_configured"))
    data["agent_version"] = node.get("agent_version")
    data["latest_agent_version"] = AGENT_LATEST_VERSION or None
    data["volume_percent"] = int(node.get("volume_percent", 75))
    data["muted"] = bool(node.get("muted"))
    data["updating"] = bool(node.get("updating"))
    data["online"] = node.get("online", node.get("type") == "browser")
    data["last_seen"] = node.get("last_seen")
    data["offline_since"] = node.get("offline_since")
    if node.get("type") in {"browser", "sonos"}:
        data["update_available"] = False
    elif AGENT_LATEST_VERSION:
        data["update_available"] = (node.get("agent_version") or "") != AGENT_LATEST_VERSION
    else:
        data["update_available"] = True
    data["restarting"] = bool(pending_restarts.get(node["id"]))
    data["playback_device"] = node.get("playback_device")
    data["outputs"] = node.get("outputs") or {}
    data["fingerprint"] = node.get("fingerprint")
    data["max_volume_percent"] = _get_node_max_volume(node)
    data["channel_id"] = resolve_node_channel_id(node)
    data["stereo_mode"] = _normalize_stereo_mode(node.get("stereo_mode"))
    data["section_id"] = node.get("section_id")
    data["section_order"] = node.get("section_order")
    if node.get("wifi"):
        data["wifi"] = node.get("wifi")
    if node.get("type") == "sonos":
        data["connection_state"] = node.get("connection_state")
        data["connection_error"] = node.get("connection_error")
        data["sonos_connecting_since"] = node.get("sonos_connecting_since")
        data["sonos_network"] = node.get("sonos_network")
    data["is_controller"] = bool(node.get("is_controller"))
    return data


def public_nodes() -> list[dict]:
    section_rank = {section.get("id"): idx for idx, section in enumerate(sections) if section.get("id")}

    def _node_key(entry: dict) -> tuple:
        sid = entry.get("section_id")
        # Unsectioned nodes should appear before user-defined sections.
        if not sid:
            rank = -1
        else:
            rank = section_rank.get(sid, 10**9)
        order = entry.get("section_order")
        try:
            order_val = int(order)
        except (TypeError, ValueError):
            order_val = 10**9
        name_val = (entry.get("name") or "").lower()
        return (rank, order_val, name_val)

    ordered = sorted(nodes.values(), key=_node_key)
    return [public_node(node) for node in ordered]


def _normalize_percent(value, *, default: int) -> int:
    try:
        percent = int(value)
    except (TypeError, ValueError):
        return default
    return max(0, min(100, percent))


def _get_node_max_volume(node: dict) -> int:
    return _normalize_percent(node.get("max_volume_percent", 100), default=100)


def _apply_volume_limit(node: dict, requested_percent: int) -> int:
    requested = _normalize_percent(requested_percent, default=0)
    limit = _get_node_max_volume(node)
    return (requested * limit) // 100


def _load_auth_state() -> None:
    global auth_state, users_by_id, users_by_username
    if USERS_PATH.exists():
        try:
            data = json.loads(USERS_PATH.read_text())
        except Exception:
            log.exception("Failed to read users file; using defaults")
            data = {}
    else:
        data = {}
    server_name = (data.get("server_name") or SERVER_DEFAULT_NAME).strip() or SERVER_DEFAULT_NAME
    users = data.get("users") or []
    auth_state = {"server_name": server_name, "users": []}
    users_by_id = {}
    users_by_username = {}
    for entry in users:
        if not isinstance(entry, dict):
            continue
        uid = entry.get("id") or str(uuid.uuid4())
        username = (entry.get("username") or "").strip()
        role = entry.get("role") or "member"
        password_hash = entry.get("password_hash")
        if not username or not password_hash:
            continue
        user = {
            "id": uid,
            "username": username,
            "role": role,
            "password_hash": password_hash,
            "created_at": entry.get("created_at") or int(time.time()),
            "updated_at": entry.get("updated_at") or int(time.time()),
        }
        users_by_id[uid] = user
        users_by_username[username.lower()] = user
    auth_state["users"] = list(users_by_id.values())


def _save_auth_state() -> None:
    data = {
        "server_name": auth_state.get("server_name", SERVER_DEFAULT_NAME),
        "users": list(users_by_id.values()),
    }
    USERS_PATH.write_text(json.dumps(data, indent=2, sort_keys=True))


_load_auth_state()


def _is_initialized() -> bool:
    return bool(users_by_id)


def _hash_password(raw: str) -> str:
    value = raw.encode("utf-8")
    hashed = bcrypt.hashpw(value, bcrypt.gensalt())
    return hashed.decode("utf-8")


def _verify_password(raw: str, hashed: str) -> bool:
    if not raw or not hashed:
        return False
    try:
        return bcrypt.checkpw(raw.encode("utf-8"), hashed.encode("utf-8"))
    except ValueError:
        return False


def _create_user(username: str, password: str, role: str = "admin") -> dict:
    normalized = username.strip()
    if not normalized:
        raise HTTPException(status_code=400, detail="Username is required")
    lowered = normalized.lower()
    if lowered in users_by_username:
        raise HTTPException(status_code=409, detail="Username already exists")
    if not password or len(password) < 4:
        raise HTTPException(status_code=400, detail="Password must be at least 4 characters")
    role_normalized = role if role in {"admin", "member"} else "member"
    now = int(time.time())
    user = {
        "id": str(uuid.uuid4()),
        "username": normalized,
        "role": role_normalized,
        "password_hash": _hash_password(password),
        "created_at": now,
        "updated_at": now,
    }
    users_by_id[user["id"]] = user
    users_by_username[normalized.lower()] = user
    auth_state["users"] = list(users_by_id.values())
    _save_auth_state()
    return user


def _update_user(user_id: str, *, username: Optional[str] = None, password: Optional[str] = None, role: Optional[str] = None) -> dict:
    user = users_by_id.get(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    if username:
        normalized = username.strip()
        if not normalized:
            raise HTTPException(status_code=400, detail="Username must not be empty")
        lowered = normalized.lower()
        existing = users_by_username.get(lowered)
        if existing and existing["id"] != user_id:
            raise HTTPException(status_code=409, detail="Username already exists")
        users_by_username.pop(user["username"].lower(), None)
        user["username"] = normalized
        users_by_username[lowered] = user
    if password:
        if len(password) < 4:
            raise HTTPException(status_code=400, detail="Password must be at least 4 characters")
        user["password_hash"] = _hash_password(password)
    if role:
        if role not in {"admin", "member"}:
            raise HTTPException(status_code=400, detail="Invalid role")
        if user.get("role") == "admin" and role == "member":
            admins = [u for u in users_by_id.values() if u.get("role") == "admin" and u.get("id") != user_id]
            if not admins:
                raise HTTPException(status_code=400, detail="Cannot remove the last admin")
        user["role"] = role
    user["updated_at"] = int(time.time())
    auth_state["users"] = list(users_by_id.values())
    _save_auth_state()
    return user


def _delete_user(user_id: str) -> None:
    user = users_by_id.get(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    if user["role"] == "admin":
        admins = [u for u in users_by_id.values() if u.get("role") == "admin" and u.get("id") != user_id]
        if not admins:
            raise HTTPException(status_code=400, detail="Cannot remove the last admin")
    users_by_id.pop(user_id, None)
    users_by_username.pop(user["username"].lower(), None)
    auth_state["users"] = list(users_by_id.values())
    _save_auth_state()


def _public_user(user: dict) -> dict:
    return {
        "id": user.get("id"),
        "username": user.get("username"),
        "role": user.get("role"),
        "created_at": user.get("created_at"),
        "updated_at": user.get("updated_at"),
    }


def _session_payload(user_id: str) -> dict:
    return {"user_id": user_id, "ts": int(time.time())}


def _encode_session(user_id: str) -> str:
    payload = _session_payload(user_id)
    return SESSION_SIGNER.dumps(payload)


def _decode_session(token: str) -> Optional[dict]:
    if not token:
        return None
    try:
        data = SESSION_SIGNER.loads(token, max_age=SESSION_MAX_AGE)
    except BadSignature:
        return None
    return data


def _resolve_session_user(token: Optional[str]) -> Optional[dict]:
    data = _decode_session(token or "")
    if not data:
        return None
    user_id = data.get("user_id")
    if not user_id:
        return None
    return users_by_id.get(user_id)


def _set_session_cookie(response: Response, user_id: str) -> None:
    token = _encode_session(user_id)
    response.set_cookie(
        SESSION_COOKIE_NAME,
        token,
        max_age=SESSION_MAX_AGE,
        httponly=True,
        secure=SESSION_COOKIE_SECURE,
        samesite=SESSION_COOKIE_SAMESITE,
        path="/",
    )


def _clear_session_cookie(response: Response) -> None:
    response.delete_cookie(SESSION_COOKIE_NAME, path="/")


async def _require_ws_user(ws: WebSocket) -> Optional[dict]:
    token = ws.cookies.get(SESSION_COOKIE_NAME)
    user = _resolve_session_user(token)
    if not user:
        await ws.close(code=4401)
        return None
    return user


def get_current_user(request: Request) -> dict:
    user = getattr(request.state, "user", None)
    if not user:
        raise HTTPException(status_code=401, detail="Authentication required")
    return user


def require_admin(request: Request) -> dict:
    user = get_current_user(request)
    if user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    return user


def _auth_server_name() -> str:
    return auth_state.get("server_name", SERVER_DEFAULT_NAME)


def _auth_set_server_name(value: str) -> None:
    auth_state["server_name"] = (value or "").strip() or SERVER_DEFAULT_NAME
    _save_auth_state()


def _auth_get_user_by_username(lowered: str) -> Optional[dict]:
    return users_by_username.get((lowered or "").strip().lower())


def _auth_list_users() -> list[dict]:
    return list(users_by_id.values())


app.include_router(
    create_auth_router(
        is_initialized=_is_initialized,
        get_server_name=_auth_server_name,
        set_server_name=_auth_set_server_name,
        public_user=_public_user,
        get_user_by_username=_auth_get_user_by_username,
        create_user=_create_user,
        update_user=_update_user,
        delete_user=_delete_user,
        list_users=_auth_list_users,
        verify_password=_verify_password,
        set_session_cookie=_set_session_cookie,
        clear_session_cookie=_clear_session_cookie,
        require_admin=require_admin,
        server_default_name=SERVER_DEFAULT_NAME,
    )
)


def _providers() -> Dict[str, ProviderState]:
    return providers_by_id


app.include_router(
    create_providers_router(
        available_providers=AVAILABLE_PROVIDERS,
        get_provider_spec=get_provider_spec,
        providers=_providers,
        provider_state_cls=ProviderState,
        save_providers_state=save_providers_state,
        require_admin=require_admin,
        spotify_provider=spotify_provider,
        apply_spotify_provider=_apply_spotify_provider,
        disable_spotify_provider=_disable_spotify_provider,
        apply_radio_provider=_apply_radio_provider,
        disable_radio_provider=_disable_radio_provider,
        apply_audiobookshelf_provider=_apply_audiobookshelf_provider,
        disable_audiobookshelf_provider=_disable_audiobookshelf_provider,
    )
)


def _auth_state() -> dict:
    return auth_state


def _nodes() -> dict:
    return nodes


def _sections() -> list:
    return sections


def _set_sections(value: list) -> None:
    global sections
    sections = value


def _browser_ws() -> dict:
    return browser_ws


def _node_watchers() -> set:
    return node_watchers


def _terminal_sessions() -> Dict[str, dict]:
    return terminal_sessions


def _pending_web_node_requests() -> Dict[str, dict]:
    return pending_web_node_requests


def _webrtc_relay() -> Optional[WebAudioRelay]:
    return webrtc_relay


app.include_router(
    create_nodes_router(
        nodes=_nodes,
        sections=_sections,
        set_sections=_set_sections,
        save_nodes=lambda: save_nodes(),
        broadcast_nodes=lambda: broadcast_nodes(),
        public_node=lambda node: public_node(node),
        public_nodes=lambda: public_nodes(),
        public_sections=lambda: public_sections(),
        require_admin=require_admin,
        require_ws_user=_require_ws_user,
        auth_state=_auth_state,
        server_default_name=SERVER_DEFAULT_NAME,
        local_agent_name_suffix=LOCAL_AGENT_NAME_SUFFIX,
        local_agent_url=local_agent_url,
        ensure_local_agent_running=ensure_local_agent_running,
        stop_local_agent=stop_local_agent,
        controller_node=lambda: _controller_node(),
        refresh_agent_metadata=lambda node, persist=True: refresh_agent_metadata(node, persist=persist),
        register_node_payload=lambda reg, mark_controller=False: _register_node_payload(reg, mark_controller=mark_controller),
        call_agent=lambda node, path, payload: _call_agent(node, path, payload),
        sync_node_max_volume=lambda node, percent=None: _sync_node_max_volume(node, percent=percent),
        get_node_max_volume=lambda node: _get_node_max_volume(node),
        send_browser_volume=lambda node, percent: _send_browser_volume(node, percent),
        browser_ws=_browser_ws,
        node_watchers=_node_watchers,
        normalize_percent=lambda value, default=75: _normalize_percent(value, default=default),
        normalize_stereo_mode=lambda mode: _normalize_stereo_mode(mode),
        apply_volume_limit=lambda node, percent: _apply_volume_limit(node, percent),
        sonos_ip_from_url=lambda url: _sonos_ip_from_url(url),
        sonos_set_volume=lambda ip, percent: _sonos_set_volume(ip, percent),
        sonos_set_mute=lambda ip, muted: _sonos_set_mute(ip, muted),
        sonos_set_eq=lambda ip, eq_type, value: _sonos_set_eq(ip, eq_type=eq_type, value=value),
        normalize_sonos_eq=lambda value: _normalize_sonos_eq(value),
        request_agent_secret=lambda node, force=False, recovery_code=None: request_agent_secret(node, force=force, recovery_code=recovery_code),
        configure_agent_audio=lambda node: configure_agent_audio(node),
        set_node_channel=lambda node, channel_id: _set_node_channel(node, channel_id),
        schedule_agent_refresh=lambda *args, **kwargs: schedule_agent_refresh(*args, **kwargs),
        schedule_restart_watch=lambda node_id: schedule_restart_watch(node_id),
        node_restart_timeout=NODE_RESTART_TIMEOUT,
        resolve_terminal_target=lambda node: _resolve_terminal_target(node),
        cleanup_terminal_sessions=lambda: _cleanup_terminal_sessions(),
        terminal_sessions=_terminal_sessions,
        node_terminal_enabled=NODE_TERMINAL_ENABLED,
        node_terminal_token_ttl=NODE_TERMINAL_TOKEN_TTL,
        node_terminal_max_duration=NODE_TERMINAL_MAX_DURATION,
        node_terminal_strict_host_key=NODE_TERMINAL_STRICT_HOST_KEY,
        cancel_node_rediscovery=lambda node_id: cancel_node_rediscovery(node_id),
        teardown_browser_node=lambda *args, **kwargs: teardown_browser_node(*args, **kwargs),
        get_webrtc_relay=_webrtc_relay,
        normalize_section_name=_normalize_section_name,
        find_section=lambda section_id: _find_section(section_id),
        public_section=_public_section,
        webrtc_enabled=WEBRTC_ENABLED,
        get_web_node_snapshot=lambda entry: _get_web_node_snapshot(entry),
        broadcast_web_node_request_event=lambda event, **kwargs: _broadcast_web_node_request_event(event, **kwargs),
        pending_web_node_requests=_pending_web_node_requests,
        pending_web_node_snapshots=lambda: _pending_web_node_snapshots(),
        pop_pending_web_node_request=lambda request_id: _pop_pending_web_node_request(request_id),
        establish_web_node_session_for_request=lambda pending: _establish_web_node_session_for_request(pending),
        web_node_approval_timeout=WEB_NODE_APPROVAL_TIMEOUT,
        detect_discovery_networks=lambda: _detect_discovery_networks(),
        hosts_for_networks=lambda networks, limit=DISCOVERY_MAX_HOSTS: _hosts_for_networks(networks, limit=limit),
        stream_host_probes=lambda hosts: _stream_host_probes(hosts),
        sonos_ssdp_discover=lambda: _sonos_ssdp_discover(),
        discovery_max_hosts=DISCOVERY_MAX_HOSTS,
        sonos_discovery_timeout=SONOS_DISCOVERY_TIMEOUT,
    )
)


app.include_router(
    create_channels_router(
        get_channels_by_id=lambda: channels_by_id,
        get_channel_order=lambda: channel_order,
        get_nodes=lambda: nodes,
        get_sources_by_id=lambda: sources_by_id,
        load_token=lambda source_id: load_token(source_id),
        require_admin=require_admin,
        resolve_channel_id=lambda channel_id: resolve_channel_id(channel_id),
        channel_detail=lambda channel_id: channel_detail(channel_id),
        all_channel_details=lambda: all_channel_details(),
        update_channel_metadata=lambda channel_id, updates: update_channel_metadata(channel_id, updates),
        apply_channel_routing=lambda channel_id: _apply_channel_routing(channel_id),
        broadcast_nodes=lambda: broadcast_nodes(),
        set_node_channel=lambda node, channel_id: _set_node_channel(node, channel_id),
        normalize_channel_entry=lambda entry, fallback_order: _normalize_channel_entry(entry, fallback_order=fallback_order),
        resequence_channel_order=lambda: _resequence_channel_order(),
        primary_channel_id=lambda: _primary_channel_id(),
        save_channels=lambda: save_channels(),
        save_nodes=lambda: save_nodes(),
        channel_id_prefix=CHANNEL_ID_PREFIX,
    )
)


app.include_router(
    create_spotify_router(
        require_spotify_provider_dep=lambda: require_spotify_provider(),
        resolve_channel_id=lambda channel_id: resolve_channel_id(channel_id),
        resolve_spotify_source_id=lambda identifier: _resolve_spotify_source_id(identifier),
        read_spotify_config=lambda identifier: read_spotify_config(identifier),
        get_spotify_source=lambda source_id: get_spotify_source(source_id),
        spotify_redirect_uri=SPOTIFY_REDIRECT_URI,
        current_spotify_creds=lambda source_id: current_spotify_creds(source_id),
        token_signer=TOKEN_SIGNER,
        save_token=lambda token, source_id: save_token(token, source_id),
        ensure_spotify_token=lambda identifier=None: _ensure_spotify_token(identifier),
        load_token=lambda channel_id=None: load_token(channel_id),
        spotify_request=lambda *args, **kwargs: spotify_request(*args, **kwargs),
        find_roomcast_device=lambda token, channel_id=None: _find_roomcast_device(token, channel_id),
        preferred_roomcast_device_names=lambda channel_id=None: _preferred_roomcast_device_names(channel_id),
        get_player_snapshot=lambda channel_id=None: _get_player_snapshot(channel_id),
        public_player_snapshot=lambda snapshot: _public_player_snapshot(snapshot),
        set_player_snapshot=lambda channel_id, status: _set_player_snapshot(channel_id, status),
        map_spotify_track_simple=lambda item: _map_spotify_track_simple(item),
        map_spotify_search_bucket=lambda bucket, mapper: _map_spotify_search_bucket(bucket, mapper),
        map_spotify_album=lambda item: _map_spotify_album(item),
        map_spotify_artist=lambda item: _map_spotify_artist(item),
        map_spotify_playlist=lambda item: _map_spotify_playlist(item),
        map_spotify_track=lambda item, position=0: _map_spotify_track(item, position=position),
        spotify_search_types=SPOTIFY_SEARCH_TYPES,
        snapserver_host=SNAPSERVER_HOST,
        snapserver_port=SNAPSERVER_PORT,
        read_librespot_status=lambda source_id: read_librespot_status(source_id),
    )
)

app.include_router(
    create_radio_router(
        require_admin=require_admin,
        require_radio_provider_dep=require_radio_provider,
        require_radio_provider=require_radio_provider,
        resolve_channel_id=resolve_channel_id,
        get_radio_channel_or_404=_get_radio_channel_or_404,
        apply_radio_station=_apply_radio_station,
        save_channels=save_channels,
        mark_radio_assignments_dirty=_mark_radio_assignments_dirty,
        channel_detail=channel_detail,
        ensure_radio_state=_ensure_radio_state,
        radio_runtime_payload=_radio_runtime_payload,
        radio_max_slots_configured=radio_max_slots_configured,
        radio_max_slots_supported=radio_max_slots_supported,
        radio_channel_slots_active=radio_channel_slots_active,
        get_providers_by_id=lambda: providers_by_id,
        save_providers_state=save_providers_state,
        get_channels_by_id=lambda: channels_by_id,
        get_channel_order=lambda: channel_order,
        get_radio_runtime_status=lambda: radio_runtime_status,
        get_radio_assignments_version=lambda: radio_assignments_version,
        wait_for_radio_assignments_change=_wait_for_radio_assignments_change,
        radio_assignment_default_wait=RADIO_ASSIGNMENT_DEFAULT_WAIT,
        radio_assignment_max_wait=RADIO_ASSIGNMENT_MAX_WAIT,
        radio_worker_token=RADIO_WORKER_TOKEN,
        radio_browser_base_url=RADIO_BROWSER_BASE_URL,
        radio_browser_timeout=RADIO_BROWSER_TIMEOUT,
        radio_browser_cache_ttl=RADIO_BROWSER_CACHE_TTL,
        radio_browser_user_agent=RADIO_BROWSER_USER_AGENT,
    )
)

app.include_router(
    create_snapcast_router(
        snapcast_client=snapcast,
        require_admin=require_admin,
    )
)

app.include_router(
    create_sonos_router(
        get_current_user=get_current_user,
        resolve_channel_id=resolve_channel_id,
        get_channels_by_id=lambda: channels_by_id,
        get_nodes=_nodes,
        resolve_node_channel_id=resolve_node_channel_id,
        sonos_attempt_reconnect=lambda channel_id, members: _sonos_attempt_reconnect(channel_id, members),
        sonos_discover=lambda: _sonos_discover(),
        sonos_client_allows_stream=lambda resolved_channel_id, client_ip: _sonos_client_allows_stream(
            resolved_channel_id=resolved_channel_id,
            client_ip=client_ip,
        ),
        sonos_mark_stream_activity=lambda channel_id, client_ip, kind: _sonos_mark_stream_activity(
            channel_id=channel_id,
            client_ip=client_ip,
            kind=kind,
        ),
        sonos_mark_stream_end=lambda client_ip: _sonos_mark_stream_end(client_ip=client_ip),
        sonos_find_node_by_ip=lambda ip: _sonos_find_node_by_ip(ip),
        normalize_stereo_mode=lambda mode: _normalize_stereo_mode(mode),
        ffmpeg_pan_filter_for_stereo_mode=lambda mode: _ffmpeg_pan_filter_for_stereo_mode(mode),
        snapcast_client=snapcast,
        snapserver_agent_host=SNAPSERVER_AGENT_HOST,
        snapclient_port=SNAPCLIENT_PORT,
        webrtc_latency_ms=WEBRTC_LATENCY_MS,
        webrtc_sample_rate=WEBRTC_SAMPLE_RATE,
        sonos_stream_bitrate_kbps=SONOS_STREAM_BITRATE_KBPS,
        server_default_name=SERVER_DEFAULT_NAME,
    )
)

def _extract_node_host(node: dict) -> Optional[str]:
    override = (node.get("ssh_host") or "").strip()
    if override:
        return override
    url = node.get("url")
    if not url:
        return None
    try:
        parsed = urlparse(url)
    except ValueError:
        return None
    return parsed.hostname


def _resolve_terminal_target(node: dict) -> Optional[dict]:
    host = _extract_node_host(node)
    if not host:
        return None
    port_raw = node.get("ssh_port") or NODE_TERMINAL_SSH_PORT
    try:
        port = int(port_raw)
    except (TypeError, ValueError):
        port = NODE_TERMINAL_SSH_PORT
    user = (node.get("ssh_user") or NODE_TERMINAL_SSH_USER).strip()
    password = (node.get("ssh_password") or NODE_TERMINAL_SSH_PASSWORD).strip()
    key_path = (node.get("ssh_key_path") or NODE_TERMINAL_SSH_KEY_PATH).strip()
    if not user:
        return None
    if not password and not key_path:
        return None
    return {
        "host": host,
        "port": port if port and port > 0 else 22,
        "user": user,
        "password": password,
        "key_path": key_path,
    }


def _cleanup_terminal_sessions() -> None:
    if not terminal_sessions:
        return
    now = time.time()
    expired = [token for token, session in terminal_sessions.items() if session.get("expires_at", 0) <= now]
    for token in expired:
        terminal_sessions.pop(token, None)


async def _send_browser_volume(node: dict, requested_percent: int) -> None:
    ws = browser_ws.get(node.get("id"))
    if not ws:
        return
    effective = _apply_volume_limit(node, requested_percent)
    await ws.send_json({"type": "volume", "percent": effective})


async def _sync_node_max_volume(node: dict, *, percent: Optional[int] = None) -> None:
    if node.get("type") != "agent":
        return
    value = _get_node_max_volume(node) if percent is None else _normalize_percent(percent, default=_get_node_max_volume(node))
    payload = {"percent": value}
    await _call_agent(node, "/config/max-volume", payload)


def _resolve_spotify_source_id(identifier: Optional[str]) -> Optional[str]:
    # Accept either a spotify source id (spotify:a) or a channel id.
    sid = _normalize_spotify_source_id(identifier)
    if sid:
        return sid if sid in sources_by_id else None
    return resolve_channel_spotify_source_id(identifier)


def _ensure_spotify_token(identifier: Optional[str] = None) -> dict:
    token = load_token(identifier)
    if not token or not token.get("access_token"):
        raise HTTPException(status_code=401, detail="Spotify not authorized")
    return token


def _preferred_roomcast_device_names(channel_id: Optional[str] = None) -> List[str]:
    cfg = read_spotify_config(channel_id)
    candidates = [
        (cfg.get("device_name") or "").strip(),
        LIBRESPOT_FALLBACK_NAME.strip(),
        "RoomCast",
    ]
    seen = set()
    preferred: List[str] = []
    for name in candidates:
        if not name:
            continue
        lowered = name.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        preferred.append(name)
    return preferred


async def _find_roomcast_device(token: dict, channel_id: Optional[str] = None) -> Optional[dict]:
    resp = await spotify_request("GET", "/me/player/devices", token, channel_id)
    if resp.status_code >= 400:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    data = resp.json()
    devices = data.get("devices") or []
    preferred_names = [name.lower() for name in _preferred_roomcast_device_names(channel_id)]
    for target in preferred_names:
        for device in devices:
            if not isinstance(device, dict):
                continue
            name = (device.get("name") or "").strip().lower()
            if name == target and device.get("id"):
                return device
    return None


def _map_spotify_image(images: Optional[list]) -> Optional[dict]:
    if not images:
        return None
    for image in images:
        if isinstance(image, dict) and image.get("url"):
            return {"url": image.get("url"), "width": image.get("width"), "height": image.get("height")}
    return None


def _map_spotify_playlist(item: dict) -> dict:
    images = item.get("images") if isinstance(item, dict) else None
    cover = _map_spotify_image(images if isinstance(images, list) else None)
    tracks = item.get("tracks") if isinstance(item, dict) else None
    owner = item.get("owner") if isinstance(item, dict) else None
    return {
        "id": item.get("id"),
        "name": item.get("name"),
        "description": item.get("description") or "",
        "uri": item.get("uri"),
        "tracks_total": tracks.get("total") if isinstance(tracks, dict) else None,
        "owner": owner.get("display_name") if isinstance(owner, dict) else None,
        "image": cover,
    }


def _map_spotify_track_core(track: Optional[dict], *, position: Optional[int] = None) -> Optional[dict]:
    if not isinstance(track, dict):
        return None
    if track.get("is_local") is True:
        return None
    artists = track.get("artists") or []
    artist_names = ", ".join(a.get("name") for a in artists if isinstance(a, dict) and a.get("name"))
    album = track.get("album") if isinstance(track.get("album"), dict) else track.get("album")
    album_name = album.get("name") if isinstance(album, dict) else None
    cover = None
    if isinstance(album, dict):
        cover = _map_spotify_image(album.get("images"))
    mapped = {
        "id": track.get("id"),
        "name": track.get("name") or "Untitled",
        "uri": track.get("uri"),
        "duration_ms": track.get("duration_ms"),
        "explicit": bool(track.get("explicit")),
        "artists": artist_names,
        "album": album_name,
        "image": cover,
    }
    if position is not None:
        mapped["position"] = position
    return mapped


def _map_spotify_track(item: dict, position: int) -> Optional[dict]:
    track = item.get("track") if isinstance(item, dict) else None
    return _map_spotify_track_core(track, position=position)


def _map_spotify_track_simple(track: dict) -> Optional[dict]:
    return _map_spotify_track_core(track)


def _map_spotify_album(item: dict) -> Optional[dict]:
    if not isinstance(item, dict):
        return None
    artists = item.get("artists") or []
    artist_names = ", ".join(a.get("name") for a in artists if isinstance(a, dict) and a.get("name"))
    cover = _map_spotify_image(item.get("images"))
    return {
        "id": item.get("id"),
        "name": item.get("name"),
        "uri": item.get("uri"),
        "artists": artist_names,
        "total_tracks": item.get("total_tracks"),
        "release_date": item.get("release_date"),
        "image": cover,
    }


def _map_spotify_artist(item: dict) -> Optional[dict]:
    if not isinstance(item, dict):
        return None
    cover = _map_spotify_image(item.get("images"))
    genres = item.get("genres") or []
    top_genres = ", ".join(g for g in genres[:3] if g)
    followers = item.get("followers") if isinstance(item.get("followers"), dict) else None
    follower_total = followers.get("total") if isinstance(followers, dict) else None
    return {
        "id": item.get("id"),
        "name": item.get("name"),
        "uri": item.get("uri"),
        "genres": top_genres,
        "followers": follower_total,
        "image": cover,
    }


def _map_spotify_search_bucket(payload: Optional[dict], mapper: Callable[[dict], Optional[dict]]) -> dict:
    items: List[dict] = []
    limit = None
    offset = None
    total = None
    has_next = False
    source_items = []
    if isinstance(payload, dict):
        limit = payload.get("limit")
        offset = payload.get("offset")
        total = payload.get("total")
        has_next = bool(payload.get("next"))
        raw_items = payload.get("items")
        if isinstance(raw_items, list):
            source_items = raw_items
    for entry in source_items or []:
        if not isinstance(entry, dict):
            continue
        mapped = mapper(entry)
        if mapped:
            items.append(mapped)
    resolved_total = total if isinstance(total, int) else len(items)
    return {
        "items": items,
        "total": resolved_total,
        "limit": limit if isinstance(limit, int) else None,
        "offset": offset if isinstance(offset, int) else None,
        "next": has_next,
    }


def _get_web_node_snapshot(entry: dict) -> dict:
    snapshot = entry.get("snapshot")
    if snapshot:
        return snapshot
    snapshot = {
        "id": entry["id"],
        "name": entry.get("name"),
        "client_host": entry.get("client_host"),
        "requested_at": entry.get("created_at"),
    }
    entry["snapshot"] = snapshot
    return snapshot


def _pending_web_node_snapshots() -> list[dict]:
    entries = sorted(pending_web_node_requests.values(), key=lambda item: item.get("created_at", 0))
    return [_get_web_node_snapshot(entry) for entry in entries]


async def _broadcast_to_node_watchers(payload: dict) -> None:
    if not node_watchers:
        return
    dead: list[WebSocket] = []
    for ws in list(node_watchers):
        try:
            await ws.send_json(payload)
        except Exception:
            dead.append(ws)
    for ws in dead:
        node_watchers.discard(ws)


async def broadcast_nodes() -> None:
    await _broadcast_to_node_watchers({
        "type": "nodes",
        "sections": public_sections(),
        "nodes": public_nodes(),
    })


async def _broadcast_web_node_request_event(
    action: str,
    *,
    snapshot: Optional[dict] = None,
    request_id: Optional[str] = None,
    status: Optional[str] = None,
    reason: Optional[str] = None,
) -> None:
    payload: dict[str, Any] = {"type": "web_node_request", "action": action}
    if snapshot:
        payload["request"] = snapshot
    if request_id:
        payload["request_id"] = request_id
    if status:
        payload["status"] = status
    if reason:
        payload["reason"] = reason
    await _broadcast_to_node_watchers(payload)


def _pop_pending_web_node_request(request_id: str) -> dict:
    pending = pending_web_node_requests.pop(request_id, None)
    if not pending:
        raise HTTPException(status_code=404, detail="Web node request not found or already resolved")
    return pending


async def _establish_web_node_session_for_request(pending: dict) -> dict:
    if not WEBRTC_ENABLED or not webrtc_relay:
        raise HTTPException(status_code=503, detail="Web nodes are disabled")
    name = pending.get("name") or "Web node"
    node = create_browser_node(name)
    channel_id = resolve_node_channel_id(node)
    channel = channels_by_id.get(channel_id) if channel_id else None
    stream_id = channel.get("snap_stream") if channel else None
    if channel_id and (not channel or not stream_id):
        await teardown_browser_node(node["id"])
        raise HTTPException(status_code=500, detail="Channel is missing snap_stream mapping")
    try:
        session = await webrtc_relay.create_session(
            node["id"],
            channel_id,
            stream_id,
            stereo_mode=_normalize_stereo_mode(node.get("stereo_mode")),
        )
        answer = await session.accept(pending["offer_sdp"], pending["offer_type"])
    except Exception as exc:  # pragma: no cover - defensive logging
        log.exception("Failed to establish web node session")
        await teardown_browser_node(node["id"])
        raise HTTPException(status_code=500, detail=f"Failed to start WebRTC session: {exc}")
    await broadcast_nodes()
    return {"node": node, "answer": answer.sdp, "answer_type": answer.type}


def _normalize_node_url(raw: str) -> str:
    value = (raw or "").strip()
    if not value:
        return value
    if value.startswith("browser:"):
        return value.rstrip("/")
    if value.startswith("sonos://"):
        return value.rstrip("/")
    if "://" not in value:
        value = f"http://{value}"
    return value.rstrip("/")


def _sonos_ip_from_url(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    if url.startswith("sonos://"):
        return url[len("sonos://"):].strip("/") or None
    return None


def _normalize_stereo_mode(value: object) -> str:
    mode = (str(value) if value is not None else "both").strip().lower()
    return mode if mode in {"both", "left", "right"} else "both"


def _ffmpeg_pan_filter_for_stereo_mode(mode: str) -> Optional[str]:
    normalized = _normalize_stereo_mode(mode)
    if normalized == "left":
        return "pan=stereo|c0=c0|c1=c0"
    if normalized == "right":
        return "pan=stereo|c0=c1|c1=c1"
    return None


def _sonos_stream_url(channel_id: str) -> str:
    return f"{_roomcast_public_base_url()}/api/sonos/stream/{channel_id}"


def _roomcast_public_base_url() -> str:
    host = (ROOMCAST_PUBLIC_HOST or "").strip() or "127.0.0.1"
    # Sonos must be able to reach the controller over the LAN; never hand out loopback.
    if host in {"127.0.0.1", "localhost"}:
        detected = _detect_primary_ipv4_host()
        if detected:
            host = detected
    return f"http://{host}:{ROOMCAST_PUBLIC_PORT}"


def _sonos_stream_uri(channel_id: str) -> str:
    """Return a Sonos-compatible URI for an HTTP live MP3 stream."""

    # Sonos expects the mp3radio scheme wrapper for many live streams.
    http_url = _sonos_stream_url(channel_id)
    parsed = urlparse(http_url)
    # Sonos typically expects: x-rincon-mp3radio://host:port/path (no nested http://)
    suffix = f"{parsed.netloc}{parsed.path}" if parsed.netloc else http_url
    if parsed.query:
        suffix = f"{suffix}?{parsed.query}"
    return f"x-rincon-mp3radio://{suffix}"


def _sonos_stream_metadata(channel_id: str) -> str:
    """Return DIDL-Lite metadata for the Sonos mp3radio stream."""

    resolved = resolve_channel_id(channel_id)
    channel = channels_by_id.get(resolved) if resolved else None
    channel_name = (channel.get("name") if isinstance(channel, dict) else None) or channel_id
    title = f"RoomCast - {channel_name}"
    http_url = _sonos_stream_url(channel_id)

    host = (ROOMCAST_PUBLIC_HOST or "").strip() or "127.0.0.1"
    if host in {"127.0.0.1", "localhost"}:
        detected = _detect_primary_ipv4_host()
        if detected:
            host = detected
    artwork_url = f"http://{host}:{ROOMCAST_PUBLIC_PORT}/static/icons/icon-192.png"
    image_url = f"{_roomcast_public_base_url()}/static/icons/icon-512.png"

    # This XML is embedded as text inside SOAP, so it must be escaped when inserted.
    # We generate the inner XML here and let _sonos_soap_action escape it.
    return (
        '<DIDL-Lite xmlns:dc="http://purl.org/dc/elements/1.1/" '
        'xmlns:upnp="urn:schemas-upnp-org:metadata-1-0/upnp/" '
        'xmlns="urn:schemas-upnp-org:metadata-1-0/DIDL-Lite/" '
        'xmlns:r="urn:schemas-rinconnetworks-com:metadata-1-0/" '
        'xmlns:dlna="urn:schemas-dlna-org:metadata-1-0/">'
        '<item id="flowmode" parentID="0" restricted="1">'
        f"<dc:title>{xml_escape(title)}</dc:title>"
        f"<upnp:albumArtURI>{xml_escape(image_url)}</upnp:albumArtURI>"
        "<dc:description>RoomCast</dc:description>"
        '<upnp:class>object.item.audioItem.audioBroadcast</upnp:class>'
        f"<res protocolInfo=\"http-get:*:audio/mpeg:DLNA.ORG_OP=01;DLNA.ORG_CI=0;DLNA.ORG_FLAGS=01700000000000000000000000000000\">{xml_escape(http_url)}</res>"
        '<desc id="cdudn" nameSpace="urn:schemas-rinconnetworks-com:metadata-1-0/">'
        'RINCON_AssociatedZPUDN'
        '</desc>'
        '</item>'
        '</DIDL-Lite>'
    )


def _sonos_extract_rincon(udn: str | None) -> Optional[str]:
    if not udn:
        return None
    raw = udn.strip()
    if raw.lower().startswith("uuid:"):
        raw = raw[5:]
    return raw if raw.startswith("RINCON_") else None


async def _sonos_fetch_description(ip: str, *, timeout: float = SONOS_CONTROL_TIMEOUT) -> Optional[dict]:
    url = f"http://{ip}:1400/xml/device_description.xml"
    headers = {"User-Agent": SONOS_HTTP_USER_AGENT}
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.get(url, headers=headers)
        if resp.status_code != 200:
            return None
        root = ElementTree.fromstring(resp.text)
    except Exception:
        return None

    def _find_text(path: str) -> Optional[str]:
        el = root.find(path)
        if el is None or el.text is None:
            return None
        value = el.text.strip()
        return value or None

    # UPnP device description uses namespaces sometimes; handle both.
    friendly = _find_text(".//friendlyName") or _find_text(".//{*}friendlyName")
    udn = _find_text(".//UDN") or _find_text(".//{*}UDN")
    dtype = _find_text(".//deviceType") or _find_text(".//{*}deviceType")
    if dtype and dtype.strip() != SONOS_DEVICE_TYPE:
        return None
    rincon = _sonos_extract_rincon(udn)
    return {
        "friendly_name": friendly,
        "udn": udn,
        "rincon": rincon,
        "device_type": dtype,
        "description_url": url,
    }


async def _sonos_soap_action(
    ip: str,
    *,
    service: str,
    action: str,
    control_path: str,
    arguments: dict[str, str],
    timeout: Optional[float] = None,
) -> None:
    await _sonos_soap_action_text(
        ip,
        service=service,
        action=action,
        control_path=control_path,
        arguments=arguments,
        timeout=timeout,
    )
    return

async def _sonos_soap_action_text(
    ip: str,
    *,
    service: str,
    action: str,
    control_path: str,
    arguments: dict[str, str],
    timeout: Optional[float] = None,
) -> str:
    """Execute a Sonos SOAP action and return the response body."""

    target = f"http://{ip}:1400{control_path}"
    ns = f"urn:schemas-upnp-org:service:{service}:1"
    body_parts = [f"<{k}>{xml_escape(v)}</{k}>" for k, v in arguments.items()]
    envelope = (
        "<?xml version=\"1.0\" encoding=\"utf-8\"?>"
        "<s:Envelope xmlns:s=\"http://schemas.xmlsoap.org/soap/envelope/\" "
        "s:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">"
        "<s:Body>"
        f"<u:{action} xmlns:u=\"{ns}\">"
        + "".join(body_parts)
        + f"</u:{action}>"
        "</s:Body>"
        "</s:Envelope>"
    )
    headers = {
        "Content-Type": "text/xml; charset=\"utf-8\"",
        "SOAPACTION": f'\"{ns}#{action}\"',
        "User-Agent": SONOS_HTTP_USER_AGENT,
        "Connection": "close",
    }
    effective_timeout = SONOS_CONTROL_TIMEOUT if timeout is None else float(timeout)
    async with httpx.AsyncClient(timeout=effective_timeout) as client:
        resp = await client.post(target, content=envelope.encode("utf-8"), headers=headers)

    def _parse_upnp_fault(xml_text: str) -> Optional[str]:
        try:
            root = ElementTree.fromstring(xml_text)
        except Exception:
            return None
        fault = root.find(".//{*}Fault")
        if fault is None:
            return None
        error_code = root.findtext(".//{*}errorCode")
        error_desc = root.findtext(".//{*}errorDescription")
        if error_code or error_desc:
            code = (error_code or "").strip()
            desc = (error_desc or "").strip()
            if code and desc:
                return f"UPnPError {code}: {desc}"
            return f"UPnPError {code or desc}".strip()
        fault_string = root.findtext(".//{*}faultstring")
        if fault_string:
            return fault_string.strip()
        return "UPnPError (unknown SOAP fault)"

    fault_msg = _parse_upnp_fault(resp.text or "")
    if resp.status_code >= 400 or fault_msg:
        detail = fault_msg or (resp.text or "").strip() or f"HTTP {resp.status_code}"
        raise HTTPException(status_code=502, detail=f"Sonos SOAP {service}.{action} failed: {detail}")
    return resp.text


async def _sonos_get_transport_state(ip: str) -> Optional[str]:
    xml_text = await _sonos_soap_action_text(
        ip,
        service="AVTransport",
        action="GetTransportInfo",
        control_path="/MediaRenderer/AVTransport/Control",
        arguments={"InstanceID": "0"},
    )
    try:
        root = ElementTree.fromstring(xml_text)
    except Exception as exc:
        log.warning("Sonos GetTransportInfo parse failed (ip=%s): %s", ip, exc)
        return None
    state = root.findtext(".//{*}CurrentTransportState")
    return state.strip() if state else None


async def _sonos_get_current_uri(ip: str) -> Optional[str]:
    xml_text = await _sonos_soap_action_text(
        ip,
        service="AVTransport",
        action="GetMediaInfo",
        control_path="/MediaRenderer/AVTransport/Control",
        arguments={"InstanceID": "0"},
    )
    try:
        root = ElementTree.fromstring(xml_text)
    except Exception as exc:
        log.warning("Sonos GetMediaInfo parse failed (ip=%s): %s", ip, exc)
        return None
    uri = root.findtext(".//{*}CurrentURI")
    return uri.strip() if uri else None


async def _sonos_get_zone_name(ip: str) -> Optional[str]:
    """Return the Sonos app 'room' / zone name for this device."""

    try:
        xml_text = await _sonos_soap_action_text(
            ip,
            service="DeviceProperties",
            action="GetZoneAttributes",
            control_path="/DeviceProperties/Control",
            arguments={},
        )
    except Exception as exc:
        log.warning("Sonos GetZoneAttributes failed (ip=%s): %s", ip, exc)
        return None

    try:
        root = ElementTree.fromstring(xml_text)
    except Exception as exc:
        log.warning("Sonos GetZoneAttributes parse failed (ip=%s): %s", ip, exc)
        return None
    name = root.findtext(".//{*}CurrentZoneName")
    if not name:
        return None
    resolved = name.strip()
    return resolved or None


async def _sonos_set_volume(ip: str, percent: int) -> None:
    await _sonos_soap_action(
        ip,
        service="RenderingControl",
        action="SetVolume",
        control_path="/MediaRenderer/RenderingControl/Control",
        arguments={
            "InstanceID": "0",
            "Channel": "Master",
            "DesiredVolume": str(max(0, min(100, int(percent)))),
        },
    )


async def _sonos_set_mute(ip: str, muted: bool) -> None:
    await _sonos_soap_action(
        ip,
        service="RenderingControl",
        action="SetMute",
        control_path="/MediaRenderer/RenderingControl/Control",
        arguments={
            "InstanceID": "0",
            "Channel": "Master",
            "DesiredMute": "1" if muted else "0",
        },
    )


def _normalize_sonos_eq(value: object) -> dict:
    data = value if isinstance(value, dict) else {}
    bass = data.get("bass")
    treble = data.get("treble")
    loudness = data.get("loudness")
    try:
        bass_i = int(bass)
    except (TypeError, ValueError):
        bass_i = 0
    try:
        treble_i = int(treble)
    except (TypeError, ValueError):
        treble_i = 0
    return {
        "bass": max(-10, min(10, bass_i)),
        "treble": max(-10, min(10, treble_i)),
        "loudness": bool(loudness) if loudness is not None else False,
    }


async def _sonos_set_eq(ip: str, *, eq_type: str, value: int) -> None:
    await _sonos_soap_action(
        ip,
        service="RenderingControl",
        action="SetEQ",
        control_path="/MediaRenderer/RenderingControl/Control",
        arguments={
            "InstanceID": "0",
            "EQType": str(eq_type),
            "DesiredValue": str(int(value)),
        },
    )


async def _sonos_become_standalone(ip: str) -> None:
    await _sonos_soap_action(
        ip,
        service="AVTransport",
        action="BecomeCoordinatorOfStandaloneGroup",
        control_path="/MediaRenderer/AVTransport/Control",
        arguments={"InstanceID": "0"},
    )


async def _sonos_join(ip: str, coordinator_rincon: str) -> None:
    uri = f"x-rincon:{coordinator_rincon}"
    await _sonos_soap_action(
        ip,
        service="AVTransport",
        action="SetAVTransportURI",
        control_path="/MediaRenderer/AVTransport/Control",
        arguments={
            "InstanceID": "0",
            "CurrentURI": uri,
            "CurrentURIMetaData": "",
        },
    )


async def _sonos_set_uri_and_play(ip: str, uri: str, metadata: str = "") -> None:
    # Some Sonos devices can be slow to respond during regrouping or when swapping streams.
    # Apply a slightly longer timeout and a short retry on timeouts.
    timeout_s = max(SONOS_CONTROL_TIMEOUT, 12.0)
    retries = 2
    for attempt in range(1, retries + 1):
        try:
            await _sonos_soap_action(
                ip,
                service="AVTransport",
                action="SetAVTransportURI",
                control_path="/MediaRenderer/AVTransport/Control",
                arguments={
                    "InstanceID": "0",
                    "CurrentURI": uri,
                    "CurrentURIMetaData": metadata or "",
                },
                timeout=timeout_s,
            )
            break
        except httpx.TimeoutException as exc:
            if attempt >= retries:
                raise HTTPException(status_code=502, detail=f"Sonos SetAVTransportURI timed out: {exc}") from exc
            await asyncio.sleep(0.25 * attempt)
        except httpx.RequestError as exc:
            if attempt >= retries:
                raise HTTPException(
                    status_code=502,
                    detail=f"Sonos SetAVTransportURI failed: {exc}",
                ) from exc
            await asyncio.sleep(0.25 * attempt)

    for attempt in range(1, retries + 1):
        try:
            await _sonos_soap_action(
                ip,
                service="AVTransport",
                action="Play",
                control_path="/MediaRenderer/AVTransport/Control",
                arguments={
                    "InstanceID": "0",
                    "Speed": "1",
                },
                timeout=timeout_s,
            )
            return
        except httpx.TimeoutException as exc:
            if attempt >= retries:
                raise HTTPException(status_code=502, detail=f"Sonos Play timed out: {exc}") from exc
            await asyncio.sleep(0.25 * attempt)
        except httpx.RequestError as exc:
            if attempt >= retries:
                raise HTTPException(
                    status_code=502,
                    detail=f"Sonos Play failed: {exc}",
                ) from exc
            await asyncio.sleep(0.25 * attempt)


async def _sonos_set_uri_and_play_with_fallback(channel_id: str, coordinator_ip: str) -> None:
    """Start playback on a coordinator, retrying with alternate URI formats.

    Some Sonos firmwares accept `x-rincon-mp3radio://http://...`, others are picky.
    We try mp3radio first, and if the transport state doesn't become PLAYING,
    we retry with a plain HTTP URI.
    """

    mp3radio_uri = _sonos_stream_uri(channel_id)
    http_uri = _sonos_stream_url(channel_id)
    # Metadata handling matters: mp3radio commonly works best with empty metadata,
    # while plain HTTP benefits from DIDL-Lite.
    metadata_mp3radio = ""
    metadata_http = _sonos_stream_metadata(channel_id)

    log.info(
        "Sonos start stream (ip=%s, channel=%s, mp3radio_uri=%s, http_uri=%s)",
        coordinator_ip,
        channel_id,
        mp3radio_uri,
        http_uri,
    )

    await _sonos_set_uri_and_play(coordinator_ip, mp3radio_uri, metadata_mp3radio)
    state = None
    for _ in range(3):
        await asyncio.sleep(0.5)
        try:
            state = await _sonos_get_transport_state(coordinator_ip)
        except Exception:
            state = None
        if state and state.upper() == "PLAYING":
            return

    log.warning(
        "Sonos coordinator transport not PLAYING after mp3radio start (ip=%s, state=%s); retrying with plain HTTP URI",
        coordinator_ip,
        state,
    )
    await _sonos_set_uri_and_play(coordinator_ip, http_uri, metadata_http)
    state2 = None
    for _ in range(3):
        await asyncio.sleep(0.5)
        try:
            state2 = await _sonos_get_transport_state(coordinator_ip)
        except Exception:
            state2 = None
        if state2 and state2.upper() == "PLAYING":
            return
    try:
        current_uri = await _sonos_get_current_uri(coordinator_ip)
    except Exception:
        current_uri = None
    log.warning(
        "Sonos coordinator still not PLAYING after HTTP retry (ip=%s, state=%s, current_uri=%s)",
        coordinator_ip,
        state2,
        current_uri,
    )


async def _sonos_stop(ip: str) -> None:
    await _sonos_soap_action(
        ip,
        service="AVTransport",
        action="Stop",
        control_path="/MediaRenderer/AVTransport/Control",
        arguments={"InstanceID": "0"},
    )


async def _sonos_ping(ip: str) -> bool:
    headers = {"User-Agent": SONOS_HTTP_USER_AGENT}
    try:
        async with httpx.AsyncClient(timeout=SONOS_CONTROL_TIMEOUT) as client:
            resp = await client.get(f"http://{ip}:1400/xml/device_description.xml", headers=headers)
        return resp.status_code == 200
    except Exception:
        return False


def _parse_sonos_network_from_review(text: str) -> Optional[dict]:
    """Best-effort parse of Sonos /support/review for network type.

    Sonos does not reliably expose RSSI via supported UPnP APIs.
    Some firmwares expose useful (but undocumented) diagnostics via
    /support/review; we only use it to determine the transport type.
    """

    if not text:
        return None

    normalized = text.strip()
    if not normalized:
        return None

    link_match = re.search(r"<Link>(\d+)</Link>", normalized)
    speed_match = re.search(r"<Speed>(\d+)</Speed>", normalized)
    try:
        link = int(link_match.group(1)) if link_match else 0
    except (TypeError, ValueError):
        link = 0
    try:
        speed = int(speed_match.group(1)) if speed_match else 0
    except (TypeError, ValueError):
        speed = 0

    wifi_mode_match = re.search(r"<WifiModeString>([^<]+)</WifiModeString>", normalized)
    wifi_mode_string = (wifi_mode_match.group(1).strip() if wifi_mode_match else "")

    if link == 1 and speed > 0:
        transport = "ethernet"
    elif wifi_mode_string:
        upper = wifi_mode_string.upper()
        if "SONOSNET" in upper:
            transport = "sonosnet"
        elif "STATION" in upper:
            transport = "wifi"
        else:
            transport = "wireless"
    else:
        transport = "unknown"

    return {
        "transport": transport,
        "wifi_mode_string": wifi_mode_string or None,
        "ethernet_link": bool(link == 1),
        "ethernet_speed": speed if speed > 0 else None,
    }


def _sonos_find_node_by_ip(ip: Optional[str]) -> Optional[dict]:
    if not ip:
        return None
    needle = ip.strip().lower()
    if not needle:
        return None
    for node in nodes.values():
        if node.get("type") != "sonos":
            continue
        node_ip = _sonos_ip_from_url(node.get("url"))
        if node_ip and node_ip.strip().lower() == needle:
            return node
    return None


def _sonos_client_allows_stream(*, resolved_channel_id: str, client_ip: Optional[str]) -> bool:
    """If the client is a known Sonos node, only allow pulling its assigned channel.

    This prevents a Sonos device that was previously assigned from continuing to
    play RoomCast after being unassigned (or after a controller restart).
    """

    node = _sonos_find_node_by_ip(client_ip)
    if not node:
        return True
    desired = resolve_node_channel_id(node)
    return bool(desired and desired == resolved_channel_id)


def _sonos_mark_stream_activity(*, channel_id: str, client_ip: Optional[str], kind: str) -> None:
    node = _sonos_find_node_by_ip(client_ip)
    if not node:
        return
    now = time.time()
    node["sonos_stream_last_client"] = client_ip
    if kind == "head":
        node["sonos_last_stream_head_ts"] = now
        return
    if kind == "get":
        node["sonos_last_stream_get_ts"] = now
        node["sonos_stream_active"] = True
        node["connection_state"] = "playing"
        node["connection_error"] = None
        node["sonos_connecting_since"] = None
        return
    if kind == "bytes":
        node["sonos_last_stream_byte_ts"] = now


def _sonos_mark_stream_end(*, client_ip: Optional[str]) -> None:
    node = _sonos_find_node_by_ip(client_ip)
    if not node:
        return
    node["sonos_stream_active"] = False


def _sonos_stream_is_stale(node: dict, *, now: float) -> bool:
    if not node or node.get("type") != "sonos":
        return False
    channel_id = resolve_node_channel_id(node)
    if not channel_id:
        return False
    if node.get("online") is False:
        return False

    connecting_since = node.get("sonos_connecting_since")
    if isinstance(connecting_since, (int, float)) and float(connecting_since) > 0:
        # Give Sonos a grace window after a (re)connect attempt before we declare it broken.
        if now - float(connecting_since) <= SONOS_CONNECT_GRACE_SECONDS:
            return False
        last_get = node.get("sonos_last_stream_get_ts")
        if not isinstance(last_get, (int, float)):
            last_head = node.get("sonos_last_stream_head_ts")
            if isinstance(last_head, (int, float)):
                return (now - float(last_head)) > SONOS_STREAM_STALE_SECONDS
            return True
        return (now - float(last_get)) > SONOS_STREAM_STALE_SECONDS

    last_byte = node.get("sonos_last_stream_byte_ts")
    if isinstance(last_byte, (int, float)):
        return (now - float(last_byte)) > SONOS_STREAM_STALE_SECONDS
    last_get = node.get("sonos_last_stream_get_ts")
    if isinstance(last_get, (int, float)):
        return (now - float(last_get)) > SONOS_STREAM_STALE_SECONDS
    last_head = node.get("sonos_last_stream_head_ts")
    if isinstance(last_head, (int, float)):
        return (now - float(last_head)) > SONOS_STREAM_STALE_SECONDS
    # Assigned but never seen any stream pull: treat as stale.
    return True


async def _sonos_attempt_reconnect(channel_id: str, members: list[dict]) -> bool:
    async with sonos_reconcile_lock:
        if not members:
            return True
        members_sorted = sorted(members, key=lambda item: (item.get("name") or "", item.get("id") or ""))
        coordinator = members_sorted[0]
        coordinator_ip = _sonos_ip_from_url(coordinator.get("url"))
        coordinator_rincon = coordinator.get("sonos_rincon")
        if not coordinator_ip or not coordinator_rincon:
            return False

        connect_ts = time.time()
        for n in members_sorted:
            n["connection_state"] = "connecting"
            n["connection_error"] = None
            n["sonos_connecting_since"] = connect_ts
        await broadcast_nodes()

        for attempt in range(1, max(1, SONOS_RECONNECT_ATTEMPTS) + 1):
            attempt_start = time.time()
            try:
                try:
                    await _sonos_become_standalone(coordinator_ip)
                except Exception:
                    pass

                await _sonos_set_uri_and_play_with_fallback(channel_id, coordinator_ip)

                for member in members_sorted[1:]:
                    member_ip = _sonos_ip_from_url(member.get("url"))
                    if not member_ip:
                        continue
                    try:
                        await _sonos_join(member_ip, coordinator_rincon)
                    except Exception as exc:
                        member["connection_state"] = "error"
                        member["connection_error"] = f"Failed to join Sonos group: {exc}"

                # Wait until Sonos actually pulls and receives bytes from our stream.
                deadline = attempt_start + max(1.0, SONOS_RECONNECT_WAIT_FOR_STREAM_SECONDS)
                while time.time() < deadline:
                    coord_node = _sonos_find_node_by_ip(coordinator_ip)
                    last_get = coord_node.get("sonos_last_stream_get_ts") if coord_node else None
                    last_byte = coord_node.get("sonos_last_stream_byte_ts") if coord_node else None
                    if isinstance(last_get, (int, float)) and float(last_get) >= attempt_start:
                        if isinstance(last_byte, (int, float)) and float(last_byte) >= attempt_start:
                            for n in members_sorted:
                                if n.get("connection_state") != "error":
                                    n["connection_state"] = "playing"
                                    n["connection_error"] = None
                                n["sonos_connecting_since"] = None
                            await broadcast_nodes()
                            return True
                    await asyncio.sleep(0.35)
            except Exception as exc:
                log.warning(
                    "Sonos reconnect attempt %s/%s failed (channel=%s, coordinator=%s): %r",
                    attempt,
                    SONOS_RECONNECT_ATTEMPTS,
                    channel_id,
                    coordinator_ip,
                    exc,
                )
                await asyncio.sleep(min(1.5, 0.35 * attempt))

        # Failed to reconnect: unassign and surface error.
        err = f"Lost connection to Sonos stream; reconnect failed ({SONOS_RECONNECT_ATTEMPTS} attempts)."
        for n in members_sorted:
            n["connection_state"] = "error"
            n["connection_error"] = err
            n["sonos_connecting_since"] = None
            ip = _sonos_ip_from_url(n.get("url"))
            if ip:
                try:
                    await _sonos_become_standalone(ip)
                except Exception:
                    pass
                try:
                    await _sonos_stop(ip)
                except Exception:
                    pass
        save_nodes()
        await broadcast_nodes()
        return False


async def _sonos_connection_loop() -> None:
    try:
        while True:
            try:
                await asyncio.sleep(max(1.0, SONOS_CONNECTION_POLL_INTERVAL))
                now = time.time()
                by_channel: dict[str, list[dict]] = {}
                for node in nodes.values():
                    if node.get("type") != "sonos":
                        continue
                    if node.get("online") is False:
                        continue
                    cid = resolve_node_channel_id(node)
                    if not cid:
                        continue
                    by_channel.setdefault(cid, []).append(node)

                for channel_id, members in by_channel.items():
                    if not members:
                        continue
                    members_sorted = sorted(members, key=lambda item: (item.get("name") or "", item.get("id") or ""))
                    coordinator = members_sorted[0]
                    if not _sonos_stream_is_stale(coordinator, now=now):
                        continue
                    log.warning(
                        "Sonos stream appears stale; attempting reconnect (channel=%s, coordinator=%s)",
                        channel_id,
                        _sonos_ip_from_url(coordinator.get("url")),
                    )
                    await _sonos_attempt_reconnect(channel_id, members_sorted)
            except asyncio.CancelledError:
                break
            except Exception:
                log.exception("Sonos connection monitor iteration failed")
                await asyncio.sleep(1.0)
    except asyncio.CancelledError:
        pass


async def _reconcile_sonos_groups() -> None:
    """Ensure Sonos speakers with the same channel_id are grouped together.

    - Each Sonos device is still represented as a separate RoomCast node.
    - If multiple Sonos nodes share the same channel, they are grouped for sync.
    - If a Sonos node is unassigned, it becomes standalone and we stop playback.
    """

    async with sonos_reconcile_lock:
        by_channel: dict[str, list[dict]] = {}
        standalone: list[dict] = []
        for node in nodes.values():
            if node.get("type") != "sonos":
                continue
            if node.get("online") is False:
                continue
            cid = resolve_node_channel_id(node)
            if not cid:
                standalone.append(node)
                continue
            by_channel.setdefault(cid, []).append(node)

        # Standalone nodes: ensure they're standalone; stop if unassigned.
        for node in standalone:
            node["connection_state"] = None
            node["connection_error"] = None
            node["sonos_connecting_since"] = None
            ip = _sonos_ip_from_url(node.get("url"))
            if not ip:
                continue
            try:
                await _sonos_become_standalone(ip)
                await _sonos_stop(ip)
            except Exception:
                # Sonos may already be standalone or playing something else; do not crash reconciliation.
                pass

        for channel_id, members in by_channel.items():
            if not members:
                continue

            connect_ts = time.time()
            for n in members:
                n["connection_state"] = "connecting"
                n["connection_error"] = None
                n["sonos_connecting_since"] = connect_ts
            await broadcast_nodes()

            # Choose deterministic coordinator.
            members_sorted = sorted(members, key=lambda item: (item.get("name") or "", item.get("id") or ""))
            coordinator = members_sorted[0]
            coordinator_ip = _sonos_ip_from_url(coordinator.get("url"))
            coordinator_rincon = coordinator.get("sonos_rincon")
            if not coordinator_ip or not coordinator_rincon:
                for n in members_sorted:
                    n["connection_state"] = "error"
                    n["connection_error"] = "Invalid Sonos coordinator configuration"
                    n["sonos_connecting_since"] = None
                continue

            # Ensure coordinator is standalone and playing the correct RoomCast channel stream.
            try:
                await _sonos_become_standalone(coordinator_ip)
            except Exception:
                pass
            try:
                await _sonos_set_uri_and_play_with_fallback(channel_id, coordinator_ip)
            except HTTPException as exc:
                detail = getattr(exc, "detail", None)
                msg = detail or str(exc) or repr(exc)
                log.warning(
                    "Sonos coordinator %s failed to start stream (ip=%s, channel=%s): %s",
                    coordinator.get("id"),
                    coordinator_ip,
                    channel_id,
                    msg,
                )
                for n in members_sorted:
                    n["connection_state"] = "error"
                    n["connection_error"] = f"Sonos failed to start stream: {msg}"
                    n["sonos_connecting_since"] = None
                continue
            except Exception as exc:
                log.warning(
                    "Sonos coordinator %s failed to start stream (ip=%s, channel=%s): %r",
                    coordinator.get("id"),
                    coordinator_ip,
                    channel_id,
                    exc,
                )
                for n in members_sorted:
                    n["connection_state"] = "error"
                    n["connection_error"] = f"Sonos failed to start stream: {exc}"
                    n["sonos_connecting_since"] = None
                continue

            # Join remaining members to coordinator.
            for member in members_sorted[1:]:
                member_ip = _sonos_ip_from_url(member.get("url"))
                if not member_ip:
                    member["connection_state"] = "error"
                    member["connection_error"] = "Invalid Sonos member configuration"
                    member["sonos_connecting_since"] = None
                    continue
                try:
                    await _sonos_join(member_ip, coordinator_rincon)
                except Exception as exc:
                    member["connection_state"] = "error"
                    member["connection_error"] = f"Failed to join Sonos group: {exc}"
                    member["sonos_connecting_since"] = None
                    log.warning("Sonos member %s failed to join group: %s", member.get("id"), exc)

            # Do not mark as playing until we actually observe the Sonos device
            # pulling bytes from our HTTP stream. That observation happens in
            # _sonos_mark_stream_activity() when the stream endpoint receives GET/bytes.



def _node_host_from_url(url: Optional[str]) -> Optional[str]:
    if not url or url.startswith("browser:"):
        return None
    try:
        parsed = urlparse(url)
    except ValueError:
        return None
    return parsed.hostname


def _node_host_aliases(node_host: Optional[str]) -> set[str]:
    aliases: set[str] = set()
    if not node_host:
        return aliases
    host = node_host.strip()
    if not host:
        return aliases
    normalized = host.lower()
    aliases.add(normalized)
    if normalized.startswith("[") and normalized.endswith("]"):
        aliases.add(normalized[1:-1])
    if ":" in normalized and normalized.count(":") > 1 and not normalized.startswith("["):
        # Likely IPv6 without brackets; add raw form for matching
        aliases.add(normalized)
    if "." in normalized:
        short = normalized.split(".", 1)[0]
        if short:
            aliases.add(short)
    try:
        resolved = socket.gethostbyname(host.strip("[]"))
    except (socket.gaierror, UnicodeError):
        resolved = None
    if resolved:
        aliases.add(resolved.strip().lower())
    return {value for value in aliases if value}


def _match_snapclient_for_node(node: dict, clients: list[dict]) -> Optional[dict]:
    if not clients:
        return None
    fallback: Optional[dict] = None

    def _consider(candidate: dict) -> Optional[dict]:
        nonlocal fallback
        if candidate.get("connected"):
            return candidate
        if fallback is None:
            fallback = candidate
        return None

    stored_id = (node.get("snapclient_id") or "").strip()
    if stored_id:
        for client in clients:
            if (client.get("id") or "").strip() == stored_id:
                match = _consider(client)
                if match:
                    return match
    fingerprint = (node.get("fingerprint") or "").strip()
    if fingerprint:
        for client in clients:
            if (client.get("id") or "").strip() == fingerprint:
                match = _consider(client)
                if match:
                    return match
    host_aliases = _node_host_aliases(_node_host_from_url(node.get("url")))
    if host_aliases:
        for client in clients:
            host = client.get("host") or {}
            client_matches: set[str] = set()
            name = (host.get("name") or "").strip().lower()
            if name:
                client_matches.add(name)
                if "." in name:
                    client_matches.add(name.split(".", 1)[0])
            ip = (host.get("ip") or "").strip().lower()
            if ip:
                client_matches.add(ip)
            if not (client_matches & host_aliases):
                continue
            match = _consider(client)
            if match:
                return match
    node_name = (node.get("name") or "").strip().lower()
    if node_name:
        for client in clients:
            config = client.get("config") or {}
            client_name = (config.get("name") or "").strip().lower()
            if client_name and client_name == node_name:
                match = _consider(client)
                if match:
                    return match
    return fallback


async def _snapcast_group_id_for_client(client_id: Optional[str]) -> Optional[str]:
    if not client_id:
        return None
    try:
        clients = await snapcast.list_clients()
    except Exception as exc:  # pragma: no cover - network dependency
        log.warning("WebRTC stream assignment: failed to list snapclients: %s", exc)
        return None
    for client in clients:
        if (client.get("id") or "").strip() == client_id:
            group_id = (client.get("_group_id") or "").strip()
            if group_id:
                return group_id
            return None
    return None


def _register_node_internal(
    reg: NodeRegistration,
    *,
    fingerprint: Optional[str] = None,
    normalized_url: Optional[str] = None,
) -> dict:
    normalized = normalized_url or _normalize_node_url(reg.url)
    now = time.time()
    if reg.id and reg.id in nodes:
        node = nodes[reg.id]
        node["url"] = normalized
        node["last_seen"] = now
        if fingerprint:
            node["fingerprint"] = fingerprint
        node["online"] = True
        node.pop("offline_since", None)
        return node
    if fingerprint:
        for existing in nodes.values():
            if existing.get("fingerprint") == fingerprint:
                existing["url"] = normalized
                existing["last_seen"] = now
                existing["fingerprint"] = fingerprint
                existing["online"] = True
                existing.pop("offline_since", None)
                return existing
    for existing in nodes.values():
        if existing["url"].rstrip("/") == normalized:
            existing["last_seen"] = now
            if fingerprint:
                existing["fingerprint"] = fingerprint
            existing["online"] = True
            existing.pop("offline_since", None)
            return existing
    node_id = reg.id or str(uuid.uuid4())
    previous = nodes.get(node_id, {})
    if normalized.startswith("browser:"):
        node_type = "browser"
    elif normalized.startswith("sonos://"):
        node_type = "sonos"
    else:
        node_type = "agent"
    nodes[node_id] = {
        "id": node_id,
        "name": reg.name,
        "url": normalized,
        "last_seen": now,
        "type": node_type,
        "eq": default_eq_state(),
        "agent_secret": previous.get("agent_secret"),
        "audio_configured": True if node_type in {"browser", "sonos"} else previous.get("audio_configured", False),
        "agent_version": previous.get("agent_version"),
        "volume_percent": _normalize_percent(previous.get("volume_percent", 75), default=75),
        "max_volume_percent": _normalize_percent(previous.get("max_volume_percent", 100), default=100),
        "muted": previous.get("muted", False),
        "stereo_mode": _normalize_stereo_mode(previous.get("stereo_mode")),
        "updating": bool(previous.get("updating", False)),
        "playback_device": previous.get("playback_device"),
        "outputs": previous.get("outputs", {}),
        "fingerprint": fingerprint or previous.get("fingerprint"),
        "channel_id": _select_initial_channel_id(previous.get("channel_id"), fallback=node_type != "browser"),
        "snapclient_id": previous.get("snapclient_id"),
        "online": True,
        "offline_since": None,
        "is_controller": bool(previous.get("is_controller")),
        "sonos_udn": previous.get("sonos_udn"),
        "sonos_rincon": previous.get("sonos_rincon"),
    }
    save_nodes()
    return nodes[node_id]


def create_browser_node(name: str) -> dict:
    reg = NodeRegistration(id=str(uuid.uuid4()), name=name, url=f"browser:{uuid.uuid4()}")
    return public_node(_register_node_internal(reg))


def _controller_node() -> Optional[dict]:
    for node in nodes.values():
        if node.get("is_controller"):
            return node
    return None


async def _fetch_agent_fingerprint(url: str) -> Optional[str]:
    normalized = _normalize_node_url(url)
    if not normalized or normalized.startswith("browser:") or normalized.startswith("sonos://"):
        return None
    target = f"{normalized}/health"
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            resp = await client.get(target)
        if resp.status_code != 200:
            return None
        data = resp.json()
    except Exception:
        return None
    fingerprint = data.get("fingerprint")
    if isinstance(fingerprint, str) and fingerprint:
        return fingerprint
    return None


async def request_agent_secret(node: dict, force: bool = False, *, recovery_code: str | None = None) -> str:
    if node.get("type") != "agent":
        raise HTTPException(status_code=400, detail="Pairing only applies to hardware nodes")
    url = f"{node['url'].rstrip('/')}/pair"
    payload: dict = {"force": bool(force)}
    if isinstance(recovery_code, str) and recovery_code.strip():
        payload["recovery_code"] = recovery_code.strip()
    headers = {}
    # If we already have a secret (same controller), prove it so the agent allows rotation.
    secret = node.get("agent_secret")
    if isinstance(secret, str) and secret:
        headers["X-Agent-Secret"] = secret
    async with httpx.AsyncClient(timeout=5) as client:
        resp = await client.post(url, json=payload, headers=headers)
    if resp.status_code >= 400:
        raise HTTPException(status_code=resp.status_code, detail=resp.text or "Failed to pair node")
    try:
        data = resp.json()
    except ValueError:
        raise HTTPException(status_code=502, detail="Invalid response from node agent")
    secret = data.get("secret")
    if not secret:
        raise HTTPException(status_code=502, detail="Node agent did not return a secret")
    return secret


async def configure_agent_audio(node: dict) -> dict:
    if node.get("type") != "agent":
        raise HTTPException(status_code=400, detail="Audio configuration only applies to hardware nodes")
    payload = {
        "snapserver_host": SNAPSERVER_AGENT_HOST,
        "snapserver_port": SNAPCLIENT_PORT,
    }
    result = await _call_agent(node, "/config/snapclient", payload)
    node["audio_configured"] = bool(result.get("configured", True))
    await refresh_agent_metadata(node, persist=False)
    save_nodes()
    return result


def _mark_node_online(node: dict, *, timestamp: Optional[float] = None) -> bool:
    now = timestamp or time.time()
    was_online = bool(node.get("online", False))
    node["online"] = True
    node["last_seen"] = now
    offline_flag = node.pop("offline_since", None)
    return not was_online or offline_flag is not None


def _mark_node_offline(node: dict, *, timestamp: Optional[float] = None) -> bool:
    if node.get("type") == "browser":
        return False
    now = timestamp or time.time()
    was_online = bool(node.get("online", False))
    node["online"] = False
    node.setdefault("offline_since", now)
    return was_online


def cancel_node_rediscovery(node_id: Optional[str]) -> None:
    if not node_id:
        return
    task = node_rediscovery_tasks.pop(node_id, None)
    if task:
        task.cancel()


def schedule_node_rediscovery(node: dict) -> None:
    if not node or node.get("type") != "agent":
        return
    node_id = node.get("id")
    fingerprint = node.get("fingerprint")
    if not node_id or not fingerprint:
        return
    if node_id in node_rediscovery_tasks:
        return
    log.info("Scheduling rediscovery for node %s (fingerprint %s)", node_id, fingerprint[:8])
    node_rediscovery_tasks[node_id] = asyncio.create_task(_rediscover_node(node_id, fingerprint))


async def refresh_agent_metadata(node: dict, *, persist: bool = True) -> tuple[bool, bool]:
    if node.get("type") != "agent":
        return False, False
    url = f"{node['url'].rstrip('/')}/health"
    now = time.time()
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            resp = await client.get(url)
        if resp.status_code != 200:
            changed = _mark_node_offline(node, timestamp=now)
            if changed and persist:
                save_nodes()
            schedule_node_rediscovery(node)
            return False, changed
        data = resp.json()
    except Exception:
        changed = _mark_node_offline(node, timestamp=now)
        if changed and persist:
            save_nodes()
        schedule_node_rediscovery(node)
        return False, changed

    changed = False
    if _mark_node_online(node, timestamp=now):
        changed = True
    fingerprint = data.get("fingerprint")
    if isinstance(fingerprint, str) and fingerprint and node.get("fingerprint") != fingerprint:
        node["fingerprint"] = fingerprint
        changed = True
    version = data.get("version")
    if version and node.get("agent_version") != version:
        node["agent_version"] = version
        changed = True
    configured = bool(data.get("configured"))
    if node.get("audio_configured") != configured:
        node["audio_configured"] = configured
        changed = True
        if configured:
            node.setdefault("_needs_reconfig", False)
        else:
            node["_needs_reconfig"] = True
    if "updating" in data:
        updating = bool(data.get("updating"))
        if node.get("updating") != updating:
            node["updating"] = updating
            changed = True
    if "playback_device" in data:
        device = data.get("playback_device")
        if node.get("playback_device") != device:
            node["playback_device"] = device
            changed = True
    if isinstance(data.get("outputs"), dict):
        outputs = data["outputs"]
        if node.get("outputs") != outputs:
            node["outputs"] = outputs
            changed = True
    wifi_payload = data.get("wifi")
    sanitized_wifi = None
    if isinstance(wifi_payload, dict):
        percent_raw = wifi_payload.get("percent")
        if isinstance(percent_raw, (int, float)):
            percent_val = max(0, min(100, int(round(percent_raw))))
            sanitized_wifi = {"percent": percent_val}
            signal_raw = wifi_payload.get("signal_dbm")
            if isinstance(signal_raw, (int, float)):
                sanitized_wifi["signal_dbm"] = float(signal_raw)
            iface_raw = wifi_payload.get("interface")
            if isinstance(iface_raw, str):
                iface = iface_raw.strip()
                if iface:
                    sanitized_wifi["interface"] = iface
    if sanitized_wifi:
        if node.get("wifi") != sanitized_wifi:
            node["wifi"] = sanitized_wifi
            changed = True
    elif "wifi" in node:
        node.pop("wifi", None)
        changed = True
    if "max_volume_percent" in data:
        max_vol = _normalize_percent(data.get("max_volume_percent"), default=_get_node_max_volume(node))
        if node.get("max_volume_percent") != max_vol:
            node["max_volume_percent"] = max_vol
            changed = True
    needs_reconfig = bool(node.pop("_needs_reconfig", False))
    if configured and needs_reconfig:
        try:
            await configure_agent_audio(node)
            await _sync_node_max_volume(node)
        except Exception:
            node["audio_configured"] = False
            node["_needs_reconfig"] = True
        else:
            node["audio_configured"] = True
            changed = True
    if persist and changed:
        save_nodes()
    return True, changed


def schedule_agent_refresh(node_id: str, delay: float = 10.0, *, repeat: bool = False, attempts: int = 6) -> None:
    existing = agent_refresh_tasks.pop(node_id, None)
    if existing:
        existing.cancel()

    async def _task() -> None:
        remaining = max(1, attempts)
        try:
            while remaining > 0:
                await asyncio.sleep(delay)
                node = nodes.get(node_id)
                if not node:
                    return
                reachable, changed = await refresh_agent_metadata(node)
                if reachable and changed:
                    await broadcast_nodes()
                if not repeat:
                    return
                if reachable and not node.get("updating"):
                    return
                remaining -= 1
            node = nodes.get(node_id)
            if repeat and node and node.get("updating"):
                node["updating"] = False
                save_nodes()
                await broadcast_nodes()
        finally:
            agent_refresh_tasks.pop(node_id, None)

    agent_refresh_tasks[node_id] = asyncio.create_task(_task())


def schedule_restart_watch(node_id: str) -> None:
    existing = pending_restarts.pop(node_id, None)
    if existing:
        task = existing.get("task")
        if task:
            task.cancel()
    deadline = time.time() + NODE_RESTART_TIMEOUT

    async def _monitor() -> None:
        saw_offline = False
        try:
            while time.time() < deadline:
                await asyncio.sleep(NODE_RESTART_INTERVAL)
                node = nodes.get(node_id)
                if not node:
                    return
                reachable, _ = await refresh_agent_metadata(node)
                if reachable:
                    if saw_offline:
                        log.info("Node %s reported healthy after restart", node_id)
                        return
                else:
                    saw_offline = True
            log.warning("Node %s did not return within restart timeout", node_id)
        finally:
            pending_restarts.pop(node_id, None)
            await broadcast_nodes()

    task = asyncio.create_task(_monitor())
    pending_restarts[node_id] = {"deadline": deadline, "task": task}


async def teardown_browser_node(node_id: str, *, remove_entry: bool = True) -> None:
    ws = browser_ws.pop(node_id, None)
    if ws:
        try:
            await ws.send_json({"type": "session", "state": "ended"})
        except Exception:
            pass
        await ws.close()
    if remove_entry and nodes.pop(node_id, None):
        save_nodes()
        await broadcast_nodes()


async def _handle_webrtc_session_closed(node_id: str) -> None:
    node = nodes.get(node_id)
    if node and node.get("type") != "browser":
        return
    await teardown_browser_node(node_id)


def load_nodes() -> None:
    global nodes, sections
    if not NODES_PATH.exists():
        nodes = {}
        sections = []
        return
    try:
        raw = json.loads(NODES_PATH.read_text())
        if isinstance(raw, dict):
            data = raw.get("nodes") or []
            section_data = raw.get("sections") or []
        elif isinstance(raw, list):
            data = raw
            section_data = []
        else:
            data = []
            section_data = []

        sections = []
        seen_section_ids: set[str] = set()
        if isinstance(section_data, list):
            for entry in section_data:
                if not isinstance(entry, dict):
                    continue
                sid = entry.get("id") or str(uuid.uuid4())
                if sid in seen_section_ids:
                    continue
                name = _normalize_section_name(entry.get("name")) or "Section"
                section = {
                    "id": sid,
                    "name": name,
                    "created_at": entry.get("created_at") or int(time.time()),
                    "updated_at": entry.get("updated_at") or int(time.time()),
                }
                sections.append(section)
                seen_section_ids.add(sid)

        nodes = {}
        next_order_by_group: Dict[str, int] = {}
        for item in data:
            item = dict(item)
            item.pop("pan", None)
            eq = item.get("eq") or default_eq_state()
            eq.setdefault("bands", [])
            eq.setdefault("band_count", len(eq["bands"]) or 15)
            eq.setdefault("preset", DEFAULT_EQ_PRESET)
            item["eq"] = eq
            item["url"] = _normalize_node_url(item.get("url", ""))
            if item.get("type") in {"browser", "sonos"}:
                item["audio_configured"] = True
            else:
                item["audio_configured"] = bool(item.get("audio_configured"))
            item["volume_percent"] = _normalize_percent(item.get("volume_percent", 75), default=75)
            item["max_volume_percent"] = _normalize_percent(item.get("max_volume_percent", 100), default=100)
            item["muted"] = bool(item.get("muted", False))
            item["updating"] = bool(item.get("updating", False))
            item["playback_device"] = item.get("playback_device")
            outputs = item.get("outputs")
            if not isinstance(outputs, dict):
                outputs = {}
            item["outputs"] = outputs
            item["fingerprint"] = item.get("fingerprint")
            try:
                item["last_seen"] = float(item.get("last_seen", 0)) or 0.0
            except (TypeError, ValueError):
                item["last_seen"] = 0.0
            offline_since = item.get("offline_since")
            try:
                item["offline_since"] = float(offline_since) if offline_since is not None else None
            except (TypeError, ValueError):
                item["offline_since"] = None
            if item.get("type") == "browser":
                item["online"] = True
            else:
                item["online"] = bool(item.get("online", False))
            if item.get("type") == "sonos":
                item["sonos_udn"] = item.get("sonos_udn")
                item["sonos_rincon"] = item.get("sonos_rincon")
            # Persist explicit unassignments:
            # - If the JSON has "channel_id": null, keep it unassigned.
            # - Only auto-assign a default channel when the field is missing entirely
            #   (backwards-compat for older configs / newly discovered nodes).
            if "channel_id" in item:
                item["channel_id"] = _select_initial_channel_id(item.get("channel_id"), fallback=False)
            else:
                item["channel_id"] = _select_initial_channel_id(None, fallback=item.get("type") != "browser")

            # Sections normalization: allow unsectioned nodes.
            section_id = item.get("section_id")
            if isinstance(section_id, str):
                section_id = section_id.strip() or None
            else:
                section_id = None
            if section_id and not _find_section(section_id):
                section_id = None
            item["section_id"] = section_id

            # Stable ordering within a group (section id or unsectioned group).
            group_key = section_id or "__unsectioned__"
            raw_order = item.get("section_order")
            try:
                order_val = int(raw_order)
            except (TypeError, ValueError):
                order_val = None
            if order_val is None or order_val < 0:
                order_val = next_order_by_group.get(group_key, 0)
            item["section_order"] = order_val
            next_order_by_group[group_key] = max(next_order_by_group.get(group_key, 0), order_val + 1)
            snapclient_id = (item.get("snapclient_id") or "").strip() or None
            item["snapclient_id"] = snapclient_id
            nodes[item["id"]] = item
    except Exception:
        nodes = {}
        sections = []


def save_nodes() -> None:
    section_rank = {section.get("id"): idx for idx, section in enumerate(sections) if section.get("id")}

    def _node_key(entry: dict) -> tuple:
        sid = entry.get("section_id")
        rank = section_rank.get(sid, 10**9)
        order = entry.get("section_order")
        try:
            order_val = int(order)
        except (TypeError, ValueError):
            order_val = 10**9
        name_val = (entry.get("name") or "").lower()
        return (rank, order_val, name_val)

    serialized_nodes = []
    for node in sorted(nodes.values(), key=_node_key):
        entry = {k: v for k, v in node.items() if k not in TRANSIENT_NODE_FIELDS}
        serialized_nodes.append(entry)

    serialized_sections = []
    for section in sections:
        if not isinstance(section, dict):
            continue
        sid = section.get("id")
        name = _normalize_section_name(section.get("name"))
        if not sid or not name:
            continue
        serialized_sections.append({
            "id": sid,
            "name": name,
            "created_at": section.get("created_at") or int(time.time()),
            "updated_at": section.get("updated_at") or int(time.time()),
        })

    NODES_PATH.write_text(json.dumps({"sections": serialized_sections, "nodes": serialized_nodes}, indent=2))


load_nodes()


async def _spotify_refresh_loop() -> None:
    while True:
        try:
            await asyncio.sleep(max(SPOTIFY_REFRESH_CHECK_INTERVAL, 5))
            for source_id, source in list(sources_by_id.items()):
                if not source or source.get("kind") != "spotify":
                    continue
                token = load_token(source_id)
                if not token or "refresh_token" not in token:
                    continue
                seconds_left = _token_seconds_until_expiry(token)
                if seconds_left is None:
                    seconds_left = -1
                if seconds_left > SPOTIFY_REFRESH_LEEWAY:
                    continue
                try:
                    await spotify_refresh(token, source_id)
                except HTTPException as exc:
                    log.warning("Background Spotify refresh failed for %s: %s", source_id, exc.detail)
                    await asyncio.sleep(SPOTIFY_REFRESH_FAILURE_BACKOFF)
        except asyncio.CancelledError:
            break
        except Exception:  # pragma: no cover - defensive
            log.exception("Spotify refresh loop crashed")
            await asyncio.sleep(SPOTIFY_REFRESH_FAILURE_BACKOFF)


@app.on_event("startup")
async def _startup_events() -> None:
    global webrtc_relay, node_health_task, spotify_refresh_task, channel_idle_task, sonos_connection_task
    # Providers are modular: by default no provider runtimes should run.
    # If a provider is installed+enabled, reconcile its runtime containers here.
    try:
        if is_provider_enabled("spotify"):
            _reconcile_spotify_runtime(spotify_instance_count())
        else:
            # Best-effort cleanup (do not mutate config/channels).
            try:
                spotify_provider.stop_runtime()
            except DockerUnavailable:
                pass
        _reconcile_radio_runtime(is_provider_enabled("radio"))
    except HTTPException as exc:
        log.warning("Provider runtime reconcile skipped: %s", exc.detail)
    except Exception:  # pragma: no cover - defensive
        log.exception("Provider runtime reconcile crashed")
    if WEBRTC_ENABLED:
        webrtc_relay = WebAudioRelay(
            snap_host=SNAPSERVER_HOST,
            snap_port=SNAPCLIENT_PORT,
            latency_ms=WEBRTC_LATENCY_MS,
            sample_rate=WEBRTC_SAMPLE_RATE,
            assign_stream=_assign_webrtc_stream,
            on_session_closed=_handle_webrtc_session_closed,
        )
        await webrtc_relay.start()
    if node_health_task is None:
        node_health_task = asyncio.create_task(_node_health_loop())
    if spotify_refresh_task is None:
        spotify_refresh_task = asyncio.create_task(_spotify_refresh_loop())
    if channel_idle_task is None:
        log.info(
            "Channel idle monitor enabled (timeout=%.0fs, poll=%.1fs)",
            CHANNEL_IDLE_TIMEOUT,
            CHANNEL_IDLE_POLL_INTERVAL,
        )
        channel_idle_task = asyncio.create_task(_channel_idle_loop())

    if sonos_connection_task is None:
        sonos_connection_task = asyncio.create_task(_sonos_connection_loop())


@app.on_event("shutdown")
async def _shutdown_events() -> None:
    global node_health_task, spotify_refresh_task, channel_idle_task, sonos_connection_task
    if webrtc_relay:
        await webrtc_relay.stop()
    if node_health_task:
        node_health_task.cancel()
        try:
            await node_health_task
        except asyncio.CancelledError:
            pass
        node_health_task = None
    if spotify_refresh_task:
        spotify_refresh_task.cancel()
        try:
            await spotify_refresh_task
        except asyncio.CancelledError:
            pass
        spotify_refresh_task = None
    if channel_idle_task:
        channel_idle_task.cancel()
        try:
            await channel_idle_task
        except asyncio.CancelledError:
            pass
        channel_idle_task = None
    if sonos_connection_task:
        sonos_connection_task.cancel()
        try:
            await sonos_connection_task
        except asyncio.CancelledError:
            pass
        sonos_connection_task = None
    await stop_local_agent()


def current_spotify_creds(identifier: Optional[str] = None) -> Tuple[str, str, str]:
    cfg = read_spotify_config(identifier, include_secret=True)
    cid = (cfg.get("client_id") or os.getenv("SPOTIFY_CLIENT_ID") or "").strip()
    secret = cfg.get("client_secret") or os.getenv("SPOTIFY_CLIENT_SECRET") or ""
    redirect = cfg.get("redirect_uri") or os.getenv("SPOTIFY_REDIRECT_URI") or SPOTIFY_REDIRECT_URI
    return cid, secret, redirect


def read_spotify_config(identifier: Optional[str] = None, include_secret: bool = False) -> dict:
    resolved_channel_id = None
    spotify_source_id = _resolve_spotify_source_id(identifier)
    if spotify_source_id is None:
        # Keep a stable shape for UI code even when channel is not Spotify.
        resolved_channel_id = resolve_channel_id(identifier)
        cfg = {
            "channel_id": resolved_channel_id,
            "source_id": None,
            "username": "",
            "device_name": "RoomCast",
            "bitrate": 320,
            "initial_volume": 75,
            "normalisation": True,
            "has_password": False,
            "client_id": (os.getenv("SPOTIFY_CLIENT_ID") or SPOTIFY_CLIENT_ID or "").strip(),
            "has_client_secret": bool(os.getenv("SPOTIFY_CLIENT_SECRET") or SPOTIFY_CLIENT_SECRET),
            "redirect_uri": os.getenv("SPOTIFY_REDIRECT_URI") or SPOTIFY_REDIRECT_URI,
            "has_oauth_token": False,
        }
        if include_secret:
            cfg["client_secret"] = (os.getenv("SPOTIFY_CLIENT_SECRET") or SPOTIFY_CLIENT_SECRET or "")
        return cfg

    # If identifier was a channel id, expose it for UI convenience.
    if not _normalize_spotify_source_id(identifier):
        resolved_channel_id = resolve_channel_id(identifier)
    source = get_spotify_source(spotify_source_id)
    cfg_path = Path(source["config_path"])
    token = load_token(spotify_source_id)
    has_token = bool(token and token.get("access_token"))
    config_exists = cfg_path.exists()
    try:
        data = json.loads(cfg_path.read_text()) if config_exists else {}
    except json.JSONDecodeError:
        data = {}

    stored_client_id = (data.get("client_id")
                        or os.getenv("SPOTIFY_CLIENT_ID")
                        or SPOTIFY_CLIENT_ID
                        or "").strip()
    stored_client_secret = (data.get("client_secret")
                            or os.getenv("SPOTIFY_CLIENT_SECRET")
                            or SPOTIFY_CLIENT_SECRET
                            or "")
    stored_redirect = (data.get("redirect_uri")
                       or os.getenv("SPOTIFY_REDIRECT_URI")
                       or SPOTIFY_REDIRECT_URI)

    cfg = {
        "channel_id": resolved_channel_id,
        "source_id": spotify_source_id,
        "username": data.get("username", ""),
        "device_name": data.get("device_name", "RoomCast"),
        "bitrate": data.get("bitrate", 320),
        "initial_volume": data.get("initial_volume", 75),
        "normalisation": data.get("normalisation", True),
        "has_password": bool(data.get("password")),
        "client_id": stored_client_id,
        "has_client_secret": bool(stored_client_secret),
        "redirect_uri": stored_redirect,
        "has_oauth_token": has_token,
    }
    if include_secret:
        cfg["client_secret"] = stored_client_secret
    elif not config_exists:
        cfg["client_secret"] = "***" if stored_client_secret else ""
    return cfg


def read_librespot_status(identifier: Optional[str] = None) -> dict:
    spotify_source_id = _resolve_spotify_source_id(identifier)
    if spotify_source_id is None:
        return {"state": "unknown", "message": "Not a Spotify channel"}
    source = get_spotify_source(spotify_source_id)
    status_path = Path(source["status_path"])
    if not status_path.exists():
        return {"state": "unknown", "message": "No status yet"}
    try:
        return json.loads(status_path.read_text())
    except json.JSONDecodeError:
        return {"state": "unknown", "message": "Invalid status file"}


def load_token(identifier: Optional[str] = None) -> Optional[dict]:
    spotify_source_id = _resolve_spotify_source_id(identifier)
    if spotify_source_id is None:
        return None
    source = get_spotify_source(spotify_source_id)
    token_path = Path(source["token_path"])
    if not token_path.exists():
        return None
    try:
        return json.loads(token_path.read_text())
    except json.JSONDecodeError:
        return None


def save_token(data: dict, identifier: Optional[str] = None) -> None:
    if not data:
        return
    spotify_source_id = _resolve_spotify_source_id(identifier)
    if spotify_source_id is None:
        raise HTTPException(status_code=400, detail="Spotify source not configured")
    source = get_spotify_source(spotify_source_id)
    token_path = Path(source["token_path"])
    expires_at: Optional[float] = None
    expires_in = data.get("expires_in")
    if expires_in is not None:
        try:
            expires_at = time.time() + max(int(expires_in), 60)
        except (TypeError, ValueError):
            expires_at = None
    if expires_at is not None:
        data["expires_at"] = expires_at
    token_path.parent.mkdir(parents=True, exist_ok=True)
    token_path.write_text(json.dumps(data, indent=2))


def _token_seconds_until_expiry(token: Optional[dict]) -> Optional[float]:
    if not token:
        return None
    expires_at = token.get("expires_at")
    if expires_at is not None:
        try:
            return float(expires_at) - time.time()
        except (TypeError, ValueError):
            return None
    expires_in = token.get("expires_in")
    if expires_in is not None:
        try:
            return float(expires_in)
        except (TypeError, ValueError):  # pragma: no cover - defensive
            return None
    return None


async def spotify_refresh(token: dict, identifier: Optional[str] = None) -> dict:
    if not token or "refresh_token" not in token:
        raise HTTPException(status_code=401, detail="Spotify not authorized")
    cid, secret, _ = current_spotify_creds(identifier)
    payload = {
        "grant_type": "refresh_token",
        "refresh_token": token["refresh_token"],
        "client_id": cid,
        "client_secret": secret,
    }
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.post("https://accounts.spotify.com/api/token", data=payload)
    if resp.status_code >= 400:
        raise HTTPException(status_code=401, detail="Failed to refresh Spotify token")
    data = resp.json()
    token["access_token"] = data["access_token"]
    token["expires_in"] = data.get("expires_in")
    save_token(token, identifier)
    return token


async def spotify_request(method: str, path: str, token: dict, identifier: Optional[str] = None, **kwargs) -> httpx.Response:
    headers = kwargs.pop("headers", {})
    headers["Authorization"] = f"Bearer {token['access_token']}"
    url = f"https://api.spotify.com/v1{path}"
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.request(method, url, headers=headers, **kwargs)
    if resp.status_code == 401:
        token = await spotify_refresh(token, identifier)
        headers["Authorization"] = f"Bearer {token['access_token']}"
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.request(method, url, headers=headers, **kwargs)
    return resp


def _parse_spotify_error(detail: Any) -> dict:
    payload: Any = detail
    if isinstance(detail, str):
        try:
            payload = json.loads(detail)
        except Exception:
            payload = {"error": {"message": detail}}
    if not isinstance(payload, dict):
        return {"message": str(detail)}
    error = payload.get("error")
    if isinstance(error, dict):
        return {
            "status": error.get("status"),
            "message": error.get("message"),
            "reason": error.get("reason"),
        }
    return {"message": payload.get("message") or str(detail)}


async def _spotify_control(path: str, method: str = "PUT", *, channel_id: Optional[str] = None) -> None:
    token = _ensure_spotify_token(channel_id)
    resp = await spotify_request(method, path, token, identifier=channel_id)
    if resp.status_code < 400:
        return
    try:
        detail = resp.json()
    except Exception:
        detail = resp.text
    raise HTTPException(status_code=resp.status_code, detail=detail)


@app.get("/api/health")
async def health() -> dict:
    return {"status": "ok"}


async def _register_node_payload(reg: NodeRegistration, *, mark_controller: bool = False) -> dict:
    normalized_url = _normalize_node_url(reg.url)
    reg.url = normalized_url
    fingerprint = reg.fingerprint
    desc: Optional[dict] = None
    if normalized_url.startswith("sonos://"):
        ip = _sonos_ip_from_url(normalized_url)
        if not ip:
            raise HTTPException(status_code=400, detail="Invalid Sonos URL")
        desc = await _sonos_fetch_description(ip)
        if not desc:
            raise HTTPException(status_code=502, detail="Failed to read Sonos device description")
        fingerprint = fingerprint or desc.get("udn")
    else:
        fingerprint = fingerprint or await _fetch_agent_fingerprint(normalized_url)
    node = _register_node_internal(reg, fingerprint=fingerprint, normalized_url=normalized_url)
    if mark_controller:
        node["is_controller"] = True
        save_nodes()
    if node.get("type") == "sonos":
        ip = _sonos_ip_from_url(node.get("url"))
        if not ip:
            raise HTTPException(status_code=400, detail="Invalid Sonos node URL")
        # Reuse the description we already fetched for fingerprinting when possible.
        if desc is None:
            desc = await _sonos_fetch_description(ip)

        friendly_name = (desc.get("friendly_name") if isinstance(desc, dict) else None) or None
        zone_name = await _sonos_get_zone_name(ip)
        sonos_app_name = zone_name or friendly_name

        if isinstance(desc, dict):
            node["sonos_udn"] = desc.get("udn")
            node["sonos_rincon"] = desc.get("rincon")
            node["fingerprint"] = node.get("fingerprint") or desc.get("udn")
        node["sonos_friendly_name"] = friendly_name
        node["sonos_zone_name"] = zone_name

        # Prevent Sonos metadata refreshes from overwriting manual renames.
        name_is_custom = node.get("name_is_custom")
        if not isinstance(name_is_custom, bool):
            current_name = (node.get("name") or "").strip()
            # Heuristic for older nodes: if the current name doesn't match any Sonos-reported
            # name, treat it as user-custom.
            inferred_custom = False
            if current_name:
                if sonos_app_name and current_name != sonos_app_name:
                    if friendly_name and current_name != friendly_name:
                        inferred_custom = True
            node["name_is_custom"] = inferred_custom
            name_is_custom = inferred_custom

        if not name_is_custom and sonos_app_name:
            node["name"] = sonos_app_name
        node["audio_configured"] = True
        node["agent_secret"] = None
        save_nodes()
        await broadcast_nodes()
        return public_node(node)
    if node.get("type") == "agent":
        try:
            secret = await request_agent_secret(node, force=True)
            node["agent_secret"] = secret
            await configure_agent_audio(node)
            await _sync_node_max_volume(node)
        except HTTPException as exc:
            # If the node is already paired to another controller, keep it registered so the
            # user can complete takeover pairing by entering the recovery code.
            if exc.status_code in {423, 403} and "recovery" in str(exc.detail).lower():
                node["agent_secret"] = None
                node["audio_configured"] = False
                save_nodes()
                await broadcast_nodes()
                return public_node(node)
            nodes.pop(node["id"], None)
            save_nodes()
            raise
        except Exception:
            nodes.pop(node["id"], None)
            save_nodes()
            raise
        save_nodes()
    await broadcast_nodes()
    return public_node(node)

async def _call_agent(node: dict, path: str, payload: dict) -> dict:
    url = f"{node['url']}{path}"
    secret = node.get("agent_secret")
    if not secret:
        raise HTTPException(status_code=409, detail="Node is not paired. Re-register or pair it first.")
    headers = {"X-Agent-Secret": secret}
    async with httpx.AsyncClient(timeout=5) as client:
        resp = await client.post(url, json=payload, headers=headers)
    if resp.status_code >= 400:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    return resp.json()


async def _get_agent(node: dict, path: str) -> dict:
    url = f"{node['url']}{path}"
    secret = node.get("agent_secret")
    if not secret:
        raise HTTPException(status_code=409, detail="Node is not paired. Re-register or pair it first.")
    headers = {"X-Agent-Secret": secret}
    async with httpx.AsyncClient(timeout=5) as client:
        resp = await client.get(url, headers=headers)
    if resp.status_code >= 400:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    return resp.json()


async def _assign_webrtc_stream(client_id: str, stream_id: str) -> None:
    if not client_id or not stream_id:
        return
    try:
        await snapcast.set_client_stream(client_id, stream_id)
        return
    except Exception as exc:  # pragma: no cover - network dependency
        if not _is_rpc_method_not_found_error(exc):
            log.warning("Failed to assign web relay client %s to %s: %s", client_id, stream_id, exc)
            raise
        group_id = await _snapcast_group_id_for_client(client_id)
        if not group_id:
            raise RuntimeError(
                f"Snapclient {client_id} is not registered with snapserver yet; cannot switch to {stream_id}"
            )
        try:
            await snapcast.set_group_stream(group_id, stream_id)
        except Exception as group_exc:  # pragma: no cover - network dependency
            raise RuntimeError(
                f"Failed to move snapclient group {group_id} (client {client_id}) to {stream_id}: {group_exc}"
            )


async def _ensure_snapclient_stream(node: dict, channel: dict) -> str:
    if node.get("type") != "agent":
        raise HTTPException(status_code=400, detail="Channel assignment only applies to hardware nodes")
    try:
        clients = await snapcast.list_clients()
    except Exception as exc:  # pragma: no cover - network dependency
        log.exception("Failed to list snapcast clients")
        raise HTTPException(status_code=502, detail=f"Failed to talk to snapserver: {exc}") from exc
    client = _match_snapclient_for_node(node, clients)
    if not client:
        raise HTTPException(status_code=409, detail="Snapclient for this node is offline or unidentified")
    if not client.get("connected"):
        raise HTTPException(status_code=409, detail="Snapclient for this node is offline")
    stream_id = channel.get("snap_stream")
    if not stream_id:
        raise HTTPException(status_code=400, detail="Channel is missing snap_stream mapping")
    if client.get("_stream_id") != stream_id:
        try:
            await snapcast.set_client_stream(client["id"], stream_id)
        except RuntimeError as exc:  # pragma: no cover - network dependency
            if _is_rpc_method_not_found_error(exc):
                group_id = client.get("_group_id")
                if not group_id:
                    log.error("Snapclient %s missing group id; cannot switch stream", client.get("id"))
                    raise HTTPException(status_code=502, detail="Snapclient group unknown; cannot switch stream") from exc
                try:
                    await snapcast.set_group_stream(group_id, stream_id)
                except Exception as group_exc:  # pragma: no cover - network dependency
                    log.exception("Failed to move snapclient group %s to stream %s", group_id, stream_id)
                    raise HTTPException(status_code=502, detail=f"Failed to switch snapclient group stream: {group_exc}") from group_exc
            else:
                log.exception("Failed to move snapclient %s to stream %s", client.get("id"), stream_id)
                raise HTTPException(status_code=502, detail=f"Failed to switch snapclient stream: {exc}") from exc
        except Exception as exc:  # pragma: no cover - network dependency
            log.exception("Failed to move snapclient %s to stream %s", client.get("id"), stream_id)
            raise HTTPException(status_code=502, detail=f"Failed to switch snapclient stream: {exc}") from exc
    return str(client.get("id"))


async def _set_node_channel(node: dict, channel_id: Optional[str]) -> None:
    normalized = (channel_id or "").strip().lower()
    if normalized and normalized not in channels_by_id:
        raise HTTPException(status_code=404, detail="Unknown channel")
    resolved = normalized or None
    current = resolve_node_channel_id(node)
    if resolved == current:
        return
    if not resolved:
        if node.get("type") == "browser" and webrtc_relay:
            await webrtc_relay.update_session_channel(node["id"], None, None)
        if node.get("type") == "sonos":
            ip = _sonos_ip_from_url(node.get("url"))
            if ip:
                async with sonos_reconcile_lock:
                    try:
                        await _sonos_become_standalone(ip)
                    except Exception:
                        pass
                    try:
                        await _sonos_stop(ip)
                    except Exception:
                        pass
            node["connection_state"] = None
            node["connection_error"] = None
            node["sonos_connecting_since"] = None
        node["channel_id"] = None
        return
    channel = channels_by_id[resolved]
    stream_id = channel.get("snap_stream")
    if not stream_id:
        raise HTTPException(status_code=400, detail="Channel is missing snap_stream mapping")
    if node.get("type") == "sonos":
        ip = _sonos_ip_from_url(node.get("url"))
        if not ip:
            raise HTTPException(status_code=400, detail="Invalid Sonos node")
        node["connection_state"] = "connecting"
        node["connection_error"] = None
        node["sonos_connecting_since"] = time.time()
        node["channel_id"] = resolved
        # Reconcile grouping based on updated desired assignments.
        await _reconcile_sonos_groups()
        return
    if node.get("type") == "agent":
        client_id = await _ensure_snapclient_stream(node, channel)
        node["snapclient_id"] = client_id
    elif node.get("type") == "browser" and webrtc_relay:
        await webrtc_relay.update_session_channel(node["id"], resolved, stream_id)
    node["channel_id"] = resolved

async def _refresh_all_agent_nodes() -> None:
    dirty = False
    for node in list(nodes.values()):
        if node.get("type") == "agent":
            _, changed = await refresh_agent_metadata(node, persist=False)
            if changed:
                dirty = True
        elif node.get("type") == "sonos":
            ip = _sonos_ip_from_url(node.get("url"))
            if not ip:
                continue
            now = time.time()
            reachable = await _sonos_ping(ip)
            prev_online = node.get("online") is not False
            node["online"] = bool(reachable)
            if reachable:
                node["last_seen"] = now
                node["offline_since"] = None

                # Best-effort network type detection for UI (Wi-Fi / Ethernet / SonosNet).
                # This relies on undocumented Sonos diagnostics and may vary by firmware.
                last_diag = node.get("sonos_network_last_refresh")
                should_refresh = not isinstance(last_diag, (int, float)) or (now - float(last_diag)) >= 60.0
                if should_refresh:
                    headers = {"User-Agent": SONOS_HTTP_USER_AGENT}
                    try:
                        async with httpx.AsyncClient(timeout=SONOS_CONTROL_TIMEOUT) as client:
                            resp = await client.get(f"http://{ip}:1400/support/review", headers=headers)
                        if resp.status_code == 200:
                            parsed = _parse_sonos_network_from_review(resp.text)
                            if parsed and parsed != node.get("sonos_network"):
                                node["sonos_network"] = parsed
                            node["sonos_network_last_refresh"] = now
                    except Exception:
                        # Never fail the refresh loop on diagnostics.
                        node["sonos_network_last_refresh"] = now
            else:
                if prev_online:
                    node["offline_since"] = now
            dirty = True
    if dirty:
        save_nodes()
        await broadcast_nodes()


async def _sonos_discover() -> list[dict]:
    """Discover Sonos speakers, preferring SSDP and falling back to HTTP scan."""
    items = await _sonos_ssdp_discover()
    if items:
        return items
    networks = _detect_discovery_networks()
    if not networks:
        return []
    log.info("Sonos SSDP returned no devices; falling back to HTTP scan")
    return await _sonos_http_scan(networks)


async def _sonos_http_scan(networks: list[str]) -> list[dict]:
    hosts = _hosts_for_networks(networks, limit=max(1, SONOS_SCAN_MAX_HOSTS))
    if not hosts:
        return []
    sem = asyncio.Semaphore(max(4, SONOS_SCAN_CONCURRENCY))
    found: dict[str, dict] = {}

    async def _probe(ip: str) -> None:
        async with sem:
            desc = await _sonos_fetch_description(ip, timeout=SONOS_SCAN_HTTP_TIMEOUT)
        if not desc:
            return
        udn = desc.get("udn")
        if not desc.get("rincon") and not (isinstance(udn, str) and "RINCON_" in udn):
            return
        found[ip] = desc

    await asyncio.gather(*(_probe(ip) for ip in hosts))
    results: list[dict] = []
    for ip, desc in found.items():
        results.append(
            {
                "kind": "sonos",
                "host": desc.get("friendly_name") or ip,
                "url": f"sonos://{ip}",
                "fingerprint": desc.get("udn"),
                "version": "Sonos",
            }
        )
    results.sort(key=lambda x: (x.get("host") or "", x.get("url") or ""))
    return results


async def _sonos_ssdp_discover() -> list[dict]:
    """Discover Sonos devices using SSDP M-SEARCH."""
    message = (
        "M-SEARCH * HTTP/1.1\r\n"
        f"HOST: {SONOS_SSDP_ADDR[0]}:{SONOS_SSDP_ADDR[1]}\r\n"
        "MAN: \"ssdp:discover\"\r\n"
        "MX: 1\r\n"
        f"ST: {SONOS_DEVICE_TYPE}\r\n"
        "\r\n"
    ).encode("utf-8")
    loop = asyncio.get_running_loop()
    found: dict[str, dict] = {}

    class _Proto(asyncio.DatagramProtocol):
        def connection_made(self, transport):
            self.transport = transport
            try:
                self.transport.sendto(message, SONOS_SSDP_ADDR)
            except Exception:
                pass

        def datagram_received(self, data, addr):
            try:
                text = data.decode("utf-8", errors="ignore")
            except Exception:
                return
            lines = [line.strip() for line in text.split("\r\n") if line.strip()]
            headers = {}
            for line in lines[1:]:
                if ":" not in line:
                    continue
                k, v = line.split(":", 1)
                headers[k.strip().lower()] = v.strip()
            location = headers.get("location")
            if not location:
                return
            host = addr[0]
            found[host] = {"ip": host, "location": location}

    transport = None
    try:
        transport, _ = await loop.create_datagram_endpoint(
            lambda: _Proto(),
            local_addr=("0.0.0.0", 0),
        )
        await asyncio.sleep(max(0.2, SONOS_DISCOVERY_TIMEOUT))
    finally:
        if transport:
            transport.close()

    results: list[dict] = []
    for item in found.values():
        ip = item.get("ip")
        if not ip:
            continue
        desc = await _sonos_fetch_description(ip)
        if not desc:
            continue
        results.append(
            {
                "kind": "sonos",
                "host": desc.get("friendly_name") or ip,
                "url": f"sonos://{ip}",
                "fingerprint": desc.get("udn"),
                "version": "Sonos",
            }
        )
    return results


async def _node_health_loop() -> None:
    try:
        while True:
            await _refresh_all_agent_nodes()
            await asyncio.sleep(max(5, NODE_HEALTH_INTERVAL))
    except asyncio.CancelledError:
        pass


def _channel_should_monitor(channel: dict) -> bool:
    if not channel.get("enabled", True):
        return False
    if not channel.get("snap_stream"):
        return False
    source = (channel.get("source") or "spotify").lower()
    if source == "radio":
        state = _ensure_radio_state(channel)
        return bool(state.get("playback_enabled", True) and state.get("stream_url"))
    return True


def _channel_has_active_hardware_listeners(channel_id: str) -> bool:
    if not channel_id:
        return False
    for node in nodes.values():
        if node.get("type") == "browser":
            continue
        if node.get("channel_id") != channel_id:
            continue
        if node.get("online"):
            return True
    return False


async def _stop_channel_due_to_idle(channel: dict) -> bool:
    cid = channel.get("id") or ""
    if not cid:
        return False
    source = (channel.get("source") or "spotify").lower()
    if source == "radio":
        state = _ensure_radio_state(channel)
        if not state.get("playback_enabled", True):
            return False
        now = int(time.time())
        state["playback_enabled"] = False
        state["updated_at"] = now
        channel["radio_state"] = state
        save_channels()
        _mark_radio_assignments_dirty()
        radio_runtime_status[cid] = {
            "state": "idle",
            "message": "Radio stopped (no listeners)",
            "bitrate": None,
            "station_id": state.get("station_id"),
            "metadata": None,
            "updated_at": now,
            "started_at": None,
        }
        log.info("Auto-stopped radio channel %s after %.0f seconds without listeners", cid, CHANNEL_IDLE_TIMEOUT)
        return True
    try:
        await _spotify_control("/me/player/pause", "PUT", channel_id=cid)
        log.info("Auto-paused Spotify channel %s after %.0f seconds without listeners", cid, CHANNEL_IDLE_TIMEOUT)
        return True
    except HTTPException as exc:
        parsed = _parse_spotify_error(exc.detail)
        reason = (parsed.get("reason") or "").strip().lower()
        message = (parsed.get("message") or "").strip()
        detail_text = message or (exc.detail if isinstance(exc.detail, str) else str(exc.detail))
        if reason == "no_active_device" or (
            isinstance(detail_text, str) and "no active device" in detail_text.lower()
        ):
            log.debug("Auto-stop skipped for Spotify channel %s: no active device", cid)
            return True
        log.warning("Auto-stop failed for Spotify channel %s: %s", cid, detail_text)
        return False
    except Exception as exc:  # pragma: no cover - defensive
        log.exception("Auto-stop failed for Spotify channel %s", cid)
        return False


async def _evaluate_channel_idle() -> None:
    counts, has_data = await _collect_channel_listener_counts()
    if not has_data:
        return
    now = time.time()
    tracked: set[str] = set()
    for cid in channel_order:
        channel = channels_by_id.get(cid)
        if not channel:
            channel_idle_state.pop(cid, None)
            continue
        if not _channel_should_monitor(channel):
            channel_idle_state.pop(cid, None)
            continue
        tracked.add(cid)
        listeners = counts.get(cid, 0)
        if listeners <= 0 and _channel_has_active_hardware_listeners(cid):
            listeners = 1
        state = channel_idle_state.setdefault(cid, {"idle_since": None, "stopped": False})
        if listeners > 0:
            state["idle_since"] = None
            state["stopped"] = False
            state["last_active"] = now
            continue
        if state.get("idle_since") is None:
            state["idle_since"] = now
        idle_for = now - (state.get("idle_since") or now)
        if idle_for < CHANNEL_IDLE_TIMEOUT or state.get("stopped"):
            continue
        stopped = await _stop_channel_due_to_idle(channel)
        if stopped:
            state["stopped"] = True
    for cid in list(channel_idle_state.keys()):
        if cid not in tracked:
            channel_idle_state.pop(cid, None)


async def _channel_idle_loop() -> None:
    try:
        while True:
            try:
                await _evaluate_channel_idle()
            except Exception:
                log.exception("Channel idle monitor iteration failed")
            await asyncio.sleep(CHANNEL_IDLE_POLL_INTERVAL)
    except asyncio.CancelledError:
        pass


@app.get("/api/streams/diagnostics")
async def stream_diagnostics(channel_id: Optional[str] = Query(None)) -> dict:
    if channel_id:
        target_ids = [resolve_channel_id(channel_id)]
    else:
        target_ids = [cid for cid in channel_order if cid in channels_by_id]
    snap_status = None
    snap_error = None
    try:
        snap_status = await snapcast.status()
    except Exception as exc:  # pragma: no cover - network dependency
        snap_error = str(exc)
        log.warning("Stream diagnostics: failed to read snapserver status: %s", exc)
    streams_by_id, clients_by_stream = _summarize_snapserver_status(snap_status)
    webrtc_diag = None
    if webrtc_relay:
        try:
            webrtc_diag = await webrtc_relay.diagnostics()
        except Exception as exc:  # pragma: no cover - defensive
            log.warning("Stream diagnostics: failed to read WebRTC stats: %s", exc)
            webrtc_diag = None
    channel_payloads = []
    node_lookup = {node_id: node for node_id, node in nodes.items()}
    snapclient_nodes = {
        node.get("snapclient_id"): node for node in nodes.values() if node.get("snapclient_id")
    }
    for cid in target_ids:
        channel = channels_by_id.get(cid)
        if not channel:
            continue
        stream_id = channel.get("snap_stream")
        snap_stream = streams_by_id.get(stream_id)
        hardware_clients = [
            _public_snap_client(client, snapclient_nodes)
            for client in clients_by_stream.get(stream_id, [])
        ]
        connected_clients = sum(1 for client in hardware_clients if client.get("connected"))
        channel_webrtc = (webrtc_diag or {}).get("channels", {}).get(cid) if webrtc_diag else None
        if channel_webrtc and channel_webrtc.get("sessions"):
            for session in channel_webrtc["sessions"]:
                node = node_lookup.get(session.get("node_id"))
                if node:
                    session["node_name"] = node.get("name")
                    session["node_type"] = node.get("type")
        spotify_summary = None
        if channel.get("source") == "spotify":
            cfg = read_spotify_config(channel["id"])
            status = read_librespot_status(channel["id"])
            spotify_summary = {
                "bitrate_kbps": cfg.get("bitrate"),
                "device_name": cfg.get("device_name"),
                "normalisation": cfg.get("normalisation"),
                "username": cfg.get("username"),
                "status": status.get("state"),
                "status_message": status.get("message"),
            }
        channel_payloads.append(
            {
                "id": channel["id"],
                "name": channel.get("name"),
                "color": channel.get("color"),
                "source": channel.get("source", "spotify"),
                "snap_stream": stream_id,
                "fifo_path": channel.get("fifo_path"),
                "spotify": spotify_summary,
                "radio_state": channel.get("radio_state") if channel.get("source") == "radio" else None,
                "snapserver_stream": snap_stream,
                "hardware_clients": hardware_clients,
                "listeners": {
                    "hardware": len(hardware_clients),
                    "hardware_connected": connected_clients,
                    "webrtc": channel_webrtc.get("listeners") if channel_webrtc else 0,
                },
                "webrtc": channel_webrtc,
            }
        )
    response = {
        "timestamp": time.time(),
        "channels": channel_payloads,
        "snapserver": {
            "host": SNAPSERVER_HOST,
            "port": SNAPSERVER_PORT,
            "error": snap_error,
        },
    }
    if webrtc_diag:
        response["webrtc"] = {"sample_rate": webrtc_diag.get("sample_rate")}
    return response

async def _probe_host(host: str) -> Optional[dict]:
    url = f"http://{host}:{AGENT_PORT}"
    try:
        async with httpx.AsyncClient(timeout=2) as client:
            resp = await client.get(f"{url}/health")
        if resp.status_code == 200:
            try:
                data = resp.json()
            except ValueError:
                data = {}
            return {
                "host": host,
                "url": f"{url}",
                "healthy": True,
                "version": data.get("version"),
                "fingerprint": data.get("fingerprint"),
            }
    except Exception:
        return None
    return None


def _configured_discovery_networks() -> list[str]:
    configured: list[str] = []
    raw_value = DISCOVERY_CIDR or ""
    if not raw_value:
        return configured
    for chunk in raw_value.replace(";", ",").split(","):
        part = chunk.strip()
        if not part:
            continue
        try:
            net = str(ipaddress.ip_network(part, strict=False))
        except ValueError:
            log.warning("Ignoring invalid DISCOVERY_CIDR entry: %s", part)
            continue
        if net not in configured:
            configured.append(net)
    return configured


def _detect_discovery_networks() -> list[str]:
    """Return IPv4 networks to scan, defaulting to DISCOVERY_CIDR."""
    networks: list[str] = _configured_discovery_networks()
    try:
        output = subprocess.check_output(
            ["ip", "-o", "-4", "addr", "show", "up", "scope", "global"],
            text=True,
        )
    except Exception:
        output = ""
    for line in output.splitlines():
        parts = line.split()
        if len(parts) < 4:
            continue
        cidr = parts[3]
        try:
            iface = ipaddress.ip_interface(cidr)
        except ValueError:
            continue
        if iface.ip.is_loopback or iface.ip.is_link_local:
            continue
        net = iface.network
        if net.num_addresses <= 2:
            continue
        if net.version != 4:
            continue
        _net_str = str(net)
        if _net_str not in networks:
            networks.append(_net_str)
    if not networks:
        return []
    return networks


def _hosts_for_networks(networks: list[str], limit: int = DISCOVERY_MAX_HOSTS) -> list[str]:
    seen: set[str] = set()
    hosts: list[str] = []
    for net_str in networks:
        try:
            net = ipaddress.ip_network(net_str, strict=False)
        except ValueError:
            continue
        if net.version != 4:
            continue
        for host in net.hosts():
            host_str = str(host)
            if host_str in seen:
                continue
            seen.add(host_str)
            hosts.append(host_str)
            if len(hosts) >= limit:
                return hosts
    return hosts


async def _stream_host_probes(hosts: list[str]) -> AsyncIterator[dict]:
    if not hosts:
        return
    sem = asyncio.Semaphore(max(1, DISCOVERY_CONCURRENCY))
    tasks = []

    async def _runner(target: str) -> Optional[dict]:
        async with sem:
            return await _probe_host(target)

    for host in hosts:
        tasks.append(asyncio.create_task(_runner(host)))

    try:
        for task in asyncio.as_completed(tasks):
            result = await task
            if result:
                yield result
    finally:
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)


async def _find_node_by_fingerprint(fingerprint: str) -> Optional[dict]:
    if not fingerprint:
        return None
    networks = _detect_discovery_networks()
    if not networks:
        return None
    hosts = _hosts_for_networks(networks)
    if not hosts:
        return None
    async for result in _stream_host_probes(hosts):
        if not result:
            continue
        if result.get("fingerprint") == fingerprint:
            return result
    return None


async def _rediscover_node(node_id: str, fingerprint: str) -> None:
    try:
        interval = max(10, NODE_REDISCOVERY_INTERVAL)
        while True:
            node = nodes.get(node_id)
            if not node or node.get("online"):
                return
            match = await _find_node_by_fingerprint(fingerprint)
            if match and match.get("url"):
                new_url = _normalize_node_url(match["url"])
                if new_url:
                    node["url"] = new_url
                    save_nodes()
                    reachable, _ = await refresh_agent_metadata(node)
                    if reachable:
                        log.info("Node %s rediscovered at %s", node_id, new_url)
                        await broadcast_nodes()
                        return
            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        pass
    finally:
        node_rediscovery_tasks.pop(node_id, None)



def _abs_settings() -> dict:
    settings = get_provider_settings("audiobookshelf")
    if not isinstance(settings, dict):
        return {}
    return settings


def _abs_base_url() -> str:
    raw = (_abs_settings().get("base_url") or "").strip()
    return raw.rstrip("/")


def _abs_token() -> str:
    return (_abs_settings().get("token") or "").strip()


def _abs_library_id_setting() -> Optional[str]:
    raw = (_abs_settings().get("library_id") or "").strip()
    return raw or None


def _require_abs_configured() -> tuple[str, str]:
    require_audiobookshelf_provider()
    base_url = _abs_base_url()
    token = _abs_token()
    if not base_url or not token:
        raise HTTPException(status_code=400, detail="Audiobookshelf provider is not configured")
    return base_url, token


def _abs_headers(token: str) -> dict:
    return {
        "Accept": "application/json",
        "Authorization": f"Bearer {token}",
    }


async def _abs_request(method: str, url: str, *, token: str, params: Optional[dict] = None, json_body: Any = None) -> Any:
    timeout = httpx.Timeout(ABS_HTTP_TIMEOUT)
    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.request(method.upper(), url, headers=_abs_headers(token), params=params, json=json_body)
        if resp.status_code >= 400:
            detail = None
            try:
                detail = resp.json()
            except Exception:
                detail = resp.text
            raise HTTPException(status_code=502, detail=f"Audiobookshelf error ({resp.status_code}): {detail}")
        try:
            return resp.json()
        except Exception:
            raise HTTPException(status_code=502, detail="Audiobookshelf returned invalid JSON")


async def _abs_list_libraries(base_url: str, token: str) -> list[dict]:
    data = await _abs_request("GET", f"{base_url}/api/libraries", token=token)
    libs = []
    for item in (data or []):
        if not isinstance(item, dict):
            continue
        libs.append(item)
    return libs


def _abs_is_podcast_library(entry: dict) -> bool:
    media_type = (entry.get("mediaType") or entry.get("media_type") or "").strip().lower()
    return media_type == "podcast"


async def _abs_resolve_podcast_library_id(base_url: str, token: str, preferred: Optional[str]) -> str:
    libs = await _abs_list_libraries(base_url, token)
    podcast_libs = [lib for lib in libs if isinstance(lib, dict) and _abs_is_podcast_library(lib)]
    if preferred and any((lib.get("id") == preferred) for lib in podcast_libs):
        return preferred
    if podcast_libs:
        lid = (podcast_libs[0].get("id") or "").strip()
        if lid:
            return lid
    raise HTTPException(status_code=404, detail="No podcast libraries found in Audiobookshelf")


def _abs_pick(obj: dict, *keys: str) -> Optional[Any]:
    for key in keys:
        if key in obj:
            return obj.get(key)
    return None


async def _abs_list_podcast_items(base_url: str, token: str, library_id: str) -> list[dict]:
    data = await _abs_request("GET", f"{base_url}/api/libraries/{library_id}/items", token=token)
    if isinstance(data, dict):
        items = data.get("items") or data.get("results") or []
    else:
        items = data or []
    if not isinstance(items, list):
        return []
    normalized = []
    for item in items:
        if not isinstance(item, dict):
            continue
        library_item_id = _abs_pick(item, "id", "libraryItemId", "library_item_id")
        if not library_item_id:
            continue
        media = item.get("media") if isinstance(item.get("media"), dict) else {}
        title = (
            (item.get("title") or item.get("name") or _abs_pick(media, "title") or "").strip()
        )
        author = (item.get("author") or _abs_pick(media, "author") or "").strip() or None
        image = item.get("imagePath") or item.get("image") or item.get("cover") or None
        normalized.append({
            "id": str(library_item_id),
            "title": title or str(library_item_id),
            "author": author,
            "image": image,
        })
    normalized.sort(key=lambda x: (x.get("title") or "").lower())
    return normalized


async def _abs_fetch_item_expanded(base_url: str, token: str, library_item_id: str) -> dict:
    params = {"expanded": "1"}
    data = await _abs_request("GET", f"{base_url}/api/items/{library_item_id}", token=token, params=params)
    return data if isinstance(data, dict) else {}


async def _abs_fetch_episode_progress(base_url: str, token: str, library_item_id: str, episode_id: str) -> Optional[dict]:
    timeout = httpx.Timeout(ABS_HTTP_TIMEOUT)
    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.get(
            f"{base_url}/api/me/progress/{library_item_id}/{episode_id}",
            headers=_abs_headers(token),
        )
        # Treat missing/unsupported progress endpoints as "unknown".
        if resp.status_code in {404, 405}:
            return None
        if resp.status_code >= 400:
            detail = None
            try:
                detail = resp.json()
            except Exception:
                detail = resp.text
            raise HTTPException(status_code=502, detail=f"Audiobookshelf error ({resp.status_code}): {detail}")
        try:
            data = resp.json()
        except Exception:
            return None
        return data if isinstance(data, dict) else None


async def _abs_list_episodes(
    base_url: str,
    token: str,
    library_item_id: str,
    *,
    show_played: bool,
) -> list[dict]:
    data = await _abs_fetch_item_expanded(base_url, token, library_item_id)
    media = data.get("media") if isinstance(data.get("media"), dict) else {}
    episodes_raw = media.get("episodes") if isinstance(media.get("episodes"), list) else []
    episodes = []
    for entry in episodes_raw:
        if not isinstance(entry, dict):
            continue
        episode_id = _abs_pick(entry, "id", "episodeId", "episode_id")
        if not episode_id:
            continue
        title = (entry.get("title") or entry.get("name") or "").strip() or str(episode_id)
        published_at = _abs_pick(entry, "publishedAt", "published_at")
        try:
            published_at_num = int(published_at) if published_at is not None else None
        except (TypeError, ValueError):
            published_at_num = None
        duration = _abs_pick(entry, "duration", "durationMs", "duration_ms")
        try:
            duration_num = int(duration) if duration is not None else None
        except (TypeError, ValueError):
            duration_num = None
        episodes.append({
            "id": str(episode_id),
            "title": title,
            "published_at": published_at_num,
            "duration_ms": duration_num,
            "finished": None,
        })

    # Oldest -> newest (default requested).
    episodes.sort(key=lambda e: (e.get("published_at") is None, e.get("published_at") or 0, e.get("title") or ""))

    sem = asyncio.Semaphore(10)

    async def _hydrate_finished(ep: dict) -> None:
        async with sem:
            progress = await _abs_fetch_episode_progress(base_url, token, library_item_id, ep["id"])
        if isinstance(progress, dict) and "isFinished" in progress:
            ep["finished"] = bool(progress.get("isFinished"))
        elif isinstance(progress, dict) and "is_finished" in progress:
            ep["finished"] = bool(progress.get("is_finished"))
        else:
            ep["finished"] = False

    await asyncio.gather(*(_hydrate_finished(ep) for ep in episodes), return_exceptions=False)

    if show_played:
        return episodes
    return [ep for ep in episodes if not ep.get("finished")]


async def _abs_get_stream_url(base_url: str, token: str, library_item_id: str, episode_id: str) -> str:
    data = await _abs_request("POST", f"{base_url}/api/items/{library_item_id}/play/{episode_id}", token=token)
    if isinstance(data, dict):
        tracks = data.get("audioTracks")
        if isinstance(tracks, list) and tracks:
            first = tracks[0] if isinstance(tracks[0], dict) else {}
            url = (first.get("contentUrl") or first.get("content_url") or "").strip()
            if url:
                return url
        url = (data.get("contentUrl") or data.get("content_url") or "").strip()
        if url:
            return url
    raise HTTPException(status_code=502, detail="Audiobookshelf did not return a playable URL")


async def _abs_mark_finished_best_effort(base_url: str, token: str, library_item_id: str, episode_id: str) -> None:
    # Best-effort: API versions vary; do not fail playback advancement if this isn't supported.
    try:
        await _abs_request(
            "PATCH",
            f"{base_url}/api/me/progress/{library_item_id}/{episode_id}",
            token=token,
            json_body={"isFinished": True},
        )
    except HTTPException:
        try:
            await _abs_request(
                "POST",
                f"{base_url}/api/me/progress/{library_item_id}/{episode_id}",
                token=token,
                json_body={"isFinished": True},
            )
        except HTTPException:
            return


def _get_abs_channel_or_404(channel_id: str) -> dict:
    channel = get_channel(channel_id)
    if (channel.get("source") or "").strip().lower() != "audiobookshelf":
        raise HTTPException(status_code=400, detail="Channel is not an audiobookshelf channel")
    return channel


@app.get("/api/audiobookshelf/libraries")
async def list_abs_libraries() -> dict:
    base_url, token = _require_abs_configured()
    libs = await _abs_list_libraries(base_url, token)
    podcast_libs = []
    for lib in libs:
        if not isinstance(lib, dict) or not _abs_is_podcast_library(lib):
            continue
        podcast_libs.append({
            "id": lib.get("id"),
            "name": lib.get("name"),
            "mediaType": lib.get("mediaType"),
        })
    return {"libraries": podcast_libs}


@app.get("/api/audiobookshelf/podcasts")
async def list_abs_podcasts(library_id: Optional[str] = Query(default=None)) -> dict:
    base_url, token = _require_abs_configured()
    resolved_library = await _abs_resolve_podcast_library_id(base_url, token, library_id or _abs_library_id_setting())
    items = await _abs_list_podcast_items(base_url, token, resolved_library)
    return {"library_id": resolved_library, "podcasts": items}


@app.get("/api/audiobookshelf/podcasts/{library_item_id}/episodes")
async def list_abs_episodes(
    library_item_id: str,
    show_played: bool = Query(default=False),
) -> dict:
    base_url, token = _require_abs_configured()
    episodes = await _abs_list_episodes(base_url, token, library_item_id, show_played=show_played)
    return {"library_item_id": library_item_id, "episodes": episodes, "show_played": bool(show_played)}


class AudiobookshelfPlayPayload(BaseModel):
    library_item_id: str = Field(..., min_length=1)
    episode_id: str = Field(..., min_length=1)
    podcast_title: Optional[str] = None
    episode_title: Optional[str] = None


@app.post("/api/audiobookshelf/{channel_id}/play")
async def audiobookshelf_play_episode(channel_id: str, payload: AudiobookshelfPlayPayload) -> dict:
    require_audiobookshelf_provider()
    resolved = resolve_channel_id(channel_id)
    channel = _get_abs_channel_or_404(resolved)
    state = _normalize_abs_state(channel.get("abs_state"))
    state.update({
        "library_item_id": payload.library_item_id,
        "episode_id": payload.episode_id,
        "podcast_title": (payload.podcast_title or state.get("podcast_title")),
        "episode_title": (payload.episode_title or state.get("episode_title")),
        "updated_at": int(time.time()),
        "playback_enabled": True,
        "content_url": None,
        "content_url_ts": None,
    })
    channel["abs_state"] = state
    save_channels()
    _mark_abs_assignments_dirty()
    abs_runtime_status.pop(resolved, None)
    return {"ok": True, "channel": channel_detail(resolved)}


class AudiobookshelfPlaybackPayload(BaseModel):
    action: str = Field(pattern="^(start|stop)$")


@app.post("/api/audiobookshelf/{channel_id}/playback")
async def audiobookshelf_playback_toggle(channel_id: str, payload: AudiobookshelfPlaybackPayload) -> dict:
    require_audiobookshelf_provider()
    resolved = resolve_channel_id(channel_id)
    channel = _get_abs_channel_or_404(resolved)
    state = _normalize_abs_state(channel.get("abs_state"))
    desired = payload.action == "start"
    if desired and not (state.get("library_item_id") and state.get("episode_id")):
        raise HTTPException(status_code=400, detail="Select an episode before starting playback")
    state["playback_enabled"] = desired
    state["updated_at"] = int(time.time())
    channel["abs_state"] = state
    save_channels()
    _mark_abs_assignments_dirty()
    if not desired:
        abs_runtime_status[resolved] = {
            "state": "idle",
            "message": "Playback stopped",
            "updated_at": state["updated_at"],
            "started_at": None,
        }
    else:
        abs_runtime_status.pop(resolved, None)
    return {"ok": True}


@app.get("/api/audiobookshelf/status/{channel_id}")
async def audiobookshelf_status(channel_id: str) -> dict:
    require_audiobookshelf_provider()
    resolved = resolve_channel_id(channel_id)
    channel = _get_abs_channel_or_404(resolved)
    state = _normalize_abs_state(channel.get("abs_state"))
    channel["abs_state"] = state
    runtime = _abs_runtime_payload(resolved)
    return {
        "channel_id": resolved,
        "abs_state": state,
        "runtime": runtime,
        "enabled": channel.get("enabled", True),
    }


@app.get("/api/audiobookshelf/worker/assignments")
async def audiobookshelf_worker_assignments(
    request: Request,
    since: Optional[int] = Query(None),
    wait: Optional[float] = Query(None),
) -> dict:
    require_audiobookshelf_provider()
    _require_abs_worker_token(request)
    timeout = ABS_ASSIGNMENT_DEFAULT_WAIT if wait is None else float(wait)
    timeout = max(1.0, min(timeout, ABS_ASSIGNMENT_MAX_WAIT))
    version = abs_assignments_version
    if since is not None:
        version = await _wait_for_abs_assignments_change(since, timeout)

    base_url = _abs_base_url()
    token = _abs_token()
    assignments = []
    for cid in channel_order:
        channel = channels_by_id.get(cid)
        if not channel or (channel.get("source") or "").strip().lower() != "audiobookshelf":
            continue
        state = _normalize_abs_state(channel.get("abs_state"))
        channel["abs_state"] = state
        enabled = channel.get("enabled", True)
        playback_enabled = state.get("playback_enabled", True)
        library_item_id = (state.get("library_item_id") or "").strip() if isinstance(state.get("library_item_id"), str) else state.get("library_item_id")
        episode_id = (state.get("episode_id") or "").strip() if isinstance(state.get("episode_id"), str) else state.get("episode_id")
        stream_url = None
        if enabled and playback_enabled and library_item_id and episode_id and base_url and token:
            # Create/refresh a playback session URL when assignments are requested.
            stream_url = await _abs_get_stream_url(base_url, token, str(library_item_id), str(episode_id))
        assignments.append({
            "channel_id": cid,
            "enabled": enabled,
            "snap_stream": channel.get("snap_stream"),
            "fifo_path": channel.get("fifo_path"),
            "stream_url": stream_url,
            "token": token if token else None,
            "library_item_id": library_item_id,
            "episode_id": episode_id,
            "playback_enabled": playback_enabled,
            "updated_at": state.get("updated_at"),
        })
    return {"assignments": assignments, "version": version}


class AudiobookshelfWorkerStatusPayload(BaseModel):
    state: str = Field(pattern="^(playing|connecting|error|idle)$")
    message: Optional[str] = None


@app.post("/api/audiobookshelf/worker/status/{channel_id}")
async def audiobookshelf_worker_status(channel_id: str, payload: AudiobookshelfWorkerStatusPayload, request: Request) -> dict:
    require_audiobookshelf_provider()
    _require_abs_worker_token(request)
    resolved = resolve_channel_id(channel_id)
    channel = _get_abs_channel_or_404(resolved)
    previous = abs_runtime_status.get(resolved) or {}
    prev_state = previous.get("state")
    prev_started_raw = previous.get("started_at")
    prev_started = int(prev_started_raw) if isinstance(prev_started_raw, (int, float)) else None
    now = int(time.time())
    started_at = None
    if payload.state == "playing":
        if prev_state == "playing" and prev_started:
            started_at = prev_started
        else:
            started_at = now
    status_payload = {
        "state": payload.state,
        "message": payload.message,
        "updated_at": now,
        "started_at": started_at if payload.state == "playing" else None,
    }
    abs_runtime_status[resolved] = status_payload
    return {"ok": True}


class AudiobookshelfEndedPayload(BaseModel):
    library_item_id: Optional[str] = None
    episode_id: Optional[str] = None


@app.post("/api/audiobookshelf/worker/ended/{channel_id}")
async def audiobookshelf_worker_ended(channel_id: str, payload: AudiobookshelfEndedPayload, request: Request) -> dict:
    require_audiobookshelf_provider()
    _require_abs_worker_token(request)
    resolved = resolve_channel_id(channel_id)
    channel = _get_abs_channel_or_404(resolved)
    state = _normalize_abs_state(channel.get("abs_state"))
    channel["abs_state"] = state
    library_item_id = payload.library_item_id or state.get("library_item_id")
    episode_id = payload.episode_id or state.get("episode_id")
    if not (library_item_id and episode_id):
        return {"ok": True, "advanced": False}
    base_url = _abs_base_url()
    token = _abs_token()
    if base_url and token:
        await _abs_mark_finished_best_effort(base_url, token, str(library_item_id), str(episode_id))

    # Advance to next episode (oldest -> newest, skipping finished).
    advanced = False
    if base_url and token:
        episodes = await _abs_list_episodes(base_url, token, str(library_item_id), show_played=True)
        ids = [ep.get("id") for ep in episodes if ep.get("id")]
        try:
            current_idx = ids.index(str(episode_id))
        except ValueError:
            current_idx = -1
        next_ep = None
        for candidate in episodes[current_idx + 1:]:
            if not candidate.get("id"):
                continue
            if candidate.get("finished"):
                continue
            next_ep = candidate
            break
        if next_ep:
            state.update({
                "episode_id": next_ep.get("id"),
                "episode_title": next_ep.get("title"),
                "updated_at": int(time.time()),
                "playback_enabled": True,
                "content_url": None,
                "content_url_ts": None,
            })
            channel["abs_state"] = state
            save_channels()
            _mark_abs_assignments_dirty()
            advanced = True
        else:
            # No more episodes; stop playback.
            state["playback_enabled"] = False
            state["updated_at"] = int(time.time())
            channel["abs_state"] = state
            save_channels()
            _mark_abs_assignments_dirty()
    return {"ok": True, "advanced": advanced}


@app.post("/api/playback/stop-all")
async def stop_all_channel_playback() -> dict:
    now = int(time.time())
    spotify_stopped: list[str] = []
    radio_stopped: list[str] = []
    radio_states: dict[str, dict] = {}
    errors: dict[str, str] = {}
    radio_mutated = False
    for cid in channel_order:
        channel = channels_by_id.get(cid)
        if not channel:
            continue
        source = (channel.get("source") or "spotify").lower()
        if source == "radio":
            require_radio_provider()
            state = _ensure_radio_state(channel)
            if state.get("playback_enabled", True):
                state["playback_enabled"] = False
                state["updated_at"] = now
                channel["radio_state"] = state
                radio_runtime_status[cid] = {
                    "state": "idle",
                    "message": "Radio stopped",
                    "bitrate": None,
                    "station_id": state.get("station_id"),
                    "metadata": None,
                    "updated_at": now,
                    "started_at": None,
                }
                radio_states[cid] = state
                radio_stopped.append(cid)
                radio_mutated = True
            continue
        require_spotify_provider()
        try:
            await _spotify_control("/me/player/pause", "PUT", channel_id=cid)
            spotify_stopped.append(cid)
        except HTTPException as exc:
            raw_detail = exc.detail
            parsed = _parse_spotify_error(raw_detail)
            reason = (parsed.get("reason") or "").strip().lower()
            message = (parsed.get("message") or "").strip()
            detail_text = message or (raw_detail if isinstance(raw_detail, str) else str(raw_detail))
            if reason == "no_active_device" or "no active device" in detail_text.lower():
                log.debug("Skipping Spotify pause for %s: no active device", cid)
                continue
            errors[cid] = detail_text or "Failed to pause Spotify playback"
        except Exception as exc:  # pragma: no cover - defensive
            errors[cid] = str(exc)
    if radio_mutated:
        save_channels()
        _mark_radio_assignments_dirty()
    return {
        "ok": True,
        "spotify_stopped": spotify_stopped,
        "radio_stopped": radio_stopped,
        "radio_states": radio_states,
        "errors": errors,
    }


@app.exception_handler(HTTPException)
async def http_exception_handler(_, exc: HTTPException):
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})


@app.get("/", include_in_schema=False)
async def serve_index():
    index_path = STATIC_DIR / "index.html"
    if not index_path.exists():
        raise HTTPException(status_code=404, detail="UI not found")
    return FileResponse(index_path)


@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    # No asset yet; avoid 404 noise
    return Response(status_code=204)


@app.get("/web-node", include_in_schema=False)
async def serve_web_node():
    path = STATIC_DIR / "web-node.html"
    if not path.exists():
        raise HTTPException(status_code=404, detail="UI not found")
    return FileResponse(path)
