import json
import asyncio
import ipaddress
import logging
import os
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
TRANSIENT_NODE_FIELDS = {"wifi"}
AGENT_LATEST_VERSION = os.getenv("AGENT_LATEST_VERSION", "0.3.21").strip()
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
RADIO_BROWSER_BASE_URL = os.getenv("RADIO_BROWSER_BASE_URL", "https://de1.api.radio-browser.info/json").rstrip("/")
RADIO_BROWSER_TIMEOUT = float(os.getenv("RADIO_BROWSER_TIMEOUT", "8"))
RADIO_BROWSER_CACHE_TTL = int(os.getenv("RADIO_BROWSER_CACHE_TTL", "300"))
RADIO_BROWSER_USER_AGENT = os.getenv("RADIO_BROWSER_USER_AGENT", "RoomCast/Radio").strip() or "RoomCast/Radio"
RADIO_WORKER_TOKEN = os.getenv("RADIO_WORKER_TOKEN", "").strip()
RADIO_WORKER_PATH_PREFIX = "/api/radio/worker"
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

channels_by_id: Dict[str, dict] = {}
channel_order: list[str] = []
player_snapshots: Dict[str, dict] = {}
player_snapshot_lock = threading.Lock()


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
    source = (entry.get("source") or "spotify").strip().lower()
    if source not in {"spotify", "radio"}:
        source = "spotify"
    radio_state = None
    if source == "radio":
        radio_state = _normalize_radio_state(entry.get("radio_state"))
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
        "radio_state": radio_state,
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


def _default_channel_entries() -> list[dict]:
    defaults = [
        {
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
            "source": "spotify",
        },
        {
            "id": f"{CHANNEL_ID_PREFIX}2",
            "name": "Channel 2",
            "order": 2,
            "snap_stream": "Spotify_CH2",
            "fifo_path": "/tmp/snapfifo-ch2",
            "config_path": "/config/spotify-ch2.json",
            "token_path": "/config/spotify-token-ch2.json",
            "status_path": "/config/librespot-status-ch2.json",
            "color": "#0ea5e9",
            "enabled": True,
            "source": "spotify",
        },
    ]
    defaults.extend(_radio_channel_templates())
    return defaults


def _radio_channel_templates() -> list[dict]:
    templates: list[dict] = []
    for idx, slot in enumerate(RADIO_CHANNEL_SLOTS, start=1):
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
    desired_radio_count = len(RADIO_CHANNEL_SLOTS)
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


def _hydrate_channels(raw_entries: list[Any]) -> list[dict]:
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


def _apply_radio_station(channel: dict, payload: "RadioStationSelectionPayload") -> dict:
    state = _default_radio_state()
    state.update({
        "station_id": payload.station_id,
        "station_name": payload.name,
        "stream_url": payload.stream_url,
        "station_country": payload.country,
        "station_countrycode": payload.countrycode,
        "station_favicon": payload.favicon,
        "station_homepage": payload.homepage,
        "bitrate": payload.bitrate,
        "tags": payload.tags or [],
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


def load_channels() -> None:
    global channels_by_id, channel_order
    entries: list[Any] = []
    if CHANNELS_PATH.exists():
        try:
            entries = json.loads(CHANNELS_PATH.read_text()) or []
        except json.JSONDecodeError:
            log.warning("channels.json is invalid; regenerating defaults")
    normalized = _hydrate_channels(entries)
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
            "source": entry.get("source", "spotify"),
            "radio_state": entry.get("radio_state"),
        })
    return items


load_channels()


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
        "source": entry.get("source", "spotify"),
        "radio_state": entry.get("radio_state"),
        "spotify": read_spotify_config(entry["id"]),
        "librespot_status": read_librespot_status(entry["id"]),
    }
    return data


def all_channel_details() -> list[dict]:
    return [channel_detail(cid) for cid in channel_order if cid in channels_by_id]


def update_channel_metadata(channel_id: str, updates: dict) -> dict:
    channel = get_channel(channel_id)
    is_radio_channel = channel.get("source") == "radio"
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
    if "enabled" in updates:
        channel["enabled"] = bool(updates.get("enabled"))
    if "order" in updates:
        try:
            channel["order"] = max(1, int(updates["order"]))
        except (TypeError, ValueError):
            raise HTTPException(status_code=400, detail="Order must be a positive integer")
    save_channels()
    if is_radio_channel:
        _mark_radio_assignments_dirty()
    return channel


class NodeRegistration(BaseModel):
    id: Optional[str] = None
    name: str
    url: str = Field(min_length=1)
    fingerprint: Optional[str] = None


class RenameNodePayload(BaseModel):
    name: str = Field(min_length=1, max_length=100)


class VolumePayload(BaseModel):
    percent: int = Field(ge=0, le=100)


class ShufflePayload(BaseModel):
    state: bool = Field(description="Enable shuffle when true")
    device_id: Optional[str] = Field(default=None, description="Target device ID")


class RepeatPayload(BaseModel):
    mode: str = Field(pattern="^(off|track|context)$", description="Repeat mode")
    device_id: Optional[str] = Field(default=None, description="Target device ID")


class ActivateRoomcastPayload(BaseModel):
    play: bool = Field(default=False, description="Start playback immediately after transfer")


class EqBand(BaseModel):
    freq: float = Field(gt=10, lt=24000, description="Frequency in Hz")
    gain: float = Field(ge=-24, le=24, description="Gain in dB")
    q: float = Field(default=1.0, gt=0.05, lt=36, description="Quality factor")


class EqPayload(BaseModel):
    preset: Optional[str] = None
    bands: list[EqBand] = Field(default_factory=list)
    band_count: int = Field(default=15, ge=1, le=31)


class OutputSelectionPayload(BaseModel):
    device: str = Field(min_length=1)


class WebNodeOffer(BaseModel):
    name: str = Field(default="Web node", min_length=1)
    sdp: str
    type: str


class WebNodeRequestDecisionPayload(BaseModel):
    action: Literal["approve", "deny"]
    reason: Optional[str] = Field(default=None, max_length=200)


class NodeChannelPayload(BaseModel):
    channel_id: Optional[str] = Field(default=None, max_length=120)


class SpotifyConfig(BaseModel):
    device_name: str = Field(default="RoomCast")
    bitrate: int = Field(default=320, ge=96, le=320)
    initial_volume: int = Field(default=75, ge=0, le=100)
    normalisation: bool = Field(default=True)
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    redirect_uri: Optional[str] = None


class InitializePayload(BaseModel):
    server_name: str = Field(default="RoomCast", min_length=1, max_length=120)
    username: str = Field(min_length=1, max_length=60)
    password: str = Field(min_length=4, max_length=128)


class LoginPayload(BaseModel):
    username: str = Field(min_length=1, max_length=60)
    password: str = Field(min_length=1, max_length=128)


class CreateUserPayload(BaseModel):
    username: str = Field(min_length=1, max_length=60)
    password: str = Field(min_length=4, max_length=128)
    role: str = Field(pattern="^(admin|member)$")


class UpdateUserPayload(BaseModel):
    username: Optional[str] = Field(default=None, min_length=1, max_length=60)
    password: Optional[str] = Field(default=None, min_length=4, max_length=128)
    role: Optional[str] = Field(default=None, pattern="^(admin|member)$")


class ServerNamePayload(BaseModel):
    server_name: str = Field(min_length=1, max_length=120)


class ChannelUpdatePayload(BaseModel):
    name: Optional[str] = Field(default=None, min_length=1, max_length=120)
    color: Optional[str] = Field(default=None, min_length=1, max_length=7)
    order: Optional[int] = Field(default=None, ge=1, le=50)
    snap_stream: Optional[str] = Field(default=None, min_length=1, max_length=160)
    enabled: Optional[bool] = None


class RadioStationSelectionPayload(BaseModel):
    station_id: str = Field(min_length=1, max_length=160)
    name: str = Field(min_length=1, max_length=200)
    stream_url: str = Field(min_length=1, max_length=500)
    country: Optional[str] = Field(default=None, max_length=120)
    countrycode: Optional[str] = Field(default=None, max_length=4)
    bitrate: Optional[int] = Field(default=None, ge=0, le=1536)
    favicon: Optional[str] = Field(default=None, max_length=500)
    homepage: Optional[str] = Field(default=None, max_length=500)
    tags: Optional[List[str]] = None


class RadioWorkerStatusPayload(BaseModel):
    state: str = Field(pattern="^(idle|connecting|buffering|playing|error)$")
    message: Optional[str] = Field(default=None, max_length=500)
    bitrate: Optional[int] = Field(default=None, ge=0, le=1536)
    metadata: Optional[dict] = None
    station_id: Optional[str] = Field(default=None, max_length=160)


class RadioPlaybackPayload(BaseModel):
    action: Literal["start", "stop"]


class SnapcastClient:
    def __init__(self, host: str, port: int = 1780) -> None:
        self.url = f"ws://{host}:{port}/jsonrpc"

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
        params = {"id": client_id, "stream_id": stream_id}
        return await self._rpc("Client.SetStream", params)

    async def set_group_stream(self, group_id: str, stream_id: str) -> dict:
        params = {"id": group_id, "stream_id": stream_id}
        return await self._rpc("Group.SetStream", params)


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


@app.middleware("http")
async def session_middleware(request: Request, call_next):
    path = request.url.path
    token = request.cookies.get(SESSION_COOKIE_NAME)
    request.state.user = _resolve_session_user(token)
    is_worker_endpoint = path.startswith(RADIO_WORKER_PATH_PREFIX)
    requires_auth = path.startswith("/api/") and (not is_worker_endpoint) and path not in PUBLIC_API_PATHS
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
radio_browser_cache: Dict[str, Tuple[float, Any]] = {}
radio_runtime_status: Dict[str, dict] = {}
radio_assignments_version = 1
radio_assignment_waiters: set[asyncio.Future] = set()
RADIO_ASSIGNMENT_DEFAULT_WAIT = float(os.getenv("RADIO_ASSIGNMENT_WAIT", "10"))
RADIO_ASSIGNMENT_DEFAULT_WAIT = max(1.0, min(RADIO_ASSIGNMENT_DEFAULT_WAIT, 30.0))
RADIO_ASSIGNMENT_MAX_WAIT = 30.0
channel_idle_task: Optional[asyncio.Task] = None
channel_idle_state: Dict[str, dict] = {}
pending_web_node_requests: Dict[str, dict] = {}


def _mark_radio_assignments_dirty() -> None:
    global radio_assignments_version
    radio_assignments_version += 1
    waiters = list(radio_assignment_waiters)
    radio_assignment_waiters.clear()
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


def default_eq_state() -> dict:
    return {"preset": DEFAULT_EQ_PRESET, "band_count": 15, "bands": []}


def _make_radio_cache_key(path: str, params: Optional[dict]) -> str:
    if not params:
        return path
    try:
        serialized = json.dumps(sorted((params or {}).items()), separators=(",", ":"))
    except TypeError:
        serialized = json.dumps(str(params), ensure_ascii=False)
    return f"{path}?{serialized}"


def _radio_cache_get(key: str) -> Optional[Any]:
    if not key:
        return None
    cached = radio_browser_cache.get(key)
    if not cached:
        return None
    expires_at, payload = cached
    if expires_at and expires_at < time.time():
        radio_browser_cache.pop(key, None)
        return None
    return payload


def _radio_cache_put(key: str, payload: Any, ttl: int) -> None:
    if not key or ttl <= 0:
        return
    radio_browser_cache[key] = (time.time() + ttl, payload)


async def _radio_browser_request(path: str, params: Optional[dict] = None, ttl: Optional[int] = None, cache_key: Optional[str] = None) -> Any:
    resolved_path = path.lstrip("/")
    ttl_value = RADIO_BROWSER_CACHE_TTL if ttl is None else ttl
    key = cache_key or _make_radio_cache_key(resolved_path, params)
    if ttl_value and key:
        cached = _radio_cache_get(key)
        if cached is not None:
            return cached
    url = f"{RADIO_BROWSER_BASE_URL}/{resolved_path}"
    headers = {"User-Agent": RADIO_BROWSER_USER_AGENT}
    try:
        async with httpx.AsyncClient(timeout=RADIO_BROWSER_TIMEOUT) as client:
            response = await client.get(url, params=params, headers=headers)
            response.raise_for_status()
            payload = response.json()
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="Radio directory timeout")
    except httpx.HTTPError as exc:
        log.warning("Radio Browser request failed for %s: %s", resolved_path, exc)
        raise HTTPException(status_code=502, detail="Radio directory unavailable") from exc
    if ttl_value and key:
        _radio_cache_put(key, payload, ttl_value)
    return payload


def _serialize_radio_station(item: Optional[dict]) -> dict:
    if not isinstance(item, dict):
        return {}
    tags_value = item.get("tags") or ""
    if isinstance(tags_value, str):
        tags = [segment.strip() for segment in tags_value.split(",") if segment.strip()]
    elif isinstance(tags_value, list):
        tags = [str(segment).strip() for segment in tags_value if str(segment).strip()]
    else:
        tags = []
    stream_url = (item.get("url_resolved") or item.get("url") or "").strip()
    return {
        "station_id": item.get("stationuuid") or item.get("id"),
        "name": item.get("name"),
        "stream_url": stream_url,
        "codec": item.get("codec"),
        "bitrate": item.get("bitrate"),
        "country": item.get("country"),
        "countrycode": item.get("countrycode"),
        "state": item.get("state"),
        "language": item.get("language"),
        "homepage": item.get("homepage"),
        "favicon": item.get("favicon"),
        "tags": tags,
    }


def _require_radio_worker_token(request: Request) -> None:
    if not RADIO_WORKER_TOKEN:
        return
    header = request.headers.get("x-radio-worker-token") or request.headers.get("authorization", "")
    if header.lower().startswith("bearer "):
        header = header[7:]
    candidate = header.strip()
    if not candidate or not secrets.compare_digest(candidate, RADIO_WORKER_TOKEN):
        raise HTTPException(status_code=401, detail="Invalid radio worker token")


def _radio_runtime_payload(channel_id: str) -> dict:
    status = radio_runtime_status.get(channel_id)
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
    if node.get("type") == "browser":
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
    if node.get("type") == "browser":
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
    if node.get("wifi"):
        data["wifi"] = node.get("wifi")
    data["is_controller"] = bool(node.get("is_controller"))
    return data


def public_nodes() -> list[dict]:
    return [public_node(node) for node in nodes.values()]


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


def _ensure_spotify_token(channel_id: Optional[str] = None) -> dict:
    token = load_token(channel_id)
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
    await _broadcast_to_node_watchers({"type": "nodes", "nodes": public_nodes()})


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
        session = await webrtc_relay.create_session(node["id"], channel_id, stream_id)
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
    if "://" not in value:
        value = f"http://{value}"
    return value.rstrip("/")


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
    node_type = "browser" if normalized.startswith("browser:") else "agent"
    nodes[node_id] = {
        "id": node_id,
        "name": reg.name,
        "url": normalized,
        "last_seen": now,
        "type": node_type,
        "eq": default_eq_state(),
        "agent_secret": previous.get("agent_secret"),
        "audio_configured": True if node_type == "browser" else previous.get("audio_configured", False),
        "agent_version": previous.get("agent_version"),
        "volume_percent": _normalize_percent(previous.get("volume_percent", 75), default=75),
        "max_volume_percent": _normalize_percent(previous.get("max_volume_percent", 100), default=100),
        "muted": previous.get("muted", False),
        "updating": bool(previous.get("updating", False)),
        "playback_device": previous.get("playback_device"),
        "outputs": previous.get("outputs", {}),
        "fingerprint": fingerprint or previous.get("fingerprint"),
        "channel_id": _select_initial_channel_id(previous.get("channel_id"), fallback=node_type != "browser"),
        "snapclient_id": previous.get("snapclient_id"),
        "online": True,
        "offline_since": None,
        "is_controller": bool(previous.get("is_controller")),
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
    if not normalized or normalized.startswith("browser:"):
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


async def request_agent_secret(node: dict, force: bool = False) -> str:
    if node.get("type") != "agent":
        raise HTTPException(status_code=400, detail="Pairing only applies to hardware nodes")
    url = f"{node['url'].rstrip('/')}/pair"
    payload = {"force": bool(force)}
    async with httpx.AsyncClient(timeout=5) as client:
        resp = await client.post(url, json=payload)
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
    global nodes
    if not NODES_PATH.exists():
        nodes = {}
        return
    try:
        data = json.loads(NODES_PATH.read_text())
        nodes = {}
        for item in data:
            item = dict(item)
            item.pop("pan", None)
            eq = item.get("eq") or default_eq_state()
            eq.setdefault("bands", [])
            eq.setdefault("band_count", len(eq["bands"]) or 15)
            eq.setdefault("preset", DEFAULT_EQ_PRESET)
            item["eq"] = eq
            item["url"] = _normalize_node_url(item.get("url", ""))
            if item.get("type") == "browser":
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
            item["channel_id"] = _select_initial_channel_id(
                item.get("channel_id"), fallback=item.get("type") != "browser"
            )
            snapclient_id = (item.get("snapclient_id") or "").strip() or None
            item["snapclient_id"] = snapclient_id
            nodes[item["id"]] = item
    except Exception:
        nodes = {}


def save_nodes() -> None:
    serialized = []
    for node in nodes.values():
        entry = {k: v for k, v in node.items() if k not in TRANSIENT_NODE_FIELDS}
        serialized.append(entry)
    NODES_PATH.write_text(json.dumps(serialized, indent=2))


load_nodes()


async def _spotify_refresh_loop() -> None:
    while True:
        try:
            await asyncio.sleep(max(SPOTIFY_REFRESH_CHECK_INTERVAL, 5))
            for channel_id in list(channel_order):
                channel_entry = channels_by_id.get(channel_id)
                if not channel_entry:
                    continue
                token = load_token(channel_id)
                if not token or "refresh_token" not in token:
                    continue
                seconds_left = _token_seconds_until_expiry(token)
                if seconds_left is None:
                    seconds_left = -1
                if seconds_left > SPOTIFY_REFRESH_LEEWAY:
                    continue
                try:
                    await spotify_refresh(token, channel_id)
                except HTTPException as exc:
                    log.warning("Background Spotify refresh failed for %s: %s", channel_id, exc.detail)
                    await asyncio.sleep(SPOTIFY_REFRESH_FAILURE_BACKOFF)
        except asyncio.CancelledError:
            break
        except Exception:  # pragma: no cover - defensive
            log.exception("Spotify refresh loop crashed")
            await asyncio.sleep(SPOTIFY_REFRESH_FAILURE_BACKOFF)


@app.on_event("startup")
async def _startup_events() -> None:
    global webrtc_relay, node_health_task, spotify_refresh_task, channel_idle_task
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


@app.on_event("shutdown")
async def _shutdown_events() -> None:
    global node_health_task, spotify_refresh_task, channel_idle_task
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
    await stop_local_agent()


def current_spotify_creds(channel_id: Optional[str] = None) -> Tuple[str, str, str]:
    cfg = read_spotify_config(channel_id, include_secret=True)
    cid = (cfg.get("client_id") or os.getenv("SPOTIFY_CLIENT_ID") or "").strip()
    secret = cfg.get("client_secret") or os.getenv("SPOTIFY_CLIENT_SECRET") or ""
    redirect = cfg.get("redirect_uri") or os.getenv("SPOTIFY_REDIRECT_URI") or SPOTIFY_REDIRECT_URI
    return cid, secret, redirect


def read_spotify_config(channel_id: Optional[str] = None, include_secret: bool = False) -> dict:
    channel = get_channel(channel_id)
    cfg_path = Path(channel["config_path"])
    token = load_token(channel["id"])
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
        "channel_id": channel["id"],
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


def read_librespot_status(channel_id: Optional[str] = None) -> dict:
    channel = get_channel(channel_id)
    status_path = Path(channel["status_path"])
    if not status_path.exists():
        return {"state": "unknown", "message": "No status yet"}
    try:
        return json.loads(status_path.read_text())
    except json.JSONDecodeError:
        return {"state": "unknown", "message": "Invalid status file"}


def load_token(channel_id: Optional[str] = None) -> Optional[dict]:
    channel = get_channel(channel_id)
    token_path = Path(channel["token_path"])
    if not token_path.exists():
        return None
    try:
        return json.loads(token_path.read_text())
    except json.JSONDecodeError:
        return None


def save_token(data: dict, channel_id: Optional[str] = None) -> None:
    if not data:
        return
    channel = get_channel(channel_id)
    token_path = Path(channel["token_path"])
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


async def spotify_refresh(token: dict, channel_id: Optional[str] = None) -> dict:
    if not token or "refresh_token" not in token:
        raise HTTPException(status_code=401, detail="Spotify not authorized")
    cid, secret, _ = current_spotify_creds(channel_id)
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
    save_token(token, channel_id)
    return token


async def spotify_request(method: str, path: str, token: dict, channel_id: Optional[str] = None, **kwargs) -> httpx.Response:
    headers = kwargs.pop("headers", {})
    headers["Authorization"] = f"Bearer {token['access_token']}"
    url = f"https://api.spotify.com/v1{path}"
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.request(method, url, headers=headers, **kwargs)
    if resp.status_code == 401:
        token = await spotify_refresh(token, channel_id)
        headers["Authorization"] = f"Bearer {token['access_token']}"
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.request(method, url, headers=headers, **kwargs)
    return resp


@app.get("/api/health")
async def health() -> dict:
    return {"status": "ok"}


@app.get("/api/auth/status")
async def auth_status(request: Request) -> dict:
    user = getattr(request.state, "user", None)
    return {
        "initialized": _is_initialized(),
        "server_name": auth_state.get("server_name", SERVER_DEFAULT_NAME),
        "authenticated": bool(user),
        "user": _public_user(user) if user else None,
    }


@app.post("/api/auth/initialize")
async def initialize_instance(payload: InitializePayload) -> JSONResponse:
    if _is_initialized():
        raise HTTPException(status_code=409, detail="Instance already initialized")
    server_name = payload.server_name.strip() or SERVER_DEFAULT_NAME
    auth_state["server_name"] = server_name
    user = _create_user(payload.username, payload.password, role="admin")
    response = JSONResponse({
        "ok": True,
        "server_name": server_name,
        "user": _public_user(user),
    })
    _set_session_cookie(response, user["id"])
    return response


@app.post("/api/auth/login")
async def login(payload: LoginPayload) -> JSONResponse:
    if not _is_initialized():
        raise HTTPException(status_code=403, detail="Instance setup required")
    user = users_by_username.get(payload.username.strip().lower()) if payload.username else None
    if not user or not _verify_password(payload.password, user.get("password_hash")):
        raise HTTPException(status_code=401, detail="Invalid credentials")
    response = JSONResponse({"ok": True, "user": _public_user(user)})
    _set_session_cookie(response, user["id"])
    return response


@app.post("/api/auth/logout")
async def logout() -> JSONResponse:
    response = JSONResponse({"ok": True})
    _clear_session_cookie(response)
    return response


@app.get("/api/users")
async def list_users(_: dict = Depends(require_admin)) -> dict:
    return {"users": [_public_user(user) for user in users_by_id.values()]}


@app.post("/api/users")
async def create_user(payload: CreateUserPayload, _: dict = Depends(require_admin)) -> dict:
    user = _create_user(payload.username, payload.password, role=payload.role)
    return {"ok": True, "user": _public_user(user)}


@app.patch("/api/users/{user_id}")
async def update_user(user_id: str, payload: UpdateUserPayload, _: dict = Depends(require_admin)) -> dict:
    updates = payload.model_dump(exclude_unset=True)
    user = _update_user(
        user_id,
        username=updates.get("username"),
        password=updates.get("password"),
        role=updates.get("role"),
    )
    return {"ok": True, "user": _public_user(user)}


@app.delete("/api/users/{user_id}")
async def delete_user(user_id: str, request: Request, _: dict = Depends(require_admin)) -> JSONResponse:
    current_user = getattr(request.state, "user", None)
    deleting_self = current_user and current_user.get("id") == user_id
    _delete_user(user_id)
    response = JSONResponse({"ok": True, "removed": user_id, "self_removed": bool(deleting_self)})
    if deleting_self:
        _clear_session_cookie(response)
    return response


@app.post("/api/server/name")
async def update_server_name(payload: ServerNamePayload, _: dict = Depends(require_admin)) -> dict:
    name = payload.server_name.strip() or SERVER_DEFAULT_NAME
    auth_state["server_name"] = name
    _save_auth_state()
    return {"ok": True, "server_name": name}


@app.get("/api/snapcast/status")
async def snapcast_status() -> dict:
    try:
        status = await snapcast.status()
        return status
    except Exception as exc:  # pragma: no cover - network paths
        log.exception("Failed to fetch snapcast status")
        raise HTTPException(status_code=502, detail=str(exc))


@app.post("/api/snapcast/clients/{client_id}/volume")
async def snapcast_volume(client_id: str, payload: VolumePayload) -> dict:
    try:
        result = await snapcast.set_client_volume(client_id, payload.percent)
        return {"ok": True, "result": result}
    except Exception as exc:  # pragma: no cover - network paths
        log.exception("Failed to set snapcast volume")
        raise HTTPException(status_code=502, detail=str(exc))


async def _register_node_payload(reg: NodeRegistration, *, mark_controller: bool = False) -> dict:
    normalized_url = _normalize_node_url(reg.url)
    reg.url = normalized_url
    fingerprint = reg.fingerprint or await _fetch_agent_fingerprint(normalized_url)
    node = _register_node_internal(reg, fingerprint=fingerprint, normalized_url=normalized_url)
    if mark_controller:
        node["is_controller"] = True
        save_nodes()
    if node.get("type") == "agent":
        try:
            secret = await request_agent_secret(node, force=True)
            node["agent_secret"] = secret
            await configure_agent_audio(node)
            await _sync_node_max_volume(node)
        except Exception:
            nodes.pop(node["id"], None)
            save_nodes()
            raise
        save_nodes()
    await broadcast_nodes()
    return public_node(node)


@app.post("/api/nodes/register")
async def register_node(reg: NodeRegistration) -> dict:
    return await _register_node_payload(reg)


@app.post("/api/nodes/register-controller")
async def register_controller_node(_: dict = Depends(require_admin)) -> dict:
    existing = _controller_node()
    if existing:
        if existing.get("online") is False:
            try:
                await ensure_local_agent_running()
                await refresh_agent_metadata(existing)
            except Exception:
                pass
        return public_node(existing)
    try:
        await ensure_local_agent_running()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to start local agent: {exc}") from exc
    name = f"{auth_state.get('server_name') or SERVER_DEFAULT_NAME}{LOCAL_AGENT_NAME_SUFFIX}"
    reg = NodeRegistration(name=name, url=local_agent_url())
    try:
        return await _register_node_payload(reg, mark_controller=True)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


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
        node["channel_id"] = None
        return
    channel = channels_by_id[resolved]
    stream_id = channel.get("snap_stream")
    if not stream_id:
        raise HTTPException(status_code=400, detail="Channel is missing snap_stream mapping")
    if node.get("type") == "agent":
        client_id = await _ensure_snapclient_stream(node, channel)
        node["snapclient_id"] = client_id
    elif node.get("type") == "browser" and webrtc_relay:
        await webrtc_relay.update_session_channel(node["id"], resolved, stream_id)
    node["channel_id"] = resolved


@app.post("/api/nodes/{node_id}/volume")
async def set_node_volume(node_id: str, payload: VolumePayload) -> dict:
    node = nodes.get(node_id)
    if not node:
        raise HTTPException(status_code=404, detail="Unknown node")
    requested = _normalize_percent(payload.percent, default=node.get("volume_percent", 75))
    if node.get("type") == "browser":
        ws = browser_ws.get(node_id)
        if not ws:
            raise HTTPException(status_code=503, detail="Browser node not connected")
        await _send_browser_volume(node, requested)
        result = {"sent": True}
    else:
        result = await _call_agent(node, "/volume", {"percent": requested})
    node["volume_percent"] = requested
    save_nodes()
    await broadcast_nodes()
    return {"ok": True, "result": result}


@app.post("/api/nodes/{node_id}/max-volume")
async def set_node_max_volume(node_id: str, payload: VolumePayload) -> dict:
    node = nodes.get(node_id)
    if not node:
        raise HTTPException(status_code=404, detail="Unknown node")
    percent = _normalize_percent(payload.percent, default=_get_node_max_volume(node))
    if node.get("type") == "agent":
        await _sync_node_max_volume(node, percent=percent)
    node["max_volume_percent"] = percent
    save_nodes()
    if node.get("type") == "browser":
        await _send_browser_volume(node, node.get("volume_percent", 75))
    await broadcast_nodes()
    return {"ok": True, "max_volume_percent": percent}


@app.post("/api/nodes/{node_id}/mute")
async def set_node_mute(node_id: str, payload: dict = Body(...)) -> dict:
    node = nodes.get(node_id)
    if not node:
        raise HTTPException(status_code=404, detail="Unknown node")
    muted = payload.get("muted") if payload else None
    if muted is None:
        raise HTTPException(status_code=400, detail="Missing muted")
    if node.get("type") == "browser":
        ws = browser_ws.get(node_id)
        if not ws:
            raise HTTPException(status_code=503, detail="Browser node not connected")
        await ws.send_json({"type": "mute", "muted": bool(muted)})
        result = {"sent": True}
    else:
        result = await _call_agent(node, "/mute", {"muted": bool(muted)})
    node["muted"] = bool(muted)
    save_nodes()
    await broadcast_nodes()
    return {"ok": True, "result": result}


@app.post("/api/snapcast/master-volume")
async def snapcast_master_volume(payload: VolumePayload) -> dict:
    try:
        clients = await snapcast.list_clients()
        updated = []
        for c in clients:
            await snapcast.set_client_volume(c["id"], payload.percent)
            updated.append(c["id"])
        return {"ok": True, "updated": updated}
    except Exception as exc:  # pragma: no cover
        log.exception("Failed to set master volume")
        raise HTTPException(status_code=502, detail=str(exc))


@app.post("/api/nodes/{node_id}/eq")
async def set_node_eq(node_id: str, payload: EqPayload) -> dict:
    node = nodes.get(node_id)
    if not node:
        raise HTTPException(status_code=404, detail="Unknown node")
    eq_data = payload.model_dump()
    node["eq"] = eq_data
    save_nodes()
    if node.get("type") == "browser":
        ws = browser_ws.get(node_id)
        if not ws:
            raise HTTPException(status_code=503, detail="Browser node not connected")
        await ws.send_json({"type": "eq", "eq": eq_data})
        result = {"sent": True}
    else:
        result = await _call_agent(node, "/eq", eq_data)
    await broadcast_nodes()
    return {"ok": True, "result": result}


@app.post("/api/nodes/{node_id}/pair")
async def pair_node(node_id: str, payload: dict | None = Body(default=None)) -> dict:
    node = nodes.get(node_id)
    if not node:
        raise HTTPException(status_code=404, detail="Unknown node")
    if node.get("type") == "browser":
        raise HTTPException(status_code=400, detail="Browser nodes do not support pairing")
    force = True if payload is None else bool(payload.get("force", False))
    secret = await request_agent_secret(node, force=force)
    node["agent_secret"] = secret
    await configure_agent_audio(node)
    save_nodes()
    await broadcast_nodes()
    return {"ok": True}


@app.post("/api/nodes/{node_id}/rename")
async def rename_node(node_id: str, payload: RenameNodePayload) -> dict:
    node = nodes.get(node_id)
    if not node:
        raise HTTPException(status_code=404, detail="Unknown node")
    new_name = payload.name.strip()
    if not new_name:
        raise HTTPException(status_code=400, detail="Name must not be empty")
    node["name"] = new_name
    save_nodes()
    await broadcast_nodes()
    return {"ok": True, "name": new_name}


@app.post("/api/nodes/{node_id}/channel")
async def update_node_channel(node_id: str, payload: NodeChannelPayload) -> dict:
    node = nodes.get(node_id)
    if not node:
        raise HTTPException(status_code=404, detail="Unknown node")
    await _set_node_channel(node, payload.channel_id)
    save_nodes()
    await broadcast_nodes()
    return {"ok": True, "channel_id": node.get("channel_id")}


@app.post("/api/nodes/{node_id}/configure")
async def configure_node(node_id: str) -> dict:
    node = nodes.get(node_id)
    if not node:
        raise HTTPException(status_code=404, detail="Unknown node")
    if node.get("type") == "browser":
        raise HTTPException(status_code=400, detail="Browser nodes do not require configuration")
    result = await configure_agent_audio(node)
    await _sync_node_max_volume(node)
    await broadcast_nodes()
    return {"ok": True, "result": result}


@app.post("/api/nodes/{node_id}/outputs")
async def set_node_output(node_id: str, payload: OutputSelectionPayload) -> dict:
    node = nodes.get(node_id)
    if not node:
        raise HTTPException(status_code=404, detail="Unknown node")
    if node.get("type") == "browser":
        raise HTTPException(status_code=400, detail="Browser nodes do not support hardware outputs")
    result = await _call_agent(node, "/outputs", payload.model_dump())
    outputs = result.get("outputs") if isinstance(result, dict) else None
    if isinstance(outputs, dict):
        node["outputs"] = outputs
        selected = outputs.get("selected")
        if isinstance(selected, str) and selected:
            node["playback_device"] = selected
    else:
        node["playback_device"] = payload.device
    save_nodes()
    await broadcast_nodes()
    return {"ok": True, "result": result}


@app.post("/api/nodes/{node_id}/check-updates")
async def check_node_updates(node_id: str) -> dict:
    node = nodes.get(node_id)
    if not node:
        raise HTTPException(status_code=404, detail="Unknown node")
    if node.get("type") == "browser":
        raise HTTPException(status_code=400, detail="Browser nodes do not support updates")
    reachable, changed = await refresh_agent_metadata(node)
    if not reachable:
        raise HTTPException(status_code=504, detail="Node agent is not responding")
    if node.get("agent_version"):
        log.info("Checked updates for node %s (version %s)", node_id, node.get("agent_version"))
    await broadcast_nodes()
    public = public_node(node)
    return {
        "ok": True,
        "agent_version": public.get("agent_version"),
        "update_available": public.get("update_available"),
        "changed": changed,
    }


@app.post("/api/nodes/{node_id}/update")
async def update_agent_node(node_id: str) -> dict:
    node = nodes.get(node_id)
    if not node:
        raise HTTPException(status_code=404, detail="Unknown node")
    if node.get("type") == "browser":
        raise HTTPException(status_code=400, detail="Browser nodes cannot be updated from the controller")
    result = await _call_agent(node, "/update", {})
    node["updating"] = True
    node["audio_configured"] = False
    node["_needs_reconfig"] = True
    save_nodes()
    await broadcast_nodes()
    schedule_agent_refresh(node_id, delay=20.0, repeat=True, attempts=12)
    return {"ok": True, "result": result}


@app.post("/api/nodes/{node_id}/restart")
async def restart_agent_node(node_id: str) -> dict:
    node = nodes.get(node_id)
    if not node:
        raise HTTPException(status_code=404, detail="Unknown node")
    if node.get("type") == "browser":
        raise HTTPException(status_code=400, detail="Browser nodes cannot be restarted from the controller")
    result = await _call_agent(node, "/restart", {})
    schedule_restart_watch(node_id)
    await broadcast_nodes()
    return {"ok": True, "result": result, "timeout": NODE_RESTART_TIMEOUT}


@app.post("/api/nodes/{node_id}/terminal-session")
async def create_terminal_session(node_id: str) -> dict:
    if not NODE_TERMINAL_ENABLED:
        raise HTTPException(status_code=503, detail="Terminal access is disabled")
    node = nodes.get(node_id)
    if not node:
        raise HTTPException(status_code=404, detail="Unknown node")
    if node.get("type") == "browser":
        raise HTTPException(status_code=400, detail="Browser nodes do not support terminal access")
    if node.get("online") is False:
        raise HTTPException(status_code=409, detail="Node is offline")
    target = _resolve_terminal_target(node)
    if not target:
        raise HTTPException(status_code=400, detail="Terminal credentials are not configured")
    _cleanup_terminal_sessions()
    token = secrets.token_urlsafe(32)
    now = time.time()
    expires_at = now + max(5, NODE_TERMINAL_TOKEN_TTL)
    deadline = now + max(30, NODE_TERMINAL_MAX_DURATION)
    terminal_sessions[token] = {
        "node_id": node_id,
        "target": target,
        "created_at": now,
        "expires_at": expires_at,
        "deadline": deadline,
    }
    return {
        "token": token,
        "expires_at": expires_at,
        "ws_path": f"/ws/terminal/{token}",
        "page_url": f"/static/terminal.html?token={token}",
    }


@app.delete("/api/nodes/{node_id}")
async def unregister_node(node_id: str) -> dict:
    node = nodes.pop(node_id, None)
    if not node:
        raise HTTPException(status_code=404, detail="Unknown node")
    cancel_node_rediscovery(node_id)
    save_nodes()
    await broadcast_nodes()
    if node.get("is_controller"):
        await stop_local_agent()
    if node.get("type") == "browser":
        if webrtc_relay:
            await webrtc_relay.drop_session(node_id)
        else:
            await teardown_browser_node(node_id, remove_entry=False)
    return {"ok": True, "removed": node_id}


async def _refresh_all_agent_nodes() -> None:
    dirty = False
    for node in list(nodes.values()):
        if node.get("type") != "agent":
            continue
        _, changed = await refresh_agent_metadata(node, persist=False)
        if changed:
            dirty = True
    if dirty:
        save_nodes()
        await broadcast_nodes()


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


@app.get("/api/nodes")
async def list_nodes() -> dict:
    return {"nodes": public_nodes()}


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


@app.post("/api/web-nodes/session")
async def web_node_session(payload: WebNodeOffer, request: Request) -> dict:
    if not WEBRTC_ENABLED or not webrtc_relay:
        raise HTTPException(status_code=503, detail="Web nodes are disabled")
    name = payload.name.strip() or "Web node"
    loop = asyncio.get_running_loop()
    future: asyncio.Future = loop.create_future()
    request_id = secrets.token_urlsafe(16)
    client_host = request.client.host if request.client else None
    entry = {
        "id": request_id,
        "name": name,
        "offer_sdp": payload.sdp,
        "offer_type": payload.type,
        "future": future,
        "client_host": client_host,
        "created_at": time.time(),
    }
    snapshot = _get_web_node_snapshot(entry)
    pending_web_node_requests[request_id] = entry
    await _broadcast_web_node_request_event("created", snapshot=snapshot)
    try:
        result = await asyncio.wait_for(future, timeout=WEB_NODE_APPROVAL_TIMEOUT)
    except asyncio.TimeoutError:
        pending = pending_web_node_requests.pop(request_id, None)
        pending_snapshot = _get_web_node_snapshot(pending) if pending else snapshot
        if pending and not pending["future"].done():
            pending["future"].set_result({"status": "expired", "status_code": 408})
        await _broadcast_web_node_request_event("resolved", snapshot=pending_snapshot, status="expired")
        raise HTTPException(status_code=408, detail="Approval timed out")
    except HTTPException as exc:
        raise exc
    except Exception as exc:  # pragma: no cover - defensive
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        pending_web_node_requests.pop(request_id, None)
    if result.get("status") != "approved":
        status_code = result.get("status_code", 403)
        detail = result.get("message") or "Web node request denied"
        raise HTTPException(status_code=status_code, detail=detail)
    return {
        "node": result["node"],
        "answer": result["answer"],
        "answer_type": result["answer_type"],
    }


@app.get("/api/web-nodes/requests")
async def list_web_node_requests(_: dict = Depends(require_admin)) -> dict:
    return {"requests": _pending_web_node_snapshots()}


@app.post("/api/web-nodes/requests/{request_id}")
async def decide_web_node_request(
    request_id: str,
    payload: WebNodeRequestDecisionPayload,
    _: dict = Depends(require_admin),
) -> dict:
    action = payload.action.lower().strip()
    pending = _pop_pending_web_node_request(request_id)
    snapshot = _get_web_node_snapshot(pending)
    future: asyncio.Future = pending.get("future")
    if action == "approve":
        try:
            result = await _establish_web_node_session_for_request(pending)
        except HTTPException as exc:
            if future and not future.done():
                future.set_result({
                    "status": "failed",
                    "status_code": exc.status_code,
                    "message": exc.detail,
                })
            await _broadcast_web_node_request_event("resolved", snapshot=snapshot, status="failed", reason=exc.detail)
            raise
        if future and not future.done():
            future.set_result({"status": "approved", **result})
        await _broadcast_web_node_request_event("resolved", snapshot=snapshot, status="approved")
        return {"ok": True, "status": "approved"}
    reason = (payload.reason or "Request denied").strip() or "Request denied"
    if future and not future.done():
        future.set_result({"status": "denied", "message": reason, "status_code": 403})
    await _broadcast_web_node_request_event("resolved", snapshot=snapshot, status="denied", reason=reason)
    return {"ok": True, "status": "denied"}


@app.websocket("/ws/web-node")
async def web_node_ws(ws: WebSocket):
    user = await _require_ws_user(ws)
    if not user:
        return
    await ws.accept()
    node_id = ws.query_params.get("node_id")
    if not node_id or node_id not in nodes:
        await ws.close(code=1008)
        return
    browser_ws[node_id] = ws
    try:
        while True:
            await ws.receive_text()
    except Exception:
        pass
    finally:
        browser_ws.pop(node_id, None)


@app.websocket("/ws/nodes")
async def nodes_ws(ws: WebSocket):
    user = await _require_ws_user(ws)
    if not user:
        return
    await ws.accept()
    node_watchers.add(ws)
    try:
        await ws.send_json({"type": "nodes", "nodes": public_nodes()})
        await ws.send_json({"type": "web_node_requests", "requests": _pending_web_node_snapshots()})
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        node_watchers.discard(ws)


@app.websocket("/ws/terminal/{token}")
async def terminal_ws(ws: WebSocket, token: str):
    if not NODE_TERMINAL_ENABLED:
        await ws.close(code=4403)
        return
    user = await _require_ws_user(ws)
    if not user:
        return
    session = terminal_sessions.pop(token, None)
    now = time.time()
    if not session or session.get("expires_at", 0) < now:
        await ws.close(code=4403)
        return
    target = session.get("target") or {}
    await ws.accept()
    key_path = (target.get("key_path") or "").strip()
    if key_path:
        key_path = os.path.expanduser(key_path)
        if not Path(key_path).exists():
            await ws.send_json({"type": "error", "message": "SSH key not found"})
            await ws.close(code=1011)
            return
    password = (target.get("password") or "").strip()
    if not key_path and not password:
        await ws.send_json({"type": "error", "message": "No SSH credentials configured"})
        await ws.close(code=1011)
        return
    ssh_host = target.get("host")
    if not ssh_host:
        await ws.send_json({"type": "error", "message": "Terminal host unavailable"})
        await ws.close(code=1011)
        return
    ssh_kwargs = {
        "host": ssh_host,
        "port": target.get("port", 22),
        "username": target.get("user"),
        "client_keys": [key_path] if key_path else None,
        "password": password or None,
    }
    if not ssh_kwargs["username"]:
        await ws.send_json({"type": "error", "message": "Terminal user is not configured"})
        await ws.close(code=1011)
        return
    if not NODE_TERMINAL_STRICT_HOST_KEY:
        ssh_kwargs["known_hosts"] = None
    term_size = asyncssh.TermSize(80, 24)
    deadline = session.get("deadline", now + NODE_TERMINAL_MAX_DURATION)

    async def _send_error(message: str) -> None:
        try:
            await ws.send_json({"type": "error", "message": message})
        finally:
            await ws.close(code=1011)

    try:
        async with asyncssh.connect(
            ssh_kwargs["host"],
            port=ssh_kwargs.get("port") or 22,
            username=ssh_kwargs.get("username"),
            client_keys=ssh_kwargs.get("client_keys"),
            password=ssh_kwargs.get("password"),
            known_hosts=ssh_kwargs.get("known_hosts"),
        ) as conn:
            process = await conn.create_process(term_type="xterm-256color", term_size=term_size, encoding=None)

            async def pump_stream(stream):
                try:
                    while True:
                        chunk = await stream.read(1024)
                        if not chunk:
                            break
                        if isinstance(chunk, bytes):
                            text = chunk.decode("utf-8", errors="ignore")
                        else:
                            text = chunk
                        try:
                            await ws.send_json({"type": "output", "data": text})
                        except Exception:
                            break
                except Exception:
                    return

            async def pump_input():
                try:
                    while True:
                        if time.time() > deadline:
                            await ws.send_json({"type": "error", "message": "Terminal session expired"})
                            break
                        raw = await ws.receive_text()
                        payload = json.loads(raw)
                        msg_type = payload.get("type")
                        if msg_type == "input":
                            data = payload.get("data", "")
                            if data:
                                process.stdin.write(data)
                                await process.stdin.drain()
                        elif msg_type == "resize":
                            cols = int(payload.get("cols") or 80)
                            rows = int(payload.get("rows") or 24)
                            try:
                                set_size = getattr(process.channel, "set_terminal_size", None)
                                if asyncio.iscoroutinefunction(set_size):
                                    await set_size(cols, rows)
                                elif set_size:
                                    set_size(cols, rows)
                            except Exception:
                                pass
                        elif msg_type == "close":
                            break
                except WebSocketDisconnect:
                    pass
                except Exception:
                    pass
                finally:
                    try:
                        process.stdin.write_eof()
                    except Exception:
                        pass

            tasks = [asyncio.create_task(pump_input())]
            if process.stdout:
                tasks.append(asyncio.create_task(pump_stream(process.stdout)))
            if process.stderr:
                tasks.append(asyncio.create_task(pump_stream(process.stderr)))

            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
            await process.wait()
            try:
                await ws.send_json({"type": "exit", "code": process.exit_status})
            except Exception:
                pass
            try:
                await ws.close()
            except Exception:
                pass
    except asyncssh.PermissionDenied as exc:
        await _send_error(f"Permission denied: {exc}")
    except asyncssh.Error as exc:
        await _send_error(f"SSH error: {exc}")
    except Exception as exc:
        await _send_error(f"Terminal failed: {exc}")


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


@app.get("/api/nodes/discover")
async def discover_nodes() -> StreamingResponse:
    networks = _detect_discovery_networks()
    if not networks:
        raise HTTPException(status_code=503, detail="No IPv4 networks available for discovery")
    hosts = _hosts_for_networks(networks)
    if not hosts:
        raise HTTPException(status_code=503, detail="No hosts available for discovery")

    async def _event_stream() -> AsyncIterator[str]:
        found = 0
        limited = len(hosts) >= DISCOVERY_MAX_HOSTS
        yield json.dumps({
            "type": "start",
            "networks": networks,
            "host_count": len(hosts),
            "limited": limited,
        }) + "\n"
        try:
            async for result in _stream_host_probes(hosts):
                found += 1
                yield json.dumps({"type": "discovered", "data": result}) + "\n"
        except asyncio.CancelledError:
            yield json.dumps({"type": "cancelled", "found": found}) + "\n"
            raise
        except Exception as exc:  # pragma: no cover - defensive
            log.exception("Discovery failed", exc_info=exc)
            yield json.dumps({"type": "error", "message": "Discovery failed"}) + "\n"
        else:
            yield json.dumps({"type": "complete", "found": found}) + "\n"

    return StreamingResponse(_event_stream(), media_type="application/x-ndjson")


@app.get("/api/channels")
async def list_channels_api() -> dict:
    return {"channels": all_channel_details()}


@app.patch("/api/channels/{channel_id}")
async def update_channel_api(channel_id: str, payload: ChannelUpdatePayload) -> dict:
    updates = payload.model_dump(exclude_unset=True)
    if updates:
        update_channel_metadata(channel_id, updates)
    return {"ok": True, "channel": channel_detail(resolve_channel_id(channel_id))}


@app.get("/api/config/spotify")
async def get_spotify_config(channel_id: Optional[str] = Query(default=None)) -> dict:
    resolved = resolve_channel_id(channel_id)
    return read_spotify_config(resolved)


@app.post("/api/config/spotify")
async def set_spotify_config(cfg: SpotifyConfig, channel_id: Optional[str] = Query(default=None)) -> dict:
    resolved = resolve_channel_id(channel_id)
    channel = get_channel(resolved)
    cfg_path = Path(channel["config_path"])
    payload = cfg.model_dump()
    existing = {}
    if cfg_path.exists():
        try:
            existing = json.loads(cfg_path.read_text())
        except json.JSONDecodeError:
            existing = {}

    if not payload.get("client_secret"):
        payload["client_secret"] = existing.get("client_secret") or os.getenv("SPOTIFY_CLIENT_SECRET")
    if not payload.get("client_id"):
        payload["client_id"] = existing.get("client_id") or os.getenv("SPOTIFY_CLIENT_ID")
    if not payload.get("redirect_uri"):
        payload["redirect_uri"] = existing.get("redirect_uri") or SPOTIFY_REDIRECT_URI

    primary_channel = _primary_channel_id()
    if resolved == primary_channel and payload.get("client_id"):
        os.environ["SPOTIFY_CLIENT_ID"] = payload["client_id"]
    if resolved == primary_channel and payload.get("client_secret"):
        os.environ["SPOTIFY_CLIENT_SECRET"] = payload["client_secret"]
    if resolved == primary_channel and payload.get("redirect_uri"):
        os.environ["SPOTIFY_REDIRECT_URI"] = payload["redirect_uri"]

    cfg_path.parent.mkdir(parents=True, exist_ok=True)
    cfg_path.write_text(json.dumps(payload, indent=2))
    return {"ok": True, "config": read_spotify_config(resolved)}


@app.get("/api/radio/genres")
async def list_radio_genres(limit: int = Query(default=50, ge=1, le=400)) -> dict:
    data = await _radio_browser_request("/tags", ttl=3600, cache_key="radio:tags")
    genres = []
    for entry in data:
        if not isinstance(entry, dict):
            continue
        name = (entry.get("name") or "").strip()
        if not name:
            continue
        try:
            count = int(entry.get("stationcount") or 0)
        except (TypeError, ValueError):
            count = 0
        genres.append({"name": name, "stationcount": count})
    genres.sort(key=lambda item: item["stationcount"], reverse=True)
    return {"genres": genres[:limit]}


@app.get("/api/radio/countries")
async def list_radio_countries(limit: int = Query(default=250, ge=1, le=400)) -> dict:
    data = await _radio_browser_request("/countries", ttl=3600, cache_key="radio:countries")
    countries = []
    for entry in data:
        if not isinstance(entry, dict):
            continue
        name = (entry.get("name") or "").strip()
        if not name:
            continue
        try:
            count = int(entry.get("stationcount") or 0)
        except (TypeError, ValueError):
            count = 0
        countries.append({
            "name": name,
            "iso_3166_1": (entry.get("iso_3166_1") or entry.get("countrycode") or "").strip() or None,
            "stationcount": count,
        })
    countries.sort(key=lambda item: item["stationcount"], reverse=True)
    return {"countries": countries[:limit]}


@app.get("/api/radio/top")
async def list_radio_top(metric: str = Query(default="votes", pattern="^(votes|clicks)$"), limit: int = Query(default=40, ge=1, le=200)) -> dict:
    normalized = "vote" if metric == "votes" else "click"
    path = f"/stations/top{normalized}/{limit}"
    cache_key = f"radio:top:{metric}:{limit}"
    data = await _radio_browser_request(path, ttl=120, cache_key=cache_key)
    stations = [_serialize_radio_station(item) for item in data]
    return {"stations": stations}


@app.get("/api/radio/search")
async def search_radio_stations(
    query: Optional[str] = Query(default=None, max_length=200),
    country: Optional[str] = Query(default=None, max_length=120),
    countrycode: Optional[str] = Query(default=None, max_length=4),
    tag: Optional[str] = Query(default=None, max_length=120),
    limit: int = Query(default=50, ge=1, le=200),
) -> dict:
    params = {"limit": limit, "hidebroken": "true"}
    has_filter = False
    if query:
        params["name"] = query
        has_filter = True
    if country:
        params["country"] = country
        has_filter = True
    if countrycode:
        params["countrycode"] = countrycode
        has_filter = True
    if tag:
        params["tag"] = tag
        has_filter = True
    if not has_filter:
        raise HTTPException(status_code=400, detail="Provide a query or filter")
    data = await _radio_browser_request("/stations/search", params=params, ttl=15)
    stations = [_serialize_radio_station(item) for item in data]
    return {"stations": stations[:limit]}


@app.post("/api/radio/{channel_id}/station")
async def assign_radio_station(channel_id: str, payload: RadioStationSelectionPayload, _: dict = Depends(require_admin)) -> dict:
    resolved = resolve_channel_id(channel_id)
    channel = _get_radio_channel_or_404(resolved)
    _apply_radio_station(channel, payload)
    save_channels()
    _mark_radio_assignments_dirty()
    radio_runtime_status.pop(resolved, None)
    return {"ok": True, "channel": channel_detail(resolved)}


@app.get("/api/radio/status/{channel_id}")
async def get_radio_status(channel_id: str) -> dict:
    resolved = resolve_channel_id(channel_id)
    channel = _get_radio_channel_or_404(resolved)
    state = _ensure_radio_state(channel)
    runtime = _radio_runtime_payload(resolved)
    return {
        "channel_id": resolved,
        "radio_state": state,
        "runtime": runtime,
        "enabled": channel.get("enabled", True),
    }


@app.post("/api/radio/{channel_id}/playback")
async def control_radio_playback(channel_id: str, payload: RadioPlaybackPayload) -> dict:
    resolved = resolve_channel_id(channel_id)
    channel = _get_radio_channel_or_404(resolved)
    state = _ensure_radio_state(channel)
    now = int(time.time())
    action = payload.action
    if action == "start" and not state.get("stream_url"):
        raise HTTPException(status_code=400, detail="Tune a station before starting playback")
    desired = action == "start"
    previous = state.get("playback_enabled", True)
    state["playback_enabled"] = desired
    state["updated_at"] = now
    channel["radio_state"] = state
    save_channels()
    _mark_radio_assignments_dirty()
    if not desired:
        radio_runtime_status[resolved] = {
            "state": "idle",
            "message": "Radio stopped",
            "bitrate": None,
            "station_id": state.get("station_id"),
            "metadata": None,
            "updated_at": now,
            "started_at": None,
        }
    else:
        radio_runtime_status.pop(resolved, None)
    return {"ok": True, "radio_state": state, "previous": previous, "current": desired}


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


@app.get("/api/radio/worker/assignments")
async def radio_worker_assignments(
    request: Request,
    since: Optional[int] = Query(None),
    wait: Optional[float] = Query(None),
) -> dict:
    _require_radio_worker_token(request)
    timeout = RADIO_ASSIGNMENT_DEFAULT_WAIT if wait is None else float(wait)
    timeout = max(1.0, min(timeout, RADIO_ASSIGNMENT_MAX_WAIT))
    version = radio_assignments_version
    if since is not None:
        version = await _wait_for_radio_assignments_change(since, timeout)
    assignments = []
    for cid in channel_order:
        channel = channels_by_id.get(cid)
        if not channel or channel.get("source") != "radio":
            continue
        state = _ensure_radio_state(channel)
        assignments.append({
            "channel_id": cid,
            "enabled": channel.get("enabled", True),
            "snap_stream": channel["snap_stream"],
            "fifo_path": channel["fifo_path"],
            "stream_url": state.get("stream_url"),
            "station_id": state.get("station_id"),
            "updated_at": state.get("updated_at"),
            "playback_enabled": state.get("playback_enabled", True),
        })
    return {"assignments": assignments, "version": version}


@app.post("/api/radio/worker/status/{channel_id}")
async def radio_worker_status(channel_id: str, payload: RadioWorkerStatusPayload, request: Request) -> dict:
    _require_radio_worker_token(request)
    resolved = resolve_channel_id(channel_id)
    channel = _get_radio_channel_or_404(resolved)
    previous = radio_runtime_status.get(resolved) or {}
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
        "bitrate": payload.bitrate,
        "station_id": payload.station_id or (channel.get("radio_state") or {}).get("station_id"),
        "metadata": payload.metadata,
        "updated_at": now,
        "started_at": started_at,
    }
    if payload.state != "playing":
        status_payload["started_at"] = None
    radio_runtime_status[resolved] = status_payload
    if payload.metadata:
        state = _ensure_radio_state(channel)
        state["last_metadata"] = payload.metadata
        state["updated_at"] = status_payload["updated_at"]
        channel["radio_state"] = state
        save_channels()
    return {"ok": True}


@app.get("/api/librespot/status")
async def librespot_status(channel_id: Optional[str] = Query(default=None)) -> dict:
    resolved = resolve_channel_id(channel_id)
    return read_librespot_status(resolved)


@app.get("/api/spotify/auth-url")
async def spotify_auth_url(channel_id: Optional[str] = Query(default=None)) -> dict:
    resolved = resolve_channel_id(channel_id)
    cid, secret, redirect = current_spotify_creds(resolved)
    if not (cid and secret):
        raise HTTPException(status_code=400, detail="Spotify client_id/client_secret not set")
    state = TOKEN_SIGNER.dumps({"t": time.time(), "channel_id": resolved})
    scope = "user-read-playback-state user-modify-playback-state streaming user-read-email user-read-private"
    params = {
        "client_id": cid,
        "response_type": "code",
        "redirect_uri": redirect,
        "scope": scope,
        "state": state,
    }

    return {"url": f"https://accounts.spotify.com/authorize?{urlencode(params)}"}


@app.get("/api/spotify/callback", include_in_schema=False)
async def spotify_callback(code: str, state: str):
    try:
        payload = TOKEN_SIGNER.loads(state)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid state")

    resolved = resolve_channel_id((payload or {}).get("channel_id"))
    cid, secret, redirect = current_spotify_creds(resolved)

    data = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": redirect,
        "client_id": cid,
        "client_secret": secret,
    }
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.post("https://accounts.spotify.com/api/token", data=data)
    if resp.status_code >= 400:
        raise HTTPException(status_code=400, detail="Failed to exchange code")
    token = resp.json()
    save_token(token, resolved)
    return Response(content="Spotify linked. You can close this tab.", media_type="text/plain")


@app.get("/api/spotify/player/status")
async def spotify_player_status(channel_id: Optional[str] = Query(default=None)) -> dict:
    resolved = resolve_channel_id(channel_id)
    token = _ensure_spotify_token(resolved)
    resp = await spotify_request("GET", "/me/player", token, resolved)
    if resp.status_code == 204:
        snapshot = _public_player_snapshot(_get_player_snapshot(resolved))
        payload: dict = {"active": False}
        if snapshot:
            payload["snapshot"] = snapshot
        return payload
    if resp.status_code >= 400:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    data = resp.json()
    active = bool(data.get("device"))
    device = data.get("device") or {}
    preferred_names = [name.lower() for name in _preferred_roomcast_device_names(resolved)]
    device_name = (device.get("name") or "").strip().lower()
    is_roomcast_device = bool(device_name) and device_name in preferred_names
    payload = {
        "active": active,
        "is_playing": data.get("is_playing", False),
        "progress_ms": data.get("progress_ms"),
        "device": device,
        "device_is_roomcast": is_roomcast_device,
        "item": data.get("item", {}),
        "shuffle_state": data.get("shuffle_state", False),
        "repeat_state": data.get("repeat_state", "off"),
        "context": data.get("context"),
    }
    if is_roomcast_device and data.get("item"):
        _set_player_snapshot(resolved, data)
    if not active:
        snapshot = _public_player_snapshot(_get_player_snapshot(resolved))
        if snapshot:
            payload["snapshot"] = snapshot
    return payload


@app.get("/api/spotify/player/queue")
async def spotify_player_queue(channel_id: Optional[str] = Query(default=None)) -> dict:
    resolved = resolve_channel_id(channel_id)
    token = _ensure_spotify_token(resolved)
    resp = await spotify_request("GET", "/me/player/queue", token, resolved)
    if resp.status_code >= 400:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    data = resp.json()
    queue_items: list[dict] = []
    for raw in data.get("queue") or []:
        mapped = _map_spotify_track_simple(raw)
        if mapped:
            queue_items.append(mapped)
    current = _map_spotify_track_simple(data.get("currently_playing"))
    return {
        "current": current,
        "queue": queue_items,
    }


def _with_query(path: str, params: Optional[dict] = None) -> str:
    if not params:
        return path
    clean = {k: v for k, v in (params or {}).items() if v is not None}
    if not clean:
        return path
    separator = "&" if "?" in path else "?"
    return f"{path}{separator}{urlencode(clean)}"


async def _spotify_control(
    path: str,
    method: str = "POST",
    body: Optional[dict] = None,
    params: Optional[dict] = None,
    channel_id: Optional[str] = None,
):
    token = load_token(channel_id)
    if not token:
        raise HTTPException(status_code=401, detail="Spotify not authorized")
    target = _with_query(path, params)
    resp = await spotify_request(method, target, token, channel_id, json=body or {})
    if resp.status_code >= 400:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    return {"ok": True}


def _parse_spotify_error(detail: Any) -> dict:
    message = None
    reason = None
    payload = None
    if isinstance(detail, dict):
        payload = detail
    else:
        text = str(detail or "").strip()
        if text:
            try:
                payload = json.loads(text)
            except json.JSONDecodeError:
                message = text
    if isinstance(payload, dict):
        error_block = payload.get("error") if isinstance(payload.get("error"), dict) else payload
        if isinstance(error_block, dict):
            message = message or error_block.get("message")
            reason = error_block.get("reason") or reason
    normalized = {}
    if message:
        normalized["message"] = message
    if reason:
        normalized["reason"] = reason
    return normalized


@app.post("/api/spotify/player/activate-roomcast")
async def spotify_activate_roomcast(
    payload: ActivateRoomcastPayload = Body(default=ActivateRoomcastPayload()),
    channel_id: Optional[str] = Query(default=None),
) -> dict:
    resolved = resolve_channel_id(channel_id)
    token = _ensure_spotify_token(resolved)
    device = await _find_roomcast_device(token, resolved)
    if not device or not device.get("id"):
        raise HTTPException(status_code=404, detail="RoomCast device is not available. Make sure Librespot is running and linked to Spotify.")
    body = {"device_ids": [device["id"]], "play": payload.play}
    resp = await spotify_request("PUT", "/me/player", token, resolved, json=body)
    if resp.status_code >= 400:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    return {
        "device_id": device.get("id"),
        "device_name": device.get("name"),
        "volume_percent": device.get("volume_percent"),
        "is_active": True,
    }


@app.get("/api/spotify/search")
async def spotify_search(
    q: str = Query(min_length=1, max_length=200),
    limit: int = Query(default=10, ge=1, le=20),
    channel_id: Optional[str] = Query(default=None),
) -> dict:
    query = (q or "").strip()
    if not query:
        raise HTTPException(status_code=400, detail="Query cannot be empty")
    resolved = resolve_channel_id(channel_id)
    token = _ensure_spotify_token(resolved)
    params = {
        "q": query,
        "type": ",".join(SPOTIFY_SEARCH_TYPES),
        "limit": limit,
    }
    path = _with_query("/search", params)
    resp = await spotify_request("GET", path, token, resolved)
    if resp.status_code >= 400:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    data = resp.json()
    return {
        "query": query,
        "limit": limit,
        "albums": _map_spotify_search_bucket(data.get("albums"), _map_spotify_album),
        "tracks": _map_spotify_search_bucket(data.get("tracks"), _map_spotify_track_simple),
        "artists": _map_spotify_search_bucket(data.get("artists"), _map_spotify_artist),
        "playlists": _map_spotify_search_bucket(data.get("playlists"), _map_spotify_playlist),
    }


@app.get("/api/spotify/playlists")
async def spotify_playlists(
    limit: int = Query(default=24, ge=1, le=50),
    offset: int = Query(default=0, ge=0),
    channel_id: Optional[str] = Query(default=None),
) -> dict:
    resolved = resolve_channel_id(channel_id)
    token = _ensure_spotify_token(resolved)
    params = {"limit": min(limit, 50), "offset": max(0, offset)}
    path = _with_query("/me/playlists", params)
    resp = await spotify_request("GET", path, token, resolved)
    if resp.status_code >= 400:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    data = resp.json()
    items = []
    for item in data.get("items", []):
        if isinstance(item, dict):
            items.append(_map_spotify_playlist(item))
    return {
        "items": items,
        "total": data.get("total", len(items)),
        "limit": data.get("limit", params["limit"]),
        "offset": data.get("offset", params["offset"]),
        "next": bool(data.get("next")),
        "previous": bool(data.get("previous")),
    }


@app.get("/api/spotify/playlists/{playlist_id}")
async def spotify_playlist_detail(playlist_id: str, channel_id: Optional[str] = Query(default=None)) -> dict:
    resolved = resolve_channel_id(channel_id)
    token = _ensure_spotify_token(resolved)
    resp = await spotify_request("GET", f"/playlists/{playlist_id}", token, resolved)
    if resp.status_code >= 400:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    data = resp.json()
    playlist = _map_spotify_playlist(data) if isinstance(data, dict) else None
    if not playlist:
        raise HTTPException(status_code=404, detail="Playlist not found")
    return {"playlist": playlist}


@app.get("/api/spotify/playlists/{playlist_id}/tracks")
async def spotify_playlist_tracks(
    playlist_id: str,
    limit: int = Query(default=100, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    channel_id: Optional[str] = Query(default=None),
) -> dict:
    resolved = resolve_channel_id(channel_id)
    token = _ensure_spotify_token(resolved)
    params = {"limit": min(limit, 100), "offset": max(0, offset)}
    path = _with_query(f"/playlists/{playlist_id}/tracks", params)
    resp = await spotify_request("GET", path, token, resolved)
    if resp.status_code >= 400:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    data = resp.json()
    tracks: list[dict] = []
    base_position = data.get("offset", params["offset"])
    for idx, item in enumerate(data.get("items", [])):
        mapped = _map_spotify_track(item, position=base_position + idx)
        if mapped:
            tracks.append(mapped)
    return {
        "items": tracks,
        "limit": data.get("limit", params["limit"]),
        "offset": data.get("offset", params["offset"]),
        "total": data.get("total", len(tracks)),
        "next": bool(data.get("next")),
        "previous": bool(data.get("previous")),
    }


@app.get("/api/spotify/playlists/{playlist_id}/summary")
async def spotify_playlist_summary(playlist_id: str, channel_id: Optional[str] = Query(default=None)) -> dict:
    resolved = resolve_channel_id(channel_id)
    token = _ensure_spotify_token(resolved)
    total_duration = 0
    total_tracks = None
    offset = 0
    limit = 100
    while True:
        params = {
            "limit": limit,
            "offset": offset,
            "fields": "items(track(duration_ms,is_local)),total,next,offset,limit",
        }
        path = _with_query(f"/playlists/{playlist_id}/tracks", params)
        resp = await spotify_request("GET", path, token, resolved)
        if resp.status_code >= 400:
            raise HTTPException(status_code=resp.status_code, detail=resp.text)
        data = resp.json()
        items = data.get("items") or []
        for item in items:
            track = item.get("track") if isinstance(item, dict) else None
            if not isinstance(track, dict):
                continue
            if track.get("is_local") is True:
                continue
            duration = track.get("duration_ms")
            if isinstance(duration, (int, float)):
                total_duration += max(0, int(duration))
        if isinstance(data.get("total"), int):
            total_tracks = data.get("total")
        next_url = data.get("next")
        offset_value = data.get("offset", offset)
        limit_value = data.get("limit", limit)
        if not isinstance(limit_value, int) or limit_value <= 0:
            break
        offset = offset_value + limit_value
        if not next_url:
            break
        if isinstance(total_tracks, int) and offset >= total_tracks:
            break
    return {
        "tracks_total": total_tracks,
        "duration_ms_total": total_duration,
    }


@app.post("/api/spotify/player/play")
async def spotify_play(
    payload: Optional[dict] = Body(default=None),
    device_id: Optional[str] = Query(default=None),
    channel_id: Optional[str] = Query(default=None),
) -> dict:
    resolved = resolve_channel_id(channel_id)
    params = {"device_id": device_id} if device_id else None
    body = payload or None
    return await _spotify_control("/me/player/play", "PUT", body=body, params=params, channel_id=resolved)


@app.post("/api/spotify/player/pause")
async def spotify_pause(
    device_id: Optional[str] = Query(default=None),
    channel_id: Optional[str] = Query(default=None),
) -> dict:
    resolved = resolve_channel_id(channel_id)
    return await _spotify_control("/me/player/pause", "PUT", params={"device_id": device_id}, channel_id=resolved)


@app.post("/api/spotify/player/next")
async def spotify_next(device_id: Optional[str] = Query(default=None), channel_id: Optional[str] = Query(default=None)) -> dict:
    resolved = resolve_channel_id(channel_id)
    return await _spotify_control("/me/player/next", "POST", params={"device_id": device_id}, channel_id=resolved)


@app.post("/api/spotify/player/previous")
async def spotify_prev(device_id: Optional[str] = Query(default=None), channel_id: Optional[str] = Query(default=None)) -> dict:
    resolved = resolve_channel_id(channel_id)
    return await _spotify_control("/me/player/previous", "POST", params={"device_id": device_id}, channel_id=resolved)


@app.post("/api/spotify/player/seek")
async def spotify_seek(
    payload: dict = Body(...),
    device_id: Optional[str] = Query(default=None),
    channel_id: Optional[str] = Query(default=None),
) -> dict:
    pos = payload.get("position_ms")
    if pos is None:
        raise HTTPException(status_code=400, detail="position_ms required")
    params = {"position_ms": int(pos), "device_id": device_id}
    resolved = resolve_channel_id(channel_id)
    return await _spotify_control("/me/player/seek", "PUT", params=params, channel_id=resolved)


@app.post("/api/spotify/player/volume")
async def spotify_volume(
    payload: VolumePayload,
    device_id: Optional[str] = Query(default=None),
    channel_id: Optional[str] = Query(default=None),
) -> dict:
    params = {"volume_percent": payload.percent, "device_id": device_id}
    resolved = resolve_channel_id(channel_id)
    return await _spotify_control("/me/player/volume", "PUT", params=params, channel_id=resolved)


@app.post("/api/spotify/player/shuffle")
async def spotify_shuffle(payload: ShufflePayload, channel_id: Optional[str] = Query(default=None)) -> dict:
    state = "true" if payload.state else "false"
    params = {"state": state, "device_id": payload.device_id}
    resolved = resolve_channel_id(channel_id)
    return await _spotify_control("/me/player/shuffle", "PUT", params=params, channel_id=resolved)


@app.post("/api/spotify/player/repeat")
async def spotify_repeat(payload: RepeatPayload, channel_id: Optional[str] = Query(default=None)) -> dict:
    mode = payload.mode.lower()
    params = {"state": mode, "device_id": payload.device_id}
    resolved = resolve_channel_id(channel_id)
    return await _spotify_control("/me/player/repeat", "PUT", params=params, channel_id=resolved)


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


@app.api_route("/stream/spotify", methods=["GET"])
async def proxy_spotify_stream(request: Request):
    stream_paths = [
        f"http://{SNAPSERVER_HOST}:{SNAPSERVER_PORT}/stream/Spotify",
        f"http://{SNAPSERVER_HOST}:{SNAPSERVER_PORT}/stream/default",
    ]
    last_exc = None
    for upstream in stream_paths:
        try:
            async with httpx.AsyncClient(timeout=None, follow_redirects=True) as client:
                upstream_req = client.build_request(
                    request.method, upstream, headers={"Accept": "*/*"}
                )
                upstream_resp = await client.send(upstream_req, stream=True)
                ct = upstream_resp.headers.get("content-type", "")
                if upstream_resp.status_code >= 400 or ct.startswith("text/html"):
                    continue
                headers = dict(upstream_resp.headers)
                headers["Access-Control-Allow-Origin"] = "*"
                return StreamingResponse(
                    upstream_resp.aiter_raw(),
                    status_code=upstream_resp.status_code,
                    headers=headers,
                    media_type=ct or "audio/flac",
                )
        except Exception as exc:
            last_exc = exc
            continue
    raise HTTPException(status_code=502, detail=str(last_exc or "Stream unavailable"))
