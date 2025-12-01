import json
import asyncio
import ipaddress
import logging
import os
import subprocess
import time
import uuid
from pathlib import Path
from typing import Dict, Optional, AsyncIterator

import httpx
import websockets
from fastapi import Body, FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import FileResponse, JSONResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from itsdangerous import URLSafeSerializer
from pydantic import BaseModel, Field
from webrtc import WebAudioRelay


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
TOKEN_SIGNER = URLSafeSerializer(os.getenv("SPOTIFY_STATE_SECRET", "changeme"))
NODES_PATH = Path(os.getenv("NODES_PATH", "/config/nodes.json"))
WEBRTC_ENABLED = os.getenv("WEBRTC_ENABLED", "1").lower() not in {"0", "false", "no"}
WEBRTC_LATENCY_MS = int(os.getenv("WEBRTC_LATENCY_MS", "150"))
SENSITIVE_NODE_FIELDS = {"agent_secret"}
AGENT_LATEST_VERSION = os.getenv("AGENT_LATEST_VERSION", "0.3.5").strip()
NODE_RESTART_TIMEOUT = int(os.getenv("NODE_RESTART_TIMEOUT", "120"))
NODE_RESTART_INTERVAL = int(os.getenv("NODE_RESTART_INTERVAL", "5"))
NODE_HEALTH_INTERVAL = int(os.getenv("NODE_HEALTH_INTERVAL", "30"))


class NodeRegistration(BaseModel):
    id: Optional[str] = None
    name: str
    url: str = Field(min_length=1)
    fingerprint: Optional[str] = None


class RenameNodePayload(BaseModel):
    name: str = Field(min_length=1, max_length=100)


class VolumePayload(BaseModel):
    percent: int = Field(ge=0, le=100)


class EqBand(BaseModel):
    freq: float = Field(gt=10, lt=24000, description="Frequency in Hz")
    gain: float = Field(ge=-24, le=24, description="Gain in dB")
    q: float = Field(default=1.0, gt=0.05, lt=36, description="Quality factor")


class EqPayload(BaseModel):
    preset: Optional[str] = None
    bands: list[EqBand] = Field(default_factory=list)
    band_count: int = Field(default=15, ge=1, le=31)


class PanPayload(BaseModel):
    pan: float = Field(ge=-1.0, le=1.0)


class OutputSelectionPayload(BaseModel):
    device: str = Field(min_length=1)


class WebNodeOffer(BaseModel):
    name: str = Field(default="Web node", min_length=1)
    sdp: str
    type: str


class SpotifyConfig(BaseModel):
    device_name: str = Field(default="RoomCast")
    bitrate: int = Field(default=320, ge=96, le=320)
    initial_volume: int = Field(default=75, ge=0, le=100)
    normalisation: bool = Field(default=True)
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    redirect_uri: Optional[str] = None


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
            for client in group.get("clients", []):
                clients.append(client)
        return clients


snapcast = SnapcastClient(SNAPSERVER_HOST, SNAPSERVER_PORT)
app = FastAPI(title="RoomCast Controller", version="0.1.0")
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

nodes: Dict[str, dict] = {}
browser_ws: Dict[str, WebSocket] = {}
node_watchers: set[WebSocket] = set()
webrtc_relay: Optional[WebAudioRelay] = None
DEFAULT_EQ_PRESET = "peq15"
pending_restarts: Dict[str, dict] = {}
agent_refresh_tasks: Dict[str, asyncio.Task] = {}
node_health_task: Optional[asyncio.Task] = None


def default_eq_state() -> dict:
    return {"preset": DEFAULT_EQ_PRESET, "band_count": 15, "bands": []}


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
    return data


def public_nodes() -> list[dict]:
    return [public_node(node) for node in nodes.values()]


async def broadcast_nodes() -> None:
    if not node_watchers:
        return
    payload = {"type": "nodes", "nodes": public_nodes()}
    dead: list[WebSocket] = []
    for ws in list(node_watchers):
        try:
            await ws.send_json(payload)
        except Exception:
            dead.append(ws)
    for ws in dead:
        node_watchers.discard(ws)


def _normalize_node_url(raw: str) -> str:
    value = (raw or "").strip()
    if not value:
        return value
    if value.startswith("browser:"):
        return value.rstrip("/")
    if "://" not in value:
        value = f"http://{value}"
    return value.rstrip("/")


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
        "pan": previous.get("pan", 0.0),
        "eq": default_eq_state(),
        "agent_secret": previous.get("agent_secret"),
        "audio_configured": True if node_type == "browser" else previous.get("audio_configured", False),
        "agent_version": previous.get("agent_version"),
        "volume_percent": previous.get("volume_percent", 75),
        "muted": previous.get("muted", False),
        "updating": bool(previous.get("updating", False)),
        "playback_device": previous.get("playback_device"),
        "outputs": previous.get("outputs", {}),
        "fingerprint": fingerprint or previous.get("fingerprint"),
        "online": True,
        "offline_since": None,
    }
    save_nodes()
    return nodes[node_id]


def create_browser_node(name: str) -> dict:
    reg = NodeRegistration(id=str(uuid.uuid4()), name=name, url=f"browser:{uuid.uuid4()}")
    return public_node(_register_node_internal(reg))


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
        "snapserver_host": SNAPSERVER_HOST,
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
            return False, changed
        data = resp.json()
    except Exception:
        changed = _mark_node_offline(node, timestamp=now)
        if changed and persist:
            save_nodes()
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
    if "configured" in data:
        configured = bool(data.get("configured"))
        if node.get("audio_configured") != configured:
            node["audio_configured"] = configured
            changed = True
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
            item.setdefault("pan", 0.0)
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
            item["volume_percent"] = int(item.get("volume_percent", 75))
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
            nodes[item["id"]] = item
    except Exception:
        nodes = {}


def save_nodes() -> None:
    NODES_PATH.write_text(json.dumps(list(nodes.values()), indent=2))


load_nodes()


@app.on_event("startup")
async def _startup_events() -> None:
    global webrtc_relay, node_health_task
    if WEBRTC_ENABLED:
        webrtc_relay = WebAudioRelay(
            snap_host=SNAPSERVER_HOST,
            snap_port=SNAPCLIENT_PORT,
            latency_ms=WEBRTC_LATENCY_MS,
            on_session_closed=_handle_webrtc_session_closed,
        )
        await webrtc_relay.start()
    if node_health_task is None:
        node_health_task = asyncio.create_task(_node_health_loop())


@app.on_event("shutdown")
async def _shutdown_events() -> None:
    global node_health_task
    if webrtc_relay:
        await webrtc_relay.stop()
    if node_health_task:
        node_health_task.cancel()
        try:
            await node_health_task
        except asyncio.CancelledError:
            pass
        node_health_task = None


def current_spotify_creds() -> tuple[str, str, str]:
    cfg = read_spotify_config()
    cid = (cfg.get("client_id") or os.getenv("SPOTIFY_CLIENT_ID") or "").strip()
    secret = cfg.get("client_secret") or os.getenv("SPOTIFY_CLIENT_SECRET") or ""
    redirect = cfg.get("redirect_uri") or os.getenv("SPOTIFY_REDIRECT_URI") or SPOTIFY_REDIRECT_URI
    return cid, secret, redirect


def read_spotify_config() -> dict:
    if not CONFIG_PATH.exists():
        return {
            "username": "",
            "device_name": "RoomCast",
            "bitrate": 320,
            "initial_volume": 75,
            "normalisation": True,
            "has_password": False,
            "client_id": SPOTIFY_CLIENT_ID,
            "client_secret": "***" if SPOTIFY_CLIENT_SECRET else "",
            "redirect_uri": SPOTIFY_REDIRECT_URI,
        }
    try:
        data = json.loads(CONFIG_PATH.read_text())
    except json.JSONDecodeError:
        data = {}
    return {
        "username": data.get("username", ""),
        "device_name": data.get("device_name", "RoomCast"),
        "bitrate": data.get("bitrate", 320),
        "initial_volume": data.get("initial_volume", 75),
        "normalisation": data.get("normalisation", True),
        "has_password": bool(data.get("password")),
        "client_id": data.get("client_id", SPOTIFY_CLIENT_ID),
        "has_client_secret": bool(data.get("client_secret") or SPOTIFY_CLIENT_SECRET),
        "redirect_uri": data.get("redirect_uri", SPOTIFY_REDIRECT_URI),
    }


def read_librespot_status() -> dict:
    if not LIBRESPOT_STATUS_PATH.exists():
        return {"state": "unknown", "message": "No status yet"}
    try:
        return json.loads(LIBRESPOT_STATUS_PATH.read_text())
    except json.JSONDecodeError:
        return {"state": "unknown", "message": "Invalid status file"}


def load_token() -> Optional[dict]:
    if not SPOTIFY_TOKEN_PATH.exists():
        return None
    try:
        return json.loads(SPOTIFY_TOKEN_PATH.read_text())
    except json.JSONDecodeError:
        return None


def save_token(data: dict) -> None:
    SPOTIFY_TOKEN_PATH.write_text(json.dumps(data, indent=2))


async def spotify_refresh(token: dict) -> dict:
    if not token or "refresh_token" not in token:
        raise HTTPException(status_code=401, detail="Spotify not authorized")
    cid, secret, _ = current_spotify_creds()
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
    save_token(token)
    return token


async def spotify_request(method: str, path: str, token: dict, **kwargs) -> httpx.Response:
    headers = kwargs.pop("headers", {})
    headers["Authorization"] = f"Bearer {token['access_token']}"
    url = f"https://api.spotify.com/v1{path}"
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.request(method, url, headers=headers, **kwargs)
    if resp.status_code == 401:
        token = await spotify_refresh(token)
        headers["Authorization"] = f"Bearer {token['access_token']}"
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.request(method, url, headers=headers, **kwargs)
    return resp


@app.get("/api/health")
async def health() -> dict:
    return {"status": "ok"}


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


@app.post("/api/nodes/register")
async def register_node(reg: NodeRegistration) -> dict:
    normalized_url = _normalize_node_url(reg.url)
    reg.url = normalized_url
    fingerprint = reg.fingerprint or await _fetch_agent_fingerprint(normalized_url)
    node = _register_node_internal(reg, fingerprint=fingerprint, normalized_url=normalized_url)
    if node.get("type") == "agent":
        try:
            secret = await request_agent_secret(node, force=True)
            node["agent_secret"] = secret
            await configure_agent_audio(node)
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


@app.post("/api/nodes/{node_id}/volume")
async def set_node_volume(node_id: str, payload: VolumePayload) -> dict:
    node = nodes.get(node_id)
    if not node:
        raise HTTPException(status_code=404, detail="Unknown node")
    if node.get("type") == "browser":
        ws = browser_ws.get(node_id)
        if not ws:
            raise HTTPException(status_code=503, detail="Browser node not connected")
        await ws.send_json({"type": "volume", "percent": payload.percent})
        result = {"sent": True}
    else:
        result = await _call_agent(node, "/volume", payload.model_dump())
    node["volume_percent"] = int(payload.percent)
    save_nodes()
    await broadcast_nodes()
    return {"ok": True, "result": result}


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


@app.post("/api/nodes/{node_id}/pan")
async def set_node_pan(node_id: str, payload: PanPayload) -> dict:
    node = nodes.get(node_id)
    if not node:
        raise HTTPException(status_code=404, detail="Unknown node")
    if node.get("type") != "browser":
        raise HTTPException(status_code=400, detail="Pan is only supported for web nodes")
    node["pan"] = payload.pan
    save_nodes()
    if webrtc_relay:
        await webrtc_relay.set_pan(node_id, payload.pan)
    await broadcast_nodes()
    return {"ok": True, "pan": payload.pan}


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


@app.post("/api/nodes/{node_id}/configure")
async def configure_node(node_id: str) -> dict:
    node = nodes.get(node_id)
    if not node:
        raise HTTPException(status_code=404, detail="Unknown node")
    if node.get("type") == "browser":
        raise HTTPException(status_code=400, detail="Browser nodes do not require configuration")
    result = await configure_agent_audio(node)
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


@app.delete("/api/nodes/{node_id}")
async def unregister_node(node_id: str) -> dict:
    node = nodes.pop(node_id, None)
    if not node:
        raise HTTPException(status_code=404, detail="Unknown node")
    save_nodes()
    await broadcast_nodes()
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


@app.get("/api/nodes")
async def list_nodes() -> dict:
    return {"nodes": public_nodes()}


@app.post("/api/web-nodes/session")
async def web_node_session(payload: WebNodeOffer) -> dict:
    if not WEBRTC_ENABLED or not webrtc_relay:
        raise HTTPException(status_code=503, detail="Web nodes are disabled")
    name = payload.name.strip() or "Web node"
    node = create_browser_node(name)
    try:
        session = await webrtc_relay.create_session(node["id"], pan=node.get("pan", 0.0))
        answer = await session.accept(payload.sdp, payload.type)
    except Exception as exc:
        log.exception("Failed to establish web node session")
        await teardown_browser_node(node["id"])
        raise HTTPException(status_code=500, detail=f"Failed to start WebRTC session: {exc}")
    await broadcast_nodes()
    return {"node": node, "answer": answer.sdp, "answer_type": answer.type}


@app.websocket("/ws/web-node")
async def web_node_ws(ws: WebSocket):
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
    await ws.accept()
    node_watchers.add(ws)
    try:
        await ws.send_json({"type": "nodes", "nodes": public_nodes()})
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        node_watchers.discard(ws)


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


@app.get("/api/config/spotify")
async def get_spotify_config() -> dict:
    return read_spotify_config()


@app.post("/api/config/spotify")
async def set_spotify_config(cfg: SpotifyConfig) -> dict:
    payload = cfg.model_dump()
    existing = {}
    if CONFIG_PATH.exists():
        try:
            existing = json.loads(CONFIG_PATH.read_text())
        except json.JSONDecodeError:
            existing = {}

    if not payload.get("client_secret"):
        payload["client_secret"] = existing.get("client_secret") or os.getenv("SPOTIFY_CLIENT_SECRET")
    if not payload.get("client_id"):
        payload["client_id"] = existing.get("client_id") or os.getenv("SPOTIFY_CLIENT_ID")
    if not payload.get("redirect_uri"):
        payload["redirect_uri"] = existing.get("redirect_uri") or SPOTIFY_REDIRECT_URI

    if payload.get("client_id"):
        os.environ["SPOTIFY_CLIENT_ID"] = payload["client_id"]
    if payload.get("client_secret"):
        os.environ["SPOTIFY_CLIENT_SECRET"] = payload["client_secret"]
    if payload.get("redirect_uri"):
        os.environ["SPOTIFY_REDIRECT_URI"] = payload["redirect_uri"]

    CONFIG_PATH.write_text(json.dumps(payload, indent=2))
    return {"ok": True, "config": read_spotify_config()}


@app.get("/api/librespot/status")
async def librespot_status() -> dict:
    return read_librespot_status()


@app.get("/api/spotify/auth-url")
async def spotify_auth_url() -> dict:
    cid, secret, redirect = current_spotify_creds()
    if not (cid and secret):
        raise HTTPException(status_code=400, detail="Spotify client_id/client_secret not set")
    state = TOKEN_SIGNER.dumps({"t": time.time()})
    scope = "user-read-playback-state user-modify-playback-state streaming user-read-email user-read-private"
    params = {
        "client_id": cid,
        "response_type": "code",
        "redirect_uri": redirect,
        "scope": scope,
        "state": state,
    }
    from urllib.parse import urlencode

    return {"url": f"https://accounts.spotify.com/authorize?{urlencode(params)}"}


@app.get("/api/spotify/callback", include_in_schema=False)
async def spotify_callback(code: str, state: str):
    try:
        TOKEN_SIGNER.loads(state)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid state")

    cid, secret, redirect = current_spotify_creds()

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
    save_token(token)
    return Response(content="Spotify linked. You can close this tab.", media_type="text/plain")


@app.get("/api/spotify/player/status")
async def spotify_player_status() -> dict:
    token = load_token()
    if not token or not token.get("access_token"):
        raise HTTPException(status_code=401, detail="Spotify not authorized")
    resp = await spotify_request("GET", "/me/player/currently-playing", token)
    if resp.status_code == 204:
        return {"active": False}
    if resp.status_code >= 400:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    data = resp.json()
    return {
        "active": True,
        "is_playing": data.get("is_playing", False),
        "progress_ms": data.get("progress_ms"),
        "device": data.get("device", {}),
        "item": data.get("item", {}),
    }


async def _spotify_control(path: str, method: str = "POST", body: Optional[dict] = None):
    token = load_token()
    if not token:
        raise HTTPException(status_code=401, detail="Spotify not authorized")
    resp = await spotify_request(method, path, token, json=body or {})
    if resp.status_code >= 400:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    return {"ok": True}


@app.post("/api/spotify/player/play")
async def spotify_play() -> dict:
    return await _spotify_control("/me/player/play", "PUT")


@app.post("/api/spotify/player/pause")
async def spotify_pause() -> dict:
    return await _spotify_control("/me/player/pause", "PUT")


@app.post("/api/spotify/player/next")
async def spotify_next() -> dict:
    return await _spotify_control("/me/player/next", "POST")


@app.post("/api/spotify/player/previous")
async def spotify_prev() -> dict:
    return await _spotify_control("/me/player/previous", "POST")


@app.post("/api/spotify/player/seek")
async def spotify_seek(payload: dict = Body(...)) -> dict:
    pos = payload.get("position_ms")
    if pos is None:
        raise HTTPException(status_code=400, detail="position_ms required")
    return await _spotify_control(f"/me/player/seek?position_ms={int(pos)}", "PUT")


@app.post("/api/spotify/player/volume")
async def spotify_volume(payload: VolumePayload) -> dict:
    return await _spotify_control(f"/me/player/volume?volume_percent={payload.percent}", "PUT")


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
