import json
import asyncio
import ipaddress
import logging
import os
import time
import uuid
from pathlib import Path
from typing import Dict, Optional

import httpx
import websockets
from fastapi import Body, FastAPI, HTTPException, WebSocket, Request
from fastapi.responses import FileResponse, JSONResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from itsdangerous import URLSafeSerializer
from pydantic import BaseModel, Field


logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("roomcast")

SNAPSERVER_HOST = os.getenv("SNAPSERVER_HOST", "snapserver")
SNAPSERVER_PORT = int(os.getenv("SNAPSERVER_PORT", "1780"))
AGENT_SHARED_SECRET = os.getenv("AGENT_SHARED_SECRET", "changeme")
CONFIG_PATH = Path(os.getenv("CONFIG_PATH", "/config/spotify.json"))
CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
STATIC_DIR = Path(__file__).parent / "static"
LIBRESPOT_STATUS_PATH = Path(os.getenv("STATUS_PATH", "/config/librespot-status.json"))
DISCOVERY_CIDR = os.getenv("DISCOVERY_CIDR", "192.168.1.0/24")
AGENT_PORT = int(os.getenv("AGENT_PORT", "9700"))
SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID", "")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET", "")
SPOTIFY_REDIRECT_URI = os.getenv("SPOTIFY_REDIRECT_URI", "http://localhost:8000/api/spotify/callback")
SPOTIFY_TOKEN_PATH = Path(os.getenv("SPOTIFY_TOKEN_PATH", "/config/spotify-token.json"))
TOKEN_SIGNER = URLSafeSerializer(os.getenv("SPOTIFY_STATE_SECRET", "changeme"))
NODES_PATH = Path(os.getenv("NODES_PATH", "/config/nodes.json"))


class NodeRegistration(BaseModel):
    id: Optional[str] = None
    name: str
    url: str = Field(min_length=1)


class VolumePayload(BaseModel):
    percent: int = Field(ge=0, le=100)


class EqBand(BaseModel):
    freq: float = Field(gt=10, lt=24000, description="Frequency in Hz")
    gain: float = Field(ge=-24, le=24, description="Gain in dB")
    q: float = Field(default=1.0, gt=0.05, lt=36, description="Quality factor")


class EqPayload(BaseModel):
    preset: Optional[str] = None
    bands: list[EqBand] = Field(default_factory=list)


class SpotifyConfig(BaseModel):
    username: Optional[str] = ""
    password: Optional[str] = ""
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


def load_nodes() -> None:
    global nodes
    if not NODES_PATH.exists():
        nodes = {}
        return
    try:
        data = json.loads(NODES_PATH.read_text())
        nodes = {item["id"]: item for item in data}
    except Exception:
        nodes = {}


def save_nodes() -> None:
    NODES_PATH.write_text(json.dumps(list(nodes.values()), indent=2))


load_nodes()


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
    for existing in nodes.values():
        if existing["url"].rstrip("/") == str(reg.url).rstrip("/"):
            return existing
    node_id = reg.id or str(uuid.uuid4())
    node_type = "browser" if str(reg.url).startswith("browser:") else "agent"
    nodes[node_id] = {
        "id": node_id,
        "name": reg.name,
        "url": str(reg.url).rstrip("/"),
        "last_seen": time.time(),
        "type": node_type,
    }
    save_nodes()
    return nodes[node_id]


async def _call_agent(node: dict, path: str, payload: dict) -> dict:
    url = f"{node['url']}{path}"
    headers = {"X-Agent-Secret": AGENT_SHARED_SECRET}
    async with httpx.AsyncClient(timeout=5) as client:
        resp = await client.post(url, json=payload, headers=headers)
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
    if node.get("type") == "browser":
        ws = browser_ws.get(node_id)
        if not ws:
            raise HTTPException(status_code=503, detail="Browser node not connected")
        await ws.send_json({"type": "eq", "bands": payload.model_dump()["bands"]})
        result = {"sent": True}
    else:
        result = await _call_agent(node, "/eq", payload.model_dump())
    return {"ok": True, "result": result}


@app.delete("/api/nodes/{node_id}")
async def unregister_node(node_id: str) -> dict:
    node = nodes.pop(node_id, None)
    if not node:
        raise HTTPException(status_code=404, detail="Unknown node")
    save_nodes()
    return {"ok": True, "removed": node_id}


@app.get("/api/nodes")
async def list_nodes() -> dict:
    return {"nodes": list(nodes.values())}


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


async def _probe_host(host: str) -> Optional[dict]:
    url = f"http://{host}:{AGENT_PORT}"
    headers = {"X-Agent-Secret": AGENT_SHARED_SECRET}
    try:
        async with httpx.AsyncClient(timeout=2) as client:
            resp = await client.get(f"{url}/health", headers=headers)
        if resp.status_code == 200:
            return {"host": host, "url": f"{url}", "healthy": True}
    except Exception:
        return None
    return None


@app.get("/api/nodes/discover")
async def discover_nodes(cidr: Optional[str] = None) -> dict:
    target_cidr = cidr or DISCOVERY_CIDR
    try:
        net = ipaddress.ip_network(target_cidr, strict=False)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid CIDR")
    hosts_list = list(net.hosts())
    if len(hosts_list) > 4096:
        raise HTTPException(status_code=400, detail="CIDR too large; please narrow the range")
    hosts = [str(h) for h in hosts_list]
    results = []
    sem = asyncio.Semaphore(25)

    async def probe(h: str):
        async with sem:
            res = await _probe_host(h)
            if res:
                results.append(res)

    await asyncio.gather(*(probe(h) for h in hosts))
    return {"discovered": results, "cidr": target_cidr}


@app.get("/api/config/spotify")
async def get_spotify_config() -> dict:
    return read_spotify_config()


@app.post("/api/config/spotify")
async def set_spotify_config(cfg: SpotifyConfig) -> dict:
    payload = cfg.model_dump()
    # Merge with existing so blank/placeholder secrets don't erase values
    existing = {}
    if CONFIG_PATH.exists():
        try:
            existing = json.loads(CONFIG_PATH.read_text())
        except json.JSONDecodeError:
            existing = {}

    if not payload.get("password"):
        payload["password"] = existing.get("password")
    if not payload.get("client_secret"):
        payload["client_secret"] = existing.get("client_secret") or os.getenv("SPOTIFY_CLIENT_SECRET")
    if not payload.get("client_id"):
        payload["client_id"] = existing.get("client_id") or os.getenv("SPOTIFY_CLIENT_ID")
    if not payload.get("redirect_uri"):
        payload["redirect_uri"] = existing.get("redirect_uri") or SPOTIFY_REDIRECT_URI

    # Update env-backed defaults if provided
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
    scope = "user-read-playback-state user-modify-playback-state"
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
