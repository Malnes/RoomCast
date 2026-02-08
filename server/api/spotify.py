import asyncio
import json
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional

import httpx
from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from api.nodes import VolumePayload


class ShufflePayload(BaseModel):
    state: bool = Field(description="Enable shuffle when true")
    device_id: Optional[str] = Field(default=None, description="Target device ID")


class RepeatPayload(BaseModel):
    mode: str = Field(pattern="^(off|track|context)$", description="Repeat mode")
    device_id: Optional[str] = Field(default=None, description="Target device ID")


class ActivateRoomcastPayload(BaseModel):
    play: bool = Field(default=False, description="Start playback immediately after transfer")


class SpotifyConfig(BaseModel):
    device_name: str = Field(default="RoomCast")
    bitrate: int = Field(default=320, ge=96, le=320)
    initial_volume: int = Field(default=75, ge=0, le=100)
    normalisation: bool = Field(default=True)
    show_output_volume_slider: bool = Field(default=True)


class SpotifyBrokerCallback(BaseModel):
    device_id: Optional[str] = None
    source_id: Optional[str] = None
    token: dict = Field(default_factory=dict)
    received_at: Optional[int] = None


class PlaylistAddTracksPayload(BaseModel):
    track_uri: Optional[str] = Field(default=None, description="Single Spotify track URI")
    uris: Optional[list[str]] = Field(default=None, description="List of Spotify track URIs")


def create_spotify_router(
    *,
    require_spotify_provider_dep: Callable[[], None],
    resolve_channel_id: Callable[[Optional[str]], str],
    resolve_spotify_source_id: Callable[[Optional[str]], Optional[str]],
    read_spotify_config: Callable[[str], dict],
    get_spotify_source: Callable[[str], dict],
    spotify_auth_broker_url: str,
    public_base_url: str,
    save_token: Callable[[dict, str], None],
    delete_token: Callable[[str], None],
    ensure_spotify_token: Callable[[Optional[str]], dict],
    load_token: Callable[[Optional[str]], Optional[dict]],
    spotify_request: Callable[..., Awaitable[Any]],
    find_roomcast_device: Callable[[dict, Optional[str]], Awaitable[Optional[dict]]],
    preferred_roomcast_device_names: Callable[[Optional[str]], list[str]],
    get_player_snapshot: Callable[[Optional[str]], Optional[dict]],
    public_player_snapshot: Callable[[Optional[dict]], Optional[dict]],
    set_player_snapshot: Callable[[str, dict], None],
    map_spotify_track_simple: Callable[[Any], Optional[dict]],
    map_spotify_search_bucket: Callable[[Any, Callable[[Any], Optional[dict]]], list[dict]],
    map_spotify_album: Callable[[Any], Optional[dict]],
    map_spotify_artist: Callable[[Any], Optional[dict]],
    map_spotify_playlist: Callable[[Any], Optional[dict]],
    map_spotify_track: Callable[..., Optional[dict]],
    spotify_search_types: list[str],
    snapserver_host: str,
    snapserver_port: int,
    read_librespot_status: Callable[[str], dict],
) -> APIRouter:
    router = APIRouter()

    def _broker_callback_url() -> str:
        if not public_base_url:
            raise HTTPException(status_code=500, detail="RoomCast public base URL not configured")
        return f"{public_base_url.rstrip('/')}/api/spotify/broker/callback"

    async def _fetch_spotify_profile(access_token: str) -> Optional[dict]:
        if not access_token:
            return None
        headers = {"Authorization": f"Bearer {access_token}"}
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get("https://api.spotify.com/v1/me", headers=headers)
        if resp.status_code >= 400:
            return None
        data = resp.json()
        return data if isinstance(data, dict) else None

    def _store_spotify_username(spotify_source_id: str, username: str) -> None:
        if not username:
            return
        source = get_spotify_source(spotify_source_id)
        cfg_path = Path(source["config_path"])
        payload: dict = {}
        if cfg_path.exists():
            try:
                payload = json.loads(cfg_path.read_text())
            except json.JSONDecodeError:
                payload = {}
        payload["username"] = username
        cfg_path.parent.mkdir(parents=True, exist_ok=True)
        cfg_path.write_text(json.dumps(payload, indent=2))

    def _with_query(path: str, params: Optional[dict] = None) -> str:
        if not params:
            return path
        clean = {k: v for k, v in (params or {}).items() if v is not None}
        if not clean:
            return path
        separator = "&" if "?" in path else "?"
        from urllib.parse import urlencode

        return f"{path}{separator}{urlencode(clean)}"

    async def _spotify_control(
        path: str,
        method: str = "POST",
        body: Optional[dict] = None,
        params: Optional[dict] = None,
        channel_id: Optional[str] = None,
    ) -> dict:
        token = load_token(channel_id)
        if not token:
            raise HTTPException(status_code=401, detail="Spotify not authorized")
        target = _with_query(path, params)
        resp = await spotify_request(method, target, token, channel_id, json=body or {})
        if resp.status_code >= 400:
            raise HTTPException(status_code=resp.status_code, detail=resp.text)
        return {"ok": True}

    def _error_detail_text(detail: Any) -> str:
        if detail is None:
            return ""
        if isinstance(detail, str):
            return detail
        if isinstance(detail, (dict, list)):
            try:
                return json.dumps(detail)
            except Exception:
                return str(detail)
        return str(detail)

    def _is_recoverable_player_error(exc: HTTPException) -> bool:
        if exc.status_code not in {403, 404}:
            return False
        detail = _error_detail_text(exc.detail).lower()
        if exc.status_code == 404:
            return "no active device" in detail
        if "restricted" in detail or "restriction" in detail:
            return True
        return '"reason"' in detail and '"unknown"' in detail

    async def _transfer_playback_to_roomcast(resolved_channel_id: str) -> str:
        token = ensure_spotify_token(resolved_channel_id)
        device = await find_roomcast_device(token, resolved_channel_id)
        if not device or not device.get("id"):
            raise HTTPException(status_code=404, detail="RoomCast device is not available")
        transfer_body = {"device_ids": [device["id"]], "play": False}
        resp = await spotify_request("PUT", "/me/player", token, resolved_channel_id, json=transfer_body)
        if resp.status_code >= 400:
            raise HTTPException(status_code=resp.status_code, detail=resp.text)
        return str(device["id"])

    async def _spotify_player_control_with_recovery(
        path: str,
        method: str,
        *,
        channel_id: str,
        body: Optional[dict] = None,
        params: Optional[dict] = None,
        settle_delay: float = 0.6,
    ) -> dict:
        try:
            return await _spotify_control(path, method, body=body, params=params, channel_id=channel_id)
        except HTTPException as exc:
            if not _is_recoverable_player_error(exc):
                raise
        roomcast_device_id = await _transfer_playback_to_roomcast(channel_id)
        retry_params = dict(params or {})
        retry_params["device_id"] = roomcast_device_id
        try:
            return await _spotify_control(path, method, body=body, params=retry_params, channel_id=channel_id)
        except HTTPException as exc:
            if settle_delay <= 0 or not _is_recoverable_player_error(exc):
                raise
        await asyncio.sleep(settle_delay)
        return await _spotify_control(path, method, body=body, params=retry_params, channel_id=channel_id)

    @router.get("/api/librespot/status")
    async def librespot_status(
        channel_id: Optional[str] = Query(default=None),
        source_id: Optional[str] = Query(default=None),
        _: None = Depends(require_spotify_provider_dep),
    ) -> dict:
        # Support querying by spotify source instance (spotify:a/b) to keep the UI
        # stable even when channels switch between Spotify and Radio.
        target = source_id if source_id else channel_id
        spotify_source_id = resolve_spotify_source_id(target)
        if spotify_source_id is None:
            return {"state": "unknown", "message": "Not a Spotify source"}
        return read_librespot_status(spotify_source_id)

    @router.get("/api/config/spotify")
    async def get_spotify_config(
        channel_id: Optional[str] = Query(default=None),
        source_id: Optional[str] = Query(default=None),
        _: None = Depends(require_spotify_provider_dep),
    ) -> dict:
        target = source_id if source_id else resolve_channel_id(channel_id)
        return read_spotify_config(target)

    @router.post("/api/config/spotify")
    async def set_spotify_config(
        cfg: SpotifyConfig,
        channel_id: Optional[str] = Query(default=None),
        source_id: Optional[str] = Query(default=None),
        _: None = Depends(require_spotify_provider_dep),
    ) -> dict:
        resolved_channel = resolve_channel_id(channel_id)
        target = source_id if source_id else resolved_channel
        spotify_source_id = resolve_spotify_source_id(target)
        if spotify_source_id is None:
            raise HTTPException(status_code=400, detail="Spotify source not configured")
        source = get_spotify_source(spotify_source_id)
        cfg_path = Path(source["config_path"])
        payload = cfg.model_dump()
        cfg_path.parent.mkdir(parents=True, exist_ok=True)
        cfg_path.write_text(json.dumps(payload, indent=2))
        return {"ok": True, "config": read_spotify_config(target)}

    @router.get("/api/spotify/auth-url")
    async def spotify_auth_url(
        request: Request,
        channel_id: Optional[str] = Query(default=None),
        source_id: Optional[str] = Query(default=None),
        _: None = Depends(require_spotify_provider_dep),
    ) -> dict:
        resolved_channel = resolve_channel_id(channel_id)
        target = source_id if source_id else resolved_channel
        spotify_source_id = resolve_spotify_source_id(target)
        if spotify_source_id is None:
            raise HTTPException(status_code=400, detail="Spotify source not configured")
        if not spotify_auth_broker_url:
            raise HTTPException(status_code=503, detail="Spotify auth broker not configured")
        callback_url = _broker_callback_url()
        payload = {
            "callback_url": callback_url,
            "source_id": spotify_source_id,
        }
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(
                f"{spotify_auth_broker_url.rstrip('/')}/authorize",
                json=payload,
            )
        if resp.status_code >= 400:
            detail = resp.text
            raise HTTPException(status_code=resp.status_code, detail=f"Auth broker error: {detail}")
        data = resp.json()
        if not isinstance(data, dict) or not data.get("url"):
            raise HTTPException(status_code=502, detail="Auth broker did not return a URL")
        return {"url": data["url"]}

    @router.post("/api/spotify/broker/callback", name="spotify_broker_callback")
    async def spotify_broker_callback(
        payload: SpotifyBrokerCallback,
        _: None = Depends(require_spotify_provider_dep),
    ) -> dict:
        spotify_source_id = resolve_spotify_source_id(payload.source_id)
        if spotify_source_id is None:
            raise HTTPException(status_code=400, detail="Invalid source")
        token = payload.token or {}
        if "access_token" not in token:
            raise HTTPException(status_code=400, detail="Invalid token payload")
        save_token(token, spotify_source_id)
        try:
            profile = await _fetch_spotify_profile(token.get("access_token"))
            username = (profile or {}).get("display_name") or (profile or {}).get("id")
            if isinstance(username, str) and username.strip():
                _store_spotify_username(spotify_source_id, username.strip())
        except Exception:
            pass
        return {"ok": True}

    @router.post("/api/spotify/logout")
    async def spotify_logout(
        channel_id: Optional[str] = Query(default=None),
        source_id: Optional[str] = Query(default=None),
        _: None = Depends(require_spotify_provider_dep),
    ) -> dict:
        resolved_channel = resolve_channel_id(channel_id)
        target = source_id if source_id else resolved_channel
        spotify_source_id = resolve_spotify_source_id(target)
        if spotify_source_id is None:
            raise HTTPException(status_code=400, detail="Spotify source not configured")
        delete_token(spotify_source_id)
        return {"ok": True}

    @router.get("/api/spotify/player/status")
    async def spotify_player_status(
        channel_id: Optional[str] = Query(default=None),
        _: None = Depends(require_spotify_provider_dep),
    ) -> dict:
        resolved = resolve_channel_id(channel_id)
        token = ensure_spotify_token(resolved)
        try:
            resp = await spotify_request("GET", "/me/player", token, resolved)
        except httpx.RequestError as exc:
            raise HTTPException(status_code=503, detail=f"Spotify API request failed: {exc}") from exc
        if resp.status_code == 204:
            snapshot = public_player_snapshot(get_player_snapshot(resolved))
            payload: dict = {"active": False}
            if snapshot:
                payload["snapshot"] = snapshot
            return payload
        if resp.status_code >= 400:
            raise HTTPException(status_code=resp.status_code, detail=resp.text)
        data = resp.json()
        active = bool(data.get("device"))
        device = data.get("device") or {}
        preferred_names = [name.lower() for name in preferred_roomcast_device_names(resolved)]
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
            set_player_snapshot(resolved, data)
        if not active:
            snapshot = public_player_snapshot(get_player_snapshot(resolved))
            if snapshot:
                payload["snapshot"] = snapshot
        return payload

    @router.get("/api/spotify/player/queue")
    async def spotify_player_queue(
        channel_id: Optional[str] = Query(default=None),
        _: None = Depends(require_spotify_provider_dep),
    ) -> dict:
        resolved = resolve_channel_id(channel_id)
        token = ensure_spotify_token(resolved)
        resp = await spotify_request("GET", "/me/player/queue", token, resolved)
        if resp.status_code >= 400:
            raise HTTPException(status_code=resp.status_code, detail=resp.text)
        data = resp.json()
        queue_items: list[dict] = []
        for raw in data.get("queue") or []:
            mapped = map_spotify_track_simple(raw)
            if mapped:
                queue_items.append(mapped)
        current = map_spotify_track_simple(data.get("currently_playing"))
        return {
            "current": current,
            "queue": queue_items,
        }

    @router.post("/api/spotify/player/activate-roomcast")
    async def spotify_activate_roomcast(
        payload: ActivateRoomcastPayload = Body(default=ActivateRoomcastPayload()),
        channel_id: Optional[str] = Query(default=None),
        _: None = Depends(require_spotify_provider_dep),
    ) -> dict:
        resolved = resolve_channel_id(channel_id)
        token = ensure_spotify_token(resolved)
        device = await find_roomcast_device(token, resolved)
        if not device or not device.get("id"):
            raise HTTPException(
                status_code=404,
                detail="RoomCast device is not available. Make sure Librespot is running and linked to Spotify.",
            )
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

    @router.get("/api/spotify/search")
    async def spotify_search(
        q: str = Query(min_length=1, max_length=200),
        limit: int = Query(default=10, ge=1, le=20),
        channel_id: Optional[str] = Query(default=None),
        _: None = Depends(require_spotify_provider_dep),
    ) -> dict:
        query = (q or "").strip()
        if not query:
            raise HTTPException(status_code=400, detail="Query cannot be empty")
        resolved = resolve_channel_id(channel_id)
        token = ensure_spotify_token(resolved)
        params = {
            "q": query,
            "type": ",".join(spotify_search_types),
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
            "albums": map_spotify_search_bucket(data.get("albums"), map_spotify_album),
            "tracks": map_spotify_search_bucket(data.get("tracks"), map_spotify_track_simple),
            "artists": map_spotify_search_bucket(data.get("artists"), map_spotify_artist),
            "playlists": map_spotify_search_bucket(data.get("playlists"), map_spotify_playlist),
        }

    @router.get("/api/spotify/playlists")
    async def spotify_playlists(
        limit: int = Query(default=24, ge=1, le=50),
        offset: int = Query(default=0, ge=0),
        channel_id: Optional[str] = Query(default=None),
        _: None = Depends(require_spotify_provider_dep),
    ) -> dict:
        resolved = resolve_channel_id(channel_id)
        token = ensure_spotify_token(resolved)
        params = {"limit": min(limit, 50), "offset": max(0, offset)}
        path = _with_query("/me/playlists", params)
        resp = await spotify_request("GET", path, token, resolved)
        if resp.status_code >= 400:
            raise HTTPException(status_code=resp.status_code, detail=resp.text)
        data = resp.json()
        items = []
        for item in data.get("items", []):
            if isinstance(item, dict):
                items.append(map_spotify_playlist(item))
        return {
            "items": items,
            "total": data.get("total", len(items)),
            "limit": data.get("limit", params["limit"]),
            "offset": data.get("offset", params["offset"]),
            "next": bool(data.get("next")),
            "previous": bool(data.get("previous")),
        }

    @router.get("/api/spotify/playlists/{playlist_id}")
    async def spotify_playlist_detail(
        playlist_id: str,
        channel_id: Optional[str] = Query(default=None),
        _: None = Depends(require_spotify_provider_dep),
    ) -> dict:
        resolved = resolve_channel_id(channel_id)
        token = ensure_spotify_token(resolved)
        resp = await spotify_request("GET", f"/playlists/{playlist_id}", token, resolved)
        if resp.status_code >= 400:
            raise HTTPException(status_code=resp.status_code, detail=resp.text)
        data = resp.json()
        playlist = map_spotify_playlist(data) if isinstance(data, dict) else None
        if not playlist:
            raise HTTPException(status_code=404, detail="Playlist not found")
        return {"playlist": playlist}

    @router.get("/api/spotify/playlists/{playlist_id}/tracks")
    async def spotify_playlist_tracks(
        playlist_id: str,
        limit: int = Query(default=100, ge=1, le=100),
        offset: int = Query(default=0, ge=0),
        channel_id: Optional[str] = Query(default=None),
        _: None = Depends(require_spotify_provider_dep),
    ) -> dict:
        resolved = resolve_channel_id(channel_id)
        token = ensure_spotify_token(resolved)
        params = {"limit": min(limit, 100), "offset": max(0, offset)}
        path = _with_query(f"/playlists/{playlist_id}/tracks", params)
        resp = await spotify_request("GET", path, token, resolved)
        if resp.status_code >= 400:
            raise HTTPException(status_code=resp.status_code, detail=resp.text)
        data = resp.json()
        tracks: list[dict] = []
        base_position = data.get("offset", params["offset"])
        for idx, item in enumerate(data.get("items", [])):
            mapped = map_spotify_track(item, position=base_position + idx)
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

    @router.post("/api/spotify/playlists/{playlist_id}/tracks")
    async def spotify_playlist_add_tracks(
        playlist_id: str,
        payload: PlaylistAddTracksPayload = Body(...),
        channel_id: Optional[str] = Query(default=None),
        _: None = Depends(require_spotify_provider_dep),
    ) -> dict:
        resolved = resolve_channel_id(channel_id)
        token = ensure_spotify_token(resolved)
        uris: list[str] = []
        if isinstance(payload.track_uri, str) and payload.track_uri.strip():
            uris.append(payload.track_uri.strip())
        if isinstance(payload.uris, list):
            for uri in payload.uris:
                if isinstance(uri, str) and uri.strip():
                    uris.append(uri.strip())
        uris = list(dict.fromkeys(uris))
        if not uris:
            raise HTTPException(status_code=400, detail="Provide at least one track URI")
        resp = await spotify_request(
            "POST",
            f"/playlists/{playlist_id}/tracks",
            token,
            resolved,
            json={"uris": uris},
        )
        if resp.status_code >= 400:
            raise HTTPException(status_code=resp.status_code, detail=resp.text)
        data = resp.json() if resp.text else {}
        return {"ok": True, "snapshot_id": data.get("snapshot_id")}

    @router.get("/api/spotify/playlists/{playlist_id}/summary")
    async def spotify_playlist_summary(
        playlist_id: str,
        channel_id: Optional[str] = Query(default=None),
        _: None = Depends(require_spotify_provider_dep),
    ) -> dict:
        resolved = resolve_channel_id(channel_id)
        token = ensure_spotify_token(resolved)
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

    @router.post("/api/spotify/player/play")
    async def spotify_play(
        payload: Optional[dict] = Body(default=None),
        device_id: Optional[str] = Query(default=None),
        channel_id: Optional[str] = Query(default=None),
        _: None = Depends(require_spotify_provider_dep),
    ) -> dict:
        resolved = resolve_channel_id(channel_id)
        params = {"device_id": device_id} if device_id else None
        body = payload or None
        return await _spotify_player_control_with_recovery(
            "/me/player/play",
            "PUT",
            body=body,
            params=params,
            channel_id=resolved,
        )

    @router.post("/api/spotify/player/pause")
    async def spotify_pause(
        device_id: Optional[str] = Query(default=None),
        channel_id: Optional[str] = Query(default=None),
        _: None = Depends(require_spotify_provider_dep),
    ) -> dict:
        resolved = resolve_channel_id(channel_id)
        params = {"device_id": device_id} if device_id else None
        return await _spotify_player_control_with_recovery(
            "/me/player/pause",
            "PUT",
            params=params,
            channel_id=resolved,
        )

    @router.post("/api/spotify/player/next")
    async def spotify_next(
        device_id: Optional[str] = Query(default=None),
        channel_id: Optional[str] = Query(default=None),
        _: None = Depends(require_spotify_provider_dep),
    ) -> dict:
        resolved = resolve_channel_id(channel_id)
        params = {"device_id": device_id} if device_id else None
        return await _spotify_player_control_with_recovery(
            "/me/player/next",
            "POST",
            params=params,
            channel_id=resolved,
        )

    @router.post("/api/spotify/player/previous")
    async def spotify_prev(
        device_id: Optional[str] = Query(default=None),
        channel_id: Optional[str] = Query(default=None),
        _: None = Depends(require_spotify_provider_dep),
    ) -> dict:
        resolved = resolve_channel_id(channel_id)
        params = {"device_id": device_id} if device_id else None
        return await _spotify_player_control_with_recovery(
            "/me/player/previous",
            "POST",
            params=params,
            channel_id=resolved,
        )

    @router.post("/api/spotify/player/seek")
    async def spotify_seek(
        payload: dict = Body(...),
        device_id: Optional[str] = Query(default=None),
        channel_id: Optional[str] = Query(default=None),
        _: None = Depends(require_spotify_provider_dep),
    ) -> dict:
        pos = payload.get("position_ms")
        if pos is None:
            raise HTTPException(status_code=400, detail="position_ms required")
        params = {"position_ms": int(pos), "device_id": device_id}
        resolved = resolve_channel_id(channel_id)
        return await _spotify_player_control_with_recovery(
            "/me/player/seek",
            "PUT",
            params=params,
            channel_id=resolved,
        )

    @router.post("/api/spotify/player/volume")
    async def spotify_volume(
        payload: VolumePayload,
        device_id: Optional[str] = Query(default=None),
        channel_id: Optional[str] = Query(default=None),
        _: None = Depends(require_spotify_provider_dep),
    ) -> dict:
        params = {"volume_percent": payload.percent, "device_id": device_id}
        resolved = resolve_channel_id(channel_id)
        return await _spotify_player_control_with_recovery(
            "/me/player/volume",
            "PUT",
            params=params,
            channel_id=resolved,
        )

    @router.post("/api/spotify/player/shuffle")
    async def spotify_shuffle(
        payload: ShufflePayload,
        channel_id: Optional[str] = Query(default=None),
        _: None = Depends(require_spotify_provider_dep),
    ) -> dict:
        state = "true" if payload.state else "false"
        params = {"state": state, "device_id": payload.device_id}
        resolved = resolve_channel_id(channel_id)
        return await _spotify_player_control_with_recovery(
            "/me/player/shuffle",
            "PUT",
            params=params,
            channel_id=resolved,
        )

    @router.post("/api/spotify/player/repeat")
    async def spotify_repeat(
        payload: RepeatPayload,
        channel_id: Optional[str] = Query(default=None),
        _: None = Depends(require_spotify_provider_dep),
    ) -> dict:
        mode = payload.mode.lower()
        params = {"state": mode, "device_id": payload.device_id}
        resolved = resolve_channel_id(channel_id)
        return await _spotify_player_control_with_recovery(
            "/me/player/repeat",
            "PUT",
            params=params,
            channel_id=resolved,
        )

    @router.api_route("/stream/spotify", methods=["GET"])
    async def proxy_spotify_stream(
        request: Request,
        _: None = Depends(require_spotify_provider_dep),
    ):
        stream_paths = [
            f"http://{snapserver_host}:{snapserver_port}/stream/Spotify",
            f"http://{snapserver_host}:{snapserver_port}/stream/default",
        ]
        last_exc = None
        for upstream in stream_paths:
            try:
                async with httpx.AsyncClient(timeout=None, follow_redirects=True) as client:
                    upstream_req = client.build_request(request.method, upstream, headers={"Accept": "*/*"})
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

    return router
