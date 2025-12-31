import json
import logging
import secrets
import time
from typing import Any, Callable, Dict, List, Literal, Optional, Tuple

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field


log = logging.getLogger("roomcast")


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


class RadioSlotsUpdatePayload(BaseModel):
    max_slots: int = Field(ge=1)


def _make_radio_cache_key(path: str, params: Optional[dict]) -> str:
    if not params:
        return path
    try:
        serialized = json.dumps(sorted((params or {}).items()), separators=(",", ":"))
    except TypeError:
        serialized = json.dumps(str(params), ensure_ascii=False)
    return f"{path}?{serialized}"


def _radio_cache_get(cache: Dict[str, Tuple[float, Any]], key: str) -> Optional[Any]:
    if not key:
        return None
    cached = cache.get(key)
    if not cached:
        return None
    expires_at, payload = cached
    if expires_at and expires_at < time.time():
        cache.pop(key, None)
        return None
    return payload


def _radio_cache_put(cache: Dict[str, Tuple[float, Any]], key: str, payload: Any, ttl: int) -> None:
    if not key or ttl <= 0:
        return
    cache[key] = (time.time() + ttl, payload)


async def _radio_browser_request(
    *,
    cache: Dict[str, Tuple[float, Any]],
    base_urls: List[str],
    timeout: float,
    default_ttl: int,
    user_agent: str,
    path: str,
    params: Optional[dict] = None,
    ttl: Optional[int] = None,
    cache_key: Optional[str] = None,
) -> Any:
    resolved_path = path.lstrip("/")
    ttl_value = default_ttl if ttl is None else ttl
    key = cache_key or _make_radio_cache_key(resolved_path, params)
    if ttl_value and key:
        cached = _radio_cache_get(cache, key)
        if cached is not None:
            return cached
    headers = {"User-Agent": user_agent}
    base_urls = [entry.strip().rstrip("/") for entry in (base_urls or []) if entry and entry.strip()]
    last_exc: Optional[BaseException] = None
    all_timeouts = True
    for base_url in base_urls:
        url = f"{base_url}/{resolved_path}".rstrip("/")
        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.get(url, params=params, headers=headers)
                response.raise_for_status()
                payload = response.json()
                break
        except httpx.TimeoutException as exc:
            last_exc = exc
            log.warning("Radio Browser request timeout for %s via %s", resolved_path, base_url)
            continue
        except httpx.HTTPStatusError as exc:
            all_timeouts = False
            status_code = exc.response.status_code if exc.response else None
            if status_code and (status_code >= 500 or status_code == 429):
                last_exc = exc
                log.warning(
                    "Radio Browser server error for %s via %s (status=%s)",
                    resolved_path,
                    base_url,
                    status_code,
                )
                continue
            last_exc = exc
            break
        except httpx.RequestError as exc:
            all_timeouts = False
            last_exc = exc
            log.warning("Radio Browser request failed for %s via %s: %s", resolved_path, base_url, exc)
            continue
    else:
        payload = None

    if payload is None:
        if all_timeouts:
            raise HTTPException(status_code=504, detail="Radio directory timeout")
        raise HTTPException(status_code=502, detail="Radio directory unavailable") from last_exc
    if ttl_value and key:
        _radio_cache_put(cache, key, payload, ttl_value)
    return payload


def _radio_browser_base_url_candidates(configured: str) -> List[str]:
    configured_parts = [
        part.strip().rstrip("/")
        for part in (configured or "").split(",")
        if part and part.strip()
    ]
    defaults = [
        "https://all.api.radio-browser.info/json",
        "https://de1.api.radio-browser.info/json",
        "https://nl1.api.radio-browser.info/json",
        "https://at1.api.radio-browser.info/json",
        "https://fr1.api.radio-browser.info/json",
    ]
    seen: set[str] = set()
    candidates: List[str] = []
    for entry in [*configured_parts, *defaults]:
        normalized = entry.strip().rstrip("/")
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        candidates.append(normalized)
    return candidates


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


def _require_radio_worker_token(request: Request, token: str) -> None:
    if not token:
        return
    header = request.headers.get("x-radio-worker-token") or request.headers.get("authorization", "")
    if header.lower().startswith("bearer "):
        header = header[7:]
    candidate = header.strip()
    if not candidate or not secrets.compare_digest(candidate, token):
        raise HTTPException(status_code=401, detail="Invalid radio worker token")


def create_radio_router(
    *,
    require_admin: Callable,
    require_radio_provider_dep: Callable,
    require_radio_provider: Callable[[], None],
    resolve_channel_id: Callable[[str], str],
    get_radio_channel_or_404: Callable[[str], dict],
    apply_radio_station: Callable[[dict, RadioStationSelectionPayload], dict],
    save_channels: Callable[[], None],
    mark_radio_assignments_dirty: Callable[[], None],
    channel_detail: Callable[[str], dict],
    ensure_radio_state: Callable[[dict], dict],
    radio_runtime_payload: Callable[[str], dict],
    radio_max_slots_configured: Callable[[], int],
    radio_max_slots_supported: Callable[[], int],
    radio_channel_slots_active: Callable[[], List[dict]],
    get_providers_by_id: Callable[[], Dict[str, Any]],
    save_providers_state: Callable[[], None],
    get_channels_by_id: Callable[[], Dict[str, dict]],
    get_channel_order: Callable[[], List[str]],
    get_radio_runtime_status: Callable[[], Dict[str, dict]],
    get_radio_assignments_version: Callable[[], int],
    wait_for_radio_assignments_change: Callable[[Optional[int], float], Any],
    radio_assignment_default_wait: float,
    radio_assignment_max_wait: float,
    radio_worker_token: str,
    radio_browser_base_url: str,
    radio_browser_timeout: float,
    radio_browser_cache_ttl: int,
    radio_browser_user_agent: str,
) -> APIRouter:
    router = APIRouter()
    radio_browser_cache: Dict[str, Tuple[float, Any]] = {}
    radio_browser_base_urls = _radio_browser_base_url_candidates(radio_browser_base_url)

    @router.get("/api/radio/genres")
    async def list_radio_genres(limit: int = Query(default=50, ge=1, le=400), _: None = Depends(require_radio_provider_dep)) -> dict:
        data = await _radio_browser_request(
            cache=radio_browser_cache,
            base_urls=radio_browser_base_urls,
            timeout=radio_browser_timeout,
            default_ttl=radio_browser_cache_ttl,
            user_agent=radio_browser_user_agent,
            path="/tags",
            ttl=3600,
            cache_key="radio:tags",
        )
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

    @router.get("/api/radio/countries")
    async def list_radio_countries(limit: int = Query(default=250, ge=1, le=400), _: None = Depends(require_radio_provider_dep)) -> dict:
        data = await _radio_browser_request(
            cache=radio_browser_cache,
            base_urls=radio_browser_base_urls,
            timeout=radio_browser_timeout,
            default_ttl=radio_browser_cache_ttl,
            user_agent=radio_browser_user_agent,
            path="/countries",
            ttl=3600,
            cache_key="radio:countries",
        )
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

    @router.get("/api/radio/top")
    async def list_radio_top(
        metric: str = Query(default="votes", pattern="^(votes|clicks)$"),
        limit: int = Query(default=40, ge=1, le=200),
        _: None = Depends(require_radio_provider_dep),
    ) -> dict:
        normalized = "vote" if metric == "votes" else "click"
        path = f"/stations/top{normalized}/{limit}"
        cache_key = f"radio:top:{metric}:{limit}"
        data = await _radio_browser_request(
            cache=radio_browser_cache,
            base_urls=radio_browser_base_urls,
            timeout=radio_browser_timeout,
            default_ttl=radio_browser_cache_ttl,
            user_agent=radio_browser_user_agent,
            path=path,
            ttl=120,
            cache_key=cache_key,
        )
        stations = [_serialize_radio_station(item) for item in data]
        return {"stations": stations}

    @router.get("/api/radio/search")
    async def search_radio_stations(
        query: Optional[str] = Query(default=None, max_length=200),
        country: Optional[str] = Query(default=None, max_length=120),
        countrycode: Optional[str] = Query(default=None, max_length=4),
        tag: Optional[str] = Query(default=None, max_length=120),
        limit: int = Query(default=50, ge=1, le=200),
        _: None = Depends(require_radio_provider_dep),
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
        data = await _radio_browser_request(
            cache=radio_browser_cache,
            base_urls=radio_browser_base_urls,
            timeout=radio_browser_timeout,
            default_ttl=radio_browser_cache_ttl,
            user_agent=radio_browser_user_agent,
            path="/stations/search",
            params=params,
            ttl=15,
        )
        stations = [_serialize_radio_station(item) for item in data]
        return {"stations": stations[:limit]}

    @router.post("/api/radio/{channel_id}/station")
    async def assign_radio_station(
        channel_id: str,
        payload: RadioStationSelectionPayload,
        _: dict = Depends(require_admin),
        __: None = Depends(require_radio_provider_dep),
    ) -> dict:
        resolved = resolve_channel_id(channel_id)
        channel = get_radio_channel_or_404(resolved)
        apply_radio_station(channel, payload)
        save_channels()
        mark_radio_assignments_dirty()
        get_radio_runtime_status().pop(resolved, None)
        return {"ok": True, "channel": channel_detail(resolved)}

    @router.get("/api/radio/slots")
    async def get_radio_slots(_: None = Depends(require_radio_provider_dep)) -> dict:
        return {
            "max_slots": radio_max_slots_configured(),
            "supported_max_slots": radio_max_slots_supported(),
        }

    @router.put("/api/radio/slots")
    async def set_radio_slots(
        payload: RadioSlotsUpdatePayload,
        _: dict = Depends(require_admin),
        __: None = Depends(require_radio_provider_dep),
    ) -> dict:
        supported = radio_max_slots_supported()
        desired = int(payload.max_slots)
        if desired < 1 or desired > supported:
            raise HTTPException(status_code=400, detail=f"max_slots must be between 1 and {supported}")

        providers_by_id = get_providers_by_id()
        state = providers_by_id.get("radio")
        if not state:
            raise HTTPException(status_code=503, detail="Radio provider is not installed")
        if not isinstance(state.settings, dict):
            state.settings = {}
        state.settings["max_slots"] = desired
        save_providers_state()

        active_streams = {(slot.get("snap_stream") or "").strip() for slot in radio_channel_slots_active()}
        channels_by_id = get_channels_by_id()
        for channel in channels_by_id.values():
            if (channel.get("source") or "").strip().lower() != "radio":
                continue
            stream = (channel.get("snap_stream") or "").strip()
            if not stream or stream in active_streams:
                continue
            channel["source"] = "none"
            channel["source_ref"] = None
            channel["enabled"] = False
            channel.pop("radio_state", None)
        save_channels()
        mark_radio_assignments_dirty()

        return {
            "ok": True,
            "max_slots": radio_max_slots_configured(),
            "supported_max_slots": radio_max_slots_supported(),
        }

    @router.get("/api/radio/status/{channel_id}")
    async def get_radio_status(channel_id: str, _: None = Depends(require_radio_provider_dep)) -> dict:
        resolved = resolve_channel_id(channel_id)
        channel = get_radio_channel_or_404(resolved)
        state = ensure_radio_state(channel)
        runtime = radio_runtime_payload(resolved)
        return {
            "channel_id": resolved,
            "radio_state": state,
            "runtime": runtime,
            "enabled": channel.get("enabled", True),
        }

    @router.post("/api/radio/{channel_id}/playback")
    async def control_radio_playback(channel_id: str, payload: RadioPlaybackPayload, _: None = Depends(require_radio_provider_dep)) -> dict:
        resolved = resolve_channel_id(channel_id)
        channel = get_radio_channel_or_404(resolved)
        state = ensure_radio_state(channel)
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
        mark_radio_assignments_dirty()
        runtime_status = get_radio_runtime_status()
        if not desired:
            runtime_status[resolved] = {
                "state": "idle",
                "message": "Radio stopped",
                "bitrate": None,
                "station_id": state.get("station_id"),
                "metadata": None,
                "updated_at": now,
                "started_at": None,
            }
        else:
            runtime_status.pop(resolved, None)
        return {"ok": True, "radio_state": state, "previous": previous, "current": desired}

    @router.get("/api/radio/worker/assignments")
    async def radio_worker_assignments(
        request: Request,
        since: Optional[int] = Query(None),
        wait: Optional[float] = Query(None),
    ) -> dict:
        require_radio_provider()
        _require_radio_worker_token(request, radio_worker_token)
        timeout = radio_assignment_default_wait if wait is None else float(wait)
        timeout = max(1.0, min(timeout, radio_assignment_max_wait))
        version = get_radio_assignments_version()
        if since is not None:
            version = await wait_for_radio_assignments_change(since, timeout)
        channels_by_id = get_channels_by_id()
        assignments = []
        for cid in get_channel_order():
            channel = channels_by_id.get(cid)
            if not channel or channel.get("source") != "radio":
                continue
            state = ensure_radio_state(channel)
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

    @router.post("/api/radio/worker/status/{channel_id}")
    async def radio_worker_status(channel_id: str, payload: RadioWorkerStatusPayload, request: Request) -> dict:
        require_radio_provider()
        _require_radio_worker_token(request, radio_worker_token)
        resolved = resolve_channel_id(channel_id)
        channel = get_radio_channel_or_404(resolved)
        runtime_status = get_radio_runtime_status()
        previous = runtime_status.get(resolved) or {}
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
        runtime_status[resolved] = status_payload
        return {"ok": True}

    return router
