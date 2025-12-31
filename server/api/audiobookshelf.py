import asyncio
import logging
import time
from typing import Any, Awaitable, Callable, Optional

import httpx
from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel, Field

log = logging.getLogger("roomcast")


class AudiobookshelfPlayPayload(BaseModel):
    library_item_id: str = Field(..., min_length=1)
    episode_id: str = Field(..., min_length=1)
    podcast_title: Optional[str] = None
    episode_title: Optional[str] = None


class AudiobookshelfPlaybackPayload(BaseModel):
    action: str = Field(pattern="^(start|stop)$")


class AudiobookshelfWorkerStatusPayload(BaseModel):
    state: str = Field(pattern="^(playing|connecting|error|idle)$")
    message: Optional[str] = None


class AudiobookshelfEndedPayload(BaseModel):
    library_item_id: Optional[str] = None
    episode_id: Optional[str] = None


def create_audiobookshelf_router(
    *,
    get_provider_settings: Callable[[str], Any],
    require_audiobookshelf_provider: Callable[[], None],
    resolve_channel_id: Callable[[Optional[str]], str],
    get_channel: Callable[[str], dict],
    channel_detail: Callable[[str], dict],
    save_channels: Callable[[], None],
    normalize_abs_state: Callable[[Optional[dict]], dict],
    mark_abs_assignments_dirty: Callable[[], None],
    require_abs_worker_token: Callable[[Request], None],
    wait_for_abs_assignments_change: Callable[[Optional[int], float], Awaitable[int]],
    get_abs_assignments_version: Callable[[], int],
    get_abs_runtime_status: Callable[[], dict],
    get_channel_order: Callable[[], list[str]],
    get_channels_by_id: Callable[[], dict],
    abs_http_timeout: float,
    abs_assignment_default_wait: float,
    abs_assignment_max_wait: float,
) -> APIRouter:
    router = APIRouter()

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

    async def _abs_request(
        method: str,
        url: str,
        *,
        token: str,
        params: Optional[dict] = None,
        json_body: Any = None,
    ) -> Any:
        timeout = httpx.Timeout(abs_http_timeout)
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
            title = ((item.get("title") or item.get("name") or _abs_pick(media, "title") or "").strip())
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

    async def _abs_fetch_episode_progress(
        base_url: str,
        token: str,
        library_item_id: str,
        episode_id: str,
    ) -> Optional[dict]:
        timeout = httpx.Timeout(abs_http_timeout)
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
        episodes.sort(
            key=lambda e: (e.get("published_at") is None, e.get("published_at") or 0, e.get("title") or "")
        )

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

    def _runtime_payload(channel_id: str) -> dict:
        status = (get_abs_runtime_status() or {}).get(channel_id)
        if not status:
            return {"state": "idle", "message": None, "updated_at": None, "started_at": None}
        return status

    @router.get("/api/audiobookshelf/libraries")
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

    @router.get("/api/audiobookshelf/podcasts")
    async def list_abs_podcasts(library_id: Optional[str] = Query(default=None)) -> dict:
        base_url, token = _require_abs_configured()
        resolved_library = await _abs_resolve_podcast_library_id(base_url, token, library_id or _abs_library_id_setting())
        items = await _abs_list_podcast_items(base_url, token, resolved_library)
        return {"library_id": resolved_library, "podcasts": items}

    @router.get("/api/audiobookshelf/podcasts/{library_item_id}/episodes")
    async def list_abs_episodes(
        library_item_id: str,
        show_played: bool = Query(default=False),
    ) -> dict:
        base_url, token = _require_abs_configured()
        episodes = await _abs_list_episodes(base_url, token, library_item_id, show_played=show_played)
        return {"library_item_id": library_item_id, "episodes": episodes, "show_played": bool(show_played)}

    @router.post("/api/audiobookshelf/{channel_id}/play")
    async def audiobookshelf_play_episode(channel_id: str, payload: AudiobookshelfPlayPayload) -> dict:
        require_audiobookshelf_provider()
        resolved = resolve_channel_id(channel_id)
        channel = _get_abs_channel_or_404(resolved)
        state = normalize_abs_state(channel.get("abs_state"))
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
        mark_abs_assignments_dirty()
        get_abs_runtime_status().pop(resolved, None)
        return {"ok": True, "channel": channel_detail(resolved)}

    @router.post("/api/audiobookshelf/{channel_id}/playback")
    async def audiobookshelf_playback_toggle(channel_id: str, payload: AudiobookshelfPlaybackPayload) -> dict:
        require_audiobookshelf_provider()
        resolved = resolve_channel_id(channel_id)
        channel = _get_abs_channel_or_404(resolved)
        state = normalize_abs_state(channel.get("abs_state"))
        desired = payload.action == "start"
        if desired and not (state.get("library_item_id") and state.get("episode_id")):
            raise HTTPException(status_code=400, detail="Select an episode before starting playback")
        state["playback_enabled"] = desired
        state["updated_at"] = int(time.time())
        channel["abs_state"] = state
        save_channels()
        mark_abs_assignments_dirty()
        runtime_status = get_abs_runtime_status()
        if not desired:
            runtime_status[resolved] = {
                "state": "idle",
                "message": "Playback stopped",
                "updated_at": state["updated_at"],
                "started_at": None,
            }
        else:
            runtime_status.pop(resolved, None)
        return {"ok": True}

    @router.get("/api/audiobookshelf/status/{channel_id}")
    async def audiobookshelf_status(channel_id: str) -> dict:
        require_audiobookshelf_provider()
        resolved = resolve_channel_id(channel_id)
        channel = _get_abs_channel_or_404(resolved)
        state = normalize_abs_state(channel.get("abs_state"))
        channel["abs_state"] = state
        runtime = _runtime_payload(resolved)
        return {
            "channel_id": resolved,
            "abs_state": state,
            "runtime": runtime,
            "enabled": channel.get("enabled", True),
        }

    @router.get("/api/audiobookshelf/worker/assignments")
    async def audiobookshelf_worker_assignments(
        request: Request,
        since: Optional[int] = Query(None),
        wait: Optional[float] = Query(None),
    ) -> dict:
        require_audiobookshelf_provider()
        require_abs_worker_token(request)

        timeout = abs_assignment_default_wait if wait is None else float(wait)
        timeout = max(1.0, min(timeout, abs_assignment_max_wait))

        version = get_abs_assignments_version()
        if since is not None:
            version = await wait_for_abs_assignments_change(since, timeout)

        base_url = _abs_base_url()
        token = _abs_token()

        channels_by_id = get_channels_by_id()
        channel_order = get_channel_order()

        assignments = []
        for cid in channel_order:
            channel = channels_by_id.get(cid)
            if not channel or (channel.get("source") or "").strip().lower() != "audiobookshelf":
                continue
            state = normalize_abs_state(channel.get("abs_state"))
            channel["abs_state"] = state
            enabled = channel.get("enabled", True)
            playback_enabled = state.get("playback_enabled", True)
            library_item_id = (
                (state.get("library_item_id") or "").strip()
                if isinstance(state.get("library_item_id"), str)
                else state.get("library_item_id")
            )
            episode_id = (
                (state.get("episode_id") or "").strip()
                if isinstance(state.get("episode_id"), str)
                else state.get("episode_id")
            )
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

    @router.post("/api/audiobookshelf/worker/status/{channel_id}")
    async def audiobookshelf_worker_status(
        channel_id: str,
        payload: AudiobookshelfWorkerStatusPayload,
        request: Request,
    ) -> dict:
        require_audiobookshelf_provider()
        require_abs_worker_token(request)
        resolved = resolve_channel_id(channel_id)
        _get_abs_channel_or_404(resolved)

        previous = (get_abs_runtime_status().get(resolved) or {})
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
        get_abs_runtime_status()[resolved] = status_payload
        return {"ok": True}

    @router.post("/api/audiobookshelf/worker/ended/{channel_id}")
    async def audiobookshelf_worker_ended(
        channel_id: str,
        payload: AudiobookshelfEndedPayload,
        request: Request,
    ) -> dict:
        require_audiobookshelf_provider()
        require_abs_worker_token(request)
        resolved = resolve_channel_id(channel_id)
        channel = _get_abs_channel_or_404(resolved)
        state = normalize_abs_state(channel.get("abs_state"))
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
            for candidate in episodes[current_idx + 1 :]:
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
                mark_abs_assignments_dirty()
                advanced = True
            else:
                # No more episodes; stop playback.
                state["playback_enabled"] = False
                state["updated_at"] = int(time.time())
                channel["abs_state"] = state
                save_channels()
                mark_abs_assignments_dirty()

        return {"ok": True, "advanced": advanced}

    return router
