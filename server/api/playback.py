from __future__ import annotations

import time
from collections.abc import Awaitable, Callable

from fastapi import APIRouter, HTTPException


def create_playback_router(
    *,
    channel_order: list[str],
    channels_by_id: dict[str, dict],
    require_radio_provider: Callable[[], None],
    ensure_radio_state: Callable[[dict], dict],
    radio_runtime_status: dict[str, dict],
    save_channels: Callable[[], None],
    mark_radio_assignments_dirty: Callable[[], None],
    require_spotify_provider: Callable[[], None],
    spotify_control: Callable[..., Awaitable[None]],
    parse_spotify_error: Callable[[object], dict],
) -> APIRouter:
    router = APIRouter()

    @router.post("/api/playback/stop-all")
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
                state = ensure_radio_state(channel)
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
                await spotify_control("/me/player/pause", "PUT", channel_id=cid)
                spotify_stopped.append(cid)
            except HTTPException as exc:
                raw_detail = exc.detail
                parsed = parse_spotify_error(raw_detail)
                reason = (parsed.get("reason") or "").strip().lower()
                message = (parsed.get("message") or "").strip()
                detail_text = message or (raw_detail if isinstance(raw_detail, str) else str(raw_detail))
                if reason == "no_active_device" or "no active device" in detail_text.lower():
                    continue
                errors[cid] = detail_text or "Failed to pause Spotify playback"
            except Exception as exc:  # pragma: no cover - defensive
                errors[cid] = str(exc)
        if radio_mutated:
            save_channels()
            mark_radio_assignments_dirty()
        return {
            "ok": True,
            "spotify_stopped": spotify_stopped,
            "radio_stopped": radio_stopped,
            "radio_states": radio_states,
            "errors": errors,
        }

    return router
