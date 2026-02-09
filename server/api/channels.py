import logging
from typing import Awaitable, Callable, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

log = logging.getLogger("roomcast")


class ChannelCountPayload(BaseModel):
    count: int = Field(ge=1, le=10)


class ChannelUpdatePayload(BaseModel):
    name: Optional[str] = Field(default=None, min_length=1, max_length=120)
    color: Optional[str] = Field(default=None, min_length=1, max_length=7)
    order: Optional[int] = Field(default=None, ge=1, le=50)
    snap_stream: Optional[str] = Field(default=None, min_length=1, max_length=160)
    enabled: Optional[bool] = None
    source_ref: Optional[str] = Field(default=None, max_length=60)


def create_channels_router(
    *,
    get_channels_by_id: Callable[[], dict],
    get_channel_order: Callable[[], list],
    get_nodes: Callable[[], dict],
    get_sources_by_id: Callable[[], dict],
    load_token: Callable[[str], Optional[dict]],
    require_admin: Callable,
    resolve_channel_id: Callable[[Optional[str]], str],
    channel_detail: Callable[[str], dict],
    all_channel_details: Callable[[], list[dict]],
    update_channel_metadata: Callable[[str, dict], dict],
    apply_channel_routing: Callable[[str], Awaitable[None]],
    broadcast_nodes: Callable[[], Awaitable[None]],
    set_node_channel: Callable[[dict, str], Awaitable[None]],
    normalize_channel_entry: Callable[[dict, int], dict],
    resequence_channel_order: Callable[[], None],
    primary_channel_id: Callable[[], str],
    save_channels: Callable[[], None],
    save_nodes: Callable[[], None],
    refresh_channels: Callable[[], None],
    channel_id_prefix: str,
) -> APIRouter:
    router = APIRouter()

    @router.get("/api/channels")
    async def list_channels_api() -> dict:
        refresh_channels()
        return {"channels": all_channel_details()}

    @router.post("/api/channels/count")
    async def set_channels_count_api(payload: ChannelCountPayload, _: dict = Depends(require_admin)) -> dict:
        raise HTTPException(
            status_code=400,
            detail="Manual channel count is deprecated. Providers are now the channel units.",
        )

    @router.get("/api/sources")
    async def list_sources_api() -> dict:
        sources = []
        for sid, source in sorted(get_sources_by_id().items()):
            if not isinstance(source, dict):
                continue
            kind = source.get("kind")
            if kind != "spotify":
                continue
            token = load_token(sid)
            sources.append(
                {
                    "id": sid,
                    "kind": kind,
                    "name": source.get("name") or sid,
                    "snap_stream": source.get("snap_stream"),
                    "has_oauth_token": bool(token and token.get("access_token")),
                }
            )
        return {"sources": sources}

    @router.patch("/api/channels/{channel_id}")
    async def update_channel_api(channel_id: str, payload: ChannelUpdatePayload) -> dict:
        updates = payload.model_dump(exclude_unset=True)
        if updates:
            result = update_channel_metadata(channel_id, updates)
            if bool(result.get("_routing_changed")):
                await apply_channel_routing(channel_id)
                await broadcast_nodes()
        return {"ok": True, "channel": channel_detail(resolve_channel_id(channel_id))}

    return router
