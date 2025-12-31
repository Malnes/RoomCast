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
    channel_id_prefix: str,
) -> APIRouter:
    router = APIRouter()

    @router.get("/api/channels")
    async def list_channels_api() -> dict:
        return {"channels": all_channel_details()}

    @router.post("/api/channels/count")
    async def set_channels_count_api(payload: ChannelCountPayload, _: dict = Depends(require_admin)) -> dict:
        desired = int(payload.count)
        desired = max(1, min(10, desired))

        channels_by_id = get_channels_by_id()
        keep_ids = [f"{channel_id_prefix}{idx}" for idx in range(1, desired + 1)]
        keep_set = set(keep_ids)
        removed_ids = [cid for cid in list(channels_by_id.keys()) if cid not in keep_set]

        # Ensure target channels exist, preserve existing entries when possible.
        for idx, cid in enumerate(keep_ids, start=1):
            existing = channels_by_id.get(cid)
            if existing:
                existing["order"] = idx
                continue
            defaults = {
                "id": cid,
                "name": f"Channel {idx}",
                "order": idx,
                "snap_stream": f"Spotify_CH{idx}",
                "fifo_path": f"/tmp/snapfifo-{cid}",
                "enabled": True,
                "source": "none",
                "source_ref": None,
            }
            channels_by_id[cid] = normalize_channel_entry(defaults, idx)

        # Remove channels outside the desired range.
        for cid in removed_ids:
            channels_by_id.pop(cid, None)

        # Re-sequence and persist channels.
        resequence_channel_order()
        save_channels()

        # Reassign nodes that were pointing at removed channels.
        primary = keep_ids[0] if keep_ids else primary_channel_id()
        moved = 0
        nodes = get_nodes()
        for node in list(nodes.values()):
            raw_channel = (node.get("channel_id") or "").strip().lower()
            if raw_channel and raw_channel not in channels_by_id:
                try:
                    await set_node_channel(node, primary)
                    moved += 1
                except Exception as exc:
                    log.warning(
                        "Failed to move node %s to %s after channel resize: %s",
                        node.get("id"),
                        primary,
                        exc,
                    )
        if moved:
            save_nodes()
            await broadcast_nodes()

        return {"ok": True, "count": len(get_channel_order()), "channels": all_channel_details()}

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
