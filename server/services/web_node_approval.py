from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, Optional

from fastapi import HTTPException


BroadcastPayload = Callable[[dict], Awaitable[None]]
GetWebRTCRelay = Callable[[], Any]


@dataclass
class WebNodeApprovalService:
    webrtc_enabled: bool
    get_webrtc_relay: GetWebRTCRelay
    create_browser_node: Callable[[str], dict]
    resolve_node_channel_id: Callable[[dict], Optional[str]]
    get_channel_by_id: Callable[[str], Optional[dict]]
    normalize_stereo_mode: Callable[[Any], str]
    teardown_browser_node: Callable[[str], Awaitable[None]]
    broadcast_nodes: Callable[[], Awaitable[None]]
    broadcast_payload: BroadcastPayload

    def __post_init__(self) -> None:
        self._pending_requests: Dict[str, dict] = {}

    def pending_requests(self) -> Dict[str, dict]:
        return self._pending_requests

    def get_snapshot(self, entry: dict) -> dict:
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

    def pending_snapshots(self) -> list[dict]:
        entries = sorted(self._pending_requests.values(), key=lambda item: item.get("created_at", 0))
        return [self.get_snapshot(entry) for entry in entries]

    async def broadcast_event(
        self,
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
        await self.broadcast_payload(payload)

    def pop_pending_request(self, request_id: str) -> dict:
        pending = self._pending_requests.pop(request_id, None)
        if not pending:
            raise HTTPException(status_code=404, detail="Web node request not found or already resolved")
        return pending

    async def establish_session_for_request(self, pending: dict) -> dict:
        relay = self.get_webrtc_relay()
        if not self.webrtc_enabled or not relay:
            raise HTTPException(status_code=503, detail="Web nodes are disabled")

        name = pending.get("name") or "Web node"
        node = self.create_browser_node(name)
        channel_id = self.resolve_node_channel_id(node)
        channel = self.get_channel_by_id(channel_id) if channel_id else None
        stream_id = channel.get("snap_stream") if channel else None
        if channel_id and (not channel or not stream_id):
            await self.teardown_browser_node(node["id"])
            raise HTTPException(status_code=500, detail="Channel is missing snap_stream mapping")

        try:
            session = await relay.create_session(
                node["id"],
                channel_id,
                stream_id,
                stereo_mode=self.normalize_stereo_mode(node.get("stereo_mode")),
            )
            answer = await session.accept(pending["offer_sdp"], pending["offer_type"])
        except Exception as exc:  # pragma: no cover - defensive logging
            await self.teardown_browser_node(node["id"])
            raise HTTPException(status_code=500, detail=f"Failed to start WebRTC session: {exc}")

        await self.broadcast_nodes()
        return {"node": node, "answer": answer.sdp, "answer_type": answer.type}
