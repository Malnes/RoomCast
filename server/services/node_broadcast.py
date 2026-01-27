from __future__ import annotations

from dataclasses import dataclass, field

from fastapi import WebSocket


@dataclass
class NodeBroadcastService:
    _watchers: dict[WebSocket, dict] = field(default_factory=dict)

    def watchers(self) -> dict[WebSocket, dict]:
        return self._watchers

    async def broadcast(self, payload: dict) -> None:
        if not self._watchers:
            return
        dead: list[WebSocket] = []
        for ws in list(self._watchers):
            try:
                await ws.send_json(payload)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self._watchers.pop(ws, None)
