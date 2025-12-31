from __future__ import annotations

from dataclasses import dataclass, field

from fastapi import WebSocket


@dataclass
class NodeBroadcastService:
    _watchers: set[WebSocket] = field(default_factory=set)

    def watchers(self) -> set[WebSocket]:
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
            self._watchers.discard(ws)
