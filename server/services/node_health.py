from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Optional

import httpx


log = logging.getLogger("roomcast")


@dataclass
class NodeHealthService:
    node_restart_timeout: int
    node_restart_interval: float
    node_health_interval: float

    get_nodes: Callable[[], dict]
    refresh_agent_metadata: Callable[..., Awaitable[tuple[bool, bool]]]
    save_nodes: Callable[[], None]
    broadcast_nodes: Callable[[], Awaitable[None]]

    sonos_service: object
    sonos_http_user_agent: str
    sonos_control_timeout: float

    _pending_restarts: dict[str, dict] = field(default_factory=dict)
    _agent_refresh_tasks: dict[str, asyncio.Task] = field(default_factory=dict)

    def is_restarting(self, node_id: Optional[str]) -> bool:
        if not node_id:
            return False
        return bool(self._pending_restarts.get(node_id))

    def schedule_agent_refresh(self, node_id: str, delay: float = 10.0, *, repeat: bool = False, attempts: int = 6) -> None:
        existing = self._agent_refresh_tasks.pop(node_id, None)
        if existing:
            existing.cancel()

        async def _task() -> None:
            remaining = max(1, int(attempts))
            try:
                while remaining > 0:
                    await asyncio.sleep(delay)
                    node = self.get_nodes().get(node_id)
                    if not node:
                        return
                    reachable, changed = await self.refresh_agent_metadata(node)
                    if reachable and changed:
                        await self.broadcast_nodes()
                    if not repeat:
                        return
                    if reachable and not node.get("updating"):
                        return
                    remaining -= 1
                node = self.get_nodes().get(node_id)
                if repeat and node and node.get("updating"):
                    node["updating"] = False
                    self.save_nodes()
                    await self.broadcast_nodes()
            finally:
                self._agent_refresh_tasks.pop(node_id, None)

        self._agent_refresh_tasks[node_id] = asyncio.create_task(_task())

    def schedule_restart_watch(self, node_id: str) -> None:
        existing = self._pending_restarts.pop(node_id, None)
        if existing:
            task = existing.get("task")
            if task:
                task.cancel()
        deadline = time.time() + float(self.node_restart_timeout)

        async def _monitor() -> None:
            saw_offline = False
            try:
                while time.time() < deadline:
                    await asyncio.sleep(float(self.node_restart_interval))
                    node = self.get_nodes().get(node_id)
                    if not node:
                        return
                    reachable, _ = await self.refresh_agent_metadata(node)
                    if reachable:
                        if saw_offline:
                            log.info("Node %s reported healthy after restart", node_id)
                            return
                    else:
                        saw_offline = True
                log.warning("Node %s did not return within restart timeout", node_id)
            finally:
                self._pending_restarts.pop(node_id, None)
                await self.broadcast_nodes()

        task = asyncio.create_task(_monitor())
        self._pending_restarts[node_id] = {"deadline": deadline, "task": task}

    async def refresh_all_nodes(self) -> None:
        nodes = self.get_nodes()
        dirty = False
        for node in list(nodes.values()):
            if node.get("type") == "agent":
                _, changed = await self.refresh_agent_metadata(node, persist=False)
                if changed:
                    dirty = True
                continue
            if node.get("type") != "sonos":
                continue
            ip = self.sonos_service.ip_from_url(node.get("url"))
            if not ip:
                continue
            now = time.time()
            reachable = await self.sonos_service.ping(ip)
            prev_online = node.get("online") is not False
            node["online"] = bool(reachable)
            if reachable:
                node["last_seen"] = now
                node["offline_since"] = None

                last_diag = node.get("sonos_network_last_refresh")
                should_refresh = not isinstance(last_diag, (int, float)) or (now - float(last_diag)) >= 60.0
                if should_refresh:
                    headers = {"User-Agent": self.sonos_http_user_agent}
                    try:
                        async with httpx.AsyncClient(timeout=self.sonos_control_timeout) as client:
                            resp = await client.get(f"http://{ip}:1400/support/review", headers=headers)
                        if resp.status_code == 200:
                            parsed = self.sonos_service.parse_network_from_review(resp.text)
                            if parsed and parsed != node.get("sonos_network"):
                                node["sonos_network"] = parsed
                            node["sonos_network_last_refresh"] = now
                    except Exception:
                        node["sonos_network_last_refresh"] = now
            else:
                if prev_online:
                    node["offline_since"] = now
            dirty = True

        if dirty:
            self.save_nodes()
            await self.broadcast_nodes()

    async def health_loop(self) -> None:
        try:
            while True:
                await self.refresh_all_nodes()
                await asyncio.sleep(max(5, float(self.node_health_interval)))
        except asyncio.CancelledError:
            pass
