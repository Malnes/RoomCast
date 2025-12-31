from __future__ import annotations

import asyncio
import ipaddress
import logging
import subprocess
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Awaitable, Callable, Optional

import httpx


log = logging.getLogger("roomcast")


@dataclass
class NodeDiscoveryService:
    agent_port: int
    discovery_cidr: str
    discovery_concurrency: int
    discovery_max_hosts: int
    node_rediscovery_interval: int

    get_nodes: Callable[[], dict]
    normalize_node_url: Callable[[str], str]
    save_nodes: Callable[[], None]
    refresh_agent_metadata: Callable[[dict], Awaitable[tuple[bool, bool]]]
    broadcast_nodes: Callable[[], Awaitable[None]]

    _rediscovery_tasks: dict[str, asyncio.Task] = field(default_factory=dict)

    def cancel_node_rediscovery(self, node_id: Optional[str]) -> None:
        if not node_id:
            return
        task = self._rediscovery_tasks.pop(node_id, None)
        if task:
            task.cancel()

    def schedule_node_rediscovery(self, node: dict) -> None:
        if not node or node.get("type") != "agent":
            return
        node_id = node.get("id")
        fingerprint = node.get("fingerprint")
        if not node_id or not fingerprint:
            return
        if node_id in self._rediscovery_tasks:
            return
        log.info("Scheduling rediscovery for node %s (fingerprint %s)", node_id, str(fingerprint)[:8])
        self._rediscovery_tasks[node_id] = asyncio.create_task(self._rediscover_node(node_id, fingerprint))

    async def probe_host(self, host: str) -> Optional[dict]:
        url = f"http://{host}:{self.agent_port}"
        try:
            async with httpx.AsyncClient(timeout=2) as client:
                resp = await client.get(f"{url}/health")
            if resp.status_code != 200:
                return None
            try:
                data = resp.json()
            except ValueError:
                data = {}
            return {
                "host": host,
                "url": f"{url}",
                "healthy": True,
                "version": data.get("version"),
                "fingerprint": data.get("fingerprint"),
            }
        except Exception:
            return None

    def _configured_discovery_networks(self) -> list[str]:
        configured: list[str] = []
        raw_value = (self.discovery_cidr or "").strip()
        if not raw_value:
            return configured
        for chunk in raw_value.replace(";", ",").split(","):
            part = chunk.strip()
            if not part:
                continue
            try:
                net = str(ipaddress.ip_network(part, strict=False))
            except ValueError:
                log.warning("Ignoring invalid DISCOVERY_CIDR entry: %s", part)
                continue
            if net not in configured:
                configured.append(net)
        return configured

    def detect_discovery_networks(self) -> list[str]:
        """Return IPv4 networks to scan, defaulting to DISCOVERY_CIDR."""
        networks: list[str] = self._configured_discovery_networks()
        try:
            output = subprocess.check_output(
                ["ip", "-o", "-4", "addr", "show", "up", "scope", "global"],
                text=True,
            )
        except Exception:
            output = ""
        for line in output.splitlines():
            parts = line.split()
            if len(parts) < 4:
                continue
            cidr = parts[3]
            try:
                iface = ipaddress.ip_interface(cidr)
            except ValueError:
                continue
            if iface.ip.is_loopback or iface.ip.is_link_local:
                continue
            net = iface.network
            if net.num_addresses <= 2:
                continue
            if net.version != 4:
                continue
            net_str = str(net)
            if net_str not in networks:
                networks.append(net_str)
        return networks

    def hosts_for_networks(self, networks: list[str], *, limit: Optional[int] = None) -> list[str]:
        max_hosts = int(limit) if isinstance(limit, int) and limit > 0 else self.discovery_max_hosts
        seen: set[str] = set()
        hosts: list[str] = []
        for net_str in networks:
            try:
                net = ipaddress.ip_network(net_str, strict=False)
            except ValueError:
                continue
            if net.version != 4:
                continue
            for host in net.hosts():
                host_str = str(host)
                if host_str in seen:
                    continue
                seen.add(host_str)
                hosts.append(host_str)
                if len(hosts) >= max_hosts:
                    return hosts
        return hosts

    async def stream_host_probes(self, hosts: list[str]) -> AsyncIterator[dict]:
        if not hosts:
            return
        sem = asyncio.Semaphore(max(1, int(self.discovery_concurrency)))
        tasks: list[asyncio.Task] = []

        async def _runner(target: str) -> Optional[dict]:
            async with sem:
                return await self.probe_host(target)

        for host in hosts:
            tasks.append(asyncio.create_task(_runner(host)))

        try:
            for task in asyncio.as_completed(tasks):
                result = await task
                if result:
                    yield result
        finally:
            for task in tasks:
                task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

    async def _find_node_by_fingerprint(self, fingerprint: str) -> Optional[dict]:
        if not fingerprint:
            return None
        networks = self.detect_discovery_networks()
        if not networks:
            return None
        hosts = self.hosts_for_networks(networks)
        if not hosts:
            return None
        async for result in self.stream_host_probes(hosts):
            if result.get("fingerprint") == fingerprint:
                return result
        return None

    async def _rediscover_node(self, node_id: str, fingerprint: str) -> None:
        try:
            interval = max(10, int(self.node_rediscovery_interval))
            while True:
                node = self.get_nodes().get(node_id)
                if not node or node.get("online"):
                    return
                match = await self._find_node_by_fingerprint(fingerprint)
                if match and match.get("url"):
                    new_url = self.normalize_node_url(match["url"])
                    if new_url:
                        node["url"] = new_url
                        self.save_nodes()
                        reachable, _ = await self.refresh_agent_metadata(node)
                        if reachable:
                            log.info("Node %s rediscovered at %s", node_id, new_url)
                            await self.broadcast_nodes()
                            return
                await asyncio.sleep(interval)
        except asyncio.CancelledError:
            pass
        finally:
            self._rediscovery_tasks.pop(node_id, None)
