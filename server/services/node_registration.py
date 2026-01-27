from __future__ import annotations

import time
import uuid
from typing import Any, Awaitable, Callable, Optional

import httpx
from fastapi import HTTPException

from services.agent_client import AgentClient


class NodeRegistrationService:
    def __init__(
        self,
        *,
        get_nodes: Callable[[], dict],
        normalize_node_url: Callable[[str], str],
        normalize_percent: Callable[..., int],
        normalize_stereo_mode: Callable[[object], str],
        select_initial_channel_id: Callable[..., Optional[str]],
        default_eq_state: Callable[[], dict],
        snapserver_agent_host: str,
        snapclient_port: int,
        agent_client: AgentClient,
        refresh_agent_metadata: Callable[..., Awaitable[tuple[bool, bool]]],
        sync_node_max_volume: Callable[..., Awaitable[None]],
        save_nodes: Callable[[], None],
        broadcast_nodes: Callable[[], Awaitable[None]],
        public_node: Callable[[dict], dict],
        sonos_service: Any,
    ) -> None:
        self._get_nodes = get_nodes
        self._normalize_node_url = normalize_node_url
        self._normalize_percent = normalize_percent
        self._normalize_stereo_mode = normalize_stereo_mode
        self._select_initial_channel_id = select_initial_channel_id
        self._default_eq_state = default_eq_state
        self._snapserver_agent_host = snapserver_agent_host
        self._snapclient_port = snapclient_port
        self._agent_client = agent_client
        self._refresh_agent_metadata = refresh_agent_metadata
        self._sync_node_max_volume = sync_node_max_volume
        self._save_nodes = save_nodes
        self._broadcast_nodes = broadcast_nodes
        self._public_node = public_node
        self._sonos_service = sonos_service

    def register_node_internal(
        self,
        reg: Any,
        *,
        fingerprint: Optional[str] = None,
        normalized_url: Optional[str] = None,
    ) -> dict:
        nodes = self._get_nodes()
        normalized = normalized_url or self._normalize_node_url(reg.url)
        now = time.time()
        if getattr(reg, "id", None) and reg.id in nodes:
            node = nodes[reg.id]
            node["url"] = normalized
            node["last_seen"] = now
            if fingerprint:
                node["fingerprint"] = fingerprint
            node["online"] = True
            node.pop("offline_since", None)
            return node
        if fingerprint:
            for existing in nodes.values():
                if existing.get("fingerprint") == fingerprint:
                    existing["url"] = normalized
                    existing["last_seen"] = now
                    existing["fingerprint"] = fingerprint
                    existing["online"] = True
                    existing.pop("offline_since", None)
                    return existing
        for existing in nodes.values():
            if (existing.get("url") or "").rstrip("/") == normalized:
                existing["last_seen"] = now
                if fingerprint:
                    existing["fingerprint"] = fingerprint
                existing["online"] = True
                existing.pop("offline_since", None)
                return existing

        node_id = getattr(reg, "id", None) or str(uuid.uuid4())
        previous = nodes.get(node_id, {})
        if normalized.startswith("browser:"):
            node_type = "browser"
        elif normalized.startswith("sonos://"):
            node_type = "sonos"
        else:
            node_type = "agent"

        nodes[node_id] = {
            "id": node_id,
            "name": reg.name,
            "url": normalized,
            "last_seen": now,
            "type": node_type,
            "eq": self._default_eq_state(),
            "agent_secret": previous.get("agent_secret"),
            "owner_id": previous.get("owner_id"),
            "audio_configured": True if node_type in {"browser", "sonos"} else previous.get("audio_configured", False),
            "agent_version": previous.get("agent_version"),
            "volume_percent": self._normalize_percent(previous.get("volume_percent", 75), default=75),
            "max_volume_percent": self._normalize_percent(previous.get("max_volume_percent", 100), default=100),
            "muted": previous.get("muted", False),
            "stereo_mode": self._normalize_stereo_mode(previous.get("stereo_mode")),
            "updating": bool(previous.get("updating", False)),
            "playback_device": previous.get("playback_device"),
            "outputs": previous.get("outputs", {}),
            "fingerprint": fingerprint or previous.get("fingerprint"),
            "channel_id": self._select_initial_channel_id(previous.get("channel_id"), fallback=node_type != "browser"),
            "snapclient_id": previous.get("snapclient_id"),
            "online": True,
            "offline_since": None,
            "is_controller": bool(previous.get("is_controller")),
            "sonos_udn": previous.get("sonos_udn"),
            "sonos_rincon": previous.get("sonos_rincon"),
        }
        self._save_nodes()
        return nodes[node_id]

    async def fetch_agent_fingerprint(self, url: str) -> Optional[str]:
        normalized = self._normalize_node_url(url)
        if not normalized or normalized.startswith("browser:") or normalized.startswith("sonos://"):
            return None
        try:
            data = await self._agent_client.get({"url": normalized}, "/health", require_secret=False)
        except Exception:
            return None
        fingerprint = data.get("fingerprint")
        if isinstance(fingerprint, str) and fingerprint:
            return fingerprint
        return None

    async def request_agent_secret(self, node: dict, force: bool = False, *, recovery_code: str | None = None) -> str:
        if node.get("type") != "agent":
            raise HTTPException(status_code=400, detail="Pairing only applies to hardware nodes")
        url = f"{node['url'].rstrip('/')}/pair"
        payload: dict = {"force": bool(force)}
        if isinstance(recovery_code, str) and recovery_code.strip():
            payload["recovery_code"] = recovery_code.strip()
        headers = {}
        # If we already have a secret (same controller), prove it so the agent allows rotation.
        secret = node.get("agent_secret")
        if isinstance(secret, str) and secret:
            headers["X-Agent-Secret"] = secret
        async with httpx.AsyncClient(timeout=5) as client:
            resp = await client.post(url, json=payload, headers=headers)
        if resp.status_code >= 400:
            raise HTTPException(status_code=resp.status_code, detail=resp.text or "Failed to pair node")
        try:
            data = resp.json()
        except ValueError:
            raise HTTPException(status_code=502, detail="Invalid response from node agent")
        secret = data.get("secret")
        if not secret:
            raise HTTPException(status_code=502, detail="Node agent did not return a secret")
        return secret

    async def configure_agent_audio(self, node: dict) -> dict:
        if node.get("type") != "agent":
            raise HTTPException(status_code=400, detail="Audio configuration only applies to hardware nodes")
        payload = {
            "snapserver_host": self._snapserver_agent_host,
            "snapserver_port": self._snapclient_port,
        }
        result = await self._agent_client.post(node, "/config/snapclient", payload)
        node["audio_configured"] = bool(result.get("configured", True))
        await self._refresh_agent_metadata(node, persist=False)
        self._save_nodes()
        return result

    async def register_node_payload(self, reg: Any, *, mark_controller: bool = False) -> dict:
        normalized_url = self._normalize_node_url(reg.url)
        reg.url = normalized_url
        fingerprint = getattr(reg, "fingerprint", None)
        desc: Optional[dict] = None

        if normalized_url.startswith("sonos://"):
            ip = self._sonos_service.ip_from_url(normalized_url)
            if not ip:
                raise HTTPException(status_code=400, detail="Invalid Sonos URL")
            desc = await self._sonos_service.fetch_description(ip)
            if not desc:
                raise HTTPException(status_code=502, detail="Failed to read Sonos device description")
            fingerprint = fingerprint or desc.get("udn")
        else:
            fingerprint = fingerprint or await self.fetch_agent_fingerprint(normalized_url)

        node = self.register_node_internal(reg, fingerprint=fingerprint, normalized_url=normalized_url)
        if mark_controller:
            node["is_controller"] = True
            self._save_nodes()

        if node.get("type") == "sonos":
            ip = self._sonos_service.ip_from_url(node.get("url"))
            if not ip:
                raise HTTPException(status_code=400, detail="Invalid Sonos node URL")
            if desc is None:
                desc = await self._sonos_service.fetch_description(ip)

            friendly_name = (desc.get("friendly_name") if isinstance(desc, dict) else None) or None
            zone_name = await self._sonos_service.get_zone_name(ip)
            sonos_app_name = zone_name or friendly_name

            if isinstance(desc, dict):
                node["sonos_udn"] = desc.get("udn")
                node["sonos_rincon"] = desc.get("rincon")
                node["fingerprint"] = node.get("fingerprint") or desc.get("udn")
            node["sonos_friendly_name"] = friendly_name
            node["sonos_zone_name"] = zone_name

            name_is_custom = node.get("name_is_custom")
            if not isinstance(name_is_custom, bool):
                current_name = (node.get("name") or "").strip()
                inferred_custom = False
                if current_name:
                    if sonos_app_name and current_name != sonos_app_name:
                        if friendly_name and current_name != friendly_name:
                            inferred_custom = True
                node["name_is_custom"] = inferred_custom
                name_is_custom = inferred_custom

            if not name_is_custom and sonos_app_name:
                node["name"] = sonos_app_name
            node["audio_configured"] = True
            node["agent_secret"] = None
            self._save_nodes()
            await self._broadcast_nodes()
            return self._public_node(node)

        if node.get("type") == "agent":
            try:
                secret = await self.request_agent_secret(node, force=True)
                node["agent_secret"] = secret
                await self.configure_agent_audio(node)
                await self._sync_node_max_volume(node)
            except HTTPException as exc:
                if exc.status_code in {423, 403} and "recovery" in str(exc.detail).lower():
                    node["agent_secret"] = None
                    node["audio_configured"] = False
                    self._save_nodes()
                    await self._broadcast_nodes()
                    return self._public_node(node)
                self._get_nodes().pop(node.get("id"), None)
                self._save_nodes()
                raise
            except Exception:
                self._get_nodes().pop(node.get("id"), None)
                self._save_nodes()
                raise
            self._save_nodes()

        await self._broadcast_nodes()
        return self._public_node(node)
