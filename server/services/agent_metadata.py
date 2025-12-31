from __future__ import annotations

import time
from typing import Any, Awaitable, Callable

from services.agent_client import AgentClient


class AgentMetadataService:
    def __init__(
        self,
        *,
        agent_client: AgentClient,
        mark_node_online: Callable[[dict], bool],
        mark_node_offline: Callable[[dict], bool],
        normalize_percent: Callable[..., int],
        get_node_max_volume: Callable[[dict], int],
        schedule_node_rediscovery: Callable[[dict], Any],
        configure_agent_audio: Callable[[dict], Awaitable[dict]],
        sync_node_max_volume: Callable[[dict], Awaitable[Any]],
        save_nodes: Callable[[], None],
    ) -> None:
        self._agent_client = agent_client
        self._mark_node_online = mark_node_online
        self._mark_node_offline = mark_node_offline
        self._normalize_percent = normalize_percent
        self._get_node_max_volume = get_node_max_volume
        self._schedule_node_rediscovery = schedule_node_rediscovery
        self._configure_agent_audio = configure_agent_audio
        self._sync_node_max_volume = sync_node_max_volume
        self._save_nodes = save_nodes

    async def refresh_agent_metadata(self, node: dict, *, persist: bool = True) -> tuple[bool, bool]:
        if node.get("type") != "agent":
            return False, False

        now = time.time()
        try:
            data = await self._agent_client.get(node, "/health", require_secret=False)
        except Exception:
            changed = self._mark_node_offline(node, timestamp=now)
            if changed and persist:
                self._save_nodes()
            self._schedule_node_rediscovery(node)
            return False, changed

        changed = False
        if self._mark_node_online(node, timestamp=now):
            changed = True

        fingerprint = data.get("fingerprint")
        if isinstance(fingerprint, str) and fingerprint and node.get("fingerprint") != fingerprint:
            node["fingerprint"] = fingerprint
            changed = True

        version = data.get("version")
        if version and node.get("agent_version") != version:
            node["agent_version"] = version
            changed = True

        configured = bool(data.get("configured"))
        if node.get("audio_configured") != configured:
            node["audio_configured"] = configured
            changed = True
            if configured:
                node.setdefault("_needs_reconfig", False)
            else:
                node["_needs_reconfig"] = True

        if "updating" in data:
            updating = bool(data.get("updating"))
            if node.get("updating") != updating:
                node["updating"] = updating
                changed = True

        if "playback_device" in data:
            device = data.get("playback_device")
            if node.get("playback_device") != device:
                node["playback_device"] = device
                changed = True

        if isinstance(data.get("outputs"), dict):
            outputs = data["outputs"]
            if node.get("outputs") != outputs:
                node["outputs"] = outputs
                changed = True

        wifi_payload = data.get("wifi")
        sanitized_wifi = None
        if isinstance(wifi_payload, dict):
            percent_raw = wifi_payload.get("percent")
            if isinstance(percent_raw, (int, float)):
                percent_val = max(0, min(100, int(round(percent_raw))))
                sanitized_wifi = {"percent": percent_val}
                signal_raw = wifi_payload.get("signal_dbm")
                if isinstance(signal_raw, (int, float)):
                    sanitized_wifi["signal_dbm"] = float(signal_raw)
                iface_raw = wifi_payload.get("interface")
                if isinstance(iface_raw, str):
                    iface = iface_raw.strip()
                    if iface:
                        sanitized_wifi["interface"] = iface

        if sanitized_wifi:
            if node.get("wifi") != sanitized_wifi:
                node["wifi"] = sanitized_wifi
                changed = True
        elif "wifi" in node:
            node.pop("wifi", None)
            changed = True

        if "max_volume_percent" in data:
            max_vol = self._normalize_percent(
                data.get("max_volume_percent"),
                default=self._get_node_max_volume(node),
            )
            if node.get("max_volume_percent") != max_vol:
                node["max_volume_percent"] = max_vol
                changed = True

        needs_reconfig = bool(node.pop("_needs_reconfig", False))
        if configured and needs_reconfig:
            try:
                await self._configure_agent_audio(node)
                await self._sync_node_max_volume(node)
            except Exception:
                node["audio_configured"] = False
                node["_needs_reconfig"] = True
            else:
                node["audio_configured"] = True
                changed = True

        if persist and changed:
            self._save_nodes()
        return True, changed
