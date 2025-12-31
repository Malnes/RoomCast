import logging
import socket
from typing import Any, Callable, Optional
from urllib.parse import parse_qs, urlparse

from fastapi import HTTPException

log = logging.getLogger("roomcast")


class SnapcastService:
    def __init__(
        self,
        *,
        snapcast_client: Any,
        normalize_percent: Callable[..., int],
        is_rpc_method_not_found_error: Callable[[Exception], bool],
    ) -> None:
        self._snapcast = snapcast_client
        self._normalize_percent = normalize_percent
        self._is_rpc_method_not_found_error = is_rpc_method_not_found_error

    @staticmethod
    def _safe_int(value: Any) -> Optional[int]:
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @classmethod
    def public_snap_stream(cls, entry: Optional[dict]) -> Optional[dict]:
        if not isinstance(entry, dict):
            return None
        uri = entry.get("uri")
        if not uri:
            status = entry.get("status") or {}
            uri = status.get("uri") or entry.get("location")
        parsed = urlparse(uri) if uri else None
        params = parse_qs(parsed.query) if parsed else {}
        sampleformat = (params.get("sampleformat") or [None])[0]
        format_info = None
        if sampleformat:
            parts = sampleformat.split(":")
            format_info = {
                "raw": sampleformat,
                "sample_rate": cls._safe_int(parts[0]) if parts else None,
                "bit_depth": parts[1] if len(parts) > 1 else None,
                "channels": parts[2] if len(parts) > 2 else None,
            }
        codec = (params.get("codec") or [None])[0]
        payload = {
            "id": entry.get("id") or entry.get("name"),
            "name": entry.get("name"),
            "uri": uri,
            "codec": codec,
            "format": format_info,
            "status": entry.get("status"),
            "properties": entry.get("properties"),
        }
        return payload

    @classmethod
    def summarize_snapserver_status(cls, status: Optional[dict]) -> tuple[dict[str, dict], dict[str, list[dict]]]:
        if not isinstance(status, dict):
            return {}, {}
        server = status.get("server") or {}
        streams: dict[str, dict] = {}
        for entry in server.get("streams", []) or []:
            if not isinstance(entry, dict):
                continue
            sid = entry.get("id") or entry.get("name")
            if not sid:
                continue
            streams[sid] = cls.public_snap_stream(entry)
        stream_clients: dict[str, list[dict]] = {}
        for group in server.get("groups", []) or []:
            if not isinstance(group, dict):
                continue
            stream_id = group.get("stream_id")
            if not stream_id:
                continue
            stream_clients.setdefault(stream_id, [])
            for client in group.get("clients", []) or []:
                if not isinstance(client, dict):
                    continue
                enriched = dict(client)
                enriched["_group_id"] = group.get("id")
                enriched["_group_name"] = group.get("name")
                stream_clients[stream_id].append(enriched)
        return streams, stream_clients

    def public_snap_client(self, client: dict, snapclient_nodes: dict[str, dict]) -> dict:
        cfg = client.get("config") or {}
        volume = cfg.get("volume") or {}
        host = client.get("host") or {}
        snap_info = client.get("snapclient") or {}
        client_id = client.get("id")
        linked_node = snapclient_nodes.get(client_id)
        volume_percent = volume.get("percent")
        try:
            volume_percent = self._normalize_percent(volume_percent, default=75)
        except Exception:
            volume_percent = None
        payload = {
            "id": client_id,
            "connected": bool(client.get("connected")),
            "latency_ms": cfg.get("latency"),
            "volume_percent": volume_percent,
            "muted": bool(volume.get("muted")) if isinstance(volume.get("muted"), bool) else None,
            "host": {
                "name": host.get("name"),
                "ip": host.get("ip"),
                "os": host.get("os"),
            },
            "version": snap_info.get("version"),
            "protocol": snap_info.get("protocolVersion"),
            "group_id": client.get("_group_id"),
            "group_name": client.get("_group_name"),
            "configured_name": cfg.get("name"),
            "node_id": linked_node.get("id") if linked_node else None,
            "node_name": linked_node.get("name") if linked_node else None,
        }
        return payload

    @staticmethod
    def _node_host_from_url(url: Optional[str]) -> Optional[str]:
        if not url or url.startswith("browser:"):
            return None
        try:
            parsed = urlparse(url)
        except ValueError:
            return None
        return parsed.hostname

    @classmethod
    def _node_host_aliases(cls, node_host: Optional[str]) -> set[str]:
        aliases: set[str] = set()
        if not node_host:
            return aliases
        host = node_host.strip()
        if not host:
            return aliases
        normalized = host.lower()
        aliases.add(normalized)
        if normalized.startswith("[") and normalized.endswith("]"):
            aliases.add(normalized[1:-1])
        if ":" in normalized and normalized.count(":") > 1 and not normalized.startswith("["):
            aliases.add(normalized)
        if "." in normalized:
            short = normalized.split(".", 1)[0]
            if short:
                aliases.add(short)
        try:
            resolved = socket.gethostbyname(host.strip("[]"))
        except (socket.gaierror, UnicodeError):
            resolved = None
        if resolved:
            aliases.add(resolved.strip().lower())
        return {value for value in aliases if value}

    def match_snapclient_for_node(self, node: dict, clients: list[dict]) -> Optional[dict]:
        if not clients:
            return None
        fallback: Optional[dict] = None

        def _consider(candidate: dict) -> Optional[dict]:
            nonlocal fallback
            if candidate.get("connected"):
                return candidate
            if fallback is None:
                fallback = candidate
            return None

        stored_id = (node.get("snapclient_id") or "").strip()
        if stored_id:
            for client in clients:
                if (client.get("id") or "").strip() == stored_id:
                    match = _consider(client)
                    if match:
                        return match
        fingerprint = (node.get("fingerprint") or "").strip()
        if fingerprint:
            for client in clients:
                if (client.get("id") or "").strip() == fingerprint:
                    match = _consider(client)
                    if match:
                        return match
        host_aliases = self._node_host_aliases(self._node_host_from_url(node.get("url")))
        if host_aliases:
            for client in clients:
                host = client.get("host") or {}
                client_matches: set[str] = set()
                name = (host.get("name") or "").strip().lower()
                if name:
                    client_matches.add(name)
                    if "." in name:
                        client_matches.add(name.split(".", 1)[0])
                ip = (host.get("ip") or "").strip().lower()
                if ip:
                    client_matches.add(ip)
                if not (client_matches & host_aliases):
                    continue
                match = _consider(client)
                if match:
                    return match
        node_name = (node.get("name") or "").strip().lower()
        if node_name:
            for client in clients:
                config = client.get("config") or {}
                client_name = (config.get("name") or "").strip().lower()
                if client_name and client_name == node_name:
                    match = _consider(client)
                    if match:
                        return match
        return fallback

    async def _snapcast_group_id_for_client(self, client_id: Optional[str]) -> Optional[str]:
        if not client_id:
            return None
        try:
            clients = await self._snapcast.list_clients()
        except Exception as exc:  # pragma: no cover - network dependency
            log.warning("WebRTC stream assignment: failed to list snapclients: %s", exc)
            return None
        for client in clients:
            if (client.get("id") or "").strip() == client_id:
                group_id = (client.get("_group_id") or "").strip()
                if group_id:
                    return group_id
                return None
        return None

    async def assign_webrtc_stream(self, client_id: str, stream_id: str) -> None:
        if not client_id or not stream_id:
            return
        try:
            await self._snapcast.set_client_stream(client_id, stream_id)
            return
        except Exception as exc:  # pragma: no cover - network dependency
            if not self._is_rpc_method_not_found_error(exc):
                log.warning("Failed to assign web relay client %s to %s: %s", client_id, stream_id, exc)
                raise
            group_id = await self._snapcast_group_id_for_client(client_id)
            if not group_id:
                raise RuntimeError(
                    f"Snapclient {client_id} is not registered with snapserver yet; cannot switch to {stream_id}"
                )
            try:
                await self._snapcast.set_group_stream(group_id, stream_id)
            except Exception as group_exc:  # pragma: no cover - network dependency
                raise RuntimeError(
                    f"Failed to move snapclient group {group_id} (client {client_id}) to {stream_id}: {group_exc}"
                )

    async def ensure_snapclient_stream(self, node: dict, channel: dict) -> str:
        if node.get("type") != "agent":
            raise HTTPException(status_code=400, detail="Channel assignment only applies to hardware nodes")
        try:
            clients = await self._snapcast.list_clients()
        except Exception as exc:  # pragma: no cover - network dependency
            log.exception("Failed to list snapcast clients")
            raise HTTPException(status_code=502, detail=f"Failed to talk to snapserver: {exc}") from exc
        client = self.match_snapclient_for_node(node, clients)
        if not client:
            raise HTTPException(status_code=409, detail="Snapclient for this node is offline or unidentified")
        if not client.get("connected"):
            raise HTTPException(status_code=409, detail="Snapclient for this node is offline")
        stream_id = channel.get("snap_stream")
        if not stream_id:
            raise HTTPException(status_code=400, detail="Channel is missing snap_stream mapping")
        if client.get("_stream_id") != stream_id:
            try:
                await self._snapcast.set_client_stream(client["id"], stream_id)
            except RuntimeError as exc:  # pragma: no cover - network dependency
                if self._is_rpc_method_not_found_error(exc):
                    group_id = client.get("_group_id")
                    if not group_id:
                        log.error("Snapclient %s missing group id; cannot switch stream", client.get("id"))
                        raise HTTPException(
                            status_code=502,
                            detail="Snapclient group unknown; cannot switch stream",
                        ) from exc
                    try:
                        await self._snapcast.set_group_stream(group_id, stream_id)
                    except Exception as group_exc:  # pragma: no cover - network dependency
                        log.exception("Failed to move snapclient group %s to stream %s", group_id, stream_id)
                        raise HTTPException(
                            status_code=502,
                            detail=f"Failed to switch snapclient group stream: {group_exc}",
                        ) from group_exc
                else:
                    log.exception("Failed to move snapclient %s to stream %s", client.get("id"), stream_id)
                    raise HTTPException(status_code=502, detail=f"Failed to switch snapclient stream: {exc}") from exc
            except Exception as exc:  # pragma: no cover - network dependency
                log.exception("Failed to move snapclient %s to stream %s", client.get("id"), stream_id)
                raise HTTPException(status_code=502, detail=f"Failed to switch snapclient stream: {exc}") from exc
        return str(client.get("id"))
