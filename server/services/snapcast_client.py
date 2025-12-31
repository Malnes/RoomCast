from __future__ import annotations

import json
from typing import Optional

import websockets


def is_rpc_method_not_found_error(exc: Exception) -> bool:
    if isinstance(exc, RuntimeError):
        payload = exc.args[0] if exc.args else None
        if isinstance(payload, dict):
            message = str(payload.get("message") or "").lower()
            code = payload.get("code")
            if code == -32601 or "method not found" in message:
                return True
        text = str(payload).lower() if payload is not None else ""
        if "method not found" in text:
            return True
    return "method not found" in str(exc).lower()


class SnapcastClient:
    def __init__(self, host: str, port: int = 1780) -> None:
        self.url = f"ws://{host}:{port}/jsonrpc"
        self._supports_client_setstream: Optional[bool] = None

    async def _rpc(self, method: str, params: Optional[dict] = None) -> dict:
        payload = {"id": 1, "jsonrpc": "2.0", "method": method}
        if params:
            payload["params"] = params

        async with websockets.connect(self.url) as ws:
            await ws.send(json.dumps(payload))
            raw = await ws.recv()
        resp = json.loads(raw)
        if "error" in resp:
            raise RuntimeError(resp["error"])
        return resp.get("result", {})

    async def status(self) -> dict:
        return await self._rpc("Server.GetStatus")

    async def set_client_volume(self, client_id: str, percent: int) -> dict:
        params = {"id": client_id, "volume": {"percent": percent}}
        return await self._rpc("Client.SetVolume", params)

    async def list_clients(self) -> list:
        status = await self.status()
        clients = []
        for group in status.get("server", {}).get("groups", []):
            group_id = group.get("id")
            stream_id = group.get("stream_id")
            for client in group.get("clients", []):
                enriched = dict(client)
                enriched["_group_id"] = group_id
                enriched["_stream_id"] = stream_id
                clients.append(enriched)
        return clients

    async def set_client_stream(self, client_id: str, stream_id: str) -> dict:
        """Assign a client to a stream.

        Snapserver JSON-RPC differs by version:
        - Some versions support `Client.SetStream`.
        - Some only support `Group.SetStream`.

        We probe once and then fall back to group switching when needed.
        """
        params = {"id": client_id, "stream_id": stream_id}

        if self._supports_client_setstream is not False:
            try:
                result = await self._rpc("Client.SetStream", params)
            except Exception as exc:
                if is_rpc_method_not_found_error(exc):
                    self._supports_client_setstream = False
                else:
                    raise
            else:
                self._supports_client_setstream = True
                return result

        status = await self.status()
        group_id: Optional[str] = None
        for group in status.get("server", {}).get("groups", []) or []:
            if not isinstance(group, dict):
                continue
            for client in group.get("clients", []) or []:
                if isinstance(client, dict) and client.get("id") == client_id:
                    group_id = group.get("id")
                    break
            if group_id:
                break
        if not group_id:
            raise RuntimeError(f"Snapclient {client_id} is not registered with snapserver yet")
        return await self.set_group_stream(group_id, stream_id)

    async def set_group_stream(self, group_id: str, stream_id: str) -> dict:
        params = {"id": group_id, "stream_id": stream_id}
        return await self._rpc("Group.SetStream", params)

    async def delete_client(self, client_id: str) -> dict:
        params = {"id": client_id}
        return await self._rpc("Server.DeleteClient", params)
