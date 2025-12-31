import logging
from typing import Any, Callable

from fastapi import APIRouter, Depends, HTTPException

from api.nodes import VolumePayload


log = logging.getLogger("roomcast")


def create_snapcast_router(
    *,
    snapcast_client: Any,
    require_admin: Callable,
) -> APIRouter:
    router = APIRouter()

    @router.get("/api/snapcast/status")
    async def snapcast_status() -> dict:
        try:
            status = await snapcast_client.status()
            return status
        except Exception as exc:  # pragma: no cover - network paths
            log.exception("Failed to fetch snapcast status")
            raise HTTPException(status_code=502, detail=str(exc))

    @router.post("/api/snapcast/clients/forget-disconnected")
    async def snapcast_forget_disconnected(_: dict = Depends(require_admin)) -> dict:
        try:
            status = await snapcast_client.status()
        except Exception as exc:  # pragma: no cover - network paths
            log.exception("Failed to fetch snapcast status")
            raise HTTPException(status_code=502, detail=str(exc))

        groups = (status.get("server") or {}).get("groups") or []
        disconnected_ids: list[str] = []
        for group in groups:
            if not isinstance(group, dict):
                continue
            for client in group.get("clients", []) or []:
                if not isinstance(client, dict):
                    continue
                if client.get("connected") is False and client.get("id"):
                    disconnected_ids.append(str(client.get("id")))

        removed: list[str] = []
        failed: list[dict] = []
        for client_id in disconnected_ids:
            try:
                await snapcast_client.delete_client(client_id)
                removed.append(client_id)
            except Exception as exc:  # pragma: no cover - network paths
                log.warning("Failed to delete snapcast client %s: %s", client_id, exc)
                failed.append({"id": client_id, "error": str(exc)})

        return {
            "ok": True,
            "found": len(disconnected_ids),
            "removed": removed,
            "failed": failed,
        }

    @router.post("/api/snapcast/clients/{client_id}/volume")
    async def snapcast_volume(client_id: str, payload: VolumePayload) -> dict:
        try:
            result = await snapcast_client.set_client_volume(client_id, payload.percent)
            return {"ok": True, "result": result}
        except Exception as exc:  # pragma: no cover - network paths
            log.exception("Failed to set snapcast volume")
            raise HTTPException(status_code=502, detail=str(exc))

    @router.post("/api/snapcast/master-volume")
    async def snapcast_master_volume(payload: VolumePayload) -> dict:
        try:
            clients = await snapcast_client.list_clients()
            updated: list[str] = []
            for client in clients:
                await snapcast_client.set_client_volume(client["id"], payload.percent)
                updated.append(client["id"])
            return {"ok": True, "updated": updated}
        except Exception as exc:  # pragma: no cover
            log.exception("Failed to set master volume")
            raise HTTPException(status_code=502, detail=str(exc))

    return router
