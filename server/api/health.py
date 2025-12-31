from __future__ import annotations

from fastapi import APIRouter


def create_health_router() -> APIRouter:
    router = APIRouter()

    @router.get("/api/health")
    async def health() -> dict:
        return {"status": "ok"}

    return router
