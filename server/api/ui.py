from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse, Response


def create_ui_router(static_dir: Path) -> APIRouter:
    router = APIRouter()

    @router.get("/sw.js", include_in_schema=False)
    async def service_worker() -> FileResponse:
        """Serve the service worker from the app root for maximum scope."""
        response = FileResponse(static_dir / "sw.js", media_type="application/javascript")
        response.headers["Cache-Control"] = "no-cache"
        return response

    @router.get("/", include_in_schema=False)
    async def serve_index():
        index_path = static_dir / "index.html"
        if not index_path.exists():
            raise HTTPException(status_code=404, detail="UI not found")
        return FileResponse(index_path)

    @router.get("/favicon.ico", include_in_schema=False)
    async def favicon():
        # No asset yet; avoid 404 noise
        return Response(status_code=204)

    @router.get("/web-node", include_in_schema=False)
    async def serve_web_node():
        path = static_dir / "web-node.html"
        if not path.exists():
            raise HTTPException(status_code=404, detail="UI not found")
        return FileResponse(path)

    return router
