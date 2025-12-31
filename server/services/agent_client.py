from __future__ import annotations

from typing import Optional

import httpx
from fastapi import HTTPException


class AgentClient:
    def __init__(self, *, timeout_seconds: float = 5.0) -> None:
        self._timeout_seconds = timeout_seconds

    async def post(self, node: dict, path: str, payload: dict, *, require_secret: bool = True) -> dict:
        url = f"{node['url']}{path}"
        headers = self._headers(node) if require_secret else {}
        async with httpx.AsyncClient(timeout=self._timeout_seconds) as client:
            resp = await client.post(url, json=payload, headers=headers)
        if resp.status_code >= 400:
            raise HTTPException(status_code=resp.status_code, detail=resp.text)
        return resp.json()

    async def get(self, node: dict, path: str, *, require_secret: bool = True) -> dict:
        url = f"{node['url']}{path}"
        headers = self._headers(node) if require_secret else {}
        async with httpx.AsyncClient(timeout=self._timeout_seconds) as client:
            resp = await client.get(url, headers=headers)
        if resp.status_code >= 400:
            raise HTTPException(status_code=resp.status_code, detail=resp.text)
        return resp.json()

    @staticmethod
    def _headers(node: dict) -> dict:
        secret: Optional[str] = node.get("agent_secret")
        if not secret:
            raise HTTPException(status_code=409, detail="Node is not paired. Re-register or pair it first.")
        return {"X-Agent-Secret": secret}
