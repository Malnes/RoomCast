from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from typing import Any, Optional

import httpx
from fastapi import HTTPException


def parse_spotify_error(detail: Any) -> dict:
    payload: Any = detail
    if isinstance(detail, str):
        try:
            payload = json.loads(detail)
        except Exception:
            payload = {"error": {"message": detail}}
    if not isinstance(payload, dict):
        return {"message": str(detail)}
    error = payload.get("error")
    if isinstance(error, dict):
        return {
            "status": error.get("status"),
            "message": error.get("message"),
            "reason": error.get("reason"),
        }
    return {"message": payload.get("message") or str(detail)}


async def spotify_refresh(
    token: dict,
    identifier: Optional[str],
    *,
    spotify_auth_broker_url: str,
    save_token: Callable[[dict, Optional[str]], None],
    timeout: float = 10,
) -> dict:
    if not token or "refresh_token" not in token:
        raise HTTPException(status_code=401, detail="Spotify not authorized")
    if not spotify_auth_broker_url:
        raise HTTPException(status_code=503, detail="Spotify auth broker not configured")
    payload = {"refresh_token": token["refresh_token"]}
    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.post(
            f"{spotify_auth_broker_url.rstrip('/')}/refresh",
            json=payload,
        )
    if resp.status_code >= 400:
        raise HTTPException(status_code=401, detail="Failed to refresh Spotify token")
    data = resp.json()
    if "access_token" not in data:
        raise HTTPException(status_code=502, detail="Spotify auth broker returned an invalid token")
    token["access_token"] = data["access_token"]
    token["expires_in"] = data.get("expires_in")
    save_token(token, identifier)
    return token


async def spotify_request(
    method: str,
    path: str,
    token: dict,
    identifier: Optional[str],
    *,
    spotify_refresh_func: Callable[[dict, Optional[str]], Awaitable[dict]],
    timeout: float = 10,
    **kwargs,
) -> httpx.Response:
    headers = kwargs.pop("headers", {})
    headers["Authorization"] = f"Bearer {token['access_token']}"
    url = f"https://api.spotify.com/v1{path}"
    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.request(method, url, headers=headers, **kwargs)
    if resp.status_code == 401:
        token = await spotify_refresh_func(token, identifier)
        headers["Authorization"] = f"Bearer {token['access_token']}"
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.request(method, url, headers=headers, **kwargs)
    return resp


async def spotify_control(
    path: str,
    method: str = "PUT",
    *,
    channel_id: Optional[str],
    ensure_spotify_token: Callable[[Optional[str]], dict],
    spotify_request_func: Callable[..., Awaitable[httpx.Response]],
) -> None:
    token = ensure_spotify_token(channel_id)
    resp = await spotify_request_func(method, path, token, identifier=channel_id)
    if resp.status_code < 400:
        return
    try:
        detail = resp.json()
    except Exception:
        detail = resp.text
    raise HTTPException(status_code=resp.status_code, detail=detail)
