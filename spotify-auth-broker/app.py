from __future__ import annotations

import base64
import hmac
import json
import os
import time
from hashlib import sha256
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlparse

import httpx
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

app = FastAPI()

SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID", "").strip()
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET", "").strip()
ROOMCAST_SHARED_SECRET = os.getenv("ROOMCAST_SHARED_SECRET", "").strip()
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "").strip().rstrip("/")
ALLOWED_CALLBACK_HOSTS = [h.strip().lower() for h in os.getenv("ALLOWED_CALLBACK_HOSTS", "").split(",") if h.strip()]
STATE_TTL_SECONDS = int(os.getenv("STATE_TTL_SECONDS", "600"))


class StateError(Exception):
    pass


def _base64url_decode(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode(value + padding)


def _base64url_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("utf-8").rstrip("=")


def _verify_state(state: str) -> Dict[str, Any]:
    if not ROOMCAST_SHARED_SECRET:
        raise StateError("ROOMCAST_SHARED_SECRET is not configured")
    if "." not in state:
        raise StateError("Invalid state format")
    payload_b64, sig_b64 = state.split(".", 1)
    expected = hmac.new(ROOMCAST_SHARED_SECRET.encode("utf-8"), payload_b64.encode("utf-8"), sha256).digest()
    if not hmac.compare_digest(_base64url_encode(expected), sig_b64):
        raise StateError("Invalid state signature")
    payload = json.loads(_base64url_decode(payload_b64))
    if not isinstance(payload, dict):
        raise StateError("Invalid state payload")
    issued_at = float(payload.get("iat", 0))
    if issued_at and time.time() - issued_at > STATE_TTL_SECONDS:
        raise StateError("State expired")
    callback_url = str(payload.get("callback_url", "")).strip()
    if not callback_url:
        raise StateError("callback_url missing")
    parsed = urlparse(callback_url)
    if parsed.scheme not in {"http", "https"}:
        raise StateError("callback_url must be http or https")
    if ALLOWED_CALLBACK_HOSTS and (parsed.hostname or "").lower() not in ALLOWED_CALLBACK_HOSTS:
        raise StateError("callback_url host not allowed")
    return payload


def _sign_state(payload: Dict[str, Any]) -> str:
    if not ROOMCAST_SHARED_SECRET:
        raise StateError("ROOMCAST_SHARED_SECRET is not configured")
    payload_b64 = _base64url_encode(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    sig = hmac.new(ROOMCAST_SHARED_SECRET.encode("utf-8"), payload_b64.encode("utf-8"), sha256).digest()
    return f"{payload_b64}.{_base64url_encode(sig)}"


def _redirect_uri() -> str:
    if not PUBLIC_BASE_URL:
        raise HTTPException(status_code=500, detail="PUBLIC_BASE_URL is not configured")
    return f"{PUBLIC_BASE_URL}/callback"


def _validate_callback_url(callback_url: str) -> None:
    if not callback_url:
        raise HTTPException(status_code=400, detail="callback_url missing")
    parsed = urlparse(callback_url)
    if parsed.scheme not in {"http", "https"}:
        raise HTTPException(status_code=400, detail="callback_url must be http or https")
    if ALLOWED_CALLBACK_HOSTS and (parsed.hostname or "").lower() not in ALLOWED_CALLBACK_HOSTS:
        raise HTTPException(status_code=400, detail="callback_url host not allowed")


class AuthorizeRequest(BaseModel):
    callback_url: str = Field(..., description="RoomCast callback URL")
    source_id: str = Field(..., description="Spotify source id, e.g. spotify:a")
    device_id: Optional[str] = Field(default=None, description="Optional device identifier")


class RefreshRequest(BaseModel):
    refresh_token: str = Field(..., description="Spotify refresh token")


async def _exchange_code(code: str) -> Dict[str, Any]:
    if not SPOTIFY_CLIENT_ID or not SPOTIFY_CLIENT_SECRET:
        raise HTTPException(status_code=500, detail="Spotify client credentials are not configured")
    data = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": _redirect_uri(),
        "client_id": SPOTIFY_CLIENT_ID,
        "client_secret": SPOTIFY_CLIENT_SECRET,
    }
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.post("https://accounts.spotify.com/api/token", data=data)
    if resp.status_code >= 400:
        raise HTTPException(status_code=400, detail="Failed to exchange Spotify code")
    return resp.json()


async def _refresh_token(refresh_token: str) -> Dict[str, Any]:
    if not SPOTIFY_CLIENT_ID or not SPOTIFY_CLIENT_SECRET:
        raise HTTPException(status_code=500, detail="Spotify client credentials are not configured")
    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": SPOTIFY_CLIENT_ID,
        "client_secret": SPOTIFY_CLIENT_SECRET,
    }
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.post("https://accounts.spotify.com/api/token", data=data)
    if resp.status_code >= 400:
        raise HTTPException(status_code=400, detail="Failed to refresh Spotify token")
    return resp.json()


async def _post_token(callback_url: str, payload: Dict[str, Any]) -> None:
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.post(callback_url, json=payload)
    if resp.status_code >= 400:
        raise HTTPException(status_code=400, detail="Failed to deliver token to callback URL")


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.post("/authorize")
async def authorize(payload: AuthorizeRequest) -> Dict[str, str]:
    if not SPOTIFY_CLIENT_ID:
        raise HTTPException(status_code=500, detail="Spotify client id is not configured")
    callback_url = payload.callback_url.strip()
    _validate_callback_url(callback_url)
    state_payload = {
        "iat": time.time(),
        "callback_url": callback_url,
        "source_id": payload.source_id,
    }
    if payload.device_id:
        state_payload["device_id"] = payload.device_id
    state = _sign_state(state_payload)
    scope = (
        "user-read-playback-state user-modify-playback-state "
        "user-read-recently-played streaming user-read-email user-read-private"
    )
    params = {
        "client_id": SPOTIFY_CLIENT_ID,
        "response_type": "code",
        "redirect_uri": _redirect_uri(),
        "scope": scope,
        "state": state,
    }
    return {"url": f"https://accounts.spotify.com/authorize?{httpx.QueryParams(params)}"}


@app.post("/refresh")
async def refresh(payload: RefreshRequest) -> Dict[str, Any]:
    return await _refresh_token(payload.refresh_token)


@app.get("/callback")
async def callback(
    code: Optional[str] = Query(default=None),
    state: Optional[str] = Query(default=None),
    error: Optional[str] = Query(default=None),
):
    if error:
        return HTMLResponse(content=f"Spotify error: {error}", status_code=400)
    if not code or not state:
        raise HTTPException(status_code=400, detail="Missing code or state")

    try:
        payload = _verify_state(state)
    except StateError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    token = await _exchange_code(code)
    callback_url = str(payload.get("callback_url"))
    deliver_payload = {
        "device_id": payload.get("device_id"),
        "source_id": payload.get("source_id"),
        "token": token,
        "received_at": int(time.time()),
    }
    await _post_token(callback_url, deliver_payload)

    return HTMLResponse(
        content="Spotify linked. You can close this tab.",
        status_code=200,
    )
