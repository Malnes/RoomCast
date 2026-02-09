from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Callable, Optional

from fastapi import HTTPException


class SpotifyConfigService:
    def __init__(
        self,
        *,
        resolve_spotify_source_id: Callable[[Optional[str]], Optional[str]],
        normalize_spotify_source_id: Callable[[Optional[str]], Optional[str]],
        resolve_channel_id: Callable[[Optional[str]], Optional[str]],
        get_spotify_source: Callable[[str], dict],
        spotify_client_id_default: Optional[str],
        spotify_client_secret_default: Optional[str],
        spotify_redirect_uri_default: Optional[str],
    ) -> None:
        self._resolve_spotify_source_id = resolve_spotify_source_id
        self._normalize_spotify_source_id = normalize_spotify_source_id
        self._resolve_channel_id = resolve_channel_id
        self._get_spotify_source = get_spotify_source
        self._spotify_client_id_default = spotify_client_id_default
        self._spotify_client_secret_default = spotify_client_secret_default
        self._spotify_redirect_uri_default = spotify_redirect_uri_default

    @staticmethod
    def _normalize_volume_value(value: object, fallback: int = 75) -> int:
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            return fallback
        return max(0, min(100, parsed))

    def _state_path(self, spotify_source_id: str) -> Path:
        source = self._get_spotify_source(spotify_source_id)
        config_path = Path(source["config_path"])
        suffix = config_path.suffix or ".json"
        stem = config_path.stem if config_path.suffix else config_path.name
        return config_path.with_name(f"{stem}-state{suffix}")

    def _read_state(self, spotify_source_id: str) -> dict:
        path = self._state_path(spotify_source_id)
        if not path.exists():
            return {}
        try:
            data = json.loads(path.read_text())
        except json.JSONDecodeError:
            return {}
        return data if isinstance(data, dict) else {}

    def _write_state(self, spotify_source_id: str, state: dict) -> None:
        path = self._state_path(spotify_source_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(state, indent=2))

    def roomcast_last_volume(self, identifier: Optional[str] = None) -> int:
        spotify_source_id = self._resolve_spotify_source_id(identifier)
        if spotify_source_id is None:
            return 75
        state = self._read_state(spotify_source_id)
        if "roomcast_last_volume" in state:
            return self._normalize_volume_value(state.get("roomcast_last_volume"), 75)
        source = self._get_spotify_source(spotify_source_id)
        cfg_path = Path(source["config_path"])
        if not cfg_path.exists():
            return 75
        try:
            data = json.loads(cfg_path.read_text())
        except json.JSONDecodeError:
            return 75
        if not isinstance(data, dict):
            return 75
        return self._normalize_volume_value(data.get("initial_volume"), 75)

    def set_roomcast_last_volume(self, percent: int, identifier: Optional[str] = None) -> int:
        spotify_source_id = self._resolve_spotify_source_id(identifier)
        if spotify_source_id is None:
            raise HTTPException(status_code=400, detail="Spotify source not configured")
        value = self._normalize_volume_value(percent, 75)
        state = self._read_state(spotify_source_id)
        state["roomcast_last_volume"] = value
        self._write_state(spotify_source_id, state)
        return value

    def read_spotify_config(self, identifier: Optional[str] = None, include_secret: bool = False) -> dict:
        resolved_channel_id = None
        spotify_source_id = self._resolve_spotify_source_id(identifier)
        if spotify_source_id is None:
            # Keep a stable shape for UI code even when channel is not Spotify.
            resolved_channel_id = self._resolve_channel_id(identifier)
            cfg = {
                "channel_id": resolved_channel_id,
                "source_id": None,
                "username": "",
                "device_name": "RoomCast",
                "bitrate": 320,
                "initial_volume": 75,
                "roomcast_last_volume": 75,
                "normalisation": True,
                "show_output_volume_slider": True,
                "has_password": False,
                "client_id": (os.getenv("SPOTIFY_CLIENT_ID") or self._spotify_client_id_default or "").strip(),
                "has_client_secret": bool(os.getenv("SPOTIFY_CLIENT_SECRET") or self._spotify_client_secret_default),
                "redirect_uri": os.getenv("SPOTIFY_REDIRECT_URI") or self._spotify_redirect_uri_default,
                "has_oauth_token": False,
            }
            if include_secret:
                cfg["client_secret"] = os.getenv("SPOTIFY_CLIENT_SECRET") or self._spotify_client_secret_default or ""
            return cfg

        # If identifier was a channel id, expose it for UI convenience.
        if not self._normalize_spotify_source_id(identifier):
            resolved_channel_id = self._resolve_channel_id(identifier)
        source = self._get_spotify_source(spotify_source_id)
        cfg_path = Path(source["config_path"])
        token = self.load_token(spotify_source_id)
        has_token = bool(token and token.get("access_token"))
        config_exists = cfg_path.exists()
        try:
            data = json.loads(cfg_path.read_text()) if config_exists else {}
        except json.JSONDecodeError:
            data = {}

        stored_client_id = (
            data.get("client_id")
            or os.getenv("SPOTIFY_CLIENT_ID")
            or self._spotify_client_id_default
            or ""
        ).strip()
        stored_client_secret = (
            data.get("client_secret")
            or os.getenv("SPOTIFY_CLIENT_SECRET")
            or self._spotify_client_secret_default
            or ""
        )
        stored_redirect = (
            data.get("redirect_uri")
            or os.getenv("SPOTIFY_REDIRECT_URI")
            or self._spotify_redirect_uri_default
        )

        cfg = {
            "channel_id": resolved_channel_id,
            "source_id": spotify_source_id,
            "username": data.get("username", ""),
            "device_name": data.get("device_name", "RoomCast"),
            "bitrate": data.get("bitrate", 320),
            "initial_volume": data.get("initial_volume", 75),
            "roomcast_last_volume": self.roomcast_last_volume(spotify_source_id),
            "normalisation": data.get("normalisation", True),
            "show_output_volume_slider": data.get("show_output_volume_slider", True),
            "has_password": bool(data.get("password")),
            "client_id": stored_client_id,
            "has_client_secret": bool(stored_client_secret),
            "redirect_uri": stored_redirect,
            "has_oauth_token": has_token,
        }
        if include_secret:
            cfg["client_secret"] = stored_client_secret
        elif not config_exists:
            cfg["client_secret"] = "***" if stored_client_secret else ""
        return cfg

    def read_librespot_status(self, identifier: Optional[str] = None) -> dict:
        spotify_source_id = self._resolve_spotify_source_id(identifier)
        if spotify_source_id is None:
            return {"state": "unknown", "message": "Not a Spotify channel"}
        source = self._get_spotify_source(spotify_source_id)
        status_path = Path(source["status_path"])
        if not status_path.exists():
            return {"state": "unknown", "message": "No status yet"}
        try:
            return json.loads(status_path.read_text())
        except json.JSONDecodeError:
            return {"state": "unknown", "message": "Invalid status file"}

    def load_token(self, identifier: Optional[str] = None) -> Optional[dict]:
        spotify_source_id = self._resolve_spotify_source_id(identifier)
        if spotify_source_id is None:
            return None
        source = self._get_spotify_source(spotify_source_id)
        token_path = Path(source["token_path"])
        if not token_path.exists():
            return None
        try:
            return json.loads(token_path.read_text())
        except json.JSONDecodeError:
            return None

    def save_token(self, data: dict, identifier: Optional[str] = None) -> None:
        if not data:
            return
        spotify_source_id = self._resolve_spotify_source_id(identifier)
        if spotify_source_id is None:
            raise HTTPException(status_code=400, detail="Spotify source not configured")
        source = self._get_spotify_source(spotify_source_id)
        token_path = Path(source["token_path"])
        expires_at: Optional[float] = None
        expires_in = data.get("expires_in")
        if expires_in is not None:
            try:
                expires_at = time.time() + max(int(expires_in), 60)
            except (TypeError, ValueError):
                expires_at = None
        if expires_at is not None:
            data["expires_at"] = expires_at
        token_path.parent.mkdir(parents=True, exist_ok=True)
        token_path.write_text(json.dumps(data, indent=2))

    def delete_token(self, identifier: Optional[str] = None) -> None:
        spotify_source_id = self._resolve_spotify_source_id(identifier)
        if spotify_source_id is None:
            raise HTTPException(status_code=400, detail="Spotify source not configured")
        source = self._get_spotify_source(spotify_source_id)
        token_path = Path(source["token_path"])
        if token_path.exists():
            token_path.unlink()

    @staticmethod
    def token_seconds_until_expiry(token: Optional[dict]) -> Optional[float]:
        if not token:
            return None
        expires_at = token.get("expires_at")
        if expires_at is not None:
            try:
                return float(expires_at) - time.time()
            except (TypeError, ValueError):
                return None
        expires_in = token.get("expires_in")
        if expires_in is not None:
            try:
                return float(expires_in)
            except (TypeError, ValueError):  # pragma: no cover - defensive
                return None
        return None
