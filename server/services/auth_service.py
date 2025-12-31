from __future__ import annotations

import json
import time
import uuid
from pathlib import Path
from typing import Optional

import bcrypt
from fastapi import HTTPException, WebSocket
from fastapi.responses import Response
from itsdangerous import BadSignature, URLSafeTimedSerializer


class AuthService:
    def __init__(
        self,
        *,
        users_path: Path,
        server_default_name: str,
        session_signer: URLSafeTimedSerializer,
        session_cookie_name: str,
        session_cookie_secure: bool,
        session_cookie_samesite: str,
        session_max_age: int,
    ) -> None:
        self._users_path = users_path
        self._server_default_name = server_default_name
        self._session_signer = session_signer
        self._session_cookie_name = session_cookie_name
        self._session_cookie_secure = bool(session_cookie_secure)
        self._session_cookie_samesite = session_cookie_samesite
        self._session_max_age = int(session_max_age)

        self._auth_state: dict = {"server_name": server_default_name, "users": []}
        self._users_by_id: dict[str, dict] = {}
        self._users_by_username: dict[str, dict] = {}
        self.load()

    @property
    def auth_state(self) -> dict:
        return self._auth_state

    def load(self) -> None:
        if self._users_path.exists():
            try:
                data = json.loads(self._users_path.read_text())
            except Exception:
                data = {}
        else:
            data = {}

        server_name = (data.get("server_name") or self._server_default_name).strip() or self._server_default_name
        users = data.get("users") or []
        self._auth_state = {"server_name": server_name, "users": []}
        self._users_by_id = {}
        self._users_by_username = {}

        for entry in users:
            if not isinstance(entry, dict):
                continue
            uid = entry.get("id") or str(uuid.uuid4())
            username = (entry.get("username") or "").strip()
            role = entry.get("role") or "member"
            password_hash = entry.get("password_hash")
            if not username or not password_hash:
                continue
            user = {
                "id": uid,
                "username": username,
                "role": role,
                "password_hash": password_hash,
                "created_at": entry.get("created_at") or int(time.time()),
                "updated_at": entry.get("updated_at") or int(time.time()),
            }
            self._users_by_id[uid] = user
            self._users_by_username[username.lower()] = user

        self._auth_state["users"] = list(self._users_by_id.values())

    def save(self) -> None:
        data = {
            "server_name": self._auth_state.get("server_name", self._server_default_name),
            "users": list(self._users_by_id.values()),
        }
        self._users_path.write_text(json.dumps(data, indent=2, sort_keys=True))

    def is_initialized(self) -> bool:
        return bool(self._users_by_id)

    def get_server_name(self) -> str:
        return self._auth_state.get("server_name", self._server_default_name)

    def set_server_name(self, value: str) -> None:
        self._auth_state["server_name"] = (value or "").strip() or self._server_default_name
        self.save()

    @staticmethod
    def public_user(user: dict) -> dict:
        return {
            "id": user.get("id"),
            "username": user.get("username"),
            "role": user.get("role"),
            "created_at": user.get("created_at"),
            "updated_at": user.get("updated_at"),
        }

    def get_user_by_username(self, lowered: str) -> Optional[dict]:
        return self._users_by_username.get((lowered or "").strip().lower())

    def list_users(self) -> list[dict]:
        return list(self._users_by_id.values())

    @staticmethod
    def _hash_password(raw: str) -> str:
        value = raw.encode("utf-8")
        hashed = bcrypt.hashpw(value, bcrypt.gensalt())
        return hashed.decode("utf-8")

    @staticmethod
    def verify_password(raw: str, hashed: str) -> bool:
        if not raw or not hashed:
            return False
        try:
            return bcrypt.checkpw(raw.encode("utf-8"), hashed.encode("utf-8"))
        except ValueError:
            return False

    def create_user(self, username: str, password: str, role: str = "admin") -> dict:
        normalized = username.strip()
        if not normalized:
            raise HTTPException(status_code=400, detail="Username is required")
        lowered = normalized.lower()
        if lowered in self._users_by_username:
            raise HTTPException(status_code=409, detail="Username already exists")
        if not password or len(password) < 4:
            raise HTTPException(status_code=400, detail="Password must be at least 4 characters")
        role_normalized = role if role in {"admin", "member"} else "member"
        now = int(time.time())
        user = {
            "id": str(uuid.uuid4()),
            "username": normalized,
            "role": role_normalized,
            "password_hash": self._hash_password(password),
            "created_at": now,
            "updated_at": now,
        }
        self._users_by_id[user["id"]] = user
        self._users_by_username[lowered] = user
        self._auth_state["users"] = list(self._users_by_id.values())
        self.save()
        return user

    def update_user(
        self,
        user_id: str,
        *,
        username: Optional[str] = None,
        password: Optional[str] = None,
        role: Optional[str] = None,
    ) -> dict:
        user = self._users_by_id.get(user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        if username:
            normalized = username.strip()
            if not normalized:
                raise HTTPException(status_code=400, detail="Username must not be empty")
            lowered = normalized.lower()
            existing = self._users_by_username.get(lowered)
            if existing and existing["id"] != user_id:
                raise HTTPException(status_code=409, detail="Username already exists")
            self._users_by_username.pop((user.get("username") or "").lower(), None)
            user["username"] = normalized
            self._users_by_username[lowered] = user
        if password:
            if len(password) < 4:
                raise HTTPException(status_code=400, detail="Password must be at least 4 characters")
            user["password_hash"] = self._hash_password(password)
        if role:
            if role not in {"admin", "member"}:
                raise HTTPException(status_code=400, detail="Invalid role")
            if user.get("role") == "admin" and role == "member":
                admins = [
                    u
                    for u in self._users_by_id.values()
                    if u.get("role") == "admin" and u.get("id") != user_id
                ]
                if not admins:
                    raise HTTPException(status_code=400, detail="Cannot remove the last admin")
            user["role"] = role
        user["updated_at"] = int(time.time())
        self._auth_state["users"] = list(self._users_by_id.values())
        self.save()
        return user

    def delete_user(self, user_id: str) -> None:
        user = self._users_by_id.get(user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        if user.get("role") == "admin":
            admins = [
                u
                for u in self._users_by_id.values()
                if u.get("role") == "admin" and u.get("id") != user_id
            ]
            if not admins:
                raise HTTPException(status_code=400, detail="Cannot remove the last admin")
        self._users_by_id.pop(user_id, None)
        self._users_by_username.pop((user.get("username") or "").lower(), None)
        self._auth_state["users"] = list(self._users_by_id.values())
        self.save()

    def encode_session(self, user_id: str) -> str:
        payload = {"user_id": user_id, "ts": int(time.time())}
        return self._session_signer.dumps(payload)

    def decode_session(self, token: str) -> Optional[dict]:
        if not token:
            return None
        try:
            return self._session_signer.loads(token, max_age=self._session_max_age)
        except BadSignature:
            return None

    def resolve_session_user(self, token: Optional[str]) -> Optional[dict]:
        data = self.decode_session(token or "")
        if not data:
            return None
        user_id = data.get("user_id")
        if not user_id:
            return None
        return self._users_by_id.get(user_id)

    def set_session_cookie(self, response: Response, user_id: str) -> None:
        token = self.encode_session(user_id)
        response.set_cookie(
            self._session_cookie_name,
            token,
            max_age=self._session_max_age,
            httponly=True,
            secure=self._session_cookie_secure,
            samesite=self._session_cookie_samesite,
            path="/",
        )

    def clear_session_cookie(self, response: Response) -> None:
        response.delete_cookie(self._session_cookie_name, path="/")

    async def require_ws_user(self, ws: WebSocket) -> Optional[dict]:
        token = ws.cookies.get(self._session_cookie_name)
        user = self.resolve_session_user(token)
        if not user:
            await ws.close(code=4401)
            return None
        return user
