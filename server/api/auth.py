from __future__ import annotations

from typing import Callable, Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field


class InitializePayload(BaseModel):
    server_name: str = Field(default="RoomCast", min_length=1, max_length=120)
    username: str = Field(min_length=1, max_length=60)
    password: str = Field(min_length=4, max_length=128)


class LoginPayload(BaseModel):
    username: str = Field(min_length=1, max_length=60)
    password: str = Field(min_length=1, max_length=128)


class CreateUserPayload(BaseModel):
    username: str = Field(min_length=1, max_length=60)
    password: str = Field(min_length=4, max_length=128)
    role: str = Field(pattern="^(admin|member)$")


class UpdateUserPayload(BaseModel):
    username: Optional[str] = Field(default=None, min_length=1, max_length=60)
    password: Optional[str] = Field(default=None, min_length=4, max_length=128)
    role: Optional[str] = Field(default=None, pattern="^(admin|member)$")


class ServerNamePayload(BaseModel):
    server_name: str = Field(min_length=1, max_length=120)


class UserSettingsPayload(BaseModel):
    browser_node_enabled: Optional[bool] = None


def create_auth_router(
    *,
    is_initialized: Callable[[], bool],
    get_server_name: Callable[[], str],
    set_server_name: Callable[[str], None],
    public_user: Callable[[dict], dict],
    get_user_by_username: Callable[[str], Optional[dict]],
    create_user: Callable[[str, str, str], dict],
    update_user: Callable[..., dict],
    delete_user: Callable[[str], None],
    list_users: Callable[[], list[dict]],
    verify_password: Callable[[str, str], bool],
    set_session_cookie: Callable[[JSONResponse, str], None],
    clear_session_cookie: Callable[[JSONResponse], None],
    require_user: Callable,
    require_admin: Callable,
    server_default_name: str,
    update_user_settings: Callable[[str, dict], dict],
) -> APIRouter:
    router = APIRouter()

    @router.get("/api/auth/status")
    async def auth_status(request: Request) -> dict:
        user = getattr(request.state, "user", None)
        public = public_user(user) if user else None
        if public and isinstance(user, dict):
            settings = user.get("settings")
            if isinstance(settings, dict):
                public["settings"] = settings
        return {
            "initialized": is_initialized(),
            "server_name": get_server_name() or server_default_name,
            "authenticated": bool(user),
            "user": public,
        }

    @router.post("/api/auth/initialize")
    async def initialize_instance(payload: InitializePayload) -> JSONResponse:
        if is_initialized():
            raise HTTPException(status_code=409, detail="Instance already initialized")
        server_name = payload.server_name.strip() or server_default_name
        set_server_name(server_name)
        user = create_user(payload.username, payload.password, "admin")
        response = JSONResponse(
            {
                "ok": True,
                "server_name": server_name,
                "user": public_user(user),
            }
        )
        set_session_cookie(response, user["id"])
        return response

    @router.post("/api/auth/login")
    async def login(payload: LoginPayload) -> JSONResponse:
        if not is_initialized():
            raise HTTPException(status_code=403, detail="Instance setup required")
        user = get_user_by_username(payload.username.strip().lower()) if payload.username else None
        if not user or not verify_password(payload.password, user.get("password_hash")):
            raise HTTPException(status_code=401, detail="Invalid credentials")
        response = JSONResponse({"ok": True, "user": public_user(user)})
        set_session_cookie(response, user["id"])
        return response

    @router.post("/api/auth/logout")
    async def logout() -> JSONResponse:
        response = JSONResponse({"ok": True})
        clear_session_cookie(response)
        return response

    @router.get("/api/users")
    async def list_users_api(_: dict = Depends(require_admin)) -> dict:
        return {"users": [public_user(user) for user in list_users()]}

    @router.post("/api/users")
    async def create_user_api(payload: CreateUserPayload, _: dict = Depends(require_admin)) -> dict:
        user = create_user(payload.username, payload.password, payload.role)
        return {"ok": True, "user": public_user(user)}

    @router.patch("/api/users/{user_id}")
    async def update_user_api(user_id: str, payload: UpdateUserPayload, _: dict = Depends(require_admin)) -> dict:
        updates = payload.model_dump(exclude_unset=True)
        user = update_user(
            user_id,
            username=updates.get("username"),
            password=updates.get("password"),
            role=updates.get("role"),
        )
        return {"ok": True, "user": public_user(user)}

    @router.delete("/api/users/{user_id}")
    async def delete_user_api(user_id: str, request: Request, _: dict = Depends(require_admin)) -> JSONResponse:
        current_user = getattr(request.state, "user", None)
        deleting_self = current_user and current_user.get("id") == user_id
        delete_user(user_id)
        response = JSONResponse({"ok": True, "removed": user_id, "self_removed": bool(deleting_self)})
        if deleting_self:
            clear_session_cookie(response)
        return response

    @router.post("/api/server/name")
    async def update_server_name_api(payload: ServerNamePayload, _: dict = Depends(require_admin)) -> dict:
        name = payload.server_name.strip() or server_default_name
        set_server_name(name)
        return {"ok": True, "server_name": name}

    @router.get("/api/users/me/settings")
    async def get_user_settings(request: Request, _: dict = Depends(require_user)) -> dict:
        user = getattr(request.state, "user", None)
        settings = user.get("settings") if isinstance(user, dict) else {}
        if not isinstance(settings, dict):
            settings = {}
        return {"settings": settings}

    @router.patch("/api/users/me/settings")
    async def update_user_settings_api(
        payload: UserSettingsPayload,
        request: Request,
        _: dict = Depends(require_user),
    ) -> dict:
        user = getattr(request.state, "user", None)
        if not user:
            raise HTTPException(status_code=401, detail="Authentication required")
        updates = payload.model_dump(exclude_unset=True)
        updated = update_user_settings(user["id"], updates)
        settings = updated.get("settings") if isinstance(updated, dict) else {}
        if not isinstance(settings, dict):
            settings = {}
        return {"ok": True, "settings": settings}

    return router
