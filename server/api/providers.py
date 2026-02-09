from __future__ import annotations

import os
from typing import Any, Callable, Dict, Optional

import httpx
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field


class ProviderInstallPayload(BaseModel):
    id: str = Field(..., min_length=1, max_length=40)


class ProviderUpdatePayload(BaseModel):
    enabled: Optional[bool] = None
    settings: Dict[str, Any] = Field(default_factory=dict)


def create_providers_router(
    *,
    available_providers: dict,
    get_provider_spec: Callable[[str], Any],
    providers: Callable[[], dict],
    provider_state_cls: Any,
    save_providers_state: Callable[[], None],
    require_admin: Callable,
    provider_instance_limit: Callable[[], int],
    provider_instance_count: Callable[[], int],
    spotify_provider: Any,
    apply_spotify_provider: Callable[[int], None],
    disable_spotify_provider: Callable[[], None],
    delete_spotify_tokens: Callable[[], None],
    delete_spotify_token: Callable[[str], None],
    apply_radio_provider: Callable[[], None],
    disable_radio_provider: Callable[[], None],
    apply_audiobookshelf_provider: Callable[[], None],
    disable_audiobookshelf_provider: Callable[[], None],
    refresh_channels: Callable[[], None],
) -> APIRouter:
    router = APIRouter(prefix="/api/providers")

    def _provider_instance_count_for(pid: str, state: Any) -> int:
        if not state or not getattr(state, "enabled", False):
            return 0
        settings = state.settings if isinstance(getattr(state, "settings", None), dict) else {}
        if pid == "spotify":
            raw = settings.get("instances")
            try:
                desired = int(raw)
            except (TypeError, ValueError):
                desired = 1
            return max(1, min(2, desired))
        if pid == "radio":
            return 1
        if pid == "audiobookshelf":
            return 1
        return 1

    def _public_provider_state(state: Any) -> dict:
        spec = get_provider_spec(state.id)
        return {
            "id": state.id,
            "name": spec.name if spec else state.id,
            "description": spec.description if spec else "",
            "enabled": bool(state.enabled),
            "settings": state.settings or {},
            "has_settings": bool(spec.has_settings) if spec else True,
        }

    def _extract_http_error_detail(resp: httpx.Response) -> str:
        try:
            payload = resp.json()
            if isinstance(payload, dict):
                for key in ("message", "error", "detail"):
                    value = payload.get(key)
                    if isinstance(value, str) and value.strip():
                        return value.strip()
        except Exception:
            pass
        text = (resp.text or "").strip()
        if text:
            return text[:220]
        return "Request rejected"

    async def _validate_audiobookshelf_connection(settings: Dict[str, Any]) -> None:
        base_url = str(settings.get("base_url") or "").strip().rstrip("/")
        token = str(settings.get("token") or "").strip()
        library_id = str(settings.get("library_id") or "").strip()

        if not base_url:
            raise HTTPException(status_code=400, detail="Audiobookshelf base URL is required")
        if not token:
            raise HTTPException(status_code=400, detail="Audiobookshelf API token is required")
        if not (base_url.startswith("http://") or base_url.startswith("https://")):
            raise HTTPException(status_code=400, detail="Audiobookshelf base URL must start with http:// or https://")

        timeout = float(os.getenv("AUDIOBOOKSHELF_HTTP_TIMEOUT", "12"))
        timeout = max(3.0, min(timeout, 60.0))
        endpoint = f"{base_url}/api/libraries"
        headers = {"Authorization": f"Bearer {token}"}

        try:
            async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
                resp = await client.get(endpoint, headers=headers)
        except httpx.RequestError as exc:
            raise HTTPException(
                status_code=502,
                detail=f"Audiobookshelf connection test failed: {exc}",
            ) from exc

        if resp.status_code >= 400:
            detail = _extract_http_error_detail(resp)
            raise HTTPException(
                status_code=502,
                detail=f"Audiobookshelf connection test failed ({resp.status_code}): {detail}",
            )

        try:
            payload = resp.json()
        except Exception as exc:
            raise HTTPException(
                status_code=502,
                detail="Audiobookshelf connection test failed: invalid JSON response",
            ) from exc

        libraries: list[dict] = []
        if isinstance(payload, list):
            libraries = [item for item in payload if isinstance(item, dict)]
        elif isinstance(payload, dict):
            raw = payload.get("libraries") if isinstance(payload.get("libraries"), list) else payload.get("results")
            if isinstance(raw, list):
                libraries = [item for item in raw if isinstance(item, dict)]

        if library_id:
            found = any(str(item.get("id") or "").strip() == library_id for item in libraries)
            if not found:
                raise HTTPException(
                    status_code=400,
                    detail="Audiobookshelf connection test failed: podcast library ID not found",
                )

    @router.get("/available")
    async def list_available_providers_api() -> dict:
        refresh_channels()
        items = []
        current = providers()
        for pid, spec in sorted(available_providers.items()):
            installed = pid in current
            enabled = bool(current.get(pid).enabled) if installed else False
            items.append(
                {
                    "id": spec.id,
                    "name": spec.name,
                    "description": spec.description,
                    "installed": installed,
                    "enabled": enabled,
                    "has_settings": spec.has_settings,
                }
            )
        return {"providers": items}

    @router.get("/installed")
    async def list_installed_providers_api() -> dict:
        refresh_channels()
        return {"providers": [_public_provider_state(state) for state in providers().values()]}

    @router.post("/install")
    async def install_provider_api(payload: ProviderInstallPayload, _: dict = Depends(require_admin)) -> dict:
        pid = (payload.id or "").strip().lower()
        spec = get_provider_spec(pid)
        if not spec:
            raise HTTPException(status_code=404, detail="Unknown provider")

        current = providers()
        existing = current.get(pid)
        previous_enabled = bool(existing.enabled) if existing else False
        previous_settings = dict(existing.settings or {}) if existing else {}
        previous_total = provider_instance_count()
        previous_count = _provider_instance_count_for(pid, existing)
        if existing:
            existing.enabled = True
            if existing.settings is None:
                existing.settings = {}
            state = existing
        else:
            defaults = {}
            if pid == "spotify":
                defaults = {"instances": 1}
            state = provider_state_cls(id=pid, enabled=True, settings=defaults)
            current[pid] = state
        desired_count = _provider_instance_count_for(pid, state)
        if previous_total - previous_count + desired_count > provider_instance_limit():
            if existing:
                existing.enabled = previous_enabled
                existing.settings = previous_settings
            else:
                current.pop(pid, None)
            save_providers_state()
            raise HTTPException(status_code=409, detail=f"Provider limit reached (max {provider_instance_limit()})")
        try:
            if pid == "spotify":
                instances = int((state.settings or {}).get("instances") or 1)
                instances = max(1, min(2, instances))
                state.settings = {**(state.settings or {}), "instances": instances}
                apply_spotify_provider(instances)
            elif pid == "radio":
                apply_radio_provider()
            elif pid == "audiobookshelf":
                apply_audiobookshelf_provider()
        except HTTPException:
            if existing:
                existing.enabled = previous_enabled
                existing.settings = previous_settings
            else:
                current.pop(pid, None)
            save_providers_state()
            raise

        save_providers_state()
        refresh_channels()
        return {"ok": True, "provider": _public_provider_state(state)}

    @router.patch("/{provider_id}")
    async def update_provider_api(provider_id: str, payload: ProviderUpdatePayload, _: dict = Depends(require_admin)) -> dict:
        pid = (provider_id or "").strip().lower()
        spec = get_provider_spec(pid)
        if not spec:
            raise HTTPException(status_code=404, detail="Unknown provider")

        current = providers()
        state = current.get(pid)
        if not state:
            raise HTTPException(status_code=404, detail="Provider not installed")

        previous_instances = int((state.settings or {}).get("instances") or 1) if pid == "spotify" else 0
        previous_enabled = bool(state.enabled)
        previous_settings = dict(state.settings or {})
        previous_total = provider_instance_count()
        previous_count = _provider_instance_count_for(pid, state)
        updates = payload.model_dump(exclude_unset=True)
        if "enabled" in updates and updates["enabled"] is not None:
            state.enabled = bool(updates["enabled"])
        if "settings" in updates and isinstance(updates["settings"], dict):
            merged = dict(state.settings or {})
            merged.update(updates["settings"])
            state.settings = merged
        desired_count = _provider_instance_count_for(pid, state)
        if previous_total - previous_count + desired_count > provider_instance_limit():
            state.enabled = previous_enabled
            state.settings = previous_settings
            save_providers_state()
            raise HTTPException(status_code=409, detail=f"Provider limit reached (max {provider_instance_limit()})")
        try:
            if pid == "spotify":
                if state.enabled:
                    instances = int((state.settings or {}).get("instances") or 1)
                    instances = max(1, min(2, instances))
                    state.settings = {**(state.settings or {}), "instances": instances}
                    apply_spotify_provider(instances)
                    if previous_instances >= 2 and instances < 2:
                        delete_spotify_token("spotify:b")
                else:
                    disable_spotify_provider()
            elif pid == "radio":
                if state.enabled:
                    apply_radio_provider()
                else:
                    disable_radio_provider()
            elif pid == "audiobookshelf":
                if state.enabled:
                    if "settings" in updates and isinstance(updates["settings"], dict):
                        await _validate_audiobookshelf_connection(state.settings or {})
                    apply_audiobookshelf_provider()
                else:
                    disable_audiobookshelf_provider()
        except HTTPException:
            state.enabled = previous_enabled
            state.settings = previous_settings
            save_providers_state()
            raise

        save_providers_state()
        refresh_channels()
        return {"ok": True, "provider": _public_provider_state(state)}

    @router.delete("/{provider_id}")
    async def remove_provider_api(provider_id: str, _: dict = Depends(require_admin)) -> dict:
        pid = (provider_id or "").strip().lower()
        spec = get_provider_spec(pid)
        if not spec:
            raise HTTPException(status_code=404, detail="Unknown provider")

        current = providers()
        state = current.get(pid)
        if not state:
            raise HTTPException(status_code=404, detail="Provider not installed")

        try:
            # Disable runtime and detach channels/sources as needed.
            if pid == "spotify":
                disable_spotify_provider()
                delete_spotify_tokens()
            elif pid == "radio":
                disable_radio_provider()
            elif pid == "audiobookshelf":
                disable_audiobookshelf_provider()
        except HTTPException:
            save_providers_state()
            raise

        current.pop(pid, None)
        save_providers_state()
        refresh_channels()
        return {"ok": True}

    return router
