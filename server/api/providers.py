from __future__ import annotations

from typing import Any, Callable, Dict, Optional

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
    spotify_provider: Any,
    apply_spotify_provider: Callable[[int], None],
    disable_spotify_provider: Callable[[], None],
    apply_radio_provider: Callable[[], None],
    disable_radio_provider: Callable[[], None],
    apply_audiobookshelf_provider: Callable[[], None],
    disable_audiobookshelf_provider: Callable[[], None],
) -> APIRouter:
    router = APIRouter(prefix="/api/providers")

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

    @router.get("/available")
    async def list_available_providers_api() -> dict:
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
        return {"providers": [_public_provider_state(state) for state in providers().values()]}

    @router.post("/install")
    async def install_provider_api(payload: ProviderInstallPayload, _: dict = Depends(require_admin)) -> dict:
        pid = (payload.id or "").strip().lower()
        spec = get_provider_spec(pid)
        if not spec:
            raise HTTPException(status_code=404, detail="Unknown provider")

        current = providers()
        existing = current.get(pid)
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

        save_providers_state()

        if pid == "spotify":
            instances = spotify_provider.normalize_instances((state.settings or {}).get("instances"), default=1)
            state.settings = {**(state.settings or {}), "instances": instances}
            save_providers_state()
            apply_spotify_provider(instances)
        elif pid == "radio":
            apply_radio_provider()
        elif pid == "audiobookshelf":
            apply_audiobookshelf_provider()

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

        updates = payload.model_dump(exclude_unset=True)
        if "enabled" in updates and updates["enabled"] is not None:
            state.enabled = bool(updates["enabled"])
        if "settings" in updates and isinstance(updates["settings"], dict):
            merged = dict(state.settings or {})
            merged.update(updates["settings"])
            state.settings = merged
        save_providers_state()

        if pid == "spotify":
            if state.enabled:
                instances = spotify_provider.normalize_instances((state.settings or {}).get("instances"), default=1)
                state.settings = {**(state.settings or {}), "instances": instances}
                save_providers_state()
                apply_spotify_provider(instances)
            else:
                disable_spotify_provider()
        elif pid == "radio":
            if state.enabled:
                apply_radio_provider()
            else:
                disable_radio_provider()
        elif pid == "audiobookshelf":
            if state.enabled:
                apply_audiobookshelf_provider()
            else:
                disable_audiobookshelf_provider()

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

        # Disable runtime and detach channels/sources as needed.
        if pid == "spotify":
            disable_spotify_provider()
        elif pid == "radio":
            disable_radio_provider()
        elif pid == "audiobookshelf":
            disable_audiobookshelf_provider()

        current.pop(pid, None)
        save_providers_state()
        return {"ok": True}

    return router
