from __future__ import annotations

import asyncio
import json
import os
import secrets
import time
import uuid
from pathlib import Path
from typing import Any, AsyncIterator, Awaitable, Callable, Dict, Literal, Optional

import asyncssh
from fastapi import APIRouter, Body, Depends, HTTPException, Request, WebSocket
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field


class NodeRegistration(BaseModel):
    id: Optional[str] = None
    name: str
    url: str = Field(min_length=1)
    fingerprint: Optional[str] = None


class RenameNodePayload(BaseModel):
    name: str = Field(min_length=1, max_length=100)


class VolumePayload(BaseModel):
    percent: int = Field(ge=0, le=100)


class EqBand(BaseModel):
    freq: float = Field(gt=10, lt=24000, description="Frequency in Hz")
    gain: float = Field(ge=-24, le=24, description="Gain in dB")
    q: float = Field(default=1.0, gt=0.05, lt=36, description="Quality factor")


class EqPayload(BaseModel):
    preset: Optional[str] = None
    bands: list[EqBand] = Field(default_factory=list)
    band_count: int = Field(default=15, ge=1, le=31)


class OutputSelectionPayload(BaseModel):
    device: str = Field(min_length=1)


class WebNodeOffer(BaseModel):
    name: str = Field(default="Web node", min_length=1)
    sdp: str
    type: str


class WebNodeRequestDecisionPayload(BaseModel):
    action: Literal["approve", "deny"]
    reason: Optional[str] = Field(default=None, max_length=200)


class NodeChannelPayload(BaseModel):
    channel_id: Optional[str] = Field(default=None, max_length=120)


class NodeStereoModePayload(BaseModel):
    mode: Literal["both", "left", "right"] = Field(default="both")


class SonosEqPayload(BaseModel):
    bass: Optional[int] = Field(default=None, ge=-10, le=10, description="Bass (-10..10)")
    treble: Optional[int] = Field(default=None, ge=-10, le=10, description="Treble (-10..10)")
    loudness: Optional[bool] = Field(default=None, description="Loudness on/off")


class CreateSectionPayload(BaseModel):
    name: str


class UpdateSectionPayload(BaseModel):
    name: str


class ReorderSectionsPayload(BaseModel):
    section_ids: list[str]


class SetNodeSectionPayload(BaseModel):
    section_id: Optional[str] = None


class ReorderNodeSectionPayload(BaseModel):
    section_id: Optional[str] = None
    node_ids: list[str]


class ReorderNodesPayload(BaseModel):
    sections: list[ReorderNodeSectionPayload]


def create_nodes_router(
    *,
    nodes: Callable[[], dict],
    sections: Callable[[], list],
    set_sections: Callable[[list], None],
    save_nodes: Callable[[], None],
    broadcast_nodes: Callable[[], Awaitable[None]],
    public_node: Callable[[dict], dict],
    public_nodes: Callable[[], list],
    public_nodes_for_user: Callable[[Optional[dict]], list],
    public_sections: Callable[[], list],
    get_current_user: Callable[[Request], dict],
    require_admin: Callable,
    require_ws_user: Callable[[WebSocket], Awaitable[Optional[dict]]],
    auth_state: Callable[[], dict],
    server_default_name: str,
    local_agent_name_suffix: str,
    local_agent_url: Callable[[], str],
    ensure_local_agent_running: Callable[[], Awaitable[None]],
    stop_local_agent: Callable[[], Awaitable[None]],
    controller_node: Callable[[], Optional[dict]],
    refresh_agent_metadata: Callable[..., Awaitable[tuple[bool, bool]]],
    register_node_payload: Callable[..., Awaitable[dict]],
    call_agent: Callable[[dict, str, dict], Awaitable[dict]],
    sync_node_max_volume: Callable[..., Awaitable[None]],
    get_node_max_volume: Callable[[dict], int],
    send_browser_volume: Callable[[dict, int], Awaitable[None]],
    browser_ws: Callable[[], dict],
    node_watchers: Callable[[], set],
    normalize_percent: Callable[..., int],
    normalize_stereo_mode: Callable[[Any], str],
    apply_volume_limit: Callable[[dict, int], int],
    sonos_ip_from_url: Callable[[Any], Optional[str]],
    sonos_set_volume: Callable[..., Awaitable[None]],
    sonos_set_mute: Callable[..., Awaitable[None]],
    sonos_set_eq: Callable[..., Awaitable[None]],
    normalize_sonos_eq: Callable[[Any], dict],
    request_agent_secret: Callable[..., Awaitable[str]],
    configure_agent_audio: Callable[..., Awaitable[dict]],
    set_node_channel: Callable[[dict, Optional[str]], Awaitable[None]],
    schedule_agent_refresh: Callable[..., None],
    schedule_restart_watch: Callable[[str], None],
    node_restart_timeout: int,
    resolve_terminal_target: Callable[[dict], Optional[dict]],
    cleanup_terminal_sessions: Callable[[], None],
    terminal_sessions: Callable[[], Dict[str, dict]],
    node_terminal_enabled: bool,
    node_terminal_token_ttl: int,
    node_terminal_max_duration: int,
    node_terminal_strict_host_key: bool,
    cancel_node_rediscovery: Callable[[Optional[str]], None],
    teardown_browser_node: Callable[..., Awaitable[None]],
    get_webrtc_relay: Callable[[], Any],
    # Sections helpers
    normalize_section_name: Callable[[Any], str],
    find_section: Callable[[str], Optional[dict]],
    public_section: Callable[[dict], dict],
    # Web node helpers
    webrtc_enabled: bool,
    get_web_node_snapshot: Callable[[dict], dict],
    broadcast_web_node_request_event: Callable[..., Awaitable[None]],
    pending_web_node_requests: Callable[[], Dict[str, dict]],
    pending_web_node_snapshots: Callable[[], list],
    pop_pending_web_node_request: Callable[[str], dict],
    establish_web_node_session_for_request: Callable[[dict], Awaitable[dict]],
    establish_private_web_node_session: Callable[[dict, str, str, str], Awaitable[dict]],
    remove_private_web_node: Callable[[str], Awaitable[Optional[dict]]],
    web_node_approval_timeout: int,
    # Node discovery helpers
    detect_discovery_networks: Callable[[], list[str]],
    hosts_for_networks: Callable[..., list[str]],
    stream_host_probes: Callable[[list[str]], AsyncIterator[dict]],
    sonos_ssdp_discover: Callable[[], Awaitable[list[dict]]],
    discovery_max_hosts: int,
    sonos_discovery_timeout: float,
) -> APIRouter:
    router = APIRouter()

    @router.post("/api/nodes/register")
    async def register_node(reg: NodeRegistration) -> dict:
        return await register_node_payload(reg)

    @router.post("/api/nodes/register-controller")
    async def register_controller_node(_: dict = Depends(require_admin)) -> dict:
        existing = controller_node()
        if existing:
            if existing.get("online") is False:
                try:
                    await ensure_local_agent_running()
                    await refresh_agent_metadata(existing)
                except Exception:
                    pass
            return public_node(existing)
        try:
            await ensure_local_agent_running()
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"Failed to start local agent: {exc}") from exc
        name = f"{auth_state().get('server_name') or server_default_name}{local_agent_name_suffix}"
        reg = NodeRegistration(name=name, url=local_agent_url())
        try:
            return await register_node_payload(reg, mark_controller=True)
        except HTTPException:
            raise
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc

    @router.post("/api/nodes/{node_id}/volume")
    async def set_node_volume(node_id: str, payload: VolumePayload) -> dict:
        node = nodes().get(node_id)
        if not node:
            raise HTTPException(status_code=404, detail="Unknown node")
        requested = normalize_percent(payload.percent, default=node.get("volume_percent", 75))
        if node.get("type") == "browser":
            ws = browser_ws().get(node_id)
            if not ws:
                raise HTTPException(status_code=503, detail="Browser node not connected")
            await send_browser_volume(node, requested)
            result = {"sent": True}
        elif node.get("type") == "sonos":
            ip = sonos_ip_from_url(node.get("url"))
            if not ip:
                raise HTTPException(status_code=400, detail="Invalid Sonos node")
            effective = apply_volume_limit(node, requested)
            await sonos_set_volume(ip, effective)
            result = {"sonos": True, "effective": effective}
        else:
            result = await call_agent(node, "/volume", {"percent": requested})
        node["volume_percent"] = requested
        save_nodes()
        await broadcast_nodes()
        return {"ok": True, "result": result}

    @router.post("/api/nodes/{node_id}/max-volume")
    async def set_node_max_volume(node_id: str, payload: VolumePayload) -> dict:
        node = nodes().get(node_id)
        if not node:
            raise HTTPException(status_code=404, detail="Unknown node")
        percent = normalize_percent(payload.percent, default=get_node_max_volume(node))
        if node.get("type") == "agent":
            await sync_node_max_volume(node, percent=percent)
        node["max_volume_percent"] = percent
        save_nodes()
        if node.get("type") == "browser":
            await send_browser_volume(node, node.get("volume_percent", 75))
        if node.get("type") == "sonos":
            ip = sonos_ip_from_url(node.get("url"))
            if ip:
                try:
                    effective = apply_volume_limit(node, int(node.get("volume_percent", 75)))
                    await sonos_set_volume(ip, effective)
                except Exception:
                    pass
        await broadcast_nodes()
        return {"ok": True, "max_volume_percent": percent}

    @router.post("/api/nodes/{node_id}/stereo")
    async def set_node_stereo_mode(node_id: str, payload: NodeStereoModePayload) -> dict:
        node = nodes().get(node_id)
        if not node:
            raise HTTPException(status_code=404, detail="Unknown node")
        mode = normalize_stereo_mode(payload.mode)
        if mode not in {"both", "left", "right"}:
            raise HTTPException(status_code=400, detail="Invalid stereo mode")

        if node.get("type") == "agent":
            try:
                await call_agent(node, "/stereo", {"mode": mode})
            except HTTPException:
                raise
            except Exception as exc:
                raise HTTPException(status_code=502, detail=f"Failed to apply stereo mode: {exc}") from exc
        elif node.get("type") == "browser":
            relay = get_webrtc_relay()
            if relay:
                try:
                    await relay.set_stereo_mode(node_id, mode)
                except Exception:
                    pass
        elif node.get("type") == "sonos":
            pass
        else:
            raise HTTPException(status_code=400, detail="Unknown node type")
        node["stereo_mode"] = mode
        save_nodes()
        await broadcast_nodes()
        return {"ok": True, "stereo_mode": mode}

    @router.post("/api/nodes/{node_id}/sonos-eq")
    async def set_sonos_eq(node_id: str, payload: SonosEqPayload) -> dict:
        node = nodes().get(node_id)
        if not node:
            raise HTTPException(status_code=404, detail="Unknown node")
        if node.get("type") != "sonos":
            raise HTTPException(status_code=400, detail="Sonos EQ only applies to Sonos nodes")
        ip = sonos_ip_from_url(node.get("url"))
        if not ip:
            raise HTTPException(status_code=400, detail="Invalid Sonos node")

        current = normalize_sonos_eq(node.get("sonos_eq"))
        desired = dict(current)
        if payload.bass is not None:
            desired["bass"] = int(payload.bass)
        if payload.treble is not None:
            desired["treble"] = int(payload.treble)
        if payload.loudness is not None:
            desired["loudness"] = bool(payload.loudness)
        desired = normalize_sonos_eq(desired)

        await sonos_set_eq(ip, eq_type="Bass", value=int(desired["bass"]))
        await sonos_set_eq(ip, eq_type="Treble", value=int(desired["treble"]))
        await sonos_set_eq(ip, eq_type="Loudness", value=1 if desired["loudness"] else 0)

        node["sonos_eq"] = desired
        save_nodes()
        await broadcast_nodes()
        return {"ok": True, "sonos_eq": desired}

    @router.post("/api/nodes/{node_id}/mute")
    async def set_node_mute(node_id: str, payload: dict = Body(...)) -> dict:
        node = nodes().get(node_id)
        if not node:
            raise HTTPException(status_code=404, detail="Unknown node")
        muted = payload.get("muted") if payload else None
        if muted is None:
            raise HTTPException(status_code=400, detail="Missing muted")
        if node.get("type") == "browser":
            ws = browser_ws().get(node_id)
            if not ws:
                raise HTTPException(status_code=503, detail="Browser node not connected")
            await ws.send_json({"type": "mute", "muted": bool(muted)})
            result = {"sent": True}
        elif node.get("type") == "sonos":
            ip = sonos_ip_from_url(node.get("url"))
            if not ip:
                raise HTTPException(status_code=400, detail="Invalid Sonos node")
            await sonos_set_mute(ip, bool(muted))
            result = {"sonos": True}
        else:
            result = await call_agent(node, "/mute", {"muted": bool(muted)})
        node["muted"] = bool(muted)
        save_nodes()
        await broadcast_nodes()
        return {"ok": True, "result": result}

    @router.post("/api/nodes/{node_id}/eq")
    async def set_node_eq(node_id: str, payload: EqPayload) -> dict:
        node = nodes().get(node_id)
        if not node:
            raise HTTPException(status_code=404, detail="Unknown node")
        eq_bands = [band.model_dump() for band in payload.bands]
        active_bands = 0
        for band in eq_bands:
            try:
                gain = float(band.get("gain", 0))
            except (TypeError, ValueError):
                gain = 0.0
            if abs(gain) >= 0.1:
                active_bands += 1
        max_bands_raw = node.get("eq_max_bands")
        try:
            max_bands = int(max_bands_raw)
        except (TypeError, ValueError):
            max_bands = None
        if max_bands is not None:
            max_bands = max(1, min(31, max_bands))
            if active_bands > max_bands:
                raise HTTPException(
                    status_code=409,
                    detail=(
                        f"EQ band limit exceeded ({active_bands} active, max {max_bands}). "
                        "Reset an active band before enabling more."
                    ),
                )
        eq_data = payload.model_dump()
        eq_data["bands"] = eq_bands
        node["eq"] = eq_data
        node["eq_active_bands"] = active_bands
        save_nodes()
        if node.get("type") == "browser":
            ws = browser_ws().get(node_id)
            if not ws:
                raise HTTPException(status_code=503, detail="Browser node not connected")
            await ws.send_json({"type": "eq", "eq": eq_data})
            result = {"sent": True}
        elif node.get("type") == "sonos":
            result = {"sonos_stream": True}
        else:
            result = await call_agent(node, "/eq", eq_data)
        await broadcast_nodes()
        return {"ok": True, "result": result}

    @router.post("/api/nodes/{node_id}/pair")
    async def pair_node(node_id: str, payload: dict | None = Body(default=None)) -> dict:
        node = nodes().get(node_id)
        if not node:
            raise HTTPException(status_code=404, detail="Unknown node")
        if node.get("type") == "browser":
            raise HTTPException(status_code=400, detail="Browser nodes do not support pairing")
        if node.get("type") == "sonos":
            raise HTTPException(status_code=400, detail="Sonos nodes do not support pairing")
        force = True if payload is None else bool(payload.get("force", False))
        recovery_code = None if payload is None else payload.get("recovery_code")
        secret = await request_agent_secret(node, force=force, recovery_code=recovery_code)
        node["agent_secret"] = secret
        await configure_agent_audio(node)
        save_nodes()
        await broadcast_nodes()
        return {"ok": True}

    @router.post("/api/nodes/{node_id}/rename")
    async def rename_node(node_id: str, payload: RenameNodePayload) -> dict:
        node = nodes().get(node_id)
        if not node:
            raise HTTPException(status_code=404, detail="Unknown node")
        new_name = payload.name.strip()
        if not new_name:
            raise HTTPException(status_code=400, detail="Name must not be empty")
        node["name"] = new_name
        node["name_is_custom"] = True
        save_nodes()
        await broadcast_nodes()
        return {"ok": True, "name": new_name}

    @router.post("/api/nodes/{node_id}/channel")
    async def update_node_channel(node_id: str, payload: NodeChannelPayload) -> dict:
        node = nodes().get(node_id)
        if not node:
            raise HTTPException(status_code=404, detail="Unknown node")
        await set_node_channel(node, payload.channel_id)
        save_nodes()
        await broadcast_nodes()
        return {"ok": True, "channel_id": node.get("channel_id")}

    @router.post("/api/nodes/{node_id}/configure")
    async def configure_node(node_id: str) -> dict:
        node = nodes().get(node_id)
        if not node:
            raise HTTPException(status_code=404, detail="Unknown node")
        if node.get("type") == "browser":
            raise HTTPException(status_code=400, detail="Browser nodes do not require configuration")
        if node.get("type") == "sonos":
            raise HTTPException(status_code=400, detail="Sonos nodes do not require configuration")
        result = await configure_agent_audio(node)
        await sync_node_max_volume(node)
        await broadcast_nodes()
        return {"ok": True, "result": result}

    @router.post("/api/nodes/{node_id}/outputs")
    async def set_node_output(node_id: str, payload: OutputSelectionPayload) -> dict:
        node = nodes().get(node_id)
        if not node:
            raise HTTPException(status_code=404, detail="Unknown node")
        if node.get("type") == "browser":
            raise HTTPException(status_code=400, detail="Browser nodes do not support hardware outputs")
        if node.get("type") == "sonos":
            raise HTTPException(status_code=400, detail="Sonos nodes do not support hardware outputs")
        result = await call_agent(node, "/outputs", payload.model_dump())
        outputs = result.get("outputs") if isinstance(result, dict) else None
        if isinstance(outputs, dict):
            node["outputs"] = outputs
            selected = outputs.get("selected")
            if isinstance(selected, str) and selected:
                node["playback_device"] = selected
        else:
            node["playback_device"] = payload.device
        save_nodes()
        await broadcast_nodes()
        return {"ok": True, "result": result}

    @router.post("/api/nodes/{node_id}/check-updates")
    async def check_node_updates(node_id: str) -> dict:
        node = nodes().get(node_id)
        if not node:
            raise HTTPException(status_code=404, detail="Unknown node")
        if node.get("type") == "browser":
            raise HTTPException(status_code=400, detail="Browser nodes do not support updates")
        if node.get("type") == "sonos":
            raise HTTPException(status_code=400, detail="Sonos nodes do not support updates")
        reachable, changed = await refresh_agent_metadata(node)
        if not reachable:
            raise HTTPException(status_code=504, detail="Node agent is not responding")
        await broadcast_nodes()
        public = public_node(node)
        return {
            "ok": True,
            "agent_version": public.get("agent_version"),
            "update_available": public.get("update_available"),
            "changed": changed,
        }

    @router.post("/api/nodes/{node_id}/update")
    async def update_agent_node(node_id: str) -> dict:
        node = nodes().get(node_id)
        if not node:
            raise HTTPException(status_code=404, detail="Unknown node")
        if node.get("type") == "browser":
            raise HTTPException(status_code=400, detail="Browser nodes cannot be updated from the controller")
        if node.get("type") == "sonos":
            raise HTTPException(status_code=400, detail="Sonos nodes cannot be updated from the controller")
        result = await call_agent(node, "/update", {})
        node["updating"] = True
        node["audio_configured"] = False
        node["_needs_reconfig"] = True
        save_nodes()
        await broadcast_nodes()
        schedule_agent_refresh(node_id, delay=20.0, repeat=True, attempts=12)
        return {"ok": True, "result": result}

    @router.post("/api/nodes/{node_id}/restart")
    async def restart_agent_node(node_id: str) -> dict:
        node = nodes().get(node_id)
        if not node:
            raise HTTPException(status_code=404, detail="Unknown node")
        if node.get("type") == "browser":
            raise HTTPException(status_code=400, detail="Browser nodes cannot be restarted from the controller")
        if node.get("type") == "sonos":
            raise HTTPException(status_code=400, detail="Sonos nodes cannot be restarted from the controller")
        result = await call_agent(node, "/restart", {})
        schedule_restart_watch(node_id)
        await broadcast_nodes()
        return {"ok": True, "result": result, "timeout": node_restart_timeout}

    @router.post("/api/nodes/{node_id}/terminal-session")
    async def create_terminal_session(node_id: str) -> dict:
        if not node_terminal_enabled:
            raise HTTPException(status_code=503, detail="Terminal access is disabled")
        node = nodes().get(node_id)
        if not node:
            raise HTTPException(status_code=404, detail="Unknown node")
        if node.get("type") == "browser":
            raise HTTPException(status_code=400, detail="Browser nodes do not support terminal access")
        if node.get("type") == "sonos":
            raise HTTPException(status_code=400, detail="Sonos nodes do not support terminal access")
        if node.get("online") is False:
            raise HTTPException(status_code=409, detail="Node is offline")
        target = resolve_terminal_target(node)
        if not target:
            raise HTTPException(status_code=400, detail="Terminal credentials are not configured")
        cleanup_terminal_sessions()
        token = secrets.token_urlsafe(32)
        now = time.time()
        expires_at = now + max(5, node_terminal_token_ttl)
        deadline = now + max(30, node_terminal_max_duration)
        terminal_sessions()[token] = {
            "node_id": node_id,
            "target": target,
            "created_at": now,
            "expires_at": expires_at,
            "deadline": deadline,
        }
        return {
            "token": token,
            "expires_at": expires_at,
            "ws_path": f"/ws/terminal/{token}",
            "page_url": f"/static/terminal.html?token={token}",
        }

    @router.delete("/api/nodes/{node_id}")
    async def unregister_node(node_id: str) -> dict:
        node = nodes().pop(node_id, None)
        if not node:
            raise HTTPException(status_code=404, detail="Unknown node")
        cancel_node_rediscovery(node_id)
        save_nodes()
        await broadcast_nodes()
        if node.get("is_controller"):
            await stop_local_agent()
        if node.get("type") == "browser":
            relay = get_webrtc_relay()
            if relay:
                await relay.drop_session(node_id)
            else:
                await teardown_browser_node(node_id, remove_entry=False)
        return {"ok": True, "removed": node_id}

    @router.get("/api/nodes")
    async def list_nodes(request: Request) -> dict:
        user = get_current_user(request)
        return {"sections": public_sections(), "nodes": public_nodes_for_user(user)}

    @router.post("/api/sections")
    async def create_section(payload: CreateSectionPayload) -> dict:
        name = normalize_section_name(payload.name)
        if not name:
            raise HTTPException(status_code=400, detail="Section name required")
        section = {
            "id": str(uuid.uuid4()),
            "name": name,
            "created_at": int(time.time()),
            "updated_at": int(time.time()),
        }
        all_sections = sections()
        all_sections.insert(0, section)
        save_nodes()
        await broadcast_nodes()
        return {"section": public_section(section), "sections": public_sections()}

    @router.patch("/api/sections/{section_id}")
    async def update_section(section_id: str, payload: UpdateSectionPayload) -> dict:
        section = find_section(section_id)
        if not section:
            raise HTTPException(status_code=404, detail="Section not found")
        name = normalize_section_name(payload.name)
        if not name:
            raise HTTPException(status_code=400, detail="Section name required")
        section["name"] = name
        section["updated_at"] = int(time.time())
        save_nodes()
        await broadcast_nodes()
        return {"section": public_section(section), "sections": public_sections()}

    @router.delete("/api/sections/{section_id}")
    async def delete_section(section_id: str) -> dict:
        section = find_section(section_id)
        if not section:
            raise HTTPException(status_code=404, detail="Section not found")

        current_sections = [s for s in sections() if s.get("id") != section_id]
        set_sections(current_sections)

        moved = [node for node in nodes().values() if node.get("section_id") == section_id]
        moved.sort(
            key=lambda n: (
                int(n.get("section_order")) if isinstance(n.get("section_order"), int) else 10**9,
                (n.get("name") or "").lower(),
            )
        )
        current_unsectioned = [node for node in nodes().values() if not (node.get("section_id") or "")]
        max_order = -1
        for node in current_unsectioned:
            try:
                max_order = max(max_order, int(node.get("section_order")))
            except (TypeError, ValueError):
                continue
        next_order = max_order + 1
        for node in moved:
            node["section_id"] = None
            node["section_order"] = next_order
            next_order += 1

        save_nodes()
        await broadcast_nodes()
        return {"ok": True, "sections": public_sections(), "nodes": public_nodes()}

    @router.post("/api/sections/reorder")
    async def reorder_sections(payload: ReorderSectionsPayload) -> dict:
        requested = [sid for sid in (payload.section_ids or []) if isinstance(sid, str) and sid]
        current_ids = [section.get("id") for section in sections() if section.get("id")]
        if not requested or set(requested) != set(current_ids) or len(requested) != len(current_ids):
            raise HTTPException(status_code=400, detail="Invalid section order")
        lookup = {section.get("id"): section for section in sections()}
        new_sections = [lookup[sid] for sid in requested if sid in lookup]
        now = int(time.time())
        for section in new_sections:
            section["updated_at"] = now
        set_sections(new_sections)
        save_nodes()
        await broadcast_nodes()
        return {"sections": public_sections()}

    @router.post("/api/nodes/{node_id}/section")
    async def set_node_section(node_id: str, payload: SetNodeSectionPayload) -> dict:
        node = nodes().get(node_id)
        if not node:
            raise HTTPException(status_code=404, detail="Node not found")
        section_id = payload.section_id
        if isinstance(section_id, str):
            section_id = section_id.strip() or None
        else:
            section_id = None

        if section_id and not find_section(section_id):
            raise HTTPException(status_code=404, detail="Section not found")

        node["section_id"] = section_id
        if section_id is None:
            try:
                node["section_order"] = int(node.get("section_order"))
            except (TypeError, ValueError):
                node["section_order"] = None
        save_nodes()
        await broadcast_nodes()
        return {"result": {"id": node_id, "section_id": section_id}}

    @router.post("/api/nodes/reorder")
    async def reorder_nodes(payload: ReorderNodesPayload) -> dict:
        if not payload.sections:
            raise HTTPException(status_code=400, detail="No sections provided")

        seen_nodes: set[str] = set()
        section_updates: list[tuple[Optional[str], list[str]]] = []
        current_nodes = nodes()
        for entry in payload.sections:
            section_id: Optional[str] = entry.section_id
            if isinstance(section_id, str):
                section_id = section_id.strip() or None
            else:
                section_id = None
            if section_id and not find_section(section_id):
                raise HTTPException(status_code=404, detail="Section not found")
            node_ids = [nid for nid in (entry.node_ids or []) if isinstance(nid, str) and nid]
            if len(node_ids) != len(entry.node_ids or []):
                raise HTTPException(status_code=400, detail="Invalid node id")
            if len(set(node_ids)) != len(node_ids):
                raise HTTPException(status_code=400, detail="Duplicate node ids")
            for nid in node_ids:
                if nid in seen_nodes:
                    raise HTTPException(status_code=400, detail="Node appears in multiple sections")
                if nid not in current_nodes:
                    raise HTTPException(status_code=404, detail=f"Node not found: {nid}")
                seen_nodes.add(nid)
            section_updates.append((section_id, node_ids))

        for section_id, node_ids in section_updates:
            for idx, nid in enumerate(node_ids):
                node = current_nodes[nid]
                node["section_id"] = section_id
                node["section_order"] = idx

            remaining = [
                node
                for node in current_nodes.values()
                if node.get("section_id") == section_id and node.get("id") not in set(node_ids)
            ]
            remaining.sort(
                key=lambda n: (
                    int(n.get("section_order")) if isinstance(n.get("section_order"), int) else 10**9,
                    (n.get("name") or "").lower(),
                )
            )
            next_index = len(node_ids)
            for node in remaining:
                node["section_order"] = next_index
                next_index += 1

        save_nodes()
        await broadcast_nodes()
        return {"ok": True, "sections": public_sections(), "nodes": public_nodes()}

    @router.get("/api/nodes/discover")
    async def discover_nodes() -> StreamingResponse:
        networks = detect_discovery_networks()
        if not networks:
            raise HTTPException(status_code=503, detail="No IPv4 networks available for discovery")
        hosts = hosts_for_networks(networks)
        if not hosts:
            raise HTTPException(status_code=503, detail="No hosts available for discovery")

        async def _event_stream() -> AsyncIterator[str]:
            found = 0
            limited = len(hosts) >= discovery_max_hosts
            yield (
                json.dumps(
                    {
                        "type": "start",
                        "networks": networks,
                        "host_count": len(hosts),
                        "limited": limited,
                    }
                )
                + "\n"
            )
            sonos_task: Optional[asyncio.Task[list[dict]]] = None
            sonos_emitted = False
            try:
                sonos_task = asyncio.create_task(sonos_ssdp_discover())
                async for result in stream_host_probes(hosts):
                    found += 1
                    yield json.dumps({"type": "discovered", "data": result}) + "\n"
                    if sonos_task and not sonos_emitted and sonos_task.done():
                        sonos_emitted = True
                        try:
                            sonos_items = sonos_task.result() or []
                        except Exception:
                            sonos_items = []
                        for item in sonos_items:
                            found += 1
                            yield json.dumps({"type": "discovered", "data": item}) + "\n"
                sonos_items: list[dict] = []
                if sonos_task:
                    try:
                        sonos_items = await asyncio.wait_for(
                            sonos_task,
                            timeout=max(0.5, float(sonos_discovery_timeout) + 1.0),
                        )
                    except Exception:
                        sonos_items = []
                if not sonos_emitted:
                    for item in sonos_items or []:
                        found += 1
                        yield json.dumps({"type": "discovered", "data": item}) + "\n"
            except asyncio.CancelledError:
                yield json.dumps({"type": "cancelled", "found": found}) + "\n"
                raise
            except Exception:
                yield json.dumps({"type": "error", "message": "Discovery failed"}) + "\n"
            else:
                yield json.dumps({"type": "complete", "found": found}) + "\n"
            finally:
                if sonos_task and not sonos_task.done():
                    sonos_task.cancel()

        return StreamingResponse(_event_stream(), media_type="application/x-ndjson")

    @router.post("/api/web-nodes/session")
    async def web_node_session(payload: WebNodeOffer, request: Request) -> dict:
        relay = get_webrtc_relay()
        if not webrtc_enabled or not relay:
            raise HTTPException(status_code=503, detail="Web nodes are disabled")
        name = payload.name.strip() or "Web node"
        loop = asyncio.get_running_loop()
        future: asyncio.Future = loop.create_future()
        request_id = secrets.token_urlsafe(16)
        client_host = request.client.host if request.client else None
        entry = {
            "id": request_id,
            "name": name,
            "offer_sdp": payload.sdp,
            "offer_type": payload.type,
            "future": future,
            "client_host": client_host,
            "created_at": time.time(),
        }
        snapshot = get_web_node_snapshot(entry)
        pending_web_node_requests()[request_id] = entry
        await broadcast_web_node_request_event("created", snapshot=snapshot)
        try:
            result = await asyncio.wait_for(future, timeout=web_node_approval_timeout)
        except asyncio.TimeoutError:
            pending = pending_web_node_requests().pop(request_id, None)
            pending_snapshot = get_web_node_snapshot(pending) if pending else snapshot
            if pending and not pending["future"].done():
                pending["future"].set_result({"status": "expired", "status_code": 408})
            await broadcast_web_node_request_event("resolved", snapshot=pending_snapshot, status="expired")
            raise HTTPException(status_code=408, detail="Approval timed out")
        except HTTPException as exc:
            raise exc
        except Exception as exc:  # pragma: no cover
            raise HTTPException(status_code=500, detail=str(exc))
        finally:
            pending_web_node_requests().pop(request_id, None)
        if result.get("status") != "approved":
            status_code = result.get("status_code", 403)
            detail = result.get("message") or "Web node request denied"
            raise HTTPException(status_code=status_code, detail=detail)
        return {
            "node": result["node"],
            "answer": result["answer"],
            "answer_type": result["answer_type"],
        }

    @router.post("/api/web-nodes/private-session")
    async def private_web_node_session(payload: WebNodeOffer, request: Request) -> dict:
        user = get_current_user(request)
        name = payload.name.strip() or f"{user.get('username') or 'User'} browser"
        return await establish_private_web_node_session(user, name, payload.sdp, payload.type)

    @router.delete("/api/web-nodes/private-session")
    async def end_private_web_node_session(request: Request) -> dict:
        user = get_current_user(request)
        removed = await remove_private_web_node(user.get("id"))
        return {"ok": True, "removed": removed.get("id") if removed else None}

    @router.get("/api/web-nodes/requests")
    async def list_web_node_requests(_: dict = Depends(require_admin)) -> dict:
        return {"requests": pending_web_node_snapshots()}

    @router.post("/api/web-nodes/requests/{request_id}")
    async def decide_web_node_request(
        request_id: str,
        payload: WebNodeRequestDecisionPayload,
        _: dict = Depends(require_admin),
    ) -> dict:
        action = payload.action.lower().strip()
        pending = pop_pending_web_node_request(request_id)
        snapshot = get_web_node_snapshot(pending)
        future: asyncio.Future = pending.get("future")
        if action == "approve":
            try:
                result = await establish_web_node_session_for_request(pending)
            except HTTPException as exc:
                if future and not future.done():
                    future.set_result(
                        {
                            "status": "failed",
                            "status_code": exc.status_code,
                            "message": exc.detail,
                        }
                    )
                await broadcast_web_node_request_event(
                    "resolved", snapshot=snapshot, status="failed", reason=exc.detail
                )
                raise
            if future and not future.done():
                future.set_result({"status": "approved", **result})
            await broadcast_web_node_request_event("resolved", snapshot=snapshot, status="approved")
            return {"ok": True, "status": "approved"}
        reason = (payload.reason or "Request denied").strip() or "Request denied"
        if future and not future.done():
            future.set_result({"status": "denied", "message": reason, "status_code": 403})
        await broadcast_web_node_request_event("resolved", snapshot=snapshot, status="denied", reason=reason)
        return {"ok": True, "status": "denied"}

    @router.websocket("/ws/web-node")
    async def web_node_ws(ws: WebSocket):
        await ws.accept()
        user = await require_ws_user(ws)
        if not user:
            return
        node_id = ws.query_params.get("node_id")
        if not node_id or node_id not in nodes():
            await ws.close(code=1008)
            return
        browser_ws()[node_id] = ws
        try:
            while True:
                await ws.receive_text()
        except Exception:
            pass
        finally:
            browser_ws().pop(node_id, None)

    @router.websocket("/ws/nodes")
    async def nodes_ws(ws: WebSocket):
        await ws.accept()
        user = await require_ws_user(ws)
        if not user:
            return
        node_watchers_set = node_watchers()
        node_watchers_set[ws] = user
        try:
            await ws.send_json({"type": "nodes", "sections": public_sections(), "nodes": public_nodes_for_user(user)})
            await ws.send_json({"type": "web_node_requests", "requests": pending_web_node_snapshots()})
            while True:
                await ws.receive_text()
        except Exception:
            pass
        finally:
            node_watchers_set.pop(ws, None)

    @router.websocket("/ws/terminal/{token}")
    async def terminal_ws(ws: WebSocket, token: str):
        await ws.accept()
        if not node_terminal_enabled:
            await ws.close(code=4403)
            return
        user = await require_ws_user(ws)
        if not user:
            return
        session = terminal_sessions().pop(token, None)
        now = time.time()
        if not session or session.get("expires_at", 0) < now:
            await ws.close(code=4403)
            return
        target = session.get("target") or {}
        key_path = (target.get("key_path") or "").strip()
        if key_path:
            key_path = os.path.expanduser(key_path)
            if not Path(key_path).exists():
                await ws.send_json({"type": "error", "message": "SSH key not found"})
                await ws.close(code=1011)
                return
        password = (target.get("password") or "").strip()
        if not key_path and not password:
            await ws.send_json({"type": "error", "message": "No SSH credentials configured"})
            await ws.close(code=1011)
            return
        ssh_host = target.get("host")
        if not ssh_host:
            await ws.send_json({"type": "error", "message": "Terminal host unavailable"})
            await ws.close(code=1011)
            return
        ssh_kwargs = {
            "host": ssh_host,
            "port": target.get("port", 22),
            "username": target.get("user"),
            "client_keys": [key_path] if key_path else None,
            "password": password or None,
        }
        if not ssh_kwargs["username"]:
            await ws.send_json({"type": "error", "message": "Terminal user is not configured"})
            await ws.close(code=1011)
            return
        if not node_terminal_strict_host_key:
            ssh_kwargs["known_hosts"] = None
        term_size = asyncssh.TermSize(80, 24)
        deadline = session.get("deadline", now + node_terminal_max_duration)

        async def _send_error(message: str) -> None:
            try:
                await ws.send_json({"type": "error", "message": message})
            finally:
                await ws.close(code=1011)

        try:
            async with asyncssh.connect(
                ssh_kwargs["host"],
                port=ssh_kwargs.get("port") or 22,
                username=ssh_kwargs.get("username"),
                client_keys=ssh_kwargs.get("client_keys"),
                password=ssh_kwargs.get("password"),
                known_hosts=ssh_kwargs.get("known_hosts"),
            ) as conn:
                process = await conn.create_process(
                    term_type="xterm-256color", term_size=term_size, encoding=None
                )

                async def pump_stream(stream):
                    try:
                        while True:
                            chunk = await stream.read(1024)
                            if not chunk:
                                break
                            if isinstance(chunk, bytes):
                                text = chunk.decode("utf-8", errors="ignore")
                            else:
                                text = chunk
                            try:
                                await ws.send_json({"type": "output", "data": text})
                            except Exception:
                                break
                    except Exception:
                        return

                async def pump_input():
                    try:
                        while True:
                            if time.time() > deadline:
                                await ws.send_json({"type": "error", "message": "Terminal session expired"})
                                break
                            raw = await ws.receive_text()
                            payload = json.loads(raw)
                            msg_type = payload.get("type")
                            if msg_type == "input":
                                data = payload.get("data", "")
                                if data:
                                    process.stdin.write(data)
                                    await process.stdin.drain()
                            elif msg_type == "resize":
                                cols = int(payload.get("cols") or 80)
                                rows = int(payload.get("rows") or 24)
                                try:
                                    set_size = getattr(process.channel, "set_terminal_size", None)
                                    if asyncio.iscoroutinefunction(set_size):
                                        await set_size(cols, rows)
                                    elif set_size:
                                        set_size(cols, rows)
                                except Exception:
                                    pass
                            elif msg_type == "close":
                                break
                    except Exception:
                        pass
                    finally:
                        try:
                            process.stdin.write_eof()
                        except Exception:
                            pass

                tasks = [asyncio.create_task(pump_input())]
                if process.stdout:
                    tasks.append(asyncio.create_task(pump_stream(process.stdout)))
                if process.stderr:
                    tasks.append(asyncio.create_task(pump_stream(process.stderr)))

                done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                for task in pending:
                    task.cancel()
                if pending:
                    await asyncio.gather(*pending, return_exceptions=True)
                await process.wait()
                try:
                    await ws.send_json({"type": "exit", "code": process.exit_status})
                except Exception:
                    pass
                try:
                    await ws.close()
                except Exception:
                    pass
        except asyncssh.PermissionDenied as exc:
            await _send_error(f"Permission denied: {exc}")
        except asyncssh.Error as exc:
            await _send_error(f"SSH error: {exc}")
        except Exception as exc:
            await _send_error(f"Terminal failed: {exc}")

    return router
