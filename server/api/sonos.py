import asyncio
import logging
import re
import time
from typing import Any, AsyncIterator, Awaitable, Callable, Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import Response, StreamingResponse


log = logging.getLogger("roomcast")


def create_sonos_router(
    *,
    get_current_user: Callable,
    resolve_channel_id: Callable[[Optional[str]], str],
    get_channels_by_id: Callable[[], dict],
    get_nodes: Callable[[], dict],
    resolve_node_channel_id: Callable[[dict], Optional[str]],
    sonos_attempt_reconnect: Callable[[str, list[dict]], Awaitable[bool]],
    sonos_discover: Callable[[], Awaitable[list[dict]]],
    sonos_client_allows_stream: Callable[[str, Optional[str]], bool],
    sonos_mark_stream_activity: Callable[[str, Optional[str], str], None],
    sonos_mark_stream_end: Callable[[Optional[str]], None],
    sonos_find_node_by_ip: Callable[[Optional[str]], Optional[dict]],
    normalize_stereo_mode: Callable[[Any], str],
    ffmpeg_pan_filter_for_stereo_mode: Callable[[str], str],
    snapcast_client: Any,
    snapserver_agent_host: str,
    snapclient_port: int,
    webrtc_latency_ms: int,
    webrtc_sample_rate: int,
    sonos_stream_bitrate_kbps: int,
    server_default_name: str,
) -> APIRouter:
    router = APIRouter()

    @router.post("/api/sonos/nodes/{node_id}/retry")
    async def sonos_retry_node(node_id: str, _: dict = Depends(get_current_user)) -> dict:
        nodes = get_nodes()
        node = nodes.get(node_id)
        if not node or node.get("type") != "sonos":
            raise HTTPException(status_code=404, detail="Unknown Sonos node")

        channel_id = resolve_node_channel_id(node)
        if not channel_id:
            raise HTTPException(status_code=409, detail="Sonos node is unassigned")

        members: list[dict] = []
        for n in nodes.values():
            if n.get("type") != "sonos":
                continue
            if n.get("online") is False:
                continue
            if resolve_node_channel_id(n) != channel_id:
                continue
            members.append(n)

        ok = await sonos_attempt_reconnect(channel_id, members)
        return {"ok": bool(ok), "channel_id": channel_id, "members": [m.get("id") for m in members]}

    @router.get("/api/sonos/stream/{channel_id}")
    async def sonos_channel_stream(channel_id: str, request: Request) -> StreamingResponse:
        client_host = request.client.host if request.client else None
        log.info("Sonos stream request started (channel=%s, client=%s)", channel_id, client_host)
        resolved = resolve_channel_id(channel_id)
        channel = (get_channels_by_id() or {}).get(resolved)
        if not channel or not channel.get("snap_stream"):
            raise HTTPException(status_code=404, detail="Unknown channel")
        if not sonos_client_allows_stream(resolved, client_host):
            log.warning(
                "Rejecting Sonos stream pull for unassigned/mismatched node (channel=%s, client=%s)",
                resolved,
                client_host,
            )
            raise HTTPException(status_code=409, detail="Sonos node is unassigned")
        sonos_mark_stream_activity(resolved, client_host, "get")
        stream_id = channel.get("snap_stream")
        if not stream_id:
            raise HTTPException(status_code=400, detail="Channel is missing snap_stream")

        sonos_node = sonos_find_node_by_ip(client_host)

        def _sanitize_snapcast_host_id(value: str) -> str:
            cleaned = re.sub(r"[^a-zA-Z0-9_.-]+", "-", (value or "").strip())
            cleaned = cleaned.strip("-._")
            return cleaned or "unknown"

        stable_suffix = None
        if isinstance(sonos_node, dict) and sonos_node.get("id"):
            stable_suffix = str(sonos_node.get("id"))
        elif client_host:
            stable_suffix = str(client_host)
        else:
            stable_suffix = "unknown"
        client_id = f"roomcast-sonos-{_sanitize_snapcast_host_id(stable_suffix)}"
        latency_ms = max(100, min(1200, int(webrtc_latency_ms)))
        sample_rate = int(webrtc_sample_rate)
        stereo_mode = normalize_stereo_mode((sonos_node or {}).get("stereo_mode"))
        pan_filter = ffmpeg_pan_filter_for_stereo_mode(stereo_mode)

        def _ffmpeg_eq_filters(eq: object) -> list[str]:
            if not isinstance(eq, dict):
                return []
            bands = eq.get("bands")
            if not isinstance(bands, list):
                return []
            filters: list[str] = []
            for band in bands[:31]:
                if not isinstance(band, dict):
                    continue
                try:
                    freq = float(band.get("freq"))
                    gain = float(band.get("gain"))
                    q = float(band.get("q"))
                except (TypeError, ValueError):
                    continue
                freq = max(20.0, min(20000.0, freq))
                gain = max(-12.0, min(12.0, gain))
                q = max(0.2, min(10.0, q))
                if abs(gain) < 0.05:
                    continue
                filters.append(f"equalizer=f={freq:.2f}:width_type=q:width={q:.3f}:g={gain:.2f}")
            return filters

        eq_filters = _ffmpeg_eq_filters((sonos_node or {}).get("eq"))

        async def _iter_bytes() -> AsyncIterator[bytes]:
            snap_proc: Optional[asyncio.subprocess.Process] = None
            ff_proc: Optional[asyncio.subprocess.Process] = None
            assign_task: Optional[asyncio.Task[None]] = None
            pump_task: Optional[asyncio.Task[None]] = None
            last_mark = 0.0
            try:
                snap_args = [
                    "snapclient",
                    "-h",
                    snapserver_agent_host,
                    "-p",
                    str(snapclient_port),
                    "--player",
                    "file:filename=stdout",
                    "--sampleformat",
                    f"{sample_rate}:16:*",
                    "--latency",
                    str(latency_ms),
                    "--logsink",
                    "stderr",
                    "--logfilter",
                    "*:warn",
                    "--hostID",
                    client_id,
                ]
                snap_proc = await asyncio.create_subprocess_exec(
                    *snap_args,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.DEVNULL,
                )

                async def _assign() -> None:
                    deadline = time.time() + 15
                    delay = 0.5
                    while time.time() < deadline:
                        if snap_proc and snap_proc.returncode is not None:
                            return
                        try:
                            await snapcast_client.set_client_stream(client_id, stream_id)
                            return
                        except Exception:
                            await asyncio.sleep(delay)
                            delay = min(2.0, delay * 1.4)

                assign_task = asyncio.create_task(_assign())

                filter_parts: list[str] = []
                if pan_filter:
                    filter_parts.append(pan_filter)
                filter_parts.extend(eq_filters)

                ff_args = [
                    "ffmpeg",
                    "-hide_banner",
                    "-loglevel",
                    "error",
                    "-f",
                    "s16le",
                    "-ar",
                    str(sample_rate),
                    "-ac",
                    "2",
                    "-i",
                    "pipe:0",
                ]
                if filter_parts:
                    ff_args += ["-af", ",".join(filter_parts)]
                ff_args += [
                    "-vn",
                    "-b:a",
                    f"{max(64, int(sonos_stream_bitrate_kbps))}k",
                    "-f",
                    "mp3",
                    "pipe:1",
                ]
                assert snap_proc.stdout is not None
                ff_proc = await asyncio.create_subprocess_exec(
                    *ff_args,
                    stdin=asyncio.subprocess.PIPE,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.DEVNULL,
                )
                assert ff_proc.stdin is not None

                async def _pump_pcm() -> None:
                    assert snap_proc and snap_proc.stdout and ff_proc and ff_proc.stdin
                    try:
                        while True:
                            try:
                                chunk = await asyncio.wait_for(
                                    snap_proc.stdout.read(16 * 1024),
                                    timeout=1.0,
                                )
                            except asyncio.TimeoutError:
                                if snap_proc and snap_proc.returncode is not None:
                                    break
                                continue
                            if not chunk:
                                break
                            ff_proc.stdin.write(chunk)
                            await ff_proc.stdin.drain()
                    except asyncio.CancelledError:
                        return
                    except Exception:
                        return
                    finally:
                        try:
                            ff_proc.stdin.close()
                        except Exception:
                            pass

                pump_task = asyncio.create_task(_pump_pcm())
                assert ff_proc.stdout is not None
                while True:
                    try:
                        if await request.is_disconnected():
                            return
                    except asyncio.CancelledError:
                        return

                    try:
                        chunk = await asyncio.wait_for(ff_proc.stdout.read(32 * 1024), timeout=1.0)
                    except asyncio.TimeoutError:
                        continue
                    except asyncio.CancelledError:
                        return
                    if not chunk:
                        return
                    now = time.time()
                    if now - last_mark > 1.0:
                        sonos_mark_stream_activity(resolved, client_host, "bytes")
                        last_mark = now
                    yield chunk
            finally:
                sonos_mark_stream_end(client_host)
                if assign_task and not assign_task.done():
                    assign_task.cancel()
                if pump_task and not pump_task.done():
                    pump_task.cancel()
                if ff_proc and ff_proc.returncode is None:
                    ff_proc.terminate()
                    try:
                        await asyncio.wait_for(ff_proc.wait(), timeout=3)
                    except asyncio.TimeoutError:
                        ff_proc.kill()
                if snap_proc and snap_proc.returncode is None:
                    snap_proc.terminate()
                    try:
                        await asyncio.wait_for(snap_proc.wait(), timeout=3)
                    except asyncio.TimeoutError:
                        snap_proc.kill()

                try:
                    await snapcast_client.delete_client(client_id)
                except Exception:
                    pass

        headers = {
            "Cache-Control": "no-store",
            "Content-Type": "audio/mpeg",
            "transferMode.dlna.org": "Streaming",
            "contentFeatures.dlna.org": "DLNA.ORG_OP=01;DLNA.ORG_CI=0;DLNA.ORG_FLAGS=01700000000000000000000000000000",
            "icy-name": server_default_name,
        }
        return StreamingResponse(_iter_bytes(), media_type="audio/mpeg", headers=headers)

    @router.head("/api/sonos/stream/{channel_id}")
    async def sonos_channel_stream_head(channel_id: str, request: Request) -> Response:
        client_host = request.client.host if request.client else None
        log.info("Sonos stream HEAD probe (channel=%s, client=%s)", channel_id, client_host)
        resolved = resolve_channel_id(channel_id)
        channel = (get_channels_by_id() or {}).get(resolved)
        if not channel or not channel.get("snap_stream"):
            raise HTTPException(status_code=404, detail="Unknown channel")
        if not sonos_client_allows_stream(resolved, client_host):
            log.warning(
                "Rejecting Sonos stream HEAD probe for unassigned/mismatched node (channel=%s, client=%s)",
                resolved,
                client_host,
            )
            raise HTTPException(status_code=409, detail="Sonos node is unassigned")
        sonos_mark_stream_activity(resolved, client_host, "head")
        headers = {
            "Cache-Control": "no-store",
            "Content-Type": "audio/mpeg",
            "transferMode.dlna.org": "Streaming",
            "contentFeatures.dlna.org": "DLNA.ORG_OP=01;DLNA.ORG_CI=0;DLNA.ORG_FLAGS=01700000000000000000000000000000",
            "icy-name": server_default_name,
        }
        return Response(status_code=200, headers=headers)

    @router.get("/api/sonos/discover")
    async def sonos_discover_route(_: dict = Depends(get_current_user)) -> dict:
        """Discover Sonos speakers for adding as RoomCast nodes."""
        try:
            items = await sonos_discover()
        except Exception as exc:  # pragma: no cover - defensive
            log.warning("Sonos discovery failed: %s", exc)
            items = []
        return {"devices": items or []}

    return router
