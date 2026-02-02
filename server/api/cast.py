from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, AsyncIterator, Awaitable, Callable, Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import Response, StreamingResponse

log = logging.getLogger("roomcast")


def create_cast_router(
    *,
    get_current_user: Callable,
    resolve_channel_id: Callable[[Optional[str]], str],
    get_channels_by_id: Callable[[], dict],
    cast_find_node_by_ip: Callable[[Optional[str]], Optional[dict]],
    cast_client_allows_stream: Callable[[str, Optional[str]], bool],
    cast_mark_stream_activity: Callable[[str, Optional[str], str], None],
    cast_mark_stream_end: Callable[[Optional[str]], None],
    cast_discover: Callable[[], Awaitable[list[dict]]],
    normalize_stereo_mode: Callable[[Any], str],
    ffmpeg_pan_filter_for_stereo_mode: Callable[[str], str],
    snapcast_client: Any,
    snapserver_agent_host: str,
    snapclient_port: int,
    webrtc_latency_ms: int,
    webrtc_sample_rate: int,
    cast_stream_bitrate_kbps: int,
    server_default_name: str,
) -> APIRouter:
    router = APIRouter()

    @router.get("/api/cast/discover")
    async def cast_discover_route(_: dict = Depends(get_current_user)) -> dict:
        """Discover Google Cast devices for adding as RoomCast nodes."""
        try:
            items = await cast_discover()
        except Exception as exc:  # pragma: no cover - defensive
            log.warning("Cast discovery failed: %s", exc)
            items = []
        return {"devices": items or []}

    @router.get("/api/cast/stream/{channel_id}")
    async def cast_channel_stream(channel_id: str, request: Request) -> StreamingResponse:
        client_host = request.client.host if request.client else None
        log.info("Cast stream request started (channel=%s, client=%s)", channel_id, client_host)
        resolved = resolve_channel_id(channel_id)
        channel = (get_channels_by_id() or {}).get(resolved)
        if not channel or not channel.get("snap_stream"):
            raise HTTPException(status_code=404, detail="Unknown channel")
        if not cast_client_allows_stream(resolved, client_host):
            log.warning(
                "Rejecting Cast stream pull for unassigned/mismatched node (channel=%s, client=%s)",
                resolved,
                client_host,
            )
            raise HTTPException(status_code=409, detail="Cast node is unassigned")
        cast_mark_stream_activity(resolved, client_host, "get")
        stream_id = channel.get("snap_stream")
        if not stream_id:
            raise HTTPException(status_code=400, detail="Channel is missing snap_stream")

        latency_ms = max(100, min(1200, int(webrtc_latency_ms)))
        sample_rate = int(webrtc_sample_rate)
        cast_node = cast_find_node_by_ip(client_host)
        stereo_mode = normalize_stereo_mode((cast_node or {}).get("stereo_mode"))
        pan_filter = ffmpeg_pan_filter_for_stereo_mode(stereo_mode)

        def _sanitize_snapcast_host_id(value: str) -> str:
            cleaned = "".join(ch if ch.isalnum() or ch in "_.-" else "-" for ch in (value or ""))
            cleaned = cleaned.strip("-._")
            return cleaned or "unknown"

        stable_suffix = None
        if isinstance(cast_node, dict) and cast_node.get("id"):
            stable_suffix = str(cast_node.get("id"))
        elif client_host:
            stable_suffix = str(client_host)
        else:
            stable_suffix = "unknown"
        client_id = f"roomcast-cast-{_sanitize_snapcast_host_id(stable_suffix)}"

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

        eq_filters = _ffmpeg_eq_filters((cast_node or {}).get("eq"))

        async def _iter_bytes() -> AsyncIterator[bytes]:
            snap_proc: Optional[asyncio.subprocess.Process] = None
            ff_proc: Optional[asyncio.subprocess.Process] = None
            assign_task: Optional[asyncio.Task[None]] = None
            pump_task: Optional[asyncio.Task[None]] = None
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
                    f"{max(96, int(cast_stream_bitrate_kbps))}k",
                    "-f",
                    "mp3",
                    "pipe:1",
                ]

                ff_proc = await asyncio.create_subprocess_exec(
                    *ff_args,
                    stdin=asyncio.subprocess.PIPE,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.DEVNULL,
                )

                async def _pump() -> None:
                    assert snap_proc and snap_proc.stdout and ff_proc and ff_proc.stdin
                    while True:
                        chunk = await snap_proc.stdout.read(4096)
                        if not chunk:
                            break
                        ff_proc.stdin.write(chunk)
                        await ff_proc.stdin.drain()
                    try:
                        ff_proc.stdin.close()
                    except Exception:
                        pass

                pump_task = asyncio.create_task(_pump())

                assert ff_proc.stdout
                while True:
                    chunk = await ff_proc.stdout.read(4096)
                    if not chunk:
                        break
                    yield chunk
            finally:
                if assign_task:
                    assign_task.cancel()
                if pump_task:
                    pump_task.cancel()
                if snap_proc and snap_proc.returncode is None:
                    snap_proc.terminate()
                    try:
                        await asyncio.wait_for(snap_proc.wait(), timeout=5)
                    except Exception:
                        snap_proc.kill()
                        await snap_proc.wait()
                if ff_proc and ff_proc.returncode is None:
                    ff_proc.terminate()
                    try:
                        await asyncio.wait_for(ff_proc.wait(), timeout=5)
                    except Exception:
                        ff_proc.kill()
                        await ff_proc.wait()
                cast_mark_stream_end(client_host)

        headers = {
            "Cache-Control": "no-store",
            "Content-Type": "audio/mpeg",
            "icy-name": server_default_name,
        }
        return StreamingResponse(_iter_bytes(), headers=headers, media_type="audio/mpeg")

    @router.head("/api/cast/stream/{channel_id}")
    async def cast_stream_head(channel_id: str, request: Request) -> Response:
        client_host = request.client.host if request.client else None
        resolved = resolve_channel_id(channel_id)
        channel = (get_channels_by_id() or {}).get(resolved)
        if not channel or not channel.get("snap_stream"):
            raise HTTPException(status_code=404, detail="Unknown channel")
        if not cast_client_allows_stream(resolved, client_host):
            raise HTTPException(status_code=409, detail="Cast node is unassigned")
        cast_mark_stream_activity(resolved, client_host, "head")
        headers = {
            "Cache-Control": "no-store",
            "Content-Type": "audio/mpeg",
            "icy-name": server_default_name,
        }
        return Response(status_code=200, headers=headers)

    return router
