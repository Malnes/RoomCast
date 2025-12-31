import logging
import time
from typing import Any, Awaitable, Callable, Optional

from fastapi import APIRouter, Query

log = logging.getLogger("roomcast")


def create_streams_router(
    *,
    resolve_channel_id: Callable[[Optional[str]], str],
    get_channel_order: Callable[[], list[str]],
    get_channels_by_id: Callable[[], dict],
    get_nodes: Callable[[], dict],
    snapcast_status: Callable[[], Awaitable[dict]],
    summarize_snapserver_status: Callable[[Any], tuple[dict, dict]],
    public_snap_client: Callable[[dict, dict], dict],
    get_webrtc_relay: Callable[[], Optional[Any]],
    read_spotify_config: Callable[[str], dict],
    read_librespot_status: Callable[[str], dict],
    snapserver_host: str,
    snapserver_port: int,
) -> APIRouter:
    router = APIRouter()

    @router.get("/api/streams/diagnostics")
    async def stream_diagnostics(channel_id: Optional[str] = Query(None)) -> dict:
        channels_by_id = get_channels_by_id()
        channel_order = get_channel_order()

        if channel_id:
            target_ids = [resolve_channel_id(channel_id)]
        else:
            target_ids = [cid for cid in channel_order if cid in channels_by_id]

        snap_status = None
        snap_error = None
        try:
            snap_status = await snapcast_status()
        except Exception as exc:  # pragma: no cover - network dependency
            snap_error = str(exc)
            log.warning("Stream diagnostics: failed to read snapserver status: %s", exc)

        streams_by_id, clients_by_stream = summarize_snapserver_status(snap_status)

        webrtc_diag = None
        relay = get_webrtc_relay()
        if relay:
            try:
                webrtc_diag = await relay.diagnostics()
            except Exception as exc:  # pragma: no cover - defensive
                log.warning("Stream diagnostics: failed to read WebRTC stats: %s", exc)
                webrtc_diag = None

        channel_payloads = []
        nodes = get_nodes()
        node_lookup = {node_id: node for node_id, node in nodes.items()}
        snapclient_nodes = {
            node.get("snapclient_id"): node for node in nodes.values() if node.get("snapclient_id")
        }

        for cid in target_ids:
            channel = channels_by_id.get(cid)
            if not channel:
                continue

            stream_id = channel.get("snap_stream")
            snap_stream = streams_by_id.get(stream_id)

            hardware_clients = [
                public_snap_client(client, snapclient_nodes) for client in clients_by_stream.get(stream_id, [])
            ]
            connected_clients = sum(1 for client in hardware_clients if client.get("connected"))

            channel_webrtc = (webrtc_diag or {}).get("channels", {}).get(cid) if webrtc_diag else None
            if channel_webrtc and channel_webrtc.get("sessions"):
                for session in channel_webrtc["sessions"]:
                    node = node_lookup.get(session.get("node_id"))
                    if node:
                        session["node_name"] = node.get("name")
                        session["node_type"] = node.get("type")

            spotify_summary = None
            if channel.get("source") == "spotify":
                cfg = read_spotify_config(channel["id"])
                status = read_librespot_status(channel["id"])
                spotify_summary = {
                    "bitrate_kbps": cfg.get("bitrate"),
                    "device_name": cfg.get("device_name"),
                    "normalisation": cfg.get("normalisation"),
                    "username": cfg.get("username"),
                    "status": status.get("state"),
                    "status_message": status.get("message"),
                }

            channel_payloads.append(
                {
                    "id": channel["id"],
                    "name": channel.get("name"),
                    "color": channel.get("color"),
                    "source": channel.get("source", "spotify"),
                    "snap_stream": stream_id,
                    "fifo_path": channel.get("fifo_path"),
                    "spotify": spotify_summary,
                    "radio_state": channel.get("radio_state") if channel.get("source") == "radio" else None,
                    "snapserver_stream": snap_stream,
                    "hardware_clients": hardware_clients,
                    "listeners": {
                        "hardware": len(hardware_clients),
                        "hardware_connected": connected_clients,
                        "webrtc": channel_webrtc.get("listeners") if channel_webrtc else 0,
                    },
                    "webrtc": channel_webrtc,
                }
            )

        response: dict = {
            "timestamp": time.time(),
            "channels": channel_payloads,
            "snapserver": {
                "host": snapserver_host,
                "port": snapserver_port,
                "error": snap_error,
            },
        }
        if webrtc_diag:
            response["webrtc"] = {"sample_rate": webrtc_diag.get("sample_rate")}
        return response

    return router
