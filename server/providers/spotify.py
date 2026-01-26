from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List

from .docker_runtime import DockerUnavailable, detect_docker_context, ensure_container_absent, ensure_container_running


@dataclass(frozen=True)
class SpotifyRuntimeSpec:
    instances: int
    librespot_image: str
    fallback_name_a: str
    fallback_name_b: str


def normalize_instances(value: object, *, default: int = 1) -> int:
    try:
        count = int(value)
    except (TypeError, ValueError):
        count = default
    if count <= 1:
        return 1
    return 2


def desired_source_entries(
    *,
    instances: int,
    config_path_a: str,
    token_path_a: str,
    status_path_a: str,
    config_path_b: str,
    token_path_b: str,
    status_path_b: str,
) -> List[dict]:
    entries = [
        {
            "id": "spotify:a",
            "kind": "spotify",
            "name": "Spotify A",
            "snap_stream": "Spotify_CH1",
            "config_path": config_path_a,
            "token_path": token_path_a,
            "status_path": status_path_a,
        }
    ]
    if instances >= 2:
        entries.append({
            "id": "spotify:b",
            "kind": "spotify",
            "name": "Spotify B",
            "snap_stream": "Spotify_CH2",
            "config_path": config_path_b,
            "token_path": token_path_b,
            "status_path": status_path_b,
        })
    return entries


def reconcile_runtime(
    *,
    controller_container_id: str,
    enable_a: bool,
    enable_b: bool,
    librespot_image: str,
    fifo_path_a: str = "/tmp/snapfifo-ch1",
    fifo_path_b: str = "/tmp/snapfifo-ch2",
    config_path_a: str = "/config/spotify.json",
    token_path_a: str = "/config/spotify-token.json",
    status_path_a: str = "/config/librespot-status.json",
    config_path_b: str = "/config/spotify-ch2.json",
    token_path_b: str = "/config/spotify-token-ch2.json",
    status_path_b: str = "/config/librespot-status-ch2.json",
    fallback_name_a: str = "RoomCast",
    fallback_name_b: str = "RoomCast CH2",
) -> None:
    if not controller_container_id:
        raise DockerUnavailable("Missing controller container id (HOSTNAME)")

    ctx = detect_docker_context(controller_container_id)
    volumes = {
        ctx.config_volume: {"bind": "/config", "mode": "rw"},
        ctx.snapfifo_volume: {"bind": "/tmp", "mode": "rw"},
    }

    def _env(cfg: str, tok: str, stat: str, fifo: str, fallback: str) -> Dict[str, str]:
        return {
            "CONFIG_PATH": cfg,
            "SPOTIFY_TOKEN_PATH": tok,
            "STATUS_PATH": stat,
            "FIFO_PATH": fifo,
            "LIBRESPOT_FALLBACK_NAME": fallback,
        }

    if enable_a:
        ensure_container_running(
            name="roomcast-provider-spotify-a",
            image=librespot_image,
            command=["/app/run.sh"],
            environment=_env(config_path_a, token_path_a, status_path_a, fifo_path_a, fallback_name_a),
            volumes=volumes,
            network=ctx.network_name,
        )
    else:
        ensure_container_absent("roomcast-provider-spotify-a")

    if enable_b:
        ensure_container_running(
            name="roomcast-provider-spotify-b",
            image=librespot_image,
            command=["/app/run.sh"],
            environment=_env(config_path_b, token_path_b, status_path_b, fifo_path_b, fallback_name_b),
            volumes=volumes,
            network=ctx.network_name,
        )
    else:
        ensure_container_absent("roomcast-provider-spotify-b")


def stop_runtime() -> None:
    ensure_container_absent("roomcast-provider-spotify-a")
    ensure_container_absent("roomcast-provider-spotify-b")
