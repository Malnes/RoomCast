from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Dict

from .docker_runtime import DockerUnavailable, detect_docker_context, ensure_container_absent, ensure_container_running


def reconcile_runtime(
    *,
    controller_container_id: str,
    enabled: bool,
    radio_worker_image: str,
    controller_base_url: str = "http://controller:8000",
    radio_worker_token: str = "",
    assignment_interval: int = 10,
) -> None:
    if not controller_container_id:
        raise DockerUnavailable("Missing controller container id (HOSTNAME)")

    if not enabled:
        ensure_container_absent("roomcast-provider-radio-worker")
        return

    ctx = detect_docker_context(controller_container_id)
    volumes = {
        ctx.snapfifo_volume: {"bind": "/tmp", "mode": "rw"},
    }

    env: Dict[str, str] = {
        "CONTROLLER_BASE_URL": controller_base_url,
        "RADIO_WORKER_TOKEN": radio_worker_token,
        "RADIO_ASSIGNMENT_INTERVAL": str(int(assignment_interval)),
    }

    ensure_container_running(
        name="roomcast-provider-radio-worker",
        image=radio_worker_image,
        command=None,
        environment=env,
        volumes=volumes,
        network=ctx.network_name,
    )
