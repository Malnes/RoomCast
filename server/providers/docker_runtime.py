from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Dict, Optional


class DockerUnavailable(RuntimeError):
    pass


@dataclass(frozen=True)
class DockerContext:
    network_name: str
    config_volume: str
    snapfifo_volume: str


def _require_docker_client():
    try:
        import docker  # type: ignore
    except Exception as exc:  # pragma: no cover
        raise DockerUnavailable("Python docker SDK is not installed") from exc

    socket_path = os.getenv("DOCKER_HOST", "")
    # docker.from_env defaults to unix socket on Linux.
    try:
        return docker.from_env()
    except Exception as exc:  # pragma: no cover
        raise DockerUnavailable("Docker daemon not reachable") from exc


def detect_docker_context(container_id: str) -> DockerContext:
    """Detect network + volume names from the running controller container."""
    client = _require_docker_client()
    try:
        container = client.containers.get(container_id)
    except Exception as exc:  # pragma: no cover
        raise DockerUnavailable("Unable to inspect controller container") from exc

    networks = (container.attrs.get("NetworkSettings") or {}).get("Networks") or {}
    if not networks:
        raise DockerUnavailable("Controller container is not attached to any Docker network")
    network_name = next(iter(networks.keys()))

    config_volume: Optional[str] = None
    snapfifo_volume: Optional[str] = None

    mounts = container.attrs.get("Mounts") or []
    for mount in mounts:
        if mount.get("Type") != "volume":
            continue
        dst = mount.get("Destination")
        name = mount.get("Name")
        if dst == "/config" and name:
            config_volume = name
        # We'll mount snapfifo into controller at /tmp to be able to detect.
        if dst == "/tmp" and name:
            snapfifo_volume = name

    if not config_volume:
        raise DockerUnavailable("Unable to detect config volume (expected mount at /config)")
    if not snapfifo_volume:
        raise DockerUnavailable("Unable to detect snapfifo volume (expected mount at /tmp)")

    return DockerContext(network_name=network_name, config_volume=config_volume, snapfifo_volume=snapfifo_volume)


def ensure_container_absent(name: str) -> None:
    client = _require_docker_client()
    try:
        container = client.containers.get(name)
    except Exception:
        return
    try:
        container.stop(timeout=8)
    except Exception:
        pass
    try:
        container.remove(force=True)
    except Exception:
        pass


def ensure_container_running(
    *,
    name: str,
    image: str,
    command: list[str] | None,
    environment: Dict[str, str],
    volumes: Dict[str, Dict[str, str]],
    network: str,
) -> None:
    client = _require_docker_client()

    existing = None
    try:
        existing = client.containers.get(name)
    except Exception:
        existing = None

    if existing is not None:
        # If it's already running, leave it. If stopped, start it.
        try:
            existing.reload()
        except Exception:
            pass
        state = ((existing.attrs.get("State") or {}).get("Status") or "").lower()
        if state == "running":
            return
        try:
            existing.start()
            return
        except Exception:
            # Fall back to recreate.
            try:
                existing.remove(force=True)
            except Exception:
                pass

    kwargs = {
        "name": name,
        "image": image,
        "environment": environment,
        "volumes": volumes,
        "restart_policy": {"Name": "unless-stopped"},
        "detach": True,
    }
    if command:
        kwargs["command"] = command

    try:
        container = client.containers.run(**kwargs)
    except Exception as exc:
        raise DockerUnavailable(f"Failed to start Docker container '{name}' (image '{image}'): {exc}") from exc
    try:
        # Attach to the same network as the controller.
        net = client.networks.get(network)
        net.connect(container)
    except Exception:
        # Best effort; container may already be on a network depending on docker config.
        pass
