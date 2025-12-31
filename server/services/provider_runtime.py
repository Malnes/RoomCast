from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Callable, Dict, List

from fastapi import HTTPException

from providers.docker_runtime import DockerUnavailable


class ProviderRuntimeService:
    def __init__(
        self,
        *,
        spotify_provider: Any,
        radio_provider: Any,
        audiobookshelf_provider: Any,
        write_sources_file: Callable[[List[dict]], None],
        load_sources: Callable[[], None],
        save_sources: Callable[[], None],
        save_channels: Callable[[], None],
        sources_by_id: Dict[str, dict],
        channels_by_id: Dict[str, dict],
        sources_path: Path,
        config_path: Path,
        spotify_token_path: Path,
        librespot_status_path: Path,
        librespot_fallback_name: str,
        radio_worker_token: str,
        abs_worker_token: str,
    ) -> None:
        self._spotify_provider = spotify_provider
        self._radio_provider = radio_provider
        self._audiobookshelf_provider = audiobookshelf_provider
        self._write_sources_file = write_sources_file
        self._load_sources = load_sources
        self._save_sources = save_sources
        self._save_channels = save_channels
        self._sources_by_id = sources_by_id
        self._channels_by_id = channels_by_id
        self._sources_path = sources_path
        self._config_path = config_path
        self._spotify_token_path = spotify_token_path
        self._librespot_status_path = librespot_status_path
        self._librespot_fallback_name = librespot_fallback_name
        self._radio_worker_token = radio_worker_token
        self._abs_worker_token = abs_worker_token

    def controller_container_id(self) -> str:
        # In Docker, HOSTNAME is set to the container id (short).
        return (os.getenv("HOSTNAME", "") or "").strip()

    def reconcile_spotify_runtime(self, instances: int) -> None:
        image = (os.getenv("ROOMCAST_LIBRESPOT_IMAGE") or "").strip() or "ghcr.io/malnes/roomcast-librespot:latest"
        try:
            self._spotify_provider.reconcile_runtime(
                controller_container_id=self.controller_container_id(),
                instances=instances,
                librespot_image=image,
                fallback_name_a=self._librespot_fallback_name,
                fallback_name_b=os.getenv("LIBRESPOT_NAME_CH2", "RoomCast CH2"),
            )
        except DockerUnavailable as exc:
            raise HTTPException(status_code=503, detail=str(exc))

    def reconcile_radio_runtime(self, enabled: bool) -> None:
        image = (os.getenv("ROOMCAST_RADIO_WORKER_IMAGE") or "").strip() or "ghcr.io/malnes/roomcast-radio-worker:latest"
        try:
            self._radio_provider.reconcile_runtime(
                controller_container_id=self.controller_container_id(),
                enabled=enabled,
                radio_worker_image=image,
                controller_base_url=os.getenv("CONTROLLER_BASE_URL", "http://controller:8000"),
                radio_worker_token=self._radio_worker_token,
                assignment_interval=int(os.getenv("RADIO_ASSIGNMENT_INTERVAL", "10")),
            )
        except DockerUnavailable as exc:
            raise HTTPException(status_code=503, detail=str(exc))

    def reconcile_audiobookshelf_runtime(self, enabled: bool) -> None:
        image = (
            (os.getenv("ROOMCAST_AUDIOBOOKSHELF_WORKER_IMAGE") or "").strip()
            or "ghcr.io/malnes/roomcast-audiobookshelf-worker:latest"
        )
        try:
            self._audiobookshelf_provider.reconcile_runtime(
                controller_container_id=self.controller_container_id(),
                enabled=enabled,
                worker_image=image,
                controller_base_url=os.getenv("CONTROLLER_BASE_URL", "http://controller:8000"),
                worker_token=self._abs_worker_token,
                assignment_interval=int(os.getenv("AUDIOBOOKSHELF_ASSIGNMENT_INTERVAL", "10")),
            )
        except DockerUnavailable as exc:
            raise HTTPException(status_code=503, detail=str(exc))

    def apply_spotify_provider(self, instances: int) -> None:
        instances = 2 if instances >= 2 else 1
        sources_entries = self._spotify_provider.desired_source_entries(
            instances=instances,
            config_path_a=str(self._config_path),
            token_path_a=str(self._spotify_token_path),
            status_path_a=str(self._librespot_status_path),
            config_path_b="/config/spotify-ch2.json",
            token_path_b="/config/spotify-token-ch2.json",
            status_path_b="/config/librespot-status-ch2.json",
        )
        self._write_sources_file(sources_entries)
        self._load_sources()
        self.reconcile_spotify_runtime(instances)

    def disable_spotify_provider(self) -> None:
        # Remove spotify sources and detach channels.
        self._sources_by_id.pop("spotify:a", None)
        self._sources_by_id.pop("spotify:b", None)
        if self._sources_path.exists():
            self._save_sources()

        for channel in self._channels_by_id.values():
            if (channel.get("source") or "").strip().lower() == "spotify":
                channel["source"] = "none"
                channel["source_ref"] = None
                channel["snap_stream"] = f"Spotify_CH{channel.get('order', 1)}"
                channel["fifo_path"] = f"/tmp/snapfifo-{channel['id']}"
        self._save_channels()
        self._spotify_provider.stop_runtime()

    def apply_radio_provider(self) -> None:
        # Radio is a per-channel source; do not auto-create channels.
        self.reconcile_radio_runtime(True)

    def disable_radio_provider(self) -> None:
        # Detach radio channels.
        for channel in self._channels_by_id.values():
            if (channel.get("source") or "").strip().lower() == "radio":
                channel["source"] = "none"
                channel["source_ref"] = None
                channel["snap_stream"] = f"Spotify_CH{channel.get('order', 1)}"
                channel["fifo_path"] = f"/tmp/snapfifo-{channel['id']}"
                channel.pop("radio_state", None)
        self._save_channels()
        self.reconcile_radio_runtime(False)

    def apply_audiobookshelf_provider(self) -> None:
        # Audiobookshelf is a per-channel source; do not auto-create channels.
        self.reconcile_audiobookshelf_runtime(True)

    def disable_audiobookshelf_provider(self) -> None:
        # Detach Audiobookshelf channels.
        for channel in self._channels_by_id.values():
            if (channel.get("source") or "").strip().lower() == "audiobookshelf":
                channel["source"] = "none"
                channel["source_ref"] = None
                channel["snap_stream"] = f"Spotify_CH{channel.get('order', 1)}"
                channel["fifo_path"] = f"/tmp/snapfifo-{channel['id']}"
                channel.pop("abs_state", None)
        self._save_channels()
        self.reconcile_audiobookshelf_runtime(False)
