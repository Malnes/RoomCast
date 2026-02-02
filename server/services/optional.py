from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

from fastapi import HTTPException


@dataclass
class NullProvider:
    name: str
    enabled: bool = False

    def _raise(self) -> None:
        raise HTTPException(status_code=503, detail=f"{self.name} provider not installed")

    def reconcile_runtime(self, *args: Any, **kwargs: Any) -> None:
        self._raise()

    def stop_runtime(self, *args: Any, **kwargs: Any) -> None:
        self._raise()

    def desired_source_entries(self, *args: Any, **kwargs: Any) -> list[dict]:
        self._raise()
        return []


@dataclass
class NullSonosService:
    enabled: bool = False

    def _raise(self) -> None:
        raise HTTPException(status_code=503, detail="Sonos support not installed")

    def ip_from_url(self, url: Optional[str]) -> Optional[str]:
        return None

    async def fetch_description(self, *args: Any, **kwargs: Any) -> Optional[dict]:
        self._raise()

    async def get_zone_name(self, *args: Any, **kwargs: Any) -> Optional[str]:
        self._raise()

    async def set_volume(self, *args: Any, **kwargs: Any) -> None:
        self._raise()

    async def set_mute(self, *args: Any, **kwargs: Any) -> None:
        self._raise()

    async def set_eq(self, *args: Any, **kwargs: Any) -> None:
        self._raise()

    def normalize_eq(self, *args: Any, **kwargs: Any) -> dict:
        self._raise()
        return {}

    async def discover(self, *args: Any, **kwargs: Any) -> list[dict]:
        return []

    async def ssdp_discover(self, *args: Any, **kwargs: Any) -> list[dict]:
        return []

    async def ping(self, *args: Any, **kwargs: Any) -> bool:
        return False

    def parse_network_from_review(self, *args: Any, **kwargs: Any) -> Optional[dict]:
        return None

    async def connection_loop(self, *args: Any, **kwargs: Any) -> None:
        return None

    def client_allows_stream(self, *args: Any, **kwargs: Any) -> bool:
        return False

    def mark_stream_activity(self, *args: Any, **kwargs: Any) -> None:
        return None

    def mark_stream_end(self, *args: Any, **kwargs: Any) -> None:
        return None

    def find_node_by_ip(self, *args: Any, **kwargs: Any) -> Optional[dict]:
        return None

    async def reconcile_groups(self, *args: Any, **kwargs: Any) -> None:
        self._raise()

    async def attempt_reconnect(self, *args: Any, **kwargs: Any) -> bool:
        self._raise()

    async def become_standalone(self, *args: Any, **kwargs: Any) -> None:
        self._raise()

    async def stop(self, *args: Any, **kwargs: Any) -> None:
        self._raise()


@dataclass
class NullCastService:
    enabled: bool = False

    def _raise(self) -> None:
        raise HTTPException(status_code=503, detail="Google Cast support not installed")

    def ip_from_url(self, url: Optional[str]) -> Optional[str]:
        return None

    async def discover(self, *args: Any, **kwargs: Any) -> list[dict]:
        return []

    async def fetch_device(self, *args: Any, **kwargs: Any) -> Optional[dict]:
        self._raise()

    async def play_stream(self, *args: Any, **kwargs: Any) -> None:
        self._raise()

    async def stop(self, *args: Any, **kwargs: Any) -> None:
        self._raise()

    async def set_volume(self, *args: Any, **kwargs: Any) -> None:
        self._raise()

    async def set_mute(self, *args: Any, **kwargs: Any) -> None:
        self._raise()
