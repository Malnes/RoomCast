from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Optional

import pychromecast

log = logging.getLogger("roomcast.cast")


@dataclass
class CastDevice:
    name: str
    host: str
    uuid: Optional[str] = None
    model: Optional[str] = None
    manufacturer: Optional[str] = None


class CastService:
    def __init__(
        self,
        *,
        public_host: str,
        public_port: int,
        detect_primary_ipv4_host,
    ) -> None:
        self._public_host = (public_host or "").strip()
        self._public_port = int(public_port)
        self._detect_primary_ipv4_host = detect_primary_ipv4_host

    @staticmethod
    def ip_from_url(url: Optional[str]) -> Optional[str]:
        if not url:
            return None
        if url.startswith("cast://"):
            return url[len("cast://") :].strip("/") or None
        return None

    def _effective_public_host(self) -> str:
        host = self._public_host or "127.0.0.1"
        if host in {"127.0.0.1", "localhost"}:
            detected = self._detect_primary_ipv4_host()
            if detected:
                host = detected
        return host

    def stream_url(self, channel_id: str) -> str:
        host = self._effective_public_host()
        return f"http://{host}:{self._public_port}/api/cast/stream/{channel_id}"

    async def discover(self) -> list[dict]:
        def _discover_sync() -> list[dict]:
            chromecasts = []
            browser = None
            try:
                chromecasts, browser = pychromecast.get_chromecasts()
                for cast in chromecasts:
                    try:
                        cast.wait()
                    except Exception:
                        pass
                items: list[dict] = []
                for cast in chromecasts:
                    device = cast.device
                    name = (device.friendly_name or cast.name or cast.host or "Cast device").strip()
                    uuid = str(device.uuid) if getattr(device, "uuid", None) else None
                    items.append(
                        {
                            "host": name,
                            "url": f"cast://{cast.host}",
                            "fingerprint": uuid,
                            "kind": "cast",
                            "model": getattr(device, "model_name", None),
                            "manufacturer": getattr(device, "manufacturer", None),
                            "ip": cast.host,
                        }
                    )
                return items
            finally:
                if browser is not None:
                    try:
                        browser.stop_discovery()
                    except Exception:
                        pass
                for cast in chromecasts or []:
                    try:
                        cast.disconnect()
                    except Exception:
                        pass

        return await asyncio.to_thread(_discover_sync)

    async def fetch_device(self, ip: str) -> Optional[CastDevice]:
        if not ip:
            return None

        def _fetch_sync() -> Optional[CastDevice]:
            cast = None
            try:
                cast = pychromecast.Chromecast(ip)
                cast.wait()
                device = cast.device
                name = (device.friendly_name or cast.name or cast.host or "Cast device").strip()
                uuid = str(device.uuid) if getattr(device, "uuid", None) else None
                return CastDevice(
                    name=name,
                    host=cast.host,
                    uuid=uuid,
                    model=getattr(device, "model_name", None),
                    manufacturer=getattr(device, "manufacturer", None),
                )
            except Exception as exc:
                log.warning("Failed to read cast device info at %s: %s", ip, exc)
                return None
            finally:
                if cast is not None:
                    try:
                        cast.disconnect()
                    except Exception:
                        pass

        return await asyncio.to_thread(_fetch_sync)

    async def play_stream(self, ip: str, channel_id: str, *, title: Optional[str] = None) -> None:
        if not ip:
            raise ValueError("Missing cast host")
        url = self.stream_url(channel_id)

        def _play_sync() -> None:
            cast = None
            try:
                cast = pychromecast.Chromecast(ip)
                cast.wait()
                media = cast.media_controller
                media.play_media(url, "audio/mpeg", title=title or "RoomCast")
                media.block_until_active()
            finally:
                if cast is not None:
                    try:
                        cast.disconnect()
                    except Exception:
                        pass

        await asyncio.to_thread(_play_sync)

    async def stop(self, ip: str) -> None:
        if not ip:
            return

        def _stop_sync() -> None:
            cast = None
            try:
                cast = pychromecast.Chromecast(ip)
                cast.wait()
                cast.media_controller.stop()
            finally:
                if cast is not None:
                    try:
                        cast.disconnect()
                    except Exception:
                        pass

        await asyncio.to_thread(_stop_sync)

    async def set_volume(self, ip: str, percent: int) -> None:
        if not ip:
            raise ValueError("Missing cast host")
        value = max(0, min(100, int(percent))) / 100.0

        def _set_volume_sync() -> None:
            cast = None
            try:
                cast = pychromecast.Chromecast(ip)
                cast.wait()
                cast.set_volume(value)
            finally:
                if cast is not None:
                    try:
                        cast.disconnect()
                    except Exception:
                        pass

        await asyncio.to_thread(_set_volume_sync)

    async def set_mute(self, ip: str, muted: bool) -> None:
        if not ip:
            raise ValueError("Missing cast host")

        def _set_mute_sync() -> None:
            cast = None
            try:
                cast = pychromecast.Chromecast(ip)
                cast.wait()
                cast.set_volume_muted(bool(muted))
            finally:
                if cast is not None:
                    try:
                        cast.disconnect()
                    except Exception:
                        pass

        await asyncio.to_thread(_set_mute_sync)
