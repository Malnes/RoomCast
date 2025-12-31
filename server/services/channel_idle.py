from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from typing import Any, Optional

from fastapi import HTTPException


class ChannelIdleService:
    def __init__(
        self,
        *,
        channel_order: list[str],
        channels_by_id: dict[str, dict],
        nodes: dict,
        snapcast: Any,
        get_webrtc_relay: Callable[[], Any],
        ensure_radio_state: Callable[[dict], dict],
        save_channels: Callable[[], None],
        mark_radio_assignments_dirty: Callable[[], None],
        radio_runtime_status: dict[str, dict],
        spotify_pause: Callable[[str], Awaitable[None]],
        parse_spotify_error: Callable[[object], dict],
        idle_timeout: float,
        poll_interval: float,
        logger: Any,
    ) -> None:
        self._channel_order = channel_order
        self._channels_by_id = channels_by_id
        self._nodes = nodes
        self._snapcast = snapcast
        self._get_webrtc_relay = get_webrtc_relay
        self._ensure_radio_state = ensure_radio_state
        self._save_channels = save_channels
        self._mark_radio_assignments_dirty = mark_radio_assignments_dirty
        self._radio_runtime_status = radio_runtime_status
        self._spotify_pause = spotify_pause
        self._parse_spotify_error = parse_spotify_error
        self._idle_timeout = float(idle_timeout)
        self._poll_interval = float(poll_interval)
        self._log = logger
        self._state: dict[str, dict] = {}

    def _channel_should_monitor(self, channel: dict) -> bool:
        if not channel.get("enabled", True):
            return False
        if not channel.get("snap_stream"):
            return False
        source = (channel.get("source") or "spotify").lower()
        if source == "radio":
            state = self._ensure_radio_state(channel)
            return bool(state.get("playback_enabled", True) and state.get("stream_url"))
        return True

    def _channel_has_active_hardware_listeners(self, channel_id: str) -> bool:
        if not channel_id:
            return False
        for node in self._nodes.values():
            if node.get("type") == "browser":
                continue
            if node.get("channel_id") != channel_id:
                continue
            if node.get("online"):
                return True
        return False

    async def _collect_channel_listener_counts(self) -> tuple[dict[str, int], bool]:
        """Return (per-channel listeners, has_any_data)."""
        counts: dict[str, int] = {cid: 0 for cid in self._channel_order}
        stream_to_channel: dict[str, str] = {}
        for cid in self._channel_order:
            channel = self._channels_by_id.get(cid)
            if not channel:
                continue
            stream_id = (channel.get("snap_stream") or "").strip()
            if stream_id:
                stream_to_channel[stream_id] = cid
        data_sources = 0
        try:
            clients = await self._snapcast.list_clients()
        except Exception as exc:  # pragma: no cover - network dependency
            self._log.warning("Channel idle monitor: failed to list snapclients: %s", exc)
            clients = None
        if clients is not None:
            data_sources += 1
            for client in clients:
                stream_id = client.get("_stream_id")
                if not stream_id:
                    continue
                cid = stream_to_channel.get(stream_id)
                if not cid or not client.get("connected"):
                    continue
                counts[cid] = counts.get(cid, 0) + 1
        relay = self._get_webrtc_relay()
        if relay:
            try:
                webrtc_counts = await relay.channel_listener_counts()
            except Exception as exc:  # pragma: no cover - defensive logging
                self._log.warning("Channel idle monitor: failed to read WebRTC listeners: %s", exc)
            else:
                data_sources += 1
                for cid, value in (webrtc_counts or {}).items():
                    if value:
                        counts[cid] = counts.get(cid, 0) + int(value)
        return counts, data_sources > 0

    async def _stop_channel_due_to_idle(self, channel: dict) -> bool:
        cid = channel.get("id") or ""
        if not cid:
            return False
        source = (channel.get("source") or "spotify").lower()
        if source == "radio":
            state = self._ensure_radio_state(channel)
            if not state.get("playback_enabled", True):
                return False
            now = int(time.time())
            state["playback_enabled"] = False
            state["updated_at"] = now
            channel["radio_state"] = state
            self._save_channels()
            self._mark_radio_assignments_dirty()
            self._radio_runtime_status[cid] = {
                "state": "idle",
                "message": "Radio stopped (no listeners)",
                "bitrate": None,
                "station_id": state.get("station_id"),
                "metadata": None,
                "updated_at": now,
                "started_at": None,
            }
            self._log.info(
                "Auto-stopped radio channel %s after %.0f seconds without listeners",
                cid,
                self._idle_timeout,
            )
            return True
        try:
            await self._spotify_pause(cid)
            self._log.info(
                "Auto-paused Spotify channel %s after %.0f seconds without listeners",
                cid,
                self._idle_timeout,
            )
            return True
        except HTTPException as exc:
            parsed = self._parse_spotify_error(exc.detail)
            reason = (parsed.get("reason") or "").strip().lower()
            message = (parsed.get("message") or "").strip()
            detail_text = message or (exc.detail if isinstance(exc.detail, str) else str(exc.detail))
            if reason == "no_active_device" or (
                isinstance(detail_text, str) and "no active device" in detail_text.lower()
            ):
                self._log.debug("Auto-stop skipped for Spotify channel %s: no active device", cid)
                return True
            self._log.warning("Auto-stop failed for Spotify channel %s: %s", cid, detail_text)
            return False
        except Exception:  # pragma: no cover - defensive
            self._log.exception("Auto-stop failed for Spotify channel %s", cid)
            return False

    async def evaluate_once(self) -> None:
        counts, has_data = await self._collect_channel_listener_counts()
        if not has_data:
            return
        now = time.time()
        tracked: set[str] = set()
        for cid in self._channel_order:
            channel = self._channels_by_id.get(cid)
            if not channel:
                self._state.pop(cid, None)
                continue
            if not self._channel_should_monitor(channel):
                self._state.pop(cid, None)
                continue
            tracked.add(cid)
            listeners = counts.get(cid, 0)
            if listeners <= 0 and self._channel_has_active_hardware_listeners(cid):
                listeners = 1
            state = self._state.setdefault(cid, {"idle_since": None, "stopped": False})
            if listeners > 0:
                state["idle_since"] = None
                state["stopped"] = False
                state["last_active"] = now
                continue
            if state.get("idle_since") is None:
                state["idle_since"] = now
            idle_for = now - (state.get("idle_since") or now)
            if idle_for < self._idle_timeout or state.get("stopped"):
                continue
            stopped = await self._stop_channel_due_to_idle(channel)
            if stopped:
                state["stopped"] = True
        for cid in list(self._state.keys()):
            if cid not in tracked:
                self._state.pop(cid, None)

    async def loop(self) -> None:
        try:
            while True:
                try:
                    await self.evaluate_once()
                except Exception:
                    self._log.exception("Channel idle monitor iteration failed")
                await asyncio.sleep(self._poll_interval)
        except asyncio.CancelledError:
            pass
