from __future__ import annotations

from typing import Any, Callable

from fastapi import HTTPException


class ChannelsService:
    def __init__(
        self,
        *,
        get_channel: Callable[[str], dict],
        sanitize_channel_color: Callable[[Any], Any],
        normalize_spotify_source_id: Callable[[Any], str | None],
        require_spotify_provider: Callable[[], Any],
        get_spotify_source: Callable[[str], dict],
        require_radio_provider: Callable[[], Any],
        radio_channel_slots_active: Callable[[], list[dict]],
        nodes: Callable[[], dict],
        resolve_node_channel_id: Callable[[dict], str | None],
        channels_by_id: Callable[[], dict],
        normalize_radio_state: Callable[[dict], dict],
        default_radio_state: Callable[[], dict],
        ensure_radio_state: Callable[[dict], dict],
        abs_channel_slots: list[dict],
        require_audiobookshelf_provider: Callable[[], Any],
        normalize_abs_state: Callable[[Any], dict],
        save_channels: Callable[[], Any],
        mark_radio_assignments_dirty: Callable[[], Any],
        mark_abs_assignments_dirty: Callable[[], Any],
    ) -> None:
        self._get_channel = get_channel
        self._sanitize_channel_color = sanitize_channel_color
        self._normalize_spotify_source_id = normalize_spotify_source_id
        self._require_spotify_provider = require_spotify_provider
        self._get_spotify_source = get_spotify_source
        self._require_radio_provider = require_radio_provider
        self._radio_channel_slots_active = radio_channel_slots_active
        self._nodes = nodes
        self._resolve_node_channel_id = resolve_node_channel_id
        self._channels_by_id = channels_by_id
        self._normalize_radio_state = normalize_radio_state
        self._default_radio_state = default_radio_state
        self._ensure_radio_state = ensure_radio_state
        self._abs_channel_slots = abs_channel_slots
        self._require_audiobookshelf_provider = require_audiobookshelf_provider
        self._normalize_abs_state = normalize_abs_state
        self._save_channels = save_channels
        self._mark_radio_assignments_dirty = mark_radio_assignments_dirty
        self._mark_abs_assignments_dirty = mark_abs_assignments_dirty

    def update_channel_metadata(self, channel_id: str, updates: dict) -> dict:
        channel = self._get_channel(channel_id)
        was_radio_channel = (channel.get("source") or "").strip().lower() == "radio"
        was_abs_channel = (channel.get("source") or "").strip().lower() == "audiobookshelf"
        routing_changed = False
        previous_snap_stream = (channel.get("snap_stream") or "").strip()

        if "name" in updates:
            name = (updates.get("name") or "").strip()
            if not name:
                raise HTTPException(status_code=400, detail="Channel name cannot be empty")
            channel["name"] = name

        if "color" in updates:
            channel["color"] = self._sanitize_channel_color(updates.get("color"))

        if "snap_stream" in updates:
            snap_stream = (updates.get("snap_stream") or "").strip()
            if not snap_stream:
                raise HTTPException(status_code=400, detail="Snapstream name cannot be empty")
            channel["snap_stream"] = snap_stream
            routing_changed = routing_changed or (snap_stream != previous_snap_stream)

        if "enabled" in updates:
            channel["enabled"] = bool(updates.get("enabled"))

        if "source_ref" in updates:
            raw_ref = (updates.get("source_ref") or "").strip().lower()
            if not raw_ref:
                channel["source"] = "none"
                channel["source_ref"] = None
                channel["snap_stream"] = f"Spotify_CH{channel.get('order', 1)}"
                channel["fifo_path"] = f"/tmp/snapfifo-{channel['id']}"
                channel.pop("radio_state", None)
                channel.pop("abs_state", None)
                routing_changed = routing_changed or (channel["snap_stream"] != previous_snap_stream)
            else:
                requested_spotify = self._normalize_spotify_source_id(raw_ref)
                requested_radio = raw_ref.startswith("radio")
                requested_abs = raw_ref.startswith("audiobookshelf")

            if raw_ref and requested_spotify:
                self._require_spotify_provider()
                source = self._get_spotify_source(requested_spotify)
                channel["source"] = "spotify"
                channel["source_ref"] = requested_spotify
                # Route this logical channel to the selected Spotify source stream.
                channel["snap_stream"] = source["snap_stream"]
                if was_radio_channel:
                    # Radio FIFO mappings are not used for Spotify channels.
                    channel["fifo_path"] = channel.get("fifo_path") or f"/tmp/snapfifo-{channel['id']}"
                routing_changed = routing_changed or (channel["snap_stream"] != previous_snap_stream)

            elif raw_ref and requested_radio:
                self._require_radio_provider()
                # Limited radio pipelines: allocate a snapserver/fifo slot.
                cid = channel.get("id")

                def _channel_has_assigned_nodes(target_id: str) -> bool:
                    for node in self._nodes().values():
                        if self._resolve_node_channel_id(node) == target_id:
                            return True
                    return False

                def _is_radio_active(entry: dict) -> bool:
                    # Only reserve a radio slot if the other channel is actually
                    # configured (tuned). This avoids
                    # "stale" radio channels permanently blocking all slots.
                    state = entry.get("radio_state")
                    normalized = (
                        self._normalize_radio_state(state)
                        if isinstance(state, dict)
                        else self._default_radio_state()
                    )
                    if not normalized.get("stream_url"):
                        return False
                    # Even if paused, keep the slot reserved so channels don't
                    # end up sharing a fifo/snap stream.
                    return True

                used_streams: set[str] = set()
                for other_id, other in self._channels_by_id().items():
                    if other_id == cid:
                        continue
                    if (other.get("source") or "").strip().lower() != "radio":
                        continue
                    if not _is_radio_active(other):
                        continue
                    stream = (other.get("snap_stream") or "").strip()
                    if stream:
                        used_streams.add(stream)

                current_stream = (channel.get("snap_stream") or "").strip()
                current_slot = None
                for slot in self._radio_channel_slots_active():
                    if slot.get("snap_stream") == current_stream:
                        current_slot = slot
                        break

                chosen_slot = None
                if current_slot and current_stream and current_stream not in used_streams:
                    chosen_slot = current_slot
                else:
                    for slot in self._radio_channel_slots_active():
                        stream = (slot.get("snap_stream") or "").strip()
                        if not stream:
                            continue
                        if stream in used_streams:
                            continue
                        chosen_slot = slot
                        break

                if not chosen_slot:
                    raise HTTPException(
                        status_code=409,
                        detail=(
                            f"No radio slots available (max {len(self._radio_channel_slots_active())}). "
                            "Switch another channel away from Radio first."
                        ),
                    )

                channel["source"] = "radio"
                channel["source_ref"] = "radio"
                channel["snap_stream"] = chosen_slot["snap_stream"]
                channel["fifo_path"] = chosen_slot["fifo_path"]
                self._ensure_radio_state(channel)
                routing_changed = routing_changed or (channel["snap_stream"] != previous_snap_stream)

            elif raw_ref and requested_abs:
                self._require_audiobookshelf_provider()

                cid = channel.get("id")

                def _channel_has_assigned_nodes(target_id: str) -> bool:
                    for node in self._nodes().values():
                        if self._resolve_node_channel_id(node) == target_id:
                            return True
                    return False

                def _is_abs_active(entry: dict) -> bool:
                    if (entry.get("enabled") is None) or bool(entry.get("enabled")):
                        return True
                    entry_id = entry.get("id")
                    return bool(entry_id and _channel_has_assigned_nodes(entry_id))

                used_streams: set[str] = set()
                for other_id, other in self._channels_by_id().items():
                    if other_id == cid:
                        continue
                    if (other.get("source") or "").strip().lower() != "audiobookshelf":
                        continue
                    if not _is_abs_active(other):
                        continue
                    stream = (other.get("snap_stream") or "").strip()
                    if stream:
                        used_streams.add(stream)

                current_stream = (channel.get("snap_stream") or "").strip()
                current_slot = None
                for slot in self._abs_channel_slots:
                    if slot.get("snap_stream") == current_stream:
                        current_slot = slot
                        break

                chosen_slot = None
                if current_slot and current_stream and current_stream not in used_streams:
                    chosen_slot = current_slot
                else:
                    for slot in self._abs_channel_slots:
                        stream = (slot.get("snap_stream") or "").strip()
                        if not stream:
                            continue
                        if stream in used_streams:
                            continue
                        chosen_slot = slot
                        break

                if not chosen_slot:
                    raise HTTPException(
                        status_code=409,
                        detail=(
                            f"No Audiobookshelf slots available (max {len(self._abs_channel_slots)}). "
                            "Switch another channel away from Audiobookshelf first."
                        ),
                    )

                channel["source"] = "audiobookshelf"
                channel["source_ref"] = "audiobookshelf"
                channel["snap_stream"] = chosen_slot["snap_stream"]
                channel["fifo_path"] = chosen_slot["fifo_path"]
                channel["abs_state"] = self._normalize_abs_state(channel.get("abs_state"))
                routing_changed = routing_changed or (channel["snap_stream"] != previous_snap_stream)

            elif raw_ref:
                raise HTTPException(status_code=400, detail="Invalid source_ref")

        if "order" in updates:
            try:
                channel["order"] = max(1, int(updates["order"]))
            except (TypeError, ValueError):
                raise HTTPException(status_code=400, detail="Order must be a positive integer")

        self._save_channels()

        now_radio_channel = (channel.get("source") or "").strip().lower() == "radio"
        now_abs_channel = (channel.get("source") or "").strip().lower() == "audiobookshelf"
        if was_radio_channel or now_radio_channel:
            self._mark_radio_assignments_dirty()
        if was_abs_channel or now_abs_channel:
            self._mark_abs_assignments_dirty()

        result = dict(channel)
        result["_routing_changed"] = routing_changed
        return result
