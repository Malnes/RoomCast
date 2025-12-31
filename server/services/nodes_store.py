from __future__ import annotations

import json
import time
import uuid
from pathlib import Path
from typing import Any, Callable, Dict, Optional


class NodesStore:
    def __init__(
        self,
        *,
        nodes_path: Path,
        transient_node_fields: set[str],
        normalize_section_name: Callable[[Any], str],
        normalize_node_url: Callable[[str], str],
        default_eq_state: Callable[[], dict],
        default_eq_preset: str,
        normalize_percent: Callable[..., int],
        select_initial_channel_id: Callable[..., Optional[str]],
        find_section: Callable[[Optional[str]], Optional[dict]],
    ) -> None:
        self._nodes_path = nodes_path
        self._transient_node_fields = transient_node_fields
        self._normalize_section_name = normalize_section_name
        self._normalize_node_url = normalize_node_url
        self._default_eq_state = default_eq_state
        self._default_eq_preset = default_eq_preset
        self._normalize_percent = normalize_percent
        self._select_initial_channel_id = select_initial_channel_id
        self._find_section = find_section

    def load(self) -> tuple[dict, list]:
        if not self._nodes_path.exists():
            return {}, []

        try:
            raw = json.loads(self._nodes_path.read_text())
            if isinstance(raw, dict):
                data = raw.get("nodes") or []
                section_data = raw.get("sections") or []
            elif isinstance(raw, list):
                data = raw
                section_data = []
            else:
                data = []
                section_data = []

            sections: list[dict] = []
            seen_section_ids: set[str] = set()
            if isinstance(section_data, list):
                for entry in section_data:
                    if not isinstance(entry, dict):
                        continue
                    sid = entry.get("id") or str(uuid.uuid4())
                    if sid in seen_section_ids:
                        continue
                    name = self._normalize_section_name(entry.get("name")) or "Section"
                    section = {
                        "id": sid,
                        "name": name,
                        "created_at": entry.get("created_at") or int(time.time()),
                        "updated_at": entry.get("updated_at") or int(time.time()),
                    }
                    sections.append(section)
                    seen_section_ids.add(sid)

            nodes: dict = {}
            next_order_by_group: Dict[str, int] = {}
            for item in data:
                item = dict(item)
                item.pop("pan", None)
                eq = item.get("eq") or self._default_eq_state()
                eq.setdefault("bands", [])
                eq.setdefault("band_count", len(eq["bands"]) or 15)
                eq.setdefault("preset", self._default_eq_preset)
                item["eq"] = eq
                item["url"] = self._normalize_node_url(item.get("url", ""))
                if item.get("type") in {"browser", "sonos"}:
                    item["audio_configured"] = True
                else:
                    item["audio_configured"] = bool(item.get("audio_configured"))
                item["volume_percent"] = self._normalize_percent(item.get("volume_percent", 75), default=75)
                item["max_volume_percent"] = self._normalize_percent(item.get("max_volume_percent", 100), default=100)
                item["muted"] = bool(item.get("muted", False))
                item["updating"] = bool(item.get("updating", False))
                item["playback_device"] = item.get("playback_device")
                outputs = item.get("outputs")
                if not isinstance(outputs, dict):
                    outputs = {}
                item["outputs"] = outputs
                item["fingerprint"] = item.get("fingerprint")
                try:
                    item["last_seen"] = float(item.get("last_seen", 0)) or 0.0
                except (TypeError, ValueError):
                    item["last_seen"] = 0.0
                offline_since = item.get("offline_since")
                try:
                    item["offline_since"] = float(offline_since) if offline_since is not None else None
                except (TypeError, ValueError):
                    item["offline_since"] = None
                if item.get("type") == "browser":
                    item["online"] = True
                else:
                    item["online"] = bool(item.get("online", False))
                if item.get("type") == "sonos":
                    item["sonos_udn"] = item.get("sonos_udn")
                    item["sonos_rincon"] = item.get("sonos_rincon")

                # Persist explicit unassignments:
                # - If the JSON has "channel_id": null, keep it unassigned.
                # - Only auto-assign a default channel when the field is missing entirely
                #   (backwards-compat for older configs / newly discovered nodes).
                if "channel_id" in item:
                    item["channel_id"] = self._select_initial_channel_id(item.get("channel_id"), fallback=False)
                else:
                    item["channel_id"] = self._select_initial_channel_id(
                        None,
                        fallback=item.get("type") != "browser",
                    )

                # Sections normalization: allow unsectioned nodes.
                section_id = item.get("section_id")
                if isinstance(section_id, str):
                    section_id = section_id.strip() or None
                else:
                    section_id = None
                if section_id and not self._find_section(section_id):
                    section_id = None
                item["section_id"] = section_id

                # Stable ordering within a group (section id or unsectioned group).
                group_key = section_id or "__unsectioned__"
                raw_order = item.get("section_order")
                try:
                    order_val = int(raw_order)
                except (TypeError, ValueError):
                    order_val = None
                if order_val is None or order_val < 0:
                    order_val = next_order_by_group.get(group_key, 0)
                item["section_order"] = order_val
                next_order_by_group[group_key] = max(next_order_by_group.get(group_key, 0), order_val + 1)
                snapclient_id = (item.get("snapclient_id") or "").strip() or None
                item["snapclient_id"] = snapclient_id
                nodes[item["id"]] = item

            return nodes, sections
        except Exception:
            return {}, []

    def save(self, *, nodes: dict, sections: list) -> None:
        section_rank = {section.get("id"): idx for idx, section in enumerate(sections) if section.get("id")}

        def _node_key(entry: dict) -> tuple:
            sid = entry.get("section_id")
            rank = section_rank.get(sid, 10**9)
            order = entry.get("section_order")
            try:
                order_val = int(order)
            except (TypeError, ValueError):
                order_val = 10**9
            name_val = (entry.get("name") or "").lower()
            return (rank, order_val, name_val)

        serialized_nodes = []
        for node in sorted(nodes.values(), key=_node_key):
            entry = {k: v for k, v in node.items() if k not in self._transient_node_fields}
            serialized_nodes.append(entry)

        serialized_sections = []
        for section in sections:
            if not isinstance(section, dict):
                continue
            sid = section.get("id")
            name = self._normalize_section_name(section.get("name"))
            if not sid or not name:
                continue
            serialized_sections.append(
                {
                    "id": sid,
                    "name": name,
                    "created_at": section.get("created_at") or int(time.time()),
                    "updated_at": section.get("updated_at") or int(time.time()),
                }
            )

        self._nodes_path.write_text(json.dumps({"sections": serialized_sections, "nodes": serialized_nodes}, indent=2))
