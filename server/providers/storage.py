from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional


log = logging.getLogger("roomcast")


@dataclass
class ProviderState:
    id: str
    enabled: bool = True
    settings: Dict[str, Any] | None = None

    def to_json(self) -> dict:
        payload: dict = {"id": self.id, "enabled": bool(self.enabled)}
        if isinstance(self.settings, dict):
            payload["settings"] = self.settings
        return payload


def _normalize_provider_id(value: Any) -> Optional[str]:
    if not isinstance(value, str):
        return None
    raw = value.strip().lower()
    if not raw:
        return None
    # Older builds sometimes persisted instance-style ids (e.g. "radio:1").
    # Providers are now single logical entries keyed by base id.
    return raw.split(":", 1)[0] or None


def load_providers(path: Path) -> Dict[str, ProviderState]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text())
    except json.JSONDecodeError:
        log.warning("providers.json is invalid; ignoring")
        return {}
    if not isinstance(data, list):
        return {}
    result: Dict[str, ProviderState] = {}
    for raw in data:
        if not isinstance(raw, dict):
            continue
        pid = _normalize_provider_id(raw.get("id"))
        if not pid:
            continue
        enabled_raw = raw.get("enabled")
        if enabled_raw is None:
            enabled = True
        elif isinstance(enabled_raw, str):
            enabled = enabled_raw.strip().lower() not in {"0", "false", "no"}
        else:
            enabled = bool(enabled_raw)
        settings = raw.get("settings") if isinstance(raw.get("settings"), dict) else {}
        result[pid] = ProviderState(id=pid, enabled=enabled, settings=settings)
    return result


def save_providers(path: Path, providers: Dict[str, ProviderState]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    ordered: List[dict] = []
    for pid in sorted(providers.keys()):
        ordered.append(providers[pid].to_json())
    path.write_text(json.dumps(ordered, indent=2))


def infer_providers(
    channels_path: Path,
    sources_path: Path,
) -> Dict[str, ProviderState]:
    """Infer providers for backwards compatibility.

    New installs (no channels/sources files) will infer none.
    Existing installs will infer Spotify if sources exist, and infer Radio/
    Audiobookshelf if any channel is set to those sources.
    """

    inferred: Dict[str, ProviderState] = {}

    if sources_path.exists():
        try:
            data = json.loads(sources_path.read_text())
        except json.JSONDecodeError:
            data = []
        if isinstance(data, list) and any(isinstance(item, dict) and str(item.get("kind") or "").lower() == "spotify" for item in data):
            inferred["spotify"] = ProviderState(id="spotify", enabled=True, settings={"instances": 1})

    if channels_path.exists():
        try:
            data = json.loads(channels_path.read_text())
        except json.JSONDecodeError:
            data = []
        if isinstance(data, list) and any(isinstance(item, dict) and str(item.get("source") or "").lower() == "radio" for item in data):
            inferred["radio"] = ProviderState(id="radio", enabled=True, settings={})
        if isinstance(data, list) and any(
            isinstance(item, dict) and str(item.get("source") or "").lower() == "audiobookshelf" for item in data
        ):
            inferred["audiobookshelf"] = ProviderState(id="audiobookshelf", enabled=True, settings={})

    return inferred
