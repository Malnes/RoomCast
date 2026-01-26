from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional


@dataclass(frozen=True)
class ProviderSpec:
    id: str
    name: str
    description: str
    has_settings: bool = True


# Keep this list small and explicit; adding a new provider should be a deliberate change.
AVAILABLE_PROVIDERS: Dict[str, ProviderSpec] = {
    "spotify": ProviderSpec(
        id="spotify",
        name="Spotify",
        description="",
        has_settings=True,
    ),
    "radio": ProviderSpec(
        id="radio",
        name="Radio",
        description="Internet radio stations via Radio Browser.",
        has_settings=True,
    ),
    "audiobookshelf": ProviderSpec(
        id="audiobookshelf",
        name="Audiobookshelf",
        description="Podcasts and audiobooks via an Audiobookshelf server.",
        has_settings=True,
    ),
}


def get_provider_spec(provider_id: str) -> Optional[ProviderSpec]:
    return AVAILABLE_PROVIDERS.get((provider_id or "").strip().lower())
