from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .fs_manager import classify_source_context


DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[1] / "config" / "source_profiles.json"


def load_source_profiles(path: str | Path = DEFAULT_CONFIG_PATH) -> list[dict[str, Any]]:
    with Path(path).open("r", encoding="utf-8") as file:
        return json.load(file)["profiles"]


def find_source_profile(url: str, path: str | Path = DEFAULT_CONFIG_PATH) -> dict[str, Any] | None:
    context = classify_source_context(url)
    for profile in load_source_profiles(path):
        match = profile.get("match", {})
        if match.get("host") and match["host"] != context["host"]:
            continue
        prefix = match.get("event_path_prefix")
        event_path = context.get("event_path") or ""
        if prefix and not event_path.startswith(prefix):
            continue
        return profile
    return None


def representation_settings_for_url(url: str) -> dict[str, Any]:
    profile = find_source_profile(url)
    if not profile:
        return {"primary": "nation", "store_nation": True, "store_club": True}
    return profile.get("representation", {"primary": "nation", "store_nation": True, "store_club": True})

