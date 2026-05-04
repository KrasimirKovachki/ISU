from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen


API_BASE = "https://api.isu-skating.com/api/"
USER_AGENT = "isu-skating-api-discovery/0.1"


@dataclass(frozen=True)
class ApiPage:
    items: list[dict[str, Any]]
    page: int
    total_pages: int | None
    total_items: int | None
    raw: dict[str, Any]


def post_form(endpoint: str, fields: dict[str, Any]) -> dict[str, Any]:
    data = urlencode({k: v for k, v in fields.items() if v is not None}).encode("utf-8")
    request = Request(
        API_BASE + endpoint.lstrip("/"),
        data=data,
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "User-Agent": USER_AGENT,
        },
        method="POST",
    )
    with urlopen(request, timeout=45) as response:
        return json.loads(response.read().decode("utf-8"))


def post_json(endpoint: str, payload: dict[str, Any]) -> dict[str, Any]:
    data = json.dumps(payload).encode("utf-8")
    request = Request(
        API_BASE + endpoint.lstrip("/"),
        data=data,
        headers={
            "Content-Type": "application/json",
            "User-Agent": USER_AGENT,
        },
        method="POST",
    )
    with urlopen(request, timeout=45) as response:
        return json.loads(response.read().decode("utf-8"))


def extract_page(raw: dict[str, Any], page: int = 1) -> ApiPage:
    data = raw.get("data")
    if isinstance(data, dict):
        item_container = data
    else:
        item_container = raw

    items = None
    for key in ("data", "items", "records", "results", "list"):
        value = item_container.get(key) if isinstance(item_container, dict) else None
        if isinstance(value, list):
            items = value
            break
    if items is None and isinstance(data, list):
        items = data
    if items is None:
        items = []

    def int_or_none(value: Any) -> int | None:
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    return ApiPage(
        items=[item for item in items if isinstance(item, dict)],
        page=page,
        total_pages=int_or_none(item_container.get("total_page") if isinstance(item_container, dict) else None)
        or int_or_none(raw.get("total_page")),
        total_items=int_or_none(item_container.get("total_items") if isinstance(item_container, dict) else None)
        or int_or_none(raw.get("total_items")),
        raw=raw,
    )


def truthy(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return bool(value)
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"1", "true", "yes", "y"}:
            return True
        if lowered in {"0", "false", "no", "n"}:
            return False
    return None


def first_present(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return value
    return None


def direct_result_urls(row: dict[str, Any]) -> list[dict[str, str]]:
    urls: list[dict[str, str]] = []
    for index in range(1, 4):
        url = row.get(f"button_{index}_url")
        if isinstance(url, str) and url.startswith(("http://", "https://")):
            urls.append(
                {
                    "button": f"button_{index}",
                    "title": row.get(f"button_{index}_title") or "",
                    "url": url,
                }
            )
    return urls
