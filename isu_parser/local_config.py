from __future__ import annotations

import os
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_LOCAL_CONFIG = PROJECT_ROOT / "config" / "local.env"


def load_env_file(path: str | Path | None = None) -> dict[str, str]:
    config_path = Path(path) if path else DEFAULT_LOCAL_CONFIG
    if not config_path.exists():
        return {}

    values: dict[str, str] = {}
    for raw_line in config_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key:
            values[key] = value
    return values


def database_dsn(path: str | Path | None = None) -> str:
    values = load_env_file(path)
    direct_dsn = os.environ.get("SKATING_DATABASE_URL") or values.get("SKATING_DATABASE_URL")
    if direct_dsn:
        return direct_dsn

    host = os.environ.get("SKATING_DB_HOST") or values.get("SKATING_DB_HOST", "127.0.0.1")
    port = os.environ.get("SKATING_DB_PORT") or values.get("SKATING_DB_PORT", "5432")
    database = os.environ.get("SKATING_DB_NAME") or values.get("SKATING_DB_NAME", "skating_data")
    user = os.environ.get("SKATING_DB_USER") or values.get("SKATING_DB_USER", "skating_app")
    password = os.environ.get("SKATING_DB_PASSWORD") or values.get("SKATING_DB_PASSWORD", "")

    auth = user if not password else f"{user}:{password}"
    return f"postgresql://{auth}@{host}:{port}/{database}"
