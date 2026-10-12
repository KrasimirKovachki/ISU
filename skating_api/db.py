# Edited 2026-10-12 by Claude (AI assistant): connection pool. Opening a new connection (with password check)
# for every request cost 20–80 ms while the queries themselves take ~1–2 ms.
from __future__ import annotations

import os
from collections.abc import Iterator

import psycopg
from psycopg_pool import ConnectionPool

from isu_parser.local_config import database_dsn

_pool: ConnectionPool | None = None


def connect() -> psycopg.Connection:
    return psycopg.connect(database_dsn())


def pool() -> ConnectionPool:
    global _pool
    if _pool is None:
        _pool = ConnectionPool(
            database_dsn(),
            min_size=int(os.environ.get("SKATING_DB_POOL_MIN", "2")),
            max_size=int(os.environ.get("SKATING_DB_POOL_MAX", "8")),
            max_idle=300,
            open=True,
        )
    return _pool


def close_pool() -> None:
    global _pool
    if _pool is not None:
        _pool.close()
        _pool = None


def connection_dependency() -> Iterator[psycopg.Connection]:
    with pool().connection() as conn:
        yield conn
