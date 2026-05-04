from __future__ import annotations

from collections.abc import Iterator

import psycopg

from isu_parser.local_config import database_dsn


def connect() -> psycopg.Connection:
    return psycopg.connect(database_dsn())


def connection_dependency() -> Iterator[psycopg.Connection]:
    with connect() as conn:
        yield conn
