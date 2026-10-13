#!/usr/bin/env python3
# Edited 2026-10-12 by Claude (AI assistant): copy profiles from config/source_profiles.json into ingest.source_profiles
# (the import picks the parser + representation from the DB row with the longest matching host/path prefix, while the
# parsers read the JSON — both must agree). Updates the row with the same host + prefix, inserts when there is none.
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import psycopg
from psycopg.types.json import Jsonb

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from isu_parser.local_config import database_dsn
from isu_parser.source_profiles import DEFAULT_CONFIG_PATH, load_source_profiles


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prefix", default="ec_", help="Only profiles whose id starts with this.")
    args = parser.parse_args()
    profiles = [p for p in load_source_profiles(DEFAULT_CONFIG_PATH) if p["id"].startswith(args.prefix)]
    updated = inserted = 0
    with psycopg.connect(database_dsn()) as conn, conn.cursor() as cur:
        for p in profiles:
            host, prefix = p["match"]["host"], p["match"]["event_path_prefix"]
            settings = {"representation": p["representation"], "source_family": "europeancriterium" if p["id"].startswith("ec_") else None,
                        "source_system": p["parser_profile"], "config_id": p["id"]}
            cur.execute(
                """UPDATE ingest.source_profiles SET parser_profile = %s, competition_stream = %s,
                       representation_primary = %s, settings = %s, updated_at = now()
                   WHERE host = %s AND event_path_prefix = %s""",
                (p["parser_profile"], p["competition_stream"], p["representation"]["primary"], Jsonb(settings), host, prefix),
            )
            if cur.rowcount:
                updated += cur.rowcount
                continue
            cur.execute(
                """INSERT INTO ingest.source_profiles (profile_key, parser_profile, host, event_path_prefix,
                       competition_stream, representation_primary, settings)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                (p["id"], p["parser_profile"], host, prefix, p["competition_stream"], p["representation"]["primary"], Jsonb(settings)),
            )
            inserted += 1
    print(json.dumps({"profiles": len(profiles), "updated": updated, "inserted": inserted}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
