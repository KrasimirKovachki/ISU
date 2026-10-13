#!/usr/bin/env python3
"""Update the API tables after imports — only the events that changed (db/40_tables_instead_of_copies.sql).

Created 2026-10-13 by Claude (AI assistant). Replaces the full rebuild (db/refresh_api_mvs.sql) once db/40 is applied.

  python scripts/refresh_events.py              # events that are new or gone since the last update
  python scripts/refresh_events.py 444 445      # these events again (e.g. after a re-import or a data fix)
  python scripts/refresh_events.py --all        # full rebuild (same result as before db/40; minutes)

New / gone = event keys in api.src_mv_dim_event (what the imported data says) vs api.mv_dim_event (what the API
serves). Prints a JSON line with the timings returned by api.refresh_events().
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from skating_api.db import connect  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("events", nargs="*", type=int, help="event keys to refresh (default: new or gone ones)")
    ap.add_argument("--all", action="store_true", help="rebuild everything")
    args = ap.parse_args()

    conn = connect()
    if args.all:
        conn.execute("SELECT api.rebuild_all()")
        conn.commit()
        print(json.dumps({"rebuilt": "all"}))
        return 0

    events = args.events
    if not events:
        events = [r[0] for r in conn.execute("""
            SELECT event_key FROM api.src_mv_dim_event EXCEPT SELECT event_key FROM api.mv_dim_event
            UNION
            SELECT event_key FROM api.mv_dim_event EXCEPT SELECT event_key FROM api.src_mv_dim_event
            ORDER BY 1""").fetchall()]
    if not events:
        print(json.dumps({"events": [], "note": "nothing new"}))
        return 0
    timing = conn.execute("SELECT api.refresh_events(%s::bigint[])", (events,)).fetchone()[0]
    # competitions published in several parts (db/44): regroup after new / gone events
    if conn.execute("SELECT to_regproc('api.rebuild_event_parts') IS NOT NULL").fetchone()[0]:
        timing["event_parts"] = conn.execute("SELECT api.rebuild_event_parts()").fetchone()[0]
    conn.commit()
    print(json.dumps({"events": events, "timing": timing}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
