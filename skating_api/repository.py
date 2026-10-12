from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any

from psycopg.rows import dict_row


DEFAULT_LIMIT = 25
MAX_LIMIT = 2000

ISO3_TO_ISO2 = {
    "AND": "AD", "ARE": "AE", "ARG": "AR", "ARM": "AM", "AUS": "AU", "AUT": "AT",
    "AZE": "AZ", "BEL": "BE", "BGR": "BG", "BIH": "BA", "BLR": "BY", "BRA": "BR",
    "BUL": "BG", "CAN": "CA", "CHE": "CH", "CHN": "CN", "CRO": "HR", "CYP": "CY",
    "CZE": "CZ", "DEN": "DK", "ESP": "ES", "EST": "EE", "FIN": "FI", "FRA": "FR",
    "GBR": "GB", "GEO": "GE", "GER": "DE", "GRE": "GR", "HKG": "HK", "HUN": "HU",
    "IRL": "IE", "ISL": "IS", "ISR": "IL", "ITA": "IT", "JPN": "JP", "KAZ": "KZ",
    "KOR": "KR", "LAT": "LV", "LTU": "LT", "LUX": "LU", "MDA": "MD", "MEX": "MX",
    "MKD": "MK", "MNE": "ME", "NED": "NL", "NOR": "NO", "NZL": "NZ", "POL": "PL",
    "POR": "PT", "ROU": "RO", "RUS": "RU", "SRB": "RS", "SLO": "SI", "SVK": "SK",
    "SUI": "CH", "SWE": "SE", "TUR": "TR", "UKR": "UA", "USA": "US", "UZB": "UZ",
}


def public_row(row: dict[str, Any]) -> dict[str, Any]:
    result = {key: public_value(value) for key, value in row.items()}
    nation_code = result.get("nation_code")
    if isinstance(nation_code, str) and nation_code:
        flag = nation_flag(nation_code)
        result["nation_flag"] = flag
        result["nation_display"] = f"{flag} {nation_code}" if flag else nation_code
    country_code = result.get("country_code")
    if isinstance(country_code, str) and country_code:
        flag = nation_flag(country_code)
        result["country_flag"] = flag
        result["country_display"] = f"{flag} {country_code}" if flag else country_code
    return result


def public_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, date | datetime):
        return value.isoformat()
    return value


def nation_flag(nation_code: str) -> str | None:
    iso2 = ISO3_TO_ISO2.get(nation_code.upper())
    if not iso2 or len(iso2) != 2:
        return None
    return "".join(chr(0x1F1E6 + ord(char) - ord("A")) for char in iso2)


def clamp_limit(limit: int | None) -> int:
    if limit is None:
        return DEFAULT_LIMIT
    return max(1, min(limit, MAX_LIMIT))


def fetch_all(conn: Any, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, params or {})
        return [public_row(row) for row in cur.fetchall()]


def fetch_one(conn: Any, sql: str, params: dict[str, Any] | None = None) -> dict[str, Any] | None:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, params or {})
        row = cur.fetchone()
        return public_row(row) if row else None


_SEG_ABBR = {
    "short program": "SP", "free skating": "FS", "free dance": "FD",
    "rhythm dance": "RD", "pattern dance": "PD", "free program": "FP",
}


def _seg_abbr(name: str) -> str:
    return _SEG_ABBR.get((name or "").strip().lower(), "")


def event_schedule(conn: Any, event_key: int) -> dict[str, Any]:
    """Categories + segments for an event, from the parsed ENTRY schedule (schedule_items).

    This is the correct source for seeding a competition's categories/segments — unlike
    results, which only cover categories that were actually scored.
    """
    rows = fetch_all(
        conn,
        """
SELECT category_name, segment_name, MIN(source_order) AS ord
FROM ingest.schedule_items
WHERE event_id = %(e)s AND category_name IS NOT NULL
GROUP BY category_name, segment_name
ORDER BY ord NULLS LAST, category_name
""",
        {"e": event_key},
    )
    cats: dict[str, dict[str, Any]] = {}
    seg_order: list[str] = []
    for r in rows:
        c, s = r["category_name"], r["segment_name"]
        entry = cats.setdefault(c, {"name": c, "segments": []})
        if s and s not in [x["name"] for x in entry["segments"]]:
            entry["segments"].append({"name": s, "abbr": _seg_abbr(s)})
        if s and s not in seg_order:
            seg_order.append(s)
    return {
        "event_key": event_key,
        "categories": list(cats.values()),
        "segments": [{"name": s, "abbr": _seg_abbr(s)} for s in seg_order],
    }


def api_meta(conn: Any) -> dict[str, Any]:
    """Freshness + size for the /meta endpoint (so consumers can show data age)."""
    return fetch_one(
        conn,
        """
SELECT
  (SELECT count(*) FROM api.mv_dim_event)               AS events,
  (SELECT count(*) FROM api.mv_skater_search)            AS skaters,
  (SELECT count(*) FROM core.clubs)                      AS clubs,
  (SELECT max(started_at) FROM ingest.import_runs)       AS last_import_at,
  (SELECT max(event_start_date) FROM api.mv_dim_event)   AS latest_event_date
""",
    )


def search_skaters(conn: Any, query: str | None, limit: int | None = None) -> list[dict[str, Any]]:
    where = ""
    params: dict[str, Any] = {"limit": clamp_limit(limit)}
    if query:
        where = """
WHERE search_text ILIKE %(pattern)s
"""
        params["pattern"] = f"%{query.strip().lower()}%"

    return fetch_all(
        conn,
        f"""
SELECT
  skater_key,
  identity_key,
  skater_name,
  nation_code,
  club_name,
  source_appearances,
  first_seen_date,
  latest_seen_date,
  best_tss,
  best_tes,
  best_pcs,
  competitions,
  scored_competitions
FROM api.mv_skater_search
{where}
ORDER BY latest_seen_date DESC NULLS LAST, skater_name
LIMIT %(limit)s
""",
        params,
    )


def dashboard_overview(conn: Any) -> dict[str, Any]:
    return fetch_one(
        conn,
        """
SELECT
  events,
  skaters,
  registered_clubs,
  scoring_clubs,
  segment_scores,
  element_scores,
  development_skaters,
  first_event_date,
  latest_event_date,
  international_events,
  federation_events,
  club_or_open_events
FROM api.mv_dashboard_overview
""",
    ) or {}


def competition_mix(conn: Any) -> list[dict[str, Any]]:
    return fetch_all(
        conn,
        """
SELECT
  scope_hint,
  competition_stream,
  competition_level,
  event_type,
  representation_primary,
  events,
  skaters_with_scores,
  clubs_with_appearances,
  skaters_with_appearances,
  segment_scores,
  first_event_date,
  latest_event_date
FROM api.mv_competition_mix
ORDER BY events DESC, segment_scores DESC, competition_stream
""",
    )


# ---------- Year-range filtered dashboard slices --------------------------
#
# When the user picks a from/to year, we recompute the same shape as the
# all-time dashboard MVs but constrained to events in that window. Slower
# than the MVs (no precomputation) but still <300ms because the underlying
# tables are well-indexed on date_key / event_start_date.

def _date_bounds(from_year: int, to_year: int) -> tuple[str, str]:
    return f"{from_year:04d}-01-01", f"{to_year:04d}-12-31"


def dashboard_overview_period(conn: Any, from_year: int, to_year: int) -> dict[str, Any]:
    start, end = _date_bounds(from_year, to_year)
    return fetch_one(
        conn,
        """
WITH e AS (
  SELECT *
  FROM api.mv_dim_event
  WHERE event_start_date BETWEEN %(start)s AND %(end)s
),
s AS (
  SELECT *
  FROM api.mv_segment_score
  WHERE date_key BETWEEN %(start)s AND %(end)s
),
dev AS (
  SELECT DISTINCT
    il.identity_key
  FROM api.mv_skater_appearances_with_club sac
  LEFT JOIN api.mv_appearance_identity il ON il.skater_appearance_id = sac.skater_appearance_id
  WHERE sac.event_start_date BETWEEN %(start)s AND %(end)s
    AND sac.category_name ILIKE 'Mlad Figurist%%'
)
SELECT
  (SELECT count(*) FROM e) AS events,
  (SELECT count(DISTINCT il.identity_key)
     FROM s
     JOIN api.mv_score_identity il ON il.segment_score_key = s.segment_score_key
     WHERE s.skater_name IS NOT NULL) AS skaters,
  (SELECT count(*) FROM core.clubs) AS registered_clubs,
  (SELECT count(DISTINCT club_name) FROM s WHERE club_name IS NOT NULL) AS scoring_clubs,
  (SELECT count(*) FROM s) AS segment_scores,
  (SELECT count(*) FROM api.mv_element_score WHERE date_key BETWEEN %(start)s AND %(end)s) AS element_scores,
  (SELECT count(*) FROM dev) AS development_skaters,
  (SELECT min(event_start_date) FROM e) AS first_event_date,
  (SELECT max(event_start_date) FROM e) AS latest_event_date,
  (SELECT count(*) FROM e WHERE competition_stream IN ('ISU','ISUCS')) AS international_events,
  (SELECT count(*) FROM e WHERE competition_stream IN ('FRP','BSF','DP') OR competition_level = 'federated') AS federation_events,
  (SELECT count(*) FROM e WHERE competition_stream IN ('club','club_amateur','NonISU')) AS club_or_open_events
""",
        {"start": start, "end": end},
    ) or {}


def competition_mix_period(conn: Any, from_year: int, to_year: int) -> list[dict[str, Any]]:
    start, end = _date_bounds(from_year, to_year)
    return fetch_all(
        conn,
        """
WITH events_in_range AS (
  SELECT *
  FROM api.mv_dim_event
  WHERE event_start_date BETWEEN %(start)s AND %(end)s
),
scores_in_range AS (
  SELECT * FROM api.mv_segment_score WHERE date_key BETWEEN %(start)s AND %(end)s
)
SELECT
  CASE
    WHEN e.competition_stream IN ('ISU','ISUCS')                                       THEN 'international'
    WHEN e.competition_stream IN ('FRP','BSF','DP') OR e.competition_level = 'federated' THEN 'federation'
    WHEN e.competition_stream IN ('club','club_amateur','NonISU')                       THEN 'club_or_open'
    ELSE 'unknown'
  END AS scope_hint,
  COALESCE(e.competition_stream, 'unknown') AS competition_stream,
  COALESCE(e.competition_level, 'unknown')  AS competition_level,
  COALESCE(e.event_type, 'unknown')          AS event_type,
  CASE WHEN e.competition_stream IN ('club','club_amateur','NonISU') THEN 'club' ELSE 'nation' END AS representation_primary,
  count(DISTINCT e.event_key)                                          AS events,
  count(DISTINCT s.skater_key) FILTER (WHERE s.skater_key IS NOT NULL) AS skaters_with_scores,
  count(DISTINCT s.club_name)  FILTER (WHERE s.club_name IS NOT NULL)  AS clubs_with_appearances,
  count(DISTINCT il.identity_key)
    FILTER (WHERE s.skater_name IS NOT NULL)                            AS skaters_with_appearances,
  count(s.segment_score_key)                                            AS segment_scores,
  min(e.event_start_date)                                               AS first_event_date,
  max(e.event_start_date)                                               AS latest_event_date
FROM events_in_range e
LEFT JOIN scores_in_range s ON s.event_key = e.event_key
LEFT JOIN api.mv_score_identity il ON il.segment_score_key = s.segment_score_key
GROUP BY 1, 2, 3, 4, 5
ORDER BY events DESC, segment_scores DESC, competition_stream
""",
        {"start": start, "end": end},
    )


def list_clubs_period(conn: Any, from_year: int, to_year: int, limit: int | None = None) -> list[dict[str, Any]]:
    start, end = _date_bounds(from_year, to_year)
    return fetch_all(
        conn,
        """
WITH s AS (
  SELECT *
  FROM api.mv_segment_score
  WHERE date_key BETWEEN %(start)s AND %(end)s
),
club_appearances AS (
  SELECT DISTINCT
    COALESCE(sac.canonical_club_name, sac.club_name) AS club_name,
    il.identity_key,
    sac.category_name ILIKE 'Mlad Figurist%%' AS is_development
  FROM api.mv_skater_appearances_with_club sac
  LEFT JOIN api.mv_appearance_identity il ON il.skater_appearance_id = sac.skater_appearance_id
  WHERE sac.club_name IS NOT NULL
    AND sac.event_start_date BETWEEN %(start)s AND %(end)s
),
club_status AS (
  SELECT
    club_name,
    identity_key,
    bool_or(is_development) AS has_development_event,
    bool_or(NOT is_development) AS has_standard_event
  FROM club_appearances
  GROUP BY club_name, identity_key
),
club_development_counts AS (
  SELECT
    club_name,
    count(*) FILTER (WHERE has_development_event) AS development_skaters,
    count(*) FILTER (WHERE has_development_event AND NOT has_standard_event) AS development_only_skaters,
    count(*) FILTER (WHERE has_development_event AND has_standard_event) AS converted_development_skaters
  FROM club_status
  GROUP BY club_name
)
SELECT
  s.club_name,
  max(cd.country_code)                  AS country_code,
  count(DISTINCT s.skater_key)          AS skaters,
  count(DISTINCT s.event_key)           AS events,
  count(*)                              AS segment_scores,
  0::bigint AS international_events,
  0::bigint AS federation_events,
  0::bigint AS club_or_open_events,
  0::bigint AS national_competitors,
  0::bigint AS recreational_only_skaters,
  0::bigint AS mixed_competitors,
  COALESCE(max(cdc.development_skaters), 0) AS development_skaters,
  COALESCE(max(cdc.development_only_skaters), 0) AS development_only_skaters,
  COALESCE(max(cdc.converted_development_skaters), 0) AS converted_development_skaters,
  max(s.tss) AS best_tss,
  max(s.tes) AS best_tes,
  max(s.pcs) AS best_pcs,
  min(s.date_key) AS first_seen_date,
  max(s.date_key) AS latest_seen_date
FROM s
LEFT JOIN api.mv_club_dashboard cd ON cd.club_name = s.club_name
LEFT JOIN club_development_counts cdc ON cdc.club_name = s.club_name
WHERE s.club_name IS NOT NULL
GROUP BY s.club_name
ORDER BY segment_scores DESC, skaters DESC, latest_seen_date DESC NULLS LAST, s.club_name
LIMIT %(limit)s
""",
        {"start": start, "end": end, "limit": clamp_limit(limit)},
    )


def list_nationalities_period(conn: Any, from_year: int, to_year: int, limit: int | None = None) -> list[dict[str, Any]]:
    start, end = _date_bounds(from_year, to_year)
    return fetch_all(
        conn,
        """
WITH s AS (
  SELECT *
  FROM api.mv_segment_score
  WHERE date_key BETWEEN %(start)s AND %(end)s
    AND nation_code IS NOT NULL
),
nationality_appearances AS (
  SELECT DISTINCT
    sac.nation_code,
    sac.event_id,
    sac.event_start_date,
    il.identity_key,
    sac.category_name ILIKE 'Mlad Figurist%%' AS is_development
  FROM api.mv_skater_appearances_with_club sac
  LEFT JOIN api.mv_appearance_identity il ON il.skater_appearance_id = sac.skater_appearance_id
  WHERE sac.nation_code IS NOT NULL
    AND sac.event_start_date BETWEEN %(start)s AND %(end)s
),
nationality_identity_flags AS (
  SELECT
    nation_code,
    identity_key,
    bool_or(is_development) AS has_development_event,
    bool_or(NOT is_development) AS has_standard_event
  FROM nationality_appearances
  GROUP BY nation_code, identity_key
),
nationality_development_counts AS (
  SELECT
    nation_code,
    count(*) FILTER (WHERE has_development_event) AS development_skaters,
    count(*) FILTER (WHERE has_development_event AND NOT has_standard_event) AS development_only_skaters,
    count(*) FILTER (WHERE has_development_event AND has_standard_event) AS converted_development_skaters
  FROM nationality_identity_flags
  GROUP BY nation_code
)
SELECT
  s.nation_code,
  count(DISTINCT il.identity_key) AS skaters,
  count(DISTINCT s.event_key) AS events,
  count(*)                    AS segment_scores,
  COALESCE(max(ndc.development_skaters), 0) AS development_skaters,
  COALESCE(max(ndc.development_only_skaters), 0) AS development_only_skaters,
  COALESCE(max(ndc.converted_development_skaters), 0) AS converted_development_skaters,
  max(s.tss) AS best_tss,
  max(s.tes) AS best_tes,
  max(s.pcs) AS best_pcs,
  min(s.date_key) AS first_seen_date,
  max(s.date_key) AS latest_seen_date
FROM s
LEFT JOIN api.mv_score_identity il ON il.segment_score_key = s.segment_score_key
LEFT JOIN nationality_development_counts ndc ON ndc.nation_code = s.nation_code
GROUP BY s.nation_code
ORDER BY segment_scores DESC, skaters DESC, latest_seen_date DESC NULLS LAST, s.nation_code
LIMIT %(limit)s
""",
        {"start": start, "end": end, "limit": clamp_limit(limit)},
    )


def list_nationalities(
    conn: Any,
    query: str | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    where = ""
    params: dict[str, Any] = {"limit": clamp_limit(limit)}
    if query:
        where = "WHERE nation_code ILIKE %(pattern)s"
        params["pattern"] = f"%{query.strip().lower()}%"

    return fetch_all(
        conn,
        f"""
SELECT
  nation_code,
  skaters,
  events,
  segment_scores,
  development_skaters,
  development_only_skaters,
  converted_development_skaters,
  best_tss,
  best_tes,
  best_pcs,
  first_seen_date,
  latest_seen_date
FROM api.mv_nationality_dashboard
{where}
ORDER BY segment_scores DESC, skaters DESC, latest_seen_date DESC NULLS LAST, nation_code
LIMIT %(limit)s
""",
        params,
    )


def skater_component_summary(conn: Any, skater_key: int) -> list[dict[str, Any]]:
    """Average / best per program component for a skater. Used for the
    radar chart on the skater profile.

    api.mv_component_score has no skater identity columns of its own —
    join through mv_segment_score (which carries skater_name + nation +
    club) and back to mv_skater_search via the identity_key expression.
    """
    return fetch_all(
        conn,
        """
WITH segs AS (
  SELECT fss.segment_score_key
  FROM api.mv_skater_search mss
  JOIN api.mv_score_identity si ON si.identity_key = mss.identity_key
  JOIN api.mv_segment_score fss ON fss.segment_score_key = si.segment_score_key
  WHERE mss.skater_key = %(skater_key)s
)
SELECT
  cs.component_name,
  count(*)      AS samples,
  avg(cs.score) AS avg_score,
  max(cs.score) AS best_score,
  min(cs.score) AS worst_score
FROM api.mv_component_score cs
JOIN segs USING (segment_score_key)
GROUP BY cs.component_name
ORDER BY samples DESC, cs.component_name
""",
        {"skater_key": skater_key},
    )


def list_marker_legend(conn: Any) -> list[dict[str, Any]]:
    return fetch_all(
        conn,
        """
SELECT code, label, meaning
FROM core.validation_legend
WHERE legend_group = 'element_marker'
ORDER BY code
""",
    )


def nationality_detail(conn: Any, nation_code: str) -> dict[str, Any] | None:
    rows = list_nationalities(conn, nation_code, limit=1)
    matches = [row for row in rows if row.get("nation_code", "").upper() == nation_code.upper()]
    return matches[0] if matches else None


def nationality_skaters(
    conn: Any,
    nation_code: str,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    return fetch_all(
        conn,
        """
SELECT
  skater_key,
  identity_key,
  skater_name,
  nation_code,
  club_name,
  source_appearances,
  first_seen_date,
  latest_seen_date,
  best_tss,
  best_tes,
  best_pcs,
  competitions,
  scored_competitions
FROM api.mv_skater_search
WHERE upper(nation_code) = upper(%(nation_code)s)
ORDER BY best_tss DESC NULLS LAST, latest_seen_date DESC NULLS LAST, skater_name
LIMIT %(limit)s
""",
        {"nation_code": nation_code, "limit": clamp_limit(limit)},
    )


def list_clubs(
    conn: Any,
    query: str | None = None,
    scope: str | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    filters = []
    params: dict[str, Any] = {"limit": clamp_limit(limit)}
    if query:
        filters.append("search_text ILIKE %(pattern)s")
        params["pattern"] = f"%{query.strip().lower()}%"
    if scope in {"international", "federation", "club_or_open"}:
        filters.append(f"{scope}_events > 0")

    where = f"WHERE {' AND '.join(filters)}" if filters else ""
    return fetch_all(
        conn,
        f"""
SELECT
  club_name,
  country_code,
  skaters,
  events,
  segment_scores,
  international_events,
  federation_events,
  club_or_open_events,
  national_competitors,
  recreational_only_skaters,
  mixed_competitors,
  development_skaters,
  development_only_skaters,
  converted_development_skaters,
  best_tss,
  best_tes,
  best_pcs,
  first_seen_date,
  latest_seen_date
FROM api.mv_club_dashboard
{where}
ORDER BY segment_scores DESC, skaters DESC, latest_seen_date DESC NULLS LAST, club_name
LIMIT %(limit)s
""",
        params,
    )


def resolve_club_name(conn: Any, club_name: str) -> str | None:
    row = fetch_one(
        conn,
        """
SELECT c.canonical_name AS club_name
FROM core.club_aliases ca
JOIN core.clubs c ON c.id = ca.club_id
WHERE ca.alias_normalized = lower(regexp_replace(trim(%(club_name)s), '\\s+', ' ', 'g'))
UNION
SELECT club_name
FROM api.mv_club_dashboard
WHERE lower(club_name) = lower(%(club_name)s)
LIMIT 1
""",
        {"club_name": club_name},
    )
    return row["club_name"] if row else None


def club_detail(conn: Any, club_name: str) -> dict[str, Any] | None:
    canonical_name = resolve_club_name(conn, club_name)
    if canonical_name is None:
        return None
    return fetch_one(
        conn,
        """
SELECT
  cd.club_name,
  cd.country_code,
  cd.skaters,
  cd.events,
  cd.segment_scores,
  cd.international_events,
  cd.federation_events,
  cd.club_or_open_events,
  cd.national_competitors,
  cd.recreational_only_skaters,
  cd.mixed_competitors,
  cd.development_skaters,
  cd.development_only_skaters,
  cd.converted_development_skaters,
  cd.best_tss,
  cd.best_tes,
  cd.best_pcs,
  cd.first_seen_date,
  cd.latest_seen_date
FROM api.mv_club_dashboard cd
WHERE cd.club_name = %(club_name)s
""",
        {"club_name": canonical_name},
    )


def club_skaters(conn: Any, club_name: str, limit: int | None = None) -> list[dict[str, Any]]:
    canonical_name = resolve_club_name(conn, club_name) or club_name
    return fetch_all(
        conn,
        """
SELECT
  skater_key,
  identity_key,
  skater_name,
  nation_code,
  club_name,
  source_appearances,
  first_seen_date,
  latest_seen_date,
  best_tss,
  best_tes,
  best_pcs,
  competitions,
  scored_competitions,
  has_national_event,
  has_recreational_event,
  has_development_event,
  competitor_status
FROM api.mv_club_skater_status
WHERE club_name = %(club_name)s
ORDER BY
  CASE competitor_status
    WHEN 'mixed' THEN 0
    WHEN 'national_competitor' THEN 1
    WHEN 'recreational' THEN 2
    ELSE 3
  END,
  best_tss DESC NULLS LAST,
  latest_seen_date DESC NULLS LAST,
  skater_name
LIMIT %(limit)s
""",
        {"club_name": canonical_name, "limit": clamp_limit(limit)},
    )


def get_skater(conn: Any, skater_key: int) -> dict[str, Any] | None:
    return fetch_one(
        conn,
        """
SELECT
  skater_key,
  identity_key,
  skater_name,
  nation_code,
  club_name,
  source_appearances,
  first_seen_date,
  latest_seen_date,
  best_tss,
  best_tes,
  best_pcs,
  best_score_total,
  competitions,
  scored_competitions
FROM api.mv_skater_search
WHERE skater_key = %(skater_key)s
""",
        {"skater_key": skater_key},
    )


def skater_scores(conn: Any, skater_key: int, limit: int | None = None) -> list[dict[str, Any]]:
    return fetch_all(
        conn,
        """
SELECT
  fss.segment_score_key,
  fss.event_key,
  de.event_name,
  de.event_start_date,
  dc.category_name,
  dseg.segment_name,
  fss.score_source,
  COALESCE((
    SELECT count(*)
    FROM api.mv_element_score es
    WHERE es.segment_score_key = fss.segment_score_key
  ), 0) AS element_count,
  fss.place,
  fss.tss,
  fss.tes,
  fss.pcs,
  fss.deduction,
  fss.base_value_total,
  fss.element_score_total,
  fss.judge_count,
  de.root_url
FROM api.mv_skater_search mss
JOIN api.mv_score_identity si ON si.identity_key = mss.identity_key
JOIN api.mv_segment_score fss ON fss.segment_score_key = si.segment_score_key
LEFT JOIN api.mv_dim_event de ON de.event_key = fss.event_key
LEFT JOIN api.mv_dim_category dc ON dc.category_key = fss.category_key
LEFT JOIN api.mv_dim_segment dseg ON dseg.segment_key = fss.segment_key
WHERE mss.skater_key = %(skater_key)s
ORDER BY de.event_start_date DESC NULLS LAST, fss.segment_score_key DESC
LIMIT %(limit)s
""",
        {"skater_key": skater_key, "limit": clamp_limit(limit)},
    )


def skater_elements(conn: Any, skater_key: int, limit: int | None = None) -> list[dict[str, Any]]:
    return fetch_all(
        conn,
        """
SELECT
  element_score_key,
  segment_score_key,
  event_key,
  category_name,
  segment_name,
  date_key,
  element_no,
  element_code,
  base_element_code,
  raw_element,
  info,
  markers,
  base_value,
  goe,
  bonus,
  panel_score
FROM api.mv_skater_search mss
JOIN api.mv_identity_lookup il ON il.identity_key = mss.identity_key
JOIN api.mv_element_score
  ON api.mv_element_score.skater_name = il.skater_name
 AND COALESCE(api.mv_element_score.nation_code, '∅') = il.nk
 AND COALESCE(api.mv_element_score.club_name, '∅') = il.ck
WHERE mss.skater_key = %(skater_key)s
ORDER BY date_key DESC NULLS LAST, segment_score_key DESC, element_no
LIMIT %(limit)s
""",
        {"skater_key": skater_key, "limit": clamp_limit(limit)},
    )


def list_events(
    conn: Any,
    query: str | None,
    limit: int | None = None,
    from_year: int | None = None,
    to_year: int | None = None,
) -> list[dict[str, Any]]:
    clauses: list[str] = []
    params: dict[str, Any] = {"limit": clamp_limit(limit)}
    if query:
        clauses.append(
            "(event_name ILIKE %(pattern)s OR location ILIKE %(pattern)s OR root_url ILIKE %(pattern)s)"
        )
        params["pattern"] = f"%{query.strip()}%"
    if from_year is not None:
        clauses.append("event_start_date >= %(start_date)s")
        params["start_date"] = f"{int(from_year):04d}-01-01"
    if to_year is not None:
        clauses.append("event_start_date <= %(end_date)s")
        params["end_date"] = f"{int(to_year):04d}-12-31"
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""

    return fetch_all(
        conn,
        f"""
SELECT
  event_key,
  event_name,
  location,
  venue,
  date_range,
  event_start_date,
  event_type,
  competition_level,
  competition_stream,
  parser_profile,
  root_url,
  event_protocol_url
FROM api.mv_dim_event
{where}
ORDER BY event_start_date DESC NULLS LAST, event_name, event_key  -- event_key: stable order for same date+name
LIMIT %(limit)s
""",
        params,
    )


def get_event(conn: Any, event_key: int) -> dict[str, Any] | None:
    return fetch_one(
        conn,
        """
SELECT
  event_key,
  event_name,
  location,
  venue,
  date_range,
  event_start_date,
  event_type,
  competition_level,
  competition_stream,
  parser_profile,
  root_url,
  event_protocol_url
FROM api.mv_dim_event
WHERE event_key = %(event_key)s
""",
        {"event_key": event_key},
    )


def event_results(conn: Any, event_key: int, limit: int | None = None) -> list[dict[str, Any]]:
    # Edited 2026-10-12 by Claude (AI assistant): skater_key is the skater PROFILE key (the one /skaters/{key} opens),
    # found through this event's appearances; fcr.skater_key is only the per-competition row and stays as
    # source_skater_key. Results carry the club code as printed ("LEI"), so name+club matching misses ~13 %.
    return fetch_all(
        conn,
        """
WITH profile AS (
  SELECT DISTINCT ON (sac.category_id, sac.skater_name) sac.category_id, sac.skater_name, ss.skater_key
  FROM api.mv_skater_appearances_with_club sac
  JOIN api.mv_appearance_identity ai ON ai.skater_appearance_id = sac.skater_appearance_id
  JOIN api.mv_skater_search ss ON ss.identity_key = ai.identity_key
  WHERE sac.event_id = %(event_key)s
  ORDER BY sac.category_id, sac.skater_name
),
-- the skater's score sheets in this category (SP, FS, ...): the event page links the name to the first one
segment_scores AS (
  SELECT fss.category_key, fss.skater_name,
         jsonb_agg(jsonb_build_object('segment_score_key', fss.segment_score_key, 'segment_name', ds.segment_name)
                   ORDER BY fss.segment_key) AS segments
  FROM api.mv_segment_score fss
  LEFT JOIN api.mv_dim_segment ds ON ds.segment_key = fss.segment_key
  WHERE fss.event_key = %(event_key)s
  GROUP BY 1, 2
)
SELECT
  fcr.competition_result_key,
  fcr.category_key,
  dc.category_name,
  p.skater_key,
  fcr.skater_key AS source_skater_key,
  fcr.skater_name,
  fcr.nation_code,
  fcr.club_name,
  fcr.final_place,
  fcr.points,
  fcr.segment_places,
  COALESCE(sc.segments, '[]'::jsonb) AS segments
FROM api.mv_competition_result fcr
LEFT JOIN api.mv_dim_category dc ON dc.category_key = fcr.category_key
LEFT JOIN profile p ON p.category_id = fcr.category_key AND p.skater_name = fcr.skater_name
LEFT JOIN segment_scores sc ON sc.category_key = fcr.category_key AND sc.skater_name = fcr.skater_name
WHERE fcr.event_key = %(event_key)s
ORDER BY dc.category_name, fcr.final_place NULLS LAST, fcr.points DESC NULLS LAST
LIMIT %(limit)s
""",
        {"event_key": event_key, "limit": clamp_limit(limit)},
    )


def score_detail(conn: Any, segment_score_key: int) -> dict[str, Any] | None:
    score = fetch_one(
        conn,
        """
SELECT
  fss.segment_score_key,
  fss.score_source,
  ss.skater_key,
  fss.skater_key AS source_skater_key,
  fss.segment_key,
  fss.skater_name,
  fss.nation_code,
  fss.club_name,
  fss.event_key,
  de.event_name,
  de.event_start_date,
  fss.category_key,
  dc.category_name,
  dseg.segment_name,
  fss.place,
  fss.tss,
  fss.tes,
  fss.pcs,
  fss.deduction,
  fss.base_value_total,
  fss.element_score_total,
  fss.judge_count,
  de.root_url
FROM api.mv_segment_score fss
LEFT JOIN api.mv_score_identity si ON si.segment_score_key = fss.segment_score_key
LEFT JOIN api.mv_skater_search ss ON ss.identity_key = si.identity_key
LEFT JOIN api.mv_dim_event de ON de.event_key = fss.event_key
LEFT JOIN api.mv_dim_category dc ON dc.category_key = fss.category_key
LEFT JOIN api.mv_dim_segment dseg ON dseg.segment_key = fss.segment_key
WHERE fss.segment_score_key = %(segment_score_key)s
""",
        {"segment_score_key": segment_score_key},
    )
    if score is None:
        return None

    # Edited 2026-10-12 by Claude (AI assistant): the same skater's other segments in this category (SP <-> FS switch)
    score["category_segments"] = fetch_all(
        conn,
        """
SELECT fss.segment_score_key, ds.segment_name, fss.place, fss.tss
FROM api.mv_segment_score fss
LEFT JOIN api.mv_dim_segment ds ON ds.segment_key = fss.segment_key
WHERE fss.event_key = %(event_key)s AND fss.category_key = %(category_key)s AND fss.skater_name = %(skater_name)s
ORDER BY fss.segment_key
""",
        {"event_key": score["event_key"], "category_key": score["category_key"], "skater_name": score["skater_name"]},
    )
    score["judges"] = fetch_all(
        conn,
        """
SELECT DISTINCT
  oa.judge_number,
  oa.name,
  oa.nation_code,
  oa.function_text,
  oa.role_group
FROM ingest.official_assignments oa
WHERE oa.segment_id = %(segment_key)s
  AND oa.role_group = 'judge'
ORDER BY oa.judge_number
""",
        {"segment_key": score["segment_key"]},
    )
    score["elements"] = fetch_all(
        conn,
        """
SELECT
  element_score_key,
  element_no,
  element_code,
  base_element_code,
  raw_element,
  info,
  markers,
  base_value,
  goe,
  bonus,
  panel_score
FROM api.mv_element_score
WHERE segment_score_key = %(segment_score_key)s
ORDER BY element_no
""",
        {"segment_score_key": segment_score_key},
    )
    element_marks = fetch_all(
        conn,
        """
SELECT
  fejm.element_score_key,
  fejm.judge_number,
  fejm.score,
  oa.name AS judge_name,
  oa.nation_code AS judge_nation_code,
  oa.function_text AS judge_function
FROM api.mv_element_judge_mark fejm
LEFT JOIN ingest.official_assignments oa
  ON oa.segment_id = fejm.segment_key
 AND oa.judge_number = fejm.judge_number
 AND oa.role_group = 'judge'
WHERE fejm.segment_score_key = %(segment_score_key)s
ORDER BY fejm.element_score_key, fejm.judge_number
""",
        {"segment_score_key": segment_score_key},
    )
    marks_by_element: dict[int, list[dict[str, Any]]] = {}
    for mark in element_marks:
        marks_by_element.setdefault(mark["element_score_key"], []).append(mark)
    for element in score["elements"]:
        element["judge_marks"] = marks_by_element.get(element["element_score_key"], [])

    score["components"] = fetch_all(
        conn,
        """
SELECT
  component_score_key,
  component_name,
  factor,
  score
FROM api.mv_component_score
WHERE segment_score_key = %(segment_score_key)s
ORDER BY component_score_key
""",
        {"segment_score_key": segment_score_key},
    )
    component_marks = fetch_all(
        conn,
        """
SELECT
  ppc.id AS component_score_key,
  pcjs.judge_number,
  pcjs.score,
  oa.name AS judge_name,
  oa.nation_code AS judge_nation_code,
  oa.function_text AS judge_function
FROM ingest.pdf_component_judge_scores pcjs
JOIN ingest.pdf_program_components ppc ON ppc.id = pcjs.pdf_program_component_id
JOIN ingest.pdf_score_summaries pss ON pss.id = ppc.pdf_score_summary_id
-- Edited 2026-10-12 by Claude (AI assistant): the segment is already known; joining the cleaned-appearance view
-- (a deep view stack) only to get it cost ~0.6 s per score sheet
LEFT JOIN ingest.official_assignments oa
  ON oa.segment_id = %(segment_key)s
 AND oa.judge_number = pcjs.judge_number
 AND oa.role_group = 'judge'
WHERE pss.id = %(segment_score_key)s
ORDER BY ppc.id, pcjs.judge_number
""",
        {"segment_score_key": segment_score_key, "segment_key": score["segment_key"]},
    )
    marks_by_component: dict[int, list[dict[str, Any]]] = {}
    for mark in component_marks:
        marks_by_component.setdefault(mark["component_score_key"], []).append(mark)
    for component in score["components"]:
        component["judge_marks"] = marks_by_component.get(component["component_score_key"], [])

    # Evidence lookup. mart.v_skater_source_evidence is a deep view stack
    # (~15s for a single-segment lookup), so we resolve documents directly:
    # PDF row -> the parsed PDF source_document. Plus the HTML segment /
    # category / event-index pages from the same import_run_id, scoped by
    # segment_id where the source_documents table records it.
    is_pdf_row = score.get("score_source") == "pdf_score_summary" and score.get("segment_score_key", 0) > 0
    pss_id = score["segment_score_key"] if is_pdf_row else None
    score["evidence"] = fetch_all(
        conn,
        """
WITH pdf_doc AS (
  SELECT sd.id AS source_document_key, sd.url AS source_url, sd.document_type,
         saf.archive_path, saf.archive_status, saf.content_hash
  FROM ingest.pdf_score_summaries pss
  JOIN ingest.source_documents sd ON sd.id = pss.source_document_id
  LEFT JOIN ingest.source_archive_files saf ON saf.source_document_id = sd.id
  WHERE pss.id = %(pss_id)s
),
event_html AS (
  SELECT sd.id AS source_document_key, sd.url AS source_url, sd.document_type,
         saf.archive_path, saf.archive_status, saf.content_hash
  FROM ingest.source_documents sd
  LEFT JOIN ingest.source_archive_files saf ON saf.source_document_id = sd.id
  WHERE sd.import_run_id = (
    SELECT import_run_id FROM ingest.events WHERE id = (
      SELECT event_id FROM ingest.source_skater_appearances WHERE id = (
        SELECT skater_appearance_id FROM ingest.pdf_score_summaries WHERE id = %(pss_id)s
      )
    )
  )
  AND sd.document_type = 'html'
  ORDER BY sd.id
  LIMIT 5
)
SELECT * FROM pdf_doc
UNION ALL
SELECT * FROM event_html
""",
        {"pss_id": pss_id or -1},
    )
    return score
