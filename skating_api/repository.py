from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any

from psycopg.rows import dict_row


DEFAULT_LIMIT = 25
MAX_LIMIT = 500

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


def list_nationalities(
    conn: Any,
    query: str | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    filters = ["nation_code IS NOT NULL"]
    params: dict[str, Any] = {"limit": clamp_limit(limit)}
    if query:
        filters.append("nation_code ILIKE %(pattern)s")
        params["pattern"] = f"%{query.strip().lower()}%"

    return fetch_all(
        conn,
        f"""
WITH nationality_appearances AS (
  SELECT DISTINCT
    sac.nation_code,
    sac.event_id,
    sac.event_start_date,
    api.skater_identity_key(
      sac.skater_name,
      sac.nation_code,
      COALESCE(sac.canonical_club_name, sac.club_name)
    ) AS identity_key,
    sac.category_name ILIKE 'Mlad Figurist%%' AS is_development
  FROM analytics.v_skater_appearances_with_club sac
  WHERE {' AND '.join(filters)}
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
nationality_scores AS (
  SELECT
    nation_code,
    count(*) AS segment_scores,
    max(tss) AS best_tss,
    max(tes) AS best_tes,
    max(pcs) AS best_pcs
  FROM mart.fact_segment_score
  WHERE nation_code IS NOT NULL
  GROUP BY nation_code
),
nationality_events AS (
  SELECT
    nation_code,
    count(DISTINCT event_id) AS events,
    min(event_start_date) AS first_seen_date,
    max(event_start_date) AS latest_seen_date
  FROM nationality_appearances
  GROUP BY nation_code
)
SELECT
  nif.nation_code,
  count(DISTINCT nif.identity_key) AS skaters,
  COALESCE(max(ne.events), 0) AS events,
  COALESCE(max(ns.segment_scores), 0) AS segment_scores,
  count(*) FILTER (WHERE nif.has_development_event) AS development_skaters,
  count(*) FILTER (WHERE nif.has_development_event AND NOT nif.has_standard_event) AS development_only_skaters,
  count(*) FILTER (WHERE nif.has_development_event AND nif.has_standard_event) AS converted_development_skaters,
  max(ns.best_tss) AS best_tss,
  max(ns.best_tes) AS best_tes,
  max(ns.best_pcs) AS best_pcs,
  min(ne.first_seen_date) AS first_seen_date,
  max(ne.latest_seen_date) AS latest_seen_date
FROM nationality_identity_flags nif
LEFT JOIN nationality_events ne ON ne.nation_code = nif.nation_code
LEFT JOIN nationality_scores ns ON ns.nation_code = nif.nation_code
GROUP BY nif.nation_code
ORDER BY segment_scores DESC, skaters DESC, latest_seen_date DESC NULLS LAST, nif.nation_code
LIMIT %(limit)s
""",
        params,
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
  fss.place,
  fss.tss,
  fss.tes,
  fss.pcs,
  fss.deduction,
  fss.base_value_total,
  fss.element_score_total,
  fss.judge_count,
  de.root_url
FROM mart.fact_segment_score fss
JOIN api.mv_skater_search mss
  ON mss.identity_key = api.skater_identity_key(fss.skater_name, fss.nation_code, fss.club_name)
LEFT JOIN mart.dim_event de ON de.event_key = fss.event_key
LEFT JOIN mart.dim_category dc ON dc.category_key = fss.category_key
LEFT JOIN mart.dim_segment dseg ON dseg.segment_key = fss.segment_key
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
FROM mart.fact_element_score
JOIN api.mv_skater_search mss
  ON mss.identity_key = api.skater_identity_key(mart.fact_element_score.skater_name, mart.fact_element_score.nation_code, mart.fact_element_score.club_name)
WHERE mss.skater_key = %(skater_key)s
ORDER BY date_key DESC NULLS LAST, segment_score_key DESC, element_no
LIMIT %(limit)s
""",
        {"skater_key": skater_key, "limit": clamp_limit(limit)},
    )


def list_events(conn: Any, query: str | None, limit: int | None = None) -> list[dict[str, Any]]:
    where = ""
    params: dict[str, Any] = {"limit": clamp_limit(limit)}
    if query:
        where = """
WHERE event_name ILIKE %(pattern)s
   OR location ILIKE %(pattern)s
   OR root_url ILIKE %(pattern)s
"""
        params["pattern"] = f"%{query.strip()}%"

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
FROM mart.dim_event
{where}
ORDER BY event_start_date DESC NULLS LAST, event_name
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
FROM mart.dim_event
WHERE event_key = %(event_key)s
""",
        {"event_key": event_key},
    )


def event_results(conn: Any, event_key: int, limit: int | None = None) -> list[dict[str, Any]]:
    return fetch_all(
        conn,
        """
SELECT
  fcr.competition_result_key,
  fcr.category_key,
  dc.category_name,
  fcr.skater_key,
  fcr.skater_name,
  fcr.nation_code,
  fcr.club_name,
  fcr.final_place,
  fcr.points,
  fcr.segment_places
FROM mart.fact_competition_result fcr
LEFT JOIN mart.dim_category dc ON dc.category_key = fcr.category_key
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
  fss.skater_key,
  fss.segment_key,
  fss.skater_name,
  fss.nation_code,
  fss.club_name,
  fss.event_key,
  de.event_name,
  de.event_start_date,
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
FROM mart.fact_segment_score fss
LEFT JOIN mart.dim_event de ON de.event_key = fss.event_key
LEFT JOIN mart.dim_category dc ON dc.category_key = fss.category_key
LEFT JOIN mart.dim_segment dseg ON dseg.segment_key = fss.segment_key
WHERE fss.segment_score_key = %(segment_score_key)s
""",
        {"segment_score_key": segment_score_key},
    )
    if score is None:
        return None

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
FROM mart.fact_element_score
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
FROM mart.fact_element_judge_mark fejm
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
FROM mart.fact_component_score
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
JOIN analytics.v_skater_appearances_clean sac ON sac.skater_appearance_id = pss.skater_appearance_id
LEFT JOIN ingest.official_assignments oa
  ON oa.segment_id = sac.segment_id
 AND oa.judge_number = pcjs.judge_number
 AND oa.role_group = 'judge'
WHERE pss.id = %(segment_score_key)s
ORDER BY ppc.id, pcjs.judge_number
""",
        {"segment_score_key": segment_score_key},
    )
    marks_by_component: dict[int, list[dict[str, Any]]] = {}
    for mark in component_marks:
        marks_by_component.setdefault(mark["component_score_key"], []).append(mark)
    for component in score["components"]:
        component["judge_marks"] = marks_by_component.get(component["component_score_key"], [])

    score["evidence"] = fetch_all(
        conn,
        """
SELECT DISTINCT
  vse.source_document_key,
  vse.source_url,
  vse.document_type,
  vse.archive_path,
  vse.archive_status,
  vse.content_hash
FROM mart.v_skater_source_evidence vse
WHERE vse.skater_name = %(skater_name)s
  AND vse.nation_code IS NOT DISTINCT FROM %(nation_code)s
  AND vse.club_name IS NOT DISTINCT FROM %(club_name)s
  AND vse.event_name IS NOT DISTINCT FROM %(event_name)s
  AND vse.event_start_date::text IS NOT DISTINCT FROM %(event_start_date)s
  AND vse.category_name IS NOT DISTINCT FROM %(category_name)s
  AND vse.segment_name IS NOT DISTINCT FROM %(segment_name)s
LIMIT 25
""",
        {
            "skater_name": score["skater_name"],
            "nation_code": score["nation_code"],
            "club_name": score["club_name"],
            "event_name": score["event_name"],
            "event_start_date": score["event_start_date"],
            "category_name": score["category_name"],
            "segment_name": score["segment_name"],
        },
    )
    return score
