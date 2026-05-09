from __future__ import annotations

from datetime import date
from decimal import Decimal

from skating_api import repository


class FakeCursor:
    def __init__(self, rows):
        self.rows = rows
        self.sql = None
        self.params = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        self.sql = sql
        self.params = params

    def fetchall(self):
        return self.rows

    def fetchone(self):
        return self.rows[0] if self.rows else None


class FakeConnection:
    def __init__(self, row_batches):
        self.row_batches = list(row_batches)
        self.cursors = []

    def cursor(self, row_factory=None):
        cursor = FakeCursor(self.row_batches.pop(0))
        self.cursors.append(cursor)
        return cursor


def test_public_row_serializes_dates_and_decimals():
    row = repository.public_row({"score": Decimal("12.34"), "date": date(2026, 3, 20)})

    assert row == {"score": 12.34, "date": "2026-03-20"}


def test_search_skaters_adds_safe_pattern_and_clamps_limit():
    conn = FakeConnection([[{"skater_key": 1, "skater_name": "A"}]])

    rows = repository.search_skaters(conn, "  eva  ", repository.MAX_LIMIT + 100)

    assert rows == [{"skater_key": 1, "skater_name": "A"}]
    assert "ILIKE %(pattern)s" in conn.cursors[0].sql
    assert "api.mv_skater_search" in conn.cursors[0].sql
    assert conn.cursors[0].params["pattern"] == "%eva%"
    assert conn.cursors[0].params["limit"] == repository.MAX_LIMIT


def test_list_clubs_can_filter_by_scope_and_name():
    conn = FakeConnection([[{"club_name": "Odesos", "segment_scores": 23}]])

    rows = repository.list_clubs(conn, "ode", "federation", 10)

    assert rows == [{"club_name": "Odesos", "segment_scores": 23}]
    assert "api.mv_club_dashboard" in conn.cursors[0].sql
    assert "federation_events > 0" in conn.cursors[0].sql
    assert conn.cursors[0].params["pattern"] == "%ode%"


def test_list_nationalities_groups_by_nation_code():
    conn = FakeConnection([[{"nation_code": "BUL", "skaters": 42, "segment_scores": 100}]])

    rows = repository.list_nationalities(conn, "bul", 10)

    assert rows == [
        {
            "nation_code": "BUL",
            "skaters": 42,
            "segment_scores": 100,
            "nation_flag": "🇧🇬",
            "nation_display": "🇧🇬 BUL",
        }
    ]
    assert "api.mv_nationality_dashboard" in conn.cursors[0].sql
    assert "nation_code ILIKE %(pattern)s" in conn.cursors[0].sql
    assert conn.cursors[0].params["pattern"] == "%bul%"


def test_dashboard_overview_reads_api_materialized_view():
    conn = FakeConnection([[{"events": 158, "skaters": 9231}]])

    overview = repository.dashboard_overview(conn)

    assert overview == {"events": 158, "skaters": 9231}
    assert "api.mv_dashboard_overview" in conn.cursors[0].sql


def test_dashboard_overview_period_counts_development_skaters():
    conn = FakeConnection([[{"events": 12, "development_skaters": 3}]])

    overview = repository.dashboard_overview_period(conn, 2024, 2026)

    assert overview == {"events": 12, "development_skaters": 3}
    assert "Mlad Figurist" in conn.cursors[0].sql
    assert "0::bigint AS development_skaters" not in conn.cursors[0].sql


def test_list_nationalities_period_counts_development_skaters():
    conn = FakeConnection([[{"nation_code": "BUL", "development_skaters": 4}]])

    rows = repository.list_nationalities_period(conn, 2024, 2026)

    assert rows == [
        {
            "nation_code": "BUL",
            "development_skaters": 4,
            "nation_flag": "🇧🇬",
            "nation_display": "🇧🇬 BUL",
        }
    ]
    assert "Mlad Figurist" in conn.cursors[0].sql
    assert "0::bigint AS development_skaters" not in conn.cursors[0].sql


def test_score_detail_attaches_elements_components_and_evidence():
    conn = FakeConnection(
        [
            [
                {
                    "segment_score_key": 10,
                    "score_source": "pdf_score_summary",
                    "segment_key": 55,
                    "skater_name": "Eva Example",
                    "nation_code": "BUL",
                    "club_name": None,
                    "event_name": "Example Cup",
                    "event_start_date": date(2026, 3, 20),
                    "category_name": "Women",
                    "segment_name": "Free Skating",
                }
            ],
            [{"judge_number": 1, "name": "Judge One", "nation_code": "BUL"}],
            [{"element_score_key": 99, "base_value": Decimal("3.30")}],
            [{"element_score_key": 99, "judge_number": 1, "score": -1}],
            [{"component_score_key": 5, "score": Decimal("7.25")}],
            [{"component_score_key": 5, "judge_number": 1, "score": Decimal("7.50")}],
            [{"source_document_key": 7, "source_url": "https://example.test"}],
        ]
    )

    score = repository.score_detail(conn, 10)

    assert score["event_start_date"] == "2026-03-20"
    assert score["judges"] == [{"judge_number": 1, "name": "Judge One", "nation_code": "BUL", "nation_flag": "🇧🇬", "nation_display": "🇧🇬 BUL"}]
    assert score["elements"] == [{"element_score_key": 99, "base_value": 3.3, "judge_marks": [{"element_score_key": 99, "judge_number": 1, "score": -1}]}]
    assert score["components"] == [{"component_score_key": 5, "score": 7.25, "judge_marks": [{"component_score_key": 5, "judge_number": 1, "score": 7.5}]}]
    assert score["evidence"] == [{"source_document_key": 7, "source_url": "https://example.test"}]
    # Evidence query uses direct PDF + import-run lookup (cheap), not the
    # deep-stack mart.v_skater_source_evidence (it took ~15s per call).
    assert "ingest.pdf_score_summaries" in conn.cursors[6].sql
    assert "ingest.source_documents" in conn.cursors[6].sql
    assert conn.cursors[6].params["pss_id"] == 10


def test_skater_scores_include_source_and_element_count():
    conn = FakeConnection(
        [
            [
                {
                    "segment_score_key": 4561,
                    "event_key": 1,
                    "event_name": "Denkova-Staviski Cup 2017",
                    "event_start_date": date(2017, 10, 31),
                    "category_name": "Senior Ladies",
                    "segment_name": "Short Program",
                    "score_source": "pdf_score_summary",
                    "element_count": 0,
                    "place": 1,
                    "tss": Decimal("77.210"),
                    "tes": Decimal("41.000"),
                    "pcs": Decimal("36.210"),
                    "deduction": Decimal("0.000"),
                    "base_value_total": Decimal("38.000"),
                    "element_score_total": Decimal("41.000"),
                    "judge_count": 5,
                    "root_url": "https://example.test",
                }
            ]
        ]
    )

    rows = repository.skater_scores(conn, 1469, 50)

    assert rows == [
        {
            "segment_score_key": 4561,
            "event_key": 1,
            "event_name": "Denkova-Staviski Cup 2017",
            "event_start_date": "2017-10-31",
            "category_name": "Senior Ladies",
            "segment_name": "Short Program",
            "score_source": "pdf_score_summary",
            "element_count": 0,
            "place": 1,
            "tss": 77.21,
            "tes": 41.0,
            "pcs": 36.21,
            "deduction": 0.0,
            "base_value_total": 38.0,
            "element_score_total": 41.0,
            "judge_count": 5,
            "root_url": "https://example.test",
        }
    ]
    assert "score_source" in conn.cursors[0].sql
    assert "element_count" in conn.cursors[0].sql


def test_score_detail_passes_negative_keys_unchanged():
    # api.mv_segment_score (snapshot of mart.fact_segment_score) encodes
    # score_source into the key by negating
    # html_segment_result rows. score_detail must thread the negative integer
    # straight through to PostgreSQL without sanitizing it; otherwise an HTML
    # fallback row would resolve to the unrelated PDF row that shares the
    # absolute-value id.
    conn = FakeConnection(
        [
            [
                {
                    "segment_score_key": -588,
                    "score_source": "html_segment_result",
                    "segment_key": 48,
                    "skater_name": "ALEXANDRA FEIGIN",
                    "nation_code": "BUL",
                    "club_name": None,
                    "event_name": "Sofia Trophy 2026",
                    "event_start_date": date(2026, 1, 6),
                    "category_name": "Women",
                    "segment_name": "Short Program",
                }
            ],
            [],  # judges
            [],  # elements
            [],  # element judge marks
            [],  # components
            [],  # component judge marks
            [],  # evidence
        ]
    )

    score = repository.score_detail(conn, -588)

    assert score is not None
    assert score["segment_score_key"] == -588
    assert score["skater_name"] == "ALEXANDRA FEIGIN"
    assert score["score_source"] == "html_segment_result"
    assert score["elements"] == []
    assert score["components"] == []
    assert "mv_segment_score" in conn.cursors[0].sql
    assert "segment_score_key = %(segment_score_key)s" in conn.cursors[0].sql
    assert conn.cursors[0].params["segment_score_key"] == -588
