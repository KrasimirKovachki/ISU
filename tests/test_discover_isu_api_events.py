from scripts.discover_isu_api_events import detail_url_for_row, source_kind_for_row


def test_detail_url_for_row_uses_canonical_slug() -> None:
    assert (
        detail_url_for_row({"slug": "cranberry-cup-international-2025"})
        == "https://www.isu-skating.com/figure-skating/events/eventdetail/cranberry-cup-international-2025/"
    )


def test_source_kind_for_row_classifies_common_api_event_types() -> None:
    assert source_kind_for_row({"name": "International Adult Competition"}) == "isu_adult_catalog"
    assert source_kind_for_row({"name": "ISU Figure Skating Challenger Series Cranberry Cup International 2025"}) == "isu_challenger_catalog"
    assert source_kind_for_row({"event_type": "International", "name": "Asian Open Figure Skating Trophy 2025"}) == "isu_international_catalog"
    assert source_kind_for_row({"event_type": "ISU Event", "name": "ISU World Championships 2026"}) == "isu_api_event_catalog"
