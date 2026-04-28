from scripts.import_event import is_old_isucalcfs_wrapper, normalize_representation, old_isucalcfs_main_url


def test_old_isucalcfs_wrapper_detection():
    html = '<html><head><script src="scripts/results.min.js"></script></head><body></body></html>'

    assert is_old_isucalcfs_wrapper(html)


def test_old_isucalcfs_wrapper_main_url_by_extension():
    assert (
        old_isucalcfs_main_url("https://cup.clubdenkovastaviski.com/2018/ISU/index.htm")
        == "https://cup.clubdenkovastaviski.com/2018/ISU/pages/main.htm"
    )
    assert (
        old_isucalcfs_main_url("https://cup.clubdenkovastaviski.com/2019/ISU/index.html")
        == "https://cup.clubdenkovastaviski.com/2019/ISU/pages/main.html"
    )


def test_normalize_representation_moves_old_club_column_from_nation():
    record = {"name": "Test Skater", "nation": "Ice P", "club": ""}
    profile = {"settings": {"representation": {"primary": "club", "nation_column": "club"}}}

    normalized = normalize_representation(record, profile)

    assert normalized["nation"] is None
    assert normalized["club"] == "Ice P"
    assert normalized["representation_type"] == "club"
    assert normalized["representation_value"] == "Ice P"


def test_normalize_representation_keeps_country_for_international_profile():
    record = {"name": "Test Skater", "nation": "BUL", "club": ""}
    profile = {"settings": {"representation": {"primary": "nation", "nation_column": "country"}}}

    normalized = normalize_representation(record, profile)

    assert normalized["nation"] == "BUL"
    assert normalized["club"] == ""
    assert normalized["representation_type"] == "nation"
    assert normalized["representation_value"] == "BUL"
