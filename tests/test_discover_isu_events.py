from scripts.discover_isu_events import discover_from_html


def test_discover_isu_events_from_static_page_text() -> None:
    html = """
    <html><body>
      <div>19 Aug - 22 Aug, 2026</div>
      <div>ISU Figure Skating Junior Grand Prix Xi'An City 2026</div>
      <div>Xi'An City / CHN</div>
      <div>FIGURE SKATING</div>
      <div>22 Feb - 28 Feb, 2027</div>
      <div>ISU Figure Skating Junior World Championships 2027</div>
      <div>Sofia / BUL</div>
      <div>FIGURE SKATING</div>
    </body></html>
    """

    rows = discover_from_html(html)

    assert len(rows) == 2
    assert rows[0].event_name == "ISU Figure Skating Junior Grand Prix Xi'An City 2026"
    assert rows[0].city == "Xi'An City"
    assert rows[0].country_code == "CHN"
    assert rows[1].event_name == "ISU Figure Skating Junior World Championships 2027"
    assert rows[1].city == "Sofia"
    assert rows[1].country_code == "BUL"
    assert rows[1].discovery_status == "catalog_only_needs_result_url"
