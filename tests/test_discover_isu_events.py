from scripts.discover_isu_events import discover_detail_from_html, discover_from_html


def test_discover_isu_events_from_static_page_text() -> None:
    html = """
    <html><body>
      <a href="/figure-skating/events/eventdetail/isu-figure-skating-junior-grand-prix-xian-city-2026/">detail</a>
      <a href="/figure-skating/events/eventdetail/isu-figure-skating-junior-world-championships-2027/">detail</a>
      <div>19 Aug - 22 Aug, 2026</div>
      <div>ISU Figure Skating Junior Grand Prix Xi'An City 2026</div>
      <div>Xi'An City / CHN</div>
      <div>FIGURE SKATING</div>
      <div>22 Feb - 28 Feb, 2027</div>
      <div>ISU Figure Skating Junior World Championships 2027</div>
      <div>Sofia / BUL</div>
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
    assert rows[0].detail_url == "https://www.isu-skating.com/figure-skating/events/eventdetail/isu-figure-skating-junior-grand-prix-xian-city-2026/"
    assert rows[0].result_url is None
    assert rows[0].city == "Xi'An City"
    assert rows[0].country_code == "CHN"
    assert rows[1].event_name == "ISU Figure Skating Junior World Championships 2027"
    assert rows[1].city == "Sofia"
    assert rows[1].country_code == "BUL"
    assert rows[1].discovery_status == "catalog_only_needs_result_url"


def test_discover_isu_event_detail_result_url_from_next_payload() -> None:
    html = r"""
    <html><body>
      <meta name="viewport" content="width=device-width, initial-scale=1"/>
      <script>
      self.__next_f.push([1,"{\"pageinfos\":{\"name\":\"International Adult Competition\",\"display_date\":\"29 Jun - 4 Jul, 2025\",\"city\":\"Oberstdorf\",\"country_code\":\"GER\",\"discipline_title\":\"Figure Skating\",\"detail_result_url\":\"https:\/\/www.deu-event.de\/results\/adult2025\/\"}}"]);
      </script>
    </body></html>
    """

    row = discover_detail_from_html(
        html,
        "https://www.isu-skating.com/figure-skating/events/eventdetail/international-adult-competition/",
    )

    assert row.event_name == "International Adult Competition"
    assert row.date_range == "29 Jun - 4 Jul, 2025"
    assert row.city == "Oberstdorf"
    assert row.country_code == "GER"
    assert row.result_url == "https://www.deu-event.de/results/adult2025/"
    assert row.discovery_status == "candidate_result_url"
