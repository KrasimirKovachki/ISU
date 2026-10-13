from scripts.discover_frp_html import discover_from_html


def test_discovers_frp_autoindex_result_folders() -> None:
    html = """
    <table>
      <tr><td><a href="/">Parent Directory</a></td><td></td></tr>
      <tr>
        <td><a href="/HTML/BUCHAREST%20OPEN%20RECREATIONAL/">BUCHAREST OPEN RECREATIONAL</a></td>
        <td>2026-02-01 12:51</td>
      </tr>
      <tr>
        <td><a href="/HTML/flags/">flags</a></td>
        <td>2022-08-24 16:51</td>
      </tr>
      <tr>
        <td><a href="/HTML/adv.pdf">adv.pdf</a></td>
        <td>2022-10-28 21:01</td>
      </tr>
    </table>
    """

    rows = discover_from_html(html)

    assert len(rows) == 1
    assert rows[0].name == "BUCHAREST OPEN RECREATIONAL"
    assert rows[0].url == "https://www.frponline.ro/HTML/BUCHAREST%20OPEN%20RECREATIONAL/"
    assert rows[0].index_url == "https://www.frponline.ro/HTML/BUCHAREST%20OPEN%20RECREATIONAL/index.htm"
    assert rows[0].last_modified == "2026-02-01 12:51"
