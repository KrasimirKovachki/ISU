from isu_parser.fs_manager import parse_category_result, parse_entries, parse_index, parse_officials, parse_segment_result


INDEX_HTML = """
<html><head><title>Denkova-Staviski Cup 2024</title><meta name="generator" content="FS Manager by Swiss Timing, Ltd." /></head>
<body><table>
<tr><td><table><tr><td class="caption3">Sofia / BUL</td><td class="caption3">Winter Sports Palace</td></tr></table></td></tr>
<tr class="caption3"><td>05.11.2024 - 10.11.2024</td></tr>
<tr class="caption3"><td><form action="denk2024_protocol.pdf" method="link"><input type="submit" value="Download Event protocol" /></form></td></tr>
<tr><td><table><tr><td><table>
<tr class="TabHeadWhite"><th>Category</th><th>Segment</th><th>&nbsp;</th><th>&nbsp;</th><th>Reports</th></tr>
<tr><td>Men</td><td></td><td><a href="CAT001EN.htm"> Entries </a></td><td><a href="CAT001RS.htm"> Result </a></td><td>&nbsp;</td></tr>
<tr><td></td><td>Short Program</td><td><a href="SEG001OF.htm">Panel of Judges</a></td><td><a href="SEG001.htm"> Starting Order / Detailed Classification </a></td><td><a href="FSKMSINGLES-----------QUAL000100--_JudgesDetailsperSkater.pdf">Judges Scores</a></td></tr>
</table></td></tr></table></td></tr>
<tr><td><table><tr><td><table>
<tr><th>Date</th><th>Time</th><th>Category</th><th>Segment</th></tr>
<tr><td>08.11.2024</td><td></td><td></td><td></td></tr>
<tr><td></td><td>20:50:00</td><td>Men</td><td><a href="SEG001.htm">Short Program</a></td></tr>
</table></td></tr></table></td></tr>
</table></body></html>
"""


ENTRIES_HTML = """
<tr class="caption2"><td>Men</td></tr>
<table><tr><th>No.</th><th>Name</th><th>Nation</th></tr>
<tr><td>1</td><td><a href="/bios/isufs00009567.htm">Maurizio ZANDRON</a></td><td><table><tr><td><img /></td><td></td><td>AUT</td></tr></table></td></tr>
</table>
"""


CLUB_ENTRIES_HTML = """
<tr class="caption2"><td>Recreational A-mini Girls</td></tr>
<table><tr><th>No.</th><th>Name</th><th>Club</th><th>Nation</th></tr>
<tr><td>1</td><td><a href="/bios/isufs00000bul.htm">Radina GOSPODINOVA</a></td><td>Elit</td><td><table><tr><td>BUL</td></tr></table></td></tr>
</table>
"""


CLUB_ENTRIES_WITH_EMPTY_CLUB_HTML = """
<tr class="caption2"><td>Recreational A-mini Girls</td></tr>
<table><tr><th>No.</th><th>Name</th><th>Club</th><th>Nation</th></tr>
<tr><td>1</td><td><a href="/bios/isufs00000rou.htm">Adelina RUGINA</a></td><td></td><td><table><tr><td>ROU</td></tr></table></td></tr>
</table>
"""


RESULT_HTML = """
<tr class="caption2"><td>Men</td></tr>
<table><tr><th>FPl.</th><th>Name</th><th>Nation</th><th>Points</th><th>SP</th><th>FS</th></tr>
<tr><td>1</td><td><a href="/bios/isufs00107843.htm">Lev VINOKUR</a></td><td><table><tr><td>ISR</td></tr></table></td><td>234.31</td><td>1</td><td>1</td></tr>
</table>
"""


CLUB_RESULT_HTML = """
<tr class="caption2"><td>Recreational A-mini Girls</td></tr>
<table><tr><th>FPl.</th><th>Name</th><th>Club</th><th>Nation</th><th>Points</th><th>FS</th></tr>
<tr><td>1</td><td><a href="/bios/isufs00000bul.htm">Alis YORDANOVA</a></td><td>Varna</td><td><table><tr><td>BUL</td></tr></table></td><td>19.83</td><td>1</td></tr>
</table>
"""


SEGMENT_HTML = """
<tr class="caption2"><td>Men - Short Program</td></tr>
<table><tr><th>Pl.</th><th>Name</th><th>Nation</th><th>TSS<br />=</th><th>TES<br />+</th><th>&nbsp;</th><th>PCS<br />+</th><th>CO</th><th>PR</th><th>SK</th><th>Ded.<br />-</th><th>StN.</th></tr>
<tr><td>1</td><td><a href="/bios/isufs00107843.htm">Lev VINOKUR</a></td><td>ISR</td><td>82.97</td><td>45.24</td><td></td><td>37.73</td><td>7.67</td><td>7.42</td><td>7.50</td><td>0.00</td><td>#4</td></tr>
</table>
"""


SEGMENT_WITH_QUALIFICATION_HTML = """
<tr class="caption2"><td>Junior Women - Short Program</td></tr>
<table><tr><th>Pl.</th><th>Qual.</th><th>Name</th><th>Nation</th><th>TSS<br />=</th><th>TES<br />+</th><th>&nbsp;</th><th>PCS<br />+</th><th>CO</th><th>PR</th><th>SK</th><th>Ded.<br />-</th><th>StN.</th></tr>
<tr><td>1</td><td>Q</td><td><a href="/bios/isufs00111111.htm">Elina GOIDINA</a></td><td>EST</td><td>64.87</td><td>38.04</td><td></td><td>26.83</td><td>6.75</td><td>6.67</td><td>6.75</td><td>0.00</td><td>#7</td></tr>
<tr><td>31</td><td></td><td><a href="/bios/isufs00222222.htm">Ginevra CARRARO</a></td><td>ITA</td><td>32.34</td><td>12.28</td><td></td><td>21.06</td><td>5.50</td><td>5.00</td><td>5.33</td><td>1.00</td><td>#32</td></tr>
</table>
"""


OFFICIALS_HTML = """
<tr class="caption2"><td>Men - Short Program</td></tr>
<table><tr><th>Function</th><th>Name</th><th>Nation</th></tr>
<tr><td>Referee</td><td>Ms. Adriana ORDEANU</td><td><table><tr><td>ISU</td></tr></table></td></tr>
<tr><td>Judge No.5</td><td>Ms. Tanay OZKAN SILAOGLU</td><td><table><tr><td>TUR</td></tr></table></td></tr>
</table>
"""


def test_parse_fs_manager_index() -> None:
    parsed = parse_index(INDEX_HTML, "https://example.test/2024/ISU/index.htm")

    assert parsed["source_profile"] == "fs_manager"
    assert parsed["source_context"]["host"] == "example.test"
    assert parsed["source_context"]["competition_stream"] == "ISU"
    assert parsed["event"]["name"] == "Denkova-Staviski Cup 2024"
    assert parsed["event"]["location"] == "Sofia / BUL"
    assert parsed["event_protocol_pdf"]["href"] == "denk2024_protocol.pdf"
    assert parsed["categories"][0]["segments"][0]["judges_scores_pdf"]["href"].endswith(".pdf")
    assert parsed["schedule"][0]["date"] == "08.11.2024"


def test_parse_fs_manager_entries() -> None:
    parsed = parse_entries(ENTRIES_HTML, "https://example.test/2024/ISU/CAT001EN.htm")

    assert parsed["entries"][0]["source_skater_id"] == "isufs00009567"
    assert parsed["entries"][0]["bio_url"] == "https://example.test/bios/isufs00009567.htm"
    assert parsed["entries"][0]["representation_type"] == "nation"
    assert parsed["entries"][0]["representation_value"] == "AUT"


def test_parse_fs_manager_club_entries_from_profile_settings() -> None:
    parsed = parse_entries(
        CLUB_ENTRIES_HTML,
        "https://www.bsf.bg/figure-skating/ice-peak-trophy/18-19.04.2026/CAT001EN.htm",
    )

    assert parsed["entries"][0]["club"] == "Elit"
    assert parsed["entries"][0]["nation"] == "BUL"
    assert parsed["entries"][0]["representation_type"] == "club"
    assert parsed["entries"][0]["representation_value"] == "Elit"


def test_parse_fs_manager_club_profile_entries_without_club_need_review() -> None:
    parsed = parse_entries(
        CLUB_ENTRIES_WITH_EMPTY_CLUB_HTML,
        "https://www.bsf.bg/figure-skating/ice-peak-trophy/18-19.04.2026/CAT001EN.htm",
    )

    assert parsed["entries"][0]["club"] == ""
    assert parsed["entries"][0]["nation"] == "ROU"
    assert parsed["entries"][0]["representation_type"] == "club"
    assert parsed["entries"][0]["representation_value"] is None


def test_parse_fs_manager_category_result() -> None:
    parsed = parse_category_result(RESULT_HTML)

    assert parsed["results"][0]["points"] == 234.31
    assert parsed["results"][0]["segment_places"] == {"SP": 1, "FS": 1}


def test_parse_fs_manager_club_category_result_from_profile_settings() -> None:
    parsed = parse_category_result(
        CLUB_RESULT_HTML,
        "https://www.bsf.bg/figure-skating/ice-peak-trophy/18-19.04.2026/CAT001RS.htm",
    )

    assert parsed["results"][0]["club"] == "Varna"
    assert parsed["results"][0]["nation"] == "BUL"
    assert parsed["results"][0]["points"] == 19.83
    assert parsed["results"][0]["segment_places"] == {"FS": 1}
    assert parsed["results"][0]["representation_type"] == "club"


def test_parse_fs_manager_segment_result() -> None:
    parsed = parse_segment_result(SEGMENT_HTML)

    assert parsed["category"] == "Men"
    assert parsed["segment"] == "Short Program"
    assert parsed["results"][0]["components"] == {"co": 7.67, "pr": 7.42, "sk": 7.5}


def test_parse_fs_manager_segment_result_with_qualification_column() -> None:
    parsed = parse_segment_result(SEGMENT_WITH_QUALIFICATION_HTML)

    assert parsed["category"] == "Junior Women"
    assert parsed["segment"] == "Short Program"
    assert parsed["results"][0]["name"] == "Elina GOIDINA"
    assert parsed["results"][0]["qualification"] == "Q"
    assert parsed["results"][0]["nation"] == "EST"
    assert parsed["results"][1]["name"] == "Ginevra CARRARO"
    assert parsed["results"][1]["qualification"] == ""
    assert parsed["results"][1]["nation"] == "ITA"
    assert parsed["results"][1]["components"] == {"co": 5.5, "pr": 5.0, "sk": 5.33}


def test_parse_fs_manager_officials() -> None:
    parsed = parse_officials(OFFICIALS_HTML)

    assert parsed["officials"][1]["role_group"] == "judge"
    assert parsed["officials"][1]["judge_number"] == 5
