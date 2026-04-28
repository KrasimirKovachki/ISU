import unittest

from isu_parser.old_isucalcfs import (
    parse_category_result,
    parse_entries,
    parse_index,
    parse_officials,
    parse_segment_result,
    validate_event_index,
)
from isu_parser.pdf_scores import (
    _parse_pdfplumber_skater_table,
    parse_judges_scores_text,
    validate_judges_scores,
)


INDEX_HTML = """
<html><head><title>Denkova-Staviski Cup 2013 </title></head>
<div align="center"><p><h2>Denkova-Staviski Cup 2013 </h2>
<p><h3>Sofia </h3>
<p><h3>29.11.2013 �. - 1.12.2013 �. </h3>
<p><h3>Winter Sport Palace </h3>
<body bgcolor='#FFFFFF'>
<table cellspacing=1>
<tr bgcolor='CC33CC'> <th>Category</th><th>Segment</th><th>&nbsp;</th><th>&nbsp;</th><th>&nbsp;</th></tr>
<tr bgcolor='#88FFFF'> <td>Advanced Novice Boys</td><td></td><td><a href=CAT001EN.HTM> Entries </a></td><td><a href=CAT001RS.HTM> Result </a></td><td>&nbsp;</td></tr>
<tr bgcolor='#88FFFF'> <td></td><td>Short Program</td><td><a href=SEG001OF.HTM>Officials<a></td><td><a href=SEG001.HTM> Starting Order / Result Details <a></td><td><a href=AdvancedNoviceBoys_SP_Scores.pdf>Judges Scores&nbsp;(pdf)</a></td></tr>
</table>
<body><table cellspacing=1><caption><h3>Time Schedule</h3></caption>
<tr bgcolor='CC33CC'><th>Date</th><th>Time</th><th>Category</th><th>Segment</th></tr>
<tr bgcolor='999999'><td>30.11.2013 �.</td></tr>
<tr bgcolor='#88FFFF'> <td></td><td>10:10:00</td><td>Advanced Novice Boys</td><td><a href=SEG001.HTM>Short Program</a></td></tr>
</table>
</body></html>
"""


INDEX_WITH_TIME_AND_SCORE_PDFS_HTML = """
<html><head><title>Denkova-Staviski Cup 2018</title></head>
<div align="center"><p><h2>Denkova-Staviski Cup 2018</h2>
<p><h3>Sofia, Bulgaria</h3>
<p><h3>27/11/2018 - 02/12/2018</h3>
<p><h3>Winter Sports Palace</h3>
<body bgcolor='#FFFFFF'>
<table cellspacing=1>
<tr bgcolor='CC33CC'> <th>Category</th><th>Segment</th><th>&nbsp;</th><th>&nbsp;</th><th>&nbsp;</th><th>&nbsp;</th></tr>
<tr bgcolor='#88FFFF'> <td>Advanced Novice Boys</td><td></td><td><a href=CAT001EN.HTM> Entries </a></td><td><a href=CAT001RS.HTM> Result </a></td><td>&nbsp;</td><td>&nbsp;</td></tr>
<tr bgcolor='#88FFFF'> <td></td><td>Short Program</td><td><a href=SEG001OF.HTM>Officials<a></td><td><a href=SEG001.HTM> Starting Order / Result Details <a></td><td><a href=DS2018_AdvancedNoviceBoys_SP_TimeSchedule.pdf>Time Schedule&nbsp;(pdf)</a></td><td><a href=DS2018_AdvancedNoviceBoys_SP_Scores.pdf>Judges Scores&nbsp;(pdf)</a></td></tr>
</table>
</body></html>
"""


RESULT_PDF_ONLY_INDEX_HTML = """
<html><head><title>National Championships 10-14.02.2016</title></head>
<div align="center"><h2>National Championships 10-14.02.2016</h2>
<h3>Sofia, Bulgaria</h3><h3>10.2.2016 - 14.2.2016</h3><h3>Winter Sport Palace</h3>
<table cellspacing="1">
<tr bgcolor="CC33CC"><th>Category</th><th>&nbsp;</th></tr>
<tr bgcolor="#FF8888"><td>Klas I Ladies</td><td><a href="KlasILadies_FS_Result.pdf"> Result </a></td></tr>
</table></div></html>
"""


ENTRIES_HTML = """
<table border=1 cellspacing=1><caption><h2>Denkova-Staviski Cup 2013</h2> <h2>Advanced Novice Boys</h2> <h3>Entries</h3></caption>
<tr><TH align="center">No.</th><th>Name</th><th>Nat.</th></tr>
<tr><td align="center">1</td><td><a>Yann FRECHON</a></td><td align="center">FRA</td></tr>
<tr><td align="center">5</td><td><a>Daniel   GRASSL</a></td><td align="center">ITA</td></tr>
</table>
"""


CATEGORY_RESULT_HTML = """
<table border=1 cellspacing=1><caption><h2>Denkova-Staviski Cup 2013</h2> <h2>Advanced Novice Boys</h2> <h3>Result</h3></caption>
<tr><th>FPl.</th><th>Name</th><th>Nation</th><th>Points</th><th>SP</th><th>FS</th></tr>
<tr><td align="center">1</td><td><a>Daniel   GRASSL</a></td><td align="center">ITA</td><td align="right">90.96</td><td align="center">2</td><td align="center">1</td></tr>
</table>
"""


CATEGORY_RESULT_WITH_CLUB_HTML = """
<table border=1 cellspacing=1><caption><h2>Denkova-Staviski Cup 2014</h2> <h2>Advanced Novice Girls</h2> <h3>Result</h3></caption>
<tr><th>FPl.</th><th>Name</th><th>Club</th><th>Nation</th><th>Points</th><th>SP</th><th>FS</th></tr>
<tr><td align="center">2</td><td><a>Alexandra FEIGIN</a></td><td>&nbsp;</td><td align="center">BUL</td><td align="right">87.03</td><td align="center">2</td><td align="center">4</td></tr>
</table>
"""


SEGMENT_HTML = """
<table border="0" cellspacing="1"><caption><h2>Denkova-Staviski Cup 2013</h2><h2>Advanced Novice Boys - Short Program</h2><h3></h3></caption></table>
<table border="1" cellspacing="1"><caption><h3>Result Details</h3></caption>
<tr><th>Pl.</th><th>Name</th><th>Nation</th><th>TSS<br />=</th><th>TES<br />+</th><th>&nbsp;</th><th>PCS<br />+</th><th>SS</th><th>TR</th><th>PE</th><th>IN</th><th>Deduction<br />-</th><th>StN.</th></tr>
<tr><td align="center">1</td><td><a>Adam SIAO HIM FA</a><br /></td><td align="center">FRA</td><td align="right">34.07</td><td align="right">18.54</td><td>&nbsp;</td><td align="right">15.53</td><td align="right">4.33</td><td align="right">4.08</td><td align="right">4.42</td><td align="right">4.42</td><td align="right">0.00</td><td align="center">#4 </td></tr>
</table>
"""


SEGMENT_WITH_TWO_COMPONENTS_HTML = """
<table border="0" cellspacing="1"><caption><h2>Denkova-Staviski Cup 2014 Non ISU</h2><h2>Cubs Boys - Free Skating</h2><h3></h3></caption></table>
<table border="1" cellspacing="1"><caption><h3>Result Details</h3></caption>
<tr><th>Pl.</th><th>Name</th><th>Nation</th><th>TSS<br />=</th><th>TES<br />+</th><th>&nbsp;</th><th>PCS<br />+</th><th>SS</th><th>PE</th><th>Deduction<br />-</th><th>StN.</th></tr>
<tr><td align="center">1</td><td><a>Kemal TONAY</a><br /></td><td align="center">TUR</td><td align="right">28.41</td><td align="right">12.90</td><td>&nbsp;</td><td align="right">17.51</td><td align="right">3.33</td><td align="right">3.67</td><td align="right">2.00</td><td align="center">#5</td></tr>
</table>
"""


SEGMENT_WITH_CLUB_HTML = """
<table border="0" cellspacing="1"><caption><h2>National Championship 20-22.12.2013</h2><h2>Klas I Men - Short Program</h2><h3></h3></caption></table>
<table border="1" cellspacing="1"><caption><h3>Result Details</h3></caption>
<tr><th>Pl.</th><th>Name</th><th>Club</th><th>Nation</th><th>TSS<br />=</th><th>TES<br />+</th><th>&nbsp;</th><th>PCS<br />+</th><th>SS</th><th>TR</th><th>PE</th><th>CH</th><th>IN</th><th>Deduction<br />-</th><th>StN.</th></tr>
<tr><td align="center">1</td><td><a>Yasen PETKOV</a><br />Dance on Ice DS<br /></td><td>DS</td><td align="center">BUL</td><td align="right">54.80</td><td align="right">27.96</td><td>&nbsp;</td><td align="right">26.84</td><td align="right">5.50</td><td align="right">5.08</td><td align="right">5.42</td><td align="right">5.42</td><td align="right">5.42</td><td align="right">0.00</td><td align="center">#2 </td></tr>
</table>
"""


OFFICIALS_HTML = """
<table border=1 cellspacing=1><caption><h2>Denkova-Staviski Cup 2013</h2> <h2>Advanced Novice Boys - Short Program</h2> <h3></h3><h3>Panel of Officials</h3></caption>
<tr><th>Function</th><th>Name</th><th>Nation</th></tr>
<tr><td>Referee</td><td>Mr. Gavril VELCHEV </td><td align="center">ISU</td></tr>
<tr><td>&nbsp;</td><td>&nbsp;</td><td>&nbsp;</td></tr>
<tr><td>Judge No.1</td><td>Mr. Gavril VELCHEV</td><td align="center">ISU</td></tr>
</table>
"""


PDF_TEXT = """
Denkova-Staviski Cup 2013
ADVANCED NOVICE BOYS SHORT PROGRAM         JUDGES DETAILS PER SKATER
Starting
Number
Nation
Name
Rank
Total
Segment
Score
Total
Element
Score
Total
Deductions
Total
Program  Component
Score (factored)
Anil CETINBAS
TUR
23.93
1.00
12.83
12.10
7
5
printed: 11/30/2013 10:50:30AM
"""


FS_MANAGER_PDF_TEXT = """
DENKOVA-STAVISKI CUP 2024JUDGES DETAILS PER SKATERMEN SHORT PROGRAM
printed:   08.11.2024 22:40
RankName NationStartingNumberTotalSegmentScoreTotalElementScoreTotal ProgramComponent Score(factored)TotalDeductions1Lev VINOKUR ISR 4 82.9745.24 37.73 0.00#Executed Elements
Info BaseValueGOEJ1J2J3J4J5J6J7J8J9 Ref.Scores ofPanel
RankName NationStartingNumberTotalSegmentScoreTotalElementScoreTotal ProgramComponent Score(factored)TotalDeductions2Burak DEMIRBOGATUR 12 71.2136.55 34.66 0.00#Executed Elements
RankName NationStartingNumberTotalSegmentScoreTotalElementScoreTotal ProgramComponent Score(factored)TotalDeductions4Fedir KULISH LAT 10 66.0335.03 32.00-1.00#Executed Elements
"""


FS_MANAGER_CLUB_PDF_TEXT = """
ICE PEAK TROPHY 04.2026JUDGES DETAILS PER SKATERRECREATIONAL A-MINI GIRLS FREE SKATING
printed:   18.04.2026 12:47
RankName NationStartingNumberTotalSegmentScoreTotalElementScoreTotal ProgramComponentScore (factored)TotalDeductions1Alis YORDANOVABUL 9 19.837.02 12.810.00#Executed Elements
Info BaseValueGOEJ1J2J3J4J5J6J7J8J9 Ref.Scores ofPanel
"""


PDFPLUMBER_SKATER_TABLE = [
    [
        "Total Total Total Program\nStarting Total\nRank Name Nation Segment Element Component Score\nNumber Deductions\nScore Score (factored)\n1 Lev VINOKUR ISR 4 82.97 45.24 37.73 0.00"
    ],
    [
        "# Executed Elements ofnI Base Scores of GOE J1 J2 J3 J4 J5 J6 J7 J8 J9 Ref.\n"
        "Value Panel\n"
        "1 4T 9.50 2.53 3 2 2 3 3 12.03\n"
        "2 1A* * 0.00 0.00 - - - - - 0.00\n"
        "3 2Ab b 3.30 0.00 -1 0 0 0 0 1.00 4.30\n"
        "4 3F!<+COMBO F !|< 4.24 -2.12 -5 -5 -5 -5 -5 2.12\n"
        "4 3F+3T 10.45 x -1.06 -2 -2 -2 -2 -2 9.39\n"
        "39.85 45.24\n"
        "Program Components Factor\n"
        "Composition 1.67 8.00 7.75 7.50 7.75 7.00 7.67\n"
        "Skating Skills 1.67 8.00 7.50 7.25 7.75 6.75 7.50\n"
        "Judges Total Program Component Score (factored) 37.73"
    ],
    ["Deductions: 0.00"],
]


PDFPLUMBER_CLUB_SKATER_TABLE = [
    [
        "Total Total Total Program\nStarting Total\nRank Name Nation Segment Element Component\nNumber Deductions\nScore Score Score (factored)\n3 Mariya OBRETENOVA BUL 4 17.40 6.21 11.69 -0.50"
    ],
    [
        "# Executed Elements ofnI Base Scores of GOE J1 J2 J3 J4 J5 J6 J7 J8 J9 Ref.\n"
        "Value Panel\n"
        "1 SSpB F 1.10 -0.55 -5 -5 -5 0.55\n"
        "2 1S 0.40 0.01 0 0 1 0.41\n"
        "6.63 6.21\n"
        "Program Components Factor\n"
        "Composition 1.67 3.00 2.75 1.75 2.50\n"
        "Presentation 1.67 2.75 2.75 1.50 2.33\n"
        "Judges Total Program Component Score (factored) 11.69"
    ],
    ["Deductions: Falls -0.50 (1) -0.50"],
]


class OldISUCalcFSTest(unittest.TestCase):
    def test_parse_index_and_validate(self) -> None:
        parsed = parse_index(INDEX_HTML, "https://example.test/2013/ISU/index.htm")

        self.assertEqual(parsed["event"]["name"], "Denkova-Staviski Cup 2013")
        self.assertEqual(parsed["event"]["location"], "Sofia")
        self.assertEqual(parsed["event"]["date_range"], "29.11.2013 - 1.12.2013")
        self.assertEqual(parsed["categories"][0]["entries"]["url"], "https://example.test/2013/ISU/CAT001EN.HTM")
        self.assertEqual(parsed["categories"][0]["segments"][0]["details"]["href"], "SEG001.HTM")
        self.assertEqual(parsed["schedule"][0]["date"], "30.11.2013")
        self.assertEqual(validate_event_index(parsed), [])

    def test_parse_index_prefers_judges_scores_when_time_schedule_is_present(self) -> None:
        parsed = parse_index(INDEX_WITH_TIME_AND_SCORE_PDFS_HTML, "https://example.test/2018/NonISU/pages/main.htm")
        link = parsed["categories"][0]["segments"][0]["judges_scores_pdf"]

        self.assertEqual(link["text"], "Judges Scores (pdf)")
        self.assertEqual(link["href"], "DS2018_AdvancedNoviceBoys_SP_Scores.pdf")

    def test_parse_index_with_result_pdf_only_categories(self) -> None:
        parsed = parse_index(RESULT_PDF_ONLY_INDEX_HTML, "https://example.test/2016/index.htm")
        category = parsed["categories"][0]

        self.assertEqual(category["name"], "Klas I Ladies")
        self.assertIsNone(category["entries"])
        self.assertEqual(category["result"]["href"], "KlasILadies_FS_Result.pdf")
        self.assertEqual(category["source_shape"], "category_result_pdf_only")
        issues = validate_event_index(parsed)
        self.assertIn("missing_category_entries", {issue["code"] for issue in issues})
        self.assertNotIn("missing_categories", {issue["code"] for issue in issues})

    def test_parse_entries(self) -> None:
        parsed = parse_entries(ENTRIES_HTML)

        self.assertEqual(parsed["category"], "Advanced Novice Boys")
        self.assertEqual(parsed["entries"][1], {"entry_no": 5, "name": "Daniel GRASSL", "nation": "ITA"})

    def test_parse_category_result(self) -> None:
        parsed = parse_category_result(CATEGORY_RESULT_HTML)

        self.assertEqual(parsed["results"][0]["points"], 90.96)
        self.assertEqual(parsed["results"][0]["segment_places"], {"SP": 2, "FS": 1})

    def test_parse_category_result_with_club_column(self) -> None:
        parsed = parse_category_result(CATEGORY_RESULT_WITH_CLUB_HTML)

        self.assertEqual(parsed["results"][0]["club"], "")
        self.assertEqual(parsed["results"][0]["nation"], "BUL")
        self.assertEqual(parsed["results"][0]["points"], 87.03)
        self.assertEqual(parsed["results"][0]["segment_places"], {"SP": 2, "FS": 4})

    def test_parse_segment_result(self) -> None:
        parsed = parse_segment_result(SEGMENT_HTML)

        self.assertEqual(parsed["category"], "Advanced Novice Boys")
        self.assertEqual(parsed["segment"], "Short Program")
        self.assertEqual(parsed["results"][0]["name"], "Adam SIAO HIM FA")
        self.assertEqual(parsed["results"][0]["tss"], 34.07)
        self.assertEqual(parsed["results"][0]["starting_number"], 4)

    def test_parse_segment_result_with_two_components(self) -> None:
        parsed = parse_segment_result(SEGMENT_WITH_TWO_COMPONENTS_HTML)

        self.assertEqual(parsed["category"], "Cubs Boys")
        self.assertEqual(parsed["segment"], "Free Skating")
        self.assertEqual(parsed["results"][0]["name"], "Kemal TONAY")
        self.assertEqual(parsed["results"][0]["tss"], 28.41)
        self.assertEqual(parsed["results"][0]["components"], {"ss": 3.33, "pe": 3.67})
        self.assertEqual(parsed["results"][0]["starting_number"], 5)

    def test_parse_segment_result_with_club_and_nation_columns(self) -> None:
        parsed = parse_segment_result(SEGMENT_WITH_CLUB_HTML)

        self.assertEqual(parsed["results"][0]["name"], "Yasen PETKOV")
        self.assertEqual(parsed["results"][0]["club"], "DS")
        self.assertEqual(parsed["results"][0]["club_display_name"], "Dance on Ice DS")
        self.assertEqual(parsed["results"][0]["nation"], "BUL")
        self.assertEqual(parsed["results"][0]["tss"], 54.80)
        self.assertEqual(parsed["results"][0]["tes"], 27.96)
        self.assertEqual(parsed["results"][0]["pcs"], 26.84)

    def test_parse_segment_result_with_empty_result_table(self) -> None:
        parsed = parse_segment_result(
            """
            <html><body>
              <table border="0" cellspacing="1">
                <caption><h2>Denkova-Staviski Cup 2018</h2><h2>Chicks Girls - Free Skating</h2></caption>
              </table>
              <table border="1" cellspacing="1"><caption><h3>Result Details</h3></caption></table>
            </body></html>
            """
        )

        self.assertEqual(parsed["category"], "Chicks Girls")
        self.assertEqual(parsed["segment"], "Free Skating")
        self.assertEqual(parsed["results"], [])

    def test_parse_officials(self) -> None:
        parsed = parse_officials(OFFICIALS_HTML)

        self.assertEqual(
            parsed["officials"],
            [
                {
                    "function": "Referee",
                    "role_group": "referee",
                    "judge_number": None,
                    "name": "Mr. Gavril VELCHEV",
                    "nation": "ISU",
                },
                {
                    "function": "Judge No.1",
                    "role_group": "judge",
                    "judge_number": 1,
                    "name": "Mr. Gavril VELCHEV",
                    "nation": "ISU",
                },
            ],
        )

    def test_parse_judges_scores_pdf_text(self) -> None:
        parsed = parse_judges_scores_text(PDF_TEXT)

        self.assertEqual(parsed["event_name"], "Denkova-Staviski Cup 2013")
        self.assertEqual(parsed["category"], "ADVANCED NOVICE BOYS")
        self.assertEqual(parsed["segment"], "SHORT PROGRAM")
        self.assertEqual(parsed["skaters"][0]["name"], "Anil CETINBAS")
        self.assertEqual(parsed["skaters"][0]["rank"], 7)
        self.assertEqual(parsed["skaters"][0]["starting_number"], 5)
        self.assertEqual(validate_judges_scores(parsed), [])

    def test_parse_fs_manager_judges_scores_pdf_text(self) -> None:
        parsed = parse_judges_scores_text(FS_MANAGER_PDF_TEXT)

        self.assertEqual(parsed["event_name"], "DENKOVA-STAVISKI CUP 2024")
        self.assertEqual(parsed["category"], "MEN")
        self.assertEqual(parsed["segment"], "SHORT PROGRAM")
        self.assertEqual(parsed["skaters"][0]["name"], "Lev VINOKUR")
        self.assertEqual(parsed["skaters"][0]["total_segment_score"], 82.97)
        self.assertEqual(parsed["skaters"][0]["total_element_score"], 45.24)
        self.assertEqual(parsed["skaters"][1]["name"], "Burak DEMIRBOGA")
        self.assertEqual(parsed["skaters"][1]["nation"], "TUR")
        self.assertEqual(parsed["skaters"][2]["name"], "Fedir KULISH")
        self.assertEqual(parsed["skaters"][2]["total_deductions"], 1.0)
        self.assertEqual(validate_judges_scores(parsed), [])

    def test_parse_fs_manager_club_judges_scores_pdf_text(self) -> None:
        parsed = parse_judges_scores_text(FS_MANAGER_CLUB_PDF_TEXT)

        self.assertEqual(parsed["event_name"], "ICE PEAK TROPHY 04.2026")
        self.assertEqual(parsed["category"], "RECREATIONAL A-MINI GIRLS")
        self.assertEqual(parsed["segment"], "FREE SKATING")
        self.assertEqual(parsed["skaters"][0]["name"], "Alis YORDANOVA")
        self.assertEqual(parsed["skaters"][0]["nation"], "BUL")
        self.assertEqual(parsed["skaters"][0]["total_element_score"], 7.02)
        self.assertEqual(validate_judges_scores(parsed), [])

    def test_parse_pdfplumber_skater_table_details(self) -> None:
        parsed = _parse_pdfplumber_skater_table(PDFPLUMBER_SKATER_TABLE)

        self.assertIsNotNone(parsed)
        assert parsed is not None
        self.assertEqual(parsed["name"], "Lev VINOKUR")
        self.assertEqual(parsed["judge_count"], 5)
        self.assertEqual(parsed["base_value_total"], 39.85)
        self.assertEqual(parsed["element_score_total"], 45.24)
        self.assertEqual(parsed["elements"][0]["element_code"], "4T")
        self.assertEqual(parsed["elements"][0]["judge_scores"], [3, 2, 2, 3, 3])
        self.assertEqual(parsed["elements"][1]["element_code"], "1A*")
        self.assertEqual(parsed["elements"][1]["base_element_code"], "1A")
        self.assertEqual(parsed["elements"][1]["info"], "*")
        self.assertEqual(parsed["elements"][1]["markers"], ["*"])
        self.assertEqual(parsed["elements"][1]["judge_scores"], [None, None, None, None, None])
        self.assertEqual(parsed["elements"][2]["element_code"], "2Ab")
        self.assertEqual(parsed["elements"][2]["base_element_code"], "2A")
        self.assertEqual(parsed["elements"][2]["info"], "b")
        self.assertEqual(parsed["elements"][2]["markers"], ["b"])
        self.assertEqual(parsed["elements"][2]["judge_scores"], [-1, 0, 0, 0, 0])
        self.assertEqual(parsed["elements"][2]["bonus"], 1.0)
        self.assertEqual(parsed["elements"][3]["element_code"], "3F!<+COMBO")
        self.assertEqual(parsed["elements"][3]["base_element_code"], "3F+COMBO")
        self.assertEqual(parsed["elements"][3]["markers"], ["!", "<", "F"])
        self.assertEqual(parsed["elements"][4]["info"], "x")
        self.assertEqual(parsed["elements"][4]["markers"], ["x"])
        self.assertEqual(parsed["program_components"][0]["component"], "Composition")
        self.assertEqual(parsed["program_components"][0]["judge_scores"], [8.0, 7.75, 7.5, 7.75, 7.0])

    def test_parse_pdfplumber_club_skater_table_details(self) -> None:
        parsed = _parse_pdfplumber_skater_table(PDFPLUMBER_CLUB_SKATER_TABLE)

        self.assertIsNotNone(parsed)
        assert parsed is not None
        self.assertEqual(parsed["name"], "Mariya OBRETENOVA")
        self.assertEqual(parsed["judge_count"], 3)
        self.assertEqual(parsed["total_deductions"], 0.5)
        self.assertEqual(parsed["deductions_detail"]["total"], 0.5)
        self.assertEqual(parsed["elements"][0]["element_code"], "SSpB")
        self.assertEqual(parsed["elements"][0]["info"], "F")
        self.assertEqual(parsed["elements"][0]["judge_scores"], [-5, -5, -5])


if __name__ == "__main__":
    unittest.main()
