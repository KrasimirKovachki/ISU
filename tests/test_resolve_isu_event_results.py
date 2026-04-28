from __future__ import annotations

from unittest.mock import patch

from scripts.resolve_isu_event_results import detail_result_summary


class _CheckResult:
    ok = True
    status = "passed"

    def summary(self):
        return {"content_kind": "html", "url_check_status": "passed"}


def test_detail_result_summary_dry_run_shape_with_validation() -> None:
    html = r"""
    <html><body>
      <script>
      self.__next_f.push([1,"{\"pageinfos\":{\"name\":\"International Adult Competition\",\"display_date\":\"29 Jun - 4 Jul, 2025\",\"city\":\"Oberstdorf\",\"country_code\":\"GER\",\"discipline_title\":\"Figure Skating\",\"detail_result_url\":\"https:\/\/www.deu-event.de\/results\/adult2025\/\"}}"]);
      </script>
    </body></html>
    """

    with (
        patch("scripts.resolve_isu_event_results.fetch_text", return_value=html),
        patch("scripts.resolve_isu_event_results.preflight_result_url", return_value=_CheckResult()),
    ):
        summary = detail_result_summary(
            "https://www.isu-skating.com/figure-skating/events/eventdetail/international-adult-competition/",
            validate=True,
        )

    assert summary["event_name"] == "International Adult Competition"
    assert summary["result_url"] == "https://www.deu-event.de/results/adult2025/"
    assert summary["parser_profile"] == "fs_manager"
    assert summary["registry_status"] == "ready_for_import"
    assert summary["validation_status"] == "passed"
