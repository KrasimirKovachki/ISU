from __future__ import annotations

import unittest
from unittest.mock import patch
from urllib.error import URLError

from isu_parser.source_check import preflight_result_url


class _Response:
    def __init__(self, url: str, body: bytes) -> None:
        self.url = url
        self.body = body

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def geturl(self) -> str:
        return self.url

    def read(self) -> bytes:
        return self.body


class SourceCheckTest(unittest.TestCase):
    def test_rejects_react_shell_before_parser_validation(self) -> None:
        body = b'<!doctype html><html><head><script src="/assets/index-test.js"></script></head><body><div id="root"></div></body></html>'

        with patch("isu_parser.source_check.urlopen", return_value=_Response("https://example.test/index.htm", body)):
            result = preflight_result_url("https://example.test/index.htm")

        self.assertFalse(result.ok)
        self.assertEqual(result.content_kind, "site_fallback")
        self.assertEqual(result.reason, "site fallback React shell, not a result index")

    def test_resolves_old_isucalcfs_wrapper_before_validation(self) -> None:
        wrapper = b'<html><head><script src="scripts/results.es5.min.js"></script></head><body></body></html>'
        main = b'<html><head><meta name="GENERATOR" content="ISUCalcFS 3.6.5"></head><body>ISUCalcFS</body></html>'

        def fake_urlopen(request, timeout=30):
            url = request.full_url
            if url.endswith("/index.htm"):
                return _Response(url, wrapper)
            if url.endswith("/pages/main.html"):
                return _Response(url, main)
            return _Response(url, b"missing")

        with patch("isu_parser.source_check.urlopen", side_effect=fake_urlopen):
            result = preflight_result_url("https://example.test/index.htm")

        self.assertTrue(result.ok)
        self.assertEqual(result.resolved_url, "https://example.test/pages/main.html")
        self.assertEqual(result.resolution, "scripts_results_wrapper")

    def test_rejects_pdf_url_that_returns_html(self) -> None:
        with patch("isu_parser.source_check.urlopen", return_value=_Response("https://example.test/file.pdf", b"<html></html>")):
            result = preflight_result_url("https://example.test/file.pdf")

        self.assertFalse(result.ok)
        self.assertEqual(result.content_kind, "html_fallback")

    def test_retries_certificate_verification_failure_with_unverified_context(self) -> None:
        calls = []

        def fake_urlopen(request, timeout=30, context=None):
            calls.append(context)
            if context is None:
                raise URLError("CERTIFICATE_VERIFY_FAILED")
            return _Response(request.full_url, b"<html><body>FS Manager JudgesDetailsperSkater.pdf</body></html>")

        with patch("isu_parser.source_check.urlopen", side_effect=fake_urlopen):
            result = preflight_result_url("https://example.test/index.htm")

        self.assertTrue(result.ok)
        self.assertEqual(result.content_kind, "html")
        self.assertEqual(len(calls), 2)
        self.assertIsNone(calls[0])
        self.assertIsNotNone(calls[1])


if __name__ == "__main__":
    unittest.main()
