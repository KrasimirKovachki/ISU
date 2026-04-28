from __future__ import annotations

from dataclasses import dataclass, field
import re
import ssl
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen


USER_AGENT = "isu-skating-source-check/0.1"


@dataclass
class SourceCheckResult:
    url: str
    ok: bool
    status: str
    content_kind: str
    body: bytes = b""
    resolved_url: str | None = None
    resolution: str | None = None
    reason: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def parse_url(self) -> str:
        return self.resolved_url or self.url

    def summary(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "url_check_status": self.status,
            "content_kind": self.content_kind,
        }
        if self.resolved_url:
            result["resolved_url"] = self.resolved_url
        if self.resolution:
            result["resolution"] = self.resolution
        if self.reason:
            result["reason"] = self.reason
        if self.metadata:
            result.update(self.metadata)
        return result


def fetch_bytes(url: str, timeout: int = 30) -> tuple[bytes, str]:
    request = Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urlopen(request, timeout=timeout) as response:
            final_url = response.geturl()
            return response.read(), final_url
    except URLError as exc:
        if "CERTIFICATE_VERIFY_FAILED" not in str(exc.reason):
            raise
        with urlopen(request, timeout=timeout, context=ssl._create_unverified_context()) as response:  # noqa: S323
            final_url = response.geturl()
            return response.read(), final_url


def decode_html(data: bytes) -> str:
    return data.decode("windows-1252", errors="replace")


def meta_refresh_url(html: str, base_url: str) -> str | None:
    match = re.search(
        r'<meta[^>]+http-equiv=["\']?refresh["\']?[^>]+content=["\'][^"\']*url=([^"\']+)["\']',
        html,
        flags=re.IGNORECASE,
    )
    if not match:
        match = re.search(
            r'<meta[^>]+content=["\'][^"\']*url=([^"\']+)["\'][^>]+http-equiv=["\']?refresh["\']?',
            html,
            flags=re.IGNORECASE,
        )
    return urljoin(base_url, match.group(1).strip()) if match else None


def old_isucalcfs_main_candidates(url: str) -> list[str]:
    candidates = [urljoin(url, "pages/main.html"), urljoin(url, "pages/main.htm")]
    unique: list[str] = []
    for candidate in candidates:
        if candidate not in unique:
            unique.append(candidate)
    return unique


def is_react_shell(html: str) -> bool:
    lower = html.lower()
    return "<!doctype html>" in lower and 'id="root"' in lower and "/assets/index-" in lower


def is_old_isucalcfs_wrapper(html: str) -> bool:
    lower = html.lower()
    return "scripts/results" in lower and "pages/main" not in lower and "<body></body>" in lower.replace(" ", "")


def preflight_result_url(url: str, max_redirect_depth: int = 3) -> SourceCheckResult:
    try:
        body, final_url = fetch_bytes(url)
    except HTTPError as exc:
        return SourceCheckResult(
            url=url,
            ok=False,
            status="failed",
            content_kind="unreachable",
            reason=f"http_error_{exc.code}",
            metadata={"http_status": exc.code},
        )
    except URLError as exc:
        return SourceCheckResult(
            url=url,
            ok=False,
            status="failed",
            content_kind="unreachable",
            reason=f"url_error: {exc.reason}",
        )
    except Exception as exc:  # noqa: BLE001 - source check diagnostics should preserve the reason.
        return SourceCheckResult(url=url, ok=False, status="failed", content_kind="unreachable", reason=str(exc))

    parse_url = final_url or url
    if body.startswith(b"%PDF") or urlparse(parse_url).path.lower().endswith(".pdf"):
        if body.startswith(b"%PDF"):
            return SourceCheckResult(url=url, ok=True, status="passed", content_kind="pdf", body=body, resolved_url=parse_url if parse_url != url else None)
        return SourceCheckResult(url=url, ok=False, status="failed", content_kind="html_fallback", body=body, reason="pdf URL did not return PDF bytes")

    html = decode_html(body)
    refresh = meta_refresh_url(html, parse_url)
    if refresh and max_redirect_depth > 0:
        resolved = preflight_result_url(refresh, max_redirect_depth=max_redirect_depth - 1)
        resolved.url = url
        resolved.resolved_url = resolved.resolved_url or refresh
        resolved.resolution = resolved.resolution or "meta_refresh"
        return resolved

    if is_react_shell(html):
        return SourceCheckResult(url=url, ok=False, status="failed", content_kind="site_fallback", body=body, reason="site fallback React shell, not a result index")

    if is_old_isucalcfs_wrapper(html):
        for candidate in old_isucalcfs_main_candidates(parse_url):
            try:
                candidate_body, candidate_final_url = fetch_bytes(candidate)
            except Exception:
                continue
            candidate_html = decode_html(candidate_body)
            if "isucalcfs" in candidate_html.lower():
                return SourceCheckResult(
                    url=url,
                    ok=True,
                    status="passed",
                    content_kind="html",
                    body=candidate_body,
                    resolved_url=candidate_final_url or candidate,
                    resolution="scripts_results_wrapper",
                )
        return SourceCheckResult(url=url, ok=False, status="failed", content_kind="html_wrapper", body=body, reason="old ISUCalcFS wrapper did not expose pages/main.html or pages/main.htm")

    lower = html.lower()
    if "isucalcfs" in lower or "fs manager" in lower or "judgesdetailsperskater" in lower:
        return SourceCheckResult(url=url, ok=True, status="passed", content_kind="html", body=body, resolved_url=parse_url if parse_url != url else None)

    return SourceCheckResult(url=url, ok=False, status="failed", content_kind="unknown_html", body=body, reason="source URL did not look like a supported result index")
