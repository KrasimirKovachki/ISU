from __future__ import annotations

import json
import re
import subprocess
import unicodedata
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


USER_AGENT = "isu-skating-video-pipeline/0.1"
API_BASE = "https://ice-stars.tv/api/play_v1"


class VideoPipelineError(RuntimeError):
    pass


class VideoAccessBlocked(VideoPipelineError):
    def __init__(self, reason: str, payload: dict[str, Any] | None = None) -> None:
        super().__init__(reason)
        self.reason = reason
        self.payload = payload or {}


@dataclass(frozen=True)
class TranscriptSegment:
    start: Decimal
    end: Decimal
    text: str


@dataclass(frozen=True)
class NameCandidate:
    skater_appearance_id: int | None
    raw_name: str
    normalized_name: str
    segment_id: int | None = None
    category_id: int | None = None


def normalize_match_text(value: str | None) -> str:
    if not value:
        return ""
    normalized = unicodedata.normalize("NFKD", value)
    normalized = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    normalized = normalized.upper()
    normalized = re.sub(r"[^A-Z0-9]+", " ", normalized)
    return re.sub(r"\s+", " ", normalized).strip()


def transcript_text_from_segments(segments: Iterable[TranscriptSegment]) -> str:
    return " ".join(segment.text.strip() for segment in segments if segment.text.strip())


def _fetch_json_response(url: str, *, headers: dict[str, str] | None = None) -> tuple[int, Any, str, dict[str, str]]:
    request = Request(
        url,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "application/json, text/plain, */*",
            **(headers or {}),
        },
    )
    try:
        with urlopen(request, timeout=60) as response:
            body = response.read().decode("utf-8")
            return response.status, json.loads(body), response.geturl(), dict(response.headers.items())
    except HTTPError as exc:
        payload = exc.read().decode("utf-8", errors="replace")
        try:
            parsed: Any = json.loads(payload)
        except json.JSONDecodeError:
            parsed = {"message": payload}
        return exc.code, parsed, exc.geturl(), dict(exc.headers.items()) if exc.headers else {}
    except URLError as exc:  # noqa: BLE001 - preserve low-level fetch reason for callers.
        raise VideoPipelineError(str(exc)) from exc


def resolve_media_object(
    *,
    company: str,
    media_object_slug: str,
    description_url: str,
) -> dict[str, Any]:
    params = urlencode(
        {
            "company": company,
            "media_object_slug": media_object_slug,
            "description_url": description_url,
        }
    )
    status, payload, _, _ = _fetch_json_response(f"{API_BASE}/media_object/watch?{params}")
    if status != 200 or not isinstance(payload, dict):
        raise VideoPipelineError(f"unexpected media_object/watch status: {status}")
    return payload


def request_stream_urls(
    *,
    company: str,
    media_object_ident: str,
    admin_access: bool = False,
) -> tuple[int, dict[str, Any]]:
    params = urlencode({"admin_access": str(bool(admin_access)).lower(), "company": company})
    status, payload, _, _ = _fetch_json_response(
        f"{API_BASE}/media_object/{media_object_ident}/request_stream_urls?{params}"
    )
    if not isinstance(payload, dict):
        payload = {"payload": payload}
    return status, payload


def request_download_url(*, company: str, media_object_ident: str) -> tuple[int, dict[str, Any]]:
    params = urlencode({"media_object": media_object_ident})
    status, payload, _, _ = _fetch_json_response(f"{API_BASE}/companies/{company}/download/video?{params}")
    if not isinstance(payload, dict):
        payload = {"payload": payload}
    return status, payload


def choose_download_url(download_payload: dict[str, Any]) -> str | None:
    download_url = download_payload.get("download_url")
    if isinstance(download_url, str) and download_url.startswith(("http://", "https://")):
        return download_url
    urls = download_payload.get("urls")
    if isinstance(urls, dict):
        for key in ("download_url", "url", "stream_url", "playback_url"):
            candidate = urls.get(key)
            if isinstance(candidate, str) and candidate.startswith(("http://", "https://")):
                return candidate
    return None


def choose_stream_url(stream_payload: dict[str, Any]) -> str | None:
    urls = stream_payload.get("urls")
    if isinstance(urls, dict):
        for key in ("download_url", "url", "stream_url", "playback_url"):
            candidate = urls.get(key)
            if isinstance(candidate, str) and candidate.startswith(("http://", "https://")):
                return candidate
    embedded = stream_payload.get("embedded")
    if isinstance(embedded, dict):
        for key in ("src", "url", "stream_url"):
            candidate = embedded.get(key)
            if isinstance(candidate, str) and candidate.startswith(("http://", "https://")):
                return candidate
    return None


def resolve_playback_url(
    *,
    company: str,
    media_object_slug: str,
    description_url: str,
    admin_access: bool = False,
) -> dict[str, Any]:
    media_object = resolve_media_object(
        company=company,
        media_object_slug=media_object_slug,
        description_url=description_url,
    )

    media_object_ident = str(media_object.get("id") or media_object.get("ident") or media_object_slug)
    download_status, download_payload = request_download_url(company=company, media_object_ident=media_object_ident)
    download_url = choose_download_url(download_payload) if download_status == 200 else None

    stream_status, stream_payload = request_stream_urls(
        company=company,
        media_object_ident=media_object_ident,
        admin_access=admin_access,
    )
    stream_url = choose_stream_url(stream_payload) if stream_status == 200 else None

    access_restriction = media_object.get("access_restriction")
    allow_download = bool(media_object.get("allow_download"))
    blocked = None
    if stream_status in {402, 403} or download_status in {402, 403} or access_restriction == "paywall":
        blocked = {
            "reason": "payment_required" if stream_status == 402 or download_status == 402 or access_restriction == "paywall" else "access_denied",
            "media_object": media_object,
            "download_response": download_payload,
            "stream_response": stream_payload,
        }

    return {
        "media_object": media_object,
        "media_object_ident": media_object_ident,
        "download_status": download_status,
        "download_payload": download_payload,
        "download_url": download_url,
        "stream_status": stream_status,
        "stream_payload": stream_payload,
        "stream_url": stream_url,
        "allow_download": allow_download,
        "access_restriction": access_restriction,
        "blocked": blocked,
    }


def download_binary(url: str, destination: Path, *, headers: dict[str, str] | None = None) -> Path:
    destination.parent.mkdir(parents=True, exist_ok=True)
    request = Request(
        url,
        headers={
            "User-Agent": USER_AGENT,
            **(headers or {}),
        },
    )
    try:
        with urlopen(request, timeout=120) as response, destination.open("wb") as output:
            while True:
                chunk = response.read(1024 * 1024)
                if not chunk:
                    break
                output.write(chunk)
            return destination
    except HTTPError as exc:
        payload = exc.read().decode("utf-8", errors="replace")
        raise VideoAccessBlocked(f"download HTTP {exc.code}", {"body": payload, "url": url}) from exc
    except URLError as exc:  # noqa: BLE001 - preserve fetch reason for caller logs.
        raise VideoPipelineError(str(exc)) from exc


def download_media_asset(url: str, destination: Path, *, headers: dict[str, str] | None = None) -> Path:
    destination.parent.mkdir(parents=True, exist_ok=True)
    header_lines = []
    for key, value in (headers or {}).items():
        header_lines.append(f"{key}: {value}")
    ffmpeg_headers = "\r\n".join(header_lines)
    command = [
        "ffmpeg",
        "-nostdin",
        "-y",
        "-loglevel",
        "error",
        "-user_agent",
        USER_AGENT,
    ]
    if headers:
        command.extend(["-headers", ffmpeg_headers])
    command.extend(["-i", url, "-c", "copy", "-movflags", "+faststart", str(destination)])
    try:
        subprocess.run(command, check=True, capture_output=True)
        return destination
    except FileNotFoundError:
        return download_binary(url, destination, headers=headers)
    except subprocess.CalledProcessError:
        return download_binary(url, destination, headers=headers)


def transcribe_with_whisper(
    media_path: Path,
    *,
    model_name: str = "small",
    language: str | None = None,
) -> dict[str, Any]:
    try:
        import whisper
    except ImportError as exc:  # pragma: no cover - environment specific.
        raise VideoPipelineError(
            "openai-whisper is not installed; install it to run local transcription"
        ) from exc

    model = whisper.load_model(model_name)
    kwargs: dict[str, Any] = {"fp16": False}
    if language:
        kwargs["language"] = language
    return model.transcribe(str(media_path), **kwargs)


def transcript_segments_from_result(result: dict[str, Any]) -> list[TranscriptSegment]:
    segments: list[TranscriptSegment] = []

    def decimal_or_zero(value: Any) -> Decimal:
        if value in (None, ""):
            return Decimal("0")
        return Decimal(str(value))

    for segment in result.get("segments", []):
        if not isinstance(segment, dict):
            continue
        text = str(segment.get("text") or "").strip()
        if not text:
            continue
        segments.append(
            TranscriptSegment(
                start=decimal_or_zero(segment.get("start")),
                end=decimal_or_zero(segment.get("end")),
                text=text,
            )
        )
    return segments


def match_name_hits(
    *,
    transcript_segments: Iterable[TranscriptSegment],
    candidates: Iterable[NameCandidate],
) -> list[dict[str, Any]]:
    normalized_candidates = []
    for candidate in candidates:
        normalized_name = normalize_match_text(candidate.normalized_name or candidate.raw_name)
        if not normalized_name:
            continue
        normalized_candidates.append((normalized_name, candidate))

    normalized_candidates.sort(key=lambda item: len(item[0]), reverse=True)

    hits: list[dict[str, Any]] = []
    for segment in transcript_segments:
        haystack = f" {normalize_match_text(segment.text)} "
        seen_for_segment: set[int | None] = set()
        for normalized_name, candidate in normalized_candidates:
            if candidate.skater_appearance_id in seen_for_segment:
                continue
            if f" {normalized_name} " not in haystack:
                continue
            seen_for_segment.add(candidate.skater_appearance_id)
            hits.append(
                {
                    "skater_appearance_id": candidate.skater_appearance_id,
                    "raw_name": candidate.raw_name,
                    "normalized_name": candidate.normalized_name,
                    "segment_id": candidate.segment_id,
                    "category_id": candidate.category_id,
                    "start_time_seconds": segment.start,
                    "end_time_seconds": segment.end,
                    "confidence": Decimal("0.9600"),
                    "evidence_kind": "transcript_match",
                    "transcript_excerpt": segment.text[:500],
                }
            )
    return hits
