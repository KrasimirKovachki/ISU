from decimal import Decimal

from isu_parser.video_pipeline import (
    NameCandidate,
    TranscriptSegment,
    match_name_hits,
    normalize_match_text,
    transcript_text_from_segments,
)


def test_normalize_match_text_strips_punctuation_and_accents() -> None:
    assert normalize_match_text("  Ánna-María  Popova / Petrov  ") == "ANNA MARIA POPOVA PETROV"


def test_transcript_text_from_segments_joins_non_empty_text() -> None:
    segments = [
        TranscriptSegment(start=Decimal("0"), end=Decimal("1"), text="First"),
        TranscriptSegment(start=Decimal("1"), end=Decimal("2"), text=""),
        TranscriptSegment(start=Decimal("2"), end=Decimal("3"), text="Second"),
    ]

    assert transcript_text_from_segments(segments) == "First Second"


def test_match_name_hits_uses_segment_timecodes() -> None:
    segments = [
        TranscriptSegment(start=Decimal("12.5"), end=Decimal("18.0"), text="Now on the ice, Eva Example skates cleanly."),
        TranscriptSegment(start=Decimal("20.0"), end=Decimal("25.0"), text="Another skater enters."),
    ]
    candidates = [
        NameCandidate(skater_appearance_id=101, raw_name="Eva Example", normalized_name="EVA EXAMPLE", segment_id=55),
        NameCandidate(skater_appearance_id=202, raw_name="Other Skater", normalized_name="OTHER SKATER", segment_id=66),
    ]

    hits = match_name_hits(transcript_segments=segments, candidates=candidates)

    assert hits == [
        {
            "skater_appearance_id": 101,
            "raw_name": "Eva Example",
            "normalized_name": "EVA EXAMPLE",
            "segment_id": 55,
            "category_id": None,
            "start_time_seconds": Decimal("12.5"),
            "end_time_seconds": Decimal("18.0"),
            "confidence": Decimal("0.9600"),
            "evidence_kind": "transcript_match",
            "transcript_excerpt": "Now on the ice, Eva Example skates cleanly.",
        }
    ]
