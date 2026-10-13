/**
 * Wire types for the FastAPI backend at SKATING_API_URL.
 *
 * These mirror the JSON shape produced by skating_api.repository — kept
 * intentionally close to the wire so we don't add a serialization
 * adapter layer (TanStack Query handles caching).
 *
 * When the backend adds a field, just add it here as `field?: T`. Optional
 * fields keep older deploys compatible without recompiling the dashboard.
 */

export type DashboardOverview = {
  events: number;
  skaters: number;
  registered_clubs: number;
  scoring_clubs: number;
  segment_scores: number;
  element_scores: number;
  development_skaters: number;
  first_event_date: string | null;
  latest_event_date: string | null;
  international_events: number;
  federation_events: number;
  club_or_open_events: number;
};

export type CompetitionMixRow = {
  scope_hint: string | null;
  competition_stream: string | null;
  competition_level: string | null;
  event_type: string | null;
  representation_primary: string | null;
  events: number;
  skaters_with_scores: number;
  clubs_with_appearances: number;
  skaters_with_appearances: number;
  segment_scores: number;
  first_event_date: string | null;
  latest_event_date: string | null;
};

export type ClubRow = {
  club_name: string;
  country_code: string | null;
  country_display?: string | null;
  country_flag?: string | null;
  skaters: number;
  events: number;
  segment_scores: number;
  international_events: number;
  federation_events: number;
  club_or_open_events: number;
  national_competitors: number;
  recreational_only_skaters: number;
  mixed_competitors: number;
  development_skaters: number;
  development_only_skaters: number;
  converted_development_skaters: number;
  best_tss: number | null;
  best_tes: number | null;
  best_pcs: number | null;
  first_seen_date: string | null;
  latest_seen_date: string | null;
};

export type NationalityRow = {
  nation_code: string;
  nation_display?: string;
  nation_flag?: string | null;
  skaters: number;
  events: number;
  segment_scores: number;
  development_skaters: number;
  development_only_skaters: number;
  converted_development_skaters: number;
  best_tss: number | null;
  best_tes: number | null;
  best_pcs: number | null;
  first_seen_date: string | null;
  latest_seen_date: string | null;
};

export type DashboardResponse = {
  period: { from_year: number; to_year: number } | null;
  overview: DashboardOverview;
  competition_mix: CompetitionMixRow[];
  clubs: ClubRow[];
  nationalities: NationalityRow[];
};

export type SkaterSearchRow = {
  skater_key: number;
  identity_key: string;
  skater_name: string;
  nation_code: string | null;
  nation_display?: string | null;
  club_name: string | null;
  source_appearances: number;
  first_seen_date: string | null;
  latest_seen_date: string | null;
  best_tss: number | null;
  best_tes: number | null;
  best_pcs: number | null;
  competitions: number;
  scored_competitions: number;
};

export type SkaterScoreRow = {
  segment_score_key: number;
  event_key: number;
  event_name: string | null;
  event_start_date: string | null;
  category_name: string | null;
  segment_name: string | null;
  score_source: "pdf_score_summary" | "html_segment_result" | string;
  element_count: number;
  place: number | null;
  tss: number | null;
  tes: number | null;
  pcs: number | null;
  deduction: number | null;
  base_value_total: number | null;
  element_score_total: number | null;
  judge_count: number | null;
  root_url: string | null;
  // Edited 2026-10-13 by Claude (AI assistant): competition part (db/44), e.g. Non-ISU / National championship
  group_key?: number | null;
  part?: string | null;
};

export type SkaterElementRow = {
  element_score_key: number;
  segment_score_key: number;
  event_key: number;
  category_name: string | null;
  segment_name: string | null;
  date_key: string | null;
  element_no: number;
  element_code: string;
  base_element_code: string | null;
  raw_element: string | null;
  info: string | null;
  markers: string[];
  base_value: number | null;
  goe: number | null;
  bonus: number | null;
  panel_score: number | null;
};

export type SkaterComponentSummary = {
  component_name: string;
  samples: number;
  avg_score: number;
  best_score: number;
  worst_score: number;
};

export type SkaterDetail = SkaterSearchRow & {
  scores: SkaterScoreRow[];
  elements: SkaterElementRow[];
  component_summary?: SkaterComponentSummary[];
  best_score_total: number | null;
};

export type ClubDetail = ClubRow & {
  skater_rows: (SkaterSearchRow & {
    has_national_event: boolean;
    has_recreational_event: boolean;
    has_development_event: boolean;
    competitor_status: "national_competitor" | "recreational" | "mixed" | "development" | string;
  })[];
};

export type NationalityDetail = NationalityRow & {
  skaters_list: SkaterSearchRow[];
};

export type EventRow = {
  event_key: number;
  event_name: string | null;
  location: string | null;
  venue: string | null;
  date_range: string | null;
  event_start_date: string | null;
  event_type: string | null;
  competition_level: string | null;
  competition_stream: string | null;
  parser_profile: string | null;
  root_url: string | null;
  event_protocol_url: string | null;
};

export type EventResultRow = {
  competition_result_key: number;
  category_key: number;
  category_name: string | null;
  skater_key: number | null;
  skater_name: string;
  nation_code: string | null;
  nation_display?: string | null;
  club_name: string | null;
  final_place: number | null;
  points: number | null;
  segment_places: Record<string, number> | null;
  // Edited 2026-10-12 by Claude (AI assistant): this skater's score sheets in the category (name links to the first)
  segments?: SegmentLink[];
};

export type SegmentLink = {
  segment_score_key: number;
  segment_name: string | null;
  place?: number | null;
  tss?: number | null;
};

export type EventPart = { event_key: number; part: string; event_name: string | null; current: boolean };

export type EventDetail = EventRow & {
  results: EventResultRow[];
  // Edited 2026-10-13 by Claude (AI assistant): other result sets of the same competition (ISU / Non-ISU / national)
  parts?: EventPart[];
};

export type Judge = {
  judge_number: number;
  name: string | null;
  nation_code: string | null;
  nation_display?: string | null;
  function_text: string | null;
  role_group: string;
};

export type ElementJudgeMark = {
  element_score_key: number;
  judge_number: number;
  score: number;
  judge_name: string | null;
  judge_nation_code: string | null;
  judge_function: string | null;
};

export type ScoreElement = SkaterElementRow & {
  judge_marks: ElementJudgeMark[];
};

export type ComponentScore = {
  component_score_key: number;
  component_name: string;
  factor: number;
  score: number;
  judge_marks: { judge_number: number; score: number }[];
};

export type ScoreEvidence = {
  source_document_key: number;
  source_url: string;
  document_type: string | null;
  archive_path: string | null;
  archive_status: string | null;
  content_hash: string | null;
};

export type ScoreDetail = {
  segment_score_key: number;
  score_source: string;
  skater_key: number | null;
  segment_key: number;
  skater_name: string;
  nation_code: string | null;
  nation_display?: string | null;
  club_name: string | null;
  event_key: number;
  event_name: string | null;
  event_start_date: string | null;
  category_name: string | null;
  segment_name: string | null;
  place: number | null;
  tss: number | null;
  tes: number | null;
  pcs: number | null;
  deduction: number | null;
  base_value_total: number | null;
  element_score_total: number | null;
  judge_count: number | null;
  root_url: string | null;
  category_segments?: SegmentLink[];
  judges: Judge[];
  elements: ScoreElement[];
  components: ComponentScore[];
  evidence: ScoreEvidence[];
};
