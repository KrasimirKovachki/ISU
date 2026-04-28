-- Manual validation legend for scoring markers, roles, and representation rules.
-- This is reference data for import QA and later UI/API explanations.

CREATE TABLE IF NOT EXISTS core.validation_legend (
  id bigserial PRIMARY KEY,
  legend_group text NOT NULL,
  code text NOT NULL,
  label text NOT NULL,
  meaning text NOT NULL,
  import_action text NOT NULL,
  validation_note text,
  source_reference text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (legend_group, code)
);

INSERT INTO core.validation_legend (
  legend_group,
  code,
  label,
  meaning,
  import_action,
  validation_note,
  source_reference
) VALUES
  (
    'element_marker',
    '<',
    'Under-rotated jump',
    'Jump rotation is short enough to receive an under-rotation call.',
    'Store in pdf_elements.markers and keep the original element_code/raw_element.',
    'Do not drop the element. It remains an executed element with called base value, GOE, judge marks, and panel score.',
    'ISU protocol legend'
  ),
  (
    'element_marker',
    '<<',
    'Downgraded jump',
    'Jump rotation is short enough to receive a downgrade call.',
    'Store in pdf_elements.markers and keep the original element_code/raw_element.',
    'Example: 4S<< is still an executed element, but it is called as downgraded. Validate that markers contains << and scores are preserved.',
    'ISU protocol legend'
  ),
  (
    'element_marker',
    'q',
    'Landed on the quarter',
    'Jump is landed on the quarter rotation boundary.',
    'Store in pdf_elements.markers.',
    'Only mark actual trailing q calls, not q letters inside step/choreo element names.',
    'ISU protocol legend'
  ),
  (
    'element_marker',
    'e',
    'Wrong edge',
    'Jump edge call, commonly on flip/lutz jumps.',
    'Store in pdf_elements.markers.',
    'Keep base_element_code without the marker for grouping, but preserve element_code and raw_element.',
    'ISU protocol legend'
  ),
  (
    'element_marker',
    '!',
    'Not clear edge',
    'Unclear edge call, commonly on flip/lutz jumps.',
    'Store in pdf_elements.markers.',
    'Can appear in element_code or Info column.',
    'ISU protocol legend'
  ),
  (
    'element_marker',
    'F',
    'Fall',
    'Fall associated with the element in the PDF info/marker data.',
    'Store in pdf_elements.markers.',
    'This is element-level info. Segment-level deductions are stored separately in pdf_score_summaries.total_deductions and deductions_detail.',
    'ISU protocol legend'
  ),
  (
    'element_marker',
    'x',
    'Highlight distribution bonus',
    'Base value multiplier/credit shown by the protocol for applicable later-program elements.',
    'Store in pdf_elements.markers.',
    'Validate that x is preserved when it appears in the Info column or compact element detail.',
    'ISU protocol legend'
  ),
  (
    'element_marker',
    '*',
    'Invalid element',
    'Element is invalidated by the panel/rules.',
    'Store in pdf_elements.markers.',
    'Do not remove the row; keep original score values for audit.',
    'ISU protocol legend'
  ),
  (
    'element_marker',
    'b',
    'Bonus marker',
    'Source-specific bonus marker found in some lower-level/recreational protocols.',
    'Store in pdf_elements.markers and pdf_elements.bonus when a numeric bonus column is parsed.',
    'Keep as source data even where ISU international protocols do not use it.',
    'Source protocol data'
  ),
  (
    'element_marker',
    'REP',
    'Repetition',
    'Jump repetition marker.',
    'Store in pdf_elements.markers and preserve element_code/raw_element.',
    'Can combine with rotation and fall markers, for example 4T<+REP with <,F,x.',
    'ISU protocol legend'
  ),
  (
    'representation',
    'club',
    'Club representation',
    'Skater row comes from a club-primary source. Club may be present, empty, individual, or unknown depending on the source row.',
    'Store club_name as-is, store nation_code separately, use representation_type=club, and set representation_value only when club_name is present.',
    'For Ice Peak Trophy, empty Club must be reported for manual review rather than auto-converted to nation.',
    'Project source profile rule'
  ),
  (
    'representation',
    'nation',
    'Country representation',
    'Skater is represented by country/nation in international-style sources.',
    'Store nation_code, representation_type=nation, representation_value=nation_code.',
    'ISU and NonISU sources usually have no Club column and should remain nation-primary.',
    'Project source profile rule'
  ),
  (
    'official_role',
    'judge',
    'Judge',
    'Segment judge with judge number J1/J2/etc.',
    'Store in official_assignments with role_group=judge and judge_number.',
    'PDF judge marks J1..J9 can later map to these segment officials.',
    'Project import rule'
  ),
  (
    'official_role',
    'technical_panel',
    'Technical panel',
    'Technical controller/specialist roles that call elements and levels.',
    'Store in official_assignments with role_group=technical_panel.',
    'Needed to audit scope of skater score calls and technical decisions.',
    'Project import rule'
  ),
  (
    'official_role',
    'event_operations',
    'Event operations',
    'Data operator/replay operator roles.',
    'Store in official_assignments with role_group=event_operations.',
    'Not judge marks, but important for complete source preservation.',
    'Project import rule'
  )
ON CONFLICT (legend_group, code) DO UPDATE SET
  label = EXCLUDED.label,
  meaning = EXCLUDED.meaning,
  import_action = EXCLUDED.import_action,
  validation_note = EXCLUDED.validation_note,
  source_reference = EXCLUDED.source_reference,
  updated_at = now();

CREATE OR REPLACE VIEW core.v_scoring_marker_legend AS
SELECT code, label, meaning, import_action, validation_note, source_reference
FROM core.validation_legend
WHERE legend_group = 'element_marker'
ORDER BY
  CASE code
    WHEN '<' THEN 1
    WHEN '<<' THEN 2
    WHEN 'q' THEN 3
    WHEN 'e' THEN 4
    WHEN '!' THEN 5
    WHEN 'F' THEN 6
    WHEN 'x' THEN 7
    WHEN '*' THEN 8
    WHEN 'b' THEN 9
    WHEN 'REP' THEN 10
    ELSE 99
  END;

CREATE OR REPLACE VIEW core.v_representation_legend AS
SELECT code, label, meaning, import_action, validation_note, source_reference
FROM core.validation_legend
WHERE legend_group = 'representation'
ORDER BY code;
