-- Some source events publish multiple segment rows with the same display name
-- inside one category, especially ice dance pattern dances. Preserve both rows
-- by using source_order as the stable segment identity within a category.

ALTER TABLE ingest.segments
  DROP CONSTRAINT IF EXISTS segments_category_id_name_key;

ALTER TABLE ingest.segments
  ADD CONSTRAINT segments_category_id_source_order_key
  UNIQUE (category_id, source_order);

CREATE INDEX IF NOT EXISTS idx_segments_category_name
  ON ingest.segments(category_id, name);
