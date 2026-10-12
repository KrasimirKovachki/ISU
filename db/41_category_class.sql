-- 2026-10-12 Claude (AI assistant): PROTOTYPE. Every category gets a level (isu / national / recreational),
-- a discipline and an age group, from editable rules (first match by priority) + per-category overrides.
CREATE TABLE IF NOT EXISTS api.category_rule (
  id serial PRIMARY KEY, priority int NOT NULL, pattern text NOT NULL,   -- case-insensitive regex on the category name
  level text, discipline text, age_group text, note text);
CREATE TABLE IF NOT EXISTS api.category_override (
  category_key bigint PRIMARY KEY, level text, discipline text, age_group text, note text, set_by text, set_at timestamptz DEFAULT now());
TRUNCATE api.category_rule;
INSERT INTO api.category_rule(priority, pattern, level, discipline, age_group) VALUES
 -- discipline (level/age from later rules)
 (10, 'synchro',                         NULL, 'synchronized', NULL),
 (11, 'ice dance|dance|plesni|tanc',     NULL, 'ice_dance', NULL),
 (12, 'solo dance',                      NULL, 'solo_dance', NULL),
 (13, 'pairs|pair skating|parovi',       NULL, 'pairs', NULL),
 (14, 'artistic|showcase|interpret',     NULL, 'artistic', NULL),
 -- recreational / amateur
 (20, 'recreat|rekreat|hobby|amateur|interclub|mlad figurist|spring|mini|debs|\mbronze\M|\msilver\M|\mgold\M|platinum|diamond|elementary|beginner|basic\s*\d|level\s*\d|\mstar\s*\d', 'recreational', NULL, NULL),
 (21, 'adult|masters|\mmaster\M',        'recreational', NULL, 'adult'),
 -- ISU age groups
 (30, '\msenior|\mmen\M|\mwomen\M|ladies', 'isu', NULL, 'senior'),
 (31, 'junior',                          'isu', NULL, 'junior'),
 (32, 'advanced novice',                 'isu', NULL, 'advanced_novice'),
 -- national (federation) age groups
 (40, 'intermediate novice',             'national', NULL, 'intermediate_novice'),
 (41, 'basic novice',                    'national', NULL, 'basic_novice'),
 (42, 'pre-?chicks',                     'national', NULL, 'pre_chicks'),
 (43, 'chicks',                          'national', NULL, 'chicks'),
 (44, 'cubs',                            'national', NULL, 'cubs');

CREATE OR REPLACE VIEW api.v_category_class AS
SELECT c.category_key, c.event_key, c.category_name,
  coalesce(o.level,      (SELECT r.level      FROM api.category_rule r WHERE r.level      IS NOT NULL AND c.category_name ~* r.pattern ORDER BY r.priority LIMIT 1), 'unclassified') AS level,
  coalesce(o.discipline, (SELECT r.discipline FROM api.category_rule r WHERE r.discipline IS NOT NULL AND c.category_name ~* r.pattern ORDER BY r.priority LIMIT 1), 'singles') AS discipline,
  coalesce(o.age_group,  (SELECT r.age_group  FROM api.category_rule r WHERE r.age_group  IS NOT NULL AND c.category_name ~* r.pattern ORDER BY r.priority LIMIT 1)) AS age_group,
  CASE WHEN c.category_name ~* '\m(girls|ladies|women|female)\M' THEN 'female'
       WHEN c.category_name ~* '\m(boys|men|male)\M' THEN 'male' END AS gender
FROM api.mv_dim_category c LEFT JOIN api.category_override o USING (category_key);

-- Bulgarian Skating Federation classes (bsf.bg National Championships, Priz Victoria, Spring Cup). Mapped from data:
-- the same skaters in the same season skate these ISU-style categories elsewhere (2026-10-12 cross-check):
-- I=Senior, II=Junior, III=Advanced Novice, IV=Intermediate Novice, V=Basic Novice, VI=Cubs, VII=Chicks,
-- VII+ / VII Plus = beginners (Recreational, Mlad Figurist).
INSERT INTO api.category_rule(priority, pattern, level, discipline, age_group, note) VALUES
 (1, 'klas\s*-?\s*vii\s*(\+|plus)',       'recreational', NULL, 'beginner',            'BSF VII+'),
 (2, 'klas\s*-?\s*vii\s*[ab]?\M',          'national', NULL, 'chicks',              'BSF VII'),
 (3, 'klas\s*-?\s*vi\s*[ab]?\M',           'national', NULL, 'cubs',                'BSF VI'),
 (4, 'klas\s*-?\s*v\s*[ab]?\M',            'national', NULL, 'basic_novice',        'BSF V'),
 (5, 'klas\s*-?\s*iv\M',                   'national', NULL, 'intermediate_novice', 'BSF IV'),
 (6, 'klas\s*-?\s*iii\M',                  'national', NULL, 'advanced_novice',     'BSF III'),
 (7, 'klas\s*-?\s*ii\M',                   'national', NULL, 'junior',              'BSF II'),
 (8, 'klas\s*-?\s*i\M',                    'national', NULL, 'senior',              'BSF I');
