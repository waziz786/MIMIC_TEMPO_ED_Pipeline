-- Event: Renal Replacement Therapy (RRT) Initiation
-- Identifies RRT from procedureevents and chartevents
-- Includes hemodialysis, CRRT, and related procedures

WITH rrt_procedure_itemids AS (
  -- Identify RRT-related procedure itemids
  SELECT DISTINCT itemid, label
  FROM {icu_schema}.d_items
  WHERE (
    LOWER(label) LIKE '%dialysis%'
    OR LOWER(label) LIKE '%crrt%'
    OR LOWER(label) LIKE '%cvvh%'
    OR LOWER(label) LIKE '%renal replacement%'
  )
  AND itemid IS NOT NULL
),
rrt_events AS (
  -- From procedureevents
  SELECT
    pe.subject_id,
    pe.hadm_id,
    pe.starttime,
    rpi.label
  FROM {icu_schema}.procedureevents pe
  INNER JOIN rrt_procedure_itemids rpi
    ON pe.itemid = rpi.itemid
  WHERE pe.starttime IS NOT NULL
  
  UNION
  
  -- From chartevents (RRT settings/documentation)
  SELECT
    ce.subject_id,
    ce.hadm_id,
    ce.charttime AS starttime,
    di.label
  FROM {icu_schema}.chartevents ce
  INNER JOIN {icu_schema}.d_items di
    ON ce.itemid = di.itemid
  WHERE ce.charttime IS NOT NULL
    AND (
      LOWER(di.label) LIKE '%dialysis%'
      OR LOWER(di.label) LIKE '%crrt%'
      OR LOWER(di.label) LIKE '%cvvh%'
    )
    AND ce.value IS NOT NULL
)
SELECT
  b.subject_id,
  b.stay_id,
  b.hadm_id,
  'RRT_START'::text AS event_type,
  MIN(re.starttime) AS event_time,
  'exact'::text AS event_time_type,
  'icu.procedureevents+chartevents'::text AS source_table,
  STRING_AGG(DISTINCT re.label, '; ') AS event_detail
FROM {base_ed_cohort} b
INNER JOIN rrt_events re
  ON b.hadm_id = re.hadm_id
WHERE re.starttime >= b.ed_intime
  AND re.starttime <= (b.ed_outtime + INTERVAL '7 days')
GROUP BY b.subject_id, b.stay_id, b.hadm_id;
