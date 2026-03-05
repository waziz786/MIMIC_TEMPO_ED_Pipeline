-- Event: Death
-- Identifies death from admissions.deathtime
-- Provides accurate timestamp for in-hospital mortality

SELECT
  b.subject_id,
  b.stay_id,
  b.hadm_id,
  'DEATH'::text AS event_type,
  a.deathtime AS event_time,
  'exact'::text AS event_time_type,
  'hosp.admissions.deathtime'::text AS source_table,
  'In-hospital death'::text AS event_detail
FROM {base_ed_cohort} b
INNER JOIN {hosp_schema}.admissions a
  ON b.hadm_id = a.hadm_id
WHERE a.deathtime IS NOT NULL
  AND a.deathtime >= b.ed_intime;  -- Death after ED arrival
