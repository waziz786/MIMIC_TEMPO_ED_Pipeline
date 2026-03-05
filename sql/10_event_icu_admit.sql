-- Event: ICU Admission after ED arrival
-- Extracts ICU admission events with accurate timestamps from icustays table

SELECT
  b.subject_id,
  b.stay_id,
  b.hadm_id,
  'ICU_ADMIT'::text AS event_type,
  i.intime AS event_time,
  'exact'::text AS event_time_type,
  'icu.icustays'::text AS source_table,
  i.first_careunit AS event_detail
FROM {base_ed_cohort} b
INNER JOIN {icu_schema}.icustays i
  ON b.hadm_id = i.hadm_id
WHERE i.intime IS NOT NULL
  AND i.intime >= b.ed_intime  -- ICU admit must be after or at ED arrival
  AND i.intime <= (b.ed_outtime + INTERVAL '7 days');  -- Within reasonable window
