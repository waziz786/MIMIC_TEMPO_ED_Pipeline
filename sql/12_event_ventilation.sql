-- Event: Mechanical Ventilation Started
-- Identifies ventilation start from procedureevents (invasive ventilation)
-- Falls back to chartevents if procedureevents not available

WITH vent_procedure_itemids AS (
  -- Identify ventilation-related procedure itemids
  SELECT DISTINCT itemid, label
  FROM {icu_schema}.d_items
  WHERE (
    LOWER(label) LIKE '%intubation%'
    OR LOWER(label) LIKE '%ventilation%'
    OR LOWER(label) LIKE '%mechanical vent%'
  )
  AND itemid IS NOT NULL
),
vent_events AS (
  -- From procedureevents
  SELECT
    pe.subject_id,
    pe.hadm_id,
    pe.starttime,
    vpi.label
  FROM {icu_schema}.procedureevents pe
  INNER JOIN vent_procedure_itemids vpi
    ON pe.itemid = vpi.itemid
  WHERE pe.starttime IS NOT NULL
  
  UNION
  
  -- From chartevents (ventilator settings as proxy)
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
      LOWER(di.label) LIKE '%vent mode%'
      OR LOWER(di.label) LIKE '%peep%'
      OR LOWER(di.label) LIKE '%tidal volume%'
    )
    AND ce.value IS NOT NULL
)
SELECT
  b.subject_id,
  b.stay_id,
  b.hadm_id,
  'VENT_START'::text AS event_type,
  MIN(ve.starttime) AS event_time,
  'exact'::text AS event_time_type,
  'icu.procedureevents+chartevents'::text AS source_table,
  STRING_AGG(DISTINCT ve.label, '; ') AS event_detail
FROM {base_ed_cohort} b
INNER JOIN vent_events ve
  ON b.hadm_id = ve.hadm_id
WHERE ve.starttime >= b.ed_intime
  AND ve.starttime <= (b.ed_outtime + INTERVAL '7 days')
GROUP BY b.subject_id, b.stay_id, b.hadm_id;
