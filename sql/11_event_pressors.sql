-- Event: Vasopressor Initiation
-- Identifies first vasopressor start from ICU inputevents
-- Maps common vasopressors by itemid and label matching

WITH pressor_itemids AS (
  -- Map vasopressor medications from d_items
  -- Common pressors: norepinephrine, epinephrine, vasopressin, dopamine, phenylephrine
  SELECT DISTINCT itemid, label
  FROM {icu_schema}.d_items
  WHERE LOWER(label) SIMILAR TO '%(norepinephrine|epinephrine|vasopressin|dopamine|phenylephrine|dobutamine)%'
    AND itemid IS NOT NULL
),
pressor_events AS (
  SELECT
    ie.subject_id,
    ie.hadm_id,
    ie.starttime,
    ie.itemid,
    pi.label
  FROM {icu_schema}.inputevents ie
  INNER JOIN pressor_itemids pi
    ON ie.itemid = pi.itemid
  WHERE ie.starttime IS NOT NULL
    AND ie.amount > 0  -- Actual administration
)
SELECT
  b.subject_id,
  b.stay_id,
  b.hadm_id,
  'PRESSOR_START'::text AS event_type,
  MIN(pe.starttime) AS event_time,  -- First pressor start
  'exact'::text AS event_time_type,
  'icu.inputevents'::text AS source_table,
  STRING_AGG(DISTINCT pe.label, '; ') AS event_detail
FROM {base_ed_cohort} b
INNER JOIN pressor_events pe
  ON b.hadm_id = pe.hadm_id
WHERE pe.starttime >= b.ed_intime
  AND pe.starttime <= (b.ed_outtime + INTERVAL '7 days')
GROUP BY b.subject_id, b.stay_id, b.hadm_id;
