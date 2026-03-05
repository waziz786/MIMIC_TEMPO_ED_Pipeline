-- Base ED Cohort
-- Generates one row per ED stay with core identifiers and timestamps
-- INCLUDES ALL ED VISITS (with optional hospital admission linkage)
-- Filters: Adult patients (age >= 18, <= 110), valid ED stay times, alive at ED arrival

DROP TABLE IF EXISTS {base_ed_cohort};

CREATE TABLE {base_ed_cohort} AS
WITH ed_stays_raw AS (
  SELECT
    e.subject_id,
    e.stay_id,
    e.hadm_id,  -- May be NULL for non-admitted patients
    e.intime AS ed_intime,
    e.outtime AS ed_outtime,
    e.arrival_transport,  -- Arrival mode (ambulance, walk-in, etc.)
    e.disposition,        -- Discharge disposition
    e.race,               -- Race/ethnicity
    p.anchor_age,
    p.anchor_year,
    p.gender,
    p.dod,
    -- Calculate age at ED visit
    (p.anchor_age + EXTRACT(YEAR FROM e.intime) - p.anchor_year) AS age_at_ed
  FROM {ed_schema}.edstays e
  INNER JOIN {hosp_schema}.patients p
    ON e.subject_id = p.subject_id
  WHERE e.intime IS NOT NULL
    AND e.outtime IS NOT NULL
)
SELECT
  subject_id,
  stay_id,
  hadm_id,
  ed_intime,
  ed_outtime,
  anchor_age,
  anchor_year,
  gender,
  dod,
  age_at_ed,
  arrival_transport,
  disposition,
  race,
  EXTRACT(EPOCH FROM (ed_outtime - ed_intime)) / 3600.0 AS ed_los_hours,
  CASE WHEN hadm_id IS NOT NULL THEN 1 ELSE 0 END AS was_admitted
FROM ed_stays_raw
WHERE age_at_ed >= 18  -- Adult (+18 years) patients only
  AND age_at_ed <= 110 -- Cap implausible ages
  AND ed_outtime > ed_intime
  AND (dod IS NULL OR ed_intime <= dod);  -- Patient must be alive at ED arrival

-- Create indexes for joins
CREATE INDEX IF NOT EXISTS idx_{base_ed_cohort}_stay_id ON {base_ed_cohort}(stay_id);
CREATE INDEX IF NOT EXISTS idx_{base_ed_cohort}_hadm_id ON {base_ed_cohort}(hadm_id);
CREATE INDEX IF NOT EXISTS idx_{base_ed_cohort}_subject_id ON {base_ed_cohort}(subject_id);
