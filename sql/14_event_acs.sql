-- Event: Acute Coronary Syndrome (ACS) (hospitalization-level)
-- Identifies ACS diagnoses from ICD codes
-- Includes STEMI, NSTEMI, unstable angina
-- Timing is NOT available in hosp.diagnoses_icd in MIMIC-IV.
-- Policy: hospitalization-level only => event_time = NULL.

WITH acs_icd_codes AS (
  -- ICD-10 codes for ACS
  SELECT icd_code, icd_version, long_title
  FROM {hosp_schema}.d_icd_diagnoses
  WHERE icd_version = 10
    AND (
      icd_code LIKE 'I21%'     -- ST elevation myocardial infarction
      OR icd_code LIKE 'I22%'  -- Subsequent STEMI
      OR icd_code LIKE 'I24.0' -- Acute coronary thrombosis not resulting in MI
      OR icd_code = 'I20.0'    -- Unstable angina
    )

  UNION

  -- ICD-9 codes for ACS (if applicable)
  SELECT icd_code, icd_version, long_title
  FROM {hosp_schema}.d_icd_diagnoses
  WHERE icd_version = 9
    AND (
      icd_code LIKE '410%'     -- Acute myocardial infarction
      OR icd_code = '411.1'    -- Intermediate coronary syndrome
      OR icd_code = '413.0'    -- Angina decubitus
    )
),
acs_hadm AS (
  SELECT
    d.subject_id,
    d.hadm_id,
    d.icd_code,
    d.icd_version,
    ac.long_title
  FROM {hosp_schema}.diagnoses_icd d
  INNER JOIN acs_icd_codes ac
    ON d.icd_code = ac.icd_code
   AND d.icd_version = ac.icd_version
)
SELECT
  b.subject_id,
  b.stay_id,
  b.hadm_id,
  'ACS'::text AS event_type,
  NULL::timestamp AS event_time,                 -- critical change: unknown timing
  'none'::text AS event_time_type,               -- exact|date|none
  'hosp.diagnoses_icd (hospitalization-level)'::text AS source_table,
  STRING_AGG(DISTINCT ah.long_title, '; ') AS event_detail
FROM {base_ed_cohort} b
INNER JOIN acs_hadm ah
  ON b.hadm_id = ah.hadm_id                      -- index admission only
GROUP BY b.subject_id, b.stay_id, b.hadm_id;
