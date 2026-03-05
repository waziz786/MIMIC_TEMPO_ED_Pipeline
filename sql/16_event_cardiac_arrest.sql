-- Event: Cardiac Arrest (hospitalization-level)
-- Identifies cardiac arrest from ICD diagnosis codes
-- Timing is NOT available in hosp.diagnoses_icd in MIMIC-IV.
-- Policy: hospitalization-level only => event_time = NULL.

WITH cardiac_arrest_icd_codes AS (
  -- ICD-10 codes for cardiac arrest
  SELECT icd_code, icd_version, long_title
  FROM {hosp_schema}.d_icd_diagnoses
  WHERE icd_version = 10
    AND (
      icd_code LIKE 'I46%'     -- Cardiac arrest
    )

  UNION

  -- ICD-9 codes for cardiac arrest
  SELECT icd_code, icd_version, long_title
  FROM {hosp_schema}.d_icd_diagnoses
  WHERE icd_version = 9
    AND (
      icd_code IN ('427.5', '427.41', '427.42')  -- Cardiac arrest and ventricular fibrillation
    )
),
ca_hadm AS (
  SELECT
    d.subject_id,
    d.hadm_id,
    d.icd_code,
    d.icd_version,
    cac.long_title
  FROM {hosp_schema}.diagnoses_icd d
  INNER JOIN cardiac_arrest_icd_codes cac
    ON d.icd_code = cac.icd_code
   AND d.icd_version = cac.icd_version
)
SELECT
  b.subject_id,
  b.stay_id,
  b.hadm_id,
  'CARDIAC_ARREST'::text AS event_type,
  NULL::timestamp AS event_time,                 -- critical change: unknown timing
  'none'::text AS event_time_type,               -- exact|date|none
  'hosp.diagnoses_icd (hospitalization-level)'::text AS source_table,
  STRING_AGG(DISTINCT ca.long_title, '; ') AS event_detail
FROM {base_ed_cohort} b
INNER JOIN ca_hadm ca
  ON b.hadm_id = ca.hadm_id                      -- index admission only
GROUP BY b.subject_id, b.stay_id, b.hadm_id;
