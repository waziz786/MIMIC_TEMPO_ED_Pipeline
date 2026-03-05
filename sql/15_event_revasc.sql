-- Event: Coronary Revascularization (PCI/CABG) (hospitalization-level)
-- Identifies revascularization procedures from ICD procedure codes
-- Includes PCI (percutaneous coronary intervention) and CABG (coronary artery bypass)
-- Timing is NOT available in hosp.procedures_icd in MIMIC-IV.
-- Policy: hospitalization-level only => event_time = NULL.

WITH pci_icd_codes AS (
  -- ICD-10-PCS codes for PCI
  SELECT icd_code, icd_version, long_title
  FROM {hosp_schema}.d_icd_procedures
  WHERE icd_version = 10
    AND (
      icd_code LIKE '02703%'   -- Dilation of coronary artery, one artery
      OR icd_code LIKE '02704%'   -- Dilation of coronary artery, two arteries
      OR icd_code LIKE '02705%'   -- Dilation of coronary artery, three arteries
      OR icd_code LIKE '02706%'   -- Dilation of coronary artery, four or more arteries
      OR icd_code LIKE '0270%'    -- Dilation procedures on heart and great vessels
    )
  
  UNION
  
  -- ICD-9 codes for PCI
  SELECT icd_code, icd_version, long_title
  FROM {hosp_schema}.d_icd_procedures
  WHERE icd_version = 9
    AND (
      icd_code IN ('00.66', '36.06', '36.07', '36.09')  -- PTCA and related
    )
),
cabg_icd_codes AS (
  -- ICD-10-PCS codes for CABG
  SELECT icd_code, icd_version, long_title
  FROM {hosp_schema}.d_icd_procedures
  WHERE icd_version = 10
    AND (
      icd_code LIKE '021%'   -- Bypass of coronary artery
    )
  
  UNION
  
  -- ICD-9 codes for CABG
  SELECT icd_code, icd_version, long_title
  FROM {hosp_schema}.d_icd_procedures
  WHERE icd_version = 9
    AND (
      icd_code LIKE '36.1%'   -- CABG procedures
    )
),
pci_procedures AS (
  SELECT
    p.subject_id,
    p.hadm_id,
    p.icd_code,
    p.icd_version,
    pc.long_title,
    'PCI'::text AS procedure_type
  FROM {hosp_schema}.procedures_icd p
  INNER JOIN pci_icd_codes pc
    ON p.icd_code = pc.icd_code
    AND p.icd_version = pc.icd_version
),
cabg_procedures AS (
  SELECT
    p.subject_id,
    p.hadm_id,
    p.icd_code,
    p.icd_version,
    cc.long_title,
    'CABG'::text AS procedure_type
  FROM {hosp_schema}.procedures_icd p
  INNER JOIN cabg_icd_codes cc
    ON p.icd_code = cc.icd_code
    AND p.icd_version = cc.icd_version
),
all_revasc AS (
  SELECT * FROM pci_procedures
  UNION ALL
  SELECT * FROM cabg_procedures
)
SELECT
  b.subject_id,
  b.stay_id,
  b.hadm_id,
  ar.procedure_type AS event_type,
  NULL::timestamp AS event_time,                 -- critical change: unknown timing
  'none'::text AS event_time_type,               -- exact|date|none
  'hosp.procedures_icd (hospitalization-level)'::text AS source_table,
  STRING_AGG(DISTINCT ar.long_title, '; ') AS event_detail
FROM {base_ed_cohort} b
INNER JOIN all_revasc ar
  ON b.hadm_id = ar.hadm_id                      -- index admission only
GROUP BY b.subject_id, b.stay_id, b.hadm_id, ar.procedure_type;
