-- Feature Basket W6 (First 6 Hours)
-- Vital sign summaries + early laboratory values

DROP TABLE IF EXISTS {features_w6};

CREATE TABLE {features_w6} AS
WITH vitals_summary AS (
  SELECT
    v.stay_id,
    -- Blood pressure
    MIN(v.sbp) AS sbp_min_6h,
    MAX(v.sbp) AS sbp_max_6h,
    AVG(v.sbp) AS sbp_mean_6h,
    STDDEV(v.sbp) AS sbp_std_6h,
    MIN(v.dbp) AS dbp_min_6h,
    
    -- Heart rate
    MIN(v.heartrate) AS hr_min_6h,
    MAX(v.heartrate) AS hr_max_6h,
    AVG(v.heartrate) AS hr_mean_6h,
    STDDEV(v.heartrate) AS hr_std_6h,
    
    -- Respiratory rate
    MAX(v.resprate) AS rr_max_6h,
    AVG(v.resprate) AS rr_mean_6h,
    
    -- Oxygen saturation
    MIN(v.o2sat) AS spo2_min_6h,
    AVG(v.o2sat) AS spo2_mean_6h,
    
    -- Temperature
    MAX(v.temperature) AS temp_max_6h,
    MIN(v.temperature) AS temp_min_6h,
    
    -- Measurement count (from vitalsign table, excludes triage)
    COUNT(*) AS n_vitalsign_measurements_6h
  FROM {ed_schema}.vitalsign v
  INNER JOIN {base_ed_cohort} b ON b.stay_id = v.stay_id
  WHERE v.charttime BETWEEN b.ed_intime AND LEAST(b.ed_outtime, b.ed_intime + INTERVAL '6 hours')
  GROUP BY v.stay_id
),
triage_check AS (
  -- Get chief complaint from triage records
  SELECT 
    t.stay_id,
    t.chiefcomplaint
  FROM {ed_schema}.triage t
),
lab_itemids AS (
  -- Map lab itemids (common labs needed for early risk assessment)
  SELECT itemid, label,
    CASE 
      WHEN LOWER(label) LIKE '%lactate%' THEN 'lactate'
      WHEN LOWER(label) LIKE '%troponin%' THEN 'troponin'
      WHEN LOWER(label) LIKE '%creatinine%' THEN 'creatinine'
      WHEN LOWER(label) LIKE '%potassium%' THEN 'potassium'
      WHEN LOWER(label) LIKE '%sodium%' THEN 'sodium'
      WHEN LOWER(label) LIKE '%bicarbonate%' OR LOWER(label) LIKE '%hco3%' THEN 'bicarbonate'
      WHEN LOWER(label) LIKE '%white blood%' OR LOWER(label) LIKE '%wbc%' THEN 'wbc'
      WHEN LOWER(label) LIKE '%hemoglobin%' OR LOWER(label) LIKE '%hgb%' THEN 'hemoglobin'
      WHEN LOWER(label) LIKE '%platelet%' THEN 'platelet'
    END AS lab_type,
    -- Flag high-sensitivity troponin (itemid 51003 = Troponin T high-sensitivity)
    CASE WHEN itemid = 51003 THEN 1 ELSE 0 END AS is_hs_troponin
  FROM {hosp_schema}.d_labitems
  WHERE LOWER(label) SIMILAR TO '%(lactate|troponin|creatinine|potassium|sodium|bicarbonate|hco3|white blood|wbc|hemoglobin|hgb|platelet)%'
),
-- BUG FIX: ROW_NUMBER now computed WITHIN the 6h window only.
-- Previously, rn was computed over the entire admission, so rn=1
-- could point to a lab drawn before ED arrival.  By joining
-- base_ed_cohort here and filtering to [ed_intime, ed_intime+6h],
-- rn=1 is guaranteed to be the first lab WITHIN the feature window.
labs_raw AS (
  SELECT
    b.stay_id,
    b.ed_intime,
    le.charttime,
    li.lab_type,
    le.valuenum,
    li.is_hs_troponin,
    ROW_NUMBER() OVER (PARTITION BY b.stay_id, li.lab_type ORDER BY le.charttime) AS rn
  FROM {hosp_schema}.labevents le
  INNER JOIN lab_itemids li ON le.itemid = li.itemid
  INNER JOIN {base_ed_cohort} b ON le.hadm_id = b.hadm_id
  WHERE le.valuenum IS NOT NULL
    AND le.valuenum > 0
    AND le.charttime BETWEEN b.ed_intime AND LEAST(b.ed_outtime, b.ed_intime + INTERVAL '6 hours')
),
labs_first AS (
  SELECT
    lr.stay_id,
    MAX(CASE WHEN lr.lab_type = 'lactate' AND lr.rn = 1 THEN lr.valuenum END) AS lactate_first_6h,
    MAX(CASE WHEN lr.lab_type = 'troponin' AND lr.rn = 1 THEN lr.valuenum END) AS troponin_first_6h,
    MAX(CASE WHEN lr.lab_type = 'troponin' AND lr.rn = 1 THEN lr.is_hs_troponin END) AS is_hs_troponin_6h,
    MAX(CASE WHEN lr.lab_type = 'creatinine' AND lr.rn = 1 THEN lr.valuenum END) AS creatinine_first_6h,
    MAX(CASE WHEN lr.lab_type = 'potassium' AND lr.rn = 1 THEN lr.valuenum END) AS potassium_first_6h,
    MAX(CASE WHEN lr.lab_type = 'sodium' AND lr.rn = 1 THEN lr.valuenum END) AS sodium_first_6h,
    MAX(CASE WHEN lr.lab_type = 'bicarbonate' AND lr.rn = 1 THEN lr.valuenum END) AS bicarbonate_first_6h,
    MAX(CASE WHEN lr.lab_type = 'wbc' AND lr.rn = 1 THEN lr.valuenum END) AS wbc_first_6h,
    MAX(CASE WHEN lr.lab_type = 'hemoglobin' AND lr.rn = 1 THEN lr.valuenum END) AS hemoglobin_first_6h,
    MAX(CASE WHEN lr.lab_type = 'platelet' AND lr.rn = 1 THEN lr.valuenum END) AS platelet_first_6h,
    -- Time to first lab result (within window)
    EXTRACT(EPOCH FROM (MIN(lr.charttime) - MIN(lr.ed_intime))) / 3600 AS time_to_first_lab_hours
  FROM labs_raw lr
  GROUP BY lr.stay_id
),
ed_process_metrics AS (
  -- Calculate time to first medication (ED LOS already in base cohort)
  SELECT
    b.stay_id,
    b.ed_intime,
    -- Time to first medication (from pyxis records)
    EXTRACT(EPOCH FROM (MIN(CASE WHEN pr.charttime IS NOT NULL THEN pr.charttime END) - b.ed_intime)) / 3600 AS time_to_first_med_hours
  FROM {base_ed_cohort} b
  LEFT JOIN {ed_schema}.pyxis pr ON b.stay_id = pr.stay_id
    AND pr.charttime BETWEEN b.ed_intime AND LEAST(b.ed_outtime, b.ed_intime + INTERVAL '6 hours')
  GROUP BY b.stay_id, b.ed_intime
),
prior_admissions AS (
  -- Count prior hospital admissions in 1 year
  SELECT
    b.subject_id,
    b.stay_id,
    COUNT(DISTINCT adm.hadm_id) AS prev_admits_1yr
  FROM {base_ed_cohort} b
  LEFT JOIN {hosp_schema}.admissions adm 
    ON b.subject_id = adm.subject_id
    AND adm.admittime >= (b.ed_intime - INTERVAL '1 year')
    AND adm.admittime < b.ed_intime
    AND adm.hadm_id != b.hadm_id
  GROUP BY b.subject_id, b.stay_id
),
prior_ed_visits AS (
  -- Count prior ED visits in 1 year
  SELECT
    b.subject_id,
    b.stay_id,
    COUNT(DISTINCT ed_vis.stay_id) AS prev_ed_visits_1yr
  FROM {base_ed_cohort} b
  LEFT JOIN {ed_schema}.edstays ed_vis
    ON b.subject_id = ed_vis.subject_id
    AND ed_vis.stay_id != b.stay_id
    AND ed_vis.intime >= (b.ed_intime - INTERVAL '1 year')
    AND ed_vis.intime < b.ed_intime
  GROUP BY b.subject_id, b.stay_id
)
SELECT
  b.stay_id,
  
  -- Demographics
  b.age_at_ed,
  b.gender,
  b.arrival_transport,
  b.race,
  
  -- Vital signs summary
  vs.sbp_min_6h,
  vs.sbp_max_6h,
  vs.sbp_mean_6h,
  vs.sbp_std_6h,
  vs.dbp_min_6h,
  vs.hr_min_6h,
  vs.hr_max_6h,
  vs.hr_mean_6h,
  vs.hr_std_6h,
  vs.rr_max_6h,
  vs.rr_mean_6h,
  vs.spo2_min_6h,
  vs.spo2_mean_6h,
  vs.temp_max_6h,
  vs.temp_min_6h,
  COALESCE(vs.n_vitalsign_measurements_6h, 0) AS n_vitalsign_measurements_6h,
  tc.chiefcomplaint,
  
  -- Laboratory values
  lf.lactate_first_6h,
  lf.troponin_first_6h,
  lf.is_hs_troponin_6h,
  lf.creatinine_first_6h,
  lf.potassium_first_6h,
  lf.sodium_first_6h,
  lf.bicarbonate_first_6h,
  lf.wbc_first_6h,
  lf.hemoglobin_first_6h,
  lf.platelet_first_6h,
  
  -- ED Process Metrics
  b.ed_los_hours,
  lf.time_to_first_lab_hours,
  epm.time_to_first_med_hours,
  
  -- Prior History
  COALESCE(pa.prev_admits_1yr, 0) AS prev_admits_1yr,
  COALESCE(pev.prev_ed_visits_1yr, 0) AS prev_ed_visits_1yr,
  
  -- Derived features (vital trends)
  CASE WHEN vs.sbp_max_6h > 0 THEN vs.sbp_std_6h / vs.sbp_mean_6h ELSE NULL END AS sbp_cv_6h,
  vs.hr_max_6h - vs.hr_min_6h AS hr_range_6h,
  
  -- Missing indicators
  CASE WHEN lf.lactate_first_6h IS NULL THEN 1 ELSE 0 END AS missing_lactate_6h,
  CASE WHEN lf.troponin_first_6h IS NULL THEN 1 ELSE 0 END AS missing_troponin_6h
  
FROM {base_ed_cohort} b
LEFT JOIN vitals_summary vs ON vs.stay_id = b.stay_id
LEFT JOIN triage_check tc ON tc.stay_id = b.stay_id
LEFT JOIN labs_first lf ON lf.stay_id = b.stay_id
LEFT JOIN ed_process_metrics epm ON epm.stay_id = b.stay_id
LEFT JOIN prior_admissions pa ON pa.stay_id = b.stay_id
LEFT JOIN prior_ed_visits pev ON pev.stay_id = b.stay_id;

-- Create index
CREATE INDEX idx_{features_w6}_stay_id ON {features_w6}(stay_id);
