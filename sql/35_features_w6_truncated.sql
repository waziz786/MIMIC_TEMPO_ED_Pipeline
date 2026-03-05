-- Feature Basket W6 TRUNCATED (Leakage-Proof)
-- ============================================
-- Identical features to 31_features_w6.sql, but the observation window is
-- capped at  min(6 h, time_to_first_deterioration)  for every patient.
--
-- For negatives (no event): window = full 6 hours  (identical to naive W6)
-- For positives with event AFTER 6h: window = full 6 hours  (no change)
-- For positives with event BEFORE 6h: window = [ed_intime, event_time]
--
-- This removes post-event data contamination from aggregate vital-sign
-- and laboratory features while keeping column names identical to naive W6
-- so the table is a drop-in replacement.

DROP TABLE IF EXISTS {features_w6_truncated};

CREATE TABLE {features_w6_truncated} AS

-- ── Per-patient truncation cutoff ──────────────────────────────────────
WITH truncation_cutoff AS (
  SELECT
    b.stay_id,
    b.subject_id,
    b.hadm_id,
    b.ed_intime,
    b.ed_outtime,
    b.age_at_ed,
    b.gender,
    b.arrival_transport,
    b.race,
    b.ed_los_hours,
    LEAST(
      b.ed_intime + INTERVAL '6 hours',
      CASE
        WHEN o.time_to_deterioration IS NOT NULL
        THEN b.ed_intime + (o.time_to_deterioration * INTERVAL '1 hour')
        ELSE b.ed_intime + INTERVAL '6 hours'
      END
    ) AS t_cut
  FROM {base_ed_cohort} b
  JOIN {outcomes} o ON b.stay_id = o.stay_id
),

-- ── Vitals aggregated within truncated window ──────────────────────────
vitals_summary AS (
  SELECT
    c.stay_id,
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

    -- Measurement count
    COUNT(*) AS n_vitalsign_measurements_6h
  FROM truncation_cutoff c
  INNER JOIN {ed_schema}.vitalsign v ON c.stay_id = v.stay_id
  WHERE v.charttime BETWEEN c.ed_intime AND c.t_cut
  GROUP BY c.stay_id
),

-- ── Triage (at arrival — unaffected by truncation) ─────────────────────
triage_check AS (
  SELECT
    t.stay_id,
    t.chiefcomplaint
  FROM {ed_schema}.triage t
),

-- ── Lab reference mapping ──────────────────────────────────────────────
lab_itemids AS (
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
    CASE WHEN itemid = 51003 THEN 1 ELSE 0 END AS is_hs_troponin
  FROM {hosp_schema}.d_labitems
  WHERE LOWER(label) SIMILAR TO '%(lactate|troponin|creatinine|potassium|sodium|bicarbonate|hco3|white blood|wbc|hemoglobin|hgb|platelet)%'
),

-- ── Raw lab events (global row numbering unchanged) ────────────────────
labs_raw AS (
  SELECT
    le.subject_id,
    le.hadm_id,
    le.charttime,
    li.lab_type,
    le.valuenum,
    li.is_hs_troponin,
    ROW_NUMBER() OVER (PARTITION BY le.hadm_id, li.lab_type ORDER BY le.charttime) AS rn
  FROM {hosp_schema}.labevents le
  INNER JOIN lab_itemids li ON le.itemid = li.itemid
  WHERE le.valuenum IS NOT NULL
    AND le.valuenum > 0
),

-- ── First lab values within TRUNCATED window ───────────────────────────
labs_first AS (
  SELECT
    c.stay_id,
    c.ed_intime,
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
    -- Time to first lab result
    EXTRACT(EPOCH FROM (MIN(CASE WHEN lr.charttime IS NOT NULL THEN lr.charttime END) - c.ed_intime)) / 3600 AS time_to_first_lab_hours
  FROM truncation_cutoff c
  INNER JOIN labs_raw lr
    ON c.hadm_id = lr.hadm_id
  WHERE lr.charttime BETWEEN c.ed_intime AND c.t_cut
  GROUP BY c.stay_id, c.ed_intime
),

-- ── ED process metrics within TRUNCATED window ─────────────────────────
ed_process_metrics AS (
  SELECT
    c.stay_id,
    c.ed_intime,
    EXTRACT(EPOCH FROM (MIN(CASE WHEN pr.charttime IS NOT NULL THEN pr.charttime END) - c.ed_intime)) / 3600 AS time_to_first_med_hours
  FROM truncation_cutoff c
  LEFT JOIN {ed_schema}.pyxis pr ON c.stay_id = pr.stay_id
    AND pr.charttime BETWEEN c.ed_intime AND c.t_cut
  GROUP BY c.stay_id, c.ed_intime
),

-- ── Prior admissions (history — unaffected by truncation) ──────────────
prior_admissions AS (
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

-- ── Prior ED visits (history — unaffected by truncation) ───────────────
prior_ed_visits AS (
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

-- ── Final SELECT (identical column set to naive W6) ────────────────────
SELECT
  c.stay_id,

  -- Demographics (unchanged)
  c.age_at_ed,
  c.gender,
  c.arrival_transport,
  c.race,

  -- Vital signs summary (aggregated within truncated window)
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

  -- Laboratory values (first values within truncated window)
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

  -- ED Process Metrics (within truncated window)
  c.ed_los_hours,
  lf.time_to_first_lab_hours,
  epm.time_to_first_med_hours,

  -- Prior History (unchanged)
  COALESCE(pa.prev_admits_1yr, 0) AS prev_admits_1yr,
  COALESCE(pev.prev_ed_visits_1yr, 0) AS prev_ed_visits_1yr,

  -- Derived features
  CASE WHEN vs.sbp_max_6h > 0 THEN vs.sbp_std_6h / vs.sbp_mean_6h ELSE NULL END AS sbp_cv_6h,
  vs.hr_max_6h - vs.hr_min_6h AS hr_range_6h,

  -- Missing indicators
  CASE WHEN lf.lactate_first_6h IS NULL THEN 1 ELSE 0 END AS missing_lactate_6h,
  CASE WHEN lf.troponin_first_6h IS NULL THEN 1 ELSE 0 END AS missing_troponin_6h

FROM truncation_cutoff c
LEFT JOIN vitals_summary vs ON vs.stay_id = c.stay_id
LEFT JOIN triage_check tc ON tc.stay_id = c.stay_id
LEFT JOIN labs_first lf ON lf.stay_id = c.stay_id
LEFT JOIN ed_process_metrics epm ON epm.stay_id = c.stay_id
LEFT JOIN prior_admissions pa ON pa.stay_id = c.stay_id
LEFT JOIN prior_ed_visits pev ON pev.stay_id = c.stay_id;

-- Create index
CREATE INDEX idx_{features_w6_truncated}_stay_id ON {features_w6_truncated}(stay_id);
