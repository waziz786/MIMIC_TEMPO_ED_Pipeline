-- Feature Basket W24 TRUNCATED (Leakage-Proof)
-- =============================================
-- Identical features to 32_features_w24.sql, but the observation window is
-- capped at  min(24 h, time_to_first_deterioration)  for every patient.
--
-- For negatives (no event): window = full 24 hours (identical to naive W24)
-- For positives with event AFTER 24h: window = full 24 hours (no change)
-- For positives with event BEFORE 24h: window = [ed_intime, event_time]
--
-- This is the most critical truncation because 24h features have COMPLETE
-- overlap with the 24h outcome window and substantial overlap with 48h.

DROP TABLE IF EXISTS {features_w24_truncated};

CREATE TABLE {features_w24_truncated} AS

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
      b.ed_intime + INTERVAL '24 hours',
      CASE
        WHEN o.time_to_deterioration IS NOT NULL
        THEN b.ed_intime + (o.time_to_deterioration * INTERVAL '1 hour')
        ELSE b.ed_intime + INTERVAL '24 hours'
      END
    ) AS t_cut
  FROM {base_ed_cohort} b
  JOIN {outcomes} o ON b.stay_id = o.stay_id
),

-- ── Vitals aggregated within truncated window ──────────────────────────
vitals_summary_24h AS (
  SELECT
    c.stay_id,
    -- Blood pressure (24h truncated)
    MIN(v.sbp) AS sbp_min_24h,
    MAX(v.sbp) AS sbp_max_24h,
    AVG(v.sbp) AS sbp_mean_24h,
    STDDEV(v.sbp) AS sbp_std_24h,

    -- Heart rate (24h truncated)
    MIN(v.heartrate) AS hr_min_24h,
    MAX(v.heartrate) AS hr_max_24h,
    AVG(v.heartrate) AS hr_mean_24h,

    -- Respiratory rate (24h truncated)
    MAX(v.resprate) AS rr_max_24h,
    AVG(v.resprate) AS rr_mean_24h,

    -- Oxygen saturation (24h truncated)
    MIN(v.o2sat) AS spo2_min_24h,
    AVG(v.o2sat) AS spo2_mean_24h,

    -- Temperature (24h truncated)
    MAX(v.temperature) AS temp_max_24h,

    -- Measurement count
    COUNT(*) AS n_vitalsign_measurements_24h
  FROM truncation_cutoff c
  INNER JOIN {ed_schema}.vitalsign v ON c.stay_id = v.stay_id
  WHERE v.charttime BETWEEN c.ed_intime AND c.t_cut
  GROUP BY c.stay_id
),

-- ── Triage (at arrival — unaffected by truncation) ─────────────────────
triage_data AS (
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
      WHEN LOWER(label) LIKE '%glucose%' THEN 'glucose'
      WHEN LOWER(label) LIKE '%bun%' OR LOWER(label) LIKE '%urea nitrogen%' THEN 'bun'
      WHEN LOWER(label) LIKE '%bilirubin%' THEN 'bilirubin'
      WHEN LOWER(label) LIKE '%inr%' THEN 'inr'
    END AS lab_type,
    CASE WHEN itemid = 51003 THEN 1 ELSE 0 END AS is_hs_troponin
  FROM {hosp_schema}.d_labitems
  WHERE LOWER(label) SIMILAR TO '%(lactate|troponin|creatinine|potassium|sodium|bicarbonate|hco3|white blood|wbc|hemoglobin|hgb|platelet|glucose|bun|urea nitrogen|bilirubin|inr)%'
),

-- ── Raw lab events (global row numbering unchanged) ────────────────────
labs_raw AS (
  SELECT
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

-- ── Lab summary within TRUNCATED window ────────────────────────────────
labs_summary AS (
  SELECT
    c.stay_id,
    c.ed_intime,
    -- First values
    MAX(CASE WHEN lr.lab_type = 'lactate' AND lr.rn = 1 THEN lr.valuenum END) AS lactate_first_24h,
    MAX(CASE WHEN lr.lab_type = 'troponin' AND lr.rn = 1 THEN lr.valuenum END) AS troponin_first_24h,
    MAX(CASE WHEN lr.lab_type = 'troponin' AND lr.rn = 1 THEN lr.is_hs_troponin END) AS is_hs_troponin_24h,
    MAX(CASE WHEN lr.lab_type = 'creatinine' AND lr.rn = 1 THEN lr.valuenum END) AS creatinine_first_24h,
    MAX(CASE WHEN lr.lab_type = 'potassium' AND lr.rn = 1 THEN lr.valuenum END) AS potassium_first_24h,
    MAX(CASE WHEN lr.lab_type = 'sodium' AND lr.rn = 1 THEN lr.valuenum END) AS sodium_first_24h,
    MAX(CASE WHEN lr.lab_type = 'bicarbonate' AND lr.rn = 1 THEN lr.valuenum END) AS bicarbonate_first_24h,
    MAX(CASE WHEN lr.lab_type = 'wbc' AND lr.rn = 1 THEN lr.valuenum END) AS wbc_first_24h,
    MAX(CASE WHEN lr.lab_type = 'hemoglobin' AND lr.rn = 1 THEN lr.valuenum END) AS hemoglobin_first_24h,
    MAX(CASE WHEN lr.lab_type = 'platelet' AND lr.rn = 1 THEN lr.valuenum END) AS platelet_first_24h,
    MAX(CASE WHEN lr.lab_type = 'glucose' AND lr.rn = 1 THEN lr.valuenum END) AS glucose_first_24h,
    MAX(CASE WHEN lr.lab_type = 'bun' AND lr.rn = 1 THEN lr.valuenum END) AS bun_first_24h,
    MAX(CASE WHEN lr.lab_type = 'bilirubin' AND lr.rn = 1 THEN lr.valuenum END) AS bilirubin_first_24h,
    MAX(CASE WHEN lr.lab_type = 'inr' AND lr.rn = 1 THEN lr.valuenum END) AS inr_first_24h,
    -- Max values (for trending, within truncated window)
    MAX(CASE WHEN lr.lab_type = 'lactate' THEN lr.valuenum END) AS lactate_max_24h,
    MAX(CASE WHEN lr.lab_type = 'creatinine' THEN lr.valuenum END) AS creatinine_max_24h,
    -- Time to first lab result
    EXTRACT(EPOCH FROM (MIN(CASE WHEN lr.charttime IS NOT NULL THEN lr.charttime END) - c.ed_intime)) / 3600 AS time_to_first_lab_hours
  FROM truncation_cutoff c
  INNER JOIN labs_raw lr ON c.hadm_id = lr.hadm_id
  WHERE lr.charttime BETWEEN c.ed_intime AND c.t_cut
  GROUP BY c.stay_id, c.ed_intime
),

-- ── ED process metrics within TRUNCATED window ─────────────────────────
ed_process_metrics_24 AS (
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
prior_admissions_24 AS (
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
prior_ed_visits_24 AS (
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

-- ── Final SELECT (identical column set to naive W24) ───────────────────
SELECT
  c.stay_id,

  -- Demographics (unchanged)
  c.age_at_ed,
  c.gender,
  c.arrival_transport,
  c.race,

  -- Vital signs (24h truncated)
  vs.sbp_min_24h,
  vs.sbp_max_24h,
  vs.sbp_mean_24h,
  vs.sbp_std_24h,
  vs.hr_min_24h,
  vs.hr_max_24h,
  vs.hr_mean_24h,
  vs.rr_max_24h,
  vs.rr_mean_24h,
  vs.spo2_min_24h,
  vs.spo2_mean_24h,
  vs.temp_max_24h,
  COALESCE(vs.n_vitalsign_measurements_24h, 0) AS n_vitalsign_measurements_24h,
  td.chiefcomplaint,

  -- Laboratory values (within truncated window)
  ls.lactate_first_24h,
  ls.troponin_first_24h,
  ls.is_hs_troponin_24h,
  ls.creatinine_first_24h,
  ls.potassium_first_24h,
  ls.sodium_first_24h,
  ls.bicarbonate_first_24h,
  ls.wbc_first_24h,
  ls.hemoglobin_first_24h,
  ls.platelet_first_24h,
  ls.glucose_first_24h,
  ls.bun_first_24h,
  ls.bilirubin_first_24h,
  ls.inr_first_24h,
  ls.lactate_max_24h,
  ls.creatinine_max_24h,

  -- ED Process Metrics (within truncated window)
  c.ed_los_hours,
  ls.time_to_first_lab_hours,
  epm.time_to_first_med_hours,

  -- Prior History (unchanged)
  COALESCE(pa.prev_admits_1yr, 0) AS prev_admits_1yr,
  COALESCE(pev.prev_ed_visits_1yr, 0) AS prev_ed_visits_1yr,

  -- Derived lab features
  CASE WHEN ls.creatinine_first_24h > 0 THEN ls.bun_first_24h / ls.creatinine_first_24h ELSE NULL END AS bun_creatinine_ratio,
  ls.lactate_max_24h - ls.lactate_first_24h AS lactate_delta_24h

FROM truncation_cutoff c
LEFT JOIN vitals_summary_24h vs ON vs.stay_id = c.stay_id
LEFT JOIN triage_data td ON td.stay_id = c.stay_id
LEFT JOIN labs_summary ls ON ls.stay_id = c.stay_id
LEFT JOIN ed_process_metrics_24 epm ON epm.stay_id = c.stay_id
LEFT JOIN prior_admissions_24 pa ON pa.stay_id = c.stay_id
LEFT JOIN prior_ed_visits_24 pev ON pev.stay_id = c.stay_id;

-- Create index
CREATE INDEX idx_{features_w24_truncated}_stay_id ON {features_w24_truncated}(stay_id);
