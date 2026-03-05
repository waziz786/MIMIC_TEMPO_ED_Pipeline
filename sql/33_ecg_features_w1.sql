-- ECG Features Window 1 (W1): First ECG within 1 hour of ED arrival
-- 
-- Extracts machine-derived ECG measurements for the first ECG recorded
-- within 1 hour of ED arrival. Features are derived from:
--   - RR interval (to compute heart rate)
--   - QRS onset/end (QRS duration)
--   - P onset (PR interval)
--   - T end (QT interval proxy)
--   - Axis measurements (P, QRS, T)
--
-- UNIT VERIFICATION (per MIMIC-IV-ECG documentation):
--   - All timing fields (rr_interval, p_onset, p_end, qrs_onset, qrs_end, t_end) are in MILLISECONDS
--   - Axis measurements (p_axis, qrs_axis, t_axis) are in DEGREES
--
-- IMPORTANT LIMITATION:
--   - mimiciv_ecg.record_list contains only subject_id (no hadm_id or stay_id)
--   - ECGs include inpatient + outpatient studies across patient lifetime
--   - Time-based filtering (ecg_time BETWEEN ed_intime AND window) is the best available
--     method to restrict to ED-visit-related ECGs, but may include ECGs from concurrent
--     admissions if timing overlaps

DROP TABLE IF EXISTS {ecg_features_w1};

CREATE TABLE {ecg_features_w1} AS
WITH candidates AS (
  -- Find all ECGs within 1 hour of ED arrival
  SELECT
    b.stay_id,
    b.subject_id,
    r.study_id,
    r.ecg_time,
    EXTRACT(EPOCH FROM (r.ecg_time - b.ed_intime)) / 3600.0 AS hours_from_ed
  FROM {base_ed_cohort} b
  INNER JOIN {ecg_record_list} r
    ON b.subject_id = r.subject_id
  WHERE r.ecg_time IS NOT NULL
    AND r.ecg_time BETWEEN b.ed_intime AND LEAST(b.ed_outtime, b.ed_intime + INTERVAL '1 hour')
),
first_ecg AS (
  -- Select only the first ECG for each stay
  SELECT DISTINCT ON (stay_id)
    stay_id,
    subject_id,
    study_id,
    ecg_time,
    hours_from_ed
  FROM candidates
  ORDER BY stay_id, ecg_time
)
SELECT
  b.stay_id,
  f.study_id AS ecg_study_id_w1,
  f.ecg_time AS ecg_time_w1,
  f.hours_from_ed AS ecg_hours_from_ed_w1,

  -- Heart rate derived from RR interval (RR in ms → HR in bpm)
  CASE 
    WHEN m.rr_interval IS NOT NULL AND m.rr_interval > 0
    THEN 60000.0 / m.rr_interval
    ELSE NULL
  END AS ecg_hr_w1,

  -- RR interval (raw, in ms)
  m.rr_interval AS ecg_rr_interval_w1,

  -- QRS duration (ms) = qrs_end - qrs_onset
  CASE 
    WHEN m.qrs_onset IS NOT NULL AND m.qrs_end IS NOT NULL
    THEN (m.qrs_end - m.qrs_onset)
    ELSE NULL
  END AS ecg_qrs_dur_w1,

  -- PR interval (ms) = qrs_onset - p_onset
  CASE 
    WHEN m.p_onset IS NOT NULL AND m.qrs_onset IS NOT NULL
    THEN (m.qrs_onset - m.p_onset)
    ELSE NULL
  END AS ecg_pr_w1,

  -- QT interval proxy (ms) = t_end - qrs_onset
  CASE 
    WHEN m.t_end IS NOT NULL AND m.qrs_onset IS NOT NULL
    THEN (m.t_end - m.qrs_onset)
    ELSE NULL
  END AS ecg_qt_w1,

  -- Raw timing measurements
  m.p_onset AS ecg_p_onset_w1,
  m.p_end AS ecg_p_end_w1,
  m.qrs_onset AS ecg_qrs_onset_w1,
  m.qrs_end AS ecg_qrs_end_w1,
  m.t_end AS ecg_t_end_w1,

  -- Axis measurements (degrees)
  m.p_axis AS ecg_p_axis_w1,
  m.qrs_axis AS ecg_qrs_axis_w1,
  m.t_axis AS ecg_t_axis_w1,

  -- Missing indicator
  CASE WHEN f.study_id IS NULL THEN 1 ELSE 0 END AS missing_ecg_w1

FROM {base_ed_cohort} b
LEFT JOIN first_ecg f ON b.stay_id = f.stay_id
LEFT JOIN {ecg_machine_measurements} m ON f.study_id = m.study_id;

-- Create index for joins
CREATE INDEX IF NOT EXISTS idx_{ecg_features_w1}_stay_id ON {ecg_features_w1}(stay_id);
