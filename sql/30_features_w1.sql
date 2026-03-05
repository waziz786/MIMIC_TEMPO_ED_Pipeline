-- Feature Basket W1 (First Hour)
-- Triage vitals + first documented vitals within 1 hour of ED arrival

DROP TABLE IF EXISTS {features_w1};

CREATE TABLE {features_w1} AS
WITH triage_data AS (
  SELECT
    t.stay_id,
    t.temperature AS triage_temperature,
    t.heartrate AS triage_heartrate,
    t.resprate AS triage_resprate,
    t.o2sat AS triage_o2sat,
    t.sbp AS triage_sbp,
    t.dbp AS triage_dbp,
    t.pain AS triage_pain,
    t.acuity AS triage_acuity,
    t.chiefcomplaint
  FROM {ed_schema}.triage t
),
first_vitals AS (
  SELECT DISTINCT ON (v.stay_id)
    v.stay_id,
    v.charttime,
    v.temperature AS v_temperature,
    v.heartrate AS v_heartrate,
    v.resprate AS v_resprate,
    v.o2sat AS v_o2sat,
    v.sbp AS v_sbp,
    v.dbp AS v_dbp
  FROM {ed_schema}.vitalsign v
  INNER JOIN {base_ed_cohort} b ON b.stay_id = v.stay_id
  WHERE v.charttime BETWEEN b.ed_intime AND LEAST(b.ed_outtime, b.ed_intime + INTERVAL '1 hour')
  ORDER BY v.stay_id, v.charttime
)
SELECT
  b.stay_id,
  b.age_at_ed,
  b.gender,
  b.arrival_transport,  -- Arrival mode (ambulance, walk-in, etc.)
  b.race,               -- Race/ethnicity
  
  -- Chief complaint (raw text - can be encoded later)
  td.chiefcomplaint,
  
  -- Vital signs (prefer triage, fallback to first charted)
  COALESCE(td.triage_temperature, fv.v_temperature) AS temp_w1,
  COALESCE(td.triage_heartrate, fv.v_heartrate) AS hr_w1,
  COALESCE(td.triage_resprate, fv.v_resprate) AS rr_w1,
  COALESCE(td.triage_o2sat, fv.v_o2sat) AS spo2_w1,
  COALESCE(td.triage_sbp, fv.v_sbp) AS sbp_w1,
  COALESCE(td.triage_dbp, fv.v_dbp) AS dbp_w1,
  
  -- Triage-specific
  td.triage_pain,
  td.triage_acuity,
  
  -- Derived features
  CASE 
    WHEN COALESCE(td.triage_sbp, fv.v_sbp) > 0 
    THEN COALESCE(td.triage_heartrate, fv.v_heartrate)::float / COALESCE(td.triage_sbp, fv.v_sbp)
    ELSE NULL 
  END AS shock_index_w1,
  
  CASE 
    WHEN COALESCE(td.triage_sbp, fv.v_sbp) IS NOT NULL 
      AND COALESCE(td.triage_dbp, fv.v_dbp) IS NOT NULL
    THEN (COALESCE(td.triage_sbp, fv.v_sbp) + 2 * COALESCE(td.triage_dbp, fv.v_dbp))::float / 3
    ELSE NULL 
  END AS map_w1,
  
  -- Missing indicators
  CASE WHEN td.triage_temperature IS NULL AND fv.v_temperature IS NULL THEN 1 ELSE 0 END AS missing_temp_w1,
  CASE WHEN td.triage_heartrate IS NULL AND fv.v_heartrate IS NULL THEN 1 ELSE 0 END AS missing_hr_w1,
  CASE WHEN td.triage_sbp IS NULL AND fv.v_sbp IS NULL THEN 1 ELSE 0 END AS missing_sbp_w1
  
FROM {base_ed_cohort} b
LEFT JOIN triage_data td ON td.stay_id = b.stay_id
LEFT JOIN first_vitals fv ON fv.stay_id = b.stay_id;

-- Create index
CREATE INDEX idx_{features_w1}_stay_id ON {features_w1}(stay_id);
