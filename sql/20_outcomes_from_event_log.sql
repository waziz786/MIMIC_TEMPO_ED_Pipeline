-- Build Outcomes from Event Log
-- Timed events => prediction-time-aligned labels (no overlap with feature windows).
-- ICD/procedure-code events (no timestamps in MIMIC-IV) => hospitalization-level labels only.
--
-- ══════════════════════════════════════════════════════════════════
-- TEMPORAL ALIGNMENT RULES
-- ══════════════════════════════════════════════════════════════════
--
-- Prediction-Time Alignment (labels):
--   hours_from_ed > P  AND  hours_from_ed <= P + H
--   P = prediction time (end of feature window), H = outcome horizon
--   W1 -> P=1, W6 -> P=6, W24 -> P=24
--
-- Event-By Flags (covariates / filters):
--   hours_from_ed <= P
--   "Did this event already happen by prediction time?"
--
-- All arrival-anchored outcomes (e.g. icu_24h measured from t=0)
-- have been REMOVED.  Every timed label is now anchored to the
-- END of its feature window, guaranteeing zero temporal overlap
-- between features and labels.

DROP TABLE IF EXISTS {outcomes};

CREATE TABLE {outcomes} AS
WITH
-- Base cohort (for anchoring ED times)
base AS (
  SELECT
    stay_id, subject_id, hadm_id, ed_intime, ed_outtime
  FROM {base_ed_cohort}
),

-- Timed events only (with defensive time-range filters)
ev AS (
  SELECT
    e.stay_id,
    e.event_type,
    e.event_time,
    EXTRACT(EPOCH FROM (e.event_time - b.ed_intime)) / 3600.0 AS hours_from_ed
  FROM {event_log} e
  JOIN base b USING (stay_id)
  WHERE e.event_time IS NOT NULL
    AND e.event_time >= b.ed_intime                        -- no events before ED arrival
    AND e.event_time <= b.ed_intime + INTERVAL '30 days'   -- cap at 30 days
),

-- ICD/procedure events: no timestamp => binary hospitalization-level only
icd_ev AS (
  SELECT DISTINCT
    e.stay_id,
    e.event_type
  FROM {event_log} e
  WHERE e.event_time IS NULL
),

-- Aggregate timed outcomes
ev_agg AS (
  SELECT
    stay_id,

    -- Hospitalization-level mortality (unlimited horizon, useful reference)
    MAX(CASE WHEN event_type = 'DEATH' THEN 1 ELSE 0 END) AS death_hosp,

    -- Time-to-event (hours from ED) — useful for survival analysis
    MIN(CASE WHEN event_type = 'ICU_ADMIT' THEN hours_from_ed END) AS time_to_icu,
    MIN(CASE WHEN event_type = 'DEATH'     THEN hours_from_ed END) AS time_to_death,
    MIN(CASE
          WHEN event_type IN ('ICU_ADMIT','PRESSOR_START','VENT_START','RRT_START','DEATH')
          THEN hours_from_ed
        END) AS time_to_deterioration,

    -- ══════════════════════════════════════════════════════════════
    -- PREDICTION-TIME-ALIGNED OUTCOMES
    -- Rule: hours_from_ed > P AND hours_from_ed <= P + H
    -- ══════════════════════════════════════════════════════════════

    -- ── ICU aligned ─────────────────────────────────────────────
    -- 24h horizon
    MAX(CASE WHEN event_type = 'ICU_ADMIT' AND hours_from_ed > 1  AND hours_from_ed <= 25 THEN 1 ELSE 0 END) AS icu_24h_from_w1,
    MAX(CASE WHEN event_type = 'ICU_ADMIT' AND hours_from_ed > 6  AND hours_from_ed <= 30 THEN 1 ELSE 0 END) AS icu_24h_from_w6,
    MAX(CASE WHEN event_type = 'ICU_ADMIT' AND hours_from_ed > 24 AND hours_from_ed <= 48 THEN 1 ELSE 0 END) AS icu_24h_from_w24,
    -- 48h horizon
    MAX(CASE WHEN event_type = 'ICU_ADMIT' AND hours_from_ed > 1  AND hours_from_ed <= 49 THEN 1 ELSE 0 END) AS icu_48h_from_w1,
    MAX(CASE WHEN event_type = 'ICU_ADMIT' AND hours_from_ed > 6  AND hours_from_ed <= 54 THEN 1 ELSE 0 END) AS icu_48h_from_w6,
    MAX(CASE WHEN event_type = 'ICU_ADMIT' AND hours_from_ed > 24 AND hours_from_ed <= 72 THEN 1 ELSE 0 END) AS icu_48h_from_w24,

    -- ── Pressor aligned ────────────────────────────────────────
    -- 24h horizon
    MAX(CASE WHEN event_type = 'PRESSOR_START' AND hours_from_ed > 1  AND hours_from_ed <= 25 THEN 1 ELSE 0 END) AS pressor_24h_from_w1,
    MAX(CASE WHEN event_type = 'PRESSOR_START' AND hours_from_ed > 6  AND hours_from_ed <= 30 THEN 1 ELSE 0 END) AS pressor_24h_from_w6,
    MAX(CASE WHEN event_type = 'PRESSOR_START' AND hours_from_ed > 24 AND hours_from_ed <= 48 THEN 1 ELSE 0 END) AS pressor_24h_from_w24,
    -- 48h horizon
    MAX(CASE WHEN event_type = 'PRESSOR_START' AND hours_from_ed > 1  AND hours_from_ed <= 49 THEN 1 ELSE 0 END) AS pressor_48h_from_w1,
    MAX(CASE WHEN event_type = 'PRESSOR_START' AND hours_from_ed > 6  AND hours_from_ed <= 54 THEN 1 ELSE 0 END) AS pressor_48h_from_w6,
    MAX(CASE WHEN event_type = 'PRESSOR_START' AND hours_from_ed > 24 AND hours_from_ed <= 72 THEN 1 ELSE 0 END) AS pressor_48h_from_w24,

    -- ── Vent aligned ───────────────────────────────────────────
    -- 24h horizon
    MAX(CASE WHEN event_type = 'VENT_START' AND hours_from_ed > 1  AND hours_from_ed <= 25 THEN 1 ELSE 0 END) AS vent_24h_from_w1,
    MAX(CASE WHEN event_type = 'VENT_START' AND hours_from_ed > 6  AND hours_from_ed <= 30 THEN 1 ELSE 0 END) AS vent_24h_from_w6,
    MAX(CASE WHEN event_type = 'VENT_START' AND hours_from_ed > 24 AND hours_from_ed <= 48 THEN 1 ELSE 0 END) AS vent_24h_from_w24,
    -- 48h horizon
    MAX(CASE WHEN event_type = 'VENT_START' AND hours_from_ed > 1  AND hours_from_ed <= 49 THEN 1 ELSE 0 END) AS vent_48h_from_w1,
    MAX(CASE WHEN event_type = 'VENT_START' AND hours_from_ed > 6  AND hours_from_ed <= 54 THEN 1 ELSE 0 END) AS vent_48h_from_w6,
    MAX(CASE WHEN event_type = 'VENT_START' AND hours_from_ed > 24 AND hours_from_ed <= 72 THEN 1 ELSE 0 END) AS vent_48h_from_w24,

    -- ── RRT aligned (24h horizon only) ─────────────────────────
    MAX(CASE WHEN event_type = 'RRT_START' AND hours_from_ed > 1  AND hours_from_ed <= 25 THEN 1 ELSE 0 END) AS rrt_24h_from_w1,
    MAX(CASE WHEN event_type = 'RRT_START' AND hours_from_ed > 6  AND hours_from_ed <= 30 THEN 1 ELSE 0 END) AS rrt_24h_from_w6,
    MAX(CASE WHEN event_type = 'RRT_START' AND hours_from_ed > 24 AND hours_from_ed <= 48 THEN 1 ELSE 0 END) AS rrt_24h_from_w24,

    -- ── Death aligned ──────────────────────────────────────────
    -- 24h horizon
    MAX(CASE WHEN event_type = 'DEATH' AND hours_from_ed > 1  AND hours_from_ed <= 25 THEN 1 ELSE 0 END) AS death_24h_from_w1,
    MAX(CASE WHEN event_type = 'DEATH' AND hours_from_ed > 6  AND hours_from_ed <= 30 THEN 1 ELSE 0 END) AS death_24h_from_w6,
    MAX(CASE WHEN event_type = 'DEATH' AND hours_from_ed > 24 AND hours_from_ed <= 48 THEN 1 ELSE 0 END) AS death_24h_from_w24,
    -- 48h horizon
    MAX(CASE WHEN event_type = 'DEATH' AND hours_from_ed > 1  AND hours_from_ed <= 49 THEN 1 ELSE 0 END) AS death_48h_from_w1,
    MAX(CASE WHEN event_type = 'DEATH' AND hours_from_ed > 6  AND hours_from_ed <= 54 THEN 1 ELSE 0 END) AS death_48h_from_w6,
    MAX(CASE WHEN event_type = 'DEATH' AND hours_from_ed > 24 AND hours_from_ed <= 72 THEN 1 ELSE 0 END) AS death_48h_from_w24,
    -- 72h horizon
    MAX(CASE WHEN event_type = 'DEATH' AND hours_from_ed > 1  AND hours_from_ed <= 73 THEN 1 ELSE 0 END) AS death_72h_from_w1,
    MAX(CASE WHEN event_type = 'DEATH' AND hours_from_ed > 6  AND hours_from_ed <= 78 THEN 1 ELSE 0 END) AS death_72h_from_w6,
    MAX(CASE WHEN event_type = 'DEATH' AND hours_from_ed > 24 AND hours_from_ed <= 96 THEN 1 ELSE 0 END) AS death_72h_from_w24,

    -- ══════════════════════════════════════════════════════════════
    -- EVENT-BY FLAGS: did an event occur WITHIN the feature window?
    -- Rule: hours_from_ed <= P  (P = end of feature window)
    -- These flag "already-happened" events, useful for filtering
    -- or as informative covariates.
    -- ══════════════════════════════════════════════════════════════

    -- ── ICU event_by flags ──────────────────────────────────────
    MAX(CASE WHEN event_type = 'ICU_ADMIT'     AND hours_from_ed <= 1  THEN 1 ELSE 0 END) AS event_by_icu_w1,
    MAX(CASE WHEN event_type = 'ICU_ADMIT'     AND hours_from_ed <= 6  THEN 1 ELSE 0 END) AS event_by_icu_w6,
    MAX(CASE WHEN event_type = 'ICU_ADMIT'     AND hours_from_ed <= 24 THEN 1 ELSE 0 END) AS event_by_icu_w24,

    -- ── Pressor event_by flags ──────────────────────────────────
    MAX(CASE WHEN event_type = 'PRESSOR_START' AND hours_from_ed <= 1  THEN 1 ELSE 0 END) AS event_by_pressor_w1,
    MAX(CASE WHEN event_type = 'PRESSOR_START' AND hours_from_ed <= 6  THEN 1 ELSE 0 END) AS event_by_pressor_w6,
    MAX(CASE WHEN event_type = 'PRESSOR_START' AND hours_from_ed <= 24 THEN 1 ELSE 0 END) AS event_by_pressor_w24,

    -- ── Vent event_by flags ─────────────────────────────────────
    MAX(CASE WHEN event_type = 'VENT_START'    AND hours_from_ed <= 1  THEN 1 ELSE 0 END) AS event_by_vent_w1,
    MAX(CASE WHEN event_type = 'VENT_START'    AND hours_from_ed <= 6  THEN 1 ELSE 0 END) AS event_by_vent_w6,
    MAX(CASE WHEN event_type = 'VENT_START'    AND hours_from_ed <= 24 THEN 1 ELSE 0 END) AS event_by_vent_w24,

    -- ── RRT event_by flags ──────────────────────────────────────
    MAX(CASE WHEN event_type = 'RRT_START'     AND hours_from_ed <= 1  THEN 1 ELSE 0 END) AS event_by_rrt_w1,
    MAX(CASE WHEN event_type = 'RRT_START'     AND hours_from_ed <= 6  THEN 1 ELSE 0 END) AS event_by_rrt_w6,
    MAX(CASE WHEN event_type = 'RRT_START'     AND hours_from_ed <= 24 THEN 1 ELSE 0 END) AS event_by_rrt_w24,

    -- ── Death event_by flags ────────────────────────────────────
    MAX(CASE WHEN event_type = 'DEATH'         AND hours_from_ed <= 1  THEN 1 ELSE 0 END) AS event_by_death_w1,
    MAX(CASE WHEN event_type = 'DEATH'         AND hours_from_ed <= 6  THEN 1 ELSE 0 END) AS event_by_death_w6,
    MAX(CASE WHEN event_type = 'DEATH'         AND hours_from_ed <= 24 THEN 1 ELSE 0 END) AS event_by_death_w24,

    -- ── Composite deterioration event_by flags ──────────────────
    MAX(CASE WHEN event_type IN ('ICU_ADMIT','PRESSOR_START','VENT_START','RRT_START','DEATH')
              AND hours_from_ed <= 1  THEN 1 ELSE 0 END) AS event_by_deterioration_w1,
    MAX(CASE WHEN event_type IN ('ICU_ADMIT','PRESSOR_START','VENT_START','RRT_START','DEATH')
              AND hours_from_ed <= 6  THEN 1 ELSE 0 END) AS event_by_deterioration_w6,
    MAX(CASE WHEN event_type IN ('ICU_ADMIT','PRESSOR_START','VENT_START','RRT_START','DEATH')
              AND hours_from_ed <= 24 THEN 1 ELSE 0 END) AS event_by_deterioration_w24

  FROM ev
  GROUP BY stay_id
),

-- ICD aggregation: hospitalization-level only
icd_agg AS (
  SELECT
    stay_id,
    MAX(CASE WHEN event_type = 'CARDIAC_ARREST' THEN 1 ELSE 0 END) AS cardiac_arrest_hosp,
    MAX(CASE WHEN event_type = 'ACS' THEN 1 ELSE 0 END)            AS acs_hosp,
    MAX(CASE WHEN event_type IN ('PCI','CABG') THEN 1 ELSE 0 END)  AS revasc_hosp,
    MAX(CASE WHEN event_type = 'PCI' THEN 1 ELSE 0 END)            AS pci_hosp,
    MAX(CASE WHEN event_type = 'CABG' THEN 1 ELSE 0 END)           AS cabg_hosp
  FROM icd_ev
  GROUP BY stay_id
)

SELECT
  b.stay_id,
  b.subject_id,
  b.hadm_id,
  b.ed_intime,
  b.ed_outtime,

  -- ══════════════════════════════════════════════════════════════
  -- HOSPITALIZATION-LEVEL OUTCOMES
  -- ══════════════════════════════════════════════════════════════

  -- Timed: in-hospital mortality (any time during admission)
  COALESCE(a.death_hosp, 0) AS death_hosp,

  -- ICD-based (no temporal alignment needed)
  COALESCE(i.cardiac_arrest_hosp, 0) AS cardiac_arrest_hosp,
  COALESCE(i.acs_hosp, 0)            AS acs_hosp,
  COALESCE(i.revasc_hosp, 0)         AS revasc_hosp,
  COALESCE(i.pci_hosp, 0)            AS pci_hosp,
  COALESCE(i.cabg_hosp, 0)           AS cabg_hosp,

  -- Composite ICD: any coronary event
  CASE
    WHEN COALESCE(i.acs_hosp, 0) = 1 OR COALESCE(i.revasc_hosp, 0) = 1
    THEN 1 ELSE 0
  END AS coronary_event_hosp,

  -- Time-to-event (timed events only, hours from ED arrival)
  a.time_to_icu,
  a.time_to_death,
  a.time_to_deterioration,

  -- ══════════════════════════════════════════════════════════════
  -- PREDICTION-TIME-ALIGNED INDIVIDUAL OUTCOMES
  -- Rule: hours_from_ed > P AND hours_from_ed <= P + H
  -- ══════════════════════════════════════════════════════════════

  -- ── ICU aligned ───────────────────────────────────────────────
  COALESCE(a.icu_24h_from_w1, 0)  AS icu_24h_from_w1,
  COALESCE(a.icu_24h_from_w6, 0)  AS icu_24h_from_w6,
  COALESCE(a.icu_24h_from_w24, 0) AS icu_24h_from_w24,
  COALESCE(a.icu_48h_from_w1, 0)  AS icu_48h_from_w1,
  COALESCE(a.icu_48h_from_w6, 0)  AS icu_48h_from_w6,
  COALESCE(a.icu_48h_from_w24, 0) AS icu_48h_from_w24,

  -- ── Pressor aligned ──────────────────────────────────────────
  COALESCE(a.pressor_24h_from_w1, 0)  AS pressor_24h_from_w1,
  COALESCE(a.pressor_24h_from_w6, 0)  AS pressor_24h_from_w6,
  COALESCE(a.pressor_24h_from_w24, 0) AS pressor_24h_from_w24,
  COALESCE(a.pressor_48h_from_w1, 0)  AS pressor_48h_from_w1,
  COALESCE(a.pressor_48h_from_w6, 0)  AS pressor_48h_from_w6,
  COALESCE(a.pressor_48h_from_w24, 0) AS pressor_48h_from_w24,

  -- ── Vent aligned ─────────────────────────────────────────────
  COALESCE(a.vent_24h_from_w1, 0)  AS vent_24h_from_w1,
  COALESCE(a.vent_24h_from_w6, 0)  AS vent_24h_from_w6,
  COALESCE(a.vent_24h_from_w24, 0) AS vent_24h_from_w24,
  COALESCE(a.vent_48h_from_w1, 0)  AS vent_48h_from_w1,
  COALESCE(a.vent_48h_from_w6, 0)  AS vent_48h_from_w6,
  COALESCE(a.vent_48h_from_w24, 0) AS vent_48h_from_w24,

  -- ── RRT aligned (24h only) ───────────────────────────────────
  COALESCE(a.rrt_24h_from_w1, 0)  AS rrt_24h_from_w1,
  COALESCE(a.rrt_24h_from_w6, 0)  AS rrt_24h_from_w6,
  COALESCE(a.rrt_24h_from_w24, 0) AS rrt_24h_from_w24,

  -- ── Death aligned ────────────────────────────────────────────
  COALESCE(a.death_24h_from_w1, 0)  AS death_24h_from_w1,
  COALESCE(a.death_24h_from_w6, 0)  AS death_24h_from_w6,
  COALESCE(a.death_24h_from_w24, 0) AS death_24h_from_w24,
  COALESCE(a.death_48h_from_w1, 0)  AS death_48h_from_w1,
  COALESCE(a.death_48h_from_w6, 0)  AS death_48h_from_w6,
  COALESCE(a.death_48h_from_w24, 0) AS death_48h_from_w24,
  COALESCE(a.death_72h_from_w1, 0)  AS death_72h_from_w1,
  COALESCE(a.death_72h_from_w6, 0)  AS death_72h_from_w6,
  COALESCE(a.death_72h_from_w24, 0) AS death_72h_from_w24,

  -- ══════════════════════════════════════════════════════════════
  -- PREDICTION-TIME-ALIGNED COMPOSITE OUTCOMES
  -- ══════════════════════════════════════════════════════════════

  -- 24h horizon composites
  CASE
    WHEN COALESCE(a.icu_24h_from_w1, 0) = 1
      OR COALESCE(a.pressor_24h_from_w1, 0) = 1
      OR COALESCE(a.vent_24h_from_w1, 0) = 1
      OR COALESCE(a.rrt_24h_from_w1, 0) = 1
      OR COALESCE(a.death_24h_from_w1, 0) = 1
    THEN 1 ELSE 0
  END AS deterioration_24h_from_w1,

  CASE
    WHEN COALESCE(a.icu_24h_from_w6, 0) = 1
      OR COALESCE(a.pressor_24h_from_w6, 0) = 1
      OR COALESCE(a.vent_24h_from_w6, 0) = 1
      OR COALESCE(a.rrt_24h_from_w6, 0) = 1
      OR COALESCE(a.death_24h_from_w6, 0) = 1
    THEN 1 ELSE 0
  END AS deterioration_24h_from_w6,

  CASE
    WHEN COALESCE(a.icu_24h_from_w24, 0) = 1
      OR COALESCE(a.pressor_24h_from_w24, 0) = 1
      OR COALESCE(a.vent_24h_from_w24, 0) = 1
      OR COALESCE(a.rrt_24h_from_w24, 0) = 1
      OR COALESCE(a.death_24h_from_w24, 0) = 1
    THEN 1 ELSE 0
  END AS deterioration_24h_from_w24,

  -- 48h horizon composites
  CASE
    WHEN COALESCE(a.icu_48h_from_w1, 0) = 1
      OR COALESCE(a.pressor_48h_from_w1, 0) = 1
      OR COALESCE(a.vent_48h_from_w1, 0) = 1
      OR COALESCE(a.rrt_24h_from_w1, 0) = 1
      OR COALESCE(a.death_48h_from_w1, 0) = 1
    THEN 1 ELSE 0
  END AS deterioration_48h_from_w1,

  CASE
    WHEN COALESCE(a.icu_48h_from_w6, 0) = 1
      OR COALESCE(a.pressor_48h_from_w6, 0) = 1
      OR COALESCE(a.vent_48h_from_w6, 0) = 1
      OR COALESCE(a.rrt_24h_from_w6, 0) = 1
      OR COALESCE(a.death_48h_from_w6, 0) = 1
    THEN 1 ELSE 0
  END AS deterioration_48h_from_w6,

  CASE
    WHEN COALESCE(a.icu_48h_from_w24, 0) = 1
      OR COALESCE(a.pressor_48h_from_w24, 0) = 1
      OR COALESCE(a.vent_48h_from_w24, 0) = 1
      OR COALESCE(a.rrt_24h_from_w24, 0) = 1
      OR COALESCE(a.death_48h_from_w24, 0) = 1
    THEN 1 ELSE 0
  END AS deterioration_48h_from_w24,

  -- ══════════════════════════════════════════════════════════════
  -- EVENT-BY FLAGS: did event occur WITHIN (i.e. before end of)
  -- the feature window?  Useful for cohort filtering or as
  -- covariates indicating "already deteriorated by prediction time".
  -- Only defined for TIMED events (not ICD-based).
  -- ══════════════════════════════════════════════════════════════

  -- ── ICU event_by ──────────────────────────────────────────────
  COALESCE(a.event_by_icu_w1, 0)  AS event_by_icu_w1,
  COALESCE(a.event_by_icu_w6, 0)  AS event_by_icu_w6,
  COALESCE(a.event_by_icu_w24, 0) AS event_by_icu_w24,

  -- ── Pressor event_by ──────────────────────────────────────────
  COALESCE(a.event_by_pressor_w1, 0)  AS event_by_pressor_w1,
  COALESCE(a.event_by_pressor_w6, 0)  AS event_by_pressor_w6,
  COALESCE(a.event_by_pressor_w24, 0) AS event_by_pressor_w24,

  -- ── Vent event_by ─────────────────────────────────────────────
  COALESCE(a.event_by_vent_w1, 0)  AS event_by_vent_w1,
  COALESCE(a.event_by_vent_w6, 0)  AS event_by_vent_w6,
  COALESCE(a.event_by_vent_w24, 0) AS event_by_vent_w24,

  -- ── RRT event_by ──────────────────────────────────────────────
  COALESCE(a.event_by_rrt_w1, 0)  AS event_by_rrt_w1,
  COALESCE(a.event_by_rrt_w6, 0)  AS event_by_rrt_w6,
  COALESCE(a.event_by_rrt_w24, 0) AS event_by_rrt_w24,

  -- ── Death event_by ────────────────────────────────────────────
  COALESCE(a.event_by_death_w1, 0)  AS event_by_death_w1,
  COALESCE(a.event_by_death_w6, 0)  AS event_by_death_w6,
  COALESCE(a.event_by_death_w24, 0) AS event_by_death_w24,

  -- ── Composite deterioration event_by ──────────────────────────
  COALESCE(a.event_by_deterioration_w1, 0)  AS event_by_deterioration_w1,
  COALESCE(a.event_by_deterioration_w6, 0)  AS event_by_deterioration_w6,
  COALESCE(a.event_by_deterioration_w24, 0) AS event_by_deterioration_w24

FROM base b
LEFT JOIN ev_agg a USING (stay_id)
LEFT JOIN icd_agg i USING (stay_id);

-- Indexes for joins
CREATE INDEX IF NOT EXISTS idx_{outcomes}_stay_id ON {outcomes}(stay_id);
CREATE INDEX IF NOT EXISTS idx_{outcomes}_hadm_id ON {outcomes}(hadm_id);
