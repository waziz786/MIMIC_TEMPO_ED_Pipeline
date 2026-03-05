-- ══════════════════════════════════════════════════════════════════
-- QA Checks — run after every pipeline build
-- Each query should return 0 rows if the assertion holds.
-- Any rows returned indicate a data integrity issue.
-- ══════════════════════════════════════════════════════════════════

-- ── CHECK 1: No timed events before ED arrival ──────────────────
-- Every timed event in the event log must have event_time >= ed_intime.
-- If rows appear here, an event extraction SQL is leaking pre-ED data.

SELECT
  'events_before_ed' AS check_name,
  e.stay_id,
  e.event_type,
  e.event_time,
  b.ed_intime,
  EXTRACT(EPOCH FROM (e.event_time - b.ed_intime)) / 3600.0 AS hours_from_ed
FROM {event_log} e
JOIN {base_ed_cohort} b USING (stay_id)
WHERE e.event_time IS NOT NULL
  AND e.event_time < b.ed_intime;

-- ── CHECK 2: ICD-based events must have NULL event_time ─────────
-- ACS, PCI, CABG, CARDIAC_ARREST come from diagnosis/procedure codes
-- with no real timestamp.  They must store event_time = NULL.

SELECT
  'icd_events_with_time' AS check_name,
  e.stay_id,
  e.event_type,
  e.event_time
FROM {event_log} e
WHERE e.event_type IN ('ACS', 'PCI', 'CABG', 'CARDIAC_ARREST')
  AND e.event_time IS NOT NULL;

-- ── CHECK 3: Monotonicity of event_by flags ─────────────────────
-- event_by flags are cumulative: if event_by_X_w1 = 1 then
-- event_by_X_w6 and event_by_X_w24 must also be 1.
-- W1 <= W6 <= W24.

SELECT
  'event_by_monotonicity_icu' AS check_name,
  stay_id
FROM {outcomes}
WHERE event_by_icu_w1 > event_by_icu_w6
   OR event_by_icu_w6 > event_by_icu_w24

UNION ALL

SELECT
  'event_by_monotonicity_pressor' AS check_name,
  stay_id
FROM {outcomes}
WHERE event_by_pressor_w1 > event_by_pressor_w6
   OR event_by_pressor_w6 > event_by_pressor_w24

UNION ALL

SELECT
  'event_by_monotonicity_vent' AS check_name,
  stay_id
FROM {outcomes}
WHERE event_by_vent_w1 > event_by_vent_w6
   OR event_by_vent_w6 > event_by_vent_w24

UNION ALL

SELECT
  'event_by_monotonicity_rrt' AS check_name,
  stay_id
FROM {outcomes}
WHERE event_by_rrt_w1 > event_by_rrt_w6
   OR event_by_rrt_w6 > event_by_rrt_w24

UNION ALL

SELECT
  'event_by_monotonicity_death' AS check_name,
  stay_id
FROM {outcomes}
WHERE event_by_death_w1 > event_by_death_w6
   OR event_by_death_w6 > event_by_death_w24

UNION ALL

SELECT
  'event_by_monotonicity_deterioration' AS check_name,
  stay_id
FROM {outcomes}
WHERE event_by_deterioration_w1 > event_by_deterioration_w6
   OR event_by_deterioration_w6 > event_by_deterioration_w24;

-- ── CHECK 4: Base cohort age bounds ─────────────────────────────
-- All patients should be 18-110 years old.

SELECT
  'age_out_of_bounds' AS check_name,
  stay_id,
  age_at_ed
FROM {base_ed_cohort}
WHERE age_at_ed < 18 OR age_at_ed > 110;

-- ── CHECK 5: No deceased patients at ED arrival ─────────────────
-- Patient dod (if known) must be on or after ed_intime.

SELECT
  'dead_before_ed' AS check_name,
  stay_id,
  ed_intime,
  dod
FROM {base_ed_cohort}
WHERE dod IS NOT NULL
  AND ed_intime > dod;

-- ── CHECK 6: ECG time within ED window ──────────────────────────
-- Every matched ECG should have ecg_time between ed_intime and ed_outtime.

SELECT
  'ecg_outside_ed_window' AS check_name,
  e.stay_id,
  e.ecg_time_w6,
  b.ed_intime,
  b.ed_outtime
FROM {ecg_features_w6} e
JOIN {base_ed_cohort} b USING (stay_id)
WHERE e.ecg_time_w6 IS NOT NULL
  AND (e.ecg_time_w6 < b.ed_intime OR e.ecg_time_w6 > b.ed_outtime);

-- ── CHECK 7: Informational — label/event_by overlap ─────────
-- Patients where ICU happened during W6 window AND ALSO in label
-- window.  These are multi-admission cases, not integrity errors.
-- The event_by_icu_w6 flag correctly marks them.

SELECT
  'label_event_by_overlap_icu_w6_INFO' AS check_name,
  COUNT(*) AS n_cases
FROM {outcomes} o
WHERE o.icu_24h_from_w6 = 1
  AND o.event_by_icu_w6 = 1
  AND o.time_to_icu <= 6;

-- ── CHECK 8: Death event uses admissions.deathtime ──────────────
-- Confirm DEATH events have source_table = 'hosp.admissions.deathtime'

SELECT
  'death_wrong_source' AS check_name,
  e.stay_id,
  e.source_table
FROM {event_log} e
WHERE e.event_type = 'DEATH'
  AND e.source_table != 'hosp.admissions.deathtime';
