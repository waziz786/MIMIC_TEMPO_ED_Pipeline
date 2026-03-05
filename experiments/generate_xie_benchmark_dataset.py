"""
Xie Benchmark Comparison - Dataset Generation
==============================================
Creates a dataset from our pipeline that matches Xie et al. benchmark structure
for direct comparison.

Mapping:
--------
Xie Outcome -> Our Pipeline Outcome
- outcome_hospitalization -> was_admitted (from base cohort)
- outcome_icu_transfer_12h -> icu_12h (custom, need to create)
- outcome_critical -> deterioration_24h (composite)
- outcome_ed_revisit_3d -> (need to compute from ED visits)
- outcome_inhospital_mortality -> death_hosp

Features:
---------
Using W1 (triage) features to match Xie's triage-level features:
- age, gender, race, arrival_transport
- triage vitals: temperature, heartrate, resprate, o2sat, sbp, dbp, pain, acuity
- chiefcomplaint
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import timedelta

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from db import get_conn
from utils import load_yaml

# ============================================================================
# CONFIG
# ============================================================================

OUTPUT = Path("artifacts/datasets/xie_benchmark_comparison.csv")
OUTPUT.parent.mkdir(parents=True, exist_ok=True)

cfg = load_yaml("config/config.yaml")


# ============================================================================
# MAIN QUERY - Generate Xie-Compatible Dataset
# ============================================================================

SQL = """
WITH base AS (
    -- Base ED cohort with hospital admission flag
    SELECT 
        bc.subject_id,
        bc.hadm_id,
        bc.stay_id,
        bc.ed_intime AS intime,
        bc.ed_outtime AS outtime,
        bc.age_at_ed AS age,
        bc.gender,
        bc.race,
        bc.arrival_transport,
        bc.was_admitted AS outcome_hospitalization,
        bc.ed_los_hours
    FROM {base_ed_cohort} bc
    WHERE bc.age_at_ed >= 18  -- Adult only
),

icu_times AS (
    -- Get ICU admission times relative to ED arrival
    SELECT 
        el.stay_id,
        MIN(EXTRACT(EPOCH FROM (el.event_time - bc.ed_intime))/3600.0) AS time_to_icu_hours
    FROM {event_log} el
    INNER JOIN {base_ed_cohort} bc ON el.stay_id = bc.stay_id
    WHERE el.event_type = 'ICU_ADMIT'
      AND el.event_time IS NOT NULL
      AND el.event_time >= bc.ed_intime
    GROUP BY el.stay_id
),

deterioration_events AS (
    -- Deterioration events within 24h
    SELECT 
        el.stay_id,
        MIN(EXTRACT(EPOCH FROM (el.event_time - bc.ed_intime))/3600.0) AS time_to_deterioration_hours
    FROM {event_log} el
    INNER JOIN {base_ed_cohort} bc ON el.stay_id = bc.stay_id
    WHERE el.event_type IN ('ICU_ADMIT', 'PRESSOR_START', 'VENT_START', 'RRT_START', 'DEATH')
      AND el.event_time IS NOT NULL
      AND el.event_time >= bc.ed_intime
      AND EXTRACT(EPOCH FROM (el.event_time - bc.ed_intime))/3600.0 <= 24
    GROUP BY el.stay_id
),

death_events AS (
    -- In-hospital mortality
    SELECT 
        el.stay_id,
        1::int AS outcome_inhospital_mortality
    FROM {event_log} el
    WHERE el.event_type = 'DEATH'
      AND el.event_time IS NOT NULL
    GROUP BY el.stay_id
),

triage_vitals AS (
    -- Triage-level vitals from features_w1
    SELECT 
        f.stay_id,
        f.temp_w1 AS triage_temperature,
        f.hr_w1 AS triage_heartrate,
        f.rr_w1 AS triage_resprate,
        f.spo2_w1 AS triage_o2sat,
        f.sbp_w1 AS triage_sbp,
        f.dbp_w1 AS triage_dbp,
        f.triage_pain,
        f.triage_acuity
    FROM {features_w1} f
),

revisit_info AS (
    -- ED 3-day revisit
    SELECT 
        b1.stay_id,
        CASE WHEN EXISTS (
            SELECT 1 FROM {base_ed_cohort} b2
            WHERE b2.subject_id = b1.subject_id
              AND b2.stay_id != b1.stay_id
              AND b2.ed_intime > b1.ed_outtime
              AND b2.ed_intime <= b1.ed_outtime + INTERVAL '3 days'
        ) THEN 1 ELSE 0 END AS outcome_ed_revisit_3d
    FROM {base_ed_cohort} b1
)

SELECT 
    b.subject_id,
    b.hadm_id,
    b.stay_id,
    b.intime,
    b.outtime,
    b.age,
    b.gender,
    b.race,
    b.arrival_transport,
    b.ed_los_hours,
    
    -- Outcomes (Xie-compatible)
    b.outcome_hospitalization,
    CASE WHEN icu.time_to_icu_hours <= 12 THEN 1 ELSE 0 END AS outcome_icu_transfer_12h,
    CASE WHEN det.stay_id IS NOT NULL THEN 1 ELSE 0 END AS outcome_critical,
    COALESCE(rev.outcome_ed_revisit_3d, 0) AS outcome_ed_revisit_3d,
    COALESCE(death.outcome_inhospital_mortality, 0) AS outcome_inhospital_mortality,
    
    -- Triage features
    tv.triage_temperature,
    tv.triage_heartrate,
    tv.triage_resprate,
    tv.triage_o2sat,
    tv.triage_sbp,
    tv.triage_dbp,
    tv.triage_pain,
    tv.triage_acuity,
    
    -- Time-to-event (for analysis)
    icu.time_to_icu_hours,
    det.time_to_deterioration_hours

FROM base b
LEFT JOIN icu_times icu ON b.stay_id = icu.stay_id
LEFT JOIN deterioration_events det ON b.stay_id = det.stay_id
LEFT JOIN death_events death ON b.stay_id = death.stay_id
LEFT JOIN triage_vitals tv ON b.stay_id = tv.stay_id
LEFT JOIN revisit_info rev ON b.stay_id = rev.stay_id

ORDER BY b.stay_id
"""


# ============================================================================
# EXECUTE
# ============================================================================

def main():
    print("=" * 70)
    print("GENERATING XIE-COMPATIBLE BENCHMARK DATASET FROM OUR PIPELINE")
    print("=" * 70)
    
    # Template variables
    params = {
        'base_ed_cohort': cfg['tables']['base_ed_cohort'],
        'event_log': cfg['tables']['event_log'],
        'features_w1': cfg['tables']['features_w1'],
    }
    
    # Render SQL
    query = SQL.format(**params)
    
    print("\nConnecting to database...")
    conn = get_conn(cfg)
    
    print("Executing query...")
    df = pd.read_sql(query, conn)
    conn.close()
    
    print(f"\nOK Retrieved {len(df):,} records")
    print(f"   Unique subjects: {df.subject_id.nunique():,}")
    print(f"   Unique stays: {df.stay_id.nunique():,}")
    
    # Report outcome prevalences
    print("\n" + "=" * 70)
    print("OUTCOME PREVALENCES (OUR PIPELINE)")
    print("=" * 70)
    
    outcomes = [
        'outcome_hospitalization',
        'outcome_icu_transfer_12h',
        'outcome_critical',
        'outcome_ed_revisit_3d',
        'outcome_inhospital_mortality'
    ]
    
    for oc in outcomes:
        n = df[oc].sum()
        pct = 100 * df[oc].mean()
        print(f"{oc:40s} {n:8.0f} ({pct:5.2f}%)")
    
    # Report feature availability
    print("\n" + "=" * 70)
    print("FEATURE AVAILABILITY")
    print("=" * 70)
    
    features = [
        'age', 'triage_temperature', 'triage_heartrate', 'triage_resprate',
        'triage_o2sat', 'triage_sbp', 'triage_dbp', 'triage_pain', 'triage_acuity'
    ]
    
    for f in features:
        missing_pct = 100 * df[f].isna().mean()
        print(f"{f:30s} {missing_pct:6.2f}% missing")
    
    # Save
    print(f"\nSaving to {OUTPUT}")
    df.to_csv(OUTPUT, index=False)
    
    print("\nOK Dataset generation complete!")
    print(f"   Output: {OUTPUT}")
    print(f"   Rows: {len(df):,}")
    print(f"   Columns: {len(df.columns)}")


if __name__ == "__main__":
    main()



