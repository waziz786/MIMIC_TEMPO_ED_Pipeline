"""
Pipeline Execution Guide and Validation
=========================================

This script demonstrates the updated pipeline workflow with hospitalization-level ICD outcomes.

PREREQUISITES:
1. PostgreSQL database running with MIMIC-IV loaded
2. Set PGPASSWORD environment variable or create .env file
3. All Python packages installed (see requirements.txt)

STEP 1: Full Pipeline Execution
================================
Run the complete pipeline to rebuild all tables:

    python -m src.main

This will execute:
- Build base ED cohort (tmp_base_ed_cohort)
- Extract and harmonize events (tmp_ed_event_log) with event_time_type column
- Create outcomes table (tmp_ed_outcomes) with hospitalization-level ICD outcomes
- Build feature tables (tmp_features_w1, tmp_features_w6, tmp_features_w24)
- Build ECG feature tables if available
- Run validation checks

STEP 2: Generate Sample Datasets
=================================
After the pipeline completes, materialize datasets:

Example A: Primary deterioration dataset (W6 features)
-------------------------------------------------------
python -m src.materialize_datasets \\
    --dataset ed_w6_det24_admitted \\
    --output-dir artifacts/datasets

Example B: Hospitalization-level cardiac outcomes
--------------------------------------------------
python -m src.materialize_datasets \\
    --dataset ed_w6_cardiac_arrest_admitted \\
    --output-dir artifacts/datasets

python -m src.materialize_datasets \\
    --dataset ed_w6_acs_hosp_admitted \\
    --output-dir artifacts/datasets

python -m src.materialize_datasets \\
    --dataset ed_w6_coronary_hosp_admitted \\
    --output-dir artifacts/datasets

Example C: Custom multi-outcome dataset
----------------------------------------
python -m src.generate_advanced_dataset \\
    --windows W6 W24 \\
    --outcomes deterioration_24h cardiac_arrest_hosp acs_hosp \\
    --cohort admitted \\
    --name custom_cardiac_analysis \\
    --reports

STEP 3: Validation Checks
==========================
Run comprehensive validation:

python -m src.validate

This validates:
- Schema existence and row counts
- Temporal consistency (only for timed events, event_time IS NOT NULL)
- Outcome rates within expected ranges
- Feature distributions

STEP 4: Quick Queries
=====================
After pipeline runs, verify ICD outcomes in database:

-- Check event log structure with new event_time_type column
SELECT event_type, event_time_type, COUNT(*), 
       COUNT(event_time) as with_timestamp
FROM tmp_ed_event_log 
GROUP BY event_type, event_time_type 
ORDER BY event_type;

-- Expected output:
-- ACS              | none  | counts with NULL event_time
-- CARDIAC_ARREST   | none  | counts with NULL event_time  
-- PCI/CABG         | none  | counts with NULL event_time
-- ICU_ADMIT        | exact | all have timestamps
-- PRESSOR_START    | exact | all have timestamps
-- VENT_START       | exact | all have timestamps
-- RRT_START        | exact | all have timestamps
-- DEATH            | exact | all have timestamps

-- Check outcomes table structure
SELECT column_name, data_type 
FROM information_schema.columns 
WHERE table_name = 'tmp_ed_outcomes'
ORDER BY ordinal_position;

-- Expected hospitalization-level outcomes:
-- cardiac_arrest_hosp (INTEGER)
-- acs_hosp (INTEGER)
-- revasc_hosp (INTEGER)
-- pci_hosp (INTEGER)
-- cabg_hosp (INTEGER)
-- coronary_event_hosp (INTEGER)

-- Check outcome rates
SELECT 
    COUNT(*) as total_visits,
    SUM(deterioration_24h) as det_24h,
    SUM(cardiac_arrest_hosp) as arrest_hosp,
    SUM(acs_hosp) as acs_hosp,
    SUM(coronary_event_hosp) as coronary_hosp,
    ROUND(100.0 * SUM(deterioration_24h) / COUNT(*), 2) as det_24h_pct,
    ROUND(100.0 * SUM(cardiac_arrest_hosp) / COUNT(*), 2) as arrest_pct,
    ROUND(100.0 * SUM(acs_hosp) / COUNT(*), 2) as acs_pct
FROM tmp_ed_outcomes;

KEY CHANGES IN THIS VERSION:
=============================
1. Event log now includes event_time_type column:
   - 'exact': Timestamp from clinical systems (ICU, pressors, vent, RRT, death)
   - 'date': Date-level precision (not currently used)
   - 'none': No timestamp available (ICD-based events)

2. ICD-based outcomes changed from time-windowed to hospitalization-level:
   - cardiac_arrest_24h → cardiac_arrest_hosp (no time window)
   - acs_72h → acs_hosp (no time window)
   - revasc_72h → revasc_hosp (no time window)
   - pci_72h → pci_hosp (no time window)
   - cabg_72h → cabg_hosp (no time window)
   - coronary_event_72h → coronary_event_hosp (no time window)

3. Deterioration composites NO LONGER include cardiac_arrest:
   - deterioration_24h = ICU + pressor + vent + RRT + death (within 24h)
   - deterioration_48h = ICU + pressor + vent + RRT + death (within 48h)
   - Cardiac arrest is now a separate hospitalization-level indicator

4. Temporal validation excludes NULL event_time records:
   - build_event_log.py filters: WHERE e.event_time IS NOT NULL
   - validate.py filters: WHERE e.event_time IS NOT NULL

CLI COMMANDS SUMMARY:
=====================
# Full pipeline
python -m src.main

# Single dataset
python -m src.materialize_datasets --dataset <name>

# Batch datasets
python -m src.materialize_datasets --batch config/datasets.yaml

# Custom multi-window dataset
python -m src.generate_advanced_dataset \\
    --windows W1 W6 W24 \\
    --outcomes death_24h icu_24h \\
    --cohort admitted

# Run validation only
python -m src.validate

# List available datasets
python -m src.materialize_datasets --list
"""

if __name__ == "__main__":
    print(__doc__)
    print("\n" + "="*80)
    print("VALIDATION CHECKLIST")
    print("="*80)
    print("\n✓ SQL files updated (14_event_acs.sql, 15_event_revasc.sql, 16_event_cardiac_arrest.sql, 20_outcomes_from_event_log.sql)")
    print("✓ Python source files updated (build_event_log.py, build_outcomes.py, make_datasets.py, validate.py)")
    print("✓ Documentation updated (COMPLETE_DOCUMENTATION.md, MANUSCRIPT.md)")
    print("✓ Config files updated (outcomes.yaml, datasets.yaml)")
    print("✓ Notebooks updated (04_DATASET_GENERATION.ipynb)")
    print("\n⚠ Database connection required to run pipeline")
    print("  Set PGPASSWORD environment variable or create .env file")
    print("\n📋 To proceed:")
    print("  1. Start PostgreSQL and ensure MIMIC-IV is loaded")
    print("  2. Set database password: $env:PGPASSWORD='your_password'")
    print("  3. Run: python -m src.main")
