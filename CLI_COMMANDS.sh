# CLI Commands Reference
# Updated: All outcomes are prediction-time-aligned or ICD-based.
# Arrival-anchored outcomes have been removed from the pipeline.

## Full Pipeline Execution
# Rebuild all tables from scratch
python -m src.main

## Dataset Materialization

### Single Predefined Dataset (uses config/datasets.yaml)
python -m src.materialize_datasets --dataset ed_w6_det24_all
python -m src.materialize_datasets --dataset ed_w1_icu24_all

### Batch Dataset Generation
python -m src.materialize_datasets --batch config/datasets.yaml

### List Available Datasets
python -m src.materialize_datasets --list

## Custom Advanced Datasets

### Multi-Window, Multi-Outcome
python -m src.generate_advanced_dataset \
    --windows W6 W24 \
    --outcomes death_24h_from_w6 death_48h_from_w24 death_hosp \
    --cohort admitted \
    --name multi_mortality \
    --reports

### Cardiac outcomes (ICD-based, hospitalization-level)
python -m src.generate_advanced_dataset \
    --windows W1 W6 W24 \
    --outcomes cardiac_arrest_hosp acs_hosp revasc_hosp \
    --cohort admitted \
    --name cardiac_events \
    --ecg \
    --reports

### Mixed timed (aligned) and ICD outcomes
python -m src.generate_advanced_dataset \
    --windows W6 \
    --outcomes deterioration_24h_from_w6 icu_24h_from_w6 cardiac_arrest_hosp acs_hosp \
    --cohort admitted \
    --name mixed_outcomes

### Early Warning (W1 features only, aligned outcomes)
python -m src.generate_advanced_dataset \
    --windows W1 \
    --outcomes deterioration_24h_from_w1 icu_24h_from_w1 \
    --cohort admitted \
    --name ultra_early_prediction

### Extended Window (W24, aligned outcomes)
python -m src.generate_advanced_dataset \
    --windows W24 \
    --outcomes deterioration_48h_from_w24 \
    --cohort admitted \
    --name extended_window

## Validation

### Full Validation Suite
python -m src.validate

### Test database connection
python -c "from src.db import check_connection; from src.utils import load_yaml; cfg = load_yaml('config/config.yaml'); print('OK' if check_connection(cfg) else 'FAILED')"

### Validate setup before running pipeline
python -m tests.validate_setup

### Run pytest suite
pytest tests/ -v

## Quick Database Queries (via psql)

### Check Event Log Structure
psql -h localhost -U postgres -d mimiciv -c "
SELECT event_type, COUNT(*),
       COUNT(event_time) as with_timestamp,
       COUNT(*) FILTER (WHERE event_time IS NULL) as without_timestamp
FROM tmp_ed_event_log
GROUP BY event_type
ORDER BY event_type;"

### Check Outcome Table Columns (should be aligned + ICD + event_by)
psql -h localhost -U postgres -d mimiciv -c "
SELECT column_name
FROM information_schema.columns
WHERE table_name = 'tmp_ed_outcomes'
ORDER BY ordinal_position;"

### Check Aligned Outcome Rates
psql -h localhost -U postgres -d mimiciv -c "
SELECT
    COUNT(*) AS total,
    ROUND(100.0 * AVG(deterioration_24h_from_w6), 2) AS det24_w6_pct,
    ROUND(100.0 * AVG(icu_24h_from_w6), 2)           AS icu24_w6_pct,
    ROUND(100.0 * AVG(death_24h_from_w6), 3)         AS death24_w6_pct,
    ROUND(100.0 * AVG(death_hosp), 2)                AS death_hosp_pct,
    ROUND(100.0 * AVG(cardiac_arrest_hosp), 2)       AS arrest_hosp_pct,
    ROUND(100.0 * AVG(acs_hosp), 2)                  AS acs_hosp_pct
FROM tmp_ed_outcomes;"

### Check Event-By Flag Monotonicity  (W1 <= W6 <= W24)
psql -h localhost -U postgres -d mimiciv -c "
SELECT
    SUM(CASE WHEN event_by_icu_w1 > event_by_icu_w6  THEN 1 ELSE 0 END) AS icu_w1_gt_w6,
    SUM(CASE WHEN event_by_icu_w6 > event_by_icu_w24 THEN 1 ELSE 0 END) AS icu_w6_gt_w24
FROM tmp_ed_outcomes;
-- Both should be 0"

## Environment Setup

### Set Database Password (Windows PowerShell)
$env:PGPASSWORD='your_password'
