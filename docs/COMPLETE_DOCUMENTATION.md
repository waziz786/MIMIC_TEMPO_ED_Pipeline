# MIMIC Deterioration Pipeline - Complete Documentation

**Last Updated:** Feb 20, 2026  
**Comprehensive Single-Document Reference**

---

## Master Table of Contents

### PART 1: PIPELINE OVERVIEW
1. [Executive Summary](#1-executive-summary)
2. [Pipeline Architecture](#2-pipeline-architecture)
3. [Data Flow](#3-data-flow)
4. [Source Data Requirements](#4-source-data-requirements)
5. [Database Tables](#5-database-tables)
6. [Feature Definitions](#6-feature-definitions)
7. [Outcome Definitions](#7-outcome-definitions)
8. [ECG Integration](#8-ecg-integration)
9. [Configuration Reference](#9-configuration-reference)
10. [Usage Examples](#10-usage-examples)
11. [API Reference](#11-api-reference)
12. [Validation Quality Assurance](#12-validation--quality-assurance)
13. [Troubleshooting](#13-troubleshooting)
14. [Appendices](#14-appendices)

### PART 2: VARIABLE DICTIONARY
15. [Identifier Variables](#part-2-variable-dictionary)
16. [Demographic Variables](#2-demographic-variables)
17. [Timestamp Variables](#3-timestamp-variables)
18. [Vital Signs Variables](#4-vital-signs-variables)
19. [Laboratory Variables](#5-laboratory-variables)
20. [ECG Variables](#6-ecg-variables)
21. [Outcome Variables](#7-outcome-variables)
22. [Derived Variables](#8-derived-variables)
23. [Missing Indicator Variables](#9-missing-indicator-variables)
24. [Event Log Variables](#10-event-log-variables)
25. [Variable Summary Statistics](#11-variable-summary-statistics)

### PART 3: SQL PIPELINE
26. [SQL Overview](#part-3-sql-pipeline-documentation)
27. [Base Cohort SQL](#2-00_base_ed_cohortsql)
28. [Event Extraction Scripts](#3-event-extraction-scripts-10-17)
29. [Outcomes SQL](#4-20_outcomes_from_event_logsql)
30. [Feature Extraction SQL](#5-feature-extraction-scripts-30-32)
31. [ECG Feature SQL](#6-ecg-feature-scripts-33-34)
32. [QA Checks SQL](#7-99_qa_checkssql)
33. [SQL Template Variables](#8-sql-template-variables)
34. [Query Optimization](#9-query-optimization-notes)

### PART 4: PYTHON MODULES
34. [Module Overview](#part-4-python-modules-documentation)
35. [Core Modules](#2-core-modules)
36. [Build Modules](#3-build-modules)
37. [ECG Modules](#4-ecg-modules)
38. [Dataset Modules](#5-dataset-modules)
39. [Validation Modules](#6-validation-modules)
40. [Utility Modules](#7-utility-modules)
41. [Class Reference](#8-class-reference)
42. [Function Reference](#9-function-reference)
43. [Error Handling](#10-error-handling)

### PART 5: USAGE EXAMPLES
44. [Quick Start Guide](#part-5-usage-examples-and-tutorials)
45. [Configuration Examples](#2-configuration-examples)
46. [Running Pipeline](#3-running-the-pipeline)
47. [Dataset Generation](#4-dataset-generation)
48. [Custom Queries](#5-custom-queries)
49. [Data Analysis Examples](#6-data-analysis-examples)
50. [Machine Learning Integration](#7-machine-learning-integration)
51. [Advanced Usage](#8-advanced-usage)
52. [Common Workflows](#9-common-workflows)
53. [Troubleshooting Examples](#10-troubleshooting-examples)

---

# PART 1: PIPELINE OVERVIEW

# Deterioration Pipeline - Comprehensive Documentation



---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Pipeline Architecture](#2-pipeline-architecture)
3. [Data Flow](#3-data-flow)
4. [Source Data Requirements](#4-source-data-requirements)
5. [Database Tables](#5-database-tables)
6. [Feature Definitions](#6-feature-definitions)
7. [Outcome Definitions](#7-outcome-definitions)
8. [ECG Integration](#8-ecg-integration)
9. [Configuration Reference](#9-configuration-reference)
10. [Usage Examples](#10-usage-examples)
11. [API Reference](#11-api-reference)
12. [Validation & Quality Assurance](#12-validation--quality-assurance)
13. [Troubleshooting](#13-troubleshooting)
14. [Appendices](#14-appendices)

---

# 1. Executive Summary

## Purpose

The MIMIC Deterioration Pipeline extracts structured features from MIMIC-IV Emergency Department data to predict adverse outcomes. It creates analysis-ready datasets for machine learning research on early warning systems.

## Key Capabilities

- **Cohort Construction:** 424,385 adult ED visits from MIMIC-IV (age 18–110, alive at arrival)
- **Feature Windows:** 1h, 6h, and 24h from ED arrival
- **ECG Integration:** Machine-derived ECG features linked to ED visits
- **Multiple Outcomes:** Deterioration, ward failure, coronary events, mortality
- **Flexible Datasets:** Modular generation with customizable filters

## Quick Statistics

| Metric | Value |
|--------|-------|
| Total ED Visits | 424,385 |
| Unique Patients | 205,080 |
| Hospital Admissions | 202,184 |
| Events Captured | 81,930 |
| ECG Coverage (6h) | 34.3% |
| Deterioration Rate (24h from W6) | 4.05% |
| QA Checks | 8/8 passing |

---

# 2. Pipeline Architecture

## 2.1 Directory Structure

```
Deterioration_Pipeline/          [ROOT DIRECTORY]
│
├── 📋 docs/ (Documentation - 13 files)
│   ├── README.md                        ⭐ Complete documentation
│   ├── QUICKSTART.md                    ⭐ Setup instructions  
│   ├── IMPLEMENTATION_SUMMARY.md        ⭐ Technical details
│   ├── LICENSE                          MIT License
│   ├── PROJECT_COMPLETE.md              Project completion summary
│   ├── COMPLETE_DOCUMENTATION.md        Full documentation
│   ├── COMPLETE_DOCUMENTATION.html      HTML version
│   ├── COMPREHENSIVE_PIPELINE_DOCUMENTATION.md
│   ├── PIPELINE_GUIDE.md                Pipeline usage guide
│   ├── PIPELINE_GUIDE.html              HTML version
│   ├── PYTHON_MODULES_DOCUMENTATION.md  Python module docs
│   ├── SQL_PIPELINE_DOCUMENTATION.md    SQL script docs
│   ├── USAGE_EXAMPLES_TUTORIALS.md      Usage examples
│   └── VARIABLE_DICTIONARY.md           ⭐ Variable definitions
│
├── ⚙️ Root Configuration Files
│   ├── .env.example                     Password template
│   ├── .gitignore                       Git exclusions
│   ├── requirements.txt                 Python dependencies
│   ├── pytest.ini                       Test configuration
│
├── 📂 config/ (YAML Configuration - 4 files)
│   ├── config.yaml                      ⭐ Database & pipeline settings
│   ├── outcomes.yaml                    ⭐ 15+ outcome definitions
│   ├── datasets.yaml                    Dataset configurations
│   └── feature_catalog.yaml             ⭐ Feature organization
│
├── 🗃️ sql/ (SQL Scripts - 15 files)
│   │
│   ├── Base Cohort
│   │   └── 00_base_ed_cohort.sql        ⭐ Base cohort (adult ED visits)
│   │
│   ├── Event Extractors (8 files)
│   │   ├── 10_event_icu_admit.sql       ICU admission
│   │   ├── 11_event_pressors.sql        Vasopressor start
│   │   ├── 12_event_ventilation.sql     Mechanical ventilation
│   │   ├── 13_event_rrt.sql             Renal replacement therapy
│   │   ├── 14_event_acs.sql             Acute coronary syndrome
│   │   ├── 15_event_revasc.sql          PCI/CABG procedures
│   │   ├── 16_event_cardiac_arrest.sql  Cardiac arrest
│   │   └── 17_event_death.sql           Mortality
│   │
│   ├── Outcomes
│   │   └── 20_outcomes_from_event_log.sql ⭐ 15+ outcome indicators
│   │
│   └── Feature Windows (5 files)
│       ├── 30_features_w1.sql           W1 features (1 hour)
│       ├── 31_features_w6.sql           W6 features (6 hours)
│       ├── 32_features_w24.sql          W24 features (24hours)
│       ├── 33_ecg_features_w1.sql       ECG W1 features
│       └── 34_ecg_features_w6.sql       ECG W6 features
│
├── 🐍 src/ (Python Modules - 12 files)
│   ├── __init__.py                      Package initialization
│   ├── db.py                            ⭐ Database utilities
│   ├── utils.py                         ⭐ Helper functions
│   ├── build_base.py                    ⭐ Base cohort builder
│   ├── build_event_log.py               ⭐ Event log builder
│   ├── build_outcomes.py                ⭐ Outcomes builder
│   ├── build_features.py                ⭐ Features builder
│   ├── build_ecg_features.py            ⭐ ECG features builder
│   ├── materialize_datasets.py          ⭐ Dataset generator
│   ├── make_datasets.py                 Dataset utilities
│   ├── validate.py                      ⭐ Validation suite
│   └── main.py                          ⭐ Main orchestration
│
├── 📓 notebooks/ (Jupyter Notebooks - 5 files)
│   ├── 01_DATABASE_SETUP.ipynb          ⭐ Database setup & data loading
│   ├── 02_GETTING_STARTED.ipynb         ⭐ Configuration & verification
│   ├── 03_PIPELINE_EXECUTION.ipynb      ⭐ Run pipeline & build tables
│   ├── 04_DATASET_GENERATION.ipynb      ⭐ Generate analysis datasets
│   └── 05_DATA_ANALYSIS.ipynb           ⭐ Exploratory data analysis
│
├── 🧪 tests/ (Test Modules - 5 files)
│   ├── __init__.py                      Package initialization
│   ├── test_db.py                       Database tests
│   ├── test_pipeline.py                 Pipeline tests
│   ├── test_validation.py               Validation tests
│   └── validate_setup.py                Setup validation
│
├── 🔧 utilities/ (Utility Scripts - 2 files)
│   ├── validate_setup.py                ⭐ Pre-flight validation
│   └── setup_wizard.py                  ⭐ Interactive setup
│
└── 📊 artifacts/ (Generated Outputs)
    ├── datasets/                        📁 Output CSV files
    │   ├── ed_w1_icu24.csv (example)
    │
    └── logs/                            📁 Pipeline logs (generated)
⭐ = Critical component
📁 = Created during execution
```

## 2.2 Technology Stack

| Component | Technology |
|-----------|------------|
| Database | PostgreSQL 15+ |
| Query Language | SQL with Jinja2 templating |
| Data Processing | Python 3.10+, pandas, psycopg2 |
| Testing | pytest |
| Configuration | YAML file |

## 2.3 Processing Pipeline

```
┌──────────────────────────────────────────────────────────────────┐
│                    MIMIC-IV PostgreSQL Database                  │
├──────────────────────────────────────────────────────────────────┤
│  mimiciv_ed.edstays    mimiciv_hosp.patients    mimiciv_icu.*    │
│  mimiciv_ed.vitalsign  mimiciv_hosp.labevents   mimiciv_hosp.*   │
└─────────────────────────────────┬────────────────────────────────┘
                                  │
                    ┌─────────────▼─────────────┐
                    │   Step 1: Base Cohort     │
                    │   (00_base_ed_cohort.sql) │
                    └─────────────┬─────────────┘
                                  │
                    ┌─────────────▼─────────────┐
                    │   Step 2: Event Log       │
                    │   (10-17_event_*.sql)     │
                    └─────────────┬─────────────┘
                                  │
                    ┌─────────────▼─────────────┐
                    │   Step 3: Outcomes        │
                    │   (20_outcomes.sql)       │
                    └─────────────┬─────────────┘
                                  │
          ┌───────────────────────┼───────────────────────┐
          │                       │                       │
┌─────────▼─────────┐   ┌────────▼────────┐   ┌─────────▼─────────┐
│  Step 4: Features │   │ Features W6     │   │ Features W24      │
│  W1 (1 hour)      │   │ (6 hours)       │   │ (24 hours)        │
└─────────┬─────────┘   └────────┬────────┘   └─────────┬─────────┘
          │                      │                      │
          └──────────────────────┼──────────────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │   Step 4.5: ECG Tables  │
                    │   (Pre-loaded via psql) │
                    │   mimiciv_ecg schema    │
                    └────────────┬────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │   Step 4.6: ECG Features│
                    │   (33-34_ecg_*.sql)     │
                    └────────────┬────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │   Step 5: Materialize   │
                    │   (Join → CSV Export)   │
                    └────────────┬────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │   Output: CSV Datasets  │
                    │   (artifacts/datasets/) │
                    └─────────────────────────┘
```

---

# 3. Data Flow

## 3.1 Step-by-Step Processing

### Step 1: Base Cohort (`tmp_base_ed_cohort`)

**Source:** `mimiciv_ed.edstays` + `mimiciv_hosp.patients`

**Process:**
1. Join ED stays with patient demographics
2. Calculate age at ED visit
3. Filter adults (age ≥ 18)
4. Validate timestamps (outtime > intime)
5. Flag hospital admissions

**Output:** One row per ED stay with core identifiers

### Step 2: Event Log (`tmp_ed_event_log`)

**Source:** Multiple MIMIC tables (ICU transfers, procedures, medications)

**Events Extracted:**
| Event Type | Source Table | Definition |
|------------|--------------|------------|
| ICU_ADMIT | mimiciv_icu.icustays | First ICU admission time |
| PRESSOR_START | mimiciv_icu.inputevents | Norepinephrine, dopamine, etc. |
| VENT_START | mimiciv_icu.procedureevents | Mechanical ventilation |
| RRT_START | mimiciv_icu.procedureevents | Dialysis/CRRT |
| CARDIAC_ARREST | mimiciv_hosp.diagnoses_icd | ICD codes for arrest (hosp-level, NULL timing) |
| DEATH | mimiciv_hosp.patients | Date of death |
| ACS | mimiciv_hosp.diagnoses_icd | Acute coronary syndrome (hosp-level, NULL timing) |
| PCI | mimiciv_hosp.procedures_icd | Percutaneous intervention (hosp-level, NULL timing) |
| CABG | mimiciv_hosp.procedures_icd | Coronary bypass (hosp-level, NULL timing) |

### Step 3: Outcomes (`tmp_ed_outcomes`)

**Process:** Aggregates events into binary outcome flags with time windows

**Time Windows:**
- **24 hours:** Early deterioration (timed events)
- **48 hours:** Extended deterioration (timed events)
- **Hospitalization-level:** ICD-based outcomes (cardiac arrest, ACS, revascularization) — no time window

### Step 4: Features (`tmp_features_w*`)

**Windows:** W1 (1h), W6 (6h), W24 (24h)
"h means hour 

**Feature Types:**
- Vital signs summaries (min, max, mean, std)
- Laboratory values (first within window)
- Derived metrics (variability, ranges)
- Missing indicators

### Step 4.5-4.6: ECG Features (`tmp_ecg_features_w*`)

**Process:**
1. Load ECG CSVs into staging tables
2. Match ECGs to ED stays by subject_id + time
3. Extract machine measurements for first ECG in respective window

---

# 4. Source Data Requirements

## 4.1 Required MIMIC-IV Modules

| Module | Schema | Tables Used |
|--------|--------|-------------|
| MIMIC-IV ED | mimiciv_ed | edstays, vitalsign, triage, medrecon, pyxis |
| MIMIC-IV Hosp | mimiciv_hosp | patients, admissions, diagnoses_icd, procedures_icd, labevents, d_labitems |
| MIMIC-IV ICU | mimiciv_icu | icustays, inputevents, procedureevents, chartevents |
| MIMIC-IV ECG | (files) | record_list.csv, machine_measurements.csv |

## 4.2 ECG Data Loading

**Pre-requisite:** ECG data must be loaded into PostgreSQL **before** running the pipeline.

**Loading Method:** Use PostgreSQL `\COPY` command (see `QUICK_START_DATA_LOADING.md`)

**Required Tables in Database:**
1. `mimiciv_ecg.record_list`
   - Columns: `subject_id`, `study_id`, `ecg_time`, `file_name`, `path`
   - Links ECG studies to patients

2. `mimiciv_ecg.machine_measurements`
   - Columns: `study_id`, `subject_id`, `ecg_time`, `rr_interval`, `qrs_onset`, `qrs_end`, `p_onset`, `p_end`, `t_end`, `p_axis`, `qrs_axis`, `t_axis`, and additional report columns
   - Contains machine-derived ECG measurements

**Source CSV Files (loaded during setup):**
- `record_list.csv` (~800K records)
- `machine_measurements.csv` (~800K records)

## 4.3 Database Setup

```sql
-- Required schemas
CREATE SCHEMA IF NOT EXISTS mimiciv_ed;
CREATE SCHEMA IF NOT EXISTS mimiciv_hosp;
CREATE SCHEMA IF NOT EXISTS mimiciv_icu;
```

---

# 5. Database Tables

## 5.1 Table Overview

| Table Name | Rows | Description |
|------------|------|-------------|
| tmp_base_ed_cohort | 424,952 | Base ED visit cohort |
| tmp_ed_event_log | 82,707 | Clinical events |
| tmp_ed_outcomes | 424,952 | Outcome labels |
| tmp_features_w1 | 424,952 | 1-hour features |
| tmp_features_w6 | 424,952 | 6-hour features |
| tmp_features_w24 | 424,952 | 24-hour features |
| mimiciv_ecg.record_list | 800,035 | ECG records (pre-loaded) |
| mimiciv_ecg.machine_measurements | 800,035 | ECG measurements (pre-loaded) |
| tmp_ecg_features_w1 | 424,952 | ECG features (1h) |
| tmp_ecg_features_w6 | 424,952 | ECG features (6h) |

## 5.2 Base Cohort Schema

**Table:** `tmp_base_ed_cohort`

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| stay_id | INTEGER | **Primary Key** - Unique ED stay identifier | 30001234 |
| subject_id | INTEGER | Patient identifier | 10000032 |
| hadm_id | INTEGER | Hospital admission ID (NULL if not admitted) | 20001234 |
| ed_intime | TIMESTAMP | ED arrival time | 2150-01-15 14:30:00 |
| ed_outtime | TIMESTAMP | ED departure time | 2150-01-15 22:45:00 |
| anchor_age | INTEGER | Age at anchor year | 65 |
| anchor_year | INTEGER | Reference year for age | 2150 |
| gender | CHAR(1) | Patient gender (M/F) | M |
| dod | DATE | Date of death (NULL if alive) | 2150-03-20 |
| age_at_ed | FLOAT | Calculated age at ED visit | 67.3 |
| ed_los_hours | FLOAT | ED length of stay (hours) | 8.25 |
| was_admitted | INTEGER | Hospital admission flag (0/1) | 1 |

## 5.3 Event Log Schema

**Table:** `tmp_ed_event_log`

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| stay_id | INTEGER | ED stay identifier | 30001234 |
| event_type | VARCHAR | Event category | ICU_ADMIT |
| event_time | TIMESTAMP | Event occurrence time (NULL for ICD events) | 2150-01-16 02:30:00 |
| event_source | VARCHAR | Source table | icustays |
| event_detail | TEXT | Additional details (optional) | MICU |
| event_time_type | TEXT | Time precision: 'exact', 'day', or 'none' | exact |

> **Note (v2.1):** The `event_time_type` column was added to distinguish timed events (`'exact'`), date-only events (`'day'` — e.g., death from `dod`), and ICD-coded events with no timestamp (`'none'`). All 8 event extraction scripts now emit this column.
- `ACS` - Acute coronary syndrome diagnosis
- `PCI` - Percutaneous coronary intervention
- `CABG` - Coronary artery bypass grafting



## 5.4 Outcomes Schema

**Table:** `tmp_ed_outcomes`

| Column | Type | Description |
|--------|------|-------------|
| stay_id | INTEGER | Primary key |
| subject_id | INTEGER | Patient ID |
| hadm_id | INTEGER | Admission ID |
| ed_intime | TIMESTAMP | ED arrival |
| ed_outtime | TIMESTAMP | ED departure |
| | | **Hospitalization-Level Outcomes** |
| **death_hosp** | INTEGER | In-hospital death |
| **cardiac_arrest_hosp** | INTEGER | Cardiac arrest during hospitalization (ICD) |
| **acs_hosp** | INTEGER | ACS during hospitalization (ICD) |
| **revasc_hosp** | INTEGER | Revascularization during hospitalization (ICD) |
| **pci_hosp** | INTEGER | PCI during hospitalization (ICD) |
| **cabg_hosp** | INTEGER | CABG during hospitalization (ICD) |
| **coronary_event_hosp** | INTEGER | **Composite** - ACS or revascularization |
| | | **Time-to-Event** |
| time_to_icu | FLOAT | Hours to first ICU admission |
| time_to_death | FLOAT | Hours to death |
| time_to_deterioration | FLOAT | Hours to first critical event |
| | | **Prediction-Time-Aligned Outcomes** |
| **{event}\_{H}h\_from\_{W}** | INTEGER | Aligned outcome (see below) |
| | | **Event-By Flags** |
| **event_by\_{event}\_{W}** | INTEGER | Did event occur within window? |
| | | **Composite Aligned Outcomes** |
| **deterioration\_{H}h\_from\_{W}** | INTEGER | Composite deterioration |

### Prediction-Time-Aligned Naming Convention

All timed outcomes follow the format `{event}_{horizon}h_from_{window}`:

- **event**: `icu`, `pressor`, `vent`, `rrt`, `death`
- **horizon**: `24`, `48`, `72` (hours after prediction time)
- **window**: `w1`, `w6`, `w24` (prediction time = end of feature window)

**Rule:** `hours_from_ed > P AND hours_from_ed <= P + H` where P = prediction time (1, 6, or 24h)

**Examples:** `icu_24h_from_w6` = ICU admission between 6-30h from ED arrival; `death_48h_from_w1` = death between 1-49h from ED arrival.

Total aligned outcome columns: 33 individual + 6 composite = **39 prediction-aligned labels**

### Event-By Flags

Format: `event_by_{event}_{window}` — binary flag indicating the event **already occurred** within the feature window.

**Rule:** `hours_from_ed <= P`

- 6 event types × 3 windows = **18 event-by flags**
- Events: `icu`, `pressor`, `vent`, `rrt`, `death`, `deterioration`
- Auto-included when aligned outcomes are selected during dataset generation

---

# 6. Feature Definitions

## 6.1 Feature Windows

| Window | Time Range | Use Case | Special Features |
|--------|------------|----------|------------------|
| W1 | 0-1 hour | Ultra-early prediction | Basic vitals only |
| W6 | 0-6 hours | Early prediction (primary) | + Lab values, ED process, history |
| W24 | 0-24 hours | Extended observation | + Extended labs (glucose, BUN, bilirubin, INR), max values, derived ratios |

> **Note (v2.1):** All feature windows are clamped with `LEAST(ed_outtime, ed_intime + INTERVAL 'Xh')` to prevent pulling data collected after the patient left the ED. This ensures features reflect only data available during the actual ED encounter.

## 6.2 Vital Signs Features

**Source:** `mimiciv_ed.vitalsign`

| Feature | Description | Units | Normal Range |
|---------|-------------|-------|--------------|
| sbp_min_Xh | Minimum systolic BP | mmHg | 90-120 |
| sbp_max_Xh | Maximum systolic BP | mmHg | 90-140 |
| sbp_mean_Xh | Mean systolic BP | mmHg | 100-130 |
| sbp_std_Xh | Std dev of systolic BP | mmHg | 0-15 |
| dbp_min_Xh | Minimum diastolic BP | mmHg | 60-80 |
| hr_min_Xh | Minimum heart rate | bpm | 60-100 |
| hr_max_Xh | Maximum heart rate | bpm | 60-100 |
| hr_mean_Xh | Mean heart rate | bpm | 60-80 |
| hr_std_Xh | Std dev of heart rate | bpm | 0-10 |
| rr_max_Xh | Maximum respiratory rate | /min | 12-20 |
| rr_mean_Xh | Mean respiratory rate | /min | 14-18 |
| spo2_min_Xh | Minimum SpO2 | % | 95-100 |
| spo2_mean_Xh | Mean SpO2 | % | 96-99 |
| temp_max_Xh | Maximum temperature | °F | 97-99 |
| temp_min_Xh | Minimum temperature | °F | 97-99 |
| n_vital_measurements | Count of measurements | count | - |

## 6.3 Laboratory Features

**Source:** `mimiciv_hosp.labevents`

| Feature | Description | Units | Normal Range |
|---------|-------------|-------|--------------|
| time_to_first_lab_hours | Hours from ED arrival to first lab result | hours | - |

**Lab Values:**

| Feature | Description | Units | Normal Range |
| lactate_first_Xh | First lactate value | mmol/L | 0.5-2.0 |
| troponin_first_Xh | First troponin value | ng/mL | <0.04 |
| creatinine_first_Xh | First creatinine value | mg/dL | 0.7-1.3 |
| potassium_first_Xh | First potassium value | mEq/L | 3.5-5.0 |
| sodium_first_Xh | First sodium value | mEq/L | 136-145 |
| bicarbonate_first_Xh | First bicarbonate value | mEq/L | 22-28 |
| wbc_first_Xh | First WBC count | K/µL | 4.5-11.0 |
| hemoglobin_first_Xh | First hemoglobin | g/dL | 12-16 |
| platelet_first_Xh | First platelet count | K/µL | 150-400 |

**Extended Lab Panel (W24 only):**

| Feature | Description | Units | Normal Range |
|---------|-------------|-------|---------------|
| glucose_first_24h | First glucose value | mg/dL | 70-100 |
| bun_first_24h | First BUN (blood urea nitrogen) | mg/dL | 7-20 |
| bilirubin_first_24h | First bilirubin | mg/dL | 0.3-1.2 |
| inr_first_24h | First INR (international normalized ratio) | ratio | 0.8-1.2 |

**Lab Max Values (W24 only):**

| Feature | Description | Units | Use Case |
|---------|-------------|-------|----------|
| lactate_max_24h | Maximum lactate in 24h | mmol/L | Track worsening |
| creatinine_max_24h | Maximum creatinine in 24h | mg/dL | Track kidney function |

## 6.3.5 ED Process Metrics (W6,W12)

| Feature | Description | Units | Source |
|---------|-------------|-------|--------|
| time_to_first_lab_hours | Time from ED arrival to first lab result | hours | labevents |
| time_to_first_med_hours | Time from ED arrival to first medication | hours | pyxis |
| n_vitalsign_measurements_6h | Count of vital sign measurements in 6h | count | vitalsign |

## 6.3.6 Prior History Variables (W6,W24)

| Feature | Description | Units | Lookback |
|---------|-------------|-------|----------|
| prev_admits_1yr | Count of prior hospital admissions | count | 1 year |
| prev_ed_visits_1yr | Count of prior ED visits | count | 1 year |

## 6.3.7 Chief Complaint (W6+)

| Feature | Description | Type | Source |
|---------|-------------|------|--------|
| chiefcomplaint | Patient's chief complaint at triage | TEXT | triage |

## 6.4 Derived Features

**Vital Signs (All Windows):**

| Feature | Formula | Description |
|---------|---------|-------------|
| sbp_cv_Xh | sbp_std / sbp_mean | Blood pressure variability |
| hr_range_Xh | hr_max - hr_min | Heart rate range |
| shock_index | hr_mean / sbp_mean | Shock index (computed in modeling) |

## 6.5 Complete Variable Listings by Feature Basket

### W1 (First Hour) - Ultra-Early Prediction

**Database Table:** `tmp_features_w1`

**Total Variables:** 19

**Demographics (3):**
- `age_at_ed` - Patient age at ED arrival
- `gender` - Biological sex (M/F)
- `arrival_transport` - Mode of arrival (ambulance, walk-in, etc.)

**Vital Signs - Triage & First Charted (6):**
- `temp_w1` - Temperature (°F)
- `hr_w1` - Heart rate (bpm)
- `rr_w1` - Respiratory rate (/min)
- `spo2_w1` - Oxygen saturation (%)
- `sbp_w1` - Systolic blood pressure (mmHg)
- `dbp_w1` - Diastolic blood pressure (mmHg)

**Triage-Specific (2):**
- `triage_pain` - Pain score at triage
- `triage_acuity` - Acuity level at triage (1-5)

**Clinical Notes (1):**
- `chiefcomplaint` - Chief complaint (free text)

**Derived Features (3):**
- `shock_index_w1` - Heart rate / Systolic BP
- `map_w1` - Mean arterial pressure = (SBP + 2*DBP)/3

**Missing Indicators (3):**
- `missing_temp_w1` - 1 if temperature is NULL
- `missing_hr_w1` - 1 if heart rate is NULL
- `missing_sbp_w1` - 1 if systolic BP is NULL

**ECG Features - Optional [if `include_ecg=True`] (11):**
- `ecg_study_id_w1` - Study ID of first ECG
- `ecg_time_w1` - Timestamp of ECG
- `ecg_hours_from_ed_w1` - Hours from ED arrival to ECG
- `ecg_hr_w1` - Heart rate derived from RR interval (bpm)
- `ecg_rr_interval_w1` - RR interval (ms)
- `ecg_qrs_dur_w1` - QRS duration (ms)
- `ecg_pr_w1` - PR interval (ms)
- `ecg_qt_w1` - QT interval (ms)
- `ecg_qrs_axis_w1` - QRS axis (degrees)
- `ecg_p_axis_w1` - P wave axis (degrees)
- `ecg_t_axis_w1` - T wave axis (degrees)
- `missing_ecg_w1` - 1 if no ECG in window, else 0

> **Note:** ECG features are extracted from machine measurements if an ECG was recorded within the first hour. Coverage is typically 25-35% depending on ED workflow patterns. Raw timing measurements (p_onset, p_end, qrs_onset, qrs_end, t_end) are also available but not listed for brevity.

---

### W6 (First 6 Hours) - Early Prediction (Primary Window)

**Database Table:** `tmp_features_w6`

**Total Variables:** 40

**Demographics (4):**
- `age_at_ed` - Patient age at ED arrival
- `gender` - Biological sex
- `arrival_transport` - Mode of arrival
- `race` - Race/ethnicity

**Vital Signs Summary (16):**
- `sbp_min_6h`, `sbp_max_6h`, `sbp_mean_6h`, `sbp_std_6h` - Systolic BP stats
- `dbp_min_6h` - Minimum diastolic BP
- `hr_min_6h`, `hr_max_6h`, `hr_mean_6h`, `hr_std_6h` - Heart rate stats
- `rr_max_6h`, `rr_mean_6h` - Respiratory rate stats
- `spo2_min_6h`, `spo2_mean_6h` - SpO2 stats
- `temp_max_6h`, `temp_min_6h` - Temperature extremes
- `n_vitalsign_measurements_6h` - Count of vital sign measurements

**Laboratory Values - First Result (10):**
- `lactate_first_6h` - First lactate (mmol/L)
- `troponin_first_6h` - First troponin (ng/mL)
- `is_hs_troponin_6h` - High-sensitivity troponin flag
- `creatinine_first_6h` - First creatinine (mg/dL)
- `potassium_first_6h` - First potassium (mEq/L)
- `sodium_first_6h` - First sodium (mEq/L)
- `bicarbonate_first_6h` - First bicarbonate (mEq/L)
- `wbc_first_6h` - First WBC count (K/µL)
- `hemoglobin_first_6h` - First hemoglobin (g/dL)
- `platelet_first_6h` - First platelet count (K/µL)

**ED Process Metrics (3):**
- `ed_los_hours` - ED length of stay (hours)
- `time_to_first_lab_hours` - Time from ED arrival to first lab
- `time_to_first_med_hours` - Time from ED arrival to first medication

**Prior History (2):**
- `prev_admits_1yr` - Hospital admissions in past 1 year
- `prev_ed_visits_1yr` - ED visits in past 1 year

**Clinical Notes (1):**
- `chiefcomplaint` - Chief complaint (free text)

**Derived Features (2):**
- `sbp_cv_6h` - Systolic BP coefficient of variation
- `hr_range_6h` - Heart rate range (max - min)

**Missing Indicators (2):**
- `missing_lactate_6h` - 1 if lactate is NULL
- `missing_troponin_6h` - 1 if troponin is NULL

**ECG Features - Optional [if `include_ecg=True`] (11):**
- `ecg_study_id_w6` - Study ID of first ECG
- `ecg_time_w6` - Timestamp of ECG
- `ecg_hours_from_ed_w6` - Hours from ED arrival to ECG
- `ecg_hr_w6` - Heart rate derived from RR interval (bpm)
- `ecg_rr_interval_w6` - RR interval (ms)
- `ecg_qrs_dur_w6` - QRS duration (ms)
- `ecg_pr_w6` - PR interval (ms)
- `ecg_qt_w6` - QT interval (ms)
- `ecg_qrs_axis_w6` - QRS axis (degrees)
- `ecg_p_axis_w6` - P wave axis (degrees)
- `ecg_t_axis_w6` - T wave axis (degrees)
- `missing_ecg_w6` - 1 if no ECG in window, else 0

> **Note:** ECG features are extracted from machine measurements if an ECG was recorded within the first 6 hours. Coverage is typically 33-40% in the 6-hour window. Raw timing measurements (p_onset, p_end, qrs_onset, qrs_end, t_end) are also available but not listed for brevity. W6 is the recommended window for ECG integration due to higher coverage vs. W1.

---

### W24 (First 24 Hours) - Extended Observation

**Database Table:** `tmp_features_w24`

**Total Variables:** 41

**Demographics (4):**
- `age_at_ed` - Patient age at ED arrival
- `gender` - Biological sex
- `arrival_transport` - Mode of arrival
- `race` - Race/ethnicity

**Vital Signs Summary (13):**
- `sbp_min_24h`, `sbp_max_24h`, `sbp_mean_24h`, `sbp_std_24h` - Systolic BP stats
- `hr_min_24h`, `hr_max_24h`, `hr_mean_24h` - Heart rate stats
- `rr_max_24h`, `rr_mean_24h` - Respiratory rate stats
- `spo2_min_24h`, `spo2_mean_24h` - SpO2 stats
- `temp_max_24h` - Maximum temperature
- `n_vitalsign_measurements_24h` - Count of vital sign measurements

**Laboratory Values - First Result (14):**
- `lactate_first_24h` - First lactate (mmol/L)
- `troponin_first_24h` - First troponin (ng/mL)
- `is_hs_troponin_24h` - High-sensitivity troponin flag
- `creatinine_first_24h` - First creatinine (mg/dL)
- `potassium_first_24h` - First potassium (mEq/L)
- `sodium_first_24h` - First sodium (mEq/L)
- `bicarbonate_first_24h` - First bicarbonate (mEq/L)
- `wbc_first_24h` - First WBC count (K/µL)
- `hemoglobin_first_24h` - First hemoglobin (g/dL)
- `platelet_first_24h` - First platelet count (K/µL)
- `glucose_first_24h` - First glucose (mg/dL) **[W24 only]**
- `bun_first_24h` - First BUN (mg/dL) **[W24 only]**
- `bilirubin_first_24h` - First bilirubin (mg/dL) **[W24 only]**
- `inr_first_24h` - First INR **[W24 only]**

**Laboratory Values - Maximum Values (2) [W24 only]:**
- `lactate_max_24h` - Maximum lactate in 24h
- `creatinine_max_24h` - Maximum creatinine in 24h

**ED Process Metrics (3):**
- `ed_los_hours` - ED length of stay (hours)
- `time_to_first_lab_hours` - Time from ED arrival to first lab
- `time_to_first_med_hours` - Time from ED arrival to first medication

**Prior History (2):**
- `prev_admits_1yr` - Hospital admissions in past 1 year
- `prev_ed_visits_1yr` - ED visits in past 1 year

**Clinical Notes (1):**
- `chiefcomplaint` - Chief complaint (free text)

**Derived Features (3) [W24 only]:**
- `sbp_cv_24h` - Systolic BP coefficient of variation
- `bun_creatinine_ratio` - BUN / Creatinine ratio
- `lactate_delta_24h` - Change in lactate (max - first)

**Missing Indicators (Up to 21):**
- Generated for features with >10% missingness
- Examples: `missing_lactate_24h`, `missing_troponin_24h`, `missing_glucose_24h`, etc.

---

## Comparison Summary: Variables Across Baskets

| Feature Category | W1 | W6 | W24 |
|------------------|:--:|:--:|:---:|
| **Demographics** | ✓ (3) | ✓ (4) | ✓ (4) |
| **Vitals (mean/min/max)** | ✓ (6 raw) | ✓ (16) | ✓ (13) |
| **Labs (basic panel)** | ✗ | ✓ (10) | ✓ (10) |
| **Labs (extended panel)** | ✗ | ✗ | ✓ (4) |
| **Labs (max values)** | ✗ | ✗ | ✓ (2) |
| **ED Process Metrics** | ✗ | ✓ (3) | ✓ (3) |
| **Prior History** | ✗ | ✓ (2) | ✓ (2) |
| **Triage-Specific** | ✓ (2) | ✗ | ✗ |
| **Clinical Notes** | ✓ (1) | ✓ (1) | ✓ (1) |
| **Derived Features** | ✓ (3) | ✓ (2) | ✓ (3) |
| **ECG Features (optional)** | ✓ (12)* | ✓ (12)* | ✗ |
| **Missing Indicators** | ✓ (3+1)† | ✓ (2+1)† | ✓ (21) |
| **TOTAL** | **19-31** | **40-52** | **41** |

**Notes:**
- *ECG features are optional and only included if `include_ecg=True` in dataset configuration
- †Missing indicators for ECG include `missing_ecg_w1`/`missing_ecg_w6` when ECG features are enabled
- W6 is the recommended window for ECG integration (higher coverage: 33-40% vs W1: 25-35%)

---

## 6.6 Missing Indicators

For each feature with >10% missingness (configurable), a binary indicator is created:

| Indicator | Meaning |
|-----------|---------|
| missing_lactate_6h | 1 if lactate is NULL, else 0 |
| missing_troponin_6h | 1 if troponin is NULL, else 0 |
| missing_ecg_w6 | 1 if no ECG in window, else 0 |

**Lab-Derived Features (W24 only):**

| Feature | Formula | Description | Clinical Significance |
|---------|---------|-------------|----------------------|
| bun_creatinine_ratio | bun_first_24h / creatinine_first_24h | BUN-to-creatinine ratio | >20 suggests pre-renal azotemia (dehydration, low perfusion) |
| lactate_delta_24h | lactate_max_24h - lactate_first_24h | Change in lactate over 24h | Positive = worsening, Negative = improving |

## 6.7 Missing Indicators

For each feature with >50% missingness (default), a binary indicator is created:

| Indicator | Meaning |
|-----------|---------|
| missing_lactate_6h | 1 if lactate is NULL, else 0 |
| missing_troponin_6h | 1 if troponin is NULL, else 0 |
| missing_ecg_w6 | 1 if no ECG in window, else 0 |

---

# 7. Outcome Definitions

## 7.1 Prediction-Time-Aligned Outcomes

All timed outcomes are **prediction-time-aligned**: the label window starts **after** the feature window ends, guaranteeing zero temporal overlap between features and labels.

**Rule:** `hours_from_ed > P AND hours_from_ed <= P + H`

| P (prediction time) | Feature Window | Example Label Column |
|---------------------|---------------|----------------------|
| 1 hour | W1 | `icu_24h_from_w1` (events in 1–25h) |
| 6 hours | W6 | `icu_24h_from_w6` (events in 6–30h) |
| 24 hours | W24 | `icu_24h_from_w24` (events in 24–48h) |

### Individual Aligned Outcomes

| Event Type | 24h Horizon | 48h Horizon | 72h Horizon |
|------------|-------------|-------------|-------------|
| ICU | `icu_24h_from_w{1,6,24}` | `icu_48h_from_w{1,6,24}` | — |
| Pressor | `pressor_24h_from_w{1,6,24}` | `pressor_48h_from_w{1,6,24}` | — |
| Vent | `vent_24h_from_w{1,6,24}` | `vent_48h_from_w{1,6,24}` | — |
| RRT | `rrt_24h_from_w{1,6,24}` | — | — |
| Death | `death_24h_from_w{1,6,24}` | `death_48h_from_w{1,6,24}` | `death_72h_from_w{1,6,24}` |

### Composite: Deterioration (aligned)

**Definition:** Any of the following **after** the prediction time within the horizon:
- ICU admission
- Vasopressor initiation
- Mechanical ventilation
- Renal replacement therapy (RRT)
- Death

> **Note:** Cardiac arrest is excluded because it is ICD-based (no timestamp). Available separately as `cardiac_arrest_hosp`.

**SQL Logic (example for W6, 24h horizon):**
```sql
CASE
  WHEN icu_24h_from_w6 = 1
    OR pressor_24h_from_w6 = 1
    OR vent_24h_from_w6 = 1
    OR rrt_24h_from_w6 = 1
    OR death_24h_from_w6 = 1
  THEN 1 ELSE 0
END AS deterioration_24h_from_w6
```

All 6 composite variants: `deterioration_{24,48}h_from_{w1,w6,w24}`

### Coronary Event (hospitalization-level)

**Definition:** Any of the following during the hospitalization (ICD-based, no time window):
- Acute Coronary Syndrome (ACS) diagnosis
- Percutaneous Coronary Intervention (PCI)
- Coronary Artery Bypass Grafting (CABG)

> **Note:** These events are identified from ICD diagnosis/procedure codes which lack precise timestamps in MIMIC-IV. They are treated as hospitalization-level binary indicators.

**SQL Logic:**
```sql
CASE
  WHEN COALESCE(i.acs_hosp, 0) = 1 OR COALESCE(i.revasc_hosp, 0) = 1
    THEN 1 ELSE 0
END AS coronary_event_hosp
```

## 7.2 Event-By Flags

**Purpose:** Binary flags indicating whether an event **already occurred** within (before the end of) a feature window. Useful as:
- Covariates indicating "already deteriorated by prediction time"
- Filters to exclude patients with prior events

**Rule:** `hours_from_ed <= P`

**Monotonicity:** `event_by_{event}_w1 <= event_by_{event}_w6 <= event_by_{event}_w24`

| Flag | W1 | W6 | W24 |
|------|-----|-----|------|
| ICU | `event_by_icu_w1` | `event_by_icu_w6` | `event_by_icu_w24` |
| Pressor | `event_by_pressor_w1` | `event_by_pressor_w6` | `event_by_pressor_w24` |
| Vent | `event_by_vent_w1` | `event_by_vent_w6` | `event_by_vent_w24` |
| RRT | `event_by_rrt_w1` | `event_by_rrt_w6` | `event_by_rrt_w24` |
| Death | `event_by_death_w1` | `event_by_death_w6` | `event_by_death_w24` |
| Deterioration | `event_by_deterioration_w1` | `event_by_deterioration_w6` | `event_by_deterioration_w24` |

**Auto-inclusion:** When a prediction-aligned outcome like `icu_24h_from_w6` is selected during dataset generation, the corresponding `event_by_icu_w6` flag is **automatically included** in the output.

## 7.3 Hospitalization-Level Outcomes

| Outcome | Description |
|---------|-------------|
| death_hosp | In-hospital death |
| cardiac_arrest_hosp | Cardiac arrest (ICD) |
| acs_hosp | ACS (ICD) |
| revasc_hosp | Any revascularization (ICD) |
| pci_hosp | PCI (ICD) |
| cabg_hosp | CABG (ICD) |
| coronary_event_hosp | ACS or revascularization (composite) |

---

# 8. ECG Integration

## 8.1 ECG Data Flow

```
┌─────────────────────────────┐
│   MIMIC-IV ECG Files        │
│   (CSV Files on Disk)       │
│                             │
│   record_list.csv           │
│   machine_measurements.csv  │
└─────────────┬───────────────┘
              │
    ┌─────────▼─────────────────┐
    │   PostgreSQL \COPY        │
    │   (Direct CSV Load)       │
    │   via psql command        │
    └─────────┬─────────────────┘
              │
    ┌─────────▼─────────────────┐
    │   PostgreSQL Schema       │
    │   mimiciv_ecg.record_list │
    │   mimiciv_ecg.machine_... │
    └─────────┬─────────────────┘
              │
    ┌─────────▼─────────────────┐
    │   ECG Feature SQL         │
    │   (Match by subject_id +  │
    │    time within window)    │
    └─────────┬─────────────────┘
              │
    ┌─────────▼─────────────────┐
    │   tmp_ecg_features_w1/w6  │
    │   (One row per ED stay)   │
    └───────────────────────────┘
```

**Note:** ECG data is loaded directly into PostgreSQL during database setup using the `\COPY` command (see QUICK_START_DATA_LOADING.md).

## 8.2 ECG Features Schema

**Table:** `tmp_ecg_features_w6`

| Column | Type | Description | Units |
|--------|------|-------------|-------|
| stay_id | INTEGER | ED stay identifier | - |
| ecg_study_id_w6 | INTEGER | ECG study ID | - |
| ecg_time_w6 | TIMESTAMP | ECG recording time | - |
| ecg_hours_from_ed_w6 | FLOAT | Hours from ED arrival | hours |
| **ecg_hr_w6** | FLOAT | Heart rate (derived) | bpm |
| ecg_rr_interval_w6 | FLOAT | RR interval (raw) | ms |
| **ecg_qrs_dur_w6** | FLOAT | QRS duration | ms |
| **ecg_pr_w6** | FLOAT | PR interval | ms |
| **ecg_qt_w6** | FLOAT | QT interval (proxy) | ms |
| ecg_p_onset_w6 | FLOAT | P wave onset | ms |
| ecg_p_end_w6 | FLOAT | P wave end | ms |
| ecg_qrs_onset_w6 | FLOAT | QRS onset | ms |
| ecg_qrs_end_w6 | FLOAT | QRS end | ms |
| ecg_t_end_w6 | FLOAT | T wave end | ms |
| **ecg_p_axis_w6** | FLOAT | P wave axis | degrees |
| **ecg_qrs_axis_w6** | FLOAT | QRS axis | degrees |
| **ecg_t_axis_w6** | FLOAT | T wave axis | degrees |
| **missing_ecg_w6** | INTEGER | No ECG in window | 0/1 |

## 8.3 ECG Feature Derivation

**Heart Rate:**
```
ecg_hr = 60000 / rr_interval
```
(Where RR interval is in milliseconds)

**QRS Duration:**
```
ecg_qrs_dur = qrs_end - qrs_onset
```

**PR Interval:**
```
ecg_pr = qrs_onset - p_onset
```

**QT Interval (proxy):**
```
ecg_qt = t_end - qrs_onset
```

## 8.4 ECG Coverage Statistics

| Window | Coverage | Count |
|--------|----------|-------|
| W1 (1h) | 26.6% | 112,948 |
| W6 (6h) | 34.3% | 145,790 |

---

# 9. Configuration Reference

## 9.1 config.yaml Structure

```yaml
# Database Configuration
db:
  host: "localhost"              # PostgreSQL host
  port: 5432                     # PostgreSQL port
  name: "mimic"                  # Database name
  user: "postgres"               # Username
  password_env: "PGPASSWORD"     # Environment variable for password

# Schema Mapping
schemas:
  ed: "mimiciv_ed"               # ED module schema
  hosp: "mimiciv_hosp"           # Hospital module schema
  icu: "mimiciv_icu"             # ICU module schema
  ecg: mimiciv_ecg

# Table Names
tables:
  base_ed_cohort: "tmp_base_ed_cohort"
  event_log: "tmp_ed_event_log"
  outcomes: "tmp_ed_outcomes"
  features_w1: "tmp_features_w1"
  features_w6: "tmp_features_w6"
  features_w24: "tmp_features_w24"
  ecg_features_w1: "tmp_ecg_features_w1"
  ecg_features_w6: "tmp_ecg_features_w6"

# Cohort Filters
cohort:
  min_age: 18                    # Minimum age
  exclude_age_over: null         # Max age (null = no limit)
  require_hadm_id: false         # Require hospital admission
ecg:
  enabled: true
```

## 9.2 Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| PGPASSWORD | PostgreSQL password | Yes |

---

# 10. Usage Examples

## 10.1 Running the Full Pipeline

```bash
# Set password
export PGPASSWORD="your_password"

# Run complete pipeline
cd MIMIC_deterioration_pipeline
python -m src.main
```

## 10.2 Generating Specific Datasets

```bash
# List available datasets
python -m src.make_datasets --list

# Generate specific datasets
python -m src.make_datasets --datasets ed_w6_det24_admitted ed_w6_coronary72_admitted

# Generate all datasets
python -m src.make_datasets --all
```

## 10.3 Available Dataset Presets

| Dataset Name | Window | Outcome | Cohort | ECG |
|--------------|--------|---------|--------|-----|
| ed_w1_icu24 | W1 | icu_24h_from_w1 | admitted | No |
| ed_w6_det24 | W6 | deterioration_24h_from_w6 | admitted | Yes |
| ed_w24_det48 | W24 | deterioration_48h_from_w24 | admitted | No |
| ed_w6_coronary_hosp | W6 | coronary_event_hosp | admitted | Yes |
| ed_w6_acs_hosp | W6 | acs_hosp | admitted | Yes |
| ed_w6_death_hosp | W6 | death_hosp | admitted | Yes |
| w1_w6_cardiac_with_ecg | W1,W6 | coronary_event_hosp | admitted | Yes |
| w6_w24_multi_mortality | W6,W24 | death_24h_from_w6, death_48h_from_w6, death_hosp | admitted | No |
| elderly_elevated_troponin | W6 | deterioration_24h_from_w6 | admitted | Yes |
| my_w6_mortality_dataset | W6 | death_24h_from_w6, death_48h_from_w6, death_hosp | admitted | Yes |

> **Note:** When a prediction-aligned outcome is selected, the corresponding `event_by_*` flags are **automatically included** in the output dataset.

## 10.4 Custom Dataset Generation

```python
from src.materialize_datasets import materialize_dataset
from src.db import get_conn
from src.utils import load_yaml

cfg = load_yaml("config/config.yaml")
conn = get_conn(cfg)

# Create custom dataset
df = materialize_dataset(
    conn=conn,
    cfg=cfg,
    window="W6",
    outcome_col="deterioration_24h_from_w6",
    out_csv="my_custom_dataset.csv",
    cohort_type="admitted",
    include_ecg=True
)

conn.close()
```

## 10.5 Advanced Dataset Generation (Multi-Outcome and Multi-Window)

The pipeline supports generating datasets with multiple outcomes and/or multiple feature windows using the `materialize_dataset_advanced()` function or the `generate_advanced_dataset.py` CLI script.

### Multi-Outcome Datasets

Create datasets with multiple outcome columns (e.g., death at 24h, 48h, and in-hospital):

```bash
# CLI: Generate multi-outcome mortality dataset
python generate_advanced_dataset.py \
    --outcomes death_24h_from_w6 death_48h_from_w6 death_hosp \
    --window W6 \
    --cohort admitted \
    --name mortality_multi_outcome
```

```python
# Python: Generate multi-outcome dataset
from src.materialize_datasets import materialize_dataset_advanced
from src.db import get_conn
from src.utils import load_yaml

cfg = load_yaml("config/config.yaml")
conn = get_conn(cfg)

df = materialize_dataset_advanced(
    conn=conn,
    cfg=cfg,
    windows="W6",                                    # Single window
    outcome_cols=["death_24h_from_w6", "death_48h_from_w6", "death_hosp"],  # Multiple outcomes
    out_csv="artifacts/datasets/mortality_multi_outcome.csv",
    cohort_type="admitted",
    include_ecg=True
)

# Result columns: y_death_24h_from_w6, y_death_48h_from_w6, y_death_hosp
# Plus auto-included event_by flags: event_by_death_w6
print(f"Columns: {[c for c in df.columns if c.startswith('y_')]}")
conn.close()
```

### Multi-Window Datasets

Create datasets combining features from multiple time windows (feature columns are suffixed with window name):

```bash
# CLI: Generate multi-window dataset
python generate_advanced_dataset.py \
    --windows W6 W24 \
    --outcome deterioration_24h_from_w6 \
    --cohort admitted \
    --name multi_window_det24
```

```python
# Python: Generate multi-window dataset
df = materialize_dataset_advanced(
    conn=conn,
    cfg=cfg,
    windows=["W6", "W24"],           # Multiple windows
    outcome_cols="deterioration_24h_from_w6",  # Single outcome
    out_csv="artifacts/datasets/multi_window_det24.csv",
    cohort_type="admitted"
)

# Features from W6 have _w6 suffix, W24 features have _w24 suffix
# e.g., sbp_mean_6h_w6, sbp_mean_24h_w24
conn.close()
```

### Combined Multi-Outcome + Multi-Window

```bash
# CLI: Full combination Example
python generate_advanced_dataset.py \
    --windows W6 W24 \
    --outcomes death_24h_from_w6 death_48h_from_w6 death_hosp \
    --cohort admitted \
    --ecg \
    --name full_mortality_analysis
```

### CLI Options for generate_advanced_dataset.py

| Option | Description | Example |
|--------|-------------|---------|
| `--window` | Single feature window | `--window W6` |
| `--windows` | Multiple feature windows | `--windows W6 W24` |
| `--outcome` | Single outcome column | `--outcome death_24h_from_w6` |
| `--outcomes` | Multiple outcome columns | `--outcomes death_24h_from_w6 death_48h_from_w6` |
| `--cohort` | Cohort filter (all/admitted/not_admitted) | `--cohort admitted` |
| `--name` | Dataset name (auto-generated if not provided) | `--name my_dataset` |
| `--ecg` | Include ECG features | `--ecg` |
| `--filter` | Additional SQL filter | `--filter "age_at_ed >= 65"` |
| `--no-missing-indicators` | Disable automatic missing indicators | |
| `--missing-threshold` | Threshold for missing indicators (default 0.10) | `--missing-threshold 0.05` |
| `--verbose` | Enable verbose logging | `--verbose` |

## 10.6 Running Validation

```bash
# Run unit tests
python -m pytest tests/ -v

# Validate pipeline integrity
python validate_pipeline_integrity.py

# Validate generated datasets
python validate_datasets.py --datasets ed_w6_*.csv
```

---

# 11. API Reference

## 11.1 Core Functions

### `get_conn(cfg)`
Create database connection.

**Parameters:**
- `cfg` (dict): Configuration dictionary

**Returns:**
- `psycopg2.connection`: Database connection object

### `execute_sql(conn, sql, params=None)`
Execute SQL query.

**Parameters:**
- `conn`: Database connection
- `sql` (str): SQL query
- `params` (dict): Template parameters

### `build_base_cohort(conn, cfg)`
Create base ED cohort table.

**Parameters:**
- `conn`: Database connection
- `cfg` (dict): Configuration

### `build_event_log(conn, cfg)`
Extract clinical events into event log.

### `build_outcomes(conn, cfg)`
Calculate outcome labels from events.

### `build_features(conn, cfg, windows=['W1', 'W6', 'W24'])`
Extract features for specified windows.

### `build_ecg_features(conn, cfg, windows=['W1', 'W6'])`
Create ECG feature tables.

### `materialize_dataset(conn, cfg, feature_window, outcome_col, cohort_filter, include_ecg)`
Generate analysis-ready dataset.

**Parameters:**
- `conn`: Database connection
- `cfg`: Configuration dictionary  
- `window` (str): 'W1', 'W6', or 'W24'
- `outcome_col` (str): Column from outcomes table
- `out_csv` (str): Output CSV path
- `cohort_type` (str): 'all', 'admitted', or 'not_admitted'
- `include_ecg` (bool): Include ECG features

**Returns:**
- `pd.DataFrame`: Dataset with features and outcomes

### `materialize_dataset_advanced(conn, cfg, windows, outcome_cols, out_csv, ...)` (Recommeneded to use)
Generate datasets with multiple outcomes and/or multiple windows.

**Parameters:**
- `conn`: Database connection
- `cfg`: Configuration dictionary
- `windows` (str or List[str]): Single window or list of windows (e.g., ["W6", "W24"])
- `outcome_cols` (str or List[str]): Single outcome or list of outcomes
- `out_csv` (str): Output CSV path
- `cohort_type` (str): 'all', 'admitted', or 'not_admitted'
- `cohort_filter_sql` (str): Optional additional SQL filter
- `include_ecg` (bool): Include ECG features
- `add_missing_ind` (bool): Add missing indicator columns
- `missing_threshold` (float): Threshold for missing indicators (default 0.10)

**Returns:**
- `pd.DataFrame`: Dataset with features and outcome(s)

**Multi-Outcome Behavior:**
- When multiple outcomes specified, the pipeline creates columns `y_<outcome_name>` for each
- Example: `y_death_24h_from_w6`, `y_death_48h_from_w6`, `y_death_hosp`
- Corresponding `event_by_*` flags are auto-included for aligned outcomes

**Multi-Window Behavior:**
- When multiple windows specified, feature columns are suffixed with window name
- Example: `sbp_mean_6h_w6`, `sbp_mean_24h_w24`

---

# 12. Validation & Quality Assurance

## 12.1 Integrity Checks

| Check | Status | Description |
|-------|--------|-------------|
| No duplicate stay_ids | PASS | Primary key integrity |
| All events after ED arrival | PASS | Temporal consistency |
| Outcomes match event log | PASS | Calculation accuracy |
| 1:1 cohort-outcome mapping | PASS | Join integrity |
| ECG timing within windows | PASS | Window constraints |

## 12.2 Automated QA Checks (sql/99_qa_checks.sql)

The pipeline runs 8 automated assertion queries after every execution. Each query returns rows only when violations exist; zero rows = PASS.

| # | Check Name | Description | Expected |
|---|-----------|-------------|----------|
| 1 | `events_before_ed` | Timed events with `event_time < ed_intime` | 0 |
| 2 | `icd_events_with_time` | ICD events (ACS/revasc/arrest) that incorrectly have a timestamp | 0 |
| 3 | `event_by_monotonicity_icu` | `event_by_icu_w1 > event_by_icu_w6` (violates monotonicity) | 0 |
| 4 | `age_out_of_bounds` | `age_at_ed < 18` or `age_at_ed > 110` | 0 |
| 5 | `dead_before_ed` | Patients with `dod < ed_intime` | 0 |
| 6 | `ecg_outside_ed_window` | ECG linked outside `[ed_intime, ed_outtime]` range | 0 |
| 7 | `label_event_by_overlap_icu_w6` | `icu_24h_from_w6 = 1 AND event_by_icu_w6 = 1` (INFO only) | ~109 (multi-admission) |
| 8 | `death_wrong_source` | Death events with `event_source` not matching expected table names | 0 |

### Latest QA Run (Feb 20, 2026)

| Check | Violations | Status |
|-------|-----------|--------|
| events_before_ed | 0 | PASS |
| icd_events_with_time | 0 | PASS |
| event_by_monotonicity_icu | 0 | PASS |
| age_out_of_bounds | 0 | PASS |
| dead_before_ed | 0 | PASS |
| ecg_outside_ed_window | 0 | PASS |
| label_event_by_overlap_icu_w6 | 109 | INFO (expected multi-admission overlap) |
| death_wrong_source | 0 | PASS |

> The 109 `label_event_by_overlap` cases represent patients with multiple hospital admissions where ICU occurred before one ED visit but after another — a real clinical scenario, not a data bug. This check is logged at INFO level.

## 12.3 Data Quality Metrics

| Metric | Value |
|--------|-------|
| Vital signs completeness (W1) | ~95-97% |
| Vital signs completeness (W6) | ~65-67% |
| Lab values completeness (6h) | 1-8% |
| ECG coverage (W1) | 26.6% |
| ECG coverage (W6) | 34.3% |
| Age data completeness | 100% |
| Outcome data completeness | 100% |

## 12.4 Running Validation
Example

```bash
# Quick integrity check
python -c "
from src.db import get_conn
from src.utils import load_yaml
cfg = load_yaml('config/config.yaml')
conn = get_conn(cfg)
cur = conn.cursor()
cur.execute('SELECT COUNT(*), COUNT(DISTINCT stay_id) FROM tmp_ed_outcomes')
total, unique = cur.fetchone()
print(f'Outcomes: {total:,} rows, {unique:,} unique')
assert total == unique, 'Duplicate stay_ids found!'
print('Integrity check passed!')
"
```

---

# 13. Troubleshooting

## 13.1 Common Issues

### Database Connection Failed

**Error:** `psycopg2.OperationalError: connection refused`

**Solution:**
1. Check PostgreSQL is running
2. Verify host/port in config.yaml
3. Ensure PGPASSWORD is set

### Missing Tables
**Error:** `relation "mimiciv_ed.edstays" does not exist`


**Solution:**
1. Verify MIMIC-IV is loaded
2. Check schema names in config.yaml


### ECG Load Failed

**Error:** `FileNotFoundError: record_list.csv not found`

**Solution:**
1. Update `ecg.local_dir` in config.yaml
2. Verify CSV files exist

### Unicode Encoding Error (Windows)

**Error:** `UnicodeEncodeError: 'charmap' codec can't encode character`

**Status:** Fixed in v2.1. All Unicode symbols (✓ ✗ ⚠️ ⏱️) in Python logger output have been replaced with ASCII equivalents (`[OK]`, `[FAIL]`, `[WARN]`, `[TIME]`) across all 10 source modules. No action required.

**If encountered in custom scripts:**
```python
import sys
sys.stdout.reconfigure(encoding='utf-8')
```

## 13.2 Performance Optimization

```yaml
# For large datasets, add PostgreSQL tuning:
# postgresql.conf
shared_buffers = 4GB
work_mem = 512MB
maintenance_work_mem = 1GB
```

---

# 14. Appendices

## Appendix A: ICD Code Definitions

### Cardiac Arrest (16_event_cardiac_arrest.sql)
```sql
-- ICD-10 codes (pipeline uses a LIKE match to capture all I46 variants)
-- SQL: `icd_code LIKE 'I46%'`  -- captures I46.0, I46.1, I46.2, I46.9, etc.

-- ICD-9 codes (explicit dotted forms used in SQL)
-- SQL: `icd_code IN ('427.5', '427.41', '427.42')`
```

### ACS (14_event_acs.sql)
```sql
-- ICD-10 codes
'I21%'   -- STEMI
'I22%'   -- Subsequent MI
'I200'   -- Unstable angina
'+ ICD 9 codes if applicable'
```

### Revascularization (15_event_revasc.sql)
```sql
-- ICD-10-PCS (PCI)
'027%'   -- Coronary dilation

-- ICD-10-PCS (CABG)
'021%'   -- Coronary bypass
```

## Appendix B: Vasopressor Definitions (11_event_pressors.sql)

```sql
-- itemids from mimiciv_icu.inputevents
221906  -- Norepinephrine
221289  -- Epinephrine
221662  -- Dopamine
222315  -- Vasopressin
221749  -- Phenylephrine
```

## Appendix C: Dataset Column Reference

### Standard Dataset Columns (ed_w6_det24_admitted.csv)

| # | Column | Type | Description |
|---|--------|------|-------------|
| 1 | stay_id | int64 | ED stay identifier |
| 2 | subject_id | int64 | Patient identifier |
| 3 | hadm_id | int64 | Hospital admission ID |
| 4 | ed_intime | datetime | ED arrival time |
| 5 | ed_outtime | datetime | ED departure time |
| 6 | age_at_ed | float64 | Age at ED visit |
| 7 | gender | object | Gender (M/F) |
| 8 | ed_los_hours | float64 | ED length of stay |
| 9-22 | Vitals (sbp, hr, rr, spo2, temp) | float64 | Vital sign features |
| 23-31 | Labs (lactate, troponin, etc.) | float64 | Laboratory features |
| 32-33 | Derived (sbp_cv, hr_range) | float64 | Derived features |
| 34-35 | Missing indicators (lactate, troponin) | int64 | Missing flags |
| 36-51 | ECG features | float64 | ECG measurements |
| 52 | missing_ecg_w6 | int64 | ECG missing flag |
| 53 | y | int64 | **Outcome label** (0/1) |
| 54+ | Auto-generated missing_* | int64 | Additional missing flags |

---

---

# PART 2: VARIABLE DICTIONARY

# Variable Dictionary - Complete Reference

---

This document provides an exhaustive reference of every variable in the MIMIC deterioration pipeline, organized by category.

---

---

## Table of Contents

1. [Identifier Variables](#1-identifier-variables)
2. [Demographic Variables](#2-demographic-variables)
3. [Timestamp Variables](#3-timestamp-variables)
4. [Vital Signs Variables](#4-vital-signs-variables)
5. [Laboratory Variables](#5-laboratory-variables)
6. [ECG Variables](#6-ecg-variables)
7. [Outcome Variables](#7-outcome-variables)
8. [Derived Variables](#8-derived-variables)
9. [Missing Indicator Variables](#9-missing-indicator-variables)
10. [Event Log Variables](#10-event-log-variables)
11. [Variable Summary Statistics](#11-variable-summary-statistics)

---

# 1. Identifier Variables

These variables uniquely identify records and link across tables.

| Variable | Data Type | Nullable | Description | Example | Source Table |
|----------|-----------|----------|-------------|---------|--------------|
| **stay_id** | INTEGER | No | **Primary Key** - Unique identifier for each Emergency Department visit. This is the core identifier used to link all pipeline tables. | 30001234 | mimiciv_ed.edstays |
| **subject_id** | INTEGER | No | Unique patient identifier. A patient can have multiple ED visits (stay_ids). | 10000032 | mimiciv_hosp.patients |
| **hadm_id** | INTEGER | Yes | Hospital admission identifier. Only populated if the ED visit resulted in hospital admission. NULL if discharged home. | 20001234 | mimiciv_ed.edstays |
| **study_id** | INTEGER | Yes | ECG study identifier. Links to machine measurements. Only available for ECG-matched records. | 40001234 | ECG record_list.csv |

## Key Relationships

```
subject_id (1) ─────< (N) stay_id ─────< (0..1) hadm_id
     │
     └──────────────< (N) study_id (ECG studies)
```

## Identifier Statistics

| Variable | Unique Values | NULL Rate |
|----------|---------------|-----------|
| stay_id | 424,952 | 0% |
| subject_id | 205,427 | 0% |
| hadm_id | 202,415 | 52.4% |

---

# 2. Demographic Variables

Patient characteristics captured at ED arrival.

| Variable | Data Type | Nullable | Description | Valid Range | Missing Rate |
|----------|-----------|----------|-------------|-------------|--------------|
| **age_at_ed** | FLOAT | No | Age in years at time of ED visit. Calculated from anchor_year adjusted to ED visit time. | 18-110 | 0% |
| **anchor_age** | INTEGER | No | Patient's age at the anchor_year reference point (used for age calculations). | 18-91 | 0% |
| **anchor_year** | INTEGER | No | Reference year for age calculations. MIMIC uses anchor years to preserve privacy. | 2100-2200 | 0% |
| **gender** | CHAR(1) | No | Patient biological sex. Binary classification. | 'M', 'F' | 0% |
| **dod** | DATE | Yes | Date of death if known. NULL if patient is alive or death date unknown. | DATE | 95.8% |

## Age Distribution

| Statistic | Value |
|-----------|-------|
| Mean | 55.2 years |
| Median | 56.0 years |
| Std Dev | 20.1 years |
| Min | 18.0 years |
| Max | 110.0 years |
| Q25 | 38.0 years |
| Q75 | 71.0 years |

## Gender Distribution

| Gender | Count | Percentage |
|--------|-------|------------|
| Male | 200,738 | 47.2% |
| Female | 224,214 | 52.8% |

---

# 3. Timestamp Variables

Time-related variables for temporal analysis.

| Variable | Data Type | Format | Description | Example |
|----------|-----------|--------|-------------|---------|
| **ed_intime** | TIMESTAMP | YYYY-MM-DD HH:MI:SS | ED arrival timestamp. This is the reference time (T=0) for all feature windows. | 2150-01-15 14:30:00 |
| **ed_outtime** | TIMESTAMP | YYYY-MM-DD HH:MI:SS | ED departure timestamp. When patient left ED (to ward, ICU, home, or death). | 2150-01-15 22:45:00 |
| **event_time** | TIMESTAMP | YYYY-MM-DD HH:MI:SS | Timestamp of clinical event (in event log). | 2150-01-16 02:30:00 |
| **ecg_time_wX** | TIMESTAMP | YYYY-MM-DD HH:MI:SS | Timestamp of first ECG within the specified window. | 2150-01-15 15:15:00 |

## Derived Time Variables

| Variable | Data Type | Units | Description | Formula |
|----------|-----------|-------|-------------|---------|
| **ed_los_hours** | FLOAT | Hours | ED length of stay in hours | (ed_outtime - ed_intime) / 3600 |
| **time_to_icu** | FLOAT | Hours | Time from ED arrival to ICU admission | (icu_time - ed_intime) / 3600 |
| **time_to_death** | FLOAT | Hours | Time from ED arrival to death | (dod - ed_intime) / 24 |
| **time_to_deterioration** | FLOAT | Hours | Time to first critical event | MIN(time_to_icu, time_to_death, ...) |
| **ecg_hours_from_ed_wX** | FLOAT | Hours | Time from ED arrival to ECG | (ecg_time - ed_intime) / 3600 |

## ED Length of Stay Distribution

| Statistic | Value |
|-----------|-------|
| Mean | 6.8 hours |
| Median | 5.2 hours |
| Std Dev | 7.2 hours |
| Min | 0.0 hours |
| Max | 720.0 hours |
| Q25 | 3.1 hours |
| Q75 | 8.4 hours |

---

# 4. Vital Signs Variables

Vital signs extracted from `mimiciv_ed.vitalsign` within specified time windows.

## 4.1 Systolic Blood Pressure (SBP)

| Variable | Data Type | Units | Description | Normal Range |
|----------|-----------|-------|-------------|--------------|
| **sbp_min_1h** | FLOAT | mmHg | Minimum SBP in first 1 hour | 90-120 |
| **sbp_max_1h** | FLOAT | mmHg | Maximum SBP in first 1 hour | 90-140 |
| **sbp_mean_1h** | FLOAT | mmHg | Mean SBP in first 1 hour | 100-130 |
| **sbp_std_1h** | FLOAT | mmHg | Standard deviation of SBP in first 1 hour | 0-15 |
| **sbp_min_6h** | FLOAT | mmHg | Minimum SBP in first 6 hours | 90-120 |
| **sbp_max_6h** | FLOAT | mmHg | Maximum SBP in first 6 hours | 90-140 |
| **sbp_mean_6h** | FLOAT | mmHg | Mean SBP in first 6 hours | 100-130 |
| **sbp_std_6h** | FLOAT | mmHg | Standard deviation of SBP in first 6 hours | 0-15 |
| **sbp_min_24h** | FLOAT | mmHg | Minimum SBP in first 24 hours | 90-120 |
| **sbp_max_24h** | FLOAT | mmHg | Maximum SBP in first 24 hours | 90-140 |
| **sbp_mean_24h** | FLOAT | mmHg | Mean SBP in first 24 hours | 100-130 |
| **sbp_std_24h** | FLOAT | mmHg | Standard deviation of SBP in first 24 hours | 0-15 |

## 4.2 Diastolic Blood Pressure (DBP)

| Variable | Data Type | Units | Description | Normal Range |
|----------|-----------|-------|-------------|--------------|
| **dbp_min_1h** | FLOAT | mmHg | Minimum DBP in first 1 hour | 60-80 |
| **dbp_max_1h** | FLOAT | mmHg | Maximum DBP in first 1 hour | 60-90 |
| **dbp_mean_1h** | FLOAT | mmHg | Mean DBP in first 1 hour | 65-85 |
| **dbp_std_1h** | FLOAT | mmHg | Standard deviation of DBP in first 1 hour | 0-10 |
| **dbp_min_6h** | FLOAT | mmHg | Minimum DBP in first 6 hours | 60-80 |
| **dbp_max_6h** | FLOAT | mmHg | Maximum DBP in first 6 hours | 60-90 |
| **dbp_mean_6h** | FLOAT | mmHg | Mean DBP in first 6 hours | 65-85 |
| **dbp_std_6h** | FLOAT | mmHg | Standard deviation of DBP in first 6 hours | 0-10 |
| **dbp_min_24h** | FLOAT | mmHg | Minimum DBP in first 24 hours | 60-80 |
| **dbp_max_24h** | FLOAT | mmHg | Maximum DBP in first 24 hours | 60-90 |
| **dbp_mean_24h** | FLOAT | mmHg | Mean DBP in first 24 hours | 65-85 |
| **dbp_std_24h** | FLOAT | mmHg | Standard deviation of DBP in first 24 hours | 0-10 |

## 4.3 Heart Rate (HR)

| Variable | Data Type | Units | Description | Normal Range |
|----------|-----------|-------|-------------|--------------|
| **hr_min_1h** | FLOAT | bpm | Minimum heart rate in first 1 hour | 60-100 |
| **hr_max_1h** | FLOAT | bpm | Maximum heart rate in first 1 hour | 60-100 |
| **hr_mean_1h** | FLOAT | bpm | Mean heart rate in first 1 hour | 60-80 |
| **hr_std_1h** | FLOAT | bpm | Standard deviation of HR in first 1 hour | 0-10 |
| **hr_min_6h** | FLOAT | bpm | Minimum heart rate in first 6 hours | 60-100 |
| **hr_max_6h** | FLOAT | bpm | Maximum heart rate in first 6 hours | 60-100 |
| **hr_mean_6h** | FLOAT | bpm | Mean heart rate in first 6 hours | 60-80 |
| **hr_std_6h** | FLOAT | bpm | Standard deviation of HR in first 6 hours | 0-10 |
| **hr_min_24h** | FLOAT | bpm | Minimum heart rate in first 24 hours | 60-100 |
| **hr_max_24h** | FLOAT | bpm | Maximum heart rate in first 24 hours | 60-100 |
| **hr_mean_24h** | FLOAT | bpm | Mean heart rate in first 24 hours | 60-80 |
| **hr_std_24h** | FLOAT | bpm | Standard deviation of HR in first 24 hours | 0-10 |

## 4.4 Respiratory Rate (RR)

| Variable | Data Type | Units | Description | Normal Range |
|----------|-----------|-------|-------------|--------------|
| **rr_min_1h** | FLOAT | /min | Minimum respiratory rate in first 1 hour | 12-20 |
| **rr_max_1h** | FLOAT | /min | Maximum respiratory rate in first 1 hour | 12-20 |
| **rr_mean_1h** | FLOAT | /min | Mean respiratory rate in first 1 hour | 14-18 |
| **rr_min_6h** | FLOAT | /min | Minimum respiratory rate in first 6 hours | 12-20 |
| **rr_max_6h** | FLOAT | /min | Maximum respiratory rate in first 6 hours | 12-20 |
| **rr_mean_6h** | FLOAT | /min | Mean respiratory rate in first 6 hours | 14-18 |
| **rr_min_24h** | FLOAT | /min | Minimum respiratory rate in first 24 hours | 12-20 |
| **rr_max_24h** | FLOAT | /min | Maximum respiratory rate in first 24 hours | 12-20 |
| **rr_mean_24h** | FLOAT | /min | Mean respiratory rate in first 24 hours | 14-18 |

## 4.5 Oxygen Saturation (SpO2)

| Variable | Data Type | Units | Description | Normal Range |
|----------|-----------|-------|-------------|--------------|
| **spo2_min_1h** | FLOAT | % | Minimum SpO2 in first 1 hour | 95-100 |
| **spo2_max_1h** | FLOAT | % | Maximum SpO2 in first 1 hour | 95-100 |
| **spo2_mean_1h** | FLOAT | % | Mean SpO2 in first 1 hour | 96-99 |
| **spo2_min_6h** | FLOAT | % | Minimum SpO2 in first 6 hours | 95-100 |
| **spo2_max_6h** | FLOAT | % | Maximum SpO2 in first 6 hours | 95-100 |
| **spo2_mean_6h** | FLOAT | % | Mean SpO2 in first 6 hours | 96-99 |
| **spo2_min_24h** | FLOAT | % | Minimum SpO2 in first 24 hours | 95-100 |
| **spo2_max_24h** | FLOAT | % | Maximum SpO2 in first 24 hours | 95-100 |
| **spo2_mean_24h** | FLOAT | % | Mean SpO2 in first 24 hours | 96-99 |

## 4.6 Temperature

| Variable | Data Type | Units | Description | Normal Range |
|----------|-----------|-------|-------------|--------------|
| **temp_min_1h** | FLOAT | °F | Minimum temperature in first 1 hour | 97.0-99.0 |
| **temp_max_1h** | FLOAT | °F | Maximum temperature in first 1 hour | 97.0-99.0 |
| **temp_mean_1h** | FLOAT | °F | Mean temperature in first 1 hour | 97.5-98.5 |
| **temp_min_6h** | FLOAT | °F | Minimum temperature in first 6 hours | 97.0-99.0 |
| **temp_max_6h** | FLOAT | °F | Maximum temperature in first 6 hours | 97.0-99.0 |
| **temp_mean_6h** | FLOAT | °F | Mean temperature in first 6 hours | 97.5-98.5 |
| **temp_min_24h** | FLOAT | °F | Minimum temperature in first 24 hours | 97.0-99.0 |
| **temp_max_24h** | FLOAT | °F | Maximum temperature in first 24 hours | 97.0-99.0 |
| **temp_mean_24h** | FLOAT | °F | Mean temperature in first 24 hours | 97.5-98.5 |

## 4.7 Vital Sign Measurement Count

| Variable | Data Type | Units | Description | 
|----------|-----------|-------|-------------|
| **n_vital_measurements_1h** | INTEGER | count | Number of vital sign measurements in first 1 hour |
| **n_vital_measurements_6h** | INTEGER | count | Number of vital sign measurements in first 6 hours |
| **n_vitalsign_measurements_24h** | INTEGER | count | Number of vital sign measurements in first 24 hours |

---

# 4.8 ED Process Metrics (W6,W12)

**Timing and utilization metrics from the Emergency Department stay.**

| Variable | Data Type | Units | Description | Window | Source |
|----------|-----------|-------|-------------|--------|--------|
| **time_to_first_lab_hours** | FLOAT | hours | Time from ED arrival to first laboratory result | W6, W24 | labevents |
| **time_to_first_med_hours** | FLOAT | hours | Time from ED arrival to first medication administration | W6, W24 | pyxis |

**Clinical Interpretation:**
- Early lab availability (<1h) suggests rapid triage and testing
- Delayed labs (>6h) may indicate low acuity or delayed concern
- Early medications suggest recognition of illness severity

---

# 4.9 Clinical Context (W1,W6,W12)

**Patient presentation information from triage.**

| Variable | Data Type | Description | Window | Source |
|----------|-----------|-------------|--------|--------|
| **chiefcomplaint** | TEXT | Patient's chief complaint at ED triage | W6, W24 | triage |

**Examples:**
- "Chest pain", "Shortness of breath", "Abdominal pain", "Weakness"
- May contain multiple complaints separated by commas

---

# 4.10 Prior History (W6,W12)

**Historical healthcare utilization in the 1 year prior to ED visit.**

| Variable | Data Type | Units | Description | Lookback | Window | Source |
|----------|-----------|-------|-------------|----------|--------|--------|
| **prev_admits_1yr** | INTEGER | count | Number of hospital admissions in prior 1 year | 1 year | W6, W24 | admissions |
| **prev_ed_visits_1yr** | INTEGER | count | Number of ED visits in prior 1 year (excluding current) | 1 year | W6, W24 | edstays |

**Clinical Interpretation:**
- High prior utilization may indicate chronic illness or frequent relapse
- Frequent ED visits without admission may suggest repeat presentations
- New patients (0 prior visits) may have different deterioration patterns


---

# 5. Laboratory Variables

Laboratory values extracted from `mimiciv_hosp.labevents` within specified time windows. First value within window is used.

## 5.1 Metabolic Panel

| Variable | Data Type | Units | Description | Normal Range | Critical |
|----------|-----------|-------|-------------|--------------|----------|
| **lactate_first_1h** | FLOAT | mmol/L | First lactate in first 1 hour | 0.5-2.0 | >4.0 |
| **lactate_first_6h** | FLOAT | mmol/L | First lactate in first 6 hours | 0.5-2.0 | >4.0 |
| **lactate_first_24h** | FLOAT | mmol/L | First lactate in first 24 hours | 0.5-2.0 | >4.0 |
| **creatinine_first_1h** | FLOAT | mg/dL | First creatinine in first 1 hour | 0.7-1.3 | >4.0 |
| **creatinine_first_6h** | FLOAT | mg/dL | First creatinine in first 6 hours | 0.7-1.3 | >4.0 |
| **creatinine_first_24h** | FLOAT | mg/dL | First creatinine in first 24 hours | 0.7-1.3 | >4.0 |
| **bicarbonate_first_1h** | FLOAT | mEq/L | First bicarbonate in first 1 hour | 22-28 | <15 |
| **bicarbonate_first_6h** | FLOAT | mEq/L | First bicarbonate in first 6 hours | 22-28 | <15 |
| **bicarbonate_first_24h** | FLOAT | mEq/L | First bicarbonate in first 24 hours | 22-28 | <15 |

## 5.2 Electrolytes

| Variable | Data Type | Units | Description | Normal Range | Critical |
|----------|-----------|-------|-------------|--------------|----------|
| **potassium_first_1h** | FLOAT | mEq/L | First potassium in first 1 hour | 3.5-5.0 | <2.5 or >6.5 |
| **potassium_first_6h** | FLOAT | mEq/L | First potassium in first 6 hours | 3.5-5.0 | <2.5 or >6.5 |
| **potassium_first_24h** | FLOAT | mEq/L | First potassium in first 24 hours | 3.5-5.0 | <2.5 or >6.5 |
| **sodium_first_1h** | FLOAT | mEq/L | First sodium in first 1 hour | 136-145 | <120 or >160 |
| **sodium_first_6h** | FLOAT | mEq/L | First sodium in first 6 hours | 136-145 | <120 or >160 |
| **sodium_first_24h** | FLOAT | mEq/L | First sodium in first 24 hours | 136-145 | <120 or >160 |

## 5.3 Cardiac Biomarkers

| Variable | Data Type | Units | Description | Normal Range | Elevated |
|----------|-----------|-------|-------------|--------------|----------|
| **troponin_first_1h** | FLOAT | ng/mL | First troponin in first 1 hour | <0.04 | >0.04 |
| **troponin_first_6h** | FLOAT | ng/mL | First troponin in first 6 hours | <0.04 | >0.04 |
| **is_hs_troponin_6h** | INTEGER | - | 1 if troponin assay is high-sensitivity (itemid 51003), 0 otherwise | - | - |
| **troponin_first_24h** | FLOAT | ng/mL | First troponin in first 24 hours | <0.04 | >0.04 |
| **is_hs_troponin_24h** | INTEGER | - | 1 if troponin assay is high-sensitivity (itemid 51003), 0 otherwise | - | - |

**Note:** Troponin assays vary (conventional vs high-sensitivity). The pipeline extracts all troponin values and flags high-sensitivity troponin T (itemid 51003) via `is_hs_troponin_*h`. Use this flag to distinguish assay types or normalize values for analysis.

## 5.4 Hematology

| Variable | Data Type | Units | Description | Normal Range | Critical |
|----------|-----------|-------|-------------|--------------|----------|
| **wbc_first_1h** | FLOAT | K/µL | First WBC count in first 1 hour | 4.5-11.0 | <2.0 or >30.0 |
| **wbc_first_6h** | FLOAT | K/µL | First WBC count in first 6 hours | 4.5-11.0 | <2.0 or >30.0 |
| **wbc_first_24h** | FLOAT | K/µL | First WBC count in first 24 hours | 4.5-11.0 | <2.0 or >30.0 |
| **hemoglobin_first_1h** | FLOAT | g/dL | First hemoglobin in first 1 hour | 12-16 | <7.0 |
| **hemoglobin_first_6h** | FLOAT | g/dL | First hemoglobin in first 6 hours | 12-16 | <7.0 |
| **hemoglobin_first_24h** | FLOAT | g/dL | First hemoglobin in first 24 hours | 12-16 | <7.0 |
| **platelet_first_1h** | FLOAT | K/µL | First platelet count in first 1 hour | 150-400 | <50 |
| **platelet_first_6h** | FLOAT | K/µL | First platelet count in first 6 hours | 150-400 | <50 |
| **platelet_first_24h** | FLOAT | K/µL | First platelet count in first 24 hours | 150-400 | <50 |

## 5.5 Extended Lab Panel (W24 Only)

| Variable | Data Type | Units | Description | Normal Range | Critical |
|----------|-----------|-------|-------------|--------------|----------|
| **glucose_first_24h** | FLOAT | mg/dL | First glucose in first 24 hours | 70-100 | <40 or >400 |
| **bun_first_24h** | FLOAT | mg/dL | First BUN in first 24 hours | 7-20 | >100 |
| **bilirubin_first_24h** | FLOAT | mg/dL | First bilirubin in first 24 hours | 0.3-1.2 | >12 |
| **inr_first_24h** | FLOAT | ratio | First INR in first 24 hours | 0.8-1.2 | >5.0 |

## 5.6 Lab Max Values (W24 Only)

| Variable | Data Type | Units | Description |
|----------|-----------|-------|-------------|
| **lactate_max_24h** | FLOAT | mmol/L | Maximum lactate in first 24 hours |
| **creatinine_max_24h** | FLOAT | mg/dL | Maximum creatinine in first 24 hours |

## 5.5 Laboratory Item IDs (MIMIC-IV Reference)

| Lab Test | itemid | Description |
|----------|--------|-------------|
| Lactate | 50813 | Lactate, blood |
| Troponin T | 51003 | Troponin T (high sensitivity) |
| Troponin I | 51002 | Troponin I |
| Creatinine | 50912 | Creatinine, serum |
| Potassium | 50971 | Potassium, serum |
| Sodium | 50983 | Sodium, serum |
| Bicarbonate | 50882 | Bicarbonate, serum |
| WBC | 51301 | White blood cell count |
| Hemoglobin | 51222 | Hemoglobin |
| Platelet | 51265 | Platelet count |

---

# 6. ECG Variables

Electrocardiogram features extracted from MIMIC-IV ECG machine measurements.

## 6.1 ECG Identifiers and Timing

| Variable | Data Type | Units | Description |
|----------|-----------|-------|-------------|
| **ecg_study_id_w1** | INTEGER | - | Study ID of first ECG within 1 hour of ED arrival |
| **ecg_study_id_w6** | INTEGER | - | Study ID of first ECG within 6 hours of ED arrival |
| **ecg_time_w1** | TIMESTAMP | - | Timestamp of first ECG within 1 hour |
| **ecg_time_w6** | TIMESTAMP | - | Timestamp of first ECG within 6 hours |
| **ecg_hours_from_ed_w1** | FLOAT | hours | Time from ED arrival to ECG (1h window) |
| **ecg_hours_from_ed_w6** | FLOAT | hours | Time from ED arrival to ECG (6h window) |

## 6.2 Heart Rate and Rhythm

| Variable | Data Type | Units | Description | Normal Range | Derivation |
|----------|-----------|-------|-------------|--------------|------------|
| **ecg_hr_w1** | FLOAT | bpm | ECG heart rate (1h window) | 60-100 | 60000 / rr_interval |
| **ecg_hr_w6** | FLOAT | bpm | ECG heart rate (6h window) | 60-100 | 60000 / rr_interval |
| **ecg_rr_interval_w1** | FLOAT | ms | RR interval (1h window) | 600-1000 | Raw measurement |
| **ecg_rr_interval_w6** | FLOAT | ms | RR interval (6h window) | 600-1000 | Raw measurement |

**Clinical Note:** Heart rate is derived from RR interval. RR interval <600ms suggests tachycardia, >1000ms suggests bradycardia.

## 6.3 QRS Complex

| Variable | Data Type | Units | Description | Normal Range | Derivation |
|----------|-----------|-------|-------------|--------------|------------|
| **ecg_qrs_dur_w1** | FLOAT | ms | QRS duration (1h window) | 80-120 | qrs_end - qrs_onset |
| **ecg_qrs_dur_w6** | FLOAT | ms | QRS duration (6h window) | 80-120 | qrs_end - qrs_onset |
| **ecg_qrs_onset_w1** | FLOAT | ms | QRS onset time (1h window) | - | Raw measurement |
| **ecg_qrs_onset_w6** | FLOAT | ms | QRS onset time (6h window) | - | Raw measurement |
| **ecg_qrs_end_w1** | FLOAT | ms | QRS end time (1h window) | - | Raw measurement |
| **ecg_qrs_end_w6** | FLOAT | ms | QRS end time (6h window) | - | Raw measurement |

**Clinical Note:** QRS >120ms may indicate bundle branch block or ventricular conduction delay. QRS >200ms is concerning for ventricular rhythm.

## 6.4 PR and QT Intervals

| Variable | Data Type | Units | Description | Normal Range | Derivation |
|----------|-----------|-------|-------------|--------------|------------|
| **ecg_pr_w1** | FLOAT | ms | PR interval (1h window) | 120-200 | qrs_onset - p_onset |
| **ecg_pr_w6** | FLOAT | ms | PR interval (6h window) | 120-200 | qrs_onset - p_onset |
| **ecg_qt_w1** | FLOAT | ms | QT interval proxy (1h window) | 350-450 | t_end - qrs_onset |
| **ecg_qt_w6** | FLOAT | ms | QT interval proxy (6h window) | 350-450 | t_end - qrs_onset |
| **ecg_p_onset_w1** | FLOAT | ms | P wave onset (1h window) | - | Raw measurement |
| **ecg_p_onset_w6** | FLOAT | ms | P wave onset (6h window) | - | Raw measurement |
| **ecg_p_end_w1** | FLOAT | ms | P wave end (1h window) | - | Raw measurement |
| **ecg_p_end_w6** | FLOAT | ms | P wave end (6h window) | - | Raw measurement |
| **ecg_t_end_w1** | FLOAT | ms | T wave end (1h window) | - | Raw measurement |
| **ecg_t_end_w6** | FLOAT | ms | T wave end (6h window) | - | Raw measurement |

**Clinical Notes:**
- PR >200ms: First-degree AV block
- PR <120ms: Pre-excitation (WPW) or junctional rhythm
- QT prolongation (rate-dependent): Risk of torsades de pointes

## 6.5 Axis Measurements

| Variable | Data Type | Units | Description | Normal Range |
|----------|-----------|-------|-------------|--------------|
| **ecg_p_axis_w1** | FLOAT | degrees | P wave axis (1h window) | 0 to +75 |
| **ecg_p_axis_w6** | FLOAT | degrees | P wave axis (6h window) | 0 to +75 |
| **ecg_qrs_axis_w1** | FLOAT | degrees | QRS axis (1h window) | -30 to +90 |
| **ecg_qrs_axis_w6** | FLOAT | degrees | QRS axis (6h window) | -30 to +90 |
| **ecg_t_axis_w1** | FLOAT | degrees | T wave axis (1h window) | 0 to +75 |
| **ecg_t_axis_w6** | FLOAT | degrees | T wave axis (6h window) | 0 to +75 |

**Axis Interpretation:**
| QRS Axis | Interpretation |
|----------|----------------|
| -30 to +90° | Normal |
| -30 to -90° | Left axis deviation |
| +90 to +180° | Right axis deviation |
| -90 to -180° | Extreme axis |

---

# 7. Outcome Variables

Binary outcome labels (0/1) derived from the event log.

## 7.1 Hospitalization-Level Outcomes

| Variable | Data Type | Description | Definition |
|----------|-----------|-------------|------------|
| **death_hosp** | INTEGER | In-hospital death | Any DEATH event during hospitalization |
| **cardiac_arrest_hosp** | INTEGER | Cardiac arrest during hospitalization | ICD-based, no time window |
| **acs_hosp** | INTEGER | ACS during hospitalization | ICD-based, no time window |
| **pci_hosp** | INTEGER | PCI during hospitalization | ICD-based, no time window |
| **cabg_hosp** | INTEGER | CABG during hospitalization | ICD-based, no time window |
| **revasc_hosp** | INTEGER | Any revascularization during hospitalization | pci_hosp OR cabg_hosp |

## 7.2 Prediction-Time-Aligned Outcomes

All timed outcomes use the naming convention `{event}_{horizon}h_from_{window}`.

**Rule:** `hours_from_ed > P AND hours_from_ed <= P + H`

| Variable Pattern | Events | Horizons | Windows | Total Columns |
|-----------------|--------|----------|---------|---------------|
| `icu_{H}h_from_{W}` | ICU_ADMIT | 24h, 48h | w1, w6, w24 | 6 |
| `pressor_{H}h_from_{W}` | PRESSOR_START | 24h, 48h | w1, w6, w24 | 6 |
| `vent_{H}h_from_{W}` | VENT_START | 24h, 48h | w1, w6, w24 | 6 |
| `rrt_24h_from_{W}` | RRT_START | 24h | w1, w6, w24 | 3 |
| `death_{H}h_from_{W}` | DEATH | 24h, 48h, 72h | w1, w6, w24 | 9 |
| `deterioration_{H}h_from_{W}` | Composite | 24h, 48h | w1, w6, w24 | 6 |
| **Total** | | | | **36** |

## 7.3 Event-By Flags

Binary flags indicating whether an event already occurred within the feature window.

**Rule:** `hours_from_ed <= P`

| Variable Pattern | Description | Total |
|-----------------|-------------|-------|
| `event_by_icu_{W}` | ICU admission by window end | 3 |
| `event_by_pressor_{W}` | Pressor start by window end | 3 |
| `event_by_vent_{W}` | Ventilation by window end | 3 |
| `event_by_rrt_{W}` | RRT by window end | 3 |
| `event_by_death_{W}` | Death by window end | 3 |
| `event_by_deterioration_{W}` | Any critical event by window end | 3 |
| **Total** | | **18** |

## 7.3 Time-to-Event Variables

| Variable | Data Type | Units | Description |
|----------|-----------|-------|-------------|
| **time_to_icu** | FLOAT | hours | Time from ED arrival to first ICU admission |
| **time_to_death** | FLOAT | hours | Time from ED arrival to death |
| **time_to_deterioration** | FLOAT | hours | Time from ED arrival to first critical event |

---

# 8. Derived Variables

Variables computed from raw measurements.

## 8.1 Vital Sign Derived Variables

| Variable | Data Type | Formula | Description | Clinical Interpretation |
|----------|-----------|---------|-------------|------------------------|
| **sbp_cv_1h** | FLOAT | sbp_std_1h / sbp_mean_1h | Coefficient of variation of SBP (1h) | Higher CV suggests hemodynamic instability |
| **sbp_cv_6h** | FLOAT | sbp_std_6h / sbp_mean_6h | Coefficient of variation of SBP (6h) | Higher CV suggests hemodynamic instability |
| **sbp_cv_24h** | FLOAT | sbp_std_24h / sbp_mean_24h | Coefficient of variation of SBP (24h) | Higher CV suggests hemodynamic instability |
| **hr_range_1h** | FLOAT | hr_max_1h - hr_min_1h | Heart rate range (1h) | Larger range may indicate arrhythmia or stress |
| **hr_range_6h** | FLOAT | hr_max_6h - hr_min_6h | Heart rate range (6h) | Larger range may indicate arrhythmia or stress |
| **hr_range_24h** | FLOAT | hr_max_24h - hr_min_24h | Heart rate range (24h) | Larger range may indicate arrhythmia or stress |

## 8.2 Laboratory Derived Variables (W24 Only)

| Variable | Data Type | Formula | Description | Clinical Interpretation |
|----------|-----------|---------|-------------|------------------------|
| **bun_creatinine_ratio** | FLOAT | bun_first_24h / creatinine_first_24h | BUN-to-creatinine ratio | >20 suggests pre-renal azotemia (dehydration/hypoperfusion), <10 suggests intrinsic renal disease |
| **lactate_delta_24h** | FLOAT | lactate_max_24h - lactate_first_24h | Change in lactate over 24h | Positive = worsening perfusion/sepsis, Negative = improving condition |

---

# 9. Missing Indicator Variables

Binary flags (0/1) indicating missing data for key variables.

## Purpose

Missing indicators allow models to:
1. Learn patterns of missingness (informative missingness)
2. Distinguish between "not measured" and "normal value"
3. Handle imputation appropriately

## Variables

| Variable | Data Type | Description | Missing Rate |
|----------|-----------|-------------|--------------|
| **missing_lactate_1h** | INTEGER | 1 if lactate_first_1h is NULL | ~90% |
| **missing_lactate_6h** | INTEGER | 1 if lactate_first_6h is NULL | ~79% |
| **missing_lactate_24h** | INTEGER | 1 if lactate_first_24h is NULL | ~75% |
| **missing_troponin_1h** | INTEGER | 1 if troponin_first_1h is NULL | ~85% |
| **missing_troponin_6h** | INTEGER | 1 if troponin_first_6h is NULL | ~74% |
| **missing_troponin_24h** | INTEGER | 1 if troponin_first_24h is NULL | ~70% |
| **missing_ecg_w1** | INTEGER | 1 if no ECG within 1 hour window | 73.4% |
| **missing_ecg_w6** | INTEGER | 1 if no ECG within 6 hour window | 65.7% |

## Clinical Interpretation

| Pattern | Possible Meaning |
|---------|------------------|
| missing_lactate = 1 | Patient not critically ill (no lactate ordered) |
| missing_troponin = 1 | No cardiac concern (troponin not ordered) |
| missing_ecg = 1 | Non-cardiac chief complaint |

---

# 10. Event Log Variables

Variables in the `tmp_ed_event_log` table.

| Variable | Data Type | Description | Example Values |
|----------|-----------|-------------|----------------|
| **stay_id** | INTEGER | ED stay identifier | 30001234 |
| **event_type** | VARCHAR(50) | Category of clinical event | ICU_ADMIT, PRESSOR_START, VENT_START, RRT_START, CARDIAC_ARREST, DEATH, ACS, PCI, CABG |
| **event_time** | TIMESTAMP | Time of event occurrence (NULL for ICD-based events) | 2150-01-16 02:30:00 |
| **event_time_type** | VARCHAR(10) | Temporal precision: 'exact', 'date', or 'none' | exact |
| **event_source** | VARCHAR(100) | MIMIC table that provided the event | icustays, inputevents, procedureevents, diagnoses_icd, procedures_icd |
| **event_detail** | TEXT | Additional context (optional) | MICU, norepinephrine, I21.0 |

## Event Types Reference

| event_type | Description | Source Table | Definition |
|------------|-------------|--------------|------------|
| ICU_ADMIT | ICU admission | mimiciv_icu.icustays | First ICU intime |
| PRESSOR_START | Vasopressor initiation | mimiciv_icu.inputevents | First vasopressor infusion |
| VENT_START | Mechanical ventilation | mimiciv_icu.procedureevents | First ventilation start |
| RRT_START | Renal replacement therapy | mimiciv_icu.procedureevents | First dialysis/CRRT |
| CARDIAC_ARREST | Cardiac arrest (hosp-level) | mimiciv_hosp.diagnoses_icd | ICD codes for arrest (event_time = NULL) |
| DEATH | Death | mimiciv_hosp.patients | dod timestamp |
| ACS | Acute coronary syndrome (hosp-level) | mimiciv_hosp.diagnoses_icd | ICD codes for MI/NSTEMI/UA (event_time = NULL) |
| PCI | Percutaneous intervention (hosp-level) | mimiciv_hosp.procedures_icd | ICD codes for coronary stent (event_time = NULL) |
| CABG | Coronary bypass (hosp-level) | mimiciv_hosp.procedures_icd | ICD codes for bypass surgery (event_time = NULL) |

---

### SQL sources for derived time variables

- `sql/00_base_ed_cohort.sql`: computes `ed_los_hours` as `EXTRACT(EPOCH FROM (ed_outtime - ed_intime)) / 3600.0`.
- `sql/20_outcomes_from_event_log.sql`: computes `hours_from_ed = EXTRACT(EPOCH FROM (event_time - ed_intime)) / 3600.0` and derives `time_to_icu`, `time_to_death`, and `time_to_deterioration` using `MIN(CASE WHEN ... THEN hours_from_ed END)`.
- `sql/33_ecg_features_w1.sql` and `sql/34_ecg_features_w6.sql`: compute `ecg_hours_from_ed_w1` and `ecg_hours_from_ed_w6` as `EXTRACT(EPOCH FROM (r.ecg_time - b.ed_intime)) / 3600.0`.

# Window-Specific Variable Summary

## W1 Features (First Hour: `tmp_features_w1`)

| Variable | Type | Description |
|----------|------|-------------|
| **stay_id** | INTEGER | Primary key |
| **age_at_ed** | FLOAT | Age at ED visit |
| **gender** | CHAR(1) | Patient gender |
| **arrival_transport** | TEXT | Arrival mode (ambulance, walk-in, etc.) |
| **race** | TEXT | Race/ethnicity |
| **chiefcomplaint** | TEXT | Chief complaint text |
| **temp_w1** | FLOAT | Temperature (°F) - triage or first charted |
| **hr_w1** | FLOAT | Heart rate (bpm) - triage or first charted |
| **rr_w1** | FLOAT | Respiratory rate (breaths/min) |
| **spo2_w1** | FLOAT | Oxygen saturation (%) |
| **sbp_w1** | FLOAT | Systolic blood pressure (mmHg) |
| **dbp_w1** | FLOAT | Diastolic blood pressure (mmHg) |
| **triage_pain** | FLOAT | Triage pain score (0-10) |
| **triage_acuity** | INTEGER | Triage acuity level (1-5) |
| **shock_index_w1** | FLOAT | Shock index (HR/SBP) |
| **map_w1** | FLOAT | Mean arterial pressure |
| **missing_temp_w1** | INTEGER | Missing temperature indicator |
| **missing_hr_w1** | INTEGER | Missing heart rate indicator |
| **missing_sbp_w1** | INTEGER | Missing blood pressure indicator |

**Total Variables: 19**

## W6 Features (First 6 Hours: `tmp_features_w6`)

| Variable | Type | Description |
|----------|------|-------------|
| **stay_id** | INTEGER | Primary key |
| **age_at_ed** | FLOAT | Age at ED visit |
| **gender** | CHAR(1) | Patient gender |
| **arrival_transport** | TEXT | Arrival mode |
| **race** | TEXT | Race/ethnicity |
| **sbp_min_6h** | FLOAT | Minimum SBP in first 6h |
| **sbp_max_6h** | FLOAT | Maximum SBP in first 6h |
| **sbp_mean_6h** | FLOAT | Mean SBP in first 6h |
| **sbp_std_6h** | FLOAT | SBP standard deviation |
| **dbp_min_6h** | FLOAT | Minimum DBP in first 6h |
| **hr_min_6h** | FLOAT | Minimum HR in first 6h |
| **hr_max_6h** | FLOAT | Maximum HR in first 6h |
| **hr_mean_6h** | FLOAT | Mean HR in first 6h |
| **hr_std_6h** | FLOAT | HR standard deviation |
| **rr_max_6h** | FLOAT | Maximum RR in first 6h |
| **rr_mean_6h** | FLOAT | Mean RR in first 6h |
| **spo2_min_6h** | FLOAT | Minimum SpO2 in first 6h |
| **spo2_mean_6h** | FLOAT | Mean SpO2 in first 6h |
| **temp_max_6h** | FLOAT | Maximum temperature in first 6h |
| **temp_min_6h** | FLOAT | Minimum temperature in first 6h |
| **n_vitalsign_measurements_6h** | INTEGER | Number of vital sign measurements |
| **chiefcomplaint** | TEXT | Chief complaint text |
| **lactate_first_6h** | FLOAT | First lactate value (mmol/L) |
| **troponin_first_6h** | FLOAT | First troponin value (ng/mL) |
| **is_hs_troponin_6h** | INTEGER | High-sensitivity troponin flag (1=yes) |
| **creatinine_first_6h** | FLOAT | First creatinine (mg/dL) |
| **potassium_first_6h** | FLOAT | First potassium (mEq/L) |
| **sodium_first_6h** | FLOAT | First sodium (mEq/L) |
| **bicarbonate_first_6h** | FLOAT | First bicarbonate (mEq/L) |
| **wbc_first_6h** | FLOAT | First WBC count (K/μL) |
| **hemoglobin_first_6h** | FLOAT | First hemoglobin (g/dL) |
| **platelet_first_6h** | FLOAT | First platelet count (K/μL) |
| **ed_los_hours** | FLOAT | ED length of stay (hours) |
| **time_to_first_lab_hours** | FLOAT | Time to first lab draw (hours) |
| **time_to_first_med_hours** | FLOAT | Time to first medication (hours) |
| **prev_admits_1yr** | INTEGER | Previous admissions in past year |
| **prev_ed_visits_1yr** | INTEGER | Previous ED visits in past year |
| **sbp_cv_6h** | FLOAT | SBP coefficient of variation |
| **hr_range_6h** | FLOAT | HR range (max - min) |
| **missing_lactate_6h** | INTEGER | Missing lactate indicator |
| **missing_troponin_6h** | INTEGER | Missing troponin indicator |

**Total Variables: 40**

## W24 Features (First 24 Hours: `tmp_features_w24`)

| Variable | Type | Description |
|----------|------|-------------|
| **stay_id** | INTEGER | Primary key |
| **age_at_ed** | FLOAT | Age at ED visit |
| **gender** | CHAR(1) | Patient gender |
| **arrival_transport** | TEXT | Arrival mode |
| **race** | TEXT | Race/ethnicity |
| **sbp_min_24h** | FLOAT | Minimum SBP in first 24h |
| **sbp_max_24h** | FLOAT | Maximum SBP in first 24h |
| **sbp_mean_24h** | FLOAT | Mean SBP in first 24h |
| **sbp_std_24h** | FLOAT | SBP standard deviation |
| **hr_min_24h** | FLOAT | Minimum HR in first 24h |
| **hr_max_24h** | FLOAT | Maximum HR in first 24h |
| **hr_mean_24h** | FLOAT | Mean HR in first 24h |
| **rr_max_24h** | FLOAT | Maximum RR in first 24h |
| **rr_mean_24h** | FLOAT | Mean RR in first 24h |
| **spo2_min_24h** | FLOAT | Minimum SpO2 in first 24h |
| **spo2_mean_24h** | FLOAT | Mean SpO2 in first 24h |
| **temp_max_24h** | FLOAT | Maximum temperature in first 24h |
| **n_vitalsign_measurements_24h** | INTEGER | Number of vital sign measurements |
| **chiefcomplaint** | TEXT | Chief complaint text |
| **lactate_first_24h** | FLOAT | First lactate value (mmol/L) |
| **troponin_first_24h** | FLOAT | First troponin value (ng/mL) |
| **is_hs_troponin_24h** | INTEGER | High-sensitivity troponin flag |
| **creatinine_first_24h** | FLOAT | First creatinine (mg/dL) |
| **potassium_first_24h** | FLOAT | First potassium (mEq/L) |
| **sodium_first_24h** | FLOAT | First sodium (mEq/L) |
| **bicarbonate_first_24h** | FLOAT | First bicarbonate (mEq/L) |
| **wbc_first_24h** | FLOAT | First WBC count (K/μL) |
| **hemoglobin_first_24h** | FLOAT | First hemoglobin (g/dL) |
| **platelet_first_24h** | FLOAT | First platelet count (K/μL) |
| **glucose_first_24h** | FLOAT | First glucose (mg/dL) |
| **bun_first_24h** | FLOAT | First BUN (mg/dL) |
| **bilirubin_first_24h** | FLOAT | First bilirubin (mg/dL) |
| **inr_first_24h** | FLOAT | First INR |
| **lactate_max_24h** | FLOAT | Maximum lactate in 24h |
| **creatinine_max_24h** | FLOAT | Maximum creatinine in 24h |
| **ed_los_hours** | FLOAT | ED length of stay (hours) |
| **time_to_first_lab_hours** | FLOAT | Time to first lab draw (hours) |
| **time_to_first_med_hours** | FLOAT | Time to first medication (hours) |
| **prev_admits_1yr** | INTEGER | Previous admissions in past year |
| **prev_ed_visits_1yr** | INTEGER | Previous ED visits in past year |
| **bun_creatinine_ratio** | FLOAT | BUN/Creatinine ratio |
| **lactate_delta_24h** | FLOAT | Lactate change (max - first) |

**Total Variables: 41**

# 11. Variable Summary Statistics


## 11.1 Numeric Variables (6h Window)

| Variable | N | Mean | Std | Min | P25 | P50 | P75 | Max | Missing % |
|----------|---|------|-----|-----|-----|-----|-----|-----|-----------|
| age_at_ed | 424,952 | 55.2 | 20.1 | 18.0 | 38.0 | 56.0 | 71.0 | 110.0 | 0.0% |
| ed_los_hours | 424,952 | 6.8 | 7.2 | 0.0 | 3.1 | 5.2 | 8.4 | 720.0 | 0.0% |
| sbp_mean_6h | 401,250 | 128.5 | 22.3 | 40.0 | 113.0 | 126.0 | 141.0 | 260.0 | 5.6% |
| hr_mean_6h | 401,102 | 85.2 | 18.7 | 30.0 | 72.0 | 83.0 | 96.0 | 220.0 | 5.6% |
| rr_mean_6h | 398,756 | 18.2 | 4.1 | 8.0 | 16.0 | 18.0 | 20.0 | 60.0 | 6.2% |
| spo2_mean_6h | 400,012 | 96.8 | 2.8 | 50.0 | 96.0 | 98.0 | 99.0 | 100.0 | 5.9% |
| lactate_first_6h | 89,240 | 2.1 | 2.0 | 0.3 | 1.1 | 1.6 | 2.4 | 30.0 | 79.0% |
| troponin_first_6h | 110,487 | 0.18 | 1.52 | 0.00 | 0.01 | 0.02 | 0.06 | 50.0 | 74.0% |
| ecg_hr_w6 | 145,790 | 78.5 | 19.2 | 30.0 | 65.0 | 76.0 | 89.0 | 220.0 | 65.7% |
| ecg_qrs_dur_w6 | 145,678 | 98.2 | 22.5 | 40.0 | 86.0 | 94.0 | 106.0 | 280.0 | 65.7% |

## 11.2 Outcome Rates

Rates are reported for the primary aligned outcomes (W6, 24h horizon) and hospitalization-level labels.

| Outcome | Rate | Description |
|---------|------|-------------|
| icu_24h_from_w1 | 6.27% | ICU admission (1-25h from ED) |
| icu_24h_from_w6 | 2.48% | ICU admission (6-30h from ED) |
| icu_24h_from_w24 | 0.40% | ICU admission (24-48h from ED) |
| deterioration_24h_from_w1 | 6.35% | Composite deterioration (1-25h from ED) |
| deterioration_24h_from_w6 | 4.05% | Composite deterioration (6-30h from ED) |
| deterioration_24h_from_w24 | 0.82% | Composite deterioration (24-48h from ED) |
| deterioration_48h_from_w24 | 1.19% | Composite deterioration (24-72h from ED) |
| death_24h_from_w6 | 0.12% | Death (6-30h from ED) |
| death_hosp | 0.98% | In-hospital death |
| cardiac_arrest_hosp | 0.11% | Cardiac arrest (ICD) |
| acs_hosp | 1.45% | ACS (ICD) |
| revasc_hosp | 0.30% | Revascularization (ICD) |
| coronary_event_hosp | 1.54% | Any coronary event (ICD) |

> **Updated Feb 20, 2026** from pipeline run on 424,385 visits. All timed outcomes are prediction-aligned.

---

---

# PART 3: SQL PIPELINE DOCUMENTATION

# SQL Pipeline Documentation

---

This document provides detailed documentation for all 16 SQL scripts in the MIMIC deterioration pipeline.

---

## Table of Contents

1. [SQL Overview](#1-sql-overview)
2. [00_base_ed_cohort.sql](#2-00_base_ed_cohortsql)
3. [Event Extraction Scripts (10-17)](#3-event-extraction-scripts-10-17)
4. [20_outcomes_from_event_log.sql](#4-20_outcomes_from_event_logsql)
5. [Feature Extraction Scripts (30-32)](#5-feature-extraction-scripts-30-32)
6. [ECG Feature Scripts (33-34)](#6-ecg-feature-scripts-33-34)
7. [99_qa_checks.sql](#7-99_qa_checkssql)
8. [SQL Template Variables](#8-sql-template-variables)
9. [Query Optimization Notes](#9-query-optimization-notes)

---

# 1. SQL Overview

## Execution Order

```
00_base_ed_cohort.sql           →  tmp_base_ed_cohort
        │
        ▼
10_event_icu_admit.sql          →  
11_event_pressors.sql           →  
12_event_ventilation.sql        →  INSERT INTO tmp_ed_event_log
13_event_rrt.sql                →  
14_event_acs.sql                →  
15_event_revasc.sql             →  
16_event_cardiac_arrest.sql     →  
17_event_death.sql              →  
        │
        ▼
20_outcomes_from_event_log.sql  →  tmp_ed_outcomes
        │
        ▼
30_features_w1.sql              →  tmp_features_w1
31_features_w6.sql              →  tmp_features_w6
32_features_w24.sql             →  tmp_features_w24
        │
        ▼
33_ecg_features_w1.sql          →  tmp_ecg_features_w1
34_ecg_features_w6.sql          →  tmp_ecg_features_w6
        │
        ▼
99_qa_checks.sql                →  8 assertion queries (in-memory)
```

## Template Engine

All SQL files use **Jinja2** templating for:
- Schema name substitution
- Table name configuration
- Dynamic time windows

---

# 2. 00_base_ed_cohort.sql

## Purpose

Creates the foundational cohort of adult ED visits from MIMIC-IV.

## Output Table

**Table:** `tmp_base_ed_cohort`

## SQL Logic

```sql
-- Full annotated version of 00_base_ed_cohort.sql

DROP TABLE IF EXISTS {{ tables.base_ed_cohort }};

CREATE TABLE {{ tables.base_ed_cohort }} AS
SELECT
    -- Primary key
    ed.stay_id,
    
    -- Foreign keys
    ed.subject_id,
    ed.hadm_id,                    -- NULL if patient not admitted
    
    -- Timestamps
    ed.intime  AS ed_intime,       -- ED arrival (T=0)
    ed.outtime AS ed_outtime,      -- ED departure
    
    -- Demographics from patients table
    pt.anchor_age,
    pt.anchor_year,
    pt.gender,
    pt.dod,                        -- Date of death (if known)
    
    -- Calculated fields
    -- Age at ED visit = anchor_age + (ED year - anchor_year)
    pt.anchor_age 
      + EXTRACT(YEAR FROM ed.intime) 
      - pt.anchor_year AS age_at_ed,
    
    -- ED length of stay in hours
    EXTRACT(EPOCH FROM (ed.outtime - ed.intime)) / 3600.0 AS ed_los_hours,
    
    -- Hospital admission flag
    CASE WHEN ed.hadm_id IS NOT NULL THEN 1 ELSE 0 END AS was_admitted

FROM {{ schemas.ed }}.edstays ed
INNER JOIN {{ schemas.hosp }}.patients pt
    ON ed.subject_id = pt.subject_id

WHERE
    -- Valid timestamps
    ed.outtime > ed.intime
    
    -- Adult filter (age >= 18)
    AND (
        pt.anchor_age 
        + EXTRACT(YEAR FROM ed.intime) 
        - pt.anchor_year
    ) >= {{ cohort.min_age }}
    
    -- Age upper bound (remove implausible ages)
    AND (
        pt.anchor_age 
        + EXTRACT(YEAR FROM ed.intime) 
        - pt.anchor_year
    ) <= 110
    
    -- Alive at ED arrival (exclude already-deceased patients)
    AND (pt.dod IS NULL OR ed.intime <= pt.dod::timestamp);
```

## Key Design Decisions

### Age Calculation

MIMIC-IV uses an "anchor" system to protect patient privacy:
- `anchor_age`: Patient's age at `anchor_year`
- `anchor_year`: A reference year (not the actual year)

To calculate age at any event:
```
age_at_event = anchor_age + (event_year - anchor_year)
```

### Age Upper Bound (v2.1)

The cohort now applies `age_at_ed <= 110` to exclude implausible ages caused by anchor-year arithmetic edge cases. The observed maximum in the current cohort is 103.

### Alive-at-Arrival Filter (v2.1)

The cohort now requires `dod IS NULL OR ed_intime <= dod` to exclude patients whose recorded death date precedes the ED arrival. This removes data-quality artifacts and ensures all patients are alive at the start of the observation window.

### Admission Flag

The `was_admitted` column indicates whether the ED visit resulted in hospital admission:
- `1` = Patient was admitted to the hospital
- `0` = Patient was discharged from ED

This allows filtering for admitted-only (hospitalized patients) analyses.

### ED Length of Stay

Calculated as:
```
ed_los_hours = (ed_outtime - ed_intime) in seconds / 3600
```

## Output Statistics

| Metric | Value |
|--------|-------|
| Total rows | 424,385 |
| Unique patients | 205,080 |
| Admitted | 202,184 (47.6%) |
| Mean age | 52.8 years |
| Age range | 18–103 |
| Mean ED LOS | 7.2 hours |

---

# 3. Event Extraction Scripts (10-17)

All event scripts follow a common pattern:
1. Query source MIMIC table
2. Join to base cohort
3. Insert into event log with standardized schema

## Event Log Schema

```sql
CREATE TABLE tmp_ed_event_log (
    stay_id          INTEGER,
    event_type       VARCHAR(50),
    event_time       TIMESTAMP,
    event_source     VARCHAR(100),
    event_detail     TEXT,
    event_time_type  TEXT          -- 'exact', 'day', or 'none'
);
```

> **Note (v2.1):** The `event_time_type` column ensures consumers can distinguish events with precise timestamps (`'exact'`), date-level precision (`'day'` for death events from `dod`), and ICD-coded events that have no timestamp (`'none'` for ACS, revascularization, cardiac arrest).

---

## 3.1 10_event_icu_admit.sql

### Purpose
Extracts ICU admission events.

### Source
`mimiciv_icu.icustays`

### SQL Logic

```sql
INSERT INTO {{ tables.event_log }}
SELECT
    bc.stay_id,
    'ICU_ADMIT' AS event_type,
    icu.intime AS event_time,
    'icustays' AS event_source,
    icu.first_careunit AS event_detail,
    'exact'::text AS event_time_type
FROM {{ tables.base_ed_cohort }} bc
INNER JOIN {{ schemas.icu }}.icustays icu
    ON bc.hadm_id = icu.hadm_id
WHERE
    -- Only events AFTER ED arrival (causal ordering)
    icu.intime >= bc.ed_intime;
```

### Key Points

- Joins via `hadm_id` (hospital admission ID)
- `first_careunit` provides ICU type (MICU, SICU, CCU, etc.)
- Only includes ICU transfers AFTER ED arrival and within reasonable window
- `event_time_type = 'exact'` indicates precise timestamp available

---

## 3.2 11_event_pressors.sql

### Purpose
Extracts vasopressor initiation events.

### Source
`mimiciv_icu.inputevents`

### Vasopressor Item IDs

| itemid | Drug |
|--------|------|
| 221906 | Norepinephrine |
| 221289 | Epinephrine |
| 221662 | Dopamine |
| 222315 | Vasopressin |
| 221749 | Phenylephrine |

### SQL Logic

```sql
INSERT INTO {{ tables.event_log }}
SELECT DISTINCT
    bc.stay_id,
    'PRESSOR_START' AS event_type,
    MIN(ie.starttime) AS event_time,      -- First pressor start
    'inputevents' AS event_source,
    NULL AS event_detail
FROM {{ tables.base_ed_cohort }} bc
INNER JOIN {{ schemas.icu }}.inputevents ie
    ON bc.hadm_id = ie.hadm_id
WHERE
    ie.itemid IN (221906, 221289, 221662, 222315, 221749)
    AND ie.starttime >= bc.ed_intime
GROUP BY bc.stay_id;
```

### Key Points

- Uses `MIN(starttime)` to capture FIRST vasopressor
- Groups by `stay_id` for one event per ED visit
- Requires ICU admission (joins via `hadm_id`)

---

## 3.3 12_event_ventilation.sql

### Purpose
Extracts mechanical ventilation events.

### Source
`mimiciv_icu.procedureevents`

### SQL Logic

```sql
INSERT INTO {{ tables.event_log }}
SELECT DISTINCT
    bc.stay_id,
    'VENT_START' AS event_type,
    MIN(pe.starttime) AS event_time,
    'procedureevents' AS event_source,
    NULL AS event_detail
FROM {{ tables.base_ed_cohort }} bc
INNER JOIN {{ schemas.icu }}.procedureevents pe
    ON bc.hadm_id = pe.hadm_id
WHERE
    pe.itemid IN (
        225792,  -- Invasive ventilation
        225794   -- Non-invasive ventilation
    )
    AND pe.starttime >= bc.ed_intime
GROUP BY bc.stay_id;
```

---

## 3.4 13_event_rrt.sql

### Purpose
Extracts renal replacement therapy (dialysis/CRRT) events.

### Source
`mimiciv_icu.procedureevents`

### SQL Logic

```sql
INSERT INTO {{ tables.event_log }}
SELECT DISTINCT
    bc.stay_id,
    'RRT_START' AS event_type,
    MIN(pe.starttime) AS event_time,
    'procedureevents' AS event_source,
    NULL AS event_detail
FROM {{ tables.base_ed_cohort }} bc
INNER JOIN {{ schemas.icu }}.procedureevents pe
    ON bc.hadm_id = pe.hadm_id
WHERE
    pe.itemid IN (
        225802,  -- Dialysis - CRRT
        225803,  -- Dialysis - CVVHD
        225809,  -- Dialysis - CVVHDF
        225955,  -- Hemodialysis
        225441   -- IHD
    )
    AND pe.starttime >= bc.ed_intime
GROUP BY bc.stay_id;
```

---

## 3.5 14_event_acs.sql

### Purpose
Extracts Acute Coronary Syndrome diagnoses.

### Source
`mimiciv_hosp.diagnoses_icd`

### ICD Codes

| ICD-10 | Description |
|--------|-------------|
| I21.% | STEMI |
| I22.% | Subsequent MI |
| I200 | Unstable angina |
| I24.% | Other acute ischemic heart disease |

| ICD-9 | Description |
|-------|-------------|
| 410.% | Acute MI |
| 411.1 | Unstable angina |

### SQL Logic

```sql
INSERT INTO {{ tables.event_log }}
SELECT DISTINCT
    bc.stay_id,
    'ACS' AS event_type,
    NULL::timestamp AS event_time,        -- Hospitalization-level: no timestamp available
    'none' AS event_time_type,
    'diagnoses_icd (hospitalization-level)' AS event_source,
    diag.icd_code AS event_detail
FROM {{ tables.base_ed_cohort }} bc
INNER JOIN {{ schemas.hosp }}.diagnoses_icd diag
    ON bc.hadm_id = diag.hadm_id
WHERE
    -- ICD-10 codes
    (diag.icd_version = 10 AND (
        diag.icd_code LIKE 'I21%' OR       -- STEMI
        diag.icd_code LIKE 'I22%' OR       -- Subsequent MI
        diag.icd_code = 'I200' OR          -- Unstable angina
        diag.icd_code LIKE 'I24%'          -- Other acute IHD
    ))
    OR
    -- ICD-9 codes
    (diag.icd_version = 9 AND (
        diag.icd_code LIKE '410%' OR       -- Acute MI
        diag.icd_code = '4111'             -- Unstable angina
    ));
```

### Note on Event Time

Diagnosis codes don't have precise timestamps in MIMIC-IV. Event time is set to NULL and `event_time_type` = 'none'. These outcomes are treated as hospitalization-level binary indicators.

---

## 3.6 15_event_revasc.sql

### Purpose
Extracts coronary revascularization procedures (PCI, CABG).

### Source
`mimiciv_hosp.procedures_icd`

### ICD-10-PCS Codes

| Code Pattern | Description |
|--------------|-------------|
| 027% | Dilation of coronary artery (PCI) |
| 021% | Bypass coronary artery (CABG) |

### SQL Logic

```sql
-- PCI Events
INSERT INTO {{ tables.event_log }}
SELECT DISTINCT
    bc.stay_id,
    'PCI' AS event_type,
    NULL::timestamp AS event_time,        -- Hospitalization-level: no timestamp available
    'none' AS event_time_type,
    'procedures_icd (hospitalization-level)' AS event_source,
    proc.icd_code AS event_detail
FROM {{ tables.base_ed_cohort }} bc
INNER JOIN {{ schemas.hosp }}.procedures_icd proc
    ON bc.hadm_id = proc.hadm_id
WHERE
    proc.icd_version = 10 
    AND proc.icd_code LIKE '027%';

-- CABG Events
INSERT INTO {{ tables.event_log }}
SELECT DISTINCT
    bc.stay_id,
    'CABG' AS event_type,
    NULL::timestamp AS event_time,        -- Hospitalization-level: no timestamp available
    'none' AS event_time_type,
    'procedures_icd (hospitalization-level)' AS event_source,
    proc.icd_code AS event_detail
FROM {{ tables.base_ed_cohort }} bc
INNER JOIN {{ schemas.hosp }}.procedures_icd proc
    ON bc.hadm_id = proc.hadm_id
WHERE
    proc.icd_version = 10 
    AND proc.icd_code LIKE '021%';
```

---

## 3.7 16_event_cardiac_arrest.sql

### Purpose
Extracts cardiac arrest events.

### Source
`mimiciv_hosp.diagnoses_icd`

### ICD Codes

| ICD-10 | Description |
|--------|-------------|
| I46.0 | Cardiac arrest with successful resuscitation |
| I46.1 | Sudden cardiac death |
| I46.9 | Cardiac arrest, unspecified |
| I46.2 | Cardiac arrest due to underlying condition |
| I49.0 | Ventricular fibrillation |
| I49.01 | VF |
| I49.02 | Ventricular flutter |

| ICD-9 | Description |
|-------|-------------|
| 427.5 | Cardiac arrest |
| 427.41 | Ventricular fibrillation |

### SQL Logic

```sql
INSERT INTO {{ tables.event_log }}
SELECT DISTINCT
    bc.stay_id,
    'CARDIAC_ARREST' AS event_type,
    NULL::timestamp AS event_time,        -- Hospitalization-level: no timestamp available
    'none' AS event_time_type,
    'diagnoses_icd (hospitalization-level)' AS event_source,
    diag.icd_code AS event_detail
FROM {{ tables.base_ed_cohort }} bc
INNER JOIN {{ schemas.hosp }}.diagnoses_icd diag
    ON bc.hadm_id = diag.hadm_id
WHERE
    (diag.icd_version = 10 AND diag.icd_code IN (
        'I460', 'I461', 'I469', 'I462',
        'I490', 'I4901', 'I4902'
    ))
    OR
    (diag.icd_version = 9 AND diag.icd_code IN (
        '4275', '42741'
    ));
```

---

## 3.8 17_event_death.sql

### Purpose
Extracts death events.

### Source
`mimiciv_hosp.admissions` (deathtime), `mimiciv_hosp.patients` (dod fallback)

### SQL Logic

```sql
INSERT INTO {{ tables.event_log }}
SELECT DISTINCT
    bc.stay_id,
    'DEATH' AS event_type,
    COALESCE(adm.deathtime, pt.dod::timestamp) AS event_time,
    CASE WHEN adm.deathtime IS NOT NULL
         THEN 'hosp.admissions.deathtime'
         ELSE 'hosp.patients.dod'
    END AS event_source,
    'In-hospital death' AS event_detail,
    CASE WHEN adm.deathtime IS NOT NULL
         THEN 'exact'::text
         ELSE 'day'::text
    END AS event_time_type
FROM {{ tables.base_ed_cohort }} bc
INNER JOIN {{ schemas.hosp }}.patients pt
    ON bc.subject_id = pt.subject_id
LEFT JOIN {{ schemas.hosp }}.admissions adm
    ON bc.hadm_id = adm.hadm_id
   AND adm.deathtime IS NOT NULL
WHERE
    (adm.deathtime IS NOT NULL
     OR pt.dod IS NOT NULL)
    AND COALESCE(adm.deathtime, pt.dod::timestamp) >= bc.ed_intime;
```

### Note

- Prefers `admissions.deathtime` (exact timestamp, `event_time_type = 'exact'`) over `patients.dod` (date only, `event_time_type = 'day'`).
- `dod` is a DATE, not TIMESTAMP. Death time precision is limited to the day when deathtime is unavailable.
- The `event_source` column records which source was used.

---

# 4. 20_outcomes_from_event_log.sql

## Purpose

Aggregates events from the event log into binary outcome flags for each ED stay.

## Output Table

**Table:** `tmp_ed_outcomes`

## SQL Logic (Annotated)

```sql
DROP TABLE IF EXISTS {outcomes};

CREATE TABLE {outcomes} AS
WITH
-- Base cohort for anchoring ED arrival times
base AS (
  SELECT stay_id, subject_id, hadm_id, ed_intime, ed_outtime
  FROM {base_ed_cohort}
),
-- Timed events only (have an exact event_time)
ev AS (
  SELECT e.stay_id, e.event_type, e.event_time,
         EXTRACT(EPOCH FROM (e.event_time - b.ed_intime)) / 3600.0 AS hours_from_ed
  FROM {event_log} e
  JOIN base b USING (stay_id)
  WHERE e.event_time IS NOT NULL
    -- Defensive: event must be after ED arrival
    AND e.event_time >= b.ed_intime
    -- Defensive: cap at 30 days to exclude implausible late events
    AND e.event_time <= b.ed_intime + INTERVAL '30 days'
),
-- ICD/procedure events: no timestamp => binary hospitalization-level only
icd_ev AS (
  SELECT DISTINCT e.stay_id, e.event_type
  FROM {event_log} e
  WHERE e.event_time IS NULL
),
-- Aggregate timed outcomes
ev_agg AS (
  SELECT stay_id,
    -- Hospitalization-level mortality
    MAX(CASE WHEN event_type = 'DEATH' THEN 1 ELSE 0 END) AS death_hosp,
    -- Time-to-event
    MIN(CASE WHEN event_type = 'ICU_ADMIT' THEN hours_from_ed END) AS time_to_icu,
    MIN(CASE WHEN event_type = 'DEATH'     THEN hours_from_ed END) AS time_to_death,
    MIN(CASE WHEN event_type IN ('ICU_ADMIT','PRESSOR_START','VENT_START','RRT_START','DEATH')
              THEN hours_from_ed END) AS time_to_deterioration,

    -- PREDICTION-TIME-ALIGNED OUTCOMES
    -- Rule: hours_from_ed > P AND hours_from_ed <= P + H
    -- ICU aligned (24h + 48h horizons x 3 windows = 6 columns)
    MAX(CASE WHEN event_type='ICU_ADMIT' AND hours_from_ed > 1  AND hours_from_ed <= 25 THEN 1 ELSE 0 END) AS icu_24h_from_w1,
    MAX(CASE WHEN event_type='ICU_ADMIT' AND hours_from_ed > 6  AND hours_from_ed <= 30 THEN 1 ELSE 0 END) AS icu_24h_from_w6,
    MAX(CASE WHEN event_type='ICU_ADMIT' AND hours_from_ed > 24 AND hours_from_ed <= 48 THEN 1 ELSE 0 END) AS icu_24h_from_w24,
    -- ... (same pattern for icu_48h, pressor, vent, rrt, death with all horizons and windows)

    -- EVENT-BY FLAGS: hours_from_ed <= P
    MAX(CASE WHEN event_type='ICU_ADMIT' AND hours_from_ed <= 1  THEN 1 ELSE 0 END) AS event_by_icu_w1,
    MAX(CASE WHEN event_type='ICU_ADMIT' AND hours_from_ed <= 6  THEN 1 ELSE 0 END) AS event_by_icu_w6,
    MAX(CASE WHEN event_type='ICU_ADMIT' AND hours_from_ed <= 24 THEN 1 ELSE 0 END) AS event_by_icu_w24,
    -- ... (same pattern for pressor, vent, rrt, death, deterioration)
  FROM ev
  GROUP BY stay_id
),
icd_agg AS (
  SELECT stay_id,
    MAX(CASE WHEN event_type = 'CARDIAC_ARREST' THEN 1 ELSE 0 END) AS cardiac_arrest_hosp,
    MAX(CASE WHEN event_type = 'ACS' THEN 1 ELSE 0 END)            AS acs_hosp,
    MAX(CASE WHEN event_type IN ('PCI','CABG') THEN 1 ELSE 0 END)  AS revasc_hosp,
    MAX(CASE WHEN event_type = 'PCI' THEN 1 ELSE 0 END)            AS pci_hosp,
    MAX(CASE WHEN event_type = 'CABG' THEN 1 ELSE 0 END)           AS cabg_hosp
  FROM icd_ev
  GROUP BY stay_id
)
SELECT
  b.stay_id, b.subject_id, b.hadm_id, b.ed_intime, b.ed_outtime,
  -- Hospitalization-level
  COALESCE(a.death_hosp, 0) AS death_hosp,
  COALESCE(i.cardiac_arrest_hosp, 0) AS cardiac_arrest_hosp,
  COALESCE(i.acs_hosp, 0) AS acs_hosp,
  COALESCE(i.revasc_hosp, 0) AS revasc_hosp,
  COALESCE(i.pci_hosp, 0) AS pci_hosp,
  COALESCE(i.cabg_hosp, 0) AS cabg_hosp,
  CASE WHEN COALESCE(i.acs_hosp,0)=1 OR COALESCE(i.revasc_hosp,0)=1
       THEN 1 ELSE 0 END AS coronary_event_hosp,
  -- Time-to-event
  a.time_to_icu, a.time_to_death, a.time_to_deterioration,
  -- Prediction-aligned individual outcomes (33 columns)
  COALESCE(a.icu_24h_from_w1, 0) AS icu_24h_from_w1,
  -- ... (all aligned outcomes with COALESCE default 0)
  -- Composite aligned outcomes (6 columns)
  CASE WHEN icu_24h_from_w6=1 OR pressor_24h_from_w6=1 OR vent_24h_from_w6=1
         OR rrt_24h_from_w6=1 OR death_24h_from_w6=1
       THEN 1 ELSE 0 END AS deterioration_24h_from_w6,
  -- ... (all 6 deterioration composites)
  -- Event-by flags (18 columns)
  COALESCE(a.event_by_icu_w1, 0) AS event_by_icu_w1,
  -- ... (all 18 event_by flags)
FROM base b
LEFT JOIN ev_agg a USING (stay_id)
LEFT JOIN icd_agg i USING (stay_id);
```

> **Note:** The full SQL has 363 lines. The above is annotated and abbreviated. See [sql/20_outcomes_from_event_log.sql](../sql/20_outcomes_from_event_log.sql) for the complete source.

## Design Notes

### CTE Structure

1. **base**: Base ED cohort (for anchoring ED arrival times)
2. **ev**: Timed events only (event_time IS NOT NULL) — computes hours from ED arrival. **Defensive filters (v2.1):** `event_time >= ed_intime` and `event_time <= ed_intime + 30 days`
3. **icd_ev**: ICD-based events (event_time IS NULL) — hospitalization-level only
4. **ev_agg**: Aggregates timed events into prediction-aligned flags, event-by flags, and time-to-event
5. **icd_agg**: Aggregates ICD events into hospitalization-level binary flags
6. **Final SELECT**: Joins both aggregation CTEs with base, adds COALESCE defaults and composite labels

### Temporal Alignment

- **Prediction-aligned labels:** `hours_from_ed > P AND hours_from_ed <= P + H` — zero overlap with features
- **Event-by flags:** `hours_from_ed <= P` — events within the feature window (covariates)
- **Arrival-anchored outcomes have been removed** — all `icu_24h`, `deterioration_24h` etc. (measured from t=0) no longer exist

### NULL Handling

- Timed events: Binary flags default to 0 via COALESCE if event time is NULL or outside window
- ICD events: Binary flags are 1 if any matching ICD code exists for the admission, 0 otherwise
- ICD events are **not** included in time-windowed composites because their timing is unknown

---

# 5. Feature Extraction Scripts (30-32)

These scripts extract vital signs and laboratory values within time windows.

## Common Structure

All three scripts (30, 31, 32) follow the same pattern with different time windows:

| Script | Window | Hours |
|--------|--------|-------|
| 30_features_w1.sql | W1 | 0-1 |
| 31_features_w6.sql | W6 | 0-6 |
| 32_features_w24.sql | W24 | 0-24 |

> **Note (v2.1):** All feature time windows are clamped with `LEAST(b.ed_outtime, b.ed_intime + INTERVAL 'Xh')` for vitals, labs, and pyxis/medication queries. This prevents features from including measurements recorded after the patient left the ED (e.g., if the 6h window extends beyond the actual ED stay).

## SQL Logic (31_features_w6.sql as example)

```sql
DROP TABLE IF EXISTS {{ tables.features_w6 }};

CREATE TABLE {{ tables.features_w6 }} AS
WITH vitals AS (
    -- Aggregate vital signs within 6-hour window
    SELECT
        bc.stay_id,
        
        -- Systolic BP aggregates
        MIN(vs.sbp) AS sbp_min_6h,
        MAX(vs.sbp) AS sbp_max_6h,
        AVG(vs.sbp) AS sbp_mean_6h,
        STDDEV(vs.sbp) AS sbp_std_6h,
        
        -- Diastolic BP aggregates
        MIN(vs.dbp) AS dbp_min_6h,
        MAX(vs.dbp) AS dbp_max_6h,
        AVG(vs.dbp) AS dbp_mean_6h,
        STDDEV(vs.dbp) AS dbp_std_6h,
        
        -- Heart rate aggregates
        MIN(vs.heartrate) AS hr_min_6h,
        MAX(vs.heartrate) AS hr_max_6h,
        AVG(vs.heartrate) AS hr_mean_6h,
        STDDEV(vs.heartrate) AS hr_std_6h,
        
        -- Respiratory rate aggregates
        MIN(vs.resprate) AS rr_min_6h,
        MAX(vs.resprate) AS rr_max_6h,
        AVG(vs.resprate) AS rr_mean_6h,
        
        -- SpO2 aggregates
        MIN(vs.o2sat) AS spo2_min_6h,
        MAX(vs.o2sat) AS spo2_max_6h,
        AVG(vs.o2sat) AS spo2_mean_6h,
        
        -- Temperature aggregates
        MIN(vs.temperature) AS temp_min_6h,
        MAX(vs.temperature) AS temp_max_6h,
        AVG(vs.temperature) AS temp_mean_6h,
        
        -- Measurement count
        COUNT(*) AS n_vital_measurements_6h
        
    FROM {{ tables.base_ed_cohort }} bc
    INNER JOIN {{ schemas.ed }}.vitalsign vs
        ON bc.stay_id = vs.stay_id
    WHERE
        -- Within 6-hour window from ED arrival, clamped to ED stay
        vs.charttime >= bc.ed_intime
        AND vs.charttime < LEAST(bc.ed_outtime, bc.ed_intime + INTERVAL '6 hours')
    GROUP BY bc.stay_id
),
labs AS (
    -- Get FIRST lab value within 6-hour window
    SELECT DISTINCT ON (bc.stay_id, lab_type)
        bc.stay_id,
        CASE
            WHEN le.itemid = 50813 THEN 'lactate'
            WHEN le.itemid IN (51003, 51002) THEN 'troponin'
            WHEN le.itemid = 50912 THEN 'creatinine'
            WHEN le.itemid = 50971 THEN 'potassium'
            WHEN le.itemid = 50983 THEN 'sodium'
            WHEN le.itemid = 50882 THEN 'bicarbonate'
            WHEN le.itemid = 51301 THEN 'wbc'
            WHEN le.itemid = 51222 THEN 'hemoglobin'
            WHEN le.itemid = 51265 THEN 'platelet'
        END AS lab_type,
        le.valuenum AS lab_value
    FROM {{ tables.base_ed_cohort }} bc
    INNER JOIN {{ schemas.hosp }}.labevents le
        ON bc.subject_id = le.subject_id
    WHERE
        le.itemid IN (50813, 51003, 51002, 50912, 50971, 
                      50983, 50882, 51301, 51222, 51265)
        AND le.charttime >= bc.ed_intime
        AND le.charttime < LEAST(bc.ed_outtime, bc.ed_intime + INTERVAL '6 hours')
        AND le.valuenum IS NOT NULL
    ORDER BY bc.stay_id, lab_type, le.charttime
),
labs_pivot AS (
    -- Pivot labs to wide format
    SELECT
        stay_id,
        MAX(CASE WHEN lab_type = 'lactate' THEN lab_value END) 
            AS lactate_first_6h,
        MAX(CASE WHEN lab_type = 'troponin' THEN lab_value END) 
            AS troponin_first_6h,
        MAX(CASE WHEN lab_type = 'creatinine' THEN lab_value END) 
            AS creatinine_first_6h,
        MAX(CASE WHEN lab_type = 'potassium' THEN lab_value END) 
            AS potassium_first_6h,
        MAX(CASE WHEN lab_type = 'sodium' THEN lab_value END) 
            AS sodium_first_6h,
        MAX(CASE WHEN lab_type = 'bicarbonate' THEN lab_value END) 
            AS bicarbonate_first_6h,
        MAX(CASE WHEN lab_type = 'wbc' THEN lab_value END) 
            AS wbc_first_6h,
        MAX(CASE WHEN lab_type = 'hemoglobin' THEN lab_value END) 
            AS hemoglobin_first_6h,
        MAX(CASE WHEN lab_type = 'platelet' THEN lab_value END) 
            AS platelet_first_6h
    FROM labs
    GROUP BY stay_id
)
SELECT
    bc.stay_id,
    
    -- Vital signs
    v.sbp_min_6h, v.sbp_max_6h, v.sbp_mean_6h, v.sbp_std_6h,
    v.dbp_min_6h, v.dbp_max_6h, v.dbp_mean_6h, v.dbp_std_6h,
    v.hr_min_6h, v.hr_max_6h, v.hr_mean_6h, v.hr_std_6h,
    v.rr_min_6h, v.rr_max_6h, v.rr_mean_6h,
    v.spo2_min_6h, v.spo2_max_6h, v.spo2_mean_6h,
    v.temp_min_6h, v.temp_max_6h, v.temp_mean_6h,
    v.n_vital_measurements_6h,
    
    -- Labs
    l.lactate_first_6h,
    l.troponin_first_6h,
    l.creatinine_first_6h,
    l.potassium_first_6h,
    l.sodium_first_6h,
    l.bicarbonate_first_6h,
    l.wbc_first_6h,
    l.hemoglobin_first_6h,
    l.platelet_first_6h,
    
    -- Derived features
    CASE WHEN v.sbp_mean_6h > 0 
         THEN v.sbp_std_6h / v.sbp_mean_6h 
         ELSE NULL END AS sbp_cv_6h,
    v.hr_max_6h - v.hr_min_6h AS hr_range_6h,
    
    -- Missing indicators
    CASE WHEN l.lactate_first_6h IS NULL THEN 1 ELSE 0 END 
        AS missing_lactate_6h,
    CASE WHEN l.troponin_first_6h IS NULL THEN 1 ELSE 0 END 
        AS missing_troponin_6h

FROM {{ tables.base_ed_cohort }} bc
LEFT JOIN vitals v ON bc.stay_id = v.stay_id
LEFT JOIN labs_pivot l ON bc.stay_id = l.stay_id;
```

---

# 6. ECG Feature Scripts (33-34)

## Overview

Extract ECG features from machine measurements linked to ED visits.

| Script | Window |
|--------|--------|
| 33_ecg_features_w1.sql | First ECG within 1 hour |
| 34_ecg_features_w6.sql | First ECG within 6 hours |

## SQL Logic (34_ecg_features_w6.sql)

```sql
DROP TABLE IF EXISTS {{ tables.ecg_features_w6 }};

CREATE TABLE {{ tables.ecg_features_w6 }} AS
WITH ecg_matched AS (
    -- Match first ECG within 6 hours of ED arrival
    SELECT DISTINCT ON (bc.stay_id)
        bc.stay_id,
        rl.study_id,
        rl.ecg_time,
        EXTRACT(EPOCH FROM (rl.ecg_time - bc.ed_intime))/3600 
            AS hours_from_ed
    FROM {{ tables.base_ed_cohort }} bc
    INNER JOIN tmp_ecg_record_list rl
        ON bc.subject_id = rl.subject_id
    WHERE
        rl.ecg_time IS NOT NULL
        AND rl.ecg_time >= bc.ed_intime
        AND rl.ecg_time < LEAST(bc.ed_outtime, bc.ed_intime + INTERVAL '6 hours')
    ORDER BY bc.stay_id, rl.ecg_time  -- First ECG
),
ecg_with_meas AS (
    -- Join with machine measurements
    SELECT
        em.stay_id,
        em.study_id,
        em.ecg_time,
        em.hours_from_ed,
        
        -- Raw measurements
        mm.rr_interval,
        mm.p_onset,
        mm.p_end,
        mm.qrs_onset,
        mm.qrs_end,
        mm.t_end,
        mm.p_axis,
        mm.qrs_axis,
        mm.t_axis,
        
        -- Derived measurements
        CASE WHEN mm.rr_interval > 0 
             THEN 60000.0 / mm.rr_interval 
             ELSE NULL END AS ecg_hr,
        mm.qrs_end - mm.qrs_onset AS qrs_duration,
        mm.qrs_onset - mm.p_onset AS pr_interval,
        mm.t_end - mm.qrs_onset AS qt_interval
        
    FROM ecg_matched em
    LEFT JOIN tmp_ecg_machine_measurements mm
        ON em.study_id = mm.study_id
)
SELECT
    bc.stay_id,
    
    -- ECG identifiers
    ewm.study_id AS ecg_study_id_w6,
    ewm.ecg_time AS ecg_time_w6,
    ewm.hours_from_ed AS ecg_hours_from_ed_w6,
    
    -- Derived heart rate
    ewm.ecg_hr AS ecg_hr_w6,
    
    -- Raw intervals
    ewm.rr_interval AS ecg_rr_interval_w6,
    
    -- Calculated intervals
    ewm.qrs_duration AS ecg_qrs_dur_w6,
    ewm.pr_interval AS ecg_pr_w6,
    ewm.qt_interval AS ecg_qt_w6,
    
    -- Raw onset/end times
    ewm.p_onset AS ecg_p_onset_w6,
    ewm.p_end AS ecg_p_end_w6,
    ewm.qrs_onset AS ecg_qrs_onset_w6,
    ewm.qrs_end AS ecg_qrs_end_w6,
    ewm.t_end AS ecg_t_end_w6,
    
    -- Axis measurements
    ewm.p_axis AS ecg_p_axis_w6,
    ewm.qrs_axis AS ecg_qrs_axis_w6,
    ewm.t_axis AS ecg_t_axis_w6,
    
    -- Missing indicator
    CASE WHEN ewm.study_id IS NULL THEN 1 ELSE 0 END AS missing_ecg_w6

FROM {{ tables.base_ed_cohort }} bc
LEFT JOIN ecg_with_meas ewm ON bc.stay_id = ewm.stay_id;
```

## ECG Derivation Formulas

| Derived Feature | Formula | Description |
|-----------------|---------|-------------|
| ecg_hr | 60000 / rr_interval | Heart rate from RR (ms→bpm) |
| ecg_qrs_dur | qrs_end - qrs_onset | QRS complex duration |
| ecg_pr | qrs_onset - p_onset | PR interval |
| ecg_qt | t_end - qrs_onset | QT interval (proxy) |

---

# 7. 99_qa_checks.sql

## Purpose

Automated data integrity assertions run after every pipeline execution. Each query is designed to return **zero rows** when the data is correct; any returned rows indicate a violation.

## Output

No table created — results are evaluated in-memory by `src/validate.py::run_qa_checks()`.

## Checks

### Check 1: `events_before_ed`
Verifies no timed events have `event_time < ed_intime`.

### Check 2: `icd_events_with_time`
Verifies ICD-coded events (ACS, REVASCULARIZATION, CARDIAC_ARREST) have `event_time IS NULL` (they should have no timestamp).

### Check 3: `event_by_monotonicity_icu`
Verifies monotonicity of event-by flags: `event_by_icu_w1 <= event_by_icu_w6 <= event_by_icu_w24`. A violation would indicate a window alignment bug.

### Check 4: `age_out_of_bounds`
Verifies all `age_at_ed` values are in `[18, 110]`.

### Check 5: `dead_before_ed`
Verifies no patients have `dod < ed_intime`.

### Check 6: `ecg_outside_ed_window`
Verifies all linked ECGs have `ecg_time` within `[ed_intime, ed_outtime]`.

### Check 7: `label_event_by_overlap_icu_w6` (INFO)
Counts patients where `icu_24h_from_w6 = 1 AND event_by_icu_w6 = 1`. This is a legitimate multi-admission scenario, not a bug. Logged at INFO level. Currently ~109 cases.

### Check 8: `death_wrong_source`
Verifies death events have `event_source` matching `'hosp.admissions.deathtime'` or `'hosp.patients.dod'`. This check caught the column-alignment bug (v2.0) where `event_detail` was landing in the `event_source` column.

## Integration

The QA checks are automatically executed at the end of every pipeline run via:
```python
# In src/main.py
from src.validate import run_qa_checks
run_qa_checks(conn, cfg)
```

The function reads `sql/99_qa_checks.sql`, renders template variables, splits on semicolons, and executes each check individually, logging PASS/FAIL for each.

---

# 8. SQL Template Variables

## Jinja2 Variables

All SQL scripts use these template variables defined in `config.yaml`:

### Schema Variables

| Variable | Config Key | Example Value |
|----------|------------|---------------|
| `{{ schemas.ed }}` | schemas.ed | mimiciv_ed |
| `{{ schemas.hosp }}` | schemas.hosp | mimiciv_hosp |
| `{{ schemas.icu }}` | schemas.icu | mimiciv_icu |

### Table Variables

| Variable | Config Key | Default Value |
|----------|------------|---------------|
| `{{ tables.base_ed_cohort }}` | tables.base_ed_cohort | tmp_base_ed_cohort |
| `{{ tables.event_log }}` | tables.event_log | tmp_ed_event_log |
| `{{ tables.outcomes }}` | tables.outcomes | tmp_ed_outcomes |
| `{{ tables.features_w1 }}` | tables.features_w1 | tmp_features_w1 |
| `{{ tables.features_w6 }}` | tables.features_w6 | tmp_features_w6 |
| `{{ tables.features_w24 }}` | tables.features_w24 | tmp_features_w24 |
| `{{ tables.ecg_features_w1 }}` | tables.ecg_features_w1 | tmp_ecg_features_w1 |
| `{{ tables.ecg_features_w6 }}` | tables.ecg_features_w6 | tmp_ecg_features_w6 |

### Cohort Variables

| Variable | Config Key | Default Value |
|----------|------------|---------------|
| `{{ cohort.min_age }}` | cohort.min_age | 18 |

---

# 9. Query Optimization Notes

## Indexing Recommendations

```sql
-- Base cohort
CREATE INDEX idx_base_stay ON tmp_base_ed_cohort(stay_id);
CREATE INDEX idx_base_subject ON tmp_base_ed_cohort(subject_id);
CREATE INDEX idx_base_hadm ON tmp_base_ed_cohort(hadm_id);
CREATE INDEX idx_base_intime ON tmp_base_ed_cohort(ed_intime);

-- Event log
CREATE INDEX idx_event_stay ON tmp_ed_event_log(stay_id);
CREATE INDEX idx_event_type ON tmp_ed_event_log(event_type);

-- ECG tables
CREATE INDEX idx_ecg_rl_subject ON tmp_ecg_record_list(subject_id);
CREATE INDEX idx_ecg_rl_time ON tmp_ecg_record_list(ecg_time);
CREATE INDEX idx_ecg_mm_study ON tmp_ecg_machine_measurements(study_id);
```

## Performance Tips

1. **Use EXPLAIN ANALYZE** to profile slow queries
2. **Increase work_mem** for large sorts and aggregations
3. **VACUUM ANALYZE** after large inserts
4. **Batch inserts** for event log (run all 10-17 scripts together)

---

---

# PART 4: PYTHON MODULES DOCUMENTATION

# Python Modules Documentation

---

This documentation provides detailed overview of all Python modules in the cardiac deterioration pipeline.

---

## Table of Contents

1. [Module Overview](#1-module-overview)
2. [Core Modules](#2-core-modules)
3. [Build Modules](#3-build-modules)
4. [ECG Modules](#4-ecg-modules)
5. [Dataset Modules](#5-dataset-modules)
6. [Validation Modules](#6-validation-modules)
7. [Utility Modules](#7-utility-modules)
8. [Class Reference](#8-class-reference)
9. [Function Reference](#9-function-reference)
10. [Error Handling](#10-error-handling)

---

# 1. Module Overview

## Directory Structure

```
src/
├── main.py                    # Pipeline orchestration
├── db.py                      # Database utilities
├── utils.py                   # Helper functions
├── config_validator.py        # Configuration validation
│
├── build_base.py              # Cohort construction
├── build_event_log.py         # Event extraction
├── build_outcomes.py          # Outcome calculation
├── build_features.py          # Feature extraction
├── build_ecg_features.py      # ECG feature extraction
│
├── load_ecg.py                # ECG data loading
│
├── materialize_datasets.py    # Dataset generation
├── make_datasets.py           # Dataset presets
│
├── validate.py                # Validation routines
├── data_quality.py            # Data quality checks
│
└── __init__.py                # Package initialization
```

## Dependency Graph

```
main.py
   │
   ├── db.py
   ├── utils.py
   ├── config_validator.py
   │
   ├── build_base.py ─────────────────┐
   ├── build_event_log.py ────────────│
   ├── build_outcomes.py ─────────────│── All use db.py, utils.py
   ├── build_features.py ─────────────│
   ├── build_ecg_features.py ─────────┘
   │
   ├── load_ecg.py
   │
   └── materialize_datasets.py ──> make_datasets.py
```

---

# 2. Core Modules

## 2.1 main.py

### Purpose

Orchestrates the entire pipeline execution, managing the sequence of operations from cohort creation to dataset export.

### Key Components

```python
"""
main.py - Pipeline Orchestration

Entry point for the MIMIC deterioration pipeline.
"""

import argparse
import logging
from pathlib import Path

from .db import get_conn, close_conn
from .utils import load_yaml, setup_logging
from .config_validator import validate_config
from .build_base import build_base_cohort
from .build_event_log import build_event_log
from .build_outcomes import build_outcomes
from .build_features import build_features
from .build_ecg_features import build_ecg_features
from .materialize_datasets import materialize_all_datasets


def run_pipeline(config_path: str = "config/config.yaml") -> None:
    """
    Execute the complete pipeline.
    
    Parameters
    ----------
    config_path : str
        Path to the YAML configuration file.
        
    Returns
    -------
    None
    
    Raises
    ------
    ConfigurationError
        If configuration is invalid.
    DatabaseError
        If database connection fails.
    """
    # Load configuration
    cfg = load_yaml(config_path)
    validate_config(cfg)
    
    # Setup logging
    setup_logging(cfg.get('logging', {}))
    logger = logging.getLogger(__name__)
    
    logger.info("Starting MIMIC deterioration pipeline")
    
    # Connect to database
    conn = get_conn(cfg)
    
    try:
        # Step 1: Build base cohort
        logger.info("Step 1: Building base ED cohort")
        build_base_cohort(conn, cfg)
        
        # Step 2: Build event log
        logger.info("Step 2: Extracting clinical events")
        build_event_log(conn, cfg)
        
        # Step 3: Build outcomes
        logger.info("Step 3: Calculating outcomes")
        build_outcomes(conn, cfg)
        
        # Step 4: Build features
        logger.info("Step 4: Extracting features")
        build_features(conn, cfg, windows=['W1', 'W6', 'W24'])
        
        # Step 4.5: Build ECG features (ECG data pre-loaded)
        if cfg.get('ecg', {}).get('enabled', True):
            logger.info("Step 4.5: Extracting ECG features")
            # Note: ECG data should be pre-loaded into mimiciv_ecg schema
            # via QUICK_START_DATA_LOADING.md instructions
            build_ecg_features(conn, cfg, windows=['W1', 'W6'])
        
        # Step 5: Materialize datasets
        logger.info("Step 5: Materializing datasets")
        materialize_all_datasets(conn, cfg)
        
        logger.info("Pipeline completed successfully")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise
        
    finally:
        close_conn(conn)


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="MIMIC Deterioration Pipeline"
    )
    parser.add_argument(
        "--config", 
        default="config/config.yaml",
        help="Path to configuration file"
    )
    parser.add_argument(
        "--step",
        choices=['cohort', 'events', 'outcomes', 'features', 'ecg', 'datasets'],
        help="Run only a specific step"
    )
    
    args = parser.parse_args()
    run_pipeline(args.config)


if __name__ == "__main__":
    main()
```

### CLI Usage

```bash
# Run full pipeline
python -m src.main

# Run with custom config
python -m src.main --config path/to/config.yaml

# Run specific step only
python -m src.main --step features
```

---

## 2.2 db.py

### Purpose

Database connection management and SQL execution utilities.

### Key Components

```python
"""
db.py - Database Utilities

Provides database connection and query execution functions.
"""

import os
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import Dict, Any, Optional, List
import logging

logger = logging.getLogger(__name__)


def get_conn(cfg: Dict[str, Any]) -> psycopg2.extensions.connection:
    """
    Create a PostgreSQL database connection.
    
    Parameters
    ----------
    cfg : dict
        Configuration dictionary with 'db' section containing:
        - host: Database host
        - port: Database port
        - name: Database name
        - user: Username
        - password_env: Environment variable name for password
        
    Returns
    -------
    psycopg2.connection
        Active database connection
        
    Raises
    ------
    EnvironmentError
        If password environment variable is not set.
    psycopg2.OperationalError
        If connection fails.
        
    Example
    -------
    >>> cfg = load_yaml("config/config.yaml")
    >>> conn = get_conn(cfg)
    >>> conn.closed
    False
    """
    db_cfg = cfg['db']
    
    password = os.environ.get(db_cfg.get('password_env', 'PGPASSWORD'))
    if not password:
        raise EnvironmentError(
            f"Database password not found in environment variable "
            f"'{db_cfg.get('password_env', 'PGPASSWORD')}'"
        )
    
    conn = psycopg2.connect(
        host=db_cfg['host'],
        port=db_cfg['port'],
        dbname=db_cfg['name'],
        user=db_cfg['user'],
        password=password
    )
    
    logger.info(f"Connected to database: {db_cfg['name']}@{db_cfg['host']}")
    return conn


def close_conn(conn: psycopg2.extensions.connection) -> None:
    """
    Close database connection.
    
    Parameters
    ----------
    conn : psycopg2.connection
        Connection to close
    """
    if conn and not conn.closed:
        conn.close()
        logger.info("Database connection closed")


def execute_sql(
    conn: psycopg2.extensions.connection,
    sql: str,
    params: Optional[Dict[str, Any]] = None,
    fetch: bool = False
) -> Optional[List[Dict]]:
    """
    Execute a SQL query.
    
    Parameters
    ----------
    conn : psycopg2.connection
        Database connection
    sql : str
        SQL query string (may contain Jinja2 templates)
    params : dict, optional
        Parameters for query templating
    fetch : bool
        If True, return results as list of dicts
        
    Returns
    -------
    list of dict or None
        Query results if fetch=True, else None
        
    Example
    -------
    >>> sql = "SELECT COUNT(*) as cnt FROM {{ table }}"
    >>> result = execute_sql(conn, sql, {'table': 'tmp_ed_outcomes'}, fetch=True)
    >>> result[0]['cnt']
    424952
    """
    from jinja2 import Template
    
    # Render template if params provided
    if params:
        template = Template(sql)
        sql = template.render(**params)
    
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql)
        conn.commit()
        
        if fetch:
            return [dict(row) for row in cur.fetchall()]
    
    return None


def execute_sql_file(
    conn: psycopg2.extensions.connection,
    filepath: str,
    cfg: Dict[str, Any]
) -> None:
    """
    Execute SQL from a file with template rendering.
    
    Parameters
    ----------
    conn : psycopg2.connection
        Database connection
    filepath : str
        Path to SQL file
    cfg : dict
        Configuration for template variables
        
    Example
    -------
    >>> execute_sql_file(conn, "sql/00_base_ed_cohort.sql", cfg)
    """
    with open(filepath, 'r') as f:
        sql_template = f.read()
    
    execute_sql(conn, sql_template, cfg)
    logger.info(f"Executed: {filepath}")


def table_exists(conn: psycopg2.extensions.connection, table_name: str) -> bool:
    """
    Check if a table exists in the database.
    
    Parameters
    ----------
    conn : psycopg2.connection
        Database connection
    table_name : str
        Name of table to check
        
    Returns
    -------
    bool
        True if table exists
    """
    sql = """
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables 
            WHERE table_name = %s
        )
    """
    with conn.cursor() as cur:
        cur.execute(sql, (table_name,))
        return cur.fetchone()[0]


def get_table_count(conn: psycopg2.extensions.connection, table_name: str) -> int:
    """
    Get row count for a table.
    
    Parameters
    ----------
    conn : psycopg2.connection
        Database connection
    table_name : str
        Name of table
        
    Returns
    -------
    int
        Number of rows in table
    """
    sql = f"SELECT COUNT(*) FROM {table_name}"
    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchone()[0]
```

---

## 2.3 utils.py

### Purpose

General utility functions used throughout the pipeline.

### Key Components

```python
"""
utils.py - Utility Functions

Common helper functions for the pipeline.
"""

import yaml
import logging
import sys
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime


def load_yaml(filepath: str) -> Dict[str, Any]:
    """
    Load a YAML configuration file.
    
    Parameters
    ----------
    filepath : str
        Path to YAML file
        
    Returns
    -------
    dict
        Parsed configuration
        
    Raises
    ------
    FileNotFoundError
        If file does not exist
    yaml.YAMLError
        If YAML parsing fails
        
    Example
    -------
    >>> cfg = load_yaml("config/config.yaml")
    >>> cfg['db']['host']
    'localhost'
    """
    with open(filepath, 'r') as f:
        return yaml.safe_load(f)


def setup_logging(
    log_cfg: Optional[Dict[str, Any]] = None,
    log_dir: str = "artifacts/logs"
) -> None:
    """
    Configure logging for the pipeline.
    
    Parameters
    ----------
    log_cfg : dict, optional
        Logging configuration
    log_dir : str
        Directory for log files
        
    Example
    -------
    >>> setup_logging({'level': 'DEBUG'})
    """
    # Windows-safe stdout encoding
    if sys.platform == 'win32':
        sys.stdout.reconfigure(encoding='utf-8')
    
    # Create log directory
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    
    # Get configuration
    level = log_cfg.get('level', 'INFO') if log_cfg else 'INFO'
    
    # Setup handlers
    handlers = [
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(
            f"{log_dir}/pipeline_{datetime.now():%Y%m%d_%H%M%S}.log"
        )
    ]
    
    # Configure root logger
    logging.basicConfig(
        level=getattr(logging, level),
        format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
        handlers=handlers
    )


def render_template(template_str: str, params: Dict[str, Any]) -> str:
    """
    Render a Jinja2 template string.
    
    Parameters
    ----------
    template_str : str
        Template with {{ variables }}
    params : dict
        Values to substitute
        
    Returns
    -------
    str
        Rendered string
        
    Example
    -------
    >>> render_template("Hello {{ name }}", {'name': 'World'})
    'Hello World'
    """
    from jinja2 import Template
    template = Template(template_str)
    return template.render(**params)


def ensure_dir(path: str) -> Path:
    """
    Ensure a directory exists, creating if necessary.
    
    Parameters
    ----------
    path : str
        Directory path
        
    Returns
    -------
    Path
        Path object for the directory
    """
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


def format_number(n: int) -> str:
    """
    Format number with thousand separators.
    
    Parameters
    ----------
    n : int or float
        Number to format
        
    Returns
    -------
    str
        Formatted string
        
    Example
    -------
    >>> format_number(424952)
    '424,952'
    """
    return f"{n:,}"


def format_percent(n: float, total: float, decimals: int = 2) -> str:
    """
    Format a percentage.
    
    Parameters
    ----------
    n : float
        Numerator
    total : float
        Denominator
    decimals : int
        Decimal places
        
    Returns
    -------
    str
        Formatted percentage string
        
    Example
    -------
    >>> format_percent(27197, 424952)
    '6.40%'
    """
    if total == 0:
        return "0.00%"
    return f"{100 * n / total:.{decimals}f}%"
```

---

# 3. Build Modules

## 3.1 build_base.py

### Purpose

Constructs the base ED cohort by executing `00_base_ed_cohort.sql`.

### Key Components

```python
"""
build_base.py - Base Cohort Construction

Creates the foundational ED visit cohort.
"""

import logging
from typing import Dict, Any
import psycopg2

from .db import execute_sql_file, get_table_count
from .utils import format_number

logger = logging.getLogger(__name__)


def build_base_cohort(
    conn: psycopg2.extensions.connection,
    cfg: Dict[str, Any]
) -> int:
    """
    Create the base ED cohort table.
    
    Executes sql/00_base_ed_cohort.sql which:
    - Joins edstays with patients
    - Calculates age at ED visit
    - Filters for adults (age >= 18)
    - Calculates ED length of stay
    - Creates admission flag
    
    Parameters
    ----------
    conn : psycopg2.connection
        Database connection
    cfg : dict
        Pipeline configuration
        
    Returns
    -------
    int
        Number of rows in cohort
        
    Raises
    ------
    FileNotFoundError
        If SQL file not found
        
    Example
    -------
    >>> n_rows = build_base_cohort(conn, cfg)
    >>> print(f"Created cohort with {n_rows:,} visits")
    Created cohort with 424,952 visits
    """
    logger.info("Building base ED cohort...")
    
    # Execute SQL
    execute_sql_file(conn, "sql/00_base_ed_cohort.sql", cfg)
    
    # Get row count
    table_name = cfg['tables']['base_ed_cohort']
    n_rows = get_table_count(conn, table_name)
    
    logger.info(f"Created {table_name} with {format_number(n_rows)} rows")
    
    return n_rows
```

---

## 3.2 build_event_log.py

### Purpose

Extracts clinical events by executing event SQL scripts (10-17).

### Key Components

```python
"""
build_event_log.py - Event Log Construction

Extracts clinical events into a unified event log.
"""

import logging
from pathlib import Path
from typing import Dict, Any, List
import psycopg2

from .db import execute_sql, execute_sql_file, get_table_count
from .utils import format_number

logger = logging.getLogger(__name__)

# Event scripts in execution order
EVENT_SCRIPTS = [
    "sql/10_event_icu_admit.sql",
    "sql/11_event_pressors.sql",
    "sql/12_event_ventilation.sql",
    "sql/13_event_rrt.sql",
    "sql/14_event_acs.sql",
    "sql/15_event_revasc.sql",
    "sql/16_event_cardiac_arrest.sql",
    "sql/17_event_death.sql",
]


def create_event_log_table(
    conn: psycopg2.extensions.connection,
    cfg: Dict[str, Any]
) -> None:
    """
    Create or recreate the event log table.
    
    Parameters
    ----------
    conn : psycopg2.connection
        Database connection
    cfg : dict
        Pipeline configuration
    """
    table_name = cfg['tables']['event_log']
    
    sql = f"""
        DROP TABLE IF EXISTS {table_name};
        CREATE TABLE {table_name} (
            stay_id      INTEGER,
            event_type   VARCHAR(50),
            event_time   TIMESTAMP,
            event_source VARCHAR(100),
            event_detail TEXT
        );
    """
    
    execute_sql(conn, sql)
    logger.info(f"Created event log table: {table_name}")


def build_event_log(
    conn: psycopg2.extensions.connection,
    cfg: Dict[str, Any]
) -> Dict[str, int]:
    """
    Build the event log by running all event extraction scripts.
    
    Parameters
    ----------
    conn : psycopg2.connection
        Database connection
    cfg : dict
        Pipeline configuration
        
    Returns
    -------
    dict
        Event type -> count mapping
        
    Example
    -------
    >>> counts = build_event_log(conn, cfg)
    >>> counts['ICU_ADMIT']
    27027
    """
    logger.info("Building event log...")
    
    # Create empty event log table
    create_event_log_table(conn, cfg)
    
    # Execute each event script
    for script_path in EVENT_SCRIPTS:
        if Path(script_path).exists():
            execute_sql_file(conn, script_path, cfg)
            logger.info(f"Processed: {script_path}")
        else:
            logger.warning(f"Script not found: {script_path}")
    
    # Get counts by event type
    table_name = cfg['tables']['event_log']
    sql = f"""
        SELECT event_type, COUNT(*) as cnt
        FROM {table_name}
        GROUP BY event_type
        ORDER BY event_type
    """
    
    with conn.cursor() as cur:
        cur.execute(sql)
        counts = {row[0]: row[1] for row in cur.fetchall()}
    
    # Log summary
    total = sum(counts.values())
    logger.info(f"Event log created with {format_number(total)} total events:")
    for event_type, count in sorted(counts.items()):
        logger.info(f"  {event_type}: {format_number(count)}")
    
    return counts
```

---

## 3.3 build_outcomes.py

### Purpose

Calculates outcome labels from the event log.

### Key Components

```python
"""
build_outcomes.py - Outcome Calculation

Creates outcome labels from the event log.
"""

import logging
from typing import Dict, Any
import psycopg2

from .db import execute_sql_file, execute_sql, get_table_count
from .utils import format_number, format_percent

logger = logging.getLogger(__name__)


def build_outcomes(
    conn: psycopg2.extensions.connection,
    cfg: Dict[str, Any]
) -> Dict[str, float]:
    """
    Create the outcomes table from event log.
    
    Executes sql/20_outcomes_from_event_log.sql which creates:
    - Prediction-aligned outcome labels ({event}_{H}h_from_{W})
    - Event-by flags (event_by_{event}_{W})
    - Composite aligned outcomes (deterioration_{H}h_from_{W})
    - Hospitalization-level outcomes (death_hosp, ICD-based)
    - Time-to-event variables
    
    Parameters
    ----------
    conn : psycopg2.connection
        Database connection
    cfg : dict
        Pipeline configuration
        
    Returns
    -------
    dict
        Outcome -> rate mapping
        
    Example
    -------
    >>> rates = build_outcomes(conn, cfg)
    >>> rates['deterioration_24h_from_w6']
    0.064
    """
    logger.info("Building outcomes...")
    
    # Execute outcomes SQL
    execute_sql_file(conn, "sql/20_outcomes_from_event_log.sql", cfg)
    
    # Calculate outcome rates
    table_name = cfg['tables']['outcomes']
    
    outcomes_to_check = [
        'death_hosp',
        'cardiac_arrest_hosp', 'acs_hosp', 'revasc_hosp',
        'pci_hosp', 'cabg_hosp', 'coronary_event_hosp',
        'icu_24h_from_w6', 'pressor_24h_from_w6',
        'vent_24h_from_w6', 'rrt_24h_from_w6',
        'death_24h_from_w6', 'deterioration_24h_from_w6'
    ]
    
    rates = {}
    total = get_table_count(conn, table_name)
    
    for outcome in outcomes_to_check:
        sql = f"SELECT SUM({outcome}) FROM {table_name}"
        with conn.cursor() as cur:
            cur.execute(sql)
            count = cur.fetchone()[0] or 0
            rates[outcome] = count / total if total > 0 else 0
    
    # Log summary
    logger.info(f"Outcomes table created with {format_number(total)} rows")
    logger.info("Outcome rates:")
    for outcome, rate in rates.items():
        logger.info(f"  {outcome}: {format_percent(rate * total, total)}")
    
    return rates
```

---

## 3.4 build_features.py

### Purpose

Extracts features from vital signs and laboratory data.

### Key Components

```python
"""
build_features.py - Feature Extraction

Extracts clinical features for multiple time windows.
"""

import logging
from typing import Dict, Any, List
import psycopg2

from .db import execute_sql_file, get_table_count
from .utils import format_number

logger = logging.getLogger(__name__)

# Window definitions
WINDOW_SCRIPTS = {
    'W1': 'sql/30_features_w1.sql',
    'W6': 'sql/31_features_w6.sql',
    'W24': 'sql/32_features_w24.sql',
}


def build_features(
    conn: psycopg2.extensions.connection,
    cfg: Dict[str, Any],
    windows: List[str] = None
) -> Dict[str, int]:
    """
    Extract features for specified time windows.
    
    Parameters
    ----------
    conn : psycopg2.connection
        Database connection
    cfg : dict
        Pipeline configuration
    windows : list of str, optional
        Windows to process. Default: ['W1', 'W6', 'W24']
        
    Returns
    -------
    dict
        Window -> row count mapping
        
    Example
    -------
    >>> counts = build_features(conn, cfg, windows=['W6'])
    >>> counts['W6']
    424952
    """
    if windows is None:
        windows = ['W1', 'W6', 'W24']
    
    logger.info(f"Building features for windows: {windows}")
    
    counts = {}
    
    for window in windows:
        if window not in WINDOW_SCRIPTS:
            logger.warning(f"Unknown window: {window}")
            continue
        
        script_path = WINDOW_SCRIPTS[window]
        logger.info(f"Processing {window}...")
        
        execute_sql_file(conn, script_path, cfg)
        
        # Get table name from config
        table_key = f"features_{window.lower()}"
        table_name = cfg['tables'].get(table_key, f"tmp_features_{window.lower()}")
        
        counts[window] = get_table_count(conn, table_name)
        logger.info(f"  Created {table_name}: {format_number(counts[window])} rows")
    
    return counts
```

---

# 4. ECG Modules

---

## 4.1 build_ecg_features.py

### Purpose

Extracts ECG features by executing ECG SQL scripts.

### Key Components

```python
"""
build_ecg_features.py - ECG Feature Extraction

Extracts ECG features from machine measurements.
"""

import logging
from typing import Dict, Any, List
import psycopg2

from .db import execute_sql_file, execute_sql, get_table_count
from .utils import format_number, format_percent

logger = logging.getLogger(__name__)

ECG_SCRIPTS = {
    'W1': 'sql/33_ecg_features_w1.sql',
    'W6': 'sql/34_ecg_features_w6.sql',
}


def build_ecg_features(
    conn: psycopg2.extensions.connection,
    cfg: Dict[str, Any],
    windows: List[str] = None
) -> Dict[str, Dict[str, float]]:
    """
    Extract ECG features for specified time windows.
    
    Parameters
    ----------
    conn : psycopg2.connection
        Database connection
    cfg : dict
        Pipeline configuration
    windows : list of str, optional
        Windows to process. Default: ['W1', 'W6']
        
    Returns
    -------
    dict
        Window -> {total, with_ecg, coverage} mapping
        
    Example
    -------
    >>> stats = build_ecg_features(conn, cfg, windows=['W6'])
    >>> stats['W6']['coverage']
    0.343
    """
    if windows is None:
        windows = ['W1', 'W6']
    
    logger.info(f"Building ECG features for windows: {windows}")
    
    stats = {}
    
    for window in windows:
        if window not in ECG_SCRIPTS:
            logger.warning(f"Unknown ECG window: {window}")
            continue
        
        script_path = ECG_SCRIPTS[window]
        logger.info(f"Processing ECG {window}...")
        
        execute_sql_file(conn, script_path, cfg)
        
        # Calculate coverage
        table_key = f"ecg_features_{window.lower()}"
        table_name = cfg['tables'].get(table_key, f"tmp_ecg_features_{window.lower()}")
        
        total = get_table_count(conn, table_name)
        
        # Count non-missing ECGs
        sql = f"SELECT COUNT(*) FROM {table_name} WHERE missing_ecg_{window.lower()} = 0"
        with conn.cursor() as cur:
            cur.execute(sql)
            with_ecg = cur.fetchone()[0]
        
        stats[window] = {
            'total': total,
            'with_ecg': with_ecg,
            'coverage': with_ecg / total if total > 0 else 0
        }
        
        logger.info(
            f"  {table_name}: {format_number(with_ecg)}/{format_number(total)} "
            f"({format_percent(with_ecg, total)}) with ECG"
        )
    
    return stats
```

---

# 5. Dataset Modules

## 5.1 materialize_datasets.py

### Purpose

Generates analysis-ready CSV datasets. Supports both standard single-outcome datasets and advanced multi-outcome/multi-window datasets.

### Key Functions

| Function | Description |
|----------|-------------|
| `materialize_dataset()` | Generate standard dataset with single outcome |
| `materialize_dataset_advanced()` | Generate dataset with multiple outcomes and/or windows |
| `materialize_multiple_datasets()` | Generate multiple datasets from config list |
| `add_missing_indicators()` | Add binary flags for high-missingness columns |
| `get_dataset_summary()` | Print detailed dataset statistics |

### Standard Dataset Generation

```python
from src.materialize_datasets import materialize_dataset
from src.db import get_conn
from src.utils import load_yaml

cfg = load_yaml("config/config.yaml")
conn = get_conn(cfg)

df = materialize_dataset(
    conn=conn,
    cfg=cfg,
    window="W6",                                   # W1, W6, or W24
    outcome_col="deterioration_24h_from_w6",         # Prediction-aligned outcome
    out_csv="artifacts/datasets/my_dataset.csv",
    cohort_type="admitted",           # all, admitted, or not_admitted
    include_ecg=True,                 # Include ECG features (W1/W6 only)
    add_missing_ind=True,             # Auto-add missing indicator columns
    missing_threshold=0.10            # Threshold for missing indicators
)

conn.close()
```

### Advanced Dataset Generation (Multi-Outcome / Multi-Window)

```python
from src.materialize_datasets import materialize_dataset_advanced
from src.db import get_conn
from src.utils import load_yaml

cfg = load_yaml("config/config.yaml")
conn = get_conn(cfg)

# Multi-outcome dataset: multiple outcome columns
df = materialize_dataset_advanced(
    conn=conn,
    cfg=cfg,
    windows="W6",                                                   # Single window
    outcome_cols=["death_24h_from_w6", "death_48h_from_w6", "death_hosp"],  # Multiple outcomes
    out_csv="mortality_multi_outcome.csv",
    cohort_type="admitted"
)
# Creates columns: y_death_24h_from_w6, y_death_48h_from_w6, y_death_hosp
# Auto-includes: event_by_death_w6

# Multi-window dataset: features from multiple time windows
df = materialize_dataset_advanced(
    conn=conn,
    cfg=cfg,
    windows=["W6", "W24"],                          # Multiple windows
    outcome_cols="deterioration_24h_from_w6",         # Single outcome
    out_csv="multi_window_features.csv",
    cohort_type="admitted"
)
# Feature columns suffixed: sbp_mean_6h_w6, sbp_mean_24h_w24, etc.
# Auto-includes: event_by_icu_w6, event_by_pressor_w6, etc.

conn.close()
```

### Cohort Filters

| Filter | SQL Condition | Description |
|--------|--------------|-------------|
| `all` | (none) | All ED visits (424,952) |
| `admitted` | `hadm_id IS NOT NULL` | Only admitted patients (202,990) |
| `not_admitted` | `hadm_id IS NULL` | Only non-admitted patients |

### Custom SQL Filters

```python
df = materialize_dataset(
    conn=conn, cfg=cfg,
    window="W6",
    outcome_col="deterioration_24h_from_w6",
    out_csv="elderly_admitted.csv",
    cohort_type="admitted",
    cohort_filter_sql="age_at_ed >= 65"  # Additional filter
)
```

---

## 5.2 generate_advanced_dataset.py (CLI Script)

### Purpose

Command-line interface for generating advanced datasets with multi-outcome and/or multi-window support. Same as materialize_dataset.py but using command line interface (CLI).

### Usage

```bash
# Multi-outcome mortality dataset
python generate_advanced_dataset.py \
    --outcomes death_24h_from_w6 death_48h_from_w6 death_hosp \
    --window W6 \
    --cohort admitted \
    --name mortality_multi_outcome

# Multi-window dataset
python generate_advanced_dataset.py \
    --windows W6 W24 \
    --outcome deterioration_24h_from_w6 \
    --cohort admitted \
    --name multi_window_det24

# Single window, single outcome (like standard behavior)
python generate_advanced_dataset.py \
    --window W6 \
    --outcome death_24h_from_w6 \
    --cohort all \
    --name w6_death24_all
```

### Arguments

| Argument | Description | Default |
|----------|-------------|---------|
| `--config` | Configuration file path | `config/config.yaml` |
| `--output-dir` | Output directory | `artifacts/datasets` |
| `--name` | Dataset name (auto-generated if not provided) | - |
| `--window` | Single feature window | W6 |
| `--windows` | Multiple feature windows | - |
| `--outcome` | Single outcome column | deterioration_24h_from_w6 |
| `--outcomes` | Multiple outcome columns | - |
| `--cohort` | Cohort filter (all/admitted/not_admitted) | all |
| `--filter` | Additional SQL filter | - |
| `--ecg` | Include ECG features | False |
| `--no-missing-indicators` | Disable missing indicators | False |
| `--missing-threshold` | Threshold for missing indicators | 0.10 |
| `--verbose` | Enable verbose logging | False |

---

## 5.3 make_datasets.py

### Purpose

Defines predefined dataset specifications and provides a CLI for batch dataset generation from `config/datasets.yaml`.

### Predefined Dataset Specifications

| Dataset Name | Window | Outcome | Cohort | Description |
|--------------|--------|---------|--------|-------------|
| ed_w1_icu24 | W1 | icu_24h_from_w1 | admitted | Ultra-early ICU prediction |
| ed_w6_det24 | W6 | deterioration_24h_from_w6 | admitted | Primary dataset |
| ed_w24_det48 | W24 | deterioration_48h_from_w24 | admitted | Extended window |
| ed_w6_coronary_hosp | W6 | coronary_event_hosp | admitted | Coronary events |
| ed_w6_acs_hosp | W6 | acs_hosp | admitted | ACS only |
| ed_w6_death_hosp | W6 | death_hosp | admitted | In-hospital mortality |
| w1_w6_cardiac_with_ecg | W1,W6 | coronary_event_hosp | admitted | Multi-window cardiac |
| w6_w24_multi_mortality | W6,W24 | death_24h_from_w6, death_48h_from_w6, death_hosp | admitted | Multi-outcome mortality |
| elderly_elevated_troponin | W6 | deterioration_24h_from_w6 | admitted | Elderly subgroup |
| my_w6_mortality_dataset | W6 | death_24h_from_w6, death_48h_from_w6, death_hosp | admitted | Mortality multi-outcome |

---

# 6. Validation Modules

## 6.1 validate.py

### Purpose

Validation routines for pipeline integrity checks.

### Key Functions

| Function | Description |
|----------|-------------|
| `sanity_counts()` | Get row counts for all pipeline tables |
| `validate_pipeline()` | Run comprehensive validation checks |
| `validate_dataset()` | Validate a single dataset DataFrame |
| `run_qa_checks()` | Execute all 8 SQL assertions from `sql/99_qa_checks.sql` |

### Usage

```python
from src.validate import sanity_counts, validate_pipeline, run_qa_checks
from src.db import get_conn
from src.utils import load_yaml

cfg = load_yaml("config/config.yaml")
conn = get_conn(cfg)

# Get table counts
counts = sanity_counts(conn, cfg)
print(counts)
# {'base_ed_cohort': 424385, 'event_log': 81930, 'outcomes': 424385, ...}

# Run full validation
is_valid = validate_pipeline(conn, cfg)
print(f"Pipeline valid: {is_valid}")

# Run QA assertion checks (8 checks from sql/99_qa_checks.sql)
run_qa_checks(conn, cfg)

conn.close()
```

### Validation Checks Performed

1. **No duplicate stay_ids** - Primary key integrity
2. **Cohort-outcomes row match** - 1:1 mapping
3. **Events have valid stay_ids** - Foreign key integrity
4. **Event times after ED arrival** - Temporal consistency
5. **ECG timing within windows** - Window constraints
6. **QA assertion checks** (8 automated SQL checks) - See Section 12.2

---

## 6.2 data_quality.py

### Purpose

Data quality analysis and reporting for generated datasets.

### DataQualityReport Class

```python
from src.data_quality import DataQualityReport
import pandas as pd

df = pd.read_csv("artifacts/datasets/ed_w6_det24_admitted.csv")

# Generate comprehensive quality report
report = DataQualityReport(df, dataset_name="ed_w6_det24_admitted")
results = report.run_all_checks()

# Print summary
report.print_summary()

# Save JSON report
report.save_report("artifacts/reports/quality_report.json")
```

### Quality Metrics Computed

| Metric | Description |
|--------|-------------|
| Basic info | Shape, dtypes, memory usage |
| Missing analysis | Missing rates by column |
| Outcome analysis | Outcome rate, class balance |
| Outlier detection | Z-score based outlier counts |
| Correlation analysis | High correlations with outcome |
| Sanity checks | Age ranges, vital sign ranges |
| Quality score | Overall 0-100 score |

---

---

# 7. Utility Modules

## 7.1 config_validator.py

### Purpose

Validates pipeline configuration files including `config.yaml`, `datasets.yaml`, and `outcomes.yaml`.

### Key Functions

| Function | Description |
|----------|-------------|
| `validate_config()` | Validate main config.yaml |
| `validate_datasets_config()` | Validate datasets.yaml |
| `validate_outcomes_config()` | Validate outcomes.yaml |
| `validate_all_configs()` | Validate all configuration files |
| `suggest_fix()` | Suggest fixes for common errors |

### ConfigValidationError Exception

```python
from src.config_validator import validate_config, ConfigValidationError
from src.utils import load_yaml

cfg = load_yaml("config/config.yaml")

try:
    is_valid, errors = validate_config(cfg)
    if not is_valid:
        print("Configuration errors:")
        for error in errors:
            print(f"  - {error}")
except ConfigValidationError as e:
    print(f"Configuration invalid: {e}")
```

---

# 8. Class Reference

## Custom Exceptions

| Exception | Module | Description |
|-----------|--------|-------------|
| ConfigValidationError | config_validator.py | Invalid configuration with error list |

## Key Classes

| Class | Module | Description |
|-------|--------|-------------|
| DataQualityReport | data_quality.py | Comprehensive dataset quality analysis |

---

# 9. Function Reference

## Quick Reference Table

| Function | Module | Purpose |
|----------|--------|---------|
| run_all | main.py | Execute complete pipeline |
| get_conn | db.py | Create database connection |
| check_connection | db.py | Test database connectivity |
| run_sql | db.py | Execute SQL with template rendering |
| fetch_df | db.py | Execute SQL and return DataFrame |
| table_exists | db.py | Check if table exists |
| get_table_row_count | db.py | Get row count for table |
| load_yaml | utils.py | Load YAML config |
| setup_logging | utils.py | Configure logging |
| render_sql_template | utils.py | Render Jinja2 SQL template |
| ensure_output_dir | utils.py | Create directory if needed |
| validate_config | config_validator.py | Validate configuration |
| build_base | build_base.py | Create cohort table |
| build_event_log | build_event_log.py | Extract events |
| build_outcomes | build_outcomes.py | Calculate outcomes |
| build_features | build_features.py | Extract features |
| build_ecg_features | build_ecg_features.py | Extract ECG features |
| materialize_dataset | materialize_datasets.py | Generate standard dataset |
| materialize_dataset_advanced | materialize_datasets.py | Generate multi-outcome/window dataset |
| materialize_multiple_datasets | materialize_datasets.py | Generate multiple datasets |
| sanity_counts | validate.py | Get table row counts |
| validate_pipeline | validate.py | Run validation checks |

---

# 10. Error Handling

## Exception Hierarchy

```
Exception
├── ConfigValidationError
│   └── Raised when config.yaml is invalid
├── DatabaseError
│   └── Raised for connection/query failures
├── FileNotFoundError
│   └── Raised when SQL/ECG files missing
└── ValidationError
    └── Raised when data integrity check fails
```

## Error Recovery

```python
try:
    run_pipeline()
except ConfigurationError as e:
    # Fix config.yaml
    logger.error(f"Config error: {e}")
except DatabaseError as e:
    # Check PostgreSQL, credentials
    logger.error(f"Database error: {e}")
except FileNotFoundError as e:
    # Check file paths
    logger.error(f"File not found: {e}")
```

---

# PART 5: USAGE EXAMPLES AND TUTORIALS

# Usage Examples and Tutorials

---

This part provides practical examples and tutorials for using the MIMIC deterioration pipeline.

---

## Table of Contents

1. [Quick Start Guide](#1-quick-start-guide)
2. [Configuration Examples](#2-configuration-examples)
3. [Running the Pipeline](#3-running-the-pipeline)
4. [Dataset Generation](#4-dataset-generation)
5. [Custom Queries](#5-custom-queries)
6. [Data Analysis Examples](#6-data-analysis-examples)
7. [Machine Learning Integration](#7-machine-learning-integration)
8. [Advanced Usage](#8-advanced-usage)
9. [Common Workflows](#9-common-workflows)
10. [Troubleshooting Examples](#10-troubleshooting-examples)

---

# 1. Quick Start Guide

## 1.1 Prerequisites

```bash
# Required software
- PostgreSQL 15+
- Python 3.10+
- MIMIC-IV loaded in PostgreSQL
- MIMIC-IV ECG files (optional)
```

## 1.2 Installation

```bash
# Clone or navigate to pipeline directory
cd MIMIC_deterioration_pipeline  # Enter folder name

# Create virtual environment
python -m venv venv
venv\Scripts\activate  # Windows
source venv/bin/activate  # Linux/Mac

# Install dependencies
pip install psycopg2-binary pandas pyyaml jinja2 pytest
```

## 1.3 Configuration

```bash
# Set database password (this passoword needs to match PosgresSQl password)
set PGPASSWORD=your_password  # Windows 
export PGPASSWORD=your_password  # Linux/Mac

# Edit config.yaml if needed
notepad config/config.yaml
```

## 1.4 Run Pipeline

```bash
# Execute full pipeline
python -m src.main

# Output will show:
# - Cohort creation: 424,952 rows
# - Event extraction: 82,707 events
# - Outcome calculation
# - Feature extraction (W1, W6, W24)
# - ECG feature extraction (W1, W6)
# - Dataset generation
```

---

# 2. Configuration Examples

## 2.1 Minimal Configuration

```yaml
# config/config.yaml - Minimal setup
db:
  host: "localhost"
  port: 5432
  name: "mimic"
  user: "postgres"
  password_env: "PGPASSWORD"

schemas:
  ed: "mimiciv_ed"
  hosp: "mimiciv_hosp"
  icu: "mimiciv_icu"

tables:
  base_ed_cohort: "tmp_base_ed_cohort"
  event_log: "tmp_ed_event_log"
  outcomes: "tmp_ed_outcomes"
  features_w1: "tmp_features_w1"
  features_w6: "tmp_features_w6"
  features_w24: "tmp_features_w24"
  ecg_features_w1: "tmp_ecg_features_w1"
  ecg_features_w6: "tmp_ecg_features_w6"

cohort:
  min_age: 18
```

## 2.2 Full Configuration with ECG

```yaml
# config/config.yaml - Full configuration
db:
  host: localhost
  port: 5432
  name: mimiciv
  user: postgres
  password_env: PGPASSWORD
schemas:
  ed: mimiciv_ed
  hosp: mimiciv_hosp
  icu: mimiciv_icu
  ecg: mimiciv_ecg
tables:
  base_ed_cohort: tmp_base_ed_cohort
  event_log: tmp_ed_event_log
  outcomes: tmp_ed_outcomes
  features_w1: tmp_features_w1
  features_w6: tmp_features_w6
  features_w24: tmp_features_w24
  ecg_record_list: mimiciv_ecg.record_list
  ecg_machine_measurements: mimiciv_ecg.machine_measurements
  ecg_features_w1: tmp_ecg_features_w1
  ecg_features_w6: tmp_ecg_features_w6
cohort:
```


## 2.3 Remote Database Configuration (Example)

```yaml
# config/config_remote.yaml - Remote PostgreSQL
db:
  host: "mimic-db.example.com"
  port: 5432
  name: "mimic_iv"
  user: "research_user"
  password_env: "MIMIC_DB_PASSWORD"
  ssl_mode: "require"
```

---

# 3. Running the Pipeline

## 3.1 Full Pipeline Execution

```bash
# Run complete pipeline
python -m src.main

# Expected output:
# 2026-01-23 10:00:00 | INFO | Starting MIMIC deterioration pipeline
# 2026-01-23 10:00:01 | INFO | Step 1: Building base ED cohort
# 2026-01-23 10:00:15 | INFO | Created tmp_base_ed_cohort with 424,952 rows
# 2026-01-23 10:00:15 | INFO | Step 2: Extracting clinical events
# 2026-01-23 10:01:30 | INFO | Event log created with 82,707 total events
# 2026-01-23 10:01:30 | INFO | Step 3: Calculating outcomes
# 2026-01-23 10:02:00 | INFO | Outcomes table created with 424,952 rows
# 2026-01-23 10:02:00 | INFO | Step 4: Extracting features
# 2026-01-23 10:05:00 | INFO | Step 4.5: Loading ECG data
# 2026-01-23 10:06:00 | INFO | Step 4.6: Extracting ECG features
# 2026-01-23 10:07:00 | INFO | Step 5: Materializing datasets
# 2026-01-23 10:10:00 | INFO | Pipeline completed successfully
```

## 3.2 Running Specific Steps

```python
# run_specific_step.py
from src.db import get_conn
from src.utils import load_yaml
from src.build_base import build_base_cohort
from src.build_features import build_features

# Load config
cfg = load_yaml("config/config.yaml")
conn = get_conn(cfg)

try:
    # Run only cohort creation
    build_base_cohort(conn, cfg)
    
    # Run only 6-hour features
    build_features(conn, cfg, windows=['W6'])
    
finally:
    conn.close()
```

## 3.3 Pipeline with Progress Tracking (Create run_with_progress.py)

```python
# run_with_progress.py
import time
from src.db import get_conn
from src.utils import load_yaml
from src.build_base import build_base_cohort
from src.build_event_log import build_event_log
from src.build_outcomes import build_outcomes
from src.build_features import build_features

cfg = load_yaml("config/config.yaml")
conn = get_conn(cfg)

steps = [
    ("Building cohort", lambda: build_base_cohort(conn, cfg)),
    ("Extracting events", lambda: build_event_log(conn, cfg)),
    ("Calculating outcomes", lambda: build_outcomes(conn, cfg)),
    ("Extracting features", lambda: build_features(conn, cfg)),
]

for i, (name, func) in enumerate(steps, 1):
    print(f"[{i}/{len(steps)}] {name}...")
    start = time.time()
    func()
    elapsed = time.time() - start
    print(f"    Completed in {elapsed:.1f}s")

conn.close()
```

---

# 4. Dataset Generation

## 4.1 Generate All Predefined Datasets

```bash
# Generate all datasets
python -m src.make_datasets --all

# Output:
# Generated: artifacts/datasets/ed_w6_det24_admitted.csv (202,415 rows)
# Generated: artifacts/datasets/ed_w6_det24_all.csv (424,952 rows)
# Generated: artifacts/datasets/ed_w1_det24_admitted.csv (202,415 rows)
# ... etc.
```

## 4.2 Generate Specific Datasets

```bash
# List available datasets
python -m src.make_datasets --list

# Generate specific datasets
python -m src.make_datasets --datasets ed_w6_det24_admitted ed_w6_coronary72_admitted

# Custom output directory
python -m src.make_datasets --datasets ed_w6_det24_admitted --output-dir ./my_datasets
```

## 4.3 Generate Custom Dataset (Python)

```python
# generate_custom_dataset.py
from src.db import get_conn
from src.utils import load_yaml
from src.materialize_datasets import materialize_dataset

cfg = load_yaml("config/config.yaml")
conn = get_conn(cfg)

# Generate custom dataset
df = materialize_dataset(
    conn=conn,
    cfg=cfg,
    feature_window='W6',           # Use 6-hour features
    outcome_col='death_hosp',       # In-hospital mortality
    cohort_filter='admitted',       # Only admitted patients
    include_ecg=True,               # Include ECG features
    output_path='my_mortality_dataset.csv'
)

print(f"Dataset shape: {df.shape}")
print(f"Outcome rate: {df['y'].mean():.2%}")
print(f"Columns: {list(df.columns)}")

conn.close()
```

## 4.4 Generate Hybrid Datasets (Multi-Window, Multi-Outcome)

### Using generate_advanced_dataset.py (CLI)

The `generate_advanced_dataset.py` utility creates datasets with multiple time windows and/or multiple outcomes. It provides a flexible interface for combining any windows and outcomes available in the pipeline.

#### Basic Syntax

```bash
# Multi-window dataset
python generate_advanced_dataset.py \
  --windows W1 W6 W24 \
  --outcome <outcome_column> \
  --cohort <cohort_type> \
  --name <dataset_name>

# Multi-outcome dataset
python generate_advanced_dataset.py \
  --window <window> \
  --outcomes <outcome_col_1> <outcome_col_2> <outcome_col_3> ... \
  --cohort <cohort_type> \
  --name <dataset_name>

# Multi-window + Multi-outcome
python generate_advanced_dataset.py \
  --windows W1 W6 W24 \
  --outcomes <outcome_1> <outcome_2> <outcome_3> ... \
  --cohort <cohort_type> \
  --name <dataset_name>
```

#### Example: Mortality Outcomes

```bash
python generate_advanced_dataset.py \
  --windows W6 W24 \
  --outcomes death_24h_from_w6 death_48h_from_w6 death_hosp \
  --cohort admitted \
  --name w6_w24_mortality
```

#### Example: Deterioration Outcomes

```bash
python generate_advanced_dataset.py \
  --windows W6 W24 \
  --outcomes deterioration_24h_from_w6 icu_24h_from_w6 pressor_24h_from_w6 vent_24h_from_w6 \
  --cohort all \
  --name w6_w24_deterioration
```

#### Example: Cardiac Event Outcomes

```bash
python generate_advanced_dataset.py \
  --windows W1 W6 W24 \
  --outcomes acs_hosp cardiac_arrest_hosp revasc_hosp \
  --cohort admitted \
  --reports \
  --name w1_w6_w24_cardiac_events
```

#### Example: Mixed Outcomes

```bash
python generate_advanced_dataset.py \
  --windows W6 W24 \
  --outcomes death_24h_from_w6 icu_24h_from_w6 pressor_24h_from_w6 rrt_24h_from_w6 \
  --cohort all \
  --name w6_w24_mixed_outcomes
```

### Command-Line Options Reference

| Option | Purpose | Values |
|--------|---------|--------|
| `--windows` | Multiple feature windows | Any combination of: W1, W6, W24 |
| `--window` | Single feature window | W1, W6, or W24 |
| `--outcomes` | Multiple outcome columns | Any outcomes from tmp_outcomes table |
| `--outcome` | Single outcome column | Any single outcome |
| `--cohort` | Filter patients | `all`, `admitted`, `not_admitted` |
| `--name` | Custom dataset name | Any string (auto-generated if omitted) |
| `--ecg` | Include ECG features | (flag, optional) |
| `--reports` | Generate quality reports | (flag, optional) |
| `--filter` | Custom SQL WHERE clause | SQL condition string |
| `--no-missing-indicators` | Skip missing indicators | (flag, optional) |
| `--config` | Configuration file | Path to YAML config |
| `--output-dir` | Output directory | Path to directory |

### Available Outcomes

You can use ANY outcome column from `tmp_outcomes` table. All timed outcomes are prediction-aligned.

**Prediction-Aligned (format: `{event}_{H}h_from_{W}`):**
- `icu_24h_from_w6` - ICU admission 6-30h from ED arrival
- `icu_48h_from_w6` - ICU admission 6-54h from ED arrival
- `pressor_24h_from_w6` - Vasopressor start 6-30h from ED arrival
- `vent_24h_from_w6` - Ventilation 6-30h from ED arrival
- `rrt_24h_from_w6` - RRT 6-30h from ED arrival
- `death_24h_from_w6` - Death 6-30h from ED arrival
- `death_48h_from_w6` - Death 6-54h from ED arrival
- `death_72h_from_w6` - Death 6-78h from ED arrival
- (Available for all windows: w1, w6, w24)

**Hospitalization-Level:**
- `death_hosp` - In-hospital death
- `cardiac_arrest_hosp` - Cardiac arrest (ICD-based)

**Cardiac Events (Hospitalization-Level, ICD-based):**
- `acs_hosp` - Acute coronary syndrome
- `pci_hosp` - Percutaneous coronary intervention
- `cabg_hosp` - Coronary artery bypass graft
- `revasc_hosp` - Any revascularization

**Composite:**
- `deterioration_24h_from_w6` - Any critical event 6-30h from ED arrival
- `deterioration_48h_from_w6` - Any critical event 6-54h from ED arrival
- `coronary_event_hosp` - Any coronary event (ICD-based)

### Python API Usage

```python
# generate_custom_hybrid.py
from generate_advanced_dataset import generate_advanced_dataset

# Example 1: Multiple mortality windows
df = generate_advanced_dataset(
    windows=['W6', 'W24'],
    outcome_cols=['death_24h_from_w6', 'death_48h_from_w6', 'death_hosp'],
    cohort_type='admitted',
    dataset_name='my_mortality_study',
    generate_reports=True
)

# Example 2: Custom outcome combination
df = generate_advanced_dataset(
    windows=['W1', 'W6'],
    outcome_cols=['icu_24h_from_w6', 'pressor_24h_from_w6', 'vent_24h_from_w6'],
    cohort_type='all',
    dataset_name='critical_events_early_warning'
)

# Example 3: With ECG features
df = generate_advanced_dataset(
    windows=['W6', 'W24'],
    outcome_cols=['acs_hosp', 'revasc_hosp'],
    cohort_type='admitted',
    include_ecg=True,
    dataset_name='cardiac_prediction'
)
```

### Automatic Features

The script automatically:
- ✓ Combines features from all specified windows
- ✓ Creates outcome columns for each outcome (prefixed with `y_`)
- ✓ Generates missing indicators for sparse features
- ✓ Creates metadata JSON for reproducibility
- ✓ Generates data quality reports (with `--reports`)
- ✓ Auto-generates dataset name (unless `--name` specified)
- ✓ Applies cohort filters (admitted/not_admitted/all)

---

## 4.5 Dataset with Custom Filters

```python
# generate_filtered_dataset.py
import pandas as pd
from src.db import get_conn
from src.utils import load_yaml

cfg = load_yaml("config/config.yaml")
conn = get_conn(cfg)

# Custom SQL for specific population
sql = """
    SELECT
        c.stay_id, c.subject_id, c.age_at_ed, c.gender,
        f.*, o.deterioration_24h_from_w6 AS y
    FROM tmp_base_ed_cohort c
    JOIN tmp_ed_outcomes o ON c.stay_id = o.stay_id
    JOIN tmp_features_w6 f ON c.stay_id = f.stay_id
    WHERE 
        c.was_admitted = 1
        AND c.age_at_ed >= 65          -- Elderly patients only
        AND f.troponin_first_6h > 0.04  -- Elevated troponin
"""

df = pd.read_sql(sql, conn)
print(f"High-risk elderly patients: {len(df):,}")
print(f"Deterioration rate: {df['y'].mean():.2%}")

df.to_csv("elderly_elevated_troponin.csv", index=False)
conn.close()
```

---

# 5. Custom Queries

## 5.1 Explore Cohort Demographics (Example)

```python
# explore_demographics.py
import pandas as pd
from src.db import get_conn
from src.utils import load_yaml

cfg = load_yaml("config/config.yaml")
conn = get_conn(cfg)

# Age distribution
sql = """
    SELECT 
        CASE 
            WHEN age_at_ed < 30 THEN '18-29'
            WHEN age_at_ed < 50 THEN '30-49'
            WHEN age_at_ed < 65 THEN '50-64'
            WHEN age_at_ed < 80 THEN '65-79'
            ELSE '80+'
        END AS age_group,
        COUNT(*) as count,
        AVG(CASE WHEN was_admitted = 1 THEN 1.0 ELSE 0.0 END) as admission_rate
    FROM tmp_base_ed_cohort
    GROUP BY 1
    ORDER BY 1
"""

df = pd.read_sql(sql, conn)
print("Age Distribution:")
print(df.to_string(index=False))

conn.close()
```

## 5.2 Analyze Outcomes by Demographics (Example)

```python
# analyze_outcomes.py
import pandas as pd
from src.db import get_conn
from src.utils import load_yaml

cfg = load_yaml("config/config.yaml")
conn = get_conn(cfg)

sql = """
    SELECT 
        c.gender,
        CASE WHEN c.age_at_ed >= 65 THEN 'Elderly' ELSE 'Non-elderly' END as age_cat,
        COUNT(*) as n,
        SUM(o.deterioration_24h_from_w6) as deterioration_count,
        AVG(o.deterioration_24h_from_w6::float) * 100 as deterioration_rate,
        SUM(o.icu_24h_from_w6) as icu_count,
        AVG(o.icu_24h_from_w6::float) * 100 as icu_rate,
        SUM(o.death_24h_from_w6) as death_count,
        AVG(o.death_24h_from_w6::float) * 100 as death_rate
    FROM tmp_base_ed_cohort c
    JOIN tmp_ed_outcomes o ON c.stay_id = o.stay_id
    WHERE c.was_admitted = 1
    GROUP BY 1, 2
    ORDER BY 1, 2
"""

df = pd.read_sql(sql, conn)
print("Outcome Rates by Demographics:")
print(df.round(2).to_string(index=False))

conn.close()
```

## 5.3 Event Timeline Analysis (Example)

```python
# event_timeline.py
import pandas as pd
from src.db import get_conn
from src.utils import load_yaml

cfg = load_yaml("config/config.yaml")
conn = get_conn(cfg)

# Time to ICU distribution
sql = """
    SELECT 
        CASE 
            WHEN time_to_icu <= 6 THEN '0-6h'
            WHEN time_to_icu <= 12 THEN '6-12h'
            WHEN time_to_icu <= 24 THEN '12-24h'
            WHEN time_to_icu <= 48 THEN '24-48h'
            ELSE '>48h'
        END as time_category,
        COUNT(*) as count
    FROM tmp_ed_outcomes
    WHERE time_to_icu IS NOT NULL
    GROUP BY 1
    ORDER BY 
        CASE 
            WHEN time_category = '0-6h' THEN 1
            WHEN time_category = '6-12h' THEN 2
            WHEN time_category = '12-24h' THEN 3
            WHEN time_category = '24-48h' THEN 4
            ELSE 5
        END
"""

df = pd.read_sql(sql, conn)
print("Time to ICU Distribution:")
print(df.to_string(index=False))

conn.close()
```

## 5.4 ECG Feature Analysis (Example)

```python
# ecg_analysis.py
import pandas as pd
from src.db import get_conn
from src.utils import load_yaml

cfg = load_yaml("config/config.yaml")
conn = get_conn(cfg)

# ECG features by outcome
sql = """
    SELECT 
        o.deterioration_24h_from_w6 as deteriorated,
        COUNT(*) as n,
        AVG(e.ecg_hr_w6) as mean_hr,
        AVG(e.ecg_qrs_dur_w6) as mean_qrs,
        AVG(e.ecg_pr_w6) as mean_pr,
        AVG(e.ecg_qt_w6) as mean_qt,
        AVG(e.missing_ecg_w6::float) * 100 as pct_missing_ecg
    FROM tmp_ed_outcomes o
    JOIN tmp_ecg_features_w6 e ON o.stay_id = e.stay_id
    WHERE o.hadm_id IS NOT NULL
    GROUP BY 1
"""

df = pd.read_sql(sql, conn)
print("ECG Features by Outcome:")
print(df.round(2).to_string(index=False))

conn.close()
```

---

# 6. Data Analysis Examples

## 6.1 Descriptive Statistics

```python
# descriptive_stats.py
import pandas as pd
import numpy as np

# Load dataset
df = pd.read_csv("artifacts/datasets/ed_w6_det24_admitted.csv")

print("="*60)
print("DATASET SUMMARY")
print("="*60)

# Basic info
print(f"\nShape: {df.shape[0]:,} rows x {df.shape[1]} columns")
print(f"Outcome rate: {df['y'].mean():.2%}")

# Numeric summaries
numeric_cols = ['age_at_ed', 'ed_los_hours', 'sbp_mean_6h', 'hr_mean_6h', 
                'lactate_first_6h', 'troponin_first_6h', 'ecg_hr_w6']

print("\n" + "="*60)
print("NUMERIC FEATURE SUMMARY")
print("="*60)

for col in numeric_cols:
    if col in df.columns:
        print(f"\n{col}:")
        print(f"  Mean: {df[col].mean():.2f}")
        print(f"  Std:  {df[col].std():.2f}")
        print(f"  Min:  {df[col].min():.2f}")
        print(f"  Max:  {df[col].max():.2f}")
        print(f"  Missing: {df[col].isna().mean():.1%}")
```

## 6.2 Missing Data Analysis

```python
# missing_analysis.py
import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv("artifacts/datasets/ed_w6_det24_admitted.csv")

# Calculate missing rates
missing = df.isnull().mean().sort_values(ascending=False)
missing = missing[missing > 0]

print("Variables with Missing Data:")
print("-"*40)
for col, rate in missing.items():
    print(f"{col}: {rate:.1%}")

# Plot
fig, ax = plt.subplots(figsize=(10, 6))
missing.head(20).plot(kind='barh', ax=ax)
ax.set_xlabel('Missing Rate')
ax.set_title('Top 20 Variables by Missing Rate')
plt.tight_layout()
plt.savefig('missing_data_plot.png')
print("\nSaved: missing_data_plot.png")
```

## 6.3 Outcome Correlation Analysis

```python
# outcome_correlation.py
import pandas as pd
import numpy as np
from scipy import stats

df = pd.read_csv("artifacts/datasets/ed_w6_det24_admitted.csv")

# Select predictive features
features = [
    'age_at_ed', 'sbp_mean_6h', 'hr_mean_6h', 'rr_mean_6h',
    'spo2_min_6h', 'lactate_first_6h', 'troponin_first_6h',
    'ecg_hr_w6', 'ecg_qrs_dur_w6'
]

print("Feature Correlations with Deterioration:")
print("-"*50)

for feat in features:
    if feat in df.columns:
        # Drop missing
        mask = df[feat].notna()
        if mask.sum() > 100:
            corr, pval = stats.pointbiserialr(df.loc[mask, 'y'], df.loc[mask, feat])
            sig = '***' if pval < 0.001 else '**' if pval < 0.01 else '*' if pval < 0.05 else ''
            print(f"{feat:25s} r={corr:+.3f} {sig}")
```

---

# 7. Machine Learning Integration

## 7.1 Basic XGBoost Model

```python
# train_xgboost.py
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score, classification_report
from sklearn.impute import SimpleImputer
import xgboost as xgb

# Load data
df = pd.read_csv("artifacts/datasets/ed_w6_det24_admitted.csv")

# Select features
feature_cols = [
    'age_at_ed', 'ed_los_hours',
    'sbp_min_6h', 'sbp_max_6h', 'sbp_mean_6h',
    'hr_min_6h', 'hr_max_6h', 'hr_mean_6h',
    'rr_mean_6h', 'spo2_min_6h', 'temp_max_6h',
    'lactate_first_6h', 'troponin_first_6h', 'creatinine_first_6h',
    'ecg_hr_w6', 'ecg_qrs_dur_w6', 'ecg_pr_w6',
    'missing_lactate_6h', 'missing_troponin_6h', 'missing_ecg_w6'
]

# Prepare data
X = df[[c for c in feature_cols if c in df.columns]]
y = df['y']

# Impute missing values
imputer = SimpleImputer(strategy='median')
X_imputed = imputer.fit_transform(X)

# Split
X_train, X_test, y_train, y_test = train_test_split(
    X_imputed, y, test_size=0.2, random_state=42, stratify=y
)

# Train
model = xgb.XGBClassifier(
    n_estimators=100,
    max_depth=5,
    learning_rate=0.1,
    scale_pos_weight=len(y_train[y_train==0]) / len(y_train[y_train==1]),
    random_state=42
)
model.fit(X_train, y_train)

# Evaluate
y_pred_proba = model.predict_proba(X_test)[:, 1]
auc = roc_auc_score(y_test, y_pred_proba)

print(f"Test AUC: {auc:.3f}")
print("\nClassification Report:")
print(classification_report(y_test, (y_pred_proba > 0.5).astype(int)))

# Feature importance
importance = pd.DataFrame({
    'feature': X.columns,
    'importance': model.feature_importances_
}).sort_values('importance', ascending=False)

print("\nTop 10 Important Features:")
print(importance.head(10).to_string(index=False))
```

## 7.2 Cross-Validated Evaluation

```python
# cross_validation.py
import pandas as pd
import numpy as np
from sklearn.model_selection import StratifiedKFold
from sklearn.metrics import roc_auc_score
from sklearn.impute import SimpleImputer
import xgboost as xgb

df = pd.read_csv("artifacts/datasets/ed_w6_det24_admitted.csv")

feature_cols = ['age_at_ed', 'sbp_mean_6h', 'hr_mean_6h', 'rr_mean_6h',
                'spo2_min_6h', 'lactate_first_6h', 'troponin_first_6h',
                'ecg_hr_w6', 'ecg_qrs_dur_w6', 'missing_ecg_w6']

X = df[[c for c in feature_cols if c in df.columns]]
y = df['y']

# Impute
imputer = SimpleImputer(strategy='median')
X_imputed = imputer.fit_transform(X)

# Cross-validation
cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
aucs = []

for fold, (train_idx, val_idx) in enumerate(cv.split(X_imputed, y)):
    X_train, X_val = X_imputed[train_idx], X_imputed[val_idx]
    y_train, y_val = y.iloc[train_idx], y.iloc[val_idx]
    
    model = xgb.XGBClassifier(n_estimators=100, max_depth=5, random_state=42)
    model.fit(X_train, y_train)
    
    y_pred = model.predict_proba(X_val)[:, 1]
    auc = roc_auc_score(y_val, y_pred)
    aucs.append(auc)
    print(f"Fold {fold+1}: AUC = {auc:.3f}")

print(f"\nMean AUC: {np.mean(aucs):.3f} (+/- {np.std(aucs):.3f})")
```

## 7.3 PyTorch Neural Network

```python
# train_neural_net.py
import pandas as pd
import numpy as np
import torch
import torch.nn as nn
from torch.utils.data import DataLoader, TensorDataset
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.metrics import roc_auc_score

# Load and prepare data
df = pd.read_csv("artifacts/datasets/ed_w6_det24_admitted.csv")

feature_cols = ['age_at_ed', 'sbp_mean_6h', 'hr_mean_6h', 'rr_mean_6h',
                'spo2_min_6h', 'lactate_first_6h', 'troponin_first_6h',
                'ecg_hr_w6', 'ecg_qrs_dur_w6']

X = df[[c for c in feature_cols if c in df.columns]].values
y = df['y'].values

# Impute and scale
imputer = SimpleImputer(strategy='median')
scaler = StandardScaler()
X = scaler.fit_transform(imputer.fit_transform(X))

# Split
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)

# Convert to tensors
X_train_t = torch.FloatTensor(X_train)
y_train_t = torch.FloatTensor(y_train)
X_test_t = torch.FloatTensor(X_test)
y_test_t = torch.FloatTensor(y_test)

# DataLoader
train_dataset = TensorDataset(X_train_t, y_train_t)
train_loader = DataLoader(train_dataset, batch_size=256, shuffle=True)

# Define model
class DeteriorationNet(nn.Module):
    def __init__(self, input_dim):
        super().__init__()
        self.layers = nn.Sequential(
            nn.Linear(input_dim, 64),
            nn.ReLU(),
            nn.Dropout(0.3),
            nn.Linear(64, 32),
            nn.ReLU(),
            nn.Dropout(0.3),
            nn.Linear(32, 1),
            nn.Sigmoid()
        )
    
    def forward(self, x):
        return self.layers(x)

model = DeteriorationNet(X_train.shape[1])
criterion = nn.BCELoss()
optimizer = torch.optim.Adam(model.parameters(), lr=0.001)

# Train
for epoch in range(50):
    model.train()
    for X_batch, y_batch in train_loader:
        optimizer.zero_grad()
        y_pred = model(X_batch).squeeze()
        loss = criterion(y_pred, y_batch)
        loss.backward()
        optimizer.step()
    
    if (epoch + 1) % 10 == 0:
        model.eval()
        with torch.no_grad():
            y_test_pred = model(X_test_t).squeeze().numpy()
            auc = roc_auc_score(y_test, y_test_pred)
            print(f"Epoch {epoch+1}: Test AUC = {auc:.3f}")

# Final evaluation
model.eval()
with torch.no_grad():
    y_pred = model(X_test_t).squeeze().numpy()
    print(f"\nFinal Test AUC: {roc_auc_score(y_test, y_pred):.3f}")
```

---

# 8. Advanced Usage

## 8.1 Custom Outcome Definition
These are mainly automatic ways to add outocmes etc ot the dataset without rerunning the pipeline, adding to the scripts etc. 

NOTE: this is temporary and rerunning the pipeline will drop these additions unless expilcitly added to the relevant SQL file.

```python
# custom_outcome.py
from src.db import get_conn, execute_sql
from src.utils import load_yaml

cfg = load_yaml("config/config.yaml")
conn = get_conn(cfg)

# Create custom outcome: "Severe deterioration" (death OR cardiac arrest OR pressors)
sql = """
    ALTER TABLE tmp_ed_outcomes
    ADD COLUMN IF NOT EXISTS severe_deterioration_24h INTEGER;
    
    UPDATE tmp_ed_outcomes
    SET severe_deterioration_24h = CASE
        WHEN death_24h_from_w6 = 1 OR cardiac_arrest_hosp = 1 OR pressor_24h_from_w6 = 1
        THEN 1 ELSE 0
    END;
"""

execute_sql(conn, sql)
print("Custom outcome 'severe_deterioration_24h' created!")

# Check rate
sql_check = "SELECT AVG(severe_deterioration_24h::float) FROM tmp_ed_outcomes"
result = execute_sql(conn, sql_check, fetch=True)
print(f"Severe deterioration rate: {result[0]['avg']:.2%}")

conn.close()
```

## 8.2 Adding New Features

```python
# add_shock_index.py
from src.db import get_conn, execute_sql
from src.utils import load_yaml

cfg = load_yaml("config/config.yaml")
conn = get_conn(cfg)

# Add shock index (HR / SBP) to features
sql = """
    ALTER TABLE tmp_features_w6
    ADD COLUMN IF NOT EXISTS shock_index_6h FLOAT;
    
    UPDATE tmp_features_w6
    SET shock_index_6h = CASE
        WHEN sbp_mean_6h > 0 THEN hr_mean_6h / sbp_mean_6h
        ELSE NULL
    END;
"""

execute_sql(conn, sql)
print("Shock index feature added!")

# Validate
sql_check = """
    SELECT 
        AVG(shock_index_6h) as mean_si,
        MIN(shock_index_6h) as min_si,
        MAX(shock_index_6h) as max_si
    FROM tmp_features_w6
    WHERE shock_index_6h IS NOT NULL
"""
result = execute_sql(conn, sql_check, fetch=True)
print(f"Shock Index - Mean: {result[0]['mean_si']:.2f}, "
      f"Min: {result[0]['min_si']:.2f}, Max: {result[0]['max_si']:.2f}")

conn.close()
```

### Adding New Events

1. Create SQL file in `sql/` (e.g., `18_event_custom.sql`)
2. Add to `EVENT_SQL_FILES` in `src/build_event_log.py`
3. Update outcome definitions in `config/outcomes.yaml`

### Adding New Features

1. Create SQL file in `sql/` (e.g., `33_features_custom.sql`)
2. Update `FEATURE_WINDOWS` in `src/build_features.py`
3. Document in `config/feature_catalog.yaml`

### Defining New Outcomes

Edit `config/outcomes.yaml` to add custom outcome definitions:

```yaml
my_custom_outcome:
  description: "Custom outcome definition"
  horizon_hours: 24
  events:
    - ICU_ADMIT
    - PRESSOR_START
```

## 8.3 Patient Subgroup Analysis

```python
# subgroup_analysis.py
import pandas as pd
from src.db import get_conn
from src.utils import load_yaml

cfg = load_yaml("config/config.yaml")
conn = get_conn(cfg)

# Analyze outcomes by chief complaint proxy (using troponin order as cardiac flag)
sql = """
    WITH patient_groups AS (
        SELECT 
            o.stay_id,
            o.deterioration_24h_from_w6,
            o.death_24h_from_w6,
            CASE WHEN f.troponin_first_6h IS NOT NULL THEN 'Cardiac workup'
                 ELSE 'Non-cardiac' END as patient_type
        FROM tmp_ed_outcomes o
        JOIN tmp_features_w6 f ON o.stay_id = f.stay_id
        JOIN tmp_base_ed_cohort c ON o.stay_id = c.stay_id
        WHERE c.was_admitted = 1
    )
    SELECT 
        patient_type,
        COUNT(*) as n,
        AVG(deterioration_24h_from_w6::float) * 100 as det_rate,
        AVG(death_24h_from_w6::float) * 100 as death_rate
    FROM patient_groups
    GROUP BY patient_type
"""

df = pd.read_sql(sql, conn)
print("Subgroup Analysis: Cardiac vs Non-Cardiac Workup")
print(df.round(2).to_string(index=False))

conn.close()
```
Troponin order (yes[Cardiac workup]/no[Non cardiac]) in Hospitalized Patients rates

| patient_type     | n      | det_rate (%) | death_rate (%) |
|------------------|--------|--------------|---------------|
| Cardiac workup   | 4,706  | 44.35        | 1.93          |
| Non-cardiac      |198,284 | 12.67        | 0.25          |

---

# 9. Common Workflows

## 9.1 Workflow: Research Paper Dataset (Example)

```python
# research_paper_workflow.py
"""
Complete workflow for generating a research-ready dataset.
"""
import pandas as pd
from src.db import get_conn
from src.utils import load_yaml
from src.materialize_datasets import materialize_dataset

# Step 1: Load configuration
cfg = load_yaml("config/config.yaml")
conn = get_conn(cfg)

# Step 2: Generate primary dataset
df = materialize_dataset(
    conn=conn,
    cfg=cfg,
    feature_window='W6',
    outcome_col='deterioration_24h_from_w6',
    cohort_filter='admitted',
    include_ecg=True
)

# Step 3: Create train/test split (by patient for no leakage)
patients = df['subject_id'].unique()
np.random.seed(42)
np.random.shuffle(patients)
train_patients = patients[:int(0.8 * len(patients))]

df['split'] = df['subject_id'].apply(
    lambda x: 'train' if x in train_patients else 'test'
)

# Step 4: Save
df[df['split'] == 'train'].to_csv('research_train.csv', index=False)
df[df['split'] == 'test'].to_csv('research_test.csv', index=False)

print(f"Train set: {len(df[df['split']=='train']):,} rows")
print(f"Test set: {len(df[df['split']=='test']):,} rows")

conn.close()
```

## 9.2 Workflow: Model Comparison Study

```python
# model_comparison_workflow.py
"""
Compare multiple outcomes using same feature set.
"""
import pandas as pd
import numpy as np
from sklearn.model_selection import cross_val_score
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer

from src.db import get_conn
from src.utils import load_yaml

cfg = load_yaml("config/config.yaml")
conn = get_conn(cfg)

# Get base data with multiple outcomes
sql = """
    SELECT 
        c.stay_id, c.age_at_ed, f.*,
        o.deterioration_24h_from_w6, o.icu_24h_from_w6, o.death_24h_from_w6,
        o.coronary_event_hosp
    FROM tmp_base_ed_cohort c
    JOIN tmp_ed_outcomes o ON c.stay_id = o.stay_id
    JOIN tmp_features_w6 f ON c.stay_id = f.stay_id
    WHERE c.was_admitted = 1
"""
df = pd.read_sql(sql, conn)
conn.close()

# Define feature columns
feature_cols = ['age_at_ed', 'sbp_mean_6h', 'hr_mean_6h', 'rr_mean_6h',
                'spo2_min_6h', 'lactate_first_6h', 'troponin_first_6h']

X = df[[c for c in feature_cols if c in df.columns]]
X = SimpleImputer(strategy='median').fit_transform(X)

# Compare outcomes
outcomes = ['deterioration_24h_from_w6', 'icu_24h_from_w6', 'death_24h_from_w6', 
            'coronary_event_hosp']

print("Model Comparison (5-fold CV AUC):")
print("-" * 50)

for outcome in outcomes:
    y = df[outcome]
    clf = RandomForestClassifier(n_estimators=100, random_state=42)
    scores = cross_val_score(clf, X, y, cv=5, scoring='roc_auc')
    print(f"{outcome:25s}: {scores.mean():.3f} (+/- {scores.std():.3f})")
```
Results:
| Outcome                  | Mean AUC | Std. Dev |
|--------------------------|----------|----------|
| deterioration_24h_from_w6 | 0.774    | 0.003    |
| icu_24h_from_w6           | 0.773    | 0.003    |
| death_24h_from_w6         | 0.782    | 0.013    |
| coronary_event_hosp       | 0.729    | 0.006    |
---

# 10. Troubleshooting Examples

## 10.1 Debug Database Connection

```python
# debug_connection.py
import os
import psycopg2
from src.utils import load_yaml

cfg = load_yaml("config/config.yaml")
db_cfg = cfg['db']

print("Checking database connection...")
print(f"Host: {db_cfg['host']}")
print(f"Port: {db_cfg['port']}")
print(f"Database: {db_cfg['name']}")
print(f"User: {db_cfg['user']}")

password = os.environ.get(db_cfg.get('password_env', 'PGPASSWORD'))
print(f"Password set: {'Yes' if password else 'NO - SET PGPASSWORD!'}")

try:
    conn = psycopg2.connect(
        host=db_cfg['host'],
        port=db_cfg['port'],
        dbname=db_cfg['name'],
        user=db_cfg['user'],
        password=password
    )
    print("Connection: SUCCESS")
    
    # Test query
    cur = conn.cursor()
    cur.execute("SELECT version()")
    version = cur.fetchone()[0]
    print(f"PostgreSQL: {version}")
    
    conn.close()
except Exception as e:
    print(f"Connection: FAILED - {e}")
```

## 10.2 Validate Table Existence

```python
# validate_tables.py
from src.db import get_conn, table_exists, get_table_count
from src.utils import load_yaml

cfg = load_yaml("config/config.yaml")
conn = get_conn(cfg)

tables = [
    'tmp_base_ed_cohort',
    'tmp_ed_event_log',
    'tmp_ed_outcomes',
    'tmp_features_w1',
    'tmp_features_w6',
    'tmp_features_w24',
    'tmp_ecg_features_w1',
    'tmp_ecg_features_w6'
]

print("Table Validation:")
print("-" * 50)

all_ok = True
for table in tables:
    exists = table_exists(conn, table)
    if exists:
        count = get_table_count(conn, table)
        print(f"{table:30s}: OK ({count:,} rows)")
    else:
        print(f"{table:30s}: MISSING")
        all_ok = False

if all_ok:
    print("\nAll tables present!")
else:
    print("\nSome tables missing - run pipeline first.")

conn.close()
```

## 10.3 Check Data Quality (like data_quality.py)

```python
# check_quality.py
from src.db import get_conn, execute_sql
from src.utils import load_yaml

cfg = load_yaml("config/config.yaml")
conn = get_conn(cfg)

print("Data Quality Checks:")
print("=" * 60)

# Check 1: Duplicate stay_ids
sql = """
    SELECT 
        'Cohort duplicates' as check_name,
        COUNT(*) - COUNT(DISTINCT stay_id) as issues
    FROM tmp_base_ed_cohort
"""
result = execute_sql(conn, sql, fetch=True)
issues = result[0]['issues']
print(f"1. Duplicate stay_ids in cohort: {issues} {'PASS' if issues==0 else 'FAIL'}")

# Check 2: Outcomes coverage
sql = """
    SELECT 
        COUNT(*) as cohort_count,
        (SELECT COUNT(*) FROM tmp_ed_outcomes) as outcomes_count
    FROM tmp_base_ed_cohort
"""
result = execute_sql(conn, sql, fetch=True)
r = result[0]
match = r['cohort_count'] == r['outcomes_count']
print(f"2. Cohort-outcomes row match: {r['cohort_count']} vs {r['outcomes_count']} "
      f"{'PASS' if match else 'FAIL'}")

# Check 3: Vital signs coverage
sql = """
    SELECT 
        COUNT(*) as total,
        SUM(CASE WHEN sbp_mean_6h IS NOT NULL THEN 1 ELSE 0 END) as with_vitals
    FROM tmp_features_w6
"""
result = execute_sql(conn, sql, fetch=True)
r = result[0]
coverage = r['with_vitals'] / r['total'] * 100
print(f"3. Vital signs coverage (6h): {coverage:.1f}% "
      f"{'PASS' if coverage > 90 else 'WARNING'}")

# Check 4: ECG coverage
sql = """
    SELECT 
        COUNT(*) as total,
        SUM(CASE WHEN missing_ecg_w6 = 0 THEN 1 ELSE 0 END) as with_ecg
    FROM tmp_ecg_features_w6
"""
result = execute_sql(conn, sql, fetch=True)
r = result[0]
ecg_coverage = r['with_ecg'] / r['total'] * 100
print(f"4. ECG coverage (6h): {ecg_coverage:.1f}% (INFO)")

conn.close()
print("\nQuality check complete!")
```

---


# PART 5: ANALYSIS NOTEBOOKS

### Design
 The pipeline contains 5 notebooks which can used to run the pipeline in a user friendly manner

 NOTE: Please see QUICK_START_DATA_LOADING file first to avoid issues.

## Production Files vs Test Files

### Production Utilities (Root Directory)
These are active utilities for dataset generation and analysis:

| File | Purpose |
|------|---------|
| \generate_datasets.py\ | Standard dataset generation from config |
| \generate_w6_mortality.py\ | W6 mortality classification dataset |
| \generate_advanced_dataset.py\ | Custom feature/outcome combinations |
| \generate_mortality_datasets.py\ | Mortality-focused datasets |
| \generate_phenotyping_datasets.py\ | Phenotyping-specific datasets |

### Test & Validation Files (tests/ Folder)
These are development/testing utilities moved to \	ests/\ folder:

| File | Purpose |
|------|---------|
| \	est_*.py\ | Unit tests for modules |
| \alidate_*.py\ | Data validation scripts |
| \nalyze_*.py\ | Analysis scripts |
| \check_*.py\ | Quick diagnostic checks |
| \generate_test_datasets.py\ | Small test datasets |

**Note:** Test files can be safely referenced but aren't part of production pipeline.

---

# Changelog

## v2.1 — Feb 20, 2026 (SQL Audit & Data Integrity Patch)

### SQL Patches

| File | Change | Rationale |
|------|--------|-----------|
| `00_base_ed_cohort.sql` | Added `age_at_ed <= 110` filter | Exclude implausible ages from anchor-year arithmetic |
| `00_base_ed_cohort.sql` | Added `dod IS NULL OR ed_intime <= dod` | Exclude patients with death date before ED arrival |
| `10_event_icu_admit.sql` | Added `'exact'::text AS event_time_type` | Column alignment fix — table has 6 columns, not 5 |
| `11_event_pressors.sql` | Added `'exact'::text AS event_time_type` | Column alignment fix |
| `12_event_ventilation.sql` | Added `'exact'::text AS event_time_type` | Column alignment fix |
| `13_event_rrt.sql` | Added `'exact'::text AS event_time_type` | Column alignment fix |
| `17_event_death.sql` | Added `'exact'/'day' AS event_time_type` + `admissions.deathtime` preference | Column alignment fix + better death time source |
| `20_outcomes_from_event_log.sql` | Added `event_time >= ed_intime` and 30-day cap to `ev` CTE | Defensive time bounds on outcomes derivation |
| `30_features_w1.sql` | Added `LEAST(ed_outtime, ed_intime + 1h)` clamp | Prevent features from post-discharge data |
| `31_features_w6.sql` | Added `LEAST(ed_outtime, ed_intime + 6h)` clamp (vitals, labs, pyxis) | Prevent features from post-discharge data |
| `32_features_w24.sql` | Added `LEAST(ed_outtime, ed_intime + 24h)` clamp (vitals, labs, pyxis) | Prevent features from post-discharge data |
| `33_ecg_features_w1.sql` | Added `ecg_time IS NOT NULL` defensive filter | Guard against NULL ecg_time values |
| `34_ecg_features_w6.sql` | Added `ecg_time IS NOT NULL` defensive filter | Guard against NULL ecg_time values |
| `99_qa_checks.sql` | **NEW** — 8 automated assertion queries | Post-pipeline data integrity verification |

### Python Changes

| File | Change |
|------|--------|
| `src/validate.py` | Added `run_qa_checks()` function |
| `src/main.py` | Integrated QA checks into pipeline execution |
| 10 source files | Replaced Unicode symbols (✓✗⚠⏱) with ASCII equivalents for Windows cp1252 compatibility |

### Impact

- **Cohort size:** 424,952 → 424,385 (−567 visits removed by age cap + alive-at-arrival filter)
- **Events:** 82,707 → 81,930 (column alignment fix corrected source_table attribution)
- **death_wrong_source QA check:** 4,374 violations → 0 (fixed by event_time_type column addition)
- **All 8 QA checks pass** (7 with 0 violations, 1 INFO-level with 109 expected multi-admission overlaps)

## v2.2 -- Feb 20, 2026 (Python Source Code Hardening)

### Python Fixes

| File | Change | Rationale |
|------|--------|-----------|
| `src/build_event_log.py` | Changed `event_type_map` from `REVASCULARIZATION` to `REVASC`; added `multi_type_map` for PCI/CABG dual-type validation | SQL produces `PCI` and `CABG` rows, not `REVASCULARIZATION`. Validation query now uses `IN ('PCI','CABG')` for REVASC. |
| `src/build_ecg_features.py` | Rewrote `get_ecg_coverage_by_outcome()` to accept `outcome_col` parameter with window-aware defaults; wrapped in try/except | Previously hardcoded `o.deterioration_24h` which was removed in v2.1 outcome refactor. Now auto-selects `deterioration_24h_from_w1/w6/w24`. |
| `src/generate_advanced_dataset.py` | Changed `from src.reproducibility import generate_dataset_metadata` to `try/except ImportError` pattern; guarded call site | Module `src.reproducibility` does not exist -- unguarded import caused crash on any advanced dataset generation. |
| `src/materialize_datasets.py` | Replaced 8-line `stay_id`-specific dedup with `df = df.loc[:, ~df.columns.duplicated()]` | Previous guard only handled `stay_id` duplicates; any future join-induced column duplication is now handled generically. |
| `src/build_features.py` | Rewrote `get_feature_summary()` with window-aware `col_map`: W1=`hr_w1/sbp_w1/rr_w1`, W6=`hr_mean_6h/sbp_mean_6h/rr_mean_6h`, W24=`hr_mean_24h/sbp_mean_24h/rr_mean_24h` | Previously hardcoded `hr_w1`/`sbp_w1`/`rr_w1` for all windows -- W6/W24 columns have different naming conventions. Summary query now adapts to the actual column names per window. |

### Validation (Fix F)

`src/utils.py` `render_sql_template()` already emits warnings for unresolved `{placeholder}` patterns (lines 66-68). No change needed -- confirmed working.

### Test Results

- **18/18 tests pass** (pytest)
- **Pipeline completes successfully** in ~9 min
- **7/8 QA checks pass** (1 INFO-level overlap check, 109 expected cases)
- All 3 materialized datasets generated correctly

### Impact

- No change to cohort size, event counts, or outcome rates (fixes were in Python validation/reporting code, not SQL extraction)
- REVASC validation now correctly counts 1,295 events (PCI + CABG combined)
- Feature summary reporting now works for all 3 windows without KeyError
- Advanced dataset generation no longer crashes on missing reproducibility module
- Duplicate column handling is generic and future-proof

---

**End of Documentation**
