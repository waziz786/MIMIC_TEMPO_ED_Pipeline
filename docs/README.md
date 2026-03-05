# Cardiac Deterioration Pipeline for MIMIC-IV ED

A robust, reusable framework for generating multiple deterioration prediction datasets from MIMIC-IV ED data.

## Features

- **Modular Event Extraction**: ICU admission, pressors, ventilation, RRT, ACS, revascularization, cardiac arrest, death
- **Flexible Outcome Definitions**: Multiple horizon windows (24h, 48h, 72h) configurable via YAML
- **Feature Baskets**: W1 (1hr), W6 (6hr), W24 (24hr) time windows
- **Multiple Dataset Generation**: Easy toggle between feature windows and outcome sets
- **Comprehensive Testing**: Validation suite for data quality and pipeline integrity
- **Database Agnostic**: PostgreSQL-based with configurable schema mapping
- **EEG Data**: Support for EEG-derived features and flags (availability, seizure indicators, summary metrics); can be included in dataset generation

## Directory Structure

```
Cardiac_Deterioration_Pipeline/ [ROOT DIRECTORY]
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
│       ├── 32_features_w24.sql          W24 features (24 hours)
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
    ├── datasets/                        📁 Output CSV files (30+ datasets)
    │   ├── ed_w1_icu24.csv (example)
    │
    └── logs/                            📁 Pipeline logs (generated)
⭐ = Critical component
📁 = Created during execution
```

## Quick Start

### 1. Installation

```bash
pip install -r requirements.txt
```

### 2. Configuration

Copy `.env.example` to `.env` and set your database password:

```bash
PGPASSWORD=your_password_here
```

Edit `config/config.yaml` to match your database setup and MIMIC schema names.

### 3. Run Pipeline

```bash
python -m src.main
```

### 4. Run Tests

```bash
python -m pytest tests/ -v
```

## Usage

### Generate Multiple Datasets

The framework allows you to generate multiple datasets by combining different feature windows and outcome definitions:

```python
from src.materialize_datasets import materialize_dataset
from src.utils import load_yaml
from src.db import get_conn

cfg = load_yaml("config/config.yaml")
conn = get_conn(cfg)

# Dataset 1: W6 features -> deterioration_24h
materialize_dataset(
    conn, cfg,
    window="W6",
    outcome_col="deterioration_24h",
    out_csv="artifacts/datasets/ed_w6_det24.csv"
)

# Dataset 2: W1 features -> icu_24h
materialize_dataset(
    conn, cfg,
    window="W1",
    outcome_col="icu_24h",
    out_csv="artifacts/datasets/ed_w1_icu24.csv"
)
```

### Add Custom Outcomes

Edit `config/outcomes.yaml` to define new outcome sets:

```yaml
custom_outcome:
  horizon_hours: 48
  events:
    - ICU_ADMIT
    - PRESSOR_START
  extra_rules:
    require_initial_non_icu: true
```

### Extend Feature Baskets

Add new feature modules in `sql/` and register them in `config/feature_catalog.yaml`.

## Data Quality Validation

The pipeline includes comprehensive validation:

- **Temporal consistency**: Event times vs ED intime/outtime
- **Completeness checks**: Missing values, join success rates
- **Prevalence sanity**: Expected outcome rates
- **Feature distributions**: Outlier detection

## Design Principles

1. **Modularity**: Each event type isolated in separate SQL file
2. **Reusability**: Template-based SQL rendering for different schemas
3. **Testability**: Comprehensive unit and integration tests
4. **Maintainability**: Clear separation of concerns, documented code
5. **Extensibility**: Easy to add new events, features, or outcomes

### Schema Compatibility

Designed for MIMIC-IV v3.1 (hosp/icu) and MIMIC-IV-ED v2.2. Adjust `config.yaml` for different versions.

## Contributing

When adding new modules:
1. Create SQL in `sql/` with template placeholders
2. Add builder function in `src/`
3. Add tests in `tests/`
4. Update documentation

