# MIMIC Deterioration Pipeline Summary

##  What Has Been Built

A **production-ready, modular framework** for generating multiple deterioration prediction datasets from MIMIC-IV Emergency Department data.

##  Complete File Structure

```
MIMIC_deterioration_pipeline/          [ROOT DIRECTORY]
│
├── 📋 Documentation (5 files)
│   ├── README.md                        ⭐ Complete documentation
│   ├── QUICKSTART.md                    ⭐ Setup instructions  
│   ├── IMPLEMENTATION_SUMMARY.md        ⭐ Technical details
│   ├── LICENSE                          MIT License
│   └── PROJECT_COMPLETE.md             This file
│
├── ⚙️ Configuration (4 files)
│   ├── .env.example                    Password template
│   ├── .gitignore                      Git exclusions
│   ├── requirements.txt                Python dependencies
│   └── pytest.ini                      Test configuration
│
├── 📂 config/ (3 YAML files)
│   ├── config.yaml                     ⭐ Database & pipeline settings
│   ├── outcomes.yaml                   ⭐ 15+ outcome definitions
│   └── feature_catalog.yaml            ⭐ Feature organization
│
├── 🗃️ sql/ (13 SQL scripts)
│   ├── 00_base_ed_cohort.sql          ⭐ Base cohort (adult ED visits)
│   │
│   ├── Event Extractors (8 files)
│   ├── 10_event_icu_admit.sql         ICU admission
│   ├── 11_event_pressors.sql          Vasopressor start
│   ├── 12_event_ventilation.sql       Mechanical ventilation
│   ├── 13_event_rrt.sql               Renal replacement therapy
│   ├── 14_event_acs.sql               Acute coronary syndrome
│   ├── 15_event_revasc.sql            PCI/CABG procedures
│   ├── 16_event_cardiac_arrest.sql    Cardiac arrest
│   ├── 17_event_death.sql             Mortality
│   │
│   ├── Outcomes & Features (4 files)
│   ├── 20_outcomes_from_event_log.sql ⭐ 15+ outcome indicators
│   ├── 30_features_w1.sql             W1 features (1 hour)
│   ├── 31_features_w6.sql             W6 features (6 hours)
│   └── 32_features_w24.sql            W24 features (24 hours)
│
├── 🐍 src/ (9 Python modules)
│   ├── __init__.py                    Package initialization
│   ├── db.py                          ⭐ Database utilities
│   ├── utils.py                       ⭐ Helper functions
│   ├── build_base.py                  ⭐ Base cohort builder
│   ├── build_event_log.py             ⭐ Event log builder
│   ├── build_outcomes.py              ⭐ Outcomes builder
│   ├── build_features.py              ⭐ Features builder
│   ├── materialize_datasets.py        ⭐ Dataset generator
│   ├── validate.py                    ⭐ Validation suite
│   └── main.py                        ⭐ Main orchestration
│
├── 🧪 tests/ (4 test modules)
│   ├── __init__.py
│   ├── test_db.py                     Database tests
│   ├── test_pipeline.py               Pipeline tests
│   └── test_validation.py             Validation tests
│
├── 📊 artifacts/
│   ├── datasets/                      📁 Output CSV files (generated)
│   └── logs/                          📁 Pipeline logs (generated)
│
└── 🔧 Utilities (2 scripts)
    ├── validate_setup.py              ⭐ Pre-flight validation
    └── setup_wizard.py                ⭐ Interactive setup

⭐ = Critical component
📁 = Created during execution
```


## Core Capabilities

### 1. Event Extraction (8 Event Types)
✅ ICU admission (accurate timestamps)
✅ Vasopressor initiation (itemid-based)
✅ Mechanical ventilation (procedure + charting)
✅ Renal replacement therapy
✅ Acute coronary syndrome (ICD codes)
✅ Revascularization (PCI/CABG)
✅ Cardiac arrest (ICD codes)
✅ In-hospital mortality (accurate timestamps)

### 2. Outcome Generation (15+ Outcomes)
✅ Deterioration (24h, 48h)
✅ ICU admission (24h, 48h)
✅ Vasopressors (24h, 48h)
✅ Ventilation (24h, 48h)
✅ RRT (24h)
✅ ACS (72h)
✅ Revascularization (72h)
✅ Cardiac arrest (24h)
✅ Death (24h, 48h, 72h, in-hospital)
✅ Time-to-event metrics (ed los, etc.)

### 3. Feature Engineering (3 Windows)
✅ **W1** (1 hour): Triage vitals + first assessments
✅ **W6** (6 hours): Vital summaries + early labs
✅ **W24** (24 hours): Comprehensive features

### 4. Data Quality & Validation
✅ Pre-flight validation script
✅ Runtime validation after each step
✅ Temporal consistency checks
✅ Logical outcome validation
✅ Feature completeness analysis
✅ Comprehensive test suite

### 5. Multiple Dataset Generation
✅ Mix any feature window with any outcome
✅ Apply custom cohort filters
✅ Generate unlimited dataset combinations
✅ Automatic CSV export

## Quick Start

### Option 1: Interactive Setup (Recommended)
```bash
python setup_wizard.py
```

### Option 2: Manual Setup
```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Configure database
copy .env.example .env
# Edit .env with your password

# 3. Update config
# Edit config/config.yaml

# 4. Validate setup
python validate_setup.py

# 5. Run pipeline
python -m src.main
```

### Option 3: Run Tests First
```bash
python -m pytest tests/ -v
```

## Usage Examples

### Generate Default Datasets
```bash
python -m src.main
```

Produces:
- `ed_w1_icu24.csv` - W1 features → ICU 24h
- `ed_w6_det24.csv` - W6 features → Deterioration 24h
- `ed_w24_det48.csv` - W24 features → Deterioration 48h

### Generate Custom Dataset
```python
from src.utils import load_yaml
from src.db import get_conn
from src.materialize_datasets import materialize_dataset

cfg = load_yaml("config/config.yaml")
conn = get_conn(cfg)

df = materialize_dataset(
    conn, cfg,
    window="W6",
    outcome_col="icu_24h",
    out_csv="artifacts/datasets/custom.csv",
    cohort_filter_sql="WHERE age_at_ed >= 65 AND age_at_ed < 90"
)
```

### Build Only Specific Components
```python
from src.build_base import build_base
from src.build_event_log import build_event_log

cfg = load_yaml("config/config.yaml")
conn = get_conn(cfg)

# Build base cohort only
build_base(conn, cfg)

# Build event log only
build_event_log(conn, cfg)
```

## Customization Guide

### Add New Event Type

1. **Create SQL file**: `sql/XX_event_new.sql`
```sql
SELECT
  b.subject_id,
  b.stay_id,
  b.hadm_id,
  'NEW_EVENT'::text AS event_type,
  e.event_time AS event_time,
  'source_table'::text AS source_table
FROM {base_ed_cohort} b
INNER JOIN source_table e ON ...
```

2. **Register in pipeline**: Edit `src/build_event_log.py`
```python
EVENT_SQL_FILES = [
    ...
    ("sql/XX_event_new.sql", "New Event Description"),
]
```

3. **Use in outcomes**: Edit `config/outcomes.yaml`
```yaml
my_outcome:
  horizon_hours: 24
  events:
    - NEW_EVENT
```

### Add New Feature Window

1. **Create SQL file**: `sql/3X_features_wN.sql`
2. **Register**: Update `FEATURE_WINDOWS` in `src/build_features.py`
3. **Document**: Add to `config/feature_catalog.yaml`

### Define Custom Outcome

Simply edit `config/outcomes.yaml`:
```yaml
custom_outcome:
  description: "My custom outcome"
  horizon_hours: 48
  events:
    - ICU_ADMIT
    - PRESSOR_START
    - VENT_START
```

Then use it:
```python
materialize_dataset(
    conn, cfg,
    window="W6",
    outcome_col="custom_outcome",
    out_csv="my_custom_dataset.csv"
)
```

## ✅ Quality Assurance

### Code Quality
- ✅ Type hints throughout
- ✅ Comprehensive docstrings
- ✅ PEP 8 style guide
- ✅ Error handling & recovery
- ✅ Input validation

### SQL Quality
- ✅ Template-based (schema-agnostic)
- ✅ Indexed for performance
- ✅ Commented logic
- ✅ Temporal filters

### Testing
- ✅ unit tests
- ✅ Integration tests
- ✅ Database connectivity tests
- ✅ Configuration validation
- ✅ Data quality checks

### Documentation
- ✅ COMPLETE_DOCUMENTATION (comprehensive)
- ✅ QUICKSTART( PosgresSQL setup guide)
- ✅ README
- ✅ Inline code docs
- ✅ Usage examples

##  Design Patterns

### 1. Modular Architecture
Each component is self-contained and independently testable

### 2. Configuration-Driven
No code changes needed for different databases/schemas

### 3. Template-Based SQL
Reusable across different MIMIC installations

### 4. Feature Baskets
Organized feature engineering by time window

### 5. Event-Based Outcomes
Clean separation between event extraction and outcome derivation

### 6. Multiple Datasets
Generate unlimited combinations without rebuilding

## Performance Considerations

- **Indexes**: Automatic index creation on key columns
- **Batch Processing**: Efficient SQL joins
- **Incremental Builds**: Each step can be run independently
- **Logging**: Detailed timing for each operation
- **Validation**: Optional (use `--skip-validation` for speed)

## Troubleshooting

### Database Connection Issues
```bash
# Test connection
python validate_setup.py

# Check config
cat config/config.yaml

# Verify password
cat .env
```

### Missing Data
```bash
# Check table counts
python validate_setup.py

# Review logs
cat artifacts/logs/pipeline_*.log
```

### Low Event Counts
- Verify MIMIC data is fully loaded
- Check `hadm_id` linkage between tables
- Review itemid mappings for your MIMIC version

## Next Steps

1. **Verify Setup**: `python validate_setup.py`
2. **Run Tests**: `python -m pytest tests/ -v`
3. **Execute Pipeline**: `python -m src.main`
4. **Review Outputs**: Check `artifacts/datasets/` and `artifacts/logs/`
5. **Customize**: Add your own events, features, or outcomes
6. **Integrate**: Use generated datasets in your ML pipeline

## Success Criteria

You'll know the pipeline is working correctly when:

✅ Validation script passes all checks
✅ Tests run successfully
✅ Pipeline completes without errors
✅ Dataset CSV files are generated
✅ Logs show expected row counts
✅ Outcome prevalence rates are reasonable


### Ready to Run

```bash
# Quick validation
python validate_setup.py

# Full pipeline
python -m src.main

# Check outputs
dir artifacts\datasets\
```

---

**Let's build better deterioration models!**
