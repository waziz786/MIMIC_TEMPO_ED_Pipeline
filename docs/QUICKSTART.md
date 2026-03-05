# Quick Start Guide

## Setup Instructions

### 1. Install Dependencies

```bash
cd "c:\Users\Lab\Desktop\MIMIC WORK\Cardiac Framework\cardiac_deterioration_pipeline"
pip install -r requirements.txt
```

### 2. Configure Database

1. Copy the example environment file:
```bash
copy .env.example .env
```

2. Edit `.env` and set your PostgreSQL password:
```
PGPASSWORD=your_actual_password
```

3. Edit `config/config.yaml` to match your database settings:
   - Database host, port, name, user
   - MIMIC schema names (if different from defaults)

### 3. Verify Setup

Run the configuration tests:
```bash
python -m pytest tests/test_pipeline.py::test_load_config -v
```

Test database connection:
```bash
python -m pytest tests/test_db.py::test_database_connection -v
```

### 4. Run the Pipeline

Execute the full pipeline:
```bash
python -m src.main
```

Or with options:
```bash
# Skip intermediate validations for faster execution
python -m src.main --skip-validation

# Build only specific feature windows
python -m src.main --windows W1 W6
```

### 5. Check Outputs

After successful execution:
- **Datasets**: `artifacts/datasets/*.csv`
- **Logs**: `artifacts/logs/pipeline_*.log`

### 6. Run Tests

Run the full test suite:
```bash
python -m pytest tests/ -v
```

Run tests with coverage:
```bash
python -m pytest tests/ --cov=src --cov-report=html
```

## Usage Examples

### Generate Custom Datasets

```python
from src.utils import load_yaml
from src.db import get_conn
from src.materialize_datasets import materialize_dataset

# Load config
cfg = load_yaml("config/config.yaml")
conn = get_conn(cfg)

# Materialize a custom dataset
df = materialize_dataset(
    conn, cfg,
    window="W6",
    outcome_col="deterioration_24h",
    out_csv="artifacts/datasets/my_dataset.csv",
    cohort_filter_sql="WHERE age_at_ed >= 65"  # Optional filter
)

print(f"Dataset shape: {df.shape}")
print(f"Outcome rate: {df['y'].mean():.2%}")
```

### Build Only Specific Components

```python
from src.utils import load_yaml
from src.db import get_conn
from src.build_base import build_base
from src.build_event_log import build_event_log

cfg = load_yaml("config/config.yaml")
conn = get_conn(cfg)

# Build only base cohort
build_base(conn, cfg)

# Build only event log
build_event_log(conn, cfg)
```

## Customization

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

## Troubleshooting

### Database Connection Fails

- Check `.env` file has correct password
- Verify PostgreSQL is running
- Check `config.yaml` has correct host/port/database name

### Schema Not Found

- Verify MIMIC data is loaded in PostgreSQL
- Check schema names in `config.yaml` match your installation
- Run: `SELECT schema_name FROM information_schema.schemata;` in psql

### Low Event Counts

- Check that MIMIC-IV ICU and Hospital data are properly linked
- Verify `hadm_id` linking between ED stays and admissions
- Review event SQL files for itemid mappings specific to your MIMIC version

### Missing Features

- Some lab itemids may vary by MIMIC version
- Update lab itemid mappings in feature SQL files
- Check `d_labitems` and `d_items` tables for your specific itemids

## Next Steps

1. Review generated datasets in `artifacts/datasets/`
2. Examine pipeline logs for warnings
3. Customize event definitions for your use case
4. Add domain-specific features
5. Integrate with your ML workflow

For detailed documentation, see [COMPLETE_DOCUMENTATION.md]
