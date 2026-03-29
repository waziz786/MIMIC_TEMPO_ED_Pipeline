# MIMIC_ED_Pipeline

A comprehensive framework for extracting, processing MIMIC-IV emergency department patient data to geenrate temporally aligned and analysis ready datatset taht can  predict cardiac deterioration, ICU admission, and mortality etc...

## Quick Start

Use the Notebooks to run the pipeline smoothly:

1. **Start with Quick Start Data Loading** - Load data into SQL
2. **Run notebooks accordingly** - Follow the numbered sequence

### Notebook Sequence

1. Quick Start Loading (loads data to MySQl, skip if already done)
2. [`02_GETTING_STARTED.ipynb`](notebooks/02_GETTING_STARTED.ipynb) - Configuration & verification
3. [`03_PIPELINE_EXECUTION.ipynb`](notebooks/03_PIPELINE_EXECUTION.ipynb) - Run pipeline & build tables
4. [`04_DATASET_GENERATION.ipynb`](notebooks/04_DATASET_GENERATION.ipynb) - Generate analysis datasets
5. [`05_DATA_ANALYSIS.ipynb`](notebooks/05_DATA_ANALYSIS.ipynb) - Exploratory data analysis

## Features

- **Modular Event Extraction**: ICU admission, pressors, ventilation, RRT, ACS, revascularization, cardiac arrest, death
- **Flexible Outcome Definitions**: Multiple horizon windows (24h, 48h, 72h) configurable via YAML
- **Feature Baskets**: W1 (1hr), W6 (6hr), W24 (24hr) time windows
- **Optionla ECG**: option to integreate ECG data for W1 (1hr) and W6 (6hr)
- **Multiple Dataset Generation**: Easy toggle between feature windows and outcome sets
- **Comprehensive Testing**: Validation suite for data quality and pipeline integrity
- **Database Agnostic**: PostgreSQL-based with configurable schema mapping

## Installation

1. Clone this repository:
```bash
git clone https://github.com/YOUR_USERNAME/MIMIC_ED_Pipeline.git
cd MIMIC_ED_Pipeline
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Configure your database connection:
```bash
cp .env.example .env
# Edit .env with your database credentials
```

## Directory Structure

```
MIMIC_Deterioration_Data_Pipeline/
├── config/          # Configuration files (YAML)
├── sql/             # SQL scripts for data extraction
├── src/             # Python source code
├── notebooks/       # Jupyter notebooks for analysis
├── tests/           # Test suite
├── docs/            # Documentation
├── experiments/     # Experimental scripts
└── artifacts/       # Generated datasets and results
```

## Documentation

For detailed documentation, see the [`docs/`](docs/) directory:
- [Complete Documentation](docs/COMPLETE_DOCUMENTATION.md)
- [Quick Start Guide](docs/QUICKSTART.md)
- [Variable Dictionary](docs/VARIABLE_DICTIONARY.md)

## Requirements

- Python 3.8+
- PostgreSQL database
- MIMIC-IV ED data access

## License

See [LICENSE](docs/LICENSE) for details.

## Citation

If you use this pipeline in your research, please cite:
```
[TBA]
```

## Contact

For questions or issues, please open a GitHub issue.
