"""
Generate Example Datasets with New Outcomes and ECG Features

This script materializes datasets using:
- New outcome windows (24h deterioration, 24-48h ward failure, 72h coronary events)
- Optional ECG features (W1 and W6 windows)
- Cohort filtering (all, admitted only, etc.)

Usage:
    python -m src.make_datasets
    python -m src.make_datasets --help
"""

import logging
import argparse
from pathlib import Path
from typing import Dict, Any

from .utils import load_yaml, setup_logging
from .db import get_conn
from .materialize_datasets import materialize_dataset, get_dataset_summary

logger = logging.getLogger(__name__)


# Dataset specifications for different research questions
DATASET_SPECS = {
    # ============================================
    # MULTI-WINDOW COMPARISON (A1) - all ED, no ECG
    # ============================================
    "ed_w1_det24_all": {
        "description": "Deterioration 1h-25h (W1 features, all patients, aligned)",
        "window": "W1",
        "outcome_col": "deterioration_24h_from_w1",
        "cohort_type": "all",
        "include_ecg": False,
    },
    "ed_w6_det24_all": {
        "description": "Early deterioration 6h-30h (W6 features, all patients, aligned)",
        "window": "W6",
        "outcome_col": "deterioration_24h_from_w6",
        "cohort_type": "all",
        "include_ecg": False,
    },
    "ed_w24_det24_all": {
        "description": "Deterioration 24h-48h (W24 features, all patients, aligned)",
        "window": "W24",
        "outcome_col": "deterioration_24h_from_w24",
        "cohort_type": "all",
        "include_ecg": False,
    },

    # ============================================
    # EARLY DETERIORATION (24h) - admitted, with ECG
    # ============================================
    "ed_w6_det24_admitted": {
        "description": "Early deterioration 6h-30h (W6 features, admitted patients, aligned)",
        "window": "W6",
        "outcome_col": "deterioration_24h_from_w6",
        "cohort_type": "admitted",
        "include_ecg": True,
    },
    "ed_w1_det24_admitted": {
        "description": "Early deterioration 1h-25h (W1 features, admitted patients, aligned)",
        "window": "W1",
        "outcome_col": "deterioration_24h_from_w1",
        "cohort_type": "admitted",
        "include_ecg": True,
    },
    
    # ============================================
    # CORONARY EVENTS (hospitalization-level) - Cardiac pathway
    # ============================================
    "ed_w6_coronary_hosp_admitted": {
        "description": "Coronary event during hospitalization (W6 features, admitted patients)",
        "window": "W6",
        "outcome_col": "coronary_event_hosp",
        "cohort_type": "admitted",
        "include_ecg": True,
    },
    "ed_w6_revasc_hosp_admitted": {
        "description": "Revascularization during hospitalization (W6 features, admitted patients)",
        "window": "W6",
        "outcome_col": "revasc_hosp",
        "cohort_type": "admitted",
        "include_ecg": True,
    },
    
    # ============================================
    # MULTI-OUTCOME (A2) - W6, admitted, no ECG
    # ============================================
    "ed_w6_icu24_admitted": {
        "description": "ICU admission 6h-30h (W6 features, admitted patients, aligned)",
        "window": "W6",
        "outcome_col": "icu_24h_from_w6",
        "cohort_type": "admitted",
        "include_ecg": False,
    },
    "ed_w6_death24_admitted": {
        "description": "Death 6h-30h (W6 features, admitted patients, aligned)",
        "window": "W6",
        "outcome_col": "death_24h_from_w6",
        "cohort_type": "admitted",
        "include_ecg": False,
    },
    "ed_w6_vent24_admitted": {
        "description": "Mechanical ventilation 6h-30h (W6 features, admitted patients, aligned)",
        "window": "W6",
        "outcome_col": "vent_24h_from_w6",
        "cohort_type": "admitted",
        "include_ecg": False,
    },
    "ed_w6_pressor24_admitted": {
        "description": "Vasopressor start 6h-30h (W6 features, admitted patients, aligned)",
        "window": "W6",
        "outcome_col": "pressor_24h_from_w6",
        "cohort_type": "admitted",
        "include_ecg": False,
    },

    # ============================================
    # ECG COMPARISON (A3) - cardiac outcomes, no-ECG vs ECG
    # ============================================
    "ed_w6_cardiac_arrest_admitted": {
        "description": "Cardiac arrest hosp (W6 features, admitted, NO ECG)",
        "window": "W6",
        "outcome_col": "cardiac_arrest_hosp",
        "cohort_type": "admitted",
        "include_ecg": False,
    },
    "ed_w6_cardiac_arrest_ecg_admitted": {
        "description": "Cardiac arrest hosp (W6 features, admitted, WITH ECG)",
        "window": "W6",
        "outcome_col": "cardiac_arrest_hosp",
        "cohort_type": "admitted",
        "include_ecg": True,
    },
    "ed_w6_acs_admitted": {
        "description": "ACS hosp (W6 features, admitted, NO ECG)",
        "window": "W6",
        "outcome_col": "acs_hosp",
        "cohort_type": "admitted",
        "include_ecg": False,
    },
    "ed_w6_acs_ecg_admitted": {
        "description": "ACS hosp (W6 features, admitted, WITH ECG)",
        "window": "W6",
        "outcome_col": "acs_hosp",
        "cohort_type": "admitted",
        "include_ecg": True,
    },

    # ============================================
    # WITHOUT ECG (for comparison / larger cohort)
    # ============================================
    "ed_w6_det24_no_ecg": {
        "description": "Early deterioration 6h-30h (W6 features, no ECG, aligned)",
        "window": "W6",
        "outcome_col": "deterioration_24h_from_w6",
        "cohort_type": "admitted",
        "include_ecg": False,
    },
    "ed_w24_det48_admitted": {
        "description": "Deterioration 24h-72h (W24 features, admitted patients, aligned)",
        "window": "W24",
        "outcome_col": "deterioration_48h_from_w24",
        "cohort_type": "admitted",
        "include_ecg": False,  # ECG not available for W24
    },
}


def list_available_datasets():
    """Print available dataset specifications."""
    print("\nAvailable Dataset Specifications:")
    print("=" * 70)
    for name, spec in DATASET_SPECS.items():
        ecg_flag = "[ECG]" if spec.get("include_ecg", False) else "  ---"
        print(f"  {name:<35} {spec['window']:<4} {spec['outcome_col']:<25} {ecg_flag}")
    print()


def make_dataset(
    conn,
    cfg: Dict[str, Any],
    dataset_name: str,
    output_dir: str = "artifacts/datasets"
) -> Any:
    """
    Generate a single dataset by name.
    
    Args:
        conn: Database connection
        cfg: Configuration dictionary
        dataset_name: Name of dataset specification
        output_dir: Output directory for CSV files
        
    Returns:
        DataFrame with the generated dataset
    """
    if dataset_name not in DATASET_SPECS:
        raise ValueError(f"Unknown dataset: {dataset_name}. Use --list to see available options.")
    
    spec = DATASET_SPECS[dataset_name]
    
    logger.info(f"\n{'=' * 60}")
    logger.info(f"Generating: {dataset_name}")
    logger.info(f"Description: {spec['description']}")
    logger.info(f"{'=' * 60}")
    
    out_csv = f"{output_dir}/{dataset_name}.csv"
    
    df = materialize_dataset(
        conn=conn,
        cfg=cfg,
        window=spec["window"],
        outcome_col=spec["outcome_col"],
        out_csv=out_csv,
        cohort_type=spec.get("cohort_type", "all"),
        include_ecg=spec.get("include_ecg", False),
    )
    
    # Print summary
    get_dataset_summary(df, dataset_name)
    
    return df


def make_all_datasets(
    conn,
    cfg: Dict[str, Any],
    output_dir: str = "artifacts/datasets"
) -> Dict[str, Any]:
    """
    Generate all available datasets.
    
    Args:
        conn: Database connection
        cfg: Configuration dictionary
        output_dir: Output directory for CSV files
        
    Returns:
        Dictionary mapping dataset names to DataFrames
    """
    results = {}
    
    for name in DATASET_SPECS:
        try:
            df = make_dataset(conn, cfg, name, output_dir)
            results[name] = df
        except Exception as e:
            logger.error(f"Failed to generate {name}: {e}")
    
    return results


def run(
    config_path: str = "config/config.yaml",
    datasets: list = None,
    output_dir: str = "artifacts/datasets",
    list_only: bool = False
):
    """
    Main entry point for dataset generation.
    
    Args:
        config_path: Path to configuration file
        datasets: List of dataset names to generate (None = all)
        output_dir: Output directory for CSV files
        list_only: If True, just list available datasets
    """
    if list_only:
        list_available_datasets()
        return
    
    # Setup
    setup_logging(verbose=True)
    cfg = load_yaml(config_path)
    
    # Ensure output directory exists
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    conn = get_conn(cfg)
    
    try:
        if datasets:
            # Generate specific datasets
            for name in datasets:
                make_dataset(conn, cfg, name, output_dir)
        else:
            # Generate key datasets (subset of all)
            key_datasets = [
                "ed_w6_det24_admitted",
                "ed_w6_ward_failure_admitted",
                "ed_w6_coronary_hosp_admitted",
            ]
            for name in key_datasets:
                make_dataset(conn, cfg, name, output_dir)
        
        logger.info("\n" + "=" * 60)
        logger.info("[OK] Dataset generation complete")
        logger.info(f"Output directory: {output_dir}")
        logger.info("=" * 60)
        
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate analysis datasets with ECG features"
    )
    parser.add_argument(
        "--config",
        default="config/config.yaml",
        help="Path to configuration file"
    )
    parser.add_argument(
        "--datasets",
        nargs="+",
        help="Specific datasets to generate (default: key datasets)"
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Generate all available datasets"
    )
    parser.add_argument(
        "--output-dir",
        default="artifacts/datasets",
        help="Output directory for CSV files"
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List available dataset specifications"
    )
    
    args = parser.parse_args()
    
    if args.list:
        list_available_datasets()
    elif args.all:
        run(
            config_path=args.config,
            datasets=list(DATASET_SPECS.keys()),
            output_dir=args.output_dir
        )
    else:
        run(
            config_path=args.config,
            datasets=args.datasets,
            output_dir=args.output_dir
        )
