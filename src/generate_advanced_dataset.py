"""
Generate advanced datasets with multi-outcome and/or multi-window support.

This script provides an easy interface for creating datasets with:
- Multiple outcome columns (e.g., death_24h, death_48h, death_inhospital)
- Multiple feature windows (e.g., W6 + W24 features combined)

Examples:
    # Multi-outcome mortality dataset
    python generate_advanced_dataset.py --outcomes death_24h death_48h death_inhospital --window W6 --cohort admitted --name mortality_multi_outcome
    
    # Multi-window dataset
    python generate_advanced_dataset.py --windows W6 W24 --outcome deterioration_24h --cohort admitted --name multi_window_det24
    
    # Combined: multi-outcome + multi-window
    python generate_advanced_dataset.py --windows W6 W24 --outcomes death_24h death_48h --cohort admitted --name full_mortality
"""

import logging
import argparse
from pathlib import Path

from src.utils import load_yaml, setup_logging
from src.db import get_conn, check_connection
from src.materialize_datasets import materialize_dataset_advanced, COHORT_FILTERS
from src.data_quality import generate_quality_report, generate_feature_summary

# Optional dependency — gracefully degrade if not available
try:
    from src.reproducibility import generate_dataset_metadata
except ImportError:
    generate_dataset_metadata = None

logger = logging.getLogger(__name__)


def generate_advanced_dataset(
    config_path: str = "config/config.yaml",
    windows: list = None,
    outcome_cols: list = None,
    cohort_type: str = "all",
    dataset_name: str = None,
    output_dir: str = "artifacts/datasets",
    include_ecg: bool = False,
    extra_filter: str = "",
    add_missing_ind: bool = True,
    missing_threshold: float = 0.10,
    generate_reports: bool = False
):
    """
    Generate a single advanced dataset with multi-outcome and/or multi-window support.
    """
    # Defaults
    if windows is None:
        windows = ["W6"]
    if outcome_cols is None:
        outcome_cols = ["deterioration_24h"]
    
    # Auto-generate name if not provided
    if dataset_name is None:
        window_str = "_".join([w.lower() for w in windows])
        outcome_str = "_".join(outcome_cols) if len(outcome_cols) <= 2 else f"{len(outcome_cols)}outcomes"
        dataset_name = f"ed_{window_str}_{outcome_str}_{cohort_type}"
    
    # Load config
    cfg = load_yaml(config_path)
    
    logger.info("=" * 70)
    logger.info("ADVANCED DATASET GENERATION")
    logger.info("=" * 70)
    logger.info(f"Dataset name: {dataset_name}")
    logger.info(f"Windows: {windows}")
    logger.info(f"Outcomes: {outcome_cols}")
    logger.info(f"Cohort: {cohort_type}")
    logger.info(f"Output: {output_dir}/{dataset_name}.csv")
    
    # Test connection
    if not check_connection(cfg):
        raise RuntimeError("Database connection failed")
    
    conn = get_conn(cfg)
    
    try:
        out_csv = f"{output_dir}/{dataset_name}.csv"
        
        df = materialize_dataset_advanced(
            conn=conn,
            cfg=cfg,
            windows=windows,
            outcome_cols=outcome_cols,
            out_csv=out_csv,
            cohort_type=cohort_type,
            cohort_filter_sql=extra_filter,
            include_ecg=include_ecg,
            add_missing_ind=add_missing_ind,
            missing_threshold=missing_threshold
        )
        
        # Generate metadata (if reproducibility module is available)
        try:
            if generate_dataset_metadata is None:
                raise ImportError("generate_dataset_metadata not available")
            multi_outcome = len(outcome_cols) > 1
            outcome_rates = {}
            if multi_outcome:
                for oc in outcome_cols:
                    col = f"y_{oc}"
                    if col in df.columns:
                        outcome_rates[oc] = df[col].mean()
            else:
                if 'y' in df.columns:
                    outcome_rates[outcome_cols[0]] = df['y'].mean()
            
            generate_dataset_metadata(
                dataset_name=dataset_name,
                n_rows=len(df),
                n_columns=len(df.columns),
                outcome_rate=outcome_rates if multi_outcome else outcome_rates.get(outcome_cols[0]),
                config=cfg,
                dataset_config={
                    "windows": windows,
                    "outcomes": outcome_cols,
                    "cohort_type": cohort_type,
                    "multi_window": len(windows) > 1,
                    "multi_outcome": multi_outcome,
                    "description": f"Advanced dataset: {windows} windows, {outcome_cols} outcomes"
                },
                output_path=f"{output_dir}/metadata/{dataset_name}_metadata.json"
            )
        except Exception as e:
            logger.debug(f"Could not save metadata: {e}")
        
        # Generate data quality reports if requested
        if generate_reports:
            try:
                reports_dir = f"{output_dir}/reports"
                Path(reports_dir).mkdir(parents=True, exist_ok=True)
                
                generate_quality_report(df, dataset_name, reports_dir)
                generate_feature_summary(df, f"{reports_dir}/{dataset_name}_features.csv")
                logger.info(f"Quality reports saved to {reports_dir}")
            except Exception as e:
                logger.warning(f"Could not generate quality reports: {e}")
        
        logger.info("\n" + "=" * 70)
        logger.info("DATASET GENERATED SUCCESSFULLY")
        logger.info("=" * 70)
        logger.info(f"Rows: {len(df):,}")
        logger.info(f"Columns: {len(df.columns)}")
        
        return df
        
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="Generate advanced datasets with multi-outcome/multi-window support",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Multi-outcome mortality dataset (death at 24h, 48h, in-hospital)
  python generate_advanced_dataset.py --outcomes death_24h death_48h death_inhospital --window W6 --cohort admitted --name mortality_multi_outcome
  
  # Multi-window dataset (W6 + W24 features)
  python generate_advanced_dataset.py --windows W6 W24 --outcome deterioration_24h --cohort admitted --name multi_window_det24
  
  # Single window, single outcome (like standard behavior)
  python generate_advanced_dataset.py --window W6 --outcome death_24h --cohort all --name w6_death24_all
        """
    )
    
    parser.add_argument("--config", default="config/config.yaml", help="Main config file")
    parser.add_argument("--output-dir", default="artifacts/datasets", help="Output directory")
    parser.add_argument("--name", dest="dataset_name", help="Dataset name (auto-generated if not provided)")
    
    # Window options (mutually compatible)
    parser.add_argument("--window", dest="single_window", help="Single feature window (W1, W6, or W24)")
    parser.add_argument("--windows", nargs="+", help="Multiple feature windows for multi-window mode")
    
    # Outcome options (mutually compatible)
    parser.add_argument("--outcome", dest="single_outcome", help="Single outcome column")
    parser.add_argument("--outcomes", nargs="+", help="Multiple outcome columns for multi-outcome mode")
    
    # Cohort and filters
    parser.add_argument("--cohort", default="all", choices=["all", "admitted", "not_admitted"], help="Cohort filter")
    parser.add_argument("--filter", default="", help="Additional SQL filter")
    
    # Options
    parser.add_argument("--ecg", action="store_true", help="Include ECG features")
    parser.add_argument("--reports", action="store_true", help="Generate data quality reports")
    parser.add_argument("--no-missing-indicators", action="store_true", help="Disable automatic missing indicators")
    parser.add_argument("--missing-threshold", type=float, default=0.10, help="Missing threshold for indicators")
    parser.add_argument("--verbose", action="store_true", help="Verbose logging")
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(verbose=args.verbose)
    
    # Resolve windows
    if args.windows:
        windows = args.windows
    elif args.single_window:
        windows = [args.single_window]
    else:
        windows = ["W6"]  # Default
    
    # Resolve outcomes
    if args.outcomes:
        outcome_cols = args.outcomes
    elif args.single_outcome:
        outcome_cols = [args.single_outcome]
    else:
        outcome_cols = ["deterioration_24h"]  # Default
    
    # Generate dataset
    df = generate_advanced_dataset(
        config_path=args.config,
        windows=windows,
        outcome_cols=outcome_cols,
        cohort_type=args.cohort,
        dataset_name=args.dataset_name,
        output_dir=args.output_dir,
        include_ecg=args.ecg,
        extra_filter=args.filter,
        add_missing_ind=not args.no_missing_indicators,
        missing_threshold=args.missing_threshold,
        generate_reports=args.reports
    )
    
    return df


if __name__ == "__main__":
    main()
