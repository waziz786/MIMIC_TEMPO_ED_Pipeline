"""
Main orchestration script for the MIMIC deterioration pipeline
"""

import logging
import time
from pathlib import Path
from datetime import datetime

from .utils import load_yaml, setup_logging, format_duration
from .db import get_conn, check_connection
from .build_base import build_base, validate_base_cohort
from .build_event_log import build_event_log, validate_event_log
from .build_outcomes import build_outcomes, validate_outcomes, get_outcome_summary
from .build_features import build_features, validate_features
#from .load_ecg import load_ecg_tables, validate_ecg_tables
from .build_ecg_features import build_ecg_features, validate_ecg_features
from .materialize_datasets import materialize_dataset, materialize_multiple_datasets
from .validate import sanity_counts, validate_pipeline, run_qa_checks

logger = logging.getLogger(__name__)


def run_all(
    config_path: str = "config/config.yaml",
    skip_validation: bool = False,
    windows: list = None
):
    """
    Run the complete pipeline from scratch.
    
    Steps:
    1. Build base ED cohort
    2. Extract and consolidate events
    3. Derive outcomes from events
    4. Build feature baskets
    5. Validate pipeline
    6. Materialize example datasets
    
    Args:
        config_path: Path to config YAML file
        skip_validation: If True, skip intermediate validation steps
        windows: List of feature windows to build (default: all)
    """
    start_time = time.time()
    
    # Setup logging
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = f"artifacts/logs/pipeline_{timestamp}.log"
    
    cfg = load_yaml(config_path)
    verbose = cfg.get("pipeline", {}).get("verbose", True)
    setup_logging(log_file=log_file, verbose=verbose)
    
    logger.info("=" * 80)
    logger.info("MIMIC DETERIORATION PIPELINE")
    logger.info("=" * 80)
    logger.info(f"Config: {config_path}")
    logger.info(f"Log file: {log_file}")
    logger.info(f"Timestamp: {timestamp}")
    logger.info("")
    
    conn = None
    
    try:
        # Test connection
        logger.info("Testing database connection...")
        if not check_connection(cfg):
            raise RuntimeError("Database connection test failed")
        
        # Get connection
        conn = get_conn(cfg)
        
        # STEP 1: Build base cohort
        logger.info("\n")
        step_start = time.time()
        n_visits = build_base(conn, cfg)
        logger.info(f"[TIME] Step 1 completed in {format_duration(time.time() - step_start)}\n")
        
        if not skip_validation:
            validate_base_cohort(conn, cfg)
        
        # STEP 2: Build event log
        logger.info("\n")
        step_start = time.time()
        n_events = build_event_log(conn, cfg)
        logger.info(f"[TIME] Step 2 completed in {format_duration(time.time() - step_start)}\n")
        
        if not skip_validation:
            validate_event_log(conn, cfg)
        
        # STEP 3: Build outcomes
        logger.info("\n")
        step_start = time.time()
        build_outcomes(conn, cfg)
        logger.info(f"[TIME] Step 3 completed in {format_duration(time.time() - step_start)}\n")
        
        if not skip_validation:
            validate_outcomes(conn, cfg)
            get_outcome_summary(conn, cfg)
        
        # STEP 4: Build features
        logger.info("\n")
        step_start = time.time()
        build_features(conn, cfg, windows=windows)
        logger.info(f"[TIME] Step 4 completed in {format_duration(time.time() - step_start)}\n")
        
        if not skip_validation:
            for window in (windows or ["W1", "W6", "W24"]):
                try:
                    validate_features(conn, cfg, window)
                except Exception as e:
                    logger.warning(f"Could not validate {window}: {e}")
        
        # STEP 4.5: Build ECG features (optional - only if ECG config exists)
        if cfg.get("ecg", {}).get("enabled", False):
            logger.info("\n")
            step_start = time.time()
            try:
                build_ecg_features(conn, cfg, windows=["W1", "W6"])
                logger.info(f"[TIME] Step 4.5 (ECG features) completed in {format_duration(time.time() - step_start)}\n")
                
                if not skip_validation:
                    for window in ["W1", "W6"]:
                        try:
                            validate_ecg_features(conn, cfg, window)
                        except Exception as e:
                            logger.warning(f"Could not validate ECG {window}: {e}")
            except FileNotFoundError as e:
                logger.warning(f"ECG data not found, skipping: {e}")
            except Exception as e:
                logger.warning(f"ECG feature extraction failed, skipping: {e}")
        
        # STEP 5: Sanity checks
        logger.info("\n")
        logger.info("Running sanity checks...")
        counts = sanity_counts(conn, cfg)
        logger.info(f"  ED visits: {counts['n_ed_visits']:,}")
        logger.info(f"  Unique patients: {counts['n_unique_patients']:,}")
        logger.info(f"  Events: {counts['n_events']:,}")
        logger.info(f"  Event types: {counts['n_event_types']}")
        logger.info(f"  Deterioration rate (24h from W6): {counts['prev_deterioration_24h_from_w6']*100:.2f}%")
        
        # STEP 6: Full validation
        if not skip_validation:
            logger.info("\n")
            validate_pipeline(conn, cfg)
        
        # STEP 6.5: QA Checks (always run)
        logger.info("\n")
        run_qa_checks(conn, cfg)
        
        # STEP 7: Materialize example datasets
        logger.info("\n")
        logger.info("=" * 60)
        logger.info("MATERIALIZING EXAMPLE DATASETS")
        logger.info("=" * 60)
        
        example_datasets = [
            {
                "name": "ed_w1_icu24",
                "window": "W1",
                "outcome_col": "icu_24h_from_w1",
            },
            {
                "name": "ed_w6_det24",
                "window": "W6",
                "outcome_col": "deterioration_24h_from_w6",
            },
            {
                "name": "ed_w24_det48",
                "window": "W24",
                "outcome_col": "deterioration_48h_from_w24",
            },
        ]
        
        datasets = materialize_multiple_datasets(
            conn, cfg,
            dataset_configs=example_datasets,
            output_dir="artifacts/datasets"
        )
        
        # Final summary
        total_time = time.time() - start_time
        logger.info("\n")
        logger.info("=" * 80)
        logger.info("[OK] PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("=" * 80)
        logger.info(f"Total runtime: {format_duration(total_time)}")
        logger.info(f"Base cohort: {n_visits:,} ED visits")
        logger.info(f"Events extracted: {n_events:,}")
        logger.info(f"Datasets generated: {len(datasets)}")
        logger.info(f"\nOutputs:")
        logger.info(f"  Datasets: artifacts/datasets/")
        logger.info(f"  Logs: {log_file}")
        logger.info("")
        
    except Exception as e:
        logger.error(f"\n{'=' * 80}")
        logger.error("[FAIL] PIPELINE FAILED")
        logger.error(f"{'=' * 80}")
        logger.error(f"Error: {e}", exc_info=True)
        raise
        
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description="MIMIC Deterioration Pipeline for MIMIC-IV ED DATA"
    )
    parser.add_argument(
        "--config",
        default="config/config.yaml",
        help="Path to configuration file"
    )
    parser.add_argument(
        "--skip-validation",
        action="store_true",
        help="Skip intermediate validation steps"
    )
    parser.add_argument(
        "--windows",
        nargs="+",
        choices=["W1", "W6", "W24"],
        help="Specific feature windows to build"
    )
    
    args = parser.parse_args()
    
    run_all(
        config_path=args.config,
        skip_validation=args.skip_validation,
        windows=args.windows
    )
