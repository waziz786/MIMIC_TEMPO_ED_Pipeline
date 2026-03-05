"""
ECG Feature Builder

Builds ECG feature tables for different time windows (W1, W6)
by extracting machine measurements from the first ECG within each window.
"""

import logging
from typing import Dict, Any, List, Optional

from .utils import read_sql, render_sql_template
from .db import run_sql

logger = logging.getLogger(__name__)


def build_ecg_features(
    conn,
    cfg: Dict[str, Any],
    windows: Optional[List[str]] = None
) -> Dict[str, int]:
    """
    Build ECG feature tables for specified time windows.
    
    Args:
        conn: Database connection
        cfg: Configuration dictionary
        windows: List of windows to build (default: ["W1", "W6"])
        
    Returns:
        Dictionary with row counts per window
    """
    logger.info("=" * 60)
    logger.info("BUILDING ECG FEATURE TABLES")
    logger.info("=" * 60)
    
    if windows is None:
        windows = ["W1", "W6"]
    
    # Build template mapping
    mapping = {
        "base_ed_cohort": cfg["tables"]["base_ed_cohort"],
        "ecg_record_list": cfg["tables"]["ecg_record_list"],
        "ecg_machine_measurements": cfg["tables"]["ecg_machine_measurements"],
        "ecg_features_w1": cfg["tables"]["ecg_features_w1"],
        "ecg_features_w6": cfg["tables"]["ecg_features_w6"],
    }
    
    results = {}
    
    for window in windows:
        if window == "W1":
            sql_file = "sql/33_ecg_features_w1.sql"
            table_name = cfg["tables"]["ecg_features_w1"]
        elif window == "W6":
            sql_file = "sql/34_ecg_features_w6.sql"
            table_name = cfg["tables"]["ecg_features_w6"]
        else:
            logger.warning(f"Unknown ECG window: {window}, skipping")
            continue
        
        logger.info(f"Building ECG features for {window}...")
        
        # Read and render SQL
        sql_template = read_sql(sql_file)
        sql = render_sql_template(sql_template, mapping)
        
        # Execute
        run_sql(conn, sql)
        
        # Get row count
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table_name};")
            n_rows = cur.fetchone()[0]
        
        results[window] = n_rows
        logger.info(f"  {window}: {n_rows:,} rows")
    
    logger.info("[OK] ECG feature tables created")
    
    return results


def validate_ecg_features(conn, cfg: Dict[str, Any], window: str = "W6"):
    """
    Validate ECG feature table for a given window.
    
    Args:
        conn: Database connection
        cfg: Configuration dictionary
        window: Window to validate (W1 or W6)
    """
    logger.info(f"Validating ECG features {window}...")
    
    if window == "W1":
        table_name = cfg["tables"]["ecg_features_w1"]
        missing_col = "missing_ecg_w1"
        hr_col = "ecg_hr_w1"
        qrs_col = "ecg_qrs_dur_w1"
    else:
        table_name = cfg["tables"]["ecg_features_w6"]
        missing_col = "missing_ecg_w6"
        hr_col = "ecg_hr_w6"
        qrs_col = "ecg_qrs_dur_w6"
    
    with conn.cursor() as cur:
        # Coverage
        cur.execute(f"""
            SELECT 
                COUNT(*) AS n_total,
                SUM(CASE WHEN {missing_col} = 0 THEN 1 ELSE 0 END) AS n_with_ecg,
                AVG(CASE WHEN {missing_col} = 0 THEN 1.0 ELSE 0.0 END) AS coverage
            FROM {table_name};
        """)
        n_total, n_with_ecg, coverage = cur.fetchone()
        logger.info(f"  Total stays: {n_total:,}")
        logger.info(f"  With ECG: {n_with_ecg:,} ({100*coverage:.1f}%)")
        
        # Heart rate statistics
        cur.execute(f"""
            SELECT 
                AVG({hr_col}) AS mean_hr,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {hr_col}) AS median_hr,
                MIN({hr_col}) AS min_hr,
                MAX({hr_col}) AS max_hr
            FROM {table_name}
            WHERE {hr_col} IS NOT NULL;
        """)
        mean_hr, median_hr, min_hr, max_hr = cur.fetchone()
        if mean_hr:
            logger.info(f"  HR: mean={mean_hr:.1f}, median={median_hr:.1f}, range=[{min_hr:.1f}, {max_hr:.1f}]")
        
        # QRS duration statistics
        cur.execute(f"""
            SELECT 
                AVG({qrs_col}) AS mean_qrs,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {qrs_col}) AS median_qrs
            FROM {table_name}
            WHERE {qrs_col} IS NOT NULL;
        """)
        mean_qrs, median_qrs = cur.fetchone()
        if mean_qrs:
            logger.info(f"  QRS duration: mean={mean_qrs:.1f}ms, median={median_qrs:.1f}ms")


def get_ecg_coverage_by_outcome(conn, cfg: Dict[str, Any], window: str = "W6",
                                 outcome_col: Optional[str] = None):
    """
    Check ECG coverage stratified by outcome.
    
    This helps identify if ECG missingness is differential (biased).
    
    Args:
        conn: Database connection
        cfg: Configuration dictionary
        window: Window to analyze (W1 or W6)
        outcome_col: Prediction-aligned outcome column to stratify by.
                     Defaults to the deterioration_24h_from_wX matching the window.
    """
    logger.info(f"ECG coverage by outcome ({window})...")
    
    # Determine ECG table and columns for the requested window
    if window == "W1":
        ecg_table = cfg["tables"]["ecg_features_w1"]
        missing_col = "missing_ecg_w1"
    else:
        ecg_table = cfg["tables"]["ecg_features_w6"]
        missing_col = "missing_ecg_w6"
    
    # Sensible default: prediction-aligned deterioration label matching the window
    if outcome_col is None:
        outcome_col = {
            "W1": "deterioration_24h_from_w1",
            "W6": "deterioration_24h_from_w6",
            "W24": "deterioration_24h_from_w24",
        }.get(window, "deterioration_24h_from_w6")
    
    outcomes_table = cfg["tables"]["outcomes"]
    
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT 
                    o.{outcome_col} AS y,
                    COUNT(*) AS n_total,
                    SUM(CASE WHEN e.{missing_col} = 0 THEN 1 ELSE 0 END) AS n_with_ecg,
                    AVG(CASE WHEN e.{missing_col} = 0 THEN 1.0 ELSE 0.0 END) AS coverage
                FROM {outcomes_table} o
                JOIN {ecg_table} e USING (stay_id)
                GROUP BY o.{outcome_col}
                ORDER BY o.{outcome_col};
            """)
            
            for row in cur.fetchall():
                det, n_total, n_ecg, coverage = row
                label = f"{outcome_col}=1" if det == 1 else f"{outcome_col}=0"
                logger.info(f"  {label}: {n_ecg:,}/{n_total:,} ({100*coverage:.1f}% ECG coverage)")
    except Exception as e:
        logger.warning(f"  Could not compute ECG coverage by outcome: {e}")
