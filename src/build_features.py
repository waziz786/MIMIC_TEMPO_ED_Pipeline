"""
Build feature basket tables (W1,W6,W24)
"""

import logging
from typing import Dict, Any, List
import psycopg2

from .utils import read_sql, render_sql_template, get_sql_mapping
from .db import execute_with_progress, get_table_row_count

logger = logging.getLogger(__name__)


FEATURE_WINDOWS = {
    "W1": {
        "sql_file": "sql/30_features_w1.sql",
        "description": "W1 features (first hour)",
        "table_key": "features_w1"
    },
    "W6": {
        "sql_file": "sql/31_features_w6.sql",
        "description": "W6 features (first 6 hours)",
        "table_key": "features_w6"
    },
    "W24": {
        "sql_file": "sql/32_features_w24.sql",
        "description": "W24 features (first 24 hours)",
        "table_key": "features_w24"
    },
    "W6T": {
        "sql_file": "sql/35_features_w6_truncated.sql",
        "description": "W6 features TRUNCATED at event time (leakage-proof)",
        "table_key": "features_w6_truncated"
    },
    "W24T": {
        "sql_file": "sql/36_features_w24_truncated.sql",
        "description": "W24 features TRUNCATED at event time (leakage-proof)",
        "table_key": "features_w24_truncated"
    },
}


def build_features(
    conn: psycopg2.extensions.connection,
    cfg: Dict[str, Any],
    windows: List[str] = None
) -> Dict[str, int]:
    """
    Build feature basket tables for specified windows.
    
    Args:
        conn: Database connection
        cfg: Configuration dictionary
        windows: List of window names to build (default: all windows)
        
    Returns:
        Dictionary mapping window names to row counts
        
    Raises:
        Exception: If build fails
    """
    logger.info("=" * 60)
    logger.info("BUILDING FEATURE BASKETS")
    logger.info("=" * 60)
    
    if windows is None:
        windows = ["W1", "W6", "W24"]
    
    counts = {}
    
    try:
        mapping = get_sql_mapping(cfg)
        
        for window in windows:
            if window not in FEATURE_WINDOWS:
                logger.warning(f"Unknown window: {window}, skipping")
                continue
            
            window_info = FEATURE_WINDOWS[window]
            
            logger.info(f"Building {window_info['description']}...")
            
            # Load and render SQL
            sql = read_sql(window_info["sql_file"])
            sql = render_sql_template(sql, mapping)
            
            # Execute
            execute_with_progress(
                conn, sql,
                f"Creating {window} feature table"
            )
            
            # Get count
            table_name = cfg["tables"][window_info["table_key"]]
            n_rows = get_table_row_count(conn, table_name)
            counts[window] = n_rows
            
            logger.info(f"  [OK] {window}: {n_rows:,} rows")
        
        logger.info(f"Feature baskets created: {len(counts)} windows")
        return counts
        
    except Exception as e:
        logger.error(f"Failed to build features: {e}")
        raise


def validate_features(
    conn: psycopg2.extensions.connection,
    cfg: Dict[str, Any],
    window: str
) -> Dict[str, Any]:
    """
    Validate a feature basket for completeness and quality.
    
    Args:
        conn: Database connection
        cfg: Configuration dictionary
        window: Window name (W1, W6, or W24)
        
    Returns:
        Dictionary with validation results
    """
    from .db import fetch_df
    
    if window not in FEATURE_WINDOWS:
        raise ValueError(f"Unknown window: {window}")
    
    table_name = cfg["tables"][FEATURE_WINDOWS[window]["table_key"]]
    
    logger.info(f"Validating {window} features...")
    
    # Get column statistics
    column_stats_sql = f"""
    SELECT
        column_name,
        data_type
    FROM information_schema.columns
    WHERE table_name = '{table_name.split('.')[-1]}'
    AND column_name NOT IN ('stay_id', 'subject_id', 'hadm_id')
    ORDER BY ordinal_position
    """
    
    columns_df = fetch_df(conn, column_stats_sql)
    
    if columns_df.empty:
        logger.warning(f"Could not retrieve column information for {table_name}")
        return {}
    
    # Build dynamic SQL to check missingness for numeric columns
    numeric_cols = columns_df[
        columns_df['data_type'].isin(['numeric', 'double precision', 'real', 'integer', 'bigint'])
    ]['column_name'].tolist()
    
    if not numeric_cols:
        logger.warning(f"No numeric columns found in {table_name}")
        return {}
    
    # Sample of columns to check (avoid too many)
    sample_cols = numeric_cols[:20] if len(numeric_cols) > 20 else numeric_cols
    
    missing_checks = [
        f"AVG(CASE WHEN {col} IS NULL THEN 1.0 ELSE 0.0 END) AS missing_{col}"
        for col in sample_cols
    ]
    
    validation_sql = f"""
    SELECT
        COUNT(*) AS total_rows,
        {', '.join(missing_checks)}
    FROM {table_name}
    """
    
    try:
        df = fetch_df(conn, validation_sql)
        results = df.to_dict('records')[0]
        
        logger.info(f"{window} Feature Validation:")
        logger.info(f"  Total rows: {results['total_rows']:,}")
        logger.info(f"  Total features: {len(columns_df)}")
        
        # Report high missingness
        high_missing = []
        for col in sample_cols:
            missing_rate = results.get(f'missing_{col}', 0)
            if missing_rate > 0.5:
                high_missing.append((col, missing_rate))
        
        if high_missing:
            logger.warning(f"  Features with >50% missing:")
            for col, rate in sorted(high_missing, key=lambda x: x[1], reverse=True)[:10]:
                logger.warning(f"    {col}: {rate*100:.1f}%")
        else:
            logger.info(f"  [OK] No features with >50% missing (checked {len(sample_cols)} features)")
        
        return results
        
    except Exception as e:
        logger.error(f"Validation query failed: {e}")
        return {}


def get_feature_summary(
    conn: psycopg2.extensions.connection,
    cfg: Dict[str, Any],
    window: str
) -> None:
    """
    Print detailed feature summary statistics.
    
    Currently implemented for W1 only (uses hardcoded column names for W1).
    For W6/W24 use validate_features() which is window-safe.
    
    Args:
        conn: Database connection
        cfg: Configuration dictionary
        window: Window name
    """
    from .db import fetch_df
    
    if window not in FEATURE_WINDOWS:
        raise ValueError(f"Unknown window: {window}")
    
    # Column names differ by window; only W1 columns are hardcoded below
    # Map window -> (hr_col, sbp_col, rr_col)
    col_map = {
        "W1":  ("hr_w1",       "sbp_w1",       "rr_w1"),
        "W6":  ("hr_mean_6h",  "sbp_mean_6h",  "rr_mean_6h"),
        "W24": ("hr_mean_24h", "sbp_mean_24h", "rr_mean_24h"),
    }
    
    if window not in col_map:
        logger.info(f"Feature summary not available for window {window}; skipping.")
        return
    
    hr_col, sbp_col, rr_col = col_map[window]
    table_name = cfg["tables"][FEATURE_WINDOWS[window]["table_key"]]
    
    logger.info(f"\n{window} Feature Summary:")
    
    vitals_sql = f"""
    SELECT
        'HR' AS vital,
        MIN({hr_col}) AS min_val,
        AVG({hr_col}) AS mean_val,
        MAX({hr_col}) AS max_val
    FROM {table_name}
    WHERE {hr_col} IS NOT NULL
    
    UNION ALL
    
    SELECT
        'SBP' AS vital,
        MIN({sbp_col}),
        AVG({sbp_col}),
        MAX({sbp_col})
    FROM {table_name}
    WHERE {sbp_col} IS NOT NULL
    
    UNION ALL
    
    SELECT
        'RR' AS vital,
        MIN({rr_col}),
        AVG({rr_col}),
        MAX({rr_col})
    FROM {table_name}
    WHERE {rr_col} IS NOT NULL
    """
    
    try:
        df = fetch_df(conn, vitals_sql)
        for _, row in df.iterrows():
            logger.info(
                f"  {row['vital']}: {row['min_val']:.1f} / "
                f"{row['mean_val']:.1f} / {row['max_val']:.1f}"
            )
    except Exception as e:
        logger.debug(f"Could not get vital sign summary: {e}")
