"""
Materialize analysis datasets by combining features and outcomes
"""

import logging
import os
import re
from typing import Dict, Any, Optional, List, Union
import pandas as pd
import psycopg2

from .db import fetch_df
from .utils import ensure_output_dir

logger = logging.getLogger(__name__)


FEATURE_TABLE_BY_WINDOW = {
    "W1": "features_w1",
    "W6": "features_w6",
    "W24": "features_w24",
    "W6T": "features_w6_truncated",
    "W24T": "features_w24_truncated",
}

# ECG feature tables by window
ECG_TABLE_BY_WINDOW = {
    "W1": "ecg_features_w1",
    "W6": "ecg_features_w6",
}

# Cohort filter presets
COHORT_FILTERS = {
    "all": "",  # All ED visits
    "admitted": "b.hadm_id IS NOT NULL",  # Only admitted patients
    "not_admitted": "b.hadm_id IS NULL",  # Only non-admitted patients
}

# ── Event-by flag auto-detection ─────────────────────────────────
# When a prediction-time-aligned outcome is chosen, automatically
# include the corresponding event_by flag so the dataset contains
# the "already happened by prediction time" indicator.
#
# Pattern: {event}_{horizon}_from_{window}  →  event_by_{event}_{window}
#
# Examples:
#   icu_24h_from_w6          →  event_by_icu_w6
#   deterioration_48h_from_w1 →  event_by_deterioration_w1
#   death_72h_from_w24       →  event_by_death_w24

_ALIGNED_OUTCOME_RE = re.compile(
    r'^(?P<event>icu|pressor|vent|rrt|death|deterioration)'
    r'_\d+h'
    r'_from_(?P<window>w\d+)$'
)


def infer_event_by_columns(outcome_cols: List[str]) -> List[str]:
    """
    Given a list of outcome column names, return the matching
    event_by flag columns.  Only prediction-time-aligned outcomes
    (pattern ``{event}_{horizon}_from_{window}``) generate flags.
    ICD / hospitalization-level outcomes are silently skipped.

    Returns a de-duplicated, sorted list.
    """
    flags: set = set()
    for oc in outcome_cols:
        m = _ALIGNED_OUTCOME_RE.match(oc)
        if m:
            flags.add(f"event_by_{m.group('event')}_{m.group('window')}")
    return sorted(flags)


def add_missing_indicators(
    df: pd.DataFrame,
    threshold: float = 0.30,
    exclude_cols: List[str] = None
) -> pd.DataFrame:
    """
    Automatically add missing indicator columns for features with high missingness.
    
    Args:
        df: Input DataFrame
        threshold: Minimum fraction of missing values to create indicator (default 0.30 = 30%)
        exclude_cols: Columns to exclude from indicator creation
        
    Returns:
        DataFrame with added missing indicator columns
    """
    if exclude_cols is None:
        exclude_cols = ['stay_id', 'subject_id', 'hadm_id', 'ed_intime', 'ed_outtime', 'y', 'gender']
    
    # Also exclude columns that are already missing indicators
    exclude_cols = set(exclude_cols) | {c for c in df.columns if 'missing' in c.lower()}
    
    new_cols = {}
    for col in df.columns:
        if col in exclude_cols:
            continue
        
        missing_frac = df[col].isna().mean()
        
        if missing_frac >= threshold:
            indicator_name = f"missing_{col}"
            # Avoid duplicates
            if indicator_name not in df.columns:
                new_cols[indicator_name] = df[col].isna().astype(int)
                logger.debug(f"Created missing indicator: {indicator_name} ({missing_frac*100:.1f}% missing)")
    
    if new_cols:
        logger.info(f"Added {len(new_cols)} missing indicator columns (threshold: {threshold*100:.0f}%)")
        df = pd.concat([df, pd.DataFrame(new_cols)], axis=1)
    
    return df


def materialize_dataset(
    conn: psycopg2.extensions.connection,
    cfg: Dict[str, Any],
    window: str,
    outcome_col: str,
    out_csv: str,
    cohort_filter_sql: str = "",
    cohort_type: str = "all",
    include_base_features: bool = True,
    include_ecg: bool = False,
    add_missing_ind: bool = True,
    missing_threshold: float = 0.30
) -> pd.DataFrame:
    """
    Materialize an analysis dataset by joining features and outcomes.
    
    Args:
        conn: Database connection
        cfg: Configuration dictionary
        window: Feature window (W1, W6, or W24)
        outcome_col: Name of outcome column to use as target
        out_csv: Path to output CSV file
        cohort_filter_sql: Optional WHERE clause for cohort filtering
        cohort_type: Cohort filter preset ("all", "admitted", "not_admitted")
        include_base_features: Whether to include base cohort features
        include_ecg: Whether to include ECG features (for W1/W6 only)
        add_missing_ind: Whether to automatically add missing indicators
        missing_threshold: Threshold for creating missing indicators (default 0.30)
        
    Returns:
        pandas DataFrame with the materialized dataset
        
    Raises:
        ValueError: If window or outcome_col is invalid
    """
    logger.info(f"Materializing dataset: {window} -> {outcome_col} (cohort: {cohort_type}, ecg: {include_ecg})")
    
    # Validate inputs
    if window not in FEATURE_TABLE_BY_WINDOW:
        raise ValueError(f"Invalid window: {window}. Must be one of {list(FEATURE_TABLE_BY_WINDOW.keys())}")
    
    # Get table names
    base = cfg["tables"]["base_ed_cohort"]
    outcomes = cfg["tables"]["outcomes"]
    features = cfg["tables"][FEATURE_TABLE_BY_WINDOW[window]]
    
    # Build cohort filter
    filters = []
    
    # Add preset cohort filter
    if cohort_type in COHORT_FILTERS and COHORT_FILTERS[cohort_type]:
        filters.append(COHORT_FILTERS[cohort_type])
    
    # Add custom filter
    if cohort_filter_sql:
        clean_filter = cohort_filter_sql.strip()
        if clean_filter.upper().startswith("WHERE"):
            clean_filter = clean_filter[5:].strip()
        filters.append(clean_filter)
    
    where_clause = ""
    if filters:
        where_clause = "WHERE " + " AND ".join(f"({f})" for f in filters)
    
    # Build ECG join if requested and available for this window
    ecg_join = ""
    ecg_select = ""
    if include_ecg and window in ECG_TABLE_BY_WINDOW:
        ecg_table_key = ECG_TABLE_BY_WINDOW[window]
        if ecg_table_key in cfg["tables"]:
            ecg_table = cfg["tables"][ecg_table_key]
            ecg_join = f"LEFT JOIN {ecg_table} ecg USING (stay_id)"
            ecg_select = ", ecg.*"
            logger.info(f"  Including ECG features from {ecg_table}")
        else:
            logger.warning(f"ECG table not configured for {window}, skipping ECG features")
    elif include_ecg:
        logger.warning(f"ECG features not available for window {window}")
    
    # Auto-detect event_by flag columns for the chosen outcome
    event_by_cols = infer_event_by_columns([outcome_col])
    event_by_select = ""
    if event_by_cols:
        event_by_select = ", " + ", ".join(f"o.{c}" for c in event_by_cols)
        logger.info(f"  Auto-including event_by flags: {event_by_cols}")
    
    if include_base_features:
        sql = f"""
        SELECT
          b.stay_id,
          b.subject_id,
          b.hadm_id,
          b.ed_intime,
          b.ed_outtime,
          b.age_at_ed,
          b.gender,
          b.ed_los_hours,
          f.*{ecg_select},
          o.{outcome_col} AS y{event_by_select}
        FROM {base} b
        INNER JOIN {features} f USING (stay_id)
        INNER JOIN {outcomes} o USING (stay_id)
        {ecg_join}
        {where_clause}
        ORDER BY b.stay_id
        """
    else:
        sql = f"""
        SELECT
          b.stay_id,
          f.*{ecg_select},
          o.{outcome_col} AS y{event_by_select}
        FROM {base} b
        INNER JOIN {features} f USING (stay_id)
        INNER JOIN {outcomes} o USING (stay_id)
        {ecg_join}
        {where_clause}
        ORDER BY b.stay_id
        """
    
    # Execute query
    logger.info("Executing materialization query...")
    df = fetch_df(conn, sql)
    
    # Remove any duplicate columns (keeps first occurrence)
    df = df.loc[:, ~df.columns.duplicated()]
    
    # Add automatic missing indicators
    if add_missing_ind:
        df = add_missing_indicators(df, threshold=missing_threshold)
    
    # Save to CSV
    ensure_output_dir(os.path.dirname(out_csv))
    df.to_csv(out_csv, index=False)
    
    # Summary statistics
    n_rows = len(df)
    n_features = len(df.columns) - 1  # Exclude target 'y'
    outcome_rate = df['y'].mean() if 'y' in df.columns else 0
    
    logger.info(f"Dataset materialized:")
    logger.info(f"  Output: {out_csv}")
    logger.info(f"  Rows: {n_rows:,}")
    logger.info(f"  Features: {n_features}")
    logger.info(f"  Outcome rate: {outcome_rate*100:.2f}%")
    
    # Check for issues
    if n_rows == 0:
        logger.warning("[WARN] Dataset is empty! Check your filters.")
    
    if outcome_rate < 0.01:
        logger.warning(f"[WARN] Very low outcome rate ({outcome_rate*100:.2f}%). May cause training issues.")
    
    if outcome_rate > 0.5:
        logger.warning(f"[WARN] High outcome rate ({outcome_rate*100:.2f}%). Verify outcome definition.")
    
    return df


def materialize_dataset_advanced(
    conn: psycopg2.extensions.connection,
    cfg: Dict[str, Any],
    windows: Union[str, List[str]],
    outcome_cols: Union[str, List[str]],
    out_csv: str,
    cohort_filter_sql: str = "",
    cohort_type: str = "all",
    include_base_features: bool = True,
    include_ecg: bool = False,
    add_missing_ind: bool = True,
    missing_threshold: float = 0.30
) -> pd.DataFrame:
    """
    Advanced dataset materialization supporting multiple outcomes and/or multiple windows.
    
    When multiple windows are specified, feature columns are suffixed with window name
    (e.g., sbp_mean_w6, sbp_mean_w24) to avoid naming conflicts.
    
    When multiple outcomes are specified, each outcome becomes a separate column
    (e.g., y_death_24h, y_death_48h, y_death_inhospital).
    
    Args:
        conn: Database connection
        cfg: Configuration dictionary
        windows: Single window (str) or list of windows (e.g., ["W6", "W24"])
        outcome_cols: Single outcome (str) or list of outcomes (e.g., ["death_24h", "death_48h"])
        out_csv: Path to output CSV file
        cohort_filter_sql: Optional WHERE clause for cohort filtering
        cohort_type: Cohort filter preset ("all", "admitted", "not_admitted")
        include_base_features: Whether to include base cohort features
        include_ecg: Whether to include ECG features (for W1/W6 only)
        add_missing_ind: Whether to automatically add missing indicators
        missing_threshold: Threshold for creating missing indicators (default 0.30)
        
    Returns:
        pandas DataFrame with the materialized dataset
        
    Example:
        # Multi-outcome dataset
        df = materialize_dataset_advanced(
            conn, cfg,
            windows="W6",
            outcome_cols=["death_24h", "death_48h", "death_inhospital"],
            out_csv="mortality_outcomes.csv"
        )
        
        # Multi-window dataset
        df = materialize_dataset_advanced(
            conn, cfg,
            windows=["W6", "W24"],
            outcome_cols="deterioration_24h",
            out_csv="multi_window.csv"
        )
    """
    # Normalize inputs to lists
    if isinstance(windows, str):
        windows = [windows]
    if isinstance(outcome_cols, str):
        outcome_cols = [outcome_cols]
    
    # Validate inputs
    for w in windows:
        if w not in FEATURE_TABLE_BY_WINDOW:
            raise ValueError(f"Invalid window: {w}. Must be one of {list(FEATURE_TABLE_BY_WINDOW.keys())}")
    
    multi_window = len(windows) > 1
    multi_outcome = len(outcome_cols) > 1
    
    logger.info(f"Materializing advanced dataset:")
    logger.info(f"  Windows: {windows} {'(multi-window mode)' if multi_window else ''}")
    logger.info(f"  Outcomes: {outcome_cols} {'(multi-outcome mode)' if multi_outcome else ''}")
    logger.info(f"  Cohort: {cohort_type}")
    
    # Get table names
    base = cfg["tables"]["base_ed_cohort"]
    outcomes_table = cfg["tables"]["outcomes"]
    
    # Build cohort filter
    filters = []
    if cohort_type in COHORT_FILTERS and COHORT_FILTERS[cohort_type]:
        filters.append(COHORT_FILTERS[cohort_type])
    if cohort_filter_sql:
        clean_filter = cohort_filter_sql.strip()
        if clean_filter.upper().startswith("WHERE"):
            clean_filter = clean_filter[5:].strip()
        filters.append(clean_filter)
    
    where_clause = ""
    if filters:
        where_clause = "WHERE " + " AND ".join(f"({f})" for f in filters)
    
    # Build outcome SELECT clause
    outcome_select_parts = []
    for oc in outcome_cols:
        if multi_outcome:
            outcome_select_parts.append(f"o.{oc} AS y_{oc}")
        else:
            outcome_select_parts.append(f"o.{oc} AS y")
    outcome_select = ", ".join(outcome_select_parts)
    
    # Auto-detect event_by flag columns for the chosen outcomes
    event_by_cols = infer_event_by_columns(outcome_cols)
    event_by_select = ""
    if event_by_cols:
        event_by_select = ", " + ", ".join(f"o.{c}" for c in event_by_cols)
        logger.info(f"  Auto-including event_by flags: {event_by_cols}")
    
    # Build feature JOINs and SELECTs for each window
    feature_joins = []
    feature_selects = []
    
    for i, w in enumerate(windows):
        features_table = cfg["tables"][FEATURE_TABLE_BY_WINDOW[w]]
        alias = f"f{i+1}" if multi_window else "f"
        
        feature_joins.append(f"INNER JOIN {features_table} {alias} USING (stay_id)")
        
        if multi_window:
            # Will need to rename columns with window suffix
            feature_selects.append(f"{alias}.*")
        else:
            feature_selects.append(f"{alias}.*")
    
    # Build ECG joins if requested
    ecg_joins = []
    ecg_selects = []
    if include_ecg:
        for i, w in enumerate(windows):
            if w in ECG_TABLE_BY_WINDOW:
                ecg_table_key = ECG_TABLE_BY_WINDOW[w]
                if ecg_table_key in cfg["tables"]:
                    ecg_table = cfg["tables"][ecg_table_key]
                    ecg_alias = f"ecg{i+1}" if multi_window else "ecg"
                    ecg_joins.append(f"LEFT JOIN {ecg_table} {ecg_alias} USING (stay_id)")
                    ecg_selects.append(f"{ecg_alias}.*")
                    logger.info(f"  Including ECG features from {ecg_table}")
    
    # Build final SQL
    if include_base_features:
        base_select = """
          b.stay_id,
          b.subject_id,
          b.hadm_id,
          b.ed_intime,
          b.ed_outtime,
          b.age_at_ed,
          b.gender,
          b.ed_los_hours"""
    else:
        base_select = "b.stay_id"
    
    all_joins = " ".join(feature_joins + ecg_joins)
    all_feature_selects = ", ".join(feature_selects + ecg_selects)
    
    sql = f"""
    SELECT
      {base_select},
      {all_feature_selects},
      {outcome_select}{event_by_select}
    FROM {base} b
    {all_joins}
    INNER JOIN {outcomes_table} o USING (stay_id)
    {where_clause}
    ORDER BY b.stay_id
    """
    
    # Execute query
    logger.info("Executing materialization query...")
    df = fetch_df(conn, sql)
    
    # Handle multi-window column renaming
    if multi_window:
        df = _rename_columns_for_multi_window(df, windows, cfg, include_base_features, outcome_cols, multi_outcome)
    
    # Remove duplicate stay_id column if present
    df = _remove_duplicate_columns(df)
    
    # Add automatic missing indicators
    if add_missing_ind:
        # Exclude outcome columns from missing indicators
        exclude_cols = ['stay_id', 'subject_id', 'hadm_id', 'ed_intime', 'ed_outtime', 'gender']
        if multi_outcome:
            exclude_cols.extend([f"y_{oc}" for oc in outcome_cols])
        else:
            exclude_cols.append('y')
        df = add_missing_indicators(df, threshold=missing_threshold, exclude_cols=exclude_cols)
    
    # Save to CSV
    ensure_output_dir(os.path.dirname(out_csv))
    df.to_csv(out_csv, index=False)
    
    # Summary statistics
    n_rows = len(df)
    n_features = len([c for c in df.columns if not c.startswith('y')])
    
    logger.info(f"Dataset materialized:")
    logger.info(f"  Output: {out_csv}")
    logger.info(f"  Rows: {n_rows:,}")
    logger.info(f"  Features: {n_features}")
    
    if multi_outcome:
        for oc in outcome_cols:
            col_name = f"y_{oc}"
            if col_name in df.columns:
                rate = df[col_name].mean()
                logger.info(f"  {oc} rate: {rate*100:.2f}%")
    else:
        if 'y' in df.columns:
            rate = df['y'].mean()
            logger.info(f"  Outcome rate: {rate*100:.2f}%")
    
    if n_rows == 0:
        logger.warning("[WARN] Dataset is empty! Check your filters.")
    
    return df


def _rename_columns_for_multi_window(
    df: pd.DataFrame,
    windows: List[str],
    cfg: Dict[str, Any],
    include_base_features: bool,
    outcome_cols: List[str],
    multi_outcome: bool
) -> pd.DataFrame:
    """
    Rename feature columns with window suffix when multiple windows are used.
    """
    # Columns that should NOT be renamed
    base_cols = {'stay_id', 'subject_id', 'hadm_id', 'ed_intime', 'ed_outtime', 
                 'age_at_ed', 'gender', 'ed_los_hours'}
    outcome_col_set = {f"y_{oc}" for oc in outcome_cols} if multi_outcome else {'y'}
    preserve_cols = base_cols | outcome_col_set
    
    # Get feature columns from each window to determine which belongs to which
    window_feature_cols = {}
    for w in windows:
        features_table = cfg["tables"][FEATURE_TABLE_BY_WINDOW[w]]
        # We need to identify columns that came from this window
        window_feature_cols[w] = set()
    
    # For now, use a simpler approach: detect duplicates and suffix them
    # First pass: count column occurrences
    col_counts = {}
    for col in df.columns:
        if col not in preserve_cols:
            col_counts[col] = col_counts.get(col, 0) + 1
    
    # Rename columns with window suffixes
    new_columns = []
    window_idx = {w: 0 for w in windows}  # Track which window we're on for each duplicate
    seen_cols = {}
    
    for col in df.columns:
        if col in preserve_cols:
            new_columns.append(col)
        elif col in seen_cols:
            # This is a duplicate - add window suffix
            occurrence = seen_cols[col]
            if occurrence < len(windows):
                w = windows[occurrence]
                new_columns.append(f"{col}_{w.lower()}")
            else:
                new_columns.append(f"{col}_{occurrence}")
            seen_cols[col] += 1
        else:
            # First occurrence
            if col_counts.get(col, 1) > 1:
                # Will have duplicates - add window suffix for first occurrence too
                w = windows[0]
                new_columns.append(f"{col}_{w.lower()}")
                seen_cols[col] = 1
            else:
                # Unique column - add window suffix anyway for clarity
                # Determine which window it came from based on position
                new_columns.append(col)
                seen_cols[col] = 1
    
    df.columns = new_columns
    return df


def _remove_duplicate_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Remove duplicate columns, keeping only first occurrence."""
    seen = set()
    keep_cols = []
    for col in df.columns:
        if col not in seen:
            keep_cols.append(col)
            seen.add(col)
    return df[keep_cols]


def materialize_multiple_datasets(
    conn: psycopg2.extensions.connection,
    cfg: Dict[str, Any],
    dataset_configs: list,
    output_dir: str = "artifacts/datasets"
) -> Dict[str, pd.DataFrame]:
    """
    Materialize multiple datasets from a list of configurations.
    
    Args:
        conn: Database connection
        cfg: Configuration dictionary
        dataset_configs: List of dicts with keys: window, outcome_col, name, filter (optional)
        output_dir: Base directory for output CSV files
        
    Returns:
        Dictionary mapping dataset names to DataFrames
        
    Example:
        dataset_configs = [
            {
                "name": "w6_det24",
                "window": "W6",
                "outcome_col": "deterioration_24h",
                "filter": "age_at_ed >= 18 AND age_at_ed < 90"
            },
            {
                "name": "w1_icu24",
                "window": "W1",
                "outcome_col": "icu_24h"
            }
        ]
    """
    logger.info(f"Materializing {len(dataset_configs)} datasets...")
    
    datasets = {}
    
    for i, config in enumerate(dataset_configs, 1):
        name = config.get("name", f"dataset_{i}")
        window = config["window"]
        outcome_col = config["outcome_col"]
        cohort_filter = config.get("filter", "")
        
        out_csv = f"{output_dir}/{name}.csv"
        
        logger.info(f"\nDataset {i}/{len(dataset_configs)}: {name}")
        
        try:
            df = materialize_dataset(
                conn, cfg,
                window=window,
                outcome_col=outcome_col,
                out_csv=out_csv,
                cohort_filter_sql=cohort_filter
            )
            datasets[name] = df
            
        except Exception as e:
            logger.error(f"Failed to materialize {name}: {e}")
    
    logger.info(f"\n[OK] Materialized {len(datasets)} datasets successfully")
    
    return datasets


def get_dataset_summary(df: pd.DataFrame, name: str = "Dataset") -> None:
    """
    Print detailed dataset summary statistics.
    
    Args:
        df: Dataset DataFrame
        name: Dataset name for display
    """
    logger.info(f"\n{name} Summary:")
    logger.info(f"  Shape: {df.shape[0]:,} rows × {df.shape[1]} columns")
    
    if 'y' in df.columns:
        outcome_rate = df['y'].mean()
        n_positive = df['y'].sum()
        n_negative = len(df) - n_positive
        logger.info(f"  Outcome distribution:")
        logger.info(f"    Positive (y=1): {n_positive:,} ({outcome_rate*100:.2f}%)")
        logger.info(f"    Negative (y=0): {n_negative:,} ({(1-outcome_rate)*100:.2f}%)")
    
    # Missing value summary
    missing_pct = (df.isnull().sum() / len(df) * 100).sort_values(ascending=False)
    high_missing = missing_pct[missing_pct > 50]
    
    if len(high_missing) > 0:
        logger.info(f"  Features with >50% missing: {len(high_missing)}")
        for col in high_missing.head(5).index:
            logger.info(f"    {col}: {missing_pct[col]:.1f}%")
    else:
        logger.info(f"  [OK] No features with >50% missing")
    
    # Data types
    dtypes_summary = df.dtypes.value_counts()
    logger.info(f"  Data types:")
    for dtype, count in dtypes_summary.items():
        logger.info(f"    {dtype}: {count} columns")
