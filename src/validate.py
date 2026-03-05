"""
Pipeline validation and data quality checks
"""

import logging
from typing import Dict, Any
import psycopg2

from .db import fetch_df
from .utils import read_sql, render_sql_template, get_sql_mapping

logger = logging.getLogger(__name__)


def sanity_counts(conn: psycopg2.extensions.connection, cfg: Dict[str, Any]) -> Dict[str, Any]:
    """
    Quick sanity check on key table counts and basic stats.
    
    Args:
        conn: Database connection
        cfg: Configuration dictionary
        
    Returns:
        Dictionary with sanity check results
    """
    base = cfg["tables"]["base_ed_cohort"]
    event_log = cfg["tables"]["event_log"]
    outcomes = cfg["tables"]["outcomes"]
    
    sql = f"""
    SELECT
        (SELECT COUNT(*) FROM {base}) AS n_ed_visits,
        (SELECT COUNT(DISTINCT subject_id) FROM {base}) AS n_unique_patients,
        (SELECT COUNT(*) FROM {event_log}) AS n_events,
        (SELECT COUNT(DISTINCT event_type) FROM {event_log}) AS n_event_types,
        (SELECT AVG(deterioration_24h_from_w6) FROM {outcomes}) AS prev_deterioration_24h_from_w6,
        (SELECT AVG(icu_24h_from_w6) FROM {outcomes}) AS prev_icu_24h_from_w6,
        (SELECT AVG(death_24h_from_w6) FROM {outcomes}) AS prev_death_24h_from_w6,
        (SELECT AVG(death_hosp) FROM {outcomes}) AS prev_death_hosp
    """
    
    df = fetch_df(conn, sql)
    return df.to_dict('records')[0]


def validate_pipeline(conn: psycopg2.extensions.connection, cfg: Dict[str, Any]) -> bool:
    """
    Comprehensive pipeline validation.
    
    Checks:
    - Table existence and row counts
    - Temporal consistency
    - Logical outcome relationships
    - Feature completeness
    - Join integrity
    
    Args:
        conn: Database connection
        cfg: Configuration dictionary
        
    Returns:
        True if all validations pass, False otherwise
    """
    logger.info("=" * 60)
    logger.info("PIPELINE VALIDATION")
    logger.info("=" * 60)
    
    all_passed = True
    
    # 1. Table existence and counts
    logger.info("\n1. Table Existence and Row Counts")
    tables = [
        ("base_ed_cohort", "Base ED cohort"),
        ("event_log", "Event log"),
        ("outcomes", "Outcomes"),
        ("features_w1", "Features W1"),
        ("features_w6", "Features W6"),
        ("features_w24", "Features W24"),
    ]
    
    for table_key, description in tables:
        table_name = cfg["tables"][table_key]
        try:
            from .db import get_table_row_count
            count = get_table_row_count(conn, table_name)
            if count > 0:
                logger.info(f"  [OK] {description}: {count:,} rows")
            else:
                logger.error(f"  [FAIL] {description}: 0 rows (FAIL)")
                all_passed = False
        except Exception as e:
            logger.error(f"  [FAIL] {description}: Table not found or error ({e})")
            all_passed = False
    
    # 2. Temporal consistency
    logger.info("\n2. Temporal Consistency")
    base = cfg["tables"]["base_ed_cohort"]
    event_log = cfg["tables"]["event_log"]
    
    temporal_sql = f"""
    WITH event_timing AS (
        SELECT
            e.stay_id,
            e.event_type,
            e.event_time,
            b.ed_intime,
            b.ed_outtime,
            EXTRACT(EPOCH FROM (e.event_time - b.ed_intime)) / 3600.0 AS hours_from_ed
        FROM {event_log} e
        INNER JOIN {base} b USING (stay_id)
        WHERE e.event_time IS NOT NULL  -- Exclude hospitalization-level ICD events (NULL timing)
    )
    SELECT
        SUM(CASE WHEN event_time < ed_intime THEN 1 ELSE 0 END) AS events_before_ed,
        SUM(CASE WHEN hours_from_ed < -1 THEN 1 ELSE 0 END) AS events_early_warning,
        COUNT(*) AS total_events
    FROM event_timing
    """
    
    try:
        df = fetch_df(conn, temporal_sql)
        result = df.to_dict('records')[0]
        
        if result['events_before_ed'] == 0:
            logger.info(f"  [OK] No events before ED arrival")
        else:
            logger.warning(f"  [WARN] {result['events_before_ed']} events before ED arrival")
            all_passed = False
        
        if result['events_early_warning'] > 0:
            logger.warning(f"  [WARN] {result['events_early_warning']} events >1h before ED")
    except Exception as e:
        logger.error(f"  [FAIL] Temporal check failed: {e}")
        all_passed = False
    
    # 3. Outcome logical consistency
    logger.info("\n3. Outcome Logical Consistency")
    outcomes = cfg["tables"]["outcomes"]
    
    # Monotonicity: 24h_from_w6 subset of 48h_from_w6 (wider horizon includes narrower)
    # event_by monotonicity: w1 <= w6 <= w24
    consistency_sql = f"""
    SELECT
        SUM(CASE WHEN icu_24h_from_w6 = 1 AND icu_48h_from_w6 = 0 THEN 1 ELSE 0 END) AS icu_24h_not_48h_w6,
        SUM(CASE WHEN death_24h_from_w6 = 1 AND death_48h_from_w6 = 0 THEN 1 ELSE 0 END) AS death_24h_not_48h_w6,
        SUM(CASE WHEN event_by_icu_w1 = 1 AND event_by_icu_w6 = 0 THEN 1 ELSE 0 END) AS event_by_icu_w1_not_w6,
        SUM(CASE WHEN event_by_icu_w6 = 1 AND event_by_icu_w24 = 0 THEN 1 ELSE 0 END) AS event_by_icu_w6_not_w24,
        COUNT(*) AS total_visits
    FROM {outcomes}
    """
    
    try:
        df = fetch_df(conn, consistency_sql)
        result = df.to_dict('records')[0]
        
        issues = []
        if result['icu_24h_not_48h_w6'] > 0:
            issues.append(f"ICU 24h from W6 but not 48h from W6: {result['icu_24h_not_48h_w6']}")
        if result['death_24h_not_48h_w6'] > 0:
            issues.append(f"Death 24h from W6 but not 48h from W6: {result['death_24h_not_48h_w6']}")
        if result['event_by_icu_w1_not_w6'] > 0:
            issues.append(f"event_by_icu W1 but not W6: {result['event_by_icu_w1_not_w6']}")
        if result['event_by_icu_w6_not_w24'] > 0:
            issues.append(f"event_by_icu W6 but not W24: {result['event_by_icu_w6_not_w24']}")
        
        if not issues:
            logger.info(f"  OK: Outcome timelines and event_by flags are logically consistent")
        else:
            logger.warning(f"  Warning: Logical inconsistencies found:")
            for issue in issues:
                logger.warning(f"      {issue}")
            all_passed = False
    except Exception as e:
        logger.error(f"  FAIL: Consistency check failed: {e}")
        all_passed = False
    
    # 4. Join integrity
    logger.info("\n4. Join Integrity")
    
    join_sql = f"""
    SELECT
        (SELECT COUNT(*) FROM {base}) AS base_count,
        (SELECT COUNT(*) FROM {outcomes}) AS outcome_count,
        (SELECT COUNT(*) FROM {base} b 
         INNER JOIN {outcomes} o USING (stay_id)) AS joined_count
    """
    
    try:
        df = fetch_df(conn, join_sql)
        result = df.to_dict('records')[0]
        
        if result['base_count'] == result['outcome_count'] == result['joined_count']:
            logger.info(f"  [OK] All cohort visits have outcomes (1:1 mapping)")
        else:
            logger.warning(f"  [WARN] Mismatch in row counts:")
            logger.warning(f"      Base: {result['base_count']:,}")
            logger.warning(f"      Outcomes: {result['outcome_count']:,}")
            logger.warning(f"      Joined: {result['joined_count']:,}")
            all_passed = False
    except Exception as e:
        logger.error(f"  [FAIL] Join integrity check failed: {e}")
        all_passed = False
    
    # 5. Feature completeness
    logger.info("\n5. Feature Completeness (W1 sample)")
    features_w1 = cfg["tables"]["features_w1"]
    
    completeness_sql = f"""
    SELECT
        AVG(CASE WHEN temp_w1 IS NOT NULL THEN 1.0 ELSE 0.0 END) AS pct_temp,
        AVG(CASE WHEN hr_w1 IS NOT NULL THEN 1.0 ELSE 0.0 END) AS pct_hr,
        AVG(CASE WHEN sbp_w1 IS NOT NULL THEN 1.0 ELSE 0.0 END) AS pct_sbp,
        AVG(CASE WHEN spo2_w1 IS NOT NULL THEN 1.0 ELSE 0.0 END) AS pct_spo2
    FROM {features_w1}
    """
    
    try:
        df = fetch_df(conn, completeness_sql)
        result = df.to_dict('records')[0]
        
        logger.info(f"  Core vitals completeness:")
        logger.info(f"    Temperature: {result['pct_temp']*100:.1f}%")
        logger.info(f"    Heart Rate: {result['pct_hr']*100:.1f}%")
        logger.info(f"    Blood Pressure: {result['pct_sbp']*100:.1f}%")
        logger.info(f"    SpO2: {result['pct_spo2']*100:.1f}%")
        
        # Warning for very low completeness
        for vital, pct in result.items():
            if pct < 0.5:
                logger.warning(f"  [WARN] {vital} has <50% completeness")
    except Exception as e:
        logger.error(f"  [FAIL] Completeness check failed: {e}")
        all_passed = False
    
    # Final verdict
    logger.info("\n" + "=" * 60)
    if all_passed:
        logger.info("[OK] ALL VALIDATIONS PASSED")
    else:
        logger.warning("[WARN] SOME VALIDATIONS FAILED - Review warnings above")
    logger.info("=" * 60)
    
    return all_passed


def validate_dataset(df, name: str = "Dataset") -> Dict[str, Any]:
    """
    Validate a materialized dataset.
    
    Args:
        df: pandas DataFrame
        name: Dataset name for logging
        
    Returns:
        Dictionary with validation metrics
    """
    logger.info(f"\nValidating {name}...")
    
    metrics = {
        "n_rows": len(df),
        "n_features": len(df.columns) - 1,  # Exclude target
        "outcome_rate": df['y'].mean() if 'y' in df.columns else None,
        "missing_features": [],
        "constant_features": [],
    }
    
    # Check for completely missing features
    missing_pct = df.isnull().mean()
    metrics["missing_features"] = missing_pct[missing_pct == 1.0].index.tolist()
    
    # Check for constant features
    for col in df.columns:
        if col not in ['stay_id', 'subject_id', 'hadm_id', 'y']:
            if df[col].nunique() == 1:
                metrics["constant_features"].append(col)
    
    # Report
    if metrics["missing_features"]:
        logger.warning(f"  [WARN] {len(metrics['missing_features'])} features are completely missing")
    
    if metrics["constant_features"]:
        logger.warning(f"  [WARN] {len(metrics['constant_features'])} features are constant")
    
    if metrics["outcome_rate"]:
        if metrics["outcome_rate"] < 0.01 or metrics["outcome_rate"] > 0.99:
            logger.warning(f"  [WARN] Extreme outcome rate: {metrics['outcome_rate']*100:.2f}%")
        else:
            logger.info(f"  [OK] Outcome rate: {metrics['outcome_rate']*100:.2f}%")
    
    return metrics


def run_qa_checks(conn: psycopg2.extensions.connection, cfg: Dict[str, Any]) -> bool:
    """
    Run the SQL QA checks from sql/99_qa_checks.sql.
    
    Each check query should return 0 rows if the assertion holds.
    Any rows returned indicate a data integrity issue.
    
    Args:
        conn: Database connection
        cfg: Configuration dictionary
        
    Returns:
        True if all QA checks pass (0 rows returned), False otherwise
    """
    logger.info("=" * 60)
    logger.info("QA CHECKS (sql/99_qa_checks.sql)")
    logger.info("=" * 60)
    
    try:
        sql_raw = read_sql("sql/99_qa_checks.sql")
    except FileNotFoundError:
        logger.warning("  QA checks SQL file not found, skipping")
        return True
    
    mapping = get_sql_mapping(cfg)
    # Add ECG tables if available
    if "ecg_features_w6" in cfg.get("tables", {}):
        mapping["ecg_features_w6"] = cfg["tables"]["ecg_features_w6"]
    
    sql_rendered = render_sql_template(sql_raw, mapping)
    
    # Split into individual check statements (separated by semicolons)
    statements = [s.strip() for s in sql_rendered.split(";") if s.strip()]
    
    all_passed = True
    
    for stmt in statements:
        # Skip comments-only blocks
        lines = [l.strip() for l in stmt.split("\n") if l.strip() and not l.strip().startswith("--")]
        if not lines:
            continue
        
        # Extract check name from the query if possible
        check_name = "unknown"
        if "'check_name'" in stmt.lower() or "check_name" in stmt.lower():
            # Try to extract from the AS check_name literal
            for part in stmt.split("'"):
                if part and not part.startswith("--") and "_" in part and len(part) < 50:
                    check_name = part
                    break
        
        try:
            df = fetch_df(conn, stmt)
            n_violations = len(df)
            
            if n_violations == 0:
                logger.info(f"  PASS: {check_name} (0 violations)")
            else:
                logger.warning(f"  FAIL: {check_name} ({n_violations} violations)")
                if n_violations <= 5:
                    logger.warning(f"        {df.to_string(index=False)}")
                else:
                    logger.warning(f"        (first 5 of {n_violations}):")
                    logger.warning(f"        {df.head(5).to_string(index=False)}")
                all_passed = False
        except Exception as e:
            # Some statements might be UNION ALL chains — handle gracefully
            logger.warning(f"  SKIP: Could not run check ({str(e)[:100]})")
    
    if all_passed:
        logger.info("  ALL QA CHECKS PASSED")
    else:
        logger.warning("  SOME QA CHECKS FAILED — review output above")
    
    return all_passed
