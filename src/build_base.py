"""
Build base ED cohort table
"""

import logging
from typing import Dict, Any
import psycopg2

from .utils import read_sql, render_sql_template, get_sql_mapping
from .db import execute_with_progress, get_table_row_count

logger = logging.getLogger(__name__)


def build_base(conn: psycopg2.extensions.connection, cfg: Dict[str, Any]) -> int:
    """
    Build the base ED cohort table.
    
    Creates one row per ED stay with:
    - Patient identifiers (subject_id, stay_id, hadm_id)
    - ED timestamps (intime, outtime)
    - Demographics (age, gender)
    - Derived metrics (ED LOS)
    
    Args:
        conn: Database connection
        cfg: Configuration dictionary
        
    Returns:
        Number of ED visits in the cohort
        
    Raises:
        Exception: If build fails
    """
    logger.info("=" * 60)
    logger.info("BUILDING BASE ED COHORT")
    logger.info("=" * 60)
    
    try:
        # Load and render SQL
        mapping = get_sql_mapping(cfg)
        sql = read_sql("sql/00_base_ed_cohort.sql")
        sql = render_sql_template(sql, mapping)
        
        # Execute
        execute_with_progress(
            conn, sql,
            "Creating base ED cohort table"
        )
        
        # Get count
        table_name = cfg["tables"]["base_ed_cohort"]
        n_visits = get_table_row_count(conn, table_name)
        
        logger.info(f"Base cohort created: {n_visits:,} ED visits")
        logger.info(f"Table: {table_name}")
        
        # Basic validation
        if n_visits == 0:
            logger.warning("[WARN] Base cohort is empty! Check your filters.")
        elif n_visits < 1000:
            logger.warning(f"[WARN] Base cohort seems small ({n_visits} visits). Verify filters.")
        
        return n_visits
        
    except Exception as e:
        logger.error(f"Failed to build base cohort: {e}")
        raise


def validate_base_cohort(conn: psycopg2.extensions.connection, cfg: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate the base ED cohort for data quality issues.
    
    Args:
        conn: Database connection
        cfg: Configuration dictionary
        
    Returns:
        Dictionary with validation results
    """
    from .db import fetch_df
    
    table_name = cfg["tables"]["base_ed_cohort"]
    
    logger.info("Validating base cohort...")
    
    validation_sql = f"""
    SELECT
        COUNT(*) AS total_visits,
        COUNT(DISTINCT subject_id) AS unique_patients,
        COUNT(DISTINCT hadm_id) AS unique_admissions,
        AVG(age_at_ed) AS mean_age,
        MIN(age_at_ed) AS min_age,
        MAX(age_at_ed) AS max_age,
        SUM(CASE WHEN gender = 'M' THEN 1 ELSE 0 END)::float / COUNT(*) AS pct_male,
        AVG(ed_los_hours) AS mean_ed_los_hours,
        MIN(ed_los_hours) AS min_ed_los_hours,
        MAX(ed_los_hours) AS max_ed_los_hours,
        SUM(CASE WHEN ed_los_hours < 1 THEN 1 ELSE 0 END) AS n_short_stays,
        SUM(CASE WHEN ed_los_hours > 48 THEN 1 ELSE 0 END) AS n_long_stays
    FROM {table_name}
    """
    
    df = fetch_df(conn, validation_sql)
    results = df.to_dict('records')[0]
    
    logger.info("Base Cohort Validation Results:")
    logger.info(f"  Total ED visits: {results['total_visits']:,}")
    logger.info(f"  Unique patients: {results['unique_patients']:,}")
    logger.info(f"  Unique admissions: {results['unique_admissions']:,}")
    logger.info(f"  Age: {results['mean_age']:.1f} years (range: {results['min_age']:.0f}-{results['max_age']:.0f})")
    logger.info(f"  % Male: {results['pct_male']*100:.1f}%")
    logger.info(f"  ED LOS: {results['mean_ed_los_hours']:.1f} hours (range: {results['min_ed_los_hours']:.1f}-{results['max_ed_los_hours']:.1f})")
    logger.info(f"  Short stays (<1h): {results['n_short_stays']:,}")
    logger.info(f"  Long stays (>48h): {results['n_long_stays']:,}")
    
    return results
