"""
Build outcome indicators from event log previously generated (build_event_log.py). 
"""

import logging
from typing import Dict, Any
import psycopg2

from .utils import read_sql, render_sql_template, get_sql_mapping
from .db import execute_with_progress, get_table_row_count

logger = logging.getLogger(__name__)


def build_outcomes(conn: psycopg2.extensions.connection, cfg: Dict[str, Any]) -> int:
    """
    Build the outcomes table from the event log.
    
    Creates binary outcome indicators for:
    - Multiple time horizons (24h, 48h, 72h, in-hospital)
    - Individual components (ICU, pressors, vent, RRT, etc.)
    - Composite deterioration
    - Time-to-event metrics
    
    Args:
        conn: Database connection
        cfg: Configuration dictionary
        
    Returns:
        Number of ED visits with outcome data
        
    Raises:
        Exception: If build fails
    """
    logger.info("=" * 60)
    logger.info("BUILDING OUTCOMES TABLE")
    logger.info("=" * 60)
    
    try:
        # Load and render SQL
        mapping = get_sql_mapping(cfg)
        sql = read_sql("sql/20_outcomes_from_event_log.sql")
        sql = render_sql_template(sql, mapping)
        
        # Execute
        execute_with_progress(
            conn, sql,
            "Creating outcomes table from event log"
        )
        
        # Get count
        table_name = cfg["tables"]["outcomes"]
        n_visits = get_table_row_count(conn, table_name)
        
        logger.info(f"Outcomes table created: {n_visits:,} ED visits")
        
        return n_visits
        
    except Exception as e:
        logger.error(f"Failed to build outcomes: {e}")
        raise


def validate_outcomes(conn: psycopg2.extensions.connection, cfg: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate outcomes table for prevalence and logical consistency.
    
    Args:
        conn: Database connection
        cfg: Configuration dictionary
        
    Returns:
        Dictionary with validation results
    """
    from .db import fetch_df
    
    outcomes_table = cfg["tables"]["outcomes"]
    
    logger.info("Validating outcomes...")
    
    validation_sql = f"""
    SELECT
        COUNT(*) AS total_visits,
        
        -- Hospitalization-level outcomes
        AVG(death_hosp) AS prev_death_hosp,
        AVG(cardiac_arrest_hosp) AS prev_cardiac_arrest_hosp,
        AVG(acs_hosp) AS prev_acs_hosp,
        AVG(revasc_hosp) AS prev_revasc_hosp,
        AVG(coronary_event_hosp) AS prev_coronary_event_hosp,

        -- Prediction-time-aligned outcomes (representative subset)
        AVG(icu_24h_from_w1) AS prev_icu_24h_from_w1,
        AVG(icu_24h_from_w6) AS prev_icu_24h_from_w6,
        AVG(icu_24h_from_w24) AS prev_icu_24h_from_w24,
        AVG(deterioration_24h_from_w1) AS prev_det24_from_w1,
        AVG(deterioration_24h_from_w6) AS prev_det24_from_w6,
        AVG(deterioration_24h_from_w24) AS prev_det24_from_w24,
        AVG(deterioration_48h_from_w24) AS prev_det48_from_w24,
        AVG(death_24h_from_w6) AS prev_death_24h_from_w6,
        
        -- Event-by flags (key ones)
        AVG(event_by_deterioration_w1) AS prev_event_by_det_w1,
        AVG(event_by_deterioration_w6) AS prev_event_by_det_w6,
        AVG(event_by_deterioration_w24) AS prev_event_by_det_w24,
        AVG(event_by_icu_w1) AS prev_event_by_icu_w1,
        AVG(event_by_icu_w6) AS prev_event_by_icu_w6,
        AVG(event_by_icu_w24) AS prev_event_by_icu_w24,
        
        -- Time-to-event summaries
        AVG(time_to_icu) AS mean_time_to_icu,
        AVG(time_to_death) AS mean_time_to_death,
        AVG(time_to_deterioration) AS mean_time_to_deterioration
        
    FROM {outcomes_table}
    """
    
    df = fetch_df(conn, validation_sql)
    results = df.to_dict('records')[0]
    
    logger.info("Prediction-Time-Aligned Prevalence:")
    logger.info(f"  ICU 24h from W1:  {results['prev_icu_24h_from_w1']*100:.2f}%")
    logger.info(f"  ICU 24h from W6:  {results['prev_icu_24h_from_w6']*100:.2f}%")
    logger.info(f"  ICU 24h from W24: {results['prev_icu_24h_from_w24']*100:.2f}%")
    logger.info(f"  Det 24h from W1:  {results['prev_det24_from_w1']*100:.2f}%")
    logger.info(f"  Det 24h from W6:  {results['prev_det24_from_w6']*100:.2f}%")
    logger.info(f"  Det 24h from W24: {results['prev_det24_from_w24']*100:.2f}%")
    logger.info(f"  Det 48h from W24: {results['prev_det48_from_w24']*100:.2f}%")
    logger.info(f"  Death 24h from W6: {results['prev_death_24h_from_w6']*100:.3f}%")
    logger.info("")
    logger.info("Hospitalization-Level Outcomes:")
    logger.info(f"  Death (in-hospital): {results['prev_death_hosp']*100:.2f}%")
    logger.info(f"  Cardiac arrest (hosp): {results['prev_cardiac_arrest_hosp']*100:.2f}%")
    logger.info(f"  ACS (hosp): {results['prev_acs_hosp']*100:.2f}%")
    logger.info(f"  Revascularization (hosp): {results['prev_revasc_hosp']*100:.2f}%")
    logger.info(f"  Coronary event (hosp): {results['prev_coronary_event_hosp']*100:.2f}%")
    logger.info("")
    logger.info("Event-By Flags (event occurred WITHIN feature window):")
    logger.info(f"  Deterioration by W1:  {results['prev_event_by_det_w1']*100:.2f}%")
    logger.info(f"  Deterioration by W6:  {results['prev_event_by_det_w6']*100:.2f}%")
    logger.info(f"  Deterioration by W24: {results['prev_event_by_det_w24']*100:.2f}%")
    logger.info(f"  ICU by W1:  {results['prev_event_by_icu_w1']*100:.2f}%")
    logger.info(f"  ICU by W6:  {results['prev_event_by_icu_w6']*100:.2f}%")
    logger.info(f"  ICU by W24: {results['prev_event_by_icu_w24']*100:.2f}%")
    
    # Warnings for unexpected prevalences
    det_w6 = results['prev_det24_from_w6']
    if det_w6 < 0.01:
        logger.warning("  Warning: Deterioration 24h from W6 rate seems very low (<1%)")
    if det_w6 > 0.5:
        logger.warning("  Warning: Deterioration 24h from W6 rate seems very high (>50%)")
    
    return results


def get_outcome_summary(conn: psycopg2.extensions.connection, cfg: Dict[str, Any]) -> None:
    """
    Print detailed outcome summary statistics.
    
    Args:
        conn: Database connection
        cfg: Configuration dictionary
    """
    from .db import fetch_df
    
    outcomes_table = cfg["tables"]["outcomes"]
    
    # Get outcome correlations
    logger.info("")
    logger.info("Outcome Co-occurrence Analysis (prediction-aligned, W6 24h):")
    
    cooccurrence_sql = f"""
    SELECT
        'ICU + Pressor (from W6)' AS combination,
        SUM(CASE WHEN icu_24h_from_w6 = 1 AND pressor_24h_from_w6 = 1 THEN 1 ELSE 0 END) AS n,
        AVG(CASE WHEN icu_24h_from_w6 = 1 AND pressor_24h_from_w6 = 1 THEN 1.0 ELSE 0.0 END) AS prevalence
    FROM {outcomes_table}
    
    UNION ALL
    
    SELECT
        'ICU + Vent (from W6)' AS combination,
        SUM(CASE WHEN icu_24h_from_w6 = 1 AND vent_24h_from_w6 = 1 THEN 1 ELSE 0 END),
        AVG(CASE WHEN icu_24h_from_w6 = 1 AND vent_24h_from_w6 = 1 THEN 1.0 ELSE 0.0 END)
    FROM {outcomes_table}
    
    UNION ALL
    
    SELECT
        'Pressor + Vent (from W6)' AS combination,
        SUM(CASE WHEN pressor_24h_from_w6 = 1 AND vent_24h_from_w6 = 1 THEN 1 ELSE 0 END),
        AVG(CASE WHEN pressor_24h_from_w6 = 1 AND vent_24h_from_w6 = 1 THEN 1.0 ELSE 0.0 END)
    FROM {outcomes_table}
    
    ORDER BY prevalence DESC
    """
    
    df = fetch_df(conn, cooccurrence_sql)
    for _, row in df.iterrows():
        logger.info(f"  {row['combination']}: {row['n']:,} ({row['prevalence']*100:.2f}%)")
