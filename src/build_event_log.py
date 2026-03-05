"""
Build an event log from individual event extractors
"""

import logging
from typing import Dict, Any, List
import psycopg2

from .utils import read_sql, render_sql_template, get_sql_mapping
from .db import run_sql, execute_with_progress, get_table_row_count

logger = logging.getLogger(__name__)


# Event SQL files in execution order
EVENT_SQL_FILES = [
    ("sql/10_event_icu_admit.sql", "ICU admission"),
    ("sql/11_event_pressors.sql", "Vasopressor start"),
    ("sql/12_event_ventilation.sql", "Ventilation start"),
    ("sql/13_event_rrt.sql", "RRT start"),
    ("sql/14_event_acs.sql", "ACS diagnosis"),
    ("sql/15_event_revasc.sql", "Revascularization"),
    ("sql/16_event_cardiac_arrest.sql", "Cardiac arrest"),
    ("sql/17_event_death.sql", "Death"),
]


def build_event_log(conn: psycopg2.extensions.connection, cfg: Dict[str, Any]) -> int:
    """
    Build the event log table by extracting and consolidating all event types.
    
    Creates a long-format event log with:
    - One row per (stay_id, event_type, event_time)
    - Temporal accuracy varies by event type
    - Source documentation for provenance
    
    Args:
        conn: Database connection
        cfg: Configuration dictionary
        
    Returns:
        Number of events in the log
        
    Raises:
        Exception: If build fails
    """
    logger.info("=" * 60)
    logger.info("BUILDING EVENT LOG")
    logger.info("=" * 60)
    
    try:
        base = cfg["tables"]["base_ed_cohort"]
        event_log = cfg["tables"]["event_log"]
        
        # Create empty event log table
        logger.info("Creating event log table structure...")
        run_sql(conn, f"DROP TABLE IF EXISTS {event_log};")
        run_sql(conn, f"""
            CREATE TABLE {event_log} (
                subject_id BIGINT,
                stay_id BIGINT,
                hadm_id BIGINT,
                event_type TEXT,
                event_time TIMESTAMP,
                event_time_type TEXT DEFAULT 'exact',
                source_table TEXT,
                event_detail TEXT
            );
        """)
        
        # Prepare SQL mapping
        mapping = get_sql_mapping(cfg)
        
        # Insert events from each extractor
        event_counts = {}
        for sql_file, description in EVENT_SQL_FILES:
            try:
                logger.info(f"Extracting: {description}...")
                sql = read_sql(sql_file)
                sql = render_sql_template(sql, mapping)
                
                # Strip trailing semicolons and whitespace to avoid double semicolons
                sql = sql.strip().rstrip(';').strip()
                
                # Insert events
                run_sql(conn, f"INSERT INTO {event_log} {sql}")
                
                # Count events of this type - get the event type from the SQL file name
                # Map SQL file to expected event type(s)
                event_type_map = {
                    "10_event_icu_admit.sql": "ICU_ADMIT",
                    "11_event_pressors.sql": "PRESSOR_START",
                    "12_event_ventilation.sql": "VENT_START",
                    "13_event_rrt.sql": "RRT_START",
                    "14_event_acs.sql": "ACS",
                    "15_event_revasc.sql": "REVASC",
                    "16_event_cardiac_arrest.sql": "CARDIAC_ARREST",
                    "17_event_death.sql": "DEATH",
                }
                # For extractors that produce multiple event_type values
                multi_type_map = {
                    "REVASC": ["PCI", "CABG"],
                }
                sql_filename = sql_file.split("/")[-1]
                expected_event_type = event_type_map.get(sql_filename, "UNKNOWN")
                
                from .db import fetch_df
                
                # Validate and count events
                allowed_types = multi_type_map.get(expected_event_type)
                if allowed_types:
                    # Multi-type extractor (e.g., REVASC produces PCI + CABG)
                    types_in = ", ".join(f"'{t}'" for t in allowed_types)
                    count_df = fetch_df(conn, f"""
                        SELECT COUNT(*) as n
                        FROM {event_log}
                        WHERE event_type IN ({types_in})
                    """)
                    if not count_df.empty and count_df.iloc[0]['n'] > 0:
                        count = count_df.iloc[0]['n']
                        event_counts[expected_event_type] = count
                        logger.info(f"  [OK] {description}: {count:,} events ({' + '.join(allowed_types)})")
                    else:
                        logger.warning(f"  [WARN] {description}: 0 events found")
                else:
                    count_df = fetch_df(conn, f"""
                        SELECT event_type, COUNT(*) as n
                        FROM {event_log}
                        WHERE event_type = '{expected_event_type}'
                        GROUP BY event_type
                    """)
                    
                    if not count_df.empty:
                        event_type = count_df.iloc[0]['event_type']
                        count = count_df.iloc[0]['n']
                        event_counts[event_type] = count
                        logger.info(f"  [OK] {description}: {count:,} events")
                    else:
                        logger.warning(f"  [WARN] {description}: 0 events found")
                    
            except Exception as e:
                logger.error(f"  [FAIL] Failed to extract {description}: {e}")
                # Continue with other events even if one fails
        
        # Create indexes
        logger.info("Creating indexes on event log...")
        run_sql(conn, f"CREATE INDEX IF NOT EXISTS idx_{event_log}_stay_id ON {event_log}(stay_id);")
        run_sql(conn, f"CREATE INDEX IF NOT EXISTS idx_{event_log}_event_type ON {event_log}(event_type);")
        run_sql(conn, f"CREATE INDEX IF NOT EXISTS idx_{event_log}_event_time ON {event_log}(event_time);")
        
        # Get total count
        total_events = get_table_row_count(conn, event_log)
        
        logger.info("")
        logger.info("Event Log Summary:")
        logger.info(f"  Total events: {total_events:,}")
        for event_type, count in sorted(event_counts.items()):
            pct = (count / total_events * 100) if total_events > 0 else 0
            logger.info(f"    {event_type}: {count:,} ({pct:.1f}%)")
        
        if total_events == 0:
            logger.warning("[WARN] Event log is empty! Check your event extractors.")
        
        return total_events
        
    except Exception as e:
        logger.error(f"Failed to build event log: {e}")
        raise


def validate_event_log(conn: psycopg2.extensions.connection, cfg: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate the event log for temporal consistency and coverage.
    
    Args:
        conn: Database connection
        cfg: Configuration dictionary
        
    Returns:
        Dictionary with validation results
    """
    from .db import fetch_df
    
    event_log = cfg["tables"]["event_log"]
    base = cfg["tables"]["base_ed_cohort"]
    
    logger.info("Validating event log...")
    
    # Temporal consistency check
    validation_sql = f"""
    WITH event_timing AS (
        SELECT
            e.*,
            b.ed_intime,
            b.ed_outtime,
            EXTRACT(EPOCH FROM (e.event_time - b.ed_intime)) / 3600.0 AS hours_from_ed
        FROM {event_log} e
        INNER JOIN {base} b USING (stay_id)
        WHERE e.event_time IS NOT NULL  -- Exclude hospitalization-level ICD events (NULL timing)
    )
    SELECT
        COUNT(*) AS total_events,
        COUNT(DISTINCT stay_id) AS visits_with_events,
        SUM(CASE WHEN event_time < ed_intime THEN 1 ELSE 0 END) AS events_before_ed,
        SUM(CASE WHEN hours_from_ed < 0 THEN 1 ELSE 0 END) AS negative_timing,
        SUM(CASE WHEN hours_from_ed <= 24 THEN 1 ELSE 0 END) AS events_within_24h,
        SUM(CASE WHEN hours_from_ed <= 48 THEN 1 ELSE 0 END) AS events_within_48h,
        AVG(hours_from_ed) AS mean_hours_from_ed,
        MIN(hours_from_ed) AS min_hours_from_ed,
        MAX(hours_from_ed) AS max_hours_from_ed
    FROM event_timing
    """
    
    df = fetch_df(conn, validation_sql)
    results = df.to_dict('records')[0]
    
    logger.info("Event Log Validation Results:")
    logger.info(f"  Total timed events: {results['total_events']:,}")
    logger.info(f"  ED visits with timed events: {results['visits_with_events']:,}")
    logger.info(f"  Events within 24h: {results['events_within_24h']:,}")
    logger.info(f"  Events within 48h: {results['events_within_48h']:,}")
    logger.info(f"  Mean time from ED: {results['mean_hours_from_ed']:.1f} hours")
    
    # Warnings
    if results['events_before_ed'] > 0:
        logger.warning(f"  [WARN] {results['events_before_ed']} events occur before ED arrival!")
    if results['negative_timing'] > 0:
        logger.warning(f"  [WARN] {results['negative_timing']} events have negative timing!")
    
    return results
