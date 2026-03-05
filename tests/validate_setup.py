"""
Standalone validation script to test pipeline setup
Run this before executing the full pipeline
"""

import sys
from pathlib import Path
import logging

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

from src.utils import load_yaml, setup_logging
from src.db import check_connection, get_conn, fetch_df

logger = logging.getLogger(__name__)


def check_config_files():
    """Check that all configuration files exist and are valid"""
    logger.info("Checking configuration files...")
    
    config_files = [
        "config/config.yaml",
        "config/outcomes.yaml",
        "config/feature_catalog.yaml",
    ]
    
    all_ok = True
    for cfg_file in config_files:
        path = Path(cfg_file)
        if not path.exists():
            logger.error(f"  ✗ Missing: {cfg_file}")
            all_ok = False
        else:
            try:
                load_yaml(str(path))
                logger.info(f"  ✓ Valid: {cfg_file}")
            except Exception as e:
                logger.error(f"  ✗ Invalid YAML in {cfg_file}: {e}")
                all_ok = False
    
    return all_ok


def check_sql_files():
    """Check that all SQL files exist"""
    logger.info("\nChecking SQL files...")
    
    required_sql = [
        "sql/00_base_ed_cohort.sql",
        "sql/10_event_icu_admit.sql",
        "sql/11_event_pressors.sql",
        "sql/12_event_ventilation.sql",
        "sql/13_event_rrt.sql",
        "sql/14_event_acs.sql",
        "sql/15_event_revasc.sql",
        "sql/16_event_cardiac_arrest.sql",
        "sql/17_event_death.sql",
        "sql/20_outcomes_from_event_log.sql",
        "sql/30_features_w1.sql",
        "sql/31_features_w6.sql",
        "sql/32_features_w24.sql",
    ]
    
    all_ok = True
    for sql_file in required_sql:
        path = Path(sql_file)
        if not path.exists():
            logger.error(f"  ✗ Missing: {sql_file}")
            all_ok = False
        elif path.stat().st_size == 0:
            logger.error(f"  ✗ Empty: {sql_file}")
            all_ok = False
        else:
            logger.info(f"  ✓ Exists: {sql_file}")
    
    return all_ok


def check_database_connection(cfg):
    """Test database connectivity"""
    logger.info("\nChecking database connection...")
    
    if not check_connection(cfg):
        logger.error("  ✗ Database connection failed")
        return False
    
    logger.info("  ✓ Database connection successful")
    return True


def check_mimic_schemas(cfg):
    """Verify MIMIC schemas are accessible"""
    logger.info("\nChecking MIMIC schemas...")
    
    try:
        conn = get_conn(cfg)
        
        schemas = {
            "ED": cfg["schemas"]["ed"],
            "Hospital": cfg["schemas"]["hosp"],
            "ICU": cfg["schemas"]["icu"],
        }
        
        all_ok = True
        for name, schema in schemas.items():
            try:
                # Try to query a table from each schema
                if name == "ED":
                    sql = f"SELECT COUNT(*) as n FROM {schema}.edstays LIMIT 1;"
                elif name == "Hospital":
                    sql = f"SELECT COUNT(*) as n FROM {schema}.admissions LIMIT 1;"
                else:  # ICU
                    sql = f"SELECT COUNT(*) as n FROM {schema}.icustays LIMIT 1;"
                
                df = fetch_df(conn, sql)
                logger.info(f"  ✓ {name} schema accessible: {schema}")
                
            except Exception as e:
                logger.error(f"  ✗ Cannot access {name} schema ({schema}): {e}")
                all_ok = False
        
        conn.close()
        return all_ok
        
    except Exception as e:
        logger.error(f"  ✗ Schema check failed: {e}")
        return False


def check_mimic_data_counts(cfg):
    """Check that MIMIC tables have data"""
    logger.info("\nChecking MIMIC data availability...")
    
    try:
        conn = get_conn(cfg)
        
        checks = [
            (f"{cfg['schemas']['ed']}.edstays", "ED stays"),
            (f"{cfg['schemas']['hosp']}.admissions", "Hospital admissions"),
            (f"{cfg['schemas']['hosp']}.patients", "Patients"),
            (f"{cfg['schemas']['icu']}.icustays", "ICU stays"),
            (f"{cfg['schemas']['ed']}.triage", "ED triage"),
            (f"{cfg['schemas']['ed']}.vitalsign", "ED vital signs"),
        ]
        
        all_ok = True
        for table, description in checks:
            try:
                df = fetch_df(conn, f"SELECT COUNT(*) as n FROM {table};")
                count = df.iloc[0]['n']
                
                if count > 0:
                    logger.info(f"  ✓ {description}: {count:,} rows")
                else:
                    logger.warning(f"  ⚠️  {description}: 0 rows (table exists but empty)")
                    all_ok = False
                    
            except Exception as e:
                logger.error(f"  ✗ Cannot query {description}: {e}")
                all_ok = False
        
        conn.close()
        return all_ok
        
    except Exception as e:
        logger.error(f"  ✗ Data check failed: {e}")
        return False


def check_output_directories():
    """Verify output directories exist"""
    logger.info("\nChecking output directories...")
    
    dirs = [
        "artifacts/datasets",
        "artifacts/logs",
    ]
    
    all_ok = True
    for dir_path in dirs:
        path = Path(dir_path)
        if not path.exists():
            logger.warning(f"  ⚠️  Missing: {dir_path} (will be created)")
            try:
                path.mkdir(parents=True, exist_ok=True)
                logger.info(f"  ✓ Created: {dir_path}")
            except Exception as e:
                logger.error(f"  ✗ Cannot create {dir_path}: {e}")
                all_ok = False
        else:
            logger.info(f"  ✓ Exists: {dir_path}")
    
    return all_ok


def main():
    """Run all validation checks"""
    setup_logging(verbose=True)
    
    logger.info("=" * 70)
    logger.info("PIPELINE PRE-FLIGHT VALIDATION")
    logger.info("=" * 70)
    
    # Load config
    try:
        cfg = load_yaml("config/config.yaml")
    except Exception as e:
        logger.error(f"Cannot load config.yaml: {e}")
        return False
    
    # Run checks
    results = {
        "Configuration files": check_config_files(),
        "SQL files": check_sql_files(),
        "Database connection": check_database_connection(cfg),
        "MIMIC schemas": check_mimic_schemas(cfg),
        "MIMIC data": check_mimic_data_counts(cfg),
        "Output directories": check_output_directories(),
    }
    
    # Summary
    logger.info("\n" + "=" * 70)
    logger.info("VALIDATION SUMMARY")
    logger.info("=" * 70)
    
    all_passed = True
    for check_name, passed in results.items():
        status = "✓ PASS" if passed else "✗ FAIL"
        logger.info(f"{status}: {check_name}")
        if not passed:
            all_passed = False
    
    logger.info("=" * 70)
    
    if all_passed:
        logger.info("✓ ALL CHECKS PASSED - Ready to run pipeline!")
        logger.info("\nRun the pipeline with:")
        logger.info("  python -m src.main")
        return True
    else:
        logger.error("✗ SOME CHECKS FAILED - Please fix issues before running pipeline")
        logger.info("\nCommon fixes:")
        logger.info("  1. Set PGPASSWORD in .env file")
        logger.info("  2. Verify config.yaml has correct database settings")
        logger.info("  3. Ensure MIMIC-IV data is loaded in PostgreSQL")
        logger.info("  4. Check schema names match your installation")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
