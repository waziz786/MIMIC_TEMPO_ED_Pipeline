"""
Utility functions for configuration, SQL templates, and file I/O
"""

from pathlib import Path
from typing import Dict, Any
import yaml
import logging

logger = logging.getLogger(__name__)


def load_yaml(path: str) -> Dict[str, Any]:
    """
    Load YAML configuration file.
    
    Args:
        path: Path to YAML file
        
    Returns:
        Dictionary with configuration
        
    Raises:
        FileNotFoundError: If file doesn't exist
        yaml.YAMLError: If file is invalid YAML
    """
    try:
        with open(path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
        logger.info(f"Loaded configuration from: {path}")
        return config
    except FileNotFoundError:
        logger.error(f"Configuration file not found: {path}")
        raise
    except yaml.YAMLError as e:
        logger.error(f"Invalid YAML in {path}: {e}")
        raise


def render_sql_template(sql_text: str, mapping: Dict[str, Any]) -> str:
    """
    Replace template placeholders in SQL with actual values.
    
    Template placeholders use {key} format.
    
    Args:
        sql_text: SQL with template placeholders
        mapping: Dictionary mapping placeholder names to values
        
    Returns:
        Rendered SQL string
        
    Example:
        >>> sql = "SELECT * FROM {schema}.{table}"
        >>> render_sql_template(sql, {"schema": "public", "table": "users"})
        'SELECT * FROM public.users'
    """
    rendered = sql_text
    for key, value in mapping.items():
        placeholder = "{" + key + "}"
        rendered = rendered.replace(placeholder, str(value))
    
    # Check for unresolved placeholders
    if "{" in rendered and "}" in rendered:
        unresolved = [s.split("}")[0] for s in rendered.split("{")[1:]]
        logger.warning(f"Unresolved placeholders in SQL: {unresolved}")
    
    return rendered


def read_sql(path: str) -> str:
    """
    Read SQL file contents.
    
    Args:
        path: Path to SQL file
        
    Returns:
        SQL file contents as string
        
    Raises:
        FileNotFoundError: If file doesn't exist
    """
    try:
        sql_path = Path(path)
        if not sql_path.exists():
            raise FileNotFoundError(f"SQL file not found: {path}")
        
        sql_text = sql_path.read_text(encoding='utf-8')
        logger.debug(f"Read SQL from: {path}")
        return sql_text
    except Exception as e:
        logger.error(f"Failed to read SQL file {path}: {e}")
        raise


def get_sql_mapping(cfg: Dict[str, Any]) -> Dict[str, str]:
    """
    Create a standard SQL template mapping from config.
    
    Args:
        cfg: Configuration dictionary
        
    Returns:
        Mapping dictionary for SQL template rendering
    """
    mapping = {
        # Schemas
        "ed_schema": cfg["schemas"]["ed"],
        "hosp_schema": cfg["schemas"]["hosp"],
        "icu_schema": cfg["schemas"]["icu"],
        
        # Tables
        "base_ed_cohort": cfg["tables"]["base_ed_cohort"],
        "event_log": cfg["tables"]["event_log"],
        "outcomes": cfg["tables"]["outcomes"],
        "features_w1": cfg["tables"]["features_w1"],
        "features_w6": cfg["tables"]["features_w6"],
        "features_w24": cfg["tables"]["features_w24"],
    }
    # Add truncated table names if configured
    if "features_w6_truncated" in cfg.get("tables", {}):
        mapping["features_w6_truncated"] = cfg["tables"]["features_w6_truncated"]
    if "features_w24_truncated" in cfg.get("tables", {}):
        mapping["features_w24_truncated"] = cfg["tables"]["features_w24_truncated"]
    return mapping


def ensure_output_dir(path: str) -> None:
    """
    Ensure output directory exists, create if needed.
    
    Args:
        path: Directory path
    """
    output_dir = Path(path)
    if not output_dir.exists():
        output_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Created output directory: {path}")


def setup_logging(
    log_file: str = None,
    level: int = logging.INFO,
    verbose: bool = False
) -> None:
    """
    Configure logging for the pipeline.
    
    Args:
        log_file: Path to log file (optional)
        level: Logging level
        verbose: If True, set to DEBUG level
    """
    if verbose:
        level = logging.DEBUG
    
    handlers = [logging.StreamHandler()]
    
    if log_file:
        ensure_output_dir(Path(log_file).parent)
        handlers.append(logging.FileHandler(log_file))
    
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=handlers
    )
    
    logger.info(f"Logging initialized (level: {logging.getLevelName(level)})")


def format_duration(seconds: float) -> str:
    """
    Format duration in seconds to human-readable string.
    
    Args:
        seconds: Duration in seconds
        
    Returns:
        Formatted string (e.g., "2m 30s")
    """
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        mins = int(seconds // 60)
        secs = int(seconds % 60)
        return f"{mins}m {secs}s"
    else:
        hours = int(seconds // 3600)
        mins = int((seconds % 3600) // 60)
        return f"{hours}h {mins}m"
