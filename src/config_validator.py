"""
Configuration Validation Module

Validates YAML configuration files with schema checking and actionable error messages.
Catches typos and misconfigurations early before database operations.
"""

import logging
from typing import Dict, Any, List, Tuple, Optional
from pathlib import Path

logger = logging.getLogger(__name__)


class ConfigValidationError(Exception):
    """Exception raised for configuration validation failures."""
    
    def __init__(self, message: str, errors: List[str] = None):
        self.errors = errors or []
        super().__init__(message)
    
    def __str__(self):
        if self.errors:
            return f"{self.args[0]}\n" + "\n".join(f"  - {e}" for e in self.errors)
        return self.args[0]


# ============================================================
# SCHEMA DEFINITIONS
# ============================================================

CONFIG_SCHEMA = {
    "required": ["schemas", "tables"],  # db can be "db" or "database"
    "db_keys": ["db", "database"],  # Accept both naming conventions
    "database": {
        "required": ["host", "port"],  # user/password can come from env
        "types": {
            "host": str,
            "port": int,
            "dbname": str,
            "name": str,  # Alias for dbname
            "user": str,
            "password": str,
            "password_env": str,
        }
    },
    "schemas": {
        "required": ["ed", "hosp", "icu"],
        "types": {"ed": str, "hosp": str, "icu": str}
    },
    "tables": {
        "required": ["base_ed_cohort", "event_log", "outcomes", "features_w1", "features_w6", "features_w24"],
        "types": {key: str for key in ["base_ed_cohort", "event_log", "outcomes", "features_w1", "features_w6", "features_w24"]}
    }
}

DATASETS_SCHEMA = {
    "required": ["datasets"],
    "settings": {
        "types": {
            "add_missing_indicators": bool,
            "missing_threshold": (int, float),
            "output_dir": str,
        }
    },
    "datasets": {
        "item_required": ["window", "outcome"],
        "item_types": {
            "window": str,
            "outcome": str,
            "cohort_type": str,
            "filter": str,
            "description": str,
        },
        "valid_windows": ["W1", "W6", "W24"],
        "valid_cohorts": ["all", "admitted", "not_admitted"],
    }
}

OUTCOMES_SCHEMA = {
    "required": ["outcome_sets"],
    "outcome_sets": {
        "item_required": ["horizon_hours", "events"],
        "item_types": {
            "description": str,
            "horizon_hours": (int, float),
            "events": list,
        },
        "valid_events": [
            "ICU_ADMIT", "PRESSOR_START", "VENT_START", "RRT_START",
            "CARDIAC_ARREST", "DEATH", "ACS", "PCI", "CABG"
        ]
    }
}


# ============================================================
# VALIDATION FUNCTIONS
# ============================================================

def validate_config(cfg: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """
    Validate main config.yaml structure.
    
    Args:
        cfg: Loaded configuration dictionary
        
    Returns:
        Tuple of (is_valid, list of error messages)
    """
    errors = []
    
    # Check required top-level keys
    for key in CONFIG_SCHEMA["required"]:
        if key not in cfg:
            errors.append(f"Missing required section: '{key}'")
    
    # Check for database section (accept 'db' or 'database')
    db_key = None
    for key in CONFIG_SCHEMA["db_keys"]:
        if key in cfg:
            db_key = key
            break
    
    if db_key is None:
        errors.append("Missing database section: need 'db' or 'database'")
    
    if errors:
        return False, errors
    
    # Check database section
    db_cfg = cfg.get(db_key, {})
    for key in CONFIG_SCHEMA["database"]["required"]:
        if key not in db_cfg:
            errors.append(f"{db_key}.{key} - required for connection")
    
    # Validate database types
    for key, expected_type in CONFIG_SCHEMA["database"]["types"].items():
        if key in db_cfg and not isinstance(db_cfg[key], expected_type):
            errors.append(f"{db_key}.{key} should be {expected_type.__name__}, got {type(db_cfg[key]).__name__}")
    
    # Check port range
    if "port" in db_cfg and isinstance(db_cfg["port"], int):
        if not (1 <= db_cfg["port"] <= 65535):
            errors.append(f"{db_key}.port must be 1-65535, got {db_cfg['port']}")
    
    # Check schemas section
    schema_cfg = cfg.get("schemas", {})
    for key in CONFIG_SCHEMA["schemas"]["required"]:
        if key not in schema_cfg:
            errors.append(f"Missing schemas.{key}")
    
    # Check tables section
    tables_cfg = cfg.get("tables", {})
    for key in CONFIG_SCHEMA["tables"]["required"]:
        if key not in tables_cfg:
            errors.append(f"Missing tables.{key}")
    
    return len(errors) == 0, errors


def validate_datasets_config(
    datasets_cfg: Dict[str, Any],
    outcomes_cfg: Dict[str, Any] = None
) -> Tuple[bool, List[str]]:
    """
    Validate datasets.yaml structure and cross-reference with outcomes.
    
    Args:
        datasets_cfg: Loaded datasets configuration
        outcomes_cfg: Loaded outcomes configuration (for cross-validation)
        
    Returns:
        Tuple of (is_valid, list of error messages)
    """
    errors = []
    warnings = []
    
    # Check required top-level keys
    if "datasets" not in datasets_cfg:
        errors.append("Missing 'datasets' section")
        return False, errors
    
    datasets = datasets_cfg.get("datasets", {})
    settings = datasets_cfg.get("settings", {})
    
    # Validate settings
    if settings:
        threshold = settings.get("missing_threshold")
        if threshold is not None:
            if not isinstance(threshold, (int, float)):
                errors.append(f"settings.missing_threshold should be number, got {type(threshold).__name__}")
            elif not (0 <= threshold <= 1):
                errors.append(f"settings.missing_threshold should be 0-1, got {threshold}")
    
    # Get valid outcome names if outcomes config provided
    valid_outcomes = set()
    if outcomes_cfg and "outcome_sets" in outcomes_cfg:
        valid_outcomes = set(outcomes_cfg["outcome_sets"].keys())
    
    # Validate each dataset
    for name, ds_cfg in datasets.items():
        prefix = f"datasets.{name}"
        
        # Check required fields
        for key in DATASETS_SCHEMA["datasets"]["item_required"]:
            if key not in ds_cfg:
                errors.append(f"{prefix}: missing required field '{key}'")
        
        # Validate window
        window = ds_cfg.get("window", "")
        if window and window not in DATASETS_SCHEMA["datasets"]["valid_windows"]:
            valid = DATASETS_SCHEMA["datasets"]["valid_windows"]
            errors.append(f"{prefix}.window: '{window}' not valid. Use one of: {valid}")
        
        # Validate cohort_type
        cohort = ds_cfg.get("cohort_type", "all")
        if cohort not in DATASETS_SCHEMA["datasets"]["valid_cohorts"]:
            valid = DATASETS_SCHEMA["datasets"]["valid_cohorts"]
            errors.append(f"{prefix}.cohort_type: '{cohort}' not valid. Use one of: {valid}")
        
        # Cross-validate outcome against outcomes.yaml
        outcome = ds_cfg.get("outcome", "")
        if outcome and valid_outcomes and outcome not in valid_outcomes:
            # Find similar outcomes (simple fuzzy match)
            similar = [o for o in valid_outcomes if outcome.split("_")[0] in o]
            suggestion = f" Did you mean: {similar}?" if similar else ""
            errors.append(f"{prefix}.outcome: '{outcome}' not found in outcomes.yaml.{suggestion}")
    
    return len(errors) == 0, errors


def validate_outcomes_config(outcomes_cfg: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """
    Validate outcomes.yaml structure.
    
    Args:
        outcomes_cfg: Loaded outcomes configuration
        
    Returns:
        Tuple of (is_valid, list of error messages)
    """
    errors = []
    
    if "outcome_sets" not in outcomes_cfg:
        errors.append("Missing 'outcome_sets' section")
        return False, errors
    
    outcome_sets = outcomes_cfg.get("outcome_sets", {})
    valid_events = set(OUTCOMES_SCHEMA["outcome_sets"]["valid_events"])
    
    for name, cfg in outcome_sets.items():
        prefix = f"outcome_sets.{name}"
        
        # Check required fields
        for key in OUTCOMES_SCHEMA["outcome_sets"]["item_required"]:
            if key not in cfg:
                errors.append(f"{prefix}: missing required field '{key}'")
        
        # Validate horizon_hours
        horizon = cfg.get("horizon_hours")
        if horizon is not None:
            if not isinstance(horizon, (int, float)):
                errors.append(f"{prefix}.horizon_hours should be number, got {type(horizon).__name__}")
            elif horizon <= 0:
                errors.append(f"{prefix}.horizon_hours must be positive, got {horizon}")
        
        # Validate events
        events = cfg.get("events", [])
        if events:
            if not isinstance(events, list):
                errors.append(f"{prefix}.events should be list, got {type(events).__name__}")
            else:
                for event in events:
                    if event not in valid_events:
                        errors.append(f"{prefix}.events: unknown event '{event}'. Valid: {sorted(valid_events)}")
    
    return len(errors) == 0, errors


def validate_all_configs(
    config_path: str = "config/config.yaml",
    datasets_path: str = "config/datasets.yaml",
    outcomes_path: str = "config/outcomes.yaml",
    raise_on_error: bool = True
) -> Dict[str, Any]:
    """
    Validate all configuration files with cross-references.
    
    Args:
        config_path: Path to main config
        datasets_path: Path to datasets config
        outcomes_path: Path to outcomes config
        raise_on_error: If True, raise ConfigValidationError on failure
        
    Returns:
        Dictionary with validation results and loaded configs
        
    Raises:
        ConfigValidationError: If validation fails and raise_on_error is True
    """
    from .utils import load_yaml
    
    result = {
        "valid": True,
        "errors": [],
        "warnings": [],
        "configs": {}
    }
    
    all_errors = []
    
    # Load and validate main config
    logger.info("Validating configuration files...")
    
    try:
        cfg = load_yaml(config_path)
        result["configs"]["config"] = cfg
        valid, errors = validate_config(cfg)
        if not valid:
            all_errors.extend([f"[config.yaml] {e}" for e in errors])
    except FileNotFoundError:
        all_errors.append(f"[config.yaml] File not found: {config_path}")
    except Exception as e:
        all_errors.append(f"[config.yaml] Failed to load: {e}")
    
    # Load and validate outcomes config
    outcomes_cfg = None
    try:
        outcomes_cfg = load_yaml(outcomes_path)
        result["configs"]["outcomes"] = outcomes_cfg
        valid, errors = validate_outcomes_config(outcomes_cfg)
        if not valid:
            all_errors.extend([f"[outcomes.yaml] {e}" for e in errors])
    except FileNotFoundError:
        all_errors.append(f"[outcomes.yaml] File not found: {outcomes_path}")
    except Exception as e:
        all_errors.append(f"[outcomes.yaml] Failed to load: {e}")
    
    # Load and validate datasets config
    try:
        datasets_cfg = load_yaml(datasets_path)
        result["configs"]["datasets"] = datasets_cfg
        valid, errors = validate_datasets_config(datasets_cfg, outcomes_cfg)
        if not valid:
            all_errors.extend([f"[datasets.yaml] {e}" for e in errors])
    except FileNotFoundError:
        # datasets.yaml is optional
        logger.debug(f"datasets.yaml not found (optional): {datasets_path}")
    except Exception as e:
        all_errors.append(f"[datasets.yaml] Failed to load: {e}")
    
    if all_errors:
        result["valid"] = False
        result["errors"] = all_errors
        
        if raise_on_error:
            raise ConfigValidationError(
                f"Configuration validation failed with {len(all_errors)} error(s):",
                all_errors
            )
    else:
        logger.info("[OK] All configuration files validated successfully")
    
    return result


def suggest_fix(error_msg: str) -> Optional[str]:
    """
    Provide actionable suggestions for common config errors.
    
    Args:
        error_msg: Error message
        
    Returns:
        Suggestion string or None
    """
    suggestions = {
        "missing required section: 'database'": 
            "Add a 'database:' section with host, port, dbname, user, password",
        "missing database.password":
            "Add password to database section or set MIMIC_PASSWORD environment variable",
        "not found in outcomes.yaml":
            "Check outcomes.yaml for available outcome names, or add a new outcome definition",
        "window: 'W12' not valid":
            "Valid windows are W1 (1 hour), W6 (6 hours), W24 (24 hours)",
    }
    
    for pattern, suggestion in suggestions.items():
        if pattern.lower() in error_msg.lower():
            return suggestion
    
    return None
