"""
Tests for pipeline components
"""

import pytest
from pathlib import Path
import os

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.utils import load_yaml, render_sql_template, read_sql, get_sql_mapping
from src.db import get_conn


@pytest.fixture
def config():
    """Load configuration"""
    config_path = Path(__file__).parent.parent / "config" / "config.yaml"
    return load_yaml(str(config_path))


@pytest.fixture
def connection(config):
    """Get database connection"""
    pwd_env = config["db"].get("password_env", "PGPASSWORD")
    if not os.environ.get(pwd_env):
        pytest.skip("Database password not set")
    
    conn = get_conn(config)
    yield conn
    conn.close()


def test_sql_template_rendering(config):
    """Test SQL template placeholder replacement"""
    template = "SELECT * FROM {schema}.{table} WHERE id = {id_value}"
    
    mapping = {
        "schema": "public",
        "table": "test_table",
        "id_value": "123"
    }
    
    result = render_sql_template(template, mapping)
    expected = "SELECT * FROM public.test_table WHERE id = 123"
    
    assert result == expected


def test_sql_file_reading():
    """Test reading SQL files"""
    sql_file = Path(__file__).parent.parent / "sql" / "00_base_ed_cohort.sql"
    
    if not sql_file.exists():
        pytest.skip("SQL file not found")
    
    content = read_sql(str(sql_file))
    
    assert content is not None
    assert len(content) > 0
    assert "SELECT" in content.upper()


def test_get_sql_mapping(config):
    """Test SQL mapping generation"""
    mapping = get_sql_mapping(config)
    
    # Check that all required keys are present
    required_keys = [
        "ed_schema", "hosp_schema", "icu_schema",
        "base_ed_cohort", "event_log", "outcomes",
        "features_w1", "features_w6", "features_w24"
    ]
    
    for key in required_keys:
        assert key in mapping, f"Missing key: {key}"


def test_base_cohort_sql_validity(config):
    """Test that base cohort SQL is valid"""
    sql_file = Path(__file__).parent.parent / "sql" / "00_base_ed_cohort.sql"
    
    if not sql_file.exists():
        pytest.skip("SQL file not found")
    
    mapping = get_sql_mapping(config)
    sql = read_sql(str(sql_file))
    rendered = render_sql_template(sql, mapping)
    
    # Check that no unresolved placeholders remain
    assert "{" not in rendered or "}" not in rendered or "{}" in rendered


def test_event_sql_files_exist():
    """Test that all event SQL files exist"""
    sql_dir = Path(__file__).parent.parent / "sql"
    
    event_files = [
        "10_event_icu_admit.sql",
        "11_event_pressors.sql",
        "12_event_ventilation.sql",
        "13_event_rrt.sql",
        "14_event_acs.sql",
        "15_event_revasc.sql",
        "16_event_cardiac_arrest.sql",
        "17_event_death.sql",
    ]
    
    for filename in event_files:
        filepath = sql_dir / filename
        assert filepath.exists(), f"Missing SQL file: {filename}"
        
        # Check file is not empty
        content = filepath.read_text()
        assert len(content) > 0, f"Empty SQL file: {filename}"


def test_feature_sql_files_exist():
    """Test that feature SQL files exist"""
    sql_dir = Path(__file__).parent.parent / "sql"
    
    feature_files = [
        "30_features_w1.sql",
        "31_features_w6.sql",
        "32_features_w24.sql",
    ]
    
    for filename in feature_files:
        filepath = sql_dir / filename
        assert filepath.exists(), f"Missing SQL file: {filename}"


def test_yaml_configs_valid():
    """Test that all YAML configs are valid"""
    config_dir = Path(__file__).parent.parent / "config"
    
    yaml_files = [
        "config.yaml",
        "outcomes.yaml",
        "feature_catalog.yaml",
    ]
    
    for filename in yaml_files:
        filepath = config_dir / filename
        assert filepath.exists(), f"Missing config file: {filename}"
        
        # Try to load it
        cfg = load_yaml(str(filepath))
        assert cfg is not None, f"Could not load {filename}"
        assert len(cfg) > 0, f"Empty config: {filename}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
