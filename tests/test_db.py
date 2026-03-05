"""
Tests for database connection and query utilities
"""

import pytest
import os
from pathlib import Path

# Add src to path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.utils import load_yaml
from src.db import get_conn, run_sql, fetch_df, check_connection


@pytest.fixture
def config():
    """Load test configuration"""
    config_path = Path(__file__).parent.parent / "config" / "config.yaml"
    return load_yaml(str(config_path))


@pytest.fixture
def connection(config):
    """Get database connection for tests"""
    # Skip if no password set
    pwd_env = config["db"].get("password_env", "PGPASSWORD")
    if not os.environ.get(pwd_env):
        pytest.skip("Database password not set")
    
    conn = get_conn(config)
    yield conn
    conn.close()


def test_load_config(config):
    """Test configuration loading"""
    assert config is not None
    assert "db" in config
    assert "schemas" in config
    assert "tables" in config


def test_database_connection(config):
    """Test database connection"""
    result = check_connection(config)
    assert result is True, "Database connection failed"


def test_query_execution(connection):
    """Test basic query execution"""
    # Simple query that should always work
    sql = "SELECT 1 AS test_value;"
    df = fetch_df(connection, sql)
    
    assert len(df) == 1
    assert df.iloc[0]['test_value'] == 1


def test_schema_access(connection, config):
    """Test access to MIMIC schemas"""
    ed_schema = config["schemas"]["ed"]
    
    # Check if we can access the ED schema
    sql = f"""
    SELECT COUNT(*) as n
    FROM {ed_schema}.edstays
    LIMIT 1;
    """
    
    try:
        df = fetch_df(connection, sql)
        assert len(df) == 1
        assert 'n' in df.columns
    except Exception as e:
        pytest.fail(f"Could not access {ed_schema}.edstays: {e}")


def test_table_creation_and_drop(connection):
    """Test table creation and deletion"""
    test_table = "test_pipeline_table"
    
    # Create table
    run_sql(connection, f"""
        DROP TABLE IF EXISTS {test_table};
        CREATE TABLE {test_table} (id INT, name TEXT);
    """)
    
    # Insert data
    run_sql(connection, f"""
        INSERT INTO {test_table} VALUES (1, 'test');
    """)
    
    # Query data
    df = fetch_df(connection, f"SELECT * FROM {test_table};")
    assert len(df) == 1
    assert df.iloc[0]['id'] == 1
    
    # Clean up
    run_sql(connection, f"DROP TABLE {test_table};")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
