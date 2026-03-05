"""
Database connection and query execution utilities code
"""

import os
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from typing import Optional, Dict, Any, List

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)


def get_conn(cfg: Dict[str, Any]) -> psycopg2.extensions.connection:
    """
    Create a PostgreSQL database connection.
    
    Args:
        cfg: Configuration dictionary with database settings
        
    Returns:
        psycopg2 connection object
        
    Raises:
        psycopg2.Error: If connection fails
    """
    password_env = cfg["db"].get("password_env", "PGPASSWORD")
    pwd = os.environ.get(password_env, "")
    
    if not pwd:
        logger.warning(f"Password not found in environment variable: {password_env}")
    
    try:
        conn = psycopg2.connect(
            host=cfg["db"]["host"],
            port=cfg["db"]["port"],
            dbname=cfg["db"]["name"],
            user=cfg["db"]["user"],
            password=pwd,
        )
        logger.info(f"Connected to database: {cfg['db']['name']} @ {cfg['db']['host']}")
        return conn
    except psycopg2.Error as e:
        logger.error(f"Database connection failed: {e}")
        raise


def run_sql(
    conn: psycopg2.extensions.connection,
    sql: str,
    params: Optional[tuple] = None,
    log_query: bool = False
) -> None:
    """
    Execute a SQL statement (DDL/DML).
    
    Args:
        conn: Database connection
        sql: SQL statement to execute
        params: Optional query parameters
        log_query: Whether to log the full query
        
    Raises:
        psycopg2.Error: If query execution fails
    """
    if log_query:
        logger.debug(f"Executing SQL:\n{sql[:500]}...")
    
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
        conn.commit()
        logger.debug("SQL executed successfully")
    except psycopg2.Error as e:
        logger.error(f"SQL execution failed: {e}")
        conn.rollback()
        raise


def fetch_df(
    conn: psycopg2.extensions.connection,
    sql: str,
    params: Optional[tuple] = None
):
    """
    Execute a SELECT query and return results as pandas DataFrame.
    
    Args:
        conn: Database connection
        sql: SELECT query
        params: Optional query parameters
        
    Returns:
        pandas DataFrame with query results
        
    Raises:
        psycopg2.Error: If query execution fails
    """
    import pandas as pd
    
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
        
        df = pd.DataFrame(rows)
        logger.debug(f"Fetched {len(df)} rows")
        return df
    except psycopg2.Error as e:
        logger.error(f"Query failed: {e}")
        raise


def check_connection(cfg: Dict[str, Any]) -> bool:
    """
    Test database connection and return status.
    
    Args:
        cfg: Configuration dictionary
        
    Returns:
        True if connection successful, False otherwise
    """
    try:
        conn = get_conn(cfg)
        with conn.cursor() as cur:
            cur.execute("SELECT version();")
            version = cur.fetchone()
            logger.info(f"PostgreSQL version: {version[0]}")
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Connection test failed: {e}")
        return False


def execute_with_progress(
    conn: psycopg2.extensions.connection,
    sql: str,
    description: str = "Executing query"
) -> None:
    """
    Execute SQL with progress indication.
    
    Args:
        conn: Database connection
        sql: SQL statement
        description: Description for logging
    """
    logger.info(f"{description}...")
    try:
        run_sql(conn, sql)
        logger.info(f"{description} - COMPLETED")
    except Exception as e:
        logger.error(f"{description} - FAILED: {e}")
        raise


def get_table_row_count(
    conn: psycopg2.extensions.connection,
    table_name: str
) -> int:
    """
    Get row count for a table.
    
    Args:
        conn: Database connection
        table_name: Name of the table
        
    Returns:
        Number of rows in the table
    """
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table_name};")
            count = cur.fetchone()[0]
        return count
    except psycopg2.Error as e:
        logger.warning(f"Could not get row count for {table_name}: {e}")
        return -1


# Aliases for compatibility
table_row_count = get_table_row_count


def table_exists(
    conn: psycopg2.extensions.connection,
    table_name: str
) -> bool:
    """
    Check if a table exists in the database.
    
    Args:
        conn: Database connection
        table_name: Name of the table (can include schema prefix)
        
    Returns:
        True if table exists, False otherwise
    """
    try:
        with conn.cursor() as cur:
            # Handle schema.table format
            if '.' in table_name:
                schema, table = table_name.split('.', 1)
            else:
                schema = 'public'
                table = table_name
            
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = %s AND table_name = %s
                );
            """, (schema, table))
            return cur.fetchone()[0]
    except psycopg2.Error as e:
        logger.warning(f"Could not check if table exists {table_name}: {e}")
        return False
