import os
import sqlite3
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import List, Dict, Any
import logging
from conf.config import DB_TYPE, DB_FILE, DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME

logger = logging.getLogger(__name__)

def connect_to_db():
    """
    Establish a connection to the database (SQLite or PostgreSQL) based on configuration.
    """
    if DB_TYPE == "sqlite":
        os.makedirs(os.path.dirname(DB_FILE), exist_ok=True)
        try:
            conn = sqlite3.connect(DB_FILE)
            conn.row_factory = sqlite3.Row  # Enable dictionary-like row access
            logger.info("Connected to SQLite database.")
            return conn
        except sqlite3.Error as e:
            logger.error(f"Error connecting to SQLite: {e}")
            raise
    elif DB_TYPE == "postgres":
        try:
            conn = psycopg2.connect(
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                host=DB_HOST,
                port=DB_PORT,
                cursor_factory=RealDictCursor  # Return rows as dictionaries
            )
            logger.info("Connected to PostgreSQL database.")
            return conn
        except psycopg2.Error as e:
            logger.error(f"Error connecting to PostgreSQL: {e}")
            raise
    else:
        raise ValueError(f"Unsupported database type: {DB_TYPE}")
    
def get_table_names():
    """
    Retrieve the list of table names from the database.
    """
    conn = connect_to_db()
    try:
        if DB_TYPE == "sqlite":
            query = "SELECT name FROM sqlite_master WHERE type='table';"
        elif DB_TYPE == "postgres":
            query = """
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public';
            """
        else:
            raise ValueError(f"Unsupported database type: {DB_TYPE}")

        cursor = conn.cursor()
        cursor.execute(query)
        tables = [row[0] for row in cursor.fetchall()]
        logger.info(f"Found tables: {tables}")
        return tables
    except Exception as e:
        logger.error(f"Error retrieving table names: {e}")
        return []
    finally:
        conn.close()


def read_table(table_name: str):
    logger.info(f"Attempting to read table '{table_name}' from DB file '{DB_FILE}'.")
    conn = connect_to_db()
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    try:
        cursor.execute(f"SELECT * FROM {table_name}")
        rows = cursor.fetchall()
        logger.info(f"Successfully read {len(rows)} rows from table '{table_name}'.")
    except sqlite3.OperationalError as e:
        logger.error(f"OperationalError while reading table '{table_name}': {e}")
        return []
    except Exception as e:
        logger.error(f"Unexpected error while reading table '{table_name}': {e}")
        return []
    finally:
        conn.close()

    return [dict(row) for row in rows]


def dynamic_import(module_name: str, function_name: str):
    """
    Dynamically import `function_name` from `module_name`.
    Example: module='condition_mapping', function='map_condition'.
    """
    import importlib
    mod = importlib.import_module(module_name)
    return getattr(mod, function_name)

def read_all_resources(resources: List[Dict[str, str]]) -> Dict[str, List[Dict[str, Any]]]:
    """
    For each resource in the provided resource list, dynamically load its mapper and read from
    a DB table named exactly the same as resource_def["name"].

    Returns a dict: { resource_name -> list_of_mapped_dicts }.
    """
    results = {}

    for resource_def in resources:
        resource_name = resource_def.get("name")  # e.g., 'Condition'
        mapper_module = resource_def.get("mapper_module")
        mapper_function = resource_def.get("mapper_function")

        # Skip if any required info is missing
        if not resource_name or not mapper_module or not mapper_function:
            logger.warning(f"Incomplete resource definition: {resource_def}. Skipping.")
            continue

        # Attempt to import the mapper function
        try:
            mapper_func = dynamic_import(mapper_module, mapper_function)
        except (ImportError, AttributeError) as e:
            logger.error(f"Could not import {mapper_module}.{mapper_function}: {e}")
            continue

        # Resource name is also the DB table name
        table_name = resource_name
        try:
            rows = read_table(table_name)
        except Exception as e:
            logger.error(f"Error reading table {table_name} for resource '{resource_name}': {e}")
            continue

        if not rows:
            logger.info(f"No rows found for table '{table_name}'.")
            results[resource_name] = []
            continue

        # Map each row
        mapped = [mapper_func(row) for row in rows]
        results[resource_name] = mapped
        logger.info(f"Mapped {len(mapped)} rows for resource '{resource_name}'.")

    return results
#print(f"DB_FILE: {DB_FILE}")
def list_tables():
    """
    Retrieve a list of all table names in the configured database.
    
    :return: A list of table names.
    """
    conn = None
    try:
        conn = connect_to_db()
        cursor = conn.cursor()
        
        if DB_TYPE == "sqlite":
            query = "SELECT name FROM sqlite_master WHERE type='table';"
        elif DB_TYPE == "postgres":
            query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public';
            """
        else:
            raise ValueError(f"Unsupported database type: {DB_TYPE}")
        
        cursor.execute(query)
        tables = [row[0] for row in cursor.fetchall()]
        logger.info(f"Found tables: {tables}")
        return tables
    except Exception as e:
        logger.error(f"Error listing tables: {e}")
        return []
    finally:
        if conn:
            conn.close()
