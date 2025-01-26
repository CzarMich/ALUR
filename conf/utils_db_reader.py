import logging
from typing import List, Dict, Any
from conf.utils_session import get_db_connection, release_db_connection

logger = logging.getLogger(__name__)

def get_table_names() -> List[str]:
    """
    Retrieve the list of table names from the database.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        if conn.__class__.__name__ == "Connection":  # SQLite
            query = "SELECT name FROM sqlite_master WHERE type='table';"
        else:  # PostgreSQL
            query = """
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public';
            """
        cursor.execute(query)
        tables = [row[0] for row in cursor.fetchall()]
        logger.info(f"Found tables: {tables}")
        return tables
    except Exception as e:
        logger.error(f"Error retrieving table names: {e}")
        return []
    finally:
        release_db_connection(conn)


def read_unprocessed_rows(table_name: str) -> List[Dict[str, Any]]:
    """
    Read unprocessed rows from the specified table.
    """
    logger.info(f"Reading unprocessed rows from table '{table_name}'.")
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        query = f"SELECT * FROM {table_name} WHERE processed = FALSE"
        cursor.execute(query)
        rows = cursor.fetchall()
        logger.info(f"Retrieved {len(rows)} unprocessed rows from table '{table_name}'.")
        return [dict(row) for row in rows]
    except Exception as e:
        logger.error(f"Error reading unprocessed rows from table '{table_name}': {e}")
        return []
    finally:
        release_db_connection(conn)


def mark_row_as_processed(table_name: str, row_id: int):
    """
    Mark a row as processed in the database.
    """
    logger.info(f"Marking row ID {row_id} as processed in table '{table_name}'.")
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        if conn.__class__.__name__ == "Connection":  # SQLite
            query = f"UPDATE {table_name} SET processed = TRUE WHERE id = ?"
        else:  # PostgreSQL
            query = f"UPDATE {table_name} SET processed = TRUE WHERE id = %s"
        cursor.execute(query, (row_id,))
        conn.commit()
        logger.info(f"Marked row ID {row_id} as processed in table '{table_name}'.")
    except Exception as e:
        logger.error(f"Error marking row ID {row_id} as processed in table '{table_name}': {e}")
    finally:
        release_db_connection(conn)


def dynamic_import(module_name: str, function_name: str):
    """
    Dynamically import `function_name` from `module_name`.
    Example: module='condition_mapping', function='map_condition'.
    """
    import importlib
    try:
        mod = importlib.import_module(module_name)
        return getattr(mod, function_name)
    except (ImportError, AttributeError) as e:
        logger.error(f"Error dynamically importing {module_name}.{function_name}: {e}")
        raise


def read_table(table_name: str) -> List[Dict[str, Any]]:
    """
    Read all rows from a specified table.
    """
    logger.info(f"Reading all rows from table '{table_name}'.")
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        query = f"SELECT * FROM {table_name}"
        cursor.execute(query)
        rows = cursor.fetchall()
        logger.info(f"Retrieved {len(rows)} rows from table '{table_name}'.")
        return [dict(row) for row in rows]
    except Exception as e:
        logger.error(f"Error reading table '{table_name}': {e}")
        return []
    finally:
        release_db_connection(conn)


def list_tables() -> List[str]:
    """
    Retrieve a list of all table names in the database.
    """
    logger.info("Listing all tables in the database.")
    return get_table_names()
